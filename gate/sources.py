"""Source legs: a database engine beside the postgres source, extracted to GCS and loaded and compacted into BigQuery
through Airflow. Every rivet config is the one `rivet init` writes against the live source, used as written: the gate
gives init a bucket and datasets of the run's own, and reads export names, prefixes and the checkpoint from what it
wrote (nothing is edited). The warehouse is read back by a client the loader does not use and compared with the source.
"""

from __future__ import annotations

import base64
import json
import re
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import duckdb
import yaml

from . import warehouse
from .stand import sh, state_db

#: A bucket the gate made for one config of a stack and engine: `rivet-afgate-<tag>-<stack>-<engine>-<batch|cdc>` (the
#: sweep drops nothing else).
BUCKET_SHAPE = re.compile(r"rivet-afgate-\d{8}[0-9a-f]{4}-\d+-[a-z]+-(batch|cdc)")


@dataclass(frozen=True)
class Limit:
    """What rivet's `init` does for a config kind the leg cannot run, and where rivet documents it. `refused`: init
    exits non-zero with `code` and writes no config; `no-load`: init writes the config without a `load:` block. The
    gate runs init for it on every stack and fails when init does anything else (the leg then has to run that kind)."""

    shape: str
    code: str
    documented: str


@dataclass(frozen=True)
class Engine:
    """What differs between source engines: the container, the Connection, the SQL client, the scenario's SQL."""

    name: str
    port: int
    conn_type: str
    #: The compose service, formatted with the stand's secrets (a leading newline-free block of `  gate-<name>:`).
    service_yaml: str
    #: The SQL client inside the container, reading statements on stdin; its password comes from `client_env`.
    client: tuple[str, ...]
    client_env: str
    ready: tuple[str, ...]
    #: Statements run once the server answers, before the seed (grants rivet needs).
    setup: str
    seed: str
    changes: str
    #: The tables of the scenario and the columns compared (the key first is `id`).
    tables: dict[str, list[str]]
    #: DuckDB's scanner for the engine and the attach string (host port and password filled in); without one, the
    #: source is read through the engine's own client as JSON (`json_select`), each column parsed by `json_kinds`.
    duck_type: str = ""
    duck_dsn: str = ""
    json_select: str = ""
    json_kinds: dict[str, str] = field(default_factory=dict)
    #: URL scheme rivet reads (the Connection's conn_type unless they differ) and init's `--tls` mode.
    scheme: str = ""
    tls: str = "disable"
    #: Appended to the root password where the server's password policy wants more than hex digits.
    root_suffix: str = ""
    #: A CDC capture instance under a name other than the engine's default, and the first rivet whose `init` reads it
    #: from the catalog (an older one writes the default name, so the gate gives it the default name).
    capture_instance: tuple[str, str, tuple[int, int, int]] | None = None
    #: The database (Oracle: the service name) the URL and the Connection name.
    database: str = "rivet"
    #: The key column of every table (compared by name, lower-case).
    key: str = "id"
    #: The configs the leg runs (`batch`, `cdc`); every other kind is a known limit of rivet's init (`limits`).
    kinds: tuple[str, ...] = ("batch", "cdc")
    limits: dict[str, Limit] = field(default_factory=dict)
    #: A statement the client answers with a `1` once the server takes statements; text put before every script.
    ping: str = "SELECT 1;"
    prefix: str = ""
    #: The source reader when neither DuckDB nor a JSON select is the engine's: `oracle` (python-oracledb) or
    #: `mongo` (pymongo), each a client rivet does not use; `select` maps a column to the expression it is read by.
    reader: str = ""
    select: dict[str, str] = field(default_factory=dict)
    ready_timeout: float = 300

    @property
    def service(self) -> str:
        return f"gate-{self.name}"

    @property
    def conn_id(self) -> str:
        return f"rivet_gate_{self.name}"

    @property
    def url_env(self) -> str:
        return f"RIVET_GATE_{self.name.upper()}_URL"

    def connection(self, secrets: Any) -> dict[str, Any]:
        """The Airflow Connection rivet reads the source through (its URL reaches rivet as `url_env`)."""
        return {
            "conn_type": self.conn_type,
            "host": self.service,
            "port": self.port,
            "login": "rivet",
            "password": getattr(secrets, self.name),
            "schema": self.database,
        }

    def url(self, secrets: Any) -> str:
        """The URL `rivet init` reads the source with (inside the stack's network)."""
        return f"{self.scheme or self.conn_type}://rivet:{getattr(secrets, self.name)}@{self.service}:{self.port}/{self.database}"

    def root_password(self, secrets: Any) -> str:
        return getattr(secrets, f"{self.name}_root") + self.root_suffix


MYSQL = Engine(
    name="mysql",
    port=3306,
    conn_type="mysql",
    service_yaml="""\
  gate-mysql:
    image: mysql:8.4
    environment: {{ MYSQL_ROOT_PASSWORD: {mysql_root}, MYSQL_DATABASE: rivet, MYSQL_USER: rivet, MYSQL_PASSWORD: {mysql} }}
    # rivet's MySQL CDC: row events with every column's name (binlog_row_metadata=FULL), GTIDs, utf8mb4 text.
    command: ["--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--binlog-format=ROW", "--binlog-row-image=FULL",
              "--binlog-row-metadata=FULL", "--character-set-server=utf8mb4", "--default-time-zone=+00:00"]
    ports: ["127.0.0.1::3306"]
""",
    client=("mysql", "--default-character-set=utf8mb4", "-uroot", "rivet"),
    client_env="MYSQL_PWD",
    ready=("mysqladmin", "ping", "-uroot", "--silent"),
    setup="GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivet'@'%';",
    # DATETIME(6) and TIMESTAMP(6) cursors, an exact DECIMAL, multi-byte text; MySQL bumps `updated_at` itself.
    seed="""
SET SESSION cte_max_recursion_depth = 10000;
DROP TABLE IF EXISTS gate_orders, gate_customers;
CREATE TABLE gate_customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL, tier VARCHAR(10) NOT NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6));
CREATE TABLE gate_orders (id INT AUTO_INCREMENT PRIMARY KEY, customer_id INT NOT NULL, amount DECIMAL(12,2) NOT NULL,
  status VARCHAR(20) NOT NULL, updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6));
INSERT INTO gate_customers (name, tier) WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 1000)
  SELECT CONCAT('клиент ', n), ELT(1 + n % 3, 'free', 'pro', 'team') FROM g;
INSERT INTO gate_orders (customer_id, amount, status) WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 5000)
  SELECT 1 + n % 1000, ROUND((n % 997) * 1.37, 2), 'new' FROM g;
""",
    # Every update changes a value (MySQL logs no event and keeps `updated_at` for a row an UPDATE leaves as it was).
    changes="""
SET SESSION cte_max_recursion_depth = 10000;
INSERT INTO gate_customers (name, tier) WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 20)
  SELECT CONCAT('новый клиент ', n), 'free' FROM g;
INSERT INTO gate_orders (customer_id, amount, status) WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 100)
  SELECT 1 + n % 1000, 9.99 * n, 'new' FROM g;
UPDATE gate_orders SET status = 'shipped' ORDER BY id LIMIT 30;
UPDATE gate_customers SET tier = 'vip' ORDER BY id LIMIT 10;
DELETE FROM gate_orders WHERE id <= 5000 ORDER BY id DESC LIMIT 5;
""",
    tables={
        "gate_customers": ["id", "name", "tier", "updated_at"],
        "gate_orders": ["id", "customer_id", "amount", "status", "updated_at"],
    },
    duck_type="mysql",
    duck_dsn="host=127.0.0.1 port={port} user=rivet password={password} database=rivet",
)
MSSQL = Engine(
    name="mssql",
    port=1433,
    conn_type="mssql",
    scheme="sqlserver",
    # SQL Server encrypts every login; the container's certificate is self-signed (`require` encrypts, verifies nothing).
    tls="require",
    root_suffix="Aa",  # sa's password policy wants three character classes; the hex secret has two
    service_yaml="""\
  gate-mssql:
    image: mcr.microsoft.com/mssql/server:2022-latest
    platform: linux/amd64
    # CDC capture runs as a SQL Server Agent job.
    environment: {{ ACCEPT_EULA: "Y", MSSQL_PID: Developer, MSSQL_AGENT_ENABLED: "true", MSSQL_SA_PASSWORD: {mssql_root}Aa }}
    ports: ["127.0.0.1::1433"]
""",
    client=("/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost", "-U", "sa", "-b", "-I", "-y", "0"),
    client_env="SQLCMDPASSWORD",
    ready=("/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost", "-U", "sa", "-b", "-Q", "SELECT 1"),
    setup="""
IF DB_ID('rivet') IS NULL CREATE DATABASE rivet;
GO
IF SUSER_ID('rivet') IS NULL CREATE LOGIN rivet WITH PASSWORD = '{password}', CHECK_POLICY = OFF;
GO
USE rivet;
IF USER_ID('rivet') IS NULL CREATE USER rivet FOR LOGIN rivet;
ALTER ROLE db_owner ADD MEMBER rivet;
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'rivet') = 0 EXEC sys.sp_cdc_enable_db;
GO
-- What rivet asks of an incremental read (without it a scan under locking READ COMMITTED can skip rows).
ALTER DATABASE rivet SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
""",
    # DATETIME2(7) cursors (rivet keeps microseconds), a DATETIMEOFFSET at +03:00, an exact DECIMAL, Cyrillic NVARCHAR,
    # a UNIQUEIDENTIFIER and VARBINARY; SQL Server has no ON UPDATE, so every change sets `updated_at` itself.
    seed="""
USE rivet;
SET NOCOUNT ON;
DROP TABLE IF EXISTS dbo.gate_orders, dbo.gate_customers;
CREATE TABLE dbo.gate_customers (id INT IDENTITY PRIMARY KEY, name NVARCHAR(100) NOT NULL, tier VARCHAR(10) NOT NULL,
  ext_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID(), updated_at DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME());
CREATE TABLE dbo.gate_orders (id INT IDENTITY PRIMARY KEY, customer_id INT NOT NULL, amount DECIMAL(12,2) NOT NULL,
  status NVARCHAR(20) NOT NULL, placed_at DATETIMEOFFSET(7) NOT NULL, payload VARBINARY(64) NULL,
  updated_at DATETIME2(7) NOT NULL DEFAULT SYSUTCDATETIME());
WITH g AS (SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.gate_customers (name, tier) SELECT CONCAT(N'клиент ', n), CHOOSE(1 + n % 3, 'free', 'pro', 'team') FROM g;
WITH g AS (SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
INSERT INTO dbo.gate_orders (customer_id, amount, status, placed_at, payload)
  SELECT 1 + n % 1000, ROUND((n % 997) * 1.37, 2), N'new',
    TODATETIMEOFFSET(DATEADD(SECOND, n, CAST('2026-01-01T10:00:00.1234567' AS DATETIME2(7))), '+03:00'),
    CASE WHEN n % 7 = 0 THEN NULL ELSE CAST(HASHBYTES('SHA2_256', CONCAT('p', n)) AS VARBINARY(64)) END
  FROM g;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'gate_customers', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'gate_orders', @role_name = NULL,
  @capture_instance = N'{orders_instance}';
""",
    # The last statement waits until the Agent's capture job has read the DELETE, so a CDC run right after sees every
    # change (`cdc.lsn_time_mapping` is filled by the capture job; its times are DATETIME, 3 ms ticks).
    changes="""
USE rivet;
SET NOCOUNT ON;
WITH g AS (SELECT TOP (20) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects)
INSERT INTO dbo.gate_customers (name, tier) SELECT CONCAT(N'новый клиент ', n), 'free' FROM g;
WITH g AS (SELECT TOP (100) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects)
INSERT INTO dbo.gate_orders (customer_id, amount, status, placed_at, payload)
  SELECT 1 + n % 1000, 9.99 * n, N'new', TODATETIMEOFFSET(SYSUTCDATETIME(), '-05:30'), 0x00FF10 FROM g;
UPDATE dbo.gate_orders SET status = N'отправлен', updated_at = SYSUTCDATETIME()
  WHERE id IN (SELECT TOP (30) id FROM dbo.gate_orders ORDER BY id);
UPDATE dbo.gate_customers SET tier = 'vip', updated_at = SYSUTCDATETIME()
  WHERE id IN (SELECT TOP (10) id FROM dbo.gate_customers ORDER BY id);
WAITFOR DELAY '00:00:00.200';
DECLARE @before DATETIME = DATEADD(MILLISECOND, -20, SYSDATETIME());
DELETE FROM dbo.gate_orders WHERE id IN (SELECT TOP (5) id FROM dbo.gate_orders WHERE id <= 5000 ORDER BY id DESC);
DECLARE @waited INT = 0;
WHILE NOT EXISTS (SELECT 1 FROM cdc.lsn_time_mapping WHERE tran_begin_time >= @before) AND @waited < 180
BEGIN WAITFOR DELAY '00:00:01'; SET @waited += 1; END;
IF @waited >= 180 THROW 50000, 'the CDC capture job did not read the changes within 180 s', 1;
""",
    tables={
        "gate_customers": ["id", "name", "tier", "ext_id", "updated_at"],
        "gate_orders": ["id", "customer_id", "amount", "status", "placed_at", "payload", "updated_at"],
    },
    json_select="SET NOCOUNT ON; SELECT {cols} FROM rivet.dbo.{table} FOR JSON PATH, INCLUDE_NULL_VALUES;",
    json_kinds={"updated_at": "ts", "placed_at": "ts", "ext_id": "uuid", "payload": "bytes", "amount": "money"},
    capture_instance=("gate_orders", "gate_orders_v2", (0, 32, 0)),
)
MONGODB = Engine(
    name="mongodb",
    port=27017,
    conn_type="mongo",
    scheme="mongodb",
    # A change stream needs a replica set (one member is enough); a replica set with users needs a key file. The
    # member is named by its service, so a client inside the stack finds the primary the set advertises.
    service_yaml="""\
  gate-mongodb:
    image: mongo:7
    environment: {{ MONGO_INITDB_ROOT_USERNAME: root, MONGO_INITDB_ROOT_PASSWORD: {mongodb_root} }}
    entrypoint: ["bash", "-c", "openssl rand -base64 48 > /tmp/rs.key && chmod 400 /tmp/rs.key && chown mongodb:mongodb /tmp/rs.key && exec docker-entrypoint.sh mongod --replSet rs0 --bind_ip_all --keyFile /tmp/rs.key"]
    ports: ["127.0.0.1::27017"]
""",
    # mongosh runs a script from a file (from stdin it exits 0 whatever fails); the script connects as root itself.
    client=("sh", "-c", 'f=$(mktemp) && cat > "$f" && exec mongosh --quiet --nodb "$f"'),
    client_env="GATE_MONGODB_ROOT",
    prefix='db = connect("mongodb://root:" + process.env.GATE_MONGODB_ROOT'
    ' + "@localhost:27017/rivet?authSource=admin&directConnection=true");\n',
    ping="print(1);",
    ready=("mongosh", "--quiet", "--eval", "db.runCommand({ping: 1}).ok"),
    setup="""
try { rs.status() } catch (e) { rs.initiate({_id: "rs0", members: [{_id: 0, host: "gate-mongodb:27017"}]}) }
let waited = 0;
while (!db.hello().isWritablePrimary) { if (waited++ > 120) throw new Error("no primary after 120 s"); sleep(1000); }
if (!db.getUser("rivet"))
  db.createUser({user: "rivet", pwd: "{password}", roles: [{role: "read", db: "rivet"}, {role: "clusterMonitor", db: "admin"}]});
""",
    # ObjectId and int64 keys, a document nested two deep with arrays, Decimal128, dates at milliseconds, an int64
    # past 2^53, a double, binary, null; every update changes a value.
    seed="""
db.gate_orders.drop(); db.gate_customers.drop();
const customers = [];
for (let n = 1; n <= 1000; n++) customers.push({
  _id: new ObjectId(), name: "клиент " + n, tier: ["free", "pro", "team"][n % 3],
  addr: {city: "Київ", zip: String(n).padStart(5, "0"), tags: ["a", n % 2 ? "odd" : "even"]},
  balance: NumberDecimal((n * 1.37).toFixed(2)), big: NumberLong("9007199254740993"), score: n + 0.25,
  note: n % 7 ? "x" : null, updated_at: new Date(Date.UTC(2026, 0, 1, 10) + n * 1000 + 123)});
db.gate_customers.insertMany(customers);
const orders = [];
for (let n = 1; n <= 5000; n++) orders.push({
  _id: NumberLong(String(n)), customer_id: customers[n % 1000]._id, amount: NumberDecimal(((n % 997) * 1.37).toFixed(2)),
  items: [{sku: "s" + n, qty: NumberInt(1 + n % 5), price: NumberDecimal("1.10")}], status: "new",
  payload: n % 7 ? BinData(0, "AP8Q") : null, placed_at: new Date(Date.UTC(2026, 0, 1, 7) + n * 1000)});
db.gate_orders.insertMany(orders);
""",
    changes="""
const now = new Date();
const some = db.gate_customers.find().sort({_id: 1}).limit(10).toArray().map(d => d._id);
db.gate_customers.insertMany(Array.from({length: 20}, (_, i) => ({
  name: "новый клиент " + (i + 1), tier: "free", addr: {city: "Львів", tags: []}, balance: NumberDecimal("0.00"),
  updated_at: now})));
db.gate_orders.insertMany(Array.from({length: 100}, (_, i) => ({
  _id: NumberLong(String(5001 + i)), customer_id: some[i % 10], amount: NumberDecimal((9.99 * (i + 1)).toFixed(2)),
  items: [], status: "new", placed_at: now})));
db.gate_orders.updateMany({_id: {$lte: NumberLong("30")}}, {$set: {status: "отправлен", updated_at: now}});
db.gate_customers.updateMany({_id: {$in: some}}, {$set: {tier: "vip", "addr.tags": ["vip"]}});
db.gate_orders.deleteMany({_id: {$gte: NumberLong("4996"), $lte: NumberLong("5000")}});
""",
    tables={"gate_customers": ["_id", "document"], "gate_orders": ["_id", "document"]},
    key="_id",
    kinds=("cdc",),
    limits={
        "batch": Limit(
            "refused",
            "RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED",
            "docs/reference/init.md: MongoDB's only batch mode is `full`; use `--mode cdc` for change capture",
        )
    },
    reader="mongo",
)
ORACLE = Engine(
    name="oracle",
    port=1521,
    conn_type="oracle",
    database="FREEPDB1",
    # rivet's own Oracle stand (gvenzl/oracle-free 23ai Free, arm64 too); scripts run as SYSDBA through the OS.
    service_yaml="""\
  gate-oracle:
    image: gvenzl/oracle-free:23-slim-faststart
    environment: {{ ORACLE_RANDOM_PASSWORD: "yes" }}
    ports: ["127.0.0.1::1521"]
""",
    client=("sqlplus", "-s", "-L", "/ as sysdba"),
    client_env="",
    prefix="WHENEVER SQLERROR EXIT FAILURE\nWHENEVER OSERROR EXIT FAILURE\nSET FEEDBACK OFF\n",
    ping="SELECT 1 FROM dual;\nEXIT\n",
    ready=("healthcheck.sh",),
    ready_timeout=600,
    # LogMiner's prerequisites as rivet's stand sets them (dev/oracle/init/02-logminer.sh): ARCHIVELOG and minimal
    # supplemental logging; the tables log ALL columns. Then the leg's user in the pluggable database.
    setup="""
HOST mkdir -p /opt/oracle/oradata/archive
ALTER SYSTEM SET log_archive_dest_1 = 'LOCATION=/opt/oracle/oradata/archive' SCOPE = BOTH;
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER PLUGGABLE DATABASE ALL SAVE STATE;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE USER rivet IDENTIFIED BY "{password}" QUOTA UNLIMITED ON USERS;
GRANT CREATE SESSION, CREATE TABLE TO rivet;
GRANT SELECT_CATALOG_ROLE TO rivet;
EXIT
""",
    # NUMBER(19) identity keys (Decimal128(19,0), BigQuery NUMERIC), NUMBER(12,2), a TIMESTAMP WITH TIME ZONE at +03:00
    # (the delta's at -05:30), Cyrillic NVARCHAR2, RAW and BLOB; no ON UPDATE, so every change sets `updated_at`.
    seed="""
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = rivet;
DROP TABLE IF EXISTS gate_orders;
DROP TABLE IF EXISTS gate_customers;
CREATE TABLE gate_customers (id NUMBER(19) GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name NVARCHAR2(100) NOT NULL,
  tier VARCHAR2(10) NOT NULL, updated_at TIMESTAMP(6) DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL);
CREATE TABLE gate_orders (id NUMBER(19) GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, customer_id NUMBER(19) NOT NULL,
  amount NUMBER(12,2) NOT NULL, status NVARCHAR2(20) NOT NULL, placed_at TIMESTAMP(6) WITH TIME ZONE NOT NULL,
  payload RAW(64), doc BLOB, updated_at TIMESTAMP(6) DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL);
INSERT INTO gate_customers (name, tier)
  SELECT N'клиент ' || LEVEL, CASE MOD(LEVEL, 3) WHEN 0 THEN 'free' WHEN 1 THEN 'pro' ELSE 'team' END
  FROM dual CONNECT BY LEVEL <= 1000;
INSERT INTO gate_orders (customer_id, amount, status, placed_at, payload, doc)
  SELECT 1 + MOD(LEVEL, 1000), ROUND(MOD(LEVEL, 997) * 1.37, 2), N'new',
    FROM_TZ(TIMESTAMP '2026-01-01 10:00:00.123456' + NUMTODSINTERVAL(LEVEL, 'SECOND'), '+03:00'),
    CASE WHEN MOD(LEVEL, 7) = 0 THEN NULL ELSE STANDARD_HASH('p' || LEVEL, 'SHA256') END,
    CASE WHEN MOD(LEVEL, 5) = 0 THEN NULL ELSE TO_BLOB(UTL_RAW.CAST_TO_RAW('документ ' || LEVEL)) END
  FROM dual CONNECT BY LEVEL <= 5000;
COMMIT;
ALTER TABLE gate_customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE gate_orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
EXIT
""",
    changes="""
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = rivet;
INSERT INTO gate_customers (name, tier) SELECT N'новый клиент ' || LEVEL, 'free' FROM dual CONNECT BY LEVEL <= 20;
INSERT INTO gate_orders (customer_id, amount, status, placed_at, payload, doc)
  SELECT 1 + MOD(LEVEL, 1000), 9.99 * LEVEL, N'new', SYSTIMESTAMP AT TIME ZONE '-05:30', HEXTORAW('00FF10'),
    TO_BLOB(HEXTORAW('00FF10'))
  FROM dual CONNECT BY LEVEL <= 100;
UPDATE gate_orders SET status = N'отправлен', updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) WHERE id <= 30;
UPDATE gate_customers SET tier = 'vip', updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) WHERE id <= 10;
DELETE FROM gate_orders WHERE id BETWEEN 4996 AND 5000;
COMMIT;
EXIT
""",
    # Unquoted names fold upper-case: init names the exports GATE_CUSTOMERS / GATE_ORDERS (the read-back is compared
    # by lower-case column name).
    tables={
        "GATE_CUSTOMERS": ["id", "name", "tier", "updated_at"],
        "GATE_ORDERS": ["id", "customer_id", "amount", "status", "placed_at", "payload", "doc", "updated_at"],
    },
    kinds=("batch",),
    limits={
        "cdc": Limit(
            "no-load",
            "",
            "docs/reference/oracle.md (Known limits) and cdc.md: an Oracle CDC export cannot feed a `load:` block",
        )
    },
    reader="oracle",
    # The instant with its offset as text, parsed here (python-oracledb hands a zoned timestamp back without its zone).
    select={"placed_at": """TO_CHAR(placed_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM')"""},
    json_kinds={"placed_at": "ts"},
)
ENGINES = {e.name: e for e in (MYSQL, MSSQL, MONGODB, ORACLE)}
#: Keys an incremental extract cannot see deleted, left in the batch target after each cycle.
DELETED_BY_BATCH = {"initial": {}, "delta": {"gate_orders": 5}, "idle": {"gate_orders": 5}}


def deleted_by_batch(cycle: str, table: str) -> int:
    """Keys of `table` the batch target keeps after `cycle` although the source deleted them (Oracle's names are
    upper-case)."""
    return DELETED_BY_BATCH.get(cycle, {}).get(table.lower(), 0)


def init_argv(kind: str, engine: Engine, bucket: str, project: str, dataset: str, out: str) -> list[str]:
    """`rivet init` for one config: batch (`incremental`) or the CDC stream, GCS and a BigQuery `load:` block, over the
    scenario's tables (`--include`: a database holds other tables, SQL Server's CDC its own `dbo.systranschemas`)."""
    mode = "cdc" if kind == "cdc" else "incremental"
    return [
        "rivet", "init", "--source-env", engine.url_env, "--tls", engine.tls, "--mode", mode, "--gcs-bucket", bucket,
        "--bigquery-project", project, "--bigquery-dataset", dataset, "--include", *engine.tables, "-o", out,
    ]  # fmt: skip


def read_config(text: str, tables: Sequence[str]) -> dict[str, Any]:
    """What the gate takes from a generated config: the batch exports of the scenario's tables, the CDC streams over
    them (one over every table, or one per table where the engine captures per table; each table as init wrote it,
    maybe schema-qualified), and any other export."""
    doc = yaml.safe_load(text) or {}
    batch, streams, extra = [], [], []
    for e in doc.get("exports") or []:
        if e.get("mode") == "cdc":
            over = [str(t) for t in e.get("tables") or [e.get("table")]]
            if {short(t) for t in over} <= set(tables):
                streams.append({"name": e["name"], "tables": over})
            else:
                extra.append(e["name"])
        elif e["name"] in tables:
            batch.append(e["name"])
        else:
            extra.append(e["name"])
    for stream in streams:
        stream["dag"] = "_cdc" if len(streams) == 1 else f"_cdc_{stream['name']}"
    return {"exports": batch, "streams": streams, "extra": extra}


class SourceLeg:
    """One engine of one stack: its bucket, datasets, init-made configs, its DAGs (batch, one per CDC stream), and their oracles."""

    def __init__(self, cloud: warehouse.Cloud, stack: str, engine: Engine) -> None:
        self.cloud, self.stack, self.engine = cloud, stack, engine
        tag = f"{cloud.tag}-{stack.replace('.', '')}"
        # A bucket per config: init's prefixes are fixed, and SQL Server's CDC exports write under `exports/<table>/cdc/`,
        # inside the batch export's `exports/<table>/`, which `rivet load` lists recursively as its own runs.
        self.buckets = {k: f"rivet-afgate-{tag}-{engine.name}-{k}" for k in ("batch", "cdc")}
        self.datasets = {k: f"{cloud.dataset(stack)}_{engine.name}_{k}" for k in ("batch", "cdc")}
        self.dags: list[str] = []  # the DAGs init's configs make (one CDC DAG per stream), known after `prepare`
        self.shape: dict[str, dict[str, Any]] = {}
        self.tasks: dict[str, list[str]] = {}
        self.may_skip: dict[str, tuple[str, ...]] = {}
        self.targets: dict[str, dict[str, str]] = {}
        #: Per kind the engine cannot run: what is wrong with init's answer (empty: the known limit holds).
        self.limits: dict[str, list[str]] = {}
        self._bq: Any = None

    def _env(self, stand: Any) -> tuple[list[str], dict[str, str]]:
        """`exec -e NAME` and its value: the root password the client reads (none where the client logs in by the OS)."""
        if not self.engine.client_env:
            return [], {}
        return ["-e", self.engine.client_env], {self.engine.client_env: self.engine.root_password(stand.secrets)}

    def sql(self, stand: Any, text: str) -> str:
        """Statements through the engine's client inside its container."""
        flags, env = self._env(stand)
        return stand.compose(
            "exec", "-T", *flags, self.engine.service, *self.engine.client, input=self.engine.prefix + text, extra_env=env
        ).stdout

    def wait_ready(self, stand: Any) -> None:
        flags, env = self._env(stand)
        timeout = self.engine.ready_timeout
        deadline = time.monotonic() + timeout
        while True:
            done = stand.compose("exec", "-T", *flags, self.engine.service, *self.engine.ready, check=False, extra_env=env)
            # The entrypoint answers once on a temporary server before the real one starts: ask the SQL client too.
            if done.returncode == 0 and self.sql_ok(stand):
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"{self.engine.service} not ready after {timeout:.0f} s")
            time.sleep(3)

    def sql_ok(self, stand: Any) -> bool:
        try:
            return "1" in self.sql(stand, self.engine.ping)
        except RuntimeError:
            return False

    def prepare(self, stand: Any) -> None:
        """Seed the source; the buckets and datasets; `rivet init` writes each config inside the stack's network (and,
        for a kind the engine cannot run, shows its known limit)."""
        for kind in self.datasets:
            (stand.rivet_dir / "state" / f"{self.engine.name}_{kind}").mkdir(parents=True, exist_ok=True)
        self.wait_ready(stand)
        self.sql(stand, self.engine.setup.replace("{password}", getattr(stand.secrets, self.engine.name)))
        self.sql(stand, self.engine.seed.replace("{orders_instance}", self.capture_instance(stand)))
        for kind, ds in self.datasets.items():
            sh(["gcloud", "storage", "buckets", "create", f"gs://{self.buckets[kind]}", f"--project={self.cloud.project}",
                f"--location={self.cloud.location}", "--uniform-bucket-level-access"], timeout=300)  # fmt: skip
            stand.psql("gate-state", "rivet_state", f"CREATE DATABASE {state_db(self.engine.name, kind)};")
            warehouse._bq(f"--location={self.cloud.location}", "mk", "-f", "--dataset", f"{self.cloud.project}:{ds}")
            host = stand.rivet_dir / "configs" / f"{self.engine.name}_{kind}"
            host.mkdir(parents=True, exist_ok=True)
            out = f"/opt/rivet/configs/{self.engine.name}_{kind}/rivet.yaml"
            argv = init_argv(kind, self.engine, self.buckets[kind], self.cloud.project, ds, out)
            limit = self.engine.limits.get(kind)
            done = stand.compose(
                "exec", "-T", "-e", self.engine.url_env, "-e", "RIVET_STATE_URL", "airflow-worker", *argv,
                extra_env={
                    self.engine.url_env: self.engine.url(stand.secrets),
                    "RIVET_STATE_URL": stand.state_url(state_db(self.engine.name, kind)),
                },
                check=limit is None,
            )  # fmt: skip
            # init records the source's primary keys (`pk: auto` reads them for a `query:` export) in the state it opens:
            # the config's PostgreSQL state, the one its DAG's runs read, so nothing is left beside the config.
            left = sorted(p.name for p in host.glob(".rivet_state.db*"))
            if left:
                raise RuntimeError(f"init left {left} beside {host} although RIVET_STATE_URL named the PostgreSQL state")
            written = host / "rivet.yaml"
            text = written.read_text() if written.exists() else None
            if limit is not None:
                said = ((done.stdout or "") + (done.stderr or "")).strip()
                print(
                    f"LIMIT {self.engine.name} {kind}: {' '.join(argv)} -> exit {done.returncode}\n{said}\n{text or ''}",
                    flush=True,
                )
                self.limits[kind] = limit_holds(limit, done.returncode, said, text, list(self.engine.tables))
                continue
            print(f"INIT {self.engine.name} {kind} config:\n{text}", flush=True)
            self.shape[kind] = read_config(text or "", list(self.engine.tables))
        batch = self.shape.get("batch", {"exports": [], "streams": [], "extra": []})
        cdc = self.shape.get("cdc", {"exports": [], "streams": [], "extra": []})
        tables = sorted(self.engine.tables)
        captured = sorted(short(t) for s in cdc["streams"] for t in s["tables"])
        extra = batch["extra"] + cdc["extra"]
        want_batch = tables if "batch" in self.engine.kinds else []
        want_cdc = tables if "cdc" in self.engine.kinds else []
        if sorted(batch["exports"]) != want_batch or captured != want_cdc or extra:
            raise ValueError(f"init wrote batch exports {batch['exports']}, streams {cdc['streams']}, others {extra}")
        # Where each scenario table lands: a batch export loads under its name, a stream's table under its own.
        self.targets = {
            "batch": {e: e for e in batch["exports"]},
            "cdc": {short(t): warehouse_table(t) for s in cdc["streams"] for t in s["tables"]},
        }
        prefix = f"gate_src_{self.engine.name}"
        self.tasks = {}
        if batch["exports"]:
            self.tasks[f"{prefix}_batch"] = [f"{g}.{e}" for e in batch["exports"] for g in ("apply", "load", "compact")]
        for stream in cdc["streams"]:
            self.tasks[prefix + stream["dag"]] = ["run", *(f"{g}.{t}" for t in stream["tables"] for g in ("load", "compact"))]
        self.dags = list(self.tasks)
        # A compaction with nothing to merge skips; the `compacted` cell holds every cycle to "no buffer left".
        self.may_skip = {dag: ("compact.",) for dag in self.dags}
        # The DAG file reads which exports and stream to build from here (the names are init's, not the gate's).
        (stand.rivet_dir / "configs" / f"{self.engine.name}_shape.json").write_text(json.dumps(self.shape))

    def capture_instance(self, stand: Any) -> str:
        """The capture instance of the engine's renamed table: its own name when this rivet's init reads the catalog."""
        if self.engine.capture_instance is None:
            return ""
        table, name, since = self.engine.capture_instance
        shown = stand.compose("exec", "-T", "airflow-worker", "rivet", "--version").stdout
        if rivet_version(shown) >= since:
            return name
        default = f"dbo_{table}"
        print(f"CAPTURE {self.engine.name} {table}: {default} ({shown.strip()} writes the default name)", flush=True)
        return default

    def apply_changes(self, stand: Any) -> None:
        self.sql(stand, self.engine.changes)

    def cleanup(self) -> None:
        """Remove both buckets and both datasets, then look again: anything left raises."""
        self._bq = None
        for bucket in self.buckets.values():
            sh(["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{bucket}"], timeout=600, check=False)
        for ds in self.datasets.values():
            warehouse._bq("rm", "-r", "-f", "--dataset", f"{self.cloud.project}:{ds}", check=False)
        left = self.leftovers()
        if left:
            raise RuntimeError(f"cloud cleanup left behind {left}")

    def leftovers(self) -> list[str]:
        left = []
        for ds in self.datasets.values():
            shown = sh(["bq", "--quiet", "--headless", "show", "--format=json", f"{self.cloud.project}:{ds}"], check=False)
            if shown.returncode == 0:
                left.append(f"bq:{ds}")
            elif "Not found" not in (shown.stdout + shown.stderr):
                raise RuntimeError(f"cannot tell whether {ds} is gone: {(shown.stderr or shown.stdout).strip()[-500:]}")
        for bucket in self.buckets.values():
            listed = sh(["gcloud", "storage", "buckets", "describe", f"gs://{bucket}"], timeout=300, check=False)
            if listed.returncode == 0:
                left.append(f"gcs:{bucket}")
            elif "404" not in listed.stderr and "not found" not in listed.stderr.lower():
                raise RuntimeError(f"cannot tell whether {bucket} is gone: {listed.stderr.strip()[-500:]}")
        return left

    def _client(self) -> Any:
        if self._bq is None:
            self._bq = warehouse._bq_client(self.cloud)
        return self._bq

    def objects(self, kind: str) -> set[str]:
        return {t.table_id.lower() for t in self._client().list_tables(f"{self.cloud.project}.{self.datasets[kind]}")}

    def read(self, kind: str, table: str) -> list[dict[str, Any]]:
        sql = f"SELECT * FROM `{self.cloud.project}.{self.datasets[kind]}.{table}`"
        rows = [dict(row.items()) for row in self._client().query(sql).result()]
        if self.engine.reader == "mongo":
            # The document lands as BigQuery JSON: read back as extended JSON by pymongo's own parser.
            rows = [{**r, "document": None if r.get("document") is None else warehouse_document(r["document"])} for r in rows]
        return rows

    def source_rows(self, stand: Any) -> dict[str, list[dict[str, Any]]]:
        """Every source row, read by a client rivet does not use: DuckDB's scanner, the engine's own SQL client,
        python-oracledb or pymongo."""
        if self.engine.reader == "oracle":
            return read_oracle(self.engine, stand.host_port(self.engine.service, self.engine.port), stand.secrets.oracle)
        if self.engine.reader == "mongo":
            return read_mongo(self.engine, stand.host_port(self.engine.service, self.engine.port), stand.secrets.mongodb)
        if not self.engine.duck_type:
            return {
                table: parse_json_rows(self.sql(stand, self.engine.json_select.format(cols=", ".join(cols), table=table)),
                                       cols, self.engine.json_kinds)
                for table, cols in self.engine.tables.items()
            }  # fmt: skip
        port = stand.host_port(self.engine.service, self.engine.port)
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'")
            con.execute(f"INSTALL {self.engine.duck_type}; LOAD {self.engine.duck_type}")
            dsn = self.engine.duck_dsn.format(port=port, password=getattr(stand.secrets, self.engine.name))
            con.execute(f"ATTACH '{dsn}' AS src (TYPE {self.engine.duck_type}, READ_ONLY)")
            out = {}
            for table, cols in self.engine.tables.items():
                cur = con.execute(f"SELECT {', '.join(cols)} FROM src.{table}")
                names = [d[0] for d in cur.description]
                out[table] = [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
            return out
        finally:
            con.close()

    def grade(self, stand: Any, v: Any, name: str) -> None:
        """Batch: each compacted base equals the source, with only the keys the cycle deleted extra. CDC: each base
        without the rows flagged deleted equals the source. Both: no buffer is left after `compact`. A kind the engine
        cannot run: init still answers with the known limit (graded once, on the first cycle)."""
        if name == "initial":
            for kind, problems in sorted(self.limits.items()):
                limit = self.engine.limits[kind]
                v.check(
                    f"{name}/{self.engine.name}/{kind}/known-limit", lambda p=problems: p, ok=f"{limit.shape}: {limit.documented}"
                )
        source = v.guard(f"{name}/{self.engine.name}/source", self.source_rows, stand)
        if source is None:
            return
        for kind in self.engine.kinds:
            objects = v.guard(f"{name}/{self.engine.name}/{kind}/objects", self.objects, kind)
            print(f"INVENTORY {name} {self.engine.name} {kind} {sorted(objects or [])}", flush=True)
            for table, cols in self.engine.tables.items():
                cell = f"{name}/{self.engine.name}/bigquery/{kind}/{table}"
                v.check(
                    cell + "/current-equals-source",
                    self._compare(kind, table, source[table], cols, 0 if kind == "cdc" else deleted_by_batch(name, table)),
                    ok=f"{len(source[table])} row(s)",
                )
                if objects is not None:
                    v.check(cell + "/compacted", warehouse.compacted, objects, self.targets[kind][table], cell)

    def _compare(
        self, kind: str, table: str, source: list[dict[str, Any]], cols: list[str], deleted: int
    ) -> Callable[[], list[str]]:
        target = self.targets[kind][table]
        what = f"bigquery {self.datasets[kind]}.{target}"
        return lambda: warehouse.current_state(
            self.read(kind, target), source, cols, self.engine.key, deleted, what, flagged=kind == "cdc"
        )


def short(table: str) -> str:
    """`dbo.gate_orders` -> `gate_orders`: the scenario's name of a table init wrote schema-qualified."""
    return table.rsplit(".", 1)[-1]


def warehouse_table(table: str) -> str:
    """The BigQuery table rivet loads a source table into: a schema-qualified one is folded, `dbo.t` -> `dbo_t`
    (rivet's `warehouse_table_name`, src/load/plan.rs)."""
    return table.replace(".", "_")


def rivet_version(shown: str) -> tuple[int, ...]:
    """`rivet 0.31.0 (abc)` -> (0, 31, 0)."""
    found = re.search(r"\brivet (\d+)\.(\d+)\.(\d+)", shown)
    if not found:
        raise ValueError(f"no rivet version in {shown!r}")
    return tuple(int(x) for x in found.groups())


FRACTION = re.compile(r"(\.\d{6})\d+")


def parse_json_rows(text: str, cols: Sequence[str], kinds: dict[str, str]) -> list[dict[str, Any]]:
    """Rows of `FOR JSON PATH, INCLUDE_NULL_VALUES` as Python values (sqlcmd cuts the document into lines).

    An instant keeps microseconds, cut, not rounded (rivet truncates `datetime2(7)`); varbinary arrives as base64."""
    lines = text.splitlines()
    # sqlcmd heads the document with the column's name and a rule (`-h -1` cannot go with `-y 0`, which keeps it whole),
    # and cuts it every 2033 characters: the pieces join as they are (a space at a cut is the data's).
    start = next((i for i, line in enumerate(lines) if line.startswith("[")), len(lines))
    body = "".join(lines[start:]).strip()
    rows = json.loads(body, parse_float=Decimal) if body else []
    out = []
    for row in rows:
        missing = [c for c in cols if c not in row]
        if missing:
            raise KeyError(f"no column(s) {missing} in the source JSON (has {sorted(row)})")
        out.append({c: _typed(kinds.get(c), row[c]) for c in cols})
    return out


def _typed(kind: str | None, value: Any) -> Any:
    if value is None:
        return None
    if kind == "ts":
        return datetime.fromisoformat(FRACTION.sub(r"\1", str(value)).replace("Z", "+00:00"))
    if kind == "uuid":
        return uuid.UUID(str(value))
    if kind == "bytes":
        return base64.b64decode(value)
    if kind == "money":
        return Decimal(str(value))
    return value


def limit_holds(limit: Limit, code: int, said: str, text: str | None, tables: Sequence[str]) -> list[str]:
    """Whether init's answer for a config kind the leg cannot run is still the documented limit."""
    if limit.shape == "refused":
        problems = [] if code != 0 else [f"init exited 0 (it writes this config now: run it in the leg)\n{said[-1500:]}"]
        if f"[{limit.code}]" not in said:
            problems.append(f"init's answer does not carry [{limit.code}]: {said[-1500:]}")
        if text is not None:
            problems.append("init wrote a config although it refused")
        return problems
    if code != 0 or text is None:
        return [f"init exited {code} without a config: {said[-1500:]}"]
    doc = yaml.safe_load(text) or {}
    problems = []
    if "load" in doc or any("load" in e for e in doc.get("exports") or []):
        problems.append("init writes a `load:` block now: load and compact this kind in the leg")
    over = sorted(short(str(e.get("table") or "")).lower() for e in doc.get("exports") or [] if e.get("mode") == "cdc")
    if over != sorted(t.lower() for t in tables):
        problems.append(f"init's CDC exports read {over}, not the scenario's tables {sorted(tables)}")
    return problems


def read_oracle(engine: Engine, port: int, password: str) -> dict[str, list[dict[str, Any]]]:
    """Every source row through python-oracledb (thin): exact decimals, LOBs as bytes, a zoned instant as its text."""
    import oracledb

    oracledb.defaults.fetch_decimals = True
    oracledb.defaults.fetch_lobs = False
    out = {}
    with oracledb.connect(user="rivet", password=password, host="127.0.0.1", port=port, service_name=engine.database) as con:
        for table, cols in engine.tables.items():
            exprs = ", ".join(engine.select.get(c, c) for c in cols)
            with con.cursor() as cur:
                cur.execute(f"SELECT {exprs} FROM rivet.{table}")
                out[table] = [{c: _typed(engine.json_kinds.get(c), v) for c, v in zip(cols, row, strict=True)} for row in cur]
    return out


def read_mongo(engine: Engine, port: int, password: str) -> dict[str, list[dict[str, Any]]]:
    """Every document through pymongo, as the `_id` text rivet keys a row by and the document in one canonical form."""
    from pymongo import MongoClient

    client = MongoClient(
        host="127.0.0.1", port=port, username="rivet", password=password, authSource=engine.database, directConnection=True
    )
    try:
        db = client[engine.database]
        return {
            table: [{"_id": mongo_id(d["_id"]), "document": mongo_canonical(d)} for d in db[table].find()]
            for table in engine.tables
        }
    finally:
        client.close()


def mongo_id(value: Any) -> str:
    """The `_id` text rivet writes beside the document: an ObjectId's hex, an integer's digits, a string as it is."""
    from bson import ObjectId

    if isinstance(value, ObjectId | str) or (isinstance(value, int) and not isinstance(value, bool)):
        return str(value)
    raise TypeError(f"no _id text for {type(value).__name__} {value!r}")


def _mongo_value(value: Any) -> Any:
    from bson import Decimal128, ObjectId

    # JSON text keeps an integer and a double apart (`2` and `2.0`), a double at its shortest round-trip digits.
    if value is None or isinstance(value, bool | str | int | float):
        return value
    if isinstance(value, Decimal128):
        return {"$numberDecimal": str(value)}
    if isinstance(value, ObjectId):
        return {"$oid": str(value)}
    if isinstance(value, datetime):
        stamp = value if value.tzinfo else value.replace(tzinfo=UTC)
        return {"$date": (stamp - warehouse.EPOCH) // timedelta(milliseconds=1)}
    if isinstance(value, bytes):
        return {"$binary": value.hex(), "subtype": getattr(value, "subtype", 0)}
    if isinstance(value, dict):
        return {str(k): _mongo_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_mongo_value(v) for v in value]
    raise TypeError(f"no canonical form for {type(value).__name__} {value!r}")


def mongo_canonical(doc: Any) -> str:
    """A BSON document as one text: every type tagged (ObjectId, Decimal128 with its exact digits, a double apart from
    an integer, a date at its millisecond, binary with its subtype), keys sorted, so two readers' equal documents give
    equal text and any difference of type or value does not."""
    return json.dumps(_mongo_value(doc), sort_keys=True, ensure_ascii=False)


def warehouse_document(value: Any) -> str:
    """The `document` BigQuery returns (JSON, parsed by the client, or its text) read as extended JSON by pymongo."""
    from bson import json_util

    return mongo_canonical(json_util.loads(value if isinstance(value, str) else json.dumps(value)))


def sweep(cloud: warehouse.Cloud) -> list[str]:
    """Buckets of the gate's shape older than a day (the datasets go with the warehouse sweep)."""
    out = sh(["gcloud", "storage", "buckets", "list", f"--project={cloud.project}", "--format=json(name,creation_time)"],
             timeout=300, check=False)  # fmt: skip
    removed = []
    for b in json.loads(out.stdout or "[]") if out.returncode == 0 else []:
        name = str(b.get("name", ""))
        if BUCKET_SHAPE.fullmatch(name) and warehouse._age_hours(b.get("creation_time")) > warehouse.SWEEP_AGE_HOURS:
            sh(["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{name}"], timeout=600, check=False)
            removed.append(f"gcs-bucket:{name}")
    return removed


def legs(cloud: warehouse.Cloud | None, stack: str, names: Sequence[str]) -> list[SourceLeg]:
    return [SourceLeg(cloud, stack, ENGINES[n]) for n in names] if cloud else []


def engines(names: Sequence[str]) -> tuple[Engine, ...]:
    return tuple(ENGINES[n] for n in names)
