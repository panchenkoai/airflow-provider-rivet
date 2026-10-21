"""Source legs: a database engine beside the postgres source, extracted to GCS and loaded and compacted into BigQuery
through Airflow. Every rivet config is the one `rivet init` writes against the live source, used as written: the gate
gives init a bucket and datasets of the run's own, and reads export names, prefixes and the checkpoint from what it
wrote (nothing is edited). The warehouse is read back by a client the loader does not use and compared with the source.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import yaml

from . import warehouse
from .stand import COMPOSE_STATE_ROLE, METASTORE_DB, sh, state_db

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
class HotDdl:
    """A column added to a captured table while a CDC run is open, and the writes on either side of it.

    `before` commits before the run opens (inside its read bound); the run pauses right after it opened
    (`cdc_after_open`), and inside that window the gate runs `alter` and then `after`, which uses the new column. init's
    stream is bounded (`until_current: true`): the open run reads only `before`, under the columns it resolves after the
    pause, and the next run reads `after`. Each `ops_*` is the change events of the scenario's tables."""

    table: str
    column: str
    before: str
    alter: str
    after: str
    ops_before: dict[str, dict[str, int]]
    ops_after: dict[str, dict[str, int]]
    #: Keys `after` deletes: an incremental extract cannot see them, so the batch target keeps them.
    deleted: int
    pause_ms: int = 45000


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
    #: The change events each cycle's `changes` make, per table and operation; with it, every rivet task of the leg's
    #: DAGs has its XCom unit and task log graded against the counts the gate made (not rivet's own report).
    ops: dict[str, dict[str, dict[str, int]]] = field(default_factory=dict)
    #: The cursor column of the batch exports (the rows an incremental extract reads are the ones past its high-water).
    cursor: str = "updated_at"
    #: A column added mid-stream, after the cycles (the MySQL client stack's schema change).
    hot: HotDdl | None = None
    #: After the hot ALTER: a second CDC config on the first stream's checkpoint must be refused, and these changes
    #: (with their events) must then reach the first stream intact.
    shared: tuple[str, dict[str, dict[str, int]]] | None = None
    #: The stream's config read through a git-sync checkout (`repo -> rev-A`), flipped to `rev-B` after the other phases;
    #: then these changes (with their events) must reach the stream intact.
    git_sync: tuple[str, dict[str, dict[str, int]]] | None = None
    #: After the worker container is recreated: these changes (with their events) must reach the stream intact.
    recreate: tuple[str, dict[str, dict[str, int]]] | None = None

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
    ops={
        "delta": {"gate_customers": {"insert": 20, "update": 10}, "gate_orders": {"insert": 100, "update": 30, "delete": 5}},
    },
    # A nullable column (MySQL fills no old row, and the binlog carries none), added at the end (ALGORITHM=INSTANT on
    # 8.4); `after` sets it on old rows, on a row `before` changed, on new rows (one left NULL), and deletes one row.
    hot=HotDdl(
        table="gate_orders",
        column="note",
        before="UPDATE gate_orders SET status = 'packed' WHERE id BETWEEN 31 AND 40;",
        alter="ALTER TABLE gate_orders ADD COLUMN note VARCHAR(40) NULL;",
        after="""
UPDATE gate_orders SET status = 'held', note = CONCAT('примітка ', id) WHERE id BETWEEN 41 AND 50;
INSERT INTO gate_orders (customer_id, amount, status, note) VALUES (1, 12.34, 'new', 'нова'), (2, 0.01, 'new', NULL);
UPDATE gate_orders SET note = 'тільки примітка' WHERE id = 31;
DELETE FROM gate_orders WHERE id = 100;
""",
        ops_before={"gate_customers": {}, "gate_orders": {"update": 10}},
        ops_after={"gate_customers": {}, "gate_orders": {"update": 11, "insert": 2, "delete": 1}},
        deleted=1,
    ),
    shared=("UPDATE gate_customers SET tier = 'gold' WHERE id <= 3;", {"gate_customers": {"update": 3}, "gate_orders": {}}),
    git_sync=("UPDATE gate_customers SET tier = 'synced' WHERE id BETWEEN 4 AND 6;", {"gate_customers": {"update": 3}}),
    recreate=(
        "UPDATE gate_orders SET status = 'recreated' WHERE id BETWEEN 51 AND 55;\n"
        "INSERT INTO gate_orders (customer_id, amount, status, note) VALUES (3, 1.23, 'new', 'після');\n"
        "DELETE FROM gate_orders WHERE id = 102;",
        {"gate_orders": {"update": 5, "insert": 1, "delete": 1}},
    ),
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
        #: The configs init wrote, by kind, as written.
        self.texts: dict[str, str] = {}
        #: Each table's cursor high-water after the last graded batch run (what the next extract reads past).
        self.high_water: dict[str, Any] = {}
        #: The DAG of the stream whose run pauses after it opened (the hot ALTER), when the engine has one.
        self.hot_dag = ""
        #: The config copy a previous provider left in the stream's state_dir (git-sync leg), placed before the cycles.
        self.stale_copy: Path | None = None
        #: The second CDC config on the first stream's checkpoint: init's answer, and its DAG when init wrote it.
        self.shared: dict[str, Any] = {}
        self.shared_dag = ""
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
            stand.make_leg_state_db(state_db(self.engine.name, kind))
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
                    "RIVET_STATE_URL": stand.leg_state_url(state_db(self.engine.name, kind)),
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
            self.texts[kind] = text or ""
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
        if self.engine.hot is not None:
            # The same stream, config and state, in a DAG whose run pauses after it opened (run after the cycles only).
            if len(cdc["streams"]) != 1:
                raise ValueError(f"the hot ALTER needs one stream over the tables, init wrote {cdc['streams']}")
            stream = cdc["streams"][0]
            self.hot_dag = f"{prefix}{stream['dag']}_hot"
            self.tasks[self.hot_dag] = self.tasks[prefix + stream["dag"]]
            self.shape["hot"] = {
                "dag": self.hot_dag,
                "stream": stream,
                "pause": f"cdc_after_open:{self.engine.hot.pause_ms}",
                "marker": f"/opt/rivet/markers/{self.engine.name}_hot",
            }
            if self.engine.shared is not None:
                self.prepare_shared(stand, f"{prefix}_cdc_shared")
        if self.engine.git_sync is not None:
            # The stream's DAGs read init's config through a git-sync checkout from the first cycle on.
            self.shape["cdc_config"] = git_sync_layout(
                stand.rivet_dir / "configs" / f"{self.engine.name}_cdc_sync", self.texts["cdc"]
            )
            # What the previous provider left in the state_dir: its copy of the config, named by the RESOLVED path (the
            # revision's), with the same text and no `.owner` record (rivet <= 0.32.1 wrote none).
            resolved = self.shape["cdc_config"].replace("/repo/", "/rev-A/")
            state_dir = stand.rivet_dir / "state" / f"{self.engine.name}_cdc"
            self.stale_copy = state_dir / old_copy_name(resolved)
            self.stale_copy.write_text(self.texts["cdc"])
        # A compaction with nothing to merge skips; the `compacted` cell holds every cycle to "no buffer left".
        self.may_skip = {dag: ("compact.",) for dag in self.tasks}
        # The DAG file reads which exports and stream to build from here (the names are init's, not the gate's).
        (stand.rivet_dir / "configs" / f"{self.engine.name}_shape.json").write_text(json.dumps(self.shape))

    def prepare_shared(self, stand: Any, dag: str) -> None:
        """A second CDC config of the same source, written by init in a directory of its own (so no sibling names its
        checkpoint), whose DAG runs in the first stream's `state_dir`: the operator copies each config there, so init's
        relative `cdc.checkpoint` of both resolves to one file. init may refuse it (`RIVET_INIT_CDC_NAME_TAKEN`)."""
        eng = self.engine.name
        host = stand.rivet_dir / "configs" / f"{eng}_cdc_shared"
        host.mkdir(parents=True, exist_ok=True)
        out = f"/opt/rivet/configs/{eng}_cdc_shared/rivet.yaml"
        argv = init_argv("cdc", self.engine, self.buckets["cdc"], self.cloud.project, self.datasets["cdc"], out)
        done = stand.compose(
            "exec", "-T", "-e", self.engine.url_env, "-e", "RIVET_STATE_URL", "airflow-worker", *argv,
            extra_env={
                self.engine.url_env: self.engine.url(stand.secrets),
                "RIVET_STATE_URL": stand.leg_state_url(state_db(eng, "cdc")),
            },
            check=False,
        )  # fmt: skip
        said = ((done.stdout or "") + (done.stderr or "")).strip()
        written = host / "rivet.yaml"
        text = written.read_text() if written.exists() else None
        print(f"INIT {eng} cdc (second config): {' '.join(argv)} -> exit {done.returncode}\n{said}\n{text or ''}", flush=True)
        self.shared = {"exit": done.returncode, "said": said, "text": text}
        if done.returncode != 0 or text is None:
            return
        streams = read_config(text, list(self.engine.tables))["streams"]
        if len(streams) != 1:
            raise ValueError(f"the second CDC config holds streams {streams}, expected one")
        self.shared_dag = dag
        self.tasks[dag] = ["run", *(f"{g}.{t}" for t in streams[0]["tables"] for g in ("load", "compact"))]
        self.shape["shared"] = {"dag": dag, "config": out, "stream": streams[0]}

    def cycle_tasks(self) -> dict[str, list[str]]:
        """The rivet tasks of the DAGs every cycle runs (not the hot-ALTER DAG, which runs once after the cycles)."""
        return {dag: self.tasks[dag] for dag in self.dags}

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

    def source_rows(self, stand: Any, added: dict[str, list[str]] | None = None) -> dict[str, list[dict[str, Any]]]:
        """Every source row, read by a client rivet does not use: DuckDB's scanner, the engine's own SQL client,
        python-oracledb or pymongo (`added`: columns beyond the scenario's, per table, through DuckDB)."""
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
                cur = con.execute(f"SELECT {', '.join([*cols, *(added or {}).get(table, [])])} FROM src.{table}")
                names = [d[0] for d in cur.description]
                out[table] = [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
            return out
        finally:
            con.close()

    def grade(self, stand: Any, v: Any, name: str, run_id: str = "") -> None:
        """Batch: each compacted base equals the source, with only the keys the cycle deleted extra. CDC: each base
        without the rows flagged deleted equals the source. Both: no buffer is left after `compact`. A kind the engine
        cannot run: init still answers with the known limit (graded once, on the first cycle). An engine whose changes
        the gate counts (`ops`): init's `load:` block is the client's, every rivet task's XCom unit and task log carry
        the rows the gate made, and no staged Parquet is left after the load."""
        if name == "initial":
            for kind, problems in sorted(self.limits.items()):
                limit = self.engine.limits[kind]
                v.check(
                    f"{name}/{self.engine.name}/{kind}/known-limit", lambda p=problems: p, ok=f"{limit.shape}: {limit.documented}"
                )
            if self.engine.ops:
                for kind, text in sorted(self.texts.items()):
                    v.check(
                        f"{name}/{self.engine.name}/{kind}/load-block-as-written",
                        load_block_problems,
                        text,
                        ok=", ".join(f"{k}: {w}" for k, w in CLIENT_LOAD.items()),
                    )
        source = v.guard(f"{name}/{self.engine.name}/source", self.source_rows, stand)
        if source is None:
            return
        deleted = {"batch": {t: deleted_by_batch(name, t) for t in self.engine.tables}}
        self.grade_targets(v, name, source, self.engine.kinds, deleted)
        if name == "initial" and getattr(stand, "state_place", "gate") == "compose-postgres":
            v.check(
                f"compose-pg/{self.engine.name}/state-in-own-database",
                lambda: placement_problems(*self.placement(stand)),
                ok=f"{', '.join(state_db(self.engine.name, k) for k in self.engine.kinds)} owned by {COMPOSE_STATE_ROLE}, "
                f"nothing of rivet's in {METASTORE_DB}",
            )
        if self.stale_copy is not None and name == "initial" and run_id:
            dag = next(d for d in self.dags if d.endswith("_cdc"))
            v.check(
                f"git-sync/{self.engine.name}/cdc/stale-copy-of-old-provider-not-refused",
                lambda: stale_copy_problems(
                    stand.run_state(dag, run_id), stand.xcom(dag, run_id, "run"), self.stale_copy.exists()
                ),
                ok=f"{self.stale_copy.name} in the state_dir, the run through repo/ succeeded",
            )
        if self.engine.ops and run_id:
            ops = self.engine.ops.get(name, {})
            self.grade_tasks(stand, v, name, run_id, self.dags, source, ops, baseline=name == "initial")
            self.grade_staged(v, name, self.engine.kinds)
            self.high_water = {t: high_water(rows, self.engine.cursor) for t, rows in source.items()}

    def grade_targets(
        self,
        v: Any,
        name: str,
        source: dict[str, list[dict[str, Any]]],
        kinds: Sequence[str],
        deleted: dict[str, dict[str, int]],
        added: dict[str, list[str]] | None = None,
    ) -> None:
        """Each kind's compacted base against `source` (`added`: CDC columns beyond the scenario's, per table)."""
        for kind in kinds:
            objects = v.guard(f"{name}/{self.engine.name}/{kind}/objects", self.objects, kind)
            print(f"INVENTORY {name} {self.engine.name} {kind} {sorted(objects or [])}", flush=True)
            for table, cols in self.engine.tables.items():
                cell = f"{name}/{self.engine.name}/bigquery/{kind}/{table}"
                more = (added or {}).get(table, []) if kind == "cdc" else []
                v.check(
                    cell + "/current-equals-source",
                    self._compare(kind, table, source[table], [*cols, *more], deleted.get(kind, {}).get(table, 0)),
                    ok=f"{len(source[table])} row(s), columns {[*cols, *more]}",
                )
                if objects is not None:
                    v.check(cell + "/compacted", warehouse.compacted, objects, self.targets[kind][table], cell)

    def expected_rows(
        self, kind: str, task: str, source: dict[str, list[dict[str, Any]]], ops: dict[str, dict[str, int]], baseline: bool
    ) -> int | None:
        """The rows a rivet task moved, from what the gate did: a CDC run, the change events of the cycle (and the
        baseline on the first); a CDC load of a table, that table's; a batch extract or load, the rows past the previous
        high-water. A compaction: not counted (None)."""
        group, _, unit = task.partition(".")
        events = {t: sum(ops.get(t, {}).values()) + (len(source[t]) if baseline else 0) for t in self.engine.tables}
        if group == "run":
            return sum(events.values())
        if group == "compact":
            return None
        if kind == "batch":
            return rows_past(source[unit], self.engine.cursor, self.high_water.get(unit))
        return events.get(short(unit))

    def grade_tasks(
        self,
        stand: Any,
        v: Any,
        name: str,
        run_id: str,
        dags: Sequence[str],
        source: dict[str, list[dict[str, Any]]],
        ops: dict[str, dict[str, int]],
        baseline: bool,
    ) -> None:
        """Every rivet task of `dags` in `run_id`: its XCom unit moved the rows the gate made, and its task log's
        headline says the same (a task Airflow skipped has neither)."""
        for dag in dags:
            states = v.guard(f"{name}/{dag}/task-states", stand.tasks, dag, run_id) or {}
            for task in self.tasks[dag]:
                if states.get(task, {}).get("state") != "success":
                    continue  # the run's own cell grades a task that did not succeed (or that may skip)
                cell = f"{name}/{dag}/{task}"
                payload = v.guard(cell + "/xcom-unit", stand.xcom, dag, run_id, task)
                if payload is None:
                    continue
                print(f"XCOM {name} {dag} {task} {json.dumps(payload, default=str)}", flush=True)
                kind = "batch" if dag.endswith("_batch") else "cdc"
                rows = self.expected_rows(kind, task, source, ops, baseline)
                v.check(cell + "/xcom-unit", xcom_problems, payload, rows, ok=f"{unit_rows(payload)} row(s)")
                if xcom_problems(payload, rows):
                    print(f"STDERR {name} {dag} {task}:\n{stderr_tail(stand, payload)}", flush=True)
                text = task_log(stand, dag, run_id, task)
                v.check(cell + "/task-log", log_problems, text, payload, ok=headline(text))

    def grade_staged(self, v: Any, name: str, kinds: Sequence[str]) -> None:
        for kind in kinds:
            v.check(f"{name}/{self.engine.name}/{kind}/staged-parts-cleaned", lambda k=kind: staged_left(self.listing(k)))

    def listing(self, kind: str) -> list[str]:
        """Every object of a config's bucket."""
        out = sh(["gcloud", "storage", "ls", "-r", f"gs://{self.buckets[kind]}/**"], timeout=300, check=False)
        if out.returncode != 0 and "matched no objects" not in (out.stdout + out.stderr):
            raise RuntimeError(f"cannot list gs://{self.buckets[kind]}: {(out.stderr or out.stdout).strip()[-500:]}")
        return [line.strip() for line in out.stdout.splitlines() if line.strip().startswith("gs://")]

    def hot_phase(self, stand: Any, v: Any, wait_runs: Callable[..., dict[str, str]], grade_runs: Callable[..., Any]) -> None:
        """A column added to a captured table while the CDC run is open, after the cycles.

        `hot-ddl-open`: the writes of `before`, then the stream's run opens and pauses after it opened; in that window the
        gate reads the source (the run's read bound) and runs the ALTER and the writes that use the new column. The run
        delivers `before`, loaded and compacted: the CDC base equals the source as the gate read it in the window.
        `hot-ddl-after`: the stream and the batch export run again: the CDC base equals the source value by value with
        the new column (every value of it reached BigQuery, the rows nobody changed after the ALTER read NULL as at the
        source); the batch base, whose `query:` init wrote with the columns of its day, equals the source on those."""
        hot = self.engine.hot
        assert hot is not None
        name, eng = "hot-ddl-open", self.engine.name
        run_id = f"gate_{eng}_hot_open"
        self.sql(stand, hot.before)
        marker = stand.rivet_dir / "markers" / f"{eng}_hot"
        marker.unlink(missing_ok=True)
        stand.trigger(self.hot_dag, run_id)
        opened, done, snapshot = wait_marker(marker), 0.0, None
        if opened is not None:
            snapshot = v.guard(f"{name}/{eng}/source-in-window", self.source_rows, stand)
            self.sql(stand, hot.alter + hot.after)
            done = time.monotonic()
        v.check(
            f"{name}/{eng}/alter-inside-the-open-run",
            window_problems,
            opened,
            done,
            hot.pause_ms,
            ok=f"{done - (opened or 0):.1f} s",
        )
        states = wait_runs(stand, [self.hot_dag], run_id)
        grade_runs(stand, v, name, run_id, states, {self.hot_dag: self.tasks[self.hot_dag]}, may_skip=self.may_skip)
        if snapshot is not None:
            self.grade_targets(v, name, snapshot, ("cdc",), {})
            self.grade_tasks(stand, v, name, run_id, [self.hot_dag], snapshot, hot.ops_before, baseline=False)
            self.grade_staged(v, name, ("cdc",))
            target = self.targets["cdc"][hot.table]
            got = v.guard(f"{name}/{eng}/columns", self.read, "cdc", target)
            print(f"HOT {name}: {target} columns {sorted(got[0]) if got else []}", flush=True)

        name, run_id = "hot-ddl-after", f"gate_{eng}_hot_after"
        dags = list(self.dags)
        for dag in dags:
            stand.trigger(dag, run_id)
        states = wait_runs(stand, dags, run_id)
        grade_runs(stand, v, name, run_id, states, {d: self.tasks[d] for d in dags}, may_skip=self.may_skip)
        source = v.guard(f"{name}/{eng}/source", self.source_rows, stand, {hot.table: [hot.column]})
        if source is None:
            return
        old = {t: [{c: r[c] for c in cols} for r in source[t]] for t, cols in self.engine.tables.items()}
        gone = {t: deleted_by_batch("idle", t) + (hot.deleted if t == hot.table else 0) for t in self.engine.tables}
        self.grade_targets(v, name, old, ("batch",), {"batch": gone})
        self.grade_targets(v, name, source, ("cdc",), {}, added={hot.table: [hot.column]})
        target = self.targets["cdc"][hot.table]
        v.check(
            f"{name}/{eng}/bigquery/cdc/{hot.table}/{hot.column}-reached",
            lambda: column_reached(self.read("cdc", target), source[hot.table], hot.column, self.engine.key),
            ok=f"{sum(1 for r in source[hot.table] if r.get(hot.column) is not None)} value(s)",
        )
        self.grade_tasks(stand, v, name, run_id, dags, old, hot.ops_after, baseline=False)
        self.grade_staged(v, name, self.engine.kinds)

    def shared_phase(self, stand: Any, v: Any, wait_runs: Callable[..., dict[str, str]], grade_runs: Callable[..., Any]) -> None:
        """A second CDC config on the first stream's checkpoint (rivet #664), after the hot ALTER: init refuses to write
        it (`RIVET_INIT_CDC_NAME_TAKEN`), or its run is refused `RIVET_SOURCE_CDC_CHECKPOINT_SHARED` (exit 5) on
        Airflow's first try, retried by nobody. Then the first stream delivers the next changes intact."""
        assert self.engine.shared is not None and self.engine.hot is not None
        name, eng, hot = "shared-checkpoint", self.engine.name, self.engine.hot
        cell = f"{name}/{eng}/cdc/refused-by-code"
        if not self.shared_dag:
            v.check(cell, init_refused, self.shared, ok=f"init refused [{NAME_TAKEN}], exit {REFUSED_EXIT}, wrote nothing")
        else:
            v.check(
                f"{name}/{eng}/cdc/one-checkpoint",
                same_checkpoint,
                self.texts["cdc"],
                self.shared["text"],
                ok="both configs name one checkpoint in the shared state_dir",
            )
            run_id = f"gate_{eng}_shared"
            stand.trigger(self.shared_dag, run_id)
            state = wait_runs(stand, [self.shared_dag], run_id).get(self.shared_dag)
            tasks = v.guard(f"{name}/{eng}/cdc/task-states", stand.tasks, self.shared_dag, run_id) or {}
            payload = None
            if tasks.get("run", {}).get("state") == "failed":
                payload = v.guard(cell + "/xcom", stand.xcom, self.shared_dag, run_id, "run")
            print(
                f"SHARED run {state}: {json.dumps({t: i.get('state') for t, i in tasks.items()})} {json.dumps(payload)}",
                flush=True,
            )
            v.check(
                cell,
                lambda: ([] if state == "failed" else [f"run {state}, expected failed"]) + shared_refused(tasks, payload),
                ok=f"[{SHARED_CODE}], exit {REFUSED_EXIT}, one try",
            )
        changes, ops = self.engine.shared
        self.sql(stand, changes)
        name, run_id = "shared-checkpoint-after", f"gate_{eng}_shared_after"
        dag = next(d for d in self.dags if d.endswith("_cdc"))
        stand.trigger(dag, run_id)
        states = wait_runs(stand, [dag], run_id)
        grade_runs(stand, v, name, run_id, states, {dag: self.tasks[dag]}, may_skip=self.may_skip)
        source = v.guard(f"{name}/{eng}/source", self.source_rows, stand, {hot.table: [hot.column]})
        if source is None:
            return
        self.grade_targets(v, name, source, ("cdc",), {}, added={hot.table: [hot.column]})
        old = {t: [{c: r[c] for c in cols} for r in source[t]] for t, cols in self.engine.tables.items()}
        self.grade_tasks(stand, v, name, run_id, [dag], old, ops, baseline=False)
        self.grade_staged(v, name, ("cdc",))

    def placement(self, stand: Any) -> tuple[dict[str, str], dict[str, list[tuple[str, str]]], list[tuple[str, str]], str]:
        """Each state database's owner and tables (with their owners), and the tables of Airflow's metadata database."""
        owners, tables = {}, {}
        for kind in self.engine.kinds:
            db = state_db(self.engine.name, kind)
            owners[db] = stand.metastore_sql(
                METASTORE_DB, f"SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = '{db}';"
            ).strip()
            tables[db] = _rows(stand.metastore_sql(db, USER_TABLES))
        return owners, tables, _rows(stand.metastore_sql(METASTORE_DB, USER_TABLES)), COMPOSE_STATE_ROLE

    def recreate_phase(self, stand: Any, v: Any, wait_runs: Callable[..., dict[str, str]]) -> None:
        """The worker container is recreated (`docker compose up -d --force-recreate airflow-worker`; under Celery the
        tasks run only there). The state_dir is the bind mount `<stack>/rivet:/opt/rivet`, so it outlives the container;
        the stream's next run must continue with no gap and no duplicate."""
        assert self.engine.recreate is not None and self.engine.hot is not None
        name, eng, hot = "recreate", self.engine.name, self.engine.hot
        dag = next(d for d in self.dags if d.endswith("_cdc"))
        before = stand.compose("ps", "-q", "airflow-worker").stdout.split()
        stand.compose("up", "-d", "--force-recreate", "airflow-worker", timeout=900)
        after = stand.compose("ps", "-q", "airflow-worker").stdout.split()
        print(f"RECREATE airflow-worker {before} -> {after}", flush=True)
        changes, ops = self.engine.recreate
        self.sql(stand, changes)
        run_id = f"gate_{eng}_recreate"
        stand.trigger(dag, run_id)
        state = wait_runs(stand, [dag], run_id).get(dag)
        tasks = v.guard(f"{name}/{eng}/cdc/task-states", stand.tasks, dag, run_id) or {}
        payload = (
            v.guard(f"{name}/{eng}/cdc/xcom", stand.xcom, dag, run_id, "run")
            if tasks.get("run", {}).get("state") in ("success", "failed")
            else None
        )
        source = v.guard(f"{name}/{eng}/source", self.source_rows, stand, {hot.table: [hot.column]})
        cols = {t: [*c, *([hot.column] if t == hot.table else [])] for t, c in self.engine.tables.items()}
        events = sum(n for per in ops.values() for n in per.values())
        v.check(
            f"{name}/{eng}/cdc/continues-after-worker-recreate",
            lambda: (
                recreated(before, after)
                + run_after_flip(state, tasks, payload, events)
                + (
                    ["no source read"]
                    if source is None
                    else stream_equals_source(
                        {t: self.read("cdc", self.targets["cdc"][t]) for t in self.engine.tables}, source, cols, self.engine.key
                    )
                )
            ),
            ok=f"worker {before[:1]} -> {after[:1]}, {events} change(s) delivered once, base equals the source",
        )

    def git_sync_phase(self, stand: Any, v: Any, wait_runs: Callable[..., dict[str, str]]) -> None:
        """git-sync publishes a new revision of the DAG folder: the config, unchanged, in `rev-B`, and the `repo` link
        flipped to it atomically (the old revision removed). The stream's next run must succeed and deliver intact."""
        assert self.engine.git_sync is not None and self.engine.hot is not None
        name, eng, hot = "git-sync", self.engine.name, self.engine.hot
        dag = next(d for d in self.dags if d.endswith("_cdc"))
        copies = stand.rivet_dir / "state" / f"{eng}_cdc"
        print(f"GIT-SYNC copies before the flip: {sorted(p.name for p in copies.glob('rivet.*.yaml'))}", flush=True)
        flipped = v.guard(f"{name}/{eng}/cdc/flip", flip, stand.rivet_dir / "configs" / f"{eng}_cdc_sync")
        if flipped is None:
            return
        print(f"GIT-SYNC repo -> {flipped}", flush=True)
        changes, ops = self.engine.git_sync
        self.sql(stand, changes)
        run_id = f"gate_{eng}_git_sync"
        stand.trigger(dag, run_id)
        state = wait_runs(stand, [dag], run_id).get(dag)
        tasks = v.guard(f"{name}/{eng}/cdc/task-states", stand.tasks, dag, run_id) or {}
        payload = None
        if tasks.get("run", {}).get("state") in ("success", "failed"):
            payload = v.guard(f"{name}/{eng}/cdc/xcom", stand.xcom, dag, run_id, "run")
        print(f"GIT-SYNC copies after: {sorted(p.name for p in copies.glob('rivet.*.yaml'))}; run {state}, "
              f"{json.dumps({t: i.get('state') for t, i in tasks.items()})} {json.dumps(payload, default=str)}", flush=True)  # fmt: skip
        events = sum(n for per in ops.values() for n in per.values())
        v.check(f"{name}/{eng}/cdc/run-after-flip", run_after_flip, state, tasks, payload, events, ok=f"{events} row(s)")
        source = v.guard(f"{name}/{eng}/source", self.source_rows, stand, {hot.table: [hot.column]})
        if source is None:
            return
        cols = {t: [*c, *([hot.column] if t == hot.table else [])] for t, c in self.engine.tables.items()}
        v.check(
            f"{name}/{eng}/cdc/current-equals-source",
            lambda: stream_equals_source(
                {t: self.read("cdc", self.targets["cdc"][t]) for t in self.engine.tables}, source, cols, self.engine.key
            ),
            ok=", ".join(f"{t} {len(rows)} row(s)" for t, rows in sorted(source.items())),
        )

    def _compare(
        self, kind: str, table: str, source: list[dict[str, Any]], cols: list[str], deleted: int
    ) -> Callable[[], list[str]]:
        target = self.targets[kind][table]
        what = f"bigquery {self.datasets[kind]}.{target}"
        return lambda: warehouse.current_state(
            self.read(kind, target), source, cols, self.engine.key, deleted, what, flagged=kind == "cdc"
        )


def git_sync_layout(root: Path, text: str, name: str = "rivet.yaml") -> str:
    """A git-sync checkout of the config: `root/rev-A/<name>` holding init's text unchanged, `root/repo -> rev-A` (a
    relative link, so it resolves the same in the containers); the path the DAG reads, in the containers."""
    (root / "rev-A").mkdir(parents=True, exist_ok=True)
    (root / "rev-A" / name).write_text(text)
    (root / "repo").symlink_to("rev-A")
    return f"/opt/rivet/{root.relative_to(root.parents[1])}/repo/{name}"


USER_TABLES = (
    "SELECT tablename, tableowner FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema') ORDER BY 1;"
)


def _rows(text: str) -> list[tuple[str, str]]:
    return [tuple(line.split("|", 1)) for line in text.splitlines() if "|" in line]  # type: ignore[misc]


def placement_problems(
    owners: dict[str, str], tables: dict[str, list[tuple[str, str]]], metastore: list[tuple[str, str]], role: str
) -> list[str]:
    """rivet's state is in databases of its own, owned by its own role, its tables owned by that role; Airflow's metadata
    database holds no table of rivet's (none of the names rivet made, none owned by its role)."""
    problems = []
    names = set()
    for db, owner in sorted(owners.items()):
        if owner != role:
            problems.append(f"{db} is owned by {owner!r}, not {role}")
        held = tables.get(db, [])
        if not held:
            problems.append(f"{db} holds no table: rivet wrote nothing there, the cell proves nothing")
        foreign = sorted(t for t, o in held if o != role)
        if foreign:
            problems.append(f"{db}: tables not owned by {role}: {foreign}")
        names |= {t for t, _ in held}
    leaked = sorted(t for t, o in metastore if t in names or o == role)
    if leaked:
        problems.append(f"Airflow's metadata database holds rivet's tables: {leaked}")
    return problems


def recreated(before: Sequence[str], after: Sequence[str]) -> list[str]:
    """The worker container was replaced: one running before and after, with a new id."""
    if len(before) != 1 or len(after) != 1:
        return [f"worker containers before {before}, after {after}: expected one each"]
    return [f"the worker container {before[0][:12]} was not recreated"] if before == after else []


def old_copy_name(resolved: str, name: str = "rivet.yaml") -> str:
    """The name the previous provider gave a config's copy: its stem and the first 8 hex digits of the sha256 of the
    symlink-RESOLVED path (`copy_name(Path(config).resolve())`)."""
    stem, _, suffix = name.rpartition(".")
    return f"{stem}.{hashlib.sha256(resolved.encode()).hexdigest()[:8]}.{suffix}"


def stale_copy_problems(state: str | None, payload: dict[str, Any] | None, present: bool) -> list[str]:
    """With the old provider's copy (no owner record) in the state_dir, the stream's run succeeded, not refused."""
    problems = [] if present else ["no stale copy in the state_dir: the cell proves nothing"]
    if state != "success":
        problems.append(f"run {state}")
    error = (payload or {}).get("error") or {}
    if error or (payload or {}).get("decision") != "success":
        problems.append(f"rivet stopped [{error.get('code')}] decision {(payload or {}).get('decision')!r}")
    return problems


def flip(root: Path) -> str:
    """What git-sync does on a new revision: the worktree `rev-B` (the config copied unchanged), a new link renamed over
    `repo` in one step, and the old worktree removed. The revision `repo` names now."""
    old = os.readlink(root / "repo")
    new = "rev-B" if old != "rev-B" else "rev-A"
    shutil.copytree(root / old, root / new)
    tmp = root / f".repo.{new}.tmp"
    tmp.unlink(missing_ok=True)
    tmp.symlink_to(new)
    os.replace(tmp, root / "repo")
    shutil.rmtree(root / old)
    return os.readlink(root / "repo")


def run_after_flip(state: str | None, tasks: dict[str, dict[str, Any]], payload: dict[str, Any] | None, events: int) -> list[str]:
    """The stream's first run after the flip succeeded, every task of it too, and its run moved the changes made."""
    problems = [] if state == "success" else [f"run {state}"]
    bad = {t: i.get("state") for t, i in tasks.items() if i.get("state") not in ("success", "skipped")}
    if bad or not tasks:
        problems.append(f"tasks {bad or 'none'}")
    error = (payload or {}).get("error") or {}
    if error:
        problems.append(f"rivet stopped [{error.get('code')}] exit {(payload or {}).get('exit_status')}")
    elif payload is not None and unit_rows(payload) != events:
        problems.append(f"the run moved {unit_rows(payload)} row(s), the source changed {events}")
    return problems


def stream_equals_source(
    got: dict[str, list[dict[str, Any]]], source: dict[str, list[dict[str, Any]]], cols: dict[str, list[str]], key: str
) -> list[str]:
    """Every table of the stream: its BigQuery base without the rows flagged deleted equals the source, value by value."""
    problems = []
    for table in sorted(source):
        problems += warehouse.current_state(got.get(table, []), source[table], cols[table], key, 0, table, flagged=True)
    return problems


#: rivet #664: a checkpoint is read by one config; init refuses a name another config in its directory takes.
SHARED_CODE = "RIVET_SOURCE_CDC_CHECKPOINT_SHARED"
NAME_TAKEN = "RIVET_INIT_CDC_NAME_TAKEN"
REFUSED_EXIT = 5


def checkpoint_of(text: str | None) -> str | None:
    """The `cdc.checkpoint` of a config's one CDC export."""
    found = [
        e.get("cdc", {}).get("checkpoint")
        for e in (yaml.safe_load(text or "") or {}).get("exports") or []
        if e.get("mode") == "cdc"
    ]
    return found[0] if len(found) == 1 else None


def same_checkpoint(first: str, second: str | None) -> list[str]:
    """Both configs name one relative checkpoint, so in one `state_dir` they read one file (else nothing is shared)."""
    a, b = checkpoint_of(first), checkpoint_of(second)
    if a is None or a != b or a.startswith("/"):
        return [f"the first config's checkpoint is {a!r}, the second's {b!r}: no shared checkpoint in one state_dir"]
    return []


def shared_refused(tasks: dict[str, dict[str, Any]], payload: dict[str, Any] | None) -> list[str]:
    """The second config's run stopped on the coded refusal, on Airflow's first try (a refusal is never retried), and
    nothing after it ran."""
    run = tasks.get("run") or {}
    problems = []
    if run.get("state") != "failed":
        problems.append(f"run task {run.get('state')!r}, expected failed")
    if run.get("try_number") != 1:
        problems.append(f"run task try {run.get('try_number')!r}: a refusal is never retried by Airflow")
    error = (payload or {}).get("error") or {}
    got = ((payload or {}).get("decision"), error.get("code"), (payload or {}).get("exit_status"))
    if got != ("fail", SHARED_CODE, REFUSED_EXIT):
        problems.append(f"decision, code, exit {got}; expected ('fail', {SHARED_CODE!r}, {REFUSED_EXIT})")
    ran = sorted(t for t, i in tasks.items() if t != "run" and i.get("state") == "success")
    if ran:
        problems.append(f"tasks ran after the refusal: {ran}")
    return problems


def init_refused(answer: dict[str, Any]) -> list[str]:
    """init refused to write the second config: exit 5, the code, nothing written."""
    problems = []
    if answer.get("exit") != REFUSED_EXIT:
        problems.append(f"init exited {answer.get('exit')!r}, expected {REFUSED_EXIT}")
    if f"[{NAME_TAKEN}]" not in (answer.get("said") or ""):
        problems.append(f"init's answer does not carry [{NAME_TAKEN}]: {(answer.get('said') or '')[-1500:]}")
    if answer.get("text") is not None:
        problems.append("init wrote a config although it refused")
    return problems


def wait_marker(path: Any, timeout: float = 300) -> float | None:
    """The monotonic time a pause marker appeared, or None after the timeout."""
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            return None
        time.sleep(0.25)
    return time.monotonic()


def task_log(stand: Any, dag: str, run_id: str, task: str) -> str | None:
    """The newest attempt's log of one task instance (`dag_id=/run_id=/task_id=/attempt=N.log`), or None."""
    root = Path(stand.dir) / "logs" / f"dag_id={dag}" / f"run_id={run_id}" / f"task_id={task}"
    found = sorted(root.glob("attempt=*.log"), key=lambda p: int(re.sub(r"\D", "", p.stem) or 0))
    return found[-1].read_text(errors="replace") if found else None


def stderr_tail(stand: Any, payload: dict[str, Any], lines: int = 40) -> str:
    """The end of rivet's stderr log of a task (the operator keeps it in the state_dir, on the stand's volume)."""
    path = str(payload.get("stderr_path") or "")
    if not path.startswith("/opt/rivet/"):
        return f"(no stderr log: {path!r})"
    host = stand.rivet_dir / path.removeprefix("/opt/rivet/")
    return "\n".join(host.read_text(errors="replace").splitlines()[-lines:]) if host.exists() else f"(no file {host})"


def headline(text: str | None) -> str:
    heads = list(HEADLINE.finditer(text or ""))
    return heads[-1].group(0).strip() if heads else ""


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


#: What a client stack on a delta mode relies on in the `load:` block init writes: BigQuery, a base and a buffer that
#: `compact` merges, and the staged Parquet deleted once loaded.
CLIENT_LOAD = {"target": "bigquery", "layout": "base_buffer", "cleanup_source": True}
#: Unit statuses of a task that did its work (a compaction with nothing to merge reports `skipped`).
DONE = frozenset({"success", "loaded", "compacted", "skipped"})
#: A task log's headline (airflow_provider_rivet/tasklog.py `render`): `rivet <subject> → [target:] OK: 3 rows ... in 1.2 s`.
HEADLINE = re.compile(
    r"rivet (?P<subject>[^\n→]*?) →(?P<rest>[^\n]*?)\b(?P<status>OK|SKIPPED|FAILED|REFUSED|STOPPED|BUSY)\b(?P<tail>[^\n]*)"
)


def load_block_problems(text: str) -> list[str]:
    """How the `load:` block init wrote differs from what the client stack runs on (`CLIENT_LOAD`)."""
    load = (yaml.safe_load(text) or {}).get("load")
    if not isinstance(load, dict):
        return ["init wrote no top-level `load:` block"]
    return [f"load.{k} is {load.get(k)!r}, expected {want!r}" for k, want in CLIENT_LOAD.items() if load.get(k) != want]


def unit_rows(payload: dict[str, Any]) -> int | None:
    """The rows a task's units report in total; None when no unit reports a count."""
    known = [u["rows"] for u in payload.get("units") or [] if type(u.get("rows")) is int]
    return sum(known) if known else None


def xcom_problems(payload: dict[str, Any] | None, rows: int | None) -> list[str]:
    """A rivet task's XCom return value says it did its work and, when `rows` is given, moved exactly that many rows."""
    if not isinstance(payload, dict):
        return [f"no XCom return value ({payload!r})"]
    problems = []
    if payload.get("decision") != "success" or payload.get("exit_status") != 0 or payload.get("error") is not None:
        problems.append(
            f"decision {payload.get('decision')!r}, exit {payload.get('exit_status')!r}, error {payload.get('error')!r}"
        )
    units = payload.get("units") or []
    if not units:
        problems.append("no units")
    bad = [(u.get("export"), u.get("table"), u.get("status")) for u in units if u.get("status") not in DONE or u.get("error")]
    if bad:
        problems.append(f"units not done: {bad}")
    if rows is not None and unit_rows(payload) != rows:
        problems.append(f"units report {unit_rows(payload)} row(s), the source changed {rows}")
    return problems


def log_problems(text: str | None, payload: dict[str, Any] | None) -> list[str]:
    """The task log's headline says what the XCom says: OK (or SKIPPED, for a unit that skipped), and the same rows."""
    if text is None:
        return ["no task log"]
    heads = list(HEADLINE.finditer(text))
    if not heads:
        return ["no `rivet ... →` headline in the task log"]
    head = heads[-1]
    statuses = {u.get("status") for u in (payload or {}).get("units") or []}
    want = ("SKIPPED", "OK") if statuses == {"skipped"} else ("OK",)
    problems = [] if head["status"] in want else [f"headline {head.group(0).strip()!r}, expected {' or '.join(want)}"]
    said = re.search(r"\b(\d+) rows?\b", head["tail"])
    reported = unit_rows(payload or {})
    # `SKIPPED: nothing to do` names no count: right for units that moved nothing.
    quiet = head["status"] == "SKIPPED" and said is None and reported in (None, 0)
    if not quiet and (int(said.group(1)) if said else None) != reported:
        problems.append(f"headline says {said.group(0) if said else 'no rows'}, the XCom units {reported}")
    return problems


def rows_past(rows: Sequence[dict[str, Any]], cursor: str, high_water: Any) -> int:
    """How many rows an incremental extract reads: the ones whose cursor is past the previous high-water (all of them
    on the first run)."""
    return sum(1 for r in rows if high_water is None or (r.get(cursor) is not None and r[cursor] > high_water))


def high_water(rows: Sequence[dict[str, Any]], cursor: str) -> Any:
    return max((r[cursor] for r in rows if r.get(cursor) is not None), default=None)


def column_reached(got: Sequence[dict[str, Any]], source: Sequence[dict[str, Any]], column: str, key: str) -> list[str]:
    """Every value of a column added mid-stream reached the warehouse under its key, and the source holds at least one
    (a check of an all-NULL column proves nothing)."""
    want = {str(r[key]): r[column] for r in source if r.get(column) is not None}
    if not want:
        return [f"the source holds no value of {column}: nothing to compare"]
    rows = [{k.lower(): v for k, v in r.items()} for r in got]
    if rows and column not in rows[0]:
        return [f"no column {column} in the read-back (has {sorted(rows[0])})"]
    live = [r for r in rows if not warehouse._deleted(r.get(warehouse.DELETED_FLAG))]
    have = {str(r[key]): r.get(column) for r in live if r.get(column) is not None}
    problems = []
    missing = sorted(k for k in want if have.get(k) != want[k])
    if missing:
        problems.append(f"{len(missing)} of {len(want)} value(s) of {column} not in the warehouse, e.g. "
                        f"{[(k, want[k], have.get(k)) for k in missing[:5]]}")  # fmt: skip
    extra = sorted(set(have) - set(want))
    if extra:
        problems.append(f"{len(extra)} value(s) of {column} the source does not hold, e.g. {extra[:5]}")
    return problems


def window_problems(opened: float | None, done: float, pause_ms: int, margin: float = 5.0) -> list[str]:
    """The hot ALTER and the writes after it landed while the run was still paused after it opened (`opened`, `done`:
    monotonic seconds; the pause sleeps `pause_ms` from the moment its marker was written)."""
    if opened is None:
        return ["the CDC run never reached cdc_after_open (no marker)"]
    took = done - opened
    if took > pause_ms / 1000 - margin:
        return [f"the ALTER and its writes ended {took:.1f} s after the pause opened; the pause is {pause_ms} ms"]
    return []


def staged_left(objects: Sequence[str]) -> list[str]:
    """`cleanup_source: true`: once a run is loaded, none of its staged Parquet is left in the bucket."""
    parts = sorted(o for o in objects if o.endswith(".parquet"))
    return [f"{len(parts)} staged Parquet object(s) left after the load, e.g. {parts[:3]}"] if parts else []


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
