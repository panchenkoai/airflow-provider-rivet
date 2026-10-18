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
from datetime import datetime
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
            "schema": "rivet",
        }

    def url(self, secrets: Any) -> str:
        """The URL `rivet init` reads the source with (inside the stack's network)."""
        return f"{self.scheme or self.conn_type}://rivet:{getattr(secrets, self.name)}@{self.service}:{self.port}/rivet"

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
ENGINES = {e.name: e for e in (MYSQL, MSSQL)}
#: Keys an incremental extract cannot see deleted, left in the batch target after each cycle.
DELETED_BY_BATCH = {"initial": {}, "delta": {"gate_orders": 5}, "idle": {"gate_orders": 5}}


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
        self._bq: Any = None

    def sql(self, stand: Any, text: str) -> str:
        """Statements through the engine's client inside its container."""
        env = {self.engine.client_env: self.engine.root_password(stand.secrets)}
        return stand.compose(
            "exec", "-T", "-e", self.engine.client_env, self.engine.service, *self.engine.client, input=text, extra_env=env
        ).stdout

    def wait_ready(self, stand: Any, timeout: float = 300) -> None:
        env = {self.engine.client_env: self.engine.root_password(stand.secrets)}
        deadline = time.monotonic() + timeout
        while True:
            done = stand.compose(
                "exec", "-T", "-e", self.engine.client_env, self.engine.service, *self.engine.ready, check=False, extra_env=env
            )
            # The entrypoint answers once on a temporary server before the real one starts: ask the SQL client too.
            if done.returncode == 0 and self.sql_ok(stand):
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"{self.engine.service} not ready after {timeout:.0f} s")
            time.sleep(3)

    def sql_ok(self, stand: Any) -> bool:
        try:
            return "1" in self.sql(stand, "SELECT 1;")
        except RuntimeError:
            return False

    def prepare(self, stand: Any) -> None:
        """Seed the source; the buckets and datasets; `rivet init` writes both configs inside the stack's network."""
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
            stand.compose(
                "exec", "-T", "-e", self.engine.url_env, "-e", "RIVET_STATE_URL", "airflow-worker", *argv,
                extra_env={
                    self.engine.url_env: self.engine.url(stand.secrets),
                    "RIVET_STATE_URL": stand.state_url(state_db(self.engine.name, kind)),
                },
            )  # fmt: skip
            # init records the source's primary keys (`pk: auto` reads them for a `query:` export) in the state it opens:
            # the config's PostgreSQL state, the one its DAG's runs read, so nothing is left beside the config.
            left = sorted(p.name for p in host.glob(".rivet_state.db*"))
            if left:
                raise RuntimeError(f"init left {left} beside {host} although RIVET_STATE_URL named the PostgreSQL state")
            text = (host / "rivet.yaml").read_text()
            print(f"INIT {self.engine.name} {kind} config:\n{text}", flush=True)
            self.shape[kind] = read_config(text, list(self.engine.tables))
        batch, cdc = self.shape["batch"], self.shape["cdc"]
        tables = sorted(self.engine.tables)
        captured = sorted(short(t) for s in cdc["streams"] for t in s["tables"])
        extra = batch["extra"] + cdc["extra"]
        if sorted(batch["exports"]) != tables or captured != tables or extra:
            raise ValueError(f"init wrote batch exports {batch['exports']}, streams {cdc['streams']}, others {extra}")
        # Where each scenario table lands: a batch export loads under its name, a stream's table under its own.
        self.targets = {
            "batch": {e: e for e in batch["exports"]},
            "cdc": {short(t): warehouse_table(t) for s in cdc["streams"] for t in s["tables"]},
        }
        prefix = f"gate_src_{self.engine.name}"
        self.tasks = {f"{prefix}_batch": [f"{g}.{e}" for e in batch["exports"] for g in ("apply", "load", "compact")]}
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
        return [dict(row.items()) for row in self._client().query(sql).result()]

    def source_rows(self, stand: Any) -> dict[str, list[dict[str, Any]]]:
        """Every source row, read by a client rivet does not use: DuckDB's scanner, or the engine's own SQL client."""
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
        without the rows flagged deleted equals the source. Both: no buffer is left after `compact`."""
        source = v.guard(f"{name}/{self.engine.name}/source", self.source_rows, stand)
        if source is None:
            return
        deleted = DELETED_BY_BATCH.get(name, {})
        for kind in ("batch", "cdc"):
            objects = v.guard(f"{name}/{self.engine.name}/{kind}/objects", self.objects, kind)
            print(f"INVENTORY {name} {self.engine.name} {kind} {sorted(objects or [])}", flush=True)
            for table, cols in self.engine.tables.items():
                cell = f"{name}/{self.engine.name}/bigquery/{kind}/{table}"
                v.check(
                    cell + "/current-equals-source",
                    self._compare(kind, table, source[table], cols, 0 if kind == "cdc" else deleted.get(table, 0)),
                    ok=f"{len(source[table])} row(s)",
                )
                if objects is not None:
                    v.check(cell + "/compacted", warehouse.compacted, objects, self.targets[kind][table], cell)

    def _compare(
        self, kind: str, table: str, source: list[dict[str, Any]], cols: list[str], deleted: int
    ) -> Callable[[], list[str]]:
        target = self.targets[kind][table]
        what = f"bigquery {self.datasets[kind]}.{target}"
        return lambda: warehouse.current_state(self.read(kind, target), source, cols, "id", deleted, what, flagged=kind == "cdc")


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
