"""Source legs: a database engine beside the postgres source, extracted to GCS and loaded and compacted into BigQuery
through Airflow. Every rivet config is the one `rivet init` writes against the live source, used as written: the gate
gives init a bucket and datasets of the run's own, and reads export names, prefixes and the checkpoint from what it
wrote (nothing is edited). The warehouse is read back by a client the loader does not use and compared with the source.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import duckdb
import yaml

from . import warehouse
from .stand import sh

#: A bucket the gate made for one stack and engine: `rivet-afgate-<tag>-<stack>-<engine>` (the sweep drops nothing else).
BUCKET_SHAPE = re.compile(r"rivet-afgate-\d{8}[0-9a-f]{4}-\d+-[a-z]+")


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
    #: DuckDB's scanner for the engine and the attach string (host port and password filled in).
    duck_type: str
    duck_dsn: str

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
        return f"{self.conn_type}://rivet:{getattr(secrets, self.name)}@{self.service}:{self.port}/rivet"


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
    duck_type="mysql",
    duck_dsn="host=127.0.0.1 port={port} user=rivet password={password} database=rivet",
)
ENGINES = {e.name: e for e in (MYSQL,)}
#: The tables every engine's scenario holds, and the columns compared (the key first is `id`).
TABLES = {
    "gate_customers": ["id", "name", "tier", "updated_at"],
    "gate_orders": ["id", "customer_id", "amount", "status", "updated_at"],
}
#: Keys an incremental extract cannot see deleted, left in the batch target after each cycle.
DELETED_BY_BATCH = {"initial": {}, "delta": {"gate_orders": 5}, "idle": {"gate_orders": 5}}


def init_argv(kind: str, engine: Engine, bucket: str, project: str, dataset: str, out: str) -> list[str]:
    """`rivet init` for one config: batch (`incremental`) or the CDC stream, GCS and a BigQuery `load:` block."""
    mode = "cdc" if kind == "cdc" else "incremental"
    return [
        "rivet", "init", "--source-env", engine.url_env, "--tls", "disable", "--mode", mode, "--gcs-bucket", bucket,
        "--bigquery-project", project, "--bigquery-dataset", dataset, "-o", out,
    ]  # fmt: skip


def read_config(text: str) -> dict[str, Any]:
    """What the gate takes from a generated config: the exports by name and the CDC stream, if there is one."""
    doc = yaml.safe_load(text) or {}
    exports = doc.get("exports") or []
    streams = [e for e in exports if e.get("mode") == "cdc"]
    if len(streams) > 1:
        raise ValueError(f"{len(streams)} CDC exports; the leg runs one stream per config")
    return {
        "exports": [e["name"] for e in exports if e.get("mode") != "cdc"],
        "stream": streams[0]["name"] if streams else None,
        "tables": list(streams[0].get("tables") or []) if streams else [],
    }


class SourceLeg:
    """One engine of one stack: its bucket, datasets, init-made configs, two DAGs, and their oracles."""

    def __init__(self, cloud: warehouse.Cloud, stack: str, engine: Engine) -> None:
        self.cloud, self.stack, self.engine = cloud, stack, engine
        tag = f"{cloud.tag}-{stack.replace('.', '')}"
        self.bucket = f"rivet-afgate-{tag}-{engine.name}"
        self.datasets = {k: f"{cloud.dataset(stack)}_{engine.name}_{k}" for k in ("batch", "cdc")}
        self.dags = [f"gate_src_{engine.name}_{k}" for k in ("batch", "cdc")]
        self.shape: dict[str, dict[str, Any]] = {}
        self.tasks: dict[str, list[str]] = {}
        # A compaction with nothing to merge skips; the `compacted` cell holds every cycle to "no buffer left".
        self.may_skip = {dag: ("compact.",) for dag in self.dags}
        self._bq: Any = None

    def sql(self, stand: Any, text: str) -> str:
        """Statements through the engine's client inside its container."""
        env = {self.engine.client_env: getattr(stand.secrets, f"{self.engine.name}_root")}
        return stand.compose(
            "exec", "-T", "-e", self.engine.client_env, self.engine.service, *self.engine.client, input=text, extra_env=env
        ).stdout

    def wait_ready(self, stand: Any, timeout: float = 300) -> None:
        env = {self.engine.client_env: getattr(stand.secrets, f"{self.engine.name}_root")}
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
        """Seed the source; the bucket and datasets; `rivet init` writes both configs inside the stack's network."""
        for kind in self.datasets:
            (stand.rivet_dir / "state" / f"{self.engine.name}_{kind}").mkdir(parents=True, exist_ok=True)
        self.wait_ready(stand)
        self.sql(stand, self.engine.setup)
        self.sql(stand, self.engine.seed)
        sh(["gcloud", "storage", "buckets", "create", f"gs://{self.bucket}", f"--project={self.cloud.project}",
            f"--location={self.cloud.location}", "--uniform-bucket-level-access"], timeout=300)  # fmt: skip
        for kind, ds in self.datasets.items():
            warehouse._bq(f"--location={self.cloud.location}", "mk", "-f", "--dataset", f"{self.cloud.project}:{ds}")
            host = stand.rivet_dir / "configs" / f"{self.engine.name}_{kind}"
            host.mkdir(parents=True, exist_ok=True)
            out = f"/opt/rivet/configs/{self.engine.name}_{kind}/rivet.yaml"
            argv = init_argv(kind, self.engine, self.bucket, self.cloud.project, ds, out)
            stand.compose(
                "exec", "-T", "-e", self.engine.url_env, "airflow-worker", *argv,
                extra_env={self.engine.url_env: self.engine.url(stand.secrets)},
            )  # fmt: skip
            # init records the source's primary keys (`pk: auto` reads them for a `query:` export) in a state beside
            # the config; the DAG keeps state in state_dir, and the provider refuses a state left beside the config
            # (RIVET_AIRFLOW_STATE_BESIDE_CONFIG). The gate does what that refusal says: it moves the file there.
            state = stand.rivet_dir / "state" / f"{self.engine.name}_{kind}"
            for suffix in ("", "-wal", "-shm"):
                beside = host / f".rivet_state.db{suffix}"
                if beside.exists():
                    beside.rename(state / beside.name)
            text = (host / "rivet.yaml").read_text()
            print(f"INIT {self.engine.name} {kind} config:\n{text}", flush=True)
            self.shape[kind] = read_config(text)
        batch, cdc = self.shape["batch"], self.shape["cdc"]
        if sorted(batch["exports"]) != sorted(TABLES) or sorted(cdc["tables"]) != sorted(TABLES) or not cdc["stream"]:
            raise ValueError(f"init wrote exports {batch['exports']} and stream {cdc['stream']} over {cdc['tables']}")
        self.tasks = {
            self.dags[0]: [f"{g}.{e}" for e in batch["exports"] for g in ("apply", "load", "compact")],
            self.dags[1]: ["run", *(f"{g}.{t}" for t in cdc["tables"] for g in ("load", "compact"))],
        }
        # The DAG file reads which exports and stream to build from here (the names are init's, not the gate's).
        (stand.rivet_dir / "configs" / f"{self.engine.name}_shape.json").write_text(json.dumps(self.shape))

    def apply_changes(self, stand: Any) -> None:
        self.sql(stand, self.engine.changes)

    def cleanup(self) -> None:
        """Remove the bucket and both datasets, then look again: anything left raises."""
        self._bq = None
        sh(["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{self.bucket}"], timeout=600, check=False)
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
        listed = sh(["gcloud", "storage", "buckets", "describe", f"gs://{self.bucket}"], timeout=300, check=False)
        if listed.returncode == 0:
            left.append(f"gcs:{self.bucket}")
        elif "404" not in listed.stderr and "not found" not in listed.stderr.lower():
            raise RuntimeError(f"cannot tell whether {self.bucket} is gone: {listed.stderr.strip()[-500:]}")
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
        """Every source row, read by DuckDB's scanner of the engine (a client rivet does not use)."""
        port = stand.host_port(self.engine.service, self.engine.port)
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'")
            con.execute(f"INSTALL {self.engine.duck_type}; LOAD {self.engine.duck_type}")
            dsn = self.engine.duck_dsn.format(port=port, password=getattr(stand.secrets, self.engine.name))
            con.execute(f"ATTACH '{dsn}' AS src (TYPE {self.engine.duck_type}, READ_ONLY)")
            out = {}
            for table, cols in TABLES.items():
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
            for table, cols in TABLES.items():
                cell = f"{name}/{self.engine.name}/bigquery/{kind}/{table}"
                v.check(
                    cell + "/current-equals-source",
                    self._compare(kind, table, source[table], cols, 0 if kind == "cdc" else deleted.get(table, 0)),
                    ok=f"{len(source[table])} row(s)",
                )
                if objects is not None:
                    v.check(cell + "/compacted", warehouse.compacted, objects, table, cell)

    def _compare(
        self, kind: str, table: str, source: list[dict[str, Any]], cols: list[str], deleted: int
    ) -> Callable[[], list[str]]:
        what = f"bigquery {self.datasets[kind]}.{table}"
        return lambda: warehouse.current_state(self.read(kind, table), source, cols, "id", deleted, what, flagged=kind == "cdc")


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
