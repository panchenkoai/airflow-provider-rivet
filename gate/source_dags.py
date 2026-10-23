"""`--source-dags`: the source legs' engines through `build_source_dags`, the way docs/dag-builders.md ("One DAG per
database") tells an operator to run them.

The gate writes no rivet config and runs no `rivet init`. Each source is a name: `<engine>_<batch|cdc>`, a data source
of the odbc.ini mounted at /etc/odbc.ini in every Airflow container (MySQL, SQL Server, Oracle; no Airflow Connection
of that name exists), or an Airflow Connection (MongoDB, whose ODBC driver the provider refuses by design). The DAG
file (gate/stand/dags/rivet_source_dags.py) passes the names to build_source_dags, one call per engine and mode, with
the stack's PostgreSQL state server as `state_connection` and `init_args` naming each source's bucket and dataset by
`{name}`. The gate unpauses each DAG (`@once` runs its `init` task, which creates `rivet_state_<name>` and writes the
config), waits for the DAG the next parse builds from that config, and then runs the source legs' cycles on it, graded
by the source legs' oracles: BigQuery value by value, or, for Oracle CDC (no `load:` block), its Parquet parts replayed
with `oracle.cdc_replay`.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import yaml

from . import oracle, warehouse
from .sources import ENGINES, Engine, SourceLeg, read_config, short, task_log, warehouse_table
from .stand import sh

#: Where the DAG file puts every source's config and state folder (in the containers; the stand's rivet mount).
CONFIG_DIR = "/opt/rivet/configs/srcdags"
STATE_ROOT = "/opt/rivet/state/srcdags"
#: The stack's PostgreSQL state server (the gate's TLS `gate-state`, its user may create databases).
STATE_CONNECTION = "rivet_gate_state"
#: The modes each engine runs as a source of its own (MongoDB's batch is not run: the source legs have none).
KINDS = {"mysql": ("batch", "cdc"), "mssql": ("batch", "cdc"), "oracle": ("batch", "cdc"), "mongodb": ("cdc",)}
#: Whether init's CDC config of the engine is one stream per table (a task group per stream) or one stream (MySQL).
GROUPED = {"mysql": False, "mssql": True, "oracle": True, "mongodb": True}
#: Oracle mines from CDB$ROOT as a common user (sources.ORACLE's setup makes it); every other source signs in as rivet.
ORACLE_CDC_USER = "c##rivetcdc"
USERS = {("oracle", "cdc"): ORACLE_CDC_USER}
#: init flags beyond the bucket, dataset, `--tls` and tables: the common user's own schema holds no table.
INIT_FLAGS = {("oracle", "cdc"): ("--schema", "RIVET")}
#: The odbc.ini data source of each engine, as its ODBC driver's documentation spells it.
ODBC = {
    "mysql": "Driver = MySQL ODBC 8.0 Unicode Driver\nServer = gate-mysql\nPort = 3306\nDatabase = rivet\n"
    "User = {user}\nPassword = {password}\n",
    "mssql": "Driver = ODBC Driver 18 for SQL Server\nServer = tcp:gate-mssql,1433\nDatabase = rivet\nUID = {user}\n"
    "PWD = {password}\nEncrypt = yes\nTrustServerCertificate = yes\n",
    "oracle": "Driver = Oracle 21 ODBC driver\nServerName = gate-oracle:1521/FREEPDB1\nUserID = {user}\nPassword = {password}\n",
}
#: A MongoDB data source in the odbc.ini (its driver reads the BI Connector), with no Connection of that name: its DAG's
#: `init` must be refused, saying why.
REFUSED_SOURCE = "mongodb_odbc"
REFUSED_ODBC = (
    "Driver = MongoDB ODBC 1.4.5 Unicode Driver\nServer = gate-mongodb\nPort = 27017\nDatabase = rivet\nUser = rivet\n"
    "Password = {password}\n"
)
REFUSAL_CODE = "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED"
REFUSAL_TEXT = "MongoDB's ODBC driver reads through the BI Connector's MySQL protocol"
#: Streams rivet refuses by a documented limit, on every run: the table, the code, where rivet says so.
CDC_REFUSED = {
    "oracle": {
        "GATE_ORDERS": (
            "RIVET_SOURCE_CDC_TABLE_UNSUPPORTED",
            "rivet's cdc.md (Oracle): the preview refuses a table with a LOB column by name; GATE_ORDERS.doc is a BLOB",
        )
    }
}
#: The change events of each cycle in the CDC parts graded as files (Oracle's `changes` on its CDC-able table).
FILE_OPS = {"delta": {"GATE_CUSTOMERS": {"insert": 20, "update": 10}}}
#: The order of Oracle change events: the commit SCN in `__pos`, then `__seq` (rivet's cheat-sheet, `__pos`).
ORACLE_POS = "json_extract_string(__pos, '$.commit_scn')::UBIGINT"
#: The rivet tasks build_source_dags adds beside the rivet commands: the run's watcher.
WATCHER = "watcher"


def source_name(engine: str, kind: str) -> str:
    """The name of one source: the odbc.ini section or Connection id, the config's file stem, the DAG id's part."""
    return f"{engine}_{kind}"


def dag_id(name: str, mode: str) -> str:
    return f"rivet_{name}_{mode}"


def state_database(name: str) -> str:
    """The state database `init` creates for a source (build_source_dags: `rivet_state_<name>`)."""
    return f"rivet_state_{name}"


@dataclass(frozen=True)
class Layout:
    """What the stand gives `--source-dags`: the odbc.ini of the containers and the Connections of the other sources."""

    engines: tuple[Engine, ...]

    def odbc_ini(self, secrets: Any) -> str:
        sections = []
        for e in self.engines:
            password = getattr(secrets, e.name)
            if e.name in ODBC:
                for kind in KINDS[e.name]:
                    body = ODBC[e.name].format(user=USERS.get((e.name, kind), "rivet"), password=password)
                    sections.append(f"[{source_name(e.name, kind)}]\n{body}")
            if e.name == "mongodb":
                sections.append(f"[{REFUSED_SOURCE}]\n{REFUSED_ODBC.format(password=password)}")
        return "\n".join(sections)

    def connections(self, secrets: Any) -> dict[str, dict[str, Any]]:
        return {
            source_name(e.name, kind): e.connection(secrets) for e in self.engines if e.name not in ODBC for kind in KINDS[e.name]
        }


def expected_tasks(text: str, mode: str) -> list[str]:
    """The task ids docs/dag-builders.md promises for a config, sorted: for `cdc`, each `mode: cdc` export a stream
    (`run`, `load.T` with a `load:` block, `compact.T` when its layout is `base_buffer`), at the top for one stream and in
    a group named after the export for several; for `batch`, `apply.E` and (with `load:`) `load.E` per export; and the
    watcher."""
    doc = yaml.safe_load(text) or {}
    exports = doc.get("exports") or []
    load = doc.get("load") or {}
    ids = [WATCHER]
    if mode == "cdc":
        streams = [(e["name"], e.get("tables") or [e.get("table")]) for e in exports if e.get("mode") == "cdc"]
        for export, tables in streams:
            group = "" if len(streams) == 1 else f"{export}."
            ids.append(f"{group}run")
            if load:
                ids += [f"{group}load.{t}" for t in tables]
            if load.get("layout") == "base_buffer":
                ids += [f"{group}compact.{t}" for t in tables]
    else:
        names = [e["name"] for e in exports if e.get("mode") != "cdc"]
        ids += [f"apply.{n}" for n in names] + ([f"load.{n}" for n in names] if load else [])
    return sorted(ids)


def shape_problems(actual: Sequence[str], text: str, mode: str, grouped: bool) -> list[str]:
    """The DAG the parse after `init` built has the task ids its config promises, and init wrote the streams the
    engine captures: one per table where it captures per table (a group per stream), else one."""
    want = expected_tasks(text, mode)
    problems = []
    if sorted(actual) != want:
        problems.append(
            f"task ids {sorted(actual)}, expected {want} (missing {sorted(set(want) - set(actual))}, "
            f"extra {sorted(set(actual) - set(want))})"
        )
    if mode == "cdc":
        streams = [e for e in (yaml.safe_load(text) or {}).get("exports") or [] if e.get("mode") == "cdc"]
        if grouped and len(streams) < 2:
            problems.append(f"init wrote {len(streams)} stream(s): this engine captures per table, a group per stream")
        if not grouped and len(streams) != 1:
            problems.append(f"init wrote {len(streams)} streams, expected one over every table")
    return problems


def init_problems(facts: Mapping[str, Any], name: str, odbc: bool) -> list[str]:
    """The DAG's `init` wrote the config, created the source's state database (absent before it ran) and read the
    source where it should (the odbc.ini data source, or the Connection); every CDC checkpoint is in the source's state
    folder after the first run (`checkpoints`: path -> present)."""
    db = state_database(name)
    log = facts.get("log") or ""
    problems = [] if facts.get("run") == "success" else [f"the init run ended {facts.get('run')!r}"]
    if not facts.get("config"):
        problems.append(f"no config {name}.yaml in the config folder")
    if facts.get("db_before"):
        problems.append(f"{db} existed before init ran: the cell proves nothing")
    if not facts.get("db_after"):
        problems.append(f"no database {db} on the state server")
    if f"state database `{db}` created" not in log:
        problems.append(f"init's log does not say it created `{db}`")
    via = f"ODBC data source [{name}]"
    if odbc and via not in log:
        problems.append(f"init's log does not say it read the {via}")
    if not odbc and "ODBC data source" in log:
        problems.append("init read the source from odbc.ini, not from its Connection")
    missing = sorted(p for p, there in (facts.get("checkpoints") or {}).items() if not there)
    if missing:
        problems.append(f"no checkpoint after the first run: {missing}")
    return problems


def refusal_problems(state: str | None, tries: Sequence[Mapping[str, Any]], payload: Any, log: str | None) -> list[str]:
    """The DAG of a MongoDB data source of the odbc.ini: its `init` refused on its first try, by code, saying why."""
    problems = [] if state == "failed" else [f"the init run ended {state!r}, expected failed"]
    if [t.get("state") for t in tries] != ["failed"]:
        problems.append(f"init tries {[t.get('state') for t in tries]}: a refusal fails once and is never retried")
    code = (payload or {}).get("preflight_refusal") if isinstance(payload, dict) else None
    if code != REFUSAL_CODE:
        problems.append(f"refusal code {code!r}, expected {REFUSAL_CODE}")
    if REFUSAL_TEXT not in (log or ""):
        problems.append(f"the task log does not say: {REFUSAL_TEXT}")
    return problems


def stream_refused_problems(task: Mapping[str, Any], payload: Any, code: str) -> list[str]:
    """A stream rivet refuses by a documented limit stopped on that code, on Airflow's first try."""
    problems = [] if task.get("state") == "failed" else [f"task {task.get('state')!r}, expected failed"]
    if task.get("try_number") not in (1, None):
        problems.append(f"try {task.get('try_number')}: a refusal is never retried")
    error = (payload or {}).get("error") or {} if isinstance(payload, dict) else {}
    if error.get("code") != code:
        problems.append(f"rivet stopped with {error.get('code')!r}, expected {code}")
    return problems


def part_table(path: str, tables: Sequence[str]) -> str | None:
    """The scenario table a Parquet part belongs to: the one whose name its path holds (None: none, or several)."""
    found = [t for t in tables if t.lower() in path.lower()]
    return found[0] if len(found) == 1 else None


def is_change_part(con: duckdb.DuckDBPyConnection, path: str, local: Path) -> bool:
    """A part of change events (an `__op` column), not of the baseline snapshot (under `/snapshot/`, or no `__op`)."""
    if "/snapshot/" in path:
        return False
    return "__op" in {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{local}')").fetchall()}


DUCK_TYPES = {"int": "BIGINT", "money": "DECIMAL(38,9)", "ts": "TIMESTAMPTZ", "bytes": "BLOB", "uuid": "UUID"}


def source_relation(con: duckdb.DuckDBPyConnection, table: str, rows: Sequence[Mapping[str, Any]], cols: Sequence[str]) -> str:
    """The source rows (read by the engine's own reader) as a DuckDB table the replay oracle compares with."""
    kinds = warehouse.kinds_of(rows)
    name = f"src_{table.lower()}"
    columns = ", ".join(f"{c} {DUCK_TYPES.get(kinds.get(c) or '', 'VARCHAR')}" for c in cols)
    con.execute(f"CREATE OR REPLACE TABLE {name} ({columns})")
    if rows:
        con.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' for _ in cols)})", [[r[c] for c in cols] for r in rows])
    return name


def wait_first_run(stand: Any, dag: str, timeout: float = 1500) -> tuple[str, str] | None:
    """The DAG's first run once it ended: (run id, state); None when none ended in time."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ended = [r for r in stand.runs(dag) if r.get("state") in ("success", "failed")]
        if ended:
            return ended[0]["dag_run_id"], ended[0]["state"]
        time.sleep(5)
    return None


def wait_rebuilt(stand: Any, dag: str, timeout: float = 600) -> list[str]:
    """The DAG's task ids once a parse rebuilt it from the config (no `init` task left)."""
    deadline = time.monotonic() + timeout
    ids = stand.dag_tasks(dag)
    while "init" in ids:
        if time.monotonic() > deadline:
            raise TimeoutError(f"{dag} still has the init task after {timeout:.0f} s: no parse rebuilt it from its config")
        time.sleep(5)
        ids = stand.dag_tasks(dag)
    return ids


class SourceDagsLeg(SourceLeg):
    """One engine of one stack, its sources run by build_source_dags."""

    extras = False

    def __init__(self, cloud: warehouse.Cloud, stack: str, engine: Engine) -> None:
        super().__init__(cloud, stack, engine)
        tag = f"{cloud.tag}-{stack.replace('.', '')}"
        self.names = {k: source_name(engine.name, k) for k in KINDS[engine.name]}
        #: init's `--gcs-bucket` / `--bigquery-dataset`, `{name}` replaced by build_source_dags.
        self.bucket_of = f"rivet-afgate-{tag}-{{name}}"
        self.dataset_of = f"{cloud.dataset(stack)}_{{name}}"
        self.buckets = {k: self.bucket_of.replace("{name}", n) for k, n in self.names.items()}
        self.datasets = {k: self.dataset_of.replace("{name}", n) for k, n in self.names.items()}
        self.source_dags = {k: dag_id(n, k) for k, n in self.names.items()}
        self.refusal_dag = dag_id(REFUSED_SOURCE, "cdc") if engine.name == "mongodb" else ""
        #: Until `adopt`, the DAGs to unpause; then the DAGs each cycle runs.
        self.dags = [*self.source_dags.values(), *filter(None, [self.refusal_dag])]
        self.kinds = ()
        #: Kinds whose config has no `load:` block: their Parquet parts are graded (Oracle CDC).
        self.file_kinds: tuple[str, ...] = ()
        #: Per kind: the state database before init, the init run, its log, the config, the database after.
        self.facts: dict[str, dict[str, Any]] = {}
        #: A refused stream's task -> its table.
        self.refused_tables: dict[str, str] = {}
        self.seen: set[str] = set()
        self.local: dict[str, Path] = {}
        self.history: dict[str, list[list[str]]] = {}

    def init_args(self, kind: str) -> list[str]:
        eng = self.engine
        return [
            "--tls", eng.tls, *INIT_FLAGS.get((eng.name, kind), ()), "--gcs-bucket", self.bucket_of,
            "--bigquery-project", self.cloud.project, "--bigquery-dataset", self.dataset_of, "--include", *eng.tables,
        ]  # fmt: skip

    def spec(self, stand: Any) -> dict[str, Any]:
        """The calls the DAG file makes for this engine: one per mode (their flags differ), names only."""
        calls = [
            {
                "sources": [name, *([REFUSED_SOURCE] if kind == "cdc" and self.refusal_dag else [])],
                "mode": kind,
                "init_args": self.init_args(kind),
            }
            for kind, name in self.names.items()
        ]
        gcp = {"cloud_credentials": ["gcp"]} if getattr(stand, "gcp_key_file", None) else {"connections": ["gate_gcp"]}
        return {
            "engine": self.engine.name,
            "config_dir": CONFIG_DIR,
            "state_root": STATE_ROOT,
            "state_connection": STATE_CONNECTION,
            "operator_kwargs": gcp,
            "calls": calls,
        }

    def host(self, stand: Any, place: str) -> Path:
        """A container path under /opt/rivet on this host."""
        return stand.rivet_dir / place.removeprefix("/opt/rivet/")

    def state_db_exists(self, stand: Any, name: str) -> bool:
        db = state_database(name)
        return stand.psql("gate-state", "rivet_state", f"SELECT count(*) FROM pg_database WHERE datname = '{db}';").strip() == "1"

    def prepare(self, stand: Any) -> None:
        """Seed the source; the buckets and datasets init's flags name; the calls the DAG file makes. No init, no config,
        no state database: the DAGs' `init` tasks make them."""
        for place in (CONFIG_DIR, STATE_ROOT):
            self.host(stand, place).mkdir(parents=True, exist_ok=True)
        self.wait_ready(stand)
        self.sql(stand, self.engine.setup.replace("{password}", getattr(stand.secrets, self.engine.name)))
        self.sql(stand, self.engine.seed.replace("{orders_instance}", self.capture_instance(stand)))
        for kind, ds in self.datasets.items():
            sh(["gcloud", "storage", "buckets", "create", f"gs://{self.buckets[kind]}", f"--project={self.cloud.project}",
                f"--location={self.cloud.location}", "--uniform-bucket-level-access"], timeout=300)  # fmt: skip
            warehouse._bq(f"--location={self.cloud.location}", "mk", "-f", "--dataset", f"{self.cloud.project}:{ds}")
            self.facts[kind] = {"db_before": self.state_db_exists(stand, self.names[kind])}
        spec = self.spec(stand)
        print(f"SOURCE-DAGS {self.engine.name}: {json.dumps(spec)}", flush=True)
        (stand.rivet_dir / "configs" / f"source_dags_{self.engine.name}.json").write_text(json.dumps(spec))

    def adopt(self, stand: Any, v: Any) -> None:
        """Each unpaused DAG's `init` run, its config, and the DAG the next parse builds from it (`shape`); then what the
        cycles run and grade, from the configs init wrote."""
        eng = self.engine.name
        if self.refusal_dag:
            self.grade_refusal(stand, v)
        built: dict[str, list[str]] = {}
        for kind, dag in self.source_dags.items():
            name, facts = self.names[kind], self.facts[kind]
            run = wait_first_run(stand, dag)
            facts["run"], run_id = (run[1], run[0]) if run else ("not run", "")
            facts["log"] = (task_log(stand, dag, run_id, "init") or "") if run_id else ""
            config = self.host(stand, f"{CONFIG_DIR}/{name}.yaml")
            facts["config"] = config.read_text() if config.exists() else None
            facts["db_after"] = self.state_db_exists(stand, name)
            print(f"INIT-DAG {dag} {facts['run']} config:\n{facts['config']}", flush=True)
            if facts["run"] != "success" or not facts["config"]:
                print(f"INIT-DAG {dag} log:\n{facts['log'][-4000:]}", flush=True)
                v.check(f"{eng}/source-dags/{kind}/init", init_problems, facts, name, eng in ODBC)
                facts["graded"] = True
                continue
            ids = v.guard(f"{eng}/source-dags/{kind}/shape", wait_rebuilt, stand, dag)
            if ids is None:
                continue
            print(f"SHAPE {dag} {ids}", flush=True)
            v.check(
                f"{eng}/source-dags/{kind}/shape", shape_problems, ids, facts["config"], kind, GROUPED[eng], ok=", ".join(ids)
            )
            self.texts[kind] = facts["config"]
            self.shape[kind] = read_config(facts["config"], list(self.engine.tables))
            built[dag] = ids
        self.plan(v, built)

    def plan(self, v: Any, built: Mapping[str, list[str]]) -> None:
        """What the cycles run and grade: the DAGs built, their rivet tasks (a refused stream's apart), the targets."""
        eng = self.engine.name
        empty = {"exports": [], "streams": [], "extra": []}
        batch, cdc = self.shape.get("batch", empty), self.shape.get("cdc", empty)
        tables = sorted(self.engine.tables)
        want_batch = tables if "batch" in self.texts else []
        want_cdc = tables if "cdc" in self.texts else []
        captured = sorted(short(t) for s in cdc["streams"] for t in s["tables"])
        v.grade(
            f"{eng}/source-dags/tables",
            [f"init wrote batch exports {batch['exports']}, streams {cdc['streams']}, others {batch['extra'] + cdc['extra']}"]
            if sorted(batch["exports"]) != want_batch or captured != want_cdc or batch["extra"] + cdc["extra"]
            else [],
            ok=f"batch {batch['exports']}, streams {[s['name'] for s in cdc['streams']]}",
        )
        self.targets = {
            "batch": {e: e for e in batch["exports"]},
            "cdc": {short(t): warehouse_table(t) for s in cdc["streams"] for t in s["tables"]},
        }
        loaded = {k for k, text in self.texts.items() if (yaml.safe_load(text) or {}).get("load")}
        self.kinds = tuple(k for k in KINDS[eng] if k in loaded)
        self.file_kinds = tuple(k for k in KINDS[eng] if k in self.texts and k not in loaded)
        refused = CDC_REFUSED.get(eng, {})
        self.tasks, self.refused = {}, {}
        for dag, ids in built.items():
            rivet = [t for t in ids if t != WATCHER]
            stops = []
            if dag == self.source_dags.get("cdc"):
                for stream in cdc["streams"]:
                    table = short(stream["tables"][0])
                    if table in refused:
                        group = f"{stream['name']}." if len(cdc["streams"]) > 1 else ""
                        stops += [t for t in rivet if t.startswith(group) and t.endswith("run")]
                        self.refused_tables.update({t: table for t in rivet if t.startswith(group) and t.endswith("run")})
            self.tasks[dag] = [t for t in rivet if t not in stops and not any(t.startswith(s.removesuffix("run")) for s in stops)]
            if stops:
                self.refused[dag] = stops
        self.dags = list(built)
        self.may_skip = {
            dag: tuple(sorted({t[: t.index("compact.") + len("compact.")] for t in ids if "compact." in t}))
            for dag, ids in built.items()
        }

    def grade_refusal(self, stand: Any, v: Any) -> None:
        cell = f"{self.engine.name}/source-dags/odbc-refused"
        run = wait_first_run(stand, self.refusal_dag, timeout=900)
        run_id, state = run if run else ("", None)
        tries = (v.guard(cell + "/tries", stand.tries, self.refusal_dag, run_id, "init") or []) if run_id else []
        try:
            payload = stand.xcom(self.refusal_dag, run_id, "init")
        except Exception as exc:  # no XCom: the cell says so through the code it lacks
            payload = None
            print(f"ODBC-REFUSED no XCom: {exc}", flush=True)
        log = task_log(stand, self.refusal_dag, run_id, "init") if run_id else None
        print(f"ODBC-REFUSED run {state}, tries {[t.get('state') for t in tries]}, {json.dumps(payload)}", flush=True)
        v.check(cell, refusal_problems, state, tries, payload, log, ok=f"[{REFUSAL_CODE}] {REFUSAL_TEXT}")

    def grade(self, stand: Any, v: Any, name: str, run_id: str = "") -> None:
        if name == "initial":
            self.grade_init(stand, v)
        super().grade(stand, v, name, run_id)
        for kind in self.file_kinds:
            self.grade_files(stand, v, name, kind)
        for dag, stops in self.refused.items():
            states = v.guard(f"{name}/{dag}/task-states", stand.tasks, dag, run_id) or {}
            for task in stops:
                table = self.refused_tables[task]
                code, documented = CDC_REFUSED[self.engine.name][table]
                try:
                    payload = stand.xcom(dag, run_id, task)
                except Exception as exc:  # no XCom: the cell says so through the code it lacks
                    payload = None
                    print(f"REFUSED no XCom of {dag} {task}: {exc}", flush=True)
                v.check(
                    f"{name}/{self.engine.name}/source-dags/cdc/{table}/known-limit",
                    stream_refused_problems,
                    states.get(task, {}),
                    payload,
                    code,
                    ok=f"[{code}]: {documented}",
                )

    def grade_init(self, stand: Any, v: Any) -> None:
        """After the first cycle: each source's init cell, with its CDC checkpoints in the state folder."""
        eng = self.engine.name
        for kind, facts in self.facts.items():
            if facts.get("graded") or "run" not in facts:
                continue
            name = self.names[kind]
            checkpoints = {}
            doc = yaml.safe_load(facts.get("config") or "") or {}
            for e in doc.get("exports") or []:
                table = short(str(e.get("table") or ""))
                place = (e.get("cdc") or {}).get("checkpoint")
                if e.get("mode") != "cdc" or not place or table in CDC_REFUSED.get(eng, {}):
                    continue
                path = self.host(stand, place) if place.startswith("/") else self.host(stand, f"{STATE_ROOT}/{name}") / place
                checkpoints[f"{name}/{place}"] = path.exists()
            facts["checkpoints"] = checkpoints
            v.check(
                f"{eng}/source-dags/{kind}/init",
                init_problems,
                facts,
                name,
                eng in ODBC,
                ok=f"{name}.yaml written, {state_database(name)} created by init, "
                f"{'odbc.ini' if eng in ODBC else 'Connection'} [{name}], checkpoints {sorted(checkpoints) or 'none (batch)'}",
            )

    def batch_mode(self, export: str) -> str | None:
        doc = yaml.safe_load(self.texts.get("batch") or "") or {}
        return next((e.get("mode") for e in doc.get("exports") or [] if e.get("name") == export), None)

    def kept_by_batch(self, cycle: str, table: str) -> int:
        """A `full` export (init's suggestion for a small table, build_source_dags passes no `--mode`) reads every row:
        its target keeps no deleted key."""
        return 0 if self.batch_mode(table) == "full" else super().kept_by_batch(cycle, table)

    def expected_rows(
        self, kind: str, task: str, source: dict[str, list[dict[str, Any]]], ops: dict[str, dict[str, int]], baseline: bool
    ) -> int | None:
        group, _, unit = task.partition(".")
        if kind == "batch" and group in ("apply", "load") and self.batch_mode(unit) == "full":
            return len(source[unit])
        return super().expected_rows(kind, task, source, ops, baseline)

    def fetch(self, stand: Any, parts: Sequence[str]) -> int:
        """Copy Parquet parts from the bucket to the stand's folder, by their path in the bucket."""
        for part in parts:
            dest = stand.dir / "source-dags-parts" / part.removeprefix("gs://")
            dest.parent.mkdir(parents=True, exist_ok=True)
            sh(["gcloud", "storage", "cp", part, str(dest)], timeout=300)
            self.local[part] = dest
        return len(parts)

    def grade_files(self, stand: Any, v: Any, name: str, kind: str) -> None:
        """A CDC config with no `load:` block (Oracle): its parts in the bucket, each table's snapshot with every
        captured change replayed in SCN order equals the source (`oracle.cdc_replay`), and the cycle's change parts
        carry exactly its change events (`oracle.cdc_ops`)."""
        eng = self.engine.name
        listing = v.guard(f"{name}/{eng}/files/{kind}/listing", self.listing, kind)
        if listing is None:
            return
        parts = sorted(p for p in listing if p.endswith(".parquet"))
        new = [p for p in parts if p not in self.seen]
        print(f"INVENTORY {name} {eng} {kind} files: {len(parts)} part(s), new {new}", flush=True)
        if v.guard(f"{name}/{eng}/files/{kind}/fetch", self.fetch, stand, new) is None:
            return
        self.seen |= set(new)
        source = v.guard(f"{name}/{eng}/files/{kind}/source", self.source_rows, stand)
        if source is None:
            return
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'")
            for table, cols in self.engine.tables.items():
                if table in CDC_REFUSED.get(eng, {}):
                    continue
                mine = [p for p in parts if part_table(p, list(self.engine.tables)) == table]
                changes = {p for p in mine if is_change_part(con, p, self.local[p])}
                snapshot = [str(self.local[p]) for p in mine if p not in changes]
                cycle = [str(self.local[p]) for p in mine if p in changes and p in new]
                self.history.setdefault(table, []).append(cycle)
                cell = f"{name}/{eng}/files/{kind}/{table}"
                v.check(cell + "/operations", oracle.cdc_ops, con, cycle, FILE_OPS.get(name, {}).get(table, {}), cell)
                relation = source_relation(con, table, source[table], cols)
                v.check(
                    cell + "/replay-equals-source",
                    oracle.cdc_replay,
                    con,
                    snapshot,
                    self.history[table],
                    relation,
                    cols,
                    self.engine.key,
                    ORACLE_POS,
                    ok=f"{len(snapshot)} snapshot part(s), {sum(map(len, self.history[table]))} change part(s), "
                    f"{len(source[table])} row(s)",
                )
        finally:
            con.close()


def legs(cloud: warehouse.Cloud | None, stack: str, names: Sequence[str]) -> list[SourceDagsLeg]:
    return [SourceDagsLeg(cloud, stack, ENGINES[n]) for n in names] if cloud else []
