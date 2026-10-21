"""The migration leg (`--migrate-from <rivet 0.32.1>`): a MySQL CDC stream run under the operator on SQLite state with
rivet 0.32.1, carried to PostgreSQL state with this binary's `rivet state copy` (rivet #657), and run on.

What a client on 0.32.1 does to move: stop the stream's runs, run `rivet state copy` against the config's copy in the
state_dir (the SQLite file beside it), point the DAG's `RIVET_STATE_URL` at a Connection of the PostgreSQL state, run.
The CDC position is a FILE (`cdc.checkpoint`, resolved from the config copy's directory, the state_dir), not a state
row: the copy leaves it where it is and the next run continues from it.

The stream reads a database of its own (`rivet_mig`) on the stack's MySQL, into a bucket and a dataset of its own. Its
config is `rivet init` of rivet 0.32.1 (it keeps SQLite beside `-o` whatever `RIVET_STATE_URL` says, so init writes into
a scratch folder and the DAG reads its file copied unchanged). Three DAGs on one config and one state_dir:
`gate_mig_mysql_sqlite` (rivet 0.32.1, SQLite), `gate_mig_mysql_pg` (this binary, the PostgreSQL Connection) and
`gate_mig_mysql_back` (this binary, SQLite again: the way back, measured, not graded).

Every command a client runs is printed as a `COMMAND` line, verbatim but for the secrets, which reach the commands only
through variables.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import duckdb

from . import warehouse
from .sources import MYSQL
from .stand import sh, state_conn_id, state_db

ENGINE = "mysql"
DATABASE = "rivet_mig"
CONN = "rivet_gate_mysql_mig"
URL_ENV = "RIVET_GATE_MYSQL_MIG_URL"
STATE_DB = state_db(ENGINE, "mig")
STATE_CONN = state_conn_id(ENGINE, "mig")
OLD_BIN = "/opt/rivet/bin/rivet-0.32.1"
CONFIG_DIR = "/opt/rivet/configs/mysql_mig"
INIT_DIR = "/opt/rivet/configs/mysql_mig_init"
STATE_DIR = "/opt/rivet/state/mysql_mig"
DAGS = {"sqlite": "gate_mig_mysql_sqlite", "pg": "gate_mig_mysql_pg", "back": "gate_mig_mysql_back"}
#: The incremental batch pipeline of the same tables, migrated the same way (its own config, state_dir and state).
B_STATE_DB = state_db(ENGINE, "migb")
B_STATE_CONN = state_conn_id(ENGINE, "migb")
B_STATE_DIR = "/opt/rivet/state/mysql_migb"
B_DAGS = {"sqlite": "gate_migb_mysql_sqlite", "pg": "gate_migb_mysql_pg", "back": "gate_migb_mysql_back"}
#: The batch steps: updates and inserts only (an incremental extract cannot see a delete), after the stream's steps.
B_STEPS: dict[str, tuple[str, int]] = {
    "sqlite-2": ("UPDATE mig_customers SET tier = 'b1' WHERE id IN (14, 15);\n"
                 "INSERT INTO mig_orders (customer_id, amount, status) VALUES (5, 9.10, 'new');", 3),
    "pg-1": ("UPDATE mig_customers SET tier = 'b2' WHERE id = 16;", 1),
    "back": ("UPDATE mig_customers SET tier = 'b3' WHERE id = 17;", 1),
}  # fmt: skip
#: The stream's tables: the scenario's under names of their own. rivet refuses an unqualified CDC table another database
#: of the server also holds (`RIVET_SOURCE_CDC_TABLE_UNRESOLVED`, the binlog is server-wide), and init writes them
#: unqualified, so the migration database may not reuse the leg's names.
TABLES = {name.replace("gate_", "mig_"): cols for name, cols in MYSQL.tables.items()}
#: State tables not compared by count: schema bookkeeping (the copy brings the rows to this release's schema) and leases.
UNCOMPARED = frozenset({"schema_version", "rivet_schema_version", "state_lease"})
#: The changes of each step, and the change events they make (every update changes a value; deletes are by key).
STEPS: dict[str, tuple[str, dict[str, dict[str, int]]]] = {
    "sqlite-2": (
        "UPDATE mig_orders SET status = 'paid' WHERE id BETWEEN 1 AND 3;\n"
        "INSERT INTO mig_orders (customer_id, amount, status) VALUES (1, 5.55, 'new'), (2, 6.66, 'new');\n"
        "DELETE FROM mig_orders WHERE id = 4999;",
        {"mig_orders": {"update": 3, "insert": 2, "delete": 1}},
    ),
    "before-switch": (
        "UPDATE mig_customers SET tier = 'moving' WHERE id IN (7, 8);\nDELETE FROM mig_orders WHERE id = 4998;",
        {"mig_customers": {"update": 2}, "mig_orders": {"delete": 1}},
    ),
    "after-switch": (
        "INSERT INTO mig_orders (customer_id, amount, status) VALUES (3, 7.77, 'new');\n"
        "UPDATE mig_orders SET status = 'moved' WHERE id = 10;\nDELETE FROM mig_orders WHERE id = 4997;",
        {"mig_orders": {"insert": 1, "update": 1, "delete": 1}},
    ),
    "pg-2": (
        "UPDATE mig_customers SET tier = 'settled' WHERE id = 9;\nDELETE FROM mig_orders WHERE id = 4996;",
        {"mig_customers": {"update": 1}, "mig_orders": {"delete": 1}},
    ),
    "back": (
        "UPDATE mig_customers SET tier = 'back' WHERE id = 11;\nINSERT INTO mig_orders (customer_id, amount, status) "
        "VALUES (4, 8.88, 'new');",
        {"mig_customers": {"update": 1}, "mig_orders": {"insert": 1}},
    ),
}


def events(ops: dict[str, dict[str, int]]) -> int:
    return sum(n for per in ops.values() for n in per.values())


# --- Pure oracles -----------------------------------------------------------------------------------------------------


def run_problems(
    state: str | None, tasks: dict[str, dict[str, Any]], payload: dict[str, Any] | None, rows: int | None
) -> list[str]:
    """The stream's run and every task of it succeeded, and its run moved exactly `rows` rows (the changes made since the
    run before: fewer is a gap, more a duplicate); `rows` None: not counted (the baseline)."""
    problems = [] if state == "success" else [f"run {state}"]
    bad = {t: i.get("state") for t, i in tasks.items() if i.get("state") not in ("success", "skipped")}
    if bad or not tasks:
        problems.append(f"tasks {bad or 'none'}")
    error = (payload or {}).get("error") or {}
    if error:
        problems.append(f"rivet stopped [{error.get('code')}] exit {(payload or {}).get('exit_status')}")
    elif rows is not None:
        moved = sum(u["rows"] for u in (payload or {}).get("units") or [] if type(u.get("rows")) is int)
        if moved != rows:
            kind = "a gap" if moved < rows else "a duplicate"
            problems.append(f"the run moved {moved} row(s), the source changed {rows} ({kind})")
    return problems


def state_problems(sqlite: dict[str, Any], pg: dict[str, Any]) -> list[str]:
    """The PostgreSQL state holds what the SQLite state held: each table's row count (bookkeeping tables aside) and the
    stored cursors, row for row."""
    problems = []
    counts, got = sqlite.get("counts", {}), pg.get("counts", {})
    if not counts or not any(counts.values()):
        return ["the SQLite state holds no rows: nothing was carried, the cell proves nothing"]
    for table in sorted(set(counts) - UNCOMPARED):
        if table not in got:
            problems.append(f"{table}: {counts[table]} row(s) in SQLite, no table in PostgreSQL")
        elif got[table] != counts[table]:
            problems.append(f"{table}: {counts[table]} row(s) in SQLite, {got[table]} in PostgreSQL")
    if sqlite.get("cursors") != pg.get("cursors"):
        problems.append(f"stored cursors: SQLite {sqlite.get('cursors')}, PostgreSQL {pg.get('cursors')}")
    return problems


def checkpoint_problems(before: dict[str, str], after: dict[str, str], what: str) -> list[str]:
    """The CDC checkpoint files (and their owner records) are byte for byte what they were."""
    if not before:
        return ["no checkpoint file in the state_dir: nothing to keep, the cell proves nothing"]
    return [] if before == after else [f"{what}: checkpoint files {before} became {after}"]


def stream_equals(
    got: dict[str, list[dict[str, Any]]], source: dict[str, list[dict[str, Any]]], cols: dict[str, list[str]]
) -> list[str]:
    """Every table's BigQuery base, without the rows flagged deleted, equals the source value by value (a duplicate
    key is an extra row; a delete not applied is a key the source lacks)."""
    problems = []
    for table in sorted(source):
        problems += warehouse.current_state(got.get(table, []), source[table], cols[table], "id", 0, table, flagged=True)
    return problems


def load_rows_problems(loaded: dict[str, int | None], expected: dict[str, int]) -> list[str]:
    """Each table's load moved exactly the rows of the runs it had not loaded (a run loaded again is a duplicate)."""
    return [
        f"load.{t} moved {loaded.get(t)} row(s), the runs it had not loaded hold {n}"
        for t, n in sorted(expected.items())
        if loaded.get(t) != n
    ]


def duplicate_keys(counts: dict[str, tuple[int, int]]) -> list[str]:
    """Each base holds every key once (`counts`: table -> (rows, distinct keys)), flagged-deleted rows included."""
    problems = [f"{t}: {rows} row(s), {keys} key(s)" for t, (rows, keys) in sorted(counts.items()) if rows != keys]
    return problems or ([] if counts else ["no base read"])


def full_copies(names: Sequence[str]) -> list[str]:
    """The provider's copies of the whole config among the state_dir's files: `<stem>.<hash>.yaml`, not the copy cut
    down to one export for a load (`<stem>.<hash>--<export>.yaml`, which a rivet without `--export` on load needs)."""
    return [n for n in names if re.fullmatch(r"[^.]+\.[0-9a-f]{8}\.yaml", n)]


def fingerprints(folder: Path) -> dict[str, str]:
    """sha256 of every checkpoint file and owner record under `folder/cdc`."""
    root = folder / "cdc"
    return {
        str(p.relative_to(folder)): hashlib.sha256(p.read_bytes()).hexdigest()[:16] for p in sorted(root.glob("*")) if p.is_file()
    }


READ_SQLITE = (
    "import json, sqlite3, sys; c = sqlite3.connect('file:' + sys.argv[1] + '?mode=ro', uri=True); "
    "ts = [r[0] for r in c.execute(\"SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'\")]; "
    "print(json.dumps({'counts': {t: c.execute(f'SELECT count(*) FROM \"{t}\"').fetchone()[0] for t in ts}, "
    "'cursors': sorted([list(r) for r in c.execute('SELECT export_name, last_cursor_value FROM export_state')])}))"
)


# --- The leg ----------------------------------------------------------------------------------------------------------


class Migration:
    """The migration leg of one stack, beside the MySQL source leg (whose container, client and cloud it uses)."""

    def __init__(self, src: Any) -> None:
        self.src = src
        tag = f"{src.cloud.tag}-{src.stack.replace('.', '')}"
        self.bucket = f"rivet-afgate-{tag}-mysqlmig-cdc"
        self.dataset = f"{src.cloud.dataset(src.stack)}_mysqlmig_cdc"
        self.b_bucket = f"rivet-afgate-{tag}-mysqlmig-batch"
        self.b_dataset = f"{src.cloud.dataset(src.stack)}_mysqlmig_batch"
        self.stream: dict[str, Any] = {}
        self.b_exports: list[str] = []
        self.commands: list[str] = []

    def command(self, text: str) -> None:
        self.commands.append(text)
        print(f"COMMAND {text}", flush=True)

    def sql(self, stand: Any, text: str) -> str:
        return self.src.sql(stand, f"USE {DATABASE};\n{text}")

    def prepare(self, stand: Any) -> None:
        """The database, the bucket, the dataset, the empty PostgreSQL state; rivet 0.32.1's init; the DAGs' shape."""
        from .sources import read_config

        shown = stand.compose("exec", "-T", "airflow-worker", OLD_BIN, "--version").stdout.strip()
        print(f"MIGRATE old binary in the image: {shown}", flush=True)
        self.src.sql(stand, f"CREATE DATABASE {DATABASE};\nGRANT ALL ON {DATABASE}.* TO 'rivet'@'%';")
        self.sql(stand, self.src.engine.seed.replace("gate_", "mig_"))
        sh(["gcloud", "storage", "buckets", "create", f"gs://{self.bucket}", f"--project={self.src.cloud.project}",
            f"--location={self.src.cloud.location}", "--uniform-bucket-level-access"], timeout=300)  # fmt: skip
        warehouse._bq(
            f"--location={self.src.cloud.location}", "mk", "-f", "--dataset", f"{self.src.cloud.project}:{self.dataset}"
        )
        stand.make_leg_state_db(STATE_DB)
        for sub in ("configs/mysql_mig", "configs/mysql_mig_init", "state/mysql_mig"):
            (stand.rivet_dir / sub).mkdir(parents=True, exist_ok=True)
        argv = [
            OLD_BIN, "init", "--source-env", URL_ENV, "--tls", "disable", "--mode", "cdc", "--gcs-bucket", self.bucket,
            "--bigquery-project", self.src.cloud.project, "--bigquery-dataset", self.dataset,
            "--include", *TABLES, "-o", f"{INIT_DIR}/rivet.yaml",
        ]  # fmt: skip
        self.command(f"{URL_ENV}=<mysql url> " + " ".join(argv))
        stand.compose("exec", "-T", "-e", URL_ENV, "-e", "RIVET_STATE_URL", "airflow-worker", *argv,
                      extra_env={URL_ENV: self.url(stand), "RIVET_STATE_URL": ""})  # fmt: skip
        text = (stand.rivet_dir / "configs" / "mysql_mig_init" / "rivet.yaml").read_text()
        print(f"INIT mysql migrate (rivet 0.32.1) config:\n{text}", flush=True)
        # The DAG's config: init's file, unchanged, in a folder without the SQLite state init left beside it.
        shutil.copyfile(
            stand.rivet_dir / "configs" / "mysql_mig_init" / "rivet.yaml",
            stand.rivet_dir / "configs" / "mysql_mig" / "rivet.yaml",
        )
        streams = read_config(text, list(TABLES))["streams"]
        if len(streams) != 1:
            raise ValueError(f"rivet 0.32.1's init wrote streams {streams}, expected one")
        self.stream = streams[0]
        self.b_exports = self.prepare_batch(stand)
        shape = {"config": f"{CONFIG_DIR}/rivet.yaml", "state_dir": STATE_DIR, "stream": self.stream, "dags": DAGS,
                 "old_bin": OLD_BIN, "url_env": URL_ENV, "conn": CONN, "state_conn": STATE_CONN,
                 "batch": {"config": f"{B_STATE_DIR}/rivet.yaml", "state_dir": B_STATE_DIR, "exports": self.b_exports,
                           "dags": B_DAGS, "state_conn": B_STATE_CONN}}  # fmt: skip
        (stand.rivet_dir / "configs" / "mysql_migrate.json").write_text(json.dumps(shape))

    def prepare_batch(self, stand: Any) -> list[str]:
        """rivet 0.32.1's init of the incremental batch config over the same tables (its own bucket, dataset, state)."""
        from .sources import read_config

        sh(["gcloud", "storage", "buckets", "create", f"gs://{self.b_bucket}", f"--project={self.src.cloud.project}",
            f"--location={self.src.cloud.location}", "--uniform-bucket-level-access"], timeout=300)  # fmt: skip
        warehouse._bq(
            f"--location={self.src.cloud.location}", "mk", "-f", "--dataset", f"{self.src.cloud.project}:{self.b_dataset}"
        )
        stand.make_leg_state_db(B_STATE_DB)
        (stand.rivet_dir / "state" / "mysql_migb").mkdir(parents=True, exist_ok=True)
        argv = [
            OLD_BIN, "init", "--source-env", URL_ENV, "--tls", "disable", "--mode", "incremental", "--gcs-bucket", self.b_bucket,
            "--bigquery-project", self.src.cloud.project, "--bigquery-dataset", self.b_dataset,
            "--include", *TABLES, "-o", f"{B_STATE_DIR}/rivet.yaml",
        ]  # fmt: skip
        # Into the state_dir itself: 0.32.1 keeps SQLite beside -o, and init records the `query:` exports' primary keys
        # there, which their loads need (`pk: auto`); the provider then runs that file as it is (no copy). Live: written
        # into a scratch folder, every load of the batch pipeline failed, from its first run on.
        self.command(f"(batch) {URL_ENV}=<mysql url> " + " ".join(argv))
        stand.compose("exec", "-T", "-e", URL_ENV, "-e", "RIVET_STATE_URL", "airflow-worker", *argv,
                      extra_env={URL_ENV: self.url(stand), "RIVET_STATE_URL": ""})  # fmt: skip
        text = (stand.rivet_dir / "state" / "mysql_migb" / "rivet.yaml").read_text()
        print(f"INIT mysql migrate batch (rivet 0.32.1) config:\n{text}", flush=True)
        exports = read_config(text, list(TABLES))["exports"]
        if sorted(exports) != sorted(TABLES):
            raise ValueError(f"rivet 0.32.1's batch init wrote exports {exports}, expected {sorted(TABLES)}")
        return exports

    @property
    def b_tasks(self) -> list[str]:
        return [f"{g}.{e}" for e in self.b_exports for g in ("apply", "load", "compact")]

    def url(self, stand: Any) -> str:
        """The URL init and `rivet state copy` read the migration database with (inside the stack's network)."""
        return self.src.engine.url(stand.secrets).rsplit("/", 1)[0] + f"/{DATABASE}"

    @property
    def dags(self) -> list[str]:
        return [*DAGS.values(), *B_DAGS.values()]

    @property
    def tasks(self) -> list[str]:
        return ["run", *(f"{g}.{t}" for t in self.stream["tables"] for g in ("load", "compact"))]

    # --- reads ---

    def source_rows(self, stand: Any) -> dict[str, list[dict[str, Any]]]:
        port = stand.host_port(self.src.engine.service, self.src.engine.port)
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'; INSTALL mysql; LOAD mysql")
            dsn = self.src.engine.duck_dsn.format(port=port, password=stand.secrets.mysql).replace(
                "database=rivet", f"database={DATABASE}"
            )
            con.execute(f"ATTACH '{dsn}' AS src (TYPE mysql, READ_ONLY)")
            out = {}
            for table, cols in TABLES.items():
                cur = con.execute(f"SELECT {', '.join(cols)} FROM src.{table}")
                names = [d[0] for d in cur.description]
                out[table] = [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
            return out
        finally:
            con.close()

    def warehouse_rows(self) -> dict[str, list[dict[str, Any]]]:
        client = self.src._client()
        return {
            t: [dict(r.items()) for r in client.query(f"SELECT * FROM `{self.src.cloud.project}.{self.dataset}.{t}`").result()]
            for t in TABLES
        }

    def sqlite_state(self, stand: Any) -> dict[str, Any]:
        out = stand.compose("exec", "-T", "airflow-worker", "python", "-c", READ_SQLITE, f"{STATE_DIR}/.rivet_state.db").stdout
        return json.loads(out)

    def pg_state(self, stand: Any) -> dict[str, Any]:
        names = [
            line
            for line in stand.leg_state_sql(
                STATE_DB, "SELECT tablename FROM pg_tables WHERE schemaname = current_schema();"
            ).splitlines()
            if line
        ]
        counts = {t: int(stand.leg_state_sql(STATE_DB, f'SELECT count(*) FROM "{t}";').strip()) for t in names}
        rows = stand.leg_state_sql(STATE_DB, "SELECT export_name, last_cursor_value FROM export_state ORDER BY 1;")
        cursors = sorted([[a, b or None] for a, b in (line.split("|", 1) for line in rows.splitlines() if "|" in line)])
        return {"counts": counts, "cursors": cursors}

    def checkpoints(self, stand: Any) -> dict[str, str]:
        return fingerprints(stand.rivet_dir / "state" / "mysql_mig")

    # --- the steps ---

    def run(
        self, stand: Any, v: Any, wait_runs: Callable[..., Any], step: str, dag: str, rows: int | None, grade: bool = True
    ) -> list[str]:
        """Run `dag` once; with `grade`, its cells: the run moved `rows`, and BigQuery equals the source."""
        run_id = f"gate_mig_{step}"
        stand.trigger(dag, run_id)
        state = wait_runs(stand, [dag], run_id).get(dag)
        tasks = {
            t: i for t, i in (v.guard(f"migrate/{step}/task-states", stand.tasks, dag, run_id) or {}).items() if t in self.tasks
        }
        payload = None
        if tasks.get("run", {}).get("state") in ("success", "failed"):
            payload = v.guard(f"migrate/{step}/xcom", stand.xcom, dag, run_id, "run")
        print(f"MIGRATE {step} {dag}: run {state}, {json.dumps({t: i.get('state') for t, i in tasks.items()})} "
              f"{json.dumps(payload, default=str)}", flush=True)  # fmt: skip
        source = v.guard(f"migrate/{step}/source", self.source_rows, stand)
        cols = dict(TABLES)
        problems = run_problems(state, tasks, payload, rows)
        if not grade:
            got = self.warehouse_rows()
            return problems + (stream_equals(got, source, cols) if source else ["no source read"])
        v.check(
            f"migrate/mysql/cdc/{step}/run",
            run_problems,
            state,
            tasks,
            payload,
            rows,
            ok=f"{rows} row(s)" if rows is not None else "baseline",
        )
        if source is not None:
            v.check(
                f"migrate/mysql/cdc/{step}/current-equals-source",
                lambda: stream_equals(self.warehouse_rows(), source, cols),
                ok=", ".join(f"{t} {len(r)} row(s)" for t, r in sorted(source.items())),
            )
        return problems

    def play(self, stand: Any, v: Any, wait_runs: Callable[..., Any]) -> None:
        """SQLite under 0.32.1 for two runs, the copy, PostgreSQL under this binary for two runs (changes made before and
        after the switch), then the way back measured."""
        self.command(f"DAG {DAGS['sqlite']}: rivet_bin={OLD_BIN}, env RIVET_STATE_URL='' (SQLite beside the config copy in "
                     f"{STATE_DIR}), single_host=True, state_dir={STATE_DIR}")  # fmt: skip
        self.run(stand, v, wait_runs, "sqlite-1", DAGS["sqlite"], None)
        sql, ops = STEPS["sqlite-2"]
        self.sql(stand, sql)
        self.run(stand, v, wait_runs, "sqlite-2", DAGS["sqlite"], events(ops))
        sqlite = v.guard("migrate/mysql/cdc/sqlite-state", self.sqlite_state, stand)
        ckpt = self.checkpoints(stand)
        print(f"MIGRATE SQLite state {json.dumps(sqlite)}; checkpoints {ckpt}", flush=True)
        before_sql, before_ops = STEPS["before-switch"]
        self.sql(stand, before_sql)
        names = sorted(p.name for p in (stand.rivet_dir / "state" / "mysql_mig").glob("rivet.*.yaml"))
        copies = full_copies(names)
        if len(copies) != 1:
            v.grade("migrate/mysql/cdc/state-copy", [f"whole config copies in the state_dir: {copies} of {names}, expected one"])
            return
        argv = ["rivet", "state", "copy", "-c", f"{STATE_DIR}/{copies[0]}"]
        self.command(
            f"RIVET_STATE_URL=postgresql://rivet_state:<password>@postgres:5432/{STATE_DB}?sslmode=disable {' '.join(argv)}"
        )
        done = stand.compose("exec", "-T", "-e", "RIVET_STATE_URL", "-e", URL_ENV, "airflow-worker", *argv, check=False,
                             extra_env={"RIVET_STATE_URL": stand.leg_state_url(STATE_DB), URL_ENV: self.url(stand)})  # fmt: skip
        said = ((done.stdout or "") + (done.stderr or "")).strip()
        print(f"MIGRATE state copy -> exit {done.returncode}\n{said}", flush=True)
        v.grade(
            "migrate/mysql/cdc/state-copy",
            [] if done.returncode == 0 else [f"exit {done.returncode}: {said[-1500:]}"],
            ok="exit 0",
        )
        if sqlite is not None:
            v.check("migrate/mysql/cdc/state-equals-sqlite", lambda: state_problems(sqlite, self.pg_state(stand)),
                    ok=f"{sum(sqlite['counts'].values())} row(s) in {len(sqlite['counts'])} table(s), cursors equal")  # fmt: skip
        v.check("migrate/mysql/cdc/checkpoint-untouched-by-copy", checkpoint_problems, ckpt, self.checkpoints(stand), "the copy",
                ok=f"{sorted(ckpt)}")  # fmt: skip
        self.command(f"DAG {DAGS['pg']}: rivet_bin=rivet (main), env_from_connections RIVET_STATE_URL={STATE_CONN} "
                     f"(postgresql://rivet_state:...@postgres:5432/{STATE_DB}?sslmode=disable), state_dir={STATE_DIR}")  # fmt: skip
        after_sql, after_ops = STEPS["after-switch"]
        self.sql(stand, after_sql)
        held = self.checkpoints(stand)
        v.check(
            "migrate/mysql/cdc/checkpoint-unchanged-at-switch", checkpoint_problems, ckpt, held, "the switch", ok="same position"
        )
        self.run(stand, v, wait_runs, "pg-1", DAGS["pg"], events(before_ops) + events(after_ops))
        pg_sql, pg_ops = STEPS["pg-2"]
        self.sql(stand, pg_sql)
        self.run(stand, v, wait_runs, "pg-2", DAGS["pg"], events(pg_ops))
        self.wayback(stand, v, wait_runs)
        self.batch(stand, v, wait_runs)
        print("COMMANDS\n" + "\n".join(self.commands), flush=True)

    def objects(self) -> list[str]:
        out = sh(["gcloud", "storage", "ls", "-r", f"gs://{self.bucket}/**"], timeout=300, check=False).stdout
        return [line.strip() for line in out.splitlines() if line.strip().startswith("gs://")]

    def bases(self, dataset: str, tables: Sequence[str]) -> tuple[dict[str, tuple[int, int]], set[str]]:
        """Each base's rows and distinct keys, and every object of the dataset (lower case)."""
        client = self.src._client()
        counts = {}
        for t in tables:
            row = next(
                iter(
                    client.query(
                        f"SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS k FROM `{self.src.cloud.project}.{dataset}.{t}`"
                    ).result()
                )
            )
            counts[t] = (row["n"], row["k"])
        names = {x.table_id.lower() for x in client.list_tables(f"{self.src.cloud.project}.{dataset}")}
        return counts, names

    def wayback(self, stand: Any, v: Any, wait_runs: Callable[..., Any]) -> None:
        """The way back: this binary on the SQLite file the copy left (its load ledger knows nothing of the runs loaded
        while on PostgreSQL), the whole DAG: run, then load and compact of each table. Graded as `wayback/mysql/cdc/*`."""
        back_sql, back_ops = STEPS["back"]
        self.sql(stand, back_sql)
        before = self.objects()
        print(f"MEASURE wayback/mysql/cdc/staged-before: {len([o for o in before if o.endswith('.parquet')])} Parquet, "
              f"{len([o for o in before if o.endswith('.json')])} JSON object(s) in gs://{self.bucket}: {before[:40]}", flush=True)  # fmt: skip
        self.command(f"(the way back) DAG {DAGS['back']}: rivet_bin=rivet (main), env RIVET_STATE_URL='' (the SQLite "
                     f"file the copy left), single_host=True, state_dir={STATE_DIR}")  # fmt: skip
        dag, run_id = DAGS["back"], "gate_mig_back"
        stand.trigger(dag, run_id)
        state = wait_runs(stand, [dag], run_id).get(dag)
        tasks = {t: i for t, i in (v.guard("wayback/task-states", stand.tasks, dag, run_id) or {}).items() if t in self.tasks}
        payloads = {}
        for t, i in tasks.items():
            if i.get("state") in ("success", "failed"):
                try:
                    payloads[t] = stand.xcom(dag, run_id, t)
                except Exception as exc:  # the cells read what is there
                    print(f"WAYBACK no XCom of {t}: {exc}", flush=True)
        for t, p in sorted(payloads.items()):
            units = [
                {k: u.get(k) for k in ("table", "status", "rows", "files", "skip_reason") if k in u} for u in p.get("units") or []
            ]
            print(f"MEASURE wayback/mysql/cdc/task {t}: {tasks[t].get('state')}, decision {p.get('decision')}, "
                  f"error {(p.get('error') or {}).get('code')}, units {units}", flush=True)  # fmt: skip
        after = self.objects()
        print(f"MEASURE wayback/mysql/cdc/staged-after: {len([o for o in after if o.endswith('.parquet')])} Parquet, "
              f"{len([o for o in after if o.endswith('.json')])} JSON object(s)", flush=True)  # fmt: skip
        v.check(
            "wayback/mysql/cdc/run",
            run_problems,
            state,
            tasks,
            payloads.get("run"),
            events(back_ops),
            ok=f"{events(back_ops)} row(s)",
        )
        loaded = {t.split(".", 1)[1]: (sum(u.get("rows") or 0 for u in p.get("units") or []) if p else None)
                  for t, p in payloads.items() if t.startswith("load.")}  # fmt: skip
        expected = {t: sum(back_ops.get(t, {}).values()) for t in TABLES}
        v.check("wayback/mysql/cdc/loads-moved-only-new-runs", load_rows_problems, loaded, expected, ok=f"{expected}")
        source = v.guard("wayback/source", self.source_rows, stand)
        if source is not None:
            v.check("wayback/mysql/cdc/current-equals-source", lambda: stream_equals(self.warehouse_rows(), source, dict(TABLES)),
                    ok=", ".join(f"{t} {len(r)} row(s)" for t, r in sorted(source.items())))  # fmt: skip
        found = v.guard("wayback/bases", self.bases, self.dataset, list(TABLES))
        if found is not None:
            counts, names = found
            print(f"MEASURE wayback/mysql/cdc/bases: {counts}; objects {sorted(names)}", flush=True)
            v.check("wayback/mysql/cdc/base-unique-keys", duplicate_keys, counts, ok=f"{counts}")
            v.check(
                "wayback/mysql/cdc/no-buffer-left",
                lambda: [p for t in TABLES for p in warehouse.compacted(names, t, t)],
                ok="each base, no __changes",
            )

    def batch(self, stand: Any, v: Any, wait_runs: Callable[..., Any]) -> None:
        """The incremental batch pipeline the same way: 0.32.1 on SQLite (baseline, a delta), `rivet state copy`, this
        binary on PostgreSQL (a delta), then back to the SQLite file (a delta): what it extracts again is measured;
        BigQuery is graded against the source and for duplicate keys (`wayback/mysql/batch/*`)."""
        tables = list(TABLES)
        self.run_batch(stand, v, wait_runs, "sqlite-1", B_DAGS["sqlite"], None)
        for step, dag in (("sqlite-2", B_DAGS["sqlite"]),):
            sql, rows = B_STEPS[step]
            self.sql(stand, sql)
            self.run_batch(stand, v, wait_runs, step, dag, rows)
        # The batch config is init's file in the state_dir itself: the SQLite state beside it is the one to copy.
        argv = ["rivet", "state", "copy", "-c", f"{B_STATE_DIR}/rivet.yaml"]
        self.command(
            f"(batch) RIVET_STATE_URL=postgresql://rivet_state:<password>@postgres:5432/{B_STATE_DB}?sslmode=disable {' '.join(argv)}"
        )
        done = stand.compose("exec", "-T", "-e", "RIVET_STATE_URL", "-e", URL_ENV, "airflow-worker", *argv, check=False,
                             extra_env={"RIVET_STATE_URL": stand.leg_state_url(B_STATE_DB), URL_ENV: self.url(stand)})  # fmt: skip
        said = ((done.stdout or "") + (done.stderr or "")).strip()
        print(f"MIGRATE batch state copy -> exit {done.returncode}\n{said}", flush=True)
        v.grade(
            "migrate/mysql/batch/state-copy",
            [] if done.returncode == 0 else [f"exit {done.returncode}: {said[-1500:]}"],
            ok="exit 0",
        )
        sql, rows = B_STEPS["pg-1"]
        self.sql(stand, sql)
        self.run_batch(stand, v, wait_runs, "pg-1", B_DAGS["pg"], rows)
        sql, rows = B_STEPS["back"]
        self.sql(stand, sql)
        moved = self.run_batch(stand, v, wait_runs, "back", B_DAGS["back"], None)
        print(f"MEASURE wayback/mysql/batch/re-exported: the extract after the way back moved {moved}; the source changed "
              f"{rows} row(s) since the last run (on PostgreSQL)", flush=True)  # fmt: skip
        found = v.guard("wayback/batch/bases", self.bases, self.b_dataset, tables)
        if found is not None:
            counts, names = found
            print(f"MEASURE wayback/mysql/batch/bases: {counts}; objects {sorted(names)}", flush=True)
            v.check("wayback/mysql/batch/base-unique-keys", duplicate_keys, counts, ok=f"{counts}")
            v.check("wayback/mysql/batch/no-buffer-left", lambda: [p for t in tables for p in warehouse.compacted(names, t, t)],
                    ok="each base, no __changes")  # fmt: skip

    def run_batch(
        self, stand: Any, v: Any, wait_runs: Callable[..., Any], step: str, dag: str, rows: int | None
    ) -> dict[str, Any]:
        """Run the batch DAG once: the run and its tasks succeed, the extracts moved `rows` (None: not counted), BigQuery
        equals the source. Returns what each extract reported (rows, files)."""
        run_id = f"gate_migb_{step}"
        stand.trigger(dag, run_id)
        state = wait_runs(stand, [dag], run_id).get(dag)
        tasks = {
            t: i
            for t, i in (v.guard(f"migrate/batch/{step}/task-states", stand.tasks, dag, run_id) or {}).items()
            if t in self.b_tasks
        }
        moved: dict[str, Any] = {}
        for t in [t for t in self.b_tasks if t.startswith("apply.") and tasks.get(t, {}).get("state") == "success"]:
            units = (v.guard(f"migrate/batch/{step}/xcom", stand.xcom, dag, run_id, t) or {}).get("units") or []
            moved[t] = {"rows": sum(u.get("rows") or 0 for u in units), "files": sum(u.get("files") or 0 for u in units)}
        total = sum(m["rows"] for m in moved.values()) if moved else None
        print(
            f"MIGRATE batch {step} {dag}: run {state}, {json.dumps({t: i.get('state') for t, i in tasks.items()})}, extracts {moved}",
            flush=True,
        )
        for t in [t for t, i in tasks.items() if i.get("state") == "failed"]:
            try:
                p = stand.xcom(dag, run_id, t)
                print(f"MIGRATE batch {step} {t} failed: {json.dumps(p.get('error'))} units {p.get('units')}", flush=True)
            except Exception as exc:  # the log says why there is none
                print(f"MIGRATE batch {step} {t} failed, no XCom: {exc}", flush=True)
        problems = [] if state == "success" else [f"run {state}"]
        bad = {t: i.get("state") for t, i in tasks.items() if i.get("state") not in ("success", "skipped")}
        if bad or not tasks:
            problems.append(f"tasks {bad or 'none'}")
        if rows is not None and total != rows:
            problems.append(f"the extracts moved {total} row(s), the source changed {rows}")
        family = "wayback" if step == "back" else "migrate"
        v.grade(f"{family}/mysql/batch/{step}/run", problems, ok=f"{total} row(s)")
        source = v.guard(f"migrate/batch/{step}/source", self.source_rows, stand)
        if source is not None:
            client = self.src._client()
            v.check(
                f"{family}/mysql/batch/{step}/current-equals-source",
                lambda: [p for t in TABLES for p in warehouse.current_state(
                    [dict(r.items()) for r in client.query(f"SELECT * FROM `{self.src.cloud.project}.{self.b_dataset}.{t}`").result()],
                    source[t], TABLES[t], "id", 0, t)],
                ok=", ".join(f"{t} {len(r)} row(s)" for t, r in sorted(source.items())),
            )  # fmt: skip
        return moved

    def cleanup(self) -> None:
        left = []
        for bucket, dataset in ((self.bucket, self.dataset), (self.b_bucket, self.b_dataset)):
            sh(["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{bucket}"], timeout=600, check=False)
            warehouse._bq("rm", "-r", "-f", "--dataset", f"{self.src.cloud.project}:{dataset}", check=False)
            if sh(["gcloud", "storage", "buckets", "describe", f"gs://{bucket}"], timeout=300, check=False).returncode == 0:
                left.append(f"gcs:{bucket}")
            if sh(["bq", "--quiet", "--headless", "show", f"{self.src.cloud.project}:{dataset}"], check=False).returncode == 0:
                left.append(f"bq:{dataset}")
        if left:
            raise RuntimeError(f"cloud cleanup left behind {left}")


def legs(src_legs: Sequence[Any], old_bin: Path | None) -> list[Migration]:
    return [Migration(s) for s in src_legs if s.engine.name == ENGINE] if old_bin else []
