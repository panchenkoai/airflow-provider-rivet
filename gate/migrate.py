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
import shutil
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import duckdb

from . import warehouse
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
#: State tables not compared by count: schema bookkeeping (the copy brings the rows to this release's schema) and leases.
UNCOMPARED = frozenset({"schema_version", "rivet_schema_version", "state_lease"})
#: The changes of each step, and the change events they make (every update changes a value; deletes are by key).
STEPS: dict[str, tuple[str, dict[str, dict[str, int]]]] = {
    "sqlite-2": (
        "UPDATE gate_orders SET status = 'paid' WHERE id BETWEEN 1 AND 3;\n"
        "INSERT INTO gate_orders (customer_id, amount, status) VALUES (1, 5.55, 'new'), (2, 6.66, 'new');\n"
        "DELETE FROM gate_orders WHERE id = 4999;",
        {"gate_orders": {"update": 3, "insert": 2, "delete": 1}},
    ),
    "before-switch": (
        "UPDATE gate_customers SET tier = 'moving' WHERE id IN (7, 8);\nDELETE FROM gate_orders WHERE id = 4998;",
        {"gate_customers": {"update": 2}, "gate_orders": {"delete": 1}},
    ),
    "after-switch": (
        "INSERT INTO gate_orders (customer_id, amount, status) VALUES (3, 7.77, 'new');\n"
        "UPDATE gate_orders SET status = 'moved' WHERE id = 10;\nDELETE FROM gate_orders WHERE id = 4997;",
        {"gate_orders": {"insert": 1, "update": 1, "delete": 1}},
    ),
    "pg-2": (
        "UPDATE gate_customers SET tier = 'settled' WHERE id = 9;\nDELETE FROM gate_orders WHERE id = 4996;",
        {"gate_customers": {"update": 1}, "gate_orders": {"delete": 1}},
    ),
    "back": (
        "UPDATE gate_customers SET tier = 'back' WHERE id = 11;\nINSERT INTO gate_orders (customer_id, amount, status) "
        "VALUES (4, 8.88, 'new');",
        {"gate_customers": {"update": 1}, "gate_orders": {"insert": 1}},
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
        self.stream: dict[str, Any] = {}
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
        self.sql(stand, self.src.engine.seed)
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
            "--include", *self.src.engine.tables, "-o", f"{INIT_DIR}/rivet.yaml",
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
        streams = read_config(text, list(self.src.engine.tables))["streams"]
        if len(streams) != 1:
            raise ValueError(f"rivet 0.32.1's init wrote streams {streams}, expected one")
        self.stream = streams[0]
        shape = {"config": f"{CONFIG_DIR}/rivet.yaml", "state_dir": STATE_DIR, "stream": self.stream, "dags": DAGS,
                 "old_bin": OLD_BIN, "url_env": URL_ENV, "conn": CONN, "state_conn": STATE_CONN}  # fmt: skip
        (stand.rivet_dir / "configs" / "mysql_migrate.json").write_text(json.dumps(shape))

    def url(self, stand: Any) -> str:
        """The URL init and `rivet state copy` read the migration database with (inside the stack's network)."""
        return self.src.engine.url(stand.secrets).rsplit("/", 1)[0] + f"/{DATABASE}"

    @property
    def dags(self) -> list[str]:
        return list(DAGS.values())

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
            for table, cols in self.src.engine.tables.items():
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
            for t in self.src.engine.tables
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
        cols = dict(self.src.engine.tables)
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
        copies = sorted(p.name for p in (stand.rivet_dir / "state" / "mysql_mig").glob("rivet.*.yaml"))
        if len(copies) != 1:
            v.grade("migrate/mysql/cdc/state-copy", [f"config copies in the state_dir: {copies}, expected one"])
            return
        argv = ["rivet", "state", "copy", "-c", f"{STATE_DIR}/{copies[0]}"]
        self.command(f"RIVET_STATE_URL=postgresql://rivet_state:<password>@postgres:5432/{STATE_DB} {' '.join(argv)}")
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
                     f"(postgresql://rivet_state:...@postgres:5432/{STATE_DB}), state_dir={STATE_DIR}")  # fmt: skip
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
        back_sql, back_ops = STEPS["back"]
        self.sql(stand, back_sql)
        self.command(f"(the way back) DAG {DAGS['back']}: rivet_bin=rivet (main), env RIVET_STATE_URL='' (the SQLite "
                     f"file the copy left), single_host=True, state_dir={STATE_DIR}")  # fmt: skip
        found = self.run(stand, v, wait_runs, "back", DAGS["back"], events(back_ops), grade=False)
        print(
            f"MEASURE migrate/mysql/cdc/way-back: {found or 'the run continued: no gap, no duplicate, BigQuery equals the source'}",
            flush=True,
        )
        print("COMMANDS\n" + "\n".join(self.commands), flush=True)

    def cleanup(self) -> None:
        sh(["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{self.bucket}"], timeout=600, check=False)
        warehouse._bq("rm", "-r", "-f", "--dataset", f"{self.src.cloud.project}:{self.dataset}", check=False)
        left = []
        if sh(["gcloud", "storage", "buckets", "describe", f"gs://{self.bucket}"], timeout=300, check=False).returncode == 0:
            left.append(f"gcs:{self.bucket}")
        if sh(["bq", "--quiet", "--headless", "show", f"{self.src.cloud.project}:{self.dataset}"], check=False).returncode == 0:
            left.append(f"bq:{self.dataset}")
        if left:
            raise RuntimeError(f"cloud cleanup left behind {left}")


def legs(src_legs: Sequence[Any], old_bin: Path | None) -> list[Migration]:
    return [Migration(s) for s in src_legs if s.engine.name == ENGINE] if old_bin else []
