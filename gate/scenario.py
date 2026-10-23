"""The gate scenario on one stack: initial load, a delta, an idle run; four DAGs per cycle, every result graded by an oracle."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import destinations, oracle, sources, warehouse
from .stand import Stand

TABLES = {
    "gate_customers": ("customers", ["id", "name", "tier", "updated_at"]),
    "gate_orders": ("orders", ["id", "customer_id", "amount", "status", "updated_at"]),
}
STATES = ("sqlite", "pg")
SEED = """
DROP TABLE IF EXISTS gate_orders, gate_customers;
CREATE TABLE gate_customers (id serial PRIMARY KEY, name text NOT NULL, tier text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE gate_orders (id serial PRIMARY KEY, customer_id int NOT NULL, amount numeric(12,2) NOT NULL,
  status text NOT NULL, updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
INSERT INTO gate_customers (name, tier) SELECT 'customer ' || g, (ARRAY['free','pro','team'])[1 + g % 3] FROM generate_series(1, 1000) g;
INSERT INTO gate_orders (customer_id, amount, status) SELECT 1 + g % 1000, round((g % 997) * 1.37, 2), 'new' FROM generate_series(1, 5000) g;
"""
CHANGES = """
INSERT INTO gate_customers (name, tier) SELECT 'new customer ' || g, 'free' FROM generate_series(1, 20) g;
INSERT INTO gate_orders (customer_id, amount, status) SELECT 1 + g % 1000, 9.99 * g, 'new' FROM generate_series(1, 100) g;
UPDATE gate_orders SET status = 'shipped', updated_at = clock_timestamp() WHERE id IN (SELECT id FROM gate_orders ORDER BY id LIMIT 30);
UPDATE gate_customers SET tier = 'team', updated_at = clock_timestamp() WHERE id IN (SELECT id FROM gate_customers ORDER BY id LIMIT 10);
DELETE FROM gate_orders WHERE id IN (SELECT id FROM gate_orders WHERE id <= 5000 ORDER BY id DESC LIMIT 5);
"""
CHANGED_OPS = {"gate_customers": {"insert": 20, "update": 10}, "gate_orders": {"insert": 100, "update": 30, "delete": 5}}
NO_OPS: dict[str, dict[str, int]] = {"gate_customers": {}, "gate_orders": {}}
CYCLES = (("initial", None, NO_OPS), ("delta", CHANGES, CHANGED_OPS), ("idle", None, NO_OPS))
DELETED_BY_BATCH = {"initial": 0, "delta": 5, "idle": 5}

BATCH = """source:
  type: postgres
  url_env: RIVET_GATE_SOURCE_URL
  tls: {{ mode: disable }}
exports:
{exports}"""
BATCH_EXPORT = """  - name: {state}_{short}
    query: SELECT {cols} FROM {table}
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: {{ type: local, path: /opt/rivet/out/batch_{state}/{short}/ }}
"""
CDC = """source:
  type: postgres
  url_env: RIVET_GATE_SOURCE_URL
  tls: {{ mode: disable }}
exports:
  - name: {state}_cdc
    tables: [{tables}]
    mode: cdc
    format: parquet
    cdc:
      slot: gate_{state}
      initial: snapshot
      checkpoint: /opt/rivet/state/{state}/{state}_cdc.ckpt
      until_current: true
    destination: {{ type: local, path: /opt/rivet/out/cdc_{state}/ }}
"""
READ_SQLITE_CURSORS = (
    "import json, sqlite3; c = sqlite3.connect('file:/opt/rivet/state/sqlite/.rivet_state.db?mode=ro', uri=True); "
    "print(json.dumps(dict(c.execute('SELECT export_name, last_cursor_value FROM export_state').fetchall())))"
)
DAGS = [f"gate_{kind}_{state}" for state in STATES for kind in ("cdc", "batch")]
RIVET_TASKS = {f"gate_batch_{s}": [f"apply.{s}_{short}" for short, _ in TABLES.values()] for s in STATES} | {
    f"gate_cdc_{s}": ["run"] for s in STATES
}


#: Cells red by a known product defect: graded XFAIL with the issue while red, XPASS (a failure of the run) once green,
#: so the entry is removed when the fix lands.
KNOWN = {
    "initial/mysql/cdc/staged-parts-cleaned": "panchenkoai/rivet#671 (cleanup_source skips the CDC baseline, 0.32.3)",
}


@dataclass
class Verdicts:
    """Graded cells of one gate run."""

    stack: str
    rows: list[dict[str, str]] = field(default_factory=list)

    def grade(self, cell: str, problems: list[str], ok: str = "") -> None:
        """Record PASS when the oracle found nothing, FAIL with its findings otherwise; print it."""
        status = "FAIL" if problems else "PASS"
        detail = "; ".join(problems) if problems else ok
        if cell in KNOWN:
            status = "XFAIL" if problems else "XPASS"
            detail = f"{KNOWN[cell]}" + (f": {detail}" if problems else ": passes now, remove it from KNOWN")
        self.rows.append({"stack": self.stack, "cell": cell, "status": status, "detail": detail})
        print(f"{status} airflow-{self.stack} {cell}" + (f" — {detail}" if detail else ""), flush=True)

    def guard(self, cell: str, step: Callable[..., Any], *args: Any) -> Any:
        """Run a step; an exception is a FAIL of that cell, not a crash of the gate."""
        try:
            return step(*args)
        except Exception as exc:
            self.grade(cell, [f"{type(exc).__name__}: {exc}"])
            return None

    def check(self, cell: str, oracle_fn: Callable[..., list[str]], *args: Any, ok: str = "") -> None:
        """Grade an oracle's findings; an exception while it runs is a FAIL of that cell."""
        problems = self.guard(cell, oracle_fn, *args)
        if problems is not None:
            self.grade(cell, problems, ok=ok)


def write_configs(rivet_dir: Path) -> None:
    """The four rivet configs the gate DAGs run."""
    for state in STATES:
        exports = "".join(
            BATCH_EXPORT.format(state=state, short=short, cols=", ".join(cols), table=table)
            for table, (short, cols) in TABLES.items()
        )
        (rivet_dir / "configs" / f"batch_{state}.yaml").write_text(BATCH.format(exports=exports))
        (rivet_dir / "configs" / f"cdc_{state}.yaml").write_text(CDC.format(state=state, tables=", ".join(TABLES)))


def parquet(out: Path) -> set[str]:
    """Every parquet file under the output root."""
    return {str(p) for p in out.rglob("*.parquet")}


def wait_runs(stand: Stand, dags: list[str], run_id: str, timeout: float = 900) -> dict[str, str]:
    """Wait until every DAG run is finished; their final states."""
    deadline = time.monotonic() + timeout
    states: dict[str, str] = {}
    while time.monotonic() < deadline:
        states = {d: stand.run_state(d, run_id) for d in dags}
        if all(s in ("success", "failed") for s in states.values()):
            return states
        time.sleep(3)
    return states


def wait_file(path: Path, timeout: float = 180) -> bool:
    """Whether the file appeared before the timeout."""
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.5)
    return True


def stored_cursors(stand: Stand, state: str) -> dict[str, str]:
    """`export_state.last_cursor_value` by export, read with a plain SQLite / psql client.

    The SQLite file is read inside a worker container: a WAL database shares its index through `-shm` memory,
    which a host process on the far side of a docker VM's file share does not see (measured: "malformed").
    """
    if state == "sqlite":
        text = stand.compose("exec", "-T", "airflow-worker", "python", "-c", READ_SQLITE_CURSORS).stdout
        return json.loads(text)
    text = stand.psql("gate-state", "rivet_state", "SELECT export_name, coalesce(last_cursor_value, '') FROM export_state")
    return dict(line.split("|", 1) for line in text.splitlines() if "|" in line)


def trigger_cycle(stand: Stand, v: Verdicts, name: str, run_id: str, dags: list[str]) -> dict[str, str]:
    """Start the SQLite CDC run, wait for its pause window, start every other DAG, wait for all; their states."""
    marker = stand.rivet_dir / "markers" / "cdc_sqlite"
    marker.unlink(missing_ok=True)
    stand.trigger("gate_cdc_sqlite", run_id)
    opened = wait_file(marker)
    v.grade(f"{name}/sqlite-pause-window-opened", [] if opened else ["the SQLite CDC run never reached cdc_after_open"])
    for dag in dags:
        if dag != "gate_cdc_sqlite":
            stand.trigger(dag, run_id)
    return wait_runs(stand, dags, run_id)


def grade_runs(
    stand: Stand,
    v: Verdicts,
    name: str,
    run_id: str,
    states: dict[str, str],
    tasks_of: dict[str, list[str]],
    may_skip: Mapping[str, tuple[str, ...]] | None = None,
    refused: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Every DAG run succeeded and every rivet task in it too (a task whose id starts with a prefix `may_skip` gives
    for its DAG may be skipped). A DAG `refused` names tasks for (a stream rivet refuses by a documented limit, graded
    by the leg's own cell) must end `failed`, those tasks failed and every other wanted task succeeded."""
    may_skip, refused = may_skip or {}, refused or {}
    tasks = {}
    for dag, wanted in tasks_of.items():
        tasks[dag] = stand.tasks(dag, run_id)
        bad = {
            t: i["state"]
            for t, i in tasks[dag].items()
            if t in wanted and i["state"] != "success" and not (i["state"] == "skipped" and t.startswith(may_skip.get(dag, ())))
        }
        missing = sorted(set(wanted) - set(tasks[dag]))
        stops = list(refused.get(dag, ()))
        want = "failed" if stops else "success"
        not_failed = {t: tasks[dag].get(t, {}).get("state") for t in stops if tasks[dag].get(t, {}).get("state") != "failed"}
        problems = (
            ([f"run {states.get(dag)}" + (f", expected {want}" if stops else "")] if states.get(dag) != want else [])
            + ([f"tasks {bad}"] if bad else [])
            + ([f"refused tasks {not_failed}, expected failed"] if not_failed else [])
        )
        v.grade(f"{name}/{dag}/run", problems + ([f"no tasks {missing}"] if missing else []), ok=f"run {states.get(dag)}")
    return tasks


def grade_base(
    stand: Stand,
    v: Verdicts,
    name: str,
    run_id: str,
    ops: dict[str, dict[str, int]],
    new: set[str],
    files_after: set[str],
    high_water: dict[str, Any],
    history: dict[str, list[list[str]]],
    tasks: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """The local-Parquet DAGs: deltas, latest versions, CDC replay, XCom counts, stored cursors, the SQLite overlap."""
    c = tasks["gate_cdc_sqlite"].get("run", {})
    b = [tasks["gate_batch_sqlite"].get(t, {}) for t in RIVET_TASKS["gate_batch_sqlite"]]
    v.check(
        f"{name}/sqlite-two-writers-overlap",
        oracle.inside,
        [(t.get("start_date"), t.get("end_date")) for t in b],
        (c.get("start_date"), c.get("end_date")),
        "SQLite batch inside the SQLite CDC run",
    )
    con = oracle.connect(stand.source_dsn)
    try:
        for state in STATES:
            cursors = v.guard(f"{name}/batch/{state}/stored-cursors", stored_cursors, stand, state)
            for table, (short, cols) in TABLES.items():
                src = f"src.public.{table}"
                mine = sorted(f for f in new if f"/batch_{state}/{short}/" in f)
                every = sorted(f for f in files_after if f"/batch_{state}/{short}/" in f)
                cell = f"{name}/batch/{state}/{short}"
                v.check(
                    cell + "/delta-values",
                    oracle.batch_delta,
                    con,
                    mine,
                    src,
                    cols,
                    "updated_at",
                    high_water.get(table),
                    ok=f"{oracle.rows_in(con, mine)} row(s)",
                )
                deleted = DELETED_BY_BATCH[name] if table == "gate_orders" else 0
                v.check(cell + "/latest-equals-source", oracle.batch_latest, con, every, src, cols, "id", "updated_at", deleted)
                xcom = v.guard(cell + "/xcom-rows", stand.xcom, f"gate_batch_{state}", run_id, f"apply.{state}_{short}")
                if xcom is not None:
                    reported = sum(u.get("rows") or 0 for u in xcom["units"])
                    v.check(
                        cell + "/xcom-rows",
                        oracle.same_count,
                        reported,
                        oracle.rows_in(con, mine),
                        cell,
                        ok=f"degraded {xcom.get('degraded')}",
                    )
                if cursors is not None:
                    top = con.execute(f"SELECT max(updated_at) FROM {src}").fetchone()[0]
                    v.check(cell + "/stored-cursor", oracle.stored_cursor, cursors.get(f"{state}_{short}") or None, top, cell)
            cdc_new = sorted(f for f in new if f"/cdc_{state}/" in f)
            for table, (_, cols) in TABLES.items():
                cell = f"{name}/cdc/{state}/{table}"
                stream_new = [f for f in cdc_new if f"/{table}/" in f and "/snapshot/" not in f]
                v.check(cell + "/operations", oracle.cdc_ops, con, stream_new, ops[table], cell)
                snapshot = sorted(f for f in files_after if f"/cdc_{state}/{table}/snapshot/" in f)
                stream = [[f for f in run if f"/{table}/" in f] for run in history[state]]
                v.check(
                    cell + "/replay-equals-source", oracle.cdc_replay, con, snapshot, stream, f"src.public.{table}", cols, "id"
                )
            xcom = v.guard(f"{name}/cdc/{state}/xcom-rows", stand.xcom, f"gate_cdc_{state}", run_id, "run")
            if xcom is not None:
                reported = sum(u.get("rows") or 0 for u in xcom["units"])
                v.check(
                    f"{name}/cdc/{state}/xcom-rows",
                    oracle.same_count,
                    reported,
                    oracle.rows_in(con, cdc_new),
                    f"cdc {state}",
                    ok=f"degraded {xcom.get('degraded')}",
                )
        return {t: con.execute(f"SELECT max(updated_at) FROM src.public.{t}").fetchone()[0] for t in TABLES}
    finally:
        con.close()


def run_cycle(
    stand: Stand,
    v: Verdicts,
    n: int,
    name: str,
    changes: str | None,
    ops: dict[str, dict[str, int]],
    files_before: set[str],
    high_water: dict[str, Any],
    history: dict[str, list[list[str]]],
    sabotage: str | None,
    leg: warehouse.Leg | None,
    src_legs: list[sources.SourceLeg] | None = None,
    dest_legs: list[destinations.S3Leg] | None = None,
) -> tuple[set[str], dict[str, Any]]:
    """One cycle: the sources change, every DAG runs (the SQLite writers overlapping), each oracle grades."""
    run_id = f"gate_{n}_{name}"
    src_legs, dest_legs = src_legs or [], dest_legs or []
    if changes:
        stand.psql("gate-source", "rivet", changes)
        for src in src_legs:
            src.apply_changes(stand)
    dags = DAGS + (leg.dags if leg else []) + [d for src in src_legs for d in src.dags] + [d for x in dest_legs for d in x.dags]
    states = trigger_cycle(stand, v, name, run_id, dags)
    files_after = parquet(stand.rivet_dir / "out")
    new = files_after - files_before
    if sabotage == "drop-delta-file" and name == "delta":
        victim = sorted(f for f in new if "/batch_pg/orders/" in f)[0]
        Path(victim).unlink()
        new.discard(victim)
        files_after.discard(victim)
        print(f"SABOTAGE removed {victim}", flush=True)
    for state in STATES:
        history[state].append(sorted(f for f in new if f"/cdc_{state}/" in f and "/snapshot/" not in f))
    tasks = grade_runs(stand, v, name, run_id, states, RIVET_TASKS)
    marks = grade_base(stand, v, name, run_id, ops, new, files_after, high_water, history, tasks)
    if leg:
        grade_runs(stand, v, name, run_id, states, leg.tasks, may_skip=warehouse.may_skip(leg.dags, name))
        if sabotage == "warehouse-value" and name == "delta":
            print(f"SABOTAGE {leg.sabotage()}", flush=True)
        leg.grade(stand, v, name, {t: DELETED_BY_BATCH[name] if t == "gate_orders" else 0 for t in TABLES})
    for src in src_legs:
        grade_runs(stand, v, name, run_id, states, src.cycle_tasks(), may_skip=src.may_skip, refused=src.refused)
        src.grade(stand, v, name, run_id)
    for dest in dest_legs:
        grade_runs(stand, v, name, run_id, states, dest.tasks, may_skip=dest.may_skip)
        dest.grade(stand, v, name, run_id)
    return files_after, marks


def play(
    stand: Stand,
    v: Verdicts,
    sabotage: str | None,
    leg: warehouse.Leg | None = None,
    src_legs: list[sources.SourceLeg] | None = None,
    dest_legs: list[destinations.S3Leg] | None = None,
    mig_legs: list[Any] | None = None,
    readme_legs: list[Any] | None = None,
) -> None:
    """Seed, configure, and run the three cycles (then each destination leg's missing-bucket run, and the migration)."""
    src_legs, dest_legs, mig_legs, readme_legs = src_legs or [], dest_legs or [], mig_legs or [], readme_legs or []
    stand.psql("gate-source", "rivet", SEED)
    write_configs(stand.rivet_dir)
    if leg:
        leg.prepare(stand)
    for src in src_legs:
        src.prepare(stand)
    for dest in dest_legs:
        dest.prepare(stand, SEED)
    for mig in mig_legs:
        mig.prepare(stand)
    for readme in readme_legs:
        readme.prepare(stand)
    stand.wait_dags(
        [d for mig in mig_legs for d in mig.dags]
        + DAGS
        + (leg.dags if leg else [])
        + [d for src in src_legs for d in [*src.dags, *filter(None, [src.hot_dag, src.shared_dag, src.overlap_dag])]]
        + [d for x in dest_legs for d in [*x.dags, *x.probe_dags]]
    )
    for src in src_legs:
        # `--source-dags`: the DAGs just unpaused write their configs (their `init` runs); the leg waits for each and for
        # the DAG built from what it wrote.
        src.adopt(stand, v)
    files: set[str] = set()
    high_water: dict[str, Any] = {}
    history: dict[str, list[list[str]]] = {state: [] for state in STATES}
    for n, (name, changes, ops) in enumerate(CYCLES, start=1):
        files, high_water = run_cycle(
            stand, v, n, name, changes, ops, files, high_water, history, sabotage, leg, src_legs, dest_legs
        )
    for src in src_legs:
        if not src.extras:
            continue
        if src.hot_dag:
            src.hot_phase(stand, v, wait_runs, grade_runs)
        if src.engine.shared is not None and src.shared:
            src.shared_phase(stand, v, wait_runs, grade_runs)
        if src.engine.overlap is not None and src.overlap_dag:
            src.overlap_phase(stand, v, wait_runs)
        if src.engine.recreate is not None:
            src.recreate_phase(stand, v, wait_runs)
        if src.engine.git_sync is not None:
            src.git_sync_phase(stand, v, wait_runs)
    for mig in mig_legs:
        mig.play(stand, v, wait_runs)
    # Before the same-names phase: rivet 0.32.1 (the cron job) refuses unqualified tables another database holds.
    for readme in readme_legs:
        readme.play(stand, v, wait_runs)
    for src in src_legs:
        if src.extras and src.engine.duplicate_db:
            src.unqualified_phase(stand, v, wait_runs)
    for dest in dest_legs:
        dest.missing_bucket_probe(stand, v, wait_runs)
        dest.cleanup_probe(stand, v, wait_runs)
