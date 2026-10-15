"""The gate scenario on one stack: initial load, a delta, an idle run; four DAGs per cycle, every result graded by an oracle."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import oracle
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
DELETE FROM gate_orders WHERE id IN (SELECT id FROM gate_orders ORDER BY id DESC LIMIT 5);
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
DAGS = [f"gate_{kind}_{state}" for state in STATES for kind in ("cdc", "batch")]
RIVET_TASKS = {f"gate_batch_{s}": [f"apply.{s}_{short}" for short, _ in TABLES.values()] for s in STATES} | {
    f"gate_cdc_{s}": ["run"] for s in STATES
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
        self.rows.append({"stack": self.stack, "cell": cell, "status": status, "detail": detail})
        print(f"{status} airflow-{self.stack} {cell}" + (f" — {detail}" if detail else ""), flush=True)

    def guard(self, cell: str, step: Callable[[], Any]) -> Any:
        """Run a step; an exception is a FAIL of that cell, not a crash of the gate."""
        try:
            return step()
        except Exception as exc:
            self.grade(cell, [f"{type(exc).__name__}: {exc}"])
            return None


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
    """`export_state.last_cursor_value` by export, read with a plain SQLite / psql client."""
    if state == "sqlite":
        db = stand.rivet_dir / "state" / "sqlite" / ".rivet_state.db"
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as con:
            return dict(con.execute("SELECT export_name, last_cursor_value FROM export_state").fetchall())
    text = stand.psql("gate-state", "rivet_state", "SELECT export_name, coalesce(last_cursor_value, '') FROM export_state")
    return dict(line.split("|", 1) for line in text.splitlines() if "|" in line)


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
) -> tuple[set[str], dict[str, Any]]:
    """Trigger the four DAGs (the SQLite CDC run paused, the SQLite batch run inside its window), wait, grade."""
    run_id = f"gate_{n}_{name}"
    if changes:
        stand.psql("gate-source", "rivet", changes)
    marker = stand.rivet_dir / "markers" / "cdc_sqlite"
    marker.unlink(missing_ok=True)
    stand.trigger("gate_cdc_sqlite", run_id)
    opened = wait_file(marker)
    v.grade(f"{name}/sqlite-pause-window-opened", [] if opened else ["the SQLite CDC run never reached cdc_after_open"])
    for dag in DAGS:
        if dag != "gate_cdc_sqlite":
            stand.trigger(dag, run_id)
    states = wait_runs(stand, DAGS, run_id)
    out = stand.rivet_dir / "out"
    files_after = parquet(out)
    new = files_after - files_before
    if sabotage == "drop-delta-file" and name == "delta":
        victim = sorted(f for f in new if "/batch_pg/orders/" in f)[0]
        Path(victim).unlink()
        new.discard(victim)
        files_after.discard(victim)
        print(f"SABOTAGE removed {victim}", flush=True)
    for state in STATES:
        history[state].append(sorted(f for f in new if f"/cdc_{state}/" in f and "/snapshot/" not in f))

    tasks = {}
    for dag in DAGS:
        tasks[dag] = stand.tasks(dag, run_id)
        bad = {t: i["state"] for t, i in tasks[dag].items() if t in RIVET_TASKS[dag] and i["state"] != "success"}
        v.grade(
            f"{name}/{dag}/run",
            ([f"run {states.get(dag)}"] if states.get(dag) != "success" else []) + ([f"tasks {bad}"] if bad else []),
            ok=f"run {states.get(dag)}",
        )
    c, b = (
        tasks["gate_cdc_sqlite"].get("run", {}),
        [tasks["gate_batch_sqlite"].get(t, {}) for t in RIVET_TASKS["gate_batch_sqlite"]],
    )
    v.guard(
        f"{name}/sqlite-two-writers-overlap",
        lambda: v.grade(
            f"{name}/sqlite-two-writers-overlap",
            oracle.inside(
                [(t["start_date"], t["end_date"]) for t in b],
                (c["start_date"], c["end_date"]),
                "SQLite batch inside the SQLite CDC run",
            ),
        ),
    )

    con = oracle.connect(stand.source_dsn)
    try:
        by_dir = {
            state: {kind: sorted(f for f in new if f"/{kind}_{state}/" in f) for kind in ("batch", "cdc")} for state in STATES
        }
        for state in STATES:
            for table, (short, cols) in TABLES.items():
                src = f"src.public.{table}"
                mine = [f for f in new if f"/batch_{state}/{short}/" in f]
                every = sorted(f for f in files_after if f"/batch_{state}/{short}/" in f)
                cell = f"{name}/batch/{state}/{short}"
                v.guard(
                    cell + "/delta-values",
                    lambda: v.grade(
                        cell + "/delta-values",
                        oracle.batch_delta(con, sorted(mine), src, cols, "updated_at", high_water.get(table)),
                        ok=f"{oracle.rows_in(con, mine)} row(s)",
                    ),
                )
                v.guard(
                    cell + "/latest-equals-source",
                    lambda: v.grade(
                        cell + "/latest-equals-source",
                        oracle.batch_latest(
                            con, every, src, cols, "id", "updated_at", DELETED_BY_BATCH[name] if table == "gate_orders" else 0
                        ),
                    ),
                )
                xcom = v.guard(cell + "/xcom-rows", lambda: stand.xcom(f"gate_batch_{state}", run_id, f"apply.{state}_{short}"))
                if xcom is not None:
                    v.grade(
                        cell + "/xcom-rows",
                        oracle.same_count(sum(u.get("rows") or 0 for u in xcom["units"]), oracle.rows_in(con, mine), cell),
                        ok=f"degraded {xcom.get('degraded')}",
                    )
                cursors = v.guard(cell + "/stored-cursor", lambda: stored_cursors(stand, state))
                if cursors is not None:
                    top = con.execute(f"SELECT max(updated_at) FROM {src}").fetchone()[0]
                    v.grade(cell + "/stored-cursor", oracle.stored_cursor(cursors.get(f"{state}_{short}") or None, top, cell))
            cdc_new = by_dir[state]["cdc"]
            for table, (_, cols) in TABLES.items():
                cell = f"{name}/cdc/{state}/{table}"
                stream_new = [f for f in cdc_new if f"/{table}/" in f and "/snapshot/" not in f]
                v.guard(
                    cell + "/operations", lambda: v.grade(cell + "/operations", oracle.cdc_ops(con, stream_new, ops[table], cell))
                )
                snapshot = sorted(f for f in files_after if f"/cdc_{state}/{table}/snapshot/" in f)
                stream = [[f for f in run if f"/{table}/" in f] for run in history[state]]
                v.guard(
                    cell + "/replay-equals-source",
                    lambda: v.grade(
                        cell + "/replay-equals-source",
                        oracle.cdc_replay(con, snapshot, stream, f"src.public.{table}", cols, "id"),
                    ),
                )
            xcom = v.guard(f"{name}/cdc/{state}/xcom-rows", lambda: stand.xcom(f"gate_cdc_{state}", run_id, "run"))
            if xcom is not None:
                v.grade(
                    f"{name}/cdc/{state}/xcom-rows",
                    oracle.same_count(
                        sum(u.get("rows") or 0 for u in xcom["units"]), oracle.rows_in(con, cdc_new), f"cdc {state}"
                    ),
                    ok=f"degraded {xcom.get('degraded')}",
                )
        marks = {t: con.execute(f"SELECT max(updated_at) FROM src.public.{t}").fetchone()[0] for t in TABLES}
    finally:
        con.close()
    return files_after, marks


def play(stand: Stand, v: Verdicts, sabotage: str | None) -> None:
    """Seed, configure, and run the three cycles."""
    stand.psql("gate-source", "rivet", SEED)
    write_configs(stand.rivet_dir)
    stand.wait_dags(DAGS)
    files: set[str] = set()
    high_water: dict[str, Any] = {}
    history: dict[str, list[list[str]]] = {state: [] for state in STATES}
    for n, (name, changes, ops) in enumerate(CYCLES, start=1):
        files, high_water = run_cycle(stand, v, n, name, changes, ops, files, high_water, history, sabotage)
