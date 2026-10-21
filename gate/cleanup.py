"""`cleanup_source` on the S3 leg: what rivet deletes from the bucket after a load into ClickHouse, and what it keeps.

`rivet init` writes `cleanup_source: true` in every `load:` block, so the staged Parquet of a loaded run is deleted
once the load succeeds. rivet promises three things about that delete (src/load/orchestrate.rs `cleanable_runs`,
src/load/staging.rs `cleanup_target_leased` / `cleanup_verdict` / `prefix_has_active_run` / `cleanup_keys`, src/load/
mod.rs `run_load` / `append_and_view`, rivet 0.32.1):

1. a failed load deletes nothing: the delete runs only after the write and its row-count gate passed;
2. unloaded runs survive: an incremental load removes only the runs it has loaded or is loading (a `full` load also
   removes the runs it supersedes; this config is incremental), and a load narrowed to one export reads one prefix;
3. a live run blocks cleanup: while a run is `running` on the prefix (the run-status ledger, or a `running` marker
   manifest in the bucket) the load proceeds and the whole cleanup of that prefix is skipped.

The config is init's own (`--mode incremental --s3-bucket --clickhouse-*`) over a database of its own on the leg's
postgres source, used as written; its DAGs extract (both exports), extract with one apply paused after its
snapshot opened (rivet's `RIVET_TEST_PAUSE_AT` sequence point: the run is live for as long as the gate needs), and
load one export each. A load is broken by a CHECK constraint on its ClickHouse table that refuses every row, so it
fails at the write, after the manifests were read and the cleanup decided. The oracles: the bucket listed by MinIO's
own `mc` (a client rivet does not use), the ledger read with psql, ClickHouse read back over HTTP against the source.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from . import warehouse
from .oracle import SHOWN

KIND = "cleanup"
#: The database of the cleanup config on gate-source (every other config reads `rivet`).
DATABASE = "rivet_cleanup"
#: Loaded alone first (guarantee 2), then broken in ClickHouse (guarantee 1).
FIRST = "gate_customers"
#: Its parts wait unloaded while FIRST is loaded (2); its extract is held live while it is loaded (3).
LIVE = "gate_orders"
#: How long the paused apply stays live: the loads beside it take well under a minute.
PAUSE_MS = 240_000
PAUSE_AT = f"pg_after_snapshot_open:{PAUSE_MS}"
MARKER = "s3_cleanup"
EXTRACT_DAG = "gate_s3_cleanup_extract"
PAUSED_DAG = "gate_s3_cleanup_extract_paused"
#: The constraint that makes a ClickHouse insert fail: no row has a negative id.
CONSTRAINT = "gate_refuse_every_row"
#: Inserts and updates only: an incremental extract cannot see a delete, so ClickHouse must equal the source exactly.
CHANGES = """
INSERT INTO gate_customers (name, tier) SELECT 'late customer ' || g, 'pro' FROM generate_series(1, 7) g;
INSERT INTO gate_orders (customer_id, amount, status) SELECT 1 + g % 1000, 4.25 * g, 'new' FROM generate_series(1, 40) g;
UPDATE gate_orders SET status = 'paid', updated_at = clock_timestamp() WHERE id IN (SELECT id FROM gate_orders ORDER BY id DESC LIMIT 9);
UPDATE gate_customers SET tier = 'team', updated_at = clock_timestamp() WHERE id IN (SELECT id FROM gate_customers ORDER BY id DESC LIMIT 3);
"""
G1 = "cleanup/failed-load-deletes-nothing"
G2 = "cleanup/unloaded-runs-survive"
G3 = "cleanup/live-run-blocks"


def shape() -> dict[str, Any]:
    """What the DAG file reads to build the cleanup DAGs beside init's config."""
    return {
        "cleanup": {
            "extract": EXTRACT_DAG,
            "paused": PAUSED_DAG,
            "loads": {e: load_dag(e) for e in (FIRST, LIVE)},
            "pause": {"export": LIVE, "at": PAUSE_AT, "marker": f"/opt/rivet/markers/{MARKER}"},
        }
    }


def load_dag(export: str) -> str:
    return f"gate_s3_cleanup_load_{export}"


def dags(exports: Sequence[str]) -> list[str]:
    return [EXTRACT_DAG, PAUSED_DAG, *(load_dag(e) for e in exports)]


def prefix(export: str) -> str:
    """init's prefix of an export in its bucket."""
    return f"exports/{export}/"


# --- Pure oracles -----------------------------------------------------------------------------------------------------


def object_keys(mc_json: str) -> set[str]:
    """Every object key of `mc ls --recursive --json`; an error line is an error, not an empty bucket."""
    keys = set()
    for line in filter(None, (x.strip() for x in mc_json.splitlines())):
        doc = json.loads(line)
        if doc.get("status") != "success":
            raise RuntimeError(f"mc ls: {line[:300]}")
        if doc.get("type") == "file":
            keys.add(doc["key"])
    return keys


def _under(keys: Iterable[str], where: str) -> list[str]:
    return sorted(k for k in keys if k.startswith(where))


def _staged(before: set[str], where: str) -> list[str]:
    """Why the cell proves nothing: no Parquet was staged under the prefix before the load."""
    if any(k.endswith(".parquet") for k in _under(before, where)):
        return []
    return [f"no Parquet under {where} before the load: nothing to keep or delete, the cell proves nothing"]


def kept(before: set[str], after: set[str], where: str) -> list[str]:
    """Every object that was under the prefix before the load is still there."""
    held = _under(before, where)
    gone = [k for k in held if k not in after]
    found = [f"{len(gone)} of {len(held)} object(s) under {where} deleted: {gone[:SHOWN]}"] if gone else []
    return _staged(before, where) + found


def cleaned(before: set[str], after: set[str], where: str) -> list[str]:
    """No object that was under the prefix before the load is left: every run there was loaded, so each one's parts,
    manifest and the directory's canonical manifest and `_SUCCESS` are removed (`cleanup_keys`)."""
    left = [k for k in _under(before, where) if k in after]
    found = [f"{len(left)} object(s) under {where} left after the load: {left[:SHOWN]}"] if left else []
    return _staged(before, where) + found


def run_rows(psql_text: str) -> list[list[str]]:
    """`run_id|status|prefix` rows of psql's unaligned output."""
    return [line.split("|", 2) for line in psql_text.splitlines() if line.count("|") >= 2]


def live(rows: Sequence[Sequence[str]], export: str) -> list[str]:
    """A run of the export is `running` in the ledger."""
    if any(r[1] == "running" for r in rows):
        return []
    return [f"no `running` run of {export} in run_status: {[list(r) for r in rows][:SHOWN]}"]


def outcome(state: str | None, tasks: Mapping[str, Mapping[str, Any]], wanted: Sequence[str]) -> list[str]:
    """The DAG run and each wanted task succeeded."""
    problems = [] if state == "success" else [f"run {state}"]
    bad = {t: (tasks.get(t) or {}).get("state") for t in wanted if (tasks.get(t) or {}).get("state") != "success"}
    return problems + ([f"tasks {bad}"] if bad else [])


def failed_load(state: str | None, inst: Mapping[str, Any] | None, payload: Mapping[str, Any] | None) -> list[str]:
    """The broken load failed, and rivet said why with a code."""
    problems = [] if state == "failed" else [f"run {state}, not failed: the broken table did not stop the load"]
    if (inst or {}).get("state") != "failed":
        problems.append(f"load task {(inst or {}).get('state')}, not failed")
    if not ((payload or {}).get("error") or {}).get("code"):
        problems.append(f"no rivet error code in the load's XCom: {payload}")
    return problems


def physical(rows: Sequence[Mapping[str, Any]], table: str) -> list[str]:
    """The tables (no views) that hold an export's rows in ClickHouse: `<table>` or its `<table>__changes` log."""
    return sorted(
        r["name"]
        for r in rows
        if "View" not in str(r.get("engine")) and (r["name"] == table or str(r["name"]).startswith(f"{table}__"))
    )


def break_sql(db: str, tables: Sequence[str]) -> list[str]:
    return [f"ALTER TABLE `{db}`.`{t}` ADD CONSTRAINT {CONSTRAINT} CHECK id < 0" for t in tables]


def mend_sql(db: str, tables: Sequence[str]) -> list[str]:
    return [f"ALTER TABLE `{db}`.`{t}` DROP CONSTRAINT IF EXISTS {CONSTRAINT}" for t in tables]


# --- The phase --------------------------------------------------------------------------------------------------------


class Phase:
    """The cleanup cells of one stack, after the leg's cycles: extract, load one export, break a load, hold a run."""

    def __init__(self, leg: Any, stand: Any, v: Any, wait_runs: Callable[..., dict[str, str]], exports: Sequence[str]):
        self.leg, self.stand, self.v, self.wait_runs = leg, stand, v, wait_runs
        self.exports = list(exports)
        self.bucket = leg.cleanup_bucket
        self.db = leg.cleanup_db

    # --- reads ---

    def listing(self, step: str) -> set[str]:
        keys = object_keys(self.leg.mc(self.stand, "ls", "--recursive", "--json", f"gate/{self.bucket}").stdout)
        print(f"CLEANUP-INVENTORY {step} " + json.dumps(sorted(keys)), flush=True)
        return keys

    def running(self, export: str) -> list[list[str]]:
        sql = f"SELECT run_id, status, coalesce(prefix, '') FROM run_status WHERE export_name = '{export}' AND status = 'running'"
        return run_rows(self.stand.psql("gate-state", self.leg.cleanup_state_db, sql))

    def equals_source(self, table: str) -> list[str]:
        source = self.leg.source_rows(self.leg.cleanup_source_dsn)[table]
        got = self.leg.read_clickhouse(self.stand, self.db, table)
        return warehouse.current_state(got, source, self.leg.columns[table], "id", 0, f"clickhouse {self.db}.{table}")

    def tables(self, table: str) -> list[str]:
        sql = f"SELECT name, engine FROM system.tables WHERE database = '{self.db}'"
        return physical(warehouse._ch(self.stand.clickhouse_port, self.stand.secrets.clickhouse, sql), table)

    # --- acts ---

    def run(self, dag: str, run_id: str) -> tuple[str | None, dict[str, dict[str, Any]]]:
        self.stand.trigger(dag, run_id)
        return self.finished(dag, run_id)

    def finished(self, dag: str, run_id: str) -> tuple[str | None, dict[str, dict[str, Any]]]:
        state = self.wait_runs(self.stand, [dag], run_id).get(dag)
        tasks = self.stand.tasks(dag, run_id)
        print(f"CLEANUP-RUN {dag} {run_id} {state}: " + json.dumps({t: i.get("state") for t, i in tasks.items()}), flush=True)
        return state, tasks

    def extract(self, step: str) -> None:
        state, tasks = self.run(EXTRACT_DAG, f"gate_s3_cleanup_{step}")
        self.v.grade(f"cleanup/extract-{step}", outcome(state, tasks, [f"apply.{e}" for e in self.exports]), ok=str(state))

    def payload(self, dag: str, run_id: str) -> dict[str, Any]:
        try:
            return self.stand.xcom(dag, run_id, "load")
        except Exception as exc:  # no XCom: the cell says so
            return {"xcom": f"{type(exc).__name__}: {exc}"}

    def ch(self, statements: Sequence[str]) -> None:
        for sql in statements:
            self.leg.ch_exec(self.stand.clickhouse_port, self.stand.secrets.clickhouse, sql)

    def wait_for(self, what: str, ready: Callable[[], bool], timeout: float = 300) -> bool:
        deadline = time.monotonic() + timeout
        while not ready():
            if time.monotonic() > deadline:
                print(f"CLEANUP-WAIT {what}: not after {timeout:.0f} s", flush=True)
                return False
            time.sleep(2)
        return True

    # --- the guarantees ---

    def play(self) -> None:
        self.extract("e1")
        self.one_export_loaded()
        self.stand.psql(self.leg.cleanup_service, DATABASE, CHANGES)
        self.extract("e2")
        self.failed_load()
        self.later_load()
        self.stand.psql(self.leg.cleanup_service, DATABASE, CHANGES)
        self.extract("e3")
        self.live_run()

    def one_export_loaded(self) -> None:
        """(2) A load narrowed to FIRST cleans FIRST's prefix and leaves LIVE's unloaded parts alone."""
        before = self.listing("e1")
        state, tasks = self.run(load_dag(FIRST), "gate_s3_cleanup_g2")
        after = self.listing("g2-first-loaded")
        self.v.grade(f"{G2}/one-export-load-succeeds", outcome(state, tasks, ["load"]), ok=str(state))
        self.v.grade(f"{G2}/loaded-export-cleaned", cleaned(before, after, prefix(FIRST)))
        self.v.check(f"{G2}/loaded-export-equals-source", self.equals_source, FIRST)
        self.v.grade(
            f"{G2}/other-export-kept", kept(before, after, prefix(LIVE)), ok=f"{len(_under(after, prefix(LIVE)))} object(s)"
        )

    def failed_load(self) -> None:
        """(1) FIRST's ClickHouse table refuses every row: the load fails and deletes nothing; mended, the next load
        delivers every row and cleans."""
        dag, cell = load_dag(FIRST), G1
        before = self.listing("e2")
        broken = self.v.guard(f"{cell}/break-clickhouse", self.tables, FIRST)
        if not broken:
            self.v.grade(f"{cell}/break-clickhouse", [f"no ClickHouse table of {FIRST} in {self.db} to break"])
            return
        self.ch(break_sql(self.db, broken))
        run_id = "gate_s3_cleanup_g1_broken"
        state, tasks = self.run(dag, run_id)
        payload = self.payload(dag, run_id) if state == "failed" else None
        after = self.listing("g1-failed")
        self.ch(mend_sql(self.db, self.tables(FIRST)))
        code = ((payload or {}).get("error") or {}).get("code")
        self.v.grade(f"{cell}/load-failed", failed_load(state, tasks.get("load"), payload), ok=f"{code}, tables {broken}")
        self.v.grade(
            f"{cell}/every-object-kept", kept(before, after, prefix(FIRST)), ok=f"{len(_under(after, prefix(FIRST)))} object(s)"
        )
        state, tasks = self.run(dag, "gate_s3_cleanup_g1_next")
        cleaned_after = self.listing("g1-next")
        self.v.grade(f"{cell}/next-load-succeeds", outcome(state, tasks, ["load"]), ok=str(state))
        self.v.check(f"{cell}/next-load-delivers", self.equals_source, FIRST)
        self.v.grade(f"{cell}/next-load-cleans", cleaned(after, cleaned_after, prefix(FIRST)))

    def later_load(self) -> None:
        """(2) LIVE's two runs, left by the narrowed load, are taken by LIVE's own load."""
        before = self.listing("g2-before-later")
        state, tasks = self.run(load_dag(LIVE), "gate_s3_cleanup_g2_later")
        after = self.listing("g2-later")
        self.v.grade(f"{G2}/later-load-succeeds", outcome(state, tasks, ["load"]), ok=str(state))
        self.v.check(f"{G2}/later-load-delivers", self.equals_source, LIVE)
        self.v.grade(f"{G2}/later-load-cleans", cleaned(before, after, prefix(LIVE)))

    def live_run(self) -> None:
        """(3) LIVE's extract is held after it marked itself running: LIVE's load succeeds and deletes nothing, FIRST's
        load beside it cleans its own prefix; once the run ended, LIVE's next load cleans."""
        cell, run_id = G3, "gate_s3_cleanup_e4"
        marker = self.stand.rivet_dir / "markers" / MARKER
        marker.unlink(missing_ok=True)
        self.stand.psql(self.leg.cleanup_service, DATABASE, CHANGES)
        self.stand.trigger(PAUSED_DAG, run_id)
        paused = self.wait_for("paused apply", marker.exists)
        first_done = self.wait_for(
            f"apply.{FIRST}",
            lambda: (self.stand.tasks(PAUSED_DAG, run_id).get(f"apply.{FIRST}") or {}).get("state") == "success",
        )
        rows = self.v.guard(f"{cell}/run-is-live", self.running, LIVE) or []
        self.v.grade(
            f"{cell}/run-is-live",
            ([] if paused else [f"the {LIVE} apply never reached {PAUSE_AT.split(':')[0]}"])
            + ([] if first_done else [f"apply.{FIRST} did not succeed beside the paused run"])
            + live(rows, LIVE),
            ok=str(rows),
        )
        before = self.listing("g3-live")
        loads = [load_dag(LIVE), load_dag(FIRST)]
        for dag in loads:
            self.stand.trigger(dag, "gate_s3_cleanup_g3")
        states = self.wait_runs(self.stand, loads, "gate_s3_cleanup_g3")
        tasks = {dag: self.stand.tasks(dag, "gate_s3_cleanup_g3") for dag in loads}
        print(f"CLEANUP-RUN g3 loads {states}", flush=True)
        still = self.v.guard(f"{cell}/still-live-after-load", self.running, LIVE)
        after = self.listing("g3-loaded")
        self.v.grade(f"{cell}/load-succeeds", outcome(states.get(loads[0]), tasks[loads[0]], ["load"]), ok=str(states))
        self.v.grade(
            f"{cell}/still-live-after-load",
            live(still, LIVE) if still is not None else ["no ledger read"],
            ok="the run was live through the load",
        )
        self.v.grade(
            f"{cell}/nothing-deleted", kept(before, after, prefix(LIVE)), ok=f"{len(_under(after, prefix(LIVE)))} object(s)"
        )
        self.v.grade(f"{cell}/other-export-load-succeeds", outcome(states.get(loads[1]), tasks[loads[1]], ["load"]))
        self.v.grade(f"{cell}/other-export-cleaned", cleaned(before, after, prefix(FIRST)))
        state, ext = self.finished(PAUSED_DAG, run_id)
        self.v.grade(f"{cell}/held-run-finished", outcome(state, ext, [f"apply.{e}" for e in self.exports]), ok=str(state))
        before = self.listing("g3-run-ended")
        state, tasks = self.run(load_dag(LIVE), "gate_s3_cleanup_g3_next")
        after = self.listing("g3-next")
        self.v.grade(f"{cell}/next-load-succeeds", outcome(state, tasks, ["load"]), ok=str(state))
        self.v.check(f"{cell}/next-load-delivers", self.equals_source, LIVE)
        self.v.grade(f"{cell}/next-load-cleans", cleaned(before, after, prefix(LIVE)))
