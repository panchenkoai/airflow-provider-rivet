"""The `cleanup_source` cells of the S3 leg (gate/cleanup.py): each oracle RED on the mutant it exists to catch, and the
phase played against a fake world that behaves as rivet promises, then as each broken promise.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from gate import cleanup, destinations
from gate.cleanup import (
    FIRST,
    LIVE,
    break_sql,
    cleaned,
    failed_load,
    kept,
    live,
    mend_sql,
    object_keys,
    outcome,
    physical,
    prefix,
    run_rows,
)
from gate.scenario import Verdicts

# --- The bucket listing ---------------------------------------------------------------------------------------------


def mc_line(key: str, kind: str = "file") -> str:
    return json.dumps({"status": "success", "type": kind, "key": key, "size": 1})


def test_the_listing_is_every_object_key_and_an_mc_error_is_no_empty_bucket() -> None:
    text = "\n".join([mc_line("exports/a/p.parquet"), mc_line("exports/a/", "folder"), "", mc_line("exports/a/manifest.json")])
    assert object_keys(text) == {"exports/a/p.parquet", "exports/a/manifest.json"}
    assert object_keys("") == set()
    with pytest.raises(RuntimeError, match="mc ls"):
        object_keys(json.dumps({"status": "error", "error": {"message": "Bucket does not exist"}}))


P = prefix(LIVE)
BEFORE = {f"{P}r1.parquet", f"{P}manifest-r1.json", f"{P}manifest.json", f"{P}_SUCCESS", "exports/gate_orders_v2/x.parquet"}


def test_kept_passes_when_every_object_under_the_prefix_is_still_there() -> None:
    assert kept(BEFORE, BEFORE | {f"{P}r2.parquet"}, P) == []
    # A sibling prefix that starts with the same name is not this export's.
    assert kept(BEFORE, BEFORE - {"exports/gate_orders_v2/x.parquet"}, P) == []


@pytest.mark.parametrize("gone", [f"{P}r1.parquet", f"{P}manifest-r1.json", f"{P}_SUCCESS"])
def test_kept_fails_on_any_object_deleted(gone: str) -> None:
    assert kept(BEFORE, BEFORE - {gone}, P)


def test_kept_and_cleaned_refuse_to_pass_when_nothing_was_staged() -> None:
    bare = {f"{P}manifest.json"}
    assert kept(bare, bare, P) and cleaned(bare, set(), P)
    assert kept(set(), set(), P) and cleaned(set(), set(), P)


def test_cleaned_passes_when_nothing_staged_before_is_left() -> None:
    assert cleaned(BEFORE, {"exports/gate_orders_v2/x.parquet"}, P) == []
    # An object that appeared during the load (a run that did not exist before it) is not this load's to delete.
    assert cleaned(BEFORE, {f"{P}r9.parquet"}, P) == []


@pytest.mark.parametrize("left", [f"{P}r1.parquet", f"{P}manifest-r1.json", f"{P}manifest.json", f"{P}_SUCCESS"])
def test_cleaned_fails_on_any_object_left(left: str) -> None:
    assert cleaned(BEFORE, {left}, P)


# --- The ledger, the runs, ClickHouse -------------------------------------------------------------------------------


def test_a_live_run_is_a_running_row_of_the_export() -> None:
    rows = run_rows("r4|running|s3://gate-s3-cleanup/exports/gate_orders/\n\n")
    assert rows == [["r4", "running", "s3://gate-s3-cleanup/exports/gate_orders/"]]
    assert live(rows, LIVE) == []
    assert live([], LIVE)
    assert live([["r4", "success", ""]], LIVE)


def test_outcome_needs_the_run_and_every_wanted_task_to_succeed() -> None:
    tasks = {"load": {"state": "success"}, "watcher": {"state": "skipped"}}
    assert outcome("success", tasks, ["load"]) == []
    assert outcome("failed", tasks, ["load"])
    assert outcome("success", {"load": {"state": "failed"}}, ["load"])
    assert outcome("success", {}, ["load"])


def test_a_broken_load_must_fail_with_a_code() -> None:
    inst, payload = {"state": "failed"}, {"error": {"code": "RIVET_LOAD_FAILED"}}
    assert failed_load("failed", inst, payload) == []
    assert failed_load("success", inst, payload)
    assert failed_load("failed", {"state": "success"}, payload)
    assert failed_load("failed", inst, {"error": {"code": None}})
    assert failed_load("failed", inst, None)


def test_the_tables_broken_are_the_exports_own_and_never_a_view() -> None:
    rows = [
        {"name": "gate_customers", "engine": "View"},
        {"name": "gate_customers__changes", "engine": "ReplacingMergeTree"},
        {"name": "gate_customers_v2", "engine": "MergeTree"},
        {"name": "gate_orders", "engine": "MergeTree"},
    ]
    assert physical(rows, "gate_customers") == ["gate_customers__changes"]
    assert physical(rows, "gate_orders") == ["gate_orders"]
    assert break_sql("db", ["t"]) == [f"ALTER TABLE `db`.`t` ADD CONSTRAINT {cleanup.CONSTRAINT} CHECK id < 0"]
    assert mend_sql("db", ["t"]) == [f"ALTER TABLE `db`.`t` DROP CONSTRAINT IF EXISTS {cleanup.CONSTRAINT}"]


def test_the_cleanup_config_is_inits_own_on_a_database_of_its_own() -> None:
    cfg = destinations.CONFIGS[cleanup.KIND]
    assert cfg.mode == "incremental" and cfg.clickhouse_db and cfg.database == cleanup.DATABASE
    argv = destinations.init_argv(cfg, "/o.yaml")
    assert argv[argv.index("--s3-bucket") + 1] == cfg.bucket and "--clickhouse-url" in argv
    leg = destinations.S3Leg()
    assert leg.connections(SimpleNamespace(minio="m", source="p", clickhouse="c"))[cfg.source_conn]["schema"] == cfg.database
    assert set(cleanup.dags(destinations.TABLES)) <= set(leg.probe_dags)
    assert cleanup.shape()["cleanup"]["pause"]["export"] == LIVE != FIRST


# --- The phase against a fake world -----------------------------------------------------------------------------------


class World:
    """A bucket, a ledger, ClickHouse and the DAGs, behaving as rivet promises unless a broken promise is named.

    `breaks`: `failed-load-deletes` (a failed load still deletes its runs), `narrow-load-deletes-all` (a load of one
    export wipes the bucket), `live-ignored` (a live run does not stop the cleanup), `never-cleans`, `fail-never` (the
    constraint does not stop the load), `stale-load` (the load leaves ClickHouse as it was)."""

    def __init__(self, tmp_path: Path, breaks: str = "") -> None:
        self.breaks = breaks
        self.rivet_dir = tmp_path
        (tmp_path / "markers").mkdir()
        self.clickhouse_port, self.secrets = 0, SimpleNamespace(clickhouse="x")
        self.keys: set[str] = set()
        self.source = {t: [{"id": 1, "v": 0}] for t in (FIRST, LIVE)}
        self.ch: dict[str, list] = {}
        self.runs: dict[str, list[dict]] = {t: [] for t in (FIRST, LIVE)}
        self.broken: set[str] = set()
        self.dag_runs: dict[tuple[str, str], tuple[str, dict]] = {}
        self.held: tuple[str, dict] | None = None
        self.n = 0

    # the stand
    def trigger(self, dag: str, run_id: str) -> None:
        if dag == cleanup.EXTRACT_DAG:
            for t in self.runs:
                self.extract(t)
            self.dag_runs[dag, run_id] = ("success", {f"apply.{t}": {"state": "success"} for t in self.runs})
        elif dag == cleanup.PAUSED_DAG:
            self.extract(FIRST)
            self.held = (LIVE, self.extract(LIVE, status="running"))
            (self.rivet_dir / "markers" / cleanup.MARKER).write_text("pg_after_snapshot_open")
            self.dag_runs[dag, run_id] = (
                "running",
                {f"apply.{FIRST}": {"state": "success"}, f"apply.{LIVE}": {"state": "running"}},
            )
        else:
            export = dag.removeprefix("gate_s3_cleanup_load_")
            self.dag_runs[dag, run_id] = self.load(export)

    def extract(self, t: str, status: str = "success") -> dict:
        self.n += 1
        run = {"id": f"r{self.n}", "status": status, "loaded": False, "rows": copy.deepcopy(self.source[t])}
        self.runs[t].append(run)
        p = prefix(t)
        self.keys |= {f"{p}manifest-{run['id']}.json", f"{p}manifest.json", f"{p}_SUCCESS"}
        if status == "success":
            self.keys.add(f"{p}{run['id']}.parquet")
        return run

    def load(self, t: str) -> tuple[str, dict]:
        new = [r for r in self.runs[t] if r["status"] == "success" and not r["loaded"]]
        if t in self.broken and "fail-never" not in self.breaks:
            if "failed-load-deletes" in self.breaks:
                self.delete(t, new)
            return "failed", {"load": {"state": "failed"}}
        for r in new:
            r["loaded"] = True
        if new and "stale-load" not in self.breaks:
            self.ch[t] = copy.deepcopy(new[-1]["rows"])
        live_here = any(r["status"] == "running" for r in self.runs[t])
        if "narrow-load-deletes-all" in self.breaks:
            self.keys.clear()
        elif "never-cleans" not in self.breaks and (not live_here or "live-ignored" in self.breaks):
            self.delete(t, [r for r in self.runs[t] if r["loaded"]])
        return "success", {"load": {"state": "success"}}

    def delete(self, t: str, runs: list[dict]) -> None:
        p = prefix(t)
        for r in runs:
            self.keys -= {f"{p}{r['id']}.parquet", f"{p}manifest-{r['id']}.json"}
        self.keys -= {f"{p}manifest.json", f"{p}_SUCCESS"}

    def tasks(self, dag: str, run_id: str) -> dict:
        return self.dag_runs[dag, run_id][1]

    def xcom(self, dag: str, run_id: str, task: str) -> dict:
        return {"error": {"code": "RIVET_LOAD_FAILED"}, "exit_status": 1}

    def psql(self, service: str, db: str, sql: str) -> str:
        if sql.startswith("SELECT run_id"):
            t = sql.split("export_name = '")[1].split("'")[0]
            return "".join(f"{r['id']}|running|s3://b/{prefix(t)}\n" for r in self.runs[t] if r["status"] == "running")
        assert db == cleanup.DATABASE and sql == cleanup.CHANGES
        for rows in self.source.values():
            rows.append({"id": len(rows) + 1, "v": 0})
            rows[0]["v"] += 1
        return ""

    def wait_runs(self, stand: World, dags: list[str], run_id: str) -> dict[str, str]:
        out = {}
        for dag in dags:
            state, tasks = self.dag_runs[dag, run_id]
            if state == "running":  # the held run ends when the gate waits for it
                t, run = self.held
                run["status"] = "success"
                self.keys.add(f"{prefix(t)}{run['id']}.parquet")
                tasks[f"apply.{t}"] = {"state": "success"}
                state = "success"
                self.dag_runs[dag, run_id] = (state, tasks)
            out[dag] = state
        return out

    # the leg
    def mc(self, stand: World, *args: str) -> SimpleNamespace:
        return SimpleNamespace(stdout="\n".join(mc_line(k) for k in sorted(self.keys)))

    def ch_exec(self, port: int, password: str, sql: str) -> None:
        table = sql.split("`.`")[1].split("`")[0]
        (self.broken.add if "ADD CONSTRAINT" in sql else self.broken.discard)(table)

    def ch_tables(self, port: int, password: str, sql: str) -> list[dict]:
        return [{"name": t, "engine": "MergeTree"} for t in self.ch]

    def source_rows(self, dsn: str) -> dict:
        return copy.deepcopy(self.source)

    def read_clickhouse(self, stand: World, db: str, table: str) -> list[dict]:
        return copy.deepcopy(self.ch.get(table, []))


def play(tmp_path: Path, monkeypatch, breaks: str = "", graded: list[str] | None = None) -> dict[str, str]:
    """The phase's cells against the world; FAIL cells with their detail (every cell graded, in `graded`)."""
    graded = [] if graded is None else graded
    world = World(tmp_path, breaks)
    monkeypatch.setattr(cleanup.time, "sleep", lambda s: None)
    monkeypatch.setattr(cleanup.warehouse, "_ch", world.ch_tables)
    leg = SimpleNamespace(
        mc=world.mc,
        ch_exec=world.ch_exec,
        source_rows=world.source_rows,
        read_clickhouse=world.read_clickhouse,
        columns={t: ["id", "v"] for t in (FIRST, LIVE)},
        cleanup_bucket="gate-s3-cleanup",
        cleanup_db="gate_s3_cleanup",
        cleanup_service="gate-source",
        cleanup_state_db="rivet_state_s3_cleanup",
        cleanup_source_dsn="",
    )
    v = Verdicts("3.3.2")
    cleanup.Phase(leg, world, v, world.wait_runs, [FIRST, LIVE]).play()
    graded.extend(r["cell"] for r in v.rows)
    return {r["cell"]: r["detail"] for r in v.rows if r["status"] == "FAIL"}


def test_the_phase_passes_a_world_that_keeps_every_promise(tmp_path: Path, monkeypatch) -> None:
    graded: list[str] = []
    assert play(tmp_path, monkeypatch, graded=graded) == {}
    assert len(graded) == len(set(graded)) == 25, graded
    assert {c.split("/")[1] for c in graded if c.count("/") == 2} == {
        "failed-load-deletes-nothing",
        "unloaded-runs-survive",
        "live-run-blocks",
    }


@pytest.mark.parametrize(
    ("breaks", "cells"),
    [
        ("failed-load-deletes", {f"{cleanup.G1}/every-object-kept"}),
        ("fail-never", {f"{cleanup.G1}/load-failed", f"{cleanup.G1}/every-object-kept"}),
        ("narrow-load-deletes-all", {f"{cleanup.G2}/other-export-kept", f"{cleanup.G3}/nothing-deleted"}),
        ("live-ignored", {f"{cleanup.G3}/nothing-deleted"}),
        (
            "never-cleans",
            {f"{cleanup.G2}/loaded-export-cleaned", f"{cleanup.G1}/next-load-cleans", f"{cleanup.G3}/next-load-cleans"},
        ),
        ("stale-load", {f"{cleanup.G2}/loaded-export-equals-source", f"{cleanup.G3}/next-load-delivers"}),
    ],
)
def test_the_phase_fails_each_broken_promise(tmp_path: Path, monkeypatch, breaks: str, cells: set[str]) -> None:
    failed = play(tmp_path, monkeypatch, breaks)
    assert cells <= set(failed), failed
