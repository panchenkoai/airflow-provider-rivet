"""The MySQL client stack (MySQL -> GCS -> BigQuery): the config init writes, every rivet task's XCom unit and task log,
the staged parts `cleanup_source` deletes, and a column added while the CDC run is open.

Each test names the mutant it is RED on.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
import yaml
from test_conns import CLOUD
from test_stand_hygiene import laid_out

from gate import sources
from gate.sources import (
    MYSQL,
    SourceLeg,
    column_reached,
    load_block_problems,
    log_problems,
    rows_past,
    staged_left,
    window_problems,
    xcom_problems,
)

FIXTURES = Path(__file__).parent / "fixtures"
BATCH = (FIXTURES / "init_mysql_batch_0.31.0.yaml").read_text()
CDC = (FIXTURES / "init_mysql_cdc_0.31.0.yaml").read_text()


def payload(*rows: int | None, status: str = "success", decision: str = "success") -> dict:
    units = [
        {"export": "cdc", "table": None, "status": status, "run_id": "r", "rows": r, "files": 1, "error": None} for r in rows
    ]
    return {"command": "run", "decision": decision, "units": units, "error": None, "exit_status": 0}


def test_the_load_block_init_writes_is_the_client_stacks() -> None:
    # Mutant: `!=` -> `==` in load_block_problems (every key reported, or none).
    assert load_block_problems(BATCH) == [] and load_block_problems(CDC) == []
    doc = yaml.safe_load(CDC)
    doc["load"].pop("layout")
    doc["load"]["cleanup_source"] = False
    assert load_block_problems(yaml.safe_dump(doc)) == [
        "load.layout is None, expected 'base_buffer'",
        "load.cleanup_source is False, expected True",
    ]
    # Mutant: dropping the "no load block" branch (a config without one passes as the client's).
    assert load_block_problems("exports: []\n") == ["init wrote no top-level `load:` block"]


def test_an_xcom_unit_must_report_the_rows_the_gate_made() -> None:
    # Mutant: `unit_rows(payload) != rows` -> `==`, or the check dropped: 164 rows of 165 would pass.
    assert xcom_problems(payload(100, 65), 165) == []
    assert xcom_problems(payload(100, 64), 165) == ["units report 164 row(s), the source changed 165"]
    # Mutant: rows=None read as 0 (a compaction is not counted).
    assert xcom_problems(payload(None), None) == []
    # Mutant: units that report no count summed as 0 (`unit_rows` returning 0 instead of None).
    assert xcom_problems(payload(None), 0) == ["units report None row(s), the source changed 0"]


def test_an_xcom_unit_that_did_not_do_its_work_fails() -> None:
    # Mutant: the decision / unit-status checks dropped.
    assert xcom_problems(payload(3, decision="fail"), 3)[0].startswith("decision 'fail'")
    assert xcom_problems(payload(3, status="already_applied"), 3) == ["units not done: [('cdc', None, 'already_applied')]"]
    assert xcom_problems(payload(), None) == ["no units"]
    assert xcom_problems(None, 3) == ["no XCom return value (None)"]
    assert xcom_problems(payload(0, status="skipped"), 0) == [], "a compaction with nothing to merge reports skipped"


LOG = """[2026-10-21, 10:00:01 UTC] {tasklog.py:90} INFO - rivet run cdc stderr: wrote part 1
[2026-10-21, 10:00:02 UTC] {tasklog.py:90} INFO - rivet load cdc table gate_orders → p.d.gate_orders__changes: OK: 165 rows, 1 file in 4.1 s (run r)
"""


def test_the_task_log_headline_says_what_the_xcom_says() -> None:
    # Mutant: the headline's row count not compared (or compared to a constant).
    assert log_problems(LOG, payload(165)) == []
    assert log_problems(LOG, payload(164)) == ["headline says 165 rows, the XCom units 164"]
    # Mutant: any headline status accepted.
    failed = LOG.replace("OK: 165 rows", "FAILED [RIVET_X] refusal: 165 rows")
    assert log_problems(failed, payload(165))[0].startswith("headline 'rivet load cdc table gate_orders")
    # SKIPPED only where every unit skipped.
    skipped = LOG.replace("OK: 165 rows, 1 file", "SKIPPED: nothing to do")
    assert log_problems(skipped, payload(None, status="skipped")) == []
    assert log_problems(skipped, payload(None))[0].endswith("expected OK")
    assert log_problems(None, payload(1)) == ["no task log"]
    assert log_problems("nothing here", payload(1)) == ["no `rivet ... →` headline in the task log"]


def test_an_incremental_extract_reads_the_rows_past_the_high_water() -> None:
    # Mutant: `>` -> `>=` (the row at the high-water re-read) or the None branch dropped (first run reads nothing).
    t = [datetime(2026, 1, 1, h) for h in range(4)]
    rows = [{"updated_at": x} for x in t]
    assert rows_past(rows, "updated_at", None) == 4
    assert rows_past(rows, "updated_at", t[1]) == 2
    assert sources.high_water(rows, "updated_at") == t[3] and sources.high_water([], "updated_at") is None


def test_every_value_of_the_added_column_must_reach_the_warehouse() -> None:
    source = [{"id": 1, "note": None}, {"id": 41, "note": "примітка 41"}, {"id": 31, "note": "тільки примітка"}]
    got = [
        {"id": 1, "note": None, "__is_deleted": False},
        {"id": 41, "note": "примітка 41", "__is_deleted": False},
        {"id": 31, "note": "тільки примітка", "__is_deleted": False},
        {"id": 100, "note": "x", "__is_deleted": True},
    ]
    assert column_reached(got, source, "note", "id") == []
    # Mutant: the comparison of values dropped (a NULL where the source has a value passes).
    lost = [{**r, "note": None} if r["id"] == 41 else r for r in got]
    assert column_reached(lost, source, "note", "id")[0].startswith("1 of 2 value(s) of note not in the warehouse")
    # Mutant: the vacuity guard dropped (an all-NULL column at both ends passes).
    assert column_reached(got, [{"id": 1, "note": None}], "note", "id") == [
        "the source holds no value of note: nothing to compare"
    ]
    # Mutant: the deleted-flag filter dropped (a deleted row's value counts as one the source lacks, or hides a loss).
    assert column_reached([{**got[3], "__is_deleted": False}, *got[:3]], source, "note", "id")[0].startswith("1 value(s)")
    assert column_reached([{"id": 41}], source, "note", "id")[0].startswith("no column note")


def test_the_alter_must_land_while_the_run_is_still_paused() -> None:
    # Mutant: `>` -> `<`, or the margin dropped (an ALTER that ended as the pause did passes).
    assert window_problems(100.0, 110.0, 45000) == []
    assert window_problems(100.0, 141.0, 45000)[0].startswith("the ALTER and its writes ended 41.0 s")
    assert window_problems(None, 0.0, 45000) == ["the CDC run never reached cdc_after_open (no marker)"]


def test_no_staged_parquet_may_be_left_after_the_load() -> None:
    # Mutant: the `.parquet` filter dropped (a manifest left counts) or inverted.
    assert staged_left(["gs://b/cdc/manifest.json", "gs://b/cdc/_SUCCESS"]) == []
    assert staged_left(["gs://b/cdc/gate_orders/part-1.parquet"]) == [
        "1 staged Parquet object(s) left after the load, e.g. ['gs://b/cdc/gate_orders/part-1.parquet']"
    ]


def prepared(tmp_path: Path, monkeypatch) -> tuple[SourceLeg, object]:
    stand = laid_out(tmp_path, sources=(MYSQL,))
    leg = SourceLeg(CLOUD, "3.3.2", MYSQL)

    def compose(*args, extra_env=None, **kw):
        if "airflow-worker" in args:
            out = Path(str(stand.rivet_dir) + args[-1].removeprefix("/opt/rivet")).parent
            (out / "rivet.yaml").write_text(CDC if "cdc" in args[args.index("--mode") + 1] else BATCH)

    monkeypatch.setattr(stand, "compose", compose)
    monkeypatch.setattr(stand, "psql", lambda *a: "")
    monkeypatch.setattr(SourceLeg, "wait_ready", lambda self, stand: None)
    monkeypatch.setattr(SourceLeg, "sql", lambda self, stand, text: "")
    monkeypatch.setattr(sources, "sh", lambda *a, **k: None)
    monkeypatch.setattr(sources.warehouse, "_bq", lambda *a, **k: None)
    leg.prepare(stand)
    return leg, stand


def test_the_hot_dag_runs_the_streams_tasks_on_its_config_and_state(tmp_path: Path, monkeypatch) -> None:
    leg, stand = prepared(tmp_path, monkeypatch)
    # Mutant: the hot DAG added to `dags` (it would run, unpaused, in every cycle beside the stream's own DAG).
    assert leg.dags == ["gate_src_mysql_batch", "gate_src_mysql_cdc"]
    assert leg.hot_dag == "gate_src_mysql_cdc_hot"
    assert leg.tasks[leg.hot_dag] == leg.tasks["gate_src_mysql_cdc"]
    assert leg.may_skip[leg.hot_dag] == ("compact.",)
    shape = json.loads((stand.rivet_dir / "configs" / "mysql_shape.json").read_text())
    assert shape["hot"] == {
        "dag": "gate_src_mysql_cdc_hot",
        "stream": {"name": "cdc", "tables": ["gate_customers", "gate_orders"], "dag": "_cdc"},
        "pause": "cdc_after_open:45000",
        "marker": "/opt/rivet/markers/mysql_hot",
    }
    assert leg.texts == {"batch": BATCH, "cdc": CDC}
    dag = (Path(sources.__file__).parent / "stand" / "dags" / "rivet_gate.py").read_text()
    block = dag[dag.index('hot = shape.get("hot")') : dag.index("# Destination legs")]
    assert '"RIVET_TEST_PAUSE_AT": hot["pause"]' in block and 'state_dir=f"{ROOT}/state/{engine}_cdc"' in block


def test_the_rows_each_task_moved_come_from_what_the_gate_did(tmp_path: Path, monkeypatch) -> None:
    leg, _ = prepared(tmp_path, monkeypatch)
    t = [datetime(2026, 1, 1, h) for h in range(3)]
    source = {
        "gate_customers": [{"updated_at": t[0]}, {"updated_at": t[2]}],
        "gate_orders": [{"updated_at": t[1]}, {"updated_at": t[2]}, {"updated_at": t[2]}],
    }
    ops = MYSQL.ops["delta"]
    # Mutant: the baseline left out of the first run, or added to every run.
    assert leg.expected_rows("cdc", "run", source, ops, baseline=False) == 165
    assert leg.expected_rows("cdc", "run", source, {}, baseline=True) == 5
    # Mutant: a CDC load counted over every table, not its own.
    assert leg.expected_rows("cdc", "load.gate_orders", source, ops, baseline=False) == 135
    assert leg.expected_rows("cdc", "compact.gate_orders", source, ops, baseline=False) is None
    leg.high_water = {"gate_orders": t[1]}
    # Mutant: the kind read from the unit's name (init names the CDC config's recipes after the tables, as the batch
    # exports): a batch unit counted as change events, or a CDC load as rows past the high-water.
    assert leg.expected_rows("batch", "apply.gate_orders", source, ops, baseline=False) == 2
    assert leg.expected_rows("batch", "load.gate_orders", source, ops, baseline=False) == 2


def test_a_task_log_is_the_newest_attempt_of_its_task(tmp_path: Path) -> None:
    stand = type("S", (), {"dir": tmp_path})()
    root = tmp_path / "logs" / "dag_id=d" / "run_id=r" / "task_id=load.gate_orders"
    root.mkdir(parents=True)
    for n in (1, 2, 10):
        (root / f"attempt={n}.log").write_text(f"try {n}")
    # Mutant: sorted as text (attempt=2 after attempt=10).
    assert sources.task_log(stand, "d", "r", "load.gate_orders") == "try 10"
    assert sources.task_log(stand, "d", "r", "load.gate_customers") is None


@pytest.mark.parametrize("cycle", ["delta"])
def test_the_counted_changes_are_the_ones_the_engine_makes(cycle: str) -> None:
    changes = MYSQL.changes
    ops = MYSQL.ops[cycle]
    assert "LIMIT 30" in changes and ops["gate_orders"]["update"] == 30
    assert "LIMIT 10" in changes and ops["gate_customers"]["update"] == 10
    assert "LIMIT 5" in changes and ops["gate_orders"]["delete"] == 5
    hot = MYSQL.hot
    assert "BETWEEN 31 AND 40" in hot.before and hot.ops_before["gate_orders"] == {"update": 10}
    assert hot.after.count("DELETE") == hot.deleted == hot.ops_after["gate_orders"]["delete"]
    assert hot.column in hot.alter and "NULL" in hot.alter.split(hot.column, 1)[1]
