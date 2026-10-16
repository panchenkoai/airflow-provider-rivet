"""What a DAG author reads from a builder: every refusal's sentence, the tags, the start date, the ids, the plan file
and the export each task names (mutants dags.x_*)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from airflow_provider_rivet.dags import build_batch_dag, build_cdc_dag, read_plan_layout, task_name


@pytest.fixture
def plan_file(tmp_path: Path) -> str:
    """A plan layout with one wave of two exports."""
    campaign = {"waves": [{"wave": 1, "exports": ["a", "b"]}], "ordered_exports": []}
    path = tmp_path / "pg.plan.json"
    path.write_text(json.dumps([{"export_name": "a", "prioritization": {"campaign": campaign}}]))
    return str(path)


@pytest.mark.parametrize(
    ("build", "message"),
    [
        (
            lambda pf: build_batch_dag("b", config="c", plan_file=pf, exports=["a"]),
            "give exactly one of `plan_file` (waves from rivet plan) or `exports` (one wave)",
        ),
        (
            lambda pf: build_batch_dag("b", config="c"),
            "give exactly one of `plan_file` (waves from rivet plan) or `exports` (one wave)",
        ),
        (
            lambda pf: build_batch_dag("b", config="c", exports=[]),
            "`exports` is empty: name at least one export, or give `plan_file`",
        ),
        (lambda pf: build_batch_dag("b", config="c", exports=["a"], extract="plan"), "`extract` is 'apply' or 'run'"),
        (
            lambda pf: build_batch_dag("b", config="c", exports=["a"], compact_exports=["z", "y"]),
            "compact_exports names exports the plan does not have: ['y', 'z']",
        ),
        (
            lambda pf: build_batch_dag("b", config="c", exports=["a"], compact_exports=["a"], load=False),
            "compact_exports needs load=True: a compaction merges what the load buffered",
        ),
        (
            lambda pf: build_cdc_dag("c", config="c", export="e", tables=[]),
            "`tables` lists the captured tables the load and compact tasks are built for",
        ),
        (
            lambda pf: build_cdc_dag("c", config="c", export="e", tables=["t"], operator_kwargs={"trigger_rule": "x"}),
            "operator_kwargs may not set `trigger_rule`: every rivet task runs only after its upstream succeeded "
            "(load after its extract, compact after its load); the watcher fails the run on any failure",
        ),
        (
            lambda pf: build_batch_dag("b", config="c", exports="ab"),
            "`exports` is a list of names, not a string: write exports=['ab']",
        ),
    ],
)
def test_each_builder_refusal_says_what_to_write_instead(plan_file: str, build: Any, message: str) -> None:
    with pytest.raises(ValueError) as caught:
        build(plan_file)
    assert str(caught.value) == message


def test_a_missing_or_foreign_plan_file_names_the_error_and_the_command_that_writes_it(tmp_path: Path) -> None:
    absent = str(tmp_path / "absent.json")
    with pytest.raises(RuntimeError) as caught:
        read_plan_layout(absent)
    assert str(caught.value) == (
        f"{absent} is missing or is not `rivet plan --format json` output (FileNotFoundError); "
        f"write it with: rivet plan --config <config> --format json > {absent}"
    )
    garbage = tmp_path / "garbage.json"
    garbage.write_text("{not json")
    with pytest.raises(RuntimeError, match=r"output \(JSONDecodeError\);"):
        read_plan_layout(str(garbage))


def test_the_dags_are_tagged_and_start_on_the_first_of_january_2026(plan_file: str) -> None:
    batch = build_batch_dag("b", config="c", plan_file=plan_file)
    cdc = build_cdc_dag("c", config="c", export="e", tables=["t"])
    assert sorted(batch.tags) == ["batch", "rivet"] and sorted(cdc.tags) == ["cdc", "rivet"]
    for dag in (batch, cdc):
        assert dag.start_date.year == 2026 and (dag.start_date.month, dag.start_date.day) == (1, 1)
    assert build_batch_dag("x", config="c", exports=["a"], tags=["mine"]).tags in (["mine"], {"mine"})


def test_the_plan_task_refreshes_the_plan_file_the_builder_read(plan_file: str) -> None:
    dag = build_batch_dag("b", config="c", plan_file=plan_file)
    assert dag.get_task("plan").plan_file == plan_file
    assert "plan" not in build_batch_dag("b2", config="c", plan_file=plan_file, refresh_plan=False).task_ids


def test_every_cdc_load_and_compact_names_the_stream_export_and_its_table() -> None:
    dag = build_cdc_dag("c", config="c", export="app_cdc", tables=["t"])
    for task_id in ("run", "load.t", "compact.t"):
        assert dag.get_task(task_id).export == "app_cdc", task_id
    assert (dag.get_task("load.t").table, dag.get_task("compact.t").table) == ("t", "t")


def test_a_sanitised_task_id_ends_with_eight_hex_digits_of_the_names_hash() -> None:

    assert task_name("daily orders") == "daily_orders_" + hashlib.sha256(b"daily orders").hexdigest()[:8]
    assert task_name("") == "_" + hashlib.sha256(b"").hexdigest()[:8]
    assert task_name("ok.name-1") == "ok.name-1"


def test_the_watcher_is_never_retried() -> None:
    dag = build_batch_dag("b", config="c", exports=["a"], default_args={"retries": 5})
    assert dag.get_task("watcher").retries == 0
