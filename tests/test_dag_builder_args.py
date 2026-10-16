"""What a DAG author reads from a builder: every refusal's sentence, the tags, the start date, the ids, the plan file
and the export each task names (mutants dags.x_*)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from airflow_provider_rivet.dags import build_batch_dag, build_cdc_dag, read_plan_layout, task_name
from airflow_provider_rivet.invocation import plan_layout
from airflow_provider_rivet.operators import CLOUD_ENV, RivetApplyOperator, RivetLoadOperator, RivetRunOperator


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


def test_every_rivet_task_reads_the_builders_config_and_state_directory(plan_file: str) -> None:
    batch = build_batch_dag("b", config="/c/pg.yaml", state_dir="/state", plan_file=plan_file, compact_exports=["a"])
    cdc = build_cdc_dag("c", config="/c/cdc.yaml", state_dir="/cdc-state", export="e", tables=["t"])
    for dag, config, state_dir in ((batch, "/c/pg.yaml", "/state"), (cdc, "/c/cdc.yaml", "/cdc-state")):
        tasks = [t for t in dag.tasks if hasattr(t, "state_dir")]
        assert tasks and {(t.config, t.state_dir) for t in tasks} == {(config, state_dir)}


def test_the_watcher_belongs_to_its_dag_and_watches_every_other_task() -> None:
    dag = build_cdc_dag("c", config="c", export="e", tables=["t"])
    watcher = dag.get_task("watcher")
    assert watcher.dag is dag and set(watcher.upstream_task_ids) == set(dag.task_ids) - {"watcher"}


def test_the_watcher_takes_the_dags_default_args_except_retries_and_the_failure_callback() -> None:
    """It runs where the author's other tasks run (owner, queue, pool from `default_args`); it is never retried and
    never alerts on its own."""
    args = {"owner": "data-team", "queue": "rivet", "retries": 5, "on_failure_callback": print}
    for dag in (
        build_batch_dag("b", config="c", exports=["a"], default_args=args),
        build_cdc_dag("c", config="c", export="e", tables=["t"], default_args=args),
    ):
        watcher = dag.get_task("watcher")
        assert (watcher.owner, watcher.queue, watcher.retries) == ("data-team", "rivet", 0)
        assert not watcher.on_failure_callback


# ---- what a builder reads and builds with: the plan layout and an operator's arguments ----
# The builders call these, so the PR mutation gate grades them with this file: invocation.x_plan_layout__mutmut_*
# and operators.xǁRivetBaseOperatorǁ__init____mutmut_*.


def one_export_plan(**rec: Any) -> dict:
    """`rivet plan --format json` for one planned export: its bare object, no campaign (D10)."""
    return {"export_name": "a", "prioritization": {"export_recommendation": {"recommended_wave": 2, **rec}}}


@pytest.mark.parametrize(
    ("plan", "layout"),
    [
        (one_export_plan(export_name="a", cost_class="low"), [{"wave": 2, "exports": ["a"], "heavy": []}]),
        (one_export_plan(export_name="a", cost_class="high"), [{"wave": 2, "exports": ["a"], "heavy": ["a"]}]),
        (
            [{"prioritization": {"campaign": {"waves": [{"wave": 1, "exports": ["x", "y"]}, {"wave": 0, "exports": []}]}}}],
            [{"wave": 1, "exports": ["x", "y"], "heavy": ["x", "y"]}],
        ),
    ],
)
def test_the_plan_layout_of_one_export_or_a_campaign_without_costs(plan: Any, layout: list) -> None:
    """One export's wave and cost come from its own recommendation; a campaign with no costs makes every export heavy."""
    assert plan_layout(plan) == layout


@pytest.mark.parametrize(
    ("plan", "message"),
    [
        (one_export_plan(export_name="b"), "the plan's recommendation names another export"),
        ({**one_export_plan(), "export_name": 5}, "the plan's recommendation names another export"),
        ([{"prioritization": {"campaign": {"waves": [{"wave": 1, "exports": []}]}}}], "the plan has no wave with an export"),
    ],
)
def test_a_plan_that_is_no_layout_says_why(plan: Any, message: str) -> None:
    with pytest.raises(ValueError) as caught:
        plan_layout(plan)
    assert str(caught.value) == message


def test_a_recommendation_without_its_export_name_is_no_plan_layout(tmp_path: Path) -> None:
    """The refusal names the error type: a recommendation that names no export is not read as naming this one."""
    path = tmp_path / "one.plan.json"
    path.write_text(json.dumps(one_export_plan(cost_class="low")))
    with pytest.raises(RuntimeError, match=r"output \(KeyError\);"):
        read_plan_layout(str(path))


def test_an_operators_defaults_are_the_documented_ones() -> None:
    op = RivetRunOperator(task_id="t", config="c.yaml", export="e")
    assert (op.rivet_bin, op.deployment, op.lock_wait, op.crashed_retries, op.stream_stderr, op.log_keep) == (
        "rivet",
        "auto",
        600,
        1,
        False,
        10,
    )
    assert (op.env, op.env_from_connections, op.extra_args, op.stderr_dir, op.cwd) == (None, None, None, None, None)
    assert (op.env_passthrough, op.cloud_credentials, op.connections) == ((), (), ())


def test_an_operators_arguments_are_kept_as_given() -> None:
    op = RivetRunOperator(
        task_id="t",
        config="c.yaml",
        export="e",
        rivet_bin="/opt/rivet",
        env={"A": "1"},
        env_from_connections={"B": "c"},
        env_passthrough=["P"],
        cloud_credentials=sorted(CLOUD_ENV)[:1],
        connections=["conn"],
        deployment="pod",
        extra_args=["-p", "X=1"],
        lock_wait=5,
        crashed_retries=3,
        stderr_dir="/e",
        stream_stderr=True,
        log_keep=None,
        cwd="/w",
    )
    assert (op.rivet_bin, op.env, op.env_from_connections, op.env_passthrough) == ("/opt/rivet", {"A": "1"}, {"B": "c"}, ("P",))
    assert (op.cloud_credentials, op.connections, op.deployment, op.extra_args) == (
        tuple(sorted(CLOUD_ENV)[:1]),
        ("conn",),
        "pod",
        ["-p", "X=1"],
    )
    assert (op.lock_wait, op.crashed_retries, op.stderr_dir, op.stream_stderr, op.log_keep, op.cwd) == (
        5,
        3,
        "/e",
        True,
        None,
        "/w",
    )


def test_an_operator_refuses_a_missing_export_or_an_unknown_cloud_by_name() -> None:
    with pytest.raises(ValueError) as caught:
        RivetApplyOperator(task_id="t", config="c.yaml")
    assert str(caught.value) == "RivetApplyOperator needs `export`"
    assert RivetLoadOperator(task_id="t2", config="c.yaml").export is None
    with pytest.raises(ValueError) as caught:
        RivetRunOperator(task_id="t", config="c.yaml", cloud_credentials=["nope", "also"])
    assert str(caught.value) == f"cloud_credentials names {sorted(CLOUD_ENV)}, got ['also', 'nope']"
