"""What a DAG author and Airflow see of an operator that the other tests left unpinned: its defaults, the exact payload,
record and exception of a refusal, a stop and a busy export, the run id and map index a try is named by, the private
key folder, and the crashed and max_events records. Mutant ids (mutmut, `airflow_provider_rivet.operators`) are in the
comments."""

from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from airflow.exceptions import AirflowException, AirflowFailException, AirflowNotFoundException
from conftest import NEW_FLAGS, FakeTI, Rig, fixture

from airflow_provider_rivet import operators
from airflow_provider_rivet.invocation import Busy, Stopped
from airflow_provider_rivet.operators import (
    RivetApplyOperator,
    RivetCdcRunOperator,
    RivetLoadOperator,
    RivetRunOperator,
    RivetRunWatcher,
    get_connection,
)
from airflow_provider_rivet.preflight import PreflightRefusal

PG_STATE = {"RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"}
FAILED_APPLY = {"exit": 2, "line": {"error": "network", "exit_class": 2}}


def raising(exc: Exception) -> Any:
    """`invoke` that raises before any rivet process starts."""

    def invoke(*_: Any) -> Any:
        raise exc

    return invoke


def test_the_defaults_a_dag_author_relies_on() -> None:
    """__init__ 1-4: the binary on PATH and the detected deployment; 12: the error names the operator class."""
    op = RivetRunOperator(task_id="t", config="c.yaml")
    assert (op.rivet_bin, op.deployment) == ("rivet", "auto")
    with pytest.raises(ValueError) as exc:
        RivetApplyOperator(task_id="t", config="c.yaml")
    assert str(exc.value) == "RivetApplyOperator needs `export`"


def test_a_missing_connection_names_the_id_and_the_remedy(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_connection 8, 9: the refusal text is what the operator reads in the task log."""

    def missing(conn_id: str) -> Any:
        raise AirflowNotFoundException(conn_id)

    monkeypatch.setattr(operators.BaseHook, "get_connection", missing)
    with pytest.raises(PreflightRefusal) as exc:
        get_connection("warehouse")
    assert str(exc.value) == (
        "[RIVET_AIRFLOW_CONNECTION_MISSING] connection `warehouse` is not defined in the metadata database or any secrets "
        "backend this worker reads; create it, or correct the id the task names"
    )


def test_the_run_watcher_fails_the_dag_run_with_its_own_text() -> None:
    """RivetRunWatcher.execute 1-4."""
    with pytest.raises(AirflowFailException) as exc:
        RivetRunWatcher(task_id="watch").execute({})
    assert str(exc.value) == "a task of this DAG run failed; see the failed and upstream_failed tasks"


def test_a_refusal_raised_by_the_invocation_is_published_and_final(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """_refuse 6, 7, 12, 13: the record and the payload Airflow's callbacks and the Slack alert read."""
    refusal = PreflightRefusal("RIVET_AIRFLOW_EXPORT_UNKNOWN", "config c.yaml has no export `orders`")
    monkeypatch.setattr(operators, "invoke", raising(refusal))
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowFailException and str(exc) == str(refusal)
    assert ti.store[("task", "return_value")] == {
        "command": "apply",
        "decision": "fail",
        "preflight_refusal": "RIVET_AIRFLOW_EXPORT_UNKNOWN",
        "units": [],
    }
    assert rig.events("rivet.refused") == [{"event": "rivet.refused", "code": refusal.code, "text": str(refusal)}]


def test_a_stop_before_rivet_started_is_published_and_retried(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """_run_with_secrets 62-81, 84, 90."""
    stop = Stopped("plan")
    monkeypatch.setattr(operators, "invoke", raising(stop))
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowException and str(exc) == str(stop)
    assert ti.store[("task", "return_value")] == {
        "command": "apply",
        "decision": "retry",
        "stopped_before": "plan",
        "units": [],
    }
    assert rig.events("rivet.stopped") == [{"event": "rivet.stopped", "step": "plan", "text": str(stop)}]


def test_a_busy_export_is_published_and_retried_and_leaves_no_process_to_stop(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """_run_with_secrets 93-104, 111, 112; 123: a stop after the try (on_kill) finds no process left from it."""
    busy = Busy(Path("/state/orders.lock"), "orders", 3)
    monkeypatch.setattr(operators, "invoke", raising(busy))
    op = rig.operator(RivetApplyOperator, export="orders")
    exc, ti = rig.run(op)
    assert type(exc) is AirflowException and str(exc) == str(busy)
    assert ti.store[("task", "return_value")] == {"command": "apply", "decision": "retry", "busy": busy.code, "units": []}
    assert rig.events("rivet.busy") == [{"event": "rivet.busy", "code": busy.code, "text": str(busy)}]
    op.on_kill()


def test_the_crashed_record_says_whether_airflow_has_a_try_left(rig: Rig) -> None:
    """_settle 106-112, 117: on the last try Airflow has (try_number == max_tries) one try is still left."""
    rig.scenario(apply={"signal": "SIGKILL"})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"), FakeTI(try_number=2, max_tries=2))
    payload = ti.store[("task", "return_value")]
    [crashed] = rig.events("rivet.crashed")
    assert crashed == {
        "event": "rivet.crashed",
        "retry": payload["crashed_retry"],
        "budget": 1,
        "try_number": 2,
        "airflow_tries_left": True,
        "ledger_problem": None,
    }
    assert str(exc).startswith("crashed") and str(exc).endswith("(orders)"), str(exc)


def test_a_crash_on_a_pod_keeps_no_ledger_in_the_state_directory(rig: Rig) -> None:
    """_settle 59: a pod with PostgreSQL state has no marker, so its crash ledger is not written into `state_dir`."""
    rig.scenario(run={"signal": "SIGKILL"})
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders", deployment="pod", env=PG_STATE))
    payload = ti.store[("task", "return_value")]
    assert payload["error"]["class"] == "crashed" and "crashed_ledger" in payload["degraded"]
    assert type(exc) is AirflowException, "the first try's crash retry needs no ledger"
    assert [(e["retry"], e["ledger_problem"]) for e in rig.events("rivet.crashed")] == [(True, None)]
    assert list(rig.state_dir.iterdir()) == []


def test_the_max_events_record_says_a_backlog_remains(rig: Rig) -> None:
    """_settle 35-38."""
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": [fixture("run_entry.json")]}})
    rig.run(rig.operator(RivetCdcRunOperator, export="orders_cdc"))
    assert rig.events("rivet.cdc.max_events") == [
        {"event": "rivet.cdc.max_events", "exports": ["orders_cdc"], "text": "stopped on max_events; backlog remains"}
    ]


def test_a_failure_of_several_exports_names_none_of_them_in_the_exception(rig: Rig) -> None:
    """_settle 51: with two failed units the exception names the task's export (none here), not the first failure."""
    summary = {"per_export": [failed("users"), failed("orders")]}
    rig.scenario(run={"exit": 2, "line": {"error": "2 export(s) failed", "exit_class": 2}, "summary": summary})
    exc, _ = rig.run(rig.operator(RivetRunOperator))
    assert type(exc) is AirflowException and not str(exc).endswith(")"), str(exc)


def failed(name: str) -> dict:
    """A failed `per_export` entry as today's binary writes it."""
    return {"export_name": name, "status": "failed", "run_id": f"{name}_1", "rows": 0, "files": 0, "mode": "full"}


def stderr_of(rig: Rig, ti: Any, context: dict) -> str:
    """The stderr file one execution names: its folder and name carry the run id the try was given."""
    op = rig.operator(RivetRunOperator, export="orders")
    return op.execute({"ti": ti, **context})["stderr_path"]


def test_the_run_id_comes_from_the_context_then_the_task_instance_then_run(rig: Rig) -> None:
    """execute 6-19."""
    named = stderr_of(rig, FakeTI(), {"run_id": "scheduled__1"})
    other_ti = FakeTI()
    other_ti.run_id = "manual__elsewhere"
    assert stderr_of(rig, other_ti, {"run_id": "scheduled__1"}) == named, "the context's run id wins"
    from_ti = FakeTI()
    from_ti.run_id = "scheduled__1"
    assert stderr_of(rig, from_ti, {}) == named, "without one in the context, the task instance's"
    bare = FakeTI()
    del bare.run_id
    assert stderr_of(rig, bare, {}) == stderr_of(rig, FakeTI(), {"run_id": "run"}), "with neither, `run`"


def artifact_of(rig: Rig, ti: FakeTI) -> str:
    """The sealed plan an apply try replays (the apply fails, so each try starts `rivet apply` again)."""
    exc, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"), ti)
    assert type(exc) is AirflowException, exc  # the failed apply, not an error of naming the try
    return rig.argv("apply")[1]


def test_an_unmapped_task_is_named_alike_with_or_without_a_map_index(rig: Rig) -> None:
    """execute 23-30: Airflow's -1 and a task instance with no map_index name one task; a mapped one is its own."""
    rig.scenario(apply=FAILED_APPLY)
    unmapped = artifact_of(rig, FakeTI())
    bare = FakeTI()
    del bare.map_index
    assert artifact_of(rig, bare) == unmapped
    mapped = FakeTI()
    mapped.map_index = 3
    assert artifact_of(rig, mapped) != unmapped


def test_the_key_folder_is_the_tries_own_and_is_removed_even_when_part_of_it_cannot_be(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """execute 48-50: the prefix the leak checks look for (test_process_io); 60-63: a removal that fails part-way does
    not fail a task that succeeded."""
    seen: list[Path] = []
    real = operators.RivetBaseOperator._run_with_secrets

    def spy(self: Any, ti: Any, run_id: str, task_part: str, secrets: Path) -> Any:
        seen.append(secrets)
        stuck = secrets / "stuck"
        stuck.mkdir()
        (stuck / "key").write_text("k")
        stuck.chmod(0o500)
        return real(self, ti, run_id, task_part, secrets)

    monkeypatch.setattr(operators.RivetBaseOperator, "_run_with_secrets", spy)
    try:
        payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
        assert isinstance(payload, dict) and payload["decision"] == "success", payload
        [secrets] = seen
        assert secrets.parent == Path(tempfile.gettempdir()) and secrets.name.startswith("rivet-airflow-secrets-")
    finally:
        for folder in seen:
            if (folder / "stuck").exists():
                (folder / "stuck").chmod(0o700)


class PullsNone(FakeTI):
    """A task instance whose `xcom_pull` of the upstream tasks finds nothing at all."""

    def xcom_pull(self, task_ids: Any = None, key: str = "return_value") -> Any:
        return None


class PullsTheValue(FakeTI):
    """A task instance whose `xcom_pull` of one upstream task returns that task's value itself, not a sequence."""

    def xcom_pull(self, task_ids: Any = None, key: str = "return_value") -> Any:
        return self.store.get((list(task_ids)[0], key))


def test_upstream_markers_are_read_from_nothing_from_one_value_and_from_a_sequence(rig: Rig) -> None:
    """_upstream_markers 10 (nothing pulled) and 13 (one value pulled bare): the state-directory check still runs."""
    store: dict = {}
    first = rig.operator(RivetApplyOperator, task_id="apply.orders", export="orders")
    payload, _ = rig.run(first, FakeTI("apply.orders", store=store))
    store[("apply.orders", "return_value")] = payload
    other = rig.tmp / "other-state"
    other.mkdir()
    load = rig.operator(RivetLoadOperator, task_id="load.orders", export="orders", state_dir=str(other))
    load.upstream_task_ids.add("apply.orders")
    exc, _ = rig.run(load, PullsTheValue("load.orders", store=store))
    assert isinstance(exc, AirflowFailException)
    assert store[("load.orders", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_STATE_DIR_DIFFERS"
    alone = rig.operator(RivetLoadOperator, task_id="load.orders", export="orders")
    alone.upstream_task_ids.add("apply.orders")
    done, _ = rig.run(alone, PullsNone("load.orders", store={}))
    assert isinstance(done, dict) and done["decision"] == "success", done


def test_without_connections_the_binary_is_checked_before_the_config_is_read(rig: Rig) -> None:
    """_environment 21: a task with no Connections reads no config itself, so the preflight's order holds: a binary too
    old is the refusal, also when the config cannot be read."""
    rig.scenario(version="0.30.9")
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders", config=str(rig.tmp / "missing.yaml")))
    assert isinstance(exc, AirflowFailException)
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_BINARY_TOO_OLD"


@pytest.mark.parametrize(
    ("variable", "kwargs"),
    [("CH_PW_VAR", {"env_passthrough": ["CH_PW_VAR"]}), ("AWS_CH_PW_VAR", {"cloud_credentials": ["aws"]})],
    ids=["env_passthrough", "cloud_credentials"],
)
def test_a_load_name_resolves_from_the_workers_variables_the_task_passes(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, variable: str, kwargs: dict
) -> None:
    """_config_doc 4, 5: `load.password_env: ${VAR}` resolves from a worker variable the task passes to rivet, by
    `env_passthrough` or by a cloud's variables, as rivet resolves it; the Connection's password goes where it names."""
    ch = SimpleNamespace(
        conn_id="ch", conn_type="clickhouse", login="loader", password="pw", host="ch", port=None, schema=None, extra_dejson={}
    )
    monkeypatch.setattr(operators.BaseHook, "get_connection", lambda conn_id: ch)
    monkeypatch.setenv(variable, "CH_PASSWORD")
    rig.config.write_text(rig.config.read_text() + f"load:\n  target: clickhouse\n  password_env: ${{{variable}}}\n")
    rig.scenario(record_env=["CH_PASSWORD"], leak_env=[])
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", connections=["ch"], **kwargs))
    assert isinstance(payload, dict), payload
    assert rig.calls()[-1]["env"]["CH_PASSWORD"] == "pw"
