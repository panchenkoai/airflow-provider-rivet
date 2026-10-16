"""What `RivetBaseOperator` shows for each way a try ends, pinned whole: the exception and its text, the XCom payload and
the structured record of a stop, a busy export, a crash, a CDC `max_events` stop and a failure; plus the try's private
secrets folder and the operator's own texts. The mutant ids each test was written for are in its docstring."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.connection import Connection
from conftest import NEW_FLAGS, FakeTI, Rig, fixture

from airflow_provider_rivet import operators
from airflow_provider_rivet.invocation import Busy, Outcome
from airflow_provider_rivet.operators import (
    RivetApplyOperator,
    RivetCdcRunOperator,
    RivetRunOperator,
    RivetRunWatcher,
    get_connection,
)
from airflow_provider_rivet.preflight import PreflightRefusal

PG_STATE = {"RIVET_PG_URL": "x", "RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"}


def published(ti: FakeTI) -> dict:
    return ti.store[(ti.task_id, "return_value")]


# --- a stop that arrives before rivet starts (D40), in this process -------------------------------------------------


def test_a_stop_before_the_first_step_starts_no_rivet_and_is_retried(rig: Rig) -> None:
    """_run_with_secrets mutants 51 / 55 / 56 (the stop flag never consulted), 62-84 / 90 (the record and payload)."""
    op = rig.operator(RivetRunOperator, export="orders")
    op.on_kill()
    exc, ti = rig.run(op)
    text = "the task was stopped before rivet run started; rivet run was not run"
    assert type(exc) is AirflowException and str(exc) == text
    assert [c["argv"][0] for c in rig.calls()] == [], "no rivet process after a stop"
    assert published(ti) == {"command": "run", "decision": "retry", "stopped_before": "run", "units": []}
    assert rig.events("rivet.stopped") == [{"event": "rivet.stopped", "step": "run", "text": text}]


def test_a_stop_between_two_steps_starts_no_second_step(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """The stop recorded while `rivet plan` ran keeps `rivet apply` from starting."""
    op = rig.operator(RivetApplyOperator, export="orders")
    real_hold = op._hold

    def hold(proc: object) -> None:
        real_hold(proc)  # the first process held is `rivet plan`; the stop arrives while it runs
        op._stop_requested = True

    monkeypatch.setattr(op, "_hold", hold)
    exc, ti = rig.run(op)
    assert type(exc) is AirflowException and "rivet apply was not run" in str(exc)
    assert [c["argv"][0] for c in rig.calls()] == ["plan"]
    assert published(ti)["stopped_before"] == "apply"


# --- the export's lock held by another try (G2) -----------------------------------------------------------------------


def test_a_busy_export_is_retried_with_its_code_in_the_record_and_the_payload(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """_run_with_secrets mutants 93-112."""
    busy = Busy(Path("/locks/orders.lock"), "export orders", 600)

    def held(*_args: object, **_kwargs: object) -> Outcome:
        raise busy

    monkeypatch.setattr(operators, "invoke", held)
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowException and str(exc) == str(busy)
    assert published(ti) == {"command": "apply", "decision": "retry", "busy": "RIVET_AIRFLOW_EXPORT_BUSY", "units": []}
    assert rig.events("rivet.busy") == [{"event": "rivet.busy", "code": "RIVET_AIRFLOW_EXPORT_BUSY", "text": str(busy)}]


def test_a_refusal_raised_by_the_invocation_is_published_like_a_preflight_one(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*_args: object, **_kwargs: object) -> Outcome:
        raise PreflightRefusal("RIVET_AIRFLOW_QUERY_FILE", "no")

    monkeypatch.setattr(operators, "invoke", refuse)
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert type(exc) is AirflowFailException and str(exc) == "[RIVET_AIRFLOW_QUERY_FILE] no"
    assert published(ti) == {"command": "run", "decision": "fail", "preflight_refusal": "RIVET_AIRFLOW_QUERY_FILE", "units": []}


def test_a_kill_after_the_task_ended_finds_no_process_to_stop(rig: Rig) -> None:
    """_run_with_secrets mutant 123 (`self._proc = ""`): Airflow's SIGTERM handler may call on_kill after `execute`
    returned; it must not raise there."""
    op = rig.operator(RivetRunOperator, export="orders")
    payload, _ = rig.run(op)
    assert payload["decision"] == "success"
    assert op._proc is None
    op.on_kill()
    assert op._stop_requested is True


# --- the try's secrets folder -----------------------------------------------------------------------------------------


def test_the_secrets_folder_is_named_for_the_package_and_removed(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """execute mutants 39-41: the folder's prefix is what an operator (and the D02 / D40 tests) look for."""
    seen: list[Path] = []
    real = operators.build_environment

    def spy(environ: object, spec: object, conns: object, secrets: Path) -> dict:
        seen.append(secrets)
        assert secrets.is_dir()
        return real(environ, spec, conns, secrets)

    monkeypatch.setattr(operators, "build_environment", spy)
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    (secrets,) = seen
    assert secrets.name.startswith("rivet-airflow-secrets-") and not secrets.exists()


def test_a_secrets_folder_already_gone_does_not_replace_the_outcome(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """execute mutants 51 / 53 / 54 (`ignore_errors` dropped): the cleanup in `finally` raised FileNotFoundError over
    the task's own success."""
    real = operators.build_environment

    def remove_it(environ: object, spec: object, conns: object, secrets: Path) -> dict:
        env = real(environ, spec, conns, secrets)
        shutil.rmtree(secrets)
        return env

    monkeypatch.setattr(operators, "build_environment", remove_it)
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


# --- success with a CDC stop at max_events ----------------------------------------------------------------------------


def test_a_max_events_stop_is_one_record_with_its_text(rig: Rig) -> None:
    """_settle mutants 31-34."""
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": [fixture("run_entry.json")]}})
    payload, _ = rig.run(rig.operator(RivetCdcRunOperator, export="orders_cdc"))
    assert payload["decision"] == "success"
    assert rig.events("rivet.cdc.max_events") == [
        {"event": "rivet.cdc.max_events", "exports": ["orders_cdc"], "text": "stopped on max_events; backlog remains"}
    ]


# --- the exception text of a failure ----------------------------------------------------------------------------------


def entry(name: str, status: str) -> dict:
    return {"export_name": name, "status": status, "run_id": f"{name}_1", "rows": 0, "files": 0, "mode": "full"}


def test_a_whole_config_run_with_two_failed_exports_names_neither(rig: Rig) -> None:
    """_settle mutant 43 (`len(failed) == 1` taken always): one failed export is named, two are not (no `export`)."""
    line = {"error": "x", "exit_class": 1}
    rig.scenario(run={"exit": 1, "line": line, "summary": {"per_export": [entry("orders", "failed"), entry("users", "failed")]}})
    exc, _ = rig.run(rig.operator(RivetRunOperator))
    assert type(exc) is AirflowFailException and str(exc) == "generic"
    rig.scenario(run={"exit": 1, "line": line, "summary": {"per_export": [entry("orders", "failed"), entry("users", "success")]}})
    exc, _ = rig.run(rig.operator(RivetRunOperator))
    assert str(exc) == "generic (orders)"


def test_a_failure_whose_summary_lists_no_failed_unit_still_publishes(rig: Rig) -> None:
    """_settle mutant 43: with no failed unit the mutant indexed `failed[0]` and raised IndexError, publishing nothing."""
    rig.scenario(run={"exit": 1, "line": {"error": "x", "exit_class": 1}, "summary": {"per_export": [entry("users", "success")]}})
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert type(exc) is AirflowFailException and str(exc) == "generic (orders)"
    assert published(ti)["decision"] == "fail"


# --- a crash ----------------------------------------------------------------------------------------------------------


def test_a_crash_is_one_record_naming_its_budget_and_the_tries(rig: Rig) -> None:
    """_settle mutants 98-103 (the record's keys) and 109 (the exception text)."""
    rig.scenario(run={"signal": "SIGKILL"})
    exc, _ = rig.run(rig.operator(RivetRunOperator, export="orders"), FakeTI(try_number=1, max_tries=2))
    assert type(exc) is AirflowException and str(exc) == "crashed (orders)"
    assert rig.events("rivet.crashed") == [
        {
            "event": "rivet.crashed",
            "retry": True,
            "budget": 1,
            "try_number": 1,
            "airflow_tries_left": True,
            "ledger_problem": None,
        }
    ]


@pytest.mark.parametrize(("try_number", "left"), [(2, True), (3, False)])
def test_a_crash_says_whether_airflow_has_a_try_left(rig: Rig, try_number: int, left: bool) -> None:
    """_settle mutant 104 (`<=` -> `<`): try 2 of max_tries 2 still has Airflow's last retry."""
    rig.scenario(run={"signal": "SIGKILL"})
    rig.run(rig.operator(RivetRunOperator, export="orders", crashed_retries=5), FakeTI(try_number=try_number, max_tries=2))
    assert rig.events("rivet.crashed")[-1]["airflow_tries_left"] is left


def test_a_crash_on_a_pod_writes_no_ledger_into_a_declared_state_directory(rig: Rig) -> None:
    """_settle mutant 51 (`and` -> `or`): a pod keeps no marker, so the ledger is not tracked there even when the task
    names a `state_dir`: the pod's disk goes with it."""
    rig.scenario(run={"signal": "SIGKILL"})
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders", deployment="pod", env=PG_STATE))
    assert type(exc) is AirflowException
    assert "crashed_ledger" in published(ti)["degraded"]
    assert not (rig.state_dir / "airflow").exists()


# --- the operator's own texts -----------------------------------------------------------------------------------------


def test_the_watcher_fails_the_run_with_its_sentence() -> None:
    """RivetRunWatcher.execute mutants 1-4."""
    with pytest.raises(AirflowFailException) as raised:
        RivetRunWatcher(task_id="w").execute({})
    assert str(raised.value) == "a task of this DAG run failed; see the failed and upstream_failed tasks"


def test_a_missing_connection_is_named_with_the_remedy(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_connection mutants 8 / 9."""
    from airflow.exceptions import AirflowNotFoundException

    def missing(conn_id: str) -> Connection:
        raise AirflowNotFoundException(conn_id)

    monkeypatch.setattr(operators.BaseHook, "get_connection", missing)
    with pytest.raises(PreflightRefusal) as raised:
        get_connection("warehouse")
    assert str(raised.value) == (
        "[RIVET_AIRFLOW_CONNECTION_MISSING] connection `warehouse` is not defined in the metadata database or any secrets "
        "backend this worker reads; create it, or correct the id the task names"
    )


@pytest.mark.parametrize("cls", [RivetApplyOperator, RivetCdcRunOperator])
def test_an_operator_that_needs_an_export_says_which(cls: type) -> None:
    """__init__ mutant 12."""
    with pytest.raises(ValueError) as raised:
        cls(task_id="t", config="c.yaml")
    assert str(raised.value) == f"{cls.__name__} needs `export`"
