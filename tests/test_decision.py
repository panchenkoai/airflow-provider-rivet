"""The retry decision (`invocation.decide`): every class of the contract, retryable or final, and what the operator raises
for it with Airflow tries left and on the last try. Also a load's failed table row that carries no error of its own."""

from __future__ import annotations

import pytest
from airflow.exceptions import AirflowException, AirflowFailException
from conftest import NEW_FLAGS, FakeTI, Rig

from airflow_provider_rivet.invocation import Outcome, decide
from airflow_provider_rivet.operators import RivetLoadOperator, RivetRunOperator
from airflow_provider_rivet.result import CLASS_BY_EXIT, ErrorObject

EXIT_BY_CLASS = {name: code for code, name in CLASS_BY_EXIT.items()}


def error(class_: str, retryable: bool) -> ErrorObject:
    return ErrorObject("RIVET_X", None, class_, EXIT_BY_CLASS.get(class_), retryable, None)


@pytest.mark.parametrize("class_", sorted(CLASS_BY_EXIT.values()))
@pytest.mark.parametrize("retryable", [True, False], ids=["retryable", "final"])
def test_a_printed_class_retries_exactly_when_rivet_says_retryable(class_: str, retryable: bool) -> None:
    """decide mutants: the class never overrides `retryable`, except `crashed` (its own budget)."""
    assert decide(error(class_, retryable)) == ("retry" if retryable else "fail")
    assert Outcome("run", error=error(class_, retryable)).decision == decide(error(class_, retryable))


@pytest.mark.parametrize("retryable", [True, False])
def test_a_crash_is_retry_crashed_whatever_its_retryable_flag(retryable: bool) -> None:
    assert decide(ErrorObject(None, None, "crashed", None, retryable, None)) == "retry_crashed"


def test_no_error_is_success() -> None:
    assert decide(None) == "success"
    assert Outcome("run").decision == "success" and Outcome("run").to_xcom()["decision"] == "success"


@pytest.mark.parametrize("class_", sorted(CLASS_BY_EXIT.values()))
@pytest.mark.parametrize("retryable", [True, False], ids=["retryable", "final"])
@pytest.mark.parametrize("try_number", [1, 3], ids=["tries_left", "last_try"])
def test_the_operator_raises_by_the_decision_on_every_try(rig: Rig, class_: str, retryable: bool, try_number: int) -> None:
    """A retryable failure raises AirflowException (Airflow decides whether a try is left), a final one
    AirflowFailException, on the first try and on the last alike; the XCom decision says the same."""
    code = EXIT_BY_CLASS[class_]
    line = {"class": class_, "retryable": retryable, "exit_code": code, "exit_class": code, "code": "RIVET_X", "error": "boom"}
    rig.scenario(run={"exit": code, "line": line})
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders"), FakeTI(try_number=try_number, max_tries=2))
    assert type(exc) is (AirflowException if retryable else AirflowFailException), exc
    assert ti.store[("task", "return_value")]["decision"] == ("retry" if retryable else "fail")
    assert str(exc) == f"[RIVET_X] {class_} (orders)"


@pytest.mark.parametrize(
    ("try_number", "budget", "raised"),
    [(1, 1, AirflowException), (2, 1, AirflowFailException), (2, 2, AirflowException)],
    ids=["first_crash", "budget_spent", "budget_left"],
)
def test_a_crash_retries_under_its_own_budget(rig: Rig, try_number: int, budget: int, raised: type) -> None:
    rig.scenario(run={"signal": "SIGKILL"})
    op = rig.operator(RivetRunOperator, export="orders", crashed_retries=budget)
    for earlier in range(1, try_number):
        rig.run(op, FakeTI(try_number=earlier, max_tries=3))
    exc, ti = rig.run(op, FakeTI(try_number=try_number, max_tries=3))
    assert type(exc) is raised
    assert ti.store[("task", "return_value")]["decision"] == "retry_crashed"


# --- a load's failed row with `error: null` ---------------------------------------------------------------------------

FAILED_ROW = {"run_id": "l1", "per_table": [{"export": "orders", "table": "users", "status": "failed", "error": None}]}


def test_a_failed_table_row_without_its_error_fails_the_task_on_exit_zero(rig: Rig) -> None:
    """invocation._load mutants 139 / 140 / 150: the row's error is the process-level one, built from the exit when rivet
    printed none; dropping it left the failed unit with no error, and the task ended success."""
    rig.scenario(flags=NEW_FLAGS, load={"summary": FAILED_ROW})
    exc, ti = rig.run(rig.operator(RivetLoadOperator, export="orders", table="users"))
    payload = ti.store[("task", "return_value")]
    assert type(exc) is AirflowFailException, exc
    assert payload["decision"] == "fail"
    (unit,) = payload["units"]
    assert unit["status"] == "failed" and unit["error"]["class"] == "generic"
    assert payload["degraded"] == ["exit_zero_failed_unit", "state_url_sqlite"], "one failed unit: no per_unit_error"


def test_a_failed_table_row_without_its_error_takes_the_printed_process_error(rig: Rig) -> None:
    """mutant 140 (`error and error_from_exit(...)`): with a printed retryable line the row's error IS that line, so the
    task is retried; the mutant built a final `generic` from the exit instead."""
    line = {"class": "retryable", "retryable": True, "exit_code": 2, "exit_class": 2, "code": "RIVET_NET", "error": "reset"}
    rig.scenario(flags=NEW_FLAGS, load={"exit": 2, "line": line, "summary": FAILED_ROW})
    exc, ti = rig.run(rig.operator(RivetLoadOperator, export="orders", table="users"))
    payload = ti.store[("task", "return_value")]
    assert type(exc) is AirflowException, exc
    assert payload["decision"] == "retry"
    assert payload["units"][0]["error"]["code"] == "RIVET_NET" and payload["units"][0]["error"]["class"] == "retryable"
