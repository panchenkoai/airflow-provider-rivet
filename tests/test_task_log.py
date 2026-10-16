"""The task log an operator reads: the exact lines each outcome writes at INFO and above, and no red word on success.

The owner's view of a successful apply on Airflow 2.10.5 (2026-10-16): every line a raw JSON dump, the `rivet.start`
lines red because argv holds `--json-errors` (Airflow colours a line holding a word of `color_log_error_keywords`), and
nothing saying in words what happened. These tests pin the readable block instead: one headline with the outcome first,
one line per rivet step, indented notes; the JSON records only at DEBUG.
"""

from __future__ import annotations

import itertools
import json
import re
from pathlib import Path
from typing import Any

import pytest
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from conftest import NEW_FLAGS, SECRET, FakeTI, Rig, fixture

from airflow_provider_rivet import invocation
from airflow_provider_rivet.capabilities import DEGRADATIONS
from airflow_provider_rivet.operators import (
    RivetApplyOperator,
    RivetBaseOperator,
    RivetCdcRunOperator,
    RivetCompactOperator,
    RivetLoadOperator,
    RivetPlanOperator,
    RivetRunOperator,
)

GUIDE = "https://github.com/panchenkoai/airflow-provider-rivet#local-worker-setup-guide"
SQLITE = f"  note     SQLite state is single-host — see {GUIDE}"
OLD_LACKS = "  note     this binary lacks: apply_summary, lock_wait, no_notify (the operator works around them)"


class Recorder:
    """Stands in for the operator's logger: keeps (level name, text) of every line."""

    def __init__(self) -> None:
        self.lines: list[tuple[str, str]] = []

    def _at(self, level: str) -> Any:
        def write(msg: str, *args: Any, **_: Any) -> None:
            self.lines.append((level, msg % args if args else msg))

        return write

    def __getattr__(self, name: str) -> Any:
        if name in ("debug", "info", "warning", "error", "exception", "critical"):
            return self._at(name.upper().replace("EXCEPTION", "ERROR"))
        raise AttributeError(name)

    def shown(self) -> list[str]:
        """The lines a default Airflow task log shows (INFO and above), with their level."""
        return [f"{level} {text}" for level, text in self.lines if level != "DEBUG"]


@pytest.fixture
def log(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    """Every operator's log goes to one recorder; each clock reading of the task log is one second later."""
    recorder = Recorder()
    monkeypatch.setattr(RivetBaseOperator, "log", property(lambda self: recorder))
    try:
        from airflow_provider_rivet import tasklog
    except ImportError:  # the base has no readable block: the exact-text assertion is what fails there
        return recorder
    ticks = itertools.count()
    monkeypatch.setattr(tasklog, "clock", lambda: float(next(ticks)))
    return recorder


def red_keywords() -> list[str]:
    """Airflow's own default `[logging] color_log_error_keywords` (2.10 and 3.x ship `error,exception`)."""
    from airflow.configuration import conf

    default = conf.get_default_value("logging", "color_log_error_keywords")
    words = {w.strip().lower() for w in str(default).split(",") if w.strip()}
    assert {"error", "exception"} <= words, words
    return sorted(words)


def summary(name: str, rows: int, files: int, run_id: str, status: str = "success", **more: Any) -> dict:
    """A `--summary-output` document with one `per_export` entry."""
    entry = {"export_name": name, "status": status, "run_id": run_id, "rows": rows, "files": files, "mode": "full"}
    return {"per_export": [{**entry, "error": None, "stop_reason": None, "tables": None, **more}]}


# --- golden: success ------------------------------------------------------------------------------------------------


def test_golden_apply_success_with_counts(rig: Rig, log: Recorder) -> None:
    """Success with counts on a binary with the contract's flags: the headline says OK, the counts, the time, the run."""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": summary("orders", 30, 1, "orders_20261016T092509")})
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert payload["decision"] == "success"
    assert log.shown() == [
        "INFO rivet apply orders → OK: 30 rows, 1 file in 5.0 s (run orders_20261016T092509)",
        "INFO   plan     sealed, strategy full in 1.0 s",
        "INFO   apply    done in 1.0 s",
        f"INFO {SQLITE}",
    ]


def test_golden_apply_success_on_a_degraded_binary(rig: Rig, log: Recorder) -> None:
    """The owner's case: rivet 0.31.0 (no apply summary, no --lock-wait, no --no-notify) and SQLite state. The missing
    flags are one note, not a `degraded: [...]` dump that reads like a fault; counts come from `rivet metrics`."""
    rig.scenario(apply={"summary": summary("users", 30, 1, "ignored")})  # the counts the fake's metrics row records
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="users"))
    assert payload["degraded"] == ["apply_summary", "lock_wait", "no_notify", "state_url_sqlite"]
    run_id = payload["units"][0]["run_id"]
    assert log.shown() == [
        f"INFO rivet apply users → OK: 30 rows, 1 file in 7.0 s (run {run_id})",
        "INFO   plan     sealed, strategy incremental in 1.0 s",
        "INFO   apply    done in 1.0 s",
        "INFO   metrics  read the run's counts from rivet's state in 1.0 s",
        f"INFO {OLD_LACKS}",
        f"INFO {SQLITE}",
    ]


def test_golden_run_of_the_whole_config_lists_each_export(rig: Rig, log: Recorder) -> None:
    """Several units: the headline sums them, one line per unit follows the step."""
    two = {"per_export": summary("orders", 500, 2, "orders_1")["per_export"] + summary("users", 7, 1, "users_1")["per_export"]}
    rig.scenario(flags=NEW_FLAGS, run={"summary": two})
    payload, _ = rig.run(rig.operator(RivetRunOperator))
    assert payload["decision"] == "success"
    assert log.shown() == [
        "INFO rivet run → OK: 2 exports, 507 rows, 3 files in 3.0 s",
        "INFO   run      done in 1.0 s",
        "INFO   export   orders: success, 500 rows, 2 files (run orders_1)",
        "INFO   export   users: success, 7 rows, 1 file (run users_1)",
        f"INFO {SQLITE}",
    ]


def test_golden_cdc_max_events_is_a_note(rig: Rig, log: Recorder) -> None:
    rig.scenario(
        flags=NEW_FLAGS, run={"summary": summary("orders_cdc", 9, 1, "orders_cdc_1", mode="cdc", stop_reason="max_events")}
    )
    payload, _ = rig.run(rig.operator(RivetCdcRunOperator, export="orders_cdc"))
    assert payload["decision"] == "success"
    assert log.shown() == [
        "INFO rivet run orders_cdc → OK: 9 rows, 1 file in 3.0 s (run orders_cdc_1)",
        "INFO   run      done in 1.0 s",
        f"INFO {SQLITE}",
        "INFO   note     stopped at max_events (orders_cdc): a backlog remains; the next run goes on from here",
    ]


# --- golden: skipped ------------------------------------------------------------------------------------------------


def test_golden_skipped_nothing_to_do(rig: Rig, log: Recorder) -> None:
    """A `skip_empty` apply that delivered nothing: SKIPPED with rivet's reason, not OK with zero rows."""
    rig.scenario(apply={"metric": {"status": "skipped", "skip_reason": "empty", "total_rows": 0}})
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="users"))
    assert payload["units"][0]["status"] == "skipped"
    run_id = payload["units"][0]["run_id"]
    assert log.shown()[0] == f"INFO rivet apply users → SKIPPED: nothing to do (empty) in 7.0 s (run {run_id})"


def test_golden_compact_skipped_is_an_airflow_skip_with_the_same_headline(rig: Rig, log: Recorder) -> None:
    rig.scenario(
        flags=NEW_FLAGS,
        compact={
            "summary": {
                "run_id": "c1",
                "per_table": [{"export": "orders", "table": "t", "status": "skipped", "skip_reason": "nothing to compact"}],
            }
        },
    )
    exc, _ = rig.run(rig.operator(RivetCompactOperator, export="orders", table="t"))
    assert isinstance(exc, AirflowSkipException)
    assert log.shown() == [
        "INFO rivet compact orders table t → SKIPPED: nothing to do (nothing to compact) in 3.0 s (run c1)",
        "INFO   compact  done in 1.0 s",
        f"INFO {SQLITE}",
    ]


def test_golden_skipped_already_applied_by_an_earlier_try(rig: Rig, log: Recorder) -> None:
    """G1b: the retry finds the run its earlier try's apply made and applies nothing; the headline says so."""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": summary("orders", 30, 1, "ignored")})
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op, FakeTI(try_number=1, max_tries=2))
    log.lines.clear()
    retry, _ = rig.run(op, FakeTI(try_number=2, max_tries=2))
    run_id = retry["units"][0]["run_id"]
    assert log.shown() == [
        f"INFO rivet apply orders → SKIPPED: an earlier try already applied this plan: 30 rows, 1 file; not applied again (run {run_id})",
        "INFO   metrics  found the earlier try's run in rivet's state in 1.0 s",
        f"INFO {SQLITE}",
    ]


# --- golden: failures -----------------------------------------------------------------------------------------------


def test_golden_refused_final_with_its_action(rig: Rig, log: Recorder) -> None:
    """A refusal: one ERROR headline, REFUSED, its code, `final`, the registry action; the stderr path, never its text."""
    line = fixture("json_errors.json")
    rig.scenario(flags=NEW_FLAGS, apply={"exit": 5, "line": line})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert isinstance(exc, AirflowFailException)
    payload = ti.store[("task", "return_value")]
    error = payload["error"]
    assert error["class"] == "refusal" and error["action"]
    assert log.shown() == [
        f"ERROR rivet apply orders → REFUSED [{error['code']}], final, action: {error['action']}",
        "INFO   plan     sealed, strategy full in 1.0 s",
        "INFO   apply    exit 5 in 1.0 s",
        f"INFO   export   orders: failed [{error['code']}] refusal",
        f"INFO   stderr   {payload['stderr_path']}",
        f"INFO {SQLITE}",
    ]


def test_golden_stale_plan_refused_on_todays_binary(rig: Rig, log: Recorder) -> None:
    """The owner's failure example: `RIVET_PLAN_ARTIFACT_STALE` from a binary that prints no `class` (exit 5)."""
    op = rig.operator(RivetApplyOperator, export="users")
    rig.scenario(cursors={"users": "5"}, apply={"exit": 2, "line": {"error": "network", "exit_class": 2}})
    rig.run(op)
    rig.scenario()
    (rig.bin / "cursors.json").write_text(json.dumps({"users": "6"}))  # another run moved the cursor past the plan
    log.lines.clear()
    exc, ti = rig.run(op, FakeTI(try_number=2))
    assert isinstance(exc, AirflowFailException)
    payload = ti.store[("task", "return_value")]
    assert log.shown() == [
        "ERROR rivet apply users → REFUSED [RIVET_PLAN_ARTIFACT_STALE], final",
        "INFO   metrics  no earlier try's apply of this plan in rivet's state in 1.0 s",
        "INFO   plan     replayed the sealed plan of an earlier try",
        "INFO   apply    exit 5 in 1.0 s",
        "INFO   export   users: failed [RIVET_PLAN_ARTIFACT_STALE] refusal",
        f"INFO   stderr   {payload['stderr_path']}",
        f"INFO {OLD_LACKS}",
        f"INFO {SQLITE}",
        "INFO   note     rivet's failure line has no class or action: the class is read from its exit class",
    ]


def test_golden_transient_failure_says_it_is_retried(rig: Rig, log: Recorder) -> None:
    """A retryable failure reads differently from a final one: WARNING, `retryable`, and that Airflow retries it."""
    line = {"error": "connection reset", "exit_class": 2, "code": "RIVET_SOURCE_CONNECT", "class": "retryable", "retryable": True}
    rig.scenario(flags=NEW_FLAGS, run={"exit": 2, "line": line})
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders"), FakeTI(try_number=1, max_tries=2))
    assert type(exc) is AirflowException
    payload = ti.store[("task", "return_value")]
    assert log.shown() == [
        "WARNING rivet run orders → FAILED [RIVET_SOURCE_CONNECT], retryable, Airflow retries it (try 1 of 3)",
        "INFO   run      exit 2 in 1.0 s",
        "INFO   export   orders: failed [RIVET_SOURCE_CONNECT] retryable",
        f"INFO   stderr   {payload['stderr_path']}",
        f"INFO {SQLITE}",
    ]
    log.lines.clear()
    rig.run(rig.operator(RivetRunOperator, export="orders"), FakeTI(try_number=3, max_tries=2))
    assert log.shown()[0] == "WARNING rivet run orders → FAILED [RIVET_SOURCE_CONNECT], retryable, no try left (try 3 of 3)"


def test_golden_crashed_by_a_signal(rig: Rig, log: Recorder) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"signal": "SIGKILL"})
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders", crashed_retries=1), FakeTI(try_number=1))
    assert type(exc) is AirflowException
    assert log.shown()[:2] == [
        "WARNING rivet run orders → CRASHED: rivet ended by signal 9; Airflow retries it (try 1 of 3) under the crash budget",
        "INFO   run      ended by signal 9 in 1.0 s",
    ]
    log.lines.clear()
    rig.run(rig.operator(RivetRunOperator, export="orders", crashed_retries=1), FakeTI(try_number=2))
    assert log.shown()[0] == "ERROR rivet run orders → CRASHED: rivet ended by signal 9, final: the crash budget is used up"


def test_golden_stopped_before_rivet_started(rig: Rig, log: Recorder) -> None:
    """Airflow stopped the task between two steps: STOPPED, and which step was not run."""
    op = rig.operator(RivetApplyOperator, export="orders")
    op.on_kill()
    exc, _ = rig.run(op)
    assert type(exc) is AirflowException
    assert log.shown() == [
        "WARNING rivet apply orders → STOPPED by Airflow before rivet plan started; rivet plan was not run",
        f"INFO {SQLITE}",
    ]


def test_golden_stopped_while_rivet_ran(rig: Rig, log: Recorder, monkeypatch: pytest.MonkeyPatch) -> None:
    """Airflow killed the task while rivet ran: the kill reads STOPPED by Airflow, not as a crash of rivet."""
    rig.scenario(flags=NEW_FLAGS, run={"signal": "SIGTERM"})
    op = rig.operator(RivetRunOperator, export="orders")
    real = invocation.Running.wait

    def stopped_then_wait(self: Any) -> int:
        op._stop_requested = True
        return real(self)

    monkeypatch.setattr(invocation.Running, "wait", stopped_then_wait)
    exc, _ = rig.run(op)
    assert isinstance(exc, AirflowException)
    assert log.shown()[:2] == [
        "WARNING rivet run orders → STOPPED by Airflow: rivet ended by signal 15",
        "INFO   run      ended by signal 15 in 1.0 s",
    ]


def test_golden_busy_lock(rig: Rig, log: Recorder, monkeypatch: pytest.MonkeyPatch) -> None:
    import fcntl
    import os

    monkeypatch.setattr(invocation, "LOCK_POLL", 0.01)
    op = rig.operator(RivetRunOperator, export="orders", lock_wait=0)
    path = rig.state_dir / "airflow" / "locks" / "run"
    path.mkdir(parents=True, exist_ok=True)
    fd = os.open(path / "orders.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        exc, _ = rig.run(op, FakeTI(try_number=1, max_tries=2))
    finally:
        os.close(fd)
    assert type(exc) is AirflowException, exc
    assert log.shown() == [
        "INFO waiting up to 0 s for the lock of export `orders`: another try of this task, or a rivet process it left running, holds it",
        "WARNING rivet run orders → BUSY [RIVET_AIRFLOW_EXPORT_BUSY]: the lock of export `orders` stayed held for 0 s; "
        "rivet was not started, Airflow retries it (try 1 of 3)",
        f"INFO {SQLITE}",
    ]


def test_golden_refused_before_rivet_started(rig: Rig, log: Recorder) -> None:
    op = rig.operator(RivetRunOperator, export="orders", connections=["no_such_conn"])
    exc, _ = rig.run(op)
    assert isinstance(exc, AirflowFailException)
    assert log.shown() == [
        "ERROR rivet run orders → REFUSED [RIVET_AIRFLOW_CONNECTION_MISSING] before rivet started, final",
        f"ERROR   why      {exc}",
    ]


# --- what moved to DEBUG, and what no success line may hold ---------------------------------------------------------


def test_the_structured_records_are_kept_at_debug(rig: Rig, log: Recorder) -> None:
    """argv, every unit and the result record are still written, as JSON at DEBUG, and none of them at INFO."""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": summary("orders", 30, 1, "orders_1")})
    rig.run(rig.operator(RivetApplyOperator, export="orders"))
    debug = [json.loads(text) for level, text in log.lines if level == "DEBUG"]
    events = [d["event"] for d in debug]
    assert {"rivet.start", "rivet.step", "rivet.unit", "rivet.result", "rivet.warning"} <= set(events)
    assert debug[events.index("rivet.start")]["argv"][0].endswith("rivet")
    assert debug == rig.records
    assert not any(text.startswith("{") for level, text in log.lines if level != "DEBUG")


PG_STATE = {"RIVET_PG_URL": SECRET, "RIVET_STATE_URL": "postgresql://s@h/db"}
CAMPAIGN = {"waves": [{"wave": 1, "exports": ["orders", "users"]}], "ordered_exports": []}
PLAN_LIST = [{"export_name": n, "prioritization": {"campaign": CAMPAIGN}} for n in ("orders", "users")]
SUCCESS_PATHS = {
    "apply_new": (RivetApplyOperator, {"export": "orders"}, {"flags": NEW_FLAGS}),
    "apply_old": (RivetApplyOperator, {"export": "orders"}, {}),
    "run_export": (RivetRunOperator, {"export": "orders"}, {}),
    "run_all": (RivetRunOperator, {}, {"flags": NEW_FLAGS}),
    "cdc_unknown_stop": (
        RivetCdcRunOperator,
        {"export": "orders_cdc"},
        {"run": {"summary": summary("orders_cdc", 1, 1, "c_1", mode="cdc")}},
    ),
    "cdc_max_events": (
        RivetCdcRunOperator,
        {"export": "orders_cdc"},
        {"flags": NEW_FLAGS, "run": {"summary": summary("orders_cdc", 1, 1, "c_1", mode="cdc", stop_reason="max_events")}},
    ),
    "load_old": (RivetLoadOperator, {"export": "orders", "table": "t"}, {}),
    "load_new": (
        RivetLoadOperator,
        {"export": "orders", "table": "t"},
        {
            "flags": NEW_FLAGS,
            "load": {
                "summary": {"run_id": "l1", "per_table": [{"export": "orders", "table": "t", "status": "loaded", "rows": 3}]}
            },
        },
    ),
    "compact": (RivetCompactOperator, {"export": "orders"}, {"flags": NEW_FLAGS}),
    "compact_skipped": (
        RivetCompactOperator,
        {"export": "orders", "table": "t"},
        {
            "flags": NEW_FLAGS,
            "compact": {
                "summary": {
                    "run_id": "c",
                    "per_table": [{"export": "orders", "table": "t", "status": "skipped", "skip_reason": "nothing"}],
                }
            },
        },
    ),
    "notifications_block": (RivetRunOperator, {"export": "orders"}, {}),
    "plan": (RivetPlanOperator, {}, {"plan_list": PLAN_LIST}),
    "run_on_postgres_state": (
        RivetRunOperator,
        {"export": "orders", "state_dir": None, "env": PG_STATE},
        {},
    ),
}


@pytest.mark.parametrize("path", sorted(SUCCESS_PATHS))
def test_no_line_of_a_successful_task_is_coloured_red(rig: Rig, log: Recorder, path: str, tmp_path: Path) -> None:
    """No INFO / WARNING line of a successful task holds a word of Airflow's default `color_log_error_keywords`, matched
    as Airflow's UI matches it (case-insensitive substring); on the base, `rivet.start` held `--json-errors`."""
    cls, kwargs, scenario = SUCCESS_PATHS[path]
    if path == "notifications_block":
        rig.config.write_text(rig.config.read_text() + "notifications:\n  slack:\n    webhook_url_env: HOOK\n")
    if cls is RivetPlanOperator:
        kwargs = {**kwargs, "plan_file": str(tmp_path / "plan.json")}
    rig.scenario(**scenario)
    result, _ = rig.run(rig.operator(cls, **kwargs))
    assert isinstance(result, dict) or isinstance(result, AirflowSkipException), result
    shown = log.shown()
    assert shown, "the task wrote readable lines"
    words = red_keywords()
    red = [line for line in shown if any(word in line.lower() for word in words)]
    assert not red, red


def test_no_line_of_a_retry_that_finds_its_apply_done_is_coloured_red(rig: Rig, log: Recorder) -> None:
    rig.scenario(flags=NEW_FLAGS, apply={"summary": summary("orders", 30, 1, "x")})
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op, FakeTI(try_number=1))
    rig.run(op, FakeTI(try_number=2))
    words = red_keywords()
    assert not [line for line in log.shown() if any(word in line.lower() for word in words)]


def test_a_failure_headline_is_coloured_red(rig: Rig, log: Recorder) -> None:
    """The other side of the oracle: a final failure's headline is an ERROR line (Airflow colours it whatever it says)."""
    rig.scenario(flags=NEW_FLAGS, run={"exit": 3, "line": {"error": "x", "exit_class": 3}})
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert log.shown()[0].startswith("ERROR rivet run orders → FAILED data_integrity, final")


def test_every_degradation_reads_as_words_without_a_red_keyword() -> None:
    """Each degradation key has its note (or is a missing flag listed by key); no note holds a red keyword."""
    from airflow_provider_rivet import tasklog

    assert set(tasklog.NOTES) | set(tasklog.LACKS) == set(DEGRADATIONS)
    words = red_keywords()
    texts = [t for t in tasklog.NOTES.values() if t] + list(tasklog.LACKS)
    assert not [t for t in texts if any(word in t.lower() for word in words)]


def test_the_readable_lines_hold_no_secret_and_no_rivet_text(rig: Rig, log: Recorder) -> None:
    """rivet's stderr (which leaks the URL here) and its error message stay out of every line, DEBUG included."""
    line = {"error": "row 4111-1111 refused", "exit_class": 5, "code": "RIVET_X"}
    rig.scenario(run={"exit": 5, "line": line})
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    blob = "\n".join(text for _, text in log.lines)
    assert SECRET not in blob and "4111-1111" not in blob and not re.search(r"connecting with", blob)
