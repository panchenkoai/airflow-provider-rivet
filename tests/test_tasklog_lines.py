"""The readable task log line by line, from `TaskLog` directly: every word, count, unit word and level a reader sees.

The golden tests in test_task_log.py drive whole tasks; these pin the branches no whole task reached (the mutation run
of 2026-10-17 left them alive: tasklog.py ids in the comments).
"""

from __future__ import annotations

import itertools
import logging
from pathlib import Path
from typing import Any

import pytest

from airflow_provider_rivet import tasklog
from airflow_provider_rivet.preflight import SETUP_GUIDE
from airflow_provider_rivet.tasklog import TaskLog

INFO, WARNING, ERROR, DEBUG = logging.INFO, logging.WARNING, logging.ERROR, logging.DEBUG


@pytest.fixture(autouse=True)
def ticks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each clock reading is one second after the last, from 0."""
    counter = itertools.count()
    monkeypatch.setattr(tasklog, "clock", lambda: float(next(counter)))


def make(command: str = "apply", export: str | None = "orders", table: str | None = None) -> tuple[TaskLog, list]:
    written: list[tuple[int, str]] = []
    return TaskLog(command, export, table, lambda level, text: written.append((level, text))), written


def render(log: TaskLog, payload: dict[str, Any], try_number: int = 1, max_tries: int = 0) -> list[tuple[int, str]]:
    return log.render(payload, try_number, max_tries, False)


def test_a_record_is_written_at_debug_as_sorted_json_whatever_its_values() -> None:
    """record 6/7/9/10/11: the keys sorted, a value JSON cannot hold (a path) written as its text."""
    log, written = make()
    log.record({"event": "x", "b": Path("/p"), "a": 1})
    assert written == [(DEBUG, '{"a": 1, "b": "/p", "event": "x"}')]


def test_a_streamed_stderr_line_is_shown_at_info_with_its_step() -> None:
    """record 67-78."""
    log, written = make()
    log.record({"event": "rivet.stderr", "step": "apply", "line": "hello"})
    assert written[-1] == (INFO, "rivet apply stderr: hello")


def test_the_duration_is_from_the_task_start_to_the_end() -> None:
    """render 2: started at second 0, rendered at second 3 (two readings by the steps between)."""
    log, _ = make("run")
    log.record({"event": "rivet.start", "step": "run"})
    log.record({"event": "rivet.step", "exit_status": 0})
    lines = render(log, {"decision": "success", "units": []})
    assert lines[0] == (INFO, "rivet run orders → OK in 3.0 s")
    assert lines[1] == (INFO, "  run      done in 1.0 s")


def test_a_retried_failure_on_the_last_try_before_the_final_one_says_airflow_retries_it() -> None:
    """render 19: try 2 of max_tries 2 still has a retry (`try_number <= max_tries`)."""
    log, _ = make()
    error = {"class": "retryable", "code": "RIVET_NET"}
    lines = render(log, {"decision": "retry", "error": error, "units": []}, try_number=2, max_tries=2)
    assert lines[0] == (WARNING, "rivet apply orders → FAILED [RIVET_NET], retryable, Airflow retries it (try 2 of 3)")
    lines = render(log, {"decision": "retry", "error": error, "units": []}, try_number=3, max_tries=2)
    assert lines[0] == (ERROR, "rivet apply orders → FAILED [RIVET_NET], retryable, no try left (try 3 of 3)")


@pytest.mark.parametrize(
    ("payload", "headline"),
    [
        (
            {"decision": "retry_crashed", "crashed_retry": True, "signal": 9, "units": []},
            "rivet apply orders → CRASHED: rivet ended by signal 9; no try left (try 3 of 3) under the crash budget",
        ),
        ({"busy": "RIVET_AIRFLOW_EXPORT_BUSY", "units": []}, "rivet apply orders → BUSY [RIVET_AIRFLOW_EXPORT_BUSY]"),
    ],
    ids=["crashed", "busy"],
)
def test_a_failure_with_no_try_left_is_an_error_whatever_rivet_called_it(payload: dict, headline: str) -> None:
    """Owner, 2026-10-17: a failure Airflow will not try again ends the task, so its headline is ERROR."""
    log, _ = make()
    last, before = render(log, payload, try_number=3, max_tries=2)[0], render(log, payload, try_number=2, max_tries=2)[0]
    assert last[0] == ERROR and last[1].startswith(headline) and "no try left (try 3 of 3)" in last[1]
    assert before[0] == WARNING and "Airflow retries it (try 2 of 3)" in before[1]


def test_a_crash_without_a_signal_names_its_exit_status() -> None:
    """_failure 13/20/21/22."""
    log, _ = make()
    payload = {"decision": "retry_crashed", "signal": None, "exit_status": 101, "crashed_retry": True, "units": []}
    assert render(log, payload, 1, 1)[0] == (
        WARNING,
        "rivet apply orders → CRASHED: rivet ended by exit 101; Airflow retries it (try 1 of 2) under the crash budget",
    )


def test_a_failure_headline_names_the_run_of_its_one_failed_unit() -> None:
    """_failure 72-77/82/86: the failed unit's run, not the run of a unit that succeeded beside it."""
    log, _ = make("load", "orders")
    units = [
        {"export": "orders", "table": "a", "status": "loaded", "run_id": "r2"},
        {"export": "orders", "table": "b", "status": "failed", "run_id": "r1", "error": {"class": "refusal", "code": "C"}},
    ]
    payload = {"decision": "fail", "error": {"class": "refusal", "code": "C"}, "units": units}
    lines = render(log, payload)
    assert lines[0] == (ERROR, "rivet load orders → REFUSED [C], final (run r1)")
    assert lines[1:3] == [
        (INFO, "  table    orders.a: loaded (run r2)"),
        (INFO, "  table    orders.b: failed [C] refusal (run r1)"),
    ]


def test_unit_lines_show_for_several_units_or_one_failed_unit_of_a_failed_task_only() -> None:
    """render 108-114: one unit that did not fail in a failed task, or a failed unit in a successful one, gets no line."""
    failed = {"export": "orders", "status": "failed", "error": {"class": "generic"}}
    ok = {"export": "orders", "status": "success"}
    not_failed = render(make()[0], {"decision": "fail", "error": {"class": "refusal", "code": "C"}, "units": [ok]})
    assert not_failed == [(ERROR, "rivet apply orders → REFUSED [C], final")]
    success = render(make()[0], {"decision": "success", "units": [failed]})
    assert success == [(INFO, "rivet apply orders → OK in 1.0 s")]
    one_failed = render(make()[0], {"decision": "fail", "error": {"class": "generic"}, "units": [failed]})
    assert one_failed[1:] == [(INFO, "  export   orders: failed generic")]


def test_a_unit_without_an_export_or_a_table_is_the_config() -> None:
    """_unit_name 11/12."""
    log, _ = make("run", None)
    unit = {"status": "failed", "error": {"class": "generic"}}
    assert render(log, {"decision": "fail", "error": {"class": "generic"}, "units": [unit]})[1] == (
        INFO,
        "  export   (config): failed generic",
    )


def test_a_success_of_several_tables_counts_tables_and_names_each_as_export_dot_table() -> None:
    """_success 25, _unit_lines 41, _unit_name 3/8/9/10."""
    log, _ = make("load", "orders")
    units = [{"export": "orders", "table": "a", "status": "loaded"}, {"export": "orders", "table": "b", "status": "loaded"}]
    assert render(log, {"decision": "success", "units": units}) == [
        (INFO, "rivet load orders → OK: 2 tables in 1.0 s"),
        (INFO, "  table    orders.a: loaded"),
        (INFO, "  table    orders.b: loaded"),
    ]


def test_success_headlines_without_counts_or_a_reason_carry_no_empty_part() -> None:
    """_success 40/43 (already applied), 57/58 (skipped), 62/65 (OK)."""
    unit = {"export": "orders", "status": "success"}
    log, _ = make()
    log.record({"event": "rivet.apply.already_applied", "export": "orders"})
    assert render(log, {"decision": "success", "units": [unit]})[0] == (
        INFO,
        "rivet apply orders → SKIPPED: an earlier try already applied this plan; not applied again",
    )
    log, _ = make("run")
    skipped = {"export": "orders", "status": "skipped"}
    assert render(log, {"decision": "success", "units": [skipped]})[0] == (
        INFO,
        "rivet run orders → SKIPPED: nothing to do in 1.0 s",
    )
    log, _ = make("run")
    assert render(log, {"decision": "success", "units": [unit]})[0] == (INFO, "rivet run orders → OK in 1.0 s")


@pytest.mark.parametrize(
    ("readings", "exit_status", "shown"),
    [
        ((0.0, 0.0, 3.0, 3.5), 0, False),  # the step is the task: the headline already says it
        ((0.0, 0.0, 3.0, 4.0), 0, True),  # a second outside the step (a lock wait) is worth its line
        ((0.0, 0.0, 3.0, 3.5), 2, True),  # a step that failed always says how
    ],
)
def test_one_step_that_took_the_task_s_time_has_no_line_of_its_own(
    monkeypatch: pytest.MonkeyPatch, readings: tuple[float, ...], exit_status: int, shown: bool
) -> None:
    """Owner, 2026-10-20: `load done in 11.9 s` under `OK: 50 rows in 12.0 s` repeats the headline."""
    monkeypatch.setattr(tasklog, "clock", iter(readings).__next__)
    log, _ = make("load", "cdc", "leads")
    log.record({"event": "rivet.start", "step": "load"})
    log.record({"event": "rivet.step", "exit_status": exit_status})
    lines = render(log, {"decision": "success" if exit_status == 0 else "fail", "error": {"class": "generic"}, "units": []})
    assert any(text.startswith("  load ") for _, text in lines) is shown


def test_step_lines_of_a_step_that_did_not_end_an_unnamed_strategy_and_a_layout_plan() -> None:
    """_step_lines 41/51/62/63/64 (no end), 87/88 (sealed without a strategy), 90/91/92 (a plan task)."""
    log, _ = make("run")
    log.record({"event": "rivet.start", "step": "run"})
    assert render(log, {"decision": "fail", "error": {"class": "generic"}, "units": []})[1] == (INFO, "  run      did not end")
    log, _ = make("load")  # a step left without its end does not hide the steps after it (_step_lines 43, r3 PR)
    log.record({"event": "rivet.start", "step": "load"})
    log.record({"event": "rivet.start", "step": "compact"})
    log.record({"event": "rivet.step", "exit_status": 0})
    assert render(log, {"decision": "success", "units": []})[1:3] == [
        (INFO, "  load     did not end"),
        (INFO, "  compact  done in 1.0 s"),
    ]
    log, _ = make()
    log.record({"event": "rivet.start", "step": "plan"})
    log.record({"event": "rivet.step", "exit_status": 0})
    log.record({"event": "rivet.plan.sealed", "strategy": None})
    assert render(log, {"decision": "success", "units": []})[1] == (INFO, "  plan     sealed in 1.0 s")
    log, _ = make("plan", None)
    log.record({"event": "rivet.start", "step": "plan"})
    log.record({"event": "rivet.step", "exit_status": 0})
    assert render(log, {"decision": "success", "units": []})[1] == (INFO, "  plan     plan layout written in 1.0 s")


def test_notes_sqlite_an_unknown_degradation_warnings_max_events_and_a_ledger_problem() -> None:
    """_notes 20/21 (SQLite from `degraded` alone), 34-36 (an unknown key), 48-51 (rivet's warning text), 59 (the
    exports of a max_events stop), 68-75 (the crash ledger's problem)."""
    log, _ = make("run")
    log.record({"event": "rivet.warning", "warning": "other", "text": "watch out"})
    log.record({"event": "rivet.cdc.max_events", "exports": ["a", "b"]})
    log.record({"event": "rivet.crashed", "ledger_problem": "the ledger is unwritable"})
    lines = render(log, {"decision": "success", "units": [], "degraded": ["state_url_sqlite", "mystery"]})
    assert lines[1:] == [
        (INFO, f"  note     SQLite state is single-host — see {SETUP_GUIDE}"),
        (INFO, "  note     degraded: mystery"),
        (WARNING, "  note     watch out"),
        (INFO, "  note     stopped at max_events (a, b): a backlog remains; the next run goes on from here"),
        (WARNING, "  note     the ledger is unwritable"),
    ]


BQ = {"target": "bigquery", "project": "p", "dataset": "d"}
EXPORTS = [{"name": "snap", "mode": "full"}, {"name": "inc", "mode": "incremental"}, {"name": "cdc", "mode": "cdc"}]


@pytest.mark.parametrize(
    ("load", "cmd", "export", "table", "expected"),
    [
        (BQ, "load", "snap", "t", ("p.d.t", None)),  # a full export's load replaces the table itself
        (BQ, "load", "inc", "t", ("p.d.t__changes", None)),  # any other appends to its log
        (BQ, "load", "cdc", "t", ("p.d.t__changes", None)),
        (BQ, "load", "unknown", "t", ("p.d.t__changes", None)),
        (BQ, "compact", "cdc", "t", ("p.d.t", "p.d.t__changes")),  # the buffer merged into the base
        ({"target": "bigquery", "dataset": "d"}, "load", "snap", "t", ("d.t", None)),  # project from the credentials
        ({"target": "snowflake", "database": "DB", "schema": "S"}, "load", "snap", "T", ("DB.S.T", None)),
        ({"target": "clickhouse", "database": "db"}, "load", "snap", "t", ("db.t", None)),
        ({"target": "duckdb"}, "load", "snap", "t", (None, None)),
        (BQ, "load", "snap", None, (None, None)),
        ("bigquery", "load", "snap", "t", (None, None)),
    ],
)
def test_the_config_names_the_warehouse_objects(load: Any, cmd: str, export: str, table: str | None, expected: tuple) -> None:
    from airflow_provider_rivet.invocation import config_target

    assert config_target({"load": load, "exports": EXPORTS}, cmd, export, table) == expected
    assert config_target({"exports": EXPORTS}, cmd, export, "t") == (None, None)


def test_several_units_each_name_their_table() -> None:
    log, _ = make("load", "orders")
    for table, target, source in (("a", "p.d.a", "rivet"), ("b", "p.d.b", "config")):
        record = {"export": "orders", "table": table, "target": target, "buffer": None, "target_from": source}
        log.record({"event": "rivet.unit", **record})
    units = [{"export": "orders", "table": t, "status": "loaded", "rows": 1} for t in "ab"]
    lines = render(log, {"decision": "success", "units": units})
    assert lines[0][1].startswith("rivet load orders → OK: 2 tables")
    assert (INFO, "  table    orders.a → p.d.a: loaded, 1 row") in lines
    assert (INFO, "  table    orders.b → p.d.b (from config): loaded, 1 row") in lines
