"""The task log's readable block, built straight from `TaskLog` records: every branch's exact words.

These texts are the task log's contract: the headline's outcome word (OK, SKIPPED, REFUSED, FAILED, CRASHED, STOPPED,
BUSY), `final` / `retryable`, the try count, the step words and the notes. tests/test_task_log.py pins whole golden
blocks through the operator; this file pins each branch of `tasklog.py` alone (its mutants, tasklog.x_* and
tasklog.xǁTaskLogǁ*, were never measured before).
"""

from __future__ import annotations

import itertools
import logging
from pathlib import Path
from typing import Any

import pytest

from airflow_provider_rivet import tasklog

GUIDE = "https://github.com/panchenkoai/airflow-provider-rivet#local-worker-setup-guide"


class Lines:
    """A `TaskLog` with its clock one second per reading and every written line kept as (level, text)."""

    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, command: str = "run", export: str | None = "orders", table: str | None = None
    ) -> None:
        ticks = itertools.count(100)  # the task starts at 100 s: a duration is a difference, never a sum
        monkeypatch.setattr(tasklog, "clock", lambda: float(next(ticks)))
        self.written: list[tuple[int, str]] = []
        self.log = tasklog.TaskLog(command, export, table, lambda level, text: self.written.append((level, text)))

    def records(self, *records: dict) -> Lines:
        for record in records:
            self.log.record(record)
        return self

    def render(self, payload: dict, try_number: int = 1, max_tries: int = 0, stop: bool = False) -> list[str]:
        names = {logging.DEBUG: "DEBUG", logging.INFO: "INFO", logging.WARNING: "WARNING", logging.ERROR: "ERROR"}
        return [f"{names[level]} {text}" for level, text in self.log.render(payload, try_number, max_tries, stop)]


def step(name: str, exit_status: int | None = 0, signal: int | None = None) -> tuple[dict, dict]:
    return {"event": "rivet.start", "step": name, "stderr_path": f"/s/{name}.log"}, {
        "event": "rivet.step",
        "exit_status": exit_status,
        "signal": signal,
    }


def test_every_record_is_one_sorted_json_line_at_debug_and_kept(monkeypatch: pytest.MonkeyPatch) -> None:
    lines = Lines(monkeypatch).records({"z": 1, "event": "rivet.unit", "path": Path("/p")})
    assert lines.written == [(logging.DEBUG, '{"event": "rivet.unit", "path": "/p", "z": 1}')]
    assert lines.log.events == [{"z": 1, "event": "rivet.unit", "path": Path("/p")}]


def test_a_lock_wait_and_a_streamed_stderr_line_are_shown_when_they_happen(monkeypatch: pytest.MonkeyPatch) -> None:
    lines = Lines(monkeypatch).records(
        {"event": "rivet.lock.wait", "lock_wait": 30, "unit": "export `orders`"},
        {"event": "rivet.stderr", "step": "apply", "line": "warning: slow"},
        {"event": "rivet.step", "exit_status": 0},  # a step record before any start changes nothing
    )
    assert [(level, text) for level, text in lines.written if level != logging.DEBUG] == [
        (
            logging.INFO,
            "waiting up to 30 s for the lock of export `orders`: another try of this task, or a rivet process it left "
            "running, holds it",
        ),
        (logging.INFO, "rivet apply stderr: warning: slow"),
    ]
    assert lines.log.steps == []


@pytest.mark.parametrize(
    ("command", "export", "table", "head"),
    [
        ("run", "orders", None, "rivet run orders →"),
        ("cdc_run", "orders", None, "rivet run orders →"),
        ("load", "orders", "public.t", "rivet load orders table public.t →"),
        ("run", None, None, "rivet run →"),
    ],
)
def test_the_headline_names_command_export_and_table(
    monkeypatch: pytest.MonkeyPatch, command: str, export: Any, table: Any, head: str
) -> None:
    assert Lines(monkeypatch, command, export, table).render({"decision": "success"}) == [f"INFO {head} OK in 1.0 s"]


@pytest.mark.parametrize(
    ("payload", "events", "shown"),
    [
        (
            {"preflight_refusal": "RIVET_AIRFLOW_X"},
            [],
            ["ERROR rivet run orders → REFUSED [RIVET_AIRFLOW_X] before rivet started, final", "ERROR   why      None"],
        ),
        (
            {"preflight_refusal": "RIVET_AIRFLOW_X"},
            [{"event": "rivet.refused", "text": "no such export"}, {"event": "rivet.refused", "text": "second"}],
            ["ERROR rivet run orders → REFUSED [RIVET_AIRFLOW_X] before rivet started, final", "ERROR   why      no such export"],
        ),
        (
            {"stopped_before": "apply"},
            [],
            ["WARNING rivet run orders → STOPPED by Airflow before rivet apply started; rivet apply was not run"],
        ),
        (
            {"busy": "RIVET_AIRFLOW_EXPORT_BUSY"},
            [],
            [
                "WARNING rivet run orders → BUSY [RIVET_AIRFLOW_EXPORT_BUSY]: the lock of None stayed held for None s; rivet was not started, no try left (try 1 of 1)"
            ],
        ),
    ],
)
def test_the_outcomes_before_rivet_ran(monkeypatch: pytest.MonkeyPatch, payload: dict, events: list, shown: list[str]) -> None:
    lines = Lines(monkeypatch).records(*events)
    assert lines.render(payload) == shown


def ok(**unit: Any) -> dict:
    return {"status": "success", **unit}


@pytest.mark.parametrize(
    ("command", "payload", "events", "headline"),
    [
        ("run", {"units": [ok(rows=3, files=1, run_id="r1")]}, [], "OK: 3 rows, 1 file in 1.0 s (run r1)"),
        ("run", {"units": [ok(rows=1, files=2)]}, [], "OK: 1 row, 2 files in 1.0 s"),
        ("run", {"units": [ok(rows=None, files=0)]}, [], "OK: 0 files in 1.0 s"),
        ("run", {"units": [ok(), "not a unit"]}, [], "OK in 1.0 s"),
        ("run", {"units": [ok(rows=1, run_id="a"), ok(rows=2, run_id="b")]}, [], "OK: 2 exports, 3 rows in 1.0 s"),
        ("run", {"units": [ok(run_id="a"), ok(run_id="a")]}, [], "OK: 2 exports in 1.0 s (run a)"),
        ("load", {"units": [ok(rows=1), ok(rows=1)]}, [], "OK: 2 tables, 2 rows in 1.0 s"),
        ("compact", {"units": [ok(), ok(), ok()]}, [], "OK: 3 tables in 1.0 s"),
        ("run", {"units": [ok(run_id=7)]}, [], "OK in 1.0 s"),
        ("run", {"units": [{"status": "skipped"}]}, [], "SKIPPED: nothing to do in 1.0 s"),
        (
            "run",
            {"units": [{"status": "skipped", "run_id": "s"}], "skip_reason": "empty"},
            [],
            "SKIPPED: nothing to do (empty) in 1.0 s (run s)",
        ),
        ("run", {"units": [{"status": "skipped"}, ok()]}, [], "OK: 2 exports in 1.0 s"),
        ("run", {"units": []}, [], "OK in 1.0 s"),
        (
            "apply",
            {"units": [ok(run_id="e")]},
            [{"event": "rivet.apply.already_applied"}],
            "SKIPPED: an earlier try already applied this plan; not applied again (run e)",
        ),
        (
            "apply",
            {"units": [ok(rows=5, files=1)]},
            [{"event": "rivet.apply.already_applied"}],
            "SKIPPED: an earlier try already applied this plan: 5 rows, 1 file; not applied again",
        ),
    ],
)
def test_a_success_headline(monkeypatch: pytest.MonkeyPatch, command: str, payload: dict, events: list, headline: str) -> None:
    lines = Lines(monkeypatch, command, "x").records(*events)
    shown = lines.render({"decision": "success", **payload})
    word = command.replace("cdc_", "")
    assert shown[0] == f"INFO rivet {word} x → {headline}"


@pytest.mark.parametrize(
    ("payload", "stop", "line"),
    [
        (
            {"decision": "retry_crashed", "signal": 9, "crashed_retry": True},
            False,
            "WARNING rivet run orders → CRASHED: rivet ended by signal 9; Airflow retries it (try 1 of 2) under the crash budget",
        ),
        (
            {"decision": "retry_crashed", "exit_status": 134},
            False,
            "ERROR rivet run orders → CRASHED: rivet ended by exit 134, final: the crash budget is used up",
        ),
        (
            {"decision": "retry_crashed", "signal": 0},
            False,
            "ERROR rivet run orders → CRASHED: rivet ended by signal 0, final: the crash budget is used up",
        ),
        (
            {"decision": "retry_crashed", "exit_status": 1, "crashed_retry": True},
            True,
            "WARNING rivet run orders → STOPPED by Airflow: rivet ended by exit 1",
        ),
        (
            {"decision": "fail", "error": {"class": "refusal", "code": "RIVET_R", "action": "fix it"}},
            False,
            "ERROR rivet run orders → REFUSED [RIVET_R], final, action: fix it",
        ),
        ({"decision": "fail", "error": {"class": "crashed"}}, False, "ERROR rivet run orders → CRASHED, final"),
        ({"decision": "fail", "error": {"class": "config"}}, False, "ERROR rivet run orders → FAILED config, final"),
        (
            {"decision": "fail", "error": {"class": "retryable", "code": "RIVET_T"}},
            False,
            "ERROR rivet run orders → FAILED [RIVET_T], final",
        ),
        ({"decision": "fail"}, False, "ERROR rivet run orders → FAILED None, final"),
        (
            {"decision": "retry", "error": {"class": "retryable", "code": "RIVET_T"}},
            False,
            "WARNING rivet run orders → FAILED [RIVET_T], retryable, Airflow retries it (try 1 of 2)",
        ),
        (
            {"decision": "fail", "error": {"class": "refusal"}, "units": [{"status": "failed", "run_id": "f1"}, ok(run_id="s1")]},
            False,
            "ERROR rivet run orders → REFUSED, final (run f1)",
        ),
        (
            {
                "decision": "fail",
                "error": {"class": "refusal"},
                "units": [{"status": "failed", "run_id": "f1"}, {"status": "failed", "run_id": "f2"}],
            },
            False,
            "ERROR rivet run orders → REFUSED, final",
        ),
    ],
)
def test_a_failure_headline_says_final_or_retryable(
    monkeypatch: pytest.MonkeyPatch, payload: dict, stop: bool, line: str
) -> None:
    assert Lines(monkeypatch).render(payload, try_number=1, max_tries=1, stop=stop)[0] == line


def test_the_step_lines_say_what_each_rivet_process_did(monkeypatch: pytest.MonkeyPatch) -> None:
    lines = Lines(monkeypatch, "apply").records(
        *step("metrics"),
        *step("plan"),
        {"event": "rivet.plan.sealed", "strategy": "incremental"},
        *step("apply", exit_status=2),
        *step("metrics"),
        *step("load", signal=15),
        step("compact")[0],
    )
    assert lines.render({"decision": "fail", "error": {"class": "x"}})[1:7] == [
        "INFO   metrics  no earlier try's apply of this plan in rivet's state in 1.0 s",
        "INFO   plan     sealed, strategy incremental in 1.0 s",
        "INFO   apply    exit 2 in 1.0 s",
        "INFO   metrics  read the run's counts from rivet's state in 1.0 s",
        "INFO   load     ended by signal 15 in 1.0 s",
        "INFO   compact  did not end",
    ]


def test_a_plan_step_outside_apply_writes_a_layout_and_a_seal_without_strategy_says_only_sealed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = Lines(monkeypatch, "plan").records(*step("plan"))
    assert plan.render({"decision": "success"})[1:] == ["INFO   plan     plan layout written in 1.0 s"]
    sealed = Lines(monkeypatch, "apply").records(*step("plan"), {"event": "rivet.plan.sealed", "strategy": None}, *step("apply"))
    assert sealed.render({"decision": "success"})[1:] == ["INFO   plan     sealed in 1.0 s", "INFO   apply    done in 1.0 s"]


def test_an_apply_without_its_plan_step_replays_the_sealed_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    replay = Lines(monkeypatch, "apply").records(*step("apply"))
    assert replay.render({"decision": "success"})[1:] == [
        "INFO   plan     replayed the sealed plan of an earlier try",
        "INFO   apply    done in 1.0 s",
    ]
    run = Lines(monkeypatch, "run").records(*step("apply"))
    assert run.render({"decision": "success"})[1:] == ["INFO   apply    done in 1.0 s"]
    done = Lines(monkeypatch, "apply").records(*step("metrics"), {"event": "rivet.apply.already_applied"}, *step("apply"))
    assert done.render({"decision": "success"})[1:] == [
        "INFO   metrics  found the earlier try's run in rivet's state in 1.0 s",
        "INFO   apply    done in 1.0 s",
    ]


@pytest.mark.parametrize(
    ("command", "payload", "tail"),
    [
        (
            "load",
            {
                "decision": "success",
                "units": [ok(export="e", table="t1", rows=2, run_id="r"), {"export": "e", "table": "t2", "status": "skipped"}],
            },
            ["INFO   table    e.t1: success, 2 rows (run r)", "INFO   table    e.t2: skipped"],
        ),
        ("run", {"decision": "success", "units": [ok(export="e", rows=2)]}, []),
        (
            "run",
            {"decision": "fail", "units": [{"status": "failed", "error": {"class": "refusal"}}], "stderr_path": "/s/x.log"},
            ["INFO   export   (config): failed refusal", "INFO   stderr   /s/x.log"],
        ),
        ("run", {"decision": "retry", "units": [ok(export="e")]}, []),
        ("run", {"units": [{"status": "failed", "export": "e"}], "stderr_path": "/s/x.log"}, ["INFO   export   e: failed None"]),
    ],
)
def test_unit_lines_and_the_stderr_path(monkeypatch: pytest.MonkeyPatch, command: str, payload: dict, tail: list[str]) -> None:
    assert Lines(monkeypatch, command).render(payload)[1:] == tail


def test_the_notes_say_what_the_operator_should_know(monkeypatch: pytest.MonkeyPatch) -> None:
    lines = Lines(monkeypatch).records(
        {"event": "rivet.warning", "warning": "sqlite_single_host", "text": "hidden"},
        {"event": "rivet.warning", "warning": "other", "text": "act on this"},
        {"event": "rivet.cdc.max_events", "exports": ["a", 2]},
        {"event": "rivet.cdc.max_events", "exports": ["second"]},
        {"event": "rivet.crashed", "ledger_problem": "the ledger is unreadable"},
    )
    payload = {"decision": "success", "degraded": ["no_notify", "lock_wait", "stderr_temp", "brand_new", 5, "state_url_sqlite"]}
    assert lines.render(payload)[1:] == [
        "INFO   note     this binary lacks: no_notify, lock_wait (the operator works around them)",
        "INFO   note     SQLite state is single-host — see " + GUIDE,
        "INFO   note     " + tasklog.NOTES["stderr_temp"],
        "INFO   note     degraded: brand_new",
        "WARNING   note     act on this",
        "INFO   note     stopped at max_events (a, 2): a backlog remains; the next run goes on from here",
        "WARNING   note     the ledger is unreadable",
    ]


def test_a_sqlite_warning_alone_is_the_sqlite_note_and_no_note_is_written_without_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    warned = Lines(monkeypatch).records({"event": "rivet.warning", "warning": "sqlite_single_host"})
    assert warned.render({"decision": "success"})[1:] == ["INFO   note     SQLite state is single-host — see " + GUIDE]
    quiet = Lines(monkeypatch).records({"event": "rivet.cdc.max_events"}, {"event": "rivet.crashed"})
    assert quiet.render({"decision": "success", "degraded": None})[1:] == [
        "INFO   note     stopped at max_events (): a backlog remains; the next run goes on from here"
    ]


def test_each_degradation_note_is_its_own_sentence() -> None:
    assert tasklog.NOTES == {
        "state_url_sqlite": None,
        "error_object": "rivet's failure line has no class or action: the class is read from its exit class",
        "per_unit_error": "several units failed and rivet gave one shared failure object for all of them",
        "stop_reason": "this binary reports no CDC stop reason: a max_events stop reads like caught up",
        "stderr_temp": "no state directory: rivet's stderr file is in the worker's temp directory and dies with it",
        "crashed_ledger": "the crash-retry ledger cannot be used: no crash retry beyond the task's first tries",
        "result_missing": "rivet wrote no summary file: units are taken from its exit status, counts are unknown",
        "result_malformed": "part of rivet's result did not have the contract's shape and was ignored (XCom `degraded`)",
        "exit_zero_failed_unit": "rivet exited 0 while its summary reports a failed unit",
    }


def test_finish_writes_the_rendered_block(monkeypatch: pytest.MonkeyPatch) -> None:
    lines = Lines(monkeypatch)
    lines.log.finish({"decision": "retry", "error": {"class": "retryable"}}, 2, 3, False)
    assert lines.written == [(logging.WARNING, "rivet run orders → FAILED, retryable, Airflow retries it (try 2 of 4)")]


def test_a_busy_headline_names_the_lock_and_how_long_it_waited(monkeypatch: pytest.MonkeyPatch) -> None:
    lines = Lines(monkeypatch).records({"event": "rivet.lock.wait", "lock_wait": 30, "unit": "export `orders`"})
    assert lines.render({"busy": "RIVET_AIRFLOW_EXPORT_BUSY"}, try_number=1, max_tries=2)[0] == (
        "WARNING rivet run orders → BUSY [RIVET_AIRFLOW_EXPORT_BUSY]: the lock of export `orders` stayed held for 30 s; "
        "rivet was not started, Airflow retries it (try 1 of 3)"
    )


def test_a_success_holds_no_unit_line_for_one_unit_and_no_stderr_line(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only a failed task names its stderr file and its one failed unit; a success with one unit says it in the headline."""
    payload = {"decision": "success", "units": [{"status": "failed", "export": "e"}], "stderr_path": "/s/x.log"}
    shown = Lines(monkeypatch).render(payload)
    assert [line for line in shown if "stderr" in line or "export   " in line] == []


def test_a_failed_unit_line_carries_its_code(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "decision": "fail",
        "units": [{"status": "failed", "export": "e", "error": {"code": "RIVET_X", "class": "refusal"}}],
    }
    assert Lines(monkeypatch).render(payload)[1:] == ["INFO   export   e: failed [RIVET_X] refusal"]


def test_sqlite_state_alone_is_the_sqlite_note(monkeypatch: pytest.MonkeyPatch) -> None:
    assert Lines(monkeypatch).render({"decision": "success", "degraded": ["state_url_sqlite"]})[1:] == [
        "INFO   note     SQLite state is single-host — see " + GUIDE
    ]


def test_finish_passes_a_stop_through(monkeypatch: pytest.MonkeyPatch) -> None:
    lines = Lines(monkeypatch)
    lines.log.finish({"decision": "retry_crashed", "signal": 15, "crashed_retry": True}, 1, 2, True)
    assert lines.written == [(logging.WARNING, "rivet run orders → STOPPED by Airflow: rivet ended by signal 15")]


def test_the_headline_time_is_the_tasks_own_duration(monkeypatch: pytest.MonkeyPatch) -> None:
    lines = Lines(monkeypatch).records(*step("run"))  # two clock readings after the start
    assert lines.render({"decision": "success"})[0] == "INFO rivet run orders → OK in 3.0 s"
