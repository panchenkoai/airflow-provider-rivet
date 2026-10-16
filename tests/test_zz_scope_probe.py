"""THROWAWAY (never merged): names callbacks.py and tasklog.py to the PR mutation gate so it measures their mutants
against the base's own tests. It calls each function once and asserts nothing, so it kills no mutant itself."""

from __future__ import annotations

import contextlib

from airflow_provider_rivet import callbacks, tasklog


def test_probe_calls_every_function_of_callbacks_and_tasklog_and_asserts_nothing() -> None:
    payload = {"units": [{"export": "e", "error": {"code": "C", "class": "x"}}], "stderr_path": "/nonexistent"}
    calls = [
        lambda: callbacks._failed_units(payload),
        lambda: callbacks._retry_word({}),
        lambda: callbacks._class_and_retry({}),
        lambda: callbacks.format_failure(payload, dag_id="d", task_id="t", run_id="r"),
        lambda: callbacks.format_title(payload, dag_id="d", task_id="t"),
        lambda: callbacks._redact("x"),
        lambda: callbacks._rivet_errors(payload),
        lambda: callbacks.format_error_text(payload, limit=10),
        lambda: callbacks._mrkdwn("x"),
        lambda: callbacks._code_block("x"),
        lambda: callbacks.slack_failure_callback(),
        lambda: callbacks._post_webhook(None, "c", {}),
        lambda: callbacks._post_threaded(None, "c", "C", {}),
        lambda: tasklog._plural(1, "row"),
        lambda: tasklog._secs(1.0),
        lambda: tasklog._counts([]),
        lambda: tasklog._unit_name({}),
    ]
    for call in calls:
        with contextlib.suppress(BaseException):
            call()
    with contextlib.suppress(BaseException):
        log = tasklog.TaskLog("run", "e", None, lambda level, text: None)
        log.record({"event": "rivet.start", "step": "run"})
        log.finish({"decision": "fail"}, 1, 0, False)
        log._first("x")
        log.render({}, 1, 0, False)
        log._success("h", [], {}, 1.0)
        log._failure("h", {}, {}, [], "r", False)
        log._step_lines({})
        log._unit_lines([])
        log._notes({})
