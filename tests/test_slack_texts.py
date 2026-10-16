"""The Slack alert's texts and the callback's calls, line by line: title, failure text, thread text, the hook each
path builds and the task-log line saying what Slack answered. Each line is what an on-call reader acts on, so a
changed word is a changed contract (mutants callbacks.x_*)."""

from __future__ import annotations

import logging
import sys
import types
from pathlib import Path
from typing import Any

import pytest
from conftest import FakeTI
from test_slack_alerts import BOT_MODULE, PREFLIGHT, WEBHOOK_MODULE, stderr_file, unit

from airflow_provider_rivet.callbacks import format_error_text, format_failure, format_title, slack_failure_callback


def test_the_failure_text_of_a_failed_unit_names_unit_table_code_class_action_stderr_and_link() -> None:
    payload = {
        "units": [
            {
                "export": "orders",
                "table": "public.t",
                "error": {"code": "RIVET_X", "class": "refusal", "retryable": False, "action": "do y"},
            }
        ],
        "stderr_path": "/s/e.log",
    }
    assert format_failure(payload, dag_id="d", task_id="t", run_id="r1", log_url="http://af/l").split("\n") == [
        ":x: *d.t* failed (run `r1`)",
        "• *orders / public.t*: `RIVET_X` `refusal` (not retryable)",
        "    action: do y",
        "stderr on the worker: `/s/e.log`",
        "<http://af/l|task log>",
    ]


@pytest.mark.parametrize(
    ("error", "line"),
    [
        ({"class": "retryable", "retryable": True}, "• *(process)*: (retryable)"),
        ({"class": "not retryable", "retryable": False}, "• *(process)*: (not retryable)"),
        ({"class": None, "retryable": True}, "• *(process)*: (retryable)"),
        ({"retryable": False}, "• *(process)*: (not retryable)"),
        ({"code": "RIVET_C", "class": "crashed", "retryable": True}, "• *(process)*: `RIVET_C` `crashed` (retryable)"),
        ({"code": "", "class": "refusal"}, "• *(process)*: `refusal` (not retryable)"),
    ],
)
def test_a_process_level_error_is_one_line_with_the_class_said_once(error: dict, line: str) -> None:
    """No unit carries an error: the payload's own error is reported as `(process)`; a class that is the retry word
    itself is not repeated."""
    payload = {"units": [{"export": "a", "status": "success", "error": None}], "error": error}
    assert format_failure(payload, dag_id="d", task_id="t", run_id="r").split("\n") == [":x: *d.t* failed (run `r`)", line]


def test_more_than_ten_failed_units_are_counted_not_listed() -> None:
    units = [unit(f"RIVET_{n}", "refusal", False, export=f"e{n}") for n in range(12)]
    lines = format_failure({"units": units}, dag_id="d", task_id="t", run_id="r").split("\n")
    assert (
        lines[1] == "• *e0*: `RIVET_0` `refusal` (not retryable)" and lines[10] == "• *e9*: `RIVET_9` `refusal` (not retryable)"
    )
    assert lines[11:] == ["… and 2 more failed units"] and len(lines) == 12
    ten = format_failure({"units": units[:10]}, dag_id="d", task_id="t", run_id="r").split("\n")
    assert len(ten) == 11 and not ten[-1].startswith("…")


@pytest.mark.parametrize("payload", [None, {}])
def test_a_task_that_published_no_result_says_it_died_outside_the_operator(payload: Any) -> None:
    assert format_failure(payload, dag_id="d", task_id="t", run_id="r").split("\n") == [
        ":x: *d.t* failed (run `r`)",
        "no rivet result was published for this try (the task died outside the operator)",
    ]


def test_a_preflight_refusal_says_rivet_never_started() -> None:
    assert format_failure(PREFLIGHT, dag_id="d", task_id="t", run_id="r").split("\n") == [
        ":x: *d.t* failed (run `r`)",
        "refused before rivet started: `RIVET_AIRFLOW_EXPORT_UNKNOWN`",
    ]


@pytest.mark.parametrize(
    ("payload", "exception", "kind"),
    [
        ({"units": [unit(None, "refusal", False)]}, None, "refusal (final)"),
        ({"units": [{"export": "a", "error": {"retryable": True}}]}, None, "rivet failed (retryable)"),
        ({"units": [], "error": {"code": "RIVET_P", "retryable": False}}, None, "RIVET_P (final)"),
        (
            {"units": [unit("RIVET_A", "refusal", False), unit("RIVET_B", "x", True), unit("RIVET_C", "x", True)]},
            None,
            "RIVET_A (final) +2 more",
        ),
        ({"units": []}, KeyError("k"), "KeyError"),
        ({}, "not an exception", "failed, no rivet result"),
        ({"preflight_refusal": "RIVET_AIRFLOW_X", "units": [unit("RIVET_A", "r", True)]}, None, "RIVET_AIRFLOW_X (final)"),
    ],
)
def test_the_title_names_the_first_failure_or_what_is_known(payload: Any, exception: Any, kind: str) -> None:
    assert format_title(payload, dag_id="d", task_id="t", exception=exception) == f"d.t — {kind}"


# ---- the thread text: rivet's error lines read from the end of its stderr file ----
# Mutants: callbacks.x__rivet_errors__mutmut_*, x_format_error_text__mutmut_*, x__mrkdwn__mutmut_*.


def error_text(tmp_path: Path, *lines: str, limit: int = 3500) -> str:
    return format_error_text({"stderr_path": stderr_file(tmp_path, *lines)}, limit=limit)


@pytest.mark.parametrize(
    ("lines", "text"),
    [
        (['{"error": "boom", "code": "RIVET_X"}'], "[RIVET_X] boom"),
        (['{"error": "boom [RIVET_X] here", "code": "RIVET_X"}'], "boom [RIVET_X] here"),
        (['{"message": "from message", "code": "RIVET_M"}'], "[RIVET_M] from message"),
        (['{"error": {"nested": 1}, "message": "msg"}'], "msg"),
        (['{"error": "no code"}'], "no code"),
        (['{"error": "int code", "code": 5}'], "int code"),
        (['{"error": "empty code", "code": ""}'], "empty code"),
        (['{"error": ""}', "Error: plain"], "Error: plain"),
        (['{"other": 1}', "  error: lower, indented  "], "error: lower, indented"),
        (['["Error: in a list"]', '"Error: a json string"', "Error: after"], "Error: after"),
        (['{"error": "a"}', "Error: a", '{"error": "b"}'], "a\nb"),
        (["Error: one", "noise", "ERROR: two"], "Error: one\nERROR: two"),
        (["warning: only", "  at frame"], "no error text: the task failed without a rivet result or an exception"),
    ],
)
def test_the_thread_text_is_the_error_objects_else_the_error_lines(tmp_path: Path, lines: list[str], text: str) -> None:
    assert error_text(tmp_path, *lines) == text


def test_only_the_last_64_kib_of_stderr_is_read(tmp_path: Path) -> None:
    """The error is the last thing rivet writes; a gigabyte of warnings before it is not read."""
    filler = "w" * 99 + "\n"
    path = tmp_path / "big.log"
    path.write_text("Error: early\n" + filler * 700 + "Error: late\n")
    assert format_error_text({"stderr_path": str(path)}, limit=3500) == "Error: late"
    small = tmp_path / "small.log"
    small.write_text("Error: early\n" + filler * 600 + "Error: late\n")  # under 64 KiB: read whole
    assert format_error_text({"stderr_path": str(small)}, limit=3500) == "Error: early\nError: late"


def test_invalid_utf8_in_stderr_is_replaced_not_raised(tmp_path: Path) -> None:
    path = tmp_path / "bin.log"
    path.write_bytes(b"\xff\xfe junk\nError: caf\xc3\xa9 \xff\n")
    assert format_error_text({"stderr_path": str(path)}, limit=3500) == "Error: café �"


@pytest.mark.parametrize(
    ("exception", "text"),
    [
        (ValueError(""), "ValueError"),
        (ValueError("why"), "why"),
        (None, "no error text: the task failed without a rivet result or an exception"),
    ],
)
def test_without_rivet_lines_the_exception_message_or_its_type_is_the_text(exception: Any, text: str) -> None:
    assert format_error_text({"stderr_path": None}, exception=exception, limit=3500) == text


def test_a_cut_text_ends_with_where_the_full_log_is() -> None:
    exc = ValueError("x" * 100)
    assert format_error_text(None, exception=exc, limit=100) == "x" * 100
    tail = "\n… truncated, full log: the task log"
    assert format_error_text(None, exception=exc, limit=99) == "x" * (99 - len(tail)) + tail
    url_tail = "\n… truncated, full log: http://l"
    assert format_error_text(None, exception=exc, limit=60, log_url="http://l") == "x" * (60 - len(url_tail)) + url_tail
    assert format_error_text(None, exception=exc, limit=5) == tail


# ---- the callback: which task instance, which payload, which hook, which line in the task log ----
# Mutants: callbacks.x_slack_failure_callback__mutmut_*, x__post_webhook__mutmut_*, x__post_threaded__mutmut_*,
# x_ǁ_Alertǁ*.


class Recording:
    """Both Slack hooks, recording every constructor argument (the shared fake above drops them)."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, top: dict | None = None, response: Any = "ok") -> None:
        self.made: list[tuple[str, dict]] = []
        self.posts: list[dict] = []
        self.sent: list[str] = []
        rec = self

        class SlackHook:
            def __init__(self, **kwargs: Any) -> None:
                rec.made.append(("SlackHook", kwargs))
                self.client = self

            def chat_postMessage(self, **kwargs: Any) -> dict:
                rec.posts.append(kwargs)
                return dict(top) if top is not None else {"ts": "9.1", "channel": "CREAL"}

        class SlackWebhookHook:
            def __init__(self, **kwargs: Any) -> None:
                rec.made.append(("SlackWebhookHook", kwargs))

            def send(self, text: str) -> Any:
                rec.sent.append(text)
                return types.SimpleNamespace(status_code=200, body="ok") if response == "ok" else response

        for name, hook in ((BOT_MODULE, SlackHook), (WEBHOOK_MODULE, SlackWebhookHook)):
            module = types.ModuleType(name)
            setattr(module, hook.__name__, hook)
            monkeypatch.setitem(sys.modules, name, module)


def slack_lines(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    return [(r.levelno, r.getMessage()) for r in caplog.records if r.name == "airflow.task.rivet_slack"]


class MappedTI(FakeTI):
    """A task instance with Airflow's pull signature; records what it was asked."""

    def __init__(self, map_index: Any = -1, pulled: Any = None, raises: bool = False, **kw: Any) -> None:
        super().__init__(task_id="load.t", **kw)
        self.dag_id, self.map_index, self.run_id = "dg", map_index, "ti_run"
        self.pulled, self.raises, self.asked = pulled, raises, []

    def xcom_pull(self, task_ids: object = None, key: str = "return_value", map_indexes: object = None) -> object:
        self.asked.append((task_ids, key, map_indexes))
        if self.raises:
            raise RuntimeError("no xcom backend")
        return self.pulled


@pytest.mark.parametrize(
    ("map_index", "task"),
    [(3, "load.t[3]"), (0, "load.t[0]"), (-1, "load.t"), (None, "load.t")],
)
def test_the_callback_pulls_its_own_map_index_and_names_it_in_the_title(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, map_index: Any, task: str
) -> None:
    rec = Recording(monkeypatch)
    caplog.set_level(logging.INFO)
    ti = MappedTI(map_index=map_index, pulled=PREFLIGHT)
    slack_failure_callback("hooky", timeout=7)({"ti": ti, "run_id": "ctx_run", "exception": None})
    assert ti.asked == [("load.t", "return_value", map_index)]
    assert rec.made == [("SlackWebhookHook", {"slack_webhook_conn_id": "hooky", "timeout": 7})]
    assert rec.sent == [
        f":x: *dg.{task} — RIVET_AIRFLOW_EXPORT_UNKNOWN (final)*\nrun `ctx_run`\n```\nno error text: the task failed without a rivet result or an exception\n```"
    ]
    assert slack_lines(caplog) == [(logging.INFO, f"Slack alert for dg.{task} sent through connection `hooky`: HTTP 200 ok")]


@pytest.mark.parametrize(("pulled", "raises"), [(None, False), (["a", "list"], False), ("text", False), (PREFLIGHT, True)])
def test_a_payload_that_cannot_be_read_as_a_result_is_no_result(
    monkeypatch: pytest.MonkeyPatch, pulled: Any, raises: bool
) -> None:
    rec = Recording(monkeypatch)
    ti = MappedTI(pulled=pulled, raises=raises)
    ti.log_url = None
    slack_failure_callback()({"ti": ti, "exception": ValueError("why")})
    assert rec.made == [("SlackWebhookHook", {"slack_webhook_conn_id": "slack_default"})]
    assert rec.sent == [":x: *dg.load.t — ValueError*\nrun `ti_run`\n```\nwhy\n```"]


@pytest.mark.parametrize(
    ("response", "answer"),
    [
        (types.SimpleNamespace(status_code=200, body=""), "HTTP 200"),
        (types.SimpleNamespace(status_code=204), "HTTP 204"),
        (None, "no response object"),
        (types.SimpleNamespace(body="x"), "no response object"),
    ],
)
def test_the_webhook_line_says_what_slack_answered(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, response: Any, answer: str
) -> None:
    Recording(monkeypatch, response=response)
    caplog.set_level(logging.INFO)
    slack_failure_callback("w")({"ti": MappedTI(pulled=None), "run_id": "r"})
    assert slack_lines(caplog) == [(logging.INFO, f"Slack alert for dg.load.t sent through connection `w`: {answer}")]


def test_the_threaded_post_replies_where_slack_says_the_message_went(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    rec = Recording(monkeypatch)
    caplog.set_level(logging.INFO)
    ti = MappedTI(pulled=PREFLIGHT)
    ti.log_url = ""
    slack_failure_callback("bot", channel="#alerts", base_url="http://slack")(
        {"ti": ti, "run_id": "r<1>", "exception": ValueError("a ``` b <c>")}
    )
    assert rec.made == [("SlackHook", {"slack_conn_id": "bot", "base_url": "http://slack"})]
    top, reply = rec.posts
    assert top == {
        "channel": "#alerts",
        "text": "dg.load.t — RIVET_AIRFLOW_EXPORT_UNKNOWN (final)",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": ":x: *dg.load.t — RIVET_AIRFLOW_EXPORT_UNKNOWN (final)*"}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": "run `r&lt;1&gt;`"}]},
        ],
    }
    assert reply == {"channel": "CREAL", "thread_ts": "9.1", "text": "```\na ''' b &lt;c&gt;\n```"}
    assert slack_lines(caplog) == [
        (
            logging.INFO,
            "Slack alert for dg.load.t posted to channel CREAL (ts 9.1) through connection `bot`, its error text in the thread",
        )
    ]


def test_without_a_channel_in_slacks_answer_the_reply_goes_to_the_channel_asked(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = Recording(monkeypatch, top={"ts": "5.5"})
    slack_failure_callback("bot", channel="C9")({"ti": MappedTI(pulled=None), "run_id": "r"})
    assert [(p["channel"], p.get("thread_ts")) for p in rec.posts] == [("C9", None), ("C9", "5.5")]


def test_the_thread_text_is_capped_at_its_limit_and_the_webhook_text_at_its_own(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = Recording(monkeypatch)
    ctx = {"ti": MappedTI(pulled=None), "run_id": "r", "exception": ValueError("y" * 5000)}
    slack_failure_callback("bot", channel="C")(ctx)
    slack_failure_callback("hook")(ctx)
    tail = "\n… truncated, full log: the task log"
    assert rec.posts[1]["text"] == "```\n" + "y" * (3500 - len(tail)) + tail + "\n```"
    assert rec.sent[0].endswith("\n```\n" + "y" * (3000 - len(tail)) + tail + "\n```")
