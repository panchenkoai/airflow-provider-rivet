"""Slack alerts: the title, the trace in its thread, the webhook fallback, the README samples and the example DAGs."""

from __future__ import annotations

import logging
import sys
import types
from pathlib import Path
from typing import Any

import pytest
from conftest import DagRig, FakeTI

from airflow_provider_rivet import callbacks
from airflow_provider_rivet.callbacks import format_failure, format_title, format_trace, slack_failure_callback

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "rivet_slack_alerts.py"
WEBHOOK_MODULE = "airflow.providers.slack.hooks.slack_webhook"
BOT_MODULE = "airflow.providers.slack.hooks.slack"
PLANTED = "pl4nted-s3cret-value"


def unit(code: str | None, cls: str, retryable: bool, export: str = "users", action: str | None = None) -> dict:
    """One failed XCom unit as the operator publishes it."""
    error = {"code": code, "kind": None, "class": cls, "exit_code": 2, "retryable": retryable, "action": action}
    return {"export": export, "table": None, "status": "failed", "error": error}


PREFLIGHT = {"command": "apply", "decision": "fail", "preflight_refusal": "RIVET_AIRFLOW_EXPORT_UNKNOWN", "units": []}
RETRYABLE = {"decision": "retry", "units": [unit("RIVET_PLAN_SOURCE_UNREADABLE", "retryable", True)]}
REFUSAL = {"decision": "fail", "units": [unit("RIVET_SOURCE_CDC_LOG_GAP", "refusal", False, action="re-baseline")]}
TWO = {"units": [unit("RIVET_SOURCE_CDC_LOG_GAP", "refusal", False, "a"), unit("RIVET_X", "refusal", False, "b")]}
CRASHED = {"units": [unit(None, "crashed", True)]}


@pytest.mark.parametrize(
    ("payload", "exception", "title"),
    [
        (PREFLIGHT, None, "d.apply.users — RIVET_AIRFLOW_EXPORT_UNKNOWN (final)"),
        (RETRYABLE, None, "d.apply.users — RIVET_PLAN_SOURCE_UNREADABLE (retryable)"),
        (REFUSAL, None, "d.apply.users — RIVET_SOURCE_CDC_LOG_GAP (final)"),
        (TWO, None, "d.apply.users — RIVET_SOURCE_CDC_LOG_GAP (final) +1 more"),
        (CRASHED, None, "d.apply.users — crashed (retryable)"),
        (None, TimeoutError("took too long"), "d.apply.users — TimeoutError"),
        (None, None, "d.apply.users — failed, no rivet result"),
    ],
)
def test_the_title_is_dag_task_and_the_error_type_per_outcome_class(payload: Any, exception: Any, title: str) -> None:
    assert format_title(payload, dag_id="d", task_id="apply.users", exception=exception) == title


def test_a_retryable_class_is_not_said_twice() -> None:
    """The owner's screenshot read "`RIVET_PLAN_SOURCE_UNREADABLE` `retryable` (retryable)": class and retry word both."""
    text = format_failure(RETRYABLE, dag_id="d", task_id="t", run_id="r")
    assert "• *users*: `RIVET_PLAN_SOURCE_UNREADABLE` (retryable)" in text.splitlines(), text
    assert "`RIVET_SOURCE_CDC_LOG_GAP` `refusal` (not retryable)" in format_failure(REFUSAL, dag_id="d", task_id="t", run_id="r")
    assert "users: RIVET_PLAN_SOURCE_UNREADABLE (retryable)" in format_trace(RETRYABLE, limit=4000).splitlines()


def raised(message: str) -> BaseException:
    """An exception with a real traceback, as Airflow hands it to the callback."""
    try:
        raise RuntimeError(message)
    except RuntimeError as exc:
        return exc


@pytest.fixture
def planted() -> Any:
    """A value registered with Airflow's secrets masker, as a Connection's password is when a task reads it."""
    try:
        from airflow.sdk.log import mask_secret
    except ImportError:
        from airflow.utils.log.secrets_masker import mask_secret
    mask_secret(PLANTED)
    return PLANTED


def test_the_trace_holds_the_units_the_traceback_and_rivets_stderr_with_secrets_masked(tmp_path: Path, planted: str) -> None:
    stderr = tmp_path / "try1.apply.stderr.log"
    stderr.write_text(f"connecting to postgresql://u:{planted}@db/app\nerror: source unreadable\n")
    payload = {**RETRYABLE, "stderr_path": str(stderr)}
    text = format_trace(payload, exception=raised(f"rivet failed with {planted}"), limit=4000, log_url="http://af/log")
    assert text.splitlines()[0] == "users: RIVET_PLAN_SOURCE_UNREADABLE (retryable)"
    assert "--- Airflow traceback ---\nTraceback (most recent call last):" in text and "in raised" in text
    assert f"--- rivet stderr: {stderr} ---\n" in text and text.endswith("error: source unreadable")
    assert planted not in text and text.count("***") == 2, text
    assert "truncated" not in text


def test_a_long_trace_keeps_the_end_of_each_part_and_says_where_the_rest_is(tmp_path: Path) -> None:
    stderr = tmp_path / "s.log"
    stderr.write_text("".join(f"line {i}\n" for i in range(5000)) + "error: the last line\n")
    text = format_trace(
        {**RETRYABLE, "stderr_path": str(stderr)}, exception=raised("x" * 9000), limit=3000, log_url="http://af/l"
    )
    assert len(text) <= 3000
    assert text.endswith("error: the last line\n… truncated, full log: http://af/l"), text[-200:]
    assert "--- Airflow traceback ---\n… " in text and "characters cut …\nxxx" in text, "each part keeps its end"
    assert text.startswith("users: RIVET_PLAN_SOURCE_UNREADABLE (retryable)\n")


class Client:
    """slack_sdk's `WebClient` as the callback uses it: every call recorded, `chat_postMessage` answers a `ts`."""

    def __init__(self, upload_fails: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.upload_fails = upload_fails

    def chat_postMessage(self, **kwargs: Any) -> dict:
        self.calls.append(("chat_postMessage", kwargs))
        return {"ok": True, "channel": "C0123", "ts": f"1760612345.00010{len(self.calls)}"}

    def files_upload_v2(self, **kwargs: Any) -> dict:
        self.calls.append(("files_upload_v2", kwargs))
        if self.upload_fails:
            raise RuntimeError("missing_scope")
        return {"ok": True}


def fake_slack(monkeypatch: pytest.MonkeyPatch, client: Client | None = None, sent: list | None = None) -> None:
    """Replace both Slack hooks: the bot hook hands out `client`, the webhook hook records (conn_id, text)."""

    class SlackHook:
        def __init__(self, slack_conn_id: str, **kwargs: Any) -> None:
            assert slack_conn_id == "slack_bot"
            self.client = client

    class SlackWebhookHook:
        def __init__(self, slack_webhook_conn_id: str, **kwargs: Any) -> None:
            self.conn_id = slack_webhook_conn_id

        def send(self, text: str) -> Any:
            sent.append((self.conn_id, text))
            return types.SimpleNamespace(status_code=200, body="ok")

    for name, hook in ((BOT_MODULE, SlackHook), (WEBHOOK_MODULE, SlackWebhookHook)):
        module = types.ModuleType(name)
        setattr(module, hook.__name__, hook)
        monkeypatch.setitem(sys.modules, name, module)


class OneTI(FakeTI):
    """A task instance whose pull takes `map_indexes`, as Airflow's does, and that has a log URL."""

    log_url = "http://af/dags/d/runs/r/tasks/apply.users"

    def xcom_pull(self, task_ids: object = None, key: str = "return_value", map_indexes: object = None) -> object:
        return super().xcom_pull(task_ids=task_ids, key=key)


def failed_context(payload: dict, exception: BaseException | None = None) -> dict:
    ti = OneTI(task_id="apply.users")
    ti.dag_id = "d"
    ti.store[("apply.users", "return_value")] = payload
    return {"ti": ti, "run_id": "manual__1", "exception": exception}


def test_the_bot_posts_the_title_then_the_trace_as_a_code_block_in_its_thread(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    client = Client()
    fake_slack(monkeypatch, client)
    caplog.set_level(logging.INFO)
    slack_failure_callback("slack_bot", channel="C0123")(failed_context(RETRYABLE, raised("boom <x> & y")))
    (first, top), (second, reply) = client.calls
    assert first == second == "chat_postMessage"
    assert top["channel"] == "C0123" and top["text"] == "d.apply.users — RIVET_PLAN_SOURCE_UNREADABLE (retryable)"
    assert top["blocks"] == [
        {"type": "section", "text": {"type": "mrkdwn", "text": ":x: *d.apply.users — RIVET_PLAN_SOURCE_UNREADABLE (retryable)*"}},
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "run `manual__1` · <http://af/dags/d/runs/r/tasks/apply.users|task log>"}],
        },
    ]
    assert "thread_ts" not in top and reply["thread_ts"] == "1760612345.000101" and reply["channel"] == "C0123"
    text = reply["text"]
    assert text.startswith("```\nusers: RIVET_PLAN_SOURCE_UNREADABLE (retryable)\n") and text.endswith("\n```"), text
    assert "RuntimeError: boom &lt;x&gt; &amp; y" in text, "Slack's control characters are escaped"
    lines = [r.getMessage() for r in caplog.records if r.name == "airflow.task.rivet_slack"]
    assert lines == [
        "Slack alert for d.apply.users posted to channel C0123 (ts 1760612345.000101) through connection `slack_bot`;"
        " its trace is a message in the thread"
    ]


@pytest.mark.parametrize("upload_fails", [False, True])
def test_a_trace_longer_than_a_message_goes_to_the_thread_as_a_file_or_capped_when_the_upload_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, upload_fails: bool
) -> None:
    stderr = tmp_path / "s.log"
    stderr.write_text("".join(f"line {i}\n" for i in range(3000)) + "error: the last line\n")
    client = Client(upload_fails=upload_fails)
    fake_slack(monkeypatch, client)
    slack_failure_callback("slack_bot", channel="C0123")(failed_context({**RETRYABLE, "stderr_path": str(stderr)}))
    names = [name for name, _ in client.calls]
    if not upload_fails:
        assert names == ["chat_postMessage", "files_upload_v2"]
        upload = client.calls[1][1]
        assert upload["thread_ts"] == "1760612345.000101" and upload["channel"] == "C0123"
        assert upload["filename"] == "d.apply.users.trace.txt" and upload["content"].endswith("error: the last line")
        assert "line 0\n" in upload["content"], "the file holds the whole trace"
    else:
        assert names == ["chat_postMessage", "files_upload_v2", "chat_postMessage"]
        reply = client.calls[2][1]
        assert reply["thread_ts"] == "1760612345.000101" and len(reply["text"]) <= callbacks.THREAD_TEXT_LIMIT + 8
        assert reply["text"].endswith("… truncated, full log: http://af/dags/d/runs/r/tasks/apply.users\n```")


def test_the_webhook_fallback_sends_title_link_and_capped_trace_as_one_message(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, planted: str
) -> None:
    sent: list[tuple[str, str]] = []
    fake_slack(monkeypatch, sent=sent)
    caplog.set_level(logging.INFO)
    slack_failure_callback("slack_rivet")(failed_context(PREFLIGHT, raised(f"refused {planted}")))
    [(conn_id, text)] = sent
    head, link, fence, summary = text.split("\n")[:4]
    assert conn_id == "slack_rivet" and head == ":x: *d.apply.users — RIVET_AIRFLOW_EXPORT_UNKNOWN (final)*"
    assert link == "run `manual__1` · <http://af/dags/d/runs/r/tasks/apply.users|task log>"
    assert (fence, summary) == ("```", "refused before rivet started: RIVET_AIRFLOW_EXPORT_UNKNOWN")
    assert text.endswith("```") and planted not in text and len(text) < 4000
    lines = [r.getMessage() for r in caplog.records if r.name == "airflow.task.rivet_slack"]
    assert lines == ["Slack alert for d.apply.users sent through connection `slack_rivet`: HTTP 200 ok"], lines


def test_the_readme_shows_the_title_and_thread_the_callback_writes() -> None:
    """The walkthrough's sample title and trace are the formatter's output, byte for byte, so they cannot drift."""
    readme = (ROOT / "README.md").read_text()
    for payload, task in ((PREFLIGHT, "apply.no_such_export"), (RETRYABLE, "apply.users")):
        title = format_title(payload, dag_id="rivet_slack_test", task_id=task)
        assert f"> :x: **{title}**\n" in readme, title
        assert f"```text\n{format_trace(payload, limit=4000)}\n" in readme


EXAMPLE_RUNNER = """\
import runpy, sys, types

class Hook:
    def __init__(self, slack_webhook_conn_id, **kwargs):
        self.conn_id = slack_webhook_conn_id
    def send(self, text):
        with open({sent!r}, "a") as out:
            out.write(self.conn_id + "\\n" + text + "\\n---\\n")
        return types.SimpleNamespace(status_code=200, body="ok")

module = types.ModuleType("airflow.providers.slack.hooks.slack_webhook")
module.SlackWebhookHook = Hook
sys.modules["airflow.providers.slack.hooks.slack_webhook"] = module
dag = runpy.run_path({example!r})[{name!r}]
"""


@pytest.mark.parametrize(
    ("name", "task", "title"),
    [
        ("rivet_slack_test_refused", "apply.no_such_export", "RIVET_AIRFLOW_EXPORT_UNKNOWN (final)"),
        ("rivet_slack_test_transient", "apply.users", "RIVET_PLAN_SOURCE_UNREADABLE (retryable)"),
    ],
)
def test_each_example_dag_fails_on_its_first_try_and_sends_one_alert_through_the_named_connection(
    dag_rig: DagRig, monkeypatch: pytest.MonkeyPatch, name: str, task: str, title: str
) -> None:
    unreadable = {"error": "connection refused", "exit_class": 2, "code": "RIVET_PLAN_SOURCE_UNREADABLE"}
    dag_rig.scenario(**{"plan:users": {"exit": 2, "line": unreadable}})
    monkeypatch.setenv("RIVET_EXAMPLE_STATE_DIR", str(dag_rig.state_dir))
    monkeypatch.setenv("RIVET_BIN", str(dag_rig.bin / "rivet"))
    monkeypatch.setenv("RIVET_SLACK_CONN_ID", "slack_test")
    monkeypatch.delenv("RIVET_SLACK_CHANNEL", raising=False)
    sent = dag_rig.tmp / "slack.txt"
    source = EXAMPLE_RUNNER.format(sent=str(sent), example=str(EXAMPLE), name=name)
    state, tasks = dag_rig.run_source(name, source)
    assert (tasks[task], tasks["watcher"], state) == ("failed", "failed", "failed"), tasks
    messages = [m for m in sent.read_text().split("\n---\n") if m.strip()]
    assert len(messages) == 1, "one alert per failed task, none for the watcher, no retry before it"
    conn_id, text = messages[0].split("\n", 1)
    assert conn_id == "slack_test" and text.startswith(f":x: *{name}.{task} — {title}*\n"), text
    assert "--- Airflow traceback ---" in text
