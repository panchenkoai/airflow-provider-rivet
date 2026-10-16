"""Slack alerts: the title, the error text in its thread, the webhook fallback, the setup guide and the example DAGs."""

from __future__ import annotations

import logging
import re
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import pytest
from conftest import DagRig, FakeTI

from airflow_provider_rivet.callbacks import format_error_text, format_failure, format_title, slack_failure_callback

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


# rivet 04c4a712's `--json-errors` line for a source nobody listens on (a stand's plan.stderr.log, 2026-10-16)
UNREADABLE_LINE = (
    '{"error":"plan \'users\': the source could not be read, so no plan was written (`rivet apply` would fail on it): '
    "nothing is listening on 127.0.0.1:1 — check the port in `url:` and that the server is up (driver: error connecting "
    'to server: Connection refused (os error 111))","exit_class":2,"code":"RIVET_PLAN_SOURCE_UNREADABLE"}'
)
UNREADABLE_TEXT = (
    "[RIVET_PLAN_SOURCE_UNREADABLE] plan 'users': the source could not be read, so no plan was written (`rivet apply` "
    "would fail on it): nothing is listening on 127.0.0.1:1 — check the port in `url:` and that the server is up "
    "(driver: error connecting to server: Connection refused (os error 111))"
)
REFUSED_EXCEPTION = "[RIVET_AIRFLOW_EXPORT_UNKNOWN] config /opt/rivet/configs/postgres_batch.yaml has no export `no_such_export`"


def stderr_file(tmp_path: Path, *lines: str) -> str:
    path = tmp_path / "try1.plan.stderr.log"
    path.write_text("".join(line + "\n" for line in lines))
    return str(path)


def test_the_thread_text_is_rivets_error_line_only_not_its_other_stderr_nor_the_traceback(tmp_path: Path) -> None:
    path = stderr_file(tmp_path, "warning: SQLite state is single-host", "  at some frame", UNREADABLE_LINE)
    text = format_error_text({**RETRYABLE, "stderr_path": path}, exception=raised("operator text"), limit=3500)
    assert text == UNREADABLE_TEXT


def test_a_human_error_line_is_taken_when_rivet_printed_no_object(tmp_path: Path) -> None:
    path = stderr_file(tmp_path, "warning: something", "Error: config file 'x.yaml': unknown field `exprots`")
    text = format_error_text({**RETRYABLE, "stderr_path": path}, limit=3500)
    assert text == "Error: config file 'x.yaml': unknown field `exprots`"


@pytest.mark.parametrize("payload", [PREFLIGHT, CRASHED, None, {**RETRYABLE, "stderr_path": "/nonexistent/stderr.log"}])
def test_without_a_rivet_error_line_the_thread_text_is_the_exception_message_not_its_traceback(payload: Any) -> None:
    text = format_error_text(payload, exception=raised(REFUSED_EXCEPTION), limit=3500)
    assert text == REFUSED_EXCEPTION


def test_the_error_text_is_redacted_and_capped_with_a_pointer_to_the_task_log(tmp_path: Path, planted: str) -> None:
    line = '{"error":"cannot convert value ' + planted + " " + "x" * 5000 + '","code":"RIVET_SOURCE_VALUE_UNREPRESENTABLE"}'
    text = format_error_text({**REFUSAL, "stderr_path": stderr_file(tmp_path, line)}, limit=3500, log_url="http://af/l")
    assert planted not in text and text.startswith("[RIVET_SOURCE_VALUE_UNREPRESENTABLE] cannot convert value *** xxx")
    assert len(text) == 3500 and text.endswith("x\n… truncated, full log: http://af/l")


class Client:
    """slack_sdk's `WebClient` as the callback uses it: every call recorded, `chat_postMessage` answers a `ts`."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def chat_postMessage(self, **kwargs: Any) -> dict:
        self.calls.append(("chat_postMessage", kwargs))
        return {"ok": True, "channel": "C0123", "ts": f"1760612345.00010{len(self.calls)}"}


def fake_slack(monkeypatch: pytest.MonkeyPatch, client: Client | None = None, sent: list | None = None) -> list[tuple]:
    """Replace both Slack hooks: the bot hook hands out `client`, the webhook hook records (conn_id, text).

    Returns every hook built, as (hook class name, its keyword arguments), so a test checks the connection id and the
    hook settings the callback passed (both hooks take keywords only)."""
    built: list[tuple[str, dict]] = []

    class SlackHook:
        def __init__(self, **kwargs: Any) -> None:
            built.append(("SlackHook", kwargs))
            self.client = client

    class SlackWebhookHook:
        def __init__(self, **kwargs: Any) -> None:
            built.append(("SlackWebhookHook", kwargs))
            self.conn_id = kwargs.get("slack_webhook_conn_id")

        def send(self, text: str) -> Any:
            sent.append((self.conn_id, text))
            return types.SimpleNamespace(status_code=200, body="ok")

    for name, hook in ((BOT_MODULE, SlackHook), (WEBHOOK_MODULE, SlackWebhookHook)):
        module = types.ModuleType(name)
        setattr(module, hook.__name__, hook)
        monkeypatch.setitem(sys.modules, name, module)
    return built


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


def test_the_bot_posts_the_title_then_the_error_text_as_a_code_block_in_its_thread(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    client = Client()
    built = fake_slack(monkeypatch, client)
    caplog.set_level(logging.INFO)
    payload = {**RETRYABLE, "stderr_path": stderr_file(tmp_path, UNREADABLE_LINE.replace("127.0.0.1:1", "<db> & co"))}
    slack_failure_callback("slack_bot", channel="C0123", timeout=7, proxy="http://proxy:3128")(
        failed_context(payload, raised("operator text"))
    )
    assert built == [("SlackHook", {"slack_conn_id": "slack_bot", "timeout": 7, "proxy": "http://proxy:3128"})]
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
    escaped = UNREADABLE_TEXT.replace("127.0.0.1:1", "&lt;db&gt; &amp; co")
    assert reply["text"] == f"```\n{escaped}\n```", "Slack's control characters are escaped, nothing else is in it"
    lines = [r.getMessage() for r in caplog.records if r.name == "airflow.task.rivet_slack"]
    assert lines == [
        "Slack alert for d.apply.users posted to channel C0123 (ts 1760612345.000101) through connection `slack_bot`,"
        " its error text in the thread"
    ]


def test_the_webhook_fallback_sends_title_link_and_error_text_as_one_message(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, planted: str
) -> None:
    sent: list[tuple[str, str]] = []
    built = fake_slack(monkeypatch, sent=sent)
    caplog.set_level(logging.INFO)
    slack_failure_callback("slack_rivet", timeout=5)(failed_context(PREFLIGHT, raised(f"refused {planted}")))
    assert built == [("SlackWebhookHook", {"slack_webhook_conn_id": "slack_rivet", "timeout": 5})]
    [(conn_id, text)] = sent
    assert conn_id == "slack_rivet"
    assert text.split("\n") == [
        ":x: *d.apply.users — RIVET_AIRFLOW_EXPORT_UNKNOWN (final)*",
        "run `manual__1` · <http://af/dags/d/runs/r/tasks/apply.users|task log>",
        "```",
        "refused ***",
        "```",
    ]
    lines = [r.getMessage() for r in caplog.records if r.name == "airflow.task.rivet_slack"]
    assert lines == ["Slack alert for d.apply.users sent through connection `slack_rivet`: HTTP 200 ok"], lines


GUIDE = ROOT / "docs" / "slack.md"


def test_the_guide_shows_the_title_and_thread_text_the_callback_writes(tmp_path: Path) -> None:
    """The guide's sample titles and thread texts are the formatter's output, byte for byte, so they cannot drift."""
    guide = GUIDE.read_text()
    retryable = {**RETRYABLE, "stderr_path": stderr_file(tmp_path, UNREADABLE_LINE)}
    for payload, task, exception in (
        (PREFLIGHT, "apply.no_such_export", raised(REFUSED_EXCEPTION)),
        (retryable, "apply.users", raised("[RIVET_PLAN_SOURCE_UNREADABLE] retryable (users)")),
    ):
        title = format_title(payload, dag_id="rivet_slack_test", task_id=task)
        assert f"> :x: **{title}**\n" in guide, title
        assert f"```text\n{format_error_text(payload, exception=exception, limit=3500)}\n```" in guide


def test_every_code_snippet_of_the_guide_parses_and_its_python_runs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The guide's commands are valid shell, and its Python builds what it says on this Airflow version."""
    blocks = re.findall(r"^```(\w+)\n(.*?)^```$", GUIDE.read_text(), re.M | re.S)
    kinds = [kind for kind, _ in blocks]
    assert kinds.count("python") >= 2 and kinds.count("bash") >= 4, kinds
    for kind, body in blocks:
        if kind == "bash":
            out = subprocess.run(["bash", "-n"], input=body, capture_output=True, text=True)
            assert out.returncode == 0, (body, out.stderr)
        elif kind == "python":
            namespace: dict = {}
            exec(compile(body, "docs/slack.md", "exec"), namespace)
            callbacks_ = [v for k, v in namespace.items() if k in ("dag", "run")]
            assert callbacks_, body


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
    # the runner plants its stub in sys.modules: put back whatever was there (the real hook) when the test ends
    monkeypatch.setitem(sys.modules, WEBHOOK_MODULE, sys.modules.get(WEBHOOK_MODULE))
    sent = dag_rig.tmp / "slack.txt"
    source = EXAMPLE_RUNNER.format(sent=str(sent), example=str(EXAMPLE), name=name)
    state, tasks = dag_rig.run_source(name, source)
    assert (tasks[task], tasks["watcher"], state) == ("failed", "failed", "failed"), tasks
    messages = [m for m in sent.read_text().split("\n---\n") if m.strip()]
    assert len(messages) == 1, "one alert per failed task, none for the watcher, no retry before it"
    conn_id, text = messages[0].split("\n", 1)
    assert conn_id == "slack_test" and text.startswith(f":x: *{name}.{task} — {title}*\n"), text
    assert text.endswith("\n```") and "Traceback" not in text, text


def test_the_fake_slack_hooks_have_the_shape_of_the_slack_providers_hooks() -> None:
    """The fakes above stand in for the real hooks: CI installs the provider under Airflow's constraints and checks them."""
    import inspect

    slack = pytest.importorskip("airflow.providers.slack.hooks.slack")
    webhook = pytest.importorskip("airflow.providers.slack.hooks.slack_webhook")
    from slack_sdk import WebClient

    # not a stub another test left in sys.modules (the example runner's did, until it was put back)
    assert slack.SlackHook.__module__ == BOT_MODULE and webhook.SlackWebhookHook.__module__ == WEBHOOK_MODULE
    assert "slack_conn_id" in inspect.signature(slack.SlackHook.__init__).parameters
    assert hasattr(slack.SlackHook, "client"), "the bot path takes the WebClient from `SlackHook.client`"
    assert {"channel", "text", "blocks", "thread_ts"} <= set(inspect.signature(WebClient.chat_postMessage).parameters)
    assert "slack_webhook_conn_id" in inspect.signature(webhook.SlackWebhookHook.__init__).parameters
    for hook in (slack.SlackHook, webhook.SlackWebhookHook):  # the hook settings the alert tests pass through
        assert {"timeout", "proxy"} <= set(inspect.signature(hook.__init__).parameters), hook
    assert "text" in inspect.signature(webhook.SlackWebhookHook.send).parameters
