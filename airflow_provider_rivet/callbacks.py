"""Failure alerting through Airflow: a one-line Slack title per failed task, and its error text in a thread.

With a bot token (`SlackHook`, a `slack` Connection) the channel gets the title, the run id and the task-log link; the
error text (rivet's error line of the failed step, else the exception's message; redacted, capped) is a reply in that
message's thread. An incoming webhook (`SlackWebhookHook`) cannot thread: it posts title and error text as one message.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .operators import XCOM_KEY

MAX_UNITS = 10
# the thread's code block; a webhook message carries title and link too, and stays under Slack's 4000
THREAD_TEXT_LIMIT = 3500
WEBHOOK_TEXT_LIMIT = 3000
# how much of the end of rivet's stderr file is read for its error lines
STDERR_TAIL = 65_536
# a child of Airflow's task logger: the line lands in the task log on Airflow 2 and 3 alike
log = logging.getLogger("airflow.task.rivet_slack")


def _failed_units(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """The units that carry an error; the process-level error when no unit does."""
    failed = [u for u in payload.get("units") or [] if u.get("error")]
    if not failed and payload.get("error"):
        failed = [{"error": payload["error"]}]  # no export, no table: read as `(process)`
    return failed


def _retry_word(error: Mapping[str, Any]) -> str:
    return "retryable" if error.get("retryable") else "not retryable"


def _class_and_retry(error: Mapping[str, Any]) -> str:
    """`class` (retryable); the class alone when it is the retry word itself (`retryable` (retryable) said it twice)."""
    cls, retry = error.get("class"), _retry_word(error)
    if not cls or cls == retry:
        return f"({retry})"
    return f"`{cls}` ({retry})"


def format_failure(payload: Mapping[str, Any] | None, *, dag_id: str, task_id: str, run_id: str, log_url: str = "") -> str:
    """A Slack message from the XCom payload: code, class, action and unit; never rivet's error text."""
    lines = [f":x: *{dag_id}.{task_id}* failed (run `{run_id}`)"]
    payload = payload or {}
    if payload.get("preflight_refusal"):
        lines.append(f"refused before rivet started: `{payload['preflight_refusal']}`")
    failed = _failed_units(payload)
    for unit in failed[:MAX_UNITS]:
        error = unit["error"]
        name = unit.get("export") or "(process)"
        if unit.get("table"):
            name += f" / {unit['table']}"
        code = f"`{error['code']}` " if error.get("code") else ""
        lines.append(f"• *{name}*: {code}{_class_and_retry(error)}")
        if error.get("action"):
            lines.append(f"    action: {error['action']}")
    if len(failed) > MAX_UNITS:
        lines.append(f"… and {len(failed) - MAX_UNITS} more failed units")
    if not payload:
        lines.append("no rivet result was published for this try (the task died outside the operator)")
    if payload.get("stderr_path"):
        lines.append(f"stderr on the worker: `{payload['stderr_path']}`")
    if log_url:
        lines.append(f"<{log_url}|task log>")
    return "\n".join(lines)


def format_title(payload: Mapping[str, Any] | None, *, dag_id: str, task_id: str, exception: Any = None) -> str:
    """`dag.task — <error type>`: the rivet code and final / retryable, else the exception's class."""
    payload = payload or {}
    if payload.get("preflight_refusal"):
        kind = f"{payload['preflight_refusal']} (final)"
    elif failed := _failed_units(payload):
        error = failed[0]["error"]
        name = error.get("code") or error.get("class") or "rivet failed"
        kind = f"{name} ({'retryable' if error.get('retryable') else 'final'})"
        if len(failed) > 1:
            kind += f" +{len(failed) - 1} more"
    elif isinstance(exception, BaseException):
        kind = type(exception).__name__
    else:
        kind = "failed, no rivet result"
    return f"{dag_id}.{task_id} — {kind}"


def _redact(text: str) -> str:
    """Airflow's secrets masker over the text: every Connection password and masked value this task process saw."""
    try:  # Airflow 3.3+; 3.0-3.2; 2.x
        from airflow.sdk._shared.secrets_masker import redact
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import redact
        except ImportError:
            from airflow.utils.log.secrets_masker import redact
    return str(redact(text))


def _rivet_errors(payload: Mapping[str, Any]) -> list[str]:
    """rivet's error lines in the failed step's stderr file: its `--json-errors` objects, else `Error:` lines.

    Only the end of the file is read: the error is the last thing rivet writes.
    """
    path = payload.get("stderr_path")
    if not path:
        return []
    try:
        with open(path, "rb") as f:
            f.seek(max(os.path.getsize(path) - STDERR_TAIL, 0))
            lines = f.read().decode(errors="replace").splitlines()  # bytes decode as UTF-8
    except OSError:
        return []
    found, plain = [], []
    for line in lines:
        obj = _json_object(line)
        if obj:
            text = obj.get("error") if isinstance(obj.get("error"), str) else obj.get("message")
            if isinstance(text, str) and text:
                code = obj.get("code")
                found.append(f"[{code}] {text}" if isinstance(code, str) and code and code not in text else text)
        elif line.lstrip().lower().startswith("error:"):
            plain.append(line.strip())
    return found or plain  # the human `Error:` line repeats the object when both are printed


def _json_object(line: str) -> dict[str, Any]:
    """The JSON object a line holds; empty for any other line."""
    try:
        obj = json.loads(line)
    except ValueError:
        return {}
    return obj if isinstance(obj, dict) else {}


def format_error_text(payload: Mapping[str, Any] | None, *, exception: Any = None, limit: int, log_url: str = "") -> str:
    """The thread's text: rivet's error line(s) of the failed step, else the exception's message; redacted, capped.

    No traceback and no other stderr. When the text is cut, the last line says so and points at the task log.
    """
    lines = _rivet_errors(payload or {})
    if not lines and exception is not None:
        lines = [str(exception) or type(exception).__name__]
    text = _redact("\n".join(lines) or "no error text: the task failed without a rivet result or an exception")
    if len(text) <= limit:
        return text
    tail = f"\n… truncated, full log: {log_url or 'the task log'}"
    return text[: max(limit - len(tail), 0)] + tail


def _mrkdwn(text: str) -> str:
    """Slack's three control characters escaped, and no fence the text could close early."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("```", "'''")


def _code_block(text: str) -> str:
    return f"```\n{_mrkdwn(text)}\n```"


@dataclass(frozen=True)
class _Alert:
    """One failed task instance, ready to post."""

    dag_id: str
    task: str  # task id, `[map_index]` added for a mapped one
    run_id: str
    log_url: str
    payload: Mapping[str, Any] | None
    exception: Any

    @property
    def title(self) -> str:
        return _redact(format_title(self.payload, dag_id=self.dag_id, task_id=self.task, exception=self.exception))

    @property
    def context_line(self) -> str:
        link = f" · <{self.log_url}|task log>" if self.log_url else ""
        return f"run `{_mrkdwn(self.run_id)}`{link}"

    def error_text(self, limit: int) -> str:
        return format_error_text(self.payload, exception=self.exception, limit=limit, log_url=self.log_url)


def slack_failure_callback(
    conn_id: str = "slack_default", *, channel: str | None = None, **hook_kwargs: Any
) -> Callable[[Any], None]:
    """An `on_failure_callback` that posts a failed rivet task to Slack.

    With `channel` (a channel id, `C…`): `conn_id` is a `slack` Connection holding a bot token (scope `chat:write`);
    the title goes to the channel and the error text into its thread. Without it: `conn_id` is a `slackwebhook`
    Connection and title and error text go out as one message.
    """

    def notify(context: Any) -> None:
        """Format this task instance's rivet result and send it."""
        ti = context["ti"]
        map_index = getattr(ti, "map_index", -1)
        alert = _Alert(
            dag_id=ti.dag_id,
            task=ti.task_id + (f"[{map_index}]" if isinstance(map_index, int) and map_index >= 0 else ""),
            run_id=str(context.get("run_id") or getattr(ti, "run_id", "")),
            log_url=getattr(ti, "log_url", None) or "",
            payload=_payload_of(ti, map_index),
            exception=context.get("exception"),
        )
        if channel:
            _post_threaded(alert, conn_id, channel, hook_kwargs)
        else:
            _post_webhook(alert, conn_id, hook_kwargs)

    return notify


def _payload_of(ti: Any, map_index: Any) -> dict[str, Any] | None:
    """This task instance's rivet result, or None when there is none to read."""
    try:
        # this index's own payload: without `map_indexes` a mapped task's pull returns every index's (D11)
        payload = ti.xcom_pull(task_ids=ti.task_id, key=XCOM_KEY, map_indexes=map_index)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _post_webhook(alert: _Alert, conn_id: str, hook_kwargs: Mapping[str, Any]) -> None:
    """Title, context and the error text as one message: a webhook returns no `ts` to thread under."""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

    text = f":x: *{_mrkdwn(alert.title)}*\n{alert.context_line}\n{_code_block(alert.error_text(WEBHOOK_TEXT_LIMIT))}"
    # the hook raises on any answer but 200; the line says the alert was posted, and what Slack answered
    response = SlackWebhookHook(slack_webhook_conn_id=conn_id, **hook_kwargs).send(text=text)
    status = getattr(response, "status_code", None)
    answer = f"HTTP {status} {getattr(response, 'body', '')}".rstrip() if status is not None else "no response object"
    log.info("Slack alert for %s.%s sent through connection `%s`: %s", alert.dag_id, alert.task, conn_id, answer)


def _post_threaded(alert: _Alert, conn_id: str, channel: str, hook_kwargs: Mapping[str, Any]) -> None:
    """The title to the channel, then the error text as a code block in its thread."""
    from airflow.providers.slack.hooks.slack import SlackHook

    client = SlackHook(slack_conn_id=conn_id, **hook_kwargs).client
    title = alert.title
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f":x: *{_mrkdwn(title)}*"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": alert.context_line}]},
    ]
    # slack_sdk raises SlackApiError on any `ok: false`: reaching the next line means Slack accepted the message
    top = client.chat_postMessage(channel=channel, text=title, blocks=blocks)
    ts, where = top["ts"], top.get("channel") or channel
    client.chat_postMessage(channel=where, thread_ts=ts, text=_code_block(alert.error_text(THREAD_TEXT_LIMIT)))
    log.info(
        "Slack alert for %s.%s posted to channel %s (ts %s) through connection `%s`, its error text in the thread",
        alert.dag_id,
        alert.task,
        where,
        ts,
        conn_id,
    )
