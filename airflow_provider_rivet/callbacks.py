"""Failure alerting through Airflow: a one-line Slack title per failed task, and its full trace in a thread.

With a bot token (`SlackHook`, a `slack` Connection) the channel gets the title, the run id and the task-log link; the
trace (the Airflow traceback and rivet's stderr of the failed step, redacted, size-capped) is a reply in that message's
thread. An incoming webhook (`SlackWebhookHook`) cannot thread: it posts the title and a capped trace as one message.
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .operators import XCOM_KEY

MAX_UNITS = 10
# a reply holds the trace as a code block up to this size; a longer one becomes a file snippet in the thread
THREAD_TEXT_LIMIT = 3500
# a webhook message has no thread and no file: title, context and the trace together stay under Slack's 4000
WEBHOOK_TRACE_LIMIT = 3000
# the snippet file, and how much of rivet's stderr file is read (its end: the failure is the last thing it wrote)
FILE_LIMIT = 200_000
# a child of Airflow's task logger: the line lands in the task log on Airflow 2 and 3 alike
log = logging.getLogger("airflow.task.rivet_slack")


def _failed_units(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """The units that carry an error; the process-level error when no unit does."""
    failed = [u for u in payload.get("units") or [] if u.get("error")]
    if not failed and payload.get("error"):
        failed = [{"export": None, "table": None, "error": payload["error"]}]
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


def _tail(text: str, budget: int) -> str:
    """The end of a text in at most `budget` characters, saying how much was cut from its start."""
    if len(text) <= budget:
        return text
    marker = f"… {len(text) - budget} characters cut …\n"
    return marker + text[len(text) - max(budget - len(marker), 0) :]


def _stderr_of(payload: Mapping[str, Any]) -> tuple[str, str, bool] | None:
    """(path, text, whether its start was not read) of the failed step's stderr file; None when there is none."""
    path = payload.get("stderr_path")
    if not path:
        return None
    try:
        with open(path, "rb") as f:
            size = f.seek(0, 2)
            f.seek(max(size - FILE_LIMIT, 0))
            data = f.read()
    except OSError as exc:
        return path, f"(not readable on the worker: {type(exc).__name__})", False
    return path, data.decode("utf-8", errors="replace").rstrip(), size > FILE_LIMIT


def _exception_text(exception: Any) -> str:
    if isinstance(exception, BaseException):
        return "".join(traceback.format_exception(type(exception), exception, exception.__traceback__)).rstrip()
    return str(exception).rstrip() if exception else ""


def _summary_lines(payload: Mapping[str, Any]) -> list[str]:
    """The failed units in plain text (the trace is a code block: no mrkdwn in it)."""
    if not payload:
        return ["no rivet result was published for this try (the task died outside the operator)"]
    lines = []
    if payload.get("preflight_refusal"):
        lines.append(f"refused before rivet started: {payload['preflight_refusal']}")
    for unit in _failed_units(payload)[:MAX_UNITS]:
        error = unit["error"]
        name = (unit.get("export") or "(process)") + (f" / {unit['table']}" if unit.get("table") else "")
        cls, retry = error.get("class"), _retry_word(error)
        lines.append(f"{name}: {error.get('code') or '-'} {cls + ' ' if cls and cls != retry else ''}({retry})")
        if error.get("action"):
            lines.append(f"  action: {error['action']}")
    return lines


def format_trace(payload: Mapping[str, Any] | None, *, exception: Any = None, limit: int, log_url: str = "") -> str:
    """The thread's text: the failed units, the Airflow traceback, rivet's stderr; redacted, at most `limit` characters.

    The traceback gets a third of the room when there is a stderr file; each keeps its end. When anything is cut the
    last line says so and points at the task log.
    """
    payload = payload or {}
    tail = f"\n… truncated, full log: {log_url or 'the task log'}"
    parts = ["\n".join(_summary_lines(payload))]
    room = limit - len(tail) - len(parts[0]) - 2
    tb, stderr = _exception_text(exception), _stderr_of(payload)
    cut = bool(stderr and stderr[2])
    if tb:
        label = "--- Airflow traceback ---\n"
        budget = max((room // 3 if stderr else room) - len(label), 0)
        cut |= len(tb) > budget
        parts.append(label + _tail(tb, budget))
        room -= len(parts[-1]) + 2
    if stderr:
        label = f"--- rivet stderr: {stderr[0]} ---\n"
        budget = max(room - len(label), 0)
        cut |= len(stderr[1]) > budget
        parts.append(label + _tail(stderr[1], budget))
    text = _redact("\n\n".join(parts))
    if len(text) > limit - len(tail):
        text, cut = text[: max(limit - len(tail), 0)], True
    return text + tail if cut else text


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

    def trace(self, limit: int) -> str:
        return format_trace(self.payload, exception=self.exception, limit=limit, log_url=self.log_url)


def slack_failure_callback(
    conn_id: str = "slack_default", *, channel: str | None = None, **hook_kwargs: Any
) -> Callable[[Any], None]:
    """An `on_failure_callback` that posts a failed rivet task to Slack.

    With `channel` (a channel id, `C…`): `conn_id` is a `slack` Connection holding a bot token (scopes `chat:write`,
    `files:write`); the title goes to the channel and the trace into its thread. Without it: `conn_id` is a
    `slackwebhook` Connection and title and trace go out as one message.
    """

    def notify(context: Any) -> None:
        """Format this task instance's rivet result and send it."""
        ti = context["ti"]
        map_index = getattr(ti, "map_index", -1)
        try:
            # this index's own payload: without `map_indexes` a mapped task's pull returns every index's (D11)
            payload = ti.xcom_pull(task_ids=ti.task_id, key=XCOM_KEY, map_indexes=map_index)
        except Exception:
            payload = None
        alert = _Alert(
            dag_id=ti.dag_id,
            task=ti.task_id + (f"[{map_index}]" if isinstance(map_index, int) and map_index >= 0 else ""),
            run_id=str(context.get("run_id") or getattr(ti, "run_id", "")),
            log_url=getattr(ti, "log_url", "") or "",
            payload=payload if isinstance(payload, dict) else None,
            exception=context.get("exception"),
        )
        if channel:
            _post_threaded(alert, conn_id, channel, hook_kwargs)
        else:
            _post_webhook(alert, conn_id, hook_kwargs)

    return notify


def _post_webhook(alert: _Alert, conn_id: str, hook_kwargs: Mapping[str, Any]) -> None:
    """Title, context and the capped trace as one message: a webhook returns no `ts` to thread under."""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

    text = f":x: *{_mrkdwn(alert.title)}*\n{alert.context_line}\n{_code_block(alert.trace(WEBHOOK_TRACE_LIMIT))}"
    # the hook raises on any answer but 200; the line says the alert was posted, and what Slack answered
    response = SlackWebhookHook(slack_webhook_conn_id=conn_id, **hook_kwargs).send(text=text)
    status = getattr(response, "status_code", None)
    answer = f"HTTP {status} {getattr(response, 'body', '')}".rstrip() if status is not None else "no response object"
    log.info("Slack alert for %s.%s sent through connection `%s`: %s", alert.dag_id, alert.task, conn_id, answer)


def _post_threaded(alert: _Alert, conn_id: str, channel: str, hook_kwargs: Mapping[str, Any]) -> None:
    """The title to the channel, then the trace as a reply in its thread: a code block, or a file when it is long."""
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
    trace, how = alert.trace(FILE_LIMIT), "message"
    if len(trace) > THREAD_TEXT_LIMIT:
        try:
            client.files_upload_v2(
                channel=where,
                thread_ts=ts,
                content=trace,
                filename=f"{alert.dag_id}.{alert.task}.trace.txt",
                title=f"{title}: trace",
            )
            how = "file"
        except Exception as exc:  # no `files:write`, or uploads disabled: the capped trace still reaches the thread
            log.warning("Slack trace file for %s.%s not uploaded (%s); posting it capped", alert.dag_id, alert.task, exc)
            trace = alert.trace(THREAD_TEXT_LIMIT)
    if how == "message":
        client.chat_postMessage(channel=where, thread_ts=ts, text=_code_block(trace))
    log.info(
        "Slack alert for %s.%s posted to channel %s (ts %s) through connection `%s`; its trace is a %s in the thread",
        alert.dag_id,
        alert.task,
        where,
        ts,
        conn_id,
        how,
    )
