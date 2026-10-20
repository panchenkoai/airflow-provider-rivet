"""The task log an operator reads: one headline with the outcome first, one line per rivet step, indented notes.

Every structured record (`rivet.start` with its argv, `rivet.unit`, `rivet.result`, ...) is still written, as one JSON
line at DEBUG; the readable lines are built from those records and the task's XCom payload when the task ends. A
successful task's INFO and WARNING lines never hold a word Airflow colours red (`[logging] color_log_error_keywords`,
`error,exception` by default): every word of them is the package's own, except names the DAG gives (export, table).
rivet's stderr text reaches the log only through `stream_stderr=True`, as before.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

from .capabilities import DEGRADATIONS
from .preflight import SETUP_GUIDE

clock = time.monotonic  # replaced by tests, so a golden log has fixed durations
Write = Callable[[int, str], None]

# What a degradation that is not a missing flag means, in words an operator reads. `state_url_sqlite` is the SQLite
# note; a missing flag is listed by its key under "this binary lacks".
NOTES = {
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
LACKS = sorted(key for key, d in DEGRADATIONS.items() if d.detect == "flag")
# A single step's line is shown only when the task took this much longer than the step (a lock wait, a slow preflight).
STEP_SLACK = 1.0
UNIT_WORD = {"load": "table", "compact": "table"}
STOP_WORD = {"refusal": "REFUSED", "crashed": "CRASHED"}


def _plural(count: int, word: str) -> str:
    return f"{count} {word}" if count == 1 else f"{count} {word}s"


def _secs(seconds: float) -> str:
    return f"{seconds:.1f} s"


def _counts(units: list[dict[str, Any]]) -> list[str]:
    """`30 rows`, `1 file`: the sums of what the units report, a count no unit reports left out."""
    parts = []
    for key, word in (("rows", "row"), ("files", "file")):
        known = [u[key] for u in units if isinstance(u.get(key), int)]
        if known:
            parts.append(_plural(sum(known), word))
    return parts


def _target(record: dict[str, Any]) -> str | None:
    """`p.d.orders__changes`, or `p.d.orders__changes into p.d.orders` for a compact; `(from config)` when the config
    named them, not rivet."""
    target, buffer = record.get("target"), record.get("buffer")
    if not isinstance(target, str):
        return None
    text = f"{buffer} into {target}" if isinstance(buffer, str) else target
    return f"{text} (from config)" if record.get("target_from") == "config" else text


def _unit_name(unit: dict[str, Any]) -> str:
    return ".".join(str(part) for part in (unit.get("export"), unit.get("table")) if part) or "(config)"


class TaskLog:
    """One task's log: the records as they come (DEBUG), the readable block when the task ends."""

    def __init__(self, command: str, export: str | None, table: str | None, write: Write) -> None:
        self.command, self.export, self.table, self.write = command, export, table, write
        self.started = clock()
        self.events: list[dict[str, Any]] = []
        self.steps: list[dict[str, Any]] = []

    def record(self, record: dict[str, Any]) -> None:
        """Keep a structured record and write it at DEBUG; a wait or a streamed stderr line is shown now."""
        self.write(logging.DEBUG, json.dumps(record, sort_keys=True, default=str))
        self.events.append(record)
        event = record.get("event")
        if event == "rivet.start":
            self.steps.append({"step": record.get("step"), "start": clock()})
        elif event == "rivet.step" and self.steps:
            self.steps[-1].update(end=clock(), exit_status=record.get("exit_status"), signal=record.get("signal"))
        elif event == "rivet.lock.wait":
            self.write(
                logging.INFO,
                f"waiting up to {record.get('lock_wait')} s for the lock of {record.get('unit')}: another try of this "
                "task, or a rivet process it left running, holds it",
            )
        elif event == "rivet.stderr":
            self.write(logging.INFO, f"rivet {record.get('step')} stderr: {record.get('line')}")

    def finish(self, payload: dict[str, Any], try_number: int, max_tries: int, stop_requested: bool) -> None:
        """Write the headline, the step lines and the notes of the ended task."""
        for level, text in self.render(payload, try_number, max_tries, stop_requested):
            self.write(level, text)

    def _first(self, event: str) -> dict[str, Any] | None:
        return next((e for e in self.events if e.get("event") == event), None)

    def render(self, payload: dict[str, Any], try_number: int, max_tries: int, stop_requested: bool) -> list[tuple[int, str]]:
        """The readable block: `[(level, line)]`, headline first."""
        seconds = clock() - self.started
        subject = " ".join(p for p in (self.command.replace("cdc_", ""), self.export, self.table and f"table {self.table}") if p)
        head = f"rivet {subject} →"
        targets = [_target(e) for e in self.events if e.get("event") == "rivet.unit"]
        if len(targets) == 1 and targets[0]:
            head += f" {targets[0]}:"
        tries = f"try {try_number} of {max_tries + 1}"
        will_retry = try_number <= max_tries
        retry_text = f"Airflow retries it ({tries})" if will_retry else f"no try left ({tries})"
        # A failure Airflow will not try again is the task's end: ERROR, also when rivet called it retryable.
        retry_level = logging.WARNING if will_retry else logging.ERROR
        units = [u for u in payload.get("units") or [] if isinstance(u, dict)]
        error = payload.get("error") or {}
        lines: list[tuple[int, str]] = []
        if "preflight_refusal" in payload:
            refused = self._first("rivet.refused") or {}
            lines.append((logging.ERROR, f"{head} REFUSED [{payload['preflight_refusal']}] before rivet started, final"))
            lines.append((logging.ERROR, f"  why      {refused.get('text')}"))
        elif "stopped_before" in payload:
            step = payload["stopped_before"]
            lines.append((logging.WARNING, f"{head} STOPPED by Airflow before rivet {step} started; rivet {step} was not run"))
        elif "busy" in payload:
            wait = self._first("rivet.lock.wait") or {}
            lines.append(
                (
                    retry_level,
                    f"{head} BUSY [{payload['busy']}]: the lock of {wait.get('unit')} stayed held for "
                    f"{wait.get('lock_wait')} s; rivet was not started, {retry_text}",
                )
            )
        elif payload.get("decision") == "success":
            lines.append((logging.INFO, self._success(head, units, payload, seconds)))
        else:
            lines.append(self._failure(head, error, payload, units, retry_text, retry_level, stop_requested))
        lines += self._step_lines(seconds)
        if len(units) > 1 or (payload.get("decision") != "success" and units and units[0].get("status") == "failed"):
            lines += self._unit_lines(units)
        if payload.get("decision") not in (None, "success") and payload.get("stderr_path"):
            lines.append((logging.INFO, f"  stderr   {payload['stderr_path']}"))
        lines += self._notes(payload)
        return lines

    def _success(self, head: str, units: list[dict[str, Any]], payload: dict[str, Any], seconds: float) -> str:
        """`OK: 30 rows, 1 file in 4.1 s (run X)`, or SKIPPED when nothing was done."""
        runs = sorted({u["run_id"] for u in units if isinstance(u.get("run_id"), str)})
        run = f" (run {runs[0]})" if len(runs) == 1 else ""
        counts = _counts(units)
        if len(units) > 1:
            counts.insert(0, _plural(len(units), UNIT_WORD.get(self.command, "export")))
        done = self._first("rivet.apply.already_applied")
        if done is not None:
            what = ", ".join(counts)
            return (
                f"{head} SKIPPED: an earlier try already applied this plan{': ' + what if what else ''}; not applied again{run}"
            )
        if units and all(u.get("status") == "skipped" for u in units):
            reason = payload.get("skip_reason")
            return f"{head} SKIPPED: nothing to do{f' ({reason})' if reason else ''} in {_secs(seconds)}{run}"
        what = f": {', '.join(counts)}" if counts else ""
        return f"{head} OK{what} in {_secs(seconds)}{run}"

    def _failure(
        self,
        head: str,
        error: dict[str, Any],
        payload: dict[str, Any],
        units: list[dict[str, Any]],
        retry_text: str,
        retry_level: int,
        stop_requested: bool,
    ) -> tuple[int, str]:
        """`REFUSED [CODE], final, action: ...`; a retried failure says it is retried."""
        class_ = error.get("class")
        if payload.get("decision") == "retry_crashed":
            how = f"signal {payload['signal']}" if payload.get("signal") is not None else f"exit {payload.get('exit_status')}"
            if stop_requested:
                return logging.WARNING, f"{head} STOPPED by Airflow: rivet ended by {how}"
            if payload.get("crashed_retry"):
                return retry_level, f"{head} CRASHED: rivet ended by {how}; {retry_text} under the crash budget"
            return logging.ERROR, f"{head} CRASHED: rivet ended by {how}, final: the crash budget is used up"
        word = STOP_WORD.get(class_, "FAILED")
        parts = [word + (f" [{error['code']}]" if error.get("code") else "")]
        if word == "FAILED" and class_ != "retryable":
            parts[0] += f" {class_}"
        retried = payload.get("decision") == "retry"
        parts.append(f"retryable, {retry_text}" if retried else "final")
        if error.get("action"):
            parts.append(f"action: {error['action']}")
        failed = [u for u in units if u.get("status") == "failed"]
        runs = [u["run_id"] for u in failed if isinstance(u.get("run_id"), str)]
        run = f" (run {runs[0]})" if len(runs) == 1 else ""
        return (retry_level if retried else logging.ERROR), f"{head} {', '.join(parts)}{run}"

    def _step_lines(self, seconds: float) -> list[tuple[int, str]]:
        """One line per rivet process the task started, and one for a sealed plan an earlier try made; none for a task
        whose one step just ended `done` in about the headline's time, which the headline already says."""
        lines = []
        names = [s["step"] for s in self.steps]
        replayed = self.command == "apply" and "plan" not in names and "apply" in names
        sealed = self._first("rivet.plan.sealed") or {}
        for step in self.steps:
            name = step["step"]
            if replayed and name == "apply":
                lines.append((logging.INFO, f"  {'plan':<8} replayed the sealed plan of an earlier try"))
            if "end" not in step:
                lines.append((logging.INFO, f"  {name:<8} did not end"))
                continue
            took = f" in {_secs(step['end'] - step['start'])}"
            if step.get("signal") is not None:
                text = f"ended by signal {step['signal']}{took}"
            elif step.get("exit_status") != 0:
                text = f"exit {step.get('exit_status')}{took}"
            elif name == "plan" and self.command == "apply":
                strategy = sealed.get("strategy")
                text = f"sealed{f', strategy {strategy}' if isinstance(strategy, str) else ''}{took}"
            elif name == "plan":
                text = f"plan layout written{took}"
            else:
                text = f"done{took}"
            lines.append((logging.INFO, f"  {name:<8} {text}"))
        quiet = len(lines) == 1 and len(self.steps) == 1 and lines[0][1].split()[1] == "done"
        if quiet and seconds - (self.steps[0]["end"] - self.steps[0]["start"]) < STEP_SLACK:
            return []
        return lines

    def _unit_lines(self, units: list[dict[str, Any]]) -> list[tuple[int, str]]:
        """One line per unit of a task with several, and the failed unit of a failed task."""
        lines = []
        targets = {(e.get("export"), e.get("table")): _target(e) for e in self.events if e.get("event") == "rivet.unit"}
        for unit in units:
            error = unit.get("error") or {}
            if unit.get("status") == "failed":
                code = f" [{error['code']}]" if error.get("code") else ""
                text = f"failed{code} {error.get('class')}"
            else:
                text = ", ".join([str(unit.get("status")), *_counts([unit])])
            run = f" (run {unit['run_id']})" if isinstance(unit.get("run_id"), str) else ""
            target = targets.get((unit.get("export"), unit.get("table")))
            name = f"{_unit_name(unit)}{f' → {target}' if target else ''}"
            lines.append((logging.INFO, f"  {UNIT_WORD.get(self.command, 'export'):<8} {name}: {text}{run}"))
        return lines

    def _notes(self, payload: dict[str, Any]) -> list[tuple[int, str]]:
        """What the operator should know: missing flags, single-host state, workarounds; WARNING when they must act."""
        degraded = [d for d in payload.get("degraded") or [] if isinstance(d, str)]
        warnings = [e for e in self.events if e.get("event") == "rivet.warning"]
        notes: list[tuple[int, str]] = []
        lacks = [d for d in degraded if d in LACKS]
        if lacks:
            # Flags no released rivet may have yet: nothing for the operator to do, so not on every task's INFO (#62).
            notes.append((logging.DEBUG, f"this binary lacks: {', '.join(lacks)} (the operator works around them)"))
        if "state_url_sqlite" in degraded or any(w.get("warning") == "sqlite_single_host" for w in warnings):
            notes.append((logging.INFO, f"SQLite state is single-host — see {SETUP_GUIDE}"))
        for key in degraded:
            if key not in LACKS and NOTES.get(key, key) is not None:
                notes.append((logging.INFO, NOTES.get(key) or f"degraded: {key}"))
        for warning in warnings:
            if warning.get("warning") != "sqlite_single_host":
                notes.append((logging.WARNING, str(warning.get("text"))))
        stops = self._first("rivet.cdc.max_events")
        if stops is not None:
            names = ", ".join(str(n) for n in stops.get("exports") or [])
            notes.append((logging.INFO, f"stopped at max_events ({names}): a backlog remains; the next run goes on from here"))
        crashed = self._first("rivet.crashed") or {}
        if crashed.get("ledger_problem"):
            notes.append((logging.WARNING, str(crashed["ledger_problem"])))
        return [(level, f"  {'note':<8} {text}") for level, text in notes]
