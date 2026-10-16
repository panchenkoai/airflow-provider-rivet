"""The ADR-0039 error object and unit result, and the one place a class is derived from an exit."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

CLASS_BY_EXIT = {
    1: "generic",
    2: "retryable",
    3: "data_integrity",
    4: "schema_drift",
    5: "refusal",
    6: "internal",
}
CLASSES = frozenset(CLASS_BY_EXIT.values()) | {"crashed"}
STOP_RANK = ("crashed", "generic", "retryable", "schema_drift", "refusal", "internal", "data_integrity")
STATUSES = frozenset({"success", "failed", "skipped", "loaded", "compacted"})
# rivet's refusals of a second holder of one lease (exit 5, refusal): their remedy is "wait for the holder to end, then
# repeat", which is what an Airflow retry does, so they are retried whatever the printed `retryable` says (G2).
BUSY_CODES = frozenset({"RIVET_STATE_RUN_IN_PROGRESS", "RIVET_LOAD_TABLE_BUSY"})


@dataclass(frozen=True)
class ErrorObject:
    """The D1 error object; `message` never leaves the worker."""

    code: str | None
    kind: str | None
    class_: str
    exit_code: int | None
    retryable: bool
    action: str | None
    message: str | None = None

    def to_xcom(self) -> dict[str, Any]:
        """The object as XCom may carry it: every key but `message`."""
        return {
            "code": self.code,
            "kind": self.kind,
            "class": self.class_,
            "exit_code": self.exit_code,
            "retryable": self.retryable,
            "action": self.action,
        }

    @classmethod
    def from_contract(cls, obj: Any, message_key: str = "message") -> ErrorObject | None:
        """Read an object rivet printed with the contract's keys; `None` when it is not one."""
        if not isinstance(obj, dict):
            return None
        class_, retryable, exit_code = obj.get("class"), obj.get("retryable"), obj.get("exit_code")
        texts = [obj.get(key) for key in ("code", "kind", "action", message_key)]
        if not isinstance(class_, str) or class_ not in CLASSES or not isinstance(retryable, bool):
            return None
        if exit_code is not None and (type(exit_code) is not int or exit_code not in CLASS_BY_EXIT):
            return None
        if any(text is not None and not isinstance(text, str) for text in texts):
            return None
        return cls(texts[0], texts[1], class_, exit_code, retryable or texts[0] in BUSY_CODES, texts[2], texts[3])

    def describe(self, export: str | None) -> str:
        """The exception text: `[code] class: action (export)`, with no error message in it."""
        code = f"[{self.code}] " if self.code else ""
        action = f": {self.action}" if self.action else ""
        unit = f" ({export})" if export else ""
        return f"{code}{self.class_}{action}{unit}"


def error_from_exit(exit_status: int | None, signal: int | None) -> ErrorObject:
    """Build the object for a process that printed none, by ADR-0039 D8's table and nothing else."""
    if signal is not None:
        return ErrorObject(None, None, "crashed", None, True, None, f"rivet was killed by signal {signal}")
    raw = f"rivet exited {exit_status} and printed no error object"
    if exit_status in (1, 3, 4, 5, 6):
        return ErrorObject(None, None, CLASS_BY_EXIT[exit_status], exit_status, False, None, None)
    if exit_status == 101:
        return ErrorObject(None, None, "internal", 6, False, None, raw)
    if exit_status is not None and 129 <= exit_status <= 255:
        return ErrorObject(None, None, "crashed", None, True, None, raw)
    return ErrorObject(None, None, "generic", 1, False, None, raw)


def error_from_line(line: dict[str, Any], exit_status: int | None = None) -> tuple[ErrorObject | None, bool]:
    """Read the `--json-errors` line: (object or `None` when malformed, whether it was derived from `exit_class`)."""
    if "class" in line or "retryable" in line:
        return ErrorObject.from_contract(line, message_key="error"), False
    known, code, text = line.get("exit_class"), line.get("code"), line.get("error")
    if type(known) is not int or known not in CLASS_BY_EXIT or exit_status not in (None, known):
        return None, True
    if any(value is not None and not isinstance(value, str) for value in (code, text)):
        return None, True
    return ErrorObject(code, None, CLASS_BY_EXIT[known], known, known == 2 or code in BUSY_CODES, None, text), True


def worst(errors: list[ErrorObject]) -> ErrorObject:
    """The failure that decides a task: the highest `stop_rank` (ADR-0039 D2), the last among equals."""
    return max(reversed(errors), key=lambda error: STOP_RANK.index(error.class_))


@dataclass
class UnitResult:
    """One unit a scheduler reports: a run / apply export, or a load / compact table."""

    export: str | None
    table: str | None = None
    status: str = "success"
    run_id: str | None = None
    rows: int | None = None
    files: int | None = None
    stop_reason: str | None = None
    error: ErrorObject | None = None
    skip_reason: str | None = field(default=None, compare=False)

    def to_xcom(self) -> dict[str, Any]:
        """The contract's XCom unit shape (tests/fixtures/scheduler/xcom_unit*.json)."""
        return {
            "export": self.export,
            "table": self.table,
            "status": self.status,
            "run_id": self.run_id,
            "rows": self.rows,
            "files": self.files,
            "stop_reason": self.stop_reason,
            "error": self.error.to_xcom() if self.error else None,
        }
