"""Reading what one rivet process returned: its error line and result file, into units, recording each degradation.

Nothing here starts a process or touches the state directory: `invocation` runs rivet and hands over a finished `_Step`
and the result document; `Reading` judges them against ADR-0039 and keeps the degradations it met.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import _yaml
from .result import DIGEST_COUNTS, DIGEST_KEYS, STATUSES, ErrorObject, UnitResult, error_from_exit, error_from_line

# The `load:` keys that hold where a warehouse's tables are created, outermost first (rivet's src/config/load.rs).
TARGET_CONTAINER = {"bigquery": ("project", "dataset"), "snowflake": ("database", "schema"), "clickhouse": ("database",)}
DONE_WORD = {"load": "loaded", "compact": "compacted"}
# A failed unit of a process that exited 0 and named no error for it (`exit_zero_failed_unit`): what D8 builds for exit 0.
EXIT_ZERO = error_from_exit(0, None)


@dataclass
class _Step:
    """One finished rivet process."""

    exit_status: int | None
    signal: int | None
    line: dict[str, Any] | None
    broken: bool = False


def _json_of(raw: bytes) -> Any:
    """The JSON document of some bytes, `None` when they are not one (not UTF-8 included)."""
    try:
        return json.loads(raw)
    except ValueError:
        return None


def config_target(config: Mapping[str, Any], cmd: str, export: str | None, table: str | None) -> tuple[str | None, str | None]:
    """`(target, buffer)` as the config's `load:` block names them, in rivet's terms: a load writes the table of a `full`
    export and the `<table>__changes` log of any other; a compact merges that log (buffer) into the base (target).
    `(None, None)` when the block names no warehouse or no table is known; a part the block leaves to the warehouse's
    default (BigQuery's project) is left out."""
    load = config.get("load")
    if not table or not isinstance(load, Mapping):
        return None, None
    keys = TARGET_CONTAINER.get(_yaml.text_of(load.get("target")))
    if keys is None:
        return None, None
    base = ".".join([*(str(_yaml.text_of(load[k])) for k in keys if load.get(k) not in (None, "")), table])
    if cmd == "compact":
        return base, f"{base}__changes"
    entry = next(
        (e for e in config.get("exports") or [] if isinstance(e, Mapping) and _yaml.text_of(e.get("name")) == export), {}
    )
    return (base if _yaml.text_of(entry.get("mode")) == "full" else f"{base}__changes"), None


class Reading:
    """The reader of one task's rivet output: the config the task ran and the command, and the degradations recorded
    while reading (a set the session shares, since it records its own as well)."""

    def __init__(self, degraded: set[str], config: Mapping[str, Any], command: str) -> None:
        self.degraded = degraded
        self.config = config
        self.command = command

    def malformed(self, value: Any, kind: type) -> Any:
        """A value of a result file when it has the type the contract gives it, else `None`, recorded."""
        if value is None or type(value) is kind:
            return value
        self.degraded.add("result_malformed")
        return None

    def status(self, value: Any) -> str:
        """A unit's status word; anything outside the contract's sets counts as failed, recorded."""
        if isinstance(value, str) and value in STATUSES:
            return value
        self.degraded.add("result_malformed")
        return "failed"

    def classify(self, step: _Step) -> ErrorObject | None:
        """The process-level error object: read when rivet printed a well-formed one, built from the exit when not."""
        if step.signal is None and step.exit_status == 0:
            return None
        if step.signal is None and step.line is not None:
            error, derived = error_from_line(step.line, step.exit_status)
            if error is not None:
                if derived:
                    self.degraded.add("error_object")
                return error
            self.degraded.add("result_malformed")
        elif step.signal is None and step.broken:
            self.degraded.add("result_malformed")
        return error_from_exit(step.exit_status, step.signal)

    def result_file(self, path: Path, step: _Step) -> Any:
        """A `--summary-output` file rivet was asked for; on exit 0 an absent or non-JSON one is recorded (D18)."""
        try:
            raw: bytes | None = path.read_bytes()
        except OSError:
            raw = None
        doc = None if raw is None else _json_of(raw)
        if step.signal is None and step.exit_status == 0:
            if raw is None:
                self.degraded.add("result_missing")
            elif not isinstance(doc, dict):
                self.degraded.add("result_malformed")
        return doc

    def entries(self, doc: Any, key: str) -> list[dict[str, Any]]:
        """The unit entries of a result file; anything that is not a list of objects is dropped and recorded."""
        entries = doc.get(key) if isinstance(doc, dict) else None
        if entries is None:
            return []
        if not isinstance(entries, list) or not all(isinstance(e, dict) for e in entries):
            self.degraded.add("result_malformed")
        return [e for e in entries if isinstance(e, dict)] if isinstance(entries, list) else []

    def unit_error(self, step: _Step, error: ErrorObject | None, own: Any, export: str | None, table: str | None) -> ErrorObject:
        """A failed unit's object: its own, its `failures[]` entry, or the process-level one (recorded): rivet's printed
        object, else the exit's, else (exit 0) `EXIT_ZERO`."""
        failures = (step.line or {}).get("failures")
        candidates = [own] if own is not None else []
        for entry in failures if isinstance(failures, list) else []:
            if not isinstance(entry, dict):
                self.degraded.add("result_malformed")
            elif entry.get("export") == export and entry.get("table") == table:
                candidates.append(entry)
        for candidate in candidates:
            parsed = ErrorObject.from_contract(candidate)
            if parsed is not None:
                return parsed
            self.degraded.add("result_malformed")
        self.degraded.add("per_unit_error")
        return error or EXIT_ZERO  # no process-level object only after a clean exit 0 (`classify`)

    def settled(self, units: list[UnitResult]) -> list[UnitResult]:
        """The units; one failed unit has the process's error as its own, so the fallback is no degradation then."""
        if len([u for u in units if u.status == "failed"]) <= 1:
            self.degraded.discard("per_unit_error")
        return units

    def run_units(self, step: _Step, summary: Any, error: ErrorObject | None, export: str | None) -> list[UnitResult]:
        """Units of a `run` / `apply` process, from its summary file when it wrote one."""
        entries = self.entries(summary, "per_export")
        if not entries:
            if error is None and export is None:
                return []
            return [UnitResult(export, status="failed" if error else "success", error=error)]
        units = []
        for entry in entries:
            name, status = self.malformed(entry.get("export_name"), str), self.status(entry.get("status"))
            unit_error = None  # pragma: no mutate - a unit not failed is read by its status, its error by truthiness
            if status == "failed":
                unit_error = self.unit_error(step, error, entry.get("error"), name, None)
            if entry.get("mode") == "cdc" and status == "success" and "stop_reason" not in entry:
                self.degraded.add("stop_reason")
            units.append(
                UnitResult(
                    export=name,
                    status=status,
                    run_id=self.malformed(entry.get("run_id"), str),
                    rows=self.malformed(entry.get("rows"), int),
                    files=self.malformed(entry.get("files"), int),
                    stop_reason=self.malformed(entry.get("stop_reason"), str),
                    error=unit_error,
                )
            )
        return self.settled(units)

    def load_units(
        self, step: _Step, doc: Any, error: ErrorObject | None, export: str | None, table: str | None
    ) -> list[UnitResult]:
        """Units of a `load` / `compact` process: one per `per_table` entry of its result (the asked table only), or the
        one the task asked for when the result has none."""
        rows = self.entries(doc, "per_table")
        if not rows:
            status = "failed" if error else DONE_WORD[self.command]
            return [self.with_target(UnitResult(export, table, status=status, error=error), {})]
        units = []
        for row in rows:
            if table and row.get("table") != table:
                continue
            name, of = self.malformed(row.get("export"), str), self.malformed(row.get("table"), str)
            status = self.status(row.get("status"))
            unit_error = None  # pragma: no mutate - a unit not failed is read by its status, its error by truthiness
            if status == "failed":
                unit_error = self.unit_error(step, error, row.get("error"), name, of)
            units.append(
                UnitResult(
                    export=name,
                    table=of,
                    status=status,
                    run_id=self.malformed(doc.get("run_id"), str),
                    rows=self.malformed(row.get("rows"), int),
                    error=unit_error,
                    skip_reason=self.malformed(row.get("skip_reason"), str),
                    digest=self.digest(row),
                )
            )
            self.with_target(units[-1], row)
        return self.settled(units)

    def with_target(self, unit: UnitResult, row: Mapping[str, Any]) -> UnitResult:
        """The unit with the warehouse objects it touched: rivet's own names when its result has them, else the config's."""
        named = row.get("target")
        if isinstance(named, str) and named:
            buffer = row.get("buffer")
            unit.target, unit.buffer, unit.target_from = named, buffer if isinstance(buffer, str) else None, "rivet"
        else:
            unit.target, unit.buffer = config_target(self.config, self.command, unit.export, unit.table)
            unit.target_from = "config" if unit.target else None
        return unit

    def digest(self, row: Mapping[str, Any]) -> dict[str, Any] | None:
        """The D3 digest of one `per_table` entry, each value of the contract's type or null (recorded); `None`, recorded
        as `load_digest`, when the entry has none of its keys (a binary older than the digest)."""
        if not any(key in row for key in DIGEST_KEYS):
            self.degraded.add("load_digest")
            return None
        digest = {key: self.malformed(row.get(key), int) for key in DIGEST_COUNTS}
        jobs = self.malformed(row.get("warehouse_job_ids"), list)
        if jobs is not None and not all(isinstance(job, str) for job in jobs):
            self.degraded.add("result_malformed")  # a list holding something other than ids: null, recorded
            jobs = None
        digest["warehouse_job_ids"] = jobs
        return digest
