"""What a task reports at the edges a mutation run found unpinned: the keys of every structured event and of the
payload, the degraded words, an apply whose sealing plan fails, the apply's argv and paths, and the stderr readers.
Each test names the mutants it kills (`mutmut show <module>.x_<function>__mutmut_<n>`)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException
from conftest import NEW_FLAGS, FakeTI, Rig

from airflow_provider_rivet import invocation
from airflow_provider_rivet.invocation import crashed_retry_allowed
from airflow_provider_rivet.operators import RivetApplyOperator, RivetLoadOperator, RivetRunOperator

RESULT_KEYS = {"event", "command", "decision", "degraded", "exit_status", "signal", "stderr_path"}
UNIT_KEYS = {"event", "skip_reason", "export", "table", "status", "run_id", "rows", "files", "stop_reason"}


def entry(status: str = "success", **more: object) -> dict:
    """One `per_export` entry of export `orders` as today's binary writes it."""
    return {"export_name": "orders", "status": status, "run_id": "orders_1", "rows": 3, "files": 1, "mode": "full", **more}


def payload_of(rig: Rig, op: object, ti: FakeTI | None = None) -> dict:
    """The XCom payload of a run, whether the task returned it or raised after pushing it."""
    out, ti = rig.run(op, ti)
    return out if isinstance(out, dict) else ti.store[("task", "return_value")]


# --- the structured events and the payload ------------------------------------------------------------------------


def test_every_structured_event_and_the_payload_carry_their_keys(rig: Rig) -> None:
    """A log pipeline reads these keys (outcome 15 18 24 34 51 52 58-69, step 60 61 88 89)."""
    rig.scenario(leak_env=[], run={"summary": {"per_export": [entry()]}, "stderr": "a warning"})
    payload = payload_of(rig, rig.operator(RivetRunOperator, export="orders", stream_stderr=True))
    assert (payload["command"], payload["exit_status"], payload["rivet_version"]) == ("run", 0, "0.31.0")
    [result] = rig.events("rivet.result")
    assert set(result) == RESULT_KEYS
    assert {k: result[k] for k in RESULT_KEYS - {"event"}} == {k: payload[k] for k in RESULT_KEYS - {"event"}}
    [unit] = rig.events("rivet.unit")
    assert set(unit) == UNIT_KEYS and unit["skip_reason"] is None
    [step] = rig.events("rivet.step")
    assert step == {"event": "rivet.step", "step": "run", "exit_status": 0, "signal": None}
    lines = rig.events("rivet.stderr")
    assert lines and all(set(e) == {"event", "step", "line"} and e["step"] == "run" for e in lines)


def test_a_killed_process_has_no_exit_status_and_its_cut_line_is_not_malformed_output(rig: Rig) -> None:
    """A signal is not an exit status (step 64); a broken `{` line before a kill is the kill's, not a malformed
    result (classify 20)."""
    rig.scenario(run={"line_raw": '{"exit_class": 2, "err', "signal": "SIGKILL"})
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    [step] = rig.events("rivet.step")
    assert (step["exit_status"], step["signal"]) == (None, 9)
    [result] = rig.events("rivet.result")
    assert "result_malformed" not in result["degraded"]


def test_a_failed_process_whose_error_line_is_cut_is_recorded_malformed(rig: Rig) -> None:
    """Exit 1 with a broken `{` line and no whole one: the error is built from the exit and the cut line is
    recorded (step 100: the broken flag reaches `classify`)."""
    rig.scenario(run={"line_raw": '{"exit_class": 2, "err', "exit": 1})
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    [result] = rig.events("rivet.result")
    assert "result_malformed" in result["degraded"]


@pytest.mark.parametrize(
    ("bad", "field", "shown"),
    [({"rows": "7"}, "rows", None), ({"status": "weird"}, "status", "failed")],
    ids=["wrong-typed-rows", "unknown-status"],
)
def test_a_value_outside_the_contract_is_dropped_and_recorded(rig: Rig, bad: dict, field: str, shown: object) -> None:
    """A wrong-typed value is `None`, an unknown status word is `failed`; both say `result_malformed` (malformed 5 6 7,
    status 1 4 5)."""
    rig.scenario(run={"summary": {"per_export": [entry(**bad)]}})
    payload = payload_of(rig, rig.operator(RivetRunOperator, export="orders"))
    assert payload["units"][0][field] == shown
    assert "result_malformed" in payload["degraded"]


def test_one_failed_unit_without_its_own_error_is_not_per_unit_error(rig: Rig) -> None:
    """One failed unit takes the process-level error, which is its own (run_units 122 124 125 126)."""
    rig.scenario(run={"summary": {"per_export": [entry("failed")]}})
    payload = payload_of(rig, rig.operator(RivetRunOperator, export="orders"))
    assert payload["units"][0]["status"] == "failed" and payload["units"][0]["error"] is not None
    assert "per_unit_error" not in payload["degraded"]


def test_two_failed_tables_without_their_errors_are_per_unit_error(rig: Rig) -> None:
    """Two failed tables share one process-level error, recorded (load 196)."""
    rows = [{"export": "orders", "table": t, "status": "failed", "error": None} for t in ("users", "items")]
    rig.scenario(flags=NEW_FLAGS, load={"summary": {"run_id": "l1", "per_table": rows}})
    payload = payload_of(rig, rig.operator(RivetLoadOperator, export="orders"))
    assert [u["status"] for u in payload["units"]] == ["failed", "failed"]
    assert "per_unit_error" in payload["degraded"]


def test_a_load_result_with_a_non_object_entry_is_recorded_malformed(rig: Rig) -> None:
    """(load 81)"""
    rows = ["junk", {"export": "orders", "table": "users", "status": "success", "rows": 1}]
    rig.scenario(flags=NEW_FLAGS, load={"summary": {"run_id": "l1", "per_table": rows}})
    payload = payload_of(rig, rig.operator(RivetLoadOperator, export="orders"))
    assert [u["table"] for u in payload["units"]] == ["users"]
    assert "result_malformed" in payload["degraded"]


# --- run units without a summary entry -----------------------------------------------------------------------------


def test_a_run_with_no_summary_entry_reports_by_what_it_ran(rig: Rig) -> None:
    """One export: one unit, success or failed; the whole config: no unit on success, one failed unit on failure
    (run_units 11 12 13 21 24 25)."""
    one = payload_of(rig, rig.operator(RivetRunOperator, export="orders"))
    assert [(u["export"], u["status"]) for u in one["units"]] == [("orders", "success")]
    whole = payload_of(rig, rig.operator(RivetRunOperator))
    assert whole["units"] == []
    rig.scenario(run={"exit": 2, "line": {"error": "network", "exit_class": 2}})
    failed = payload_of(rig, rig.operator(RivetRunOperator))
    assert [(u["export"], u["status"]) for u in failed["units"]] == [(None, "failed")]


# --- apply -----------------------------------------------------------------------------------------------------------


def test_an_apply_whose_sealing_plan_fails_reports_that_plans_error_on_its_unit(rig: Rig) -> None:
    """The plan's own error decides, and the unit carries it (apply 106 110 111 112 114 115 116 117)."""
    rig.scenario(plan={"exit": 2, "line": {"error": "network", "exit_class": 2}})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    payload = ti.store[("task", "return_value")]
    assert type(exc) is AirflowException and payload["decision"] == "retry"
    [unit] = payload["units"]
    assert (unit["export"], unit["status"], unit["error"]["class"]) == ("orders", "failed", "retryable")
    assert "exit_zero_failed_unit" not in payload["degraded"]
    assert [c["argv"][0] for c in rig.calls()] == ["plan"]


def test_an_apply_seals_under_plans_passes_its_globals_and_starts_rivet_with_no_stdin(rig: Rig) -> None:
    """(apply 29 31 32 167 168, step 32 40)"""
    # the flags on `apply` alone: the fake lists `*` flags for any subcommand's --help, even one rivet lacks
    flags = {"apply": ["--no-notify", "--lock-wait", "--summary-output"]}
    rig.scenario(flags=flags, record_stdin=True, apply={"summary": {"per_export": [entry()]}})
    read, write = os.pipe()
    saved = os.dup(0)
    os.dup2(read, 0)  # a stdin that is not /dev/null, which a child would inherit
    try:
        payload = payload_of(rig, rig.operator(RivetApplyOperator, export="orders"))
    finally:
        os.dup2(saved, 0)
        for fd in (saved, read, write):
            os.close(fd)
    argv = rig.argv("apply")
    assert Path(argv[1]).parent.parent.name == "plans"
    assert "--no-notify" in argv and "--lock-wait" in argv and "no_notify" not in payload["degraded"]
    assert all(c["stdin_null"] for c in rig.calls())


def test_an_apply_summary_with_a_failed_export_gives_it_an_error(rig: Rig) -> None:
    """(apply 212)"""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": {"per_export": [entry("failed")]}})
    payload = payload_of(rig, rig.operator(RivetApplyOperator, export="orders"))
    assert payload["units"][0]["error"]["class"] == "generic"


def test_a_sealed_plan_that_is_not_an_object_has_no_strategy(rig: Rig) -> None:
    """(apply 137, and the `export` key of the event: 132 133)"""
    rig.scenario(plan={"artifact_raw": "[]"})
    rig.run(rig.operator(RivetApplyOperator, export="orders"))
    [sealed] = rig.events("rivet.plan.sealed")
    assert sealed == {"event": "rivet.plan.sealed", "export": "orders", "strategy": None}


def test_a_retry_finds_the_earlier_apply_and_says_where_its_lines_are(rig: Rig) -> None:
    """(apply 54 55 62 63 66 67 68)"""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": {"per_export": [entry()]}})
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op, FakeTI(try_number=1, max_tries=2))
    retry = payload_of(rig, op, FakeTI(try_number=2, max_tries=2))
    [done] = rig.events("rivet.apply.already_applied")
    assert set(done) == {"event", "export", "run_id", "artifact"}
    assert (done["export"], done["artifact"]) == ("orders", rig.argv("apply")[1])
    assert retry["stderr_path"].endswith(".metrics.stderr.log") and Path(retry["stderr_path"]).exists()


def test_a_record_of_another_export_is_not_this_applys(rig: Rig) -> None:
    """The record must name this export, whatever its pid (applied_by_earlier_try 3)."""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": {"per_export": [entry()]}})
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op, FakeTI(try_number=1, max_tries=2))
    artifact = Path(rig.argv("apply")[1])
    record = artifact.with_name(f"{artifact.stem}.apply.json")
    record.write_text(json.dumps({**json.loads(record.read_text()), "export": "users"}))
    rig.run(op, FakeTI(try_number=2, max_tries=2))
    assert [c["argv"][0] for c in rig.calls()].count("apply") == 2


def test_a_failed_metrics_call_gives_no_counts_whatever_it_printed(rig: Rig) -> None:
    """(_metrics_rows 28)"""
    rig.scenario(metrics={"exit": 2})
    payload = payload_of(rig, rig.operator(RivetApplyOperator, export="orders"))
    unit = payload["units"][0]
    assert (unit["status"], unit["run_id"], unit["rows"]) == ("success", None, None)


# --- the stderr readers and the crash ledger -------------------------------------------------------------------------


def test_the_stderr_readers() -> None:
    """(_lines 4, _error_line 12 13, _has_broken_object 6)"""
    assert invocation._lines("a\nb") == ["a", "b"]
    line = {"exit_class": 2, "error": "x"}
    assert invocation._error_line([json.dumps(line), "{cut"]) == line
    assert invocation._error_line([json.dumps(line), '{"other": 1}']) == line
    assert invocation._has_broken_object(["plain", '{"a": 1}']) is False


def test_a_crash_ledger_that_cannot_be_written_names_the_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """(crashed_retry_allowed 41)"""

    def refuse(path: Path, data: bytes) -> str:
        raise PermissionError(path)

    monkeypatch.setattr(invocation, "_write_atomic", refuse)
    allowed, why = crashed_retry_allowed(tmp_path / "crashed" / "ledger", 1, 3, 1)
    assert allowed is False and "cannot be written (PermissionError)" in str(why)
