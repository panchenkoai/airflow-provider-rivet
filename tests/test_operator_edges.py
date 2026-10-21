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
UNIT_KEYS = {
    "event",
    "skip_reason",
    "target",
    "buffer",
    "target_from",
    "digest",
    "export",
    "table",
    "status",
    "run_id",
    "rows",
    "files",
    "stop_reason",
}


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
    assert (payload["command"], payload["exit_status"], payload["rivet_version"]) == ("run", 0, "0.32.0 (fake)")
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


def test_rivet_notifications_true_keeps_rivets_own_notifications_and_records_no_degradation(rig: Rig) -> None:
    rig.scenario(flags={"run": ["--no-notify", "--lock-wait"]}, run={"summary": {"per_export": [entry()]}})
    payload = payload_of(rig, rig.operator(RivetRunOperator, export="orders", rivet_notifications=True))
    assert "--no-notify" not in rig.argv("run") and "--lock-wait" in rig.argv("run")
    assert "no_notify" not in payload["degraded"]


def test_rivet_notifications_true_is_not_warned_about_a_notifications_block(rig: Rig) -> None:
    rig.config.write_text(rig.config.read_text() + "notifications:\n  slack: {webhook_env: HOOK}\n")
    rig.scenario(flags={"run": ["--lock-wait"]}, run={"summary": {"per_export": [entry()]}})
    warned = lambda: [e["warning"] for e in rig.events("rivet.warning")].count("notifications_block")  # noqa: E731
    payload_of(rig, rig.operator(RivetRunOperator, export="orders", rivet_notifications=True))
    assert warned() == 0
    payload_of(rig, rig.operator(RivetRunOperator, export="orders"))
    assert warned() == 1, "the default warns: this binary cannot turn rivet's notifications off"


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


def test_a_replay_rivet_answers_already_applied_names_the_export_run_and_artifact(rig: Rig) -> None:
    """The `rivet.apply.already_applied` record of a replay rivet answered (rivet >= 0.32)."""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": {"per_export": [entry()]}})
    op = rig.operator(RivetApplyOperator, export="orders")
    first = payload_of(rig, op, FakeTI(try_number=1, max_tries=2))
    payload_of(rig, op, FakeTI(try_number=2, max_tries=2))
    [done] = rig.events("rivet.apply.already_applied")
    assert done == {
        "event": "rivet.apply.already_applied",
        "export": "orders",
        "run_id": first["units"][0]["run_id"],
        "artifact": rig.argv("apply")[1],
    }


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


# --- the digest of a load / compact entry (#63) --------------------------------------------------------------------


def digest_of(row: dict) -> tuple[dict | None, set[str]]:
    """`_digest` of one entry, and the degradations it recorded."""
    from types import SimpleNamespace

    s = SimpleNamespace(degraded=set())
    s.malformed = lambda value, kind: invocation._Session.malformed(s, value, kind)
    return invocation._digest(s, row), s.degraded  # type: ignore[arg-type]


def test_a_digest_is_read_with_the_contracts_types_and_nothing_else() -> None:
    from conftest import fixture

    for name in ("load_result.json", "compact_result.json"):
        for row in fixture(name)["per_table"]:
            digest, degraded = digest_of(row)
            assert digest == {k: row[k] for k in invocation.DIGEST_KEYS} and degraded == set(), (name, row["table"])
    digest, degraded = digest_of({"rows_merged": "12", "bytes_billed": True, "files": 1.0, "warehouse_job_ids": ["j", 1]})
    assert digest == {k: None for k in invocation.DIGEST_KEYS} and degraded == {"result_malformed"}
    digest, degraded = digest_of({"warehouse_job_ids": ["j", 1]})  # one id that is not a name: no ids at all
    assert digest["warehouse_job_ids"] is None and degraded == {"result_malformed"}
    digest, degraded = digest_of({"warehouse_job_ids": "j1", "runs": 2})
    assert digest["warehouse_job_ids"] is None and digest["runs"] == 2 and degraded == {"result_malformed"}


def test_an_entry_without_a_digest_key_is_an_older_binary_and_says_so() -> None:
    assert digest_of({"export": "e", "table": "t", "rows": 1, "target": "p.d.t"}) == (None, {"load_digest"})
    for key in invocation.DIGEST_KEYS:  # any one key is the digest's shape: the rest read as null
        digest, degraded = digest_of({key: None})
        assert digest == {k: None for k in invocation.DIGEST_KEYS} and degraded == set(), key


def test_the_unit_record_carries_the_digest_and_the_configs_names_where_xcom_has_none(rig: Rig) -> None:
    """A row without the digest (a binary older than 0.32.2): the log names the config's table, XCom nothing."""
    rig.config.write_text(rig.config.read_text() + "load:\n  target: bigquery\n  project: p\n  dataset: d\n")
    row = {"export": "users", "table": "users", "status": "loaded", "rows": 4}
    rig.scenario(flags=NEW_FLAGS, load={"summary": {"run_id": "l1", "per_table": [row]}})
    payload = payload_of(rig, rig.operator(RivetLoadOperator, export="users", table="users"))
    [unit] = rig.events("rivet.unit")
    assert (unit["target"], unit["target_from"], unit["digest"]) == ("p.d.users__changes", "config", None)
    assert (payload["units"][0]["target"], payload["units"][0]["buffer"]) == (None, None)
    assert "load_digest" in payload["degraded"]
