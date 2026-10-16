"""An apply on a binary without `apply --summary-output` reads its unit from `rivet metrics`: only the row of THIS apply.

rivet's run id names the process that made a run, `<export>_<UTC start, ms>_<pid>` (G1b). Another run of the export (a
run outside Airflow, another config of the same export name on a shared PostgreSQL state) writes rows too, and the
newest row is not this apply's whenever such a run started later. Reading that row gave the task another run's run id
and counts, and a `running` / `failed` row of it made a failed unit with no error, which the task reported as success
(`invocation._Session.outcome` mutant 2, root cause `_unit_from_metrics`)."""

from __future__ import annotations

import re

from airflow.exceptions import AirflowFailException
from conftest import FakeTI, Rig

from airflow_provider_rivet.operators import RivetApplyOperator

COUNTS = {"per_export": [{"export_name": "orders", "status": "success", "mode": "full", "rows": 500, "files": 3}]}
OWN_RUN = re.compile(r"orders_\d{8}T\d{6}\.\d{3}_\d+")


def foreign(status: str, run_id: str = "orders_20991231T235959.999_1") -> dict:
    """A row another run of `orders` wrote after this apply's (a later start, another pid)."""
    return {"export_name": "orders", "run_id": run_id, "status": status, "total_rows": 9, "files_produced": 9}


def test_the_unit_is_this_applys_run_even_when_another_run_wrote_a_newer_row(rig: Rig) -> None:
    """Newest row: another run's (success, then running): the unit still carries this apply's run id and counts."""
    for status in ("success", "running"):
        rig.scenario(apply={"summary": COUNTS, "metrics_after": [foreign(status)]})
        payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
        assert isinstance(payload, dict), payload
        (unit,) = payload["units"]
        assert payload["decision"] == "success" and payload["error"] is None
        assert OWN_RUN.fullmatch(unit["run_id"]) and unit["run_id"] != foreign(status)["run_id"]
        assert (unit["status"], unit["rows"], unit["files"]) == ("success", 500, 3)
        metrics = [c["argv"] for c in rig.calls() if c["argv"][0] == "metrics"][-1]
        assert metrics[metrics.index("--last") + 1] == "50", "this apply's row may sit below newer rows of other runs"


def test_another_runs_running_or_failed_row_never_makes_the_task_a_success(rig: Rig) -> None:
    """Every row rivet metrics lists belongs to another run: the task fails with a code, carrying none of its ids."""
    for status in ("running", "failed", "success"):
        rig.scenario(metrics={"rows": [foreign(status)]})  # what `rivet metrics` answers: no row of this apply
        exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"), FakeTI(task_id="task"))
        assert type(exc) is AirflowFailException, (status, exc)
        payload = ti.store[("task", "return_value")]
        assert str(exc).startswith("[RIVET_AIRFLOW_APPLY_RUN_UNKNOWN] generic: ")
        assert payload["decision"] == "fail"
        assert payload["error"]["code"] == "RIVET_AIRFLOW_APPLY_RUN_UNKNOWN"
        (unit,) = payload["units"]
        assert unit["status"] == "failed" and unit["run_id"] is None and unit["rows"] is None
        assert unit["error"]["code"] == "RIVET_AIRFLOW_APPLY_RUN_UNKNOWN"


def test_a_metrics_call_that_fails_or_lists_nothing_fails_the_task_with_the_code(rig: Rig) -> None:
    for metrics in ({"exit": 1}, {"rows": []}):
        rig.scenario(metrics=metrics)
        exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"), FakeTI(task_id="task"))
        assert type(exc) is AirflowFailException, (metrics, exc)
        assert ti.store[("task", "return_value")]["error"]["code"] == "RIVET_AIRFLOW_APPLY_RUN_UNKNOWN"


def test_a_skipped_run_of_this_apply_is_a_skipped_unit(rig: Rig) -> None:
    """D31 through the same rule: this apply's own row says `skipped`, with rivet's reason."""
    rig.scenario(
        apply={"metric": {"status": "skipped", "skip_reason": "source returned 0 rows"}, "metrics_after": [foreign("success")]}
    )
    payload, _ = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert payload["decision"] == "success"
    assert payload["units"][0]["status"] == "skipped" and OWN_RUN.fullmatch(payload["units"][0]["run_id"])
    assert payload["skip_reason"] == "source returned 0 rows"
