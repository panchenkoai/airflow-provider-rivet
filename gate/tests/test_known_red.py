"""Known reds: a cell in KNOWN is XFAIL while red and fails the run once it passes (XPASS)."""

from __future__ import annotations

from gate.__main__ import summary
from gate.scenario import KNOWN, Verdicts

CELL = "initial/mysql/cdc/staged-parts-cleaned"


def test_a_known_red_cell_is_xfail_with_its_issue_and_does_not_fail_the_run() -> None:
    v = Verdicts("3.3.2")
    v.grade("initial/x/y", [])
    v.grade(CELL, ["2 objects left"])
    assert [r["status"] for r in v.rows] == ["PASS", "XFAIL"]
    assert v.rows[1]["detail"] == f"{KNOWN[CELL]}: 2 objects left"
    assert summary(v.rows, 1) == ("airflow gate: 1 PASS, 0 FAIL, 1 XFAIL over 1 stack(s)", True)


def test_a_known_red_cell_that_passes_is_xpass_and_fails_the_run() -> None:
    v = Verdicts("3.3.2")
    v.grade(CELL, [])
    assert v.rows[0]["status"] == "XPASS" and "remove it from KNOWN" in v.rows[0]["detail"]
    assert summary(v.rows, 1) == ("airflow gate: 0 PASS, 1 FAIL over 1 stack(s)", False)


def test_an_unknown_red_cell_fails_and_an_empty_run_does_not_pass() -> None:
    v = Verdicts("2.10.5")
    v.grade("delta/mysql/cdc/staged-parts-cleaned", ["left"])
    assert summary(v.rows, 2) == ("airflow gate: 0 PASS, 1 FAIL over 2 stack(s)", False)
    assert summary([], 1)[1] is False


def test_every_known_cell_names_an_issue() -> None:
    assert all("#" in issue for issue in KNOWN.values())
