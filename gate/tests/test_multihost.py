"""The multi-host leg's SQLite cell: every extract of the run refused before rivet started, nothing else accepted."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from gate.multihost import SQLITE_MULTI_HOST, MultiHost


def judge(state: str, tasks: dict[str, str], codes: dict[str, str | None]) -> list[str]:
    stand = SimpleNamespace(
        tasks=lambda dag, run_id: {t: {"state": s} for t, s in tasks.items()},
        xcom=lambda dag, run_id, task: {"preflight_refusal": codes.get(task)},
    )
    leg = MultiHost.__new__(MultiHost)
    leg.stand = stand
    return leg.sqlite_refused(state, "run")


def test_a_run_whose_extracts_were_all_refused_passes() -> None:
    tasks = {"apply.a": "failed", "apply.b": "failed", "watcher": "failed"}
    assert judge("failed", tasks, {"apply.a": SQLITE_MULTI_HOST, "apply.b": SQLITE_MULTI_HOST}) == []


@pytest.mark.parametrize(
    ("state", "tasks", "codes"),
    [
        ("success", {"apply.a": "success"}, {}),  # rivet ran on SQLite state across hosts
        ("failed", {"apply.a": "failed", "apply.b": "success"}, {"apply.a": SQLITE_MULTI_HOST}),  # one ran
        ("failed", {"apply.a": "failed"}, {"apply.a": "RIVET_AIRFLOW_STATE_DIR_MISSING"}),  # refused for another reason
        ("failed", {"apply.a": "failed"}, {"apply.a": None}),  # rivet started and failed
    ],
)
def test_anything_but_the_refusal_of_every_extract_fails(state: str, tasks: dict, codes: dict) -> None:
    assert judge(state, tasks, codes)
