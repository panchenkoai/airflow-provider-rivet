"""The MySQL CDC stream read through a git-sync checkout: the layout, the flip, and the two cells graded after it.

Each test names the mutant it is RED on.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from test_sources_mysql_client import CDC, prepared

from gate import sources
from gate.sources import flip, git_sync_layout, run_after_flip, stream_equals_source


def test_the_config_is_read_through_a_relative_repo_link(tmp_path: Path) -> None:
    root = tmp_path / "rivet" / "configs" / "mysql_cdc_sync"
    path = git_sync_layout(root, CDC)
    assert path == "/opt/rivet/configs/mysql_cdc_sync/repo/rivet.yaml"
    # Mutant: an absolute link target (a host path, which the containers cannot resolve).
    assert os.readlink(root / "repo") == "rev-A"
    assert (root / "repo" / "rivet.yaml").read_text() == CDC, "init's text, unchanged"


def test_the_flip_publishes_rev_b_as_git_sync_does(tmp_path: Path) -> None:
    root = tmp_path / "rivet" / "configs" / "mysql_cdc_sync"
    git_sync_layout(root, CDC)
    assert flip(root) == "rev-B"
    # Mutant: the link pointed at an absolute path, or the old link left in place (no flip).
    assert os.readlink(root / "repo") == "rev-B"
    assert (root / "repo" / "rivet.yaml").read_text() == CDC, "the config is copied unchanged"
    # Mutant: the old revision kept (git-sync removes it; a provider that still read it would pass).
    assert not (root / "rev-A").exists()
    assert sorted(p.name for p in root.iterdir()) == ["repo", "rev-B"], "no temporary link left behind"
    assert flip(root) == "rev-A" and not (root / "rev-B").exists(), "a second flip works the same way"


def payload(rows: int | None = 3, code: str | None = None) -> dict:
    error = {"code": code} if code else None
    units = [] if code else [{"rows": rows}]
    return {"decision": "fail" if code else "success", "error": error, "exit_status": 5 if code else 0, "units": units}


TASKS = {"run": {"state": "success"}, "load.gate_customers": {"state": "success"}, "compact.gate_orders": {"state": "skipped"}}


def test_the_run_after_the_flip_must_succeed_and_move_the_changes() -> None:
    assert run_after_flip("success", TASKS, payload(), 3) == []
    # The defect this cell exists for: the old copy owns the checkpoint, and the run is refused.
    failed = {"run": {"state": "failed"}, "load.gate_customers": {"state": "upstream_failed"}}
    assert run_after_flip("failed", failed, payload(code="RIVET_SOURCE_CDC_CHECKPOINT_SHARED"), 3) == [
        "run failed",
        "tasks {'run': 'failed', 'load.gate_customers': 'upstream_failed'}",
        "rivet stopped [RIVET_SOURCE_CDC_CHECKPOINT_SHARED] exit 5",
    ]
    # Mutant: the row count not compared (a run that read nothing after the flip passes).
    assert run_after_flip("success", TASKS, payload(0), 3) == ["the run moved 0 row(s), the source changed 3"]
    # Mutant: the task states not read (a load that failed after a good run passes).
    assert run_after_flip("success", {**TASKS, "load.gate_customers": {"state": "failed"}}, payload(), 3) == [
        "tasks {'load.gate_customers': 'failed'}"
    ]
    assert run_after_flip("success", {}, payload(), 3) == ["tasks none"]


def test_the_stream_must_equal_the_source_on_every_table() -> None:
    source = {"gate_customers": [{"id": 4, "tier": "synced"}], "gate_orders": [{"id": 1, "note": None}]}
    cols = {"gate_customers": ["id", "tier"], "gate_orders": ["id", "note"]}
    got = {
        "gate_customers": [{"id": 4, "tier": "synced", "__is_deleted": False}],
        "gate_orders": [{"id": 1, "note": None, "__is_deleted": False}, {"id": 2, "note": "x", "__is_deleted": True}],
    }
    assert stream_equals_source(got, source, cols, "id") == []
    # Mutant: the change made after the flip not compared (the stream stopped at the flip).
    stale = {**got, "gate_customers": [{"id": 4, "tier": "free", "__is_deleted": False}]}
    assert any("missing" in p for p in stream_equals_source(stale, source, cols, "id"))
    # Mutant: one table only compared.
    assert stream_equals_source({"gate_customers": got["gate_customers"]}, source, cols, "id") != []


def test_the_streams_dags_read_the_config_through_the_checkout(tmp_path: Path, monkeypatch) -> None:
    leg, stand = prepared(tmp_path, monkeypatch)
    shape = json.loads((stand.rivet_dir / "configs" / "mysql_shape.json").read_text())
    # Mutant: the DAG left on init's own path (no symlink: the flip changes nothing the operator reads).
    assert shape["cdc_config"] == "/opt/rivet/configs/mysql_cdc_sync/repo/rivet.yaml"
    assert (stand.rivet_dir / "configs" / "mysql_cdc_sync" / "repo").is_symlink()
    dag = (Path(sources.__file__).parent / "stand" / "dags" / "rivet_gate.py").read_text()
    block = dag[dag.index("cdc_config = shape.get") : dag.index("# A second CDC config")]
    assert block.count("config=cdc_config") == 2, "the stream's DAG and the hot DAG read one config path"
