"""Retries that must not run rivet twice: a try that died after its apply succeeded (G1, G1b), two tries of one export
at once (G2: the per-export lock passed to rivet, the parent-death signal), and rivet's busy refusals (G2c)."""

from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException, AirflowFailException
from conftest import NEW_FLAGS, FakeTI, Rig

from airflow_provider_rivet.operators import RivetApplyOperator, RivetLoadOperator, RivetRunOperator

FULL = {"per_export": [{"export_name": "orders", "status": "success", "mode": "full", "rows": 7, "files": 2}]}
INCREMENTAL = {"per_export": [{"export_name": "users", "status": "success", "mode": "incremental", "rows": 3, "files": 1}]}


def applies(rig: Rig) -> list[list[str]]:
    return [c["argv"] for c in rig.calls() if c["argv"][0] == "apply"]


def recorded_runs(rig: Rig) -> list[dict]:
    """The fake state's `export_metrics` rows, oldest first."""
    path = rig.bin / "metrics.json"
    return json.loads(path.read_text()) if path.exists() else []


def wait_for(path: Path, timeout: float = 30.0) -> str:
    """The text of a file another process writes once it reached a point; a forced interleaving, not a sleep."""
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            pytest.fail(f"{path} was never written")
        time.sleep(0.02)
    time.sleep(0.05)
    return path.read_text()


def held(lock: Path) -> bool:
    """True when another open file description holds an exclusive flock on the file."""
    with open(lock) as probe:
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(probe, fcntl.LOCK_UN)
        return False


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


# --- G1 / G1b: a retry of an apply whose plan already ran to success -------------------------------------------------


@pytest.mark.parametrize(
    ("export", "summary", "cursors"), [("orders", FULL, {}), ("users", INCREMENTAL, {"users": "5"})], ids=["full", "incremental"]
)
def test_g1b_g1_a_retry_after_the_task_died_past_a_successful_apply_does_not_apply_again(
    rig: Rig, export: str, summary: dict, cursors: dict
) -> None:
    """G1b (`mode: full`) / G1 (incremental): rivet apply exited 0, the task process died before Airflow recorded the
    success, and the retry is a try of the same series. It replayed the sealed plan: a full export was extracted a
    second time, an incremental one was refused RIVET_PLAN_ARTIFACT_STALE (exit 5, a red DAG run with no retry). The
    retry finds the apply it started in rivet's state and ends success with that run's unit, starting no apply."""
    rig.scenario(flags=NEW_FLAGS, cursors=cursors, apply={"summary": summary})
    op = rig.operator(RivetApplyOperator, export=export)
    first, _ = rig.run(op, FakeTI(try_number=1, max_tries=2))
    assert first["decision"] == "success"  # ... and the task process died before Airflow saw it
    [run] = [r for r in recorded_runs(rig) if r["status"] == "success"]
    retry, _ = rig.run(op, FakeTI(try_number=2, max_tries=2))
    assert len(applies(rig)) == 1, f"the retry applied the plan again: {retry}"
    assert isinstance(retry, dict) and retry["decision"] == "success", retry
    expected = summary["per_export"][0]
    unit = retry["units"][0]
    assert (unit["export"], unit["status"], unit["run_id"]) == (export, "success", run["run_id"])
    assert (unit["rows"], unit["files"]) == (expected["rows"], expected["files"])
    [done] = rig.events("rivet.apply.already_applied")
    assert done["run_id"] == run["run_id"]


@pytest.mark.parametrize("how", ["failed", "killed"])
def test_g1b_a_retry_after_a_failed_or_killed_apply_replays_the_plan(rig: Rig, how: str) -> None:
    """G1b negative: the apply the earlier try started did not succeed (exit 2, or killed and left `running`), so the
    retry applies the sealed plan again, as before."""
    first_play = {"exit": 2, "line": {"error": "network", "exit_class": 2}} if how == "failed" else {"signal": "SIGKILL"}
    rig.scenario(flags=NEW_FLAGS, apply=first_play)
    op = rig.operator(RivetApplyOperator, export="orders")
    first, _ = rig.run(op, FakeTI(try_number=1, max_tries=2))
    assert type(first) is AirflowException
    assert [r["status"] for r in recorded_runs(rig)] == ["failed" if how == "failed" else "running"]
    rig.scenario(flags=NEW_FLAGS, apply={"summary": FULL})
    retry, _ = rig.run(op, FakeTI(try_number=2, max_tries=2))
    assert retry["decision"] == "success" and len(applies(rig)) == 2
    assert applies(rig)[0][1] == applies(rig)[1][1], "the same sealed artifact is replayed"
    assert not rig.events("rivet.apply.already_applied")


def test_g1b_a_successful_run_of_the_export_that_this_apply_did_not_start_is_not_taken_for_it(rig: Rig) -> None:
    """G1b negative: after the apply failed, a `rivet run` of the same export succeeded (another process, started after
    the apply): it is not this plan's run, so the retry still applies the plan."""
    rig.scenario(flags=NEW_FLAGS, apply={"exit": 2, "line": {"error": "network", "exit_class": 2}}, run={"summary": FULL})
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op, FakeTI(try_number=1, max_tries=2))
    other, _ = rig.run(rig.operator(RivetRunOperator, task_id="other", export="orders"))
    assert other["decision"] == "success"
    assert [r["status"] for r in recorded_runs(rig)] == ["failed", "success"]
    rig.scenario(flags=NEW_FLAGS, apply={"summary": FULL})
    retry, _ = rig.run(op, FakeTI(try_number=2, max_tries=2))
    assert retry["decision"] == "success" and len(applies(rig)) == 2
    assert not rig.events("rivet.apply.already_applied")


def test_g1b_a_new_try_series_plans_and_applies_again(rig: Rig) -> None:
    """G1b negative: a clear (a new `max_tries`) plans a new artifact and applies it; the record of the earlier series
    does not stand in for it."""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": FULL})
    op = rig.operator(RivetApplyOperator, export="orders")
    rig.run(op, FakeTI(try_number=1, max_tries=2))
    again, _ = rig.run(op, FakeTI(try_number=2, max_tries=4))
    assert again["decision"] == "success" and len(applies(rig)) == 2 and applies(rig)[0][1] != applies(rig)[1][1]


# --- G2: two tries of one export at once ---------------------------------------------------------------------------------


TASK_PROCESS = r"""
import json, sys, tempfile
from pathlib import Path
sys.path.insert(0, sys.argv[2])
tmp = Path(sys.argv[1])
(tmp / "tmp").mkdir(exist_ok=True)
tempfile.tempdir = str(tmp / "tmp")
from conftest import FakeTI, Rig
from airflow_provider_rivet import operators
spec = json.loads(sys.argv[3])
rig = Rig(tmp)
rig.scenario(**spec["scenario"])
op = rig.operator(getattr(operators, spec["operator"]), **spec["kwargs"])
op.execute({"ti": FakeTI(max_tries=2), "run_id": FakeTI().run_id})
"""


def task_process(
    tmp: Path, scenario: dict, operator: str = "RivetApplyOperator", **kwargs: object
) -> tuple[subprocess.Popen, Rig]:
    """A task process: one operator run in a child Python (a zombie try, or one the test SIGKILLs as an OOM kill would),
    and a rig over its bin, config and state directory for the try the test runs beside it."""
    work = tmp / "task"
    work.mkdir()
    script = tmp / "task.py"
    script.write_text(TASK_PROCESS)
    tests = str(Path(__file__).resolve().parent)
    spec = {"operator": operator, "kwargs": kwargs or {"export": "orders"}, "scenario": scenario}
    proc = subprocess.Popen([sys.executable, str(script), str(work), tests, json.dumps(spec)], cwd=tests)
    rig = Rig.__new__(Rig)
    rig.tmp, rig.bin, rig.state_dir, rig.config, rig.records = work, work / "bin", work / "state", work / "cfg" / "pg.yaml", []
    return proc, rig


def test_g2_a_retry_while_the_earlier_try_still_runs_rivet_starts_nothing_and_is_retried(tmp_path: Path) -> None:
    """G2 (a): a zombie try's rivet apply still runs (held here by a sleeping fake) when the retry starts: the retry
    waits `lock_wait` seconds for the export's lock, starts no rivet process and ends retryable, never failed."""
    started = tmp_path / "started"
    proc, rig = task_process(tmp_path, {"flags": NEW_FLAGS, "apply": {"sleep": 60, "started": str(started), "summary": FULL}})
    pid = int(wait_for(started))
    try:
        retry, ti = rig.run(rig.operator(RivetApplyOperator, export="orders", lock_wait=1), FakeTI(try_number=2, max_tries=2))
        assert [c["argv"][0] for c in rig.calls()] == ["plan", "apply"], "the retry started a second rivet"
        assert type(retry) is AirflowException, retry
        assert "RIVET_AIRFLOW_EXPORT_BUSY" in str(retry)
        payload = ti.store[("task", "return_value")]
        assert payload["decision"] == "retry" and payload["busy"] == "RIVET_AIRFLOW_EXPORT_BUSY"
    finally:
        os.kill(pid, signal.SIGKILL)
        proc.wait()


def test_g2_a_load_of_another_table_does_not_wait_for_the_lock_of_this_one(tmp_path: Path) -> None:
    """G2 (a): the lock is the export's (and the table's, for a load with `--table`), not the state directory's."""
    started = tmp_path / "started"
    play = {"flags": NEW_FLAGS, "load:users:users": {"sleep": 60, "started": str(started)}}
    proc, rig = task_process(tmp_path, play, "RivetLoadOperator", export="users", table="users")
    pid = int(wait_for(started))
    try:
        other, _ = rig.run(rig.operator(RivetLoadOperator, export="users", table="other", lock_wait=1))
        same, _ = rig.run(rig.operator(RivetLoadOperator, export="users", table="users", lock_wait=1))
        assert isinstance(other, dict) and other["decision"] == "success", other
        assert type(same) is AirflowException and "RIVET_AIRFLOW_EXPORT_BUSY" in str(same)
    finally:
        os.kill(pid, signal.SIGKILL)
        proc.wait()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="PR_SET_PDEATHSIG exists on Linux only (CI is Linux)")
def test_g2_a_killed_task_process_stops_its_rivet_through_the_parent_death_signal(tmp_path: Path) -> None:
    """G2 (b): the task process is SIGKILLed (OOM, the runner alone reaped) while rivet runs: rivet was orphaned and ran
    on beside the retry. The parent-death signal sends it SIGTERM."""
    started, terminated = tmp_path / "started", tmp_path / "terminated"
    proc, _ = task_process(tmp_path, {"apply": {"sleep": 60, "started": str(started), "term_marker": str(terminated)}})
    pid = int(wait_for(started))
    proc.kill()
    proc.wait()
    try:
        assert wait_for(terminated, timeout=10) == str(pid), "rivet did not get SIGTERM when its task process died"
    finally:
        if alive(pid):
            os.kill(pid, signal.SIGKILL)


def test_g2_the_export_lock_passes_to_rivet_and_outlives_the_killed_task_process(tmp_path: Path) -> None:
    """G2 (a) / G3: the lock was the operator's own descriptor, freed the moment the task process died while its rivet
    kept running. Passed to rivet, it is held as long as rivet lives (here one that ignores SIGTERM, as an orphan the
    parent-death signal could not stop), and a retry started meanwhile starts no rivet and ends retryable."""
    started = tmp_path / "started"
    proc, rig = task_process(tmp_path, {"apply": {"sleep": 60, "started": str(started), "ignore_term": True}})
    pid = int(wait_for(started))
    proc.kill()
    proc.wait()
    try:
        lock = rig.state_dir / "airflow" / "locks" / "run" / "orders.lock"
        assert lock.exists(), "no export lock was taken for the apply"
        assert held(lock), "the export lock was freed with the task process while its rivet runs on"
        retry, _ = rig.run(rig.operator(RivetApplyOperator, export="orders", lock_wait=1), FakeTI(try_number=2, max_tries=2))
        assert type(retry) is AirflowException and "RIVET_AIRFLOW_EXPORT_BUSY" in str(retry), retry
        assert len(applies(rig)) == 1, "the retry started a second rivet beside the orphan"
    finally:
        os.kill(pid, signal.SIGKILL)


def test_g2_the_lock_is_released_by_an_explicit_unlock_not_by_close(rig: Rig) -> None:
    """G2 (a): a copy of the lock's descriptor in any other process (here a fork made while rivet runs, as a library or
    a sibling thread can make one) keeps a lock released by close alone held. The task unlocks explicitly once rivet
    has exited, so the next task takes the lock at once."""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": FULL})
    op = rig.operator(RivetApplyOperator, export="orders")
    forks: list[int] = []
    hold = op._hold

    def fork_beside(proc: object) -> None:
        pid = os.fork()
        if pid == 0:
            time.sleep(30)
            os._exit(0)
        forks.append(pid)
        hold(proc)

    op._hold = fork_beside
    try:
        payload, _ = rig.run(op)
        assert payload["decision"] == "success" and forks
        lock = rig.state_dir / "airflow" / "locks" / "run" / "orders.lock"
        assert lock.exists(), "no export lock was taken for the apply"
        assert not held(lock), "the lock stayed held by a copy of its descriptor after the task ended"
    finally:
        for pid in forks:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)


# --- G2 (c): rivet's own busy refusals are retried -----------------------------------------------------------------------


@pytest.mark.parametrize("contract", [False, True], ids=["exit_class", "contract_line"])
@pytest.mark.parametrize(
    ("code", "command", "kwargs"),
    [
        ("RIVET_STATE_RUN_IN_PROGRESS", RivetRunOperator, {"export": "users"}),
        ("RIVET_STATE_RUN_IN_PROGRESS", RivetApplyOperator, {"export": "users"}),
        ("RIVET_LOAD_TABLE_BUSY", RivetLoadOperator, {"export": "users", "table": "users"}),
    ],
)
def test_g2c_rivets_busy_refusals_are_retried(rig: Rig, code: str, command: type, kwargs: dict, contract: bool) -> None:
    """G2 (c): rivet main refuses a second run / load of one export while the first holds the lease, exit 5, with the
    remedy "wait, then repeat". Read as a refusal it failed the task for good; it is retried under `retries`."""
    line = {"error": "another run holds the lease", "exit_class": 5, "code": code}
    if contract:
        line |= {"class": "refusal", "retryable": False, "exit_code": 5}
    sub = command.command
    rig.scenario(flags=NEW_FLAGS, **{sub: {"exit": 5, "line": line}})
    exc, ti = rig.run(rig.operator(command, **kwargs))
    assert type(exc) is AirflowException, exc
    payload = ti.store[("task", "return_value")]
    assert payload["decision"] == "retry" and payload["error"]["code"] == code and payload["error"]["retryable"] is True


def test_g2c_another_refusal_is_still_not_retried(rig: Rig) -> None:
    """G2 (c) negative: only the busy codes changed; any other exit-5 refusal still fails without retry."""
    line = {"error": "stale", "exit_class": 5, "code": "RIVET_STATE_CURSOR_OWNER_MISMATCH"}
    rig.scenario(flags=NEW_FLAGS, run={"exit": 5, "line": line})
    exc, _ = rig.run(rig.operator(RivetRunOperator, export="users"))
    assert type(exc) is AirflowFailException
