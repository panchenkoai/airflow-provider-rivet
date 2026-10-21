"""The export lock at its edges: which file a command locks and what it says it covers, the wait for a held lock (its
record, its deadline, a stop while waiting), its release, and the reaping of a stopped rivet process. Mutant ids are
the mutmut names of airflow_provider_rivet/invocation.py."""

from __future__ import annotations

import fcntl
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow_provider_rivet import invocation
from airflow_provider_rivet.invocation import Busy, Stopped, hashed_name, path_name


def lock_session(tmp_path: Path, command: str, export: str | None, table: str | None = None, table_flag: bool = True):
    config = tmp_path / "rivet.yaml"
    config.write_text("")
    caps = SimpleNamespace(has_flag=lambda cmd, flag: table_flag and flag == "--table")
    (tmp_path / "state").mkdir(exist_ok=True)
    return SimpleNamespace(
        pf=SimpleNamespace(state_dir=tmp_path / "state", caps=caps),
        req=SimpleNamespace(command=command, export=export, table=table, config=str(config)),
    )


def test_plan_and_a_task_without_a_state_directory_take_no_lock(tmp_path: Path) -> None:
    """_lock_path 4/5: `plan` only reads."""
    assert invocation._lock_path(lock_session(tmp_path, "plan", "orders")) is None
    s = lock_session(tmp_path, "run", "orders")
    s.pf.state_dir = None
    assert invocation._lock_path(s) is None


def test_each_command_locks_its_own_file_and_names_what_it_covers(tmp_path: Path) -> None:
    """_lock_path 18-25/30/31/43-51/54-59/68-73: run/apply share `locks/run/<export>`, load and compact narrowed to a
    table lock `locks/table/<export, table>`, otherwise `locks/<export>`; without an export the config's own name."""
    locks = tmp_path / "state" / "airflow" / "locks"
    whole = hashed_name("config", str((tmp_path / "rivet.yaml").resolve()))
    cases = {
        ("run", "orders", None, True): (locks / "run" / "orders.lock", "export `orders`"),
        ("apply", None, None, True): (locks / "run" / f"{whole}.lock", "the config"),
        ("load", "orders", "users", True): (
            locks / "table" / f"{hashed_name('orders', 'users')}.lock",
            "table `users` of `orders`",
        ),
        ("compact", "orders", "users", True): (
            locks / "table" / f"{hashed_name('orders', 'users')}.lock",
            "table `users` of `orders`",
        ),
        ("load", None, "users", True): (locks / "table" / f"{hashed_name('', 'users')}.lock", "table `users` of `None`"),
        ("compact", "orders", "users", False): (locks / f"{path_name('orders')}.lock", "export `orders`"),
        ("load", None, None, True): (locks / f"{whole}.lock", "the config"),
    }
    for (command, export, table, flag), want in cases.items():
        assert invocation._lock_path(lock_session(tmp_path, command, export, table, flag)) == want, (command, export, table)


def test_a_config_reached_through_a_symlink_locks_by_the_path_the_dag_gave(tmp_path: Path) -> None:
    """_lock_path: git-sync swaps `repo -> rev-<sha>`; the lock of a task without an export stays one file across syncs."""
    s = lock_session(tmp_path, "apply", None)
    (tmp_path / "repo").symlink_to(tmp_path)
    s.req.config = str(tmp_path / "repo" / "rivet.yaml")
    want = tmp_path / "state" / "airflow" / "locks" / "run" / f"{hashed_name('config', s.req.config)}.lock"
    assert invocation._lock_path(s) == (want, "the config")


class Clock:
    """`time` for the lock wait: monotonic stands still unless `sleep` moves it."""

    def __init__(self) -> None:
        self.now, self.sleeps = 100.0, []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def held(tmp_path: Path):
    """A lock file another open file description holds; the descriptor the task waits on."""
    path = tmp_path / "x.lock"
    holder = os.open(path, os.O_RDWR | os.O_CREAT)
    fcntl.flock(holder, fcntl.LOCK_EX)
    fd = os.open(path, os.O_RDWR)
    yield path, fd
    os.close(fd)
    os.close(holder)


def wait_session(lock_wait: int, stopped=None):
    events: list[dict] = []
    s = SimpleNamespace(req=SimpleNamespace(lock_wait=lock_wait, command="load"), stopped=stopped, emit=events.append)
    return s, events


def test_a_lock_held_past_lock_wait_is_busy_after_polling_and_the_wait_is_recorded_once(
    held, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_wait_for_lock 20/21/27-29/35-37 and Busy 2/3: one record however many polls; Busy names the lock, the unit
    and the seconds waited."""
    path, fd = held
    clock = Clock()
    monkeypatch.setattr(invocation, "time", clock)
    s, events = wait_session(1)
    with pytest.raises(Busy) as busy:
        invocation._wait_for_lock(s, fd, path, "export `orders`")
    assert events == [{"event": "rivet.lock.wait", "lock": str(path), "unit": "export `orders`", "lock_wait": 1}]
    assert len(clock.sleeps) == 5 and set(clock.sleeps) == {invocation.LOCK_POLL}
    assert str(busy.value) == (
        f"[RIVET_AIRFLOW_EXPORT_BUSY] the lock {path} of export `orders` stayed held for 1 s: another try of this task, "
        "or a rivet process an earlier try left running, still works on it. rivet was not started; the task is retried"
    )


def test_a_lock_wait_of_zero_is_busy_at_once(held, monkeypatch: pytest.MonkeyPatch) -> None:
    """_wait_for_lock 7/34: no poll when the deadline is now (`>=`, and no one-second floor)."""
    path, fd = held
    clock = Clock()
    monkeypatch.setattr(invocation, "time", clock)
    with pytest.raises(Busy):
        invocation._wait_for_lock(wait_session(0)[0], fd, path, "u")
    assert clock.sleeps == []


def test_a_stop_while_waiting_raises_stopped_naming_the_step(held, monkeypatch: pytest.MonkeyPatch) -> None:
    """_wait_for_lock 17/18 and Stopped 1."""
    path, fd = held
    monkeypatch.setattr(invocation, "time", Clock())
    with pytest.raises(Stopped) as stopped:
        invocation._wait_for_lock(wait_session(60, stopped=lambda: True)[0], fd, path, "u")
    assert stopped.value.step == "load"
    assert str(stopped.value) == "the task was stopped before rivet load started; rivet load was not run"


def test_releasing_the_lock_forgets_its_descriptor(tmp_path: Path) -> None:
    """_release_lock 1."""
    fd = os.open(tmp_path / "l", os.O_RDWR | os.O_CREAT)
    fcntl.flock(fd, fcntl.LOCK_EX)
    s = SimpleNamespace(lock_fd=fd)
    invocation._release_lock(s, fd)
    assert s.lock_fd is None
    os.close(fd)


class SlowProc:
    """A process that outlives `waits` polls; records each timeout and the kill."""

    def __init__(self, clock: Clock, waits: int) -> None:
        self.clock, self.waits, self.timeouts, self.kills = clock, waits, [], []

    def wait(self, timeout: float) -> int:
        self.timeouts.append(timeout)
        if self.waits == 0:
            return -9
        self.waits -= 1
        self.clock.now += 10.0
        raise subprocess.TimeoutExpired("rivet", timeout)

    def kill(self) -> None:
        self.kills.append(self.clock.now)


def test_a_stopped_process_is_killed_once_it_outlives_the_grace_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    """Running.wait 3/6/7: polled every 0.2 s; killed at the poll where `now - stop_at` reaches STOP_GRACE (30 s:
    the third 10-second poll), not before."""
    clock = Clock()
    clock.now = 1000.0
    monkeypatch.setattr(invocation, "time", clock)
    proc = SlowProc(clock, waits=3)
    running = invocation.Running(proc)  # type: ignore[arg-type]
    running.stop_at = 1000.0
    assert running.wait() == -9
    assert proc.kills == [1000.0 + invocation.STOP_GRACE] and set(proc.timeouts) == {0.2}


def test_a_lock_taken_after_a_wait_records_how_long_the_wait_took(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """_wait_for_lock (#63, #61): the wait's end is recorded once, with the seconds from its start to the lock, so the
    task log can tell the wait from rivet's work."""
    path = tmp_path / "x.lock"
    holder = os.open(path, os.O_RDWR | os.O_CREAT)
    fcntl.flock(holder, fcntl.LOCK_EX)
    fd = os.open(path, os.O_RDWR)
    clock = Clock()

    def sleep(seconds: float) -> None:
        Clock.sleep(clock, seconds)
        if len(clock.sleeps) == 3:  # the other holder lets go during the third poll
            fcntl.flock(holder, fcntl.LOCK_UN)

    clock.sleep = sleep  # type: ignore[method-assign]
    monkeypatch.setattr(invocation, "time", clock)
    s, events = wait_session(60)
    try:
        invocation._wait_for_lock(s, fd, path, "export `orders`")
    finally:
        os.close(fd)
        os.close(holder)
    assert len(clock.sleeps) == 3
    assert events == [
        {"event": "rivet.lock.wait", "lock": str(path), "unit": "export `orders`", "lock_wait": 60},
        {"event": "rivet.lock.acquired", "lock": str(path), "unit": "export `orders`", "waited": clock.now - 100.0},
    ]
    assert events[1]["waited"] == pytest.approx(3 * invocation.LOCK_POLL)


def test_a_free_lock_is_taken_without_a_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """_wait_for_lock: no wait, no record of one."""
    monkeypatch.setattr(invocation, "time", Clock())
    fd = os.open(tmp_path / "free.lock", os.O_RDWR | os.O_CREAT)
    s, events = wait_session(60)
    try:
        invocation._wait_for_lock(s, fd, tmp_path / "free.lock", "u")
    finally:
        os.close(fd)
    assert events == []
