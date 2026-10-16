"""`invocation.invoke` driven directly (no operator) against the fake binary: the export lock and its wait, what a
started rivet inherits, the events each step logs, and how a process, its result file and its units become one
outcome. Each group names the mutants of `airflow_provider_rivet/invocation.py` it exists to kill (mutmut ids)."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import stat
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from airflow.exceptions import AirflowException, AirflowFailException
from conftest import NEW_FLAGS, SECRET, FakeTI, Rig, fixture

from airflow_provider_rivet import invocation
from airflow_provider_rivet.invocation import BUSY_CODE, LOCK_POLL, Busy, Outcome, Request, Stopped, invoke
from airflow_provider_rivet.operators import RivetApplyOperator, RivetLoadOperator, RivetRunOperator
from airflow_provider_rivet.preflight import PreflightRefusal
from airflow_provider_rivet.result import ErrorObject

FULL = {"per_export": [{"export_name": "orders", "status": "success", "mode": "full", "rows": 7, "files": 2}]}
RETRYABLE_LINE = {"error": "connection reset by peer", "exit_class": 2}
REFUSAL_LINE = {"error": "refused", "exit_class": 5, "code": "RIVET_X"}
GENERIC_EXIT_0 = {"code": None, "kind": None, "class": "generic", "exit_code": 1, "retryable": False, "action": None}
XCOM_KEYS = {
    "command", "decision", "units", "error", "degraded", "exit_status", "signal", "stderr_path", "skip_reason",
    "state_marker", "rivet_version",
}  # fmt: skip


def hashed(*parts: Any) -> str:
    """`hashed_name` written out independently: the lock, log and scratch names two provider versions must share."""
    texts = [str(part) for part in parts]
    readable = "__".join(re.sub(r"[^A-Za-z0-9_.-]+", "_", text) for text in texts)[:80]
    return f"{readable}-{hashlib.sha256(json.dumps(texts).encode()).hexdigest()[:12]}"


def request(rig: Rig, command: str = "run", **kwargs: Any) -> Request:
    """A request for the rig's fake binary, config and state directory."""
    kwargs.setdefault("env", {"RIVET_PG_URL": SECRET})
    kwargs.setdefault("state_dir", str(rig.state_dir))
    kwargs.setdefault("deployment", "local")
    return Request(command=command, config=str(rig.config), rivet_bin=str(rig.bin / "rivet"), **kwargs)


def attempt(
    rig: Rig, command: str = "run", stopped: Any = None, on_process: Any = None, **kwargs: Any
) -> tuple[Outcome | Exception, list[dict]]:
    """Invoke one task; (its outcome or the exception it raised, the events it emitted)."""
    events: list[dict] = []
    try:
        return invoke(request(rig, command, **kwargs), events.append, on_process, stopped), events
    except Exception as exc:
        return exc, events


def of(events: list[dict], name: str) -> list[dict]:
    return [e for e in events if e["event"] == name]


def open_fds() -> list[int]:
    """This process's open descriptors (a leaked lock descriptor shows up here)."""
    return sorted(int(fd) for fd in os.listdir("/dev/fd"))


def free(lock: Path) -> bool:
    """True when no open file description holds the lock: a fresh one takes it at once."""
    fd = os.open(lock, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    finally:
        os.close(fd)


@contextlib.contextmanager
def holding(lock: Path) -> Iterator[int]:
    """Hold the lock file through a descriptor of the test's own, as another try of the task would."""
    lock.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        yield fd
    finally:
        os.close(fd)


class Clock:
    """`invocation.time` with a clock the test moves: each `sleep` records its argument and moves 0.25 s, so the
    polls before the deadline are counted exactly (0.25 is exact in binary, the deadline is met, not straddled)."""

    def __init__(self, on_sleep: Any = None) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []
        self.on_sleep = on_sleep

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += 0.25
        if self.on_sleep:
            self.on_sleep(len(self.sleeps))


# --- the export lock's wait (`_take_export_lock`, `_release_export_lock`; `_export_lock` was not mutated) -------------


def test_a_lock_held_for_lock_wait_seconds_is_busy_after_polling_and_no_rivet_starts(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The wait polls every LOCK_POLL seconds until `lock_wait` has passed, logs one `rivet.lock.wait`, then raises the
    retryable Busy with nothing held: no rivet started, the descriptor closed."""
    clock = Clock()
    monkeypatch.setattr(invocation, "time", clock)
    lock = rig.state_dir / "airflow" / "locks" / "run" / "orders.lock"
    before = open_fds()
    with holding(lock):
        exc, events = attempt(rig, "run", export="orders", lock_wait=1)
    assert type(exc) is Busy and exc.code == BUSY_CODE == "RIVET_AIRFLOW_EXPORT_BUSY"
    assert str(exc) == (
        f"[RIVET_AIRFLOW_EXPORT_BUSY] the lock {lock} of export `orders` stayed held for 1 s: another try of this "
        "task, or a rivet process an earlier try left running, still works on it. rivet was not started; the task is "
        "retried"
    )
    assert clock.sleeps == [LOCK_POLL] * 4 and LOCK_POLL == 0.2, "polled until the deadline, not past it"
    assert of(events, "rivet.lock.wait") == [
        {"event": "rivet.lock.wait", "lock": str(lock), "unit": "export `orders`", "lock_wait": 1}
    ]
    assert rig.calls() == [] and not of(events, "rivet.start")
    assert open_fds() == before, "the lock descriptor of a busy task was left open"
    assert free(lock)


@pytest.mark.parametrize("lock_wait", [0, -5])
def test_a_lock_wait_of_zero_or_less_gives_up_at_the_first_try(rig: Rig, monkeypatch: pytest.MonkeyPatch, lock_wait: int) -> None:
    clock = Clock()
    monkeypatch.setattr(invocation, "time", clock)
    lock = rig.state_dir / "airflow" / "locks" / "run" / "orders.lock"
    with holding(lock):
        exc, events = attempt(rig, "run", export="orders", lock_wait=lock_wait)
    assert type(exc) is Busy and f"stayed held for {lock_wait} s" in str(exc)
    assert clock.sleeps == []
    assert [e["lock_wait"] for e in of(events, "rivet.lock.wait")] == [lock_wait]


def test_a_lock_freed_during_the_wait_is_taken_and_rivet_inherits_it(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """The holder lets go after the second poll: the task takes the lock, logs the wait once, runs rivet with the
    lock's descriptor, and releases it by an explicit unlock when the step has ended."""
    lock = rig.state_dir / "airflow" / "locks" / "run" / "orders.lock"
    lock.parent.mkdir(parents=True)
    before = open_fds()
    holder = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(holder, fcntl.LOCK_EX)

    def let_go(polls: int) -> None:
        if polls == 2:
            fcntl.flock(holder, fcntl.LOCK_UN)

    clock = Clock(let_go)
    monkeypatch.setattr(invocation, "time", clock)
    rig.scenario(flags=NEW_FLAGS, record_fds=True, run={"summary": FULL})
    try:
        out, events = attempt(rig, "run", export="orders", lock_wait=10)
    finally:
        os.close(holder)
    assert isinstance(out, Outcome) and out.decision == "success", out
    assert clock.sleeps == [LOCK_POLL, LOCK_POLL]
    assert len(of(events, "rivet.lock.wait")) == 1
    inherited = {(dev, ino) for fd, dev, ino in rig.calls()[0]["fds"] if fd != 0}
    info = os.stat(lock)
    assert inherited == {(info.st_dev, info.st_ino)}, "rivet must hold the export lock's descriptor and nothing else"
    assert open_fds() == before and free(lock)


def test_a_stop_while_waiting_for_the_lock_raises_stopped_and_starts_nothing(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    clock = Clock()
    monkeypatch.setattr(invocation, "time", clock)
    lock = rig.state_dir / "airflow" / "locks" / "run" / "orders.lock"
    before = open_fds()
    with holding(lock):
        exc, events = attempt(rig, "run", stopped=lambda: True, export="orders", lock_wait=10)
    assert type(exc) is Stopped and exc.step == "run"
    assert str(exc) == "the task was stopped before rivet run started; rivet run was not run"
    assert clock.sleeps == [] and not of(events, "rivet.lock.wait") and rig.calls() == []
    assert open_fds() == before


def test_a_stop_that_never_comes_does_not_end_the_wait(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    clock = Clock()
    monkeypatch.setattr(invocation, "time", clock)
    lock = rig.state_dir / "airflow" / "locks" / "run" / "orders.lock"
    with holding(lock):
        exc, _ = attempt(rig, "run", stopped=lambda: False, export="orders", lock_wait=1)
    assert type(exc) is Busy and len(clock.sleeps) == 4


def lock_session(state: Path, **req: Any) -> Any:
    """The parts of a session `_take_export_lock` / `_release_export_lock` read."""
    req = {"command": "run", "export": "orders", "table": None, "config": str(state / "pg.yaml"), "lock_wait": 0, **req}
    caps = SimpleNamespace(has_flag=lambda sub, flag: False)
    return SimpleNamespace(
        pf=SimpleNamespace(state_dir=state, caps=caps), req=SimpleNamespace(**req), emit=lambda e: None, stopped=None,
        lock_fd=None,
    )  # fmt: skip


def test_the_lock_descriptor_is_private_close_on_exec_and_forgotten_once_released(tmp_path: Path) -> None:
    s = lock_session(tmp_path)
    old = os.umask(0)
    try:
        fd = invocation._take_export_lock(s)
    finally:
        os.umask(old)
    lock = tmp_path / "airflow" / "locks" / "run" / "orders.lock"
    assert s.lock_fd == fd and fd is not None and not free(lock)
    assert not os.get_inheritable(fd), "only `pass_fds` hands the lock to rivet; no other child inherits it"
    assert stat.S_IMODE(os.fstat(fd).st_mode) == 0o600 and os.fstat(fd).st_ino == os.stat(lock).st_ino
    invocation._release_export_lock(s, fd)
    assert s.lock_fd is None and free(lock)
    with pytest.raises(OSError):
        os.fstat(fd)


def test_a_plan_session_locks_nothing_and_releases_nothing(tmp_path: Path) -> None:
    s = lock_session(tmp_path, command="plan")
    assert invocation._take_export_lock(s) is None and s.lock_fd is None
    invocation._release_export_lock(s, None)
    assert not (tmp_path / "airflow").exists()


def whole(rig: Rig) -> str:
    return hashed("config", str(rig.config.resolve()))


# (command, request kwargs, binary flags, lock path below <state>/airflow/locks, the unit the wait names)
LOCKS = [
    ("run", {"export": "orders"}, {}, lambda r: "run/orders.lock", "export `orders`"),
    ("run", {}, {}, lambda r: f"run/{whole(r)}.lock", "the config"),
    ("cdc_run", {"export": "orders_cdc"}, {}, lambda r: "run/orders_cdc.lock", "export `orders_cdc`"),
    ("apply", {"export": "orders"}, {}, lambda r: "run/orders.lock", "export `orders`"),
    ("load", {"export": "users", "table": "users"}, NEW_FLAGS, lambda r: f"table/{hashed('users', 'users')}.lock", "table `users` of `users`"),
    ("load", {"table": "t"}, NEW_FLAGS, lambda r: f"table/{hashed('', 't')}.lock", "table `t` of `None`"),
    ("load", {"export": "users", "table": "users"}, {}, lambda r: "users.lock", "export `users`"),
    ("load", {}, {}, lambda r: f"{whole(r)}.lock", "the config"),
    ("compact", {"export": "orders"}, {}, lambda r: "orders.lock", "export `orders`"),
    ("compact", {"export": "orders", "table": "orders"}, NEW_FLAGS, lambda r: f"table/{hashed('orders', 'orders')}.lock", "table `orders` of `orders`"),
]  # fmt: skip


@pytest.mark.parametrize(("command", "kwargs", "flags", "where", "unit"), LOCKS, ids=[f"{c[0]}-{i}" for i, c in enumerate(LOCKS)])
def test_each_task_waits_on_the_lock_file_its_unit_names(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, command: str, kwargs: dict, flags: dict, where: Any, unit: str
) -> None:
    """The lock layout is what two provider versions on one worker must agree on: run / cdc_run / apply share
    `locks/run/<export>`, load / compact take `locks/table/<export, table>` where the binary narrows to a table, else
    `locks/<export>`; with no export the config's hashed path names it. Holding exactly that file makes the task busy
    (`_lock_path` mutants 18-25, 30, 31, 43-59, 68-73)."""
    monkeypatch.setattr(invocation, "time", Clock())
    rig.scenario(flags=flags)
    lock = rig.state_dir / "airflow" / "locks" / where(rig)
    with holding(lock):
        exc, events = attempt(rig, command, lock_wait=0, **kwargs)
    assert type(exc) is Busy, exc
    assert of(events, "rivet.lock.wait") == [{"event": "rivet.lock.wait", "lock": str(lock), "unit": unit, "lock_wait": 0}]
    assert f"the lock {lock} of {unit} stayed held" in str(exc)


def test_two_configs_in_one_state_directory_take_two_whole_config_locks(rig: Rig) -> None:
    other = rig.config_dir / "other.yaml"
    other.write_text(rig.config.read_text())
    with holding(rig.state_dir / "airflow" / "locks" / "run" / f"{whole(rig)}.lock"):
        events: list[dict] = []
        out = invoke(Request("run", str(other), {"RIVET_PG_URL": SECRET}, rivet_bin=str(rig.bin / "rivet"),
                             state_dir=str(rig.state_dir), deployment="local", lock_wait=0), events.append)  # fmt: skip
    assert out.decision == "success"
    assert (rig.state_dir / "airflow" / "locks" / "run" / f"{hashed('config', str(other.resolve()))}.lock").exists()


def test_a_plan_takes_no_lock(rig: Rig) -> None:
    rig.scenario(plan_list=[])
    attempt(rig, "plan", plan_file=str(rig.tmp / "layout.json"))
    assert not (rig.state_dir / "airflow" / "locks").exists()


# --- a stop before a step (D40), in-process --------------------------------------------------------------------------


def test_a_stop_before_the_first_step_starts_no_rivet_and_frees_the_lock(rig: Rig) -> None:
    """`Stopped` was raised only inside the D40 child processes; here the step's stop check runs in this process
    (`_Session.step` 2/3, `Stopped.__init__`, `_invoke` 39/44, `invoke` 6, `_Session.__init__` 5)."""
    exc, events = attempt(rig, "apply", stopped=lambda: True, export="orders")
    assert type(exc) is Stopped and exc.step == "plan"
    assert str(exc) == "the task was stopped before rivet plan started; rivet plan was not run"
    assert rig.calls() == [] and not of(events, "rivet.start")
    assert free(rig.state_dir / "airflow" / "locks" / "run" / "orders.lock")


def test_an_operator_stopped_before_execute_is_retried_with_the_step_it_did_not_start(rig: Rig) -> None:
    op = rig.operator(RivetApplyOperator, export="orders")
    op.on_kill()
    exc, ti = rig.run(op)
    assert type(exc) is AirflowException
    payload = ti.store[("task", "return_value")]
    assert payload["decision"] == "retry" and payload["stopped_before"] == "plan" and rig.calls() == []


# --- what a started rivet inherits, and the events of a step ---------------------------------------------------------


def test_rivet_reads_dev_null_and_inherits_only_the_lock(rig: Rig) -> None:
    """stdin is /dev/null even when the worker's own stdin is something else (`_Session.step` 32/40), and the only
    descriptor passed on is the export lock (`_Session.step` 43); a plan has none to pass."""
    rig.scenario(flags=NEW_FLAGS, record_fds=True, plan_list=[])
    read, write = os.pipe()
    saved = os.dup(0)
    os.dup2(read, 0)
    try:
        attempt(rig, "run", export="orders")
        attempt(rig, "plan", plan_file=str(rig.tmp / "layout.json"))
    finally:
        os.dup2(saved, 0)
        for fd in (saved, read, write):
            os.close(fd)
    null = os.stat(os.devnull)
    lock = os.stat(rig.state_dir / "airflow" / "locks" / "run" / "orders.lock")
    run, plan = rig.calls()
    assert {fd: (dev, ino) for fd, dev, ino in run["fds"]} == {
        0: (null.st_dev, null.st_ino),
        **{fd: (lock.st_dev, lock.st_ino) for fd, _, _ in run["fds"] if fd != 0},
    }
    assert len(run["fds"]) == 2
    assert plan["fds"] == [[0, null.st_dev, null.st_ino]]


def test_a_step_logs_its_start_its_end_and_its_streamed_stderr(rig: Rig) -> None:
    """The `rivet.start`, `rivet.step` and `rivet.stderr` records are a log contract (`_Session.step` 15-18, 68, 69)."""
    rig.scenario(flags=NEW_FLAGS, leak_env=[], run={"exit": 2, "stderr": "warming up", "line": RETRYABLE_LINE})
    out, events = attempt(rig, "run", export="orders", stream_stderr=True)
    [start] = of(events, "rivet.start")
    assert start == {
        "event": "rivet.start",
        "step": "run",
        "argv": [str(rig.bin / "rivet"), *rig.argv("run")],
        "stderr_path": out.stderr_path,
    }
    assert out.stderr_path.endswith(".run.stderr.log")
    assert of(events, "rivet.step") == [{"event": "rivet.step", "step": "run", "exit_status": 2, "signal": None}]
    streamed = of(events, "rivet.stderr")
    assert streamed[0] == {"event": "rivet.stderr", "step": "run", "line": "warming up"}
    assert {e["step"] for e in streamed} == {"run"} and len(streamed) == 2


def test_a_killed_step_logs_its_signal_and_no_exit_status(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"signal": "SIGKILL"})
    out, events = attempt(rig, "run", export="orders")
    assert of(events, "rivet.step") == [{"event": "rivet.step", "step": "run", "exit_status": None, "signal": 9}]
    assert (out.exit_status, out.signal, out.decision) == (None, 9, "retry_crashed")
    assert "result_malformed" not in out.degraded, "a kill is not a malformed result (`_Session.classify` 20)"


# --- a failure is never graded by the summary alone (`_run` 39, `_apply` 218) ----------------------------------------


@pytest.mark.parametrize(
    ("line", "raised", "decision", "error"),
    [
        (RETRYABLE_LINE, AirflowException, "retry", {**GENERIC_EXIT_0, "class": "retryable", "exit_code": 2, "retryable": True}),
        (REFUSAL_LINE, AirflowFailException, "fail", {**GENERIC_EXIT_0, "class": "refusal", "exit_code": 5, "code": "RIVET_X"}),
    ],
    ids=["exit-2", "exit-5"],
)
@pytest.mark.parametrize("operator", [RivetRunOperator, RivetApplyOperator])
def test_a_failed_process_beside_an_all_success_summary_fails_the_task(
    rig: Rig, operator: type, line: dict, raised: type, decision: str, error: dict
) -> None:
    """rivet exited non-zero with its error line, and its summary lists the export as `success` (a failure after the
    units were written, e.g. finalising state): the process error decides, never the units."""
    rig.scenario(flags=NEW_FLAGS, **{operator.command: {"exit": line["exit_class"], "line": line, "summary": FULL}})
    exc, ti = rig.run(rig.operator(operator, export="orders"))
    assert type(exc) is raised, exc
    payload = ti.store[("task", "return_value")]
    assert payload["decision"] == decision and payload["error"] == error
    assert payload["exit_status"] == line["exit_class"]
    assert [(u["export"], u["status"]) for u in payload["units"]] == [("orders", "success")]
    assert sorted(payload["degraded"]) == ["error_object", "state_url_sqlite"]


# --- the outcome and its events (`_Session.outcome`, `Outcome.to_xcom`) -----------------------------------------------


def test_the_outcome_names_its_command_version_and_exit_and_logs_one_result_record(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"exit": 2, "line": RETRYABLE_LINE})
    out, events = attempt(rig, "run", export="orders")
    payload = out.to_xcom()
    assert set(payload) == XCOM_KEYS
    assert (payload["command"], payload["rivet_version"], payload["exit_status"]) == ("run", "0.31.0", 2)
    assert of(events, "rivet.result") == [
        {
            "event": "rivet.result",
            "command": "run",
            "decision": "retry",
            "degraded": ["error_object", "state_url_sqlite"],
            "exit_status": 2,
            "signal": None,
            "stderr_path": payload["stderr_path"],
        }
    ]


def test_a_unit_record_carries_its_skip_reason(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, load={"summary": fixture("load_result.json")})
    out, events = attempt(rig, "load", export="countries", table="countries")
    assert out.decision == "success"
    assert [(e["table"], e["skip_reason"]) for e in of(events, "rivet.unit")] == [("countries", "up_to_date")]


def test_the_preflight_warnings_are_logged_with_their_text(rig: Rig) -> None:
    out, events = attempt(rig, "run", export="orders")
    [warning] = of(events, "rivet.warning")
    assert warning["warning"] == "sqlite_single_host" and warning["text"].startswith(f"SQLite state in {rig.state_dir}")


def test_an_unknown_command_is_refused_by_name() -> None:
    with pytest.raises(ValueError, match=r"^unknown rivet command 'bogus'$"):
        invoke(Request("bogus", "c.yaml", {}), lambda e: None)


def test_a_cdc_run_task_is_cdc_whatever_mode_its_export_names(rig: Rig) -> None:
    """`cdc=` is what makes a cdc_run task of a `mode: full` export CDC for the pod guard (`_invoke` 8, 17, 21, 22)."""
    env = {"RIVET_PG_URL": SECRET, "RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"}
    exc, _ = attempt(rig, "cdc_run", export="orders", env=env, deployment="pod", state_dir=None)
    assert isinstance(exc, PreflightRefusal) and exc.code == "RIVET_AIRFLOW_CDC_ON_POD"
    out, _ = attempt(rig, "run", export="orders", env=env, deployment="pod", state_dir=None)
    assert isinstance(out, Outcome) and out.decision == "success"


# --- units of run / apply (`_run_units`, `_entries`, `_Session.malformed/status/unit_error`) ---------------------------


def test_an_export_run_with_an_empty_summary_is_one_successful_unit(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": []}})
    out, _ = attempt(rig, "run", export="orders")
    assert [u.to_xcom() for u in out.units] == [
        {"export": "orders", "table": None, "status": "success", "run_id": None, "rows": None, "files": None,
         "stop_reason": None, "error": None}
    ]  # fmt: skip


def test_a_whole_config_run_with_an_empty_summary_has_no_unit_and_a_refused_one_has_one(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": []}})
    out, _ = attempt(rig, "run")
    assert out.units == [] and out.decision == "success"
    rig.scenario(flags=NEW_FLAGS, run={"exit": 5, "line": REFUSAL_LINE})
    out, _ = attempt(rig, "run")
    assert [(u.export, u.status, u.error) for u in out.units] == [(None, "failed", out.error)]
    assert out.error.code == "RIVET_X"


def test_a_refused_export_run_without_a_summary_is_one_failed_unit(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"exit": 5, "line": REFUSAL_LINE})
    out, _ = attempt(rig, "run", export="orders")
    assert [(u.export, u.status, u.error) for u in out.units] == [("orders", "failed", out.error)]
    assert "exit_zero_failed_unit" not in out.degraded


def test_a_wrong_typed_field_is_dropped_and_recorded(rig: Rig) -> None:
    entry = {"export_name": "orders", "status": "success", "mode": "full", "rows": "7", "files": 2, "run_id": 3}
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": [entry]}})
    out, _ = attempt(rig, "run", export="orders")
    [unit] = out.units
    assert (unit.rows, unit.files, unit.run_id) == (None, 2, None)
    assert out.degraded == ["result_malformed", "state_url_sqlite"]


def test_a_status_outside_the_contract_is_a_failed_unit_and_recorded(rig: Rig) -> None:
    entry = {"export_name": "orders", "status": "bogus", "mode": "full", "rows": 1, "files": 1}
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": [entry]}})
    out, _ = attempt(rig, "run", export="orders")
    assert [u.status for u in out.units] == ["failed"]
    assert out.error.to_xcom() == GENERIC_EXIT_0
    assert out.degraded == ["exit_zero_failed_unit", "result_malformed", "state_url_sqlite"], "per_unit_error: one unit"


@pytest.mark.parametrize(
    ("per_export", "units"),
    [([1, {"export_name": "orders", "status": "success"}], [("orders", "success")]), (7, [("orders", "success")])],
    ids=["junk-entry", "not-a-list"],
)
def test_a_summary_whose_entries_are_not_objects_is_recorded(rig: Rig, per_export: Any, units: list) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": per_export}})
    out, _ = attempt(rig, "run", export="orders")
    assert [(u.export, u.status) for u in out.units] == units
    assert out.degraded == ["result_malformed", "state_url_sqlite"]


def test_one_failed_unit_with_its_own_valid_error_and_junk_failures_records_only_the_junk(rig: Rig) -> None:
    own = {"class": "refusal", "retryable": False, "exit_code": 5, "code": "RIVET_OWN", "kind": None, "action": None}
    line = {**REFUSAL_LINE, "class": "refusal", "retryable": False, "exit_code": 5, "failures": ["junk"]}
    entry = {"export_name": "orders", "status": "failed", "mode": "full", "error": own}
    rig.scenario(flags=NEW_FLAGS, run={"exit": 5, "line": line, "summary": {"per_export": [entry]}})
    out, _ = attempt(rig, "run", export="orders")
    assert out.units[0].error.code == "RIVET_OWN"
    assert out.degraded == ["result_malformed", "state_url_sqlite"]


def test_an_unparsable_own_error_falls_back_to_the_process_error_and_is_recorded(rig: Rig) -> None:
    entry = {"export_name": "orders", "status": "failed", "mode": "full", "error": {"class": "refusal"}}
    rig.scenario(flags=NEW_FLAGS, run={"exit": 5, "line": REFUSAL_LINE, "summary": {"per_export": [entry]}})
    out, _ = attempt(rig, "run", export="orders")
    assert out.units[0].error == out.error and out.error.code == "RIVET_X"
    assert out.degraded == ["error_object", "result_malformed", "state_url_sqlite"]


def test_a_unit_without_its_own_error_records_nothing_malformed(rig: Rig) -> None:
    entry = {"export_name": "orders", "status": "failed", "mode": "full"}
    rig.scenario(flags=NEW_FLAGS, run={"exit": 5, "line": REFUSAL_LINE, "summary": {"per_export": [entry]}})
    out, _ = attempt(rig, "run", export="orders")
    assert out.degraded == ["error_object", "state_url_sqlite"], "no own error is not a malformed one (unit_error 8)"


def test_a_failed_run_without_its_summary_is_not_result_missing(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"exit": 2, "line": RETRYABLE_LINE, "no_summary": True})
    out, _ = attempt(rig, "run", export="orders")
    assert out.degraded == ["error_object", "state_url_sqlite"], "D18 is about exit 0 only (result_file 9)"


def test_one_failed_unit_at_exit_0_has_no_per_unit_degradation(rig: Rig) -> None:
    entry = {"export_name": "orders", "status": "failed", "mode": "full"}
    rig.scenario(flags=NEW_FLAGS, run={"summary": {"per_export": [entry]}})
    out, _ = attempt(rig, "run", export="orders")
    assert out.degraded == ["exit_zero_failed_unit", "state_url_sqlite"]


# --- load / compact units (`_load`) ------------------------------------------------------------------------------------

LOADED = {"export": "users", "table": "users", "status": "loaded", "rows": 4, "error": None, "skip_reason": None}


def test_a_load_that_exits_0_with_a_failed_row_and_no_error_fails_with_the_exit_table_object(rig: Rig) -> None:
    row = {**LOADED, "status": "failed", "rows": None}
    rig.scenario(flags=NEW_FLAGS, load={"summary": {"run_id": "r", "per_table": [row]}})
    exc, ti = rig.run(rig.operator(RivetLoadOperator, export="users", table="users"))
    assert type(exc) is AirflowFailException, exc
    payload = ti.store[("task", "return_value")]
    assert payload["units"][0]["error"] == GENERIC_EXIT_0 == payload["error"]
    assert "per_unit_error" not in payload["degraded"]


def test_a_refused_load_without_a_result_file_is_one_failed_unit(rig: Rig) -> None:
    rig.scenario(load={"exit": 5, "line": REFUSAL_LINE})
    out, _ = attempt(rig, "load", export="users")
    assert [u.to_xcom() for u in out.units] == [
        {"export": "users", "table": None, "status": "failed", "run_id": None, "rows": None, "files": None,
         "stop_reason": None, "error": out.error.to_xcom()}
    ]  # fmt: skip
    assert out.error.code == "RIVET_X" and "exit_zero_failed_unit" not in out.degraded


def test_a_load_row_with_its_own_refusal_fails_with_it(rig: Rig) -> None:
    own = {"class": "refusal", "retryable": False, "exit_code": 5, "code": "RIVET_X", "kind": None, "action": None}
    row = {**LOADED, "status": "failed", "error": own}
    rig.scenario(flags=NEW_FLAGS, load={"summary": {"run_id": "r", "per_table": [row]}})
    out, _ = attempt(rig, "load", export="users", table="users")
    assert out.error.code == "RIVET_X" and out.error.class_ == "refusal" and out.units[0].error == out.error


def test_a_load_row_without_its_own_error_takes_its_failures_entry(rig: Rig) -> None:
    mine = {"export": "events", "table": "events", "class": "generic", "retryable": False, "exit_code": 1, "code": "RIVET_F"}
    other = {**mine, "table": "other", "code": "RIVET_OTHER"}
    line = {**RETRYABLE_LINE, "failures": [other, mine]}
    row = {"export": "events", "table": "events", "status": "failed", "rows": 0, "error": None, "skip_reason": None}
    rig.scenario(flags=NEW_FLAGS, load={"exit": 2, "line": line, "summary": {"run_id": "r", "per_table": [row]}})
    out, _ = attempt(rig, "load", export="events", table="events")
    assert out.units[0].error.code == "RIVET_F" and "per_unit_error" not in out.degraded


def test_load_units_name_their_export_table_and_run(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, compact={"summary": fixture("compact_result.json")})
    out, events = attempt(rig, "compact", export="events", table="events")
    [unit] = out.units
    assert (unit.export, unit.table, unit.run_id) == ("events", "events", "20261006T102300.789-compact")
    assert out.error.describe(unit.export).endswith("(events)")
    assert of(events, "rivet.start")[0]["step"] == "compact" and out.stderr_path.endswith(".compact.stderr.log")


@pytest.mark.parametrize(("failed", "per_unit"), [(1, False), (2, True)])
def test_failed_load_rows_without_their_own_error_are_recorded_only_when_several_share_one(
    rig: Rig, failed: int, per_unit: bool
) -> None:
    rows = [{**LOADED, "table": name, "status": "failed", "rows": None} for name in ("users", "orders")[:failed]]
    rig.scenario(flags=NEW_FLAGS, load={"exit": 2, "line": RETRYABLE_LINE, "summary": {"run_id": "r", "per_table": rows}})
    out, events = attempt(rig, "load", export="users")
    assert len(out.units) == failed
    assert ("per_unit_error" in out.degraded) is per_unit
    assert of(events, "rivet.start")[0]["step"] == "load" and out.stderr_path.endswith(".load.stderr.log")


def test_a_junk_load_row_is_recorded_and_the_rest_is_read(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, load={"summary": {"run_id": "r", "per_table": ["junk", LOADED]}})
    out, _ = attempt(rig, "load", export="users", table="users")
    assert [(u.table, u.status, u.rows) for u in out.units] == [("users", "loaded", 4)]
    assert "result_malformed" in out.degraded


# --- plan (`_plan`) ------------------------------------------------------------------------------------------------------

LAYOUT = [
    {
        "export_name": name,
        "prioritization": {
            "campaign": {
                "waves": [{"wave": 1, "exports": ["orders", "users"]}],
                "ordered_exports": [{"export_name": "orders", "cost_class": "low"}],
            }
        },
    }
    for name in ("orders", "users")
]


def test_a_plan_asks_for_json_and_swaps_in_a_readable_layout(rig: Rig) -> None:
    target = rig.tmp / "layout.json"
    rig.scenario(plan_list=LAYOUT)
    old = os.umask(0o077)
    try:
        out, events = attempt(rig, "plan", plan_file=str(target))
    finally:
        os.umask(old)
    assert rig.argv("plan") == ["plan", "--config", str(rig.copy()), "--format", "json", "--json-errors"]
    assert of(events, "rivet.start")[0]["step"] == "plan" and out.stderr_path.endswith(".plan.stderr.log")
    assert [u.to_xcom() for u in out.units] == [
        {"export": None, "table": None, "status": "success", "run_id": None, "rows": None, "files": None,
         "stop_reason": None, "error": None}
    ]  # fmt: skip
    assert json.loads(target.read_text()) == LAYOUT
    assert os.stat(target).st_mode & 0o777 == 0o644, "a new layout file is readable by the DAG processor"


def test_a_failed_plan_is_classified_by_its_error_line_and_leaves_the_layout(rig: Rig) -> None:
    target = rig.tmp / "layout.json"
    target.write_text('["old"]')
    rig.scenario(plan={"exit": 1, "line": {"error": "bad config", "exit_class": 1}})
    out, _ = attempt(rig, "plan", plan_file=str(target))
    assert out.error.to_xcom() == GENERIC_EXIT_0
    assert [u.to_xcom() for u in out.units] == [
        {"export": None, "table": None, "status": "failed", "run_id": None, "rows": None, "files": None,
         "stop_reason": None, "error": GENERIC_EXIT_0}
    ]  # fmt: skip
    assert "exit_zero_failed_unit" not in out.degraded and target.read_text() == '["old"]'


def test_a_plan_that_printed_no_layout_is_the_internal_object(rig: Rig) -> None:
    rig.scenario(plan_list=[])
    out, _ = attempt(rig, "plan", plan_file=str(rig.tmp / "layout.json"))
    internal = {"code": None, "kind": None, "class": "internal", "exit_code": 6, "retryable": False, "action": None}
    assert out.error.to_xcom() == internal and out.units[0].error == out.error and out.units[0].status == "failed"
    assert "exit_zero_failed_unit" not in out.degraded


def test_the_plan_scratch_file_is_named_after_the_whole_label(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    opened: list[Path] = []
    real = invocation._open_private
    monkeypatch.setattr(invocation, "_open_private", lambda path: opened.append(path) or real(path))
    rig.scenario(plan_list=LAYOUT)
    target = rig.tmp / "layout.json"
    attempt(rig, "plan", plan_file=str(target), label=("dag", "run", "task", 1))
    assert opened[0] == target.with_name(f"layout.json.{hashed('dag', 'run', 'task', 'try1')}.tmp")


# --- apply (`_apply`, `_applied_by_earlier_try`, `_unit_from_metrics`, `_metrics_rows`) --------------------------------


def test_an_apply_whose_plan_fails_is_one_failed_unit_of_the_plans_error(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, plan={"exit": 5, "line": REFUSAL_LINE})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    assert type(exc) is AirflowFailException
    payload = ti.store[("task", "return_value")]
    assert payload["decision"] == "fail" and payload["error"]["code"] == "RIVET_X" and payload["exit_status"] == 5
    assert [(u["export"], u["status"], u["error"]) for u in payload["units"]] == [("orders", "failed", payload["error"])]
    assert "exit_zero_failed_unit" not in payload["degraded"]
    assert [c["argv"][0] for c in rig.calls()] == ["plan"]


def test_the_sealed_plan_lives_under_plans_of_its_run(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, apply={"summary": FULL})
    out, _ = attempt(rig, "apply", export="orders", label=("dag", "run", "task", 1))
    assert out.decision == "success"
    artifact = Path(rig.argv("apply")[1])
    assert artifact.parent == rig.state_dir / "airflow" / "dag" / "plans" / "run"


def test_an_apply_without_an_export_plans_the_empty_name(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, apply={"summary": {"per_export": []}})
    attempt(rig, "apply")
    plan = rig.argv("plan")
    assert plan[plan.index("--export") + 1] == ""


def test_a_failed_apply_unit_takes_the_error_of_its_process(rig: Rig) -> None:
    entry = {"export_name": "orders", "status": "failed", "mode": "full"}
    rig.scenario(flags=NEW_FLAGS, apply={"exit": 1, "line": {"error": "x", "exit_class": 1}, "summary": {"per_export": [entry]}})
    out, _ = attempt(rig, "apply", export="orders")
    assert [(u.status, u.error) for u in out.units] == [("failed", out.error)] and out.error.class_ == "generic"


def first_apply(rig: Rig) -> tuple[Path, dict]:
    """Try 1 applies the plan and succeeds (and its task process dies before Airflow sees it); the record it left."""
    rig.scenario(flags=NEW_FLAGS, apply={"summary": FULL})
    out, _ = attempt(rig, "apply", export="orders", label=("dag", "run", "task", 1))
    assert out.decision == "success"
    artifact = Path(rig.argv("apply")[1])
    record = artifact.with_name(f"{artifact.stem}.apply.json")
    return record, json.loads(record.read_text())


def retry_with(rig: Rig, rows: list[dict]) -> tuple[Outcome, list[dict]]:
    rig.scenario(flags=NEW_FLAGS, apply={"summary": FULL}, metrics={"rows": rows})
    return attempt(rig, "apply", export="orders", label=("dag", "run", "task", 2))


def row(pid: int, at: str, status: str = "success", **more: Any) -> dict:
    return {
        "export_name": "orders",
        "run_id": f"orders_{at}_{pid}",
        "status": status,
        "total_rows": 7,
        "files_produced": 2,
        **more,
    }


def applies(rig: Rig) -> int:
    return [c["argv"][0] for c in rig.calls()].count("apply")


@pytest.mark.parametrize(("at", "taken"), [("20261016T100000.500", True), ("20261016T100000.499", False)])
def test_an_earlier_tries_run_counts_from_the_millisecond_its_record_started(rig: Rig, at: str, taken: bool) -> None:
    """rivet's run id carries its start to the millisecond: a run in the record's own millisecond is that apply's, one a
    millisecond earlier is not (`_applied_by_earlier_try` 35, 36, 89)."""
    record, doc = first_apply(rig)
    record.write_text(json.dumps({**doc, "started_at": "2026-10-16T10:00:00.500700+00:00"}))
    out, events = retry_with(rig, [row(doc["pid"], at)])
    assert out.decision == "success"
    assert applies(rig) == (1 if taken else 2)
    assert bool(of(events, "rivet.apply.already_applied")) is taken


def test_an_earlier_tries_skipped_run_is_taken_with_its_status(rig: Rig) -> None:
    record, doc = first_apply(rig)
    out, events = retry_with(rig, [row(doc["pid"], "29991231T000000.000", "skipped")])
    assert applies(rig) == 1 and [u.status for u in out.units] == ["skipped"]
    [done] = of(events, "rivet.apply.already_applied")
    assert done == {
        "event": "rivet.apply.already_applied",
        "export": "orders",
        "run_id": f"orders_29991231T000000.000_{doc['pid']}",
        "artifact": str(record.with_name(record.name.replace(".apply.json", ".json"))),
    }
    assert out.stderr_path == str(Path(out.stderr_path).parent / "run__try2.metrics.stderr.log")


@pytest.mark.parametrize("newer", ["failed", "foreign_pid"])
def test_a_newer_row_that_is_not_the_earlier_apply_does_not_hide_it(rig: Rig, newer: str) -> None:
    record, doc = first_apply(rig)
    first = row(doc["pid"], "29991231T000000.001", "failed") if newer == "failed" else row(doc["pid"] + 1, "29991231T000000.001")
    out, events = retry_with(rig, [first, row(doc["pid"], "29991231T000000.000")])
    assert applies(rig) == 1 and of(events, "rivet.apply.already_applied")


def test_a_record_without_a_pid_runs_no_metrics_step(rig: Rig) -> None:
    record, doc = first_apply(rig)
    record.write_text(json.dumps({**doc, "pid": None}))
    before = len(rig.calls())
    retry_with(rig, [])
    assert [c["argv"][0] for c in rig.calls()[before:]] == ["apply"]


@pytest.mark.parametrize("metrics", [{"exit": 1}, {"rows": []}, {"rows": None}, {"exit": 1, "rows": [{"export_name": "orders"}]}])
def test_an_apply_whose_metrics_say_nothing_is_a_success_without_counts(rig: Rig, metrics: dict) -> None:
    """Today's binary (no `apply --summary-output`): the counts come from `rivet metrics`; a failed call or no row
    leaves them empty (`_unit_from_metrics` 20, `_metrics_rows` 28)."""
    rig.scenario(metrics=metrics)
    out, _ = attempt(rig, "apply", export="orders")
    assert out.decision == "success"
    assert [(u.export, u.status, u.rows, u.run_id) for u in out.units] == [("orders", "success", None, None)]


# --- the session's names and folders (`_Session.__init__`, `_Session.close`, `_Session.config_for`) -------------------


def test_the_summary_scratch_file_is_named_after_run_task_and_try(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS)
    attempt(rig, "run", export="orders", label=("dag", "run", "task", 1))
    argv = rig.argv("run")
    summary = Path(argv[argv.index("--summary-output") + 1])
    assert summary == rig.state_dir / "airflow" / "dag" / f"{hashed('run', 'task', 'try1')}.summary.json"


def diskless(**more: Any) -> dict:
    return {
        "env": {"RIVET_PG_URL": SECRET, "RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"},
        "state_dir": None,
        **more,
    }


def test_a_diskless_worker_works_in_a_private_scratch_folder_it_removes(rig: Rig, own_temp_dir: Path) -> None:
    rig.scenario(flags=NEW_FLAGS)
    attempt(rig, "run", export="orders", **diskless())
    argv = rig.argv("run")
    scratch = Path(argv[argv.index("--summary-output") + 1]).parent
    assert scratch.parent == own_temp_dir and scratch.name.startswith("rivet-airflow-") and not scratch.exists()


def test_a_scratch_folder_that_cannot_be_removed_whole_does_not_fail_the_task(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    real = invocation.shutil.rmtree

    def rmtree(path: Any, ignore_errors: bool = False) -> None:
        if not ignore_errors:
            raise PermissionError(path)
        real(path, ignore_errors=True)

    monkeypatch.setattr(invocation.shutil, "rmtree", rmtree)
    rig.scenario(flags=NEW_FLAGS)
    out, _ = attempt(rig, "run", export="orders", **diskless())
    assert isinstance(out, Outcome) and out.decision == "success", out


def test_a_diskless_load_narrows_its_config_copy_in_the_scratch_folder(rig: Rig) -> None:
    rig.scenario(record_config=True)
    out, _ = attempt(rig, "load", export="users", **diskless())
    assert isinstance(out, Outcome) and out.decision == "success", out
    assert [e["name"] for e in rig.calls()[0]["config"]["exports"]] == ["users"]


def test_a_stderr_dir_whose_parent_is_missing_is_created(rig: Rig) -> None:
    where = rig.tmp / "a" / "b"
    out, _ = attempt(rig, "run", export="orders", stderr_dir=str(where))
    assert Path(out.stderr_path).is_relative_to(where)


def test_stderr_files_are_kept_per_dag_and_per_task(rig: Rig) -> None:
    where = str(rig.tmp / "logs")
    paths = {
        attempt(rig, "run", export="orders", stderr_dir=where, label=label)[0].stderr_path
        for label in (("sales", "r", "t", 1), ("Sales", "r", "t", 1), ("sales", "r", "u", 1))
    }
    assert len(paths) == 3


def test_a_narrow_refusal_names_the_config_it_could_not_cut(rig: Rig) -> None:
    rig.config.write_text(
        "source: &src\n  type: postgres\n  url_env: RIVET_PG_URL\nexports:\n"
        "  - &first\n    name: orders\n    query: SELECT 1\n  - <<: *first\n    name: users\n"
    )
    exc, _ = attempt(rig, "load", export="users")
    assert isinstance(exc, PreflightRefusal) and exc.code == "RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE", exc
    text = str(exc)
    assert f"loads from a copy of {rig.config.resolve()} cut down to `users`" in text
    assert "and the text cannot be cut safely: " in text
    assert text.endswith(
        "Write `exports:` as a plain list whose items each start their own `- name:` line, with no anchor another item uses"
    )


# --- the decision (`decision_of`; `Outcome.decision` was not mutated) ---------------------------------------------------


@pytest.mark.parametrize(
    ("error", "decision"),
    [
        (None, "success"),
        (ErrorObject(None, None, "crashed", None, True, None), "retry_crashed"),
        (ErrorObject(None, None, "crashed", None, False, None), "retry_crashed"),
        (ErrorObject(None, None, "retryable", 2, True, None), "retry"),
        (ErrorObject("RIVET_STATE_RUN_IN_PROGRESS", None, "refusal", 5, True, None), "retry"),
        (ErrorObject(None, None, "refusal", 5, False, None), "fail"),
        (ErrorObject(None, None, "generic", 1, False, None), "fail"),
    ],
)
def test_the_decision_of_each_error(error: ErrorObject | None, decision: str) -> None:
    assert invocation.decision_of(error) == decision
    assert Outcome("run", error=error).decision == decision
    assert Outcome("run", error=error).to_xcom()["decision"] == decision


def test_an_operator_payload_carries_the_command_and_the_binarys_version(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS, run={"summary": FULL})
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"), FakeTI())
    assert (payload["command"], payload["rivet_version"]) == ("run", "0.31.0")
