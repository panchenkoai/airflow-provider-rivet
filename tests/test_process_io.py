"""Process I/O and the stop path: the stderr error line, the summary file, and a SIGTERM in the middle of a step."""

from __future__ import annotations

import json
import random
import subprocess
import sys
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException, AirflowFailException
from conftest import Rig

from airflow_provider_rivet import invocation
from airflow_provider_rivet.operators import RivetApplyOperator, RivetLoadOperator, RivetRunOperator

# Every character `str.splitlines()` cuts at besides "\n" and "\r": serde_json and rivet's terminal sanitiser leave
# them raw inside a JSON string, so each one can sit in the message of a `--json-errors` line.
LINE_BREAKERS = [" ", " ", "\u0085", "\x0b", "\x0c", "\x1c", "\x1d", "\x1e"]
TRANSIENT = {"exit_class": 2, "code": "RIVET_SOURCE_CONNECT"}


def corpus(seed: int, count: int) -> list[str]:
    """Generated messages over arbitrary UTF-8 (no surrogates), each holding at least one line breaker."""
    rng = random.Random(seed)
    pool = [*LINE_BREAKERS, "\r", "\t", "{", "}", '"', "\\", " ", "é", "ж", "漢", "🦀", "​", "﻿"]
    out = []
    for _ in range(count):
        chars = []
        for _ in range(rng.randint(1, 40)):
            if rng.random() < 0.5:
                chars.append(rng.choice(pool))
            else:
                point = rng.randint(0x20, 0x10FFFF)
                chars.append(chr(point) if not 0xD800 <= point <= 0xDFFF else "x")
        chars.insert(rng.randint(0, len(chars)), rng.choice(LINE_BREAKERS))
        out.append("".join(chars))
    return out


def test_d01_the_error_line_survives_any_utf8_in_its_message() -> None:
    """D01: the `--json-errors` line is found, code and class intact, whatever its message holds; CRLF is a newline."""
    for index, message in enumerate(corpus(seed=2028, count=600)):
        line = json.dumps({"error": message, **TRANSIENT}, ensure_ascii=False)
        newline = "\r\n" if index % 2 else "\n"
        text = newline.join(["[INFO] connecting", line, "warning: trailing"]) + newline
        found = invocation._error_line(text)
        assert found is not None, f"D01: error line lost for message {message!r}"
        assert (found.get("code"), found.get("exit_class"), found.get("error")) == ("RIVET_SOURCE_CONNECT", 2, message)


RAW_IN_JSON = ["\u2028", "\u2029", "\u0085"]


@pytest.mark.parametrize("breaker", RAW_IN_JSON, ids=[f"U+{ord(c):04X}" for c in RAW_IN_JSON])
def test_d01_a_retryable_failure_with_a_line_breaker_in_its_message_is_retried_with_its_code(rig: Rig, breaker: str) -> None:
    """D01: exit 2 whose message quotes U+2028 (and kin) keeps class `retryable`, the retry, and the RIVET_* code.

    JSON escapes the C0 breakers (U+000B, U+000C, U+001C-1E) inside a string, so only these three reach stderr raw.
    """
    rig.scenario(apply={"exit": 2, "line": {"error": f"connection reset{breaker}by peer", **TRANSIENT}})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    payload = ti.store[("task", "return_value")]
    error = payload["units"][0]["error"]
    assert (error["code"], error["class"], error["retryable"]) == ("RIVET_SOURCE_CONNECT", "retryable", True)
    assert type(exc) is AirflowException and payload["decision"] == "retry"


def test_d01_streamed_stderr_is_cut_at_newlines_only(rig: Rig) -> None:
    """D01: the task log gets one `rivet.stderr` record per line rivet wrote, without its "\r"."""
    rig.scenario(run={"stderr": "a\u2028b\r\nc\x1cd"}, leak_env=[])
    rig.run(rig.operator(RivetRunOperator, export="orders", stream_stderr=True))
    assert [e["line"] for e in rig.events("rivet.stderr")] == ["a b", "c\x1cd"]


def test_d01_a_broken_json_line_is_recorded_as_result_malformed(rig: Rig) -> None:
    """D01: a stderr line that starts with `{` and does not parse is recorded, not silently skipped."""
    rig.scenario(apply={"exit": 2, "line_raw": '{"error": "connection reset", "exit_class": 2, "co'})
    exc, ti = rig.run(rig.operator(RivetApplyOperator, export="orders"))
    payload = ti.store[("task", "return_value")]
    assert "result_malformed" in payload["degraded"]
    assert isinstance(exc, AirflowFailException)


@pytest.mark.parametrize(
    ("play", "key"),
    [({"no_summary": True}, "result_missing"), ({"summary_raw": "per_export: [] # not JSON"}, "result_malformed")],
    ids=["missing", "not_json"],
)
@pytest.mark.parametrize("command", ["run", "apply", "load"])
def test_d18_an_exit_zero_without_a_readable_summary_is_recorded(rig: Rig, command: str, play: dict, key: str) -> None:
    """D18: `--summary-output` was passed, rivet exited 0, and the file is absent or not JSON: recorded in degraded."""
    flags = {"apply": ["--summary-output"], "load": ["--export", "--table", "--summary-output"]}
    rig.scenario(flags=flags, **{command: play})
    cls = {"run": RivetRunOperator, "apply": RivetApplyOperator, "load": RivetLoadOperator}[command]
    payload, _ = rig.run(rig.operator(cls, export="orders"))
    assert payload["decision"] == "success"
    assert key in payload["degraded"]


def test_d18_a_readable_summary_records_nothing(rig: Rig) -> None:
    """D18 control: the summary the binary writes on exit 0 records neither key."""
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert not {"result_missing", "result_malformed"} & set(payload["degraded"])


STOP_CHILD = r"""
import json, os, signal, sys, tempfile, threading, time
from pathlib import Path
sys.path.insert(0, sys.argv[3])
tmp, how = Path(sys.argv[1]), sys.argv[2]
(tmp / "tmp").mkdir()
tempfile.tempdir = str(tmp / "tmp")
from conftest import Rig
from airflow_provider_rivet import invocation
from airflow_provider_rivet.operators import RivetRunOperator
invocation.STOP_GRACE = 1.0
rig = Rig(tmp)
started = tmp / "started"
rig.scenario(run={"sleep": 120, "started": str(started), "ignore_term": how == "ignore"})
op = rig.operator(RivetRunOperator, export="orders")

class Terminated(BaseException):
    pass

def on_term(signum, frame):
    # Airflow 3's task runner calls on_kill and returns; Airflow 2's raises after it.
    op.on_kill()
    if how == "raise":
        raise Terminated()

signal.signal(signal.SIGTERM, on_term)

def deliver():
    while not started.exists():
        time.sleep(0.02)
    os.kill(os.getpid(), signal.SIGTERM)

threading.Thread(target=deliver, daemon=True).start()
t0 = time.monotonic()
try:
    op.execute({"ti": __import__("conftest").FakeTI(), "run_id": "r"})
    raised = None
except BaseException as exc:
    raised = type(exc).__name__
pid = int(started.read_text())
try:
    os.kill(pid, 0)
    alive = True
except ProcessLookupError:
    alive = False
print(json.dumps({"elapsed": time.monotonic() - t0, "raised": raised, "rivet_alive": alive,
                  "secrets": sorted(p.name for p in (tmp / "tmp").glob("rivet-airflow-secrets-*"))}))
"""


@pytest.mark.parametrize("how", ["return", "raise", "ignore"])
def test_d02_a_sigterm_in_the_middle_of_a_step_stops_rivet_and_cleans_up(tmp_path: Path, how: str) -> None:
    """D02: SIGTERM while rivet runs: `on_kill` only signals, the step reaps (and kills after the grace), execute ends.

    `return` is Airflow 3's handler, `raise` Airflow 2's, `ignore` a rivet that ignores SIGTERM (escalation to SIGKILL).
    The scenario runs in a child process under a watchdog: the defect is a deadlock, which would hang the suite.
    """
    script = tmp_path / "stop_child.py"
    script.write_text(STOP_CHILD)
    work = tmp_path / "work"
    work.mkdir()
    tests = str(Path(__file__).resolve().parent)
    try:
        done = subprocess.run([sys.executable, str(script), str(work), how, tests], capture_output=True, text=True, timeout=25)
    except subprocess.TimeoutExpired:
        pytest.fail("D02: execute did not return within 25 s of SIGTERM (on_kill deadlocked in the signal handler)")
    assert done.returncode == 0, done.stderr[-2000:]
    report = json.loads(done.stdout.strip().splitlines()[-1])
    assert report["elapsed"] < 10, f"D02: execute took {report['elapsed']:.1f} s after SIGTERM"
    assert report["secrets"] == [], "D02: the rivet-airflow-secrets-* folder was left behind"
    assert report["rivet_alive"] is False
    assert report["raised"] in ("AirflowException", "AirflowFailException", "Terminated")


def test_d02_on_kill_only_signals_and_never_waits_or_polls(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """D02: `on_kill` runs inside the SIGTERM handler while the step may hold the wait lock: one SIGTERM, no wait/poll."""

    class Locked:
        pid, returncode = 4242, None

        def wait(self, timeout: float | None = None) -> int:
            raise AssertionError("D02: on_kill waited on the process inside the signal handler")

        poll = wait

    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(invocation.os, "kill", lambda pid, sig: sent.append((pid, sig)))
    op = rig.operator(RivetRunOperator, export="orders")
    op._hold(invocation.Running(Locked()))
    op.on_kill()
    op.on_kill()
    assert sent == [(4242, invocation._signal.SIGTERM)]


GAP_CHILD = r"""
import json, os, signal, sys, tempfile, time
from pathlib import Path
sys.path.insert(0, sys.argv[4])
tmp, how, gap = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
(tmp / "tmp").mkdir()
tempfile.tempdir = str(tmp / "tmp")
from conftest import FakeTI, Rig
from airflow_provider_rivet import invocation
from airflow_provider_rivet.operators import RivetApplyOperator
rig = Rig(tmp)
op = rig.operator(RivetApplyOperator, export="orders")
handled = []

class Terminated(BaseException):
    pass

def on_term(signum, frame):
    # Airflow 3's task runner calls on_kill and returns; Airflow 2's raises after it.
    handled.append(signum)
    op.on_kill()
    if how == "raise":
        raise Terminated()

signal.signal(signal.SIGTERM, on_term)

def stop_now():
    # The SIGTERM lands here, with no rivet process running, and is handled before this returns.
    os.kill(os.getpid(), signal.SIGTERM)
    deadline = time.monotonic() + 5
    while not handled and time.monotonic() < deadline:
        time.sleep(0.01)

if gap == "before_first_step":
    preflight = invocation.run_preflight
    def run_preflight(**kwargs):
        pf = preflight(**kwargs)
        stop_now()
        return pf
    invocation.run_preflight = run_preflight
else:
    step = invocation._Session.step
    def gated(self, name, argv, stdout_to=None):
        if name == "apply":
            stop_now()
        return step(self, name, argv, stdout_to)
    invocation._Session.step = gated

t0 = time.monotonic()
try:
    op.execute({"ti": FakeTI(), "run_id": "r"})
    raised = None
except BaseException as exc:
    raised = type(exc).__name__
print(json.dumps({"elapsed": time.monotonic() - t0, "raised": raised, "handled": len(handled),
                  "steps": [c["argv"][0] for c in rig.calls()],
                  "secrets": sorted(p.name for p in (tmp / "tmp").glob("rivet-airflow-secrets-*"))}))
"""


@pytest.mark.parametrize("how", ["return", "raise"])
@pytest.mark.parametrize("gap", ["between_plan_and_apply", "before_first_step"])
def test_d40_a_sigterm_while_no_rivet_runs_stops_the_next_step_from_starting(tmp_path: Path, how: str, gap: str) -> None:
    """D40: SIGTERM between two steps (plan done, apply not started) or before the first: no rivet runs to hold it.

    `on_kill` must still record the stop, so no later step starts rivet; the task fails and does not succeed. The
    signal is delivered synchronously at the gap (a patched seam), not by a sleep race. `return` is Airflow 3's
    handler, `raise` Airflow 2's.
    """
    script = tmp_path / "gap_child.py"
    script.write_text(GAP_CHILD)
    work = tmp_path / "work"
    work.mkdir()
    tests = str(Path(__file__).resolve().parent)
    try:
        done = subprocess.run(
            [sys.executable, str(script), str(work), how, gap, tests], capture_output=True, text=True, timeout=25
        )
    except subprocess.TimeoutExpired:
        pytest.fail("D40: execute did not return within 25 s of SIGTERM")
    assert done.returncode == 0, done.stderr[-2000:]
    report = json.loads(done.stdout.strip().splitlines()[-1])
    assert report["handled"] == 1, "D40: the SIGTERM never reached the handler: the test proves nothing"
    started = ["plan"] if gap == "between_plan_and_apply" else []
    assert report["steps"] == started, f"D40: rivet was started after the stop: {report['steps']}"
    expected = "AirflowException" if how == "return" else "Terminated"
    assert report["raised"] == expected, f"D40: execute ended with {report['raised']!r}, not a stop Airflow retries"
    assert report["elapsed"] < 10, f"D40: execute took {report['elapsed']:.1f} s after SIGTERM"
    assert report["secrets"] == [], "D40: the rivet-airflow-secrets-* folder was left behind"


def test_d40_a_process_started_after_the_stop_is_stopped_at_once(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """D40: a SIGTERM between the step's stop check and `_hold` met no process; the process `_hold` then gets is stopped."""

    class Fresh:
        pid, returncode = 4243, None

    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(invocation.os, "kill", lambda pid, sig: sent.append((pid, sig)))
    op = rig.operator(RivetApplyOperator, export="orders")
    op.on_kill()
    op._hold(invocation.Running(Fresh()))
    assert sent == [(4243, invocation._signal.SIGTERM)]
