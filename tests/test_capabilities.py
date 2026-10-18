"""The binary probe: the environment it runs the binary in, and what it says when the binary gives no answer."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from airflow_provider_rivet.capabilities import Capabilities, ProbeFailed, probe


def fake_rivet(tmp_path: Path, body: str, mode: int = 0o755) -> str:
    """A `rivet` that runs `body` before it prints its version."""
    binary = tmp_path / "rivet"
    binary.write_text(f"#!/bin/sh\n{body}\necho 'rivet 0.31.0'\n")
    binary.chmod(mode)
    return str(binary)


def env_seen(tmp_path: Path) -> tuple[str, Path]:
    """A `rivet` that writes the PATH, HOME and RIVET_PROBE_LEAK it was started with to a file."""
    seen = tmp_path / "env.txt"
    shown = '"${PATH-unset}|${HOME-unset}|${RIVET_PROBE_LEAK-unset}"'
    return fake_rivet(tmp_path, f"printf '%s\\n' {shown} > {seen}"), seen


def test_a_probe_runs_the_binary_with_path_and_home_and_nothing_else(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutants `_run` 1-3, 8, 9, 11, 12, 17, 18, 28, 34: another env, a renamed key, or the worker's whole env."""
    binary, seen = env_seen(tmp_path)
    monkeypatch.setenv("PATH", "/bin:/usr/bin")
    monkeypatch.setenv("HOME", "/home/worker")
    monkeypatch.setenv("RIVET_PROBE_LEAK", "secret")
    assert Capabilities(binary).version == (0, 31, 0)
    assert seen.read_text() == "/bin:/usr/bin|/home/worker|unset\n"


def test_a_worker_without_path_or_home_probes_with_both_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutants `_run` 5, 7, 10, 14, 16, 19: an unset PATH or HOME is passed as empty, not as None or a placeholder."""
    binary, seen = env_seen(tmp_path)
    monkeypatch.delenv("PATH", raising=False)
    monkeypatch.delenv("HOME", raising=False)
    Capabilities(binary)
    assert seen.read_text() == "||unset\n"


def exactly(text: str) -> str:
    """A `pytest.raises(match=)` pattern for the whole message."""
    return f"^{re.escape(text)}$"


def test_each_probe_that_gets_no_answer_says_why(tmp_path: Path) -> None:
    """Mutants `probe` 3, 4, `__init__` 10, `_run` 40, 41: the message names the binary and the error's type."""
    absent = str(tmp_path / "absent")
    with pytest.raises(ProbeFailed, match=exactly(f"`{absent}` cannot be read: FileNotFoundError")):
        probe(absent)
    mute = fake_rivet(tmp_path, "echo 'hello'; exit 0")
    with pytest.raises(ProbeFailed, match=exactly(f"`{mute} --version` did not print a rivet version")):
        Capabilities(mute)
    (tmp_path / "rivet").unlink()
    locked = fake_rivet(tmp_path, "", mode=0o644)
    with pytest.raises(ProbeFailed, match=exactly(f"`{locked} --version` could not be run: PermissionError")):
        Capabilities(locked)
