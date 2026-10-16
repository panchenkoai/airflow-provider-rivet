"""The probes of the rivet binary (`--version`, `<sub> --help`) run with the minimal environment and fail by name.

A probe runs before any task setting is applied, from the worker's own process: whatever it inherits, every binary on
the worker's PATH that answers `--version` sees. So it gets PATH and HOME and nothing else (no Fernet key, no cloud
secret). These tests run the real `Capabilities` against a small binary that records the environment it was given.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from airflow_provider_rivet.capabilities import Capabilities, ProbeFailed, probe

# What the child adds itself, never the probe: Python sets LC_CTYPE when it starts in the C locale (PEP 538), and
# macOS's CoreFoundation sets __CF_USER_TEXT_ENCODING in every process that lacks it.
ADDED_BY_THE_INTERPRETER = {"LC_CTYPE", "__CF_USER_TEXT_ENCODING"}


def recording_binary(folder: Path, version: str = "rivet 0.31.0") -> Path:
    """A binary that appends the environment of each call to `probe_env.jsonl` beside it and prints `version`."""
    script = folder / "rivet"
    log = folder / "probe_env.jsonl"
    script.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        f"with open({str(log)!r}, 'a') as f:\n"
        "    f.write(json.dumps({'argv': sys.argv[1:], 'env': dict(os.environ)}) + '\\n')\n"
        f"print({version!r} if sys.argv[1:] == ['--version'] else 'Usage: rivet run [--lock-wait <S>]')\n"
    )
    script.chmod(0o755)
    return script


def probes(folder: Path) -> list[dict]:
    return [json.loads(line) for line in (folder / "probe_env.jsonl").read_text().splitlines()]


def given(call: dict) -> dict[str, str]:
    return {k: v for k, v in call["env"].items() if k not in ADDED_BY_THE_INTERPRETER}


def test_a_probe_gets_path_and_home_and_none_of_the_workers_secrets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutants `_run_1, 2, 3, 8, 9, 11, 12, 17, 18, 28, 34`: `env=None` (or a renamed key) made `--version` and
    `--help` inherit the whole worker environment, the Fernet key and cloud secrets with it."""
    monkeypatch.setenv("AIRFLOW__CORE__FERNET_KEY", "fernet-secret")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    caps = Capabilities(str(recording_binary(tmp_path)))
    assert caps.has_flag("run", "--lock-wait") and not caps.has_flag("run", "--no-notify")
    calls = probes(tmp_path)
    assert [c["argv"] for c in calls] == [["--version"], ["run", "--help"]], "one --help probe per subcommand"
    for call in calls:
        assert given(call) == {"PATH": os.environ["PATH"], "HOME": str(tmp_path / "home")}, call["argv"]


@pytest.mark.parametrize("unset", ["PATH", "HOME"])
def test_a_worker_without_path_or_home_gives_the_probe_an_empty_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, unset: str
) -> None:
    """Mutants `_run_5, 7, 10, 14, 16, 19`: a missing variable is passed empty, never as a placeholder, and never as
    None (which `subprocess` cannot pass and would crash the probe with a TypeError)."""
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv(unset)
    Capabilities(str(recording_binary(tmp_path)))
    assert given(probes(tmp_path)[0])[unset] == ""


def test_a_probe_that_fails_says_which_probe_and_why(tmp_path: Path) -> None:
    """The refusal text names the probe and the reason by the exception's class, never `None`. Mutants
    `__init___10`, `_run_40, 41`, `probe_3, 4`."""
    with pytest.raises(ProbeFailed) as garbage:
        Capabilities(str(recording_binary(tmp_path, version="rivet garbage")))
    assert str(garbage.value) == f"`{tmp_path / 'rivet'} --version` did not print a rivet version"
    not_executable = tmp_path / "plain"
    not_executable.write_text("")
    with pytest.raises(ProbeFailed) as denied:
        Capabilities(str(not_executable))
    assert str(denied.value) == f"`{not_executable} --version` could not be run: PermissionError"
    with pytest.raises(ProbeFailed) as missing:
        probe(str(tmp_path / "nowhere"))
    assert str(missing.value) == f"`{tmp_path / 'nowhere'}` cannot be read: FileNotFoundError"
