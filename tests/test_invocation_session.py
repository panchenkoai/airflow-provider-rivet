"""One task's session and the invoke entry at their edges: the names and folders a session makes, closing it, the
config copy it hands rivet, the result file it reads, the worker's variables, the parent-death signal, and the
dispatch of `invoke`. Mutant ids are the mutmut names of airflow_provider_rivet/invocation.py."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow_provider_rivet import invocation
from airflow_provider_rivet.invocation import Request, hashed_name, path_name
from airflow_provider_rivet.preflight import STATE_URL_ENV, PreflightRefusal
from airflow_provider_rivet.reading import Reading

LABEL = ("dag", "run", "task", 2)


def session(tmp_path: Path, state: bool = True, stderr_dir: str | None = None, **pf: object) -> invocation._Session:
    config = tmp_path / "src" / "rivet.yaml"
    config.parent.mkdir(exist_ok=True)
    config.write_text(pf.pop("text", "exports: []\n"))  # type: ignore[arg-type]
    state_dir = tmp_path / "state" if state else None
    if state_dir:
        state_dir.mkdir(exist_ok=True)
    fields = {"state_dir": state_dir, "state_kind": "postgres", "config": {"exports": []}, "value_of": lambda n: None}
    req = Request("load", str(config), {}, stderr_dir=stderr_dir, label=LABEL, log_keep=None)
    return invocation._Session(req, SimpleNamespace(**{**fields, **pf}), lambda e: None, None)  # type: ignore[arg-type]


def test_a_session_names_its_files_by_run_task_and_try(tmp_path: Path) -> None:
    """_Session.__init__ 7/25/26/31: the result files' name, the plan scratch name, the log file stem; no step has
    written a stderr file yet."""
    s = session(tmp_path)
    assert s.stderr_path is None
    assert s.name == path_name("run", "task", "try2")
    assert s.global_name == path_name("dag", "run", "task", "try2")
    assert s.log_name == "run__try2"
    assert s.logs == tmp_path / "state" / "logs" / "dag" / hashed_name("task")


def test_a_diskless_session_works_in_a_fresh_temp_folder_and_closing_removes_it_even_twice(tmp_path: Path) -> None:
    """_Session.__init__ 45-47 and close 3/5/6."""
    s = session(tmp_path, state=False)
    assert s.tmp is not None and s.work == s.tmp and s.tmp.name.startswith("rivet-airflow-") and s.tmp.is_dir()
    shutil.rmtree(s.tmp)
    s.close()  # already gone: ignored
    assert "stderr_temp" in s.degraded


def test_closing_a_session_with_a_state_directory_prunes_old_ledgers(tmp_path: Path) -> None:
    """_Session.__init__ 8: no scratch folder, so `close` prunes the state's airflow folder."""
    s = session(tmp_path)
    ledger = s.work / "crashed" / "old"
    ledger.parent.mkdir(parents=True)
    ledger.write_text("[[1, 2]]")
    os.utime(ledger, (0, 0))
    s.close()
    assert not ledger.exists()


def test_the_ledger_an_operator_writes_is_one_a_session_prunes(tmp_path: Path) -> None:
    """crashed_ledger and _Session.work name one folder: a ledger the operator keeps is pruned by the session's close,
    whatever the DAG id's spelling (path_name hashes an upper-case one in both)."""
    for dag in ("dag", "Daily Orders"):
        s = invocation._Session(
            Request("load", str(session(tmp_path).req.config), {}, label=(dag, "run", "task[3]", 1), log_keep=None),
            session(tmp_path).pf,
            lambda e: None,
            None,
        )
        ledger = invocation.crashed_ledger(tmp_path / "state", dag, "run", "task[3]")
        assert ledger.parent == s.work / invocation.CRASHED
        ledger.parent.mkdir(parents=True, exist_ok=True)
        ledger.write_text("[[1, 2]]")
        os.utime(ledger, (0, 0))
        s.close()
        assert not ledger.exists()


def test_a_stderr_directory_whose_parents_are_missing_is_created(tmp_path: Path) -> None:
    """_Session.__init__ 48/50/53."""
    logs = tmp_path / "a" / "b" / "logs"
    s = session(tmp_path, stderr_dir=str(logs))
    assert s.logs == logs / "dag" / hashed_name("task") and s.logs.is_dir()


def test_an_existing_stderr_directory_is_used_with_the_task_folder_below_it(tmp_path: Path) -> None:
    """_Session.__init__ 49/51/54/61/62."""
    logs = tmp_path / "logs"
    logs.mkdir()
    s = session(tmp_path, stderr_dir=str(logs))
    assert s.logs == logs / "dag" / hashed_name("task") and s.logs.is_dir()


def test_a_narrowed_copy_without_a_state_directory_lands_in_the_work_folder(tmp_path: Path) -> None:
    """_Session.config_for 20."""
    text = "exports:\n  - name: a\n  - name: b\n"
    s = session(tmp_path, state=False, text=text, config={"exports": [{"name": "a"}, {"name": "b"}]}, config_text=text)
    copy = Path(s.config_for("a"))
    assert copy.parent == s.work and copy.read_text() == "exports:\n  - name: a\n"


def test_a_narrowed_copy_that_cannot_be_cut_names_the_source_config(tmp_path: Path) -> None:
    """_Session.config_for 48/53: the refusal names the config the task was given."""
    text = "exports:\n  - name: a\n    x: 1\n  - name: b\n"
    s = session(tmp_path, text=text, config={"exports": [{"name": "a"}, {"name": "b"}]}, config_text=text)
    with pytest.raises(PreflightRefusal, match=f"a copy of {tmp_path / 'src' / 'rivet.yaml'} cut down to `a`"):
        s.config_for("a")


@pytest.mark.parametrize(
    ("body", "degraded"),
    [(None, {"result_missing"}), ("{", {"result_malformed"}), (b"\xff", {"result_malformed"}), ("{}", set())],
    ids=["missing", "not-json", "not-utf8", "object"],
)
def test_a_result_file_on_exit_zero_records_what_is_wrong_with_it(tmp_path: Path, body: object, degraded: set) -> None:
    """Reading.result_file 7: missing and malformed are different words; a good file is neither."""
    s = Reading(set(), {}, "run")
    path = tmp_path / "summary.json"
    if isinstance(body, bytes):
        path.write_bytes(body)
    elif body is not None:
        path.write_text(body)
    step = SimpleNamespace(signal=None, exit_status=0)
    s.result_file(path, step)  # type: ignore[arg-type]
    assert s.degraded == degraded


# --- the worker's variables -----------------------------------------------------------------------------------------


def test_the_worker_environment_is_read_from_the_process_or_the_given_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    """worker_environment 3/4/5."""
    monkeypatch.setenv("LANG", "from-process")
    assert invocation.worker_environment()["LANG"] == "from-process"
    assert invocation.worker_environment(environ={"LANG": "given", "SECRET": "x"}) == {"LANG": "given"}


def test_an_undeclared_postgres_state_variable_is_read_from_the_process_or_the_given_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """undeclared_state_variables 2/3/4."""
    url = "postgresql://u@h/db"
    monkeypatch.setenv(STATE_URL_ENV, url)
    assert invocation.undeclared_state_variables({}) == [STATE_URL_ENV]
    assert invocation.undeclared_state_variables({}, environ={}) == []
    monkeypatch.delenv(STATE_URL_ENV)
    assert invocation.undeclared_state_variables({}, environ={STATE_URL_ENV: url}) == [STATE_URL_ENV]


# --- the parent-death signal ----------------------------------------------------------------------------------------


def test_the_parent_death_signal_asks_prctl_for_sigterm_with_zero_padding(monkeypatch: pytest.MonkeyPatch) -> None:
    """_parent_death_signal 6-8/11-13/15-17: `prctl(PR_SET_PDEATHSIG, SIGTERM, 0, 0, 0)`, five arguments."""
    calls = []
    monkeypatch.setattr(invocation, "_prctl", lambda: lambda *args: calls.append(args))
    arm = invocation._parent_death_signal()
    assert arm is not None
    monkeypatch.setattr(invocation.os, "getppid", lambda: os.getpid())  # the parent is alive
    arm()
    assert calls == [(1, 15, 0, 0, 0)]


# --- invoke ---------------------------------------------------------------------------------------------------------


def test_an_unknown_command_is_named_in_the_error() -> None:
    """invoke 2."""
    with pytest.raises(ValueError, match=r"^unknown rivet command 'bogus'$"):
        invocation.invoke(Request("bogus", "c", {}), lambda e: None)


class Stop(Exception):
    pass


@pytest.mark.parametrize(("command", "cdc"), [("cdc_run", True), ("run", False)])
def test_the_preflight_is_told_whether_the_task_is_a_cdc_run_and_its_warnings_are_recorded(
    monkeypatch: pytest.MonkeyPatch, command: str, cdc: bool
) -> None:
    """_invoke 8/17/21/22/32/33."""
    seen = {}

    def preflight(**kwargs: object) -> SimpleNamespace:
        seen.update(kwargs)
        return SimpleNamespace(warnings=[("no_notify", "the binary has no --no-notify")])

    def no_session(*args: object) -> None:
        raise Stop

    monkeypatch.setattr(invocation, "run_preflight", preflight)
    monkeypatch.setattr(invocation, "_Session", no_session)
    events: list[dict] = []
    with pytest.raises(Stop):
        invocation._invoke(Request(command, "c", {}), events.append, None, None)
    assert seen["cdc"] is cdc
    assert events == [{"event": "rivet.warning", "warning": "no_notify", "text": "the binary has no --no-notify"}]


def test_every_dispatched_command_names_its_lock() -> None:
    """LOCK_SCOPE: a command added to the dispatch without a lock row is caught here, not by a run blocked for lock_wait."""
    assert set(invocation.LOCK_SCOPE) == set(invocation._DISPATCH)


def test_a_failed_unit_without_its_own_error_takes_the_one_rivet_printed() -> None:
    """Reading.run_units 42: the process-level object rivet printed (its code), not one rebuilt from the exit."""
    from conftest import fixture

    from airflow_provider_rivet.reading import EXIT_ZERO, _Step
    from airflow_provider_rivet.result import ErrorObject

    printed = ErrorObject.from_contract(fixture("error_object.json"))
    read = Reading(set(), {}, "run")
    step = _Step(5, None, None)
    units = read.run_units(step, {"per_export": [{"export_name": "a", "status": "failed"}]}, printed, None)
    assert units[0].error == printed and printed.code
    units = read.run_units(_Step(0, None, None), {"per_export": [{"export_name": "a", "status": "failed"}]}, None, None)
    assert units[0].error == EXIT_ZERO and EXIT_ZERO.message == "rivet exited 0 and printed no error object"
