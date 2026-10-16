"""The helpers of `airflow_provider_rivet/invocation.py` called directly: names, private files and folders, pruning,
stderr reading, plan layouts and artifacts, the crashed ledger, the reaping loop, and the parent-death signal. Each
group names the mutants (mutmut ids) it exists to kill."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import pathlib
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from airflow_provider_rivet import invocation
from airflow_provider_rivet.invocation import (
    STALE_AFTER,
    STDERR_TAIL,
    copy_name,
    crashed_retry_allowed,
    hashed_name,
    narrowed_config,
    narrowed_exports,
    plan_layout,
    undeclared_state_variables,
    worker_environment,
)
from airflow_provider_rivet.preflight import PreflightRefusal

LINUX = sys.platform.startswith("linux")


def mode(path: Path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


# --- environment and names (`worker_environment` 3-5, `undeclared_state_variables` 2-4, `hashed_name` 5, 11) ----------


def test_the_worker_environment_is_read_from_the_mapping_given_else_from_the_process(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATH", "/from/process")
    monkeypatch.setenv("SECRET_TOKEN", "s")
    assert worker_environment((), (), {"PATH": "/given", "SECRET_TOKEN": "s"}) == {"PATH": "/given"}
    assert worker_environment()["PATH"] == "/from/process" and "SECRET_TOKEN" not in worker_environment()


def test_an_undeclared_state_variable_is_read_from_the_mapping_given_else_from_the_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pg = {"RIVET_STATE_URL": "postgresql://h/db"}
    assert undeclared_state_variables({}, pg) == ["RIVET_STATE_URL"]
    assert undeclared_state_variables(pg, pg) == []
    assert undeclared_state_variables({}) == []
    monkeypatch.setenv("RIVET_STATE_URL", "postgresql://h/db")
    assert undeclared_state_variables({}) == ["RIVET_STATE_URL"]
    assert undeclared_state_variables({}, {}) == []


def test_a_hashed_name_is_its_readable_parts_capped_at_80_and_12_hex_digits() -> None:
    digest = hashlib.sha256(json.dumps(["a b", "c"]).encode()).hexdigest()[:12]
    assert hashed_name("a b", "c") == f"a_b__c-{digest}"
    long = "a" * 100
    assert hashed_name(long) == "a" * 80 + "-" + hashlib.sha256(json.dumps([long]).encode()).hexdigest()[:12]


@pytest.mark.parametrize(
    ("source", "export", "name"),
    [("pg.yml", None, "pg.{d}.yml"), ("pipeline", None, "pipeline.{d}.yaml"), ("pg.yaml", "users", "pg.{d}--users.yaml")],
)
def test_a_config_copy_keeps_its_suffix_or_gets_yaml(tmp_path: Path, source: str, export: str | None, name: str) -> None:
    path = tmp_path / source
    digest = hashlib.sha256(str(path).encode()).hexdigest()[:8]
    assert copy_name(path, export) == name.format(d=digest)


# --- private files and folders (`_write_atomic` 4-7, `_open_private` 7, 10, 19, `_private_dir` 7-31, `_temp_base`) -----


def test_an_atomic_write_makes_its_hidden_temp_file_beside_the_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    made: list[dict] = []
    real = tempfile.mkstemp
    monkeypatch.setattr(invocation.tempfile, "mkstemp", lambda **kw: made.append(kw) or real(**kw))
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path / "missing"))
    target = tmp_path / "copy.yaml"
    assert invocation._write_atomic(target, b"x") == str(target) and target.read_bytes() == b"x"
    assert made == [{"dir": tmp_path, "prefix": ".copy.yaml."}]
    invocation._write_atomic(target, b"x")
    assert len(made) == 1, "a copy that already holds these bytes is left alone"


def test_a_private_file_is_0600_from_its_creation_and_open_for_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    at_chmod: list[int] = []
    real = os.fchmod

    def spy(fd: int, wanted: int) -> None:
        at_chmod.append(stat.S_IMODE(os.fstat(fd).st_mode))
        real(fd, wanted)

    monkeypatch.setattr(invocation.os, "fchmod", spy)
    old = os.umask(0)
    try:
        with invocation._open_private(tmp_path / "f.log") as handle:
            handle.write(b"bytes")
    finally:
        os.umask(old)
    assert at_chmod == [0o600] and mode(tmp_path / "f.log") == 0o600


def test_a_private_folder_is_created_0700(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    made: list[int] = []
    real = os.mkdir
    monkeypatch.setattr(invocation.os, "mkdir", lambda path, *mode_: made.append(mode_) or real(path, *mode_))
    assert invocation._private_dir(tmp_path, "a", "b") == tmp_path / "a" / "b"
    assert made == [(0o700,), (0o700,)]


def test_an_existing_folder_one_bit_too_open_is_tightened(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "a").chmod(0o701)
    invocation._private_dir(tmp_path, "a")
    assert mode(tmp_path / "a") == 0o700


def test_a_folder_that_cannot_be_created_is_refused_with_its_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def deny(path: Any, *_: Any) -> None:
        raise PermissionError(path)

    monkeypatch.setattr(invocation.os, "mkdir", deny)
    with pytest.raises(PreflightRefusal) as caught:
        invocation._private_dir(tmp_path, "airflow")
    assert caught.value.code == "RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE"
    assert str(caught.value) == f"[RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE] cannot create {tmp_path / 'airflow'}: PermissionError"


def test_a_folder_that_cannot_be_made_private_is_refused_with_its_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "airflow").mkdir(mode=0o755)
    (tmp_path / "airflow").chmod(0o755)

    def deny(path: Any, *_: Any) -> None:
        raise PermissionError(path)

    monkeypatch.setattr(invocation.os, "chmod", deny)
    with pytest.raises(PreflightRefusal) as caught:
        invocation._private_dir(tmp_path, "airflow")
    assert caught.value.code == "RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE"
    assert str(caught.value) == (
        f"[RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE] cannot make {tmp_path / 'airflow'} private: PermissionError"
    )


def test_the_temp_base_is_reused_only_when_it_is_our_own_private_folder(own_temp_dir: Path) -> None:
    ours = own_temp_dir / f"rivet-airflow-{os.getuid()}"
    assert invocation._temp_base() == ours and mode(ours) == 0o700
    assert invocation._temp_base() == ours, "our own private folder is reused"
    ours.chmod(0o755)
    elsewhere = invocation._temp_base()
    assert elsewhere != ours and elsewhere.parent == own_temp_dir and elsewhere.name.startswith("rivet-airflow-")
    ours.chmod(0o700)
    ours.rmdir()
    ours.write_text("")
    ours.chmod(0o700)
    instead = invocation._temp_base()
    assert instead != ours and instead.is_dir() and instead.name.startswith("rivet-airflow-")


# --- pruning (`_prune_stale` 4-26, `_prune_logs` 19, 20) ----------------------------------------------------------------


def aged(path: Path, text: str, mtime: float) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    os.utime(path, (mtime, mtime))
    return path


LEDGER = json.dumps([[1, 2]])


def test_pruning_reads_the_clock_it_is_given(tmp_path: Path) -> None:
    ledger = aged(tmp_path / "crashed" / "r" / "t.json", LEDGER, time.time())
    invocation._prune_stale(tmp_path, now=time.time() + 2 * STALE_AFTER)
    assert not ledger.exists()


def test_a_file_exactly_stale_after_old_is_kept(tmp_path: Path) -> None:
    now = 2_000_000_000
    ledger = aged(tmp_path / "crashed" / "t.json", LEDGER, now - STALE_AFTER)
    invocation._prune_stale(tmp_path, now=now)
    assert ledger.exists()
    invocation._prune_stale(tmp_path, now=now + 1)
    assert not ledger.exists()


def test_an_unlistable_plans_folder_does_not_stop_the_crashed_ledgers_pruning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = aged(tmp_path / "crashed" / "t.json", LEDGER, 0)
    (tmp_path / "plans").mkdir()
    real = pathlib.Path.rglob

    def rglob(self: Path, pattern: str, **kw: Any) -> Any:
        if self.name == "plans":
            raise PermissionError(self)
        return real(self, pattern, **kw)

    monkeypatch.setattr(pathlib.Path, "rglob", rglob)
    invocation._prune_stale(tmp_path)
    assert not ledger.exists()


def test_an_old_crashed_file_that_is_no_ledger_is_left_for_a_person(tmp_path: Path) -> None:
    junk = aged(tmp_path / "crashed" / "t.json", "not json", 0)
    invocation._prune_stale(tmp_path)
    assert junk.exists()


def test_every_expired_plan_is_pruned_whatever_comes_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for n in range(4):
        aged(tmp_path / "plans" / "r" / f"p{n}.json", "{}", 0)
    answers = iter([True, False, False, False])
    monkeypatch.setattr(invocation, "_unexpired", lambda path: next(answers))
    invocation._prune_stale(tmp_path)
    assert len(list((tmp_path / "plans" / "r").iterdir())) == 1, "only the one still replayable is kept"


def test_a_plan_another_task_pruned_first_is_no_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for n in range(2):
        aged(tmp_path / "plans" / "r" / f"p{n}.json", "{}", 0)

    def gone(path: Path) -> bool:
        path.unlink()
        return False

    monkeypatch.setattr(invocation, "_unexpired", gone)
    invocation._prune_stale(tmp_path)
    assert not (tmp_path / "plans" / "r").exists()


def test_a_log_file_another_task_pruned_first_does_not_stop_the_rest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tmp_path = tmp_path / "logs"
    for n, attempt in enumerate(("r__try1", "r__try2", "r__try3")):
        for stream in ("stderr", "stdout"):
            aged(tmp_path / f"{attempt}.run.{stream}.log", "x", 1_000_000 + n)
    real = pathlib.Path.unlink
    vanished: list[Path] = []

    def unlink(self: Path, missing_ok: bool = False) -> None:
        if not vanished:
            vanished.append(self)
            real(self)
        real(self, missing_ok=missing_ok)

    monkeypatch.setattr(pathlib.Path, "unlink", unlink)
    invocation._prune_logs(tmp_path, 1)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["r__try3.run.stderr.log", "r__try3.run.stdout.log"]


# --- reading rivet's stderr (`_tail` 10-32, `_lines` 4, `_error_line` 12, 13, `_has_broken_object` 6) -------------------


def tail_of(tmp_path: Path, data: bytes, monkeypatch: pytest.MonkeyPatch, tail: int = 16) -> str:
    monkeypatch.setattr(invocation, "STDERR_TAIL", tail)
    path = tmp_path / "err.log"
    path.write_bytes(data)
    return invocation._tail(path)


def test_a_file_of_exactly_the_tail_is_read_whole(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert tail_of(tmp_path, b"abc\ndefghijklmno", monkeypatch) == "abc\ndefghijklmno"


def test_a_tail_one_byte_into_a_line_starts_at_the_next_whole_line(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert tail_of(tmp_path, b"Xab\ncdefghijklmnop", monkeypatch) == "cdefghijklmnop"
    assert tail_of(tmp_path, b"0123456789ab\ncd\nef", monkeypatch) == "cd\nef"


def test_a_tail_without_a_newline_is_kept_whole(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert tail_of(tmp_path, b"x" * 40 + b"0123456789abcdef", monkeypatch) == "0123456789abcdef"


def test_invalid_utf8_reads_as_the_replacement_character(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert tail_of(tmp_path, b"\xff\nok", monkeypatch, tail=1 << 20) == "�\nok"


def test_a_stderr_over_a_mebibyte_keeps_its_last_mebibyte_from_a_whole_line_and_its_error_line(tmp_path: Path) -> None:
    """The hostile size, at the real STDERR_TAIL and under a time budget: 1.5 MiB of numbered lines, then the error."""
    error = json.dumps({"error": "network", "exit_class": 2})
    lines = [f"noise line {n:07d}" for n in range(80_000)]
    path = tmp_path / "err.log"
    path.write_bytes(("\n".join([*lines, error]) + "\n").encode())
    assert path.stat().st_size > STDERR_TAIL * 1.4
    result: list[str] = []
    worker = threading.Thread(target=lambda: result.append(invocation._tail(path)), daemon=True)
    started = time.monotonic()
    worker.start()
    worker.join(10)
    assert result and time.monotonic() - started < 5, "the tail of 1.5 MiB took too long"
    text = result[0]
    assert len(text.encode()) <= STDERR_TAIL and text.endswith(error + "\n")
    first = text.split("\n", 1)[0]
    assert first in lines and lines.index(first) > 0, "the tail starts at a whole line"
    assert invocation._error_line(text) == {"error": "network", "exit_class": 2}


def test_stderr_lines_are_cut_at_newlines_only() -> None:
    assert invocation._lines("a\nb") == ["a", "b"]
    assert invocation._lines("a\r\nb\r\n") == ["a", "b"]
    assert invocation._lines("") == []
    assert invocation._lines("a b\n") == ["a b"]


def test_the_error_line_is_found_behind_a_broken_or_foreign_json_line() -> None:
    error = {"error": "x", "exit_class": 2}
    assert invocation._error_line(json.dumps(error) + "\n{not json\n") == error
    assert invocation._error_line(json.dumps(error) + '\n{"level": "info"}\n') == error
    assert invocation._error_line(["plain"]) is None


def test_only_a_line_that_starts_like_an_object_and_does_not_parse_is_broken() -> None:
    assert invocation._has_broken_object(["plain", '{"a": 1}']) is False
    assert invocation._has_broken_object(["plain", "{cut"]) is True


# --- the plan layout (`plan_layout` 9-84) --------------------------------------------------------------------------------


def bare(name: Any, rec: dict) -> dict:
    return {"export_name": name, "prioritization": {"export_recommendation": {"recommended_wave": 2, **rec}}}


def test_a_bare_plan_is_its_own_wave_and_names_its_cost() -> None:
    low = {"export_name": "orders", "cost_class": "low"}
    assert plan_layout(bare("orders", low)) == [{"wave": 2, "exports": ["orders"], "heavy": []}]
    high = {"export_name": "orders", "cost_class": "high"}
    assert plan_layout(bare("orders", high)) == [{"wave": 2, "exports": ["orders"], "heavy": ["orders"]}]


@pytest.mark.parametrize("plan", [bare("orders", {"export_name": "users"}), bare(5, {"export_name": 5})])
def test_a_bare_plan_whose_recommendation_names_another_export_is_no_layout(plan: dict) -> None:
    with pytest.raises(ValueError, match=r"^the plan's recommendation names another export$"):
        plan_layout(plan)


def test_a_campaign_without_costs_makes_every_export_heavy() -> None:
    campaign = {"waves": [{"wave": 1, "exports": ["orders", "users"]}]}
    assert plan_layout([{"prioritization": {"campaign": campaign}}]) == [
        {"wave": 1, "exports": ["orders", "users"], "heavy": ["orders", "users"]}
    ]


def test_a_campaign_whose_waves_are_empty_is_no_layout() -> None:
    campaign = {"waves": [{"wave": 1, "exports": []}], "ordered_exports": []}
    with pytest.raises(ValueError, match=r"^the plan has no wave with an export$"):
        plan_layout([{"prioritization": {"campaign": campaign}}])


# --- narrowing (`narrowed_exports` 12, 40-46, `narrowed_config` 21-23) ---------------------------------------------------


def test_no_export_narrows_to_nothing() -> None:
    assert narrowed_exports([], "x") == set()


def test_a_streams_auto_backfill_keeps_the_batch_exports_of_its_tables_and_no_other_stream() -> None:
    exports = [
        {"name": "orders", "table": "orders", "mode": "full"},
        {"name": "users", "table": "users", "mode": "full"},
        {"name": "stream", "tables": ["orders", "users"], "mode": "cdc", "cdc": {"backfill": "auto"}},
        {"name": "other", "table": "orders", "mode": "cdc"},
    ]
    assert narrowed_exports(exports, "stream") == {0, 1, 2}


def test_a_cut_that_reads_back_as_another_document_is_refused_and_says_why() -> None:
    text = "exports:\n  - name: a\n    query: SELECT 1\n  - name: b\n    query: SELECT 2\n"
    config = {"exports": [{"name": "a", "query": "SELECT 9"}, {"name": "b", "query": "SELECT 2"}]}
    pf = SimpleNamespace(config=config, config_text=text, value_of=lambda name: None)
    with pytest.raises(PreflightRefusal) as caught:
        narrowed_config(pf, "src.yaml", "a")
    assert caught.value.code == "RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE"
    assert "the narrowed document differs from the chosen exports of the original. Write" in str(caught.value)


# --- query files beside the copy (`_copy_query_files` 20-61) -------------------------------------------------------------


def query_config(tmp_path: Path, *entries: dict) -> tuple[dict, Path, Path]:
    source = tmp_path / "cfg" / "pg.yaml"
    (tmp_path / "cfg" / "sql").mkdir(parents=True)
    source.write_text("")
    folder = tmp_path / "state"
    folder.mkdir()
    return {"exports": list(entries)}, source, folder


def test_a_query_file_after_an_export_without_one_is_copied(tmp_path: Path) -> None:
    config, source, folder = query_config(tmp_path, {"name": "orders"}, {"name": "users", "query_file": "sql/users.sql"})
    (source.parent / "sql" / "users.sql").write_text("SELECT 1")
    invocation._copy_query_files(config, source, folder, "users")
    assert (folder / "sql" / "users.sql").read_text() == "SELECT 1"


def test_another_exports_missing_query_file_does_not_stop_this_ones_copy(tmp_path: Path) -> None:
    config, source, folder = query_config(
        tmp_path, {"name": "orders", "query_file": "sql/missing.sql"}, {"name": "users", "query_file": "sql/users.sql"}
    )
    (source.parent / "sql" / "users.sql").write_text("SELECT 1")
    invocation._copy_query_files(config, source, folder, "users")
    assert (folder / "sql" / "users.sql").read_text() == "SELECT 1"


@pytest.mark.parametrize(
    ("ref", "why"),
    [
        ("/abs/q.sql", "must be a relative path with no `..`: rivet reads it only inside the config's directory"),
        ("../q.sql", "must be a relative path with no `..`: rivet reads it only inside the config's directory"),
        ("sql/missing.sql", "is not a file beside the config"),
        ("sql/link.sql", "resolves outside the config's directory, which rivet refuses"),
    ],
)
def test_a_query_file_rivet_would_not_read_is_refused_with_its_reason(tmp_path: Path, ref: str, why: str) -> None:
    config, source, folder = query_config(tmp_path, {"name": "orders", "query_file": ref})
    (tmp_path / "outside.sql").write_text("SELECT 1")
    (source.parent / "sql" / "link.sql").symlink_to(tmp_path / "outside.sql")
    with pytest.raises(PreflightRefusal) as caught:
        invocation._copy_query_files(config, source, folder, "orders")
    assert str(caught.value) == f"[RIVET_AIRFLOW_QUERY_FILE] export `orders` of {source}: query_file `{ref}` {why}"


def test_one_relative_query_path_of_two_config_folders_is_refused_with_the_remedy(tmp_path: Path) -> None:
    config, source, folder = query_config(tmp_path, {"name": "orders", "query_file": "sql/q.sql"})
    (source.parent / "sql" / "q.sql").write_text("SELECT 1")
    (folder / invocation.QUERY_OWNERS).write_text(json.dumps({"sql/q.sql": "/elsewhere"}))
    with pytest.raises(PreflightRefusal) as caught:
        invocation._copy_query_files(config, source, folder, "orders")
    assert str(caught.value).endswith(
        f"query_file `sql/q.sql` is already used in {folder} by a config of /elsewhere; two config directories that "
        "share a state directory cannot use one relative path: give them separate state directories"
    )


# --- sealed plans (`_unexpired` 13-19, `_plan_inputs` 24-36, `_refuse_foreign_artifact` 29-31) --------------------------


def artifact(tmp_path: Path, expires: str) -> Path:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"expires_at": expires}))
    return path


@pytest.mark.parametrize(
    ("expires", "unexpired"),
    [
        ("2999-01-01T00:00:00Z", True),
        ("2999-01-01T00:00:00z", False),
        ("2999-01-01T00:00:00", True),
        ("2999-01-01T00:00:00+05:00", True),
        ("2001-01-01T00:00:00", False),
    ],
)
def test_an_artifact_is_unexpired_while_its_expiry_lies_ahead(tmp_path: Path, expires: str, unexpired: bool) -> None:
    assert invocation._unexpired(artifact(tmp_path, expires)) is unexpired


def test_an_artifact_expiring_this_instant_is_expired(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)

    class Frozen(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return now

    monkeypatch.setattr(invocation, "datetime", Frozen)
    assert invocation._unexpired(artifact(tmp_path, "2030-01-01T00:00:00+00:00")) is False
    assert invocation._unexpired(artifact(tmp_path, "2030-01-01T00:00:00.000001+00:00")) is True


def plan_session(config: Path, exports: list[dict]) -> SimpleNamespace:
    return SimpleNamespace(req=SimpleNamespace(config=str(config)), pf=SimpleNamespace(config={"exports": exports}))


def inputs_digest(*parts: bytes) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(len(part).to_bytes(8, "big") + part)
    return digest.hexdigest()[:16]


def test_the_plan_inputs_are_a_fixed_digest_and_a_missing_query_file_counts_as_empty(tmp_path: Path) -> None:
    config = tmp_path / "pg.yaml"
    config.write_bytes(b"exports: []\n")
    s = plan_session(config, [{"name": "orders", "query_file": "sql/missing.sql"}])
    assert invocation._plan_inputs(s, "orders") == inputs_digest(str(config).encode(), b"exports: []\n", b"")
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "missing.sql").write_bytes(b"")
    assert invocation._plan_inputs(s, "orders") == inputs_digest(str(config).encode(), b"exports: []\n", b"")
    assert len(invocation._plan_inputs(s, "users")) == 16


def test_a_foreign_artifact_is_refused_with_its_remedy(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"export_name": "users", "config_path": str(tmp_path / "pg.yaml")}))
    with pytest.raises(PreflightRefusal) as caught:
        invocation._refuse_foreign_artifact(path, "orders", str(tmp_path / "pg.yaml"))
    assert caught.value.code == "RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN"
    assert str(caught.value).endswith(
        "applying it would extract something else under this task's name. It is neither applied nor replaced; delete "
        "the file once you know what wrote it"
    )


# --- the crashed ledger (`crashed_retry_allowed` 12, 29, 41) -------------------------------------------------------------


def test_a_ledger_that_cannot_be_read_names_why(tmp_path: Path) -> None:
    ledger = tmp_path / "crashed" / "t.json"
    ledger.mkdir(parents=True)
    assert crashed_retry_allowed(ledger, 1, 5, 1) == (
        False,
        f"crashed ledger {ledger} cannot be written (IsADirectoryError): no crash retry is granted",
    )


def test_a_ledger_that_cannot_be_written_names_why(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def deny(path: Path, data: bytes) -> str:
        raise PermissionError(path)

    monkeypatch.setattr(invocation, "_write_atomic", deny)
    ledger = tmp_path / "crashed" / "t.json"
    assert crashed_retry_allowed(ledger, 1, 5, 1) == (
        False,
        f"crashed ledger {ledger} cannot be written (PermissionError): no crash retry is granted",
    )


def test_asking_twice_for_one_try_does_not_count_it_as_an_earlier_crash(tmp_path: Path) -> None:
    ledger = tmp_path / "crashed" / "t.json"
    assert crashed_retry_allowed(ledger, 1, 5, 1) == (True, None)
    assert crashed_retry_allowed(ledger, 1, 5, 1) == (True, None)
    assert crashed_retry_allowed(ledger, 2, 5, 1) == (False, None)


# --- reaping (`Running.wait` 3, 6) ---------------------------------------------------------------------------------------


class Slow:
    """A process that outlives three polls, recording each poll's timeout and any kill."""

    pid, returncode = 4244, None

    def __init__(self) -> None:
        self.timeouts: list[float] = []
        self.killed = False

    def wait(self, timeout: float | None = None) -> int:
        self.timeouts.append(timeout)
        if len(self.timeouts) <= 3:
            raise subprocess.TimeoutExpired("rivet", timeout)
        return 0

    def kill(self) -> None:
        self.killed = True


def test_a_stopped_process_is_polled_every_fifth_of_a_second_and_not_killed_inside_the_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(invocation.os, "kill", lambda pid, sig: None)
    proc = Slow()
    running = invocation.Running(proc)
    running.request_stop()
    assert running.wait() == 0
    assert proc.timeouts == [0.2] * 4 and proc.killed is False


# --- the parent-death signal (`_find_prctl`, `_parent_death_signal`) ----------------------------------------------------


def test_prctl_is_looked_up_on_linux_only() -> None:
    assert invocation._find_prctl("darwin") is None
    found = invocation._find_prctl("linux")
    assert (found is not None) is LINUX
    assert invocation._prctl() is invocation._prctl() and (invocation._prctl() is None) is not LINUX


@pytest.mark.skipif(not LINUX, reason="prctl exists on Linux only (the mutation runs are Linux)")
def test_prctl_reports_its_errno() -> None:
    prctl = invocation._find_prctl("linux")
    ctypes.set_errno(0)
    assert prctl(-1, 0, 0, 0, 0) == -1 and ctypes.get_errno() == errno.EINVAL


def in_a_child(arm: Any, grandchild: bool) -> tuple[int, int | None]:
    """Run `arm` in a forked child (or in a child's child), as `subprocess` runs a `preexec_fn` in a task process
    whose SIGTERM handler returns (Airflow 3's task runner calls `on_kill` and returns); (how that process ended as
    os.waitstatus, the parent-death signal it reported)."""
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:  # pragma: no cover - the child
        try:
            if grandchild:
                inner = os.fork()
                if inner:
                    _, status = os.waitpid(inner, 0)
                    os.write(write, (json.dumps({"status": status}) + "\n").encode())
                    os._exit(0)
            os.close(read)
            signal.signal(signal.SIGTERM, lambda signum, frame: None)
            try:
                arm()
            except BaseException:
                os._exit(3)
            got = ctypes.c_int(-1)
            invocation._prctl()(2, ctypes.byref(got), 0, 0, 0)  # PR_GET_PDEATHSIG
            os.write(write, (json.dumps({"pdeathsig": got.value}) + "\n").encode())
            os._exit(0)
        finally:
            os._exit(4)
    os.close(write)
    _, status = os.waitpid(pid, 0)
    with os.fdopen(read) as pipe:
        report = {k: v for line in pipe.read().splitlines() for k, v in json.loads(line).items()}
    return report.get("status", status), report.get("pdeathsig")


@pytest.mark.skipif(not LINUX, reason="PR_SET_PDEATHSIG exists on Linux only (the mutation runs are Linux)")
def test_the_started_process_asks_for_sigterm_when_its_parent_dies() -> None:
    status, pdeathsig = in_a_child(invocation._parent_death_signal(), grandchild=False)
    assert os.waitstatus_to_exitcode(status) == 0 and pdeathsig == signal.SIGTERM


@pytest.mark.skipif(not LINUX, reason="PR_SET_PDEATHSIG exists on Linux only (the mutation runs are Linux)")
def test_a_process_whose_parent_died_before_the_call_stops_itself() -> None:
    """The task process died before `prctl` took effect (here: the process that runs the call is not a child of the
    one that armed it): it must end before it can exec rivet as an orphan, whatever SIGTERM handler the task process
    installed. A SIGTERM to itself ran Airflow 3's handler, which returns, and the child went on to exec rivet."""
    status, pdeathsig = in_a_child(invocation._parent_death_signal(), grandchild=True)
    assert os.waitstatus_to_exitcode(status) == -signal.SIGKILL, (status, pdeathsig)


def test_without_prctl_there_is_no_preexec_function(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(invocation, "_prctl", lambda: None)
    assert invocation._parent_death_signal() is None


def test_utc_is_used_for_a_naive_record(tmp_path: Path) -> None:
    """A naive `expires_at` is read as UTC: one an hour behind UTC-now is expired whatever the local zone."""
    behind = (datetime.now(UTC) - timedelta(hours=1)).replace(tzinfo=None).isoformat()
    assert invocation._unexpired(artifact(tmp_path, behind)) is False
