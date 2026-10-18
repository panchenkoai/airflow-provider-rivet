"""The invocation module's file helpers at their edges: private directories and files, the atomic copy, the stderr
tail, the temp base, a plan's expiry, the names of copies and hashes, the plan inputs digest and the pruning of old
artifacts, ledgers and logs. Mutant ids (`x_<fn>` N) are the mutmut names of airflow_provider_rivet/invocation.py."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow_provider_rivet import invocation
from airflow_provider_rivet.preflight import PreflightRefusal


def mode(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


# --- private directories and files ----------------------------------------------------------------------------------


def test_a_private_directory_that_exists_with_another_mode_is_tightened_to_0700(tmp_path: Path) -> None:
    """_private_dir 19: an existing 0701 directory (one bit off) is made 0700."""
    (tmp_path / "a").mkdir(mode=0o701)
    os.chmod(tmp_path / "a", 0o701)
    assert invocation._private_dir(tmp_path, "a", "b") == tmp_path / "a" / "b"
    assert (mode(tmp_path / "a"), mode(tmp_path / "a" / "b")) == (0o700, 0o700)


def test_a_private_directory_that_cannot_be_created_is_refused_with_its_code_and_cause(tmp_path: Path) -> None:
    """_private_dir 9/10/13/14/15: a file where the directory goes."""
    (tmp_path / "f").write_text("")
    with pytest.raises(PreflightRefusal) as refused:
        invocation._private_dir(tmp_path, "f", "x")
    assert refused.value.code == "RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE"
    assert (
        str(refused.value) == f"[RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE] cannot create {tmp_path / 'f' / 'x'}: NotADirectoryError"
    )


def test_a_private_directory_that_cannot_be_tightened_is_refused_with_its_code_and_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_private_dir 25-31: another user's 0755 directory cannot be chmod-ed."""
    (tmp_path / "theirs").mkdir(mode=0o755)
    os.chmod(tmp_path / "theirs", 0o755)

    def chmod(path: object, mode: int) -> None:
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(invocation.os, "chmod", chmod)
    with pytest.raises(PreflightRefusal) as refused:
        invocation._private_dir(tmp_path, "theirs")
    assert (
        str(refused.value) == f"[RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE] cannot make {tmp_path / 'theirs'} private: PermissionError"
    )
    assert refused.value.code == "RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE"


def test_a_private_file_is_opened_for_binary_writing_with_mode_0600(tmp_path: Path) -> None:
    """_open_private 19: the handle writes bytes; an earlier 0644 file of the name becomes 0600."""
    path = tmp_path / "err.log"
    path.write_text("old")
    os.chmod(path, 0o644)
    with invocation._open_private(path) as handle:
        handle.write(b"new")
    assert (path.read_bytes(), mode(path)) == (b"new", 0o600)


def test_an_atomic_write_renames_a_hidden_temp_file_from_the_target_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_write_atomic 4-7: the temp file lies beside the target, named `.<name>.*`, so a reader of the directory never
    takes a partial copy for the file and the rename never crosses a file system."""
    renamed = []
    replace = os.replace
    monkeypatch.setattr(invocation.os, "replace", lambda src, dst: (renamed.append(Path(src)), replace(src, dst)))
    target = tmp_path / "cfg.yaml"
    assert invocation._write_atomic(target, b"x") == str(target)
    [src] = renamed
    assert src.parent == tmp_path and src.name.startswith(".cfg.yaml.") and target.read_bytes() == b"x"


# --- the stderr tail ------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("extra", [1, 7])
def test_a_long_stderr_is_read_from_the_first_whole_line_of_its_last_tail_bytes(tmp_path: Path, extra: int) -> None:
    """_tail 10/11/12/23-29: the cut starts `extra` bytes into the file, inside the first line; the tail is every
    whole line after it (several lines, so `rfind` is not `find`), byte-exact."""
    path = tmp_path / "e.log"
    first = b"x" * (extra + 3) + b"\n"
    body = first + b"second line\n" + b"y" * (invocation.STDERR_TAIL - 22) + b"\nlast\n"
    path.write_bytes(body)
    tail = invocation._tail(path)
    start = len(body) - invocation.STDERR_TAIL
    assert start == extra
    window = body[start:]
    assert tail == window[window.find(b"\n") + 1 :].decode()
    assert tail.startswith("second line\n") and tail.count("\n") >= 2


def test_a_cut_that_lands_on_a_newline_keeps_the_line_after_it(tmp_path: Path) -> None:
    """_tail 12: the window starts exactly at the first line's newline, so the tail is everything after that newline
    (seeking one byte further would drop the next whole line)."""
    path = tmp_path / "e.log"
    extra = 4
    body = b"x" * extra + b"\n" + b"second line\n" + b"y" * (invocation.STDERR_TAIL - 19) + b"\nlast\n"
    assert len(body) == invocation.STDERR_TAIL + extra
    path.write_bytes(body)
    assert invocation._tail(path) == body[extra + 1 :].decode()


def test_an_undecodable_byte_in_the_tail_is_replaced_not_raised(tmp_path: Path) -> None:
    """_tail 31/32."""
    path = tmp_path / "e.log"
    path.write_bytes(b"bad \xff byte\n")
    assert invocation._tail(path) == "bad � byte\n"


# --- the temp base --------------------------------------------------------------------------------------------------


@pytest.fixture
def temp_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(invocation.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    return tmp_path / f"rivet-airflow-{os.getuid()}"


def test_the_temp_base_is_this_users_0700_directory_when_it_exists(temp_root: Path) -> None:
    """_temp_base 13/15/18: our own 0700 directory is used as it is."""
    temp_root.mkdir(mode=0o700)
    os.chmod(temp_root, 0o700)
    assert invocation._temp_base() == temp_root


@pytest.mark.parametrize("shape", ["file", "loose_dir", "other_owner"])
def test_a_temp_base_name_that_is_not_safely_ours_gives_a_fresh_random_directory(
    temp_root: Path, monkeypatch: pytest.MonkeyPatch, shape: str
) -> None:
    """_temp_base 12/20-22: a file of that name (mode 0700, ours), a 0755 directory, or another user's directory."""
    if shape == "file":
        temp_root.write_text("")
        os.chmod(temp_root, 0o700)
    else:
        temp_root.mkdir(mode=0o755)
        os.chmod(temp_root, 0o700 if shape == "other_owner" else 0o755)
    if shape == "other_owner":
        real = os.lstat
        monkeypatch.setattr(
            invocation.os,
            "lstat",
            lambda p: SimpleNamespace(st_mode=real(p).st_mode, st_uid=os.getuid() + 1) if Path(p) == temp_root else real(p),
        )
    fresh = invocation._temp_base()
    assert fresh != temp_root and fresh.parent == temp_root.parent and fresh.name.startswith("rivet-airflow-")
    assert fresh.is_dir() and mode(fresh) == 0o700


# --- a sealed plan's expiry -----------------------------------------------------------------------------------------


def write_plan(path: Path, expires: str) -> Path:
    path.write_text(json.dumps({"expires_at": expires}))
    return path


def test_a_naive_expiry_is_read_as_utc(tmp_path: Path) -> None:
    """_unexpired 16/17/18: a time without a zone is UTC, an hour ahead is unexpired, an hour ago expired."""
    ahead = (datetime.now(UTC) + timedelta(hours=1)).replace(tzinfo=None).isoformat()
    ago = (datetime.now(UTC) - timedelta(hours=1)).replace(tzinfo=None).isoformat()
    assert invocation._unexpired(write_plan(tmp_path / "a.json", ahead)) is True
    assert invocation._unexpired(write_plan(tmp_path / "b.json", ago)) is False


def test_a_plan_expiring_this_instant_is_expired(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """_unexpired 19: `expires_at` equal to now is no longer ahead."""
    instant = datetime(2026, 10, 17, 12, 0, tzinfo=UTC)

    class Frozen(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:  # type: ignore[override]
            return instant

    monkeypatch.setattr(invocation, "datetime", Frozen)
    assert invocation._unexpired(write_plan(tmp_path / "p.json", "2026-10-17T12:00:00Z")) is False
    assert invocation._unexpired(write_plan(tmp_path / "q.json", "2026-10-17T12:00:00.000001Z")) is True


# --- names ----------------------------------------------------------------------------------------------------------


def test_a_copy_of_a_config_without_a_suffix_is_named_yaml(tmp_path: Path) -> None:
    """copy_name 11/12/13."""
    source = tmp_path / "rivet"
    digest = hashlib.sha256(str(source).encode()).hexdigest()[:8]
    assert invocation.copy_name(source) == f"rivet.{digest}.yaml"
    assert invocation.copy_name(tmp_path / "r.yml") == f"r.{hashlib.sha256(str(tmp_path / 'r.yml').encode()).hexdigest()[:8]}.yml"


def test_a_hashed_name_joins_its_parts_with_two_underscores_and_keeps_80_readable_characters() -> None:
    """hashed_name 5/11."""
    digest = hashlib.sha256(json.dumps(["a b", "c"]).encode()).hexdigest()[:12]
    assert invocation.hashed_name("a b", "c") == f"a_b__c-{digest}"
    long = invocation.hashed_name("x" * 100)
    assert long.rsplit("-", 1)[0] == "x" * 80


# --- the plan inputs digest -----------------------------------------------------------------------------------------


def plan_session(config: Path, exports: list) -> SimpleNamespace:
    return SimpleNamespace(req=SimpleNamespace(config=str(config)), pf=SimpleNamespace(config={"exports": exports}))


def test_the_plan_inputs_digest_is_16_hex_of_length_prefixed_parts(tmp_path: Path) -> None:
    """_plan_inputs 33/36: SHA-256 over each part with its length in 8 big-endian bytes, 16 hex digits."""
    config = tmp_path / "rivet.yaml"
    config.write_bytes(b"exports: []\n")
    (tmp_path / "q.sql").write_bytes(b"select 1")
    exports = [{"name": "orders", "query_file": "q.sql"}]
    digest = hashlib.sha256()
    for part in (str(config.resolve()).encode(), b"exports: []\n", b"select 1"):
        digest.update(len(part).to_bytes(8, "big") + part)
    assert invocation._plan_inputs(plan_session(config, exports), "orders") == digest.hexdigest()[:16]


def test_an_unreadable_query_file_counts_as_an_empty_one(tmp_path: Path) -> None:
    """_plan_inputs 24/25: a query file that cannot be read is an empty part, the digest of an empty file."""
    config = tmp_path / "rivet.yaml"
    config.write_bytes(b"x")
    exports = [{"name": "orders", "query_file": "q.sql"}]
    missing = invocation._plan_inputs(plan_session(config, exports), "orders")
    (tmp_path / "q.sql").write_bytes(b"")
    assert invocation._plan_inputs(plan_session(config, exports), "orders") == missing


# --- pruning --------------------------------------------------------------------------------------------------------

NOW = 2_000_000_000.0


def aged(path: Path, body: str, mtime: float) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    os.utime(path, (mtime, mtime))
    return path


def test_pruning_uses_the_given_now_and_keeps_a_file_exactly_at_the_cutoff(tmp_path: Path) -> None:
    """_prune_stale 4/15: `now` is the given one, not the clock; a file touched exactly STALE_AFTER ago stays."""
    cutoff = NOW - invocation.STALE_AFTER
    at_cutoff = aged(tmp_path / "crashed" / "a", "[[1, 2]]", cutoff)
    older = aged(tmp_path / "crashed" / "b", "[[1, 2]]", cutoff - 1)
    invocation._prune_stale(tmp_path, now=NOW)
    assert at_cutoff.exists() and not older.exists()


def test_pruning_reads_each_kind_by_its_own_rule_and_goes_on_past_a_kept_file(tmp_path: Path) -> None:
    """_prune_stale 18/24: a crashed file that is not a ledger stays (not read as a plan); after an unexpired plan
    the next expired plan is still removed."""
    old = NOW - invocation.STALE_AFTER - 10
    not_ledger = aged(tmp_path / "crashed" / "x", '{"not": "a ledger"}', old)
    live = aged(tmp_path / "plans" / "r1" / "a.json", json.dumps({"expires_at": "2999-01-01T00:00:00Z"}), old)
    dead = aged(tmp_path / "plans" / "r1" / "b.json", json.dumps({"expires_at": "2001-01-01T00:00:00Z"}), old)
    dead2 = aged(tmp_path / "plans" / "r1" / "c.json", json.dumps({"expires_at": "2001-01-01T00:00:00Z"}), old)
    invocation._prune_stale(tmp_path, now=NOW)
    assert not_ledger.exists() and live.exists() and not dead.exists() and not dead2.exists()


def test_a_kind_that_cannot_be_listed_does_not_stop_the_other_kind_from_being_pruned(tmp_path: Path) -> None:
    """_prune_stale 16: listing `plans` fails; the old crashed ledger is still removed."""

    class Unlistable(type(tmp_path)):  # type: ignore[misc]
        def rglob(self, pattern: str, **kwargs: object):  # type: ignore[override]
            if self.name == "plans":
                raise PermissionError(13, "Permission denied")
            return super().rglob(pattern, **kwargs)

    ledger = aged(tmp_path / "crashed" / "l", "[[1, 2]]", NOW - invocation.STALE_AFTER - 10)
    invocation._prune_stale(Unlistable(tmp_path), now=NOW)
    assert not ledger.exists()


# --- result entries -------------------------------------------------------------------------------------------------


def test_entries_that_are_not_a_list_are_dropped_and_recorded() -> None:
    """_entries 14: a number where the list goes."""
    s = SimpleNamespace(degraded=set())
    assert invocation._entries(s, {"per_export": 5}, "per_export") == []
    assert s.degraded == {"result_malformed"}
