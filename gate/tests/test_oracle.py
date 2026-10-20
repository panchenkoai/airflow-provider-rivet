"""Each oracle passes on a faithful output and fails on every mutant of it the gate exists to catch."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gate import oracle

COLS = ["id", "status", "amount", "updated_at"]
T0 = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)


@pytest.fixture
def con():
    """DuckDB with an in-memory `src.public.orders` standing in for the attached PostgreSQL source."""
    c = oracle.connect(None)
    c.execute("ATTACH ':memory:' AS src; CREATE SCHEMA src.public")
    c.execute("CREATE TABLE src.public.orders (id INTEGER, status VARCHAR, amount DECIMAL(12,2), updated_at TIMESTAMPTZ)")
    c.execute(
        "INSERT INTO src.public.orders SELECT i, 'new', i * 1.5, TIMESTAMPTZ '2026-10-15 12:00:00+00' FROM range(1, 11) t(i)"
    )
    yield c
    c.close()


def write(con, path: Path, sql: str) -> str:
    """Write the rows of `sql` to a parquet file; its path."""
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")
    return str(path)


def initial(con, tmp_path: Path) -> str:
    """A faithful first incremental run: the whole table."""
    return write(con, tmp_path / "run1.parquet", "SELECT * FROM src.public.orders")


def change(con) -> None:
    """Two updates, one insert, one delete at the source, one minute after T0."""
    con.execute(
        "UPDATE src.public.orders SET status = 'shipped', updated_at = TIMESTAMPTZ '2026-10-15 12:01:00+00' WHERE id IN (1, 2)"
    )
    con.execute("INSERT INTO src.public.orders VALUES (11, 'new', 9.99, TIMESTAMPTZ '2026-10-15 12:01:00+00')")
    con.execute("DELETE FROM src.public.orders WHERE id = 10")


DELTA = "SELECT * FROM src.public.orders WHERE updated_at > TIMESTAMPTZ '2026-10-15 12:00:00+00'"


def test_batch_delta_passes_on_the_exact_delta_and_fails_on_each_mutant(con, tmp_path: Path) -> None:
    assert oracle.batch_delta(con, [initial(con, tmp_path)], "src.public.orders", COLS, "updated_at", None) == []
    change(con)
    good = write(con, tmp_path / "d.parquet", DELTA)
    assert oracle.batch_delta(con, [good], "src.public.orders", COLS, "updated_at", T0) == []
    mutants = {
        "a row lost": f"{DELTA} AND id <> 11",
        "a row twice": f"{DELTA} UNION ALL SELECT * FROM src.public.orders WHERE id = 1",
        "a stale value": f"SELECT id, CASE WHEN id = 1 THEN 'new' ELSE status END AS status, amount, updated_at FROM ({DELTA})",
        "the whole table again": "SELECT * FROM src.public.orders",
    }
    for name, sql in mutants.items():
        bad = write(con, tmp_path / f"m{len(name)}.parquet", sql)
        assert oracle.batch_delta(con, [bad], "src.public.orders", COLS, "updated_at", T0), name
    assert oracle.batch_delta(con, [], "src.public.orders", COLS, "updated_at", T0), "no file where a delta was due"


def test_batch_latest_counts_deletes_and_catches_a_lost_update(con, tmp_path: Path) -> None:
    run1 = initial(con, tmp_path)
    change(con)
    run2 = write(con, tmp_path / "d.parquet", DELTA)
    assert oracle.batch_latest(con, [run1, run2], "src.public.orders", COLS, "id", "updated_at", deleted=1) == []
    assert oracle.batch_latest(con, [run1, run2], "src.public.orders", COLS, "id", "updated_at", deleted=0)
    lost = write(con, tmp_path / "lost.parquet", f"{DELTA} AND id <> 1")
    assert oracle.batch_latest(con, [run1, lost], "src.public.orders", COLS, "id", "updated_at", deleted=1)


def events(con, tmp_path: Path, rows: str, lsn: str = "lsn") -> str:
    """A change file from a VALUES list of (op, seq, id, status, amount, minute[, lsn]); one transaction unless `lsn` is a column."""
    names = "op, seq, id, status, amount, m" + (", lsn" if lsn == "lsn_col" else "")
    pos = "'{\"lsn\":\"0/' || lsn || '\"}'" if lsn == "lsn_col" else '\'{"lsn":"0/16B3F28"}\''
    return write(
        con,
        tmp_path / f"e{abs(hash(rows))}.parquet",
        f"SELECT op AS __op, {pos} AS __pos, seq AS __seq, id::INTEGER AS id, status, amount::DECIMAL(12,2) AS amount, "
        f"TIMESTAMPTZ '2026-10-15 12:00:00+00' + to_minutes(m) AS updated_at FROM (VALUES {rows}) t({names})",
    )


def test_cdc_replay_and_operations_catch_lost_reordered_and_missing_events(con, tmp_path: Path) -> None:
    snap = initial(con, tmp_path)
    change(con)
    good = events(
        con,
        tmp_path,
        "('update', 1, 1, 'shipped', 1.5, 1), ('update', 2, 2, 'shipped', 3.0, 1), "
        "('insert', 3, 11, 'new', 9.99, 1), ('delete', 4, 10, 'new', 15.0, 0)",
    )
    assert oracle.cdc_replay(con, [snap], [[], [good]], "src.public.orders", COLS, "id") == []
    assert oracle.cdc_ops(con, [good], {"insert": 1, "update": 2, "delete": 1}, "orders") == []
    no_delete = events(
        con,
        tmp_path,
        "('update', 1, 1, 'shipped', 1.5, 1), ('update', 2, 2, 'shipped', 3.0, 1), ('insert', 3, 11, 'new', 9.99, 1)",
    )
    assert oracle.cdc_replay(con, [snap], [[no_delete]], "src.public.orders", COLS, "id")
    assert oracle.cdc_ops(con, [no_delete], {"insert": 1, "update": 2, "delete": 1}, "orders")
    reordered = events(
        con,
        tmp_path,
        "('update', 1, 1, 'shipped', 1.5, 1), ('update', 2, 1, 'new', 1.5, 0), ('update', 3, 2, 'shipped', 3.0, 1), "
        "('insert', 4, 11, 'new', 9.99, 1), ('delete', 5, 10, 'new', 15.0, 0)",
    )
    assert oracle.cdc_replay(con, [snap], [[reordered]], "src.public.orders", COLS, "id"), "the older event applied last"
    assert oracle.cdc_replay(con, [], [[good]], "src.public.orders", COLS, "id"), "no snapshot"
    assert oracle.cdc_ops(con, [], {"insert": 0}, "idle") == []


def test_cdc_replay_orders_transactions_by_lsn_before_seq(con, tmp_path: Path) -> None:
    """`__seq` restarts in every transaction: an insert and a later transaction's delete of it, numbered 7 and 0."""
    snap = initial(con, tmp_path)
    con.execute("INSERT INTO src.public.orders VALUES (11, 'new', 9.99, TIMESTAMPTZ '2026-10-15 12:01:00+00')")
    con.execute("DELETE FROM src.public.orders WHERE id = 11")
    good = events(
        con, tmp_path, "('insert', 7, 11, 'new', 9.99, 1, 'A0'), ('delete', 0, 11, 'new', 9.99, 1, 'B0')", lsn="lsn_col"
    )
    assert oracle.cdc_replay(con, [snap], [[good]], "src.public.orders", COLS, "id") == []
    swapped = events(
        con, tmp_path, "('insert', 7, 11, 'new', 9.99, 1, 'B0'), ('delete', 0, 11, 'new', 9.99, 1, 'A0')", lsn="lsn_col"
    )
    assert oracle.cdc_replay(con, [snap], [[swapped]], "src.public.orders", COLS, "id"), "the delete logged before the insert"
    hi = events(
        con,
        tmp_path,
        "('insert', 7, 11, 'new', 9.99, 1, 'FFFFFFF0'), ('delete', 0, 11, 'new', 9.99, 1, '100000000')",
        lsn="lsn_col",
    )
    assert oracle.cdc_replay(con, [snap], [[hi]], "src.public.orders", COLS, "id") == [], "LSNs compare as numbers, not text"


def test_counts_cursor_and_overlap() -> None:
    assert oracle.same_count(5, 5, "x") == [] and oracle.same_count(5, 4, "x")
    assert oracle.stored_cursor("2026-10-15T12:01:00.123456", datetime(2026, 10, 15, 12, 1, 0, 123456, tzinfo=UTC), "c") == []
    assert oracle.stored_cursor("2026-10-15T12:01:00.123455", datetime(2026, 10, 15, 12, 1, 0, 123456, tzinfo=UTC), "c")
    assert oracle.stored_cursor(None, T0, "c")
    outer = ("2026-10-15T12:00:00Z", "2026-10-15T12:00:30Z")
    assert oracle.inside([("2026-10-15T12:00:05Z", "2026-10-15T12:00:10Z")], outer, "o") == []
    assert oracle.inside([("2026-10-15T12:00:05Z", "2026-10-15T12:00:31Z")], outer, "o")
    assert oracle.inside([], outer, "o"), "nothing ran inside the window"


# --- The multi-host leg: each version once across both hosts, no cursor moved back, every try where it was routed ----


def test_duplicates_passes_once_and_catches_a_version_or_event_written_on_both_hosts(con, tmp_path: Path) -> None:
    a = initial(con, tmp_path)
    change(con)
    b = write(con, tmp_path / "b.parquet", DELTA)
    assert oracle.duplicates(con, [a, b], ["id", "updated_at"], "x") == [], "an update is a new version, not a repeat"
    assert oracle.duplicates(con, [], ["id"], "x") == []
    mutants = {
        "the delta extracted again on the other host": [a, b, write(con, tmp_path / "again.parquet", DELTA)],
        "the whole table re-read by a host with an empty state": [
            a,
            b,
            write(con, tmp_path / "full.parquet", "SELECT * FROM src.public.orders WHERE id = 3"),
        ],
    }
    for name, files in mutants.items():
        assert oracle.duplicates(con, files, ["id", "updated_at"], "x"), name
    assert oracle.duplicates(con, [a, b], ["id"], "a baseline"), "a key in two baselines"


def test_not_regressed_allows_equal_and_forward_and_catches_a_cursor_moved_back() -> None:
    assert oracle.not_regressed(None, "2026-10-15T12:00:00Z", "c") == []
    assert oracle.not_regressed("2026-10-15T12:00:00Z", "2026-10-15T12:00:00+00:00", "c") == []
    assert oracle.not_regressed("2026-10-15T12:00:00Z", "2026-10-15T12:00:01Z", "c") == []
    assert oracle.not_regressed("2026-10-15T12:00:01Z", "2026-10-15T12:00:00.999999Z", "c")
    assert oracle.not_regressed("2026-10-15T12:00:01Z", None, "c"), "a stored cursor that disappeared"


def test_hosts_as_routed_reads_each_try_and_catches_a_task_on_the_wrong_host() -> None:
    expected = {"apply.a": ["a"], "apply.b": ["a", "b"]}
    seen = {"apply.a": ["gate-host-a"], "apply.b": ["gate-host-a.local", "gate-host-b"]}
    assert oracle.hosts_as_routed(seen, {k: [f"gate-host-{h}" for h in v] for k, v in expected.items()}, "r") == []
    mutants = {
        "the retry stayed on its host": {"apply.a": ["gate-host-a"], "apply.b": ["gate-host-a", "gate-host-a"]},
        "one try missing": {"apply.a": ["gate-host-a"], "apply.b": ["gate-host-b"]},
        "a third try": {"apply.a": ["gate-host-a"], "apply.b": ["gate-host-a", "gate-host-b", "gate-host-b"]},
        "no hostname recorded": {"apply.a": [""], "apply.b": ["gate-host-a", "gate-host-b"]},
        "a task never ran": {"apply.b": ["gate-host-a", "gate-host-b"]},
    }
    for name, got in mutants.items():
        assert oracle.hosts_as_routed(got, {k: [f"gate-host-{h}" for h in v] for k, v in expected.items()}, "r"), name
