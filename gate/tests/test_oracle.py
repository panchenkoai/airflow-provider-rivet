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


def events(con, tmp_path: Path, rows: str) -> str:
    """A change file with `__op` / `__seq` from a VALUES list of (op, seq, id, status, amount, minute)."""
    return write(
        con,
        tmp_path / f"e{abs(hash(rows))}.parquet",
        "SELECT op AS __op, seq AS __seq, id::INTEGER AS id, status, amount::DECIMAL(12,2) AS amount, "
        f"TIMESTAMPTZ '2026-10-15 12:00:00+00' + to_minutes(m) AS updated_at FROM (VALUES {rows}) t(op, seq, id, status, amount, m)",
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


def test_counts_cursor_and_overlap() -> None:
    assert oracle.same_count(5, 5, "x") == [] and oracle.same_count(5, 4, "x")
    assert oracle.stored_cursor("2026-10-15T12:01:00.123456", datetime(2026, 10, 15, 12, 1, 0, 123456, tzinfo=UTC), "c") == []
    assert oracle.stored_cursor("2026-10-15T12:01:00.123455", datetime(2026, 10, 15, 12, 1, 0, 123456, tzinfo=UTC), "c")
    assert oracle.stored_cursor(None, T0, "c")
    outer = ("2026-10-15T12:00:00Z", "2026-10-15T12:00:30Z")
    assert oracle.inside([("2026-10-15T12:00:05Z", "2026-10-15T12:00:10Z")], outer, "o") == []
    assert oracle.inside([("2026-10-15T12:00:05Z", "2026-10-15T12:00:31Z")], outer, "o")
    assert oracle.inside([], outer, "o"), "nothing ran inside the window"
