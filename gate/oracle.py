"""Oracles: what rivet wrote, read back by DuckDB and compared with the source read by DuckDB's postgres scanner.

Every function returns the list of problems it found; an empty list is a pass. None of them reads rivet's own
report: the XCom and the stored cursor are compared with counts and values taken here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

import duckdb

SHOWN = 5
#: The order of PostgreSQL change events (docs/cdc-bigquery-load.md): the LSN in `__pos`, then `__seq` inside it.
PG_LSN = (
    "((('0x' || split_part(json_extract_string(__pos, '$.lsn'), '/', 1))::UBIGINT << 32) + "
    "('0x' || split_part(json_extract_string(__pos, '$.lsn'), '/', 2))::UBIGINT)"
)


def connect(source_dsn: str | None) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the source attached read-only as `src` (no source for the unit tests)."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    if source_dsn:
        con.execute("INSTALL postgres; LOAD postgres")
        con.execute(f"ATTACH '{source_dsn}' AS src (TYPE postgres, READ_ONLY)")
    return con


def _files(files: Sequence[str]) -> str:
    """A DuckDB list literal of parquet paths."""
    return "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in files) + "]"


def _diff(con: duckdb.DuckDBPyConnection, got: str, expected: str, what: str) -> list[str]:
    """Rows in one relation and not the other, both ways, counted with multiplicity."""
    problems = []
    for label, a, b in (("missing", expected, got), ("unexpected", got, expected)):
        rows = con.execute(f"SELECT * FROM (({a}) EXCEPT ALL ({b})) LIMIT {SHOWN + 1}").fetchall()
        if rows:
            total = con.execute(f"SELECT count(*) FROM (({a}) EXCEPT ALL ({b}))").fetchone()[0]
            problems.append(f"{what}: {total} {label} row(s), e.g. {rows[:SHOWN]}")
    return problems


def rows_in(con: duckdb.DuckDBPyConnection, files: Sequence[str]) -> int:
    """Rows in the given parquet files (0 for none)."""
    if not files:
        return 0
    return con.execute(f"SELECT count(*) FROM read_parquet({_files(files)}, union_by_name=true)").fetchone()[0]


def batch_delta(
    con: duckdb.DuckDBPyConnection, files: Sequence[str], source: str, columns: Sequence[str], cursor: str, after: Any
) -> list[str]:
    """The files of one incremental run hold exactly the source rows past the previous high-water, with their values."""
    cols = ", ".join(columns)
    got = (
        f"SELECT {cols} FROM read_parquet({_files(files)}, union_by_name=true)"
        if files
        else f"SELECT {cols} FROM {source} WHERE false"
    )
    where = "" if after is None else f" WHERE {cursor} > '{_instant(after).isoformat()}'::TIMESTAMPTZ"
    return _diff(con, got, f"SELECT {cols} FROM {source}{where}", f"delta of {source}")


def batch_latest(
    con: duckdb.DuckDBPyConnection, files: Sequence[str], source: str, columns: Sequence[str], key: str, cursor: str, deleted: int
) -> list[str]:
    """The newest version of every key across all runs equals the source; only the `deleted` keys are extra."""
    cols = ", ".join(columns)
    latest = (
        f"SELECT {cols} FROM (SELECT *, row_number() OVER (PARTITION BY {key} ORDER BY {cursor} DESC) AS rn "
        f"FROM read_parquet({_files(files)}, union_by_name=true)) WHERE rn = 1"
    )
    live = f"SELECT {cols} FROM ({latest}) WHERE {key} IN (SELECT {key} FROM {source})"
    problems = _diff(con, live, f"SELECT {cols} FROM {source}", f"latest versions of {source}")
    gone = con.execute(f"SELECT count(*) FROM ({latest}) WHERE {key} NOT IN (SELECT {key} FROM {source})").fetchone()[0]
    if gone != deleted:
        problems.append(f"latest versions of {source}: {gone} key(s) absent at the source, expected {deleted} deleted")
    return problems


def cdc_replay(
    con: duckdb.DuckDBPyConnection,
    snapshot: Sequence[str],
    stream: Sequence[Sequence[str]],
    source: str,
    columns: Sequence[str],
    key: str,
    position: str = PG_LSN,
) -> list[str]:
    """The snapshot with every captured change applied in log order (`position`, then `__seq`) equals the source."""
    cols = ", ".join(columns)
    parts = (
        [
            f"SELECT 0 AS run_no, 0::UBIGINT AS pos, 0 AS seq, 'insert' AS op, {cols} FROM read_parquet({_files(snapshot)}, union_by_name=true)"
        ]
        if snapshot
        else []
    )
    parts += [
        f"SELECT {n} AS run_no, {position} AS pos, __seq AS seq, __op AS op, {cols} FROM read_parquet({_files(files)}, union_by_name=true)"
        for n, files in enumerate(stream, start=1)
        if files
    ]
    if not parts:
        return [f"replay of {source}: no snapshot and no change files"]
    events = " UNION ALL BY NAME ".join(f"({p})" for p in parts)
    final = (
        f"SELECT {cols} FROM (SELECT *, row_number() OVER (PARTITION BY {key} ORDER BY run_no DESC, pos DESC, seq DESC) AS rn "
        f"FROM ({events})) WHERE rn = 1 AND op <> 'delete'"
    )
    return _diff(con, final, f"SELECT {cols} FROM {source}", f"replay of {source}")


def cdc_ops(con: duckdb.DuckDBPyConnection, files: Sequence[str], expected: Mapping[str, int], what: str) -> list[str]:
    """The change files of one run carry exactly the expected number of events per operation."""
    got: dict[str, int] = {}
    if files:
        got = dict(
            con.execute(f"SELECT __op, count(*) FROM read_parquet({_files(files)}, union_by_name=true) GROUP BY 1").fetchall()
        )
    want = {op: n for op, n in expected.items() if n}
    return [] if got == want else [f"{what}: operations {got}, expected {want}"]


def same_count(reported: int, counted: int, what: str) -> list[str]:
    """A count rivet reported equals the one read from its files."""
    return [] if reported == counted else [f"{what}: reported {reported} row(s), the files hold {counted}"]


def _instant(value: Any) -> datetime:
    """A timestamp from a datetime or a stored text form; a naive one is UTC (rivet stores the cursor in UTC)."""
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


def stored_cursor(stored: Any, source_max: Any, what: str) -> list[str]:
    """The cursor rivet stored is the source's high-water."""
    if stored is None:
        return [f"{what}: no stored cursor"]
    return [] if _instant(stored) == _instant(source_max) else [f"{what}: stored cursor {stored}, source high-water {source_max}"]


def inside(inner: Sequence[tuple[str, str]], outer: tuple[str, str], what: str) -> list[str]:
    """Every inner task ran wholly inside the outer task's window, i.e. the two processes overlapped."""
    o_start, o_end = (_instant(t) for t in outer)
    late = [(s, e) for s, e in inner if not (o_start <= _instant(s) and _instant(e) <= o_end)]
    return [f"{what}: {late} not inside {outer}"] if late or not inner else []
