"""The S3 destination leg: what it reads from `rivet init`'s configs, the stand it lays out, and its oracles, each RED
on the mutant it exists to catch (the Parquet in MinIO, ClickHouse read back, the missing-bucket refusal).

The fixtures are init's output (rivet 0.32.1) on the gate's postgres scenario, as the leg runs it.
"""

from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from test_stand_hygiene import Loader, compose_view, laid_out

from gate import __main__ as gate_main
from gate import destinations, oracle
from gate.destinations import CONFIGS, MISSING_BUCKET_CODE, S3Leg, check_shape, init_argv, missing_bucket, table_parts
from gate.scenario import Verdicts
from gate.sources import read_config

FIXTURES = Path(__file__).parent / "fixtures"
TABLES = list(destinations.TABLES)


# --- init's configs ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["batch", "cdc", "ch_batch", "ch_cdc", "cleanup"])
def test_the_configs_init_writes_have_the_shape_the_leg_runs(kind: str) -> None:
    text = (FIXTURES / f"init_s3_{kind}_0.32.1.yaml").read_text()
    shape = read_config(text, TABLES)
    assert check_shape(kind, shape) == []
    doc = yaml.safe_load(text)
    dests = [e["destination"] for e in doc["exports"]]
    assert {d["type"] for d in dests} == {"s3"} and {d["bucket"] for d in dests} == {CONFIGS[kind].bucket}
    assert {d.get("region") for d in dests} == {destinations.REGION}
    # init names no endpoint and no key variable: both reach rivet only from the `aws` Connection's environment.
    assert not [d for d in dests if {"endpoint", "access_key_env", "secret_key_env"} & set(d)]
    load = doc.get("load")
    if CONFIGS[kind].clickhouse_db:
        assert load["target"] == "clickhouse" and load["database"] == CONFIGS[kind].clickhouse_db
        assert load["password_env"] == "CLICKHOUSE_PASSWORD"
        # The cleanup cells (gate/cleanup.py) grade the default init writes, not one the gate sets.
        assert load["cleanup_source"] is True
    else:
        assert load is None


def test_a_config_init_wrote_otherwise_fails_the_leg() -> None:
    cdc = read_config((FIXTURES / "init_s3_cdc_0.32.1.yaml").read_text(), TABLES)
    assert check_shape("cdc", {**cdc, "streams": []}), "no stream"
    assert check_shape("cdc", {**cdc, "extra": ["orders_archive"]}), "an export of another table"
    batch = read_config((FIXTURES / "init_s3_batch_0.32.1.yaml").read_text(), TABLES)
    assert check_shape("batch", {**batch, "exports": ["gate_orders"]}), "a table missing"
    assert check_shape("batch", {**batch, "streams": cdc["streams"]}), "a stream in a batch config"


def test_init_gets_the_bucket_and_region_and_no_url_and_clickhouse_only_for_the_load_half() -> None:
    for kind, cfg in CONFIGS.items():
        argv = init_argv(cfg, "/o.yaml")
        assert argv[argv.index("--s3-bucket") + 1] == cfg.bucket
        assert argv[argv.index("--s3-region") + 1] == destinations.REGION
        assert ("--clickhouse-url" in argv) == bool(cfg.clickhouse_db), kind
        assert not [a for a in argv if "://" in a and "clickhouse" not in a], "no source URL on init's command line"
        assert argv[argv.index("--include") + 1 : argv.index("-o")] == TABLES
    # The extract-only half and the load half read different servers: init's slot `rivet_public_cdc` is one per server.
    assert CONFIGS["cdc"].service != CONFIGS["ch_cdc"].service
    assert len({c.bucket for c in CONFIGS.values()}) == len(CONFIGS)
    assert len({c.clickhouse_db for c in CONFIGS.values() if c.clickhouse_db}) == 3


# --- The stand -----------------------------------------------------------------------------------------------------------


def test_the_stand_runs_minio_and_hands_rivet_its_endpoint_through_an_aws_connection(tmp_path: Path) -> None:
    stand = laid_out(tmp_path, destinations=(S3Leg(),))
    overlay = yaml.load((stand.dir / "docker-compose.gate.yaml").read_text(), Loader=Loader)
    minio = overlay["services"]["gate-minio"]
    assert minio["environment"]["MINIO_ROOT_PASSWORD"] == stand.secrets.minio
    assert overlay["services"]["gate-source-s3"]["environment"]["POSTGRES_PASSWORD"] == stand.secrets.source_s3
    worker = compose_view(stand)["airflow-worker"]["environment"]
    s3 = json.loads(worker["AIRFLOW_CONN_RIVET_GATE_S3"])
    assert s3["conn_type"] == "aws" and s3["password"] == stand.secrets.minio
    assert s3["extra"] == {"region_name": "us-east-1", "endpoint_url": "http://gate-minio:9000"}
    for kind in CONFIGS:
        assert f"AIRFLOW_CONN_RIVET_GATE_STATE_S3_{kind.upper()}" in worker
    assert worker["GATE_DESTINATIONS"] == "s3"


def test_no_destination_leg_leaves_the_stand_as_it_was(tmp_path: Path) -> None:
    overlay = (laid_out(tmp_path).dir / "docker-compose.gate.yaml").read_text()
    assert "gate-minio" not in overlay and "gate-source-s3" not in overlay


def test_an_unknown_destination_is_refused(capsys) -> None:
    with pytest.raises(SystemExit):
        gate_main.main(["--rivet-bin", "x", "--destinations", "azure"])
    assert "unknown destination" in capsys.readouterr().err


# --- The Parquet in the bucket ---------------------------------------------------------------------------------------

T0 = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


def test_parts_are_told_apart_by_table_and_role() -> None:
    root = "s3://b/"
    files = [
        "s3://b/exports/gate_orders/run1/part-0.parquet",
        "s3://b/exports/gate_orders_v2/part-0.parquet",
        "s3://b/cdc/gate_orders/snapshot/part-0.parquet",
        "s3://b/cdc/gate_orders/part-1.parquet",
        "s3://b/cdc/gate_customers/part-1.parquet",
    ]
    assert table_parts(files, root, "gate_orders") == {
        "batch": ["s3://b/exports/gate_orders/run1/part-0.parquet"],
        "snapshot": ["s3://b/cdc/gate_orders/snapshot/part-0.parquet"],
        "changes": ["s3://b/cdc/gate_orders/part-1.parquet"],
    }


class FakeStand:
    """The stand's reads the Parquet oracles make: XComs (the rows rivet reported) and the stored cursors."""

    def __init__(self, reported: dict[tuple[str, str], int], cursors: dict[str, str]) -> None:
        self.reported, self.cursors = reported, cursors

    def xcom(self, dag: str, run_id: str, task: str) -> dict:
        return {"units": [{"rows": self.reported.get((dag, task), 0)}]}

    def psql(self, service: str, db: str, sql: str) -> str:
        return "".join(f"{k}|{v}\n" for k, v in self.cursors.items())


@pytest.fixture
def con():
    """DuckDB with the scenario's two tables in an in-memory `src.public`."""
    c = oracle.connect(None)
    c.execute("ATTACH ':memory:' AS src; CREATE SCHEMA src.public")
    c.execute("CREATE TABLE src.public.gate_customers (id INTEGER, name VARCHAR, tier VARCHAR, updated_at TIMESTAMPTZ)")
    c.execute(
        "CREATE TABLE src.public.gate_orders (id INTEGER, customer_id INTEGER, amount DECIMAL(12,2), status VARCHAR, "
        "updated_at TIMESTAMPTZ)"
    )
    c.execute(
        "INSERT INTO src.public.gate_customers SELECT i, 'c' || i, 'free', TIMESTAMPTZ '2026-10-20 12:00:00+00' FROM range(1, 6) t(i)"
    )
    c.execute(
        "INSERT INTO src.public.gate_orders SELECT i, i, i * 1.37, 'new', TIMESTAMPTZ '2026-10-20 12:00:00+00' FROM range(1, 11) t(i)"
    )
    yield c
    c.close()


def put(con, path: Path, sql: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT parquet)")
    return con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]


def snapshot_and_batch(con, root: Path) -> dict[tuple[str, str], int]:
    """A faithful initial cycle in both buckets: each table whole in the batch bucket and as the stream's snapshot."""
    reported = {}
    for t in TABLES:
        reported[("gate_s3_batch", f"apply.{t}")] = put(
            con, root / "gate-s3-batch" / "exports" / t / "run1" / "part-0.parquet", f"SELECT * FROM src.public.{t}"
        )
        put(con, root / "gate-s3-cdc" / "cdc" / t / "snapshot" / "part-0.parquet", f"SELECT * FROM src.public.{t}")
    reported[("gate_s3_cdc", "run")] = sum(con.execute(f"SELECT count(*) FROM src.public.{t}").fetchone()[0] for t in TABLES)
    return reported


def change(con) -> None:
    """The delta cycle at the source: one update, one insert, one delete of gate_orders."""
    con.execute(
        "UPDATE src.public.gate_orders SET status = 'shipped', updated_at = TIMESTAMPTZ '2026-10-20 12:01:00+00' WHERE id = 1"
    )
    con.execute("INSERT INTO src.public.gate_orders VALUES (11, 1, 9.99, 'new', TIMESTAMPTZ '2026-10-20 12:01:00+00')")
    con.execute("DELETE FROM src.public.gate_orders WHERE id = 10")


DELTA = "SELECT * FROM src.public.gate_orders WHERE updated_at > TIMESTAMPTZ '2026-10-20 12:00:00+00'"
EVENTS = (
    'SELECT op AS __op, \'{"lsn":"0/16B3F28"}\' AS __pos, seq AS __seq, id::INTEGER AS id, customer_id::INTEGER AS customer_id, '
    "amount::DECIMAL(12,2) AS amount, status, TIMESTAMPTZ '2026-10-20 12:00:00+00' + to_minutes(m) AS updated_at "
    "FROM (VALUES {rows}) t(op, seq, id, customer_id, amount, status, m)"
)
GOOD_EVENTS = (
    "('update', 1, 1, 1, 1.37, 'shipped', 1), ('insert', 2, 11, 1, 9.99, 'new', 1), ('delete', 3, 10, 10, 13.70, 'new', 0)"
)
OPS = {"gate_customers": {}, "gate_orders": {"insert": 1, "update": 1, "delete": 1}}


def delta(con, root: Path, batch_sql: str = DELTA, events: str = GOOD_EVENTS) -> dict[tuple[str, str], int]:
    """The delta cycle's parts: the new orders rows in the batch bucket, their events in the stream's."""
    n = put(con, root / "gate-s3-batch" / "exports" / "gate_orders" / "run2" / "part-0.parquet", batch_sql)
    e = put(con, root / "gate-s3-cdc" / "cdc" / "gate_orders" / "part-1.parquet", EVENTS.replace("{rows}", events))
    return {("gate_s3_batch", "apply.gate_orders"): n, ("gate_s3_cdc", "run"): e}


def leg_on(root: Path) -> S3Leg:
    leg = S3Leg()
    leg.root = lambda bucket: f"{root}/{bucket}/"  # type: ignore[method-assign]
    return leg


def grade_parquet(leg: S3Leg, con, name: str, reported: dict, cursors: dict, monkeypatch) -> dict[str, str]:
    """The leg's Parquet cells for one cycle; FAIL cells with their detail."""
    monkeypatch.setattr(destinations, "DELETED_BY_BATCH", {"initial": {}, "delta": {"gate_orders": 1}})
    from gate import scenario

    monkeypatch.setattr(scenario, "CHANGED_OPS", OPS)
    v = Verdicts("3.3.2")
    leg.grade_parquet(FakeStand(reported, cursors), v, name, f"gate_{name}", con)
    assert v.rows, "no cell graded"
    return {r["cell"]: r["detail"] for r in v.rows if r["status"] == "FAIL"}


CURSORS0 = {t: "2026-10-20T12:00:00Z" for t in TABLES}
CURSORS1 = {**CURSORS0, "gate_orders": "2026-10-20T12:01:00Z"}


def two_cycles(con, tmp_path: Path, monkeypatch, **mutant) -> dict[str, str]:
    leg = leg_on(tmp_path)
    first = grade_parquet(leg, con, "initial", snapshot_and_batch(con, tmp_path), CURSORS0, monkeypatch)
    assert first == {}, first
    change(con)
    reported = delta(con, tmp_path, **{k: v for k, v in mutant.items() if k in ("batch_sql", "events")})
    reported.update(mutant.get("reported", {}))
    return grade_parquet(leg, con, "delta", reported, mutant.get("cursors", CURSORS1), monkeypatch)


def test_the_parquet_oracle_passes_a_faithful_bucket(con, tmp_path: Path, monkeypatch) -> None:
    assert two_cycles(con, tmp_path, monkeypatch) == {}


@pytest.mark.parametrize(
    ("mutant", "cell"),
    [
        ({"batch_sql": f"{DELTA} AND id <> 11"}, "delta/s3/batch/gate_orders/delta-values"),
        ({"batch_sql": "SELECT * FROM src.public.gate_orders"}, "delta/s3/batch/gate_orders/delta-values"),
        (
            {"batch_sql": f"SELECT id, customer_id, amount + 0.01 AS amount, status, updated_at FROM ({DELTA})"},
            "delta/s3/batch/gate_orders/latest-equals-source",
        ),
        (
            {"events": "('update', 1, 1, 1, 1.37, 'shipped', 1), ('insert', 2, 11, 1, 9.99, 'new', 1)"},
            "delta/s3/cdc/gate_orders/replay-equals-source",
        ),
        (
            {"events": "('update', 1, 1, 1, 1.37, 'shipped', 1), ('insert', 2, 11, 1, 9.99, 'new', 1)"},
            "delta/s3/cdc/gate_orders/operations",
        ),
        ({"reported": {("gate_s3_batch", "apply.gate_orders"): 1}}, "delta/s3/batch/gate_orders/xcom-rows"),
        ({"reported": {("gate_s3_cdc", "run"): 2}}, "delta/s3/cdc/xcom-rows"),
        ({"cursors": CURSORS0}, "delta/s3/batch/gate_orders/stored-cursor"),
    ],
    ids=["lost-row", "whole-table-again", "wrong-value", "lost-delete", "lost-delete-ops", "xcom-batch", "xcom-cdc", "cursor"],
)
def test_the_parquet_oracle_fails_each_mutant(con, tmp_path: Path, monkeypatch, mutant: dict, cell: str) -> None:
    assert cell in two_cycles(con, tmp_path, monkeypatch, **mutant)


def test_a_part_dropped_from_the_bucket_fails_the_delta(con, tmp_path: Path, monkeypatch) -> None:
    leg = leg_on(tmp_path)
    assert grade_parquet(leg, con, "initial", snapshot_and_batch(con, tmp_path), CURSORS0, monkeypatch) == {}
    change(con)
    reported = delta(con, tmp_path)
    (tmp_path / "gate-s3-batch" / "exports" / "gate_orders" / "run2" / "part-0.parquet").unlink()
    assert "delta/s3/batch/gate_orders/delta-values" in grade_parquet(leg, con, "delta", reported, CURSORS1, monkeypatch)


# --- ClickHouse read back ---------------------------------------------------------------------------------------------

SOURCE = {
    "gate_customers": [{"id": 1, "name": "c1", "tier": "pro", "updated_at": T0}],
    "gate_orders": [
        {"id": 1, "customer_id": 2, "amount": Decimal("1.37"), "status": "new", "updated_at": T0},
        {
            "id": 2,
            "customer_id": 3,
            "amount": Decimal("13.70"),
            "status": "shipped",
            "updated_at": T0 + timedelta(microseconds=1),
        },
    ],
}
GONE = {"id": 9, "customer_id": 1, "amount": Decimal("2.00"), "status": "new", "updated_at": T0}


def ch(row: dict) -> dict:
    """A row as ClickHouse's JSONEachRow hands it back: ISO instants, numbers as JSON numbers."""
    return {k: (v.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if isinstance(v, datetime) else v) for k, v in row.items()}


def faithful_clickhouse() -> dict[tuple[str, str], list[dict]]:
    rows = {}
    for t, src in SOURCE.items():
        rows[("gate_s3_batch", t)] = [ch(r) for r in src] + ([ch(GONE)] if t == "gate_orders" else [])
        rows[("gate_s3_cdc", t)] = [ch({**r, "__is_deleted": False}) for r in src] + (
            [ch({**GONE, "__is_deleted": True})] if t == "gate_orders" else []
        )
    return rows


def grade_clickhouse(monkeypatch, rows: dict) -> dict[str, str]:
    monkeypatch.setattr(destinations, "DELETED_BY_BATCH", {"delta": {"gate_orders": 1}})
    monkeypatch.setattr(S3Leg, "source_rows", lambda self, dsn: copy.deepcopy(SOURCE))
    monkeypatch.setattr(S3Leg, "read_clickhouse", lambda self, stand, db, t: copy.deepcopy(rows[(db, t)]))
    v = Verdicts("3.3.2")
    S3Leg().grade_clickhouse(SimpleNamespace(), v, "delta")
    assert len(v.rows) == 4
    return {r["cell"]: r["detail"] for r in v.rows if r["status"] == "FAIL"}


def test_the_clickhouse_oracle_passes_a_faithful_load(monkeypatch) -> None:
    assert grade_clickhouse(monkeypatch, faithful_clickhouse()) == {}


@pytest.mark.parametrize(
    ("db", "change", "cell"),
    [
        (
            "gate_s3_batch",
            lambda rows: rows[0].update(amount=1.38),
            "delta/s3/clickhouse/batch/gate_orders/current-equals-source",
        ),
        ("gate_s3_batch", lambda rows: rows.pop(1), "delta/s3/clickhouse/batch/gate_orders/current-equals-source"),
        ("gate_s3_batch", lambda rows: rows.pop(), "delta/s3/clickhouse/batch/gate_orders/current-equals-source"),
        (
            "gate_s3_cdc",
            lambda rows: rows[-1].update(__is_deleted=False),
            "delta/s3/clickhouse/cdc/gate_orders/current-equals-source",
        ),
        (
            "gate_s3_cdc",
            lambda rows: rows[1].update(updated_at="2026-10-20T12:00:00.000000Z"),
            "delta/s3/clickhouse/cdc/gate_orders/current-equals-source",
        ),
    ],
    ids=["a-cent", "lost-row", "deleted-key-gone-from-batch", "delete-not-flagged", "a-microsecond"],
)
def test_the_clickhouse_oracle_fails_each_mutant(monkeypatch, db: str, change, cell: str) -> None:
    rows = faithful_clickhouse()
    change(rows[(db, "gate_orders")])
    assert cell in grade_clickhouse(monkeypatch, rows)


# --- The missing bucket ----------------------------------------------------------------------------------------------


def stopped(**over) -> tuple[dict, dict]:
    inst = {"state": "failed", "try_number": 1, "start_date": "2026-10-20T12:00:00Z", "end_date": "2026-10-20T12:00:04Z"}
    payload = {"decision": "fail", "error": {"code": MISSING_BUCKET_CODE}, "exit_status": 1}
    inst.update(over.get("inst", {}))
    payload.update(over.get("payload", {}))
    states = {"apply.gate_customers": inst, "apply.gate_orders": {"state": "upstream_failed"}}
    states.update(over.get("states", {}))
    return states, {"apply.gate_customers": payload}


def test_a_coded_refusal_at_once_and_never_retried_passes() -> None:
    assert missing_bucket(*stopped(), 120) == []


@pytest.mark.parametrize(
    "over",
    [
        {"inst": {"state": "success"}},
        {"inst": {"try_number": 2}},
        {"inst": {"end_date": "2026-10-20T12:09:21Z"}},
        {"payload": {"error": {"code": None}}},
        {"payload": {"error": {"code": "RIVET_RUN_UNITS_FAILED"}}},
        {"payload": {"exit_status": 2, "decision": "retry"}},
        {"payload": {"exit_status": 2}},
        {"states": {"apply.gate_orders": {"state": "success"}}},
    ],
    ids=["no-stop", "retried", "nine-minutes", "uncoded", "other-code", "transient", "exit-transient", "a-sibling-ran"],
)
def test_the_missing_bucket_oracle_fails_each_mutant(over: dict) -> None:
    assert missing_bucket(*stopped(**over), 120)
