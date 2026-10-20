"""The MongoDB and Oracle source legs: what the gate reads from `rivet init`'s answers, the known limits it holds init
to, and the readers that compare the warehouse with the source.

The fixtures are init's output on the gate's MongoDB and Oracle scenarios (rivet 0.32.1), the bucket and dataset renamed.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from bson import Binary, Decimal128, Int64, ObjectId
from test_stand_hygiene import Loader, compose_view, laid_out

from airflow_provider_rivet.connections import connection_url
from gate import sources, warehouse
from gate.sources import MONGODB, ORACLE, limit_holds, mongo_canonical, mongo_id, read_config, warehouse_document

FIXTURES = Path(__file__).parent / "fixtures"
#: What rivet 0.32.1's `init --mode incremental` answers for a MongoDB source (stdout and stderr).
MONGO_BATCH_REFUSAL = (
    "Error: [RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED] init: --mode incremental: source type 'Mongo' supports `mode: full` "
    "(batch) and `mode: cdc` (change streams) (got `mode: incremental`). MongoDB has no SQL, so chunked / incremental / "
    "keyset / time-window are not available; every document exports as `_id` + a `document` JSON column."
)


def test_oracle_init_names_its_batch_exports_upper_case_and_the_gate_matches_them() -> None:
    shape = read_config((FIXTURES / "init_oracle_batch_0.32.1.yaml").read_text(), list(ORACLE.tables))
    assert shape == {"exports": ["GATE_CUSTOMERS", "GATE_ORDERS"], "streams": [], "extra": []}


def test_mongodb_init_writes_one_cdc_export_and_dag_per_collection() -> None:
    shape = read_config((FIXTURES / "init_mongodb_cdc_0.32.1.yaml").read_text(), list(MONGODB.tables))
    assert shape == {
        "exports": [],
        "streams": [
            {"name": "gate_customers_cdc", "tables": ["gate_customers"], "dag": "_cdc_gate_customers_cdc"},
            {"name": "gate_orders_cdc", "tables": ["gate_orders"], "dag": "_cdc_gate_orders_cdc"},
        ],
        "extra": [],
    }


def test_the_mongodb_batch_limit_holds_only_while_init_refuses_with_its_code() -> None:
    limit = MONGODB.limits["batch"]
    tables = list(MONGODB.tables)
    assert limit_holds(limit, 1, MONGO_BATCH_REFUSAL, None, tables) == []
    assert limit_holds(limit, 0, "Config written to x", "exports: []", tables), "init writing the config lifts the limit"
    assert limit_holds(limit, 1, "Error: connection refused", None, tables), "another failure is not the documented limit"
    assert limit_holds(limit, 1, MONGO_BATCH_REFUSAL, "exports: []", tables), "a refusal leaves no config"
    assert MONGODB.kinds == ("cdc",) and "batch" not in MONGODB.kinds


def test_the_oracle_cdc_limit_holds_only_while_init_writes_no_load_block() -> None:
    limit = ORACLE.limits["cdc"]
    text = (FIXTURES / "init_oracle_cdc_0.32.1.yaml").read_text()
    tables = list(ORACLE.tables)
    assert limit_holds(limit, 0, "", text, tables) == []
    doc = yaml.safe_load(text)
    with_load = yaml.safe_dump({**doc, "load": {"target": "bigquery"}})
    assert limit_holds(limit, 0, "", with_load, tables), "a `load:` block means the leg must load Oracle CDC"
    per_export = yaml.safe_dump({**doc, "exports": [{**doc["exports"][0], "load": {}}, *doc["exports"][1:]]})
    assert limit_holds(limit, 0, "", per_export, tables)
    assert limit_holds(limit, 1, "Error: ORA-01017", None, tables), "init failing is not the limit"
    one = yaml.safe_dump({**doc, "exports": doc["exports"][:1]})
    assert limit_holds(limit, 0, "", one, tables), "a CDC config over other tables than the scenario's"


ROW_FROM_RIVET = (
    '{"_id":{"$oid":"6ad7a731676037c1f9d2f225"},"addr":{"city":"Kyiv","tags":["a","b"]},'
    '"balance":{"$numberDecimal":"12.35"},"big":9007199254740993,"name":"клиент 1","note":null,"score":1.25,'
    '"payload":{"$binary":{"base64":"AP8Q","subType":"00"}},"updated_at":{"$date":"2026-10-20T17:38:57.127Z"}}'
)


def source_document(**change) -> dict:
    """The document of ROW_FROM_RIVET as pymongo hands it back (a naive UTC datetime at milliseconds)."""
    doc = {
        "_id": ObjectId("6ad7a731676037c1f9d2f225"),
        "name": "клиент 1",
        "addr": {"city": "Kyiv", "tags": ["a", "b"]},
        "balance": Decimal128("12.35"),
        "big": Int64(9007199254740993),
        "score": 1.25,
        "note": None,
        "payload": b"\x00\xff\x10",
        "updated_at": datetime(2026, 10, 20, 17, 38, 57, 127000),
    }
    return {**doc, **change}


def test_a_mongodb_document_equals_its_warehouse_json_value_by_value() -> None:
    want = mongo_canonical(source_document())
    # BigQuery's client parses a JSON column; its text form (another client's answer) reads the same.
    assert warehouse_document(json.loads(ROW_FROM_RIVET)) == want
    assert warehouse_document(ROW_FROM_RIVET) == want


@pytest.mark.parametrize(
    "change",
    [
        {"balance": Decimal128("12.350")},  # Decimal128 keeps its digits
        {"big": Int64(9007199254740992)},  # an int64 past 2^53 rounded by a double reader
        {"score": 1.0},
        {"_id": "6ad7a731676037c1f9d2f225"},  # an ObjectId's hex as a string is another _id
        {"updated_at": datetime(2026, 10, 20, 17, 38, 57, 128000)},
        {"addr": {"city": "Kyiv", "tags": ["b", "a"]}},  # array order is the value's
        {"payload": Binary(b"\x00\xff\x10", 4)},  # binary of another subtype
        {"note": "null"},
    ],
)
def test_any_difference_of_type_or_value_in_a_document_is_seen(change: dict) -> None:
    assert warehouse_document(json.loads(ROW_FROM_RIVET)) != mongo_canonical(source_document(**change)), change


def test_an_integer_and_a_double_of_the_same_value_differ() -> None:
    assert mongo_canonical({"x": 2}) != mongo_canonical({"x": 2.0})
    assert warehouse_document({"x": 2}) == mongo_canonical({"x": Int64(2)})


def test_the_mongodb_id_text_is_rivets_stringification() -> None:
    assert mongo_id(ObjectId("6ad7a731676037c1f9d2f225")) == "6ad7a731676037c1f9d2f225"
    assert mongo_id(Int64(4996)) == "4996" and mongo_id("k") == "k"
    with pytest.raises(TypeError):
        mongo_id(1.5)


def test_an_oracle_zoned_instant_is_read_with_its_offset_and_compared_as_the_instant() -> None:
    assert "TZH:TZM" in ORACLE.select["placed_at"] and ORACLE.json_kinds["placed_at"] == "ts"
    stamp = sources._typed("ts", "2026-01-01T10:00:01.123456+03:00")
    utc = sources._typed("ts", "2026-01-01T07:00:01.123456+00:00")
    assert warehouse.canon("ts", stamp) == warehouse.canon("ts", utc) == 1767250801123456
    assert warehouse.canon("ts", sources._typed("ts", "2026-01-01T10:00:01.123456-05:30")) != warehouse.canon("ts", utc)


def test_oracle_numbers_compare_exactly_whatever_the_reader_hands_back() -> None:
    kinds = warehouse.kinds_of([{"id": Decimal("4996"), "amount": Decimal("12.35")}])
    assert warehouse.canon(kinds["id"], Decimal("4996")) == warehouse.canon(kinds["id"], 4996)
    assert warehouse.canon(kinds["amount"], Decimal("12.35")) != warehouse.canon(kinds["amount"], Decimal("12.34"))


def test_the_batch_deletes_of_each_engine_are_the_count_the_oracle_expects() -> None:
    assert "BETWEEN 4996 AND 5000" in ORACLE.changes
    deleted = sources.DELETED_BY_BATCH["delta"]
    assert [deleted.get(t.lower(), 0) for t in ORACLE.tables] == [0, 5]


def test_compaction_is_judged_on_the_upper_case_names_bigquery_keeps() -> None:
    assert warehouse.compacted({"GATE_ORDERS"}, "GATE_ORDERS", "x") == []
    assert warehouse.compacted({"GATE_ORDERS", "GATE_ORDERS__changes"}, "GATE_ORDERS", "x")
    assert warehouse.compacted({"gate_customers"}, "GATE_ORDERS", "x")


def test_the_url_init_reads_is_the_one_the_dags_build_from_the_connection(tmp_path: Path) -> None:
    stand = laid_out(tmp_path, sources=(MONGODB, ORACLE))
    for engine in (MONGODB, ORACLE):
        fields = stand.connections()[engine.conn_id]
        conn = SimpleNamespace(conn_id=engine.conn_id, extra_dejson={}, **fields)
        assert connection_url(conn) == engine.url(stand.secrets), engine.name
    assert ORACLE.url(stand.secrets).endswith("@gate-oracle:1521/FREEPDB1"), "the path is the PDB's service name"


def test_the_stand_runs_mongodb_as_a_replica_set_with_users_and_oracle_free(tmp_path: Path) -> None:
    stand = laid_out(tmp_path, sources=(MONGODB, ORACLE))
    services = yaml.load((stand.dir / "docker-compose.gate.yaml").read_text(), Loader=Loader)["services"]
    mongo = " ".join(services["gate-mongodb"]["entrypoint"])
    assert "--replSet rs0" in mongo and "--keyFile" in mongo, "a change stream needs a replica set; users need a key file"
    assert compose_view(stand)["gate-mongodb"]["environment"]["MONGO_INITDB_ROOT_PASSWORD"] == stand.secrets.mongodb_root
    assert 'host: "gate-mongodb:27017"' in MONGODB.setup, "the member a client inside the stack can reach"
    assert services["gate-oracle"]["image"].startswith("gvenzl/oracle-free:23")
    assert "ALTER DATABASE ARCHIVELOG" in ORACLE.setup and "ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS" in ORACLE.seed
    # A state per config the leg runs (and init's state for the kind it only asks about comes from the same names).
    conns = stand.connections()
    assert "rivet_gate_state_mongodb_cdc" in conns and "rivet_gate_state_mongodb_batch" not in conns
    assert "rivet_gate_state_oracle_batch" in conns and "rivet_gate_state_oracle_cdc" not in conns
    assert len({stand.secrets.mongodb, stand.secrets.mongodb_root, stand.secrets.oracle}) == 3


def test_a_limit_kind_runs_init_without_failing_the_leg_and_builds_no_dag(tmp_path: Path, monkeypatch) -> None:
    stand = laid_out(tmp_path, sources=(MONGODB,))
    leg = sources.SourceLeg(
        SimpleNamespace(tag="10201200abcd", project="p", location="US", dataset=lambda s: "rivet_tmp_afgate_10201200abcd_332"),
        "3.3.2",
        MONGODB,
    )

    def compose(*args, extra_env=None, check=True, **kw):
        if "airflow-worker" in args:
            out = Path(str(stand.rivet_dir) + args[-1].removeprefix("/opt/rivet"))
            if "cdc" in args:
                out.write_text((FIXTURES / "init_mongodb_cdc_0.32.1.yaml").read_text())
                return SimpleNamespace(returncode=0, stdout="Config written", stderr="")
            assert check is False, "a kind with a known limit must not fail the leg on init's refusal"
            return SimpleNamespace(returncode=1, stdout="", stderr=MONGO_BATCH_REFUSAL)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(stand, "compose", compose)
    monkeypatch.setattr(stand, "psql", lambda *a: "")
    monkeypatch.setattr(sources.SourceLeg, "wait_ready", lambda self, stand: None)
    monkeypatch.setattr(sources.SourceLeg, "sql", lambda self, stand, text: "")
    monkeypatch.setattr(sources, "sh", lambda *a, **k: None)
    monkeypatch.setattr(sources.warehouse, "_bq", lambda *a, **k: None)
    leg.prepare(stand)
    assert leg.limits == {"batch": []}
    assert leg.dags == ["gate_src_mongodb_cdc_gate_customers_cdc", "gate_src_mongodb_cdc_gate_orders_cdc"]
    assert leg.tasks["gate_src_mongodb_cdc_gate_orders_cdc"] == ["run", "load.gate_orders", "compact.gate_orders"]
    assert "batch" not in json.loads((stand.rivet_dir / "configs" / "mongodb_shape.json").read_text())
