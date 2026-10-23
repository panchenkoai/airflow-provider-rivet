"""`--source-dags`: the sources named in the odbc.ini (or a Connection) the stand mounts, the calls the DAG file makes,
and the oracles of the cells the mode adds (init, shape, the ODBC refusal, a refused stream, Oracle CDC parts).

The configs are init's output on the gate's scenarios (fixtures/, rivet 0.31.0 and 0.32.1, bucket and dataset renamed).
"""

from __future__ import annotations

import glob
import json
import runpy
import stat
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from urllib.parse import unquote

import duckdb
import pytest
import yaml
from test_conns import CLOUD
from test_stand_hygiene import compose_view, laid_out

from airflow_provider_rivet.connections import UNSUPPORTED, connection_url, odbc_connection
from airflow_provider_rivet.preflight import PreflightRefusal
from gate import __main__ as gate_main
from gate import oracle, source_dags
from gate.source_dags import (
    ORACLE_POS,
    REFUSAL_CODE,
    REFUSAL_TEXT,
    Layout,
    SourceDagsLeg,
    expected_tasks,
    init_problems,
    refusal_problems,
    shape_problems,
    source_relation,
    stream_refused_problems,
)
from gate.sources import BUCKET_SHAPE, MONGODB, MSSQL, MYSQL, ORACLE
from gate.warehouse import DATASET_SHAPE

FIXTURES = Path(__file__).parent / "fixtures"
ENGINES = (MYSQL, MSSQL, ORACLE, MONGODB)
SECRETS = SimpleNamespace(mysql="a1b2", mssql="c3d4", oracle="e5f6", mongodb="0a0b")


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def ini(tmp_path: Path) -> str:
    path = tmp_path / "odbc.ini"
    path.write_text(Layout(ENGINES).odbc_ini(SECRETS))
    return str(path)


def test_mysql_sql_server_and_oracle_are_odbc_data_sources_the_provider_turns_into_rivet_urls(tmp_path: Path) -> None:
    path = ini(tmp_path)
    text = Path(path).read_text()
    for driver in ("ODBC Driver 18 for SQL Server", "Oracle 21 ODBC driver", "MySQL ODBC 8.0 Unicode Driver"):
        assert f"Driver = {driver}\n" in text
    assert "ServerName = gate-oracle:1521/FREEPDB1\n" in text
    urls = {name: connection_url(odbc_connection(path, name)) for name in ("mysql_cdc", "mssql_batch", "oracle_batch")}
    assert urls == {
        "mysql_cdc": "mysql://rivet:a1b2@gate-mysql:3306/rivet",
        "mssql_batch": "sqlserver://rivet:c3d4@gate-mssql:1433/rivet",
        "oracle_batch": "oracle://rivet:e5f6@gate-oracle:1521/FREEPDB1",
    }
    # LogMiner mines from CDB$ROOT: Oracle CDC signs in as the common user the engine's setup makes.
    cdc = connection_url(odbc_connection(path, "oracle_cdc"))
    assert unquote(cdc) == "oracle://c##rivetcdc:e5f6@gate-oracle:1521/FREEPDB1"
    assert "CREATE USER c##rivetcdc" in ORACLE.setup and "GRANT SELECT ON gate_orders TO c##rivetcdc" in ORACLE.seed


def test_mongodb_is_a_connection_and_its_odbc_data_source_is_refused_with_the_text_the_cell_asserts(tmp_path: Path) -> None:
    path = ini(tmp_path)
    conns = Layout(ENGINES).connections(SECRETS)
    assert conns == {"mongodb_cdc": MONGODB.connection(SECRETS)}, "every other source is in odbc.ini, no Connection"
    sections = {line.strip("[]\n") for line in Path(path).read_text().splitlines() if line.startswith("[")}
    assert not sections & set(conns)
    assert odbc_connection(path, "mongodb_cdc") is None
    with pytest.raises(PreflightRefusal) as refused:
        odbc_connection(path, source_dags.REFUSED_SOURCE)
    assert refused.value.code == UNSUPPORTED == REFUSAL_CODE and REFUSAL_TEXT in str(refused.value)


def test_the_stand_mounts_its_private_odbc_ini_and_the_dag_file_only_for_source_dags(tmp_path: Path) -> None:
    stand = laid_out(tmp_path / "a", sources=(MSSQL, MONGODB), source_dags=Layout((MSSQL, MONGODB)))
    view = compose_view(stand)
    assert f"{stand.dir / 'odbc.ini'}:/etc/odbc.ini:ro" in view["airflow-worker"]["volumes"]
    assert stat.S_IMODE((stand.dir / "odbc.ini").stat().st_mode) == 0o600
    assert (stand.dir / "dags" / "rivet_source_dags.py").exists()
    assert "[mssql_cdc]" in (stand.dir / "odbc.ini").read_text() and "mongodb_cdc" in stand.connections()
    plain = laid_out(tmp_path / "b", sources=(MSSQL,))
    assert not (plain.dir / "odbc.ini").exists() and not (plain.dir / "dags" / "rivet_source_dags.py").exists()
    assert not [v for v in compose_view(plain)["airflow-worker"]["volumes"] if "odbc" in v]


def test_each_engine_and_mode_is_one_call_of_names_whose_init_args_name_the_bucket_and_dataset_by_name() -> None:
    stand = SimpleNamespace(gcp_key_file=None)
    calls = {e.name: SourceDagsLeg(CLOUD, "3.3.2", e).spec(stand)["calls"] for e in ENGINES}
    assert [c["sources"] for c in calls["mongodb"]] == [["mongodb_cdc", "mongodb_odbc"]]
    assert [(c["sources"], c["mode"]) for c in calls["mssql"]] == [(["mssql_batch"], "batch"), (["mssql_cdc"], "cdc")]
    for engine, own in calls.items():
        for call in own:
            args = call["init_args"]
            assert "--mode" not in args and "-o" not in args, "the operator sets them"
            bucket, dataset = args[args.index("--gcs-bucket") + 1], args[args.index("--bigquery-dataset") + 1]
            assert bucket.endswith("-{name}") and dataset.endswith("_{name}")
            for name in call["sources"]:
                assert BUCKET_SHAPE.fullmatch(bucket.replace("{name}", name)) or name == "mongodb_odbc"
                assert len(bucket.replace("{name}", name)) <= 63
                assert DATASET_SHAPE.fullmatch(dataset.replace("{name}", name)) or name == "mongodb_odbc"
            assert args[args.index("--tls") + 1] == ("require" if engine == "mssql" else "disable")
    oracle_cdc = calls["oracle"][1]["init_args"]
    assert oracle_cdc[oracle_cdc.index("--schema") + 1] == "RIVET" and "--schema" not in calls["oracle"][0]["init_args"]
    spec = SourceDagsLeg(CLOUD, "3.3.2", MYSQL).spec(SimpleNamespace(gcp_key_file=Path("/k.json")))
    assert spec["operator_kwargs"] == {"cloud_credentials": ["gcp"]} and spec["state_connection"] == "rivet_gate_state"


def test_the_dag_file_passes_each_call_to_build_source_dags_as_written(tmp_path: Path, monkeypatch) -> None:
    spec = SourceDagsLeg(CLOUD, "3.3.2", ORACLE).spec(SimpleNamespace(gcp_key_file=None))
    (tmp_path / "source_dags_oracle.json").write_text(json.dumps(spec))
    seen = []

    def build_source_dags(sources, **kw):
        seen.append((sources, kw))
        return {f"rivet_{s}_{kw['mode']}": object() for s in sources}

    module = ModuleType("airflow_provider_rivet.dags")
    module.build_source_dags = build_source_dags
    monkeypatch.setitem(sys.modules, "airflow_provider_rivet.dags", module)
    real = glob.glob
    monkeypatch.setattr(glob, "glob", lambda p: real(str(tmp_path / Path(p).name)))
    dag_file = Path(source_dags.__file__).parent / "stand" / "dags" / "rivet_source_dags.py"
    made = runpy.run_path(str(dag_file))
    assert "rivet_oracle_batch_batch" in made and "rivet_oracle_cdc_cdc" in made
    for (sources, kw), call in zip(seen, spec["calls"], strict=True):
        assert sources == call["sources"] and kw["mode"] == call["mode"] and kw["init_args"] == call["init_args"]
        assert (kw["config_dir"], kw["state_root"], kw["state_connection"]) == (
            source_dags.CONFIG_DIR,
            source_dags.STATE_ROOT,
            "rivet_gate_state",
        )
        assert kw["schedule"] == "@once" and "source_connection" not in kw and "odbc_ini" not in kw


def test_the_shape_is_one_group_per_stream_where_init_writes_a_stream_per_table() -> None:
    mssql = fixture("init_mssql_cdc_0.31.0.yaml")
    want = expected_tasks(mssql, "cdc")
    assert want == sorted(
        ["watcher"]
        + [f"{s}.{g}{t}" for s in ("gate_customers", "gate_orders") for g, t in (("run", ""),)]
        + [f"{s}.{g}.dbo.{s}" for s in ("gate_customers", "gate_orders") for g in ("load", "compact")]
    )
    assert shape_problems(want, mssql, "cdc", grouped=True) == []
    assert shape_problems(["init"], mssql, "cdc", grouped=True), "the DAG was never rebuilt"
    assert shape_problems([t for t in want if "compact" not in t], mssql, "cdc", grouped=True), "no compaction"
    assert shape_problems([*want, "run"], mssql, "cdc", grouped=True), "an extra task"
    one = yaml.safe_dump({**yaml.safe_load(mssql), "exports": yaml.safe_load(mssql)["exports"][:1]})
    assert shape_problems(expected_tasks(one, "cdc"), one, "cdc", grouped=True), "one stream where the engine groups"


def test_mysql_keeps_one_stream_at_the_top_and_oracle_cdc_has_no_load_or_compaction() -> None:
    mysql = fixture("init_mysql_cdc_0.31.0.yaml")
    assert expected_tasks(mysql, "cdc") == sorted(
        ["watcher", "run", "load.gate_customers", "load.gate_orders", "compact.gate_customers", "compact.gate_orders"]
    ), "the batch recipes beside the stream are no tasks of a CDC DAG"
    assert shape_problems(expected_tasks(mysql, "cdc"), mysql, "cdc", grouped=False) == []
    assert shape_problems(expected_tasks(mysql, "cdc"), mysql, "cdc", grouped=True)
    doc = yaml.safe_load(mysql)
    changelog = yaml.safe_dump({**doc, "load": {k: w for k, w in doc["load"].items() if k != "layout"}})
    assert "compact.gate_orders" not in expected_tasks(changelog, "cdc"), "a change log has no buffer to merge"
    oracle_cdc = fixture("init_oracle_cdc_0.32.1.yaml")
    assert expected_tasks(oracle_cdc, "cdc") == ["GATE_CUSTOMERS_cdc.run", "GATE_ORDERS_cdc.run", "watcher"]
    batch = fixture("init_oracle_batch_0.32.1.yaml")
    assert expected_tasks(batch, "batch") == sorted(
        ["watcher", "apply.GATE_CUSTOMERS", "apply.GATE_ORDERS", "load.GATE_CUSTOMERS", "load.GATE_ORDERS"]
    ), "build_source_dags compacts no batch export"


GOOD = {
    "run": "success",
    "config": "exports: []",
    "db_before": False,
    "db_after": True,
    "log": "database `mssql_cdc`: no such Connection, ODBC data source [mssql_cdc] in /etc/odbc.ini\n"
    "state database `rivet_state_mssql_cdc` created on gate-state",
    "checkpoints": {"mssql_cdc/./cdc/gate_orders.ckpt": True},
}


@pytest.mark.parametrize(
    ("change", "odbc"),
    [
        ({"run": "failed"}, True),
        ({"config": None}, True),
        ({"db_before": True}, True),
        ({"db_after": False}, True),
        ({"log": GOOD["log"].splitlines()[0]}, True),
        ({"log": GOOD["log"].splitlines()[1]}, True),
        ({}, False),
        ({"checkpoints": {"mssql_cdc/./cdc/gate_orders.ckpt": False}}, True),
    ],
)
def test_the_init_cell_holds_init_to_the_config_the_state_database_the_odbc_source_and_the_checkpoint(change, odbc) -> None:
    assert init_problems(GOOD, "mssql_cdc", odbc=True) == []
    assert init_problems({**GOOD, **change}, "mssql_cdc", odbc=odbc)


def test_the_odbc_refusal_cell_wants_one_failed_try_the_code_and_the_reason() -> None:
    payload = {"decision": "fail", "preflight_refusal": REFUSAL_CODE}
    log = f"[{REFUSAL_CODE}] ODBC data source [mongodb_odbc] in /etc/odbc.ini uses driver x; {REFUSAL_TEXT}: create..."
    once = [{"state": "failed", "try_number": 1}]
    assert refusal_problems("failed", once, payload, log) == []
    assert refusal_problems("success", once, payload, log)
    assert refusal_problems("failed", [*once, {"state": "failed"}], payload, log), "retried"
    assert refusal_problems("failed", once, {"preflight_refusal": "RIVET_AIRFLOW_CONNECTION_MISSING"}, log)
    assert refusal_problems("failed", once, None, log)
    assert refusal_problems("failed", once, payload, "connection `mongodb_odbc` is not defined")


def test_a_refused_stream_is_its_code_on_the_first_try() -> None:
    code = "RIVET_SOURCE_CDC_TABLE_UNSUPPORTED"
    failed = {"state": "failed", "try_number": 1}
    assert stream_refused_problems(failed, {"error": {"code": code}}, code) == []
    assert stream_refused_problems({**failed, "state": "success"}, {"error": None}, code)
    assert stream_refused_problems({**failed, "try_number": 2}, {"error": {"code": code}}, code)
    assert stream_refused_problems(failed, {"error": {"code": "RIVET_SOURCE_CONNECT"}}, code)
    assert stream_refused_problems(failed, None, code)


def leg_with(engine, texts: dict[str, str]) -> SourceDagsLeg:
    leg = SourceDagsLeg(CLOUD, "3.3.2", engine)
    leg.texts = texts
    leg.shape = {k: source_dags.read_config(t, list(engine.tables)) for k, t in texts.items()}
    v = SimpleNamespace(rows=[], grade=lambda cell, problems, ok="": v.rows.append((cell, problems)))
    built = {leg.source_dags[k]: expected_tasks(t, k) for k, t in texts.items()}
    leg.plan(v, built)
    leg.graded = v.rows
    return leg


def test_oracle_cdc_runs_its_capturable_stream_graded_on_files_and_its_refused_stream_apart() -> None:
    leg = leg_with(ORACLE, {"batch": fixture("init_oracle_batch_0.32.1.yaml"), "cdc": fixture("init_oracle_cdc_0.32.1.yaml")})
    assert leg.graded == [("oracle/source-dags/tables", [])]
    assert (leg.kinds, leg.file_kinds) == (("batch",), ("cdc",))
    assert leg.dags == ["rivet_oracle_batch_batch", "rivet_oracle_cdc_cdc"]
    assert leg.tasks["rivet_oracle_cdc_cdc"] == ["GATE_CUSTOMERS_cdc.run"]
    assert leg.refused == {"rivet_oracle_cdc_cdc": ["GATE_ORDERS_cdc.run"]}
    assert leg.refused_tables == {"GATE_ORDERS_cdc.run": "GATE_ORDERS"}


def test_sql_server_streams_load_and_may_skip_a_compaction_in_their_groups() -> None:
    leg = leg_with(MSSQL, {"batch": fixture("init_mssql_batch_0.31.0.yaml"), "cdc": fixture("init_mssql_cdc_0.31.0.yaml")})
    assert leg.kinds == ("batch", "cdc") and leg.file_kinds == () and leg.refused == {}
    assert leg.may_skip["rivet_mssql_cdc_cdc"] == ("gate_customers.compact.", "gate_orders.compact.")
    assert leg.targets["cdc"] == {"gate_customers": "dbo_gate_customers", "gate_orders": "dbo_gate_orders"}
    assert "watcher" not in leg.tasks["rivet_mssql_cdc_cdc"] and "gate_orders.run" in leg.tasks["rivet_mssql_cdc_cdc"]


def test_a_full_batch_export_keeps_no_deleted_key_and_moves_every_row() -> None:
    doc = yaml.safe_load(fixture("init_mysql_batch_0.31.0.yaml"))
    doc["exports"][1]["mode"] = "full"
    leg = leg_with(MYSQL, {"batch": yaml.safe_dump(doc)})
    source = {"gate_customers": [{"updated_at": 1}] * 3, "gate_orders": [{"updated_at": 1}] * 7}
    assert leg.kept_by_batch("delta", "gate_orders") == 0
    assert leg.expected_rows("batch", "load.gate_orders", source, {}, False) == 7
    assert leg.kept_by_batch("delta", "gate_customers") == source_dags.SourceLeg.kept_by_batch(leg, "delta", "gate_customers")


def write_part(con: duckdb.DuckDBPyConnection, path: Path, rows: list[tuple], change: bool) -> str:
    cols = (
        "__op VARCHAR, __pos VARCHAR, __seq INTEGER, ID DECIMAL(19,0), NAME VARCHAR"
        if change
        else "ID DECIMAL(19,0), NAME VARCHAR"
    )
    con.execute(f"CREATE OR REPLACE TABLE part ({cols})")
    con.executemany(f"INSERT INTO part VALUES ({', '.join('?' * (5 if change else 2))})", rows)
    con.execute(f"COPY part TO '{path}' (FORMAT parquet)")
    return str(path)


def test_oracle_parts_replay_in_commit_scn_order_numerically(tmp_path: Path) -> None:
    con = duckdb.connect()
    snap = write_part(con, tmp_path / "snap.parquet", [(1, "a"), (2, "b")], False)

    def pos(scn: int) -> str:
        return json.dumps({"low_water": "5", "commit_scn": str(scn)})

    good = write_part(con, tmp_path / "c1.parquet", [("update", pos(9), 0, 1, "x"), ("update", pos(10), 0, 1, "y")], True)
    source = source_relation(con, "GATE_CUSTOMERS", [{"id": 1, "name": "y"}, {"id": 2, "name": "b"}], ["id", "name"])
    assert oracle.cdc_replay(con, [snap], [[good]], source, ["id", "name"], "id", ORACLE_POS) == []
    # Read as text, "9" > "10": the older image would be the one kept.
    text_pos = "json_extract_string(__pos, '$.commit_scn')"
    assert oracle.cdc_replay(con, [snap], [[good]], source, ["id", "name"], "id", text_pos)
    gone = write_part(con, tmp_path / "c2.parquet", [("delete", pos(11), 0, 2, "b")], True)
    assert oracle.cdc_replay(con, [snap], [[good, gone]], source, ["id", "name"], "id", ORACLE_POS), "a delete applied"
    assert oracle.cdc_ops(con, [good], {"update": 2}, "c") == [] and oracle.cdc_ops(con, [good], {"update": 1}, "c")
    assert source_dags.is_change_part(con, "gs://b/x/GATE_CUSTOMERS/part.parquet", Path(good))
    assert not source_dags.is_change_part(con, "gs://b/x/GATE_CUSTOMERS/part.parquet", Path(snap))
    assert not source_dags.is_change_part(con, "gs://b/x/snapshot/part.parquet", Path(good))


def test_a_part_belongs_to_the_one_table_its_path_names() -> None:
    tables = list(ORACLE.tables)
    assert source_dags.part_table("gs://b/oracle_cdc/cdc/GATE_ORDERS/1.parquet", tables) == "GATE_ORDERS"
    assert source_dags.part_table("gs://b/exports/gate_customers_cdc/x.parquet", tables) == "GATE_CUSTOMERS"
    assert source_dags.part_table("gs://b/exports/other/x.parquet", tables) is None


@pytest.mark.parametrize(
    "extra",
    [[], ["--migrate-from", "old"], ["--state", "compose-postgres"]],
)
def test_source_dags_runs_the_source_legs_on_the_gates_own_state(capsys, extra) -> None:
    with pytest.raises(SystemExit):
        gate_main.main(
            ["--rivet-bin", "x", "--warehouses", "bigquery", "--source-dags", *(["--sources", "mysql"] if extra else []), *extra]
        )
    assert "--source-dags runs the source legs" in capsys.readouterr().err
