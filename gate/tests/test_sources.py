"""Source legs: what the gate reads from `rivet init`'s configs, the names it gives the cloud, the stand it lays out.

The fixtures are init's output on the gate's MySQL and SQL Server scenarios (rivet 0.31.0), the bucket and dataset
renamed.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from test_conns import CLOUD
from test_stand_hygiene import Loader, compose_view, laid_out

from gate import __main__ as gate_main
from gate import sources
from gate.sources import BUCKET_SHAPE, MSSQL, MYSQL, SourceLeg, init_argv, parse_json_rows, read_config, rivet_version
from gate.warehouse import DATASET_SHAPE

FIXTURES = Path(__file__).parent / "fixtures"


TABLES = list(MYSQL.tables)


def test_the_batch_config_init_writes_names_one_export_per_table() -> None:
    shape = read_config((FIXTURES / "init_mysql_batch_0.31.0.yaml").read_text(), TABLES)
    assert shape == {"exports": ["gate_customers", "gate_orders"], "streams": [], "extra": []}


def test_the_cdc_config_init_writes_is_one_stream_over_the_tables_beside_its_recipes() -> None:
    text = (FIXTURES / "init_mysql_cdc_0.31.0.yaml").read_text()
    shape = read_config(text, TABLES)
    stream = {"name": "cdc", "tables": ["gate_customers", "gate_orders"], "dag": "_cdc"}
    assert shape == {"exports": ["gate_customers", "gate_orders"], "streams": [stream], "extra": []}
    # The fixed replica id rivet 0.31 writes (0.32.1 names it from -o): one CDC config per MySQL server is safe.
    [stream] = [e for e in yaml.safe_load(text)["exports"] if e["name"] == "cdc"]
    assert stream["cdc"]["server_id"] == 4271


def test_sql_server_captures_per_table_so_its_cdc_config_is_one_stream_and_dag_per_table() -> None:
    shape = read_config((FIXTURES / "init_mssql_cdc_0.31.0.yaml").read_text(), list(MSSQL.tables))
    assert shape == {
        "exports": [],
        "streams": [
            {"name": "gate_customers", "tables": ["dbo.gate_customers"], "dag": "_cdc_gate_customers"},
            {"name": "gate_orders", "tables": ["dbo.gate_orders"], "dag": "_cdc_gate_orders"},
        ],
        "extra": [],
    }
    # Their CDC exports carry the batch exports' names: each config needs a state of its own (Stand.connections).
    batch = read_config((FIXTURES / "init_mssql_batch_0.31.0.yaml").read_text(), list(MSSQL.tables))
    assert batch == {"exports": ["gate_customers", "gate_orders"], "streams": [], "extra": []}
    # rivet folds a schema-qualified table into one BigQuery name (src/load/plan.rs `warehouse_table_name`).
    assert sources.warehouse_table("dbo.gate_orders") == "dbo_gate_orders" and sources.warehouse_table("t") == "t"


def test_init_is_given_only_the_scenarios_tables() -> None:
    argv = init_argv("batch", MSSQL, "b", "p", "d", "o")
    at = argv.index("--include")
    assert argv[at + 1 : argv.index("-o")] == ["gate_customers", "gate_orders"], "dbo.systranschemas is CDC's own table"


def test_init_reads_the_url_from_the_variable_the_dag_fills_from_the_connection() -> None:
    argv = init_argv("cdc", MYSQL, "bkt", "proj", "ds", "/opt/rivet/configs/mysql_cdc/rivet.yaml")
    assert argv[argv.index("--source-env") + 1] == "RIVET_GATE_MYSQL_URL" == MYSQL.url_env
    assert argv[argv.index("--mode") + 1] == "cdc"
    assert init_argv("batch", MYSQL, "b", "p", "d", "o")[init_argv("batch", MYSQL, "b", "p", "d", "o").index("--mode") + 1] == (
        "incremental"
    )
    assert not [a for a in argv if "://" in a], "no URL on init's command line"


def test_the_legs_bucket_and_datasets_have_the_shapes_the_sweep_recognises() -> None:
    leg = SourceLeg(CLOUD, "2.10.5", MYSQL)
    for bucket in leg.buckets.values():
        assert BUCKET_SHAPE.fullmatch(bucket), bucket
        assert len(bucket) <= 63
    assert len(set(leg.buckets.values())) == 2, "SQL Server's CDC prefix nests in the batch one: a bucket per config"
    for ds in leg.datasets.values():
        assert DATASET_SHAPE.fullmatch(ds), ds
    assert len(set(leg.datasets.values())) == 2, "batch and CDC load tables of the same names: two datasets"
    assert not BUCKET_SHAPE.fullmatch("rivet_data_test") and not BUCKET_SHAPE.fullmatch("rivet-afgate-x-1-mysql-cdc")


def test_the_stand_runs_the_engine_with_its_own_password_and_hands_rivet_a_connection(tmp_path: Path) -> None:
    stand = laid_out(tmp_path, sources=(MYSQL,))
    view = compose_view(stand)
    env = view["gate-mysql"]["environment"]
    assert env["MYSQL_PASSWORD"] == stand.secrets.mysql and env["MYSQL_ROOT_PASSWORD"] == stand.secrets.mysql_root
    assert view["airflow-worker"]["environment"]["GATE_SOURCES"] == "mysql"
    conn = stand.connections()["rivet_gate_mysql"]
    assert (conn["conn_type"], conn["host"], conn["port"], conn["password"]) == ("mysql", "gate-mysql", 3306, stand.secrets.mysql)
    command = " ".join(
        yaml.load((stand.dir / "docker-compose.gate.yaml").read_text(), Loader=Loader)["services"]["gate-mysql"]["command"]
    )
    assert "--binlog-row-metadata=FULL" in command and "--gtid-mode=ON" in command


def test_no_source_leg_leaves_the_stand_as_it_was(tmp_path: Path) -> None:
    stand = laid_out(tmp_path)
    assert "gate-mysql" not in compose_view(stand) and "rivet_gate_mysql" not in stand.connections()


def test_sources_need_bigquery(capsys) -> None:
    with pytest.raises(SystemExit):
        gate_main.main(["--rivet-bin", "x", "--sources", "mysql"])
    assert "--warehouses with bigquery" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        gate_main.main(["--rivet-bin", "x", "--sources", "db2", "--warehouses", "bigquery"])
    assert "unknown source" in capsys.readouterr().err


def test_every_engine_is_known_by_name() -> None:
    assert sources.engines(["mysql", "mssql", "mongodb", "oracle"]) == (MYSQL, MSSQL, sources.MONGODB, sources.ORACLE)


def test_init_and_the_dags_of_each_config_share_a_postgresql_state_of_its_own(tmp_path: Path) -> None:
    stand = laid_out(tmp_path, sources=(MYSQL, MSSQL))
    dbs = set()
    for engine in ("mysql", "mssql"):
        for kind in ("batch", "cdc"):
            conn = stand.connections()[f"rivet_gate_state_{engine}_{kind}"]
            url = stand.state_url(sources.state_db(engine, kind))
            assert url.startswith("postgresql://") and "sslmode=require" in url
            assert f"rivet:{conn['password']}@{conn['host']}:{conn['port']}/{conn['schema']}?" in url
            dbs.add(conn["schema"])
    assert len(dbs) == 4, "a config's export names may repeat another's (SQL Server: CDC exports named after tables)"
    dag = (Path(sources.__file__).parent / "stand" / "dags" / "rivet_gate.py").read_text()
    block = dag[dag.index("def leg_kwargs") :]
    assert '"RIVET_STATE_URL": f"rivet_gate_state_{engine}_{kind}"' in block and '"RIVET_STATE_URL": ""' not in block
    assert 'leg_kwargs(engine, "batch")' in block and 'leg_kwargs(engine, "cdc")' in block


def test_init_leaving_a_state_beside_the_config_fails_the_leg(tmp_path: Path, monkeypatch) -> None:
    stand = laid_out(tmp_path, sources=(MYSQL,))
    leg = SourceLeg(CLOUD, "2.10.5", MYSQL)
    calls = []

    def compose(*args, extra_env=None, **kw):
        calls.append((args, extra_env))
        if "airflow-worker" in args:
            out = Path(str(stand.rivet_dir) + args[-1].removeprefix("/opt/rivet")).parent
            (out / "rivet.yaml").write_text((FIXTURES / "init_mysql_batch_0.31.0.yaml").read_text())
            (out / ".rivet_state.db").write_text("")

    monkeypatch.setattr(stand, "compose", compose)
    monkeypatch.setattr(stand, "psql", lambda *a: calls.append((a, None)))
    monkeypatch.setattr(SourceLeg, "wait_ready", lambda self, stand: None)
    monkeypatch.setattr(SourceLeg, "sql", lambda self, stand, text: "")
    monkeypatch.setattr(sources, "sh", lambda *a, **k: None)
    monkeypatch.setattr(sources.warehouse, "_bq", lambda *a, **k: None)
    with pytest.raises(RuntimeError, match=r"init left \['.rivet_state.db'\]"):
        leg.prepare(stand)
    [(args, env)] = [c for c in calls if "airflow-worker" in c[0]]
    assert args[args.index("RIVET_STATE_URL") - 1] == "-e"
    assert env["RIVET_STATE_URL"] == stand.state_url("rivet_state_mysql_batch")
    assert ("gate-state", "rivet_state", "CREATE DATABASE rivet_state_mysql_batch;") in [c[0] for c in calls]
    assert not [a for a in args if "://" in a], "no URL on the exec command line"


def test_sql_server_runs_with_its_agent_and_a_sa_password_its_policy_accepts(tmp_path: Path) -> None:
    stand = laid_out(tmp_path, sources=(MSSQL,))
    view = compose_view(stand)
    env = view["gate-mssql"]["environment"]
    assert env["MSSQL_SA_PASSWORD"] == stand.secrets.mssql_root + "Aa" == MSSQL.root_password(stand.secrets)
    assert env["MSSQL_AGENT_ENABLED"] == "true", "CDC capture is an Agent job"
    service = yaml.load((stand.dir / "docker-compose.gate.yaml").read_text(), Loader=Loader)["services"]["gate-mssql"]
    assert service["platform"] == "linux/amd64"
    conn = stand.connections()["rivet_gate_mssql"]
    assert (conn["conn_type"], conn["host"], conn["port"]) == ("mssql", "gate-mssql", 1433)
    assert MSSQL.url(stand.secrets).startswith("sqlserver://rivet:"), "rivet reads SQL Server under sqlserver://"
    argv = init_argv("cdc", MSSQL, "b", "p", "d", "o")
    assert argv[argv.index("--tls") + 1] == "require", "SQL Server encrypts every login: init may not write disable"


def test_the_sql_server_source_is_read_from_its_json_with_microseconds_cut_not_rounded() -> None:
    text = (
        "JSON_F52E2B61-18A1-11d1-B105-00805F49916B\n"
        "-----------------------------------------\n"
        '[{"id":1,"amount":12.35,"ext_id":"5050BEBA-37A8-48AB-AB22-49DE93372036","payload":"AP8Q",'
        '"placed_at":"2026-01-01T10:00:01.1234567+03:00","upd\n'
        'ated_at":"2026-10-18T06:22:21.9999999","payload2":null,"name":"клиент\n'
        ' 7"}]\n'
    )
    cols = ["id", "amount", "ext_id", "payload", "placed_at", "updated_at", "payload2", "name"]
    [row] = parse_json_rows(text, cols, MSSQL.json_kinds | {"payload2": "bytes"})
    assert row["name"] == "клиент 7", "a space at sqlcmd's cut is the value's"
    assert row["amount"] == sources.Decimal("12.35") and row["payload"] == b"\x00\xff\x10" and row["payload2"] is None
    assert str(row["ext_id"]) == "5050beba-37a8-48ab-ab22-49de93372036"
    assert row["updated_at"].microsecond == 999999, "rivet truncates datetime2(7); rounding would carry into the next second"
    assert row["placed_at"].isoformat() == "2026-01-01T10:00:01.123456+03:00"
    assert parse_json_rows("JSON_F52E\n----\n", cols, {}) == []
    with pytest.raises(KeyError, match="amount"):
        parse_json_rows('[{"id":1}]', ["id", "amount"], {})


def test_the_renamed_capture_instance_is_given_only_to_a_rivet_whose_init_reads_the_catalog(monkeypatch) -> None:
    leg = SourceLeg(CLOUD, "3.3.2", MSSQL)

    class Shown:
        def __init__(self, text: str) -> None:
            self.stdout = text

    for shown, want in (("rivet 0.31.0 (unknown)\n", "dbo_gate_orders"), ("rivet 0.32.0 (4119c8c)\n", "gate_orders_v2")):
        stand = type("S", (), {"compose": staticmethod(lambda *a, text=shown, **k: Shown(text))})()
        assert leg.capture_instance(stand) == want
    assert "@capture_instance = N'{orders_instance}'" in MSSQL.seed
    assert SourceLeg(CLOUD, "3.3.2", MYSQL).capture_instance(None) == ""
    assert rivet_version("rivet 1.2.10 (x)") == (1, 2, 10)
    with pytest.raises(ValueError):
        rivet_version("command not found")


def test_the_engines_changes_wait_for_the_capture_job_before_a_cdc_run_reads() -> None:
    tail = MSSQL.changes[MSSQL.changes.index("DELETE") :]
    assert "cdc.lsn_time_mapping" in tail and "THROW" in tail, "a CDC run right after the changes must see the DELETE"


def test_an_export_init_writes_for_a_table_the_scenario_does_not_hold_fails_the_leg(tmp_path: Path, monkeypatch) -> None:
    stand = laid_out(tmp_path, sources=(MYSQL,))
    leg = SourceLeg(CLOUD, "2.10.5", MYSQL)
    doc = yaml.safe_load((FIXTURES / "init_mysql_batch_0.31.0.yaml").read_text())
    doc["exports"].append({**doc["exports"][0], "name": "audit_log"})

    def compose(*args, extra_env=None, **kw):
        if "airflow-worker" in args:
            out = Path(str(stand.rivet_dir) + args[-1].removeprefix("/opt/rivet")).parent
            (out / "rivet.yaml").write_text(yaml.safe_dump(doc))

    monkeypatch.setattr(stand, "compose", compose)
    monkeypatch.setattr(stand, "psql", lambda *a: "")
    monkeypatch.setattr(SourceLeg, "wait_ready", lambda self, stand: None)
    monkeypatch.setattr(SourceLeg, "sql", lambda self, stand, text: "")
    monkeypatch.setattr(sources, "sh", lambda *a, **k: None)
    monkeypatch.setattr(sources.warehouse, "_bq", lambda *a, **k: None)
    with pytest.raises(ValueError, match=r"others \['audit_log'"):
        leg.prepare(stand)
