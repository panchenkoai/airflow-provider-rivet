"""Source legs: what the gate reads from `rivet init`'s configs, the names it gives the cloud, the stand it lays out.

The fixtures are init's output on the gate's MySQL scenario (rivet 0.31.0), the bucket and dataset renamed.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from test_conns import CLOUD
from test_stand_hygiene import Loader, compose_view, laid_out

from gate import __main__ as gate_main
from gate import sources
from gate.sources import BUCKET_SHAPE, MYSQL, SourceLeg, init_argv, read_config
from gate.warehouse import DATASET_SHAPE

FIXTURES = Path(__file__).parent / "fixtures"


def test_the_batch_config_init_writes_names_one_export_per_table() -> None:
    shape = read_config((FIXTURES / "init_mysql_batch_0.31.0.yaml").read_text())
    assert shape == {"exports": ["gate_customers", "gate_orders"], "stream": None, "tables": []}


def test_the_cdc_config_init_writes_is_one_stream_over_the_tables_beside_its_recipes() -> None:
    text = (FIXTURES / "init_mysql_cdc_0.31.0.yaml").read_text()
    shape = read_config(text)
    assert shape == {"exports": ["gate_customers", "gate_orders"], "stream": "cdc", "tables": ["gate_customers", "gate_orders"]}
    # The fixed replica id rivet 0.31 writes (0.32.1 names it from -o): one CDC config per MySQL server is safe.
    [stream] = [e for e in yaml.safe_load(text)["exports"] if e["name"] == "cdc"]
    assert stream["cdc"]["server_id"] == 4271


def test_two_streams_in_one_config_are_refused() -> None:
    doc = {"exports": [{"name": "a", "mode": "cdc"}, {"name": "b", "mode": "cdc"}]}
    with pytest.raises(ValueError, match="2 CDC exports"):
        read_config(yaml.safe_dump(doc))


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
    assert BUCKET_SHAPE.fullmatch(leg.bucket), leg.bucket
    assert len(leg.bucket) <= 63
    for ds in leg.datasets.values():
        assert DATASET_SHAPE.fullmatch(ds), ds
    assert len(set(leg.datasets.values())) == 2, "batch and CDC load tables of the same names: two datasets"
    assert not BUCKET_SHAPE.fullmatch("rivet_data_test") and not BUCKET_SHAPE.fullmatch("rivet-afgate-x-1-mysql")


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
        gate_main.main(["--rivet-bin", "x", "--sources", "oracle", "--warehouses", "bigquery"])
    assert "unknown source" in capsys.readouterr().err


def test_every_engine_is_known_by_name() -> None:
    assert sources.engines(["mysql"]) == (MYSQL,)


def test_init_and_the_dags_of_a_source_leg_share_the_stacks_postgresql_state(tmp_path: Path) -> None:
    stand = laid_out(tmp_path, sources=(MYSQL,))
    conn = stand.connections()["rivet_gate_state"]
    url = stand.state_url()
    assert url.startswith("postgresql://") and "sslmode=require" in url
    assert f"rivet:{conn['password']}@{conn['host']}:{conn['port']}/{conn['schema']}?" in url
    dag = (Path(sources.__file__).parent / "stand" / "dags" / "rivet_gate.py").read_text()
    block = dag[dag.index("for engine in") :]
    assert '"RIVET_STATE_URL": "rivet_gate_state"' in block and '"RIVET_STATE_URL": ""' not in block


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
    monkeypatch.setattr(SourceLeg, "wait_ready", lambda self, stand: None)
    monkeypatch.setattr(SourceLeg, "sql", lambda self, stand, text: "")
    monkeypatch.setattr(sources, "sh", lambda *a, **k: None)
    monkeypatch.setattr(sources.warehouse, "_bq", lambda *a, **k: None)
    with pytest.raises(RuntimeError, match=r"init left \['.rivet_state.db'\]"):
        leg.prepare(stand)
    [(args, env)] = [c for c in calls if "airflow-worker" in c[0]]
    assert args[args.index("RIVET_STATE_URL") - 1] == "-e" and env["RIVET_STATE_URL"] == stand.state_url()
    assert not [a for a in args if "://" in a], "no URL on the exec command line"
