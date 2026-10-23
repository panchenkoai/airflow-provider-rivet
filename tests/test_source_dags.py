"""build_source_dags: a DAG per named database, which writes its own config with `rivet init` and then runs it."""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from airflow_provider_rivet._compat import AirflowFailException, AirflowNotFoundException
from airflow_provider_rivet.connections import database_ref, odbc_connection, with_database
from airflow_provider_rivet.dags import build_source_dags
from airflow_provider_rivet.operators import RivetInitOperator
from airflow_provider_rivet.preflight import PreflightRefusal

ODBC = """\
[shop]
Driver = MySQL ODBC 8.0 Unicode Driver
SERVER = db1.internal
Port = 3307
UID = reader
PWD = p@ss:word
DATABASE = shop

[pg]
Driver = /usr/lib/psqlodbcw.so
Servername = pg1
Username = u
Database = billing

[mssql]
Driver = ODBC Driver 18 for SQL Server
Server = ms1
"""
LOAD = "  target: bigquery\n  project: p\n  dataset: shop\n  layout: base_buffer\n"
CDC_CONFIG = """\
source:
  type: mysql
  url_env: {env}
load:
  target: bigquery
  project: p
  dataset: shop
  layout: base_buffer
exports:
  - name: shop_orders
    table: orders
    mode: full
  - name: shop_cdc
    mode: cdc
    tables: [orders, customers]
    cdc:
      checkpoint: ./cdc/shop_cdc.ckpt
"""
BATCH_CONFIG = """\
source:
  type: mysql
  url_env: RIVET_SOURCE_URL
load:
  target: bigquery
  project: p
  dataset: shop
exports:
  - name: orders
    table: orders
    mode: incremental
  - name: customers
    table: customers
    mode: full
"""
FAKE_RIVET = """\
import json, os, sys
from pathlib import Path
here = Path(__file__).resolve().parent
argv = sys.argv[1:]
with open(here / "calls.jsonl", "a") as log:
    log.write(json.dumps({"argv": argv, "url": os.environ.get("RIVET_SOURCE_URL"),
                          "state": os.environ.get("RIVET_STATE_URL"), "cwd": os.getcwd()}) + "\\n")
spec = json.loads((here / "spec.json").read_text())
if argv[0] == "init":
    if spec.get("init_exit"):
        print("error: cannot connect", file=sys.stderr)
        sys.exit(spec["init_exit"])
    Path(argv[argv.index("-o") + 1]).write_text(spec["yaml"])
    print("wrote config")
elif argv[0] == "check":
    print("check ok" if not spec.get("check_exit") else "error: binlog_format is STATEMENT", file=sys.stderr)
    sys.exit(spec.get("check_exit", 0))
"""


class Conn(types.SimpleNamespace):
    """An Airflow Connection's fields."""


def connection(conn_id: str, conn_type: str, schema: str | None = None, extra: dict | None = None) -> Conn:
    return Conn(
        conn_id=conn_id,
        conn_type=conn_type,
        host=f"{conn_id}.host",
        port=None,
        login="u",
        password="pw",
        schema=schema,
        extra_dejson=extra or {},
    )


@pytest.fixture
def odbc_ini(tmp_path: Path) -> str:
    path = tmp_path / "odbc.ini"
    path.write_text(ODBC)
    return str(path)


# --- odbc.ini and database references ---


def test_an_odbc_data_source_reads_as_a_connection_whatever_the_key_case(odbc_ini: str) -> None:
    shop = odbc_connection(odbc_ini, "shop")
    assert (shop.conn_type, shop.host, shop.port, shop.login, shop.password, shop.schema) == (
        "mysql",
        "db1.internal",
        3307,
        "reader",
        "p@ss:word",
        "shop",
    )
    pg = odbc_connection(odbc_ini, "pg")
    assert (pg.conn_type, pg.host, pg.port, pg.login, pg.password, pg.schema) == ("postgres", "pg1", None, "u", None, "billing")


def test_a_missing_file_or_data_source_is_none_and_a_foreign_driver_is_refused(odbc_ini: str, tmp_path: Path) -> None:
    assert odbc_connection(odbc_ini, "nope") is None
    assert odbc_connection(str(tmp_path / "absent.ini"), "shop") is None
    assert odbc_connection(odbc_ini, "SHOP") is None  # a data source name is case-sensitive, as in unixODBC
    with pytest.raises(PreflightRefusal, match=r"\[mssql\].*ODBC Driver 18 for SQL Server.*Airflow Connection `mssql`"):
        odbc_connection(odbc_ini, "mssql")


def test_a_port_that_is_no_number_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "odbc.ini"
    path.write_text("[shop]\nDriver = MySQL\nServer = h\nPort = 33o6\n")
    with pytest.raises(PreflightRefusal, match="port `33o6`, which is no number"):
        odbc_connection(str(path), "shop")


def test_every_reference_form_reads_as_one_dict() -> None:
    assert database_ref("pg") == {"conn_id": "pg"}
    assert database_ref(("pg", "postgresql")) == {"conn_id": "pg", "scheme": "postgresql"}
    assert database_ref({"conn_id": "pg", "database": "d"}) == {"conn_id": "pg", "database": "d"}
    for bad in ({"conn_id": "pg", "db": "d"}, {"database": "d"}):
        with pytest.raises(ValueError, match="has `conn_id` and optionally"):
            database_ref(bad)


def test_with_database_replaces_only_the_database() -> None:
    conn = connection("state", "postgres", schema="postgres", extra={"rivet_params": "sslmode=disable"})
    moved = with_database(conn, "rivet_state_shop")
    assert (moved.schema, moved.host, moved.extra_dejson) == (
        "rivet_state_shop",
        "state.host",
        {"rivet_params": "sslmode=disable"},
    )
    assert conn.schema == "postgres" and with_database(conn, None) is conn


# --- the operator's lookup: an Airflow Connection, else the data source ---


def lookup(monkeypatch: pytest.MonkeyPatch, conns: dict[str, Any]) -> None:
    def get(conn_id: str) -> Any:
        if conn_id not in conns:
            raise AirflowNotFoundException(conn_id)
        return conns[conn_id]

    monkeypatch.setattr("airflow_provider_rivet.operators.BaseHook.get_connection", get)


def init_operator(tmp_path: Path, **kwargs: Any) -> RivetInitOperator:
    kwargs.setdefault("env_from_connections", {"RIVET_SOURCE_URL": {"conn_id": "shop", "odbc_ini": str(tmp_path / "odbc.ini")}})
    kwargs.setdefault("state_dir", str(tmp_path / "state" / "shop"))
    return RivetInitOperator(
        task_id="init",
        mode="cdc",
        source_env="RIVET_SOURCE_URL",
        config=str(tmp_path / "cfg" / "shop.yaml"),
        rivet_bin=str(tmp_path / "bin" / "rivet"),
        **kwargs,
    )


def test_a_connection_wins_over_a_data_source_of_the_same_name(
    odbc_ini: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookup(monkeypatch, {"shop": connection("shop", "mysql", schema="from_airflow")})
    op = init_operator(tmp_path)
    assert op._database(database_ref(op.env_from_connections["RIVET_SOURCE_URL"])).schema == "from_airflow"


def test_without_a_connection_the_data_source_is_used_and_logged(
    odbc_ini: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture,
) -> None:
    lookup(monkeypatch, {})
    op = init_operator(tmp_path)
    with caplog.at_level("INFO"):
        conn = op._database(database_ref(op.env_from_connections["RIVET_SOURCE_URL"]))
    assert (conn.host, conn.schema) == ("db1.internal", "shop")
    assert f"ODBC data source [shop] in {odbc_ini}" in caplog.text + capsys.readouterr().out  # Airflow 3 logs to stdout


def test_neither_a_connection_nor_a_data_source_is_refused_naming_both(
    odbc_ini: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lookup(monkeypatch, {})
    op = init_operator(tmp_path, env_from_connections={"RIVET_SOURCE_URL": {"conn_id": "nope", "odbc_ini": odbc_ini}})
    with pytest.raises(PreflightRefusal, match=rf"connection `nope` is not defined.*and {odbc_ini} has no data source \[nope\]"):
        op._database(database_ref(op.env_from_connections["RIVET_SOURCE_URL"]))


# --- RivetInitOperator ---


class Init:
    """A fake `rivet` for init and check, the operator and what the binary saw."""

    def __init__(self, tmp: Path, monkeypatch: pytest.MonkeyPatch, **spec: Any) -> None:
        self.tmp = tmp
        (tmp / "bin").mkdir()
        script = tmp / "bin" / "rivet"
        script.write_text(f"#!{sys.executable}\n{FAKE_RIVET}")
        script.chmod(0o755)
        (tmp / "odbc.ini").write_text(ODBC)
        self.spec(**spec)
        lookup(monkeypatch, {})
        self.config = tmp / "cfg" / "shop.yaml"
        (tmp / "state").mkdir()  # the state root, a mounted volume in a deployment

    def spec(self, **spec: Any) -> None:
        (self.tmp / "bin" / "spec.json").write_text(json.dumps({"yaml": CDC_CONFIG.format(env="RIVET_SOURCE_URL"), **spec}))

    def calls(self) -> list[dict]:
        log = self.tmp / "bin" / "calls.jsonl"
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def run(self, **kwargs: Any) -> Any:
        try:
            return init_operator(self.tmp, **kwargs).execute({})
        except Exception as exc:
            return exc


@pytest.fixture
def init(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Init:
    return Init(tmp_path, monkeypatch)


def test_init_writes_the_config_after_init_and_check_ran_on_it_in_the_state_folder(init: Init) -> None:
    out = init.run(init_args=["--gcs-bucket", "b", "--tls", "disable"])
    assert out["written"] is True and init.config.read_text() == CDC_CONFIG.format(env="RIVET_SOURCE_URL")
    assert out["exports"][1] == {"name": "shop_cdc", "mode": "cdc", "tables": ["orders", "customers"]}
    draft = str(init.tmp / "state" / "shop" / ".init" / "shop.yaml")  # the stem names the stream's server_id and prefix
    first, second = init.calls()
    assert first["argv"] == [
        "init",
        "--source-env",
        "RIVET_SOURCE_URL",
        "--mode",
        "cdc",
        "-o",
        draft,
        "--gcs-bucket",
        "b",
        "--tls",
        "disable",
    ]
    assert second["argv"] == ["check", "--config", draft]
    assert first["url"] == "mysql://reader:p%40ss%3Aword@db1.internal:3307/shop" == second["url"]
    assert not list(init.config.parent.glob(".*.tmp"))


@pytest.mark.parametrize("failing", ["init_exit", "check_exit"])
def test_a_failed_init_or_check_writes_no_config_and_is_not_retried(init: Init, failing: str) -> None:
    init.spec(**{failing: 5})
    out = init.run()
    assert isinstance(out, AirflowFailException) and "exited 5" in str(out) and "no config was written" in str(out)
    assert not init.config.exists()
    init.spec()
    assert init.run()["written"] is True  # the next try starts over from a fresh draft


def test_an_existing_config_is_never_replaced(init: Init) -> None:
    init.config.parent.mkdir()
    init.config.write_text("# the operator's edit\n")
    assert init.run() == {"config": str(init.config), "written": False}
    assert init.config.read_text() == "# the operator's edit\n" and init.calls() == []


def test_a_config_written_meanwhile_is_kept(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    real = RivetInitOperator._rivet

    def racing(self: RivetInitOperator, argv: list[str], env: Any, cwd: Path) -> None:
        real(self, argv, env, cwd)
        if argv[0] == "check":
            init.config.parent.mkdir(exist_ok=True)
            init.config.write_text("# another try's\n")

    monkeypatch.setattr(RivetInitOperator, "_rivet", racing)
    init.run()
    assert init.config.read_text() == "# another try's\n"


def test_a_source_with_neither_connection_nor_data_source_fails_before_rivet(init: Init) -> None:
    out = init.run(env_from_connections={"RIVET_SOURCE_URL": {"conn_id": "nope", "odbc_ini": str(init.tmp / "odbc.ini")}})
    assert isinstance(out, AirflowFailException) and "RIVET_AIRFLOW_CONNECTION_MISSING" in str(out)
    assert init.calls() == []


def test_the_operator_owns_the_source_the_output_and_the_mode(tmp_path: Path) -> None:
    for owned in ("--source-env", "-o", "--mode"):
        with pytest.raises(ValueError, match="init_args may not set"):
            init_operator(tmp_path, init_args=[owned, "x"])
    with pytest.raises(ValueError, match="names no `RIVET_SOURCE_URL`"):
        init_operator(tmp_path, env_from_connections={})


class FakeCursor:
    def __init__(self, server: FakeServer) -> None:
        self.server = server

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str, args: tuple = ()) -> None:
        self.server.sql.append((sql, args))
        if sql.startswith("CREATE DATABASE"):
            self.server.databases.add(sql.split('"')[1])
        self.found = bool(args) and args[0] in self.server.databases

    def fetchone(self) -> Any:
        return (1,) if self.found else None


class FakeServer:
    def __init__(self, *databases: str) -> None:
        self.databases, self.sql, self.dsn = set(databases), [], []

    def connect(self, dsn: str) -> Any:
        self.dsn.append(dsn)
        return types.SimpleNamespace(cursor=lambda: FakeCursor(self), close=lambda: None, autocommit=False)


def test_the_state_database_is_created_once_on_the_state_server(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    server = FakeServer()
    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(connect=server.connect))
    lookup(monkeypatch, {"rivet_state": connection("rivet_state", "postgres", extra={"rivet_params": "sslmode=disable"})})
    refs = {
        "RIVET_SOURCE_URL": {"conn_id": "shop", "odbc_ini": str(init.tmp / "odbc.ini")},
        "RIVET_STATE_URL": {"conn_id": "rivet_state", "database": "rivet_state_shop"},
    }
    init.run(env_from_connections=refs)
    assert server.dsn == ["postgresql://u:pw@rivet_state.host/postgres?sslmode=disable"]  # the server's own database
    assert server.databases == {"rivet_state_shop"}
    assert {c["state"] for c in init.calls()} == {"postgresql://u:pw@rivet_state.host/rivet_state_shop?sslmode=disable"}
    init.config.unlink()
    init.run(env_from_connections=refs)
    assert [s for s, _ in server.sql if s.startswith("CREATE")] == ['CREATE DATABASE "rivet_state_shop"']


# --- build_source_dags ---


def build(tmp_path: Path, mode: str = "cdc", **kwargs: Any) -> dict:
    kwargs.setdefault("init_args", ["--gcs-bucket", "b", "--bigquery-dataset", "rivet_{name}"])
    return build_source_dags(
        ["shop"],
        mode=mode,
        config_dir=str(tmp_path),
        state_root="/srv/state",
        state_connection="rivet_state",
        schedule="0 */2 * * *",
        **kwargs,
    )


def test_a_source_without_a_config_gets_a_paused_dag_whose_one_task_writes_it(tmp_path: Path) -> None:
    dag = build(tmp_path)["rivet_shop_cdc"]
    assert [t.task_id for t in dag.tasks] == ["init"]
    assert getattr(dag, "is_paused_upon_creation", None) is True and dag.max_active_runs == 1
    task = dag.get_task("init")
    assert (task.config, task.state_dir, task.mode, task.retries) == (str(tmp_path / "shop.yaml"), "/srv/state/shop", "cdc", 0)
    assert task.init_args == ["--gcs-bucket", "b", "--bigquery-dataset", "rivet_shop"]
    assert task.env_from_connections == {
        "RIVET_SOURCE_URL": {"conn_id": "shop", "odbc_ini": "/etc/odbc.ini", "dsn": "shop"},
        "RIVET_STATE_URL": {"conn_id": "rivet_state", "database": "rivet_state_shop"},
    }


def test_with_a_config_the_same_dag_streams_loads_and_compacts_each_table(tmp_path: Path) -> None:
    (tmp_path / "shop.yaml").write_text(CDC_CONFIG.format(env="RIVET_SOURCE_URL"))
    dag = build(tmp_path, odbc_ini=None, operator_kwargs={"cloud_credentials": ["gcp"]})["rivet_shop_cdc"]
    assert {t.task_id for t in dag.tasks} == {
        "run",
        "load.orders",
        "load.customers",
        "compact.orders",
        "compact.customers",
        "watcher",
    }
    run = dag.get_task("run")
    assert (run.export, run.config, run.state_dir, run.cloud_credentials) == (
        "shop_cdc",
        str(tmp_path / "shop.yaml"),
        "/srv/state/shop",
        ("gcp",),
    )
    assert run.env_from_connections["RIVET_SOURCE_URL"] == {"conn_id": "shop"}


def test_a_config_moved_from_cron_gets_the_source_under_the_variable_it_names(tmp_path: Path) -> None:
    (tmp_path / "shop.yaml").write_text(CDC_CONFIG.format(env="RIVET_MYSQL_URL"))
    run = build(tmp_path)["rivet_shop_cdc"].get_task("run")
    assert set(run.env_from_connections) == {"RIVET_MYSQL_URL", "RIVET_STATE_URL"}


def test_batch_runs_and_loads_every_export(tmp_path: Path) -> None:
    (tmp_path / "shop.yaml").write_text(BATCH_CONFIG)
    dag = build(tmp_path, mode="batch")["rivet_shop_batch"]
    assert {t.task_id for t in dag.tasks} == {"apply.orders", "apply.customers", "load.orders", "load.customers", "watcher"}


@pytest.mark.parametrize("sources", [["Shop"], ["shop-1"], ["shop", "shop"], "shop"])
def test_source_names_are_distinct_file_and_database_names(tmp_path: Path, sources: Any) -> None:
    with pytest.raises(ValueError):
        build_source_dags(sources, mode="cdc", config_dir=str(tmp_path), state_root="/s", state_connection="st")


def test_the_builder_owns_each_source_s_connections_config_and_state(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"may not set \['env_from_connections'\]"):
        build(tmp_path, operator_kwargs={"env_from_connections": {}})


def cdc_config(load: str = LOAD, stream: str = "    tables: [orders, customers]\n") -> str:
    head, tail = CDC_CONFIG.format(env="RIVET_SOURCE_URL").split("load:\n" + LOAD)
    return head + (f"load:\n{load}" if load else "") + tail.replace("    tables: [orders, customers]\n", stream)


def tasks(tmp_path: Path, text: str) -> set[str]:
    (tmp_path / "shop.yaml").write_text(text)
    return {t.task_id for t in build(tmp_path)["rivet_shop_cdc"].tasks}


def test_a_stream_without_a_warehouse_only_runs(tmp_path: Path) -> None:
    assert tasks(tmp_path, cdc_config(load="")) == {"run", "watcher"}


def test_a_change_log_warehouse_loads_without_compacting(tmp_path: Path) -> None:
    clickhouse = "  target: clickhouse\n  database: shop\n"
    assert tasks(tmp_path, cdc_config(load=clickhouse)) == {"run", "load.orders", "load.customers", "watcher"}


def test_a_stream_of_one_table_named_by_table(tmp_path: Path) -> None:
    assert tasks(tmp_path, cdc_config(stream="    table: orders\n")) == {"run", "load.orders", "compact.orders", "watcher"}


@pytest.mark.parametrize(
    ("other", "shared"),
    [
        (CDC_CONFIG.format(env="X"), "bigquery p.shop"),
        (
            CDC_CONFIG.format(env="X").replace("dataset: shop", "dataset: other")
            + "    destination: {type: gcs, bucket: b, prefix: shop/cdc/}\n",
            "gcs://b/shop/cdc/",
        ),
    ],
)
def test_a_config_writing_where_another_source_writes_is_refused(init: Init, other: str, shared: str) -> None:
    init.spec(yaml=CDC_CONFIG.format(env="RIVET_SOURCE_URL") + "    destination: {type: gcs, bucket: b, prefix: shop/cdc/}\n")
    init.config.parent.mkdir()
    (init.config.parent / "billing.yaml").write_text(other)
    out = init.run()
    assert isinstance(out, AirflowFailException) and f"writes to {shared}, as" in str(out) and "no config was written" in str(out)
    assert not init.config.exists()


def test_a_config_writing_its_own_places_is_written_beside_others(init: Init) -> None:
    init.config.parent.mkdir()
    (init.config.parent / "billing.yaml").write_text(CDC_CONFIG.format(env="X").replace("dataset: shop", "dataset: billing"))
    assert init.run()["written"] is True


def test_init_makes_the_source_s_state_folder_but_never_the_state_root(init: Init) -> None:
    assert init.run(state_dir=str(init.tmp / "absent" / "shop")).args[0].startswith(f"{init.tmp / 'absent'} does not exist")
    assert not (init.tmp / "absent").exists() and init.calls() == []


def test_init_with_storage_and_warehouse_connections_reads_no_config(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    lookup(monkeypatch, {"s3": connection("s3", "aws")})  # every task of the DAG gets them, init before any config
    out = init.run(connections=["s3"])
    assert out["written"] is True


def test_the_client_layout_connection_rivet_name_falls_back_to_the_data_source_name(
    odbc_ini: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "shop.yaml").write_text(CDC_CONFIG.format(env="RIVET_MYSQL_URL"))
    run = build(tmp_path, source_connection="rivet_{name}", odbc_ini=odbc_ini)["rivet_shop_cdc"].get_task("run")
    ref = run.env_from_connections["RIVET_MYSQL_URL"]
    assert ref == {"conn_id": "rivet_shop", "odbc_ini": odbc_ini, "dsn": "shop"}
    lookup(monkeypatch, {"rivet_shop": connection("rivet_shop", "mysql", schema="from_airflow")})
    assert run._database(database_ref(ref)).schema == "from_airflow"
    lookup(monkeypatch, {})
    assert run._database(database_ref(ref)).host == "db1.internal"  # [shop], not [rivet_shop]


def test_a_config_that_vanishes_after_a_stat_still_builds_the_init_dag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Path, "exists", lambda self: True)  # Docker Desktop's file cache answered so for a deleted file
    monkeypatch.setattr(Path, "is_file", lambda self: True)
    assert [t.task_id for t in build(tmp_path)["rivet_shop_cdc"].tasks] == ["init"]


def test_init_writes_a_config_a_stale_stat_claims_is_there(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Path, "exists", lambda self: True)
    assert init.run()["written"] is True
