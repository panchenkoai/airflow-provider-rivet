"""build_source_dags: a DAG per named database, which writes its own config with `rivet init` and then runs it."""

from __future__ import annotations

import json
import re
import sys
import types
from pathlib import Path
from typing import Any

import pytest
from conftest import FakeTI

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
    if spec.get("sleep"):
        import time
        time.sleep(spec["sleep"])
    if spec.get("raw"):
        sys.stdout.buffer.write(b"caf\\xe9 table\\n")
    if spec.get("init_exit"):
        print("error: cannot connect", file=sys.stderr)
        sys.exit(spec["init_exit"])
    Path(argv[argv.index("-o") + 1]).write_text(spec["yaml"])
    print("wrote config, 100% %s of it")
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
    assert odbc_connection(odbc_ini, "mssql").conn_type == "mssql"


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
        source_env="RIVET_SOURCE_URL",
        config=str(tmp_path / "cfg" / "shop.yaml"),
        **{"rivet_bin": str(tmp_path / "bin" / "rivet"), "mode": "cdc", **kwargs},
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
            return init_operator(self.tmp, **kwargs).execute({"ti": FakeTI(task_id="init"), "run_id": "r"})
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

    def racing(self: RivetInitOperator, binary: str, argv: list[str], env: Any, cwd: Path) -> None:
        real(self, binary, argv, env, cwd)
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


def stale(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stat that says `shop.yaml` is there when it is not, as Docker Desktop's file cache answered for a deleted file;
    every other path (mutmut's own files among them) is asked for real."""
    for name in ("exists", "is_file"):
        real = getattr(Path, name)
        monkeypatch.setattr(Path, name, lambda self, real=real: self.name == "shop.yaml" or real(self))


def test_a_config_that_vanishes_after_a_stat_still_builds_the_init_dag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stale(monkeypatch)
    assert [t.task_id for t in build(tmp_path)["rivet_shop_cdc"].tasks] == ["init"]


def test_init_writes_a_config_a_stale_stat_claims_is_there(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    stale(monkeypatch)
    assert init.run()["written"] is True


# --- each message and decision, word for word ---


@pytest.fixture
def logged(caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture) -> Any:
    """The task log so far: Airflow 2 logs through logging, Airflow 3 to stdout."""
    caplog.set_level("INFO")
    return lambda: caplog.text + capsys.readouterr().out


def said(text: str, message: str) -> bool:
    """`message` is one whole log line: `] message [airflow...` on Airflow 3, `... message` at a line's end on 2.10."""
    framed = re.compile(rf"\] {re.escape(message)} +\[")  # Airflow 3 pads the message to a column
    return any(framed.search(line) or line.endswith(f" {message}") for line in text.splitlines())


def refusal(call: Any) -> PreflightRefusal:
    with pytest.raises(PreflightRefusal) as exc:
        call()
    return exc.value


def write(tmp_path: Path, text: str) -> str:
    path = tmp_path / "odbc.ini"
    path.write_text(text)
    return str(path)


def test_odbc_refusals_carry_their_code_and_text(tmp_path: Path) -> None:
    path = write(tmp_path, "Driver = MySQL\n")
    r = refusal(lambda: odbc_connection(path, "shop"))
    assert (r.code, str(r)) == (
        "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED",
        f"[{r.code}] {path} is not a readable odbc.ini (MissingSectionHeaderError)",
    )
    path = write(tmp_path, "[shop]\nServer = h\n")
    r = refusal(lambda: odbc_connection(path, "shop"))
    assert str(r) == (
        f"[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] ODBC data source [shop] in {path} uses driver ``; rivet reads MySQL, "
        "PostgreSQL, SQL Server and Oracle data sources from odbc.ini: create an Airflow Connection `shop` instead"
    )
    path = write(tmp_path, "[shop]\nDriver = MySQL\nPort = x\n")
    r = refusal(lambda: odbc_connection(path, "shop"))
    assert (r.code, str(r).split("] ", 1)[1]) == (
        "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED",
        f"ODBC data source [shop] in {path} has port `x`, which is no number",
    )


def test_odbc_reads_percent_signs_and_repeated_keys_as_written(tmp_path: Path) -> None:
    path = write(tmp_path, "[shop]\nDriver = MySQL\nServer = h\nPassword = 50%off\nServer = h2\n[shop]\nDatabase = d\n")
    conn = odbc_connection(path, "shop")
    assert (conn.conn_id, conn.host, conn.password, conn.schema) == ("shop", "h2", "50%off", "d")


def test_a_bad_reference_names_the_keys_it_takes() -> None:
    with pytest.raises(ValueError) as exc:
        database_ref({"conn_id": "pg", "db": "d"})
    assert str(exc.value) == (
        "an env_from_connections dict has `conn_id` and optionally ['database', 'dsn', 'odbc_ini', 'scheme']; got ['conn_id', 'db']"
    )


def test_with_database_reads_a_connection_without_every_field() -> None:
    bare = types.SimpleNamespace(conn_id="s", conn_type="postgres", host="h")
    moved = with_database(bare, "d")
    assert (moved.port, moved.login, moved.password, moved.schema, moved.extra_dejson) == (None, None, None, "d", {})


def test_the_missing_source_refusal_word_for_word(odbc_ini: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lookup(monkeypatch, {})
    head = "[RIVET_AIRFLOW_CONNECTION_MISSING] connection `nope` is not defined in the metadata database or any secrets backend this worker reads"
    op = init_operator(tmp_path)
    r = refusal(lambda: op._database({"conn_id": "nope", "odbc_ini": odbc_ini, "dsn": "nada"}))
    assert str(r) == f"{head}, and {odbc_ini} has no data source [nada]; create it, or correct the id the task names"
    r = refusal(lambda: op._database({"conn_id": "nope"}))
    assert str(r) == f"{head}; create it, or correct the id the task names"


def test_the_data_source_log_line_word_for_word(
    odbc_ini: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, logged: Any
) -> None:
    lookup(monkeypatch, {})
    init_operator(tmp_path)._database({"conn_id": "rivet_shop", "odbc_ini": odbc_ini, "dsn": "shop"})
    assert said(logged(), f"database `rivet_shop`: no such Connection, ODBC data source [shop] in {odbc_ini}")


@pytest.mark.parametrize("owned", ["--source", "--source-env", "--source-file", "-o", "--output", "--mode"])
def test_every_flag_the_operator_owns_is_refused(tmp_path: Path, owned: str) -> None:
    with pytest.raises(ValueError) as exc:
        init_operator(tmp_path, init_args=["--tls", "disable", owned, "x"])
    assert str(exc.value) == f"init_args may not set ['{owned}']: the operator sets the source, the output and the CDC mode"


def test_init_takes_cdc_or_batch(tmp_path: Path) -> None:
    with pytest.raises(ValueError) as exc:
        RivetInitOperator(task_id="i", mode="CDC", source_env="S", config="c", env_from_connections={"S": "s"})
    assert str(exc.value) == "`mode` is 'cdc' or 'batch'"
    assert (
        RivetInitOperator(task_id="i", mode="batch", source_env="S", config="c", env_from_connections={"S": "s"}).mode == "batch"
    )


def test_batch_init_asks_rivet_for_no_mode_and_runs_where_it_writes(init: Init) -> None:
    op = init_operator(init.tmp)
    op.mode = "batch"
    out = op.execute({"ti": FakeTI(task_id="init"), "run_id": "r"})
    work = str(init.tmp / "state" / "shop" / ".init")
    first, second = init.calls()
    assert "--mode" not in first["argv"] and first["cwd"] == work == second["cwd"]
    assert out == {
        "config": str(init.config),
        "written": True,
        "exports": [
            {"name": "shop_orders", "mode": "full", "tables": None},
            {"name": "shop_cdc", "mode": "cdc", "tables": ["orders", "customers"]},
        ],
    }


def test_init_logs_rivet_s_lines_and_what_it_wrote_word_for_word(init: Init, logged: Any) -> None:
    init.run()
    text = logged()
    assert said(text, "wrote config, 100% %s of it")  # a line is logged as text, never as a format
    lines = [line.split(" [airflow.task")[0].rstrip() for line in text.splitlines()]  # Airflow 3 appends the logger
    assert any(line.endswith("export shop_orders (full)") for line in lines)
    assert any(line.endswith("export shop_cdc (cdc): orders, customers") for line in lines)
    assert said(text, f"{init.config} written; the next DAG parse replaces this task with the rivet tasks it describes")


def test_init_names_the_failing_command(init: Init) -> None:
    init.spec(check_exit=5)
    assert str(init.run()) == "`rivet check` exited 5; its output is above, and no config was written"


def test_init_makes_a_config_folder_two_levels_deep(init: Init) -> None:
    op = init_operator(init.tmp)
    op.config = str(init.tmp / "a" / "b" / "shop.yaml")
    assert op.execute({"ti": FakeTI(task_id="init"), "run_id": "r"})["written"] is True


def test_the_existing_config_log_line_word_for_word(init: Init, logged: Any) -> None:
    init.config.parent.mkdir()
    init.config.write_text("x: 1\n")
    init.run()
    assert said(
        logged(),
        f"{init.config} exists: the next DAG parse builds this DAG's rivet tasks from it. If this task keeps running "
        "instead, the scheduler does not see this file: put the config folder where both read it",
    )


def test_a_config_written_meanwhile_is_logged_and_its_places_are_not_another_source_s(
    init: Init, monkeypatch: pytest.MonkeyPatch, logged: Any
) -> None:
    real = RivetInitOperator._rivet

    def racing(self: RivetInitOperator, binary: str, argv: list[str], env: Any, cwd: Path) -> None:
        real(self, binary, argv, env, cwd)
        if argv[0] == "check":
            init.config.parent.mkdir(exist_ok=True)
            init.config.write_text(CDC_CONFIG.format(env="RIVET_SOURCE_URL"))  # the same places: the same source

    monkeypatch.setattr(RivetInitOperator, "_rivet", racing)
    assert init.run()["written"] is True
    assert said(logged(), f"{init.config} was written meanwhile; it is kept")


def test_an_unreadable_neighbour_is_skipped_and_the_next_one_still_checked(init: Init) -> None:
    init.config.parent.mkdir()
    (init.config.parent / "a_broken.yaml").write_bytes(b"\xff\xfe")
    (init.config.parent / "billing.yaml").write_text(CDC_CONFIG.format(env="X"))
    out = init.run()
    assert str(out) == (
        f"the config `rivet init` wrote for shop.yaml writes to bigquery p.shop, as {init.config.parent / 'billing.yaml'} "
        "does; give each source its own place in init_args (`--gcs-bucket`/`--s3-bucket`, `--bigquery-dataset {name}`, "
        "`--clickhouse-database {name}`); no config was written"
    )


def test_places_read_every_warehouse_name_and_a_bucket_root() -> None:
    from airflow_provider_rivet.operators import places

    doc = {
        "load": {"target": "snowflake", "database": "DB", "schema": "S"},
        "exports": [{"destination": {"type": "s3", "bucket": "b"}}, {"destination": {"type": "local", "path": "/x"}}],
    }
    assert places(doc) == {"s3://b/", "snowflake DB.S"}


class Server(FakeServer):
    """A server that, as PostgreSQL, refuses CREATE DATABASE inside a transaction."""

    def connect(self, dsn: str) -> Any:
        self.dsn.append(dsn)
        conn = types.SimpleNamespace(close=lambda: None, autocommit=False)
        conn.cursor = lambda: Cursor(self, conn)
        return conn


class Cursor(FakeCursor):
    def __init__(self, server: FakeServer, conn: Any) -> None:
        super().__init__(server)
        self.conn = conn

    def execute(self, sql: str, args: tuple = ()) -> None:
        if sql.startswith("CREATE") and self.conn.autocommit is not True:
            raise RuntimeError("CREATE DATABASE cannot run inside a transaction block")
        super().execute(sql, args)


def state_refs(init: Init, database: str) -> dict:
    return {
        "RIVET_SOURCE_URL": {"conn_id": "shop", "odbc_ini": str(init.tmp / "odbc.ini")},
        "RIVET_STATE_URL": {"conn_id": "rivet_state", "database": database},
    }


def test_the_state_database_sql_word_for_word(init: Init, monkeypatch: pytest.MonkeyPatch, logged: Any) -> None:
    server = Server()
    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(connect=server.connect))
    lookup(monkeypatch, {"rivet_state": connection("rivet_state", "postgres", schema="admin")})
    init.run(env_from_connections=state_refs(init, 'we"ird'))
    assert server.dsn == ["postgresql://u:pw@rivet_state.host/admin"]
    assert server.sql == [("SELECT 1 FROM pg_database WHERE datname = %s", ('we"ird',)), ('CREATE DATABASE "we""ird"', ())]
    assert said(logged(), 'state database `we"ird` created on rivet_state.host')


def test_without_psycopg2_the_state_database_is_refused(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    lookup(monkeypatch, {"rivet_state": connection("rivet_state", "postgres")})
    out = init.run(env_from_connections=state_refs(init, "rivet_state_shop"))
    assert str(out) == "[RIVET_AIRFLOW_STATE_DATABASE] creating state database `rivet_state_shop` needs psycopg2 on the worker"


# --- the builder's decisions ---


def test_builder_refusals_word_for_word(tmp_path: Path) -> None:
    def message(**kwargs: Any) -> str:
        args = {"sources": ["shop"], "mode": "cdc", "config_dir": str(tmp_path), "state_root": "/s", "state_connection": "st"}
        with pytest.raises(ValueError) as exc:
            build_source_dags(**{**args, **kwargs})
        return str(exc.value)

    assert message(sources=["Shop", "ok"]) == "source names are distinct and of [a-z0-9_]: ['Shop']"
    assert message(sources=["a", "a"]) == "source names are distinct and of [a-z0-9_]: ['a']"
    assert message(sources="shop") == "`sources` is a list of names, not a string: write sources=['shop']"
    assert message(mode="CDC") == "`mode` is 'cdc' or 'batch'"
    for key in ("config", "state_dir"):
        assert message(operator_kwargs={key: "x"}) == f"operator_kwargs may not set ['{key}']: each source's DAG sets them"
    assert message(operator_kwargs={"trigger_rule": "all_done"}).startswith("operator_kwargs may not set `trigger_rule`")
    (tmp_path / "shop.yaml").write_text("exports: []\n")
    assert message() == f"{tmp_path / 'shop.yaml'}: no export with `mode: cdc`"


def test_every_source_is_built_whichever_has_a_config(tmp_path: Path) -> None:
    (tmp_path / "zeta.yaml").write_text(CDC_CONFIG.format(env="RIVET_SOURCE_URL"))
    dags = build_source_dags(["alpha", "zeta"], mode="cdc", config_dir=str(tmp_path), state_root="/s", state_connection="st")
    assert {d: [t.task_id for t in dag.tasks][:1] for d, dag in dags.items()} == {
        "rivet_alpha_cdc": ["init"],
        "rivet_zeta_cdc": ["run"],
    }
    assert sorted(dags["rivet_alpha_cdc"].tags) == ["cdc", "init", "rivet"]


def test_a_config_naming_no_url_env_gets_the_source_under_rivet_source_url(tmp_path: Path) -> None:
    (tmp_path / "shop.yaml").write_text(CDC_CONFIG.format(env="RIVET_SOURCE_URL").replace("  url_env: RIVET_SOURCE_URL\n", ""))
    assert set(build(tmp_path)["rivet_shop_cdc"].get_task("run").env_from_connections) == {"RIVET_SOURCE_URL", "RIVET_STATE_URL"}


def test_a_stream_naming_no_table_without_a_warehouse_only_runs(tmp_path: Path) -> None:
    assert tasks(tmp_path, cdc_config(load="", stream="")) == {"run", "watcher"}


def test_batch_leaves_out_the_stream_and_loads_only_with_a_warehouse(tmp_path: Path) -> None:
    stream = "  - name: cdc\n    mode: cdc\n    tables: [orders]\n"
    (tmp_path / "shop.yaml").write_text(BATCH_CONFIG + stream)
    assert {t.task_id for t in build(tmp_path, mode="batch")["rivet_shop_batch"].tasks} == {
        "apply.orders",
        "apply.customers",
        "load.orders",
        "load.customers",
        "watcher",
    }
    (tmp_path / "shop.yaml").write_text(BATCH_CONFIG.replace("load:\n  target: bigquery\n  project: p\n  dataset: shop\n", ""))
    assert {t.task_id for t in build(tmp_path, mode="batch")["rivet_shop_batch"].tasks} == {
        "apply.orders",
        "apply.customers",
        "watcher",
    }


GCS_DEST = "    destination: {type: gcs, bucket: b, prefix: shop/cdc/}\n"


def test_every_shared_place_is_named(init: Init) -> None:
    init.spec(yaml=CDC_CONFIG.format(env="RIVET_SOURCE_URL") + GCS_DEST)
    init.config.parent.mkdir()
    (init.config.parent / "billing.yaml").write_text(CDC_CONFIG.format(env="X") + GCS_DEST)
    assert "writes to bigquery p.shop, gcs://b/shop/cdc/, as" in str(init.run())


def test_a_missing_binary_is_refused_with_the_payload_every_task_pushes(init: Init) -> None:
    ti = FakeTI(task_id="init")
    op = init_operator(init.tmp, rivet_bin="rivet-nowhere")
    with pytest.raises(AirflowFailException, match=r"^\[RIVET_AIRFLOW_BINARY_MISSING\] no rivet binary `rivet-nowhere`"):
        op.execute({"ti": ti, "run_id": "r"})
    assert ti.store[("init", "return_value")] == {
        "command": "init",
        "decision": "fail",
        "preflight_refusal": "RIVET_AIRFLOW_BINARY_MISSING",
        "units": [],
    }
    assert init.calls() == []


def test_the_packaged_binary_is_found_off_the_path(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("airflow_provider_rivet.preflight._packaged_rivet", lambda: str(init.tmp / "bin" / "rivet"))
    op = init_operator(init.tmp, rivet_bin="rivet", env_passthrough=())
    monkeypatch.setenv("PATH", "/nonexistent")
    assert op.execute({"ti": FakeTI(task_id="init"), "run_id": "r"})["written"] is True


def test_a_stop_before_a_step_starts_none_and_is_retried(init: Init) -> None:
    ti = FakeTI(task_id="init")
    op = init_operator(init.tmp)
    op.on_kill()
    with pytest.raises(Exception) as exc:
        op.execute({"ti": ti, "run_id": "r"})
    assert type(exc.value).__name__ == "AirflowException"
    assert ti.store[("init", "return_value")] == {"command": "init", "decision": "retry", "stopped_before": "init", "units": []}
    assert init.calls() == [] and not init.config.exists()


def test_a_stop_reaches_the_running_rivet(init: Init) -> None:
    import threading
    import time

    init.spec(sleep=60)
    op = init_operator(init.tmp)
    threading.Timer(1.0, op.on_kill).start()
    started = time.monotonic()
    with pytest.raises(AirflowFailException, match="`rivet init` exited -15"):
        op.execute({"ti": FakeTI(task_id="init"), "run_id": "r"})
    assert time.monotonic() - started < 20 and not init.config.exists()


def test_rivet_s_stderr_and_bytes_that_are_not_utf8_reach_the_log(init: Init, logged: Any) -> None:
    init.spec(raw=True, init_exit=5)
    init.run()
    text = logged()
    assert said(text, "caf\ufffd table") and said(text, "error: cannot connect")
    assert sorted(p.name for p in (init.tmp / "state" / "shop" / ".init").glob("*.log")) == ["init.log"]


def test_a_key_file_connection_gets_the_task_s_private_folder(init: Init, monkeypatch: pytest.MonkeyPatch) -> None:
    gcp = connection("gcp", "google_cloud_platform", extra={"keyfile_dict": {"type": "service_account"}})
    lookup(monkeypatch, {"gcp": gcp})
    assert init.run(connections=["gcp"])["written"] is True


def test_a_stop_is_told_in_its_own_words_and_a_later_on_kill_finds_no_process(init: Init) -> None:
    op = init_operator(init.tmp)
    assert op.execute({"ti": FakeTI(task_id="init"), "run_id": "r"})["written"] is True
    op.on_kill()  # after the task: no process is held, nothing to signal
    init.config.unlink()
    with pytest.raises(Exception) as exc:
        op.execute({"ti": FakeTI(task_id="init"), "run_id": "r"})
    assert str(exc.value) == "the task was stopped before rivet init started; rivet init was not run"


def test_another_source_sharing_a_place_is_found_beside_a_config_written_meanwhile(
    init: Init, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = RivetInitOperator._rivet

    def racing(self: RivetInitOperator, binary: str, argv: list[str], env: Any, cwd: Path) -> None:
        real(self, binary, argv, env, cwd)
        if argv[0] == "check":
            init.config.write_text("x: 1\n")  # this source's own, written by another try

    init.config.parent.mkdir()
    (init.config.parent / "zeta.yaml").write_text(CDC_CONFIG.format(env="X"))  # sorted after shop.yaml
    monkeypatch.setattr(RivetInitOperator, "_rivet", racing)
    assert "writes to bigquery p.shop, as" in str(init.run())


def test_resolve_database_takes_any_lookup(odbc_ini: str) -> None:
    from airflow_provider_rivet.connections import resolve_database

    found = {"shop": connection("shop", "mysql", schema="a")}
    assert resolve_database({"conn_id": "shop", "database": "b"}, found.get)[0].schema == "b"
    conn, dsn = resolve_database({"conn_id": "rivet_shop", "odbc_ini": odbc_ini, "dsn": "shop"}, {}.get)
    assert (conn.host, dsn) == ("db1.internal", "shop")
    assert resolve_database({"conn_id": "shop", "odbc_ini": odbc_ini}, found.get)[1] is None  # the Connection: no data source
    with pytest.raises(PreflightRefusal, match=r"connection `x` is not defined .*reads; create it"):
        resolve_database({"conn_id": "x"}, {}.get)


PER_TABLE_STREAMS = """\
source:
  type: mssql
  url_env: RIVET_SOURCE_URL
load:
  target: bigquery
  project: p
  dataset: shop
  layout: base_buffer
exports:
  - name: dbo_orders
    table: dbo.orders
    mode: cdc
  - name: dbo_users
    table: dbo.users
    mode: cdc
  - name: users_recipe
    table: dbo.users
    mode: full
"""


def test_a_stream_per_table_gets_a_group_per_stream(tmp_path: Path) -> None:
    """rivet init on SQL Server, Oracle, MongoDB and PostgreSQL outside `public`: one CDC export per table."""
    (tmp_path / "shop.yaml").write_text(PER_TABLE_STREAMS)
    dag = build(tmp_path)["rivet_shop_cdc"]
    assert {t.task_id for t in dag.tasks} == {
        "dbo_orders.run",
        "dbo_orders.load.dbo_orders",
        "dbo_orders.compact.dbo_orders",
        "dbo_users.run",
        "dbo_users.load.dbo_users",
        "dbo_users.compact.dbo_users",
        "watcher",
    }
    run, load = dag.get_task("dbo_users.run"), dag.get_task("dbo_users.load.dbo_users")
    # `rivet load --table` takes the warehouse name: init's `dbo.users` folded (rivet 0.32.2: RIVET_CLI_TABLE_UNKNOWN)
    assert (run.export, load.export, load.table) == ("dbo_users", "dbo_users", "dbo_users")
    assert load.upstream_task_ids == {"dbo_users.run"}
    assert dag.get_task("dbo_users.compact.dbo_users").upstream_task_ids == {"dbo_users.load.dbo_users"}
    assert run.trigger_rule == "all_success" and run.env_from_connections["RIVET_SOURCE_URL"]["conn_id"] == "shop"


@pytest.mark.parametrize(
    ("section", "expected"),
    [
        (
            "Driver = ODBC Driver 18 for SQL Server\nServer = tcp:ms1,14330\nDatabase = shop\nUID = sa\nPWD = p\n",
            ("mssql", "ms1", 14330, "shop", "sqlserver://sa:p@ms1:14330/shop"),
        ),
        (
            "Driver = /usr/lib/libtdsodbc.so\nServer = ms1\nPort = 1433\nDatabase = shop\nUID = u\nPWD = p\n",
            ("mssql", "ms1", 1433, "shop", "sqlserver://u:p@ms1:1433/shop"),
        ),
        (
            "Driver = Oracle 21 ODBC driver\nServerName = //ora1:1521/FREEPDB1\nUserID = rivet\nPassword = p\n",
            ("oracle", "ora1", 1521, "FREEPDB1", "oracle://rivet:p@ora1:1521/FREEPDB1"),
        ),
        (
            "Driver = /opt/oracle/libsqora.so.21.1\nServerName = ora1/ORCL\nUserID = rivet\nPassword = p\n",
            ("oracle", "ora1", None, "ORCL", "oracle://rivet:p@ora1/ORCL"),
        ),
    ],
    ids=["msodbc", "freetds", "oracle-ezconnect", "oracle-no-port"],
)
def test_sql_server_and_oracle_data_sources_read_as_connections(tmp_path: Path, section: str, expected: tuple) -> None:
    from airflow_provider_rivet.connections import connection_url

    conn = odbc_connection(write(tmp_path, f"[src]\n{section}"), "src")
    assert (conn.conn_type, conn.host, conn.port, conn.schema, connection_url(conn)) == expected


@pytest.mark.parametrize(
    ("section", "why"),
    [
        (
            "Driver = ODBC Driver 18 for SQL Server\nServer = ms1\nTrusted_Connection = yes\n",
            "signs in with Windows or Entra ID; rivet signs in with a SQL login: set UID and PWD",
        ),
        (
            "Driver = ODBC Driver 18 for SQL Server\nServer = ms1\nAuthentication = ActiveDirectoryPassword\n",
            "signs in with Windows or Entra ID; rivet signs in with a SQL login: set UID and PWD",
        ),
        (
            "Driver = ODBC Driver 18 for SQL Server\nServer = ms1\\SQLEXPRESS\n",
            "names the instance `ms1\\SQLEXPRESS`; give the instance's TCP port: Server = host,port",
        ),
        (
            "Driver = Oracle 21 ODBC driver\nServerName = PRODDB\n",
            "names `PRODDB`, a tnsnames.ora alias; write ServerName = host:port/service, or create an Airflow Connection",
        ),
        (
            "Driver = MongoDB ODBC 1.4.2 Unicode Driver\nServer = m1\n",
            "uses driver `MongoDB ODBC 1.4.2 Unicode Driver`; MongoDB's ODBC driver reads through the BI Connector's MySQL "
            "protocol, not the database rivet reads: create an Airflow Connection `src` instead",
        ),
        (
            "Driver = Snowflake\nServer = s\n",
            "uses driver `Snowflake`; rivet reads MySQL, PostgreSQL, SQL Server and Oracle data sources from odbc.ini: "
            "create an Airflow Connection `src` instead",
        ),
    ],
    ids=["trusted", "entra", "named-instance", "tns-alias", "mongodb", "other"],
)
def test_what_a_data_source_cannot_give_rivet_is_refused(tmp_path: Path, section: str, why: str) -> None:
    path = write(tmp_path, f"[src]\n{section}")
    r = refusal(lambda: odbc_connection(path, "src"))
    assert str(r) == f"[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] ODBC data source [src] in {path} {why}"


def test_a_batch_dag_may_choose_init_s_mode_but_not_cdc(tmp_path: Path) -> None:
    op = init_operator(tmp_path, mode="batch", init_args=["--mode", "incremental"])
    assert op.init_args == ["--mode", "incremental"]
    with pytest.raises(ValueError, match="may not ask for `--mode cdc` in a batch DAG"):
        init_operator(tmp_path, mode="batch", init_args=["--mode", "cdc"])
    with pytest.raises(ValueError, match=r"may not set \['--mode'\]"):
        init_operator(tmp_path, mode="cdc", init_args=["--mode", "incremental"])
