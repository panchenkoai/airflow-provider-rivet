"""quickstart/: the DAG file builds one DAG per pipeline from its config, and the files that name a pipeline's
resources (compose services, demo SQL, configs, PIPELINES) name the same ones."""

from __future__ import annotations

import importlib.util
import re
import shutil
from pathlib import Path

import pytest
import yaml

QUICKSTART = Path(__file__).resolve().parents[1] / "quickstart"
DAG_FILE = QUICKSTART / "dags" / "rivet_quickstart.py"
CONFIGS = QUICKSTART / "dags" / "rivet"
COMPOSE = {v: QUICKSTART / f"airflow-{v}" / "docker-compose.yaml" for v in ("2.10", "3.3")}
AIRFLOW_SERVICES = {
    "postgres",
    "airflow-init",
    "airflow-scheduler",
    "airflow-webserver",
    "airflow-apiserver",
    "airflow-dag-processor",
}


def load(dags: Path):
    spec = importlib.util.spec_from_file_location(f"rivet_quickstart_{id(dags)}", dags / "rivet_quickstart.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def config(name: str) -> dict:
    return yaml.safe_load((CONFIGS / f"{name}.yaml").read_text())


def compose(version: str) -> dict:
    return yaml.safe_load(COMPOSE[version].read_text())


def test_each_pipeline_is_a_dag_of_its_kind_with_its_own_state_and_connections() -> None:
    module = load(DAG_FILE.parent)
    assert set(module.PIPELINES) == {p.stem for p in CONFIGS.glob("*.yaml")}
    for pipeline, conns in module.PIPELINES.items():
        dag = getattr(module, f"rivet_{pipeline}")
        tasks = {t.task_id for t in dag.tasks}
        if pipeline.endswith("_cdc"):
            assert tasks == {"run", "load.customers", "load.orders", "watcher"}, pipeline  # ClickHouse: no compact
        else:
            assert tasks == {"apply.customers", "apply.orders", "load.customers", "load.orders", "watcher"}, pipeline
        for task in dag.tasks:
            if task.task_id == "watcher":
                continue
            assert task.config == str(CONFIGS / f"{pipeline}.yaml")
            assert task.state_dir == f"/opt/airflow/rivet-state/{pipeline}"
            assert task.env_from_connections == {
                "RIVET_SOURCE_URL": conns["source"],
                "RIVET_STATE_URL": f"rivet_state_{pipeline}",
            }
            assert tuple(task.connections) == ("rivet_s3", "rivet_clickhouse")


def test_a_cdc_config_of_one_table_and_own_storage_and_warehouse(tmp_path: Path) -> None:
    dags = tmp_path / "dags"
    shutil.copytree(DAG_FILE.parent, dags)
    (dags / "rivet" / "billing_cdc.yaml").write_text(
        "source: {type: postgres, url_env: RIVET_SOURCE_URL}\n"
        "exports:\n  - name: invoices_cdc\n    table: invoices\n    mode: cdc\n"
    )
    text = (dags / "rivet_quickstart.py").read_text()
    line = '    "billing_cdc": {"source": "rivet_billing", "storage": "rivet_s3_aws", "warehouse": "rivet_clickhouse_own"},\n'
    (dags / "rivet_quickstart.py").write_text(text.replace("PIPELINES = {\n", "PIPELINES = {\n" + line, 1))
    dag = load(dags).rivet_billing_cdc
    assert {t.task_id for t in dag.tasks} == {"run", "load.invoices", "watcher"}
    assert tuple(dag.get_task("run").connections) == ("rivet_s3_aws", "rivet_clickhouse_own")


def test_every_config_reads_the_source_the_dag_fills_and_batch_declares_its_key() -> None:
    for path in CONFIGS.glob("*.yaml"):
        doc = config(path.stem)
        assert doc["source"]["url_env"] == "RIVET_SOURCE_URL", path.name
        assert doc["load"]["target"] == "clickhouse" and doc["load"]["database"] == path.stem, path.name
        if path.stem.endswith("_batch"):
            # rivet#679: a `query:` export has no recorded key on a state `init` did not write
            assert doc["load"]["pk"] == ["id"], path.name


def test_the_two_compose_files_differ_only_in_airflow() -> None:
    a, b = compose("2.10"), compose("3.3")
    shared = lambda d: {k: v for k, v in d["services"].items() if k not in AIRFLOW_SERVICES}  # noqa: E731
    assert shared(a) == shared(b)
    conns = lambda d: {k: v for k, v in d["x-airflow-common"]["environment"].items() if k.startswith("AIRFLOW_CONN_")}  # noqa: E731
    assert conns(a) == conns(b)
    assert a["volumes"] == b["volumes"]


def test_every_pipeline_has_its_bucket_state_database_and_clickhouse_database() -> None:
    pipelines = set(load(DAG_FILE.parent).PIPELINES)
    services = compose("3.3")["services"]
    buckets = {arg.split("/", 1)[1] for arg in services["minio-bucket"]["entrypoint"] if arg.startswith("quickstart/")}
    assert buckets == {e["destination"]["bucket"] for p in pipelines for e in config(p)["exports"]}
    sql = (QUICKSTART / "demo" / "rivet-state.sql").read_text()
    assert set(re.findall(r"CREATE DATABASE rivet_state_(\w+)", sql)) == pipelines
    assert (
        set(re.findall(r"CREATE DATABASE IF NOT EXISTS (\w+)", (QUICKSTART / "demo" / "clickhouse.sql").read_text())) == pipelines
    )
    env = compose("3.3")["x-airflow-common"]["environment"]
    assert {
        k.removeprefix("AIRFLOW_CONN_RIVET_STATE_").lower() for k in env if k.startswith("AIRFLOW_CONN_RIVET_STATE_")
    } == pipelines
    init = " ".join(services["airflow-init"]["command"])
    assert set(re.search(r"rivet-state/\{([^}]*)\}", init).group(1).split(",")) == pipelines


@pytest.mark.parametrize("version", sorted(COMPOSE))
def test_compose_pins_its_airflow_version(version: str) -> None:
    common = compose(version)["x-airflow-common"]
    full = common["build"]["args"]["AIRFLOW_VERSION"]
    assert full.startswith(version + ".") and common["image"] == f"rivet-quickstart:{full}"
