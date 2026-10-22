"""Gate DAGs: incremental batch and CDC, each on one shared SQLite state and on a dedicated PostgreSQL state."""

from __future__ import annotations

import json
import os
from datetime import datetime

from airflow_provider_rivet._compat import DAG
from airflow_provider_rivet.dags import build_batch_dag, build_cdc_dag
from airflow_provider_rivet.operators import RivetLoadOperator

ROOT = "/opt/rivet"
SOURCE = {"RIVET_GATE_SOURCE_URL": "rivet_gate_source"}
PAUSE = {"RIVET_TEST_PAUSE_AT": "cdc_after_open:20000", "RIVET_TEST_PAUSE_MARKER": f"{ROOT}/markers/cdc_sqlite"}


def operator_kwargs(state: str) -> dict:
    """SQLite state lives in the shared state_dir (the SQLite CDC run holds a pause window); PostgreSQL state comes from a connection."""
    if state == "pg":
        return {"cwd": f"{ROOT}/out", "env_from_connections": {**SOURCE, "RIVET_STATE_URL": "rivet_gate_state"}}
    # One state_dir volume that every worker of the stack mounts: one disk, so SQLite under Celery is declared single-host.
    return {"cwd": f"{ROOT}/out", "env_from_connections": dict(SOURCE), "env": {"RIVET_STATE_URL": ""}, "single_host": True}


for state in ("sqlite", "pg"):
    state_dir = f"{ROOT}/state/{state}"
    globals()[f"gate_batch_{state}"] = build_batch_dag(
        f"gate_batch_{state}",
        config=f"{ROOT}/configs/batch_{state}.yaml",
        state_dir=state_dir,
        exports=[f"{state}_customers", f"{state}_orders"],
        load=False,
        operator_kwargs=operator_kwargs(state),
        tags=["rivet", "gate"],
    )
    cdc_kwargs = operator_kwargs(state)
    if state == "sqlite":
        cdc_kwargs["env"] = {**cdc_kwargs["env"], **PAUSE}
    globals()[f"gate_cdc_{state}"] = build_cdc_dag(
        f"gate_cdc_{state}",
        config=f"{ROOT}/configs/cdc_{state}.yaml",
        export=f"{state}_cdc",
        tables=["gate_customers", "gate_orders"],
        state_dir=state_dir,
        load=False,
        operator_kwargs=cdc_kwargs,
        tags=["rivet", "gate"],
    )


# The warehouse leg: GCS, then BigQuery / Snowflake / ClickHouse, every credential from a Connection.
# `--gcp-auth env-file` (the client's): no GCP Connection; the key file the compose env names reaches rivet through
# `cloud_credentials=["gcp"]` (GOOGLE_APPLICATION_CREDENTIALS and the other GOOGLE_* variables).
ENV_KEY = os.environ.get("GATE_GCP_AUTH") == "env-file"
# Where the source legs' state_dirs are: the client's own mount under `--state rivet-service`.
SOURCE_STATE = os.environ.get("GATE_SOURCE_STATE_ROOT") or f"{ROOT}/state"
GCP = {"cloud_credentials": ["gcp"]} if ENV_KEY else {"connections": ["gate_gcp"]}
WAREHOUSE_CONNECTIONS = {
    "bigquery": ["gate_gcp"],
    "snowflake": ["gate_gcp", "gate_snowflake"],
    "clickhouse": ["gate_gcp", "gate_clickhouse"],
}
for wh in filter(None, os.environ.get("GATE_WAREHOUSES", "").split(",")):
    kwargs = {
        "cwd": f"{ROOT}/out",
        "env_from_connections": dict(SOURCE),
        "env": {"RIVET_STATE_URL": ""},
        "single_host": True,  # the stack's one state volume
        "connections": [c for c in WAREHOUSE_CONNECTIONS[wh] if not (ENV_KEY and c == "gate_gcp")],
        **({"cloud_credentials": ["gcp"]} if ENV_KEY else {}),
    }
    globals()[f"gate_wh_{wh}_batch"] = build_batch_dag(
        f"gate_wh_{wh}_batch",
        config=f"{ROOT}/configs/wh_{wh}_batch.yaml",
        state_dir=f"{ROOT}/state/wh_{wh}",
        exports=[f"{wh}_customers", f"{wh}_orders"],
        compact_exports=[f"{wh}_customers", f"{wh}_orders"],
        operator_kwargs=kwargs,
        tags=["rivet", "gate", "warehouse", wh],
    )
    globals()[f"gate_wh_{wh}_cdc"] = build_cdc_dag(
        f"gate_wh_{wh}_cdc",
        config=f"{ROOT}/configs/wh_{wh}_cdc.yaml",
        export=f"{wh}_cdc",
        tables=["gate_customers", "gate_orders"],
        state_dir=f"{ROOT}/state/wh_{wh}",
        operator_kwargs=kwargs,
        tags=["rivet", "gate", "warehouse", wh],
    )


def leg_kwargs(engine: str, kind: str) -> dict:
    """The config's own PostgreSQL state: the one `rivet init` recorded the source's primary keys in."""
    state = f"rivet_gate_state_{engine}_{kind}"
    return {
        "env_from_connections": {f"RIVET_GATE_{engine.upper()}_URL": f"rivet_gate_{engine}", "RIVET_STATE_URL": state},
        **GCP,
    }


def leg_state_dir(engine: str, kind: str) -> str:
    """The config's state_dir under the source state root."""
    return f"{SOURCE_STATE}/{engine}_{kind}"


# Source legs (gate/sources.py): another engine to GCS and BigQuery, on the configs `rivet init` wrote; the export and
# stream names are init's, read from the shape the gate wrote beside them (no DAG until it exists).
for engine in filter(None, os.environ.get("GATE_SOURCES", "").split(",")):
    shape_file = f"{ROOT}/configs/{engine}_shape.json"
    if not os.path.exists(shape_file):
        continue
    with open(shape_file) as handle:
        shape = json.load(handle)

    # A kind the engine cannot run (MongoDB batch, Oracle CDC into a warehouse) has no shape and no DAG.
    batch = shape.get("batch", {}).get("exports", [])
    if batch:
        globals()[f"gate_src_{engine}_batch"] = build_batch_dag(
            f"gate_src_{engine}_batch",
            config=f"{ROOT}/configs/{engine}_batch/rivet.yaml",
            state_dir=leg_state_dir(engine, "batch"),
            exports=batch,
            compact_exports=batch,
            operator_kwargs=leg_kwargs(engine, "batch"),
            tags=["rivet", "gate", "source", engine],
        )
    # The stream's config: init's file, or the same text read through a git-sync checkout (`repo -> rev-<n>`).
    cdc_config = shape.get("cdc_config", f"{ROOT}/configs/{engine}_cdc/rivet.yaml")
    # One DAG per CDC stream: one over every table, or one per table where the engine captures per table (SQL Server,
    # MongoDB).
    for stream in shape.get("cdc", {}).get("streams", []):
        globals()[f"gate_src_{engine}{stream['dag']}"] = build_cdc_dag(
            f"gate_src_{engine}{stream['dag']}",
            config=cdc_config,
            export=stream["name"],
            tables=stream["tables"],
            state_dir=leg_state_dir(engine, "cdc"),
            operator_kwargs=leg_kwargs(engine, "cdc"),
            tags=["rivet", "gate", "source", engine],
        )
    # The stream's run that pauses right after it opened, while the gate adds a column to a captured table (run once,
    # after the cycles; the same config, export and state as the stream's own DAG, never at the same time).
    hot = shape.get("hot")
    if hot:
        hot_kwargs = leg_kwargs(engine, "cdc")
        hot_kwargs["env"] = {"RIVET_TEST_PAUSE_AT": hot["pause"], "RIVET_TEST_PAUSE_MARKER": hot["marker"]}
        globals()[hot["dag"]] = build_cdc_dag(
            hot["dag"],
            config=cdc_config,
            export=hot["stream"]["name"],
            tables=hot["stream"]["tables"],
            state_dir=leg_state_dir(engine, "cdc"),
            operator_kwargs=hot_kwargs,
            tags=["rivet", "gate", "source", engine],
        )
    # The second of two overlapping runs of the stream (the hot DAG's held run is the first): a short wait for the
    # provider's export lock, then one retry a minute later.
    overlap = shape.get("overlap")
    if overlap:
        from datetime import timedelta

        globals()[overlap["dag"]] = build_cdc_dag(
            overlap["dag"],
            config=cdc_config,
            export=overlap["stream"]["name"],
            tables=overlap["stream"]["tables"],
            state_dir=leg_state_dir(engine, "cdc"),
            operator_kwargs={**leg_kwargs(engine, "cdc"), "lock_wait": overlap["lock_wait"]},
            default_args={"retries": 1, "retry_delay": timedelta(seconds=60)},
            tags=["rivet", "gate", "source", engine],
        )
    # A second CDC config init wrote in a directory of its own, run in the first stream's state_dir: its copy there
    # names the same checkpoint file, which another config owns (rivet #664 refuses the run by code).
    shared = shape.get("shared")
    if shared:
        globals()[shared["dag"]] = build_cdc_dag(
            shared["dag"],
            config=shared["config"],
            export=shared["stream"]["name"],
            tables=shared["stream"]["tables"],
            state_dir=leg_state_dir(engine, "cdc"),
            operator_kwargs=leg_kwargs(engine, "cdc"),
            tags=["rivet", "gate", "source", engine],
        )


# Destination legs (gate/destinations.py): the postgres scenario to S3 (MinIO in the stack), extract-only and loaded
# into ClickHouse, on the configs `rivet init` wrote; the S3 credential and endpoint come from the `aws` Connection.
if "s3" in os.environ.get("GATE_DESTINATIONS", "").split(",") and os.path.exists(f"{ROOT}/configs/s3_shape.json"):
    with open(f"{ROOT}/configs/s3_shape.json") as handle:
        s3_shape = json.load(handle)
    for kind, cfg in s3_shape.items():
        kwargs = {
            "env_from_connections": {cfg["url_env"]: cfg["source_conn"], "RIVET_STATE_URL": cfg["state_conn"]},
            "connections": cfg["connections"],
        }
        dag_id = f"gate_s3_{kind}"
        tags = ["rivet", "gate", "destination", "s3"]
        if cfg.get("cleanup"):
            # `cleanup_source` (gate/cleanup.py): extract, extract with one apply held live, and load one export per
            # DAG; nothing retries, so a failed load is the run's verdict.
            c = cfg["cleanup"]
            once = {"default_args": {"retries": 0}}
            common = {"config": cfg["config"], "state_dir": cfg["state_dir"], "operator_kwargs": kwargs, "tags": tags}
            globals()[c["extract"]] = build_batch_dag(c["extract"], exports=cfg["exports"], load=False, **common, **once)
            held = build_batch_dag(c["paused"], exports=cfg["exports"], load=False, **common, **once)
            held.get_task(f"apply.{c['pause']['export']}").env = {
                "RIVET_TEST_PAUSE_AT": c["pause"]["at"],
                "RIVET_TEST_PAUSE_MARKER": c["pause"]["marker"],
            }
            globals()[c["paused"]] = held
            for export, load_id in c["loads"].items():
                with DAG(
                    dag_id=load_id, schedule=None, start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
                    default_args={"retries": 0}, tags=tags,
                ) as only_load:  # fmt: skip
                    RivetLoadOperator(task_id="load", export=export, config=cfg["config"], state_dir=cfg["state_dir"], **kwargs)
                globals()[load_id] = only_load
            continue
        if cfg["streams"]:
            stream = cfg["streams"][0]
            globals()[dag_id] = build_cdc_dag(
                dag_id,
                config=cfg["config"],
                export=stream["name"],
                tables=stream["tables"],
                state_dir=cfg["state_dir"],
                load=cfg["load"],
                compact=cfg["load"],
                operator_kwargs=kwargs,
                tags=tags,
            )
        else:
            globals()[dag_id] = build_batch_dag(
                dag_id,
                config=cfg["config"],
                state_dir=cfg["state_dir"],
                exports=cfg["exports"],
                load=cfg["load"],
                compact_exports=cfg["exports"] if cfg["load"] else [],
                operator_kwargs=kwargs,
                tags=tags,
            )


# The multi-host leg (gate/multihost.py, `--workers 2`): two Celery workers, each a host with its own disk at
# /opt/rivet-host. The configs are the ones `rivet init` wrote, read as the shape the gate wrote beside them, with the
# queue of each task; the gate moves the queues between the hosts, so tasks of one DAG run land on both.
MULTIHOST_SHAPE = f"{ROOT}/configs/mh_shape.json"
if os.environ.get("GATE_WORKERS", "1") != "1" and os.path.exists(MULTIHOST_SHAPE):
    from datetime import timedelta

    with open(MULTIHOST_SHAPE) as handle:
        mh = json.load(handle)
    # The retry waits long enough for the gate to move the killed task's queue to the other host.
    retry_after = {"default_args": {"retry_delay": timedelta(seconds=90)}}
    for name, p in mh["pipelines"].items():
        if p["state_conn"] is None:  # SQLite in the state_dir
            kwargs = {"env_from_connections": dict(SOURCE), "env": {"RIVET_STATE_URL": ""}}
        else:
            kwargs = {"env_from_connections": {**SOURCE, "RIVET_STATE_URL": p["state_conn"]}}
        if p["gcs"]:
            kwargs["connections"] = ["gate_gcp"]
        common = {
            "config": f"{ROOT}/configs/mh_{name}/rivet.yaml",
            "state_dir": f"/opt/rivet-host/{name}",
            "operator_kwargs": kwargs,
            "tags": ["rivet", "gate", "multihost"],
            **retry_after,
        }
        if p["kind"] == "cdc":
            dag = build_cdc_dag(f"gate_mh_{name}", export=p["stream"]["name"], tables=p["stream"]["tables"], load=False, **common)
        else:
            exports = p["exports"][:1] if p["load"] else p["exports"]
            dag = build_batch_dag(f"gate_mh_{name}", exports=exports, load=p["load"], **common)
        for task in dag.tasks:
            if task.task_id in p["queues"]:
                task.queue = p["queues"][task.task_id]
        if p["pause"]:
            # One apply pauses after its first fetch, announced by a marker the gate polls: the window in which the
            # gate kills it and sends its retry to the other host.
            dag.get_task(f"apply.{mh['paused']}").env = {
                "RIVET_TEST_PAUSE_AT": "pg_after_snapshot_open:15000",
                "RIVET_TEST_PAUSE_MARKER": f"{ROOT}/markers/mh_{mh['paused']}",
            }
        globals()[f"gate_mh_{name}"] = dag


# The migration leg (gate/migrate.py): one MySQL CDC config and one state_dir, run by rivet 0.32.1 on SQLite, then by this
# binary on the PostgreSQL state `rivet state copy` filled, then (the way back) by this binary on the SQLite file again.
if os.path.exists(f"{ROOT}/configs/mysql_migrate.json"):
    with open(f"{ROOT}/configs/mysql_migrate.json") as handle:
        mig = json.load(handle)
    on_sqlite = {"env_from_connections": {mig["url_env"]: mig["conn"]}, "env": {"RIVET_STATE_URL": ""}, "single_host": True}
    on_pg = {"env_from_connections": {mig["url_env"]: mig["conn"], "RIVET_STATE_URL": mig["state_conn"]}}
    for kind, kwargs in (("sqlite", {**on_sqlite, "rivet_bin": mig["old_bin"]}), ("pg", on_pg), ("back", on_sqlite)):
        globals()[mig["dags"][kind]] = build_cdc_dag(
            mig["dags"][kind],
            config=mig["config"],
            export=mig["stream"]["name"],
            tables=mig["stream"]["tables"],
            state_dir=mig["state_dir"],
            operator_kwargs={**kwargs, **GCP},
            tags=["rivet", "gate", "migrate"],
        )
    # The incremental batch pipeline of the same tables, migrated the same way.
    b = mig["batch"]
    on_sqlite_b = {**on_sqlite, "env_from_connections": {mig["url_env"]: mig["conn"]}}
    on_pg_b = {"env_from_connections": {mig["url_env"]: mig["conn"], "RIVET_STATE_URL": b["state_conn"]}}
    for kind, kwargs in (("sqlite", {**on_sqlite_b, "rivet_bin": mig["old_bin"]}), ("pg", on_pg_b), ("back", on_sqlite_b)):
        globals()[b["dags"][kind]] = build_batch_dag(
            b["dags"][kind],
            config=b["config"],
            state_dir=b["state_dir"],
            exports=b["exports"],
            compact_exports=b["exports"],
            operator_kwargs={**kwargs, **GCP},
            tags=["rivet", "gate", "migrate"],
        )


# The config `rivet init` writes beside a database of the same table names (sources.py `unqualified_phase`), made after
# everything else; its DAG appears once the gate writes the shape.
if os.path.exists(f"{ROOT}/configs/mysql_unq.json"):
    with open(f"{ROOT}/configs/mysql_unq.json") as handle:
        unq = json.load(handle)
    globals()[unq["dag"]] = build_cdc_dag(
        unq["dag"],
        config=unq["config"],
        export=unq["stream"]["name"],
        tables=unq["stream"]["tables"],
        state_dir=unq["state_dir"],
        operator_kwargs=leg_kwargs("mysql", "unq"),
        tags=["rivet", "gate", "source", "mysql"],
    )


# The client's README (gate/client.py, `--client-override`): the stream a cron job ran with rivet 0.32.1, moved into
# Airflow by its steps 5 and 6 — the cron config as it ran, the client's own Connections and state_dir, verbatim.
if os.path.exists(f"{ROOT}/configs/client_readme.json"):
    with open(f"{ROOT}/configs/client_readme.json") as handle:
        readme = json.load(handle)
    globals()[readme["dag"]] = build_cdc_dag(
        readme["dag"],
        config=readme["config"],
        export=readme["stream"]["name"],
        tables=readme["stream"]["tables"],
        state_dir=readme["state_dir"],
        operator_kwargs={
            "env_from_connections": {"RIVET_STATE_URL": "rivet_state", "RIVET_MYSQL_URL": "rivet_mysql"},
            "cloud_credentials": ["gcp"],
        },
        tags=["rivet", "gate", "client"],
    )
