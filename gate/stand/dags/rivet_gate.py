"""Gate DAGs: incremental batch and CDC, each on one shared SQLite state and on a dedicated PostgreSQL state."""

from __future__ import annotations

import json
import os

from airflow_provider_rivet.dags import build_batch_dag, build_cdc_dag

ROOT = "/opt/rivet"
SOURCE = {"RIVET_GATE_SOURCE_URL": "rivet_gate_source"}
PAUSE = {"RIVET_TEST_PAUSE_AT": "cdc_after_open:20000", "RIVET_TEST_PAUSE_MARKER": f"{ROOT}/markers/cdc_sqlite"}


def operator_kwargs(state: str) -> dict:
    """SQLite state lives in the shared state_dir (the SQLite CDC run holds a pause window); PostgreSQL state comes from a connection."""
    if state == "pg":
        return {"cwd": f"{ROOT}/out", "env_from_connections": {**SOURCE, "RIVET_STATE_URL": "rivet_gate_state"}}
    return {"cwd": f"{ROOT}/out", "env_from_connections": dict(SOURCE), "env": {"RIVET_STATE_URL": ""}}


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
        "connections": WAREHOUSE_CONNECTIONS[wh],
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
    return {
        "env_from_connections": {
            f"RIVET_GATE_{engine.upper()}_URL": f"rivet_gate_{engine}",
            "RIVET_STATE_URL": f"rivet_gate_state_{engine}_{kind}",
        },
        "connections": ["gate_gcp"],
    }


# Source legs (gate/sources.py): another engine to GCS and BigQuery, on the configs `rivet init` wrote; the export and
# stream names are init's, read from the shape the gate wrote beside them (no DAG until it exists).
for engine in filter(None, os.environ.get("GATE_SOURCES", "").split(",")):
    shape_file = f"{ROOT}/configs/{engine}_shape.json"
    if not os.path.exists(shape_file):
        continue
    with open(shape_file) as handle:
        shape = json.load(handle)

    batch = shape["batch"]["exports"]
    globals()[f"gate_src_{engine}_batch"] = build_batch_dag(
        f"gate_src_{engine}_batch",
        config=f"{ROOT}/configs/{engine}_batch/rivet.yaml",
        state_dir=f"{ROOT}/state/{engine}_batch",
        exports=batch,
        compact_exports=batch,
        operator_kwargs=leg_kwargs(engine, "batch"),
        tags=["rivet", "gate", "source", engine],
    )
    # One DAG per CDC stream: one over every table, or one per table where the engine captures per table (SQL Server).
    for stream in shape["cdc"]["streams"]:
        globals()[f"gate_src_{engine}{stream['dag']}"] = build_cdc_dag(
            f"gate_src_{engine}{stream['dag']}",
            config=f"{ROOT}/configs/{engine}_cdc/rivet.yaml",
            export=stream["name"],
            tables=stream["tables"],
            state_dir=f"{ROOT}/state/{engine}_cdc",
            operator_kwargs=leg_kwargs(engine, "cdc"),
            tags=["rivet", "gate", "source", engine],
        )
