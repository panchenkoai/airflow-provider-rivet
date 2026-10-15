"""Gate DAGs: incremental batch and CDC, each on one shared SQLite state and on a dedicated PostgreSQL state."""

from __future__ import annotations

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
