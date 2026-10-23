"""Example: one MySQL CDC stream per source database, every DAG built from that source's own rivet config.

The layout of docs/cron-to-airflow-cdc.md: for a source `<src>`, the config `<configs>/<src>.yaml` (one export with
`mode: cdc`), the state directory `<state root>/<src>`, the state database behind the Connection `rivet_state_<src>`,
the MySQL Connection `rivet_<src>`, and the DAG `rivet_<src>_cdc`. The export name and its tables are read from the
config, so the DAG captures what the config captures.

Set before the scheduler parses this file (the defaults are the recipe's paths in the official docker-compose image):
  RIVET_EXAMPLE_SOURCES      comma-separated source names, e.g. `shop,billing`
  RIVET_EXAMPLE_CONFIG_DIR   where `<src>.yaml` live (default /opt/airflow/dags/rivet)
  RIVET_EXAMPLE_STATE_ROOT   the parent of every state directory (default /opt/airflow/rivet-state)
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import yaml

from airflow_provider_rivet.dags import build_cdc_dag

SOURCES = [s for s in os.environ.get("RIVET_EXAMPLE_SOURCES", "").split(",") if s]
CONFIG_DIR = Path(os.environ.get("RIVET_EXAMPLE_CONFIG_DIR", "/opt/airflow/dags/rivet"))
STATE_ROOT = Path(os.environ.get("RIVET_EXAMPLE_STATE_ROOT", "/opt/airflow/rivet-state"))


def cdc_export(config: Path) -> dict:
    """The one `mode: cdc` export of a rivet config: its `name` and `tables`."""
    exports = [e for e in yaml.safe_load(config.read_text())["exports"] if e.get("mode") == "cdc"]
    if len(exports) != 1:
        raise ValueError(f"{config}: expected one export with `mode: cdc`, found {len(exports)}")
    return exports[0]


for src in SOURCES:
    config = CONFIG_DIR / f"{src}.yaml"
    export = cdc_export(config)
    globals()[f"rivet_{src}_cdc"] = build_cdc_dag(
        f"rivet_{src}_cdc",
        config=str(config),
        export=export["name"],
        tables=export["tables"],
        state_dir=str(STATE_ROOT / src),
        schedule="0 */2 * * *",
        start_date=datetime(2026, 10, 1),  # in the past: a future start_date gives runs with no tasks
        catchup=False,
        operator_kwargs={
            "env_from_connections": {"RIVET_STATE_URL": f"rivet_state_{src}", "RIVET_MYSQL_URL": f"rivet_{src}"},
            "cloud_credentials": ["gcp"],
        },
    )
