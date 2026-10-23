"""`--source-dags` (gate/source_dags.py): one DAG per named database, as build_source_dags makes it.

Each source leg writes its calls to `configs/source_dags_<engine>.json` (names, the mode, `init_args` with `{name}`);
this file passes them to build_source_dags as they are, with the stack's PostgreSQL state server as `state_connection`.
The sources are names only: an Airflow Connection of that name, else the data source of /etc/odbc.ini. Until a
source's config exists its DAG is the one `init` task; the gate unpauses it (`@once` runs it) and waits for the DAG
the next parse builds from what init wrote.
"""

from __future__ import annotations

import glob
import json
from datetime import datetime

from airflow_provider_rivet.dags import build_source_dags

for spec_file in sorted(glob.glob("/opt/rivet/configs/source_dags_*.json")):
    with open(spec_file) as handle:
        spec = json.load(handle)
    for call in spec["calls"]:
        globals().update(
            build_source_dags(
                call["sources"],
                mode=call["mode"],
                config_dir=spec["config_dir"],
                state_root=spec["state_root"],
                state_connection=spec["state_connection"],
                init_args=call["init_args"],
                operator_kwargs=spec["operator_kwargs"],
                schedule="@once",
                start_date=datetime(2026, 1, 1),
                catchup=False,
                tags=["rivet", "gate", "source-dags", spec["engine"]],
            )
        )
