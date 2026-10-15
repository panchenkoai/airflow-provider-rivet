"""The warehouse leg: extract to GCS, load and compact into BigQuery, Snowflake and ClickHouse, every credential from an
Airflow Connection; the warehouse is read back by a client the loader does not use and compared with the source."""

from __future__ import annotations

import json
import os
import re
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .stand import CH_PASSWORD, sh

WAREHOUSES = ("bigquery", "snowflake", "clickhouse")
SWEEP_AGE_HOURS = 24


@dataclass(frozen=True)
class Cloud:
    """Where the warehouse leg writes, and the local logins it copies into Airflow Connections."""

    keyfile: Path
    bucket: str
    project: str
    location: str
    sf_connection: str
    sf_warehouse: str
    sf_database: str
    sf_integration: str
    tag: str

    def dataset(self, stack: str) -> str:
        """The BigQuery dataset of one stack (the stand's tmp prefix, swept by age)."""
        return f"rivet_tmp_afgate_{self.tag}_{stack.replace('.', '')}"

    def schema(self, stack: str) -> str:
        """The Snowflake schema of one stack."""
        return f"AFGATE_{self.tag}_{stack.replace('.', '')}".upper()

    def prefix(self, stack: str, wh: str) -> str:
        """The GCS prefix one warehouse's exports land under."""
        return f"airflow-gate/{self.tag}/{stack.replace('.', '')}/{wh}/"


def snow_login(name: str) -> dict[str, str]:
    """Account, user, role and key file of a local `snow` connection."""
    homes = [
        os.environ.get("SNOWFLAKE_HOME"),
        str(Path.home() / "Library/Application Support/snowflake"),
        str(Path.home() / ".snowflake"),
    ]
    for home in filter(None, homes):
        path = Path(home) / "config.toml"
        if path.exists():
            conn = tomllib.loads(path.read_text()).get("connections", {}).get(name)
            if conn:
                return {k: str(v) for k, v in conn.items()}
    raise RuntimeError(
        f"no `snow` connection `{name}` in SNOWFLAKE_HOME, ~/Library/Application Support/snowflake or ~/.snowflake"
    )


def connections(cloud: Cloud) -> dict[str, str]:
    """AIRFLOW_CONN_* values for the stack: GCP (GCS + BigQuery), Snowflake, ClickHouse."""
    sf = snow_login(cloud.sf_connection)
    key = Path(sf["private_key_file"]).expanduser().read_text()
    conns = {
        "AIRFLOW_CONN_GATE_GCP": {
            "conn_type": "google_cloud_platform",
            "extra": {"keyfile_dict": cloud.keyfile.read_text(), "project": cloud.project},
        },
        "AIRFLOW_CONN_GATE_SNOWFLAKE": {
            "conn_type": "snowflake",
            "login": sf["user"],
            "extra": {
                "account": sf["account"],
                "role": sf.get("role", ""),
                "warehouse": cloud.sf_warehouse,
                "database": cloud.sf_database,
                "private_key_content": key,
            },
        },
        "AIRFLOW_CONN_GATE_CLICKHOUSE": {
            "conn_type": "clickhouse",
            "host": "gate-clickhouse",
            "port": 8123,
            "login": "loader",
            "password": CH_PASSWORD,
        },
    }
    return {name: json.dumps(value) for name, value in conns.items()}


LOADS = {
    "bigquery": "load:\n  target: bigquery\n  project: {project}\n  dataset: {dataset}\n",
    "snowflake": (
        "load:\n  target: snowflake\n  connection: gate\n  warehouse: {sf_warehouse}\n  database: {sf_database}\n"
        "  schema: {schema}\n  storage_integration: {sf_integration}\n"
    ),
    "clickhouse": (
        "load:\n  target: clickhouse\n  url: ${{RIVET_CLICKHOUSE_URL}}\n  database: gate\n  user: ${{RIVET_CLICKHOUSE_USER}}\n"
        "  password_env: RIVET_CLICKHOUSE_PASSWORD\n"
    ),
}
BATCH = """source:
  type: postgres
  url_env: RIVET_GATE_SOURCE_URL
  tls: {{ mode: disable }}
exports:
{exports}{load}"""
BATCH_EXPORT = """  - name: {wh}_{short}
    query: SELECT {cols} FROM {table}
    mode: incremental
    cursor_column: updated_at
    format: parquet
    destination: {{ type: gcs, bucket: {bucket}, prefix: {prefix}{short}/ }}
"""
CDC = """source:
  type: postgres
  url_env: RIVET_GATE_SOURCE_URL
  tls: {{ mode: disable }}
exports:
  - name: {wh}_cdc
    tables: [{tables}]
    mode: cdc
    format: parquet
    cdc:
      slot: gate_wh_{wh}
      initial: snapshot
      checkpoint: /opt/rivet/state/wh_{wh}/cdc.ckpt
      until_current: true
    destination: {{ type: gcs, bucket: {bucket}, prefix: {prefix}cdc/ }}
{load}"""


def write_configs(rivet_dir: Path, cloud: Cloud, stack: str, tables: dict[str, tuple[str, list[str]]]) -> None:
    """One batch and one CDC config per warehouse; BigQuery batch keeps a base table and a buffer that compact merges."""
    for wh in WAREHOUSES:
        (rivet_dir / "state" / f"wh_{wh}").mkdir(parents=True, exist_ok=True)
        load = LOADS[wh].format(
            project=cloud.project,
            dataset=cloud.dataset(stack),
            schema=cloud.schema(stack),
            sf_warehouse=cloud.sf_warehouse,
            sf_database=cloud.sf_database,
            sf_integration=cloud.sf_integration,
        )
        prefix = cloud.prefix(stack, wh)
        exports = "".join(
            BATCH_EXPORT.format(wh=wh, short=short, cols=", ".join(cols), table=table, bucket=cloud.bucket, prefix=prefix)
            for table, (short, cols) in tables.items()
        )
        layout = "  layout: base_buffer\n" if wh == "bigquery" else ""
        (rivet_dir / "configs" / f"wh_{wh}_batch.yaml").write_text(BATCH.format(exports=exports, load=load + layout))
        (rivet_dir / "configs" / f"wh_{wh}_cdc.yaml").write_text(
            CDC.format(wh=wh, tables=", ".join(tables), bucket=cloud.bucket, prefix=prefix, load=load)
        )


def _bq(*args: str, timeout: float = 300, check: bool = True) -> str:
    """`bq` against the gate's project."""
    return sh(["bq", "--quiet", "--headless", *args], timeout=timeout, check=check).stdout


def _snow(cloud: Cloud, sql: str, check: bool = True) -> list[dict[str, Any]]:
    """SQL through the local `snow` connection (setup and cleanup only; never an oracle read)."""
    out = sh(["snow", "sql", "-c", cloud.sf_connection, "--format", "json", "-q", sql], timeout=300, check=check).stdout
    try:
        doc = json.loads(out or "[]")
    except json.JSONDecodeError:
        return []
    return doc if isinstance(doc, list) else []


def prepare(cloud: Cloud, stack: str) -> None:
    """The stack's BigQuery dataset and Snowflake schema."""
    _bq(f"--location={cloud.location}", "mk", "-f", "--dataset", f"{cloud.project}:{cloud.dataset(stack)}")
    _snow(cloud, f"CREATE SCHEMA IF NOT EXISTS {cloud.sf_database}.{cloud.schema(stack)}")


def cleanup(cloud: Cloud, stack: str) -> None:
    """Remove what one stack wrote: the dataset, the schema, the GCS prefix."""
    _bq("rm", "-r", "-f", "--dataset", f"{cloud.project}:{cloud.dataset(stack)}", check=False)
    _snow(cloud, f"DROP SCHEMA IF EXISTS {cloud.sf_database}.{cloud.schema(stack)}", check=False)
    sh(
        ["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{cloud.bucket}/airflow-gate/{cloud.tag}/{stack.replace('.', '')}/"],
        timeout=600,
        check=False,
    )


def sweep(cloud: Cloud) -> list[str]:
    """Remove leftovers of earlier gate runs older than a day, by age only (one bucket and project serve several machines)."""
    removed = []
    # `bq ls <project>` reads the argument as a dataset; the project goes in `--project_id`.
    listing = _bq("ls", "--format=json", "--max_results=10000", f"--project_id={cloud.project}")
    for ds in json.loads(listing or "[]"):
        name = ds.get("datasetReference", {}).get("datasetId", "")
        if not name.startswith("rivet_tmp_afgate_"):
            continue
        shown = json.loads(_bq("show", "--format=json", f"{cloud.project}:{name}", check=False) or "{}")
        age_h = (_now_ms() - int(shown.get("creationTime", _now_ms()))) / 3.6e6
        if age_h > SWEEP_AGE_HOURS:
            _bq("rm", "-r", "-f", "--dataset", f"{cloud.project}:{name}", check=False)
            removed.append(f"bq:{name}")
    for row in _snow(cloud, f"SHOW SCHEMAS LIKE 'AFGATE\\_%' IN DATABASE {cloud.sf_database}", check=False):
        if _age_hours(row.get("created_on")) > SWEEP_AGE_HOURS:
            _snow(cloud, f"DROP SCHEMA IF EXISTS {cloud.sf_database}.{row['name']}", check=False)
            removed.append(f"snowflake:{row['name']}")
    objects = sh(["gcloud", "storage", "ls", "-l", f"gs://{cloud.bucket}/airflow-gate/**"], timeout=600, check=False).stdout
    old = [
        line.split()[-1]
        for line in objects.splitlines()
        if re.match(r"\s*\d+\s+\d{4}-", line) and _age_hours(line.split()[1]) > SWEEP_AGE_HOURS
    ]
    if old:
        sh(["gcloud", "storage", "rm", "--quiet", *old], timeout=600, check=False)
        removed.append(f"gcs:{len(old)} object(s)")
    return removed


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


def _age_hours(stamp: Any) -> float:
    """Hours since a timestamp text; 0 when it cannot be read (never swept on a guess)."""
    from datetime import UTC, datetime

    try:
        when = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    when = when if when.tzinfo else when.replace(tzinfo=UTC)
    return (datetime.now(UTC) - when).total_seconds() / 3600


def dags() -> list[str]:
    """The warehouse DAG ids."""
    return [f"gate_wh_{wh}_{kind}" for wh in WAREHOUSES for kind in ("batch", "cdc")]


def rivet_tasks(tables: Sequence[str], shorts: Sequence[str]) -> dict[str, list[str]]:
    """The rivet tasks of each warehouse DAG (the compact tasks of Snowflake / ClickHouse may be skipped)."""
    out = {}
    for wh in WAREHOUSES:
        out[f"gate_wh_{wh}_batch"] = [f"{g}.{wh}_{s}" for s in shorts for g in ("apply", "load", "compact")]
        out[f"gate_wh_{wh}_cdc"] = ["run", *(f"{g}.{t}" for t in tables for g in ("load", "compact"))]
    return out


class Leg:
    """The warehouse leg of one stack: its configs and cloud objects, its DAGs, and its oracles."""

    may_skip = ("compact.",)

    def __init__(self, cloud: Cloud, stack: str, tables: dict[str, tuple[str, list[str]]]) -> None:
        self.cloud = cloud
        self.stack = stack
        self.tables = tables
        self.dags = dags()
        self.tasks = rivet_tasks(list(tables), [short for short, _ in tables.values()])

    def prepare(self, stand: Any) -> None:
        """Configs in the stand, the dataset and the schema in the clouds."""
        write_configs(stand.rivet_dir, self.cloud, self.stack, self.tables)
        prepare(self.cloud, self.stack)

    def cleanup(self) -> None:
        cleanup(self.cloud, self.stack)

    def inventory(self, stand: Any) -> dict[str, list[str]]:
        """The objects each warehouse holds after a cycle."""
        bq = _bq(
            "ls", "--format=json", "--max_results=1000", f"{self.cloud.project}:{self.cloud.dataset(self.stack)}", check=False
        )
        sf = _snow(self.cloud, f"SHOW OBJECTS IN SCHEMA {self.cloud.sf_database}.{self.cloud.schema(self.stack)}", check=False)
        ch = sh(
            [
                "curl",
                "-s",
                "-u",
                f"loader:{CH_PASSWORD}",
                f"http://127.0.0.1:{stand.clickhouse_port}/",
                "--data-binary",
                "SELECT name || ' ' || engine FROM system.tables WHERE database = 'gate' FORMAT TSV",
            ],
            timeout=60,
            check=False,
        ).stdout
        return {
            "bigquery": sorted(f"{t['tableReference']['tableId']} {t.get('type')}" for t in json.loads(bq or "[]")),
            "snowflake": sorted(f"{r.get('name')} {r.get('kind')}" for r in sf),
            "clickhouse": sorted(ch.split("\n")) if ch else [],
        }

    def grade(self, stand: Any, v: Any, name: str, deleted: int) -> None:
        """Warehouse oracles for one cycle."""
        print(f"INVENTORY {name} {json.dumps(self.inventory(stand))}", flush=True)
