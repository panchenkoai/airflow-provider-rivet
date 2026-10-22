"""The warehouse leg: extract to GCS, load and compact into BigQuery, Snowflake and ClickHouse, every credential from an
Airflow Connection; the warehouse is read back by a client the loader does not use and compared with the source."""

from __future__ import annotations

import base64
import json
import os
import re
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import oracle
from .stand import sh

WAREHOUSES = ("bigquery", "snowflake", "clickhouse")
SWEEP_AGE_HOURS = 24
#: A run tag (`%m%d%H%M` + 4 hex digits) and the names the gate builds from it; the sweep drops nothing else.
TAG_SHAPE = re.compile(r"\d{8}[0-9a-f]{4}")
SCHEMA_SHAPE = re.compile(r"AFGATE_\d{8}[0-9A-F]{4}_\d+")
DATASET_SHAPE = re.compile(r"rivet_tmp_afgate_\d{8}[0-9a-f]{4}_\d+(_[a-z]+_(batch|cdc))?")
#: A Secret Manager secret holding one gate Connection: `<connections_prefix>_<conn_id>`.
SECRET_SHAPE = re.compile(r"airflow-connections-afgate_\d{8}[0-9a-f]{4}_\d+_[a-z0-9_]+")
SECRETS_BACKEND = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
#: The one DAG whose compaction must run: its base_buffer layout keeps the current state in the base only after it.
MUST_COMPACT = "gate_wh_bigquery_batch"
#: The cycles whose batch load buffers changes over an existing base. The first load creates the base from its pass
#: (rivet #652: only an absent name is created, so its compaction has nothing to merge) and the idle load buffers
#: nothing; a compaction that skips there is right, and the `compacted` cell still holds every cycle to "no buffer left".
MUST_COMPACT_CYCLES = ("delta",)


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
    #: `key`: the gate_gcp Connection carries the credential (`keyfile_dict`); `adc`: it holds only `project` and
    #: rivet runs as the identity the containers were given (the credential file mounted read-only).
    gcp_auth: str = "key"
    #: `env`: AIRFLOW_CONN_* variables; `gsm`: Google Secret Manager as Airflow's secrets backend.
    connections_from: str = "env"
    #: The service account the secrets backend reads as (impersonated from the mounted credential); empty: as itself.
    gsm_reader: str = ""

    def dataset(self, stack: str) -> str:
        """The BigQuery dataset of one stack (the stand's tmp prefix, swept by age)."""
        return f"rivet_tmp_afgate_{self.tag}_{stack.replace('.', '')}"

    def schema(self, stack: str) -> str:
        """The Snowflake schema of one stack."""
        return f"AFGATE_{self.tag}_{stack.replace('.', '')}".upper()

    def root(self, stack: str) -> str:
        """The GCS prefix every export of one stack lands under."""
        return f"airflow-gate/{self.tag}/{stack.replace('.', '')}/"

    def prefix(self, stack: str, wh: str) -> str:
        """The GCS prefix one warehouse's exports land under."""
        return f"{self.root(stack)}{wh}/"

    def connections_prefix(self, stack: str) -> str:
        """The secrets backend's `connections_prefix` for one stack; with `sep` `_` a conn_id is the rest of the name."""
        return f"airflow-connections-afgate_{self.tag}_{stack.replace('.', '')}"

    def secret(self, stack: str, conn_id: str) -> str:
        """The Secret Manager secret that holds one Connection of one stack (what the backend asks for)."""
        return f"{self.connections_prefix(stack)}_{conn_id}"

    def secrets_backend(self, stack: str) -> dict[str, str]:
        """The `[secrets]` settings of a stack whose Connections live in Secret Manager. Variables and config are
        not looked up there (prefix null), so the backend only ever asks for the gate's own Connection secrets."""
        kwargs: dict[str, Any] = {
            "connections_prefix": self.connections_prefix(stack),
            "sep": "_",
            "variables_prefix": None,
            "config_prefix": None,
            "project_id": self.project,
        }
        if self.gsm_reader:
            kwargs["impersonation_chain"] = self.gsm_reader
        return {"AIRFLOW__SECRETS__BACKEND": SECRETS_BACKEND, "AIRFLOW__SECRETS__BACKEND_KWARGS": json.dumps(kwargs)}


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


def gcp_connection(cloud: Cloud) -> dict[str, Any]:
    """The gate_gcp Connection (GCS + BigQuery): the project, and the credential unless rivet runs as the containers'."""
    gcp: dict[str, Any] = {"project": cloud.project}
    if cloud.gcp_auth == "key":
        gcp["keyfile_dict"] = cloud.keyfile.read_text()
    return {"conn_type": "google_cloud_platform", "extra": gcp}


def connections(cloud: Cloud, clickhouse_password: str) -> dict[str, dict[str, Any]]:
    """The warehouse leg's Connections by conn_id: GCP (GCS + BigQuery), Snowflake, ClickHouse."""
    sf = snow_login(cloud.sf_connection)
    key = Path(sf["private_key_file"]).expanduser().read_text()
    # `env-file` (the client's): no GCP Connection at all; rivet reads GOOGLE_APPLICATION_CREDENTIALS from the env.
    gcp = {} if cloud.gcp_auth == "env-file" else {"gate_gcp": gcp_connection(cloud)}
    return {
        **gcp,
        "gate_snowflake": {
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
        "gate_clickhouse": {
            "conn_type": "clickhouse",
            "host": "gate-clickhouse",
            "port": 8123,
            "login": "loader",
            "password": clickhouse_password,
        },
    }


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
        # A `query:` export records no primary key: the current-state view (and BigQuery's merge) needs it declared.
        layout = "  pk: [id]\n" + ("  layout: base_buffer\n" if wh == "bigquery" else "")
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


def _schema_exists(cloud: Cloud, stack: str) -> bool:
    """Whether the stack's Snowflake schema exists (`LIKE` ignores case and `_`, so the name is compared exactly)."""
    rows = _snow(cloud, f"SHOW SCHEMAS LIKE '{cloud.schema(stack)}' IN DATABASE {cloud.sf_database}")
    return any(r.get("name") == cloud.schema(stack) for r in rows)


def prepare(cloud: Cloud, stack: str, warehouses: Sequence[str] = WAREHOUSES) -> None:
    """The stack's BigQuery dataset and Snowflake schema (a schema an administrator made for the run is kept)."""
    _bq(f"--location={cloud.location}", "mk", "-f", "--dataset", f"{cloud.project}:{cloud.dataset(stack)}")
    if "snowflake" in warehouses and not _schema_exists(cloud, stack):
        _snow(cloud, f'CREATE SCHEMA IF NOT EXISTS {cloud.sf_database}."{cloud.schema(stack)}"')


def leftovers(cloud: Cloud, stack: str, warehouses: Sequence[str] = WAREHOUSES) -> list[str]:
    """What of one stack's dataset, schema and GCS prefix is still there; a listing that cannot answer raises."""
    left = []
    ds = f"{cloud.project}:{cloud.dataset(stack)}"
    shown = sh(["bq", "--quiet", "--headless", "show", "--format=json", ds], timeout=300, check=False)
    if shown.returncode == 0:
        left.append(f"bq:{ds}")
    elif "Not found" not in (shown.stdout + shown.stderr):
        raise RuntimeError(f"cannot tell whether {ds} is gone: {(shown.stderr or shown.stdout).strip()[-500:]}")
    if "snowflake" in warehouses and _schema_exists(cloud, stack):
        left.append(f"snowflake:{cloud.sf_database}.{cloud.schema(stack)}")
    prefix = f"gs://{cloud.bucket}/{cloud.root(stack)}"
    listed = sh(["gcloud", "storage", "ls", "-r", prefix], timeout=600, check=False)
    if listed.returncode == 0 and listed.stdout.strip():
        left.append(f"gcs:{prefix} ({len(listed.stdout.split())} object(s))")
    elif listed.returncode != 0 and "matched no objects" not in (listed.stdout + listed.stderr):
        raise RuntimeError(f"cannot tell whether {prefix} is empty: {(listed.stderr or listed.stdout).strip()[-500:]}")
    return left


def cleanup(cloud: Cloud, stack: str, warehouses: Sequence[str] = WAREHOUSES) -> None:
    """Remove what one stack wrote (the dataset, the schema, the GCS prefix), then list again: anything left raises."""
    _bq("rm", "-r", "-f", "--dataset", f"{cloud.project}:{cloud.dataset(stack)}", check=False)
    if "snowflake" in warehouses:
        _snow(cloud, f'DROP SCHEMA IF EXISTS {cloud.sf_database}."{cloud.schema(stack)}"', check=False)
    sh(["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{cloud.bucket}/{cloud.root(stack)}"], timeout=600, check=False)
    left = leftovers(cloud, stack, warehouses)
    if left:
        raise RuntimeError(f"cloud cleanup left behind {left}")


def _secrets(cloud: Cloud) -> list[dict[str, Any]]:
    """Every Secret Manager secret of the gate's shape in the project: `id`, `created`; a listing that fails raises."""
    out = sh(
        ["gcloud", "secrets", "list", f"--project={cloud.project}", "--format=json(name,createTime)"],
        timeout=300,
        check=True,
    ).stdout
    found = [{"id": str(r.get("name", "")).rsplit("/", 1)[-1], "created": r.get("createTime")} for r in json.loads(out or "[]")]
    return [r for r in found if SECRET_SHAPE.fullmatch(r["id"])]


def publish_connections(cloud: Cloud, stack: str, conns: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """One secret per Connection (Airflow's JSON form) under the stack's prefix; the secret ids. The value goes in
    on stdin, never on a command line."""
    made = []
    for conn_id, conn in conns.items():
        name = cloud.secret(stack, conn_id)
        if not SECRET_SHAPE.fullmatch(name):
            raise ValueError(f"{name} is not of the shape the sweep recognises")
        sh(
            [
                "gcloud", "secrets", "create", name, f"--project={cloud.project}", "--replication-policy=automatic",
                f"--labels=afgate-tag={cloud.tag}", "--data-file=-",
            ],
            input=json.dumps(conn),
            timeout=300,
        )  # fmt: skip
        made.append(name)
    return made


def secret_leftovers(cloud: Cloud, stack: str) -> list[str]:
    """The stack's Connection secrets still in Secret Manager."""
    mine = cloud.connections_prefix(stack) + "_"
    return [f"gsm:{r['id']}" for r in _secrets(cloud) if r["id"].startswith(mine)]


def delete_connections(cloud: Cloud, stack: str) -> None:
    """Delete the stack's Connection secrets, then list again: anything left raises."""
    for left in secret_leftovers(cloud, stack):
        sh(["gcloud", "secrets", "delete", left.removeprefix("gsm:"), f"--project={cloud.project}", "--quiet"], check=False)
    left = secret_leftovers(cloud, stack)
    if left:
        raise RuntimeError(f"secret cleanup left behind {left}")


def sweep(cloud: Cloud) -> list[str]:
    """Remove leftovers of earlier gate runs older than a day, by age only (one bucket and project serve several machines)."""
    removed = []
    # `bq ls <project>` reads the argument as a dataset; the project goes in `--project_id`.
    listing = _bq("ls", "--format=json", "--max_results=10000", f"--project_id={cloud.project}")
    for ds in json.loads(listing or "[]"):
        name = ds.get("datasetReference", {}).get("datasetId", "")
        if not DATASET_SHAPE.fullmatch(name):
            continue
        shown = json.loads(_bq("show", "--format=json", f"{cloud.project}:{name}", check=False) or "{}")
        age_h = (_now_ms() - int(shown.get("creationTime", _now_ms()))) / 3.6e6
        if age_h > SWEEP_AGE_HOURS:
            _bq("rm", "-r", "-f", "--dataset", f"{cloud.project}:{name}", check=False)
            removed.append(f"bq:{name}")
    # `LIKE` ignores case and reads `_` as any character; the gate's own shape is decided here, exactly.
    for row in _snow(cloud, f"SHOW SCHEMAS LIKE 'AFGATE%' IN DATABASE {cloud.sf_database}", check=False):
        name = str(row.get("name", ""))
        if SCHEMA_SHAPE.fullmatch(name) and _age_hours(row.get("created_on")) > SWEEP_AGE_HOURS:
            _snow(cloud, f'DROP SCHEMA IF EXISTS {cloud.sf_database}."{name}"', check=False)
            removed.append(f"snowflake:{name}")
    objects = sh(["gcloud", "storage", "ls", "-l", f"gs://{cloud.bucket}/airflow-gate/**"], timeout=600, check=False).stdout
    old = [
        line.split()[-1]
        for line in objects.splitlines()
        if re.match(r"\s*\d+\s+\d{4}-", line) and _age_hours(line.split()[1]) > SWEEP_AGE_HOURS
    ]
    if old:
        sh(["gcloud", "storage", "rm", "--quiet", *old], timeout=600, check=False)
        removed.append(f"gcs:{len(old)} object(s)")
    try:
        stale = [r["id"] for r in _secrets(cloud) if _age_hours(r["created"]) > SWEEP_AGE_HOURS]
    except RuntimeError as exc:
        stale = []
        if "SERVICE_DISABLED" not in str(exc) and "has not been used" not in str(exc):
            removed.append(f"gsm:listing failed, nothing swept: {exc}")  # a disabled API holds nothing of the gate's
    for name in stale:
        sh(["gcloud", "secrets", "delete", name, f"--project={cloud.project}", "--quiet"], check=False)
        removed.append(f"gsm:{name}")
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


def dags(warehouses: Sequence[str] = WAREHOUSES) -> list[str]:
    """The warehouse DAG ids."""
    return [f"gate_wh_{wh}_{kind}" for wh in warehouses for kind in ("batch", "cdc")]


def rivet_tasks(tables: Sequence[str], shorts: Sequence[str], warehouses: Sequence[str] = WAREHOUSES) -> dict[str, list[str]]:
    """The rivet tasks of each warehouse DAG."""
    out = {}
    for wh in warehouses:
        out[f"gate_wh_{wh}_batch"] = [f"{g}.{wh}_{s}" for s in shorts for g in ("apply", "load", "compact")]
        out[f"gate_wh_{wh}_cdc"] = ["run", *(f"{g}.{t}" for t in tables for g in ("load", "compact"))]
    return out


def may_skip(dag_ids: Sequence[str], cycle: str = "delta") -> dict[str, tuple[str, ...]]:
    """Task prefixes each DAG may skip in `cycle`: a compaction that has nothing to merge (Snowflake and ClickHouse keep a
    change log behind a view, BigQuery CDC has no base) skips, except in the BigQuery batch DAG on a cycle whose load
    buffered changes over its base (`MUST_COMPACT_CYCLES`): base_buffer holds the current state in the base only once
    `compact` merged the buffer into it."""
    strict = cycle in MUST_COMPACT_CYCLES
    return {dag: () if strict and dag == MUST_COMPACT else ("compact.",) for dag in dag_ids}


# --- The second-client read-back: canonical values, compared with the source --------------------------------------
#
# Each warehouse is read by a client the loader does not use (google-cloud-bigquery, snowflake-connector-python, the
# ClickHouse HTTP interface as JSON), each hands back its own Python shapes, and every value is brought to one form
# before the comparison: an integer, integer cents for an exact decimal (a sub-cent value stays a Decimal, never
# rounded away), epoch microseconds for an instant (a naive one is UTC), a UUID in its text form, bytes as hex, text
# as text.

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
DELETED_FLAG = "__is_deleted"


def kinds_of(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """The kind of each column, from the first non-null source value."""
    kinds: dict[str, str] = {}
    for row in rows:
        for col, value in row.items():
            if col in kinds or value is None:
                continue
            if isinstance(value, bool) or isinstance(value, int):
                kinds[col] = "int"
            elif isinstance(value, Decimal | float):
                kinds[col] = "money"
            elif isinstance(value, datetime):
                kinds[col] = "ts"
            elif isinstance(value, uuid.UUID):
                kinds[col] = "uuid"
            elif isinstance(value, bytes | bytearray | memoryview):
                kinds[col] = "bytes"
            else:
                kinds[col] = "text"
    return kinds


def _instant(value: Any) -> datetime:
    if isinstance(value, datetime):
        stamp = value
    else:
        text = str(value).strip().replace(" ", "T", 1).replace("Z", "+00:00")
        stamp = datetime.fromisoformat(text)
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=UTC)


def canon(kind: str | None, value: Any) -> Any:
    """One value in the form every client's value of the same source value takes."""
    if value is None:
        return None
    if kind == "int":
        d = Decimal(str(value))
        return int(d) if d == d.to_integral_value() else d
    if kind == "money":
        cents = Decimal(str(value)) * 100
        return int(cents) if cents == cents.to_integral_value() else cents
    if kind == "ts":
        delta = _instant(value) - EPOCH
        return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    if kind == "text":
        return str(value)
    if kind == "uuid":
        # 16 bytes in RFC 4122 order (a Parquet UUID read back as BYTES), or any spelling of the text form.
        return str(
            uuid.UUID(bytes=bytes(value)) if isinstance(value, bytes | bytearray) and len(value) == 16 else uuid.UUID(str(value))
        )
    if kind == "bytes":
        return bytes(value).hex() if isinstance(value, bytes | bytearray | memoryview) else str(value)
    return value


def canon_row(row: Mapping[str, Any], cols: Sequence[str], kinds: Mapping[str, str]) -> tuple:
    """A row as a tuple of canonical values in `cols` order; a column the row lacks raises."""
    missing = [c for c in cols if c not in row]
    if missing:
        raise KeyError(f"no column(s) {missing} in the read-back (has {sorted(row)})")
    return tuple(canon(kinds.get(c), row[c]) for c in cols)


def _deleted(value: Any) -> bool:
    return str(value).strip().lower() in ("true", "1", "t")


def current_state(
    got: Sequence[Mapping[str, Any]],
    source: Sequence[Mapping[str, Any]],
    cols: Sequence[str],
    key: str,
    deleted: int,
    what: str,
    *,
    flagged: bool = False,
) -> list[str]:
    """The warehouse's current state equals the source, value by value.

    `flagged` (CDC): rows whose `__is_deleted` is set are dropped first, and nothing else may be extra. Otherwise
    (incremental batch, which cannot see a delete) exactly `deleted` keys absent at the source may remain.
    """
    rows = [{k.lower(): v for k, v in r.items()} for r in got]
    if flagged:
        if rows and DELETED_FLAG not in rows[0]:
            return [f"{what}: no {DELETED_FLAG} column in the read-back (has {sorted(rows[0])})"]
        rows = [r for r in rows if not _deleted(r.get(DELETED_FLAG))]
    kinds = kinds_of(source)
    want = Counter(canon_row(r, cols, kinds) for r in source)
    keys = {canon(kinds.get(key), r[key]) for r in source}
    have = [canon_row(r, cols, kinds) for r in rows]
    at = list(cols).index(key)
    live = Counter(t for t in have if t[at] in keys)
    gone = {t[at] for t in have if t[at] not in keys}
    problems = []
    for label, a, b in (("missing", want, live), ("unexpected", live, want)):
        diff = a - b
        if diff:
            shown = sorted(diff.elements(), key=repr)[: oracle.SHOWN]
            problems.append(f"{what}: {sum(diff.values())} {label} row(s), e.g. {shown}")
    allowed = 0 if flagged else deleted
    if len(gone) != allowed:
        problems.append(
            f"{what}: {len(gone)} key(s) absent at the source {sorted(gone, key=repr)[: oracle.SHOWN]}, expected {allowed}"
        )
    return problems


def compacted(objects: set[str], table: str, what: str) -> list[str]:
    """After `compact`, a base_buffer table has its base and no buffer left."""
    names = {o.lower() for o in objects}
    table = table.lower()  # BigQuery lists Oracle's GATE_ORDERS as it was created; the objects are compared lower-case
    problems = [] if table in names else [f"{what}: no base table {table}"]
    if f"{table}__changes" in names:
        problems.append(f"{what}: the buffer {table}__changes is still there (the compaction was skipped or did not merge)")
    return problems


def _bq_client(cloud: Cloud) -> Any:
    import google.auth
    from google.cloud import bigquery

    creds, _ = google.auth.load_credentials_from_file(
        str(cloud.keyfile), scopes=["https://www.googleapis.com/auth/cloud-platform"], quota_project_id=cloud.project
    )
    return bigquery.Client(project=cloud.project, credentials=creds, location=cloud.location)


def _sf_connect(cloud: Cloud) -> Any:
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    login = snow_login(cloud.sf_connection)
    key = serialization.load_pem_private_key(Path(login["private_key_file"]).expanduser().read_bytes(), password=None)
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    return snowflake.connector.connect(
        account=login["account"],
        user=login["user"],
        role=login.get("role") or None,
        private_key=der,
        warehouse=cloud.sf_warehouse,
        database=cloud.sf_database,
        session_parameters={"TIMEZONE": "UTC"},
    )


def _ch(port: int, password: str, sql: str) -> list[dict[str, Any]]:
    """Rows of a ClickHouse query over HTTP as JSON: ISO instants, unquoted 64-bit integers and decimals."""
    params = {
        "query": sql + " FORMAT JSONEachRow",
        "date_time_output_format": "iso",
        "output_format_json_quote_64bit_integers": "0",
        "output_format_json_quote_decimals": "0",
    }
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/?{urllib.parse.urlencode(params)}",
        headers={"Authorization": "Basic " + base64.b64encode(f"loader:{password}".encode()).decode()},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            body = r.read().decode()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"clickhouse {exc.code}: {exc.read().decode(errors='replace').strip()[:500]}") from None
    return [json.loads(line, parse_float=Decimal) for line in body.splitlines() if line.strip()]


class Leg:
    """The warehouse leg of one stack: its configs and cloud objects, its DAGs, and its oracles."""

    def __init__(
        self, cloud: Cloud, stack: str, tables: dict[str, tuple[str, list[str]]], warehouses: Sequence[str] = WAREHOUSES
    ) -> None:
        self.cloud = cloud
        self.stack = stack
        self.tables = tables
        self.warehouses = tuple(warehouses)
        self.dags = dags(self.warehouses)
        self.tasks = rivet_tasks(list(tables), [short for short, _ in tables.values()], self.warehouses)
        self.may_skip = may_skip(self.dags)
        self._clients: dict[str, Any] = {}

    def prepare(self, stand: Any) -> None:
        """Configs in the stand, the dataset and the schema in the clouds."""
        write_configs(stand.rivet_dir, self.cloud, self.stack, self.tables)
        prepare(self.cloud, self.stack, self.warehouses)

    def cleanup(self) -> None:
        sf = self._clients.pop("snowflake", None)
        if sf is not None:
            sf.close()
        cleanup(self.cloud, self.stack, self.warehouses)

    def inventory(self, stand: Any) -> dict[str, list[str]]:
        """The objects each warehouse holds after a cycle."""
        return {wh: sorted(self.objects(stand, wh)) for wh in self.warehouses}

    def _client(self, wh: str) -> Any:
        if wh not in self._clients:
            self._clients[wh] = _bq_client(self.cloud) if wh == "bigquery" else _sf_connect(self.cloud)
        return self._clients[wh]

    def objects(self, stand: Any, wh: str) -> set[str]:
        """The names (lower case) of the tables and views the warehouse holds for this stack."""
        if wh == "bigquery":
            ds = f"{self.cloud.project}.{self.cloud.dataset(self.stack)}"
            return {t.table_id.lower() for t in self._client(wh).list_tables(ds)}
        if wh == "snowflake":
            cur = self._client(wh).cursor()
            try:
                cur.execute(f'SHOW OBJECTS IN SCHEMA {self.cloud.sf_database}."{self.cloud.schema(self.stack)}"')
                at = [d[0].lower() for d in cur.description].index("name")
                return {row[at].lower() for row in cur.fetchall()}
            finally:
                cur.close()
        return {
            r["name"].lower()
            for r in _ch(
                stand.clickhouse_port, stand.secrets.clickhouse, "SELECT name FROM system.tables WHERE database = 'gate'"
            )
        }

    def read(self, stand: Any, wh: str, obj: str) -> list[dict[str, Any]]:
        """Every row of one table or view, read by the warehouse's own client (not the one the loader uses)."""
        if wh == "bigquery":
            sql = f"SELECT * FROM `{self.cloud.project}.{self.cloud.dataset(self.stack)}.{obj}`"
            return [dict(row.items()) for row in self._client(wh).query(sql).result()]
        if wh == "snowflake":
            cur = self._client(wh).cursor()
            try:
                cur.execute(f'SHOW OBJECTS IN SCHEMA {self.cloud.sf_database}."{self.cloud.schema(self.stack)}"')
                at = [d[0].lower() for d in cur.description].index("name")
                exact = [row[at] for row in cur.fetchall() if row[at].lower() == obj.lower()]
                if len(exact) != 1:
                    raise LookupError(f"snowflake: {len(exact)} object(s) named {obj} in {self.cloud.schema(self.stack)}")
                cur.execute(f'SELECT * FROM {self.cloud.sf_database}."{self.cloud.schema(self.stack)}"."{exact[0]}"')
                names = [d[0] for d in cur.description]
                return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
            finally:
                cur.close()
        return _ch(stand.clickhouse_port, stand.secrets.clickhouse, f"SELECT * FROM gate.`{obj}`")

    def sabotage(self) -> str:
        """Break one value in a warehouse on purpose (one cent on one BigQuery batch row): the oracle must go red."""
        table = f"{self.cloud.project}.{self.cloud.dataset(self.stack)}.bigquery_orders"
        self._client("bigquery").query(f"UPDATE `{table}` SET amount = amount + 0.01 WHERE id = 1").result()
        return f"{table} id=1 amount + 0.01"

    def source_rows(self, stand: Any) -> dict[str, list[dict[str, Any]]]:
        """Every source row of every table, read by DuckDB's postgres scanner."""
        con = oracle.connect(stand.source_dsn)
        try:
            out = {}
            for table, (_, cols) in self.tables.items():
                cur = con.execute(f"SELECT {', '.join(cols)} FROM src.public.{table}")
                names = [d[0] for d in cur.description]
                out[table] = [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
            return out
        finally:
            con.close()

    def grade(self, stand: Any, v: Any, name: str, deleted: Mapping[str, int]) -> None:
        """Every warehouse read back by a second client and compared with the source.

        Batch: the current state (the view, or BigQuery's compacted base) equals the source, with exactly the keys the
        cycle deleted left over (an incremental extract cannot see a delete). CDC: the current state without the rows
        flagged deleted equals the source. BigQuery batch: the buffer is gone after `compact`.
        """
        print(f"INVENTORY {name} {json.dumps(v.guard(f'{name}/wh/inventory', self.inventory, stand))}", flush=True)
        source = v.guard(f"{name}/wh/source", self.source_rows, stand)
        if source is None:
            return
        for wh in self.warehouses:
            for table, (short, cols) in self.tables.items():
                obj = f"{wh}_{short}"
                cell = f"{name}/wh/{wh}/batch/{short}"
                v.check(
                    cell + "/current-equals-source",
                    self._compare(stand, wh, obj, source[table], cols, deleted.get(table, 0), flagged=False),
                    ok=f"{len(source[table])} row(s)",
                )
                if wh == "bigquery":
                    v.check(cell + "/compacted", lambda wh=wh, obj=obj, c=cell: compacted(self.objects(stand, wh), obj, c))
                v.check(
                    f"{name}/wh/{wh}/cdc/{table}/live-equals-source",
                    self._compare(stand, wh, table, source[table], cols, 0, flagged=True),
                    ok=f"{len(source[table])} row(s)",
                )

    def _compare(
        self, stand: Any, wh: str, obj: str, source: list[dict[str, Any]], cols: list[str], deleted: int, *, flagged: bool
    ) -> Callable[[], list[str]]:
        what = f"{wh} {obj}"
        return lambda: current_state(self.read(stand, wh, obj), source, cols, "id", deleted, what, flagged=flagged)
