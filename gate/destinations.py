"""Destination legs: the postgres scenario extracted to an S3 bucket (MinIO in the stack) and, from the same kind of
S3-staged export, loaded and compacted into the stack's ClickHouse. Every rivet config is the one `rivet init` writes
(`--s3-bucket --s3-region`, with `--clickhouse-*` for the load half), used as written; the S3 credential and the
MinIO endpoint reach rivet only through an Airflow `aws` Connection (`connections=[...]`).

Oracles: the Parquet in MinIO read by DuckDB's httpfs (a client rivet does not use) and compared with the source as
the local-Parquet oracles do; ClickHouse read back over HTTP as JSON and compared value by value; a bucket that does
not exist is a coded refusal Airflow does not retry.

init writes one fixed replication slot per schema (`rivet_public_cdc`) and `cleanup_source: true` in every `load:`
block (the staged parts are deleted once loaded), so the extract-only half (its Parquet stays to be read) and the load
half run against two postgres servers: `gate-source` and `gate-source-s3`, which play the same scenario.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from . import oracle, warehouse
from .stand import SOURCE_DSN, state_db

NAME = "s3"
REGION = "us-east-1"
#: MinIO's root user: the `aws` Connection's login (a name, not a secret: only the password is masked and random).
MINIO_USER = "afgate-minio"
MINIO_ENDPOINT = "http://gate-minio:9000"
TABLES = {
    "gate_customers": ["id", "name", "tier", "updated_at"],
    "gate_orders": ["id", "customer_id", "amount", "status", "updated_at"],
}
#: Keys an incremental extract cannot see deleted, left in the batch target after each cycle.
DELETED_BY_BATCH = {"initial": {}, "delta": {"gate_orders": 5}, "idle": {"gate_orders": 5}}
#: The refusal a write into a bucket that does not exist must end in (rivet's src/error.rs), and how long it may take:
#: a persistent 404 read as transient is retried for minutes (rivet #606, Azurite's `keep-alive: timeout=5`).
MISSING_BUCKET_CODE = "RIVET_DEST_CONTAINER_NOT_FOUND"
MISSING_BUCKET_SECONDS = 120
SERVICES = """\
  gate-minio:
    # Chainguard's build: `minio/minio` (Docker Hub) and `quay.io/minio/minio` answer "unauthorized" since 2026-09.
    image: cgr.dev/chainguard/minio:latest
    command: ["server", "/data"]
    environment: {{ MINIO_ROOT_USER: afgate-minio, MINIO_ROOT_PASSWORD: {minio} }}
    ports: ["127.0.0.1::9000"]
  gate-source-s3:
    image: postgres:16
    environment: {{ POSTGRES_USER: rivet, POSTGRES_PASSWORD: {source_s3}, POSTGRES_DB: rivet }}
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10"]
    ports: ["127.0.0.1::5432"]
    healthcheck: {{ test: ["CMD", "pg_isready", "-U", "rivet"], interval: 5s, retries: 20 }}
"""


@dataclass(frozen=True)
class Config:
    """One config `rivet init` writes for the leg."""

    kind: str
    mode: str  # init's --mode
    service: str  # the postgres server it reads
    url_env: str
    source_conn: str
    bucket: str
    clickhouse_db: str = ""  # with a ClickHouse `load:` block (the load half)

    @property
    def dag(self) -> str:
        return f"gate_s3_{self.kind}"


CONFIGS = {
    c.kind: c
    for c in (
        Config("batch", "incremental", "gate-source", "RIVET_GATE_SOURCE_URL", "rivet_gate_source", "gate-s3-batch"),
        Config("cdc", "cdc", "gate-source", "RIVET_GATE_SOURCE_URL", "rivet_gate_source", "gate-s3-cdc"),
        Config(
            "ch_batch", "incremental", "gate-source-s3", "RIVET_GATE_S3_SOURCE_URL", "rivet_gate_s3_source",
            "gate-s3-ch-batch", "gate_s3_batch",
        ),
        Config(
            "ch_cdc", "cdc", "gate-source-s3", "RIVET_GATE_S3_SOURCE_URL", "rivet_gate_s3_source",
            "gate-s3-ch-cdc", "gate_s3_cdc",
        ),
        # The bucket is never made: the run must stop on a coded refusal, once.
        Config("missing", "incremental", "gate-source", "RIVET_GATE_SOURCE_URL", "rivet_gate_source", "gate-s3-missing"),
    )
}  # fmt: skip
#: The configs whose DAGs run in every cycle (the missing-bucket DAG runs once, on its own).
CYCLED = ("batch", "cdc", "ch_batch", "ch_cdc")


def init_argv(cfg: Config, out: str) -> list[str]:
    """`rivet init` for one config of the leg, over the scenario's tables."""
    argv = [
        "rivet", "init", "--source-env", cfg.url_env, "--tls", "disable", "--mode", cfg.mode,
        "--s3-bucket", cfg.bucket, "--s3-region", REGION,
    ]  # fmt: skip
    if cfg.clickhouse_db:
        argv += ["--clickhouse-url", "http://gate-clickhouse:8123", "--clickhouse-database", cfg.clickhouse_db,
                 "--clickhouse-user", "loader"]  # fmt: skip
    return [*argv, "--include", *TABLES, "-o", out]


def check_shape(kind: str, shape: Mapping[str, Any]) -> list[str]:
    """What is wrong with the exports init wrote for a config: a batch config is one export per scenario table; a CDC
    config is one stream over both tables beside a recipe per table; no config holds anything else."""
    tables = sorted(TABLES)
    problems = []
    if sorted(shape["exports"]) != tables:
        problems.append(f"{kind}: batch exports {shape['exports']}, expected {tables}")
    streams = shape["streams"]
    if CONFIGS[kind].mode == "cdc":
        if len(streams) != 1 or sorted(streams[0]["tables"]) != tables:
            problems.append(f"{kind}: streams {streams}, expected one over {tables}")
    elif streams:
        problems.append(f"{kind}: streams {streams} in a batch config")
    if shape["extra"]:
        problems.append(f"{kind}: other exports {shape['extra']}")
    return problems


# --- Oracles --------------------------------------------------------------------------------------------------------


def table_parts(files: Sequence[str], bucket: str, table: str) -> dict[str, list[str]]:
    """A config's Parquet parts of one table by role: `batch` (an export's `exports/<table>/`), `snapshot` (a stream's
    baseline, `cdc/<table>/snapshot/`), `changes` (the rest of `cdc/<table>/`)."""
    out: dict[str, list[str]] = {"batch": [], "snapshot": [], "changes": []}
    for f in sorted(files):
        key = f.removeprefix(f"s3://{bucket}/")
        if key.startswith(f"exports/{table}/"):
            out["batch"].append(f)
        elif key.startswith(f"cdc/{table}/"):
            out["snapshot" if "/snapshot/" in key else "changes"].append(f)
    return out


def missing_bucket(states: Mapping[str, Mapping[str, Any]], payloads: Mapping[str, Any], seconds: float) -> list[str]:
    """A run into a bucket that does not exist stops on rivet's coded refusal (exit 5), at once, and is not retried.

    `states`: the run's rivet task instances by task id; `payloads`: their XCom return values (None when absent)."""
    failed = {t: i for t, i in states.items() if i.get("state") == "failed"}
    if not failed:
        return [f"no task failed: {sorted((t, i.get('state')) for t, i in states.items())}"]
    problems = []
    for task, inst in sorted(failed.items()):
        tries = inst.get("try_number")
        if tries != 1:
            problems.append(f"{task}: try {tries}, a refusal is never retried by Airflow")
        start, end = inst.get("start_date"), inst.get("end_date")
        took = (_instant(end) - _instant(start)).total_seconds() if start and end else None
        if took is None or took > seconds:
            problems.append(f"{task}: took {took} s, more than {seconds} s (a persistent 404 retried as transient?)")
        payload = payloads.get(task) or {}
        error = payload.get("error") or {}
        if payload.get("decision") != "fail" or error.get("code") != MISSING_BUCKET_CODE or payload.get("exit_status") != 5:
            problems.append(
                f"{task}: decision {payload.get('decision')!r}, code {error.get('code')!r}, exit {payload.get('exit_status')!r}; "
                f"expected 'fail', {MISSING_BUCKET_CODE!r}, 5"
            )
    other = sorted(t for t, i in states.items() if i.get("state") not in ("failed", "upstream_failed", "skipped"))
    if other:
        problems.append(f"tasks that did not stop: {other}")
    return problems


def _instant(value: Any) -> datetime:
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=UTC)


# --- The leg ---------------------------------------------------------------------------------------------------------


class S3Leg:
    """The S3 destination leg of one stack: MinIO and the second source in the stack, init's configs, their DAGs, and
    their oracles. Nothing leaves the stack: `down -v` removes MinIO, its buckets and both sources."""

    name = NAME
    service_yaml = SERVICES
    configs = tuple(CONFIGS)

    def __init__(self) -> None:
        self.dags = [CONFIGS[k].dag for k in CYCLED]
        self.tasks: dict[str, list[str]] = {}
        self.may_skip: dict[str, tuple[str, ...]] = {}
        self.shape: dict[str, dict[str, Any]] = {}
        self.minio_port = 0
        self.source_s3_dsn = ""
        # What the earlier cycles left: every part, the batch high-water, each cycle's new change parts.
        self.files_before: dict[str, set[str]] = {k: set() for k in ("batch", "cdc")}
        self.high_water: dict[str, Any] = {}
        self.history: list[list[str]] = []

    def connections(self, secrets: Any) -> dict[str, dict[str, Any]]:
        """The S3 Connection (MinIO's endpoint in its extra), ClickHouse's, and the second source's."""
        return {
            "rivet_gate_s3": {
                "conn_type": "aws",
                "login": MINIO_USER,
                "password": secrets.minio,
                "extra": {"region_name": REGION, "endpoint_url": MINIO_ENDPOINT},
            },
            "rivet_gate_s3_clickhouse": {
                "conn_type": "clickhouse",
                "host": "gate-clickhouse",
                "port": 8123,
                "login": "loader",
                "password": secrets.clickhouse,
            },
            "rivet_gate_s3_source": {
                "conn_type": "postgres",
                "host": "gate-source-s3",
                "port": 5432,
                "login": "rivet",
                "password": secrets.source_s3,
                "schema": "rivet",
            },
        }

    # --- setup ---

    def mc(self, stand: Any, *args: str, check: bool = True) -> Any:
        """MinIO's own client inside its container; the root login reaches it as MC_HOST_gate, never on an argv."""
        env = {"MC_HOST_gate": f"http://{MINIO_USER}:{stand.secrets.minio}@localhost:9000"}
        return stand.compose("exec", "-T", "-e", "MC_HOST_gate", "gate-minio", "mc", *args, extra_env=env, check=check)

    def wait_ready(self, stand: Any, timeout: float = 300) -> None:
        deadline = time.monotonic() + timeout
        while True:
            ok = self.mc(stand, "ls", "gate", check=False).returncode == 0
            ok = ok and stand.compose("exec", "-T", "gate-source-s3", "pg_isready", "-U", "rivet", check=False).returncode == 0
            if ok:
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"MinIO or gate-source-s3 not ready after {timeout:.0f} s")
            time.sleep(3)

    def prepare(self, stand: Any, seed: str) -> None:
        """Seed the second source, make the buckets (not the missing one), the ClickHouse databases and the states;
        `rivet init` writes every config inside the stack's network."""
        from .sources import read_config

        self.wait_ready(stand)
        self.minio_port = stand.host_port("gate-minio", 9000)
        self.source_s3_dsn = SOURCE_DSN.format(port=stand.host_port("gate-source-s3", 5432), password=stand.secrets.source_s3)
        stand.psql("gate-source-s3", "rivet", seed)
        problems = []
        for kind, cfg in CONFIGS.items():
            if kind != "missing":
                self.mc(stand, "mb", f"gate/{cfg.bucket}")
            if cfg.clickhouse_db:
                warehouse._ch(
                    stand.clickhouse_port, stand.secrets.clickhouse, f"CREATE DATABASE IF NOT EXISTS {cfg.clickhouse_db}"
                )
            stand.psql("gate-state", "rivet_state", f"CREATE DATABASE {state_db(NAME, kind)};")
            (stand.rivet_dir / "state" / f"s3_{kind}").mkdir(parents=True, exist_ok=True)
            host = stand.rivet_dir / "configs" / f"s3_{kind}"
            host.mkdir(parents=True, exist_ok=True)
            out = f"/opt/rivet/configs/s3_{kind}/rivet.yaml"
            password = stand.secrets.source if cfg.service == "gate-source" else stand.secrets.source_s3
            stand.compose(
                "exec", "-T", "-e", cfg.url_env, "-e", "RIVET_STATE_URL", "airflow-worker", *init_argv(cfg, out),
                extra_env={
                    cfg.url_env: f"postgresql://rivet:{password}@{cfg.service}:5432/rivet",
                    "RIVET_STATE_URL": stand.state_url(state_db(NAME, kind)),
                },
            )  # fmt: skip
            left = sorted(p.name for p in host.glob(".rivet_state.db*"))
            if left:
                raise RuntimeError(f"init left {left} beside {host} although RIVET_STATE_URL named the PostgreSQL state")
            text = (host / "rivet.yaml").read_text()
            print(f"INIT s3 {kind} config:\n{text}", flush=True)
            shape = read_config(text, list(TABLES))
            problems += check_shape(kind, shape)
            self.shape[kind] = {
                **shape,
                "config": out,
                "state_dir": f"/opt/rivet/state/s3_{kind}",
                "url_env": cfg.url_env,
                "source_conn": cfg.source_conn,
                "state_conn": f"rivet_gate_state_{NAME}_{kind}",
                "connections": ["rivet_gate_s3"] + (["rivet_gate_s3_clickhouse"] if cfg.clickhouse_db else []),
                "load": bool(cfg.clickhouse_db),
            }
        if problems:
            raise ValueError("; ".join(problems))
        for kind in CYCLED:
            shape, dag = self.shape[kind], CONFIGS[kind].dag
            if shape["streams"]:
                tables = shape["streams"][0]["tables"]
                groups = ("load", "compact") if shape["load"] else ()
                self.tasks[dag] = ["run", *(f"{g}.{t}" for t in tables for g in groups)]
            else:
                groups = ("apply", "load", "compact") if shape["load"] else ("apply",)
                self.tasks[dag] = [f"{g}.{e}" for e in shape["exports"] for g in groups]
            # A compaction with nothing to merge skips (ClickHouse keeps a change log behind a view).
            self.may_skip[dag] = ("compact.",)
        (stand.rivet_dir / "configs" / "s3_shape.json").write_text(json.dumps(self.shape))

    def apply_changes(self, stand: Any, changes: str) -> None:
        stand.psql("gate-source-s3", "rivet", changes)

    # --- reads ---

    def duck(self, stand: Any) -> Any:
        """DuckDB with `src` (the postgres scanner on gate-source) and httpfs pointed at MinIO."""
        con = oracle.connect(stand.source_dsn)
        con.execute("INSTALL httpfs; LOAD httpfs")
        con.execute(
            "CREATE SECRET gate_minio (TYPE s3, KEY_ID ?, SECRET ?, REGION ?, ENDPOINT ?, URL_STYLE 'path', USE_SSL false)",
            [MINIO_USER, stand.secrets.minio, REGION, f"127.0.0.1:{self.minio_port}"],
        )
        return con

    def parts(self, con: Any, bucket: str) -> set[str]:
        """Every Parquet object of a bucket, as `s3://bucket/key`."""
        return {r[0] for r in con.execute(f"SELECT file FROM glob('s3://{bucket}/**/*.parquet')").fetchall()}

    def source_rows(self, dsn: str) -> dict[str, list[dict[str, Any]]]:
        con = oracle.connect(dsn)
        try:
            out = {}
            for table, cols in TABLES.items():
                cur = con.execute(f"SELECT {', '.join(cols)} FROM src.public.{table}")
                names = [d[0] for d in cur.description]
                out[table] = [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
            return out
        finally:
            con.close()

    def stored_cursors(self, stand: Any) -> dict[str, str]:
        text = stand.psql(
            "gate-state", state_db(NAME, "batch"), "SELECT export_name, coalesce(last_cursor_value, '') FROM export_state"
        )
        return dict(line.split("|", 1) for line in text.splitlines() if "|" in line)

    def read_clickhouse(self, stand: Any, db: str, table: str) -> list[dict[str, Any]]:
        return warehouse._ch(stand.clickhouse_port, stand.secrets.clickhouse, f"SELECT * FROM `{db}`.`{table}`")

    # --- grading ---

    def grade(self, stand: Any, v: Any, name: str, run_id: str) -> None:
        """The extract-only half: the Parquet in MinIO against gate-source. The load half: ClickHouse against
        gate-source-s3."""
        con = v.guard(f"{name}/s3/duckdb", self.duck, stand)
        if con is not None:
            try:
                self.grade_parquet(stand, v, name, run_id, con)
            finally:
                con.close()
        self.grade_clickhouse(stand, v, name)

    def grade_parquet(self, stand: Any, v: Any, name: str, run_id: str, con: Any) -> None:
        after = {k: v.guard(f"{name}/s3/{k}/listing", self.parts, con, CONFIGS[k].bucket) for k in ("batch", "cdc")}
        print(f"INVENTORY {name} s3 " + json.dumps({k: sorted(p or []) for k, p in after.items()}), flush=True)
        if after["batch"] is not None:
            cursors = v.guard(f"{name}/s3/batch/stored-cursors", self.stored_cursors, stand)
            bucket = CONFIGS["batch"].bucket
            new = after["batch"] - self.files_before["batch"]
            for table, cols in TABLES.items():
                cell, src = f"{name}/s3/batch/{table}", f"src.public.{table}"
                mine = table_parts(new, bucket, table)["batch"]
                every = table_parts(after["batch"], bucket, table)["batch"]
                v.check(
                    cell + "/delta-values", oracle.batch_delta, con, mine, src, cols, "updated_at", self.high_water.get(table),
                    ok=f"{oracle.rows_in(con, mine)} row(s) in {len(mine)} part(s)",
                )  # fmt: skip
                deleted = DELETED_BY_BATCH[name].get(table, 0)
                v.check(cell + "/latest-equals-source", oracle.batch_latest, con, every, src, cols, "id", "updated_at", deleted)
                xcom = v.guard(cell + "/xcom-rows", stand.xcom, CONFIGS["batch"].dag, run_id, f"apply.{table}")
                if xcom is not None:
                    reported = sum(u.get("rows") or 0 for u in xcom["units"])
                    v.check(cell + "/xcom-rows", oracle.same_count, reported, oracle.rows_in(con, mine), cell)
                if cursors is not None:
                    top = con.execute(f"SELECT max(updated_at) FROM {src}").fetchone()[0]
                    v.check(cell + "/stored-cursor", oracle.stored_cursor, cursors.get(table) or None, top, cell)
            self.files_before["batch"] = after["batch"]
            self.high_water = {t: con.execute(f"SELECT max(updated_at) FROM src.public.{t}").fetchone()[0] for t in TABLES}
        if after["cdc"] is not None:
            from .scenario import CHANGED_OPS, NO_OPS

            ops = CHANGED_OPS if name == "delta" else NO_OPS
            bucket = CONFIGS["cdc"].bucket
            new = after["cdc"] - self.files_before["cdc"]
            changes_new = sorted(f for t in TABLES for f in table_parts(new, bucket, t)["changes"])
            self.history.append(changes_new)
            for table, cols in TABLES.items():
                cell = f"{name}/s3/cdc/{table}"
                stream_new = table_parts(new, bucket, table)["changes"]
                v.check(cell + "/operations", oracle.cdc_ops, con, stream_new, ops[table], cell)
                snapshot = table_parts(after["cdc"], bucket, table)["snapshot"]
                stream = [table_parts(run, bucket, table)["changes"] for run in self.history]
                v.check(
                    cell + "/replay-equals-source", oracle.cdc_replay, con, snapshot, stream, f"src.public.{table}", cols, "id",
                    ok=f"snapshot {len(snapshot)} part(s), {sum(map(len, stream))} change part(s)",
                )  # fmt: skip
            xcom = v.guard(f"{name}/s3/cdc/xcom-rows", stand.xcom, CONFIGS["cdc"].dag, run_id, "run")
            if xcom is not None:
                reported = sum(u.get("rows") or 0 for u in xcom["units"])
                v.check(f"{name}/s3/cdc/xcom-rows", oracle.same_count, reported, oracle.rows_in(con, sorted(new)), "s3 cdc")
            self.files_before["cdc"] = after["cdc"]

    def grade_clickhouse(self, stand: Any, v: Any, name: str) -> None:
        source = v.guard(f"{name}/s3/clickhouse/source", self.source_rows, self.source_s3_dsn)
        if source is None:
            return
        for kind in ("ch_batch", "ch_cdc"):
            db, flagged = CONFIGS[kind].clickhouse_db, kind == "ch_cdc"
            for table, cols in TABLES.items():
                deleted = 0 if flagged else DELETED_BY_BATCH[name].get(table, 0)
                v.check(
                    f"{name}/s3/clickhouse/{kind.removeprefix('ch_')}/{table}/current-equals-source",
                    lambda db=db, t=table, c=cols, d=deleted, f=flagged: warehouse.current_state(
                        self.read_clickhouse(stand, db, t), source[t], c, "id", d, f"clickhouse {db}.{t}", flagged=f
                    ),
                    ok=f"{len(source[table])} row(s)",
                )

    def missing_bucket_probe(self, stand: Any, v: Any, wait_runs: Any) -> None:
        """The missing-bucket DAG runs once: its rivet task must stop on the coded refusal, at once, with no retry."""
        dag, run_id = CONFIGS["missing"].dag, "gate_s3_missing_bucket"
        stand.trigger(dag, run_id)
        state = wait_runs(stand, [dag], run_id).get(dag)
        tasks = {t: i for t, i in stand.tasks(dag, run_id).items() if t.startswith(("apply.", "plan"))}
        payloads = {}
        for task, inst in tasks.items():
            if inst.get("state") == "failed":
                try:
                    payloads[task] = stand.xcom(dag, run_id, task)
                except Exception as exc:  # no XCom: the cell says which task and why
                    payloads[task] = {"error": {"code": f"no XCom: {type(exc).__name__}"}}
        print(
            f"MISSING-BUCKET run {state}: "
            + json.dumps({t: {k: i.get(k) for k in ("state", "try_number", "start_date", "end_date")} for t, i in tasks.items()}),
            flush=True,
        )
        problems = missing_bucket(tasks, payloads, MISSING_BUCKET_SECONDS)
        if state != "failed":
            problems.insert(0, f"run {state}, expected failed")
        v.grade("s3/missing-bucket/coded-refusal", problems, ok=f"{MISSING_BUCKET_CODE}, exit 5, one try")


def legs(names: Sequence[str]) -> list[S3Leg]:
    return [S3Leg() for n in names if n == NAME]


DESTINATIONS = (NAME,)
