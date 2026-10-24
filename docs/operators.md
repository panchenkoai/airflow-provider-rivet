# Operators

The operators, the XCom value every task returns, and the block each task writes to its log.

## Operators

Every operator takes the same parameters (plus Airflow's own: `task_id`, `retries`, ...).
**Templated** ones accept Jinja; `env` and `env_from_connections` are not templated, so their
values are never stored as rendered fields. No operator builds argv: one module,
`invocation.py`, builds it and runs the process; `reading.py` reads rivet's outputs into units and classifies.

| Parameter | Default | Templated | What it is |
|---|---|---|---|
| `config` | required | yes | path of the rivet YAML config on the worker |
| `export` | `None` | yes | the export to run; required by `RivetApplyOperator` and `RivetCdcRunOperator`; `RivetRunOperator` without it runs the whole config |
| `table` | `None` | yes | one table of the export, for `load` and `compact` |
| `state_dir` | `None` | yes | an existing absolute directory on the worker for SQLite state, the config copy, sealed plans, locks and stderr files; never created by a task; required for SQLite state and CDC checkpoints (see [State](state.md)) |
| `plan_file` | `None` | yes | the layout file `RivetPlanOperator` rewrites and `build_batch_dag` reads waves from |
| `rivet_bin` | `"rivet"` | no | the binary: a name looked up on the worker's `PATH`, else the one `rivet-cli` installed; or an absolute path |
| `env` | `None` | no | extra variables for the rivet process; computed by your DAG file, so no secret literals |
| `env_from_connections` | `None` | no | `{ENV_NAME: conn_id}`: a database URL built from an Airflow Connection inside the task (see [Connections and secrets](connections-and-secrets.md)); the value may also be `{"conn_id": …, "database": …, "odbc_ini": …, "dsn": …}`: another database on the same server, and the odbc.ini data source `dsn` (default the id; MySQL, PostgreSQL, SQL Server and Oracle drivers) when no Connection has the id |
| `env_passthrough` | `()` | no | worker variables (names or `MY_*` patterns) passed on; nothing outside the allow-list is inherited otherwise |
| `cloud_credentials` | `()` | no | `"gcp"`, `"aws"`, `"azure"`: pass the worker's credential variables of that kind |
| `connections` | `()` | no | storage and warehouse Connection ids (see ["Storage and warehouse connections"](connections-and-secrets.md#storage-and-warehouse-connections)) |
| `deployment` | `"auto"` | no | `local` (a host that keeps its disk) or `pod` (ephemeral); `auto` reads `KUBERNETES_SERVICE_HOST` |
| `extra_args` | `None` | yes | arguments appended to rivet's argv, e.g. `["-p", "day=2026-10-20"]` |
| `lock_wait` | `600` | no | seconds a task waits for another task holding its export's lock before it ends retryable (`RIVET_AIRFLOW_EXPORT_BUSY`) |
| `rivet_notifications` | `False` | no | `True` keeps rivet's own `notifications:` on: `--no-notify` is not passed and nothing is warned (see [Slack](slack.md)) |
| `crashed_retries` | `1` | no | retries of a crash (a signal, exit 129-255) on their own budget, beside Airflow's `retries` (see [Retries](retries.md)) |
| `stderr_dir` | `None` | yes | where rivet's stdout/stderr files go; default `<state_dir>/logs`, else the temp directory |
| `stream_stderr` | `False` | no | also copy rivet's stderr lines into the task log as they come |
| `log_keep` | `10` | no | stderr/stdout files kept per task; `None` keeps all |
| `cwd` | `None` | yes | the rivet process's working directory (default: the original config's directory); it does not move a relative local `destination.path`, which lands under `state_dir` |
| `single_host` | `False` | no | declares that every task of this deployment runs on one host; needed for SQLite state under an executor that may use several (Celery, Kubernetes), which is refused otherwise (`RIVET_AIRFLOW_STATE_SQLITE_MULTI_HOST`, see [State](state.md)) |

| Operator | Runs | Notes |
|---|---|---|
| `RivetApplyOperator` | `rivet plan -e X --format json -o <artifact>` then `rivet apply <artifact>` | a retry replays the same sealed artifact while it is unexpired, and rivet answers a replay of a plan it already applied (["Retries"](retries.md)); a clear plans again |
| `RivetRunOperator` | `rivet run [-e X] --summary-output <file>` | the fallback when sealed apply is not wanted; also runs a whole config |
| `RivetCdcRunOperator` | `rivet run -e X --summary-output <file>` for a `mode: cdc` export | one bounded drain; succeeds on `max_events`, reports it, never loops |
| `RivetLoadOperator` | `rivet load` | per export, per table where the binary can narrow |
| `RivetCompactOperator` | `rivet compact` | with `load_result` degraded (rivet 0.31.0 and main) exit 0 is `compacted`, also when rivet merged nothing; a binary with compact's `--summary-output` reports `skipped`, which becomes an Airflow skip |
| `RivetInitOperator` | `rivet init` then `rivet check` of what it wrote | writes `config` once and never replaces it; the task `build_source_dags` adds while a source has no config (see [DAG builders](dag-builders.md#one-dag-per-database-build_source_dags)). It takes `config`, `state_dir`, `rivet_bin` and the environment parameters (`env`, `env_from_connections`, `env_passthrough`, `cloud_credentials`, `connections`), plus `mode`, `source_env` and `init_args`; the parameters of a rivet run (`lock_wait`, `extra_args`, `crashed_retries`, ...) are not its own, and `build_source_dags` gives them to the run tasks only |
| `RivetPlanOperator` | `rivet plan --format json` | rewrites the plan layout file atomically, only when the output is a layout `build_batch_dag` can read (waves with exports; one planned export, which rivet prints as a bare object with no campaign, is one wave from its `export_recommendation`); the file keeps its mode |

```python
from airflow_provider_rivet.operators import RivetApplyOperator

orders = RivetApplyOperator(
    task_id="orders",
    config="/opt/pipelines/postgres.yaml",
    export="orders",
    state_dir="/var/lib/rivet/postgres",
    env_from_connections={"RIVET_PG_URL": "rivet_postgres"},
)
```

Every task returns (and, on failure, pushes before raising) one XCom value under `return_value`:

```json
{ "command": "apply", "decision": "success",
  "units": [ { "export": "orders", "table": null, "target": null, "buffer": null,
               "status": "success", "run_id": "orders_...",
               "rows": 2500, "files": 1, "stop_reason": null, "error": null } ],
  "error": null, "degraded": ["lock_wait", "no_notify"],
  "exit_status": 0, "signal": null, "stderr_path": "/var/lib/rivet/postgres/logs/...",
  "skip_reason": null, "state_marker": "1d86...", "rivet_version": "0.32.1 (5ee08c0a)" }
```

A unit has exactly the shape of `tests/fixtures/scheduler/xcom_unit*.json`; a package test
compares the two, so the package and the Rust contract cannot drift. `target` and `buffer` are the
warehouse objects a load or compact unit wrote and read, as rivet's result names them (null for a
run or apply unit, and null when the binary's result has no names: the config's guess is never
put in XCom). What the warehouse did (rows inserted, updated and deleted, parts, jobs, bytes) is
not part of the XCom unit: it is in the task log and in the `rivet.unit` record's `digest`.

## Reading the task log

Each task writes one block when it ends: a headline with the outcome first, one line per rivet
step, and indented `note` lines. A successful apply on rivet 0.32.0 with SQLite state:

```
rivet apply users → OK: 30 rows, 1 file in 4.1 s (run users_20261016T092509.120_4242)
  plan     sealed, strategy incremental in 0.8 s
  apply    done in 2.9 s
  note     SQLite state is single-host — see https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/state.md#local-worker-setup-guide
```

A failed one (a retry whose sealed plan another run has overtaken):

```
rivet apply users → REFUSED [RIVET_PLAN_ARTIFACT_STALE], final
  plan     replayed the sealed plan of an earlier try
  apply    exit 5 in 0.6 s
  export   users: failed [RIVET_PLAN_ARTIFACT_STALE] refusal
  stderr   <state_dir>/logs/<dag_id>/<task_id>-<hash>/<run_id>__try2.apply.stderr.log
  note     SQLite state is single-host — see https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/state.md#local-worker-setup-guide
  note     rivet's failure line has no class or action: the class is read from its exit class
```

A task of one step that took the task's time (a load, a compact, a run) has no step line: the
headline says it. The step line stays when the step failed or the task spent a second or more
outside it (a lock wait). Flags the binary lacks are noted at DEBUG only. `rivet_version` is the
whole `rivet --version` line after `rivet`, so a branch build is told apart from the release.

A load or compact task names the warehouse object it wrote after the arrow, as rivet's result names
it: a load the table of a `full` export, else its `__changes` log
(`rivet load cdc table orders → my-project.demo_cdc.orders__changes: OK: 50 rows in 12.0 s`); a
compact the buffer it merged and the base (`….orders__changes into ….orders`). A binary whose
result has no such name gets it from the config's `load:` block, marked `(from config)`
(`load_digest`).

With rivet >= 0.32.2 a load or compact also says what the warehouse did, from rivet's per-table
digest (ADR-0039 D3): the headline sums the rows, the warehouse jobs and the bytes billed, and
one line per table gives a compact's inserted / updated / deleted rows and the base's rows after,
a load's Parquet parts and source runs, the job ids (the first three) and the bytes billed. A
value rivet did not return (bytes on ClickHouse and Snowflake, job ids on Snowflake) is left out,
never shown as 0:

```
rivet compact cdc table orders → p.orders__changes into p.orders: OK: 1200 rows, 1 job, 50.0 MiB billed in 3.4 s, after 30.0 s waiting for the lock of export `cdc` (run …)
  table    cdc.orders → p.orders__changes into p.orders: compacted, 1200 rows, 200 inserted, 950 updated, 50 deleted, base 10150 rows, job rivet_compact_orders_1, 50.0 MiB billed (run …)
```

A task that waited for the export's lock says so when it takes it (``took the lock of export
`cdc` after 30.0 s``), and its headline gives the time after the wait apart from the wait.

- The headline's outcome is `OK`, `SKIPPED` (nothing to do: rivet's `skip_reason`, or an earlier
  try already applied the plan), `REFUSED` (class `refusal`, or a preflight refusal before rivet
  started), `FAILED` (with its class), `CRASHED` (a signal or a 129-255 exit), `STOPPED` (Airflow
  stopped the task) or `BUSY` (the export's lock stayed held, or rivet's own lease of the export or
  table was held by another rivet: `RIVET_STATE_LOCK_TIMEOUT`, `RIVET_STATE_RUN_IN_PROGRESS`,
  `RIVET_LOAD_TABLE_BUSY`; retried). A final failure says `final` and is
  an ERROR line; one Airflow retries says `retryable, Airflow retries it (try N of M)` and is a
  WARNING line. The registry `action` follows when rivet printed one.
- `note` lines are INFO, except what you must act on: a `notifications:` block rivet will send
  from, a crash-retry ledger that cannot be used (WARNING).
- No INFO or WARNING line of a successful task holds `error` or `exception`, the words Airflow
  colours red by default (`[logging] color_log_error_keywords`), unless an export or table name
  holds one.
- rivet's stderr text is never in the block: a failed task prints the file's path. argv and the
  structured records are at DEBUG (`[logging] logging_level = DEBUG`); XCom carries the outcome,
  units, error object, degradations and paths.
