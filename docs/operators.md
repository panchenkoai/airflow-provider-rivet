# Operators

The operators, the XCom value every task returns, and the block each task writes to its log.

## Operators

All of them take `config`, `export`, `table` (one table of the export, for `load` and `compact`),
`state_dir`, `plan_file` (the layout file `RivetPlanOperator` rewrites), `rivet_bin`, `env`,
`env_from_connections`, `env_passthrough`, `cloud_credentials`, `connections` (storage and
warehouse Connections, see ["Storage and warehouse connections"](connections-and-secrets.md#storage-and-warehouse-connections)), `deployment` (`auto` / `local` /
`pod`), `extra_args`, `lock_wait` (default 600), `crashed_retries` (default 1), `stderr_dir`,
`stream_stderr`, `log_keep` (default 10) and `cwd`. Templated: `config`, `export`, `table`, `state_dir`, `plan_file`,
`extra_args`, `stderr_dir`, `cwd`. **`env` and `env_from_connections` are not templated**, so their
values are never stored as rendered fields. No operator builds argv: one module, `invocation.py`,
builds it, runs the process, reads the outputs and classifies.

| Operator | Runs | Notes |
|---|---|---|
| `RivetApplyOperator` | `rivet plan -e X --format json -o <artifact>` then `rivet apply <artifact>` | a retry replays the same sealed artifact while it is unexpired, unless rivet's state shows the apply an earlier try started already succeeded ("Retries"); a clear plans again |
| `RivetRunOperator` | `rivet run [-e X] --summary-output <file>` | the fallback when sealed apply is not wanted; also runs a whole config |
| `RivetCdcRunOperator` | `rivet run -e X --summary-output <file>` for a `mode: cdc` export | one bounded drain; succeeds on `max_events`, reports it, never loops |
| `RivetLoadOperator` | `rivet load` | per export, per table where the binary can narrow |
| `RivetCompactOperator` | `rivet compact` | with `load_result` degraded (rivet 0.31.0 and main) exit 0 is `compacted`, also when rivet merged nothing; a binary with compact's `--summary-output` reports `skipped`, which becomes an Airflow skip |
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
  "units": [ { "export": "orders", "table": null, "status": "success", "run_id": "orders_...",
               "rows": 2500, "files": 1, "stop_reason": null, "error": null } ],
  "error": null, "degraded": ["apply_summary", "lock_wait", "no_notify", "state_url_sqlite"],
  "exit_status": 0, "signal": null, "stderr_path": "/var/lib/rivet/postgres/logs/...",
  "skip_reason": null, "state_marker": "1d86...", "rivet_version": "0.31.0" }
```

A unit has exactly the shape of `tests/fixtures/scheduler/xcom_unit*.json`; a package test
compares the two, so the package and the Rust contract cannot drift.

## Reading the task log

Each task writes one block when it ends: a headline with the outcome first, one line per rivet
step, and indented `note` lines. A successful apply on rivet 0.31.0 with SQLite state:

```
rivet apply users → OK: 30 rows, 1 file in 4.1 s (run users_20261016T092509.120_4242)
  plan     sealed, strategy incremental in 0.8 s
  apply    done in 2.9 s
  metrics  read the run's counts from rivet's state in 0.3 s
  note     this binary lacks: apply_summary, lock_wait, no_notify (the operator works around them)
  note     SQLite state is single-host — see https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/state.md#local-worker-setup-guide
```

A failed one (a retry whose sealed plan another run has overtaken):

```
rivet apply users → REFUSED [RIVET_PLAN_ARTIFACT_STALE], final
  metrics  no earlier try's apply of this plan in rivet's state in 0.3 s
  plan     replayed the sealed plan of an earlier try
  apply    exit 5 in 0.6 s
  export   users: failed [RIVET_PLAN_ARTIFACT_STALE] refusal
  stderr   <state_dir>/logs/<dag_id>/<task_id>-<hash>/<run_id>__try2.apply.stderr.log
  note     this binary lacks: apply_summary, lock_wait, no_notify (the operator works around them)
  note     SQLite state is single-host — see https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/state.md#local-worker-setup-guide
  note     rivet's failure line has no class or action: the class is read from its exit class
```

- The headline's outcome is `OK`, `SKIPPED` (nothing to do: rivet's `skip_reason`, or an earlier
  try already applied the plan), `REFUSED` (class `refusal`, or a preflight refusal before rivet
  started), `FAILED` (with its class), `CRASHED` (a signal or a 129-255 exit), `STOPPED` (Airflow
  stopped the task) or `BUSY` (the export's lock stayed held). A final failure says `final` and is
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
