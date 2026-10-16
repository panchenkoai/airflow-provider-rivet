# airflow-provider-rivet

Apache Airflow operators and DAG builders that run the [rivet](https://github.com/panchenkoai/rivet)
binary on the worker as a subprocess and turn its result into Airflow's: retry or stop, which
unit failed, and what to do about it. The contract it implements is
[ADR-0039](https://github.com/panchenkoai/rivet/blob/main/docs/adr/0039-scheduler-contract.md).

Status: alpha. It is versioned independently of the binary and checks
the binary's version at run time (minimum **rivet 0.31.0**; a lower one is refused with both
versions in the message). Supported: Airflow 2.10+ and 3.x, Python 3.12+ (Airflow 2.10 only on 3.12).

## Install

```bash
pip install airflow-provider-rivet            # or airflow-provider-rivet[slack]
```

Install it into the Airflow environment with Airflow's own constraints file, so it does not move
Airflow's pins.

The rivet binary must be on every worker. On the official Airflow images (Debian 12, glibc 2.36)
take it from rivet's container image, which is built on the same Debian release; the
`*-linux-gnu` archive of rivet 0.31.0 needs glibc 2.38 and does not start there:

```dockerfile
FROM apache/airflow:3.3.2
COPY --from=ghcr.io/panchenkoai/rivet:v0.31.0 /usr/local/bin/rivet /usr/local/bin/rivet
RUN pip install --no-cache-dir airflow-provider-rivet
```

The rivet binary must be on the worker's `PATH`, or pass `rivet_bin="/abs/path/rivet"`.

## Works today / needs a newer rivet

ADR-0039 describes several things no released binary emits yet. The package detects what the
installed binary can do and takes a recorded fallback for the rest: every task's XCom carries
`degraded: [<key>, ...]`; the task log names the missing flags in one `note` line (`this binary
lacks: ...`) and the other keys in words, and the `rivet.result` record at DEBUG carries the list.
Nothing is guessed without a key.

**How it detects.** A flag is detected by probing `rivet <subcommand> --help` once per process
(not by a version table: the features below have no release number yet, and a development or
backported build has a flag whatever its version string says). An output member (`class`,
`failures[]`, `stop_reason`) is detected by its presence in the output itself. The version is
used for one thing only: the minimum-version refusal.

"Needs" names the ADR-0039 section; none of it is in a released rivet at this package's date,
so the "rivet >=" column is the first release that carries that section.

| Key | Missing in rivet 0.31.0 | What the package does today | Needs (ADR-0039) |
|---|---|---|---|
| `no_notify` | global `--no-notify` | does not pass it; warns when the config has a `notifications:` block | D9, rivet >= unreleased |
| `lock_wait` | global `--lock-wait` and the per-export run lease | does not pass it; the builders set `max_active_runs=1` and `max_active_tis_per_dag=1` | D7, rivet >= unreleased |
| `state_url_sqlite` | `RIVET_STATE_URL=sqlite:<dir>` | copies the config into the state directory, so `.rivet_state.db` lands beside it there | D5, rivet >= unreleased |
| `error_object` | `class`, `retryable`, `kind`, `action` on the `--json-errors` line | derives `class` and `retryable` from the printed line's integer `exit_class`; `kind` and `action` are null | D1/D2, rivet >= unreleased |
| `per_unit_error` | `failures[]` and `per_export[].error` | when several units of one process failed, each carries the process-level object | D2/D3, rivet >= unreleased |
| `stop_reason` | CDC `stop_reason` and `tables[]` | `stop_reason` is null: a `max_events` stop cannot be told from `caught_up` | D4, rivet >= unreleased |
| `apply_summary` | `--summary-output` on `rivet apply` | reads run id, counts, status and `skip_reason` (a `skip_empty` export that delivered nothing is `skipped`) from `rivet metrics --json` after a successful apply | D3, rivet >= unreleased |
| `load_filter` | `--export` on `rivet load` / `rivet compact` | writes a single-export copy of the config and loads that: the other `- name:` items are cut out of the original text, so what stays (quoted `${VAR}`, comments, anchors) is byte-exact; a stream keeps the batch exports its `cdc.backfill` reads; a text that cannot be cut safely (an anchor a kept item reads, an item sharing a line) is refused before rivet starts (`RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE`) | D3, rivet >= unreleased |
| `load_table_filter` | `--table` on `rivet load` / `rivet compact` | the per-table task handles every table of its export; tasks of one export are serialised by a file lock in the state directory | D3, rivet >= unreleased |
| `load_result` | `--summary-output` on `rivet load` / `rivet compact` | status is `loaded` / `compacted` on exit 0 and `failed` otherwise; `skipped`, `table` and row counts are unknown (null) | D3, rivet >= unreleased |

Five more keys record something the worker or one output lacks, not the binary's version:

| Key | What is missing | What the package does |
|---|---|---|
| `stderr_temp` | a state directory or `stderr_dir` on the worker | writes rivet's stdout and stderr to a private per-user directory under the temp directory (see Secrets); a pod loses them with its disk |
| `crashed_ledger` | a readable and writable crashed-budget ledger | without a state directory: a crash is retried only within the first `crashed_retries` tries of the task instance; with a ledger that cannot be read or written: no crash retry at all |
| `result_malformed` | an error object or a result entry with the contract's keys and types; also a stderr line that starts with `{` and is not JSON, or a summary file of an exit-0 process that is not a JSON object | a malformed error object is ignored and the task is classified by the exit-code table; a value of the wrong type is null; a status outside the contract's words counts as `failed` |
| `result_missing` | the `--summary-output` file of a process that exited 0 | rivet could not write it (it only warns); the units are built as if the binary had no summary flag: one unit per requested export or table, status from the exit |
| `exit_zero_failed_unit` | a non-zero exit when the summary reports a failed unit | the task fails, classified by the worst failed unit (`data_integrity` > `internal` > `refusal` > `schema_drift` > `retryable` > `generic`, ADR-0039 D2) |

Not detectable, so not recorded per task:

- A child process killed by a signal is not reported as `crashed` by today's binary: the parent
  exits 1 (or 2, by a text match) and the task follows that exit. A kill of the rivet process
  the operator started is seen directly and is `crashed` today.
- There is no `RIVET_STATE_FOREIGN` refusal yet. The package's own marker check (below) is the
  only guard against a wrong state directory.
- rivet does not yet warn about a relative `cdc.checkpoint` under PostgreSQL state.

The list lives in one place in the code, `airflow_provider_rivet/capabilities.py`
(`DEGRADATIONS`), and a test fails when a key has no row in this table.

## Operators

All of them take `config`, `export`, `table` (one table of the export, for `load` and `compact`),
`state_dir`, `plan_file` (the layout file `RivetPlanOperator` rewrites), `rivet_bin`, `env`,
`env_from_connections`, `env_passthrough`, `cloud_credentials`, `connections` (storage and
warehouse Connections, see "Storage and warehouse connections"), `deployment` (`auto` / `local` /
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

## DAG builders

### Batch: `build_batch_dag`

```python
from airflow_provider_rivet.dags import build_batch_dag

dag = build_batch_dag(
    "rivet_postgres",
    config="/opt/pipelines/postgres.yaml",
    state_dir="/var/lib/rivet/postgres",
    plan_file="/opt/pipelines/postgres.plan.json",  # or exports=["users", "orders"]
    compact_exports=["orders"],
    operator_kwargs={"env_from_connections": {"RIVET_PG_URL": "rivet_postgres"}},
)
```

```
plan ──> apply: [a] [b] ─> wave_2_done ─> [big] ─> after_big ─> [huge]
  │              │   │                     │                     │
  └─> (every     │   │                     │                     │
      apply)    [a] [b]            load: [big]                 [huge]
         load:                             │
      compact:                           [big]          every task ─> watcher
```

- Three TaskGroups, `apply`, `load`, `compact`. They are the picture; **dependencies run per
  table**: `apply.X >> load.X >> compact.X`, all `all_success`. A failed `apply.a` leaves
  `apply.b`, `load.b` and every later wave runnable; it blocks `load.a` and `compact.a`.
- Waves come from the plan file, as in the recipe, and each wave is ordered the way `rivet apply`'s
  cost gate orders it: every heavy export alone, one after another, then the cheap exports of the
  wave together; wave N+1 starts after wave N. Ordering goes through tasks that
  do nothing, `apply.wave_<n>_done` and `apply.after_<export>`, with `trigger_rule="all_done"`:
  the next export waits for the earlier one to *finish*, not to succeed.
- Every rivet task is `trigger_rule="all_success"` whatever `default_args` says; a `trigger_rule`
  in `operator_kwargs` is refused. `default_args` you pass are merged over the builders'
  (`retries=2`, `retry_delay` 5 minutes), so adding a callback keeps the retries.
- `exports`, `compact_exports` and `tables` are lists; a bare string is refused. A name that is
  not an ASCII task id (`daily orders`, `orders:v2`, `заказы`) gets a task id with each character
  outside `[A-Za-z0-9_.-]` replaced by `_` and an 8-character hash of the name (`apply.daily_orders_<hash>`); rivet still
  receives the real name.
- `plan` is a direct upstream of every `apply` task: a failed plan refresh blocks the whole
  run (`upstream_failed`), because a run that could not plan has nothing it should extract.
- **A run with a failed task is a failed run.** Airflow colours a run by its leaf tasks, and an
  ordering task that succeeds after a failure would otherwise leave the run green. Every DAG the
  builders make ends in one task, `watcher` (`trigger_rule="one_failed"`, no retries), downstream
  of every other task: it is skipped when nothing failed and fails when anything did (the
  watcher pattern of Airflow's documentation).

| What fails | Its own table | Other tables | Run |
|---|---|---|---|
| `plan` | every `apply`, `load`, `compact`: `upstream_failed`; nothing reaches rivet | the same | failed |
| `apply.X` | `load.X`, `compact.X`: `upstream_failed` | run; the next of a heavy chain and the next wave still start | failed |
| `load.X` | `compact.X`: `upstream_failed` | run | failed |
| `compact.X` | | run | failed |
| nothing | | | success (`watcher` skipped) |
- `plan_file` is read at parse time (no database access); without one, pass `exports=[...]`.
- With `plan_file`, a `plan` task rewrites that file from `rivet plan` at the start of every run;
  `refresh_plan=False` drops the task and keeps the file as it is.
- `extract="run"` uses `rivet run -e X` instead of plan + apply.
- **Compact tasks are created only for the exports named in `compact_exports`.** Whether a table
  compacts depends on the load mode, `load.layout` and the warehouse, which only rivet resolves
  (`full` never compacts; see ADR-0039 D3 for the skip reasons), and today's binary cannot report
  `skipped` in a form a machine can read. A task that could only skip would show green having done
  nothing. Once `rivet compact` reports per-table status, a named export that rivet skips shows as
  an Airflow skip with its `skip_reason` in XCom.
- `load=False` drops the load group (a config with no `load:` block).

### CDC: `build_cdc_dag`

```python
from airflow_provider_rivet.dags import build_cdc_dag

dag = build_cdc_dag(
    "rivet_orders_cdc",
    config="/opt/pipelines/cdc.yaml",
    export="app_cdc",
    tables=["orders", "users"],
    state_dir="/var/lib/rivet/cdc",
)
```

- `run` is ONE task for the stream. `load.T` and `compact.T` are separate tasks per table.
- `run >> load.T >> compact.T`, all `all_success`: **nothing is loaded or compacted after a
  drain that did not succeed** (a refusal such as a log gap means the files of this cycle are
  not the stream), and a failed `load.T` blocks `compact.T` but not another table. The parts of
  an earlier cycle whose load failed are loaded by the next run whose drain succeeds: the load
  consumes what rivet's ledger says is unloaded. A load that finds nothing to do is a success
  (never an Airflow skip: a skip would propagate to `compact.T`).
- The same `watcher` task ends the DAG: a failed `run`, `load.T` or `compact.T` fails the run.
- On `stop_reason: max_events` the run task succeeds, puts it in XCom and logs a `note` line
  `stopped at max_events (...)` (record `rivet.cdc.max_events`). It does not loop.
- `load=False` drops the load and compact tasks (the stream's files are the product);
  `compact=False` keeps `load.T` and drops `compact.T`. Use `compact=False` on Snowflake and
  ClickHouse, where there is nothing to compact (see "Storage and warehouse connections").
- `operator_kwargs` go to every task, as in the batch builder.

## Reading the task log

Each task writes one block when it ends: a headline with the outcome first, one line per rivet
step, and indented `note` lines. A successful apply on rivet 0.31.0 with SQLite state:

```
rivet apply users → OK: 30 rows, 1 file in 4.1 s (run users_20261016T092509.120_4242)
  plan     sealed, strategy incremental in 0.8 s
  apply    done in 2.9 s
  metrics  read the run's counts from rivet's state in 0.3 s
  note     this binary lacks: apply_summary, lock_wait, no_notify (the operator works around them)
  note     SQLite state is single-host — see https://github.com/panchenkoai/airflow-provider-rivet#local-worker-setup-guide
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
  note     SQLite state is single-host — see https://github.com/panchenkoai/airflow-provider-rivet#local-worker-setup-guide
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

## Retries

| What rivet did | Object | Airflow |
|---|---|---|
| exit 0, no failed unit in the summary | none | success |
| exit 0, a failed unit in the summary | the worst failed unit's (`exit_zero_failed_unit`) | by that object: `retryable` is retried, anything else is not |
| a printed object that is malformed (no `retryable`, `"retryable": "true"`, an unknown `class`, an `exit_class` that is not the exit status) | built from the exit status by the rows below (`result_malformed`) | by the built object; never an unhandled exception |
| exit 2, error line printed with `retryable: true` (today: `exit_class: 2`) | `retryable` | `AirflowException`: retried under the task's `retries` |
| exit 1, 3, 4, 5, 6 | `generic`, `data_integrity`, `schema_drift`, `refusal`, `internal` | `AirflowFailException`: no retry |
| exit 2, no error line (argument error) | built: `generic`, exit_code 1 | `AirflowFailException`: no retry |
| exit 101 (panic) | built: `internal`, exit_code 6 | `AirflowFailException`: no retry |
| killed by a signal, or exit 129-255 | built: `crashed`, exit_code null | retried under its OWN budget, `crashed_retries` (default 1), then `AirflowFailException` |
| exit 1 with a printed line saying `class: "crashed"` (a killed child, future binary) | `crashed` | the same crashed budget |
| any other status | built: `generic` | `AirflowFailException`: no retry |
| the package's own preflight refused | `preflight_refusal: RIVET_AIRFLOW_*` in XCom | `AirflowFailException`: no retry |
| exit 5 with code `RIVET_STATE_RUN_IN_PROGRESS` or `RIVET_LOAD_TABLE_BUSY` (another rivet holds the export's run lease or the table's load lease; rivet's remedy is "wait, then repeat") | `refusal`, `retryable: true` whatever the line says | `AirflowException`: retried under `retries` |
| the export's lock stayed held for `lock_wait` seconds ("Overlap") | `busy: RIVET_AIRFLOW_EXPORT_BUSY` in XCom; rivet is not started | `AirflowException`: retried under `retries` |

The exception text is `[code] class: action (export)`. It never contains rivet's error message.

**The crashed budget across tries.** Airflow clears a task instance's XCom at the start of every
try, so the count cannot live there. On a local worker the operator keeps a small ledger file,
`<state_dir>/airflow/<dag_id>/crashed/<run_id>__<task_id>-<hash>.json`, holding `[try, max_tries]` for
every try that ended in a crash. A crash is retried when fewer than `crashed_retries` earlier
crashes carry the task instance's current `max_tries`. Airflow moves `max_tries` when a task is
cleared, so a manual clear gets a fresh budget; changing `retries` in the DAG file in the middle
of a series does not move it, so it cannot reset the budget.

Every way the ledger can fail grants NO crash retry (`crashed_ledger` in `degraded`, the reason
in a WARNING `note` line and in the `rivet.crashed` record under `ledger_problem`): a directory that cannot be created or
written, and a file that is not the ledger's JSON, which is left as it is until a person deletes
it. On a worker with no state directory (a pod) there is no file: a crash is retried only within
the first `crashed_retries` tries of the task instance, so neither a crash after a transient
failure nor a crash after a manual clear is retried.

Limit: the crashed budget can only shorten Airflow's own budget, never extend it. With
`retries=0` Airflow retries nothing, a crash included. The builders default to `retries=2`.

The operators never run `rivet state reset-chunks` before a retry: a crashed checkpointed run is
resumed by the next process.

**A retry of an apply that already succeeded.** The task process can die after `rivet apply` exited
0 and before Airflow recorded the success (OOM of the task runner, a pod evicted, a zombie). The
retry is a try of the same series, so it used to replay the same sealed plan: a `mode: full` export
was extracted a second time, an incremental one was refused `RIVET_PLAN_ARTIFACT_STALE` (exit 5, a
failed task and a skipped load). Before it starts `rivet apply`, the task writes
`<artifact>.apply.json` beside the sealed plan: the plan's SHA-256, the start time and, once the
process runs, its pid. A retry that finds this record for the same plan bytes asks
`rivet metrics --export X --json` for a run of the export whose status is `success` (or `skipped`)
and whose run id, `<export>_<UTC start>_<pid>` as rivet makes it, carries the recorded pid and a
start no earlier than the record's. When there is one, the task starts no `rivet apply`, logs
`SKIPPED: an earlier try already applied this plan` (record `rivet.apply.already_applied`) and ends success with that run's `run_id`, `rows` and `files`. A run
recorded `running`, `interrupted` or failed, another process's run (a `rivet run` of the export, a
second DAG), a plan that was replaced, or a state `rivet metrics` cannot read: the plan is applied
again, as before. Residual risk: an unrelated successful run of the same export by a rivet process
that got the same pid after the record was written (the pid reused on that host, or another host
sharing the state directory) would be taken for it. A checkpointed run that a later apply resumed
under the dead run's id carries the first process's pid, so it is not recognised and the plan is
applied again.

## State, by deployment

| Worker | State | What the operator does |
|---|---|---|
| Ephemeral pod (`KUBERNETES_SERVICE_HOST` in the environment, or `deployment="pod"`) | PostgreSQL, required | refuses SQLite (`RIVET_AIRFLOW_STATE_SQLITE_ON_POD`); refuses every CDC export (`RIVET_AIRFLOW_CDC_ON_POD`: the checkpoint is a file and would live on the pod's disk) until rivet stores it in the state database; runs the config in place; keeps rivet's stderr in a private file on the pod's disk (`stderr_temp`), never in the task log |
| Local or long-lived worker (`deployment="local"` overrides the detection for a long-lived Kubernetes worker with a persistent volume) | SQLite in a declared directory, or PostgreSQL | requires `state_dir` for SQLite state and for CDC; checks it; refuses state or a checkpoint left beside the original config; warns once per task that SQLite is single-host |

`state_dir` is an operator parameter, not an Airflow Variable. The builders pass one value to
every task they create, so "the same path for every task" holds by construction, it is visible
in the rendered fields, and reading it needs no metadata-database access on the worker.

PostgreSQL state is `RIVET_STATE_URL=postgresql://...` in the task's environment:
`env_from_connections={"RIVET_STATE_URL": "rivet_state"}`, or set it on the worker and name it in
`env_passthrough=["RIVET_STATE_URL"]` (a worker variable is not inherited unless it is named).

**A PostgreSQL `RIVET_STATE_URL` set on the worker and not passed on is refused.** If the
worker's `RIVET_STATE_URL` is one rivet would use (it starts with `postgres`, rivet's own rule;
an empty, blank or `sqlite:` value selects nothing and is not refused) and the task's
environment has no entry for it
(not in `env_passthrough`, `env` or `env_from_connections`), the task fails before rivet starts
with `RIVET_AIRFLOW_STATE_ENV_NOT_PASSED` and is not retried: rivet would otherwise run green on
an empty SQLite state while the real state sits in the database the worker names. The message
names the variable and never its value. Fix it one of two ways: inherit the worker's value with
`env_passthrough=["RIVET_STATE_URL"]`, or give the task its own value with
`env_from_connections={"RIVET_STATE_URL": "<conn_id>"}` (or `env`). If the worker's variable is
not meant for this pipeline, unset it on the worker, or declare `env={"RIVET_STATE_URL": ""}` on
the task: a declared value, also an empty one, is the task's own decision and rivet then keeps
SQLite state in `state_dir`.

### Local worker setup guide

1. **Create the state directory yourself, once, on storage that outlives the worker process.**
   A host path or a named volume, never a container's own filesystem. One directory per
   pipeline (per source database), for example `/var/lib/rivet/<pipeline>`. The operator never
   creates it: a directory that silently appears empty is exactly the failure this guide exists
   to prevent, so a missing one is refused (`RIVET_AIRFLOW_STATE_DIR_MISSING`).
2. **Every task of every DAG that touches the pipeline must see the SAME absolute path.** With
   Docker, mount the same host path at the same container path in every worker. Pass that path
   as `state_dir` (the builders do it for all their tasks).
3. **What is in it.**
   - `.rivet_state.db`: rivet's state, under its default name.
   - `<config name>.<hash>.yaml` (and `<config name>.<hash>--<export>.yaml`): the operator's
     copy of your config, byte for byte. `<hash>` is the first 8 hex digits of the SHA-256 of the
     original's absolute path, so two configs with one file name never share a copy. Today's
     rivet can only keep SQLite state beside the config file, so the operator materialises the
     config here and runs that copy (`state_url_sqlite`). Edit your original; the copy is
     rewritten on every task.
   - every relative `query_file` of the config, copied to the same relative path (see 5), and
     `.rivet_airflow_query_files.json`, which records the config directory each one came from.
   - a relative `cdc.checkpoint`, which resolves beside the copied config, that is, here.
   - `airflow/<dag_id>/`: sealed plan artifacts
     (`plans/<run_id>/<export>__<task_id>__s<max_tries>__<inputs>-<hash>.json`), the crashed ledger, scratch
     files; `airflow/<dag_id>/plans/<run_id>/<artifact>.apply.json` (the record of an apply a try
     started, "Retries"); `airflow/locks/run/<export>.lock` (run, apply, cdc_run),
     `airflow/locks/<export>.lock` (load and compact of a whole export) and
     `airflow/locks/table/<export>__<table>-<hash>.lock` (load and compact of one table) ("Overlap").
   - `logs/<dag_id>/<task_id>-<hash>/`: rivet's stdout and stderr, one file per step and try.
   - **How a name becomes a path.** A DAG id, task id, run id or export name that is one lower-case
     word of `a-z 0-9 _ . -` (at most 80 characters, not starting with `.` or `-`) is used as it
     is. Any other name (other characters such as `:` `+` or non-ASCII letters, upper case, a
     mapped task's `task_id[index]`) is written as its readable form (each run of other
     characters replaced by `_`) followed by `-` and 12 hex digits of the SHA-256 of the exact
     name: run id `scheduled__2026-10-06T00:00:00+00:00` becomes
     `scheduled__2026-10-06T00_00_00_00_00-b07b0ae9e551`. Two different names
     therefore never share a directory, a sealed plan, a log file or a ledger, also on a file
     system that ignores case. A task's log folder always carries the hash, also for a plain
     task id, so it is never a folder an older layout wrote to.
   - **One sealed plan per task, try series, config and export.** The artifact's name is made from the
     export, the task id (with the map index), the try series (`s<max_tries>`; Airflow moves
     `max_tries` when a task is cleared) and `<inputs>`: a digest of the config's absolute
     path, the config's bytes and the export's `query_file`. Two tasks of one DAG run never
     share a plan, also when they run two configs that both have an export of one name; a
     retry of the same task replays its own plan; a clear starts a new series and plans again,
     so a clear after a successful apply never replays a cursor snapshot that apply already
     moved past (rivet would refuse it `RIVET_PLAN_ARTIFACT_STALE`); a config or query file edited between two
     tries is planned again under a new name.
   - A retry replays its own plan only while the resolved config path and the config's bytes
     are unchanged. A config reached through a symlink that is re-pointed between two tries
     (a new git-sync revision of the DAG folder) is planned again, also when the bytes are
     the same, and leaves one more config copy and plan artifact in the state directory.
   - **The plan digest does not cover the environment.** Changing the source between two
     tries of one task instance (another URL in `env`, an edited Connection) replays the plan
     made for the old source against the new one. Clear the task in a new DAG run, or delete
     its artifact, after such a change.
   - The artifact records the export and the config it was planned from (`export_name`,
     `config_path`). `apply` refuses a plan object in which either is missing or is not the
     task's own (`RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN`), expired or not, and leaves the file as
     it is, so the retry is refused for the same reason until a person deletes it (or, once it has
     expired and is 24 h old, the pruning below removes it). The config
     is compared as a file, not as text: another case, Unicode form or symlink of the same
     file is the task's own, and a recorded path that no longer exists is not. A file at
     that path that is not a JSON object (empty, cut short) is planned over.
   - **Upgrading a state directory** written by an earlier build of this package: files of an
     older layout are left alone. They are never read, never pruned and never deleted, so
     remove `logs/<dag_id>/<task_id>/` folders without a `-<hash>` suffix and
     `airflow/<dag_id>/plans/<run_id>/<export>.json` files by hand when you no longer need
     them. A run in flight during the upgrade plans again, and its crashed budget starts empty
     (at most one extra crash retry).
   - `.rivet_airflow_marker`: a random id written by the first task that used the directory.

   The copies and every file under `airflow/` and `logs/` are created with mode `0600`, in
   directories of mode `0700` (an existing `airflow/` or `logs/` directory is tightened).
   **Moving an existing pipeline under Airflow:** if `.rivet_state.db` or a relative
   `cdc.checkpoint` file already sits beside your original config and not here, the task is
   refused before rivet starts (`RIVET_AIRFLOW_STATE_BESIDE_CONFIG`,
   `RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG`): the copy would not see it, and rivet would start
   from an empty state or re-anchor the stream. Stop the runs, move the file into the state
   directory (the state database with its `-wal` and `-shm` files; the checkpoint to the same
   relative path), and run again. Using the config's own directory as `state_dir` needs no copy
   and no move.
4. **What breaks when a task sees a different or an empty directory.** rivet creates a new,
   empty state there and treats every export as a first run: an incremental export re-reads
   from the start, a CDC stream re-anchors and the changes between the lost position and the new
   anchor are gone, with no error. What the operator can check without rivet's state identity:
   the directory exists, is absolute and writable, and its marker equals the marker of each
   direct upstream rivet task of the same DAG run (taken from their XCom); a difference is
   refused with `RIVET_AIRFLOW_STATE_DIR_DIFFERS`. It cannot see a directory that was emptied
   between two DAG runs, or two hosts that each have their own copy.
5. **Relative paths in the config.** The copy changes the directory rivet takes as the
   config's, and nothing else:

   | Key | rivet resolves it against | Under this package |
   |---|---|---|
   | `query_file` | the config's directory; an absolute path, `..` and a symlink out of it are refused by rivet | stays as written; the file is copied to the same relative path in the state directory before every task. Refused before rivet starts (`RIVET_AIRFLOW_QUERY_FILE`) when it is absolute, has `..`, leaves the directory through a symlink, is missing for the task's export, or when a config from ANOTHER directory already put a file at that relative path in this state directory |
   | `cdc.checkpoint` | the config's directory | resolves in the state directory; a file left beside the original is refused, see 3 |
   | `destination.path` (and the other paths rivet opens as written, such as `tls.ca_file`) | the process's working directory | unchanged: the working directory defaults to the directory of your ORIGINAL config; set `cwd` to change it |
   The preflight reads the config as rivet does: every `${name}` is taken from a `-p` /
   `--param` in `extra_args`, then from the task's environment, and spliced into the text before
   the YAML is parsed, with YAML 1.2 scalars (`017` and `name: 2024` stay the text written). A
   `mode` no variable resolves is treated as `cdc` by the guards; an export name or a
   `cdc.checkpoint` no variable resolves is refused (`RIVET_AIRFLOW_CONFIG_UNRESOLVED`), as rivet
   refuses the config.
6. **Single host.** SQLite state and the lock files are visible to one host only. Two workers
   on two hosts need PostgreSQL state.
7. **Moving to PostgreSQL state.** Stop the DAGs; create a database; set `RIVET_STATE_URL` to
   its `postgresql://` URL for every task (the worker's environment, or
   `env_from_connections`). This package does not copy the SQLite state into PostgreSQL: the
   new state starts empty, so plan the switch as a first run of every export (a full export
   simply runs again; an incremental export has no cursor to resume from). Keep `state_dir`
   for CDC: a checkpoint is a file on every state backend today, and it stays where it is.

## Secrets

- rivet is configured through environment variables and nothing else, and **the subprocess gets
  a minimal environment, not the worker's**. It is built from three things:
  1. The worker's variables on one allow-list (`ENV_ALLOWLIST` in `invocation.py`): `PATH`,
     `HOME`, `USER`, `LOGNAME`, `TZ`, `LANG`, `LC_*`, `TMPDIR`, `SSL_CERT_FILE`, `SSL_CERT_DIR`,
     `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`, `ALL_PROXY`, `http_proxy`, `https_proxy`,
     `no_proxy`, `all_proxy`, `KUBERNETES_SERVICE_HOST`, `KUBERNETES_SERVICE_PORT`. Nothing else
     is inherited: not `AIRFLOW__*`, not `AIRFLOW_CONN_*`, not a `RIVET_*` variable set on the
     worker. `env_passthrough=["RIVET_STATE_URL", "MY_*"]` extends the list with names or
     shell-style patterns. `cloud_credentials=["gcp", "aws", "azure"]` adds
     `GOOGLE_APPLICATION_CREDENTIALS` and `GOOGLE_CLOUD_PROJECT`, `AWS_*`, `AZURE_*` for a
     destination or warehouse that authenticates from the environment. A storage Connection
     (`connections=`, below) that supplies a credential of one of these kinds **replaces** the
     worker's variables of that kind, whichever way they were passed: the worker's
     `AWS_SESSION_TOKEN` never rides beside a Connection's static keys, nor its account key beside
     a Connection's SAS token. A Connection that supplies no credential (an AWS one with only a
     region, a GCP one with only a project) leaves the worker's identity in place.
  2. `env={NAME: value}`. It is not templated and not stored in the metadata database, but it
     is whatever your DAG file computes: do not write a secret literal there.
  3. `env_from_connections={ENV_NAME: conn_id}`, which builds a URL from an Airflow Connection's
     fields inside `execute`, credentials and the database name percent-encoded. The scheme
     comes from `{ENV_NAME: (conn_id, "postgresql")}`, else the connection extra `rivet_scheme`,
     else the connection type (`postgres`, `mysql`, `mssql`, `mongo`, `oracle`); a type that
     names no rivet scheme, such as `generic`, is refused before rivet starts
     (`RIVET_AIRFLOW_CONNECTION_SCHEME`). A password without a login is kept, an IPv6 literal is
     bracketed with its zone encoded (`fe80::1%eth0` becomes `[fe80::1%25eth0]`), any other host
     is kept as written (`db:5432` with an empty port field), an Oracle connection's extra
     `service_name` is used as the path. The standard extras that have a URL form become query
     parameters: PostgreSQL `sslmode`, `application_name`, `connect_timeout`; MongoDB `srv` (the
     `mongodb+srv` scheme, which takes no port), `authSource`, `authMechanism`, `replicaSet`,
     `tls`/`ssl`, `allow_insecure`, `directConnection`, `readPreference`, `appName`. An extra
     that changes how the hook authenticates and has no URL form (PostgreSQL `iam`, `redshift`;
     MySQL `unix_socket`) is refused (`RIVET_AIRFLOW_CONNECTION_UNSUPPORTED`). Other extras are
     client-library settings and are not read. The extra `rivet_params` is appended as written
     and wins over a mapped extra of the same name. A `conn_id` no backend
     defines, here or in `connections=[...]`, is refused before rivet starts
     (`RIVET_AIRFLOW_CONNECTION_MISSING`), not retried.
  4. `connections=[conn_id, ...]`: storage and warehouse Connections, see below.

  argv carries the config path, export names and flags.
- XCom carries code, class, exit_code, retryable, action, kind, counts, `stop_reason`, the
  degradations and file paths. It never carries the error `message`: redaction covers
  credentials only, and error text can embed source cell values.
- The task log gets a readable block in the package's own words (see "Reading the task log"):
  outcome, codes, class, action, counts, run id, step exit statuses, paths. Names the DAG gives
  (export, table) are the only text in it the package did not write. The one-line JSON records,
  `rivet.start` (argv), `rivet.step`, `rivet.unit` (one per unit), `rivet.result`, `rivet.warning`,
  `rivet.crashed`, `rivet.cdc.max_events`, `rivet.refused`, ..., are written at DEBUG, under the
  same rules.
- **rivet's raw stdout and stderr go to files and never to the task log or to the worker
  process's own stderr** (which Airflow 3, and Airflow 2 with `run_as_user`, copy into the task
  log). The files are
  `<base>/<dag_id>/<task_id>-<hash>/<run_id>__try<N>.<step>.stderr.log` (and `.stdout.log`), mode `0600`
  in `0700` directories; XCom and a failed task's `stderr` log line name the path (and the
`rivet.start` record at DEBUG). `<base>` is
  `stderr_dir` when given, else `<state_dir>/logs`, else, on a worker with no state directory,
  `<temp dir>/rivet-airflow-<uid>/logs` (recorded as `stderr_temp`; a pod loses the files with
  its disk, and what a failure was is still in the error object).
- **Retention.** When a task ends, the files of its newest `log_keep` tries (default 10, counted
  per task across runs) are kept and the older ones are removed. `log_keep=None` keeps
  everything. Only the task's own folder is looked at; a file of an older layout is never
  removed (see "Upgrading a state directory"). Under `airflow/<dag_id>/`, a sealed plan artifact
  that has expired and was last written over 24 h ago is removed (with the `plans/<run_id>/`
  folder it leaves empty), and so is a crashed ledger untouched for 24 h (a crash of that series
  more than 24 h after its last one starts a fresh budget). A ledger that is not the ledger's JSON
  is left for a person to delete.
- `stream_stderr=True` copies stderr into the task log when each step ends. It is the one way
  raw stderr reaches the task log, and it is for debugging: it puts possibly data-bearing text
  into a store with wider access than the worker.

## Storage and warehouse connections

`connections=["gcp_rivet", "sf_rivet"]` (on every operator, or in a builder's `operator_kwargs`)
turns each Airflow Connection into what rivet reads, inside `execute`: variables, and key files
written mode `0600` into a private folder that is removed when the task ends, whatever its
outcome. The kind comes from the connection type, or from the extra `rivet_kind`; one task
takes at most one Connection of each kind (`RIVET_AIRFLOW_CONNECTION_KIND_TWICE`), and a type
that is none of them is refused before rivet starts (`RIVET_AIRFLOW_CONNECTION_KIND`).

| Kind (connection type) | Read from the Connection | rivet gets | The config names it |
|---|---|---|---|
| `gcp` (`google_cloud_platform`) | extra `keyfile_dict` (written to a file) or `key_path`; `project` | `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_CLOUD_PROJECT` | nothing: GCS and BigQuery read it |
| `aws` (`aws`) | login, password; extras `aws_session_token`, `region_name`, `endpoint_url`. A role (`role_arn`, `assume_role_*`), a `profile_name`, or keys in extras (also under `session_kwargs`) are refused, `RIVET_AIRFLOW_CONNECTION_UNSUPPORTED`: rivet signs with static or session keys only | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`, `AWS_ENDPOINT_URL` | `access_key_env: AWS_ACCESS_KEY_ID`, `secret_key_env: AWS_SECRET_ACCESS_KEY` (+ `session_token_env`) |
| `azure` (`wasb`) | login (account), password (key); extra `sas_token` | `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `AZURE_STORAGE_SAS_TOKEN` | `account_name: ${AZURE_STORAGE_ACCOUNT}` (rivet reads no account variable of its own), and `account_key_env: AZURE_STORAGE_KEY` or `sas_token_env: AZURE_STORAGE_SAS_TOKEN` |
| `snowflake` (`snowflake`) | extras `account`, `region`, `role`, `warehouse`, `database`, `private_key_content` (PEM, or base64 of PEM as Airflow's SnowflakeHook takes it; written to a file) or `private_key_file`; with a key the password is the key's passphrase, else the login password | a `snow` connection named as `load.connection` in a private `SNOWFLAKE_HOME`; `RIVET_SNOWFLAKE_KEY`; `PRIVATE_KEY_PASSPHRASE` for an encrypted key | `load.connection` |
| `clickhouse` (`clickhouse`, or any type with `rivet_kind: clickhouse`) | host (required, `RIVET_AIRFLOW_CONNECTION_INCOMPLETE` without one), port, login, password; extra `secure` or `protocol` | `RIVET_CLICKHOUSE_URL`, `RIVET_CLICKHOUSE_USER`, `RIVET_CLICKHOUSE_PASSWORD`, and the password also under the config's own `password_env` (`rivet init` writes `CLICKHOUSE_PASSWORD`) | `url: ${RIVET_CLICKHOUSE_URL}`, `user: ${RIVET_CLICKHOUSE_USER}`, `password_env: RIVET_CLICKHOUSE_PASSWORD` (or any name) |

The config keeps what is not a secret (bucket, project, dataset, database, schema); the
Connection holds who may write there. The Snowflake loader runs the `snow` CLI, which must be on
the worker (`pip install snowflake-cli` into its own environment). On BigQuery `rivet compact`
merges the buffer into the base table; on Snowflake and ClickHouse the current state is a view
over `<table>__changes` and there is nothing to merge. rivet 0.31.0 (and main) says so only in a
stderr line (`compact [<table>]: skipped — this warehouse keeps a change log behind a view and
never compacts`) and exits 0; `rivet compact` has no `--summary-output` yet (`load_result`), so the
task ends green as `compacted`, having done nothing. For these warehouses build the CDC DAG with
`compact=False` and leave such exports out of `compact_exports`.

## Slack alerts

**Setup guide: [docs/slack.md](docs/slack.md)** (the Slack app and its token, storing it,
wiring, what arrives, a DAG that fails on purpose to test it, troubleshooting).

A failed rivet task posts one title to a Slack channel, `dag_id.task_id — <error type>` (the
rivet code and `final` or `retryable`, else the exception's class), with the run id and a
task-log link; its error text (rivet's error line for the failed step, else the exception's
message, through Airflow's secrets masker) is a code block in the thread. A bot token
(`slack` Connection, scope `chat:write`, `channel=` the channel id) threads; an incoming webhook
(`slackwebhook` Connection, no `channel`) posts title and error text as one message.

```python
from airflow_provider_rivet.callbacks import slack_failure_callback

dag = build_batch_dag(
    "rivet_postgres",
    config=...,
    state_dir=...,
    exports=[...],
    default_args={"retries": 2, "on_failure_callback": slack_failure_callback("slack_rivet", channel="C0123ABCDEF")},
)
```

The `watcher` task carries no failure callback: the builders set its `on_failure_callback` to
none, so a failed run sends one message per failed task and nothing for the watcher (which has no
rivet result to show). Unlike XCom and the task log ("Secrets"), the thread carries rivet's error
text, which can hold source values. `format_title`, `format_error_text` and `format_failure` are
the formatters on their own, to send through another channel.

Mutual exclusion: under Airflow, rivet's own notifications are off and alerting is Airflow's.
The operators pass `--no-notify` to a binary that has it. Until then **do not configure
`notifications:` in a rivet config run under Airflow**: every failure would be sent twice. The
operator warns (a WARNING `note` line; record `rivet.warning`, `notifications_block`) when it sees
the block.

## Overlap

Two runs of the same export must not overlap. rivet's lease (`--lock-wait`, default 600 s: the
second run waits) is not released yet. Until then the builders set `max_active_runs=1` on the
DAG and `max_active_tis_per_dag=1` on every rivet task. **This does not protect against a run
started outside Airflow** (cron, a shell), nor against a second DAG over the same export.

Airflow does not stop two tries of one task from running at once: a zombie try (its worker lost,
the task still running) and its retry, or a rivet left running by a task process that was killed
(the scheduler counts task instances, and the dead try's is already failed). With a state
directory every task but `plan` holds an exclusive lock file while it runs rivet: one per export
for `run`, `apply` and `cdc_run`; one per table for `load` and `compact` with `--table`, else one
per export. The lock's descriptor is passed to every rivet process the task starts, so a rivet that
outlives its task keeps the lock until it exits. A task that finds the lock held waits up to
`lock_wait` seconds, then starts no rivet and ends retryable (`RIVET_AIRFLOW_EXPORT_BUSY`; the
waiting is logged when it starts, `waiting up to <lock_wait> s for the lock of ...`, record `rivet.lock.wait`). The task releases the lock by an explicit unlock once its
rivet has exited. With `load_table_filter` degraded, the per-table tasks of one export share the
export's lock and wait for one another, now up to `lock_wait` seconds, then retry.

On Linux each rivet process gets SIGTERM when the task process that started it dies
(`PR_SET_PDEATHSIG`), so a task runner killed outright (OOM, SIGKILL) no longer leaves rivet
running beside its retry. Where that cannot fire (not Linux; the signal is tied to the thread that
started rivet; a rivet that does not stop on SIGTERM) the lock above still keeps a retry from
starting a second rivet until the first has exited.

**Without a state directory (a pod) there is no lock file.** Only rivet's own lease protects the
export: rivet main refuses the second holder with `RIVET_STATE_RUN_IN_PROGRESS` /
`RIVET_LOAD_TABLE_BUSY` (exit 5), which the task retries ("Retries"). rivet 0.31.0 has no lease for
a non-checkpointed run, so on 0.31.0 a pod's zombie try and its retry can both run.

## Known limits

- The Kubernetes mode has unit tests only (no pod was run).
- On rivet 0.31.0 the `rows` / `files` of a CDC `run` task leave out its `initial: snapshot`: the first
  run of a CDC DAG reports 0 rows while the snapshot files hold the whole table (the binary's
  `--summary-output` counts the stream only; found by the live gate).
- This package's tests run `load` and `compact` only through the fake binary, which replays
  rivet's output and talks to no warehouse. A live run against BigQuery, Snowflake and ClickHouse
  exists only as the warehouse leg of the live gate (`python -m gate --warehouses ...`, see "Live
  gate"), which reads each warehouse back through a second client and compares it with the source.
- On Snowflake and ClickHouse a compact task is green as `compacted` while rivet merged nothing
  (see "Storage and warehouse connections").
- With `load_table_filter` degraded, the per-table tasks of a multi-table CDC export each load
  the whole export; the first does the work, the others find the ledger up to date.
- `rivet apply` prints no result today, so a successful apply task reads its counts from
  `rivet metrics`; a failed one has no counts.
- A failure callback runs where Airflow runs task callbacks, in the task's process on the worker;
  exercised live on Airflow 2.10.5 and 3.3.2 by hand (docs/slack.md), not by the live gate.

## Development

```bash
git clone https://github.com/panchenkoai/rivet ../rivet   # the contract fixtures the tests read
uv sync                                                   # latest Airflow 3 from uv.lock, plus pytest
uv run pytest -q
```

The tests read rivet's scheduler contract fixtures (`tests/fixtures/scheduler/`) and
`src/config/export.rs` and `src/cli/args.rs` (every flag the fake rivet of `tests/fake_rivet.py` accepts is one
rivet defines; any other is refused with exit 2, as clap refuses it) from a rivet checkout: `../rivet` by default, or
the path in `RIVET_SRC`.
CI checks rivet out at the commit in `RIVET_CONTRACT_REF` and runs the suite on Airflow 2.10 and
3.x under Airflow's constraints files.

The tests need no rivet binary and no running Airflow: a fake `rivet` script replays each
scenario (and enforces rivet's own `query_file` rule, tied to the Rust source by a test).
`tests/test_dag_runs.py` runs the builders' DAGs through `dag.test()` against a SQLite metadata
database created in a throwaway `AIRFLOW_HOME`, with real task instances and trigger rules.

### Mutation testing

[mutmut](https://github.com/boxed/mutmut) 3.8 mutates `airflow_provider_rivet/` only (never `tests/`, `gate/` or
`examples/`) and runs, for each mutant, the tests that cover the mutated function. Run it in Linux, never on the
Mac's own Python: mutmut forks a worker per mutant, and macOS aborts a forked child of a multithreaded process that
loads an Objective-C class (psutil, under Airflow's import), so there a mutant's verdict is the crash, not its tests.
`dev/mutants.sh` runs it in a Linux container (Docker), as the calling user, which also runs the Linux-only
`PR_SET_PDEATHSIG` test of `tests/test_retry.py` (in Linux the two case- and Unicode-folding file-system tests of
`tests/test_operators.py` skip instead):

```bash
make mutants                                  # dev/mutants.sh mutmut run, then the survivors; resumable (mutants/)
make mutants-results                          # the survivors, timeouts and other non-killed mutants of the last run
dev/mutants.sh mutmut show <mutant name>      # the diff of one mutant
dev/mutants.sh python dev/mutants_gate.py pr  # what the PR job runs: the mutants of the functions your branch touches
```

`RIVET_SRC` applies as for `pytest`. The configuration is `[tool.mutmut]` in `pyproject.toml`. Workers fork from a
server that only collected the tests (`process_isolation = "forkserver"`), and `dev/mutmut_isolation.py` gives each
worker its own Airflow metadata database, dags folder and pytest base temp: on one shared SQLite file the
`airflow_db` fixture's `resetdb` of one worker would kill another worker's DAG runs and count its mutant as
detected. It also caps each worker's memory (a mutant can loop while growing a list, and on a CI runner the OOM
killer then takes the runner), hands the mutated tree to the child Pythons of `tests/test_retry.py` and
`tests/test_process_io.py` (the task processes those tests kill), and has mutmut select those tests for every
mutant, since a child's calls are invisible to mutmut's test selection. A timeout is not a detection: a cluster of
timeouts in one function is a finding about its loop or wait.
mutmut does not mutate a decorated function (`Outcome.decision`, `_prctl`, `_export_lock`): their behaviour is
held by tests alone.

When mutants run (`.github/workflows/mutation.yml`, on Linux, through `dev/mutants_gate.py mode`):

| change | what runs | fails when |
|---|---|---|
| a pull request changes `airflow_provider_rivet/` code | the mutants of every function it touches | any of them is not killed |
| a pull request changes only tests, docs or the harness | nothing | — |
| a release (`release.yml` runs it on the tagged commit before it builds) | the full run | as above: nothing is published from a commit whose mutants were not counted |
| on demand (workflow_dispatch) | the full run | as above |

There is no full run on push to main and no nightly run. Every run is split into ten parallel shards (the mutants
whose number ends in 0 … 9). Each shard prints `shard N: M of TOTAL mutants`, its progress as `shard N: done/M` and
`shard N: tested T, killed K, not killed S`. The ratchet job prints the per-shard table and fails unless the shards
graded every mutant exactly once (`sum(tested) == total`) and `survivors N <= ratchet M`. A survivor of a function
every test passes through (`_Session.step`) runs the whole suite, so one runner would need most of a day.

Moving the ratchet: it changes only through a full run (on demand or at a release).
`.mutmut-survivors` lists the survivors of that run by name (the ratchet job uploads it as `mutmut-survivors`); the
two are committed together.
Lowering it (after tests kill survivors) is an ordinary PR; raising it needs a dated comment line in that file saying
which mutants are accepted and why.

## Live gate

`gate/` runs this provider and a rivet Linux binary in real Airflow 2.10.5 and 3.3.2 stacks (the official
compose files, Celery) and grades the result with oracles, never with rivet's own report:

```bash
uv run --only-group gate python -m gate --rivet-bin /path/to/linux/rivet            # both Airflow versions
uv run --only-group gate python -m gate --rivet-bin ./rivet --airflow 3.3.2 --keep  # one stack, left up
uv run --only-group gate python -m pytest -q gate/tests                             # the oracles, RED on every mutant
```

Per stack: four DAGs (incremental batch and CDC with `initial: snapshot`, each on a shared SQLite state and on
a TLS PostgreSQL state), three cycles (initial load, a delta of inserts / updates / deletes, an idle run). The
SQLite CDC run pauses at `cdc_after_open` and the SQLite batch run must finish inside that window, so the two
writers on one SQLite file overlap on every cycle. Oracles: DuckDB over the Parquet against the source read by
DuckDB's postgres scanner (delta == source rows past the previous high-water; newest version per key == source;
CDC snapshot + changes replayed by `__seq` == source; exact operation counts), the stored cursor read by a plain
SQLite / psql client, and the Airflow REST API (run and task states, XCom rows == rows in the files).
`--sabotage drop-delta-file` must turn it red. Each stack is torn down with its volumes before and after; the
compose project and the image carry the run tag (`--tag`, default `%m%d%H%M` + 4 hex), so two runs never share one.

Options: `--rivet-bin` (required), `--airflow` (comma-separated versions, default both), `--work` (working
directory, default a fresh temporary one), `--verdicts <file>` (one JSON line per graded cell), `--keep`,
`--sabotage drop-delta-file` or `warehouse-value`, `--tag` (the run tag).

**Warehouse leg.** `--warehouses bigquery,snowflake,clickhouse` (any subset) also extracts to GCS and runs
`load` and `compact` into those warehouses from Airflow Connections (`connections=`). It needs a GCP credential
(`--gcp-keyfile`, default the gcloud application-default credentials), the bucket `--gcs-bucket`, the BigQuery
`--bq-project` and `--bq-location`, the `bq`, `gcloud` and `snow` CLIs on the host (ClickHouse runs inside the
stack), and for Snowflake a local `snow` connection (`--sf-connection`) whose login the gate's Connection copies,
`--sf-warehouse`, `--sf-database` and the storage integration `--sf-integration` (a role that cannot create
schemas needs the run's `AFGATE_<TAG>_<stack>` schema made beforehand: pass `--tag`). Each warehouse is read back by a
client the loader does not use (google-cloud-bigquery, snowflake-connector-python with the key of the local `snow`
connection, ClickHouse over HTTP as JSON), every value is brought to one form (integer, integer cents, epoch
microseconds, text) and compared with the source: the batch current state equals the source with exactly the keys the
cycle deleted left over (an incremental extract cannot see a delete); the CDC current state without the rows flagged
`__is_deleted` equals the source; the BigQuery batch buffer `<table>__changes` is gone after `compact`, which only
that DAG may not skip. `--sabotage warehouse-value` adds a cent to one BigQuery row after the delta load and must turn
it red. The cleanup lists the dataset, schema and GCS prefix again and fails on anything left; the sweep of runs older
than a day drops only names of the gate's own shape.

rivet's own release gate does not run this gate: rivet has no hook for it. The `Live gate` workflow here runs it
against a released binary, on demand and on an `airflow-v*` tag.
