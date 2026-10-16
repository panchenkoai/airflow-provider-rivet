# Changelog

All notable changes to `airflow-provider-rivet`. The package is versioned independently of the
rivet binary; its release tags will use the prefix `airflow-v`.

## 0.1.0 (unreleased)

First release, against rivet 0.31.0 and the contract of ADR-0039; Python 3.12+, Airflow 2.10 and 3.x.

- Operators: `RivetApplyOperator`, `RivetRunOperator`, `RivetCdcRunOperator`, `RivetLoadOperator`,
  `RivetCompactOperator`, `RivetPlanOperator`; one invocation module builds argv and env, runs
  the process, reads its outputs and classifies the result.
- DAG builders: `build_batch_dag` (plan, then apply / load / compact groups with per-table
  edges and ordered waves) and `build_cdc_dag` (one run task, load and compact per table).
- Retries by the contract: a printed retryable failure is retried, exits 1 and 3-6 are not, an
  exit 2 with no error object is not, a kill is retried under its own budget.
- Preflight: binary present and at least 0.31.0, state location by deployment (a pod needs
  PostgreSQL state and refuses CDC), state directory checks, a warning for `notifications:`.
- Secrecy: XCom and the task log never carry the error message; raw stderr goes to a file.
- `slack_failure_callback` for the Slack provider: a one-line title per failed task,
  `dag.task — <rivet code> (final|retryable)` (else the exception's class), with the run id and
  the task-log link; with a bot token and `channel=` the error text (rivet's error line for the
  failed step, else the exception's message; through Airflow's secrets masker, capped) is a code
  block in its thread; with an incoming webhook, title and error text are one message. The task
  log says what was sent and what Slack answered. Setup guide `docs/slack.md`, test DAGs
  `examples/rivet_slack_alerts.py`.
- `connections=[conn_id, ...]` on every operator: storage and warehouse credentials from Airflow
  Connections of kind gcp, aws, azure, snowflake and clickhouse (the connection type, or the
  extra `rivet_kind`); key files are written `0600` into a private folder removed when the task
  ends, and an unknown kind or two Connections of one kind are refused before rivet starts.
  ("Storage and warehouse connections")
- Every fallback taken against a binary that lacks a contract feature is recorded as
  `degraded` in the task's result and listed in the README.

Changed before the first release, after review (see the README sections named):

- The task log is readable: one headline per task with the outcome first (`OK`, `SKIPPED`,
  `REFUSED`, `FAILED`, `CRASHED`, `STOPPED`, `BUSY`), counts, duration and run id; one line per
  rivet step; `note` lines for degradations and single-host SQLite. A successful task's lines
  were raw JSON and red in the Airflow UI (argv held `--json-errors`); argv and the JSON records
  are now at DEBUG. ("Reading the task log")
- A retry after the task process died past a successful `rivet apply` no longer applies the plan
  again: a `mode: full` export was extracted twice and an incremental one was refused
  `RIVET_PLAN_ARTIFACT_STALE` (a failed task, its load skipped). The task records the apply it
  starts beside the sealed plan and, on a retry, takes the run rivet's state shows for that
  process as its result (`rivet.apply.already_applied`). ("Retries")
- Two tries of one export no longer run rivet at once: every task but `plan` holds a per-export
  (per-table for a load with `--table`) lock file whose descriptor rivet inherits, so a rivet that
  outlives its task keeps it; a task that finds it held for `lock_wait` seconds starts no rivet and
  is retried (`RIVET_AIRFLOW_EXPORT_BUSY`). The degraded per-table load lock was freed when the task
  process died while its rivet ran on. On Linux rivet gets SIGTERM when its task process dies.
  ("Overlap")
- rivet's busy refusals `RIVET_STATE_RUN_IN_PROGRESS` and `RIVET_LOAD_TABLE_BUSY` (exit 5, "wait,
  then repeat") are retried instead of failing the task. ("Retries")
- **A DAG run with a failed task is a failed run.** Both builders end in a `watcher` task
  (`one_failed`). A failed `plan` blocks every extract; a failed CDC `run` blocks every load and
  compaction (`load.T` was `all_done`); extract tasks are `all_success` and ordering goes through
  no-op tasks (`apply.wave_<n>_done`, new `apply.after_<export>`). ("DAG builders")
- A multi-wave batch DAG no longer fails on Airflow 2.10 when an upstream published no XCom.
- A relative `query_file` stays relative and the file is copied beside the config's copy; it was
  rewritten to an absolute path, which rivet refuses. The copy is named
  `<name>.<hash of the source path>.yaml`. State or a CDC checkpoint left beside the original
  config is refused (`RIVET_AIRFLOW_STATE_BESIDE_CONFIG`, `RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG`,
  `RIVET_AIRFLOW_QUERY_FILE`). ("Local worker setup guide")
- The subprocess gets an allow-listed environment instead of the worker's; `env_passthrough`
  and `cloud_credentials` extend it. `env` is no longer a templated field. ("Secrets")
- Raw stderr is never written to the worker's own stderr; without a state directory it goes to
  a private temp directory (`stderr_temp`). Log files are `0600` in `0700` directories, under
  `logs/<dag_id>/<task_id>/`, and only the newest `log_keep` tries of a task are kept. ("Secrets")
- Exit 0 with a failed unit in the summary fails the task (`exit_zero_failed_unit`); a malformed
  error object or result entry falls back to the exit-code table (`result_malformed`) instead of
  raising; the error line is searched in the last 1 MiB of stderr, not the last 200 lines.
- The crashed-budget ledger fails closed (`crashed_ledger`) and is keyed by `max_tries`, so
  lowering `retries` cannot reset it. ("Retries")
- `RivetPlanOperator` swaps the layout file in only when a builder can read it.
- Every path component made from a DAG id, task id, run id or export name is unique to that
  name: a name that is not one lower-case word of safe characters gets a hash of the exact name
  appended. Two exports or two DAGs whose names differed only in replaced characters shared one
  sealed plan, so one export applied the other's plan and was never extracted. `apply` also
  refuses an artifact that records another export (`RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN`).
  ("Local worker setup guide")
- A sealed plan belongs to one task, one config and one export: its name carries the task id
  and a digest of the config's path, the config's bytes and the export's query file. Two apply
  tasks of one DAG run with two configs and one export name shared `plans/<run>/<export>.json`,
  so the second applied the first config's plan and was never extracted. `apply` refuses an
  artifact whose `export_name` or `config_path` is missing or not the task's own, expired or
  not. The scratch file of `RivetPlanOperator` is no longer shared by two DAGs that refresh
  one layout file. ("Local worker setup guide")
- A task's log folder is `logs/<dag_id>/<task_id>-<hash>/` for every task id. Retention looks
  only there: files of an older layout are never pruned or deleted (on a file system that
  ignores case, DAG `sales` removed log files of DAG `Sales`). ("Upgrading a state directory")
- A `RIVET_STATE_URL` in the worker's environment that rivet would use (it starts with
  `postgres`) and that the task would not pass to rivet is refused before rivet starts
  (`RIVET_AIRFLOW_STATE_ENV_NOT_PASSED`). A declared value, also an empty one, is not refused.
- The README said a Snowflake or ClickHouse compact task "is skipped"; on rivet 0.31.0 it ends
  `compacted` having merged nothing. It now says so, documents `build_cdc_dag(compact=False)` for
  those warehouses, and names every operator and builder parameter (a test holds the lists to the
  signatures). ("Storage and warehouse connections", "DAG builders", "Operators")
- `build_batch_dag(exports=[])` raises `ValueError`; the watcher carries no failure callback, so
  a callback in `default_args` fires once per failed task.
- The config is read as rivet reads it. `${name}` is resolved from `-p` and the task's env before
  the guards look at `mode`, `name` and `cdc.checkpoint` (a templated CDC mode passed the
  CDC-on-pod guard; a templated checkpoint was looked for under its literal name); an unresolved
  mode counts as `cdc`, an unresolved name or checkpoint is refused
  (`RIVET_AIRFLOW_CONFIG_UNRESOLVED`). Scalars follow serde_yaml_ng: `08` / `09` no longer crash
  the preflight, `017` is not 15, and `name: 2024` is found. The single-export copy for load /
  compact is cut out of the original text instead of re-serialised, so a quoted `'${SECRET}'`
  keeps its quotes; a text that cannot be cut safely is refused
  (`RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE`). (D06, D07, D09)
- `connection_url`: a password without a login, IPv6 hosts, Oracle `service_name`, an encoded
  database name. The version has one source, `airflow_provider_rivet/__init__.py`.
- `slack_failure_callback` on a mapped task reports its own index's code and class; it said "no
  rivet result was published" because the XCom pull returned every index's payload.
- DAG builders: every rivet task is `all_success` whatever `default_args` says, and a
  `trigger_rule` in `operator_kwargs` is refused (with `all_done`, load and compact ran after a
  refused drain); user `default_args` are merged over the builders' `retries=2`; a bare string
  for `exports`, `compact_exports` or `tables` is refused; a name that is no ASCII task id
  (`daily orders`, `orders:v2`, `заказы`) gets a sanitised task id with a hash and keeps its real name;
  in a wave, each heavy export runs alone and the cheap ones after them, like `rivet apply`.
  ("DAG builders")
- On Airflow 3 the exceptions are imported from `airflow.sdk.exceptions`, not the deprecated
  `airflow.exceptions`.
- A clear of a finished `RivetApplyOperator` plans again instead of replaying a sealed plan whose
  cursor the successful apply already moved (rivet refused it `RIVET_PLAN_ARTIFACT_STALE` on
  every clear for 24 h): the artifact is named by the try series (`max_tries`). (D04)
- `RivetPlanOperator` and `build_batch_dag` read a plan of one export, which rivet prints as a
  bare object with no campaign; it was a `KeyError` and an `internal` failure. (D10)
- The sealed plan artifact is mode `0600` whatever rivet's umask, as the README said. (D38)
- Expired plan artifacts and crashed ledgers untouched for 24 h are pruned. (D33)
- An apply whose counts come from `rivet metrics --json` reports the row's status: a
  `skip_empty` export that delivered nothing is `skipped` with its `skip_reason`. (D31)

The environment built from Connections, after the 2026-10 bug hunt (`connections.build_environment`
is now the one place rivet's environment is assembled; README "Storage and warehouse connections"):

- A storage Connection that supplies a credential replaces the worker's variables of its kind
  (`cloud_credentials`, `env_passthrough`): the worker's `AWS_SESSION_TOKEN` no longer rides beside
  a Connection's static keys, nor its Azure account key beside a Connection's SAS token. (D12)
- An AWS Connection with `role_arn`, `assume_role_*`, `profile_name` or keys in extras is refused
  (`RIVET_AIRFLOW_CONNECTION_UNSUPPORTED`) instead of running as the worker's identity. (D13)
- Snowflake: with a private key, the Connection's password reaches `snow` as
  `PRIVATE_KEY_PASSPHRASE` (D14); a base64 `private_key_content` is decoded to PEM, and content
  that is neither is refused (D15); the extra `region` is written to the `snow` connection (D19);
  `config.toml` is written as TOML, so a non-BMP or control character in any field reads back
  unchanged (D20).
- `connection_url` maps the standard extras with a URL form (PostgreSQL `sslmode`, MongoDB `srv`,
  `authSource`, ...) and refuses `iam`, `redshift`, `unix_socket` (D21); only a real IPv6 literal
  is bracketed, with its zone encoded as `%25` (D23).
- A ClickHouse Connection without a host is refused (`RIVET_AIRFLOW_CONNECTION_INCOMPLETE`)
  instead of becoming `http://None`. (D22)
- A rivet child whose task process died before the parent-death signal took effect stops itself with SIGKILL;
  its SIGTERM ran the inherited Airflow 3 handler, which returns, and the child went on to run rivet as an orphan.
- A database Connection without a host is refused (`RIVET_AIRFLOW_CONNECTION_INCOMPLETE`) like a ClickHouse one;
  it became `postgresql://u:p@/d` and the driver dialled its default socket or host. One check, in `url_host`.
- README: an Azure config reads `account_name: ${AZURE_STORAGE_ACCOUNT}`. (D32)
- A Connection id no backend defines (`env_from_connections`, `connections`) is refused,
  `RIVET_AIRFLOW_CONNECTION_MISSING`, with the refusal in XCom; it was retried with no XCom.
- A config that is not UTF-8, nests too deep, holds a scalar Python cannot convert, or whose
  `exports` is not a list of mappings with a scalar `name` (read as its text: `name: 2024` is the
  export `2024`), is refused `RIVET_AIRFLOW_CONFIG_UNREADABLE`; it crashed with a traceback and
  was retried. The Connection resolution reads the config through the same reader, with `${...}`
  resolved from `-p` and the task's env, so a templated `load.password_env` / `load.connection`
  names the variable rivet reads.
- A rivet binary whose `--version` or `<sub> --help` probe fails or times out, in the preflight
  or while argv is built, is refused `RIVET_AIRFLOW_BINARY_MISSING` with the refusal in XCom; it
  raised a raw `RuntimeError` / `TimeoutExpired` and was retried.
- `on_kill` only sends SIGTERM; the step that started rivet reaps it and kills it after 30 s.
  It used to wait inside Airflow's SIGTERM handler on the lock the interrupted `wait` held, so
  the task hung until the supervisor's SIGKILL and its secrets folder was never removed.
- A stop Airflow requests while no rivet process runs (between `rivet plan` and `rivet apply`, or
  before the first step) keeps every later step from starting: the task fails with a retryable
  error and `stopped_before` in XCom. On Airflow 3, whose SIGTERM handler returns, the next step
  used to start rivet and run it to completion after the stop. (D40)
- rivet's stderr is cut into lines at `\n` only: a `--json-errors` line whose message holds
  U+2028, U+2029 or U+0085 was cut apart, so a retryable failure lost its code and its retry.
  A stderr line that starts with `{` and is not JSON is recorded as `result_malformed`.
- A summary file that is missing (`result_missing`) or not a JSON object (`result_malformed`)
  after an exit-0 process is recorded in `degraded`. ("Works today / needs a newer rivet")
- Live gate (`python -m gate`, not part of the package): the warehouse leg is graded by values a
  second client reads back (google-cloud-bigquery, snowflake-connector-python, ClickHouse HTTP)
  against the source, per warehouse, mode and table; it printed an inventory and graded nothing.
  Only the BigQuery batch compaction may not be skipped; the cloud cleanup lists again and fails
  on a leftover; the Snowflake sweep drops only the gate's own schema shape, quoted; the run tag
  names the compose project and image; the `Live gate` workflow takes the `-glibc2.28` asset or
  the ghcr.io image, which start on Debian 12.
