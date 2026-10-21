# Retries

How rivet's exit and error object become an Airflow success, retry or stop.

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
| code `RIVET_STATE_RUN_IN_PROGRESS` (rivet 0.32.0 prints it with exit class 2) or `RIVET_LOAD_TABLE_BUSY` (exit 5) (another rivet holds the export's run lease or the table's load lease; rivet's remedy is "wait, then repeat") | `refusal`, `retryable: true` whatever the line says | `AirflowException`: retried under `retries`; the headline says `BUSY` |
| code `RIVET_STATE_LOCK_TIMEOUT` (rivet >= 0.32.2, kind `busy`, exit 2: rivet waited `--lock-wait` seconds for a lease another rivet process holds and gave up; the same end as `RIVET_AIRFLOW_EXPORT_BUSY`, inside rivet) | `retryable`, `retryable: true` whatever the line says | `AirflowException`: retried under `retries`; the headline says `BUSY`, the exception names the export |
| code `RIVET_LOAD_BLOCK_MISSING` (rivet >= 0.32.2, kind `usage`, exit 1: load or compact on a config with no `load:` block) | `generic` | `AirflowFailException`: no retry; add the block |
| code `RIVET_SOURCE_CDC_CHECKPOINT_SHARED` (rivet >= 0.32.2, exit 5: another config's export owns the checkpoint, `<checkpoint>.owner`) | `refusal` | `AirflowFailException`: no retry; give the export its own `cdc.checkpoint` |
| code `RIVET_INIT_CDC_NAME_TAKEN` (rivet >= 0.32.2, exit 5; `rivet init` only, which no operator runs) | `refusal` | `AirflowFailException`: no retry |
| the export's lock stayed held for `lock_wait` seconds ([Overlap](limits.md#overlap)) | `busy: RIVET_AIRFLOW_EXPORT_BUSY` in XCom; rivet is not started | `AirflowException`: retried under `retries` |

The exception text is `[code] class: action (export)`. It never contains rivet's error message.

**The crashed budget across tries.** Airflow clears a task instance's XCom at the start of every
try, so the count cannot live there. On a local worker with SQLite state the operator keeps a small ledger file,
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
The same holds on PostgreSQL state, where tries may run on different hosts and a ledger on one
host's disk would grant the budget again on the next (`crashed_ledger` in `degraded`).

Limit: the crashed budget can only shorten Airflow's own budget, never extend it. With
`retries=0` Airflow retries nothing, a crash included. The builders default to `retries=2`.

The operators never run `rivet state reset-chunks` before a retry: a crashed checkpointed run is
resumed by the next process.

**A retry of an apply that already succeeded.** The task process can die after `rivet apply` exited
0 and before Airflow recorded the success (OOM of the task runner, a pod evicted, a zombie). The
retry is a try of the same series, so it replays the same sealed plan, and rivet (>= 0.32.0, the
minimum) answers it: a point-in-time plan (`mode: full`) it already applied is not extracted again,
the summary names the run that applied it (`status: already_applied`, `answered_by_run`), and the
task logs `SKIPPED: an earlier try already applied this plan` (record
`rivet.apply.already_applied`) and ends success with that run's `run_id`, `rows` and `files`. An
incremental plan whose own apply moved the cursor is not refused `RIVET_PLAN_ARTIFACT_STALE`
(rivet 0.31 refused it, a failed task and a skipped load): the replay reads from the moved cursor
and finds only rows that arrived since, so nothing is extracted twice. A cursor another process
moved is still refused STALE. An apply that failed or was killed is replayed and runs as before.
