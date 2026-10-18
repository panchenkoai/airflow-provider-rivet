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
| exit 5 with code `RIVET_STATE_RUN_IN_PROGRESS` or `RIVET_LOAD_TABLE_BUSY` (another rivet holds the export's run lease or the table's load lease; rivet's remedy is "wait, then repeat") | `refusal`, `retryable: true` whatever the line says | `AirflowException`: retried under `retries` |
| the export's lock stayed held for `lock_wait` seconds ([Overlap](limits.md#overlap)) | `busy: RIVET_AIRFLOW_EXPORT_BUSY` in XCom; rivet is not started | `AirflowException`: retried under `retries` |

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
