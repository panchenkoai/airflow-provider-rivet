# Overlap and known limits

How runs of one export are kept apart, and what the package cannot see.

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
export: rivet (>= 0.32.0) refuses the second holder with `RIVET_STATE_RUN_IN_PROGRESS` (exit class 2) /
`RIVET_LOAD_TABLE_BUSY` (exit 5), which the task retries ([Retries](retries.md)); a plain `mode: full` run takes the
run lease too (measured on 0.32.0).

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
  (see ["Storage and warehouse connections"](connections-and-secrets.md#storage-and-warehouse-connections)).
- With `load_table_filter` degraded, the per-table tasks of a multi-table CDC export each load
  the whole export; the first does the work, the others find the ledger up to date.
- rivet 0.32.0's apply summary carries no `skip_reason`: a `skip_empty` apply that delivered
  nothing is `SKIPPED` without the reason.
- A failure callback runs where Airflow runs task callbacks, in the task's process on the worker;
  exercised live on Airflow 2.10.5 and 3.3.2 by hand (docs/slack.md), not by the live gate.
