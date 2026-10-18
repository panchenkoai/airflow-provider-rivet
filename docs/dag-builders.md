# DAG builders

`build_batch_dag` and `build_cdc_dag` build whole DAGs from the operators.

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
  ClickHouse, where there is nothing to compact (see ["Storage and warehouse connections"](connections-and-secrets.md#storage-and-warehouse-connections)).
- `operator_kwargs` go to every task, as in the batch builder.
