# airflow-provider-rivet

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/panchenkoai/airflow-provider-rivet/airflow-v0.1.0/docs/assets/provider-flow-dark.svg">
    <img src="https://raw.githubusercontent.com/panchenkoai/airflow-provider-rivet/airflow-v0.1.0/docs/assets/provider-flow.svg" alt="Apache Airflow runs rivet as tasks: PostgreSQL, MySQL, SQL Server, MongoDB and Oracle → rivet → S3, GCS, Azure Blob or local disk → BigQuery, Snowflake or ClickHouse" width="900">
  </picture>
</p>

<p align="center">
  <a href="https://github.com/panchenkoai/airflow-provider-rivet/blob/main/airflow_provider_rivet/__init__.py"><img src="https://img.shields.io/badge/version-0.2.0%20alpha-orange" alt="version"></a>
  <a href="https://github.com/panchenkoai/airflow-provider-rivet/blob/main/pyproject.toml"><img src="https://img.shields.io/badge/python-3.12%20%7C%203.13%20%7C%203.14-blue" alt="python"></a>
  <a href="https://github.com/panchenkoai/airflow-provider-rivet/blob/main/pyproject.toml"><img src="https://img.shields.io/badge/airflow-2.10%2B%20%7C%203.x-017CEE" alt="airflow"></a>
  <a href="https://github.com/panchenkoai/airflow-provider-rivet/actions/workflows/ci.yml?query=branch%3Amain"><img src="https://github.com/panchenkoai/airflow-provider-rivet/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="https://github.com/panchenkoai/airflow-provider-rivet/blob/main/LICENSE"><img src="https://img.shields.io/github/license/panchenkoai/airflow-provider-rivet" alt="license"></a>
</p>

Apache Airflow operators and DAG builders that run the [rivet](https://github.com/panchenkoai/rivet)
binary on the worker as a subprocess and turn its result into Airflow's: retry or stop, which
unit failed, and what to do about it. The contract it implements is
[ADR-0039](https://github.com/panchenkoai/rivet/blob/main/docs/adr/0039-scheduler-contract.md).

rivet itself (sources, destinations, warehouses, CDC, configs): **<https://github.com/panchenkoai/rivet>**.

## How it works

- **A task runs the rivet binary on the worker**, as a subprocess with a minimal environment; no
  rivet code runs inside Airflow.
- **Plan, then apply.** `RivetApplyOperator` seals a plan (`rivet plan`) and applies it
  (`rivet apply`); a retry replays the same sealed plan, a cleared task plans again. `load` and
  `compact` tasks follow per table.
- **rivet's exit becomes Airflow's decision**: success, a retry for what may pass by itself, a
  final stop for a refusal. The code, class and action go to XCom and to a short block in the
  task log; rivet's raw output stays in files.
- **DAG builders** make the whole batch DAG (plan, apply, load, compact, in waves) or the CDC DAG
  (one bounded drain, then load and compact per table) from a config.
- **State** lives in a declared directory on the worker (`state_dir`, SQLite) or in PostgreSQL
  (`RIVET_STATE_URL`); the operator refuses a missing or mismatched one instead of starting from
  empty state.
- **Credentials come from Airflow Connections** (databases, storage, warehouses), resolved inside
  the task; nothing secret is in argv, XCom or the task log.

Status: alpha. It is versioned independently of the binary and checks
the binary's version at run time (minimum **rivet 0.32.2**; a lower one is refused with both
versions in the message). Supported: Airflow 2.10+ and 3.x, Python 3.12+ (Airflow 2.10 only on 3.12).

## Install

```bash
pip install airflow-provider-rivet            # or airflow-provider-rivet[slack]
```

Install it into the Airflow environment with Airflow's own constraints file, so it does not move
Airflow's pins. It depends on [`rivet-cli`](https://pypi.org/project/rivet-cli/), which installs the
rivet binary (wheels for Linux glibc 2.28+ x86_64/aarch64 and macOS), so the official Airflow images
need nothing else:

```dockerfile
FROM apache/airflow:3.3.2
RUN pip install --no-cache-dir airflow-provider-rivet
```

The operators run `rivet` from the worker's `PATH`, else the binary `rivet-cli` installed into
Airflow's environment. To run another build, pass `rivet_bin="/abs/path/rivet"` (for example one
copied from `ghcr.io/panchenkoai/rivet:v0.32.2`).

## Quickstart

A daily PostgreSQL → Parquet DAG on one worker, with SQLite state. Run the commands on the worker
(or in the image) where Airflow runs.

1. **Generate the config** from the live database with `rivet init` (the `rivet` that `rivet-cli`
   installed; 0.32.2 or newer for `RIVET_STATE_URL=sqlite:`), and keep its state in the directory
   the DAG will use. The directory must exist, on persistent storage, at the same path on every
   worker:

   ```bash
   mkdir -p /var/lib/rivet/postgres
   export RIVET_PG_URL='postgresql://user:password@db-host:5432/app'
   RIVET_STATE_URL=sqlite:/var/lib/rivet/postgres/.rivet_state.db \
     rivet init --source-env RIVET_PG_URL --include 'users' 'orders' -o /opt/pipelines/postgres.yaml
   ```

   Review the file: one export per table, `mode`, `destination`. Without `RIVET_STATE_URL`, init
   leaves `.rivet_state.db` beside the config and the operator refuses to start from another one
   (`RIVET_AIRFLOW_STATE_BESIDE_CONFIG`).
2. **Add the Connection** the task builds `RIVET_PG_URL` from (type `postgres`; `generic` is
   refused):

   ```bash
   airflow connections add rivet_postgres --conn-uri 'postgres://user:password@db-host:5432/app'
   ```
3. **Write the DAG.** The builders default to `schedule=None` (manual runs); pass your own:

   ```python
   from datetime import datetime

   from airflow_provider_rivet.dags import build_batch_dag

   dag = build_batch_dag(
       "rivet_postgres",
       config="/opt/pipelines/postgres.yaml",
       state_dir="/var/lib/rivet/postgres",
       exports=["users", "orders"],
       load=False,  # no warehouse yet: True adds load tasks, which need a `load:` block
       schedule="@daily",
       start_date=datetime(2026, 10, 1),
       catchup=False,
       operator_kwargs={"env_from_connections": {"RIVET_PG_URL": "rivet_postgres"}},
   )
   ```
4. **Trigger it.** Each task logs one line, e.g.
   `rivet apply users → OK: 300 rows, 1 file in 0.3 s (run users_20261020T165708.134_54927)`.
   The Parquet lands in the config's `destination`; a relative `path` such as init's
   `./output/users/` is resolved by rivet against the directory of the config it reads, which under
   the operator is `state_dir`: `/var/lib/rivet/postgres/output/users/`. Write an absolute path to
   put it elsewhere.

If the worker has an older `rivet` on its `PATH`, the operator refuses it with both versions
(`RIVET_AIRFLOW_BINARY_TOO_OLD`); remove it or pass `rivet_bin=`. Several workers, Kubernetes pods
or a shared state: see [State](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/state.md).

## Documentation

The same pages, with the command builder: https://panchenkoai.github.io/airflow-provider-rivet/

| Topic | |
|---|---|
| [Quickstart](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/quickstart.md) | Docker Compose with Airflow 2.10 or 3.3, demo MySQL and PostgreSQL, MinIO and ClickHouse: CDC and batch pipelines in minutes, then your own database ([command builder](https://panchenkoai.github.io/airflow-provider-rivet/your-database.html)) |
| [Add your own database](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/your-database.md) | the commands that add your MySQL or PostgreSQL database as a pipeline of the quickstart stack; with the form that writes them on the [docs site](https://panchenkoai.github.io/airflow-provider-rivet/your-database.html) |
| [Operators](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/operators.md) | each operator, its parameters, the XCom value, reading the task log |
| [DAG builders](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/dag-builders.md) | `build_batch_dag`, `build_cdc_dag`: tasks, waves, trigger rules, the `watcher` |
| [Retries](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/retries.md) | exit status → success, retry or stop; the crashed budget; a retry of an apply that succeeded |
| [State](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/state.md) | state by deployment, the local worker setup guide, moving to PostgreSQL |
| [Recipe: CDC from cron to Airflow](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/cron-to-airflow-cdc.md) | a MySQL CDC stream run by cron moved to a DAG with PostgreSQL state, one stream per source, no new baseline |
| [Connections and secrets](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/connections-and-secrets.md) | the subprocess environment, database / storage / warehouse Connections, where to keep them |
| [Slack alerts](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/slack.md) | the failure callback and its setup guide |
| [Capabilities](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/capabilities.md) | what works with today's rivet and what waits for a newer one |
| [Overlap and known limits](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/limits.md) | locks between runs of one export; what the package cannot see |
| [Development](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/development.md) | tests, mutation testing, the live gate |
| [Changelog](https://github.com/panchenkoai/airflow-provider-rivet/blob/main/CHANGELOG.md) | |
