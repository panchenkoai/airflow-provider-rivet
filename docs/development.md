# Development

Running the tests, mutation testing and the live gate.

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
| a pull request changes `airflow_provider_rivet/` code | the mutants of every function it touches | one that mutates a line the pull request changed is not killed (the other survivors of those functions are listed, not failed) |
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

**Source legs.** `--sources mysql` (needs `--warehouses` with `bigquery`) adds another source engine beside the
postgres one: a MySQL 8.4 container in the stack (row binlog with `binlog_row_metadata=FULL`, GTIDs, utf8mb4) with
the same tables in MySQL types (`DATETIME(6)` and `TIMESTAMP(6)` cursors, `DECIMAL(12,2)`, Cyrillic text), the same
three cycles, and two DAGs: incremental batch and one CDC stream, each extracted to GCS and loaded and compacted into
BigQuery from the `rivet_gate_mysql` and `gate_gcp` Connections. Every rivet config of the leg is the one
`rivet init` writes against the live source inside the stack (`--mode incremental` / `--mode cdc`, `--gcs-bucket`,
`--bigquery-project`, `--bigquery-dataset`), used as written: init's prefixes are fixed (`exports/<table>/`, `cdc/`),
so the gate gives it a bucket of the run's own (`rivet-afgate-<tag>-<stack>-<engine>`), and since init names the
batch exports after their tables, the batch and CDC loads get a dataset each; the export and stream names the DAGs
run are read from what init wrote. init and the DAGs share the stack's TLS PostgreSQL state (`RIVET_STATE_URL`, the
`rivet_gate_state` Connection), so the primary keys init records are the ones the runs read, and nothing is left beside
the config (the leg fails if init leaves a `.rivet_state.db` there). rivet 0.31's `init` gives every MySQL stream the replica id `server_id: 4271`, so
the leg runs one CDC config per MySQL server. Oracles: each compacted BigQuery base, read by google-cloud-bigquery,
equals the source read by DuckDB's mysql scanner value by value (batch: with exactly the keys the cycle deleted left
over; CDC: without the rows flagged `__is_deleted`), and no `__changes` buffer is left. The cleanup deletes the bucket
and both datasets and looks again; the sweep drops gate-shaped buckets older than a day.

**Connection legs.** Each stack runs with passwords made for it (32 random hex digits for the source, state,
ClickHouse and metastore databases and the admin user) and a generated Fernet key, so no task log masks a word it
carries: the cells `hygiene/no-masked-words` (no `***` in any task log) and `hygiene/fernet-key-set` (no
`empty cryptography key` in the task or service logs) hold every stack to it. With the warehouse leg,
`--connections env` (the default) passes the Connections as `AIRFLOW_CONN_*` variables in a `0600` env file, the
worst place (see [Connections and secrets](connections-and-secrets.md#connections-and-secrets-recommended-setup)); `--connections gsm` puts each one in Google Secret Manager as
`airflow-connections-afgate_<tag>_<stack>_<conn_id>` and points the stack's `[secrets] backend` at it, with no
`AIRFLOW_CONN_*` variable left. The backend runs as the service account `--gsm-reader` (default
`afgate-secrets-reader@<project>.iam.gserviceaccount.com`, which holds `secretmanager.secretAccessor` only on
names starting `airflow-connections-afgate_`), impersonated from the credential file `--gcp-keyfile` mounted
read-only; the cells `connections/<conn_id>/from-secret-manager` require the variable absent from every Airflow
container (`docker inspect`), no metastore row, the secret listed, and `airflow connections get` in the worker
resolving the published fields. `--gcp-auth adc` leaves `gate_gcp` with only `project` and mounts the same
file read-only at the containers' ADC path, so rivet runs as that identity (`connections/gate_gcp/keyless`).
Local only: that file is the operator's own gcloud login, wider than a worker's identity; on GKE or Composer it is
Workload Identity and no file exists. The cleanup deletes the stack's secrets and lists again; the sweep drops
gate-shaped secrets older than a day. One-time setup of the project for `gsm`:

```bash
gcloud services enable secretmanager.googleapis.com iamcredentials.googleapis.com --project rivet-data-tool
gcloud iam service-accounts create afgate-secrets-reader --project rivet-data-tool
gcloud projects add-iam-policy-binding rivet-data-tool \
  --member serviceAccount:afgate-secrets-reader@rivet-data-tool.iam.gserviceaccount.com \
  --role roles/secretmanager.secretAccessor \
  --condition='title=afgate-connections-only,expression=resource.name.startsWith("projects/1034205769011/secrets/airflow-connections-afgate_")'
gcloud iam service-accounts add-iam-policy-binding afgate-secrets-reader@rivet-data-tool.iam.gserviceaccount.com \
  --member user:<you> --role roles/iam.serviceAccountTokenCreator
```

rivet's own release gate does not run this gate: rivet has no hook for it. The `Live gate` workflow here runs it
against a released binary, on demand and on an `airflow-v*` tag; a release older than rivet 0.32.0 (the first with
`apply --summary-output` and the `already_applied` answer) is refused before the binary is downloaded.
