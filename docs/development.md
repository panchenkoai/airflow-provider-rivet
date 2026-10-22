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
| on demand (workflow_dispatch) | the full run | as above |

There is no full run on push to main and no nightly run. Every run is split into ten parallel shards (the mutants
whose number ends in 0 … 9). Each shard prints `shard N: M of TOTAL mutants`, its progress as `shard N: done/M` and
`shard N: tested T, killed K, not killed S`. The ratchet job prints the per-shard table and fails unless the shards
graded every mutant exactly once (`sum(tested) == total`) and `survivors N <= ratchet M`. A survivor of a function
every test passes through (`_Session.step`) runs the whole suite, so one runner would need most of a day.

Moving the ratchet: it changes only through a full run on demand.
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
`--sabotage drop-delta-file` or `warehouse-value`, `--tag` (the run tag), `--destinations s3` (below), `--workers 2`
(the multi-host leg below), `--state compose-postgres`, `--migrate-from <rivet 0.32.1>` and `--client-override <dir>`
(the client stack, below).

**Known reds.** A cell red by a confirmed product defect is listed in `gate/scenario.py` `KNOWN` with its issue: it
grades `XFAIL` (counted apart, the run still passes) while red and `XPASS` once it passes, which fails the run until the
entry is removed. Only a defect with an issue and a planned fix goes there; anything else stays a FAIL.

**Multi-host leg.** `--workers 2` runs, instead of the scenario above, a stack with two Celery workers, each a host
with a disk of its own (`hosts/a` and `hosts/b` of the stack's directory, each mounted at `/opt/rivet-host` in one
worker only): every `state_dir` (the config copy, sealed plans, locks, crashed ledger, logs, marker) is on the host
that ran the task. rivet's state is the stack's TLS PostgreSQL (`RIVET_STATE_URL` from the `rivet_gate_state`
Connection; the CDC config's from `rivet_gate_state_mh_cdc`, since init names its baseline exports after the tables,
as the batch ones), and the files go to GCS, which both hosts reach: each config is the one `rivet init` writes against
the live source (`--mode incremental` / `--mode cdc`, `--include` the two tables, `--gcs-bucket` a bucket of the run's
own, `rivet-afgate-<tag>-<stack>-mh*-batch|cdc`), used as written, with the `gate_gcp` Connection (`--gcp-keyfile`,
`--bq-project`, `--bq-location` as in the warehouse leg). Every task names a queue (`gate_a`, `gate_b`, `gate_cdc`)
and the gate moves each queue to one worker per cycle with Celery's `add_consumer` / `cancel_consumer`, so the two
extracts of one batch run land on both hosts and every export and the stream change host between cycles; each try's
`hostname` from the REST API proves it (`.../hosts`). Four cycles (initial, delta, idle, and `retry`: a delta whose
orders extract pauses at `pg_after_snapshot_open` on host a, is SIGKILLed there, and is retried on host b, where its
sealed plan is not). Oracles, reading the buckets through `gcloud storage` and the source through DuckDB: the batch
delta and latest-version cells of the base scenario, each `(id, cursor)` version written once across both hosts
(`each-version-once`), the stored cursor (psql) equal to the source high-water and never moved back
(`cursor-not-regressed`), XCom rows == file rows; for the stream: the baseline once, each event `(__pos, __seq)` once,
replay == source, exact operation counts. `multihost/budget` kills the paused extract on a and its retry on b:
`crashed_retries=1` must not grant the second crash (it does today: each host keeps its own crashed ledger).
`multihost/chain/load-on-other-host`: an extract on a and its load on b, on PostgreSQL state, must run rivet rather than
be refused for the other host's state_dir marker. Printed as `MEASURE` lines, not graded: the same batch config on
SQLite state in each host's state_dir, and init's default local `./output/` destination (which lands in the state_dir)
on PostgreSQL state. The cleanup deletes the buckets and looks again.

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
that DAG may not skip, on the delta cycle (the first load creates the base from its pass, rivet #652, and the idle load
buffers nothing, so there a skip is right). `--sabotage warehouse-value` adds a cent to one BigQuery row after the delta load and must turn
it red. The cleanup lists the dataset, schema and GCS prefix again and fails on anything left; the sweep of runs older
than a day drops only names of the gate's own shape.

**Source legs.** `--sources mysql` (needs `--warehouses` with `bigquery`) adds another source engine beside the
postgres one: a MySQL 8.4 container in the stack (row binlog with `binlog_row_metadata=FULL`, GTIDs, utf8mb4) with
the same tables in MySQL types (`DATETIME(6)` and `TIMESTAMP(6)` cursors, `DECIMAL(12,2)`, Cyrillic text), the same
three cycles, and two DAGs: incremental batch and one CDC stream, each extracted to GCS and loaded and compacted into
BigQuery from the `rivet_gate_mysql` and `gate_gcp` Connections. Every rivet config of the leg is the one
`rivet init` writes against the live source inside the stack (`--mode incremental` / `--mode cdc`, `--gcs-bucket`,
`--bigquery-project`, `--bigquery-dataset`, `--include` with the scenario's tables), used as written: init's prefixes are fixed (`exports/<table>/`, `cdc/`),
so the gate gives each config a bucket of the run's own (`rivet-afgate-<tag>-<stack>-<engine>-<batch|cdc>`: SQL
Server's CDC exports write under `exports/<table>/cdc/`, inside the batch export's prefix, which `rivet load` lists
recursively), and since init names the batch exports after their tables, the batch and CDC loads get a dataset each; the export and stream names the DAGs
run are read from what init wrote. Each config has a TLS PostgreSQL state database of its own in the stack
(`RIVET_STATE_URL`, the `rivet_gate_state_<engine>_<batch|cdc>` Connection) that init and its DAGs share, so the primary
keys init records are the ones the runs read, nothing is left beside the config (the leg fails if init leaves a
`.rivet_state.db` there), and two configs whose exports share a name (SQL Server's init names its CDC exports after
their tables, as the batch ones) do not share their progress. rivet 0.31's `init` gives every MySQL stream the replica id `server_id: 4271`, so
the leg runs one CDC config per MySQL server. `--sources mssql` is the same leg on SQL Server 2022 (`linux/amd64`,
SQL Server Agent on for the CDC capture job, `READ_COMMITTED_SNAPSHOT` on as rivet asks of an incremental read; init
with `--tls require`, SQL Server encrypts every login): `DATETIME2(7)`
cursors, a `DATETIMEOFFSET(7)` at a non-UTC offset, `DECIMAL(12,2)`, Cyrillic `NVARCHAR`, `UNIQUEIDENTIFIER` and
`VARBINARY`. SQL Server captures per table, so its init writes one CDC export per table and the leg runs one CDC DAG
each (`gate_src_mssql_cdc_<export>`), loaded into the folded `dbo_<table>`; one table's capture instance has a non-default name (`gate_orders_v2`), which init must read from
`cdc.change_tables` (rivet 0.32.0; the gate gives an older rivet the default name), and the changes wait for the
capture job before a CDC run reads. Both engines run with `--sources mysql,mssql`. Oracles: each compacted BigQuery
base, read by google-cloud-bigquery, equals the source value by value (read by DuckDB's mysql scanner, or as `FOR JSON`
through `sqlcmd`, an instant cut to the microseconds rivet keeps; batch: with exactly the keys the cycle deleted left
over; CDC: without the rows flagged `__is_deleted`), and no `__changes` buffer is left. The cleanup deletes both buckets
and both datasets and looks again; the sweep drops gate-shaped buckets older than a day.

**The MySQL client stack** (MySQL → GCS → BigQuery, `--warehouses bigquery --sources mysql`) adds, on init's configs as
written: `initial/mysql/<batch|cdc>/load-block-as-written` (init's `load:` block is BigQuery, `layout: base_buffer`,
`cleanup_source: true`); for every rivet task of each cycle (`run`, `apply.*`, `load.*`, `compact.*`), its XCom unit
(decision `success`, every unit done, the rows the gate made: a CDC run the cycle's change events, plus the baseline
on the first; a CDC load its table's; a batch extract or load the source rows past the previous high-water) and its task
log (the `rivet … →` headline says OK, or SKIPPED where every unit skipped, with the XCom's rows):
`<cycle>/<dag>/<task>/xcom-unit|task-log`; and `<cycle>/mysql/<kind>/staged-parts-cleaned` (no Parquet left in the
bucket once loaded). After the cycles, a column added while the CDC run is open (rivet #665): `hot-ddl-open` runs the
stream in `gate_src_mysql_cdc_hot` (the same config, export and state, paused 45 s at `cdc_after_open`); inside the pause
the gate reads the source (the run's read bound), runs `ALTER TABLE gate_orders ADD COLUMN note VARCHAR(40) NULL` and
writes rows that use it (`alter-inside-the-open-run` fails if the pause ended first). init's stream is bounded
(`until_current: true`), so that run delivers the writes before the ALTER, compared with the source as read in the
window; `hot-ddl-after` runs the stream and the batch export again: the CDC base equals the source value by value with
`note` (rows nobody changed after the ALTER read NULL, as at the source; `note-reached` fails when the source holds no
value of it), the batch base, whose `query:` init wrote with the columns of its day, equals the source on those. Then a
second CDC config on the first stream's checkpoint (rivet #664): init writes it in a directory of its own (so no sibling
names its checkpoint), and its DAG (`gate_src_mysql_cdc_shared`) runs in the first stream's `state_dir`, where the
operator's copies of both configs resolve init's relative `cdc.checkpoint` to one file (`one-checkpoint`). Its run must
stop on `RIVET_SOURCE_CDC_CHECKPOINT_SHARED`, exit 5, on Airflow's first try, nothing after it running
(`shared-checkpoint/mysql/cdc/refused-by-code`; when init refuses the config instead, that cell is init's
`RIVET_INIT_CDC_NAME_TAKEN`, exit 5, nothing written). `shared-checkpoint-after` then runs the first stream over three
more updates: its base equals the source value by value and its tasks moved those three rows. Last, git-sync: from the
first cycle on, the stream's DAGs read init's config, unchanged, through a git-sync checkout
(`configs/mysql_cdc_sync/repo -> rev-A`, the DAG's `config=` the path under `repo/`). After the other phases the gate
publishes a new revision as git-sync does (the config copied unchanged into `rev-B`, a new link renamed over `repo`,
`rev-A` removed), makes three updates and runs the stream: `git-sync/mysql/cdc/run-after-flip` (the run and its tasks
succeed and the run moved those rows) and `git-sync/mysql/cdc/current-equals-source` (every table's base equals the
source value by value). The config's copy in the `state_dir` must not change its name with the revision: a copy named by
the resolved path leaves the old copy owning the checkpoint, and the run is refused `RIVET_SOURCE_CDC_CHECKPOINT_SHARED`.
The cell `git-sync/mysql/cdc/stale-copy-of-old-provider-not-refused` puts, before the first cycle, the copy the previous
provider made (named by the sha256 of the resolved `rev-A` path, the same text, no `.owner` record) in the `state_dir`,
and requires the first stream run through `repo/` to succeed. Before the git-sync flip, `recreate/mysql/cdc/continues-
after-worker-recreate` runs `docker compose up -d --force-recreate airflow-worker` (under Celery the tasks run only in
the worker; the `state_dir` is the bind mount `<stack>/rivet:/opt/rivet`, so it outlives the container), makes seven
changes and requires the stream's next run to move exactly those (no gap, no duplicate) and its base to equal the source.
Before that, two overlapping runs of the stream: the hot DAG's run held inside rivet (its 45 s pause after it opened)
and a run of `gate_src_mysql_cdc_second` (the same config and state_dir, `lock_wait` 30 s, one retry after 60 s)
started beside it. `overlap/mysql/cdc/second-run-busy`: the held run goes through, the second's first try ends on a busy
code (`RIVET_AIRFLOW_EXPORT_BUSY`, the provider's export lock, or rivet's `RIVET_STATE_RUN_IN_PROGRESS` /
`RIVET_STATE_LOCK_TIMEOUT`, read from that try's task log) and its retry goes through, and the two moved each change once;
`overlap/mysql/cdc/current-equals-source`: BigQuery equals the source value by value.

**The client's own files.** `--client-override <dir> --airflow 2.10.2` runs the client's delivered compose override
(`docker-compose.rivet.yml`, `Dockerfile.rivet`, `env.rivet.example`, `README.md`, copied as delivered) between the
official 2.10.2 `docker-compose.yaml` (`gate/stand/airflow-2.10.2.yaml`, the official file unchanged, metadata on
postgres:13) and the gate's overlay: `docker compose -f docker-compose.yaml -f docker-compose.rivet.yml -f
docker-compose.gate.yaml`. Its state server `rivet-state` (postgres:16, its own volume, no port, no TLS), its
`rivet_state` Connection (`AIRFLOW_CONN_RIVET_STATE`, JSON with `rivet_params: sslmode=disable`), its `./rivet-state`
mount and its `GOOGLE_APPLICATION_CREDENTIALS=/etc/gcloud_cred.json` run as written; the MySQL stream's DAGs take its
README's kwargs (`env_from_connections` `RIVET_STATE_URL` -> `rivet_state`, `cloud_credentials=["gcp"]`, `state_dir`
`/opt/airflow/rivet-state/mysql`), and no GCP Connection exists (`--gcp-auth env-file`). What the gate adds on top, each a
gap in the delivered files: the image (its two PyPI pins are not published yet: `gate/stand/Dockerfile.client` does what
`Dockerfile.rivet` does with a `rivet-cli` wheel of the binary under test, laid out as rivet's own, and this provider's
wheel, named through the file's `RIVET_AIRFLOW_IMAGE`), the key file mounted at `/etc/gcloud_cred.json`, the
`RIVET_STATE_*` values, the MySQL source and its Connection, and a state database of its own on `rivet-state` for each of
the gate's other configs. Cells: `client-override/files/as-delivered` (the files ran byte for byte as copied),
`client-override/conn/env-json-connection` (the JSON variable in every worker, host `rivet-state`, `sslmode=disable`,
resolved by Airflow, the stream's state rows in `rivet_state`), `client-override/gcp/env-key-file` (the variable in every
worker, the key read-only, no GCP Connection, the stream's run and loads went through) and
`client-override/mysql/cdc/checkpoint-on-host` (`./rivet-state/mysql/cdc/<name>.ckpt`), beside every cell of the MySQL
leg and the migration. `MEASURE client-override/readme/step5-variables` says whether the `RIVET_STATE_*` variables its
README's step 5 reads are in the worker's environment.

**The client stack.** `--state compose-postgres` keeps the source legs' state where the client does: in a database of
its own (`rivet_state_<engine>_<kind>`), owned by a role of its own (`rivet_state`, LOGIN, its own password), on the
compose file's `postgres` server beside Airflow's metadata database `airflow`, reached through a Connection
(`postgresql://rivet_state:…@postgres:5432/<db>?sslmode=disable`: the compose `postgres` has no TLS, and rivet refuses a
remote state URL that names no `sslmode`, `RIVET_STATE_URL_TLS_REQUIRED`) the DAGs read with `env_from_connections={"RIVET_STATE_URL": …}`; init
runs with that `RIVET_STATE_URL`. `compose-pg/mysql/state-in-own-database` reads the server as its superuser: each state
database is owned by `rivet_state` and holds tables owned by it, and `airflow` holds none of rivet's tables (none of
their names, none owned by the role). The default (`--state gate`, the gate's TLS `gate-state`) is unchanged.

**The migration leg.** `--migrate-from <rivet 0.32.1>` (beside `--sources mysql`) mounts that binary at
`/opt/rivet/bin/rivet-0.32.1` and runs a MySQL CDC stream over a database of its own (`rivet_mig`) into a bucket and a
dataset of its own: its config is rivet 0.32.1's init (0.32.1 keeps SQLite beside `-o`, so init writes into a scratch
folder and the DAG reads the file copied unchanged). `gate_mig_mysql_sqlite` (0.32.1, SQLite at `<state_dir>/.rivet_state.db`,
`single_host`) runs the baseline and a delta; then, with changes made before the switch, `rivet state copy -c
<state_dir>/<config copy> ` (the target from `RIVET_STATE_URL`) fills the empty PostgreSQL state; `gate_mig_mysql_pg`
(this binary, the state Connection) runs after more changes, twice. Cells `migrate/mysql/cdc/*`: each run succeeded and
moved exactly the changes made since the run before (fewer is a gap, more a duplicate), BigQuery equals the source value
by value with the deletes applied, the PostgreSQL state holds the SQLite state's rows (per table) and cursors, and the
checkpoint file is byte for byte what 0.32.1 left, through the copy and the switch. The way back (`gate_mig_mysql_back`:
this binary on the SQLite file the copy left, whose load ledger knows nothing of the runs loaded on PostgreSQL) runs the
whole DAG and is graded as `wayback/mysql/cdc/*`: the run moved exactly the new changes, each table's load moved only
the runs it had not loaded (a PostgreSQL-era run loaded again is a duplicate), BigQuery equals the source, each base holds
every key once, and no `__changes` buffer is left; the bucket's staged objects before and after and each task's units
are printed as `MEASURE` lines. The incremental batch pipeline of the same tables goes the same way (rivet 0.32.1's init,
`gate_migb_mysql_sqlite` / `_pg` / `_back`): `migrate/mysql/batch/*` and `wayback/mysql/batch/*`, with what the extract
after the way back takes again measured (`MEASURE wayback/mysql/batch/re-exported`). The migration
database's tables are `mig_customers` / `mig_orders`: rivet refuses an unqualified CDC table another database of the
server also holds. Last of all, `unqualified/mysql/cdc/same-names-in-another-db` makes that case on purpose: a database
`rivet_dup` with the scenario's table names, then init's CDC config over `rivet` (its own bucket, dataset and state),
used as written, run once. It must run (init named `<db>.<table>`: BigQuery equals the source) or be refused by code at
exit 5 on Airflow's first try (panchenkoai/rivet#675, fixed in 0.32.2; earlier builds stop the run at exit 1). Every
command a client runs is printed as a `COMMAND` line.

`--sources mongodb` and `--sources oracle` (together: `--sources mongodb,oracle`) are the same leg where rivet can run
it, and a known-limit cell where it cannot. **MongoDB 7** runs as a one-member replica set (a change stream needs one)
with users and a key file, the member named `gate-mongodb:27017` so clients inside the stack reach the primary it
advertises; the collections hold ObjectId and int64 `_id`s, documents nested two deep with arrays, Decimal128, dates,
an int64 past 2^53, doubles, binary and nulls. init writes one CDC export per collection (`<collection>_cdc`, one
DAG each), loaded and compacted into BigQuery, where `document` is a JSON column. `init --mode incremental` is refused
(`RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED`: MongoDB's only batch mode is `full`), so the leg has no MongoDB batch DAG; the
cell `initial/mongodb/batch/known-limit` runs that init on every stack and fails when it does anything but that
refusal. **Oracle 23ai Free** (`gvenzl/oracle-free:23-slim-faststart`, rivet's own stand image, arm64 too) gets
LogMiner's prerequisites as rivet's stand sets them (`dev/oracle/init/02-logminer.sh`: ARCHIVELOG, minimal and per-table
ALL COLUMNS supplemental logging); its tables hold `NUMBER(19)` identity keys, `NUMBER(12,2)`, a `TIMESTAMP(6) WITH
TIME ZONE` at `+03:00` (the delta's at `-05:30`), Cyrillic `NVARCHAR2`, `RAW` and `BLOB`, with `TIMESTAMP(6)` cursors.
init names Oracle's batch exports after the upper-case tables (`GATE_ORDERS`); they run as incremental batch, loaded
and compacted. Oracle CDC cannot feed a `load:` block (rivet's oracle.md, *Known limits*), and `init --mode cdc` writes
the config without one: `initial/oracle/cdc/known-limit` fails when init writes a `load:` block (the leg must then load
and compact Oracle CDC). Oracles: MongoDB is read by pymongo, each document brought to one canonical text (every type
tagged: ObjectId, Decimal128 with its digits, a double apart from an integer, a date at its millisecond, binary with
its subtype) and compared with BigQuery's `document` read back as extended JSON by pymongo's own parser, beside the
`_id` text; Oracle is read by python-oracledb (exact decimals, LOBs as bytes, the zoned instant as `TO_CHAR` text with
its offset).

**Destination legs.** `--destinations s3` (needs no cloud) adds a MinIO container to the stack and runs the postgres
scenario to S3 on the configs `rivet init --s3-bucket <bucket> --s3-region us-east-1` writes, used as written: an
extract-only half (incremental batch and the CDC stream, `gate_s3_batch` / `gate_s3_cdc`) and a load half that adds
`--clickhouse-url http://gate-clickhouse:8123 --clickhouse-database gate_s3_<batch|cdc> --clickhouse-user loader` and
runs `load` and `compact` into the stack's ClickHouse from the S3-staged parts (`gate_s3_ch_batch` / `gate_s3_ch_cdc`).
Both halves read `gate-source`: init names each CDC config's replication slot after its `-o` file (rivet #639), so each
config is written to a file of its own name (`configs/s3_<kind>/s3_<kind>.yaml`), and since rivet #664 a CDC config's
parts sit under `<stem>/cdc/`, which the gate reads from the config. init writes `cleanup_source: true` in every
`load:` block (the staged parts are deleted once loaded), so only the extract-only half's Parquet is left to read. The S3 credential and the endpoint reach rivet
only through an Airflow `aws` Connection (`rivet_gate_s3`: login and password, extras `region_name` and `endpoint_url`
`http://gate-minio:9000`, as `AWS_ENDPOINT_URL`, which rivet's S3 client reads when the config names no `endpoint`);
init writes neither an endpoint nor a key variable. Each config has a TLS PostgreSQL state of its own
(`rivet_gate_state_s3_<kind>`). Oracles: the Parquet in MinIO read by DuckDB's httpfs (path-style, the stack's own
MinIO password) and graded as the local Parquet is (each delta, the newest version per key, the CDC snapshot + changes
replayed, exact operation counts, XCom rows, the stored cursor read by psql); each ClickHouse current-state view read
over HTTP as JSON and compared value by value with `gate-source` (warehouse.py's canonical forms: batch with
exactly the keys the cycle deleted left over, CDC without the rows flagged `__is_deleted`). A third config names a
bucket that is never made: its DAG (`gate_s3_missing`) runs once after the cycles and must stop on
`RIVET_DEST_CONTAINER_NOT_FOUND` within 120 s with exit 1 (rivet declares the code an `environment` error: 2 only
when transient), on Airflow's first try (`s3/missing-bucket/coded-final-stop`).
One more config (init's `--mode incremental` with ClickHouse, over a database of its own, `cleanup_source: true` as
init writes it) grades rivet's three promises about deleting staged Parquet (`gate/cleanup.py`, cells `cleanup/...`),
with the bucket listed by `mc`, the ledger read with psql and ClickHouse compared with the source: a load narrowed
to one export cleans that export's prefix and keeps the other's unloaded runs, which the other's own load later takes
(`unloaded-runs-survive`); a load whose ClickHouse table refuses every row (a CHECK constraint) fails with a code and
deletes nothing, and once mended the next load delivers every row and cleans (`failed-load-deletes-nothing`); while
an extract is held `running` after its snapshot opened (`RIVET_TEST_PAUSE_AT=pg_after_snapshot_open`), a load of
its export succeeds and deletes nothing, a load of the other export beside it cleans, and after the run ends the next
load cleans (`live-run-blocks`).
Everything lives in the stack and goes with its `down -v`.

There is no Azure leg: rivet 0.32's `init` has no Azure destination flag (only `--gcs-bucket` and `--s3-bucket`), and
the gate's configs come only from `rivet init`.

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
