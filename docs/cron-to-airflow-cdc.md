# Recipe: a MySQL CDC stream from cron to Airflow

How to move a rivet MySQL CDC stream that cron runs today (a config, its `.rivet_state.db` and its checkpoint file
in one folder) to an Airflow DAG with PostgreSQL state, one stream per source database. The stream continues from
the binlog position cron stopped at: no new baseline, and only the changes made after cron stopped are loaded.

Proven on Airflow 2.10.2 with CeleryExecutor and one worker (the official docker-compose file), `rivet-cli` 0.32.2
and `airflow-provider-rivet` 0.2.0 from PyPI, with configs and states written by rivet 0.32.1 and 0.30.0 (state schema
v31). The steps below were run as written on synthetic data, including the way back and a second source. Values read
back from BigQuery with `bq` matched the source, and no change arrived twice.

## The layout, per source

Every source database is its own stream with its own names. For a source `shop`:

| What | Name | Example |
|---|---|---|
| Airflow Connection to MySQL (type `mysql`) | `rivet_<src>` | `rivet_shop` |
| rivet state database | `rivet_state_<src>` | `rivet_state_shop` |
| Airflow Connection to it (type `postgres`) | `rivet_state_<src>` | `rivet_state_shop` |
| rivet config | `dags/rivet/<src>.yaml` | `dags/rivet/shop.yaml` |
| `state_dir` | `/opt/airflow/rivet-state/<src>` | `/opt/airflow/rivet-state/shop` |
| BigQuery dataset | one per source | `shop` |
| DAG | `rivet_<src>_cdc` | `rivet_shop_cdc` |

Why each one is separate:

- **The checkpoint file** is one server's binlog position for one stream. rivet 0.32.2 writes `<file>.ckpt.owner`
  beside it and refuses a second config on it (`RIVET_SOURCE_CDC_CHECKPOINT_SHARED`).
- **The state database**: `rivet state copy` writes only into an empty database, so each cron `.rivet_state.db` needs
  its own.
- **`server_id`** is the stream's MySQL replica id; two streams on one MySQL server with one `server_id` knock each
  other off.
- **The bucket prefix**: two configs in one prefix mix their files.
- **The warehouse dataset**: `orders` of two sources would be one table, and the second source's load is refused
  (`RIVET_LOAD_TARGET_NOT_RIVETS`).

## Before you start

- **A PostgreSQL for the state**, apart from Airflow's metadata database, e.g. a `postgres:16` service with its own volume in
  the compose file and no published port. Create one database per source in it.
- **The state directory**, `./rivet-state` on the host, bind-mounted at `/opt/airflow/rivet-state` in every Airflow
  service and owned by the Airflow user. The CDC checkpoint lives here, not in PostgreSQL: if this directory is lost,
  the stream loses its position. Back it up together with the state database's volume.
- **`rivet-cli` and `airflow-provider-rivet` in the image** (see [Install](../README.md#install)). Check with
  `docker compose exec airflow-worker rivet --version`.
- **Google credentials.** The operator passes nothing from the worker's environment to rivet except a short
  allow-list ([Connections and secrets](connections-and-secrets.md)). A key file named by `GOOGLE_APPLICATION_CREDENTIALS`
  in the compose environment reaches rivet only with `cloud_credentials=["gcp"]` on the tasks.
- **MySQL**: `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL` (otherwise
  `RIVET_SOURCE_CDC_PREREQUISITE`), and the grants `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT`.

The commands below run in the compose project folder, in `bash`. The compose file passes `RIVET_STATE_USER` and
`RIVET_STATE_PASSWORD` from `.env` into the Airflow services' environment (steps 4 and 6 use them inside the worker):

```bash
DC="docker compose --env-file .env"
name=shop
env_value() { grep "^$1=" .env | cut -d= -f2-; }
AIRFLOW_UID=$(env_value AIRFLOW_UID)   # the compose file's value, not the shell's
```

## 1. Connections and the state database

```bash
$DC exec -T rivet-state createdb -U "$(env_value RIVET_STATE_USER)" "rivet_state_$name"
read -rs MYSQL_PASSWORD && export MYSQL_PASSWORD   # typed, not on any command line
$DC exec -T -e MYSQL_PASSWORD airflow-worker bash -c "airflow connections add rivet_$name \
  --conn-type mysql --conn-host db1.internal --conn-port 3306 --conn-schema $name \
  --conn-login rivet --conn-password \"\$MYSQL_PASSWORD\""
$DC exec -T airflow-worker bash -c "airflow connections add rivet_state_$name \
  --conn-type postgres --conn-host rivet-state --conn-port 5432 --conn-schema rivet_state_$name \
  --conn-login \"\$RIVET_STATE_USER\" --conn-password \"\$RIVET_STATE_PASSWORD\" \
  --conn-extra '{\"rivet_params\": \"sslmode=disable\"}'"
mkdir -p "rivet-state/$name" && sudo chown -R "${AIRFLOW_UID}:0" "rivet-state/$name"
```

- **Spell the MySQL host exactly as the cron config does.** rivet keys its state by export name and source
  `host:port/database`: `127.0.0.1` and the server's DNS name are two sources. A CDC stream still continues, because its
  position is in the checkpoint file. An incremental export, however, finds no cursor and silently re-reads its table in
  full: exit 0, duplicate rows at the destination
  ([rivet#690](https://github.com/panchenkoai/rivet/issues/690)).
- `sslmode=disable` is for a state server reached only over the compose network. rivet refuses a state URL to another
  host that names no `sslmode` (`RIVET_STATE_URL_TLS_REQUIRED`).
- A Connection with the same id in a secrets backend wins over these: Airflow asks the backend first.
- `--conn-schema` is the MySQL database; here it has the source's name.

## 2. Stop cron, and find its folder and config

Comment out the rivet line in the crontab and wait until `pgrep -x rivet` prints nothing. If cron and Airflow both
ran the stream, each would read the binlog from the same position and every change would arrive twice.

The commented line names the two things the next steps need: the folder cron runs rivet in and the config it passes.
Set them in the shell you run the rest of the steps in:

```bash
crontab -l | grep rivet
# e.g.  # 0 */2 * * * cd /srv/elt/shop && rivet run -c cdc.yaml
CRON_DIR=/srv/elt/shop    # the folder after `cd`
CONFIG=cdc.yaml           # the file after `-c` / `--config`
ls -la "$CRON_DIR"        # $CONFIG, .rivet_state.db and the checkpoint's folder (cdc/)
grep -nE 'url_env|url:|^[[:space:]]+checkpoint:|server_id:' "$CRON_DIR/$CONFIG"
```

With no `cd` and a full path after `-c`, `CRON_DIR` is that file's folder and `CONFIG` its name. If the line runs a
script, the script holds both. No `.rivet_state.db` beside the config means the state lives elsewhere
(`RIVET_STATE_URL` in cron's environment): this recipe does not cover that case.

**Check the host now.** If the config says `url_env: <VAR>`, the MySQL address is not in the config but in cron's
environment: find `<VAR>` in the crontab line or the script it runs, and read its `host:port/database`. Compare it with
the Connection from step 1 (this prints no password):

```bash
$DC exec -T airflow-worker python -c "from airflow.hooks.base import BaseHook; c = BaseHook.get_connection('rivet_$name'); print(f'{c.host}:{c.port or 3306}/{c.schema}')" | tail -n 1
```

If the spelling differs, edit the Connection, not the cron config (step 1 explains why it matters).

## 3. Copy the cron folder

```bash
mkdir -p rivet-state/migration-src
cp -a "$CRON_DIR" "rivet-state/migration-src/$name"
sudo chown -R "${AIRFLOW_UID}:0" rivet-state/migration-src
```

Work on the copy. rivet 0.32.2 run on the original folder (even `rivet check`) upgrades its SQLite schema in place and
keeps the old file as `.rivet_state.db.v31…`. The untouched original is the way back.

## 4. Move the state, the checkpoint and the config

```bash
cfg=/opt/airflow/rivet-state/migration-src/$name/$CONFIG
$DC exec -T airflow-worker bash -c "RIVET_STATE_URL=\"postgresql://\$RIVET_STATE_USER:\$RIVET_STATE_PASSWORD@rivet-state:5432/rivet_state_$name?sslmode=disable\" \
  rivet state copy -c $cfg"
```

This reads the `.rivet_state.db` beside the config and copies every table into the empty database in one transaction,
checking each table's row count. The SQLite file is only read. It prints `copied N table(s), M row(s) …`.
`RIVET_STATE_USER`/`_PASSWORD` must be in the worker's environment for this line.

**The checkpoint** goes into `state_dir` at the config's own relative `cdc.checkpoint` path. The operator runs rivet on
a copy of the config in `state_dir`, so a relative path resolves there:

```bash
grep -nE '^[[:space:]]+checkpoint:' "rivet-state/migration-src/$name/$CONFIG"   # e.g. checkpoint: ./cdc/cdc.ckpt
mkdir -p "rivet-state/$name/cdc"
cp -a "rivet-state/migration-src/$name/cdc/cdc.ckpt" "rivet-state/$name/cdc/"   # the file name the grep printed
sudo chown -R "${AIRFLOW_UID}:0" "rivet-state/$name"
```

An absolute `checkpoint:` (a path on the cron machine) does not exist in the container. Make it relative in the copy.

**The config** goes to `dags/rivet/$name.yaml`, unchanged except the source variable, which the DAG maps from the
Connection:

```bash
mkdir -p dags/rivet
sed -E 's/^([[:space:]]*)url_env:.*/\1url_env: RIVET_MYSQL_URL/' "rivet-state/migration-src/$name/$CONFIG" > "dags/rivet/$name.yaml"
```

Never put the config inside `rivet-state/`, and keep no `cdc/` beside it in `dags/rivet/`: the checkpoint lives only
in `state_dir`.

## 5. The DAG

[`examples/rivet_cdc_per_source.py`](../examples/rivet_cdc_per_source.py) builds one DAG per source. It reads the
`mode: cdc` export's name and tables from `dags/rivet/<src>.yaml` and wires `state_dir`, the two Connections and
`cloud_credentials=["gcp"]` by the layout above. Copy it into `dags/`, then list the sources (`RIVET_EXAMPLE_SOURCES`,
or edit `SOURCES`) and set the schedule to cron's. The scheduler and the worker read `RIVET_EXAMPLE_SOURCES` when
they parse the file: after changing it in `.env`, run `$DC up -d` so they see it.

List only the sources that already have `dags/rivet/<src>.yaml`, each once. The source name is the key every other
name hangs on (Connections, state database, `state_dir`), so a missing config is fixed by moving or generating it,
never by renaming another source's file. The example refuses both mistakes at parse, and `list-import-errors` below
shows why: `no config for <src> …` or `sources listed twice: <src>`. A source cron never ran joins the list after its
config is generated (below).

A new DAG file is picked up by the scheduler within `dag_dir_list_interval` (5 minutes by default). `airflow dags
list` and `airflow tasks list` parse the files themselves and show the DAG earlier, so they are no evidence that the
scheduler has it. Check the tasks, then unpause, and repeat the unpause until it prints `rivet_<src>_cdc | False`.
Before that it prints `No paused DAGs were found` and does nothing:

```bash
$DC exec -T airflow-scheduler airflow tasks list "rivet_${name}_cdc"   # run, load.*, compact.*, watcher
$DC exec -T airflow-scheduler airflow dags unpause "rivet_${name}_cdc"  # until: rivet_<src>_cdc | False
$DC exec -T airflow-scheduler airflow dags list-import-errors          # then: No data found
```

## 6. The first run

Unpause one DAG at a time (step 5). Unpausing starts the latest interval at once, so do not also press Trigger.

When it has finished:

- **The tasks are green and `watcher` is skipped.** That is normal: it runs nothing and fails the run only when another
  task failed. The `run` log line reads `rivet run cdc → OK: <n> rows, <k> files …`, counting the change rows since
  cron stopped. An UPDATE that moves a row to another partition of its `load:` table is written as a delete and an
  insert, so it counts as two rows.
- **No new baseline.** In rivet's run journal, the runs that started after cron stopped are of the CDC export, with
  no `…__snapshot_…` run. Cron's own baseline, which is older, may still be in the list:

  ```bash
  $DC exec -T airflow-worker bash -c "RIVET_STATE_URL=\"postgresql://\$RIVET_STATE_USER:\$RIVET_STATE_PASSWORD@rivet-state:5432/rivet_state_$name?sslmode=disable\" \
    rivet state runs -c /opt/airflow/dags/rivet/$name.yaml --last 5"
  ```

  With `cleanup_source: true` (what `rivet init` writes) the bucket's `snapshot/` files are gone after a load, so the
  bucket is no evidence.
- **BigQuery holds the changes made after cron stopped**: a changed row has its new value, and a deleted row has
  `__is_deleted = true`.
- **A run with no new changes** reports `OK: 0 rows, 0 files`, and `load.*` / `compact.*` are `SKIPPED: nothing to do`.

When every stream has passed its first run, delete `rivet-state/migration-src/`.

## When the checkpoint is in the wrong place

Both cases were measured, and in both nothing is written:

| Where the checkpoint is | What happens | What to do |
|---|---|---|
| Beside the config (`dags/rivet/cdc/`) and not in `state_dir` | the task stops before rivet starts: `RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG`, naming where to move it | move it into `state_dir` |
| Missing or empty | `RIVET_SOURCE_CDC_CHECKPOINT_INVALID`: the state knows the stream ran before | put the file back and run again; **do not re-baseline** as the message offers |

## A source cron never ran

Run step 1 with `name=<src>` (its state database, both Connections, its `state_dir`). Then generate its config
inside the worker, with the state in the source's own database (so that `init` records its keys where the DAG's runs
read them) and a BigQuery dataset of its own:

```bash
read -rs RIVET_MYSQL_URL && export RIVET_MYSQL_URL   # mysql://user:password@db1.internal:3306/<database>
$DC exec -T -e RIVET_MYSQL_URL airflow-worker bash -c "cd /opt/airflow/dags/rivet && \
  RIVET_STATE_URL=\"postgresql://\$RIVET_STATE_USER:\$RIVET_STATE_PASSWORD@rivet-state:5432/rivet_state_$name?sslmode=disable\" \
  rivet init --source-env RIVET_MYSQL_URL --mode cdc --include orders customers --tls disable \
    --gcs-bucket <bucket> --bigquery-project <project> --bigquery-dataset $name -o $name.yaml"
```

`--tls disable` writes `tls: { mode: disable }` in `source:`, for a MySQL reached without TLS; drop it for one with TLS.
`init` names everything after the `-o` file: the checkpoint `./cdc/<src>_cdc.ckpt`, the stream's prefix `<src>/cdc/`
and the `server_id`, so two sources on one server do not collide. Review the `load:` block, then add the source to the
DAG's sources (step 5). Its first run reads the baseline: every captured table in full from the source, so unpause
it in a quiet hour on a busy server.

## The way back

- **Before the first DAG run:** pause the DAG and uncomment the crontab line. cron continues from its original folder,
  which these steps did not touch.
- **After it:** the binlog position has moved on in `rivet-state/<src>/cdc/`, and the cron folder's checkpoint is
  behind. Do not just re-enable cron: it would read again from where it stopped, and the changes Airflow already
  loaded would arrive a second time. Pause the DAG, then copy the current checkpoint over the cron folder's
  (`rivet-state/<src>/cdc/<file>.ckpt` → `<cron folder>/cdc/<file>.ckpt`) before re-enabling cron. A CDC stream on its
  old SQLite state with the current checkpoint was measured to continue with no gap and no duplicate. An incremental
  export, by contrast, re-reads from the SQLite cursor everything exported since the copy; the load deduplicates it by key.
- **rivet 0.32.1 or earlier on cron:** once another database on the same MySQL server holds a table with a name the
  stream captures (for example after "A source cron never ran" adds one), every cron run stops with
  `RIVET_SOURCE_CDC_TABLE_UNRESOLVED`. rivet 0.32.2 reads only the connection's database. Upgrade cron's rivet to 0.32.2
  before re-enabling it (it upgrades the folder's SQLite in place and keeps the old file), or qualify the tables
  (`shop.orders`) in the cron config.
