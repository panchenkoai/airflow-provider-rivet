# Quickstart: CDC and batch pipelines in Airflow with Docker Compose

One `docker compose up` starts Airflow with rivet and this provider, two demo source databases, a bucket and a
warehouse. Four pipelines are ready to run:

| Pipeline | Source | Mode | Lands in ClickHouse |
|---|---|---|---|
| `rivet_mysql_cdc` | MySQL `shop` | CDC (binlog): inserts, updates, deletes | `mysql_cdc.customers`, `mysql_cdc.orders` |
| `rivet_mysql_batch` | MySQL `shop` | batch, incremental on `updated_at` | `mysql_batch.*` |
| `rivet_postgres_cdc` | PostgreSQL `shop` | CDC (logical slot) | `postgres_cdc.*` |
| `rivet_postgres_batch` | PostgreSQL `shop` | batch, incremental on `updated_at` | `postgres_batch.*` |

Everything runs on your machine: no cloud account is needed. The same files then take your own database: the
[command builder](https://panchenkoai.github.io/rivet/airflow-quickstart.html) writes the commands for it.

The files are in [`quickstart/`](../quickstart):

| File | What it is |
|---|---|
| `airflow-2.10/docker-compose.yaml` | Airflow 2.10.5 (webserver, scheduler; LocalExecutor) and the demo services |
| `airflow-3.3/docker-compose.yaml` | Airflow 3.3.2 (api-server, scheduler, dag-processor; LocalExecutor) and the same demo services |
| `Dockerfile` | the official Airflow image plus `rivet-cli` and `airflow-provider-rivet`, pinned under Airflow's constraints |
| `.env.example` | `AIRFLOW_UID` and the host ports |
| `dags/rivet_quickstart.py` | one DAG per pipeline, built from the pipeline's config |
| `dags/rivet/<pipeline>.yaml` | the four rivet configs, as `rivet init` writes them |
| `demo/*.sql` | the demo tables (1,000 customers, 5,000 orders), grants, and the changes for a second run |

Both compose files are proven the same way: from an empty stack, the first run of every DAG lands every table, and
after the demo changes the second run lands them too. Each table in ClickHouse was compared with its source, row
count and sums of key, amount and text columns, on Airflow 2.10.5 and 3.3.2 with `rivet-cli` 0.32.2 and this
provider 0.2.0.

## What runs where

```text
MySQL / PostgreSQL ──► rivet (an Airflow task) ──► MinIO bucket (Parquet) ──► rivet load ──► ClickHouse
                              │
                              └── state: cursors, run journal, load ledger ──► rivet-state (PostgreSQL)
```

| Service | Image | Reach it at |
|---|---|---|
| Airflow UI | `rivet-quickstart:<version>` (built from `Dockerfile`) | http://localhost:8080, `airflow` / `airflow` |
| `mysql` | `mysql:8.4`, binlog in ROW format with full metadata | `docker compose exec mysql mysql -uroot -proot_demo shop` |
| `source-postgres` | `postgres:16`, `wal_level=logical` | `docker compose exec source-postgres psql -U postgres -d shop` |
| `minio` | MinIO (S3 API) | console http://localhost:9001, `rivet` / `rivet_minio_demo` |
| `clickhouse` | `clickhouse/clickhouse-server:24.8` | http://localhost:8123/play, user `loader`, password `rivet_clickhouse_demo` |
| `rivet-state` | `postgres:16`, one database per pipeline | inside the compose network only |

Every port is bound to `127.0.0.1`. Every password is a demo value: change them before the stack holds anything real.

## 1. Start

Needs Docker with Compose v2 and about 6 GB of memory for Docker. The commands are for `bash`.

```bash
git clone --depth 1 https://github.com/panchenkoai/airflow-provider-rivet.git
cd airflow-provider-rivet/quickstart/airflow-3.3        # or airflow-2.10
cp ../.env.example .env                                 # on Linux: set AIRFLOW_UID to the output of `id -u`
docker compose up -d --build --wait
```

The first start builds the image (the Airflow base image and a `pip install`), then every service reports healthy.
A port already in use: change it in `.env` (`AIRFLOW_PORT`, `MINIO_CONSOLE_PORT`, `CLICKHOUSE_PORT`).

## 2. Run the pipelines

Open http://localhost:8080 and switch on the four `rivet_*` DAGs, or:

```bash
for d in mysql_cdc mysql_batch postgres_cdc postgres_batch; do
  docker compose exec -T airflow-scheduler airflow dags unpause rivet_$d
done
docker compose exec -T airflow-scheduler airflow dags list      # is_paused: False for all four
```

A DAG unpaused before the scheduler has read its file is not unpaused: Airflow 2.10 prints
`No paused DAGs were found`, and Airflow 3.3 prints the DAG as it was. The `dags list` line is the evidence; repeat
the unpause until it shows `False`.

Unpausing starts the latest interval at once; the schedule is every 10 minutes. The first run of each DAG reads the
whole tables (a CDC stream anchors its position first and then reads the tables, so no change falls between the
two). In ClickHouse:

```bash
docker compose exec -T clickhouse clickhouse-client --user loader --password rivet_clickhouse_demo \
  -q "SELECT count() FROM mysql_cdc.orders WHERE NOT __is_deleted"      # 5000
```

`<table>` in ClickHouse is a view: one row per key, the latest version. A CDC table keeps a deleted key with
`__is_deleted = 1`, so live rows are `WHERE NOT __is_deleted`. The change log itself is `<table>__changes`.

## 3. Change the sources and run again

```bash
docker compose exec -T mysql mysql -uroot -proot_demo shop < ../demo/changes-mysql.sql
docker compose exec -T source-postgres psql -U postgres -d shop < ../demo/changes-postgres.sql
for d in mysql_cdc mysql_batch postgres_cdc postgres_batch; do
  docker compose exec -T airflow-scheduler airflow dags trigger rivet_$d
done
```

The changes: 20 new customers, 30 orders moved to `shipped`, 5 orders deleted. After the runs:

| | CDC pipelines | batch pipelines |
|---|---|---|
| new customers | 1020 rows | 1020 rows |
| shipped orders | updated | updated |
| deleted orders | gone (4995 live rows) | still there (5000 rows) |

A batch pipeline reads the rows whose `updated_at` moved; a deleted row has none. To see deletes, use CDC.

## 4. Your own database

The [command builder](https://panchenkoai.github.io/rivet/airflow-quickstart.html) takes your database's name, host,
tables and mode, and writes every command below with those values. Without it, the steps are:

1. **Grants** on your database: `SELECT` on the tables; for MySQL CDC also `REPLICATION SLAVE` and
   `REPLICATION CLIENT` and the binlog settings above; for PostgreSQL CDC the `REPLICATION` attribute and
   `wal_level = logical` ([rivet's CDC guide](https://panchenkoai.github.io/rivet/reference/cdc.html)).
2. **A Connection** for the source, e.g. `airflow connections add rivet_billing --conn-type postgres ...`. The host must
   resolve from the containers: a database on your machine is `host.docker.internal`.
3. **The pipeline's own state database, bucket, ClickHouse database and `state_dir`**: `createdb` in `rivet-state`,
   the Connection `rivet_state_<pipeline>`, `mc mb` in `minio`, `CREATE DATABASE` in `clickhouse`,
   `mkdir /opt/airflow/rivet-state/<pipeline>`.
4. **The config**, generated inside the Airflow container from the live database by `rivet init`, with the state in
   the pipeline's own database, and `--mode cdc` or `--mode incremental`.
5. **One line** in `PIPELINES` in `dags/rivet_quickstart.py`:
   `"billing_batch": {"source": "rivet_billing"},`. Optional keys `storage` and `warehouse` name the Connections of
   your own S3 bucket and ClickHouse instead of the stack's.
6. **Unpause** its DAG as in step 2.

Why each pipeline has its own of everything: `rivet init` gives every batch export the prefix `exports/<table>/`, so
two pipelines in one bucket would write into each other's folders; two sources with the same table names would be one
table in one ClickHouse database; and a pipeline's cursors and checkpoint belong to that pipeline only.

A batch config: `rivet init` writes a `query:` for each table and `pk: auto` in `load:`. For a `query:` export
`rivet run` records no key, so `pk: auto` works only while the key `init` recorded is in the state the DAG uses
([rivet#679](https://github.com/panchenkoai/rivet/issues/679)). The demo configs and the builder's commands declare
`pk: [id]`; for your tables, name their key. Without it a load stops with `RIVET_LOAD_PRIMARY_KEY_MISSING`.

## Airflow 2.10 and 3.3

The two compose files differ only where Airflow does:

| | 2.10 | 3.3 |
|---|---|---|
| UI and API | `airflow-webserver` | `airflow-apiserver`, plus `airflow-dag-processor` |
| Auth | Airflow's own | FAB auth manager, the same `airflow` user |
| Tasks talk to | the metadata database | the api-server (`AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, a JWT secret) |

The demo services, the Connections, the DAG file and the configs are the same files.

## From quickstart to production

- **Passwords and keys**: every demo password is in the compose file. Put real ones in a secrets backend or in
  Connections (`airflow connections add`), and set `FERNET_KEY` in `.env` so Airflow encrypts them.
- **TLS**: the demo connects without TLS inside one compose network (`--tls disable`, `sslmode=disable`). A database
  across a network takes `--tls verify-full` in `rivet init`.
- **CeleryExecutor**: run the commands in `airflow-worker` instead of `airflow-scheduler`; `./rivet-state` must be
  the same directory on every worker ([State](state.md)).
- **Back up** the `rivet-state` volume and `./rivet-state`: a CDC checkpoint is a file there, and a lost one means a
  new baseline.
- **ClickHouse** loading is a preview in rivet
  ([known limits](https://panchenkoai.github.io/rivet/recipes/clickhouse-load.html)).

## Stop and clean up

```bash
docker compose down -v      # removes the containers and every volume: databases, bucket, warehouse, state
rm -rf logs rivet-state     # the task logs and rivet's files on the host
```
