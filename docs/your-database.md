# Add your own database

The [quickstart](quickstart.md) runs four pipelines on demo databases. This page writes the commands that add your
own MySQL or PostgreSQL database as one more pipeline in the same stack: CDC or batch, into the stack's MinIO and
ClickHouse or into your own S3 bucket and ClickHouse.

**Command builder.** Fill in the form below and every command on this page is rewritten with your values, ready to
copy. On GitHub the form is not rendered: replace the `{{…}}` placeholders by hand.

<div id="af-builder"></div>

---

## 1. The stack

```bash
git clone --depth 1 https://github.com/panchenkoai/airflow-provider-rivet.git
cd airflow-provider-rivet/quickstart/{{FOLDER}}
cp ../.env.example .env                  # on Linux: set AIRFLOW_UID to the output of `id -u`
docker compose up -d --build --wait      # then http://localhost:8080, airflow / airflow
DC="docker compose"                      # the commands below run `$DC exec …` in this folder, in bash
```

Skip this if the stack from the [quickstart](quickstart.md) is up: run the commands below in its folder.

## 2. Your database: grants

Run on your database, as an administrator. The containers must reach its host: a database on your own machine is
`host.docker.internal`.

```sql
{{GRANTS}}
```

## 3. The source Connection

The password is read from the terminal, never typed into a command line:

```bash
{{CONN}}
```

## 4. The pipeline's state, bucket and warehouse

Each pipeline has its own state database, bucket, ClickHouse database and state directory: its cursors and CDC
checkpoint belong to it alone, and two sources with the same table names never share a folder or a table.

```bash
{{STATE}}
{{BUCKET}}
{{WAREHOUSE}}
```

## 5. The config, generated from the live database

`rivet init` runs inside the Airflow container, reads your tables and writes `dags/rivet/{{P}}.yaml`, with its key
recorded in the pipeline's own state:

```bash
{{INIT}}
```

Review the file before the first run: `rivet init` guesses partitions and modes from the tables (see
[rivet's config reference](https://panchenkoai.github.io/rivet/reference/config.html)).

## 6. The DAG

Add this line to `PIPELINES` in `dags/rivet_quickstart.py`:

```python
{{DAG_LINE}}
```

A config with a `mode: cdc` export becomes a CDC DAG (one stream, then a load per table); any other becomes a batch
DAG (an extract and a load per table). Then:

```bash
{{UNPAUSE}}
```

A DAG unpaused before the scheduler has read its file stays paused: Airflow 2.10 prints `No paused DAGs were found`,
Airflow 3.3 prints the DAG as it was. Repeat until `dags list` shows it `False`.

## 7. Check the warehouse

After the first run of `{{DAG}}`:

```bash
{{CHECK}}
```

Each count should be the source table's row count. `<table>` in ClickHouse is a view with one row per key, the
latest version; a CDC table keeps a deleted key with `__is_deleted = 1`.

## Notes

- **Batch and the key.** A batch config reads each table through a `query:`, and for a `query:` export `rivet run`
  records no key, so the builder writes `pk: [<key>]` into the config instead of leaving `pk: auto`
  ([rivet#679](https://github.com/panchenkoai/rivet/issues/679)).
- **Your own bucket or ClickHouse** gets its own Connection, named in the pipeline's `PIPELINES` line (`storage`,
  `warehouse`). The demo pipelines keep the stack's MinIO and ClickHouse.
- **CeleryExecutor**: pick `airflow-worker` as the service that runs tasks.
- **ClickHouse** loading is a preview: [known limits](https://panchenkoai.github.io/rivet/recipes/clickhouse-load.html).
- What the builder's commands were proven with: every command for a PostgreSQL CDC pipeline on Airflow 3.3.2 and for
  a MySQL batch pipeline with its own ClickHouse Connection on Airflow 2.10.5 was run as generated against the
  quickstart stack, and the ClickHouse tables were compared with the source. The `s3` (AWS) bucket branch was not run.

<script src="your-database.js"></script>
