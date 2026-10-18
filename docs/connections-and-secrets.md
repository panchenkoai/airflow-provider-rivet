# Connections and secrets

What reaches the rivet process, and how Airflow Connections become what rivet reads.

## Secrets

- rivet is configured through environment variables and nothing else, and **the subprocess gets
  a minimal environment, not the worker's**. It is built from three things:
  1. The worker's variables on one allow-list (`ENV_ALLOWLIST` in `invocation.py`): `PATH`,
     `HOME`, `USER`, `LOGNAME`, `TZ`, `LANG`, `LC_*`, `TMPDIR`, `SSL_CERT_FILE`, `SSL_CERT_DIR`,
     `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`, `ALL_PROXY`, `http_proxy`, `https_proxy`,
     `no_proxy`, `all_proxy`, `KUBERNETES_SERVICE_HOST`, `KUBERNETES_SERVICE_PORT`. Nothing else
     is inherited: not `AIRFLOW__*`, not `AIRFLOW_CONN_*`, not a `RIVET_*` variable set on the
     worker. `env_passthrough=["RIVET_STATE_URL", "MY_*"]` extends the list with names or
     shell-style patterns. `cloud_credentials=["gcp", "aws", "azure"]` adds
     `GOOGLE_APPLICATION_CREDENTIALS` and `GOOGLE_CLOUD_PROJECT`, `AWS_*`, `AZURE_*` for a
     destination or warehouse that authenticates from the environment. A storage Connection
     (`connections=`, below) that supplies a credential of one of these kinds **replaces** the
     worker's variables of that kind, whichever way they were passed: the worker's
     `AWS_SESSION_TOKEN` never rides beside a Connection's static keys, nor its account key beside
     a Connection's SAS token. A Connection that supplies no credential (an AWS one with only a
     region, a GCP one with only a project) leaves the worker's identity in place.
  2. `env={NAME: value}`. It is not templated and not stored in the metadata database, but it
     is whatever your DAG file computes: do not write a secret literal there.
  3. `env_from_connections={ENV_NAME: conn_id}`, which builds a URL from an Airflow Connection's
     fields inside `execute`, credentials and the database name percent-encoded. The scheme
     comes from `{ENV_NAME: (conn_id, "postgresql")}`, else the connection extra `rivet_scheme`,
     else the connection type (`postgres`, `mysql`, `mssql`, `mongo`, `oracle`); a type that
     names no rivet scheme, such as `generic`, is refused before rivet starts
     (`RIVET_AIRFLOW_CONNECTION_SCHEME`). A password without a login is kept, an IPv6 literal is
     bracketed with its zone encoded (`fe80::1%eth0` becomes `[fe80::1%25eth0]`), any other host
     is kept as written (`db:5432` with an empty port field), an Oracle connection's extra
     `service_name` is used as the path. The standard extras that have a URL form become query
     parameters: PostgreSQL `sslmode`, `application_name`, `connect_timeout`; MongoDB `srv` (the
     `mongodb+srv` scheme, which takes no port), `authSource`, `authMechanism`, `replicaSet`,
     `tls`/`ssl`, `allow_insecure`, `directConnection`, `readPreference`, `appName`. An extra
     that changes how the hook authenticates and has no URL form (PostgreSQL `iam`, `redshift`;
     MySQL `unix_socket`) is refused (`RIVET_AIRFLOW_CONNECTION_UNSUPPORTED`). Other extras are
     client-library settings and are not read. The extra `rivet_params` is appended as written
     and wins over a mapped extra of the same name. A `conn_id` no backend
     defines, here or in `connections=[...]`, is refused before rivet starts
     (`RIVET_AIRFLOW_CONNECTION_MISSING`), not retried.
  4. `connections=[conn_id, ...]`: storage and warehouse Connections, see below.

  argv carries the config path, export names and flags.
- XCom carries code, class, exit_code, retryable, action, kind, counts, `stop_reason`, the
  degradations and file paths. It never carries the error `message`: redaction covers
  credentials only, and error text can embed source cell values.
- The task log gets a readable block in the package's own words (see ["Reading the task log"](operators.md#reading-the-task-log)):
  outcome, codes, class, action, counts, run id, step exit statuses, paths. Names the DAG gives
  (export, table) are the only text in it the package did not write. The one-line JSON records,
  `rivet.start` (argv), `rivet.step`, `rivet.unit` (one per unit), `rivet.result`, `rivet.warning`,
  `rivet.crashed`, `rivet.cdc.max_events`, `rivet.refused`, ..., are written at DEBUG, under the
  same rules.
- **rivet's raw stdout and stderr go to files and never to the task log or to the worker
  process's own stderr** (which Airflow 3, and Airflow 2 with `run_as_user`, copy into the task
  log). The files are
  `<base>/<dag_id>/<task_id>-<hash>/<run_id>__try<N>.<step>.stderr.log` (and `.stdout.log`), mode `0600`
  in `0700` directories; XCom and a failed task's `stderr` log line name the path (and the
`rivet.start` record at DEBUG). `<base>` is
  `stderr_dir` when given, else `<state_dir>/logs`, else, on a worker with no state directory,
  `<temp dir>/rivet-airflow-<uid>/logs` (recorded as `stderr_temp`; a pod loses the files with
  its disk, and what a failure was is still in the error object).
- **Retention.** When a task ends, the files of its newest `log_keep` tries (default 10, counted
  per task across runs) are kept and the older ones are removed. `log_keep=None` keeps
  everything. Only the task's own folder is looked at; a file of an older layout is never
  removed (see "Upgrading a state directory" in [State](state.md#local-worker-setup-guide)). Under `airflow/<dag_id>/`, a sealed plan artifact
  that has expired and was last written over 24 h ago is removed (with the `plans/<run_id>/`
  folder it leaves empty), and so is a crashed ledger untouched for 24 h (a crash of that series
  more than 24 h after its last one starts a fresh budget). A ledger that is not the ledger's JSON
  is left for a person to delete.
- `stream_stderr=True` copies stderr into the task log when each step ends. It is the one way
  raw stderr reaches the task log, and it is for debugging: it puts possibly data-bearing text
  into a store with wider access than the worker.

## Storage and warehouse connections

`connections=["gcp_rivet", "sf_rivet"]` (on every operator, or in a builder's `operator_kwargs`)
turns each Airflow Connection into what rivet reads, inside `execute`: variables, and key files
written mode `0600` into a private folder that is removed when the task ends, whatever its
outcome. The kind comes from the connection type, or from the extra `rivet_kind`; one task
takes at most one Connection of each kind (`RIVET_AIRFLOW_CONNECTION_KIND_TWICE`), and a type
that is none of them is refused before rivet starts (`RIVET_AIRFLOW_CONNECTION_KIND`).

| Kind (connection type) | Read from the Connection | rivet gets | The config names it |
|---|---|---|---|
| `gcp` (`google_cloud_platform`) | extra `keyfile_dict` (written to a file) or `key_path`; `project` | `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_CLOUD_PROJECT` | nothing: GCS and BigQuery read it |
| `aws` (`aws`) | login, password; extras `aws_session_token`, `region_name`, `endpoint_url`. A role (`role_arn`, `assume_role_*`), a `profile_name`, or keys in extras (also under `session_kwargs`) are refused, `RIVET_AIRFLOW_CONNECTION_UNSUPPORTED`: rivet signs with static or session keys only | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`, `AWS_ENDPOINT_URL` | `access_key_env: AWS_ACCESS_KEY_ID`, `secret_key_env: AWS_SECRET_ACCESS_KEY` (+ `session_token_env`) |
| `azure` (`wasb`) | login (account), password (key); extra `sas_token` | `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `AZURE_STORAGE_SAS_TOKEN` | `account_name: ${AZURE_STORAGE_ACCOUNT}` (rivet reads no account variable of its own), and `account_key_env: AZURE_STORAGE_KEY` or `sas_token_env: AZURE_STORAGE_SAS_TOKEN` |
| `snowflake` (`snowflake`) | extras `account`, `region`, `role`, `warehouse`, `database`, `private_key_content` (PEM, or base64 of PEM as Airflow's SnowflakeHook takes it; written to a file) or `private_key_file`; with a key the password is the key's passphrase, else the login password | a `snow` connection named as `load.connection` in a private `SNOWFLAKE_HOME`; `RIVET_SNOWFLAKE_KEY`; `PRIVATE_KEY_PASSPHRASE` for an encrypted key | `load.connection` |
| `clickhouse` (`clickhouse`, or any type with `rivet_kind: clickhouse`) | host (required, `RIVET_AIRFLOW_CONNECTION_INCOMPLETE` without one), port, login, password; extra `secure` or `protocol` | `RIVET_CLICKHOUSE_URL`, `RIVET_CLICKHOUSE_USER`, `RIVET_CLICKHOUSE_PASSWORD`, and the password also under the config's own `password_env` (`rivet init` writes `CLICKHOUSE_PASSWORD`) | `url: ${RIVET_CLICKHOUSE_URL}`, `user: ${RIVET_CLICKHOUSE_USER}`, `password_env: RIVET_CLICKHOUSE_PASSWORD` (or any name) |

The config keeps what is not a secret (bucket, project, dataset, database, schema); the
Connection holds who may write there. The Snowflake loader runs the `snow` CLI, which must be on
the worker (`pip install snowflake-cli` into its own environment). On BigQuery `rivet compact`
merges the buffer into the base table; on Snowflake and ClickHouse the current state is a view
over `<table>__changes` and there is nothing to merge. rivet 0.31.0 (and main) says so only in a
stderr line (`compact [<table>]: skipped — this warehouse keeps a change log behind a view and
never compacts`) and exits 0; `rivet compact` has no `--summary-output` yet (`load_result`), so the
task ends green as `compacted`, having done nothing. For these warehouses build the CDC DAG with
`compact=False` and leave such exports out of `compact_exports`.

## Connections and secrets: recommended setup

Where the Connections of a rivet DAG live decides who else can read them. Safest first:

1. **Keyless GCP: the worker's own identity.** On GKE the worker pod runs as a Kubernetes service
   account bound to a Google service account (Workload Identity); on Cloud Composer it is the
   environment's service account. No key file exists anywhere. The GCP Connection holds only
   `project`; a Connection that supplies no credential leaves the worker's identity in place, and
   rivet finds it through Google's default chain (GCS: an ADC file, else the metadata server).
   rivet's BigQuery loader has no metadata-server path of its own: with no ADC
   file it runs `gcloud auth application-default print-access-token`, so a Workload Identity
   worker that loads BigQuery needs the `gcloud` CLI on its `PATH`. The live gate proves this
   leg with a credential file mounted read-only where the default chain looks; it has no
   Workload Identity to test.
2. **Google Secret Manager as Airflow's secrets backend** for what has to be a secret: the
   Snowflake key, the ClickHouse password, source database passwords. Airflow reads a Connection
   from Secret Manager when a task asks for it; it is never in the metadata database, a compose
   file or the container's environment. The worker's identity is granted access to the
   connection secrets only, not to the project's other secrets.
3. **Connections in the metadata database, with a Fernet key set** (`[core] fernet_key`).
   Without one Airflow stores every password in clear and logs `empty cryptography key`.
4. **`AIRFLOW_CONN_*` environment variables: the worst place.** They are readable through
   `docker inspect`, `/proc/<pid>/environ` of every Airflow process, compose files and
   deployment manifests. Use them for a throwaway stand only (the live gate's default leg).

**GCP, keyless.** The Connection (it holds no secret, so it may live anywhere):

```bash
airflow connections add gcp_rivet --conn-json '{"conn_type": "google_cloud_platform", "extra": {"project": "my-project"}}'
```

The grants, for a worker running as `rivet-worker@my-project.iam.gserviceaccount.com` (these are the
usual roles; the gate runs as a project owner and does not measure the minimum):

```bash
GSA=rivet-worker@my-project.iam.gserviceaccount.com
gcloud iam service-accounts add-iam-policy-binding "$GSA" --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:my-project.svc.id.goog[airflow/airflow-worker]"     # GKE: namespace/KSA
gcloud storage buckets add-iam-policy-binding gs://my-bucket --member "serviceAccount:$GSA" --role roles/storage.objectAdmin
gcloud projects add-iam-policy-binding my-project --member "serviceAccount:$GSA" --role roles/bigquery.jobUser
bq add-iam-policy-binding --member "serviceAccount:$GSA" --role roles/bigquery.dataEditor my-project:raw
```

**Secrets, from Google Secret Manager.** Airflow's configuration (`airflow.cfg`, or the same as
`AIRFLOW__SECRETS__BACKEND` / `AIRFLOW__SECRETS__BACKEND_KWARGS`, which hold no secret):

```ini
[secrets]
backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
backend_kwargs = {"connections_prefix": "airflow-connections", "sep": "-", "variables_prefix": null, "config_prefix": null, "project_id": "my-project"}
```

A conn_id `sf_rivet` is then the secret `airflow-connections-sf_rivet`, holding the Connection in
Airflow's JSON form. Make each secret from a file or stdin, never from a command-line argument:

```bash
# Snowflake: the key file goes in whole (private_key_content), the login is the key's user
jq -n --rawfile key rsa_key.p8 '{conn_type: "snowflake", login: "RIVET_LOADER",
  extra: {account: "xy12345", role: "LOADER", warehouse: "LOAD_WH", database: "RAW", private_key_content: $key}}' |
  gcloud secrets create airflow-connections-sf_rivet --data-file=-
# ClickHouse and a source database: the password is read from the terminal, not typed into argv
read -rs PW
jq -n --rawfile pw /dev/stdin '{conn_type: "clickhouse", host: "ch.internal", port: 8443, login: "loader",
  password: $pw, extra: {secure: true}}' < <(printf %s "$PW") | gcloud secrets create airflow-connections-ch_rivet --data-file=-
jq -n --rawfile pw /dev/stdin '{conn_type: "postgres", host: "db.internal", port: 5432, login: "rivet_reader",
  password: $pw, schema: "app", extra: {sslmode: "verify-full"}}' < <(printf %s "$PW") |
  gcloud secrets create airflow-connections-pg_app --data-file=-
```

(`printf %s` is a shell builtin: the password is in no process's argv, and it gets no trailing newline.) The worker's identity reads these secrets only, through an IAM
condition on the secret name (it needs the project NUMBER):

```bash
gcloud projects add-iam-policy-binding my-project --member "serviceAccount:$GSA" \
  --role roles/secretmanager.secretAccessor \
  --condition='title=airflow-connections,expression=resource.name.startsWith("projects/123456789012/secrets/airflow-connections-")'
```

The DAG names the same conn_ids whatever backend serves them:
`connections=["gcp_rivet", "sf_rivet"]`, `env_from_connections={"PG_URL": "pg_app"}`.

**What the operator does with a secret.** Inside `execute`, each try makes a private folder
(`tempfile.mkdtemp`, mode `0700`, in the worker's temp directory); a key a Connection carries
(`keyfile_dict`, `private_key_content`) and the Snowflake `config.toml` are written into it with
mode `0600`, and the folder is removed in a `finally` when the try ends, whatever its outcome. A
worker killed with SIGKILL runs no `finally`, and the folder stays in its temp directory. A
password (ClickHouse, a database URL) is passed in the environment of the rivet process only, not
of the worker: the same user and root can read it in `/proc/<pid>/environ` while rivet runs.
