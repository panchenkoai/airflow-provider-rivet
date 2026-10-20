# State

Where rivet's state lives under Airflow, and how to set up a worker for it.

## State, by deployment

| Worker | State | What the operator does |
|---|---|---|
| Ephemeral pod (`KUBERNETES_SERVICE_HOST` in the environment, or `deployment="pod"`) | PostgreSQL, required | refuses SQLite (`RIVET_AIRFLOW_STATE_SQLITE_ON_POD`); refuses every CDC export (`RIVET_AIRFLOW_CDC_ON_POD`: the checkpoint is a file and would live on the pod's disk) until rivet stores it in the state database; runs the config in place; keeps rivet's stderr in a private file on the pod's disk (`stderr_temp`), never in the task log |
| Local or long-lived worker (`deployment="local"` overrides the detection for a long-lived Kubernetes worker with a persistent volume) | SQLite in a declared directory, or PostgreSQL | requires `state_dir` for SQLite state and for CDC; checks it; refuses state or a checkpoint left beside the original config; warns once per task that SQLite is single-host |

`state_dir` is an operator parameter, not an Airflow Variable. The builders pass one value to
every task they create, so "the same path for every task" holds by construction, it is visible
in the rendered fields, and reading it needs no metadata-database access on the worker.

PostgreSQL state is `RIVET_STATE_URL=postgresql://...` in the task's environment:
`env_from_connections={"RIVET_STATE_URL": "rivet_state"}`, or set it on the worker and name it in
`env_passthrough=["RIVET_STATE_URL"]` (a worker variable is not inherited unless it is named).

**A PostgreSQL `RIVET_STATE_URL` set on the worker and not passed on is refused.** If the
worker's `RIVET_STATE_URL` is one rivet would use (it starts with `postgres`, rivet's own rule;
an empty, blank or `sqlite:` value selects nothing and is not refused) and the task's
environment has no entry for it
(not in `env_passthrough`, `env` or `env_from_connections`), the task fails before rivet starts
with `RIVET_AIRFLOW_STATE_ENV_NOT_PASSED` and is not retried: rivet would otherwise run green on
an empty SQLite state while the real state sits in the database the worker names. The message
names the variable and never its value. Fix it one of two ways: inherit the worker's value with
`env_passthrough=["RIVET_STATE_URL"]`, or give the task its own value with
`env_from_connections={"RIVET_STATE_URL": "<conn_id>"}` (or `env`). If the worker's variable is
not meant for this pipeline, unset it on the worker, or declare `env={"RIVET_STATE_URL": ""}` on
the task: a declared value, also an empty one, is the task's own decision and rivet then keeps
SQLite state in `state_dir`.

### Local worker setup guide

1. **Create the state directory yourself, once, on storage that outlives the worker process.**
   A host path or a named volume, never a container's own filesystem. One directory per
   pipeline (per source database), for example `/var/lib/rivet/<pipeline>`. The operator never
   creates it: a directory that silently appears empty is exactly the failure this guide exists
   to prevent, so a missing one is refused (`RIVET_AIRFLOW_STATE_DIR_MISSING`).
2. **Every task of every DAG that touches the pipeline must see the SAME absolute path.** With
   Docker, mount the same host path at the same container path in every worker. Pass that path
   as `state_dir` (the builders do it for all their tasks).
3. **What is in it.**
   - `.rivet_state.db`: rivet's state, under its default name.
   - `<config name>.<hash>.yaml` (and `<config name>.<hash>--<export>.yaml`): the operator's
     copy of your config, byte for byte. `<hash>` is the first 8 hex digits of the SHA-256 of the
     original's absolute path, so two configs with one file name never share a copy. Today's
     rivet can only keep SQLite state beside the config file, so the operator materialises the
     config here and runs that copy (`state_url_sqlite`). Edit your original; the copy is
     rewritten on every task.
   - every relative `query_file` of the config, copied to the same relative path (see 5), and
     `.rivet_airflow_query_files.json`, which records the config directory each one came from.
   - a relative `cdc.checkpoint`, which resolves beside the copied config, that is, here.
   - `airflow/<dag_id>/`: sealed plan artifacts
     (`plans/<run_id>/<export>__<task_id>__s<max_tries>__<inputs>-<hash>.json`), the crashed ledger, scratch
     files; `airflow/<dag_id>/plans/<run_id>/<artifact>.apply.json` (the record of an apply a try
     started, [Retries](retries.md)); `airflow/locks/run/<export>.lock` (run, apply, cdc_run),
     `airflow/locks/<export>.lock` (load and compact of a whole export) and
     `airflow/locks/table/<export>__<table>-<hash>.lock` (load and compact of one table) ([Overlap](limits.md#overlap)).
   - `logs/<dag_id>/<task_id>-<hash>/`: rivet's stdout and stderr, one file per step and try.
   - **How a name becomes a path.** A DAG id, task id, run id or export name that is one lower-case
     word of `a-z 0-9 _ . -` (at most 80 characters, not starting with `.` or `-`) is used as it
     is. Any other name (other characters such as `:` `+` or non-ASCII letters, upper case, a
     mapped task's `task_id[index]`) is written as its readable form (each run of other
     characters replaced by `_`) followed by `-` and 12 hex digits of the SHA-256 of the exact
     name: run id `scheduled__2026-10-06T00:00:00+00:00` becomes
     `scheduled__2026-10-06T00_00_00_00_00-b07b0ae9e551`. Two different names
     therefore never share a directory, a sealed plan, a log file or a ledger, also on a file
     system that ignores case. A task's log folder always carries the hash, also for a plain
     task id, so it is never a folder an older layout wrote to.
   - **One sealed plan per task, try series, config and export.** The artifact's name is made from the
     export, the task id (with the map index), the try series (`s<max_tries>`; Airflow moves
     `max_tries` when a task is cleared) and `<inputs>`: a digest of the config's absolute
     path, the config's bytes and the export's `query_file`. Two tasks of one DAG run never
     share a plan, also when they run two configs that both have an export of one name; a
     retry of the same task replays its own plan; a clear starts a new series and plans again,
     so a clear after a successful apply never replays a cursor snapshot that apply already
     moved past (rivet would refuse it `RIVET_PLAN_ARTIFACT_STALE`); a config or query file edited between two
     tries is planned again under a new name.
   - A retry replays its own plan only while the resolved config path and the config's bytes
     are unchanged. A config reached through a symlink that is re-pointed between two tries
     (a new git-sync revision of the DAG folder) is planned again, also when the bytes are
     the same, and leaves one more config copy and plan artifact in the state directory.
   - **The plan digest does not cover the environment.** Changing the source between two
     tries of one task instance (another URL in `env`, an edited Connection) replays the plan
     made for the old source against the new one. Clear the task in a new DAG run, or delete
     its artifact, after such a change.
   - The artifact records the export and the config it was planned from (`export_name`,
     `config_path`). `apply` refuses a plan object in which either is missing or is not the
     task's own (`RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN`), expired or not, and leaves the file as
     it is, so the retry is refused for the same reason until a person deletes it (or, once it has
     expired and is 24 h old, the pruning below removes it). The config
     is compared as a file, not as text: another case, Unicode form or symlink of the same
     file is the task's own, and a recorded path that no longer exists is not. A file at
     that path that is not a JSON object (empty, cut short) is planned over.
   - **Upgrading a state directory** written by an earlier build of this package: files of an
     older layout are left alone. They are never read, never pruned and never deleted, so
     remove `logs/<dag_id>/<task_id>/` folders without a `-<hash>` suffix and
     `airflow/<dag_id>/plans/<run_id>/<export>.json` files by hand when you no longer need
     them. A run in flight during the upgrade plans again, and its crashed budget starts empty
     (at most one extra crash retry).
   - `.rivet_airflow_marker`: a random id written by the first task that used the directory.

   The copies and every file under `airflow/` and `logs/` are created with mode `0600`, in
   directories of mode `0700` (an existing `airflow/` or `logs/` directory is tightened).
   **Moving an existing pipeline under Airflow:** if `.rivet_state.db` or a relative
   `cdc.checkpoint` file already sits beside your original config and not here, the task is
   refused before rivet starts (`RIVET_AIRFLOW_STATE_BESIDE_CONFIG`,
   `RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG`): the copy would not see it, and rivet would start
   from an empty state or re-anchor the stream. Stop the runs, move the file into the state
   directory (the state database with its `-wal` and `-shm` files; the checkpoint to the same
   relative path), and run again. Using the config's own directory as `state_dir` needs no copy
   and no move.
   **After `rivet init -o X`:** init records the source's primary keys in a `.rivet_state.db`
   beside `X`, which the task then refuses the same way: move it (with `-wal` and `-shm`) into
   `state_dir`, or run `init` with `RIVET_STATE_URL` set to the PostgreSQL state the tasks use.
   rivet 0.32.1 adds `RIVET_STATE_URL=sqlite:<path>`, so init and the tasks can name one file.
4. **What breaks when a task sees a different or an empty directory.** rivet creates a new,
   empty state there and treats every export as a first run: an incremental export re-reads
   from the start, a CDC stream re-anchors and the changes between the lost position and the new
   anchor are gone, with no error. What the operator can check without rivet's state identity:
   the directory exists, is absolute and writable, and its marker equals the marker of each
   direct upstream rivet task of the same DAG run (taken from their XCom); on SQLite state a
   difference is refused with `RIVET_AIRFLOW_STATE_DIR_DIFFERS`. On PostgreSQL state it is not:
   no task reads what another left in the directory, and several hosts, each with its own
   directory at one path, are the deployment that state is for (an extract on one host and its
   load on the other, measured by the live gate's multi-host leg). It cannot see a directory that
   was emptied between two DAG runs, or two hosts that each have their own copy.
5. **Relative paths in the config.** The copy changes the directory rivet takes as the
   config's, and nothing else:

   | Key | rivet resolves it against | Under this package |
   |---|---|---|
   | `query_file` | the config's directory; an absolute path, `..` and a symlink out of it are refused by rivet | stays as written; the file is copied to the same relative path in the state directory before every task. Refused before rivet starts (`RIVET_AIRFLOW_QUERY_FILE`) when it is absolute, has `..`, leaves the directory through a symlink, is missing for the task's export, or when a config from ANOTHER directory already put a file at that relative path in this state directory |
   | `cdc.checkpoint` | the config's directory | resolves in the state directory; a file left beside the original is refused, see 3 |
   | a local `destination.path` (or `prefix`) | the config's directory (rivet 0.32+) | the state directory: init's `./output/orders/` lands in `<state_dir>/output/orders/`; `cwd` does not move it. Write an absolute path to put the files elsewhere |
   | the other paths rivet opens as written, such as `tls.ca_file` | the process's working directory | unchanged: the working directory defaults to the directory of your ORIGINAL config; set `cwd` to change it |
   The preflight reads the config as rivet does: every `${name}` is taken from a `-p` /
   `--param` in `extra_args`, then from the task's environment, and spliced into the text before
   the YAML is parsed, with YAML 1.2 scalars (`017` and `name: 2024` stay the text written). A
   `mode` no variable resolves is treated as `cdc` by the guards; an export name or a
   `cdc.checkpoint` no variable resolves is refused (`RIVET_AIRFLOW_CONFIG_UNRESOLVED`), as rivet
   refuses the config.
6. **Single host.** SQLite state and the lock files are visible to one host only. Two workers
   on two hosts need PostgreSQL state.
7. **Moving to PostgreSQL state.** Stop the DAGs; create a database; set `RIVET_STATE_URL` to
   its `postgresql://` URL for every task (the worker's environment, or
   `env_from_connections`). This package does not copy the SQLite state into PostgreSQL: the
   new state starts empty, so plan the switch as a first run of every export (a full export
   simply runs again; an incremental export has no cursor to resume from). Keep `state_dir`
   for CDC: a checkpoint is a file on every state backend today, and it stays where it is.
