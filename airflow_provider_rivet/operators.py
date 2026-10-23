"""Airflow operators: thin adapters over `invocation.invoke`; none of them builds argv."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ._compat import (
    AirflowException,
    AirflowFailException,
    AirflowNotFoundException,
    AirflowSkipException,
    BaseHook,
    BaseOperator,
    executor_spreading_hosts,
)
from ._yaml import params_of
from .connections import (
    EnvSpec,
    build_environment,
    connection_url,
    database_ref,
    odbc_connection,
    with_database,
)
from .invocation import (
    CLOUD_ENV,
    Busy,
    Outcome,
    Request,
    Stopped,
    crashed_ledger,
    crashed_retry_allowed,
    invoke,
    spawn,
    worker_environment,
)
from .preflight import PreflightRefusal, read_config, resolve_binary
from .result import BUSY, REFUSED, STOPPED, unstarted
from .tasklog import TaskLog

XCOM_KEY = "return_value"


def get_connection(conn_id: str) -> Any:
    """The Airflow Connection the task names; a refusal when no backend defines it, since a retry cannot create it."""
    try:
        return BaseHook.get_connection(conn_id)
    except AirflowNotFoundException as exc:
        raise PreflightRefusal(
            "RIVET_AIRFLOW_CONNECTION_MISSING",
            f"connection `{conn_id}` is not defined in the metadata database or any secrets backend this worker reads; "
            "create it, or correct the id the task names",
        ) from exc


class RivetBaseOperator(BaseOperator):
    """Runs one rivet command on the worker and turns its ADR-0039 result into an Airflow outcome."""

    command = ""
    needs_export = False
    template_fields: Sequence[str] = (
        "config",
        "export",
        "table",
        "state_dir",
        "plan_file",
        "extra_args",
        "stderr_dir",
        "cwd",
    )
    ui_color = "#e8f0fe"
    _tasklog: TaskLog  # this try's task log; `execute` starts a new one before anything is recorded

    def __init__(
        self,
        *,
        config: str,
        export: str | None = None,
        table: str | None = None,
        state_dir: str | None = None,
        plan_file: str | None = None,
        rivet_bin: str = "rivet",
        env: Mapping[str, str] | None = None,
        env_from_connections: Mapping[str, Any] | None = None,
        env_passthrough: Sequence[str] = (),
        cloud_credentials: Sequence[str] = (),
        connections: Sequence[str] = (),
        deployment: str = "auto",
        extra_args: Sequence[str] | None = None,
        lock_wait: int = 600,
        rivet_notifications: bool = False,
        crashed_retries: int = 1,
        stderr_dir: str | None = None,
        stream_stderr: bool = False,
        log_keep: int | None = 10,
        cwd: str | None = None,
        single_host: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if self.needs_export and not export:
            raise ValueError(f"{type(self).__name__} needs `export`")
        if set(cloud_credentials) - set(CLOUD_ENV):
            raise ValueError(f"cloud_credentials names {sorted(CLOUD_ENV)}, got {sorted(cloud_credentials)}")
        self.config = config
        self.export = export
        self.table = table
        self.state_dir = state_dir
        self.plan_file = plan_file
        self.rivet_bin = rivet_bin
        self.env = env
        self.env_from_connections = env_from_connections
        self.env_passthrough = tuple(env_passthrough)
        self.cloud_credentials = tuple(cloud_credentials)
        self.connections = tuple(connections)
        self.deployment = deployment
        self.single_host = single_host
        self.extra_args = extra_args
        self.lock_wait = lock_wait
        self.rivet_notifications = rivet_notifications
        self.crashed_retries = crashed_retries
        self.stderr_dir = stderr_dir
        self.stream_stderr = stream_stderr
        self.log_keep = log_keep
        self.cwd = cwd
        self._proc: Any = None
        self._stop_requested = False  # pragma: no mutate - only ever read as a truth value, so None reads as False

    def _environment(self, secrets: Path) -> dict[str, str]:
        """The subprocess environment (`connections.build_environment`), with every Connection looked up here; key
        files go to `secrets`, which the task removes when rivet has exited."""
        refs = {name: database_ref(ref) for name, ref in (self.env_from_connections or {}).items()}
        spec = EnvSpec(
            passthrough=self.env_passthrough,
            clouds=self.cloud_credentials,
            env=self.env or {},
            urls={name: (self._database(ref), ref.get("scheme")) for name, ref in refs.items()},
            config=self._config_doc() if self.connections else {},
        )
        # The worker's environment: every reader of it takes None as os.environ, so its mutants are equivalent.
        return build_environment(os.environ, spec, [get_connection(c) for c in self.connections], secrets)  # pragma: no mutate

    def _database(self, ref: Mapping[str, Any]) -> Any:
        """The database an `env_from_connections` entry names: its Airflow Connection, else (with `odbc_ini`) the data
        source of that name in the file; its `database` instead of the one stored. Where it came from is logged."""
        conn_id, odbc_ini, dsn = ref["conn_id"], ref.get("odbc_ini"), ref.get("dsn") or ref["conn_id"]
        try:
            conn = BaseHook.get_connection(conn_id)
        except AirflowNotFoundException:
            conn = odbc_connection(odbc_ini, dsn) if odbc_ini else None
            if conn is None:
                raise PreflightRefusal(
                    "RIVET_AIRFLOW_CONNECTION_MISSING",
                    f"connection `{conn_id}` is not defined in the metadata database or any secrets backend this worker reads"
                    + (f", and {odbc_ini} has no data source [{dsn}]" if odbc_ini else "")
                    + "; create it, or correct the id the task names",
                ) from None
            self.log.info("database `%s`: no such Connection, ODBC data source [%s] in %s", conn_id, dsn, odbc_ini)
        return with_database(conn, ref.get("database"))

    def _config_doc(self) -> dict[str, Any]:
        """The config as rivet builds it, for the names a Connection is filed under; read by the preflight's reader, so a
        config it refuses is refused here too, and a `${...}` in `load.password_env` / `load.connection` resolves from
        `-p` and the task's environment (the worker's allow-listed variables and `env`) as rivet resolves it (D06)."""
        env = {**worker_environment(self.env_passthrough, self.cloud_credentials), **(self.env or {})}
        return read_config(self.config, {str(k): str(v) for k, v in env.items()}, params_of(self.extra_args or ()))

    def _emit(self, record: dict[str, Any]) -> None:
        """Keep one structured record: written as JSON at DEBUG, read into the task's readable lines (`tasklog`)."""
        self._tasklog.record(record)

    def _write(self, level: int, text: str) -> None:
        """One line of the task log at a `logging` level."""
        method = {logging.DEBUG: "debug", logging.INFO: "info", logging.WARNING: "warning"}.get(level, "error")
        getattr(self.log, method)("%s", text)  # pragma: no mutate - a logger formats no message given no args

    def _finish(self, ti: Any, payload: dict[str, Any]) -> None:
        """Write the task's readable block: the outcome's headline, a line per step, the notes."""
        self._tasklog.finish(payload, ti.try_number, ti.max_tries, self._stop_requested)

    def _hold(self, proc: Any) -> None:
        """Remember the running process so `on_kill` can stop it; one started after a stop is stopped at once (D40)."""
        self._proc = proc
        if self._stop_requested:
            proc.request_stop()

    def on_kill(self) -> None:
        """Ask rivet to stop when Airflow kills the task: a signal only, since this runs inside the SIGTERM handler;
        the step that started the process reaps it and kills it after the grace (D02). The stop is recorded first, so a
        stop that meets no running process (between two steps, before the first) still keeps the next one from
        starting: Airflow 3's handler returns instead of raising (D40)."""
        self._stop_requested = True
        if self._proc is not None:
            self._proc.request_stop()

    def _upstream_markers(self, ti: Any) -> list[str]:
        """State-directory markers the direct upstream rivet tasks of this run reported."""
        upstream = sorted(self.upstream_task_ids)
        if not upstream:
            return []
        pulled = ti.xcom_pull(task_ids=upstream, key=XCOM_KEY)  # pragma: no mutate - XCOM_KEY is Airflow's default key
        values = [] if pulled is None else [pulled] if isinstance(pulled, dict) else list(pulled)
        return [p["state_marker"] for p in values if isinstance(p, dict) and p.get("state_marker")]

    def _fail(self, ti: Any, payload: dict[str, Any], text: str, retry: bool) -> None:
        """Publish the payload and write the task's readable block, then raise the exception Airflow retries or the one
        it does not."""
        ti.xcom_push(key=XCOM_KEY, value=payload)
        self._finish(ti, payload)
        raise (AirflowException if retry else AirflowFailException)(text)

    def _refuse(self, ti: Any, refusal: PreflightRefusal) -> None:
        """Log and publish a refusal made before rivet started, then fail without retry."""
        self._emit({"event": "rivet.refused", "code": refusal.code, "text": str(refusal)})
        payload = unstarted(self.command, REFUSED, refusal.code)
        self._fail(ti, payload, str(refusal), retry=False)  # pragma: no mutate - `retry` is read as a truth value

    def _stopped(self, ti: Any, stop: Stopped) -> None:
        """Log and publish a stop Airflow asked for before a rivet step started, then fail for a retry (D40)."""
        self._emit({"event": "rivet.stopped", "step": stop.step, "text": str(stop)})
        self._fail(ti, unstarted(self.command, STOPPED, stop.step), str(stop), retry=True)

    def execute(self, context: Any) -> dict[str, Any]:
        """Run the command; return the XCom payload or raise by the contract's retry table."""
        ti = context["ti"]
        run_id = str(context.get("run_id") or getattr(ti, "run_id", "run"))
        map_index = getattr(ti, "map_index", None)
        task_part = ti.task_id if map_index in (-1, None) else f"{ti.task_id}[{map_index}]"
        self._tasklog = TaskLog(self.command, self.export, self.table, self._write)
        secrets = Path(tempfile.mkdtemp(prefix="rivet-airflow-secrets-"))
        try:
            return self._run_with_secrets(ti, run_id, task_part, secrets)
        finally:
            shutil.rmtree(secrets, ignore_errors=True)

    def _run_with_secrets(self, ti: Any, run_id: str, task_part: str, secrets: Path) -> dict[str, Any]:
        """`execute` with a private folder for the key files of this try."""
        try:
            env = self._environment(secrets)
        except PreflightRefusal as refusal:
            self._refuse(ti, refusal)
        request = Request(
            command=self.command,
            config=self.config,
            env=env,
            export=self.export,
            table=self.table,
            rivet_bin=self.rivet_bin,
            state_dir=self.state_dir,
            deployment=self.deployment,
            spreads_hosts=None if self.single_host else executor_spreading_hosts(),
            extra_args=list(self.extra_args or ()),
            lock_wait=self.lock_wait,
            rivet_notifications=self.rivet_notifications,
            plan_file=self.plan_file,
            cwd=self.cwd,
            stderr_dir=self.stderr_dir,
            stream_stderr=self.stream_stderr,
            log_keep=self.log_keep,
            label=(ti.dag_id, run_id, task_part, ti.try_number),
            series=ti.max_tries,
            upstream_markers=self._upstream_markers(ti),
        )
        try:
            outcome = invoke(request, self._emit, self._hold, lambda: self._stop_requested)
        except PreflightRefusal as refusal:
            self._refuse(ti, refusal)
        except Stopped as stop:
            self._stopped(ti, stop)
        except Busy as busy:
            self._emit({"event": "rivet.busy", "code": busy.code, "text": str(busy)})
            self._fail(ti, unstarted(self.command, BUSY, busy.code), str(busy), retry=True)
        finally:
            self._proc = None
        return self._settle(ti, run_id, task_part, outcome)

    def _settle(self, ti: Any, run_id: str, task_part: str, outcome: Outcome) -> dict[str, Any]:
        """Map the outcome's decision onto success, skip, retry or fail."""
        payload = outcome.to_xcom()
        decision = outcome.decision
        if decision == "success":
            if self.command == "compact" and outcome.units and all(u.status == "skipped" for u in outcome.units):
                ti.xcom_push(key=XCOM_KEY, value=payload)
                self._finish(ti, payload)
                raise AirflowSkipException(f"compact skipped: {outcome.skip_reason}")
            stops = [u.export for u in outcome.units if u.stop_reason == "max_events"]
            if stops:
                self._emit({"event": "rivet.cdc.max_events", "exports": stops, "text": "stopped on max_events; backlog remains"})
            self._finish(ti, payload)
            return payload
        assert outcome.error is not None
        failed = [u.export for u in outcome.units if u.status == "failed"]
        text = outcome.error.describe(failed[0] if len(failed) == 1 else self.export)
        if decision == "retry_crashed":
            ledger = None
            # The ledger is a file on this worker's disk: it counts every crash only while every try runs here, which
            # SQLite state guarantees and PostgreSQL state does not (a retry on another host saw an empty ledger and
            # granted the budget again). Elsewhere the budget is the first `crashed_retries` tries, as on a pod.
            if outcome.marker is not None and self.state_dir and outcome.state_kind == "sqlite":
                ledger = crashed_ledger(self.state_dir, ti.dag_id, run_id, task_part)
            allowed, problem = crashed_retry_allowed(ledger, ti.try_number, ti.max_tries, self.crashed_retries)
            payload["crashed_retry"] = allowed
            if ledger is None or problem:
                payload["degraded"] = sorted({*payload["degraded"], "crashed_ledger"})
            self._emit(
                {
                    "event": "rivet.crashed",
                    "retry": allowed,
                    "budget": self.crashed_retries,
                    "try_number": ti.try_number,
                    "airflow_tries_left": ti.try_number <= ti.max_tries,
                    "ledger_problem": problem,
                }
            )
            self._fail(ti, payload, text, retry=allowed)
        self._fail(ti, payload, text, retry=decision == "retry")
        return payload


class RivetRunOperator(RivetBaseOperator):
    """`rivet run`, for one export or the whole config."""

    command = "run"


class RivetApplyOperator(RivetBaseOperator):
    """`rivet plan -e <export>` into a sealed artifact, then `rivet apply` of it; a retry replays the artifact."""

    command = "apply"
    needs_export = True


class RivetCdcRunOperator(RivetBaseOperator):
    """One bounded CDC drain of a stream; succeeds on `max_events` and reports it."""

    command = "cdc_run"
    needs_export = True
    ui_color = "#e6f4ea"


class RivetLoadOperator(RivetBaseOperator):
    """`rivet load` for one export, and one table where the binary can narrow to it."""

    command = "load"
    ui_color = "#fef7e0"


class RivetCompactOperator(RivetBaseOperator):
    """`rivet compact` for one export; a compaction rivet reports as skipped is an Airflow skip."""

    command = "compact"
    ui_color = "#fce8e6"


class RivetPlanOperator(RivetBaseOperator):
    """`rivet plan --format json` for the whole config, written atomically to `plan_file`."""

    command = "plan"

    def __init__(self, *, plan_file: str, **kwargs: Any) -> None:
        super().__init__(plan_file=plan_file, **kwargs)


def config_present(path: Path) -> bool:
    """Whether the config file is there, by opening it: a stat can answer from a stale cache (Docker Desktop's file
    sharing kept a deleted file `exists()`), and the generator and `init` must agree with the reader that follows."""
    try:
        path.open("rb").close()
    except FileNotFoundError:
        return False
    return True


def places(doc: Mapping[str, Any]) -> set[str]:
    """Where a config writes: each export's bucket prefix (`s3://b/p/`) and its warehouse dataset or database."""
    out = set()
    for export in doc.get("exports") or []:
        dest = export.get("destination") or {}
        if dest.get("bucket"):
            out.add(f"{dest.get('type')}://{dest['bucket']}/{dest.get('prefix') or ''}")
    load = doc.get("load") or {}
    names = [load.get(k) for k in ("project", "dataset", "database", "schema") if load.get(k)]
    if names:
        out.add(f"{load.get('target')} {'.'.join(map(str, names))}")
    return out


class RivetInitOperator(RivetBaseOperator):
    """Writes the config a source's DAG is built from: `rivet init` against the source, `rivet check` of what it wrote,
    then the file at `config`, never over one that exists. The data source is the `source_env` entry of
    `env_from_connections`; a `database` named for RIVET_STATE_URL is created on its PostgreSQL server if missing."""

    command = "init"
    template_fields: Sequence[str] = ("config", "state_dir", "init_args")
    ui_color = "#f3e8fd"

    def __init__(self, *, mode: str, source_env: str, init_args: Sequence[str] = (), **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if mode not in ("cdc", "batch"):
            raise ValueError("`mode` is 'cdc' or 'batch'")
        if not (self.env_from_connections or {}).get(source_env):
            raise ValueError(f"env_from_connections names no `{source_env}`, the source `rivet init` reads")
        owned = sorted({"--source", "--source-env", "--source-file", "-o", "--output", "--mode"} & set(init_args))
        if owned:
            raise ValueError(f"init_args may not set {owned}: the operator sets the source, the output and the mode")
        self.mode = mode
        self.source_env = source_env
        self.init_args = list(init_args)

    def _run_with_secrets(self, ti: Any, run_id: str, task_part: str, secrets: Path) -> dict[str, Any]:
        """Write the config, or report the one already there; `execute` gives the private folder for key files."""
        target = Path(self.config)
        if config_present(target):
            self.log.info(
                "%s exists: the next DAG parse builds this DAG's rivet tasks from it. If this task keeps running instead, "
                "the scheduler does not see this file: put the config folder where both read it",
                target,
            )
            return {"config": str(target), "written": False}
        try:
            env = self._environment(secrets)
            binary = resolve_binary(self.rivet_bin, env)
            self._create_state_database()
        except PreflightRefusal as refusal:
            self._refuse(ti, refusal)
        try:
            return self._write_config(target, env, binary)
        except Stopped as stop:
            self._stopped(ti, stop)
        finally:
            self._proc = None

    def _config_doc(self) -> dict[str, Any]:
        """No config yet: a storage or warehouse Connection is filed under its own name."""
        return {}

    def _create_state_database(self) -> None:
        """CREATE DATABASE for a RIVET_STATE_URL entry that names a `database`, on the server its Connection points at."""
        ref = (self.env_from_connections or {}).get("RIVET_STATE_URL")
        ref = database_ref(ref) if ref else {}
        if not ref.get("database"):
            return
        server = self._database({**ref, "database": None})
        name = ref["database"]
        try:
            import psycopg2
        except ImportError:
            raise PreflightRefusal(
                "RIVET_AIRFLOW_STATE_DATABASE", f"creating state database `{name}` needs psycopg2 on the worker"
            ) from None
        conn = psycopg2.connect(connection_url(with_database(server, server.schema or "postgres")))
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
                if cur.fetchone():
                    return
                quoted = '"' + name.replace('"', '""') + '"'
                cur.execute(f"CREATE DATABASE {quoted}")
                self.log.info("state database `%s` created on %s", name, server.host)
        finally:
            conn.close()

    def _rivet(self, binary: str, argv: list[str], env: Mapping[str, str], cwd: Path) -> None:
        """Run one rivet command, stoppable by `on_kill`, its output in the task log; a nonzero exit fails the task
        without a retry. A stop requested before it starts raises `Stopped`."""
        if self._stop_requested:
            raise Stopped(argv[0])
        output = cwd / f"{argv[0]}.log"
        with output.open("w") as out:
            code = spawn([binary, *argv], out, subprocess.STDOUT, env, str(cwd), self._hold)
        for line in output.read_text(errors="replace").splitlines():
            self.log.info(
                "%s", line
            )  # pragma: no mutate - info(line) is the same line: a logger formats no message given no args
        if code != 0:
            raise AirflowFailException(f"`rivet {argv[0]}` exited {code}; its output is above, and no config was written")

    def _refuse_shared_places(self, doc: Mapping[str, Any], target: Path) -> None:
        """Refuse a config that writes where another config of the folder writes: one bucket prefix (their files mix) or
        one warehouse dataset or database (two sources' tables of one name become one table)."""
        mine = places(doc)
        for other in sorted(target.parent.glob("*.yaml")):
            if other == target:
                continue
            try:
                shared = sorted(mine & places(read_config(str(other))))
            except PreflightRefusal:
                continue  # a config this reader cannot read is no evidence either way
            if shared:
                raise AirflowFailException(
                    f"the config `rivet init` wrote for {target.name} writes to {', '.join(shared)}, as {other} does; give each "
                    "source its own place in init_args (`--gcs-bucket`/`--s3-bucket`, `--bigquery-dataset {name}`, "
                    "`--clickhouse-database {name}`); no config was written"
                )

    def _write_config(self, target: Path, env: Mapping[str, str], binary: str) -> dict[str, Any]:
        """`rivet init` into the state folder (where the tasks run the config), `rivet check` there, then `target`."""
        state_dir = Path(self.state_dir or target.parent)
        if not state_dir.parent.is_dir():  # a state root that is not there is a volume not mounted: never made here
            raise AirflowFailException(f"{state_dir.parent} does not exist on this worker: mount the state folder first")
        state_dir.mkdir(exist_ok=True)
        work = state_dir / ".init"
        work.mkdir(exist_ok=True)
        draft = work / target.name  # init names the stream's server_id, slot, checkpoint and prefix after this stem
        draft.unlink(missing_ok=True)
        mode = ["--mode", "cdc"] if self.mode == "cdc" else []
        self._rivet(binary, ["init", "--source-env", self.source_env, *mode, "-o", str(draft), *self.init_args], env, work)
        self._rivet(binary, ["check", "--config", str(draft)], env, work)
        doc = read_config(str(draft))  # init writes `url_env:`, no `${...}`
        target.parent.mkdir(parents=True, exist_ok=True)
        self._refuse_shared_places(doc, target)
        staged = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        shutil.copyfile(draft, staged)
        try:
            os.link(staged, target)  # refuses when another try wrote it meanwhile: a config is never replaced
        except FileExistsError:
            self.log.info("%s was written meanwhile; it is kept", target)
        finally:
            staged.unlink()
        exports = [{"name": e.get("name"), "mode": e.get("mode"), "tables": e.get("tables")} for e in doc.get("exports") or []]
        for e in exports:
            self.log.info(
                "export %s (%s)%s", e["name"], e["mode"], f": {', '.join(map(str, e['tables']))}" if e["tables"] else ""
            )
        self.log.info("%s written; the next DAG parse replaces this task with the rivet tasks it describes", target)
        return {"config": str(target), "written": True, "exports": exports}


class RivetWaveBarrier(BaseOperator):
    """Does nothing: an ordering point between two waves, run when the wave before it has finished."""

    ui_color = "#f1f3f4"

    def execute(self, context: Any) -> None:
        """Nothing to run."""
        return None


class RivetRunWatcher(BaseOperator):
    """Fails when any task of the run failed, so the DAG run is failed whichever tasks are its leaves."""

    ui_color = "#f1f3f4"

    def execute(self, context: Any) -> None:
        """Runs only under `trigger_rule="one_failed"`; always fails."""
        raise AirflowFailException("a task of this DAG run failed; see the failed and upstream_failed tasks")
