"""Test harness: a throwaway AIRFLOW_HOME, a fake `rivet`, a plain task instance, and real DAG runs through `dag.test()`."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import logging
import os
import shutil
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="rivet-provider-airflow-home-")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
RIVET_SRC = Path(os.environ.get("RIVET_SRC") or PACKAGE_ROOT.parent / "rivet").resolve()
FIXTURES = RIVET_SRC / "tests" / "fixtures" / "scheduler"
SECRET = "postgresql://rivet:s3cr3t-pw@db.internal:5432/app"
# Flags no rivet release has yet, which the package probes `--help` for (capabilities.DEGRADATIONS). The fake binary
# accepts them only in a scenario that declares them, as a switch (`--name`) or with a value (`--name <V>`).
NEW_FLAGS = {
    "*": ["--no-notify", "--lock-wait <SECONDS>"],
    "apply": ["--summary-output <PATH>"],
    "load": ["--export <EXPORT>", "--table <TABLE>", "--summary-output <PATH>"],
    "compact": ["--export <EXPORT>", "--table <TABLE>", "--summary-output <PATH>"],
}
# What a process's environment holds whatever its parent passed: macOS CoreFoundation's variable, and the LC_CTYPE the
# fake's own Python interpreter sets when it coerces a C locale (PEP 538). Real rivet would see neither from the package.
PLATFORM_ENV = {"__CF_USER_TEXT_ENCODING", "LC_CTYPE"}
# A probe of the binary (`--version`, `<sub> --help`) runs with these variables only (capabilities.Capabilities._run).
PROBE_ENV = {"PATH", "HOME"}
CONFIG = """\
source:
  type: postgres
  url_env: RIVET_PG_URL
exports:
  - name: orders
    query: SELECT * FROM orders
    mode: full
  - name: users
    query: SELECT * FROM users
    mode: incremental
  - name: orders_cdc
    mode: cdc
  - name: events
    mode: incremental
  - name: countries
    mode: full
"""


def fixture(name: str) -> Any:
    """One pinned contract fixture of the Rust repo."""
    return json.loads((FIXTURES / name).read_text())


class FakeTI:
    """The task-instance surface the operators use."""

    def __init__(self, task_id: str = "task", try_number: int = 1, max_tries: int = 2, store: dict | None = None) -> None:
        self.dag_id = "dag"
        self.task_id = task_id
        self.run_id = "manual__2026-10-06"
        self.try_number = try_number
        self.max_tries = max_tries
        self.map_index = -1
        self.store = store if store is not None else {}

    def xcom_push(self, key: str, value: Any) -> None:
        """Keep the value under (task, key)."""
        json.dumps(value)
        self.store[(self.task_id, key)] = value

    def xcom_pull(self, task_ids: Any = None, key: str = "return_value") -> Any:
        """One value for a task id; for several, a sequence with Airflow 2.10's truth-value trap."""
        if isinstance(task_ids, str):
            return self.store.get((task_ids, key))
        return LazyPull([self.store[(t, key)] for t in task_ids if (t, key) in self.store])


class LazyPull:
    """`LazyXComSelectSequence` as Airflow 2.10.5 behaves: iterable, and `bool()` of an empty one raises."""

    def __init__(self, values: list) -> None:
        self.values = values

    def __iter__(self) -> Any:
        return iter(self.values)

    def __bool__(self) -> bool:
        if not self.values:
            raise TypeError("__bool__ should return bool, returned NoneType")
        return True


class Rig:
    """A fake binary, a config, a state directory, and the calls and log records of one test."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.bin = tmp / "bin"
        self.bin.mkdir()
        body = (Path(__file__).parent / "fake_rivet.py").read_text()
        script = self.bin / "rivet"
        script.write_text(f"#!{sys.executable}\n{body}")
        script.chmod(0o755)
        shutil.copy(Path(__file__).parent / "rivet_cli_surface.json", self.bin / "rivet_cli_surface.json")
        self.state_dir = tmp / "state"
        self.state_dir.mkdir()
        self.config_dir = tmp / "cfg"
        self.config_dir.mkdir()
        self.config = self.config_dir / "pg.yaml"
        self.config.write_text(CONFIG)
        self.records: list[dict] = []
        self.spec: dict = {}
        self.scenario()

    def scenario(self, **spec: Any) -> None:
        """Set what the fake binary does next."""
        self.spec = {"record_env": ["RIVET_PG_URL", "RIVET_STATE_URL"], "leak_env": ["RIVET_PG_URL"], **spec}
        (self.bin / "scenario.json").write_text(json.dumps(self.spec))

    def copy(self, export: str | None = None, config: Path | None = None) -> Path:
        """Where the package materialises a config in the state directory: named after the source path's hash."""
        source = (config or self.config).resolve()
        digest = hashlib.sha256(str(source).encode()).hexdigest()[:8]
        suffix = f"--{export}" if export else ""
        return self.state_dir / f"{source.stem}.{digest}{suffix}{source.suffix}"

    def calls(self) -> list[dict]:
        """Every non-probe call the fake binary received."""
        log = self.bin / "calls.jsonl"
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def invocations(self, kind: str | None = None) -> list[dict]:
        """Every start of the fake binary (`kind`: "probe", "call" or "refused"), each with its whole environment."""
        log = self.bin / "invocations.jsonl"
        rows = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        if kind == "refused":
            self.refusals_read = True
        return [r for r in rows if kind in (None, r["kind"])]

    def probes(self) -> list[dict]:
        """The `--version` / `<sub> --help` probes the fake binary answered."""
        return self.invocations("probe")

    def assert_probes_minimal_env(self, allowed: set[str] = PROBE_ENV) -> None:
        """Every probe saw no variable beyond `allowed`, with the values the worker has: a probe that inherits the
        worker's environment hands its secrets (Fernet key, cloud keys, the Connection's URL) to a process that needs none."""
        probes = self.probes()
        assert probes, "no probe reached the binary"
        for row in probes:
            extra = set(row["env"]) - allowed - PLATFORM_ENV
            assert not extra, f"{row['argv']} saw {sorted(extra)}"
            for name in allowed & set(row["env"]):
                assert row["env"][name] == os.environ.get(name, ""), f"{row['argv']}: {name} is not the worker's"

    def unread_refusals(self) -> list[dict]:
        """Command lines the fake refused as clap would (exit 2) that the test never looked at: a test that passes over
        one is green by a refusal it did not mean (the rig fixtures fail such a test at teardown)."""
        if getattr(self, "refusals_read", False):
            return []
        log = self.bin / "invocations.jsonl"
        rows = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
        return [r for r in rows if r["kind"] == "refused"]

    def argv(self, sub: str) -> list[str]:
        """The argv of the last call of one subcommand."""
        return [c["argv"] for c in self.calls() if c["argv"][0] == sub][-1]

    def operator(self, cls: Any, **kwargs: Any) -> Any:
        """An operator wired to the fake binary. Its structured records are read back from the task log (`task_log`)."""
        kwargs.setdefault("task_id", "task")
        kwargs.setdefault("config", str(self.config))
        kwargs.setdefault("state_dir", str(self.state_dir))
        kwargs.setdefault("env", {"RIVET_PG_URL": SECRET})
        kwargs.setdefault("deployment", "local")
        return cls(rivet_bin=str(self.bin / "rivet"), retries=2, **kwargs)

    def run(self, op: Any, ti: FakeTI | None = None) -> tuple[Any, FakeTI]:
        """Execute an operator; return (payload or raised exception, the task instance)."""
        ti = ti or FakeTI(task_id=op.task_id)
        try:
            return op.execute({"ti": ti, "run_id": ti.run_id}), ti
        except Exception as exc:
            return exc, ti

    def events(self, name: str) -> list[dict]:
        """Log records of one event name."""
        return [r for r in self.records if r.get("event") == name]


@pytest.fixture(autouse=True)
def own_temp_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Each test gets its own `tempfile.gettempdir()`, so a stateless worker's files stay inside the test."""
    path = tmp_path / "tmp"
    path.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(path))
    monkeypatch.delenv("RIVET_STATE_URL", raising=False)
    return path


class TaskLog(logging.Handler):
    """The task log as Airflow writes it: every line the operators' logger emits, formatted by `logging` the way a task
    log handler formats it (`%(message)s` is `msg % args`), at DEBUG and above.

    `lines` holds (level name, message) per line; `records` the structured records parsed back from their DEBUG JSON
    lines, i.e. from the text that reached the log, not from the dict before `TaskLog.record` formatted it."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        self.lines: list[tuple[str, str]] = []
        self.records: list[dict] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.format(record)
        except Exception:  # a line `logging` cannot format never reaches the log: it is dropped, as a handler drops it
            self.handleError(record)
            return
        self.lines.append((record.levelname, record.message))
        if record.levelno == logging.DEBUG and record.message.startswith("{"):
            self.records.append(json.loads(record.message))

    def shown(self) -> list[str]:
        """The lines a default Airflow task log shows (INFO and above), each as `LEVEL text`."""
        return [f"{level} {text}" for level, text in self.lines if level != "DEBUG"]

    def debug(self) -> list[str]:
        """The exact text of every DEBUG line."""
        return [text for level, text in self.lines if level == "DEBUG"]

    def text(self) -> str:
        """The whole log, one `LEVEL text` per line."""
        return "\n".join(f"{level} {text}" for level, text in self.lines)


@pytest.fixture
def task_log(monkeypatch: pytest.MonkeyPatch) -> TaskLog:
    """Every operator of the test logs through one real `logging.Logger` (the name Airflow gives an operator's logger,
    DEBUG, no propagation) into one `TaskLog`."""
    from airflow_provider_rivet.operators import RivetBaseOperator

    capture = TaskLog()
    logger = logging.Logger("airflow.task.operators.airflow_provider_rivet.operators", logging.DEBUG)
    logger.addHandler(capture)
    monkeypatch.setattr(RivetBaseOperator, "log", property(lambda self: logger))
    return capture


@pytest.fixture
def rig(tmp_path: Path, task_log: TaskLog) -> Iterator[Rig]:
    """A fresh rig per test; a command line the fake refused and the test did not read fails it."""
    rig = Rig(tmp_path)
    rig.task_log, rig.records = task_log, task_log.records
    yield rig
    refused = rig.unread_refusals()
    assert not refused, (
        f"rivet refused a command line (exit 2) the test did not expect: {refused[0]['argv']}: {refused[0]['refusal']}"
    )


@pytest.fixture(scope="session")
def airflow_db() -> str:
    """A migrated SQLite metadata database in the throwaway AIRFLOW_HOME; the path of that home."""
    from airflow.utils import db

    db.resetdb()
    return os.environ["AIRFLOW_HOME"]


class DagRig(Rig):
    """A rig whose DAGs are written to the dags folder and run by `dag.test()` with real task instances."""

    def __init__(self, tmp: Path, home: str) -> None:
        super().__init__(tmp)
        self.home = Path(home)
        (self.home / "dags").mkdir(exist_ok=True)

    def run_dag(
        self, builder: str, dag_id: str, env: dict | None = None, failures_to: Path | None = None, **kwargs: Any
    ) -> tuple[str, dict[str, str]]:
        """Build a DAG with one of the package's builders and run it; (run state, state by task id).

        With `failures_to`, `default_args` carries a failure callback that appends the failed task's id to that file.
        """
        kwargs.setdefault("config", str(self.config))
        kwargs.setdefault("state_dir", str(self.state_dir))
        kwargs.setdefault("default_args", {"retries": 0})
        op = {"rivet_bin": str(self.bin / "rivet"), "deployment": "local"}
        kwargs["operator_kwargs"] = {**op, **kwargs.get("operator_kwargs", {})}
        env = env or {"RIVET_PG_URL": SECRET}
        return self.run_source(
            dag_id,
            "import os\n"
            f"from airflow_provider_rivet.dags import {builder}\n"
            f"kwargs = {kwargs!r}\n"
            f"kwargs['operator_kwargs']['env'] = {{name: os.environ[name] for name in {sorted(env)!r}}}\n"
            "def record(context):\n"
            f"    with open({str(failures_to)!r}, 'a') as log:\n"
            "        log.write(context['ti'].task_id + '\\n')\n"
            + ("kwargs['default_args']['on_failure_callback'] = record\n" if failures_to else "")
            + f"dag = {builder}({dag_id!r}, **kwargs)\n",
            env,
        )

    def run_source(self, dag_id: str, source: str, env: dict | None = None) -> tuple[str, dict[str, str]]:
        """Write a DAG file whose module defines `dag`, run it; (run state, state by task id, `[i]` added when mapped)."""
        env = env or {"RIVET_PG_URL": SECRET}
        path = self.home / "dags" / f"{dag_id}.py"
        path.write_text(source)
        before = {name: os.environ.get(name) for name in env}
        os.environ.update(env)
        try:
            spec = importlib.util.spec_from_file_location(dag_id, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            run = module.dag.test()
            states = {
                ti.task_id + ("" if ti.map_index in (-1, None) else f"[{ti.map_index}]"): str(
                    getattr(ti.state, "value", ti.state)
                )
                for ti in run.get_task_instances()
            }
            return str(getattr(run.state, "value", run.state)), states
        finally:
            path.unlink()
            for name, value in before.items():
                os.environ.pop(name, None) if value is None else os.environ.__setitem__(name, value)

    def ran(self) -> list[tuple[str, str]]:
        """(subcommand, export) of every call the fake binary received."""
        return [(c["argv"][0], c["export"]) for c in self.calls()]


@pytest.fixture
def dag_rig(tmp_path: Path, airflow_db: str) -> Iterator[DagRig]:
    """A fresh rig per test over the session's metadata database; unread refusals fail it, as for `rig`."""
    rig = DagRig(tmp_path, airflow_db)
    yield rig
    refused = rig.unread_refusals()
    assert not refused, (
        f"rivet refused a command line (exit 2) the test did not expect: {refused[0]['argv']}: {refused[0]['refusal']}"
    )
