"""The one place a rivet process is built, run, read and classified; it imports nothing from Airflow."""

from __future__ import annotations

import contextlib
import ctypes
import fcntl
import fnmatch
import functools
import hashlib
import itertools
import json
import os
import re
import shutil
import signal as _signal
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import _yaml
from .capabilities import ProbeFailed
from .preflight import STATE_URL_ENV, Preflight, PreflightRefusal, binary_unanswering, run_preflight, state_kind
from .reading import Reading, _json_of, _Step, config_target  # noqa: F401  (config_target is re-exported)
from .result import ErrorObject, UnitResult, worst

Emit = Callable[[dict[str, Any]], None]
ENV_ALLOWLIST = (
    "PATH", "HOME", "USER", "LOGNAME", "TZ", "LANG", "LC_*", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR",
    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "no_proxy", "all_proxy",
    "KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT",
)  # fmt: skip
CLOUD_ENV = {
    "gcp": ("GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CLOUD_PROJECT"),
    "aws": ("AWS_*",),
    "azure": ("AZURE_*",),
}
STDERR_TAIL = 1 << 20
STOP_GRACE = 30.0  # seconds a stopped rivet process gets between SIGTERM and SIGKILL
QUERY_OWNERS = ".rivet_airflow_query_files.json"
STALE_AFTER = 24 * 3600  # seconds after which an expired plan artifact or an untouched crashed ledger is pruned
PLAN_ERRORS = (ValueError, KeyError, IndexError, TypeError, AttributeError)
_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")
_PLAIN = re.compile(r"[a-z0-9_][a-z0-9_.-]{0,79}")
_HASHED = re.compile(r".*-[0-9a-f]{12}")
_LOG_FILE = re.compile(r"^(?P<attempt>.+)\.[a-z]+\.(?:stderr|stdout)\.log$")
LOCK_POLL = 0.2  # seconds between two tries of a held export lock
BUSY_CODE = "RIVET_AIRFLOW_EXPORT_BUSY"
PR_SET_PDEATHSIG = 1  # <linux/prctl.h>


def worker_environment(
    passthrough: Sequence[str] = (), clouds: Sequence[str] = (), environ: Mapping[str, str] | None = None
) -> dict[str, str]:
    """The worker's variables a rivet process may inherit: the allow-list, plus the named extras."""
    patterns = [*ENV_ALLOWLIST, *passthrough, *(name for cloud in clouds for name in CLOUD_ENV[cloud])]
    source = os.environ if environ is None else environ
    return {k: v for k, v in source.items() if any(fnmatch.fnmatchcase(k, pattern) for pattern in patterns)}


def hashed_name(*parts: Any) -> str:
    """A readable form of the parts plus 12 hex digits of the SHA-256 of their JSON list; never a plain name."""
    texts = [str(part) for part in parts]
    readable = "__".join(_SAFE.sub("_", text) for text in texts)[:80]
    return f"{readable}-{hashlib.sha256(json.dumps(texts).encode()).hexdigest()[:12]}"


def path_name(*parts: Any) -> str:
    """One path component for user-supplied names, never shared by two different inputs.

    A lone lower-case name of safe characters is kept as it is. Anything else (other characters, upper case,
    several parts, a leading dot, over 80 characters, or a name shaped like `hashed_name`'s output) is hashed.
    """
    texts = [str(part) for part in parts]
    if len(texts) == 1 and _PLAIN.fullmatch(texts[0]) and not _HASHED.fullmatch(texts[0]):
        return texts[0]
    return hashed_name(*texts)


def undeclared_state_variables(env: Mapping[str, str], environ: Mapping[str, str] | None = None) -> list[str]:
    """The state variable, when the worker's value selects PostgreSQL state and the task's environment has no entry for it."""
    source = os.environ if environ is None else environ
    return [STATE_URL_ENV] if state_kind(source) == "postgres" and STATE_URL_ENV not in env else []


def plan_layout(plan: Any) -> list[dict[str, Any]]:
    """Waves of a `rivet plan --format json` document, lowest first; raises one of PLAN_ERRORS when it is not one.

    Several planned exports print as an array whose objects carry the campaign; one planned export prints as its
    bare object with no campaign, and its one wave is its own `export_recommendation` (D10).
    """
    if isinstance(plan, dict):
        rec = plan["prioritization"]["export_recommendation"]
        name = plan["export_name"]
        if not isinstance(name, str) or rec["export_name"] != name:  # no name there: KeyError, as the cost line read it
            raise ValueError("the plan's recommendation names another export")
        campaign = {"waves": [{"wave": int(rec["recommended_wave"]), "exports": [name]}], "ordered_exports": [rec]}
    else:
        campaign = plan[0]["prioritization"]["campaign"]
    cost = {e["export_name"]: e.get("cost_class") for e in campaign.get("ordered_exports", [])}
    waves = sorted(campaign["waves"], key=lambda w: w["wave"])
    layout = [
        {"wave": w["wave"], "exports": list(w["exports"]), "heavy": [n for n in w["exports"] if cost.get(n) != "low"]}
        for w in waves
        if w["exports"]
    ]
    if not layout:
        raise ValueError("the plan has no wave with an export")
    return layout


@dataclass
class Request:
    """Everything one rivet task needs; secrets are only ever inside `env`."""

    command: str
    config: str
    env: Mapping[str, str]
    export: str | None = None
    table: str | None = None
    rivet_bin: str = "rivet"
    state_dir: str | None = None
    deployment: str = "auto"
    # The scheduler's executor when it may run one DAG's tasks on several hosts and the DAG did not declare one host.
    spreads_hosts: str | None = None
    extra_args: Sequence[str] = ()
    lock_wait: int = 600
    rivet_notifications: bool = False
    plan_file: str | None = None
    cwd: str | None = None
    stderr_dir: str | None = None
    stream_stderr: bool = False
    log_keep: int | None = 10
    label: Sequence[Any] = ("dag", "run", "task", 1)
    series: int | None = None
    upstream_markers: Sequence[str] = ()


def decision_of(error: ErrorObject | None) -> str:
    """The retry table: `success`, `retry` (transient budget), `retry_crashed` (its own budget) or `fail`.

    A plain function, not the property body, so mutation testing reaches it (mutmut skips decorated functions)."""
    if error is None:
        return "success"
    if error.class_ == "crashed":
        return "retry_crashed"
    return "retry" if error.retryable else "fail"


@dataclass
class Outcome:
    """What a task reports: its units, the representative error and what the binary could not do."""

    command: str
    units: list[UnitResult] = field(default_factory=list)
    error: ErrorObject | None = None
    exit_status: int | None = 0
    signal: int | None = None
    degraded: list[str] = field(default_factory=list)
    stderr_path: str | None = None
    marker: str | None = None
    # `sqlite` or `postgres`: where rivet's state lives, so the operator knows whether this worker's disk is the only
    # one its tries run on (not in XCom).
    state_kind: str | None = field(default=None, compare=False)
    rivet_version: str | None = None
    skip_reason: str | None = None

    @property
    def decision(self) -> str:
        """`success`, `retry` (transient budget), `retry_crashed` (its own budget) or `fail`."""
        return decision_of(self.error)

    def to_xcom(self) -> dict[str, Any]:
        """The XCom payload: units without `message`, never an environment value."""
        return {
            "command": self.command,
            "decision": self.decision,
            "units": [u.to_xcom() for u in self.units],
            "error": self.error.to_xcom() if self.error else None,
            "degraded": list(self.degraded),
            "exit_status": self.exit_status,
            "signal": self.signal,
            "stderr_path": self.stderr_path,
            "skip_reason": self.skip_reason,
            "state_marker": self.marker,
            "rivet_version": self.rivet_version,
        }


class Stopped(Exception):
    """Airflow asked the task to stop while no rivet process was running: the next step is not started (D40)."""

    def __init__(self, step: str) -> None:
        super().__init__(f"the task was stopped before rivet {step} started; rivet {step} was not run")
        self.step = step


class Busy(Exception):
    """The export's lock stayed held for `lock_wait` seconds: a try of this task, or a rivet process one left behind,
    still works on the export. No rivet process was started; the task is retried (G2)."""

    code = BUSY_CODE

    def __init__(self, lock: Path, unit: str, waited: int) -> None:
        super().__init__(
            f"[{BUSY_CODE}] the lock {lock} of {unit} stayed held for {waited} s: another try of this task, or a rivet "
            "process an earlier try left running, still works on it. rivet was not started; the task is retried"
        )


class _Session:
    """One task's invocation: its preflight result, working files and collected degradations."""

    def __init__(
        self, request: Request, pf: Preflight, emit: Emit, on_process: Callable | None, stopped: Callable[[], bool] | None = None
    ) -> None:
        self.req = request
        self.pf = pf
        self.emit = emit
        self.on_process = on_process
        self.stopped = stopped
        self.degraded: set[str] = set()
        self.read = Reading(self.degraded, pf.config, request.command)
        self.stderr_path: str | None = None
        self.tmp: Path | None = None
        self.lock_fd: int | None = None
        dag, run = (path_name(part) for part in request.label[:2])
        task = hashed_name(request.label[2])
        attempt = f"try{int(request.label[3])}"
        self.run = run
        self.name = path_name(*request.label[1:3], attempt)
        self.global_name = path_name(*request.label[:3], attempt)
        self.log_name = f"{run}__{attempt}"
        if pf.state_dir is not None:
            self.work = _private_dir(pf.state_dir, "airflow", dag)
        else:
            self.tmp = Path(tempfile.mkdtemp(prefix="rivet-airflow-"))
            self.work = self.tmp
        if request.stderr_dir:
            Path(request.stderr_dir).mkdir(parents=True, exist_ok=True)
            self.logs = _private_dir(Path(request.stderr_dir), dag, task)
        elif pf.state_dir is not None:
            self.logs = _private_dir(pf.state_dir, "logs", dag, task)
        else:
            self.logs = _private_dir(_temp_base(), "logs", dag, task)
            self.degraded.add("stderr_temp")

    def close(self) -> None:
        """Drop the scratch directory of a diskless worker and the log files past the retention."""
        if self.tmp is not None:
            shutil.rmtree(self.tmp, ignore_errors=True)
        else:
            _prune_stale(self.work)
        if self.req.log_keep is not None:
            _prune_logs(self.logs, max(1, self.req.log_keep))

    def config_for(self, only_export: str | None) -> str:
        """The config path rivet reads: a copy in the state directory, narrowed to one export when asked. rivet resolves
        the config's relative paths (its SQLite state, a `cdc.checkpoint`, a `query_file`, a local destination) against
        the config's directory, so the copy puts all of them in the state directory (docs/state.md).

        The copy is named by the path the DAG gave, symlinks kept: git-sync swaps `repo -> rev-<sha>` on every sync, and a
        copy named by the resolved path would leave the previous copy behind, still naming the CDC checkpoint, which
        rivet 0.32.2 then refuses as another config's (`RIVET_SOURCE_CDC_CHECKPOINT_SHARED`)."""
        source = Path(self.req.config).resolve()
        named = Path(os.path.abspath(self.req.config))
        target_dir = self.pf.state_dir
        if only_export is None and (target_dir is None or source.parent == target_dir.resolve()):
            return str(source)
        folder = target_dir if target_dir is not None else self.work
        if folder.resolve() != source.parent:
            _copy_query_files(self.pf.config, source, folder, self.req.export)
        if only_export is None:
            return _write_atomic(folder / copy_name(named), source.read_bytes())
        return _write_atomic(folder / copy_name(named, only_export), narrowed_config(self.pf, str(source), only_export).encode())

    def globals_for(self, subcommand: str) -> list[str]:
        """The global flags this binary accepts; a missing one is recorded, never passed."""
        flags = ["--json-errors"]
        if self.req.rivet_notifications:
            pass  # the user keeps rivet's own `notifications:` on
        elif self.pf.caps.has_flag(subcommand, "--no-notify"):
            flags.append("--no-notify")
        else:
            self.degraded.add("no_notify")
        if self.pf.caps.has_flag(subcommand, "--lock-wait"):
            flags += ["--lock-wait", str(self.req.lock_wait)]
        else:
            self.degraded.add("lock_wait")
        return flags

    def step(self, name: str, argv: list[str], stdout_to: Path | None = None) -> _Step:
        """Run one rivet process with stdout and stderr sent to private files, and read its error line.

        A stop Airflow requested before this step raises `Stopped` instead of starting rivet: on Airflow 3 the SIGTERM
        handler returns, so a stop that met no running process is seen only here (D40).

        The process inherits the export's lock descriptor, so the lock is held as long as rivet lives, also when the
        task process dies first; on Linux it gets SIGTERM when the task process dies (G2)."""
        if self.stopped is not None and self.stopped():
            raise Stopped(name)
        stderr_path = self.logs / f"{self.log_name}.{name}.stderr.log"
        stdout_path = stdout_to or self.logs / f"{self.log_name}.{name}.stdout.log"
        self.emit({"event": "rivet.start", "step": name, "argv": argv, "stderr_path": str(stderr_path)})
        cwd = self.req.cwd or str(Path(self.req.config).resolve().parent)
        with _open_private(stdout_path) as out, _open_private(stderr_path) as err:
            pass_fds = () if self.lock_fd is None else (self.lock_fd,)
            code = spawn(argv, out, err, self.req.env, cwd, self.on_process, pass_fds)
        self.emit(
            {
                "event": "rivet.step",
                "step": name,
                "exit_status": None if code < 0 else code,
                "signal": -code if code < 0 else None,
            }
        )
        self.stderr_path = str(stderr_path)
        lines = _lines(_tail(stderr_path))
        if self.req.stream_stderr:
            for raw in lines:
                self.emit({"event": "rivet.stderr", "step": name, "line": raw})
        signal = -code if code < 0 else None
        return _Step(None if signal else code, signal, _error_line(lines), _has_broken_object(lines))

    def step_with_result(self, name: str, argv: list[str], result: Path | None) -> tuple[_Step, ErrorObject | None, Any]:
        """Run a step asked to write `result` (`None`: no result file), classify its exit, read and remove the file."""
        step = self.step(name, argv)
        error = self.read.classify(step)
        if result is None:
            return step, error, None
        doc = self.read.result_file(result, step)
        result.unlink(missing_ok=True)
        return step, error, doc

    def outcome(self, step: _Step, units: list[UnitResult], error: ErrorObject | None) -> Outcome:
        """Assemble the task's result and log one structured line per unit; a failed unit fails the task."""
        failed = [u.error for u in units if u.status == "failed" and u.error is not None]
        if error is None and failed:
            error = worst(failed)
            self.degraded.add("exit_zero_failed_unit")
        out = Outcome(
            command=self.req.command,
            units=units,
            error=error,
            exit_status=step.exit_status,
            signal=step.signal,
            degraded=sorted(self.degraded),
            stderr_path=self.stderr_path,
            marker=self.pf.marker,
            state_kind=self.pf.state_kind,
            rivet_version=self.pf.caps.version_text,
        )
        if len(units) == 1:
            out.skip_reason = units[0].skip_reason
        for unit in units:
            record = unit.to_xcom()
            record.update(record.pop("error") or {})
            extra = {
                "skip_reason": unit.skip_reason,
                "target": unit.target,
                "buffer": unit.buffer,
                "target_from": unit.target_from,
                "digest": unit.digest,
            }
            self.emit({"event": "rivet.unit", **record, **extra})
        self.emit(
            {
                "event": "rivet.result",
                "command": out.command,
                "decision": out.decision,
                "degraded": out.degraded,
                "exit_status": out.exit_status,
                "signal": out.signal,
                "stderr_path": out.stderr_path,
            }
        )
        return out


def _write_atomic(path: Path, data: bytes) -> str:
    """Write a 0600 file by temp file and rename, so parallel tasks never read a partial copy.

    A copy that already holds these bytes is left alone: a rename over a file another task is opening reads as
    "not found" on a docker VM file share or NFS (measured: `rivet plan` refused its config on Airflow 3).
    """
    try:
        if path.read_bytes() == data:
            return str(path)
    except OSError:
        pass
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    os.replace(tmp, path)
    return str(path)


def _open_private(path: Path) -> Any:
    """Open a file for writing with mode 0600, whatever mode an earlier file of that name had."""
    # The creation mode only closes the window before fchmod, which sets 0600 whatever this one is: no observable mutant.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)  # pragma: no mutate
    os.fchmod(fd, 0o600)
    return os.fdopen(fd, "wb")


def _private_dir(base: Path, *parts: str) -> Path:
    """Create the directories below `base` with mode 0700 and tighten the ones that exist."""
    path = base
    for part in parts:
        path = path / part
        try:
            # The creation mode only closes the window before the chmod below, which sets 0700 whatever this one is.
            os.mkdir(path, 0o700)  # pragma: no mutate
        except FileExistsError:
            pass
        except OSError as exc:
            raise PreflightRefusal("RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE", f"cannot create {path}: {type(exc).__name__}") from exc
        try:
            if stat.S_IMODE(os.stat(path).st_mode) != 0o700:
                os.chmod(path, 0o700)
        except OSError as exc:
            raise PreflightRefusal(
                "RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE", f"cannot make {path} private: {type(exc).__name__}"
            ) from exc
    return path


def _temp_base() -> Path:
    """This user's private directory under the temp directory; a fresh random one when that name is not safely ours."""
    path = Path(tempfile.gettempdir()) / f"rivet-airflow-{os.getuid()}"
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        info = os.lstat(path)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            return Path(tempfile.mkdtemp(prefix="rivet-airflow-"))
    return path


def _tighten(path: Path) -> None:
    """Mode 0600 on a file rivet wrote through its umask (D38); a missing file is left to the step that reads it."""
    with contextlib.suppress(FileNotFoundError):
        os.chmod(path, 0o600)


def _prune_stale(work: Path, now: float | None = None) -> None:
    """Remove expired plan artifacts and crashed ledgers untouched for STALE_AFTER, and the run folders left empty (D33)."""
    cutoff = (time.time() if now is None else now) - STALE_AFTER
    for kind in ("plans", "crashed"):
        try:
            files = [p for p in (work / kind).rglob("*") if p.is_file() and p.stat().st_mtime < cutoff]
        except OSError:
            continue
        for path in files:
            if _unexpired(path) if kind == "plans" else not isinstance(_read_json(path), list):
                continue  # a replayable plan, or a ledger that is not one: left until a person deletes it
            path.unlink(missing_ok=True)  # pragma: no mutate - just listed; gone only if a parallel task removed it
        if kind == "plans":
            for folder in (work / kind).iterdir() if (work / kind).is_dir() else ():
                try:
                    folder.rmdir()
                except OSError:
                    pass


def _prune_logs(folder: Path, keep: int) -> None:
    """Keep the stdout and stderr files of a task's newest `keep` tries; remove the older tries' files."""
    attempts: dict[str, list[Path]] = {}
    try:
        for entry in folder.iterdir():
            match = _LOG_FILE.match(entry.name)
            if match:
                attempts.setdefault(match["attempt"], []).append(entry)
        newest_last = sorted(attempts, key=lambda a: max(f.stat().st_mtime_ns for f in attempts[a]))
        for attempt in newest_last[:-keep]:
            for entry in attempts[attempt]:
                entry.unlink(missing_ok=True)  # pragma: no mutate - just listed; gone only if a parallel task removed it
    except OSError:
        return


def _tail(path: Path) -> str:
    """The last STDERR_TAIL bytes of a file, as text; when that cuts a line, from the next whole line."""
    with open(path, "rb") as handle:
        size = handle.seek(0, os.SEEK_END)
        start = handle.seek(max(0, size - STDERR_TAIL))
        data = handle.read()
    if start > 0:
        data = data[data.find(b"\n") + 1 :]
    return data.decode(errors="replace")


def narrowed_exports(exports: list[Any], only_export: str) -> set[int]:
    """The positions of the export and of the batch exports its `cdc.backfill` reads its baseline through: rivet refuses
    a stream whose recipes are missing (`backfill: auto` pairs by `table:`, a list names them). Names compare as the
    text rivet reads (D07: `name: 2024` is `"2024"`)."""
    text = _yaml.text_of
    entries = {i: e for i, e in enumerate(exports) if isinstance(e, dict)}
    chosen = [e for e in entries.values() if text(e.get("name")) == only_export]
    cdc = chosen[0].get("cdc") if chosen else None
    backfill = cdc.get("backfill") if isinstance(cdc, dict) else None
    if text(backfill) == "auto":
        tables = {text(t) for t in chosen[0].get("tables") or []}
        recipes = {
            text(e.get("name")) for e in entries.values() if text(e.get("mode")) != "cdc" and text(e.get("table")) in tables
        }
    elif isinstance(backfill, list):
        recipes = {text(b) for b in backfill}
    else:
        recipes = set()
    return {i for i, e in entries.items() if text(e.get("name")) == only_export or text(e.get("name")) in recipes}


def narrowed_config(pf: Preflight, source: str, only_export: str) -> str:
    """The config text narrowed to one export (and its backfill recipes) by cutting the other items out of the original
    text (D09): rivet splices `${...}` into the raw text before parsing, so a re-serialised copy changes what a quoted
    value reads as. A cut that does not read back as the intended document is refused, never guessed."""
    exports = pf.config.get("exports") or []
    keep = narrowed_exports(exports, only_export)
    expected = {**pf.config, "exports": [exports[i] for i in sorted(keep)]}

    def check(cut: str) -> None:
        doc, _ = _yaml.resolve_and_load(cut, pf.value_of)
        if doc != expected:
            raise ValueError("the narrowed document differs from the chosen exports of the original")

    try:
        return _yaml.narrow_text(pf.config_text, keep, len(exports), check)
    except _yaml.NarrowRefused as exc:
        raise PreflightRefusal(
            "RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE",
            f"this rivet has no `--export` on load/compact, so the task loads from a copy of {source} cut down to "
            f"`{only_export}`, and the text cannot be cut safely: {exc}. Write `exports:` as a plain list whose items "
            f"each start their own `- name:` line, with no anchor another item uses",
        ) from exc


def copy_name(source: Path, only_export: str | None = None) -> str:
    """The name of a config's copy: its own stem plus a hash of its absolute path, so two sources never share one."""
    digest = hashlib.sha256(str(source).encode()).hexdigest()[:8]
    suffix = f"--{path_name(only_export)}" if only_export is not None else ""
    return f"{source.stem}.{digest}{suffix}{source.suffix or '.yaml'}"


def _copy_query_files(config: Mapping[str, Any], source: Path, folder: Path, touched: str | None) -> None:
    """Copy every relative `query_file` to the same relative path beside the config's copy; refuse what rivet would."""
    owners = _read_json(folder / QUERY_OWNERS)
    owners = owners if isinstance(owners, dict) else {}
    claimed = dict(owners)
    for entry in config.get("exports") or []:
        ref = entry.get("query_file") if isinstance(entry, dict) else None
        if not ref:
            continue
        name, ref, origin = _yaml.text_of(entry.get("name")), str(ref), source.parent / str(ref)

        def refuse(why: str, name: str = name, ref: str = ref) -> None:
            raise PreflightRefusal("RIVET_AIRFLOW_QUERY_FILE", f"export `{name}` of {source}: query_file `{ref}` {why}")

        if os.path.isabs(ref) or ".." in Path(ref).parts:
            refuse("must be a relative path with no `..`: rivet reads it only inside the config's directory")
        if not origin.is_file():
            if touched in (None, name):
                refuse("is not a file beside the config")
            continue
        if source.parent not in origin.resolve().parents:
            refuse("resolves outside the config's directory, which rivet refuses")
        if claimed.get(ref, str(source.parent)) != str(source.parent):
            refuse(
                f"is already used in {folder} by a config of {claimed[ref]}; two config directories that share a "
                "state directory cannot use one relative path: give them separate state directories"
            )
        claimed[ref] = str(source.parent)
        _private_dir(folder, *Path(ref).parts[:-1])
        _write_atomic(folder / ref, origin.read_bytes())
    if claimed != owners:
        _write_atomic(folder / QUERY_OWNERS, json.dumps(claimed, sort_keys=True).encode())


@functools.cache
def _prctl() -> Any:
    """libc's `prctl` on Linux, else `None`."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        return ctypes.CDLL(None, use_errno=True).prctl
    except (OSError, AttributeError):
        return None


def _parent_death_signal() -> Callable[[], None] | None:
    """A `preexec_fn` that has the kernel send rivet SIGTERM when the thread that started it dies (Linux
    `PR_SET_PDEATHSIG`): a task process killed outright (OOM, SIGKILL of the runner alone) no longer leaves an orphaned
    rivet beside its retry. `None` where there is no such call; the export lock still covers that case (G2)."""
    prctl = _prctl()
    if prctl is None:
        return None
    parent = os.getpid()

    def arm() -> None:
        prctl(PR_SET_PDEATHSIG, int(_signal.SIGTERM), 0, 0, 0)
        if os.getppid() != parent:  # the task process died before the call took effect
            # SIGKILL, not SIGTERM: this forked child still runs the task process's SIGTERM handler, and Airflow 3's
            # returns, so the child would go on to exec rivet as the orphan this exists to prevent
            os.kill(os.getpid(), _signal.SIGKILL)

    return arm


def _lock_path(s: _Session) -> tuple[Path, str] | None:
    """The export's lock file and what it covers; `None` for `plan` (read-only) and without a state directory.

    run / cdc_run / apply share one lock per export (rivet's run lease covers the three); load / compact take one per
    table where the binary narrows to a table, else one per export, as the load lease covers (G2)."""
    if s.pf.state_dir is None or s.req.command == "plan":
        return None
    cmd, export, table = s.req.command, s.req.export, s.req.table
    locks = _private_dir(s.pf.state_dir, "airflow", "locks")
    whole = hashed_name("config", os.path.abspath(s.req.config))
    if cmd in ("load", "compact"):
        if table and s.pf.caps.has_flag(cmd, "--table"):
            return _private_dir(locks, "table") / f"{hashed_name(export or '', table)}.lock", f"table `{table}` of `{export}`"
        return locks / f"{path_name(export) if export else whole}.lock", f"export `{export}`" if export else "the config"
    return _private_dir(locks, "run") / f"{path_name(export) if export else whole}.lock", (
        f"export `{export}`" if export else "the config"
    )


@contextlib.contextmanager
def _export_lock(s: _Session) -> Iterator[None]:
    """Hold the export's exclusive lock while this task runs rivet: wait up to `lock_wait` seconds, then raise `Busy`.

    Every rivet step inherits the descriptor (`step`), so a rivet that outlives the task keeps the lock and a retry
    cannot start a second one beside it. The lock is released by an explicit unlock once the steps have ended: a copy
    of the descriptor in any other process would keep a lock released by close alone held."""
    found = _lock_path(s)
    if found is None:
        yield
        return
    path, unit = found
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    try:
        _wait_for_lock(s, fd, path, unit)
        s.lock_fd = fd
        try:
            yield
        finally:
            _release_lock(s, fd)
    finally:
        os.close(fd)


def _wait_for_lock(s: _Session, fd: int, path: Path, unit: str) -> None:
    """Take the exclusive lock on `fd`, polling every `LOCK_POLL` seconds for up to `lock_wait`; raise `Busy` after
    it and `Stopped` when the task is stopped while waiting. Outside `_export_lock` so mutation testing reaches it."""
    started = time.monotonic()
    deadline = started + max(0, s.req.lock_wait)
    for attempt in itertools.count():
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if attempt:  # the wait is recorded once more, when it ends, with how long it took
                s.emit({"event": "rivet.lock.acquired", "lock": str(path), "unit": unit, "waited": time.monotonic() - started})
            return
        except BlockingIOError:
            if s.stopped is not None and s.stopped():
                raise Stopped(s.req.command) from None
            if attempt == 0:  # the wait is recorded once, when it starts
                s.emit({"event": "rivet.lock.wait", "lock": str(path), "unit": unit, "lock_wait": s.req.lock_wait})
            if time.monotonic() >= deadline:
                raise Busy(path, unit, s.req.lock_wait) from None
            time.sleep(LOCK_POLL)


def _release_lock(s: _Session, fd: int) -> None:
    """Release by an explicit unlock, never by close alone: a copy of the descriptor elsewhere keeps a closed lock."""
    s.lock_fd = None
    fcntl.flock(fd, fcntl.LOCK_UN)


def spawn(
    argv: list[str], out: Any, err: Any, env: Mapping[str, str], cwd: str, on_process: Callable | None, pass_fds: tuple = ()
) -> int:
    """Run one process to its end and return its exit code (negative: the signal). `on_process` gets it as `Running`, so
    a stop can reach it; an exception while waiting stops and reaps it first. stdin is /dev/null, and on Linux the
    process gets SIGTERM when the task process dies (G2)."""
    proc = subprocess.Popen(
        argv,
        stdout=out,
        stderr=err,
        stdin=subprocess.DEVNULL,
        env=dict(env),
        cwd=cwd,
        pass_fds=pass_fds,
        preexec_fn=_parent_death_signal(),
    )
    running = Running(proc)
    if on_process:
        on_process(running)
    try:
        return running.wait()
    except BaseException:
        running.request_stop()
        running.wait()
        raise


class Running:
    """A started rivet process: the step reaps it, `on_kill` may ask it to stop.

    `request_stop` runs inside Airflow's SIGTERM handler, on the thread that is inside `wait`, holding
    `Popen._waitpid_lock`; so it only sends a signal and never waits or polls (D02). The reaping, and the SIGKILL
    after STOP_GRACE seconds, happen in `wait` on the step's own loop.
    """

    def __init__(self, proc: subprocess.Popen) -> None:
        self.proc = proc
        self.stop_at: float | None = None

    def request_stop(self) -> None:
        """Send SIGTERM once and note when; nothing that takes the process's wait lock."""
        if self.stop_at is not None or self.proc.returncode is not None:
            return
        self.stop_at = time.monotonic()
        try:
            os.kill(self.proc.pid, _signal.SIGTERM)
        except ProcessLookupError:
            pass

    def wait(self) -> int:
        """Reap the process; once a stop was requested, kill it when it outlives the grace."""
        while True:
            try:
                return self.proc.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                if self.stop_at is not None and time.monotonic() - self.stop_at >= STOP_GRACE:
                    self.proc.kill()


def _lines(stderr_text: str) -> list[str]:
    """The lines of rivet's stderr: cut at "\n" only, a trailing "\r" dropped (D01).

    `str.splitlines` also cuts at U+2028, U+2029, U+0085 and C0 separators, which rivet leaves raw inside the JSON
    string of a `--json-errors` line.
    """
    pieces = stderr_text.split("\n")
    if pieces and pieces[-1] == "":
        pieces.pop()
    return [piece.removesuffix("\r") for piece in pieces]


def _error_line(stderr: str | list[str]) -> dict[str, Any] | None:
    """The last line of the stderr tail that is a JSON object with `exit_class`: the `--json-errors` line."""
    for raw in reversed(_lines(stderr) if isinstance(stderr, str) else stderr):
        raw = raw.strip()
        if not raw.startswith("{"):
            continue
        try:
            obj = json.loads(raw)
        except ValueError:
            continue
        if isinstance(obj, dict) and "exit_class" in obj:
            return obj
    return None


def _has_broken_object(lines: list[str]) -> bool:
    """True when a stderr line starts like a JSON object and does not parse: a cut or garbled error line (D01)."""
    for raw in lines:
        raw = raw.strip()
        if raw.startswith("{"):
            try:
                json.loads(raw)
            except ValueError:
                return True
    return False


def _read_json(path: Path) -> Any:
    """Parse a JSON file rivet wrote, `None` when it is absent or not JSON."""
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    return _json_of(raw)


def _unexpired(artifact: Path) -> bool:
    """True when a sealed plan artifact exists and its `expires_at` is still ahead."""
    doc = _read_json(artifact)
    try:
        expires = datetime.fromisoformat(str(doc["expires_at"]))
    except (TypeError, KeyError, ValueError):
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    return expires > datetime.now(UTC)


def _plan_inputs(s: _Session, export: str) -> str:
    """16 hex digits over what a plan of this export is made from: the config's path, its bytes, the export's query file."""
    source = Path(s.req.config).resolve()
    parts = [str(source).encode(), source.read_bytes()]
    for entry in s.pf.config.get("exports") or []:
        if isinstance(entry, dict) and _yaml.text_of(entry.get("name")) == export and entry.get("query_file"):
            try:
                parts.append((source.parent / str(entry["query_file"])).read_bytes())
            except OSError:
                parts.append(b"")
    digest = hashlib.sha256()
    for part in parts:
        digest.update(len(part).to_bytes(8) + part)  # big-endian, the default
    return digest.hexdigest()[:16]


def _same_file(recorded: str, given: str) -> bool:
    """True when two spellings name one existing file, whatever their case, Unicode form or symlinks; False when either is gone."""
    try:
        return os.path.samefile(recorded, given)
    except (OSError, ValueError):
        return False


def _refuse_foreign_artifact(artifact: Path, export: str, config: str) -> None:
    """Refuse a plan object that does not record this task's export and config, expired or not; the file is left alone."""
    doc = _read_json(artifact)
    if not isinstance(doc, dict):
        return
    named, planned = doc.get("export_name"), doc.get("config_path")
    same_config = isinstance(planned, str) and _same_file(planned, config)  # "" names no file: not the same
    if named != export or not same_config:
        raise PreflightRefusal(
            "RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN",
            f"the sealed plan artifact {artifact} records export `{named}` of config `{planned}`, and this task "
            f"applies export `{export}` of config `{config}`: applying it would extract something else under this "
            "task's name. It is neither applied nor replaced; delete the file once you know what wrote it",
        )


def _run(s: _Session) -> Outcome:
    """`rivet run`, for one export or the whole config."""
    cfg = s.config_for(None)
    summary = s.work / f"{s.name}.summary.json"
    argv = [s.pf.binary, "run", "--config", cfg]
    if s.req.export:
        argv += ["--export", s.req.export]
    argv += ["--summary-output", str(summary), *s.req.extra_args, *s.globals_for("run")]
    step, error, doc = s.step_with_result("run", argv, summary)
    return s.outcome(step, s.read.run_units(step, doc, error, s.req.export), error)


def _apply(s: _Session) -> Outcome:
    """`rivet plan -e X -o artifact` when no unexpired artifact exists, then `rivet apply artifact`."""
    export = s.req.export or ""
    cfg = s.config_for(None)
    # One sealed plan per try series (Airflow's `max_tries`, which a clear moves): a retry replays it, a clear after a
    # success plans again instead of replaying a snapshot the successful apply already moved the cursor past (D04).
    series = () if s.req.series is None else (f"s{int(s.req.series)}",)
    name = path_name(export, s.req.label[2], *series, _plan_inputs(s, export))
    artifact = _private_dir(s.work, "plans", s.run) / f"{name}.json"
    _refuse_foreign_artifact(artifact, export, cfg)
    if not _unexpired(artifact):
        plan_argv = [s.pf.binary, "plan", "--config", cfg, "--export", export, "--format", "json"]
        plan_argv += ["--output", str(artifact), "--json-errors"]
        planned = s.step("plan", plan_argv)
        error = s.read.classify(planned)
        if error is not None:
            return s.outcome(planned, [UnitResult(export, status="failed", error=error)], error)
        _refuse_foreign_artifact(artifact, export, cfg)
        _tighten(artifact)
        sealed = _read_json(artifact)
        s.emit(
            {
                "event": "rivet.plan.sealed",
                "export": export,
                "strategy": sealed.get("strategy") if isinstance(sealed, dict) else None,
            }
        )
    summary = s.work / f"{s.name}.summary.json"
    argv = [s.pf.binary, "apply", str(artifact), "--summary-output", str(summary), *s.req.extra_args]
    step, error, doc = s.step_with_result("apply", [*argv, *s.globals_for("apply")], summary)
    units = s.read.run_units(step, doc, error, s.req.export)
    for unit in units:
        # rivet >= 0.32 answers a replay of a plan it already applied with the run that applied it (an earlier try's
        # result lost to a dead task process); an incremental plan replays as an empty run of its moved cursor
        if unit.status == "already_applied":
            _emit_already_applied(s, export, unit, artifact)
    return s.outcome(step, units, error)


def _emit_already_applied(s: _Session, export: str, unit: UnitResult, artifact: Path) -> None:
    """The record that an earlier try applied the sealed plan, as rivet answered the replay."""
    s.emit({"event": "rivet.apply.already_applied", "export": export, "run_id": unit.run_id, "artifact": str(artifact)})


def _plan(s: _Session) -> Outcome:
    """`rivet plan --format json` for the whole config, swapped into `plan_file` only when a builder can read it."""
    target = Path(str(s.req.plan_file))
    scratch = target.with_name(f"{target.name}.{s.global_name}.tmp")
    argv = [s.pf.binary, "plan", "--config", s.config_for(None), "--format", "json", *s.req.extra_args, "--json-errors"]
    step = s.step("plan", argv, stdout_to=scratch)
    error = s.read.classify(step)
    if error is None:
        try:
            plan_layout(_read_json(scratch))
            os.chmod(scratch, stat.S_IMODE(target.stat().st_mode) if target.exists() else 0o644)
            os.replace(scratch, target)
        except PLAN_ERRORS:
            error = ErrorObject(None, None, "internal", 6, False, None, "rivet plan printed no plan layout")
    scratch.unlink(missing_ok=True)
    return s.outcome(step, [UnitResult(None, status="failed" if error else "success", error=error)], error)


def _load(s: _Session) -> Outcome:
    """`rivet load` / `rivet compact` for one export and, where the binary can, one table."""
    cmd, export, table = s.req.command, s.req.export, s.req.table
    by_export = s.pf.caps.has_flag(cmd, "--export")
    by_table = s.pf.caps.has_flag(cmd, "--table")
    has_result = s.pf.caps.has_flag(cmd, "--summary-output")
    if export and not by_export:
        s.degraded.add("load_filter")
    argv = [s.pf.binary, cmd, "--config", s.config_for(export if export and not by_export else None)]
    if export and by_export:
        argv += ["--export", export]
    if table and by_table:
        argv += ["--table", table]
    result = s.work / f"{s.name}.result.json"
    if has_result:
        argv += ["--summary-output", str(result)]
    else:
        s.degraded.add("load_result")
    argv += [*s.req.extra_args, *s.globals_for(cmd)]
    if table and not by_table:
        s.degraded.add("load_table_filter")  # the whole export is loaded, under the export's lock (`_lock_path`)
    step, error, doc = s.step_with_result(cmd, argv, result if has_result else None)
    return s.outcome(step, s.read.load_units(step, doc, error, export, table), error)


_DISPATCH = {"run": _run, "cdc_run": _run, "apply": _apply, "plan": _plan, "load": _load, "compact": _load}


def invoke(
    request: Request, emit: Emit, on_process: Callable | None = None, stopped: Callable[[], bool] | None = None
) -> Outcome:
    """Preflight, run and classify one rivet task; a probe of the binary that fails at any point is a refusal.

    `stopped` answers whether Airflow asked the task to stop; once it does, no further rivet step starts (D40)."""
    if request.command not in _DISPATCH:
        raise ValueError(f"unknown rivet command {request.command!r}")
    try:
        return _invoke(request, emit, on_process, stopped)
    except ProbeFailed as exc:
        raise binary_unanswering(exc) from exc


def _invoke(request: Request, emit: Emit, on_process: Callable | None, stopped: Callable[[], bool] | None) -> Outcome:
    """`invoke` with the probes' failures still raw: the binary's `<sub> --help` is read lazily while argv is built."""
    pf = run_preflight(
        rivet_bin=request.rivet_bin,
        config_path=request.config,
        env=request.env,
        state_dir=request.state_dir,
        deployment=request.deployment,
        spreads_hosts=request.spreads_hosts,
        export=request.export,
        cdc=request.command == "cdc_run",
        upstream_markers=tuple(request.upstream_markers),
        extra_args=tuple(request.extra_args),
        rivet_notifications=request.rivet_notifications,
    )
    for key, text in pf.warnings:
        emit({"event": "rivet.warning", "warning": key, "text": text})
    session = _Session(request, pf, emit, on_process, stopped)
    try:
        with _export_lock(session):
            return _DISPATCH[request.command](session)
    finally:
        session.close()


def crashed_retry_allowed(ledger: Path | None, try_number: int, max_tries: int, budget: int) -> tuple[bool, str | None]:
    """Whether a crash on this try is retried, and what is wrong with the ledger when it grants nothing; never raises."""
    if ledger is None:
        return try_number <= budget, None
    try:
        _private_dir(ledger.parent.parent, ledger.parent.name)
        seen = json.loads(ledger.read_text()) if ledger.exists() else []
    except (OSError, PreflightRefusal) as exc:
        return False, f"crashed ledger {ledger} cannot be written ({type(exc).__name__}): no crash retry is granted"
    except ValueError:
        seen = None  # pragma: no mutate - "" fails the list check below exactly as None does
    pairs = isinstance(seen, list) and all(isinstance(e, list) and len(e) == 2 and all(type(n) is int for n in e) for e in seen)
    if not pairs:
        return False, f"crashed ledger {ledger} is unreadable: no crash retry is granted until it is deleted"
    earlier = [t for t, series in seen if series == max_tries and t < try_number]
    try:
        _write_atomic(ledger, json.dumps(sorted({*map(tuple, seen), (try_number, max_tries)})).encode())
    except OSError as exc:
        return False, f"crashed ledger {ledger} cannot be written ({type(exc).__name__}): no crash retry is granted"
    return len(earlier) < budget, None
