"""pytest plugin for mutation runs only (docs/development.md, "Mutation testing"): each mutmut worker gets its own scratch space.

mutmut forks one worker per mutant and runs the workers in parallel. Two things the suite shares across one process
would then be shared across concurrent workers:

- the SQLite metadata database in the session's AIRFLOW_HOME, which the `airflow_db` fixture resets (`resetdb`)
  in every worker: two workers dropping and recreating one file's tables kill each other's DAG runs, and a mutant
  killed that way is counted as detected when no test saw it;
- pytest's base temp directory, whose old numbered entries a starting session may prune.

A mutant can also turn a loop unbounded in memory (`_yaml.substitute` with `i = None` scans the same `${` forever,
growing its output list): mutmut's CPU-time limit comes too late, and on a CI runner the kernel's OOM killer then
takes the runner itself. So each worker's data segment is capped (`MUTANTS_MEM_LIMIT_MB`, default 3072: the cap is on the virtual data segment, where glibc reserves an arena per thread): the mutant
dies with MemoryError, which is a detection, and the run goes on.

So each process gets a base temp of its own, and Airflow's ORM is pointed at `apr-mutmut-<pid>.db` once the session
has collected its tests. Every pytest session in one process (mutmut runs several: stats, clean run, then one per
forked worker) executes `tests/conftest.py` again, which makes a new AIRFLOW_HOME, while Airflow read its dags folder
once, at import; the DAG files of `DagRig` go to the new home and `dag.test()` would find no DAG. So the dags folder
is pointed at the session's AIRFLOW_HOME as well. Loaded by `-p mutmut_isolation` with `dev/` on PYTHONPATH
(`make mutants`); a plain `pytest` never loads it.
"""

from __future__ import annotations

import os
import resource
import shutil
import tempfile
from pathlib import Path

import pytest

_OWNED: list[Path] = []
_MUTATED_ROOT: list[str] = []
_BASE_PYTHONPATH = os.environ.get("PYTHONPATH", "")


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    """Before the tmp_path factory exists: a base temp directory and a memory cap of this process."""
    cap = int(os.environ.get("MUTANTS_MEM_LIMIT_MB", "3072")) << 20
    _, hard = resource.getrlimit(resource.RLIMIT_DATA)
    resource.setrlimit(resource.RLIMIT_DATA, (cap if hard == resource.RLIM_INFINITY else min(cap, hard), hard))
    base = Path(tempfile.gettempdir()) / f"apr-mutmut-{os.getpid()}"
    shutil.rmtree(base, ignore_errors=True)
    config.option.basetemp = str(base)
    _OWNED.append(base)


def pytest_collection_finish(session: pytest.Session) -> None:
    """Before any fixture runs: this process's own metadata database, and the mutated package for child Pythons."""
    # tests/test_retry.py and tests/test_process_io.py run the operator in a child Python (a task process the test
    # kills). That child imported the package installed in the environment, the unmutated source, so the guards those
    # tests exist for (PR_SET_PDEATHSIG, the lock passed to rivet, stop-before-step, on_kill) were never mutation
    # tested. The child inherits MUTANT_UNDER_TEST; with the mutated tree first on its path it runs the mutant too.
    import airflow_provider_rivet

    _MUTATED_ROOT.append(str(Path(airflow_provider_rivet.__file__).resolve().parents[1]))
    if not os.environ.get("AIRFLOW_HOME"):
        return  # the suite's conftest sets it before Airflow is imported; nothing to isolate without it
    db = Path(tempfile.gettempdir()) / f"apr-mutmut-{os.getpid()}.db"
    for path in (db, Path(f"{db}-wal"), Path(f"{db}-shm"), Path(f"{db}-journal")):
        path.unlink(missing_ok=True)
        _OWNED.append(path)
    conn = f"sqlite:///{db}"
    from airflow import settings
    from airflow.configuration import conf

    conf.set("database", "sql_alchemy_conn", conn)
    dags = str(Path(os.environ["AIRFLOW_HOME"]) / "dags")
    conf.set("core", "dags_folder", dags)
    settings.DAGS_FOLDER = dags
    settings.dispose_orm(do_log=False)
    settings.SQL_ALCHEMY_CONN = conn
    settings.SQL_ALCHEMY_CONN_ASYNC = settings._get_async_conn_uri_from_sync(sync_uri=conn)
    settings.configure_orm()


# The test modules that run the operator in a child Python (above). mutmut picks the tests of a mutant from the
# functions each test called in its stats run, and a child's calls are not seen there: without this, the stop / kill
# / lock tests of these modules were never chosen for the mutants of the code they exist for (`_run_with_secrets`'s
# Stopped and Busy arms, `Running.wait`, `Stopped`). In the stats run, a test of these modules counts as calling
# every function of the package; that over-selects (a few seconds per surviving mutant), never under-selects.
CHILD_PYTHON_TESTS = ("test_retry.py::", "test_process_io.py::")


def pytest_runtest_setup(item: pytest.Item) -> None:
    """A child Python of a mutant's test imports the mutated tree; one of the stats run does not: there the trampoline
    would record its calls through mutmut's config, which it cannot find from the child's working directory."""
    parts = [*(_MUTATED_ROOT if os.environ.get("MUTANT_UNDER_TEST") != "stats" else []), _BASE_PYTHONPATH]
    os.environ["PYTHONPATH"] = os.pathsep.join(p for p in parts if p)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item: pytest.Item):
    yield
    if os.environ.get("MUTANT_UNDER_TEST") != "stats" or not any(m in item.nodeid for m in CHILD_PYTHON_TESTS):
        return
    import sys

    from mutmut.stats import record_trampoline_hit

    for name, module in list(sys.modules.items()):
        if name == "airflow_provider_rivet" or name.startswith("airflow_provider_rivet."):
            for attr in vars(module):
                if attr.startswith("mutants_") and attr.endswith("__mutmut"):
                    record_trampoline_hit(f"{name}.{attr[len('mutants_') : -len('__mutmut')]}")


def pytest_unconfigure(config: pytest.Config) -> None:
    """A worker leaves nothing behind."""
    for path in _OWNED:
        shutil.rmtree(path, ignore_errors=True) if path.is_dir() else path.unlink(missing_ok=True)
    _OWNED.clear()
