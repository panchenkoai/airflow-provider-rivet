"""pytest plugin for mutation runs only (README, "Mutation testing"): each mutmut worker gets its own scratch space.

mutmut forks one worker per mutant and runs the workers in parallel. Two things the suite shares across one process
would then be shared across concurrent workers:

- the SQLite metadata database in the session's AIRFLOW_HOME, which the `airflow_db` fixture resets (`resetdb`)
  in every worker: two workers dropping and recreating one file's tables kill each other's DAG runs, and a mutant
  killed that way is counted as detected when no test saw it;
- pytest's base temp directory, whose old numbered entries a starting session may prune.

So each process gets a base temp of its own, and Airflow's ORM is pointed at `apr-mutmut-<pid>.db` once the session
has collected its tests. Every pytest session in one process (mutmut runs several: stats, clean run, then one per
forked worker) executes `tests/conftest.py` again, which makes a new AIRFLOW_HOME, while Airflow read its dags folder
once, at import; the DAG files of `DagRig` go to the new home and `dag.test()` would find no DAG. So the dags folder
is pointed at the session's AIRFLOW_HOME as well. Loaded by `-p mutmut_isolation` with `dev/` on PYTHONPATH
(`make mutants`); a plain `pytest` never loads it.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest

_OWNED: list[Path] = []


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    """Before the tmp_path factory exists: a base temp directory of this process."""
    base = Path(tempfile.gettempdir()) / f"apr-mutmut-{os.getpid()}"
    shutil.rmtree(base, ignore_errors=True)
    config.option.basetemp = str(base)
    _OWNED.append(base)


def pytest_collection_finish(session: pytest.Session) -> None:
    """Before any fixture runs: this process's own metadata database."""
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


def pytest_unconfigure(config: pytest.Config) -> None:
    """A worker leaves nothing behind."""
    for path in _OWNED:
        shutil.rmtree(path, ignore_errors=True) if path.is_dir() else path.unlink(missing_ok=True)
    _OWNED.clear()
