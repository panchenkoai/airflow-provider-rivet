"""The names that moved between Airflow 2.10 and 3.x, imported in one place."""

from __future__ import annotations

try:  # Airflow 3.x: the task SDK is the public authoring interface
    from airflow.sdk import DAG, BaseHook, BaseOperator, TaskGroup
except ImportError:  # Airflow 2.10+
    from airflow.hooks.base import BaseHook
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG
    from airflow.utils.task_group import TaskGroup

try:  # Airflow 3.x: `airflow.exceptions` is a deprecated shim for these (D27)
    from airflow.sdk.exceptions import AirflowException, AirflowFailException, AirflowNotFoundException, AirflowSkipException
except ImportError:  # Airflow 2.10+
    from airflow.exceptions import AirflowException, AirflowFailException, AirflowNotFoundException, AirflowSkipException

# Executors that run every task of a deployment on the scheduler's own host.
SINGLE_HOST_EXECUTORS = frozenset({"LocalExecutor", "SequentialExecutor", "DebugExecutor"})


def executor_spreading_hosts(value: str | None = None) -> str | None:
    """The configured executor that may run one DAG's tasks on several hosts (Celery, Kubernetes, Edge, ...), else
    None. `[core] executor` is a class name, a module path or `alias:path`, or a comma-separated list of them."""
    if value is None:
        from airflow.configuration import conf

        value = conf.get("core", "executor")
    names = [part.rpartition(":")[2].strip().rpartition(".")[2] for part in value.split(",") if part.strip()]
    return next((name for name in names if name not in SINGLE_HOST_EXECUTORS), None)


__all__ = [
    "DAG",
    "AirflowException",
    "AirflowFailException",
    "AirflowNotFoundException",
    "AirflowSkipException",
    "BaseHook",
    "BaseOperator",
    "TaskGroup",
    "executor_spreading_hosts",
]
