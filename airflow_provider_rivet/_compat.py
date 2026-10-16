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

__all__ = [
    "DAG",
    "AirflowException",
    "AirflowFailException",
    "AirflowNotFoundException",
    "AirflowSkipException",
    "BaseHook",
    "BaseOperator",
    "TaskGroup",
]
