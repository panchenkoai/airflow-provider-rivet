"""DAG builders: TaskGroups are the picture, dependencies run per table, and a watcher fails the run on any failure."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ._compat import DAG, TaskGroup
from .invocation import PLAN_ERRORS, plan_layout
from .operators import (
    RivetApplyOperator,
    RivetCdcRunOperator,
    RivetCompactOperator,
    RivetLoadOperator,
    RivetPlanOperator,
    RivetRunOperator,
    RivetRunWatcher,
    RivetWaveBarrier,
)

ORDER_ONLY = "all_done"
WATCHER_ID = "watcher"
GATE = "all_success"
DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=5)}
NOT_IN_TASK_ID = re.compile(r"[^\w.-]")


def task_name(name: str) -> str:
    """The task id part for an export or table name: itself when Airflow accepts it, else sanitised plus a hash (D29).

    The hash is of the real name, so `daily orders` and `daily_orders` never share an id; the real name stays in `export=`.
    """
    safe = NOT_IN_TASK_ID.sub("_", name)
    if safe == name and name:
        return name
    return f"{safe}_{hashlib.sha256(name.encode()).hexdigest()[:8]}"


def _names(field: str, value: Any) -> list[str]:
    """A list of export or table names; a bare string would be split into one task per character (D28)."""
    if isinstance(value, str | bytes):
        raise ValueError(f"`{field}` is a list of names, not a string: write {field}=[{value!r}]")
    return list(value)


def _common(config: str, state_dir: str | None, operator_kwargs: Mapping[str, Any] | None) -> dict[str, Any]:
    """The arguments every rivet task gets; its trigger rule is the builder's gate, whatever default_args says (D24)."""
    if operator_kwargs and "trigger_rule" in operator_kwargs:
        raise ValueError(
            "operator_kwargs may not set `trigger_rule`: every rivet task runs only after its upstream succeeded "
            "(load after its extract, compact after its load); the watcher fails the run on any failure"
        )
    return {
        "config": config,
        "state_dir": state_dir,
        "max_active_tis_per_dag": 1,
        **(operator_kwargs or {}),
        "trigger_rule": GATE,
    }


def read_plan_layout(plan_file: str) -> list[dict[str, Any]]:
    """Waves from a `rivet plan --format json` file: `[{"wave", "exports", "heavy"}]`, lowest wave first."""
    try:
        return plan_layout(json.loads(Path(plan_file).read_text()))
    except (OSError, *PLAN_ERRORS) as exc:
        raise RuntimeError(
            f"{plan_file} is missing or is not `rivet plan --format json` output ({type(exc).__name__}); "
            f"write it with: rivet plan --config <config> --format json > {plan_file}"
        ) from exc


def _watch(dag: DAG) -> None:
    """Add the task that fails the run when any other task failed; it carries no failure callback of its own."""
    tasks = list(dag.tasks)
    tasks >> RivetRunWatcher(task_id=WATCHER_ID, trigger_rule="one_failed", retries=0, on_failure_callback=None, dag=dag)


def _dag(dag_id: str, tags: Sequence[str], dag_kwargs: Mapping[str, Any]) -> DAG:
    """A DAG with the overlap limits ADR-0039's lease will later make redundant."""
    settings: dict[str, Any] = {
        "schedule": None,
        "start_date": datetime(2026, 1, 1),
        "catchup": False,
        "max_active_runs": 1,
        "tags": list(tags),
    }
    settings.update(dag_kwargs)
    settings["default_args"] = {**DEFAULT_ARGS, **(dag_kwargs.get("default_args") or {})}  # D25: merged, not replaced
    return DAG(dag_id=dag_id, **settings)


def build_batch_dag(
    dag_id: str,
    *,
    config: str,
    state_dir: str | None = None,
    plan_file: str | None = None,
    exports: Sequence[str] | None = None,
    refresh_plan: bool = True,
    extract: str = "apply",
    load: bool = True,
    compact_exports: Sequence[str] = (),
    operator_kwargs: Mapping[str, Any] | None = None,
    **dag_kwargs: Any,
) -> DAG:
    """plan -> apply / load / compact groups, chained per export: apply.X >> load.X >> compact.X."""
    exports = None if exports is None else _names("exports", exports)
    compact_exports = _names("compact_exports", compact_exports)
    if (plan_file is None) == (exports is None):
        raise ValueError("give exactly one of `plan_file` (waves from rivet plan) or `exports` (one wave)")
    if exports is not None and not exports:
        raise ValueError("`exports` is empty: name at least one export, or give `plan_file`")
    if extract not in ("apply", "run"):
        raise ValueError("`extract` is 'apply' or 'run'")
    waves = read_plan_layout(plan_file) if plan_file else [{"wave": 1, "exports": list(exports or ()), "heavy": []}]
    names = [n for w in waves for n in w["exports"]]
    unknown = sorted(set(compact_exports) - set(names))
    if unknown:
        raise ValueError(f"compact_exports names exports the plan does not have: {unknown}")
    if compact_exports and not load:
        raise ValueError("compact_exports needs load=True: a compaction merges what the load buffered")
    common = _common(config, state_dir, operator_kwargs)
    extract_cls = RivetApplyOperator if extract == "apply" else RivetRunOperator

    with _dag(dag_id, ("rivet", "batch"), dag_kwargs) as dag:
        plan = RivetPlanOperator(task_id="plan", plan_file=plan_file, **common) if plan_file and refresh_plan else None
        apply_tasks: dict[str, Any] = {}
        with TaskGroup(group_id="apply"):
            wave_done: Any | None = None
            for index, wave in enumerate(waves):
                # D26: rivet's `cost_gate_batches` order: each heavy export alone, one after another, then the cheap batch
                heavy = [n for n in wave["exports"] if n in wave["heavy"]]
                cheap = [n for n in wave["exports"] if n not in wave["heavy"]]
                order_done: Any | None = None
                for position, name in enumerate(heavy + cheap):
                    task = extract_cls(task_id=task_name(name), export=name, **common)
                    apply_tasks[name] = task
                    for gate in (plan, wave_done, order_done):
                        if gate is not None:
                            gate >> task
                    if name in heavy and position < len(heavy + cheap) - 1:
                        order_done = RivetWaveBarrier(task_id=f"after_{task_name(name)}", trigger_rule=ORDER_ONLY)
                        task >> order_done
                if index < len(waves) - 1:
                    wave_done = RivetWaveBarrier(task_id=f"wave_{wave['wave']}_done", trigger_rule=ORDER_ONLY)
                    for name in wave["exports"]:
                        apply_tasks[name] >> wave_done
        load_tasks: dict[str, Any] = {}
        if load:
            with TaskGroup(group_id="load"):
                for name in names:
                    load_tasks[name] = RivetLoadOperator(task_id=task_name(name), export=name, **common)
                    apply_tasks[name] >> load_tasks[name]
        if compact_exports:
            with TaskGroup(group_id="compact"):
                for name in names:
                    if name in compact_exports:
                        load_tasks[name] >> RivetCompactOperator(task_id=task_name(name), export=name, **common)
        _watch(dag)
    return dag


def build_cdc_dag(
    dag_id: str,
    *,
    config: str,
    export: str,
    tables: Sequence[str],
    state_dir: str | None = None,
    load: bool = True,
    compact: bool = True,
    operator_kwargs: Mapping[str, Any] | None = None,
    **dag_kwargs: Any,
) -> DAG:
    """One `run` task for the stream, then load.T >> compact.T per captured table."""
    tables = _names("tables", tables)
    if not tables and load:
        raise ValueError("`tables` lists the captured tables the load and compact tasks are built for")
    common = _common(config, state_dir, operator_kwargs)
    with _dag(dag_id, ("rivet", "cdc"), dag_kwargs) as dag:
        run = RivetCdcRunOperator(task_id="run", export=export, **common)
        if load:
            loads = {}
            with TaskGroup(group_id="load"):
                for table in tables:
                    loads[table] = RivetLoadOperator(task_id=task_name(table), export=export, table=table, **common)
                    run >> loads[table]
            if compact:
                with TaskGroup(group_id="compact"):
                    for table in tables:
                        loads[table] >> RivetCompactOperator(task_id=task_name(table), export=export, table=table, **common)
        _watch(dag)
    return dag
