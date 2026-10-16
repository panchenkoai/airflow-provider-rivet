"""Failures before rivet starts are coded refusals: not retried, and the refusal is in XCom (D05, D08, C1-a)."""

from __future__ import annotations

from typing import Any

import pytest
from airflow.exceptions import AirflowFailException, AirflowNotFoundException
from conftest import FakeTI, Rig

from airflow_provider_rivet import capabilities
from airflow_provider_rivet.operators import RivetLoadOperator, RivetRunOperator
from airflow_provider_rivet.preflight import PreflightRefusal, read_config


def refusal(rig: Rig, op: Any) -> tuple[str, str]:
    """Run an operator that must be refused before any rivet process; (the XCom refusal code, the exception text)."""
    ti = FakeTI(task_id=op.task_id)
    exc, ti = rig.run(op, ti)
    assert isinstance(exc, AirflowFailException), f"retried or raw: {type(exc).__name__}: {exc}"
    assert not rig.calls(), "a refused task must not start rivet"
    payload = ti.store.get((ti.task_id, "return_value"))
    assert payload is not None, "the refusal must be published to XCom before the task fails"
    assert payload["decision"] == "fail" and payload["units"] == []
    return payload["preflight_refusal"], str(exc)


def missing_connection(conn_id: str) -> Any:
    """`BaseHook.get_connection` for an id the metadata database and every secrets backend lack."""
    raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")


@pytest.mark.parametrize(
    "kwargs",
    [
        pytest.param({"env_from_connections": {"RIVET_PG_URL": "no_such_conn"}}, id="env_from_connections"),
        pytest.param({"env_from_connections": {"RIVET_PG_URL": ("no_such_conn", "postgresql")}}, id="env_with_scheme"),
        pytest.param({"connections": ["no_such_conn"]}, id="connections"),
    ],
)
def test_d05_a_connection_id_that_does_not_exist_is_refused_without_retry(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, kwargs: dict
) -> None:
    """D05: a missing Connection is a configuration error; it was an `AirflowNotFoundException`, retried, with no XCom."""
    monkeypatch.setattr("airflow_provider_rivet.operators.BaseHook.get_connection", missing_connection)
    code, text = refusal(rig, rig.operator(RivetRunOperator, export="orders", env={}, **kwargs))
    assert code == "RIVET_AIRFLOW_CONNECTION_MISSING"
    assert "no_such_conn" in text


BAD_CONFIGS = {
    "non_utf8": b"source:\n  type: postgres\nexports:\n  - name: caf\xe9\n    mode: full\n",
    "deep_nesting": b"exports: " + b"[" * 5000,
    "integer_past_the_digit_limit": b"exports:\n  - name: orders\n    page_size: " + b"9" * 5000 + b"\n",
    "exports_a_number": b"exports: 5\n",
    "exports_a_mapping": b"exports:\n  orders:\n    mode: full\n",
    "export_entry_not_a_mapping": b"exports:\n  - orders\n",
    "name_a_list": b"exports:\n  - name: [orders]\n    mode: full\n",
    "name_a_mapping": b"exports:\n  - name: {orders: 1}\n    mode: full\n",
}


@pytest.mark.parametrize("text", BAD_CONFIGS.values(), ids=BAD_CONFIGS.keys())
def test_d08_an_unreadable_or_wrongly_shaped_config_is_refused_without_retry(rig: Rig, text: bytes) -> None:
    """D08: `read_config` caught only OSError and YAMLError, so these crashed with a traceback and were retried."""
    rig.config.write_bytes(text)
    code, _ = refusal(rig, rig.operator(RivetRunOperator))
    assert code == "RIVET_AIRFLOW_CONFIG_UNREADABLE"


@pytest.mark.parametrize("text", BAD_CONFIGS.values(), ids=BAD_CONFIGS.keys())
def test_d08_the_connection_reader_and_the_preflight_read_one_config_the_same_way(rig: Rig, text: bytes) -> None:
    """D08: `_config_doc` swallowed what `read_config` refused (or crashed on), so the two readers disagreed."""
    rig.config.write_bytes(text)
    op = rig.operator(RivetRunOperator, connections=["gcs"])
    with pytest.raises(PreflightRefusal) as connections:
        op._config_doc()
    with pytest.raises(PreflightRefusal) as preflight:
        read_config(str(rig.config))
    assert connections.value.code == preflight.value.code == "RIVET_AIRFLOW_CONFIG_UNREADABLE"


def test_d08_a_version_probe_that_times_out_is_refused_without_retry(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """D08: `rivet --version` timing out raised `TimeoutExpired`, which the preflight did not catch."""
    monkeypatch.setattr(capabilities, "PROBE_TIMEOUT", 0.5, raising=False)
    rig.scenario(probe_broken={"--version": "hang"})
    code, text = refusal(rig, rig.operator(RivetRunOperator, export="orders"))
    assert code == "RIVET_AIRFLOW_BINARY_MISSING", text


@pytest.mark.parametrize(
    ("cls", "sub", "how"),
    [
        pytest.param(RivetRunOperator, "run", "exit", id="run_help_exits_non_zero"),
        pytest.param(RivetRunOperator, "run", "hang", id="run_help_times_out"),
        pytest.param(RivetLoadOperator, "load", "exit", id="load_help_exits_non_zero"),
    ],
)
def test_c1a_a_help_probe_that_fails_after_the_preflight_is_refused_without_retry(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, cls: Any, sub: str, how: str
) -> None:
    """C1-a: `rivet <sub> --help` is probed lazily while argv is built; its RuntimeError or TimeoutExpired left the
    operator raw, retried, with no XCom."""
    monkeypatch.setattr(capabilities, "PROBE_TIMEOUT", 0.5, raising=False)
    rig.scenario(probe_broken={sub: how})
    code, text = refusal(rig, rig.operator(cls, export="orders"))
    assert code == "RIVET_AIRFLOW_BINARY_MISSING", text
