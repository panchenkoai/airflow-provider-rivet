"""The stand's own secrets: every password and key is made for the stack, random, and no word its logs carry.

The stand is read the way docker compose reads it (the official compose file, then the gate overlay, then the env
file), so a password or key is found wherever a stand puts it.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import yaml

from gate.stand import FLAVORS, Stand

TAG = "1016120000ab"
#: Words every task log carries. Airflow masks each Connection password (and the metastore's) wherever it occurs in a
#: log line, so a password that is one of these, or inside one, turns `/home/airflow` into `/home/***`.
LOG_WORDS = (
    "rivet", "airflow", "gate", "postgres", "postgresql", "clickhouse", "loader", "snowflake", "bigquery", "source",
    "state", "rivet_state", "home", "opt", "provider", "operators", "start", "result", "warning", "batch", "cdc",
)  # fmt: skip


class Loader(yaml.SafeLoader):
    """YAML with compose's `!override` / `!reset` tags read as plain values."""


Loader.add_constructor("!override", lambda loader, node: loader.construct_sequence(node))
Loader.add_constructor("!reset", lambda loader, node: None)


def laid_out(tmp_path: Path, version: str = "3.3.2", **kw) -> Stand:
    stand = Stand(version, tmp_path / "rivet", tmp_path, tag=TAG, **kw)
    stand.lay_out()
    return stand


def compose_view(stand: Stand) -> dict:
    """What compose would run: per service, the environment after the base file, the overlay and the env file."""
    base = yaml.load((stand.dir / "docker-compose.yaml").read_text(), Loader=Loader)
    overlay = yaml.load((stand.dir / "docker-compose.gate.yaml").read_text(), Loader=Loader)
    env_file = dict(line.split("=", 1) for line in (stand.dir / "gate-connections.env").read_text().splitlines() if "=" in line)
    view = {}
    for name in {*base["services"], *overlay["services"]}:
        b = base["services"].get(name) or {}
        o = overlay["services"].get(name) or {}
        env = {**(b.get("environment") or {}), **(o.get("environment") or {})}
        if name.startswith("airflow-"):
            env.update(env_file)
        view[name] = {"environment": {k: str(v) for k, v in env.items()}, "volumes": [*(o.get("volumes") or [])]}
    return view


def stand_secrets(stand: Stand) -> dict[str, str]:
    """Every password and key the stack runs with, by where it is set."""
    view = compose_view(stand)
    found = {}
    for service, key in (
        ("gate-source", "POSTGRES_PASSWORD"),
        ("gate-state", "POSTGRES_PASSWORD"),
        ("gate-clickhouse", "CLICKHOUSE_PASSWORD"),
        ("postgres", "POSTGRES_PASSWORD"),
    ):
        found[f"{service}.{key}"] = view[service]["environment"][key]
    worker = view["airflow-worker"]["environment"]
    for key, value in worker.items():
        if key.startswith("AIRFLOW_CONN_"):
            conn = json.loads(value) if value.startswith("{") else {"password": urlsplit(value).password}
            found[key] = conn.get("password") or ""
    found["sql_alchemy_conn"] = urlsplit(worker["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]).password or ""
    found["fernet"] = worker.get("AIRFLOW__CORE__FERNET_KEY", "")
    return found


# --- Stand hygiene: every password and key is the stack's own, random, and no log word holds it ----------------------


@pytest.mark.parametrize("version", sorted(FLAVORS))
def test_no_stand_password_is_a_word_its_logs_carry(tmp_path: Path, version: str) -> None:
    """Source and state passwords were `rivet` (and the metastore's `airflow`): Airflow masks a Connection password
    wherever it occurs, so every task log read `/home/***/`, `airflow_provider_***`, `***.start`."""
    found = stand_secrets(laid_out(tmp_path, version))
    passwords = {k: v for k, v in found.items() if k != "fernet"}
    assert passwords, "no password found in the stand"
    for where, password in passwords.items():
        inside = [w for w in LOG_WORDS if password in w]
        assert len(password) >= 24 and not inside, f"{where} = {password!r} is inside log word(s) {inside}"


@pytest.mark.parametrize("version", sorted(FLAVORS))
def test_the_stand_runs_with_a_fernet_key(tmp_path: Path, version: str) -> None:
    """An empty AIRFLOW__CORE__FERNET_KEY stored every Connection in clear and logged `empty cryptography key`."""
    key = stand_secrets(laid_out(tmp_path, version))["fernet"]
    try:
        raw = base64.urlsafe_b64decode(key.encode())
    except ValueError:
        raw = b""
    assert len(raw) == 32, f"fernet key {key!r} is not 32 url-safe bytes"


def test_two_stands_never_share_a_password(tmp_path: Path) -> None:
    a = stand_secrets(laid_out(tmp_path / "a"))
    b = stand_secrets(laid_out(tmp_path / "b"))
    assert not set(a.values()) & set(b.values())


def test_the_gate_reads_the_stand_with_its_own_passwords(tmp_path: Path) -> None:
    """The host-side clients (DuckDB over the source, ClickHouse over HTTP, the REST API) use the generated values."""
    stand = laid_out(tmp_path, "2.10.5")
    view = compose_view(stand)
    assert stand.secrets.source == view["gate-source"]["environment"]["POSTGRES_PASSWORD"]
    assert stand.secrets.clickhouse == view["gate-clickhouse"]["environment"]["CLICKHOUSE_PASSWORD"]
    assert base64.b64decode(stand._auth().removeprefix("Basic ")).decode() == f"airflow:{stand.secrets.admin}"
