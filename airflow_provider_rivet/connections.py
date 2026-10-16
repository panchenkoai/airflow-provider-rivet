"""Storage and warehouse Connections turned into what rivet reads: environment variables and private key files."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from .preflight import PreflightRefusal

CLICKHOUSE_URL = "RIVET_CLICKHOUSE_URL"
CLICKHOUSE_USER = "RIVET_CLICKHOUSE_USER"
CLICKHOUSE_PASSWORD = "RIVET_CLICKHOUSE_PASSWORD"
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
SNOWFLAKE_KEY = "RIVET_SNOWFLAKE_KEY"


def _extra(conn: Any, name: str, kind: str) -> Any:
    """An extra by its plain name or by the `extra__<kind>__<name>` form older providers wrote."""
    extra = getattr(conn, "extra_dejson", None) or {}
    value = extra.get(name)
    return value if value not in (None, "") else extra.get(f"extra__{kind}__{name}") or None


def _secret_file(folder: Path, name: str, data: str) -> str:
    """Write a 0600 file in the task's private folder; its path."""
    path = folder / name
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(data)
    return str(path)


def _gcp(conn: Any, folder: Path, config: Mapping[str, Any]) -> dict[str, str]:
    """A service-account (or authorized-user) key for GCS and BigQuery; without one, the worker's ADC is used."""
    env: dict[str, str] = {}
    keyfile = _extra(conn, "keyfile_dict", "google_cloud_platform")
    path = _extra(conn, "key_path", "google_cloud_platform")
    if keyfile:
        text = keyfile if isinstance(keyfile, str) else json.dumps(keyfile)
        env["GOOGLE_APPLICATION_CREDENTIALS"] = _secret_file(folder, f"gcp-{conn.conn_id}.json", text)
    elif path:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = str(path)
    project = _extra(conn, "project", "google_cloud_platform")
    if project:
        env["GOOGLE_CLOUD_PROJECT"] = str(project)
    return env


def _aws(conn: Any, folder: Path, config: Mapping[str, Any]) -> dict[str, str]:
    """Static or session keys for S3; the config names them with `access_key_env: AWS_ACCESS_KEY_ID` and so on."""
    env = {}
    if conn.login:
        env["AWS_ACCESS_KEY_ID"] = conn.login
    if conn.password:
        env["AWS_SECRET_ACCESS_KEY"] = conn.password
    for extra, name in (
        ("aws_session_token", "AWS_SESSION_TOKEN"),
        ("region_name", "AWS_REGION"),
        ("endpoint_url", "AWS_ENDPOINT_URL"),
    ):
        value = _extra(conn, extra, "aws")
        if value:
            env[name] = str(value)
    return env


def _azure(conn: Any, folder: Path, config: Mapping[str, Any]) -> dict[str, str]:
    """The storage account and its key or SAS token; the config names them with `account_key_env` / `sas_token_env`."""
    env = {}
    if conn.login:
        env["AZURE_STORAGE_ACCOUNT"] = conn.login
    if conn.password:
        env["AZURE_STORAGE_KEY"] = conn.password
    sas = _extra(conn, "sas_token", "wasb")
    if sas:
        env["AZURE_STORAGE_SAS_TOKEN"] = str(sas)
    return env


def _snowflake(conn: Any, folder: Path, config: Mapping[str, Any]) -> dict[str, str]:
    """A `snow` connection under the name the config's `load.connection` gives, in a private SNOWFLAKE_HOME."""
    name = ((config.get("load") or {}).get("connection") if isinstance(config.get("load"), dict) else None) or conn.conn_id
    fields: dict[str, str] = {"account": str(_extra(conn, "account", "snowflake") or conn.host or ""), "user": conn.login or ""}
    for key in ("role", "warehouse", "database", "authenticator"):
        value = _extra(conn, key, "snowflake")
        if value:
            fields[key] = str(value)
    env: dict[str, str] = {}
    key_text = _extra(conn, "private_key_content", "snowflake")
    key_path = _extra(conn, "private_key_file", "snowflake")
    if key_text:
        key_path = _secret_file(folder, f"snowflake-{conn.conn_id}.p8", key_text)
    if key_path:
        fields["authenticator"] = "SNOWFLAKE_JWT"
        fields["private_key_file"] = str(key_path)
        env[SNOWFLAKE_KEY] = str(key_path)
    elif conn.password:
        fields["password"] = conn.password
    if not fields["account"]:
        raise PreflightRefusal(
            "RIVET_AIRFLOW_CONNECTION_INCOMPLETE", f"Snowflake connection `{conn.conn_id}` has no `account` extra"
        )
    home = folder / "snowflake"
    home.mkdir(mode=0o700)
    body = f"[connections.{json.dumps(name)}]\n" + "".join(f"{k} = {json.dumps(v)}\n" for k, v in fields.items())
    _secret_file(home, "config.toml", body)
    env["SNOWFLAKE_HOME"] = str(home)
    return env


def _clickhouse(conn: Any, folder: Path, config: Mapping[str, Any]) -> dict[str, str]:
    """The HTTP endpoint, user and password; the config reads `url: ${RIVET_CLICKHOUSE_URL}` and the password from its
    `password_env` (RIVET_CLICKHOUSE_PASSWORD always carries it as well)."""
    secure = str(_extra(conn, "secure", "clickhouse") or "").lower() in ("1", "true", "yes")
    scheme = str(_extra(conn, "protocol", "clickhouse") or ("https" if secure else "http"))
    port = f":{conn.port}" if conn.port else ""
    env = {CLICKHOUSE_URL: f"{scheme}://{conn.host}{port}", CLICKHOUSE_USER: conn.login or "default"}
    if conn.password:
        # `rivet init --clickhouse-url` writes `password_env: CLICKHOUSE_PASSWORD`; the variable the config names gets it too.
        load = config.get("load") if isinstance(config.get("load"), dict) else {}
        named = load.get("password_env")
        for name in {CLICKHOUSE_PASSWORD, *([named] if isinstance(named, str) and ENV_NAME.fullmatch(named) else [])}:
            env[name] = conn.password
    return env


KINDS: dict[str, Callable[[Any, Path, Mapping[str, Any]], dict[str, str]]] = {
    "gcp": _gcp,
    "aws": _aws,
    "azure": _azure,
    "snowflake": _snowflake,
    "clickhouse": _clickhouse,
}
CONN_TYPES = {
    "google_cloud_platform": "gcp",
    "gcpbigquery": "gcp",
    "aws": "aws",
    "wasb": "azure",
    "azure_blob_storage": "azure",
    "snowflake": "snowflake",
    "clickhouse": "clickhouse",
}


def kind_of(conn: Any) -> str:
    """The storage or warehouse a Connection is for: the extra `rivet_kind`, else its connection type."""
    kind = _extra(conn, "rivet_kind", "rivet") or CONN_TYPES.get(conn.conn_type)
    if kind not in KINDS:
        raise PreflightRefusal(
            "RIVET_AIRFLOW_CONNECTION_KIND",
            f"connection `{conn.conn_id}` has conn_type `{conn.conn_type}`, which names no storage or warehouse rivet reads; "
            f"set its extra `rivet_kind` to one of {sorted(KINDS)}",
        )
    return kind


def cloud_environment(conns: list[Any], folder: Path, config: Mapping[str, Any] | None = None) -> dict[str, str]:
    """The variables (and files under `folder`) of every Connection; two of one kind are refused."""
    env: dict[str, str] = {}
    seen: dict[str, str] = {}
    for conn in conns:
        kind = kind_of(conn)
        if kind in seen:
            raise PreflightRefusal(
                "RIVET_AIRFLOW_CONNECTION_KIND_TWICE",
                f"connections `{seen[kind]}` and `{conn.conn_id}` are both {kind}: one task reads one {kind} credential",
            )
        seen[kind] = conn.conn_id
        env.update(KINDS[kind](conn, folder, config or {}))
    return env
