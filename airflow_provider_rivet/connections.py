"""Airflow Connections turned into what rivet reads: environment variables, URLs and private key files.

Nothing here imports Airflow: the operator looks every Connection up and hands the objects to `build_environment`,
which is the one place the subprocess environment is assembled.
"""

from __future__ import annotations

import base64
import binascii
import fnmatch
import ipaddress
import json
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

from .invocation import CLOUD_ENV, undeclared_state_variables, worker_environment
from .preflight import PreflightRefusal

CLICKHOUSE_URL = "RIVET_CLICKHOUSE_URL"
CLICKHOUSE_USER = "RIVET_CLICKHOUSE_USER"
CLICKHOUSE_PASSWORD = "RIVET_CLICKHOUSE_PASSWORD"
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
SNOWFLAKE_KEY = "RIVET_SNOWFLAKE_KEY"
SNOWFLAKE_PASSPHRASE = "PRIVATE_KEY_PASSPHRASE"  # what `snow` reads to decrypt an encrypted key
INCOMPLETE = "RIVET_AIRFLOW_CONNECTION_INCOMPLETE"
UNSUPPORTED = "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED"
# The variables by which a Connection of a kind names WHO rivet is; when it sets one, the worker's variables of that
# kind (`CLOUD_ENV`) are dropped, never merged with it.
CREDENTIALS = {
    "gcp": ("GOOGLE_APPLICATION_CREDENTIALS",),
    "aws": ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"),
    "azure": ("AZURE_STORAGE_KEY", "AZURE_STORAGE_SAS_TOKEN"),
}
_AWS_UNSUPPORTED = {
    "role_arn": "rivet assumes no role: store the role's session keys in login/password and extra `aws_session_token`, "
    "or run the worker as the role with an empty login and `cloud_credentials=['aws']`",
    "assume_role_method": "rivet assumes no role",
    "assume_role_kwargs": "rivet assumes no role",
    "profile_name": "rivet reads no AWS profile: put the keys in the Connection's login and password",
    "aws_access_key_id": "move the key to the Connection's login",
    "aws_secret_access_key": "move the secret to the Connection's password",
}
_AWS_SESSION_UNSUPPORTED = ("profile_name", "aws_access_key_id", "aws_secret_access_key", "aws_session_token")


def _extra(conn: Any, name: str, kind: str) -> Any:
    """An extra by its plain name or by the `extra__<kind>__<name>` form older providers wrote."""
    extra = getattr(conn, "extra_dejson", None) or {}
    value = extra.get(name)
    return value if value not in (None, "") else extra.get(f"extra__{kind}__{name}") or None


def _quote(text: str) -> str:
    """Percent-encode every byte that is not unreserved, `/` included."""
    # Equivalent mutant: safe="XXXX" encodes the same (quote never escapes `X`); `/` is pinned by its tests.
    return quote(text, safe="")  # pragma: no mutate


def _yes(value: Any) -> bool:
    """An extra that switches something on: `true`, `1` or `yes` in any case, or the boolean True."""
    return str(value).lower() in ("1", "true", "yes")


def _secret_file(folder: Path, name: str, data: str) -> str:
    """Write a 0600 UTF-8 file in the task's private folder; its path."""
    path = folder / name
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data.encode())
    return str(path)


_TOML_ESCAPES = {
    **{c: f"\\u{c:04x}" for c in (*range(0x20), 0x7F)},
    ord('"'): '\\"',
    ord("\\"): "\\\\",
    ord("\b"): "\\b",
    ord("\t"): "\\t",
    ord("\n"): "\\n",
    ord("\f"): "\\f",
    ord("\r"): "\\r",
}
_SURROGATE = re.compile("[\ud800-\udfff]")


def toml_string(text: str, what: str) -> str:
    """A TOML basic string holding exactly `text`: quote, backslash and control characters escaped, every other
    character (non-BMP included) written as itself. A lone surrogate has no TOML form and is refused; `what` names
    the field, never its value."""
    if _SURROGATE.search(text):
        raise PreflightRefusal(UNSUPPORTED, f"{what} holds a lone surrogate, which a TOML file cannot carry")
    return '"' + text.translate(_TOML_ESCAPES) + '"'


def url_host(conn: Any) -> str:
    """The Connection's host as a URL authority: an IPv6 literal bracketed with its zone encoded (`%25`, RFC 6874).
    A host that holds `:` and is not IPv6 is kept as written (`db:5432`), unless the port field is set too. No host is
    refused: every URL rivet gets from a Connection is built here, and without a host its driver dials a default."""
    if not conn.host:
        raise PreflightRefusal(INCOMPLETE, f"{conn.conn_type} connection `{conn.conn_id}` has no host")
    host = str(conn.host)
    bracketed = host.startswith("[") and host.endswith("]")
    bare = host[1:-1] if bracketed else host
    address, percent, zone = bare.partition("%")
    try:
        ipaddress.IPv6Address(address)
    except ValueError:
        if ":" in host and conn.port:
            raise PreflightRefusal(
                UNSUPPORTED,
                f"connection `{getattr(conn, 'conn_id', '?')}` has host `{host}`, which holds a `:` but is no IPv6 address, "
                "and a port as well; put the port in the port field only",
            ) from None
        return host
    if percent:
        zone = _quote(unquote(zone[2:] if bracketed and zone.startswith("25") else zone))
        address = f"{address}%25{zone}"
    return f"[{address}]"


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
    """Static or session keys for S3; the config names them with `access_key_env: AWS_ACCESS_KEY_ID` and so on.
    A setting rivet cannot honour (a role, a profile, keys in extras) is refused, never dropped."""
    session = _extra(conn, "session_kwargs", "aws")
    unsupported = [name for name in _AWS_UNSUPPORTED if _extra(conn, name, "aws")]
    unsupported += [f"session_kwargs.{k}" for k in _AWS_SESSION_UNSUPPORTED if isinstance(session, dict) and session.get(k)]
    if unsupported:
        name = unsupported[0]
        raise PreflightRefusal(
            UNSUPPORTED,
            f"AWS connection `{conn.conn_id}` sets `{name}`, which rivet cannot use: "
            f"{_AWS_UNSUPPORTED.get(name.removeprefix('session_kwargs.'), 'rivet reads no session_kwargs')}",
        )
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
    """The storage account and its key or SAS token; the config reads `account_name: ${AZURE_STORAGE_ACCOUNT}` and
    names the secret with `account_key_env` / `sas_token_env`."""
    env = {}
    if conn.login:
        env["AZURE_STORAGE_ACCOUNT"] = conn.login
    if conn.password:
        env["AZURE_STORAGE_KEY"] = conn.password
    sas = _extra(conn, "sas_token", "wasb")
    if sas:
        env["AZURE_STORAGE_SAS_TOKEN"] = str(sas)
    return env


def _pem(conn: Any, text: str) -> str:
    """`private_key_content` as PEM: as written, or decoded from the base64 form Airflow's SnowflakeHook takes."""
    if text.lstrip().startswith("-----BEGIN"):
        return text
    try:
        decoded = base64.b64decode("".join(text.split()), validate=True).decode()
        if decoded.lstrip().startswith("-----BEGIN"):
            return decoded
    except (binascii.Error, UnicodeDecodeError):
        pass
    raise PreflightRefusal(
        UNSUPPORTED, f"Snowflake connection `{conn.conn_id}`: extra `private_key_content` is neither a PEM key nor base64 of one"
    )


def _snowflake(conn: Any, folder: Path, config: Mapping[str, Any]) -> dict[str, str]:
    """A `snow` connection under the name the config's `load.connection` gives, in a private SNOWFLAKE_HOME. With a
    private key the Connection's password is the key's passphrase, as in Airflow's SnowflakeHook."""
    name = ((config.get("load") or {}).get("connection") if isinstance(config.get("load"), dict) else None) or conn.conn_id
    fields: dict[str, str] = {"account": str(_extra(conn, "account", "snowflake") or conn.host or ""), "user": conn.login or ""}
    if not fields["account"]:
        raise PreflightRefusal(INCOMPLETE, f"Snowflake connection `{conn.conn_id}` has no `account` extra")
    for key in ("region", "role", "warehouse", "database", "authenticator"):
        value = _extra(conn, key, "snowflake")
        if value:
            fields[key] = str(value)
    env: dict[str, str] = {}
    key_text = _extra(conn, "private_key_content", "snowflake")
    key_path = _extra(conn, "private_key_file", "snowflake")
    if key_text:
        key_path = _secret_file(folder, f"snowflake-{conn.conn_id}.p8", _pem(conn, str(key_text)))
    if key_path:
        fields["authenticator"] = "SNOWFLAKE_JWT"
        fields["private_key_file"] = str(key_path)
        env[SNOWFLAKE_KEY] = str(key_path)
        if conn.password:
            env[SNOWFLAKE_PASSPHRASE] = conn.password
    elif conn.password:
        fields["password"] = conn.password
    where = f"Snowflake connection `{conn.conn_id}`"
    body = f"[connections.{toml_string(str(name), f'{where}: the connection name')}]\n" + "".join(
        f"{k} = {toml_string(v, f'{where}: field `{k}`')}\n" for k, v in fields.items()
    )
    # Equivalent mutant: the folder's name is private to the task; `snow` finds it through SNOWFLAKE_HOME only.
    home = folder / "snowflake"  # pragma: no mutate
    home.mkdir(mode=0o700)
    _secret_file(home, "config.toml", body)
    env["SNOWFLAKE_HOME"] = str(home)
    return env


def _clickhouse(conn: Any, folder: Path, config: Mapping[str, Any]) -> dict[str, str]:
    """The HTTP endpoint, user and password; the config reads `url: ${RIVET_CLICKHOUSE_URL}` and the password from its
    `password_env` (RIVET_CLICKHOUSE_PASSWORD always carries it as well)."""
    secure = _yes(_extra(conn, "secure", "clickhouse"))
    scheme = str(_extra(conn, "protocol", "clickhouse") or ("https" if secure else "http"))
    port = f":{conn.port}" if conn.port else ""
    env = {CLICKHOUSE_URL: f"{scheme}://{url_host(conn)}{port}", CLICKHOUSE_USER: conn.login or "default"}
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
    return _cloud(conns, folder, config or {})[0]


def _cloud(conns: Sequence[Any], folder: Path, config: Mapping[str, Any]) -> tuple[dict[str, str], set[str]]:
    """`cloud_environment`, and the kinds whose Connection supplied a credential."""
    env: dict[str, str] = {}
    seen: dict[str, str] = {}
    replaced: set[str] = set()
    for conn in conns:
        kind = kind_of(conn)
        if kind in seen:
            raise PreflightRefusal(
                "RIVET_AIRFLOW_CONNECTION_KIND_TWICE",
                f"connections `{seen[kind]}` and `{conn.conn_id}` are both {kind}: one task reads one {kind} credential",
            )
        seen[kind] = conn.conn_id
        made = KINDS[kind](conn, folder, config)
        if any(name in made for name in CREDENTIALS.get(kind, ())):
            replaced.add(kind)
        env.update(made)
    return env, replaced


# --- Database URLs (`env_from_connections`) ---

_SCHEMES = {"postgres": "postgresql", "mssql": "sqlserver", "mongo": "mongodb"}
RIVET_SCHEMES = ("postgresql", "postgres", "mysql", "sqlserver", "mongodb", "mongodb+srv", "oracle")
# Standard extras of Airflow's database Connections that have a URL form rivet's drivers accept: extra -> parameter.
URL_EXTRAS = {
    "postgresql": {"sslmode": "sslmode", "application_name": "application_name", "connect_timeout": "connect_timeout"},
    "mongodb": {
        "authSource": "authSource",
        "authMechanism": "authMechanism",
        "replicaSet": "replicaSet",
        "tls": "tls",
        "ssl": "tls",
        "allow_insecure": "tlsAllowInvalidCertificates",
        "directConnection": "directConnection",
        "readPreference": "readPreference",
        "appName": "appName",
    },
}
URL_EXTRAS["postgres"] = URL_EXTRAS["postgresql"]
URL_EXTRAS["mongodb+srv"] = URL_EXTRAS["mongodb"]
# Extras that change where or as whom the hook connects and have no URL form: refused, never dropped.
URL_UNSUPPORTED = {
    "postgresql": {"iam": "IAM token authentication", "redshift": "Redshift IAM authentication"},
    "mysql": {"unix_socket": "a Unix socket"},
}
URL_UNSUPPORTED["postgres"] = URL_UNSUPPORTED["postgresql"]


def _param(value: Any) -> str:
    return ("true" if value else "false") if isinstance(value, bool) else _quote(str(value))


def connection_url(conn: Any, scheme: str | None = None) -> str:
    """A rivet URL from an Airflow Connection's fields, credentials percent-encoded; an unknown scheme is refused.
    Well-known extras (`URL_EXTRAS`) become query parameters, Mongo's `srv` the `mongodb+srv` scheme; an extra in
    `URL_UNSUPPORTED` is refused; `rivet_params` is appended and wins for a parameter it names."""
    extra = getattr(conn, "extra_dejson", None) or {}
    conn_id = getattr(conn, "conn_id", "?")
    scheme = scheme or extra.get("rivet_scheme") or _SCHEMES.get(conn.conn_type, conn.conn_type)
    if scheme not in RIVET_SCHEMES:
        raise PreflightRefusal(
            "RIVET_AIRFLOW_CONNECTION_SCHEME",
            f"connection `{conn_id}` has conn_type `{conn.conn_type}`, which names no rivet URL "
            f"scheme; pass the scheme as (conn_id, scheme) or set the connection extra `rivet_scheme` to one of {RIVET_SCHEMES}",
        )
    for name, what in URL_UNSUPPORTED.get(scheme, {}).items():
        if extra.get(name):
            raise PreflightRefusal(
                UNSUPPORTED, f"connection `{conn_id}` sets extra `{name}` ({what}), which a rivet URL cannot carry"
            )
    if scheme == "mongodb" and _yes(extra.get("srv")):
        scheme = "mongodb+srv"
    if scheme == "mongodb+srv" and conn.port:
        raise PreflightRefusal(
            UNSUPPORTED, f"connection `{conn_id}` is an SRV (`mongodb+srv`) connection with a port; clear the port"
        )
    auth = _quote(conn.login or "")
    if conn.password:
        auth += ":" + _quote(conn.password)
    auth += "@" if auth else ""
    port = f":{conn.port}" if conn.port else ""
    database = (extra.get("service_name") if scheme == "oracle" else None) or conn.schema or ""
    own = str(extra.get("rivet_params") or "")
    named = {pair.partition("=")[0] for pair in own.split("&") if pair}
    params = [
        f"{key}={_param(extra[name])}"
        for name, key in URL_EXTRAS.get(scheme, {}).items()
        if extra.get(name) not in (None, "") and key not in named
    ]
    query = "&".join([*params, *([own] if own else [])])
    return f"{scheme}://{auth}{url_host(conn)}{port}/{_quote(str(database))}{'?' + query if query else ''}"


# --- A database by name: an Airflow Connection, else an ODBC data source ---

# What an odbc.ini `Driver` names -> the Connection type it is; matched on the lower-cased name or library path.
ODBC_DRIVERS = (
    ("mysql", "mysql"), ("myodbc", "mysql"), ("mariadb", "mysql"),
    ("postgres", "postgres"), ("psqlodbc", "postgres"),
    ("sql server", "mssql"), ("msodbcsql", "mssql"), ("freetds", "mssql"), ("tdsodbc", "mssql"),
    ("oracle", "oracle"), ("sqora", "oracle"),
)  # fmt: skip
# odbc.ini keys (case-insensitive, first one set wins) -> the Connection field they fill.
ODBC_FIELDS = {
    "host": ("server", "servername", "host", "dbq"),
    "port": ("port",),
    "login": ("user", "uid", "username", "userid"),
    "password": ("password", "pwd"),
    "schema": ("database", "db"),
}
# Oracle's EZConnect `[//]host[:port]/service`; anything else in ServerName is a tnsnames.ora alias.
EZCONNECT = re.compile(r"(?://)?(?P<host>[^:/\s]+)(?::(?P<port>[^/]*))?/(?P<service>\S+)")
REF_KEYS = {"conn_id", "scheme", "database", "odbc_ini", "dsn"}


@dataclass(frozen=True)
class OdbcConnection:
    """An odbc.ini data source in the shape `connection_url` reads from an Airflow Connection."""

    conn_id: str
    conn_type: str
    host: str | None
    port: int | None
    login: str | None
    password: str | None
    schema: str | None
    extra_dejson: Mapping[str, Any] = field(default_factory=dict)


def odbc_connection(path: str, name: str) -> OdbcConnection | None:
    """The data source `[name]` of the odbc.ini at `path`, or None when the file or the section is absent. A driver
    rivet has no URL for, or a port that is no number, is refused: the data source exists, so a fallback would hide it."""
    import configparser

    # Passwords hold `%` (no interpolation); a file that repeats a section or key is read, the last one winning.
    ini = configparser.ConfigParser(interpolation=None, strict=False)  # pragma: no mutate - strict=None reads as False
    try:
        ini.read(path)
    except configparser.Error as exc:
        raise PreflightRefusal(UNSUPPORTED, f"{path} is not a readable odbc.ini ({type(exc).__name__})") from None
    if not ini.has_section(name):
        return None
    section = dict(ini[name])  # keys lower-cased by the parser
    driver = section.get("driver", "")
    conn_type = next((kind for word, kind in ODBC_DRIVERS if word in driver.lower()), None)
    where = f"ODBC data source [{name}] in {path}"
    if conn_type is None:
        why = (
            "MongoDB's ODBC driver reads through the BI Connector's MySQL protocol, not the database rivet reads"
            if "mongo" in driver.lower()
            else "rivet reads MySQL, PostgreSQL, SQL Server and Oracle data sources from odbc.ini"
        )
        raise PreflightRefusal(
            UNSUPPORTED, f"{where} uses driver `{driver}`; {why}: create an Airflow Connection `{name}` instead"
        )
    values = {f: next((section[k].strip() for k in keys if section.get(k, "").strip()), None) for f, keys in ODBC_FIELDS.items()}
    if conn_type == "mssql":
        _odbc_sqlserver(section, values, where)
    elif conn_type == "oracle":
        _odbc_oracle(values, where)
    port = values.pop("port")
    if port is not None and not port.isdigit():
        raise PreflightRefusal(UNSUPPORTED, f"ODBC data source [{name}] in {path} has port `{port}`, which is no number")
    return OdbcConnection(conn_id=name, conn_type=conn_type, port=int(port) if port else None, **values)


def _odbc_sqlserver(section: Mapping[str, str], values: dict[str, Any], where: str) -> None:
    """SQL Server's `Server`: `[tcp:]host[,port]`; a named instance (`host\\name`) and Windows or Entra sign-in are
    refused, since a rivet URL carries a host, a port and a password."""
    if section.get("trusted_connection", "").strip().lower() in ("yes", "true") or section.get("authentication", "").strip():
        raise PreflightRefusal(
            UNSUPPORTED, f"{where} signs in with Windows or Entra ID; rivet signs in with a SQL login: set UID and PWD"
        )
    host = values["host"] or ""
    if "\\" in host:
        raise PreflightRefusal(
            UNSUPPORTED, f"{where} names the instance `{host}`; give the instance's TCP port: Server = host,port"
        )
    host, comma, port = host.removeprefix("tcp:").partition(",")
    values["host"] = host.strip() or None
    if comma:
        values["port"] = port.strip()


def _odbc_oracle(values: dict[str, Any], where: str) -> None:
    """Oracle's `ServerName` as EZConnect `[//]host[:port]/service`, the service becoming the database; a tnsnames.ora
    alias is refused, since resolving it needs the worker's Oracle network files."""
    match = EZCONNECT.fullmatch(values["host"] or "")
    if match is None:
        raise PreflightRefusal(
            UNSUPPORTED,
            f"{where} names `{values['host']}`, a tnsnames.ora alias; write ServerName = host:port/service, or create "
            "an Airflow Connection",
        )
    values["host"], values["schema"] = match["host"], match["service"]
    if match["port"] is not None:
        values["port"] = match["port"]


def database_ref(ref: Any) -> dict[str, Any]:
    """An `env_from_connections` value as a dict: `conn_id`, `(conn_id, scheme)`, or a dict of `conn_id` and optional
    `scheme`, `database` (instead of the Connection's own) and `odbc_ini` (the file whose data source `dsn`, default
    `conn_id`, is used when no Airflow Connection has that id)."""
    if isinstance(ref, str):
        return {"conn_id": ref}
    if isinstance(ref, Mapping):
        unknown = sorted(set(ref) - REF_KEYS)
        if unknown or not ref.get("conn_id"):
            raise ValueError(
                f"an env_from_connections dict has `conn_id` and optionally {sorted(REF_KEYS - {'conn_id'})}; got {sorted(ref)}"
            )
        return dict(ref)
    conn_id, scheme = ref
    return {"conn_id": conn_id, "scheme": scheme}


def missing_connection(conn_id: str, odbc_ini: str | None = None, dsn: str | None = None) -> PreflightRefusal:
    """The refusal of a Connection no backend defines (and, with `odbc_ini`, no data source `dsn` of that file): final,
    since a retry cannot create it."""
    return PreflightRefusal(
        "RIVET_AIRFLOW_CONNECTION_MISSING",
        f"connection `{conn_id}` is not defined in the metadata database or any secrets backend this worker reads"
        + (f", and {odbc_ini} has no data source [{dsn}]" if odbc_ini else "")
        + "; create it, or correct the id the task names",
    )


def resolve_database(ref: Mapping[str, Any], lookup: Callable[[str], Any]) -> tuple[Any, str | None]:
    """The database an `env_from_connections` entry (`database_ref`) names, and the data source it was read from:
    `lookup(conn_id)` (the Connection, or None when no backend defines it), else with `odbc_ini` the data source `dsn`
    (default the id) of that file, `None` naming no data source; its `database` instead of the one stored."""
    conn_id, odbc_ini = ref["conn_id"], ref.get("odbc_ini")
    dsn = ref.get("dsn") or conn_id
    conn, source = lookup(conn_id), None
    if conn is None and odbc_ini:
        conn, source = odbc_connection(odbc_ini, dsn), dsn
    if conn is None:
        raise missing_connection(conn_id, odbc_ini, dsn)
    return with_database(conn, ref.get("database")), source


def with_database(conn: Any, database: str | None) -> Any:
    """`conn` with its database (`schema`) replaced, the Connection itself untouched; `conn` when `database` is None."""
    if database is None:
        return conn
    fields = {name: getattr(conn, name, None) for name in ("conn_id", "conn_type", "host", "port", "login", "password")}
    return OdbcConnection(**fields, schema=database, extra_dejson=dict(getattr(conn, "extra_dejson", None) or {}))


# --- The whole subprocess environment ---


@dataclass(frozen=True)
class EnvSpec:
    """What a task declares about rivet's environment. `urls` maps a variable to an already looked-up Connection and
    an optional scheme; `config` is the config document, for the names a Connection is filed under."""

    passthrough: Sequence[str] = ()
    clouds: Sequence[str] = ()
    env: Mapping[str, Any] = field(default_factory=dict)
    urls: Mapping[str, tuple[Any, str | None]] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)


def build_environment(
    worker_env: Mapping[str, str], spec: EnvSpec, resolved_connections: Sequence[Any], secrets_dir: Path
) -> dict[str, str]:
    """rivet's environment, in order: the worker's allow-listed and passed-through variables; the storage and
    warehouse Connections, each REPLACING the worker's variables of its kind when it supplies a credential (a
    Connection's static keys never ride beside the worker's session token); `env`; URLs from Connections. Key
    files go to `secrets_dir`. A worker PostgreSQL state the task would not pass is refused."""
    env = worker_environment(spec.passthrough, spec.clouds, worker_env)
    if resolved_connections:
        made, replaced = _cloud(resolved_connections, secrets_dir, spec.config)
        patterns = [pattern for kind in replaced for pattern in CLOUD_ENV[kind]]
        env = {k: v for k, v in env.items() if not any(fnmatch.fnmatchcase(k, p) for p in patterns)}
        env.update(made)
    env.update({str(k): str(v) for k, v in spec.env.items()})
    for name, (conn, scheme) in spec.urls.items():
        env[name] = connection_url(conn, scheme)
    for name in undeclared_state_variables(env, worker_env):
        raise PreflightRefusal(
            "RIVET_AIRFLOW_STATE_ENV_NOT_PASSED",
            f"the worker's environment sets {name} to a PostgreSQL state and this task would not pass it to rivet, which would then "
            f"run on an empty SQLite state and treat every export as a first run. Either inherit the worker's "
            f'value with env_passthrough=["{name}"], or give the task its own with '
            f'env_from_connections={{"{name}": "<conn_id>"}} (or `env`); unset it on the worker if it is not '
            "meant for this pipeline",
        )
    return env
