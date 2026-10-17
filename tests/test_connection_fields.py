"""Every field a Connection carries reaches rivet under its exact name, in each spelling Airflow stores it, and every
refusal names the Connection and says what to do. Written from the mutants the full run of 2026-10-17 left alive in
`connections.py` (run 39020026835); each test names the mutants it kills."""

from __future__ import annotations

import base64
import json
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow_provider_rivet.connections import EnvSpec, build_environment, cloud_environment, connection_url
from airflow_provider_rivet.preflight import PreflightRefusal

PEM = "-----BEGIN PRIVATE KEY-----\nMIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEA\n-----END PRIVATE KEY-----\n"


def conn(conn_id: str, conn_type: str, **kw: object) -> SimpleNamespace:
    fields = {"login": None, "password": None, "host": None, "port": None, "schema": None, "extra_dejson": {}}
    return SimpleNamespace(conn_id=conn_id, conn_type=conn_type, **{**fields, **kw})


def url_conn(**fields: object) -> SimpleNamespace:
    base = {"conn_id": "c", "conn_type": "postgres", "login": "u", "password": "p", "host": "h", "port": None}
    return SimpleNamespace(**{**base, "schema": "d", "extra_dejson": {}, **fields})


def refusal(call, *args: object) -> PreflightRefusal:
    with pytest.raises(PreflightRefusal) as refused:
        call(*args)
    return refused.value


def said(refused: PreflightRefusal) -> str:
    """A refusal's message without its `[code] ` prefix."""
    return str(refused).removeprefix(f"[{refused.code}] ")


def sub(tmp_path: Path, name: str) -> Path:
    """A fresh folder for one more environment (the task's private folder exists before the Connections are read)."""
    (folder := tmp_path / name).mkdir()
    return folder


def snow(env: dict[str, str]) -> dict:
    return tomllib.loads(Path(env["SNOWFLAKE_HOME"], "config.toml").read_text(encoding="utf-8"))


# --- The `extra__<kind>__<name>` form older providers wrote is read for every extra of every kind ----------------------
# Mutants: _aws 4/10/11/63/67/68, _azure 11/17/18, _clickhouse 7/13/14/27/33/34, _gcp 5/11/12/44/50/51,
# _snowflake 20/26/27/53/57/58/65/71/72/76/82/83, kind_of 5/11/12.


def test_every_extra_is_read_in_the_prefixed_form_older_providers_wrote(tmp_path: Path) -> None:
    gcp = conn(
        "gcp",
        "google_cloud_platform",
        extra_dejson={"extra__google_cloud_platform__keyfile_dict": "{}", "extra__google_cloud_platform__project": "p1"},
    )
    aws = conn(
        "aws",
        "aws",
        extra_dejson={
            "extra__aws__aws_session_token": "tok",
            "extra__aws__region_name": "eu-north-1",
            "extra__aws__endpoint_url": "http://minio:9000",
        },
    )
    az = conn("az", "wasb", extra_dejson={"extra__wasb__sas_token": "sv=1"})
    ch = conn("ch", "clickhouse", host="ch", extra_dejson={"extra__clickhouse__secure": "true"})
    env = cloud_environment([gcp, aws, az, ch], tmp_path)
    assert Path(env["GOOGLE_APPLICATION_CREDENTIALS"]).read_text() == "{}" and env["GOOGLE_CLOUD_PROJECT"] == "p1"
    assert (env["AWS_SESSION_TOKEN"], env["AWS_REGION"], env["AWS_ENDPOINT_URL"]) == ("tok", "eu-north-1", "http://minio:9000")
    assert env["AZURE_STORAGE_SAS_TOKEN"] == "sv=1" and env["RIVET_CLICKHOUSE_URL"] == "https://ch"
    proto = conn("ch", "clickhouse", host="ch", extra_dejson={"extra__clickhouse__protocol": "https"})
    assert cloud_environment([proto], sub(tmp_path, "p"))["RIVET_CLICKHOUSE_URL"] == "https://ch"
    sf = conn(
        "sf",
        "snowflake",
        login="L",
        extra_dejson={
            "extra__snowflake__account": "ab12345",
            "extra__snowflake__role": "R",
            "extra__snowflake__private_key_content": PEM,
        },
    )
    env = cloud_environment([sf], sub(tmp_path, "sf"))
    read = snow(env)["connections"]["sf"]
    assert (read["account"], read["role"], read["authenticator"]) == ("ab12345", "R", "SNOWFLAKE_JWT")
    assert Path(env["RIVET_SNOWFLAKE_KEY"]).read_text() == PEM
    filed = conn("sf", "snowflake", extra_dejson={"account": "a", "extra__snowflake__private_key_file": "/keys/k.p8"})
    assert cloud_environment([filed], sub(tmp_path, "f"))["RIVET_SNOWFLAKE_KEY"] == "/keys/k.p8"
    tagged = conn("t", "http", host="ch", extra_dejson={"extra__rivet__rivet_kind": "clickhouse"})
    assert cloud_environment([tagged], sub(tmp_path, "t"))["RIVET_CLICKHOUSE_URL"] == "http://ch"
    session = conn("aws", "aws", extra_dejson={"extra__aws__session_kwargs": {"profile_name": "prod"}})
    assert refusal(cloud_environment, [session], sub(tmp_path, "s")).code == "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED"


def test_an_empty_plain_extra_falls_back_to_the_prefixed_one_and_a_connection_without_extras_reads_none(
    tmp_path: Path,
) -> None:
    """_extra 7 (no default for a missing `extra_dejson`), _extra 15 (an empty plain value hides the prefixed one),
    connection_url 7."""
    aws = conn("aws", "aws", extra_dejson={"region_name": "", "extra__aws__region_name": "eu-north-1"})
    assert cloud_environment([aws], tmp_path)["AWS_REGION"] == "eu-north-1"
    bare = SimpleNamespace(conn_id="aws", conn_type="aws", login="AKIA", password=None)
    assert cloud_environment([bare], sub(tmp_path, "b")) == {"AWS_ACCESS_KEY_ID": "AKIA"}
    no_extra = SimpleNamespace(conn_id="c", conn_type="postgres", login="u", password=None, host="h", port=None, schema="d")
    assert connection_url(no_extra) == "postgresql://u@h/d"


# --- Each field reaches the variable or `snow` key rivet reads, under its exact name -------------------------------------
# Mutants: _aws 48-51/56-59, _azure 5-7, _clickhouse 17/20/21/25/26/31/32/48/49, _gcp 26/27,
# _snowflake 4/32/48/49/73-75/80/81.


def test_every_field_of_a_storage_connection_reaches_its_variable(tmp_path: Path) -> None:
    aws = conn("aws", "aws", extra_dejson={"aws_session_token": "tok", "endpoint_url": "http://minio:9000"})
    env = cloud_environment([aws], tmp_path)
    assert env == {"AWS_SESSION_TOKEN": "tok", "AWS_ENDPOINT_URL": "http://minio:9000"}
    az = cloud_environment([conn("az", "wasb", login="acct", password="key")], sub(tmp_path, "az"))
    assert az == {"AZURE_STORAGE_ACCOUNT": "acct", "AZURE_STORAGE_KEY": "key"}
    gcp = conn("gcp", "google_cloud_platform", extra_dejson={"keyfile_dict": {"type": "service_account"}})
    written = cloud_environment([gcp], sub(tmp_path, "gcp"))["GOOGLE_APPLICATION_CREDENTIALS"]
    assert json.loads(Path(written).read_text()) == {"type": "service_account"}, "a dict keyfile is written as its JSON"


@pytest.mark.parametrize(
    ("extra", "url"),
    [
        ({"secure": "1"}, "https://ch"),
        ({"secure": "yes"}, "https://ch"),
        ({"secure": "TRUE"}, "https://ch"),
        ({"secure": True}, "https://ch"),
        ({"secure": "no"}, "http://ch"),
        ({"secure": True, "protocol": "http"}, "http://ch"),
    ],
)
def test_clickhouse_secure_and_protocol_choose_the_scheme(tmp_path: Path, extra: dict, url: str) -> None:
    env = cloud_environment([conn("ch", "clickhouse", host="ch", extra_dejson=extra)], tmp_path)
    assert env["RIVET_CLICKHOUSE_URL"] == url and env["RIVET_CLICKHOUSE_USER"] == "default", (
        "no login is ClickHouse's default user"
    )


def test_every_field_of_a_snowflake_connection_reaches_snow(tmp_path: Path) -> None:
    sf = conn("sf", "snowflake", extra_dejson={"account": "a", "authenticator": "externalbrowser"})
    env = cloud_environment([sf], tmp_path, {"load": "not a mapping"})
    read = snow(env)["connections"]["sf"]
    assert read == {"account": "a", "user": "", "authenticator": "externalbrowser"}, "no login is an empty user"
    filed = conn("sf", "snowflake", password="pass", extra_dejson={"account": "a", "private_key_file": "/keys/k.p8"})
    env = cloud_environment([filed], sub(tmp_path, "f"))
    read = snow(env)["connections"]["sf"]
    assert (read["authenticator"], read["private_key_file"], env["RIVET_SNOWFLAKE_KEY"]) == (
        "SNOWFLAKE_JWT",
        "/keys/k.p8",
        "/keys/k.p8",
    )
    assert env["PRIVATE_KEY_PASSPHRASE"] == "pass" and "password" not in read


# --- A Snowflake key: PEM as written (leading blank lines too), or strict base64 of PEM --------------------------------
# Mutants: _pem 2/8/10/12/13/20/24.


def test_a_private_key_is_pem_or_strict_base64_of_pem(tmp_path: Path) -> None:
    def key(text: str, where: str) -> str:
        sf = conn("sf", "snowflake", extra_dejson={"account": "a", "private_key_content": text})
        return Path(cloud_environment([sf], sub(tmp_path, where))["RIVET_SNOWFLAKE_KEY"]).read_text()

    assert key("\n" + PEM, "lead") == "\n" + PEM, "a PEM after a blank line is PEM"
    wrapped = base64.encodebytes(PEM.encode()).decode()  # 76-column lines, as `base64` prints it
    assert "\n" in wrapped.strip() and key(wrapped, "wrapped") == PEM
    assert key(base64.b64encode(("\n" + PEM).encode()).decode(), "b64lead") == "\n" + PEM
    noisy = base64.b64encode(PEM.encode()).decode()
    noisy = noisy[:8] + "*" + noisy[8:]
    sf = conn("sf", "snowflake", extra_dejson={"account": "a", "private_key_content": noisy})
    refused = refusal(cloud_environment, [sf], sub(tmp_path, "noisy"))
    assert (refused.code, said(refused)) == (
        "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED",
        "Snowflake connection `sf`: extra `private_key_content` is neither a PEM key nor base64 of one",
    ), "base64 with a character outside its alphabet is not a key"


# --- Every refusal: its code, and a message that names the Connection and the remedy ------------------------------------
# Mutants: _aws 28/31-34/36-40, _cloud 8/13, _snowflake 36/111/115/122, kind_of 16, toml_string 2-5,
# url_host 29/32/34/37-42, connection_url 10/11/13/17-19/32/43/73, build_environment 46/47.


@pytest.mark.parametrize(
    ("session", "why"),
    [
        ({"profile_name": "prod"}, "rivet reads no AWS profile: put the keys in the Connection's login and password"),
        ({"aws_session_token": "t"}, "rivet reads no session_kwargs"),
    ],
)
def test_an_aws_session_setting_is_refused_with_its_reason(tmp_path: Path, session: dict, why: str) -> None:
    key = next(iter(session))
    refused = refusal(cloud_environment, [conn("aws", "aws", extra_dejson={"session_kwargs": session})], tmp_path)
    assert said(refused) == f"AWS connection `aws` sets `session_kwargs.{key}`, which rivet cannot use: {why}"


def test_the_kind_refusals_name_the_connections(tmp_path: Path) -> None:
    twice = refusal(cloud_environment, [conn("a", "aws", login="1"), conn("b", "aws", login="2")], tmp_path)
    assert said(twice) == "connections `a` and `b` are both aws: one task reads one aws credential"
    unknown = refusal(cloud_environment, [conn("h", "http")], tmp_path)
    assert said(unknown) == (
        "connection `h` has conn_type `http`, which names no storage or warehouse rivet reads; "
        "set its extra `rivet_kind` to one of ['aws', 'azure', 'clickhouse', 'gcp', 'snowflake']"
    )
    no_account = refusal(cloud_environment, [conn("sf", "snowflake")], tmp_path)
    assert no_account.code == "RIVET_AIRFLOW_CONNECTION_INCOMPLETE"


def test_a_lone_surrogate_is_refused_naming_the_field_never_its_value(tmp_path: Path) -> None:
    sf = conn("sf", "snowflake", extra_dejson={"account": "a", "role": "x\ud800y"})
    refused = refusal(cloud_environment, [sf], tmp_path)
    assert (refused.code, said(refused)) == (
        "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED",
        "Snowflake connection `sf`: field `role` holds a lone surrogate, which a TOML file cannot carry",
    )
    named = refusal(
        cloud_environment,
        [conn("sf", "snowflake", extra_dejson={"account": "a"})],
        sub(tmp_path, "n"),
        {"load": {"connection": "r\udc00"}},
    )
    assert said(named) == "Snowflake connection `sf`: the connection name holds a lone surrogate, which a TOML file cannot carry"


def test_the_url_refusals_name_the_connection_or_a_question_mark() -> None:
    nameless = SimpleNamespace(conn_type="http", login=None, password=None, host="h", port=None, schema=None, extra_dejson={})
    assert said(refusal(connection_url, nameless)) == (
        "connection `?` has conn_type `http`, which names no rivet URL scheme; pass the scheme as (conn_id, scheme) "
        "or set the connection extra `rivet_scheme` to one of ('postgresql', 'postgres', 'mysql', 'sqlserver', "
        "'mongodb', 'mongodb+srv', 'oracle')"
    )
    assert said(refusal(connection_url, url_conn(conn_type="http"))).startswith("connection `c` has conn_type `http`")
    iam = refusal(connection_url, url_conn(extra_dejson={"iam": True}))
    assert said(iam) == "connection `c` sets extra `iam` (IAM token authentication), which a rivet URL cannot carry"
    srv = refusal(connection_url, url_conn(conn_type="mongo", port=27017, extra_dejson={"srv": True}))
    assert said(srv) == "connection `c` is an SRV (`mongodb+srv`) connection with a port; clear the port"
    message = "has host `db:1`, which holds a `:` but is no IPv6 address, and a port as well; put the port in the port field only"
    assert said(refusal(connection_url, url_conn(host="db:1", port=5432))) == f"connection `c` {message}"
    hostport = SimpleNamespace(**{k: v for k, v in vars(url_conn(host="db:1", port=5432)).items() if k != "conn_id"})
    assert said(refusal(connection_url, hostport)) == f"connection `?` {message}"


def test_a_worker_state_the_task_would_not_pass_is_refused_with_the_whole_remedy(tmp_path: Path) -> None:
    """build_environment 4/7/37/39: the worker environment is the one passed in, not this process's; 46/47: the text."""
    worker = {"RIVET_STATE_URL": "postgresql://state/db", "AWS_REGION": "eu-north-1"}
    refused = refusal(build_environment, worker, EnvSpec(), [], tmp_path)
    assert refused.code == "RIVET_AIRFLOW_STATE_ENV_NOT_PASSED"
    assert said(refused).endswith(
        'env_from_connections={"RIVET_STATE_URL": "<conn_id>"} (or `env`); unset it on the worker if it is not meant for this pipeline'
    )
    env = build_environment(worker, EnvSpec(passthrough=["RIVET_STATE_URL"], clouds=["aws"]), [], tmp_path)
    assert (env["RIVET_STATE_URL"], env["AWS_REGION"]) == ("postgresql://state/db", "eu-north-1")


# --- A database URL: scheme, database, encoding and parameters exactly as the driver reads them -----------------------
# Mutants: _param 1/3-8/12, connection_url 23-25/46/62-64/80/107/114/127/131/132/142/146/153, url_host 9/47/52.


def test_the_url_scheme_comes_from_rivet_scheme_and_srv_only_turns_mongodb_into_srv() -> None:
    assert connection_url(url_conn(conn_type="generic", extra_dejson={"rivet_scheme": "mysql"})) == "mysql://u:p@h/d"
    assert connection_url(url_conn(extra_dejson={"srv": True})) == "postgresql://u:p@h/d", "srv is Mongo's only"
    for word in ("1", "yes", "YES", "true"):
        assert connection_url(url_conn(conn_type="mongo", extra_dejson={"srv": word})) == "mongodb+srv://u:p@h/d"
    assert connection_url(url_conn(conn_type="mongo", extra_dejson={"srv": "no"})) == "mongodb://u:p@h/d"


def test_the_url_encodes_slashes_in_credentials_and_database_and_reads_service_name_on_oracle_only() -> None:
    assert connection_url(url_conn(login="a/b", password="c/d", schema="e/f")) == "postgresql://a%2Fb:c%2Fd@h/e%2Ff"
    assert connection_url(url_conn(schema=None)) == "postgresql://u:p@h/", "no database is an empty path"
    assert connection_url(url_conn(extra_dejson={"service_name": "ORCL"})) == "postgresql://u:p@h/d"
    assert connection_url(url_conn(conn_type="oracle", extra_dejson={"service_name": "ORCL"})) == "oracle://u:p@h/ORCL"


def test_the_url_parameters_join_with_ampersand_and_rivet_params_names_its_keys_exactly() -> None:
    two = url_conn(extra_dejson={"sslmode": "require", "application_name": "a/b", "connect_timeout": ""})
    assert connection_url(two) == "postgresql://u:p@h/d?sslmode=require&application_name=a%2Fb"
    own = url_conn(extra_dejson={"sslmode": "require", "application_name": "x", "rivet_params": "sslmode=a=b&application_name=y"})
    assert connection_url(own) == "postgresql://u:p@h/d?sslmode=a=b&application_name=y"
    flags = url_conn(conn_type="mongo", extra_dejson={"tls": True, "allow_insecure": False})
    assert connection_url(flags) == "mongodb://u:p@h/d?tls=true&tlsAllowInvalidCertificates=false"


def test_the_host_is_bracketed_only_when_both_brackets_hold_an_ipv6_literal() -> None:
    assert connection_url(url_conn(host="[::1")) == "postgresql://u:p@[::1/d", "a host that is no IPv6 literal is kept"
    assert connection_url(url_conn(host="fe80::1%a/b")) == "postgresql://u:p@[fe80::1%25a%2Fb]/d"
    assert connection_url(url_conn(host="fe80::1%25x")) == "postgresql://u:p@[fe80::1%2525x]/d", "unbracketed: `25` is the zone"
