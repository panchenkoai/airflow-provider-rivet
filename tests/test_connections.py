"""Storage and warehouse Connections reach rivet as variables and private key files, and leave nothing behind."""

from __future__ import annotations

import base64
import json
import random
import stat
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowFailException
from conftest import Rig

from airflow_provider_rivet import connections
from airflow_provider_rivet.connections import EnvSpec, build_environment, cloud_environment, toml_string, url_host
from airflow_provider_rivet.operators import RivetLoadOperator, RivetRunOperator, connection_url
from airflow_provider_rivet.preflight import PreflightRefusal

KEY = '{"type": "service_account", "private_key": "-----BEGIN PRIVATE KEY-----\\nSECRETKEY\\n-----END PRIVATE KEY-----\\n"}'


def conn(conn_id: str, conn_type: str, **kw) -> SimpleNamespace:
    """A Connection with the fields rivet's mapping reads."""
    fields = {"login": None, "password": None, "host": None, "port": None, "schema": None, "extra_dejson": {}}
    return SimpleNamespace(conn_id=conn_id, conn_type=conn_type, **{**fields, **kw})


def mode(path: str) -> int:
    return stat.S_IMODE(Path(path).stat().st_mode)


def test_each_kind_maps_to_the_variables_rivet_reads(tmp_path: Path) -> None:
    gcp = conn("gcp", "google_cloud_platform", extra_dejson={"keyfile_dict": KEY, "project": "p1"})
    aws = conn("aws", "aws", login="AKIA1", password="s3cret", extra_dejson={"region_name": "eu-north-1"})
    azure = conn("az", "wasb", login="acct", extra_dejson={"sas_token": "sv=1&sig=x"})
    ch = conn("ch", "clickhouse", host="ch", port=8443, login="loader", password="pw", extra_dejson={"secure": True})
    env = cloud_environment([gcp, aws, azure, ch], tmp_path)
    assert Path(env["GOOGLE_APPLICATION_CREDENTIALS"]).read_text() == KEY and mode(env["GOOGLE_APPLICATION_CREDENTIALS"]) == 0o600
    assert env["GOOGLE_CLOUD_PROJECT"] == "p1"
    assert (env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"], env["AWS_REGION"]) == ("AKIA1", "s3cret", "eu-north-1")
    assert (env["AZURE_STORAGE_ACCOUNT"], env["AZURE_STORAGE_SAS_TOKEN"]) == ("acct", "sv=1&sig=x")
    assert env["RIVET_CLICKHOUSE_URL"] == "https://ch:8443" and env["RIVET_CLICKHOUSE_USER"] == "loader"
    assert env["RIVET_CLICKHOUSE_PASSWORD"] == "pw"
    old = conn("gcp", "google_cloud_platform", extra_dejson={"extra__google_cloud_platform__key_path": "/k.json"})
    assert cloud_environment([old], tmp_path)["GOOGLE_APPLICATION_CREDENTIALS"] == "/k.json", "the prefixed extra form"


def test_a_snowflake_connection_is_a_snow_connection_under_the_configs_name(tmp_path: Path) -> None:
    sf = conn(
        "sf",
        "snowflake",
        login="LOADER",
        extra_dejson={
            "account": "ab12345",
            "role": "R",
            "warehouse": "WH",
            "private_key_content": "-----BEGIN PRIVATE KEY-----\nK\n",
        },
    )
    env = cloud_environment([sf], tmp_path, {"load": {"target": "snowflake", "connection": "rivet"}})
    toml = Path(env["SNOWFLAKE_HOME"], "config.toml")
    assert mode(str(toml)) == 0o600 and mode(env["SNOWFLAKE_HOME"]) == 0o700
    body = toml.read_text()
    assert (
        body.startswith('[connections."rivet"]\n') and 'account = "ab12345"' in body and 'authenticator = "SNOWFLAKE_JWT"' in body
    )
    assert (
        Path(env["RIVET_SNOWFLAKE_KEY"]).read_text().startswith("-----BEGIN")
        and f'private_key_file = "{env["RIVET_SNOWFLAKE_KEY"]}"' in body
    )
    with pytest.raises(PreflightRefusal, match="no `account`"):
        cloud_environment([conn("sf2", "snowflake", login="x")], tmp_path / "x")


def test_an_unknown_kind_and_two_of_one_kind_are_refused(tmp_path: Path) -> None:
    with pytest.raises(PreflightRefusal) as unknown:
        cloud_environment([conn("h", "http")], tmp_path)
    assert unknown.value.code == "RIVET_AIRFLOW_CONNECTION_KIND"
    tagged = conn("h", "http", host="ch", extra_dejson={"rivet_kind": "clickhouse"})
    assert cloud_environment([tagged], tmp_path)["RIVET_CLICKHOUSE_URL"] == "http://ch"
    with pytest.raises(PreflightRefusal) as twice:
        cloud_environment([conn("a", "aws", login="1"), conn("b", "aws", login="2")], tmp_path)
    assert twice.value.code == "RIVET_AIRFLOW_CONNECTION_KIND_TWICE"


def test_the_operator_passes_the_key_file_removes_it_afterwards_and_never_shows_it(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    gcp = conn("gcp", "google_cloud_platform", extra_dejson={"keyfile_dict": KEY})
    monkeypatch.setattr("airflow_provider_rivet.operators.BaseHook.get_connection", lambda conn_id: gcp)
    rig.scenario(record_env=["GOOGLE_APPLICATION_CREDENTIALS"], leak_env=[])
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", connections=["gcp"]))
    key_path = rig.calls()[-1]["env"]["GOOGLE_APPLICATION_CREDENTIALS"]
    assert payload["decision"] == "success" and not Path(key_path).exists(), "the key file outlives the task"
    assert not Path(key_path).parent.exists(), "the private folder outlives the task"
    assert "SECRETKEY" not in json.dumps(payload) + json.dumps(rig.records)
    assert "connections" not in RivetLoadOperator.template_fields


def test_a_refused_connection_fails_the_task_before_rivet_and_leaves_no_folder(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    made: list[str] = []
    real = __import__("tempfile").mkdtemp
    monkeypatch.setattr("airflow_provider_rivet.operators.tempfile.mkdtemp", lambda **kw: made.append(real(**kw)) or made[-1])
    monkeypatch.setattr("airflow_provider_rivet.operators.BaseHook.get_connection", lambda conn_id: conn(conn_id, "http"))
    exc, ti = rig.run(rig.operator(RivetRunOperator, export="orders", connections=["h"]))
    assert isinstance(exc, AirflowFailException) and not rig.calls()
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_CONNECTION_KIND"
    assert made and not Path(made[0]).exists()


def test_the_clickhouse_password_reaches_the_variable_the_config_names(tmp_path: Path) -> None:
    ch = conn("ch", "clickhouse", host="ch", login="loader", password="pw")
    scaffold = {"load": {"target": "clickhouse", "password_env": "CLICKHOUSE_PASSWORD"}}  # what `rivet init` writes
    env = cloud_environment([ch], tmp_path, scaffold)
    assert env["CLICKHOUSE_PASSWORD"] == "pw" and env["RIVET_CLICKHOUSE_PASSWORD"] == "pw"
    odd = cloud_environment([ch], tmp_path / "x", {"load": {"password_env": "PATH; rm -rf /"}})
    assert "PATH; rm -rf /" not in odd and odd["RIVET_CLICKHOUSE_PASSWORD"] == "pw"


# --- The environment built from Connections: one test per defect of the 2026-10 bug hunt (D12-D23, D32) ---

TOML_SAFE_FIELDS = ("password", "role", "warehouse", "database")


def sf_toml(env: dict[str, str]) -> dict:
    """The `snow` config this package wrote, read the way `snow` reads it."""
    return tomllib.loads(Path(env["SNOWFLAKE_HOME"], "config.toml").read_text(encoding="utf-8"))


def pem_key() -> str:
    return "-----BEGIN PRIVATE KEY-----\nMIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEA\n-----END PRIVATE KEY-----\n"


def test_d12_a_connection_of_one_kind_replaces_the_workers_credentials_of_that_kind(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D12: `cloud_credentials=["aws", "azure"]` with an AWS and an Azure Connection sent the worker's temporary
    `AWS_SESSION_TOKEN` beside the Connection's static key pair (S3 rejects the mix) and the worker's account key
    beside the Connection's SAS token. A Connection that supplies a credential of a kind replaces the worker's
    variables of that kind; one that supplies none (only a region) leaves the worker's identity in place."""
    worker = {
        "AWS_ACCESS_KEY_ID": "ASIAWORKER",
        "AWS_SECRET_ACCESS_KEY": "worker-secret",
        "AWS_SESSION_TOKEN": "worker-token",
        "AWS_PROFILE": "worker-profile",
        "AZURE_STORAGE_KEY": "worker-account-key",
        "AZURE_STORAGE_ACCOUNT": "workeracct",
    }
    for name, value in worker.items():
        monkeypatch.setenv(name, value)
    conns = {
        "aws": conn("aws", "aws", login="AKIACONN", password="conn-secret"),
        "az": conn("az", "wasb", login="connacct", extra_dejson={"sas_token": "sv=1&sig=x"}),
        "aws_region_only": conn("aws_region_only", "aws", extra_dejson={"region_name": "eu-north-1"}),
    }
    monkeypatch.setattr("airflow_provider_rivet.operators.BaseHook.get_connection", lambda conn_id: conns[conn_id])
    rig.scenario(record_env=[*worker, "AZURE_STORAGE_SAS_TOKEN", "AWS_REGION"], leak_env=[])
    op = rig.operator(RivetRunOperator, export="orders", cloud_credentials=["aws", "azure"], connections=["aws", "az"])
    payload, _ = rig.run(op)
    seen = rig.calls()[-1]["env"]
    assert payload["decision"] == "success"
    assert seen["AWS_SESSION_TOKEN"] is None and seen["AWS_PROFILE"] is None, "the worker's AWS session leaked in"
    assert (seen["AWS_ACCESS_KEY_ID"], seen["AWS_SECRET_ACCESS_KEY"]) == ("AKIACONN", "conn-secret")
    assert seen["AZURE_STORAGE_KEY"] is None, "the worker's account key sits beside the Connection's SAS token"
    assert (seen["AZURE_STORAGE_SAS_TOKEN"], seen["AZURE_STORAGE_ACCOUNT"]) == ("sv=1&sig=x", "connacct")
    keep = rig.operator(RivetRunOperator, export="orders", cloud_credentials=["aws"], connections=["aws_region_only"])
    rig.run(keep)
    seen = rig.calls()[-1]["env"]
    assert (seen["AWS_SESSION_TOKEN"], seen["AWS_REGION"]) == ("worker-token", "eu-north-1"), "no credential, no replace"


@pytest.mark.parametrize(
    "extra",
    [
        {"role_arn": "arn:aws:iam::1:role/r"},
        {"profile_name": "prod"},
        {"aws_access_key_id": "AKIAEXTRA", "aws_secret_access_key": "extra-secret"},
        {"extra__aws__role_arn": "arn:aws:iam::1:role/r"},
        {"session_kwargs": {"profile_name": "prod"}},
    ],
)
def test_d13_aws_settings_rivet_cannot_honour_are_refused_not_dropped(tmp_path: Path, extra: dict) -> None:
    """D13: an AWS Connection's `role_arn`, `profile_name` and keys held in extras produced no variable and no
    refusal, so rivet ran as the worker's identity (or failed on an unset variable)."""
    aws = conn("aws", "aws", login="AKIA1", password="s", extra_dejson=extra)
    with pytest.raises(PreflightRefusal) as refused:
        cloud_environment([aws], tmp_path)
    assert refused.value.code == "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED"
    assert "extra-secret" not in str(refused.value)


def test_d14_the_passphrase_of_an_encrypted_snowflake_key_reaches_snow(tmp_path: Path) -> None:
    """D14: with a private key, the Connection's password is the key's passphrase (as in Airflow's SnowflakeHook);
    it was dropped, so `snow` could not decrypt the key. `snow` reads it from PRIVATE_KEY_PASSPHRASE."""
    sf = conn(
        "sf",
        "snowflake",
        login="LOADER",
        password="key-pass",
        extra_dejson={"account": "ab12345", "private_key_content": pem_key()},
    )
    env = cloud_environment([sf], tmp_path, {"load": {"connection": "rivet"}})
    assert env.get("PRIVATE_KEY_PASSPHRASE") == "key-pass"
    assert "password" not in sf_toml(env)["connections"]["rivet"], "the passphrase is not a login password"


def test_d15_a_base64_private_key_content_is_written_as_pem(tmp_path: Path) -> None:
    """D15: Airflow's SnowflakeHook takes `private_key_content` base64-encoded; it was written verbatim to the .p8
    file, which `snow` cannot read. Content that is neither PEM nor base64 of PEM is refused."""
    encoded = base64.b64encode(pem_key().encode()).decode()
    sf = conn("sf", "snowflake", login="L", extra_dejson={"account": "ab12345", "private_key_content": encoded})
    env = cloud_environment([sf], tmp_path)
    assert Path(env["RIVET_SNOWFLAKE_KEY"]).read_text() == pem_key()
    junk = conn("sf", "snowflake", login="L", extra_dejson={"account": "ab12345", "private_key_content": "not a key"})
    with pytest.raises(PreflightRefusal) as refused:
        cloud_environment([junk], tmp_path / "junk")
    assert refused.value.code == "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED" and "not a key" not in str(refused.value)


def test_d19_the_snowflake_region_reaches_the_snow_connection(tmp_path: Path) -> None:
    """D19: a legacy account locator (`xy12345`) with the extra `region` lost the region, so `snow` dialled the
    wrong account. `region` is a `snow` connection key; the connector builds the host from it."""
    sf = conn("sf", "snowflake", login="L", password="p", extra_dejson={"account": "xy12345", "region": "eu-west-1"})
    env = cloud_environment([sf], tmp_path)
    assert sf_toml(env)["connections"]["sf"].get("region") == "eu-west-1"


ALPHABET = [chr(c) for c in range(0x80)] + ["é", "ß", "€", " ", "﻿", "\U0001f600", "\U0001d11e", "\U0010fffd"]


def test_d20_every_snowflake_field_round_trips_through_toml(tmp_path: Path) -> None:
    """D20: `config.toml` was written with `json.dumps`, which escapes a non-BMP character as a surrogate pair that no
    TOML reader accepts (an emoji in a password broke every load). Property: any text in any field, and in the
    connection name, reads back unchanged through `tomllib`; one case holds every non-surrogate code point."""
    rng = random.Random(20)
    every = "".join(chr(c) for c in range(0x110000) if not 0xD800 <= c <= 0xDFFF)
    cases = [every, "\U0001f600", 'a"b\\c\nd\te\x00f\x7f\x1b', ""] + [
        "".join(rng.choice(ALPHABET) for _ in range(rng.randrange(1, 40))) for _ in range(300)
    ]
    for i, text in enumerate(cases):
        extra = {"account": "ab12345", **{k: text for k in TOML_SAFE_FIELDS if k != "password"}}
        sf = conn("sf", "snowflake", login=text or "L", password=text or None, extra_dejson=extra)
        name = text or "rivet"
        (folder := tmp_path / str(i)).mkdir()
        env = cloud_environment([sf], folder, {"load": {"connection": name}})
        read = sf_toml(env)["connections"][name]
        assert read["user"] == (text or "L")
        assert all(read[k] == text for k in TOML_SAFE_FIELDS if text), f"case {i} did not round-trip"


def test_d22_a_clickhouse_connection_without_a_host_is_refused(tmp_path: Path) -> None:
    """D22: an empty host became `http://None`, a DNS failure at load time instead of a refusal before rivet."""
    for host in (None, ""):
        with pytest.raises(PreflightRefusal) as refused:
            cloud_environment([conn("ch", "clickhouse", host=host, login="u")], tmp_path)
        assert refused.value.code == "RIVET_AIRFLOW_CONNECTION_INCOMPLETE"
    v6 = cloud_environment([conn("ch", "clickhouse", host="fd00::5", port=8123)], tmp_path)
    assert v6["RIVET_CLICKHOUSE_URL"] == "http://[fd00::5]:8123"


def test_d32_the_readme_tells_the_config_to_read_the_azure_account_variable() -> None:
    """D32: the Azure Connection exports AZURE_STORAGE_ACCOUNT, which rivet never reads (its config needs
    `account_name`), and the README row omitted it: a user following the row was refused "requires account_name"."""
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()
    row = next(line for line in readme.splitlines() if line.startswith("| `azure`"))
    assert "account_name: ${AZURE_STORAGE_ACCOUNT}" in row


def url_conn(**fields: object) -> SimpleNamespace:
    base = {"conn_id": "c", "conn_type": "postgres", "login": "u", "password": "p", "host": "h", "port": None}
    return SimpleNamespace(**{**base, "schema": "d", "extra_dejson": {}, **fields})


def test_d21_connection_url_maps_the_standard_extras_or_refuses_them() -> None:
    """D21: `connection_url` forwarded only `rivet_params`, so Postgres `sslmode` (remote PostgreSQL state is
    then refused for want of TLS) and Mongo `srv` / `authSource` (an SRV cluster dialled as a plain host) were
    dropped. Well-known extras become URL parameters; an extra that changes how rivet authenticates and has no URL
    form is refused; `rivet_params` still wins for a key it names."""
    assert connection_url(url_conn(extra_dejson={"sslmode": "require"})) == "postgresql://u:p@h/d?sslmode=require"
    both = url_conn(extra_dejson={"sslmode": "require", "rivet_params": "sslmode=verify-full&application_name=x"})
    assert connection_url(both) == "postgresql://u:p@h/d?sslmode=verify-full&application_name=x"
    mongo = url_conn(conn_type="mongo", host="cluster0.example.net", extra_dejson={"srv": True, "authSource": "admin"})
    assert connection_url(mongo) == "mongodb+srv://u:p@cluster0.example.net/d?authSource=admin"
    for extra in ({"iam": True}, {"srv": True}):
        with pytest.raises(PreflightRefusal) as refused:
            connection_url(url_conn(conn_type="postgres" if "iam" in extra else "mongo", port=27017, extra_dejson=extra))
        assert refused.value.code == "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED"


def test_d23_connection_url_brackets_only_an_ipv6_literal_and_encodes_its_zone() -> None:
    """D23: any host holding `:` was bracketed, so `db.internal:5432` became the invalid `[db.internal:5432]`, and an
    IPv6 zone's `%` was left raw instead of `%25` (RFC 6874)."""
    assert connection_url(url_conn(host="db.internal:5432")) == "postgresql://u:p@db.internal:5432/d"
    assert connection_url(url_conn(host="fe80::1%eth0", port=5432)) == "postgresql://u:p@[fe80::1%25eth0]:5432/d"
    assert connection_url(url_conn(host="[fe80::1%25eth0]")) == "postgresql://u:p@[fe80::1%25eth0]/d"
    assert connection_url(url_conn(host="::1")) == "postgresql://u:p@[::1]/d"
    with pytest.raises(PreflightRefusal) as refused:
        connection_url(url_conn(host="db.internal:5432", port=5432))
    assert refused.value.code == "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED"


# --- Phase-2 mutation hardening: what rivet receives, byte for byte (the mutant ids are in each test's comment) ---


@pytest.mark.parametrize("scheme", ["postgresql", "postgres", "mysql", "sqlserver", "mongodb", "mongodb+srv", "oracle"])
@pytest.mark.parametrize("host", [None, ""])
def test_a_database_connection_without_a_host_is_refused_like_clickhouse(scheme: str, host: object) -> None:
    """Defect `url_host_4`: a database Connection with no host became `postgresql://u:p@/d`, and the driver dialled its
    default socket or host, while the same mistake on a ClickHouse Connection is refused (D22). Every URL that names a
    host is built by `url_host`, so the refusal lives there, for every kind."""
    with pytest.raises(PreflightRefusal) as refused:
        connection_url(url_conn(host=host), scheme)
    assert refused.value.code == "RIVET_AIRFLOW_CONNECTION_INCOMPLETE"
    assert str(refused.value).endswith("connection `c` has no host")


def sub(tmp_path: Path, name: str) -> Path:
    """A fresh private folder per call, as each task gets one."""
    (path := tmp_path / name).mkdir()
    return path


def mongo(**extra: object) -> SimpleNamespace:
    return url_conn(conn_type="mongo", host="m", extra_dejson=extra)


def refusal_of(call: object, *args: object) -> PreflightRefusal:
    with pytest.raises(PreflightRefusal) as refused:
        call(*args)  # type: ignore[operator]
    return refused.value


def test_the_tls_and_endpoint_settings_reach_rivet_exactly(tmp_path: Path) -> None:
    """The settings that decide whether a credential travels encrypted, and to whom, reach rivet byte for byte:
    Mongo `tls` / `allow_insecure` (mutants `_param_3/_4`: `tls=true` written as `tls=false`), ClickHouse `secure`
    (`_clickhouse_22..37`: the password over plain `http://`) and the AWS `endpoint_url` (`_aws_56..59`: the
    Connection's keys sent to real AWS). `build_environment` is the whole environment the operator gives the rivet
    process (`RivetBaseOperator._environment`), so the test compares all of it, the worker's variables included."""
    aws = conn("aws", "aws", login="AKIA1", password="s", extra_dejson={"endpoint_url": "https://minio.internal:9000"})
    ch = conn("ch", "clickhouse", host="ch", port=8443, login="l", password="pw", extra_dejson={"secure": "yes"})
    spec = EnvSpec(urls={"RIVET_MONGO_URL": (mongo(tls=True, allow_insecure=False), None)})
    worker = {"PATH": "/w", "AWS_SESSION_TOKEN": "worker-token", "AIRFLOW__CORE__FERNET_KEY": "f"}
    assert build_environment(worker, spec, [aws, ch], tmp_path) == {
        "PATH": "/w",
        "AWS_ACCESS_KEY_ID": "AKIA1",
        "AWS_SECRET_ACCESS_KEY": "s",
        "AWS_ENDPOINT_URL": "https://minio.internal:9000",
        "RIVET_CLICKHOUSE_URL": "https://ch:8443",
        "RIVET_CLICKHOUSE_USER": "l",
        "RIVET_CLICKHOUSE_PASSWORD": "pw",
        "RIVET_MONGO_URL": "mongodb://u:p@m/d?tls=true&tlsAllowInvalidCertificates=false",
    }


def test_mongo_booleans_are_written_as_the_url_words_they_mean() -> None:
    """`_param`: a boolean extra is `true` / `false`, never Python's `True` or the opposite word; any other value is
    percent-encoded whole, `/` included. Mutants `_param_1, _3.._8, _12`."""
    assert connection_url(mongo(tls=True, allow_insecure=False)) == "mongodb://u:p@m/d?tls=true&tlsAllowInvalidCertificates=false"
    assert connection_url(mongo(tls=False, allow_insecure=True)) == "mongodb://u:p@m/d?tls=false&tlsAllowInvalidCertificates=true"
    assert connection_url(mongo(ssl=True)) == "mongodb://u:p@m/d?tls=true"
    assert (
        connection_url(mongo(appName="etl/a b&c=d", replicaSet=0)) == "mongodb://u:p@m/d?replicaSet=0&appName=etl%2Fa%20b%26c%3Dd"
    )


@pytest.mark.parametrize("secure", ["1", "true", "TRUE", "yes", "Yes", True, 1])
def test_a_secure_clickhouse_connection_is_an_https_endpoint(tmp_path: Path, secure: object) -> None:
    """Every spelling of `secure` Airflow's ClickHouse Connections use gives `https://`, under either extra form;
    without a login rivet's user is `default`. Mutants `_clickhouse_12, _18, _19, _22, _25, _26, _53, _54`."""
    for i, extra in enumerate(({"secure": secure}, {"extra__clickhouse__secure": secure})):
        env = cloud_environment([conn("ch", "clickhouse", host="ch", password="pw", extra_dejson=extra)], sub(tmp_path, str(i)))
        assert env == {
            "RIVET_CLICKHOUSE_URL": "https://ch",
            "RIVET_CLICKHOUSE_USER": "default",
            "RIVET_CLICKHOUSE_PASSWORD": "pw",
        }


@pytest.mark.parametrize("secure", ["0", "false", "no", "", None, False, "XXXX"])
def test_a_clickhouse_connection_that_is_not_secure_is_plain_http(tmp_path: Path, secure: object) -> None:
    env = cloud_environment([conn("ch", "clickhouse", host="ch", login="l", extra_dejson={"secure": secure})], tmp_path)
    assert env == {"RIVET_CLICKHOUSE_URL": "http://ch", "RIVET_CLICKHOUSE_USER": "l"}


def test_the_clickhouse_protocol_extra_names_the_scheme(tmp_path: Path) -> None:
    """`protocol` wins over `secure`, under either extra form. Mutants `_clickhouse_30..39`."""
    cases = [
        ({"protocol": "https"}, "https://ch"),
        ({"extra__clickhouse__protocol": "https"}, "https://ch"),
        ({"protocol": "http", "secure": True}, "http://ch"),
    ]
    for i, (extra, url) in enumerate(cases):
        env = cloud_environment([conn("ch", "clickhouse", host="ch", extra_dejson=extra)], sub(tmp_path, str(i)))
        assert env["RIVET_CLICKHOUSE_URL"] == url, extra


def test_every_aws_setting_reaches_its_variable_under_either_extra_form(tmp_path: Path) -> None:
    """The session token, region and endpoint, by their plain names or the `extra__aws__` form: nothing dropped, nothing
    renamed. Mutants `_aws_48..51, 56..59, 63, 67, 68`."""
    want = {
        "AWS_ACCESS_KEY_ID": "AKIA1",
        "AWS_SECRET_ACCESS_KEY": "s",
        "AWS_SESSION_TOKEN": "tok",
        "AWS_REGION": "eu-north-1",
        "AWS_ENDPOINT_URL": "https://minio.internal:9000",
    }
    plain = {"aws_session_token": "tok", "region_name": "eu-north-1", "endpoint_url": "https://minio.internal:9000"}
    for i, extra in enumerate((plain, {f"extra__aws__{k}": v for k, v in plain.items()})):
        assert (
            cloud_environment([conn("aws", "aws", login="AKIA1", password="s", extra_dejson=extra)], sub(tmp_path, str(i)))
            == want
        )


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (
            {"role_arn": "arn:aws:iam::1:role/r"},
            "AWS connection `aws` sets `role_arn`, which rivet cannot use: rivet assumes no role: store the role's session "
            "keys in login/password and extra `aws_session_token`, or run the worker as the role with an empty login and "
            "`cloud_credentials=['aws']`",
        ),
        (
            {"session_kwargs": {"profile_name": "prod"}},
            "AWS connection `aws` sets `session_kwargs.profile_name`, which rivet cannot use: rivet reads no AWS profile: "
            "put the keys in the Connection's login and password",
        ),
        (
            {"extra__aws__session_kwargs": {"aws_session_token": "t"}},
            "AWS connection `aws` sets `session_kwargs.aws_session_token`, which rivet cannot use: rivet reads no session_kwargs",
        ),
    ],
)
def test_an_aws_refusal_names_the_setting_and_its_remedy(tmp_path: Path, extra: dict, message: str) -> None:
    """Mutants `_aws_4, _10, _11, _28, _31..40`."""
    refused = refusal_of(cloud_environment, [conn("aws", "aws", login="AKIA1", extra_dejson=extra)], tmp_path)
    assert (refused.code, str(refused)) == (
        "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED",
        f"[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] {message}",
    )


def test_gcp_and_azure_settings_reach_rivet_under_either_extra_form(tmp_path: Path) -> None:
    """A `keyfile_dict` given as a mapping is written as its JSON; the `extra__<kind>__` forms are read; an Azure
    password is the account key. Mutants `_gcp_5, 11, 12, 26, 27, 44, 50, 51`, `_azure_5..7, 11, 17, 18`."""
    key = json.loads(KEY)
    gcp = conn("g", "google_cloud_platform", extra_dejson={"keyfile_dict": key, "extra__google_cloud_platform__project": "p1"})
    env = cloud_environment([gcp], sub(tmp_path, "a"))
    assert json.loads(Path(env.pop("GOOGLE_APPLICATION_CREDENTIALS")).read_text()) == key and env == {
        "GOOGLE_CLOUD_PROJECT": "p1"
    }
    old = conn("g", "google_cloud_platform", extra_dejson={"extra__google_cloud_platform__keyfile_dict": KEY})
    assert Path(cloud_environment([old], sub(tmp_path, "b"))["GOOGLE_APPLICATION_CREDENTIALS"]).read_text() == KEY
    azure = conn("az", "wasb", login="acct", password="acct-key", extra_dejson={"extra__wasb__sas_token": "sv=1"})
    assert cloud_environment([azure], sub(tmp_path, "c")) == {
        "AZURE_STORAGE_ACCOUNT": "acct",
        "AZURE_STORAGE_KEY": "acct-key",
        "AZURE_STORAGE_SAS_TOKEN": "sv=1",
    }


def test_a_snowflake_key_is_accepted_in_every_form_snowflakes_hook_takes(tmp_path: Path) -> None:
    """PEM with leading blank lines; base64 wrapped over lines (`base64` wraps at 76) of PEM that itself starts with a
    blank line. Base64 holding a character outside its alphabet is refused, by a message that never shows the value.
    Mutants `_pem_2, 8, 10, 12, 13, 20, 24`."""
    cases = {"pem": "\n  " + pem_key(), "wrapped": base64.encodebytes(("\n" + pem_key() * 3).encode()).decode()}
    for name, text in cases.items():
        sf = conn("sf", "snowflake", login="L", extra_dejson={"account": "a", "private_key_content": text})
        written = Path(cloud_environment([sf], sub(tmp_path, name))["RIVET_SNOWFLAKE_KEY"]).read_text()
        assert written == ("\n  " + pem_key() if name == "pem" else "\n" + pem_key() * 3)
    bad = base64.b64encode(pem_key().encode()).decode()
    bad = bad[:8] + "!" + bad[8:]
    sf = conn("sf", "snowflake", login="L", extra_dejson={"account": "a", "private_key_content": bad})
    refused = refusal_of(cloud_environment, [sf], sub(tmp_path, "bad"))
    assert str(refused) == (
        "[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] Snowflake connection `sf`: extra `private_key_content` is neither a PEM "
        "key nor base64 of one"
    )


def test_every_snowflake_extra_is_read_under_either_extra_form(tmp_path: Path) -> None:
    """`account`, the connection keys, `authenticator`, `private_key_content` and `private_key_file`, plain or
    `extra__snowflake__`; a `load:` that is no mapping names nothing; no login is an empty user. Mutants
    `_snowflake_4, 20, 26, 27, 32, 48, 49, 53, 57, 58, 65, 71..83`."""
    plain = {"account": "ab1", "role": "R", "authenticator": "externalbrowser"}
    for i, extra in enumerate((plain, {f"extra__snowflake__{k}": v for k, v in plain.items()})):
        folder = sub(tmp_path, str(i))
        env = cloud_environment([conn("sf", "snowflake", extra_dejson=extra)], folder, {"load": "not a mapping"})
        assert env == {"SNOWFLAKE_HOME": str(folder / "snowflake")}, "the private SNOWFLAKE_HOME's place is the layout"
        assert sf_toml(env) == {
            "connections": {"sf": {"account": "ab1", "user": "", "role": "R", "authenticator": "externalbrowser"}}
        }
    for i, extra in enumerate(({"private_key_file": "/keys/k.p8"}, {"extra__snowflake__private_key_file": "/keys/k.p8"})):
        sf = conn("sf", "snowflake", login="L", password="pass", extra_dejson={"account": "ab1", **extra})
        env = cloud_environment([sf], sub(tmp_path, f"f{i}"))
        assert sf_toml(env)["connections"]["sf"] == {
            "account": "ab1",
            "user": "L",
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": "/keys/k.p8",
        }
        assert (env["RIVET_SNOWFLAKE_KEY"], env["PRIVATE_KEY_PASSPHRASE"]) == ("/keys/k.p8", "pass")
    sf = conn("sf", "snowflake", login="L", extra_dejson={"account": "ab1", "extra__snowflake__private_key_content": pem_key()})
    assert Path(cloud_environment([sf], sub(tmp_path, "c"))["RIVET_SNOWFLAKE_KEY"]).read_text() == pem_key()
    refused = refusal_of(cloud_environment, [conn("sf2", "snowflake", login="x")], sub(tmp_path, "x"))
    assert (refused.code, str(refused)) == (
        "RIVET_AIRFLOW_CONNECTION_INCOMPLETE",
        "[RIVET_AIRFLOW_CONNECTION_INCOMPLETE] Snowflake connection `sf2` has no `account` extra",
    )


def test_a_lone_surrogate_is_refused_naming_the_field_never_its_value(tmp_path: Path) -> None:
    """Mutants `toml_string_2..5`, `_snowflake_111, 115, 122`."""
    refused = refusal_of(toml_string, "a\ud800b", "the field")
    assert (refused.code, str(refused)) == (
        "RIVET_AIRFLOW_CONNECTION_UNSUPPORTED",
        "[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] the field holds a lone surrogate, which a TOML file cannot carry",
    )
    sf = conn("sf", "snowflake", login="L", extra_dejson={"account": "a", "role": "r\udc00"})
    assert str(refusal_of(cloud_environment, [sf], sub(tmp_path, "a"))).endswith(
        "] Snowflake connection `sf`: field `role` holds a lone surrogate, which a TOML file cannot carry"
    )
    sf = conn("sf", "snowflake", login="L", extra_dejson={"account": "a"})
    assert str(refusal_of(cloud_environment, [sf], sub(tmp_path, "b"), {"load": {"connection": "n\ud800"}})).endswith(
        "] Snowflake connection `sf`: the connection name holds a lone surrogate, which a TOML file cannot carry"
    )


def test_a_key_file_is_utf8_whatever_the_workers_locale(tmp_path: Path) -> None:
    """`_secret_file` writes the text's UTF-8 bytes (a worker in the C locale would otherwise write ASCII and fail on
    a non-ASCII password in `config.toml`), mode 0600. Baseline mutants `_secret_file_15,
    18, 22` (the encoding argument) no longer exist: the file is written as bytes."""
    path = connections._secret_file(tmp_path, "k", "é€\U0001f600")
    assert path == str(tmp_path / "k") and mode(path) == 0o600
    assert Path(path).read_bytes() == "é€\U0001f600".encode()


def test_a_kind_is_read_from_either_extra_form_and_a_refusal_names_both_connections(tmp_path: Path) -> None:
    """Mutants `kind_of_5, 11, 12, 16`, `_cloud_8, 13`, `_extra_7, 15`."""
    tagged = SimpleNamespace(conn_id="h", conn_type="http", host="ch", port=None, login=None, password=None)
    tagged.extra_dejson = {"extra__rivet__rivet_kind": "clickhouse"}
    assert cloud_environment([tagged], tmp_path)["RIVET_CLICKHOUSE_URL"] == "http://ch"
    blank_plain = conn("ch", "clickhouse", host="ch", extra_dejson={"secure": "", "extra__clickhouse__secure": "1"})
    assert cloud_environment([blank_plain], tmp_path)["RIVET_CLICKHOUSE_URL"] == "https://ch", (
        "an empty plain extra hides the old form"
    )
    bare = SimpleNamespace(conn_id="h", conn_type="http")  # no extra_dejson at all
    refused = refusal_of(cloud_environment, [bare], tmp_path)
    assert str(refused) == (
        "[RIVET_AIRFLOW_CONNECTION_KIND] connection `h` has conn_type `http`, which names no storage or warehouse rivet "
        "reads; set its extra `rivet_kind` to one of ['aws', 'azure', 'clickhouse', 'gcp', 'snowflake']"
    )
    refused = refusal_of(cloud_environment, [conn("a", "aws", login="1"), conn("b", "aws", login="2")], tmp_path)
    assert (
        str(refused)
        == "[RIVET_AIRFLOW_CONNECTION_KIND_TWICE] connections `a` and `b` are both aws: one task reads one aws credential"
    )


def test_url_host_refusals_name_the_connection_and_zones_are_encoded_whole() -> None:
    """Mutants `url_host_6, 26..39, 44, 49`: the refusal text (with `?` for an object without a conn_id); a zone is
    percent-encoded whole (`/` too); an unbracketed zone that starts with `25` is a zone, not an escape; a host with
    one bracket is kept as written, never read as another address."""
    text = "has host `db.internal:5432`, which holds a `:` but is no IPv6 address, and a port as well; put the port in the port field only"
    refused = refusal_of(url_host, url_conn(host="db.internal:5432", port=5432))
    assert str(refused) == f"[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] connection `c` {text}"
    anonymous = SimpleNamespace(host="db.internal:5432", port=5432)
    assert str(refusal_of(url_host, anonymous)) == f"[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] connection `?` {text}"
    assert str(refusal_of(url_host, SimpleNamespace(host=None, port=None))).endswith("connection `?` has no host")
    assert url_host(url_conn(host="fe80::1%a/b")) == "[fe80::1%25a%2Fb]"
    assert url_host(url_conn(host="fe80::1%25")) == "[fe80::1%2525]", "interface 25"
    assert url_host(url_conn(host="[fe80::1%2525]")) == "[fe80::1%2525]"
    assert url_host(url_conn(host="fe80::1]")) == "fe80::1]"


def test_connection_url_messages_and_the_extras_it_reads() -> None:
    """Mutants `connection_url_7, 10..19, 23..25, 32, 43, 46, 62..64, 73, 80, 107, 114, 127, 131, 132, 142, 146, 153`."""
    anonymous = SimpleNamespace(conn_type="generic", login="u", password="p", host="h", port=None, schema="d")
    assert str(refusal_of(connection_url, anonymous)) == (
        "[RIVET_AIRFLOW_CONNECTION_SCHEME] connection `?` has conn_type `generic`, which names no rivet URL scheme; pass "
        "the scheme as (conn_id, scheme) or set the connection extra `rivet_scheme` to one of ('postgresql', 'postgres', "
        "'mysql', 'sqlserver', 'mongodb', 'mongodb+srv', 'oracle')"
    )
    assert connection_url(url_conn(conn_type="generic", extra_dejson={"rivet_scheme": "mysql"})) == "mysql://u:p@h/d"
    assert str(refusal_of(connection_url, url_conn(extra_dejson={"iam": True}))) == (
        "[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] connection `c` sets extra `iam` (IAM token authentication), which a rivet URL cannot carry"
    )
    assert str(refusal_of(connection_url, url_conn(conn_type="mongo", port=1, extra_dejson={"srv": "yes"}))) == (
        "[RIVET_AIRFLOW_CONNECTION_UNSUPPORTED] connection `c` is an SRV (`mongodb+srv`) connection with a port; clear the port"
    )
    for srv in ("1", "yes", "YES", True):
        assert connection_url(mongo(srv=srv)) == "mongodb+srv://u:p@m/d", srv
    assert connection_url(url_conn(extra_dejson={"srv": True})) == "postgresql://u:p@h/d", "srv is Mongo's alone"
    assert connection_url(url_conn(extra_dejson={"service_name": "S"})) == "postgresql://u:p@h/d", "service_name is Oracle's"
    assert connection_url(url_conn(login="a/b", password="c/d", schema="e/f")) == "postgresql://a%2Fb:c%2Fd@h/e%2Ff"
    assert connection_url(url_conn(schema=None)) == "postgresql://u:p@h/"
    assert connection_url(url_conn(extra_dejson={"sslmode": "", "connect_timeout": None})) == "postgresql://u:p@h/d"
    two = {"sslmode": "require", "application_name": "a", "connect_timeout": 5}
    assert (
        connection_url(url_conn(extra_dejson=two)) == "postgresql://u:p@h/d?sslmode=require&application_name=a&connect_timeout=5"
    )
    own = {**two, "rivet_params": "application_name=x=y&connect_timeout=9"}
    assert (
        connection_url(url_conn(extra_dejson=own))
        == "postgresql://u:p@h/d?sslmode=require&application_name=x=y&connect_timeout=9"
    )


def test_build_environment_reads_the_worker_environment_it_is_given(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """`build_environment(worker_env, ...)` reads the mapping it is handed, not `os.environ`, both for what it passes
    and for the PostgreSQL-state guard, whose refusal names both remedies. Mutants `build_environment_4, 7, 37, 39, 46, 47`."""
    monkeypatch.delenv("PASS_ME", raising=False)
    worker = {"PATH": "/w", "PASS_ME": "x"}
    assert build_environment(worker, EnvSpec(passthrough=["PASS_ME"]), [], tmp_path)["PASS_ME"] == "x"
    refused = refusal_of(build_environment, {"RIVET_STATE_URL": "postgresql://s/state"}, EnvSpec(), [], tmp_path)
    assert str(refused) == (
        "[RIVET_AIRFLOW_STATE_ENV_NOT_PASSED] the worker's environment sets RIVET_STATE_URL to a PostgreSQL state and "
        "this task would not pass it to rivet, which would then run on an empty SQLite state and treat every export as "
        'a first run. Either inherit the worker\'s value with env_passthrough=["RIVET_STATE_URL"], or give the task its '
        'own with env_from_connections={"RIVET_STATE_URL": "<conn_id>"} (or `env`); unset it on the worker if it is not '
        "meant for this pipeline"
    )
