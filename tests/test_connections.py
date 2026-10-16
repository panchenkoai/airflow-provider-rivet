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

from airflow_provider_rivet.connections import cloud_environment
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


@pytest.mark.parametrize("host", [None, ""])
def test_d22_a_connection_without_a_host_is_refused(tmp_path: Path, host: str | None) -> None:
    """D22: an empty ClickHouse host became `http://None`, a DNS failure at load time instead of a refusal before rivet;
    a database Connection without one became `postgresql://u:p@/d`, and the driver dialled its default socket."""
    with pytest.raises(PreflightRefusal) as refused:
        cloud_environment([conn("ch", "clickhouse", host=host, login="u")], tmp_path)
    assert refused.value.code == "RIVET_AIRFLOW_CONNECTION_INCOMPLETE" and "`ch` has no host" in str(refused.value)
    with pytest.raises(PreflightRefusal) as refused:
        connection_url(conn("pg", "postgres", host=host, login="u", password="p", schema="d"))
    assert refused.value.code == "RIVET_AIRFLOW_CONNECTION_INCOMPLETE" and "`pg` has no host" in str(refused.value)
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
