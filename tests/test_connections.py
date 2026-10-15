"""Storage and warehouse Connections reach rivet as variables and private key files, and leave nothing behind."""

from __future__ import annotations

import json
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowFailException
from conftest import Rig

from airflow_provider_rivet.operators import RivetLoadOperator, RivetRunOperator
from airflow_provider_rivet.connections import cloud_environment
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
    sf = conn("sf", "snowflake", login="LOADER", extra_dejson={"account": "ab12345", "role": "R", "warehouse": "WH",
                                                                 "private_key_content": "-----BEGIN PRIVATE KEY-----\nK\n"})
    env = cloud_environment([sf], tmp_path, {"load": {"target": "snowflake", "connection": "rivet"}})
    toml = Path(env["SNOWFLAKE_HOME"], "config.toml")
    assert mode(str(toml)) == 0o600 and mode(env["SNOWFLAKE_HOME"]) == 0o700
    body = toml.read_text()
    assert body.startswith('[connections."rivet"]\n') and 'account = "ab12345"' in body and 'authenticator = "SNOWFLAKE_JWT"' in body
    assert Path(env["RIVET_SNOWFLAKE_KEY"]).read_text().startswith("-----BEGIN") and f'private_key_file = "{env["RIVET_SNOWFLAKE_KEY"]}"' in body
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


def test_the_operator_passes_the_key_file_removes_it_afterwards_and_never_shows_it(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
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
