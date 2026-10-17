"""The stand's own secrets, and the Connection legs: Secret Manager as the secrets backend, a keyless gate_gcp.

The stand is read the way docker compose reads it (the official compose file, then the gate overlay, then the env
file), so a password or key is found wherever a stand puts it; every grader passes on a faithful stack and fails on
each mutant of it.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from test_stand_hygiene import compose_view, laid_out

from gate import __main__ as gate_main
from gate import conns, warehouse
from gate.stand import ADC_IN_CONTAINER, FLAVORS, Stand
from gate.warehouse import SECRET_SHAPE, TAG_SHAPE, Cloud

TAG = "1016120000ab"
CLOUD = Cloud(Path("/nonexistent/adc.json"), "bucket", "proj", "US", "rivet", "WH", "DB", "INT", TAG)
GSM = Cloud(
    Path("/nonexistent/adc.json"),
    "bucket",
    "proj",
    "US",
    "rivet",
    "WH",
    "DB",
    "INT",
    TAG,
    gcp_auth="adc",
    connections_from="gsm",
    gsm_reader="reader@proj.iam.gserviceaccount.com",
)


# --- Where the Connections live -------------------------------------------------------------------------------------


def test_env_leg_puts_every_connection_in_a_0600_env_file(tmp_path: Path) -> None:
    stand = laid_out(tmp_path)
    env_file = stand.dir / "gate-connections.env"
    names = {line.split("=", 1)[0] for line in env_file.read_text().splitlines()}
    assert names == {"AIRFLOW_CONN_RIVET_GATE_SOURCE", "AIRFLOW_CONN_RIVET_GATE_STATE"}
    assert env_file.stat().st_mode & 0o777 == 0o600


def test_gsm_leg_sets_the_backend_and_no_connection_variable(tmp_path: Path) -> None:
    adc = tmp_path / "adc.json"
    adc.write_text("{}")
    stand = laid_out(tmp_path, secrets_backend=GSM.secrets_backend("3.3.2"), adc=adc)
    stand.extra_connections = {"gate_gcp": {"conn_type": "google_cloud_platform", "extra": {"project": "proj"}}}
    stand.lay_out()
    view = compose_view(stand)
    for service in FLAVORS["3.3.2"].services:
        env = view[service]["environment"]
        assert not [k for k in env if k.startswith("AIRFLOW_CONN_")], service
        assert env["AIRFLOW__SECRETS__BACKEND"] == warehouse.SECRETS_BACKEND
        assert f"{adc}:{ADC_IN_CONTAINER}:ro" in view[service]["volumes"]
    assert (stand.dir / "gate-connections.env").read_text() == ""


def test_the_backend_asks_for_exactly_the_secrets_the_gate_publishes(monkeypatch, tmp_path: Path) -> None:
    """Airflow's backend asks for `<connections_prefix><sep><conn_id>` (BaseSecretsBackend.build_path); every one of
    them must be a secret the gate made, of the shape the sweep and the reader's IAM condition recognise."""
    calls = []
    monkeypatch.setattr(warehouse, "sh", lambda args, **kw: calls.append((args, kw)) or subprocess.CompletedProcess(args, 0))
    stand = Stand("2.10.5", tmp_path / "rivet", tmp_path, tag=TAG)
    made = warehouse.publish_connections(GSM, "2.10.5", stand.connections())
    kwargs = json.loads(GSM.secrets_backend("2.10.5")["AIRFLOW__SECRETS__BACKEND_KWARGS"])
    asked = {f"{kwargs['connections_prefix']}{kwargs['sep']}{c}" for c in stand.connections()}
    assert asked == set(made)
    assert all(SECRET_SHAPE.fullmatch(m) and TAG_SHAPE.search(m) for m in made)
    assert all(m.startswith("airflow-connections-afgate_") for m in made)  # the reader's IAM condition
    assert kwargs["variables_prefix"] is None and kwargs["config_prefix"] is None
    assert kwargs["impersonation_chain"] == GSM.gsm_reader
    for args, _kw in calls:  # the value goes in on stdin, never on a command line
        assert stand.secrets.source not in " ".join(args)
    assert any(stand.secrets.source in (kw.get("input") or "") for _, kw in calls)


def test_keyless_gcp_connection_holds_only_the_project(monkeypatch, tmp_path: Path) -> None:
    key = tmp_path / "sf.p8"
    key.write_text("PEM")
    login = {"user": "U", "account": "A", "private_key_file": str(key)}
    monkeypatch.setattr(warehouse, "snow_login", lambda name: login)
    adc = tmp_path / "adc.json"
    adc.write_text('{"type": "authorized_user"}')
    keyless = warehouse.connections(Cloud(adc, "b", "proj", "US", "r", "W", "D", "I", TAG, gcp_auth="adc"), "pw")
    keyed = warehouse.connections(Cloud(adc, "b", "proj", "US", "r", "W", "D", "I", TAG), "pw")
    assert keyless["gate_gcp"]["extra"] == {"project": "proj"}
    assert keyed["gate_gcp"]["extra"]["keyfile_dict"] == adc.read_text()


def test_gsm_and_adc_need_the_warehouse_leg(capsys) -> None:
    with pytest.raises(SystemExit):
        gate_main.main(["--rivet-bin", "/x", "--connections", "gsm"])
    assert "--warehouses" in capsys.readouterr().err


# --- Cleanup and sweep of the secrets -------------------------------------------------------------------------------


class Secrets:
    """Fake `gcloud secrets`: list answers what is left, delete removes it (or not)."""

    def __init__(self, ids: dict[str, str], sticky: set[str] = frozenset()) -> None:
        self.ids = dict(ids)
        self.sticky = sticky
        self.deleted: list[str] = []

    def sh(self, args, **kw):
        if args[:3] == ["gcloud", "secrets", "list"]:
            rows = [{"name": f"projects/1/secrets/{i}", "createTime": t} for i, t in self.ids.items()]
            return subprocess.CompletedProcess(args, 0, json.dumps(rows), "")
        if args[:3] == ["gcloud", "secrets", "delete"]:
            self.deleted.append(args[3])
            if args[3] not in self.sticky:
                self.ids.pop(args[3], None)
        return subprocess.CompletedProcess(args, 0, "[]", "")


def test_cleanup_deletes_the_stack_secrets_and_lists_again(monkeypatch) -> None:
    other = GSM.secret("2.10.5", "gate_gcp")
    fake = Secrets({GSM.secret("3.3.2", "gate_gcp"): "2026-10-16T00:00:00Z", other: "2026-10-16T00:00:00Z"})
    monkeypatch.setattr(warehouse, "sh", fake.sh)
    warehouse.delete_connections(GSM, "3.3.2")
    assert fake.deleted == [GSM.secret("3.3.2", "gate_gcp")] and other in fake.ids


def test_cleanup_raises_when_a_secret_survives(monkeypatch) -> None:
    mine = GSM.secret("3.3.2", "rivet_gate_source")
    monkeypatch.setattr(warehouse, "sh", Secrets({mine: "2026-10-16T00:00:00Z"}, sticky={mine}).sh)
    with pytest.raises(RuntimeError, match="left behind"):
        warehouse.delete_connections(GSM, "3.3.2")


def test_sweep_deletes_only_old_secrets_of_the_gate_shape(monkeypatch) -> None:
    old, new = "2026-10-01T00:00:00Z", "2999-01-01T00:00:00Z"
    fake = Secrets(
        {
            "airflow-connections-afgate_10010000beef_332_gate_gcp": old,
            "airflow-connections-afgate_10160000beef_2105_gate_gcp": new,
            "airflow-connections-prod_db": old,
            "airflow-connections-afgate_someone_gate_gcp": old,
        }
    )
    monkeypatch.setattr(warehouse, "sh", fake.sh)
    monkeypatch.setattr(warehouse, "_bq", lambda *a, **k: "[]")
    monkeypatch.setattr(warehouse, "_snow", lambda *a, **k: [])
    removed = warehouse.sweep(GSM)
    assert fake.deleted == ["airflow-connections-afgate_10010000beef_332_gate_gcp"]
    assert removed == ["gsm:airflow-connections-afgate_10010000beef_332_gate_gcp"]


# --- The proof cells: pass on a faithful stack, fail on each mutant -------------------------------------------------

PUBLISHED = {"conn_type": "clickhouse", "host": "gate-clickhouse", "port": 8123, "login": "loader", "password": "x"}
SECRET = GSM.secret("3.3.2", "gate_clickhouse")
CLEAN_ENVS = {"airflow-worker": ["PATH=/usr/bin", "AIRFLOW__SECRETS__BACKEND=x"], "airflow-scheduler": ["HOME=/h"]}


def gsm_problems(**over) -> list[str]:
    args = {
        "conn_id": "gate_clickhouse",
        "published": PUBLISHED,
        "container_envs": CLEAN_ENVS,
        "metastore_rows": 0,
        "listed": [SECRET],
        "secret": SECRET,
        "resolved": {**PUBLISHED, "port": 8123},
    } | over
    return conns.from_secret_manager(**args)


def test_from_secret_manager_passes_on_a_faithful_stack() -> None:
    assert gsm_problems() == []


@pytest.mark.parametrize(
    "mutant",
    [
        {"container_envs": {**CLEAN_ENVS, "airflow-worker": ["AIRFLOW_CONN_GATE_CLICKHOUSE={}"]}},
        {"container_envs": {}},
        {"container_envs": {**CLEAN_ENVS, "airflow-triggerer": None}},
        {"metastore_rows": 1},
        {"listed": []},
        {"resolved": None},
        {"resolved": {**PUBLISHED, "host": "elsewhere"}},
        {"resolved": {**PUBLISHED, "login": "default"}},
    ],
)
def test_from_secret_manager_fails_on_each_mutant(mutant: dict) -> None:
    assert gsm_problems(**mutant)


KEYLESS = {"conn_type": "google_cloud_platform", "extra_dejson": {"project": "proj"}}


def test_keyless_passes_on_a_project_only_connection() -> None:
    assert conns.keyless(KEYLESS, CLEAN_ENVS, adc_writable=False) == []


@pytest.mark.parametrize(
    "resolved, envs, writable",
    [
        ({"extra_dejson": {"project": "proj", "keyfile_dict": "{}"}}, CLEAN_ENVS, False),
        ({"extra_dejson": {"project": "proj", "extra__google_cloud_platform__key_path": "/k"}}, CLEAN_ENVS, False),
        ({"extra_dejson": {}}, CLEAN_ENVS, False),
        (KEYLESS, {"airflow-worker": ["GOOGLE_APPLICATION_CREDENTIALS=/k.json"]}, False),
        (KEYLESS, CLEAN_ENVS, True),
        (None, CLEAN_ENVS, False),
    ],
)
def test_keyless_fails_on_each_mutant(resolved, envs, writable) -> None:
    assert conns.keyless(resolved, envs, adc_writable=writable)


def test_log_hygiene_names_masked_words_and_the_empty_key_warning() -> None:
    clean = {"a.log": '{"event": "rivet.start", "argv": ["/usr/local/bin/rivet"]}'}
    assert conns.log_hygiene(clean, conns.MASK) == []
    assert conns.log_hygiene({**clean, "b.log": "WARNING - /home/***/.local"}, conns.MASK)
    assert conns.log_hygiene({"s": "empty cryptography key - values will not be stored encrypted."}, conns.EMPTY_FERNET)


def test_parse_connection_reads_the_cli_json_after_warnings() -> None:
    out = 'WARNING something\n[{"conn_id": "c", "host": "h", "extra_dejson": {}}]\n'
    assert conns.parse_connection(out) == {"conn_id": "c", "host": "h", "extra_dejson": {}}
    assert conns.parse_connection("") is None
    assert conns.parse_connection("[]") is None


def test_parse_connection_skips_a_log_line_that_holds_a_bracket() -> None:
    """Airflow 3.3.2 logs a structlog line before the JSON; its `[info ]` was read as the JSON's start, so every
    connection resolved from Secret Manager read as "resolved nothing" on the live gate."""
    log = "2026-10-17 07:45:07 [info     ] Getting connection using `google.auth.default()` since no explicit credentials\n"
    row = {"conn_id": "c", "host": "h"}
    assert conns.parse_connection(log + json.dumps([row]) + "\n") == row
    assert conns.parse_connection(log + json.dumps([row], indent=4) + "\n") == row
