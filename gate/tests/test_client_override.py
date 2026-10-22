"""The client's own compose files run in the gate (`--client-override`): the stack, the image's wheels, and the cells.

Each test names the mutant it is RED on.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest
import yaml
from test_stand_hygiene import Loader

from gate import __main__ as gate_main
from gate import client, conns, sources
from gate.sources import MYSQL
from gate.stand import (
    CLIENT_FILES,
    CLIENT_STATE_DB,
    GCP_KEY_IN_CONTAINER,
    Stand,
    elf_platform,
    rivet_cli_wheel,
    rivet_version,
)

ELF_AARCH64 = b"\x7fELF\x02\x01\x01" + b"\x00" * 11 + (0xB7).to_bytes(2, "little") + b"\x00" * 40
ELF_X86 = b"\x7fELF\x02\x01\x01" + b"\x00" * 11 + (0x3E).to_bytes(2, "little") + b"\x00" * 40


def test_the_rivet_cli_wheel_is_laid_out_as_rivets_own(tmp_path: Path) -> None:
    binary = tmp_path / "rivet"
    binary.write_bytes(ELF_AARCH64)
    wheel = rivet_cli_wheel(binary, "0.32.2", tmp_path)
    # Mutant: the platform tag of the build host, or the wrong machine table entry.
    assert wheel.name == "rivet_cli-0.32.2-py3-none-manylinux_2_28_aarch64.whl"
    with zipfile.ZipFile(wheel) as z:
        names = set(z.namelist())
        script = z.getinfo("rivet_cli-0.32.2.data/scripts/rivet")
        # Mutant: the script not executable (pip would install a `rivet` the operator cannot run).
        assert (script.external_attr >> 16) & 0o111
        assert z.read("rivet_cli-0.32.2.data/scripts/rivet") == ELF_AARCH64
        record = z.read("rivet_cli-0.32.2.dist-info/RECORD").decode()
        assert "Tag: py3-none-manylinux_2_28_aarch64" in z.read("rivet_cli-0.32.2.dist-info/WHEEL").decode()
    assert {"rivet_cli/__init__.py", "rivet_cli-0.32.2.dist-info/METADATA"} <= names
    assert all(n in record for n in names), "every file is in RECORD"
    assert elf_platform(ELF_X86) == "manylinux_2_28_x86_64"
    with pytest.raises(ValueError, match="not an ELF"):
        elf_platform(b"\xcf\xfa\xed\xfe" + b"\x00" * 60)
    assert rivet_version("rivet 0.32.2 (7fba31c4a)\n") == "0.32.2"


def client_dir(tmp_path: Path) -> Path:
    d = tmp_path / "delivery"
    d.mkdir()
    for name in CLIENT_FILES:
        (d / name).write_text(f"# {name}\n")
    return d


def client_stand(tmp_path: Path) -> Stand:
    return Stand("2.10.2", Path("/bin/true"), tmp_path / "w", sources=(MYSQL,), client_override=client_dir(tmp_path))


def test_the_clients_state_is_left_to_its_readme_stream(tmp_path: Path) -> None:
    stand = client_stand(tmp_path)
    assert stand.state_place == "client-override"
    # The gate's own configs keep a database and a state_dir of their own: `rivet state copy` needs the client's
    # `rivet_state` empty, and its README stream takes `./rivet-state/mysql`. Mutant: the leg's stream on the client's.
    assert stand.leg_db("mysql", "cdc") == "rivet_state_mysql_cdc" != CLIENT_STATE_DB
    assert stand.leg_state_sub("mysql", "cdc") == "mysql_cdc"
    assert stand.source_state_root == "/opt/airflow/rivet-state" and stand.source_state_host == stand.dir / "rivet-state"
    conns_ = stand.connections()
    # The client's own Connections (rivet_state, rivet_mysql) come from its compose file, never from the gate.
    assert "rivet_state" not in conns_ and "rivet_mysql" not in conns_
    cdc = conns_["rivet_gate_state_mysql_cdc"]
    assert (cdc["host"], cdc["login"], cdc["extra"]) == ("rivet-state", "rivet", {"rivet_params": "sslmode=disable"})
    assert stand.leg_state_url("rivet_state").endswith("@rivet-state:5432/rivet_state?sslmode=disable")


def test_the_compose_files_are_official_then_the_clients_then_the_gates(tmp_path: Path, monkeypatch) -> None:
    stand = client_stand(tmp_path)
    seen = {}

    def sh(args, env=None, **kw):
        seen["args"], seen["env"] = args, env

    monkeypatch.setattr("gate.stand.sh", sh)
    stand.compose("ps")
    files = [a for a, b in zip(seen["args"], seen["args"][1:], strict=False) if a == "-f" and b] and [
        Path(seen["args"][i + 1]).name for i, a in enumerate(seen["args"]) if a == "-f"
    ]
    # Mutant: the client's file left out, or layered over the gate's (the gate's adaptations would not win).
    assert files == ["docker-compose.yaml", "docker-compose.rivet.yml", "docker-compose.gate.yaml"]
    env = seen["env"]
    assert (env["RIVET_STATE_USER"], env["RIVET_STATE_DB"], env["RIVET_AIRFLOW_IMAGE"]) == ("rivet", "rivet_state", stand.image)
    assert env["RIVET_STATE_PASSWORD"] == stand.secrets.compose_state


def test_the_gates_overlay_adds_the_key_and_leaves_the_clients_variables(tmp_path: Path, monkeypatch) -> None:
    key = tmp_path / "key.json"
    key.write_text("{}")
    stand = Stand(
        "2.10.2", Path("/bin/true"), tmp_path / "w", sources=(MYSQL,), client_override=client_dir(tmp_path), gcp_key_file=key
    )
    monkeypatch.setattr(Stand, "lay_out_client", lambda self: setattr(self, "client_files", {}))
    stand.lay_out()
    overlay = yaml.load((stand.dir / "docker-compose.gate.yaml").read_text(), Loader=Loader)
    anchor = overlay["x-rivet"]
    # Mutant: the overlay sets GOOGLE_APPLICATION_CREDENTIALS itself (the client's variable would go untested).
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in anchor["environment"]
    assert f"{key}:{GCP_KEY_IN_CONTAINER}:ro" in anchor["volumes"]
    assert anchor["environment"]["GATE_CLIENT_OVERRIDE"] == "1" and anchor["environment"]["GATE_GCP_AUTH"] == "env-file"
    assert "gate_gcp" not in stand.connections()


def test_the_readme_dag_takes_its_step_6_kwargs() -> None:
    dag = (Path(sources.__file__).parent / "stand" / "dags" / "rivet_gate.py").read_text()
    block = dag[dag.index("# The client's README (gate/client.py") :]
    # Mutant: the gate's own Connections or state_dir for the README's stream.
    assert '"env_from_connections": {"RIVET_STATE_URL": "rivet_state", "RIVET_MYSQL_URL": "rivet_mysql"}' in block
    assert '"cloud_credentials": ["gcp"]' in block and 'state_dir=readme["state_dir"]' in block
    assert client.STATE_DIR == "/opt/airflow/rivet-state/mysql"
    assert 'GCP = {"cloud_credentials": ["gcp"]} if ENV_KEY else {"connections": ["gate_gcp"]}' in dag


README_LINE = (
    "`docker compose ... exec airflow-worker bash -c 'RIVET_STATE_URL=\"postgresql://$RIVET_STATE_USER:$RIVET_STATE_PASSWORD"
    "@rivet-state:5432/$RIVET_STATE_DB?sslmode=disable\" rivet state copy -c /opt/airflow/rivet-state/migration-src/<config file>'`"
)


def test_step_5_runs_the_readmes_own_line() -> None:
    copied = "copied 20 table(s), 83 row(s), from `…/.rivet_state.db`"
    assert client.step5_problems(0, copied, README_LINE) == []
    # Mutant: the README not compared (a command the gate rewrote would pass as the README's).
    assert client.step5_problems(0, copied, README_LINE.replace("$RIVET_STATE_DB", "rivet_state")) == [
        "the command run is not the README's step 5 line"
    ]
    assert client.step5_problems(1, "Error: …", README_LINE) == ["exit 1", "no `copied … table(s)` line: Error: …"]


def test_the_stream_continues_from_the_cron_jobs_position() -> None:
    tasks = {"run": {"state": "success"}, "load.gate_orders": {"state": "success"}}
    assert client.continues_problems("success", tasks, {"units": [{"rows": 4}]}, 4) == []
    # Mutant: the rows not compared (a new baseline, the whole table, passes as a continuation).
    assert client.continues_problems("success", tasks, {"units": [{"rows": 6120}]}, 4) == [
        "the run moved 6120 row(s), the changes since cron stopped are 4"
    ]
    refused = {"error": {"code": "RIVET_SOURCE_CDC_CHECKPOINT_INVALID"}, "exit_status": 5}
    assert client.continues_problems("failed", {"run": {"state": "failed"}}, refused, 4)[-1] == (
        "rivet stopped [RIVET_SOURCE_CDC_CHECKPOINT_INVALID] exit 5"
    )


WORKER = "airflow-worker/abc"
CLIENT_CONN = json.dumps(
    {"conn_type": "postgres", "host": "rivet-state", "port": 5432, "login": "rivet", "password": "p", "schema": "rivet_state",
     "extra": {"rivet_params": "sslmode=disable"}}
)  # fmt: skip
OK_TASKS = {"run": {"state": "success"}, "load.gate_orders": {"state": "success"}, "compact.gate_orders": {"state": "skipped"}}


def test_the_key_file_is_the_only_gcp_credential() -> None:
    envs = {WORKER: [f"GOOGLE_APPLICATION_CREDENTIALS={GCP_KEY_IN_CONTAINER}"]}
    assert conns.env_key_file(None, envs, False, OK_TASKS) == []
    # Mutant: a GCP Connection not looked for (a run authenticated by it passes as the key file's).
    assert conns.env_key_file({"host": None}, envs, False, OK_TASKS) == ["a gate_gcp Connection resolves"]
    # Mutant: the variable not read.
    assert conns.env_key_file(None, {WORKER: []}, False, OK_TASKS)[0].endswith(f"is not {GCP_KEY_IN_CONTAINER}")
    assert conns.env_key_file(None, envs, True, OK_TASKS) == ["the key file is writable from the worker"]
    # Mutant: the tasks not read (a stack whose loads failed on the credential passes).
    failed = {**OK_TASKS, "load.gate_orders": {"state": "failed"}}
    assert conns.env_key_file(None, envs, False, failed)[0].startswith("the stream's run and loads did not all succeed")


def test_the_json_env_connection_reaches_rivet_with_sslmode_disable() -> None:
    envs = {WORKER: [f"AIRFLOW_CONN_RIVET_STATE={CLIENT_CONN}"]}
    resolved = {"host": "rivet-state", "extra_dejson": {"rivet_params": "sslmode=disable"}}
    assert conns.env_json_connection(envs, resolved, 3, OK_TASKS) == []
    # Mutant: the extra not read (a Connection without sslmode would be refused by rivet, TLS_REQUIRED).
    bare = json.dumps({**json.loads(CLIENT_CONN), "extra": {}})
    assert conns.env_json_connection({WORKER: [f"AIRFLOW_CONN_RIVET_STATE={bare}"]}, resolved, 3, OK_TASKS)[0].endswith(
        "rivet_params None"
    )
    assert conns.env_json_connection({WORKER: []}, resolved, 3, OK_TASKS) == [f"{WORKER}: no AIRFLOW_CONN_RIVET_STATE"]
    # Mutant: the state rows not counted (the stream wrote its state elsewhere).
    assert conns.env_json_connection(envs, resolved, 0, OK_TASKS) == [
        "no state row of the stream in the client's rivet_state database"
    ]
    assert conns.env_json_connection(envs, None, 3, OK_TASKS) == ["`airflow connections get rivet_state` resolved nothing"]


def test_the_clients_files_ran_as_delivered() -> None:
    assert conns.as_delivered({"a": "1" * 64}, {"a": "1" * 64}) == []
    # Mutant: the hashes not compared.
    assert conns.as_delivered({"a": "1" * 64}, {"a": "2" * 64}) == ["a: 111111111111 copied, 222222222222 at the end"]
    assert conns.as_delivered({}, {}) == ["no client file copied"]


def test_the_checkpoint_lands_where_the_readme_says(tmp_path: Path) -> None:
    mysql = tmp_path / "rivet-state" / "mysql"
    (mysql / "cdc").mkdir(parents=True)
    assert sources.checkpoint_on_host(mysql) == ["no checkpoint at mysql/cdc/*.ckpt; checkpoints under the mount: none"]
    # One level off (the README's earlier `cdc` state_dir): `cdc/cdc/`.
    (tmp_path / "rivet-state" / "cdc" / "cdc").mkdir(parents=True)
    (tmp_path / "rivet-state" / "cdc" / "cdc" / "rivet_cdc.ckpt").write_text("{}")
    assert sources.checkpoint_on_host(mysql) == [
        "no checkpoint at mysql/cdc/*.ckpt; checkpoints under the mount: ['cdc/cdc/rivet_cdc.ckpt']"
    ]
    # Mutant: the glob widened to the whole mount (the one-level-off file passes).
    (mysql / "cdc" / "rivet_cdc.ckpt").write_text("{}")
    assert sources.checkpoint_on_host(mysql) == []


def test_the_client_stack_is_the_clients_airflow(capsys) -> None:
    with pytest.raises(SystemExit):
        gate_main.main(["--rivet-bin", "x", "--client-override", "d", "--airflow", "2.10.5", "--warehouses", "bigquery",
                        "--sources", "mysql"])  # fmt: skip
    assert "--airflow 2.10.2" in capsys.readouterr().err


def test_the_provider_wheel_carries_the_version_the_clients_dockerfile_pins(tmp_path: Path) -> None:
    from gate.stand import CLIENT_PROVIDER_VERSION, provider_wheel

    wheel = provider_wheel(tmp_path, CLIENT_PROVIDER_VERSION)
    # Mutant: the checkout's own version kept (`--find-links` would not find `airflow-provider-rivet==0.2.0`).
    assert wheel.name.startswith(f"airflow_provider_rivet-{CLIENT_PROVIDER_VERSION}-")
    with zipfile.ZipFile(wheel) as z:
        meta = z.read(f"airflow_provider_rivet-{CLIENT_PROVIDER_VERSION}.dist-info/METADATA").decode()
    assert f"Version: {CLIENT_PROVIDER_VERSION}\n" in meta
