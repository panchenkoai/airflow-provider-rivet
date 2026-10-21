"""The client's state placement (`--state compose-postgres`) and the worker-recreate cell.

Each test names the mutant it is RED on.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from test_stand_hygiene import laid_out

from gate import __main__ as gate_main
from gate import sources
from gate.sources import MYSQL, placement_problems, recreated
from gate.stand import COMPOSE_STATE_ROLE, METASTORE_DB, Stand

ROLE = COMPOSE_STATE_ROLE
OWNERS = {"rivet_state_mysql_batch": ROLE, "rivet_state_mysql_cdc": ROLE}
TABLES = {db: [("export_state", ROLE), ("run_status", ROLE)] for db in OWNERS}
METASTORE = [("dag", "airflow"), ("task_instance", "airflow")]


def test_the_state_is_in_databases_and_tables_of_its_own_role() -> None:
    assert placement_problems(OWNERS, TABLES, METASTORE, ROLE) == []
    # Mutant: the database owner not compared (a database Airflow's superuser owns passes).
    assert placement_problems({**OWNERS, "rivet_state_mysql_cdc": "airflow"}, TABLES, METASTORE, ROLE) == [
        "rivet_state_mysql_cdc is owned by 'airflow', not rivet_state"
    ]
    # Mutant: the table owners not compared.
    mixed = {**TABLES, "rivet_state_mysql_cdc": [("export_state", "airflow")]}
    assert placement_problems(OWNERS, mixed, METASTORE, ROLE) == [
        "rivet_state_mysql_cdc: tables not owned by rivet_state: ['export_state']"
    ]
    # Mutant: the vacuity guard dropped (an empty database passes: rivet may have written elsewhere).
    assert placement_problems(OWNERS, {**TABLES, "rivet_state_mysql_batch": []}, METASTORE, ROLE)[0].endswith("proves nothing")


def test_nothing_of_rivets_may_be_in_airflows_metadata_database() -> None:
    # Mutant: the metastore not read, or only one of the two marks (rivet's names, its role's ownership) checked.
    assert placement_problems(OWNERS, TABLES, [*METASTORE, ("export_state", "airflow")], ROLE) == [
        "Airflow's metadata database holds rivet's tables: ['export_state']"
    ]
    assert placement_problems(OWNERS, TABLES, [*METASTORE, ("other", ROLE)], ROLE) == [
        "Airflow's metadata database holds rivet's tables: ['other']"
    ]


def test_the_worker_must_have_been_replaced() -> None:
    assert recreated(["a" * 64], ["b" * 64]) == []
    # Mutant: `before == after` not checked (a restart that recreated nothing passes).
    assert recreated(["a" * 64], ["a" * 64]) == [f"the worker container {'a' * 12} was not recreated"]
    assert recreated([], ["b"]) == ["worker containers before [], after ['b']: expected one each"]


def test_compose_postgres_state_is_a_role_and_a_database_on_airflows_server(tmp_path: Path) -> None:
    stand = laid_out(tmp_path, sources=(MYSQL,))
    client = Stand("3.3.2", stand.rivet_bin, tmp_path / "c", sources=(MYSQL,), state_place="compose-postgres")
    conn = client.connections()["rivet_gate_state_mysql_cdc"]
    # Mutant: the client mode left on the gate's own state server, or the gate's role.
    assert (conn["host"], conn["login"], conn["schema"]) == ("postgres", ROLE, "rivet_state_mysql_cdc")
    assert conn["password"] == client.secrets.compose_state != client.secrets.state and "extra" not in conn
    assert client.leg_state_url("d") == f"postgresql://{ROLE}:{client.secrets.compose_state}@postgres:5432/d"
    # The default is unchanged: the gate's TLS state server.
    assert stand.connections()["rivet_gate_state_mysql_cdc"]["host"] == "gate-state"
    assert stand.leg_state_url("d") == stand.state_url("d")
    with pytest.raises(ValueError):
        Stand("3.3.2", stand.rivet_bin, tmp_path / "x", state_place="sqlite")


def test_the_state_database_is_made_owned_by_its_role(tmp_path: Path, monkeypatch) -> None:
    client = Stand("3.3.2", Path("/bin/true"), tmp_path, sources=(MYSQL,), state_place="compose-postgres")
    calls = []
    monkeypatch.setattr(client, "metastore_sql", lambda db, sql: calls.append((db, sql)) or "")
    monkeypatch.setattr(client, "psql", lambda *a: calls.append(a))
    client.make_leg_state_db("rivet_state_mysql_cdc")
    # Mutant: the database made without OWNER (Airflow's superuser would own it), or on the gate's state server.
    assert [db for db, _ in calls] == [METASTORE_DB, METASTORE_DB]
    assert "CREATE ROLE rivet_state LOGIN PASSWORD" in calls[0][1] and "IF NOT EXISTS" in calls[0][1]
    assert calls[1][1] == "CREATE DATABASE rivet_state_mysql_cdc OWNER rivet_state;"


def test_the_client_state_place_needs_a_source_leg(capsys) -> None:
    with pytest.raises(SystemExit):
        gate_main.main(["--rivet-bin", "x", "--state", "compose-postgres"])
    assert "pass --sources" in capsys.readouterr().err


def test_mysql_carries_the_recreate_changes_it_counts() -> None:
    sql, ops = MYSQL.recreate
    assert sql.count("BETWEEN 51 AND 55") == 1 and ops["gate_orders"]["update"] == 5
    assert sql.count("INSERT") == ops["gate_orders"]["insert"] and sql.count("DELETE") == ops["gate_orders"]["delete"]
    assert sources.recreated is recreated
