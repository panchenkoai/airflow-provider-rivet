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
    assert conn["password"] == client.secrets.compose_state != client.secrets.state
    # Mutant: no `sslmode` (rivet refuses a remote state URL that names none, RIVET_STATE_URL_TLS_REQUIRED, exit 5).
    assert conn["extra"] == {"rivet_params": "sslmode=disable"}
    assert client.leg_state_url("d") == f"postgresql://{ROLE}:{client.secrets.compose_state}@postgres:5432/d?sslmode=disable"
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


# --- Two overlapping runs of one stream ---------------------------------------------------------------------------------

BUSY_LOG = (
    "[2026-10-21, 22:00:31 UTC] {tasklog.py:90} WARNING - rivet run cdc → BUSY [RIVET_AIRFLOW_EXPORT_BUSY]: the lock of "
    "cdc stayed held for 30 s; rivet was not started, Airflow retries it (try 1 of 2)\n"
)
TRIES = [{"try_number": 1, "state": "up_for_retry"}, {"try_number": 2, "state": "success"}]


def test_the_second_run_ends_its_first_try_busy_and_its_retry_goes_through() -> None:
    codes = sources.codes_in(BUSY_LOG)
    assert codes == ["RIVET_AIRFLOW_EXPORT_BUSY"]
    assert sources.overlap_problems("success", "success", TRIES, codes, 3, 3) == []
    # Rivet's own run lease, reached when the provider's lock is not the one held, is as good.
    assert sources.overlap_problems("success", "success", TRIES, ["RIVET_STATE_RUN_IN_PROGRESS"], 3, 3) == []
    # Mutant: the first try's code not read (a run that failed for another cause passes).
    assert sources.overlap_problems("success", "success", TRIES, ["RIVET_SOURCE_CDC_CHECKPOINT_SHARED"], 3, 3) == [
        "its first try names no busy code (['RIVET_SOURCE_CDC_CHECKPOINT_SHARED']), expected one of "
        "['RIVET_AIRFLOW_EXPORT_BUSY', 'RIVET_STATE_RUN_IN_PROGRESS', 'RIVET_STATE_LOCK_TIMEOUT']",
        "its first try names other codes ['RIVET_SOURCE_CDC_CHECKPOINT_SHARED']",
    ]
    # Mutant: the tries not read (two runs that both went through at once, no overlap, pass).
    assert sources.overlap_problems("success", "success", [{"state": "success"}], codes, 3, 3) == [
        "its second run's `run` tries ['success']: its first try must end busy and be retried".replace("its second", "the second")
    ]
    # Mutant: the rows not summed over both runs (a change moved twice, or by neither).
    assert sources.overlap_problems("success", "success", TRIES, codes, 6, 3) == [
        "the two runs moved 6 row(s) together, the source changed 3"
    ]
    assert sources.overlap_problems("failed", "success", TRIES, codes, 3, 3) == ["the held run failed"]


def test_the_second_dag_waits_briefly_and_retries_once() -> None:
    dag = (Path(sources.__file__).parent / "stand" / "dags" / "rivet_gate.py").read_text()
    block = dag[dag.index('overlap = shape.get("overlap")') : dag.index("# A second CDC config")]
    # Mutant: the default lock_wait (600 s: the held run ends first and nothing is busy) or no retry.
    assert '"lock_wait": overlap["lock_wait"]' in block and '"retries": 1' in block
    assert 0 < sources.OVERLAP_LOCK_WAIT < MYSQL.hot.pause_ms / 1000, "the wait must end inside the held run's pause"


def test_a_try_log_is_read_by_its_number(tmp_path: Path) -> None:
    stand = type("S", (), {"dir": tmp_path})()
    root = tmp_path / "logs" / "dag_id=d" / "run_id=r" / "task_id=run"
    root.mkdir(parents=True)
    (root / "attempt=1.log").write_text("first")
    (root / "attempt=2.log").write_text("second")
    # Mutant: the attempt ignored (the retry's log read for the first try's code).
    assert sources.task_log(stand, "d", "r", "run", attempt=1) == "first"
    assert sources.task_log(stand, "d", "r", "run") == "second"
    assert sources.task_log(stand, "d", "r", "run", attempt=3) is None


# --- init's CDC config beside a database of the same table names (panchenkoai/rivet#675) --------------------------------


def test_the_unqualified_config_must_run_or_be_refused_at_exit_5() -> None:
    from test_conns import CLOUD

    from gate.scenario import KNOWN
    from gate.sources import BUCKET_SHAPE, SourceLeg, unqualified_problems
    from gate.warehouse import DATASET_SHAPE

    refused = {"error": {"code": "RIVET_SOURCE_CDC_TABLE_UNRESOLVED"}, "exit_status": 5}
    one = [{"state": "failed"}]
    assert unqualified_problems(0, "", "failed", one, refused, []) == []
    # Today (#675): the same code at exit 1. Mutant: the exit not compared (the cell would pass today).
    assert unqualified_problems(0, "", "failed", one, {**refused, "exit_status": 1}, []) == [
        "the run stopped [RIVET_SOURCE_CDC_TABLE_UNRESOLVED] exit 1, not a refusal at exit 5"
    ]
    # Mutant: the tries not counted (a refusal Airflow retried passes).
    assert unqualified_problems(0, "", "failed", one * 3, refused, []) == [
        "the run's `run` was tried 3 time(s): a refusal is never retried"
    ]
    # The other way 0.32.3 may go: init names `<db>.<table>` and the stream runs; then BigQuery is graded.
    assert unqualified_problems(0, "", "success", one, None, []) == []
    assert unqualified_problems(0, "", "success", one, None, ["gate_orders: 1 missing row(s)"]) == [
        "gate_orders: 1 missing row(s)"
    ]
    # Or init refuses it, by code at exit 5. Mutant: any init failure accepted.
    assert unqualified_problems(5, "Error: [RIVET_INIT_X] ...", None, [], None, []) == []
    assert unqualified_problems(1, "Error: boom", None, [], None, []) == ["init exited 1 with no code"]
    assert unqualified_problems(1, "Error: [RIVET_INIT_X] ...", None, [], None, []) == ["init exited 1 RIVET_INIT_X"]
    assert "panchenkoai/rivet#675" in KNOWN["unqualified/mysql/cdc/same-names-in-another-db"]
    leg = SourceLeg(CLOUD, "3.3.2", MYSQL)
    assert BUCKET_SHAPE.fullmatch(leg.unq_bucket) and DATASET_SHAPE.fullmatch(leg.unq_dataset), "the sweep drops them"
