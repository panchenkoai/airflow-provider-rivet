"""Every preflight refusal as the operator shows it: the exact code, the exact sentence (its remedy is the contract an
operator reads), the `rivet.refused` record and the XCom payload; and the guards' edges that decide WHETHER to refuse.

Each refusal goes through one helper that pins all four, so a dropped or renamed key of `RivetBaseOperator._refuse`
fails every test here (operators `_refuse` mutants 6/7/12/13)."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest
from airflow.exceptions import AirflowFailException
from conftest import FakeTI, Rig

from airflow_provider_rivet.operators import (
    RivetApplyOperator,
    RivetCdcRunOperator,
    RivetLoadOperator,
    RivetRunOperator,
)
from airflow_provider_rivet.preflight import MARKER_NAME, SETUP_GUIDE, run_preflight

PG_STATE = {"RIVET_PG_URL": "x", "RIVET_STATE_URL": "postgresql://state:pw@state-db/rivet_state"}
UNRESOLVED_TAIL = (
    "holds a `${...}` that neither a `-p` param nor the task's env sets; rivet refuses an unresolved placeholder: "
    "pass the variable through `env`, `env_passthrough` or `-p`"
)


def recorded(rig: Rig, op: object) -> object:
    """An operator built without `rig.operator` (to keep a default it sets), its records kept as `rig.operator` keeps them."""
    original = op._emit

    def record(entry: dict) -> None:
        rig.records.append(entry)
        original(entry)

    op._emit = record
    return op


def refusal(rig: Rig, op: object, code: str, text: str, ti: FakeTI | None = None) -> None:
    """The task is refused before rivet starts, with exactly this code and sentence everywhere it is shown."""
    before = len(rig.calls())
    exc, ti = rig.run(op, ti)
    full = f"[{code}] {text}"
    assert type(exc) is AirflowFailException, f"{type(exc).__name__}: {exc}"
    assert str(exc) == full
    assert len(rig.calls()) == before, "a refused task must not start rivet"
    assert rig.events("rivet.refused")[-1] == {"event": "rivet.refused", "code": code, "text": full}
    payload = ti.store[(ti.task_id, "return_value")]
    assert payload == {"command": op.command, "decision": "fail", "preflight_refusal": code, "units": []}


# --- the binary -------------------------------------------------------------------------------------------------------


def test_a_binary_that_is_not_on_the_path_is_named(rig: Rig) -> None:
    """preflight resolve_binary mutants 11 / 14; and operators `_environment` 21: with no `connections` the config is
    not read before the binary is found, so a missing binary is the refusal even when the config is missing too."""
    op = RivetApplyOperator(task_id="task", config=str(rig.tmp / "nope.yaml"), export="orders", rivet_bin="rivet-not-installed")
    refusal(rig, recorded(rig, op), "RIVET_AIRFLOW_BINARY_MISSING", "no rivet binary `rivet-not-installed` on the worker's PATH")


def test_the_binary_is_looked_up_on_the_tasks_path(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """resolve_binary mutants 3 / 5-8: `rivet_bin="rivet"` (the default) is found on the PATH the task's env gives
    rivet, not on the worker's own PATH; and operators `__init__` 1 / 2: the default name is `rivet`."""
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    op = recorded(
        rig,
        RivetRunOperator(
            task_id="task",
            config=str(rig.config),
            export="orders",
            state_dir=str(rig.state_dir),
            deployment="local",
            env={"RIVET_PG_URL": "x", "PATH": f"{rig.bin}:/usr/bin:/bin"},
        ),
    )
    payload, _ = rig.run(op)
    assert isinstance(payload, dict), payload
    assert payload["decision"] == "success"
    assert rig.events("rivet.start")[0]["argv"][0] == str(rig.bin / "rivet")


# --- the config -------------------------------------------------------------------------------------------------------

SHAPES = {
    "empty_file": (b"", "config {cfg} is not a YAML mapping"),
    "top_level_list": (b"- a\n", "config {cfg} is not a YAML mapping"),
    "non_utf8": (b"exports:\n  - name: caf\xe9\n", "cannot read config {cfg}: UnicodeDecodeError"),
    "exports_a_number": (b"exports: 5\n", "config {cfg}: `exports` is not a list"),
    "export_entry_not_a_mapping": (b"exports:\n  - orders\n", "config {cfg}: `exports[0]` is not a mapping"),
    "second_name_a_list": (
        b"exports:\n  - name: a\n  - name: [orders]\n",
        "config {cfg}: `exports[1].name` is not a scalar",
    ),
}


@pytest.mark.parametrize(("text", "sentence"), SHAPES.values(), ids=SHAPES.keys())
def test_a_config_rivet_cannot_build_is_refused_with_what_is_wrong(rig: Rig, text: bytes, sentence: str) -> None:
    """read_config_source mutants 19 / 24 / 26-31 / 40 / 49 / 63."""
    rig.config.write_bytes(text)
    refusal(rig, rig.operator(RivetRunOperator), "RIVET_AIRFLOW_CONFIG_UNREADABLE", sentence.format(cfg=rig.config))


def test_an_unknown_export_is_named(rig: Rig) -> None:
    """run_preflight mutant 70."""
    refusal(
        rig,
        rig.operator(RivetApplyOperator, export="not_in_config"),
        "RIVET_AIRFLOW_EXPORT_UNKNOWN",
        f"config {rig.config} has no export `not_in_config`",
    )


def test_an_unresolved_export_name_is_named_with_its_placeholder(rig: Rig) -> None:
    """refuse_unresolved mutant 2; run_preflight mutants 61-68."""
    rig.config.write_text(rig.config.read_text().replace("  - name: orders\n", "  - name: orders_${ENV}\n"))
    refusal(
        rig,
        rig.operator(RivetRunOperator, export="orders_prod"),
        "RIVET_AIRFLOW_CONFIG_UNRESOLVED",
        f"export name `orders_${{ENV}}` of config {rig.config} {UNRESOLVED_TAIL}",
    )


def test_an_unresolved_checkpoint_is_named_with_its_placeholder(rig: Rig) -> None:
    """check_nothing_left_beside_config mutants 43-51."""
    rig.config.write_text(
        rig.config.read_text().replace("    mode: cdc\n", "    mode: cdc\n    cdc:\n      checkpoint: ${CKPT}\n")
    )
    refusal(
        rig,
        rig.operator(RivetCdcRunOperator, export="orders_cdc"),
        "RIVET_AIRFLOW_CONFIG_UNRESOLVED",
        f"cdc.checkpoint `${{CKPT}}` of config {rig.config} {UNRESOLVED_TAIL}",
    )


def test_an_export_entry_without_a_name_is_not_an_export_and_breaks_nothing(rig: Rig) -> None:
    """export_modes mutant 20 (`and` -> `or`): an entry with no `name` raised a raw KeyError, retried."""
    rig.config.write_text("exports:\n  - mode: full\n  - name: orders\n    mode: full\n")
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


def test_a_config_with_no_exports_lets_rivet_answer_for_the_export(rig: Rig) -> None:
    """run_preflight mutants 79 / 81: nothing to check the export against, so rivet is asked (no raw TypeError)."""
    rig.config.write_text("source:\n  type: postgres\nexports: []\n")
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload
    assert rig.argv("run")[:2] == ["run", "--config"]


# --- the deployment ---------------------------------------------------------------------------------------------------


def test_a_pod_without_postgres_state_is_told_why(rig: Rig) -> None:
    """run_preflight mutants 101 / 106-110."""
    refusal(
        rig,
        rig.operator(RivetRunOperator, export="orders", deployment="pod"),
        "RIVET_AIRFLOW_STATE_SQLITE_ON_POD",
        "this worker is an ephemeral pod and RIVET_STATE_URL is not a postgres URL; SQLite state would be lost with the "
        "pod and every later task would start from an empty state",
    )


CDC_ON_POD = (
    "CDC is refused on an ephemeral pod: the stream's checkpoint is a file and would live on the pod's disk; this holds "
    "until rivet stores the checkpoint in the state database"
)


def test_cdc_on_a_pod_is_told_why(rig: Rig) -> None:
    """run_preflight mutants 117-121."""
    op = rig.operator(RivetCdcRunOperator, export="orders_cdc", deployment="pod", env=PG_STATE)
    refusal(rig, op, "RIVET_AIRFLOW_CDC_ON_POD", CDC_ON_POD)


def test_a_whole_config_run_on_a_pod_is_refused_when_the_config_holds_a_stream(rig: Rig) -> None:
    """run_preflight mutant 77: with no `export` every export of the config is touched, the CDC one included."""
    refusal(rig, rig.operator(RivetRunOperator, deployment="pod", env=PG_STATE), "RIVET_AIRFLOW_CDC_ON_POD", CDC_ON_POD)
    rig.config.write_text("source:\n  type: postgres\nexports:\n  - name: orders\n    mode: full\n")
    payload, _ = rig.run(rig.operator(RivetRunOperator, deployment="pod", env=PG_STATE))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


# --- the state directory ----------------------------------------------------------------------------------------------


def test_a_cdc_task_under_postgres_state_still_needs_a_state_directory(rig: Rig) -> None:
    """run_preflight mutant 122 (`is_cdc and state_dir`): the stream's checkpoint is a file on the worker whatever the
    state backend, so a CDC task with no `state_dir` is refused; it was let through to anchor nowhere durable."""
    op = rig.operator(RivetCdcRunOperator, export="orders_cdc", env=PG_STATE, state_dir=None)
    refusal(
        rig,
        op,
        "RIVET_AIRFLOW_STATE_DIR_REQUIRED",
        f"this task keeps state or a CDC checkpoint on the worker and needs `state_dir`; see {SETUP_GUIDE}",
    )
    batch = rig.operator(RivetRunOperator, export="orders", env=PG_STATE, state_dir=None)
    payload, _ = rig.run(batch)
    assert payload["decision"] == "success" and payload["state_marker"] is None, "a batch task on PostgreSQL needs none"


def test_a_state_directory_given_under_postgres_state_is_checked_and_marked(rig: Rig) -> None:
    """run_preflight mutant 122, the other half: a declared `state_dir` is checked and gets its marker, any backend."""
    payload, _ = rig.run(rig.operator(RivetRunOperator, export="orders", env=PG_STATE))
    assert payload["state_marker"] == (rig.state_dir / MARKER_NAME).read_text()
    missing = str(rig.tmp / "never-created")
    refusal(
        rig,
        rig.operator(RivetRunOperator, export="orders", env=PG_STATE, state_dir=missing),
        "RIVET_AIRFLOW_STATE_DIR_MISSING",
        f"state directory {missing} is not an existing absolute directory on this worker; it is never created by a task, "
        f"because an empty one reads as a first run; see {SETUP_GUIDE}",
    )


def test_a_state_directory_the_worker_cannot_write_is_named(rig: Rig) -> None:
    """check_state_dir mutant 26."""
    locked = rig.tmp / "locked"
    locked.mkdir(mode=0o500)
    op = rig.operator(RivetApplyOperator, export="orders", state_dir=str(locked))
    refusal(rig, op, "RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE", f"state directory {locked} is not writable")


def test_the_marker_is_created_readable_by_every_task_user(rig: Rig) -> None:
    """ensure_marker mutants 9 / 12: the marker is 0644 (under umask 022), so a task of another user can read it."""
    old = os.umask(0o022)
    try:
        rig.run(rig.operator(RivetRunOperator, export="orders"))
    finally:
        os.umask(old)
    assert stat.S_IMODE(os.stat(rig.state_dir / MARKER_NAME).st_mode) == 0o644


def test_an_upstream_marker_of_another_directory_is_named_by_eight_characters(rig: Rig) -> None:
    """run_preflight mutants 136 / 142 / 143."""
    store: dict = {}
    payload, _ = rig.run(rig.operator(RivetApplyOperator, task_id="up", export="orders"), FakeTI("up", store=store))
    store[("up", "return_value")] = payload
    other = rig.tmp / "other-state"
    other.mkdir()
    load = rig.operator(RivetLoadOperator, task_id="load", export="orders", state_dir=str(other))
    load.upstream_task_ids.add("up")
    rig.run(rig.operator(RivetRunOperator, export="orders", state_dir=str(other)))  # writes other's marker
    mine = (other / MARKER_NAME).read_text()
    theirs = payload["state_marker"]
    refusal(
        rig,
        load,
        "RIVET_AIRFLOW_STATE_DIR_DIFFERS",
        f"an upstream task of this DAG run used another state directory than {other} on this worker "
        f"(marker {theirs[:8]} against {mine[:8]}); every task must see the same path; see {SETUP_GUIDE}",
        FakeTI("load", store=store),
    )


# --- a checkpoint left beside the config ------------------------------------------------------------------------------


def with_checkpoints(rig: Rig, refs: dict[str, str]) -> None:
    """Give named CDC exports a `cdc.checkpoint`."""
    text = rig.config.read_text()
    for name, ref in refs.items():
        text = text.replace(
            f"  - name: {name}\n    mode: cdc\n", f"  - name: {name}\n    mode: cdc\n    cdc:\n      checkpoint: {ref}\n"
        )
    rig.config.write_text(text)


def test_a_checkpoint_left_beside_the_config_is_named_with_both_paths(rig: Rig) -> None:
    """check_nothing_left_beside_config mutants 67-69 / 71."""
    with_checkpoints(rig, {"orders_cdc": "ckpt/orders.ckpt"})
    (rig.config_dir / "ckpt").mkdir()
    (rig.config_dir / "ckpt" / "orders.ckpt").write_text("lsn")
    origin = rig.config_dir.resolve() / "ckpt" / "orders.ckpt"
    refusal(
        rig,
        rig.operator(RivetCdcRunOperator, export="orders_cdc"),
        "RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG",
        f"the checkpoint of `orders_cdc` is at {origin}, and under Airflow the relative `cdc.checkpoint` resolves in "
        f"the state directory: a stream that starts without its checkpoint re-anchors and skips the changes in between. "
        f"Stop the stream, then move the file to {rig.state_dir / 'ckpt' / 'orders.ckpt'}; see {SETUP_GUIDE}",
    )


def test_an_absolute_checkpoint_does_not_end_the_search_for_the_next_stream(rig: Rig) -> None:
    """check_nothing_left_beside_config mutant 54 (`continue` -> `break`)."""
    rig.config.write_text(
        rig.config.read_text() + "  - name: second_cdc\n    mode: cdc\n    cdc:\n      checkpoint: ckpt/b.ckpt\n"
    )
    with_checkpoints(rig, {"orders_cdc": str(rig.tmp / "a.ckpt")})
    (rig.config_dir / "ckpt").mkdir()
    (rig.config_dir / "ckpt" / "b.ckpt").write_text("lsn")
    exc, ti = rig.run(rig.operator(RivetRunOperator))
    assert type(exc) is AirflowFailException, exc
    assert ti.store[("task", "return_value")]["preflight_refusal"] == "RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG"
    assert "`second_cdc`" in str(exc)


def test_a_new_stream_whose_checkpoint_exists_nowhere_yet_runs(rig: Rig) -> None:
    """check_nothing_left_beside_config mutant 55 (`and` -> `or`)."""
    with_checkpoints(rig, {"orders_cdc": "ckpt/new.ckpt"})
    payload, _ = rig.run(rig.operator(RivetCdcRunOperator, export="orders_cdc"))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload


def test_a_checkpoint_already_in_the_state_directory_runs_though_a_copy_is_left_beside_the_config(rig: Rig) -> None:
    """check_nothing_left_beside_config mutant 60: the state directory's copy is the one rivet reads."""
    with_checkpoints(rig, {"orders_cdc": "ckpt/orders.ckpt"})
    for root in (rig.config_dir, rig.state_dir):
        (root / "ckpt").mkdir()
        (root / "ckpt" / "orders.ckpt").write_text("lsn")
    payload, _ = rig.run(rig.operator(RivetCdcRunOperator, export="orders_cdc"))
    assert isinstance(payload, dict) and payload["decision"] == "success", payload
    assert Path(rig.state_dir / "ckpt" / "orders.ckpt").read_text() == "lsn"


# --- the notifications warning ----------------------------------------------------------------------------------------


def test_a_notifications_block_is_warned_about_in_words_until_run_has_no_notify(rig: Rig) -> None:
    """run_preflight mutants 172 / 173 (the subcommand probed) and 179-183 (the warning's text)."""
    rig.config.write_text(rig.config.read_text() + "notifications:\n  slack:\n    on: [failure]\n")
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    warning = [w for w in rig.events("rivet.warning") if w["warning"] == "notifications_block"]
    assert warning == [
        {
            "event": "rivet.warning",
            "warning": "notifications_block",
            "text": "the config has a `notifications:` block and this rivet has no `--no-notify`: rivet will send its "
            "own messages beside Airflow's alerts; remove the block from configs run under Airflow",
        }
    ]


def test_no_notifications_warning_when_rivet_run_has_no_notify(rig: Rig) -> None:
    """run_preflight mutants 172 / 173: the flag is looked for in `rivet run --help`, the only help that lists it here."""
    rig.config.write_text(rig.config.read_text() + "notifications:\n  slack:\n    on: [failure]\n")
    rig.scenario(flags={"run": ["--no-notify"]})
    rig.run(rig.operator(RivetRunOperator, export="orders"))
    assert [w["warning"] for w in rig.events("rivet.warning")] == ["sqlite_single_host"]
    assert "--no-notify" in rig.argv("run")


# --- `run_preflight` called as a function -----------------------------------------------------------------------------


def test_run_preflight_called_with_its_defaults_checks_a_batch_task_as_one(rig: Rig) -> None:
    """run_preflight mutants 3 (`cdc` defaults to False) and 41 (the deployment it reports); export_modes mutants
    13 / 15 / 18 / 19 (an export without `mode` is `full`, as rivet reads it)."""
    rig.config.write_text("source:\n  type: postgres\nexports:\n  - name: orders\n  - name: users\n    mode: cdc\n")
    pf = run_preflight(
        rivet_bin=str(rig.bin / "rivet"), config_path=str(rig.config), env=PG_STATE, state_dir=None, export="orders"
    )
    assert (pf.deployment, pf.state_kind, pf.state_dir, pf.marker) == ("local", "postgres", None, None)
    assert pf.export_modes() == {"orders": "full", "users": "cdc"}
    pod = run_preflight(
        rivet_bin=str(rig.bin / "rivet"),
        config_path=str(rig.config),
        env={**PG_STATE, "KUBERNETES_SERVICE_HOST": "h"},
        state_dir=None,
        export="orders",
    )
    assert pod.deployment == "pod"
