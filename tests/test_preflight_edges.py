"""The preflight at its edges, called directly: the exact text of every refusal and warning (a user reads it and acts on
it), and the behaviour the mutation run found unpinned (the PATH it searches, the marker's mode, the state directory a
CDC task on PostgreSQL state still needs, the default mode, a checkpoint in both places or in neither)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from conftest import Rig

from airflow_provider_rivet.preflight import (
    SETUP_GUIDE,
    Preflight,
    PreflightRefusal,
    check_nothing_left_beside_config,
    check_state_dir,
    ensure_marker,
    read_config_source,
    resolve_binary,
    run_preflight,
)

PG_URL = "postgresql://state:pw@state-db/rivet_state"


def pre(rig: Rig, **kw: object) -> Preflight:
    """`run_preflight` on the rig's fake binary and config, a local worker with SQLite state by default."""
    args: dict = {
        "rivet_bin": "rivet",
        "config_path": str(rig.config),
        "env": {"PATH": str(rig.bin)},
        "state_dir": str(rig.state_dir),
        "deployment": "local",
        "export": None,
        "cdc": False,
    }
    return run_preflight(**{**args, **kw})


def refusal(call: object, *args: object, **kw: object) -> str:
    """The refusal text a call raises."""
    with pytest.raises(PreflightRefusal) as info:
        call(*args, **kw)  # type: ignore[operator]
    return str(info.value)


def with_exports(rig: Rig, exports: str) -> str:
    """Replace the rig's config with one whose `exports:` block is `exports`; its path."""
    rig.config.write_text(f"source:\n  type: postgres\n  url_env: RIVET_PG_URL\n{exports}")
    return str(rig.config)


# --- the text of each refusal -----------------------------------------------------------------------------------------


def test_a_missing_binary_is_refused_naming_it_and_the_path_searched(tmp_path: Path) -> None:
    # resolve_binary 11, 14
    assert refusal(resolve_binary, "rivet", {"PATH": str(tmp_path)}) == (
        "[RIVET_AIRFLOW_BINARY_MISSING] no rivet binary `rivet` on the worker's PATH"
    )


def test_each_state_directory_refusal_says_what_is_wrong_and_where_to_read_more(tmp_path: Path) -> None:
    # check_state_dir 3, 14, 26
    assert refusal(check_state_dir, None) == (
        "[RIVET_AIRFLOW_STATE_DIR_REQUIRED] this task keeps state or a CDC checkpoint on the worker and needs "
        f"`state_dir`; see {SETUP_GUIDE}"
    )
    assert refusal(check_state_dir, "relative/dir") == (
        "[RIVET_AIRFLOW_STATE_DIR_MISSING] state directory relative/dir is not an existing absolute directory on this "
        "worker; it is never created by a task, because an empty one reads as a first run; see " + SETUP_GUIDE
    )
    locked = tmp_path / "locked"
    locked.mkdir(mode=0o500)
    assert refusal(check_state_dir, str(locked)) == (
        f"[RIVET_AIRFLOW_STATE_DIR_NOT_WRITABLE] state directory {locked} is not writable"
    )


def test_each_unreadable_config_is_refused_naming_the_file_and_what_is_wrong(tmp_path: Path) -> None:
    # read_config_source 19, 24, 26-31, 40, 49, 63
    cfg = tmp_path / "c.yaml"
    cases = [
        (b"\xff\xfe", "cannot read config {}: UnicodeDecodeError"),
        (b"- a\n", "config {} is not a YAML mapping"),
        (b"exports: 5\n", "config {}: `exports` is not a list"),
        (b"exports: [1]\n", "config {}: `exports[0]` is not a mapping"),
        (b"exports:\n  - name: [1]\n", "config {}: `exports[0].name` is not a scalar"),
    ]
    for body, text in cases:
        cfg.write_bytes(body)
        assert refusal(read_config_source, str(cfg)) == "[RIVET_AIRFLOW_CONFIG_UNREADABLE] " + text.format(cfg), body


def test_an_unknown_export_and_an_unresolved_export_name_are_refused_in_words(rig: Rig) -> None:
    # run_preflight 70; refuse_unresolved 2 and run_preflight 61-68 (the export-name placeholder)
    assert refusal(pre, rig, export="nope") == f"[RIVET_AIRFLOW_EXPORT_UNKNOWN] config {rig.config} has no export `nope`"
    cfg = with_exports(rig, "exports:\n  - name: ${WHICH}\n    mode: full\n")
    assert refusal(pre, rig, export="orders") == (
        f"[RIVET_AIRFLOW_CONFIG_UNRESOLVED] export name `${{WHICH}}` of config {cfg} holds a `${{...}}` that neither a "
        "`-p` param nor the task's env sets; rivet refuses an unresolved placeholder: pass the variable through `env`, "
        "`env_passthrough` or `-p`"
    )


def test_an_unresolved_checkpoint_is_refused_naming_the_key_and_its_value(rig: Rig) -> None:
    # check_nothing_left_beside_config 43-45, 49-51
    cfg = with_exports(rig, "exports:\n  - name: s\n    mode: cdc\n    cdc:\n      checkpoint: ${CK}/s.ckpt\n")
    assert refusal(pre, rig, export="s", cdc=True) == (
        f"[RIVET_AIRFLOW_CONFIG_UNRESOLVED] cdc.checkpoint `${{CK}}/s.ckpt` of config {cfg} holds a `${{...}}` that "
        "neither a `-p` param nor the task's env sets; rivet refuses an unresolved placeholder: pass the variable "
        "through `env`, `env_passthrough` or `-p`"
    )


def test_both_pod_refusals_say_why_the_pod_cannot_hold_it(rig: Rig) -> None:
    # run_preflight 101, 106-110 (SQLite on a pod), 117-121 (CDC on a pod)
    assert refusal(pre, rig, deployment="pod") == (
        "[RIVET_AIRFLOW_STATE_SQLITE_ON_POD] this worker is an ephemeral pod and RIVET_STATE_URL is not a postgres URL; "
        "SQLite state would be lost with the pod and every later task would start from an empty state"
    )
    pg = {"PATH": str(rig.bin), "RIVET_STATE_URL": PG_URL}
    assert refusal(pre, rig, deployment="pod", env=pg, export="orders_cdc") == (
        "[RIVET_AIRFLOW_CDC_ON_POD] CDC is refused on an ephemeral pod: the stream's checkpoint is a file and would "
        "live on the pod's disk; this holds until rivet stores the checkpoint in the state database"
    )


def test_a_task_beside_another_state_directory_is_refused_with_both_markers_cut_to_eight(rig: Rig) -> None:
    # run_preflight 136, 142, 143
    mine = ensure_marker(rig.state_dir)
    assert refusal(pre, rig, upstream_markers=("f" * 32,)) == (
        f"[RIVET_AIRFLOW_STATE_DIR_DIFFERS] an upstream task of this DAG run used another state directory than "
        f"{rig.state_dir} on this worker (marker ffffffff against {mine[:8]}); every task must see the same path; "
        f"see {SETUP_GUIDE}"
    )


def test_a_checkpoint_left_beside_the_config_is_refused_naming_the_stream_and_both_paths(rig: Rig) -> None:
    # check_nothing_left_beside_config 67-69, 71
    origin = rig.config.resolve().parent
    (origin / "s.ckpt").write_text("lsn")
    config = {"exports": [{"name": "s", "mode": "cdc", "cdc": {"checkpoint": "s.ckpt"}}]}
    assert refusal(check_nothing_left_beside_config, str(rig.config), config, rig.state_dir, False, None) == (
        f"[RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG] the checkpoint of `s` is at {origin / 's.ckpt'}, and under Airflow "
        "the relative `cdc.checkpoint` resolves in the state directory: a stream that starts without its checkpoint "
        "re-anchors and skips the changes in between. Stop the stream, then move the file to "
        f"{rig.state_dir / 's.ckpt'}; see {SETUP_GUIDE}"
    )


NOTIFYING = "notifications:\n  slack: {webhook_env: HOOK}\n"


def test_the_notifications_warning_says_what_to_do(rig: Rig) -> None:
    # run_preflight 179-183 (the text); a binary whose `rivet run` has no `--no-notify` (another subcommand's does)
    rig.config.write_text(rig.config.read_text() + NOTIFYING)
    rig.scenario(flags={"apply": ["--no-notify"]})
    assert dict(pre(rig).warnings)["notifications_block"] == (
        "the config has a `notifications:` block and this rivet has no `--no-notify`: rivet will send its own messages "
        "beside Airflow's alerts; remove the block from configs run under Airflow"
    )


def test_the_notifications_warning_reads_the_run_subcommands_flags(rig: Rig) -> None:
    # run_preflight 172, 173: the help of `rivet run`, the command the operators start, decides it
    rig.config.write_text(rig.config.read_text() + NOTIFYING)
    rig.scenario(flags={"run": ["--no-notify"]})
    assert "notifications_block" not in dict(pre(rig).warnings)


# --- behaviour ----------------------------------------------------------------------------------------------------------


def test_the_binary_is_looked_up_on_the_tasks_path_not_the_workers_own(rig: Rig) -> None:
    # resolve_binary 3, 5-8
    assert str(rig.bin) not in os.environ.get("PATH", "").split(os.pathsep)
    assert resolve_binary("rivet", {"PATH": str(rig.bin)}) == str(rig.bin / "rivet")


def test_the_state_directory_marker_is_written_0644_whatever_the_umask(tmp_path: Path) -> None:
    # ensure_marker 9 (the default mode, 0o777 through the umask), 12 (0o645)
    old = os.umask(0)
    try:
        marker = ensure_marker(tmp_path)
    finally:
        os.umask(old)
    path = tmp_path / ".rivet_airflow_marker"
    assert (path.stat().st_mode & 0o777, path.read_text()) == (0o644, marker)


def test_a_cdc_task_on_postgres_state_still_needs_its_state_directory_for_the_checkpoint(rig: Rig) -> None:
    # run_preflight 122 (`or is_cdc and state_dir` skipped the check when no directory was given)
    pg = {"PATH": str(rig.bin), "RIVET_STATE_URL": PG_URL}
    assert refusal(pre, rig, env=pg, state_dir=None, export="orders_cdc").startswith("[RIVET_AIRFLOW_STATE_DIR_REQUIRED]")
    assert refusal(pre, rig, env=pg, state_dir=None, cdc=True).startswith("[RIVET_AIRFLOW_STATE_DIR_REQUIRED]")
    assert pre(rig, env=pg, state_dir=None, export="orders").state_dir is None, "a batch export on PostgreSQL needs none"


def test_a_task_without_an_export_is_held_to_every_exports_mode(rig: Rig) -> None:
    # the `touched` modes: with no export every mode counts, so a config holding a CDC export is refused on a pod
    pg = {"PATH": str(rig.bin), "RIVET_STATE_URL": PG_URL}
    assert refusal(pre, rig, deployment="pod", env=pg).startswith("[RIVET_AIRFLOW_CDC_ON_POD]")
    with_exports(rig, "exports: []\n")
    assert pre(rig, deployment="pod", env=pg, export="orders").state_dir is None, "an empty config refuses nothing here"


def test_an_export_without_a_mode_is_full_and_one_without_a_name_is_not_an_export() -> None:
    # Preflight.export_modes 13, 15, 18, 19 (rivet's default mode), 20 (a mapping without `name`)
    config = {"exports": [{"name": "a"}, {"mode": "cdc"}, {"name": "b", "mode": "cdc"}]}
    pf = Preflight("rivet", None, "sqlite", None, config)  # type: ignore[arg-type]
    assert pf.export_modes() == {"a": "full", "b": "cdc"}


def test_a_checkpoint_in_neither_place_or_in_both_is_not_left_behind(rig: Rig) -> None:
    # check_nothing_left_beside_config 55 (`or`: a first run with no checkpoint anywhere was refused),
    # 60 (the state directory's copy looked up under another name)
    origin = rig.config.resolve().parent
    config = {"exports": [{"name": "s", "mode": "cdc", "cdc": {"checkpoint": "s.ckpt"}}]}
    check_nothing_left_beside_config(str(rig.config), config, rig.state_dir, False, None)
    (origin / "s.ckpt").write_text("old")
    (rig.state_dir / "s.ckpt").write_text("moved")
    check_nothing_left_beside_config(str(rig.config), config, rig.state_dir, False, None)
    absolute = {"exports": [{"name": "s", "mode": "cdc", "cdc": {"checkpoint": str(origin / "s.ckpt")}}]}
    (rig.state_dir / "s.ckpt").unlink()
    check_nothing_left_beside_config(str(rig.config), absolute, rig.state_dir, False, None)  # one path from both
