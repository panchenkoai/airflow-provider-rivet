"""The harness itself: the fake binary refuses what real rivet refuses, records what every start of it saw, and the
CLI table it reads is the one rivet's clap definitions give."""

from __future__ import annotations

import json
import subprocess

import pytest
import rivet_cli_surface
from conftest import NEW_FLAGS, RIVET_SRC, Rig

from airflow_provider_rivet.capabilities import Capabilities, probe


def test_the_cli_table_is_the_one_rivets_clap_definitions_give() -> None:
    """tests/rivet_cli_surface.json is regenerated from src/cli/args.rs at each impersonated ref (v0.31.0, and
    RIVET_CONTRACT_REF as `main`); a bump of the pin or an edit of the table by hand fails here."""
    checked_in = json.loads(rivet_cli_surface.TABLE.read_text())
    assert checked_in == rivet_cli_surface.table(RIVET_SRC), f"the table drifted: run `{rivet_cli_surface.REGENERATE}`"
    assert checked_in["main"]["ref"] == rivet_cli_surface.contract_ref()


def test_the_table_reader_sees_every_kind_of_clap_argument() -> None:
    """The reader's own check, on shapes args.rs holds today: short and long, a value enum, an integer, a positional,
    a nested subcommand, an alias, a global flag, a defaulted (not required) value."""
    table = json.loads(rivet_cli_surface.TABLE.read_text())["0.31.0"]
    subs = table["subcommands"]
    opts = {o["long"]: o for o in subs["plan"]["options"]}
    assert (opts["config"]["short"], opts["config"]["required"], opts["config"]["switch"]) == ("c", True, False)
    assert (opts["format"]["kind"], opts["format"]["values"], opts["format"]["required"]) == ("enum", ["pretty", "json"], False)
    assert opts["param"]["multiple"] and opts["param"]["value_name"] == "KEY=VALUE"
    assert opts["annotate-waves"]["switch"]
    assert [p["name"] for p in subs["apply"]["positionals"]] == ["plan_file"]
    assert {o["long"]: o["kind"] for o in subs["metrics"]["options"]}["last"] == "int"
    reset_chunks = subs["state"]["subcommands"]["reset-chunks"]["options"]
    assert {o["long"]: o["aliases"] for o in reset_chunks}["stuck-checkpoints"] == ["failed"]
    assert [o["long"] for o in table["globals"]] == ["json-errors"]
    assert not {o["long"]: o for o in subs["load"]["options"]}["run-id"]["required"], "`env =` makes it optional"


def fake(rig: Rig, *argv: str) -> subprocess.CompletedProcess:
    return subprocess.run([str(rig.bin / "rivet"), *argv], capture_output=True, text=True, check=False)


# clap 4's messages, as rivet prints them (checked against a rivet binary: exit 2, stderr, this text)
REFUSALS = [
    (["XXapplyXX", "--help"], "error: unrecognized subcommand 'XXapplyXX'\n\nUsage: rivet [OPTIONS] <COMMAND>"),
    (["plan", "-c", "x", "--FORMAT", "json"], "error: unexpected argument '--FORMAT' found"),
    (
        ["plan", "-c", "x", "--format", "XX"],
        "error: invalid value 'XX' for '--format <FORMAT>'\n  [possible values: pretty, json]",
    ),
    (["metrics", "-c", "x", "--last", "abc"], "error: invalid value 'abc' for '--last <LAST>': invalid digit found in string"),
    (["plan", "--export", "a"], "error: the following required arguments were not provided:\n  --config <CONFIG>"),
    (["plan", "-c", "x", "-e", "a", "-e", "b"], "error: the argument '--export <EXPORT>' cannot be used multiple times"),
    (["plan", "-c"], "error: a value is required for '--config <CONFIG>' but none was supplied"),
    (["apply"], "error: the following required arguments were not provided:\n  <PLAN_FILE>"),
    (["apply", "a", "b"], "error: unexpected argument 'b' found"),
    (["run", "-c", "x", "--version"], "error: unexpected argument '--version' found"),
    (["run", "-c", "x", "--no-notify"], "error: unexpected argument '--no-notify' found"),
    (["load", "-c", "x", "--export", "orders"], "error: unexpected argument '--export' found"),
    ([], "error: 'rivet' requires a subcommand but one was not provided"),
]


@pytest.mark.parametrize(("argv", "message"), REFUSALS, ids=[" ".join(a) or "none" for a, _ in REFUSALS])
def test_the_fake_refuses_a_command_line_as_rivet_does(rig: Rig, argv: list[str], message: str) -> None:
    done = fake(rig, *argv)
    assert (done.returncode, done.stdout) == (2, ""), done
    assert done.stderr.startswith(message) and done.stderr.endswith("For more information, try '--help'.\n"), done.stderr
    assert [r["argv"] for r in rig.invocations("refused")] == [argv]
    assert not rig.calls(), "a refused command line is not a call: no scenario is replayed"


def test_a_flag_a_scenario_declares_is_accepted_with_its_value_and_listed_in_help(rig: Rig) -> None:
    rig.scenario(flags=NEW_FLAGS)
    assert fake(rig, "load", "-c", str(rig.config), "--export", "orders", "--lock-wait", "3").returncode == 0
    assert fake(rig, "load", "-c", str(rig.config), "--lock-wait").returncode == 2, "a declared value is required"
    assert fake(rig, "run", "-c", str(rig.config), "--no-notify", "x").returncode == 2, "a declared switch takes none"
    rig.invocations("refused")
    shown = fake(rig, "load", "--help").stdout
    assert "--export <EXPORT>" in shown and "--lock-wait <SECONDS>" in shown and "--rebuild-changelog" in shown


def test_the_help_lists_the_rivet_flags_of_the_impersonated_version(rig: Rig) -> None:
    """`rivet state accept` exists on main, not in 0.31.0: the fake answers as the version it says it is."""
    assert fake(rig, "state", "accept", "--help").returncode == 2
    rig.invocations("refused")
    rig.scenario(version="0.32.0")
    assert "--param <KEY=VALUE>" in fake(rig, "state", "accept", "--help").stdout


def test_every_start_of_the_binary_is_recorded_with_its_whole_environment(rig: Rig, monkeypatch: pytest.MonkeyPatch) -> None:
    """Probes included: `Capabilities._run` hands a probe PATH and HOME only (a probe that inherited the worker's
    environment would give `rivet --version` the Fernet key and every cloud secret)."""
    monkeypatch.setenv("AIRFLOW__CORE__FERNET_KEY", "fernet-planted")
    caps = probe(str(rig.bin / "rivet"))
    assert caps.has_flag("run", "--export")
    assert [p["argv"] for p in rig.probes()] == [["--version"], ["run", "--help"]]
    rig.assert_probes_minimal_env()


def test_the_probe_environment_helper_fails_on_a_probe_that_inherits_the_worker_environment(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The helper's own negative control: a probe run with `os.environ` (the `_run__mutmut_*` shape) is caught."""
    import os

    monkeypatch.setenv("AIRFLOW__CORE__FERNET_KEY", "fernet-planted")
    original = subprocess.run
    monkeypatch.setattr(subprocess, "run", lambda argv, **kw: original(argv, **{**kw, "env": dict(os.environ)}))
    Capabilities(str(rig.bin / "rivet"))
    with pytest.raises(AssertionError, match="AIRFLOW__CORE__FERNET_KEY"):
        rig.assert_probes_minimal_env()
