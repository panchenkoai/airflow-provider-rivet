"""The fake rivet answers like the real CLI: a flag it lists exists in rivet's clap definitions, and a flag or a
subcommand it does not list is refused with exit 2, as clap refuses it."""

from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path

from conftest import RIVET_SRC, Rig

FAKE = ast.parse((Path(__file__).parent / "fake_rivet.py").read_text())  # the fake runs `main()` on import
BASE_FLAGS = next(ast.literal_eval(n.value) for n in FAKE.body if isinstance(n, ast.Assign) and n.targets[0].id == "BASE_FLAGS")


def test_every_flag_the_fake_accepts_is_one_rivet_defines() -> None:
    args = (RIVET_SRC / "src" / "cli" / "args.rs").read_text()
    for sub, flags in BASE_FLAGS.items():
        for flag in flags:
            field = flag[2:].replace("-", "_")
            assert re.search(rf'^\s+(pub )?{field}:|long = "{flag[2:]}"', args, re.M), f"rivet {sub} has no {flag}"


def test_an_unknown_flag_or_subcommand_is_refused_as_clap_refuses_it(rig: Rig) -> None:
    rivet, cfg = str(rig.bin / "rivet"), str(rig.config)
    for argv, said in (
        (["run", "--config", cfg, "--bogus"], "unexpected argument '--bogus'"),
        (["plan", "--CONFIG", cfg], "unexpected argument '--CONFIG'"),
        (["XXplanXX", "--config", cfg], "unrecognized subcommand 'XXplanXX'"),
    ):
        done = subprocess.run([rivet, *argv], capture_output=True, text=True, cwd=rig.tmp)
        assert (done.returncode, said in done.stderr) == (2, True), (argv, done.stderr)
    done = subprocess.run([rivet, "run", "--config", cfg, "-p", "A=1", "--param=B=2", "--json-errors"], cwd=rig.tmp)
    assert done.returncode == 0
