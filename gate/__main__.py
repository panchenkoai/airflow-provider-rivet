"""python -m gate --rivet-bin <linux rivet> [--airflow 2.10.5,3.3.2] [--verdicts out.jsonl] [--keep] [--sabotage drop-delta-file]

Exit 0 only when every cell ran and passed; each stack is torn down (volumes too) before and after it runs.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

from .scenario import Verdicts, play
from .stand import FLAVORS, Stand


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m gate", description=__doc__.splitlines()[0])
    ap.add_argument("--rivet-bin", required=True, type=Path, help="a rivet Linux binary for the docker host's architecture")
    ap.add_argument("--airflow", default=",".join(FLAVORS), help="comma-separated Airflow versions")
    ap.add_argument("--work", type=Path, help="working directory (default: a fresh temporary one)")
    ap.add_argument("--verdicts", type=Path, help="write one JSON line per graded cell")
    ap.add_argument("--keep", action="store_true", help="leave the stacks up and the working directory in place")
    ap.add_argument("--sabotage", choices=["drop-delta-file"], help="break one output on purpose: the gate must go red")
    args = ap.parse_args(argv)

    versions = [v.strip() for v in args.airflow.split(",") if v.strip()]
    unknown = sorted(set(versions) - set(FLAVORS))
    if unknown:
        ap.error(f"unknown Airflow version(s) {unknown}; known: {sorted(FLAVORS)}")
    work = args.work or Path(tempfile.mkdtemp(prefix="rivet-airflow-gate-"))
    work.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, str]] = []
    for version in versions:
        stand = Stand(version, args.rivet_bin.resolve(), work.resolve())
        verdicts = Verdicts(version)
        try:
            verdicts.guard("stand-up", stand.up)
            if not verdicts.rows:
                verdicts.guard("scenario", lambda: play(stand, verdicts, args.sabotage))
        finally:
            if not args.keep:
                verdicts.guard("teardown", stand.down)
        rows += verdicts.rows
    if not args.keep and args.work is None:
        shutil.rmtree(work, ignore_errors=True)
    if args.verdicts:
        args.verdicts.write_text("".join(json.dumps(r) + "\n" for r in rows))
    failed = [r for r in rows if r["status"] != "PASS"]
    print(f"airflow gate: {len(rows) - len(failed)} PASS, {len(failed)} FAIL over {len(versions)} stack(s)")
    return 0 if rows and not failed else 1


if __name__ == "__main__":
    sys.exit(main())
