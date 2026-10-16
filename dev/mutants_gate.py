"""The mutation gate of CI and the release (README, "Mutation testing"); run inside the project environment.

    python dev/mutants_gate.py changed [BASE]        # the mutmut name globs of the functions `git diff BASE...HEAD` touches
    python dev/mutants_gate.py pr [BASE]             # run those mutants; fail on any that is not killed
    python dev/mutants_gate.py full                  # run every mutant; fail when more survive than `.mutmut-ratchet`
    python dev/mutants_gate.py full OUT --shard D    # one of ten shards (mutant numbers ending in D); survivors to OUT
    python dev/mutants_gate.py ratchet DIR           # the ten shard files under DIR, judged against the ratchet
    python dev/mutants_gate.py pr [BASE] --shard D   # the PR gate, one shard

CI runs the shards in parallel: a mutant of a function every test passes through (`_Session.step`) runs the whole
suite when it survives, so one runner would need most of a day for the package.

A mutant "survives" here when mutmut reports anything but `killed`: `survived`, `no tests`, `timeout`,
`suspicious`, a crash. A timeout is not a detection: it is a loop or a wait no test bounds.
"""

from __future__ import annotations

import ast
import fnmatch
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "airflow_provider_rivet"
RATCHET = ROOT / ".mutmut-ratchet"
MUTMUT = [sys.executable, "-m", "mutmut"]
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def changed_lines(base: str) -> dict[str, set[int]]:
    """Lines of HEAD's package files that the diff from the merge base adds or changes; a pure deletion marks the line
    after it, so a function whose line was only removed is still touched."""
    diff = subprocess.run(
        ["git", "diff", "--unified=0", "--no-color", f"{base}...HEAD", "--", f"{PACKAGE}/*.py"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout  # fmt: skip
    lines: dict[str, set[int]] = {}
    path = None
    for line in diff.splitlines():
        if line.startswith("+++ "):
            path = None if line == "+++ /dev/null" else line[len("+++ b/") :]
        elif path and (hunk := HUNK.match(line)):
            start, count = int(hunk[1]), int(hunk[2] if hunk[2] is not None else 1)
            lines.setdefault(path, set()).update(range(start, start + max(count, 1)))
    return lines


def mangled_functions(path: str) -> list[tuple[str, int, int]]:
    """(mutmut's name, first line, last line) of every function mutmut mutates in one file: top-level functions
    (`x_name`) and methods (`xǁClassǁname`), decorators included; a nested function belongs to its enclosing one."""
    tree = ast.parse((ROOT / path).read_text(), filename=path)
    found = []

    def span(node: ast.AST) -> tuple[int, int]:
        first = min([node.lineno, *(d.lineno for d in getattr(node, "decorator_list", []))])
        return first, node.end_lineno or node.lineno

    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            found.append((f"x_{node.name}", *span(node)))
        elif isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                    found.append((f"xǁ{node.name}ǁ{item.name}", *span(item)))
    return found


def changed_globs(base: str) -> list[str]:
    """`<module>.<mangled function>__mutmut_*` for every function the diff touches."""
    globs = []
    for path, lines in sorted(changed_lines(base).items()):
        if not (ROOT / path).is_file():
            continue
        module = path.removesuffix(".py").removesuffix("/__init__").replace("/", ".")
        for name, first, last in mangled_functions(path):
            if lines & set(range(first, last + 1)):
                globs.append(f"{module}.{name}__mutmut_*")
    return globs


PROGRESS = re.compile(r"(\d+)/(\d+)(?=\s)")


def statuses() -> dict[str, str]:
    """Every mutant mutmut generated, with its status (`not checked` until a run grades it), from mutants/*.meta."""
    from mutmut.stats import status_by_exit_code

    found = {}
    for meta in sorted((ROOT / "mutants" / PACKAGE).rglob("*.meta")):
        for name, code in json.loads(meta.read_text())["exit_code_by_key"].items():
            found[name] = status_by_exit_code[code]
    return found


def matching(names, globs: list[str]) -> list[str]:
    return [n for n in names if any(fnmatch.fnmatchcase(n, g) for g in globs)]


def run_mutmut(args: list[str], label: str = "", globs: list[str] | None = None) -> subprocess.CompletedProcess:
    """`mutmut run ...` with the project's own settings (`[tool.mutmut]`, `dev/` for the isolation plugin).

    With `globs`, once mutmut has generated its mutants this prints how many of all of them the run covers, and
    mutmut's progress `done/total` (total = every mutant of the package) is shown against that number instead."""
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [str(ROOT / "dev"), os.environ.get("PYTHONPATH")]))}
    if env.get("RIVET_SRC"):  # mutmut's workers do not run in this directory: a relative checkout path would miss
        env["RIVET_SRC"] = str((ROOT / env["RIVET_SRC"]).resolve())
    children = os.environ.get("MUTANTS_MAX_CHILDREN")
    argv = [*MUTMUT, "run", *(["--max-children", children] if children else []), *args]
    print("+", " ".join(argv), flush=True)
    proc = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    seen, pending, count = [], "", None
    for chunk in iter(lambda: proc.stdout.read(4096), ""):  # streamed: a full run is silent for an hour otherwise
        seen.append(chunk)
        pending += chunk
        *lines, pending = re.split(r"(\r|\n)", pending + "\x00")
        pending = pending[:-1]
        out = []
        for piece in lines:
            if globs is not None and count is None and "files mutated" in piece:
                every = statuses()
                count = len(matching(every, globs))
                out.append(f"{piece}\nmutation gate: {label}: {count} of {len(every)} mutants\n")
                continue
            if count is not None and PROGRESS.search(piece) and "\U0001f389" in piece:
                piece = f"{label}: " + PROGRESS.sub(lambda m, n=count: f"{m[1]}/{n}", piece, count=1)
            out.append(piece)
        sys.stdout.write("".join(out))
        sys.stdout.flush()
    sys.stdout.write(pending)
    return subprocess.CompletedProcess(argv, proc.wait(), "".join(seen)[-20000:], "")


def tally(globs: list[str], label: str) -> tuple[dict[str, str], list[str], int]:
    """(not-killed mutants by name, the names graded, how many were killed) of the run's mutants; printed."""
    graded = {n: s for n, s in statuses().items() if any(fnmatch.fnmatchcase(n, g) for g in globs)}
    tested = sorted(n for n, s in graded.items() if s != "not checked")
    killed = sum(1 for s in graded.values() if s == "killed")
    left = {n: s for n, s in graded.items() if s != "killed"}
    print(f"mutation gate: {label}: tested {len(tested)}, killed {killed}, not killed {len(left)}", flush=True)
    return left, tested, killed


def show(names: list[str]) -> None:
    """The diff of each named mutant."""
    for name in names:
        print(subprocess.run([*MUTMUT, "show", name], cwd=ROOT, capture_output=True, text=True).stdout, flush=True)


def sharded(globs: list[str], shard: int | None) -> list[str]:
    """The globs narrowed to one of ten shards: the mutants whose number ends in the shard's digit."""
    return globs if shard is None else [f"{g}{shard}" for g in globs]


def gate_pr(base: str, shard: int | None) -> int:
    """Run the mutants of the touched functions; every one must be killed."""
    globs = sharded(changed_globs(base), shard)
    if not globs:
        print(f"mutation gate: the diff from {base} touches no function of {PACKAGE}/; nothing to mutate")
        return 0
    print("mutation gate: functions touched:\n  " + "\n  ".join(globs), flush=True)
    label = "touched functions" if shard is None else f"shard {shard}"
    done = run_mutmut(globs, label, globs)
    if done.returncode != 0:
        if "nothing matches" in done.stdout:
            print("mutation gate: the touched functions have no mutants in this shard")
            return 0
        print(f"mutation gate: mutmut run exited {done.returncode}")
        return 1
    left, _, _ = tally(globs, label)
    if left:
        print(f"mutation gate: {len(left)} mutant(s) of the touched functions not killed:")
        for name, status in sorted(left.items()):
            print(f"  {status:10} {name}")
        show(sorted(left))
        print("Kill each with a test (or show it is equivalent and restructure the code so no such mutant exists).")
        return 1
    print("mutation gate: every mutant of the touched functions is killed")
    return 0


def read_ratchet() -> int:
    """The committed number: the first line of `.mutmut-ratchet` that is not a comment."""
    return int(next(line for line in RATCHET.read_text().splitlines() if line.strip() and not line.startswith("#")))


def judge(left: dict[str, str]) -> int:
    """Hold the not-killed mutants of the whole package to the ratchet."""
    ratchet = read_ratchet()
    by_status: dict[str, int] = {}
    for status in left.values():
        by_status[status] = by_status.get(status, 0) + 1
    print("mutation gate: not killed by status: " + ", ".join(f"{k} {v}" for k, v in sorted(by_status.items())))
    verdict = "<=" if len(left) <= ratchet else ">"
    print(f"mutation gate: survivors {len(left)} {verdict} ratchet {ratchet}")
    if len(left) > ratchet:
        print("More mutants survive than `.mutmut-ratchet` allows; kill the new ones with tests:")
        for name, status in sorted(left.items()):
            print(f"  {status:10} {name}")
        return 1
    if len(left) < ratchet:
        print(f"mutation gate: {ratchet - len(left)} fewer than the ratchet; lower `.mutmut-ratchet` to {len(left)}")
    return 0


def gate_full(shard: int | None, out: Path | None) -> int:
    """Run every mutant (of one shard); judge against the ratchet, or write the shard's account to `out`."""
    globs = sharded([f"{PACKAGE}.*__mutmut_*"], shard)
    label = "all mutants" if shard is None else f"shard {shard}"
    done = run_mutmut(globs if shard is not None else [], label, globs)
    if done.returncode != 0:
        print(f"mutation gate: mutmut run exited {done.returncode}")
        return 1
    left, tested, killed = tally(globs, label)
    if shard is None:
        return judge(left)
    assert out is not None
    every = statuses()
    doc = {
        "shard": shard,
        "total_mutants": len(every),
        "mutants": len(matching(every, globs)),
        "tested": tested,
        "killed": killed,
        "not_killed": left,
    }
    out.write_text(json.dumps(doc, indent=1, sort_keys=True))
    print(f"mutation gate: shard {shard}: account written to {out}")
    return 0


def gate_ratchet(folder: Path) -> int:
    """Check that the ten shards together graded every mutant exactly once, then judge their total."""
    docs = {}
    for path in sorted(folder.rglob("*.json")):
        doc = json.loads(path.read_text())
        docs[doc["shard"]] = doc
    if set(docs) != set(range(10)):
        print(f"mutation gate: shards {sorted(set(range(10)) - set(docs))} reported nothing; the run is incomplete")
        return 1
    print("| shard | mutants | tested | killed | not killed |\n|---|---|---|---|---|")
    for i, doc in sorted(docs.items()):
        print(f"| {i} | {doc['mutants']} | {len(doc['tested'])} | {doc['killed']} | {len(doc['not_killed'])} |")
    totals = {doc["total_mutants"] for doc in docs.values()}
    tested = [name for doc in docs.values() for name in doc["tested"]]
    total = next(iter(totals))
    print(
        f"| all | {sum(d['mutants'] for d in docs.values())} | {len(tested)} | "
        f"{sum(d['killed'] for d in docs.values())} | {sum(len(d['not_killed']) for d in docs.values())} |"
    )
    problems = []
    if len(totals) != 1:
        problems.append(f"the shards saw different totals {sorted(totals)}: they ran different code")
    if len(set(tested)) != len(tested):
        problems.append(f"{len(tested) - len(set(tested))} mutants were graded by more than one shard")
    if len(set(tested)) != total:
        problems.append(f"sum(tested) {len(set(tested))} != total mutants {total}: some mutants were graded by no shard")
    for problem in problems:
        print(f"mutation gate: {problem}")
    if problems:
        return 1
    print(f"mutation gate: sum(tested) == total mutants == {total}")
    left: dict[str, str] = {}
    for doc in docs.values():
        left.update(doc["not_killed"])
    return judge(left)


def main(argv: list[str]) -> int:
    shard = None
    if "--shard" in argv:
        i = argv.index("--shard")
        shard = int(argv[i + 1])
        assert 0 <= shard <= 9, "a shard is the last digit of the mutant number, 0-9"
        argv = argv[:i] + argv[i + 2 :]
    mode = argv[0] if argv else ""
    if mode == "changed":
        print("\n".join(changed_globs(argv[1] if len(argv) > 1 else "origin/main")))
        return 0
    if mode == "pr":
        return gate_pr(argv[1] if len(argv) > 1 else "origin/main", shard)
    if mode == "full":
        return gate_full(shard, Path(argv[1]) if len(argv) > 1 else None)
    if mode == "ratchet":
        return gate_ratchet(Path(argv[1]))
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
