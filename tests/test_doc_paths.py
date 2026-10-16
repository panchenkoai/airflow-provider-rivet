"""What the README and the workflows cite, held to what exists: repo and rivet paths, and the gate's options."""

from __future__ import annotations

import fnmatch
import re
import subprocess

from conftest import PACKAGE_ROOT, RIVET_SRC

README = (PACKAGE_ROOT / "README.md").read_text()
CITING = [PACKAGE_ROOT / "README.md", *sorted((PACKAGE_ROOT / ".github" / "workflows").glob("*.yml"))]
# A path inside this repo or inside rivet, by its first component; `<placeholder>` paths are layouts, not files.
CITED = re.compile(r"(?<![\w/.<-])(?:airflow_provider_rivet|dev|docs|examples|gate|src|tests)/[A-Za-z0-9_.*/-]*[A-Za-z0-9_*]")


def rivet_has(path: str) -> bool:
    """Whether rivet's tree at RIVET_SRC holds the path; read from git, so CI's sparse checkout answers for every path."""
    parent, _, name = path.rpartition("/")
    out = subprocess.run(
        ["git", "-C", str(RIVET_SRC), "ls-tree", "--name-only", "HEAD", f"{parent}/"],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        return any(RIVET_SRC.glob(path))
    return any(fnmatch.fnmatch(entry.rpartition("/")[2], name) for entry in out.stdout.splitlines())


def section(heading: str) -> str:
    """The README text under one `## ` heading, up to the next one."""
    start = README.index(heading + "\n")
    stop = README.find("\n## ", start + 1)
    return README[start : stop if stop != -1 else len(README)]


def test_d37_every_path_the_readme_and_the_workflows_cite_exists() -> None:
    """D37: the README and gate.yml cited `dev/release_oracle/airflow_provider.py` (and its `.ref`) as rivet's release
    gate hook; rivet has never had that file, so no rivet release candidate is graded by this gate."""
    missing = []
    for doc in CITING:
        for path in sorted(set(CITED.findall(doc.read_text()))):
            path = path.rstrip("/")
            if not any(PACKAGE_ROOT.glob(path)) and not rivet_has(path):
                missing.append(f"{doc.relative_to(PACKAGE_ROOT)}: {path}")
    assert not missing, f"cited paths that exist neither here nor in rivet ({RIVET_SRC}): {missing}"


def test_d39_the_live_gate_section_names_every_gate_option() -> None:
    """D39: `python -m gate --warehouses ...` (the warehouse leg) and its cloud options were in no README text."""
    options = re.findall(r'add_argument\(\s*"(--[a-z-]+)"', (PACKAGE_ROOT / "gate" / "__main__.py").read_text())
    assert "--warehouses" in options
    text = section("## Live gate")
    missing = [o for o in options if f"`{o}" not in text and f" {o} " not in text]
    assert not missing, f"the README's Live gate section does not name {missing}"
