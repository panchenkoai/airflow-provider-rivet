"""What the docs (README and docs/) and the workflows cite, held to what exists: repo and rivet paths, and the gate's options."""

from __future__ import annotations

import fnmatch
import os
import re
import subprocess

from conftest import DOC_FILES, PACKAGE_ROOT, RIVET_SRC, doc_section

CITING = [*DOC_FILES, *sorted((PACKAGE_ROOT / ".github" / "workflows").glob("*.yml"))]
# A path inside this repo or inside rivet, by its first component; `<placeholder>` paths are layouts, not files.
CITED = re.compile(r"(?<![\w/.<-])(?:airflow_provider_rivet|dev|docs|examples|gate|src|tests)/[A-Za-z0-9_.*/-]*[A-Za-z0-9_*]")


def rivet_has(path: str) -> bool:
    """Whether rivet's tree at RIVET_SRC holds the path; read from git, so CI's sparse checkout answers for every path."""
    parent, _, name = path.rpartition("/")
    out = subprocess.run(
        ["git", "-C", str(RIVET_SRC), "ls-tree", "--name-only", "HEAD", f"{parent}/"],
        capture_output=True,
        text=True,
        # A git hook (pre-push runs this suite) exports GIT_DIR and friends, which would point -C at this repo.
        env={k: v for k, v in os.environ.items() if not k.startswith("GIT_")},
    )
    if out.returncode != 0:
        return any(RIVET_SRC.glob(path))
    return any(fnmatch.fnmatch(entry.rpartition("/")[2], name) for entry in out.stdout.splitlines())


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
    text = doc_section("## Live gate")
    missing = [o for o in options if f"`{o}" not in text and f" {o} " not in text]
    assert not missing, f"the Live gate section of docs/development.md does not name {missing}"


def slug(heading: str) -> str:
    """The anchor GitHub gives a heading: lower case, punctuation other than `-` and `_` dropped, spaces to `-`."""
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


def test_every_link_between_doc_pages_reaches_a_page_and_a_heading() -> None:
    """The README was split into docs/ pages that link to each other by relative path and anchor; a renamed heading
    or page would leave a link that opens the page top or a 404."""
    broken = []
    for doc in DOC_FILES:
        for target in re.findall(r"\]\(([^)\s]+)\)", doc.read_text()):
            if re.match(r"[a-z]+:", target):
                continue
            path, _, anchor = target.partition("#")
            page = (doc.parent / path).resolve() if path else doc
            if not page.exists():
                broken.append(f"{doc.relative_to(PACKAGE_ROOT)}: {target}")
            elif anchor and page.suffix == ".md":
                headings = re.findall(r"^#+ (.+)$", page.read_text(), re.M)
                if anchor not in {slug(h) for h in headings}:
                    broken.append(f"{doc.relative_to(PACKAGE_ROOT)}: {target}")
    assert not broken, f"links that reach no page or no heading: {broken}"


def test_the_readme_links_every_doc_page_by_an_absolute_url() -> None:
    """PyPI shows the README alone, so the front page reaches each docs/ page by a github.com URL, and every page is
    reachable from it."""
    readme = DOC_FILES[0].read_text()
    pages = [p.name for p in DOC_FILES[1:]]
    missing = [n for n in pages if f"github.com/panchenkoai/airflow-provider-rivet/blob/main/docs/{n})" not in readme]
    assert not missing, f"README links no page {missing}"


def test_the_version_badge_is_the_package_version() -> None:
    """The front page's version badge is a static shields.io badge until the package is on PyPI."""
    from airflow_provider_rivet import __version__

    assert f"badge/version-{__version__}%20" in DOC_FILES[0].read_text()
