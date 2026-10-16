"""What the README and the CHANGELOG say, held to the code: parameters, connection kinds, and the warehouse compaction."""

from __future__ import annotations

import inspect
import re
from pathlib import Path

from conftest import Rig

from airflow_provider_rivet import dags
from airflow_provider_rivet.connections import KINDS
from airflow_provider_rivet.operators import RivetBaseOperator, RivetCompactOperator

ROOT = Path(__file__).resolve().parents[1]
README = (ROOT / "README.md").read_text()
CHANGELOG = (ROOT / "CHANGELOG.md").read_text()
# What real rivet prints for a table of a Snowflake or ClickHouse export, then exits 0 (src/load/compact.rs,
# `compact_skip_reason` and its `eprintln!`): rivet 0.31.0 and main have no `--summary-output` on compact.
WAREHOUSE_COMPACT_SKIP = (
    "  compact [orders]: skipped — this warehouse keeps a change log behind a view and never compacts; nothing to merge"
)


def section(heading: str) -> str:
    """The README text under one heading, up to the next heading of the same or a higher level."""
    level = heading.split(" ", 1)[0]
    start = README.index(heading + "\n") + len(heading) + 1
    stop = re.compile(rf"^#{{1,{len(level)}}} ", re.M).search(README, start)
    return README[start : stop.start() if stop else len(README)]


def keyword_parameters(fn: object) -> list[str]:
    """The named parameters a caller passes by keyword."""
    kinds = (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    return [p.name for p in inspect.signature(fn).parameters.values() if p.kind in kinds and p.name not in ("self", "dag_id")]


def named(text: str, name: str) -> bool:
    """Whether a text names a parameter: as code (`name`, `name=...`) or as a keyword in an example."""
    return re.search(rf"`{name}(=[^`]*)?`|\b{name}=", text) is not None


def test_d30_a_warehouse_compact_on_todays_rivet_ends_compacted_as_the_readme_says(rig: Rig) -> None:
    """D30: on Snowflake and ClickHouse rivet passes every table by and exits 0 with no machine-readable status, so the
    task is green as `compacted` having merged nothing. The README said it "is skipped with the reason rivet gives"."""
    rig.scenario(compact={"stderr": WAREHOUSE_COMPACT_SKIP, "exit": 0})
    payload, _ = rig.run(rig.operator(RivetCompactOperator, export="orders"))
    assert isinstance(payload, dict), f"the task did not succeed: {payload!r}"
    assert [u["status"] for u in payload["units"]] == ["compacted"] and payload["skip_reason"] is None
    assert "load_result" in payload["degraded"]
    text = section("## Storage and warehouse connections")
    assert "compact task is skipped" not in README, "README claims a skip today's binary never produces"
    assert "`compacted`" in text and "`compact=False`" in text, "README must say the task ends `compacted` and how to drop it"
    row = next(line for line in section("## Operators").splitlines() if line.startswith("| `RivetCompactOperator`"))
    assert "`load_result`" in row, "the skip-to-Airflow-skip note holds only for a binary with compact's summary"


def test_d30_every_builder_parameter_is_named_in_its_readme_section() -> None:
    """D30: `build_cdc_dag(compact=...)`, the way to drop the compaction no-op, was in no README text."""
    missing = [
        f"{fn.__name__}({p}=)"
        for fn, heading in (
            (dags.build_batch_dag, "### Batch: `build_batch_dag`"),
            (dags.build_cdc_dag, "### CDC: `build_cdc_dag`"),
        )
        for p in keyword_parameters(fn)
        if not named(section(heading), p)
    ]
    assert not missing, f"the README's builder sections do not name {missing}"


def test_d39_the_operator_parameter_list_names_every_init_parameter() -> None:
    """D39: the README's "All of them take" list left out `connections`, `table` and `plan_file`."""
    text = section("## Operators")
    listed = text[text.index("All of them take") : text.index("Templated:")]
    missing = [p for p in keyword_parameters(RivetBaseOperator.__init__) if not named(listed, p)]
    assert not missing, f"the README's parameter list does not name {missing}"


def test_d39_the_changelog_names_connections_and_every_connection_kind() -> None:
    """D39: `connections=` (storage and warehouse Connections) shipped with no CHANGELOG entry."""
    entry = next((p for p in CHANGELOG.split("\n- ") if "`connections=" in p), None)
    assert entry is not None, "the CHANGELOG has no `connections=` entry"
    missing = [k for k in KINDS if k not in entry.lower()]
    assert not missing, f"the `connections=` entry does not name {missing}"
