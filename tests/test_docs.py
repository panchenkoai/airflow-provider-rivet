"""What the docs (README and docs/) and the CHANGELOG say, held to the code: parameters, connection kinds, compaction."""

from __future__ import annotations

import inspect
import re
from pathlib import Path

from conftest import DOCS, Rig, doc_section

from airflow_provider_rivet import dags
from airflow_provider_rivet.connections import KINDS
from airflow_provider_rivet.operators import RivetBaseOperator, RivetCompactOperator

ROOT = Path(__file__).resolve().parents[1]
CHANGELOG = (ROOT / "CHANGELOG.md").read_text()
# What real rivet prints for a table of a Snowflake or ClickHouse export, then exits 0 (src/load/compact.rs,
# `compact_skip_reason` and its `eprintln!`): rivet 0.31.0 and main have no `--summary-output` on compact.
WAREHOUSE_COMPACT_SKIP = (
    "  compact [orders]: skipped — this warehouse keeps a change log behind a view and never compacts; nothing to merge"
)


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
    text = doc_section("## Storage and warehouse connections")
    assert "compact task is skipped" not in DOCS, "the docs claim a skip today's binary never produces"
    assert "`compacted`" in text and "`compact=False`" in text, "the docs must say the task ends `compacted` and how to drop it"
    row = next(line for line in doc_section("## Operators").splitlines() if line.startswith("| `RivetCompactOperator`"))
    assert "`load_result`" in row, "the skip-to-Airflow-skip note holds only for a binary with compact's summary"


def test_d30_every_builder_parameter_is_named_in_its_doc_section() -> None:
    """D30: `build_cdc_dag(compact=...)`, the way to drop the compaction no-op, was in no README text."""
    missing = [
        f"{fn.__name__}({p}=)"
        for fn, heading in (
            (dags.build_batch_dag, "### Batch: `build_batch_dag`"),
            (dags.build_cdc_dag, "### CDC: `build_cdc_dag`"),
        )
        for p in keyword_parameters(fn)
        if not named(doc_section(heading), p)
    ]
    assert not missing, f"the builder sections of docs/dag-builders.md do not name {missing}"


def test_d39_the_operator_parameter_table_has_every_init_parameter_its_default_and_templating() -> None:
    """D39: the README's "All of them take" list left out `connections`, `table` and `plan_file`. The table in
    docs/operators.md has one row per parameter, with the signature's default and the class's template fields."""
    rows = {}
    for line in doc_section("## Operators").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 4 and re.fullmatch(r"`\w+`", cells[0]):
            rows[cells[0].strip("`")] = cells
    signature = inspect.signature(RivetBaseOperator.__init__).parameters
    for name in keyword_parameters(RivetBaseOperator.__init__):
        assert name in rows, f"docs/operators.md has no row for `{name}`"
        default = signature[name].default
        expected = "required" if default is inspect.Parameter.empty else f"`{default!r}`".replace("'", '"')
        assert rows[name][1] == expected, f"`{name}`: the table says {rows[name][1]}, the signature {expected}"
        templated = "yes" if name in RivetBaseOperator.template_fields else "no"
        assert rows[name][2] == templated, f"`{name}`: templated is {templated}"
    assert set(rows) == set(keyword_parameters(RivetBaseOperator.__init__)), "a row names no parameter"


def test_d39_the_changelog_names_connections_and_every_connection_kind() -> None:
    """D39: `connections=` (storage and warehouse Connections) shipped with no CHANGELOG entry."""
    entry = next((p for p in CHANGELOG.split("\n- ") if "`connections=" in p), None)
    assert entry is not None, "the CHANGELOG has no `connections=` entry"
    missing = [k for k in KINDS if k not in entry.lower()]
    assert not missing, f"the `connections=` entry does not name {missing}"
