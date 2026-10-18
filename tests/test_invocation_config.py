"""The config side of an invocation at its edges: query files copied beside a config's copy (and every refusal's
text), a plan's wave layout, the exports a narrowed copy keeps, the narrowed copy's refusal, and a foreign sealed plan.
Mutant ids are the mutmut names of airflow_provider_rivet/invocation.py."""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow_provider_rivet import invocation
from airflow_provider_rivet.preflight import PreflightRefusal

# --- query files ----------------------------------------------------------------------------------------------------


def copy(tmp_path: Path, exports: list, touched: str | None = None) -> Path:
    folder = tmp_path / "copies"
    folder.mkdir(exist_ok=True)
    invocation._copy_query_files({"exports": exports}, tmp_path / "src" / "rivet.yaml", folder, touched)
    return folder


@pytest.fixture
def src(tmp_path: Path) -> Path:
    (tmp_path / "src" / "sql").mkdir(parents=True)
    (tmp_path / "src" / "sql" / "a.sql").write_text("select 1")
    (tmp_path / "src" / "sql" / "b.sql").write_text("select 2")
    return tmp_path / "src"


def test_entries_without_a_query_file_or_not_objects_are_skipped_and_the_rest_copied(tmp_path: Path, src: Path) -> None:
    """_copy_query_files 15/20/48: a non-object entry, an export without `query_file` and one whose file is missing
    but is not this task's export come first; the later exports' files are still copied."""
    exports = [
        "junk",
        {"name": "plain"},
        {"name": "gone", "query_file": "sql/none.sql"},
        {"name": "a", "query_file": "sql/a.sql"},
    ]
    folder = copy(tmp_path, exports, touched="a")
    assert (folder / "sql" / "a.sql").read_text() == "select 1"


def refusal(tmp_path: Path, exports: list, touched: str | None = None) -> str:
    with pytest.raises(PreflightRefusal) as refused:
        copy(tmp_path, exports, touched)
    assert refused.value.code == "RIVET_AIRFLOW_QUERY_FILE"
    return str(refused.value)


def test_each_query_file_refusal_says_what_is_wrong(tmp_path: Path, src: Path) -> None:
    """_copy_query_files 40-42/45-47/50-52."""
    source = src / "rivet.yaml"
    head = f"[RIVET_AIRFLOW_QUERY_FILE] export `e` of {source}: query_file"
    assert refusal(tmp_path, [{"name": "e", "query_file": "../x.sql"}]) == (
        f"{head} `../x.sql` must be a relative path with no `..`: rivet reads it only inside the config's directory"
    )
    assert refusal(tmp_path, [{"name": "e", "query_file": "sql/none.sql"}]) == (
        f"{head} `sql/none.sql` is not a file beside the config"
    )
    (tmp_path / "outside.sql").write_text("x")
    os.symlink(tmp_path / "outside.sql", src / "sql" / "link.sql")
    assert refusal(tmp_path, [{"name": "e", "query_file": "sql/link.sql"}]) == (
        f"{head} `sql/link.sql` resolves outside the config's directory, which rivet refuses"
    )


def test_a_query_file_path_another_config_directory_claimed_is_refused(tmp_path: Path, src: Path) -> None:
    """_copy_query_files 61: the owners file names another directory for this relative path."""
    folder = tmp_path / "copies"
    folder.mkdir()
    (folder / invocation.QUERY_OWNERS).write_text(json.dumps({"sql/a.sql": "/elsewhere"}))
    head = f"[RIVET_AIRFLOW_QUERY_FILE] export `e` of {src / 'rivet.yaml'}: query_file `sql/a.sql`"
    assert refusal(tmp_path, [{"name": "e", "query_file": "sql/a.sql"}]) == (
        f"{head} is already used in {folder} by a config of /elsewhere; two config directories that share a state "
        "directory cannot use one relative path: give them separate state directories"
    )


def test_the_owners_file_is_written_with_sorted_keys(tmp_path: Path, src: Path) -> None:
    """_copy_query_files 83/85/86: claimed in config order b, a; written sorted."""
    folder = copy(tmp_path, [{"name": "b", "query_file": "sql/b.sql"}, {"name": "a", "query_file": "sql/a.sql"}])
    owners = {"sql/a.sql": str(src), "sql/b.sql": str(src)}
    assert (folder / invocation.QUERY_OWNERS).read_text() == json.dumps(owners, sort_keys=True)


# --- the plan's waves -----------------------------------------------------------------------------------------------


def single(name: object = "orders", **rec: object) -> dict:
    rec = {"export_name": name, "recommended_wave": 2, **rec}
    return {"export_name": name, "prioritization": {"export_recommendation": rec}}


def test_a_single_plan_takes_its_cost_from_its_recommendation() -> None:
    """plan_layout 31/32: `low` is not heavy, anything else is."""
    assert invocation.plan_layout(single(cost_class="low")) == [{"wave": 2, "exports": ["orders"], "heavy": []}]
    assert invocation.plan_layout(single()) == [{"wave": 2, "exports": ["orders"], "heavy": ["orders"]}]


def test_a_recommendation_without_its_export_name_is_not_a_plan() -> None:
    plan = single()
    del plan["prioritization"]["export_recommendation"]["export_name"]
    with pytest.raises(KeyError):
        invocation.plan_layout(plan)


@pytest.mark.parametrize(
    "plan",
    [{**single(), "export_name": 5}, single(export_name="users") | {"export_name": "orders"}],
    ids=["name-not-text", "other-export"],
)
def test_a_single_plan_naming_another_export_is_refused(plan: dict) -> None:
    """plan_layout 9/11/15/16/18-20."""
    with pytest.raises(ValueError, match="^the plan's recommendation names another export$"):
        invocation.plan_layout(plan)


def test_a_campaign_without_ordered_exports_makes_every_export_heavy() -> None:
    """plan_layout 46/48."""
    plan = [{"prioritization": {"campaign": {"waves": [{"wave": 1, "exports": ["a"]}]}}}]
    assert invocation.plan_layout(plan) == [{"wave": 1, "exports": ["a"], "heavy": ["a"]}]


def test_a_campaign_whose_waves_are_empty_is_refused() -> None:
    """plan_layout 82-84."""
    plan = [{"prioritization": {"campaign": {"waves": [{"wave": 1, "exports": []}], "ordered_exports": []}}}]
    with pytest.raises(ValueError, match="^the plan has no wave with an export$"):
        invocation.plan_layout(plan)


# --- the narrowed copy ----------------------------------------------------------------------------------------------


def test_narrowing_to_an_export_the_config_lacks_keeps_nothing() -> None:
    """narrowed_exports 12."""
    assert invocation.narrowed_exports([{"name": "a"}], "missing") == set()


def test_an_auto_backfill_keeps_the_batch_recipes_of_its_tables_not_other_streams() -> None:
    """narrowed_exports 40-46: a second stream over the same table is no recipe."""
    exports = [
        {"name": "stream", "mode": "cdc", "tables": ["orders"], "cdc": {"backfill": "auto"}},
        {"name": "orders_batch", "mode": "full", "table": "orders"},
        {"name": "other_stream", "mode": "cdc", "table": "orders"},
        {"name": "users_batch", "mode": "full", "table": "users"},
    ]
    assert invocation.narrowed_exports(exports, "stream") == {0, 1}


def test_a_cut_that_reads_back_as_another_document_is_refused_with_the_remedy(tmp_path: Path) -> None:
    """narrowed_config 21-23/33: the text's `a` holds a field the parsed config does not, so the cut cannot match."""
    text = "exports:\n  - name: a\n    x: 1\n  - name: b\n"
    pf = SimpleNamespace(config={"exports": [{"name": "a"}, {"name": "b"}]}, config_text=text, value_of=lambda name: None)
    with pytest.raises(PreflightRefusal) as refused:
        invocation.narrowed_config(pf, "/cfg/rivet.yaml", "a")
    assert str(refused.value) == (
        "[RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE] this rivet has no `--export` on load/compact, so the task loads from a copy "
        "of /cfg/rivet.yaml cut down to `a`, and the text cannot be cut safely: the cut text does not read back as the "
        "chosen exports: the narrowed document differs from the chosen exports of the original. Write `exports:` as a "
        "plain list whose items each start their own `- name:` line, with no anchor another item uses"
    )


# --- a foreign sealed plan ------------------------------------------------------------------------------------------


def test_a_sealed_plan_of_another_export_is_refused_and_says_so(tmp_path: Path) -> None:
    """_refuse_foreign_artifact 29-31."""
    config = tmp_path / "rivet.yaml"
    config.write_text("")
    artifact = tmp_path / "p.json"
    artifact.write_text(json.dumps({"export_name": "users", "config_path": str(config)}))
    with pytest.raises(PreflightRefusal) as refused:
        invocation._refuse_foreign_artifact(artifact, "orders", str(config))
    assert str(refused.value) == (
        f"[RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN] the sealed plan artifact {artifact} records export `users` of config "
        f"`{config}`, and this task applies export `orders` of config `{config}`: applying it would extract something "
        "else under this task's name. It is neither applied nor replaced; delete the file once you know what wrote it"
    )
