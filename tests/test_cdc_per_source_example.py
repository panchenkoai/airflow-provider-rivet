"""examples/rivet_cdc_per_source.py: one CDC DAG per source, its export and tables read from that source's config."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "rivet_cdc_per_source.py"
CONFIG = """\
source:
  type: mysql
  url_env: RIVET_MYSQL_URL
exports:
  - name: {src}_orders
    table: {src}.orders
    mode: incremental
  - name: cdc
    tables: [orders, customers]
    mode: cdc
    cdc:
      checkpoint: ./cdc/{src}_cdc.ckpt
"""


def load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sources: str):
    monkeypatch.setenv("RIVET_EXAMPLE_SOURCES", sources)
    monkeypatch.setenv("RIVET_EXAMPLE_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("RIVET_EXAMPLE_STATE_ROOT", "/srv/rivet-state")
    spec = importlib.util.spec_from_file_location("rivet_cdc_per_source", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_each_source_gets_its_own_dag_state_dir_and_connections(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for src in ("shop", "billing"):
        (tmp_path / f"{src}.yaml").write_text(CONFIG.format(src=src))
    module = load(tmp_path, monkeypatch, "shop,billing")
    for src in ("shop", "billing"):
        dag = getattr(module, f"rivet_{src}_cdc")
        assert dag.dag_id == f"rivet_{src}_cdc" and not dag.catchup
        run = dag.get_task("run")
        assert (run.config, run.export, run.state_dir) == (str(tmp_path / f"{src}.yaml"), "cdc", f"/srv/rivet-state/{src}")
        assert run.env_from_connections == {"RIVET_STATE_URL": f"rivet_state_{src}", "RIVET_MYSQL_URL": f"rivet_{src}"}
        assert run.cloud_credentials == ("gcp",)
        assert {t.task_id for t in dag.tasks} == {
            "run",
            "load.orders",
            "load.customers",
            "compact.orders",
            "compact.customers",
            "watcher",
        }


def test_a_config_without_exactly_one_cdc_export_is_refused_at_parse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "shop.yaml").write_text("exports:\n  - name: a\n    mode: incremental\n")
    with pytest.raises(ValueError, match="expected one export with `mode: cdc`, found 0"):
        load(tmp_path, monkeypatch, "shop")


def test_no_sources_builds_no_dag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = load(tmp_path, monkeypatch, "")
    assert not [name for name in vars(module) if name.startswith("rivet_") and name.endswith("_cdc")]


def test_a_source_without_a_config_is_refused_at_parse_naming_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "shop.yaml").write_text(CONFIG.format(src="shop"))
    with pytest.raises(ValueError, match=r"no config for billing in .*list only sources that have"):
        load(tmp_path, monkeypatch, "shop,billing")


def test_a_source_listed_twice_is_refused_at_parse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "shop.yaml").write_text(CONFIG.format(src="shop"))
    with pytest.raises(ValueError, match="sources listed twice: shop"):
        load(tmp_path, monkeypatch, "shop,shop")


def test_spaces_around_listed_sources_are_ignored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for src in ("shop", "billing"):
        (tmp_path / f"{src}.yaml").write_text(CONFIG.format(src=src))
    module = load(tmp_path, monkeypatch, " shop , billing ")
    assert module.rivet_shop_cdc.dag_id == "rivet_shop_cdc" and module.rivet_billing_cdc.dag_id == "rivet_billing_cdc"
