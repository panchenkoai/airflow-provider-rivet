"""dev/mutants_gate.py: a pull request fails only on a survivor of a line it changed."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("mutants_gate", Path(__file__).resolve().parents[1] / "dev" / "mutants_gate.py")
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)

SOURCE = "class K:\n    @classmethod\n    def m(cls, a):\n        b = a\n        return b + 1\n"
MUTATED = (
    "class K:\n"
    "    def xǁKǁm__mutmut_orig(cls, a):\n        b = a\n        return b + 1\n"
    "    def xǁKǁm__mutmut_1(cls, a):\n        b = a\n        return b - 1\n"
)


def test_a_mutant_is_placed_on_the_source_line_it_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for root, text in ((tmp_path, SOURCE), (tmp_path / "mutants", MUTATED)):
        (root / gate.PACKAGE).mkdir(parents=True)
        (root / gate.PACKAGE / "mod.py").write_text(text)
    monkeypatch.setattr(gate, "ROOT", tmp_path)
    assert gate.mutated_line(f"{gate.PACKAGE}.mod.xǁKǁm__mutmut_1") == (f"{gate.PACKAGE}/mod.py", 5)
    assert gate.mutated_line(f"{gate.PACKAGE}.mod.xǁKǁm__mutmut_9") is None
