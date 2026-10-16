"""The ADR-0039 readers of `result.py` at their edges: which printed objects are refused, and the exit table's bounds."""

from __future__ import annotations

import pytest

from airflow_provider_rivet.result import ErrorObject, error_from_exit, error_from_line


@pytest.mark.parametrize("exit_code", [7, 0, True, 5.0, "5"])
def test_an_error_object_whose_exit_code_is_not_a_contract_exit_is_refused(exit_code: object) -> None:
    """from_contract mutant 29 (`or` -> `and`): an int outside 1-6, and a bool or float inside it, is not an exit code."""
    assert ErrorObject.from_contract({"class": "refusal", "retryable": False, "exit_code": exit_code}) is None
    assert ErrorObject.from_contract({"class": "refusal", "retryable": False, "exit_code": 5}) is not None


@pytest.mark.parametrize("export", [None, ""])
def test_the_exception_text_names_no_unit_when_there_is_none(export: str | None) -> None:
    """describe mutants 11 / 12: a whole-config run with several failed exports passes `None`."""
    error = ErrorObject(None, None, "generic", 1, False, None)
    assert error.describe(export) == "generic"
    assert error.describe("orders") == "generic (orders)"
    assert ErrorObject("C", None, "refusal", 5, False, "do x").describe(export) == "[C] refusal: do x"


def test_the_exit_table_ends_at_255() -> None:
    """error_from_exit mutant 55: 129-255 is a signal death, 256 is not."""
    assert error_from_exit(255, None).class_ == "crashed"
    assert error_from_exit(256, None).class_ == "generic"
    assert error_from_exit(128, None).class_ == "generic"


def test_a_line_with_retryable_but_no_class_is_read_by_the_contract_and_refused() -> None:
    """error_from_line mutants 5 / 6: `retryable` alone sends the line down the contract path (no class: malformed),
    never to the `exit_class` derivation that would build a retryable object from it."""
    assert error_from_line({"retryable": True, "exit_class": 2, "error": "x"}) == (None, False)


@pytest.mark.parametrize("line", [{"exit_class": 1, "error": 5}, {"exit_class": 1, "code": 5}], ids=["error", "code"])
def test_a_derived_line_whose_text_is_not_a_string_is_malformed(line: dict) -> None:
    """error_from_line mutants 22-24: the `error` text is type-checked like the code."""
    assert error_from_line(line) == (None, True)


@pytest.mark.parametrize("known", [7, 0, True, 2.0, "2"])
def test_a_derived_line_whose_exit_class_is_not_a_contract_exit_is_malformed(known: object) -> None:
    """error_from_line mutant 26 (`or` -> `and`): an int outside the table reached CLASS_BY_EXIT[7], a KeyError."""
    assert error_from_line({"exit_class": known}) == (None, True)
    assert error_from_line({"exit_class": 2})[0].retryable is True
