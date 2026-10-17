"""`_yaml` at its edges: the scalars rivet reads as written, the `${name}` scan, and the cut of one export out of the
operator's config text on hostile text. Mutant ids (`x_<fn>__mutmut_<n>`) are those of the full run on main 6ddc6d9."""

from __future__ import annotations

import math
import re
import signal

import pytest

from airflow_provider_rivet import _yaml


def refused(text: str, keep: set[int], count: int) -> str:
    """The refusal `narrow_text` raises (the operator puts it in RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE)."""
    with pytest.raises(_yaml.NarrowRefused) as exc:
        _yaml.narrow_text(text, keep, count, lambda cut: None)
    return str(exc.value)


# --- scalars ---------------------------------------------------------------------------------------------------------


def test_prefixed_and_signed_ints_read_their_value_and_keep_their_text() -> None:
    """A sign before a base prefix (_int 8, 14, 16, 17, 18); an explicit `!!int` that is no int stays text (_int 3)."""
    doc = _yaml.load("a: -0x1F\nb: +0x10\nc: +0o17\nd: -0b11\ne: !!int nope\n")
    assert doc == {"a": -31, "b": 16, "c": 15, "d": -3, "e": "nope"}
    assert [_yaml.text_of(doc[k]) for k in "abcd"] == ["-0x1F", "+0x10", "+0o17", "-0b11"]


@pytest.mark.parametrize(
    ("text", "value"),
    [(".inf", math.inf), ("-.inf", -math.inf), ("+.Inf", math.inf), ("-.INF", -math.inf), (".NaN", math.nan)],
)
def test_the_special_floats_read_with_their_sign_and_keep_their_text(text: str, value: float) -> None:
    """_float 4, 5, 6, 7, 10-23: every spelling of .inf / .nan, with its sign."""
    got = _yaml.load(f"v: {text}")["v"]
    assert (math.isnan(got) if math.isnan(value) else got == value) and _yaml.text_of(got) == text


def test_a_float_keeps_the_text_it_was_written_as() -> None:
    """_float 29: `1.50` is the string field `1.50`, not `1.5`."""
    assert _yaml.text_of(_yaml.load("v: 1.50")["v"]) == "1.50"


def test_text_of_spells_each_value_as_rivets_string_field_reads_it() -> None:
    """text_of 1 (None stays None), 3-8 (bools), 17 (a number with no written text)."""
    assert _yaml.text_of(None) is None
    assert (_yaml.text_of(True), _yaml.text_of(False)) == ("true", "false")
    assert _yaml.text_of(5) == "5"


# --- the `${name}` scan --------------------------------------------------------------------------------------------


def test_a_placeholder_at_the_very_start_is_substituted() -> None:
    """substitute 10, 11: the first `${` at offset 0."""
    assert _yaml.substitute("${A}x", {"A": "v"}.get) == ("vx", [])


def test_an_unresolved_placeholder_reads_back_spelled_as_written() -> None:
    """unmasked 9, 10, 12: the mask is undone in a string, in a list and in a mapping key."""
    doc, unresolved = _yaml.resolve_and_load("a: x${U}y\nb: [${V}]\n${W}: 1\n", lambda name: None)
    assert doc == {"a": "x${U}y", "b": ["${V}"], "${W}": 1} and unresolved == ["U", "V", "W"]


def test_the_same_length_mask_masks_every_placeholders_braces_in_place() -> None:
    """_same_length_mask 7, 8, 9, 10, 17, 20, 24: each `${name}` from offset 0 on, an unclosed one left as written."""
    open_, close = _yaml.MASK
    text = "${A}-${B} ${open"
    masked = _yaml._same_length_mask(text)
    assert masked == f"${open_}A{close}-${open_}B{close} ${{open" and len(masked) == len(text)


def test_a_trailing_param_flag_with_no_value_is_ignored() -> None:
    """params_of 12, 15, 17: `-p` / `--param` as the last argument."""
    assert _yaml.params_of(["-p", "A=1", "-p"]) == {"A": "1"}
    assert _yaml.params_of(["--param"]) == {}


# --- what the cut refuses, with the words the operator reads -------------------------------------------------------


def test_each_refusal_before_the_cut_says_why() -> None:
    """narrow_text 3, 4, 10; _exports_node 8-11, 18-21, 24, 30-32: the message is the operator's only clue."""
    assert refused("a: [", set(), 0) == "the config text does not compose before substitution: ParserError"
    assert refused("- a\n- b\n", set(), 0) == "the config is not one YAML mapping"
    assert refused("exports: []\nexports: []\n", set(), 0) == "the config has more than one `exports:` key"
    assert refused("exports: 5\n", set(), 0) == "`exports:` is not a sequence"
    assert refused("exports:\n  - name: a\n", set(), 2) == "the text holds 1 export items and rivet reads 2 after substitution"


@pytest.mark.parametrize("text", ["source: {}\n", "exports:\n", "exports: ~\n", "exports: null\n"])
def test_a_config_with_no_exports_is_returned_as_it_is(text: str) -> None:
    """_exports_node 22, 25-28; narrow_text 7: no `exports:` key, or a null one, holds no item to cut."""
    assert _yaml.narrow_text(text, set(), 0, lambda cut: None) == text


def test_an_export_that_is_an_alias_of_a_node_written_elsewhere_is_refused() -> None:
    """narrow_text 19, 20, 21."""
    text = "x: &e {name: a}\nexports:\n  - *e\n  - name: b\n"
    assert refused(text, {1}, 2) == "an export is an alias of a node written elsewhere"


def test_a_cut_that_reads_back_wrong_is_refused_with_the_reason() -> None:
    """narrow_text 49."""

    def check(cut: str) -> None:
        raise ValueError("differs")

    with pytest.raises(_yaml.NarrowRefused, match=re.escape("the cut text does not read back as the chosen exports: differs")):
        _yaml.narrow_text("exports:\n  - name: a\n  - name: b\n", {0}, 2, check)


@pytest.mark.parametrize("sep", ["\x85", " ", " "])
def test_a_line_break_other_than_newline_is_refused_not_cut_across(sep: str) -> None:
    """YAML breaks lines at NEL / LS / PS; the cut works on `\\n` lines, so an item that does not start or end a `\\n`
    line is refused (_block_cuts 62-64, 84, 89-91), never cut through the middle of another item."""
    text = f"exports:\n  - name: a{sep}  - name: b\n"
    assert refused(text, {0}, 2) == "export #2 does not start its own line"
    assert refused(text, {1}, 2) == "export #1 shares its last line with other text"


# --- the cut on hostile text ----------------------------------------------------------------------------------------


CUTS = [
    # a block scalar's text ends in its own newline (_block_cuts 66, 68, 69)
    ("exports:\n  - name: a\n    query: |\n      select 1\n  - name: b\n", {1}, "exports:\n  - name: b\n"),
    # the last item with no newline after it (_block_cuts 78)
    ("exports:\n  - name: a\n  - name: b", {0}, "exports:\n  - name: a\n"),
    # an item ending in a key with no value
    ("exports:\n  - name: a\n    note:\n  - name: b\n", {1}, "exports:\n  - name: b\n"),
    # ... at the end of a text with no newline: its empty scalar starts on the sequence end (_content_end 11)
    ("exports:\n  - name: a\n  - name: b\n    note:", {0}, "exports:\n  - name: a\n"),
    # an empty last item is dropped whole
    ("exports:\n- name: a\n-\n", {0}, "exports:\n- name: a\n"),
    # two kept items of a flow sequence, joined as rivet's own list (narrow_text 30)
    ("exports: [{name: a}, {name: b}, {name: c}]\n", {0, 2}, "exports: [{name: a}, {name: c}]\n"),
    # multi-byte text before and inside the items, CRLF line ends, a BOM
    ('\ufeffexports:\r\n  - name: café\r\n    q: "é—ж"\r\n  - name: 日本\r\n', {1}, "\ufeffexports:\r\n  - name: 日本\r\n"),
]


@pytest.mark.parametrize(("text", "keep", "cut"), CUTS)
def test_the_cut_removes_whole_lines_of_the_dropped_items(text: str, keep: set[int], cut: str) -> None:
    count = len(_yaml.load(text)["exports"])
    assert _yaml.narrow_text(text, keep, count, lambda c: None) == cut


def test_an_export_holding_a_recursive_anchor_is_cut_and_the_walk_ends() -> None:
    """_content_end 3, 6, 7: a node that holds itself (`&t [a, *t]`) is walked once, or the walk never ends."""
    text = "exports:\n  - name: a\n    t: &t [x, *t]\n    u: &u\n      - *u\n  - name: b\n"

    def stop(signum: int, frame: object) -> None:
        raise TimeoutError("the walk over a recursive node did not end")

    old = signal.signal(signal.SIGALRM, stop)
    signal.alarm(5)
    try:
        assert _yaml.narrow_text(text, {1}, 2, lambda c: None) == "exports:\n  - name: b\n"
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)
