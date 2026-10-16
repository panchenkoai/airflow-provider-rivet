"""The config reader of `_yaml.py`: rivet's one `${name}` scan, its YAML 1.2 scalars and the export cut, pinned value by
value. The scans read text the operator wrote, so each one is held to a megabyte of every hostile shape inside a time
budget: rivet's scan reads its input once, and a scan that restarts (`i = None` rescans the first `${` forever, growing
its output) must fail here on the clock, not by exhausting the worker's memory."""

from __future__ import annotations

import contextlib
import math
import signal
import time
from collections.abc import Callable, Iterator

import pytest

from airflow_provider_rivet import _yaml

M0, M1 = _yaml.MASK
MB = 1 << 20
BUDGET = 0.5  # seconds, for a scan of a few bytes
MB_BUDGET = 2.0  # seconds, for a megabyte; each one below takes under 0.1 s on a laptop


class OverBudget(Exception):
    """The scan did not end inside its time budget."""


@contextlib.contextmanager
def watchdog(seconds: float = BUDGET) -> Iterator[None]:
    """Raise `OverBudget` inside the block when it runs longer than `seconds`.

    A real-time timer interrupts the scan between two bytecodes, so a loop that never ends is stopped at the deadline
    and its growing output is freed at once; a thread that only waits on it would leave the loop running, eating
    memory until the process dies. pytest runs a test in the main thread, where the signal is delivered."""

    def expire(signum: int, frame: object) -> None:
        raise OverBudget(f"the scan ran past its {seconds} s budget: it reads its input more than once")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    started = time.monotonic()
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
    assert time.monotonic() - started < seconds


def once_each(
    values: dict[str, str | None], budget: int, default: str | None = None
) -> tuple[Callable[[str], str | None], list[str]]:
    """A `value_of` that records each name it is asked and refuses to be asked more than `budget` times: rivet asks
    once per placeholder, so a scan that asks again is restarting."""
    asked: list[str] = []

    def value_of(name: str) -> str | None:
        asked.append(name)
        if len(asked) > budget:
            raise OverBudget(f"value_of asked {len(asked)} times for {budget} placeholders")
        return values.get(name, default)

    return value_of, asked


# ---- substitute: one scan, `${}` empty, an unclosed `${` ends it, a value is never scanned again ----
# Mutants: x_substitute__mutmut_* (the start / end searches, the slice bounds, `i = end + 1`, the masked spelling).


@pytest.mark.parametrize(
    ("text", "values", "out", "unresolved", "asked"),
    [
        ("", {}, "", [], []),
        ("plain", {}, "plain", [], []),
        ("${A}", {"A": "1"}, "1", [], ["A"]),
        ("a${A}b", {"A": "1"}, "a1b", [], ["A"]),
        ("${A}${B}", {"A": "1", "B": "2"}, "12", [], ["A", "B"]),
        ("x${A}y${A}z", {"A": "<>"}, "x<>y<>z", [], ["A", "A"]),
        ("${}", {}, "", [], []),
        ("a${}b${}c", {}, "abc", [], []),
        ("${A", {"A": "1"}, "${A", [], []),
        ("${A}${B", {"A": "1"}, "1${B", [], ["A"]),
        ("${A}}", {"A": "1"}, "1}", [], ["A"]),
        ("$${A}", {"A": "1"}, "$1", [], ["A"]),
        ("${${A}", {"${A": "n"}, "n", [], ["${A"]),
        ("{A}$A}", {}, "{A}$A}", [], []),
        ("${A}", {"A": "${B}", "B": "no"}, "${B}", [], ["A"]),
        ("${A}", {"A": ""}, "", [], ["A"]),
        ("${C}", {}, f"{M0}C{M1}", ["C"], ["C"]),
        ("k: ${C} ${D}!", {"D": "d"}, f"k: {M0}C{M1} d!", ["C"], ["C", "D"]),
        ("${C}${C}", {}, f"{M0}C{M1}{M0}C{M1}", ["C", "C"], ["C", "C"]),
    ],
)
def test_substitute_reads_each_placeholder_once_as_resolve_rs(
    text: str, values: dict[str, str | None], out: str, unresolved: list[str], asked: list[str]
) -> None:
    value_of, seen = once_each(values, len(asked))
    with watchdog():
        assert _yaml.substitute(text, value_of) == (out, unresolved)
    assert seen == asked


def _hostile() -> list[tuple[str, str, str, list[str], int, str]]:
    """(shape, text, substituted, unresolved, value_of calls, masked) for a megabyte of each shape."""
    n = MB // 4
    opens = "${" * (MB // 2)
    return [
        ("placeholders", "${a}" * n, "v" * n, [], n, f"${M0}a{M1}" * n),
        ("empty placeholders", "${}" * (MB // 3), "", [], 0, f"${M0}{M1}" * (MB // 3)),
        ("unclosed opens", opens, opens, [], 0, opens),
        ("opens closed once", opens + "}", "v", [], 1, f"${M0}{opens[2:]}{M1}"),
        ("one long name", "x${" + "a" * MB + "}y", "xvy", [], 1, f"x${M0}{'a' * MB}{M1}y"),
        ("dollars", "$" * MB, "$" * MB, [], 0, "$" * MB),
        ("braces", "{}" * (MB // 2), "{}" * (MB // 2), [], 0, "{}" * (MB // 2)),
        ("closes", "}" * MB, "}" * MB, [], 0, "}" * MB),
        ("dollar braces", "$}{" * (MB // 3), "$}{" * (MB // 3), [], 0, "$}{" * (MB // 3)),
        ("unresolved", "${u}" * n, f"{M0}u{M1}" * n, ["u"] * n, n, f"${M0}u{M1}" * n),
    ]


HOSTILE = _hostile()
SHAPES = [h[0] for h in HOSTILE]


@pytest.mark.parametrize(("shape", "text", "out", "unresolved", "calls", "masked"), HOSTILE, ids=SHAPES)
def test_substitute_reads_a_hostile_megabyte_once_inside_its_budget(
    shape: str, text: str, out: str, unresolved: list[str], calls: int, masked: str
) -> None:
    value_of, asked = once_each({"u": None}, calls, default="v")
    with watchdog(MB_BUDGET):
        result = _yaml.substitute(text, value_of)
    assert result == (out, unresolved)
    assert len(asked) == calls


# ---- _same_length_mask: braces masked, every position unchanged; the same one scan ----
# Mutants: x__same_length_mask__mutmut_*.


@pytest.mark.parametrize(
    ("text", "masked"),
    [
        ("", ""),
        ("plain", "plain"),
        ("${A}", f"${M0}A{M1}"),
        ("a: '${A}' # ${B}", f"a: '${M0}A{M1}' # ${M0}B{M1}"),
        ("${}", f"${M0}{M1}"),
        ("${A", "${A"),
        ("${A}${B", f"${M0}A{M1}${{B"),
        ("${A}}", f"${M0}A{M1}}}"),
        ("$${A}x", f"$${M0}A{M1}x"),
    ],
)
def test_the_mask_keeps_every_position_and_hides_only_placeholder_braces(text: str, masked: str) -> None:
    with watchdog():
        assert _yaml._same_length_mask(text) == masked
    assert len(masked) == len(text)


@pytest.mark.parametrize(("shape", "text", "out", "unresolved", "calls", "masked"), HOSTILE, ids=SHAPES)
def test_the_mask_reads_a_hostile_megabyte_once_inside_its_budget(
    shape: str, text: str, out: str, unresolved: list[str], calls: int, masked: str
) -> None:
    with watchdog(MB_BUDGET):
        result = _yaml._same_length_mask(text)
    assert result == masked
    assert len(result) == len(text)


# ---- YAML 1.2 scalars as serde_yaml_ng reads them, each keeping the text it was written as ----
# Mutants: x__int__mutmut_*, x__float__mutmut_*, x_text_of__mutmut_*.


@pytest.mark.parametrize(
    ("written", "value"),
    [
        ("0", 0),
        ("5", 5),
        ("+5", 5),
        ("-5", -5),
        ("0x1F", 31),
        ("+0x1F", 31),
        ("-0x1F", -31),
        ("0o17", 15),
        ("+0o17", 15),
        ("-0o17", -15),
        ("0b101", 5),
        ("+0b101", 5),
        ("-0b101", -5),
    ],
)
def test_an_int_is_read_in_its_base_with_its_sign_and_keeps_its_text(written: str, value: int) -> None:
    read = _yaml.load(f"a: {written}")["a"]
    assert type(read) is _yaml.ScalarInt and read == value
    assert _yaml.text_of(read) == written


@pytest.mark.parametrize("written", ["08", "-08", "+08", "017", "012", "1e999", "-1e999", "0x", "1_000"])
def test_a_scalar_rivet_reads_as_a_string_stays_its_text(written: str) -> None:
    """A leading zero before digits is a string (`digits_but_not_number`); a float that overflows to infinity is the
    string rivet keeps (serde_yaml_ng's `parse_f64` refuses an infinite result unless it is spelled `.inf`)."""
    assert _yaml.load(f"a: {written}") == {"a": written}


def test_an_explicit_int_tag_on_text_that_is_not_an_int_stays_text() -> None:
    assert _yaml.load("a: !!int abc\nb: !!int 08\nc: !!int 0x10") == {"a": "abc", "b": "08", "c": 16}


@pytest.mark.parametrize(
    ("written", "value"),
    [
        ("1.5", 1.5),
        ("1.50", 1.5),
        ("-2.", -2.0),
        (".5", 0.5),
        ("1e3", 1000.0),
        ("+1.5e-2", 0.015),
        (".inf", math.inf),
        (".Inf", math.inf),
        (".INF", math.inf),
        ("+.inf", math.inf),
        ("-.inf", -math.inf),
        ("-.Inf", -math.inf),
        ("-.INF", -math.inf),
    ],
)
def test_a_float_keeps_its_text_and_infinity_is_spelled_as_yaml_spells_it(written: str, value: float) -> None:
    read = _yaml.load(f"a: {written}")["a"]
    assert type(read) is _yaml.ScalarFloat and read == value
    assert _yaml.text_of(read) == written


@pytest.mark.parametrize("written", [".nan", ".NaN", ".NAN"])
def test_nan_is_a_float_that_keeps_its_text(written: str) -> None:
    read = _yaml.load(f"a: {written}")["a"]
    assert type(read) is _yaml.ScalarFloat and math.isnan(read)
    assert _yaml.text_of(read) == written


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (None, None),
        ("", ""),
        ("orders", "orders"),
        (True, "true"),
        (False, "false"),
        (5, "5"),
        (0, "0"),
        (1.5, "1.5"),
        (["a"], "['a']"),
    ],
)
def test_text_of_is_what_a_string_field_of_rivet_reads(value: object, text: str | None) -> None:
    assert _yaml.text_of(value) == text


def test_text_of_a_parsed_bool_is_its_yaml_1_2_spelling() -> None:
    doc = _yaml.load("a: true\nb: False\nc: on\nd: yes\ne: TRUE")
    assert [_yaml.text_of(v) for v in doc.values()] == ["true", "false", "on", "yes", "true"]


# ---- placeholders spelled again, and `-p` as clap reads it ----
# Mutants: x_unmasked__mutmut_*, x_params_of__mutmut_*.


def test_unmasked_spells_every_unresolved_placeholder_as_written_through_lists_and_keys() -> None:
    masked = {f"{M0}K{M1}": [f"a{M0}B{M1}c", 1, None, {"x": f"{M0}{M1}"}], "plain": "}{"}
    assert _yaml.unmasked(masked) == {"${K}": ["a${B}c", 1, None, {"x": "${}"}], "plain": "}{"}


def test_resolve_and_load_reads_the_document_rivet_builds() -> None:
    text = "name: ${N}\nnote: '${MISSING}'\nlist: [${N}, x]\n"
    assert _yaml.resolve_and_load(text, {"N": "017"}.get) == (
        {"name": "017", "note": "${MISSING}", "list": ["017", "x"]},
        ["MISSING"],
    )


@pytest.mark.parametrize(
    ("args", "params"),
    [
        ([], {}),
        (["-p"], {}),
        (["--param"], {}),
        (["-p", "A=1", "-p"], {"A": "1"}),
        (["-p", "noequals"], {}),
        (["-p", "A=1", "--param=A=2"], {"A": "2"}),
        (["-pA=1", "-p=B=2", "--param", "C="], {"A": "1", "B": "2", "C": ""}),
        (["--px=1", "--paramA=1", "-q", "A=9"], {}),
        (["--json-errors", "-p", "K=a=b"], {"K": "a=b"}),
    ],
)
def test_params_of_reads_every_spelling_of_p_and_a_trailing_flag_reads_nothing(args: list[str], params: dict) -> None:
    assert _yaml.params_of(args) == params


def test_a_param_wins_over_the_environment() -> None:
    value_of = _yaml.resolver({"A": "env", "B": "env"}, {"A": "param", "C": ""})
    assert [value_of(n) for n in ("A", "B", "C", "D")] == ["param", "env", "", None]


# ---- narrow_text: the original text with whole export items cut, or a refusal that names why ----
# The refusal text reaches the operator inside RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE, so each sentence is pinned.
# Mutants: x__exports_node__mutmut_*, x__content_end__mutmut_*, x__block_cuts__mutmut_*, x_narrow_text__mutmut_*.


def no_check(text: str) -> None:
    """A read-back that accepts any cut: what is pinned is the cut itself."""


def narrow(text: str, keep: set[int], count: int) -> str:
    with watchdog():
        return _yaml.narrow_text(text, keep, count, no_check)


def refusal(text: str, keep: set[int], count: int) -> str:
    with watchdog(), pytest.raises(_yaml.NarrowRefused) as caught:
        _yaml.narrow_text(text, keep, count, no_check)
    return str(caught.value)


@pytest.mark.parametrize(
    ("text", "count", "why"),
    [
        ("- a\n", 0, "the config is not one YAML mapping"),
        ("plain\n", 0, "the config is not one YAML mapping"),
        ("exports: []\nexports: []\n", 0, "the config has more than one `exports:` key"),
        ("exports: []\nx: 1\nexports: []\nexports: []\n", 0, "the config has more than one `exports:` key"),
        ("exports: abc\n", 0, "`exports:` is not a sequence"),
        ("exports: {a: 1}\n", 0, "`exports:` is not a sequence"),
        ("exports: !!str null\n", 0, "`exports:` is not a sequence"),
        ("a: [\n", 0, "the config text does not compose before substitution: ParserError"),
        ("a: b: c\n", 0, "the config text does not compose before substitution: ScannerError"),
        ("exports:\n  - name: a\n  - name: b\n", 3, "the text holds 2 export items and rivet reads 3 after substitution"),
        ("x: 1\n", 1, "the text holds 0 export items and rivet reads 1 after substitution"),
        ("exports:\n", 1, "the text holds 0 export items and rivet reads 1 after substitution"),
        ("x: &e {name: a}\nexports:\n  - *e\n  - name: b\n", 2, "an export is an alias of a node written elsewhere"),
        ("exports:\n  - name: a\n  - name: b\n    x: 1\n", 2, None),
    ],
)
def test_a_config_that_cannot_be_cut_is_refused_with_its_reason(text: str, count: int, why: str | None) -> None:
    if why is None:  # the control: the same shapes cut fine
        assert narrow(text, {0}, count) == "exports:\n  - name: a\n"
    else:
        assert refusal(text, {0}, count) == why


@pytest.mark.parametrize(
    "text",
    ["x: 1\n", "exports:\n", "exports: ~\n", "exports: null\n", "exports: []\n", "exports:\n  - name: a\n"],
)
def test_nothing_to_drop_returns_the_text_itself(text: str) -> None:
    count = text.count("name:")
    assert narrow(text, set(range(count)), count) is text


ITEMS = """\
top: 1
exports:
  - name: a       # a
    mode: full
  # about b
  - name: b
    tables:
      - t1
      - t2
  - name: c
    x: {k: v,
      j: w}
  - name: d
    note: |
      line one
      line two

  - name: e
    q: "last"
rest: 2
"""
LINES = ITEMS.splitlines(keepends=True)
SPANS = {0: (2, 4), 1: (5, 9), 2: (9, 12), 3: (12, 17), 4: (17, 19)}  # [first, end) line of each item


@pytest.mark.parametrize("keep", [{0}, {1}, {2}, {3}, {4}, {0, 4}, {1, 3}, {0, 1, 2, 3}, {1, 2, 3, 4}, set()])
def test_a_block_cut_removes_each_dropped_item_as_whole_lines_and_keeps_every_other_byte(keep: set[int]) -> None:
    gone = {n for i, (a, b) in SPANS.items() if i not in keep for n in range(a, b)}
    assert narrow(ITEMS, keep, 5) == "".join(line for n, line in enumerate(LINES) if n not in gone)


def test_an_item_whose_last_line_holds_a_comment_is_cut_with_it() -> None:
    text = "exports:\n  - name: a\n    mode: full   # the last\n  - name: b\n"
    assert narrow(text, {1}, 2) == "exports:\n  - name: b\n"


def test_the_last_item_at_the_end_of_the_text_without_a_newline_is_cut() -> None:
    assert narrow("exports:\n  - name: a\n  - name: b", {0}, 2) == "exports:\n  - name: a\n"
    assert narrow("exports:\n  - name: a\n  - name: b\n    q: 'x'", {0}, 2) == "exports:\n  - name: a\n"


def test_an_item_ending_in_an_empty_value_is_cut_whole() -> None:
    assert narrow("exports:\n  - name: a\n    note:\n  - name: b\n", {1}, 2) == "exports:\n  - name: b\n"
    assert narrow("exports:\n  - name: a\n  - name: b\n    note:\n", {0}, 2) == "exports:\n  - name: a\n"


@pytest.mark.parametrize(
    ("text", "why"),
    [
        ("exports:\n  - name: a\n  - name: b\n", None),
        ("exports: [\n  {name: a}, {name: b}]\n", None),
        ("exports:\n- name: a\n- name: b\n", None),
        ("exports:\n  - name: a\n    x: &v 1\n  - name: b\n    y: *v\n", None),
    ],
)
def test_items_cut_at_any_indentation(text: str, why: str | None) -> None:
    out = narrow(text, {1}, 2)
    assert "name: a" not in out and "name: b" in out


@pytest.mark.parametrize(
    ("text", "first", "second"),
    [
        ("exports:\n  - name: a\n  - - name: b\n", "exports:\n  - name: a\n", "exports:\n  - - name: b\n"),
        ("exports:\n  - - x\n    - y\n  - name: b\n", "exports:\n  - - x\n    - y\n", "exports:\n  - name: b\n"),
        (
            "exports:\n  -\n    name: a\n  - !!map\n    name: b\n",
            "exports:\n  -\n    name: a\n",
            "exports:\n  - !!map\n    name: b\n",
        ),
        ("exports: !!seq\n- name: a\n- &b\n  name: b\n", "exports: !!seq\n- name: a\n", "exports: !!seq\n- &b\n  name: b\n"),
    ],
)
def test_an_item_is_cut_from_its_own_dash_whatever_its_node_starts_with(text: str, first: str, second: str) -> None:
    """The item's node may start at an inner `-`, a tag, an anchor or the next line; its own `-` is the nearest one
    before it, and the lines from there to its last content are what is cut."""
    assert narrow(text, {0}, 2) == first
    assert narrow(text, {1}, 2) == second


@pytest.mark.parametrize(
    ("keep", "out"),
    [
        ({0}, "exports: [{name: a}]\n"),
        ({1}, "exports: [{name: b, x: [1, 2]}]\n"),
        ({0, 2}, "exports: [{name: a}, {name: c}]\n"),
        ({1, 2}, "exports: [{name: b, x: [1, 2]}, {name: c}]\n"),
    ],
)
def test_a_flow_list_is_rewritten_with_the_kept_items_byte_exact(keep: set[int], out: str) -> None:
    assert narrow("exports: [{name: a},  {name: b, x: [1, 2]},\n  {name: c}]\n", keep, 3) == out


def test_a_cut_the_read_back_rejects_is_refused_with_the_read_backs_reason() -> None:
    def check(text: str) -> None:
        raise ValueError("the narrowed document differs")

    with watchdog(), pytest.raises(_yaml.NarrowRefused) as caught:
        _yaml.narrow_text("exports:\n  - name: a\n  - name: b\n", {1}, 2, check)
    assert str(caught.value) == "the cut text does not read back as the chosen exports: the narrowed document differs"
    assert isinstance(caught.value.__cause__, ValueError)


def test_the_read_back_reads_the_cut_text() -> None:
    seen: list[str] = []
    out = _yaml.narrow_text("exports:\n  - name: a\n  - name: b\n", {1}, 2, seen.append)
    assert seen == [out] == ["exports:\n  - name: b\n"]


def test_a_self_referencing_anchor_in_an_item_is_walked_once() -> None:
    """`&r [1, *r]` composes to a node that holds itself: the walk that finds an item's end must visit it once, or it
    never ends (the `seen` set of `_content_end`)."""
    text = "exports:\n  - name: a\n    k: &r [1, *r]\n    j: &m {a: *m}\n  - name: b\n"
    assert narrow(text, {1}, 2) == "exports:\n  - name: b\n"
    assert narrow(text, {0}, 2) == "exports:\n  - name: a\n    k: &r [1, *r]\n    j: &m {a: *m}\n"


# ---- an alias at the end of an export item is that item's text (phase-1 defect, mutant x__block_cuts__mutmut_91) ----

ALIASED_LAST = """\
source: {type: postgres, url_env: RIVET_PG_URL}
x-dest: &s3
  type: local
  path: ./out/
x-tables: &tables [orders]
exports:
  - name: orders
    mode: full
    destination: *s3
  - name: users
    mode: full
    destination: *s3   # shared
  - name: stream
    mode: cdc
    tables:
      - *tables
"""


@pytest.mark.parametrize("kept", [0, 1, 2])
def test_an_export_ending_in_an_alias_of_a_top_level_anchor_is_cut_not_refused(kept: int) -> None:
    """`destination: *s3` as an item's last line was refused (`RIVET_AIRFLOW_CONFIG_NARROW_UNSAFE`, "shares its last
    line with other text") for every load/compact of such a config: the alias composes to its anchor's node, written
    above `exports:`, so the item's text was taken to end at `destination` and ` *s3` read as foreign text. The remedy
    the refusal printed ("no anchor another item uses") did not even apply. The cut is the original with the other
    items' whole lines removed, and it reads back as the chosen export with its aliases resolved."""
    original = _yaml.load(ALIASED_LAST)

    def check(cut: str) -> None:  # the operator's read-back (invocation.narrowed_config)
        if _yaml.load(cut) != {**original, "exports": [original["exports"][kept]]}:
            raise ValueError("the narrowed document differs from the chosen exports of the original")

    head, body = ALIASED_LAST.split("exports:\n")
    items = ["  - name" + item for item in body.split("  - name")[1:]]
    with watchdog():
        assert _yaml.narrow_text(ALIASED_LAST, {kept}, 3, check) == head + "exports:\n" + items[kept]
