"""Read a rivet config the way rivet does: `${name}` spliced into the raw text first (src/config/resolve.rs), then the
YAML 1.2 scalars of serde_yaml_ng, so `on` and `12:30` stay strings and `017` is the string `017`."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import yaml

MASK = ("", "")


class ScalarInt(int):
    """An int read from a plain scalar, with the text it was written as: rivet reads a string field as that text."""

    text: str


class ScalarFloat(float):
    """A float read from a plain scalar, with the text it was written as."""

    text: str


_INT = re.compile(r"^(?:[-+]?(?:0|[1-9][0-9]*)|\+?0x[0-9a-fA-F]+|\+?0o[0-7]+|\+?0b[01]+|-0x[0-9a-fA-F]+|-0o[0-7]+|-0b[01]+)$")
_FLOAT = re.compile(r"^(?:[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$")
_LEADING_ZERO = re.compile(r"^[-+]?0[0-9]+$")
_CORE = (
    ("tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF")),
    ("tag:yaml.org,2002:int", _INT, list("-+0123456789")),
    ("tag:yaml.org,2002:float", _FLOAT, list("-+0123456789.")),
)
_REPLACED = {tag for tag, _, _ in _CORE} | {"tag:yaml.org,2002:timestamp"}


class ConfigLoader(yaml.SafeLoader):
    """SafeLoader with the YAML 1.1 bool / int / float / timestamp guesses replaced by serde_yaml_ng's 1.2 ones."""


ConfigLoader.yaml_implicit_resolvers = {
    first: [(tag, rx) for tag, rx in pairs if tag not in _REPLACED]
    for first, pairs in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
for _tag, _rx, _first in _CORE:
    ConfigLoader.add_implicit_resolver(_tag, _rx, _first)


def _int(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Any:
    """serde_yaml_ng's `parse_*_int`: base 10, or 0x / 0o / 0b; a leading zero before digits is a string."""
    text = loader.construct_scalar(node)
    if _LEADING_ZERO.match(text) or not _INT.match(text):
        return text
    value = ScalarInt(int(text, 0))  # base 0: a sign, then 0x / 0o / 0b or decimal, exactly the `_INT` forms
    value.text = text
    return value


def _float(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Any:
    """serde_yaml_ng's `parse_f64`."""
    text = loader.construct_scalar(node)
    if _LEADING_ZERO.match(text):
        return text
    word = text.lower().removeprefix("+").removeprefix("-")
    if word in (".inf", ".nan"):
        number = float(("-" if text.startswith("-") else "") + word[1:])
    else:
        number = float(text)
        if math.isinf(number):
            return text
    value = ScalarFloat(number)
    value.text = text
    return value


ConfigLoader.add_constructor("tag:yaml.org,2002:int", _int)
ConfigLoader.add_constructor("tag:yaml.org,2002:float", _float)


def load(text: str) -> Any:
    """Parse config text (already substituted)."""
    return yaml.load(text, Loader=ConfigLoader)


def text_of(value: Any) -> str | None:
    """A scalar as rivet's string field reads it: the written text (`name: 2024` is `"2024"`, `name: 08` is `"08"`)."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return getattr(value, "text", None) or str(value)


def substitute(text: str, value_of: Callable[[str], str | None]) -> tuple[str, list[str]]:
    """rivet's one scan of `${name}` (src/config/resolve.rs `substitute`): `${}` becomes empty, an unclosed `${` ends the
    scan, a value is never scanned again. A name `value_of` cannot resolve stays, masked so it parses as plain text
    (rivet's `masked`), and is returned; rivet itself refuses such a config."""
    unresolved: list[str] = []
    out, i = [], 0
    while (start := text.find("${", i)) >= 0:
        end = text.find("}", start)
        if end == -1:
            break
        name = text[start + 2 : end]
        value = "" if not name else value_of(name)
        if value is None:
            unresolved.append(name)
            value = f"{MASK[0]}{name}{MASK[1]}"
        out += [text[i:start], value]
        i = end + 1
    out.append(text[i:])
    return "".join(out), unresolved


def unmasked(value: Any) -> Any:
    """A parsed value with its unresolved placeholders spelled `${name}` again."""
    if isinstance(value, str):
        return value.replace(MASK[0], "${").replace(MASK[1], "}")
    if isinstance(value, list):
        return [unmasked(v) for v in value]
    if isinstance(value, dict):
        return {unmasked(k): unmasked(v) for k, v in value.items()}
    return value


def params_of(args: Sequence[str]) -> dict[str, str]:
    """The `-p KEY=VALUE` / `--param KEY=VALUE` values among extra arguments, in every spelling clap accepts; last wins."""
    params: dict[str, str] = {}
    args = list(args)
    for i, arg in enumerate(args):
        if arg in ("-p", "--param"):
            item = args[i + 1] if i + 1 < len(args) else None  # a trailing `-p` has no value
        elif arg.startswith("--param="):
            item = arg[len("--param=") :]
        elif arg.startswith("-p"):
            item = arg[2:].removeprefix("=")
        else:
            continue
        if item and "=" in item:
            key, value = item.split("=", 1)
            params[key] = value
    return params


def resolver(env: Mapping[str, str], params: Mapping[str, str]) -> Callable[[str], str | None]:
    """A placeholder's value as rivet takes it: a `-p` param wins over the environment."""
    return lambda name: params[name] if name in params else env.get(name)


def resolve_and_load(text: str, value_of: Callable[[str], str | None]) -> tuple[Any, list[str]]:
    """The document rivet would build from this text, and the placeholders nothing resolved (spelled `${name}`)."""
    resolved, unresolved = substitute(text, value_of)
    return unmasked(load(resolved)), unresolved


class NarrowRefused(Exception):
    """The config text cannot be cut down to the chosen exports without guessing."""


def _same_length_mask(text: str) -> str:
    """`text` with each placeholder's braces masked, character positions unchanged, so it composes as rivet's would."""
    out, i = [], 0
    while (start := text.find("${", i)) >= 0:
        end = text.find("}", start)
        if end == -1:
            break
        out += [text[i:start], "$", MASK[0], text[start + 2 : end], MASK[1]]
        i = end + 1
    out.append(text[i:])
    return "".join(out)


def _compose(text: str) -> yaml.Node | None:
    """`yaml.compose` with the config loader."""
    loader = ConfigLoader(text)
    try:
        return loader.get_single_node()
    finally:
        loader.dispose()


def _tokens(text: str) -> list[yaml.Token]:
    """`yaml.scan` with the config loader, as a list."""
    loader = ConfigLoader(text)
    try:
        return list(iter(loader.get_token, None))
    finally:
        loader.dispose()


def _exports_node(text: str) -> yaml.SequenceNode | None:
    root = _compose(_same_length_mask(text))
    if not isinstance(root, yaml.MappingNode):
        raise NarrowRefused("the config is not one YAML mapping")
    found = [value for key, value in root.value if isinstance(key, yaml.ScalarNode) and key.value == "exports"]
    if len(found) > 1:
        raise NarrowRefused("the config has more than one `exports:` key")
    if not found or (isinstance(found[0], yaml.ScalarNode) and found[0].tag == "tag:yaml.org,2002:null"):
        return None
    if not isinstance(found[0], yaml.SequenceNode):
        raise NarrowRefused("`exports:` is not a sequence")
    return found[0]


def _block_cuts(text: str, seq: yaml.SequenceNode, drop: set[int]) -> list[tuple[int, int, str]]:
    """Whole lines from each dropped item's `-` to the end of the line of its last token.

    In a block sequence every token from an item's start to the next item's `-` (the end of the sequence for the last
    item) is that item's text: its scalars, flow collections, anchors, tags and aliases (an alias composes to its
    anchor's node, whose marks are where the anchor is written, so the composed node cannot tell where its text ends).
    `narrow_text` reads the cut back and refuses anything that is not the chosen document."""
    tokens = _tokens(_same_length_mask(text))
    dashes = [t.start_mark.index for t in tokens if isinstance(t, yaml.BlockEntryToken)]
    own = [max(d for d in dashes if d < item.start_mark.index) for item in seq.value]  # each item's own `-`
    bounds = [*own[1:], seq.end_mark.index]
    cuts = []
    for i in sorted(drop):
        start, bound = seq.value[i].start_mark.index, bounds[i]
        end = max(t.end_mark.index for t in tokens if start <= t.start_mark.index < bound)
        line = text[: own[i]].rfind("\n") + 1
        cuts.append((line, text.find("\n", end - 1) + 1 or len(text), ""))
    return cuts


def narrow_text(text: str, keep: set[int], count: int, check: Callable[[str], None]) -> str:
    """`text` with every export item `keep` rejects cut out of the original text, so every byte of what stays (quoted
    `${VAR}`, comments, anchors) is what the operator wrote (D09). `count` is the number of exports rivet reads; `check`
    reads the result back and raises when it is not the intended document; anything that cannot be cut safely raises
    `NarrowRefused` rather than guessing."""
    try:
        seq = _exports_node(text)
    except yaml.YAMLError as exc:
        raise NarrowRefused(f"the config text does not compose before substitution: {type(exc).__name__}") from exc
    items = seq.value if seq is not None else []
    if len(items) != count:
        raise NarrowRefused(f"the text holds {len(items)} export items and rivet reads {count} after substitution")
    drop = set(range(len(items))) - keep
    if not drop:
        return text
    for item in items:
        if item.start_mark.index not in range(seq.start_mark.index, seq.end_mark.index):
            raise NarrowRefused("an export is an alias of a node written elsewhere")
    if seq.flow_style:
        kept = [text[item.start_mark.index : item.end_mark.index] for i, item in enumerate(items) if i not in drop]
        cuts = [(seq.start_mark.index, seq.end_mark.index, "[" + ", ".join(kept) + "]")]
    else:
        cuts = _block_cuts(text, seq, drop)
    out = text
    for start, end, new in sorted(cuts, reverse=True):
        out = out[:start] + new + out[end:]
    try:
        check(out)
    except (yaml.YAMLError, ValueError) as exc:
        raise NarrowRefused(f"the cut text does not read back as the chosen exports: {exc}") from exc
    return out
