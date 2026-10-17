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
    sign, body = (-1, text[1:]) if text.startswith("-") else (1, text.lstrip("+"))
    base = {"0x": 16, "0o": 8, "0b": 2}.get(body[:2], 10)
    value = ScalarInt(sign * int(body, base))  # int() reads the 0x / 0o / 0b prefix of its own base
    value.text = text
    return value


def _float(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Any:
    """serde_yaml_ng's `parse_f64`."""
    text = loader.construct_scalar(node)
    if _LEADING_ZERO.match(text):
        return text
    word = text.lstrip("+-").lower()  # pragma: no mutate -- a float scalar never starts with `X`, so `XX+-XX` strips alike
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
        if end < 0:  # pragma: no mutate -- a found `}` is at start + 2 or later, so `< 0`, `<= 0` and `< 1` agree
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
            item = args[i + 1] if i + 1 < len(args) else ""  # pragma: no mutate -- any default without `=` is ignored alike
        elif arg.startswith("--param="):
            item = arg[len("--param=") :]
        elif arg.startswith("-p"):
            item = arg[2:].removeprefix("=")
        else:
            continue
        if "=" in item:
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
        if end < 0:  # pragma: no mutate -- a found `}` is at start + 2 or later, so `< 0`, `<= 0` and `< 1` agree
            break
        out += [text[i:start], "$", MASK[0], text[start + 2 : end], MASK[1]]
        i = end + 1
    out.append(text[i:])
    return "".join(out)


def _exports_node(text: str) -> yaml.SequenceNode | None:
    # only the null tag is read from the composed nodes, and every loader resolves null alike
    root = yaml.compose(_same_length_mask(text), Loader=ConfigLoader)  # pragma: no mutate
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


def _content_end(node: yaml.Node, low: int, high: int) -> int:
    """The end of the last text that belongs to an item: the furthest scalar or flow collection written inside it."""
    # The stack pops the last child first, so a node taken after another is written before it, and any node met out of
    # order (seen again, or outside the item) is an alias, whose text `_block_cuts` reads from the alias tokens: a
    # `break` for those `continue`s, or a stack replaced by `=`, finds the same furthest end, so they are not mutated.
    best, seen, todo = low, set(), [node]
    while todo:
        cur = todo.pop()
        if id(cur) in seen:
            continue  # pragma: no mutate
        seen.add(id(cur))
        start, end = cur.start_mark.index, cur.end_mark.index
        if not low <= start <= high:
            continue  # pragma: no mutate
        if isinstance(cur, yaml.ScalarNode) or cur.flow_style:
            best = max(best, min(end, high))
        if isinstance(cur, yaml.MappingNode):
            todo += [part for pair in cur.value for part in pair]  # pragma: no mutate
        elif isinstance(cur, yaml.SequenceNode):
            todo += list(cur.value)  # pragma: no mutate
    return best


def _block_cuts(text: str, seq: yaml.SequenceNode, drop: set[int]) -> list[tuple[int, int, str]]:
    """Whole lines from each dropped item's `-` to the end of its last content line."""
    tokens = list(yaml.scan(_same_length_mask(text)))  # the scanner is SafeLoader's whatever the loader
    dashes = sorted(t.start_mark.index for t in tokens if isinstance(t, yaml.BlockEntryToken))
    # An alias composes to its anchor's node, whose marks are where the anchor is written (often above `exports:`), so
    # the node walk of `_content_end` cannot see `*name` as text of the item that writes it: read it from the tokens.
    aliases = [(t.start_mark.index, t.end_mark.index) for t in tokens if isinstance(t, yaml.AliasToken)]
    items = seq.value
    cuts = []
    for i in sorted(drop):
        start = items[i].start_mark.index
        # Every item of a block sequence starts after its own `-` (an item's node never starts on one), written after the
        # previous item: this refusal never fires on text PyYAML composes (12 000 generated configs) and stays so that
        # text composed otherwise is refused, not cut on a guess. Its comparisons and default are not mutated.
        dash = max((d for d in dashes if d < start), default=-1)  # pragma: no mutate
        if dash < 0 or (i > 0 and dash < items[i - 1].start_mark.index):  # pragma: no mutate
            raise NarrowRefused(f"cannot find the `-` of export #{i + 1}")  # pragma: no mutate
        if i + 1 < len(items):  # the next item's own `-`
            high = max(d for d in dashes if d < items[i + 1].start_mark.index)  # pragma: no mutate
        else:
            high = seq.end_mark.index
        # the `exports:` key is written on a line before the first `-`, so no `-` is on the line a newline at 0 ends
        line = text.rfind("\n", 0, dash) + 1  # pragma: no mutate
        if text[line:dash].strip():
            raise NarrowRefused(f"export #{i + 1} does not start its own line")
        # no alias token starts at the item's own start (an alias item is refused above) or at the next `-`: the
        # bounds' `<` / `<=` are equivalent, so they are not mutated
        end = max([_content_end(items[i], start, high), *(e for s, e in aliases if start <= s < high)])  # pragma: no mutate
        if text[end - 1] == "\n":
            stop = end
        else:
            stop = text.find("\n", end)
            stop = len(text) if stop < 0 else stop + 1  # pragma: no mutate -- a found newline is at `end` or later, never 0
        tail = text[end:stop].strip()
        if tail and not tail.startswith("#"):
            raise NarrowRefused(f"export #{i + 1} shares its last line with other text")
        cuts.append((line, stop, ""))
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
        if not seq.start_mark.index <= item.start_mark.index <= seq.end_mark.index:
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
