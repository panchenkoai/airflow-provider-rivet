"""The command-line surface of real rivet, read from its clap definitions (`src/cli/args.rs`), for `tests/fake_rivet.py`.

The fake binary accepts exactly the subcommands and flags the rivet it impersonates accepts and refuses the rest the way
clap does (exit 2), so a mutated subcommand or flag name in the package fails the test that runs it. The surface of each
impersonated rivet is checked in as `tests/rivet_cli_surface.json`; `tests/test_harness.py` regenerates it from the
rivet checkout (RIVET_SRC) and fails when the two differ.

Regenerate after bumping RIVET_CONTRACT_REF (the `main` entry) or adding a release (an entry in VERSIONS):

    uv run python tests/rivet_cli_surface.py          # RIVET_SRC defaults to ../rivet, as for pytest

A ref the rivet checkout lacks is fetched from its `origin` (CI clones the pinned commit only).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
TABLE = Path(__file__).resolve().parent / "rivet_cli_surface.json"
ARGS_RS = "src/cli/args.rs"
# The impersonated binaries: a release by its tag, and `main` = the rivet commit this package's contract is pinned to.
VERSIONS = {"0.31.0": "v0.31.0", "main": None}
INT_TYPES = {"u8", "u16", "u32", "u64", "usize", "i8", "i16", "i32", "i64", "isize"}
REGENERATE = "uv run python tests/rivet_cli_surface.py"


def contract_ref() -> str:
    """The rivet commit RIVET_CONTRACT_REF pins."""
    return (PACKAGE_ROOT / "RIVET_CONTRACT_REF").read_text().strip()


def kebab(name: str) -> str:
    """clap's rename of a variant (`ResetChunks` -> `reset-chunks`) or a field (`run_id` -> `run-id`)."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "-", name).replace("_", "-").lower()


def _strip(text: str) -> str:
    """The source without comments and without the `#[cfg(test)]` module."""
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("//")]
    text = "\n".join(lines)
    while (start := text.find("#[cfg(test)]")) >= 0:
        brace = text.index("{", start)
        end = _close(text, brace)
        text = text[:start] + text[end + 1 :]
    return text


def _close(text: str, opening: int) -> int:
    """The index of the bracket that closes the one at `opening`, skipping string and char literals."""
    pairs = {"{": "}", "(": ")", "[": "]", "<": ">"}
    stack, i = [pairs[text[opening]]], opening + 1
    while stack:
        ch = text[i]
        if ch == '"':
            i = text.index('"', i + 1) if text[i + 1] != '"' else i + 1
            while text[i - 1] == "\\":
                i = text.index('"', i + 1)
        elif ch == "'" and i + 2 < len(text) and text[i + 2] == "'":
            i += 2
        elif ch in pairs and (ch != "<" or stack[-1] == ">"):
            stack.append(pairs[ch])
        elif ch == stack[-1]:
            stack.pop()
        i += 1
    return i - 1


def _items(text: str) -> dict[str, tuple[str, str, str]]:
    """Every `enum`/`struct` of the file: name -> (kind, the derive/attribute text before it, its body)."""
    out = {}
    for match in re.finditer(r"((?:#\[[^\n]*\]\s*)*)pub (enum|struct) (\w+)\s*\{", text):
        body_start = match.end() - 1
        out[match.group(3)] = (match.group(2), match.group(1), text[body_start + 1 : _close(text, body_start)])
    return out


def _split_top(body: str) -> list[str]:
    """The comma-separated members of a body at bracket depth 0, each with its attributes."""
    parts, depth, start, i = [], 0, 0, 0
    while i < len(body):
        ch = body[i]
        if ch == '"':
            i = body.index('"', i + 1)
            while body[i - 1] == "\\":
                i = body.index('"', i + 1)
        elif ch in "{([":
            depth += 1
        elif ch in "})]":
            depth -= 1
        elif ch == "<" and depth >= 0:
            depth += 1
        elif ch == ">" and body[i - 1] != "=" and depth > 0:
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(body[start:i])
            start = i + 1
        elif ch == "}" and depth == 0:
            pass
        i += 1
    parts.append(body[start:])
    return [p.strip() for p in parts if p.strip()]


def _members(body: str) -> list[tuple[list[str], str]]:
    """(attribute texts, the member after them) for each member of an enum or struct body."""
    out, i, n = [], 0, len(body)
    attrs: list[str] = []
    while i < n:
        if body[i].isspace() or body[i] == ",":
            i += 1
        elif body.startswith("#[", i):
            end = _close(body, i + 1)
            attrs.append(body[i + 2 : end])
            i = end + 1
        else:
            j, depth = i, 0
            while j < n and not (body[j] == "," and depth == 0):
                if body[j] in "{(":
                    end = _close(body, j)
                    j = end
                elif body[j] == "<":
                    j = _close(body, j)
                j += 1
            out.append((attrs, body[i:j].strip()))
            attrs, i = [], j + 1
    return out


def _arg_options(attrs: list[str], prefix: str) -> list[str]:
    """The comma-separated options of every `#[<prefix>(...)]` attribute."""
    out = []
    for attr in attrs:
        if attr.startswith(prefix + "("):
            out += [o.strip() for o in _split_top(attr[len(prefix) + 1 : -1])]
    return out


def _option(options: list[str], key: str) -> str | None:
    """The value of `key = value` among an attribute's options, quotes removed; "" for a bare `key`."""
    for opt in options:
        if opt == key:
            return ""
        if opt.split("=", 1)[0].strip() == key:
            return opt.split("=", 1)[1].strip().strip("\"'")
    return None


def _value_enums(items: dict) -> dict[str, list[str]]:
    """Every `clap::ValueEnum` of the file: name -> its possible values."""
    out = {}
    for name, (kind, attrs, body) in items.items():
        if kind == "enum" and "ValueEnum" in attrs:
            out[name] = [kebab(member.split("(")[0].split("{")[0].strip()) for _, member in _members(body)]
    return out


def _field(attrs: list[str], member: str, enums: dict[str, list[str]]) -> dict | None:
    """One field's argument: None for a `#[command(subcommand)]` / `#[command(flatten)]` field (handled by the caller)."""
    name, _, rust_type = member.removeprefix("pub ").partition(":")
    name, rust_type = name.strip(), re.sub(r"\s+", "", rust_type)
    opts = _arg_options(attrs, "arg")
    wrapper = re.match(r"(Option|Vec)<(.+)>$", rust_type)
    inner = wrapper.group(2) if wrapper else rust_type
    inner_name = inner.rsplit("::", 1)[-1]
    long, short = _option(opts, "long"), _option(opts, "short")
    arg = {
        "name": name,
        "long": (kebab(name) if long == "" else long) if long is not None else None,
        "short": (name[0] if short == "" else short) if short is not None else None,
        "aliases": sorted(
            o.split("=", 1)[1].strip().strip('"') for o in opts if o.split("=", 1)[0].strip() in ("alias", "visible_alias")
        ),
        "global": _option(opts, "global") == "true",
        "switch": inner == "bool",
        "multiple": bool(wrapper and wrapper.group(1) == "Vec"),
        "num_args": _option(opts, "num_args"),
        "value_name": None,
        "kind": None if inner == "bool" else "int" if inner_name in INT_TYPES else ("enum" if inner_name in enums else "string"),
        "values": enums.get(inner_name),
    }
    if not arg["switch"]:
        arg["value_name"] = _option(opts, "value_name") or name.upper()
    defaulted = any(_option(opts, k) is not None for k in ("default_value", "default_value_t", "env"))
    arg["required"] = not (arg["switch"] or wrapper or defaulted)
    return arg


def _command(attrs: list[str], fields_body: str | None, items: dict, enums: dict) -> dict:
    """One (sub)command: its options, positionals and nested subcommands."""
    cmd: dict = {"options": [], "positionals": [], "subcommands": None}
    for field_attrs, member in _members(fields_body or ""):
        command_opts = _arg_options(field_attrs, "command")
        rust_type = member.partition(":")[2].strip()
        if "subcommand" in command_opts:
            cmd["subcommands"] = _subcommands(items[rust_type][2], items, enums)
            continue
        if "flatten" in command_opts:
            nested = _command([], items[rust_type][2], items, enums)
            cmd["options"] += nested["options"]
            cmd["positionals"] += nested["positionals"]
            continue
        arg = _field(field_attrs, member, enums)
        (cmd["options"] if arg["long"] or arg["short"] else cmd["positionals"]).append(arg)
    return cmd


def _subcommands(body: str, items: dict, enums: dict) -> dict[str, dict]:
    """The variants of a `Subcommand` enum: name -> command."""
    out = {}
    for attrs, member in _members(body):
        head = re.match(r"(\w+)\s*([({]?)", member)
        variant, bracket = head.group(1), head.group(2)
        name = _option(_arg_options(attrs, "command"), "name") or kebab(variant)
        if bracket == "(":
            fields = items[member[member.index("(") + 1 : member.rindex(")")].strip()][2]
        elif bracket == "{":
            fields = member[member.index("{") + 1 : member.rindex("}")]
        else:
            fields = None
        out[name] = _command(attrs, fields, items, enums)
    return out


def surface(source: str) -> dict:
    """The CLI surface `src/cli/args.rs` defines: the top-level command with every subcommand, as the fake reads it."""
    items = _items(_strip(source))
    top = _command([], items["Cli"][2], items, _value_enums(items))
    return {"globals": [o for o in top["options"] if o["global"]], "subcommands": top["subcommands"]}


def read_source(rivet_src: Path, ref: str) -> str:
    """`src/cli/args.rs` at one ref of the rivet checkout; a ref the checkout lacks (CI clones the pinned commit only)
    is fetched first."""

    def show() -> subprocess.CompletedProcess:
        argv = ["git", "-C", str(rivet_src), "show", f"{ref}:{ARGS_RS}"]
        return subprocess.run(argv, capture_output=True, text=True, check=False)

    done = show()
    if done.returncode != 0:
        spec = f"refs/tags/{ref}:refs/tags/{ref}" if ref.startswith("v") else ref
        subprocess.run(["git", "-C", str(rivet_src), "fetch", "-q", "--depth", "1", "origin", spec], check=False)
        done = show()
    if done.returncode != 0:
        raise RuntimeError(f"cannot read {ARGS_RS} at {ref} in {rivet_src}: {done.stderr.strip()}")
    return done.stdout


def table(rivet_src: Path) -> dict:
    """The whole checked-in table, regenerated from the rivet checkout."""
    out = {"_regenerate": REGENERATE}
    for version, ref in VERSIONS.items():
        ref = ref or contract_ref()
        out[version] = {"ref": ref, **surface(read_source(rivet_src, ref))}
    return out


def rivet_src() -> Path:
    """The rivet checkout, as tests/conftest.py finds it."""
    return Path(os.environ.get("RIVET_SRC") or PACKAGE_ROOT.parent / "rivet").resolve()


if __name__ == "__main__":
    TABLE.write_text(json.dumps(table(rivet_src()), indent=1, sort_keys=True) + "\n")
    print(f"wrote {TABLE.relative_to(PACKAGE_ROOT)}", file=sys.stderr)
