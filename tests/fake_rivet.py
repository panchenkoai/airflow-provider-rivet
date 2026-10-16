"""A stand-in `rivet` that replays one scenario from `scenario.json` beside it and logs every call."""

import json
import os
import signal
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUERY_FILE_RULES = (
    "query_file must be a relative path",
    "query_file path must not contain '..'",
    "resolves outside the config directory",
)


class Clap(Exception):
    """clap's refusal of a command line: exit 2, its message on stderr (src/cli/args.rs `parse_cli`; clap 4)."""

    def __init__(self, message, usage=None):
        tail = f"\n\nUsage: {usage}" if usage else ""
        super().__init__(f"error: {message}{tail}\n\nFor more information, try '--help'.")


def surface_of(spec):
    """The CLI of the rivet this scenario impersonates (`rivet_cli_surface.json`, regenerated from rivet's clap
    definitions by tests/rivet_cli_surface.py): `cli` names it, else the version (`main` for any but a release in it).

    `flags` adds what a newer rivet would have, per subcommand or `*` for every one: `--name` a switch, `--name <V>` a
    flag with a value. The fake accepts those and nothing else rivet does not."""
    table = json.loads((HERE / "rivet_cli_surface.json").read_text())
    version = spec.get("version", "0.31.0")
    key = spec.get("cli") or (version if version in table else "main")
    return table[key], spec.get("flags", {})


def extra_option(text):
    """A scenario's extra flag as the surface spells an option."""
    name, _, value = text.partition(" ")
    return {
        "long": name[2:],
        "short": None,
        "aliases": [],
        "switch": not value,
        "multiple": False,
        "num_args": None,
        "value_name": value.strip("<>") or None,
        "kind": "string",
        "values": None,
        "required": False,
    }


def shown(opt):
    """How clap names an option in its messages: `--config <CONFIG>`."""
    return f"--{opt['long']}" + ("" if opt["switch"] else f" <{opt['value_name']}>")


def usage(path, cmd, globals_):
    """clap's usage line of a (sub)command."""
    words = ["rivet", *path, "[OPTIONS]"]
    if cmd.get("subcommands"):
        return " ".join([*words, "<COMMAND>"])
    words += [shown(o) for o in cmd["options"] if o["required"]]
    return " ".join(words + [f"<{p['name'].upper()}>" if p["required"] else f"[{p['name'].upper()}]" for p in cmd["positionals"]])


def help_text(path, cmd, globals_):
    """`rivet <sub> --help` as clap prints it: one line per option, the flag first."""
    lines = [f"Usage: {usage(path, cmd, globals_)}", ""]
    if cmd.get("subcommands"):
        lines += ["Commands:"] + [f"  {name}" for name in [*cmd["subcommands"], "help"]] + [""]
    lines.append("Options:")
    for opt in [*cmd["options"], *globals_]:
        short = f"-{opt['short']}, " if opt["short"] else "    "
        lines.append(f"  {short}{shown(opt)}")
        lines += [f"          [aliases: --{a}]" for a in opt["aliases"]]
    lines.append("  -h, --help  Print help")
    if not path:
        lines.append("  -V, --version  Print version")
    return "\n".join(lines)


def check_value(opt, value):
    """clap's value parser for an option: an integer type or a `ValueEnum`."""
    if opt["kind"] == "int" and not value.isdigit():
        reason = "cannot parse integer from empty string" if not value else "invalid digit found in string"
        raise Clap(f"invalid value '{value}' for '{shown(opt)}': {reason}")
    if opt["kind"] == "enum" and value not in opt["values"]:
        raise Clap(f"invalid value '{value}' for '{shown(opt)}'\n  [possible values: {', '.join(opt['values'])}]")


def parse_cli(argv, surface, extras):
    """Walk argv as clap would: ("help", text) for `--help`, ("version", None), or ("run", subcommand path).

    Refuses with `Clap` what real rivet refuses: an unknown subcommand, an unknown flag, a flag's missing or invalid
    value, a single-valued flag given twice, a missing required flag or positional, a stray positional. Not modelled:
    argument groups, `requires` and `conflicts_with` (no command line of the package reaches them)."""
    globals_ = surface["globals"]
    cmd, path, seen, positionals, i = {"options": [], "positionals": [], "subcommands": surface["subcommands"]}, [], {}, [], 0
    while i < len(argv):
        arg = argv[i]
        i += 1
        options = cmd["options"] + globals_
        if arg in ("-h", "--help"):
            return "help", help_text(path, cmd, globals_)
        if arg in ("-V", "--version") and not path:
            return "version", None
        if arg == "--" and not cmd.get("subcommands"):
            positionals += argv[i:]
            break
        if arg.startswith("-") and len(arg) > 1:
            name, eq, inline = arg.partition("=") if arg.startswith("--") else (arg[:2], "", arg[2:].removeprefix("="))
            eq = eq or (inline and not arg.startswith("--"))
            opt = next(
                (
                    o
                    for o in options
                    if (name.startswith("--") and name[2:] in [o["long"], *o["aliases"]])
                    or (not name.startswith("--") and o["short"] == name[1:])
                ),
                None,
            )
            if opt is None:
                raise Clap(
                    f"unexpected argument '{arg if not eq or arg.startswith('--') else name}' found", usage(path, cmd, globals_)
                )
            if opt["long"] in seen and not opt["multiple"] and not opt.get("global"):
                raise Clap(f"the argument '{shown(opt)}' cannot be used multiple times", usage(path, cmd, globals_))
            seen[opt["long"]] = opt
            if opt["switch"]:
                if eq:
                    raise Clap(f"unexpected value '{inline}' for '{shown(opt)}' found; no more were expected")
                continue
            values = [inline] if eq else []
            while not values or opt["num_args"]:
                if i >= len(argv) or (argv[i].startswith("-") and len(argv[i]) > 1):
                    break
                values.append(argv[i])
                i += 1
            if not values:
                if i < len(argv):
                    nxt = argv[i]
                    known = any(
                        nxt.partition("=")[0][2:] in [o["long"], *o["aliases"]] or nxt[1:2] == o["short"] for o in options
                    )
                    if not known:
                        raise Clap(f"unexpected argument '{nxt}' found", usage(path, cmd, globals_))
                raise Clap(f"a value is required for '{shown(opt)}' but none was supplied")
            for value in values:
                check_value(opt, value)
            continue
        if cmd.get("subcommands"):
            if arg == "help":
                return "help", help_text(path, cmd, globals_)
            if arg not in cmd["subcommands"]:
                raise Clap(f"unrecognized subcommand '{arg}'", usage(path, cmd, globals_))
            path.append(arg)
            cmd = cmd["subcommands"][arg]
            if len(path) == 1:
                for flag in extras.get(arg, []) + extras.get("*", []):
                    cmd = {**cmd, "options": [*cmd["options"], extra_option(flag)]}
            continue
        positionals.append(arg)
    if cmd.get("subcommands"):
        if not path:
            raise Clap("'rivet' requires a subcommand but one was not provided", usage(path, cmd, globals_))
        raise Clap(f"'rivet {' '.join(path)}' requires a subcommand but one was not provided", usage(path, cmd, globals_))
    if len(positionals) > len(cmd["positionals"]):
        raise Clap(f"unexpected argument '{positionals[len(cmd['positionals'])]}' found", usage(path, cmd, globals_))
    missing = [shown(o) for o in cmd["options"] if o["required"] and o["long"] not in seen]
    missing += [f"<{p['name'].upper()}>" for p in cmd["positionals"][len(positionals) :] if p["required"]]
    if missing:
        raise Clap("the following required arguments were not provided:\n  " + "\n  ".join(missing), usage(path, cmd, globals_))
    return "run", path


def value_of(argv, flag):
    """The value following a flag, or None."""
    return argv[argv.index(flag) + 1] if flag in argv else None


def params_of(argv):
    """`-p KEY=VALUE` / `--param KEY=VALUE` (the spellings the package passes through)."""
    out = {}
    for i, arg in enumerate(argv[:-1]):
        if arg in ("-p", "--param") and "=" in argv[i + 1]:
            key, value = argv[i + 1].split("=", 1)
            out[key] = value
    return out


def substitute(text, params):
    """rivet's `resolve_vars` (src/config/resolve.rs): `${name}` from `-p` then the environment, spliced into the raw
    text before the YAML parse; an unset name is refused."""
    out, i = [], 0
    while (start := text.find("${", i)) >= 0:
        end = text.find("}", start)
        if end < 0:
            break
        name = text[start + 2 : end]
        if name and name not in params and name not in os.environ:
            raise KeyError(f"'${{{name}}}' in the config is unresolved")
        out += [text[i:start], params.get(name, os.environ.get(name, "")) if name else ""]
        i = end + 1
    return "".join(out + [text[i:]])


def read_config(argv):
    """The parsed config a call names as rivet reads it, its directory, and the refusal rivet prints when it cannot."""
    cfg = value_of(argv, "--config")
    if not cfg:
        return {}, None, None
    import yaml

    try:
        doc = yaml.safe_load(substitute(Path(cfg).read_text(), params_of(argv))) or {}
    except (KeyError, yaml.YAMLError) as exc:
        return {}, Path(cfg).resolve().parent, f"config file '{cfg}': {type(exc).__name__}"
    return doc, Path(cfg).resolve().parent, None


def query_files(sub, argv, doc, base):
    """rivet's `query_file` rule (src/config/export.rs, src/config/mod.rs): the refusal text, and the SQL read."""
    only, read = value_of(argv, "--export"), {}
    for entry in doc.get("exports") or []:
        name, ref = entry.get("name"), entry.get("query_file")
        if not ref:
            continue
        if Path(ref).is_absolute():
            return f"export '{name}': {QUERY_FILE_RULES[0]}: '{ref}'", read
        if ".." in Path(ref).parts:
            return f"export '{name}': {QUERY_FILE_RULES[1]}: '{ref}'", read
        if sub not in ("run", "plan") or only not in (None, name):
            continue
        joined = base / ref
        if joined.exists() and base not in joined.resolve().parents:
            return f"export '{name}': query_file '{ref}' {QUERY_FILE_RULES[2]}", read
        if not joined.is_file():
            return "No such file or directory (os error 2)", read
        read[name] = joined.read_text()
    return None, read


def unit_of(sub, argv, doc):
    """The export and table a call is about, as far as its arguments say."""
    export = value_of(argv, "--export")
    if export is None and sub == "apply" and len(argv) > 1:
        try:
            export = json.loads(Path(argv[1]).read_text()).get("export_name")
        except (OSError, ValueError):
            export = None
    names = [e.get("name") for e in doc.get("exports") or []]
    if export is None and sub in ("load", "compact") and len(names) == 1:
        export = names[0]
    return export or "", value_of(argv, "--table") or ""


def cursors(spec, update=None):
    """The fake state store's cursor per export (`cursors.json` beside the fake, seeded from the scenario's `cursors`)."""
    path = HERE / "cursors.json"
    held = json.loads(path.read_text()) if path.exists() else dict(spec.get("cursors", {}))
    if update is not None or not path.exists():
        held.update(update or {})
        path.write_text(json.dumps(held))
    return held


def stale_artifact(spec, argv, export):
    """rivet main's PA4 check (src/pipeline/apply_cmd.rs): a sealed snapshot the state's cursor has moved past."""
    try:
        snapshot = json.loads(Path(argv[1]).read_text()).get("computed", {}).get("cursor_snapshot")
    except (OSError, ValueError, AttributeError, IndexError):
        return False
    return snapshot is not None and "--force" not in argv and snapshot != cursors(spec).get(export)


def run_id_of(export):
    """rivet's `fresh_run_id` (src/pipeline/summary.rs): `<export>_<UTC %Y%m%dT%H%M%S%.3f>_<pid>`."""
    now = datetime.now(UTC)
    return f"{export}_{now:%Y%m%dT%H%M%S}.{now.microsecond // 1000:03d}_{os.getpid()}"


def record_metric(row):
    """rivet's `export_metrics` (src/state/metrics.rs): a `running` row while the run lives, replaced by its terminal
    row when it ends; a process killed mid-run leaves its `running` row behind. Kept in `metrics.json` beside the fake."""
    path = HERE / "metrics.json"
    rows = json.loads(path.read_text()) if path.exists() else []
    rows = [r for r in rows if r.get("run_id") != row["run_id"]] + [row]
    path.write_text(json.dumps(rows))


def counts_of(play, export):
    """(rows, files) the play's summary reports for the export, 0 when it reports none."""
    entries = (play.get("summary") or {}).get("per_export") if isinstance(play.get("summary"), dict) else None
    for entry in entries if isinstance(entries, list) else []:
        if isinstance(entry, dict) and entry.get("export_name") == export:
            return entry.get("rows") or 0, entry.get("files") or 0
    return 0, 0


def canonical(path):
    """A path as rivet's `canonicalize()` records it: symlinks resolved, each component as its directory lists it."""
    out = os.sep
    for part in Path(os.path.realpath(path)).parts[1:]:
        try:
            names = os.listdir(out)
            if part not in names:
                inode = os.lstat(os.path.join(out, part)).st_ino
                part = next((n for n in names if os.lstat(os.path.join(out, n)).st_ino == inode), part)
        except OSError:
            pass
        out = os.path.join(out, part)
    return out


def record_invocation(argv, kind, refusal=None):
    """Every start of the binary, probes and refused command lines included, with its whole environment and how it
    ended (`invocations.jsonl` beside the fake; `Rig.invocations`)."""
    row = {"argv": argv, "kind": kind, "env": dict(os.environ), "refusal": refusal}
    with open(HERE / "invocations.jsonl", "a") as log:
        log.write(json.dumps(row) + "\n")


def main():
    """Answer `--version` / `--help`, or replay the scenario for the subcommand."""
    spec = json.loads((HERE / "scenario.json").read_text())
    argv = sys.argv[1:]
    broken = spec.get("probe_broken", {})
    surface, extras = surface_of(spec)
    try:
        verdict, out = parse_cli(argv, surface, extras)
    except Clap as refusal:
        record_invocation(argv, "refused", str(refusal))
        print(refusal, file=sys.stderr)
        return 2
    if verdict == "version":
        record_invocation(argv, "probe")
        if broken.get("--version") == "hang":
            time.sleep(90)
        print(f"rivet {spec.get('version', '0.31.0')} (fake)")
        return 0
    if verdict == "help":
        record_invocation(argv, "probe")
        if broken.get(argv[0]) == "hang":
            time.sleep(90)
        if broken.get(argv[0]) == "exit":
            print(f"error: unrecognized subcommand '{argv[0]}'", file=sys.stderr)
            return 2
        print(out)
        return 0
    record_invocation(argv, "call")
    sub = argv[0]
    doc, base, unreadable = read_config(argv)
    refusal, sql = query_files(sub, argv, doc, base) if base else (None, {})
    refusal = unreadable or refusal
    export, table = unit_of(sub, argv, doc)
    seen = {k: os.environ.get(k) for k in spec.get("record_env", [])}
    call = {"argv": argv, "env": seen, "env_keys": sorted(os.environ), "cwd": os.getcwd(), "sql": sql, "export": export}
    call["config"] = doc if spec.get("record_config") and not unreadable else None
    with open(HERE / "calls.jsonl", "a") as log:
        log.write(json.dumps(call) + "\n")
    if refusal:
        text = refusal if unreadable else f"config file '{value_of(argv, '--config')}': {refusal}"
        print(json.dumps({"error": text, "exit_class": 1}), file=sys.stderr)
        return 1
    play = {}
    for key in (f"{sub}:{export}:{table}", f"{sub}:{export}", sub):
        if key in spec:
            play = spec[key]
            break
    if sub == "plan" and not play.get("exit"):
        out = value_of(argv, "--output")
        expires = (datetime.now(UTC) + timedelta(hours=spec.get("plan_expires_hours", 24))).isoformat()
        if out:
            source = canonical(value_of(argv, "--config"))
            snapshot = cursors(spec).get(value_of(argv, "--export"))
            doc = {"export_name": value_of(argv, "--export"), "expires_at": expires, "config_path": source}
            # rivet's `PlanArtifact.strategy` (src/plan/artifact.rs): the export's mode label
            modes = {e.get("name"): e.get("mode") for e in (read_config(argv)[0].get("exports") or [])}
            doc["strategy"] = modes.get(doc["export_name"])
            Path(out).write_text(json.dumps({**doc, "computed": {"cursor_snapshot": snapshot}}))
        else:
            # rivet prints one plan as its bare object (no campaign) and several as an array (plan_cmd.rs emit_artifacts)
            planned = spec.get("plan_list", [])
            print(json.dumps(planned[0] if isinstance(planned, list) and len(planned) == 1 else planned))
        return 0
    if sub == "apply" and stale_artifact(spec, argv, export):
        line = {"error": f"plan '{export}': cursor has drifted since plan was generated", "exit_class": 5}
        print(json.dumps({**line, "code": "RIVET_PLAN_ARTIFACT_STALE"}), file=sys.stderr)
        return 5
    if sub == "metrics":
        if "rows" in play:
            print(json.dumps(play["rows"]))
        else:
            held = json.loads((HERE / "metrics.json").read_text()) if (HERE / "metrics.json").exists() else []
            held = [r for r in reversed(held) if export in ("", r["export_name"])]
            print(json.dumps(held[: int(value_of(argv, "--last") or len(held))]))
        return play.get("exit", 0)
    metric = None
    if sub in ("run", "apply") and export:
        rows, files = counts_of(play, export)
        metric = {"export_name": export, "run_id": run_id_of(export), "status": "running", "total_rows": rows}
        metric["files_produced"] = files
        record_metric(metric)
    if play.get("stderr"):
        print(play["stderr"], file=sys.stderr)
    for name in spec.get("leak_env", []):
        print(f"connecting with {os.environ.get(name)}", file=sys.stderr)
    target = value_of(argv, "--summary-output")
    if target and play.get("summary") is not None:
        Path(target).write_text(json.dumps(play["summary"]))
    if target and play.get("summary_raw") is not None:
        Path(target).write_text(play["summary_raw"])
    elif target and play.get("summary") is None and not play.get("exit") and not play.get("no_summary"):
        # rivet writes the summary file on every exit-0 run it was asked for (src/pipeline/aggregate.rs `persist`).
        Path(target).write_text(json.dumps({"per_table" if sub in ("load", "compact") else "per_export": []}))
    if play.get("line") is not None:
        # serde_json writes non-ASCII raw (U+2028 included), not as `\u` escapes: so does the fake.
        sys.stderr.flush()
        sys.stderr.buffer.write((json.dumps(play["line"], ensure_ascii=False) + "\n").encode())
    if play.get("line_raw") is not None:
        sys.stderr.flush()
        sys.stderr.buffer.write((play["line_raw"] + "\n").encode())
    for index in range(play.get("stderr_after", 0)):
        print(f"warning: trailing line {index}", file=sys.stderr)
    sys.stderr.flush()
    if play.get("sleep"):
        if play.get("ignore_term"):
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        elif play.get("term_marker"):

            def on_term(signum, frame):
                Path(play["term_marker"]).write_text(str(os.getpid()))
                os._exit(143)

            signal.signal(signal.SIGTERM, on_term)
        Path(play["started"]).write_text(str(os.getpid()))
        time.sleep(play["sleep"])
    if play.get("signal"):
        os.kill(os.getpid(), getattr(signal, play["signal"]))
    if sub == "apply" and not play.get("exit"):
        cursors(spec, {export: str(int(cursors(spec).get(export) or 0) + 1)})
    if metric is not None:
        record_metric({**metric, "status": "failed" if play.get("exit") else "success"})
    return play.get("exit", 0)


sys.exit(main())
