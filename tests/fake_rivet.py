"""A stand-in `rivet` that replays one scenario from `scenario.json` beside it and logs every call."""

import json
import os
import signal
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
BASE_FLAGS = {
    "run": ["--config", "--export", "--summary-output", "--json", "--json-errors"],
    "plan": ["--config", "--export", "--format", "--output", "--json-errors"],
    "apply": ["--resume", "--force", "--json-errors"],
    "load": ["--config", "--run-id", "--pool", "--json-errors"],
    "compact": ["--config", "--run-id", "--pool", "--json-errors"],
    "metrics": ["--config", "--export", "--last", "--json", "--json-errors"],
}
QUERY_FILE_RULES = (
    "query_file must be a relative path",
    "query_file path must not contain '..'",
    "resolves outside the config directory",
)


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


def main():
    """Answer `--version` / `--help`, or replay the scenario for the subcommand."""
    spec = json.loads((HERE / "scenario.json").read_text())
    argv = sys.argv[1:]
    broken = spec.get("probe_broken", {})
    if argv == ["--version"]:
        if broken.get("--version") == "hang":
            time.sleep(90)
        print(f"rivet {spec.get('version', '0.31.0')} (fake)")
        return 0
    sub = argv[0]
    if "--help" in argv:
        if broken.get(sub) == "hang":
            time.sleep(90)
        if broken.get(sub) == "exit":
            print(f"error: unrecognized subcommand '{sub}'", file=sys.stderr)
            return 2
        flags = BASE_FLAGS.get(sub, []) + spec.get("flags", {}).get(sub, []) + spec.get("flags", {}).get("*", [])
        print("Usage: rivet " + sub + " [OPTIONS]\n\nOptions:\n" + "\n".join(f"      {f} <X>" for f in flags))
        return 0
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
