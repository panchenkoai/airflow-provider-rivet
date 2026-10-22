"""Where the stack's Connections came from, and what its logs show: graded from outside Airflow's own answer.

A Connection served by Google Secret Manager is proven by three reads that do not ask the secrets backend whether it
worked: no Airflow container carries its AIRFLOW_CONN_* variable (`docker inspect`), the metastore has no row for it,
and the secret is in Secret Manager; `airflow connections get` in the worker then resolves it with the fields the gate
published. A keyless gate_gcp Connection holds no key and no container holds GOOGLE_APPLICATION_CREDENTIALS, so the
GCS and BigQuery tasks that succeed ran as the identity the containers were given.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .stand import ADC_IN_CONTAINER, CLIENT_STATE_DB, GCP_KEY_IN_CONTAINER, sh
from .warehouse import _secrets

#: Extras of a google_cloud_platform Connection that carry or point at a credential.
GCP_KEY_EXTRAS = ("keyfile_dict", "key_path", "keyfile_path", "credential_config_file", "key_secret_name")
#: Fields compared between what the gate published and what Airflow resolved (never the password).
COMPARED = ("conn_type", "host", "login", "port", "schema")
MASK = "***"
EMPTY_FERNET = "empty cryptography key"


def from_secret_manager(
    conn_id: str,
    published: Mapping[str, Any],
    container_envs: Mapping[str, Sequence[str] | None],
    metastore_rows: int,
    listed: Sequence[str],
    secret: str,
    resolved: Mapping[str, Any] | None,
) -> list[str]:
    """Problems with the claim that `conn_id` reached Airflow from Secret Manager and from nowhere else."""
    problems = []
    var = f"AIRFLOW_CONN_{conn_id.upper()}="
    carriers = sorted(name for name, env in container_envs.items() if env and any(e.startswith(var) for e in env))
    down = sorted(name for name, env in container_envs.items() if env is None)
    if not container_envs:
        problems.append("no Airflow container inspected")
    if down:
        problems.append(f"no running container of {down}: its environment was not checked")
    if carriers:
        problems.append(f"{var[:-1]} is set in {carriers}")
    if metastore_rows:
        problems.append(f"the metastore holds {metastore_rows} row(s) for {conn_id}")
    if secret not in listed:
        problems.append(f"secret {secret} is not in Secret Manager")
    if resolved is None:
        problems.append(f"`airflow connections get {conn_id}` resolved nothing")
    else:
        for field in COMPARED:
            want, got = published.get(field), resolved.get(field)
            if want is not None and str(want) != str(got):
                problems.append(f"{field}: published {want!r}, resolved {got!r}")
    return problems


def keyless(resolved: Mapping[str, Any] | None, container_envs: Mapping[str, Sequence[str]], adc_writable: bool) -> list[str]:
    """Problems with the claim that gate_gcp carries no credential and rivet ran as the containers' own identity."""
    if resolved is None:
        return ["`airflow connections get gate_gcp` resolved nothing"]
    extra = resolved.get("extra_dejson") or {}
    problems = [
        f"gate_gcp extra `{k}` is set" for k in GCP_KEY_EXTRAS if extra.get(k) or extra.get(f"extra__google_cloud_platform__{k}")
    ]
    if not (extra.get("project") or extra.get("extra__google_cloud_platform__project")):
        problems.append("gate_gcp has no `project`")
    carriers = sorted(n for n, env in container_envs.items() if any(e.startswith("GOOGLE_APPLICATION_CREDENTIALS=") for e in env))
    if carriers:
        problems.append(f"GOOGLE_APPLICATION_CREDENTIALS is set in {carriers}")
    if adc_writable:
        problems.append("the mounted credential is writable from the worker")
    return problems


def env_key_file(
    resolved: Mapping[str, Any] | None,
    container_envs: Mapping[str, Sequence[str] | None],
    key_writable: bool,
    tasks: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Problems with the claim that rivet authenticated to GCP with the key file the compose env names, and nothing
    else: no GCP Connection, GOOGLE_APPLICATION_CREDENTIALS=/etc/gcloud_cred.json in every worker, the file read-only,
    and the stream's run and loads (GCS, BigQuery) went through."""
    problems = []
    if resolved is not None:
        problems.append("a gate_gcp Connection resolves")
    workers = {n: env for n, env in container_envs.items() if n.startswith("airflow-worker")}
    if not workers:
        problems.append("no worker container inspected")
    for name, env in sorted(workers.items()):
        if env is None or f"GOOGLE_APPLICATION_CREDENTIALS={GCP_KEY_IN_CONTAINER}" not in env:
            problems.append(f"{name}: GOOGLE_APPLICATION_CREDENTIALS is not {GCP_KEY_IN_CONTAINER}")
    carriers = sorted(n for n, env in container_envs.items() if env and any(e.startswith("AIRFLOW_CONN_GATE_GCP=") for e in env))
    if carriers:
        problems.append(f"AIRFLOW_CONN_GATE_GCP is set in {carriers}")
    if key_writable:
        problems.append("the key file is writable from the worker")
    gcp = {t: i.get("state") for t, i in tasks.items() if t == "run" or t.startswith("load.")}
    if not gcp or any(state != "success" for state in gcp.values()):
        problems.append(f"the stream's run and loads did not all succeed: {gcp or 'none'}")
    return problems


def env_json_connection(
    container_envs: Mapping[str, Sequence[str] | None],
    resolved: Mapping[str, Any] | None,
    stream_rows: int,
    tasks: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    """Problems with the claim that the client's `rivet_state` Connection, an AIRFLOW_CONN_* JSON variable, reached
    rivet with `sslmode=disable`: in every worker's env (host rivet-state, rivet_params sslmode=disable), resolved by
    Airflow, and the stream's state rows are in its database after a run that succeeded."""
    problems = []
    workers = {n: env for n, env in container_envs.items() if n.startswith("airflow-worker")}
    if not workers:
        problems.append("no worker container inspected")
    for name, env in sorted(workers.items()):
        value = next((e.split("=", 1)[1] for e in env or [] if e.startswith("AIRFLOW_CONN_RIVET_STATE=")), None)
        if value is None:
            problems.append(f"{name}: no AIRFLOW_CONN_RIVET_STATE")
            continue
        try:
            conn = json.loads(value)
        except json.JSONDecodeError:
            problems.append(f"{name}: AIRFLOW_CONN_RIVET_STATE is not JSON")
            continue
        extra = conn.get("extra") or {}
        if (
            conn.get("host") != "rivet-state"
            or (extra.get("rivet_params") if isinstance(extra, dict) else None) != "sslmode=disable"
        ):
            problems.append(
                f"{name}: host {conn.get('host')!r}, rivet_params {extra.get('rivet_params') if isinstance(extra, dict) else extra!r}"
            )
    if resolved is None:
        problems.append("`airflow connections get rivet_state` resolved nothing")
    elif resolved.get("host") != "rivet-state" or "sslmode=disable" not in json.dumps(
        resolved.get("extra_dejson") or resolved.get("extra") or {}
    ):
        problems.append(f"rivet_state resolved to host {resolved.get('host')!r} without sslmode=disable")
    if stream_rows <= 0:
        problems.append("no state row of the stream in the client's rivet_state database")
    if (tasks.get("run") or {}).get("state") != "success":
        problems.append(f"the stream's run {(tasks.get('run') or {}).get('state')}")
    return problems


def as_delivered(copied: Mapping[str, str], now: Mapping[str, str]) -> list[str]:
    """The client's files the stack ran are byte for byte the ones copied from the delivery (sha256 by name)."""
    if not copied:
        return ["no client file copied"]
    return [
        f"{name}: {copied[name][:12]} copied, {now.get(name, 'missing')[:12]} at the end"
        for name in sorted(copied)
        if now.get(name) != copied[name]
    ]


def log_hygiene(texts: Mapping[str, str], word: str) -> list[str]:
    """Each log that holds `word`, with its count (the first three logs named)."""
    hits = {name: text.count(word) for name, text in texts.items() if word in text}
    if not hits:
        return []
    shown = ", ".join(f"{n} ({c})" for n, c in sorted(hits.items())[:3])
    return [f"{sum(hits.values())} `{word}` in {len(hits)} log(s): {shown}"]


def parse_connection(stdout: str) -> dict[str, Any] | None:
    """The one Connection `airflow connections get <id> -o json` printed (lines before the JSON are skipped)."""
    # A log line before the JSON may hold a `[` itself (Airflow 3: `... [info     ] Getting connection ...`), so the JSON
    # is the first line that starts with one and decodes.
    rows = None
    for at in (m.start(1) for m in re.finditer(r"^\s*(\[)", stdout, re.M)):
        try:
            rows = json.JSONDecoder().raw_decode(stdout, at)[0]
            break
        except json.JSONDecodeError:
            continue
    return rows[0] if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict) else None


# --- Reads of a running stack ----------------------------------------------------------------------------------------


def container_envs(stand: Any) -> dict[str, list[str] | None]:
    """`Config.Env` of every container of the stack's Airflow services, by `service/container`; a service with no
    running container is `service: None`, so a check of every container cannot pass on one that is down."""
    out: dict[str, list[str] | None] = {}
    for service in stand.flavor.services:
        if service in ("airflow-init", "airflow-cli"):
            continue
        ids = stand.compose("ps", "-q", service).stdout.split()
        if not ids:
            out[service] = None
        for cid in ids:
            env = sh(["docker", "inspect", "--format", "{{json .Config.Env}}", cid]).stdout
            out[f"{service}/{cid[:12]}"] = json.loads(env or "[]")
    return out


def resolve(stand: Any, conn_id: str) -> dict[str, Any] | None:
    """The Connection as the worker's Airflow resolves it (secrets backend, variables, metastore, in that order)."""
    done = stand.compose("exec", "-T", "airflow-worker", "airflow", "connections", "get", conn_id, "-o", "json", check=False)
    found = parse_connection(done.stdout) if done.returncode == 0 else None
    if found is None:  # the cell says only "resolved nothing": the cause goes to the gate log (stderr only, no values)
        print(f"connections get {conn_id}: exit {done.returncode}: {done.stderr.strip()[-2000:]}", file=sys.stderr)
    return found


def metastore_rows(stand: Any, conn_id: str) -> int:
    """Rows of Airflow's `connection` table for the conn_id."""
    out = stand.compose(
        "exec", "-T", "postgres", "psql", "-At", "-U", "airflow", "-d", "airflow",
        "-c", f"SELECT count(*) FROM connection WHERE conn_id = '{conn_id}'",
    ).stdout  # fmt: skip
    return int(out.strip() or 0)


def adc_writable(stand: Any, path: str) -> bool:
    """Whether the worker can write the mounted credential file."""
    return stand.compose("exec", "-T", "airflow-worker", "test", "-w", path, check=False).returncode == 0


def stream_tasks(stand: Any) -> dict[str, dict[str, Any]]:
    """The MySQL stream's task instances (its run and its loads read GCS and BigQuery): under `client-override` the
    README's stream (gate/client.py), on the client's Connections; else the leg's first cycle."""
    dag, run_id = (
        ("gate_client_cron", "gate_client_cron_1")
        if getattr(stand, "client_override", None)
        else ("gate_src_mysql_cdc", "gate_1_initial")
    )
    try:
        return stand.tasks(dag, run_id)
    except Exception:  # no such run: the cells say "none"
        return {}


def stream_state_rows(stand: Any) -> int:
    """How many rows of the client's state database hold the stream's progress (export_state and run_status)."""
    text = stand.state_admin_sql(
        CLIENT_STATE_DB, "SELECT (SELECT count(*) FROM export_state) + (SELECT count(*) FROM run_status);"
    )
    return int(text.strip() or 0)


def client_file_hashes(stand: Any) -> dict[str, str]:
    import hashlib

    return {n: hashlib.sha256((Path(stand.dir) / n).read_bytes()).hexdigest() for n in stand.client_files}


def task_logs(stand: Any) -> dict[str, str]:
    """Every task log file of the stack, by path relative to its logs folder."""
    root = Path(stand.dir) / "logs"
    return {str(p.relative_to(root)): p.read_text(errors="replace") for p in root.rglob("*.log") if p.is_file()}


def service_logs(stand: Any) -> dict[str, str]:
    """`docker compose logs` of each Airflow service."""
    return {s: stand.compose("logs", "--no-color", s, check=False).stdout for s in stand.flavor.services}


def prove(stand: Any, v: Any, cloud: Any | None) -> None:
    """The connection-source and log-hygiene cells of one stack."""
    logs = v.guard("hygiene/task-logs", task_logs, stand)
    if logs is not None:
        v.check("hygiene/no-masked-words", log_hygiene, logs, MASK, ok=f"{len(logs)} task log(s), no `{MASK}`")
        services = v.guard("hygiene/service-logs", service_logs, stand) or {}
        v.check("hygiene/fernet-key-set", log_hygiene, {**logs, **services}, EMPTY_FERNET, ok=f"no `{EMPTY_FERNET}`")
    if cloud is None:
        return
    envs = v.guard("connections/container-env", container_envs, stand)
    if envs is None:
        return
    if cloud.connections_from == "gsm":
        listed = [r["id"] for r in (v.guard("connections/secret-listing", _secrets, cloud) or [])]
        for conn_id, conn in stand.connections().items():
            v.check(
                f"connections/{conn_id}/from-secret-manager",
                lambda c=conn_id, p=conn: from_secret_manager(
                    c, p, envs, metastore_rows(stand, c), listed, cloud.secret(stand.flavor.version, c), resolve(stand, c)
                ),
                ok=cloud.secret(stand.flavor.version, conn_id),
            )
    if cloud.gcp_auth == "env-file":
        family = "client-override" if getattr(stand, "client_override", None) else "connections"
        stream = stream_tasks(stand)
        v.check(
            f"{family}/gcp/env-key-file",
            lambda: env_key_file(resolve(stand, "gate_gcp"), envs, adc_writable(stand, GCP_KEY_IN_CONTAINER), stream),
            ok=f"GOOGLE_APPLICATION_CREDENTIALS={GCP_KEY_IN_CONTAINER} (read-only), no GCP Connection, run and loads ok",
        )
    if getattr(stand, "client_override", None):
        # README step 5 builds a state URL from $RIVET_STATE_USER / _PASSWORD / _DB inside the worker: are they there?
        names = ("RIVET_STATE_USER", "RIVET_STATE_PASSWORD", "RIVET_STATE_DB")
        worker = next((env for n, env in envs.items() if n.startswith("airflow-worker") and env), [])
        print(
            f"MEASURE client-override/readme/step5-variables: {[(n, any(e.startswith(n + '=') for e in worker)) for n in names]}",
            flush=True,
        )
        stream = stream_tasks(stand)
        v.check(
            "client-override/conn/env-json-connection",
            lambda: env_json_connection(envs, resolve(stand, "rivet_state"), stream_state_rows(stand), stream),
            ok="AIRFLOW_CONN_RIVET_STATE (JSON, host rivet-state, sslmode=disable) resolved; the stream's rows in rivet_state",
        )
        v.check(
            "client-override/files/as-delivered",
            lambda: as_delivered(stand.client_files, client_file_hashes(stand)),
            ok=", ".join(f"{n} {h[:12]}" for n, h in sorted(stand.client_files.items())),
        )
    if cloud.gcp_auth == "adc":
        v.check(
            "connections/gate_gcp/keyless",
            lambda: keyless(resolve(stand, "gate_gcp"), envs, adc_writable(stand, ADC_IN_CONTAINER)),
            ok="project only; credential mounted read-only",
        )
