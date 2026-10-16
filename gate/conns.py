"""Where the stack's Connections came from, and what its logs show: graded from outside Airflow's own answer.

A Connection served by Google Secret Manager is proven by three reads that do not ask the secrets backend whether it
worked: no Airflow container carries its AIRFLOW_CONN_* variable (`docker inspect`), the metastore has no row for it,
and the secret is in Secret Manager; `airflow connections get` in the worker then resolves it with the fields the gate
published. A keyless gate_gcp Connection holds no key and no container holds GOOGLE_APPLICATION_CREDENTIALS, so the
GCS and BigQuery tasks that succeed ran as the identity the containers were given.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .stand import ADC_IN_CONTAINER, sh
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
    container_envs: Mapping[str, Sequence[str]],
    metastore_rows: int,
    listed: Sequence[str],
    secret: str,
    resolved: Mapping[str, Any] | None,
) -> list[str]:
    """Problems with the claim that `conn_id` reached Airflow from Secret Manager and from nowhere else."""
    problems = []
    var = f"AIRFLOW_CONN_{conn_id.upper()}="
    carriers = sorted(name for name, env in container_envs.items() if any(e.startswith(var) for e in env))
    if not container_envs:
        problems.append("no Airflow container inspected")
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


def log_hygiene(texts: Mapping[str, str], word: str) -> list[str]:
    """Each log that holds `word`, with its count (the first three logs named)."""
    hits = {name: text.count(word) for name, text in texts.items() if word in text}
    if not hits:
        return []
    shown = ", ".join(f"{n} ({c})" for n, c in sorted(hits.items())[:3])
    return [f"{sum(hits.values())} `{word}` in {len(hits)} log(s): {shown}"]


def parse_connection(stdout: str) -> dict[str, Any] | None:
    """The one Connection `airflow connections get <id> -o json` printed (lines before the JSON are skipped)."""
    at = stdout.find("[")
    if at < 0:
        return None
    try:
        rows = json.loads(stdout[at:])
    except json.JSONDecodeError:
        return None
    return rows[0] if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict) else None


# --- Reads of a running stack ----------------------------------------------------------------------------------------


def container_envs(stand: Any) -> dict[str, list[str]]:
    """`Config.Env` of every container of the stack's Airflow services, by service."""
    out = {}
    for service in stand.flavor.services:
        if service == "airflow-init" or service == "airflow-cli":
            continue
        ids = stand.compose("ps", "-q", service).stdout.split()
        for cid in ids:
            env = sh(["docker", "inspect", "--format", "{{json .Config.Env}}", cid]).stdout
            out[service] = json.loads(env or "[]")
    return out


def resolve(stand: Any, conn_id: str) -> dict[str, Any] | None:
    """The Connection as the worker's Airflow resolves it (secrets backend, variables, metastore, in that order)."""
    done = stand.compose("exec", "-T", "airflow-worker", "airflow", "connections", "get", conn_id, "-o", "json", check=False)
    return parse_connection(done.stdout) if done.returncode == 0 else None


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
    if cloud.gcp_auth == "adc":
        v.check(
            "connections/gate_gcp/keyless",
            lambda: keyless(resolve(stand, "gate_gcp"), envs, adc_writable(stand, ADC_IN_CONTAINER)),
            ok="project only; credential mounted read-only",
        )
