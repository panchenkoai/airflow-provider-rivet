"""One Airflow stack for the gate: the image, compose up/down, the source and state databases, and the REST client."""

from __future__ import annotations

import ast
import base64
import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
PACKAGE_FILES = ("airflow_provider_rivet", "pyproject.toml", "README.md", "LICENSE", "CHANGELOG.md")
STATE_CONN = {"conn_type": "postgres", "host": "gate-state", "port": 5432, "login": "rivet", "password": "rivet",
              "schema": "rivet_state", "extra": {"rivet_params": "sslmode=require"}}
SOURCE_DSN = "host=127.0.0.1 port={port} dbname=rivet user=rivet password=rivet"


@dataclass(frozen=True)
class Flavor:
    """What differs between the Airflow major versions the gate runs."""

    version: str
    api: str
    ui_service: str
    health: str
    services: tuple[str, ...]


FLAVORS = {
    "2.10.5": Flavor("2.10.5", "/api/v1", "airflow-webserver", "/health",
                     ("airflow-webserver", "airflow-scheduler", "airflow-worker", "airflow-triggerer", "airflow-init", "airflow-cli")),
    "3.3.2": Flavor("3.3.2", "/api/v2", "airflow-apiserver", "/api/v2/monitor/health",
                    ("airflow-apiserver", "airflow-scheduler", "airflow-dag-processor", "airflow-worker", "airflow-triggerer",
                     "airflow-init", "airflow-cli")),
}

OVERLAY = """\
x-rivet: &rivet
  image: {image}
  environment:
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW_CONN_RIVET_GATE_SOURCE: postgresql://rivet:rivet@gate-source:5432/rivet
    AIRFLOW_CONN_RIVET_GATE_STATE: '{state_conn}'
  volumes:
    - {rivet_dir}:/opt/rivet
services:
{services}
  gate-source:
    image: postgres:16
    environment: {{ POSTGRES_USER: rivet, POSTGRES_PASSWORD: rivet, POSTGRES_DB: rivet }}
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10"]
    ports: ["127.0.0.1::5432"]
    healthcheck: {{ test: ["CMD", "pg_isready", "-U", "rivet"], interval: 5s, retries: 20 }}
  gate-state:
    image: postgres:16
    environment: {{ POSTGRES_USER: rivet, POSTGRES_PASSWORD: rivet, POSTGRES_DB: rivet_state }}
    command: ["bash", "-c", "install -d -o postgres -g postgres -m 700 /ssl && openssl req -new -x509 -days 2 -nodes -subj /CN=gate-state -out /ssl/server.crt -keyout /ssl/server.key && chown postgres:postgres /ssl/server.crt /ssl/server.key && chmod 600 /ssl/server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/ssl/server.crt -c ssl_key_file=/ssl/server.key"]
    healthcheck: {{ test: ["CMD", "pg_isready", "-U", "rivet"], interval: 5s, retries: 20 }}
"""


def sh(args: list[str], *, input: str | None = None, timeout: float = 900, check: bool = True, **kw: Any) -> subprocess.CompletedProcess:
    """Run a command, failing with its output when `check` is set."""
    done = subprocess.run(args, input=input, text=True, capture_output=True, timeout=timeout, **kw)
    if check and done.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:6])} exited {done.returncode}: {(done.stderr or done.stdout).strip()[-2000:]}")
    return done


class Stand:
    """A compose project under `work/<version>`: Airflow from the official compose plus the gate overlay."""

    def __init__(self, version: str, rivet_bin: Path, work: Path) -> None:
        self.flavor = FLAVORS[version]
        self.rivet_bin = rivet_bin
        self.dir = work / f"airflow-{version}"
        self.rivet_dir = self.dir / "rivet"
        self.project = "rivet-gate-af" + version.replace(".", "")
        self.image = f"rivet-gate-airflow:{version}"
        self.ui_port = 0
        self.source_dsn = ""
        self._token: str | None = None

    def compose(self, *args: str, **kw: Any) -> subprocess.CompletedProcess:
        """`docker compose` on this stack's project."""
        files = ["-f", str(self.dir / "docker-compose.yaml"), "-f", str(self.dir / "docker-compose.gate.yaml")]
        env = {**os.environ, "AIRFLOW_UID": str(os.getuid()), "AIRFLOW_PROJ_DIR": str(self.dir)}
        return sh(["docker", "compose", "-p", self.project, *files, *args], env=env, **kw)

    def build(self) -> None:
        """The Airflow image with the rivet binary under test and this provider."""
        ctx = self.dir / "image"
        shutil.copy2(self.rivet_bin, ctx / "rivet")
        for name in PACKAGE_FILES:
            src = REPO / name
            if src.is_dir():
                shutil.copytree(src, ctx / "provider" / name, ignore=shutil.ignore_patterns("__pycache__"))
            else:
                shutil.copy2(src, ctx / "provider" / name)
        sh(["docker", "build", "-q", "-t", self.image, "--build-arg", f"AIRFLOW_IMAGE=apache/airflow:{self.flavor.version}",
            "-f", str(HERE / "stand" / "Dockerfile"), str(ctx)], timeout=1800)

    def lay_out(self) -> None:
        """A fresh project dir: compose files, the DAG file, the rivet dirs the containers mount."""
        if self.dir.exists():
            shutil.rmtree(self.dir)
        for sub in ("dags", "logs", "config", "plugins", "image/provider", "rivet/configs", "rivet/out", "rivet/markers",
                    "rivet/state/sqlite", "rivet/state/pg"):
            (self.dir / sub).mkdir(parents=True)
        shutil.copy2(HERE / "stand" / f"airflow-{self.flavor.version}.yaml", self.dir / "docker-compose.yaml")
        (self.dir / ".env").write_text("")
        shutil.copy2(HERE / "stand" / "dags" / "rivet_gate.py", self.dir / "dags" / "rivet_gate.py")
        services = "\n".join(f"  {s}:\n    <<: *rivet" + ('\n    ports: !override ["127.0.0.1::8080"]' if s == self.flavor.ui_service else "")
                             for s in self.flavor.services)
        (self.dir / "docker-compose.gate.yaml").write_text(OVERLAY.format(
            image=self.image, state_conn=json.dumps(STATE_CONN), rivet_dir=self.rivet_dir, services=services))

    def down(self) -> None:
        """Remove the project's containers and volumes (a leftover from an earlier run too)."""
        sh(["docker", "compose", "-p", self.project, "down", "-v", "--remove-orphans"], timeout=900, check=False)

    def up(self) -> None:
        """Lay out, build, start, and wait until the API answers."""
        self.down()
        self.lay_out()
        self.build()
        # No `--wait`: an Airflow 2 webserver on a loaded host can miss the official healthcheck and come up later.
        self.compose("up", "-d", timeout=1800)
        self.ui_port = self.host_port(self.flavor.ui_service, 8080)
        self.source_dsn = SOURCE_DSN.format(port=self.host_port("gate-source", 5432))
        deadline = time.monotonic() + 900
        while True:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{self.ui_port}{self.flavor.health}", timeout=5)
                ready = [self.compose("exec", "-T", db, "pg_isready", "-U", "rivet", check=False).returncode == 0
                         for db in ("gate-source", "gate-state")]
                if all(ready):
                    return
            except (urllib.error.URLError, OSError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"airflow {self.flavor.version} API or the gate databases not ready after 900 s")
            time.sleep(3)

    def host_port(self, service: str, port: int) -> int:
        """The host port docker published for a service's container port."""
        return int(self.compose("port", service, str(port)).stdout.strip().rsplit(":", 1)[1])

    def psql(self, service: str, db: str, sql: str) -> str:
        """Run SQL through psql inside a database container; the unaligned result."""
        return self.compose("exec", "-T", service, "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", "rivet", "-d", db,
                            input=sql).stdout

    def _call(self, method: str, path: str, body: Any = None) -> Any:
        """One REST call with this version's auth; JSON in and out."""
        url = f"http://127.0.0.1:{self.ui_port}{self.flavor.api}{path}"
        headers = {"Content-Type": "application/json", "Authorization": self._auth()}
        data = json.dumps(body).encode() if body is not None else None
        with urllib.request.urlopen(urllib.request.Request(url, data=data, method=method, headers=headers), timeout=30) as r:
            return json.loads(r.read() or b"null")

    def _auth(self) -> str:
        """Basic auth on Airflow 2, a JWT from /auth/token on Airflow 3."""
        if self.flavor.version.startswith("2."):
            return "Basic " + base64.b64encode(b"airflow:airflow").decode()
        if self._token is None:
            req = urllib.request.Request(f"http://127.0.0.1:{self.ui_port}/auth/token", method="POST",
                                         data=json.dumps({"username": "airflow", "password": "airflow"}).encode(),
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                self._token = json.loads(r.read())["access_token"]
        return f"Bearer {self._token}"

    def wait_dags(self, dag_ids: list[str], timeout: float = 300) -> None:
        """Wait until every DAG is parsed, then unpause it."""
        deadline = time.monotonic() + timeout
        for dag in dag_ids:
            while True:
                try:
                    self._call("GET", f"/dags/{dag}")
                    break
                except urllib.error.HTTPError as exc:
                    if exc.code != 404 or time.monotonic() > deadline:
                        raise
                    time.sleep(3)
            self._call("PATCH", f"/dags/{dag}", {"is_paused": False})

    def trigger(self, dag: str, run_id: str) -> None:
        """Start a DAG run under a known id."""
        body: dict[str, Any] = {"dag_run_id": run_id}
        if not self.flavor.version.startswith("2."):
            body["logical_date"] = None
        self._call("POST", f"/dags/{dag}/dagRuns", body)

    def run_state(self, dag: str, run_id: str) -> str:
        """The DAG run's state."""
        return self._call("GET", f"/dags/{dag}/dagRuns/{run_id}")["state"]

    def tasks(self, dag: str, run_id: str) -> dict[str, dict[str, Any]]:
        """Task instances of a run by task id: state, start, end."""
        return {t["task_id"]: t for t in self._call("GET", f"/dags/{dag}/dagRuns/{run_id}/taskInstances")["task_instances"]}

    def xcom(self, dag: str, run_id: str, task: str) -> dict[str, Any]:
        """A task's return value (Airflow 2 serves it as Python repr text)."""
        value = self._call("GET", f"/dags/{dag}/dagRuns/{run_id}/taskInstances/{task}/xcomEntries/return_value")["value"]
        return ast.literal_eval(value) if isinstance(value, str) else value
