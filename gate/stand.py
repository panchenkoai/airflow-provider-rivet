"""One Airflow stack for the gate: the image, compose up/down, the source and state databases, and the REST client."""

from __future__ import annotations

import ast
import base64
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
PACKAGE_FILES = ("airflow_provider_rivet", "pyproject.toml", "README.md", "LICENSE", "CHANGELOG.md")
#: The client's GCP key file, named by GOOGLE_APPLICATION_CREDENTIALS in the compose env (`--gcp-auth env-file`).
GCP_KEY_IN_CONTAINER = "/etc/gcloud_cred.json"
#: ELF `e_machine` -> the manylinux platform tag of a `rivet-cli` wheel (rivet's dev/pytools/pypi_wheels.py tags).
ELF_PLATFORMS = {0xB7: "manylinux_2_28_aarch64", 0x3E: "manylinux_2_28_x86_64"}
RIVET_CLI_MODULE = '''"""The rivet binaries this wheel installed."""

import os
import shutil
import sysconfig

__version__ = "{version}"


def path(name: str = "rivet") -> str:
    """Absolute path of `name` from this wheel: the environment's scripts directory, else PATH."""
    candidate = os.path.join(sysconfig.get_path("scripts"), name)
    if os.access(candidate, os.X_OK):
        return candidate
    found = shutil.which(name)
    if found is None:
        raise FileNotFoundError(f"{{name}} is not in {{sysconfig.get_path('scripts')}} nor on PATH")
    return found
'''


#: The provider version the client's Dockerfile.rivet pins. Not released yet: the gate builds this checkout under it
#: (an adaptation, reported), since `--find-links` only finds a wheel of the pinned version.
CLIENT_PROVIDER_VERSION = "0.2.0"


def provider_wheel(out: Path, version: str) -> Path:
    """This checkout's provider as a wheel of `version` (the package's `__version__` set to it in a copy)."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "provider"
        for name in PACKAGE_FILES:
            if (REPO / name).is_dir():
                shutil.copytree(REPO / name, src / name, ignore=shutil.ignore_patterns("__pycache__"))
            else:
                src.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPO / name, src / name)
        init = src / "airflow_provider_rivet" / "__init__.py"
        text = init.read_text()
        bumped = re.sub(r'^__version__ = "[^"]+"', f'__version__ = "{version}"', text, count=1, flags=re.M)
        if bumped == text and f'__version__ = "{version}"' not in text:
            raise ValueError("no __version__ line in airflow_provider_rivet/__init__.py")
        init.write_text(bumped)
        sh(["uv", "build", "--wheel", "--out-dir", str(out), str(src)], timeout=600)
    found = sorted(out.glob(f"airflow_provider_rivet-{version}-*.whl"))
    if len(found) != 1:
        raise RuntimeError(f"uv build left {sorted(out.glob('*.whl'))} in {out}, expected one provider wheel {version}")
    return found[0]


def rivet_version(shown: str) -> str:
    """`rivet 0.32.2 (7fba31c4)` -> `0.32.2`."""
    found = re.search(r"\brivet (\d+\.\d+\.\d+)\b", shown)
    if not found:
        raise ValueError(f"no rivet version in {shown!r}")
    return found.group(1)


def elf_platform(data: bytes) -> str:
    """The wheel platform tag of a Linux rivet binary, from its ELF header (a binary of no known machine raises)."""
    if data[:4] != b"\x7fELF":
        raise ValueError("not an ELF binary: the client image installs a Linux rivet")
    machine = int.from_bytes(data[18:20], "little")
    if machine not in ELF_PLATFORMS:
        raise ValueError(f"ELF machine {machine:#x}: no wheel platform for it")
    return ELF_PLATFORMS[machine]


def rivet_cli_wheel(binary: Path, version: str, out: Path) -> Path:
    """`rivet_cli-<version>-py3-none-<platform>.whl` holding `binary` as the `rivet` script, the layout of rivet's own
    `rivet-cli` wheels (dev/pytools/pypi_wheels.py): what `pip install rivet-cli==<version>` would install."""
    data = binary.read_bytes()
    platform = elf_platform(data)
    dist = "rivet_cli"
    info = f"{dist}-{version}.dist-info"
    files = [
        (f"{dist}-{version}.data/scripts/rivet", data, 0o755),
        (f"{dist}/__init__.py", RIVET_CLI_MODULE.format(version=version).encode(), 0o644),
        (
            f"{info}/METADATA",
            f"Metadata-Version: 2.1\nName: rivet-cli\nVersion: {version}\nRequires-Python: >=3.8\n".encode(),
            0o644,
        ),
        (
            f"{info}/WHEEL",
            f"Wheel-Version: 1.0\nGenerator: airflow-provider-rivet gate\nRoot-Is-Purelib: false\nTag: py3-none-{platform}\n".encode(),
            0o644,
        ),
        (f"{info}/top_level.txt", f"{dist}\n".encode(), 0o644),
    ]
    record = (
        "".join(
            f"{name},sha256={base64.urlsafe_b64encode(hashlib.sha256(body).digest()).rstrip(b'=').decode()},{len(body)}\n"
            for name, body, _ in files
        )
        + f"{info}/RECORD,,\n"
    )
    files.append((f"{info}/RECORD", record.encode(), 0o644))
    path = out / f"{dist}-{version}-py3-none-{platform}.whl"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, body, mode in files:
            entry = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            entry.external_attr = (0o100000 | mode) << 16
            entry.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(entry, body)
    path.write_bytes(buf.getvalue())
    return path


SOURCE_DSN = "host=127.0.0.1 port={port} dbname=rivet user=rivet password={password}"
#: Where a mounted credential sits in the containers: the path Google's default chain (and rivet's) reads under HOME.
ADC_IN_CONTAINER = "/home/airflow/.config/gcloud/application_default_credentials.json"
#: The multi-host leg (`--workers 2`): each Celery worker is a host of its own, with a disk of its own mounted at this
#: one absolute path (two hosts share no disk); `HOST_DIRS` are the state directories on it, one per pipeline.
HOST_DISK = "/opt/rivet-host"
HOST_DIRS = ("batch", "cdc", "sqlite", "local", "chain")
#: The Celery app every Airflow worker runs (the provider's; the official healthcheck names the same).
CELERY_APP = "airflow.providers.celery.executors.celery_executor.app"
#: Where the source legs keep rivet's state: `gate` (the gate's own TLS PostgreSQL, `gate-state`) or `compose-postgres`
#: (the client's: a database of its own, owned by a role of its own, on the compose file's `postgres` server, beside
#: Airflow's metadata database `airflow`).
STATE_PLACES = ("gate", "compose-postgres", "client-override")
COMPOSE_STATE_ROLE = "rivet_state"
METASTORE_DB = "airflow"
#: `client-override`: the client's own compose files on top of the official one (`--client-override <dir>`): its
#: state server `rivet-state` (postgres:16, its own volume, no port, no TLS), its `rivet_state` Connection (an
#: AIRFLOW_CONN_* JSON variable) and its state_dir mount, as delivered. The gate supplies the variables they read.
RIVET_SERVICE = "rivet-state"
CLIENT_FILES = ("docker-compose.rivet.yml", "Dockerfile.rivet", "env.rivet.example", "README.md", "README.en.md")
CLIENT_STATE_USER = "rivet"
CLIENT_STATE_DB = "rivet_state"
CLIENT_STATE_CONN = "rivet_state"
#: The client's state_dir of its MySQL stream (its README, step 6), under its mount.
CLIENT_CDC_STATE_SUB = "mysql"
#: The client's state_dir mount (the source legs' state_dirs live under it in `client-override`).
CLIENT_STATE_ROOT = "/opt/airflow/rivet-state"
#: The queues of the multi-host leg; each task of its DAGs names one, and the gate moves a queue between the workers.
GATE_QUEUES = ("gate_a", "gate_b", "gate_cdc")


@dataclass(frozen=True)
class StandSecrets:
    """The passwords and keys of one stack, made for it: 32 random hex digits, so no log word holds one."""

    source: str
    state: str
    clickhouse: str
    metastore: str
    admin: str
    jwt: str
    mysql: str
    mysql_root: str
    mssql: str
    mssql_root: str
    mongodb: str
    mongodb_root: str
    oracle: str
    minio: str
    compose_state: str
    fernet: str

    @classmethod
    def generate(cls) -> StandSecrets:
        hex_fields = [f.name for f in fields(cls) if f.name != "fernet"]
        return cls(
            **{name: secrets.token_hex(16) for name in hex_fields},
            fernet=base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        )


def write_private(path: Path, text: str) -> None:
    """A 0600 file: the stand's env file and compose overlay hold its passwords and keys."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(fd, 0o600)  # a file left wider by an earlier run is tightened too
    with os.fdopen(fd, "w") as handle:
        handle.write(text)


def write_env_file(path: Path, values: dict[str, str]) -> None:
    """A 0600 env file docker compose reads (empty when the warehouse leg is off)."""
    write_private(path, "".join(f"{k}={v}\n" for k, v in values.items()))


@dataclass(frozen=True)
class Flavor:
    """What differs between the Airflow major versions the gate runs."""

    version: str
    api: str
    ui_service: str
    health: str
    services: tuple[str, ...]
    result_backend: str


FLAVORS = {
    # The client's Airflow: the official 2.10.2 compose file (Python 3.12, Debian 12).
    "2.10.2": Flavor(
        "2.10.2",
        "/api/v1",
        "airflow-webserver",
        "/health",
        ("airflow-webserver", "airflow-scheduler", "airflow-worker", "airflow-triggerer", "airflow-init", "airflow-cli"),
        "db+postgresql",
    ),
    "2.10.5": Flavor(
        "2.10.5",
        "/api/v1",
        "airflow-webserver",
        "/health",
        ("airflow-webserver", "airflow-scheduler", "airflow-worker", "airflow-triggerer", "airflow-init", "airflow-cli"),
        "db+postgresql",
    ),
    "3.3.2": Flavor(
        "3.3.2",
        "/api/v2",
        "airflow-apiserver",
        "/api/v2/monitor/health",
        (
            "airflow-apiserver",
            "airflow-scheduler",
            "airflow-dag-processor",
            "airflow-worker",
            "airflow-triggerer",
            "airflow-init",
            "airflow-cli",
        ),
        "db+postgresql+psycopg2",
    ),
}

OVERLAY = """\
x-rivet: &rivet
  image: {image}
  environment:
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__FERNET_KEY: '{fernet}'
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:{metastore}@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: {result_backend}://airflow:{metastore}@postgres/airflow
    AIRFLOW__API_AUTH__JWT_SECRET: '{jwt}'
    GATE_WAREHOUSES: '{warehouses}'
    GATE_SOURCES: '{sources}'
    GATE_WORKERS: '{workers}'
    GATE_DESTINATIONS: '{destinations}'
    GATE_SOURCE_STATE_ROOT: '{source_state_root}'
    GATE_GCP_AUTH: '{gcp_auth}'
    GATE_CLIENT_OVERRIDE: '{client}'
{gcp_env}{backend}  env_file: [gate-connections.env]
  volumes:
    - {rivet_dir}:/opt/rivet
{extra_volumes}{adc}services:
{services}
  postgres:
    environment: {{ POSTGRES_PASSWORD: {metastore} }}
    # The official 50 s of health checks lose to a first initdb on a docker host busy with other stacks.
    healthcheck: {{ retries: 60, start_period: 60s }}
  gate-source:
    image: postgres:16
    environment: {{ POSTGRES_USER: rivet, POSTGRES_PASSWORD: {source}, POSTGRES_DB: rivet }}
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10"]
    ports: ["127.0.0.1::5432"]
    healthcheck: {{ test: ["CMD", "pg_isready", "-U", "rivet"], interval: 5s, retries: 20 }}
  gate-state:
    image: postgres:16
    environment: {{ POSTGRES_USER: rivet, POSTGRES_PASSWORD: {state}, POSTGRES_DB: rivet_state }}
    command: ["bash", "-c", "install -d -o postgres -g postgres -m 700 /ssl && openssl req -new -x509 -days 2 -nodes -subj /CN=gate-state -out /ssl/server.crt -keyout /ssl/server.key && chown postgres:postgres /ssl/server.crt /ssl/server.key && chmod 600 /ssl/server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/ssl/server.crt -c ssl_key_file=/ssl/server.key"]
    healthcheck: {{ test: ["CMD", "pg_isready", "-U", "rivet"], interval: 5s, retries: 20 }}
  gate-clickhouse:
    image: clickhouse/clickhouse-server:24.8
    environment: {{ CLICKHOUSE_USER: loader, CLICKHOUSE_PASSWORD: {clickhouse}, CLICKHOUSE_DB: gate, CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1" }}
    ulimits: {{ nofile: {{ soft: 262144, hard: 262144 }} }}
    ports: ["127.0.0.1::8123"]
{source_services}{destination_services}"""


def sh(
    args: list[str], *, input: str | None = None, timeout: float = 900, check: bool = True, **kw: Any
) -> subprocess.CompletedProcess:
    """Run a command, failing with its output when `check` is set."""
    done = subprocess.run(args, input=input, text=True, capture_output=True, timeout=timeout, **kw)
    if check and done.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:6])} exited {done.returncode}: {(done.stderr or done.stdout).strip()[-2000:]}")
    return done


def state_db(engine: str, kind: str) -> str:
    """The state database of one config of a source leg."""
    return f"rivet_state_{engine}_{kind}"


def state_conn_id(engine: str, kind: str) -> str:
    return f"rivet_gate_state_{engine}_{kind}"


#: The multi-host leg's state databases beside `rivet_state` (its batch pipeline's), each behind a Connection
#: `rivet_gate_state_mh_<name>`: a config's exports share their names with another's (init names the CDC config's
#: baseline exports after their tables, as the batch ones).
MULTIHOST_STATE = ("cdc", "local", "chain")


def worker_service(name: str) -> str:
    """The compose service of one host of the multi-host leg (`a` is the official `airflow-worker`)."""
    return "airflow-worker" if name == "a" else f"airflow-worker-{name}"


def worker_hostname(name: str) -> str:
    """The container hostname of a host, which Celery names `celery@<it>` and Airflow records on each task instance."""
    return f"gate-host-{name}"


class Stand:
    """A compose project under `work/<version>`: Airflow from the official compose plus the gate overlay."""

    def __init__(
        self,
        version: str,
        rivet_bin: Path,
        work: Path,
        warehouses: tuple[str, ...] = (),
        tag: str | None = None,
        secrets_backend: dict[str, str] | None = None,
        adc: Path | None = None,
        sources: tuple[Any, ...] = (),
        workers: int = 1,
        destinations: tuple[Any, ...] = (),
        state_place: str = "gate",
        migrate_from: Path | None = None,
        gcp_key_file: Path | None = None,
        client_override: Path | None = None,
    ) -> None:
        self.flavor = FLAVORS[version]
        #: The client's delivered compose files (`--client-override`): the stack runs them as delivered, on top of the
        #: official file; the gate's overlay goes on top of both.
        self.client_override = client_override
        if client_override is not None:
            state_place = "client-override"
        if state_place not in STATE_PLACES:
            raise ValueError(f"state place {state_place!r}: one of {STATE_PLACES}")
        #: Where the source legs' configs keep rivet's state (`STATE_PLACES`).
        self.state_place = state_place
        #: `--gcp-auth env-file`: the GCP key every container reads at GCP_KEY_IN_CONTAINER (GOOGLE_APPLICATION_CREDENTIALS).
        self.gcp_key_file = gcp_key_file
        #: rivet 0.32.1 for the migration leg (gate/migrate.py), mounted at /opt/rivet/bin/rivet-0.32.1.
        self.migrate_from = migrate_from
        #: 1: one Celery worker on the shared volume; 2: the multi-host leg, two workers with a disk each.
        self.workers = workers
        # The run tag keeps two gate runs of one Airflow version apart: each `down -v` and image build is its own.
        self.tag = tag or secrets.token_hex(4)
        self.rivet_bin = rivet_bin
        self.dir = work / f"airflow-{version}"
        self.rivet_dir = self.dir / "rivet"
        self.project = f"rivet-gate-af{version.replace('.', '')}-{self.tag}"
        #: The gate's own image of this run: the client's (`Dockerfile.rivet`'s install from local wheels) in
        #: `client-override`, which its compose file reads as RIVET_AIRFLOW_IMAGE.
        self.image = f"rivet-gate-{'client' if client_override else 'airflow'}:{version}-{self.tag}"
        self.ui_port = 0
        self.source_dsn = ""
        self.clickhouse_port = 0
        self.warehouses = warehouses
        self.secrets = StandSecrets.generate()
        self.extra_connections: dict[str, dict[str, Any]] = {}
        # `[secrets]` settings (AIRFLOW__SECRETS__*) when the Connections come from a secrets backend, not from
        # AIRFLOW_CONN_* variables; the credential file the containers read as their own identity (read-only).
        self.secrets_backend = secrets_backend
        self.adc = adc
        #: Source engines beside the postgres source (gate.sources.Engine): their containers and Connections.
        self.sources = sources
        #: Destination legs (gate.destinations): their containers, Connections and the states of their configs.
        self.destinations = destinations
        self._token: str | None = None

    def connections(self) -> dict[str, dict[str, Any]]:
        """Every Connection the gate DAGs read, by conn_id, in Airflow's JSON form."""
        return {
            "rivet_gate_source": {
                "conn_type": "postgres",
                "host": "gate-source",
                "port": 5432,
                "login": "rivet",
                "password": self.secrets.source,
                "schema": "rivet",
            },
            "rivet_gate_state": {
                "conn_type": "postgres",
                "host": "gate-state",
                "port": 5432,
                "login": "rivet",
                "password": self.secrets.state,
                "schema": "rivet_state",
                "extra": {"rivet_params": "sslmode=require"},
            },
            **{e.conn_id: e.connection(self.secrets) for e in self.sources},
            # A database of its own per config of a source leg: SQL Server's init names its CDC exports after their
            # tables, as the batch ones, and an export's state is keyed by its name and source (one state, one name).
            # Under `client-override` the MySQL stream reads the client's own `rivet_state` (its compose file defines it).
            **{
                state_conn_id(e.name, kind): self.leg_state_conn(state_db(e.name, kind))
                for e in self.sources
                for kind in e.kinds
                if self.leg_db(e.name, kind) == state_db(e.name, kind)
            },
            **(
                {
                    f"rivet_gate_state_mh_{kind}": {
                        "conn_type": "postgres",
                        "host": "gate-state",
                        "port": 5432,
                        "login": "rivet",
                        "password": self.secrets.state,
                        "schema": f"rivet_state_mh_{kind}",
                        "extra": {"rivet_params": "sslmode=require"},
                    }
                    for kind in MULTIHOST_STATE
                }
                if self.workers > 1
                else {}
            ),
            **{conn_id: conn for d in self.destinations for conn_id, conn in d.connections(self.secrets).items()},
            **{
                state_conn_id(d.name, kind): {
                    "conn_type": "postgres",
                    "host": "gate-state",
                    "port": 5432,
                    "login": "rivet",
                    "password": self.secrets.state,
                    "schema": state_db(d.name, kind),
                    "extra": {"rivet_params": "sslmode=require"},
                }
                for d in self.destinations
                for kind in d.configs
            },
            **self.migration_connections(),
            **self.extra_connections,
        }

    def migration_connections(self) -> dict[str, dict[str, Any]]:
        """The migration leg's (gate/migrate.py): its MySQL database and its PostgreSQL state, where `state_place` says."""
        mysql = [e for e in self.sources if e.name == "mysql"]
        if not mysql:
            return {}
        # The config init writes beside a database of the same table names (sources.SourceLeg.unqualified_phase).
        unq = {state_conn_id("mysql", "unq"): self.leg_state_conn(state_db("mysql", "unq"))}
        if not self.migrate_from:
            return unq
        return unq | {
            "rivet_gate_mysql_mig": {**mysql[0].connection(self.secrets), "schema": "rivet_mig"},
            state_conn_id("mysql", "mig"): self.leg_state_conn(state_db("mysql", "mig")),
            state_conn_id("mysql", "migb"): self.leg_state_conn(state_db("mysql", "migb")),
        }

    def leg_state_sql(self, db: str, sql: str) -> str:
        """Read a source leg config's state database (the gate's state server, or the compose `postgres` as its owner)."""
        if self.state_place in ("compose-postgres", "client-override"):
            return self.state_admin_sql(db, sql)
        return self.psql("gate-state", db, sql)

    @property
    def hosts(self) -> tuple[str, ...]:
        """The hosts of the stack: `a` alone, or `a` and `b` in the multi-host leg."""
        return ("a", "b")[: self.workers]

    def host_disk(self, name: str) -> Path:
        """The directory on the docker host that is host `name`'s own disk (mounted at HOST_DISK in its worker only)."""
        return self.dir / "hosts" / name

    def connection_variables(self) -> dict[str, str]:
        """The AIRFLOW_CONN_* variables of the stack: none when a secrets backend holds the Connections."""
        if self.secrets_backend is not None:
            return {}
        return {f"AIRFLOW_CONN_{conn_id.upper()}": json.dumps(conn) for conn_id, conn in self.connections().items()}

    def compose(self, *args: str, extra_env: dict[str, str] | None = None, **kw: Any) -> subprocess.CompletedProcess:
        """`docker compose` on this stack's project; `extra_env` reaches `exec -e NAME` without a value on any argv."""
        client = ["-f", str(self.dir / "docker-compose.rivet.yml")] if self.client_override else []
        files = ["-f", str(self.dir / "docker-compose.yaml"), *client, "-f", str(self.dir / "docker-compose.gate.yaml")]
        env = {
            **os.environ,
            **(extra_env or {}),
            # What the client's compose file reads (its env.rivet.example), and its image as the gate built it.
            **(
                {
                    "RIVET_STATE_USER": CLIENT_STATE_USER,
                    "RIVET_STATE_PASSWORD": self.secrets.compose_state,
                    "RIVET_STATE_DB": CLIENT_STATE_DB,
                    "RIVET_AIRFLOW_IMAGE": self.image,
                    # The client's `rivet_mysql` Connection (AIRFLOW_CONN_RIVET_MYSQL from RIVET_MYSQL_*): the stack's MySQL,
                    # spelled as the cron config reaches it.
                    "RIVET_MYSQL_HOST": "gate-mysql",
                    "RIVET_MYSQL_PORT": "3306",
                    "RIVET_MYSQL_USER": "rivet",
                    "RIVET_MYSQL_PASSWORD": self.secrets.mysql,
                    "RIVET_MYSQL_DB": "rivet",
                }
                if self.client_override
                else {}
            ),
            "AIRFLOW_UID": str(os.getuid()),
            "AIRFLOW_PROJ_DIR": str(self.dir),
            "_AIRFLOW_WWW_USER_PASSWORD": self.secrets.admin,
        }
        return sh(["docker", "compose", "-p", self.project, *files, *args], env=env, **kw)

    def build(self) -> None:
        """The Airflow image with the rivet binary under test and this provider."""
        ctx = self.dir / "image"
        if self.client_override:
            # The client's own build: its compose file's `build:` (Dockerfile.rivet, context the stack's folder), the
            # two pins found in ./wheels (`--find-links`), tagged by its RIVET_AIRFLOW_IMAGE.
            self.compose("build", "airflow-worker", timeout=1800)
            return
        shutil.copy2(self.rivet_bin, ctx / "rivet")
        for name in PACKAGE_FILES:
            src = REPO / name
            if src.is_dir():
                shutil.copytree(src, ctx / "provider" / name, ignore=shutil.ignore_patterns("__pycache__"))
            else:
                shutil.copy2(src, ctx / "provider" / name)
        sh(
            [
                "docker",
                "build",
                "-q",
                "-t",
                self.image,
                "--build-arg",
                f"AIRFLOW_IMAGE=apache/airflow:{self.flavor.version}",
                "-f",
                str(HERE / "stand" / "Dockerfile"),
                str(ctx),
            ],
            timeout=1800,
        )

    def lay_out(self) -> None:
        """A fresh project dir: compose files, the DAG file, the rivet dirs the containers mount."""
        if self.dir.exists():
            shutil.rmtree(self.dir)
        for sub in (
            "dags",
            "logs",
            "config",
            "plugins",
            "image/provider",
            "rivet/configs",
            "rivet/out",
            "rivet/markers",
            "rivet/state/sqlite",
            "rivet/state/pg",
        ):
            (self.dir / sub).mkdir(parents=True)
        shutil.copy2(HERE / "stand" / f"airflow-{self.flavor.version}.yaml", self.dir / "docker-compose.yaml")
        (self.dir / ".env").write_text("")
        for host in self.hosts if self.workers > 1 else ():
            for sub in HOST_DIRS:
                (self.host_disk(host) / sub).mkdir(parents=True)
        shutil.copy2(HERE / "stand" / "dags" / "rivet_gate.py", self.dir / "dags" / "rivet_gate.py")
        if self.client_override is not None:
            self.lay_out_client()
        if self.migrate_from:
            # The previous release the migration leg starts on, in the mount every container has (/opt/rivet/bin).
            (self.rivet_dir / "bin").mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.migrate_from, self.rivet_dir / "bin" / "rivet-0.32.1")
            (self.rivet_dir / "bin" / "rivet-0.32.1").chmod(0o755)
        services = "\n".join(
            f"  {s}:\n    <<: *rivet"
            + (
                '\n    ports: !override ["127.0.0.1::8080"]'
                # The official 30 s + 5 x 30 s lose to an API server's first start on a docker host busy with other
                # stacks; the workers wait for it healthy, so `up` fails with "dependency failed to start".
                "\n    healthcheck: { retries: 30, start_period: 120s }"
                if s == self.flavor.ui_service
                else ""
            )
            for s in self.flavor.services
            if not (self.workers > 1 and s == "airflow-worker")
        )
        if self.workers > 1:
            services += "".join(self.worker_yaml(host) for host in self.hosts)
        write_private(self.dir / "docker-compose.gate.yaml", self.overlay(services))
        write_env_file(self.dir / "gate-connections.env", self.connection_variables())

    def lay_out_client(self) -> None:
        """The client's files, copied as delivered (their sha256 kept for the `as-delivered` cell), and its `./wheels`:
        `rivet-cli` (the binary under test, laid out as rivet's own wheel) and this provider's wheel, which its
        Dockerfile.rivet finds there before PyPI."""
        assert self.client_override is not None
        self.client_files = {}
        for name in CLIENT_FILES:
            shutil.copy2(self.client_override / name, self.dir / name)
            self.client_files[name] = hashlib.sha256((self.dir / name).read_bytes()).hexdigest()
        (self.dir / "rivet-state").mkdir(parents=True, exist_ok=True)
        out = self.dir / "wheels"
        out.mkdir(parents=True, exist_ok=True)
        shown = sh(["docker", "run", "--rm", "--entrypoint", "/rivet", "-v", f"{self.rivet_bin}:/rivet:ro",
                    f"apache/airflow:{self.flavor.version}", "--version"]).stdout  # fmt: skip
        cli = rivet_cli_wheel(self.rivet_bin, rivet_version(shown), out)
        provider = provider_wheel(out, CLIENT_PROVIDER_VERSION)
        print(f"CLIENT wheels {cli.name} {provider.name} ({shown.strip()}); files {self.client_files}", flush=True)

    def worker_yaml(self, host: str) -> str:
        """One host of the multi-host leg: a Celery worker with its own hostname, its own disk at HOST_DISK, and the
        queues it consumes at the start (the gate moves them with `route`)."""
        queues = ",".join(["default", *(q for q in GATE_QUEUES if q == f"gate_{host}" or (q == "gate_cdc" and host == "a"))])
        extends = (
            "" if host == "a" else "\n    extends: { file: docker-compose.yaml, service: airflow-worker }"
        )  # the official worker, once more
        return (
            f"\n  {worker_service(host)}:\n    <<: *rivet{extends}\n    hostname: {worker_hostname(host)}\n"
            f"    command: celery worker -q {queues}\n    volumes:\n      - {self.rivet_dir}:/opt/rivet\n"
            f"      - {self.host_disk(host)}:{HOST_DISK}\n"
        )

    def overlay(self, services: str) -> str:
        """The gate's compose overlay: the image, this stack's own passwords and keys, where Connections come from."""
        backend = "".join(f"    {k}: '{v}'\n" for k, v in (self.secrets_backend or {}).items())
        adc = f"    - {self.adc}:{ADC_IN_CONTAINER}:ro\n" if self.adc else ""
        return OVERLAY.format(
            image=self.image,
            rivet_dir=self.rivet_dir,
            services=services,
            warehouses=",".join(self.warehouses),
            sources=",".join(e.name for e in self.sources),
            source_services="".join(e.service_yaml.format(**vars(self.secrets)) for e in self.sources),
            destinations=",".join(d.name for d in self.destinations),
            destination_services="".join(d.service_yaml.format(**vars(self.secrets)) for d in self.destinations),
            backend=backend,
            adc=adc,
            result_backend=self.flavor.result_backend,
            workers=self.workers,
            source_state_root=self.source_state_root,
            gcp_auth="env-file" if self.gcp_key_file else "",
            # The client's compose sets it itself; the gate sets it only for its own stacks.
            gcp_env=(
                f"    GOOGLE_APPLICATION_CREDENTIALS: {GCP_KEY_IN_CONTAINER}\n"
                if self.gcp_key_file and not self.client_override
                else ""
            ),
            client="1" if self.client_override else "",
            # The key file the client's compose names but does not mount (the client's prod file mounts it).
            extra_volumes=f"    - {self.gcp_key_file}:{GCP_KEY_IN_CONTAINER}:ro\n" if self.gcp_key_file else "",
            **vars(self.secrets),
        )

    def down(self) -> None:
        """Remove the project's containers, volumes and image (a leftover of an earlier attempt of this run too)."""
        sh(["docker", "compose", "-p", self.project, "down", "-v", "--remove-orphans"], timeout=900, check=False)
        sh(["docker", "image", "rm", "-f", self.image], timeout=300, check=False)

    def up(self) -> None:
        """Lay out, build, start, and wait until the API answers."""
        self.down()
        self.lay_out()
        self.build()
        # No `--wait`: an Airflow 2 webserver on a loaded host can miss the official healthcheck and come up later.
        self.compose("up", "-d", timeout=1800)
        self.ui_port = self.host_port(self.flavor.ui_service, 8080)
        self.source_dsn = SOURCE_DSN.format(port=self.host_port("gate-source", 5432), password=self.secrets.source)
        self.clickhouse_port = self.host_port("gate-clickhouse", 8123)
        deadline = time.monotonic() + 900
        while True:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{self.ui_port}{self.flavor.health}", timeout=5)
                ready = [
                    self.compose("exec", "-T", db, "pg_isready", "-U", "rivet", check=False).returncode == 0
                    for db in ("gate-source", "gate-state")
                ]
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

    def state_url(self, db: str = "rivet_state") -> str:
        """A PostgreSQL state of the stack as `rivet init` reads it inside the stack's network (the Connection of `db`)."""
        return f"postgresql://rivet:{self.secrets.state}@gate-state:5432/{db}?sslmode=require"

    @property
    def source_state_root(self) -> str:
        """Where the source legs' state_dirs are in the containers: the client's mount in `client-override`."""
        return CLIENT_STATE_ROOT if self.client_override else "/opt/rivet/state"

    @property
    def source_state_host(self) -> Path:
        """The same place on this host (bind-mounted)."""
        return self.dir / "rivet-state" if self.client_override else self.rivet_dir / "state"

    def leg_db(self, engine: str, kind: str) -> str:
        """The state database of one of the gate's source configs (a database of its own, also under
        `client-override`: the client's `rivet_state` is the README stream's, `gate/client.py`)."""
        return state_db(engine, kind)

    def leg_state_sub(self, engine: str, kind: str) -> str:
        """The state_dir of a source leg's config under the source state root."""
        return f"{engine}_{kind}"

    def leg_state_conn(self, db: str) -> dict[str, Any]:
        """The Connection of a source leg config's state database, where `state_place` puts it."""
        if self.state_place in ("compose-postgres", "client-override"):
            client = self.state_place == "client-override"
            return {
                "conn_type": "postgres",
                "host": RIVET_SERVICE if client else "postgres",
                "port": 5432,
                "login": CLIENT_STATE_USER if client else COMPOSE_STATE_ROLE,
                "password": self.secrets.compose_state,
                "schema": db,
                # The compose `postgres` has no TLS: rivet refuses a remote state URL without an `sslmode`
                # (RIVET_STATE_URL_TLS_REQUIRED), so plaintext on the compose network is said, not assumed.
                "extra": {"rivet_params": "sslmode=disable"},
            }
        return {
            "conn_type": "postgres",
            "host": "gate-state",
            "port": 5432,
            "login": "rivet",
            "password": self.secrets.state,
            "schema": db,
            "extra": {"rivet_params": "sslmode=require"},
        }

    def leg_state_url(self, db: str) -> str:
        """A source leg config's state as `rivet init` reads it inside the stack's network (its Connection's URL)."""
        if self.state_place == "client-override":
            return f"postgresql://{CLIENT_STATE_USER}:{self.secrets.compose_state}@{RIVET_SERVICE}:5432/{db}?sslmode=disable"
        if self.state_place == "compose-postgres":
            return f"postgresql://{COMPOSE_STATE_ROLE}:{self.secrets.compose_state}@postgres:5432/{db}?sslmode=disable"
        return self.state_url(db)

    def make_leg_state_db(self, db: str) -> None:
        """Create a source leg config's state database: on `gate-state`, or on the compose `postgres` server owned by the
        role `rivet_state` (made once, LOGIN, its own password; the statement goes in on stdin, never on an argv)."""
        if self.state_place == "gate":
            self.psql("gate-state", "rivet_state", f"CREATE DATABASE {db};")
            return
        if self.state_place == "client-override":
            # The client's server makes its own database (POSTGRES_DB); the gate's other configs get one each beside it,
            # owned by the client's user (the server's superuser, as its compose file makes it).
            if db != CLIENT_STATE_DB:
                self.state_admin_sql(None, f"CREATE DATABASE {db};")
            return
        role, password = COMPOSE_STATE_ROLE, self.secrets.compose_state
        self.state_admin_sql(
            None,
            f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN "
            f"CREATE ROLE {role} LOGIN PASSWORD '{password}'; END IF; END $$;",
        )
        self.state_admin_sql(None, f"CREATE DATABASE {db} OWNER {role};")

    def state_admin_sql(self, db: str | None, sql: str) -> str:
        """SQL as the superuser of the source legs' state server: Airflow's `postgres` (`airflow`), or the client's own
        `rivet-state` (its `RIVET_STATE_USER`); `db` None: the server's own database."""
        if self.state_place == "client-override":
            return self.compose(
                "exec", "-T", RIVET_SERVICE, "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", CLIENT_STATE_USER,
                "-d", db or CLIENT_STATE_DB, input=sql,
            ).stdout  # fmt: skip
        return self.metastore_sql(db or METASTORE_DB, sql)

    def metastore_sql(self, db: str, sql: str) -> str:
        """SQL on the compose file's `postgres` server as its superuser `airflow` (setup and the placement oracle)."""
        return self.compose(
            "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", "airflow", "-d", db, input=sql
        ).stdout

    def psql(self, service: str, db: str, sql: str) -> str:
        """Run SQL through psql inside a database container; the unaligned result."""
        return self.compose(
            "exec", "-T", service, "psql", "-v", "ON_ERROR_STOP=1", "-At", "-U", "rivet", "-d", db, input=sql
        ).stdout

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
            return "Basic " + base64.b64encode(f"airflow:{self.secrets.admin}".encode()).decode()
        if self._token is None:
            req = urllib.request.Request(
                f"http://127.0.0.1:{self.ui_port}/auth/token",
                method="POST",
                data=json.dumps({"username": "airflow", "password": self.secrets.admin}).encode(),
                headers={"Content-Type": "application/json"},
            )
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

    def tries(self, dag: str, run_id: str, task: str) -> list[dict[str, Any]]:
        """Every try of one task instance (hostname, state, try_number), oldest first."""
        found = self._call("GET", f"/dags/{dag}/dagRuns/{run_id}/taskInstances/{task}/tries")["task_instances"]
        return sorted(found, key=lambda t: t["try_number"])

    def celery(self, *args: str) -> str:
        """A Celery control or inspect command against the stack's broker, run in the first worker."""
        # `--timeout`: a worker on a busy docker host answers after celery's default second
        return self.compose(
            "exec", "-T", "airflow-worker", "celery", "--app", CELERY_APP, *args, "--timeout", "20", timeout=180
        ).stdout

    def queues(self) -> dict[str, set[str]]:
        """The queues each host's worker consumes now, by host (`celery inspect active_queues`)."""
        found: dict[str, set[str]] = {}
        for host in self.hosts:
            text = self.celery("inspect", "active_queues", "-j", "-d", f"celery@{worker_hostname(host)}")
            reply = json.loads(text[text.index("{") :])
            found[host] = {q["name"] for qs in reply.values() for q in qs}
        return found

    def route(self, queue: str, host: str, timeout: float = 300) -> None:
        """Make `host` the only worker that consumes `queue`; a task already running stays where it runs. Asked again
        until the workers' answer shows it: a worker still starting, or busy, answers no control command."""
        deadline = time.monotonic() + timeout
        now: dict[str, set[str]] | str = {}
        while time.monotonic() < deadline:
            try:
                now = self.queues()
                if {h for h, qs in now.items() if queue in qs} == {host}:
                    return
                for other in self.hosts:
                    verb = "add_consumer" if other == host else "cancel_consumer"
                    if (queue in now.get(other, set())) != (other == host):
                        self.celery("control", verb, queue, "-d", f"celery@{worker_hostname(other)}")
            except (RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
                now = str(exc)[-300:]
            time.sleep(2)
        raise TimeoutError(f"queue {queue} is consumed as {now}, not by {host} alone, after {timeout:.0f} s")

    def tasks(self, dag: str, run_id: str) -> dict[str, dict[str, Any]]:
        """Task instances of a run by task id: state, start, end."""
        return {t["task_id"]: t for t in self._call("GET", f"/dags/{dag}/dagRuns/{run_id}/taskInstances")["task_instances"]}

    def xcom(self, dag: str, run_id: str, task: str) -> dict[str, Any]:
        """A task's return value (Airflow 2 serves it as Python repr text)."""
        value = self._call("GET", f"/dags/{dag}/dagRuns/{run_id}/taskInstances/{task}/xcomEntries/return_value")["value"]
        return ast.literal_eval(value) if isinstance(value, str) else value
