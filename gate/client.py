"""The client's README, steps 5 and 6, run as written (`--client-override <dir>`, beside `--migrate-from <rivet 0.32.1>`).

Before: a cron job runs rivet 0.32.1 in a folder of its own (the rivet config, its `.rivet_state.db`, `cdc/` with the
checkpoint), run, load and compact, twice. The gate plays that cron job inside the worker container (the folder
`/opt/rivet/cron`; the binary the migration leg mounts), on the stack's MySQL as `rivet_mysql` spells it.

Then the README: step 5 copies the cron folder into the mounted `./rivet-state/migration-src`, runs its `rivet state
copy` command VERBATIM (only `docker compose ...` filled in, and `-T`: no terminal) into the client's empty
`rivet_state`, and copies the checkpoint to `./rivet-state/mysql/cdc/`; step 6 runs the same config under Airflow with
its kwargs (`env_from_connections` RIVET_STATE_URL -> rivet_state, RIVET_MYSQL_URL -> rivet_mysql,
`cloud_credentials=["gcp"]`, `state_dir="/opt/airflow/rivet-state/mysql"`). The stream must continue: the run moves
exactly the changes made since the cron job stopped, and BigQuery equals the source.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from . import warehouse
from .migrate import OLD_BIN
from .sources import read_config, stream_equals_source
from .stand import sh

CRON = "/opt/rivet/cron"
STATE_DIR = "/opt/airflow/rivet-state/mysql"
DAG = "gate_client_cron"
#: The changes the cron job reads (one run), and the ones made after it stopped, which the Airflow run must move.
CRON_CHANGES = ("UPDATE gate_customers SET tier = 'cron' WHERE id IN (21, 22);", {"gate_customers": {"update": 2}})
AFTER_CRON = (
    "UPDATE gate_customers SET tier = 'moved' WHERE id IN (23, 24, 25);\nDELETE FROM gate_orders WHERE id = 104;",
    {"gate_customers": {"update": 3}, "gate_orders": {"delete": 1}},
)
#: README step 5, its `rivet state copy` command as written (its `<config file>` named, `docker compose ...` filled in).
STEP5_COPY = (
    'RIVET_STATE_URL="postgresql://$RIVET_STATE_USER:$RIVET_STATE_PASSWORD@rivet-state:5432/$RIVET_STATE_DB?sslmode=disable" '
    "rivet state copy -c /opt/airflow/rivet-state/migration-src/{config}"
)


def events(ops: dict[str, dict[str, int]]) -> int:
    return sum(n for per in ops.values() for n in per.values())


def step5_problems(code: int, said: str, readme: str) -> list[str]:
    """README step 5's `rivet state copy` went through: the command run is the README's own line (only its `<config
    file>` named), exit 0, and rivet's own `copied N table(s)` line."""
    problems = [] if STEP5_COPY.format(config="<config file>") in readme else ["the command run is not the README's step 5 line"]
    if code != 0:
        problems.append(f"exit {code}")
    if "copied " not in said or " table(s)" not in said:
        problems.append(f"no `copied … table(s)` line: {said[-800:]}")
    return problems


def continues_problems(
    state: str | None, tasks: dict[str, dict[str, Any]], payload: dict[str, Any] | None, rows: int
) -> list[str]:
    """Step 6: the stream's first run under Airflow continued where cron stopped: it and its tasks succeeded and it moved
    exactly the changes made since (a new baseline moves the whole table; a lost position, fewer)."""
    problems = [] if state == "success" else [f"run {state}"]
    bad = {t: i.get("state") for t, i in tasks.items() if i.get("state") not in ("success", "skipped")}
    if bad or not tasks:
        problems.append(f"tasks {bad or 'none'}")
    error = (payload or {}).get("error") or {}
    if error:
        problems.append(f"rivet stopped [{error.get('code')}] exit {(payload or {}).get('exit_status')}")
    else:
        moved = sum(u["rows"] for u in (payload or {}).get("units") or [] if type(u.get("rows")) is int)
        if moved != rows:
            problems.append(f"the run moved {moved} row(s), the changes since cron stopped are {rows}")
    return problems


class ClientReadme:
    """The README's migration of a cron-run stream, on the client's stack."""

    def __init__(self, src: Any) -> None:
        self.src = src
        tag = f"{src.cloud.tag}-{src.stack.replace('.', '')}"
        self.bucket = f"rivet-afgate-{tag}-mysqlcron-cdc"
        self.dataset = f"{src.cloud.dataset(src.stack)}_mysqlcron_cdc"
        self.stream: dict[str, Any] = {}
        self.commands: list[str] = []

    @property
    def dags(self) -> list[str]:
        return [DAG]

    @property
    def tasks(self) -> list[str]:
        return ["run", *(f"{g}.{t}" for t in self.stream.get("tables", []) for g in ("load", "compact"))]

    def command(self, text: str) -> None:
        self.commands.append(text)
        print(f"COMMAND {text}", flush=True)

    def cron(self, stand: Any, *args: str) -> subprocess.CompletedProcess:
        """rivet 0.32.1 as the cron job runs it: in its folder, the source URL in RIVET_MYSQL_URL, SQLite beside the
        config, the key file the worker's env names."""
        line = f"cd {CRON} && {OLD_BIN} " + " ".join(args)
        self.command(f"(cron) RIVET_MYSQL_URL=<url> {line}")
        done = stand.compose("exec", "-T", "-e", "RIVET_MYSQL_URL", "-e", "RIVET_STATE_URL", "airflow-worker", "bash", "-c", line,
                             extra_env={"RIVET_MYSQL_URL": self.src.engine.url(stand.secrets), "RIVET_STATE_URL": ""},
                             check=False)  # fmt: skip
        print(f"CRON {' '.join(args)} -> exit {done.returncode}\n{(done.stdout + done.stderr).strip()[-1500:]}", flush=True)
        if done.returncode != 0:
            raise RuntimeError(f"cron `{' '.join(args)}` exited {done.returncode}")
        return done

    def prepare(self, stand: Any) -> None:
        """The cron folder (rivet 0.32.1's init over the leg's tables, into a bucket and dataset of its own) and the DAG
        of step 6, over the same config, the client's Connections and state_dir."""
        sh(["gcloud", "storage", "buckets", "create", f"gs://{self.bucket}", f"--project={self.src.cloud.project}",
            f"--location={self.src.cloud.location}", "--uniform-bucket-level-access"], timeout=300)  # fmt: skip
        warehouse._bq(
            f"--location={self.src.cloud.location}", "mk", "-f", "--dataset", f"{self.src.cloud.project}:{self.dataset}"
        )
        (stand.rivet_dir / "cron").mkdir(parents=True, exist_ok=True)
        self.cron(stand, "init", "--source-env", "RIVET_MYSQL_URL", "--tls", "disable", "--mode", "cdc", "--gcs-bucket", self.bucket,
                  "--bigquery-project", self.src.cloud.project, "--bigquery-dataset", self.dataset,
                  "--include", *self.src.engine.tables, "-o", f"{CRON}/rivet.yaml")  # fmt: skip
        text = (stand.rivet_dir / "cron" / "rivet.yaml").read_text()
        print(f"INIT client cron (rivet 0.32.1) config:\n{text}", flush=True)
        streams = read_config(text, list(self.src.engine.tables))["streams"]
        if len(streams) != 1:
            raise ValueError(f"the cron config holds streams {streams}, expected one")
        self.stream = streams[0]
        # Step 6 runs the same config: a copy where a DAG folder keeps it (not in migration-src, whose checkpoint sits
        # beside it and would be refused, RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG).
        (stand.rivet_dir / "configs" / "client_cron").mkdir(parents=True, exist_ok=True)
        shape = {"dag": DAG, "config": "/opt/rivet/configs/client_cron/rivet.yaml", "stream": self.stream, "state_dir": STATE_DIR}
        (stand.rivet_dir / "configs" / "client_readme.json").write_text(json.dumps(shape))

    def play(self, stand: Any, v: Any, wait_runs: Callable[..., Any]) -> None:
        eng, hot = self.src.engine, self.src.engine.hot
        for _ in range(2):
            for step in ("run", "load", "compact"):
                self.cron(stand, step, "-c", f"{CRON}/rivet.yaml")
            if _ == 0:
                self.src.sql(stand, CRON_CHANGES[0])
        # The cron job is stopped; what changes now only Airflow may deliver.
        after_sql, after_ops = AFTER_CRON
        self.src.sql(stand, after_sql)
        shutil.copyfile(stand.rivet_dir / "cron" / "rivet.yaml", stand.rivet_dir / "configs" / "client_cron" / "rivet.yaml")
        # Step 5: the cron folder into the mounted directory (`cp -a "$CRON_DIR" ./rivet-state/migration-src`; the
        # `chown -R 50000:0` is the stack's AIRFLOW_UID here, which the host files already have).
        src = stand.source_state_host / "migration-src"
        shutil.copytree(stand.rivet_dir / "cron", src, symlinks=True)
        self.command('cp -a "$CRON_DIR" ./rivet-state/migration-src')
        line = STEP5_COPY.format(config="rivet.yaml")
        self.command(f"docker compose ... exec airflow-worker bash -c '{line}'")
        done = stand.compose("exec", "-T", "airflow-worker", "bash", "-c", line, check=False)
        said = ((done.stdout or "") + (done.stderr or "")).strip()
        print(
            f"README step 5 state copy -> exit {done.returncode}\n{said.replace(stand.secrets.compose_state, '***')}", flush=True
        )
        readme = (stand.dir / "README.en.md").read_text()
        v.check(
            "client-override/readme/step5-verbatim",
            step5_problems,
            done.returncode,
            said,
            readme,
            ok="the README's line, exit 0, copied",
        )
        # Step 5: the checkpoint where the config's relative path resolves from the state_dir.
        ckpt = sorted((src / "cdc").glob("*.ckpt"))
        dest = stand.source_state_host / "mysql" / "cdc"
        dest.mkdir(parents=True, exist_ok=True)
        for f in ckpt:
            shutil.copy2(f, dest / f.name)
        self.command(
            "mkdir -p ./rivet-state/mysql/cdc && cp -a ./rivet-state/migration-src/cdc/<file>.ckpt ./rivet-state/mysql/cdc/"
        )
        # Step 6: the DAG over the same config.
        stand.wait_dags([DAG])
        run_id = "gate_client_cron_1"
        stand.trigger(DAG, run_id)
        state = wait_runs(stand, [DAG], run_id).get(DAG)
        tasks = {
            t: i
            for t, i in (v.guard("client-override/readme/task-states", stand.tasks, DAG, run_id) or {}).items()
            if t in self.tasks
        }
        payload = (
            v.guard("client-override/readme/xcom", stand.xcom, DAG, run_id, "run")
            if tasks.get("run", {}).get("state") in ("success", "failed")
            else None
        )
        print(
            f"README step 6 run {state}: {json.dumps({t: i.get('state') for t, i in tasks.items()})} {json.dumps(payload, default=str)}",
            flush=True,
        )
        v.check("client-override/readme/step6-continues", continues_problems, state, tasks, payload, events(after_ops),
                ok=f"{events(after_ops)} row(s), the changes since cron stopped")  # fmt: skip
        from .sources import checkpoint_on_host

        v.check(
            "client-override/mysql/cdc/checkpoint-on-host",
            checkpoint_on_host,
            stand.source_state_host / "mysql",
            ok=f"{sorted(str(p.relative_to(stand.dir)) for p in (stand.source_state_host / 'mysql').glob('cdc/*.ckpt'))}",
        )
        source = v.guard("client-override/readme/source", self.src.source_rows, stand, {hot.table: [hot.column]})
        if source is not None:
            cols = {t: [*c, *([hot.column] if t == hot.table else [])] for t, c in eng.tables.items()}
            client = self.src._client()
            v.check(
                "client-override/readme/current-equals-source",
                lambda: stream_equals_source(
                    {t: [dict(r.items()) for r in client.query(f"SELECT * FROM `{self.src.cloud.project}.{self.dataset}.{t}`").result()]
                     for t in eng.tables},
                    source, cols, eng.key,
                ),
                ok=", ".join(f"{t} {len(r)} row(s)" for t, r in sorted(source.items())),
            )  # fmt: skip
        print("COMMANDS (README)\n" + "\n".join(self.commands), flush=True)

    def stream_tasks(self, stand: Any) -> dict[str, dict[str, Any]]:
        try:
            return stand.tasks(DAG, "gate_client_cron_1")
        except Exception:  # no run: the cells say "none"
            return {}

    def cleanup(self) -> None:
        sh(["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{self.bucket}"], timeout=600, check=False)
        warehouse._bq("rm", "-r", "-f", "--dataset", f"{self.src.cloud.project}:{self.dataset}", check=False)
        left = []
        if sh(["gcloud", "storage", "buckets", "describe", f"gs://{self.bucket}"], timeout=300, check=False).returncode == 0:
            left.append(f"gcs:{self.bucket}")
        if sh(["bq", "--quiet", "--headless", "show", f"{self.src.cloud.project}:{self.dataset}"], check=False).returncode == 0:
            left.append(f"bq:{self.dataset}")
        if left:
            raise RuntimeError(f"cloud cleanup left behind {left}")


def legs(src_legs: Sequence[Any], client_override: Path | None) -> list[ClientReadme]:
    return [ClientReadme(s) for s in src_legs if s.engine.name == "mysql"] if client_override else []
