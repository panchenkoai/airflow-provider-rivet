"""The multi-host leg (`--workers 2`): two Celery workers, each a host with a disk of its own, one PostgreSQL state.

Each worker mounts its own directory at one absolute path (`HOST_DISK`), as two hosts do: the provider's state_dir
(config copy, sealed plans, locks, crashed ledger, logs, marker) lives on the host that ran the task. rivet's state is
the stack's TLS PostgreSQL (`RIVET_STATE_URL` from a Connection), the deployment docs/state.md recommends for several
workers, and the files go to GCS, storage both hosts reach. Every config is the one `rivet init` writes against the
live source (`--gcs-bucket`: a bucket of the run's own per config), used as written. Every task names a queue and the
gate moves each queue to one host per cycle (Celery `add_consumer` / `cancel_consumer`), so the two extracts of one
batch run land on both hosts, every export changes host between cycles, and a retry is sent to the other host; each
try's hostname, read from the REST API, proves where it ran. The oracles read the bucket through `gcloud storage`,
a client rivet does not use.

Two measurements run beside the graded pipelines and print `MEASURE` lines instead of verdicts: SQLite state in each
host's state_dir, and init's default local `./output/` destination (which lands in the state_dir) on PostgreSQL state.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from . import oracle, warehouse
from .scenario import CHANGED_OPS, CHANGES, NO_OPS, SEED, Verdicts, grade_runs, wait_file, wait_runs
from .sources import read_config
from .stand import Stand, sh, worker_hostname, worker_service

TABLES = {
    "gate_customers": ["id", "name", "tier", "updated_at"],
    "gate_orders": ["id", "customer_id", "amount", "status", "updated_at"],
}
URL_ENV = "RIVET_GATE_SOURCE_URL"
#: The export whose apply pauses after its first fetch (the window in which the gate kills it).
PAUSED = "gate_orders"
QUEUE_OF = {"apply.gate_customers": "gate_a", "apply.gate_orders": "gate_b", "run": "gate_cdc"}
#: Which host consumes each queue in each cycle: every export changes host from one cycle to the next. In `retry` the
#: paused apply starts on `a`, is killed there, and its retry is sent to `b`.
CYCLES: tuple[tuple[str, str | None, Mapping[str, Mapping[str, int]], Mapping[str, str]], ...] = (
    ("initial", None, NO_OPS, {"gate_a": "a", "gate_b": "b", "gate_cdc": "a"}),
    ("delta", CHANGES, CHANGED_OPS, {"gate_a": "b", "gate_b": "a", "gate_cdc": "b"}),
    ("idle", None, NO_OPS, {"gate_a": "a", "gate_b": "b", "gate_cdc": "a"}),
    ("retry", CHANGES, CHANGED_OPS, {"gate_a": "b", "gate_b": "a", "gate_cdc": "b"}),
)
#: Keys an incremental extract cannot see deleted (CHANGES deletes five orders each time it runs).
DELETED = {"initial": 0, "delta": 5, "idle": 5, "retry": 10}


@dataclass(frozen=True)
class Pipeline:
    """One config `rivet init` writes and the DAG that runs it; its state_dir is `HOST_DISK/<name>` on each host."""

    name: str
    mode: str  # init's --mode
    state_db: str | None  # the PostgreSQL state database; None: SQLite in the state_dir
    gcs: bool = True  # init's --gcs-bucket; False: init's local `./output/`, which lands in the state_dir
    graded: bool = True  # False: a measurement, printed, never a verdict
    cycles: tuple[str, ...] = ("initial", "delta", "idle", "retry")
    load: bool = False

    @property
    def dag(self) -> str:
        return f"gate_mh_{self.name}"

    @property
    def state_conn(self) -> str | None:
        """The Connection of the state database (the stack's own `rivet_gate_state` for `rivet_state`)."""
        if self.state_db is None:
            return None
        return (
            "rivet_gate_state" if self.state_db == "rivet_state" else f"rivet_gate_state_{self.state_db[len('rivet_state_') :]}"
        )


PIPELINES = (
    Pipeline("batch", "incremental", "rivet_state"),
    Pipeline("cdc", "cdc", "rivet_state_mh_cdc"),
    # Measurements: SQLite state on two hosts; init's default local destination on two hosts.
    Pipeline("sqlite", "incremental", None, graded=False, cycles=("initial", "delta", "idle")),
    Pipeline("local", "incremental", "rivet_state_mh_local", gcs=False, graded=False, cycles=("initial", "delta", "idle")),
    # An extract on one host and its load on the other (run once, after the first cycle).
    Pipeline("chain", "incremental", "rivet_state_mh_chain", graded=False, cycles=(), load=True),
)
BY_NAME = {p.name: p for p in PIPELINES}
#: The databases the leg makes in the stack's state server (`rivet_state` is the stack's own); init of the SQLite
#: pipeline records the source's keys in one of its own, which no run reads.
STATE_DBS = sorted({p.state_db for p in PIPELINES if p.state_db and p.state_db != "rivet_state"} | {"rivet_state_mh_init"})

#: SIGKILL the paused `rivet apply` on a host (the one whose environment names the pause), nothing else; print how many.
KILL_PAUSED = (
    "import os\n"
    "n = 0\n"
    "for p in filter(str.isdigit, os.listdir('/proc')):\n"
    "    try:\n"
    "        argv = open(f'/proc/{p}/cmdline', 'rb').read().split(b'\\0')\n"
    "        env = open(f'/proc/{p}/environ', 'rb').read().split(b'\\0')\n"
    "    except OSError:\n"
    "        continue\n"
    "    if argv[0].endswith(b'/rivet') and argv[1:2] == [b'apply'] and any(e.startswith(b'RIVET_TEST_PAUSE_AT=') for e in env):\n"
    "        os.kill(int(p), 9)\n"
    "        n += 1\n"
    "print(n)\n"
)


class Measure(Verdicts):
    """Oracle findings printed as `MEASURE` lines and kept out of the verdicts: what a setup the provider documents as
    unsupported does, recorded as evidence."""

    def grade(self, cell: str, problems: list[str], ok: str = "") -> None:
        if problems:  # what the oracle accepts is not evidence of anything
            print(f"MEASURE airflow-{self.stack} {cell} — {'; '.join(problems)}"[:1500], flush=True)


def bucket(cloud: warehouse.Cloud, stack: str, pipeline: str) -> str:
    """The pipeline's bucket, of the shape the source legs' sweep drops: `rivet-afgate-<tag>-<stack>-mh<p>-batch|cdc`."""
    kind = "cdc" if pipeline == "cdc" else "batch"
    middle = "mh" if pipeline in ("batch", "cdc") else f"mh{pipeline}"
    return f"rivet-afgate-{cloud.tag}-{stack.replace('.', '')}-{middle}-{kind}"


class MultiHost:
    """The leg on one stack: init's configs, the routing, the cycles and their oracles."""

    def __init__(self, stand: Stand, v: Verdicts, cloud: warehouse.Cloud) -> None:
        self.stand, self.v, self.cloud = stand, v, cloud
        self.measure = Measure(v.stack)
        self.stack = stand.flavor.version
        self.shape: dict[str, Any] = {"paused": PAUSED, "pipelines": {}}
        self.cursor_col: dict[str, str] = {}
        self.cursors: dict[str, dict[str, str]] = {}
        self.mirror = stand.dir / "mirror"

    # --- setup ------------------------------------------------------------------------------------------------------

    def prepare(self) -> None:
        """Seed the source, make the state databases and the buckets, and let `rivet init` write each config."""
        s = self.stand
        s.psql("gate-source", "rivet", SEED)
        for db in STATE_DBS:
            s.psql("gate-state", "rivet_state", f"CREATE DATABASE {db};")
        source_url = f"postgresql://rivet:{s.secrets.source}@gate-source:5432/rivet"
        for p in PIPELINES:
            argv = ["rivet", "init", "--source-env", URL_ENV, "--tls", "disable", "--mode", p.mode, "--include", *TABLES]
            if p.gcs:
                name = bucket(self.cloud, self.stack, p.name)
                sh(["gcloud", "storage", "buckets", "create", f"gs://{name}", f"--project={self.cloud.project}",
                    f"--location={self.cloud.location}", "--uniform-bucket-level-access"], timeout=300)  # fmt: skip
                argv += ["--gcs-bucket", name]
            host = s.rivet_dir / "configs" / f"mh_{p.name}"
            host.mkdir(parents=True)
            s.compose(
                "exec", "-T", "-e", URL_ENV, "-e", "RIVET_STATE_URL", "airflow-worker", *argv,
                "-o", f"/opt/rivet/configs/mh_{p.name}/rivet.yaml",
                extra_env={URL_ENV: source_url, "RIVET_STATE_URL": s.state_url(p.state_db or "rivet_state_mh_init")},
            )  # fmt: skip
            left = sorted(f.name for f in host.glob(".rivet_state.db*"))
            if left:
                raise RuntimeError(f"init left {left} beside {host} although RIVET_STATE_URL named the PostgreSQL state")
            text = (host / "rivet.yaml").read_text()
            if p.name in ("batch", "cdc"):
                print(f"INIT multihost {p.name} config:\n{text}", flush=True)
            found = read_config(text, list(TABLES))
            if p.mode == "cdc":
                if len(found["streams"]) != 1:
                    raise ValueError(f"init wrote streams {found['streams']}: not one stream over the tables")
            elif sorted(found["exports"]) != sorted(TABLES) or found["extra"] or found["streams"]:
                raise ValueError(f"init wrote {found}: not one incremental export per table")
            if p.name == "batch":
                self.cursor_col = {e["name"]: e["cursor_column"] for e in yaml.safe_load(text)["exports"]}
            self.shape["pipelines"][p.name] = {
                "kind": "cdc" if p.mode == "cdc" else "batch",
                "exports": found["exports"] if p.mode != "cdc" else [],
                "stream": found["streams"][0] if p.mode == "cdc" else None,
                "state_conn": p.state_conn,
                "gcs": p.gcs,
                "load": p.load,
                "queues": {t: q for t, q in QUEUE_OF.items() if (t == "run") == (p.mode == "cdc")}
                | ({"load.gate_customers": "gate_b"} if p.load else {}),
                "pause": p.name == "batch",
            }
        (s.rivet_dir / "configs" / "mh_shape.json").write_text(json.dumps(self.shape))
        s.wait_dags([p.dag for p in PIPELINES])

    def cleanup(self) -> None:
        """Delete the leg's buckets and look again: nothing may be left."""
        names = [bucket(self.cloud, self.stack, p.name) for p in PIPELINES if p.gcs]
        for name in names:
            sh(["gcloud", "storage", "rm", "-r", "--quiet", f"gs://{name}"], timeout=600, check=False)
        left = [n for n in names if sh(["gcloud", "storage", "buckets", "describe", f"gs://{n}"], check=False).returncode == 0]
        if left:
            raise RuntimeError(f"buckets left after the cleanup: {left}")

    # --- what the oracles read --------------------------------------------------------------------------------------

    def sync(self) -> None:
        """Mirror every bucket of the leg here, with `gcloud storage` (not rivet's client)."""
        for p in PIPELINES:
            if p.gcs:
                dest = self.mirror / p.name
                dest.mkdir(parents=True, exist_ok=True)
                sh(["gcloud", "storage", "rsync", "-r", "--quiet", f"gs://{bucket(self.cloud, self.stack, p.name)}", str(dest)], timeout=600)  # fmt: skip

    def roots(self, p: Pipeline) -> list[Path]:
        """Where a pipeline's files are: its bucket's mirror, or every host's `<state_dir>/output`."""
        if p.gcs:
            return [self.mirror / p.name]
        return [self.stand.host_disk(h) / p.name / "output" for h in self.stand.hosts]

    def files(self, p: Pipeline, under: str) -> set[str]:
        """Parquet files under `under` (init's prefix of a table, or `cdc/<table>`) of one pipeline."""
        if p.gcs and p.mode != "cdc":
            under = f"exports/{under}"
        return {str(f) for root in self.roots(p) for f in (root / under).rglob("*.parquet")}

    def everything(self) -> set[str]:
        self.sync()
        return {str(f) for p in PIPELINES for root in self.roots(p) for f in root.rglob("*.parquet")}

    def stored(self, db: str) -> dict[str, str]:
        """`export_state.last_cursor_value` by export, read with psql."""
        text = self.stand.psql("gate-state", db, "SELECT export_name, coalesce(last_cursor_value, '') FROM export_state")
        return dict(line.split("|", 1) for line in text.splitlines() if "|" in line)

    def hostnames(self, dag: str, run_id: str, tasks: Sequence[str]) -> dict[str, list[str]]:
        """Where each try of each task ran, from the REST API."""
        return {t: [str(x.get("hostname") or "") for x in self.stand.tries(dag, run_id, t)] for t in tasks}

    def outcomes(self, dag: str, run_id: str) -> str:
        """Each task's state, host(s) and, when it failed, the code its XCom names: the evidence of a failed run."""
        shown = []
        for task, ti in sorted(self.stand.tasks(dag, run_id).items()):
            hosts = [str(t.get("hostname") or "").split(".")[0] for t in self.stand.tries(dag, run_id, task)]
            line = f"{task} {ti['state']} on {hosts}"
            if ti["state"] not in ("success", "skipped", None) and task != "watcher":
                try:
                    x = self.stand.xcom(dag, run_id, task)
                    line += f" ({x.get('preflight_refusal') or x.get('busy') or (x.get('error') or {}).get('code')})"
                except Exception as exc:  # no XCom: the task never published one
                    line += f" (no xcom: {type(exc).__name__})"
            shown.append(line)
        return "; ".join(shown)

    # --- routing and the crash ----------------------------------------------------------------------------------------

    def route(self, routes: Mapping[str, str]) -> None:
        for queue, host in routes.items():
            self.stand.route(queue, host)

    def kill_paused(self, host: str) -> int:
        """SIGKILL the paused `rivet apply` on one host: a crash of the extract its task is running there."""
        out = self.stand.compose("exec", "-T", worker_service(host), "python", "-c", KILL_PAUSED).stdout
        return int(out.strip() or 0)

    def marker(self) -> Path:
        return self.stand.rivet_dir / "markers" / f"mh_{PAUSED}"

    def paused_then_killed(self, cell: str, on: str, then: str) -> None:
        """Wait for the paused apply on `on`, kill it, and send its queue to `then`, where its retry runs (the DAG's
        retry_delay outlasts the queue move)."""
        opened = wait_file(self.marker(), timeout=300)
        self.v.grade(f"{cell}/paused-on-{on}", [] if opened else [f"the {PAUSED} apply never reached its pause"])
        self.marker().unlink(missing_ok=True)
        killed = self.kill_paused(on) if opened else 0
        self.v.grade(f"{cell}/killed-on-{on}", [] if killed == 1 else [f"{killed} paused rivet apply process(es) killed on {on}"])
        self.stand.route("gate_b", then)

    # --- the cycles -------------------------------------------------------------------------------------------------

    def play(self) -> None:
        self.prepare()
        files: set[str] = set()
        marks: dict[str, Any] = {}
        history: list[list[str]] = []
        for n, (name, changes, ops, routes) in enumerate(CYCLES, start=1):
            run_id = f"gate_mh_{n}_{name}"
            if changes:
                self.stand.psql("gate-source", "rivet", changes)
            self.route(routes)
            self.marker().unlink(missing_ok=True)
            # In `retry` only the graded pipelines run: another extract on host a would race the queue move.
            dags = {p.dag: p for p in PIPELINES if name in p.cycles}
            for dag in dags:
                self.stand.trigger(dag, run_id)
            if name == "retry":
                self.paused_then_killed(f"multihost/{name}", "a", "b")
            states = wait_runs(self.stand, list(dags), run_id)
            if name == "retry":
                # The sealed plan the killed try made is on host a's disk; the retry on b finds none and plans again.
                plans = {
                    h: sorted(f.name for f in (self.stand.host_disk(h) / "batch" / "airflow").rglob("*.json") if run_id in str(f))
                    for h in self.stand.hosts
                }
                print(f"MEASURE airflow-{self.stack} multihost/retry sealed plans by host: {plans}", flush=True)
            for dag, state in states.items():
                if state != "success":
                    print(
                        f"MEASURE airflow-{self.stack} multihost/{name}/{dag} {state}: {self.outcomes(dag, run_id)}", flush=True
                    )
            after = self.everything()
            new = after - files
            history.append(sorted(f for f in new if f.startswith(str(self.mirror / "cdc")) and "/snapshot/" not in f))
            self.grade_cycle(name, run_id, routes, states, ops, new, history, marks)
            if name == "initial":
                self.chain()
            files, marks = after, self.high_water()
        self.budget(len(CYCLES) + 1)

    def high_water(self) -> dict[str, Any]:
        con = oracle.connect(self.stand.source_dsn)
        try:
            return {t: con.execute(f"SELECT max(updated_at) FROM src.public.{t}").fetchone()[0] for t in TABLES}
        finally:
            con.close()

    def grade_cycle(
        self,
        name: str,
        run_id: str,
        routes: Mapping[str, str],
        states: dict[str, str],
        ops: Mapping[str, Mapping[str, int]],
        new: set[str],
        history: list[list[str]],
        marks: dict[str, Any],
    ) -> None:
        cell = f"multihost/{name}"
        con = oracle.connect(self.stand.source_dsn)
        try:
            for dag in states:
                p = next(x for x in PIPELINES if x.dag == dag)
                v = self.v if p.graded else self.measure
                wanted = ["run"] if p.mode == "cdc" else [f"apply.{t}" for t in TABLES]
                grade_runs(self.stand, v, cell, run_id, {dag: states[dag]}, {dag: wanted})
                expected = {t: [routes[QUEUE_OF[t]]] for t in wanted}
                if name == "retry" and p.name == "batch":
                    expected[f"apply.{PAUSED}"] = ["a", "b"]  # killed on a, retried on b
                expected = {t: [worker_hostname(h) for h in hosts] for t, hosts in expected.items()}
                seen = v.guard(f"{cell}/{dag}/hosts", self.hostnames, dag, run_id, wanted)
                if seen is not None:
                    v.check(f"{cell}/{dag}/hosts", oracle.hosts_as_routed, seen, expected, dag, ok=str(seen))
                if p.mode == "cdc":
                    self.grade_cdc(con, cell, run_id, p, ops, history)
                else:
                    self.grade_batch(v, con, f"{cell}/{p.name}", run_id, p, new, marks, name)
        finally:
            con.close()

    def grade_batch(
        self, v: Verdicts, con: Any, cell: str, run_id: str, p: Pipeline, new: set[str], marks: dict, name: str
    ) -> None:
        cursors = v.guard(f"{cell}/stored-cursors", self.stored, p.state_db) if p.state_db else None
        for table, cols in TABLES.items():
            src, cur = f"src.public.{table}", self.cursor_col[table]
            every = sorted(self.files(p, table))
            mine = sorted(set(every) & new)
            c = f"{cell}/{table}"
            rows = oracle.rows_in(con, mine)
            v.check(c + "/delta-values", oracle.batch_delta, con, mine, src, cols, cur, marks.get(table), ok=f"{rows} row(s)")
            deleted = DELETED[name] if table == "gate_orders" else 0
            v.check(c + "/latest-equals-source", oracle.batch_latest, con, every, src, cols, "id", cur, deleted)
            v.check(c + "/each-version-once", oracle.duplicates, con, every, ["id", cur], c)
            xcom = v.guard(c + "/xcom-rows", self.stand.xcom, p.dag, run_id, f"apply.{table}")
            if xcom is not None:
                reported = sum(u.get("rows") or 0 for u in xcom["units"])
                v.check(c + "/xcom-rows", oracle.same_count, reported, rows, c, ok=f"degraded {xcom.get('degraded')}")
            if cursors is not None:
                top = con.execute(f"SELECT max({cur}) FROM {src}").fetchone()[0]
                v.check(c + "/stored-cursor", oracle.stored_cursor, cursors.get(table) or None, top, c)
                before = self.cursors.get(p.name, {}).get(table)
                v.check(c + "/cursor-not-regressed", oracle.not_regressed, before, cursors.get(table) or None, c)
        if cursors is not None:
            self.cursors[p.name] = cursors

    def grade_cdc(
        self, con: Any, cell: str, run_id: str, p: Pipeline, ops: Mapping[str, Mapping[str, int]], history: list[list[str]]
    ) -> None:
        v = self.v
        for table, cols in TABLES.items():
            c = f"{cell}/cdc/{table}"
            # backfill: auto runs the table's batch recipe once, after the anchor, into the stream's `<table>/snapshot/`
            baseline = sorted(self.files(p, f"cdc/{table}/snapshot"))
            changes = [f for f in history[-1] if f"/cdc/{table}/" in f]
            v.check(c + "/operations", oracle.cdc_ops, con, changes, ops[table], c)
            stream = [[f for f in run if f"/cdc/{table}/" in f] for run in history]
            v.check(c + "/replay-equals-source", oracle.cdc_replay, con, baseline, stream, f"src.public.{table}", cols, "id")
            v.check(c + "/baseline-once", oracle.duplicates, con, baseline, ["id"], c + " baseline")
            events = sorted(f for f in self.files(p, f"cdc/{table}") if "/snapshot/" not in f)
            v.check(c + "/each-event-once", oracle.duplicates, con, events, ["__pos", "__seq"], c)
        xcom = v.guard(f"{cell}/cdc/xcom-rows", self.stand.xcom, p.dag, run_id, "run")
        if xcom is not None:
            reported = sum(u.get("rows") or 0 for u in xcom["units"])
            counted = oracle.rows_in(con, history[-1] + self._new_snapshots(p, len(history)))
            v.check(f"{cell}/cdc/xcom-rows", oracle.same_count, reported, counted, "cdc", ok=f"degraded {xcom.get('degraded')}")

    def _new_snapshots(self, p: Pipeline, cycle: int) -> list[str]:
        """The baseline counts in the first cycle's XCom (the stream ran it); never after."""
        return sorted(f for t in TABLES for f in self.files(p, f"cdc/{t}/snapshot")) if cycle == 1 else []

    # --- measurements beyond the cycles -----------------------------------------------------------------------------

    def chain(self) -> None:
        """An extract on host a and the load after it on host b, both on PostgreSQL state: what the load does."""
        run_id = "gate_mh_chain_1"
        self.route({"gate_a": "a", "gate_b": "b"})
        self.stand.trigger("gate_mh_chain", run_id)
        state = wait_runs(self.stand, ["gate_mh_chain"], run_id)["gate_mh_chain"]
        print(f"MEASURE airflow-{self.stack} multihost/chain {state}: {self.outcomes('gate_mh_chain', run_id)}", flush=True)
        # The config has no `load:` block, so rivet's load itself fails; what is graded is that the task ran rivet on
        # host b instead of refusing the other host's state_dir (PostgreSQL state reads nothing from it).
        load = "load.gate_customers"
        seen = self.hostnames("gate_mh_chain", run_id, ["apply.gate_customers", load])
        x = self.v.guard("multihost/chain/load-on-other-host", self.stand.xcom, "gate_mh_chain", run_id, load)
        if x is not None:
            hosts = [h.split(".")[0] for h in seen["apply.gate_customers"] + seen[load][:1]]
            self.v.grade(
                "multihost/chain/load-on-other-host",
                ([f"refused {x['preflight_refusal']}"] if x.get("preflight_refusal") else [])
                + ([f"ran on {hosts}"] if hosts != [worker_hostname("a"), worker_hostname("b")] else []),
                ok=f"apply on a, load on b ran rivet: {(x.get('error') or {}).get('code')}",
            )

    def budget(self, n: int) -> None:
        """crashed_retries=1 across two hosts: the paused apply is killed on a, its retry routed to b and killed there."""
        run_id, cell = f"gate_mh_{n}_budget", "multihost/budget"
        self.route({"gate_a": "a", "gate_b": "a"})
        self.marker().unlink(missing_ok=True)
        self.stand.trigger("gate_mh_batch", run_id)
        self.paused_then_killed(cell, "a", "b")
        self.paused_then_killed(cell, "b", "b")
        wait_runs(self.stand, ["gate_mh_batch"], run_id)
        tries = self.stand.tries("gate_mh_batch", run_id, f"apply.{PAUSED}")
        shown = [(t["try_number"], (t.get("hostname") or "").split(".")[0], t["state"]) for t in tries]
        ledgers = {
            h: sorted(
                f.read_text()
                for f in (self.stand.host_disk(h) / "batch" / "airflow").rglob("*.json")
                if f.parent.name == "crashed"
            )
            for h in self.stand.hosts
        }
        print(f"MEASURE airflow-{self.stack} {cell}: tries {shown}; crashed ledgers {ledgers}", flush=True)
        # crashed_retries=1: one crash is retried, the second crash of the task instance is not, wherever it ran.
        self.v.grade(
            f"{cell}/second-crash-not-retried",
            [] if len(tries) == 2 else [f"tries {shown}: the crash on the second host was retried too; ledgers {ledgers}"],
            ok=str(shown),
        )
