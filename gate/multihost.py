"""The multi-host leg (`--workers 2`): two Celery workers, each a host with a disk of its own, one PostgreSQL state.

Each worker mounts its own directory at one absolute path (`HOST_DISK`), as two hosts do: the provider's state_dir
(config copy, sealed plans, locks, crashed ledger, logs, marker) and init's relative `./output/` destination live on
the host that ran the task. rivet's state is the stack's TLS PostgreSQL (`RIVET_STATE_URL` from a Connection), the
deployment docs/state.md recommends for several workers. Every config is the one `rivet init` writes against the live
source, used as written. Every task names a queue and the gate moves each queue to one host per cycle (Celery
`add_consumer` / `cancel_consumer`), so the two extracts of one batch run land on both hosts, every export changes host
between cycles, and a retry can be sent to the other host; each task instance's hostname, read from the REST API,
proves where it ran. The oracles read the files of both hosts.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from . import oracle
from .scenario import CHANGED_OPS, CHANGES, NO_OPS, SEED, Verdicts, grade_runs, wait_file, wait_runs
from .sources import read_config
from .stand import MULTIHOST_STATE, Stand, worker_hostname, worker_service

TABLES = {
    "gate_customers": ["id", "name", "tier", "updated_at"],
    "gate_orders": ["id", "customer_id", "amount", "status", "updated_at"],
}
URL_ENV = "RIVET_GATE_SOURCE_URL"
#: The export whose apply pauses after its first fetch (the window in which the gate kills it).
PAUSED = "gate_orders"
#: Which host consumes each queue in each cycle. The batch exports sit on gate_a / gate_b (init's order), the stream on
#: gate_cdc; each export changes host from one cycle to the next. `retry` and `budget` start the paused apply on `a`
#: and send its retry to `b`.
CYCLES: tuple[tuple[str, str | None, Mapping[str, Mapping[str, int]], Mapping[str, str]], ...] = (
    ("initial", None, NO_OPS, {"gate_a": "a", "gate_b": "b", "gate_cdc": "a"}),
    ("delta", CHANGES, CHANGED_OPS, {"gate_a": "b", "gate_b": "a", "gate_cdc": "b"}),
    ("idle", None, NO_OPS, {"gate_a": "a", "gate_b": "b", "gate_cdc": "a"}),
    ("retry", CHANGES, CHANGED_OPS, {"gate_a": "b", "gate_b": "a", "gate_cdc": "a"}),
)
#: Keys an incremental extract cannot see deleted (CHANGES deletes five orders each time it runs).
DELETED = {"initial": 0, "delta": 5, "idle": 5, "retry": 10}
QUEUE_OF = {"gate_customers": "gate_a", "gate_orders": "gate_b"}
#: The DAGs graded every cycle, and the pipeline (state_dir under HOST_DISK) each one writes.
PIPELINES = {"gate_mh_batch": "batch", "gate_mh_sqlite": "sqlite", "gate_mh_cdc": "cdc"}
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


class MultiHost:
    """The leg on one stack: init's configs, the routing, the cycles and their oracles."""

    def __init__(self, stand: Stand, v: Verdicts) -> None:
        self.stand, self.v = stand, v
        self.shape: dict[str, Any] = {}
        self.cursor_col: dict[str, str] = {}
        self.slot = ""
        self.cursors: dict[str, dict[str, str]] = {}

    # --- setup ------------------------------------------------------------------------------------------------------

    def prepare(self) -> None:
        """Seed the source, make the state databases, and let `rivet init` write the batch and the CDC config."""
        s = self.stand
        s.psql("gate-source", "rivet", SEED)
        for db in MULTIHOST_STATE.values():
            s.psql("gate-state", "rivet_state", f"CREATE DATABASE {db};")
        source_url = f"postgresql://rivet:{s.secrets.source}@gate-source:5432/rivet"
        docs = {}
        for kind, mode, db in (("batch", "incremental", "rivet_state"), ("cdc", "cdc", MULTIHOST_STATE["cdc"])):
            host = s.rivet_dir / "configs" / f"mh_{kind}"
            host.mkdir(parents=True)
            argv = ["rivet", "init", "--source-env", URL_ENV, "--tls", "disable", "--mode", mode, "--include", *TABLES]
            s.compose(
                "exec", "-T", "-e", URL_ENV, "-e", "RIVET_STATE_URL", "airflow-worker", *argv,
                "-o", f"/opt/rivet/configs/mh_{kind}/rivet.yaml",
                extra_env={URL_ENV: source_url, "RIVET_STATE_URL": s.state_url(db)},
            )  # fmt: skip
            left = sorted(p.name for p in host.glob(".rivet_state.db*"))
            if left:
                raise RuntimeError(f"init left {left} beside {host} although RIVET_STATE_URL named the PostgreSQL state")
            text = (host / "rivet.yaml").read_text()
            print(f"INIT multihost {kind} config:\n{text}", flush=True)
            self.shape[kind] = read_config(text, list(TABLES))
            docs[kind] = yaml.safe_load(text)
        batch, cdc = self.shape["batch"], self.shape["cdc"]
        if sorted(batch["exports"]) != sorted(TABLES) or batch["extra"] or len(cdc["streams"]) != 1:
            raise ValueError(f"init wrote batch {batch}, cdc {cdc}: not one incremental export per table and one stream")
        self.cursor_col = {e["name"]: e["cursor_column"] for e in docs["batch"]["exports"]}
        self.slot = next(e["cdc"]["slot"] for e in docs["cdc"]["exports"] if e.get("mode") == "cdc")
        self.shape |= {"paused": PAUSED, "chain": "gate_customers"}
        (s.rivet_dir / "configs" / "mh_shape.json").write_text(json.dumps(self.shape))
        s.wait_dags([*PIPELINES, "gate_mh_chain"])

    # --- what the oracles read --------------------------------------------------------------------------------------

    def files(self, pipeline: str, under: str) -> set[str]:
        """Parquet files of one pipeline under `output/<under>` on every host's disk."""
        return {
            str(p) for h in self.stand.hosts for p in (self.stand.host_disk(h) / pipeline / "output" / under).rglob("*.parquet")
        }

    def everything(self) -> set[str]:
        return {str(p) for h in self.stand.hosts for p in (self.stand.host_disk(h)).rglob("*.parquet")}

    def stored(self, db: str) -> dict[str, str]:
        """`export_state.last_cursor_value` by export, read with psql."""
        text = self.stand.psql("gate-state", db, "SELECT export_name, coalesce(last_cursor_value, '') FROM export_state")
        return dict(line.split("|", 1) for line in text.splitlines() if "|" in line)

    def hostnames(self, dag: str, run_id: str, tasks: Sequence[str]) -> dict[str, list[str]]:
        """Where each try of each task ran, from the REST API."""
        return {t: [str(x.get("hostname") or "") for x in self.stand.tries(dag, run_id, t)] for t in tasks}

    # --- routing and sabotage ---------------------------------------------------------------------------------------

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
        """Wait for the paused apply on `on`, send its queue to `then`, kill it: its retry runs on `then`."""
        opened = wait_file(self.marker())
        self.v.grade(f"{cell}/paused-on-{on}", [] if opened else [f"the {PAUSED} apply never reached its pause"])
        self.marker().unlink(missing_ok=True)
        self.stand.route(QUEUE_OF[PAUSED], then)
        killed = self.kill_paused(on)
        self.v.grade(f"{cell}/killed-on-{on}", [] if killed == 1 else [f"{killed} rivet apply process(es) killed on {on}"])

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
            # SQLite state on two hosts is measured over the first three cycles; in `retry` its extract would race the
            # queue move the kill makes.
            dags = [d for d in PIPELINES if not (name == "retry" and d == "gate_mh_sqlite")]
            for dag in dags:
                self.stand.trigger(dag, run_id)
            if name == "retry":
                self.paused_then_killed(f"multihost/{name}", "a", "b")
            states = wait_runs(self.stand, dags, run_id)
            after = self.everything()
            new = after - files
            history.append(sorted(f for f in new if "/cdc/output/cdc/" in f and "/snapshot/" not in f))
            self.grade_cycle(name, run_id, states, ops, new, history, marks)
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
        states: dict[str, str],
        ops: Mapping[str, Mapping[str, int]],
        new: set[str],
        history: list[list[str]],
        marks: dict[str, Any],
    ) -> None:
        v, cell = self.v, f"multihost/{name}"
        apply_tasks = [f"apply.{e}" for e in self.shape["batch"]["exports"]]
        tasks = {d: (["run"] if d == "gate_mh_cdc" else apply_tasks) for d in states}
        grade_runs(self.stand, v, cell, run_id, states, tasks)
        routes = dict(next(r for c, _, _, r in CYCLES if c == name))
        for dag, wanted in tasks.items():
            expected = {t: [routes[QUEUE_OF[t.split(".", 1)[1]]] if t != "run" else routes["gate_cdc"]] for t in wanted}
            if name == "retry" and dag == "gate_mh_batch":
                expected[f"apply.{PAUSED}"] = ["a", "b"]  # killed on a, retried on b
            expected = {t: [worker_hostname(h) for h in hosts] for t, hosts in expected.items()}
            seen = v.guard(f"{cell}/{dag}/hosts", self.hostnames, dag, run_id, wanted)
            if seen is not None:
                v.check(f"{cell}/{dag}/hosts", oracle.hosts_as_routed, seen, expected, dag, ok=str(seen))
        con = oracle.connect(self.stand.source_dsn)
        try:
            for pipeline, dag, db in (("batch", "gate_mh_batch", "rivet_state"), ("sqlite", "gate_mh_sqlite", None)):
                if dag not in states:
                    continue
                self.grade_batch(con, f"{cell}/{pipeline}", dag, run_id, pipeline, db, new, marks, name)
            self.grade_cdc(con, cell, run_id, ops, new, history)
        finally:
            con.close()

    def grade_batch(
        self, con: Any, cell: str, dag: str, run_id: str, pipeline: str, db: str | None, new: set[str], marks: dict, name: str
    ) -> None:
        v = self.v
        cursors = v.guard(f"{cell}/stored-cursors", self.stored, db) if db else None
        for table, cols in TABLES.items():
            src, cur = f"src.public.{table}", self.cursor_col[table]
            every = sorted(self.files(pipeline, table))
            mine = sorted(set(every) & new)
            c = f"{cell}/{table}"
            v.check(
                c + "/delta-values",
                oracle.batch_delta,
                con,
                mine,
                src,
                cols,
                cur,
                marks.get(table),
                ok=f"{oracle.rows_in(con, mine)} row(s)",
            )
            deleted = DELETED[name] if table == "gate_orders" else 0
            v.check(c + "/latest-equals-source", oracle.batch_latest, con, every, src, cols, "id", cur, deleted)
            v.check(c + "/each-version-once", oracle.duplicates, con, every, ["id", cur], c)
            xcom = v.guard(c + "/xcom-rows", self.stand.xcom, dag, run_id, f"apply.{table}")
            if xcom is not None:
                reported = sum(u.get("rows") or 0 for u in xcom["units"])
                v.check(
                    c + "/xcom-rows",
                    oracle.same_count,
                    reported,
                    oracle.rows_in(con, mine),
                    c,
                    ok=f"degraded {xcom.get('degraded')}",
                )
            if cursors is not None:
                top = con.execute(f"SELECT max({cur}) FROM {src}").fetchone()[0]
                v.check(c + "/stored-cursor", oracle.stored_cursor, cursors.get(table) or None, top, c)
                before = self.cursors.get(pipeline, {}).get(table)
                v.check(c + "/cursor-not-regressed", oracle.not_regressed, before, cursors.get(table) or None, c)
        if cursors is not None:
            self.cursors[pipeline] = cursors

    def grade_cdc(
        self, con: Any, cell: str, run_id: str, ops: Mapping[str, Mapping[str, int]], new: set[str], history: list[list[str]]
    ) -> None:
        v = self.v
        stream_new = history[-1]
        for table, cols in TABLES.items():
            c = f"{cell}/cdc/{table}"
            # backfill: auto runs the table's batch recipe once, after the anchor, into the stream's `<table>/snapshot/`
            baseline = sorted(self.files("cdc", f"cdc/{table}/snapshot"))
            changes = [f for f in stream_new if f"/cdc/output/cdc/{table}/" in f]
            v.check(c + "/operations", oracle.cdc_ops, con, changes, ops[table], c)
            stream = [[f for f in run if f"/cdc/output/cdc/{table}/" in f] for run in history]
            v.check(c + "/replay-equals-source", oracle.cdc_replay, con, baseline, stream, f"src.public.{table}", cols, "id")
            v.check(c + "/baseline-once", oracle.duplicates, con, baseline, ["id"], c + " baseline")
            events = sorted(f for f in self.files("cdc", f"cdc/{table}") if "/snapshot/" not in f)
            v.check(c + "/each-event-once", oracle.duplicates, con, events, ["__pos", "__seq"], c)
        xcom = v.guard(f"{cell}/cdc/xcom-rows", self.stand.xcom, "gate_mh_cdc", run_id, "run")
        if xcom is not None:
            reported = sum(u.get("rows") or 0 for u in xcom["units"])
            counted = oracle.rows_in(con, sorted(f for f in new if "/cdc/output/" in f))
            v.check(f"{cell}/cdc/xcom-rows", oracle.same_count, reported, counted, "cdc", ok=f"degraded {xcom.get('degraded')}")

    # --- measurements beyond the cycles -----------------------------------------------------------------------------

    def chain(self) -> None:
        """An extract on host a and the load after it on host b, both on PostgreSQL state: what the load does."""
        run_id = "gate_mh_chain_1"
        self.route({"gate_a": "a", "gate_b": "b"})
        self.stand.trigger("gate_mh_chain", run_id)
        wait_runs(self.stand, ["gate_mh_chain"], run_id)
        chain = self.shape["chain"]
        tasks = self.stand.tasks("gate_mh_chain", run_id)
        load = tasks.get(f"load.{chain}", {})
        xcom = self.v.guard("multihost/chain/xcom", self.stand.xcom, "gate_mh_chain", run_id, f"load.{chain}") or {}
        seen = self.hostnames("gate_mh_chain", run_id, [f"apply.{chain}", f"load.{chain}"])
        print(
            f"MEASURE multihost/chain: apply {tasks.get(f'apply.{chain}', {}).get('state')} on {seen[f'apply.{chain}']}, "
            f"load {load.get('state')} on {seen[f'load.{chain}']}: refusal {xcom.get('preflight_refusal')}, "
            f"error {xcom.get('error')}",
            flush=True,
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
                p.read_text()
                for p in (self.stand.host_disk(h) / "batch" / "airflow").rglob("*.json")
                if p.parent.name == "crashed"
            )
            for h in self.stand.hosts
        }
        print(f"MEASURE {cell}: tries {shown}; crashed ledgers {ledgers}", flush=True)
        # crashed_retries=1: one crash is retried, the second crash of the task instance is not, wherever it ran.
        self.v.grade(
            f"{cell}/second-crash-not-retried",
            [] if len(tries) == 2 else [f"tries {shown}: the crash on the second host was retried too; ledgers {ledgers}"],
            ok=str(shown),
        )


def play(stand: Stand, v: Verdicts) -> None:
    MultiHost(stand, v).play()
