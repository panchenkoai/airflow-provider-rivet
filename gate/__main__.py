"""python -m gate --rivet-bin <linux rivet> [--airflow 2.10.5,3.3.2] [--sources mysql,mssql,mongodb,oracle] [--destinations s3] [--workers 2] [--verdicts out.jsonl] [--keep] [--sabotage drop-delta-file|warehouse-value]

With --warehouses: [--connections env|gsm] [--gcp-auth key|adc] choose where the stack's Connections live and how
rivet authenticates to GCP; see docs/connections-and-secrets.md.

Exit 0 only when every cell ran and passed; each stack is torn down (volumes too) before and after it runs.
"""

from __future__ import annotations

import argparse
import json
import secrets
import shutil
import sys
import tempfile
import time
from pathlib import Path

from . import conns as proofs
from . import destinations, migrate, multihost, sources
from .scenario import TABLES, Verdicts, play
from .stand import FLAVORS, Stand
from .warehouse import (
    TAG_SHAPE,
    WAREHOUSES,
    Cloud,
    Leg,
    connections,
    delete_connections,
    gcp_connection,
    publish_connections,
    sweep,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m gate", description=__doc__.splitlines()[0])
    ap.add_argument("--rivet-bin", required=True, type=Path, help="a rivet Linux binary for the docker host's architecture")
    ap.add_argument("--airflow", default=",".join(FLAVORS), help="comma-separated Airflow versions")
    ap.add_argument("--work", type=Path, help="working directory (default: a fresh temporary one)")
    ap.add_argument("--verdicts", type=Path, help="write one JSON line per graded cell")
    ap.add_argument("--keep", action="store_true", help="leave the stacks up and the working directory in place")
    ap.add_argument("--warehouses", default="", help="comma-separated: bigquery,snowflake,clickhouse (the warehouse leg)")
    ap.add_argument(
        "--sources",
        default="",
        help=f"comma-separated source legs beside postgres ({','.join(sources.ENGINES)}): each to GCS and BigQuery on the "
        "configs `rivet init` writes; needs --warehouses",
    )
    ap.add_argument(
        "--destinations",
        default="",
        help=f"comma-separated destination legs ({','.join(destinations.DESTINATIONS)}): MinIO in the stack, the configs "
        "`rivet init --s3-bucket` writes, extract-only and loaded into the stack's ClickHouse",
    )
    ap.add_argument(
        "--state",
        choices=["gate", "compose-postgres"],
        default="gate",
        help="where the source legs keep rivet's state: the gate's own TLS PostgreSQL, or (the client's) a database and a "
        "role of its own on the compose file's `postgres` server, beside Airflow's metadata database",
    )
    ap.add_argument(
        "--client-override",
        type=Path,
        help="the client's delivered compose files (docker-compose.rivet.yml, Dockerfile.rivet, env.rivet.example, "
        "README.md): run them as delivered on top of the official file (--airflow 2.10.2, --sources mysql); implies "
        "--gcp-auth env-file",
    )
    ap.add_argument(
        "--migrate-from",
        type=Path,
        help="rivet 0.32.1 (Linux): the migration leg, a MySQL CDC stream on SQLite state under it, carried to PostgreSQL "
        "state with this --rivet-bin's `rivet state copy` (needs --sources with mysql)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        choices=[1, 2],
        default=1,
        help="2: the multi-host leg (two Celery workers, a disk each, PostgreSQL state) instead of the base scenario",
    )
    ap.add_argument("--gcp-keyfile", type=Path, help="GCP credential JSON for the gate_gcp Connection (service account or ADC)")
    ap.add_argument("--gcs-bucket", default="rivet_data_test")
    ap.add_argument("--bq-project", default="rivet-data-tool")
    ap.add_argument("--bq-location", default="US")
    ap.add_argument(
        "--sf-connection", default="rivet", help="local `snow` connection whose login the gate_snowflake Connection copies"
    )
    ap.add_argument("--sf-warehouse", default="RIVET_LOAD_TEST")
    ap.add_argument("--sf-database", default="RIVET_LOAD_TEST")
    ap.add_argument("--sf-integration", default="RIVET_GCS")
    ap.add_argument(
        "--connections",
        choices=["env", "gsm"],
        default="env",
        help="where the stack's Connections live: AIRFLOW_CONN_* variables, or Google Secret Manager as the secrets "
        "backend (secrets airflow-connections-afgate_<tag>_<stack>_<conn_id>, deleted after the stack)",
    )
    ap.add_argument(
        "--gcp-auth",
        choices=["key", "adc", "env-file"],
        default="key",
        help="key: gate_gcp carries the credential; adc: gate_gcp holds only `project`, and rivet runs as the "
        "containers' identity (--gcp-keyfile mounted read-only where Google's default chain looks); env-file (the "
        "client's): no GCP Connection, --gcp-keyfile mounted read-only at /etc/gcloud_cred.json and named by "
        "GOOGLE_APPLICATION_CREDENTIALS, the DAGs pass it with cloud_credentials=['gcp']",
    )
    ap.add_argument(
        "--gsm-reader",
        default="afgate-secrets-reader@{project}.iam.gserviceaccount.com",
        help="service account the secrets backend impersonates (secretAccessor on airflow-connections-afgate_* only); "
        "empty: the mounted credential reads the secrets itself",
    )
    ap.add_argument(
        "--sabotage",
        choices=["drop-delta-file", "warehouse-value"],
        help="break one output on purpose (a delta file; one BigQuery value after the load): the gate must go red",
    )
    ap.add_argument(
        "--tag",
        help="run tag (default: %%m%%d%%H%%M + 4 hex): names the compose projects, images, dataset, schema and GCS prefix",
    )
    args = ap.parse_args(argv)

    versions = [v.strip() for v in args.airflow.split(",") if v.strip()]
    unknown = sorted(set(versions) - set(FLAVORS))
    if unknown:
        ap.error(f"unknown Airflow version(s) {unknown}; known: {sorted(FLAVORS)}")
    warehouses = tuple(w for w in args.warehouses.split(",") if w)
    if set(warehouses) - set(WAREHOUSES):
        ap.error(f"unknown warehouse(s) {sorted(set(warehouses) - set(WAREHOUSES))}; known: {list(WAREHOUSES)}")
    engines = tuple(e for e in args.sources.split(",") if e)
    if set(engines) - set(sources.ENGINES):
        ap.error(f"unknown source(s) {sorted(set(engines) - set(sources.ENGINES))}; known: {list(sources.ENGINES)}")
    if args.client_override:
        if versions != ["2.10.2"] or "mysql" not in engines:
            ap.error("--client-override runs the client's stack: --airflow 2.10.2 and --sources with mysql")
        args.gcp_auth = "env-file"  # the client's: GOOGLE_APPLICATION_CREDENTIALS, no GCP Connection
    if args.migrate_from and "mysql" not in engines:
        ap.error("--migrate-from runs beside the MySQL source leg: pass --sources with mysql")
    if args.state != "gate" and not engines:
        ap.error("--state compose-postgres places the source legs' state: pass --sources")
    if engines and "bigquery" not in warehouses:
        ap.error("--sources loads into BigQuery: pass --warehouses with bigquery")
    dests = tuple(d for d in args.destinations.split(",") if d)
    if set(dests) - set(destinations.DESTINATIONS):
        ap.error(
            f"unknown destination(s) {sorted(set(dests) - set(destinations.DESTINATIONS))}; known: {list(destinations.DESTINATIONS)}"
        )
    if args.workers > 1 and (warehouses or engines or args.destinations or args.sabotage):
        ap.error("--workers 2 is a leg of its own: no --warehouses, --sources, --destinations or --sabotage beside it")
    tag = args.tag or time.strftime("%m%d%H%M") + secrets.token_hex(2)
    if not TAG_SHAPE.fullmatch(tag):
        ap.error(f"--tag {tag!r} is not 8 digits and 4 lower-case hex digits (the shape the cloud sweep recognises)")
    if not warehouses and (args.connections != "env" or args.gcp_auth != "key"):
        ap.error("--connections gsm and --gcp-auth adc are legs of the warehouse leg: pass --warehouses")
    cloud = None
    if warehouses or args.workers > 1:
        keyfile = args.gcp_keyfile or Path.home() / ".config/gcloud/application_default_credentials.json"
        cloud = Cloud(
            keyfile,
            args.gcs_bucket,
            args.bq_project,
            args.bq_location,
            args.sf_connection,
            args.sf_warehouse,
            args.sf_database,
            args.sf_integration,
            tag,
            gcp_auth=args.gcp_auth,
            connections_from=args.connections,
            gsm_reader=args.gsm_reader.format(project=args.bq_project),
        )
        print(
            f"{'warehouse leg ' + str(list(warehouses)) if warehouses else 'multi-host leg'}, tag {tag}, connections {args.connections}, gcp auth {args.gcp_auth}; "
            f"swept: {(sweep(cloud) if warehouses else []) + sources.sweep(cloud)}",
            flush=True,
        )
    work = args.work or Path(tempfile.mkdtemp(prefix="rivet-airflow-gate-"))
    work.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, str]] = []
    for version in versions:
        gsm = cloud is not None and cloud.connections_from == "gsm"
        stand = Stand(
            version,
            args.rivet_bin.resolve(),
            work.resolve(),
            warehouses,
            tag=tag,
            secrets_backend=cloud.secrets_backend(version) if gsm else None,
            # The secrets backend and a keyless gate_gcp both run as the containers' identity: this file, read-only.
            adc=cloud.keyfile.resolve() if cloud and (gsm or cloud.gcp_auth == "adc") else None,
            gcp_key_file=cloud.keyfile.resolve() if cloud and cloud.gcp_auth == "env-file" else None,
            client_override=args.client_override.resolve() if args.client_override else None,
            sources=sources.engines(engines),
            destinations=tuple(destinations.legs(dests)),
            workers=args.workers,
            state_place=args.state,
            migrate_from=args.migrate_from.resolve() if args.migrate_from else None,
        )
        leg = Leg(cloud, version, TABLES, warehouses) if cloud and warehouses else None
        hosts = multihost.MultiHost(stand, Verdicts(version), cloud) if cloud and args.workers > 1 else None
        src_legs = sources.legs(cloud, version, engines)
        mig_legs = migrate.legs(src_legs, args.migrate_from)
        dest_legs = list(stand.destinations)
        verdicts = hosts.v if hosts else Verdicts(version)
        try:
            if hosts:
                stand.extra_connections = {"gate_gcp": gcp_connection(cloud)}
            elif cloud:
                stand.extra_connections = verdicts.guard("connections", connections, cloud, stand.secrets.clickhouse) or {}
            if gsm:
                verdicts.guard("gsm-publish", publish_connections, cloud, version, stand.connections())
            if not verdicts.rows:
                verdicts.guard("stand-up", stand.up)
            if not verdicts.rows:
                if hosts:
                    verdicts.guard("multihost", hosts.play)
                else:
                    verdicts.guard("scenario", play, stand, verdicts, args.sabotage, leg, src_legs, dest_legs, mig_legs)
                verdicts.guard("proofs", proofs.prove, stand, verdicts, cloud if warehouses else None)
        finally:
            if not args.keep:
                verdicts.guard("teardown", stand.down)
                if leg:
                    verdicts.guard("cloud-cleanup", leg.cleanup)
                if hosts:
                    verdicts.guard("cloud-cleanup/multihost", hosts.cleanup)
                for src in src_legs:
                    verdicts.guard(f"cloud-cleanup/{src.engine.name}", src.cleanup)
                for mig in mig_legs:
                    verdicts.guard("cloud-cleanup/migrate", mig.cleanup)
                if gsm:
                    verdicts.guard("gsm-cleanup", delete_connections, cloud, version)
        rows += verdicts.rows
    if not args.keep and args.work is None:
        shutil.rmtree(work, ignore_errors=True)
    if args.verdicts:
        args.verdicts.write_text("".join(json.dumps(r) + "\n" for r in rows))
    line, ok = summary(rows, len(versions))
    print(line)
    return 0 if ok else 1


def summary(rows: list[dict[str, str]], stacks: int) -> tuple[str, bool]:
    """The verdict line, and whether the run passed: every cell PASS or a known red (XFAIL); an XPASS fails it."""
    failed = sum(r["status"] not in ("PASS", "XFAIL") for r in rows)
    xfail = sum(r["status"] == "XFAIL" for r in rows)
    known = f", {xfail} XFAIL" if xfail else ""
    line = f"airflow gate: {len(rows) - failed - xfail} PASS, {failed} FAIL{known} over {stacks} stack(s)"
    return line, bool(rows) and not failed


if __name__ == "__main__":
    sys.exit(main())
