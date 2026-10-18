"""The warehouse leg grades what a second client reads back, refuses what it may not skip, and cleans up provably.

Every oracle passes on a faithful read-back in each client's own value shapes and fails on each mutant of it.
"""

from __future__ import annotations

import copy
import os
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from gate import scenario, warehouse
from gate.scenario import TABLES, Verdicts
from gate.stand import Stand
from gate.warehouse import Cloud, Leg

REPO = Path(__file__).resolve().parents[2]
TAG = "1016120000ab"
T0 = datetime(2026, 10, 16, 12, 0, 0, 123456, tzinfo=UTC)
CLOUD = Cloud(Path("/nonexistent/key.json"), "bucket", "proj", "US", "rivet", "WH", "DB", "INT", TAG)

SOURCE = {
    "gate_customers": [
        {"id": 1, "name": "customer 1", "tier": "pro", "updated_at": T0},
        {"id": 2, "name": "customer 2", "tier": "team", "updated_at": T0 + timedelta(seconds=1)},
    ],
    "gate_orders": [
        {"id": 1, "customer_id": 2, "amount": Decimal("1.37"), "status": "new", "updated_at": T0},
        {
            "id": 2,
            "customer_id": 3,
            "amount": Decimal("13.70"),
            "status": "shipped",
            "updated_at": T0 + timedelta(microseconds=1),
        },
    ],
}
#: A key the source has deleted: the batch view keeps it (an incremental extract cannot see a delete), CDC flags it.
GONE = {"id": 9, "customer_id": 1, "amount": Decimal("2.00"), "status": "new", "updated_at": T0}


def shape(wh: str, row: dict) -> dict:
    """A row in the value shapes the warehouse's own client hands back."""
    if wh == "bigquery":  # google-cloud-bigquery: aware UTC datetime, Decimal for NUMERIC
        return dict(row)
    if wh == "snowflake":  # snowflake-connector-python: upper-case names, naive UTC datetime for TIMESTAMP_NTZ
        out = {k.upper(): v for k, v in row.items()}
        if isinstance(out.get("UPDATED_AT"), datetime):
            out["UPDATED_AT"] = out["UPDATED_AT"].astimezone(UTC).replace(tzinfo=None)
        return out
    # ClickHouse JSONEachRow: 64-bit integers quoted, ISO timestamps, booleans as JSON booleans
    out = {k: (str(v) if isinstance(v, int) and not isinstance(v, bool) else v) for k, v in row.items()}
    if isinstance(row.get("updated_at"), datetime):
        out["updated_at"] = row["updated_at"].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return out


def faithful(deleted: dict[str, int]) -> tuple[dict, dict]:
    """What a correct load leaves in each warehouse: (rows by (wh, object), objects by wh)."""
    rows, objects = {}, {}
    for wh in warehouse.WAREHOUSES:
        names = set()
        for table, (short, _) in TABLES.items():
            batch = [*SOURCE[table], *([GONE] * deleted.get(table, 0))]
            rows[(wh, f"{wh}_{short}")] = [shape(wh, r) for r in batch]
            cdc = [{**r, "__is_deleted": False} for r in SOURCE[table]] + (
                [{**GONE, "__is_deleted": True}] if table == "gate_orders" else []
            )
            rows[(wh, table)] = [shape(wh, r) for r in cdc]
            names |= {f"{wh}_{short}", table, f"{table}__changes"}
            if wh != "bigquery":
                names.add(f"{wh}_{short}__changes")
        objects[wh] = names
    return rows, objects


def grade(monkeypatch, rows: dict, objects: dict, deleted: dict[str, int]) -> list[dict]:
    """Leg.grade over a fake stand whose warehouses hold `rows` / `objects`; the verdict rows."""
    monkeypatch.setattr(Leg, "inventory", lambda self, stand: {}, raising=False)
    monkeypatch.setattr(Leg, "source_rows", lambda self, stand: copy.deepcopy(SOURCE), raising=False)
    monkeypatch.setattr(Leg, "read", lambda self, stand, wh, obj: copy.deepcopy(rows[(wh, obj)]), raising=False)
    monkeypatch.setattr(Leg, "objects", lambda self, stand, wh: set(objects[wh]), raising=False)
    v = Verdicts("3.3.2")
    Leg(CLOUD, "3.3.2", TABLES).grade(SimpleNamespace(), v, "delta", deleted)
    return v.rows


DELETED = {"gate_customers": 0, "gate_orders": 1}


def failed(rows: list[dict]) -> dict[str, str]:
    return {r["cell"]: r["detail"] for r in rows if r["status"] == "FAIL"}


def test_d03_grade_reads_every_warehouse_back_and_passes_a_faithful_load(monkeypatch) -> None:
    """D03: Leg.grade printed an inventory and recorded no verdict; now one PASS cell per warehouse, mode and table."""
    rows, objects = faithful(DELETED)
    verdicts = grade(monkeypatch, rows, objects, DELETED)
    cells = {r["cell"] for r in verdicts if r["status"] == "PASS"}
    assert failed(verdicts) == {}
    for wh in warehouse.WAREHOUSES:
        for table, (short, _) in TABLES.items():
            assert f"delta/wh/{wh}/batch/{short}/current-equals-source" in cells
            assert f"delta/wh/{wh}/cdc/{table}/live-equals-source" in cells
    assert {"delta/wh/bigquery/batch/customers/compacted", "delta/wh/bigquery/batch/orders/compacted"} <= cells


def mutate(rows: dict, key: tuple[str, str], fn) -> dict:
    out = copy.deepcopy(rows)
    out[key] = fn(out[key])
    return out


def test_d03_every_warehouse_mutant_is_a_fail(monkeypatch) -> None:
    """D03: one wrong value, one missing row, one shifted instant, an unflagged delete, an unexplained extra key."""
    rows, objects = faithful(DELETED)

    def cent_off(rs):
        rs[0]["AMOUNT"] = rs[0]["AMOUNT"] + Decimal("0.01")
        return rs

    def one_microsecond(rs):
        rs[1]["updated_at"] = "2026-10-16T12:00:00.123458Z"
        return rs

    def unflag(rs):
        rs[-1]["__is_deleted"] = False
        return rs

    mutants = {
        "one wrong value": (
            (("snowflake", "snowflake_orders"), cent_off),
            "delta/wh/snowflake/batch/orders/current-equals-source",
        ),
        "one missing row": (
            (("clickhouse", "gate_customers"), lambda rs: rs[:-1]),
            "delta/wh/clickhouse/cdc/gate_customers/live-equals-source",
        ),
        "a shifted instant": (
            (("clickhouse", "clickhouse_orders"), one_microsecond),
            "delta/wh/clickhouse/batch/orders/current-equals-source",
        ),
        "a delete not flagged": ((("bigquery", "gate_orders"), unflag), "delta/wh/bigquery/cdc/gate_orders/live-equals-source"),
        "a key the source never deleted": (
            (("bigquery", "bigquery_customers"), lambda rs: [*rs, dict(rs[0], id=77)]),
            "delta/wh/bigquery/batch/customers/current-equals-source",
        ),
        "a row twice": (
            (("bigquery", "bigquery_customers"), lambda rs: [*rs, rs[0]]),
            "delta/wh/bigquery/batch/customers/current-equals-source",
        ),
        "an empty warehouse": (
            (("snowflake", "gate_orders"), lambda rs: []),
            "delta/wh/snowflake/cdc/gate_orders/live-equals-source",
        ),
    }
    for name, ((key, fn), cell) in mutants.items():
        assert list(failed(grade(monkeypatch, mutate(rows, key, fn), objects, DELETED))) == [cell], name


def test_d03_a_skipped_bigquery_compaction_is_a_fail(monkeypatch) -> None:
    """D03: after `compact` the BigQuery buffer `<table>__changes` is gone and the base equals the source."""
    rows, objects = faithful(DELETED)
    objects["bigquery"].add("bigquery_orders__changes")
    assert list(failed(grade(monkeypatch, rows, objects, DELETED))) == ["delta/wh/bigquery/batch/orders/compacted"]


def test_d03_the_deleted_count_of_the_cycle_is_enforced(monkeypatch) -> None:
    rows, objects = faithful(DELETED)
    bad = failed(grade(monkeypatch, rows, objects, {"gate_customers": 0, "gate_orders": 2}))
    assert set(bad) == {f"delta/wh/{wh}/batch/orders/current-equals-source" for wh in warehouse.WAREHOUSES}


def test_d03_canonical_forms_of_one_value_agree_across_clients() -> None:
    kinds = warehouse.kinds_of(SOURCE["gate_orders"])
    want = warehouse.canon_row(SOURCE["gate_orders"][1], ["id", "customer_id", "amount", "status", "updated_at"], kinds)
    for wh in warehouse.WAREHOUSES:
        got = {k.lower(): v for k, v in shape(wh, SOURCE["gate_orders"][1]).items()}
        assert warehouse.canon_row(got, ["id", "customer_id", "amount", "status", "updated_at"], kinds) == want, wh
    assert warehouse.canon("money", 13.7) == warehouse.canon("money", Decimal("13.70")) == 1370
    assert warehouse.canon("money", Decimal("13.705")) != 1370, "a sub-cent value is not rounded away"
    assert warehouse.canon("int", "5") == 5 and warehouse.canon("int", Decimal("5.5")) != 5


# --- D16: a skip is allowed per DAG, and never for the BigQuery batch compaction ------------------------------------


class TaskStand:
    """A stand whose every warehouse DAG run succeeded, with the compact tasks skipped."""

    def tasks(self, dag: str, run_id: str) -> dict[str, dict]:
        leg = Leg(CLOUD, "3.3.2", TABLES)
        return {t: {"state": "skipped" if t.startswith("compact.") else "success"} for t in leg.tasks[dag]}


def test_d16_a_skipped_compaction_fails_only_the_bigquery_batch_dag() -> None:
    """D16: `may_skip=('compact.',)` for every DAG let a skipped BigQuery base_buffer compaction grade PASS."""
    leg = Leg(CLOUD, "3.3.2", TABLES)
    v = Verdicts("3.3.2")
    states = dict.fromkeys(leg.dags, "success")
    scenario.grade_runs(TaskStand(), v, "delta", "r", states, leg.tasks, may_skip=leg.may_skip)
    assert {r["cell"] for r in v.rows if r["status"] == "FAIL"} == {"delta/gate_wh_bigquery_batch/run"}
    assert {r["cell"] for r in v.rows if r["status"] == "PASS"} >= {
        "delta/gate_wh_bigquery_cdc/run",
        "delta/gate_wh_snowflake_batch/run",
        "delta/gate_wh_clickhouse_cdc/run",
    }


# --- D34: the cleanup lists again and raises on anything left ---------------------------------------------------------


class Clouds:
    """Fake `bq` / `snow` / `gcloud`: deletes succeed or are ignored, listings say what is still there."""

    def __init__(self, leftover: set[str]) -> None:
        self.leftover = leftover
        self.calls: list[list[str]] = []

    def sh(self, args, **kw):
        self.calls.append(list(args))
        text = " ".join(args)
        if args[0] == "bq" and " show " in f" {text} ":
            hit = "bq" in self.leftover
            return subprocess.CompletedProcess(args, 0 if hit else 2, "{}" if hit else "", "" if hit else "Not found: Dataset")
        if args[0] == "gcloud" and " ls " in f" {text} ":
            hit = "gcs" in self.leftover
            return subprocess.CompletedProcess(
                args,
                0 if hit else 1,
                "gs://bucket/airflow-gate/x.parquet\n" if hit else "",
                "" if hit else "One or more URLs matched no objects.",
            )
        if args[0] == "snow" and "SHOW SCHEMAS" in text:
            rows = (
                f'[{{"name": "{CLOUD.schema("3.3.2")}", "created_on": "2026-10-16T00:00:00Z"}}]'
                if "snowflake" in self.leftover
                else "[]"
            )
            return subprocess.CompletedProcess(args, 0, rows, "")
        return subprocess.CompletedProcess(args, 0, "[]", "")


@pytest.mark.parametrize("left", ["bq", "snowflake", "gcs"])
def test_d34_cleanup_raises_when_an_object_survives(monkeypatch, left: str) -> None:
    """D34: every delete ran with check=False and nothing listed afterwards, so a leftover still exited 0."""
    fake = Clouds({left})
    monkeypatch.setattr(warehouse, "sh", fake.sh)
    with pytest.raises(RuntimeError, match="left behind"):
        warehouse.cleanup(CLOUD, "3.3.2")


def test_d34_cleanup_passes_when_everything_is_gone(monkeypatch) -> None:
    fake = Clouds(set())
    monkeypatch.setattr(warehouse, "sh", fake.sh)
    warehouse.cleanup(CLOUD, "3.3.2")


# --- D35: the Snowflake sweep drops only schemas of the gate's own shape, quoted -------------------------------------


def test_d35_sweep_drops_only_the_gate_schema_shape_and_quotes_it(monkeypatch) -> None:
    """D35: `LIKE 'AFGATE\\_%'` loses its escape and ignores case, so any old AFGATE* schema was dropped, unquoted."""
    old = "2026-10-01T00:00:00Z"
    schemas = [
        {"name": "AFGATE_10010000beef_332".upper(), "created_on": old},
        {"name": "AFGATEX_PROD", "created_on": old},
        {"name": "AFGATE_SOMEONE_ELSES", "created_on": old},
        {"name": "afgate_10010000beef_332", "created_on": old},
        {"name": "AFGATE_10150000beef_2105".upper(), "created_on": "2999-01-01T00:00:00Z"},
    ]
    dropped: list[str] = []

    def fake_snow(cloud, sql, check=True):
        if sql.startswith("SHOW SCHEMAS"):
            return schemas
        dropped.append(sql)
        return []

    monkeypatch.setattr(warehouse, "_snow", fake_snow)
    monkeypatch.setattr(warehouse, "_bq", lambda *a, **k: "[]")
    monkeypatch.setattr(warehouse, "sh", lambda *a, **k: subprocess.CompletedProcess(a, 0, "", ""))
    removed = warehouse.sweep(CLOUD)
    assert dropped == ['DROP SCHEMA IF EXISTS DB."AFGATE_10010000BEEF_332"']
    assert removed == ["snowflake:AFGATE_10010000BEEF_332"]


@pytest.mark.parametrize(("error", "reported"), [("PERMISSION_DENIED", True), ("SERVICE_DISABLED", False)])
def test_a_secret_listing_that_fails_is_reported_unless_the_api_is_off(monkeypatch, error: str, reported: bool) -> None:
    """A failed `gcloud secrets list` (expired login, network) read as "Secret Manager not enabled": the sweep skipped
    the gate's secrets and returned as if clean."""

    def listing(cloud):
        raise RuntimeError(f"gcloud secrets list exited 1: {error}")

    monkeypatch.setattr(warehouse, "_snow", lambda *a, **k: [])
    monkeypatch.setattr(warehouse, "_bq", lambda *a, **k: "[]")
    monkeypatch.setattr(warehouse, "sh", lambda *a, **k: subprocess.CompletedProcess(a, 0, "", ""))
    monkeypatch.setattr(warehouse, "_secrets", listing)
    assert any(r.startswith("gsm:listing failed") for r in warehouse.sweep(CLOUD)) is reported


def test_d35_every_schema_the_gate_makes_has_the_swept_shape() -> None:
    assert warehouse.SCHEMA_SHAPE.fullmatch(CLOUD.schema("3.3.2")) and warehouse.SCHEMA_SHAPE.fullmatch(CLOUD.schema("2.10.5"))


# --- D36: two gate runs never share a compose project or an image tag ------------------------------------------------


def test_d36_two_runs_of_one_airflow_version_get_their_own_project_and_image(tmp_path: Path) -> None:
    """D36: the project was `rivet-gate-af<version>` for every run, so a second run's `down -v` tore down the first."""
    a = Stand("3.3.2", tmp_path / "rivet", tmp_path / "a")
    b = Stand("3.3.2", tmp_path / "rivet", tmp_path / "b")
    assert a.project != b.project
    assert a.image != b.image


def test_d36_the_run_tag_names_the_project_and_the_image(tmp_path: Path) -> None:
    s = Stand("2.10.5", tmp_path / "rivet", tmp_path, tag=TAG)
    assert TAG in s.project and s.image.endswith(TAG)


# --- D17: the Live gate workflow gets a binary that starts on Debian 12 -----------------------------------------------


def fetch_script() -> str:
    flow = yaml.safe_load((REPO / ".github/workflows/gate.yml").read_text())
    (step,) = [s for s in flow["jobs"]["gate"]["steps"] if s.get("name") == "Download the rivet Linux binary"]
    return step["run"]


def run_fetch(tmp_path: Path, assets: list[str], given: str = "") -> list[str]:
    """Run the workflow step with fake `gh` / `docker` on PATH; the commands they were called with."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "calls"
    listing = "\\n".join(assets)
    (bin_dir / "gh").write_text(
        "#!/bin/bash\n"
        f'echo "gh $*" >> {log}\n'
        'if [ "$1 $2" = "release view" ]; then\n'
        '  case "$*" in *tagName*) echo v0.31.0 ;; *) printf "' + listing + '\\n" ;; esac\n'
        "  exit 0\nfi\n"
        'if [ "$1 $2" = "release download" ]; then\n'
        '  pat=""; dir=""; while [ $# -gt 0 ]; do case "$1" in -p) pat=$2; shift ;; -D) dir=$2; shift ;; esac; shift; done\n'
        '  mkdir -p "$dir/x" && printf "#!/bin/sh\\n" > "$dir/x/rivet" && tar -czf "$dir/$pat" -C "$dir" x && rm -r "$dir/x"\n'
        "fi\n"
    )
    (bin_dir / "docker").write_text(
        f'#!/bin/bash\necho "docker $*" >> {log}\nif [ "$1" = cp ]; then printf "#!/bin/sh\\n" > "$3"; fi\n'
    )
    for f in bin_dir.iterdir():
        f.chmod(0o755)
    script = fetch_script().replace("${{ inputs.rivet }}", given)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "GH_TOKEN": "x"}
    subprocess.run(["bash", "-euo", "pipefail", "-c", script], cwd=tmp_path, env=env, check=True, capture_output=True, text=True)
    assert (tmp_path / "rivet-bin").exists()
    return log.read_text().splitlines()


@pytest.mark.skipif(shutil.which("bash") is None or shutil.which("tar") is None, reason="needs bash and tar")
def test_d17_the_glibc228_asset_is_downloaded_when_the_release_has_one(tmp_path: Path) -> None:
    """D17: the step downloaded the glibc 2.38/2.39 `-linux-gnu` asset, which cannot start in the Debian 12 image."""
    calls = run_fetch(
        tmp_path,
        ["rivet-v0.32.0-x86_64-unknown-linux-gnu.tar.gz", "rivet-v0.32.0-x86_64-unknown-linux-gnu-glibc2.28.tar.gz"],
        given="v0.32.0",
    )
    downloads = [c for c in calls if c.startswith("gh release download")]
    assert downloads and all("-p rivet-v0.32.0-x86_64-unknown-linux-gnu-glibc2.28.tar.gz" in c for c in downloads), calls
    assert not any(c.startswith("docker") for c in calls)


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
@pytest.mark.parametrize("given", ["0.31.0", "v0.4.0", ""])  # "": the latest release, v0.31.0 in the fake
def test_a_release_older_than_0_32_is_refused_before_anything_is_downloaded(tmp_path: Path, given: str) -> None:
    """The provider reads `apply --summary-output` and `already_applied` (rivet 0.32): an older binary is refused."""
    with pytest.raises(subprocess.CalledProcessError) as refused:
        run_fetch(tmp_path, ["rivet-v0.31.0-x86_64-unknown-linux-gnu.tar.gz"], given=given)
    assert "is older than the minimum v0.32.0" in refused.value.stdout
    calls = (tmp_path / "calls").read_text().splitlines() if (tmp_path / "calls").exists() else []
    assert not any(c.startswith(("gh release download", "docker")) for c in calls), calls


def test_a_uuid_and_bytes_compare_by_value_whatever_shape_the_warehouse_hands_back() -> None:
    import uuid

    u = uuid.UUID("5050beba-37a8-48ab-ab22-49de93372036")
    kinds = warehouse.kinds_of([{"u": u, "b": b"\x00\xff"}])
    assert kinds == {"u": "uuid", "b": "bytes"}
    for read_back in (u.bytes, str(u).upper(), str(u)):
        assert warehouse.canon("uuid", read_back) == str(u)
    assert warehouse.canon("uuid", u.bytes_le) != str(u), "SQL Server's mixed-endian byte order is a different UUID"
    assert warehouse.canon("bytes", b"\x00\xff") == warehouse.canon("bytes", memoryview(b"\x00\xff")) == "00ff"
