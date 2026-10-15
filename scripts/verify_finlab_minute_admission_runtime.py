"""Read-only native FinLab minute-priority adoption and bounded source readback.

An active unit or matching code file is not enough: require a fresh decision
that the formal acquisition owner actually executed. No provider calls, process
signals, service restarts or source mutations are performed.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from contextlib import closing

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.audit_finlab_runtime import _verify_partition_rows
from scripts.benchmark_finlab_effective_partitions import file_sha
from stockagent.data.finlab_acquisition_contract import process_owner, process_owner_alive

UNIT = "stockagent-finlab-local-refresh.service"
CODE = ROOT / "scripts/download_finlab_market_intraday.py"


def unit_identity(unit: str) -> dict:
    raw = subprocess.run(["systemctl", "show", unit, "-p", "ActiveState", "-p", "InvocationID",
                          "-p", "MainPID", "-p", "ControlGroup"], check=True,
                         capture_output=True, text=True, timeout=8).stdout
    return dict(line.split("=", 1) for line in raw.splitlines())


def executed_admission(progress: dict, *, now: datetime) -> bool:
    observation = progress.get("minute_priority_admission")
    if observation is None:
        return False
    if (not isinstance(observation, dict) or observation.get("engine") != "minute_priority_only_v1"
            or observation.get("contract_version") != 1
            or type(observation.get("checks")) is not int or observation["checks"] < 1
            or observation.get("complete_inventory_generated") is not False
            or not isinstance(observation.get("decision"), dict)
            or type(observation["decision"].get("tick_residual_allowed")) is not bool):
        raise ValueError("invalid executed minute-priority observation")
    elapsed = observation.get("wall_seconds")
    if type(elapsed) not in (float, int) or not math.isfinite(elapsed) or elapsed < 0:
        raise ValueError("invalid measured admission duration")
    for field, payload in (("checked_at_utc", observation), ("observed_at_utc", progress)):
        checked = datetime.fromisoformat(payload[field])
        if checked.tzinfo is None or not 0 <= (now-checked).total_seconds() <= 360:
            raise ValueError("stale or invalid native admission clock")
    return True


def native_identity(progress: dict) -> dict:
    if progress.get("kind") != "tw_tick":
        raise ValueError("minute-priority admission requires the actual tick owner")
    owner = progress.get("owner")
    now = datetime.now(UTC)
    if not process_owner_alive(owner, started_at_utc=progress.get("run_started_at_utc"), now=now):
        raise ValueError("native producer generation unavailable")
    proc = Path(f"/proc/{owner['pid']}")
    unit = unit_identity(UNIT)
    cmd = [part.decode() for part in (proc / "cmdline").read_bytes().split(b"\0") if part]
    if (unit["ActiveState"] not in ("active", "activating") or not unit["InvocationID"]
            or unit["MainPID"] == "0" or not unit["ControlGroup"]
            or unit["ControlGroup"] not in (proc / "cgroup").read_text()
            or (proc / "exe").resolve() != Path(sys.executable).resolve()
            or (proc / "cwd").resolve() != ROOT or len(cmd) < 2 or Path(cmd[1]).resolve() != CODE):
        raise ValueError("not the canonical FinLab acquisition owner")
    if datetime.fromisoformat(progress["run_started_at_utc"]).timestamp() < CODE.stat().st_mtime:
        raise ValueError("native owner predates the candidate source generation")
    return {"owner": process_owner(owner["pid"]), "unit": unit, "executable": str((proc / "exe").resolve()),
            "cwd": str((proc / "cwd").resolve()), "cmdline": cmd}


def verify(source: Path, benchmark: Path, protected: Path, *, sample_seconds: float = 5) -> dict:
    if not 1 <= sample_seconds <= 20:
        raise ValueError("bounded 1-20 second observation required")
    source = source.resolve()
    proof = json.loads(benchmark.read_text())
    expected = proof.get("code_identity", {}).get("scripts/download_finlab_market_intraday.py")
    if (proof.get("state") != "accepted_bounded_comparison" or proof.get("full_outputs_equal") is not True
            or proof.get("provider_calls") != 0 or proof.get("native_source_writes") != 0
            or expected != file_sha(CODE)):
        raise ValueError("matching complete-index comparison required")
    prior = json.loads(protected.read_text())["protected"]
    before = json.loads((source / "intraday/active_run.json").read_text())
    if not executed_admission(before, now=datetime.now(UTC)):
        return {"contract": "finlab_minute_admission_native_observation_v1", "state": "awaiting_native_adoption",
                "observed_at_utc": datetime.now(UTC).isoformat(), "owner": before.get("owner"),
                "provider_calls_from_probe": 0, "service_restarted_by_probe": False}
    identity = native_identity(before)
    controls = {unit: unit_identity(unit) for unit in prior}
    for unit, original in prior.items():
        if any(controls[unit].get(key) != original.get(key) for key in ("InvocationID", "MainPID", "ActiveState")):
            raise ValueError("protected service owner changed")
    with closing(sqlite3.connect((source / "intraday/market_index.sqlite3").as_uri()+"?mode=ro", uri=True)) as connection:
        rows = connection.execute("SELECT symbol,trade_date,rows,parquet_bytes,checked_at_utc FROM partitions "
            "WHERE kind='tw_minute' AND status='downloaded_unverified_for_pit' "
            "ORDER BY checked_at_utc DESC,symbol LIMIT 3").fetchall()
    readbacks = _verify_partition_rows(source, rows)
    if len(readbacks) != 3 or not all(row["accepted"] for row in readbacks):
        raise ValueError("bounded independent source readback failed")
    time.sleep(sample_seconds)
    after = json.loads((source / "intraday/active_run.json").read_text())
    if (not executed_admission(after, now=datetime.now(UTC)) or native_identity(after) != identity
            or file_sha(CODE) != expected or after["attempted"] < before["attempted"]):
        raise ValueError("native generation or progress changed during readback")
    for unit, original in controls.items():
        if unit_identity(unit) != original:
            raise ValueError("protected service changed during readback")
    return {"contract": "finlab_minute_admission_native_observation_v1", "state": "accepted_native_adoption",
        "observed_at_utc": datetime.now(UTC).isoformat(), "benchmark": str(benchmark.relative_to(ROOT)),
        "code_sha256": expected, "native_identity": identity, "before": before, "after": after,
        "recent_verified_minute_partitions": readbacks, "protected": controls,
        "same_owner_reobserved": True, "source_output_parity": proof["full_outputs_equal"],
        "provider_calls_from_probe": 0, "service_restarted_by_probe": False,
        "source_writes": 0, "whole_download_speedup_proven": False, "full_history_complete_claim": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument("--benchmark", required=True, type=Path)
    parser.add_argument("--protected", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.absolute()
    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to(ROOT / "artifacts/operations"):
        parser.error("fresh engineering receipt under artifacts/operations required")
    result = verify(args.source_root, args.benchmark.resolve(), args.protected.resolve())
    atomic_write_json(output, result, durable=True)
    print(json.dumps({key: result[key] for key in ("state", "provider_calls_from_probe", "service_restarted_by_probe")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
