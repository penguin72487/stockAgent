#!/usr/bin/env python3
"""Bounded, local-only FinLab audit; never authenticates or downloads.

Reconcile the canonical workload with its dataset rows, retain current service
identities, and time the existing public projection. Receipts remain read-only.
"""
from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.download_finlab_history import _atomic_json  # noqa: E402
from scripts.finlab_workload import read_json  # noqa: E402
from stockagent.live.finlab_dashboard import build_finlab_public_status  # noqa: E402

UNITS = (
    "stockagent-finlab-local-refresh.service", "stockagent-finlab-local-refresh.timer",
    "stockagent-finlab-quota-snapshot.timer", "stockagent-public-dashboards.service",
    "stockagent-finmind-free.service", "stockagent-finmind-complement.service",
    "stockagent-finmind-sponsor.service", "stockagent-shioaji-taifex-bidask.service",
    "stockagent-tw-day-trade-simulation.service", "stockagent-tw-overnight-simulation.service",
)


def write_inventory(path: Path, public: dict, workload: dict) -> None:
    """Export receipt-backed grains; a source date is not a release timestamp."""
    catalog = {row["key"]: row for row in public["datasets"]}
    fields = ("key", "category", "stage", "state", "downloaded", "needs_refresh", "blocked_reason",
              "first_observed", "last_observed", "stored_rows", "record_unit", "record_count",
              "local_parquet_bytes", "source_weight_bytes", "expected_next_payload_bytes",
              "source_checked_at_utc", "next_source_check_at_utc", "retry_at_utc", "time_basis")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8-sig", newline="", dir=path.parent,
                                     prefix=".finlab-inventory-", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in sorted(workload["datasets"], key=lambda item: item["key"]):
            source = catalog.get(row["key"], {})
            writer.writerow({**{key: row.get(key) for key in fields}, "stage": row.get("queue_role"),
                             "state": source.get("state"), "first_observed": source.get("first"),
                             "last_observed": source.get("last"), "source_weight_bytes": row.get("transfer_bytes"),
                             "expected_next_payload_bytes": row.get("expected_payload_bytes")})
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def audit(root: Path, *, baseline: Path | None = None, inventory: Path | None = None) -> dict:
    now = datetime.now(UTC)
    quota = read_json(root / "artifacts/live/finlab/quota_latest.json")
    workload = read_json(root / "artifacts/live/finlab/workload_latest.json")
    rows = workload.get("datasets", [])
    started = time.perf_counter()
    public = build_finlab_public_status(root, now=now)
    elapsed = time.perf_counter() - started
    if inventory:
        write_inventory(inventory, public, workload)
    checks = {
        "unique_workload_keys": len({r["key"] for r in rows}) == len(rows),
        "general_keys_reconciled": workload.get("general_keys") == len(rows),
        "pending_reconciled": workload.get("refresh_pending") == sum(
            r["needs_refresh"] and not r["blocked_reason"] for r in rows),
        "blocked_reconciled": set(workload.get("blocked_keys", [])) == {
            r["key"] for r in rows if r["blocked_reason"]},
        "stages_reconciled": all(s["total_keys"] == sum(r["queue_role"] == s["id"] for r in rows)
                                 for s in workload.get("stages", []) if s["id"] != "tick"),
        "public_read_only": public.get("read_only") is True and public.get("production_control_possible") is False,
    }
    services = {}
    for unit in UNITS:
        result = subprocess.run(["systemctl", "show", unit, "--property=ActiveState,SubState,MainPID,"
                                 "InvocationID,NRestarts,Result,NextElapseUSecRealtime,LastTriggerUSec,"
                                 "TimersMonotonic,ExecCondition"],
                                capture_output=True, text=True, timeout=5)
        services[unit] = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    protected = None
    if baseline:
        before = read_json(baseline).get("services", {})
        protected = {unit: all(services[unit].get(field) == before.get(unit, {}).get(field)
                              for field in ("InvocationID", "MainPID", "NRestarts"))
                     for unit in UNITS[4:]}
        checks["protected_runtime_identities_unchanged"] = all(protected.values())
        checks["minute_timer_deployed"] = "OnUnitInactiveUSec=1min" in services[
            "stockagent-finlab-local-refresh.timer"].get("TimersMonotonic", "")
        checks["local_dispatch_guard_deployed"] = "run_finlab_refresh_due.sh" in services[
            "stockagent-finlab-local-refresh.service"].get("ExecCondition", "")
    market = read_json(root / "data_finlab/intraday/market_status.json")
    return {
        "schema_version": 1, "observed_at_utc": now.isoformat(), "provider_calls": 0,
        "checks": checks, "accepted": all(checks.values()), "services": services,
        "public_build_seconds": round(elapsed, 4), "public_health": public["health"],
        "protected_units": protected, "inventory_path": str(inventory) if inventory else None,
        "quota": {k: quota.get(k) for k in ("observed_at_utc", "used_mb", "limit_mb", "remaining_mb", "reset_at_utc")},
        "workload": {k: workload.get(k) for k in ("contract_version", "generated_at_utc", "state",
                     "catalog_keys", "general_keys", "refresh_pending", "blocked_keys", "transfer",
                     "records", "stages", "scenarios", "tick", "measurement")},
        "public_execution": public.get("execution"),
        "run": read_json(root / "data_finlab/runs/latest.json"),
        "intraday": {k: v for k, v in market.items() if k != "symbols"},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--inventory", type=Path)
    args = parser.parse_args()
    result = audit(args.root, baseline=args.baseline, inventory=args.inventory)
    _atomic_json(args.output, result)
    print(json.dumps({"accepted": result["accepted"], "checks": result["checks"],
                      "public_build_seconds": result["public_build_seconds"],
                      "provider_calls": 0, "output": str(args.output)}, ensure_ascii=False))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
