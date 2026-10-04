#!/usr/bin/env python3
"""Bounded, local-only FinLab audit; never authenticates or downloads.

Reconcile the canonical workload with its dataset rows, retain current service
identities, and time the existing public projection. Receipts remain read-only.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import subprocess
import sys
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


def audit(root: Path) -> dict:
    now = datetime.now(UTC)
    quota = read_json(root / "artifacts/live/finlab/quota_latest.json")
    workload = read_json(root / "artifacts/live/finlab/workload_latest.json")
    rows = workload.get("datasets", [])
    started = time.perf_counter()
    public = build_finlab_public_status(root, now=now)
    elapsed = time.perf_counter() - started
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
                                 "InvocationID,NRestarts,Result,NextElapseUSecRealtime,LastTriggerUSec"],
                                capture_output=True, text=True, timeout=5)
        services[unit] = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    market = read_json(root / "data_finlab/intraday/market_status.json")
    return {
        "schema_version": 1, "observed_at_utc": now.isoformat(), "provider_calls": 0,
        "checks": checks, "accepted": all(checks.values()), "services": services,
        "public_build_seconds": round(elapsed, 4), "public_health": public["health"],
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
    args = parser.parse_args()
    result = audit(args.root)
    _atomic_json(args.output, result)
    print(json.dumps({"accepted": result["accepted"], "checks": result["checks"],
                      "public_build_seconds": result["public_build_seconds"],
                      "provider_calls": 0, "output": str(args.output)}, ensure_ascii=False))
    return 0 if result["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
