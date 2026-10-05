#!/usr/bin/env python3
"""Cheap local FinLab timer admission; no SDK, login, network or dataset scan.

The minute timer is a dispatch clock, not a mandate to run a heavy sweep. Local
fresh workload and quota receipts suppress idle processes; uncertain/stale
evidence falls through to the canonical collector for recovery.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.finlab_acquisition_contract import (  # noqa: E402
    TAIPEI, WORKLOAD_CONTRACT_VERSION, quota_cycle_start, utc_time,
)


def read(path: Path) -> dict:
    try:
        if path.stat().st_size > 16 * 1024**2:
            return {}
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def refresh_due(root: Path, *, now: datetime) -> tuple[bool, str]:
    local = now.astimezone(TAIPEI)
    if local.hour == 8 and local.minute < 20:
        return True, "reset_confirmation_window"
    from stockagent.data.finlab_gap_priority import pending_gap_requests
    try:
        if pending_gap_requests(root / "data_finlab", now=now):
            return True, "owner_gap_recheck_due"
    except (OSError, ValueError, TypeError):
        return True, "gap_priority_unverified_recovery"
    workload = read(root / "artifacts/live/finlab/workload_latest.json")
    observed = utc_time(workload.get("generated_at_utc"))
    rows = workload.get("datasets")
    if (workload.get("contract_version") not in {3, WORKLOAD_CONTRACT_VERSION}
            or workload.get("state") != "available" or not observed
            or not timedelta(0) <= now - observed <= timedelta(minutes=3)
            or not isinstance(rows, list) or not rows or len(rows) != workload.get("general_keys")):
        return True, "workload_unverified_recovery"
    for row in rows:
        if not isinstance(row, dict):
            return True, "workload_unverified_recovery"
        due = utc_time(row.get("next_source_check_at_utc"))
        if row.get("needs_refresh") or (due and due <= now):
            retry = utc_time(row.get("retry_at_utc"))
            if retry is None or retry <= now:
                return True, "general_work_due"
    quota = read(root / "artifacts/live/finlab/quota_latest.json")
    quota_at = utc_time(quota.get("observed_at_utc"))
    remaining = quota.get("remaining_mb")
    if (not quota_at or not timedelta(0) <= now - quota_at <= timedelta(minutes=3)
            or isinstance(remaining, bool) or not isinstance(remaining, (int, float))
            or not math.isfinite(remaining) or remaining < 0):
        return True, "quota_unverified_recovery"
    core = read(root / "data_finlab/core_acquisition_status.json")
    reserve = core.get("scheduled_reserve_mb", 500)
    if isinstance(reserve, bool) or not isinstance(reserve, (int, float)) or not math.isfinite(reserve):
        reserve = 500
    market = read(root / "data_finlab/intraday/market_status.json")
    kinds = market.get("by_kind")
    tick = kinds.get("tw_tick", {}) if isinstance(kinds, dict) else {}
    tick = tick if isinstance(tick, dict) else {}
    partitions = tick.get("remaining_partitions", 1)
    if type(partitions) is not int or partitions < 0:
        partitions = 1  # unknown scope is not zero remaining work
    checked = utc_time(market.get("observed_at_utc"))
    # A fully searched frontier can only advance after another source cycle;
    # keep unknown history incomplete without rediscovering it every minute.
    exhausted = (market.get("state") == "frontiers_scanned; retries_due_next_cycle"
                 and checked and checked >= quota_cycle_start(now))
    if (core.get("supplemental_allowed") is True and not exhausted
            and remaining > max(50, reserve) and partitions > 0):
        return True, "residual_tick_capacity"
    return False, "nothing_actionable_now"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    allowed, reason = refresh_due(args.root, now=datetime.now(UTC))
    print(json.dumps({"allowed": allowed, "reason": reason, "provider_calls": 0}))
    return 0 if allowed else 1  # systemd ExecCondition skips, not fails.


if __name__ == "__main__":
    raise SystemExit(main())
