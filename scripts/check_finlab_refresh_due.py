#!/usr/bin/env python3
"""Cheap local FinLab timer admission; no SDK, login, network or dataset scan.

The minute timer is a dispatch clock, not a mandate to run a heavy sweep. Local
fresh workload and quota receipts suppress idle processes; uncertain/stale
evidence falls through to the canonical collector for recovery.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import fcntl
import json
import math
import os
from pathlib import Path
import shutil
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.finlab_acquisition_contract import (  # noqa: E402
    INTRADAY_MINIMUM_FREE_GB, TAIPEI, WORKLOAD_CONTRACT_VERSION, quota_cycle_start, utc_time,
)
from stockagent.data.finlab_quota_policy import load_policy, quota_admission  # noqa: E402


def read(path: Path) -> dict:
    try:
        if path.stat().st_size > 16 * 1024**2:
            return {}
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def research_owner_busy(root: Path) -> bool | None:
    """Probe the existing writer lease, never create/unlink/release its owner.

    An unknown or redirected inode is not evidence of an idle/blocked producer.
    The actual builder still acquires this lease again before touching inputs.
    """
    try:
        public = (root / "data_tw_public").resolve(strict=True)
        lock = public.parent / ".locks/tw-public-refresh.lock"
        if lock.resolve(strict=True) != lock:
            return None
        fd = os.open(lock, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                return None
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        finally:
            os.close(fd)
    except (OSError, RuntimeError):
        return None


def refresh_due(root: Path, *, now: datetime) -> tuple[bool, str]:
    try:
        policy = load_policy(root / "configs/finlab_quota_policy.json")
    except (OSError, ValueError):
        return True, "quota_policy_unverified_recovery"  # Collector fails closed with an explicit error.
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
    market = read(root / "data_finlab/intraday/market_status.json")
    if market.get("state") == "disk_reserve_reached":
        try:
            disk_ready = intraday_disk_admission(root)["allowed"]
        except OSError:
            disk_ready = True  # Unknown evidence falls through to the original worker.
        if not disk_ready and research_owner_busy(root) is True:
            # Both independent branches are currently excluded by their real
            # resources. Recheck statvfs/flock next minute; no fixed cooldown,
            # provider call, SDK import, partition scan or false completion.
            return False, "intraday_disk_full_research_owner_busy"
    # A provider's shared cooldown applies to intraday work, not unrelated
    # general releases. Avoid starting an idle process pool every timer tick.
    retry = utc_time(read(root / "data_finlab/intraday/source_backoff.json").get("retry_at_utc"))
    if retry is not None and now < retry:
        return False, "intraday_source_cooldown"
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
    kinds = market.get("by_kind")
    tick = kinds.get("tw_tick", {}) if isinstance(kinds, dict) else {}
    tick = tick if isinstance(tick, dict) else {}
    minute_first = (market.get("acquisition_mode") == "minute_first_tick_residual"
                    or type(core.get("minute_residual_allowed")) is bool)
    minute = kinds.get("tw_minute", {}) if isinstance(kinds, dict) else {}
    minute = minute if isinstance(minute, dict) else {}
    def remaining_work(item):
        count = item.get("remaining_partitions", 1)
        return count if type(count) is int and count >= 0 else 1
    checked = utc_time(market.get("observed_at_utc"))
    # A fully searched frontier can only advance after another source cycle;
    # keep unknown history incomplete without rediscovering it every minute.
    exhausted = (not minute_first and market.get("state") == "frontiers_scanned; retries_due_next_cycle"
                 and checked and checked >= quota_cycle_start(now))
    minute_wait = False
    if minute_first:
        priority = market.get("minute_priority", {})
        priority = priority if isinstance(priority, dict) else {}
        next_check = utc_time(priority.get("next_source_check_at_utc"))
        minute_wait = (priority.get("actionable_known_window_partitions", priority.get("pending_known_window_partitions")) == 0
                       and priority.get("due_publication_rechecks") == 0
                       and next_check is not None and now < next_check)
        if minute_wait and core.get("supplemental_allowed") is not True:
            return False, "minute_source_wait_tick_general_debt"
    admitted = core.get("minute_residual_allowed", core.get("supplemental_allowed")) is True
    minute_policy = quota_admission(kind="tw_minute", now=now, policy=policy)
    if minute_first and admitted and not minute_wait and remaining_work(minute) > 0:
        if remaining > minute_policy["effective_reserve_mb"]:
            return True, "minute_pre_reset_drain" if minute_policy["mode"] == "pre_reset_drain" else "minute_first_residual_capacity"
        return False, "minute_quota_floor_reached"
    tick_policy = quota_admission(kind="tw_tick", now=now, scheduled_reserve_mb=reserve, policy=policy)
    if (core.get("supplemental_allowed") is True and not exhausted and remaining_work(tick) > 0
            and remaining > tick_policy["effective_reserve_mb"]):
        return True, "tick_pre_reset_drain" if tick_policy["mode"] == "pre_reset_drain" else "residual_tick_capacity"
    return False, "nothing_actionable_now"


def intraday_disk_admission(root: Path) -> dict:
    """One statvfs, no inventory/SDK; collector still checks every boundary.

    This preflight belongs after general/research work, not in the overall
    ExecCondition: low intraday headroom must not suppress an independently
    recoverable general download or a pending verified research publication.
    """
    available = shutil.disk_usage(root / "data_finlab").free
    required = int(INTRADAY_MINIMUM_FREE_GB * 1024**3)
    allowed = available >= required
    return {"allowed": allowed, "reason": "intraday_disk_ready" if allowed else "disk_reserve_reached",
            "free_bytes": available, "minimum_free_bytes": required, "provider_calls": 0}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--intraday-disk-only", action="store_true",
                        help="preflight only the shared intraday disk floor, after independent research work")
    args = parser.parse_args(argv)
    if args.intraday_disk_only:
        try:
            admission = intraday_disk_admission(args.root)
        except OSError:
            print(json.dumps({"allowed": False, "reason": "intraday_disk_probe_failed", "provider_calls": 0}))
            return 2
        print(json.dumps(admission))
        return 0 if admission["allowed"] else 1
    allowed, reason = refresh_due(args.root, now=datetime.now(UTC))
    print(json.dumps({"allowed": allowed, "reason": reason, "provider_calls": 0}))
    return 0 if allowed else 1  # systemd ExecCondition skips, not fails.


if __name__ == "__main__":
    raise SystemExit(main())
