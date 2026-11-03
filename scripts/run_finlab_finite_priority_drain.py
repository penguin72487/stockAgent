#!/usr/bin/env python3
"""Run bounded, serial FinLab priority passes through the existing owner.

This is a finite orchestration command, not a provider client or a daemon.
The child owns the account lock, all source admission and all native receipts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
OWNER = ROOT / "scripts/download_finlab_market_intraday.py"
CAPACITY_GUARD = ROOT / "scripts/check_acquisition_capacity.py"
MAX_PASSES = 32
PER_PASS = 4
MAX_BUSY_RETRIES = 32
BUSY_RETRY_SECONDS = 45
ADMISSION_RETRY_SECONDS = 60
HEADROOM_BYTES = 24 * 1024**3
MAX_WAIT_HOURS = 168


def wait_for_headroom(deadline: float, *, sample=None, clock=time.monotonic, sleep=time.sleep,
                      still_valid=lambda: True) -> bool:
    """Wait unlocked, stopping promptly when the caller's pinned owner changes.

    The callback is checked before each probe and again before returning an
    admission. It does not authorize a provider call or replace the child's
    capacity, source, quota and account-lock checks.
    """
    if sample is None:
        sys.path.insert(0, str(ROOT))
        from scripts.check_acquisition_capacity import priority_dispatch_status
        sample = lambda: priority_dispatch_status(ROOT / "data_finlab", additional_headroom_bytes=HEADROOM_BYTES)
    previous = None
    while clock() < deadline:
        if not still_valid():
            return False
        try:
            result = sample()
        except (OSError, ValueError, RuntimeError):
            result = {"state": "deferred", "reason": "capacity_probe_failed"}
        if not isinstance(result, dict):
            result = {"state": "deferred", "reason": "capacity_probe_invalid"}
        state, reason = result.get("state"), result.get("reason")
        if state == "admitted":
            return bool(still_valid())
        marker = (state, reason)
        if marker != previous:
            print(json.dumps({"contract": "finite_priority_headroom_wait_v1", "state": "waiting",
                              "capacity_state": state, "reason": reason,
                              "additional_headroom_bytes": HEADROOM_BYTES}), flush=True)
            previous = marker
        sleep(min(60, max(0, deadline - clock())))
    return False


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _batch(kind: str = "tw_minute", *, futures_workspace_only: bool = False) -> tuple[int, dict | None]:
    if kind not in {"tw_minute", "tw_tick"}:
        raise ValueError("unsupported finite priority family")
    limit = PER_PASS if kind == "tw_minute" else 1
    command = [sys.executable, str(OWNER), "--priority-only", "--capacity-deferred-priority",
               "--kind", kind, "--limit", str(limit), "--max-run-seconds", "1200"]
    if futures_workspace_only:
        if kind != "tw_minute":
            raise ValueError("futures workspace requires minute priority")
        command.append("--futures-workspace-only")
    last = None
    with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, bufsize=1) as child:
        assert child.stdout is not None
        for line in child.stdout:
            print(line, end="", flush=True)
            try:
                value = json.loads(line)
            except (TypeError, ValueError):
                continue
            if isinstance(value, dict):
                last = value
        code = child.wait()
    return code, last


def drain(max_passes: int, *, kind: str = "tw_minute", run_batch=None, owner_digest=None,
          guard_digest=None, capacity_wait=None, busy_sleep=time.sleep,
          futures_workspace_only: bool = False) -> tuple[int, dict]:
    if type(max_passes) is not int or not 1 <= max_passes <= MAX_PASSES:
        raise ValueError("finite priority drain needs 1..32 passes")
    if kind not in {"tw_minute", "tw_tick"}:
        raise ValueError("unsupported finite priority family")
    if type(futures_workspace_only) is not bool or futures_workspace_only and kind != "tw_minute":
        raise ValueError("futures workspace requires minute priority")
    limit = PER_PASS if kind == "tw_minute" else 1
    run = run_batch or (lambda: _batch(kind, futures_workspace_only=futures_workspace_only))
    digest = owner_digest or (lambda: _digest(OWNER))
    guard = guard_digest or (lambda: _digest(CAPACITY_GUARD))
    source = digest()
    capacity_source = guard()
    result = {"contract": "finlab_finite_priority_drain_v1", "state": "running",
              "owner_sha256": source, "capacity_guard_sha256": capacity_source, "maximum_passes": max_passes,
              "kind": kind, "partitions_per_pass": limit, "passes": 0, "attempted": 0,
              "successful": 0, "busy_retries": 0, "admission_waits": 0,
              "futures_workspace_only": futures_workspace_only, "full_history_completion_claim": False}
    while result["passes"] < max_passes:
        if digest() != source:
            result["state"] = "owner_changed"
            return 75, result
        if guard() != capacity_source:
            result["state"] = "capacity_guard_changed"
            return 75, result
        if capacity_wait is not None and not capacity_wait():
            result["state"] = "capacity_wait_timeout"
            return 75, result
        if capacity_wait is not None and digest() != source:
            result["state"] = "owner_changed"
            return 75, result
        if capacity_wait is not None and guard() != capacity_source:
            result["state"] = "capacity_guard_changed"
            return 75, result
        code, summary = run()
        if code == 0 and isinstance(summary, dict) and summary.get("state") == "account_sync_active":
            # Wait without holding the account lock if another bounded owner
            # won the race. Keep the retry count and total sleep bounded.
            if capacity_wait is not None and result["busy_retries"] < MAX_BUSY_RETRIES:
                result["busy_retries"] += 1
                busy_sleep(BUSY_RETRY_SECONDS)
                continue
            result["state"] = "account_busy"
            return 75, result
        result["passes"] += 1
        attempts = summary.get("attempted_this_run") if isinstance(summary, dict) else None
        successes = summary.get("successes_this_run") if isinstance(summary, dict) else None
        valid_counts = bool(isinstance(summary, dict)
            and summary.get("contract") == "finlab_finite_intraday_priority_pass_v1"
            and summary.get("selection_scope") == "finite_registered_priority"
            and summary.get("kind") == kind
            and (not futures_workspace_only or summary.get("futures_workspace_only") is True)
            and type(attempts) is int and type(successes) is int
            and 0 <= successes <= attempts <= limit)
        if valid_counts:
            result["attempted"] += attempts
            result["successful"] += successes
        if (code == 75 and capacity_wait is not None and valid_counts
                and summary.get("state") in {"waiting_required_general_data", "waiting_required_minutes",
                                             "opening_window_yield"}):
            # The owner may temporarily be unable to prove the finite-capacity
            # exception, or must yield to required general/minute work or the
            # protected opening window. Release
            # its lock, retain any partial attempts, then ask the same owner to
            # re-evaluate after the existing bounded wait. Never admit a source
            # request from this retry decision itself.
            result["passes"] -= 1
            result["admission_waits"] += 1
            print(json.dumps({"contract": "finlab_finite_priority_drain_v1",
                              "state": "waiting_owner_admission",
                              "owner_state": summary["state"],
                              "admission_waits": result["admission_waits"]}), flush=True)
            busy_sleep(ADMISSION_RETRY_SECONDS)
            continue
        if (code == 75 and capacity_wait is not None and isinstance(summary, dict)
                and summary.get("contract") == "acquisition_dispatch_capacity_v1"
                and summary.get("state") == "deferred"
                and summary.get("reason") in {"filesystem_reserve", "physical_backing_reserve"}):
            # The pre-lock guard can also lose the race to a concurrent writer.
            # No source attempt took place; wait again within the pass bound.
            continue
        if (code == 75 and capacity_wait is not None and futures_workspace_only
                and isinstance(summary, dict)
                and summary.get("contract") == "finlab_futures_workspace_dispatch_v1"
                and summary.get("state") == "deferred"
                and summary.get("reason") in {"control_capacity_unavailable", "workspace_unavailable"}):
            continue
        if code == 75 and valid_counts and summary.get("state") == "capacity_deferred" and capacity_wait is not None:
            # A concurrent writer can consume headroom between the parent's
            # sample and the owner's per-request check. Release the owner and
            # wait for the same bounded admission again.
            continue
        if code != 0 or not isinstance(summary, dict):
            result.update(state="child_deferred" if code == 75 else "child_failed",
                          child_exit_code=code, child_state=summary.get("state") if isinstance(summary, dict) else None)
            return code or 1, result
        if not valid_counts or summary.get("state") != "finite_priority_pass_complete":
            result.update(state="unexpected_child_result", child_state=summary.get("state"))
            return 1, result
        if attempts < limit:
            result["state"] = "no_more_actionable_in_pass"
            return 0, result
    result["state"] = "pass_limit_reached"
    return 0, result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-passes", type=int, default=1,
                        help="number of finite passes, at most 32")
    parser.add_argument("--kind", choices=("tw_minute", "tw_tick"), default="tw_minute",
                        help="four-minute or one-stock-Tick registered partitions per pass")
    parser.add_argument("--wait-for-capacity", action="store_true",
                        help="wait for finite-priority headroom without holding the account lock")
    parser.add_argument("--capacity-wait-hours", type=int, default=12,
                        help="bounded wait duration, default 12 hours and at most 168 hours")
    parser.add_argument("--futures-workspace-only", action="store_true",
                        help="use the enrolled E workspace for bounded futures minute objects")
    args = parser.parse_args(argv)
    if not 1 <= args.max_passes <= MAX_PASSES:
        parser.error("--max-passes must be between 1 and 32")
    if not 1 <= args.capacity_wait_hours <= MAX_WAIT_HOURS:
        parser.error("--capacity-wait-hours must be between 1 and 168")
    if args.futures_workspace_only and args.kind != "tw_minute":
        parser.error("futures workspace requires minute priority")
    deadline = time.monotonic() + args.capacity_wait_hours * 60 * 60
    if args.wait_for_capacity and args.futures_workspace_only:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        from scripts.check_acquisition_capacity import futures_workspace_dispatch_status
        waiter = lambda: wait_for_headroom(deadline,
            sample=lambda: futures_workspace_dispatch_status(ROOT / "data_finlab"))
    else:
        waiter = (lambda: wait_for_headroom(deadline)) if args.wait_for_capacity else None
    kwargs = {"futures_workspace_only": True} if args.futures_workspace_only else {}
    code, result = drain(args.max_passes, kind=args.kind, capacity_wait=waiter, **kwargs)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
