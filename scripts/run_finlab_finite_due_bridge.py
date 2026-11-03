#!/usr/bin/env python3
"""Drain a bounded set of due prerequisites through the existing FinLab owner.

This supervisor does not call a provider or unpause maintenance.  Each short
pass releases the original account lock and keeps its capacity, quota, opening,
priority-handoff and retry rules.  Remaining work is not completion evidence.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import time

from downloader.artifact_io import sha256_file


ROOT = Path(__file__).resolve().parents[1]
OWNER_FILES = tuple(ROOT / path for path in (
    "scripts/runtime_env.sh", "scripts/run_finlab_refresh.sh", "scripts/download_finlab_history.py",
    "scripts/check_acquisition_capacity.py", "downloader/provider_priority.py",
    "stockagent/data/finlab_acquisition_contract.py",
))


def classify_pass(code: int, lines: list[str]) -> tuple[str, dict | None]:
    """Interpret only summaries actually produced by this child invocation."""
    summaries = []
    for line in lines:
        if line.startswith("finlab_sync="):
            try:
                row = json.loads(line[len("finlab_sync="):])
            except (ValueError, TypeError) as error:
                raise ValueError("invalid owner summary") from error
            if not isinstance(row, dict) or not isinstance(row.get("state"), str):
                raise ValueError("invalid owner summary")
            summaries.append(row)
    if code == 75:
        return "capacity_deferred", summaries[-1] if summaries else None
    if code:
        return "owner_failed", summaries[-1] if summaries else None
    if not summaries:
        if any("another local sync is active" in line for line in lines):
            return "owner_busy", None
        raise ValueError("owner returned without an attributable sync summary")
    last = summaries[-1]
    if last["state"] == "priority_handoff":
        return "priority_handoff", last
    if last["state"] == "partial":
        return "partial", last
    return "owner_stopped", last


def _run_owner() -> tuple[int, list[str]]:
    env = os.environ.copy()
    env.update(FINLAB_FINITE_GENERAL_ONLY="1", FINLAB_SYNC_MAX_BATCHES="2", FINLAB_SYNC_BATCH_SIZE="8")
    lines = []
    with subprocess.Popen(["bash", "scripts/run_finlab_refresh.sh"], cwd=ROOT, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1) as child:
        assert child.stdout is not None
        for line in child.stdout:
            print(line, end="", flush=True)
            lines.append(line.rstrip("\n"))
        return child.wait(), lines


def bridge(prerequisite: Path, *, max_passes: int, wait_seconds: int) -> tuple[int, dict]:
    if not 1 <= max_passes <= 64 or not 60 <= wait_seconds <= 12 * 60 * 60:
        raise ValueError("a bounded number of passes and wait time are required")
    prerequisite = prerequisite.resolve()
    if not prerequisite.is_relative_to((ROOT / "artifacts/operations/agent-workflow/runs").resolve()):
        raise ValueError("prerequisite must be an existing workflow run")
    pins = {path: sha256_file(path) for path in OWNER_FILES}
    deadline = time.monotonic() + wait_seconds
    result = {"contract": "finlab_finite_due_bridge_v1", "state": "waiting_prerequisite",
              "prerequisite": str(prerequisite), "max_passes": max_passes,
              "pass_limit_keys": 16, "passes": 0, "owner_busy_waits": 0,
              "priority_handoffs": 0, "capacity_waits": 0, "provider_calls_by_bridge": 0,
              "source_history_or_training_complete": False}
    while time.monotonic() < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            result["state"] = "owner_changed"
            return 75, result
        run = json.loads(prerequisite.read_text(encoding="utf-8"))
        if run.get("state") in {"queued", "running"}:
            time.sleep(min(10, max(0, deadline - time.monotonic())))
            continue
        if run.get("state") != "succeeded" or run.get("exit_code") != 0:
            result["state"] = "prerequisite_not_successful"
            return 75, result
        break
    else:
        result["state"] = "prerequisite_wait_elapsed"
        return 75, result
    while result["passes"] < max_passes and time.monotonic() < deadline:
        if any(sha256_file(path) != digest for path, digest in pins.items()):
            result["state"] = "owner_changed"
            return 75, result
        code, lines = _run_owner()
        state, last = classify_pass(code, lines)
        if state in {"owner_busy", "capacity_deferred", "priority_handoff"}:
            counter = {"owner_busy": "owner_busy_waits", "capacity_deferred": "capacity_waits",
                       "priority_handoff": "priority_handoffs"}[state]
            result[counter] += 1
            print(json.dumps({"state": state, "at_utc": datetime.now(UTC).isoformat(),
                              "provider_calls_by_bridge": 0}), flush=True)
            time.sleep(min(60, max(0, deadline - time.monotonic())))
            continue
        if state == "owner_failed":
            result.update(state=state, owner_exit_code=code)
            return code, result
        result["passes"] += 1
        result["last_owner_state"] = last["state"]
        result["last_required_pending"] = last.get("required_pending")
        if state == "owner_stopped":
            result["state"] = "owner_stopped_review_receipts"
            return 0, result
    result["state"] = "pass_limit_reached" if result["passes"] >= max_passes else "wait_elapsed"
    return 0 if result["state"] == "pass_limit_reached" else 75, result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prerequisite-run", type=Path, required=True)
    parser.add_argument("--max-passes", type=int, default=32)
    parser.add_argument("--wait-seconds", type=int, default=21600)
    args = parser.parse_args()
    code, result = bridge(args.prerequisite_run, max_passes=args.max_passes, wait_seconds=args.wait_seconds)
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
