#!/usr/bin/env python3
"""Verify persisted paper state can restart without editing the production ledger.

Copies only canonical restart inputs into an isolated directory, constructs the
same engine there and compares its restored inventory and consumed signals. The
retained byte hashes can later prove production ledgers kept their exact prefix.
No services, signal pointers, production state or orders are modified.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine, _atomic_json

INPUTS = ("state.json", "events.jsonl", "orders.jsonl", "fills.jsonl")


def prefix_sha256(path, size):
    digest = hashlib.sha256()
    remaining = size
    with path.open("rb") as handle:
        while remaining:
            chunk = handle.read(min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeError(f"ledger was truncated: {path.name}")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def inventory(state):
    return {market: {
        "signal_id": mode.get("signal_id"),
        "processed_signal_ids": mode.get("processed_signal_ids") or [],
        "session_sizing_nav_twd": mode.get("session_sizing_nav_twd"),
        "cumulative_realized_net_pnl_twd": mode.get("cumulative_realized_net_pnl_twd"),
        "positions": mode.get("positions") or {},
        "pending_entry_orders": mode.get("pending_entry_orders") or {},
    } for market, mode in (state.get("modes") or {}).items()}


def verify_prefix(state_dir, previous):
    results = {}
    for name, original in previous["inputs"].items():
        if name == "state.json":  # mutable checkpoint, not an append-only ledger
            continue
        path = state_dir / name
        size = original["size_bytes"]
        digest = prefix_sha256(path, size)
        if digest != original["sha256"]:
            raise RuntimeError(f"ledger prefix changed: {name}")
        results[name] = {"verified_prefix_bytes": size, "sha256": digest,
                         "current_size_bytes": path.stat().st_size}
    return results


def audit(state_dir, output):
    with tempfile.TemporaryDirectory(prefix="tw-paper-restart-") as temporary:
        isolated = Path(temporary)
        # Reject a checkpoint copied across an in-flight append/commit. Retrying
        # this read-only audit is safe; silently accepting mismatched state is not.
        # With no pending retry commit, construction reads only checkpoint and
        # commit events. Do not copy/reparse gigabytes of historical signal rows.
        paths = [state_dir / name for name in ("state.json", "events.jsonl") if (state_dir / name).is_file()]
        before = {path.name: (path.stat().st_size, path.stat().st_mtime_ns) for path in paths}
        hashes = {}
        for path in paths:
            copy = isolated / path.name
            shutil.copyfile(path, copy)
            hashes[path.name] = {"size_bytes": copy.stat().st_size,
                                 "sha256": hashlib.sha256(copy.read_bytes()).hexdigest()}
        after = {path.name: (path.stat().st_size, path.stat().st_mtime_ns) for path in paths}
        if before != after:
            raise RuntimeError("canonical state changed during audit; retry at a stable boundary")
        original = json.loads((isolated / "state.json").read_text())
        for market, mode in (original.get("modes") or {}).items():
            if mode.get("entry_retry_pending_commit") or mode.get("ledger_state_divergence"):
                raise RuntimeError(f"{market}: pending or divergent ledger commit; reload not accepted")
        engine = TwDayTradeSimulationEngine(isolated)
        if inventory(original) != inventory(engine.state):
            raise RuntimeError("restart altered persisted inventory, raw targets or consumed signals")
        for market, mode in (engine.state.get("modes") or {}).items():
            if mode.get("ledger_state_divergence"):
                raise RuntimeError(f"{market}: restart found ledger-state divergence")
        for name in ("orders.jsonl", "fills.jsonl"):
            path = state_dir / name
            if path.is_file():
                size = path.stat().st_size
                hashes[name] = {"size_bytes": size, "sha256": prefix_sha256(path, size)}
        # Retain the exact checkpoint for independent recovery; ledger data stays
        # append-only in production and is protected by its byte-prefix hashes.
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(isolated / "state.json", output.with_suffix(".state.json"))
        receipt = {"schema_version": 1, "status": "accepted", "simulation_only": True,
                   "production_order_possible": False, "production_state_modified": False,
                   "observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
                   "state_dir": str(state_dir), "inputs": hashes,
                   "inventory_restored_exactly": True,
                   "open_position_count": sum(bool(p.get("signed_shares"))
                       for mode in original.get("modes", {}).values()
                       for p in (mode.get("positions") or {}).values()),
                   "markets": {market: {"signal_id": mode.get("signal_id"),
                       "open_position_count": sum(bool(p.get("signed_shares"))
                           for p in (mode.get("positions") or {}).values())}
                       for market, mode in original.get("modes", {}).items()}}
        _atomic_json(output, receipt)
        return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=ROOT / "artifacts/live/tw_day_trade_simulation")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--verify-prefix", type=Path)
    args = parser.parse_args()
    if args.verify_prefix:
        previous = json.loads(args.verify_prefix.read_text())
        receipt = {"status": "accepted", "production_state_modified": False,
                   "observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
                   "ledger_prefixes": verify_prefix(args.state_dir, previous)}
        _atomic_json(args.output, receipt)
    else:
        receipt = audit(args.state_dir, args.output)
    print(json.dumps(receipt, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
