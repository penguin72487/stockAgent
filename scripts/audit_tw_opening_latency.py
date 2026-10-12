#!/usr/bin/env python3
"""Retain real opening attempts and benchmark current models without publishing.

The optional inference probe is receipt-only and never writes a signal pointer,
ledger, or broker order. Its midday/hot-cache timing is NOT a 09:00 latency.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import time
import subprocess
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.discord_bot import bot
from stockagent.live import signal_engine
from stockagent.live.tw_day_trade_simulation import _atomic_json


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", default=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--infer", action="store_true")
    parser.add_argument("--runtime", action="store_true", help="Attach current owners, Gateway, execution and source-repair receipts; no mutations")
    parser.add_argument("--rounds", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 10:
        parser.error("rounds must be 1..10")
    if args.infer and args.session_date != datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat():
        parser.error("receipt-only inference cannot backdate current observations")
    if args.infer and not (datetime.now(ZoneInfo("Asia/Taipei")).hour >= 9):
        parser.error("receipt-only current-open inference cannot run before 09:00")
    report = {"schema_version": 1, "session_date": args.session_date,
              "observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
              "simulation_only": True, "ledger_modified": False, "signal_published": False,
              "orders_submitted": False, "real_opening_attempts": {}, "receipt_only_model_probes": []}
    path = bot._opening_signal_latency_path()
    with path.open() as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("session_date") != args.session_date:
                continue
            stats = report["real_opening_attempts"].setdefault(row["market"], {
                "attempt_count": 0, "failed_count": 0, "first": row,
                "last": row, "ready": []})
            stats["attempt_count"] += 1
            stats["failed_count"] += int(row["status"] == "failed")
            stats["last"] = row
            if row["status"] == "ready":
                stats["ready"].append(row)
    _atomic_json(args.output, report)
    if args.runtime:
        runtime = {"observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
                   "service_restart_performed": False, "broker_orders_submitted": False}
        for name, path in {
            "engine": bot._day_trade_state_dir() / "status.json",
            "discord": bot._discord_service_status_path(),
            "source_repair": bot._day_trade_state_dir() / "opening_source_repair.json",
        }.items():
            try:
                runtime[name] = json.loads(path.read_text())
            except (OSError, ValueError) as exc:
                runtime[name] = {"status": "unavailable", "error_type": type(exc).__name__}
        owner = subprocess.run(["systemctl", "show",
            "stockagent-discord-bot.service", "stockagent-tw-day-trade-simulation.service",
            "-p", "Id", "-p", "ActiveState", "-p", "SubState", "-p", "InvocationID",
            "-p", "MainPID", "-p", "ActiveEnterTimestamp", "-p", "NRestarts"],
            capture_output=True, text=True, timeout=10, check=True)
        runtime["service_owners"] = [dict(line.split("=", 1) for line in block.splitlines()
             if "=" in line) for block in owner.stdout.strip().split("\n\n")]
        runtime["current_session_signal_ready"] = {
            market: bool(stats["ready"]) for market, stats in report["real_opening_attempts"].items()}
        runtime["current_session_engine_registered"] = {
            market: mode.get("session_date") == args.session_date
            for market, mode in (runtime.get("engine", {}).get("modes") or {}).items()}
        runtime["opening_slo_success_claim"] = False
        report["runtime_verification"] = runtime
        _atomic_json(args.output, report)
    if not args.infer:
        return 0

    def receipt_only(symbols, fallback, *, parquet_root, fetch_mis):
        return fetch_mis(allow_network=False), True

    with patch.object(signal_engine, "_fetch_tw_opening_source", receipt_only):
        for round_id in range(1, args.rounds + 1):
            for original in bot._market_configs().values():
                if not original.day_trade_simulation_enabled or not bot._market_enabled(original):
                    continue
                cfg = bot._effective_market_config(original)
                checkpoint_sha = hashlib.sha256(Path(cfg.checkpoint_path).read_bytes()).hexdigest()
                started = time.perf_counter()
                timing = {}
                item = {"round": round_id, "market": cfg.market,
                        "measurement_boundary": "current_session_receipt_only_no_network_no_publication",
                        "checkpoint_sha256": checkpoint_sha,
                        "live_opening_slo_claim": False}
                try:
                    result = signal_engine.generate_live_signal(**cfg.signal_kwargs(
                        price_source="tw", write=False, publish_latest=False,
                        ensure_previous_signal=False, previous_signal_backfill_limit=0,
                        defer_rich_artifacts=True, _timing_sink=timing))
                    item.update(status="ready", summary=result.summary,
                                missing_model_targets=[
                                    {k: r.get(k) for k in ("symbol", "model_weight", "target_weight", "open_price", "opening_price_wait_reason")}
                                    for r in result.weights_rows if r.get("opening_price_wait_reason")])
                except Exception as exc:
                    item.update(status="failed", error_type=type(exc).__name__, error=str(exc), timing=timing)
                item["elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
                report["receipt_only_model_probes"].append(item)
                _atomic_json(args.output, report)
                print(json.dumps({k: v for k, v in item.items() if k not in {"summary", "missing_model_targets", "timing"}}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
