#!/usr/bin/env python3
"""Read-only formal-history lifecycle/calendar preflight, with compute forbidden.

Only the requested engineering receipt is written. This is not full source,
checkpoint, report-content or numerical acceptance, and does not dispatch work.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch
from contextlib import ExitStack

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def forbidden(*_args, **_kwargs):
    raise AssertionError("formal input diagnosis attempted model/provider/child/CUDA work")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError("preserve engineering readback receipts")
    if not args.output.resolve().is_relative_to((ROOT / "artifacts/operations").resolve()):
        raise ValueError("readback receipt must remain under artifacts/operations")
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime

    pools = configure_cpu_source_runtime()
    import requests
    import torch
    from services.discord_bot import bot
    from downloader.artifact_io import atomic_write_json

    if torch.cuda.is_initialized():
        raise RuntimeError("input readback must start CUDA cold")
    source = ROOT / "services/discord_bot/bot.py"
    implementation_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    rows = []
    started = time.perf_counter()
    with ExitStack() as stack:
        stack.enter_context(patch.object(requests.Session, "request", forbidden))
        stack.enter_context(patch.object(bot.subprocess, "run", forbidden))
        stack.enter_context(patch.object(bot, "generate_live_signal", forbidden))
        for name in ("is_available", "init", "_lazy_init", "synchronize", "empty_cache", "current_device"):
            stack.enter_context(patch.object(torch.cuda, name, forbidden))
        for name in bot._artifact_maintenance_markets():
            cfg = bot._effective_market_config(bot._resolve_market(name))
            if not cfg.day_trade_simulation_enabled:
                continue
            t = time.perf_counter()
            raw_latest = bot._formal_history_latest_date(cfg)
            completed_latest = bot._formal_history_completed_date(cfg)
            preflight_error = None
            try:
                bot._preflight_formal_history_calendar(cfg)
            except bot.BotUserError as exc:
                preflight_error = str(exc)[:2000]
            rows.append({
                "market": name, "raw_returns_latest": raw_latest,
                "completed_fold_latest": completed_latest,
                "matching_lifecycle_hint": completed_latest is not None,
                "preflight_state": "proven_calendar_mismatch" if preflight_error else "not_rejected",
                "preflight_error": preflight_error,
                "formal_history_timeout_seconds": bot._formal_history_timeout_seconds(cfg),
                "observed_seconds": time.perf_counter() - t,
            })
    if torch.cuda.is_initialized() or hashlib.sha256(source.read_bytes()).hexdigest() != implementation_sha:
        raise RuntimeError("CUDA initialized or implementation changed during readback")
    result = {
        "contract": "formal_history_read_only_preflight_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "bot_sha256": implementation_sha, "processing_pools": pools,
        "observed_seconds": time.perf_counter() - started, "markets": rows,
        "cuda_initialized": False, "model_forward_calls": 0, "provider_requests": 0,
        "child_process_calls": 0, "production_writes": False,
        "scope": "Cheap full-scope rejection/lifecycle scheduling hints, not complete artifact or model output acceptance",
    }
    atomic_write_json(args.output, result)
    print(json.dumps({"markets": len(rows), "blocked_calendars": sum(row["preflight_error"] is not None for row in rows),
                      "observed_seconds": result["observed_seconds"], "cuda_initialized": False}))


if __name__ == "__main__":
    main()
