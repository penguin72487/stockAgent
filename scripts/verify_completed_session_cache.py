"""Read back canonical completed-close artifacts, forbidding all model/CUDA work.

This verifies reuse, not fills, full fold history or future opening readiness.
It writes only the explicitly requested engineering receipt.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from services.discord_bot import bot as discord_bot
from stockagent.config import load_config
from stockagent.live.completed_inference_cache import CONTRACT
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard
import torch


def forbidden(*args, **kwargs):
    raise AssertionError("completed artifact reuse attempted model or CUDA work")


def verify() -> dict:
    if torch.cuda.is_initialized():
        raise RuntimeError("verification must start in a CUDA-cold process")
    rows = []
    with ExitStack() as stack:
        stack.enter_context(patch.object(discord_bot, "generate_live_signal", forbidden))
        for name in ("is_available", "init", "synchronize", "empty_cache"):
            stack.enter_context(patch.object(torch.cuda, name, forbidden))
        for market in discord_bot._artifact_maintenance_markets():
            cfg = discord_bot._effective_market_config(discord_bot._resolve_market(market))
            if not cfg.day_trade_simulation_enabled:
                continue
            config = load_config(cfg.config_path)
            with live_tw_public_source_read_guard(config.data.parquet_root):
                status = discord_bot._ensure_signal_ready(cfg)
                if status.market_open or not status.data.fresh:
                    raise RuntimeError(f"{market}: not a fresh completed session")
                if not discord_bot._completed_session_receipt_ready(status):
                    raise RuntimeError(f"{market}: completed source receipt not accepted")
                latest = discord_bot._latest_market_signal(cfg)
                if latest is None:
                    raise RuntimeError(f"{market}: canonical latest artifact missing")
                path, summary = latest
                reusable, reason = discord_bot._can_reuse_latest_signal_now(
                    cfg, status, summary, requested_price_source="panel")
                if not reusable or summary.get("postclose_fast_preview"):
                    raise RuntimeError(f"{market}: artifact not reusable: {reason}")
                weights = discord_bot._summary_artifact_path(summary, "weights_path", path)
                if weights is None or not weights.is_file():
                    raise RuntimeError(f"{market}: complete weights artifact missing")
                forward = (summary.get("live_latency") or {}).get("completed_forward_cache") or {}
                if forward.get("contract") != CONTRACT or not forward.get("key"):
                    raise RuntimeError(f"{market}: old forward cache contract")
                # Invoke the real canonical consumer. It must reuse the pointer;
                # returning a recomputed signal is impossible under this patch.
                if discord_bot._run_completed_session_signal_cache_sync(cfg) is not None:
                    raise RuntimeError(f"{market}: reused artifact unexpectedly recomputed")
                rows.append({"market": market, "summary_path": str(path),
                             "panel_date": summary.get("panel_date"),
                             "signal_id": summary.get("signal_id"),
                             "cache_key": forward["key"], "reuse_reason": reason,
                             "completed_source_identity": summary.get("completed_panel_source_fingerprint"),
                             "canonical_reuse": True, "gpu_forward_calls": 0})
        if not rows or torch.cuda.is_initialized():
            raise RuntimeError("no verified markets or unexpected CUDA initialization")
    return {"contract": "completed_session_canonical_reuse_acceptance_v1",
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "current_enabled_tw_day_trade_close_artifacts_not_ledger_or_formal_history",
            "forward_contract": CONTRACT, "market_count": len(rows), "markets": rows,
            "cuda_initialized": False, "model_forward_calls": 0,
            "provider_calls": 0, "trading_writes": False}


def matched_benchmarks(result: dict, root: Path, controls_root: Path | None = None) -> list[dict]:
    """Match the actual formal cache keys to independent full-output controls."""
    controls_root = controls_root or root
    rows = []
    for market in result["markets"]:
        name = market["market"]
        candidate_path = root / f"postclose-accepted-fresh-{name}.json"
        control_path = controls_root / ("postclose-control-current.json"
            if name == "tw_day_trade_factorized_scale_separated_cash"
            else f"postclose-control-{name}.json")
        candidate = json.loads(candidate_path.read_text())
        control = json.loads(control_path.read_text())
        if (candidate["market"] != name or candidate["cuda_initialized"] is not False
                or candidate["output_sha256"] != control["output_sha256"]
                or candidate["network_requests"] != 0 or candidate["production_writes"] is not False
                or not candidate["samples"]):
            raise RuntimeError(f"{name}: benchmark/control acceptance mismatch")
        for sample in candidate["samples"]:
            receipt = sample["stages"]["completed_forward_cache"]
            if (receipt["key"] != market["cache_key"] or receipt["contract"] != CONTRACT
                    or receipt["state"] != "disk_hit" or receipt["gpu_forward_calls"] != 0
                    or sample["gpu_peak_allocated_bytes"] != 0 or sample["gpu_peak_reserved_bytes"] != 0):
                raise RuntimeError(f"{name}: cached benchmark attempted work or used another key")
        rows.append({"market": name, "control_path": str(control_path),
                     "benchmark_path": str(candidate_path), "output_sha256": candidate["output_sha256"],
                     "warm_median_ms": candidate["warm_median_ms"],
                     "rss_peak_bytes": max(row["rss_peak_bytes"] for row in candidate["samples"]),
                     "same_formal_cache_key": True, "exact_output_parity": True})
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--benchmarks", type=Path,
                        help="Also require matching independent no-CUDA full-output benchmarks")
    parser.add_argument("--controls", type=Path,
                        help="Independent existing controls; do not copy/overwrite historical evidence")
    args = parser.parse_args()
    if args.controls is not None and args.benchmarks is None:
        parser.error("--controls requires --benchmarks")
    result = verify()
    if args.benchmarks is not None:
        result["benchmarks"] = matched_benchmarks(result, args.benchmarks, args.controls)
    atomic_write_json(args.output, result)
    print(f"verified canonical reuse for {result['market_count']} markets; CUDA initialized=False")


if __name__ == "__main__":
    main()
