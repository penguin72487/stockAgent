"""Read back canonical completed-close artifacts, forbidding all model/CUDA work.

This verifies reuse, not fills, full fold history or future opening readiness.
It writes only the explicitly requested engineering receipt.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from services.discord_bot import bot as discord_bot
from stockagent.config import load_config
from stockagent.live import completed_inference_cache as forward_cache
from stockagent.live.completed_inference_cache import CONTRACT
from stockagent.live.tw_public_source_guard import (
    TWPublicSourceUpdatePending, live_tw_public_source_read_guard,
)
import torch


def forbidden(*args, **kwargs):
    raise AssertionError("completed artifact reuse attempted model or CUDA work")


def identity_diagnostic(cfg, summary: dict) -> dict:
    """Inspect canonical identity components; stat hints are not acceptance.

    The source calculator owns the member set. Capture its digest preimages,
    rather than maintaining another enumeration that could omit a dependency.
    This private, CUDA-cold observer never modifies a summary/cache or source.
    """
    captured = {}
    original = forward_cache.identity_digest

    def record(value):
        if "members" in value:
            captured["source_members"] = value["members"]
        elif "source" in value and "configuration" in value and "code" in value:
            captured["current_identity_components"] = value
        return original(value)

    with patch.object(forward_cache, "identity_digest", record):
        current = discord_bot.completed_panel_source_fingerprint(
            cfg.config_path, rule_data_dir=cfg.day_trade_rule_data_dir)
    key = ((summary.get("live_latency") or {}).get("completed_forward_cache") or {}).get("key")
    detail = {"saved_composite_identity": summary.get("completed_panel_source_fingerprint"),
              "current_composite_identity": current, "forward_key": key, **captured,
              "scope": "generation/config/code diagnostic, not payload integrity or source acceptance"}
    if not isinstance(key, str) or re.fullmatch(r"[0-9a-f]{64}", key) is None:
        detail["forward_receipt_error"] = "missing_or_invalid_key"
        return detail
    root = Path(os.environ.get("STOCKAGENT_COMPLETED_INFERENCE_CACHE_ROOT") or
                str(ROOT / "artifacts/cache/completed_live_inference"))
    try:
        with (root / f"{key}.receipt.json").open("rb") as handle:
            raw = handle.read(1024**2 + 1)
        if len(raw) > 1024**2:
            raise ValueError("forward receipt exceeds bounded metadata size")
        receipt = json.loads(raw)
        if receipt.get("contract") != CONTRACT or receipt.get("key") != key:
            raise ValueError("forward receipt contract/key mismatch")
        saved = receipt["identity"]
        components = captured["current_identity_components"]
        detail.update(resolved_config_matches=saved["resolved_config"] == components["configuration"],
                      implementation_matches=saved["code"] == components["code"],
                      forward_input_identity=saved["input"],
                      forward_mask_identity=saved["mask"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        detail["forward_receipt_error"] = f"{type(exc).__name__}: {exc}"
    return detail


def verify(*, diagnose: bool = False) -> dict:
    if torch.cuda.is_initialized():
        raise RuntimeError("verification must start in a CUDA-cold process")
    rows, errors = [], []
    requested = 0
    with ExitStack() as stack:
        stack.enter_context(patch.object(discord_bot, "generate_live_signal", forbidden))
        for name in ("is_available", "init", "synchronize", "empty_cache"):
            stack.enter_context(patch.object(torch.cuda, name, forbidden))
        import requests
        stack.enter_context(patch.object(requests.sessions.Session, "request", forbidden))
        for market in discord_bot._artifact_maintenance_markets():
            cfg = discord_bot._effective_market_config(discord_bot._resolve_market(market))
            if not cfg.day_trade_simulation_enabled:
                continue
            requested += 1
            config = load_config(cfg.config_path)
            try:
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
                        error = RuntimeError(f"{market}: artifact not reusable: {reason}")
                        if not diagnose:
                            raise error
                        errors.append({"market": market, "summary_path": str(path),
                                       "error_type": type(error).__name__, "error": str(error),
                                       "identity_diagnostic": identity_diagnostic(cfg, summary)})
                        continue
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
                    row = {"market": market, "summary_path": str(path),
                           "panel_date": summary.get("panel_date"),
                           "signal_id": summary.get("signal_id"),
                           "cache_key": forward["key"], "reuse_reason": reason,
                           "completed_source_identity": summary.get("completed_panel_source_fingerprint"),
                           "canonical_reuse": True, "gpu_forward_calls": 0}
                    if diagnose:
                        row["identity_diagnostic"] = identity_diagnostic(cfg, summary)
                    rows.append(row)
            except (TWPublicSourceUpdatePending, forward_cache.LoadedImplementationChanged) as error:
                if not diagnose:
                    raise
                # Expected admission boundaries are still non-acceptance. Keep
                # every market visible without a stale read or an attempted
                # inference; never catch an unexpected validation/CUDA failure.
                errors.append({"market": market, "error_type": type(error).__name__,
                               "error": str(error), "admission": error.details})
        if (not rows and not (diagnose and errors)) or torch.cuda.is_initialized():
            raise RuntimeError("no verified markets or unexpected CUDA initialization")
    return {"contract": ("completed_session_canonical_reuse_diagnostic_v1" if diagnose else
                         "completed_session_canonical_reuse_acceptance_v1"),
            "state": "rejected" if errors else "accepted", "errors": errors,
            "requested_market_count": requested,
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
    parser.add_argument("--diagnose", action="store_true",
                        help="Persist all identity-mismatch observations; still exit nonzero if any mode rejects")
    args = parser.parse_args()
    if args.controls is not None and args.benchmarks is None:
        parser.error("--controls requires --benchmarks")
    if args.diagnose and args.benchmarks is not None:
        parser.error("diagnostics and independent full-output acceptance are distinct modes")
    result = verify(diagnose=args.diagnose)
    if args.benchmarks is not None:
        result["benchmarks"] = matched_benchmarks(result, args.benchmarks, args.controls)
    atomic_write_json(args.output, result)
    print(f"{result['state']}: canonical reuse for {result['market_count']}/{result['requested_market_count']} "
          f"markets; rejected={len(result['errors'])}; CUDA initialized=False")
    if result["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
