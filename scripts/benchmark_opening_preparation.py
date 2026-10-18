"""Compare complete deployed opening-status checks without live side effects.

Both variants recompute the current market clock, eligibility of the session,
checkpoint/config fingerprints and data freshness. The control restores full
config validation and the old double benchmark-Parquet metadata read. This
measures prewarmed application work, NOT historical PIT or live 09:00 latency.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime
import hashlib
import json
import numpy as np
from pathlib import Path
import statistics
import sys
import time
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.discord_bot import bot
from stockagent.config import load_config
from stockagent.live import market_status


def input_preparation_probe(cfgs, *, repeats):
    """Full current mode inputs, cold then warm; no forward/network/publication."""
    from stockagent.live import signal_engine
    import torch

    if torch.cuda.is_initialized():
        raise RuntimeError("input probe requires a fresh CUDA-cold process")
    samples = []

    def receipt_only(symbols, fallback, *, parquet_root, fetch_mis, **kwargs):
        return fetch_mis(allow_network=False), True

    def probe(original):
        cfg = bot._effective_market_config(original)
        started = clock = time.perf_counter()
        phase, stages = "start", {}

        def progress(event):
            nonlocal phase, clock
            now = time.perf_counter()
            stages[phase] = stages.get(phase, 0.) + (now - clock) * 1000
            phase, clock = str(event["message"]), now

        result = signal_engine.prefetch_live_signal_prices(**cfg.signal_kwargs(
            price_source="tw", progress_callback=progress))
        progress({"message": "finished"})
        digest = hashlib.sha256()
        for field in ("prices", "open_prices", "available_mask", "exchange_timestamps_ms"):
            digest.update(np.asarray(getattr(result.snapshot, field)).tobytes())
        return {"market": cfg.market, "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
            "input_stages_ms": {key: round(ms, 3) for key, ms in stages.items()},
            "request_key": result.request_key, "source_value_clock_sha256": digest.hexdigest()}

    with patch.object(signal_engine, "_fetch_tw_opening_source", receipt_only), \
            patch("requests.sessions.Session.request", side_effect=AssertionError("input probe forbids network")):
        with ThreadPoolExecutor(max_workers=len(cfgs)) as pool:
            for repeat in range(repeats):
                started = time.perf_counter()
                modes = list(pool.map(probe, cfgs))
                samples.append({"repeat": repeat, "cache_state": "fresh_process_first_use" if repeat == 0 else "process_warm_revalidated",
                    "all_mode_inputs_ms": round((time.perf_counter() - started) * 1000, 3), "modes": modes})
    if torch.cuda.is_initialized():
        raise RuntimeError("input preparation unexpectedly initialized CUDA")
    return {"contract": "five_mode_input_preparation_probe_v1",
        "scope": "current_session_receipt_only_no_forward_not_real_opening_slo", "mode_count": len(cfgs),
        "signal_published": False, "ledger_modified": False,
        "derived_cache_writes_possible": True, "network_requests": 0, "model_forwards": 0,
        "cuda_initialized": False, "opening_slo_claim": False, "samples": samples}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=8)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inputs-only", action="store_true", help="Probe all five current complete input paths without quotes, CUDA, model forward or publication")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("repeats must be positive")
    cfgs = [bot._resolve_market(market) for market in bot._scheduled_markets()
            if bot._resolve_market(market).day_trade_simulation_enabled]
    if not cfgs:
        parser.error("no deployed day-trade modes")
    if args.inputs_only:
        from downloader.artifact_io import atomic_write_json
        result = input_preparation_probe(cfgs, repeats=args.repeats)
        atomic_write_json(args.output, result, durable=True)
        print(json.dumps({**{key: value for key, value in result.items() if key != "samples"},
            "all_mode_input_samples_ms": [row["all_mode_inputs_ms"] for row in result["samples"]]}))
        return
    clock = datetime.now(ZoneInfo("Asia/Taipei"))
    contracts, warmup = {}, []
    for cfg in cfgs:
        started = time.perf_counter()
        status = market_status.runtime_status(cfg, root=ROOT, now=clock)
        warmup.append({"market": cfg.market, "elapsed_ms": (time.perf_counter() - started) * 1000})
        if status.data.scanned_files != 1 or status.data.benchmark_date is None:
            raise RuntimeError("paired control requires the daily single benchmark freshness anchor")
        contracts[cfg.market] = _digest(status)

    locator = market_status._validated_data_locator
    reader = market_status._max_date_from_parquet
    inventory = market_status._feature_files

    def uncached_locator(path):
        config = load_config(path)
        return config.data.parquet_root, config.data.benchmark_name

    def double_read(path, **kwargs):
        value = market_status._max_date_from_parquet_uncached(path, **kwargs)
        market_status._max_date_from_parquet_uncached(path, **kwargs)
        return value

    def check(cfg):
        started = time.perf_counter()
        status = market_status.runtime_status(cfg, root=ROOT, now=clock)
        if _digest(status) != contracts[cfg.market]:
            raise RuntimeError(f"{cfg.market}: complete runtime status changed; reject comparison")
        return {"market": cfg.market, "elapsed_ms": (time.perf_counter() - started) * 1000,
                "contract_sha256": contracts[cfg.market], "status": status.status,
                "market_open": status.market_open}

    samples = []
    with ThreadPoolExecutor(max_workers=len(cfgs)) as pool:
        for repeat in range(args.repeats):
            for variant in (("control", "candidate") if repeat % 2 == 0 else ("candidate", "control")):
                with patch.object(market_status, "_validated_data_locator", locator if variant == "candidate" else uncached_locator), \
                        patch.object(market_status, "_max_date_from_parquet", reader if variant == "candidate" else double_read), \
                        patch.object(market_status, "_feature_files", inventory if variant == "candidate" else lambda root: tuple(root.glob(f"*{market_status.FEATURE_SUFFIX}"))):
                    started = time.perf_counter()
                    modes = list(pool.map(check, cfgs))
                    samples.append({"repeat": repeat, "variant": variant,
                                    "all_modes_status_checked_ms": (time.perf_counter() - started) * 1000,
                                    "modes": modes})
    payload = {"scope": "read_only_prewarmed_complete_runtime_checks_not_live_opening_or_PIT",
               "production_writes": False, "network_requests": 0, "observed_clock": clock.isoformat(),
               "mode_count": len(cfgs), "contracts": contracts, "warmup_samples": warmup,
               "all_samples_retained": True,
               "variant_median_ms": {variant: statistics.median(row["all_modes_status_checked_ms"]
                                                                for row in samples if row["variant"] == variant)
                                     for variant in ("control", "candidate")},
               "samples": samples}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in payload.items() if key != "samples"}, ensure_ascii=False, indent=2))


def _digest(status) -> str:
    return hashlib.sha256(json.dumps(asdict(status), ensure_ascii=False, sort_keys=True,
                                     default=str).encode()).hexdigest()


if __name__ == "__main__":
    main()
