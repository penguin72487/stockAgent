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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=8)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("repeats must be positive")
    cfgs = [bot._resolve_market(market) for market in bot._scheduled_markets()
            if bot._resolve_market(market).day_trade_simulation_enabled]
    if not cfgs:
        parser.error("no deployed day-trade modes")
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
