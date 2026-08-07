#!/usr/bin/env python3
"""Build the shared causal TX and direct-TXO tick strategy dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    build_index_derivatives_tick_dataset,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-root",
        default="data_tw_index_derivatives_ticks",
        help="Root created by download_taifex_recent_index_derivatives_ticks.py",
    )
    parser.add_argument(
        "--output-root",
        default="data_tw_index_derivatives_ticks/strategy_dataset_bidask",
    )
    parser.add_argument(
        "--execution-price-source",
        choices=("shioaji_bidask", "taifex_next_trade_proxy"),
        default="shioaji_bidask",
    )
    parser.add_argument(
        "--bidask-capture-root",
        default="data_tw_index_derivatives_ticks/shioaji_fop_captures",
    )
    parser.add_argument("--execution-latency-ms", type=float, default=250.0)
    parser.add_argument("--execution-max-wait-ms", type=float, default=1_000.0)
    parser.add_argument("--max-transport-delay-ms", type=float, default=2_000.0)
    parser.add_argument("--session", choices=("day",), default="day")
    parser.add_argument("--decision-interval-seconds", type=int, default=5)
    parser.add_argument("--rolling-window-seconds", type=int, default=60)
    parser.add_argument("--warmup-seconds", type=int, default=300)
    parser.add_argument("--atm-moneyness-fraction", type=float, default=0.01)
    args = parser.parse_args()
    manifest = build_index_derivatives_tick_dataset(
        args.raw_root,
        args.output_root,
        execution_price_source=args.execution_price_source,
        bidask_capture_root=args.bidask_capture_root,
        execution_latency_ms=args.execution_latency_ms,
        execution_max_wait_ms=args.execution_max_wait_ms,
        max_transport_delay_ms=args.max_transport_delay_ms,
        session=args.session,
        decision_interval_seconds=args.decision_interval_seconds,
        rolling_window_seconds=args.rolling_window_seconds,
        warmup_seconds=args.warmup_seconds,
        atm_moneyness_fraction=args.atm_moneyness_fraction,
    )
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "dates": manifest["summary"]["dates"],
                "rows": manifest["summary"]["rows"],
                "option_rows": manifest["summary"]["option_rows"],
                "output_root": args.output_root,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
