#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live.signal_engine import generate_live_signal
from stockagent.live.market_config import load_market_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a live stockAgent portfolio signal.")
    parser.add_argument("--market-config", default=None, help="Per-market YAML config file.")
    parser.add_argument("--market", default=None, help="Market id for output namespace/message.")
    parser.add_argument("--market-label", default=None, help="Human-readable market label.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--live-output-dir", default=None)
    parser.add_argument("--fold-id", type=int, default=None, help="Fold checkpoint to use. Defaults to latest fold with checkpoint_best.pt.")
    parser.add_argument("--checkpoint", default=None, help="Explicit checkpoint path.")
    parser.add_argument("--weights-path", default=None, help="Previous daily_weights table. Defaults to the selected fold output.")
    parser.add_argument("--panel-date", default=None, help="Panel date to use for model features, or latest.")
    parser.add_argument("--asof-date", default=None, help="Signal date label. Defaults to panel date.")
    parser.add_argument("--price-source", choices=("panel", "csv", "yahoo"), default=None)
    parser.add_argument("--prices-csv", default=None, help="CSV with symbol/code/ticker and price/close/last columns.")
    parser.add_argument("--yahoo-chunk-size", type=int, default=None)
    parser.add_argument("--device", default=None, help="Override config environment.device, e.g. cuda or cpu.")
    parser.add_argument("--top-n", type=int, default=None)
    parser.add_argument("--min-abs-delta", type=float, default=None)
    parser.add_argument("--signal-id", default=None)
    parser.add_argument("--write", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--json", action="store_true", help="Print summary JSON instead of Discord message.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    overrides = {
        "market": args.market,
        "market_label": args.market_label,
        "config_path": args.config,
        "output_dir": args.output_dir,
        "live_output_dir": args.live_output_dir,
        "fold_id": args.fold_id,
        "checkpoint_path": args.checkpoint,
        "weights_path": args.weights_path,
        "panel_date": args.panel_date,
        "asof_date": args.asof_date,
        "price_source": args.price_source,
        "prices_csv": args.prices_csv,
        "yahoo_chunk_size": args.yahoo_chunk_size,
        "device": args.device,
        "top_n": args.top_n,
        "min_abs_delta": args.min_abs_delta,
        "signal_id": args.signal_id,
        "write": args.write,
    }
    if args.market_config:
        market_cfg = load_market_config(args.market_config)
        kwargs = market_cfg.signal_kwargs(**overrides)
    else:
        kwargs = {key: value for key, value in overrides.items() if value is not None}
        kwargs.setdefault("config_path", "configs/markets/tw.yaml")
        kwargs.setdefault("panel_date", "latest")
        kwargs.setdefault("price_source", "panel")
        kwargs.setdefault("yahoo_chunk_size", 80)
        kwargs.setdefault("top_n", 20)
        kwargs.setdefault("min_abs_delta", 0.001)
    result = generate_live_signal(**kwargs)
    if args.json:
        print(json.dumps(result.summary, ensure_ascii=False, indent=2))
    else:
        print(result.message)
        if result.output_dir:
            print(f"\noutput_dir={result.output_dir}")


if __name__ == "__main__":
    main()
