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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a live stockAgent portfolio signal.")
    parser.add_argument("--config", default="configs/experiment_baseline.yaml")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--fold-id", type=int, default=None, help="Fold checkpoint to use. Defaults to latest fold with checkpoint_best.pt.")
    parser.add_argument("--checkpoint", default=None, help="Explicit checkpoint path.")
    parser.add_argument("--weights-path", default=None, help="Previous daily_weights table. Defaults to the selected fold output.")
    parser.add_argument("--panel-date", default="latest", help="Panel date to use for model features, or latest.")
    parser.add_argument("--asof-date", default=None, help="Signal date label. Defaults to panel date.")
    parser.add_argument("--price-source", choices=("panel", "csv", "yahoo"), default="panel")
    parser.add_argument("--prices-csv", default=None, help="CSV with symbol/code/ticker and price/close/last columns.")
    parser.add_argument("--yahoo-chunk-size", type=int, default=80)
    parser.add_argument("--device", default=None, help="Override config environment.device, e.g. cuda or cpu.")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--min-abs-delta", type=float, default=0.001)
    parser.add_argument("--write", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--json", action="store_true", help="Print summary JSON instead of Discord message.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = generate_live_signal(
        config_path=args.config,
        output_dir=args.output_dir,
        fold_id=args.fold_id,
        checkpoint_path=args.checkpoint,
        weights_path=args.weights_path,
        panel_date=args.panel_date,
        asof_date=args.asof_date,
        price_source=args.price_source,
        prices_csv=args.prices_csv,
        yahoo_chunk_size=args.yahoo_chunk_size,
        device=args.device,
        top_n=args.top_n,
        min_abs_delta=args.min_abs_delta,
        write=args.write,
    )
    if args.json:
        print(json.dumps(result.summary, ensure_ascii=False, indent=2))
    else:
        print(result.message)
        if result.output_dir:
            print(f"\noutput_dir={result.output_dir}")


if __name__ == "__main__":
    main()
