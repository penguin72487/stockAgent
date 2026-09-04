#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from stockagent.data.tw_public_features import (
    DEFAULT_MARKET_SYMBOL,
    build_tw_public_training_features,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build sparse Taiwan public-data training features for stockAgent panel ingestion."
    )
    parser.add_argument("--input-dir", default="data_tw_public", help="Directory containing downloaded TW public parquet files.")
    parser.add_argument(
        "--output-path",
        default="data_tw_public/features/tw_public_stock_daily.parquet",
        help="Output sparse feature parquet with date/symbol plus numeric feature columns.",
    )
    parser.add_argument(
        "--symbols-root",
        default="data_tw_public/stocks",
        help="Optional *_features.parquet directory used to keep only trainable TW symbols.",
    )
    parser.add_argument("--market-symbol", default=DEFAULT_MARKET_SYMBOL, help="Synthetic symbol for market-wide rows.")
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive completed-session cutoff (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--allow-daily-publication-lag",
        action="store_true",
        help=(
            "Permit only a missing latest-session day-trade eligibility receipt. "
            "No eligibility row is fabricated, so day-trade execution fails closed."
        ),
    )
    parser.add_argument(
        "--incremental-tail-days",
        type=int,
        default=0,
        help=(
            "Rebuild only this many trailing calendar days and stream-copy the "
            "receipt-verified older output. Requires --end-date; an absent or "
            "incompatible prior output automatically falls back to a full build."
        ),
    )
    parser.add_argument("--summary-path", default=None, help="Optional JSON summary path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if int(args.incremental_tail_days) < 0:
        raise ValueError("--incremental-tail-days must be non-negative")
    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    if int(args.incremental_tail_days) and end_date is None:
        raise ValueError("--incremental-tail-days requires --end-date")
    incremental_start_date = (
        end_date - timedelta(days=int(args.incremental_tail_days) - 1)
        if end_date is not None and int(args.incremental_tail_days)
        else None
    )
    result = build_tw_public_training_features(
        input_dir=Path(args.input_dir),
        output_path=Path(args.output_path),
        symbols_root=Path(args.symbols_root) if args.symbols_root else None,
        market_symbol=str(args.market_symbol),
        summary_path=Path(args.summary_path) if args.summary_path else None,
        end_date=end_date,
        allow_daily_publication_lag=bool(args.allow_daily_publication_lag),
        incremental_start_date=incremental_start_date,
    )
    print(
        "[tw-public-features] "
        f"rows={result.rows} stock_rows={result.stock_rows} market_rows={result.market_rows} "
        f"features={result.feature_count} mode={result.build_mode} "
        f"reused_rows={result.reused_rows} output={result.output_path}"
    )


if __name__ == "__main__":
    main()
