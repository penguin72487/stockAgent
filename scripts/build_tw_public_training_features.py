#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import hashlib
import time
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


def _writer_lock_path(output_path: Path) -> Path:
    target = output_path.resolve()
    lock_key = hashlib.sha256(str(target).encode("utf-8")).hexdigest()[:20]
    return ROOT_DIR / "artifacts" / "data_locks" / f"tw_public_features_{lock_key}.lock"


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
    parser.add_argument(
        "--lock-timeout-seconds", type=float, default=7200.0,
        help="Maximum time to wait for another writer of the same output path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if int(args.incremental_tail_days) < 0:
        raise ValueError("--incremental-tail-days must be non-negative")
    if args.lock_timeout_seconds <= 0:
        raise ValueError("--lock-timeout-seconds must be positive")
    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    if int(args.incremental_tail_days) and end_date is None:
        raise ValueError("--incremental-tail-days requires --end-date")
    incremental_start_date = (
        end_date - timedelta(days=int(args.incremental_tail_days) - 1)
        if end_date is not None and int(args.incremental_tail_days)
        else None
    )
    # The full and incremental paths share one deterministic .tmp filename.
    # Serialize by resolved target so catalog symlinks and absolute paths
    # cannot run competing builders against the same production artifact.
    target = Path(args.output_path).resolve()
    lock_path = _writer_lock_path(Path(args.output_path))
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + float(args.lock_timeout_seconds)
    with lock_path.open("a+") as handle:
        warned = False
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if not warned:
                    print(f"[tw-public-features] waiting for output writer lock: {target}", flush=True)
                    warned = True
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out waiting for TW public feature writer: {target}")
                time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))
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
