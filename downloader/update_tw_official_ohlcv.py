from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh official TWSE/TPEx OHLCV data and backfill data_yahoo/tw_stocks parquet files."
    )
    parser.add_argument("--public-output-dir", default="data_tw_public")
    parser.add_argument("--symbols-root", default="data_yahoo/tw_stocks")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--public-workers", type=int, default=2)
    parser.add_argument("--date-workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-backoff", type=float, default=1.0)
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--flush-every-dates", type=int, default=250)
    parser.add_argument("--backfill-workers", type=int, default=8)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--skip-public-download", action="store_true")
    parser.add_argument("--skip-raw", action="store_true")
    return parser.parse_args()


def _run(command: list[str]) -> None:
    print("[tw-official-update] run " + " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def main() -> None:
    args = parse_args()
    if not args.skip_public_download:
        command = [
            sys.executable,
            "downloader/download_tw_public_data.py",
            "--mode",
            "daily-update",
            "--datasets",
            "twse_daily_ohlcv",
            "tpex_daily_ohlcv",
            "--output-dir",
            args.public_output_dir,
            "--end-date",
            args.end_date,
            "--workers",
            str(args.public_workers),
            "--date-workers",
            str(args.date_workers),
            "--timeout",
            str(args.timeout),
            "--retries",
            str(args.retries),
            "--retry-backoff",
            str(args.retry_backoff),
            "--sleep",
            str(args.sleep),
            "--flush-every-dates",
            str(args.flush_every_dates),
        ]
        if args.skip_raw:
            command.append("--skip-raw")
        _run(command)

    command = [
        sys.executable,
        "downloader/backfill_tw_public_to_yahoo.py",
        "--input-dir",
        args.public_output_dir,
        "--symbols-root",
        args.symbols_root,
        "--workers",
        str(args.backfill_workers),
    ]
    if args.start_date:
        command.extend(["--start-date", args.start_date])
    _run(command)


if __name__ == "__main__":
    main()
