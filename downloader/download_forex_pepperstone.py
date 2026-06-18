from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from common import resolve_end_date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Pepperstone-like forex data using the Yahoo forex pipeline."
    )
    parser.add_argument(
        "--mode",
        choices=["download", "repair", "daily-update"],
        default="download",
        help="download: fetch full history; repair/daily-update: refill missing/stale local files incrementally.",
    )
    parser.add_argument("--start-date", default="2000-01-01", help="Inclusive start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", default="today", help="Inclusive end date in YYYY-MM-DD, or 'today'.")
    parser.add_argument("--output-dir", default="data_yahoo/forex_pepperstone", help="Output folder for Pepperstone forex parquet files.")
    parser.add_argument("--workers", type=int, default=12, help="Parallel symbol workers.")
    parser.add_argument("--retries", type=int, default=2, help="Retries per symbol when Yahoo temporarily fails.")
    parser.add_argument("--refresh", action="store_true", help="Re-download even if parquet exists.")
    parser.add_argument(
        "--symbols-file",
        default="configs/forex_pepperstone_pairs.txt",
        help="One pair per line (e.g., EURUSD). Defaults to Pepperstone pair list.",
    )
    parser.add_argument(
        "--repair-overlap-days",
        type=int,
        default=7,
        help="When --mode repair, overlap days before local last date.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    yahoo_downloader = Path(__file__).resolve().with_name("download_yahoo_ohlcv.py")
    symbols_file = (repo_root / args.symbols_file).resolve() if not Path(args.symbols_file).is_absolute() else Path(args.symbols_file)
    output_dir = (repo_root / args.output_dir).resolve() if not Path(args.output_dir).is_absolute() else Path(args.output_dir)

    if not symbols_file.exists():
        raise FileNotFoundError(f"Symbols file not found: {symbols_file}")

    cmd = [
        sys.executable,
        str(yahoo_downloader),
        "--mode",
        args.mode,
        "--asset",
        "forex",
        "--start-date",
        args.start_date,
        "--end-date",
        resolve_end_date(args.end_date),
        "--output-dir",
        str(output_dir),
        "--workers",
        str(args.workers),
        "--retries",
        str(args.retries),
        "--symbols-file",
        str(symbols_file),
        "--repair-overlap-days",
        str(args.repair_overlap_days),
    ]
    if args.refresh:
        cmd.append("--refresh")

    subprocess.run(cmd, check=True, cwd=str(repo_root))


if __name__ == "__main__":
    main()
