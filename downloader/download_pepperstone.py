from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

GROUP_CONFIG = {
    "24hTrading": {"asset": "forex", "symbols": "configs/pepperstone_24hTrading_symbols.txt"},
    "commodites": {"asset": "us_stocks", "symbols": "configs/pepperstone_commodites_symbols.txt"},
    "crypto": {"asset": "crypto", "symbols": "configs/pepperstone_crypto_symbols.txt"},
    "fores": {"asset": "forex", "symbols": "configs/pepperstone_fores_symbols.txt"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Pepperstone grouped data to data_peperstone/{24hTrading,commodites,crypto,fores}."
    )
    parser.add_argument("--mode", choices=["download", "repair"], default="download")
    parser.add_argument("--output-root", default="data_peperstone", help="Root output folder.")
    parser.add_argument("--start-date", default="2000-01-01", help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end-date", default="today", help="Inclusive end date YYYY-MM-DD or 'today'")
    parser.add_argument("--workers", type=int, default=12, help="Parallel symbol workers per group")
    parser.add_argument("--retries", type=int, default=2, help="Retries per symbol")
    parser.add_argument("--refresh", action="store_true", help="Re-download even if parquet exists")
    parser.add_argument("--repair-overlap-days", type=int, default=7, help="Overlap days for repair mode")
    parser.add_argument(
        "--groups",
        nargs="+",
        choices=list(GROUP_CONFIG.keys()) + ["all"],
        default=["all"],
        help="Target groups. Default all.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit passed through to the base downloader for quick test runs.",
    )
    return parser.parse_args()


def _resolve_end_date(value: str) -> str:
    text = value.strip().lower()
    if text in {"today", "now"}:
        return date.today().isoformat()
    return value.strip()


def _resolve_groups(values: list[str]) -> list[str]:
    if not values or "all" in values:
        return list(GROUP_CONFIG.keys())
    return values


def _run_group(repo_root: Path, args: argparse.Namespace, group: str) -> None:
    base_downloader = repo_root / "downloader" / "download_yahoo_ohlcv.py"
    config = GROUP_CONFIG[group]
    symbols_file = repo_root / config["symbols"]
    output_dir = repo_root / args.output_root / group
    output_dir.mkdir(parents=True, exist_ok=True)

    if not symbols_file.exists():
        raise FileNotFoundError(f"Symbols file not found: {symbols_file}")

    cmd = [
        sys.executable,
        str(base_downloader),
        "--mode",
        args.mode,
        "--asset",
        config["asset"],
        "--start-date",
        args.start_date,
        "--end-date",
        _resolve_end_date(args.end_date),
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
    if args.limit is not None:
        cmd.extend(["--limit", str(args.limit)])

    print(f"[pepperstone] group={group} mode={args.mode} output={output_dir}")
    subprocess.run(cmd, check=True, cwd=str(repo_root))


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    groups = _resolve_groups(args.groups)

    for group in groups:
        _run_group(repo_root, args, group)

    print(f"[pepperstone] completed groups={groups} root={(repo_root / args.output_root)}")


if __name__ == "__main__":
    main()
