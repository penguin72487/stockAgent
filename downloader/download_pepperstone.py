from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import UTC, datetime
import json
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import atomic_write_text, resolve_end_date

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
    parser.add_argument(
        "--mode",
        choices=["download", "repair", "daily-update"],
        default="download",
        help="download: full fetch; repair/daily-update: incremental refill for stale or missing files.",
    )
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
    if args.limit is not None:
        cmd.extend(["--limit", str(args.limit)])

    print(f"[pepperstone] group={group} mode={args.mode} output={output_dir}")
    subprocess.run(cmd, check=True, cwd=str(repo_root))


def _write_root_summary(output_root: Path, *, selected_groups: list[str]) -> dict[str, object]:
    """Aggregate existing group receipts without duplicating their data files."""

    status_counts: Counter[str] = Counter()
    group_rows: list[dict[str, object]] = []
    data_dates: list[str] = []
    checked_dates: list[str] = []
    for group in GROUP_CONFIG:
        group_root = output_root / group
        summary_path = group_root / "download_summary.json"
        report_path = group_root / "download_report.csv"
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(summary, dict):
            continue
        group_statuses = summary.get("status_counts")
        if isinstance(group_statuses, dict):
            for status, count in group_statuses.items():
                status_counts[str(status)] += int(count)
        try:
            with report_path.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    if row.get("last_date"):
                        data_dates.append(str(row["last_date"]))
                    if row.get("checked_through_date"):
                        checked_dates.append(str(row["checked_through_date"]))
        except (OSError, UnicodeError, csv.Error):
            pass
        group_rows.append(
            {
                "group": group,
                "summary_path": str(summary_path),
                "symbol_count": int(summary.get("symbol_count") or 0),
                "row_count": int(summary.get("row_count") or 0),
                "status_counts": group_statuses if isinstance(group_statuses, dict) else {},
            }
        )
    observed = datetime.now(UTC)
    missing_groups = sorted(set(GROUP_CONFIG) - {str(row["group"]) for row in group_rows})
    failure_count = sum(
        count
        for status, count in status_counts.items()
        if any(token in status.lower() for token in ("fail", "error", "mismatch"))
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "asset_class": "pepperstone_grouped_public_market_data",
        "state": "complete" if not missing_groups and not failure_count else "partial",
        "generated_at_utc": observed.isoformat(),
        "selected_groups": list(selected_groups),
        "registered_groups": list(GROUP_CONFIG),
        "completed_group_count": len(group_rows),
        "missing_groups": missing_groups,
        "symbol_count": sum(int(row["symbol_count"]) for row in group_rows),
        "row_count": sum(int(row["row_count"]) for row in group_rows),
        "status_counts": dict(sorted(status_counts.items())),
        "failed_count": failure_count,
        "end_date": max(data_dates) if data_dates else None,
        "checked_through_date": max(checked_dates) if checked_dates else None,
        "groups": group_rows,
    }
    atomic_write_text(
        output_root / "download_summary.json",
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )
    return payload


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    groups = _resolve_groups(args.groups)

    for group in groups:
        _run_group(repo_root, args, group)

    output_root = repo_root / args.output_root
    summary = _write_root_summary(output_root, selected_groups=groups)
    print(
        f"[pepperstone] completed groups={groups} root={output_root} "
        f"state={summary['state']} symbols={summary['symbol_count']}"
    )


if __name__ == "__main__":
    main()
