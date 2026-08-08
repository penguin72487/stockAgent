#!/usr/bin/env python3
"""Download official TAIFEX option daily history and build monthly ATM pairs.

Completed calendar years use the official annual ZIP archive.  The current
year uses one-month daily requests because the TAIFEX endpoint limits each
query to a month.  Raw receipts are immutable and SHA-256 recorded before the
normalized research dataset is rebuilt.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
import sys
import time
from typing import Final

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.taifex_daily_download_common import (  # noqa: E402
    atomic_write_json,
    download_taifex_attachment,
    month_ranges,
    parse_iso_date,
    sha256_path,
    validate_taifex_receipt,
)
from stockagent.data.tw_index_options_daily import (  # noqa: E402
    TAIFEX_OPTION_SERIES_SCOPES,
    TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
    TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
    build_taifex_opening_atm_straddles,
    load_taifex_opening_atm_straddles,
)


TAIFEX_OPTION_DAILY_PAGE: Final[str] = (
    "https://www.taifex.com.tw/cht/3/optDailyMarketView"
)
TAIFEX_OPTION_DOWNLOAD_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/optDataDown"
)


def _parse_date(value: str) -> date:
    try:
        return parse_iso_date(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected ISO date YYYY-MM-DD, got {value!r}"
        ) from exc


def _download(
    payload: dict[str, str],
    target: Path,
    *,
    attempts: int,
    request_interval: float,
) -> Path:
    return download_taifex_attachment(
        TAIFEX_OPTION_DOWNLOAD_URL,
        payload,
        target,
        attempts=attempts,
        request_interval=request_interval,
        user_agent="stockAgent/taifex-option-daily-research",
    )


def _quality_summary(normalized: Path, *, series_scope: str) -> dict[str, object]:
    table = load_taifex_opening_atm_straddles(
        normalized,
        expected_series_scope=series_scope,
    )
    frame = table.select(
        [
            "date",
            "executable",
            "exclusion_reason",
            "option_series",
            "strike",
        ]
    ).to_pandas()
    dates = frame["date"].astype(str)
    duplicate_dates = int(dates.duplicated().sum())
    reasons: Counter[str] = Counter()
    for raw in frame.loc[~frame["executable"], "exclusion_reason"].dropna():
        reasons.update(str(raw).split("|"))
    executable = int(frame["executable"].sum())
    rows = int(len(frame))
    return {
        "rows": rows,
        "first_date": dates.min() if rows else None,
        "last_date": dates.max() if rows else None,
        "duplicate_dates": duplicate_dates,
        "executable_rows": executable,
        "excluded_rows": rows - executable,
        "executable_share": executable / rows if rows else 0.0,
        "exclusion_reason_counts": dict(sorted(reasons.items())),
        "series_scope": series_scope,
        "series_identity_valid": bool(
            frame["option_series"]
            .dropna()
            .astype(str)
            .str.fullmatch(
                r"\d{6}" if series_scope == "monthly" else r"\d{6}[WF][1-5]"
            )
            .all()
        ),
        "selected_strike_rows": int(frame["strike"].notna().sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="data_tw_index_options_daily",
        help="Raw receipts and normalized parquet root.",
    )
    parser.add_argument(
        "--futures-path",
        default="data_tw_index_futures/day_session_front_month.parquet",
        help="Official normalized front-month TX day-session parquet.",
    )
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument(
        "--series-scope",
        choices=TAIFEX_OPTION_SERIES_SCOPES,
        default="monthly",
        help="Monthly series or nearest-expiry weekly series.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=date.today() - timedelta(days=1),
        help="Last completed candidate session (default: yesterday).",
    )
    parser.add_argument("--request-interval", type=float, default=1.0)
    parser.add_argument("--attempts", type=int, default=3)
    args = parser.parse_args()

    if args.start_year < 2001:
        parser.error("--start-year cannot precede the listed TAIFEX option archive (2001)")
    if args.end_date.year < args.start_year:
        parser.error("--end-date precedes --start-year")
    if args.request_interval < 0.0:
        parser.error("--request-interval must be non-negative")
    if args.attempts < 1:
        parser.error("--attempts must be positive")

    output_dir = Path(args.output_dir).expanduser().resolve()
    futures_path = Path(args.futures_path).expanduser().resolve()
    if not futures_path.is_file():
        parser.error(f"futures parquet does not exist: {futures_path}")
    raw_dir = output_dir / "raw"
    receipts: list[Path] = []

    for year in range(args.start_year, args.end_date.year):
        target = raw_dir / "annual" / f"{year}_opt.zip"
        path = _download(
            {"down_type": "2", "his_year": str(year)},
            target,
            attempts=args.attempts,
            request_interval=args.request_interval,
        )
        validate_taifex_receipt(path)
        receipts.append(path)
        print(f"verified annual option receipt {year}: {path.stat().st_size:,} bytes", flush=True)
        time.sleep(args.request_interval)

    current_start = date(args.end_date.year, 1, 1)
    for range_start, range_end in month_ranges(current_start, args.end_date):
        target = raw_dir / "ranges" / (
            f"{range_start.isoformat()}_{range_end.isoformat()}_TXO.csv"
        )
        path = _download(
            {
                "down_type": "1",
                "queryStartDate": range_start.strftime("%Y/%m/%d"),
                "queryEndDate": range_end.strftime("%Y/%m/%d"),
                "commodity_id": "TXO",
                "commodity_id2": "",
            },
            target,
            attempts=args.attempts,
            request_interval=args.request_interval,
        )
        validate_taifex_receipt(path)
        receipts.append(path)
        print(
            f"verified option receipt {range_start}..{range_end}: "
            f"{path.stat().st_size:,} bytes",
            flush=True,
        )
        time.sleep(args.request_interval)

    output_names = {
        "monthly": "monthly_opening_atm_pairs.parquet",
        "weekly": "weekly_nearest_expiry_opening_atm_pairs.parquet",
    }
    dataset_names = {
        "monthly": "taifex_monthly_opening_atm_straddles",
        "weekly": "taifex_nearest_expiry_weekly_opening_atm_straddles",
    }
    normalized = output_dir / output_names[args.series_scope]
    build_taifex_opening_atm_straddles(
        receipts,
        futures_path,
        normalized,
        series_scope=args.series_scope,
    )
    quality = _quality_summary(normalized, series_scope=args.series_scope)
    manifest = {
        "dataset": dataset_names[args.series_scope],
        "contract_version": TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
        "status": "complete",
        "official_page": TAIFEX_OPTION_DAILY_PAGE,
        "official_download_endpoint": TAIFEX_OPTION_DOWNLOAD_URL,
        "product": "TXO",
        "session": "一般",
        "series_scope": (
            "nearest_unexpired_monthly_only"
            if args.series_scope == "monthly"
            else "nearest_expiry_weekly_only"
        ),
        "atm_reference": "official_front_month_TX_day_session_open",
        "price_source": TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
        "price_boundary": (
            "option open/close are each leg's first/last official transaction; "
            "they are not simultaneous executable bid/ask quotes"
        ),
        "start_year": int(args.start_year),
        "end_date": args.end_date.isoformat(),
        "futures_path": str(futures_path),
        "futures_sha256": sha256_path(futures_path),
        "normalized_path": str(normalized),
        "normalized_sha256": sha256_path(normalized),
        "quality": quality,
        "receipts": [
            {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": sha256_path(path),
            }
            for path in receipts
        ],
    }
    manifest_name = "manifest.json" if args.series_scope == "monthly" else "manifest_weekly.json"
    atomic_write_json(output_dir / manifest_name, manifest)
    print(
        f"built {normalized} from {len(receipts)} official receipt(s); "
        f"{quality['executable_rows']:,}/{quality['rows']:,} candidate sessions executable",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
