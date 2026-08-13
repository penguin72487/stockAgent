#!/usr/bin/env python3
"""Download official TAIFEX futures files and build the day-session target.

Annual ZIP files are used for completed years.  The current year is downloaded
in calendar-month chunks because TAIFEX limits the daily CSV endpoint to one
month per request. Raw receipts remain immutable inputs to the normalized
all-contract parquet.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import json
from pathlib import Path
import sys
import time
from typing import Final

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_index_futures import (  # noqa: E402
    TAIFEX_FUTURES_DATA_CONTRACT_VERSION,
    TAIFEX_INDEX_FUTURES_PRODUCTS,
    build_taifex_index_futures_day_session,
)
from scripts.taifex_daily_download_common import (  # noqa: E402
    download_taifex_attachment,
    month_ranges,
    parse_iso_date,
    sha256_path,
    validate_taifex_receipt,
)


TAIFEX_DOWNLOAD_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/futDataDown"
)


def _parse_date(value: str) -> date:
    try:
        return parse_iso_date(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected ISO date YYYY-MM-DD, got {value!r}"
        ) from exc


def _sha256(path: Path) -> str:
    return sha256_path(path)


def _download(
    payload: dict[str, str],
    target: Path,
    *,
    attempts: int,
    request_interval: float,
) -> Path:
    return download_taifex_attachment(
        TAIFEX_DOWNLOAD_URL,
        payload,
        target,
        attempts=attempts,
        request_interval=request_interval,
        user_agent="stockAgent/taifex-day-session-research",
    )


def _month_ranges(start: date, end: date):
    yield from month_ranges(start, end)


def _validate_receipt(path: Path) -> None:
    validate_taifex_receipt(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="data_tw_index_futures",
        help="Raw receipts and normalized parquet root.",
    )
    parser.add_argument("--start-year", type=int, default=2005)
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=date.today() - timedelta(days=1),
        help="Last completed candidate session (default: yesterday).",
    )
    parser.add_argument("--request-interval", type=float, default=1.0)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument(
        "--rebuild-normalized-only",
        action="store_true",
        help=(
            "Do not access the network; validate every existing raw ZIP/CSV "
            "receipt and rebuild the normalized v2 contract parquet."
        ),
    )
    args = parser.parse_args()

    if args.start_year < 1998:
        parser.error("--start-year cannot precede the TAIFEX archive (1998)")
    if args.end_date.year < args.start_year:
        parser.error("--end-date precedes --start-year")
    if args.request_interval < 0.0:
        parser.error("--request-interval must be non-negative")
    if args.attempts < 1:
        parser.error("--attempts must be positive")

    output_dir = Path(args.output_dir).expanduser().resolve()
    raw_dir = output_dir / "raw"
    receipts: list[Path] = []
    if args.rebuild_normalized_only:
        receipts = sorted(
            [*raw_dir.rglob("*.zip"), *raw_dir.rglob("*.csv")],
            key=lambda path: str(path),
        )
        if not receipts:
            parser.error(
                "--rebuild-normalized-only found no raw ZIP/CSV receipts under "
                f"{raw_dir}"
            )
        for receipt in receipts:
            _validate_receipt(receipt)
    else:
        for year in range(args.start_year, args.end_date.year):
            target = raw_dir / "annual" / f"{year}_fut.zip"
            receipts.append(
                _download(
                    {"down_type": "2", "his_year": str(year)},
                    target,
                    attempts=args.attempts,
                    request_interval=args.request_interval,
                )
            )
            _validate_receipt(receipts[-1])
            time.sleep(args.request_interval)

        current_start = date(args.end_date.year, 1, 1)
        for range_start, range_end in _month_ranges(current_start, args.end_date):
            target = raw_dir / "ranges" / (
                f"{range_start.isoformat()}_{range_end.isoformat()}_all.csv"
            )
            receipts.append(
                _download(
                    {
                        "down_type": "1",
                        "queryStartDate": range_start.strftime("%Y/%m/%d"),
                        "queryEndDate": range_end.strftime("%Y/%m/%d"),
                        "commodity_id": "all",
                        "commodity_id2": "",
                    },
                    target,
                    attempts=args.attempts,
                    request_interval=args.request_interval,
                )
            )
            _validate_receipt(receipts[-1])
            time.sleep(args.request_interval)

    normalized = output_dir / "day_session_contracts.parquet"
    build_taifex_index_futures_day_session(
        receipts,
        normalized,
        products=TAIFEX_INDEX_FUTURES_PRODUCTS,
    )
    manifest = {
        "dataset": "tw_index_futures_day_session_contracts",
        "contract_version": TAIFEX_FUTURES_DATA_CONTRACT_VERSION,
        "products": list(TAIFEX_INDEX_FUTURES_PRODUCTS),
        "session": "一般",
        "front_month_policy": "nearest_unexpired_monthly",
        "rolling_benchmark": "1x_long_front_month_gross",
        "roll_timing": "preceding_session_close",
        "roll_gap_treatment": "same_contract_close_to_close",
        "start_year": int(args.start_year),
        "end_date": args.end_date.isoformat(),
        "normalized_path": str(normalized),
        "normalized_sha256": _sha256(normalized),
        "receipts": [
            {
                "path": str(path),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in receipts
        ],
    }
    manifest_path = output_dir / "manifest.json"
    temporary_manifest = manifest_path.with_suffix(".json.tmp")
    temporary_manifest.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_manifest.replace(manifest_path)
    print(
        f"built {normalized} from {len(receipts)} official receipt(s)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
