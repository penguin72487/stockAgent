#!/usr/bin/env python3
"""Download official TAIFEX futures files and build the day-session target.

Annual ZIP files are used for completed years.  The current year is downloaded
in calendar-month chunks because TAIFEX limits the daily CSV endpoint to one
month per request. Raw receipts remain immutable inputs to the normalized
all-contract parquet.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, timedelta
import json
from pathlib import Path
import re
import sys
import time
from typing import Final

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_index_futures import (  # noqa: E402
    TAIFEX_ALL_FUTURES_DAILY_CONTRACT_VERSION,
    TAIFEX_FUTURES_DATA_CONTRACT_VERSION,
    TAIFEX_INDEX_FUTURES_PRODUCTS,
    build_taifex_all_futures_daily_sessions,
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
_ANNUAL_RECEIPT_RE: Final[re.Pattern[str]] = re.compile(r"^(\d{4})_fut\.zip$")
_RANGE_RECEIPT_RE: Final[re.Pattern[str]] = re.compile(
    r"^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_all\.csv$"
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


def _select_rebuild_receipts(raw_dir: Path) -> list[Path]:
    annual: dict[int, Path] = {}
    latest_range: dict[tuple[int, int], tuple[date, Path]] = {}
    for path in sorted([*raw_dir.rglob("*.zip"), *raw_dir.rglob("*.csv")]):
        annual_match = _ANNUAL_RECEIPT_RE.fullmatch(path.name)
        if annual_match is not None:
            year = int(annual_match.group(1))
            if year in annual:
                raise ValueError(f"duplicate annual TAIFEX receipt for {year}")
            annual[year] = path
            continue
        range_match = _RANGE_RECEIPT_RE.fullmatch(path.name)
        if range_match is None:
            continue
        start = date.fromisoformat(range_match.group(1))
        end = date.fromisoformat(range_match.group(2))
        if end < start or (end.year, end.month) != (start.year, start.month):
            raise ValueError(f"invalid monthly TAIFEX receipt range: {path.name}")
        key = (start.year, start.month)
        previous = latest_range.get(key)
        if previous is None or end > previous[0]:
            latest_range[key] = (end, path)
    selected = [annual[year] for year in sorted(annual)]
    selected.extend(latest_range[key][1] for key in sorted(latest_range))
    return selected


def _all_futures_quality(path: Path) -> dict[str, object]:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    metadata = parquet.schema_arrow.metadata or {}
    expected_contract = str(TAIFEX_ALL_FUTURES_DAILY_CONTRACT_VERSION).encode(
        "ascii"
    )
    if metadata.get(b"stockagent.contract_version") != expected_contract:
        raise ValueError(f"{path} has unsupported all-futures contract metadata")
    products: set[str] = set()
    sessions: Counter[str] = Counter()
    series: Counter[str] = Counter()
    first_date: date | None = None
    last_date: date | None = None
    rows = 0
    for batch in parquet.iter_batches(
        columns=["date", "product", "session", "series_type"],
        batch_size=131_072,
    ):
        payload = batch.to_pydict()
        batch_dates = payload["date"]
        rows += len(batch_dates)
        if batch_dates:
            batch_first = min(batch_dates)
            batch_last = max(batch_dates)
            first_date = (
                batch_first if first_date is None else min(first_date, batch_first)
            )
            last_date = (
                batch_last if last_date is None else max(last_date, batch_last)
            )
        products.update(str(value) for value in payload["product"])
        sessions.update(str(value) for value in payload["session"])
        series.update(str(value) for value in payload["series_type"])
    if rows != parquet.metadata.num_rows or not rows:
        raise ValueError(
            f"{path} row-count validation failed: {rows} != "
            f"{parquet.metadata.num_rows}"
        )
    return {
        "rows": rows,
        "first_date": first_date.isoformat() if first_date else None,
        "last_date": last_date.isoformat() if last_date else None,
        "product_count": len(products),
        "products": sorted(products),
        "session_counts": dict(sorted(sessions.items())),
        "series_type_counts": dict(sorted(series.items())),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="data_tw_index_futures",
        help="Raw receipts and normalized parquet root.",
    )
    parser.add_argument("--start-year", type=int, default=1998)
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
        try:
            receipts = _select_rebuild_receipts(raw_dir)
        except ValueError as exc:
            parser.error(str(exc))
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
            was_cached = target.is_file() and target.stat().st_size > 0
            receipts.append(
                _download(
                    {"down_type": "2", "his_year": str(year)},
                    target,
                    attempts=args.attempts,
                    request_interval=args.request_interval,
                )
            )
            _validate_receipt(receipts[-1])
            if not was_cached:
                time.sleep(args.request_interval)

        current_start = date(args.end_date.year, 1, 1)
        for range_start, range_end in _month_ranges(current_start, args.end_date):
            target = raw_dir / "ranges" / (
                f"{range_start.isoformat()}_{range_end.isoformat()}_all.csv"
            )
            was_cached = target.is_file() and target.stat().st_size > 0
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
            if not was_cached:
                time.sleep(args.request_interval)

    normalized = output_dir / "day_session_contracts.parquet"
    build_taifex_index_futures_day_session(
        receipts,
        normalized,
        products=TAIFEX_INDEX_FUTURES_PRODUCTS,
    )
    all_futures = output_dir / "all_futures_daily_sessions.parquet"
    build_taifex_all_futures_daily_sessions(receipts, all_futures)
    all_futures_quality = _all_futures_quality(all_futures)
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
        "all_futures_daily": {
            "contract_version": TAIFEX_ALL_FUTURES_DAILY_CONTRACT_VERSION,
            "path": str(all_futures),
            "sha256": _sha256(all_futures),
            "session_policy": "source_sessions_separate",
            "legacy_session_policy": "day_only_unreported",
            "quality": all_futures_quality,
        },
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
        f"built {normalized} and {all_futures} from {len(receipts)} official "
        f"receipt(s); all-futures rows={all_futures_quality['rows']:,}, "
        f"products={all_futures_quality['product_count']:,}, "
        f"coverage={all_futures_quality['first_date']}.."
        f"{all_futures_quality['last_date']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
