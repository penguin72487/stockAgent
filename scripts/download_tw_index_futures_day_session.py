#!/usr/bin/env python3
"""Download official TAIFEX futures files and build the day-session target.

Annual ZIP files are used for completed years.  The current year is downloaded
in calendar-month chunks because TAIFEX limits the daily CSV endpoint to one
month per request.  Raw receipts remain immutable inputs to the normalized
front-month parquet.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Final
from urllib import parse, request
import zipfile

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_index_futures import (  # noqa: E402
    TAIFEX_INDEX_FUTURES_PRODUCTS,
    build_taifex_index_futures_day_session,
)


TAIFEX_DOWNLOAD_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/futDataDown"
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected ISO date YYYY-MM-DD, got {value!r}"
        ) from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _download(
    payload: dict[str, str],
    target: Path,
    *,
    attempts: int,
    request_interval: float,
) -> Path:
    if target.is_file() and target.stat().st_size > 0:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = parse.urlencode(payload).encode("ascii")
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            http_request = request.Request(
                TAIFEX_DOWNLOAD_URL,
                data=encoded,
                headers={
                    "User-Agent": "stockAgent/taifex-day-session-research",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                method="POST",
            )
            with request.urlopen(http_request, timeout=120) as response:
                body = response.read()
                disposition = str(response.headers.get("Content-Disposition") or "")
            if len(body) < 100 or "attachment" not in disposition.casefold():
                raise RuntimeError(
                    "TAIFEX response was not a downloadable attachment"
                )
            with tempfile.NamedTemporaryFile(
                dir=target.parent,
                prefix=target.name + ".",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(target)
            return target
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(max(request_interval, float(attempt)))
    raise RuntimeError(
        f"failed to download {target.name} after {attempts} attempts"
    ) from last_error


def _month_ranges(start: date, end: date):
    current = start.replace(day=1)
    while current <= end:
        next_month = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
        range_start = max(start, current)
        range_end = min(end, next_month - timedelta(days=1))
        yield range_start, range_end
        current = next_month


def _validate_receipt(path: Path) -> None:
    if path.suffix.lower() == ".zip":
        if not zipfile.is_zipfile(path):
            raise ValueError(f"downloaded annual file is not a ZIP: {path}")
        with zipfile.ZipFile(path) as archive:
            csv_members = [
                name for name in archive.namelist() if name.lower().endswith(".csv")
            ]
            if not csv_members:
                raise ValueError(f"annual ZIP contains no CSV: {path}")
    elif path.suffix.lower() == ".csv":
        with path.open("rb") as handle:
            header = handle.readline()
        if b"," not in header or len(header) < 40:
            raise ValueError(f"downloaded range file has no CSV header: {path}")
    else:
        raise ValueError(f"unsupported receipt: {path}")


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

    normalized = output_dir / "day_session_front_month.parquet"
    build_taifex_index_futures_day_session(
        receipts,
        normalized,
        products=TAIFEX_INDEX_FUTURES_PRODUCTS,
    )
    manifest = {
        "dataset": "tw_index_futures_day_session_front_month",
        "products": list(TAIFEX_INDEX_FUTURES_PRODUCTS),
        "session": "一般",
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
