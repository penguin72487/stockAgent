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
import csv
from datetime import date, datetime, time as datetime_time, timedelta
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
from typing import Final
from zoneinfo import ZoneInfo

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
from stockagent.data.tw_customized_futures_daily import (  # noqa: E402
    CUSTOMIZED_DAILY_SOURCE_CONTRACT,
    iter_customized_daily_rows,
)
from downloader.dataset_lock import exclusive_dataset_lock  # noqa: E402
from scripts.taifex_daily_download_common import (  # noqa: E402
    atomic_write_json,
    download_taifex_attachment,
    month_ranges,
    parse_iso_date,
    sha256_path,
    validate_taifex_receipt,
)


TAIFEX_DOWNLOAD_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/futDataDown"
)
TAIFEX_CUSTOMIZED_DOWNLOAD_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/futDataDownFlx"
)
TAIFEX_CUSTOMIZED_FIRST_DATE: Final[date] = date(2024, 1, 22)
_ANNUAL_RECEIPT_RE: Final[re.Pattern[str]] = re.compile(r"^(\d{4})_fut\.zip$")
_RANGE_RECEIPT_RE: Final[re.Pattern[str]] = re.compile(
    r"^(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_all\.csv$"
)
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")


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


def _customized_source_readback(path: Path, *, start: date, end: date) -> dict:
    digest = _sha256(path)
    rows = list(iter_customized_daily_rows(path, expected_sha256=digest, start=start, end=end))
    products = Counter(row["product"] for row in rows)
    positive, unknown = 0, 0
    for row in rows:
        raw_volume = str(row["raw"]["成交量"]).strip().replace(",", "")
        if re.fullmatch(r"\d+", raw_volume):
            positive += int(raw_volume) > 0
        else:
            unknown += 1
    return {
        "path": str(path.resolve()), "sha256": digest, "bytes": path.stat().st_size,
        "start": start.isoformat(), "end": end.isoformat(), "rows": len(rows),
        "positive_volume_rows": positive, "unknown_volume_rows": unknown,
        "products": dict(sorted(products.items())),
        "status": "rows_observed" if rows else "header_only_observed",
    }


def _download_customized_history(
    root: Path, *, start: date, end: date, attempts: int, request_interval: float,
) -> Path:
    """Extend the same official owner for the distinct FLEX daily endpoint."""
    if start < TAIFEX_CUSTOMIZED_FIRST_DATE or start > end:
        raise ValueError("customized history must start on/after 2024-01-22 and precede the end")
    with exclusive_dataset_lock(root / "state" / "download.lock",
                                provider="taifex_customized_futures_daily", timeout_seconds=0):
        return _download_customized_history_locked(
            root, start=start, end=end, attempts=attempts, request_interval=request_interval,
        )


def _download_customized_history_locked(
    root: Path, *, start: date, end: date, attempts: int, request_interval: float,
) -> Path:
    sources = []
    for first, last in _month_ranges(start, end):
        target = root / "raw" / f"{first.isoformat()}_{last.isoformat()}_all.csv"
        receipt_path = target.with_suffix(".receipt.json")
        if target.exists() and not receipt_path.is_file():
            # A stopped process can leave data before publishing its receipt.
            # Preserve that unverified response and refetch; never adopt it.
            quarantine = root / "quarantined"
            quarantine.mkdir(parents=True, exist_ok=True)
            orphan = quarantine / f"{target.stem}.{_sha256(target)}.{time.time_ns()}.raw"
            os.replace(target, orphan)
        if target.exists():
            old = json.loads(receipt_path.read_text(encoding="utf-8"))
            if (old.get("contract") != CUSTOMIZED_DAILY_SOURCE_CONTRACT
                    or old.get("url") != TAIFEX_CUSTOMIZED_DOWNLOAD_URL
                    or old.get("source", {}).get("sha256") != _sha256(target)
                    or old.get("source", {}).get("start") != first.isoformat()
                    or old.get("source", {}).get("end") != last.isoformat()):
                raise ValueError(f"customized raw source receipt mismatch: {target}")
        else:
            payload = {
                "down_type": "1", "queryStartDate": first.strftime("%Y/%m/%d"),
                "queryEndDate": last.strftime("%Y/%m/%d"),
                "commodity_id": "all", "commodity_id2": "",
            }
            download_taifex_attachment(
                TAIFEX_CUSTOMIZED_DOWNLOAD_URL, payload, target, attempts=attempts,
                request_interval=request_interval, user_agent="stockAgent/taifex-customized-daily-research",
                cooldown_after_download=True,
            )
            readback = _customized_source_readback(target, start=first, end=last)
            atomic_write_json(receipt_path, {
                "contract": CUSTOMIZED_DAILY_SOURCE_CONTRACT,
                "url": TAIFEX_CUSTOMIZED_DOWNLOAD_URL,
                "retrieved_at": datetime.now(TAIPEI).isoformat(), "source": readback,
            })
        readback = _customized_source_readback(target, start=first, end=last)
        readback["receipt"] = str(receipt_path.resolve())
        sources.append(readback)
        print(json.dumps({"state": "source_readback", "start": first.isoformat(),
                          "end": last.isoformat(), "rows": readback["rows"],
                          "positive_volume_rows": readback["positive_volume_rows"],
                          "status": readback["status"]}, ensure_ascii=False), flush=True)
    manifest = root / "manifest.json"
    atomic_write_json(manifest, {
        "contract": CUSTOMIZED_DAILY_SOURCE_CONTRACT,
        "start": start.isoformat(), "end": end.isoformat(), "sources": sources,
        "rows": sum(item["rows"] for item in sources),
        "positive_volume_rows": sum(item["positive_volume_rows"] for item in sources),
        "scope": "source observations only; not execution, PIT listing or account admission",
    })
    return manifest


def _needs_postclose_refresh(
    path: Path,
    end_date: date,
    *,
    now: datetime | None = None,
) -> bool:
    """Recheck today's file after each observed publication checkpoint.

    An official 16:30 snapshot can omit late-closing product rows. The second
    checkpoint is a reconciliation attempt, not a guarantee that TAIFEX has
    published every row by that time.
    """

    local = (now or datetime.now(TAIPEI)).astimezone(TAIPEI)
    if not path.is_file() or end_date != local.date() or local.time() < datetime_time(16, 30):
        return False
    checkpoint = datetime_time(18, 30) if local.time() >= datetime_time(18, 30) else datetime_time(16, 30)
    checkpoint_time = datetime.combine(end_date, checkpoint, tzinfo=TAIPEI)
    return path.stat().st_mtime < checkpoint_time.timestamp()


def _range_receipt_coverage(
    path: Path,
) -> tuple[set[date], set[tuple[date, str]], set[tuple[date, str, str, str]]]:
    """Read date, index and physical-row identities before replacement."""

    dates: set[date] = set()
    index_days: set[tuple[date, str]] = set()
    physical_rows: set[tuple[date, str, str, str]] = set()
    with path.open("r", encoding="cp950", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"交易日期", "契約", "到期月份(週別)", "交易時段"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError(f"TAIFEX range file lacks required columns: {path}")
        for line_number, row in enumerate(reader, start=2):
            value = str(row.get("交易日期") or "").strip().replace("/", "-")
            try:
                trading_date = date.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(
                    f"TAIFEX range file has invalid trading date at line {line_number}: {path}"
                ) from exc
            dates.add(trading_date)
            product = str(row.get("契約") or "").strip()
            contract = str(row.get("到期月份(週別)") or "").strip()
            session = str(row.get("交易時段") or "").strip()
            if not product or not contract or not session:
                raise ValueError(f"TAIFEX range file has incomplete row identity at line {line_number}: {path}")
            physical_rows.add((trading_date, product, contract, session))
            if product in TAIFEX_INDEX_FUTURES_PRODUCTS and session == "一般":
                index_days.add((trading_date, product))
    if not dates:
        raise ValueError(f"TAIFEX range file contains no dated rows: {path}")
    return dates, index_days, physical_rows


def _retain_rejected_refresh(staged: Path, target: Path) -> Path:
    """Keep a rejected official reply separately for diagnosis."""

    digest = _sha256(staged)
    rejected = target.parent.parent / "quarantined" / f"{target.stem}.{digest}.raw"
    rejected.parent.mkdir(parents=True, exist_ok=True)
    if rejected.exists():
        if _sha256(rejected) != digest:
            raise RuntimeError(f"TAIFEX quarantine hash mismatch: {rejected}")
    else:
        os.replace(staged, rejected)
    return rejected


def _refresh_postclose_range(
    payload: dict[str, str],
    target: Path,
    *,
    attempts: int,
    request_interval: float,
) -> Path:
    """Stage a fresh official reply, preserving any differing early bytes."""

    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=target.parent,
        prefix=f".{target.stem}.postclose.",
        suffix=".csv",
        delete=False,
    ) as handle:
        staged = Path(handle.name)
    try:
        _download(
            payload,
            staged,
            attempts=attempts,
            request_interval=request_interval,
        )
        _validate_receipt(staged)
        old_dates, old_index_days, old_rows = _range_receipt_coverage(target)
        fresh_dates, fresh_index_days, fresh_rows = _range_receipt_coverage(staged)
        if not old_dates.issubset(fresh_dates):
            _retain_rejected_refresh(staged, target)
            raise RuntimeError(
                f"TAIFEX post-close reply loses previously observed trading dates: "
                f"{sorted(old_dates - fresh_dates)}"
            )
        if not old_index_days.issubset(fresh_index_days):
            _retain_rejected_refresh(staged, target)
            raise RuntimeError(
                "TAIFEX post-close reply loses previously observed index day "
                f"sessions: {sorted(old_index_days - fresh_index_days)}"
            )
        if not old_rows.issubset(fresh_rows):
            _retain_rejected_refresh(staged, target)
            missing = sorted(old_rows - fresh_rows)
            raise RuntimeError(
                "TAIFEX post-close reply loses previously observed physical "
                f"contract/session rows: count={len(missing)} examples={missing[:10]}"
            )
        old_hash = _sha256(target)
        if _sha256(staged) != old_hash:
            archive = target.parent.parent / "superseded" / f"{target.stem}.{old_hash}.raw"
            archive.parent.mkdir(parents=True, exist_ok=True)
            if archive.exists():
                if _sha256(archive) != old_hash:
                    raise RuntimeError(f"TAIFEX archive hash mismatch: {archive}")
            else:
                with tempfile.NamedTemporaryFile(
                    dir=archive.parent,
                    prefix=f".{archive.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as handle:
                    archive_temp = Path(handle.name)
                try:
                    with target.open("rb") as source, archive_temp.open("wb") as sink:
                        shutil.copyfileobj(source, sink)
                        sink.flush()
                        os.fsync(sink.fileno())
                    if _sha256(archive_temp) != old_hash:
                        raise RuntimeError("TAIFEX source changed during post-close archive")
                    os.replace(archive_temp, archive)
                finally:
                    archive_temp.unlink(missing_ok=True)
        if _sha256(target) != old_hash:
            raise RuntimeError("TAIFEX source changed before post-close promotion")
        os.replace(staged, target)
        return target
    finally:
        staged.unlink(missing_ok=True)


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


def _all_futures_quality(path: Path, *, include_valuation_rows: bool = False,
                         include_reference_evidence: bool = False) -> dict[str, object]:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    metadata = parquet.schema_arrow.metadata or {}
    if include_reference_evidence and not include_valuation_rows:
        raise ValueError('reference evidence requires reported valuation rows')
    expected_contract = str(3 if include_reference_evidence else
                            (2 if include_valuation_rows else TAIFEX_ALL_FUTURES_DAILY_CONTRACT_VERSION)).encode(
        "ascii"
    )
    if metadata.get(b"stockagent.contract_version") != expected_contract:
        raise ValueError(f"{path} has unsupported all-futures contract metadata")
    if include_reference_evidence:
        for name in ('reported_price_change','reported_price_change_percent'):
            if name not in parquet.schema_arrow.names or str(parquet.schema_arrow.field(name).type) != 'string':
                raise ValueError('reference evidence must preserve source text columns')
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
    parser.add_argument("--customized-only", action="store_true",
                        help="Collect FLEX daily sources separately; do not change standard datasets.")
    parser.add_argument("--customized-start-date", type=_parse_date,
                        default=TAIFEX_CUSTOMIZED_FIRST_DATE)
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
    if args.customized_only:
        if args.rebuild_normalized_only:
            parser.error("--customized-only cannot be combined with --rebuild-normalized-only")
        try:
            manifest = _download_customized_history(
                output_dir / "customized", start=args.customized_start_date,
                end=args.end_date, attempts=args.attempts, request_interval=args.request_interval,
            )
        except ValueError as exc:
            parser.error(str(exc))
        print(f"customized futures source manifest: {manifest}")
        return 0
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
            payload = {
                "down_type": "1",
                "queryStartDate": range_start.strftime("%Y/%m/%d"),
                "queryEndDate": range_end.strftime("%Y/%m/%d"),
                "commodity_id": "all",
                "commodity_id2": "",
            }
            receipts.append(
                _refresh_postclose_range(
                    payload, target, attempts=args.attempts,
                    request_interval=args.request_interval,
                ) if _needs_postclose_refresh(target, range_end) else _download(
                    payload, target, attempts=args.attempts,
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
