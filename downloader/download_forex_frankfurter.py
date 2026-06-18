from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import requests

from common import resolve_end_date, run_parallel_tasks

API_BASE = "https://api.frankfurter.app"
DEFAULT_SYMBOLS_PATH = Path("data_yahoo") / "forex" / "symbols.csv"


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path))


def _write_parquet(frame: pl.DataFrame, path: Path) -> None:
    pq.write_table(frame.to_arrow(), path, compression="snappy", write_statistics=True)

@dataclass(slots=True)
class SymbolRecord:
    code: str
    name: str
    market: str
    base: str
    quote: str


@dataclass(slots=True)
class DownloadResult:
    code: str
    status: str
    rows: int
    output_path: str | None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download forex OHLC-like data from Frankfurter (ECB rates).")
    parser.add_argument(
        "--mode",
        choices=["daily-update", "full"],
        default="daily-update",
        help="daily-update: append only missing dates; full: skip existing unless --refresh.",
    )
    parser.add_argument("--start-date", default="2000-01-01", help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument(
        "--end-date",
        default="today",
        help="Inclusive end date in YYYY-MM-DD, or 'today'/'now' to use current local date.",
    )
    parser.add_argument("--output-dir", default="data_forex_frankfurter", help="Output directory")
    parser.add_argument("--symbols-file", default=None, help="Optional text file with one 6-letter pair per line")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent workers")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument("--refresh", action="store_true", help="Re-download even if parquet exists")
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Deprecated compatibility flag. Same as --mode daily-update.",
    )
    parser.add_argument(
        "--skip-manifest",
        action="store_true",
        help="Do not overwrite output_dir/symbols.csv",
    )
    return parser.parse_args()


def _get_json(url: str, timeout: int) -> dict:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected response shape for {url}")
    return data


def _load_supported_currencies(timeout: int) -> set[str]:
    payload = _get_json(f"{API_BASE}/currencies", timeout)
    return {str(code).upper() for code in payload.keys()}


def _resolve_api_end_date(timeout: int) -> str:
    payload = _get_json(f"{API_BASE}/latest", timeout)
    end_date = str(payload.get("date", "")).strip()
    if not end_date:
        raise RuntimeError("Frankfurter /latest did not include date")
    return end_date


def _load_default_pairs() -> list[str]:
    if not DEFAULT_SYMBOLS_PATH.exists():
        return [
            "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD",
            "EURJPY", "EURGBP", "EURCHF", "EURAUD", "EURNZD", "EURCAD", "GBPJPY", "GBPCHF",
        ]

    frame = pl.read_csv(DEFAULT_SYMBOLS_PATH, infer_schema=False, ignore_errors=True).fill_null("")
    if "code" not in frame.columns:
        return []

    pairs: list[str] = []
    for raw in frame["code"].to_list():
        code = str(raw).strip().upper()
        if len(code) == 6 and code.isalpha():
            pairs.append(code)
    return pairs


def _load_pairs_from_txt(file_path: Path) -> list[str]:
    pairs: list[str] = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip().upper().replace("=X", "").replace("-", "")
            if not value or value.startswith("#"):
                continue
            if len(value) == 6 and value.isalpha():
                pairs.append(value)
    return pairs


def _build_symbol_records(pairs: list[str], supported: set[str]) -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    seen: set[str] = set()
    for pair in pairs:
        if pair in seen:
            continue
        seen.add(pair)
        base, quote = pair[:3], pair[3:]
        if base not in supported or quote not in supported:
            continue
        records.append(SymbolRecord(code=pair, name=pair, market="forex", base=base, quote=quote))
    return records


def _normalize_date_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty() or "date" not in frame.columns:
        return frame
    date_expr = (
        pl.col("date").str.to_datetime(strict=False).alias("date")
        if frame.schema.get("date") == pl.String
        else pl.col("date").cast(pl.Datetime("us"), strict=False).alias("date")
    )
    return frame.with_columns(date_expr).drop_nulls("date").sort("date")


def _max_frame_date(frame: pl.DataFrame) -> str | None:
    normalized = _normalize_date_frame(frame)
    if normalized.is_empty():
        return None
    latest = normalized.select(pl.col("date").max()).item()
    return latest.date().isoformat()


def _normalize_rate_rows(rows: list[dict[str, object]], fetch_start_date: str, end_date: str) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    start_dt = datetime.strptime(fetch_start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    return (
        pl.DataFrame(rows)
        .with_columns(
            pl.col("date").str.to_datetime(strict=False).alias("date"),
            *[
                pl.col(column).cast(pl.Float64, strict=False).alias(column)
                for column in ("open", "max", "min", "close", "adjclose", "Trading_Volume")
                if column in rows[0]
            ],
        )
        .filter(pl.col("date").is_between(start_dt, end_dt, closed="both"))
        .drop_nulls(["date", "close"])
        .sort("date")
        .unique(subset=["date"], keep="last", maintain_order=True)
        .sort("date")
    )


def _download_pair(
    record: SymbolRecord,
    start_date: str,
    end_date: str,
    output_dir: Path,
    timeout: int,
    refresh: bool,
    incremental: bool,
) -> DownloadResult:
    output_path = output_dir / f"{record.code}_features.parquet"
    existing_frame: pl.DataFrame | None = None
    existing_rows = 0
    fetch_start_date = start_date

    if output_path.exists() and incremental:
        try:
            existing_frame = _read_parquet(output_path)
            existing_rows = int(existing_frame.height)
            latest_date = _max_frame_date(existing_frame)
            if latest_date is not None:
                next_date = (datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)).date().isoformat()
                fetch_start_date = max(start_date, next_date)
            if datetime.strptime(fetch_start_date, "%Y-%m-%d") > datetime.strptime(end_date, "%Y-%m-%d"):
                return DownloadResult(
                    code=record.code,
                    status="up_to_date",
                    rows=existing_rows,
                    output_path=str(output_path),
                )
        except Exception as exc:
            return DownloadResult(
                code=record.code,
                status="failed_existing_read",
                rows=0,
                output_path=str(output_path),
                message=str(exc),
            )

    if output_path.exists() and not refresh and not incremental:
        try:
            rows = _read_parquet(output_path).height
            return DownloadResult(code=record.code, status="skipped_existing", rows=int(rows), output_path=str(output_path))
        except Exception as exc:
            return DownloadResult(
                code=record.code,
                status="failed_existing_read",
                rows=0,
                output_path=str(output_path),
                message=str(exc),
            )

    url = f"{API_BASE}/{fetch_start_date}..{end_date}?from={record.base}&to={record.quote}"
    try:
        payload = _get_json(url, timeout)
        rates = payload.get("rates", {})
        if not isinstance(rates, dict) or not rates:
            return DownloadResult(code=record.code, status="empty", rows=0, output_path=None, message="No rates returned")

        rows: list[dict[str, object]] = []
        for d, item in rates.items():
            if not isinstance(item, dict):
                continue
            close_value = item.get(record.quote)
            if close_value is None:
                continue
            close_num = float(close_value)
            rows.append(
                {
                    "date": d,
                    "open": close_num,
                    "max": close_num,
                    "min": close_num,
                    "close": close_num,
                    "adjclose": close_num,
                    "Trading_Volume": None,
                }
            )

        if not rows:
            return DownloadResult(code=record.code, status="empty", rows=0, output_path=None, message="No usable rate points")

        frame = _normalize_rate_rows(rows, fetch_start_date, end_date)
        if frame.is_empty():
            return DownloadResult(code=record.code, status="empty", rows=0, output_path=None, message="No usable rate points after date filtering")

        if incremental and existing_frame is not None:
            merged = (
                pl.concat([_normalize_date_frame(existing_frame), frame], how="diagonal_relaxed")
                .sort("date")
                .unique(subset=["date"], keep="last", maintain_order=True)
                .sort("date")
            )
            _write_parquet(merged, output_path)
            return DownloadResult(
                code=record.code,
                status="updated_incremental",
                rows=int(merged.height),
                output_path=str(output_path),
            )

        _write_parquet(frame, output_path)
        return DownloadResult(code=record.code, status="updated", rows=int(frame.height), output_path=str(output_path))
    except Exception as exc:
        return DownloadResult(code=record.code, status="failed", rows=0, output_path=None, message=str(exc))


def main() -> None:
    args = parse_args()
    incremental_mode = args.incremental or args.mode == "daily-update"

    if args.refresh and incremental_mode:
        raise RuntimeError("--refresh cannot be combined with daily incremental mode (--mode daily-update or --incremental)")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    supported = _load_supported_currencies(args.timeout)
    api_latest = _resolve_api_end_date(args.timeout)
    requested_end = resolve_end_date(str(args.end_date))
    applied_end = min(requested_end, api_latest)

    if args.symbols_file:
        pairs = _load_pairs_from_txt(Path(args.symbols_file))
    else:
        pairs = _load_default_pairs()

    records = _build_symbol_records(pairs, supported)
    if not records:
        raise RuntimeError("No valid forex pairs resolved for Frankfurter")

    if not args.skip_manifest:
        pl.DataFrame([asdict(item) for item in records]).write_csv(output_dir / "symbols.csv")

    def _worker(record: SymbolRecord) -> DownloadResult:
        return _download_pair(
            record,
            args.start_date,
            applied_end,
            output_dir,
            args.timeout,
            args.refresh,
            incremental_mode,
        )

    results = run_parallel_tasks(
        records,
        _worker,
        max_workers=args.workers,
        desc="download:forex:frankfurter",
        unit="symbol",
    )

    results.sort(key=lambda item: item.code)

    report_columns = ["code", "status", "rows", "output_path", "message"]
    report_rows = [asdict(item) for item in results]
    report_frame = (
        pl.DataFrame(report_rows, infer_schema_length=None).select(report_columns)
        if report_rows
        else pl.DataFrame({column: [] for column in report_columns})
    )
    report_frame.write_csv(output_dir / "download_report.csv")

    status_counts: dict[str, int] = {}
    row_count = 0
    for item in results:
        status_counts[item.status] = status_counts.get(item.status, 0) + 1
        row_count += int(item.rows)

    summary = {
        "provider": "frankfurter",
        "mode": "daily-update" if incremental_mode else "full",
        "requested_start_date": args.start_date,
        "requested_end_date": requested_end,
        "provider_end_date": api_latest,
        "applied_end_date": applied_end,
        "symbol_count": len(records),
        "row_count": row_count,
        "status_counts": status_counts,
    }
    (output_dir / "download_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(
        "[download] provider=frankfurter "
        f"start={args.start_date} requested_end={requested_end} "
        f"provider_latest={api_latest} applied_end={applied_end} symbols={len(records)}"
    )
    print(f"[download] completed status_counts={status_counts}")


if __name__ == "__main__":
    main()
