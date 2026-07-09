#!/usr/bin/env python
"""Download U.S. daily OHLCV from Cboe delayed historical charts.

The output intentionally matches the existing stockAgent Yahoo feature layout
under data_yahoo/us_stocks so training, panel builds, and Discord live signals
can keep using the same configured parquet root.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import polars as pl

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional runtime dependency
    pa = None
    pq = None

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - progress is optional
    tqdm = None


ASSET_CLASS = "us_stocks"
BASE_COLUMNS = ["date", "open", "max", "min", "close", "adjclose", "Trading_Volume"]
DEFAULT_START_DATE = "2000-01-01"
DEFAULT_OUTPUT_ROOT = Path("data_yahoo")
DEFAULT_SYMBOLS_FALLBACK = Path("configs") / "fallback_us_stocks_symbols.csv"
CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/{symbol}.json"
_RATE_LIMIT_LOCK = threading.Lock()
_NEXT_REQUEST_TIME = 0.0

PARQUET_META_SOURCE_KEY = b"stockagent.source"
PARQUET_META_ASSET_CLASS_KEY = b"stockagent.asset_class"
PARQUET_META_CHECKED_THROUGH_KEY = b"stockagent.yahoo_checked_through"
PARQUET_META_REQUESTED_END_KEY = b"stockagent.yahoo_requested_end"
PARQUET_META_FIRST_DATE_KEY = b"stockagent.first_date"
PARQUET_META_LAST_DATE_KEY = b"stockagent.last_date"
PARQUET_META_WRITE_TS_UTC_KEY = b"stockagent.write_ts_utc"


@dataclass(frozen=True)
class SymbolRecord:
    code: str
    name: str = ""
    market: str = ASSET_CLASS
    yahoo_symbol: str = ""


@dataclass
class DownloadResult:
    code: str
    cboe_symbol: str
    status: str
    rows: int
    first_date: str | None
    last_date: str | None
    output_path: str
    elapsed_sec: float
    message: str = ""


def _parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _symbol_filename(code: str) -> str:
    return f"{code.replace('/', '_').replace('\\\\', '_')}_features.parquet"


def _cboe_symbol_candidates(code: str) -> list[str]:
    base = code.strip().upper()
    if base.endswith("_DL"):
        base = base[:-3]
    candidates = [
        base,
        base.replace(".", "-"),
        base.replace("/", "-"),
        base.replace(" ", "-"),
    ]
    seen: set[str] = set()
    out: list[str] = []
    for candidate in candidates:
        normalized = candidate.strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def _load_symbol_records(symbols: list[str] | None, symbols_file: Path | None, output_dir: Path) -> list[SymbolRecord]:
    if symbols:
        return [SymbolRecord(code=s.strip().upper(), yahoo_symbol=s.strip().upper()) for s in symbols if s.strip()]

    manifest = symbols_file or output_dir / "symbols.csv"
    if not manifest.exists():
        manifest = DEFAULT_SYMBOLS_FALLBACK
    if not manifest.exists():
        raise FileNotFoundError(f"symbol manifest not found: {manifest}")

    frame = pl.read_csv(manifest, infer_schema_length=0)
    columns = set(frame.columns)
    code_col = "code" if "code" in columns else frame.columns[0]
    name_col = "name" if "name" in columns else None
    yahoo_col = "yahoo_symbol" if "yahoo_symbol" in columns else None
    records: list[SymbolRecord] = []
    for row in frame.iter_rows(named=True):
        code = str(row.get(code_col) or "").strip().upper()
        if not code:
            continue
        records.append(
            SymbolRecord(
                code=code,
                name=str(row.get(name_col) or "") if name_col else "",
                yahoo_symbol=str(row.get(yahoo_col) or code).strip().upper() if yahoo_col else code,
            )
        )
    return records


def _write_symbol_manifest(records: list[SymbolRecord], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [asdict(item) for item in records]
    if rows:
        pl.DataFrame(rows).write_csv(output_dir / "symbols.csv")


class CboeRateLimitError(RuntimeError):
    pass


def _rate_limit_wait(interval: float) -> None:
    global _NEXT_REQUEST_TIME
    if interval <= 0:
        return
    with _RATE_LIMIT_LOCK:
        now = time.monotonic()
        wait = max(0.0, _NEXT_REQUEST_TIME - now)
        _NEXT_REQUEST_TIME = max(now, _NEXT_REQUEST_TIME) + interval
    if wait > 0:
        time.sleep(wait)


def _fetch_cboe_json(
    symbol: str,
    *,
    timeout: float,
    retries: int,
    retry_backoff: float,
    request_interval: float,
) -> dict:
    url = CBOE_URL.format(symbol=symbol)
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            _rate_limit_wait(request_interval)
            req = Request(url, headers={"User-Agent": "stockAgent-cboe-us-downloader/1.0"})
            with urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_error = exc
            if exc.code == 429 and attempt >= retries:
                raise CboeRateLimitError(f"{symbol}: HTTP 429 Too Many Requests")
            if attempt < retries:
                time.sleep(retry_backoff * (2**attempt))
                continue
            break
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(retry_backoff * (2**attempt))
                continue
            break
    raise RuntimeError(f"{symbol}: {last_error}")


def _json_to_frame(payload: dict, *, start_date: date, end_date: date) -> pl.DataFrame:
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not rows:
        return pl.DataFrame({column: [] for column in BASE_COLUMNS})
    frame = pl.DataFrame(rows)
    if "date" not in frame.columns:
        return pl.DataFrame({column: [] for column in BASE_COLUMNS})
    frame = frame.with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
        pl.col("open").cast(pl.Float64, strict=False).alias("open"),
        pl.col("high").cast(pl.Float64, strict=False).alias("max"),
        pl.col("low").cast(pl.Float64, strict=False).alias("min"),
        pl.col("close").cast(pl.Float64, strict=False).alias("close"),
        pl.col("volume").cast(pl.Float64, strict=False).alias("Trading_Volume"),
    )
    frame = frame.filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))
    if frame.is_empty():
        return pl.DataFrame({column: [] for column in BASE_COLUMNS})
    frame = frame.with_columns(pl.col("close").alias("adjclose"))
    frame = frame.select(BASE_COLUMNS)
    frame = frame.drop_nulls(["date", "open", "max", "min", "close"])
    frame = frame.unique(subset=["date"], keep="last").sort("date")
    return frame.with_columns(pl.col("date").cast(pl.Datetime("us")))


def _read_existing(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame({column: [] for column in BASE_COLUMNS})
    frame = pl.read_parquet(path)
    expressions = []
    for column in BASE_COLUMNS:
        if column in frame.columns:
            expressions.append(pl.col(column))
        elif column == "adjclose" and "close" in frame.columns:
            expressions.append(pl.col("close").alias("adjclose"))
        else:
            expressions.append(pl.lit(None).alias(column))
    frame = frame.select(expressions)
    if frame.is_empty():
        return frame
    return frame.with_columns(pl.col("date").cast(pl.Datetime("us"), strict=False))


def _date_bounds(frame: pl.DataFrame) -> tuple[str | None, str | None]:
    if frame.is_empty() or "date" not in frame.columns:
        return None, None
    bounds = frame.select(pl.col("date").min().alias("first"), pl.col("date").max().alias("last")).row(0, named=True)

    def fmt(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return str(value)[:10]

    return fmt(bounds["first"]), fmt(bounds["last"])


def _last_date(path: Path) -> date | None:
    metadata = _read_parquet_metadata(path)
    last = metadata.get("last_date")
    if last:
        return _parse_date(last)
    frame = _read_existing(path)
    _, fallback_last = _date_bounds(frame)
    return _parse_date(fallback_last) if fallback_last else None


def _read_parquet_metadata(path: Path) -> dict[str, str | int]:
    if not path.exists() or pq is None:
        return {}
    try:
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.schema_arrow.metadata or {}
        out: dict[str, str | int] = {"rows": parquet_file.metadata.num_rows}
        raw_last = metadata.get(PARQUET_META_LAST_DATE_KEY)
        raw_first = metadata.get(PARQUET_META_FIRST_DATE_KEY)
        if raw_first:
            out["first_date"] = raw_first.decode("utf-8")
        if raw_last:
            out["last_date"] = raw_last.decode("utf-8")
        return out
    except Exception:
        return {}


def _write_parquet_atomic(frame: pl.DataFrame, output_path: Path, *, requested_end_date: str) -> tuple[str | None, str | None]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = frame.select(BASE_COLUMNS).sort("date")
    first, last = _date_bounds(frame)
    handle = tempfile.NamedTemporaryFile(prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent, delete=False)
    tmp_path = Path(handle.name)
    handle.close()
    try:
        if pa is not None and pq is not None:
            table = frame.to_arrow()
            metadata = dict(table.schema.metadata or {})
            metadata.update(
                {
                    PARQUET_META_SOURCE_KEY: b"cboe",
                    PARQUET_META_ASSET_CLASS_KEY: ASSET_CLASS.encode("utf-8"),
                    PARQUET_META_REQUESTED_END_KEY: requested_end_date.encode("utf-8"),
                    PARQUET_META_CHECKED_THROUGH_KEY: requested_end_date.encode("utf-8"),
                    PARQUET_META_WRITE_TS_UTC_KEY: datetime.now(timezone.utc)
                    .replace(microsecond=0)
                    .isoformat()
                    .encode("utf-8"),
                }
            )
            if first is not None:
                metadata[PARQUET_META_FIRST_DATE_KEY] = first.encode("utf-8")
            if last is not None:
                metadata[PARQUET_META_LAST_DATE_KEY] = last.encode("utf-8")
            pq.write_table(
                table.replace_schema_metadata(metadata),
                tmp_path,
                compression="snappy",
                use_dictionary=True,
                write_statistics=True,
                row_group_size=128_000,
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )
        else:
            frame.write_parquet(tmp_path, compression="snappy")
        os.replace(tmp_path, output_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return first, last


def _merge_frames(existing: pl.DataFrame, incoming: pl.DataFrame) -> pl.DataFrame:
    if existing.is_empty():
        return incoming.select(BASE_COLUMNS)
    if incoming.is_empty():
        return existing.select(BASE_COLUMNS)
    return (
        pl.concat([existing.select(BASE_COLUMNS), incoming.select(BASE_COLUMNS)], how="vertical")
        .unique(subset=["date"], keep="last")
        .sort("date")
    )


def _download_one(
    record: SymbolRecord,
    *,
    output_dir: Path,
    start_date: date,
    end_date: date,
    mode: str,
    repair_overlap_days: int,
    daily_stale_max_lag_days: int,
    timeout: float,
    retries: int,
    retry_backoff: float,
    request_interval: float,
) -> DownloadResult:
    start_ts = time.perf_counter()
    output_path = output_dir / _symbol_filename(record.code)
    try:
        existing_last = _last_date(output_path)
        effective_start = start_date
        if mode in {"daily-update", "incremental"} and existing_last is not None:
            expected_current = end_date - timedelta(days=max(0, daily_stale_max_lag_days))
            if existing_last >= expected_current:
                metadata = _read_parquet_metadata(output_path)
                first = str(metadata.get("first_date")) if metadata.get("first_date") else None
                last = str(metadata.get("last_date")) if metadata.get("last_date") else existing_last.isoformat()
                rows = int(metadata.get("rows") or 0)
                return DownloadResult(
                    code=record.code,
                    cboe_symbol=_cboe_symbol_candidates(record.yahoo_symbol or record.code)[0],
                    status="current",
                    rows=rows,
                    first_date=first,
                    last_date=last,
                    output_path=str(output_path),
                    elapsed_sec=time.perf_counter() - start_ts,
                )
            effective_start = max(start_date, existing_last - timedelta(days=max(0, repair_overlap_days)))

        payload = None
        cboe_symbol = ""
        last_error = ""
        for candidate in _cboe_symbol_candidates(record.yahoo_symbol or record.code):
            cboe_symbol = candidate
            try:
                payload = _fetch_cboe_json(
                    candidate,
                    timeout=timeout,
                    retries=retries,
                    retry_backoff=retry_backoff,
                    request_interval=request_interval,
                )
                incoming = _json_to_frame(payload, start_date=effective_start, end_date=end_date)
                if not incoming.is_empty():
                    break
                last_error = "no rows in requested date range"
            except CboeRateLimitError as exc:
                raise exc
            except Exception as exc:  # try alternate class-share spelling before failing
                last_error = str(exc)
                incoming = pl.DataFrame({column: [] for column in BASE_COLUMNS})
                continue
        else:
            incoming = pl.DataFrame({column: [] for column in BASE_COLUMNS})

        existing = _read_existing(output_path)
        if incoming.is_empty():
            if existing.is_empty():
                return DownloadResult(
                    code=record.code,
                    cboe_symbol=cboe_symbol,
                    status="not_found_skip",
                    rows=0,
                    first_date=None,
                    last_date=None,
                    output_path=str(output_path),
                    elapsed_sec=time.perf_counter() - start_ts,
                    message=last_error or "no Cboe data",
                )
            first, last = _date_bounds(existing)
            return DownloadResult(
                code=record.code,
                cboe_symbol=cboe_symbol,
                status="unchanged",
                rows=existing.height,
                first_date=first,
                last_date=last,
                output_path=str(output_path),
                elapsed_sec=time.perf_counter() - start_ts,
                message=last_error or "no new Cboe rows",
            )

        merged = _merge_frames(existing, incoming)
        first, last = _write_parquet_atomic(merged, output_path, requested_end_date=end_date.isoformat())
        return DownloadResult(
            code=record.code,
            cboe_symbol=cboe_symbol,
            status="updated" if existing.is_empty() or merged.height != existing.height else "repaired",
            rows=merged.height,
            first_date=first,
            last_date=last,
            output_path=str(output_path),
            elapsed_sec=time.perf_counter() - start_ts,
        )
    except Exception as exc:
        return DownloadResult(
            code=record.code,
            cboe_symbol="",
            status="failed",
            rows=0,
            first_date=None,
            last_date=None,
            output_path=str(output_path),
            elapsed_sec=time.perf_counter() - start_ts,
            message=str(exc),
        )


def _iter_limited(records: Iterable[SymbolRecord], limit: int | None) -> list[SymbolRecord]:
    out = list(records)
    if limit is not None and limit > 0:
        return out[:limit]
    return out


def _write_reports(results: list[DownloadResult], output_dir: Path, *, started_at: str, ended_at: str) -> None:
    rows = [asdict(item) for item in results]
    report_path = output_dir / "download_report.csv"
    summary_path = output_dir / "download_summary.json"
    if rows:
        pl.DataFrame(rows).write_csv(report_path)
    status_counts: dict[str, int] = {}
    last_dates = [item.last_date for item in results if item.last_date]
    for item in results:
        status_counts[item.status] = status_counts.get(item.status, 0) + 1
    summary = {
        "source": "cboe",
        "asset_class": ASSET_CLASS,
        "started_at_utc": started_at,
        "ended_at_utc": ended_at,
        "symbols": len(results),
        "status_counts": status_counts,
        "latest_date": max(last_dates) if last_dates else None,
        "report_path": str(report_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["download", "daily-update", "incremental"], default="daily-update")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--symbols-file", type=Path)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--retry-backoff", type=float, default=0.5)
    parser.add_argument("--request-interval", type=float, default=0.25)
    parser.add_argument("--repair-overlap-days", type=int, default=7)
    parser.add_argument("--daily-stale-max-lag-days", type=int, default=0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--no-write-symbols", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    output_dir = args.output_root / ASSET_CLASS
    output_dir.mkdir(parents=True, exist_ok=True)
    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    records = _iter_limited(_load_symbol_records(args.symbols, args.symbols_file, output_dir), args.limit)
    if not records:
        raise SystemExit("no symbols to download")
    if not args.no_write_symbols:
        _write_symbol_manifest(records, output_dir)

    print(
        f"[cboe-us] mode={args.mode} symbols={len(records)} start={start_date} end={end_date} "
        f"output={output_dir} workers={args.workers}",
        flush=True,
    )
    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [
            pool.submit(
                _download_one,
                record,
                output_dir=output_dir,
                start_date=start_date,
                end_date=end_date,
                mode=args.mode,
                repair_overlap_days=args.repair_overlap_days,
                daily_stale_max_lag_days=args.daily_stale_max_lag_days,
                timeout=args.timeout,
                retries=args.retries,
                retry_backoff=args.retry_backoff,
                request_interval=args.request_interval,
            )
            for record in records
        ]
        iterator = as_completed(futures)
        if tqdm is not None:
            iterator = tqdm(iterator, total=len(futures), desc="cboe-us", unit="symbol")
        for future in iterator:
            result = future.result()
            results.append(result)
            if result.status == "failed":
                print(f"[cboe-us] failed {result.code}: {result.message}", flush=True)

    results.sort(key=lambda item: item.code)
    ended_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    _write_reports(results, output_dir, started_at=started_at, ended_at=ended_at)
    counts: dict[str, int] = {}
    for item in results:
        counts[item.status] = counts.get(item.status, 0) + 1
    print(f"[cboe-us] status_counts={counts}", flush=True)
    return 1 if counts.get("failed", 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
