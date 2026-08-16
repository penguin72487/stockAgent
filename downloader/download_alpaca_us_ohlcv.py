#!/usr/bin/env python
"""Download U.S. daily OHLCV from Alpaca Market Data in symbol batches.

The output matches stockAgent's existing ``data_yahoo/us_stocks`` parquet
layout so panel builds and live signals can switch providers without migration.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import polars as pl
import requests

try:
    from downloader.common import SharedRateLimiter, retry_delay_seconds
except ImportError:  # pragma: no cover - direct script execution from downloader/
    from common import SharedRateLimiter, retry_delay_seconds

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
ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
ALPACA_BASIC_REQUESTS_PER_MINUTE = 200.0
ALPACA_MAX_PAGE_ROWS = 10_000

PARQUET_META_SOURCE_KEY = b"stockagent.source"
PARQUET_META_PROVIDER_KEY = b"stockagent.provider"
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

    @property
    def alpaca_symbol(self) -> str:
        return _alpaca_symbol(self.yahoo_symbol or self.code)


@dataclass(frozen=True)
class PreparedRecord:
    record: SymbolRecord
    output_path: Path
    effective_start: date


@dataclass
class DownloadResult:
    code: str
    alpaca_symbol: str
    status: str
    rows: int
    first_date: str | None
    last_date: str | None
    output_path: str
    elapsed_sec: float
    message: str = ""


class AlpacaRequestError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = int(status_code)


def _parse_date(value: str | None) -> date:
    if not value:
        return datetime.now(ZoneInfo("America/New_York")).date()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _symbol_filename(code: str) -> str:
    return f"{code.replace('/', '_').replace('\\\\', '_')}_features.parquet"


def _alpaca_symbol(value: str) -> str:
    symbol = str(value).strip().upper()
    if symbol.endswith("_DL"):
        symbol = symbol[:-3]
    parts = symbol.rsplit("-", 1)
    if len(parts) == 2 and parts[1] in {"A", "B", "C"} and 1 <= len(parts[0]) <= 4:
        symbol = f"{parts[0]}.{parts[1]}"
    return symbol


def _load_symbol_records(
    symbols: list[str] | None, symbols_file: Path | None, output_dir: Path
) -> list[SymbolRecord]:
    if symbols:
        return [
            SymbolRecord(code=s.strip().upper(), yahoo_symbol=s.strip().upper())
            for s in symbols
            if s.strip()
        ]

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
                yahoo_symbol=str(row.get(yahoo_col) or code).strip().upper()
                if yahoo_col
                else code,
            )
        )
    return records


def _write_symbol_manifest(records: list[SymbolRecord], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [asdict(item) for item in records]
    if rows:
        output_path = output_dir / "symbols.csv"
        temporary = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
        try:
            pl.DataFrame(rows).write_csv(temporary)
            os.replace(temporary, output_path)
        finally:
            temporary.unlink(missing_ok=True)


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
    first, last = frame.select(
        pl.col("date").min().alias("first"),
        pl.col("date").max().alias("last"),
    ).row(0)

    def fmt(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return str(value)[:10]

    return fmt(first), fmt(last)


def _read_parquet_metadata(path: Path) -> dict[str, str | int]:
    if not path.exists() or pq is None:
        return {}
    try:
        parquet_file = pq.ParquetFile(path)
        metadata = parquet_file.schema_arrow.metadata or {}
        out: dict[str, str | int] = {"rows": parquet_file.metadata.num_rows}
        for output_key, metadata_key in (
            ("first_date", PARQUET_META_FIRST_DATE_KEY),
            ("last_date", PARQUET_META_LAST_DATE_KEY),
        ):
            raw = metadata.get(metadata_key)
            if raw:
                out[output_key] = raw.decode("utf-8")
        return out
    except Exception:
        return {}


def _last_date(path: Path) -> date | None:
    metadata = _read_parquet_metadata(path)
    last = metadata.get("last_date")
    if last:
        return _parse_date(str(last))
    frame = _read_existing(path)
    _, fallback_last = _date_bounds(frame)
    return _parse_date(fallback_last) if fallback_last else None


def _write_parquet_atomic(
    frame: pl.DataFrame, output_path: Path, *, requested_end_date: str
) -> tuple[str | None, str | None]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = frame.select(BASE_COLUMNS).sort("date")
    first, last = _date_bounds(frame)
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    )
    tmp_path = Path(handle.name)
    handle.close()
    try:
        if pa is not None and pq is not None:
            table = frame.to_arrow()
            metadata = dict(table.schema.metadata or {})
            metadata.update(
                {
                    PARQUET_META_SOURCE_KEY: b"alpaca",
                    PARQUET_META_PROVIDER_KEY: b"alpaca_market_data",
                    PARQUET_META_ASSET_CLASS_KEY: ASSET_CLASS.encode("utf-8"),
                    PARQUET_META_REQUESTED_END_KEY: requested_end_date.encode("utf-8"),
                    PARQUET_META_CHECKED_THROUGH_KEY: requested_end_date.encode(
                        "utf-8"
                    ),
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
        pl.concat(
            [existing.select(BASE_COLUMNS), incoming.select(BASE_COLUMNS)],
            how="vertical",
        )
        .unique(subset=["date"], keep="last")
        .sort("date")
    )


def _bars_to_frame(
    rows: list[dict[str, Any]], *, start_date: date, end_date: date
) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame({column: [] for column in BASE_COLUMNS})
    frame = pl.DataFrame(rows)
    required = {"t", "o", "h", "l", "c", "v"}
    if not required.issubset(frame.columns):
        return pl.DataFrame({column: [] for column in BASE_COLUMNS})
    frame = frame.select(
        pl.col("t")
        .cast(pl.String)
        .str.slice(0, 10)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .alias("date"),
        pl.col("o").cast(pl.Float64, strict=False).alias("open"),
        pl.col("h").cast(pl.Float64, strict=False).alias("max"),
        pl.col("l").cast(pl.Float64, strict=False).alias("min"),
        pl.col("c").cast(pl.Float64, strict=False).alias("close"),
        pl.col("v").cast(pl.Float64, strict=False).alias("Trading_Volume"),
    )
    frame = frame.filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))
    if frame.is_empty():
        return pl.DataFrame({column: [] for column in BASE_COLUMNS})
    return (
        frame.with_columns(pl.col("close").alias("adjclose"))
        .select(BASE_COLUMNS)
        .drop_nulls(["date", "open", "max", "min", "close"])
        .unique(subset=["date"], keep="last")
        .sort("date")
        .with_columns(pl.col("date").cast(pl.Datetime("us")))
    )


class AlpacaClient:
    def __init__(
        self,
        *,
        api_key_id: str,
        api_secret_key: str,
        requests_per_minute: float,
        timeout: float,
        retries: int,
        retry_backoff: float,
    ) -> None:
        if not api_key_id or not api_secret_key:
            raise ValueError(
                "Alpaca credentials are required: set APCA_API_KEY_ID and APCA_API_SECRET_KEY "
                "or pass --api-key-id/--api-secret-key"
            )
        self.api_key_id = api_key_id
        self.api_secret_key = api_secret_key
        self.requests_per_minute = max(0.001, float(requests_per_minute))
        self.timeout = max(1.0, float(timeout))
        self.retries = max(0, int(retries))
        self.retry_backoff = max(0.1, float(retry_backoff))
        self.limiter = SharedRateLimiter(
            60.0 / self.requests_per_minute, name="alpaca_market_data"
        )
        self._local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=32, pool_maxsize=32
            )
            session.mount("https://", adapter)
            session.headers.update(
                {
                    "APCA-API-KEY-ID": self.api_key_id,
                    "APCA-API-SECRET-KEY": self.api_secret_key,
                    "Accept": "application/json",
                    "User-Agent": "stockAgent-alpaca-us-downloader/1.0",
                }
            )
            self._local.session = session
        return session

    def _get(self, params: dict[str, str | int]) -> dict[str, Any]:
        last_error = "unknown error"
        for attempt in range(self.retries + 1):
            self.limiter.wait()
            try:
                response = self._session().get(
                    ALPACA_BARS_URL, params=params, timeout=self.timeout
                )
            except requests.RequestException as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < self.retries:
                    self.limiter.defer(
                        retry_delay_seconds(attempt, base=self.retry_backoff)
                    )
                    continue
                raise RuntimeError(last_error) from exc

            if response.status_code == 200:
                payload = response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError("Alpaca returned a non-object JSON payload")
                return payload

            try:
                payload = response.json()
                message = str(payload.get("message") or payload)
            except Exception:
                message = response.text[:500]
            last_error = f"Alpaca HTTP {response.status_code}: {message}"
            if response.status_code in {400, 401, 403, 404, 422}:
                raise AlpacaRequestError(response.status_code, last_error)
            if attempt < self.retries and response.status_code in {
                408,
                429,
                500,
                502,
                503,
                504,
            }:
                retry_after = response.headers.get("Retry-After")
                self.limiter.defer(
                    retry_delay_seconds(
                        attempt,
                        base=self.retry_backoff,
                        retry_after=retry_after,
                    )
                )
                continue
            raise AlpacaRequestError(response.status_code, last_error)
        raise RuntimeError(last_error)

    def fetch_bars(
        self,
        symbols: list[str],
        *,
        start_date: date,
        end_date: date,
        feed: str,
        adjustment: str,
    ) -> tuple[dict[str, list[dict[str, Any]]], int]:
        output: dict[str, list[dict[str, Any]]] = defaultdict(list)
        page_token: str | None = None
        requests_made = 0
        while True:
            params: dict[str, str | int] = {
                "symbols": ",".join(symbols),
                "timeframe": "1Day",
                "start": start_date.isoformat(),
                "end": (end_date + timedelta(days=1)).isoformat(),
                "limit": ALPACA_MAX_PAGE_ROWS,
                "adjustment": adjustment,
                "feed": feed,
                "sort": "asc",
            }
            if page_token:
                params["page_token"] = page_token
            payload = self._get(params)
            requests_made += 1
            bars = payload.get("bars") or {}
            if isinstance(bars, dict):
                for symbol, rows in bars.items():
                    if isinstance(rows, list):
                        output[str(symbol).upper()].extend(
                            row for row in rows if isinstance(row, dict)
                        )
            page_token = str(payload.get("next_page_token") or "").strip() or None
            if page_token is None:
                break
        return dict(output), requests_made


def _fetch_batch_resilient(
    client: AlpacaClient,
    records: list[PreparedRecord],
    *,
    start_date: date,
    end_date: date,
    feed: str,
    adjustment: str,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str], int]:
    symbols = sorted({item.record.alpaca_symbol for item in records})
    try:
        bars, requests_made = client.fetch_bars(
            symbols,
            start_date=start_date,
            end_date=end_date,
            feed=feed,
            adjustment=adjustment,
        )
        return bars, {}, requests_made
    except AlpacaRequestError as exc:
        if exc.status_code not in {400, 404, 422}:
            raise
        if len(symbols) <= 1:
            return {}, {symbol: str(exc) for symbol in symbols}, 1

    midpoint = len(records) // 2
    left_bars, left_errors, left_requests = _fetch_batch_resilient(
        client,
        records[:midpoint],
        start_date=start_date,
        end_date=end_date,
        feed=feed,
        adjustment=adjustment,
    )
    right_bars, right_errors, right_requests = _fetch_batch_resilient(
        client,
        records[midpoint:],
        start_date=start_date,
        end_date=end_date,
        feed=feed,
        adjustment=adjustment,
    )
    merged_bars = dict(left_bars)
    for symbol, rows in right_bars.items():
        merged_bars.setdefault(symbol, []).extend(rows)
    return (
        merged_bars,
        {**left_errors, **right_errors},
        left_requests + right_requests + 1,
    )


def _existing_result(
    record: SymbolRecord, output_path: Path, *, status: str, message: str = ""
) -> DownloadResult:
    started = time.perf_counter()
    metadata = _read_parquet_metadata(output_path)
    first = str(metadata.get("first_date")) if metadata.get("first_date") else None
    last = str(metadata.get("last_date")) if metadata.get("last_date") else None
    rows = int(metadata.get("rows") or 0)
    if (first is None or last is None) and output_path.exists():
        frame = _read_existing(output_path)
        first, last = _date_bounds(frame)
        rows = frame.height
    return DownloadResult(
        code=record.code,
        alpaca_symbol=record.alpaca_symbol,
        status=status,
        rows=rows,
        first_date=first,
        last_date=last,
        output_path=str(output_path),
        elapsed_sec=time.perf_counter() - started,
        message=message,
    )


def _prepare_records(
    records: list[SymbolRecord],
    *,
    output_dir: Path,
    start_date: date,
    end_date: date,
    mode: str,
    repair_overlap_days: int,
    daily_stale_max_lag_days: int,
    metadata_workers: int = 1,
) -> tuple[list[PreparedRecord], list[DownloadResult]]:
    def prepare(record: SymbolRecord) -> PreparedRecord | DownloadResult:
        output_path = output_dir / _symbol_filename(record.code)
        if record.code.endswith("_DL"):
            return _existing_result(
                record,
                output_path,
                status="delisted_skip",
                message="preserved historical delisted file; not mapped to an active Alpaca symbol",
            )
        existing_last = _last_date(output_path)
        effective_start = start_date
        if mode in {"daily-update", "incremental"} and existing_last is not None:
            expected_current = end_date - timedelta(
                days=max(0, daily_stale_max_lag_days)
            )
            if existing_last >= expected_current:
                return _existing_result(record, output_path, status="current")
            effective_start = max(
                start_date, existing_last - timedelta(days=max(0, repair_overlap_days))
            )
        return PreparedRecord(
            record=record, output_path=output_path, effective_start=effective_start
        )

    workers = max(1, int(metadata_workers))
    if workers == 1:
        prepared = [prepare(record) for record in records]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            prepared = list(executor.map(prepare, records))

    pending = [item for item in prepared if isinstance(item, PreparedRecord)]
    results = [item for item in prepared if isinstance(item, DownloadResult)]
    return pending, results


def _apply_batch(
    records: list[PreparedRecord],
    bars: dict[str, list[dict[str, Any]]],
    errors: dict[str, str],
    *,
    end_date: date,
) -> list[DownloadResult]:
    results: list[DownloadResult] = []
    for item in records:
        started = time.perf_counter()
        record = item.record
        symbol = record.alpaca_symbol
        existing = _read_existing(item.output_path)
        error = errors.get(symbol)
        if error:
            results.append(
                DownloadResult(
                    code=record.code,
                    alpaca_symbol=symbol,
                    status="failed",
                    rows=existing.height,
                    first_date=_date_bounds(existing)[0],
                    last_date=_date_bounds(existing)[1],
                    output_path=str(item.output_path),
                    elapsed_sec=time.perf_counter() - started,
                    message=error,
                )
            )
            continue

        incoming = _bars_to_frame(
            bars.get(symbol, []), start_date=item.effective_start, end_date=end_date
        )
        if incoming.is_empty():
            first, last = _date_bounds(existing)
            results.append(
                DownloadResult(
                    code=record.code,
                    alpaca_symbol=symbol,
                    status="not_found_skip" if existing.is_empty() else "unchanged",
                    rows=existing.height,
                    first_date=first,
                    last_date=last,
                    output_path=str(item.output_path),
                    elapsed_sec=time.perf_counter() - started,
                    message="no Alpaca bars in requested date range",
                )
            )
            continue

        merged = _merge_frames(existing, incoming)
        first, last = _write_parquet_atomic(
            merged, item.output_path, requested_end_date=end_date.isoformat()
        )
        results.append(
            DownloadResult(
                code=record.code,
                alpaca_symbol=symbol,
                status="updated"
                if existing.is_empty() or merged.height != existing.height
                else "repaired",
                rows=merged.height,
                first_date=first,
                last_date=last,
                output_path=str(item.output_path),
                elapsed_sec=time.perf_counter() - started,
            )
        )
    return results


def _chunked(items: list[PreparedRecord], size: int) -> list[list[PreparedRecord]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _iter_limited(
    records: Iterable[SymbolRecord], limit: int | None
) -> list[SymbolRecord]:
    output = list(records)
    return output[:limit] if limit is not None and limit > 0 else output


def _write_reports(
    results: list[DownloadResult],
    output_dir: Path,
    *,
    started_at: str,
    ended_at: str,
    api_requests: int,
) -> None:
    rows = [asdict(item) for item in results]
    report_path = output_dir / "download_report.csv"
    summary_path = output_dir / "download_summary.json"
    if rows:
        temporary = report_path.with_name(f".{report_path.name}.{os.getpid()}.tmp")
        try:
            pl.DataFrame(rows).write_csv(temporary)
            os.replace(temporary, report_path)
        finally:
            temporary.unlink(missing_ok=True)
    status_counts: dict[str, int] = {}
    last_dates = [item.last_date for item in results if item.last_date]
    for item in results:
        status_counts[item.status] = status_counts.get(item.status, 0) + 1
    summary = {
        "source": "alpaca",
        "provider": "alpaca_market_data",
        "asset_class": ASSET_CLASS,
        "started_at_utc": started_at,
        "ended_at_utc": ended_at,
        "symbols": len(results),
        "api_requests": api_requests,
        "status_counts": status_counts,
        "latest_date": max(last_dates) if last_dates else None,
        "report_path": str(report_path),
    }
    temporary = summary_path.with_name(f".{summary_path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, summary_path)
    finally:
        temporary.unlink(missing_ok=True)


def _env_credential(primary: str, alias: str) -> str:
    return str(os.environ.get(primary) or os.environ.get(alias) or "").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["download", "daily-update", "incremental"],
        default="daily-update",
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--symbols-file", type=Path)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date")
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Concurrent Alpaca symbol-batch requests",
    )
    parser.add_argument(
        "--metadata-workers",
        type=int,
        default=4,
        help="Concurrent local parquet metadata readers",
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Symbols per Alpaca request batch"
    )
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retry-backoff", type=float, default=0.5)
    parser.add_argument(
        "--requests-per-minute",
        type=float,
        default=float(
            os.environ.get(
                "ALPACA_REQUESTS_PER_MINUTE", ALPACA_BASIC_REQUESTS_PER_MINUTE
            )
        ),
        help="Exact Alpaca plan limit. Basic=200; Algo Trader Plus=10000.",
    )
    parser.add_argument(
        "--api-key-id", default=_env_credential("APCA_API_KEY_ID", "ALPACA_API_KEY_ID")
    )
    parser.add_argument(
        "--api-secret-key",
        default=_env_credential("APCA_API_SECRET_KEY", "ALPACA_API_SECRET_KEY"),
    )
    parser.add_argument("--feed", choices=["sip", "iex", "otc", "boats"], default="sip")
    parser.add_argument(
        "--adjustment", choices=["raw", "split", "dividend", "all"], default="raw"
    )
    parser.add_argument("--repair-overlap-days", type=int, default=7)
    parser.add_argument("--daily-stale-max-lag-days", type=int, default=0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--no-write-symbols", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    if args.requests_per_minute <= 0:
        raise SystemExit("--requests-per-minute must be positive")

    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    output_dir = args.output_root / ASSET_CLASS
    output_dir.mkdir(parents=True, exist_ok=True)
    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    records = _iter_limited(
        _load_symbol_records(args.symbols, args.symbols_file, output_dir), args.limit
    )
    if not records:
        raise SystemExit("no symbols to download")
    if not args.no_write_symbols:
        _write_symbol_manifest(records, output_dir)

    pending, results = _prepare_records(
        records,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        mode=args.mode,
        repair_overlap_days=args.repair_overlap_days,
        daily_stale_max_lag_days=args.daily_stale_max_lag_days,
        metadata_workers=args.metadata_workers,
    )
    pending.sort(key=lambda item: (item.effective_start, item.record.alpaca_symbol))
    batches = _chunked(pending, args.batch_size)

    print(
        f"[alpaca-us] mode={args.mode} symbols={len(records)} pending={len(pending)} batches={len(batches)} "
        f"start={start_date} end={end_date} output={output_dir} workers={args.workers} "
        f"batch_size={args.batch_size} rpm={args.requests_per_minute:g} "
        f"rps={args.requests_per_minute / 60.0:.6f} feed={args.feed} adjustment={args.adjustment}",
        flush=True,
    )

    api_requests = 0
    if batches:
        client = AlpacaClient(
            api_key_id=args.api_key_id,
            api_secret_key=args.api_secret_key,
            requests_per_minute=args.requests_per_minute,
            timeout=args.timeout,
            retries=args.retries,
            retry_backoff=args.retry_backoff,
        )

        def fetch(batch: list[PreparedRecord]):
            effective_start = min(item.effective_start for item in batch)
            bars, errors, requests_made = _fetch_batch_resilient(
                client,
                batch,
                start_date=effective_start,
                end_date=end_date,
                feed=args.feed,
                adjustment=args.adjustment,
            )
            return batch, bars, errors, requests_made

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = [executor.submit(fetch, batch) for batch in batches]
            iterator = as_completed(futures)
            if tqdm is not None:
                iterator = tqdm(
                    iterator, total=len(futures), desc="alpaca-us", unit="batch"
                )
            for future in iterator:
                batch, bars, errors, requests_made = future.result()
                api_requests += requests_made
                batch_results = _apply_batch(batch, bars, errors, end_date=end_date)
                results.extend(batch_results)
                for result in batch_results:
                    if result.status == "failed":
                        print(
                            f"[alpaca-us] failed {result.code}: {result.message}",
                            flush=True,
                        )

    results.sort(key=lambda item: item.code)
    ended_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    _write_reports(
        results,
        output_dir,
        started_at=started_at,
        ended_at=ended_at,
        api_requests=api_requests,
    )
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    print(f"[alpaca-us] api_requests={api_requests} status_counts={counts}", flush=True)
    return 1 if counts.get("failed", 0) else 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except (AlpacaRequestError, RuntimeError, ValueError) as exc:
        print(f"[alpaca-us] fatal: {exc}", file=sys.stderr, flush=True)
        exit_code = 2
    raise SystemExit(exit_code)
