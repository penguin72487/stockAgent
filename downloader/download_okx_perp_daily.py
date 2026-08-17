from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (  # noqa: E402
    PersistentProgress,
    SharedRateLimiter,
    atomic_write_text,
    describe_rate_limit,
    provider_rate_limit,
    resolve_end_date,
    resolve_incremental_reconcile_start_ms,
    resolve_request_interval,
    retry_delay_seconds,
    run_parallel_tasks,
)
from okx_historical_features import (  # noqa: E402
    FEATURE_STAGE_IDS,
    feature_catalog_payload,
    result_rows as historical_feature_result_rows,
    run_historical_feature_downloads,
)


BASE_URL = "https://www.okx.com"
INSTRUMENTS_ENDPOINT = "/api/v5/public/instruments"
HISTORY_CANDLES_ENDPOINT = "/api/v5/market/history-candles"
OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "adjclose", "Trading_Volume"]
KLINE_BAR = "1m"
CANDLE_INTERVAL_MS = 60 * 1000
OKX_HISTORY_LIMIT = "300"


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path))


def _read_parquet_row_count(path: Path) -> int:
    return int(pq.ParquetFile(path, memory_map=True).metadata.num_rows)


def _read_date_column(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path, columns=["date"], memory_map=True))


def _write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            frame.to_arrow(), tmp_path, compression="snappy", write_statistics=True
        )
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


@dataclass(slots=True)
class SymbolRecord:
    code: str
    name: str
    market: str
    okx_symbol: str
    base_ccy: str | None
    quote_ccy: str | None
    settle_ccy: str | None
    ct_type: str | None
    inst_family: str | None
    uly: str | None
    state: str | None
    list_time: str | None


@dataclass(slots=True)
class DownloadResult:
    asset_class: str
    code: str
    okx_symbol: str
    market: str
    status: str
    rows: int
    output_path: str | None
    message: str | None = None


@dataclass(slots=True)
class ExistingCandleInfo:
    rows: int
    latest_ms: int | None
    interval_ok: bool
    earliest_ms: int | None = None
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all OKX perpetual swap one-minute bars to parquet files."
    )
    parser.add_argument("--output-dir", default="data_okx/1m", help="Output folder.")
    parser.add_argument(
        "--mode",
        choices=["incremental", "daily-update", "full"],
        default="incremental",
        help="incremental: reconcile missing head/tail coverage; daily-update: deprecated alias; full: reconcile requested coverage; --refresh forces rebuild.",
    )
    parser.add_argument(
        "--start-date", default="2019-01-01", help="Inclusive start date YYYY-MM-DD"
    )
    parser.add_argument(
        "--end-date", default="today", help="Inclusive end date YYYY-MM-DD or 'today'"
    )
    parser.add_argument(
        "--workers", type=int, default=16, help="Parallel symbol workers"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Optional symbol limit for quick tests"
    )
    parser.add_argument(
        "--refresh", action="store_true", help="Re-download even if parquet exists"
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help="Global minimum seconds between API requests. Default uses official OKX history-candles profile.",
    )
    parser.add_argument(
        "--max-retries", type=int, default=8, help="Max retries per HTTP request"
    )
    parser.add_argument(
        "--retry-base",
        type=float,
        default=0.6,
        help="Base seconds for exponential backoff",
    )
    parser.add_argument(
        "--skip-historical-features",
        action="store_true",
        help=(
            "Download only canonical 1m candles. By default, point-in-time "
            "reconstructable OKX historical features are also updated."
        ),
    )
    parser.add_argument(
        "--skip-funding-archive",
        action="store_true",
        help=(
            "Skip OKX monthly historical funding ZIP files and use only the "
            "three-month REST funding history."
        ),
    )
    parser.add_argument(
        "--feature-workers",
        type=int,
        default=None,
        help="Parallel workers for historical features; defaults to --workers.",
    )
    return parser.parse_args()


def _date_to_ms(date_str: str, *, end_of_day: bool) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    return int(dt.timestamp() * 1000)


def _normalize_date_frame(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty() or "date" not in frame.columns:
        return frame
    normalized = frame.with_columns(
        pl.col("date")
        .str.to_datetime(strict=False)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .alias("date")
        if frame.schema.get("date") == pl.String
        else pl.col("date")
        .cast(pl.Datetime("us"), strict=False)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
        .alias("date")
    )
    return normalized.drop_nulls("date").sort("date")


class OkxClient:
    def __init__(
        self, request_interval: float | None, max_retries: int, retry_base: float
    ) -> None:
        self.request_interval = resolve_request_interval(
            "okx_history_candles", request_interval
        )
        self.max_retries = max(0, max_retries)
        self.retry_base = max(0.1, retry_base)
        self._limiter_lock = threading.Lock()
        self._limiters: dict[str, SharedRateLimiter] = {}
        print(
            f"[okx] {describe_rate_limit('okx_history_candles', self.request_interval)}",
            flush=True,
        )

    def _limiter_for_request(
        self, path: str, params: dict[str, Any] | None = None
    ) -> SharedRateLimiter:
        profile_by_path = {
            HISTORY_CANDLES_ENDPOINT: "okx_history_candles",
            "/api/v5/market/history-mark-price-candles": (
                "okx_history_mark_price_candles"
            ),
            "/api/v5/market/history-index-candles": "okx_history_index_candles",
            "/api/v5/public/funding-rate-history": "okx_funding_rate_history",
        }
        profile_name = profile_by_path.get(path)
        interval = self.request_interval
        limiter_name = f"okx:{path}"
        if profile_name is not None:
            interval = max(interval, provider_rate_limit(profile_name).interval_seconds)
            limiter_name = profile_name
        if path == "/api/v5/public/funding-rate-history":
            instrument = str((params or {}).get("instId") or "unknown")
            limiter_name = f"{limiter_name}:{instrument}"

        with self._limiter_lock:
            limiter = self._limiters.get(limiter_name)
            if limiter is None:
                limiter = SharedRateLimiter(interval, name=limiter_name)
                self._limiters[limiter_name] = limiter
            return limiter

    def _defer_retry(
        self,
        limiter: SharedRateLimiter,
        attempt: int,
        *,
        retry_after: str | None = None,
    ) -> None:
        limiter.defer(
            retry_delay_seconds(
                attempt,
                base=self.retry_base,
                retry_after=retry_after,
            )
        )

    def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        limiter = self._limiter_for_request(path, params)
        for attempt in range(self.max_retries + 1):
            limiter.wait()
            url = f"{BASE_URL}{path}?{urlencode(params)}"
            req = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                },
            )
            try:
                with urlopen(req, timeout=30) as response:
                    retry_after = response.headers.get("Retry-After")
                    payload = json.load(response)

                if payload.get("code") == "0":
                    return payload

                msg = str(payload.get("msg") or "")
                code = str(payload.get("code") or "")
                retriable_code = {"50011", "50040", "50061"}
                if code in retriable_code and attempt < self.max_retries:
                    self._defer_retry(limiter, attempt, retry_after=retry_after)
                    continue
                raise RuntimeError(f"OKX API error code={code} msg={msg}")

            except HTTPError as exc:
                last_error = exc
                if exc.code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    self._defer_retry(
                        limiter,
                        attempt,
                        retry_after=(
                            exc.headers.get("Retry-After")
                            if exc.headers is not None
                            else None
                        ),
                    )
                    continue
                raise
            except URLError as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self._defer_retry(limiter, attempt)
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("OKX request failed without explicit error")

    def get_bytes(self, url: str) -> bytes:
        last_error: Exception | None = None
        limiter = self._limiter_for_request("okx-public-archive")
        for attempt in range(self.max_retries + 1):
            limiter.wait()
            req = Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/zip,application/octet-stream,*/*",
                },
            )
            try:
                with urlopen(req, timeout=60) as response:
                    return response.read()
            except HTTPError as exc:
                last_error = exc
                if exc.code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    self._defer_retry(
                        limiter,
                        attempt,
                        retry_after=(
                            exc.headers.get("Retry-After")
                            if exc.headers is not None
                            else None
                        ),
                    )
                    continue
                raise
            except URLError as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self._defer_retry(limiter, attempt)
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("OKX binary request failed without explicit error")


def _ms_to_date_string(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _latest_closed_candle_start_ms(now: datetime | None = None) -> int:
    current = (
        now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    )
    current_ms = int(current.timestamp() * 1000)
    return (current_ms // CANDLE_INTERVAL_MS) * CANDLE_INTERVAL_MS - CANDLE_INTERVAL_MS


def _frames_equal(left: pl.DataFrame, right: pl.DataFrame) -> bool:
    common = [column for column in left.columns if column in right.columns]
    if not common or left.height != right.height:
        return False
    return left.select(common).equals(right.select(common))


def _merge_existing_with_fresh(
    existing_df: pl.DataFrame, fresh_df: pl.DataFrame, effective_start_ms: int
) -> tuple[pl.DataFrame, bool]:
    existing = _normalize_date_frame(existing_df)
    cutoff = _ms_to_date_string(effective_start_ms)
    kept_existing = (
        existing.filter(pl.col("date") < cutoff)
        if "date" in existing.columns
        else existing
    )
    overlap_existing = (
        existing.filter(pl.col("date") >= cutoff)
        if "date" in existing.columns
        else pl.DataFrame()
    )
    overlap = pl.concat(
        [
            overlap_existing.with_columns(pl.lit(0).alias("__source_priority")),
            fresh_df.with_columns(pl.lit(1).alias("__source_priority")),
        ],
        how="diagonal_relaxed",
    ).sort(["date", "__source_priority"])
    value_columns = [
        column
        for column in overlap.columns
        if column not in {"date", "__source_priority"}
    ]
    merged_overlap = (
        overlap.group_by("date", maintain_order=True)
        .agg(
            [
                pl.col(column).drop_nulls().last().alias(column)
                for column in value_columns
            ]
        )
        .sort("date")
    )
    combined = pl.concat(
        [kept_existing, merged_overlap],
        how="diagonal_relaxed",
    ).sort("date")
    return combined, not _frames_equal(existing, combined)


def _frame_matches_1m_interval(frame: pl.DataFrame) -> bool:
    if frame.is_empty() or "date" not in frame.columns:
        return True

    parsed = (
        _normalize_date_frame(frame)
        .select(pl.col("date").str.to_datetime(strict=False).alias("date"))
        .drop_nulls("date")
    )
    if parsed.height < 3:
        return True

    deltas = (
        parsed.sort("date")
        .select(pl.col("date").diff().dt.total_seconds().alias("delta"))
        .drop_nulls("delta")
        .filter(pl.col("delta") > 0)
    )
    if deltas.is_empty():
        return True

    median_delta = float(deltas.select(pl.col("delta").median()).item())
    large_gap_share = float(
        deltas.select(pl.col("delta").ge(12 * 60 * 60).mean()).item()
    )
    if large_gap_share > 0.05:
        return False
    midnight_share = float(
        parsed.select(
            (
                pl.col("date").dt.hour().eq(0)
                & pl.col("date").dt.minute().eq(0)
                & pl.col("date").dt.second().eq(0)
            )
            .mean()
            .alias("midnight_share")
        ).item()
    )
    if midnight_share > 0.95 and median_delta >= 12 * 60 * 60:
        return False
    return median_delta <= (CANDLE_INTERVAL_MS / 1000) * 4


def _latest_ms_from_date_frame(frame: pl.DataFrame) -> int | None:
    if frame.is_empty() or "date" not in frame.columns:
        return None
    parsed = _normalize_date_frame(frame)
    if parsed.is_empty():
        return None
    latest = parsed.select(pl.col("date").str.to_datetime(strict=False).max()).item()
    if latest is None:
        return None
    return int(latest.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _earliest_ms_from_date_frame(frame: pl.DataFrame) -> int | None:
    if frame.is_empty() or "date" not in frame.columns:
        return None
    parsed = _normalize_date_frame(frame)
    if parsed.is_empty():
        return None
    earliest = parsed.select(pl.col("date").str.to_datetime(strict=False).min()).item()
    if earliest is None:
        return None
    return int(earliest.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _load_existing_candle_info(path: Path) -> ExistingCandleInfo:
    try:
        row_count = _read_parquet_row_count(path)
        schema_names = set(pq.read_schema(path).names)
        if "date" not in schema_names:
            return ExistingCandleInfo(
                rows=row_count,
                latest_ms=None,
                interval_ok=False,
                error="missing date column",
            )
        date_frame = _read_date_column(path)
        return ExistingCandleInfo(
            rows=row_count,
            latest_ms=_latest_ms_from_date_frame(date_frame),
            interval_ok=_frame_matches_1m_interval(date_frame),
            earliest_ms=_earliest_ms_from_date_frame(date_frame),
        )
    except Exception as exc:
        return ExistingCandleInfo(
            rows=0,
            latest_ms=None,
            interval_ok=False,
            error=f"{type(exc).__name__}: {exc}",
        )


def _fetch_swap_symbols(
    client: OkxClient, limit: int | None = None
) -> list[SymbolRecord]:
    payload = client.get(INSTRUMENTS_ENDPOINT, {"instType": "SWAP"})
    records: list[SymbolRecord] = []

    for item in payload.get("data", []):
        if item.get("state") != "live":
            continue
        inst_id = item.get("instId")
        if not inst_id:
            continue
        code = inst_id.replace("-", "")
        records.append(
            SymbolRecord(
                code=code,
                name=inst_id,
                market="okx_swap",
                okx_symbol=inst_id,
                base_ccy=item.get("baseCcy"),
                quote_ccy=item.get("quoteCcy"),
                settle_ccy=item.get("settleCcy"),
                ct_type=item.get("ctType"),
                inst_family=item.get("instFamily"),
                uly=item.get("uly"),
                state=item.get("state"),
                list_time=_ms_to_date_string(int(item["listTime"]))
                if item.get("listTime")
                else None,
            )
        )

    records.sort(key=lambda x: x.okx_symbol)
    if limit is not None:
        return records[:limit]
    return records


def _normalize_candles(raw_rows: list[list[str]]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if len(row) < 9:
            continue
        ts = int(row[0])
        rows.append(
            {
                "ts": ts,
                "date": _ms_to_date_string(ts),
                "open": float(row[1]),
                "max": float(row[2]),
                "min": float(row[3]),
                "close": float(row[4]),
                "adjclose": float(row[4]),
                "Trading_Volume": float(row[7]) if row[7] else float(row[5]),
                "okx_volume_contract": float(row[5]) if row[5] else 0.0,
                "okx_volume_base": float(row[6]) if row[6] else 0.0,
                "okx_volume_quote": float(row[7]) if row[7] else 0.0,
                "okx_confirm": int(row[8]) if row[8] else 0,
            }
        )

    if not rows:
        return pl.DataFrame({column: [] for column in OUTPUT_COLUMNS})

    return (
        pl.DataFrame(rows)
        .sort("ts")
        .unique(subset=["date"], keep="last")
        .sort("ts")
        .drop("ts")
    )


def _download_symbol_1m(
    client: OkxClient,
    record: SymbolRecord,
    output_dir: Path,
    start_ms: int,
    end_ms: int,
    mode: str,
    refresh: bool,
    page_progress_callback: Any = None,
) -> DownloadResult:
    output_path = output_dir / f"{record.code}_features.parquet"
    existing_info: ExistingCandleInfo | None = None
    effective_start_ms = start_ms
    if record.list_time:
        effective_start_ms = max(
            effective_start_ms,
            int(
                datetime.strptime(record.list_time, "%Y-%m-%d %H:%M:%S")
                .replace(tzinfo=timezone.utc)
                .timestamp()
                * 1000
            ),
        )

    if output_path.exists() and not refresh:
        existing_info = _load_existing_candle_info(output_path)
        if existing_info.error is not None or not existing_info.interval_ok:
            print(
                f"[okx] {record.okx_symbol}: existing parquet does not look like "
                f"{KLINE_BAR}; rebuilding from start_date"
            )
            existing_info = None

        if existing_info is not None and existing_info.rows > 0:
            effective_start_ms, _ = resolve_incremental_reconcile_start_ms(
                expected_first_ms=effective_start_ms,
                earliest_existing_ms=existing_info.earliest_ms,
                latest_existing_ms=existing_info.latest_ms,
                overlap_ms=CANDLE_INTERVAL_MS,
            )
            if effective_start_ms > end_ms:
                return DownloadResult(
                    asset_class="crypto_okx_perp",
                    code=record.code,
                    okx_symbol=record.okx_symbol,
                    market=record.market,
                    status="skipped_up_to_date",
                    rows=existing_info.rows,
                    output_path=str(output_path),
                )

    all_rows: list[list[str]] = []
    cursor_after: str | None = None
    seen_oldest: set[str] = set()

    while True:
        params: dict[str, Any] = {
            "instId": record.okx_symbol,
            "bar": KLINE_BAR,
            "limit": OKX_HISTORY_LIMIT,
        }
        if cursor_after:
            params["after"] = cursor_after

        payload = client.get(HISTORY_CANDLES_ENDPOINT, params)
        if page_progress_callback is not None:
            page_progress_callback(record.code)
        chunk = payload.get("data", [])
        if not chunk:
            break

        all_rows.extend(chunk)

        oldest_ms = int(chunk[-1][0])
        if oldest_ms < effective_start_ms:
            break

        cursor_after = chunk[-1][0]
        if cursor_after in seen_oldest:
            break
        seen_oldest.add(cursor_after)

    if not all_rows:
        return DownloadResult(
            asset_class="crypto_okx_perp",
            code=record.code,
            okx_symbol=record.okx_symbol,
            market=record.market,
            status="failed",
            rows=0,
            output_path=None,
            message="No candles returned by OKX.",
        )

    closed_end_ms = min(end_ms, _latest_closed_candle_start_ms())
    filtered_rows = [
        row
        for row in all_rows
        if effective_start_ms <= int(row[0]) <= closed_end_ms
        and (len(row) < 9 or str(row[8]) == "1")
    ]
    df = _normalize_candles(filtered_rows)
    if df.is_empty():
        if existing_info is not None and existing_info.rows > 0:
            return DownloadResult(
                asset_class="crypto_okx_perp",
                code=record.code,
                okx_symbol=record.okx_symbol,
                market=record.market,
                status="skipped_up_to_date",
                rows=existing_info.rows,
                output_path=str(output_path),
            )
        return DownloadResult(
            asset_class="crypto_okx_perp",
            code=record.code,
            okx_symbol=record.okx_symbol,
            market=record.market,
            status="failed",
            rows=0,
            output_path=None,
            message="No rows in requested date range.",
        )

    if existing_info is not None and existing_info.rows > 0:
        existing_df = _read_parquet(output_path)
        combined, changed = _merge_existing_with_fresh(
            existing_df, df, effective_start_ms
        )
        if not changed:
            return DownloadResult(
                asset_class="crypto_okx_perp",
                code=record.code,
                okx_symbol=record.okx_symbol,
                market=record.market,
                status="skipped_up_to_date",
                rows=existing_info.rows,
                output_path=str(output_path),
            )
        df = combined

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(df, output_path)

    return DownloadResult(
        asset_class="crypto_okx_perp",
        code=record.code,
        okx_symbol=record.okx_symbol,
        market=record.market,
        status="updated",
        rows=df.height,
        output_path=str(output_path),
    )


def main() -> None:
    started_at = datetime.now(timezone.utc)
    args = parse_args()
    output_dir = Path(args.output_dir)

    start_date = args.start_date.strip()
    end_date = resolve_end_date(args.end_date)
    start_ms = _date_to_ms(start_date, end_of_day=False)
    end_ms = _date_to_ms(end_date, end_of_day=True)

    client = OkxClient(
        request_interval=args.request_interval,
        max_retries=args.max_retries,
        retry_base=args.retry_base,
    )

    symbols = _fetch_swap_symbols(client, limit=args.limit)
    if not symbols:
        raise RuntimeError("No live OKX SWAP symbols found.")

    output_dir.mkdir(parents=True, exist_ok=True)
    symbols_path = output_dir / "symbols.csv"
    atomic_write_text(
        symbols_path,
        pl.DataFrame([asdict(s) for s in symbols]).write_csv(),
    )
    closed_end_ms = min(end_ms, _latest_closed_candle_start_ms())
    expected_candle_pages = 0
    for record in symbols:
        output_path = output_dir / f"{record.code}_features.parquet"
        if output_path.is_file() and not args.refresh:
            expected_candle_pages += 1
            continue
        listing_start = start_ms
        if record.list_time:
            listing_start = max(
                listing_start,
                int(
                    datetime.strptime(record.list_time, "%Y-%m-%d %H:%M:%S")
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                    * 1000
                ),
            )
        candle_count = max(
            0, (closed_end_ms - listing_start) // CANDLE_INTERVAL_MS + 1
        )
        expected_candle_pages += max(
            1,
            (candle_count + int(OKX_HISTORY_LIMIT) - 1)
            // int(OKX_HISTORY_LIMIT),
        )
    pipeline_progress = PersistentProgress(
        output_dir / "progress.json",
        label="OKX 永續合約 1 分鐘 K線與歷史特徵",
        total=expected_candle_pages
        + len(symbols)
        * (0 if args.skip_historical_features else len(FEATURE_STAGE_IDS)),
        unit="request-page-or-feature-stage",
        basis=(
            "completed one-minute request pages and feature stages divided by full "
            "elapsed time; funding archives can change the estimate"
        ),
        started_at=started_at,
    )

    def _worker(record: SymbolRecord) -> DownloadResult:
        result = _download_symbol_1m(
            client,
            record,
            output_dir,
            start_ms,
            end_ms,
            args.mode,
            args.refresh,
            page_progress_callback=lambda _code: pipeline_progress.update(
                "candles", "page_fetched"
            ),
        )
        return result

    def _on_error(record: SymbolRecord, exc: Exception) -> DownloadResult:
        result = DownloadResult(
            asset_class="crypto_okx_perp",
            code=record.code,
            okx_symbol=record.okx_symbol,
            market=record.market,
            status="failed",
            rows=0,
            output_path=None,
            message=str(exc),
        )
        pipeline_progress.update("candles", result.status)
        return result

    results = run_parallel_tasks(
        symbols,
        _worker,
        max_workers=args.workers,
        desc="download:okx",
        unit="symbol",
        on_error=_on_error,
    )

    feature_catalog_path = output_dir / "okx_historical_feature_catalog.json"
    atomic_write_text(
        feature_catalog_path,
        json.dumps(feature_catalog_payload(), ensure_ascii=False, indent=2) + "\n",
    )

    historical_feature_results = []
    if not args.skip_historical_features:
        historical_feature_results = run_historical_feature_downloads(
            client,
            symbols,
            output_dir,
            start_ms=start_ms,
            end_ms=end_ms,
            workers=args.feature_workers or args.workers,
            include_funding_archive=not args.skip_funding_archive,
            stage_progress_callback=lambda _code, stage, status: (
                pipeline_progress.update(stage, status)
            ),
        )

    historical_feature_report_path = output_dir / "historical_feature_report.csv"
    feature_rows = historical_feature_result_rows(historical_feature_results)
    feature_report = (
        pl.DataFrame(feature_rows, infer_schema_length=None)
        if feature_rows
        else pl.DataFrame(
            schema={
                "code": pl.String,
                "okx_symbol": pl.String,
                "status": pl.String,
                "rows": pl.Int64,
                "output_path": pl.String,
                "changed": pl.Boolean,
                "stage_status_json": pl.String,
                "coverage_json": pl.String,
                "errors_json": pl.String,
            }
        )
    )
    atomic_write_text(
        historical_feature_report_path,
        feature_report.write_csv(),
    )

    report_path = output_dir / "download_report.csv"
    summary_path = output_dir / "download_summary.json"

    historical_by_code = {result.code: result for result in historical_feature_results}
    result_rows = []
    for result in results:
        row = asdict(result)
        historical = historical_by_code.get(result.code)
        row.update(
            {
                "historical_feature_status": (
                    historical.status
                    if historical is not None
                    else "disabled_or_missing"
                ),
                "historical_feature_changed": (
                    historical.changed if historical is not None else False
                ),
                "historical_feature_coverage_json": (
                    historical.coverage_json if historical is not None else "{}"
                ),
                "historical_feature_errors_json": (
                    historical.errors_json if historical is not None else "{}"
                ),
            }
        )
        result_rows.append(row)
    result_df = (
        pl.DataFrame(result_rows, infer_schema_length=None).sort(
            ["status", "okx_symbol"]
        )
        if result_rows
        else pl.DataFrame()
    )
    atomic_write_text(report_path, result_df.write_csv())
    status_counts: dict[str, int] = {}
    row_count = 0
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
        row_count += int(result.rows)
    historical_status_counts: dict[str, int] = {}
    for result in historical_feature_results:
        historical_status_counts[result.status] = (
            historical_status_counts.get(result.status, 0) + 1
        )

    summary = {
        "asset_class": "crypto_okx_perp",
        "interval": KLINE_BAR,
        "symbol_count": len(symbols),
        "row_count": row_count,
        "status_counts": status_counts,
        "historical_features_enabled": not args.skip_historical_features,
        "funding_archive_enabled": (
            not args.skip_historical_features and not args.skip_funding_archive
        ),
        "historical_feature_status_counts": historical_status_counts,
        "historical_feature_report": str(historical_feature_report_path),
        "historical_feature_catalog": str(feature_catalog_path),
        "start_date": start_date,
        "end_date": end_date,
    }
    atomic_write_text(
        summary_path,
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
    )
    feature_incomplete = any(
        result.status in {"failed", "partial"} for result in historical_feature_results
    )
    pipeline_progress.finish(
        failed=any(result.status == "failed" for result in results)
        or feature_incomplete
    )

    print(f"[okx] symbols.csv -> {symbols_path}")
    print(f"[okx] download_report.csv -> {report_path}")
    print(f"[okx] download_summary.json -> {summary_path}")
    print(f"[okx] historical_feature_report.csv -> {historical_feature_report_path}")
    print(f"[okx] okx_historical_feature_catalog.json -> {feature_catalog_path}")
    print(f"[okx] done: {json.dumps(summary, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
