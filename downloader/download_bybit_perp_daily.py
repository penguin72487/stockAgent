from __future__ import annotations

import argparse
import json
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from common import resolve_end_date, run_parallel_tasks


BASE_URL = "https://api.bybit.com"
INSTRUMENTS_ENDPOINT = "/v5/market/instruments-info"
KLINE_ENDPOINT = "/v5/market/kline"
OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "adjclose", "Trading_Volume"]
KLINE_INTERVAL = "15"
KLINE_INTERVAL_LABEL = "15m"
CANDLE_INTERVAL_MS = 15 * 60 * 1000
BYBIT_MAX_REQ_PER_SEC = 10.0
BYBIT_MIN_REQUEST_INTERVAL = 1.0 / BYBIT_MAX_REQ_PER_SEC
BYBIT_MAX_KLINE_LIMIT = "1000"
BYBIT_MAX_CANDLES_PER_REQUEST = int(BYBIT_MAX_KLINE_LIMIT)
BYBIT_WINDOW_SPAN_MS = (BYBIT_MAX_CANDLES_PER_REQUEST - 1) * CANDLE_INTERVAL_MS


@dataclass(slots=True)
class SymbolRecord:
    code: str
    name: str
    market: str
    bybit_symbol: str
    category: str
    base_coin: str | None
    quote_coin: str | None
    settle_coin: str | None
    contract_type: str | None
    status: str | None
    launch_time: str | None


@dataclass(slots=True)
class DownloadResult:
    asset_class: str
    code: str
    bybit_symbol: str
    market: str
    status: str
    rows: int
    output_path: str | None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Bybit perpetual futures 15-minute bars to parquet files."
    )
    parser.add_argument("--output-dir", default="data_bybit", help="Output folder.")
    parser.add_argument(
        "--mode",
        choices=["daily-update", "full"],
        default="daily-update",
        help="daily-update: only fetch missing dates; full: skip existing unless --refresh.",
    )
    parser.add_argument("--start-date", default="2019-01-01", help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end-date", default="today", help="Inclusive end date YYYY-MM-DD or 'today'")
    parser.add_argument(
        "--categories",
        nargs="+",
        default=["linear", "inverse"],
        help="Bybit categories to fetch: linear inverse (default: both)",
    )
    parser.add_argument("--workers", type=int, default=16, help="Parallel symbol workers")
    parser.add_argument("--limit", type=int, default=None, help="Optional symbol limit for quick tests")
    parser.add_argument("--refresh", action="store_true", help="Re-download even if parquet exists")
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.1,
        help="Global minimum seconds between API requests (default 0.1 = 10 req/s).",
    )
    parser.add_argument("--max-retries", type=int, default=8, help="Max retries per HTTP request")
    parser.add_argument("--retry-base", type=float, default=0.6, help="Base seconds for exponential backoff")
    return parser.parse_args()


def _resolve_categories(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        text = value.strip().lower()
        if text == "all":
            normalized.extend(["linear", "inverse"])
            continue
        if text in {"linear", "inverse"}:
            normalized.append(text)

    unique = sorted(set(normalized))
    if not unique:
        raise ValueError("No valid categories. Use: linear inverse or all.")
    return unique


def _date_to_ms(date_str: str, *, end_of_day: bool) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
    return int(dt.timestamp() * 1000)


def _resolve_next_start_ms(existing_df: pd.DataFrame, fallback_start_ms: int) -> int:
    if "date" not in existing_df.columns:
        return fallback_start_ms

    parsed = pd.to_datetime(existing_df["date"], errors="coerce", utc=True).dropna()
    if parsed.empty:
        return fallback_start_ms

    latest_ms = int(parsed.max().timestamp() * 1000)
    return max(fallback_start_ms, latest_ms + CANDLE_INTERVAL_MS)


def _ms_to_date_string(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _frame_matches_15m_interval(frame: pd.DataFrame) -> bool:
    if frame.empty or "date" not in frame.columns:
        return True

    parsed = pd.to_datetime(frame["date"], errors="coerce", utc=True).dropna().sort_values()
    if len(parsed) < 3:
        return True

    deltas = parsed.diff().dropna().dt.total_seconds()
    deltas = deltas[deltas > 0]
    if deltas.empty:
        return True

    median_delta = float(deltas.median())
    large_gap_share = float((deltas >= 12 * 60 * 60).mean())
    if large_gap_share > 0.05:
        return False
    midnight_share = float(
        ((parsed.dt.hour == 0) & (parsed.dt.minute == 0) & (parsed.dt.second == 0)).mean()
    )
    if midnight_share > 0.95 and median_delta >= 12 * 60 * 60:
        return False
    return median_delta <= (CANDLE_INTERVAL_MS / 1000) * 4


def _iter_windows(start_ms: int, end_ms: int) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    cursor = start_ms

    while cursor <= end_ms:
        chunk_end = min(cursor + BYBIT_WINDOW_SPAN_MS, end_ms)
        windows.append((cursor, chunk_end))
        cursor = chunk_end + CANDLE_INTERVAL_MS

    return windows


class BybitClient:
    def __init__(self, request_interval: float, max_retries: int, retry_base: float) -> None:
        self.request_interval = max(0.0, request_interval)
        if 0.0 < self.request_interval < BYBIT_MIN_REQUEST_INTERVAL:
            print(
                "[bybit] request_interval too small for 10 req/s limit; "
                f"clamp {self.request_interval} -> {BYBIT_MIN_REQUEST_INTERVAL:.3f}"
            )
            self.request_interval = BYBIT_MIN_REQUEST_INTERVAL
        self.max_retries = max(0, max_retries)
        self.retry_base = max(0.1, retry_base)
        self._lock = threading.Lock()
        self._last_request_time = 0.0

    def _wait_for_slot(self) -> None:
        if self.request_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_s = self.request_interval - elapsed
            if wait_s > 0:
                time.sleep(wait_s)
            self._last_request_time = time.monotonic()

    def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            self._wait_for_slot()
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
                    payload = json.load(response)

                if int(payload.get("retCode", -1)) == 0:
                    return payload

                code = str(payload.get("retCode"))
                message = str(payload.get("retMsg") or "")
                retriable_code = {"10006", "429", "10000"}
                if code in retriable_code and attempt < self.max_retries:
                    backoff = self.retry_base * (2**attempt)
                    time.sleep(min(backoff, 30.0))
                    continue
                raise RuntimeError(f"Bybit API error retCode={code} retMsg={message}")

            except HTTPError as exc:
                last_error = exc
                if exc.code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                    backoff = self.retry_base * (2**attempt)
                    time.sleep(min(backoff, 30.0))
                    continue
                raise
            except URLError as exc:
                last_error = exc
                if attempt < self.max_retries:
                    backoff = self.retry_base * (2**attempt)
                    time.sleep(min(backoff, 30.0))
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("Bybit request failed without explicit error")


def _fetch_perp_symbols(
    client: BybitClient,
    categories: list[str],
    limit: int | None = None,
) -> list[SymbolRecord]:
    records: list[SymbolRecord] = []

    for category in categories:
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {
                "category": category,
                "limit": "1000",
            }
            if cursor:
                params["cursor"] = cursor

            payload = client.get(INSTRUMENTS_ENDPOINT, params)
            result = payload.get("result", {})
            items = result.get("list", [])

            for item in items:
                if item.get("status") != "Trading":
                    continue
                contract_type = str(item.get("contractType") or "")
                if "Perpetual" not in contract_type:
                    continue

                symbol = item.get("symbol")
                if not symbol:
                    continue

                market = f"bybit_{category}_perp"
                records.append(
                    SymbolRecord(
                        code=symbol,
                        name=symbol,
                        market=market,
                        bybit_symbol=symbol,
                        category=category,
                        base_coin=item.get("baseCoin"),
                        quote_coin=item.get("quoteCoin"),
                        settle_coin=item.get("settleCoin"),
                        contract_type=item.get("contractType"),
                        status=item.get("status"),
                        launch_time=_ms_to_date_string(int(item["launchTime"]))
                        if item.get("launchTime")
                        else None,
                    )
                )

            cursor = result.get("nextPageCursor")
            if not cursor:
                break

    records.sort(key=lambda x: (x.category, x.bybit_symbol))
    if limit is not None:
        return records[:limit]
    return records


def _normalize_candles(raw_rows: list[list[str]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if len(row) < 7:
            continue

        ts = int(row[0])
        volume = float(row[5]) if row[5] else 0.0
        turnover = float(row[6]) if row[6] else 0.0
        rows.append(
            {
                "ts": ts,
                "date": _ms_to_date_string(ts),
                "open": float(row[1]),
                "max": float(row[2]),
                "min": float(row[3]),
                "close": float(row[4]),
                "adjclose": float(row[4]),
                "Trading_Volume": volume,
                "bybit_volume": volume,
                "bybit_turnover": turnover,
            }
        )

    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(rows)
    df = df.sort_values("ts").drop_duplicates(subset=["date"], keep="last")
    return df.drop(columns=["ts"])


def _normalize_existing_dates(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return frame

    normalized = frame.copy()
    parsed = pd.to_datetime(normalized["date"], errors="coerce", utc=True)
    normalized = normalized.assign(date=parsed.dt.strftime("%Y-%m-%d %H:%M:%S"))
    normalized = normalized.dropna(subset=["date"]).sort_values("date")
    return normalized.reset_index(drop=True)


def _download_symbol_daily(
    client: BybitClient,
    record: SymbolRecord,
    output_dir: Path,
    start_ms: int,
    end_ms: int,
    mode: str,
    refresh: bool,
) -> DownloadResult:
    output_path = output_dir / f"{record.code}_features.parquet"
    existing_df: pd.DataFrame | None = None
    effective_start_ms = start_ms

    if output_path.exists() and not refresh:
        try:
            existing_df = pd.read_parquet(output_path)
            if not _frame_matches_15m_interval(existing_df):
                print(
                    f"[bybit] {record.bybit_symbol}: existing parquet does not look like "
                    f"{KLINE_INTERVAL_LABEL}; rebuilding from start_date"
                )
                existing_df = None
        except Exception:
            existing_df = None

        if mode == "full" and existing_df is not None:
            rows = len(existing_df) if existing_df is not None else 0
            return DownloadResult(
                asset_class="crypto_bybit_perp",
                code=record.code,
                bybit_symbol=record.bybit_symbol,
                market=record.market,
                status="skipped_existing",
                rows=rows,
                output_path=str(output_path),
            )

        if existing_df is not None and not existing_df.empty:
            effective_start_ms = _resolve_next_start_ms(existing_df, start_ms)
            if effective_start_ms > end_ms:
                return DownloadResult(
                    asset_class="crypto_bybit_perp",
                    code=record.code,
                    bybit_symbol=record.bybit_symbol,
                    market=record.market,
                    status="skipped_up_to_date",
                    rows=len(existing_df),
                    output_path=str(output_path),
                )

    all_rows: list[list[str]] = []
    for window_start, window_end in _iter_windows(effective_start_ms, end_ms):
        payload = client.get(
            KLINE_ENDPOINT,
            {
                "category": record.category,
                "symbol": record.bybit_symbol,
                "interval": KLINE_INTERVAL,
                "start": str(window_start),
                "end": str(window_end),
                "limit": BYBIT_MAX_KLINE_LIMIT,
            },
        )
        chunk = payload.get("result", {}).get("list", [])
        if chunk:
            all_rows.extend(chunk)

    if not all_rows:
        if existing_df is not None and not existing_df.empty:
            return DownloadResult(
                asset_class="crypto_bybit_perp",
                code=record.code,
                bybit_symbol=record.bybit_symbol,
                market=record.market,
                status="skipped_up_to_date",
                rows=len(existing_df),
                output_path=str(output_path),
            )
        return DownloadResult(
            asset_class="crypto_bybit_perp",
            code=record.code,
            bybit_symbol=record.bybit_symbol,
            market=record.market,
            status="skipped_no_data",
            rows=0,
            output_path=None,
            message="No candles returned by Bybit.",
        )

    filtered_rows = [row for row in all_rows if effective_start_ms <= int(row[0]) <= end_ms]
    df = _normalize_candles(filtered_rows)
    if df.empty:
        if existing_df is not None and not existing_df.empty:
            return DownloadResult(
                asset_class="crypto_bybit_perp",
                code=record.code,
                bybit_symbol=record.bybit_symbol,
                market=record.market,
                status="skipped_up_to_date",
                rows=len(existing_df),
                output_path=str(output_path),
            )
        return DownloadResult(
            asset_class="crypto_bybit_perp",
            code=record.code,
            bybit_symbol=record.bybit_symbol,
            market=record.market,
            status="skipped_no_data",
            rows=0,
            output_path=None,
            message="No rows in requested date range.",
        )

    if existing_df is not None and not existing_df.empty:
        combined = pd.concat([_normalize_existing_dates(existing_df), df], ignore_index=True)
        combined = combined.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        added_rows = max(0, len(combined) - len(existing_df))
        if added_rows == 0:
            return DownloadResult(
                asset_class="crypto_bybit_perp",
                code=record.code,
                bybit_symbol=record.bybit_symbol,
                market=record.market,
                status="skipped_up_to_date",
                rows=len(existing_df),
                output_path=str(output_path),
            )
        df = combined

    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    return DownloadResult(
        asset_class="crypto_bybit_perp",
        code=record.code,
        bybit_symbol=record.bybit_symbol,
        market=record.market,
        status="updated",
        rows=len(df),
        output_path=str(output_path),
    )


def main() -> None:
    args = parse_args()
    categories = _resolve_categories(args.categories)
    output_dir = Path(args.output_dir)

    start_date = args.start_date.strip()
    end_date = resolve_end_date(args.end_date)
    start_ms = _date_to_ms(start_date, end_of_day=False)
    end_ms = _date_to_ms(end_date, end_of_day=True)

    client = BybitClient(
        request_interval=args.request_interval,
        max_retries=args.max_retries,
        retry_base=args.retry_base,
    )

    symbols = _fetch_perp_symbols(client, categories=categories, limit=args.limit)
    if not symbols:
        raise RuntimeError("No Bybit perpetual symbols found for selected categories.")

    output_dir.mkdir(parents=True, exist_ok=True)
    symbols_path = output_dir / "symbols.csv"
    pd.DataFrame([asdict(s) for s in symbols]).to_csv(symbols_path, index=False)

    total_symbols = len(symbols)
    print(
        "[bybit] start "
        f"symbols={total_symbols} interval={KLINE_INTERVAL_LABEL} "
        f"workers={args.workers} request_interval={args.request_interval}s"
    )

    progress_lock = threading.Lock()
    started_symbols = 0
    finished_symbols = 0

    def _worker(record: SymbolRecord) -> DownloadResult:
        nonlocal started_symbols, finished_symbols

        with progress_lock:
            started_symbols += 1
            started_idx = started_symbols

        if started_idx <= 5 or started_idx % 50 == 0:
            print(
                f"[bybit] start symbol {started_idx}/{total_symbols} "
                f"{record.bybit_symbol} ({record.category})"
            )

        result = _download_symbol_daily(
            client,
            record,
            output_dir,
            start_ms,
            end_ms,
            args.mode,
            args.refresh,
        )

        with progress_lock:
            finished_symbols += 1
            done_idx = finished_symbols

        if done_idx <= 5 or done_idx % 25 == 0:
            print(
                f"[bybit] done symbol {done_idx}/{total_symbols} "
                f"{record.bybit_symbol} status={result.status} rows={result.rows}"
            )

        return result

    def _on_error(record: SymbolRecord, exc: Exception) -> DownloadResult:
        return DownloadResult(
            asset_class="crypto_bybit_perp",
            code=record.code,
            bybit_symbol=record.bybit_symbol,
            market=record.market,
            status="failed",
            rows=0,
            output_path=None,
            message=str(exc),
        )

    results = run_parallel_tasks(
        symbols,
        _worker,
        max_workers=args.workers,
        desc="download:bybit",
        unit="symbol",
        on_error=_on_error,
    )

    report_path = output_dir / "download_report.csv"
    summary_path = output_dir / "download_summary.json"

    result_df = pd.DataFrame([asdict(r) for r in results]).sort_values(["status", "bybit_symbol"])
    result_df.to_csv(report_path, index=False)

    summary = {
        "asset_class": "crypto_bybit_perp",
        "interval": KLINE_INTERVAL_LABEL,
        "symbol_count": len(symbols),
        "row_count": int(result_df["rows"].sum()) if not result_df.empty else 0,
        "status_counts": {k: int(v) for k, v in result_df["status"].value_counts().to_dict().items()},
        "categories": categories,
        "start_date": start_date,
        "end_date": end_date,
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[bybit] symbols.csv -> {symbols_path}")
    print(f"[bybit] download_report.csv -> {report_path}")
    print(f"[bybit] download_summary.json -> {summary_path}")
    print(f"[bybit] done: {json.dumps(summary, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
