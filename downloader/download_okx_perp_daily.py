from __future__ import annotations

import argparse
import json
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from common import resolve_end_date, run_parallel_tasks


BASE_URL = "https://www.okx.com"
INSTRUMENTS_ENDPOINT = "/api/v5/public/instruments"
HISTORY_CANDLES_ENDPOINT = "/api/v5/market/history-candles"
OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "adjclose", "Trading_Volume"]
KLINE_BAR = "15m"
CANDLE_INTERVAL_MS = 15 * 60 * 1000


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all OKX perpetual swap 15-minute bars to parquet files."
    )
    parser.add_argument("--output-dir", default="data_okx", help="Output folder.")
    parser.add_argument(
        "--mode",
        choices=["daily-update", "full"],
        default="daily-update",
        help="daily-update: only fetch missing dates; full: skip existing unless --refresh.",
    )
    parser.add_argument("--start-date", default="2019-01-01", help="Inclusive start date YYYY-MM-DD")
    parser.add_argument("--end-date", default="today", help="Inclusive end date YYYY-MM-DD or 'today'")
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


class OkxClient:
    def __init__(self, request_interval: float, max_retries: int, retry_base: float) -> None:
        self.request_interval = max(0.0, request_interval)
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

                if payload.get("code") == "0":
                    return payload

                msg = str(payload.get("msg") or "")
                code = str(payload.get("code") or "")
                retriable_code = {"50011", "50040", "50061"}
                if code in retriable_code and attempt < self.max_retries:
                    backoff = self.retry_base * (2**attempt)
                    time.sleep(min(backoff, 30.0))
                    continue
                raise RuntimeError(f"OKX API error code={code} msg={msg}")

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
        raise RuntimeError("OKX request failed without explicit error")


def _ms_to_date_string(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _fetch_swap_symbols(client: OkxClient, limit: int | None = None) -> list[SymbolRecord]:
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
                state=item.get("state"),
                list_time=_ms_to_date_string(int(item["listTime"])) if item.get("listTime") else None,
            )
        )

    records.sort(key=lambda x: x.okx_symbol)
    if limit is not None:
        return records[:limit]
    return records


def _normalize_candles(raw_rows: list[list[str]]) -> pd.DataFrame:
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
    client: OkxClient,
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
        except Exception:
            existing_df = None

        if mode == "full":
            rows = len(existing_df) if existing_df is not None else 0
            return DownloadResult(
                asset_class="crypto_okx_perp",
                code=record.code,
                okx_symbol=record.okx_symbol,
                market=record.market,
                status="skipped_existing",
                rows=rows,
                output_path=str(output_path),
            )

        if existing_df is not None and not existing_df.empty:
            effective_start_ms = _resolve_next_start_ms(existing_df, start_ms)
            if effective_start_ms > end_ms:
                return DownloadResult(
                    asset_class="crypto_okx_perp",
                    code=record.code,
                    okx_symbol=record.okx_symbol,
                    market=record.market,
                    status="skipped_up_to_date",
                    rows=len(existing_df),
                    output_path=str(output_path),
                )

    all_rows: list[list[str]] = []
    cursor_after: str | None = None
    seen_oldest: set[str] = set()

    while True:
        params: dict[str, Any] = {
            "instId": record.okx_symbol,
            "bar": KLINE_BAR,
            "limit": "100",
        }
        if cursor_after:
            params["after"] = cursor_after

        payload = client.get(HISTORY_CANDLES_ENDPOINT, params)
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

    filtered_rows = [row for row in all_rows if effective_start_ms <= int(row[0]) <= end_ms]
    df = _normalize_candles(filtered_rows)
    if df.empty:
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

    if existing_df is not None and not existing_df.empty:
        combined = pd.concat([_normalize_existing_dates(existing_df), df], ignore_index=True)
        combined = combined.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        added_rows = max(0, len(combined) - len(existing_df))
        if added_rows == 0:
            return DownloadResult(
                asset_class="crypto_okx_perp",
                code=record.code,
                okx_symbol=record.okx_symbol,
                market=record.market,
                status="skipped_up_to_date",
                rows=len(existing_df),
                output_path=str(output_path),
            )
        df = combined

    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    return DownloadResult(
        asset_class="crypto_okx_perp",
        code=record.code,
        okx_symbol=record.okx_symbol,
        market=record.market,
        status="updated",
        rows=len(df),
        output_path=str(output_path),
    )


def main() -> None:
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
    pd.DataFrame([asdict(s) for s in symbols]).to_csv(symbols_path, index=False)

    def _worker(record: SymbolRecord) -> DownloadResult:
        return _download_symbol_daily(
            client,
            record,
            output_dir,
            start_ms,
            end_ms,
            args.mode,
            args.refresh,
        )

    def _on_error(record: SymbolRecord, exc: Exception) -> DownloadResult:
        return DownloadResult(
            asset_class="crypto_okx_perp",
            code=record.code,
            okx_symbol=record.okx_symbol,
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
        desc="download:okx",
        unit="symbol",
        on_error=_on_error,
    )

    report_path = output_dir / "download_report.csv"
    summary_path = output_dir / "download_summary.json"

    result_df = pd.DataFrame([asdict(r) for r in results]).sort_values(["status", "okx_symbol"])
    result_df.to_csv(report_path, index=False)

    summary = {
        "asset_class": "crypto_okx_perp",
        "symbol_count": len(symbols),
        "row_count": int(result_df["rows"].sum()) if not result_df.empty else 0,
        "status_counts": {k: int(v) for k, v in result_df["status"].value_counts().to_dict().items()},
        "start_date": start_date,
        "end_date": end_date,
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[okx] symbols.csv -> {symbols_path}")
    print(f"[okx] download_report.csv -> {report_path}")
    print(f"[okx] download_summary.json -> {summary_path}")
    print(f"[okx] done: {json.dumps(summary, ensure_ascii=False)}")


if __name__ == "__main__":
    main()