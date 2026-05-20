from __future__ import annotations

import argparse
import csv
import contextlib
import io
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from tqdm import tqdm


ISIN_SOURCES = {
    "listed": ("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", ".TW"),
    "otc": ("https://isin.twse.com.tw/isin/C_public.jsp?strMode=4", ".TWO"),
}
CODE_NAME_PATTERN = re.compile(r"^(?P<code>\d{4,6}[A-Z]{0,2})[\s\u3000]+(?P<name>.+)$")
OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "Trading_Volume"]
PROBE_STATUSES = {"historical_found", "no_history", "failed"}
ASSET_CFICODE_PREFIXES = ("ES", "CE")


@dataclass(slots=True)
class SymbolRecord:
    code: str
    name: str
    market: str
    yahoo_symbol: str


@dataclass(slots=True)
class DownloadResult:
    code: str
    yahoo_symbol: str
    market: str
    status: str
    rows: int
    output_path: str | None
    message: str | None = None


@dataclass(slots=True)
class FillResult:
    code: str
    status: str
    rows_before: int
    rows_after: int
    filled_rows: int
    first_date: str | None
    last_date: str | None
    output_path: str
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Taiwan stock OHLCV history from Yahoo Finance into parquet files.",
    )
    parser.add_argument("--output-dir", default="data_yahoo_tw_ohlcv", help="Directory to write parquet files.")
    parser.add_argument("--start-date", default="2000-01-01", help="Inclusive start date in YYYY-MM-DD.")
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="Inclusive end date in YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(12, max(4, os.cpu_count() or 4)),
        help="Maximum parallel Yahoo requests.",
    )
    parser.add_argument("--retries", type=int, default=2, help="Retries per symbol when Yahoo temporarily fails.")
    parser.add_argument("--refresh", action="store_true", help="Re-download full history even if parquet exists.")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N symbols after filtering.")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Optional stock codes to download, for example: --symbols 2330 6488",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Probe 0001-9999 with .TW/.TWO to find historically available Yahoo symbols, then exit.",
    )
    parser.add_argument(
        "--probe-start-year",
        type=int,
        default=2000,
        help="Start year for probe mode (January-only checks).",
    )
    parser.add_argument(
        "--probe-end-year",
        type=int,
        default=date.today().year,
        help="End year for probe mode (January-only checks).",
    )
    parser.add_argument(
        "--probe-codes",
        default="1-9999",
        help="Code range for probe mode, e.g. 1-9999 or 1000-9999.",
    )
    parser.add_argument(
        "--probe-skip-existing-files",
        action="store_true",
        help="In probe mode, skip codes that already have *_features.parquet in output-dir.",
    )
    parser.add_argument(
        "--probe-list-file",
        default="historical_symbols.txt",
        help="Output list filename (inside output-dir) for historically available Yahoo symbols.",
    )
    parser.add_argument(
        "--probe-report-file",
        default="probe_report.csv",
        help="Probe report filename (inside output-dir).",
    )
    parser.add_argument(
        "--fill-only",
        action="store_true",
        help="Skip download; only run missing-date fill on existing *_features.parquet files.",
    )
    parser.add_argument(
        "--fill-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After download, fill missing dates per symbol using market calendar inferred from all files.",
    )
    parser.add_argument(
        "--fill-report-file",
        default="fill_report.csv",
        help="Fill report filename (inside output-dir).",
    )
    return parser.parse_args()


def _parse_code_range(spec: str) -> tuple[int, int]:
    match = re.fullmatch(r"\s*(\d{1,4})\s*-\s*(\d{1,4})\s*", spec)
    if not match:
        raise ValueError(f"Invalid --probe-codes format: {spec}. Expected format like 1-9999")
    start = int(match.group(1))
    end = int(match.group(2))
    if start < 1 or end > 9999 or start > end:
        raise ValueError("--probe-codes must be within 1-9999 and start <= end")
    return start, end


def _read_existing_probe_status(report_path: Path) -> dict[str, str]:
    if not report_path.exists():
        return {}
    try:
        frame = pd.read_csv(report_path)
    except Exception:
        return {}
    if "yahoo_symbol" not in frame.columns or "status" not in frame.columns:
        return {}
    status_map: dict[str, str] = {}
    for row in frame.itertuples(index=False):
        symbol = str(getattr(row, "yahoo_symbol", "")).strip()
        status = str(getattr(row, "status", "")).strip()
        if symbol and status in PROBE_STATUSES:
            status_map[symbol] = status
    return status_map


def _load_existing_historical_symbols(list_path: Path) -> set[str]:
    if not list_path.exists():
        return set()
    symbols = set()
    with open(list_path, "r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip()
            if value:
                symbols.add(value)
    return symbols


def _probe_symbol_january(yahoo_symbol: str, start_year: int, end_year: int, retries: int) -> tuple[str, str, int, str | None]:
    jan_hits = 0
    period_start = pd.Timestamp(year=start_year, month=1, day=1)
    period_end = pd.Timestamp(year=end_year + 1, month=1, day=1)
    last_error: str | None = None

    for attempt in range(retries + 1):
        try:
            # yfinance 對不存在代碼會輸出大量訊息，試探時直接抑制以免汙染進度輸出。
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                frame = yf.download(
                    tickers=yahoo_symbol,
                    start=period_start.strftime("%Y-%m-%d"),
                    end=period_end.strftime("%Y-%m-%d"),
                    interval="1mo",
                    auto_adjust=False,
                    actions=False,
                    progress=False,
                    threads=False,
                    timeout=20,
                )
            frame = _normalize_history(frame)
            if not frame.empty:
                jan_rows = frame[frame["date"].dt.month == 1]
                jan_hits = int(jan_rows["date"].dt.year.nunique())
            return yahoo_symbol, ("historical_found" if jan_hits > 0 else "no_history"), jan_hits, None
        except Exception as exc:  # pragma: no cover - network path
            last_error = str(exc)
            if attempt < retries:
                time.sleep(0.8 * (attempt + 1))

    return yahoo_symbol, "failed", jan_hits, last_error


def run_historical_probe(
    output_dir: Path,
    workers: int,
    retries: int,
    start_year: int,
    end_year: int,
    code_range_spec: str,
    skip_existing_files: bool,
    list_filename: str,
    report_filename: str,
) -> None:
    if end_year < start_year:
        raise ValueError("probe-end-year must be on or after probe-start-year")

    logging.getLogger("yfinance").setLevel(logging.CRITICAL)

    code_start, code_end = _parse_code_range(code_range_spec)
    output_dir.mkdir(parents=True, exist_ok=True)

    list_path = output_dir / list_filename
    report_path = output_dir / report_filename
    existing_symbols = _load_existing_historical_symbols(list_path)
    existing_status = _read_existing_probe_status(report_path)

    skip_codes: set[str] = set()
    if skip_existing_files:
        for parquet_path in output_dir.glob("*_features.parquet"):
            code = parquet_path.name.split("_features.parquet")[0]
            if re.fullmatch(r"\d{4}", code):
                skip_codes.add(code)

    candidates: list[str] = []
    for code_num in range(code_start, code_end + 1):
        code = f"{code_num:04d}"
        if code in skip_codes:
            continue
        candidates.append(f"{code}.TW")
        candidates.append(f"{code}.TWO")

    pending = [symbol for symbol in candidates if symbol not in existing_symbols and symbol not in existing_status]

    print(
        f"[probe] range={code_start:04d}-{code_end:04d} years={start_year}-{end_year} "
        f"candidates={len(candidates)} pending={len(pending)} workers={workers}"
    )

    new_rows: list[dict[str, object]] = []
    if pending:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(_probe_symbol_january, symbol, start_year, end_year, retries): symbol
                for symbol in pending
            }
            for future in tqdm(as_completed(future_map), total=len(future_map), desc="Probe Jan history"):
                symbol, status, jan_hits, message = future.result()
                row = {
                    "yahoo_symbol": symbol,
                    "status": status,
                    "jan_hit_years": int(jan_hits),
                    "message": message,
                }
                new_rows.append(row)
                existing_status[symbol] = status
                if status == "historical_found":
                    existing_symbols.add(symbol)

    all_symbols_sorted = sorted(existing_symbols)
    with open(list_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(all_symbols_sorted))
        if all_symbols_sorted:
            handle.write("\n")

    merged_rows = [{"yahoo_symbol": symbol, "status": status} for symbol, status in sorted(existing_status.items())]
    if new_rows:
        detail_index = {row["yahoo_symbol"]: row for row in new_rows}
        for row in merged_rows:
            details = detail_index.get(row["yahoo_symbol"], None)
            if details:
                row.update(details)

    report_frame = pd.DataFrame(merged_rows)
    report_frame.to_csv(report_path, index=False)

    print(
        f"[probe summary] historical_symbols={len(all_symbols_sorted)} "
        f"newly_probed={len(new_rows)} list={list_path.name} report={report_path.name}"
    )


def _read_isin_table(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    text = response.content.decode("cp950", errors="ignore")
    table = pd.read_html(io.StringIO(text))[0]
    table.columns = table.iloc[0]
    return table.iloc[1:].reset_index(drop=True)


def load_taiwan_stock_symbols() -> list[SymbolRecord]:
    records: list[SymbolRecord] = []

    for market, (url, suffix) in ISIN_SOURCES.items():
        table = _read_isin_table(url)
        if "有價證券代號及名稱" not in table.columns:
            raise RuntimeError(f"Unexpected ISIN table columns for {market}: {table.columns.tolist()}")

        for _, row in table.iterrows():
            code_name = str(row.get("有價證券代號及名稱", "")).strip()
            match = CODE_NAME_PATTERN.match(code_name)
            if not match:
                continue

            cficode = str(row.get("CFICode", "")).strip().upper()
            if not cficode.startswith(ASSET_CFICODE_PREFIXES):
                continue

            code = match.group("code").upper()
            name = match.group("name").strip()
            records.append(
                SymbolRecord(
                    code=code,
                    name=name,
                    market=market,
                    yahoo_symbol=f"{code}{suffix}",
                )
            )

    deduped = {record.code: record for record in records}
    return sorted(deduped.values(), key=lambda item: item.code)


def _normalize_history(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)

    rename_map = {
        "Open": "open",
        "High": "max",
        "Low": "min",
        "Close": "close",
        "Volume": "Trading_Volume",
    }
    missing = [column for column in rename_map if column not in frame.columns]
    if missing:
        raise ValueError(f"Yahoo response missing expected columns: {missing}")

    normalized = frame.rename(columns=rename_map)[list(rename_map.values())].copy()
    normalized = normalized.reset_index().rename(columns={normalized.index.name or "Date": "date"})
    if "date" not in normalized.columns:
        normalized = normalized.rename(columns={normalized.columns[0]: "date"})

    normalized["date"] = pd.to_datetime(normalized["date"], utc=True).dt.tz_localize(None)
    for column in ["open", "max", "min", "close"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized["Trading_Volume"] = pd.to_numeric(normalized["Trading_Volume"], errors="coerce")

    normalized = normalized.dropna(subset=["date", "open", "max", "min", "close"])
    normalized["Trading_Volume"] = normalized["Trading_Volume"].fillna(0).astype("int64")
    normalized = normalized.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return normalized[OUTPUT_COLUMNS].reset_index(drop=True)


def _load_existing_history(path: Path) -> pd.DataFrame:
    existing = pd.read_parquet(path)
    if existing.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    existing = existing.copy()
    existing["date"] = pd.to_datetime(existing["date"], utc=True).dt.tz_localize(None)
    return existing[OUTPUT_COLUMNS].sort_values("date").drop_duplicates(subset=["date"], keep="last")


def _load_dates_from_parquet(path: Path) -> pd.DatetimeIndex:
    frame = pd.read_parquet(path, columns=["date"])
    if frame.empty:
        return pd.DatetimeIndex([])
    dates = pd.to_datetime(frame["date"], utc=True).dt.tz_localize(None).dt.normalize()
    return pd.DatetimeIndex(dates.dropna().unique()).sort_values()


def _infer_market_calendar(parquet_paths: list[Path], start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DatetimeIndex:
    all_dates: list[pd.DatetimeIndex] = []
    for path in parquet_paths:
        try:
            dates = _load_dates_from_parquet(path)
            if len(dates) > 0:
                all_dates.append(dates)
        except Exception:
            continue

    if not all_dates:
        return pd.DatetimeIndex([])

    merged = pd.DatetimeIndex(np.concatenate([idx.to_numpy() for idx in all_dates])).drop_duplicates().sort_values()
    calendar = merged[(merged >= start_date.normalize()) & (merged <= end_date.normalize())]
    return pd.DatetimeIndex(calendar)


def _fill_symbol_by_calendar(path: Path, calendar: pd.DatetimeIndex) -> FillResult:
    code = path.name.replace("_features.parquet", "")
    try:
        frame = _load_existing_history(path)
    except Exception as exc:
        return FillResult(
            code=code,
            status="failed",
            rows_before=0,
            rows_after=0,
            filled_rows=0,
            first_date=None,
            last_date=None,
            output_path=str(path),
            message=str(exc),
        )

    if frame.empty:
        return FillResult(
            code=code,
            status="empty",
            rows_before=0,
            rows_after=0,
            filled_rows=0,
            first_date=None,
            last_date=None,
            output_path=str(path),
            message="No OHLCV rows.",
        )

    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"], utc=True).dt.tz_localize(None).dt.normalize()
    frame = frame.sort_values("date").drop_duplicates(subset=["date"], keep="last")

    first_date = frame["date"].min()
    last_date = frame["date"].max()
    target_dates = calendar[(calendar >= first_date) & (calendar <= last_date)]
    rows_before = int(len(frame))

    if len(target_dates) == 0:
        return FillResult(
            code=code,
            status="skipped",
            rows_before=rows_before,
            rows_after=rows_before,
            filled_rows=0,
            first_date=str(first_date.date()),
            last_date=str(last_date.date()),
            output_path=str(path),
            message="No overlap with inferred market calendar.",
        )

    if rows_before == len(target_dates):
        return FillResult(
            code=code,
            status="unchanged",
            rows_before=rows_before,
            rows_after=rows_before,
            filled_rows=0,
            first_date=str(first_date.date()),
            last_date=str(last_date.date()),
            output_path=str(path),
        )

    reindexed = frame.set_index("date").reindex(target_dates)
    inserted_rows = reindexed["close"].isna()
    filled_rows = int(inserted_rows.sum())
    if filled_rows == 0:
        return FillResult(
            code=code,
            status="unchanged",
            rows_before=rows_before,
            rows_after=rows_before,
            filled_rows=0,
            first_date=str(first_date.date()),
            last_date=str(last_date.date()),
            output_path=str(path),
        )

    prev_close = reindexed["close"].ffill()
    for column in ["open", "max", "min", "close"]:
        reindexed.loc[inserted_rows, column] = prev_close.loc[inserted_rows]

    reindexed.loc[inserted_rows, "Trading_Volume"] = 0
    reindexed["Trading_Volume"] = pd.to_numeric(reindexed["Trading_Volume"], errors="coerce").fillna(0).astype("int64")
    reindexed = reindexed.reset_index().rename(columns={"index": "date"})
    reindexed = reindexed[OUTPUT_COLUMNS]
    reindexed.to_parquet(path, index=False)

    return FillResult(
        code=code,
        status="filled",
        rows_before=rows_before,
        rows_after=int(len(reindexed)),
        filled_rows=filled_rows,
        first_date=str(first_date.date()),
        last_date=str(last_date.date()),
        output_path=str(path),
    )


def fill_missing_dates(
    output_dir: Path,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    report_filename: str,
) -> list[FillResult]:
    parquet_paths = sorted(output_dir.glob("*_features.parquet"))
    if not parquet_paths:
        print(f"[fill] no *_features.parquet found in {output_dir}")
        return []

    max_passes = 3
    last_results: list[FillResult] = []
    total_filled_rows = 0

    for pass_id in range(1, max_passes + 1):
        market_calendar = _infer_market_calendar(parquet_paths, start_date, end_date)
        if len(market_calendar) == 0:
            print("[fill] inferred market calendar is empty, skip fill step")
            return []

        print(
            f"[fill] pass={pass_id}/{max_passes} symbols={len(parquet_paths)} "
            f"calendar_days={len(market_calendar)} range={market_calendar.min().date()}..{market_calendar.max().date()}"
        )

        pass_results: list[FillResult] = []
        for path in tqdm(parquet_paths, desc=f"Fill missing dates (pass {pass_id})"):
            pass_results.append(_fill_symbol_by_calendar(path, market_calendar))

        pass_filled_rows = sum(result.filled_rows for result in pass_results)
        pass_filled_symbols = sum(result.status == "filled" for result in pass_results)
        pass_failed = sum(result.status == "failed" for result in pass_results)
        total_filled_rows += pass_filled_rows

        print(
            f"[fill pass summary] pass={pass_id} filled_symbols={pass_filled_symbols} "
            f"filled_rows={pass_filled_rows} failed={pass_failed}"
        )
        last_results = pass_results

        if pass_filled_rows == 0:
            break

    report_frame = pd.DataFrame([asdict(result) for result in last_results])
    report_frame.to_csv(output_dir / report_filename, index=False)

    final_filled_symbols = sum(result.status == "filled" for result in last_results)
    final_failed = sum(result.status == "failed" for result in last_results)
    print(
        f"[fill summary] final_filled_symbols={final_filled_symbols} total_filled_rows={total_filled_rows} "
        f"failed={final_failed}"
    )
    return last_results


def download_symbol(
    record: SymbolRecord,
    output_dir: Path,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    refresh: bool,
    retries: int,
) -> DownloadResult:
    output_path = output_dir / f"{record.code}_features.parquet"
    had_existing_file = output_path.exists()
    existing = pd.DataFrame(columns=OUTPUT_COLUMNS)
    effective_start = start_date

    if output_path.exists() and not refresh:
        existing = _load_existing_history(output_path)
        if not existing.empty:
            last_date = existing["date"].max()
            if pd.notna(last_date) and last_date.normalize() >= end_date.normalize():
                return DownloadResult(
                    code=record.code,
                    yahoo_symbol=record.yahoo_symbol,
                    market=record.market,
                    status="up_to_date",
                    rows=int(len(existing)),
                    output_path=str(output_path),
                )
            if pd.notna(last_date):
                effective_start = max(start_date, last_date - pd.Timedelta(days=7))

    last_error: str | None = None
    for attempt in range(retries + 1):
        try:
            history = yf.download(
                tickers=record.yahoo_symbol,
                start=effective_start.strftime("%Y-%m-%d"),
                end=(end_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                auto_adjust=False,
                actions=False,
                progress=False,
                threads=False,
                timeout=30,
            )
            history = _normalize_history(history)

            if history.empty and existing.empty:
                return DownloadResult(
                    code=record.code,
                    yahoo_symbol=record.yahoo_symbol,
                    market=record.market,
                    status="unavailable",
                    rows=0,
                    output_path=None,
                    message="Yahoo Finance returned no OHLCV rows.",
                )

            if existing.empty:
                merged = history.copy()
            elif history.empty:
                merged = existing.copy()
            else:
                merged = pd.concat([existing, history], ignore_index=True)
            merged = merged.sort_values("date").drop_duplicates(subset=["date"], keep="last")
            merged = merged[(merged["date"] >= start_date) & (merged["date"] <= end_date)]

            if merged.empty:
                return DownloadResult(
                    code=record.code,
                    yahoo_symbol=record.yahoo_symbol,
                    market=record.market,
                    status="unavailable",
                    rows=0,
                    output_path=None,
                    message="Yahoo Finance returned rows outside the requested date range.",
                )

            merged.to_parquet(output_path, index=False)
            return DownloadResult(
                code=record.code,
                yahoo_symbol=record.yahoo_symbol,
                market=record.market,
                status="updated" if had_existing_file else "created",
                rows=int(len(merged)),
                output_path=str(output_path),
            )
        except Exception as exc:  # pragma: no cover - network path
            last_error = str(exc)
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))

    return DownloadResult(
        code=record.code,
        yahoo_symbol=record.yahoo_symbol,
        market=record.market,
        status="failed",
        rows=0,
        output_path=None,
        message=last_error,
    )


def write_reports(output_dir: Path, symbols: list[SymbolRecord], results: list[DownloadResult]) -> None:
    symbols_frame = pd.DataFrame([asdict(symbol) for symbol in symbols])
    # Quote all fields so spreadsheet tools keep ETF leading zeros (e.g. 0050, 006208).
    symbols_frame.to_csv(output_dir / "symbols.csv", index=False, quoting=csv.QUOTE_ALL)

    # Provide a clean numeric-code list without Yahoo suffixes for downstream filtering.
    symbol_codes = symbols_frame[["code"]].copy()
    symbol_codes = symbol_codes.rename(columns={"code": "symbol"})
    symbol_codes.to_csv(output_dir / "symbols_numeric.csv", index=False, quoting=csv.QUOTE_ALL)

    result_frame = pd.DataFrame([asdict(result) for result in results])
    result_frame.to_csv(output_dir / "download_report.csv", index=False)

    summary = {
        "requested_symbols": len(symbols),
        "created": int((result_frame["status"] == "created").sum()) if not result_frame.empty else 0,
        "updated": int((result_frame["status"] == "updated").sum()) if not result_frame.empty else 0,
        "up_to_date": int((result_frame["status"] == "up_to_date").sum()) if not result_frame.empty else 0,
        "unavailable": int((result_frame["status"] == "unavailable").sum()) if not result_frame.empty else 0,
        "failed": int((result_frame["status"] == "failed").sum()) if not result_frame.empty else 0,
    }
    with open(output_dir / "download_summary.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.probe_only:
        run_historical_probe(
            output_dir=output_dir,
            workers=args.workers,
            retries=args.retries,
            start_year=args.probe_start_year,
            end_year=args.probe_end_year,
            code_range_spec=args.probe_codes,
            skip_existing_files=args.probe_skip_existing_files,
            list_filename=args.probe_list_file,
            report_filename=args.probe_report_file,
        )
        return

    start_date = pd.Timestamp(args.start_date)
    end_date = pd.Timestamp(args.end_date)
    if end_date < start_date:
        raise ValueError("end-date must be on or after start-date")

    if args.fill_only:
        fill_results = fill_missing_dates(
            output_dir=output_dir,
            start_date=start_date,
            end_date=end_date,
            report_filename=args.fill_report_file,
        )
        print(f"[fill-only] processed={len(fill_results)}")
        return

    symbols = load_taiwan_stock_symbols()
    if args.symbols:
        requested_codes = {code.strip() for code in args.symbols}
        symbols = [record for record in symbols if record.code in requested_codes]
        missing_codes = sorted(requested_codes - {record.code for record in symbols})
        if missing_codes:
            print(f"[symbols] not found in TWSE/OTC lists: {', '.join(missing_codes)}")

    if args.limit is not None:
        symbols = symbols[: args.limit]

    if not symbols:
        raise RuntimeError("No Taiwan stock symbols matched the given filters.")

    print(
        f"[download] symbols={len(symbols)} range={start_date.date()}..{end_date.date()} "
        f"workers={args.workers} output_dir={output_dir}"
    )

    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(
                download_symbol,
                record,
                output_dir,
                start_date,
                end_date,
                args.refresh,
                args.retries,
            ): record
            for record in symbols
        }
        for future in tqdm(as_completed(future_map), total=len(future_map), desc="Yahoo Finance"):
            results.append(future.result())

    results.sort(key=lambda item: item.code)
    write_reports(output_dir, symbols, results)

    if args.fill_missing:
        fill_missing_dates(
            output_dir=output_dir,
            start_date=start_date,
            end_date=end_date,
            report_filename=args.fill_report_file,
        )

    created = sum(result.status == "created" for result in results)
    updated = sum(result.status == "updated" for result in results)
    up_to_date = sum(result.status == "up_to_date" for result in results)
    unavailable = sum(result.status == "unavailable" for result in results)
    failed = sum(result.status == "failed" for result in results)
    print(
        "[summary] "
        f"created={created} updated={updated} up_to_date={up_to_date} unavailable={unavailable} failed={failed}"
    )


if __name__ == "__main__":
    main()