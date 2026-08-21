from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time
from typing import Any

import numpy as np
import polars as pl
import pyarrow.parquet as pq

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import atomic_write_text  # noqa: E402
from download_bybit_perp_daily import (  # noqa: E402
    BYBIT_MAX_KLINE_LIMIT,
    BYBIT_WINDOW_SPAN_MS,
    CANDLE_INTERVAL_MS,
    KLINE_ENDPOINT,
    KLINE_INTERVAL,
    BybitClient,
    _normalize_candles,
    _read_parquet,
    _write_parquet,
)


@dataclass(slots=True)
class GapRepairResult:
    symbol: str
    status: str
    rows_before: int
    internal_missing_before: int
    request_windows: int
    recovered_candles: int
    rows_after: int
    internal_missing_after: int
    output_path: str | None
    sha256: str | None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Repair internal gaps in retained Bybit one-minute parquet files by "
            "fetching only the missing official Klines. No interpolation is used."
        )
    )
    parser.add_argument("--input-dir", default="data_bybit/1m")
    parser.add_argument(
        "--instruments",
        default="data_bybit/funding/instruments.csv",
        help="Current Bybit instrument snapshot produced by the funding downloader.",
    )
    parser.add_argument("--workers", type=int, default=48)
    parser.add_argument("--request-interval", type=float, default=None)
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--retry-base", type=float, default=0.6)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _standard_symbols(path: Path) -> list[str]:
    if not path.is_file():
        raise FileNotFoundError(f"missing Bybit instrument snapshot: {path}")
    frame = pl.read_csv(path, infer_schema_length=10_000)
    required = {
        "code",
        "category",
        "quote_coin",
        "settle_coin",
        "contract_type",
        "status",
        "symbol_type",
        "is_pre_listing",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"instrument snapshot missing columns: {sorted(missing)}")
    return (
        frame.filter(
            (pl.col("category") == "linear")
            & (pl.col("quote_coin") == "USDT")
            & (pl.col("settle_coin") == "USDT")
            & pl.col("contract_type").cast(pl.String).str.contains("LinearPerpetual")
            & (pl.col("status") == "Trading")
            & pl.col("symbol_type")
            .fill_null("")
            .cast(pl.String)
            .str.strip_chars()
            .eq("")
            & ~pl.col("is_pre_listing").cast(pl.Boolean, strict=False).fill_null(False)
        )
        .get_column("code")
        .cast(pl.String)
        .sort()
        .to_list()
    )


def _initial_download_row_audit(
    input_dir: Path,
    symbols: list[str],
    *,
    current_rows: int,
    remaining_internal_gaps: int,
) -> dict[str, int | str] | None:
    """Reconcile inserted official candles to the original download receipt."""

    path = input_dir / "download_report.csv"
    if not path.is_file():
        return None
    frame = pl.read_csv(path, infer_schema_length=10_000)
    if not {"code", "rows"}.issubset(frame.columns):
        return None
    selected = frame.filter(pl.col("code").cast(pl.String).is_in(symbols))
    if selected.height != len(symbols) or selected["code"].n_unique() != len(symbols):
        return None
    initial_rows = int(selected["rows"].cast(pl.Int64).sum())
    inserted = int(current_rows) - initial_rows
    if inserted < 0:
        raise RuntimeError(
            "current Bybit row count is smaller than the original download receipt"
        )
    return {
        "basis": "download_report_rows_vs_exact_post_repair_rows",
        "original_download_rows": initial_rows,
        "post_repair_rows": int(current_rows),
        "official_candles_inserted_since_download": inserted,
        "initial_internal_missing_inferred": inserted
        + int(remaining_internal_gaps),
    }


def _missing_timestamp_ms(path: Path) -> tuple[int, list[int]]:
    frame = pl.from_arrow(pq.read_table(path, columns=["date"], memory_map=True))
    if frame.is_empty():
        return 0, []
    expression = (
        pl.col("date").str.to_datetime(strict=False, time_zone="UTC")
        if frame.schema["date"] == pl.String
        else pl.col("date").cast(pl.Datetime("us", "UTC"), strict=False)
    )
    timestamps = (
        frame.select(expression.alias("date"))
        .drop_nulls("date")
        .sort("date")
        .get_column("date")
    )
    if len(timestamps) != frame.height:
        raise ValueError("date column contains unparseable/null timestamps")
    values_ms = timestamps.cast(pl.Int64).to_numpy() // 1000
    if values_ms.size == 0:
        return 0, []
    if int(values_ms[0]) % CANDLE_INTERVAL_MS != 0:
        raise ValueError("first candle timestamp is off the one-minute UTC grid")
    deltas = np.diff(values_ms)
    if np.any(deltas <= 0):
        raise ValueError("date column is duplicated or not strictly increasing")
    if np.any(deltas % CANDLE_INTERVAL_MS != 0):
        raise ValueError("date column contains off-grid interval transitions")
    gaps: list[int] = []
    for previous, current, delta in zip(
        values_ms[:-1], values_ms[1:], deltas, strict=True
    ):
        if delta <= CANDLE_INTERVAL_MS:
            continue
        gaps.extend(
            range(
                int(previous + CANDLE_INTERVAL_MS),
                int(current),
                CANDLE_INTERVAL_MS,
            )
        )
    return int(values_ms.size), gaps


def _request_windows(missing: list[int]) -> list[tuple[int, int, set[int]]]:
    """Pack nearby missing instants into inclusive requests of at most 1,000 bars."""

    if not missing:
        return []
    ordered = sorted(set(int(value) for value in missing))
    windows: list[tuple[int, int, set[int]]] = []
    start = ordered[0]
    expected: set[int] = {start}
    for timestamp in ordered[1:]:
        if timestamp - start <= BYBIT_WINDOW_SPAN_MS:
            expected.add(timestamp)
            continue
        windows.append((start, max(expected), expected))
        start = timestamp
        expected = {timestamp}
    windows.append((start, max(expected), expected))
    return windows


def _repair_symbol(
    client: BybitClient,
    *,
    symbol: str,
    path: Path,
    rows_before: int,
    missing: list[int],
    note_request: Any,
) -> GapRepairResult:
    windows = _request_windows(missing)
    raw_rows: list[list[str]] = []
    recovered: set[int] = set()
    for start_ms, end_ms, expected in windows:
        payload = client.get(
            KLINE_ENDPOINT,
            {
                "category": "linear",
                "symbol": symbol,
                "interval": KLINE_INTERVAL,
                "start": str(start_ms),
                "end": str(end_ms),
                "limit": BYBIT_MAX_KLINE_LIMIT,
            },
        )
        rows = payload.get("result", {}).get("list", [])
        raw_rows.extend(rows)
        returned = {int(row[0]) for row in rows if row}
        recovered.update(expected & returned)
        note_request(symbol)
    unresolved = sorted(set(missing) - recovered)
    missing_set = set(missing)
    fresh = _normalize_candles(
        [row for row in raw_rows if row and int(row[0]) in missing_set]
    )
    if fresh.height != len(missing_set) - len(unresolved):
        raise RuntimeError(
            "normalized recovery row mismatch: "
            f"expected={len(missing_set) - len(unresolved)} "
            f"actual={fresh.height}"
        )
    if fresh.height:
        existing = _read_parquet(path)
        combined = (
            pl.concat([existing, fresh], how="diagonal_relaxed")
            .sort("date")
            .unique(subset=["date"], keep="last", maintain_order=True)
            .sort("date")
        )
        _write_parquet(combined, path)
    rows_after, remaining = _missing_timestamp_ms(path)
    if remaining != unresolved:
        raise RuntimeError(
            "post-write missing timestamps differ from the exact official API "
            f"unavailable set: remaining={len(remaining)} unresolved={len(unresolved)}"
        )
    preview = ", ".join(
        datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
        for value in unresolved[:5]
    )
    return GapRepairResult(
        symbol=symbol,
        status=(
            "repaired"
            if not unresolved
            else "repaired_with_official_unavailable"
        ),
        rows_before=rows_before,
        internal_missing_before=len(missing),
        request_windows=len(windows),
        recovered_candles=fresh.height,
        rows_after=rows_after,
        internal_missing_after=len(unresolved),
        output_path=str(path),
        sha256=_sha256(path),
        message=(
            None
            if not unresolved
            else f"official API returned no candle for: {preview}"
        ),
    )


def main() -> None:
    args = parse_args()
    started = datetime.now(timezone.utc)
    input_dir = Path(args.input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = (input_dir / ".download.lock").open("a+", encoding="utf-8")
    if fcntl is not None:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
    symbols = _standard_symbols(Path(args.instruments))
    requested = {
        str(value).strip().upper()
        for value in (args.symbols or [])
        if str(value).strip()
    }
    if requested:
        selected = [symbol for symbol in symbols if symbol.upper() in requested]
        absent = requested - {symbol.upper() for symbol in selected}
        if absent:
            raise ValueError(
                f"requested symbols are not current standard USDT perps: {sorted(absent)}"
            )
        symbols = selected
    if args.limit is not None:
        symbols = symbols[: max(0, int(args.limit))]
    if not symbols:
        raise RuntimeError("no Bybit symbols selected")

    print(
        f"[bybit-gap-repair] scanning {len(symbols)} source files for exact internal gaps",
        flush=True,
    )
    scans: dict[str, tuple[int, list[int]]] = {}
    scan_failures: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max(1, min(12, int(args.workers)))) as pool:
        futures = {
            pool.submit(
                _missing_timestamp_ms,
                input_dir / f"{symbol}_features.parquet",
            ): symbol
            for symbol in symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                scans[symbol] = future.result()
            except Exception as exc:
                scan_failures[symbol] = f"{type(exc).__name__}: {exc}"

    repair_symbols = [
        symbol for symbol in symbols if symbol in scans and scans[symbol][1]
    ]
    total_missing = sum(len(scans[symbol][1]) for symbol in repair_symbols)
    total_windows = sum(
        len(_request_windows(scans[symbol][1])) for symbol in repair_symbols
    )
    print(
        "[bybit-gap-repair] "
        f"bad_symbols={len(repair_symbols)} missing_candles={total_missing} "
        f"official_api_windows={total_windows}",
        flush=True,
    )

    client = BybitClient(
        args.request_interval,
        max_retries=int(args.max_retries),
        retry_base=float(args.retry_base),
    )
    progress_path = input_dir / "gap_repair_progress.json"
    progress_lock = threading.Lock()
    completed_windows = 0
    last_report = 0.0

    def note_request(_symbol: str) -> None:
        nonlocal completed_windows, last_report
        now = time.monotonic()
        with progress_lock:
            completed_windows += 1
            if now - last_report < 30.0 and completed_windows < total_windows:
                return
            elapsed = max(1e-9, (datetime.now(timezone.utc) - started).total_seconds())
            rate = completed_windows / elapsed
            remaining = max(0, total_windows - completed_windows)
            payload = {
                "started_at_utc": started.isoformat(),
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "completed_request_windows": completed_windows,
                "total_request_windows": total_windows,
                "progress": completed_windows / max(1, total_windows),
                "average_requests_per_second": rate,
                "eta_seconds_low_confidence": remaining / max(rate, 1e-9),
            }
            atomic_write_text(
                progress_path,
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            )
            print(
                "[bybit-gap-repair] "
                f"requests={completed_windows}/{total_windows} "
                f"rate={rate:.2f}/s eta={remaining / max(rate, 1e-9):.0f}s",
                flush=True,
            )
            last_report = now

    results: list[GapRepairResult] = []
    for symbol in symbols:
        if symbol in scan_failures:
            results.append(
                GapRepairResult(
                    symbol=symbol,
                    status="failed",
                    rows_before=0,
                    internal_missing_before=0,
                    request_windows=0,
                    recovered_candles=0,
                    rows_after=0,
                    internal_missing_after=0,
                    output_path=None,
                    sha256=None,
                    message=scan_failures[symbol],
                )
            )
        elif not scans[symbol][1]:
            path = input_dir / f"{symbol}_features.parquet"
            results.append(
                GapRepairResult(
                    symbol=symbol,
                    status="already_continuous",
                    rows_before=scans[symbol][0],
                    internal_missing_before=0,
                    request_windows=0,
                    recovered_candles=0,
                    rows_after=scans[symbol][0],
                    internal_missing_after=0,
                    output_path=str(path),
                    sha256=_sha256(path),
                )
            )

    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
        futures = {
            pool.submit(
                _repair_symbol,
                client,
                symbol=symbol,
                path=input_dir / f"{symbol}_features.parquet",
                rows_before=scans[symbol][0],
                missing=scans[symbol][1],
                note_request=note_request,
            ): symbol
            for symbol in repair_symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                rows_before, gaps = scans[symbol]
                result = GapRepairResult(
                    symbol=symbol,
                    status="failed",
                    rows_before=rows_before,
                    internal_missing_before=len(gaps),
                    request_windows=len(_request_windows(gaps)),
                    recovered_candles=0,
                    rows_after=rows_before,
                    internal_missing_after=len(gaps),
                    output_path=str(input_dir / f"{symbol}_features.parquet"),
                    sha256=None,
                    message=f"{type(exc).__name__}: {exc}",
                )
            results.append(result)
            print(
                f"[bybit-gap-repair] {symbol} status={result.status} "
                f"recovered={result.recovered_candles}",
                flush=True,
            )

    ordered = sorted(results, key=lambda item: item.symbol)
    atomic_write_text(
        input_dir / "gap_repair_report.csv",
        pl.DataFrame(
            [asdict(item) for item in ordered], infer_schema_length=None
        ).write_csv(),
    )
    failed = [item for item in ordered if item.status == "failed"]
    summary = {
        "source": "bybit_v5_market_kline_exact_missing_instants",
        "no_interpolation": True,
        "selected_symbols": len(symbols),
        "repaired_symbols": sum(item.status.startswith("repaired") for item in ordered),
        "official_unavailable_symbols": sum(
            item.status == "repaired_with_official_unavailable" for item in ordered
        ),
        "already_continuous_symbols": sum(
            item.status == "already_continuous" for item in ordered
        ),
        "failed_symbols": len(failed),
        "internal_missing_before": sum(
            item.internal_missing_before for item in ordered
        ),
        "recovered_candles": sum(item.recovered_candles for item in ordered),
        "internal_missing_after": sum(item.internal_missing_after for item in ordered),
        "request_windows": sum(item.request_windows for item in ordered),
        "started_at_utc": started.isoformat(),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    initial_row_audit = _initial_download_row_audit(
        input_dir,
        symbols,
        current_rows=sum(item.rows_after for item in ordered),
        remaining_internal_gaps=summary["internal_missing_after"],
    )
    if initial_row_audit is not None:
        summary["initial_download_reconciliation"] = initial_row_audit
    atomic_write_text(
        input_dir / "gap_repair_summary.json",
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
    )
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
