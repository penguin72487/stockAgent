from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
import math
import os
from pathlib import Path
import sys
import time
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_shioaji_tw_kbars import (  # noqa: E402
    MAX_KBAR_QUERY_DAYS,
    SHIOAJI_STOCK_HISTORY_START,
    SOURCE_NAME,
    SymbolResult,
    TrafficBudgetReached,
    UniverseRow,
    _atomic_write_json,
    _check_traffic_budget,
    _load_universe,
    _payload_dict,
    _positive_volume_dates,
    _read_json,
    _sha256,
    _taiwan_market_hours_now,
    _usage_values,
    iter_date_chunks,
    normalize_kbars,
)


STORAGE_FREQUENCY = "minute"
RECEIPT_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
DEFAULT_CHUNK_DAYS = 29


class MarketHoursReached(RuntimeError):
    """Pause a long historical run before it competes with live quote capture."""


def query_minute_chunk(
    api: Any,
    contract: Any,
    row: UniverseRow,
    *,
    contract_unit: float,
    start: date,
    end: date,
    timeout_ms: int,
    retries: int,
    retry_backoff: float,
    expected_dates: set[date],
) -> tuple[pl.DataFrame, int]:
    """Query one chunk and remove only Shioaji's all-zero placeholder bars."""

    last_error: Exception | None = None
    for attempt in range(max(0, retries) + 1):
        try:
            payload = api.kbars(
                contract=contract,
                start=start.isoformat(),
                end=end.isoformat(),
                timeout=int(timeout_ms),
            )
            values = _payload_dict(payload)
            required = ("ts", "Open", "High", "Low", "Close", "Volume", "Amount")
            missing = [name for name in required if name not in values]
            if missing:
                raise ValueError(f"Shioaji Kbars payload is missing fields: {missing}")
            raw = pl.DataFrame({name: values[name] for name in required})
            placeholder_rows = 0
            cleaned_payload: Any = values
            if raw.height:
                all_zero_placeholder = pl.all_horizontal(
                    *[
                        pl.col(name).cast(pl.Float64, strict=False).fill_nan(None)
                        == 0.0
                        for name in (
                            "Open",
                            "High",
                            "Low",
                            "Close",
                            "Volume",
                            "Amount",
                        )
                    ]
                ).fill_null(False)
                placeholder_rows = raw.filter(all_zero_placeholder).height
                if placeholder_rows:
                    cleaned_payload = raw.filter(~all_zero_placeholder).to_dict(
                        as_series=False
                    )
            frame = normalize_kbars(
                cleaned_payload,
                symbol=row.symbol,
                market=row.market,
                contract_unit=contract_unit,
            )
            returned_dates = set(frame["date"].to_list()) if frame.height else set()
            missing_dates = sorted(expected_dates - returned_dates)
            if missing_dates:
                raise RuntimeError(
                    f"Shioaji Kbars omitted {len(missing_dates)} public "
                    f"positive-volume sessions for {row.symbol}: {missing_dates[:10]}"
                )
            return frame, placeholder_rows
        except Exception as exc:
            last_error = exc
            if attempt >= max(0, retries):
                break
            time.sleep(float(retry_backoff) * (2**attempt))
    assert last_error is not None
    raise last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download and retain Shioaji Taiwan stock one-minute Kbars in "
            "receipt-backed, resumable <=29-day chunks. This module is separate "
            "from both daily history and Tick/BidAsk HFT captures."
        )
    )
    parser.add_argument(
        "--base-stock-root", type=Path, default=Path("data_tw_public/stocks")
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_tw_minute/shioaji_1m"),
    )
    parser.add_argument("--start-date", default=SHIOAJI_STOCK_HISTORY_START.isoformat())
    parser.add_argument(
        "--end-date", default=(date.today() - timedelta(days=1)).isoformat()
    )
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS)
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols. Required unless --universe-csv or --all-symbols.",
    )
    parser.add_argument(
        "--universe-csv",
        type=Path,
        help="Optional CSV containing a symbol or code column.",
    )
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Explicitly request the entire public stock/ETF universe.",
    )
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--request-interval", type=float, default=0.25)
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-backoff", type=float, default=3.0)
    parser.add_argument("--max-traffic-fraction", type=float, default=0.90)
    parser.add_argument("--traffic-reserve-mb", type=float, default=25.0)
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument("--allow-market-hours", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def minute_chunk_paths(
    root: Path,
    symbol: str,
    start: date,
    end: date,
) -> tuple[Path, Path]:
    stem = f"{start.isoformat()}_{end.isoformat()}"
    symbol_root = root / "minute_chunks" / symbol
    data_path = symbol_root / f"{stem}.parquet"
    return data_path, data_path.with_suffix(".receipt.json")


def _write_minute_parquet(frame: pl.DataFrame, path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.write_parquet(
        temporary,
        compression="zstd",
        compression_level=7,
        statistics=True,
        row_group_size=128_000,
    )
    os.replace(temporary, path)
    return {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": _sha256(path),
    }


def validate_minute_kbars(
    frame: pl.DataFrame,
    *,
    symbol: str,
    start: date,
    end: date,
) -> dict[str, Any]:
    if frame.is_empty():
        return {
            "rows": 0,
            "sessions": 0,
            "first_ts": None,
            "last_ts": None,
            "duplicate_timestamps": 0,
            "out_of_session_rows": 0,
            "non_minute_rows": 0,
        }
    minute_of_day = pl.col("ts").dt.hour().cast(pl.Int16) * 60 + pl.col(
        "ts"
    ).dt.minute().cast(pl.Int16)
    duplicate_timestamps = frame.group_by("ts").len().filter(pl.col("len") > 1).height
    out_of_session_rows = frame.filter(
        (minute_of_day < 9 * 60 + 1)
        | (minute_of_day > 13 * 60 + 30)
        | (pl.col("ts").dt.second() != 0)
    ).height
    # Do not infer the integer timestamp unit: Polars may hold an otherwise
    # valid input as us or ns depending on how the frame was constructed.
    non_minute_rows = frame.filter(
        (pl.col("ts").dt.second() != 0) | (pl.col("ts").dt.nanosecond() != 0)
    ).height
    wrong_symbol_rows = frame.filter(pl.col("symbol") != symbol).height
    wrong_date_rows = frame.filter(
        (pl.col("date") < pl.lit(start)) | (pl.col("date") > pl.lit(end))
    ).height
    too_many_bars = frame.group_by("date").len().filter(pl.col("len") > 270).height
    failures = {
        "duplicate_timestamps": duplicate_timestamps,
        "out_of_session_rows": out_of_session_rows,
        "non_minute_rows": non_minute_rows,
        "wrong_symbol_rows": wrong_symbol_rows,
        "wrong_date_rows": wrong_date_rows,
        "sessions_over_270_bars": too_many_bars,
    }
    if any(failures.values()):
        raise RuntimeError(f"invalid minute Kbars for {symbol}: {failures}")
    return {
        "rows": frame.height,
        "sessions": frame["date"].n_unique(),
        "first_ts": str(frame["ts"].min()),
        "last_ts": str(frame["ts"].max()),
        **failures,
    }


def minute_receipt_valid(
    path: Path,
    *,
    symbol: str,
    start: date,
    end: date,
    simulation: bool | None = None,
) -> bool:
    payload = _read_json(path)
    if payload is None or not (
        payload.get("schema_version") == RECEIPT_SCHEMA_VERSION
        and payload.get("source") == SOURCE_NAME
        and payload.get("storage_frequency") == STORAGE_FREQUENCY
        and payload.get("symbol") == symbol
        and payload.get("start_date") == start.isoformat()
        and payload.get("end_date") == end.isoformat()
        and payload.get("status") in {"ok", "empty"}
        and (simulation is None or payload.get("simulation") is bool(simulation))
    ):
        return False
    if payload["status"] == "empty":
        return int(payload.get("rows", -1)) == 0
    output = payload.get("output_receipt")
    if not isinstance(output, dict):
        return False
    output_path = Path(str(output.get("path", "")))
    try:
        return (
            output_path.is_file()
            and int(output.get("size", -1)) == output_path.stat().st_size
            and str(output.get("sha256", "")) == _sha256(output_path)
        )
    except OSError:
        return False


def _symbols_from_csv(path: Path) -> set[str]:
    if not path.is_file():
        raise FileNotFoundError(f"minute universe CSV does not exist: {path}")
    frame = pl.read_csv(path, infer_schema_length=0)
    column = "symbol" if "symbol" in frame.columns else "code"
    if column not in frame.columns:
        raise ValueError(f"{path} requires a symbol or code column")
    return {
        str(value or "").strip().upper()
        for value in frame[column].to_list()
        if str(value or "").strip()
    }


def select_universe(
    universe: list[UniverseRow],
    *,
    symbols: str,
    universe_csv: Path | None,
    all_symbols: bool,
    max_symbols: int,
) -> list[UniverseRow]:
    modes = sum(
        (
            bool(str(symbols).strip()),
            universe_csv is not None,
            bool(all_symbols),
        )
    )
    if modes != 1:
        raise ValueError(
            "select exactly one of --symbols, --universe-csv, or --all-symbols"
        )
    requested = (
        {item.strip().upper() for item in str(symbols).split(",") if item.strip()}
        if str(symbols).strip()
        else (_symbols_from_csv(universe_csv) if universe_csv is not None else None)
    )
    known = {row.symbol for row in universe}
    if requested is not None:
        unknown = sorted(requested - known)
        if unknown:
            raise ValueError(
                f"requested symbols are absent from the public universe: {unknown}"
            )
        selected = [row for row in universe if row.symbol in requested]
    else:
        selected = list(universe)
    if max_symbols > 0:
        selected = selected[:max_symbols]
    return selected


def stock_contract_map(api: Any) -> dict[str, Any]:
    """Load only Taiwan stock/ETF Base contracts, never derivative families."""

    bases = api.contracts.list("STK", region="TW")
    result = {
        str(getattr(base, "code", "") or "").strip().upper(): base for base in bases
    }
    result.pop("", None)
    if not result:
        raise RuntimeError("Shioaji returned no Taiwan STK Base contracts")
    return result


def contract_for_stock_symbol(
    api: Any,
    row: UniverseRow,
    contracts_by_code: dict[str, Any],
) -> tuple[Any | None, float, str]:
    contract = contracts_by_code.get(row.symbol)
    if contract is None:
        return None, 0.0, "stock_contract_not_found"
    security_type = str(
        getattr(getattr(contract, "security_type", ""), "value", "")
        or getattr(contract, "security_type", "")
    ).upper()
    if security_type not in {"STK", "STOCK"}:
        return None, 0.0, f"unexpected_security_type={security_type}"
    exchange = str(
        getattr(getattr(contract, "exchange", ""), "value", "")
        or getattr(contract, "exchange", "")
    ).upper()
    if exchange not in {"TSE", "OTC"}:
        return None, 0.0, f"unexpected_exchange={exchange}"
    info = api.contracts.info(contract)
    unit = float(getattr(info, "unit", 0.0) or 0.0)
    if not math.isfinite(unit) or unit <= 0.0:
        return None, 0.0, f"invalid_contract_unit={unit}"
    market = "twse" if exchange == "TSE" else "tpex"
    if row.market and row.market not in {market, exchange.lower()}:
        return None, 0.0, f"market_mismatch=public:{row.market},shioaji:{market}"
    return contract, unit, ""


def _write_symbol_manifest(
    output_dir: Path,
    row: UniverseRow,
    chunks: list[tuple[date, date]],
    *,
    requested_start: date,
    requested_end: date,
    simulation: bool,
) -> SymbolResult:
    entries: list[dict[str, Any]] = []
    total_rows = 0
    dates: set[date] = set()
    for chunk_start, chunk_end in chunks:
        data_path, receipt_path = minute_chunk_paths(
            output_dir, row.symbol, chunk_start, chunk_end
        )
        if not minute_receipt_valid(
            receipt_path,
            symbol=row.symbol,
            start=chunk_start,
            end=chunk_end,
            simulation=simulation,
        ):
            raise RuntimeError(f"incomplete minute chunk receipt: {receipt_path}")
        receipt = _read_json(receipt_path)
        assert receipt is not None
        total_rows += int(receipt.get("rows", 0))
        entries.append(
            {
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
                "status": receipt["status"],
                "rows": int(receipt.get("rows", 0)),
                "sessions": int(receipt.get("sessions", 0)),
                "data_path": str(data_path) if receipt["status"] == "ok" else None,
                "data_sha256": (
                    receipt["output_receipt"]["sha256"]
                    if receipt["status"] == "ok"
                    else None
                ),
                "receipt_path": str(receipt_path),
            }
        )
        for raw in receipt.get("returned_dates", []):
            dates.add(date.fromisoformat(str(raw)))
    manifest_path = output_dir / "symbols" / f"{row.symbol}.manifest.json"
    _atomic_write_json(
        manifest_path,
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "source": SOURCE_NAME,
            "storage_frequency": STORAGE_FREQUENCY,
            "simulation": simulation,
            "symbol": row.symbol,
            "name": row.name,
            "market": row.market,
            "security_type": row.security_type,
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "chunks": entries,
            "minute_rows": total_rows,
            "sessions": len(dates),
            "first_date": min(dates).isoformat() if dates else None,
            "last_date": max(dates).isoformat() if dates else None,
            "written_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    return SymbolResult(
        symbol=row.symbol,
        status="complete",
        chunks_total=len(chunks),
        chunks_complete=len(chunks),
        source_minute_rows=total_rows,
        daily_rows=len(dates),
        first_date=min(dates).isoformat() if dates else None,
        last_date=max(dates).isoformat() if dates else None,
        output_path=str(manifest_path),
    )


def completed_symbol_manifest_result(
    output_dir: Path,
    row: UniverseRow,
    chunks: list[tuple[date, date]],
    *,
    requested_start: date,
    requested_end: date,
    simulation: bool,
) -> SymbolResult | None:
    """Fast restart path for an already sealed symbol.

    Chunk hashes are verified when the manifest is first written and again by
    the research dataset builder. Rehashing every completed symbol on every
    quota-window restart would turn a multi-day full-market backfill into an
    increasingly expensive O(completed data) scan.
    """

    manifest_path = output_dir / "symbols" / f"{row.symbol}.manifest.json"
    payload = _read_json(manifest_path)
    if payload is None or not (
        payload.get("schema_version") == MANIFEST_SCHEMA_VERSION
        and payload.get("source") == SOURCE_NAME
        and payload.get("storage_frequency") == STORAGE_FREQUENCY
        and payload.get("simulation") is simulation
        and payload.get("symbol") == row.symbol
        and payload.get("requested_start") == requested_start.isoformat()
        and payload.get("requested_end") == requested_end.isoformat()
    ):
        return None
    entries = payload.get("chunks")
    if not isinstance(entries, list) or len(entries) != len(chunks):
        return None
    for entry, (chunk_start, chunk_end) in zip(entries, chunks, strict=True):
        if not isinstance(entry, dict) or not (
            entry.get("start_date") == chunk_start.isoformat()
            and entry.get("end_date") == chunk_end.isoformat()
            and entry.get("status") in {"ok", "empty"}
        ):
            return None
        receipt_path = Path(str(entry.get("receipt_path", "")))
        if not receipt_path.is_file():
            return None
        if entry["status"] == "ok":
            data_path = Path(str(entry.get("data_path", "")))
            if not (
                data_path.is_file()
                and int(entry.get("rows", 0)) > 0
                and str(entry.get("data_sha256", ""))
            ):
                return None
    return SymbolResult(
        symbol=row.symbol,
        status="complete",
        chunks_total=len(chunks),
        chunks_complete=len(chunks),
        source_minute_rows=int(payload.get("minute_rows", 0)),
        daily_rows=int(payload.get("sessions", 0)),
        first_date=payload.get("first_date"),
        last_date=payload.get("last_date"),
        output_path=str(manifest_path),
        message="resumed_from_sealed_manifest",
    )


def _write_run_summary(
    output_dir: Path,
    *,
    args: argparse.Namespace,
    selected: list[UniverseRow],
    results: list[SymbolResult],
    traffic: tuple[int, int] | None,
    stopped_for_traffic: bool,
    stopped_for_market_hours: bool,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "download_report.csv"
    if results:
        pl.DataFrame([asdict(item) for item in results]).sort("symbol").write_csv(
            report_path
        )
    else:
        pl.DataFrame(
            schema={name: pl.String for name in SymbolResult.__dataclass_fields__}
        ).write_csv(report_path)
    complete = len(results) == len(selected) and all(
        item.status in {"complete", "contract_unavailable"} for item in results
    )
    _atomic_write_json(
        output_dir / "download_summary.json",
        {
            "schema_version": 1,
            "source": SOURCE_NAME,
            "storage_frequency": STORAGE_FREQUENCY,
            "start_date": str(args.start_date),
            "end_date": str(args.end_date),
            "chunk_days": int(args.chunk_days),
            "simulation": bool(args.simulation),
            "selected_symbols": len(selected),
            "reported_symbols": len(results),
            "complete_symbols": sum(x.status == "complete" for x in results),
            "contract_unavailable_symbols": sum(
                x.status == "contract_unavailable" for x in results
            ),
            "failed_symbols": sum(x.status == "failed" for x in results),
            "partial_symbols": sum(x.status == "partial" for x in results),
            "selected_coverage_complete": complete,
            "stopped_for_traffic": stopped_for_traffic,
            "stopped_for_market_hours": stopped_for_market_hours,
            "traffic_used_bytes": traffic[0] if traffic else None,
            "traffic_limit_bytes": traffic[1] if traffic else None,
            "report_path": str(report_path),
            "written_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(str(args.start_date))
    end = date.fromisoformat(str(args.end_date))
    if start < SHIOAJI_STOCK_HISTORY_START:
        raise ValueError(
            f"Shioaji stock history starts at {SHIOAJI_STOCK_HISTORY_START}; got {start}"
        )
    if start > end:
        raise ValueError("--start-date must not be after --end-date")
    if not 1 <= int(args.chunk_days) <= MAX_KBAR_QUERY_DAYS:
        raise ValueError("--chunk-days must be between 1 and 30")
    if float(args.request_interval) < 0:
        raise ValueError("--request-interval must be nonnegative")
    if not 0 < float(args.max_traffic_fraction) < 1:
        raise ValueError("--max-traffic-fraction must be between 0 and 1")

    universe = _load_universe(args.base_stock_root)
    selected = select_universe(
        universe,
        symbols=str(args.symbols),
        universe_csv=args.universe_csv,
        all_symbols=bool(args.all_symbols),
        max_symbols=int(args.max_symbols),
    )
    chunks = list(iter_date_chunks(start, end, int(args.chunk_days)))
    if args.dry_run:
        api_query_chunks = 0
        for row in selected:
            expected_dates = _positive_volume_dates(row.base_path, start, end)
            api_query_chunks += len(
                {
                    (value - start).days // int(args.chunk_days)
                    for value in expected_dates
                    if start <= value <= end
                }
            )
        print(
            f"[shioaji-minute] dry_run symbols={len(selected)} "
            f"receipt_chunks={len(selected) * len(chunks)} "
            f"api_query_chunks={api_query_chunks} range={start}..{end} "
            f"output={args.output_dir}",
            flush=True,
        )
        return
    if _taiwan_market_hours_now() and not args.allow_market_hours:
        raise RuntimeError(
            "Refusing historical minute backfill during Taiwan market hours "
            "(08:30-14:30 safety window)."
        )
    api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError(
            "Set SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY locally; credentials "
            "are intentionally not accepted as command-line arguments."
        )
    try:
        import shioaji as sj
    except ImportError as exc:
        raise RuntimeError("Shioaji is unavailable in the selected runtime") from exc

    api = sj.Shioaji(simulation=bool(args.simulation))
    api.set_event_callback(lambda _code, _event_code, _info, _event: None)
    api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)
    contracts_by_code = stock_contract_map(api)
    print(
        f"[shioaji-minute] stock_contracts={len(contracts_by_code)} "
        "region=TW security_type=STK",
        flush=True,
    )
    results: list[SymbolResult] = []
    traffic: tuple[int, int] | None = None
    stopped_for_traffic = False
    stopped_for_market_hours = False
    processed_chunks = 0
    queried_chunks = 0
    skipped_empty_chunks = 0
    started = time.monotonic()
    reserve_bytes = int(float(args.traffic_reserve_mb) * 1024 * 1024)
    try:
        for symbol_index, row in enumerate(selected, start=1):
            sealed_result = completed_symbol_manifest_result(
                args.output_dir,
                row,
                chunks,
                requested_start=start,
                requested_end=end,
                simulation=bool(args.simulation),
            )
            if sealed_result is not None:
                results.append(sealed_result)
                continue
            completed = sum(
                minute_receipt_valid(
                    minute_chunk_paths(args.output_dir, row.symbol, a, b)[1],
                    symbol=row.symbol,
                    start=a,
                    end=b,
                    simulation=bool(args.simulation),
                )
                for a, b in chunks
            )
            if completed == len(chunks):
                results.append(
                    _write_symbol_manifest(
                        args.output_dir,
                        row,
                        chunks,
                        requested_start=start,
                        requested_end=end,
                        simulation=bool(args.simulation),
                    )
                )
                continue
            contract, unit, contract_message = contract_for_stock_symbol(
                api, row, contracts_by_code
            )
            if contract is None:
                print(
                    f"[shioaji-minute] symbol={row.symbol} "
                    f"symbol_index={symbol_index}/{len(selected)} "
                    f"status=contract_unavailable reason={contract_message}",
                    flush=True,
                )
                results.append(
                    SymbolResult(
                        symbol=row.symbol,
                        status="contract_unavailable",
                        chunks_total=len(chunks),
                        chunks_complete=completed,
                        source_minute_rows=0,
                        daily_rows=0,
                        first_date=None,
                        last_date=None,
                        output_path="",
                        message=contract_message,
                    )
                )
                continue
            expected_all = _positive_volume_dates(row.base_path, start, end)
            try:
                for chunk_index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
                    data_path, receipt_path = minute_chunk_paths(
                        args.output_dir, row.symbol, chunk_start, chunk_end
                    )
                    if minute_receipt_valid(
                        receipt_path,
                        symbol=row.symbol,
                        start=chunk_start,
                        end=chunk_end,
                        simulation=bool(args.simulation),
                    ):
                        continue
                    if _taiwan_market_hours_now() and not args.allow_market_hours:
                        raise MarketHoursReached(
                            "Taiwan market-hours safety window reached; "
                            "resume after 14:30 Asia/Taipei"
                        )
                    expected_dates = {
                        value
                        for value in expected_all
                        if chunk_start <= value <= chunk_end
                    }
                    query_performed = bool(expected_dates)
                    if query_performed:
                        traffic = _check_traffic_budget(
                            api,
                            max_fraction=float(args.max_traffic_fraction),
                            reserve_bytes=reserve_bytes,
                        )
                        frame, zero_placeholder_rows_dropped = query_minute_chunk(
                            api,
                            contract,
                            row,
                            contract_unit=unit,
                            start=chunk_start,
                            end=chunk_end,
                            timeout_ms=int(args.timeout_ms),
                            retries=int(args.retries),
                            retry_backoff=float(args.retry_backoff),
                            expected_dates=expected_dates,
                        )
                        queried_chunks += 1
                    else:
                        # The public point-in-time panel is the universe and
                        # coverage reference. With no positive-volume session,
                        # this chunk cannot contribute a tradable minute bar.
                        frame = pl.DataFrame()
                        zero_placeholder_rows_dropped = 0
                        skipped_empty_chunks += 1
                    processed_chunks += 1
                    audit = validate_minute_kbars(
                        frame,
                        symbol=row.symbol,
                        start=chunk_start,
                        end=chunk_end,
                    )
                    returned_dates = sorted(
                        value.isoformat()
                        for value in (
                            set(frame["date"].to_list()) if frame.height else set()
                        )
                    )
                    output_receipt = (
                        _write_minute_parquet(frame, data_path)
                        if frame.height
                        else None
                    )
                    _atomic_write_json(
                        receipt_path,
                        {
                            "schema_version": RECEIPT_SCHEMA_VERSION,
                            "source": SOURCE_NAME,
                            "storage_frequency": STORAGE_FREQUENCY,
                            "simulation": bool(args.simulation),
                            "symbol": row.symbol,
                            "name": row.name,
                            "market": row.market,
                            "security_type": row.security_type,
                            "contract_unit": unit,
                            "start_date": chunk_start.isoformat(),
                            "end_date": chunk_end.isoformat(),
                            "status": "ok" if frame.height else "empty",
                            "rows": frame.height,
                            "sessions": audit["sessions"],
                            "first_ts": audit["first_ts"],
                            "last_ts": audit["last_ts"],
                            "expected_positive_volume_sessions": len(expected_dates),
                            "returned_dates": returned_dates,
                            "query_performed": query_performed,
                            "query_skipped_reason": (
                                None
                                if query_performed
                                else "no_public_positive_volume_session"
                            ),
                            "zero_placeholder_rows_dropped": (
                                zero_placeholder_rows_dropped
                            ),
                            "audit": audit,
                            "output_receipt": output_receipt,
                            "downloaded_at_utc": datetime.now(timezone.utc)
                            .replace(microsecond=0)
                            .isoformat(),
                        },
                    )
                    completed += 1
                    print(
                        f"[shioaji-minute] symbol={row.symbol} "
                        f"symbol_index={symbol_index}/{len(selected)} "
                        f"chunk={chunk_index}/{len(chunks)} rows={frame.height} "
                        f"api_queried={str(query_performed).lower()} "
                        f"queried={queried_chunks} "
                        f"skipped_empty={skipped_empty_chunks}",
                        flush=True,
                    )
                    if query_performed and float(args.request_interval):
                        time.sleep(float(args.request_interval))
                results.append(
                    _write_symbol_manifest(
                        args.output_dir,
                        row,
                        chunks,
                        requested_start=start,
                        requested_end=end,
                        simulation=bool(args.simulation),
                    )
                )
            except TrafficBudgetReached as exc:
                stopped_for_traffic = True
                print(
                    f"[shioaji-minute] symbol={row.symbol} "
                    f"symbol_index={symbol_index}/{len(selected)} "
                    f"status=stopped_for_traffic reason={exc}",
                    flush=True,
                )
                results.append(
                    SymbolResult(
                        symbol=row.symbol,
                        status="partial",
                        chunks_total=len(chunks),
                        chunks_complete=completed,
                        source_minute_rows=0,
                        daily_rows=0,
                        first_date=None,
                        last_date=None,
                        output_path="",
                        message=str(exc),
                    )
                )
                break
            except MarketHoursReached as exc:
                stopped_for_market_hours = True
                print(
                    f"[shioaji-minute] symbol={row.symbol} "
                    f"symbol_index={symbol_index}/{len(selected)} "
                    f"status=stopped_for_market_hours reason={exc}",
                    flush=True,
                )
                results.append(
                    SymbolResult(
                        symbol=row.symbol,
                        status="partial",
                        chunks_total=len(chunks),
                        chunks_complete=completed,
                        source_minute_rows=0,
                        daily_rows=0,
                        first_date=None,
                        last_date=None,
                        output_path="",
                        message=str(exc),
                    )
                )
                break
            except Exception as exc:
                print(
                    f"[shioaji-minute] symbol={row.symbol} "
                    f"symbol_index={symbol_index}/{len(selected)} "
                    f"status=failed error={type(exc).__name__}: {exc}",
                    flush=True,
                )
                results.append(
                    SymbolResult(
                        symbol=row.symbol,
                        status="failed",
                        chunks_total=len(chunks),
                        chunks_complete=completed,
                        source_minute_rows=0,
                        daily_rows=0,
                        first_date=None,
                        last_date=None,
                        output_path="",
                        message=f"{type(exc).__name__}: {exc}",
                    )
                )
        try:
            traffic = _usage_values(api)
        except Exception:
            pass
    finally:
        try:
            api.logout()
        except Exception:
            pass
        _write_run_summary(
            args.output_dir,
            args=args,
            selected=selected,
            results=results,
            traffic=traffic,
            stopped_for_traffic=stopped_for_traffic,
            stopped_for_market_hours=stopped_for_market_hours,
        )
        _atomic_write_json(
            args.output_dir / "progress.json",
            {
                "schema_version": 1,
                "state": (
                    "stopped_for_traffic"
                    if stopped_for_traffic
                    else (
                        "stopped_for_market_hours"
                        if stopped_for_market_hours
                        else "finished"
                    )
                ),
                "selected_symbols": len(selected),
                "reported_symbols": len(results),
                "processed_chunks_this_run": processed_chunks,
                "queried_chunks_this_run": queried_chunks,
                "skipped_empty_chunks_this_run": skipped_empty_chunks,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "traffic_used_bytes": traffic[0] if traffic else None,
                "traffic_limit_bytes": traffic[1] if traffic else None,
                "updated_at_utc": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
            },
        )
    print(
        f"[shioaji-minute] complete={sum(x.status == 'complete' for x in results)} "
        f"failed={sum(x.status == 'failed' for x in results)} "
        f"partial={sum(x.status == 'partial' for x in results)} "
        f"summary={args.output_dir / 'download_summary.json'}",
        flush=True,
    )


if __name__ == "__main__":
    main()
