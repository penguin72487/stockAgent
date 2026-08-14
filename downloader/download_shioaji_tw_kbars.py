from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import polars as pl

from stockagent.live.shioaji_traffic_ledger import record_avoided_query, shioaji_query


SHIOAJI_STOCK_HISTORY_START = date(2020, 3, 2)
MAX_KBAR_QUERY_DAYS = 30
SOURCE_NAME = "shioaji_kbars_1m"
RECEIPT_SCHEMA_VERSION = 2
STORAGE_FREQUENCY = "daily"


class TrafficBudgetReached(RuntimeError):
    """Raised after a successful usage check reaches the configured budget."""


@dataclass(slots=True, frozen=True)
class UniverseRow:
    symbol: str
    name: str
    market: str
    security_type: str
    base_path: Path


@dataclass(slots=True)
class SymbolResult:
    symbol: str
    status: str
    chunks_total: int
    chunks_complete: int
    source_minute_rows: int
    daily_rows: int
    first_date: str | None
    last_date: str | None
    output_path: str
    message: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Query Shioaji Taiwan stock Kbars in resumable 30-day chunks, aggregate "
            "each response in memory, and persist daily OHLCV only. No minute-level "
            "files are stored. Credentials are read only from SHIOAJI_API_KEY and "
            "SHIOAJI_SECRET_KEY."
        )
    )
    parser.add_argument(
        "--base-stock-root", type=Path, default=Path("data_tw_public/stocks")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data_tw_public/shioaji")
    )
    parser.add_argument(
        "--minute-cache-root",
        type=Path,
        default=Path("data_tw_minute/shioaji_1m"),
        help="Receipt-backed canonical minute source used before any API KBar query.",
    )
    parser.add_argument(
        "--start-date", default=SHIOAJI_STOCK_HISTORY_START.isoformat()
    )
    parser.add_argument(
        "--end-date", default=(date.today() - timedelta(days=1)).isoformat()
    )
    parser.add_argument("--chunk-days", type=int, default=MAX_KBAR_QUERY_DAYS)
    parser.add_argument(
        "--request-interval",
        type=float,
        default=0.25,
        help="Seconds between requests. 0.25 stays below the documented 50/10s cap.",
    )
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-backoff", type=float, default=3.0)
    parser.add_argument(
        "--max-traffic-fraction",
        type=float,
        default=0.90,
        help="Stop cleanly after this fraction of the Shioaji daily traffic quota.",
    )
    parser.add_argument(
        "--traffic-reserve-mb",
        type=float,
        default=25.0,
        help="Always leave at least this much daily traffic unused.",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated pilot subset, for example 2330,0050.",
    )
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument(
        "--simulation",
        action="store_true",
        help="Use the Shioaji simulation environment (required by simulation-only tokens).",
    )
    parser.add_argument(
        "--allow-market-hours",
        action="store_true",
        help="Explicitly allow historical queries during Taiwan market hours.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def iter_date_chunks(start: date, end: date, chunk_days: int) -> Iterable[tuple[date, date]]:
    if not 1 <= int(chunk_days) <= MAX_KBAR_QUERY_DAYS:
        raise ValueError(
            f"chunk_days must be between 1 and {MAX_KBAR_QUERY_DAYS}, got {chunk_days}"
        )
    if start > end:
        return
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=int(chunk_days) - 1))
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.write_parquet(
        temporary,
        compression="snappy",
        statistics=True,
        row_group_size=128_000,
    )
    os.replace(temporary, path)
    return {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": _sha256(path),
    }


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _receipt_valid(path: Path, *, symbol: str, start: date, end: date) -> bool:
    payload = _read_json(path)
    if payload is None:
        return False
    if not (
        payload.get("schema_version") == RECEIPT_SCHEMA_VERSION
        and payload.get("source") == SOURCE_NAME
        and payload.get("storage_frequency") == STORAGE_FREQUENCY
        and payload.get("symbol") == symbol
        and payload.get("start_date") == start.isoformat()
        and payload.get("end_date") == end.isoformat()
        and payload.get("status") in {"ok", "empty"}
    ):
        return False
    if payload["status"] == "empty":
        return (
            int(payload.get("rows", -1)) == 0
            and int(payload.get("daily_rows", -1)) == 0
            and int(payload.get("source_minute_rows", -1)) == 0
        )
    output = payload.get("output_receipt")
    if not isinstance(output, dict):
        return False
    output_path = Path(str(output.get("path", "")))
    try:
        return (
            output_path.is_file()
            and int(output.get("size", -1)) == int(output_path.stat().st_size)
            and str(output.get("sha256", "")) == _sha256(output_path)
        )
    except OSError:
        return False


def _load_universe(root: Path) -> list[UniverseRow]:
    manifest_path = root / "symbols.csv"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"TW public symbol manifest is missing: {manifest_path}")
    frame = pl.read_csv(manifest_path, infer_schema_length=0)
    required = {"code", "name", "market", "security_type"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{manifest_path} is missing columns: {missing}")
    rows: list[UniverseRow] = []
    for item in frame.iter_rows(named=True):
        security_type = str(item.get("security_type") or "").strip().lower()
        if security_type not in {"stock", "etf"}:
            continue
        symbol = str(item.get("code") or "").strip().upper()
        base_path = root / f"{symbol}_features.parquet"
        if not base_path.is_file():
            raise FileNotFoundError(f"base symbol parquet is missing: {base_path}")
        rows.append(
            UniverseRow(
                symbol=symbol,
                name=str(item.get("name") or symbol).strip(),
                market=str(item.get("market") or "").strip().lower(),
                security_type=security_type,
                base_path=base_path,
            )
        )
    return sorted(rows, key=lambda item: item.symbol)


def _scalar_text(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip()


def _payload_dict(payload: Any) -> dict[str, Any]:
    if hasattr(payload, "dict"):
        values = payload.dict()
    elif isinstance(payload, dict):
        values = payload
    else:
        values = {
            name: getattr(payload, name)
            for name in ("ts", "Open", "High", "Low", "Close", "Volume", "Amount")
            if hasattr(payload, name)
        }
    if not isinstance(values, dict):
        raise TypeError(f"unexpected Shioaji Kbars payload: {type(payload).__name__}")
    return values


def normalize_kbars(
    payload: Any,
    *,
    symbol: str,
    market: str,
    contract_unit: float,
) -> pl.DataFrame:
    values = _payload_dict(payload)
    required = ("ts", "Open", "High", "Low", "Close", "Volume", "Amount")
    missing = [name for name in required if name not in values]
    if missing:
        raise ValueError(f"Shioaji Kbars payload is missing fields: {missing}")
    lengths = {name: len(values[name]) for name in required}
    if len(set(lengths.values())) != 1:
        raise ValueError(f"Shioaji Kbars fields have different lengths: {lengths}")
    if not math.isfinite(contract_unit) or contract_unit <= 0.0:
        raise ValueError(f"invalid Shioaji contract unit for {symbol}: {contract_unit}")
    if not lengths["ts"]:
        return pl.DataFrame(
            schema={
                "ts": pl.Datetime("ns"),
                "date": pl.Date,
                "symbol": pl.String,
                "market": pl.String,
                "Open": pl.Float64,
                "High": pl.Float64,
                "Low": pl.Float64,
                "Close": pl.Float64,
                "Volume": pl.Float64,
                "Amount": pl.Float64,
                "contract_unit": pl.Float64,
            }
        )
    frame = pl.DataFrame({name: values[name] for name in required}).with_columns(
        pl.col("ts").cast(pl.Datetime("ns"), strict=False),
        *[
            pl.col(name).cast(pl.Float64, strict=False).fill_nan(None).alias(name)
            for name in ("Open", "High", "Low", "Close", "Volume", "Amount")
        ],
    ).with_columns(
        pl.col("ts").cast(pl.Date).alias("date"),
        pl.lit(symbol).alias("symbol"),
        pl.lit(market).alias("market"),
        pl.lit(float(contract_unit)).alias("contract_unit"),
    )
    valid = (
        pl.col("ts").is_not_null()
        & pl.all_horizontal(
            *[
                pl.col(name).is_not_null()
                & pl.col(name).is_finite()
                & (pl.col(name) > 0.0)
                for name in ("Open", "High", "Low", "Close")
            ]
        )
        & (pl.col("High") >= pl.max_horizontal("Open", "Low", "Close"))
        & (pl.col("Low") <= pl.min_horizontal("Open", "High", "Close"))
        & pl.col("Volume").is_not_null()
        & pl.col("Volume").is_finite()
        & (pl.col("Volume") >= 0.0)
        & pl.col("Amount").is_not_null()
        & pl.col("Amount").is_finite()
        & (pl.col("Amount") >= 0.0)
    )
    invalid = int(frame.select((~valid).fill_null(True).sum()).item() or 0)
    if invalid:
        raise ValueError(f"Shioaji returned {invalid} invalid Kbar rows for {symbol}")
    duplicate = frame.group_by("ts").len().filter(pl.col("len") > 1).height
    if duplicate:
        raise ValueError(f"Shioaji returned {duplicate} duplicate timestamps for {symbol}")
    return frame.sort("ts")


def aggregate_daily(frame: pl.DataFrame, *, name: str) -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "symbol": pl.String,
                "name": pl.String,
                "market": pl.String,
                "open": pl.Float64,
                "max": pl.Float64,
                "min": pl.Float64,
                "close": pl.Float64,
                "Trading_Volume": pl.Float64,
                "Trading_Value": pl.Float64,
                "shioaji_volume_lots": pl.Float64,
                "shioaji_minute_bars": pl.Int64,
                "data_source": pl.String,
            }
        )
    daily = (
        frame.sort("ts")
        .group_by(["date", "symbol", "market"], maintain_order=True)
        .agg(
            pl.col("Open").first().alias("open"),
            pl.col("High").max().alias("max"),
            pl.col("Low").min().alias("min"),
            pl.col("Close").last().alias("close"),
            (pl.col("Volume") * pl.col("contract_unit")).sum().alias("Trading_Volume"),
            pl.col("Amount").sum().alias("Trading_Value"),
            pl.col("Volume").sum().alias("shioaji_volume_lots"),
            pl.len().alias("shioaji_minute_bars"),
        )
        .with_columns(
            pl.lit(name).alias("name"),
            pl.lit(SOURCE_NAME).alias("data_source"),
        )
        .select(
            "date",
            "symbol",
            "name",
            "market",
            "open",
            "max",
            "min",
            "close",
            "Trading_Volume",
            "Trading_Value",
            "shioaji_volume_lots",
            "shioaji_minute_bars",
            "data_source",
        )
        .sort("date")
    )
    invalid_value_scale = daily.filter(
        (pl.col("Trading_Volume") > 0.0)
        & (
            (pl.col("Trading_Value") <= 0.0)
            | (
                (pl.col("Trading_Value") / pl.col("Trading_Volume"))
                < pl.col("min") * 0.999
            )
            | (
                (pl.col("Trading_Value") / pl.col("Trading_Volume"))
                > pl.col("max") * 1.001
            )
        )
    ).height
    if invalid_value_scale:
        raise ValueError(
            f"Shioaji daily amount/volume scale is inconsistent with prices for "
            f"{invalid_value_scale} rows; check the contract trading unit"
        )
    return daily


def _positive_volume_dates(path: Path, start: date, end: date) -> set[date]:
    frame = pl.read_parquet(path, columns=["date", "Trading_Volume"]).with_columns(
        pl.col("date").cast(pl.Date, strict=False),
        pl.col("Trading_Volume").cast(pl.Float64, strict=False),
    )
    return set(
        frame.filter(
            (pl.col("date") >= pl.lit(start))
            & (pl.col("date") <= pl.lit(end))
            & pl.col("Trading_Volume").is_not_null()
            & (pl.col("Trading_Volume") > 0.0)
        ).get_column("date")
    )


def _usage_values(api: Any) -> tuple[int, int]:
    usage = api.usage()
    used = int(getattr(usage, "bytes"))
    limit = int(getattr(usage, "limit_bytes"))
    if used < 0 or limit <= 0:
        raise RuntimeError(f"invalid Shioaji usage response: used={used} limit={limit}")
    return used, limit


def _taiwan_market_hours_now() -> bool:
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    minute = now.hour * 60 + now.minute
    # A conservative buffer around the continuous 09:00-13:30 session. The
    # official fair-use guidance asks historical queries to run after market.
    return now.weekday() < 5 and 8 * 60 + 30 <= minute <= 14 * 60 + 30


def _check_traffic_budget(
    api: Any, *, max_fraction: float, reserve_bytes: int
) -> tuple[int, int]:
    used, limit = _usage_values(api)
    ceiling = min(int(limit * max_fraction), limit - reserve_bytes)
    if used >= max(0, ceiling):
        raise TrafficBudgetReached(
            f"Shioaji daily traffic reserve reached: used={used} limit={limit} "
            f"ceiling={ceiling}"
        )
    return used, limit


def _contract_for_symbol(api: Any, row: UniverseRow) -> tuple[Any | None, float, str]:
    contract = api.contracts.get(row.symbol)
    if contract is None:
        return None, 0.0, "contract_not_found"
    security_type = _scalar_text(getattr(contract, "security_type", "")).upper()
    if security_type not in {"STK", "STOCK"}:
        return None, 0.0, f"unexpected_security_type={security_type}"
    exchange = _scalar_text(getattr(contract, "exchange", "")).upper()
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


def _query_chunk(
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
) -> pl.DataFrame:
    last_error: Exception | None = None
    for attempt in range(max(0, retries) + 1):
        try:
            with shioaji_query(
                api,
                consumer="stock_daily_legacy_backfill",
                method="kbars",
                asset_class="stock",
                details={
                    "contract": row.symbol,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                },
            ) as set_ledger_result:
                payload = api.kbars(
                    contract=contract,
                    start=start.isoformat(),
                    end=end.isoformat(),
                    timeout=int(timeout_ms),
                )
                set_ledger_result(payload)
            frame = normalize_kbars(
                payload,
                symbol=row.symbol,
                market=row.market,
                contract_unit=contract_unit,
            )
            returned_dates = set(frame.get_column("date")) if frame.height else set()
            missing_dates = sorted(expected_dates - returned_dates)
            if missing_dates:
                raise RuntimeError(
                    f"Shioaji Kbars omitted {len(missing_dates)} public positive-volume "
                    f"sessions for {row.symbol}: {missing_dates[:10]}"
                )
            return frame
        except Exception as exc:  # API exceptions vary by SDK/runtime version.
            last_error = exc
            if attempt >= max(0, retries):
                break
            time.sleep(float(retry_backoff) * (2**attempt))
    assert last_error is not None
    raise last_error


def _cached_minute_chunk(
    root: Path,
    row: UniverseRow,
    *,
    start: date,
    end: date,
    expected_dates: set[date],
) -> pl.DataFrame | None:
    """Return a verified local minute slice, or None so the API path remains intact."""

    manifest = _read_json(root / "symbols" / f"{row.symbol}.manifest.json")
    if not manifest or manifest.get("source") != "shioaji_kbars_1m":
        return None
    try:
        covered_start = date.fromisoformat(str(manifest["requested_start"]))
        covered_end = date.fromisoformat(str(manifest["requested_end"]))
    except (KeyError, ValueError):
        return None
    if covered_start > start or covered_end < end:
        return None
    gaps = {
        date.fromisoformat(str(value))
        for value in manifest.get("source_gap_dates", [])
    }
    if gaps & expected_dates:
        return None
    frames: list[pl.DataFrame] = []
    for entry in manifest.get("chunks", []):
        try:
            entry_start = date.fromisoformat(str(entry["start_date"]))
            entry_end = date.fromisoformat(str(entry["end_date"]))
        except (KeyError, ValueError, TypeError):
            return None
        if entry_end < start or entry_start > end:
            continue
        data_path_text = entry.get("data_path")
        if not data_path_text:
            continue
        data_path = Path(str(data_path_text))
        expected_sha = str(entry.get("data_sha256") or "")
        try:
            if not data_path.is_file() or not expected_sha or _sha256(data_path) != expected_sha:
                return None
            frames.append(
                pl.read_parquet(data_path).filter(
                    (pl.col("date") >= pl.lit(start))
                    & (pl.col("date") <= pl.lit(end))
                )
            )
        except (OSError, pl.exceptions.PolarsError):
            return None
    if not frames:
        return None if expected_dates else aggregate_daily(pl.DataFrame(), name=row.name)
    frame = pl.concat(frames, how="vertical_relaxed").sort("ts")
    if frame.get_column("ts").n_unique() != frame.height:
        return None
    returned_dates = set(frame.get_column("date")) if frame.height else set()
    if not expected_dates.issubset(returned_dates):
        return None
    return frame


def _chunk_paths(root: Path, symbol: str, start: date, end: date) -> tuple[Path, Path]:
    stem = f"{start.isoformat()}_{end.isoformat()}"
    symbol_root = root / "daily_chunks" / symbol
    return symbol_root / f"{stem}.parquet", symbol_root / f"{stem}.receipt.json"


def _aggregate_symbol_from_receipts(
    output_dir: Path,
    row: UniverseRow,
    chunks: list[tuple[date, date]],
) -> tuple[pl.DataFrame, int]:
    frames: list[pl.DataFrame] = []
    source_minute_rows = 0
    for start, end in chunks:
        daily_chunk_path, receipt_path = _chunk_paths(
            output_dir, row.symbol, start, end
        )
        payload = _read_json(receipt_path)
        if payload is None or payload.get("status") not in {"ok", "empty"}:
            raise RuntimeError(f"missing complete Shioaji receipt: {receipt_path}")
        source_minute_rows += int(payload.get("source_minute_rows", 0))
        if payload["status"] == "ok":
            frames.append(pl.read_parquet(daily_chunk_path))
    daily = (
        pl.concat(frames, how="vertical_relaxed").sort("date")
        if frames
        else aggregate_daily(pl.DataFrame(), name=row.name)
    )
    if daily.height and daily.get_column("date").n_unique() != daily.height:
        raise RuntimeError(f"duplicate Shioaji daily dates across chunks: {row.symbol}")
    return daily, source_minute_rows


def _finalize_symbol(
    output_dir: Path,
    row: UniverseRow,
    chunks: list[tuple[date, date]],
    *,
    requested_start: date,
    requested_end: date,
) -> SymbolResult:
    daily, source_minute_rows = _aggregate_symbol_from_receipts(
        output_dir, row, chunks
    )
    daily_path = output_dir / "daily" / f"{row.symbol}.parquet"
    output_receipt = _write_parquet_atomic(daily, daily_path)
    _atomic_write_json(
        daily_path.with_suffix(".summary.json"),
        {
            "schema_version": 1,
            "source": SOURCE_NAME,
            "storage_frequency": STORAGE_FREQUENCY,
            "symbol": row.symbol,
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "chunks": len(chunks),
            "source_minute_rows": source_minute_rows,
            "daily_rows": daily.height,
            "first_date": str(daily["date"].min()) if daily.height else None,
            "last_date": str(daily["date"].max()) if daily.height else None,
            "output_receipt": output_receipt,
        },
    )
    return SymbolResult(
        symbol=row.symbol,
        status="complete",
        chunks_total=len(chunks),
        chunks_complete=len(chunks),
        source_minute_rows=source_minute_rows,
        daily_rows=daily.height,
        first_date=str(daily["date"].min()) if daily.height else None,
        last_date=str(daily["date"].max()) if daily.height else None,
        output_path=str(daily_path),
    )


def _completed_daily_result(
    output_dir: Path,
    row: UniverseRow,
    chunks: list[tuple[date, date]],
    *,
    requested_start: date,
    requested_end: date,
) -> SymbolResult | None:
    daily_path = output_dir / "daily" / f"{row.symbol}.parquet"
    summary = _read_json(daily_path.with_suffix(".summary.json"))
    if summary is None:
        return None
    output = summary.get("output_receipt")
    if not (
        summary.get("source") == SOURCE_NAME
        and summary.get("storage_frequency") == STORAGE_FREQUENCY
        and summary.get("symbol") == row.symbol
        and summary.get("requested_start") == requested_start.isoformat()
        and summary.get("requested_end") == requested_end.isoformat()
        and int(summary.get("chunks", -1)) == len(chunks)
        and isinstance(output, dict)
        and daily_path.is_file()
        and int(output.get("size", -1)) == int(daily_path.stat().st_size)
        and str(output.get("sha256", "")) == _sha256(daily_path)
    ):
        return None
    return SymbolResult(
        symbol=row.symbol,
        status="complete",
        chunks_total=len(chunks),
        chunks_complete=len(chunks),
        source_minute_rows=int(summary.get("source_minute_rows", 0)),
        daily_rows=int(summary.get("daily_rows", 0)),
        first_date=(
            str(summary.get("first_date")) if summary.get("first_date") else None
        ),
        last_date=(
            str(summary.get("last_date")) if summary.get("last_date") else None
        ),
        output_path=str(daily_path),
    )


def _write_summary(
    output_dir: Path,
    *,
    args: argparse.Namespace,
    universe_size: int,
    selected_size: int,
    results: list[SymbolResult],
    traffic: tuple[int, int] | None,
    stopped_for_traffic: bool,
) -> None:
    report_path = output_dir / "download_report.csv"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if results:
        pl.DataFrame([asdict(item) for item in results]).sort("symbol").write_csv(report_path)
    else:
        pl.DataFrame(
            schema={name: pl.String for name in SymbolResult.__dataclass_fields__}
        ).write_csv(report_path)
    complete_statuses = {"complete", "contract_unavailable"}
    selected_complete = (
        len(results) == selected_size
        and all(item.status in complete_statuses for item in results)
    )
    full_selection = not str(args.symbols).strip() and int(args.max_symbols) <= 0
    summary = {
        "schema_version": 2,
        "source": SOURCE_NAME,
        "storage_frequency": STORAGE_FREQUENCY,
        "historical_start": SHIOAJI_STOCK_HISTORY_START.isoformat(),
        "start_date": str(args.start_date),
        "end_date": str(args.end_date),
        "chunk_days": int(args.chunk_days),
        "simulation": bool(args.simulation),
        "universe_symbols": universe_size,
        "selected_symbols": selected_size,
        "reported_symbols": len(results),
        "complete_symbols": sum(item.status == "complete" for item in results),
        "contract_unavailable_symbols": sum(
            item.status == "contract_unavailable" for item in results
        ),
        "failed_symbols": sum(item.status == "failed" for item in results),
        "partial_symbols": sum(item.status == "partial" for item in results),
        "selected_coverage_complete": selected_complete,
        "universe_coverage_complete": bool(full_selection and selected_complete),
        "stopped_for_traffic": stopped_for_traffic,
        "traffic_used_bytes": traffic[0] if traffic else None,
        "traffic_limit_bytes": traffic[1] if traffic else None,
        "report_path": str(report_path),
        "written_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    _atomic_write_json(output_dir / "download_summary.json", summary)


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
    if not 0.0 < float(args.max_traffic_fraction) < 1.0:
        raise ValueError("--max-traffic-fraction must be between 0 and 1")
    if float(args.request_interval) < 0.0:
        raise ValueError("--request-interval must be >= 0")
    list(iter_date_chunks(start, end, int(args.chunk_days)))

    universe = _load_universe(args.base_stock_root)
    requested = {
        item.strip().upper() for item in str(args.symbols).split(",") if item.strip()
    }
    if requested:
        known = {item.symbol for item in universe}
        unknown = sorted(requested - known)
        if unknown:
            raise ValueError(f"requested symbols are absent from the public universe: {unknown}")
        selected = [item for item in universe if item.symbol in requested]
    else:
        selected = list(universe)
    if int(args.max_symbols) > 0:
        selected = selected[: int(args.max_symbols)]
    if args.dry_run:
        total_chunks = sum(
            len(
                list(
                    iter_date_chunks(
                        max(start, pl.read_parquet(item.base_path, columns=["date"])["date"].min()),
                        end,
                        int(args.chunk_days),
                    )
                )
            )
            for item in selected
        )
        print(
            f"[shioaji] dry_run symbols={len(selected)} chunks<={total_chunks} "
            f"range={start}..{end} output={args.output_dir}",
            flush=True,
        )
        return

    if _taiwan_market_hours_now() and not args.allow_market_hours:
        raise RuntimeError(
            "Refusing a historical Shioaji backfill during Taiwan market hours "
            "(08:30-14:30 safety window). Run it after market close; use "
            "--allow-market-hours only for a deliberate exception."
        )

    api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError(
            "Shioaji credentials are required; export SHIOAJI_API_KEY and "
            "SHIOAJI_SECRET_KEY. Credentials are intentionally not accepted as CLI args."
        )
    try:
        import shioaji as sj
    except ImportError as exc:
        raise RuntimeError(
            "Shioaji SDK is not installed in the selected environment; install requirements.txt"
        ) from exc

    api = sj.Shioaji(simulation=bool(args.simulation))
    # The SDK's default event printer can include account/person identifiers in
    # connection topic strings. Keep resumable logs credential-safe; API call
    # failures still surface through their exceptions below.
    api.set_event_callback(lambda _code, _event_code, _info, _event: None)
    api.login(
        api_key=api_key,
        secret_key=secret_key,
        subscribe_trade=False,
    )
    results: list[SymbolResult] = []
    stopped_for_traffic = False
    traffic: tuple[int, int] | None = None
    queried_chunks = 0
    run_started = time.monotonic()
    last_progress_write = 0.0
    progress_path = args.output_dir / "progress.json"

    def persist_progress(
        *,
        state: str,
        symbol_index: int,
        current_symbol: str,
        chunk_index: int = 0,
        chunk_total: int = 0,
        force: bool = False,
    ) -> None:
        nonlocal last_progress_write
        now = time.monotonic()
        if not force and queried_chunks % 100 != 0 and now - last_progress_write < 60.0:
            return
        payload = {
            "schema_version": 2,
            "source": SOURCE_NAME,
            "storage_frequency": STORAGE_FREQUENCY,
            "state": state,
            "simulation": bool(args.simulation),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "selected_symbols": len(selected),
            "reported_symbols_this_run": len(results),
            "symbol_index": symbol_index,
            "current_symbol": current_symbol,
            "chunk_index": chunk_index,
            "chunk_total": chunk_total,
            "queried_chunks_this_run": queried_chunks,
            "complete_symbols_this_run": sum(
                item.status == "complete" for item in results
            ),
            "contract_unavailable_symbols_this_run": sum(
                item.status == "contract_unavailable" for item in results
            ),
            "failed_symbols_this_run": sum(item.status == "failed" for item in results),
            "traffic_used_bytes": traffic[0] if traffic else None,
            "traffic_limit_bytes": traffic[1] if traffic else None,
            "elapsed_seconds": round(now - run_started, 3),
            "updated_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        }
        _atomic_write_json(progress_path, payload)
        last_progress_write = now
        if force or queried_chunks % 100 == 0:
            print(
                f"[shioaji-progress] state={state} symbol={current_symbol} "
                f"symbol_index={symbol_index}/{len(selected)} "
                f"chunk={chunk_index}/{chunk_total} queried={queried_chunks} "
                f"traffic={payload['traffic_used_bytes']}/{payload['traffic_limit_bytes']}",
                flush=True,
            )

    reserve_bytes = int(float(args.traffic_reserve_mb) * 1024 * 1024)
    try:
        traffic = _check_traffic_budget(
            api,
            max_fraction=float(args.max_traffic_fraction),
            reserve_bytes=reserve_bytes,
        )
    except TrafficBudgetReached as exc:
        stopped_for_traffic = True
        try:
            traffic = _usage_values(api)
        except Exception:
            pass
        try:
            api.logout()
        finally:
            # Preserve the previous resumable full report when a same-day rerun
            # starts after the quota reserve has already been reached.
            if not (args.output_dir / "download_summary.json").is_file():
                _write_summary(
                    args.output_dir,
                    args=args,
                    universe_size=len(universe),
                    selected_size=len(selected),
                    results=results,
                    traffic=traffic,
                    stopped_for_traffic=True,
                )
        print(f"[shioaji] {exc}", flush=True)
        return
    try:
        persist_progress(
            state="running",
            symbol_index=0,
            current_symbol="",
            force=True,
        )
        for symbol_index, row in enumerate(selected, start=1):
            base_dates = pl.read_parquet(row.base_path, columns=["date"]).get_column("date")
            symbol_start = max(start, base_dates.min())
            chunks = list(iter_date_chunks(symbol_start, end, int(args.chunk_days)))
            completed_daily = _completed_daily_result(
                args.output_dir,
                row,
                chunks,
                requested_start=symbol_start,
                requested_end=end,
            )
            if completed_daily is not None:
                results.append(completed_daily)
                continue
            positive_volume_dates = _positive_volume_dates(
                row.base_path, symbol_start, end
            )
            completed = sum(
                _receipt_valid(
                    _chunk_paths(args.output_dir, row.symbol, chunk_start, chunk_end)[1],
                    symbol=row.symbol,
                    start=chunk_start,
                    end=chunk_end,
                )
                for chunk_start, chunk_end in chunks
            )
            if completed == len(chunks):
                results.append(
                    _finalize_symbol(
                        args.output_dir,
                        row,
                        chunks,
                        requested_start=symbol_start,
                        requested_end=end,
                    )
                )
                continue
            contract, unit, contract_message = _contract_for_symbol(api, row)
            if contract is None:
                results.append(
                    SymbolResult(
                        symbol=row.symbol,
                        status="contract_unavailable",
                        chunks_total=len(chunks),
                        chunks_complete=0,
                        source_minute_rows=0,
                        daily_rows=0,
                        first_date=None,
                        last_date=None,
                        output_path="",
                        message=contract_message,
                    )
                )
                continue
            try:
                for chunk_index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
                    daily_chunk_path, receipt_path = _chunk_paths(
                        args.output_dir, row.symbol, chunk_start, chunk_end
                    )
                    if _receipt_valid(
                        receipt_path,
                        symbol=row.symbol,
                        start=chunk_start,
                        end=chunk_end,
                    ):
                        continue
                    expected_dates = {
                        value
                        for value in positive_volume_dates
                        if chunk_start <= value <= chunk_end
                    }
                    minute_frame = _cached_minute_chunk(
                        args.minute_cache_root,
                        row,
                        start=chunk_start,
                        end=chunk_end,
                        expected_dates=expected_dates,
                    )
                    cache_reused = minute_frame is not None
                    if minute_frame is None:
                        traffic = _check_traffic_budget(
                            api,
                            max_fraction=float(args.max_traffic_fraction),
                            reserve_bytes=reserve_bytes,
                        )
                        minute_frame = _query_chunk(
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
                    else:
                        record_avoided_query(
                            consumer="stock_daily_materializer",
                            method="kbars",
                            asset_class="stock",
                            reason="verified_minute_chunk_reuse",
                            rows=minute_frame.height,
                            details={
                                "contract": row.symbol,
                                "start": chunk_start.isoformat(),
                                "end": chunk_end.isoformat(),
                            },
                        )
                    # The API exposes intraday Kbars, but this module owns only
                    # daily history. Aggregate immediately and never persist the
                    # minute-level response.
                    daily_chunk = aggregate_daily(minute_frame, name=row.name)
                    output_receipt = (
                        _write_parquet_atomic(daily_chunk, daily_chunk_path)
                        if daily_chunk.height
                        else None
                    )
                    _atomic_write_json(
                        receipt_path,
                        {
                            "schema_version": RECEIPT_SCHEMA_VERSION,
                            "source": SOURCE_NAME,
                            "storage_frequency": STORAGE_FREQUENCY,
                            "symbol": row.symbol,
                            "market": row.market,
                            "contract_unit": unit,
                            "start_date": chunk_start.isoformat(),
                            "end_date": chunk_end.isoformat(),
                            "status": "ok" if daily_chunk.height else "empty",
                            "rows": daily_chunk.height,
                            "daily_rows": daily_chunk.height,
                            "source_minute_rows": minute_frame.height,
                            "minute_source_reused": cache_reused,
                            "minute_source_root": (
                                str(args.minute_cache_root) if cache_reused else None
                            ),
                            "expected_positive_volume_sessions": len(expected_dates),
                            "output_receipt": output_receipt,
                            "downloaded_at_utc": datetime.now(timezone.utc)
                            .replace(microsecond=0)
                            .isoformat(),
                        },
                    )
                    completed += 1
                    if not cache_reused:
                        queried_chunks += 1
                        traffic = _check_traffic_budget(
                            api,
                            max_fraction=float(args.max_traffic_fraction),
                            reserve_bytes=reserve_bytes,
                        )
                    persist_progress(
                        state="running",
                        symbol_index=symbol_index,
                        current_symbol=row.symbol,
                        chunk_index=chunk_index,
                        chunk_total=len(chunks),
                    )
                    if float(args.request_interval):
                        time.sleep(float(args.request_interval))
                results.append(
                    _finalize_symbol(
                        args.output_dir,
                        row,
                        chunks,
                        requested_start=symbol_start,
                        requested_end=end,
                    )
                )
            except TrafficBudgetReached as exc:
                stopped_for_traffic = True
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
        _write_summary(
            args.output_dir,
            args=args,
            universe_size=len(universe),
            selected_size=len(selected),
            results=results,
            traffic=traffic,
            stopped_for_traffic=stopped_for_traffic,
        )
        persist_progress(
            state="stopped_for_traffic" if stopped_for_traffic else "finished",
            symbol_index=len(results),
            current_symbol=results[-1].symbol if results else "",
            force=True,
        )
    print(
        f"[shioaji] complete={sum(x.status == 'complete' for x in results)} "
        f"unavailable={sum(x.status == 'contract_unavailable' for x in results)} "
        f"failed={sum(x.status == 'failed' for x in results)} "
        f"partial={sum(x.status == 'partial' for x in results)} "
        f"summary={args.output_dir / 'download_summary.json'}",
        flush=True,
    )


if __name__ == "__main__":
    main()
