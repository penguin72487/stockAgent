from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import threading
from typing import Any

import polars as pl

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import PersistentProgress, atomic_write_text  # noqa: E402
from artifact_io import (  # noqa: E402
    atomic_write_parquet,
    sha256_file,
)
from download_bybit_perp_daily import (  # noqa: E402
    BybitClient,
    SymbolRecord,
    _date_to_ms,
    _fetch_perp_symbols,
    _ms_to_date_string,
)


FUNDING_ENDPOINT = "/v5/market/funding/history"
MARK_PRICE_KLINE_ENDPOINT = "/v5/market/mark-price-kline"
FUNDING_CONTRACT_VERSION = 3
HOUR_MS = 60 * 60 * 1000
MARK_LIMIT = 1000


@dataclass(slots=True)
class FundingResult:
    symbol: str
    status: str
    rows: int
    requested_start_utc: str
    coverage_start_utc: str
    coverage_end_utc: str
    first_funding_utc: str | None
    last_funding_utc: str | None
    head_complete: bool
    quarantined_prefix_events: int
    output_path: str | None
    sha256: str | None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download point-in-time Bybit perpetual funding events. The output "
            "is an executor audit source, never a model feature."
        )
    )
    parser.add_argument("--output-dir", default="data_bybit/funding")
    parser.add_argument("--start-date", default="2019-01-01")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--request-interval", type=float, default=None)
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--retry-base", type=float, default=0.6)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--symbols", nargs="*", default=None)
    parser.add_argument(
        "--historical-instruments", type=Path,
        help="Explicit retained instrument CSV for requested historical symbols; not current trading eligibility.",
    )
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    if args.historical_instruments is not None and not any(
        str(value).strip() for value in (args.symbols or [])
    ):
        parser.error("--historical-instruments requires explicit nonempty --symbols")
    return args


def _standard_linear_usdt(record: SymbolRecord) -> bool:
    return _standard_linear_usdt_identity(record) and str(record.status or "") == "Trading"


def _standard_linear_usdt_identity(record: SymbolRecord) -> bool:
    """Product identity only: an old status must never establish current trading."""
    return bool(
        record.category == "linear"
        and record.quote_coin == "USDT"
        and record.settle_coin == "USDT"
        and "LinearPerpetual" in str(record.contract_type or "")
        and not str(record.symbol_type or "").strip()
        and not bool(record.is_pre_listing)
    )


def _historical_instrument_records(
    path: Path, requested_symbols: set[str]
) -> list[SymbolRecord]:
    """Read explicit identities without manufacturing launch/interval metadata."""
    if not requested_symbols:
        raise ValueError("historical instruments require explicit nonempty symbols")
    frame = pl.read_csv(path, infer_schema_length=10_000)
    required = {field.name for field in fields(SymbolRecord)}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"historical instrument CSV missing columns: {sorted(missing)}")
    frame = frame.filter(pl.col("code").cast(pl.String).str.to_uppercase().is_in(requested_symbols))
    records: list[SymbolRecord] = []
    seen: set[str] = set()
    for row in frame.select(sorted(required)).to_dicts():
        record = SymbolRecord(**row)
        code = str(record.code).strip().upper()
        if code in seen:
            raise ValueError(f"duplicate historical instrument: {code}")
        seen.add(code)
        if (
            record.code != code or not code.isascii() or not code.isalnum()
            or record.bybit_symbol != code or not _standard_linear_usdt_identity(record)
            or record.is_pre_listing is not False
            or not record.launch_time
            or not isinstance(record.funding_interval_minutes, int)
            or isinstance(record.funding_interval_minutes, bool)
            or record.funding_interval_minutes <= 0
        ):
            raise ValueError(f"incompatible or incomplete historical linear-USDT instrument: {code}")
        _utc_text_to_ms(str(record.launch_time))
        records.append(record)
    missing = requested_symbols - seen
    if missing:
        raise ValueError(f"requested symbols absent from historical instrument CSV: {sorted(missing)}")
    return sorted(records, key=lambda record: record.code)


def _select_funding_instruments(
    current: list[SymbolRecord], requested_symbols: set[str], historical_path: Path | None
) -> list[SymbolRecord]:
    if historical_path is not None and not requested_symbols:
        raise ValueError("historical instruments require explicit nonempty symbols")
    if historical_path is not None and not historical_path.is_file():
        raise FileNotFoundError(f"missing historical instrument CSV: {historical_path}")
    selected = [record for record in current if _standard_linear_usdt(record)]
    if not requested_symbols:
        return selected
    selected = [record for record in selected if record.code.upper() in requested_symbols]
    missing = requested_symbols - {record.code.upper() for record in selected}
    if historical_path is not None and missing:
        # A live identity conflict cannot be overridden with an old same-name CSV.
        for record in current:
            if record.code.upper() in missing and not _standard_linear_usdt_identity(record):
                raise ValueError(f"current instrument identity conflicts with historical request: {record.code}")
        selected.extend(_historical_instrument_records(historical_path, missing))
        missing = requested_symbols - {record.code.upper() for record in selected}
    if missing:
        raise ValueError(f"requested symbols are not standard linear USDT perps: {sorted(missing)}")
    return selected


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(path, frame, compression="snappy", write_statistics=True)


def _sha256(path: Path) -> str:
    return sha256_file(path)


def _utc_text_to_ms(value: str) -> int:
    resolved = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if resolved.tzinfo is None:
        resolved = resolved.replace(tzinfo=timezone.utc)
    return int(resolved.astimezone(timezone.utc).timestamp() * 1000)


def _funding_mark_prices(
    client: BybitClient,
    record: SymbolRecord,
    event_times_ms: list[int],
) -> dict[int, float]:
    """Resolve official mark-price Kline opens at funding timestamps.

    Bybit funding settlements occur on hour boundaries, including dynamically
    shortened one-hour schedules.  Hourly mark Klines reduce request volume by
    roughly 60x versus a full 1m mark archive while preserving the mark at the
    event boundary. Off-grid events fail closed. The caller distinguishes a
    quarantinable launch-prefix mark gap from an illegal internal gap.
    """

    if not event_times_ms:
        return {}
    if any(timestamp % HOUR_MS != 0 for timestamp in event_times_ms):
        raise RuntimeError("funding event is not aligned to an hourly mark boundary")
    start_ms = min(event_times_ms)
    end_ms = max(event_times_ms)
    mark_by_time: dict[int, float] = {}
    cursor = start_ms
    span = (MARK_LIMIT - 1) * HOUR_MS
    while cursor <= end_ms:
        window_end = min(cursor + span, end_ms)
        payload = client.get(
            MARK_PRICE_KLINE_ENDPOINT,
            {
                "category": "linear",
                "symbol": record.bybit_symbol,
                "interval": "60",
                "start": str(cursor),
                "end": str(window_end),
                "limit": str(MARK_LIMIT),
            },
        )
        for row in payload.get("result", {}).get("list", []):
            if len(row) >= 2:
                mark_by_time[int(row[0])] = float(row[1])
        cursor = window_end + HOUR_MS
    return mark_by_time


def _quarantine_unmarked_launch_prefix(
    rows: list[dict[str, Any]],
    mark_by_time: dict[int, float],
) -> tuple[list[dict[str, Any]], int | None, int]:
    """Quarantine only a contiguous unavailable head; internal gaps are fatal."""

    event_times = sorted({int(row["funding_timestamp_ms"]) for row in rows})
    missing_marks = sorted(set(event_times) - set(mark_by_time))
    if not missing_marks:
        return rows, None, 0
    available_marks = sorted(set(event_times) & set(mark_by_time))
    if not available_marks or any(
        timestamp > available_marks[0] for timestamp in missing_marks
    ):
        preview = ", ".join(_ms_to_date_string(value) for value in missing_marks[:5])
        raise RuntimeError(
            "official hourly mark price has a non-prefix gap at "
            f"{len(missing_marks)} funding events: {preview}"
        )
    prefix_end_ms = max(missing_marks)
    retained = [row for row in rows if int(row["funding_timestamp_ms"]) > prefix_end_ms]
    quarantined = len(rows) - len(retained)
    return retained, prefix_end_ms, quarantined


def _download_symbol(
    client: BybitClient,
    record: SymbolRecord,
    *,
    requested_start_ms: int,
    snapshot_ms: int,
    output_dir: Path,
    refresh: bool,
) -> FundingResult:
    path = output_dir / f"{record.code}_funding.parquet"
    effective_start_ms = max(
        requested_start_ms,
        _utc_text_to_ms(str(record.launch_time))
        if record.launch_time
        else requested_start_ms,
    )
    requested_text = _ms_to_date_string(requested_start_ms)
    coverage_start_text = _ms_to_date_string(effective_start_ms)
    snapshot_text = _ms_to_date_string(snapshot_ms)
    if path.is_file() and not refresh:
        frame = pl.read_parquet(path)
        current_schema = {
            "bybit_funding_contract_version",
            "funding_time_utc",
            "funding_timestamp_ms",
            "funding_rate",
            "funding_mark_price",
            "funding_mark_price_source",
            "funding_prefix_quarantined_events",
            "funding_coverage_start_utc",
            "download_snapshot_utc",
        }
        if (
            frame.height
            and current_schema.issubset(frame.columns)
            and int(frame["bybit_funding_contract_version"].max())
            == FUNDING_CONTRACT_VERSION
        ):
            latest_snapshot = str(frame["download_snapshot_utc"].max())
            if latest_snapshot[:10] == snapshot_text[:10]:
                return FundingResult(
                    symbol=record.code,
                    status="skipped_current_snapshot",
                    rows=frame.height,
                    requested_start_utc=requested_text,
                    coverage_start_utc=str(frame["funding_coverage_start_utc"].max()),
                    coverage_end_utc=latest_snapshot,
                    first_funding_utc=(
                        str(frame["funding_time_utc"].min()) if frame.height else None
                    ),
                    last_funding_utc=(
                        str(frame["funding_time_utc"].max()) if frame.height else None
                    ),
                    head_complete=True,
                    quarantined_prefix_events=int(
                        frame["funding_prefix_quarantined_events"].max()
                    ),
                    output_path=str(path),
                    sha256=_sha256(path),
                )

    end_ms = snapshot_ms
    rows: list[dict[str, Any]] = []
    head_complete = False
    seen_oldest: int | None = None
    while end_ms >= effective_start_ms:
        payload = client.get(
            FUNDING_ENDPOINT,
            {
                "category": "linear",
                "symbol": record.bybit_symbol,
                "endTime": str(end_ms),
                "limit": "200",
            },
        )
        items = payload.get("result", {}).get("list", [])
        if not items:
            head_complete = True
            break
        page_times: list[int] = []
        for item in items:
            timestamp = int(item["fundingRateTimestamp"])
            page_times.append(timestamp)
            if timestamp < effective_start_ms or timestamp > snapshot_ms:
                continue
            rows.append(
                {
                    "funding_time_utc": _ms_to_date_string(timestamp),
                    "funding_timestamp_ms": timestamp,
                    "funding_rate": float(item["fundingRate"]),
                    "symbol": record.code,
                    "category": "linear",
                    "download_snapshot_utc": snapshot_text,
                }
            )
        oldest = min(page_times)
        if oldest <= effective_start_ms:
            head_complete = True
            break
        if seen_oldest is not None and oldest >= seen_oldest:
            raise RuntimeError("funding pagination did not move backward")
        seen_oldest = oldest
        end_ms = oldest - 1

    if not head_complete:
        raise RuntimeError(
            "funding history did not reach the requested/launch boundary"
        )
    event_times = sorted({int(row["funding_timestamp_ms"]) for row in rows})
    mark_by_time = _funding_mark_prices(client, record, event_times)
    rows, prefix_end_ms, quarantined_prefix_events = _quarantine_unmarked_launch_prefix(
        rows, mark_by_time
    )
    if prefix_end_ms is not None:
        effective_start_ms = max(effective_start_ms, prefix_end_ms)
        coverage_start_text = _ms_to_date_string(effective_start_ms)
    for row in rows:
        row["funding_mark_price"] = mark_by_time[int(row["funding_timestamp_ms"])]
        row["funding_mark_price_source"] = "bybit_hourly_mark_kline_open"
        row["bybit_funding_contract_version"] = FUNDING_CONTRACT_VERSION
        row["funding_prefix_quarantined_events"] = quarantined_prefix_events
        row["funding_coverage_start_utc"] = coverage_start_text
    frame = (
        pl.DataFrame(rows)
        .unique(subset=["funding_timestamp_ms"], keep="last")
        .sort("funding_timestamp_ms")
        if rows
        else pl.DataFrame(
            schema={
                "funding_time_utc": pl.String,
                "funding_timestamp_ms": pl.Int64,
                "funding_rate": pl.Float64,
                "funding_mark_price": pl.Float64,
                "funding_mark_price_source": pl.String,
                "bybit_funding_contract_version": pl.Int16,
                "funding_prefix_quarantined_events": pl.Int32,
                "funding_coverage_start_utc": pl.String,
                "symbol": pl.String,
                "category": pl.String,
                "download_snapshot_utc": pl.String,
            }
        )
    )
    _write_parquet_atomic(frame, path)
    return FundingResult(
        symbol=record.code,
        status="updated",
        rows=frame.height,
        requested_start_utc=requested_text,
        coverage_start_utc=coverage_start_text,
        coverage_end_utc=snapshot_text,
        first_funding_utc=(
            str(frame["funding_time_utc"].min()) if frame.height else None
        ),
        last_funding_utc=(
            str(frame["funding_time_utc"].max()) if frame.height else None
        ),
        head_complete=True,
        quarantined_prefix_events=quarantined_prefix_events,
        output_path=str(path),
        sha256=_sha256(path),
    )


def main() -> None:
    args = parse_args()
    started = datetime.now(timezone.utc)
    snapshot_ms = int(started.timestamp() * 1000)
    requested_start_ms = _date_to_ms(args.start_date, end_of_day=False)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    client = BybitClient(args.request_interval, args.max_retries, args.retry_base)
    all_instruments = _fetch_perp_symbols(client, categories=["linear"])
    requested_symbols = {
        str(value).strip().upper()
        for value in (args.symbols or [])
        if str(value).strip()
    }
    historical_path = getattr(args, "historical_instruments", None)
    selected = _select_funding_instruments(all_instruments, requested_symbols, historical_path)
    if args.limit is not None:
        selected = selected[: max(0, int(args.limit))]
    if not selected:
        raise RuntimeError("no standard linear USDT perpetual instruments selected")
    # Keep this file an actual current API snapshot, never a merged historical list.
    atomic_write_text(
        output_dir / "instruments.csv",
        pl.DataFrame([asdict(item) for item in all_instruments]).write_csv(),
    )

    progress = PersistentProgress(
        output_dir / "progress.json",
        label="Bybit funding 歷史",
        total=len(selected),
        unit="symbol",
        basis="completed official funding-history symbol requests",
        started_at=started,
    )
    results: list[FundingResult] = []
    lock = threading.Lock()

    def work(record: SymbolRecord) -> FundingResult:
        return _download_symbol(
            client,
            record,
            requested_start_ms=requested_start_ms,
            snapshot_ms=snapshot_ms,
            output_dir=output_dir,
            refresh=bool(args.refresh),
        )

    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {executor.submit(work, item): item for item in selected}
        for future in as_completed(futures):
            item = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = FundingResult(
                    symbol=item.code,
                    status="failed",
                    rows=0,
                    requested_start_utc=_ms_to_date_string(requested_start_ms),
                    coverage_start_utc="",
                    coverage_end_utc="",
                    first_funding_utc=None,
                    last_funding_utc=None,
                    head_complete=False,
                    quarantined_prefix_events=0,
                    output_path=None,
                    sha256=None,
                    message=f"{type(exc).__name__}: {exc}",
                )
            with lock:
                results.append(result)
            progress.update("funding_history", result.status)

    ordered = sorted(results, key=lambda item: item.symbol)
    atomic_write_text(
        output_dir / "funding_coverage.csv",
        pl.DataFrame([asdict(item) for item in ordered]).write_csv(),
    )
    failed = [item for item in ordered if item.status == "failed"]
    summary = {
        "contract_version": FUNDING_CONTRACT_VERSION,
        "source": "bybit_v5_market_funding_history",
        "universe": (
            "explicit_symbols_with_historical_identity_fallback"
            if historical_path is not None else "current_standard_linear_usdt_perpetual"
        ),
        "historical_instruments": (
            {"path": str(historical_path), "sha256": _sha256(historical_path),
             "role": "historical_product_identity_only_not_current_trading_eligibility"}
            if historical_path is not None else None
        ),
        "selected_symbols": len(selected),
        "completed_symbols": len(selected) - len(failed),
        "failed_symbols": len(failed),
        "funding_rows": sum(item.rows for item in ordered),
        "quarantined_prefix_funding_events": sum(
            item.quarantined_prefix_events for item in ordered
        ),
        "requested_start_date": args.start_date,
        "snapshot_utc": started.isoformat(),
    }
    atomic_write_text(
        output_dir / "funding_summary.json",
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
    )
    progress.finish(failed=bool(failed), require_exact=True)
    print(json.dumps(summary, ensure_ascii=False))
    if failed:
        raise RuntimeError(f"funding history failed for {len(failed)} symbols")


if __name__ == "__main__":
    main()
