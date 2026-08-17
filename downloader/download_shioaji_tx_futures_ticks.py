#!/usr/bin/env python3
"""Resumably download all available ticks for one continuous futures alias."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import fcntl
import json
import os
from pathlib import Path
import sys
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from downloader.common import (
        SharedRateLimiter,
        describe_rate_limit,
        resolve_request_interval,
    )
except ModuleNotFoundError:  # direct script execution
    from common import SharedRateLimiter, describe_rate_limit, resolve_request_interval
from stockagent.live.shioaji_traffic_ledger import shioaji_query

from downloader.download_shioaji_tw_kbars import (
    TrafficBudgetReached,
    _atomic_write_json,
    _check_traffic_budget,
    _sha256,
    _taiwan_market_hours_now,
    _write_parquet_atomic,
)


SOURCE = "shioaji_continuous_futures_historical_ticks_v1"
LEGACY_TX_SOURCE = "shioaji_txfr1_historical_ticks_v1"
SCHEMA_VERSION = 1
HISTORY_START = date(2020, 3, 22)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", default="TXFR1")
    parser.add_argument(
        "--calendar-path",
        type=Path,
        default=Path("data_tw_index_futures/day_session_front_month.parquet"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_tw_index_futures/shioaji_history/TXFR1"),
    )
    parser.add_argument("--start-date", default=HISTORY_START.isoformat())
    parser.add_argument(
        "--end-date", default=(date.today() - timedelta(days=1)).isoformat()
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help=(
            "Host-global seconds between quote requests; defaults to the official "
            "selected 10 requests/second account-wide ceiling."
        ),
    )
    parser.add_argument("--timeout-ms", type=int, default=120_000)
    parser.add_argument("--max-traffic-fraction", type=float, default=0.75)
    parser.add_argument("--traffic-reserve-mb", type=float, default=512.0)
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument("--allow-market-hours", action="store_true")
    parser.add_argument("--max-dates", type=int, default=0)
    parser.add_argument("--oldest-first", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _calendar(path: Path, start: date, end: date) -> list[date]:
    frame = (
        pl.scan_parquet(path)
        .filter(
            (pl.col("product") == "TX")
            & (pl.col("date") >= pl.lit(start))
            & (pl.col("date") <= pl.lit(end))
        )
        .select("date")
        .unique()
        .sort("date")
        .collect()
    )
    return list(frame.get_column("date"))


def _receipt_path(root: Path, trading_date: date) -> Path:
    return root / "receipts" / f"trading_date={trading_date.isoformat()}.json"


def _data_path(root: Path, trading_date: date) -> Path:
    return root / "ticks" / f"trading_date={trading_date.isoformat()}" / "data.parquet"


def _valid_receipt(root: Path, trading_date: date) -> dict[str, Any] | None:
    path = _receipt_path(root, trading_date)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    allowed_sources = {SOURCE}
    if root.name == "TXFR1":
        allowed_sources.add(LEGACY_TX_SOURCE)
    if not isinstance(payload, dict) or not (
        payload.get("schema_version") == SCHEMA_VERSION
        and payload.get("source") in allowed_sources
        and payload.get("contract") == root.name
        and payload.get("trading_date") == trading_date.isoformat()
        and payload.get("status") in {"complete", "source_empty"}
    ):
        return None
    if payload.get("status") == "source_empty":
        return payload
    data_path = _data_path(root, trading_date)
    if not data_path.is_file() or _sha256(data_path) != payload.get("sha256"):
        return None
    return payload


def _ticks_frame(
    payload: Any, *, trading_date: date, contract_code: str
) -> tuple[pl.DataFrame, bool]:
    fields = (
        "ts",
        "close",
        "volume",
        "bid_price",
        "bid_volume",
        "ask_price",
        "ask_volume",
        "tick_type",
    )
    values = {field: list(getattr(payload, field)) for field in fields}
    lengths = {field: len(value) for field, value in values.items()}
    if len(set(lengths.values())) != 1:
        raise ValueError(f"inconsistent Shioaji tick field lengths: {lengths}")
    if not values["ts"]:
        return pl.DataFrame(), True
    frame = (
        pl.DataFrame(values)
        .with_row_index("source_row_index")
        .with_columns(
            pl.col("ts").cast(pl.Int64),
            pl.col("ts").cast(pl.Datetime("ns")).alias("event_ts"),
            pl.lit(trading_date).cast(pl.Date).alias("trading_date"),
            pl.lit(contract_code).alias("query_contract"),
        )
    )
    source_order_monotonic = bool(frame.get_column("ts").is_sorted())
    if not source_order_monotonic:
        frame = frame.sort(["ts", "source_row_index"], maintain_order=True)
    return frame, source_order_monotonic


def _write_manifest(
    root: Path,
    *,
    contract: str,
    expected: list[date],
    stopped_for_traffic: bool,
    stopped_for_market_hours: bool,
    usage: tuple[int, int] | None,
) -> dict[str, Any]:
    resolved: list[dict[str, Any]] = []
    for trading_date in expected:
        receipt = _valid_receipt(root, trading_date)
        if receipt is not None:
            resolved.append(receipt)
    resolved_dates = {str(item["trading_date"]) for item in resolved}
    available = [item for item in resolved if item.get("status") == "complete"]
    source_empty = [item for item in resolved if item.get("status") == "source_empty"]
    missing = [value.isoformat() for value in expected if value.isoformat() not in resolved_dates]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "dataset": SOURCE,
        "status": "complete" if not missing else "partial",
        "contract": contract,
        "history_start": expected[0].isoformat() if expected else None,
        "history_end": expected[-1].isoformat() if expected else None,
        "expected_trading_dates": len(expected),
        "resolved_trading_dates": len(resolved),
        "complete_trading_dates": len(available),
        "source_empty_trading_dates": len(source_empty),
        "missing_trading_dates": missing,
        "rows": sum(int(item.get("rows", 0)) for item in available),
        "bytes": sum(int(item.get("size", 0)) for item in available),
        "stopped_for_traffic": stopped_for_traffic,
        "stopped_for_market_hours": stopped_for_market_hours,
        "traffic_used_bytes": usage[0] if usage else None,
        "traffic_limit_bytes": usage[1] if usage else None,
        "timestamp_contract": (
            "ts decodes directly to Asia/Taipei market wall-clock time; trading_date D "
            "contains the prior trading day's night session through D day close"
        ),
        "quote_contract": (
            "bid/ask fields are the one-level values attached to each historical trade; "
            "they are not historical five-level order books"
        ),
    }
    _atomic_write_json(root / "manifest.json", manifest)
    return manifest


def main() -> int:
    args = parse_args()
    if args.request_interval is not None and float(args.request_interval) < 0.0:
        raise ValueError("--request-interval must be >= 0")
    request_interval = resolve_request_interval(
        "shioaji_quote_query", args.request_interval
    )
    rate_limiter = SharedRateLimiter(request_interval, name="shioaji_quote_query")
    print(
        "[shioaji-tx-history] "
        f"{describe_rate_limit('shioaji_quote_query', request_interval)}",
        flush=True,
    )
    start = max(date.fromisoformat(args.start_date), HISTORY_START)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("start date must not be after end date")
    if not 0.0 < args.max_traffic_fraction < 1.0:
        raise ValueError("max traffic fraction must be between zero and one")
    if not 0.0 <= args.traffic_reserve_mb < float("inf"):
        raise ValueError("traffic reserve must be finite and nonnegative")
    if args.timeout_ms < 1 or args.max_dates < 0:
        raise ValueError("timeout and max dates must be valid")
    if _taiwan_market_hours_now() and not args.allow_market_hours:
        print(
            "[shioaji-tx-history] status=stopped_for_market_hours "
            "window=07:45-14:31",
            flush=True,
        )
        return 76

    expected = _calendar(args.calendar_path, start, end)
    if not expected:
        raise RuntimeError("official TX calendar contains no selected trading dates")
    pending = [value for value in expected if _valid_receipt(args.output_dir, value) is None]
    pending.sort(reverse=not args.oldest_first)
    if args.max_dates:
        pending = pending[: args.max_dates]
    if args.dry_run:
        print(
            f"[shioaji-tx-history] expected={len(expected)} pending={len(pending)} "
            f"range={expected[0]}..{expected[-1]} output={args.output_dir}"
        )
        return 0

    args.output_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = (args.output_dir / "download.lock").open("a+b")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_handle.close()
        raise RuntimeError("another TX history downloader holds the lock") from exc

    import shioaji as sj

    api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required")
    api = sj.Shioaji(simulation=bool(args.simulation))
    stopped_for_traffic = False
    stopped_for_market_hours = False
    usage: tuple[int, int] | None = None
    try:
        api.set_event_callback(lambda *_args: None)
        api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)
        contract = api.contracts.get(str(args.contract))
        if contract is None:
            raise LookupError(f"future contract not found: {args.contract}")
        reserve_bytes = int(args.traffic_reserve_mb * 1024 * 1024)
        for index, trading_date in enumerate(pending, start=1):
            if _taiwan_market_hours_now() and not args.allow_market_hours:
                stopped_for_market_hours = True
                break
            try:
                usage = _check_traffic_budget(
                    api,
                    max_fraction=float(args.max_traffic_fraction),
                    reserve_bytes=reserve_bytes,
                )
            except TrafficBudgetReached:
                stopped_for_traffic = True
                break
            with shioaji_query(
                api,
                consumer="futures_history_backfill",
                method="ticks",
                asset_class="futures",
                details={
                    "contract": str(args.contract),
                    "date": trading_date.isoformat(),
                },
            ) as set_ledger_result:
                rate_limiter.wait()
                ticks = api.ticks(
                    contract=contract,
                    date=trading_date.isoformat(),
                    timeout=int(args.timeout_ms),
                )
                set_ledger_result(ticks)
            frame, source_order_monotonic = _ticks_frame(
                ticks, trading_date=trading_date, contract_code=str(args.contract)
            )
            if frame.is_empty():
                current_usage = api.usage()
                usage = int(current_usage.bytes), int(current_usage.limit_bytes)
                try:
                    _check_traffic_budget(
                        api,
                        max_fraction=float(args.max_traffic_fraction),
                        reserve_bytes=reserve_bytes,
                    )
                except TrafficBudgetReached:
                    stopped_for_traffic = True
                    break
                _atomic_write_json(
                    _receipt_path(args.output_dir, trading_date),
                    {
                        "schema_version": SCHEMA_VERSION,
                        "source": SOURCE,
                        "status": "source_empty",
                        "contract": str(args.contract),
                        "trading_date": trading_date.isoformat(),
                        "rows": 0,
                        "traffic_used_bytes_after_query": usage[0],
                    },
                )
                print(
                    f"[shioaji-futures-history] {index}/{len(pending)} "
                    f"contract={args.contract} date={trading_date} status=source_empty",
                    flush=True,
                )
                continue
            output = _write_parquet_atomic(frame, _data_path(args.output_dir, trading_date))
            receipt = {
                "schema_version": SCHEMA_VERSION,
                "source": SOURCE,
                "status": "complete",
                "contract": str(args.contract),
                "resolved_target_code_at_query": getattr(contract, "target_code", None),
                "trading_date": trading_date.isoformat(),
                "rows": frame.height,
                "source_order_monotonic": source_order_monotonic,
                **output,
            }
            _atomic_write_json(_receipt_path(args.output_dir, trading_date), receipt)
            print(
                f"[shioaji-futures-history] {index}/{len(pending)} "
                f"contract={args.contract} date={trading_date} "
                f"rows={frame.height:,} bytes={int(output['size']):,} "
                f"traffic={usage[0]:,}/{usage[1]:,}",
                flush=True,
            )
        if usage is None:
            current = api.usage()
            usage = int(current.bytes), int(current.limit_bytes)
    finally:
        try:
            api.logout()
        finally:
            lock_handle.close()
    manifest = _write_manifest(
        args.output_dir,
        contract=str(args.contract),
        expected=expected,
        stopped_for_traffic=stopped_for_traffic,
        stopped_for_market_hours=stopped_for_market_hours,
        usage=usage,
    )
    print(
        "[shioaji-futures-history] "
        f"contract={args.contract} status={manifest['status']} "
        f"resolved={manifest['resolved_trading_dates']}"
        f"/{manifest['expected_trading_dates']} rows={manifest['rows']:,} "
        f"bytes={manifest['bytes']:,} stopped_for_traffic={stopped_for_traffic} "
        f"stopped_for_market_hours={stopped_for_market_hours}",
        flush=True,
    )
    if manifest["status"] == "complete":
        return 0
    return 76 if stopped_for_market_hours else 75


if __name__ == "__main__":
    raise SystemExit(main())
