#!/usr/bin/env python3
"""Capture TX front-month and a bounded near-ATM TXO Tick/BidAsk strip."""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, time as datetime_time, timezone
import itertools
import math
import os
from pathlib import Path
import re
import signal
import threading
import time
from typing import Any, Iterable

from downloader.stream_shioaji_tw_microstructure import (
    EventSink,
    TAIPEI,
    _atomic_json,
    normalize_fop_book,
    normalize_fop_tick,
)


SOURCE_NAME = "shioaji_taifex_tick_bidask_v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
    )
    parser.add_argument("--future-code", default="TXFR1")
    parser.add_argument("--option-root", default="TXO")
    parser.add_argument("--option-expiries", type=int, default=2)
    parser.add_argument("--strikes-per-expiry", type=int, default=16)
    parser.add_argument("--underlying-reference", type=float, default=None)
    parser.add_argument("--stop-time", default="13:45:05")
    parser.add_argument("--queue-size", type=int, default=250_000)
    parser.add_argument("--flush-rows", type=int, default=50_000)
    parser.add_argument("--flush-seconds", type=float, default=10.0)
    parser.add_argument("--stale-ms", type=float, default=2_000.0)
    parser.add_argument("--subscribe-interval", type=float, default=0.04)
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument("--capture-id", default="")
    return parser.parse_args()


def _scalar(value: Any) -> Any:
    return getattr(value, "value", value)


def _option_right(value: Any) -> str:
    normalized = str(_scalar(value)).strip().lower()
    if normalized in {"c", "call", "optionright.call"}:
        return "C"
    if normalized in {"p", "put", "optionright.put"}:
        return "P"
    raise ValueError(f"unsupported option right: {value!r}")


def _base(info: Any) -> Any:
    base = getattr(info, "base", None)
    if base is None:
        raise ValueError(f"contract info has no Base contract: {info!r}")
    return base


def select_option_strip(
    option_infos: Iterable[Any],
    *,
    trade_date: date,
    underlying_reference: float,
    expiry_count: int,
    strikes_per_expiry: int,
) -> list[Any]:
    """Select paired Call/Put contracts deterministically within the quote cap."""

    if not math.isfinite(underlying_reference) or underlying_reference <= 0.0:
        raise ValueError("underlying_reference must be finite and positive")
    if expiry_count < 1 or strikes_per_expiry < 1:
        raise ValueError("expiry and strike counts must be positive")
    grouped: dict[date, dict[float, dict[str, Any]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for info in option_infos:
        delivery_date = getattr(info, "delivery_date", None)
        last_trading_date = getattr(info, "last_trading_date", delivery_date)
        if not isinstance(delivery_date, date) or delivery_date < trade_date:
            continue
        if isinstance(last_trading_date, date) and last_trading_date < trade_date:
            continue
        try:
            right = _option_right(getattr(info, "option_right", None))
            strike = float(getattr(info, "strike_price"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(strike) and strike > 0.0:
            grouped[delivery_date][strike][right] = info

    selected: list[Any] = []
    paired_expiries = [
        expiry
        for expiry in sorted(grouped)
        if any({"C", "P"} <= set(rights) for rights in grouped[expiry].values())
    ]
    for expiry in paired_expiries[:expiry_count]:
        strikes = [
            strike
            for strike, rights in grouped[expiry].items()
            if {"C", "P"} <= set(rights)
        ]
        strikes.sort(key=lambda strike: (abs(strike - underlying_reference), strike))
        for strike in sorted(strikes[:strikes_per_expiry]):
            selected.extend(
                [grouped[expiry][strike]["C"], grouped[expiry][strike]["P"]]
            )
    if not selected:
        raise RuntimeError("no unexpired paired TXO contracts were available")
    return selected


def _metadata(info: Any, *, logical_code: str | None = None) -> dict[str, Any]:
    base = _base(info)
    security_type = str(_scalar(getattr(base, "security_type", "")))
    row: dict[str, Any] = {
        "security_type": security_type,
        "exchange": str(_scalar(getattr(base, "exchange", "TAIFEX"))),
        "code": str(getattr(base, "code")),
        "target_code": getattr(base, "target_code", None),
        "logical_code": logical_code,
        "root": getattr(info, "root", None),
        "delivery_month": getattr(info, "delivery_month", None),
        "delivery_date": (
            getattr(info, "delivery_date").isoformat()
            if isinstance(getattr(info, "delivery_date", None), date)
            else None
        ),
        "last_trading_date": (
            getattr(info, "last_trading_date").isoformat()
            if isinstance(getattr(info, "last_trading_date", None), date)
            else None
        ),
        "multiplier": float(getattr(info, "multiplier", 0.0) or 0.0),
        "reference": float(getattr(info, "reference", 0.0) or 0.0),
    }
    if hasattr(info, "option_right"):
        row["strike_price"] = float(getattr(info, "strike_price"))
        row["option_right"] = _option_right(getattr(info, "option_right"))
    return row


def _stop_datetime(value: str) -> datetime:
    parsed = datetime_time.fromisoformat(value)
    now = datetime.now(TAIPEI)
    return datetime.combine(now.date(), parsed, tzinfo=TAIPEI)


def main() -> int:
    args = parse_args()
    capture_id = str(args.capture_id).strip() or f"single-{time.time_ns()}"
    if re.fullmatch(r"[A-Za-z0-9_.-]+", capture_id) is None:
        raise ValueError("capture-id contains unsupported characters")
    api_key = os.environ.get("SHIOAJI_API_KEY", "").strip()
    secret_key = os.environ.get("SHIOAJI_SECRET_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY are required")

    import shioaji as sj

    started_at = datetime.now(timezone.utc)
    stop_at = _stop_datetime(args.stop_time)
    if stop_at <= datetime.now(TAIPEI):
        raise RuntimeError(f"stop time has already passed for today: {stop_at}")
    shutdown = threading.Event()
    event_sequence = itertools.count(1)
    sink = EventSink(
        args.output_dir,
        worker_index=0,
        capture_id=capture_id,
        queue_size=int(args.queue_size),
        flush_rows=int(args.flush_rows),
        flush_seconds=float(args.flush_seconds),
        stale_ms=float(args.stale_ms),
    )

    def request_shutdown(_signum: int, _frame: Any) -> None:
        shutdown.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    api = sj.Shioaji(simulation=bool(args.simulation))
    api.set_event_callback(lambda *_args: None)
    api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)

    logical_future = api.contracts.get(str(args.future_code))
    if logical_future is None:
        api.logout()
        raise LookupError(f"future contract not found: {args.future_code}")
    target_code = getattr(logical_future, "target_code", None)
    future_base = api.contracts.get(str(target_code)) if target_code else logical_future
    if future_base is None:
        api.logout()
        raise LookupError(f"resolved future contract not found: {target_code}")
    future_info = api.contracts.info(future_base)
    if future_info is None:
        api.logout()
        raise LookupError(f"future info not found: {getattr(future_base, 'code', '')}")
    reference = (
        float(args.underlying_reference)
        if args.underlying_reference is not None
        else float(getattr(future_info, "reference", 0.0) or 0.0)
    )
    option_infos = api.contracts.options(str(args.option_root))
    selected_option_infos = select_option_strip(
        option_infos,
        trade_date=datetime.now(TAIPEI).date(),
        underlying_reference=reference,
        expiry_count=int(args.option_expiries),
        strikes_per_expiry=int(args.strikes_per_expiry),
    )
    selected_infos = [future_info, *selected_option_infos]
    contracts = [_base(info) for info in selected_infos]
    subscriptions_requested = len(contracts) * 2
    if subscriptions_requested > 200:
        api.logout()
        raise ValueError(
            f"selected {len(contracts)} contracts / {subscriptions_requested} "
            "subscriptions, exceeding the 200-subscription guard"
        )
    contract_metadata = [
        _metadata(
            info,
            logical_code=(str(args.future_code) if index == 0 else None),
        )
        for index, info in enumerate(selected_infos)
    ]

    @api.on_tick_fop_v1()
    def on_tick(tick: Any) -> None:
        receive_ts_ns = time.time_ns()
        receive_monotonic_ns = time.monotonic_ns()
        try:
            sink.enqueue(
                "tick",
                normalize_fop_tick(
                    tick,
                    event_seq=next(event_sequence),
                    worker_index=0,
                    receive_ts_ns=receive_ts_ns,
                    receive_monotonic_ns=receive_monotonic_ns,
                ),
            )
        except Exception:
            sink.fatal_event.set()

    @api.on_bidask_fop_v1()
    def on_book(book: Any) -> None:
        receive_ts_ns = time.time_ns()
        receive_monotonic_ns = time.monotonic_ns()
        try:
            sink.enqueue(
                "book",
                normalize_fop_book(
                    book,
                    event_seq=next(event_sequence),
                    worker_index=0,
                    receive_ts_ns=receive_ts_ns,
                    receive_monotonic_ns=receive_monotonic_ns,
                ),
            )
        except Exception:
            sink.fatal_event.set()

    sink.start()
    subscribed = 0
    status = "running"
    try:
        for contract in contracts:
            api.subscribe(
                contract,
                quote_type=sj.QuoteType.Tick,
                version=sj.QuoteVersion.v1,
            )
            subscribed += 1
            if args.subscribe_interval:
                time.sleep(float(args.subscribe_interval))
            api.subscribe(
                contract,
                quote_type=sj.QuoteType.BidAsk,
                version=sj.QuoteVersion.v1,
            )
            subscribed += 1
            if args.subscribe_interval:
                time.sleep(float(args.subscribe_interval))
        print(
            f"[shioaji-taifex] contracts={len(contracts)} "
            f"subscriptions={subscribed} reference={reference} stop_at={stop_at}",
            flush=True,
        )
        while datetime.now(TAIPEI) < stop_at and not shutdown.wait(0.5):
            if sink.fatal_event.is_set():
                status = "failed_event_loss_or_normalization"
                break
        if shutdown.is_set():
            status = "stopped_by_signal"
        elif status == "running":
            status = "complete"
        if status == "complete" and sink.stats.book_events == 0:
            status = "failed_no_bidask_events"
    finally:
        sink.stop()
        try:
            api.logout()
        except Exception:
            if status == "complete":
                status = "failed_logout"
        finished_at = datetime.now(timezone.utc)
        stats = sink.stats
        manifest = {
            "schema_version": 3,
            "source": SOURCE_NAME,
            "status": status,
            "capture_id": capture_id,
            "simulation": bool(args.simulation),
            "worker_index": 0,
            "workers": 1,
            "trade_date": datetime.now(TAIPEI).date().isoformat(),
            "selection": {
                "future_code": str(args.future_code),
                "resolved_future_code": str(getattr(future_base, "code")),
                "option_root": str(args.option_root),
                "option_expiries": int(args.option_expiries),
                "strikes_per_expiry": int(args.strikes_per_expiry),
                "underlying_reference": reference,
            },
            "contract_metadata": contract_metadata,
            "contract_count": len(contracts),
            "subscriptions_requested": subscribed,
            "tick_events": stats.tick_events,
            "book_events": stats.book_events,
            "book_1s_rows": stats.book_1s_rows,
            "dropped_events": stats.dropped_events,
            "queue_high_watermark": stats.queue_high_watermark,
            "missed_snapshot_seconds": stats.missed_snapshot_seconds,
            "tick_rows_written": sink.tick_writer.total_rows,
            "book_rows_written": sink.book_writer.total_rows,
            "book_1s_rows_written": sink.snapshot_writer.total_rows,
            "tick_parts": sink.tick_writer.total_parts,
            "book_parts": sink.book_writer.total_parts,
            "book_1s_parts": sink.snapshot_writer.total_parts,
            "started_at_utc": started_at.replace(microsecond=0).isoformat(),
            "finished_at_utc": finished_at.replace(microsecond=0).isoformat(),
        }
        manifest_path = (
            args.output_dir
            / "manifests"
            / f"trade_date={datetime.now(TAIPEI).date()}"
            / "worker=00.json"
        )
        _atomic_json(manifest_path, manifest)
        print(
            f"[shioaji-taifex] status={status} ticks={stats.tick_events} "
            f"books={stats.book_events} dropped={stats.dropped_events} "
            f"manifest={manifest_path}",
            flush=True,
        )
    return 0 if status == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
