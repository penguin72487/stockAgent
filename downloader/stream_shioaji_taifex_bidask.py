#!/usr/bin/env python3
"""Capture TX front-month and nearest monthly/weekly TXO Tick/BidAsk strips."""

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
DEFAULT_OPTION_ROOTS = "TXO,TX1,TX2,TX4,TX5,TXU,TXV,TXX,TXY,TXZ"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
    )
    parser.add_argument("--future-code", default="TXFR1")
    parser.add_argument("--hedge-future-code", default="MXFR1")
    parser.add_argument("--option-roots", default=DEFAULT_OPTION_ROOTS)
    parser.add_argument(
        "--option-expiries",
        type=int,
        default=1,
        help="Number of nearest monthly TXO expiries to keep.",
    )
    parser.add_argument(
        "--weekly-expiries",
        type=int,
        default=1,
        help="Number of nearest weekly TXO expiries across all weekly roots to keep.",
    )
    parser.add_argument("--strikes-per-expiry", type=int, default=100)
    parser.add_argument("--underlying-reference", type=float, default=None)
    parser.add_argument("--stop-time", default="13:45:05")
    parser.add_argument("--queue-size", type=int, default=250_000)
    parser.add_argument("--flush-rows", type=int, default=50_000)
    parser.add_argument("--flush-seconds", type=float, default=10.0)
    parser.add_argument("--stale-ms", type=float, default=2_000.0)
    parser.add_argument("--subscribe-interval", type=float, default=0.04)
    parser.add_argument("--simulation", action="store_true")
    parser.add_argument(
        "--execute-strategies",
        action="store_true",
        help=(
            "run the seven-strategy Shioaji simulation engine on worker 0; "
            "requires --simulation"
        ),
    )
    parser.add_argument(
        "--strategy-state-dir",
        type=Path,
        default=Path("artifacts/live/shioaji_taifex_volatility_simulation"),
    )
    parser.add_argument(
        "--final-settlement-path",
        type=Path,
        default=Path(
            "data_tw_index_options_daily/txo_final_settlement_history.parquet"
        ),
    )
    parser.add_argument("--strategy-calibration-time", default="13:29:00")
    parser.add_argument("--strategy-bootstrap-after", default="")
    parser.add_argument("--capture-id", default="")
    parser.add_argument("--worker-index", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
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


def select_balanced_option_strips(
    option_infos: Iterable[Any],
    *,
    trade_date: date,
    underlying_reference: float,
    monthly_expiry_count: int,
    weekly_expiry_count: int,
    strikes_per_expiry: int,
    max_pairs: int,
) -> list[Any]:
    """Round-robin paired strikes across only the nearest monthly/weeklies."""

    if not math.isfinite(underlying_reference) or underlying_reference <= 0.0:
        raise ValueError("underlying_reference must be finite and positive")
    if (
        monthly_expiry_count < 1
        or weekly_expiry_count < 1
        or strikes_per_expiry < 1
        or max_pairs < 1
    ):
        raise ValueError("expiry, strike, and pair limits must be positive")
    grouped: dict[tuple[str, date], dict[float, dict[str, Any]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    for info in option_infos:
        root = str(getattr(info, "root", "")).strip().upper()
        delivery_date = getattr(info, "delivery_date", None)
        last_trading_date = getattr(info, "last_trading_date", delivery_date)
        if (
            not root
            or not isinstance(delivery_date, date)
            or delivery_date < trade_date
        ):
            continue
        if isinstance(last_trading_date, date) and last_trading_date < trade_date:
            continue
        try:
            right = _option_right(getattr(info, "option_right", None))
            strike = float(getattr(info, "strike_price"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(strike) and strike > 0.0:
            grouped[(root, delivery_date)][strike][right] = info

    paired_keys = {
        key
        for key, strikes in grouped.items()
        if any({"C", "P"} <= set(rights) for rights in strikes.values())
    }
    monthly_keys = sorted(key for key in paired_keys if key[0] == "TXO")[
        :monthly_expiry_count
    ]
    weekly_keys = sorted(
        (key for key in paired_keys if key[0] != "TXO"),
        key=lambda item: (item[1], item[0]),
    )[:weekly_expiry_count]
    if not monthly_keys:
        raise RuntimeError("no unexpired paired monthly TXO series were available")
    if not weekly_keys:
        raise RuntimeError("no unexpired paired weekly TXO series were available")

    series: list[tuple[tuple[str, date], list[float]]] = []
    for key in sorted(
        [*monthly_keys, *weekly_keys], key=lambda item: (item[1], item[0])
    ):
        strikes = [
            strike
            for strike, rights in grouped[key].items()
            if {"C", "P"} <= set(rights)
        ]
        strikes.sort(key=lambda strike: (abs(strike - underlying_reference), strike))
        if strikes:
            series.append((key, strikes[:strikes_per_expiry]))
    if not series:
        raise RuntimeError("no unexpired paired TXO contracts were available")
    if not any(key[0] != "TXO" for key, _strikes in series):
        raise RuntimeError("no unexpired paired weekly TXO contracts were available")

    selected_pairs: list[tuple[tuple[str, date], float]] = []
    for offset in range(strikes_per_expiry):
        for key, strikes in series:
            if offset < len(strikes):
                selected_pairs.append((key, strikes[offset]))
                if len(selected_pairs) >= max_pairs:
                    break
        if len(selected_pairs) >= max_pairs:
            break
    selected: list[Any] = []
    for key, strike in selected_pairs:
        selected.extend([grouped[key][strike]["C"], grouped[key][strike]["P"]])
    return selected


def partition_contract_infos(
    *,
    futures_infos: list[Any],
    option_infos: list[Any],
    worker_index: int,
    workers: int,
    contracts_per_worker: int = 100,
) -> list[Any]:
    """Keep Call/Put pairs together and reserve worker 0 for strategy data."""

    if workers < 1 or not 0 <= worker_index < workers:
        raise ValueError("worker index is outside the configured worker count")
    if contracts_per_worker < 2 or len(futures_infos) > contracts_per_worker:
        raise ValueError("invalid per-worker contract capacity")
    if len(option_infos) % 2:
        raise ValueError("option selection must contain complete Call/Put pairs")
    pairs = [
        option_infos[index : index + 2] for index in range(0, len(option_infos), 2)
    ]
    pair_capacities = [
        (contracts_per_worker - len(futures_infos)) // 2,
        *([contracts_per_worker // 2] * (workers - 1)),
    ]
    if len(pairs) > sum(pair_capacities):
        raise ValueError("selected option pairs exceed worker capacity")
    offset = 0
    selected_pairs: list[list[Any]] = []
    for capacity in pair_capacities:
        selected_pairs.append(pairs[offset : offset + capacity])
        offset += capacity
    selected = [item for pair in selected_pairs[worker_index] for item in pair]
    if worker_index == 0:
        selected = [*futures_infos, *selected]
    if len(selected) > contracts_per_worker:
        raise AssertionError("worker contract partition exceeds its hard cap")
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
    if args.workers < 1 or not 0 <= args.worker_index < args.workers:
        raise ValueError("worker-index must satisfy 0 <= worker-index < workers")
    if args.execute_strategies and not args.simulation:
        raise RuntimeError("strategy execution requires the literal --simulation mode")
    if args.execute_strategies and int(args.worker_index) != 0:
        raise RuntimeError("strategy execution is owned only by worker 0")
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
        worker_index=int(args.worker_index),
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
    api.login(
        api_key=api_key,
        secret_key=secret_key,
        subscribe_trade=bool(args.execute_strategies),
    )

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
    logical_hedge = api.contracts.get(str(args.hedge_future_code))
    if logical_hedge is None:
        api.logout()
        raise LookupError(f"hedge future contract not found: {args.hedge_future_code}")
    hedge_target_code = getattr(logical_hedge, "target_code", None)
    hedge_base = (
        api.contracts.get(str(hedge_target_code))
        if hedge_target_code
        else logical_hedge
    )
    if hedge_base is None:
        api.logout()
        raise LookupError(f"resolved hedge future not found: {hedge_target_code}")
    hedge_info = api.contracts.info(hedge_base)
    if hedge_info is None:
        api.logout()
        raise LookupError(
            f"hedge future info not found: {getattr(hedge_base, 'code', '')}"
        )
    option_roots = tuple(
        dict.fromkeys(
            root.strip().upper()
            for root in str(args.option_roots).split(",")
            if root.strip()
        )
    )
    if "TXO" not in option_roots:
        api.logout()
        raise ValueError("option-roots must include monthly root TXO")
    option_infos: list[Any] = []
    unavailable_roots: dict[str, str] = {}
    for root in option_roots:
        try:
            option_infos.extend(api.contracts.options(root))
        except Exception as exc:
            unavailable_roots[root] = f"{type(exc).__name__}: {exc}"
    futures_infos = [future_info, hedge_info]
    max_contracts = int(args.workers) * 100
    selected_option_infos = select_balanced_option_strips(
        option_infos,
        trade_date=datetime.now(TAIPEI).date(),
        underlying_reference=reference,
        monthly_expiry_count=int(args.option_expiries),
        weekly_expiry_count=int(args.weekly_expiries),
        strikes_per_expiry=int(args.strikes_per_expiry),
        max_pairs=(max_contracts - len(futures_infos)) // 2,
    )
    all_selected_infos = [*futures_infos, *selected_option_infos]
    selected_infos = partition_contract_infos(
        futures_infos=futures_infos,
        option_infos=selected_option_infos,
        worker_index=int(args.worker_index),
        workers=int(args.workers),
    )
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
            logical_code=(
                str(args.future_code)
                if info is future_info
                else str(args.hedge_future_code)
                if info is hedge_info
                else None
            ),
        )
        for info in selected_infos
    ]

    strategy_engine = None
    if args.execute_strategies:
        from stockagent.live.taifex_volatility_simulation import (
            FuturesInstrument,
            TaifexVolatilitySimulation,
        )

        bootstrap_after = (
            date.fromisoformat(str(args.strategy_bootstrap_after))
            if str(args.strategy_bootstrap_after).strip()
            else None
        )
        calibration_time = datetime_time.fromisoformat(
            str(args.strategy_calibration_time)
        )
        strategy_engine = TaifexVolatilitySimulation(
            api=api,
            shioaji_module=sj,
            state_dir=Path(args.strategy_state_dir),
            option_infos=selected_option_infos[: (100 - len(futures_infos))],
            underlying=FuturesInstrument(
                logical_code=str(args.future_code),
                code=str(getattr(future_base, "code")),
                contract=future_base,
                last_trading_date=getattr(future_info, "last_trading_date", None),
            ),
            hedge=FuturesInstrument(
                logical_code=str(args.hedge_future_code),
                code=str(getattr(hedge_base, "code")),
                contract=hedge_base,
                last_trading_date=getattr(hedge_info, "last_trading_date", None),
            ),
            final_settlement_path=Path(args.final_settlement_path),
            calibration_time=calibration_time,
            bootstrap_after=bootstrap_after,
            broker_orders_enabled=True,
        )
        api.set_order_callback(strategy_engine.on_order_event)

    @api.on_tick_fop_v1()
    def on_tick(tick: Any) -> None:
        receive_ts_ns = time.time_ns()
        receive_monotonic_ns = time.monotonic_ns()
        try:
            row = normalize_fop_tick(
                tick,
                event_seq=next(event_sequence),
                worker_index=int(args.worker_index),
                receive_ts_ns=receive_ts_ns,
                receive_monotonic_ns=receive_monotonic_ns,
            )
            sink.enqueue("tick", row)
            if strategy_engine is not None:
                strategy_engine.on_tick(row)
        except Exception:
            sink.fatal_event.set()

    @api.on_bidask_fop_v1()
    def on_book(book: Any) -> None:
        receive_ts_ns = time.time_ns()
        receive_monotonic_ns = time.monotonic_ns()
        try:
            row = normalize_fop_book(
                book,
                event_seq=next(event_sequence),
                worker_index=int(args.worker_index),
                receive_ts_ns=receive_ts_ns,
                receive_monotonic_ns=receive_monotonic_ns,
            )
            sink.enqueue("book", row)
            if strategy_engine is not None:
                strategy_engine.on_book(row)
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
            f"[shioaji-taifex] worker={args.worker_index}/{args.workers} "
            f"contracts={len(contracts)} "
            f"subscriptions={subscribed} reference={reference} stop_at={stop_at}",
            flush=True,
        )
        while datetime.now(TAIPEI) < stop_at and not shutdown.wait(0.5):
            if sink.fatal_event.is_set():
                status = "failed_event_loss_or_normalization"
                break
            if strategy_engine is not None:
                strategy_engine.step()
        if shutdown.is_set():
            status = "stopped_by_signal"
        elif status == "running":
            status = "complete"
        if status == "complete" and sink.stats.book_events == 0:
            status = "failed_no_bidask_events"
    finally:
        sink.stop()
        if strategy_engine is not None:
            strategy_engine.close()
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
            "worker_index": int(args.worker_index),
            "workers": int(args.workers),
            "trade_date": datetime.now(TAIPEI).date().isoformat(),
            "selection": {
                "future_code": str(args.future_code),
                "resolved_future_code": str(getattr(future_base, "code")),
                "hedge_future_code": str(args.hedge_future_code),
                "resolved_hedge_future_code": str(getattr(hedge_base, "code")),
                "option_roots": list(option_roots),
                "unavailable_option_roots": unavailable_roots,
                "selection_policy": "nearest_monthly_and_nearest_weekly_atm_outward",
                "monthly_option_expiries": int(args.option_expiries),
                "weekly_option_expiries": int(args.weekly_expiries),
                "strikes_per_expiry": int(args.strikes_per_expiry),
                "underlying_reference": reference,
                "selected_contracts_all_workers": len(all_selected_infos),
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
            "strategy_simulation": {
                "enabled": bool(args.execute_strategies),
                "simulation_only": True,
                "state_dir": str(args.strategy_state_dir),
                "status_path": str(Path(args.strategy_state_dir) / "status.json"),
            },
        }
        manifest_path = (
            args.output_dir
            / "manifests"
            / f"trade_date={datetime.now(TAIPEI).date()}"
            / f"worker={int(args.worker_index):02d}.json"
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
