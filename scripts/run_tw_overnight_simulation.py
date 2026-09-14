#!/usr/bin/env python3
"""Consume 13:20 model signals and maintain close-to-next-open paper ledgers."""

from __future__ import annotations

import argparse
from datetime import datetime, time as datetime_time
import json
import math
from pathlib import Path
import sys
import time
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_tw_day_trade_simulation import (  # noqa: E402
    _SignalPointerWatcher,
    _acquire_engine_lock,
    _fee_schedule,
    _latest_signal,
    _repo_path,
)
from stockagent.config import load_config  # noqa: E402
from stockagent.live.market_config import (  # noqa: E402
    LiveMarketConfig,
    load_market_configs,
    resolved_live_output_dir,
)
from stockagent.live.market_status import verified_tw_stock_session_day  # noqa: E402
from stockagent.live.quote_provider import (  # noqa: E402
    fetch_shared_day_trade_stock_snapshots,
)
from stockagent.live.service_notify import notify_systemd  # noqa: E402
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    ModeSpec,
    load_symbol_metadata,
    quote_map_from_snapshot,
    resolve_day_trade_rule_data_dir,
)
from stockagent.live.tw_overnight_simulation import (  # noqa: E402
    CLOSE_ORDER_GATE,
    OPEN_ORDER_GATE,
    OVERNIGHT_SWITCH_GATE,
    REGULAR_CLOSE,
    TwOvernightSimulationEngine,
)


TAIPEI = ZoneInfo("Asia/Taipei")
DAY_TRADE_QUOTE_BROKER_STATE = Path("artifacts/live/tw_day_trade_simulation")


def _service_status_text(
    engine: TwOvernightSimulationEngine,
    observed: datetime,
) -> str:
    wall = observed.time()
    if wall < OVERNIGHT_SWITCH_GATE:
        phase = "waiting for 13:00 switch"
    elif wall < CLOSE_ORDER_GATE:
        phase = "armed; waiting for 13:20 calculation"
    elif wall < REGULAR_CLOSE:
        phase = "13:20 calculation/order window"
    else:
        phase = "post-close audit"
    states = sorted(
        {
            str(mode.get("engine_status") or "unknown")
            for mode in (engine.state.get("modes") or {}).values()
        }
    )
    state_text = ",".join(states) if states else "no-modes"
    return f"TW overnight paper executor ready; {phase}; modes={state_text}"


def _mode_specs(
    markets_dir: Path,
) -> tuple[list[ModeSpec], dict[str, LiveMarketConfig], dict[str, str]]:
    specs: list[ModeSpec] = []
    selected: dict[str, LiveMarketConfig] = {}
    errors: dict[str, str] = {}
    for market, live in load_market_configs(markets_dir).items():
        if not live.enabled or not live.overnight_simulation_enabled:
            continue
        selected[market] = live
        try:
            experiment = load_config(_repo_path(live.config_path))
            if str(experiment.trading.execution_mode) != "tw_day_trade":
                raise ValueError(
                    "temporary overnight adapter requires execution_mode=tw_day_trade"
                )
            initial_capital = float(
                live.current_capital
                or live.initial_capital
                or experiment.trading.volume_participation_equity
            )
            specs.append(
                ModeSpec(
                    market=market,
                    label=live.label,
                    initial_capital_twd=initial_capital,
                    config_path=str(_repo_path(live.config_path)),
                    checkpoint_path=(
                        str(_repo_path(live.checkpoint_path))
                        if live.checkpoint_path
                        else None
                    ),
                    parquet_root=_repo_path(experiment.data.parquet_root),
                    live_output_dir=_repo_path(resolved_live_output_dir(live)),
                    fee_schedule=_fee_schedule(experiment),
                    lot_size=int(experiment.trading.tw_day_trade_lot_size),
                    signal_market=market,
                )
            )
        except Exception as exc:
            errors[market] = f"{type(exc).__name__}: {exc}"
    return specs, selected, errors


def _session_open(
    spec: ModeSpec,
    live: LiveMarketConfig,
    observed: datetime,
) -> tuple[bool, str]:
    root = resolve_day_trade_rule_data_dir(
        live.day_trade_rule_data_dir,
        parquet_root=spec.parquet_root,
        repo_root=REPO_ROOT,
    )
    return verified_tw_stock_session_day(
        observed.date(),
        tuple(live.holidays or ()),
        parquet_root=root,
    )


def _row_symbols(
    rows: list[dict[str, Any]], spec: ModeSpec
) -> tuple[set[str], dict[str, float]]:
    symbols: set[str] = set()
    fallback: dict[str, float] = {}
    for row in rows:
        try:
            weight = float(row.get("target_weight") or 0.0)
        except (TypeError, ValueError):
            continue
        if weight == 0.0:
            continue
        symbol = str(row.get("symbol") or "").strip()
        price = float(row.get("current_price") or row.get("open_price") or 0.0)
        if not symbol or not math.isfinite(price) or price <= 0.0:
            continue
        if abs(weight) * float(spec.initial_capital_twd) / price < spec.lot_size:
            continue
        symbols.add(symbol)
        fallback[symbol] = price
    return symbols, fallback


def _ledger_symbols(
    engine: TwOvernightSimulationEngine,
) -> tuple[set[str], dict[str, float]]:
    symbols: set[str] = set()
    fallback: dict[str, float] = {}
    for mode in (engine.state.get("modes") or {}).values():
        for order in (mode.get("pending_entry_orders") or {}).values():
            if order.get("status") != "working":
                continue
            symbol = str(order.get("symbol") or "")
            if symbol:
                symbols.add(symbol)
                fallback[symbol] = float(
                    order.get("sizing_price_at_decision")
                    or order.get("sizing_price_at_13_25")
                    or 1.0
                )
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            symbol = str(position.get("symbol") or "")
            if symbol:
                symbols.add(symbol)
                fallback[symbol] = float(
                    position.get("last_mark_price")
                    or position.get("entry_price")
                    or 1.0
                )
    return symbols, fallback


def _fetch_quotes(
    symbols: set[str],
    fallback: dict[str, float],
    *,
    observed: datetime,
    broker_state_dir: Path,
) -> dict[str, dict[str, Any]]:
    ordered = sorted(symbols)
    if not ordered:
        return {}
    snapshot = fetch_shared_day_trade_stock_snapshots(
        ordered,
        np.asarray([fallback.get(symbol, 1.0) for symbol in ordered], dtype=np.float64),
        state_dir=broker_state_dir,
        timeout_seconds=8.0,
        purpose=(
            "opening_signal"
            if datetime_time(8, 30) <= observed.time() < datetime_time(9, 10)
            else "latest_quote"
        ),
    )
    return quote_map_from_snapshot(
        ordered,
        snapshot,
        trading_date=observed.date(),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--markets-dir", type=Path, default=Path("services/discord_bot/markets")
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("artifacts/live/tw_overnight_simulation"),
    )
    parser.add_argument(
        "--quote-broker-state-dir",
        type=Path,
        default=DAY_TRADE_QUOTE_BROKER_STATE,
    )
    parser.add_argument("--poll-seconds", type=float, default=0.1)
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.poll_seconds <= 0:
        raise ValueError("poll-seconds must be positive")
    state_dir = _repo_path(args.state_dir)
    broker_state_dir = _repo_path(args.quote_broker_state_dir)
    engine_lock = _acquire_engine_lock(state_dir)
    # Keep the descriptor referenced for the process lifetime; dropping the
    # final reference would release flock immediately under CPython.
    _ = engine_lock
    engine = TwOvernightSimulationEngine(state_dir)
    watcher = _SignalPointerWatcher()
    specs: list[ModeSpec] = []
    configs: dict[str, LiveMarketConfig] = {}
    last_reload = 0.0
    last_readiness = 0.0
    last_quote_minute = ""
    last_mark_minute = ""
    ready_notified = False
    print(
        f"[tw-overnight-sim] state_dir={state_dir} simulation_only=true",
        flush=True,
    )
    while True:
        notify_systemd("WATCHDOG=1")
        observed = datetime.now(TAIPEI)
        monotonic = time.monotonic()
        if monotonic - last_reload >= 30.0 or not specs:
            specs, configs, errors = _mode_specs(_repo_path(args.markets_dir))
            watcher.configure([spec.live_output_dir for spec in specs])
            engine.update_readiness(specs, now=observed, errors=errors)
            last_reload = monotonic
            last_readiness = monotonic
            if not ready_notified:
                notify_systemd(
                    f"READY=1\nSTATUS={_service_status_text(engine, observed)}"
                )
                ready_notified = True
            else:
                notify_systemd(f"STATUS={_service_status_text(engine, observed)}")
        elif monotonic - last_readiness >= 10.0:
            engine.update_readiness(specs, now=observed)
            notify_systemd(f"STATUS={_service_status_text(engine, observed)}")
            last_readiness = monotonic

        wall = observed.time()
        pending_signals: list[
            tuple[ModeSpec, dict[str, Any], list[dict[str, Any]], datetime]
        ] = []
        signal_symbols: set[str] = set()
        fallback: dict[str, float] = {}
        if CLOSE_ORDER_GATE <= wall < REGULAR_CLOSE:
            for spec in specs:
                is_open, _reason = _session_open(spec, configs[spec.market], observed)
                if not is_open:
                    continue
                latest = _latest_signal(spec, observed)
                if latest is None:
                    continue
                summary, rows = latest
                mode = (engine.state.get("modes") or {}).get(spec.market) or {}
                if str(summary.get("signal_id") or "") in set(
                    mode.get("processed_signal_ids") or ()
                ):
                    continue
                pending_signals.append(
                    (spec, summary, rows, datetime.now(TAIPEI))
                )
                symbols, prices = _row_symbols(rows, spec)
                signal_symbols.update(symbols)
                fallback.update(prices)

        ledger_symbols, ledger_fallback = _ledger_symbols(engine)
        minute_key = observed.strftime("%Y-%m-%dT%H:%M")
        close_hot = REGULAR_CLOSE <= wall <= datetime_time(13, 35)
        open_hot = OPEN_ORDER_GATE <= wall <= datetime_time(9, 10)
        opening_reconciliation = OPEN_ORDER_GATE <= wall < CLOSE_ORDER_GATE
        ledger_due = bool(ledger_symbols) and (
            close_hot
            or open_hot
            or (opening_reconciliation and last_quote_minute != minute_key)
        )
        all_symbols = signal_symbols | (ledger_symbols if ledger_due else set())
        fallback.update(ledger_fallback)
        quotes: dict[str, dict[str, Any]] = {}
        quote_fetch_ms = 0.0
        if all_symbols:
            quote_started = time.perf_counter()
            try:
                quotes = _fetch_quotes(
                    all_symbols,
                    fallback,
                    observed=observed,
                    broker_state_dir=broker_state_dir,
                )
                last_quote_minute = minute_key
            except Exception as exc:
                print(
                    f"[tw-overnight-sim] quote_error={type(exc).__name__}: {exc}",
                    flush=True,
                )
            finally:
                quote_fetch_ms = (time.perf_counter() - quote_started) * 1_000.0

        metadata_by_root: dict[Path, dict[str, dict[str, str]]] = {}
        for spec, summary, rows, detected_at in pending_signals:
            metadata_started = time.perf_counter()
            metadata = metadata_by_root.get(spec.parquet_root)
            if metadata is None:
                metadata = load_symbol_metadata(spec.parquet_root)
                metadata_by_root[spec.parquet_root] = metadata
            metadata_ms = (time.perf_counter() - metadata_started) * 1_000.0
            security_types = {
                symbol: str(row.get("security_type") or "stock")
                for symbol, row in metadata.items()
            }
            try:
                ledger_started = time.perf_counter()
                result = engine.register_close_signal(
                    spec=spec,
                    summary=summary,
                    signal_rows=rows,
                    quotes=quotes,
                    security_types=security_types,
                    now=datetime.now(TAIPEI),
                )
                ledger_ms = (time.perf_counter() - ledger_started) * 1_000.0
                persisted_at = datetime.now(TAIPEI)
                engine.record_close_signal_latency_sample(
                    market=spec.market,
                    signal_id=str(summary.get("signal_id") or ""),
                    result=result,
                    summary=summary,
                    consumer_detected_at=detected_at,
                    ledger_persisted_at=persisted_at,
                    executor_quote_fetch_ms=quote_fetch_ms,
                    security_metadata_load_ms=metadata_ms,
                    ledger_compute_persist_ms=ledger_ms,
                    shared_quote_batch_mode_count=len(pending_signals),
                )
                print(
                    f"[tw-overnight-sim] market={spec.market} "
                    f"signal={summary.get('signal_id')} result={result}",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"[tw-overnight-sim] market={spec.market} "
                    f"signal_error={type(exc).__name__}: {exc}",
                    flush=True,
                )

        if ledger_due or pending_signals:
            append_mark = bool(quotes) and last_mark_minute != minute_key
            engine.process_quotes(
                quotes=quotes,
                now=datetime.now(TAIPEI),
                append_mark_history=append_mark,
            )
            if append_mark:
                last_mark_minute = minute_key
        if args.once:
            return 0
        fast = bool(pending_signals) or close_hot or (bool(ledger_symbols) and open_hot)
        watcher.wait(args.poll_seconds if fast else max(1.0, args.poll_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
