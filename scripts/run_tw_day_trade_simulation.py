#!/usr/bin/env python3
"""Consume Discord live signals and maintain one-minute paper ledgers."""

from __future__ import annotations

import argparse
from datetime import datetime, time as datetime_time
import json
from pathlib import Path
import sys
import time as time_module
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.tw_execution import TaiwanFeeSchedule  # noqa: E402
from stockagent.config import load_config  # noqa: E402
from stockagent.live.market_config import (  # noqa: E402
    LiveMarketConfig,
    load_market_configs,
    resolved_live_output_dir,
)
from stockagent.live.quote_provider import (  # noqa: E402
    fetch_futures_snapshot_prefer_stream,
    fetch_shioaji_stock_snapshots,
)
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    ModeSpec,
    STOCK_BENCHMARKS,
    TX_CONTINUOUS_LOGICAL_CODE,
    TwDayTradeSimulationEngine,
    load_live_eligibility,
    quote_map_from_snapshot,
    resolve_day_trade_rule_data_dir,
)


TAIPEI = ZoneInfo("Asia/Taipei")


def _repo_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else REPO_ROOT / path


def _fee_schedule(config: Any) -> TaiwanFeeSchedule:
    trading = config.trading
    return TaiwanFeeSchedule(
        commission_rate=float(trading.tw_commission_rate),
        commission_discount=float(trading.tw_commission_discount),
        commission_rebate_timing=str(trading.tw_commission_rebate_timing),
        stock_sell_tax=float(trading.tw_stock_sell_tax),
        etf_sell_tax=float(trading.tw_etf_sell_tax),
        day_trade_stock_sell_tax=float(trading.tw_day_trade_stock_sell_tax),
        day_trade_etf_sell_tax=float(trading.tw_day_trade_etf_sell_tax),
        minimum_commission=float(trading.tw_minimum_commission),
        commission_rounding=str(trading.tw_commission_rounding),
        tax_rounding=str(trading.tw_tax_rounding),
        settlement_lag_sessions=int(trading.tw_settlement_lag_sessions),
        cash_lot_size=int(trading.tw_cash_lot_size),
        day_trade_default_lot_size=int(trading.tw_day_trade_lot_size),
    )


def _mode_specs(
    markets_dir: Path,
) -> tuple[list[ModeSpec], dict[str, LiveMarketConfig], dict[str, str]]:
    configs = load_market_configs(markets_dir)
    specs: list[ModeSpec] = []
    selected: dict[str, LiveMarketConfig] = {}
    errors: dict[str, str] = {}
    for market, live in configs.items():
        if not live.enabled or not live.day_trade_simulation_enabled:
            continue
        selected[market] = live
        try:
            experiment = load_config(_repo_path(live.config_path))
            if str(experiment.trading.execution_mode) != "tw_day_trade":
                raise ValueError(
                    "simulation consumer requires execution_mode=tw_day_trade"
                )
            initial_capital = float(
                live.current_capital
                or live.initial_capital
                or experiment.trading.volume_participation_equity
            )
            checkpoint = (
                str(_repo_path(live.checkpoint_path)) if live.checkpoint_path else None
            )
            specs.append(
                ModeSpec(
                    market=market,
                    label=live.label,
                    initial_capital_twd=initial_capital,
                    config_path=str(_repo_path(live.config_path)),
                    checkpoint_path=checkpoint,
                    parquet_root=_repo_path(experiment.data.parquet_root),
                    live_output_dir=_repo_path(resolved_live_output_dir(live)),
                    fee_schedule=_fee_schedule(experiment),
                    lot_size=int(experiment.trading.tw_day_trade_lot_size),
                    price_limit_offset_ticks=1,
                )
            )
        except Exception as exc:
            errors[market] = f"{type(exc).__name__}: {exc}"
    return specs, selected, errors


def _latest_signal(
    spec: ModeSpec, now: datetime
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    candidates = list(
        spec.live_output_dir.glob(f"{now.date().isoformat()}*/**/summary.json")
    )
    ranked: list[tuple[datetime, Path, dict[str, Any]]] = []
    for path in candidates:
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
            if (
                not isinstance(summary, dict)
                or str(summary.get("market"))
                != str(spec.signal_market or spec.market)
            ):
                continue
            generated = datetime.fromisoformat(str(summary.get("generated_at")))
            if generated.tzinfo is None:
                generated = generated.replace(tzinfo=TAIPEI)
            ranked.append((generated.astimezone(TAIPEI), path, summary))
        except Exception:
            continue
    if not ranked:
        return None
    _generated, summary_path, summary = max(ranked, key=lambda item: item[0])
    weights_path = summary_path.with_name("target_weights.parquet")
    if not weights_path.is_file():
        return None
    rows = pl.read_parquet(weights_path).to_dicts()
    summary = dict(summary)
    summary["summary_path"] = str(summary_path)
    return summary, rows


def _fetch_quotes(
    *,
    symbols: list[str],
    fallback_by_symbol: dict[str, float],
    parquet_root: Path,
    trading_date: datetime,
) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    fallback = np.asarray(
        [float(fallback_by_symbol.get(symbol) or 1.0) for symbol in symbols],
        dtype=np.float64,
    )
    snapshot = fetch_shioaji_stock_snapshots(
        symbols,
        fallback,
        cache_ttl_seconds=0.0,
    )
    return quote_map_from_snapshot(
        symbols,
        snapshot,
        trading_date=trading_date.date(),
    )


def _rule_data_dir(live: LiveMarketConfig, spec: ModeSpec) -> Path:
    return resolve_day_trade_rule_data_dir(
        live.day_trade_rule_data_dir,
        parquet_root=spec.parquet_root,
        repo_root=REPO_ROOT,
    )


def _current_eligibility_coverage(
    specs: list[ModeSpec],
    live_configs: dict[str, LiveMarketConfig],
    *,
    trading_date: datetime,
) -> dict[str, dict[str, Any]]:
    cache: dict[tuple[str, str], dict[str, Any]] = {}
    by_market: dict[str, dict[str, Any]] = {}
    for spec in specs:
        live = live_configs[spec.market]
        rule_data_dir = _rule_data_dir(live, spec)
        key = (str(rule_data_dir.resolve()), str(spec.parquet_root.resolve()))
        if key not in cache:
            _resolved, cache[key] = load_live_eligibility(
                rule_data_dir=rule_data_dir,
                parquet_root=spec.parquet_root,
                symbols=(),
                trading_date=trading_date.date(),
            )
        by_market[spec.market] = cache[key]
    return by_market


def _active_symbols(
    engine: TwDayTradeSimulationEngine,
) -> tuple[list[str], dict[str, float]]:
    symbols: set[str] = set()
    fallback: dict[str, float] = {}
    for mode in engine.state.get("modes", {}).values():
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            symbol = str(position.get("symbol") or "")
            if not symbol:
                continue
            symbols.add(symbol)
            fallback[symbol] = float(
                position.get("last_mark_price") or position.get("entry_price") or 1.0
            )
    return sorted(symbols), fallback


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--markets-dir", type=Path, default=Path("services/discord_bot/markets")
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("artifacts/live/tw_day_trade_simulation"),
    )
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--rearm-flat-session",
        action="store_true",
        help="Rearm today's consumed signal only when that mode has no positions or fills.",
    )
    parser.add_argument(
        "--rearm-market",
        action="append",
        default=[],
        help="Market to rearm; repeat as needed. Defaults to every enabled paper mode.",
    )
    parser.add_argument(
        "--rearm-reason",
        default="manual_recovery_after_same_session_rule_refresh",
        help="Audit reason persisted with the rearm event.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if float(args.poll_seconds) <= 0.0:
        raise ValueError("poll-seconds must be positive")
    markets_dir = _repo_path(args.markets_dir)
    engine = TwDayTradeSimulationEngine(_repo_path(args.state_dir))
    if args.rearm_flat_session:
        specs, _live_configs, errors = _mode_specs(markets_dir)
        if errors:
            raise RuntimeError(f"cannot resolve paper modes for rearm: {errors}")
        enabled = {spec.market for spec in specs}
        selected = list(dict.fromkeys(args.rearm_market or sorted(enabled)))
        unknown = sorted(set(selected) - enabled)
        if unknown:
            raise ValueError(f"unknown or disabled rearm markets: {unknown}")
        observed = datetime.now(TAIPEI)
        results = {
            market: engine.rearm_flat_session(
                market,
                now=observed,
                reason=args.rearm_reason,
            )
            for market in selected
        }
        print(
            json.dumps(
                {
                    "simulation_only": True,
                    "production_order_possible": False,
                    "observed_at": observed.isoformat(timespec="seconds"),
                    "results": results,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )
        return 0
    specs: list[ModeSpec] = []
    live_configs: dict[str, LiveMarketConfig] = {}
    last_reload = 0.0
    last_readiness = 0.0
    last_quote_minute: str | None = None
    last_benchmark_minute: str | None = None
    pending_retry_minute: dict[str, str] = {}
    print(
        f"[tw-day-trade-sim] state_dir={engine.state_dir} simulation_only=true",
        flush=True,
    )

    while True:
        monotonic_now = time_module.monotonic()
        observed = datetime.now(TAIPEI)
        if monotonic_now - last_reload >= 30.0 or not specs:
            specs, live_configs, errors = _mode_specs(markets_dir)
            current_coverage = _current_eligibility_coverage(
                specs,
                live_configs,
                trading_date=observed,
            )
            engine.update_readiness(
                specs,
                now=observed,
                errors=errors,
                current_eligibility_coverage=current_coverage,
            )
            last_reload = monotonic_now
            last_readiness = monotonic_now
        elif monotonic_now - last_readiness >= 10.0:
            engine.update_readiness(specs, now=observed)
            last_readiness = monotonic_now

        pending: list[
            tuple[ModeSpec, LiveMarketConfig, dict[str, Any], list[dict[str, Any]]]
        ] = []
        pending_symbols: set[str] = set()
        pending_fallback: dict[str, float] = {}
        minute_key = observed.replace(second=0, microsecond=0).isoformat(
            timespec="minutes"
        )
        for spec in specs:
            latest = _latest_signal(spec, observed)
            if latest is None:
                continue
            summary, rows = latest
            mode = engine.state.get("modes", {}).get(spec.market, {})
            signal_id = str(summary.get("signal_id") or "")
            if signal_id in set(mode.get("processed_signal_ids") or ()):
                continue
            if pending_retry_minute.get(spec.market) == minute_key:
                continue
            pending.append((spec, live_configs[spec.market], summary, rows))
            for row in rows:
                if abs(float(row.get("target_weight") or 0.0)) <= 0.0:
                    continue
                symbol = str(row.get("symbol") or "")
                if not symbol:
                    continue
                pending_symbols.add(symbol)
                pending_fallback[symbol] = float(
                    row.get("current_price") or row.get("open_price") or 1.0
                )

        active_symbols, active_fallback = _active_symbols(engine)
        wall_time = observed.timetz().replace(tzinfo=None)
        quote_due = (
            bool(active_symbols)
            and datetime_time(9, 0) <= wall_time <= datetime_time(13, 30)
            and last_quote_minute != minute_key
        )
        benchmark_due = (
            observed.weekday() < 5
            and datetime_time(9, 0) <= wall_time < datetime_time(13, 30)
            and last_benchmark_minute != minute_key
        )
        benchmark_symbols = (
            {symbol for _benchmark_id, symbol, _label, _type in STOCK_BENCHMARKS}
            if benchmark_due
            else set()
        )
        all_symbols = sorted(
            pending_symbols
            | (set(active_symbols) if quote_due else set())
            | benchmark_symbols
        )
        quotes: dict[str, dict[str, Any]] = {}
        if all_symbols and specs:
            fallback = {
                **engine.benchmark_fallback_prices(),
                **active_fallback,
                **pending_fallback,
            }
            if quote_due:
                # One mark/exit decision per wall-clock minute, even when the
                # quote request fails.  A failed observation is processed as
                # missing evidence rather than retried every two seconds.
                last_quote_minute = minute_key
            try:
                quotes = _fetch_quotes(
                    symbols=all_symbols,
                    fallback_by_symbol=fallback,
                    parquet_root=specs[0].parquet_root,
                    trading_date=observed,
                )
                quotes = engine.prepare_minute_quotes(quotes, now=observed)
            except Exception as exc:
                print(
                    f"[tw-day-trade-sim] quote_error={type(exc).__name__}: {exc}",
                    flush=True,
                )

        future_snapshot: dict[str, Any] = {}
        if benchmark_due and specs:
            last_benchmark_minute = minute_key
            previous_contract = engine.benchmark_tx_contract()
            try:
                future_snapshot = fetch_futures_snapshot_prefer_stream(
                    TX_CONTINUOUS_LOGICAL_CODE,
                    additional_contract_codes=(previous_contract,)
                    if previous_contract
                    else (),
                    decision_time=observed,
                )
            except Exception as exc:
                print(
                    f"[tw-day-trade-sim] benchmark_future_quote_error="
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )

        for spec, live, summary, rows in pending:
            symbols = [str(row.get("symbol") or "") for row in rows]
            try:
                eligibility, coverage = load_live_eligibility(
                    rule_data_dir=_rule_data_dir(live, spec),
                    parquet_root=spec.parquet_root,
                    symbols=symbols,
                    trading_date=observed.date(),
                )
                result = engine.register_signal(
                    spec=spec,
                    summary=summary,
                    signal_rows=rows,
                    quotes=quotes,
                    eligibility=eligibility,
                    eligibility_coverage=coverage,
                    now=observed,
                )
                if result in {"waiting_quote", "waiting_first_minute"}:
                    pending_retry_minute[spec.market] = minute_key
                else:
                    pending_retry_minute.pop(spec.market, None)
                if result == "registered":
                    # register_signal already persisted this minute's complete
                    # liquidation mark from the causally later fill quote.
                    last_quote_minute = minute_key
                print(
                    f"[tw-day-trade-sim] market={spec.market} "
                    f"signal={summary.get('signal_id')} result={result}",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"[tw-day-trade-sim] market={spec.market} "
                    f"signal_error={type(exc).__name__}: {exc}",
                    flush=True,
                )

        if quote_due:
            engine.process_quotes(quotes=quotes, now=observed)
        if benchmark_due and specs:
            current_contract = str(future_snapshot.get("current_contract_code") or "")
            future_quotes = future_snapshot.get("quotes") or {}
            previous_contract = engine.benchmark_tx_contract()
            engine.process_benchmarks(
                stock_quotes=quotes,
                stock_fee_schedule=specs[0].fee_schedule,
                current_future_contract_code=current_contract or None,
                current_future_quote=future_quotes.get(current_contract) or {},
                previous_future_quote=future_quotes.get(previous_contract) or {},
                now=observed,
            )

        if args.once:
            return 0
        time_module.sleep(float(args.poll_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
