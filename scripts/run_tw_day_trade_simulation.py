#!/usr/bin/env python3
"""Consume Discord live signals and maintain one-minute paper ledgers."""

from __future__ import annotations

import argparse
import ctypes
from datetime import datetime, time as datetime_time, timedelta
import fcntl
import json
import os
from pathlib import Path
import select
import sys
import time as time_module
from typing import Any
import uuid
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
from stockagent.live.market_status import verified_tw_stock_session_day  # noqa: E402
from stockagent.live.quote_provider import (  # noqa: E402
    fetch_futures_snapshot_prefer_stream,
    fetch_shioaji_stock_snapshots,
    prepare_tw_price_limit_snapshot,
    warm_shioaji_stock_quote_client,
)
from stockagent.live.service_notify import notify_systemd  # noqa: E402
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    ENTRY_FILL_POLICY_CAUSAL_BOOK,
    LIVE_ENTRY_GATE,
    CLOSING_AUCTION_TIME,
    FORCE_EXIT_TIME,
    ModeSpec,
    STOCK_BENCHMARKS,
    TX_CONTINUOUS_LOGICAL_CODE,
    TwDayTradeSimulationEngine,
    load_live_eligibility,
    load_symbol_metadata,
    quote_map_from_snapshot,
    resolve_day_trade_rule_data_dir,
)


TAIPEI = ZoneInfo("Asia/Taipei")
_LATEST_SIGNAL_CACHE: dict[
    str, tuple[tuple[int, int], dict[str, Any], list[dict[str, Any]]]
] = {}
_ELIGIBILITY_CACHE: dict[
    tuple[str, str, str], tuple[set[str], dict[str, Any], dict[str, Any]]
] = {}
_LEGACY_SIGNAL_SCAN_CACHE: dict[
    str, tuple[float, tuple[dict[str, Any], list[dict[str, Any]]] | None]
] = {}
_BENCHMARK_PREVIOUS_CLOSE_CACHE: dict[tuple[str, str, str], dict[str, Any]] = {}


def _opening_batch_max_wait_seconds() -> float:
    return max(
        0.0,
        float(os.getenv("STOCKAGENT_OPENING_SIGNAL_BATCH_WAIT_SECONDS", "2.0") or 2.0),
    )


def _opening_batch_cutoff_seconds() -> float:
    # Preserve three seconds of the 09:00:15 acceptance window for one causal
    # quote request plus the durable per-mode ledger commits.
    return min(
        12.0,
        max(
            0.0,
            float(
                os.getenv("STOCKAGENT_OPENING_SIGNAL_BATCH_CUTOFF_SECONDS", "12.0")
                or 12.0
            ),
        ),
    )


def _pending_signal_retry_delay_seconds(result: str, observed: datetime) -> float:
    if result != "waiting_first_minute":
        return 0.5
    next_minute = observed.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return max(0.5, (next_minute - observed).total_seconds() + 0.05)


def _repo_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else REPO_ROOT / path


def _acquire_engine_lock(state_dir: Path):
    """Keep exactly one writer for the in-memory simulation state."""

    root = Path(state_dir)
    root.mkdir(parents=True, exist_ok=True)
    handle = (root / ".engine.lock").open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError(
            "simulation state already has a live writer; stop the running "
            "executor before using --rearm-flat-session"
        ) from None
    return handle


def _write_preopen_readiness(
    state_dir: Path,
    *,
    session_date: str,
    component: str,
    status: str,
    observed: datetime,
    elapsed_ms: float | None = None,
    details: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    """Atomically receipt the executor's independent pre-open dependencies."""

    path = Path(state_dir) / "preopen_readiness.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        payload = {}
    if not isinstance(payload, dict) or payload.get("session_date") != session_date:
        payload = {
            "schema_version": 1,
            "session_date": session_date,
            "simulation_only": True,
            "production_order_possible": False,
            "components": {},
        }
    components = payload.get("components")
    if not isinstance(components, dict):
        components = {}
    components[component] = {
        "status": str(status),
        "checked_at": observed.isoformat(timespec="milliseconds"),
        "elapsed_ms": (
            round(max(0.0, float(elapsed_ms)), 3) if elapsed_ms is not None else None
        ),
        "details": dict(details or {}),
        "error": error,
    }
    payload["components"] = components
    component_statuses = {
        str((components.get(name) or {}).get("status") or "pending")
        for name in ("eligibility", "shioaji_quote")
    }
    payload["status"] = (
        "failed"
        if "failed" in component_statuses
        else "ready"
        if component_statuses == {"ready"}
        else "warming"
    )
    payload["updated_at"] = observed.isoformat(timespec="milliseconds")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return payload


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
    *,
    include_disabled: bool = False,
) -> tuple[list[ModeSpec], dict[str, LiveMarketConfig], dict[str, str]]:
    configs = load_market_configs(markets_dir)
    specs: list[ModeSpec] = []
    selected: dict[str, LiveMarketConfig] = {}
    errors: dict[str, str] = {}
    for market, live in configs.items():
        if (
            not live.enabled and not include_disabled
        ) or not live.day_trade_simulation_enabled:
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
                    # Live execution is causal: after the 09:00 signal is
                    # atomically published, buy/cover uses the first later best
                    # Ask and sell/short uses the first later best Bid. Replay
                    # tools explicitly replace this with the 09:01 official-open
                    # counterfactual policy.
                    entry_fill_policy=ENTRY_FILL_POLICY_CAUSAL_BOOK,
                    entry_price_offset_ticks=0,
                )
            )
        except Exception as exc:
            errors[market] = f"{type(exc).__name__}: {exc}"
    return specs, selected, errors


def _latest_signal(
    spec: ModeSpec, now: datetime
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    pointer_path = spec.live_output_dir / "latest_signal.json"
    try:
        pointer_stat = pointer_path.stat()
        signature = (pointer_stat.st_mtime_ns, pointer_stat.st_size)
        cache_key = str(pointer_path.resolve())
        cached = _LATEST_SIGNAL_CACHE.get(cache_key)
        if cached and cached[0] == signature:
            summary, rows = cached[1], cached[2]
            if str(summary.get("asof_date") or "")[:10] == now.date().isoformat():
                return dict(summary), list(rows)
            return None
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        summary_path = Path(str(pointer.get("summary_path") or ""))
        weights_path = Path(str(pointer.get("weights_path") or ""))
        execution_weights_path = Path(str(pointer.get("execution_weights_path") or ""))
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if (
            isinstance(summary, dict)
            and str(summary.get("market")) == str(spec.signal_market or spec.market)
            and str(summary.get("asof_date") or "")[:10] == now.date().isoformat()
            and (execution_weights_path.is_file() or weights_path.is_file())
        ):
            rows: list[dict[str, Any]] | None = None
            if execution_weights_path.is_file():
                try:
                    execution_payload = json.loads(
                        execution_weights_path.read_text(encoding="utf-8")
                    )
                    execution_rows = execution_payload.get("rows")
                    if (
                        int(execution_payload.get("schema_version") or 0) == 1
                        and str(execution_payload.get("market") or "")
                        == str(spec.signal_market or spec.market)
                        and str(execution_payload.get("signal_id") or "")
                        == str(summary.get("signal_id") or "")
                        and isinstance(execution_rows, list)
                        and all(isinstance(row, dict) for row in execution_rows)
                    ):
                        rows = [dict(row) for row in execution_rows]
                except (OSError, TypeError, ValueError, json.JSONDecodeError):
                    rows = None
            if rows is None:
                rows = pl.read_parquet(weights_path).to_dicts()
            summary = dict(summary)
            summary["summary_path"] = str(summary_path)
            summary["weights_path"] = str(weights_path)
            summary["execution_weights_path"] = (
                str(execution_weights_path)
                if execution_weights_path.is_file()
                else None
            )
            _LATEST_SIGNAL_CACHE[cache_key] = (signature, summary, rows)
            return dict(summary), list(rows)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        pass

    # Compatibility path for artifacts written before latest_signal.json.
    # Current writers avoid this recursive scan on the opening hot path, and
    # old writers are sampled at most once per five seconds.
    legacy_cache_key = f"{spec.live_output_dir.resolve()}:{now.date().isoformat()}"
    cached_legacy = _LEGACY_SIGNAL_SCAN_CACHE.get(legacy_cache_key)
    monotonic_now = time_module.monotonic()
    if cached_legacy and monotonic_now < cached_legacy[0]:
        cached_result = cached_legacy[1]
        return (
            (dict(cached_result[0]), list(cached_result[1]))
            if cached_result is not None
            else None
        )
    candidates = list(
        spec.live_output_dir.glob(f"{now.date().isoformat()}*/**/summary.json")
    )
    ranked: list[tuple[datetime, Path, dict[str, Any]]] = []
    for path in candidates:
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(summary, dict) or str(summary.get("market")) != str(
                spec.signal_market or spec.market
            ):
                continue
            generated = datetime.fromisoformat(str(summary.get("generated_at")))
            if generated.tzinfo is None:
                generated = generated.replace(tzinfo=TAIPEI)
            ranked.append((generated.astimezone(TAIPEI), path, summary))
        except Exception:
            continue
    if not ranked:
        _LEGACY_SIGNAL_SCAN_CACHE[legacy_cache_key] = (monotonic_now + 5.0, None)
        return None
    _generated, summary_path, summary = max(ranked, key=lambda item: item[0])
    weights_path = summary_path.with_name("target_weights.parquet")
    if not weights_path.is_file():
        _LEGACY_SIGNAL_SCAN_CACHE[legacy_cache_key] = (monotonic_now + 5.0, None)
        return None
    rows = pl.read_parquet(weights_path).to_dicts()
    summary = dict(summary)
    summary["summary_path"] = str(summary_path)
    result = (summary, rows)
    _LEGACY_SIGNAL_SCAN_CACHE[legacy_cache_key] = (
        monotonic_now + 5.0,
        result,
    )
    return dict(summary), list(rows)


def _entry_candidate_symbols(
    *,
    spec: ModeSpec,
    rows: list[dict[str, Any]],
    eligibility: dict[str, Any],
) -> tuple[set[str], dict[str, float]]:
    """Quote only eligible names capable of producing a whole-lot order."""

    symbols: set[str] = set()
    fallback: dict[str, float] = {}
    for row in rows:
        weight = float(row.get("target_weight") or 0.0)
        if weight == 0.0 or not bool(row.get("tradable")):
            continue
        if weight > 0.0 and not bool(row.get("can_buy")):
            continue
        if weight < 0.0 and not bool(row.get("can_sell")):
            continue
        symbol = str(row.get("symbol") or "")
        evidence = eligibility.get(symbol)
        if (
            not symbol
            or evidence is None
            or not evidence.covered
            or not evidence.eligible
            or (weight < 0.0 and not evidence.short_open)
        ):
            continue
        sizing_price = float(row.get("open_price") or row.get("current_price") or 0.0)
        if np.isfinite(sizing_price) and sizing_price > 0.0:
            requested = int(
                abs(weight) * float(spec.initial_capital_twd) / sizing_price
            )
            if requested < int(spec.lot_size):
                continue
            fallback[symbol] = sizing_price
        else:
            fallback[symbol] = 1.0
        symbols.add(symbol)
    return symbols, fallback


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
    upper = (
        np.asarray(snapshot.upper_limit_prices, dtype=np.float64)
        if snapshot.upper_limit_prices is not None
        else np.full((len(symbols),), np.nan, dtype=np.float64)
    )
    lower = (
        np.asarray(snapshot.lower_limit_prices, dtype=np.float64)
        if snapshot.lower_limit_prices is not None
        else np.full((len(symbols),), np.nan, dtype=np.float64)
    )
    missing_limits = ~np.isfinite(upper) | ~np.isfinite(lower)
    if np.any(missing_limits):
        # Static daily limits are normally prepared before 09:00.  If an
        # unrelated pre-open refresh fails first, do not let otherwise valid
        # Shioaji Bid/Ask quotes collapse every paper order to flat.  Recover
        # the missing official reference/limits from TWSE/TPEx MIS once, then
        # reuse the process-cached Shioaji observations so the fallback never
        # substitutes MIS prices for the execution book.
        prepare_tw_price_limit_snapshot(
            symbols,
            fallback,
            parquet_root=parquet_root,
            trading_date=trading_date.date().isoformat(),
        )
        snapshot = fetch_shioaji_stock_snapshots(
            symbols,
            fallback,
            cache_ttl_seconds=60.0,
        )
    return quote_map_from_snapshot(
        symbols,
        snapshot,
        trading_date=trading_date.date(),
    )


def _attach_benchmark_previous_close_context(
    quotes: dict[str, dict[str, Any]],
    *,
    symbols: set[str],
    parquet_root: Path,
    trading_date: datetime,
) -> None:
    """Attach the last completed official close needed for live total return.

    The corporate-action archive is intentionally complete only through the
    preceding session before the open.  Pairing that boundary with today's
    official Shioaji/MIS reference price proves today's adjustment factor
    without waiting for an after-close archive rebuild or issuing another API
    request.
    """

    day = trading_date.date()
    root = Path(parquet_root).resolve()
    for symbol in sorted(symbols):
        quote = quotes.get(symbol)
        if not isinstance(quote, dict):
            continue
        key = (str(root), day.isoformat(), symbol)
        context = _BENCHMARK_PREVIOUS_CLOSE_CACHE.get(key)
        if context is None:
            path = root / f"{symbol}_features.parquet"
            if not path.is_file():
                continue
            try:
                lazy = pl.scan_parquet(path)
                schema = lazy.collect_schema()
                expressions: list[pl.Expr] = [
                    pl.col("date").cast(pl.Date),
                    pl.col("close").cast(pl.Float64),
                ]
                if "data_source" in schema.names():
                    expressions.append(pl.col("data_source").cast(pl.String))
                frame = (
                    lazy.filter(pl.col("date").cast(pl.Date) < day)
                    .select(expressions)
                    .sort("date")
                    .tail(1)
                    .collect()
                )
                if frame.height != 1:
                    continue
                source_row = frame.row(0, named=True)
                previous_close = float(source_row["close"])
                previous_date = source_row["date"]
                source = str(source_row.get("data_source") or "")
                if (
                    not np.isfinite(previous_close)
                    or previous_close <= 0.0
                    or not source.endswith("_official")
                ):
                    continue
                context = {
                    "previous_close": previous_close,
                    "previous_close_date": previous_date.isoformat(),
                    "previous_close_source": f"{source}:{path}",
                }
                _BENCHMARK_PREVIOUS_CLOSE_CACHE[key] = context
            except (OSError, TypeError, ValueError):
                continue
        quote.update(context)
        quote["reference_price_source"] = str(
            quote.get("source") or "official_shioaji_or_mis_reference"
        )


def _rule_data_dir(live: LiveMarketConfig, spec: ModeSpec) -> Path:
    return resolve_day_trade_rule_data_dir(
        live.day_trade_rule_data_dir,
        parquet_root=spec.parquet_root,
        repo_root=REPO_ROOT,
    )


def _cached_live_eligibility(
    *,
    rule_data_dir: Path,
    parquet_root: Path,
    symbols: list[str],
    trading_date: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    requested = {symbol for symbol in symbols if symbol}
    key = (
        str(rule_data_dir.resolve()),
        str(parquet_root.resolve()),
        trading_date.date().isoformat(),
    )
    cached = _ELIGIBILITY_CACHE.get(key)
    if cached and requested.issubset(cached[0]):
        return ({symbol: cached[1][symbol] for symbol in requested}, dict(cached[2]))
    union = requested | (cached[0] if cached else set())
    resolved, coverage = load_live_eligibility(
        rule_data_dir=rule_data_dir,
        parquet_root=parquet_root,
        symbols=sorted(union),
        trading_date=trading_date.date(),
    )
    # Missing same-day files can legitimately appear during pre-open recovery;
    # do not freeze a fail-closed miss in the process cache.
    if all(bool(row.get("covered")) for row in coverage.values()):
        _ELIGIBILITY_CACHE[key] = (union, resolved, coverage)
    return ({symbol: resolved[symbol] for symbol in requested}, coverage)


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


def _prewarm_live_eligibility(
    specs: list[ModeSpec],
    live_configs: dict[str, LiveMarketConfig],
    *,
    trading_date: datetime,
) -> dict[str, dict[str, Any]]:
    """Resolve the complete checkpoint universe before the opening event."""

    warmed: dict[str, dict[str, Any]] = {}
    grouped: dict[tuple[str, str], list[ModeSpec]] = {}
    for spec in specs:
        live = live_configs[spec.market]
        rule_data_dir = _rule_data_dir(live, spec)
        key = (str(rule_data_dir.resolve()), str(spec.parquet_root.resolve()))
        grouped.setdefault(key, []).append(spec)

    for (rule_dir_text, parquet_root_text), grouped_specs in grouped.items():
        rule_data_dir = Path(rule_dir_text)
        parquet_root = Path(parquet_root_text)
        symbols = sorted(load_symbol_metadata(parquet_root))
        started = time_module.perf_counter()
        resolved, coverage = _cached_live_eligibility(
            rule_data_dir=rule_data_dir,
            parquet_root=parquet_root,
            symbols=symbols,
            trading_date=trading_date,
        )
        missing = {
            venue: row
            for venue, row in coverage.items()
            if not bool(row.get("covered"))
        }
        if missing:
            raise RuntimeError(f"same-session eligibility not covered: {missing}")
        elapsed_ms = (time_module.perf_counter() - started) * 1000.0
        row = {
            "symbol_count": len(symbols),
            "resolved_count": len(resolved),
            "elapsed_ms": round(elapsed_ms, 3),
            "coverage": coverage,
        }
        for spec in grouped_specs:
            warmed[spec.market] = row
    return warmed


def _eligible_execution_quote_symbols(
    specs: list[ModeSpec],
    live_configs: dict[str, LiveMarketConfig],
    *,
    trading_date: datetime,
) -> list[str]:
    """Resolve every symbol that can legally become an opening candidate.

    Contract V2 lookup is deterministic process-local work.  Moving it to the
    08:55 prewarm leaves only the actual post-signal Snapshot request on the
    causal 09:00 path.
    """

    symbols: set[str] = set()
    grouped: set[tuple[str, str]] = set()
    for spec in specs:
        live = live_configs[spec.market]
        rule_data_dir = _rule_data_dir(live, spec)
        key = (str(rule_data_dir.resolve()), str(spec.parquet_root.resolve()))
        if key in grouped:
            continue
        grouped.add(key)
        universe = sorted(load_symbol_metadata(spec.parquet_root))
        resolved, coverage = _cached_live_eligibility(
            rule_data_dir=rule_data_dir,
            parquet_root=spec.parquet_root,
            symbols=universe,
            trading_date=trading_date,
        )
        if not all(bool(row.get("covered")) for row in coverage.values()):
            raise RuntimeError(
                "cannot prewarm quotes without exact-session eligibility"
            )
        symbols.update(
            symbol
            for symbol, evidence in resolved.items()
            if bool(evidence.covered and evidence.eligible)
        )
    return sorted(symbols)


def _verified_stock_session(
    specs: list[ModeSpec],
    live_configs: dict[str, LiveMarketConfig],
    *,
    observed: datetime,
) -> tuple[bool, dict[str, str]]:
    """Fail closed before Shioaji access when any paper mode lacks a session."""

    decisions: dict[str, str] = {}
    for spec in specs:
        live = live_configs[spec.market]
        is_open, reason = verified_tw_stock_session_day(
            observed.date(),
            tuple(live.holidays or ()),
            parquet_root=_rule_data_dir(live, spec),
        )
        if not is_open:
            decisions[spec.market] = reason
    return bool(specs) and not decisions, decisions


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


def _active_quote_due(
    active_symbols: list[str],
    *,
    observed: datetime,
    last_quote_minute: str | None,
) -> bool:
    """Keep polling an open ledger through terminal flatten catch-up."""

    minute_key = observed.replace(second=0, microsecond=0).isoformat(timespec="minutes")
    wall_time = observed.timetz().replace(tzinfo=None)
    force_exit_retry = FORCE_EXIT_TIME <= wall_time < CLOSING_AUCTION_TIME
    return (
        bool(active_symbols)
        and wall_time >= datetime_time(9, 0)
        and (force_exit_retry or last_quote_minute != minute_key)
    )


def _loop_sleep_seconds(
    observed: datetime,
    *,
    fast_seconds: float,
    has_pending_signal: bool,
    has_open_position: bool,
) -> float:
    """Use 10 Hz only around latency-critical state transitions."""

    wall_time = observed.timetz().replace(tzinfo=None)
    opening_hot_path = datetime_time(8, 59, 30) <= wall_time < datetime_time(9, 10)
    exit_hot_path = datetime_time(13, 19, 30) <= wall_time < CLOSING_AUCTION_TIME
    force_exit_hot_path = (
        has_open_position and FORCE_EXIT_TIME <= wall_time < CLOSING_AUCTION_TIME
    )
    if has_pending_signal or opening_hot_path or exit_hot_path or force_exit_hot_path:
        return max(0.01, float(fast_seconds))
    return max(1.0, float(fast_seconds))


class _SignalPointerWatcher:
    """Wake immediately when an atomic latest_signal.json pointer is published.

    Linux inotify removes the 0-100 ms polling quantization from the opening
    path. The ordinary timeout remains a portable fail-safe and continues to
    drive quote/exit/readiness work when no signal file changes.
    """

    _WATCH_MASK = 0x00000008 | 0x00000080 | 0x00000100 | 0x00004000

    def __init__(self) -> None:
        self._fd: int | None = None
        self._watched_paths: set[str] = set()
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            init = libc.inotify_init1
            init.argtypes = [ctypes.c_int]
            init.restype = ctypes.c_int
            fd = int(init(os.O_NONBLOCK | os.O_CLOEXEC))
            if fd < 0:
                raise OSError(ctypes.get_errno(), "inotify_init1 failed")
            add_watch = libc.inotify_add_watch
            add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
            add_watch.restype = ctypes.c_int
            self._libc = libc
            self._add_watch = add_watch
            self._fd = fd
        except (AttributeError, OSError):
            self._libc = None
            self._add_watch = None

    @property
    def enabled(self) -> bool:
        return self._fd is not None and bool(self._watched_paths)

    def configure(self, directories: list[Path]) -> None:
        if self._fd is None or self._add_watch is None:
            return
        for directory in directories:
            try:
                resolved = str(Path(directory).resolve())
                if resolved in self._watched_paths or not Path(resolved).is_dir():
                    continue
                descriptor = int(
                    self._add_watch(
                        self._fd,
                        os.fsencode(resolved),
                        self._WATCH_MASK,
                    )
                )
                if descriptor < 0:
                    continue
                self._watched_paths.add(resolved)
            except OSError:
                continue

    def wait(self, timeout_seconds: float) -> bool:
        timeout = max(0.0, float(timeout_seconds))
        if not self.enabled or self._fd is None:
            time_module.sleep(timeout)
            return False
        try:
            ready, _writable, _errors = select.select([self._fd], [], [], timeout)
            if not ready:
                return False
            while True:
                try:
                    if not os.read(self._fd, 64 * 1024):
                        break
                except BlockingIOError:
                    break
            return True
        except (OSError, ValueError):
            time_module.sleep(timeout)
            return False

    def close(self) -> None:
        if self._fd is None:
            return
        try:
            os.close(self._fd)
        finally:
            self._fd = None
            self._watched_paths.clear()

    def __del__(self) -> None:
        self.close()


def _mode_needs_opening_signal(
    engine: TwDayTradeSimulationEngine,
    spec: ModeSpec,
    *,
    session_date: str,
) -> bool:
    mode = engine.state.get("modes", {}).get(spec.market, {})
    if str(mode.get("engine_status") or "") == "critical_ledger_state_divergence":
        return False
    return not (
        str(mode.get("session_date") or "") == session_date
        and bool(mode.get("entry_completed_at"))
    )


def _collect_opening_signal_batch(
    specs: list[ModeSpec],
    engine: TwDayTradeSimulationEngine,
    observed: datetime,
    *,
    signal_watcher: _SignalPointerWatcher,
    max_wait_seconds: float,
    cutoff_seconds: float,
    poll_seconds: float = 0.02,
    now_fn: Any | None = None,
    monotonic_fn: Any | None = None,
) -> tuple[
    dict[
        str,
        tuple[dict[str, Any], list[dict[str, Any]], datetime],
    ],
    dict[str, Any],
]:
    """Collect due opening signals before requesting one causal quote batch.

    Waiting begins only after the first atomic signal pointer exists.  It ends
    immediately when every uncommitted mode is visible and is capped before
    the 09:00:15 commit SLO.  If one model fails, already-ready modes continue
    after the bound rather than being held indefinitely.
    """

    clock_now = now_fn or (lambda: datetime.now(TAIPEI))
    monotonic = monotonic_fn or time_module.monotonic
    session_date = observed.date().isoformat()
    expected = [
        spec
        for spec in specs
        if _mode_needs_opening_signal(
            engine,
            spec,
            session_date=session_date,
        )
    ]
    found: dict[
        str,
        tuple[dict[str, Any], list[dict[str, Any]], datetime],
    ] = {}

    def scan(current: datetime) -> None:
        for spec in expected:
            if spec.market in found:
                continue
            latest = _latest_signal(spec, current)
            if latest is None:
                continue
            summary, rows = latest
            mode = engine.state.get("modes", {}).get(spec.market, {})
            signal_id = str(summary.get("signal_id") or "")
            if not signal_id or signal_id in set(
                mode.get("processed_signal_ids") or ()
            ):
                continue
            found[spec.market] = (summary, rows, current)

    scan(observed)
    started = monotonic()
    opening_gate = observed.replace(hour=9, minute=0, second=0, microsecond=0)
    cutoff_at = opening_gate + timedelta(seconds=max(0.0, float(cutoff_seconds)))
    wait_budget = min(
        max(0.0, float(max_wait_seconds)),
        max(0.0, (cutoff_at - observed).total_seconds()),
    )
    should_wait = (
        len(expected) > 1
        and 0 < len(found) < len(expected)
        and wait_budget > 0.0
        and LIVE_ENTRY_GATE
        <= observed.timetz().replace(tzinfo=None)
        < datetime_time(9, 0, 15)
    )
    deadline = started + wait_budget
    current = observed
    while should_wait and len(found) < len(expected):
        remaining = deadline - monotonic()
        if remaining <= 0.0:
            break
        signal_watcher.wait(min(max(0.005, float(poll_seconds)), remaining))
        current = clock_now()
        scan(current)

    completed = clock_now() if should_wait else observed
    wait_by_market_ms = {
        market: round(
            max(0.0, (completed - detected_at).total_seconds() * 1000.0),
            3,
        )
        for market, (_summary, _rows, detected_at) in found.items()
    }
    metadata = {
        "expected_markets": [spec.market for spec in expected],
        "observed_markets": list(found),
        "expected_mode_count": len(expected),
        "observed_mode_count": len(found),
        "complete": bool(expected) and len(found) == len(expected),
        "timed_out": bool(should_wait and len(found) < len(expected)),
        "wait_ms": round(max(0.0, (monotonic() - started) * 1000.0), 3),
        "wait_by_market_ms": wait_by_market_ms,
        "cutoff_seconds_after_open": max(0.0, float(cutoff_seconds)),
    }
    return found, metadata


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
    parser.add_argument("--poll-seconds", type=float, default=0.1)
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
    state_dir = _repo_path(args.state_dir)
    engine_lock = _acquire_engine_lock(state_dir)
    engine = TwDayTradeSimulationEngine(state_dir)
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
    pending_retry_after: dict[str, float] = {}
    quote_client_warmed_session: str | None = None
    quote_client_prewarm_retry_after = 0.0
    eligibility_prewarmed_session: str | None = None
    signal_watcher = _SignalPointerWatcher()
    last_session_gate_log: tuple[str, tuple[tuple[str, str], ...]] | None = None
    non_session_invalidation_attempts: set[tuple[str, str]] = set()
    ready_notified = False
    print(
        f"[tw-day-trade-sim] state_dir={engine.state_dir} simulation_only=true",
        flush=True,
    )
    while True:
        notify_systemd("WATCHDOG=1")
        monotonic_now = time_module.monotonic()
        observed = datetime.now(TAIPEI)
        if monotonic_now - last_reload >= 30.0 or not specs:
            specs, live_configs, errors = _mode_specs(markets_dir)
            signal_watcher.configure([spec.live_output_dir for spec in specs])
            session_open, session_errors = _verified_stock_session(
                specs,
                live_configs,
                observed=observed,
            )
            current_coverage = (
                _current_eligibility_coverage(
                    specs,
                    live_configs,
                    trading_date=observed,
                )
                if session_open
                else {}
            )
            session_date_text = observed.date().isoformat()
            eligibility_prewarm_due = observed.timetz().replace(
                tzinfo=None
            ) >= datetime_time(8, 30)
            if (
                session_open
                and eligibility_prewarm_due
                and eligibility_prewarmed_session != session_date_text
            ):
                try:
                    eligibility_warm = _prewarm_live_eligibility(
                        specs,
                        live_configs,
                        trading_date=observed,
                    )
                    if eligibility_warm:
                        slowest = max(
                            eligibility_warm.values(),
                            key=lambda row: float(row.get("elapsed_ms") or 0.0),
                        )
                        print(
                            "[tw-day-trade-sim] eligibility_prewarm=ready "
                            f"symbols={slowest.get('symbol_count')} "
                            f"elapsed_ms={slowest.get('elapsed_ms')}",
                            flush=True,
                        )
                        _write_preopen_readiness(
                            engine.state_dir,
                            session_date=session_date_text,
                            component="eligibility",
                            status="ready",
                            observed=datetime.now(TAIPEI),
                            elapsed_ms=float(slowest.get("elapsed_ms") or 0.0),
                            details={
                                "proof": "exact_session_twse_tpex_coverage",
                                "symbol_count": int(slowest.get("symbol_count") or 0),
                                "markets": {
                                    market: dict(row)
                                    for market, row in eligibility_warm.items()
                                },
                            },
                        )
                    eligibility_prewarmed_session = session_date_text
                except Exception as exc:
                    _write_preopen_readiness(
                        engine.state_dir,
                        session_date=session_date_text,
                        component="eligibility",
                        status="failed",
                        observed=datetime.now(TAIPEI),
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    errors = {
                        **errors,
                        **{
                            spec.market: (
                                "eligibility_prewarm_failed: "
                                f"{type(exc).__name__}: {exc}"
                            )
                            for spec in specs
                        },
                    }
            engine.update_readiness(
                specs,
                now=observed,
                errors={
                    **errors,
                    **{
                        market: f"market_session_blocked: {reason}"
                        for market, reason in session_errors.items()
                    },
                },
                current_eligibility_coverage=current_coverage,
            )
            last_reload = monotonic_now
            last_readiness = monotonic_now
            if not ready_notified:
                notify_systemd(
                    "READY=1\nSTATUS=TW day-trade executor ready; waiting for 09:00 live signal and causal best quote"
                )
                ready_notified = True
        session_open, session_errors = _verified_stock_session(
            specs,
            live_configs,
            observed=observed,
        )
        if not session_open:
            for market, reason in session_errors.items():
                invalidation_key = (observed.date().isoformat(), market)
                if invalidation_key in non_session_invalidation_attempts:
                    continue
                try:
                    result = engine.invalidate_non_session_flat_signal(
                        market,
                        now=observed,
                        reason=reason,
                    )
                    if result == "invalidated":
                        print(
                            "[tw-day-trade-sim] non_session_signal=invalidated "
                            f"market={market} date={observed.date().isoformat()} "
                            f"reason={reason}",
                            flush=True,
                        )
                except Exception as exc:
                    print(
                        "[tw-day-trade-sim] non_session_signal=blocked "
                        f"market={market} error={type(exc).__name__}: {exc}",
                        flush=True,
                    )
                non_session_invalidation_attempts.add(invalidation_key)
            if monotonic_now - last_readiness >= 10.0:
                engine.update_readiness(
                    specs,
                    now=observed,
                    errors={
                        market: f"market_session_blocked: {reason}"
                        for market, reason in session_errors.items()
                    },
                )
                last_readiness = monotonic_now
            gate_key = (
                observed.date().isoformat(),
                tuple(sorted(session_errors.items())),
            )
            if gate_key != last_session_gate_log:
                print(
                    "[tw-day-trade-sim] market_session=blocked "
                    f"date={observed.date().isoformat()} reasons={session_errors}",
                    flush=True,
                )
                last_session_gate_log = gate_key
            if args.once:
                return 0
            time_module.sleep(max(1.0, float(args.poll_seconds)))
            continue
        last_session_gate_log = None
        if monotonic_now - last_readiness >= 10.0:
            engine.update_readiness(specs, now=observed)
            last_readiness = monotonic_now

        prewarm_wall_time = observed.timetz().replace(tzinfo=None)
        prewarm_session = observed.date().isoformat()
        if (
            quote_client_warmed_session != prewarm_session
            and prewarm_wall_time >= datetime_time(8, 55)
            and monotonic_now >= quote_client_prewarm_retry_after
        ):
            prewarm_started = time_module.perf_counter()
            try:
                quote_symbols = _eligible_execution_quote_symbols(
                    specs,
                    live_configs,
                    trading_date=observed,
                )
                quote_warm = warm_shioaji_stock_quote_client(quote_symbols)
                if not bool(quote_warm.get("ready")):
                    raise RuntimeError(
                        "Shioaji Contract V2 prewarm incomplete: "
                        f"resolved={quote_warm.get('resolved_count')} "
                        f"requested={quote_warm.get('requested_count')}"
                    )
                prewarm_elapsed_ms = (
                    time_module.perf_counter() - prewarm_started
                ) * 1000.0
                quote_client_warmed_session = prewarm_session
                _write_preopen_readiness(
                    engine.state_dir,
                    session_date=prewarm_session,
                    component="shioaji_quote",
                    status="ready",
                    observed=datetime.now(TAIPEI),
                    elapsed_ms=prewarm_elapsed_ms,
                    details={
                        "proof": "simulation_client_usage_probe",
                        "candidate_contract_probe": True,
                        "fresh_login_required_on_new_client": True,
                        **quote_warm,
                    },
                )
                print(
                    "[tw-day-trade-sim] shioaji_prewarm=ready "
                    f"contracts={quote_warm.get('resolved_count')}/"
                    f"{quote_warm.get('requested_count')} "
                    f"elapsed_ms={prewarm_elapsed_ms:.3f}",
                    flush=True,
                )
            except Exception as exc:
                quote_client_prewarm_retry_after = time_module.monotonic() + 5.0
                _write_preopen_readiness(
                    engine.state_dir,
                    session_date=prewarm_session,
                    component="shioaji_quote",
                    status="failed",
                    observed=datetime.now(TAIPEI),
                    elapsed_ms=(time_module.perf_counter() - prewarm_started) * 1000.0,
                    details={
                        "proof": "simulation_client_usage_probe",
                        "candidate_contract_probe": True,
                        "retry_after_seconds": 5.0,
                    },
                    error=f"{type(exc).__name__}: {exc}",
                )
                print(
                    f"[tw-day-trade-sim] shioaji_prewarm=failed "
                    f"error={type(exc).__name__}: {exc}",
                    flush=True,
                )

        opening_wall_time = observed.timetz().replace(tzinfo=None)
        opening_batch_active = (
            LIVE_ENTRY_GATE <= opening_wall_time < datetime_time(9, 0, 15)
        )
        opening_signals: dict[
            str,
            tuple[dict[str, Any], list[dict[str, Any]], datetime],
        ] = {}
        opening_batch: dict[str, Any] = {
            "expected_mode_count": 0,
            "observed_mode_count": 0,
            "complete": False,
            "timed_out": False,
            "wait_ms": 0.0,
            "wait_by_market_ms": {},
        }
        if opening_batch_active:
            opening_signals, opening_batch = _collect_opening_signal_batch(
                specs,
                engine,
                observed,
                signal_watcher=signal_watcher,
                max_wait_seconds=_opening_batch_max_wait_seconds(),
                cutoff_seconds=_opening_batch_cutoff_seconds(),
                poll_seconds=min(0.02, float(args.poll_seconds)),
            )
            if opening_signals:
                observed = datetime.now(TAIPEI)
                monotonic_now = time_module.monotonic()
                print(
                    "[tw-day-trade-sim] opening_signal_batch "
                    f"observed={opening_batch.get('observed_mode_count')}/"
                    f"{opening_batch.get('expected_mode_count')} "
                    f"wait_ms={opening_batch.get('wait_ms')} "
                    f"complete={opening_batch.get('complete')}",
                    flush=True,
                )

        pending: list[
            tuple[
                ModeSpec,
                LiveMarketConfig,
                dict[str, Any],
                list[dict[str, Any]],
                dict[str, Any],
                dict[str, Any],
                float,
                datetime,
                float,
                int,
                int,
                bool,
            ]
        ] = []
        pending_symbols: set[str] = set()
        pending_fallback: dict[str, float] = {}
        minute_key = observed.replace(second=0, microsecond=0).isoformat(
            timespec="minutes"
        )
        for spec in specs:
            if observed.timetz().replace(tzinfo=None) < LIVE_ENTRY_GATE:
                # Live signals start at 09:00. The engine separately enforces
                # that every execution quote is strictly later than the atomic
                # signal publication timestamp.
                continue
            mode = engine.state.get("modes", {}).get(spec.market, {})
            if str(mode.get("engine_status") or "") == (
                "critical_ledger_state_divergence"
            ):
                continue
            if str(
                mode.get("session_date") or ""
            ) == observed.date().isoformat() and mode.get("entry_completed_at"):
                continue
            if opening_batch_active:
                batched = opening_signals.get(spec.market)
                if batched is None:
                    continue
                summary, rows, detected_at = batched
            else:
                latest = _latest_signal(spec, observed)
                if latest is None:
                    continue
                summary, rows = latest
                detected_at = datetime.now(TAIPEI)
            signal_id = str(summary.get("signal_id") or "")
            if signal_id in set(mode.get("processed_signal_ids") or ()):
                continue
            if monotonic_now < pending_retry_after.get(spec.market, 0.0):
                continue
            live = live_configs[spec.market]
            eligibility_started = time_module.perf_counter()
            row_symbols = [str(row.get("symbol") or "") for row in rows]
            try:
                eligibility, coverage = _cached_live_eligibility(
                    rule_data_dir=_rule_data_dir(live, spec),
                    parquet_root=spec.parquet_root,
                    symbols=row_symbols,
                    trading_date=observed,
                )
            except Exception as exc:
                print(
                    f"[tw-day-trade-sim] market={spec.market} "
                    f"eligibility_error={type(exc).__name__}: {exc}",
                    flush=True,
                )
                pending_retry_after[spec.market] = time_module.monotonic() + 0.5
                continue
            candidate_symbols, candidate_fallback = _entry_candidate_symbols(
                spec=spec,
                rows=rows,
                eligibility=eligibility,
            )
            eligibility_ms = (time_module.perf_counter() - eligibility_started) * 1000.0
            pending.append(
                (
                    spec,
                    live,
                    summary,
                    rows,
                    eligibility,
                    coverage,
                    eligibility_ms,
                    detected_at,
                    float(
                        (opening_batch.get("wait_by_market_ms") or {}).get(
                            spec.market,
                            0.0,
                        )
                    ),
                    int(opening_batch.get("observed_mode_count") or 1),
                    int(opening_batch.get("expected_mode_count") or 1),
                    bool(opening_batch.get("complete", True)),
                )
            )
            pending_symbols.update(candidate_symbols)
            pending_fallback.update(candidate_fallback)

        active_symbols, active_fallback = _active_symbols(engine)
        wall_time = observed.timetz().replace(tzinfo=None)
        same_minute_force_exit_retry = (
            bool(active_symbols)
            and FORCE_EXIT_TIME <= wall_time < CLOSING_AUCTION_TIME
            and last_quote_minute == minute_key
        )
        # Keep observing only while a paper position remains.  In particular,
        # do not make terminal flatten depend on the process being alive during
        # the exact 13:30 minute: a late start/restart must catch up immediately
        # and the symbol set naturally becomes empty once the ledger is flat.
        quote_due = _active_quote_due(
            active_symbols,
            observed=observed,
            last_quote_minute=last_quote_minute,
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
        quote_fetch_ms = 0.0
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
                quote_started = time_module.perf_counter()
                quotes = _fetch_quotes(
                    symbols=all_symbols,
                    fallback_by_symbol=fallback,
                    parquet_root=specs[0].parquet_root,
                    trading_date=observed,
                )
                if benchmark_due:
                    _attach_benchmark_previous_close_context(
                        quotes,
                        symbols=benchmark_symbols,
                        parquet_root=specs[0].parquet_root,
                        trading_date=observed,
                    )
                quotes = engine.prepare_minute_quotes(quotes, now=observed)
                quote_fetch_ms = (time_module.perf_counter() - quote_started) * 1000.0
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

        for (
            spec,
            _live,
            summary,
            rows,
            eligibility,
            coverage,
            eligibility_ms,
            detected_at,
            opening_batch_wait_ms,
            opening_batch_mode_count,
            opening_batch_expected_mode_count,
            opening_batch_complete,
        ) in pending:
            try:
                ledger_started = time_module.perf_counter()
                register_observed = datetime.now(TAIPEI)
                result = engine.register_signal(
                    spec=spec,
                    summary=summary,
                    signal_rows=rows,
                    quotes=quotes,
                    eligibility=eligibility,
                    eligibility_coverage=coverage,
                    now=register_observed,
                )
                ledger_ms = (time_module.perf_counter() - ledger_started) * 1000.0
                persisted_at = datetime.now(TAIPEI)
                if result in {"waiting_quote", "waiting_first_minute"}:
                    pending_retry_after[spec.market] = (
                        time_module.monotonic()
                        + _pending_signal_retry_delay_seconds(result, persisted_at)
                    )
                else:
                    pending_retry_after.pop(spec.market, None)
                    engine.record_latency_sample(
                        market=spec.market,
                        signal_id=str(summary.get("signal_id") or ""),
                        result=result,
                        summary=summary,
                        consumer_detected_at=detected_at,
                        ledger_persisted_at=persisted_at,
                        executor_quote_fetch_ms=quote_fetch_ms,
                        eligibility_load_ms=eligibility_ms,
                        ledger_compute_persist_ms=ledger_ms,
                        opening_signal_batch_wait_ms=opening_batch_wait_ms,
                        opening_signal_batch_mode_count=opening_batch_mode_count,
                        opening_signal_batch_expected_mode_count=(
                            opening_batch_expected_mode_count
                        ),
                        opening_signal_batch_complete=opening_batch_complete,
                    )
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
                current_mode = engine.state.get("modes", {}).get(spec.market, {})
                if str(current_mode.get("engine_status") or "") == (
                    "critical_ledger_state_divergence"
                ):
                    raise

        if quote_due:
            engine.process_quotes(
                quotes=quotes,
                now=datetime.now(TAIPEI),
                append_mark_history=not same_minute_force_exit_retry,
            )
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
                corporate_action_reference_path=(
                    _rule_data_dir(live_configs[specs[0].market], specs[0])
                    / "tw_corporate_action_reference.parquet"
                ),
                now=datetime.now(TAIPEI),
            )

        if args.once:
            return 0
        signal_watcher.wait(
            _loop_sleep_seconds(
                datetime.now(TAIPEI),
                fast_seconds=float(args.poll_seconds),
                has_pending_signal=bool(pending),
                has_open_position=bool(active_symbols),
            )
        )


if __name__ == "__main__":
    raise SystemExit(main())
