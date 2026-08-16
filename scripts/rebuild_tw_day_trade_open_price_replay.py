#!/usr/bin/env python3
"""Rebuild selected day-trade paper sessions from actual session opens.

This is an explicitly counterfactual recovery utility.  It never submits a
broker order. Entries use the official daily open for completed sessions and a
fresh Shioaji snapshot's session-open field for the current session, with
synthetic unbounded paper liquidity. Completed sessions settle at the official
daily close. Current sessions remain open for the ordinary live quote service
to mark and manage after the rebuilt ledger is installed.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_tw_day_trade_simulation import _mode_specs, _rule_data_dir  # noqa: E402
from downloader.download_tw_public_data import (  # noqa: E402
    DEFAULT_DATASETS,
    _parse_historical_response_content,
)
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    ModeSpec,
    TwDayTradeSimulationEngine,
    load_live_eligibility,
)
from stockagent.live.quote_provider import fetch_shioaji_stock_snapshots  # noqa: E402


TAIPEI = ZoneInfo("Asia/Taipei")
PAPER_LIQUIDITY_LOTS = 1_000_000_000.0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _latest_valid_signal(
    spec: ModeSpec,
    trading_date: date,
) -> tuple[datetime, Path, Path, dict[str, Any], list[dict[str, Any]]]:
    ranked: list[tuple[datetime, Path, Path, dict[str, Any]]] = []
    day = trading_date.isoformat()
    for summary_path in spec.live_output_dir.glob(f"{day}*/**/summary.json"):
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            generated_at = datetime.fromisoformat(str(summary.get("generated_at")))
        except (OSError, TypeError, ValueError):
            continue
        if generated_at.tzinfo is None:
            generated_at = generated_at.replace(tzinfo=TAIPEI)
        generated_at = generated_at.astimezone(TAIPEI)
        weights_path = summary_path.with_name("target_weights.parquet")
        if (
            str(summary.get("market") or "")
            != str(spec.signal_market or spec.market)
            or str(summary.get("execution_mode") or "") != "tw_day_trade"
            or not bool(summary.get("live_session_open_feature_applied"))
            or generated_at.date() != trading_date
            or not weights_path.is_file()
        ):
            continue
        ranked.append((generated_at, summary_path, weights_path, summary))
    if not ranked:
        raise FileNotFoundError(
            f"{spec.market}: no open-feature-applied live signal for {day}"
        )
    generated_at, summary_path, weights_path, summary = max(
        ranked, key=lambda item: item[0]
    )
    rows = pl.read_parquet(weights_path).to_dicts()
    if not rows:
        raise ValueError(f"{weights_path} contains no target rows")
    return generated_at, summary_path, weights_path, summary, rows


def _load_price_limits(path: Path) -> dict[str, dict[str, Any]]:
    required = {
        "symbol",
        "reference_price",
        "upper_limit_price",
        "lower_limit_price",
    }
    frame = pl.read_parquet(path)
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path} missing price-limit columns: {sorted(missing)}")
    return {
        str(row["symbol"]): row
        for row in frame.select(sorted(required)).to_dicts()
    }


def _materialize_exact_session_rules(
    *,
    source_root: Path,
    destination_root: Path,
    trading_date: date,
) -> dict[str, dict[str, Any]]:
    """Restore exact-session eligibility parquet from retained official raw JSON."""

    provenance: dict[str, dict[str, Any]] = {}
    destination_root.mkdir(parents=True, exist_ok=True)
    for dataset in (
        "twse_day_trade_eligibility",
        "tpex_day_trade_eligibility",
    ):
        raw_path = (
            source_root
            / "raw"
            / dataset
            / f"{trading_date.isoformat()}.json"
        )
        if not raw_path.is_file():
            raise FileNotFoundError(
                f"retained official exact-session rule is missing: {raw_path}"
            )
        raw = raw_path.read_bytes()
        frame, _suffix = _parse_historical_response_content(
            DEFAULT_DATASETS[dataset],
            trading_date,
            raw,
            "json",
        )
        destination = destination_root / f"{dataset}.parquet"
        frame.write_parquet(destination)
        provenance[dataset] = {
            "raw_path": str(raw_path),
            "raw_sha256": hashlib.sha256(raw).hexdigest(),
            "normalized_path": str(destination),
            "normalized_sha256": _sha256(destination),
            "rows": frame.height,
        }
    return provenance


def _entry_quotes(
    rows: list[dict[str, Any]],
    limits: Mapping[str, Mapping[str, Any]],
    *,
    quote_at: datetime,
    spec: ModeSpec,
    canonical_open_by_symbol: Mapping[str, float],
    canonical_open_source: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    quotes: dict[str, dict[str, Any]] = {}
    potential_whole_lot_rows = 0
    official_open_covered_rows = 0
    missing_official_open_symbols: list[str] = []
    official_open_mismatches: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        try:
            recorded_signal_open = float(row.get("open_price"))
        except (TypeError, ValueError):
            recorded_signal_open = math.nan
        try:
            opening_price = float(canonical_open_by_symbol.get(symbol, math.nan))
        except (TypeError, ValueError):
            opening_price = math.nan
        target_weight = abs(float(row.get("target_weight") or 0.0))
        potentially_executable = (
            math.isfinite(recorded_signal_open)
            and recorded_signal_open > 0.0
            and target_weight
            * float(spec.initial_capital_twd)
            / recorded_signal_open
            >= int(spec.lot_size)
        )
        if potentially_executable:
            potential_whole_lot_rows += 1
        if potentially_executable:
            if not (math.isfinite(opening_price) and opening_price > 0.0):
                missing_official_open_symbols.append(symbol)
            else:
                official_open_covered_rows += 1
                if not math.isclose(
                    recorded_signal_open,
                    opening_price,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                ):
                    official_open_mismatches.append(
                        {
                            "symbol": symbol,
                            "recorded_signal_open": recorded_signal_open,
                            "canonical_session_open": opening_price,
                        }
                    )
        evidence = limits.get(symbol) or {}
        serialized_open = (
            opening_price
            if math.isfinite(opening_price) and opening_price > 0.0
            else None
        )
        quotes[symbol] = {
            "symbol": symbol,
            "open": serialized_open,
            "last": serialized_open,
            "bid": serialized_open,
            "ask": serialized_open,
            "bid_volume": PAPER_LIQUIDITY_LOTS,
            "ask_volume": PAPER_LIQUIDITY_LOTS,
            "minute_volume_lots": PAPER_LIQUIDITY_LOTS,
            "upper_limit": evidence.get("upper_limit_price"),
            "lower_limit": evidence.get("lower_limit_price"),
            "reference_price": evidence.get("reference_price"),
            "quote_at": quote_at.isoformat(timespec="seconds"),
            "source": canonical_open_source,
        }
    return quotes, {
        "potential_whole_lot_rows": potential_whole_lot_rows,
        "official_open_covered_rows": official_open_covered_rows,
        "missing_official_open_symbols": sorted(missing_official_open_symbols),
        "official_open_mismatches": official_open_mismatches,
        "canonical_open_source": canonical_open_source,
    }


@lru_cache(maxsize=None)
def _official_daily_row(
    parquet_root: Path,
    symbol: str,
    trading_date: date,
) -> dict[str, Any] | None:
    path = parquet_root / f"{symbol}_features.parquet"
    if not path.is_file():
        return None
    frame = (
        pl.scan_parquet(path)
        .filter(pl.col("date") == trading_date)
        .select("date", "open", "max", "min", "close")
        .collect()
    )
    return frame.row(0, named=True) if frame.height == 1 else None


def _official_open_map(
    specs: list[ModeSpec],
    selected: Mapping[str, tuple[datetime, Path, Path, dict[str, Any], list[dict[str, Any]]]],
    trading_date: date,
) -> dict[str, float]:
    opens: dict[str, float] = {}
    for spec in specs:
        rows = selected[spec.market][4]
        for row in rows:
            symbol = str(row.get("symbol") or "")
            if not symbol or symbol in opens:
                continue
            daily = _official_daily_row(spec.parquet_root, symbol, trading_date)
            if daily is None:
                continue
            opening = float(daily.get("open") or math.nan)
            if math.isfinite(opening) and opening > 0.0:
                opens[symbol] = opening
    return opens


def _capture_current_open_map(
    *,
    specs: list[ModeSpec],
    selected: Mapping[str, tuple[datetime, Path, Path, dict[str, Any], list[dict[str, Any]]]],
    state_dir: Path,
    trading_date: date,
) -> tuple[dict[str, float], dict[str, Any]]:
    fallback_by_symbol: dict[str, float] = {}
    for spec in specs:
        for row in selected[spec.market][4]:
            if abs(float(row.get("target_weight") or 0.0)) <= 0.0:
                continue
            symbol = str(row.get("symbol") or "")
            try:
                fallback = float(row.get("open_price"))
            except (TypeError, ValueError):
                fallback = math.nan
            if symbol and math.isfinite(fallback) and fallback > 0.0:
                fallback_by_symbol.setdefault(symbol, fallback)
    symbols = sorted(fallback_by_symbol)
    snapshot = fetch_shioaji_stock_snapshots(
        symbols,
        np.asarray([fallback_by_symbol[symbol] for symbol in symbols]),
        cache_ttl_seconds=0.0,
    )
    open_prices = np.asarray(snapshot.open_prices, dtype=np.float64)
    available = np.asarray(snapshot.available_mask, dtype=bool)
    timestamps = np.asarray(snapshot.timestamps_ms, dtype=np.int64)
    rows: list[dict[str, Any]] = []
    resolved: dict[str, float] = {}
    for index, symbol in enumerate(symbols):
        opening = float(open_prices[index])
        valid = bool(available[index]) and math.isfinite(opening) and opening > 0.0
        if valid:
            resolved[symbol] = opening
        rows.append(
            {
                "trading_date": trading_date,
                "symbol": symbol,
                "open_price": opening if valid else None,
                "snapshot_available": bool(available[index]),
                "timestamp_ms": int(timestamps[index]),
                "source": snapshot.source,
            }
        )
    path = state_dir / "replay_open_data" / f"{trading_date.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    return resolved, {
        "path": str(path),
        "sha256": _sha256(path),
        "requested_symbols": len(symbols),
        "snapshot_available_symbols": int(available.sum()),
        "valid_open_symbols": len(resolved),
        "source": snapshot.source,
    }


def _reuse_current_open_map(
    *,
    source_path: Path,
    state_dir: Path,
    trading_date: date,
) -> tuple[dict[str, float], dict[str, Any]]:
    """Reuse a retained same-session Shioaji open snapshot without more quota."""

    source_path = source_path.resolve()
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    frame = pl.read_parquet(source_path)
    required = {
        "trading_date",
        "symbol",
        "open_price",
        "snapshot_available",
        "timestamp_ms",
        "source",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(
            f"{source_path} missing cached-open columns: {sorted(missing)}"
        )
    frame = frame.filter(pl.col("trading_date").cast(pl.Date) == trading_date)
    if frame.is_empty():
        raise ValueError(
            f"{source_path} has no rows for current session {trading_date}"
        )
    if frame.select(pl.col("symbol").n_unique()).item() != frame.height:
        raise ValueError(f"{source_path} has duplicate symbols")
    resolved = {
        str(row["symbol"]): float(row["open_price"])
        for row in frame.to_dicts()
        if bool(row.get("snapshot_available"))
        and row.get("open_price") is not None
        and math.isfinite(float(row["open_price"]))
        and float(row["open_price"]) > 0.0
    }
    if not resolved:
        raise ValueError(f"{source_path} has no valid session-open prices")
    destination = state_dir / "replay_open_data" / f"{trading_date.isoformat()}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(destination)
    return resolved, {
        "path": str(destination),
        "sha256": _sha256(destination),
        "reused_from_path": str(source_path),
        "reused_from_sha256": _sha256(source_path),
        "requested_symbols": frame.height,
        "snapshot_available_symbols": int(
            frame.select(pl.col("snapshot_available").sum()).item()
        ),
        "valid_open_symbols": len(resolved),
        "source": "retained_same_session_shioaji_snapshot",
        "additional_shioaji_requests": 0,
    }


def _close_quotes(
    engine: TwDayTradeSimulationEngine,
    specs_by_market: Mapping[str, ModeSpec],
    *,
    trading_date: date,
    quote_at: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    quotes: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    open_mismatches: list[dict[str, Any]] = []
    observed_symbols: set[str] = set()
    for market, mode in (engine.state.get("modes") or {}).items():
        if str(mode.get("session_date") or "") != trading_date.isoformat():
            continue
        spec = specs_by_market[str(market)]
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            symbol = str(position.get("symbol") or "")
            if symbol in observed_symbols:
                continue
            observed_symbols.add(symbol)
            row = _official_daily_row(spec.parquet_root, symbol, trading_date)
            if row is None or not math.isfinite(float(row.get("close") or math.nan)):
                missing.append(symbol)
                continue
            entry_open = float(position.get("sizing_open_price") or math.nan)
            official_open = float(row.get("open") or math.nan)
            if not math.isclose(entry_open, official_open, rel_tol=0.0, abs_tol=1e-9):
                open_mismatches.append(
                    {
                        "symbol": symbol,
                        "signal_open": entry_open,
                        "official_open": official_open,
                    }
                )
            close_price = float(row["close"])
            quotes[symbol] = {
                "symbol": symbol,
                "open": official_open,
                "last": close_price,
                "bid": close_price,
                "ask": close_price,
                "minute_volume_lots": PAPER_LIQUIDITY_LOTS,
                "bid_volume": PAPER_LIQUIDITY_LOTS,
                "ask_volume": PAPER_LIQUIDITY_LOTS,
                "quote_at": quote_at.isoformat(timespec="seconds"),
                "source": "official_daily_close_replay",
            }
    if missing:
        raise ValueError(
            f"official close unavailable for {trading_date}: {sorted(missing)}"
        )
    return quotes, {
        "position_symbols": len(observed_symbols),
        "official_close_covered_symbols": len(quotes),
        "recorded_open_matches_official": not open_mismatches,
        "recorded_open_mismatches": open_mismatches,
    }


def _position_stats(mode: Mapping[str, Any]) -> dict[str, Any]:
    positions = [
        row
        for row in (mode.get("positions") or {}).values()
        if isinstance(row, Mapping)
    ]
    return {
        "position_rows": len(positions),
        "open_position_rows": sum(
            int(row.get("signed_shares") or 0) != 0 for row in positions
        ),
        "long_shares": sum(
            max(int(row.get("signed_shares") or 0), 0) for row in positions
        ),
        "short_shares": sum(
            max(-int(row.get("signed_shares") or 0), 0) for row in positions
        ),
        "engine_status": mode.get("engine_status"),
        "signal_counts": mode.get("signal_counts") or {},
        "execution_projection": mode.get("execution_projection") or {},
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--markets-dir", type=Path, default=Path("services/discord_bot/markets")
    )
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--price-limit-dir",
        type=Path,
        default=Path("artifacts/live/tw_price_limits"),
    )
    current_open = parser.add_mutually_exclusive_group()
    current_open.add_argument(
        "--fetch-current-open",
        action="store_true",
        help=(
            "Fetch today's actual session-open fields from Shioaji snapshots. "
            "Required when rebuilding an unclosed current session."
        ),
    )
    current_open.add_argument(
        "--current-open-path",
        type=Path,
        help=(
            "Reuse a retained same-session Shioaji open snapshot parquet instead "
            "of consuming more quote requests."
        ),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("--start-date must not be after --end-date")
    state_dir = args.state_dir.resolve()
    if state_dir.exists() and any(state_dir.iterdir()):
        raise RuntimeError(f"refusing non-empty rebuild target: {state_dir}")

    specs, live_configs, errors = _mode_specs(args.markets_dir)
    if errors:
        raise RuntimeError(f"mode configuration errors: {errors}")
    if not specs:
        raise RuntimeError("no enabled day-trade simulation modes")
    specs_by_market = {spec.market: spec for spec in specs}
    engine = TwDayTradeSimulationEngine(state_dir)
    current = datetime.now(TAIPEI)
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "created_at": current.isoformat(timespec="seconds"),
        "simulation_only": True,
        "production_order_possible": False,
        "replay_contract": {
            "entry": (
                "official daily open for completed sessions; fresh Shioaji "
                "snapshot open for the current session"
            ),
            "entry_liquidity": "counterfactual_unbounded; no broker fill claim",
            "whole_lot_and_directional_mix": "ordinary live simulation rules retained",
            "completed_session_exit": "official daily close",
            "intraday_path": "not reconstructed; daily-limit bracket ordering is not inferred from OHLC",
            "current_session": "left open for ordinary live quote service",
            "recorded_entry_time": "09:00:00 Asia/Taipei for every replayed session",
            "source_signal_time": "retained separately for provenance",
        },
        "state_dir": str(state_dir),
        "sessions": [],
    }

    day = start
    while day <= end:
        if day.weekday() >= 5:
            raise ValueError(f"weekend session requested: {day}")
        limit_path = (args.price_limit_dir / f"{day.isoformat()}.parquet").resolve()
        if not limit_path.is_file():
            raise FileNotFoundError(limit_path)
        limits = _load_price_limits(limit_path)
        session_receipt: dict[str, Any] = {
            "session_date": day.isoformat(),
            "price_limit_path": str(limit_path),
            "price_limit_sha256": _sha256(limit_path),
            "price_limit_rows": len(limits),
            "modes": [],
        }
        selected = {
            spec.market: _latest_valid_signal(spec, day) for spec in specs
        }
        should_close = day < current.date() or (
            day == current.date()
            and current.timetz().replace(tzinfo=None) >= time(13, 30)
        )
        if should_close:
            canonical_open_by_symbol = _official_open_map(specs, selected, day)
            canonical_open_source = "official_daily_session_open"
            session_receipt["canonical_open"] = {
                "source": canonical_open_source,
                "valid_open_symbols": len(canonical_open_by_symbol),
            }
        else:
            if day != current.date() or not (
                bool(args.fetch_current_open) or args.current_open_path is not None
            ):
                raise RuntimeError(
                    "an unclosed current session requires --fetch-current-open "
                    "or --current-open-path"
                )
            if args.current_open_path is not None:
                canonical_open_by_symbol, open_provenance = _reuse_current_open_map(
                    source_path=args.current_open_path,
                    state_dir=state_dir,
                    trading_date=day,
                )
                canonical_open_source = (
                    "retained_same_session_shioaji_snapshot_session_open"
                )
            else:
                canonical_open_by_symbol, open_provenance = _capture_current_open_map(
                    specs=specs,
                    selected=selected,
                    state_dir=state_dir,
                    trading_date=day,
                )
                canonical_open_source = "fresh_shioaji_snapshot_session_open"
            session_receipt["canonical_open"] = {
                **open_provenance,
                "canonical_source": canonical_open_source,
            }
        configured_rule_roots = {
            _rule_data_dir(live_configs[spec.market], spec).resolve()
            for spec in specs
        }
        if len(configured_rule_roots) != 1:
            raise RuntimeError(
                "replay requires one shared exact-session rule source; got "
                f"{sorted(str(path) for path in configured_rule_roots)}"
            )
        configured_rule_root = next(iter(configured_rule_roots))
        replay_rule_root = state_dir / "replay_rule_data" / day.isoformat()
        session_receipt["eligibility_rules"] = _materialize_exact_session_rules(
            source_root=configured_rule_root,
            destination_root=replay_rule_root,
            trading_date=day,
        )
        for spec in specs:
            generated_at, summary_path, weights_path, summary, rows = (
                selected[spec.market]
            )
            observed = datetime.combine(day, time(9, 0), tzinfo=TAIPEI)
            symbols = [str(row.get("symbol") or "") for row in rows]
            eligibility, coverage = load_live_eligibility(
                rule_data_dir=replay_rule_root,
                parquet_root=spec.parquet_root,
                symbols=symbols,
                trading_date=day,
            )
            replay_summary = dict(summary)
            replay_summary.update(
                {
                    "summary_path": str(summary_path.resolve()),
                    "weights_path": str(weights_path.resolve()),
                    "simulation_replay": True,
                    "replay_basis": (
                        "actual_session_open_to_official_close"
                        if should_close
                        else "actual_session_open_to_live_quotes"
                    ),
                    "replay_source": "immutable_live_signal_and_official_market_data",
                    "entry_fill_contract": (
                        "retrospective_actual_session_open_price_counterfactual"
                    ),
                    "entry_liquidity_assumption": (
                        "counterfactual_unbounded_open_liquidity_requested_for_rebuild"
                    ),
                    "replay_effective_signal_at": observed.isoformat(
                        timespec="seconds"
                    ),
                    "source_signal_generated_at": generated_at.isoformat(
                        timespec="seconds"
                    ),
                }
            )
            entry_quotes, entry_price_quality = _entry_quotes(
                rows,
                limits,
                quote_at=observed,
                spec=spec,
                canonical_open_by_symbol=canonical_open_by_symbol,
                canonical_open_source=canonical_open_source,
            )
            result = engine.register_signal(
                spec=spec,
                summary=replay_summary,
                signal_rows=rows,
                quotes=entry_quotes,
                eligibility=eligibility,
                eligibility_coverage=coverage,
                now=observed,
                counterfactual_open_replay=True,
            )
            mode = engine.state["modes"][spec.market]
            session_receipt["modes"].append(
                {
                    "market": spec.market,
                    "signal_id": summary.get("signal_id"),
                    "signal_generated_at": generated_at.isoformat(timespec="seconds"),
                    "replay_effective_signal_at": observed.isoformat(
                        timespec="seconds"
                    ),
                    "summary_path": str(summary_path.resolve()),
                    "summary_sha256": _sha256(summary_path),
                    "weights_path": str(weights_path.resolve()),
                    "weights_sha256": _sha256(weights_path),
                    "target_rows": len(rows),
                    "actionable_target_rows": sum(
                        abs(float(row.get("target_weight") or 0.0)) > 0.0
                        for row in rows
                    ),
                    "eligibility_coverage": coverage,
                    "register_result": result,
                    "entry_price_quality": entry_price_quality,
                    "entry": _position_stats(mode),
                }
            )

        if should_close:
            close_at = datetime.combine(day, time(13, 30), tzinfo=TAIPEI)
            close_quotes, close_quality = _close_quotes(
                engine,
                specs_by_market,
                trading_date=day,
                quote_at=close_at,
            )
            engine.process_quotes(quotes=close_quotes, now=close_at)
            session_receipt["close"] = {
                "status": "settled_official_close",
                **close_quality,
            }
            for mode_receipt in session_receipt["modes"]:
                mode_receipt["after_close"] = _position_stats(
                    engine.state["modes"][mode_receipt["market"]]
                )
        else:
            session_receipt["close"] = {
                "status": "current_session_left_open_for_live_service"
            }
        receipt["sessions"].append(session_receipt)
        day += timedelta(days=1)

    _atomic_json(state_dir / "rebuild_receipt.json", receipt)
    print(
        json.dumps(
            {
                "state_dir": str(state_dir),
                "sessions": [
                    {
                        "session_date": session["session_date"],
                        "close_status": session["close"]["status"],
                        "modes": {
                            mode["market"]: mode["entry"]
                            for mode in session["modes"]
                        },
                    }
                    for session in receipt["sessions"]
                ],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
