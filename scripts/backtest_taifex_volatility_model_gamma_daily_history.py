#!/usr/bin/env python3
"""Run six daily causal TXO volatility-delta models over the long archive.

The canonical one-lot weekly ATM straddle ledger is reused unchanged.  A model
is calibrated only from a completed session's official TXO daily surface, and
its MTX hedge target becomes executable at the following session's official
day-session open.  Daily archives do not contain synchronized historical
Bid/Ask, so this is an end-of-day causal research proxy rather than a tick-fill
backtest.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, time, timezone
import hashlib
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import sys
from typing import Any, Final, Iterable, Mapping, Sequence

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_atm_straddle_rolling import (  # noqa: E402
    _sha256_path,
)
from scripts.backtest_taifex_opening_straddle_hold_to_expiry import (  # noqa: E402
    _load_official_final_settlements,
)
from scripts.backtest_taifex_option_benchmarks import (  # noqa: E402
    FIXED_FEES_PER_CONTRACT_SIDE,
    FUTURES_MULTIPLIERS,
    OPTION_MULTIPLIER,
    _round_nearest_contract,
)
from scripts.backtest_taifex_volatility_model_gamma import (  # noqa: E402
    _parameter_at_bound,
)
from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    _atomic_json,
    _atomic_parquet,
    taifex_option_expiry,
)
from stockagent.data.tw_index_options_daily import (  # noqa: E402
    TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
    iter_taifex_option_daily_rows,
)
from stockagent.research.taifex_transaction_tax import (  # noqa: E402
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)
from stockagent.research.taifex_volatility_models import (  # noqa: E402
    CausalVolatilitySurface,
    SECONDS_PER_YEAR,
    SurfacePoint,
    VOLATILITY_MODEL_IDS,
    VOLATILITY_MODEL_IMPLEMENTATION,
    VOLATILITY_MODEL_LABELS,
    black76_implied_volatility,
    fit_volatility_model,
)


OUTPUT_SCHEMA_VERSION: Final[int] = 1
SURFACE_CACHE_SCHEMA_VERSION: Final[int] = 1
CLASSIC_VARIANT_ID: Final[str] = "classic_opening_straddle"
MODEL_VARIANT_PREFIX: Final[str] = "daily_vol_model_gamma__"
HEDGE_PRODUCT: Final[str] = "MTX"
START_DATE: Final[date] = date(2012, 11, 21)
END_DATE: Final[date] = date(2026, 8, 7)
CALIBRATION_TIME: Final[time] = time(13, 45)
EXPIRY_TIME: Final[time] = time(13, 30)
DEFAULT_BASELINE_DIR: Final[Path] = Path(
    "artifacts/research/taifex_opening_straddle_hold_to_weekly_expiry"
)
DEFAULT_OPTION_RAW_ROOT: Final[Path] = Path("data_tw_index_options_daily/raw")
DEFAULT_FINAL_SETTLEMENT: Final[Path] = Path(
    "data_tw_index_options_daily/txo_final_settlement_history.parquet"
)
DEFAULT_FUTURES_PATH: Final[Path] = Path(
    "data_tw_index_futures/day_session_front_month.parquet"
)
DEFAULT_OUTPUT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_volatility_model_gamma_daily_2012_2026"
)


def _variant_id(model_id: str) -> str:
    return f"{MODEL_VARIANT_PREFIX}{model_id}"


def _surface_source_paths(root: Path, start: date, end: date) -> list[Path]:
    paths: list[Path] = []
    for year in range(start.year, min(end.year, 2025) + 1):
        path = root / "annual" / f"{year}_opt.zip"
        if not path.is_file():
            raise FileNotFoundError(f"missing official annual TXO receipt: {path}")
        paths.append(path)
    if end.year >= 2026:
        ranges = sorted((root / "ranges").glob("2026-*_TXO.csv"))
        if not ranges:
            raise FileNotFoundError(f"missing official 2026 TXO range receipts under {root}")
        paths.extend(ranges)
    return paths


def _source_identity(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(str(path.resolve()).encode("utf-8"))
        digest.update(b"\0")
        digest.update(_sha256_path(path).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _finite_positive(value: object) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0.0


def _official_expiry_by_series(
    final_settlement_path: Path,
) -> dict[str, date]:
    rows = _load_official_final_settlements(final_settlement_path)
    output: dict[str, date] = {}
    for settlement_date, series in rows:
        previous = output.get(series)
        if previous is not None and previous != settlement_date:
            raise ValueError(f"multiple official expiry dates for {series}")
        output[series] = settlement_date
    return output


def _surface_schema():
    import pyarrow as pa

    return pa.schema(
        [
            ("date", pa.date32()),
            ("forward", pa.float64()),
            ("option_series", pa.string()),
            ("series_scope", pa.string()),
            ("expiry", pa.date32()),
            ("strike", pa.float64()),
            ("option_right", pa.string()),
            ("price_points", pa.float64()),
            ("price_source", pa.string()),
            ("years_to_expiry", pa.float64()),
            ("log_moneyness", pa.float64()),
            ("implied_volatility", pa.float64()),
            ("volume", pa.int64()),
            ("source_file", pa.string()),
            ("source_sha256", pa.string()),
        ]
    )


def _build_surface_cache(
    *,
    source_paths: Sequence[Path],
    target_dates: set[date],
    tx_close_by_date: Mapping[date, float],
    expiry_by_series: Mapping[str, date],
    output_path: Path,
    maximum_abs_log_moneyness: float,
    source_identity: str,
) -> dict[str, Any]:
    """Build one OTM close/settlement IV point per date/series/strike."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    if maximum_abs_log_moneyness <= 0.0:
        raise ValueError("maximum_abs_log_moneyness must be positive")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    if temporary.exists():
        temporary.unlink()
    schema = _surface_schema()
    metadata = {
        b"stockagent.dataset": b"taifex_daily_close_iv_surface",
        b"stockagent.contract_version": str(SURFACE_CACHE_SCHEMA_VERSION).encode("ascii"),
        b"stockagent.source_identity": source_identity.encode("ascii"),
        b"stockagent.date_start": min(target_dates).isoformat().encode("ascii"),
        b"stockagent.date_end": max(target_dates).isoformat().encode("ascii"),
        b"stockagent.maximum_abs_log_moneyness": str(
            maximum_abs_log_moneyness
        ).encode("ascii"),
    }
    writer = pq.ParquetWriter(
        temporary,
        schema.with_metadata(metadata),
        compression="zstd",
    )
    total_rows = 0
    settlement_rows = 0
    close_fallback_rows = 0
    observed_dates: set[date] = set()
    try:
        for source_index, source_path in enumerate(source_paths, start=1):
            candidates: dict[
                tuple[date, str, float], dict[str, dict[str, Any]]
            ] = {}
            for raw in iter_taifex_option_daily_rows(
                [source_path],
                trading_dates=target_dates,
            ):
                trading_date = raw["date"]
                assert isinstance(trading_date, date)
                forward = tx_close_by_date.get(trading_date)
                if forward is None or not _finite_positive(forward):
                    continue
                series = str(raw["option_series"])
                expiry = expiry_by_series.get(series)
                if expiry is None:
                    try:
                        expiry = taifex_option_expiry(series)
                    except ValueError:
                        continue
                if expiry <= trading_date:
                    continue
                strike = float(raw["strike"])
                log_moneyness = math.log(strike / float(forward))
                if abs(log_moneyness) > maximum_abs_log_moneyness:
                    continue
                settlement = raw.get("settlement")
                close = raw.get("close")
                if _finite_positive(settlement):
                    price = float(settlement)
                    price_source = "official_daily_settlement"
                elif _finite_positive(close):
                    price = float(close)
                    price_source = "official_daily_last_trade_fallback"
                else:
                    continue
                calibration_dt = datetime.combine(
                    trading_date,
                    CALIBRATION_TIME,
                )
                expiry_dt = datetime.combine(expiry, EXPIRY_TIME)
                years = (expiry_dt - calibration_dt).total_seconds() / SECONDS_PER_YEAR
                if years <= 0.0:
                    continue
                right = str(raw["option_right"])
                implied = black76_implied_volatility(
                    forward=float(forward),
                    strike=strike,
                    years_to_expiry=years,
                    price_points=price,
                    option_right=right,
                )
                if implied is None:
                    continue
                point = {
                    "date": trading_date,
                    "forward": float(forward),
                    "option_series": series,
                    "series_scope": str(raw["series_scope"]),
                    "expiry": expiry,
                    "strike": strike,
                    "option_right": right,
                    "price_points": price,
                    "price_source": price_source,
                    "years_to_expiry": years,
                    "log_moneyness": log_moneyness,
                    "implied_volatility": implied,
                    "volume": int(raw["volume"]),
                    "source_file": str(raw["source_file"]),
                    "source_sha256": str(raw["source_sha256"]),
                }
                candidates.setdefault((trading_date, series, strike), {})[right] = point

            selected: list[dict[str, Any]] = []
            for (trading_date, _series, strike), rights in sorted(candidates.items()):
                forward = tx_close_by_date[trading_date]
                preferred = "C" if strike >= forward else "P"
                point = rights.get(preferred)
                if point is None and rights:
                    point = min(
                        rights.values(),
                        key=lambda value: str(value["option_right"]),
                    )
                if point is not None:
                    selected.append(point)
            if selected:
                writer.write_table(pa.Table.from_pylist(selected, schema=schema))
                total_rows += len(selected)
                observed_dates.update(row["date"] for row in selected)
                settlement_rows += sum(
                    row["price_source"] == "official_daily_settlement"
                    for row in selected
                )
                close_fallback_rows += sum(
                    row["price_source"] == "official_daily_last_trade_fallback"
                    for row in selected
                )
            print(
                f"[daily-surface] receipt={source_index}/{len(source_paths)} "
                f"file={source_path.name} selected={len(selected):,} "
                f"total={total_rows:,}",
                flush=True,
            )
    except BaseException:
        writer.close()
        if temporary.exists():
            temporary.unlink()
        raise
    writer.close()
    if total_rows == 0:
        temporary.unlink(missing_ok=True)
        raise ValueError("daily IV surface cache is empty")
    temporary.replace(output_path)
    return {
        "surface_rows": total_rows,
        "surface_dates": len(observed_dates),
        "settlement_price_rows": settlement_rows,
        "last_trade_fallback_rows": close_fallback_rows,
    }


def _verify_surface_cache(
    path: Path,
    *,
    source_identity: str,
    start: date,
    end: date,
    maximum_abs_log_moneyness: float,
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    import pyarrow.parquet as pq

    metadata = pq.read_schema(path).metadata or {}
    expected = {
        b"stockagent.contract_version": str(SURFACE_CACHE_SCHEMA_VERSION).encode("ascii"),
        b"stockagent.source_identity": source_identity.encode("ascii"),
        b"stockagent.date_start": start.isoformat().encode("ascii"),
        b"stockagent.date_end": end.isoformat().encode("ascii"),
        b"stockagent.maximum_abs_log_moneyness": str(
            maximum_abs_log_moneyness
        ).encode("ascii"),
    }
    if any(metadata.get(key) != value for key, value in expected.items()):
        return None
    frame = pl.scan_parquet(path).select(
        pl.len().alias("rows"),
        pl.col("date").n_unique().alias("dates"),
        (pl.col("price_source") == "official_daily_settlement")
        .sum()
        .alias("settlement_rows"),
        (pl.col("price_source") == "official_daily_last_trade_fallback")
        .sum()
        .alias("fallback_rows"),
    ).collect()
    return {
        "surface_rows": int(frame.item(0, "rows")),
        "surface_dates": int(frame.item(0, "dates")),
        "settlement_price_rows": int(frame.item(0, "settlement_rows")),
        "last_trade_fallback_rows": int(frame.item(0, "fallback_rows")),
    }


def _surface_from_frame(frame: pl.DataFrame, decision_date: date) -> CausalVolatilitySurface:
    points = tuple(
        SurfacePoint(
            series=str(row["option_series"]),
            expiry=row["expiry"],
            strike=float(row["strike"]),
            option_right=str(row["option_right"]),
            price_points=float(row["price_points"]),
            years_to_expiry=float(row["years_to_expiry"]),
            log_moneyness=float(row["log_moneyness"]),
            implied_volatility=float(row["implied_volatility"]),
            staleness_seconds=0.0,
        )
        for row in frame.iter_rows(named=True)
    )
    if not points:
        raise ValueError(f"empty IV surface on {decision_date}")
    forward_values = frame.get_column("forward").unique().to_list()
    if len(forward_values) != 1:
        raise ValueError(f"surface forward is not unique on {decision_date}")
    decision_ns = int(
        datetime.combine(decision_date, CALIBRATION_TIME)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1_000_000_000
    )
    return CausalVolatilitySurface(
        calibration_decision_ns=decision_ns,
        observable_through_ns=decision_ns,
        forward=float(forward_values[0]),
        points=points,
    )


def _fit_model_history_worker(
    model_id: str,
    surface_cache: str,
    baseline_daily_path: str,
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    surfaces = pl.read_parquet(surface_cache).sort(
        ["date", "years_to_expiry", "strike"]
    )
    surface_by_date = {
        key[0] if isinstance(key, tuple) else key: frame
        for key, frame in surfaces.partition_by("date", as_dict=True).items()
    }
    baseline = pl.read_parquet(baseline_daily_path).sort("trading_date")
    rows = baseline.iter_rows(named=True)
    daily_rows = list(rows)
    signals: list[dict[str, Any]] = []
    calibrations: list[dict[str, Any]] = []
    for index, row in enumerate(daily_rows[:-1]):
        decision_date = row["trading_date"]
        effective_date = daily_rows[index + 1]["trading_date"]
        cycle_id = row["cycle_id"]
        series = row["option_series"]
        if cycle_id is None or series is None or bool(row["is_expiry_session"]):
            continue
        base = {
            "volatility_model": model_id,
            "variant_id": _variant_id(model_id),
            "decision_date": decision_date,
            "effective_trading_date": effective_date,
            "cycle_id": str(cycle_id),
            "option_series": str(series),
            "strike": float(row["strike"]),
        }
        try:
            frame = surface_by_date.get(decision_date)
            if frame is None:
                raise ValueError("no completed-session surface")
            surface = _surface_from_frame(frame, decision_date)
            fitted = fit_volatility_model(
                surface,
                model_id=model_id,
                held_series=str(series),
            )
            held = [point for point in surface.points if point.series == str(series)]
            if not held:
                raise ValueError("held-series points are missing")
            years = float(np.median([point.years_to_expiry for point in held]))
            delta = fitted.straddle_delta(
                forward=surface.forward,
                strike=float(row["strike"]),
                years_to_expiry=years,
            )
            target = _round_nearest_contract(
                -delta
                * OPTION_MULTIPLIER
                / FUTURES_MULTIPLIERS[HEDGE_PRODUCT]
            )
            diagnostics = fitted.diagnostics()
            parameters = diagnostics.pop("parameters")
            held_points = sum(
                point.series == str(series) for point in surface.points
            )
            calibration = {
                **base,
                **diagnostics,
                "status": "success",
                "error": None,
                "surface_forward": surface.forward,
                "surface_points_total": len(surface.points),
                "surface_maturities_total": surface.maturity_count,
                "held_series_points": held_points,
                "parameter_at_bound": _parameter_at_bound(model_id, parameters),
                "parameters_json": json.dumps(
                    parameters,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "model_delta": delta,
                "target_mtx_contracts": target,
            }
        except Exception as exc:  # Fail closed to zero hedge, retain audit row.
            calibration = {
                **base,
                "volatility_model_label": VOLATILITY_MODEL_LABELS[model_id],
                "implementation_level": VOLATILITY_MODEL_IMPLEMENTATION[model_id],
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "surface_forward": None,
                "surface_points_total": 0,
                "surface_maturities_total": 0,
                "held_series_points": 0,
                "calibration_points": 0,
                "calibration_maturities": 0,
                "calibration_rmse_iv": None,
                "maximum_calibration_staleness_seconds": None,
                "parameter_at_bound": False,
                "parameters_json": None,
                "model_delta": None,
                "target_mtx_contracts": 0,
            }
        calibrations.append(calibration)
        signals.append(
            {
                **base,
                "status": calibration["status"],
                "model_delta": calibration["model_delta"],
                "target_mtx_contracts": calibration["target_mtx_contracts"],
            }
        )
    return model_id, signals, calibrations


def _future_trade(
    *,
    trading_date: date,
    variant_id: str,
    contract_month: str,
    price: float,
    delta_contracts: int,
    reason: str,
    execution_phase: str,
    decision_date: date,
    terminal_proxy: bool,
) -> dict[str, Any]:
    quantity = abs(int(delta_contracts))
    multiplier = float(FUTURES_MULTIPLIERS[HEDGE_PRODUCT])
    fee = quantity * float(FIXED_FEES_PER_CONTRACT_SIDE[HEDGE_PRODUCT])
    tax_rate = stock_index_futures_tax_rate(trading_date)
    tax = quantity * taifex_tax_per_contract_twd(
        price,
        multiplier_twd_per_point=multiplier,
        tax_rate=tax_rate,
    )
    return {
        "trading_date": trading_date,
        "variant_id": variant_id,
        "instrument_type": "future",
        "product": HEDGE_PRODUCT,
        "contract_month": contract_month,
        "decision_date": decision_date,
        "execution_phase": execution_phase,
        "price_points": price,
        "delta_contracts": int(delta_contracts),
        "reason": reason,
        "terminal_mark_proxy": terminal_proxy,
        "gross_cash_flow_twd": -int(delta_contracts) * price * multiplier,
        "fixed_fee_twd": fee,
        "transaction_tax_rate": tax_rate,
        "transaction_tax_twd": tax,
        "slippage_cost_twd": 0.0,
        "net_cash_flow_twd": -int(delta_contracts) * price * multiplier - fee - tax,
    }


def _classic_daily_rows(baseline: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in baseline.iter_rows(named=True):
        fee = float(row["commission_twd"])
        tax = float(row["transaction_tax_twd"])
        net = float(row["net_after_fee_and_tax_twd"])
        rows.append(
            {
                "trading_date": row["trading_date"],
                "variant_id": CLASSIC_VARIANT_ID,
                "volatility_model": None,
                "cycle_id": row["cycle_id"],
                "option_series": row["option_series"],
                "strike": row["strike"],
                "is_entry_session": row["is_entry_session"],
                "is_expiry_session": row["is_expiry_session"],
                "option_net_pnl_twd": net,
                "futures_gross_pnl_twd": 0.0,
                "gross_pnl_twd": net + fee + tax,
                "fixed_fees_twd": fee,
                "transaction_tax_twd": tax,
                "slippage_cost_twd": 0.0,
                "net_pnl_twd": net,
                "future_position_eod": 0,
                "option_trade_sides": int(round(fee / 22.0)),
                "futures_trade_sides": 0,
                "hedge_count": 0,
                "calibration_status": None,
            }
        )
    return rows


def _build_model_ledger(
    *,
    model_id: str,
    signals: Sequence[Mapping[str, Any]],
    baseline: pl.DataFrame,
    futures: pl.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    variant_id = _variant_id(model_id)
    signal_by_effective = {
        row["effective_trading_date"]: row for row in signals
    }
    futures_by_date = {
        row["date"]: row for row in futures.iter_rows(named=True)
    }
    baseline_rows = baseline.iter_rows(named=True)
    ordered = list(baseline_rows)
    position = 0
    previous_close: float | None = None
    previous_contract: str | None = None
    daily_rows: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    for index, row in enumerate(ordered):
        trading_date = row["trading_date"]
        future = futures_by_date.get(trading_date)
        if future is None:
            raise ValueError(f"missing MTX daily row on {trading_date}")
        contract_month = str(future["contract_month"])
        open_price = float(future["open"])
        close_price = float(future["close"])
        old_position = position
        if old_position and previous_contract != contract_month:
            raise ValueError(
                f"unclosed MTX roll: {previous_contract}->{contract_month} on {trading_date}"
            )
        gross_future = 0.0
        if old_position and previous_close is not None:
            gross_future += (
                old_position
                * (open_price - previous_close)
                * FUTURES_MULTIPLIERS[HEDGE_PRODUCT]
            )
        day_trades: list[dict[str, Any]] = []
        signal = signal_by_effective.get(trading_date)
        target = 0
        if (
            signal is not None
            and row["cycle_id"] is not None
            and str(signal["cycle_id"]) == str(row["cycle_id"])
        ):
            target = int(signal["target_mtx_contracts"])
        change = target - position
        if change:
            trade = _future_trade(
                trading_date=trading_date,
                variant_id=variant_id,
                contract_month=contract_month,
                price=open_price,
                delta_contracts=change,
                reason="prior_completed_session_model_delta",
                execution_phase="official_day_session_open_proxy",
                decision_date=signal["decision_date"] if signal is not None else trading_date,
                terminal_proxy=False,
            )
            day_trades.append(trade)
            position = target
        gross_future += (
            position
            * (close_price - open_price)
            * FUTURES_MULTIPLIERS[HEDGE_PRODUCT]
        )

        next_contract = None
        if index + 1 < len(ordered):
            next_future = futures_by_date.get(ordered[index + 1]["trading_date"])
            next_contract = None if next_future is None else str(next_future["contract_month"])
        terminal_reason: str | None = None
        if bool(row["is_expiry_session"]):
            terminal_reason = "close_hedge_at_option_expiry_daily_close_proxy"
        elif next_contract is not None and next_contract != contract_month:
            terminal_reason = "close_hedge_before_front_month_roll_daily_close_proxy"
        elif index + 1 == len(ordered):
            terminal_reason = "close_hedge_at_sample_end_daily_close_proxy"
        if terminal_reason is not None and position:
            trade = _future_trade(
                trading_date=trading_date,
                variant_id=variant_id,
                contract_month=contract_month,
                price=close_price,
                delta_contracts=-position,
                reason=terminal_reason,
                execution_phase="official_day_session_close_terminal_proxy",
                decision_date=trading_date,
                terminal_proxy=True,
            )
            day_trades.append(trade)
            position = 0

        day_fee = sum(float(trade["fixed_fee_twd"]) for trade in day_trades)
        day_tax = sum(float(trade["transaction_tax_twd"]) for trade in day_trades)
        future_net = gross_future - day_fee - day_tax
        option_fee = float(row["commission_twd"])
        option_tax = float(row["transaction_tax_twd"])
        option_net = float(row["net_after_fee_and_tax_twd"])
        option_gross = option_net + option_fee + option_tax
        active_signal = signal_by_effective.get(
            ordered[index + 1]["trading_date"]
            if index + 1 < len(ordered)
            else trading_date
        )
        daily_rows.append(
            {
                "trading_date": trading_date,
                "variant_id": variant_id,
                "volatility_model": model_id,
                "cycle_id": row["cycle_id"],
                "option_series": row["option_series"],
                "strike": row["strike"],
                "is_entry_session": row["is_entry_session"],
                "is_expiry_session": row["is_expiry_session"],
                "option_net_pnl_twd": option_net,
                "futures_gross_pnl_twd": gross_future,
                "gross_pnl_twd": option_gross + gross_future,
                "fixed_fees_twd": option_fee + day_fee,
                "transaction_tax_twd": option_tax + day_tax,
                "slippage_cost_twd": 0.0,
                "net_pnl_twd": option_net + future_net,
                "future_position_eod": position,
                "option_trade_sides": int(round(option_fee / 22.0)),
                "futures_trade_sides": sum(
                    abs(int(trade["delta_contracts"])) for trade in day_trades
                ),
                "hedge_count": len(day_trades),
                "calibration_status": (
                    None if active_signal is None else active_signal["status"]
                ),
            }
        )
        trades.extend(day_trades)
        previous_close = close_price
        previous_contract = contract_month
    if position != 0:
        raise ValueError(f"sample-end MTX position is not flat for {variant_id}: {position}")
    ledger_net = sum(float(row["net_cash_flow_twd"]) for row in trades)
    daily_future_net = sum(
        float(row["net_pnl_twd"]) - float(row["option_net_pnl_twd"])
        for row in daily_rows
    )
    if not math.isclose(ledger_net, daily_future_net, abs_tol=1e-6):
        raise ValueError(
            f"MTX cash ledger mismatch for {variant_id}: {ledger_net}/{daily_future_net}"
        )
    return daily_rows, trades


def _capital_and_metrics(
    daily: pl.DataFrame,
    *,
    maximum_opening_option_premium_twd: float,
    futures: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, float]:
    future_close = {
        row["date"]: max(float(row["open"]), float(row["close"]))
        for row in futures.iter_rows(named=True)
    }
    peak_future_notional = 0.0
    for row in daily.filter(pl.col("variant_id") != CLASSIC_VARIANT_ID).iter_rows(
        named=True
    ):
        peak_future_notional = max(
            peak_future_notional,
            abs(int(row["future_position_eod"]))
            * future_close[row["trading_date"]]
            * FUTURES_MULTIPLIERS[HEDGE_PRODUCT],
        )
    common_capital = math.ceil(
        (maximum_opening_option_premium_twd + peak_future_notional) / 1_000.0
    ) * 1_000.0
    if common_capital <= 0.0:
        raise ValueError("common fully cash-secured capital is not positive")

    normalized_frames: list[pl.DataFrame] = []
    metric_rows: list[dict[str, Any]] = []
    annual_frames: list[pl.DataFrame] = []
    for frame in daily.partition_by("variant_id", maintain_order=True):
        frame = frame.sort("trading_date")
        variant_id = str(frame.item(0, "variant_id"))
        pnl = frame.get_column("net_pnl_twd").to_numpy().astype(np.float64)
        returns = pnl / common_capital
        cumulative_pnl = np.cumsum(pnl)
        wealth = 1.0 + cumulative_pnl / common_capital
        running_peak = np.maximum.accumulate(
            np.concatenate([np.asarray([1.0]), wealth])
        )[1:]
        drawdown = wealth / running_peak - 1.0
        normalized = frame.with_columns(
            pl.Series("common_capital_twd", np.full(len(frame), common_capital)),
            pl.Series("daily_return_on_common_capital", returns),
            pl.Series("cumulative_net_pnl_twd", cumulative_pnl),
            pl.Series(
                "cumulative_return_on_common_capital",
                cumulative_pnl / common_capital,
            ),
            pl.Series("fixed_capital_wealth", wealth),
            pl.Series("fixed_capital_drawdown", drawdown),
        )
        normalized_frames.append(normalized)
        standard_deviation = float(np.std(returns, ddof=1))
        sharpe = (
            float(np.mean(returns) / standard_deviation * math.sqrt(252.0))
            if standard_deviation > 0.0
            else 0.0
        )
        downside = float(np.sqrt(np.mean(np.square(np.minimum(returns, 0.0)))))
        sortino = (
            float(np.mean(returns) / downside * math.sqrt(252.0))
            if downside > 0.0
            else 0.0
        )
        maximum_drawdown = float(np.min(drawdown))
        cumulative_return = float(cumulative_pnl[-1] / common_capital)
        positive = float(pnl[pnl > 0.0].sum())
        negative = float(-pnl[pnl < 0.0].sum())
        metric_rows.append(
            {
                "variant_id": variant_id,
                "volatility_model": frame.item(0, "volatility_model"),
                "common_capital_twd": common_capital,
                "gross_pnl_twd": float(frame.get_column("gross_pnl_twd").sum()),
                "fixed_fees_twd": float(frame.get_column("fixed_fees_twd").sum()),
                "transaction_tax_twd": float(
                    frame.get_column("transaction_tax_twd").sum()
                ),
                "net_pnl_twd": float(pnl.sum()),
                "cumulative_return_on_common_capital": cumulative_return,
                "annualized_sharpe": sharpe,
                "annualized_sortino": sortino,
                "maximum_drawdown": maximum_drawdown,
                "sample_calmar_nonannualized": (
                    cumulative_return / abs(maximum_drawdown)
                    if maximum_drawdown < 0.0
                    else None
                ),
                "daily_win_rate": float(np.mean(pnl > 0.0)),
                "daily_profit_factor": positive / negative if negative > 0.0 else None,
                "total_option_trade_sides": int(
                    frame.get_column("option_trade_sides").sum()
                ),
                "total_futures_trade_sides": int(
                    frame.get_column("futures_trade_sides").sum()
                ),
                "total_hedge_events": int(frame.get_column("hedge_count").sum()),
            }
        )
        annual_frames.append(
            normalized.with_columns(pl.col("trading_date").dt.year().alias("year"))
            .group_by("year")
            .agg(
                pl.lit(variant_id).alias("variant_id"),
                pl.col("net_pnl_twd").sum().alias("net_pnl_twd"),
                pl.col("fixed_fees_twd").sum().alias("fixed_fees_twd"),
                pl.col("transaction_tax_twd").sum().alias("transaction_tax_twd"),
            )
            .with_columns(
                (pl.col("net_pnl_twd") / common_capital).alias(
                    "return_on_common_capital"
                )
            )
        )
    normalized_daily = pl.concat(normalized_frames).sort(
        ["variant_id", "trading_date"]
    )
    metrics = pl.DataFrame(metric_rows).sort(
        "cumulative_return_on_common_capital",
        descending=True,
    )
    annual = pl.concat(annual_frames).sort(["year", "variant_id"])
    return normalized_daily, metrics, annual, common_capital


def _validate_daily(
    daily: pl.DataFrame,
    metrics: pl.DataFrame,
    trading_dates: Sequence[date],
) -> None:
    expected_variants = 1 + len(VOLATILITY_MODEL_IDS)
    expected_rows = expected_variants * len(trading_dates)
    if daily.height != expected_rows:
        raise ValueError(f"daily coverage mismatch: {daily.height}/{expected_rows}")
    if daily.select(pl.struct(["variant_id", "trading_date"]).n_unique()).item() != expected_rows:
        raise ValueError("daily variant/date keys are not unique")
    if float(daily.get_column("slippage_cost_twd").abs().max()) != 0.0:
        raise ValueError("artificial slippage must remain zero")
    endpoints = daily.group_by("variant_id").agg(
        pl.col("net_pnl_twd").sum().alias("daily_net")
    )
    reconciled = metrics.join(endpoints, on="variant_id", validate="1:1")
    if reconciled.filter(
        (pl.col("net_pnl_twd") - pl.col("daily_net")).abs() > 1e-6
    ).height:
        raise ValueError("metric/daily endpoint mismatch")


def run_backtest(
    *,
    start: date,
    end: date,
    baseline_dir: Path,
    option_raw_root: Path,
    final_settlement_path: Path,
    futures_path: Path,
    output_dir: Path,
    maximum_abs_log_moneyness: float,
    workers: int,
    rebuild_surface_cache: bool,
) -> dict[str, Any]:
    if start != START_DATE or end != END_DATE:
        raise ValueError(
            "this delivery is pinned to the requested 2012-11-21 through 2026-08-07 window"
        )
    baseline_daily_path = baseline_dir / "daily_results.parquet"
    cycles_path = baseline_dir / "cycles.parquet"
    baseline_summary_path = baseline_dir / "summary.json"
    if not all(path.is_file() for path in (baseline_daily_path, cycles_path, baseline_summary_path)):
        raise FileNotFoundError(f"canonical expiry-carry artifacts are incomplete: {baseline_dir}")
    baseline_summary = json.loads(baseline_summary_path.read_text(encoding="utf-8"))
    baseline = pl.read_parquet(baseline_daily_path).filter(
        pl.col("trading_date").is_between(start, end)
    ).sort("trading_date")
    trading_dates = baseline.get_column("trading_date").to_list()
    if not trading_dates or trading_dates[0] != start or trading_dates[-1] != end:
        raise ValueError("canonical daily benchmark does not cover the requested endpoints")
    if len(set(trading_dates)) != len(trading_dates):
        raise ValueError("canonical daily benchmark has duplicate dates")

    futures_all = pl.read_parquet(futures_path).filter(
        pl.col("date").is_between(start, end)
        & pl.col("product").is_in(["TX", HEDGE_PRODUCT])
    ).sort(["date", "product"])
    tx = futures_all.filter(pl.col("product") == "TX")
    mtx = futures_all.filter(pl.col("product") == HEDGE_PRODUCT)
    if tx.height != len(trading_dates) or mtx.height != len(trading_dates):
        raise ValueError(
            f"TX/MTX daily coverage mismatch: TX={tx.height} MTX={mtx.height} dates={len(trading_dates)}"
        )
    if tx.get_column("date").to_list() != trading_dates or mtx.get_column("date").to_list() != trading_dates:
        raise ValueError("TX/MTX daily dates do not align to the canonical option curve")
    tx_close_by_date = {
        row["date"]: float(row["close"]) for row in tx.iter_rows(named=True)
    }

    source_paths = _surface_source_paths(option_raw_root, start, end)
    source_identity = _source_identity(source_paths)
    expiry_by_series = _official_expiry_by_series(final_settlement_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    surface_cache = output_dir / "daily_iv_surface.parquet"
    surface_quality = None if rebuild_surface_cache else _verify_surface_cache(
        surface_cache,
        source_identity=source_identity,
        start=start,
        end=end,
        maximum_abs_log_moneyness=maximum_abs_log_moneyness,
    )
    if surface_quality is None:
        surface_quality = _build_surface_cache(
            source_paths=source_paths,
            target_dates=set(trading_dates),
            tx_close_by_date=tx_close_by_date,
            expiry_by_series=expiry_by_series,
            output_path=surface_cache,
            maximum_abs_log_moneyness=maximum_abs_log_moneyness,
            source_identity=source_identity,
        )
    else:
        print(
            f"[daily-surface] reuse cache rows={surface_quality['surface_rows']:,} "
            f"dates={surface_quality['surface_dates']:,}",
            flush=True,
        )

    worker_count = max(1, min(int(workers), len(VOLATILITY_MODEL_IDS)))
    signals_by_model: dict[str, list[dict[str, Any]]] = {}
    calibration_rows: list[dict[str, Any]] = []
    # Polars owns a native thread pool after the surface build.  Forking that
    # process can strand child workers on inherited futexes, so use fresh
    # interpreters for the model-parallel stage.
    with ProcessPoolExecutor(
        max_workers=worker_count,
        mp_context=mp.get_context("spawn"),
    ) as executor:
        futures_pending = {
            executor.submit(
                _fit_model_history_worker,
                model_id,
                str(surface_cache),
                str(baseline_daily_path),
            ): model_id
            for model_id in VOLATILITY_MODEL_IDS
        }
        for completed in as_completed(futures_pending):
            model_id, signals, calibrations = completed.result()
            signals_by_model[model_id] = signals
            calibration_rows.extend(calibrations)
            successes = sum(row["status"] == "success" for row in calibrations)
            print(
                f"[daily-model] model={model_id} calibrations={len(calibrations):,} "
                f"success={successes:,}",
                flush=True,
            )

    all_daily = _classic_daily_rows(baseline)
    all_trades: list[dict[str, Any]] = []
    all_signals: list[dict[str, Any]] = []
    for model_id in VOLATILITY_MODEL_IDS:
        signals = signals_by_model[model_id]
        model_daily, model_trades = _build_model_ledger(
            model_id=model_id,
            signals=signals,
            baseline=baseline,
            futures=mtx,
        )
        all_daily.extend(model_daily)
        all_trades.extend(model_trades)
        all_signals.extend(signals)

    daily = pl.DataFrame(all_daily, infer_schema_length=None).sort(
        ["variant_id", "trading_date"]
    )
    trades = pl.DataFrame(all_trades, infer_schema_length=None).sort(
        ["variant_id", "trading_date", "execution_phase"]
    )
    signals = pl.DataFrame(all_signals, infer_schema_length=None).sort(
        ["variant_id", "decision_date"]
    )
    calibrations = pl.DataFrame(calibration_rows, infer_schema_length=None).sort(
        ["variant_id", "decision_date"]
    )
    cycles = pl.read_parquet(cycles_path)
    maximum_opening_premium = float(cycles.get_column("opening_premium_twd").max())
    normalized_daily, metrics, annual, common_capital = _capital_and_metrics(
        daily,
        maximum_opening_option_premium_twd=maximum_opening_premium,
        futures=mtx,
    )
    _validate_daily(normalized_daily, metrics, trading_dates)

    artifacts = {
        "daily_results.parquet": normalized_daily,
        "futures_trades.parquet": trades,
        "signals.parquet": signals,
        "calibrations.parquet": calibrations,
        "annual_results.parquet": annual,
    }
    for filename, frame in artifacts.items():
        _atomic_parquet(frame, output_dir / filename)
    metrics.write_csv(output_dir / "metrics.csv")
    annual.write_csv(output_dir / "annual_results.csv")
    calibrations.write_csv(output_dir / "calibrations.csv")

    calibration_quality = calibrations.group_by("volatility_model").agg(
        pl.len().alias("attempts"),
        (pl.col("status") == "success").sum().alias("successes"),
        (pl.col("status") == "failed").sum().alias("failures"),
        pl.col("calibration_rmse_iv").drop_nulls().mean().alias("mean_rmse_iv"),
        pl.col("parameter_at_bound").sum().alias("parameter_bound_hits"),
    ).sort("volatility_model")
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    summary: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": generated_at,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "trading_days": len(trading_dates),
        "complete_option_cycles": int(cycles.height),
        "model_count": len(VOLATILITY_MODEL_IDS),
        "variant_count_including_classic": 1 + len(VOLATILITY_MODEL_IDS),
        "baseline_summary": str(baseline_summary_path),
        "baseline_summary_sha256": _sha256_path(baseline_summary_path),
        "baseline_net_pnl_twd": baseline_summary.get("net_pnl_twd"),
        "surface_cache": str(surface_cache),
        "surface_cache_sha256": _sha256_path(surface_cache),
        "surface_source_identity": source_identity,
        "surface_quality": surface_quality,
        "common_fully_cash_secured_capital_twd": common_capital,
        "maximum_opening_option_premium_twd": maximum_opening_premium,
        "execution_contract": {
            "option_ledger": "exact canonical weekly opening ATM straddle hold-to-official-expiry daily ledger",
            "calibration_information": "completed official TXO daily settlement, else that contract daily last trade",
            "calibration_time": "after completed session; represented as 13:45 Asia/Taipei",
            "hedge_execution": "next verified MTX day-session official open",
            "expiry_and_roll_close": "same-day MTX official close terminal proxy",
            "historical_synchronized_bidask_available": False,
            "one_lot_option_pair": True,
            "quantity_or_depth_constraint": False,
            "artificial_slippage_twd": 0.0,
            "pressure_tests": "not run",
        },
        "cost_contract": {
            "TXO_fixed_fee_per_contract_side_twd": 22.0,
            "MTX_fixed_fee_per_contract_side_twd": float(
                FIXED_FEES_PER_CONTRACT_SIDE[HEDGE_PRODUCT]
            ),
            "statutory_option_and_futures_transaction_tax_included": True,
        },
        "data_contract": {
            "option_daily_price_source": TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
            "futures_path": str(futures_path),
            "futures_sha256": _sha256_path(futures_path),
            "final_settlement_path": str(final_settlement_path),
            "final_settlement_sha256": _sha256_path(final_settlement_path),
            "raw_receipt_count": len(source_paths),
        },
        "model_implementations": [
            {
                "model_id": model_id,
                "label": VOLATILITY_MODEL_LABELS[model_id],
                "implementation_level": VOLATILITY_MODEL_IMPLEMENTATION[model_id],
            }
            for model_id in VOLATILITY_MODEL_IDS
        ],
        "calibration_quality": calibration_quality.to_dicts(),
        "results": metrics.to_dicts(),
        "artifacts": {},
    }
    artifact_names = [
        *artifacts,
        "metrics.csv",
        "annual_results.csv",
        "calibrations.csv",
        "daily_iv_surface.parquet",
    ]
    for filename in artifact_names:
        path = output_dir / filename
        summary["artifacts"][filename] = {
            "path": str(path),
            "sha256": _sha256_path(path),
        }
    _atomic_json(output_dir / "summary.json", summary)
    receipt = {
        "schema_version": 1,
        "status": "complete",
        "generated_at_utc": generated_at,
        "summary": str(output_dir / "summary.json"),
        "summary_sha256": _sha256_path(output_dir / "summary.json"),
        "checks": {
            "requested_endpoint_coverage": True,
            "classic_option_daily_ledger_reused": True,
            "completed_session_to_next_session_open": True,
            "sample_end_futures_positions_flat": True,
            "daily_and_cash_ledgers_reconciled": True,
            "common_capital_for_all_variants": True,
            "historical_synchronized_bidask_claimed": False,
            "pressure_tests_run": False,
        },
    }
    _atomic_json(output_dir / "receipt.json", receipt)
    print(
        json.dumps(
            {
                "status": "complete",
                "date_start": start.isoformat(),
                "date_end": end.isoformat(),
                "trading_days": len(trading_dates),
                "models": len(VOLATILITY_MODEL_IDS),
                "surface_rows": surface_quality["surface_rows"],
                "output_dir": str(output_dir),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return summary


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=_parse_date, default=START_DATE)
    parser.add_argument("--end", type=_parse_date, default=END_DATE)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE_DIR)
    parser.add_argument("--option-raw-root", type=Path, default=DEFAULT_OPTION_RAW_ROOT)
    parser.add_argument(
        "--final-settlement-path",
        type=Path,
        default=DEFAULT_FINAL_SETTLEMENT,
    )
    parser.add_argument("--futures-path", type=Path, default=DEFAULT_FUTURES_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--maximum-abs-log-moneyness", type=float, default=0.12)
    parser.add_argument(
        "--workers",
        type=int,
        default=min(6, os.cpu_count() or 1),
    )
    parser.add_argument("--rebuild-surface-cache", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_backtest(
        start=args.start,
        end=args.end,
        baseline_dir=args.baseline_dir,
        option_raw_root=args.option_raw_root,
        final_settlement_path=args.final_settlement_path,
        futures_path=args.futures_path,
        output_dir=args.output_dir,
        maximum_abs_log_moneyness=float(args.maximum_abs_log_moneyness),
        workers=int(args.workers),
        rebuild_surface_cache=bool(args.rebuild_surface_cache),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
