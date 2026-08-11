#!/usr/bin/env python3
"""Compare six causal volatility-delta models on one carried TXO straddle.

The option ledger is copied exactly from the canonical expiry-carry classic
ATM straddle.  Every model therefore owns the same one-lot Call and Put; only
the TMF delta hedge differs.  This isolates the model contribution without
creating a second option entry, settlement, fee, tax, or capital implementation.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timezone
import json
import math
from pathlib import Path
import sys
from typing import Any, Final, Mapping, Sequence

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_atm_straddle_rolling import (  # noqa: E402
    _build_day_market,
    _datetime_from_ns,
    _ns,
    _parse_time,
    _sha256_path,
    _verify_manifest,
)
from scripts.backtest_taifex_option_benchmarks import (  # noqa: E402
    FIXED_FEES_PER_CONTRACT_SIDE,
    FUTURES_MULTIPLIERS,
    OPTION_MULTIPLIER,
    Variant,
    _future_trade,
    _parameter_json,
    _round_nearest_contract,
)
from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    TAIFEX_TRADE_PROXY_SOURCE,
    TAIPEI,
    _atomic_json,
    _atomic_parquet,
)
from stockagent.research.taifex_capital_returns import (  # noqa: E402
    build_capital_normalized_returns,
)
from stockagent.research.taifex_volatility_models import (  # noqa: E402
    OBSERVABILITY_DELAY_NS,
    SECONDS_PER_YEAR,
    VOLATILITY_MODEL_IDS,
    VOLATILITY_MODEL_IMPLEMENTATION,
    VOLATILITY_MODEL_LABELS,
    extract_causal_iv_surface,
    fit_volatility_model,
)


OUTPUT_SCHEMA_VERSION: Final[int] = 1
FAMILY_VOLATILITY_GAMMA: Final[str] = "volatility_model_gamma"
CLASSIC_VARIANT_ID: Final[str] = "classic_opening_straddle"
HEDGE_PRODUCT: Final[str] = "TMF"
BASELINE_DIR: Final[Path] = Path(
    "artifacts/research/taifex_option_benchmarks_expiry_carry"
)
DEFAULT_OUTPUT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_volatility_model_gamma"
)
EXPIRY_SETTLEMENT_TIME: Final[time] = time(13, 30)
EXPIRY_HEDGE_CLOSE_TIME: Final[time] = time(13, 25)
SESSION_MARK_TIME: Final[time] = time(13, 45)


def _model_variant(model_id: str, *, hedge_interval_minutes: int) -> Variant:
    return Variant(
        family=FAMILY_VOLATILITY_GAMMA,
        variant_id=f"vol_model_gamma__{model_id}",
        role="candidate",
        parameters={
            "volatility_model": model_id,
            "volatility_model_label": VOLATILITY_MODEL_LABELS[model_id],
            "implementation_level": VOLATILITY_MODEL_IMPLEMENTATION[model_id],
            "hedge_product": HEDGE_PRODUCT,
            "hedge_interval_minutes": int(hedge_interval_minutes),
            "option_contracts_per_leg": 1,
            "option_multiplier_twd_per_point": OPTION_MULTIPLIER,
            "futures_multiplier_twd_per_point": FUTURES_MULTIPLIERS[HEDGE_PRODUCT],
        },
    )


def _ceil_to_minute(event_ns: int) -> int:
    minute_ns = 60 * 1_000_000_000
    return ((int(event_ns) + minute_ns - 1) // minute_ns) * minute_ns


def _daily_decisions(start_ns: int, end_ns: int, interval_minutes: int) -> range:
    if interval_minutes <= 0:
        raise ValueError("hedge_interval_minutes must be positive")
    step_ns = interval_minutes * 60 * 1_000_000_000
    return range(start_ns, end_ns + 1, step_ns)


def _retag_option_trade(row: Mapping[str, Any], variant: Variant) -> dict[str, Any]:
    tagged = dict(row)
    tagged.update(
        {
            "strategy": variant.variant_id,
            "benchmark_family": variant.family,
            "variant_id": variant.variant_id,
            "variant_role": variant.role,
            "parameters_json": _parameter_json(variant),
            "model_delta": None,
            "target_tmf_contracts": None,
            "observed_forward": None,
        }
    )
    return tagged


def _parameter_at_bound(model_id: str, parameters: Mapping[str, Any]) -> bool:
    tolerance = 2e-3
    bounds: dict[str, tuple[float, float]] = {
        "rho": (-0.999, 0.999),
        "nu": (1e-4, 10.0),
        "hurst": (0.03, 0.49),
    }
    relevant = {
        "sabr_hagan_beta1": ("rho", "nu"),
        "rough_vol_power_law_proxy": ("hurst",),
    }.get(model_id, ())
    return any(
        abs(float(parameters[key]) - low) <= tolerance * max(1.0, abs(low))
        or abs(float(parameters[key]) - high)
        <= tolerance * max(1.0, abs(high))
        for key in relevant
        for low, high in (bounds[key],)
    )


def _copy_classic_daily_for_model(
    classic_row: Mapping[str, Any],
    *,
    variant: Variant,
    future_daily_gross: float,
    future_daily_fees: float,
    future_daily_taxes: float,
    future_position: int,
    future_trade_rows: Sequence[Mapping[str, Any]],
    calibration: Mapping[str, Any] | None,
) -> dict[str, Any]:
    gross = float(classic_row["gross_pnl_twd"]) + future_daily_gross
    fees = float(classic_row["fixed_fees_twd"]) + future_daily_fees
    taxes = float(classic_row["transaction_tax_twd"]) + future_daily_taxes
    slippage = float(classic_row["slippage_cost_twd"])
    future_sides = sum(abs(int(row["delta_contracts"])) for row in future_trade_rows)
    diagnostics = json.loads(str(classic_row["diagnostics_json"]))
    diagnostics.update(
        {
            "volatility_model": variant.parameters["volatility_model"],
            "implementation_level": variant.parameters["implementation_level"],
            "hedge_product": HEDGE_PRODUCT,
            "historical_bidask_available": False,
            "execution_price_source": "first_strictly_later_transaction_print",
            "calibration": dict(calibration) if calibration is not None else None,
            "future_position_eod": int(future_position),
        }
    )
    cycle_id = classic_row.get("cycle_id")
    return {
        **dict(classic_row),
        "benchmark_family": variant.family,
        "variant_id": variant.variant_id,
        "variant_role": variant.role,
        "parameters_json": _parameter_json(variant),
        "cycle_id": (
            str(cycle_id).replace(CLASSIC_VARIANT_ID, variant.variant_id, 1)
            if cycle_id is not None
            else None
        ),
        "position_carried_overnight": bool(
            classic_row["position_carried_overnight"] or future_position
        ),
        "hedge_count": len(future_trade_rows),
        "trade_sides": int(classic_row["option_trade_sides"]) + future_sides,
        "futures_trade_sides": future_sides,
        "gross_pnl_twd": gross,
        "fixed_fees_twd": fees,
        "transaction_tax_twd": taxes,
        "net_after_fee_twd": gross - fees,
        "net_after_fee_tax_twd": gross - fees - taxes,
        "net_pnl_twd": gross - fees - taxes - slippage,
        "diagnostics_json": json.dumps(
            diagnostics,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    }


def _append_risk_metrics(
    metrics: pl.DataFrame,
    normalized_daily: pl.DataFrame,
    daily: pl.DataFrame,
) -> pl.DataFrame:
    cost_rows = daily.group_by("variant_id").agg(
        pl.col("gross_pnl_twd").sum().alias("gross_pnl_twd"),
        pl.col("fixed_fees_twd").sum().alias("fixed_fees_twd"),
        pl.col("transaction_tax_twd").sum().alias("transaction_tax_twd"),
        pl.col("net_pnl_twd").sum().alias("net_pnl_twd"),
        pl.col("trade_sides").sum().alias("total_trade_sides"),
        pl.col("option_trade_sides").sum().alias("total_option_trade_sides"),
        pl.col("futures_trade_sides").sum().alias("total_futures_trade_sides"),
        pl.col("hedge_count").sum().alias("total_hedge_events"),
    )
    extra: list[dict[str, Any]] = []
    for frame in normalized_daily.partition_by("variant_id", maintain_order=True):
        variant_id = str(frame.item(0, "variant_id"))
        returns = frame.get_column("daily_return_on_capital").to_numpy().astype(np.float64)
        negative = np.minimum(returns, 0.0)
        downside = float(np.sqrt(np.mean(np.square(negative))))
        sortino = (
            float(np.mean(returns) / downside * math.sqrt(252.0))
            if downside > 0.0
            else 0.0
        )
        cumulative = float(frame.item(-1, "cumulative_return_on_capital"))
        maximum_drawdown = float(frame.get_column("fixed_capital_drawdown_return").min())
        calmar = cumulative / abs(maximum_drawdown) if maximum_drawdown < 0.0 else None
        positive = float(returns[returns > 0.0].sum())
        negative_sum = float(-returns[returns < 0.0].sum())
        extra.append(
            {
                "variant_id": variant_id,
                "annualized_sortino": sortino,
                "sample_calmar_nonannualized": calmar,
                "win_rate": float(np.mean(returns > 0.0)),
                "profit_factor": positive / negative_sum if negative_sum > 0.0 else None,
            }
        )
    return (
        metrics.join(cost_rows, on="variant_id", how="left", validate="1:1")
        .join(pl.DataFrame(extra), on="variant_id", how="left", validate="1:1")
        .sort("cumulative_return_on_capital", descending=True)
    )


def _validate_results(
    *,
    daily: pl.DataFrame,
    trades: pl.DataFrame,
    classic_option_trades: pl.DataFrame,
    variants: Sequence[Variant],
    trading_dates: Sequence[date],
) -> None:
    expected_variants = 1 + len(variants)
    expected_daily = expected_variants * len(trading_dates)
    if daily.height != expected_daily:
        raise ValueError(f"daily coverage mismatch: {daily.height}/{expected_daily}")
    if daily.select(pl.struct(["variant_id", "trading_date"]).n_unique()).item() != expected_daily:
        raise ValueError("daily variant/date keys are not unique and complete")
    if float(trades.get_column("slippage_cost_twd").abs().max()) != 0.0:
        raise ValueError("artificial slippage must remain zero")
    causal = trades.filter(pl.col("reason") != "official_expiry_cash_settlement")
    if causal.filter(pl.col("fill_ts") <= pl.col("decision_ts")).height:
        raise ValueError("a non-terminal fill is not strictly later than its decision")

    identity_columns = [
        "trading_date",
        "series",
        "strike",
        "option_right",
        "decision_ts",
        "fill_ts",
        "price_points",
        "delta_contracts",
        "gross_cash_flow_twd",
        "fixed_fee_twd",
        "transaction_tax_twd",
        "reason",
    ]
    expected_options = classic_option_trades.select(identity_columns).sort(identity_columns)
    for variant in variants:
        observed = (
            trades.filter(
                (pl.col("variant_id") == variant.variant_id)
                & (pl.col("instrument_type") == "option")
            )
            .select(identity_columns)
            .sort(identity_columns)
        )
        if not observed.equals(expected_options, null_equal=True):
            raise ValueError(f"option ledger diverged from classic: {variant.variant_id}")

    positions = (
        trades.group_by(
            ["variant_id", "instrument_type", "product", "series", "strike", "option_right"]
        )
        .agg(pl.col("delta_contracts").sum().alias("contracts"))
        .filter(pl.col("contracts") != 0)
    )
    if positions.height:
        raise ValueError(f"sample-end positions are not flat: {positions.head(8)}")
    for frame in daily.partition_by("variant_id", maintain_order=True):
        variant_id = str(frame.item(0, "variant_id"))
        ledger = trades.filter(pl.col("variant_id") == variant_id)
        ledger_net = float(
            ledger.get_column("gross_cash_flow_twd").sum()
            - ledger.get_column("fixed_fee_twd").sum()
            - ledger.get_column("transaction_tax_twd").sum()
            - ledger.get_column("slippage_cost_twd").sum()
        )
        daily_net = float(frame.get_column("net_pnl_twd").sum())
        if not math.isclose(ledger_net, daily_net, abs_tol=1e-6):
            raise ValueError(
                f"daily/ledger endpoint mismatch: {variant_id} {daily_net}/{ledger_net}"
            )


def run_backtest(
    *,
    raw_root: Path,
    baseline_dir: Path,
    output_dir: Path,
    calibration_time: time,
    hedge_end_time: time,
    hedge_interval_minutes: int,
) -> dict[str, Any]:
    manifest, manifest_sha, verified_dates = _verify_manifest(raw_root)
    summary_path = baseline_dir / "summary.json"
    baseline_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if baseline_summary.get("status") != "complete":
        raise ValueError("canonical expiry-carry benchmark is not complete")
    if baseline_summary.get("source_manifest_sha256") != manifest_sha:
        raise ValueError("baseline/source manifest identity mismatch")

    baseline_daily = pl.read_parquet(baseline_dir / "daily_benchmarks.parquet")
    baseline_trades = pl.read_parquet(baseline_dir / "trades.parquet")
    classic_daily = (
        baseline_daily.filter(pl.col("variant_id") == CLASSIC_VARIANT_ID)
        .sort("trading_date")
    )
    classic_trades = (
        baseline_trades.filter(pl.col("variant_id") == CLASSIC_VARIANT_ID)
        .sort(["trading_date", "fill_ts", "option_right"])
    )
    classic_option_trades = classic_trades.filter(pl.col("instrument_type") == "option")
    trading_dates = classic_daily.get_column("trading_date").to_list()
    if trading_dates != verified_dates:
        raise ValueError("classic daily coverage does not equal the verified tick manifest")

    variants = [
        _model_variant(model_id, hedge_interval_minutes=hedge_interval_minutes)
        for model_id in VOLATILITY_MODEL_IDS
    ]
    future_positions = {variant.variant_id: 0 for variant in variants}
    cumulative_future_gross_cash = {variant.variant_id: 0.0 for variant in variants}
    cumulative_future_fees = {variant.variant_id: 0.0 for variant in variants}
    cumulative_future_taxes = {variant.variant_id: 0.0 for variant in variants}
    previous_future_equity = {variant.variant_id: 0.0 for variant in variants}

    all_trade_rows = classic_trades.to_dicts()
    for variant in variants:
        all_trade_rows.extend(
            _retag_option_trade(row, variant)
            for row in classic_option_trades.iter_rows(named=True)
        )
    all_daily_rows = classic_daily.to_dicts()
    calibration_rows: list[dict[str, Any]] = []

    opening_fills: dict[date, int] = {}
    for frame in classic_option_trades.filter(
        pl.col("reason") == "open_atm_straddle"
    ).partition_by("trading_date", maintain_order=True):
        latest = max(_ns(value) for value in frame.get_column("fill_ts").to_list())
        opening_fills[frame.item(0, "trading_date")] = latest

    for day_index, classic_row in enumerate(classic_daily.iter_rows(named=True), start=1):
        trading_date = classic_row["trading_date"]
        print(
            f"[vol-model-gamma] date={trading_date} "
            f"progress={day_index}/{len(trading_dates)} models={len(variants)}",
            flush=True,
        )
        market = _build_day_market(
            raw_root,
            trading_date,
            futures_products=("TX", HEDGE_PRODUCT),
        )
        series = classic_row["option_series"]
        calibration_by_variant: dict[str, Mapping[str, Any] | None] = {
            variant.variant_id: None for variant in variants
        }
        future_trades_by_variant: dict[str, list[dict[str, Any]]] = {
            variant.variant_id: [] for variant in variants
        }
        if series is not None:
            call_strike = float(classic_row["opening_call_strike"])
            put_strike = float(classic_row["opening_put_strike"])
            if not math.isclose(call_strike, put_strike, abs_tol=1e-9):
                raise ValueError(f"classic straddle strikes diverged on {trading_date}")
            strike = call_strike
            expiry = classic_row["option_expiry"]
            calibration_ns = _ns(
                datetime.combine(trading_date, calibration_time, tzinfo=TAIPEI)
            )
            if trading_date in opening_fills:
                calibration_ns = max(
                    calibration_ns,
                    _ceil_to_minute(opening_fills[trading_date] + 1_000_000_000),
                )
            expiry_ns = _ns(
                datetime.combine(expiry, EXPIRY_SETTLEMENT_TIME, tzinfo=TAIPEI)
            )
            is_expiry = bool(classic_row["is_expiry_session"])
            hedge_end_ns = _ns(
                datetime.combine(
                    trading_date,
                    time(13, 24) if is_expiry else hedge_end_time,
                    tzinfo=TAIPEI,
                )
            )
            surface = extract_causal_iv_surface(
                market,
                calibration_decision_ns=calibration_ns,
                # The canonical carry ledger follows official settlement.
                # This matters when a scheduled weekly expiry is postponed by
                # a market closure (202607F2 settled on 2026-07-13 rather than
                # its name-derived 2026-07-10 date).
                expiry_overrides={str(series): expiry},
            )
            for variant in variants:
                model_id = str(variant.parameters["volatility_model"])
                fitted = fit_volatility_model(
                    surface,
                    model_id=model_id,
                    held_series=str(series),
                )
                diagnostics = fitted.diagnostics()
                at_bound = _parameter_at_bound(model_id, fitted.parameters)
                fitted_parameters = diagnostics.pop("parameters")
                calibration = {
                    **diagnostics,
                    "trading_date": trading_date.isoformat(),
                    "series": str(series),
                    "strike": strike,
                    "calibration_decision_ts": _datetime_from_ns(
                        calibration_ns
                    ).isoformat(),
                    "observable_through_ts": _datetime_from_ns(
                        calibration_ns - OBSERVABILITY_DELAY_NS
                    ).isoformat(),
                    "surface_forward": surface.forward,
                    "surface_points_total": len(surface.points),
                    "surface_maturities_total": surface.maturity_count,
                    "parameter_at_bound": at_bound,
                    "parameters_json": json.dumps(
                        fitted_parameters,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
                calibration_by_variant[variant.variant_id] = calibration
                calibration_rows.append(calibration)
                available_after_ns = calibration_ns
                for decision_ns in _daily_decisions(
                    calibration_ns,
                    hedge_end_ns,
                    hedge_interval_minutes,
                ):
                    if decision_ns < available_after_ns:
                        continue
                    observed_ns = decision_ns - OBSERVABILITY_DELAY_NS
                    forward = market.underlying_at_or_before(observed_ns)
                    if forward is None:
                        raise ValueError(
                            f"{trading_date}: no causal TX mark at "
                            f"{_datetime_from_ns(decision_ns)}"
                        )
                    years = (expiry_ns - decision_ns) / 1_000_000_000.0 / SECONDS_PER_YEAR
                    if years <= 0.0:
                        break
                    model_delta = fitted.straddle_delta(
                        forward=float(forward),
                        strike=strike,
                        years_to_expiry=years,
                    )
                    target = _round_nearest_contract(
                        -model_delta
                        * OPTION_MULTIPLIER
                        / FUTURES_MULTIPLIERS[HEDGE_PRODUCT]
                    )
                    current = future_positions[variant.variant_id]
                    change = target - current
                    if change == 0:
                        continue
                    fill = market.first_future_trade_after(
                        HEDGE_PRODUCT,
                        decision_ns,
                        before_ns=expiry_ns if is_expiry else None,
                    )
                    if fill is None:
                        raise ValueError(
                            f"{trading_date}: no strictly later {HEDGE_PRODUCT} hedge fill"
                        )
                    trade = _future_trade(
                        market=market,
                        variant=variant,
                        product=HEDGE_PRODUCT,
                        fill=fill,
                        delta_contracts=change,
                        reason="volatility_model_delta_hedge",
                        decision_ns=decision_ns,
                    )
                    trade.update(
                        {
                            "model_delta": model_delta,
                            "target_tmf_contracts": target,
                            "observed_forward": float(forward),
                        }
                    )
                    future_trades_by_variant[variant.variant_id].append(trade)
                    future_positions[variant.variant_id] = target
                    available_after_ns = fill.event_ns

            if is_expiry:
                close_decision_ns = _ns(
                    datetime.combine(
                        trading_date,
                        EXPIRY_HEDGE_CLOSE_TIME,
                        tzinfo=TAIPEI,
                    )
                )
                for variant in variants:
                    current = future_positions[variant.variant_id]
                    if current == 0:
                        continue
                    fill = market.first_future_trade_after(
                        HEDGE_PRODUCT,
                        close_decision_ns,
                        before_ns=expiry_ns,
                    )
                    if fill is None:
                        raise ValueError(f"{trading_date}: no expiry TMF close fill")
                    trade = _future_trade(
                        market=market,
                        variant=variant,
                        product=HEDGE_PRODUCT,
                        fill=fill,
                        delta_contracts=-current,
                        reason="close_delta_hedge_before_official_expiry",
                        decision_ns=close_decision_ns,
                    )
                    trade.update(
                        {
                            "model_delta": None,
                            "target_tmf_contracts": 0,
                            "observed_forward": None,
                        }
                    )
                    future_trades_by_variant[variant.variant_id].append(trade)
                    future_positions[variant.variant_id] = 0

        session_end_ns = _ns(
            datetime.combine(trading_date, SESSION_MARK_TIME, tzinfo=TAIPEI)
        )
        for variant in variants:
            variant_id = variant.variant_id
            future_rows = future_trades_by_variant[variant_id]
            all_trade_rows.extend(future_rows)
            day_gross_cash = sum(float(row["gross_cash_flow_twd"]) for row in future_rows)
            day_fees = sum(float(row["fixed_fee_twd"]) for row in future_rows)
            day_taxes = sum(float(row["transaction_tax_twd"]) for row in future_rows)
            cumulative_future_gross_cash[variant_id] += day_gross_cash
            cumulative_future_fees[variant_id] += day_fees
            cumulative_future_taxes[variant_id] += day_taxes
            marked_value = 0.0
            position = future_positions[variant_id]
            if position:
                mark = market.last_future_trade_before(HEDGE_PRODUCT, session_end_ns)
                if mark is None:
                    raise ValueError(f"{trading_date}: no TMF session-end mark")
                marked_value = (
                    position * float(mark.price) * FUTURES_MULTIPLIERS[HEDGE_PRODUCT]
                )
            gross_equity = cumulative_future_gross_cash[variant_id] + marked_value
            net_equity = (
                gross_equity
                - cumulative_future_fees[variant_id]
                - cumulative_future_taxes[variant_id]
            )
            future_daily_net = net_equity - previous_future_equity[variant_id]
            future_daily_gross = future_daily_net + day_fees + day_taxes
            previous_future_equity[variant_id] = net_equity
            model_daily = _copy_classic_daily_for_model(
                classic_row,
                variant=variant,
                future_daily_gross=future_daily_gross,
                future_daily_fees=day_fees,
                future_daily_taxes=day_taxes,
                future_position=position,
                future_trade_rows=future_rows,
                calibration=calibration_by_variant[variant_id],
            )
            model_daily["marked_equity_twd"] = (
                float(classic_row["marked_equity_twd"]) + net_equity
            )
            all_daily_rows.append(model_daily)

    if any(future_positions.values()):
        raise ValueError(f"sample-end TMF positions are not flat: {future_positions}")

    daily = pl.DataFrame(all_daily_rows, infer_schema_length=None).sort(
        ["variant_id", "trading_date"]
    )
    trades = pl.DataFrame(all_trade_rows, infer_schema_length=None).sort(
        ["variant_id", "fill_ts", "instrument_type"]
    )
    calibrations = pl.DataFrame(calibration_rows, infer_schema_length=None).sort(
        ["volatility_model", "trading_date"]
    )
    _validate_results(
        daily=daily,
        trades=trades,
        classic_option_trades=classic_option_trades,
        variants=variants,
        trading_dates=trading_dates,
    )
    normalized_daily, base_metrics = build_capital_normalized_returns(
        daily,
        trades,
        carry_across_sessions=True,
        pnl_column="net_pnl_twd",
    )
    metrics = _append_risk_metrics(base_metrics, normalized_daily, daily)
    for row in metrics.iter_rows(named=True):
        curve = normalized_daily.filter(pl.col("variant_id") == row["variant_id"])
        endpoint = float(curve.item(-1, "cumulative_return_on_capital"))
        if not math.isclose(
            endpoint,
            float(row["cumulative_return_on_capital"]),
            abs_tol=1e-12,
        ):
            raise ValueError(f"capital curve endpoint mismatch: {row['variant_id']}")

    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_frames = {
        "daily_results.parquet": daily,
        "trades.parquet": trades,
        "calibrations.parquet": calibrations,
        "capital_normalized_daily.parquet": normalized_daily,
    }
    for filename, frame in artifact_frames.items():
        _atomic_parquet(frame, output_dir / filename)
    metrics.write_csv(output_dir / "metrics.csv")
    normalized_daily.write_csv(output_dir / "capital_normalized_daily.csv")
    calibrations.write_csv(output_dir / "calibrations.csv")

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    summary: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": generated_at,
        "source": TAIFEX_TRADE_PROXY_SOURCE,
        "source_manifest": str(raw_root / "manifest.json"),
        "source_manifest_sha256": manifest_sha,
        "source_parser_contract_version": manifest["parser_contract_version"],
        "baseline_summary": str(summary_path),
        "baseline_summary_sha256": _sha256_path(summary_path),
        "date_start": trading_dates[0].isoformat(),
        "date_end": trading_dates[-1].isoformat(),
        "trading_days": len(trading_dates),
        "model_count": len(variants),
        "variant_count_including_classic": 1 + len(variants),
        "daily_result_rows": daily.height,
        "trade_rows": trades.height,
        "calibration_rows": calibrations.height,
        "execution_contract": {
            "option_ledger": "exact copy of classic_opening_straddle",
            "calibration_frequency": "once per active trading session",
            "calibration_time": calibration_time.isoformat(),
            "surface_observability_delay_seconds": 1,
            "hedge_frequency_minutes": hedge_interval_minutes,
            "hedge_end_time": hedge_end_time.isoformat(),
            "expiry_hedge_close_time": EXPIRY_HEDGE_CLOSE_TIME.isoformat(),
            "hedge_product": HEDGE_PRODUCT,
            "non_terminal_execution": "first strictly later transaction print",
            "historical_bidask_available": False,
            "one_lot_fill_assumption": "guaranteed; no quantity/depth cap",
            "artificial_slippage_twd": 0.0,
            "pressure_tests": "not run",
        },
        "cost_contract": {
            "TXO_fixed_fee_per_contract_side_twd": FIXED_FEES_PER_CONTRACT_SIDE["TXO"],
            "TMF_fixed_fee_per_contract_side_twd": FIXED_FEES_PER_CONTRACT_SIDE[HEDGE_PRODUCT],
            "statutory_transaction_tax_included": True,
        },
        "model_implementations": [
            {
                "model_id": model_id,
                "label": VOLATILITY_MODEL_LABELS[model_id],
                "implementation_level": VOLATILITY_MODEL_IMPLEMENTATION[model_id],
            }
            for model_id in VOLATILITY_MODEL_IDS
        ],
        "results": metrics.to_dicts(),
        "artifacts": {},
    }
    for filename in [*artifact_frames, "metrics.csv", "capital_normalized_daily.csv", "calibrations.csv"]:
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
            "full_verified_date_coverage": True,
            "classic_option_ledger_reused_exactly": True,
            "strictly_later_non_terminal_fills": True,
            "sample_end_positions_flat": True,
            "daily_ledger_endpoints_reconciled": True,
            "capital_curve_endpoints_reconciled": True,
            "historical_bidask_claimed": False,
            "pressure_tests_run": False,
        },
    }
    _atomic_json(output_dir / "receipt.json", receipt)
    print(
        json.dumps(
            {
                "status": "complete",
                "days": len(trading_dates),
                "models": len(variants),
                "daily_rows": daily.height,
                "trade_rows": trades.height,
                "calibration_rows": calibrations.height,
                "output_dir": str(output_dir),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks"),
    )
    parser.add_argument("--baseline-dir", type=Path, default=BASELINE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--calibration-time", default="08:55:00")
    parser.add_argument("--hedge-end-time", default="13:40:00")
    parser.add_argument("--hedge-interval-minutes", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_backtest(
        raw_root=args.raw_root,
        baseline_dir=args.baseline_dir,
        output_dir=args.output_dir,
        calibration_time=_parse_time(args.calibration_time),
        hedge_end_time=_parse_time(args.hedge_end_time),
        hedge_interval_minutes=int(args.hedge_interval_minutes),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
