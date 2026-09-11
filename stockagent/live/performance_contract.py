"""Presentation boundaries: stored log returns, simple rates, and paper NAV.

Canonical training return tables store log returns (including -inf on ruin).
Consumer DTOs expose simple returns. Never infer units from value magnitudes.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

PERFORMANCE_SCHEMA_VERSION = 2


def log_to_simple_return(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    if math.isnan(number) or number == math.inf:
        raise ValueError("Invalid canonical log return")
    # -inf is an absorbing -100% return, not a missing observation.
    return math.expm1(number)


def compound_simple_returns(values) -> float | None:
    terms = []
    ruined = False
    for value in values:
        if value is None:
            continue
        number = float(value)
        if not math.isfinite(number) or number < -1:
            raise ValueError("Invalid simple return")
        if number == -1:
            ruined = True
        else:
            terms.append(math.log1p(number))
    if ruined:
        return -1.0
    return math.expm1(math.fsum(terms)) if terms else None


def resolve_return_artifact(fold_dir: Path, *, prefer_integer: bool = True) -> Path | None:
    stems = (["integer_share_daily_portfolio_returns"] if prefer_integer else []) + ["daily_portfolio_returns"]
    for stem in stems:
        for suffix in (".parquet", ".csv"):
            path = Path(fold_dir) / (stem + suffix)
            if path.exists():
                return path
    return None


def simple_return_frame(frame):
    """Decode canonical table columns once, retaining explicit DTO units.

    Missing return_type means the canonical legacy log contract. An explicit
    simple DTO is idempotent; unknown or mixed units fail closed.
    """
    import polars as pl

    units = set(frame["return_type"].drop_nulls().to_list()) if "return_type" in frame.columns else {"log"}
    if len(units) != 1 or not units.issubset({"log", "simple"}):
        raise ValueError(f"Unsupported or mixed return units: {units}")
    if "return_type" in frame.columns and frame["return_type"].null_count():
        raise ValueError("Missing explicit return units")
    names = [name for name in ("portfolio_return", "benchmark_return") if name in frame.columns]
    if units == {"log"}:
        expressions = []
        for name in names:
            raw = pl.col(name).cast(pl.Float64)
            if frame.select((raw.is_nan() | (raw == math.inf)).any()).item():
                raise ValueError(f"Invalid canonical log return in {name}")
            expressions.extend([raw.alias(name.replace("_return", "_log_return")), raw.exp().sub(1.0).alias(name)])
        frame = frame.with_columns(expressions)
    else:
        for name in names:
            raw = pl.col(name).cast(pl.Float64)
            if frame.select((~raw.is_finite() | (raw < -1)).any()).item():
                raise ValueError(f"Invalid simple return in {name}")
    return frame.with_columns(pl.lit("simple").alias("return_type"))


def paper_account_performance(mode: Mapping[str, Any], *, revision: Any = None) -> dict[str, Any]:
    """One arithmetic owner for web and Discord's cumulative paper account.

    This is NOT a model backtest, selected chart range, or hypothetical sizing.
    Inputs must come from one atomic engine state (or one historical mark).
    """
    def finite(key):
        value = mode.get(key)
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    initial = finite("initial_capital_twd")
    equity = finite("total_equity_twd")
    valid = initial is not None and initial > 0 and equity is not None
    pnl = equity - initial if valid else None
    return {
        "schema_version": PERFORMANCE_SCHEMA_VERSION,
        "kind": "paper_account", "source": "paper_execution_ledger",
        "basis": "initial_capital_to_latest_net_liquidation", "return_type": "simple",
        "session_date": mode.get("session_date"),
        "asof": mode.get("last_mark_at") or mode.get("closing_auction_settled_at"),
        "state_revision": revision,
        "initial_capital_twd": initial, "total_equity_twd": equity,
        "net_pnl_twd": pnl, "return_fraction": pnl / initial if valid else None,
        "return_pct": 100.0 * pnl / initial if valid else None,
        "valuation_stale": bool(mode.get("valuation_stale") or mode.get("stale_position_count")),
        "status": "available" if valid else "unavailable",
        "simulation_only": True,
    }
