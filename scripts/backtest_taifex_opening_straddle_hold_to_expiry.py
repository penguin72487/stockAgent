#!/usr/bin/env python3
"""Hold one opening ATM TXO straddle until its weekly expiry.

This is a long-history screening study.  Entry uses each selected leg's official
daily open, keeps that exact series/strike across sessions, and enters the next
weekly series only after the exchange's official settlement date.  Terminal
value always uses the official final settlement price.  Only the two opening
contract-sides pay the user-specified broker fee.  The ledger applies statutory
option-premium tax to each opening leg and, for the one automatically exercised
in-the-money leg, stock-index futures tax to the cash-settlement contract
amount.  Tax is rounded per contract to whole TWD.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timezone
import math
from pathlib import Path
import sys
from typing import Any, Final
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_classic_opening_straddle_daily import (  # noqa: E402
    TAIFEX_OPTION_POSITION_SIDES,
    _atomic_csv,
    _atomic_parquet,
    _longest_streak,
    _position_direction,
    _safe_profit_factor,
    _sha256,
)
from scripts.taifex_daily_download_common import atomic_write_json  # noqa: E402
from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    taifex_option_expiry,
)
from stockagent.data.tw_index_options_daily import (  # noqa: E402
    TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
    TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
    TAIFEX_TXO_MULTIPLIER,
    load_taifex_option_daily_contract_rows,
    load_taifex_weekly_atm_straddles,
)
from stockagent.research.taifex_capital_returns import (  # noqa: E402
    build_capital_normalized_returns,
)
from stockagent.research.taifex_transaction_tax import (  # noqa: E402
    TAIFEX_OPTION_PREMIUM_TAX_RATE,
    TAIFEX_STOCK_INDEX_FUTURES_CURRENT_TAX_SOURCE_URL,
    TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_BEFORE_2013_04_01,
    TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_CHANGE_DATE,
    TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_FROM_2013_04_01,
    TAIFEX_STOCK_INDEX_FUTURES_2013_TAX_SOURCE_URL,
    TAIFEX_TRANSACTION_TAX_ACT_URL,
    TAIFEX_TRANSACTION_TAX_QA_URL,
    TAIFEX_TRANSACTION_TAX_ROUNDING_URL,
    option_cash_settlement_transaction_tax_twd,
    option_premium_transaction_tax_twd,
    stock_index_futures_tax_rate,
)


TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
STRATEGY: Final[str] = "opening_atm_straddle_hold_to_weekly_expiry"
FAMILY: Final[str] = "hold_to_expiry"
OUTPUT_SCHEMA_VERSION: Final[int] = 3
DEFAULT_INPUT: Final[Path] = Path(
    "data_tw_index_options_daily/weekly_nearest_expiry_opening_atm_pairs.parquet"
)
DEFAULT_RAW_ROOT: Final[Path] = Path("data_tw_index_options_daily/raw")
DEFAULT_FINAL_SETTLEMENT_PATH: Final[Path] = Path(
    "data_tw_index_options_daily/txo_final_settlement_history.parquet"
)
DEFAULT_OUTPUT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_opening_straddle_hold_to_weekly_expiry"
)
DEFAULT_OUTPUT_DIRS: Final[dict[str, Path]] = {
    "long": DEFAULT_OUTPUT_DIR,
    "short": Path(
        "artifacts/research/taifex_opening_short_straddle_hold_to_weekly_expiry"
    ),
}
OFFICIAL_FINAL_SETTLEMENT_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/5/optIndxFSP"
)


def _strategy_id(position_side: str) -> str:
    direction = _position_direction(position_side)
    return (
        STRATEGY
        if direction == 1
        else "opening_atm_short_straddle_hold_to_weekly_expiry"
    )


def _raw_paths(root: Path) -> list[Path]:
    paths = sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.casefold() in {".csv", ".zip"}
    )
    if not paths:
        raise FileNotFoundError(f"no official option receipts under {root}")
    return paths


def _finite_positive(value: object) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0.0


def _load_official_final_settlements(
    path: Path,
) -> dict[tuple[date, str], dict[str, object]]:
    if not path.is_file():
        raise FileNotFoundError(
            f"official TXO final-settlement history does not exist: {path}"
        )
    source_sha256 = _sha256(path)
    frame = pd.read_parquet(path)
    normalized_required = {
        "settlement_date",
        "option_series",
        "final_settlement_price",
        "product",
    }
    legacy_required = {"最後結算日", "契約月份", "商品代號", "最後結算價"}
    if normalized_required.issubset(frame.columns):
        selected = frame.loc[
            frame["product"].astype(str).str.strip().str.upper().eq("TXO")
        ].copy()
        date_column = "settlement_date"
        series_column = "option_series"
        price_column = "final_settlement_price"
        normalized = True
    elif legacy_required.issubset(frame.columns):
        selected = frame.loc[
            frame["商品代號"].astype(str).str.strip().str.upper().eq("TXO")
        ].copy()
        date_column = "最後結算日"
        series_column = "契約月份"
        price_column = "最後結算價"
        normalized = False
    else:
        missing_normalized = sorted(normalized_required.difference(frame.columns))
        raise ValueError(
            "official settlement history is missing normalized columns: "
            f"{missing_normalized}"
        )
    output: dict[tuple[date, str], dict[str, object]] = {}
    for row in selected.to_dict(orient="records"):
        raw_date = row[date_column]
        series = str(row[series_column]).strip().upper()
        try:
            settlement_date = (
                pd.Timestamp(raw_date).date()
                if normalized
                else pd.to_datetime(str(raw_date).strip(), format="%Y%m%d").date()
            )
            settlement = float(row[price_column])
        except (TypeError, ValueError):
            continue
        if not math.isfinite(settlement) or settlement <= 0.0:
            continue
        key = (settlement_date, series)
        previous = output.get(key)
        if previous is not None and float(previous["settlement_price"]) != settlement:
            raise ValueError(
                "conflicting official TXO final settlements for "
                f"{settlement_date}/{series}"
            )
        output[key] = {
            "settlement_price": settlement,
            "source_file": str(row.get("source_file") or path),
            "source_sha256": str(row.get("source_sha256") or source_sha256),
            "source_url": str(row.get("source_url") or OFFICIAL_FINAL_SETTLEMENT_URL),
        }
    if not output:
        raise ValueError(f"official settlement history has no valid TXO rows: {path}")
    return output


def _plan_cycles(
    source: pd.DataFrame,
    official_settlements: dict[tuple[date, str], dict[str, object]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ordered = source.copy()
    ordered["date"] = pd.to_datetime(ordered["date"])
    ordered = ordered.sort_values("date", kind="stable").reset_index(drop=True)
    if ordered.empty or ordered["date"].duplicated().any():
        raise ValueError("weekly ATM source must contain unique ordered dates")
    sessions = [timestamp.date() for timestamp in ordered["date"]]
    last_source_date = sessions[-1]
    settlement_date_by_series: dict[str, date] = {}
    for settlement_date, series in official_settlements:
        previous = settlement_date_by_series.get(series)
        if previous is not None and previous != settlement_date:
            raise ValueError(
                f"official settlement history has multiple dates for {series}: "
                f"{previous} and {settlement_date}"
            )
        settlement_date_by_series[series] = settlement_date
    occupied_through: date | None = None
    plans: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []

    for row in ordered.to_dict(orient="records"):
        entry_date = pd.Timestamp(row["date"]).date()
        if occupied_through is not None and entry_date <= occupied_through:
            continue
        series = str(row.get("option_series") or "").strip().upper()
        strike = row.get("strike")
        entry_ok = (
            bool(series)
            and _finite_positive(strike)
            and _finite_positive(row.get("tx_open"))
            and _finite_positive(row.get("call_open"))
            and _finite_positive(row.get("put_open"))
        )
        if not entry_ok:
            skipped.append(
                {
                    "date": entry_date,
                    "reason": "missing_point_in_time_entry_pair",
                    "source_exclusion_reason": row.get("exclusion_reason"),
                }
            )
            continue
        expiry_date = settlement_date_by_series.get(series)
        if expiry_date is None:
            skipped.append(
                {
                    "date": entry_date,
                    "reason": "missing_official_final_settlement",
                    "option_series": series,
                    "source_exclusion_reason": None,
                }
            )
            continue
        if expiry_date < entry_date:
            skipped.append(
                {
                    "date": entry_date,
                    "reason": "selected_series_already_expired",
                    "source_exclusion_reason": None,
                }
            )
            continue
        if expiry_date > last_source_date:
            skipped.append(
                {
                    "date": entry_date,
                    "reason": "expiry_after_dataset_end",
                    "source_exclusion_reason": None,
                }
            )
            break
        holding_sessions = sum(entry_date <= session <= expiry_date for session in sessions)
        plans.append(
            {
                "entry_date": entry_date,
                "expiry_date": expiry_date,
                "calendar_expiry_date": taifex_option_expiry(series),
                "official_minus_calendar_expiry_days": (
                    expiry_date - taifex_option_expiry(series)
                ).days,
                "option_series": series,
                "strike": float(strike),
                "tx_open": float(row["tx_open"]),
                "opening_abs_moneyness_points": float(
                    row["opening_abs_moneyness_points"]
                ),
                "call_entry_price_points": float(row["call_open"]),
                "put_entry_price_points": float(row["put_open"]),
                "holding_sessions": int(holding_sessions),
                "call_entry_source_file": row.get("call_source_file"),
                "call_entry_source_sha256": row.get("call_source_sha256"),
                "put_entry_source_file": row.get("put_source_file"),
                "put_entry_source_sha256": row.get("put_source_sha256"),
            }
        )
        occupied_through = expiry_date

    return pd.DataFrame(plans), pd.DataFrame(skipped)


def _build_cycle_results(
    plans: pd.DataFrame,
    terminal_rows: dict[tuple[date, str, float, str], dict[str, object]],
    *,
    fee_per_contract_side_twd: float,
    option_premium_tax_rate: float = TAIFEX_OPTION_PREMIUM_TAX_RATE,
    position_side: str = "long",
    official_settlements: dict[
        tuple[date, str], dict[str, object]
    ] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    direction = _position_direction(position_side)
    normalized_side = "long" if direction == 1 else "short"
    strategy_id = _strategy_id(normalized_side)
    official_settlements = official_settlements or {}
    cycles: list[dict[str, object]] = []
    trades: list[dict[str, object]] = []
    excluded: list[dict[str, object]] = []
    for plan in plans.to_dict(orient="records"):
        expiry_date = plan["expiry_date"]
        series = str(plan["option_series"])
        strike = float(plan["strike"])
        marks = {
            right: terminal_rows.get((expiry_date, series, strike, right))
            for right in ("C", "P")
        }
        settlement = official_settlements.get((expiry_date, series))
        has_proxy_marks = all(
            mark is not None and _finite_positive(mark.get("close"))
            for mark in marks.values()
        )
        if settlement is None:
            excluded.append(
                {
                    **plan,
                    "reason": "missing_official_final_settlement",
                }
            )
            continue
        settlement_price = float(settlement["settlement_price"])
        call_exit = max(settlement_price - strike, 0.0)
        put_exit = max(strike - settlement_price, 0.0)
        terminal_value_source = "official_taifex_final_settlement_price"

        proxy_call_points = (
            float(marks["C"]["close"])  # type: ignore[index]
            if has_proxy_marks
            else math.nan
        )
        proxy_put_points = (
            float(marks["P"]["close"])  # type: ignore[index]
            if has_proxy_marks
            else math.nan
        )
        proxy_terminal_points = proxy_call_points + proxy_put_points
        proxy_settlement_tax_basis_points = (
            strike + proxy_call_points - proxy_put_points
            if has_proxy_marks
            else math.nan
        )
        settlement_market_price_for_tax = settlement_price
        if not _finite_positive(settlement_market_price_for_tax):
            excluded.append(
                {
                    **plan,
                    "reason": "invalid_cash_settlement_tax_basis_proxy",
                }
            )
            continue
        if settlement_market_price_for_tax > strike:
            cash_settlement_option_right: str | None = "C"
        elif settlement_market_price_for_tax < strike:
            cash_settlement_option_right = "P"
        else:
            cash_settlement_option_right = None

        opening_points = float(plan["call_entry_price_points"]) + float(
            plan["put_entry_price_points"]
        )
        terminal_points = call_exit + put_exit
        opening_premium_twd = opening_points * TAIFEX_TXO_MULTIPLIER
        terminal_value_twd = terminal_points * TAIFEX_TXO_MULTIPLIER
        gross_pnl_twd = direction * (terminal_value_twd - opening_premium_twd)
        fee_twd = 2.0 * fee_per_contract_side_twd
        entry_taxes = {
            "C": option_premium_transaction_tax_twd(
                plan["call_entry_price_points"],
                multiplier_twd_per_point=TAIFEX_TXO_MULTIPLIER,
                tax_rate=option_premium_tax_rate,
            ),
            "P": option_premium_transaction_tax_twd(
                plan["put_entry_price_points"],
                multiplier_twd_per_point=TAIFEX_TXO_MULTIPLIER,
                tax_rate=option_premium_tax_rate,
            ),
        }
        entry_transaction_tax_twd = sum(entry_taxes.values())
        settlement_tax_rate = stock_index_futures_tax_rate(expiry_date)
        settlement_transaction_tax_twd = (
            option_cash_settlement_transaction_tax_twd(
                settlement_market_price_for_tax,
                settlement_date=expiry_date,
                multiplier_twd_per_point=TAIFEX_TXO_MULTIPLIER,
            )
            if cash_settlement_option_right is not None
            else 0.0
        )
        proxy_settlement_transaction_tax_twd = (
            option_cash_settlement_transaction_tax_twd(
                proxy_settlement_tax_basis_points,
                settlement_date=expiry_date,
                multiplier_twd_per_point=TAIFEX_TXO_MULTIPLIER,
            )
            if _finite_positive(proxy_settlement_tax_basis_points)
            and proxy_settlement_tax_basis_points != strike
            else 0.0
        )
        transaction_tax_twd = (
            entry_transaction_tax_twd + settlement_transaction_tax_twd
        )
        net_after_fee_twd = gross_pnl_twd - fee_twd
        net_pnl_twd = net_after_fee_twd - transaction_tax_twd
        cycle = {
            **plan,
            "position_side": normalized_side,
            "position_direction": direction,
            "variant_id": strategy_id,
            "date": expiry_date,
            "year": expiry_date.year,
            "month": expiry_date.strftime("%Y-%m"),
            "call_terminal_price_points": call_exit,
            "put_terminal_price_points": put_exit,
            "official_final_settlement_points": settlement_price,
            "expiry_last_trade_proxy_call_points": proxy_call_points,
            "expiry_last_trade_proxy_put_points": proxy_put_points,
            "expiry_last_trade_proxy_settlement_tax_basis_points": (
                proxy_settlement_tax_basis_points
            ),
            "expiry_last_trade_proxy_settlement_transaction_tax_twd": (
                proxy_settlement_transaction_tax_twd
            ),
            "cash_settlement_tax_basis_points": settlement_market_price_for_tax,
            "cash_settlement_tax_basis_source": (
                "official_taifex_final_settlement_price"
            ),
            "cash_settlement_option_right": cash_settlement_option_right,
            "expiry_last_trade_proxy_terminal_points": proxy_terminal_points,
            "proxy_minus_settlement_terminal_twd": (
                (proxy_terminal_points - terminal_points) * TAIFEX_TXO_MULTIPLIER
                if math.isfinite(proxy_terminal_points)
                and math.isfinite(settlement_price)
                else math.nan
            ),
            "opening_premium_points": opening_points,
            "terminal_value_points": terminal_points,
            "opening_premium_twd": opening_premium_twd,
            "terminal_value_twd": terminal_value_twd,
            "gross_pnl_twd": gross_pnl_twd,
            "fee_twd": fee_twd,
            "entry_transaction_tax_twd": entry_transaction_tax_twd,
            "settlement_transaction_tax_twd": settlement_transaction_tax_twd,
            "proxy_minus_settlement_transaction_tax_twd": (
                proxy_settlement_transaction_tax_twd
                - settlement_transaction_tax_twd
                if has_proxy_marks
                else math.nan
            ),
            "transaction_tax_twd": transaction_tax_twd,
            "net_after_fee_twd": net_after_fee_twd,
            "net_pnl_twd": net_pnl_twd,
            "required_capital_twd": (
                opening_premium_twd + fee_twd + entry_transaction_tax_twd
                if direction == 1
                else math.nan
            ),
            "option_premium_transaction_tax_rate": option_premium_tax_rate,
            "cash_settlement_transaction_tax_rate": settlement_tax_rate,
            "terminal_value_source": terminal_value_source,
            "official_settlement_source_file": (
                settlement["source_file"]
            ),
            "official_settlement_source_sha256": (
                settlement["source_sha256"]
            ),
            "call_terminal_source_file": (
                marks["C"]["source_file"] if marks["C"] is not None else None
            ),
            "call_terminal_source_sha256": (
                marks["C"]["source_sha256"] if marks["C"] is not None else None
            ),
            "put_terminal_source_file": (
                marks["P"]["source_file"] if marks["P"] is not None else None
            ),
            "put_terminal_source_sha256": (
                marks["P"]["source_sha256"] if marks["P"] is not None else None
            ),
            "final_call_contracts": 0,
            "final_put_contracts": 0,
        }
        cycles.append(cycle)

        entry_ts = datetime.combine(plan["entry_date"], time(9, 0), tzinfo=TAIPEI)
        terminal_ts = datetime.combine(expiry_date, time(13, 30), tzinfo=TAIPEI)
        for right, entry_price, terminal_price in (
            ("C", float(plan["call_entry_price_points"]), call_exit),
            ("P", float(plan["put_entry_price_points"]), put_exit),
        ):
            common = {
                "trading_date": expiry_date,
                "benchmark_family": FAMILY,
                "variant_id": strategy_id,
                "position_side": normalized_side,
                "position_direction": direction,
                "instrument_type": "option",
                "product": "TXO",
                "series": series,
                "strike": strike,
                "option_right": right,
            }
            trades.append(
                {
                    **common,
                    "fill_ts": entry_ts,
                    "delta_contracts": direction,
                    "price_points": entry_price,
                    "reason": "open_atm_straddle",
                    "gross_cash_flow_twd": (
                        -direction * entry_price * TAIFEX_TXO_MULTIPLIER
                    ),
                    "fixed_fee_twd": fee_per_contract_side_twd,
                    "transaction_tax_basis_points": entry_price,
                    "transaction_tax_rate": option_premium_tax_rate,
                    "transaction_tax_twd": entry_taxes[right],
                    "terminal_mark_proxy": False,
                }
            )
            trades.append(
                {
                    **common,
                    "fill_ts": terminal_ts,
                    "delta_contracts": -direction,
                    "price_points": terminal_price,
                    "reason": (
                        "official_expiry_cash_settlement"
                    ),
                    "gross_cash_flow_twd": (
                        direction * terminal_price * TAIFEX_TXO_MULTIPLIER
                    ),
                    "fixed_fee_twd": 0.0,
                    "transaction_tax_basis_points": (
                        settlement_market_price_for_tax
                        if right == cash_settlement_option_right
                        else 0.0
                    ),
                    "transaction_tax_rate": (
                        settlement_tax_rate
                        if right == cash_settlement_option_right
                        else 0.0
                    ),
                    "transaction_tax_twd": (
                        settlement_transaction_tax_twd
                        if right == cash_settlement_option_right
                        else 0.0
                    ),
                    "terminal_mark_proxy": False,
                }
            )

    frame = pd.DataFrame(cycles)
    if not frame.empty:
        frame = frame.sort_values("date", kind="stable").reset_index(drop=True)
        frame["cumulative_net_pnl_twd"] = frame["net_pnl_twd"].cumsum()
        peak = frame["cumulative_net_pnl_twd"].cummax().clip(lower=0.0)
        frame["drawdown_twd"] = frame["cumulative_net_pnl_twd"] - peak
    return frame, pd.DataFrame(trades), pd.DataFrame(excluded)


def _assert_accounting(cycles: pd.DataFrame, trades: pd.DataFrame) -> None:
    if cycles.empty:
        raise ValueError("no complete hold-to-expiry cycles")
    if not (
        (cycles["final_call_contracts"] == 0)
        & (cycles["final_put_contracts"] == 0)
    ).all():
        raise AssertionError("a hold-to-expiry cycle did not finish flat")
    if not np.allclose(
        cycles["gross_pnl_twd"],
        cycles["position_direction"]
        * (cycles["terminal_value_twd"] - cycles["opening_premium_twd"]),
        rtol=0.0,
        atol=1e-9,
    ):
        raise AssertionError("cycle gross P&L mismatch")
    if not np.allclose(
        cycles["net_after_fee_twd"],
        cycles["gross_pnl_twd"] - cycles["fee_twd"],
        rtol=0.0,
        atol=1e-9,
    ):
        raise AssertionError("cycle fixed-fee P&L mismatch")
    if not np.allclose(
        cycles["net_pnl_twd"],
        cycles["net_after_fee_twd"] - cycles["transaction_tax_twd"],
        rtol=0.0,
        atol=1e-9,
    ):
        raise AssertionError("cycle net P&L mismatch")
    ledger = trades.groupby("trading_date", as_index=False).agg(
        ledger_net=("gross_cash_flow_twd", "sum"),
        ledger_fee=("fixed_fee_twd", "sum"),
        ledger_tax=("transaction_tax_twd", "sum"),
        terminal_rows=("terminal_mark_proxy", "sum"),
    )
    ledger["ledger_net"] -= ledger["ledger_fee"] + ledger["ledger_tax"]
    joined = cycles.merge(
        ledger,
        left_on="date",
        right_on="trading_date",
        how="inner",
        validate="one_to_one",
    )
    if len(joined) != len(cycles) or not np.allclose(
        joined["net_pnl_twd"], joined["ledger_net"], rtol=0.0, atol=1e-9
    ):
        raise AssertionError("cycle P&L does not reconcile to trade ledger")
    expected_proxy_rows = cycles["terminal_value_source"].eq(
        "expiry_day_last_trade_cash_settlement_proxy"
    ).astype(int) * 2
    if not np.array_equal(joined["terminal_rows"].to_numpy(), expected_proxy_rows):
        raise AssertionError("terminal proxy markers do not match settlement source")
    if not np.allclose(
        joined["transaction_tax_twd"],
        joined["ledger_tax"],
        rtol=0.0,
        atol=1e-9,
    ):
        raise AssertionError("cycle transaction tax does not reconcile to ledger")


def _daily_mark(
    row: dict[str, object] | None,
    *,
    label: str,
) -> tuple[float, str]:
    if row is None:
        raise ValueError(f"missing held-contract daily row: {label}")
    settlement = row.get("settlement")
    if settlement is not None:
        value = float(settlement)
        if math.isfinite(value) and value >= 0.0:
            return value, "official_daily_settlement"
    close = row.get("close")
    if close is not None:
        value = float(close)
        if math.isfinite(value) and value >= 0.0:
            return value, "daily_last_trade_fallback"
    raise ValueError(f"held contract has no finite daily mark: {label}")


def _build_daily_results(
    cycles: pd.DataFrame,
    contract_rows: dict[tuple[date, str, float, str], dict[str, object]],
    sessions: list[date],
    *,
    capital_base_twd: float | None,
) -> tuple[pd.DataFrame, dict[str, float | int | None]]:
    """Mark fixed contracts once per session and build a true daily curve.

    Daily option settlement is the preferred mark.  Some historical weekly
    contracts have no published settlement field, so their official last trade
    is used only as an end-of-day mark fallback.  The expiry row always replaces
    both marks with official intrinsic cash-settlement value.
    """

    if capital_base_twd is not None and (
        not math.isfinite(capital_base_twd) or capital_base_twd <= 0.0
    ):
        raise ValueError("capital_base_twd must be finite and positive")
    if not sessions:
        raise ValueError("daily curve requires at least one verified session")
    position_rows: dict[date, dict[str, object]] = {}
    cycle_expected: dict[str, float] = {}
    fallback_leg_marks = 0
    settlement_leg_marks = 0
    for cycle_number, cycle in enumerate(cycles.to_dict(orient="records"), start=1):
        entry_date = cycle["entry_date"]
        expiry_date = cycle["expiry_date"]
        series = str(cycle["option_series"])
        strike = float(cycle["strike"])
        direction = int(cycle["position_direction"])
        position_side = str(cycle["position_side"])
        variant_id = str(cycle["variant_id"])
        cycle_id = f"{cycle_number:04d}_{series}_{entry_date}"
        if direction not in {-1, 1}:
            raise AssertionError(
                f"invalid position direction for {cycle_id}: {direction}"
            )
        held_sessions = [
            session for session in sessions if entry_date <= session <= expiry_date
        ]
        if len(held_sessions) != int(cycle["holding_sessions"]):
            raise AssertionError(
                f"holding-session mismatch for {cycle_id}: "
                f"planned={cycle['holding_sessions']}, actual={len(held_sessions)}"
            )
        previous_value_twd: float | None = None
        for session in held_sessions:
            if session in position_rows:
                raise AssertionError(f"overlapping hold cycles on {session}")
            is_entry = session == entry_date
            is_expiry = session == expiry_date
            if is_expiry:
                call_mark = float(cycle["call_terminal_price_points"])
                put_mark = float(cycle["put_terminal_price_points"])
                call_source = "official_final_settlement_intrinsic"
                put_source = "official_final_settlement_intrinsic"
            else:
                call_mark, call_source = _daily_mark(
                    contract_rows.get((session, series, strike, "C")),
                    label=f"{session}/{series}/{strike}/C",
                )
                put_mark, put_source = _daily_mark(
                    contract_rows.get((session, series, strike, "P")),
                    label=f"{session}/{series}/{strike}/P",
                )
                fallback_leg_marks += int(call_source == "daily_last_trade_fallback")
                fallback_leg_marks += int(put_source == "daily_last_trade_fallback")
                settlement_leg_marks += int(call_source == "official_daily_settlement")
                settlement_leg_marks += int(put_source == "official_daily_settlement")

            mark_value_twd = (call_mark + put_mark) * TAIFEX_TXO_MULTIPLIER
            commission_twd = float(cycle["fee_twd"]) if is_entry else 0.0
            opening_tax_twd = (
                float(cycle["entry_transaction_tax_twd"]) if is_entry else 0.0
            )
            settlement_tax_twd = (
                float(cycle["settlement_transaction_tax_twd"])
                if is_expiry
                else 0.0
            )
            if previous_value_twd is None:
                daily_pnl_twd = (
                    direction
                    * (mark_value_twd - float(cycle["opening_premium_twd"]))
                    - commission_twd
                    - opening_tax_twd
                )
            else:
                daily_pnl_twd = direction * (mark_value_twd - previous_value_twd)
            daily_pnl_twd -= settlement_tax_twd
            position_rows[session] = {
                "trading_date": session,
                "benchmark_family": FAMILY,
                "variant_id": variant_id,
                "position_side": position_side,
                "position_direction": direction,
                "cycle_id": cycle_id,
                "option_series": series,
                "strike": strike,
                "is_entry_session": is_entry,
                "is_expiry_session": is_expiry,
                "call_mark_points": call_mark,
                "put_mark_points": put_mark,
                "call_mark_source": call_source,
                "put_mark_source": put_source,
                "position_mark_value_twd": mark_value_twd,
                "signed_position_mark_value_twd": direction * mark_value_twd,
                "commission_twd": commission_twd,
                "opening_transaction_tax_twd": opening_tax_twd,
                "settlement_transaction_tax_twd": settlement_tax_twd,
                "transaction_tax_twd": opening_tax_twd + settlement_tax_twd,
                "net_after_fee_and_tax_twd": daily_pnl_twd,
                "net_after_fee_twd": daily_pnl_twd,
            }
            previous_value_twd = mark_value_twd
        cycle_expected[cycle_id] = float(cycle["net_pnl_twd"])

    first_date = min(cycles["entry_date"])
    last_date = max(cycles["expiry_date"])
    position_sides = cycles["position_side"].astype(str).unique().tolist()
    variant_ids = cycles["variant_id"].astype(str).unique().tolist()
    if len(position_sides) != 1 or len(variant_ids) != 1:
        raise AssertionError("daily curve requires one position side and one variant")
    position_side = position_sides[0]
    direction = _position_direction(position_side)
    variant_id = variant_ids[0]
    rows: list[dict[str, object]] = []
    for session in sessions:
        if session < first_date or session > last_date:
            continue
        row = position_rows.get(session)
        if row is None:
            row = {
                "trading_date": session,
                "benchmark_family": FAMILY,
                "variant_id": variant_id,
                "position_side": position_side,
                "position_direction": direction,
                "cycle_id": None,
                "option_series": None,
                "strike": None,
                "is_entry_session": False,
                "is_expiry_session": False,
                "call_mark_points": None,
                "put_mark_points": None,
                "call_mark_source": "flat",
                "put_mark_source": "flat",
                "position_mark_value_twd": 0.0,
                "signed_position_mark_value_twd": 0.0,
                "commission_twd": 0.0,
                "opening_transaction_tax_twd": 0.0,
                "settlement_transaction_tax_twd": 0.0,
                "transaction_tax_twd": 0.0,
                "net_after_fee_and_tax_twd": 0.0,
                "net_after_fee_twd": 0.0,
            }
        rows.append(row)
    daily = pd.DataFrame(rows).sort_values("trading_date", kind="stable")
    if daily.empty or daily["trading_date"].duplicated().any():
        raise AssertionError("daily hold curve is empty or has duplicate sessions")

    realized = daily.loc[daily["cycle_id"].notna()].groupby(
        "cycle_id", as_index=False
    )["net_after_fee_and_tax_twd"].sum()
    for row in realized.to_dict(orient="records"):
        expected = cycle_expected[str(row["cycle_id"])]
        if not math.isclose(
            float(row["net_after_fee_and_tax_twd"]),
            expected,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise AssertionError(
                f"daily P&L does not reconcile for {row['cycle_id']}: "
                f"daily={row['net_after_fee_and_tax_twd']}, cycle={expected}"
            )
    pnl = daily["net_after_fee_and_tax_twd"].to_numpy(dtype=np.float64)
    daily["cumulative_net_pnl_twd"] = np.cumsum(pnl)
    running_pnl_peak = np.maximum.accumulate(
        np.concatenate(
            [np.asarray([0.0], dtype=np.float64), np.cumsum(pnl)]
        )
    )[1:]
    daily["drawdown_twd"] = daily["cumulative_net_pnl_twd"] - running_pnl_peak
    metrics: dict[str, float | int | None] = {
        "trading_days": int(len(daily)),
        "total_net_pnl_twd": float(np.sum(pnl)),
        "maximum_drawdown_twd": float(daily["drawdown_twd"].min()),
        "official_daily_settlement_leg_marks": int(settlement_leg_marks),
        "daily_last_trade_fallback_leg_marks": int(fallback_leg_marks),
    }
    if capital_base_twd is not None:
        returns = pnl / capital_base_twd
        if np.any(returns <= -1.0) or not np.all(np.isfinite(returns)):
            raise ValueError("daily capital returns are non-finite or below -100%")
        daily["capital_base_twd"] = capital_base_twd
        daily["daily_return_on_capital"] = returns
        daily["cumulative_return_on_capital"] = np.cumsum(returns)
        wealth = np.cumprod(1.0 + returns)
        daily["cumulative_compounded_return"] = wealth - 1.0
        running_peak = np.maximum.accumulate(
            np.concatenate([np.asarray([1.0]), wealth])
        )[1:]
        daily["compounded_drawdown"] = wealth / running_peak - 1.0
        standard_deviation = (
            float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0
        )
        sharpe = (
            float(np.mean(returns)) / standard_deviation * math.sqrt(252.0)
            if standard_deviation > 0.0
            else 0.0
        )
        metrics.update(
            {
                "capital_base_twd": float(capital_base_twd),
                "cumulative_return_on_capital": float(
                    np.sum(pnl) / capital_base_twd
                ),
                "cumulative_compounded_return": float(wealth[-1] - 1.0),
                "annualized_sharpe": float(sharpe),
                "maximum_drawdown_compounded_return": float(
                    daily["compounded_drawdown"].min()
                ),
            }
        )
    else:
        daily["capital_base_twd"] = np.nan
        daily["daily_return_on_capital"] = np.nan
        daily["cumulative_return_on_capital"] = np.nan
        daily["cumulative_compounded_return"] = np.nan
        daily["compounded_drawdown"] = np.nan
        metrics.update(
            {
                "capital_base_twd": None,
                "cumulative_return_on_capital": None,
                "cumulative_compounded_return": None,
                "annualized_sharpe": None,
                "maximum_drawdown_compounded_return": None,
            }
        )
    if not math.isclose(
        float(metrics["total_net_pnl_twd"]),
        float(cycles["net_pnl_twd"].sum()),
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise AssertionError("daily strategy P&L does not reconcile to cycle P&L")
    return daily, metrics


def _aggregate(cycles: pd.DataFrame, period: str) -> pd.DataFrame:
    grouped = cycles.groupby(period, as_index=False).agg(
        cycles=("date", "count"),
        holding_sessions=("holding_sessions", "sum"),
        gross_pnl_twd=("gross_pnl_twd", "sum"),
        fee_twd=("fee_twd", "sum"),
        transaction_tax_twd=("transaction_tax_twd", "sum"),
        net_after_fee_twd=("net_after_fee_twd", "sum"),
        net_pnl_twd=("net_pnl_twd", "sum"),
        winning_cycles=("net_pnl_twd", lambda values: int((values > 0.0).sum())),
        win_rate=("net_pnl_twd", lambda values: float((values > 0.0).mean())),
        period_end_cumulative_net_pnl_twd=("cumulative_net_pnl_twd", "last"),
        worst_drawdown_twd=("drawdown_twd", "min"),
    )
    return grouped


def _summary(
    source: pd.DataFrame,
    plans: pd.DataFrame,
    cycles: pd.DataFrame,
    skipped_entries: pd.DataFrame,
    excluded_cycles: pd.DataFrame,
    capital_metrics: pl.DataFrame | None,
    true_daily_metrics: dict[str, float | int | None],
    *,
    input_path: Path,
    manifest_path: Path,
    raw_paths: list[Path],
    final_settlement_path: Path,
    fee: float,
    option_premium_tax_rate: float,
    position_side: str,
    artifact_paths: dict[str, Path],
) -> dict[str, Any]:
    direction = _position_direction(position_side)
    normalized_side = "long" if direction == 1 else "short"
    strategy_id = _strategy_id(normalized_side)
    net = cycles["net_pnl_twd"]
    first_date = pd.Timestamp(source["date"].min()).date()
    last_date = pd.Timestamp(source["date"].max()).date()
    elapsed_years = max((last_date - first_date).days / 365.2425, 1.0)
    results: dict[str, Any] = {
        "gross_pnl_twd": float(cycles["gross_pnl_twd"].sum()),
        "fees_twd": float(cycles["fee_twd"].sum()),
        "entry_transaction_taxes_twd": float(
            cycles["entry_transaction_tax_twd"].sum()
        ),
        "settlement_transaction_taxes_twd": float(
            cycles["settlement_transaction_tax_twd"].sum()
        ),
        "transaction_taxes_twd": float(cycles["transaction_tax_twd"].sum()),
        "net_after_fixed_fee_twd": float(cycles["net_after_fee_twd"].sum()),
        "net_pnl_twd": float(net.sum()),
        "winning_cycles": int((net > 0.0).sum()),
        "losing_cycles": int((net < 0.0).sum()),
        "flat_cycles": int((net == 0.0).sum()),
        "win_rate": float((net > 0.0).mean()),
        "average_net_pnl_twd": float(net.mean()),
        "median_net_pnl_twd": float(net.median()),
        "profit_factor": _safe_profit_factor(net),
        "maximum_realized_cycle_drawdown_twd": float(
            cycles["drawdown_twd"].min()
        ),
        "longest_winning_streak_cycles": _longest_streak(
            (net > 0.0).to_numpy()
        ),
        "longest_losing_streak_cycles": _longest_streak(
            (net < 0.0).to_numpy()
        ),
        "daily_mark_to_market_maximum_drawdown_twd": float(
            true_daily_metrics["maximum_drawdown_twd"]
        ),
        "daily_mark_to_market_observations": int(
            true_daily_metrics["trading_days"]
        ),
        "official_daily_settlement_leg_marks": int(
            true_daily_metrics["official_daily_settlement_leg_marks"]
        ),
        "daily_last_trade_fallback_leg_marks": int(
            true_daily_metrics["daily_last_trade_fallback_leg_marks"]
        ),
    }
    if capital_metrics is not None:
        metric = capital_metrics.row(0, named=True)
        results.update(
            {
                "capital_base_twd": float(metric["capital_base_twd"]),
                "one_lot_cumulative_return_on_capital": float(
                    metric["cumulative_return_on_capital"]
                ),
                "daily_mark_to_market_annualized_sharpe": float(
                    true_daily_metrics["annualized_sharpe"]
                ),
                "daily_mark_to_market_maximum_drawdown_compounded_return": float(
                    true_daily_metrics["maximum_drawdown_compounded_return"]
                ),
            }
        )
    limitations = [
        "Call and Put daily opens are not simultaneous bid/ask fills.",
        "Intermediate daily marks prefer official option settlement; missing settlement fields fall back to that contract's official daily last trade.",
        "Only opening commissions are charged because terminal rows represent synthetic cash settlement.",
        "No settlement handling fee, slippage, or depth constraint is modeled.",
        "Daily drawdown uses end-of-day marks and therefore omits intraday mark-to-market drawdown.",
    ]
    if direction == -1:
        limitations.extend(
            [
                "This naked-short result is a pre-margin TWD P&L screen, not an executable capital return.",
                "Historical option margin, intraday margin calls, forced liquidation, and absorbing account ruin are not modeled.",
            ]
        )
    return {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy": strategy_id,
        "profitability_after_fixed_fee": float(
            cycles["net_after_fee_twd"].sum()
        ) > 0.0,
        "profitability_after_fixed_fee_and_transaction_tax": (
            float(net.sum()) > 0.0
        ),
        "engineering_boundary": (
            "complete fixed-contract multi-session accounting screen with official "
            "TXO final settlement for every included cycle and daily held-contract "
            "mark-to-market between entry and expiry"
            if direction == 1
            else "pre-margin naked-short TWD P&L screen with official TXO final "
            "settlement and daily mark-to-market; no capital return, margin calls, "
            "forced liquidation, or absorbing ruin"
        ),
        "formal_naked_short_backtest_complete": (
            None if direction == 1 else False
        ),
        "capital_return_reportable": direction == 1,
        "parameters": {
            "product": "TXO",
            "series_scope": "nearest expiry weekly W/F only",
            "position_side": normalized_side,
            "entry": (
                "next flat session, buy opening ATM Call and Put at each leg daily open"
                if direction == 1
                else "next flat session, sell opening ATM Call and Put at each leg daily open"
            ),
            "holding_policy": "keep fixed series and strike through resolved expiry session",
            "terminal_value": "official TAIFEX final settlement intrinsic value for every included cycle",
            "next_entry": "first eligible session strictly after prior expiry",
            "contracts_per_leg": 1,
            "multiplier_twd_per_point": TAIFEX_TXO_MULTIPLIER,
            "fee_per_contract_side_twd": fee,
            "commissioned_sides_per_cycle": 2,
            "fee_per_cycle_twd": 2.0 * fee,
            "terminal_broker_fee_twd": 0.0,
            "transaction_tax_in_primary_result": True,
            "option_premium_transaction_tax_rate": option_premium_tax_rate,
            "option_premium_transaction_tax_basis": (
                "each Call/Put opening premium amount, rounded per contract to TWD"
            ),
            "cash_settlement_transaction_tax_basis": (
                "one automatically exercised ITM leg's settlement index contract "
                "amount, rounded per contract to TWD"
            ),
            "cash_settlement_tax_rate_schedule": [
                {
                    "start_date": "2008-10-06",
                    "end_date_inclusive": (
                        TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_CHANGE_DATE
                        - pd.Timedelta(days=1)
                    ).isoformat(),
                    "rate": (
                        TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_BEFORE_2013_04_01
                    ),
                },
                {
                    "start_date": (
                        TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_CHANGE_DATE.isoformat()
                    ),
                    "end_date_inclusive": None,
                    "rate": TAIFEX_STOCK_INDEX_FUTURES_TAX_RATE_FROM_2013_04_01,
                },
            ],
            "tax_rounding": "per contract, nearest whole TWD, half up",
            "slippage_points": 0.0,
        },
        "coverage": {
            "source_first_date": first_date.isoformat(),
            "source_last_date": last_date.isoformat(),
            "source_sessions": int(len(source)),
            "planned_cycles": int(len(plans)),
            "complete_cycles": int(len(cycles)),
            "excluded_cycles": int(len(excluded_cycles)),
            "skipped_flat_entry_sessions": int(len(skipped_entries)),
            "first_entry_date": str(cycles["entry_date"].min()),
            "last_expiry_date": str(cycles["expiry_date"].max()),
            "mean_holding_sessions": float(cycles["holding_sessions"].mean()),
            "observed_cycles_per_year": float(len(cycles) / elapsed_years),
            "official_final_settlement_cycles": int(
                cycles["terminal_value_source"]
                .eq("official_taifex_final_settlement_price")
                .sum()
            ),
            "expiry_last_trade_proxy_cycles": int(
                cycles["terminal_value_source"]
                .eq("expiry_day_last_trade_cash_settlement_proxy")
                .sum()
            ),
            "expiry_last_trade_validation_cycles": int(
                cycles["expiry_last_trade_proxy_terminal_points"].notna().sum()
            ),
        },
        "results": results,
        "data_contract": {
            "version": TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
            "entry_price_source": TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
            "terminal_price_source": "official_taifex_final_settlement_history",
            "input_path": str(input_path),
            "input_sha256": _sha256(input_path),
            "manifest_path": str(manifest_path),
            "manifest_sha256": _sha256(manifest_path),
            "raw_receipts": len(raw_paths),
            "raw_receipt_sha256": {
                str(path): _sha256(path) for path in raw_paths
            },
            "official_final_settlement_snapshot_url": OFFICIAL_FINAL_SETTLEMENT_URL,
            "official_final_settlement_snapshot_path": str(final_settlement_path),
            "official_final_settlement_snapshot_sha256": (
                _sha256(final_settlement_path)
                if final_settlement_path.is_file()
                else None
            ),
            "transaction_tax_sources": {
                "act": TAIFEX_TRANSACTION_TAX_ACT_URL,
                "taifex_qa": TAIFEX_TRANSACTION_TAX_QA_URL,
                "per_contract_rounding": TAIFEX_TRANSACTION_TAX_ROUNDING_URL,
                "2013_stock_index_futures_rate_change": (
                    TAIFEX_STOCK_INDEX_FUTURES_2013_TAX_SOURCE_URL
                ),
                "current_stock_index_futures_rate": (
                    TAIFEX_STOCK_INDEX_FUTURES_CURRENT_TAX_SOURCE_URL
                ),
            },
        },
        "limitations": limitations,
        "artifacts": {
            key: {"path": str(path), "sha256": _sha256(path)}
            for key, path in artifact_paths.items()
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument(
        "--final-settlement-path",
        type=Path,
        default=DEFAULT_FINAL_SETTLEMENT_PATH,
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--position-side",
        choices=TAIFEX_OPTION_POSITION_SIDES,
        default="long",
    )
    parser.add_argument("--fee-per-contract-side-twd", type=float, default=22.0)
    parser.add_argument(
        "--option-premium-tax-rate",
        type=float,
        default=TAIFEX_OPTION_PREMIUM_TAX_RATE,
    )
    args = parser.parse_args()
    if args.fee_per_contract_side_twd < 0.0:
        parser.error("--fee-per-contract-side-twd must be non-negative")
    if (
        not math.isfinite(args.option_premium_tax_rate)
        or args.option_premium_tax_rate < 0.0
    ):
        parser.error("--option-premium-tax-rate must be finite and non-negative")

    input_path = args.input.expanduser().resolve()
    raw_root = args.raw_root.expanduser().resolve()
    final_settlement_path = args.final_settlement_path.expanduser().resolve()
    position_side = str(args.position_side)
    direction = _position_direction(position_side)
    strategy_id = _strategy_id(position_side)
    output_dir = (
        args.output_dir
        if args.output_dir is not None
        else DEFAULT_OUTPUT_DIRS[position_side]
    ).expanduser().resolve()
    table = load_taifex_weekly_atm_straddles(input_path)
    source = table.to_pandas()
    official_settlements = _load_official_final_settlements(final_settlement_path)
    plans, skipped_entries = _plan_cycles(source, official_settlements)
    sessions = sorted(pd.to_datetime(source["date"]).dt.date.unique().tolist())
    targets = {
        (session, row.option_series, float(row.strike), right)
        for row in plans.itertuples(index=False)
        for session in sessions
        if row.entry_date <= session <= row.expiry_date
        for right in ("C", "P")
    }
    raw_paths = _raw_paths(raw_root)
    contract_rows = load_taifex_option_daily_contract_rows(
        raw_paths,
        targets,
        series_scope="weekly",
    )
    cycles, trades, excluded_cycles = _build_cycle_results(
        plans,
        contract_rows,
        fee_per_contract_side_twd=args.fee_per_contract_side_twd,
        option_premium_tax_rate=args.option_premium_tax_rate,
        position_side=position_side,
        official_settlements=official_settlements,
    )
    _assert_accounting(cycles, trades)

    capital_daily: pl.DataFrame | None = None
    capital_metrics: pl.DataFrame | None = None
    if direction == 1:
        observations_per_year = max(
            len(cycles)
            / max(
                (
                    pd.Timestamp(cycles["expiry_date"].max())
                    - pd.Timestamp(cycles["entry_date"].min())
                ).days
                / 365.2425,
                1.0,
            ),
            1.0,
        )
        capital_daily, capital_metrics = build_capital_normalized_returns(
            pl.from_pandas(
                cycles.rename(columns={"date": "trading_date"})
                .assign(
                    benchmark_family=FAMILY,
                    variant_id=strategy_id,
                    capital_normalized_pnl_twd=cycles["net_pnl_twd"].to_numpy(),
                )[
                    [
                        "trading_date",
                        "benchmark_family",
                        "variant_id",
                        "capital_normalized_pnl_twd",
                    ]
                ]
                .rename(
                    columns={"capital_normalized_pnl_twd": "net_after_fee_twd"}
                )[
                    [
                        "trading_date",
                        "benchmark_family",
                        "variant_id",
                        "net_after_fee_twd",
                    ]
                ]
            ),
            pl.from_pandas(trades),
            periods_per_year=observations_per_year,
        )
    true_daily, true_daily_metrics = _build_daily_results(
        cycles,
        contract_rows,
        sessions,
        capital_base_twd=(
            float(capital_metrics.item(0, "capital_base_twd"))
            if capital_metrics is not None
            else None
        ),
    )
    annual = _aggregate(cycles, "year")
    monthly = _aggregate(cycles, "month")

    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths: dict[str, Path] = {
        "cycles": output_dir / "cycles.parquet",
        "trades": output_dir / "trades.parquet",
        "annual_results": output_dir / "annual_results.csv",
        "monthly_results": output_dir / "monthly_results.csv",
        "skipped_entry_sessions": output_dir / "skipped_entry_sessions.csv",
        "excluded_cycles": output_dir / "excluded_cycles.csv",
        "daily_results": output_dir / "daily_results.parquet",
        "daily_metrics": output_dir / "daily_metrics.csv",
    }
    if capital_metrics is not None:
        artifact_paths.update(
            {
                "capital_normalized_cycles": (
                    output_dir / "capital_normalized_cycles.parquet"
                ),
                "capital_metrics": output_dir / "capital_metrics.csv",
            }
        )
    _atomic_parquet(cycles, artifact_paths["cycles"])
    _atomic_parquet(trades, artifact_paths["trades"])
    _atomic_csv(annual, artifact_paths["annual_results"])
    _atomic_csv(monthly, artifact_paths["monthly_results"])
    _atomic_csv(skipped_entries, artifact_paths["skipped_entry_sessions"])
    _atomic_csv(excluded_cycles, artifact_paths["excluded_cycles"])
    if capital_daily is not None and capital_metrics is not None:
        capital_daily_path = artifact_paths["capital_normalized_cycles"]
        capital_daily_temp = capital_daily_path.with_suffix(".parquet.tmp")
        capital_daily.write_parquet(capital_daily_temp)
        capital_daily_temp.replace(capital_daily_path)
        capital_metrics_path = artifact_paths["capital_metrics"]
        capital_metrics_temp = capital_metrics_path.with_suffix(".csv.tmp")
        capital_metrics.write_csv(capital_metrics_temp)
        capital_metrics_temp.replace(capital_metrics_path)
    _atomic_parquet(true_daily, artifact_paths["daily_results"])
    _atomic_csv(
        pd.DataFrame([true_daily_metrics]),
        artifact_paths["daily_metrics"],
    )

    manifest_path = input_path.parent / "manifest_weekly.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"weekly manifest does not exist: {manifest_path}")
    summary = _summary(
        source,
        plans,
        cycles,
        skipped_entries,
        excluded_cycles,
        capital_metrics,
        true_daily_metrics,
        input_path=input_path,
        manifest_path=manifest_path,
        raw_paths=raw_paths,
        final_settlement_path=final_settlement_path,
        fee=args.fee_per_contract_side_twd,
        option_premium_tax_rate=args.option_premium_tax_rate,
        position_side=position_side,
        artifact_paths=artifact_paths,
    )
    atomic_write_json(output_dir / "summary.json", summary)
    print(
        f"Hold-to-expiry ATM {position_side} straddle: {len(cycles):,} complete cycles, "
        f"net TWD {summary['results']['net_pnl_twd']:,.2f}, "
        f"fees TWD {summary['results']['fees_twd']:,.2f}, "
        f"taxes TWD {summary['results']['transaction_taxes_twd']:,.2f}"
        + (
            ", capital return "
            f"{summary['results']['one_lot_cumulative_return_on_capital']:.2%}"
            if direction == 1
            else ", pre-margin P&L screen (capital return not reportable)"
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
