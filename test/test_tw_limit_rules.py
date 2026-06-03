#!/usr/bin/env python3
"""Regression tests for TW limit-up/limit-down pricing and execution constraints."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stockagent.backtest.simulator import run_backtest_integer_shares
from stockagent.data.panel import _compute_tw_limit_masks, _tw_limit_price


def _assert_close(actual: float, expected: float, tol: float = 1e-9) -> None:
    if not math.isfinite(actual) or abs(actual - expected) > tol:
        raise AssertionError(f"expected {expected}, got {actual}")


def _shares_by_date(records, symbol: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in records:
        if row.symbol == symbol:
            out[row.date] = int(row.shares)
    return out


def test_limit_price_examples() -> None:
    prev = pd.Series([9.95, 10.0, 49.8, 50.0, 99.9, 100.0, 499.0, 500.0, 999.0, 1000.0])
    up = _tw_limit_price(prev, 1.10)
    down = _tw_limit_price(prev, 0.90)

    expected_up = [10.9, 11.0, 54.7, 55.0, 109.5, 110.0, 548.0, 550.0, 1095.0, 1100.0]
    expected_down = [8.95, 9.0, 44.8, 45.0, 89.9, 90.0, 449.0, 450.0, 899.0, 900.0]

    for actual, expected in zip(up.tolist(), expected_up):
        _assert_close(float(actual), float(expected))
    for actual, expected in zip(down.tolist(), expected_down):
        _assert_close(float(actual), float(expected))


def test_limit_masks() -> None:
    frame = pd.DataFrame(
        {
            "tradable": [True, True, True],
            "close_raw": [100.0, 110.0, 90.0],
        }
    )

    can_buy, can_sell = _compute_tw_limit_masks(frame)

    # First row has no prev_close, only tradable rule applies.
    assert bool(can_buy.iloc[0]) is True
    assert bool(can_sell.iloc[0]) is True

    # Row 1 is limit-up from prev_close=100 -> cannot buy, can sell.
    assert bool(can_buy.iloc[1]) is False
    assert bool(can_sell.iloc[1]) is True

    # Row 2 is limit-down (or lower) from prev_close=110 -> can buy, cannot sell.
    assert bool(can_buy.iloc[2]) is True
    assert bool(can_sell.iloc[2]) is False


def test_limit_masks_ex_dividend_reference_price() -> None:
    # Day 1 has ex-dividend cash=1.0. Reference should be prev_close - dividend.
    # prev_close=35.1 -> reference=34.1 -> limit_up=37.5, so close=37.5 is limit-up.
    frame = pd.DataFrame(
        {
            "tradable": [True, True],
            "close_raw": [35.1, 37.5],
            "Dividends": [0.0, 1.0],
            "Stock Splits": [0.0, 0.0],
        }
    )

    can_buy, can_sell = _compute_tw_limit_masks(frame)

    assert bool(can_buy.iloc[1]) is False
    assert bool(can_sell.iloc[1]) is True


def test_limit_masks_split_reference_price() -> None:
    # Day 1 has 2-for-1 split. Reference should be prev_close / 2.
    # prev_close=100 -> reference=50 -> limit_up=55.0, so close=55.0 is limit-up.
    frame = pd.DataFrame(
        {
            "tradable": [True, True],
            "close_raw": [100.0, 55.0],
            "Dividends": [0.0, 0.0],
            "Stock Splits": [0.0, 2.0],
        }
    )

    can_buy, can_sell = _compute_tw_limit_masks(frame)

    assert bool(can_buy.iloc[1]) is False
    assert bool(can_sell.iloc[1]) is True


def test_no_buy_on_limit_up_day() -> None:
    # With one-symbol long-only backtest, positive weights are normalized to full exposure.
    # Use 0->1 target transition so day 1 genuinely attempts to buy.
    weights = np.array([[0.0], [1.0], [1.0]], dtype=np.float32)
    future_returns = np.zeros_like(weights)
    tradable = np.ones_like(weights, dtype=bool)

    # Day 1 is limit-up day: cannot buy.
    can_buy = np.array([[True], [False], [True]], dtype=bool)
    can_sell = np.ones_like(weights, dtype=bool)

    benchmark = np.zeros((weights.shape[0],), dtype=np.float32)
    close_prices = np.array([[100.0], [110.0], [110.0]], dtype=np.float32)
    dates = np.array(["2024-01-01", "2024-01-02", "2024-01-03"], dtype="datetime64[D]")

    _, records = run_backtest_integer_shares(
        weights=weights,
        future_returns=future_returns,
        tradable_mask=tradable,
        benchmark_returns=benchmark,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        initial_capital=1000.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        close_prices=close_prices,
        symbols=["A"],
        dates=dates,
    )

    shares = _shares_by_date(records, "A")
    assert shares.get("2024-01-01", 0) == 0
    assert shares.get("2024-01-02", 0) == 0, "should not increase shares on limit-up day"
    assert shares.get("2024-01-03", 0) == 9, "should buy after limit-up constraint is lifted"


def test_no_sell_on_limit_down_day() -> None:
    weights = np.array([[1.0], [0.0], [0.0]], dtype=np.float32)
    future_returns = np.zeros_like(weights)
    tradable = np.ones_like(weights, dtype=bool)

    can_buy = np.ones_like(weights, dtype=bool)
    # Day 1 is limit-down day: cannot sell.
    can_sell = np.array([[True], [False], [True]], dtype=bool)

    benchmark = np.zeros((weights.shape[0],), dtype=np.float32)
    close_prices = np.array([[100.0], [90.0], [90.0]], dtype=np.float32)
    dates = np.array(["2024-01-01", "2024-01-02", "2024-01-03"], dtype="datetime64[D]")

    _, records = run_backtest_integer_shares(
        weights=weights,
        future_returns=future_returns,
        tradable_mask=tradable,
        benchmark_returns=benchmark,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        initial_capital=1000.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        close_prices=close_prices,
        symbols=["A"],
        dates=dates,
    )

    shares = _shares_by_date(records, "A")
    assert shares["2024-01-01"] == 10
    assert shares["2024-01-02"] == 10, "should not decrease shares on limit-down day"


def test_tw_holdings_price_has_no_float_tail() -> None:
    weights = np.array([[1.0]], dtype=np.float32)
    future_returns = np.zeros_like(weights)
    tradable = np.ones_like(weights, dtype=bool)
    can_buy = np.ones_like(weights, dtype=bool)
    can_sell = np.ones_like(weights, dtype=bool)
    benchmark = np.zeros((weights.shape[0],), dtype=np.float32)

    # Mimic float32 tail value seen in raw parquet/csv pipelines.
    close_prices = np.array([[21.399999618530273]], dtype=np.float32)
    dates = np.array(["2024-01-01"], dtype="datetime64[D]")

    _, records = run_backtest_integer_shares(
        weights=weights,
        future_returns=future_returns,
        tradable_mask=tradable,
        benchmark_returns=benchmark,
        can_buy_mask=can_buy,
        can_sell_mask=can_sell,
        initial_capital=1000.0,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        close_prices=close_prices,
        symbols=["3516"],
        dates=dates,
    )

    stock_rows = [row for row in records if (not row.is_cash and row.symbol == "3516")]
    assert len(stock_rows) == 1
    assert stock_rows[0].price == 21.4


def main() -> None:
    test_limit_price_examples()
    test_limit_masks()
    test_limit_masks_ex_dividend_reference_price()
    test_limit_masks_split_reference_price()
    test_no_buy_on_limit_up_day()
    test_no_sell_on_limit_down_day()
    test_tw_holdings_price_has_no_float_tail()
    print("All TW limit rule tests passed.")


if __name__ == "__main__":
    main()
