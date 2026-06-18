from __future__ import annotations

import math

import numpy as np

from stockagent.data import panel_numba


def test_round_shift_log_and_sanitize_kernels() -> None:
    values = np.array([[1.005, -1.005], [np.nan, np.inf]], dtype=np.float64)
    rounded = panel_numba.round_half_up(values, decimals=2)
    assert np.allclose(rounded[:1], np.array([[1.0, -1.0]]))
    assert np.isnan(rounded[1, 0])
    assert np.isnan(rounded[1, 1])

    shifted = panel_numba.shift_array(np.array([[1.0, 2.0], [3.0, 4.0]]), 1)
    expected_shifted = np.array([[np.nan, np.nan], [1.0, 2.0]])
    assert np.allclose(shifted[1:], expected_shifted[1:])
    assert np.isnan(shifted[0]).all()

    log_ratio = panel_numba.safe_log_ratio_array(
        np.array([2.0, -1.0, np.nan, np.inf]),
        np.array([1.0, 1.0, 1.0, 1.0]),
    )
    assert math.isclose(float(log_ratio[0]), math.log(2.0), rel_tol=1e-12)
    assert np.isnan(log_ratio[1:]).all()

    sanitized = panel_numba.sanitize_price_log_return_array(np.array([0.1, 2.0, -2.0, np.inf]), math.log(5.0))
    assert sanitized[0] == 0.1
    assert np.isnan(sanitized[1])
    assert np.isnan(sanitized[2])
    assert np.isinf(sanitized[3])


def test_tw_limit_mask_kernel_matches_limit_rule_examples() -> None:
    close_raw = np.array([100.0, 110.0, 90.0], dtype=np.float64)
    tradable = np.array([True, True, True])
    dividends = np.full(close_raw.shape, np.nan)
    stock_splits = np.full(close_raw.shape, np.nan)

    can_buy, can_sell = panel_numba.tw_limit_masks_from_arrays(close_raw, tradable, dividends, stock_splits)

    assert can_buy.tolist() == [True, False, True]
    assert can_sell.tolist() == [True, True, False]


def test_tw_limit_mask_kernel_handles_dividends_and_splits() -> None:
    can_buy_div, can_sell_div = panel_numba.tw_limit_masks_from_arrays(
        np.array([35.1, 37.5]),
        np.array([True, True]),
        np.array([0.0, 1.0]),
        np.array([0.0, 0.0]),
    )
    assert can_buy_div.tolist() == [True, False]
    assert can_sell_div.tolist() == [True, True]

    can_buy_split, can_sell_split = panel_numba.tw_limit_masks_from_arrays(
        np.array([100.0, 55.0]),
        np.array([True, True]),
        np.array([0.0, 0.0]),
        np.array([0.0, 2.0]),
    )
    assert can_buy_split.tolist() == [True, False]
    assert can_sell_split.tolist() == [True, True]
