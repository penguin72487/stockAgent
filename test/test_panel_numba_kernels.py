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


def test_tw_limit_mask_kernel_rounds_limit_down_up_to_tick() -> None:
    close_raw = np.array([524.0, 472.0], dtype=np.float64)
    tradable = np.array([True, True])
    dividends = np.full(close_raw.shape, np.nan)
    stock_splits = np.full(close_raw.shape, np.nan)

    limit_down = panel_numba.tw_limit_price(np.array([524.0], dtype=np.float64), 0.90)
    can_buy, can_sell = panel_numba.tw_limit_masks_from_arrays(close_raw, tradable, dividends, stock_splits)

    assert float(limit_down[0]) == 472.0
    assert can_buy.tolist() == [True, True]
    assert can_sell.tolist() == [True, False]


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
def test_small_serial_dispatch_matches_original_parallel_price_kernels():
    import stockagent.data.panel_numba as kernels
    assert (kernels._safe_log_ratio_flat._cache._impl._filename_base !=
            kernels._safe_log_ratio_serial._cache._impl._filename_base)
    assert (kernels._shift_rows_flat.py_func.__code__.co_code ==
            kernels._shift_rows_serial.py_func.__code__.co_code)
    import numba
    previous=numba.get_num_threads()
    try:
        numba.set_num_threads(min(4,previous))
        values=np.array([0.,1.,-1.,np.nan,np.inf,4.999,5.,9.999,10.,50.,99.95,100.,500.,1000.])
        dates=np.array([730000,740000]*7,dtype=np.int64)
        cases=[(kernels._round_half_up_flat,kernels._round_half_up_serial,(values,100.)),
            (kernels._tw_tick_size_flat,kernels._tw_tick_size_serial,(values,dates)),
            (kernels._tw_limit_price_flat,kernels._tw_limit_price_serial,(values,1.10,dates)),
            (kernels._tw_limit_price_flat,kernels._tw_limit_price_serial,(values,.90,dates)),
            (kernels._shift_rows_flat,kernels._shift_rows_serial,(values,len(values),1,2)),
            (kernels._safe_log_ratio_flat,kernels._safe_log_ratio_serial,(values,values+1)),
            (kernels._sanitize_price_log_return_flat,kernels._sanitize_price_log_return_serial,(values,.5)),
            (kernels._tw_limit_masks_kernel,kernels._tw_limit_masks_serial,(values,np.ones(len(values),dtype=bool),np.zeros(len(values)),np.ones(len(values)),dates))]
        for parallel,serial,args in cases:
            left,right=parallel(*args),serial(*args)
            if isinstance(left,tuple):
                for a,b in zip(left,right):np.testing.assert_array_equal(a,b)
            else:np.testing.assert_array_equal(left,right)
    finally:numba.set_num_threads(previous)
