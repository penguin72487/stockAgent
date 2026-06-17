from __future__ import annotations

import math

import numpy as np
import numba as nb


@nb.njit(parallel=True, cache=True)
def _round_half_up_flat(values: np.ndarray, factor: float) -> np.ndarray:
    out = np.empty(values.size, dtype=np.float64)
    for idx in nb.prange(values.size):
        value = float(values[idx])
        if not math.isfinite(value):
            out[idx] = np.nan
        elif value >= 0.0:
            out[idx] = math.floor(value * factor + 0.5) / factor
        else:
            out[idx] = math.ceil(value * factor - 0.5) / factor
    return out


@nb.njit(parallel=True, cache=True)
def _tw_tick_size_flat(price: np.ndarray) -> np.ndarray:
    out = np.empty(price.size, dtype=np.float64)
    for idx in nb.prange(price.size):
        value = float(price[idx])
        if not math.isfinite(value) or value <= 0.0:
            out[idx] = np.nan
        elif value < 10.0:
            out[idx] = 0.01
        elif value < 50.0:
            out[idx] = 0.05
        elif value < 100.0:
            out[idx] = 0.1
        elif value < 500.0:
            out[idx] = 0.5
        elif value < 1000.0:
            out[idx] = 1.0
        else:
            out[idx] = 5.0
    return out


@nb.njit(parallel=True, cache=True)
def _tw_limit_price_flat(prev_close: np.ndarray, ratio: float) -> np.ndarray:
    out = np.empty(prev_close.size, dtype=np.float64)
    for idx in nb.prange(prev_close.size):
        prev = float(prev_close[idx])
        theoretical = prev * ratio
        if not math.isfinite(theoretical) or theoretical <= 0.0:
            out[idx] = np.nan
            continue
        if theoretical < 10.0:
            tick = 0.01
        elif theoretical < 50.0:
            tick = 0.05
        elif theoretical < 100.0:
            tick = 0.1
        elif theoretical < 500.0:
            tick = 0.5
        elif theoretical < 1000.0:
            tick = 1.0
        else:
            tick = 5.0
        floored = math.floor((theoretical / tick) + 1e-12) * tick
        if not math.isfinite(floored):
            out[idx] = np.nan
        elif floored >= 0.0:
            out[idx] = math.floor(floored * 100.0 + 0.5) / 100.0
        else:
            out[idx] = math.ceil(floored * 100.0 - 0.5) / 100.0
    return out


@nb.njit(parallel=True, cache=True)
def _shift_rows_flat(values: np.ndarray, rows: int, row_width: int, periods: int) -> np.ndarray:
    out = np.empty(values.size, dtype=np.float64)
    if periods == 0:
        for idx in nb.prange(values.size):
            out[idx] = values[idx]
        return out
    for row in nb.prange(rows):
        source_row = row - periods
        for col in range(row_width):
            out_idx = row * row_width + col
            if source_row < 0 or source_row >= rows:
                out[out_idx] = np.nan
            else:
                out[out_idx] = values[source_row * row_width + col]
    return out


@nb.njit(parallel=True, cache=True)
def _safe_log_ratio_flat(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    out = np.empty(numerator.size, dtype=np.float64)
    for idx in nb.prange(numerator.size):
        num = float(numerator[idx])
        den = float(denominator[idx])
        if math.isfinite(num) and math.isfinite(den) and num > 0.0 and den > 0.0:
            out[idx] = math.log(num / den)
        else:
            out[idx] = np.nan
    return out


@nb.njit(parallel=True, cache=True)
def _sanitize_price_log_return_flat(values: np.ndarray, max_abs: float) -> np.ndarray:
    out = np.empty(values.size, dtype=np.float64)
    for idx in nb.prange(values.size):
        value = float(values[idx])
        if math.isfinite(value) and abs(value) > max_abs:
            out[idx] = np.nan
        else:
            out[idx] = value
    return out


@nb.njit(parallel=True, cache=True)
def _tw_limit_masks_kernel(
    close_raw: np.ndarray,
    tradable: np.ndarray,
    dividends: np.ndarray,
    stock_splits: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    rows = close_raw.size
    can_buy = np.empty(rows, dtype=np.bool_)
    can_sell = np.empty(rows, dtype=np.bool_)
    for idx in nb.prange(rows):
        base = bool(tradable[idx])
        close_value = float(close_raw[idx])
        if not math.isfinite(close_value):
            close = np.nan
        elif close_value >= 0.0:
            close = math.floor(close_value * 100.0 + 0.5) / 100.0
        else:
            close = math.ceil(close_value * 100.0 - 0.5) / 100.0

        if idx <= 0:
            reference = np.nan
        else:
            prev_value = float(close_raw[idx - 1])
            if not math.isfinite(prev_value):
                reference = np.nan
            elif prev_value >= 0.0:
                reference = math.floor(prev_value * 100.0 + 0.5) / 100.0
            else:
                reference = math.ceil(prev_value * 100.0 - 0.5) / 100.0

        dividend = float(dividends[idx])
        if math.isfinite(dividend):
            reference -= dividend

        split = float(stock_splits[idx])
        if math.isfinite(split) and split > 0.0 and split != 1.0:
            reference /= split

        if math.isfinite(reference) and reference > 0.0:
            if reference >= 0.0:
                reference = math.floor(reference * 100.0 + 0.5) / 100.0
            else:
                reference = math.ceil(reference * 100.0 - 0.5) / 100.0
        else:
            reference = np.nan

        is_limit_up = False
        is_limit_down = False
        if math.isfinite(reference) and reference > 0.0 and math.isfinite(close):
            theoretical_up = reference * 1.10
            theoretical_down = reference * 0.90
            if theoretical_up < 10.0:
                tick_up = 0.01
            elif theoretical_up < 50.0:
                tick_up = 0.05
            elif theoretical_up < 100.0:
                tick_up = 0.1
            elif theoretical_up < 500.0:
                tick_up = 0.5
            elif theoretical_up < 1000.0:
                tick_up = 1.0
            else:
                tick_up = 5.0
            if theoretical_down < 10.0:
                tick_down = 0.01
            elif theoretical_down < 50.0:
                tick_down = 0.05
            elif theoretical_down < 100.0:
                tick_down = 0.1
            elif theoretical_down < 500.0:
                tick_down = 0.5
            elif theoretical_down < 1000.0:
                tick_down = 1.0
            else:
                tick_down = 5.0

            limit_up = math.floor((theoretical_up / tick_up) + 1e-12) * tick_up
            limit_down = math.floor((theoretical_down / tick_down) + 1e-12) * tick_down
            limit_up = math.floor(limit_up * 100.0 + 0.5) / 100.0
            limit_down = math.floor(limit_down * 100.0 + 0.5) / 100.0
            is_limit_up = close >= (limit_up - 1e-9)
            is_limit_down = close <= (limit_down + 1e-9)

        can_buy[idx] = base and not is_limit_up
        can_sell[idx] = base and not is_limit_down
    return can_buy, can_sell


def _as_float64_contiguous(values: np.ndarray) -> np.ndarray:
    return np.ascontiguousarray(np.asarray(values, dtype=np.float64))


def round_half_up(values: np.ndarray, decimals: int = 2) -> np.ndarray:
    arr = _as_float64_contiguous(values)
    factor = float(10**int(decimals))
    return _round_half_up_flat(arr.reshape(-1), factor).reshape(arr.shape)


def tw_tick_size(price: np.ndarray) -> np.ndarray:
    arr = _as_float64_contiguous(price)
    return _tw_tick_size_flat(arr.reshape(-1)).reshape(arr.shape)


def tw_limit_price(prev_close: np.ndarray, ratio: float) -> np.ndarray:
    arr = _as_float64_contiguous(prev_close)
    return _tw_limit_price_flat(arr.reshape(-1), float(ratio)).reshape(arr.shape)


def shift_array(values: np.ndarray, periods: int) -> np.ndarray:
    arr = _as_float64_contiguous(values)
    if arr.ndim == 0:
        return arr.copy()
    rows = int(arr.shape[0])
    row_width = int(arr.size // max(rows, 1)) if rows > 0 else 0
    return _shift_rows_flat(arr.reshape(-1), rows, row_width, int(periods)).reshape(arr.shape)


def safe_log_ratio_array(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    num = _as_float64_contiguous(numerator)
    den = _as_float64_contiguous(denominator)
    if num.shape != den.shape:
        num, den = np.broadcast_arrays(num, den)
        num = _as_float64_contiguous(num)
        den = _as_float64_contiguous(den)
    return _safe_log_ratio_flat(num.reshape(-1), den.reshape(-1)).reshape(num.shape)


def sanitize_price_log_return_array(values: np.ndarray, max_abs: float) -> np.ndarray:
    arr = _as_float64_contiguous(values)
    return _sanitize_price_log_return_flat(arr.reshape(-1), float(max_abs)).reshape(arr.shape)


def tw_limit_masks_from_arrays(
    close_raw: np.ndarray,
    tradable: np.ndarray,
    dividends: np.ndarray,
    stock_splits: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    close = _as_float64_contiguous(close_raw).reshape(-1)
    base = np.ascontiguousarray(np.asarray(tradable, dtype=np.bool_)).reshape(-1)
    div = _as_float64_contiguous(dividends).reshape(-1)
    splits = _as_float64_contiguous(stock_splits).reshape(-1)
    if not (close.size == base.size == div.size == splits.size):
        raise ValueError("TW limit mask inputs must have the same flattened length")
    return _tw_limit_masks_kernel(close, base, div, splits)
