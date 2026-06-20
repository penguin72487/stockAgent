from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
import hashlib
import pickle
import os
from typing import Any

import numpy as np

try:
    import polars as pl
except Exception:  # pragma: no cover - optional parquet reader
    pl = None

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional parquet reader
    pa = None
    pc = None
    pq = None

from stockagent.data.panel_cache import (
    legacy_panel_cache_path,
    legacy_panel_meta_path,
    load_panel_cache_v2,
    panel_cache_v2_is_valid,
    panel_cache_v2_dir,
    save_panel_cache_v2,
)

try:
    from stockagent.data import panel_numba as _panel_numba
except Exception:  # pragma: no cover - Numba is an acceleration dependency
    _panel_numba = None


RESERVED_COLUMNS = {"date", "symbol", "return_1d", "tradable"}
LOG_RETURN_FEATURE_COLUMNS = [
    # ==================================================
    # Price Log Return
    # 前一日價格變化
    # ==================================================
    "open_logret_1d",
    "max_logret_1d",
    "min_logret_1d",
    "close_logret_1d",

    # ==================================================
    # Volume
    # 成交量變化
    # ==================================================
    "trading_volume_logret_1d",
    "signed_vol",

    # ==================================================
    # Intraday Price Structure
    # 日內價格結構
    # ==================================================
    # "intraday_return_co",
    # "overnight_gap_oc",
    # "intraday_range",

    # ==================================================
    # Body
    # K棒實體
    # ==================================================
    "body_ratio",
    "signed_body_ratio",
    "delta_body_ratio",

    # ==================================================
    # CLV
    # 收盤位置
    # ==================================================
    "clv",
    "clv_centered",
    "delta_clv",

    # ==================================================
    # Shadow
    # 上下影線
    # ==================================================
    "upper_shadow",
    "lower_shadow",
    "shadow_imbalance",
]
PANEL_CACHE_VERSION = 19
FEATURE_FILE_SUFFIX = "_features.parquet"
EPSILON = 1e-8
# Treat single-day price moves beyond +/-100% log-return magnitude as unusable
# labels/features. The full US universe contains stale/delisted Yahoo rows with
# penny-to-thousands jumps that otherwise dominate log-return backtests.
MAX_ABS_DAILY_PRICE_LOG_RETURN = float(np.log(5.0))
PREV_DAY_LOG_RETURN_RENAME = {
    "open": "open_logret_1d",
    "max": "max_logret_1d",
    "min": "min_logret_1d",
    "close": "close_logret_1d",
    "Trading_Volume": "trading_volume_logret_1d",
}
_MISSING_VOLUME_WARNED_SYMBOLS: set[str] = set()


class _MissingTradingVolumeError(ValueError):
    pass


def _normalize_trading_volume_policy(policy: str | bool | None) -> str:
    if isinstance(policy, bool):
        return "required" if policy else "optional"
    normalized = str(policy or "auto").strip().lower()
    if normalized not in {"auto", "required", "optional"}:
        raise ValueError(
            "trading_volume_policy must be one of: auto, required, optional; "
            f"got {policy!r}"
        )
    return normalized


def _path_requires_trading_volume(path: Path, policy: str | bool | None) -> bool:
    normalized = _normalize_trading_volume_policy(policy)
    if normalized == "required":
        return True
    if normalized == "optional":
        return False
    parts = {part.lower() for part in path.parts}
    path_text = path.as_posix().lower()
    if {"forex", "forex_pepperstone", "data_forex_frankfurter"} & parts:
        return False
    if "frankfurter" in path_text or "pepperstone" in path_text:
        return False
    volume_assets = {"tw_stocks", "us_stocks", "crypto", "data_parquet", "data_okx", "data_bybit"}
    return bool(volume_assets & parts)


def _require_trading_volume_column(path: Path, columns: set[str], policy: str | bool | None) -> None:
    if "Trading_Volume" in columns or not _path_requires_trading_volume(path, policy):
        return
    raise _MissingTradingVolumeError(
        f"{path.name} is missing required Trading_Volume column under "
        f"trading_volume_policy={_normalize_trading_volume_policy(policy)!r}. "
        "Use trading_volume_policy='optional' only for assets without meaningful volume."
    )


def _round_half_up(values: np.ndarray, decimals: int = 2) -> np.ndarray:
    """Round with half-up semantics (0.5 always rounds away from zero)."""
    if _panel_numba is not None:
        return _panel_numba.round_half_up(values, decimals=decimals)
    arr = np.asarray(values, dtype=np.float64)
    factor = float(10**decimals)
    out = np.full(arr.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(arr)
    pos = valid & (arr >= 0.0)
    neg = valid & (arr < 0.0)
    out[pos] = np.floor(arr[pos] * factor + 0.5) / factor
    out[neg] = np.ceil(arr[neg] * factor - 0.5) / factor
    return out

def _price_decimals_for_path(path: Path) -> int:
    """Return market-specific price precision: TW=2, others=8 decimals."""
    parts = {part.lower() for part in path.parts}
    symbol = _symbol_name_from_path(path)
    is_tw_market = "tw_stocks" in parts or symbol.isdigit()
    return 2 if is_tw_market else 8


def _return_price_column(frame: Any, path: Path) -> str:
    """Choose the price series used for forward return labels."""
    # Use adjusted close whenever available so corporate actions
    # (splits/dividends/capital changes) do not create fake label jumps.
    if "adjclose" in frame.columns:
        return "adjclose"
    return "close"


@dataclass(slots=True)
class PanelData:
    dates: np.ndarray
    symbols: list[str]
    feature_names: list[str]
    features: np.ndarray
    returns_1d: np.ndarray
    tradable_mask: np.ndarray
    alive_mask: np.ndarray
    benchmark_returns: np.ndarray
    close_prices: np.ndarray
    can_buy_mask: np.ndarray | None = None
    can_sell_mask: np.ndarray | None = None

    @property
    def num_dates(self) -> int:
        return int(self.features.shape[0])

    @property
    def num_symbols(self) -> int:
        return int(self.features.shape[1])


@dataclass(slots=True)
class _SymbolPanelArrays:
    symbol: str
    dates: np.ndarray
    features: np.ndarray
    returns_1d: np.ndarray
    close_prices: np.ndarray
    tradable_mask: np.ndarray
    can_buy_mask: np.ndarray
    can_sell_mask: np.ndarray
    alive_mask: np.ndarray


def _symbol_name_from_path(path: Path) -> str:
    return path.name.removesuffix(FEATURE_FILE_SUFFIX)


def _is_usd_trading_pair(path: Path) -> bool:
    return _symbol_name_from_path(path).upper().endswith("USD")


def _tw_tick_size(price: np.ndarray) -> np.ndarray:
    """TWSE tick size by price bucket (vectorized)."""
    if _panel_numba is not None:
        return _panel_numba.tw_tick_size(price)
    p = np.asarray(price, dtype=np.float64)
    tick = np.full(p.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(p) & (p > 0.0)
    tick[valid] = 5.0
    tick[valid & (p < 1000.0)] = 1.0
    tick[valid & (p < 500.0)] = 0.5
    tick[valid & (p < 100.0)] = 0.1
    tick[valid & (p < 50.0)] = 0.05
    tick[valid & (p < 10.0)] = 0.01
    return tick


def _to_float_array(values: Any, rows: int | None = None, default: float = np.nan) -> np.ndarray:
    if values is None:
        if rows is None:
            return np.asarray([], dtype=np.float64)
        return np.full(int(rows), default, dtype=np.float64)
    if pl is not None and isinstance(values, pl.Series):
        values = values.to_numpy()
    arr = np.asarray(values)
    try:
        return arr.astype(np.float64, copy=False)
    except (TypeError, ValueError):
        out = np.full(arr.shape, default, dtype=np.float64)
        flat = out.reshape(-1)
        for idx, value in enumerate(arr.reshape(-1)):
            try:
                flat[idx] = float(value)
            except (TypeError, ValueError):
                flat[idx] = default
        return out


def _frame_height(frame: Any) -> int:
    if pl is not None and isinstance(frame, pl.DataFrame):
        return int(frame.height)
    return int(len(frame))


def _frame_column_float_array(frame: Any, name: str, *, default: float = np.nan) -> np.ndarray:
    rows = _frame_height(frame)
    if name not in frame.columns:
        return np.full(rows, default, dtype=np.float64)
    if pl is not None and isinstance(frame, pl.DataFrame):
        return frame.get_column(name).cast(pl.Float64, strict=False).to_numpy()
    return _to_float_array(frame[name], rows=rows, default=default)


def _frame_column_bool_array(frame: Any, name: str, *, default: bool = False) -> np.ndarray:
    rows = _frame_height(frame)
    if name not in frame.columns:
        return np.full(rows, default, dtype=bool)
    if pl is not None and isinstance(frame, pl.DataFrame):
        return frame.get_column(name).cast(pl.Boolean, strict=False).fill_null(default).to_numpy()
    return np.asarray(frame[name], dtype=bool)


def _tw_limit_price(prev_close: np.ndarray, ratio: float) -> np.ndarray:
    """Compute TW daily limit price with floor-to-tick rule from theoretical price."""
    if _panel_numba is not None:
        return _panel_numba.tw_limit_price(prev_close, ratio)
    prev = _to_float_array(prev_close)
    theoretical = prev * ratio
    tick = _tw_tick_size(theoretical)

    out = np.full(theoretical.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(theoretical) & np.isfinite(tick) & (tick > 0.0)
    # Small epsilon avoids floating-point edge cases around exact tick boundaries.
    out[valid] = np.floor((theoretical[valid] / tick[valid]) + 1e-12) * tick[valid]
    return _round_half_up(out, decimals=2)


def _tw_reference_price_for_limits(frame: Any, prev_close_raw: np.ndarray) -> np.ndarray:
    """Compute TW daily reference price used for limit-up/down checks.

    Base rule uses previous close, then applies ex-right/ex-dividend adjustments
    when source columns are available:
    - Dividends: subtract cash dividend on ex-dividend day.
    - Stock Splits: divide by split ratio on ex-right day.
    """
    reference = _to_float_array(prev_close_raw).astype(np.float64, copy=True)

    if "Dividends" in frame.columns:
        dividends = np.nan_to_num(_frame_column_float_array(frame, "Dividends"), nan=0.0)
        reference = reference - dividends

    if "Stock Splits" in frame.columns:
        split_ratio = _frame_column_float_array(frame, "Stock Splits")
        valid_split = np.isfinite(split_ratio) & (split_ratio > 0.0) & (split_ratio != 1.0)
        reference[valid_split] = reference[valid_split] / split_ratio[valid_split]

    reference = np.where(reference > 0.0, reference, np.nan)
    return _round_half_up(reference, decimals=2)


def _compute_tw_limit_masks(frame: Any) -> tuple[np.ndarray, np.ndarray]:
    """Return (can_buy, can_sell) masks under TW 10% daily limit assumptions.

    Rule:
    - limit-up day: cannot buy, can sell
    - limit-down day: can buy, cannot sell
    """
    tradable = _frame_column_bool_array(frame, "tradable")
    close_raw = _round_half_up(_frame_column_float_array(frame, "close_raw"), decimals=2)
    if _panel_numba is not None:
        dividends = (
            _frame_column_float_array(frame, "Dividends")
            if "Dividends" in frame.columns
            else np.full(tradable.shape, np.nan, dtype=np.float64)
        )
        stock_splits = (
            _frame_column_float_array(frame, "Stock Splits")
            if "Stock Splits" in frame.columns
            else np.full(tradable.shape, np.nan, dtype=np.float64)
        )
        return _panel_numba.tw_limit_masks_from_arrays(close_raw, tradable, dividends, stock_splits)
    prev_close_raw = _shift_array(close_raw, 1)
    reference_price = _tw_reference_price_for_limits(frame, prev_close_raw)

    limit_up_price = _tw_limit_price(reference_price, 1.10)
    limit_down_price = _tw_limit_price(reference_price, 0.90)

    # Use small price tolerance to absorb source rounding noise.
    is_limit_up = (close_raw >= (limit_up_price - 1e-9)) & (reference_price > 0.0)
    is_limit_down = (close_raw <= (limit_down_price + 1e-9)) & (reference_price > 0.0)

    can_buy = tradable & ~np.nan_to_num(is_limit_up, nan=False).astype(bool)
    can_sell = tradable & ~np.nan_to_num(is_limit_down, nan=False).astype(bool)
    return can_buy, can_sell


def _warn_missing_trading_volume(path: Path) -> None:
    symbol = _symbol_name_from_path(path)
    if symbol in _MISSING_VOLUME_WARNED_SYMBOLS:
        return
    _MISSING_VOLUME_WARNED_SYMBOLS.add(symbol)
    print(
        f"[panel] WARN {path.name}: missing Trading_Volume column; "
        "volume features (trading_volume_logret_1d, signed_vol) will be NaN"
    )


def _polars_datetime_ns_expr(schema: dict[str, Any], column: str = "date") -> Any:
    if pl is None:
        raise RuntimeError("Polars is not available")
    dtype = schema.get(column)
    expr = pl.col(column)
    if dtype == pl.String:
        return expr.str.to_datetime(strict=False).cast(pl.Datetime("ns"), strict=False).alias(column)
    return expr.cast(pl.Datetime("ns"), strict=False).alias(column)


def _prepare_symbol_frame(frame: Any, path: Path) -> Any:
    if pl is None:
        raise RuntimeError("_prepare_symbol_frame requires polars")
    if not isinstance(frame, pl.DataFrame):
        if pa is not None and isinstance(frame, pa.Table):
            frame = pl.from_arrow(frame)
        else:
            frame = pl.DataFrame(frame)
    if "date" not in frame.columns:
        raise ValueError(f"{path.name} is missing required date column")

    price_decimals = _price_decimals_for_path(path)

    def num(name: str):
        if name in frame.columns:
            return pl.col(name).cast(pl.Float64, strict=False)
        return pl.lit(None, dtype=pl.Float64)

    frame = (
        frame.with_columns(_polars_datetime_ns_expr(frame.schema, "date"))
        .drop_nulls("date")
        .sort("date")
        .with_columns(
            [
                _polars_round_half_up(num("open"), price_decimals).alias("open"),
                _polars_round_half_up(num("max"), price_decimals).alias("max"),
                _polars_round_half_up(num("min"), price_decimals).alias("min"),
                _polars_round_half_up(num("close"), price_decimals).alias("close"),
                _polars_round_half_up(num("adjclose"), price_decimals).alias("adjclose"),
                pl.lit(_symbol_name_from_path(path)).alias("symbol"),
            ]
        )
        .with_columns(pl.col("close").cast(pl.Float32, strict=False).alias("close_raw"))
    )

    spread = (pl.col("max") - pl.col("min")).clip(0.0, None)
    denom = spread + EPSILON
    return_price = pl.col(_return_price_column(frame, path))
    close_valid = _polars_not_nan_or_null(pl.col("close"))
    if "Trading_Volume" in frame.columns:
        volume = num("Trading_Volume")
        volume_missing = volume.is_null() | volume.is_nan().fill_null(False)
        tradable_expr = close_valid & ((volume.fill_nan(0.0).fill_null(0.0) > 0.0) | volume_missing)
    else:
        _warn_missing_trading_volume(path)
        volume = pl.lit(None, dtype=pl.Float64)
        tradable_expr = close_valid

    frame = frame.with_columns(
        [
            _polars_safe_log(pl.col("close"), pl.col("open")).alias("intraday_return_co"),
            _polars_safe_log(pl.col("open"), pl.col("close").shift(1)).alias("overnight_gap_oc"),
            _polars_safe_log(pl.col("max"), pl.col("min")).alias("intraday_range"),
            ((pl.col("close") - pl.col("open")).abs() / denom).alias("body_ratio"),
            ((pl.col("close") - pl.col("open")) / denom).alias("signed_body_ratio"),
            ((pl.col("close") - pl.col("min")) / denom).alias("clv"),
            ((pl.col("max") - pl.max_horizontal("open", "close")) / denom).alias("upper_shadow"),
            ((pl.min_horizontal("open", "close") - pl.col("min")) / denom).alias("lower_shadow"),
            _polars_price_log_return(return_price.shift(-1), return_price).alias("return_1d"),
            _polars_price_log_return(pl.col("open"), pl.col("open").shift(1)).alias("open_logret_1d"),
            _polars_price_log_return(pl.col("max"), pl.col("max").shift(1)).alias("max_logret_1d"),
            _polars_price_log_return(pl.col("min"), pl.col("min").shift(1)).alias("min_logret_1d"),
            _polars_price_log_return(pl.col("close"), pl.col("close").shift(1)).alias("close_logret_1d"),
            _polars_safe_log(volume, volume.shift(1)).alias("trading_volume_logret_1d"),
            tradable_expr.alias("tradable"),
        ]
    )
    frame = frame.with_columns(
        [
            (pl.col("clv") - 0.5).alias("clv_centered"),
            (pl.col("upper_shadow") - pl.col("lower_shadow")).alias("shadow_imbalance"),
            (pl.col("clv") - pl.col("clv").shift(1)).alias("delta_clv"),
            (pl.col("body_ratio") - pl.col("body_ratio").shift(1)).alias("delta_body_ratio"),
            (pl.col("intraday_return_co").sign() * pl.col("trading_volume_logret_1d")).alias("signed_vol"),
        ]
    )
    for col in LOG_RETURN_FEATURE_COLUMNS:
        if col not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
    return frame


def _load_symbol_frame(path: Path) -> Any:
    if pq is None:
        raise RuntimeError("PyArrow is not available")
    return _prepare_symbol_frame(pq.read_table(path), path)


def _coerce_arrow_numeric_column(table, name: str, rows: int) -> np.ndarray:
    if name not in table.column_names:
        return np.full(rows, np.nan, dtype=np.float64)
    column = table[name].combine_chunks()
    values = column.to_numpy(zero_copy_only=False)
    try:
        return np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError):
        if pc is not None and pa is not None:
            try:
                casted = pc.cast(column, pa.float64(), safe=False)
                return np.asarray(casted.to_numpy(zero_copy_only=False), dtype=np.float64)
            except Exception:
                pass
        if pl is not None:
            return pl.Series(values).cast(pl.Float64, strict=False).to_numpy()
        return _to_float_array(values, rows=rows)


def _coerce_arrow_datetime_ns_column(table, name: str, rows: int) -> np.ndarray:
    if name not in table.column_names:
        return np.full(rows, np.datetime64("NaT", "ns"), dtype="datetime64[ns]")
    column = table[name].combine_chunks()
    values = column.to_numpy(zero_copy_only=False)
    try:
        return np.asarray(values, dtype="datetime64[ns]")
    except (TypeError, ValueError):
        if pc is not None and pa is not None:
            try:
                casted = pc.cast(column, pa.timestamp("ns"), safe=False)
                return np.asarray(casted.to_numpy(zero_copy_only=False), dtype="datetime64[ns]")
            except Exception:
                pass
        if pl is not None:
            return (
                pl.Series(values)
                .cast(pl.String, strict=False)
                .str.to_datetime(strict=False)
                .to_numpy()
                .astype("datetime64[ns]", copy=False)
            )
        out = np.full(rows, np.datetime64("NaT", "ns"), dtype="datetime64[ns]")
        flat = np.asarray(values).reshape(-1)
        for idx, value in enumerate(flat[:rows]):
            try:
                out[idx] = np.datetime64(str(value), "ns")
            except Exception:
                out[idx] = np.datetime64("NaT", "ns")
        return out


def _shift_array(values: np.ndarray, periods: int) -> np.ndarray:
    if _panel_numba is not None:
        return _panel_numba.shift_array(values, periods)
    arr = np.asarray(values, dtype=np.float64)
    out = np.full(arr.shape, np.nan, dtype=np.float64)
    if periods > 0:
        out[periods:] = arr[:-periods]
    elif periods < 0:
        out[:periods] = arr[-periods:]
    else:
        out[:] = arr
    return out


def _safe_log_ratio_array(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    if _panel_numba is not None:
        return _panel_numba.safe_log_ratio_array(numerator, denominator)
    num = np.asarray(numerator, dtype=np.float64)
    den = np.asarray(denominator, dtype=np.float64)
    out = np.full(num.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(num) & np.isfinite(den) & (num > 0.0) & (den > 0.0)
    np.divide(num, den, out=out, where=valid)
    np.log(out, out=out, where=valid)
    out[~valid] = np.nan
    return out


def _sanitize_price_log_return_array(values: np.ndarray) -> np.ndarray:
    if _panel_numba is not None:
        return _panel_numba.sanitize_price_log_return_array(values, MAX_ABS_DAILY_PRICE_LOG_RETURN)
    out = np.asarray(values, dtype=np.float64).copy()
    invalid = np.isfinite(out) & (np.abs(out) > MAX_ABS_DAILY_PRICE_LOG_RETURN)
    out[invalid] = np.nan
    return out


def _polars_safe_log(num, den):
    if pl is None:
        raise RuntimeError("Polars is not available")
    return (
        pl.when(num.is_finite() & den.is_finite() & (num > 0.0) & (den > 0.0))
        .then((num / den).log())
        .otherwise(None)
    )


def _polars_sanitize_price_log_return(expr):
    if pl is None:
        raise RuntimeError("Polars is not available")
    return (
        pl.when(expr.is_null() | ~expr.is_finite())
        .then(None)
        .when(expr.abs() > MAX_ABS_DAILY_PRICE_LOG_RETURN)
        .then(None)
        .otherwise(expr)
    )


def _polars_price_log_return(num, den):
    return _polars_sanitize_price_log_return(_polars_safe_log(num, den))


def _polars_round_half_up(expr, decimals: int):
    factor = float(10**int(decimals))
    return (
        pl.when(expr.is_null() | expr.is_nan())
        .then(None)
        .when(expr >= 0.0)
        .then(((expr * factor) + 0.5).floor() / factor)
        .otherwise(((expr * factor) - 0.5).ceil() / factor)
    )


def _collect_polars_lazy_frame(lazy, *, engine: str = "auto"):
    engine = str(engine or "auto").strip().lower()
    if engine not in {"auto", "streaming"}:
        raise ValueError(f"Unsupported Polars collect engine: {engine!r}")
    try:
        return lazy.collect(engine=engine)
    except TypeError:
        if engine == "streaming":
            return lazy.collect(streaming=True)
        return lazy.collect()


def _polars_not_nan_or_null(expr):
    return expr.is_not_null() & ~expr.is_nan().fill_null(False)


def _tw_limit_masks_from_arrays(
    close_raw: np.ndarray,
    tradable: np.ndarray,
    dividends: np.ndarray,
    stock_splits: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    if _panel_numba is not None:
        return _panel_numba.tw_limit_masks_from_arrays(close_raw, tradable, dividends, stock_splits)
    close = _round_half_up(np.asarray(close_raw, dtype=np.float64), decimals=2)
    prev_close = _shift_array(close, 1)
    reference = np.asarray(prev_close, dtype=np.float64).copy()

    div = np.nan_to_num(np.asarray(dividends, dtype=np.float64), nan=0.0)
    reference = reference - div

    splits = np.asarray(stock_splits, dtype=np.float64)
    valid_split = np.isfinite(splits) & (splits > 0.0) & (splits != 1.0)
    reference[valid_split] = reference[valid_split] / splits[valid_split]
    reference = np.where(reference > 0.0, reference, np.nan)

    limit_up = _tw_limit_price(reference, 1.10).astype(np.float64, copy=False)
    limit_down = _tw_limit_price(reference, 0.90).astype(np.float64, copy=False)

    base = np.asarray(tradable, dtype=bool)
    is_limit_up = np.isfinite(reference) & (close >= (limit_up - 1e-9))
    is_limit_down = np.isfinite(reference) & (close <= (limit_down + 1e-9))
    return base & ~is_limit_up, base & ~is_limit_down


def _load_symbol_arrays_pyarrow(
    path: Path,
    tradable_mode: str = "tradable",
    trading_volume_policy: str | bool | None = "auto",
) -> _SymbolPanelArrays:
    if pq is None:
        raise RuntimeError("PyArrow is not available")

    table = pq.read_table(path)
    _require_trading_volume_column(path, set(table.column_names), trading_volume_policy)
    rows = int(table.num_rows)
    if rows == 0:
        empty_1d = np.empty((0,), dtype=np.float32)
        empty_mask = np.empty((0,), dtype=bool)
        return _SymbolPanelArrays(
            symbol=_symbol_name_from_path(path),
            dates=np.empty((0,), dtype="datetime64[ns]"),
            features=np.empty((0, len(LOG_RETURN_FEATURE_COLUMNS)), dtype=np.float32),
            returns_1d=empty_1d,
            close_prices=empty_1d,
            tradable_mask=empty_mask,
            can_buy_mask=empty_mask,
            can_sell_mask=empty_mask,
            alive_mask=empty_mask,
        )

    dates = _coerce_arrow_datetime_ns_column(table, "date", rows)
    order = np.argsort(dates)
    dates = dates[order]

    def col(name: str) -> np.ndarray:
        return _coerce_arrow_numeric_column(table, name, rows)[order]

    price_decimals = _price_decimals_for_path(path)
    open_px = _round_half_up(col("open"), decimals=price_decimals)
    high_px = _round_half_up(col("max"), decimals=price_decimals)
    low_px = _round_half_up(col("min"), decimals=price_decimals)
    close_px = _round_half_up(col("close"), decimals=price_decimals)
    adjclose = _round_half_up(col("adjclose"), decimals=price_decimals)
    volume = col("Trading_Volume")

    spread = np.clip(high_px - low_px, 0.0, None)
    denom = spread + EPSILON
    intraday_return_co = _safe_log_ratio_array(close_px, open_px)
    body_ratio = np.abs(close_px - open_px) / denom
    signed_body_ratio = (close_px - open_px) / denom
    clv = (close_px - low_px) / denom
    clv_centered = clv - 0.5
    upper_shadow = (high_px - np.maximum(open_px, close_px)) / denom
    lower_shadow = (np.minimum(open_px, close_px) - low_px) / denom
    shadow_imbalance = upper_shadow - lower_shadow
    delta_clv = clv - _shift_array(clv, 1)
    delta_body_ratio = body_ratio - _shift_array(body_ratio, 1)

    return_price = adjclose if "adjclose" in table.column_names else close_px
    return_1d = _safe_log_ratio_array(_shift_array(return_price, -1), return_price)
    open_logret_1d = _safe_log_ratio_array(open_px, _shift_array(open_px, 1))
    max_logret_1d = _safe_log_ratio_array(high_px, _shift_array(high_px, 1))
    min_logret_1d = _safe_log_ratio_array(low_px, _shift_array(low_px, 1))
    close_logret_1d = _safe_log_ratio_array(close_px, _shift_array(close_px, 1))
    return_1d = _sanitize_price_log_return_array(return_1d)
    open_logret_1d = _sanitize_price_log_return_array(open_logret_1d)
    max_logret_1d = _sanitize_price_log_return_array(max_logret_1d)
    min_logret_1d = _sanitize_price_log_return_array(min_logret_1d)
    close_logret_1d = _sanitize_price_log_return_array(close_logret_1d)
    trading_volume_logret_1d = _safe_log_ratio_array(volume, _shift_array(volume, 1))
    signed_vol = np.sign(intraday_return_co) * trading_volume_logret_1d

    if "Trading_Volume" not in table.column_names:
        _warn_missing_trading_volume(path)
        trading_volume_logret_1d[:] = np.nan
        signed_vol[:] = np.nan

    feature_map = {
        "open_logret_1d": open_logret_1d,
        "max_logret_1d": max_logret_1d,
        "min_logret_1d": min_logret_1d,
        "close_logret_1d": close_logret_1d,
        "trading_volume_logret_1d": trading_volume_logret_1d,
        "signed_vol": signed_vol,
        "body_ratio": body_ratio,
        "signed_body_ratio": signed_body_ratio,
        "delta_body_ratio": delta_body_ratio,
        "clv": clv,
        "clv_centered": clv_centered,
        "delta_clv": delta_clv,
        "upper_shadow": upper_shadow,
        "lower_shadow": lower_shadow,
        "shadow_imbalance": shadow_imbalance,
    }
    features = np.column_stack([feature_map[name] for name in LOG_RETURN_FEATURE_COLUMNS]).astype(np.float32, copy=False)

    close_notna = ~np.isnan(close_px)
    if "Trading_Volume" in table.column_names:
        volume_missing = np.isnan(volume)
        tradable = close_notna & ((np.nan_to_num(volume, nan=0.0) > 0.0) | volume_missing)
    else:
        tradable = close_notna

    valid_dates = ~np.isnat(dates)
    if not bool(valid_dates.all()):
        dates = dates[valid_dates]
        features = features[valid_dates]
        return_1d = return_1d[valid_dates]
        close_px = close_px[valid_dates]
        tradable = tradable[valid_dates]
        close_notna = close_notna[valid_dates]

    tradable = np.asarray(tradable, dtype=bool)
    if tradable_mode == "tw_limit_guard":
        dividends = col("Dividends") if "Dividends" in table.column_names else np.full(tradable.shape, np.nan)
        stock_splits = col("Stock Splits") if "Stock Splits" in table.column_names else np.full(tradable.shape, np.nan)
        if not bool(valid_dates.all()):
            dividends = dividends[valid_dates]
            stock_splits = stock_splits[valid_dates]
        can_buy_mask, can_sell_mask = _tw_limit_masks_from_arrays(close_px, tradable, dividends, stock_splits)
    elif tradable_mode == "tradable":
        can_buy_mask = tradable.copy()
        can_sell_mask = tradable.copy()
    else:
        raise RuntimeError(f"Unsupported tradable_mode for PyArrow panel backend: {tradable_mode!r}")
    return _SymbolPanelArrays(
        symbol=_symbol_name_from_path(path),
        dates=dates,
        features=features,
        returns_1d=return_1d.astype(np.float32, copy=False),
        close_prices=close_px.astype(np.float32, copy=False),
        tradable_mask=tradable,
        can_buy_mask=np.asarray(can_buy_mask, dtype=bool),
        can_sell_mask=np.asarray(can_sell_mask, dtype=bool),
        alive_mask=np.asarray(close_notna, dtype=bool),
    )


def _load_symbol_arrays_polars_lazy(
    path: Path,
    tradable_mode: str = "tradable",
    *,
    collect_engine: str = "auto",
    trading_volume_policy: str | bool | None = "auto",
) -> _SymbolPanelArrays:
    if pl is None:
        raise RuntimeError("Polars is not available")
    if pq is None:
        raise RuntimeError("PyArrow is not available")

    frame = pl.from_arrow(pq.read_table(path, memory_map=True))
    lazy = frame.lazy().sort("date")
    schema_names = set(frame.columns)
    _require_trading_volume_column(path, schema_names, trading_volume_policy)
    price_decimals = _price_decimals_for_path(path)

    def num(name: str):
        if name in schema_names:
            return pl.col(name).cast(pl.Float64, strict=False)
        return pl.lit(None, dtype=pl.Float64)

    price_columns = [
        _polars_round_half_up(num("open"), price_decimals).alias("_open"),
        _polars_round_half_up(num("max"), price_decimals).alias("_max"),
        _polars_round_half_up(num("min"), price_decimals).alias("_min"),
        _polars_round_half_up(num("close"), price_decimals).alias("_close"),
        _polars_round_half_up(num("adjclose"), price_decimals).alias("_adjclose"),
        num("Trading_Volume").alias("_volume"),
    ]
    if tradable_mode == "tw_limit_guard":
        price_columns.extend(
            [
                num("Dividends").alias("_dividends"),
                num("Stock Splits").alias("_stock_splits"),
            ]
        )
    lazy = lazy.with_columns(price_columns)
    spread = (pl.col("_max") - pl.col("_min")).clip(0.0, None)
    denom = spread + EPSILON
    return_price = pl.col("_adjclose") if "adjclose" in schema_names else pl.col("_close")
    close_valid = _polars_not_nan_or_null(pl.col("_close"))
    if "Trading_Volume" in schema_names:
        volume_missing = pl.col("_volume").is_null() | pl.col("_volume").is_nan().fill_null(False)
        tradable_expr = close_valid & (
            (pl.col("_volume").fill_nan(0.0).fill_null(0.0) > 0.0) | volume_missing
        )
    else:
        tradable_expr = close_valid

    lazy = lazy.with_columns(
        [
            _polars_safe_log(pl.col("_close"), pl.col("_open")).alias("intraday_return_co"),
            ((pl.col("_close") - pl.col("_open")).abs() / denom).alias("body_ratio"),
            ((pl.col("_close") - pl.col("_open")) / denom).alias("signed_body_ratio"),
            ((pl.col("_close") - pl.col("_min")) / denom).alias("clv"),
            ((pl.col("_max") - pl.max_horizontal("_open", "_close")) / denom).alias("upper_shadow"),
            ((pl.min_horizontal("_open", "_close") - pl.col("_min")) / denom).alias("lower_shadow"),
            _polars_price_log_return(return_price.shift(-1), return_price).alias("return_1d"),
            _polars_price_log_return(pl.col("_open"), pl.col("_open").shift(1)).alias("open_logret_1d"),
            _polars_price_log_return(pl.col("_max"), pl.col("_max").shift(1)).alias("max_logret_1d"),
            _polars_price_log_return(pl.col("_min"), pl.col("_min").shift(1)).alias("min_logret_1d"),
            _polars_price_log_return(pl.col("_close"), pl.col("_close").shift(1)).alias("close_logret_1d"),
            _polars_safe_log(pl.col("_volume"), pl.col("_volume").shift(1)).alias("trading_volume_logret_1d"),
            tradable_expr.alias("tradable"),
        ]
    )
    lazy = lazy.with_columns(
        [
            (pl.col("clv") - 0.5).alias("clv_centered"),
            (pl.col("upper_shadow") - pl.col("lower_shadow")).alias("shadow_imbalance"),
            (pl.col("clv") - pl.col("clv").shift(1)).alias("delta_clv"),
            (pl.col("body_ratio") - pl.col("body_ratio").shift(1)).alias("delta_body_ratio"),
            (pl.col("intraday_return_co").sign() * pl.col("trading_volume_logret_1d")).alias("signed_vol"),
        ]
    )
    selected_columns = [
        _polars_datetime_ns_expr(frame.schema, "date"),
        pl.col("_close").alias("close_px"),
        pl.col("return_1d"),
        pl.col("tradable"),
        *[pl.col(name) for name in LOG_RETURN_FEATURE_COLUMNS],
    ]
    if tradable_mode == "tw_limit_guard":
        selected_columns[2:2] = [
            pl.col("_dividends").alias("dividends"),
            pl.col("_stock_splits").alias("stock_splits"),
        ]
    out = _collect_polars_lazy_frame(lazy.select(selected_columns), engine=collect_engine)

    rows = int(out.height)
    if rows == 0:
        empty_1d = np.empty((0,), dtype=np.float32)
        empty_mask = np.empty((0,), dtype=bool)
        return _SymbolPanelArrays(
            symbol=_symbol_name_from_path(path),
            dates=np.empty((0,), dtype="datetime64[ns]"),
            features=np.empty((0, len(LOG_RETURN_FEATURE_COLUMNS)), dtype=np.float32),
            returns_1d=empty_1d,
            close_prices=empty_1d,
            tradable_mask=empty_mask,
            can_buy_mask=empty_mask,
            can_sell_mask=empty_mask,
            alive_mask=empty_mask,
        )

    dates = out["date"].to_numpy().astype("datetime64[ns]", copy=False)
    close_px = out["close_px"].to_numpy().astype(np.float64, copy=False)
    return_1d = out["return_1d"].to_numpy().astype(np.float64, copy=False)
    tradable = out["tradable"].to_numpy().astype(bool, copy=False)
    features = np.column_stack(
        [out[name].to_numpy().astype(np.float64, copy=False) for name in LOG_RETURN_FEATURE_COLUMNS]
    ).astype(np.float32, copy=False)
    close_notna = ~np.isnan(close_px)

    valid_dates = ~np.isnat(dates)
    if not bool(valid_dates.all()):
        dates = dates[valid_dates]
        features = features[valid_dates]
        return_1d = return_1d[valid_dates]
        close_px = close_px[valid_dates]
        tradable = tradable[valid_dates]
        close_notna = close_notna[valid_dates]

    if tradable_mode == "tw_limit_guard":
        dividends = out["dividends"].to_numpy().astype(np.float64, copy=False)
        stock_splits = out["stock_splits"].to_numpy().astype(np.float64, copy=False)
        if not bool(valid_dates.all()):
            dividends = dividends[valid_dates]
            stock_splits = stock_splits[valid_dates]
        can_buy_mask, can_sell_mask = _tw_limit_masks_from_arrays(close_px, tradable, dividends, stock_splits)
    elif tradable_mode == "tradable":
        can_buy_mask = tradable.copy()
        can_sell_mask = tradable.copy()
    else:
        raise RuntimeError(f"Unsupported tradable_mode for Polars Lazy panel backend: {tradable_mode!r}")

    return _SymbolPanelArrays(
        symbol=_symbol_name_from_path(path),
        dates=dates,
        features=features,
        returns_1d=return_1d.astype(np.float32, copy=False),
        close_prices=close_px.astype(np.float32, copy=False),
        tradable_mask=np.asarray(tradable, dtype=bool),
        can_buy_mask=np.asarray(can_buy_mask, dtype=bool),
        can_sell_mask=np.asarray(can_sell_mask, dtype=bool),
        alive_mask=np.asarray(close_notna, dtype=bool),
    )


def _build_panel_from_symbol_arrays(
    symbol_arrays: list[_SymbolPanelArrays],
    benchmark_name: str = "universe_average_return",
) -> PanelData:
    if not symbol_arrays:
        raise RuntimeError("No valid parquet files could be loaded.")

    symbols = [item.symbol for item in symbol_arrays]
    dated_items = [item.dates for item in symbol_arrays if item.dates.size]
    if not dated_items:
        raise RuntimeError("No valid dated rows could be loaded.")
    all_dates = np.unique(np.concatenate(dated_items))
    all_dates.sort()
    num_dates = int(all_dates.size)
    num_symbols = len(symbol_arrays)
    num_features = len(LOG_RETURN_FEATURE_COLUMNS)

    features = np.full((num_dates, num_symbols, num_features), np.nan, dtype=np.float32)
    returns_1d = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    close_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    tradable_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    can_buy_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    can_sell_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    alive_mask = np.zeros((num_dates, num_symbols), dtype=bool)

    for sym_idx, item in enumerate(symbol_arrays):
        if item.dates.size == 0:
            continue
        row_idx = np.searchsorted(all_dates, item.dates)
        valid = (row_idx >= 0) & (row_idx < num_dates) & (all_dates[row_idx] == item.dates)
        if not bool(valid.all()):
            row_idx = row_idx[valid]
        features[row_idx, sym_idx, :] = item.features[valid]
        returns_1d[row_idx, sym_idx] = item.returns_1d[valid]
        close_prices[row_idx, sym_idx] = item.close_prices[valid]
        tradable_mask[row_idx, sym_idx] = item.tradable_mask[valid]
        can_buy_mask[row_idx, sym_idx] = item.can_buy_mask[valid]
        can_sell_mask[row_idx, sym_idx] = item.can_sell_mask[valid]
        alive_mask[row_idx, sym_idx] = item.alive_mask[valid]

    benchmark_symbol_index = _resolve_benchmark_index(symbols, benchmark_name)
    if benchmark_symbol_index is None:
        valid_returns = np.isfinite(returns_1d)
        n_valid = valid_returns.sum(axis=1)
        sum_ret = np.nansum(np.where(valid_returns, returns_1d, 0.0), axis=1)
        benchmark_returns = np.zeros_like(sum_ret, dtype=np.float32)
        np.divide(sum_ret, n_valid, out=benchmark_returns, where=n_valid > 0)
    else:
        benchmark_returns = np.nan_to_num(
            returns_1d[:, benchmark_symbol_index],
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        ).astype(np.float32, copy=False)

    features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)
    return PanelData(
        dates=np.asarray(all_dates, dtype="datetime64[ns]"),
        symbols=symbols,
        feature_names=list(LOG_RETURN_FEATURE_COLUMNS),
        features=features,
        returns_1d=returns_1d,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        alive_mask=alive_mask,
        benchmark_returns=benchmark_returns,
        close_prices=close_prices,
    )


def _resolve_benchmark_index(symbols: list[str], benchmark_name: str) -> int | None:
    key = (str(benchmark_name) if benchmark_name is not None else "").strip()
    if not key:
        return None

    if key.lower() in {"universe_average_return", "universe_average", "universe", "average"}:
        return None

    normalized = key.upper().replace("-", "").replace("_", "")
    alias_candidates = [normalized]
    if not normalized.endswith("USD"):
        alias_candidates.append(f"{normalized}USD")

    symbol_to_idx = {symbol.upper(): idx for idx, symbol in enumerate(symbols)}
    for candidate in alias_candidates:
        if candidate in symbol_to_idx:
            return symbol_to_idx[candidate]

    known = ", ".join(symbols[:10])
    raise ValueError(
        f"benchmark_name={benchmark_name!r} not found in panel symbols. "
        f"Try one of: universe_average_return, BTC, BTCUSD (sample symbols: {known}...)"
    )


def _panel_cache_path(parquet_root: str | Path) -> Path:
    return legacy_panel_cache_path(parquet_root)


def _cache_meta_path(parquet_root: str | Path) -> Path:
    return legacy_panel_meta_path(parquet_root)


def _compute_source_hash(paths: list[Path]) -> str:
    """Compute hash of all parquet files' mtime and size."""
    hasher = hashlib.md5()
    for path in sorted(paths):
        mtime = path.stat().st_mtime
        size = path.stat().st_size
        hasher.update(f"{path.name}:{mtime}:{size}".encode())
    return hasher.hexdigest()


def _save_panel_cache(
    parquet_root: str | Path,
    panel: PanelData,
    source_hash: str,
    backend_key: str,
) -> None:
    save_panel_cache_v2(
        parquet_root,
        panel,
        source_hash=source_hash,
        backend_key=backend_key,
        version=PANEL_CACHE_VERSION,
    )


def _load_panel_cache(cache_path: Path) -> PanelData:
    cached = np.load(cache_path, allow_pickle=True)
    cached_keys = set(cached.files)
    tradable_mask = cached["tradable_mask"]
    can_buy_mask = cached["can_buy_mask"] if "can_buy_mask" in cached_keys else tradable_mask
    can_sell_mask = cached["can_sell_mask"] if "can_sell_mask" in cached_keys else tradable_mask
    return PanelData(
        dates=cached["dates"],
        symbols=cached["symbols"].tolist(),
        feature_names=cached["feature_names"].tolist(),
        features=cached["features"],
        returns_1d=cached["returns_1d"],
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        alive_mask=cached["alive_mask"],
        benchmark_returns=cached["benchmark_returns"],
        close_prices=cached["close_prices"],
    )


def _panel_from_cache_payload(payload: dict) -> PanelData:
    tradable_mask = payload["tradable_mask"]
    can_buy_mask = payload.get("can_buy_mask", tradable_mask)
    can_sell_mask = payload.get("can_sell_mask", tradable_mask)
    return PanelData(
        dates=payload["dates"],
        symbols=list(payload["symbols"]),
        feature_names=list(payload["feature_names"]),
        features=payload["features"],
        returns_1d=payload["returns_1d"],
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        alive_mask=payload["alive_mask"],
        benchmark_returns=payload["benchmark_returns"],
        close_prices=payload["close_prices"],
    )


def _print_feature_overview(panel: PanelData) -> None:
    feature_list = ", ".join(panel.feature_names)
    print(f"[panel] features ({len(panel.feature_names)}): {feature_list}")


def _check_cache_valid(cache_path: Path, meta_path: Path, parquet_paths: list[Path], backend_key: str) -> bool:
    """Check if cache is valid based on source hash and mtime."""
    if (not cache_path.exists()) or (not meta_path.exists()):
        return False
    
    try:
        with meta_path.open('rb') as f:
            meta = pickle.load(f)
        
        # ✅ OPTIMIZATION: Check both version and source hash for cache validity
        expected_hash = _compute_source_hash(parquet_paths)
        cache_valid = (
            meta.get('source_hash') == expected_hash and 
            meta.get('version') == PANEL_CACHE_VERSION and
            meta.get('backend_key') == backend_key
        )
        
        if cache_valid:
            # Also verify that cache file itself is newer than source files
            cache_mtime = cache_path.stat().st_mtime
            source_mtimes = [p.stat().st_mtime for p in parquet_paths]
            if cache_mtime < max(source_mtimes):
                # Cache is older than source files, invalidate
                return False
        
        return cache_valid
    except Exception as e:
        print(f"[panel] cache validation error: {e}")
        return False


def _load_valid_panel_cache(
    parquet_root: str | Path,
    parquet_paths: list[Path],
    backend_key: str,
    source_hash: str,
) -> PanelData | None:
    if panel_cache_v2_is_valid(
        parquet_root,
        source_hash=source_hash,
        backend_key=backend_key,
        version=PANEL_CACHE_VERSION,
        source_paths=parquet_paths,
    ):
        cache_dir = panel_cache_v2_dir(parquet_root)
        print(f"[panel] loading cache v2 (valid): {cache_dir}")
        return _panel_from_cache_payload(load_panel_cache_v2(parquet_root, mmap_mode="c"))

    cache_path = _panel_cache_path(parquet_root)
    meta_path = _cache_meta_path(parquet_root)
    if _check_cache_valid(cache_path, meta_path, parquet_paths, backend_key):
        print(f"[panel] loading legacy cache (valid): {cache_path}")
        return _load_panel_cache(cache_path)
    return None


def build_panel(
    parquet_root: str | Path,
    use_rapids: bool = True,
    benchmark_name: str = "universe_average_return",
    usd_only_trading_pairs: bool = False,
    tradable_mode: str = "tradable",
    trading_volume_policy: str | bool | None = "auto",
    strict_no_fallback: bool | None = None,
    buy_tradable_mode: str | None = None,
    sell_tradable_mode: str | None = None,
    panel_backend: str = "auto",
    panel_load_workers: int = 4,
) -> PanelData:
    parquet_root = Path(parquet_root)
    parquet_paths = sorted(parquet_root.glob(f"*{FEATURE_FILE_SUFFIX}"))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files found under {parquet_root}")

    if usd_only_trading_pairs:
        parquet_paths = [path for path in parquet_paths if _is_usd_trading_pair(path)]
        if not parquet_paths:
            raise FileNotFoundError(f"No USD trading pairs found under {parquet_root}")

    panel_backend = str(panel_backend).strip().lower()
    valid_backends = {"auto", "polars", "polars_lazy", "polars_streaming", "pyarrow"}
    if panel_backend not in valid_backends:
        raise ValueError(f"panel_backend must be one of {sorted(valid_backends)}, got {panel_backend!r}")
    panel_load_workers = max(0, int(panel_load_workers))
    if buy_tradable_mode is not None or sell_tradable_mode is not None:
        buy_mode = str(buy_tradable_mode if buy_tradable_mode is not None else tradable_mode).strip().lower()
        sell_mode = str(sell_tradable_mode if sell_tradable_mode is not None else tradable_mode).strip().lower()
        if buy_mode != sell_mode:
            raise ValueError(
                "buy_tradable_mode and sell_tradable_mode must be identical when provided"
            )
        tradable_mode = buy_mode

    tradable_mode = str(tradable_mode).strip().lower()
    valid_tradable_modes = {"tradable", "tw_limit_guard"}
    if tradable_mode not in valid_tradable_modes:
        raise ValueError(
            f"tradable_mode must be one of {sorted(valid_tradable_modes)}, got {tradable_mode!r}"
        )
    trading_volume_policy = _normalize_trading_volume_policy(trading_volume_policy)
    if strict_no_fallback is None:
        strict_no_fallback = str(os.getenv("STOCKAGENT_STRICT_NO_FALLBACK", "")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
    else:
        strict_no_fallback = bool(strict_no_fallback)

    if panel_backend == "pyarrow":
        if pq is None:
            raise RuntimeError("data.panel_backend='pyarrow' requires the pyarrow package")
        selected_backend = "pyarrow"
    elif panel_backend in {"polars", "polars_lazy", "polars_streaming"}:
        if pl is None or pq is None:
            raise RuntimeError(f"data.panel_backend={panel_backend!r} requires the polars and pyarrow packages")
        selected_backend = "polars_streaming" if panel_backend == "polars_streaming" else "polars_lazy"
    elif panel_backend == "auto" and pl is not None and pq is not None:
        selected_backend = "polars_lazy"
    elif panel_backend == "auto" and pq is not None:
        selected_backend = "pyarrow"
    else:
        raise RuntimeError("data.panel_backend='auto' requires pyarrow")

    backend_key = (
        f"{selected_backend}|benchmark={benchmark_name}|"
        f"usd_only={usd_only_trading_pairs}|tradable_mode={tradable_mode}|"
        f"trading_volume_policy={trading_volume_policy}"
    )
    source_hash = _compute_source_hash(parquet_paths)

    panel = _load_valid_panel_cache(parquet_root, parquet_paths, backend_key, source_hash)
    if panel is not None:
        _print_feature_overview(panel)
        return panel

    print(
        f"[panel] building from {len(parquet_paths)} parquet files "
        f"(backend={selected_backend}, workers={panel_load_workers})..."
    )
    polars_collect_engine = "streaming" if selected_backend == "polars_streaming" else "auto"

    def _load_one_arrays(path: Path) -> tuple[Path, _SymbolPanelArrays | None, Exception | None]:
        try:
            if selected_backend == "pyarrow":
                arrays = _load_symbol_arrays_pyarrow(
                    path,
                    tradable_mode=tradable_mode,
                    trading_volume_policy=trading_volume_policy,
                )
            else:
                arrays = _load_symbol_arrays_polars_lazy(
                    path,
                    tradable_mode=tradable_mode,
                    collect_engine=polars_collect_engine,
                    trading_volume_policy=trading_volume_policy,
                )
            if int(arrays.dates.size) == 0:
                raise ValueError(f"Symbol file is empty: {path.name}")
            return path, arrays, None
        except Exception as exc:
            return path, None, exc

    if panel_load_workers > 1 and len(parquet_paths) > 1:
        with ThreadPoolExecutor(max_workers=panel_load_workers) as executor:
            loaded_arrays = list(executor.map(_load_one_arrays, parquet_paths))
    else:
        loaded_arrays = [_load_one_arrays(path) for path in parquet_paths]

    valid_arrays: list[_SymbolPanelArrays] = []
    for path, arrays, exc in loaded_arrays:
        if exc is not None:
            if strict_no_fallback or isinstance(exc, _MissingTradingVolumeError):
                raise type(exc)(f"{path.name}: {exc}") from exc
            print(f"[panel] SKIP {path.name}: {exc}")
            continue
        if arrays is not None:
            valid_arrays.append(arrays)
    panel = _build_panel_from_symbol_arrays(valid_arrays, benchmark_name=benchmark_name)
    _save_panel_cache(parquet_root, panel, source_hash, backend_key)
    print(f"[panel] cache v2 saved: {panel_cache_v2_dir(parquet_root)}")
    _print_feature_overview(panel)
    return panel
