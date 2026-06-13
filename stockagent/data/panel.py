from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
import hashlib
import pickle
import os

import numpy as np
import pandas as pd

try:
    import cudf
except Exception:  # pragma: no cover - optional GPU dependency
    cudf = None

try:
    import polars as pl
except Exception:  # pragma: no cover - optional parquet reader
    pl = None

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional parquet reader
    pq = None

try:
    import duckdb
except Exception:  # pragma: no cover - optional panel backend
    duckdb = None

from stockagent.data.panel_cache import (
    legacy_panel_cache_path,
    legacy_panel_meta_path,
    load_panel_cache_v2,
    panel_cache_v2_is_valid,
    panel_cache_v2_dir,
    save_panel_cache_v2,
)


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
PANEL_CACHE_VERSION = 18
FEATURE_FILE_SUFFIX = "_features.parquet"
EPSILON = 1e-8
PREV_DAY_LOG_RETURN_RENAME = {
    "open": "open_logret_1d",
    "max": "max_logret_1d",
    "min": "min_logret_1d",
    "close": "close_logret_1d",
    "Trading_Volume": "trading_volume_logret_1d",
}
_MISSING_VOLUME_WARNED_SYMBOLS: set[str] = set()


def _round_half_up(values: np.ndarray | pd.Series, decimals: int = 2) -> np.ndarray:
    """Round with half-up semantics (0.5 always rounds away from zero)."""
    arr = np.asarray(values, dtype=np.float64)
    factor = float(10**decimals)
    out = np.full(arr.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(arr)
    pos = valid & (arr >= 0.0)
    neg = valid & (arr < 0.0)
    out[pos] = np.floor(arr[pos] * factor + 0.5) / factor
    out[neg] = np.ceil(arr[neg] * factor - 0.5) / factor
    return out


def _round_tw_price_series(series: pd.Series) -> pd.Series:
    """Normalize TW stock prices to 2 decimals using half-up rounding."""
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.Series(_round_half_up(numeric, decimals=2), index=series.index)


def _price_decimals_for_path(path: Path) -> int:
    """Return market-specific price precision: TW=2, others=8 decimals."""
    parts = {part.lower() for part in path.parts}
    symbol = _symbol_name_from_path(path)
    is_tw_market = "tw_stocks" in parts or symbol.isdigit()
    return 2 if is_tw_market else 8


def _return_price_column(frame: pd.DataFrame, path: Path) -> str:
    """Choose the price series used for forward return labels."""
    # Use adjusted close whenever available so corporate actions
    # (splits/dividends/capital changes) do not create fake label jumps.
    if "adjclose" in frame.columns:
        return "adjclose"
    return "close"


def _round_price_series(series: pd.Series, decimals: int) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return pd.Series(_round_half_up(numeric, decimals=decimals), index=series.index)


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


def _safe_log_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Compute log(numerator / denominator) only where both values are finite and > 0."""
    num = pd.to_numeric(numerator, errors="coerce").to_numpy(dtype=np.float64, copy=False)
    den = pd.to_numeric(denominator, errors="coerce").to_numpy(dtype=np.float64, copy=False)
    out = np.full(num.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(num) & np.isfinite(den) & (num > 0.0) & (den > 0.0)
    np.divide(num, den, out=out, where=valid)
    np.log(out, out=out, where=valid)
    out[~valid] = np.nan
    return pd.Series(out, index=numerator.index)


def _compute_tradable_from_frame(frame: pd.DataFrame) -> pd.Series:
    close_is_valid = frame["close"].notna()
    if "Trading_Volume" not in frame.columns:
        return close_is_valid

    volume = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
    has_positive_volume = volume.fillna(0).gt(0)
    volume_missing = volume.isna()
    return close_is_valid & (has_positive_volume | volume_missing)


def _tw_tick_size(price: np.ndarray) -> np.ndarray:
    """TWSE tick size by price bucket (vectorized)."""
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


def _tw_limit_price(prev_close: pd.Series, ratio: float) -> pd.Series:
    """Compute TW daily limit price with floor-to-tick rule from theoretical price."""
    prev = pd.to_numeric(prev_close, errors="coerce").to_numpy(dtype=np.float64, copy=False)
    theoretical = prev * ratio
    tick = _tw_tick_size(theoretical)

    out = np.full(theoretical.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(theoretical) & np.isfinite(tick) & (tick > 0.0)
    # Small epsilon avoids floating-point edge cases around exact tick boundaries.
    out[valid] = np.floor((theoretical[valid] / tick[valid]) + 1e-12) * tick[valid]
    return pd.Series(_round_half_up(out, decimals=2), index=prev_close.index)


def _tw_reference_price_for_limits(frame: pd.DataFrame, prev_close_raw: pd.Series) -> pd.Series:
    """Compute TW daily reference price used for limit-up/down checks.

    Base rule uses previous close, then applies ex-right/ex-dividend adjustments
    when source columns are available:
    - Dividends: subtract cash dividend on ex-dividend day.
    - Stock Splits: divide by split ratio on ex-right day.
    """
    reference = pd.to_numeric(prev_close_raw, errors="coerce").astype(np.float64)

    if "Dividends" in frame.columns:
        dividends = pd.to_numeric(frame["Dividends"], errors="coerce").fillna(0.0)
        reference = reference - dividends

    if "Stock Splits" in frame.columns:
        split_ratio = pd.to_numeric(frame["Stock Splits"], errors="coerce")
        valid_split = split_ratio.notna() & split_ratio.gt(0.0) & split_ratio.ne(1.0)
        reference = reference.where(~valid_split, reference / split_ratio)

    reference = reference.where(reference.gt(0.0))
    return pd.Series(_round_half_up(reference, decimals=2), index=prev_close_raw.index)


def _compute_tw_limit_masks(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Return (can_buy, can_sell) masks under TW 10% daily limit assumptions.

    Rule:
    - limit-up day: cannot buy, can sell
    - limit-down day: can buy, cannot sell
    """
    tradable = frame["tradable"].astype(bool)
    close_raw = _round_tw_price_series(frame.get("close_raw"))
    prev_close_raw = close_raw.shift(1)
    reference_price = _tw_reference_price_for_limits(frame, prev_close_raw)

    limit_up_price = _tw_limit_price(reference_price, 1.10)
    limit_down_price = _tw_limit_price(reference_price, 0.90)

    # Use small price tolerance to absorb source rounding noise.
    is_limit_up = close_raw.ge(limit_up_price - 1e-9) & reference_price.gt(0)
    is_limit_down = close_raw.le(limit_down_price + 1e-9) & reference_price.gt(0)

    can_buy = tradable & ~is_limit_up.fillna(False)
    can_sell = tradable & ~is_limit_down.fillna(False)
    return can_buy, can_sell


def _add_derived_features(frame: pd.DataFrame) -> pd.DataFrame:
    open_px = pd.to_numeric(frame.get("open"), errors="coerce")
    high_px = pd.to_numeric(frame.get("max"), errors="coerce")
    low_px = pd.to_numeric(frame.get("min"), errors="coerce")
    close_px = pd.to_numeric(frame.get("close"), errors="coerce")

    frame["intraday_return_co"] = _safe_log_ratio(close_px, open_px)
    frame["overnight_gap_oc"] = _safe_log_ratio(open_px, close_px.shift(1))
    frame["intraday_range"] = _safe_log_ratio(high_px, low_px)

    spread = (high_px - low_px).clip(lower=0.0)
    denom = spread + EPSILON

    frame["body_ratio"] = (close_px - open_px).abs() / denom
    frame["signed_body_ratio"] = (close_px - open_px) / denom
    frame["clv"] = (close_px - low_px) / denom
    frame["clv_centered"] = frame["clv"] - 0.5
    frame["upper_shadow"] = (high_px - np.maximum(open_px, close_px)) / denom
    frame["lower_shadow"] = (np.minimum(open_px, close_px) - low_px) / denom
    frame["shadow_imbalance"] = frame["upper_shadow"] - frame["lower_shadow"]

    frame["delta_clv"] = frame["clv"] - frame["clv"].shift(1)
    frame["delta_body_ratio"] = frame["body_ratio"] - frame["body_ratio"].shift(1)

    return frame


def _apply_prev_day_log_returns(frame: pd.DataFrame, columns: list[str]) -> None:
    """Convert selected columns to log returns vs previous day in-place."""
    for col in columns:
        if col in frame.columns:
            out_col = PREV_DAY_LOG_RETURN_RENAME.get(col, f"{col}_logret_1d")
            frame[out_col] = _safe_log_ratio(frame[col], frame[col].shift(1))


def _warn_missing_trading_volume(path: Path) -> None:
    symbol = _symbol_name_from_path(path)
    if symbol in _MISSING_VOLUME_WARNED_SYMBOLS:
        return
    _MISSING_VOLUME_WARNED_SYMBOLS.add(symbol)
    print(
        f"[panel] WARN {path.name}: missing Trading_Volume column; "
        "volume features (trading_volume_logret_1d, signed_vol) will be NaN"
    )


def _ensure_feature_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    """Materialize all requested feature columns; fill missing ones with NaN."""
    for col in columns:
        if col not in frame.columns:
            frame[col] = np.nan


def _prepare_symbol_frame(frame: pd.DataFrame, path: Path) -> pd.DataFrame:
    frame = frame.copy()
    if not pd.api.types.is_datetime64_any_dtype(frame["date"]):
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["symbol"] = _symbol_name_from_path(path)
    price_decimals = _price_decimals_for_path(path)
    for col in ["open", "max", "min", "close"]:
        if col in frame.columns:
            frame[col] = _round_price_series(frame[col], decimals=price_decimals)
    if "adjclose" in frame.columns:
        frame["adjclose"] = _round_price_series(frame["adjclose"], decimals=price_decimals)
    frame["close_raw"] = pd.to_numeric(frame["close"], errors="coerce").astype(np.float32)

    frame = _add_derived_features(frame)

    return_price_col = _return_price_column(frame, path)
    frame["return_1d"] = _safe_log_ratio(frame[return_price_col].shift(-1), frame[return_price_col])

    frame["tradable"] = _compute_tradable_from_frame(frame)

    _apply_prev_day_log_returns(frame, ["open", "max", "min", "close"])

    if "Trading_Volume" in frame.columns:
        vol = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
        frame[PREV_DAY_LOG_RETURN_RENAME["Trading_Volume"]] = _safe_log_ratio(vol, vol.shift(1))

        volume_log_delta = pd.to_numeric(
            frame[PREV_DAY_LOG_RETURN_RENAME["Trading_Volume"]], errors="coerce"
        )
        signed_intraday = np.sign(pd.to_numeric(frame["intraday_return_co"], errors="coerce"))
        frame["signed_vol"] = signed_intraday * volume_log_delta
    else:
        _warn_missing_trading_volume(path)

    _ensure_feature_columns(frame, LOG_RETURN_FEATURE_COLUMNS)
    return frame


def _load_symbol_frame(path: Path) -> pd.DataFrame:
    return _prepare_symbol_frame(pd.read_parquet(path), path)


def _load_symbol_frame_polars(path: Path) -> pd.DataFrame:
    if pl is None:
        raise RuntimeError("Polars is not available")
    return _prepare_symbol_frame(pl.read_parquet(path).to_pandas(), path)


def _load_symbol_frame_cudf(path: Path) -> pd.DataFrame:
    if cudf is None:
        raise RuntimeError("cuDF is not available")

    gdf = cudf.read_parquet(path)
    gdf["date"] = cudf.to_datetime(gdf["date"])
    gdf = gdf.sort_values("date").reset_index(drop=True)
    gdf["symbol"] = _symbol_name_from_path(path)
    price_decimals = _price_decimals_for_path(path)
    for col in ["open", "max", "min", "close"]:
        if col in gdf.columns:
            gdf[col] = gdf[col].round(price_decimals)
    if "adjclose" in gdf.columns:
        gdf["adjclose"] = gdf["adjclose"].round(price_decimals)
    gdf["close_raw"] = gdf["close"].astype("float32")

    frame = gdf.to_pandas()
    frame = _add_derived_features(frame)

    return_price_col = _return_price_column(frame, path)
    frame["return_1d"] = _safe_log_ratio(frame[return_price_col].shift(-1), frame[return_price_col])
    frame["tradable"] = _compute_tradable_from_frame(frame)

    _apply_prev_day_log_returns(frame, ["open", "max", "min", "close"])

    if "Trading_Volume" in frame.columns:
        vol = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
        frame[PREV_DAY_LOG_RETURN_RENAME["Trading_Volume"]] = _safe_log_ratio(vol, vol.shift(1))

    if "Trading_Volume" in frame.columns:
        volume_log_delta = pd.to_numeric(
            frame[PREV_DAY_LOG_RETURN_RENAME["Trading_Volume"]], errors="coerce"
        )
        signed_intraday = np.sign(pd.to_numeric(frame["intraday_return_co"], errors="coerce"))
        frame["signed_vol"] = signed_intraday * volume_log_delta
    else:
        _warn_missing_trading_volume(path)

    _ensure_feature_columns(frame, LOG_RETURN_FEATURE_COLUMNS)
    return frame


def _coerce_arrow_numeric_column(table, name: str, rows: int) -> np.ndarray:
    if name not in table.column_names:
        return np.full(rows, np.nan, dtype=np.float64)
    values = table[name].combine_chunks().to_numpy(zero_copy_only=False)
    try:
        return np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError):
        return pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(dtype=np.float64)


def _coerce_arrow_datetime_ns_column(table, name: str, rows: int) -> np.ndarray:
    if name not in table.column_names:
        return np.full(rows, np.datetime64("NaT", "ns"), dtype="datetime64[ns]")
    values = table[name].combine_chunks().to_numpy(zero_copy_only=False)
    try:
        return np.asarray(values, dtype="datetime64[ns]")
    except (TypeError, ValueError):
        return pd.to_datetime(pd.Series(values), errors="coerce").to_numpy(dtype="datetime64[ns]")


def _shift_array(values: np.ndarray, periods: int) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float64)
    out = np.full(arr.shape, np.nan, dtype=np.float64)
    if periods > 0:
        out[periods:] = arr[:-periods]
    elif periods < 0:
        out[:periods] = arr[-periods:]
    else:
        out[:] = arr
    return out


def _load_symbol_arrays_pyarrow(path: Path, tradable_mode: str = "tradable") -> _SymbolPanelArrays:
    if pq is None:
        raise RuntimeError("PyArrow is not available")
    if tradable_mode != "tradable":
        raise RuntimeError("PyArrow panel backend currently supports tradable_mode='tradable' only")

    table = pq.read_table(path)
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
    intraday_return_co = _safe_log_ratio(pd.Series(close_px), pd.Series(open_px)).to_numpy(dtype=np.float64)
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
    return_1d = _safe_log_ratio(
        pd.Series(_shift_array(return_price, -1)),
        pd.Series(return_price),
    ).to_numpy(dtype=np.float64)
    open_logret_1d = _safe_log_ratio(pd.Series(open_px), pd.Series(_shift_array(open_px, 1))).to_numpy(dtype=np.float64)
    max_logret_1d = _safe_log_ratio(pd.Series(high_px), pd.Series(_shift_array(high_px, 1))).to_numpy(dtype=np.float64)
    min_logret_1d = _safe_log_ratio(pd.Series(low_px), pd.Series(_shift_array(low_px, 1))).to_numpy(dtype=np.float64)
    close_logret_1d = _safe_log_ratio(pd.Series(close_px), pd.Series(_shift_array(close_px, 1))).to_numpy(dtype=np.float64)
    trading_volume_logret_1d = _safe_log_ratio(
        pd.Series(volume),
        pd.Series(_shift_array(volume, 1)),
    ).to_numpy(dtype=np.float64)
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
    return _SymbolPanelArrays(
        symbol=_symbol_name_from_path(path),
        dates=dates,
        features=features,
        returns_1d=return_1d.astype(np.float32, copy=False),
        close_prices=close_px.astype(np.float32, copy=False),
        tradable_mask=tradable,
        can_buy_mask=tradable.copy(),
        can_sell_mask=tradable.copy(),
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


def _duckdb_quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _duckdb_column_or_null(schema_names: set[str], name: str) -> str:
    if name in schema_names:
        return f"try_cast({_duckdb_quote(name)} AS DOUBLE)"
    return "NULL::DOUBLE"


def _duckdb_safe_log(num: str, den: str) -> str:
    return f"CASE WHEN {num} > 0.0 AND {den} > 0.0 THEN ln({num} / {den}) ELSE NULL END"


def _duckdb_union_schema_names(paths: list[Path]) -> set[str]:
    if pq is None:
        raise RuntimeError("DuckDB panel backend requires pyarrow for parquet schema inspection")
    names: set[str] = set()
    for path in paths:
        names.update(pq.read_schema(path).names)
    return names


def _duckdb_arrow_column(table, name: str, dtype) -> np.ndarray:
    values = table[name].combine_chunks().to_numpy(zero_copy_only=False)
    return np.asarray(values, dtype=dtype)


def _build_panel_from_duckdb(
    parquet_root: Path,
    parquet_paths: list[Path],
    *,
    benchmark_name: str,
    panel_load_workers: int,
) -> PanelData:
    if duckdb is None:
        raise RuntimeError("data.panel_backend='duckdb' requires the duckdb package")
    if pq is None:
        raise RuntimeError("data.panel_backend='duckdb' requires the pyarrow package")

    symbols = [_symbol_name_from_path(path) for path in parquet_paths]
    symbol_map = pd.DataFrame(
        {
            "symbol": symbols,
            "symbol_idx": np.arange(len(symbols), dtype=np.int32),
        }
    )
    schema_names = _duckdb_union_schema_names(parquet_paths)
    decimals_by_path = {_price_decimals_for_path(path) for path in parquet_paths}
    if len(decimals_by_path) != 1:
        raise RuntimeError("DuckDB panel backend currently requires one market price precision")
    decimals = int(next(iter(decimals_by_path)))
    return_source = "adjclose_px" if "adjclose" in schema_names else "close_px"
    volume_expr = "volume_px"
    tradable_expr = (
        "close_px IS NOT NULL AND (volume_px > 0.0 OR volume_px IS NULL)"
        if "Trading_Volume" in schema_names
        else "close_px IS NOT NULL"
    )
    pattern = (parquet_root / f"*{FEATURE_FILE_SUFFIX}").as_posix()

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={max(1, int(panel_load_workers))}")
        con.register("symbol_map_df", symbol_map)
        con.execute("CREATE TEMP TABLE symbol_map AS SELECT * FROM symbol_map_df")
        con.execute(
            f"""
            CREATE TEMP TABLE computed AS
            WITH raw AS (
                SELECT
                    m.symbol_idx,
                    m.symbol,
                    CAST(date AS TIMESTAMP) AS date_key,
                    round({_duckdb_column_or_null(schema_names, "open")}, {decimals}) AS open_px,
                    round({_duckdb_column_or_null(schema_names, "max")}, {decimals}) AS max_px,
                    round({_duckdb_column_or_null(schema_names, "min")}, {decimals}) AS min_px,
                    round({_duckdb_column_or_null(schema_names, "close")}, {decimals}) AS close_px,
                    round({_duckdb_column_or_null(schema_names, "adjclose")}, {decimals}) AS adjclose_px,
                    {_duckdb_column_or_null(schema_names, "Trading_Volume")} AS volume_px
                FROM read_parquet(?, filename=true, union_by_name=true) AS r
                JOIN symbol_map AS m
                  ON m.symbol = regexp_extract(r.filename, '([^/\\\\]+)_features\\.parquet$', 1)
            ),
            calc1 AS (
                SELECT
                    *,
                    CASE
                        WHEN max_px IS NULL OR min_px IS NULL THEN NULL
                        ELSE greatest(max_px - min_px, 0.0) + {EPSILON}
                    END AS denom,
                    {_duckdb_safe_log("close_px", "open_px")} AS intraday_return_co
                FROM raw
                WHERE date_key IS NOT NULL
            ),
            calc2 AS (
                SELECT
                    *,
                    abs(close_px - open_px) / denom AS body_ratio,
                    (close_px - open_px) / denom AS signed_body_ratio,
                    (close_px - min_px) / denom AS clv,
                    (max_px - greatest(open_px, close_px)) / denom AS upper_shadow,
                    (least(open_px, close_px) - min_px) / denom AS lower_shadow,
                    {_duckdb_safe_log(f"lead({return_source}) OVER (PARTITION BY symbol_idx ORDER BY date_key)", return_source)} AS return_1d,
                    {_duckdb_safe_log("open_px", "lag(open_px) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS open_logret_1d,
                    {_duckdb_safe_log("max_px", "lag(max_px) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS max_logret_1d,
                    {_duckdb_safe_log("min_px", "lag(min_px) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS min_logret_1d,
                    {_duckdb_safe_log("close_px", "lag(close_px) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS close_logret_1d,
                    {_duckdb_safe_log(volume_expr, f"lag({volume_expr}) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS trading_volume_logret_1d,
                    {tradable_expr} AS tradable,
                    close_px IS NOT NULL AS alive
                FROM calc1
            ),
            calc3 AS (
                SELECT
                    symbol_idx,
                    symbol,
                    date_key,
                    close_px,
                    return_1d,
                    tradable,
                    alive,
                    open_logret_1d,
                    max_logret_1d,
                    min_logret_1d,
                    close_logret_1d,
                    trading_volume_logret_1d,
                    sign(intraday_return_co) * trading_volume_logret_1d AS signed_vol,
                    body_ratio,
                    signed_body_ratio,
                    body_ratio - lag(body_ratio) OVER (PARTITION BY symbol_idx ORDER BY date_key) AS delta_body_ratio,
                    clv,
                    clv - 0.5 AS clv_centered,
                    clv - lag(clv) OVER (PARTITION BY symbol_idx ORDER BY date_key) AS delta_clv,
                    upper_shadow,
                    lower_shadow,
                    upper_shadow - lower_shadow AS shadow_imbalance
                FROM calc2
            )
            SELECT * FROM calc3
            """,
            [pattern],
        )
        con.execute(
            """
            CREATE TEMP TABLE date_map AS
            SELECT date_key, row_number() OVER (ORDER BY date_key) - 1 AS date_idx
            FROM (SELECT DISTINCT date_key FROM computed)
            """
        )
        dates_table = con.execute("SELECT date_key FROM date_map ORDER BY date_idx").to_arrow_table()
        dates = _duckdb_arrow_column(dates_table, "date_key", "datetime64[ns]")
        num_dates = int(dates.size)
        num_symbols = len(symbols)
        num_features = len(LOG_RETURN_FEATURE_COLUMNS)
        features = np.full((num_dates, num_symbols, num_features), np.nan, dtype=np.float32)
        returns_1d = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
        close_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
        tradable_mask = np.zeros((num_dates, num_symbols), dtype=bool)
        alive_mask = np.zeros((num_dates, num_symbols), dtype=bool)

        select_cols = ", ".join(
            [
                "dm.date_idx",
                "c.symbol_idx",
                "c.return_1d",
                "c.close_px",
                "c.tradable",
                "c.alive",
                *[f"c.{name}" for name in LOG_RETURN_FEATURE_COLUMNS],
            ]
        )
        final_table = con.execute(
            f"""
            SELECT {select_cols}
            FROM computed AS c
            JOIN date_map AS dm USING (date_key)
            ORDER BY c.symbol_idx, dm.date_idx
            """
        ).to_arrow_table()
    finally:
        con.close()

    row_idx = _duckdb_arrow_column(final_table, "date_idx", np.int64)
    sym_idx = _duckdb_arrow_column(final_table, "symbol_idx", np.int64)
    returns_1d[row_idx, sym_idx] = _duckdb_arrow_column(final_table, "return_1d", np.float32)
    close_prices[row_idx, sym_idx] = _duckdb_arrow_column(final_table, "close_px", np.float32)
    tradable_mask[row_idx, sym_idx] = _duckdb_arrow_column(final_table, "tradable", bool)
    alive_mask[row_idx, sym_idx] = _duckdb_arrow_column(final_table, "alive", bool)
    for feat_idx, name in enumerate(LOG_RETURN_FEATURE_COLUMNS):
        features[row_idx, sym_idx, feat_idx] = _duckdb_arrow_column(final_table, name, np.float32)
    features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)

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

    return PanelData(
        dates=np.asarray(dates, dtype="datetime64[ns]"),
        symbols=symbols,
        feature_names=list(LOG_RETURN_FEATURE_COLUMNS),
        features=features,
        returns_1d=returns_1d,
        tradable_mask=tradable_mask,
        can_buy_mask=tradable_mask.copy(),
        can_sell_mask=tradable_mask.copy(),
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


def _build_panel_from_frame(
    frame_all: pd.DataFrame,
    symbols: list[str],
    benchmark_name: str = "universe_average_return",
) -> PanelData:
    feature_columns = _get_feature_columns(frame_all)

    all_dates = sorted(frame_all["date"].dropna().unique().tolist())
    num_dates = len(all_dates)
    num_symbols = len(symbols)
    num_features = len(feature_columns)

    features = np.full((num_dates, num_symbols, num_features), np.nan, dtype=np.float32)
    returns_1d = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    close_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    tradable_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    can_buy_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    can_sell_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    alive_mask = np.zeros((num_dates, num_symbols), dtype=bool)

    date_index = {date: idx for idx, date in enumerate(all_dates)}
    symbol_index = {symbol: idx for idx, symbol in enumerate(symbols)}

    frame_all = frame_all[frame_all["symbol"].isin(symbols)].copy()
    row_idx = frame_all["date"].map(date_index).to_numpy(dtype=np.int64)
    sym_idx = frame_all["symbol"].map(symbol_index).to_numpy(dtype=np.int64)

    for feat_idx, col in enumerate(feature_columns):
        features[row_idx, sym_idx, feat_idx] = frame_all[col].to_numpy(dtype=np.float32, copy=False)

    returns_1d[row_idx, sym_idx] = frame_all["return_1d"].to_numpy(dtype=np.float32, copy=False)
    close_prices[row_idx, sym_idx] = frame_all["close_raw"].to_numpy(dtype=np.float32, copy=False)
    tradable_mask[row_idx, sym_idx] = frame_all["tradable"].to_numpy(dtype=bool, copy=False)
    if "can_buy" in frame_all.columns:
        can_buy_mask[row_idx, sym_idx] = frame_all["can_buy"].to_numpy(dtype=bool, copy=False)
    else:
        can_buy_mask[row_idx, sym_idx] = tradable_mask[row_idx, sym_idx]
    if "can_sell" in frame_all.columns:
        can_sell_mask[row_idx, sym_idx] = frame_all["can_sell"].to_numpy(dtype=bool, copy=False)
    else:
        can_sell_mask[row_idx, sym_idx] = tradable_mask[row_idx, sym_idx]
    alive_mask[row_idx, sym_idx] = frame_all["close"].notna().to_numpy(dtype=bool, copy=False)

    benchmark_symbol_index = _resolve_benchmark_index(symbols, benchmark_name)
    if benchmark_symbol_index is None:
        valid_returns = np.isfinite(returns_1d)
        n_valid = valid_returns.sum(axis=1)
        sum_ret = np.nansum(np.where(valid_returns, returns_1d, 0.0), axis=1)
        benchmark_returns = np.zeros_like(sum_ret, dtype=np.float32)
        np.divide(
            sum_ret,
            n_valid,
            out=benchmark_returns,
            where=n_valid > 0,
        )
    else:
        benchmark_returns = np.nan_to_num(
            returns_1d[:, benchmark_symbol_index],
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        ).astype(np.float32, copy=False)

    features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)

    return PanelData(
        dates=np.array(all_dates, dtype="datetime64[ns]"),
        symbols=symbols,
        feature_names=feature_columns,
        features=features,
        returns_1d=returns_1d,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        alive_mask=alive_mask,
        benchmark_returns=benchmark_returns,
        close_prices=close_prices,
    )


def _get_feature_columns(frame: pd.DataFrame) -> list[str]:
    numeric_columns = set(frame.select_dtypes(include=[np.number]).columns.tolist())
    feature_columns = [
        column
        for column in LOG_RETURN_FEATURE_COLUMNS
        if column in numeric_columns and column not in RESERVED_COLUMNS
    ]
    if not feature_columns:
        raise ValueError("No log-return feature columns found in symbol frame")
    return feature_columns


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
    valid_backends = {"auto", "pandas", "polars", "pyarrow", "duckdb"}
    if panel_backend not in valid_backends:
        raise ValueError(f"panel_backend must be one of {sorted(valid_backends)}, got {panel_backend!r}")
    panel_load_workers = max(0, int(panel_load_workers))
    use_cudf = bool(use_rapids) and cudf is not None and panel_backend == "auto"
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

    if use_cudf:
        selected_backend = "cudf"
    elif panel_backend == "duckdb":
        if duckdb is None:
            raise RuntimeError("data.panel_backend='duckdb' requires the duckdb package")
        selected_backend = "duckdb"
    elif panel_backend == "pyarrow":
        if pq is None:
            raise RuntimeError("data.panel_backend='pyarrow' requires the pyarrow package")
        selected_backend = "pyarrow"
    elif panel_backend == "polars":
        if pl is None:
            raise RuntimeError("data.panel_backend='polars' requires the polars package")
        selected_backend = "polars"
    elif panel_backend == "auto" and duckdb is not None and pq is not None and tradable_mode == "tradable":
        selected_backend = "duckdb"
    elif panel_backend == "auto" and pq is not None and tradable_mode == "tradable":
        selected_backend = "pyarrow"
    elif panel_backend == "auto" and pl is not None:
        selected_backend = "polars"
    else:
        selected_backend = "pandas"
    if selected_backend in {"duckdb", "pyarrow"} and tradable_mode != "tradable":
        if panel_backend in {"duckdb", "pyarrow"}:
            raise RuntimeError(
                f"data.panel_backend={panel_backend!r} currently requires data.tradable_mode='tradable'"
            )
        selected_backend = "polars" if pl is not None else "pandas"

    backend_key = (
        f"{selected_backend}|benchmark={benchmark_name}|"
        f"usd_only={usd_only_trading_pairs}|tradable_mode={tradable_mode}"
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
    if selected_backend == "duckdb":
        panel = _build_panel_from_duckdb(
            parquet_root,
            parquet_paths,
            benchmark_name=benchmark_name,
            panel_load_workers=panel_load_workers,
        )
        _save_panel_cache(parquet_root, panel, source_hash, backend_key)
        print(f"[panel] cache v2 saved: {panel_cache_v2_dir(parquet_root)}")
        _print_feature_overview(panel)
        return panel

    if selected_backend == "pyarrow":
        def _load_one_pyarrow(path: Path) -> tuple[Path, _SymbolPanelArrays | None, Exception | None]:
            try:
                arrays = _load_symbol_arrays_pyarrow(path, tradable_mode=tradable_mode)
                if int(arrays.dates.size) == 0:
                    raise ValueError(f"Symbol file is empty: {path.name}")
                return path, arrays, None
            except Exception as exc:
                return path, None, exc

        if panel_load_workers > 1 and len(parquet_paths) > 1:
            with ThreadPoolExecutor(max_workers=panel_load_workers) as executor:
                loaded_pyarrow = list(executor.map(_load_one_pyarrow, parquet_paths))
        else:
            loaded_pyarrow = [_load_one_pyarrow(path) for path in parquet_paths]

        valid_arrays: list[_SymbolPanelArrays] = []
        for path, arrays, exc in loaded_pyarrow:
            if exc is not None:
                print(f"[panel] SKIP {path.name}: {exc}")
                continue
            if arrays is not None:
                valid_arrays.append(arrays)
        panel = _build_panel_from_symbol_arrays(valid_arrays, benchmark_name=benchmark_name)
        _save_panel_cache(parquet_root, panel, source_hash, backend_key)
        print(f"[panel] cache v2 saved: {panel_cache_v2_dir(parquet_root)}")
        _print_feature_overview(panel)
        return panel

    if use_cudf:
        try:
            symbol_frames_cudf: list[pd.DataFrame] = []
            valid_paths_cudf: list[Path] = []
            for path in parquet_paths:
                frame = _load_symbol_frame_cudf(path)
                if len(frame) == 0:
                    continue
                use_tw_limit_guard = tradable_mode == "tw_limit_guard"
                if use_tw_limit_guard:
                    can_buy_limit, can_sell_limit = _compute_tw_limit_masks(frame)
                    frame["can_buy"] = can_buy_limit
                    frame["can_sell"] = can_sell_limit
                else:
                    frame["can_buy"] = frame["tradable"].astype(bool)
                    frame["can_sell"] = frame["tradable"].astype(bool)
                symbol_frames_cudf.append(frame)
                valid_paths_cudf.append(path)

            if symbol_frames_cudf:
                symbols_cudf = [_symbol_name_from_path(path) for path in valid_paths_cudf]
                frame_all_cudf = pd.concat(symbol_frames_cudf, ignore_index=True)
                panel = _build_panel_from_frame(
                    frame_all_cudf,
                    symbols_cudf,
                    benchmark_name=benchmark_name,
                )
                _save_panel_cache(parquet_root, panel, source_hash, backend_key)
                print(f"[panel] cache v2 saved: {panel_cache_v2_dir(parquet_root)} (cuDF path)")
                _print_feature_overview(panel)
                return panel
        except Exception as exc:
            print(f"[panel] cuDF path failed, fallback to pandas: {exc}")
            selected_backend = "pandas"
            backend_key = (
                f"pandas|benchmark={benchmark_name}|"
                f"usd_only={usd_only_trading_pairs}|tradable_mode={tradable_mode}"
            )
            panel = _load_valid_panel_cache(parquet_root, parquet_paths, backend_key, source_hash)
            if panel is not None:
                _print_feature_overview(panel)
                return panel
    
    symbol_frames: list[pd.DataFrame] = []
    valid_paths: list[Path] = []
    load_frame = _load_symbol_frame_polars if selected_backend == "polars" else _load_symbol_frame

    def _load_one(path: Path) -> tuple[Path, pd.DataFrame | None, Exception | None]:
        try:
            frame = load_frame(path)
            if len(frame) == 0:
                raise ValueError(f"Symbol file is empty: {path.name}")
            use_tw_limit_guard = tradable_mode == "tw_limit_guard"
            if use_tw_limit_guard:
                can_buy_limit, can_sell_limit = _compute_tw_limit_masks(frame)
                frame["can_buy"] = can_buy_limit
                frame["can_sell"] = can_sell_limit
            else:
                frame["can_buy"] = frame["tradable"].astype(bool)
                frame["can_sell"] = frame["tradable"].astype(bool)
            return path, frame, None
        except Exception as exc:
            return path, None, exc

    if panel_load_workers > 1 and len(parquet_paths) > 1:
        with ThreadPoolExecutor(max_workers=panel_load_workers) as executor:
            loaded = list(executor.map(_load_one, parquet_paths))
    else:
        loaded = [_load_one(path) for path in parquet_paths]

    for path, frame, exc in loaded:
        if exc is not None:
            print(f"[panel] SKIP {path.name}: {exc}")
            continue
        if frame is None:
            continue
        symbol_frames.append(frame)
        valid_paths.append(path)

    if not symbol_frames:
        raise RuntimeError("No valid parquet files could be loaded.")

    symbols = [_symbol_name_from_path(path) for path in valid_paths]
    frame_all = pd.concat(symbol_frames, ignore_index=True)
    panel = _build_panel_from_frame(frame_all, symbols, benchmark_name=benchmark_name)
    
    _save_panel_cache(parquet_root, panel, source_hash, backend_key)
    print(f"[panel] cache v2 saved: {panel_cache_v2_dir(parquet_root)}")
    _print_feature_overview(panel)
    return panel
