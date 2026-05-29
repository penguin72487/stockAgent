from __future__ import annotations

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


RESERVED_COLUMNS = {"date", "symbol", "return_1d", "tradable"}
LOG_RETURN_FEATURE_COLUMNS = [
    "open",
    "max",
    "min",
    "close",
    "Trading_Volume",
    "intraday_return_co",
    "overnight_gap_oc",
    "intraday_range",
    "body_ratio",
    "clv",
    "upper_shadow",
    "lower_shadow",
    "delta_intraday_return_co",
    "delta_intraday_range",
    "delta_clv",
    "delta_body_ratio",
    "gap_cont",
    "signed_vol",
    "effort",
    "vol_impact",
    "gap_vol",
]
PANEL_CACHE_VERSION = 13
FEATURE_FILE_SUFFIX = "_features.parquet"
EPSILON = 1e-8


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
    frame["clv"] = (close_px - low_px) / denom
    frame["upper_shadow"] = (high_px - np.maximum(open_px, close_px)) / denom
    frame["lower_shadow"] = (np.minimum(open_px, close_px) - low_px) / denom

    frame["delta_intraday_return_co"] = frame["intraday_return_co"] - frame["intraday_return_co"].shift(1)
    frame["delta_intraday_range"] = frame["intraday_range"] - frame["intraday_range"].shift(1)
    frame["delta_clv"] = frame["clv"] - frame["clv"].shift(1)
    frame["delta_body_ratio"] = frame["body_ratio"] - frame["body_ratio"].shift(1)
    frame["gap_cont"] = frame["overnight_gap_oc"] * frame["intraday_return_co"]

    return frame


def _load_symbol_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    if not pd.api.types.is_datetime64_any_dtype(frame["date"]):
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["symbol"] = _symbol_name_from_path(path)
    price_decimals = _price_decimals_for_path(path)
    for col in ["open", "max", "min", "close"]:
        if col in frame.columns:
            frame[col] = _round_price_series(frame[col], decimals=price_decimals)
    frame["close_raw"] = pd.to_numeric(frame["close"], errors="coerce").astype(np.float32)

    frame = _add_derived_features(frame)

    frame["return_1d"] = _safe_log_ratio(frame["close"].shift(-1), frame["close"])

    frame["tradable"] = _compute_tradable_from_frame(frame)

    for col in ["open", "max", "min", "close"]:
        if col in frame.columns:
            frame[col] = _safe_log_ratio(frame[col], frame[col].shift(1))

    if "Trading_Volume" in frame.columns:
        vol = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
        frame["Trading_Volume"] = _safe_log_ratio(vol, vol.shift(1))

        volume_log_delta = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
        signed_intraday = np.sign(pd.to_numeric(frame["intraday_return_co"], errors="coerce"))
        abs_volume_log_delta = volume_log_delta.abs()

        frame["signed_vol"] = signed_intraday * volume_log_delta
        frame["effort"] = frame["intraday_return_co"].abs() / (abs_volume_log_delta + EPSILON)
        frame["vol_impact"] = frame["intraday_range"] / (abs_volume_log_delta + EPSILON)
        frame["gap_vol"] = frame["overnight_gap_oc"] * volume_log_delta

    return frame


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
    gdf["close_raw"] = gdf["close"].astype("float32")

    nxt_close = gdf["close"].shift(-1)
    valid_ret = (nxt_close > 0) & (gdf["close"] > 0)
    ret_ratio = (nxt_close / gdf["close"]).where(valid_ret)
    gdf["return_1d"] = np.log(ret_ratio)

    if "Trading_Volume" in gdf.columns:
        vol = gdf["Trading_Volume"]
        gdf["tradable"] = gdf["close"].notnull() & ((vol.fillna(0) > 0) | vol.isnull())
    else:
        gdf["tradable"] = gdf["close"].notnull()

    for col in ["open", "max", "min", "close"]:
        if col in gdf.columns:
            prev = gdf[col].shift(1)
            valid = (gdf[col] > 0) & (prev > 0)
            ratio = (gdf[col] / prev).where(valid)
            gdf[col] = np.log(ratio)

    if "Trading_Volume" in gdf.columns:
        vol = gdf["Trading_Volume"].astype("float64")
        prev_vol = vol.shift(1)
        valid_vol = (vol > 0) & (prev_vol > 0)
        vol_ratio = (vol / prev_vol).where(valid_vol)
        gdf["Trading_Volume"] = np.log(vol_ratio)

    frame = gdf.to_pandas()
    frame = _add_derived_features(frame)

    if "Trading_Volume" in frame.columns:
        volume_log_delta = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
        signed_intraday = np.sign(pd.to_numeric(frame["intraday_return_co"], errors="coerce"))
        abs_volume_log_delta = volume_log_delta.abs()

        frame["signed_vol"] = signed_intraday * volume_log_delta
        frame["effort"] = frame["intraday_return_co"].abs() / (abs_volume_log_delta + EPSILON)
        frame["vol_impact"] = frame["intraday_range"] / (abs_volume_log_delta + EPSILON)
        frame["gap_vol"] = frame["overnight_gap_oc"] * volume_log_delta

    return frame


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
    return Path(parquet_root) / "panel_cache.npz"


def _cache_meta_path(parquet_root: str | Path) -> Path:
    return Path(parquet_root) / ".panel_meta.pkl"


def _compute_source_hash(paths: list[Path]) -> str:
    """Compute hash of all parquet files' mtime and size."""
    hasher = hashlib.md5()
    for path in sorted(paths):
        mtime = path.stat().st_mtime
        size = path.stat().st_size
        hasher.update(f"{path.name}:{mtime}:{size}".encode())
    return hasher.hexdigest()


def _save_panel_cache(
    cache_path: Path,
    meta_path: Path,
    panel: PanelData,
    source_hash: str,
    backend_key: str,
) -> None:
    np.savez_compressed(
        cache_path,
        dates=panel.dates,
        symbols=np.array(panel.symbols, dtype=object),
        feature_names=np.array(panel.feature_names, dtype=object),
        features=panel.features,
        returns_1d=panel.returns_1d,
        tradable_mask=panel.tradable_mask,
        can_buy_mask=panel.can_buy_mask if panel.can_buy_mask is not None else panel.tradable_mask,
        can_sell_mask=panel.can_sell_mask if panel.can_sell_mask is not None else panel.tradable_mask,
        alive_mask=panel.alive_mask,
        benchmark_returns=panel.benchmark_returns,
        close_prices=panel.close_prices,
    )
    
    meta = {
        'version': PANEL_CACHE_VERSION,
        'source_hash': source_hash,
        'backend_key': backend_key,
        'num_dates': panel.num_dates,
        'num_symbols': panel.num_symbols,
    }
    with meta_path.open('wb') as f:
        pickle.dump(meta, f)


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


def build_panel(
    parquet_root: str | Path,
    use_rapids: bool = True,
    benchmark_name: str = "universe_average_return",
    usd_only_trading_pairs: bool = False,
    tradable_mode: str = "tradable",
    buy_tradable_mode: str | None = None,
    sell_tradable_mode: str | None = None,
) -> PanelData:
    parquet_root = Path(parquet_root)
    parquet_paths = sorted(parquet_root.glob(f"*{FEATURE_FILE_SUFFIX}"))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files found under {parquet_root}")

    if usd_only_trading_pairs:
        parquet_paths = [path for path in parquet_paths if _is_usd_trading_pair(path)]
        if not parquet_paths:
            raise FileNotFoundError(f"No USD trading pairs found under {parquet_root}")

    cache_path = _panel_cache_path(parquet_root)
    meta_path = _cache_meta_path(parquet_root)

    use_cudf = bool(use_rapids) and cudf is not None
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

    backend_key = (
        f"{'cudf' if use_cudf else 'pandas'}|benchmark={benchmark_name}|"
        f"usd_only={usd_only_trading_pairs}|tradable_mode={tradable_mode}"
    )
    
    # Check cache validity
    if _check_cache_valid(cache_path, meta_path, parquet_paths, backend_key):
        print(f"[panel] loading cache (valid): {cache_path}")
        panel = _load_panel_cache(cache_path)
        _print_feature_overview(panel)
        return panel

    print(f"[panel] building from {len(parquet_paths)} parquet files...")
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
                source_hash = _compute_source_hash(parquet_paths)
                _save_panel_cache(cache_path, meta_path, panel, source_hash, backend_key)
                print(f"[panel] cache saved: {cache_path} (cuDF path)")
                _print_feature_overview(panel)
                return panel
        except Exception as exc:
            print(f"[panel] cuDF path failed, fallback to pandas: {exc}")
            backend_key = (
                f"pandas|benchmark={benchmark_name}|"
                f"usd_only={usd_only_trading_pairs}|tradable_mode={tradable_mode}"
            )
    
    symbol_frames: list[pd.DataFrame] = []
    valid_paths: list[Path] = []
    for path in parquet_paths:
        try:
            frame = _load_symbol_frame(path)
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
            symbol_frames.append(frame)
            valid_paths.append(path)
        except Exception as exc:
            print(f"[panel] SKIP {path.name}: {exc}")

    if not symbol_frames:
        raise RuntimeError("No valid parquet files could be loaded.")

    symbols = [_symbol_name_from_path(path) for path in valid_paths]
    frame_all = pd.concat(symbol_frames, ignore_index=True)
    panel = _build_panel_from_frame(frame_all, symbols, benchmark_name=benchmark_name)
    
    source_hash = _compute_source_hash(parquet_paths)
    _save_panel_cache(cache_path, meta_path, panel, source_hash, backend_key)
    print(f"[panel] cache saved: {cache_path}")
    _print_feature_overview(panel)
    return panel
