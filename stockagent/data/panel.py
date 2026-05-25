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
LOG_RETURN_FEATURE_COLUMNS = ["open", "max", "min", "close", "Trading_Volume"]
PANEL_CACHE_VERSION = 8
FEATURE_FILE_SUFFIX = "_features.parquet"


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

    @property
    def num_dates(self) -> int:
        return int(self.features.shape[0])

    @property
    def num_symbols(self) -> int:
        return int(self.features.shape[1])


def _symbol_name_from_path(path: Path) -> str:
    return path.name.removesuffix(FEATURE_FILE_SUFFIX)


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


def _load_symbol_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    if not pd.api.types.is_datetime64_any_dtype(frame["date"]):
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["symbol"] = _symbol_name_from_path(path)
    frame["close_raw"] = frame["close"].astype(np.float32)

    frame["return_1d"] = _safe_log_ratio(frame["close"].shift(-1), frame["close"])

    frame["tradable"] = _compute_tradable_from_frame(frame)

    for col in ["open", "max", "min", "close"]:
        if col in frame.columns:
            frame[col] = _safe_log_ratio(frame[col], frame[col].shift(1))

    if "Trading_Volume" in frame.columns:
        vol = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
        frame["Trading_Volume"] = _safe_log_ratio(vol, vol.shift(1))

    return frame


def _load_symbol_frame_cudf(path: Path) -> pd.DataFrame:
    if cudf is None:
        raise RuntimeError("cuDF is not available")

    gdf = cudf.read_parquet(path)
    gdf["date"] = cudf.to_datetime(gdf["date"])
    gdf = gdf.sort_values("date").reset_index(drop=True)
    gdf["symbol"] = _symbol_name_from_path(path)
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

    return gdf.to_pandas()


def _resolve_benchmark_index(symbols: list[str], benchmark_name: str) -> int | None:
    key = (benchmark_name or "").strip()
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
    return PanelData(
        dates=cached["dates"],
        symbols=cached["symbols"].tolist(),
        feature_names=cached["feature_names"].tolist(),
        features=cached["features"],
        returns_1d=cached["returns_1d"],
        tradable_mask=cached["tradable_mask"],
        alive_mask=cached["alive_mask"],
        benchmark_returns=cached["benchmark_returns"],
        close_prices=cached["close_prices"],
    )


def _print_feature_overview(panel: PanelData) -> None:
    feature_list = ", ".join(panel.feature_names)
    print(f"[panel] features ({len(panel.feature_names)}): {feature_list}")


def _check_cache_valid(meta_path: Path, parquet_paths: list[Path], backend_key: str) -> bool:
    """Check if cache is valid based on source hash and mtime."""
    if not meta_path.exists():
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
            cache_mtime = meta_path.stat().st_mtime
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
) -> PanelData:
    parquet_root = Path(parquet_root)
    parquet_paths = sorted(parquet_root.glob(f"*{FEATURE_FILE_SUFFIX}"))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files found under {parquet_root}")

    cache_path = _panel_cache_path(parquet_root)
    meta_path = _cache_meta_path(parquet_root)

    env_rapids = os.environ.get("STOCKAGENT_USE_CUDF")
    use_cudf = ((env_rapids == "1") if env_rapids is not None else use_rapids) and cudf is not None
    backend_key = f"{'cudf' if use_cudf else 'pandas'}|benchmark={benchmark_name}"
    
    # Check cache validity
    if _check_cache_valid(meta_path, parquet_paths, backend_key):
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
            backend_key = f"pandas|benchmark={benchmark_name}"
    
    symbol_frames: list[pd.DataFrame] = []
    valid_paths: list[Path] = []
    for path in parquet_paths:
        try:
            frame = _load_symbol_frame(path)
            if len(frame) == 0:
                raise ValueError(f"Symbol file is empty: {path.name}")
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
