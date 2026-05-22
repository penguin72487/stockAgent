from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import pickle
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

try:
    import cudf
except Exception:  # pragma: no cover - optional GPU dependency
    cudf = None


RESERVED_COLUMNS = {"date", "symbol", "return_1d", "tradable"}
LOG_RETURN_FEATURE_COLUMNS = ["open", "max", "min", "close", "Trading_Volume", "next_open"]
PANEL_CACHE_VERSION = 9


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
    open_prices: np.ndarray
    close_prices: np.ndarray

    @property
    def num_dates(self) -> int:
        return int(self.features.shape[0])

    @property
    def num_symbols(self) -> int:
        return int(self.features.shape[1])


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


def _load_symbol_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    if not pd.api.types.is_datetime64_any_dtype(frame["date"]):
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["symbol"] = path.name.replace("_features.parquet", "")
    open_num = pd.to_numeric(frame["open"], errors="coerce")
    max_num = pd.to_numeric(frame["max"], errors="coerce")
    min_num = pd.to_numeric(frame["min"], errors="coerce")
    close_num = pd.to_numeric(frame["close"], errors="coerce")
    if "Trading_Volume" in frame.columns:
        volume_num = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
    else:
        volume_num = pd.Series(0.0, index=frame.index, dtype=np.float64)

    frame["open_raw"] = open_num.astype(np.float32)
    frame["close_raw"] = close_num.astype(np.float32)

    # User-defined feature timing:
    # - open/max/min/close/Trading_Volume use previous-day values at row t
    # - next_open is today's open used as execution buy price
    frame["open"] = open_num.shift(1).astype(np.float32)
    frame["max"] = max_num.shift(1).astype(np.float32)
    frame["min"] = min_num.shift(1).astype(np.float32)
    frame["close"] = close_num.shift(1).astype(np.float32)
    frame["Trading_Volume"] = volume_num.shift(1).astype(np.float32)
    frame["next_open"] = open_num.astype(np.float32)

    # Day-trading target: same-day open->close log return.
    frame["return_1d"] = _safe_log_ratio(close_num, open_num)

    volume = volume_num
    frame["tradable"] = (
        frame["open_raw"].gt(0)
        & frame["close_raw"].gt(0)
        & pd.Series(volume).fillna(0).gt(0)
    )

    return frame


def _load_symbol_frame_cudf(path: Path) -> pd.DataFrame:
    if cudf is None:
        raise RuntimeError("cuDF is not available")

    gdf = cudf.read_parquet(path)
    gdf["date"] = cudf.to_datetime(gdf["date"])
    gdf = gdf.sort_values("date").reset_index(drop=True)
    gdf["symbol"] = path.name.replace("_features.parquet", "")
    open_num = gdf["open"].astype("float64")
    max_num = gdf["max"].astype("float64")
    min_num = gdf["min"].astype("float64")
    close_num = gdf["close"].astype("float64")
    if "Trading_Volume" in gdf.columns:
        volume_num = gdf["Trading_Volume"].astype("float64")
    else:
        volume_num = cudf.Series(0.0, index=gdf.index, dtype="float64")

    gdf["open_raw"] = open_num.astype("float32")
    gdf["close_raw"] = close_num.astype("float32")
    gdf["open"] = open_num.shift(1).astype("float32")
    gdf["max"] = max_num.shift(1).astype("float32")
    gdf["min"] = min_num.shift(1).astype("float32")
    gdf["close"] = close_num.shift(1).astype("float32")
    gdf["Trading_Volume"] = volume_num.shift(1).astype("float32")
    gdf["next_open"] = open_num.astype("float32")

    valid_ret = (close_num > 0) & (open_num > 0)
    ret_ratio = (close_num / open_num).where(valid_ret)
    gdf["return_1d"] = np.log(ret_ratio)

    vol = volume_num.fillna(0)
    gdf["tradable"] = (gdf["open_raw"] > 0) & (gdf["close_raw"] > 0) & (vol > 0)

    return gdf.to_pandas()


def _build_panel_from_frame(frame_all: pd.DataFrame, symbols: list[str]) -> PanelData:
    feature_columns = _get_feature_columns(frame_all)

    all_dates = sorted(frame_all["date"].dropna().unique().tolist())
    num_dates = len(all_dates)
    num_symbols = len(symbols)
    num_features = len(feature_columns)

    features = np.full((num_dates, num_symbols, num_features), np.nan, dtype=np.float32)
    returns_1d = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    open_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    close_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    tradable_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    alive_mask = np.zeros((num_dates, num_symbols), dtype=bool)

    date_index = {date: idx for idx, date in enumerate(all_dates)}
    symbol_index = {symbol: idx for idx, symbol in enumerate(symbols)}

    frame_all = frame_all[frame_all["symbol"].isin(symbols)].copy()
    row_idx = frame_all["date"].map(date_index).to_numpy(dtype=np.int64)
    sym_idx = frame_all["symbol"].map(symbol_index).to_numpy(dtype=np.int64)

    feature_values = frame_all[feature_columns].to_numpy(dtype=np.float32, copy=False)
    features[row_idx, sym_idx, :] = feature_values

    returns_1d[row_idx, sym_idx] = frame_all["return_1d"].to_numpy(dtype=np.float32, copy=False)
    open_prices[row_idx, sym_idx] = frame_all["next_open"].to_numpy(dtype=np.float32, copy=False)
    close_prices[row_idx, sym_idx] = frame_all["close_raw"].to_numpy(dtype=np.float32, copy=False)
    tradable_mask[row_idx, sym_idx] = frame_all["tradable"].to_numpy(dtype=bool, copy=False)
    alive_mask[row_idx, sym_idx] = frame_all["close_raw"].notna().to_numpy(dtype=bool, copy=False)

    valid_ret_mask = np.isfinite(returns_1d)
    n_valid = valid_ret_mask.sum(axis=1)
    sum_ret = np.nansum(np.where(valid_ret_mask, returns_1d, 0.0), axis=1)
    benchmark_returns = np.zeros_like(sum_ret, dtype=np.float32)
    np.divide(
        sum_ret,
        n_valid,
        out=benchmark_returns,
        where=n_valid > 0,
    )

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
        open_prices=open_prices,
        close_prices=close_prices,
    )


def _get_feature_columns(frame: pd.DataFrame) -> list[str]:
    numeric_columns = set(frame.select_dtypes(include=[np.number]).columns.tolist())
    feature_columns = [
        column
        for column in LOG_RETURN_FEATURE_COLUMNS
        if column in numeric_columns and column not in RESERVED_COLUMNS
    ]
    if len(feature_columns) != len(LOG_RETURN_FEATURE_COLUMNS):
        raise ValueError(
            "Missing log-return feature columns in symbol frame: "
            f"expected={LOG_RETURN_FEATURE_COLUMNS}, got={feature_columns}"
        )
    return feature_columns


def _resolve_panel_workers() -> int:
    env_workers = os.environ.get("STOCKAGENT_PANEL_WORKERS")
    if env_workers is not None:
        try:
            workers = int(env_workers)
            return max(1, workers)
        except ValueError:
            pass
    cpu_count = os.cpu_count() or 4
    return max(1, min(16, cpu_count))


def _load_symbol_frames_parallel(parquet_paths: list[Path]) -> tuple[list[pd.DataFrame], list[Path]]:
    workers = _resolve_panel_workers()
    symbol_frames: list[pd.DataFrame] = []
    valid_paths: list[Path] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_load_symbol_frame, path): path for path in parquet_paths}
        for future in as_completed(futures):
            path = futures[future]
            try:
                frame = future.result()
                if len(frame) == 0:
                    raise ValueError(f"Symbol file is empty: {path.name}")
                symbol_frames.append(frame)
                valid_paths.append(path)
            except Exception as exc:
                print(f"[panel] SKIP {path.name}: {exc}")

    ordered = sorted(zip(valid_paths, symbol_frames), key=lambda item: item[0].name)
    ordered_paths = [item[0] for item in ordered]
    ordered_frames = [item[1] for item in ordered]
    return ordered_frames, ordered_paths


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


def _save_panel_cache(cache_path: Path, meta_path: Path, panel: PanelData, source_hash: str) -> None:
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
        open_prices=panel.open_prices,
        close_prices=panel.close_prices,
    )
    
    meta = {
        'version': PANEL_CACHE_VERSION,
        'source_hash': source_hash,
        'num_dates': panel.num_dates,
        'num_symbols': panel.num_symbols,
    }
    with open(meta_path, 'wb') as f:
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
        open_prices=cached["open_prices"],
        close_prices=cached["close_prices"],
    )


def _print_feature_overview(panel: PanelData) -> None:
    feature_list = ", ".join(panel.feature_names)
    print(f"[panel] features ({len(panel.feature_names)}): {feature_list}")


def _check_cache_valid(meta_path: Path, parquet_paths: list[Path]) -> bool:
    """Check if cache is valid based on source hash and mtime."""
    if not meta_path.exists():
        return False
    
    try:
        with open(meta_path, 'rb') as f:
            meta = pickle.load(f)
        
        # ✅ OPTIMIZATION: Check both version and source hash for cache validity
        expected_hash = _compute_source_hash(parquet_paths)
        cache_valid = (
            meta.get('source_hash') == expected_hash and 
            meta.get('version') == PANEL_CACHE_VERSION
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


def build_panel(parquet_root: str | Path, use_rapids: bool = True) -> PanelData:
    parquet_root = Path(parquet_root)
    parquet_paths = sorted(parquet_root.glob("*_features.parquet"))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files found under {parquet_root}")

    cache_path = _panel_cache_path(parquet_root)
    meta_path = _cache_meta_path(parquet_root)
    
    # Check cache validity
    if _check_cache_valid(meta_path, parquet_paths):
        print(f"[panel] loading cache (valid): {cache_path}")
        panel = _load_panel_cache(cache_path)
        _print_feature_overview(panel)
        return panel

    print(f"[panel] building from {len(parquet_paths)} parquet files...")

    env_rapids = os.environ.get("STOCKAGENT_USE_CUDF")
    use_cudf = ((env_rapids == "1") if env_rapids is not None else use_rapids) and cudf is not None
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
                symbols_cudf = [path.name.replace("_features.parquet", "") for path in valid_paths_cudf]
                frame_all_cudf = pd.concat(symbol_frames_cudf, ignore_index=True)
                panel = _build_panel_from_frame(frame_all_cudf, symbols_cudf)
                source_hash = _compute_source_hash(parquet_paths)
                _save_panel_cache(cache_path, meta_path, panel, source_hash)
                print(f"[panel] cache saved: {cache_path} (cuDF path)")
                _print_feature_overview(panel)
                return panel
        except Exception as exc:
            print(f"[panel] cuDF path failed, fallback to pandas: {exc}")
    
    symbol_frames, valid_paths = _load_symbol_frames_parallel(parquet_paths)

    if not symbol_frames:
        raise RuntimeError("No valid parquet files could be loaded.")

    symbols = [path.name.replace("_features.parquet", "") for path in valid_paths]
    frame_all = pd.concat(symbol_frames, ignore_index=True)
    panel = _build_panel_from_frame(frame_all, symbols)
    
    source_hash = _compute_source_hash(parquet_paths)
    _save_panel_cache(cache_path, meta_path, panel, source_hash)
    print(f"[panel] cache saved: {cache_path}")
    _print_feature_overview(panel)
    return panel
