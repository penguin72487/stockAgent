from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import pickle

import numpy as np
import pandas as pd


RESERVED_COLUMNS = {"date", "symbol", "return_1d", "tradable"}
LOG_RETURN_FEATURE_COLUMNS = ["open", "max", "min", "close", "Trading_Volume"]
PANEL_CACHE_VERSION = 5


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


def _load_symbol_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date").reset_index(drop=True)
    frame["symbol"] = path.name.replace("_features.parquet", "")
    frame["close_raw"] = frame["close"].astype(np.float32)

    frame["return_1d"] = np.log(frame["close"].shift(-1) / frame["close"])

    volume = frame["Trading_Volume"] if "Trading_Volume" in frame.columns else pd.Series(0.0, index=frame.index)
    frame["tradable"] = frame["close"].notna() & pd.Series(volume).fillna(0).gt(0)

    for col in ["open", "max", "min", "close"]:
        if col in frame.columns:
            frame[col] = np.log(frame[col] / frame[col].shift(1))

    if "Trading_Volume" in frame.columns:
        vol = frame["Trading_Volume"].replace(0, np.nan)
        frame["Trading_Volume"] = np.log(vol / vol.shift(1))

    return frame


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


def build_panel(parquet_root: str | Path) -> PanelData:
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

    feature_columns = _get_feature_columns(symbol_frames[0])

    all_dates = sorted({date for frame in symbol_frames for date in frame["date"].tolist()})
    symbols = [path.name.replace("_features.parquet", "") for path in valid_paths]

    num_dates = len(all_dates)
    num_symbols = len(symbols)
    num_features = len(feature_columns)

    features = np.full((num_dates, num_symbols, num_features), np.nan, dtype=np.float32)
    returns_1d = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    close_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
    tradable_mask = np.zeros((num_dates, num_symbols), dtype=bool)
    alive_mask = np.zeros((num_dates, num_symbols), dtype=bool)

    date_index = {date: idx for idx, date in enumerate(all_dates)}
    for symbol_idx, frame in enumerate(symbol_frames):
        frame = frame.set_index("date")
        valid_dates = frame.index.intersection(all_dates)
        row_indices = np.array([date_index[date] for date in valid_dates], dtype=np.int64)
        aligned = frame.loc[valid_dates]

        features[row_indices, symbol_idx, :] = aligned[feature_columns].to_numpy(dtype=np.float32)
        returns_1d[row_indices, symbol_idx] = aligned["return_1d"].to_numpy(dtype=np.float32)
        close_prices[row_indices, symbol_idx] = aligned["close_raw"].to_numpy(dtype=np.float32)
        tradable_mask[row_indices, symbol_idx] = aligned["tradable"].to_numpy(dtype=bool)
        alive_mask[row_indices, symbol_idx] = aligned["close"].notna().to_numpy(dtype=bool)

    n_tradable = tradable_mask.sum(axis=1)
    sum_ret = np.nansum(np.where(tradable_mask, returns_1d, 0.0), axis=1)
    benchmark_returns = np.where(n_tradable > 0, sum_ret / n_tradable, 0.0).astype(np.float32)

    # Keep raw log-return features in panel; fold-local normalization is applied
    # later in training using train-period statistics only to prevent data leakage.
    features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)

    panel = PanelData(
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
    
    source_hash = _compute_source_hash(parquet_paths)
    _save_panel_cache(cache_path, meta_path, panel, source_hash)
    print(f"[panel] cache saved: {cache_path}")
    _print_feature_overview(panel)
    return panel
