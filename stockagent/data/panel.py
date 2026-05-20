from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import hashlib
import pickle

import numpy as np
import pandas as pd


RESERVED_COLUMNS = {"date", "symbol", "return_1d", "tradable"}


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

    frame["return_1d"] = np.log(frame["close"].shift(-1) / frame["close"])

    volume = frame["Trading_Volume"] if "Trading_Volume" in frame.columns else pd.Series(0.0, index=frame.index)
    frame["tradable"] = frame["close"].notna() & pd.Series(volume).fillna(0).gt(0)

    for col in ["open", "max", "min", "close"]:
        if col in frame.columns:
            frame[col] = np.log(frame[col] / frame[col].shift(1))

    if "Trading_Volume" in frame.columns:
        vol = frame["Trading_Volume"].replace(0, np.nan)
        frame["Trading_Volume"] = np.log(vol / vol.shift(1))

    fundamental_cols = [c for c in frame.columns
                        if c not in {"open", "max", "min", "close", "Trading_Volume",
                                     "date", "symbol", "return_1d", "tradable"}
                        and pd.api.types.is_numeric_dtype(frame[c])]
    for col in fundamental_cols:
        x = frame[col].to_numpy(dtype=np.float64)
        frame[col] = np.sign(x) * np.log1p(np.abs(x))

    return frame


def _get_feature_columns(frame: pd.DataFrame) -> list[str]:
    numeric_columns = frame.select_dtypes(include=[np.number]).columns.tolist()
    return [column for column in numeric_columns if column not in RESERVED_COLUMNS]


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
    )
    
    meta = {
        'version': 2,
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
    )


def _check_cache_valid(meta_path: Path, parquet_paths: list[Path]) -> bool:
    """Check if cache is valid based on source hash."""
    if not meta_path.exists():
        return False
    
    try:
        with open(meta_path, 'rb') as f:
            meta = pickle.load(f)
        
        expected_hash = _compute_source_hash(parquet_paths)
        return meta.get('source_hash') == expected_hash and meta.get('version') == 2
    except Exception:
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
        return _load_panel_cache(cache_path)

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
        tradable_mask[row_indices, symbol_idx] = aligned["tradable"].to_numpy(dtype=bool)
        alive_mask[row_indices, symbol_idx] = aligned["close"].notna().to_numpy(dtype=bool)

    n_tradable = tradable_mask.sum(axis=1)
    sum_ret = np.nansum(np.where(tradable_mask, returns_1d, 0.0), axis=1)
    benchmark_returns = np.where(n_tradable > 0, sum_ret / n_tradable, 0.0).astype(np.float32)

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
    )
    
    source_hash = _compute_source_hash(parquet_paths)
    _save_panel_cache(cache_path, meta_path, panel, source_hash)
    print(f"[panel] cache saved: {cache_path}")
    return panel
