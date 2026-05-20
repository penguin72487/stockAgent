from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

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

    # target: next-day log return (forward-looking, computed from raw prices)
    frame["return_1d"] = np.log(frame["close"].shift(-1) / frame["close"])

    # tradable: computed from original prices/volume before log transformation
    volume = frame["Trading_Volume"] if "Trading_Volume" in frame.columns else pd.Series(0.0, index=frame.index)
    frame["tradable"] = frame["close"].notna() & pd.Series(volume).fillna(0).gt(0)

    # convert OHLC prices to log returns (stationary, scale-free, O(1) range)
    for col in ["open", "max", "min", "close"]:
        if col in frame.columns:
            frame[col] = np.log(frame[col] / frame[col].shift(1))

    # convert volume to log return; zeros become NaN and are handled downstream
    if "Trading_Volume" in frame.columns:
        vol = frame["Trading_Volume"].replace(0, np.nan)
        frame["Trading_Volume"] = np.log(vol / vol.shift(1))

    # compress fundamental features: sign(x)*log1p(|x|) keeps monotonicity,
    # pulls extreme values (e.g. ROE=772) to ~6.6, avoids z-score / time-leakage
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


def build_panel(parquet_root: str | Path) -> PanelData:
    parquet_paths = sorted(Path(parquet_root).glob("*_features.parquet"))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files found under {parquet_root}")

    symbol_frames: list[pd.DataFrame] = []
    valid_paths: list[Path] = []
    for path in parquet_paths:
        try:
            frame = _load_symbol_frame(path)
            if len(frame) == 0:
                print(f"[panel] SKIP {path.name}: empty dataframe")
                continue
            symbol_frames.append(frame)
            valid_paths.append(path)
        except Exception as exc:
            print(f"[panel] SKIP {path.name}: {exc}")

    if not symbol_frames:
        raise RuntimeError("No valid parquet files could be loaded.")

    feature_columns = _get_feature_columns(symbol_frames[0])

    all_dates = sorted({date for frame in symbol_frames for date in frame["date"].tolist()})
    # derive symbol names from filenames (avoids relying on frame data)
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
        # alive: has valid feature data (log return computed → both current & previous price exist)
        alive_mask[row_indices, symbol_idx] = aligned["close"].notna().to_numpy(dtype=bool)

    # benchmark: universe-average log return; 0 on days with no tradable symbols (no NaN warning)
    n_tradable = tradable_mask.sum(axis=1)
    sum_ret = np.nansum(np.where(tradable_mask, returns_1d, 0.0), axis=1)
    benchmark_returns = np.where(n_tradable > 0, sum_ret / n_tradable, 0.0).astype(np.float32)

    # replace NaN/inf with 0 so the model sees a valid (zero-information) input
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
    )
