from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np
import polars as pl


MINUTE_SESSION_BARS = 270
MINUTE_FEATURE_COLUMNS = (
    "log_close_return_1m",
    "gap_log_return",
    "intrabar_log_return",
    "range_log",
    "close_location",
    "relative_volume_20",
    "realized_volatility_20",
    "minutes_from_open",
    "time_sin",
    "time_cos",
)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON root is not an object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _partition_path(root: Path, summary: dict[str, Any]) -> Path:
    raw = Path(str(summary.get("output", "")))
    candidates = (
        raw,
        (Path.cwd() / raw),
        (root / raw),
        root / f"trade_date={summary.get('trade_date')}" / "data.parquet",
    )
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_file():
            return resolved
    return candidates[-1].resolve()


@dataclass(frozen=True, slots=True)
class MinuteFeatureNormalizer:
    mean: np.ndarray
    scale: np.ndarray
    counts: np.ndarray

    def __post_init__(self) -> None:
        width = len(MINUTE_FEATURE_COLUMNS)
        if self.mean.shape != (width,) or self.scale.shape != (width,):
            raise ValueError("minute normalizer width does not match feature schema")
        if self.counts.shape != (width,):
            raise ValueError("minute normalizer counts do not match feature schema")
        if not np.isfinite(self.mean).all() or not np.isfinite(self.scale).all():
            raise ValueError("minute normalizer contains non-finite values")
        if np.any(self.scale <= 0.0) or np.any(self.counts <= 1):
            raise ValueError("minute normalizer requires positive scales and counts > 1")


@dataclass(frozen=True, slots=True)
class MinuteDayPanel:
    trade_date: np.datetime64
    features: np.ndarray
    feature_mask: np.ndarray
    execution_mask: np.ndarray
    execution_open: np.ndarray
    exit_close: np.ndarray
    future_volume_shares: np.ndarray
    session_close: np.ndarray
    session_exit_mask: np.ndarray


@dataclass(slots=True)
class MinuteDatasetIndex:
    root: Path
    manifest: dict[str, Any]
    symbols: tuple[str, ...]
    dates: np.ndarray
    partitions: dict[str, dict[str, Any]]

    @property
    def num_symbols(self) -> int:
        return len(self.symbols)

    def fit_normalizer(self, indices: Sequence[int] | np.ndarray) -> MinuteFeatureNormalizer:
        counts = np.zeros(len(MINUTE_FEATURE_COLUMNS), dtype=np.float64)
        sums = np.zeros_like(counts)
        sum_squares = np.zeros_like(counts)
        selected = np.asarray(indices, dtype=np.int64)
        if selected.size == 0:
            raise ValueError("cannot fit minute normalizer on an empty split")
        for index in selected.tolist():
            key = str(self.dates[index].astype("datetime64[D]"))
            summary = self.partitions[key]
            for feature_index, name in enumerate(MINUTE_FEATURE_COLUMNS):
                counts[feature_index] += float(summary["feature_counts"][name])
                sums[feature_index] += float(summary["feature_sums"][name])
                sum_squares[feature_index] += float(
                    summary["feature_sum_squares"][name]
                )
        if np.any(counts <= 1.0):
            missing = [
                MINUTE_FEATURE_COLUMNS[index]
                for index in np.flatnonzero(counts <= 1.0)
            ]
            raise RuntimeError(
                f"minute training split has insufficient feature statistics: {missing}"
            )
        mean = sums / counts
        variance = np.maximum(sum_squares / counts - mean * mean, 1e-12)
        return MinuteFeatureNormalizer(
            mean=mean.astype(np.float32),
            scale=np.sqrt(variance).astype(np.float32),
            counts=counts.astype(np.int64),
        )

    def load_day(
        self,
        index: int,
        *,
        normalizer: MinuteFeatureNormalizer,
    ) -> MinuteDayPanel:
        date_key = str(self.dates[int(index)].astype("datetime64[D]"))
        summary = self.partitions[date_key]
        path = _partition_path(self.root, summary)
        columns = [
            "ts",
            "symbol",
            *MINUTE_FEATURE_COLUMNS,
            "feature_valid",
            "label_valid_1m",
            "execution_open_next_1m",
            "exit_close_next_1m",
            "future_volume_shares_next_1m",
            "session_close",
            "session_exit_valid",
        ]
        frame = pl.read_parquet(path, columns=columns).sort(["ts", "symbol"])
        if frame.is_empty():
            raise RuntimeError(f"minute partition is empty: {path}")

        symbol_values = frame["symbol"].to_numpy()
        symbol_indices = np.searchsorted(
            np.asarray(self.symbols, dtype=object), symbol_values
        )
        if np.any(symbol_indices >= self.num_symbols) or np.any(
            np.asarray(self.symbols, dtype=object)[symbol_indices] != symbol_values
        ):
            raise RuntimeError(f"minute partition contains an unknown symbol: {path}")
        timestamps = frame["ts"]
        minutes = (
            timestamps.dt.hour().to_numpy().astype(np.int16) * 60
            + timestamps.dt.minute().to_numpy().astype(np.int16)
            - 9 * 60
        )
        if np.any((minutes < 1) | (minutes > MINUTE_SESSION_BARS)):
            raise RuntimeError(f"minute partition contains out-of-session rows: {path}")
        minute_indices = minutes.astype(np.int64) - 1
        flat = minute_indices * self.num_symbols + symbol_indices
        if np.unique(flat).size != flat.size:
            raise RuntimeError(f"minute partition has duplicate symbol timestamps: {path}")

        shape = (MINUTE_SESSION_BARS, self.num_symbols)
        features = np.zeros((*shape, len(MINUTE_FEATURE_COLUMNS)), dtype=np.float32)
        feature_mask = np.zeros(shape, dtype=bool)
        execution_mask = np.zeros(shape, dtype=bool)
        execution_open = np.zeros(shape, dtype=np.float32)
        exit_close = np.zeros(shape, dtype=np.float32)
        future_volume = np.zeros(shape, dtype=np.float32)
        session_close = np.zeros(self.num_symbols, dtype=np.float32)
        session_exit_mask = np.zeros(self.num_symbols, dtype=bool)

        valid_features = frame["feature_valid"].fill_null(False).to_numpy()
        raw_features = (
            frame.select(MINUTE_FEATURE_COLUMNS)
            .fill_null(0.0)
            .to_numpy()
            .astype(np.float32, copy=False)
        )
        normalized = (raw_features - normalizer.mean) / normalizer.scale
        normalized[~np.isfinite(normalized)] = 0.0
        features[minute_indices, symbol_indices] = normalized
        feature_mask[minute_indices, symbol_indices] = valid_features

        raw_execution_mask = frame["label_valid_1m"].fill_null(False).to_numpy()
        opens = (
            frame["execution_open_next_1m"].cast(pl.Float64).fill_null(0.0).to_numpy()
        )
        closes = frame["exit_close_next_1m"].cast(pl.Float64).fill_null(0.0).to_numpy()
        volumes = (
            frame["future_volume_shares_next_1m"]
            .cast(pl.Float64)
            .fill_null(0.0)
            .to_numpy()
        )
        valid_execution = (
            raw_execution_mask
            & np.isfinite(opens)
            & np.isfinite(closes)
            & np.isfinite(volumes)
            & (opens > 0.0)
            & (closes > 0.0)
            & (volumes > 0.0)
        )
        execution_mask[minute_indices, symbol_indices] = valid_execution
        execution_open[minute_indices, symbol_indices] = np.where(
            valid_execution, opens, 0.0
        )
        exit_close[minute_indices, symbol_indices] = np.where(
            valid_execution, closes, 0.0
        )
        future_volume[minute_indices, symbol_indices] = np.where(
            valid_execution, volumes, 0.0
        )

        # The builder repeats these session fields on every source row. Taking
        # the last occurrence is deterministic after timestamp sorting.
        session_rows = frame.group_by("symbol").agg(
            pl.col("session_close").last().alias("session_close"),
            pl.col("session_exit_valid").any().alias("session_exit_valid"),
        )
        session_symbols = session_rows["symbol"].to_numpy()
        session_indices = np.searchsorted(
            np.asarray(self.symbols, dtype=object), session_symbols
        )
        session_values = (
            session_rows["session_close"]
            .cast(pl.Float64)
            .fill_null(0.0)
            .to_numpy()
        )
        exit_values = session_rows["session_exit_valid"].fill_null(False).to_numpy()
        session_close[session_indices] = session_values.astype(np.float32, copy=False)
        session_exit_mask[session_indices] = exit_values
        session_exit_mask &= np.isfinite(session_close) & (session_close > 0.0)
        return MinuteDayPanel(
            trade_date=self.dates[int(index)],
            features=features,
            feature_mask=feature_mask,
            execution_mask=execution_mask,
            execution_open=execution_open,
            exit_close=exit_close,
            future_volume_shares=future_volume,
            session_close=session_close,
            session_exit_mask=session_exit_mask,
        )


def load_minute_dataset_index(
    root: str | Path,
    *,
    require_research_ready: bool = True,
    verify_partition_sha256: bool = True,
    progress: Callable[[str], None] | None = None,
) -> MinuteDatasetIndex:
    resolved_root = Path(root).resolve()
    manifest_path = resolved_root / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"minute dataset manifest is missing: {manifest_path}")
    manifest = _read_json(manifest_path)
    if manifest.get("schema_version") != 3:
        raise RuntimeError(
            "minute dataset schema is stale; rebuild with "
            "scripts/build_shioaji_tw_minute_dataset.py"
        )
    if manifest.get("source") != "shioaji_kbars_1m":
        raise RuntimeError("minute dataset source is not Shioaji Kbars")
    if require_research_ready and not bool(manifest.get("research_ready")):
        raise RuntimeError(
            f"minute dataset is not research-ready: status={manifest.get('status')!r}"
        )
    if manifest.get("decision_clock") != "completed_right_labelled_1m_bar":
        raise RuntimeError("minute decision clock contract is missing or incompatible")
    if manifest.get("execution_clock") != "next_1m_bar_open_proxy":
        raise RuntimeError("minute execution clock contract is missing or incompatible")
    if tuple(manifest.get("model_feature_columns", [])) != MINUTE_FEATURE_COLUMNS:
        raise RuntimeError("minute model feature schema is incompatible")

    symbols = tuple(sorted(str(value) for value in manifest.get("symbols", [])))
    date_strings = [str(value) for value in manifest.get("dates", [])]
    if not symbols or not date_strings:
        raise RuntimeError("minute dataset has no symbols or dates")
    if date_strings != sorted(set(date_strings)):
        raise RuntimeError("minute manifest dates are not unique and sorted")
    partitions: dict[str, dict[str, Any]] = {}
    partition_summaries = list(manifest.get("partitions", []))
    for position, summary in enumerate(partition_summaries, start=1):
        key = str(summary.get("trade_date", ""))
        if key in partitions:
            raise RuntimeError(f"duplicate minute manifest partition: {key}")
        for stat_name in ("feature_counts", "feature_sums", "feature_sum_squares"):
            values = summary.get(stat_name)
            if not isinstance(values, dict) or set(values) != set(
                MINUTE_FEATURE_COLUMNS
            ):
                raise RuntimeError(
                    f"minute partition {key} lacks causal normalization statistics"
                )
        path = _partition_path(resolved_root, summary)
        if not path.is_file():
            raise RuntimeError(f"minute partition is missing: {path}")
        if verify_partition_sha256:
            expected = str(summary.get("output_sha256", ""))
            actual = _sha256(path)
            if not expected or actual != expected:
                raise RuntimeError(
                    f"minute partition fingerprint mismatch: {path} "
                    f"expected={expected} actual={actual}"
                )
        partitions[key] = summary
        if progress is not None and (
            position == 1
            or position % 100 == 0
            or position == len(partition_summaries)
        ):
            progress(
                "[tw-minute data] verified partitions="
                f"{position}/{len(partition_summaries)} last_date={key}"
            )
    if set(partitions) != set(date_strings):
        raise RuntimeError("minute manifest date list and partition list disagree")
    dates = np.asarray(date_strings, dtype="datetime64[D]")
    return MinuteDatasetIndex(
        root=resolved_root,
        manifest=manifest,
        symbols=symbols,
        dates=dates,
        partitions=partitions,
    )


__all__ = [
    "MINUTE_FEATURE_COLUMNS",
    "MINUTE_SESSION_BARS",
    "MinuteDatasetIndex",
    "MinuteDayPanel",
    "MinuteFeatureNormalizer",
    "load_minute_dataset_index",
]
