from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np
import polars as pl


MINUTE_SESSION_BARS = 270
MINUTE_DATASET_SCHEMA_VERSION = 5
MINUTE_FEATURE_STATISTICS_CONTRACT = "float64_sums_developing_candle_v2"
MINUTE_DEVELOPING_CANDLE_CONTRACT = (
    "completed_minute_session_to_date_previous_completed_symbol_session_v2"
)
MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS = (
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
MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS = (
    "open_logret_1d",
    "max_logret_1d",
    "min_logret_1d",
    "close_logret_1d",
    "trading_volume_logret_1d",
    "signed_vol",
    "body_ratio",
    "signed_body_ratio",
    "delta_body_ratio",
    "clv",
    "clv_centered",
    "delta_clv",
    "upper_shadow",
    "lower_shadow",
    "shadow_imbalance",
)
MINUTE_FEATURE_COLUMNS = (
    *MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS,
    *MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS,
)
MINUTE_DAILY_OPEN_GAP_FEATURE = "next_session_open_gap_logret"
MINUTE_DAILY_CONTEXT_CONTRACT = (
    "causal_completed_daily_history_plus_observed_open_v3"
)
_TW_MAX_ABS_DAILY_PRICE_LOG_RETURN = float(np.log(2.0))


def minute_static_daily_context_feature_names(
    feature_names: Sequence[str],
) -> tuple[str, ...]:
    """Split the ordinary daily feature list without losing information.

    Pure OHLCV/candlestick fields are rebuilt causally from the developing
    current-session candle on every completed minute.  Only the remaining
    point-in-time fields are broadcast once per session as daily context.
    """

    requested = tuple(str(name) for name in feature_names)
    missing = [
        name
        for name in MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS
        if name not in requested
    ]
    if missing:
        raise RuntimeError(
            "tw_minute complete daily-feature contract lacks developing "
            f"candlestick fields: {missing}"
        )
    return tuple(
        name
        for name in requested
        if name not in MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS
    )


def _finite(expr: pl.Expr) -> pl.Expr:
    return expr.is_not_null() & expr.is_finite().fill_null(False)


def _positive_finite(expr: pl.Expr) -> pl.Expr:
    return _finite(expr) & (expr > 0.0)


def _bounded_candle_ratio(
    numerator: pl.Expr,
    spread: pl.Expr,
    valid_envelope: pl.Expr,
    *,
    flat_value: float,
    lower: float,
    upper: float,
) -> pl.Expr:
    return (
        pl.when(valid_envelope & (spread > 1e-12))
        .then((numerator / spread).clip(lower, upper))
        .when(valid_envelope)
        .then(pl.lit(flat_value, dtype=pl.Float64))
        .otherwise(None)
    )


def _developing_price_log_return(
    numerator: pl.Expr,
    denominator: pl.Expr,
) -> pl.Expr:
    value = (numerator / denominator).log()
    valid = (
        _positive_finite(numerator)
        & _positive_finite(denominator)
        & _finite(value)
        & (value.abs() <= _TW_MAX_ABS_DAILY_PRICE_LOG_RETURN)
    )
    return pl.when(valid).then(value).otherwise(None)


def _developing_safe_log_return(
    numerator: pl.Expr,
    denominator: pl.Expr,
) -> pl.Expr:
    value = (numerator / denominator).log()
    valid = (
        _positive_finite(numerator)
        & _positive_finite(denominator)
        & _finite(value)
    )
    return pl.when(valid).then(value).otherwise(None)


def add_developing_daily_candle_features(
    frame: pl.DataFrame,
    previous_sessions: pl.DataFrame,
) -> pl.DataFrame:
    """Add causal session-to-date daily-candle features to minute bars.

    Each output row uses only bars at or before that row.  The comparison
    baseline is the immediately preceding completed minute-data session.  The
    joined baseline must use the ``previous_*`` schema produced by
    :func:`summarize_minute_sessions_for_next_day`.
    """

    required = {
        "ts",
        "symbol",
        "Open",
        "High",
        "Low",
        "Close",
        "volume_shares",
        "source_volume_unit_valid",
        "feature_valid",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(
            f"minute frame lacks developing-candle inputs: {missing}"
        )
    prior_required = {
        "symbol",
        "previous_open",
        "previous_high",
        "previous_low",
        "previous_close",
        "previous_volume",
        "previous_body_ratio",
        "previous_clv",
        "previous_session_valid",
    }
    prior_missing = sorted(prior_required - set(previous_sessions.columns))
    if prior_missing:
        raise RuntimeError(
            f"previous minute sessions lack baseline fields: {prior_missing}"
        )
    daily_baseline_names = {
        name: f"_daily_{name}"
        for name in prior_required
        if name != "symbol"
    }
    daily_baseline = previous_sessions.select(sorted(prior_required)).rename(
        daily_baseline_names
    )

    output = (
        frame.sort(["symbol", "ts"])
        .join(daily_baseline, on="symbol", how="left")
        .with_columns(
            pl.col("Open").cast(pl.Float64).first().over("symbol").alias("_developing_open"),
            pl.col("High").cast(pl.Float64).cum_max().over("symbol").alias("_developing_high"),
            pl.col("Low").cast(pl.Float64).cum_min().over("symbol").alias("_developing_low"),
            pl.col("Close").cast(pl.Float64).alias("_developing_close"),
            pl.col("volume_shares")
            .cast(pl.Float64)
            .cum_sum()
            .over("symbol")
            .alias("_developing_volume"),
            (
                _positive_finite(pl.col("Open"))
                & _positive_finite(pl.col("High"))
                & _positive_finite(pl.col("Low"))
                & _positive_finite(pl.col("Close"))
                & (pl.col("High") >= pl.max_horizontal("Open", "Close"))
                & (pl.col("Low") <= pl.min_horizontal("Open", "Close"))
            )
            .cast(pl.Int8)
            .cum_min()
            .over("symbol")
            .cast(pl.Boolean)
            .alias("_developing_price_prefix_valid"),
            (
                pl.col("source_volume_unit_valid")
                .fill_null(False)
                .cast(pl.Int8)
                .cum_min()
                .over("symbol")
                .cast(pl.Boolean)
                & _finite(pl.col("volume_shares"))
                & (pl.col("volume_shares") >= 0.0)
            ).alias("_developing_volume_prefix_valid"),
        )
    )
    open_px = pl.col("_developing_open")
    high_px = pl.col("_developing_high")
    low_px = pl.col("_developing_low")
    close_px = pl.col("_developing_close")
    spread = high_px - low_px
    valid_envelope = (
        _positive_finite(open_px)
        & _positive_finite(high_px)
        & _positive_finite(low_px)
        & _positive_finite(close_px)
        & (high_px >= pl.max_horizontal(open_px, close_px))
        & (low_px <= pl.min_horizontal(open_px, close_px))
    )
    output = output.with_columns(
        _bounded_candle_ratio(
            (close_px - open_px).abs(),
            spread,
            valid_envelope,
            flat_value=0.0,
            lower=0.0,
            upper=1.0,
        ).alias("body_ratio"),
        _bounded_candle_ratio(
            close_px - open_px,
            spread,
            valid_envelope,
            flat_value=0.0,
            lower=-1.0,
            upper=1.0,
        ).alias("signed_body_ratio"),
        _bounded_candle_ratio(
            close_px - low_px,
            spread,
            valid_envelope,
            flat_value=0.5,
            lower=0.0,
            upper=1.0,
        ).alias("clv"),
        _bounded_candle_ratio(
            high_px - pl.max_horizontal(open_px, close_px),
            spread,
            valid_envelope,
            flat_value=0.0,
            lower=0.0,
            upper=1.0,
        ).alias("upper_shadow"),
        _bounded_candle_ratio(
            pl.min_horizontal(open_px, close_px) - low_px,
            spread,
            valid_envelope,
            flat_value=0.0,
            lower=0.0,
            upper=1.0,
        ).alias("lower_shadow"),
        _developing_price_log_return(open_px, pl.col("_daily_previous_open")).alias(
            "open_logret_1d"
        ),
        _developing_price_log_return(high_px, pl.col("_daily_previous_high")).alias(
            "max_logret_1d"
        ),
        _developing_price_log_return(low_px, pl.col("_daily_previous_low")).alias(
            "min_logret_1d"
        ),
        _developing_price_log_return(close_px, pl.col("_daily_previous_close")).alias(
            "close_logret_1d"
        ),
        _developing_safe_log_return(
            pl.col("_developing_volume"), pl.col("_daily_previous_volume")
        ).alias("trading_volume_logret_1d"),
    ).with_columns(
        (
            pl.col("body_ratio") - pl.col("_daily_previous_body_ratio")
        ).alias("delta_body_ratio"),
        (pl.col("clv") - 0.5).alias("clv_centered"),
        (pl.col("clv") - pl.col("_daily_previous_clv")).alias("delta_clv"),
        (pl.col("upper_shadow") - pl.col("lower_shadow")).alias(
            "shadow_imbalance"
        ),
        (
            (close_px - open_px).sign()
            * pl.col("trading_volume_logret_1d")
        ).alias("signed_vol"),
    )
    all_developing_finite = pl.all_horizontal(
        *[
            _finite(pl.col(name))
            for name in MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS
        ]
    )
    return output.with_columns(
        (
            pl.col("feature_valid").fill_null(False)
            & pl.col("_daily_previous_session_valid").fill_null(False)
            & pl.col("_developing_price_prefix_valid").fill_null(False)
            & pl.col("_developing_volume_prefix_valid").fill_null(False)
            & all_developing_finite
        ).alias("feature_valid")
    ).drop(
        *daily_baseline_names.values(),
        "_developing_open",
        "_developing_high",
        "_developing_low",
        "_developing_close",
        "_developing_volume",
        "_developing_price_prefix_valid",
        "_developing_volume_prefix_valid",
    )


def summarize_minute_sessions_for_next_day(frame: pl.DataFrame) -> pl.DataFrame:
    """Create the completed-session baseline used by the next partition."""

    required = {
        "ts",
        "symbol",
        "Open",
        "High",
        "Low",
        "Close",
        "volume_shares",
        "source_volume_unit_valid",
        "session_exit_valid",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"minute frame lacks session-summary inputs: {missing}")
    summary = (
        frame.sort(["symbol", "ts"])
        .group_by("symbol", maintain_order=True)
        .agg(
            pl.col("Open").cast(pl.Float64).first().alias("previous_open"),
            pl.col("High").cast(pl.Float64).max().alias("previous_high"),
            pl.col("Low").cast(pl.Float64).min().alias("previous_low"),
            pl.col("Close").cast(pl.Float64).last().alias("previous_close"),
            pl.col("volume_shares").cast(pl.Float64).sum().alias("previous_volume"),
            pl.col("source_volume_unit_valid").fill_null(False).all().alias(
                "_volume_valid"
            ),
            pl.col("session_exit_valid").fill_null(False).any().alias(
                "_exit_valid"
            ),
            (
                _positive_finite(pl.col("Open"))
                & _positive_finite(pl.col("High"))
                & _positive_finite(pl.col("Low"))
                & _positive_finite(pl.col("Close"))
                & (pl.col("High") >= pl.max_horizontal("Open", "Close"))
                & (pl.col("Low") <= pl.min_horizontal("Open", "Close"))
            )
            .all()
            .alias("_prices_valid"),
        )
    )
    open_px = pl.col("previous_open")
    high_px = pl.col("previous_high")
    low_px = pl.col("previous_low")
    close_px = pl.col("previous_close")
    spread = high_px - low_px
    valid_envelope = (
        _positive_finite(open_px)
        & _positive_finite(high_px)
        & _positive_finite(low_px)
        & _positive_finite(close_px)
        & (high_px >= pl.max_horizontal(open_px, close_px))
        & (low_px <= pl.min_horizontal(open_px, close_px))
    )
    return summary.with_columns(
        _bounded_candle_ratio(
            (close_px - open_px).abs(),
            spread,
            valid_envelope,
            flat_value=0.0,
            lower=0.0,
            upper=1.0,
        ).alias("previous_body_ratio"),
        _bounded_candle_ratio(
            close_px - low_px,
            spread,
            valid_envelope,
            flat_value=0.5,
            lower=0.0,
            upper=1.0,
        ).alias("previous_clv"),
        (
            pl.col("_volume_valid")
            & pl.col("_exit_valid")
            & pl.col("_prices_valid")
            & _positive_finite(pl.col("previous_volume"))
            & valid_envelope
        ).alias("previous_session_valid"),
    ).drop("_volume_valid", "_exit_valid", "_prices_valid")


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


def _partition_verification_cache_path(root: Path) -> Path:
    """Return a machine-local receipt path without mutating the dataset tree."""

    cache_root = Path(
        os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))
    )
    root_key = hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:24]
    return cache_root / "stockagent" / "tw_minute_sha256" / f"{root_key}.json"


def _partition_stat_contract(path: Path) -> dict[str, int | str]:
    stat = path.stat()
    return {
        "path": str(path),
        "size": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
        "ctime_ns": int(stat.st_ctime_ns),
        "device": int(stat.st_dev),
        "inode": int(stat.st_ino),
    }


def _load_partition_verification_receipt(
    root: Path,
    *,
    manifest_sha256: str,
    contracts: list[dict[str, int | str]],
) -> bool:
    path = _partition_verification_cache_path(root)
    try:
        payload = _read_json(path)
    except (OSError, json.JSONDecodeError, RuntimeError):
        return False
    return bool(
        payload.get("schema_version") == 1
        and payload.get("manifest_sha256") == manifest_sha256
        and payload.get("partitions") == contracts
    )


def _write_partition_verification_receipt(
    root: Path,
    *,
    manifest_sha256: str,
    contracts: list[dict[str, int | str]],
) -> None:
    path = _partition_verification_cache_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "dataset_root": str(root),
        "manifest_sha256": manifest_sha256,
        "partitions": contracts,
    }
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(temporary, path)


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
    short_open_mask: np.ndarray | None = None
    short_capacity_shares: np.ndarray | None = None
    daily_context_features: np.ndarray | None = None
    benchmark_log_return: float = float("nan")
    benchmark_price_available: bool = False


@dataclass(frozen=True, slots=True)
class MinuteDailyFeatureContext:
    """Memory-mapped ordinary day-trade features aligned to minute sessions.

    For minute session ``D``, every close-complete daily feature comes from the
    immediately preceding panel session.  The optional opening-gap feature is
    the daily contract's row ``D-1`` value ``log(open[D] / close[D-1])``.  It
    is known before any configured minute decision and never exposes D's
    high/low/close/full-session volume.
    """

    feature_names: tuple[str, ...]
    panel_features: np.ndarray
    panel_open_prices: np.ndarray
    panel_close_prices: np.ndarray
    panel_symbol_indices: np.ndarray
    selected_feature_indices: np.ndarray
    selected_output_indices: np.ndarray
    previous_session_indices: np.ndarray
    current_session_indices: np.ndarray
    opening_gap_output_index: int | None
    fingerprint: str
    benchmark_name: str
    benchmark_log_returns: np.ndarray
    benchmark_price_available: np.ndarray
    benchmark_fingerprint: str

    @property
    def num_features(self) -> int:
        return len(self.feature_names)

    def for_minute_day(self, index: int, *, lookback: int = 1) -> np.ndarray:
        day_index = int(index)
        previous = int(self.previous_session_indices[day_index])
        history = int(lookback)
        if history < 1:
            raise ValueError("minute daily-context lookback must be positive")
        first = previous - history + 1
        if first < 0:
            raise RuntimeError(
                "tw_minute daily-context panel lacks a complete causal "
                f"lookback={history} before minute day index={day_index}"
            )
        panel_rows = np.asarray(
            self.panel_features[first : previous + 1], dtype=np.float32
        )
        selected = panel_rows[:, self.panel_symbol_indices][
            :, :, self.selected_feature_indices
        ]
        output = np.zeros(
            (history, int(self.panel_symbol_indices.size), self.num_features),
            dtype=np.float32,
        )
        output[:, :, self.selected_output_indices] = selected

        gap_index = self.opening_gap_output_index
        current = int(self.current_session_indices[day_index])
        if gap_index is not None:
            completed_rows = np.arange(first, previous + 1, dtype=np.int64)
            opening_rows = completed_rows + 1
            # Every historical gap is already observed.  The final row may use
            # only the exact current session's opening quote; if the public
            # panel lacks that session it fails closed instead of reading the
            # next future row returned by searchsorted.
            opening_rows[-1] = current
            valid_rows = (opening_rows >= 0) & (
                opening_rows < int(self.panel_open_prices.shape[0])
            )
            safe_opening_rows = np.clip(
                opening_rows, 0, int(self.panel_open_prices.shape[0]) - 1
            )
            opens = np.asarray(
                self.panel_open_prices[safe_opening_rows], dtype=np.float64
            )[:, self.panel_symbol_indices]
            closes = np.asarray(
                self.panel_close_prices[completed_rows], dtype=np.float64
            )[:, self.panel_symbol_indices]
            valid = (
                valid_rows[:, None]
                & np.isfinite(opens)
                & np.isfinite(closes)
                & (opens > 0.0)
                & (closes > 0.0)
            )
            gap = np.zeros(opens.shape, dtype=np.float64)
            np.log(
                np.divide(opens, closes, out=np.ones_like(opens), where=valid),
                out=gap,
                where=valid,
            )
            # Match the daily panel's fail-closed 2x price-jump quarantine.
            valid &= np.isfinite(gap) & (np.abs(gap) <= float(np.log(2.0)))
            output[:, :, gap_index] = np.where(valid, gap, 0.0).astype(
                np.float32, copy=False
            )
        return output[0] if history == 1 else output


@dataclass(slots=True)
class MinuteDatasetIndex:
    root: Path
    manifest: dict[str, Any]
    symbols: tuple[str, ...]
    dates: np.ndarray
    partitions: dict[str, dict[str, Any]]
    short_open_mask: np.ndarray | None = None
    short_capacity_shares: np.ndarray | None = None
    short_rules_fingerprint: str | None = None
    daily_feature_context: MinuteDailyFeatureContext | None = None
    daily_context_lookback: int = 1

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
        if not (
            np.isfinite(counts).all()
            and np.isfinite(sums).all()
            and np.isfinite(sum_squares).all()
        ):
            raise RuntimeError("minute feature statistics contain non-finite values")
        mean = sums / counts
        second_moment = sum_squares / counts
        raw_variance = second_moment - mean * mean
        cancellation_tolerance = (
            64.0
            * np.finfo(np.float64).eps
            * np.maximum.reduce(
                [np.abs(second_moment), np.square(mean), np.ones_like(mean)]
            )
        )
        corrupted = raw_variance < -cancellation_tolerance
        if np.any(corrupted):
            details = {
                MINUTE_FEATURE_COLUMNS[index]: float(raw_variance[index])
                for index in np.flatnonzero(corrupted)
            }
            raise RuntimeError(
                "minute feature statistics imply negative variance; rebuild the "
                f"dataset with Float64 accumulation: {details}"
            )
        variance = np.maximum(raw_variance, 1e-12)
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
            short_open_mask=(
                None
                if self.short_open_mask is None
                else self.short_open_mask[int(index)]
            ),
            short_capacity_shares=(
                None
                if self.short_capacity_shares is None
                else self.short_capacity_shares[int(index)]
            ),
            daily_context_features=(
                None
                if self.daily_feature_context is None
                else self.daily_feature_context.for_minute_day(
                    int(index), lookback=int(self.daily_context_lookback)
                )
            ),
            benchmark_log_return=(
                float("nan")
                if self.daily_feature_context is None
                else float(self.daily_feature_context.benchmark_log_returns[int(index)])
            ),
            benchmark_price_available=(
                False
                if self.daily_feature_context is None
                else bool(
                    self.daily_feature_context.benchmark_price_available[int(index)]
                )
            ),
        )


def _resolve_panel_cache_array_root(
    meta_path: Path,
    metadata: dict[str, Any],
) -> Path:
    dates_entry = metadata.get("arrays", {}).get("dates", {})
    relative = Path(str(dates_entry.get("file", "")))
    for candidate in (meta_path.parent, *meta_path.parents):
        if relative.parts and (candidate / relative).is_file():
            return candidate
    raise RuntimeError(
        f"minute daily-context metadata references a missing dates array: {meta_path}"
    )


def _load_minute_daily_feature_context(
    meta_path: str | Path,
    *,
    minute_symbols: tuple[str, ...],
    minute_dates: np.ndarray,
    requested_feature_names: Sequence[str],
    benchmark_name: str | None = None,
) -> MinuteDailyFeatureContext:
    resolved_meta = Path(meta_path).resolve()
    if not resolved_meta.is_file():
        raise RuntimeError(
            f"tw_minute daily-context panel metadata is missing: {resolved_meta}"
        )
    metadata = _read_json(resolved_meta)
    if int(metadata.get("version", 0)) < 2:
        raise RuntimeError("tw_minute daily context requires panel cache v2 metadata")
    cache_root = _resolve_panel_cache_array_root(resolved_meta, metadata)

    def array_path(name: str) -> Path:
        entry = metadata.get("arrays", {}).get(name)
        if not isinstance(entry, dict) or not str(entry.get("file", "")):
            raise RuntimeError(
                f"tw_minute daily-context metadata lacks array {name!r}"
            )
        path = (cache_root / str(entry["file"])).resolve()
        if not path.is_file():
            raise RuntimeError(
                f"tw_minute daily-context array {name!r} is missing: {path}"
            )
        return path

    symbols_path = (cache_root / str(metadata.get("symbols_file", ""))).resolve()
    feature_names_path = (
        cache_root / str(metadata.get("feature_names_file", ""))
    ).resolve()
    if not symbols_path.is_file() or not feature_names_path.is_file():
        raise RuntimeError("tw_minute daily-context symbol/feature metadata is missing")
    panel_symbols_payload = json.loads(symbols_path.read_text(encoding="utf-8"))
    cached_feature_payload = json.loads(
        feature_names_path.read_text(encoding="utf-8")
    )
    if not isinstance(panel_symbols_payload, list) or not isinstance(
        cached_feature_payload, list
    ):
        raise RuntimeError("tw_minute daily-context metadata lists are malformed")
    panel_symbols = np.asarray(
        [str(value) for value in panel_symbols_payload], dtype=object
    )
    if panel_symbols.size == 0:
        raise RuntimeError("tw_minute daily-context panel has no symbols")
    panel_symbol_lookup = {
        symbol: index for index, symbol in enumerate(panel_symbols.tolist())
    }
    if len(panel_symbol_lookup) != int(panel_symbols.size):
        raise RuntimeError("tw_minute daily-context panel symbols are not unique")
    minute_symbol_values = np.asarray(minute_symbols, dtype=object)
    missing = [
        str(symbol)
        for symbol in minute_symbol_values.tolist()
        if str(symbol) not in panel_symbol_lookup
    ]
    if missing:
        raise RuntimeError(
            "tw_minute daily-context panel lacks minute symbols: "
            f"{missing[:20]}"
        )
    panel_symbol_indices = np.asarray(
        [panel_symbol_lookup[str(symbol)] for symbol in minute_symbol_values],
        dtype=np.int64,
    )
    resolved_benchmark_name = str(benchmark_name or "").strip()
    if not resolved_benchmark_name:
        raise RuntimeError("tw_minute daily context requires a benchmark symbol")
    if resolved_benchmark_name not in panel_symbol_lookup:
        raise RuntimeError(
            "tw_minute daily-context panel lacks configured benchmark "
            f"{resolved_benchmark_name!r}"
        )
    backend_fields = {
        field.split("=", 1)[0]: field.split("=", 1)[1]
        for field in str(metadata.get("backend_key", "")).split("|")
        if "=" in field
    }
    cached_benchmark_name = str(backend_fields.get("benchmark", "")).strip()
    if cached_benchmark_name != resolved_benchmark_name:
        raise RuntimeError(
            "tw_minute daily-context benchmark disagrees with its panel cache: "
            f"configured={resolved_benchmark_name!r} "
            f"cached={cached_benchmark_name!r}"
        )
    benchmark_symbol_index = int(panel_symbol_lookup[resolved_benchmark_name])

    cached_feature_names = tuple(str(value) for value in cached_feature_payload)
    cached_feature_lookup = {
        name: index for index, name in enumerate(cached_feature_names)
    }
    requested = tuple(str(value) for value in requested_feature_names)
    if not requested:
        raise RuntimeError("tw_minute daily context requires at least one feature")
    if len(set(requested)) != len(requested):
        raise RuntimeError("tw_minute daily-context feature list contains duplicates")
    unsupported = [
        name
        for name in requested
        if name != MINUTE_DAILY_OPEN_GAP_FEATURE and name not in cached_feature_lookup
    ]
    if unsupported:
        raise RuntimeError(
            "tw_minute daily-context panel lacks configured day-trade features: "
            f"{unsupported}"
        )
    selected_output_indices = np.asarray(
        [
            index
            for index, name in enumerate(requested)
            if name != MINUTE_DAILY_OPEN_GAP_FEATURE
        ],
        dtype=np.int64,
    )
    selected_feature_indices = np.asarray(
        [
            cached_feature_lookup[name]
            for name in requested
            if name != MINUTE_DAILY_OPEN_GAP_FEATURE
        ],
        dtype=np.int64,
    )
    opening_gap_output_index = (
        requested.index(MINUTE_DAILY_OPEN_GAP_FEATURE)
        if MINUTE_DAILY_OPEN_GAP_FEATURE in requested
        else None
    )

    panel_dates = np.load(array_path("dates"), mmap_mode="r").astype("datetime64[D]")
    if panel_dates.ndim != 1 or panel_dates.size < 2:
        raise RuntimeError("tw_minute daily-context panel dates are malformed")
    if not bool(np.all(panel_dates[1:] > panel_dates[:-1])):
        raise RuntimeError("tw_minute daily-context panel dates are not sorted/unique")
    minute_day_values = np.asarray(minute_dates, dtype="datetime64[D]")
    insertion = np.searchsorted(panel_dates, minute_day_values)
    previous_session_indices = insertion - 1
    if np.any(previous_session_indices < 0):
        raise RuntimeError(
            "tw_minute daily-context panel lacks a prior completed session"
        )
    current_session_indices = np.full(minute_day_values.shape, -1, dtype=np.int64)
    exact = insertion < panel_dates.size
    exact &= (
        panel_dates[np.clip(insertion, 0, panel_dates.size - 1)]
        == minute_day_values
    )
    current_session_indices[exact] = insertion[exact]

    panel_returns = np.load(array_path("returns_1d"), mmap_mode="r")
    panel_close_prices = np.load(array_path("close_prices"), mmap_mode="r")
    expected_panel_shape = (int(panel_dates.size), int(panel_symbols.size))
    if panel_returns.shape != expected_panel_shape:
        raise RuntimeError("tw_minute daily-context returns_1d shape is malformed")
    if panel_close_prices.shape != expected_panel_shape:
        raise RuntimeError("tw_minute daily-context close_prices shape is malformed")
    benchmark_log_returns = np.full(minute_day_values.shape, np.nan, dtype=np.float32)
    benchmark_price_available = np.zeros(minute_day_values.shape, dtype=np.bool_)
    if bool(np.any(exact)):
        exact_rows = current_session_indices[exact]
        # ``returns_1d[p]`` is the forward adjusted-close return from panel
        # row p to p+1.  Place it on the minute session where that move ends so
        # daily benchmark rows and strategy rows share the same wall-clock
        # date.  The split runner later zeroes its first row so a standalone
        # split begins exactly at its first close rather than the prior split.
        ending_return_rows = previous_session_indices[exact]
        benchmark_log_returns[exact] = np.asarray(
            panel_returns[ending_return_rows, benchmark_symbol_index],
            dtype=np.float32,
        )
        benchmark_closes = np.asarray(
            panel_close_prices[exact_rows, benchmark_symbol_index], dtype=np.float64
        )
        benchmark_price_available[exact] = np.isfinite(benchmark_closes) & (
            benchmark_closes > 0.0
        )

    arrays = metadata.get("arrays", {})
    digest_payload = {
        "contract": MINUTE_DAILY_CONTEXT_CONTRACT,
        "metadata_sha256": _sha256(resolved_meta),
        "features_sha256": arrays.get("features", {}).get(
            "content_fingerprint", {}
        ).get("sha256"),
        "open_prices_sha256": arrays.get("open_prices", {}).get(
            "content_fingerprint", {}
        ).get("sha256"),
        "close_prices_sha256": arrays.get("close_prices", {}).get(
            "content_fingerprint", {}
        ).get("sha256"),
        "feature_names": list(requested),
        "minute_symbols": list(minute_symbols),
        "minute_dates": [str(value) for value in minute_day_values],
    }
    if not all(
        isinstance(digest_payload[name], str) and digest_payload[name]
        for name in (
            "features_sha256",
            "open_prices_sha256",
            "close_prices_sha256",
        )
    ):
        raise RuntimeError(
            "tw_minute daily-context cache lacks immutable content fingerprints"
        )
    fingerprint = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    benchmark_digest_payload = {
        "contract": "configured_symbol_adjusted_close_buy_hold_first_to_last_v1",
        "benchmark_name": resolved_benchmark_name,
        "returns_1d_sha256": arrays.get("returns_1d", {}).get(
            "content_fingerprint", {}
        ).get("sha256"),
        "close_prices_sha256": arrays.get("close_prices", {}).get(
            "content_fingerprint", {}
        ).get("sha256"),
        "minute_dates": [str(value) for value in minute_day_values],
    }
    if not all(
        isinstance(benchmark_digest_payload[name], str)
        and benchmark_digest_payload[name]
        for name in ("returns_1d_sha256", "close_prices_sha256")
    ):
        raise RuntimeError(
            "tw_minute daily-context cache lacks benchmark content fingerprints"
        )
    benchmark_fingerprint = hashlib.sha256(
        json.dumps(
            benchmark_digest_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return MinuteDailyFeatureContext(
        feature_names=requested,
        panel_features=np.load(array_path("features"), mmap_mode="r"),
        panel_open_prices=np.load(array_path("open_prices"), mmap_mode="r"),
        panel_close_prices=np.load(array_path("close_prices"), mmap_mode="r"),
        panel_symbol_indices=panel_symbol_indices.astype(np.int64, copy=False),
        selected_feature_indices=selected_feature_indices,
        selected_output_indices=selected_output_indices,
        previous_session_indices=previous_session_indices.astype(np.int64, copy=False),
        current_session_indices=current_session_indices,
        opening_gap_output_index=opening_gap_output_index,
        fingerprint=fingerprint,
        benchmark_name=resolved_benchmark_name,
        benchmark_log_returns=benchmark_log_returns,
        benchmark_price_available=benchmark_price_available,
        benchmark_fingerprint=benchmark_fingerprint,
    )


def _load_minute_short_rules(
    path: str | Path,
    *,
    symbols: tuple[str, ...],
    dates: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, str]:
    """Load exact-session sell-first rules into compact day-level arrays.

    The minute parquet intentionally does not repeat a daily rule on every one
    of its roughly 270 rows.  Eligibility is read for the exact session.  The
    official margin-short evidence and capacity are post-close observations,
    so row ``t`` becomes usable only on the next observed minute session.
    Missing observations fail closed and are never forward-filled.
    """

    resolved = Path(path).resolve()
    if not resolved.is_file():
        raise RuntimeError(f"tw_minute short-rule parquet is missing: {resolved}")
    required = (
        "date",
        "symbol",
        "_twpub_day_trade_eligible",
        "_twpub_day_trade_short_open",
        "_twpub_margin_short_evidence_next_session",
        "_twpub_short_capacity_shares_next_session",
    )
    frame = (
        pl.scan_parquet(resolved)
        .select(required)
        .filter(
            pl.col("date").is_between(
                pl.lit(dates[0].astype("datetime64[D]").astype(object)),
                pl.lit(dates[-1].astype("datetime64[D]").astype(object)),
                closed="both",
            )
            & pl.col("symbol").is_in(pl.Series(symbols).implode())
        )
        .collect()
    )
    if frame.is_empty():
        raise RuntimeError("tw_minute short-rule parquet has no overlapping rows")

    date_values = frame["date"].to_numpy().astype("datetime64[D]")
    symbol_values = frame["symbol"].cast(pl.String).to_numpy()
    symbol_array = np.asarray(symbols, dtype=object)
    date_indices = np.searchsorted(dates, date_values)
    symbol_indices = np.searchsorted(symbol_array, symbol_values)
    aligned = (
        (date_indices >= 0)
        & (date_indices < dates.size)
        & (symbol_indices >= 0)
        & (symbol_indices < symbol_array.size)
    )
    aligned &= dates[np.clip(date_indices, 0, dates.size - 1)] == date_values
    aligned &= (
        symbol_array[np.clip(symbol_indices, 0, symbol_array.size - 1)]
        == symbol_values
    )
    date_indices = date_indices[aligned]
    symbol_indices = symbol_indices[aligned]
    flat = date_indices * symbol_array.size + symbol_indices
    if np.unique(flat).size != flat.size:
        raise RuntimeError("tw_minute short-rule parquet has duplicate date-symbol rows")

    shape = (int(dates.size), int(symbol_array.size))
    eligible = np.zeros(shape, dtype=np.bool_)
    sell_first = np.zeros(shape, dtype=np.bool_)
    evidence = np.zeros(shape, dtype=np.bool_)
    source_capacity = np.zeros(shape, dtype=np.int64)

    def numeric(name: str) -> np.ndarray:
        return (
            frame[name]
            .cast(pl.Float64)
            .fill_null(float("nan"))
            .to_numpy()[aligned]
        )

    eligible_values = numeric("_twpub_day_trade_eligible")
    direction_values = numeric("_twpub_day_trade_short_open")
    evidence_values = numeric("_twpub_margin_short_evidence_next_session")
    capacity_values = numeric("_twpub_short_capacity_shares_next_session")
    eligible[date_indices, symbol_indices] = (
        np.isfinite(eligible_values) & (eligible_values > 0.0)
    )
    sell_first[date_indices, symbol_indices] = (
        np.isfinite(direction_values) & (direction_values > 0.0)
    )
    valid_capacity = (
        np.isfinite(evidence_values)
        & (evidence_values > 0.0)
        & np.isfinite(capacity_values)
        & (capacity_values > 0.0)
        & (capacity_values == np.floor(capacity_values))
        & (capacity_values <= float(np.iinfo(np.int64).max))
    )
    evidence[date_indices, symbol_indices] = valid_capacity
    source_capacity[date_indices[valid_capacity], symbol_indices[valid_capacity]] = (
        capacity_values[valid_capacity].astype(np.int64, copy=False)
    )

    next_session_evidence = np.zeros_like(evidence)
    next_session_capacity = np.zeros_like(source_capacity)
    if dates.size > 1:
        next_session_evidence[1:] = evidence[:-1]
        next_session_capacity[1:] = source_capacity[:-1]
    short_open = eligible & sell_first & next_session_evidence
    short_capacity = np.where(
        short_open,
        next_session_capacity,
        np.zeros_like(next_session_capacity),
    )
    if not bool(short_open.any()):
        raise RuntimeError(
            "tw_minute short rules contain no point-in-time eligible capacity"
        )
    digest = hashlib.sha256()
    digest.update(np.ascontiguousarray(short_open).view(np.uint8))
    digest.update(np.ascontiguousarray(short_capacity).view(np.uint8))
    return short_open, short_capacity, digest.hexdigest()


def load_minute_dataset_index(
    root: str | Path,
    *,
    require_research_ready: bool = True,
    verify_partition_sha256: bool = True,
    progress: Callable[[str], None] | None = None,
    short_rule_path: str | Path | None = None,
    require_short_rules: bool = False,
    daily_context_panel_meta: str | Path | None = None,
    daily_context_feature_names: Sequence[str] = (),
    daily_context_lookback: int = 1,
    benchmark_name: str | None = None,
) -> MinuteDatasetIndex:
    resolved_root = Path(root).resolve()
    manifest_path = resolved_root / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"minute dataset manifest is missing: {manifest_path}")
    manifest = _read_json(manifest_path)
    if manifest.get("schema_version") != MINUTE_DATASET_SCHEMA_VERSION:
        raise RuntimeError(
            "minute dataset schema is stale; upgrade the schema-4 receipts with "
            "scripts/upgrade_tw_minute_developing_candles.py"
        )
    if (
        manifest.get("feature_statistics_contract")
        != MINUTE_FEATURE_STATISTICS_CONTRACT
    ):
        raise RuntimeError(
            "minute dataset feature statistics are stale; rebuild with "
            "Float64 accumulation"
        )
    if (
        manifest.get("developing_candle_contract")
        != MINUTE_DEVELOPING_CANDLE_CONTRACT
    ):
        raise RuntimeError(
            "minute dataset developing-candle contract is incompatible"
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
    partition_paths: list[Path] = []
    partition_contracts: list[dict[str, int | str]] = []
    for summary in partition_summaries:
        path = _partition_path(resolved_root, summary)
        if not path.is_file():
            raise RuntimeError(f"minute partition is missing: {path}")
        contract = _partition_stat_contract(path)
        contract["expected_sha256"] = str(summary.get("output_sha256", ""))
        partition_paths.append(path)
        partition_contracts.append(contract)
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    verification_cached = bool(
        verify_partition_sha256
        and _load_partition_verification_receipt(
            resolved_root,
            manifest_sha256=manifest_sha256,
            contracts=partition_contracts,
        )
    )
    if verification_cached and progress is not None:
        progress(
            "[tw-minute data] reused SHA256 receipt for "
            f"{len(partition_summaries)} unchanged partitions"
        )
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
        path = partition_paths[position - 1]
        if verify_partition_sha256 and not verification_cached:
            expected = str(summary.get("output_sha256", ""))
            actual = _sha256(path)
            if not expected or actual != expected:
                raise RuntimeError(
                    f"minute partition fingerprint mismatch: {path} "
                    f"expected={expected} actual={actual}"
                )
        partitions[key] = summary
        if progress is not None and not verification_cached and (
            position == 1
            or position % 100 == 0
            or position == len(partition_summaries)
        ):
            progress(
                "[tw-minute data] verified partitions="
                f"{position}/{len(partition_summaries)} last_date={key}"
            )
    if verify_partition_sha256 and not verification_cached:
        _write_partition_verification_receipt(
            resolved_root,
            manifest_sha256=manifest_sha256,
            contracts=partition_contracts,
        )
    if set(partitions) != set(date_strings):
        raise RuntimeError("minute manifest date list and partition list disagree")
    dates = np.asarray(date_strings, dtype="datetime64[D]")
    short_open_mask: np.ndarray | None = None
    short_capacity_shares: np.ndarray | None = None
    short_rules_fingerprint: str | None = None
    if short_rule_path is not None:
        (
            short_open_mask,
            short_capacity_shares,
            short_rules_fingerprint,
        ) = _load_minute_short_rules(
            short_rule_path,
            symbols=symbols,
            dates=dates,
        )
    elif require_short_rules:
        raise RuntimeError(
            "tw_minute long/short requires a point-in-time short-rule parquet"
        )
    resolved_daily_context_lookback = int(daily_context_lookback)
    if resolved_daily_context_lookback < 1:
        raise ValueError("tw_minute daily-context lookback must be positive")
    daily_feature_context = None
    if daily_context_panel_meta is not None:
        daily_feature_context = _load_minute_daily_feature_context(
            daily_context_panel_meta,
            minute_symbols=symbols,
            minute_dates=dates,
            requested_feature_names=daily_context_feature_names,
            benchmark_name=benchmark_name,
        )
    elif daily_context_feature_names:
        raise RuntimeError(
            "tw_minute daily-context features require "
            "data.minute_daily_context_panel_meta"
        )
    return MinuteDatasetIndex(
        root=resolved_root,
        manifest=manifest,
        symbols=symbols,
        dates=dates,
        partitions=partitions,
        short_open_mask=short_open_mask,
        short_capacity_shares=short_capacity_shares,
        short_rules_fingerprint=short_rules_fingerprint,
        daily_feature_context=daily_feature_context,
        daily_context_lookback=resolved_daily_context_lookback,
    )


__all__ = [
    "MINUTE_FEATURE_COLUMNS",
    "MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS",
    "MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS",
    "MINUTE_DEVELOPING_CANDLE_CONTRACT",
    "MINUTE_DAILY_CONTEXT_CONTRACT",
    "MINUTE_DAILY_OPEN_GAP_FEATURE",
    "MINUTE_SESSION_BARS",
    "MinuteDailyFeatureContext",
    "MinuteDatasetIndex",
    "MinuteDayPanel",
    "MinuteFeatureNormalizer",
    "add_developing_daily_candle_features",
    "load_minute_dataset_index",
    "minute_static_daily_context_feature_names",
    "summarize_minute_sessions_for_next_day",
]
