from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import Any, Mapping

import numpy as np


def normalize_year_boundary_mode(value: str) -> str:
    mode = str(value).strip().lower().replace('-', '_')
    if mode not in {'calendar', 'lookback_shifted'}:
        raise ValueError("year_boundary_mode must be 'calendar' or 'lookback_shifted'")
    return mode


def year_boundary_offset_sessions(config: Any) -> int:
    mode = normalize_year_boundary_mode(getattr(config.walk_forward, 'year_boundary_mode', 'calendar'))
    offset = int(config.training.lookback) if mode == 'lookback_shifted' else 0
    if offset < 0 or (mode == 'lookback_shifted' and offset == 0):
        raise ValueError('lookback-shifted year boundary requires positive lookback')
    if offset and normalize_lookback_context(config.walk_forward.lookback_context) != 'panel_history':
        raise ValueError('lookback_shifted requires panel_history; split_only would discard the warmup twice')
    return offset


def year_period_contract(dates: np.ndarray, offset_sessions: int = 0) -> dict[str, Any]:
    """Own [first session of Y + L, first session of Y+1 + L).

    L is a count of observed sessions, not calendar days. The first L sessions
    of a new calendar year remain owned by the preceding annual period. A
    partial final calendar cannot fabricate a future boundary.
    """
    dates = np.asarray(dates, dtype='datetime64[D]')
    if dates.ndim != 1 or not dates.size or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError('annual boundary calendar must contain unique increasing finite dates')
    if isinstance(offset_sessions, bool) or int(offset_sessions) != offset_sessions or offset_sessions < 0:
        raise ValueError('year boundary offset must be a nonnegative integer session count')
    years = dates.astype('datetime64[Y]').astype(np.int64) + 1970
    labels, first = np.unique(years, return_index=True)
    boundaries = [dict(year=int(year), start_date=str(dates[index + int(offset_sessions)]))
                  for year, index in zip(labels, first) if index + int(offset_sessions) < len(dates)]
    return dict(schema_version=1, mode='lookback_shifted' if offset_sessions else 'calendar',
                offset_sessions=int(offset_sessions), offset_unit='observed_trading_sessions',
                interval='inclusive_start_exclusive_next_start', boundaries=boundaries,
                panel_start=str(dates[0]), panel_end=str(dates[-1]), panel_sessions=len(dates),
                panel_calendar_sha256=hashlib.sha256(dates.astype(np.int64).astype('<i8').tobytes()).hexdigest())


def period_labels_from_contract(dates: np.ndarray, contract: Mapping[str, Any]) -> np.ndarray:
    """Label a subset using the full calendar's boundaries; never shift it twice."""
    dates = np.asarray(dates, dtype='datetime64[D]')
    if dates.ndim != 1 or np.isnat(dates).any():
        raise ValueError('period reporting dates must be finite and one-dimensional')
    if not dates.size:
        return np.empty(0, dtype=np.int64)
    if np.any(dates < np.datetime64(contract['panel_start'])) or np.any(dates > np.datetime64(contract['panel_end'])):
        raise ValueError('report dates are outside the authoritative annual boundary calendar')
    boundaries = contract['boundaries']
    if not boundaries:
        raise ValueError('calendar has insufficient sessions for an annual boundary')
    starts = np.asarray([row['start_date'] for row in boundaries], dtype='datetime64[D]')
    labels = np.asarray([row['year'] for row in boundaries], dtype=np.int64)
    if np.any(starts[1:] <= starts[:-1]) or np.any(labels[1:] <= labels[:-1]):
        raise ValueError('annual period starts must be strictly increasing')
    positions = np.searchsorted(starts, dates, side='right') - 1
    return np.where(positions < 0, labels[0] - 1, labels[np.maximum(positions, 0)])


def annual_period_years(dates: np.ndarray, offset_sessions: int = 0) -> np.ndarray:
    if not offset_sessions:
        return np.asarray(dates, dtype='datetime64[Y]').astype(np.int64) + 1970
    return period_labels_from_contract(dates, year_period_contract(dates, offset_sessions))


def normalize_lookback_context(value: str) -> str:
    normalized = str(value).strip().lower().replace("-", "_")
    aliases = {
        "split": "split_only",
        "split_only": "split_only",
        "fold": "split_only",
        "fold_only": "split_only",
        "panel": "panel_history",
        "history": "panel_history",
        "panel_history": "panel_history",
        "cross_split": "panel_history",
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ValueError(
            "lookback_context must be 'split_only' or 'panel_history', got "
            f"{value!r}"
        ) from exc


@dataclass(slots=True)
class WalkForwardFold:
    fold_id: int
    train_indices: np.ndarray
    val_indices: np.ndarray
    test_indices: np.ndarray
    train_years: list[int]
    val_years: list[int]
    test_years: list[int]


def validate_walk_forward_year_contract(
    dates: np.ndarray,
    *,
    expected_first_year: int | None = None,
    require_contiguous_years: bool = False,
) -> list[int]:
    years = sorted(
        np.unique(np.asarray(dates, dtype="datetime64[Y]").astype(np.int64) + 1970)
        .astype(int)
        .tolist()
    )
    if not years:
        raise ValueError("walk-forward dates are empty")
    if expected_first_year is not None and years[0] != int(expected_first_year):
        raise ValueError(
            "walk-forward first-year contract failed: "
            f"expected={int(expected_first_year)}, actual={years[0]}. "
            "Backfill the missing source years; do not silently renumber folds."
        )
    if require_contiguous_years:
        missing_years = sorted(set(range(years[0], years[-1] + 1)) - set(years))
        if missing_years:
            raise ValueError(
                "walk-forward year-continuity contract failed: "
                f"missing_years={missing_years}. Backfill the official source before training."
            )
    return years


def build_expanding_year_folds(
    dates: np.ndarray,
    min_train_years: int,
    val_years: int = 1,
    require_future_test_year: bool = True,
    split_start_year: int | None = None,
    year_boundary_offset_sessions: int = 0,
) -> list[WalkForwardFold]:
    """Build expanding-window folds.

    For each valid validation start i (0-based index into unique_years):
    - train = unique_years[:i]                         (expanding)
    - val   = unique_years[i:i + val_years]            (fixed window)
    - test  = unique_years[i + val_years:]             (all future years)

    When require_future_test_year is false, one experimental final fold is also
    allowed with no future test year. In that final fold, the validation window
    is reused as the test window. This intentionally overlaps val/test and is
    useful only for latest-year experiments, not unbiased model selection.

    ``split_start_year`` removes older panel years from target ownership without
    removing their rows from the panel. They can therefore serve as causal
    feature context without renumbering the requested folds.
    """
    years = annual_period_years(dates, year_boundary_offset_sessions)
    calendar_years = set((np.asarray(dates, dtype='datetime64[Y]').astype(np.int64) + 1970).tolist())
    panel_years = sorted(set(years.astype(int).tolist()) & calendar_years)
    if split_start_year is None:
        unique_years = panel_years
    else:
        requested_start = int(split_start_year)
        unique_years = [year for year in panel_years if year >= requested_start]
        if not unique_years or unique_years[0] != requested_start:
            raise ValueError(
                "walk-forward split_start_year is absent from panel dates: "
                f"requested={requested_start}, panel_years={panel_years[:3]}...{panel_years[-3:]}"
            )
    folds: list[WalkForwardFold] = []

    total_years = len(unique_years)
    val_year_count = max(1, int(val_years))
    last_start_exclusive = total_years - val_year_count
    if not require_future_test_year:
        last_start_exclusive += 1

    for i in range(int(min_train_years), last_start_exclusive):
        train_year_slice = unique_years[:i]
        val_year_slice = unique_years[i : i + val_year_count]
        test_year_slice = unique_years[i + val_year_count :]
        if not test_year_slice and not require_future_test_year:
            test_year_slice = list(val_year_slice)

        train_indices = np.flatnonzero(np.isin(years, train_year_slice))
        val_indices = np.flatnonzero(np.isin(years, val_year_slice))
        test_indices = np.flatnonzero(np.isin(years, test_year_slice))
        if train_indices.size == 0 or val_indices.size == 0 or test_indices.size == 0:
            continue

        folds.append(
            WalkForwardFold(
                fold_id=len(folds) + 1,
                train_indices=train_indices,
                val_indices=val_indices,
                test_indices=test_indices,
                train_years=train_year_slice,
                val_years=val_year_slice,
                test_years=test_year_slice,
            )
        )
    if not folds:
        raise ValueError("No valid walk-forward folds could be constructed")
    return folds


def build_checkpoint_inference_fold(
    dates: np.ndarray,
    checkpoint: Mapping[str, Any],
    *,
    year_boundary_offset_sessions: int | None = None,
) -> WalkForwardFold:
    """Restore a deployment fold without renumbering its checkpoint contract."""

    required = ("fold_id", "train_years", "val_years", "test_years")
    missing = [name for name in required if checkpoint.get(name) is None]
    if missing:
        raise ValueError(
            "checkpoint-defined inference fold is missing metadata: "
            + ", ".join(missing)
        )
    fold_id = int(checkpoint["fold_id"])
    train_years = [int(year) for year in checkpoint["train_years"]]
    val_years = [int(year) for year in checkpoint["val_years"]]
    test_years = [int(year) for year in checkpoint["test_years"]]
    if fold_id < 1 or not train_years or not val_years or not test_years:
        raise ValueError("checkpoint-defined inference fold has invalid year metadata")

    saved_contract = checkpoint.get('experiment_manifest', {}).get('contracts', {}).get('walk_forward', {})
    saved_offset = int(saved_contract.get('year_boundary_offset_sessions', 0))
    if year_boundary_offset_sessions is not None and int(year_boundary_offset_sessions) != saved_offset:
        raise ValueError('checkpoint annual boundary differs from the requested session offset')
    years = annual_period_years(dates, saved_offset)
    available = set(np.unique(years).astype(int).tolist())
    missing_val = sorted(set(val_years) - available)
    missing_test = sorted(set(test_years) - available)
    if missing_val or missing_test:
        raise ValueError(
            "checkpoint-defined inference fold is unavailable in the current panel: "
            f"missing_val_years={missing_val} missing_test_years={missing_test}"
        )

    train_indices = np.flatnonzero(np.isin(years, train_years))
    val_indices = np.flatnonzero(np.isin(years, val_years))
    test_indices = np.flatnonzero(np.isin(years, test_years))
    if val_indices.size == 0 or test_indices.size == 0:
        raise ValueError("checkpoint-defined inference fold has empty validation/test rows")
    return WalkForwardFold(
        fold_id=fold_id,
        train_indices=train_indices,
        val_indices=val_indices,
        test_indices=test_indices,
        train_years=train_years,
        val_years=val_years,
        test_years=test_years,
    )
