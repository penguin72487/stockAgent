from __future__ import annotations

from dataclasses import dataclass

import numpy as np


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
    years = np.asarray(dates, dtype="datetime64[Y]").astype(np.int64) + 1970
    panel_years = sorted(np.unique(years).astype(int).tolist())
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
