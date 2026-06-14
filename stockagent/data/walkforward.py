from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(slots=True)
class WalkForwardFold:
    fold_id: int
    train_indices: np.ndarray
    val_indices: np.ndarray
    test_indices: np.ndarray
    train_years: list[int]
    val_years: list[int]
    test_years: list[int]


def build_expanding_year_folds(
    dates: np.ndarray,
    min_train_years: int,
    val_years: int = 1,
    require_future_test_year: bool = True,
) -> list[WalkForwardFold]:
    """Build expanding-window folds with fixed 1-year validation.

    For each valid train endpoint i (0-based index into unique_years):
    - train = unique_years[:i]          (expanding, at least min_train_years)
    - val   = [unique_years[i]]         (fixed 1 year)
    - test  = unique_years[i+1:]        (all remaining years)

    i ranges from min_train_years to n-2, so there is always at least 1 test year.
    val_years and require_future_test_year are kept for API compatibility.
    """
    _ = val_years
    _ = require_future_test_year

    years = np.asarray(dates, dtype="datetime64[Y]").astype(np.int64) + 1970
    unique_years = sorted(np.unique(years).astype(int).tolist())
    folds: list[WalkForwardFold] = []

    total_years = len(unique_years)
    # i is the index of the val year; train = years[:i], val = [years[i]], test = years[i+1:]
    # need at least min_train_years before val, and at least 1 year after val
    for i in range(min_train_years, total_years - 1):
        train_year_slice = unique_years[:i]
        val_year_slice = [unique_years[i]]
        test_year_slice = unique_years[i + 1:]

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
