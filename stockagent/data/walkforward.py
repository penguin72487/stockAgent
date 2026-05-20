from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


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
) -> list[WalkForwardFold]:
    """Build non-overlapping folds by enumerating train/val split points.

    Given unique years [1..n], enumerate:
    - train = [1..i]
    - val   = [i+1..j]
    - test  = [j+1..n]
    where i in [min_train_years .. n-2], j in [i+1 .. n-1].
    """
    years = pd.to_datetime(dates).year.to_numpy()
    unique_years = sorted(pd.unique(years).tolist())
    folds: list[WalkForwardFold] = []

    total_years = len(unique_years)
    min_i = max(1, min_train_years)

    for i in range(min_i, total_years - 1):
        for j in range(i + 1, total_years):
            train_year_slice = unique_years[:i]
            val_year_slice = unique_years[i:j]
            test_year_slice = unique_years[j:]

            if not test_year_slice:
                continue

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
