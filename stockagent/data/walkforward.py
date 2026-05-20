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
    val_years: int,
    require_future_test_year: bool,
) -> list[WalkForwardFold]:
    years = pd.to_datetime(dates).year.to_numpy()
    unique_years = sorted(pd.unique(years).tolist())
    folds: list[WalkForwardFold] = []

    max_start = len(unique_years) - val_years
    for train_end_idx in range(min_train_years, max_start):
        train_years = unique_years[:train_end_idx]
        val_year_slice = unique_years[train_end_idx : train_end_idx + val_years]
        test_years = unique_years[train_end_idx + val_years :]

        if require_future_test_year and not test_years:
            continue

        train_indices = np.flatnonzero(np.isin(years, train_years))
        val_indices = np.flatnonzero(np.isin(years, val_year_slice))
        test_indices = np.flatnonzero(np.isin(years, test_years))
        if train_indices.size == 0 or val_indices.size == 0 or test_indices.size == 0:
            continue

        folds.append(
            WalkForwardFold(
                fold_id=len(folds) + 1,
                train_indices=train_indices,
                val_indices=val_indices,
                test_indices=test_indices,
                train_years=train_years,
                val_years=val_year_slice,
                test_years=test_years,
            )
        )

    if not folds:
        raise ValueError("No valid walk-forward folds could be constructed")
    return folds
