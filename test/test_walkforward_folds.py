from __future__ import annotations

import numpy as np

from stockagent.data.walkforward import build_expanding_year_folds


def _year_end_dates(start_year: int, end_year: int) -> np.ndarray:
    return np.asarray([f"{year}-12-31" for year in range(start_year, end_year + 1)], dtype="datetime64[D]")


def test_expanding_folds_require_future_test_year_by_default() -> None:
    folds = build_expanding_year_folds(
        _year_end_dates(2020, 2024),
        min_train_years=1,
        val_years=1,
        require_future_test_year=True,
    )

    assert [fold.val_years for fold in folds] == [[2021], [2022], [2023]]
    assert folds[-1].test_years == [2024]


def test_expanding_folds_can_add_final_val_test_overlap_fold() -> None:
    folds = build_expanding_year_folds(
        _year_end_dates(2020, 2024),
        min_train_years=1,
        val_years=1,
        require_future_test_year=False,
    )

    assert [fold.val_years for fold in folds] == [[2021], [2022], [2023], [2024]]
    assert folds[-1].train_years == [2020, 2021, 2022, 2023]
    assert folds[-1].val_years == [2024]
    assert folds[-1].test_years == [2024]
    assert np.array_equal(folds[-1].val_indices, folds[-1].test_indices)


def test_expanding_folds_overlap_uses_full_validation_window_when_val_years_gt_one() -> None:
    folds = build_expanding_year_folds(
        _year_end_dates(2020, 2025),
        min_train_years=1,
        val_years=2,
        require_future_test_year=False,
    )

    assert folds[-1].val_years == [2024, 2025]
    assert folds[-1].test_years == [2024, 2025]
    assert np.array_equal(folds[-1].val_indices, folds[-1].test_indices)
