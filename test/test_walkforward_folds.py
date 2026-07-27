from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.data.walkforward import (
    WalkForwardFold,
    build_checkpoint_inference_fold,
    build_expanding_year_folds,
    validate_walk_forward_year_contract,
)
from stockagent.training.execution_coverage import (
    validate_training_execution_coverage,
)


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


def test_split_start_year_keeps_older_panel_years_context_only() -> None:
    dates = _year_end_dates(2012, 2018)
    folds = build_expanding_year_folds(
        dates,
        min_train_years=1,
        val_years=1,
        require_future_test_year=True,
        split_start_year=2014,
    )

    assert folds[0].train_years == [2014]
    assert folds[0].val_years == [2015]
    assert folds[0].test_years == [2016, 2017, 2018]
    assert set(folds[0].train_indices.tolist()) == {2}
    owned = np.concatenate(
        [folds[0].train_indices, folds[0].val_indices, folds[0].test_indices]
    )
    assert not np.isin([0, 1], owned).any()


def test_split_start_year_must_exist_in_panel() -> None:
    with pytest.raises(ValueError, match="split_start_year is absent"):
        build_expanding_year_folds(
            _year_end_dates(2012, 2018),
            min_train_years=1,
            split_start_year=2011,
        )


def test_walk_forward_year_contract_rejects_first_year_shift() -> None:
    with pytest.raises(ValueError, match="expected=2000, actual=2004"):
        validate_walk_forward_year_contract(
            _year_end_dates(2004, 2026),
            expected_first_year=2000,
            require_contiguous_years=True,
        )


def test_walk_forward_year_contract_rejects_missing_whole_year() -> None:
    dates = np.asarray(["2000-12-31", "2002-12-31"], dtype="datetime64[D]")

    with pytest.raises(ValueError, match=r"missing_years=\[2001\]"):
        validate_walk_forward_year_contract(
            dates,
            expected_first_year=2000,
            require_contiguous_years=True,
        )


def test_checkpoint_inference_fold_preserves_saved_id_and_year_contract() -> None:
    dates = np.asarray(
        ["2004-01-02", "2024-12-31", "2025-01-02", "2026-01-02"],
        dtype="datetime64[D]",
    )
    checkpoint = {
        "fold_id": 25,
        "train_years": list(range(2000, 2025)),
        "val_years": [2025],
        "test_years": [2026],
    }

    fold = build_checkpoint_inference_fold(dates, checkpoint)

    assert fold.fold_id == 25
    assert fold.train_years == list(range(2000, 2025))
    assert fold.val_years == [2025]
    assert fold.test_years == [2026]
    np.testing.assert_array_equal(fold.train_indices, [0, 1])
    np.testing.assert_array_equal(fold.val_indices, [2])
    np.testing.assert_array_equal(fold.test_indices, [3])


def test_checkpoint_inference_fold_requires_saved_test_year() -> None:
    dates = np.asarray(["2024-01-02", "2025-01-02"], dtype="datetime64[D]")
    checkpoint = {
        "fold_id": 25,
        "train_years": list(range(2000, 2025)),
        "val_years": [2025],
        "test_years": [2026],
    }

    with pytest.raises(ValueError, match=r"missing_test_years=\[2026\]"):
        build_checkpoint_inference_fold(dates, checkpoint)


def _day_trade_panel(actionable: bool) -> SimpleNamespace:
    shape = (8, 2)
    eligible = np.zeros(shape, dtype=bool)
    if actionable:
        eligible[:, 0] = True
    return SimpleNamespace(
        dates=np.asarray(
            [
                "2020-01-02",
                "2020-01-03",
                "2020-01-06",
                "2020-01-07",
                "2021-01-04",
                "2021-01-05",
                "2021-01-06",
                "2021-01-07",
            ],
            dtype="datetime64[D]",
        ),
        num_dates=shape[0],
        tradable_mask=np.ones(shape, dtype=bool),
        intraday_returns=np.full(shape, 0.01, dtype=np.float32),
        day_trade_eligible_mask=eligible,
        day_trade_can_buy_open_mask=np.ones(shape, dtype=bool),
        day_trade_can_sell_open_mask=np.ones(shape, dtype=bool),
        day_trade_can_short_open_mask=np.ones(shape, dtype=bool),
        can_buy_mask=np.ones(shape, dtype=bool),
        can_sell_mask=np.ones(shape, dtype=bool),
        force_exit_mask=np.zeros(shape, dtype=bool),
    )


def _single_day_trade_fold() -> WalkForwardFold:
    return WalkForwardFold(
        fold_id=1,
        train_indices=np.arange(0, 4, dtype=np.int64),
        val_indices=np.arange(4, 8, dtype=np.int64),
        test_indices=np.arange(4, 8, dtype=np.int64),
        train_years=[2020],
        val_years=[2021],
        test_years=[2021],
    )


def test_day_trade_execution_coverage_rejects_constant_loss_fold() -> None:
    with pytest.raises(
        ValueError,
        match=r"zero executable round trips.*gradients would be exactly zero",
    ):
        validate_training_execution_coverage(
            _day_trade_panel(actionable=False),
            [_single_day_trade_fold()],
            execution_mode="tw_day_trade",
            long_only=False,
            lookback=1,
        )


def test_day_trade_execution_coverage_accepts_supported_fold() -> None:
    coverage = validate_training_execution_coverage(
        _day_trade_panel(actionable=True),
        [_single_day_trade_fold()],
        execution_mode="tw_day_trade",
        long_only=False,
        lookback=1,
    )

    assert coverage is not None
    assert str(coverage.first_actionable_date) == "2020-01-02"
    assert int(coverage.actionable_cells_by_row.sum()) == 8
