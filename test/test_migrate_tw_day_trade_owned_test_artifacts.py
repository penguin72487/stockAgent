from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from scripts.migrate_tw_day_trade_owned_test_artifacts import migrate_run
from stockagent.backtest.simulator import BacktestResult
from stockagent.training.trainer import (
    _load_backtest_artifact,
    _save_backtest_artifact,
    _save_deployment_test_artifacts,
)


def _result(values: list[float]) -> BacktestResult:
    rows = len(values)
    return BacktestResult(
        strategy_returns=np.asarray(values, dtype=np.float64),
        benchmark_returns=np.zeros(rows, dtype=np.float64),
        turnovers=np.full(rows, 0.5, dtype=np.float64),
        weights_history=np.ones((rows, 1), dtype=np.float64),
        requested_weights_history=np.ones((rows, 1), dtype=np.float64),
    )


def test_migration_repairs_final_owner_backs_up_and_is_idempotent(
    tmp_path: Path,
) -> None:
    rows = [
        {
            "fold_id": 1,
            "train_years": [2020],
            "val_years": [2021],
            "test_years": [2022, 2023],
        },
        {
            "fold_id": 2,
            "train_years": [2020, 2021],
            "val_years": [2022],
            "test_years": [2023],
        },
        {
            "fold_id": 3,
            "train_years": [2020, 2021, 2022],
            "val_years": [2023],
            "test_years": [2023],
        },
    ]
    (tmp_path / "summary.json").write_text(json.dumps(rows))
    (tmp_path / "run_manifest.json").write_text(
        json.dumps(
            {
                "configuration": {
                    "walk_forward": {"lookback_context": "split_only"},
                    "training": {"lookback": 2},
                    "trading": {"execution_mode": "naive"},
                }
            }
        )
    )
    full_dates = {
        1: np.asarray(["2022-01-03", "2022-01-04", "2023-01-03", "2023-01-04"], dtype="datetime64[D]"),
        2: np.asarray(["2023-01-03", "2023-01-04"], dtype="datetime64[D]"),
        3: np.asarray(["2023-01-03", "2023-01-04"], dtype="datetime64[D]"),
    }
    for fold_id in (1, 2, 3):
        fold_dir = tmp_path / f"fold_{fold_id:02d}"
        fold_dir.mkdir()
        full = _result([0.001] * len(full_dates[fold_id]))
        _save_backtest_artifact(
            fold_dir / "test_backtest.npz",
            full,
            full_dates[fold_id],
        )
        # Fold 1 is already the correct owned prefix. Fold 2 is missing and
        # the same-year experimental Fold 3 incorrectly owns the whole span.
        old_rows = 2 if fold_id == 1 else (0 if fold_id == 2 else len(full_dates[fold_id]))
        _save_deployment_test_artifacts(
            fold_dir,
            _result(([0.9] if fold_id == 3 else [0.001]) * old_rows),
            full_dates[fold_id][:old_rows],
        )

    first = migrate_run(tmp_path, write_plots=False)

    assert first["status"] == "changed"
    assert first["changed_fold_ids"] == [2, 3]
    fold2, fold2_dates = _load_backtest_artifact(
        tmp_path / "fold_02" / "deployment_test_backtest.npz"
    )
    fold3, fold3_dates = _load_backtest_artifact(
        tmp_path / "fold_03" / "deployment_test_backtest.npz"
    )
    assert fold2.strategy_returns.tolist() == [0.001, 0.001]
    assert np.asarray(fold2_dates).astype("datetime64[D]").astype(str).tolist() == [
        "2023-01-03",
        "2023-01-04",
    ]
    assert fold3.strategy_returns.size == 0
    assert np.asarray(fold3_dates).size == 0
    assert (
        tmp_path
        / "fold_02"
        / "deployment_test_backtest.pre_owned_handoff_v1.npz"
    ).is_file()
    assert (
        tmp_path
        / "fold_03"
        / "deployment_test_backtest.pre_owned_handoff_v1.npz"
    ).is_file()
    summary = json.loads((tmp_path / "owned_test_summary.json").read_text())
    assert [row["fold_id"] for row in summary["folds"]] == [1, 2]

    second = migrate_run(tmp_path, write_plots=False)
    assert second["status"] == "already_correct"
    assert second["changed_fold_ids"] == []
    assert second["historical_changed_fold_ids"] == [2, 3]
    assert second["historical_backups"]
