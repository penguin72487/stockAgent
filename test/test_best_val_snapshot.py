import json
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
import torch
from torch import nn

from stockagent.backtest.simulator import BacktestResult, BacktestResultTensor
from stockagent.data.walkforward import WalkForwardFold
from stockagent.training.trainer import (
    FoldResult,
    _realized_leverage_backtest,
    _save_best_val_backtest_snapshot,
    _save_fold_output_artifacts,
)


def test_save_best_val_backtest_snapshot_writes_compressed_npz_and_metadata(tmp_path: Path) -> None:
    backtest = BacktestResultTensor(
        strategy_returns=torch.tensor([0.01, -0.02, 0.03], dtype=torch.float32),
        benchmark_returns=torch.tensor([0.0, 0.01, -0.01], dtype=torch.float32),
        turnovers=torch.tensor([0.1, 0.2, 0.3], dtype=torch.float32),
        weights_history=torch.empty((0, 5), dtype=torch.float32),
    )
    fold = WalkForwardFold(
        fold_id=25,
        train_indices=np.array([0], dtype=np.int64),
        val_indices=np.array([1, 2, 3], dtype=np.int64),
        test_indices=np.array([4], dtype=np.int64),
        train_years=[2024],
        val_years=[2025],
        test_years=[2026],
    )

    _save_best_val_backtest_snapshot(
        fold_dir=tmp_path,
        fold=fold,
        epoch=7,
        val_loss=-1.25,
        val_backtest=backtest,
        row_start=1,
        row_end=3,
        dates=np.array(["2025-01-02", "2025-01-03"], dtype="datetime64[D]"),
        objective="log_utility",
    )

    metadata = json.loads((tmp_path / "best_val_snapshot.json").read_text(encoding="utf-8"))
    archive = np.load(tmp_path / "best_val_backtest.npz")

    assert metadata["fold_id"] == 25
    assert metadata["epoch"] == 7
    assert metadata["best_val_loss"] == -1.25
    assert metadata["rows"] == 2
    assert metadata["has_weights_history"] is False
    assert metadata["date_start"] == "2025-01-02"
    assert metadata["date_end"] == "2025-01-03"
    assert archive["strategy_returns"].tolist() == [-0.019999999552965164, 0.029999999329447746]
    assert archive["dates"].astype("datetime64[D]").tolist() == [
        np.datetime64("2025-01-02", "D"),
        np.datetime64("2025-01-03", "D"),
    ]
    assert archive["weights_history"].shape == (0, 5)


def test_save_fold_output_artifacts_writes_standard_files_with_compressed_backtest(tmp_path: Path) -> None:
    model = nn.Linear(2, 1)
    backtest = BacktestResult(
        strategy_returns=np.array([0.01, -0.02], dtype=np.float32),
        benchmark_returns=np.array([0.0, 0.005], dtype=np.float32),
        turnovers=np.array([0.1, 0.2], dtype=np.float32),
        weights_history=np.array([[0.6, -0.4], [0.2, -0.8]], dtype=np.float32),
    )
    fold_result = FoldResult(
        fold_id=25,
        train_years=[2023],
        val_years=[2024],
        test_years=[2025],
        best_val_loss=-1.5,
        val_ic={"ic_mean": 0.1, "ic_std": 0.2, "ic_ir": 0.3, "ic_positive_ratio": 1.0},
        val_metrics={"cumulative_return": 0.01},
        test_ic={"ic_mean": 0.2, "ic_std": 0.3, "ic_ir": 0.4, "ic_positive_ratio": 1.0},
        test_metrics={"cumulative_return": 0.02},
        test_integer_metrics=None,
    )
    config = SimpleNamespace(
        training=SimpleNamespace(
            table_output_format="csv",
            save_daily_weights_table=True,
            save_daily_weights_csv=True,
            save_integer_share_daily_weights_table=False,
            save_integer_share_holdings_table=False,
            backtest_artifact_compression="none",
        )
    )

    _save_fold_output_artifacts(
        fold_dir=tmp_path,
        fold_result=fold_result,
        model=model,
        test_backtest=backtest,
        test_dates=np.array(["2025-01-02", "2025-01-03"], dtype="datetime64[D]"),
        symbols=["A", "B"],
        config=config,  # type: ignore[arg-type]
        backtest_artifact_compression="compressed",
        print_report=False,
        write_plots=False,
    )

    assert (tmp_path / "model.pt").exists()
    assert (tmp_path / "metrics.json").exists()
    assert (tmp_path / "test_backtest.npz").exists()
    assert (tmp_path / "daily_portfolio_returns.csv").exists()
    assert (tmp_path / "daily_weights.csv").exists()
    assert (tmp_path / "annual_report.txt").exists()
    assert (tmp_path / "save_timing.json").exists()
    assert (tmp_path / "plot_timing.json").exists()

    metrics = json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["fold_id"] == 25
    assert metrics["best_val_loss"] == -1.5
    with ZipFile(tmp_path / "test_backtest.npz") as archive_zip:
        assert archive_zip.infolist()
        assert all(info.compress_type == ZIP_DEFLATED for info in archive_zip.infolist())
    archive = np.load(tmp_path / "test_backtest.npz")
    assert archive["weights_history"].shape == (2, 2)


def test_realized_leverage_backtest_multiplies_realized_positions_before_returns_and_fees() -> None:
    base = BacktestResult(
        strategy_returns=np.zeros(2, dtype=np.float32),
        benchmark_returns=np.array([0.01, -0.02], dtype=np.float32),
        turnovers=np.zeros(2, dtype=np.float32),
        weights_history=np.array([[0.5, -0.5], [0.2, -0.1]], dtype=np.float32),
    )
    future_returns = np.array([[0.10, 0.20], [0.05, -0.10]], dtype=np.float32)

    leveraged = _realized_leverage_backtest(
        base,
        future_returns,
        leverage_multiplier=2.0,
        buy_fee_rate=0.01,
        sell_fee_rate=0.02,
    )

    np.testing.assert_allclose(
        leveraged.weights_history,
        np.array([[1.0, -1.0], [0.4, -0.2]], dtype=np.float32),
        atol=1e-7,
    )
    np.testing.assert_allclose(leveraged.turnovers, np.array([2.0, 1.4], dtype=np.float32), atol=1e-7)
    np.testing.assert_allclose(leveraged.strategy_returns, np.array([-0.13, 0.02], dtype=np.float32), atol=1e-7)
    np.testing.assert_allclose(leveraged.benchmark_returns, base.benchmark_returns, atol=1e-7)
