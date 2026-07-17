import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
import torch
from torch import nn

from stockagent.backtest.simulator import BacktestResult, BacktestResultTensor
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
import stockagent.training.trainer as trainer_module
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
        strategy_returns=np.array([0.01, -0.02, 0.03], dtype=np.float32),
        benchmark_returns=np.array([0.0, 0.005, 0.01], dtype=np.float32),
        turnovers=np.array([0.1, 0.2, 0.3], dtype=np.float32),
        weights_history=np.array(
            [[0.6, -0.4], [0.2, -0.8], [0.7, -0.3]],
            dtype=np.float32,
        ),
    )
    deployment_backtest = BacktestResult(
        strategy_returns=backtest.strategy_returns[:2].copy(),
        benchmark_returns=backtest.benchmark_returns[:2].copy(),
        turnovers=backtest.turnovers[:2].copy(),
        weights_history=backtest.weights_history[:2].copy(),
    )
    fold_result = FoldResult(
        fold_id=25,
        train_years=[2023],
        val_years=[2024],
        test_years=[2024, 2025, 2026],
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
        test_dates=np.array(["2024-01-02", "2025-01-02", "2026-01-02"], dtype="datetime64[D]"),
        symbols=["A", "B"],
        config=config,  # type: ignore[arg-type]
        deployment_backtest=deployment_backtest,
        deployment_dates=np.array(["2024-01-02", "2025-01-02"], dtype="datetime64[D]"),
        backtest_artifact_compression="compressed",
        print_report=False,
        write_plots=False,
        mark_complete=True,
    )

    assert (tmp_path / "model.pt").exists()
    assert (tmp_path / "metrics.json").exists()
    assert (tmp_path / "test_backtest.npz").exists()
    assert (tmp_path / "deployment_test_backtest.npz").exists()
    assert (tmp_path / "daily_portfolio_returns.csv").exists()
    assert (tmp_path / "daily_weights.csv").exists()
    assert (tmp_path / "annual_report.txt").exists()
    assert (tmp_path / "deployment_annual_report.txt").exists()
    assert (tmp_path / "fold_complete.json").exists()
    assert (tmp_path / "save_timing.json").exists()
    assert (tmp_path / "plot_timing.json").exists()

    metrics = json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["fold_id"] == 25
    assert metrics["best_val_loss"] == -1.5
    with ZipFile(tmp_path / "test_backtest.npz") as archive_zip:
        assert archive_zip.infolist()
        assert all(info.compress_type == ZIP_DEFLATED for info in archive_zip.infolist())
    archive = np.load(tmp_path / "test_backtest.npz")
    assert archive["weights_history"].shape == (3, 2)
    assert archive["dates"].astype("datetime64[D]")[-1] == np.datetime64("2026-01-02")
    deployment_archive = np.load(tmp_path / "deployment_test_backtest.npz")
    assert deployment_archive["weights_history"].shape == (2, 2)
    assert deployment_archive["dates"].astype("datetime64[D]")[-1] == np.datetime64("2025-01-02")
    assert "2026" in (tmp_path / "annual_report.txt").read_text(encoding="utf-8")
    deployment_report = (tmp_path / "deployment_annual_report.txt").read_text(encoding="utf-8")
    assert deployment_report.startswith("Annual Performance Report (Stitched Deployment)")
    assert "2026" not in deployment_report
    completion = json.loads((tmp_path / "fold_complete.json").read_text(encoding="utf-8"))
    assert completion["artifact_scope_version"] == 2
    assert completion["test_scope"] == "full_horizon"
    assert completion["test_rows"] == 3
    assert completion["test_date_end"] == "2026-01-02"
    assert completion["deployment_scope"] == "stitched_deployment"
    assert completion["deployment_rows"] == 2
    assert completion["deployment_date_end"] == "2025-01-02"


def test_walkforward_refresh_prefers_deployment_artifact_with_legacy_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fold_dir = tmp_path / "fold_01"
    fold_dir.mkdir()
    standard = BacktestResult(
        strategy_returns=np.array([0.01], dtype=np.float32),
        benchmark_returns=np.array([0.0], dtype=np.float32),
        turnovers=np.array([0.1], dtype=np.float32),
        weights_history=np.array([[1.0]], dtype=np.float32),
    )
    deployment = BacktestResult(
        strategy_returns=np.array([0.02], dtype=np.float32),
        benchmark_returns=np.array([0.0], dtype=np.float32),
        turnovers=np.array([0.2], dtype=np.float32),
        weights_history=np.array([[1.0]], dtype=np.float32),
    )
    trainer_module._save_backtest_artifact(
        trainer_module._backtest_path(fold_dir),
        standard,
        np.array(["2026-01-02"], dtype="datetime64[D]"),
    )
    trainer_module._save_backtest_artifact(
        trainer_module._deployment_backtest_path(fold_dir),
        deployment,
        np.array(["2025-01-02"], dtype="datetime64[D]"),
    )
    result = FoldResult(
        fold_id=1,
        train_years=[2023],
        val_years=[2024],
        test_years=[2025, 2026],
        best_val_loss=0.0,
        val_ic={},
        val_metrics={},
        test_ic={},
        test_metrics={},
    )
    captured_dates: list[np.ndarray] = []

    def _capture_dates(dates, *_args, **_kwargs) -> None:
        captured_dates.append(np.asarray(dates[0]))

    monkeypatch.setattr(trainer_module, "plot_fold_first_year_returns", _capture_dates)
    monkeypatch.setattr(trainer_module, "plot_fold_first_year_returns_log10", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(trainer_module, "plot_first_year_fold_metric_bars", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(trainer_module, "plot_first_year_turnover_concentration", lambda *_args, **_kwargs: None)

    trainer_module._refresh_walkforward_artifacts(tmp_path, [result])
    assert captured_dates[-1][0] == np.datetime64("2025-01-02")

    trainer_module._deployment_backtest_path(fold_dir).unlink()
    trainer_module._refresh_walkforward_artifacts(tmp_path, [result])
    assert captured_dates[-1][0] == np.datetime64("2026-01-02")

    empty_deployment = BacktestResult(
        strategy_returns=np.empty((0,), dtype=np.float32),
        benchmark_returns=np.empty((0,), dtype=np.float32),
        turnovers=np.empty((0,), dtype=np.float32),
        weights_history=np.empty((0, 1), dtype=np.float32),
    )
    trainer_module._save_backtest_artifact(
        trainer_module._deployment_backtest_path(fold_dir),
        empty_deployment,
        np.empty((0,), dtype="datetime64[D]"),
    )
    stale_first_year_plots = (
        tmp_path / "walkforward_first_year_cumulative_returns.png",
        tmp_path / "walkforward_first_year_cumulative_returns_log10.png",
        tmp_path / "walkforward_first_year_fold_metrics.png",
        tmp_path / "walkforward_first_year_turnover_concentration.png",
    )
    for path in stale_first_year_plots:
        path.write_bytes(b"stale")

    trainer_module._refresh_walkforward_artifacts(tmp_path, [result])
    assert all(not path.exists() for path in stale_first_year_plots)


def test_legacy_full_horizon_artifact_upgrades_without_model_inference(
    tmp_path: Path,
) -> None:
    dates = np.asarray(["2025-01-02", "2026-01-02", "2026-07-02"], dtype="datetime64[D]")
    mask = np.ones((3, 2), dtype=bool)
    panel = PanelData(
        dates=dates,
        symbols=["A", "B"],
        feature_names=["f0"],
        features=np.ones((3, 2, 1), dtype=np.float32),
        returns_1d=np.full((3, 2), 0.001, dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros((3,), dtype=np.float32),
        close_prices=np.ones((3, 2), dtype=np.float32),
    )
    fold = WalkForwardFold(
        fold_id=1,
        train_indices=np.empty((0,), dtype=np.int64),
        val_indices=np.empty((0,), dtype=np.int64),
        test_indices=np.arange(3, dtype=np.int64),
        train_years=[2023],
        val_years=[2024],
        test_years=[2025, 2026],
    )
    next_fold = WalkForwardFold(
        fold_id=2,
        train_indices=np.empty((0,), dtype=np.int64),
        val_indices=np.empty((0,), dtype=np.int64),
        test_indices=np.asarray([1, 2], dtype=np.int64),
        train_years=[2023, 2024],
        val_years=[2025],
        test_years=[2026],
    )
    fold_dir = trainer_module._fold_dir(tmp_path, fold.fold_id)
    fold_dir.mkdir(parents=True)
    full_backtest = BacktestResult(
        strategy_returns=np.asarray([0.01, 0.02, 0.03], dtype=np.float32),
        benchmark_returns=np.zeros((3,), dtype=np.float32),
        turnovers=np.asarray([0.1, 0.2, 0.3], dtype=np.float32),
        weights_history=np.ones((3, 2), dtype=np.float32),
    )
    trainer_module._save_backtest_artifact(
        trainer_module._backtest_path(fold_dir),
        full_backtest,
        dates,
    )
    fold_result = FoldResult(
        fold_id=1,
        train_years=fold.train_years,
        val_years=fold.val_years,
        test_years=fold.test_years,
        best_val_loss=0.0,
        val_ic={},
        val_metrics={},
        test_ic={},
        test_metrics={},
    )
    (fold_dir / "metrics.json").write_text(
        json.dumps(asdict(fold_result)),
        encoding="utf-8",
    )
    (fold_dir / "fold_complete.json").write_text(
        json.dumps({"status": "complete"}),
        encoding="utf-8",
    )
    config = SimpleNamespace(
        training=SimpleNamespace(lookback=1, backtest_artifact_compression="none")
    )

    assert trainer_module._upgrade_full_horizon_artifacts_without_inference(
        panel=panel,
        fold=fold,
        next_fold=next_fold,
        config=config,  # type: ignore[arg-type]
        output_path=tmp_path,
    )
    assert "2026" in (fold_dir / "annual_report.txt").read_text(encoding="utf-8")
    assert "2026" not in (fold_dir / "deployment_annual_report.txt").read_text(encoding="utf-8")
    marker = json.loads((fold_dir / "fold_complete.json").read_text(encoding="utf-8"))
    assert marker["artifact_scope_version"] == 2
    assert marker["test_rows"] == 3
    assert marker["deployment_rows"] == 1

    # A deployment-truncated artifact can mention every expected year while
    # still containing only the first part of the latest year. It must not be
    # mistaken for a complete full-horizon artifact.
    trainer_module._save_backtest_artifact(
        trainer_module._backtest_path(fold_dir),
        trainer_module._prefix_backtest_result(full_backtest, 2),
        dates[:2],
    )
    (fold_dir / "fold_complete.json").write_text(
        json.dumps({"status": "complete"}),
        encoding="utf-8",
    )
    assert not trainer_module._upgrade_full_horizon_artifacts_without_inference(
        panel=panel,
        fold=fold,
        next_fold=next_fold,
        config=config,  # type: ignore[arg-type]
        output_path=tmp_path,
    )


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
    expected_simple = np.einsum("ts,ts->t", leveraged.weights_history, np.expm1(future_returns))
    expected_fees = np.array([0.03, 0.02], dtype=np.float32)
    expected_strategy_returns = np.log1p(np.clip(expected_simple - expected_fees, -0.999999, None))
    np.testing.assert_allclose(leveraged.strategy_returns, expected_strategy_returns.astype(np.float32), atol=1e-7)
    np.testing.assert_allclose(leveraged.benchmark_returns, base.benchmark_returns, atol=1e-7)
