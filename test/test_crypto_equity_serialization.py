"""Crypto NAV precision survives tensor, best-val, and NPZ boundaries."""

from types import SimpleNamespace

import numpy as np
import pytest
import torch

from stockagent.backtest.simulator import BacktestResultTensor
from stockagent.training import trainer


def _result(mode="crypto_perpetual", scale=1e50):
    return BacktestResultTensor(
        strategy_returns=torch.zeros(2), benchmark_returns=torch.zeros(2),
        turnovers=torch.zeros(2), weights_history=torch.zeros(2, 1),
        requested_weights_history=torch.zeros(2, 1), final_weights=torch.zeros(1),
        final_alive=torch.tensor(True), execution_mode=mode,
        settlement_ledger_unit="notional_weight" if mode == "crypto_perpetual" else None,
        equity_scale_history=torch.full((2,), scale, dtype=torch.float64),
        final_equity_scale=torch.tensor(scale, dtype=torch.float64),
    )


@pytest.mark.parametrize("scale", [1.000000000123, 1e50, 1e-50])
def test_crypto_to_numpy_keeps_equity_fp64_without_promoting_other_arrays(scale):
    source = _result(scale=scale)
    result = source.to_numpy()
    for name in ("equity_scale_history", "final_equity_scale"):
        assert getattr(result, name).dtype == np.float64
        np.testing.assert_array_equal(getattr(result, name), getattr(source, name).numpy())
    for name in ("strategy_returns", "weights_history", "final_weights"):
        assert getattr(result, name).dtype == np.float32


def test_to_numpy_noncrypto_precision_and_absent_equity_are_unchanged():
    source = _result(mode="naive", scale=1.000000000123)
    result = source.to_numpy()
    assert result.equity_scale_history.dtype == np.float32
    assert result.final_equity_scale.dtype == np.float32
    source.equity_scale_history = source.final_equity_scale = None
    result = source.to_numpy()
    assert result.equity_scale_history is result.final_equity_scale is None


@pytest.mark.parametrize("row_end", [1, 2])
def test_best_val_crypto_npz_roundtrip_keeps_equity_fp64(tmp_path, row_end):
    source = _result()
    dates = np.arange("2024-01-01", "2024-01-03", dtype="datetime64[D]")
    trainer._save_best_val_backtest_snapshot(
        fold_dir=tmp_path,
        fold=SimpleNamespace(fold_id=1, train_years=[2022], val_years=[2023], test_years=[2024]),
        epoch=1, val_loss=0.0, val_backtest=source,
        row_start=0, row_end=row_end, dates=dates[:row_end], objective="log_utility",
    )
    loaded = trainer._load_backtest_artifact(tmp_path / "best_val_backtest.npz")[0]
    assert loaded.equity_scale_history.dtype == np.float64
    np.testing.assert_array_equal(loaded.equity_scale_history,
                                  source.equity_scale_history[:row_end].numpy())
    if row_end == 2:
        assert loaded.final_equity_scale.dtype == np.float64
        np.testing.assert_array_equal(loaded.final_equity_scale, source.final_equity_scale.numpy())
    else:
        assert loaded.final_equity_scale is None
        assert loaded.final_weights is None
    assert loaded.strategy_returns.dtype == np.float32


def test_best_val_noncrypto_optional_float_precision_unchanged(tmp_path, monkeypatch):
    # A capture isolates the conversion from unrelated naive artifact contracts.
    captured = []
    monkeypatch.setattr(trainer, "_save_backtest_artifact",
                        lambda path, result, dates, **kwargs: captured.append(result))
    source = _result(mode="naive", scale=1.000000000123)
    trainer._save_best_val_backtest_snapshot(
        fold_dir=tmp_path,
        fold=SimpleNamespace(fold_id=1, train_years=[2022], val_years=[2023], test_years=[2024]),
        epoch=1, val_loss=0.0, val_backtest=source, row_start=0, row_end=2,
        dates=np.arange("2024-01-01", "2024-01-03", dtype="datetime64[D]"),
        objective="log_utility",
    )
    assert captured[0].equity_scale_history.dtype == np.float32
    assert captured[0].final_equity_scale.dtype == np.float32
