from __future__ import annotations

import numpy as np
import json
import pytest
import torch

from stockagent.backtest.report import compute_metrics
from stockagent.backtest.return_metrics import (
    RETURN_METRICS_CONTRACT_VERSION,
    ZERO_NAV_LOG,
    clean_log_returns,
    clean_log_returns_torch,
)
from stockagent.backtest.simulator import BacktestResult
from stockagent.training.lifecycle import canonical_mode_artifact_contract
from stockagent.training import trainer


@pytest.mark.parametrize("device", ["cpu", "cuda"])
@pytest.mark.parametrize("default", [-np.inf, np.finfo(np.float64).min, ZERO_NAV_LOG])
@pytest.mark.parametrize("position", [0, 2, 4])
def test_default_is_absorbing_and_matches_numpy_reporting(device, default, position):
    if device == "cuda" and not torch.cuda.is_available():
        pytest.skip("CUDA is unavailable")
    raw = np.array([0.02, -0.01, 0.03, 0.01, -0.02], dtype=np.float64)
    raw[position] = default
    values = torch.tensor(raw, device=device, dtype=torch.float64)
    actual = clean_log_returns_torch(values)
    np.testing.assert_allclose(actual.cpu().numpy(), clean_log_returns(raw), rtol=0, atol=1e-13)
    np.testing.assert_array_equal(values.cpu().numpy(), raw)
    assert actual.dtype == torch.float64
    assert str(actual.device).startswith(device)
    assert torch.count_nonzero(actual[position + 1:]).item() == 0
    assert torch.expm1(actual.sum()).item() == -1.0


@pytest.mark.parametrize("helper", [trainer._compute_metrics_from_tensors, trainer._compute_eval_metrics_like_legacy_online])
@pytest.mark.parametrize("ruined", ["strategy", "benchmark", "both"])
@pytest.mark.parametrize("mode,periods", [("naive", 252.0), ("crypto_perpetual", 365.0)])
def test_all_tensor_report_metrics_match_canonical_default_policy(helper, ruined, mode, periods):
    strategy = np.array([0.02, -0.01, 0.03, 0.01, -0.02], dtype=np.float64)
    benchmark = np.array([0.01, 0.02, -0.01, 0.03, 0.01], dtype=np.float64)
    if ruined in {"strategy", "both"}:
        strategy[2] = -np.inf
    if ruined in {"benchmark", "both"}:
        benchmark[1] = -np.inf
    turnover = np.array([0.1, 0.2, 0.3, 0.4, 0.5], dtype=np.float64)
    expected = compute_metrics(BacktestResult(
        strategy_returns=strategy, benchmark_returns=benchmark, turnovers=turnover,
        weights_history=np.zeros((len(strategy), 1)), execution_mode=mode,
    ))
    actual = helper(*[torch.from_numpy(v) for v in (strategy, benchmark, turnover)], periods_per_year=periods)
    assert actual == pytest.approx(expected, rel=1e-12, abs=1e-12)
    assert all(np.isfinite(v) for v in actual.values())
    if ruined in {"strategy", "both"}:
        assert actual["cumulative_return"] == actual["max_drawdown"] == -1.0
        assert actual["daily_hit_rate"] == pytest.approx(1 / 5)


@pytest.mark.parametrize("helper", [trainer._compute_metrics_from_tensors, trainer._compute_eval_metrics_like_legacy_online])
def test_metrics_keep_fp64_and_the_existing_missing_value_policy(helper):
    raw = np.array([0.123456789012345, np.nan, np.inf, -0.023456789012346], dtype=np.float64)
    cleaned = clean_log_returns(raw)
    values = torch.from_numpy(raw)
    actual = helper(values, torch.zeros_like(values), torch.zeros_like(values))
    assert actual["cumulative_return"] == pytest.approx(np.expm1(cleaned.sum()), rel=0, abs=1e-15)
    assert clean_log_returns_torch(values).cpu().numpy() == pytest.approx(cleaned, rel=0, abs=0)
    empty = helper(*(torch.empty(0, dtype=torch.float64) for _ in range(3)))
    assert all(v == 0 for v in empty.values())


def test_reporting_contract_records_the_normalization_version():
    assert canonical_mode_artifact_contract("tw_cash")["metrics_contract_version"] == RETURN_METRICS_CONTRACT_VERSION == 2


def test_resume_does_not_reuse_metrics_without_current_reporting_contract(tmp_path, monkeypatch):
    fold_dir = trainer._fold_dir(tmp_path, 1)
    fold_dir.mkdir()
    for name in ("metrics.json", "model.pt", "test_backtest.npz", "checkpoint_best.pt"):
        (fold_dir / name).write_bytes(b"saved")
    monkeypatch.setattr(trainer, "_has_completed_fold_marker", lambda _path: True)
    monkeypatch.setattr(trainer, "_load_fold_result", lambda _path: "current-report")
    checkpoint_reads = []
    monkeypatch.setattr(trainer, "_load_checkpoint", lambda path: checkpoint_reads.append(path) or {})
    monkeypatch.setattr(trainer, "_validate_checkpoint_manifest", lambda *_args, **_kwargs: None)
    for version in (None, 1, "2"):
        (fold_dir / "mode_artifact_contract.json").write_text(json.dumps({"metrics_contract_version": version}))
        assert trainer._load_completed_fold_result(tmp_path, 1, expected_manifest={}) is None
    assert checkpoint_reads == []
    (fold_dir / "mode_artifact_contract.json").write_text(json.dumps(canonical_mode_artifact_contract("tw_cash")))
    assert trainer._load_completed_fold_result(tmp_path, 1, expected_manifest={}) == "current-report"
    assert len(checkpoint_reads) == 1
