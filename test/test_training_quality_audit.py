"""Saved-tape audit changes reporting, not checkpoint selection or accounts."""
import json

import numpy as np
import pytest
import torch

from scripts import audit_training_quality as audit


def saved_case(tmp_path):
    root = tmp_path / "runs"
    case = root / "example"
    fold = case / "fold_01"
    fold.mkdir(parents=True)
    r = np.array([.01, -.02, .03, -.01])
    b = np.array([.005, -.01, .02, -.005])
    turnover = np.array([.1, .2, .3, .4])
    nav = np.full((4, 270), 100.)
    nav[1, :10] = 80.
    np.savez(fold / "test_backtest.npz", dates=np.arange("2026-01-01", "2026-01-05", dtype="datetime64[D]"),
             strategy_returns=r, benchmark_returns=b, turnovers=turnover,
             minute_nav=nav, carry_segment_initial_nav=100.)
    (fold / "metrics.json").write_text(json.dumps({
        "fold_id": 1, "val_metrics": {},
        "test_metrics": audit.return_metrics(r, b, turnover),
    }))
    torch.save({"epoch": 7, "model_state_dict": {"weight": torch.tensor([1.])}},
               fold / "checkpoint_best.pt")
    curve = case / "train_example"
    curve.mkdir()
    (curve / "epoch_curve.jsonl").write_text("\n".join(json.dumps(row) for row in (
        {"epoch": 1, "val_mean": -10.}, {"epoch": 7, "val_mean": -1.})))
    return root, fold


def test_audit_uses_saved_checkpoint_selection_and_ordered_minute_nav(tmp_path):
    root, _ = saved_case(tmp_path)
    result = audit.audit(root)
    assert result["contract"] == "saved_daily_log_return_quality_audit_v2"
    row, = result["cases"]
    assert row["selected_epoch"] == row["selected_curve"]["epoch"] == 7
    assert row["curve_raw_min_epoch"] == 1
    assert row["intraday_nav"]["max_drawdown"] == pytest.approx(-.2)
    assert row["max_saved_metric_error"] == 0
    assert row["test_metrics"]["turnover"] == pytest.approx(.25)
    assert "paired_block_bootstrap" not in row


def test_nonfinite_saved_model_is_not_accepted(tmp_path):
    root, fold = saved_case(tmp_path)
    torch.save({"epoch": 7, "model_state_dict": {"weight": torch.tensor([float("nan")])}},
               fold / "checkpoint_best.pt")
    with pytest.raises(ValueError, match="nonfinite checkpoint"):
        audit.audit(root)


def test_bootstrap_pairs_strategy_and_benchmark_without_new_holdout_claim():
    values = np.array([.01, -.02, .03, -.01, .005, -.004])
    kwargs = dict(repetitions=20, block_length=2)
    result = audit.paired_block_bootstrap(values, values, **kwargs)
    assert result == audit.paired_block_bootstrap(values, values, **kwargs)
    assert all(interval == [0., 0.] for interval in result["delta_95_percentile_interval"].values())
    assert result["resample_fraction_all_four_deltas_positive"] == 0
    assert "not new out-of-sample evidence" in result["boundary"]
