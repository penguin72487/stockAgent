from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from scripts.audit_crypto_training import (
    _Sources,
    analyze_backtest,
    analyze_epoch_curve,
    audit_root,
    main,
    return_metrics,
)


def _arrays():
    return {
        "dates": np.array(["2022-01-01", "2022-01-02", "2023-01-01"], dtype="datetime64[D]"),
        "strategy_returns": np.log1p([-0.1, 0.2, 0.5]),
        "turnovers": np.array([0.3, 0.4, 0.5]),
        "requested_weights_history": np.array([[0.5, -0.2], [0.4, -0.1], [0.6, 0.0]]),
        "weights_history": np.array([[0.4, -0.1], [0.3, -0.1], [0.5, 0.0]]),
    }


def test_return_drawdown_includes_initial_nav():
    metrics = return_metrics(np.log1p([-0.1, 0.2]))
    assert metrics["cumulative_return"] == pytest.approx(0.08)
    assert metrics["max_drawdown_including_initial_nav"] == pytest.approx(-0.1)
    assert return_metrics([0.0, 0.0])["max_drawdown_including_initial_nav"] == 0.0


def test_first_year_only_and_same_path_fee_accounting():
    metrics, dates, returns = analyze_backtest(_arrays(), first_test_year=2022, equal_side_fee_rate=0.001)
    assert len(dates) == len(returns) == metrics["rows"] == 2
    assert metrics["cumulative_return"] == pytest.approx(0.08)
    assert metrics["turnover_mean"] == pytest.approx(0.35)
    assert metrics["requested_weights"]["gross_mean"] == pytest.approx(0.6)
    assert metrics["executed_weights"]["gross_mean"] == pytest.approx(0.45)
    assert metrics["executed_weights"]["net_mean"] == pytest.approx(0.25)
    assert metrics["executed_weights"]["max_abs_weight"] == 0.4
    added = metrics["same_path_fee_addback"]
    assert "NOT a zero-fee rerun" in added["interpretation"]
    assert added["cumulative_return"] == pytest.approx((0.9 + 0.0003) * (1.2 + 0.0004) - 1)
    assert added["simple_fee_fraction_sum_on_daily_nav"] == pytest.approx(0.0007)
    assert not metrics["full_calendar_year"]


def test_zero_fee_and_missing_side_fee_evidence():
    zero = analyze_backtest(_arrays(), first_test_year=2022, equal_side_fee_rate=0.0)[0]
    assert zero["same_path_fee_addback"]["cumulative_return"] == zero["cumulative_return"]
    missing = analyze_backtest(_arrays(), first_test_year=2022, equal_side_fee_rate=None)[0]
    assert missing["same_path_fee_addback"] is None


@pytest.mark.parametrize("key", ["strategy_returns", "turnovers", "weights_history", "requested_weights_history"])
def test_nonfinite_backtest_rejected(key):
    arrays = _arrays()
    arrays[key].flat[0] = np.nan
    with pytest.raises(ValueError, match="finite"):
        analyze_backtest(arrays, first_test_year=2022, equal_side_fee_rate=0.001)


@pytest.mark.parametrize("problem", ["duplicate", "unordered", "nat", "length", "negative_turnover", "weights_shape", "wrong_first_year"])
def test_backtest_contract_guards(problem):
    arrays = _arrays()
    first_year = 2022
    if problem == "duplicate":
        arrays["dates"][1] = arrays["dates"][0]
    elif problem == "unordered":
        arrays["dates"] = arrays["dates"][::-1]
    elif problem == "nat":
        arrays["dates"][0] = np.datetime64("NaT")
    elif problem == "length":
        arrays["strategy_returns"] = arrays["strategy_returns"][:-1]
    elif problem == "negative_turnover":
        arrays["turnovers"][0] = -0.2
    elif problem == "weights_shape":
        arrays["weights_history"] = arrays["weights_history"][:, :1]
    else:
        first_year = 2023
    with pytest.raises(ValueError):
        analyze_backtest(arrays, first_test_year=first_year, equal_side_fee_rate=0.001)


def _curve():
    return [
        {"epoch": 1, "train_loss": -0.1, "val_mean": -0.2, "test_mean": 0.3, "no_improve": 0, "train_optimizer_steps": 1},
        {"epoch": 101, "train_loss": -5.0, "val_mean": 0.2, "test_mean": -0.8, "no_improve": 100, "train_optimizer_steps": 1},
    ]


def test_curve_measures_actual_steps_and_validation_selection():
    result = analyze_epoch_curve(_curve(), epochs_cap=1000, early_stop_ratio=0.1)
    assert result["minimum_observed_val_loss_epoch"]["epoch"] == 1
    assert result["end_consistent_with_early_stopping"]
    assert result["early_stop_patience"] == 100
    assert result["last_epoch"] == 101
    assert not result["reached_configured_epochs_cap"]
    assert result["optimizer_steps_observed_total"] == 2
    assert result["optimizer_steps_distinct"] == [1]
    assert not result["epoch_coverage_contiguous"]


def test_missing_optimizer_telemetry_is_not_claimed_as_zero_steps():
    rows = _curve()
    del rows[0]["train_optimizer_steps"]
    result = analyze_epoch_curve(rows, epochs_cap=1000, early_stop_ratio=0.1)
    assert result["optimizer_steps_missing_rows"] == 1
    assert result["optimizer_steps_observed_total"] == 1


def test_gradient_audit_distinguishes_unmeasured_and_multi_update_means():
    rows = _curve()
    rows[0].update(train_grad_norm_measured=1, train_grad_norm_observations=1,
                   train_grad_norm_before_clip_mean=3.0, train_zero_grad_batches=0,
                   train_first_dead_portfolio_batch=-1, lr=0.0003)
    rows[1].update(train_grad_norm_measured=1, train_grad_norm_observations=2,
                   train_optimizer_steps=2, train_grad_norm_before_clip_mean=5.0,
                   train_zero_grad_batches=1, train_first_dead_portfolio_batch=2, lr=0.0001)
    result = analyze_epoch_curve(rows, epochs_cap=1000, early_stop_ratio=0.1, grad_clip_norm=1.0)
    telemetry = result["gradient_telemetry"]
    assert telemetry["measured_epoch_rows"] == 2
    assert telemetry["single_update_observations"] == telemetry["single_updates_above_clip"] == 1
    assert telemetry["epoch_mean_norm_median"] == 4.0
    assert telemetry["epochs_with_zero_gradient_batch"] == 1
    assert telemetry["epochs_with_dead_account"] == 1
    assert result["learning_rate_distinct"] == [0.0001, 0.0003]
    rows[0]["train_grad_norm_measured"] = 0
    rows[1]["train_grad_norm_observations"] = 0
    telemetry = analyze_epoch_curve(rows, epochs_cap=1000, early_stop_ratio=0.1)["gradient_telemetry"]
    assert telemetry["measured_epoch_rows"] == 0
    assert telemetry["missing_epoch_rows"] == 2
    assert telemetry["epoch_mean_norm_min"] is None
    assert telemetry["single_updates_above_clip"] is None


def test_missing_gradient_telemetry_does_not_claim_no_dead_accounts():
    result = analyze_epoch_curve(_curve(), epochs_cap=1000, early_stop_ratio=0.1)
    assert result["gradient_telemetry"]["epochs_with_dead_account"] is None
    assert result["gradient_telemetry"]["epochs_with_zero_gradient_batch"] is None
    assert result["learning_rate_missing_rows"] == 2


@pytest.mark.parametrize("field", ["lr", "train_grad_norm_before_clip_mean"])
def test_nonfinite_gradient_audit_rejected(field):
    rows = _curve()
    rows[0].update(train_grad_norm_measured=1, train_grad_norm_observations=1,
                   train_grad_norm_before_clip_mean=3.0, lr=0.0003)
    rows[0][field] = float("nan")
    with pytest.raises(ValueError, match="finite"):
        analyze_epoch_curve(rows, epochs_cap=1000, early_stop_ratio=0.1)


@pytest.mark.parametrize("problem", ["duplicate", "nonfinite", "bad_steps", "no_val"])
def test_curve_guards(problem):
    rows = _curve()
    if problem == "duplicate":
        rows[1]["epoch"] = 1
    elif problem == "nonfinite":
        rows[0]["train_loss"] = float("inf")
    elif problem == "bad_steps":
        rows[0]["train_optimizer_steps"] = -1
    else:
        for row in rows:
            row["val_mean"] = None
    with pytest.raises(ValueError):
        analyze_epoch_curve(rows, epochs_cap=1000, early_stop_ratio=0.1)


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _artifact_root(tmp_path: Path) -> Path:
    root = tmp_path / "crypto"
    summary = []
    # Fold 2 intentionally reuses its validation year and must be excluded.
    for fold_id, val_year, test_year in [(1, 2021, 2022), (2, 2023, 2023)]:
        record = {
            "fold_id": fold_id, "train_years": list(range(2020, val_year)),
            "val_years": [val_year], "test_years": [test_year],
            "best_val_loss": -0.2, "val_metrics": {"cumulative_return": 0.2},
        }
        summary.append(record)
        folder = root / f"fold_{fold_id:02d}"
        _write_json(folder / "fold_complete.json", {**record, "status": "complete"})
        arrays = _arrays()
        arrays["dates"] = np.arange(f"{test_year}-01-01", f"{test_year}-01-04", dtype="datetime64[D]")
        np.savez(folder / "test_backtest.npz", **arrays)
        group = root / ("train_" + "-".join(map(str, record["train_years"])))
        group.mkdir()
        (group / "epoch_curve.jsonl").write_text("\n".join(map(json.dumps, _curve())), encoding="utf-8")
    _write_json(root / "summary.json", summary)
    _write_json(root / "progress.json", {"state": "complete"})
    _write_json(root / "run_manifest.json", {
        "execution_mode": "crypto_perpetual",
        "configuration": {
            "training": {"epochs": 1000, "early_stopping_no_improve_ratio": 0.1},
            "trading": {"buy_fee_rate": 0.001, "sell_fee_rate": 0.001},
        },
    })
    return root


def test_root_excludes_validation_overlap_and_preserves_inputs(tmp_path):
    root = _artifact_root(tmp_path)
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    result = audit_root(root)
    stitch = result["oos_first_year_reset_account_stitch"]
    assert stitch["fold_ids"] == [1]
    assert stitch["rows"] == 3
    assert "reset account" in stitch["account_contract"]
    assert not result["folds"][1]["included_in_oos_first_year_stitch"]
    assert "overlaps" in result["folds"][1]["exclusion_reason"]
    assert len(result["sources_sha256"]) == len(before)
    assert all(p.read_bytes() == data for p, data in before.items())


def test_root_rejects_duplicate_folds(tmp_path):
    root = _artifact_root(tmp_path)
    records = json.loads((root / "summary.json").read_text())
    _write_json(root / "summary.json", [records[0], records[0]])
    with pytest.raises(ValueError, match="duplicate fold"):
        audit_root(root)


def test_stitch_rejects_duplicate_out_of_sample_days(tmp_path):
    root = _artifact_root(tmp_path)
    records = json.loads((root / "summary.json").read_text())
    records[1]["test_years"] = [2022]
    records[1]["train_years"] = [2020]
    records[1]["val_years"] = [2021]
    _write_json(root / "summary.json", records)
    _write_json(root / "fold_02/fold_complete.json", {**records[1], "status": "complete"})
    arrays = _arrays()
    arrays["dates"] = np.arange("2022-01-01", "2022-01-04", dtype="datetime64[D]")
    np.savez(root / "fold_02/test_backtest.npz", **arrays)
    with pytest.raises(ValueError, match="strictly increasing"):
        audit_root(root)


def test_root_rejects_backtest_outside_declared_years(tmp_path):
    root = _artifact_root(tmp_path)
    np.savez(root / "fold_01/test_backtest.npz", **_arrays())
    with pytest.raises(ValueError, match="exceed declared"):
        audit_root(root)


def test_root_excludes_test_before_selection_window(tmp_path):
    root = _artifact_root(tmp_path)
    records = json.loads((root / "summary.json").read_text())
    records[0]["val_years"] = [2024]
    _write_json(root / "summary.json", records)
    _write_json(root / "fold_01/fold_complete.json", {**records[0], "status": "complete"})
    result = audit_root(root)
    assert result["oos_first_year_reset_account_stitch"] is None
    assert not result["folds"][0]["included_in_oos_first_year_stitch"]


def test_source_mutation_is_detected(tmp_path):
    (tmp_path / "source").write_bytes(b"original")
    sources = _Sources(tmp_path)
    sources.read("source")
    (tmp_path / "source").write_bytes(b"changed")
    with pytest.raises(ValueError, match="source changed"):
        sources.verify()


def test_cli_output_is_optional_and_cannot_replace_artifacts(tmp_path, capsys):
    root = _artifact_root(tmp_path)
    main(["--root", str(root)])
    assert json.loads(capsys.readouterr().out)["audit_schema_version"] == 1
    with pytest.raises(SystemExit):
        main(["--root", str(root), "--output", str(root / "summary.json")])
    output = tmp_path / "audit.json"
    main(["--root", str(root), "--output", str(output)])
    assert json.loads(output.read_text())["audit_schema_version"] == 1
