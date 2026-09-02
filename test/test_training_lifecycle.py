import json
from pathlib import Path
import zipfile

import pytest

from stockagent.training.lifecycle import (
    TRAINING_ARTIFACT_LAYOUT_VERSION,
    TRAINING_LIFECYCLE_SCHEMA_VERSION,
    TRAINING_PROGRESS_SCHEMA_VERSION,
    TrainingArtifactLayout,
    TrainingRunLifecycle,
    canonical_mode_artifact_contract,
    normalize_epoch_curve_record,
    validate_completed_training_artifacts,
)


_BACKTEST_MEMBER_NAMES = (
    "artifact_schema_version.npy",
    "execution_mode.npy",
    "strategy_returns.npy",
    "benchmark_returns.npy",
    "turnovers.npy",
    "weights_history.npy",
    "dates.npy",
)


def _write_minimal_completed_artifacts(
    layout: TrainingArtifactLayout,
    *,
    group_name: str,
    fold_id: int,
    execution_mode: str,
) -> None:
    required = [
        layout.summary_path,
        layout.group_checkpoint_path(group_name),
        layout.group_pre_epoch_timing_path(group_name),
        layout.group_curve_path(group_name),
        *layout.required_fold_paths(fold_id),
    ]
    for path in required:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"artifact")
    for path in (
        layout.fold_dir(fold_id) / "test_backtest.npz",
        layout.fold_dir(fold_id) / "deployment_test_backtest.npz",
    ):
        with zipfile.ZipFile(path, "w") as archive:
            for name in _BACKTEST_MEMBER_NAMES:
                archive.writestr(name, b"array")
    for name in (
        "equity_curve.png",
        "equity_curve_log.png",
        "annual_performance.png",
    ):
        (layout.fold_dir(fold_id) / name).write_bytes(b"\x89PNG\r\n\x1a\nimage")
    layout.summary_path.write_text(json.dumps([{"fold_id": fold_id}]))
    layout.group_pre_epoch_timing_path(group_name).write_text(
        json.dumps({"stage": "setup"}) + "\n"
    )
    layout.group_curve_path(group_name).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "epoch": 1,
                "train_loss": 0.1,
                "val_mean": 0.2,
                "test_mean": 0.3,
                "lr": 1.0e-3,
                "no_improve": 0,
                "best_val_loss": 0.2,
                "improved": 1,
                "epoch_total_s": 1.0,
            }
        )
        + "\n"
    )
    (layout.fold_dir(fold_id) / "mode_artifact_contract.json").write_text(
        json.dumps(
            {
                "schema_version": TRAINING_LIFECYCLE_SCHEMA_VERSION,
                "artifact_layout_version": TRAINING_ARTIFACT_LAYOUT_VERSION,
                "execution_mode": execution_mode,
            }
        )
    )
    layout.fold_complete_path(fold_id).write_text(
        json.dumps(
            {
                "artifact_scope_version": 2,
                "status": "complete",
                "fold_id": fold_id,
                "test_scope": "full_horizon",
                "deployment_scope": "stitched_deployment",
            }
        )
    )
    (layout.fold_dir(fold_id) / "metrics.json").write_text(
        json.dumps({"fold_id": fold_id})
    )
    (layout.fold_dir(fold_id) / "plot_timing.json").write_text("{}")


def test_artifact_layout_is_frequency_independent(tmp_path: Path) -> None:
    layout = TrainingArtifactLayout(tmp_path)

    assert layout.fold_dir(3) == tmp_path / "fold_03"
    assert layout.best_checkpoint_path(3) == tmp_path / "fold_03/checkpoint_best.pt"
    assert layout.year_group_name([2020, 2021]) == "train_2020-2021"
    assert layout.group_checkpoint_path("train_20200102-20201231") == (
        tmp_path / "train_20200102-20201231/checkpoint_last.pt"
    )
    assert layout.group_curve_path("train_2020-2021") == (
        tmp_path / "train_2020-2021/epoch_curve.jsonl"
    )

    with pytest.raises(ValueError, match="strictly increasing"):
        layout.year_group_name([2021, 2020])
    with pytest.raises(ValueError, match="must start"):
        layout.group_dir("minute_2020")
    with pytest.raises(ValueError, match="invalid"):
        layout.group_dir("train_../../escape")


@pytest.mark.parametrize(
    "execution_mode",
    ["naive", "tw_day_trade", "tw_minute", "tw_index_derivatives_tick"],
)
def test_run_and_progress_schema_do_not_change_by_mode(
    tmp_path: Path,
    execution_mode: str,
) -> None:
    root = tmp_path / execution_mode
    legacy_manifest = root / f"{execution_mode}_legacy_manifest.json"
    lifecycle = TrainingRunLifecycle(
        root,
        execution_mode=execution_mode,
        run_mode="train",
        strategy="none",
        model_name="transformer_base_portfolio",
    )
    lifecycle.start(
        fold_ids=[1],
        dataset_fingerprint="data-fingerprint",
        configuration_fingerprint="config-fingerprint",
        contract_versions={"execution": 1},
        data_summary={"dates": 3, "symbols": 2},
        compatibility_manifest_paths=[legacy_manifest],
    )
    lifecycle.start_group(
        group_name="train_2020-2021",
        group_index=1,
        group_total=1,
        fold_ids=[1],
        epoch_total=2,
    )
    lifecycle.set_phase(
        "training",
        fold_id=1,
        epoch=1,
        epoch_total=2,
        work_total=3,
        work_unit="day",
    )
    lifecycle.update_work(
        completed=3,
        total=3,
        unit="day",
        last_sample="2023-01-03",
        metrics={"last_return": 0.01},
    )
    lifecycle.finish_epoch(
        epoch=1,
        epoch_total=2,
        metrics={
            "train_loss": 0.1,
            "val_mean": 0.2,
            "test_mean": None,
            "best_val_loss": float("inf"),
        },
    )
    lifecycle.finish_fold(1)
    _write_minimal_completed_artifacts(
        lifecycle.layout,
        group_name="train_2020-2021",
        fold_id=1,
        execution_mode=execution_mode,
    )
    lifecycle.complete(fold_ids=[1])

    manifest = json.loads(layout_path(root, "run_manifest.json").read_text())
    progress = json.loads(layout_path(root, "progress.json").read_text())
    legacy = json.loads(legacy_manifest.read_text())

    assert manifest == legacy
    assert manifest["schema_version"] == TRAINING_LIFECYCLE_SCHEMA_VERSION
    assert manifest["artifact_layout_version"] == TRAINING_ARTIFACT_LAYOUT_VERSION
    assert manifest["execution_mode"] == execution_mode
    assert manifest["selected_fold_ids"] == [1]
    assert progress["schema_version"] == TRAINING_PROGRESS_SCHEMA_VERSION
    assert progress["state"] == "complete"
    assert progress["phase"] == "complete"
    assert progress["group"]["name"] == "train_2020-2021"
    assert progress["fold"]["completed_ids"] == [1]
    assert progress["work"] == {"completed": 1, "total": 1, "unit": "fold"}


def layout_path(root: Path, name: str) -> Path:
    return root / name


def test_failed_lifecycle_preserves_the_same_progress_envelope(
    tmp_path: Path,
) -> None:
    lifecycle = TrainingRunLifecycle(
        tmp_path,
        execution_mode="tw_minute",
        run_mode="train",
        strategy="none",
        model_name="transformer_base_portfolio",
    )
    lifecycle.start(fold_ids=[])
    lifecycle.fail(RuntimeError("dataset contract failed"))

    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["state"] == "failed"
    assert progress["phase"] == "failed"
    assert progress["failure"] == {
        "type": "RuntimeError",
        "message": "dataset contract failed",
    }


def test_mode_artifact_core_is_shared_across_frequencies() -> None:
    minute = canonical_mode_artifact_contract("tw_minute")
    tick = canonical_mode_artifact_contract("tw_index_derivatives_tick")
    daily = canonical_mode_artifact_contract("tw_day_trade")
    stable_keys = {
        "schema_version",
        "artifact_layout_version",
        "execution_mode",
        "product_family",
        "frequency",
        "decision_clock",
        "execution_clock",
        "recurrent_state_scope",
        "terminal_policy",
        "split_ownership",
        "sample_order_contract",
        "benchmark_contract",
        "weight_snapshot_contract",
        "turnover_contract",
    }
    assert stable_keys <= minute.keys()
    assert stable_keys <= tick.keys()
    assert stable_keys <= daily.keys()


def test_lifecycle_persists_explicit_execution_variant_contract(tmp_path: Path) -> None:
    lifecycle = TrainingRunLifecycle(
        tmp_path,
        execution_mode="tw_day_trade",
        run_mode="train",
        strategy="none",
        model_name="financial_transformer",
    )
    lifecycle.start(
        fold_ids=[1],
        mode_contract_overrides={
            "frequency": "daily_policy_exact_minute_execution",
            "execution_clock": "official_open_sizing_then_minute_event_tape",
            "sample_order_contract": "strict_chronological_sessions",
        },
    )

    manifest = json.loads((tmp_path / "run_manifest.json").read_text())
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert manifest["execution_mode"] == "tw_day_trade"
    assert manifest["frequency"] == "daily_policy_exact_minute_execution"
    assert manifest["execution_clock"] == (
        "official_open_sizing_then_minute_event_tape"
    )
    assert manifest["sample_order_contract"] == "strict_chronological_sessions"
    assert progress["frequency"] == "daily_policy_exact_minute_execution"
    assert progress["sample_order_contract"] == "strict_chronological_sessions"


def test_lifecycle_rejects_unknown_mode_contract_override(tmp_path: Path) -> None:
    lifecycle = TrainingRunLifecycle(
        tmp_path,
        execution_mode="tw_day_trade",
        run_mode="train",
        strategy="none",
        model_name="financial_transformer",
    )
    with pytest.raises(ValueError, match="unsupported lifecycle mode-contract"):
        lifecycle.start(
            fold_ids=[1],
            mode_contract_overrides={"execution_mode": "tw_minute"},
        )


def test_epoch_curve_core_is_flat_and_mode_extensions_are_preserved() -> None:
    row = normalize_epoch_curve_record(
        {"epoch": 2, "train_loss": 0.3, "minute_cache_gib": 1.5}
    )

    assert row["schema_version"] == 1
    assert row["epoch"] == 2
    assert row["train_loss"] == 0.3
    assert row["val_mean"] is None
    assert row["test_mean"] is None
    assert row["best_val_loss"] is None
    assert row["minute_cache_gib"] == 1.5
    with pytest.raises(ValueError, match="require an epoch"):
        normalize_epoch_curve_record({"train_loss": 0.1})


def test_completed_artifact_conformance_reports_exact_missing_path(
    tmp_path: Path,
) -> None:
    layout = TrainingArtifactLayout(tmp_path)
    group_name = "train_2020-2021"
    lifecycle = TrainingRunLifecycle(
        tmp_path,
        execution_mode="tw_minute",
        run_mode="train",
        strategy="none",
        model_name="transformer_base_portfolio",
    )
    lifecycle.start(fold_ids=[1])
    lifecycle.start_group(
        group_name=group_name,
        group_index=1,
        group_total=1,
        fold_ids=[1],
        epoch_total=1,
    )
    lifecycle.finish_fold(1)
    _write_minimal_completed_artifacts(
        layout,
        group_name=group_name,
        fold_id=1,
        execution_mode="tw_minute",
    )
    lifecycle.complete(fold_ids=[1])

    valid = validate_completed_training_artifacts(
        tmp_path,
        fold_ids=[1],
        group_names=[group_name],
    )
    assert valid.ok
    assert not valid.missing

    model_path = layout.fold_dir(1) / "model.pt"
    model_path.write_bytes(b"")
    empty = validate_completed_training_artifacts(
        tmp_path,
        fold_ids=[1],
        group_names=[group_name],
    )
    assert any(
        str(model_path) in issue and "required artifact is empty" in issue
        for issue in empty.invalid
    )
    model_path.write_bytes(b"artifact")

    curve_path = layout.group_curve_path(group_name)
    valid_curve = curve_path.read_text()
    curve_path.write_text(
        valid_curve + json.dumps({"schema_version": 1, "epoch": 2}) + "\n"
    )
    corrupt_curve = validate_completed_training_artifacts(
        tmp_path,
        fold_ids=[1],
        group_names=[group_name],
    )
    assert any("row 2 missing core fields" in issue for issue in corrupt_curve.invalid)
    curve_path.write_text(valid_curve)

    missing_path = layout.fold_dir(1) / "metrics.json"
    missing_path.unlink()
    invalid = validate_completed_training_artifacts(
        tmp_path,
        fold_ids=[1],
        group_names=[group_name],
    )
    assert invalid.missing == (missing_path,)
    with pytest.raises(RuntimeError, match="metrics.json"):
        invalid.require()


def test_lifecycle_completion_gate_rejects_incomplete_artifacts(
    tmp_path: Path,
) -> None:
    layout = TrainingArtifactLayout(tmp_path)
    lifecycle = TrainingRunLifecycle(
        tmp_path,
        execution_mode="tw_minute",
        run_mode="train",
        strategy="none",
        model_name="transformer_base_portfolio",
    )
    lifecycle.start(fold_ids=[1])
    lifecycle.start_group(
        group_name="train_2020-2021",
        group_index=1,
        group_total=1,
        fold_ids=[1],
        epoch_total=1,
    )
    lifecycle.finish_fold(1)

    with pytest.raises(RuntimeError, match="training artifact contract"):
        lifecycle.complete(fold_ids=[1])

    progress = json.loads(layout.progress_path.read_text())
    assert progress["state"] == "failed"
    assert progress["phase"] == "failed"


def test_lifecycle_rejects_partial_selected_fold_completion(tmp_path: Path) -> None:
    lifecycle = TrainingRunLifecycle(
        tmp_path,
        execution_mode="tw_minute",
        run_mode="train",
        strategy="none",
        model_name="transformer_base_portfolio",
    )
    lifecycle.start(fold_ids=[1, 2])

    with pytest.raises(RuntimeError, match="partial fold coverage"):
        lifecycle.complete(fold_ids=[1])

    progress = json.loads((tmp_path / "progress.json").read_text())
    assert progress["state"] == "failed"
    assert progress["fold"]["selected_ids"] == [1, 2]
