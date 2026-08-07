import json
from pathlib import Path

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
    required = [
        layout.run_manifest_path,
        layout.progress_path,
        layout.summary_path,
        layout.group_checkpoint_path(group_name),
        layout.group_pre_epoch_timing_path(group_name),
        layout.group_curve_path(group_name),
        *layout.required_fold_paths(1),
    ]
    for path in required:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    layout.run_manifest_path.write_text(
        json.dumps(
            {
                "lifecycle_schema_version": TRAINING_LIFECYCLE_SCHEMA_VERSION,
                "artifact_layout_version": TRAINING_ARTIFACT_LAYOUT_VERSION,
            }
        )
    )
    layout.progress_path.write_text(
        json.dumps(
            {
                "schema_version": TRAINING_PROGRESS_SCHEMA_VERSION,
                "state": "complete",
                "phase": "complete",
            }
        )
    )
    layout.summary_path.write_text("[]")
    layout.group_curve_path(group_name).write_text(
        json.dumps({"schema_version": 1, "epoch": 1}) + "\n"
    )
    (layout.fold_dir(1) / "mode_artifact_contract.json").write_text(
        json.dumps({"artifact_layout_version": TRAINING_ARTIFACT_LAYOUT_VERSION})
    )
    layout.fold_complete_path(1).write_text(
        json.dumps({"status": "complete"})
    )

    valid = validate_completed_training_artifacts(
        tmp_path,
        fold_ids=[1],
        group_names=[group_name],
    )
    assert valid.ok
    assert not valid.missing

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
