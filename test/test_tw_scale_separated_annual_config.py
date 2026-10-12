"""The explicitly selected annual contract owns complete years, not lookback."""
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json

import numpy as np
import pytest

from stockagent.config import load_config
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.training.trainer import _validate_annual_day_trade_training_boundaries


CONFIG = Path("configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml")


def test_annual_scale_separated_config_keeps_full_model_and_fresh_resume_root():
    c = load_config(CONFIG)
    m = c.training.financial_transformer
    assert c.training.day_trade_training_annual_episodes
    assert not c.training.day_trade_sub_lot_recovery
    assert not c.training.day_trade_sparse_events
    assert c.training.pretrained_initialization_root is None
    assert c.training.multi_gpu_strategy == "distributed_data_parallel"
    assert c.training.batch_size_train == 256
    assert c.data.factorized_host_cache_bytes == 48 * 1024**3
    assert c.environment.amp_dtype == "bf16" and c.environment.use_tensor_cores
    assert m.portfolio_output_mode == "score_entmax_scale_separated_cash"
    assert m.factorized_compact_projection
    assert m.factorized_compact_stream_chunk_bytes == 2 * 1024**3
    assert not m.temporal_basis_families and m.feature_svd_components == 0
    assert m.feature_bottleneck_dim == 0 and m.d_model == 32
    assert c.runner.resume and c.runner.require_cuda and c.runner.start_fold == 10
    assert "20261007_no_basis_scale_separated_cash_annual_v1" in c.runner.output_dir
    assert c.walk_forward.split_start_year == 2015
    assert c.walk_forward.lookback_context == "panel_history"
    assert c.walk_forward.require_future_test_year


def test_annual_selected_fold_uses_2014_only_as_context_and_keeps_2025_2026():
    c = load_config(CONFIG)
    # Synthetic calendar only: this validates ownership, not official coverage.
    dates = np.arange("2014-01-06", "2026-10-03", dtype="datetime64[D]")
    folds = build_expanding_year_folds(dates, min_train_years=c.walk_forward.min_train_years,
        val_years=c.walk_forward.val_years, require_future_test_year=True, split_start_year=2015)
    fold = next(f for f in folds if f.fold_id == 10)
    assert fold.train_years == list(range(2015, 2025))
    assert fold.val_years == [2025] and fold.test_years == [2026]
    report = _validate_annual_day_trade_training_boundaries(SimpleNamespace(dates=dates), [fold], c)
    assert report is not None
    assert dates[fold.train_indices[0]] == np.datetime64("2015-01-01")


def test_annual_all_folds_keep_complete_year_ownership():
    c = load_config(CONFIG)
    dates = np.arange("2014-01-06", "2026-10-03", dtype="datetime64[D]")
    folds = build_expanding_year_folds(dates, min_train_years=c.walk_forward.min_train_years,
        val_years=c.walk_forward.val_years, require_future_test_year=True, split_start_year=2015)
    assert [fold.fold_id for fold in folds] == list(range(1, 11))
    for fold in folds:
        assert fold.train_years == list(range(2015, 2015 + fold.fold_id))
        assert fold.val_years == [2015 + fold.fold_id]
        assert fold.test_years == list(range(2016 + fold.fold_id, 2027))
        assert dates[fold.train_indices[0]] == np.datetime64("2015-01-01")
    assert _validate_annual_day_trade_training_boundaries(SimpleNamespace(dates=dates), folds, c)


@pytest.mark.parametrize("start,limit,expected_start,expected_limit", [
    (None, None, 10, 1), (1, None, 1, None), (1, 10, 1, 10), (4, None, 4, None),
    (10, 1, 10, 1), (None, 2, 10, 2),
])
def test_annual_launcher_uses_canonical_fold_selection_and_resume(start, limit, expected_start, expected_limit):
    from scripts import run_relocated_training as entry
    launch = {"contract": entry.ANNUAL_LAUNCH_CONTRACT, "fold_ids": [10], "config": "/fixed/config.yaml"}
    command = entry._training_command(launch, start_fold=start, max_folds=limit)
    assert command[1] == "train.py"
    assert command[command.index("--config") + 1] == launch["config"]
    assert command[command.index("--start-fold") + 1] == str(expected_start)
    if expected_limit is None:
        assert "--max-folds" not in command
    else:
        assert command[command.index("--max-folds") + 1] == str(expected_limit)
    assert "--resume" in command and "--no-retrain-completed-folds" in command
    assert "--no-profile-timing" in command and "--profile-timing" not in command
    assert "--no-isolate-train-folds" in command
    assert launch["fold_ids"] == [10]  # Measured acceptance is not rewritten.


@pytest.mark.parametrize("start,limit", [(0, None), (-1, None), (1, 0), (None, -1)])
def test_annual_launcher_rejects_invalid_fold_ranges(start, limit):
    from scripts import run_relocated_training as entry
    launch = {"contract": entry.ANNUAL_LAUNCH_CONTRACT, "fold_ids": [10], "config": "/fixed/config.yaml"}
    with pytest.raises(ValueError):
        entry._training_command(launch, start_fold=start, max_folds=limit)


def test_original_fold11_launcher_remains_pinned():
    from scripts import run_relocated_training as entry
    launch = {"fold_ids": [11], "config": "/fixed/config.yaml"}
    command = entry._training_command(launch)
    assert command[command.index("--start-fold") + 1] == "11"
    assert command[command.index("--max-folds") + 1] == "1"
    with pytest.raises(ValueError, match="annual launch"):
        entry._training_command(launch, start_fold=1)


@pytest.mark.parametrize("edit,accepted", [
    (None, None), ("start-fold", 1), ("invalid-fold", False), ("batch", False),
    ("resume", False), ("output", False), ("start-fold-and-batch", False),
    ("non-annual", False),
])
def test_annual_config_identity_allows_only_fold_selector_edit(tmp_path, edit, accepted):
    from scripts import run_relocated_training as entry
    original = ("runner:\n  start_fold: 10\n  resume: true\n  output_dir: /fixed/formal\n"
                "training:\n  batch_size_train: 256\n")
    selected = original
    if edit in {"start-fold", "start-fold-and-batch", "non-annual"}:
        selected = selected.replace("start_fold: 10", "start_fold: 1")
    if edit == "invalid-fold": selected = selected.replace("start_fold: 10", "start_fold: 0")
    if edit in {"batch", "start-fold-and-batch"}: selected = selected.replace("batch_size_train: 256", "batch_size_train: 512")
    if edit == "resume": selected = selected.replace("resume: true", "resume: false")
    if edit == "output": selected = selected.replace("/fixed/formal", "/other/formal")
    config = tmp_path / "config.yaml"
    config.write_text(selected)
    launch = {"contract": entry.ANNUAL_LAUNCH_CONTRACT if edit != "non-annual" else None,
              "fold_ids": [10], "config": str(config),
              "config_sha256": hashlib.sha256(original.encode()).hexdigest()}
    if accepted is False:
        with pytest.raises(AssertionError, match="config"):
            entry._verify_launch_config(launch)
    else:
        assert entry._verify_launch_config(launch) == accepted
    assert config.read_text() == selected  # Never overwrite the user's selector.


@pytest.mark.parametrize("fault", [None, "receipt_sha", "config", "source", "fold", "feature", "result_sha",
                                  "lifecycle", "single_gpu", "batch", "warmup_only", "fallback", "vram", "ram", "optimizer"])
def test_new_annual_launch_requires_selected_complete_ddp_evidence(tmp_path, fault):
    from scripts import run_relocated_training as entry
    feature = tmp_path / "feature.json"; feature.write_text("{}")
    result_path = tmp_path / "result.json"
    result = {"ok": True, "return_code": 0, "lifecycle_ok": True, "world_size": 2,
              "batch_size": 256, "local_batch_size": 128, "steady_epochs": [3], "failure_patterns": [],
              "memory": {"min_headroom_gib": 4}, "host_memory": {"min_headroom_gib": 24}}
    if fault in {"lifecycle", "single_gpu", "batch", "warmup_only", "fallback", "vram", "ram"}:
        if fault == "lifecycle": result["lifecycle_ok"] = False
        if fault == "single_gpu": result["world_size"] = 1
        if fault == "batch": result["batch_size"] = 128
        if fault == "warmup_only": result["steady_epochs"] = [1]
        if fault == "fallback": result["failure_patterns"] = ["hit config.recompile_limit"]
        if fault == "vram": result["memory"]["min_headroom_gib"] = 1
        if fault == "ram": result["host_memory"]["min_headroom_gib"] = 1
    result_path.write_text(json.dumps(result))
    proof = {"contract": entry.ANNUAL_LAUNCH_CONTRACT, "state": "accepted", "source_sha256": "source",
             "config_sha256": "config", "fold_ids": [10], "context_years": [2014],
             "train_years": list(range(2015,2025)), "val_years": [2025], "test_years": [2026],
             "feature_manifest": str(feature), "feature_manifest_sha256": hashlib.sha256(feature.read_bytes()).hexdigest(),
             "result": str(result_path), "result_sha256": hashlib.sha256(result_path.read_bytes()).hexdigest(),
             "batch_size": 256, "engineering_optimizer_donated": fault == "optimizer"}
    path = tmp_path / "acceptance.json"
    if fault in {"config", "source"}: proof[fault+"_sha256"] = "different"
    if fault == "fold": proof["fold_ids"] = [11]
    if fault == "feature": proof["feature_manifest_sha256"] = "wrong"
    if fault == "result_sha": proof["result_sha256"] = "wrong"
    path.write_text(json.dumps(proof))
    launch = {"contract": entry.ANNUAL_LAUNCH_CONTRACT, "source_sha256": "source", "config_sha256": "config",
              "fold_ids": [10], "acceptance_receipt": str(path), "acceptance_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    if fault == "receipt_sha": launch["acceptance_sha256"] = "wrong"
    if fault:
        with pytest.raises(AssertionError): entry._annual_acceptance(launch)
    else:
        assert entry._annual_acceptance(launch) == proof
