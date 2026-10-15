"""Full-feature, foreground entry delegates compatible resume to train.py."""
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

import train
from scripts import run_relocated_training as entry
from stockagent import config as config_module
from stockagent.data.factorized_panel import VALUE_ONLY_CONTRACT


REPO = Path(__file__).resolve().parents[1]
ENTRY = REPO / "scripts/run_tw_no_basis_scale_separated_cash_b128_vastai1t.sh"
ROOT = Path("/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_scale_separated_cash_v1")
BASE = Path("/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_v1/config.yaml")
DEPLOYMENT = REPO / "configs/deployments/tw_day_trade_factorized_values_20261006_no_basis_scale_separated_cash_b128_v1.yaml"


@pytest.fixture
def experiment(tmp_path):
    # Exercise the maintained deployment's real override through load_config,
    # with a materialized fixture base instead of skipping on non-Vast hosts.
    raw = config_module._load_raw_config(REPO / "configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_no_basis_bf16_v2.yaml")
    feature_path = tmp_path / "factorized_manifest.json"
    for name in ("physical", "panel", "public"):
        (tmp_path / name).mkdir()
    raw = config_module._deep_merge_config(raw, {
        "data": {"factorized_feature_manifest": str(feature_path),
                 "day_trade_minute_execution_cache_dir": str(tmp_path / "physical"),
                 "tw_public_feature_path": str(tmp_path / "public"),
                 "panel_cache_root": str(tmp_path / "panel")},
        "environment": {"amp_dtype": "bf16", "use_tensor_cores": True, "cpu_threads": 8},
        "training": {"batch_size_train": 128, "epochs": 1000},
        "runner": {"output_dir": str(BASE.parent / "training-bf16"), "resume": True,
                   "isolate_train_folds": False},
    })
    base_path = tmp_path / "base.yaml"
    base_path.write_text(yaml.safe_dump(raw))
    override = yaml.safe_load(DEPLOYMENT.read_text())
    assert Path(override["base_config"]) == BASE
    override["base_config"] = str(base_path)
    override["runner"]["output_dir"] = str(tmp_path / "formal")
    candidate_path = tmp_path / "candidate.yaml"
    candidate_path.write_text(yaml.safe_dump(override))
    baseline = config_module.load_config(base_path)
    candidate = config_module.load_config(candidate_path)
    manifest = {
        "contract": VALUE_ONLY_CONTRACT, "status": "complete",
        "model_channel_policy": "value_only",
        "base_feature_names": baseline.data.feature_include,
        "individual_channels": [f"asset_{i}" for i in range(14655)],
        "common_channels": [f"common_{i}" for i in range(65)],
        "logical_model_channels": 14726, "value_features": 14720,
    }
    feature_path.write_text(json.dumps(manifest))
    return baseline, candidate, manifest, feature_path


def test_factorized_scale_separated_cash_preserves_baseline_settings(experiment):
    baseline, candidate, _, _ = experiment
    expected = asdict(baseline)
    for alias in ("financial_transformer", "executable_portfolio_transformer"):
        expected["training"][alias]["portfolio_output_mode"] = "score_entmax_scale_separated_cash"
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    assert asdict(candidate) == expected
    assert candidate.runner.output_dir != baseline.runner.output_dir
    assert candidate.training.pretrained_initialization_root is None
    assert candidate.runner.resume and candidate.training.batch_size_train == 128
    assert not candidate.training.day_trade_sparse_events
    assert not candidate.training.financial_transformer.temporal_basis_families
    assert candidate.training.financial_transformer.feature_svd_components == 0


def _run_entry(monkeypatch, experiment, *, check_only=False, busy=False, launches=None, annual=False,
               stream_fault=False):
    baseline, candidate, _, _ = experiment
    fixture_root = Path(candidate.data.panel_cache_root).parent
    if annual:
        chosen = config_module.load_config(REPO / "configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml")
        candidate = replace(chosen, data=replace(candidate.data, factorized_host_cache_bytes=48 * 1024**3),
                            runner=replace(chosen.runner, output_dir=candidate.runner.output_dir))
    config = fixture_root / "launch-config.yaml"
    config.write_text(yaml.safe_dump(asdict(candidate)))
    original = fixture_root / "source-manifest.json"
    original.write_text(json.dumps({"configuration": asdict(candidate)}))
    # Keep this fixture's original-run contract: a resume root must exist.
    output = Path(candidate.runner.output_dir)
    output.mkdir(exist_ok=True)
    if not annual:
        (output / "run_manifest.json").write_text("{}")
    spec = {"state": "ready", "source_receipt": str(REPO / "release.json"), "source_sha256": "source",
            "config": str(config), "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
            "source_manifest": str(original), "source_manifest_sha256": hashlib.sha256(original.read_bytes()).hexdigest(),
            "output_root": str(output), "transform_cache": str(fixture_root / "transform"), "fold_ids": [11]}
    if annual:
        from types import SimpleNamespace
        from stockagent.training import lifecycle
        result = fixture_root / "accepted-result.json"
        result.write_text(json.dumps({"ok": True, "return_code": 0, "lifecycle_ok": True,
            "world_size": 2, "batch_size": candidate.training.batch_size_train,
            "local_batch_size": candidate.training.batch_size_train // 2, "steady_epochs": [3],
            "failure_patterns": [], "memory": {"min_headroom_gib": 4},
            "host_memory": {"min_headroom_gib": 24}}))
        proof = {"contract": entry.ANNUAL_LAUNCH_CONTRACT, "state": "accepted",
            "source_sha256": "source", "config_sha256": spec["config_sha256"], "fold_ids": [10],
            "context_years": [2014], "train_years": list(range(2015,2025)), "val_years": [2025], "test_years": [2026],
            "feature_manifest": candidate.data.factorized_feature_manifest,
            "feature_manifest_sha256": hashlib.sha256(Path(candidate.data.factorized_feature_manifest).read_bytes()).hexdigest(),
            "result": str(result), "result_sha256": hashlib.sha256(result.read_bytes()).hexdigest(),
            "batch_size": candidate.training.batch_size_train, "host_cache_bytes": 48*1024**3,
            "stream_chunk_bytes": candidate.training.financial_transformer.factorized_compact_stream_chunk_bytes + int(stream_fault),
            "transport_block_rows": 16, "engineering_optimizer_donated": False,
            "artifact_root": str(fixture_root / "engineering-evidence")}
        proof_path = fixture_root / "acceptance.json"
        proof_path.write_text(json.dumps(proof))
        spec.update(contract=entry.ANNUAL_LAUNCH_CONTRACT, fold_ids=[10], acceptance_receipt=str(proof_path),
                    acceptance_sha256=hashlib.sha256(proof_path.read_bytes()).hexdigest())
        def accepted(root, **kwargs):
            assert root == Path(proof["artifact_root"]) and kwargs["fold_ids"] == [10]
            return SimpleNamespace(ok=True)
        monkeypatch.setattr(lifecycle, "validate_completed_training_artifacts", accepted)
    launch = fixture_root / "launch.json"
    launch.write_text(json.dumps(spec))
    monkeypatch.setattr(config_module, "load_config", lambda path: candidate)
    monkeypatch.setattr(entry, "verify_source_release", lambda *a: {"source_sha256": "source"})
    monkeypatch.setattr(entry, "verify_release_bundles", lambda *a: None)
    monkeypatch.setattr(entry, "_busy_gpu_indices", lambda: {0} if busy else set())
    monkeypatch.setattr(entry, "renew_original_source_leases", lambda *a: None)
    if launches is None:
        launches = []

    def leased(command, cards, *, sharing):
        launches.append((command, cards, sharing))
        return command

    monkeypatch.setattr(entry, "_gpu_lease_command", leased)

    def run(command, **kwargs):
        if command[1] == "-c":
            monkeypatch.setattr(sys, "argv", command[2:])
            exec(compile(command[2], "frozen-config-preflight", "exec"), {})
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(entry.subprocess, "run", run)
    monkeypatch.setattr(sys, "argv", [str(ENTRY), "--launch", str(launch), *(["--check-only"] if check_only else [])])
    assert "scripts/run_relocated_training.py" in ENTRY.read_text()
    raise SystemExit(entry.main())


def test_entry_foreground_resume_fold11_dual_card_no_profile(monkeypatch, experiment):
    calls = []
    with pytest.raises(SystemExit) as result:
        _run_entry(monkeypatch, experiment, launches=calls)
    assert result.value.code == 0 and len(calls) == 1
    command, cards, sharing = calls[0]
    assert cards == [0, 1] and sharing is False
    monkeypatch.setattr(sys, "argv", command[1:])
    args = train.parse_args()
    assert args.resume and not args.retrain_completed_folds
    assert not args.profile_timing and not args.debug_timing_sync and not args.isolate_train_folds
    assert args.start_fold == 11 and args.max_folds == 1 and args.torch_compile_threads == 16
    assert Path(args.config).name == "launch-config.yaml" and args.output_dir is None


def test_entry_check_only_reports_expected_not_attached_width(monkeypatch, experiment, capsys):
    calls = []
    with pytest.raises(SystemExit) as result:
        _run_entry(monkeypatch, experiment, check_only=True, launches=calls)
    assert result.value.code == 0 and calls == []
    assert '"expected_model_input": 14726' in capsys.readouterr().out


@pytest.mark.parametrize("fault", ["empty_manifest", "missing_file", "six_only", "incomplete", "bad_count", "bad_base"])
def test_entry_rejects_missing_full_features_before_launch(monkeypatch, experiment, fault):
    calls = []
    baseline, candidate, manifest, path = experiment
    if fault in {"empty_manifest", "missing_file"}:
        data = replace(baseline.data, factorized_feature_manifest="" if fault == "empty_manifest" else str(path.parent / "missing.json"))
        experiment = (replace(baseline, data=data), replace(candidate, data=data), manifest, path)
    else:
        if fault == "six_only":
            manifest["individual_channels"] = []
            manifest["common_channels"] = []
        elif fault == "incomplete":
            manifest["status"] = "building"
        elif fault == "bad_count":
            manifest["logical_model_channels"] = 6
        else:
            manifest["base_feature_names"] = ["wrong"] * 6
        path.write_text(json.dumps(manifest))
    with pytest.raises((AssertionError, FileNotFoundError)):
        _run_entry(monkeypatch, experiment, launches=calls)
    assert calls == []


def test_entry_rejects_gpu_owner(monkeypatch, experiment):
    with pytest.raises(RuntimeError, match="GPU 0/1"):
        _run_entry(monkeypatch, experiment, busy=True)


def test_accepted_annual_entry_initializes_new_root_with_canonical_resume(monkeypatch, experiment):
    calls = []
    with pytest.raises(SystemExit) as result:
        _run_entry(monkeypatch, experiment, launches=calls, annual=True)
    assert result.value.code == 0 and len(calls) == 1
    command, cards, sharing = calls[0]
    monkeypatch.setattr(sys, "argv", command[1:])
    args = train.parse_args()
    assert args.start_fold == 10 and args.max_folds == 1 and args.resume
    assert not args.profile_timing and not args.debug_timing_sync and cards == [0, 1] and not sharing
    assert not (Path(experiment[1].runner.output_dir) / "run_manifest.json").exists()


def test_annual_entry_rejects_unmeasured_stock_stream_budget(monkeypatch, experiment):
    calls = []
    with pytest.raises(AssertionError, match="stock stream budget"):
        _run_entry(monkeypatch, experiment, launches=calls, annual=True, stream_fault=True)
    assert not calls
