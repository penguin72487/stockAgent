"""Full-feature, foreground entry delegates compatible resume to train.py."""
from dataclasses import asdict, replace
import json
from pathlib import Path
import re
import subprocess
import sys

import pytest
import yaml

import train
from scripts import manage_gpu_jobs
from stockagent import config as config_module
from stockagent import runtime_identity
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
    raw = config_module._deep_merge_config(raw, {
        "data": {"factorized_feature_manifest": str(feature_path)},
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


def _run_entry(monkeypatch, experiment, *, check_only=False, busy=False, launches=None):
    baseline, candidate, _, _ = experiment
    spec = {"source_receipt": "unused/release.json", "source_sha256": "source",
            "config_sha256": {}, "baseline_config": str(BASE)}
    original = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **kw: json.dumps(spec)
                        if path == ROOT / "run-spec.json" else original(path, *a, **kw))
    monkeypatch.setattr(config_module, "load_config", lambda path: baseline if Path(path) == BASE else candidate)
    monkeypatch.setattr(runtime_identity, "verify_source_release", lambda *a: {"source_sha256": "source"})
    monkeypatch.setattr(runtime_identity, "verify_release_bundles", lambda *a: None)
    monkeypatch.setattr(manage_gpu_jobs, "_busy_gpu_indices", lambda: {0} if busy else set())
    if launches is None:
        launches = []

    def leased(command, cards, *, sharing):
        launches.append((command, cards, sharing))
        return command

    monkeypatch.setattr(manage_gpu_jobs, "_gpu_lease_command", leased)
    monkeypatch.setattr(subprocess, "run", lambda command: subprocess.CompletedProcess(command, 0))
    monkeypatch.setattr(sys, "argv", ["-", str(ROOT), str(int(check_only))])
    blocks = re.findall(r"run_fintech_python - .*? <<'PY'\n(.*?)\nPY", ENTRY.read_text(), re.S)
    assert len(blocks) == 1
    exec(compile(blocks[0], str(ENTRY), "exec"), {})


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
    assert Path(args.config) == ROOT / "config.yaml" and args.output_dir is None


def test_entry_check_only_reports_expected_not_attached_width(monkeypatch, experiment, capsys):
    calls = []
    with pytest.raises(SystemExit) as result:
        _run_entry(monkeypatch, experiment, check_only=True, launches=calls)
    assert result.value.code == 0 and calls == []
    assert "expected model input=14726" in capsys.readouterr().out


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
