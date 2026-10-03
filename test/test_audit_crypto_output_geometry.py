"""The bounded audit must fail before reading models for unsafe requests."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from stockagent.config import load_config


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/audit_crypto_output_geometry.py"
CONTROL = ROOT / "configs/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v6.yaml"


def _invoke(*arguments: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, arguments)],
        cwd=ROOT, capture_output=True, text=True, timeout=30, check=False,
    )


@pytest.mark.parametrize("location", ["training", "parquet", "panel", "external", "outside", "unrelated_audit"])
def test_audit_rejects_protected_or_unrelated_output(location: str, tmp_path: Path) -> None:
    config = load_config(CONTROL)
    targets = {
        "training": Path(config.runner.output_dir) / "fold_01/metrics.json",
        "parquet": Path(config.data.parquet_root) / "manifest.json",
        "panel": Path(config.data.panel_cache_root) / "panel_cache_v2/meta.json",
        "external": Path(config.data.external_feature_path).parent / "receipt.json",
        "outside": tmp_path / "output_mechanism.json",
        "unrelated_audit": ROOT / "artifacts/markets/bybit_perpetual_daily_0005_v6_training_audit/performance.json",
    }
    completed = _invoke("--config", CONTROL, "--output", targets[location])
    assert completed.returncode == 2
    assert "--output must be" in completed.stderr
    assert "torch.load" not in completed.stderr


def _fixture_config(tmp_path: Path, selected: list[int]) -> tuple[Path, Path]:
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    (artifact_root / "run_manifest.json").write_text(json.dumps({
        "model_name": "financial_transformer",
        "execution_mode": "crypto_perpetual",
        "configuration": {"training": {"financial_transformer": {"portfolio_output_mode": "learned_cash"}}},
        "selected_fold_ids": selected,
    }))
    config_path = tmp_path / "audit.yaml"
    config_path.write_text(yaml.safe_dump({
        "base_config": str(CONTROL), "runner": {"output_dir": str(artifact_root)},
    }))
    return config_path, artifact_root


@pytest.mark.parametrize("selected", [[], [1, 1], [0], [True]])
def test_audit_rejects_invalid_manifest_fold_selection(tmp_path: Path, selected: list[int]) -> None:
    config_path, _ = _fixture_config(tmp_path, selected)
    completed = _invoke("--config", config_path, "--stdout")
    assert completed.returncode == 2
    assert "selected_fold_ids" in completed.stderr


def test_audit_does_not_silently_scan_only_present_folds(tmp_path: Path) -> None:
    config_path, artifact_root = _fixture_config(tmp_path, [1, 2])
    first_fold = artifact_root / "fold_01"
    first_fold.mkdir()
    for name in ("checkpoint_best.pt", "causal_feature_rms_normalization.json", "temporal_basis_selection.json", "fold_complete.json"):
        (first_fold / name).write_bytes(b"not loaded: all declared folds are checked first")
    completed = _invoke("--config", config_path, "--stdout")
    assert completed.returncode == 2
    assert "selected folds are incomplete" in completed.stderr
    assert "fold_02/checkpoint_best.pt" in completed.stderr
    assert "torch.load" not in completed.stderr


def test_audit_rejects_non_learned_cash_candidate_before_model_loading() -> None:
    candidate = ROOT / "configs/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v7.yaml"
    completed = _invoke("--config", candidate, "--stdout")
    assert completed.returncode == 2
    assert "requires a crypto FinancialTransformer learned_cash policy" in completed.stderr
