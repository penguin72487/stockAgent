from __future__ import annotations

import json
from pathlib import Path

import pytest

from stockagent.live.market_config import load_market_config
from stockagent.live.model_deployment import (
    attempt_model_deployment,
    discover_model_candidate,
    load_deployment,
)


def _complete_fold(root: Path, fold_id: int) -> Path:
    fold = root / f"fold_{fold_id}"
    fold.mkdir(parents=True)
    (fold / "checkpoint_best.pt").write_bytes(f"checkpoint-{fold_id}".encode())
    (fold / "daily_weights.parquet").write_bytes(b"weights")
    (fold / "metrics.json").write_text("{}", encoding="utf-8")
    (fold / "fold_complete.json").write_text(
        json.dumps({"status": "complete", "fold_id": fold_id}),
        encoding="utf-8",
    )
    return fold


def test_discover_model_candidate_uses_latest_complete_fold(tmp_path: Path) -> None:
    artifact = tmp_path / "candidate"
    config = tmp_path / "market.yaml"
    config.write_text("market: tw\n", encoding="utf-8")
    _complete_fold(artifact, 20)
    latest = _complete_fold(artifact, 21)
    incomplete = artifact / "fold_22"
    incomplete.mkdir()
    (incomplete / "checkpoint_best.pt").write_bytes(b"partial")

    candidate = discover_model_candidate(
        [str(artifact)],
        candidate_configs=[str(config)],
        root=tmp_path,
    )

    assert candidate is not None
    assert candidate.fold_id == 21
    assert Path(candidate.checkpoint_path) == latest / "checkpoint_best.pt"
    assert Path(candidate.config_path) == config


def test_failed_smoke_keeps_active_deployment_and_is_not_retried(tmp_path: Path) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    config = tmp_path / "market.yaml"
    manifest = tmp_path / "deployment.json"
    config.write_text("market: tw\n", encoding="utf-8")
    _complete_fold(first_root, 1)
    _complete_fold(second_root, 2)

    status, first = attempt_model_deployment(
        market="tw",
        candidate_roots=[str(first_root)],
        candidate_configs=[str(config)],
        manifest_path=manifest,
        root=tmp_path,
        smoke_test=lambda candidate: {"panel_date": "2026-07-17"},
    )
    assert status == "promoted"
    assert load_deployment(manifest, root=tmp_path) == first

    with pytest.raises(RuntimeError, match="incompatible"):
        attempt_model_deployment(
            market="tw",
            candidate_roots=[str(second_root)],
            candidate_configs=[str(config)],
            manifest_path=manifest,
            root=tmp_path,
            smoke_test=lambda candidate: (_ for _ in ()).throw(RuntimeError("incompatible")),
        )
    assert load_deployment(manifest, root=tmp_path) == first

    status, _ = attempt_model_deployment(
        market="tw",
        candidate_roots=[str(second_root)],
        candidate_configs=[str(config)],
        manifest_path=manifest,
        root=tmp_path,
        smoke_test=lambda candidate: pytest.fail("known failed candidate retried"),
    )
    assert status == "known_failed"


def test_candidate_roots_and_configs_must_match(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="equal length"):
        discover_model_candidate(["one"], candidate_configs=[], root=tmp_path)


def test_manual_model_selection_pins_requested_fold(tmp_path: Path) -> None:
    models = tmp_path / "models"
    markets = tmp_path / "markets"
    models.mkdir()
    markets.mkdir()
    (models / "tw.yaml").write_text(
        "\n".join(
            (
                "mode: manual",
                "config_path: configs/candidate.yaml",
                "output_dir: artifacts/candidate",
                "fold_id: 20",
                "checkpoint_path: artifacts/candidate/fold_20/checkpoint_best.pt",
                "weights_path: artifacts/candidate/fold_20/daily_weights.parquet",
                "model_scoped_live_output: true",
            )
        ),
        encoding="utf-8",
    )
    market_path = markets / "tw.yaml"
    market_path.write_text(
        "\n".join(
            (
                "market: tw",
                "label: TW",
                "config_path: configs/fallback.yaml",
                "model_selection_path: ../models/tw.yaml",
                "model_auto_deploy: true",
            )
        ),
        encoding="utf-8",
    )

    cfg = load_market_config(market_path)

    assert cfg.model_auto_deploy is False
    assert cfg.config_path == "configs/candidate.yaml"
    assert cfg.output_dir == "artifacts/candidate"
    assert cfg.fold_id == 20
    assert cfg.model_scoped_live_output is True
