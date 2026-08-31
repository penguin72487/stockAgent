from __future__ import annotations

import json
from pathlib import Path

import pytest

from stockagent.config import load_config
from stockagent.live.market_config import load_market_config, load_market_configs
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


def test_repo_tw_modes_have_independent_market_and_artifact_routes() -> None:
    root = Path(__file__).resolve().parents[1]
    configs = load_market_configs(root / "services/discord_bot/markets")

    assert {
        "tw",
        "tw_cash",
        "tw_day_trade_multi_basis",
        "tw_day_trade_100m",
        "tw_day_trade_multi_basis_projection_l1_gelu",
    }.issubset(configs)
    naive = configs["tw"]
    assert naive.market_type == "tw"
    assert naive.fold_id == 25
    assert naive.config_path == "configs/deployments/tw_naive_fold25.yaml"
    assert naive.output_dir == "artifacts/markets/tw"
    assert naive.checkpoint_path == "artifacts/markets/tw/fold_25/checkpoint_best.pt"
    assert configs["tw_cash"].output_dir != configs["tw"].output_dir
    day_trade_100m = configs["tw_day_trade_100m"]
    assert day_trade_100m.fold_id == 11
    assert day_trade_100m.initial_capital == 100_000_000.0
    assert day_trade_100m.config_path == "configs/deployments/tw_day_trade_100m_fold11.yaml"
    assert day_trade_100m.output_dir == "artifacts/markets/tw_day_trade_100m"
    assert day_trade_100m.checkpoint_path == (
        "artifacts/markets/tw_day_trade_100m/fold_11/checkpoint_best.pt"
    )
    multi_basis = configs["tw_day_trade_multi_basis"]
    multi_basis_root = (
        "artifacts/markets/"
        "tw_public_candles_multi_basis_online_complete_raw_feature_input_lookback32_v5"
    )
    assert multi_basis.fold_id == 11
    assert multi_basis.initial_capital == 10_000_000.0
    assert multi_basis.config_path == (
        "configs/deployments/tw_day_trade_multi_basis_fold11.yaml"
    )
    assert multi_basis.output_dir == multi_basis_root
    assert multi_basis.checkpoint_path == (
        f"{multi_basis_root}/fold_11/checkpoint_best.pt"
    )
    projection = configs["tw_day_trade_multi_basis_projection_l1_gelu"]
    assert projection.fold_id == 11
    assert projection.initial_capital == 10_000_000.0
    assert projection.config_path == (
        "configs/deployments/tw_day_trade_multi_basis_projection_l1_gelu_fold11.yaml"
    )
    assert (
        multi_basis.live_output_dir
        == day_trade_100m.live_output_dir
        == projection.live_output_dir
        == "artifacts/live_signals"
    )
    shared_refresh = (
        "scripts/activate_tw_public_opening_data.py",
        "--link",
        "data_tw_public",
    )
    assert multi_basis.pre_signal_command == shared_refresh
    assert day_trade_100m.pre_signal_command == shared_refresh
    assert projection.pre_signal_command == shared_refresh
    completed_close = ("scripts/finalize_tw_public_completed_session.py",)
    assert multi_basis.completed_session_command == completed_close
    assert day_trade_100m.completed_session_command == completed_close
    assert projection.completed_session_command == completed_close


def test_repo_multi_basis_fold11_deployment_keeps_checkpoint_model_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "configs/deployments/tw_day_trade_multi_basis_fold11.yaml")

    assert config.runner.output_dir == (
        "artifacts/markets/"
        "tw_public_candles_multi_basis_online_complete_raw_feature_input_lookback32_v5"
    )
    assert config.training.transformer_base_portfolio.temporal_basis_input == "raw_features"
