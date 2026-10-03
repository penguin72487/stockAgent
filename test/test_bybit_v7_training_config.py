from dataclasses import asdict
from pathlib import Path

from stockagent.config import load_config
from stockagent.training.trainer import _mode_artifact_contract_for_config


ROOT = Path(__file__).resolve().parents[1]
CONTROL = ROOT / "configs/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v6.yaml"
CANDIDATE = ROOT / "configs/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v7.yaml"


def test_v7_changes_only_requested_clock_output_and_artifact_identity():
    control, candidate = load_config(CONTROL), load_config(CANDIDATE)
    expected = asdict(control)
    expected["experiment_name"] = candidate.experiment_name
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    expected["data"]["parquet_root"] = "artifacts/cache/bybit_perpetual_daily_0000_repaired/perpetual_daily"
    expected["data"]["panel_cache_root"] = "artifacts/cache/bybit_perpetual_daily_0000_repaired/panel"
    expected["trading"]["crypto_execution_minute_utc"] = 0
    expected["training"]["financial_transformer"]["portfolio_output_mode"] = "score_entmax_cash_v2"
    assert asdict(candidate) == expected
    assert candidate.runner.output_dir.startswith("artifacts/markets/")
    assert candidate.runner.output_dir != control.runner.output_dir
    assert candidate.training.epochs == 1000
    assert candidate.training.crypto_optimizer_step_per_trajectory
    assert candidate.training.pretrained_initialization_root is None
    assert candidate.trading.portfolio_activation == "pre_normalized"
    assert candidate.training.loss_portfolio_activation == "pre_normalized"


def test_v7_mode_records_zero_latency_and_boundary_funding():
    candidate = load_config(CANDIDATE)
    contract = _mode_artifact_contract_for_config(candidate)
    assert "0000_utc_zero_latency_research_assumption" in contract["execution_clock"]
    assert contract["mode_details"]["funding_boundary_order"] == "boundary_funding_settles_before_new_target"
    assert contract["mode_details"]["crypto_backward_contract_version"] == 1
