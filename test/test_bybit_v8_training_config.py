"""The authorized announcement fill is isolated from data/model and v7 replay."""
from dataclasses import asdict
from pathlib import Path

from stockagent.config import load_config
from stockagent.training.checkpoint_contract import (
    _configuration_fingerprint_snapshot,
    _trading_checkpoint_contract,
)
from stockagent.training.trainer import _mode_artifact_contract_for_config

ROOT = Path(__file__).resolve().parents[1]
CONTROL = ROOT / "configs/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v7.yaml"
CANDIDATE = ROOT / "configs/markets/bybit_perpetual_daily_0000_historical_public_pit_score_cash_trajectory_v8.yaml"


def test_v8_changes_only_approved_exit_participation_and_artifact_identity():
    control, candidate = load_config(CONTROL), load_config(CANDIDATE)
    expected = asdict(control)
    expected["experiment_name"] = candidate.experiment_name
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    expected["trading"]["crypto_announced_exit_unlimited_volume"] = True
    expected["trading"]["max_volume_participation"] = 0.5
    assert asdict(candidate) == expected
    assert candidate.runner.output_dir.startswith("artifacts/markets/")
    assert candidate.runner.output_dir != control.runner.output_dir
    assert candidate.training.epochs == 1000
    assert candidate.training.pretrained_initialization_root is None
    assert candidate.trading.crypto_execution_minute_utc == 0
    assert candidate.trading.max_volume_participation == 0.5
    assert control.trading.max_volume_participation == 0.01
    assert candidate.trading.max_turnover_ratio == 0.0
    assert candidate.trading.buy_fee_rate == candidate.trading.sell_fee_rate == 0.00055
    assert candidate.data.parquet_root == control.data.parquet_root
    assert candidate.data.panel_cache_root == control.data.panel_cache_root


def test_disabled_flag_keeps_legacy_trading_and_configuration_contracts():
    control = load_config(CONTROL)
    assert not control.trading.crypto_announced_exit_unlimited_volume
    assert _trading_checkpoint_contract(control)["crypto_perpetual"] == {
        "backtest_contract_version": 5,
        "execution_minute_utc": 0,
        "stateful_proximal_allocator": True,
        "proximal_cost_multiplier": 1.0,
    }
    assert "crypto_announced_exit_unlimited_volume" not in _configuration_fingerprint_snapshot(control)["trading"]
    assert _mode_artifact_contract_for_config(control)["mode_details"]["announced_exit_policy"] == (
        "zero_target_request_obeys_side_capacity_turnover_not_exchange_settlement"
    )


def test_v8_checkpoint_and_artifact_disclose_volume_exemption():
    candidate = load_config(CANDIDATE)
    contract = _trading_checkpoint_contract(candidate)["crypto_perpetual"]
    assert _trading_checkpoint_contract(candidate)["max_volume_participation"] == 0.5
    assert contract["backtest_contract_version"] == 6
    assert contract["announced_exit_unlimited_volume"] is True
    assert contract["announced_exit_fees"] == "ordinary_executed_buy_sell_fees"
    assert "not_exchange_settlement" in contract["announced_exit_assumption"]
    assert _configuration_fingerprint_snapshot(candidate)["trading"]["crypto_announced_exit_unlimited_volume"] is True
    artifact = _mode_artifact_contract_for_config(candidate)
    assert "0000_utc_zero_latency_research_assumption" in artifact["execution_clock"]
    assert artifact["mode_details"]["crypto_backtest_contract_version"] == 6
    assert artifact["mode_details"]["crypto_backward_contract_version"] == 1
