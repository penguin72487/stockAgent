from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from stockagent.config import load_config


ROOT = Path(__file__).resolve().parents[1]
CONTROL = (
    ROOT
    / "configs/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v4.yaml"
)
CANDIDATE = (
    ROOT
    / "configs/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v5.yaml"
)


def test_bybit_v5_changes_only_normalizer_and_artifact_identity() -> None:
    control = load_config(CONTROL)
    candidate = load_config(CANDIDATE)

    assert control.training.financial_transformer.causal_feature_rms_normalization is False
    assert candidate.training.financial_transformer.causal_feature_rms_normalization is True
    assert candidate.runner.output_dir == (
        "artifacts/markets/"
        "bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v5"
    )
    assert candidate.runner.output_dir != control.runner.output_dir
    expected = asdict(control)
    expected["experiment_name"] = candidate.experiment_name
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    expected["training"]["financial_transformer"][
        "causal_feature_rms_normalization"
    ] = True
    assert asdict(candidate) == expected


def test_bybit_v5_reuses_data_and_preserves_full_training_contract() -> None:
    control = load_config(CONTROL)
    candidate = load_config(CANDIDATE)

    assert asdict(candidate.data) == asdict(control.data)
    assert candidate.data.parquet_root == "data_bybit/perpetual_daily"
    assert candidate.data.external_feature_path == (
        "data_bybit/public_features/bybit_crypto_public_daily.parquet"
    )
    assert candidate.data.panel_cache_root == (
        "artifacts/cache/bybit_perpetual_daily_0005_historical_public_pit_trajectory_v4"
    )
    assert candidate.data.crypto_exchange_scope == "bybit"
    assert candidate.data.crypto_information_scope == "historical_public_pit"
    assert candidate.data.feature_zero_fill == []
    assert len(candidate.data.feature_include) == 74
    assert len(candidate.data.feature_availability_indicators) == 59
    assert candidate.training.epochs == 1000
    assert candidate.training.crypto_optimizer_step_per_trajectory is True
    assert candidate.training.loss_type == "log_utility"
    assert candidate.training.lookback == 32
    assert candidate.training.multi_gpu_strategy == "distributed_data_parallel"
    assert candidate.environment.amp_dtype == "bf16"
    assert candidate.training.financial_transformer.portfolio_output_mode == "learned_cash"
    assert candidate.training.financial_transformer.causal_feature_window_rms_normalization is False
    assert candidate.trading.execution_mode == "crypto_perpetual"
    assert candidate.trading.crypto_execution_minute_utc == 5
    assert candidate.trading.long_only is False
    assert candidate.trading.buy_fee_rate == 0.00055
    assert candidate.trading.sell_fee_rate == 0.00055
