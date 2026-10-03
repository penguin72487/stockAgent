from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from stockagent.config import load_config


ROOT = Path(__file__).resolve().parents[1]
CONTROL = ROOT / "configs/markets/bybit_perpetual_daily_0005_historical_pit_v1.yaml"
CANDIDATE = (
    ROOT
    / "configs/markets/bybit_perpetual_daily_0005_historical_pit_trajectory_v1.yaml"
)
LEARNED_CASH = (
    ROOT
    / "configs/markets/bybit_perpetual_daily_0005_historical_pit_learned_cash_trajectory_v2.yaml"
)
LIFECYCLE_V3 = (
    ROOT
    / "configs/markets/bybit_perpetual_daily_0005_historical_pit_learned_cash_trajectory_v3.yaml"
)
HISTORICAL_PUBLIC_V4 = (
    ROOT
    / "configs/markets/bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_v4.yaml"
)


def test_bybit_trajectory_candidate_changes_only_training_cadence_and_roots() -> None:
    control = load_config(CONTROL)
    candidate = load_config(CANDIDATE)

    assert control.training.crypto_optimizer_step_per_trajectory is False
    assert candidate.training.crypto_optimizer_step_per_trajectory is True
    assert candidate.training.lookback == 32
    assert candidate.training.epochs == 1000
    assert candidate.training.multi_gpu_strategy == "distributed_data_parallel"
    assert candidate.data.crypto_exchange_scope == "bybit"
    assert candidate.trading.execution_mode == "crypto_perpetual"
    assert candidate.trading.crypto_execution_minute_utc == 5
    assert candidate.trading.buy_fee_rate == 0.00055
    assert candidate.trading.sell_fee_rate == 0.00055

    expected = asdict(control)
    expected["experiment_name"] = candidate.experiment_name
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    expected["data"]["panel_cache_root"] = candidate.data.panel_cache_root
    expected["training"]["crypto_optimizer_step_per_trajectory"] = True
    assert asdict(candidate) == expected


def test_bybit_trajectory_candidate_uses_explicit_venue_features() -> None:
    candidate = load_config(CANDIDATE)
    selected = candidate.data.feature_include

    assert "*" not in selected
    assert len(selected) == 22
    assert len(candidate.data.feature_availability_indicators) == 7
    external = [name for name in selected if name.startswith("crypto_")]
    assert external
    assert all(name.startswith("crypto_bybit_") for name in external)


def test_bybit_learned_cash_changes_only_action_abi_and_artifact_root() -> None:
    control = load_config(CANDIDATE)
    candidate = load_config(LEARNED_CASH)

    assert control.training.financial_transformer.portfolio_output_mode == "projection_l1"
    assert candidate.training.financial_transformer.portfolio_output_mode == "learned_cash"
    assert candidate.training.crypto_optimizer_step_per_trajectory is True
    assert candidate.training.lookback == 32

    expected = asdict(control)
    expected["experiment_name"] = candidate.experiment_name
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    expected["training"]["financial_transformer"]["portfolio_output_mode"] = (
        "learned_cash"
    )
    assert asdict(candidate) == expected


def test_bybit_lifecycle_v3_uses_a_fresh_artifact_root_only() -> None:
    control = load_config(LEARNED_CASH)
    candidate = load_config(LIFECYCLE_V3)

    expected = asdict(control)
    expected["experiment_name"] = candidate.experiment_name
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    assert asdict(candidate) == expected


def test_bybit_historical_public_v4_separates_execution_from_information() -> None:
    control = load_config(LIFECYCLE_V3)
    candidate = load_config(HISTORICAL_PUBLIC_V4)

    assert candidate.data.crypto_exchange_scope == "bybit"
    assert candidate.data.crypto_information_scope == "historical_public_pit"
    assert candidate.data.parquet_root == "data_bybit/perpetual_daily"
    assert candidate.data.external_feature_path.endswith(
        "bybit_crypto_public_daily.parquet"
    )
    assert len(candidate.data.feature_include) == 74
    assert len(candidate.data.feature_availability_indicators) == 59
    assert any(
        name.startswith("crypto_public_macro_")
        for name in candidate.data.feature_include
    )
    assert any(
        name.startswith("crypto_public_sec_")
        for name in candidate.data.feature_include
    )
    assert not any(
        name.startswith(
            (
                "crypto_public_onchain_",
                "crypto_public_coingecko_",
                "crypto_public_etf_",
            )
        )
        for name in candidate.data.feature_include
    )
    assert candidate.training.lookback == 32
    assert candidate.training.epochs == 1000
    assert candidate.training.financial_transformer.portfolio_output_mode == (
        "learned_cash"
    )
    assert candidate.training.crypto_optimizer_step_per_trajectory is True
    assert candidate.trading.execution_mode == "crypto_perpetual"
    assert candidate.trading.buy_fee_rate == 0.00055
    assert candidate.trading.sell_fee_rate == 0.00055

    assert control.data.crypto_information_scope == "venue_only"
    assert candidate.runner.output_dir != control.runner.output_dir
    assert candidate.data.panel_cache_root != control.data.panel_cache_root
