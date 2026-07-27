from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import yaml

from stockagent.backtest.tw_execution import (
    TaiwanFeeSchedule,
    effective_fee_rate_vectors,
    lot_size_vector,
    normalize_execution_mode,
)
from stockagent.config import DAY_TRADE_OPEN_GAP_FEATURE, load_config
from stockagent.training.trainer import _format_ic_summary_for_console


def _write_config(
    tmp_path: Path,
    *,
    execution_mode: object,
    model_name: object = "transformer_base_portfolio",
    loss_type: object = "log_utility",
    model_output_mode: str = "logits",
    trading_activation: str = "identity",
    loss_activation: str = "auto",
    day_trade_open_feature: bool = False,
    feature_include: list[str] | None = None,
    return_rank_ic_weight: float = 0.0,
    direction_weight: float = 0.0,
    explain_after_each_fold: bool = False,
) -> Path:
    path = tmp_path / "phase-mode.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "experiment_name": "tw-phase-mode-contract",
                "environment": {
                    "device": "cuda",
                    "use_tensor_cores": True,
                    "amp_dtype": "bf16",
                },
                "data": {
                    "parquet_root": "data_yahoo/tw_stocks",
                    "benchmark_name": "2330",
                    "day_trade_open_feature": day_trade_open_feature,
                    "feature_include": list(feature_include or ()),
                },
                "walk_forward": {
                    "min_train_years": 1,
                    "val_years": 1,
                    "require_future_test_year": False,
                },
                "trading": {
                    "frequency": "daily",
                    "buy_fee_rate": 0.0007,
                    "sell_fee_rate": 0.0037,
                    "long_only": False,
                    "execution_mode": execution_mode,
                    "portfolio_activation": trading_activation,
                },
                "training": {
                    "non_blocking_transfer": True,
                    "model_name": model_name,
                    "loss_type": loss_type,
                    "loss_portfolio_activation": loss_activation,
                    "transformer_base_portfolio": {
                        "portfolio_output_mode": model_output_mode,
                    },
                    "multitask_loss": {
                        "return_rank_ic_weight": return_rank_ic_weight,
                        "direction_weight": direction_weight,
                    },
                    "explain_after_each_fold": explain_after_each_fold,
                },
                "evaluation": {},
            }
        ),
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize(
    "raw_mode",
    [
        "tw_overnight",
        "TW-OVERNIGHT-TRADE",
        "taiwan overnight",
        "overnight",
        "next day trade",
        "next-session-trade",
        "隔日沖",
        "台股隔日沖",
    ],
)
def test_overnight_aliases_are_explicit_and_canonical(raw_mode: str) -> None:
    assert normalize_execution_mode(raw_mode) == "tw_overnight"


def test_overnight_alias_is_canonicalized_during_config_load(
    tmp_path: Path,
) -> None:
    config = load_config(
        _write_config(tmp_path, execution_mode="隔日沖")
    )

    assert config.trading.execution_mode == "tw_overnight"


def test_overnight_market_template_loads_open_aware_phase_contract() -> None:
    config = load_config(
        Path(
            "configs/markets/"
            "tw_public_lanten_market_candles_overnight.yaml"
        )
    )

    assert config.trading.execution_mode == "tw_overnight"
    assert config.data.day_trade_open_feature is True
    assert DAY_TRADE_OPEN_GAP_FEATURE in config.data.feature_include
    assert config.training.tw_continuous_compile_chunk_rows == 32
    assert config.training.tw_continuous_gradient_horizon_rows == 32


def test_overnight_uses_ordinary_sell_tax_not_day_trade_tax() -> None:
    schedule = TaiwanFeeSchedule(
        commission_rate=0.002,
        commission_discount=0.5,
        stock_sell_tax=0.004,
        etf_sell_tax=0.002,
        day_trade_stock_sell_tax=0.0004,
        day_trade_etf_sell_tax=0.0002,
    )

    buy, overnight_sell = effective_fee_rate_vectors(
        ["2330", "0050"],
        "tw_overnight",
        fee_schedule=schedule,
    )
    _, day_trade_sell = effective_fee_rate_vectors(
        ["2330", "0050"],
        "tw_day_trade",
        fee_schedule=schedule,
    )

    np.testing.assert_allclose(buy, [0.001, 0.001], rtol=0.0, atol=0.0)
    np.testing.assert_allclose(
        overnight_sell,
        [0.005, 0.003],
        rtol=0.0,
        atol=0.0,
    )
    np.testing.assert_allclose(
        day_trade_sell,
        [0.001 + 0.0004, 0.001 + 0.0002],
        rtol=0.0,
        atol=0.0,
    )


@pytest.mark.parametrize("mode", ["tw_cash", "tw_overnight"])
def test_phase_carrying_modes_use_cash_lot_profile(mode: str) -> None:
    schedule = TaiwanFeeSchedule(
        cash_lot_size=37,
        day_trade_default_lot_size=1000,
    )

    lots = lot_size_vector(
        ["2330", "0050"],
        mode,
        fee_schedule=schedule,
    )

    np.testing.assert_array_equal(lots, [37, 37])


@pytest.mark.parametrize(
    "model_name",
    [
        "transformer_base_portfolio",
        "transformer-base-portfolio-model",
        "tbp",
        "financial_transformer",
        "financial-transformer-model",
        "financial_tokenized_transformer",
    ],
)
@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_phase_modes_accept_only_supported_phase_head_families(
    tmp_path: Path,
    execution_mode: str,
    model_name: str,
) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            execution_mode=execution_mode,
            model_name=model_name,
        )
    )

    assert config.trading.execution_mode == execution_mode
    assert config.training.model_name == model_name


@pytest.mark.parametrize(
    "model_name",
    [
        "mlp",
        "low_rank_market_transformer_portfolio",
        "gradient_boosted_portfolio_transformer",
        "bottleneck_portfolio_autoencoder",
        "lightgbm",
    ],
)
@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_phase_modes_reject_models_without_phase_heads(
    tmp_path: Path,
    execution_mode: str,
    model_name: str,
) -> None:
    with pytest.raises(ValueError, match="phase-action head"):
        load_config(
            _write_config(
                tmp_path,
                execution_mode=execution_mode,
                model_name=model_name,
            )
        )


@pytest.mark.parametrize(
    "loss_type",
    ["log_utility", "log-util", "kelly", "growth", "mean_log_return"],
)
@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_phase_modes_accept_canonical_log_utility_aliases(
    tmp_path: Path,
    execution_mode: str,
    loss_type: str,
) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            execution_mode=execution_mode,
            loss_type=loss_type,
        )
    )

    assert config.training.loss_type == loss_type


@pytest.mark.parametrize(
    "loss_type",
    [
        "mse",
        "sharpe",
        "rank_ic",
        "pure_rank",
        "factor_generalization",
        "portfolio_autoencoder",
    ],
)
@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_phase_modes_reject_noncanonical_or_scalar_target_objectives(
    tmp_path: Path,
    execution_mode: str,
    loss_type: str,
) -> None:
    with pytest.raises(ValueError, match="path-dependent log-utility"):
        load_config(
            _write_config(
                tmp_path,
                execution_mode=execution_mode,
                loss_type=loss_type,
            )
        )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("return_rank_ic_weight", 0.1),
        ("direction_weight", 0.1),
    ],
)
def test_phase_modes_reject_undefined_scalar_auxiliary_targets(
    tmp_path: Path,
    execution_mode: str,
    field_name: str,
    field_value: float,
) -> None:
    kwargs = {field_name: field_value}
    with pytest.raises(ValueError, match=r"multi-phase \[T,P,S\].*must be 0"):
        load_config(
            _write_config(
                tmp_path,
                execution_mode=execution_mode,
                **kwargs,
            )
        )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_phase_modes_reject_unlabelled_explainability(
    tmp_path: Path,
    execution_mode: str,
) -> None:
    with pytest.raises(ValueError, match=r"phase actions \[B,P,S\]"):
        load_config(
            _write_config(
                tmp_path,
                execution_mode=execution_mode,
                explain_after_each_fold=True,
            )
        )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
@pytest.mark.parametrize("enable_via_flag", [False, True])
def test_phase_auctions_accept_causal_current_open_quote_feature(
    tmp_path: Path,
    execution_mode: str,
    enable_via_flag: bool,
) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            execution_mode=execution_mode,
            day_trade_open_feature=enable_via_flag,
            feature_include=(
                []
                if enable_via_flag
                else [DAY_TRADE_OPEN_GAP_FEATURE]
            ),
        )
    )

    assert DAY_TRADE_OPEN_GAP_FEATURE in config.data.feature_include


def test_naive_and_day_trade_keep_their_existing_model_objective_contracts(
    tmp_path: Path,
) -> None:
    naive = load_config(
        _write_config(
            tmp_path,
            execution_mode="naive",
            model_name="mlp",
            loss_type="rank_ic",
        )
    )
    assert naive.trading.execution_mode == "naive"
    assert naive.training.model_name == "mlp"
    assert naive.training.loss_type == "rank_ic"

    day_trade = load_config(
        _write_config(
            tmp_path,
            execution_mode="tw_day_trade",
            model_name="mlp",
            loss_type="rank_ic",
            day_trade_open_feature=True,
        )
    )
    assert day_trade.trading.execution_mode == "tw_day_trade"
    assert day_trade.data.day_trade_open_feature is True
    assert DAY_TRADE_OPEN_GAP_FEATURE in day_trade.data.feature_include


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
@pytest.mark.parametrize(
    "model_output_mode",
    ["l1", "projection_l1", "signed_softmax"],
)
def test_phase_modes_require_pre_normalized_consumers_for_resolved_model_outputs(
    tmp_path: Path,
    execution_mode: str,
    model_output_mode: str,
) -> None:
    with pytest.raises(ValueError, match="must be 'pre_normalized'"):
        load_config(
            _write_config(
                tmp_path,
                execution_mode=execution_mode,
                model_output_mode=model_output_mode,
                trading_activation="identity",
                loss_activation="auto",
            )
        )

    config = load_config(
        _write_config(
            tmp_path,
            execution_mode=execution_mode,
            model_output_mode=model_output_mode,
            trading_activation="pre_normalized",
            loss_activation="pre_normalized",
        )
    )
    assert config.trading.portfolio_activation == "pre_normalized"
    assert config.training.loss_portfolio_activation == "pre_normalized"


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_phase_modes_reject_semantically_inert_activation_l1(
    tmp_path: Path,
    execution_mode: str,
) -> None:
    with pytest.raises(ValueError, match="silently replace.*identity"):
        load_config(
            _write_config(
                tmp_path,
                execution_mode=execution_mode,
                model_output_mode="activation_l1",
                trading_activation="pre_normalized",
                loss_activation="pre_normalized",
            )
        )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_phase_modes_reject_pre_normalized_consumers_for_raw_logits(
    tmp_path: Path,
    execution_mode: str,
) -> None:
    with pytest.raises(ValueError, match="unresolved raw phase logits"):
        load_config(
            _write_config(
                tmp_path,
                execution_mode=execution_mode,
                model_output_mode="logits",
                trading_activation="pre_normalized",
                loss_activation="pre_normalized",
            )
        )


@pytest.mark.parametrize(
    "bad_mode",
    ["tw_swing", "tomorrow", "", None, 1],
)
def test_unknown_execution_mode_remains_fail_closed(
    tmp_path: Path,
    bad_mode: object,
) -> None:
    with pytest.raises(ValueError, match="execution_mode"):
        load_config(
            _write_config(tmp_path, execution_mode=bad_mode)
        )


def test_phase_console_reports_unavailable_scalar_ic_without_crashing() -> None:
    assert _format_ic_summary_for_console({}) == "IC=N/A  IC_IR=N/A"
    assert (
        _format_ic_summary_for_console(
            {"ic_mean": float("nan"), "ic_ir": 0.0}
        )
        == "IC=N/A  IC_IR=N/A"
    )
    assert (
        _format_ic_summary_for_console(
            {"ic_mean": 0.125, "ic_ir": -0.5}
        )
        == "IC=+0.1250  IC_IR=-0.5000"
    )
