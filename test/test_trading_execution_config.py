from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import torch
import yaml

from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.training.trainer import _build_execution_runtime


def _write_config(tmp_path: Path, trading_overrides: dict[str, object] | None = None) -> Path:
    trading: dict[str, object] = {
        "frequency": "daily",
        "buy_fee_rate": 0.0007,
        "sell_fee_rate": 0.0037,
        "long_only": False,
    }
    trading.update(trading_overrides or {})
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "experiment_name": "trading-execution-config-test",
                "environment": {
                    "device": "cuda",
                    "use_tensor_cores": True,
                    "amp_dtype": "bf16",
                },
                "data": {
                    "parquet_root": "data_yahoo/tw_stocks",
                    "benchmark_name": "2330",
                },
                "walk_forward": {
                    "min_train_years": 1,
                    "val_years": 1,
                    "require_future_test_year": False,
                },
                "trading": trading,
                "training": {
                    "non_blocking_transfer": True,
                    "model_name": "transformer_base_portfolio",
                    "loss_type": "log_utility",
                    # Keep this generic config helper valid for phase-aware
                    # execution without applying an already-resolved action
                    # transform twice.
                    "transformer_base_portfolio": {
                        "portfolio_output_mode": "logits",
                    },
                    "multitask_loss": {
                        "return_rank_ic_weight": 0.0,
                        "direction_weight": 0.0,
                    },
                },
                "evaluation": {},
            }
        ),
        encoding="utf-8",
    )
    return path


def test_execution_config_defaults_preserve_naive_mode_and_legacy_fees(tmp_path: Path) -> None:
    config = load_config(_write_config(tmp_path))
    trading = config.trading

    assert trading.execution_mode == "naive"
    assert trading.buy_fee_rate == 0.0007
    assert trading.sell_fee_rate == 0.0037
    assert trading.tw_commission_rate == 0.001425
    assert trading.tw_commission_discount == 0.2
    assert trading.tw_commission_rebate_timing == "monthly_15th"
    assert trading.tw_stock_sell_tax == 0.003
    assert trading.tw_etf_sell_tax == 0.001
    assert trading.tw_day_trade_stock_sell_tax == 0.0015
    assert trading.tw_day_trade_etf_sell_tax == 0.001
    assert trading.tw_minimum_commission == 0.0
    assert trading.tw_commission_rounding == "none"
    assert trading.tw_tax_rounding == "none"
    assert trading.tw_settlement_lag_sessions == 2
    assert trading.tw_cash_lot_size == 1
    assert trading.tw_day_trade_lot_size == 1000
    assert trading.tw_short_initial_margin_rate == 0.9
    assert trading.tw_short_maintenance_ratio == 1.3
    assert trading.tw_short_lot_size == 1000
    assert trading.tw_short_handling_fee_rate == 0.0
    assert trading.tw_short_capacity_limit_enabled is True
    assert trading.tw_corporate_action_mode == "avoid"
    assert trading.tw_corporate_action_claim_queue_sessions == 256


def test_tw_gradient_horizon_must_align_with_compile_chunks(tmp_path: Path) -> None:
    path = _write_config(tmp_path, {"execution_mode": "tw_cash"})
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["training"]["tw_continuous_compile_chunk_rows"] = 4
    payload["training"]["tw_continuous_gradient_horizon_rows"] = 30
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="exact multiple"):
        load_config(path)


@pytest.mark.parametrize(
    ("raw_mode", "canonical_mode"),
    [
        ("legacy", "naive"),
        ("TW-CASH", "tw_cash"),
        ("現股", "tw_cash"),
        ("day trade", "tw_day_trade"),
        ("當沖", "tw_day_trade"),
    ],
)
def test_execution_mode_is_canonicalized_during_yaml_load(
    tmp_path: Path,
    raw_mode: str,
    canonical_mode: str,
) -> None:
    overrides: dict[str, object] = {"execution_mode": raw_mode}
    if canonical_mode == "tw_cash":
        overrides["long_only"] = True
    config = load_config(_write_config(tmp_path, overrides))

    assert config.trading.execution_mode == canonical_mode


def test_custom_taiwan_execution_schedule_is_retained_after_validation(tmp_path: Path) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            {
                "execution_mode": "tw_cash",
                "long_only": True,
                "tw_commission_rate": 0.002,
                "tw_commission_discount": 0.5,
                "tw_commission_rebate_timing": "daily",
                "tw_stock_sell_tax": 0.004,
                "tw_etf_sell_tax": 0.002,
                "tw_day_trade_stock_sell_tax": 0.0025,
                "tw_day_trade_etf_sell_tax": 0.0012,
                "tw_minimum_commission": 20.0,
                "tw_commission_rounding": "truncate-to-twd",
                "tw_tax_rounding": "round half up",
                "tw_settlement_lag_sessions": 3,
                "tw_cash_lot_size": 2,
                "tw_day_trade_lot_size": 500,
                "tw_short_initial_margin_rate": 1.05,
                "tw_short_maintenance_ratio": 1.4,
                "tw_short_lot_size": 2000,
                "tw_short_handling_fee_rate": 0.0008,
                "tw_short_capacity_limit_enabled": False,
            },
        )
    )
    trading = config.trading

    assert trading.execution_mode == "tw_cash"
    assert trading.tw_commission_rate == 0.002
    assert trading.tw_commission_discount == 0.5
    assert trading.tw_commission_rebate_timing == "daily_close"
    assert trading.tw_stock_sell_tax == 0.004
    assert trading.tw_etf_sell_tax == 0.002
    assert trading.tw_day_trade_stock_sell_tax == 0.0025
    assert trading.tw_day_trade_etf_sell_tax == 0.0012
    assert trading.tw_minimum_commission == 20.0
    assert trading.tw_commission_rounding == "floor"
    assert trading.tw_tax_rounding == "half_up"
    assert trading.tw_settlement_lag_sessions == 3
    assert trading.tw_cash_lot_size == 2
    assert trading.tw_day_trade_lot_size == 500
    assert trading.tw_short_initial_margin_rate == 1.05
    assert trading.tw_short_maintenance_ratio == 1.4
    assert trading.tw_short_lot_size == 2000
    assert trading.tw_short_handling_fee_rate == 0.0008
    assert trading.tw_short_capacity_limit_enabled is False


def test_tw_cash_allows_margin_short_positions(tmp_path: Path) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            {
                "execution_mode": "tw_cash",
                "long_only": False,
            },
        )
    )

    assert config.trading.execution_mode == "tw_cash"
    assert config.trading.long_only is False
    assert config.trading.tw_short_lot_size == 1000


@pytest.mark.parametrize("bad_value", [0, 1, "false", None])
def test_short_capacity_limit_switch_requires_yaml_boolean(
    tmp_path: Path,
    bad_value: object,
) -> None:
    with pytest.raises(ValueError, match="tw_short_capacity_limit_enabled"):
        load_config(
            _write_config(
                tmp_path,
                {"tw_short_capacity_limit_enabled": bad_value},
            )
        )


def test_disabled_short_capacity_limit_allows_missing_capacity_but_not_eligibility(
    tmp_path: Path,
) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            {
                "execution_mode": "tw_cash",
                "long_only": False,
                "tw_short_capacity_limit_enabled": False,
            },
        )
    )
    shape = (3, 1)
    mask = np.ones(shape, dtype=np.bool_)
    panel = PanelData(
        dates=np.arange(3).astype("datetime64[D]"),
        symbols=["2330"],
        feature_names=["f0"],
        features=np.zeros((3, 1, 1), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=mask,
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(3, dtype=np.float32),
        close_prices=np.ones(shape, dtype=np.float32),
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        can_short_open_open_mask=mask.copy(),
        unresolved_corporate_action_mask=np.zeros(shape, dtype=np.bool_),
        short_capacity_shares=None,
    )

    runtime = _build_execution_runtime(panel, config, torch.device("cpu"))
    assert runtime.short_capacity_limit_enabled is False

    config.trading.tw_short_capacity_limit_enabled = True
    with pytest.raises(ValueError, match="demonstrated short capacity"):
        _build_execution_runtime(panel, config, torch.device("cpu"))

    config.trading.tw_short_capacity_limit_enabled = False
    panel.can_short_open_mask = None
    with pytest.raises(ValueError, match="margin-short eligibility"):
        _build_execution_runtime(panel, config, torch.device("cpu"))


def test_exact_corporate_action_runtime_requires_and_bounds_claim_ledger(
    tmp_path: Path,
) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            {
                "execution_mode": "tw_cash",
                "long_only": True,
                "tw_corporate_action_mode": "exact",
                "tw_corporate_action_claim_queue_sessions": 4,
            },
        )
    )
    shape = (3, 1)
    mask = np.ones(shape, dtype=np.bool_)
    panel = PanelData(
        dates=np.arange(3).astype("datetime64[D]"),
        symbols=["2330"],
        feature_names=["f0"],
        features=np.zeros((3, 1, 1), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=mask,
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(3, dtype=np.float32),
        close_prices=np.ones(shape, dtype=np.float32),
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        unresolved_corporate_action_mask=np.zeros(shape, dtype=np.bool_),
        cash_dividend_yield=np.zeros(shape, dtype=np.float32),
        cash_dividend_payment_delay_sessions=np.asarray([[0], [3], [0]]),
    )

    runtime = _build_execution_runtime(panel, config, torch.device("cpu"))
    assert runtime.corporate_action_mode == "exact"
    assert runtime.claim_queue_sessions == 4

    panel.cash_dividend_payment_delay_sessions[1, 0] = 5
    with pytest.raises(ValueError, match="claim_queue_sessions"):
        _build_execution_runtime(panel, config, torch.device("cpu"))

    panel.cash_dividend_payment_delay_sessions = None
    with pytest.raises(ValueError, match="MOPS"):
        _build_execution_runtime(panel, config, torch.device("cpu"))


def test_avoid_corporate_action_runtime_uses_only_t_plus_two_queue(
    tmp_path: Path,
) -> None:
    config = load_config(
        _write_config(
            tmp_path,
            {
                "execution_mode": "tw_cash",
                "long_only": True,
                "tw_corporate_action_mode": "avoid",
                "tw_corporate_action_claim_queue_sessions": 256,
            },
        )
    )
    shape = (2, 1)
    mask = np.ones(shape, dtype=np.bool_)
    panel = PanelData(
        dates=np.arange(2).astype("datetime64[D]"),
        symbols=["2330"],
        feature_names=["f0"],
        features=np.zeros((2, 1, 1), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=mask,
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(2, dtype=np.float32),
        close_prices=np.ones(shape, dtype=np.float32),
        unresolved_corporate_action_mask=np.zeros(shape, dtype=np.bool_),
    )

    runtime = _build_execution_runtime(panel, config, torch.device("cpu"))
    assert runtime.claim_queue_sessions == config.trading.tw_settlement_lag_sessions


@pytest.mark.parametrize("bad_mode", ["", "tw_margin", "tomorrow", 1, None])
def test_execution_config_rejects_unknown_or_non_string_mode(
    tmp_path: Path,
    bad_mode: object,
) -> None:
    with pytest.raises(ValueError, match="execution_mode"):
        load_config(_write_config(tmp_path, {"execution_mode": bad_mode}))


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("tw_commission_rate", -0.001),
        ("tw_commission_rate", float("nan")),
        ("tw_commission_discount", -0.1),
        ("tw_commission_discount", 1.01),
        ("tw_stock_sell_tax", -0.003),
        ("tw_etf_sell_tax", float("inf")),
        ("tw_day_trade_stock_sell_tax", True),
        ("tw_day_trade_etf_sell_tax", "0.001"),
        ("tw_minimum_commission", -1.0),
        ("tw_commission_rounding", "bankers"),
        ("tw_tax_rounding", "ceil"),
        ("tw_short_initial_margin_rate", 0.0),
        ("tw_short_initial_margin_rate", float("nan")),
        ("tw_short_maintenance_ratio", 1.0),
        ("tw_short_maintenance_ratio", float("inf")),
        ("tw_short_handling_fee_rate", -0.001),
        ("tw_short_handling_fee_rate", 1.001),
    ],
)
def test_execution_config_rejects_invalid_rates(
    tmp_path: Path,
    field_name: str,
    bad_value: object,
) -> None:
    with pytest.raises(ValueError):
        load_config(_write_config(tmp_path, {field_name: bad_value}))


@pytest.mark.parametrize(
    "bad_value",
    ["", "immediate", "weekly", 1, True, None],
)
def test_execution_config_rejects_invalid_commission_rebate_timing(
    tmp_path: Path,
    bad_value: object,
) -> None:
    with pytest.raises(ValueError, match="commission_rebate_timing"):
        load_config(
            _write_config(
                tmp_path,
                {"tw_commission_rebate_timing": bad_value},
            )
        )


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("tw_settlement_lag_sessions", 0),
        ("tw_settlement_lag_sessions", 2.0),
        ("tw_cash_lot_size", -1),
        ("tw_cash_lot_size", True),
        ("tw_day_trade_lot_size", 0),
        ("tw_day_trade_lot_size", 1000.5),
        ("tw_short_lot_size", 0),
        ("tw_short_lot_size", 1000.5),
        ("tw_short_lot_size", True),
    ],
)
def test_execution_config_rejects_non_positive_or_non_integer_units(
    tmp_path: Path,
    field_name: str,
    bad_value: object,
) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        load_config(_write_config(tmp_path, {field_name: bad_value}))
