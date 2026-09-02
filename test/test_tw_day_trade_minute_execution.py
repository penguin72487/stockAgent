from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.config import load_config
from stockagent.backtest.simulator import (
    BacktestResult,
    run_backtest_integer_shares,
    run_backtest_torch,
)
from stockagent.backtest.tw_day_trade_minute import (
    MARGIN_FINANCING_ONE_DAY_RATE,
    MARGIN_SHORT_HANDLING_FEE_RATE,
    MARGIN_SHORT_ONE_DAY_BORROW_RATE,
    run_tw_day_trade_minute_execution,
)
from stockagent.data.tw_day_trade_execution import (
    DAILY_PROXY_SESSION_MINUTE_BARS,
    DAY_TRADE_FULL_SESSION_FIELDS,
    DAY_TRADE_FULL_SESSION_MINUTES,
    DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME,
    DAY_TRADE_EXECUTION_FIELD_COUNT,
    DayTradeExecutionField as F,
    FULL_SESSION_PRICE,
    FULL_SESSION_VOLUME_SHARES,
    load_tw_day_trade_execution_tape,
)
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _mode_artifact_contract_for_config


def _tape(days: int = 1, symbols: int = 1) -> torch.Tensor:
    tape = torch.full((days, symbols, DAY_TRADE_EXECUTION_FIELD_COUNT), float("nan"))
    tape[:, :, F.OFFICIAL_OPEN] = 100.0
    tape[:, :, F.OFFICIAL_CLOSE] = 102.0
    tape[:, :, F.ENTRY_VWAP_0901] = 101.0
    tape[:, :, F.ENTRY_VOLUME_0901] = 20_000.0
    tape[:, :, F.LIMIT_PRICE_1320] = 105.0
    tape[:, :, F.HIGH_1321] = 106.0
    tape[:, :, F.LOW_1321] = 104.0
    tape[:, :, F.VOLUME_1321] = 4_000.0
    tape[:, :, F.HIGH_1322] = 105.0
    tape[:, :, F.LOW_1322] = 105.0
    tape[:, :, F.VOLUME_1322] = 100_000.0
    tape[:, :, F.HIGH_1323] = 105.0
    tape[:, :, F.LOW_1323] = 105.0
    tape[:, :, F.VOLUME_1323] = 100_000.0
    tape[:, :, F.MARKET_VWAP_1324] = 103.0
    tape[:, :, F.MARKET_VOLUME_1324] = 4_000.0
    tape[:, :, F.MARKET_VWAP_1325] = 102.5
    tape[:, :, F.MARKET_VOLUME_1325] = 0.0
    tape[:, :, F.AUCTION_PRICE_1330] = 102.0
    tape[:, :, F.AUCTION_VOLUME_1330] = 4_000.0
    return tape


def _run(
    weights: torch.Tensor,
    tape: torch.Tensor,
    *,
    buy_fee_rates: torch.Tensor | None = None,
    sell_fee_rates: torch.Tensor | None = None,
    normal_sell_fee_rates: torch.Tensor | None = None,
    commission_rebate_rates: torch.Tensor | None = None,
):
    mask = torch.ones_like(weights, dtype=torch.bool)
    zeros = torch.zeros(weights.size(1))
    buy_fees = zeros if buy_fee_rates is None else buy_fee_rates
    sell_fees = zeros if sell_fee_rates is None else sell_fee_rates
    normal_sell_fees = (
        sell_fees if normal_sell_fee_rates is None else normal_sell_fee_rates
    )
    return run_tw_day_trade_minute_execution(
        weights,
        tape,
        mask,
        mask,
        mask,
        mask,
        mask,
        buy_fees,
        sell_fees,
        normal_sell_fees,
        commission_rebate_rates=commission_rebate_rates,
        initial_capital_twd=1_000_000.0,
    )


def test_execution_tape_loader_rejects_missing_or_empty_root(tmp_path) -> None:
    dates = np.asarray(["2020-03-02"], dtype="datetime64[D]")
    opens = np.asarray([[100.0]], dtype=np.float32)
    closes = np.asarray([[101.0]], dtype=np.float32)
    volumes = np.asarray([[20_000.0]], dtype=np.float32)
    missing = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="root does not exist"):
        load_tw_day_trade_execution_tape(
            missing,
            panel_dates=dates,
            panel_symbols=["2330"],
            official_open_prices=opens,
            official_close_prices=closes,
            daily_volume_shares=volumes,
        )

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(FileNotFoundError, match="contains no"):
        load_tw_day_trade_execution_tape(
            empty,
            panel_dates=dates,
            panel_symbols=["2330"],
            official_open_prices=opens,
            official_close_prices=closes,
            daily_volume_shares=volumes,
        )


def test_pre_minute_daily_proxy_is_directional_adverse_and_does_not_fill_later_gaps(
    tmp_path,
) -> None:
    # The existence of this canonical partition defines the minute-data
    # boundary.  Neither requested panel row reads it, so an empty placeholder
    # is sufficient for this loader-boundary unit test.
    partition = tmp_path / "minute" / "trade_date=2020-03-02"
    partition.mkdir(parents=True)
    (partition / "data.parquet").touch()
    dates = np.asarray(["2014-01-06", "2020-03-03"], dtype="datetime64[D]")
    tape = load_tw_day_trade_execution_tape(
        tmp_path / "minute",
        panel_dates=dates,
        panel_symbols=["2330"],
        official_open_prices=np.asarray([[100.0], [200.0]], dtype=np.float32),
        official_close_prices=np.asarray([[110.0], [210.0]], dtype=np.float32),
        daily_volume_shares=np.asarray(
            [[1_000_000.0], [1_000_000.0]], dtype=np.float32
        ),
    )

    # At the 100-dollar bucket boundary, the legal tick is asymmetric:
    # one tick up is 0.5, while one tick down enters the 0.1 bucket.
    assert tape[0, 0, F.DAILY_PROXY_FLAG] == pytest.approx(1.0)
    assert tape[0, 0, F.DAILY_PROXY_LONG_ENTRY_PRICE] == pytest.approx(100.5)
    assert tape[0, 0, F.DAILY_PROXY_SHORT_ENTRY_PRICE] == pytest.approx(99.9)
    assert tape[0, 0, F.DAILY_PROXY_LONG_EXIT_PRICE] == pytest.approx(109.5)
    assert tape[0, 0, F.DAILY_PROXY_SHORT_EXIT_PRICE] == pytest.approx(110.5)
    assert tape[0, 0, F.DAILY_PROXY_VOLUME] == pytest.approx(
        1_000_000.0 / DAILY_PROXY_SESSION_MINUTE_BARS
    )
    assert tape[0, 0, F.OFFICIAL_CLOSE] == pytest.approx(110.0)

    # Missing rows after minute history begins remain fail-closed.  The loader
    # must not silently convert arbitrary minute holes into daily proxies.
    assert tape[1, 0, F.DAILY_PROXY_FLAG] == pytest.approx(0.0)
    assert np.isnan(tape[1, 0, F.ENTRY_VWAP_0901])


def test_strict_minute_tape_rejects_any_pre_partition_proxy_row(tmp_path) -> None:
    partition = tmp_path / "minute" / "trade_date=2020-03-02"
    partition.mkdir(parents=True)
    (partition / "data.parquet").touch()

    with pytest.raises(ValueError, match="strict minute execution forbids"):
        load_tw_day_trade_execution_tape(
            tmp_path / "minute",
            panel_dates=np.asarray(["2019-12-31"], dtype="datetime64[D]"),
            panel_symbols=["2330"],
            official_open_prices=np.asarray([[100.0]], dtype=np.float32),
            official_close_prices=np.asarray([[101.0]], dtype=np.float32),
            daily_volume_shares=np.asarray([[1_000_000.0]], dtype=np.float32),
            allow_daily_proxy=False,
        )


def test_daily_proxy_prices_are_used_inside_exact_loss_forward_and_backward() -> None:
    tape = torch.full((2, 1, DAY_TRADE_EXECUTION_FIELD_COUNT), float("nan"))
    tape[:, :, F.OFFICIAL_OPEN] = 100.0
    tape[:, :, F.DAILY_PROXY_LONG_ENTRY_PRICE] = 100.5
    tape[:, :, F.DAILY_PROXY_SHORT_ENTRY_PRICE] = 99.9
    tape[:, :, F.DAILY_PROXY_LONG_EXIT_PRICE] = 109.5
    tape[:, :, F.DAILY_PROXY_SHORT_EXIT_PRICE] = 110.5
    tape[:, :, F.DAILY_PROXY_VOLUME] = 20_000.0
    tape[:, :, F.DAILY_PROXY_FLAG] = 1.0
    weights = torch.tensor([[0.10], [-0.10]], requires_grad=True)

    returns, turnover, history, _ = _run(weights, tape)
    assert torch.allclose(
        returns.exp() - 1.0,
        torch.tensor([0.0090, -0.0106 / 1.0090]),
        atol=1.0e-7,
    )
    assert torch.allclose(history[0, 0], torch.tensor(0.1005), atol=1.0e-7)
    assert history[1, 0] < 0.0
    assert torch.all(turnover > 0.0)
    (-returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0.0


def _full_session_tape(days: int = 1, symbols: int = 1) -> torch.Tensor:
    tape = torch.full(
        (
            days,
            symbols,
            DAY_TRADE_FULL_SESSION_MINUTES,
            DAY_TRADE_FULL_SESSION_FIELDS,
        ),
        float("nan"),
    )
    tape[:, :, :, FULL_SESSION_VOLUME_SHARES] = 0.0
    tape[:, :, 0, FULL_SESSION_PRICE] = 100.0
    tape[:, :, 1, FULL_SESSION_PRICE] = 100.0
    tape[:, :, 1, FULL_SESSION_VOLUME_SHARES] = 5_000.0
    tape[:, :, 2, FULL_SESSION_PRICE] = 102.0
    tape[:, :, 2, FULL_SESSION_VOLUME_SHARES] = 5_000.0
    tape[:, :, 261, FULL_SESSION_PRICE] = 110.0
    tape[:, :, 261, FULL_SESSION_VOLUME_SHARES] = 4_000.0
    tape[:, :, 262, FULL_SESSION_PRICE] = 108.0
    tape[:, :, 262, FULL_SESSION_VOLUME_SHARES] = 6_000.0
    tape[:, :, 270, FULL_SESSION_PRICE] = 107.0
    return tape


def test_full_volume_order_carries_entry_and_exit_across_minutes() -> None:
    tape = _full_session_tape()
    mask = torch.ones((1, 1), dtype=torch.bool)
    zeros = torch.zeros(1)
    result = run_tw_day_trade_minute_execution(
        torch.tensor([[1.0]]),
        tape,
        mask,
        mask,
        mask,
        mask,
        mask,
        zeros,
        zeros,
        zeros,
        initial_capital_twd=1_000_000.0,
        maximum_volume_participation=1.0,
    )

    # Requested 10 lots: 5 fill at 09:01 and 5 at 09:02. Exit fills 4 lots
    # at 13:21 and 6 at 13:22. No 13:30 accounting conversion is required.
    assert torch.allclose(
        result.weights_history,
        torch.tensor([[1.01]]),
        atol=1.0e-7,
    )
    assert torch.allclose(
        result.strategy_returns.exp() - 1.0,
        torch.tensor([0.078]),
        atol=1.0e-7,
    )
    assert torch.allclose(result.turnovers, torch.tensor([2.098]), atol=1.0e-7)


def test_full_volume_execution_retains_action_gradient() -> None:
    weights = torch.tensor([[1.0]], requires_grad=True)
    mask = torch.ones((1, 1), dtype=torch.bool)
    zeros = torch.zeros(1)
    result = run_tw_day_trade_minute_execution(
        weights,
        _full_session_tape(),
        mask,
        mask,
        mask,
        mask,
        mask,
        zeros,
        zeros,
        zeros,
        initial_capital_twd=1_000_000.0,
        maximum_volume_participation=1.0,
    )
    (-result.strategy_returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0.0


def test_exact_minute_loss_applies_twenty_percent_commission_at_daily_close() -> None:
    gross_commission = 0.001425
    rebate = gross_commission * 0.8
    result = _run(
        torch.tensor([[0.10]]),
        _tape(),
        buy_fee_rates=torch.tensor([gross_commission]),
        sell_fee_rates=torch.tensor([gross_commission + 0.0015]),
        normal_sell_fee_rates=torch.tensor([gross_commission + 0.0030]),
        commission_rebate_rates=torch.tensor([rebate]),
    )
    expected_pnl = (
        1_000.0 * (105.0 - 101.0)
        - 1_000.0 * 101.0 * (gross_commission - rebate)
        - 1_000.0 * 105.0 * (gross_commission - rebate + 0.0015)
    )
    assert torch.allclose(
        result.strategy_returns.exp() - 1.0,
        torch.tensor([expected_pnl / 1_000_000.0]),
        atol=1.0e-7,
    )


def test_tensor_backtest_passes_commission_rebate_into_exact_minute_loss() -> None:
    weights = torch.tensor([[0.10]])
    mask = torch.ones_like(weights, dtype=torch.bool)
    gross_commission = torch.tensor([0.001425])
    rebate = torch.tensor([0.001425 * 0.8])
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(1),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        execution_mode="tw_day_trade",
        buy_fee_rates=gross_commission,
        sell_fee_rates=gross_commission + 0.0015,
        normal_sell_fee_rates=gross_commission + 0.0030,
        commission_rebate_rates=rebate,
        commission_rebate_timing="daily_close",
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        overnight_returns=_tape(),
        day_trade_execution_initial_capital=1_000_000.0,
    )
    expected_pnl = (
        1_000.0 * (105.0 - 101.0)
        - 1_000.0 * 101.0 * 0.000285
        - 1_000.0 * 105.0 * 0.001785
    )
    assert torch.allclose(
        result.strategy_returns.exp() - 1.0,
        torch.tensor([expected_pnl / 1_000_000.0]),
        atol=1.0e-7,
    )


def test_hybrid_config_inherits_canonical_multi_basis_10m_baseline() -> None:
    baseline = load_config(
        "configs/markets/"
        "tw_day_trade_daily_multi_basis_projection_l1_tplus2_close_capital10m.yaml"
    )
    hybrid = load_config("configs/markets/tw_day_trade_1m_realistic.yaml")

    assert hybrid.trading.volume_participation_equity == pytest.approx(10_000_000.0)
    assert hybrid.trading.tw_commission_rate == pytest.approx(0.001425)
    assert hybrid.trading.tw_commission_discount == pytest.approx(0.2)
    assert hybrid.training.model_name == "financial_transformer"
    assert hybrid.training.epochs == 1_000
    assert hybrid.training.financial_transformer.portfolio_output_mode == "projection_l1"
    assert hybrid.training.financial_transformer.temporal_basis_input == (
        baseline.training.financial_transformer.temporal_basis_input
    )
    assert hybrid.training.financial_transformer.temporal_basis_families == (
        baseline.training.financial_transformer.temporal_basis_families
    )
    assert hybrid.training.financial_transformer.temporal_basis_components == (
        baseline.training.financial_transformer.temporal_basis_components
    )
    # The realistic run consumes the complete causally admissible feature ABI
    # declared by the canonical baseline.  It must not inherit the deployment
    # config's legacy research exclusions (which reduced the live panel to 24
    # columns).  Snapshot-only families are absent from feature_include and
    # remain rejected centrally by load_config.
    assert hybrid.data.feature_include == baseline.data.feature_include
    assert len(hybrid.data.feature_include) == 99
    assert hybrid.data.feature_exclude == []
    assert hybrid.runner.output_dir.endswith(
        "capital10m_all_features_causal_rms_v8"
    )
    assert hybrid.training.financial_transformer.causal_feature_rms_normalization
    assert (
        hybrid.training.financial_transformer.causal_feature_min_active_dates
        == 1
    )
    assert {
        "open_logret_1d",
        "twpub_margin_balance_log",
        "twpub_foreign_net_buy_flow",
        "twpub_dividend_cash_per_share",
        "twpub_cbc_overnight_rate",
        "twpub_taifex_tx_open_interest_log",
        "next_session_open_gap_logret",
    }.issubset(hybrid.data.feature_include)
    assert hybrid.walk_forward.lookback_context == "panel_history"
    assert hybrid.training.record_epoch_curve
    assert hybrid.training.curve_plot_interval == 1
    assert not hybrid.training.defer_epoch_curve_plot_until_end


def test_strict_minute_config_uses_exact_all_feature_cash_capable_contract() -> None:
    config = load_config(
        "configs/markets/tw_day_trade_1m_strict_exact_2020.yaml"
    )
    model = config.training.executable_portfolio_transformer

    assert config.data.panel_start_date == "2020-03-02"
    assert config.data.day_trade_minute_execution_allow_daily_proxy is False
    assert config.data.day_trade_minute_execution_root is not None
    assert config.data.feature_exclude == []
    assert len(config.data.feature_include) == 99
    assert config.walk_forward.expected_first_year == 2020
    assert config.walk_forward.require_future_test_year is True
    assert config.walk_forward.lookback_context == "panel_history"

    assert config.trading.volume_participation_equity == pytest.approx(
        10_000_000.0
    )
    assert config.trading.max_volume_participation == pytest.approx(0.5)
    assert config.trading.tw_commission_discount == pytest.approx(0.2)
    assert config.trading.tw_day_trade_unlimited_margin_conversion is False
    assert config.trading.tw_short_capacity_limit_enabled is False

    assert config.training.model_name == "executable_portfolio_transformer"
    assert config.training.epochs == 1_000
    assert config.training.batch_size_train == 64
    assert config.training.batch_size_eval == 32
    assert config.training.batch_size_train.bit_count() == 1
    assert config.training.batch_size_eval.bit_count() == 1
    assert model.portfolio_output_mode == "projection_l1"
    assert model.center_long_short_logits is False
    assert model.projection_l1_scale_by_active_count is True
    assert model.feature_bottleneck_dim == 32
    assert model.causal_feature_rms_normalization is True
    assert model.use_execution_context_features is True
    assert len(model.temporal_basis_families) == 22
    assert sum(model.temporal_basis_components_by_family.values()) == 524
    assert config.training.record_epoch_curve is True
    assert config.training.curve_plot_interval == 1
    assert config.training.curve_test_interval == 1
    assert config.training.defer_epoch_curve_plot_until_end is False
    assert all(
        getattr(config.training.multitask_loss, name) == 0.0
        for name in (
            "rank_ic_weight",
            "return_rank_ic_weight",
            "direction_weight",
            "volatility_regime_weight",
            "concentration_weight",
            "net_exposure_weight",
        )
    )


def test_hybrid_and_strict_configs_keep_separate_checkpoint_abis() -> None:
    hybrid = load_config("configs/markets/tw_day_trade_1m_realistic.yaml")
    strict = load_config("configs/markets/tw_day_trade_1m_strict_exact_2020.yaml")

    assert hybrid.data.day_trade_minute_execution_allow_daily_proxy is True
    assert hybrid.training.model_name == "financial_transformer"
    assert hybrid.training.financial_transformer.feature_bottleneck_dim == 0
    assert hybrid.runner.output_dir != strict.runner.output_dir


def test_executable_minute_model_rejects_proxy_enabled_config(tmp_path) -> None:
    base = Path(
        "configs/markets/tw_day_trade_1m_strict_exact_2020.yaml"
    ).resolve()
    invalid = tmp_path / "invalid_proxy_enabled.yaml"
    invalid.write_text(
        f"base_config: {base}\n"
        "data:\n"
        "  day_trade_minute_execution_allow_daily_proxy: true\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="allow_daily_proxy=false"):
        load_config(invalid)


def test_full_volume_sub_lot_forward_is_flat_but_gradient_can_learn_concentration() -> None:
    weights = torch.tensor([[0.05]], requires_grad=True)
    mask = torch.ones((1, 1), dtype=torch.bool)
    zeros = torch.zeros(1)
    result = run_tw_day_trade_minute_execution(
        weights,
        _full_session_tape(),
        mask,
        mask,
        mask,
        mask,
        mask,
        zeros,
        zeros,
        zeros,
        initial_capital_twd=1_000_000.0,
        maximum_volume_participation=1.0,
    )

    assert result.weights_history.item() == 0.0
    assert result.strategy_returns.item() == 0.0
    (-result.strategy_returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0.0


def test_full_session_loader_preserves_every_right_labelled_minute(
    tmp_path,
) -> None:
    root = tmp_path / "minute"
    day_root = root / "trade_date=2026-08-13"
    day_root.mkdir(parents=True)
    (root / "manifest.json").write_text(
        '{"schema_version":4,"source":"shioaji_kbars_1m",'
        '"research_ready":true,"status":"research_ready",'
        '"dates":["2026-08-13"],"partitions":[{}]}',
        encoding="utf-8",
    )
    pl.DataFrame(
        {
            "symbol": ["2330", "2330", "2330"],
            "minutes_from_open": [1, 2, 270],
            "Close": [100.0, 102.0, 110.0],
            "Amount": [500_000.0, 510_000.0, 550_000.0],
            "volume_shares": [5_000.0, 5_000.0, 5_000.0],
        }
    ).write_parquet(day_root / "data.parquet")

    tape = load_tw_day_trade_execution_tape(
        root,
        panel_dates=np.asarray(["2026-08-13"], dtype="datetime64[D]"),
        panel_symbols=["2330"],
        official_open_prices=np.asarray([[99.0]], dtype=np.float64),
        cache_dir=tmp_path / "cache",
        policy=DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME,
    )

    assert tape.shape == (1, 1, 271, 2)
    assert tape[0, 0, 0, FULL_SESSION_PRICE] == pytest.approx(99.0)
    assert tape[0, 0, 1, FULL_SESSION_PRICE] == pytest.approx(100.0)
    assert tape[0, 0, 2, FULL_SESSION_PRICE] == pytest.approx(102.0)
    assert tape[0, 0, 2, FULL_SESSION_VOLUME_SHARES] == pytest.approx(5_000.0)
    assert tape[0, 0, 270, FULL_SESSION_PRICE] == pytest.approx(110.0)
    assert tape[0, 0, 3, FULL_SESSION_VOLUME_SHARES] == 0.0


def test_multi_basis_full_volume_config_is_an_independent_contract() -> None:
    config = load_config(
        "configs/markets/"
        "tw_day_trade_daily_multi_basis_projection_l1_minute_volume100_capital10m.yaml"
    )
    assert config.trading.execution_mode == "tw_day_trade"
    assert config.trading.max_volume_participation == 1.0
    assert config.trading.volume_participation_equity == 10_000_000.0
    assert not config.trading.tw_day_trade_unlimited_margin_conversion
    assert (
        config.data.day_trade_minute_execution_policy
        == DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME
    )
    assert config.training.transformer_base_portfolio.temporal_basis_input == (
        "input_features"
    )
    assert config.training.financial_transformer.portfolio_output_mode == (
        "projection_l1"
    )


def test_daily_target_executes_first_minute_then_limit_market_and_auction() -> None:
    # 10% / official open 100 = exactly 1,000 requested shares.  The first
    # later bar penetrates the 105 sell limit and has 2,000-share capacity, so
    # the complete position exits there; equality-only later bars are inert.
    returns, turnover, history, equity = _run(torch.tensor([[0.10]]), _tape())
    assert torch.allclose(history, torch.tensor([[0.101]]))
    assert torch.allclose(returns.exp() - 1.0, torch.tensor([0.004]), atol=1e-7)
    assert torch.allclose(
        turnover, torch.tensor([(101_000.0 + 105_000.0) / 1_000_000.0])
    )
    assert torch.allclose(equity, torch.tensor([1.004]))


def test_touch_without_penetration_does_not_claim_queue_fill() -> None:
    tape = _tape()
    tape[:, :, F.HIGH_1321] = 105.0
    returns, _, _, _ = _run(torch.tensor([[0.10]]), tape)
    # 13:24 has enough capacity, so the exact forward PnL is 1,000*(103-101).
    assert torch.allclose(returns.exp() - 1.0, torch.tensor([0.002]), atol=1e-7)


def test_market_exit_continues_at_1325_before_auction_or_margin_conversion() -> None:
    tape = _tape()
    tape[:, :, F.HIGH_1321] = 105.0
    tape[:, :, F.MARKET_VOLUME_1324] = 0.0
    tape[:, :, F.MARKET_VOLUME_1325] = 4_000.0
    returns, turnover, _, _ = _run(torch.tensor([[0.10]]), tape)

    # The 13:24 attempt has no executable depth; the same position continues
    # to the real 13:25 VWAP instead of jumping directly to the auction.
    assert torch.allclose(
        returns.exp() - 1.0,
        torch.tensor([(102.5 - 101.0) * 1_000.0 / 1_000_000.0]),
        atol=1.0e-7,
    )
    assert torch.allclose(
        turnover,
        torch.tensor([(101_000.0 + 102_500.0) / 1_000_000.0]),
        atol=1.0e-7,
    )


def test_residual_long_and_short_use_unlimited_margin_costs_without_default() -> None:
    tape = _tape(symbols=2)
    for field in (
        F.VOLUME_1321,
        F.VOLUME_1322,
        F.VOLUME_1323,
        F.MARKET_VOLUME_1324,
        F.MARKET_VOLUME_1325,
        F.AUCTION_VOLUME_1330,
    ):
        tape[:, :, field] = 0.0
    buy_fees = torch.full((2,), 0.000855)
    day_trade_sell_fees = torch.full((2,), 0.002355)
    normal_sell_fees = torch.full((2,), 0.003855)
    returns, turnover, _, _ = _run(
        torch.tensor([[0.10, -0.10]]),
        tape,
        buy_fee_rates=buy_fees,
        sell_fee_rates=day_trade_sell_fees,
        normal_sell_fee_rates=normal_sell_fees,
    )
    long_pnl = (
        1_000.0 * (102.0 - 101.0)
        - 1_000.0 * 101.0 * 0.000855
        - 1_000.0 * 102.0 * 0.003855
        - 1_000.0 * 102.0 * MARGIN_FINANCING_ONE_DAY_RATE
    )
    short_pnl = (
        1_000.0 * (101.0 - 102.0)
        - 1_000.0 * 101.0 * 0.002355
        - 1_000.0 * 102.0 * 0.000855
        - 1_000.0 * 101.0 * (0.003855 - 0.002355)
        - 1_000.0 * 101.0 * MARGIN_SHORT_HANDLING_FEE_RATE
        - 1_000.0 * 102.0 * MARGIN_SHORT_ONE_DAY_BORROW_RATE
    )
    expected = (long_pnl + short_pnl) / 1_000_000.0
    assert torch.allclose(returns.exp() - 1.0, torch.tensor([expected]), atol=1e-7)
    assert torch.allclose(
        turnover,
        torch.tensor([(2.0 * 101_000.0 + 2.0 * 102_000.0) / 1_000_000.0]),
    )


def test_residual_margin_conversion_does_not_carry_position_to_next_day() -> None:
    tape = _tape(days=2)
    for field in (
        F.VOLUME_1321,
        F.VOLUME_1322,
        F.VOLUME_1323,
        F.MARKET_VOLUME_1324,
        F.MARKET_VOLUME_1325,
        F.AUCTION_VOLUME_1330,
    ):
        tape[:, :, field] = 0.0
    returns, turnover, history, _ = _run(torch.tensor([[0.10], [0.10]]), tape)
    assert torch.isfinite(returns).all()
    assert torch.all(history > 0.0)
    assert torch.all(turnover > 0.0)


def test_canonical_result_has_no_settlement_or_cross_day_position_state() -> None:
    weights = torch.tensor([[0.10], [0.10]])
    mask = torch.ones_like(weights, dtype=torch.bool)
    tape = _tape(days=2)
    for field in (
        F.VOLUME_1321,
        F.VOLUME_1322,
        F.VOLUME_1323,
        F.MARKET_VOLUME_1324,
        F.AUCTION_VOLUME_1330,
    ):
        tape[:, :, field] = 0.0
    zeros = torch.zeros(1)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(2),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        execution_mode="tw_day_trade",
        buy_fee_rates=zeros,
        sell_fee_rates=zeros,
        normal_sell_fee_rates=zeros,
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        overnight_returns=tape,
        day_trade_execution_initial_capital=1_000_000.0,
    )
    assert torch.equal(result.final_weights, torch.zeros_like(result.final_weights))
    assert result.settlement_default is not None
    assert not bool(result.settlement_default.any())
    assert result.cash_history is not None
    assert torch.equal(result.cash_history, torch.ones_like(result.cash_history))


def test_stateful_carry_rejects_legacy_minute_executor_that_drops_residuals() -> None:
    weights = torch.tensor([[0.10]])
    mask = torch.ones_like(weights, dtype=torch.bool)
    zeros_mask = torch.zeros_like(mask)
    zeros = torch.zeros(1)
    with pytest.raises(ValueError, match="discards per-symbol residual holdings"):
        run_backtest_torch(
            weights,
            torch.zeros_like(weights),
            mask,
            torch.zeros(1),
            0.0,
            0.0,
            long_only=False,
            portfolio_activation="pre_normalized",
            can_buy_mask=mask,
            can_sell_mask=mask,
            can_short_open_mask=mask,
            can_short_open_open_mask=mask,
            force_short_cover_mask=zeros_mask,
            force_exit_mask=zeros_mask,
            short_margin_rate=torch.full_like(weights, 0.9),
            short_capacity_weights=torch.full_like(weights, float("inf")),
            unresolved_corporate_action_mask=zeros_mask,
            execution_mode="tw_day_trade",
            buy_fee_rates=zeros,
            sell_fee_rates=zeros,
            normal_sell_fee_rates=zeros,
            day_trade_eligible_mask=mask,
            day_trade_can_buy_open_mask=mask,
            day_trade_can_sell_open_mask=mask,
            day_trade_unlimited_margin_conversion=True,
            overnight_returns=_tape(days=1),
        )


def test_t_profit_settles_after_t_plus_2_close_and_sizes_only_t_plus_3() -> None:
    tape = _tape(days=4)
    # T earns exactly one initial-capital unit from one 1,000-share lot.
    tape[0, :, F.LIMIT_PRICE_1320] = 1_101.0
    tape[0, :, F.HIGH_1321] = 1_102.0
    # Later sessions close flat at the entry price.  Their entries reveal
    # whether the pending T gain was incorrectly used for sizing.
    tape[1:, :, F.LIMIT_PRICE_1320] = 101.0
    tape[1:, :, F.HIGH_1321] = 102.0
    result = _run(torch.full((4, 1), 0.10), tape)

    # T economic PnL is recognized immediately by the loss/NAV.
    assert torch.allclose(
        result.strategy_returns.exp() - 1.0,
        torch.tensor([1.0, 0.0, 0.0, 0.0]),
        atol=1.0e-7,
    )
    # It remains a receivable through T+1. T+2 orders still use cash=1.0;
    # only after those orders does the close settlement raise cash to 2.0.
    assert torch.allclose(
        result.cash_history,
        torch.tensor([1.0, 1.0, 2.0, 2.0]),
        atol=1.0e-7,
    )
    assert torch.allclose(
        result.receivables_history,
        torch.tensor([[0.0, 1.0], [1.0, 0.0], [0.0, 0.0], [0.0, 0.0]]),
        atol=1.0e-7,
    )
    # Relative to economic NAV=2, T+1/T+2 execute one lot (5.05%), while
    # T+3 can finally use the settled cash and executes two lots (10.10%).
    assert torch.allclose(
        result.weights_history[:, 0],
        torch.tensor([0.1010, 0.0505, 0.0505, 0.1010]),
        atol=1.0e-7,
    )


def test_canonical_minute_result_carries_only_net_t_plus_2_claim() -> None:
    weights = torch.tensor([[0.10], [0.0], [0.0]])
    mask = torch.ones_like(weights, dtype=torch.bool)
    tape = _tape(days=3)
    zeros = torch.zeros(1)
    result = run_backtest_torch(
        weights,
        torch.zeros_like(weights),
        mask,
        torch.zeros(3),
        0.0,
        0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        execution_mode="tw_day_trade",
        buy_fee_rates=zeros,
        sell_fee_rates=zeros,
        normal_sell_fee_rates=zeros,
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        overnight_returns=tape,
        day_trade_execution_initial_capital=1_000_000.0,
        settlement_lag_sessions=2,
    )
    assert result.receivables_history is not None
    assert result.payables_history is not None
    assert result.cash_history is not None
    assert torch.allclose(
        result.receivables_history,
        torch.tensor([[0.0, 0.004], [0.004, 0.0], [0.0, 0.0]]),
        atol=1.0e-7,
    )
    assert torch.equal(
        result.payables_history, torch.zeros_like(result.payables_history)
    )
    assert torch.allclose(
        result.cash_history, torch.tensor([1.0, 1.0, 1.004]), atol=1.0e-7
    )
    assert torch.equal(result.final_weights, torch.zeros_like(result.final_weights))


def test_log_utility_loss_carries_the_same_t_plus_2_net_claim_state() -> None:
    weights = torch.tensor([[0.10], [0.0], [0.0]], requires_grad=True)
    mask = torch.ones_like(weights, dtype=torch.bool)
    aux: dict[str, torch.Tensor] = {}
    zeros = torch.zeros(1)
    loss = risk_aware_loss(
        weights,
        torch.zeros_like(weights),
        mask,
        benchmark_returns=torch.zeros(3),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        long_only=False,
        portfolio_activation="pre_normalized",
        execution_mode="tw_day_trade",
        buy_fee_rates=zeros,
        sell_fee_rates=zeros,
        normal_sell_fee_rates=zeros,
        day_trade_eligible_mask=mask,
        day_trade_can_buy_open_mask=mask,
        day_trade_can_sell_open_mask=mask,
        settlement_lag_sessions=2,
        objective="log_utility",
        gamma_turnover=0.0,
        rank_ic_weight=0.0,
        return_rank_ic_weight=0.0,
        direction_weight=0.0,
        volatility_regime_weight=0.0,
        concentration_weight=0.0,
        overnight_log_returns=_tape(days=3),
        day_trade_execution_initial_capital=1_000_000.0,
        aux_outputs=aux,
    )
    loss.backward()
    assert torch.isfinite(loss)
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert torch.allclose(aux["_final_cash"], torch.tensor(1.004), atol=1.0e-7)
    assert torch.equal(aux["_final_payables"], torch.zeros_like(aux["_final_payables"]))
    assert torch.equal(
        aux["_final_receivables"], torch.zeros_like(aux["_final_receivables"])
    )


def test_exact_forward_lot_rounding_still_supplies_training_gradient() -> None:
    weights = torch.tensor([[0.151]], requires_grad=True)
    returns, _, _, _ = _run(weights, _tape())
    loss = -returns.mean()
    loss.backward()
    # Forward request is exactly one board lot even though the raw request is
    # 1,510 shares; backward remains useful via the documented STE.
    assert torch.allclose(returns.exp() - 1.0, torch.tensor([0.004]), atol=1e-7)
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0.0


def test_two_sided_request_keeps_side_with_an_executable_lot() -> None:
    tape = _tape(symbols=2)
    tape[:, 1, F.ENTRY_VOLUME_0901] = 0.0
    returns, turnover, history, _ = _run(torch.tensor([[0.10, -0.10]]), tape)
    assert history[0, 0] > 0.0
    assert history[0, 1] == 0.0
    assert turnover[0] > 0.0
    assert returns[0] != 0.0


def test_independent_side_execution_keeps_nonzero_gradient() -> None:
    tape = _tape(symbols=2)
    tape[:, 1, F.ENTRY_VOLUME_0901] = 0.0
    weights = torch.tensor([[0.10, -0.10]], requires_grad=True)
    returns, _, history, _ = _run(weights, tape)
    assert history[0, 0] > 0.0
    assert history[0, 1] == 0.0
    (-returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0.0


def test_missing_exit_prices_fail_closed_without_nan_gradient() -> None:
    tape = _tape()
    for field in (
        F.LIMIT_PRICE_1320,
        F.MARKET_VWAP_1324,
        F.MARKET_VWAP_1325,
        F.AUCTION_PRICE_1330,
        F.OFFICIAL_CLOSE,
    ):
        tape[:, :, field] = float("nan")
    weights = torch.tensor([[0.10]], requires_grad=True)
    returns, _, _, _ = _run(weights, tape)
    assert torch.isfinite(returns).all()
    (-returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_missing_auction_uses_official_close_only_for_margin_conversion() -> None:
    tape = _tape()
    for field in (
        F.VOLUME_1321,
        F.VOLUME_1322,
        F.VOLUME_1323,
        F.MARKET_VOLUME_1324,
        F.MARKET_VOLUME_1325,
        F.AUCTION_PRICE_1330,
        F.AUCTION_VOLUME_1330,
    ):
        tape[:, :, field] = float("nan")
    # Mirrors a limit-locked/illiquid short such as the diagnosed 2025-08-12
    # case: a valid entry exists, the minute archive has no auction row, and
    # the official daily close is still a valid valuation observation.
    tape[:, :, F.ENTRY_VWAP_0901] = 57.397245
    tape[:, :, F.OFFICIAL_OPEN] = 57.1
    tape[:, :, F.OFFICIAL_CLOSE] = 61.0
    tape[:, :, F.ENTRY_VOLUME_0901] = 1_089_000.0
    weights = torch.tensor([[-0.10]], requires_grad=True)

    result = _run(
        weights,
        tape,
        buy_fee_rates=torch.tensor([0.000285]),
        sell_fee_rates=torch.tensor([0.001785]),
        normal_sell_fee_rates=torch.tensor([0.003285]),
    )
    simple_return = result.strategy_returns.exp() - 1.0

    # The position loses only its marked price move, fees, and highest-rate
    # margin-short costs.  It must not lose the entire entered principal.
    assert -0.02 < simple_return.item() < 0.0
    (-result.strategy_returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0.0


def test_four_day_block_and_padded_tail_preserve_daily_recurrence() -> None:
    weights = torch.full((6, 1), 0.10, requires_grad=True)
    returns, _, history, equity = _run(weights, _tape(days=6))
    assert returns.shape == (6,)
    assert history.shape == (6, 1)
    expected_simple = 0.004 / (1.0 + torch.arange(6, dtype=torch.float32) * 0.004)
    assert torch.allclose(returns.exp() - 1.0, expected_simple, atol=1.0e-7)
    expected_equity = 1.0 + torch.arange(1, 7, dtype=torch.float32) * 0.004
    assert torch.allclose(equity, expected_equity, atol=1.0e-6)
    (-returns.sum()).backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_exact_minute_result_is_not_replaced_by_open_close_audit() -> None:
    exact = BacktestResult(
        strategy_returns=np.asarray([0.01, -0.02], dtype=np.float64),
        benchmark_returns=np.zeros(2, dtype=np.float64),
        turnovers=np.asarray([0.2, 0.3], dtype=np.float64),
        weights_history=np.zeros((2, 1), dtype=np.float64),
        execution_mode="tw_day_trade",
    )
    result, holdings = run_backtest_integer_shares(
        weights=np.zeros((2, 1), dtype=np.float64),
        future_returns=np.zeros((2, 1), dtype=np.float64),
        tradable_mask=np.ones((2, 1), dtype=bool),
        benchmark_returns=np.zeros(2, dtype=np.float64),
        execution_mode="tw_day_trade",
        precomputed_exact_backtest=exact,
    )
    assert result is exact
    assert holdings == []


def test_pretrained_22_basis_config_keeps_target_execution_loss_contract() -> None:
    config = load_config(
        Path(
            "configs/markets/"
            "tw_day_trade_1m_hybrid_22_effective_rank_pretrained_guard_v11.yaml"
        )
    )
    model = config.training.financial_transformer

    assert config.data.panel_start_date == "2014-01-06"
    assert config.data.feature_exclude == []
    assert len(config.data.feature_include) == 99
    assert config.data.day_trade_minute_execution_allow_daily_proxy is True
    assert config.walk_forward.min_train_years == 1
    assert config.walk_forward.require_future_test_year is True
    assert config.training.epochs == 1000
    assert config.training.model_name == "financial_transformer"
    assert config.training.warm_start_from_previous_fold is False
    assert config.training.pretrained_initialization_validation_guard is True
    assert (
        config.training.pretrained_initialization_feature_adapter
        == "identity_by_feature_name"
    )
    assert config.training.pretrained_initialization_trainable_parameter_prefixes == [
        "candle_encoder.continuous_feature_bottleneck.",
        "score_head.",
    ]
    assert config.training.enable_lr_scheduler is True
    assert config.training.lr_scheduler_warmup_steps == 256
    assert model.feature_bottleneck_dim == 24
    assert model.center_long_short_logits is True
    assert model.projection_l1_scale_by_active_count is False
    assert len(model.temporal_basis_families) == 22
    assert sum(model.temporal_basis_components_by_family.values()) == 524
    assert config.trading.volume_participation_equity == pytest.approx(10_000_000.0)
    assert config.trading.tw_commission_discount == pytest.approx(0.2)
    assert config.trading.max_volume_participation == pytest.approx(0.5)

    contract = _mode_artifact_contract_for_config(config)
    assert contract["execution_mode"] == "tw_day_trade"
    assert contract["frequency"] == "daily_policy_exact_minute_execution"
    assert contract["sample_order_contract"] == "strict_chronological_sessions"
    assert contract["mode_details"] == {
        "execution_variant": "exact_board_lot_minute_event_tape_v1",
        "daily_policy_decisions_per_session": 1,
        "daily_proxy_allowed": True,
    }


def test_v12_uses_corrected_tape_and_fold_matched_v11_epoch_zero() -> None:
    config = load_config(
        Path(
            "configs/markets/"
            "tw_day_trade_1m_hybrid_22_effective_rank_pretrained_guard_v12.yaml"
        )
    )

    assert config.runner.output_dir.endswith(
        "pretrained_exact_official_close_commission20_capital10m_all_features_v12"
    )
    assert config.data.day_trade_minute_execution_cache_dir.endswith(
        "tw_day_trade_execution_v7_official_close_1325"
    )
    assert config.training.pretrained_initialization_root is not None
    assert config.training.pretrained_initialization_root.endswith(
        "pretrained_exact_guard_commission20_capital10m_all_features_v11"
    )
    assert config.training.pretrained_initialization_validation_guard is True
    assert config.training.warm_start_from_previous_fold is False
    assert config.training.epochs == 1000
    assert config.training.model_name == "financial_transformer"
    assert len(config.data.feature_include) == 99
    assert config.data.feature_exclude == []
    assert config.trading.volume_participation_equity == pytest.approx(10_000_000.0)
    assert config.trading.tw_commission_discount == pytest.approx(0.2)
    assert config.trading.max_volume_participation == pytest.approx(0.5)
    assert DAY_TRADE_EXECUTION_FIELD_COUNT == 26
