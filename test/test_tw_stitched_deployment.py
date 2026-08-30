from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest

from stockagent.backtest.simulator import BacktestResult
from stockagent.config import ExperimentConfig, load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_portfolio_daily import TaiwanFuturesPortfolioDaily
from stockagent.training import trainer as trainer_module


REPO_ROOT = Path(__file__).resolve().parents[1]


def _day_trade_config() -> ExperimentConfig:
    config = load_config(REPO_ROOT / "configs/experiment_baseline.yaml")
    trading = config.trading
    trading.execution_mode = "tw_day_trade"
    trading.long_only = False
    trading.portfolio_activation = "identity"
    trading.min_trade_weight = 0.0
    trading.max_turnover_ratio = 0.0
    trading.max_volume_participation = 0.0
    trading.volume_participation_equity = 100.0
    trading.tw_commission_rate = 0.0
    trading.tw_commission_discount = 0.0
    trading.tw_stock_sell_tax = 0.0
    trading.tw_etf_sell_tax = 0.0
    trading.tw_day_trade_stock_sell_tax = 0.0
    trading.tw_day_trade_etf_sell_tax = 0.0
    trading.tw_minimum_commission = 0.0
    trading.tw_settlement_lag_sessions = 2
    trading.tw_day_trade_lot_size = 1
    config.training.backtest_artifact_compression = "none"
    return config


def _cash_config() -> ExperimentConfig:
    config = _day_trade_config()
    config.trading.execution_mode = "tw_cash"
    config.trading.long_only = True
    config.trading.tw_cash_lot_size = 1
    return config


def _futures_portfolio_config() -> ExperimentConfig:
    return load_config(
        REPO_ROOT
        / "configs/markets/tw_futures_portfolio_day_multi_basis_projection_l1.yaml"
    )


def _crypto_perpetual_config() -> ExperimentConfig:
    config = load_config(
        REPO_ROOT
        / "configs/markets/bybit_perpetual_daily_multi_basis_projection_l1.yaml"
    )
    config.training.backtest_artifact_compression = "none"
    config.trading.max_volume_participation = 0.01
    config.trading.volume_participation_equity = 1_000_000.0
    return config


def _day_trade_panel(
    close_prices: np.ndarray,
    *,
    symbols: tuple[str, ...] = ("2317", "2330", "0050"),
) -> PanelData:
    closes = np.asarray(close_prices, dtype=np.float32)
    if closes.ndim != 2 or closes.shape[1] != len(symbols):
        raise ValueError("close_prices must align with symbols")
    rows, symbol_count = closes.shape
    opens = np.full((rows, symbol_count), 10.0, dtype=np.float32)
    mask = np.ones((rows, symbol_count), dtype=np.bool_)
    intraday = np.log(closes / opens).astype(np.float32)
    return PanelData(
        dates=(
            np.datetime64("2025-01-02")
            + np.arange(rows).astype("timedelta64[D]")
        ),
        symbols=list(symbols),
        feature_names=["f0"],
        features=np.zeros((rows, symbol_count, 1), dtype=np.float32),
        returns_1d=np.zeros((rows, symbol_count), dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        can_short_open_open_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=closes,
        open_prices=opens,
        daily_volumes=np.full((rows, symbol_count), 1_000_000.0, dtype=np.float32),
        intraday_returns=intraday,
        raw_close_returns_1d=np.zeros((rows, symbol_count), dtype=np.float32),
        day_trade_eligible_mask=mask.copy(),
        day_trade_can_short_open_mask=mask.copy(),
        day_trade_can_buy_open_mask=mask.copy(),
        day_trade_can_sell_open_mask=mask.copy(),
        unresolved_corporate_action_mask=np.zeros_like(mask),
    )


def _futures_portfolio_panel() -> PanelData:
    rows = 4
    symbols = ("TX_M01_L1", "MTX_M01_L1")
    symbol_count = len(symbols)
    mask = np.ones((rows, symbol_count), dtype=np.bool_)
    returns = np.asarray(
        [[0.0, 0.0], [0.10, -0.10], [0.02, 0.03], [0.0, 0.0]],
        dtype=np.float32,
    )
    panel = PanelData(
        dates=np.asarray(
            ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"],
            dtype="datetime64[D]",
        ),
        symbols=list(symbols),
        feature_names=["f0"],
        features=np.zeros((rows, symbol_count, 1), dtype=np.float32),
        returns_1d=returns,
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        can_short_open_open_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=np.full((rows, symbol_count), 100.0, dtype=np.float32),
        open_prices=np.full((rows, symbol_count), 100.0, dtype=np.float32),
        daily_volumes=np.full((rows, symbol_count), 1_000.0, dtype=np.float32),
        force_exit_mask=np.zeros((rows, symbol_count), dtype=np.bool_),
    )
    panel.futures_portfolio_daily = TaiwanFuturesPortfolioDaily(
        dates=panel.dates.copy(),
        symbols=symbols,
        contracts=np.full((rows, symbol_count), "contract", dtype=object),
        holding_log_returns=returns.copy(),
        executable_mask=mask.copy(),
        must_liquidate_mask=np.zeros_like(mask),
        can_hold_overnight_mask=mask.copy(),
        contract_multipliers=np.asarray([200.0, 50.0], dtype=np.float64),
        fee_per_contract_per_side_twd=np.asarray([60.0, 24.0], dtype=np.float64),
        fee_rate_per_open_notional=np.full(
            (rows, symbol_count), 0.01, dtype=np.float32
        ),
        source_path="unit-test",
        manifest_path="unit-test",
    )
    return panel


def _crypto_perpetual_panel() -> PanelData:
    # The final panel row supplies the next 00:05 execution price for the last
    # deployable target. It is not itself a deployment target.
    rows = 4
    symbols = ("BTCUSDT", "ETHUSDT")
    symbol_count = len(symbols)
    mask = np.ones((rows, symbol_count), dtype=np.bool_)
    # Price is flat, while a positive funding rate costs every carried long 1%
    # of notional per row.  The two return surfaces must remain distinct.
    effective_log_returns = np.log1p(
        np.full((rows, symbol_count), -0.01, dtype=np.float64)
    ).astype(np.float32)
    raw_price_log_returns = np.zeros((rows, symbol_count), dtype=np.float32)
    return PanelData(
        dates=np.asarray(
            ["2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
            dtype="datetime64[D]",
        ),
        symbols=list(symbols),
        feature_names=["f0"],
        features=np.zeros((rows, symbol_count, 1), dtype=np.float32),
        returns_1d=effective_log_returns,
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        can_short_open_open_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=np.full((rows, symbol_count), 100.0, dtype=np.float32),
        open_prices=None,
        # Equivalent contract quantity. At 100 USDT, 1% of the completed
        # session's 10M USDT turnover is 0.1 of the configured 1M NAV.
        daily_volumes=np.full(
            (rows, symbol_count), 100_000.0, dtype=np.float32
        ),
        raw_close_returns_1d=raw_price_log_returns,
        force_exit_mask=np.zeros((rows, symbol_count), dtype=np.bool_),
    )


def _fold_result(fold_id: int) -> trainer_module.FoldResult:
    return trainer_module.FoldResult(
        fold_id=fold_id,
        train_years=[],
        val_years=[],
        test_years=[],
        best_val_loss=0.0,
        val_ic={},
        val_metrics={},
        test_ic={},
        test_metrics={},
    )


def _write_fold_requests(
    output_path: Path,
    *,
    fold_id: int,
    dates: np.ndarray,
    symbols: tuple[str, ...],
    requests: np.ndarray,
    execution_mode: str = "tw_day_trade",
) -> trainer_module.FoldResult:
    values = np.asarray(requests, dtype=np.float64)
    rows, symbol_count = values.shape
    assert rows == len(dates)
    assert symbol_count == len(symbols)
    fold_dir = trainer_module._fold_dir(output_path, fold_id)
    fold_dir.mkdir(parents=True, exist_ok=True)
    placeholder = BacktestResult(
        strategy_returns=np.zeros(rows, dtype=np.float64),
        benchmark_returns=np.zeros(rows, dtype=np.float64),
        turnovers=np.zeros(rows, dtype=np.float64),
        weights_history=np.zeros((rows, symbol_count), dtype=np.float64),
        execution_mode=execution_mode,
        settlement_ledger_unit=(
            "notional_weight"
            if execution_mode in {
                "tw_futures_portfolio_day",
                "crypto_perpetual",
            }
            else "currency"
        ),
        requested_weights_history=values,
        cash_history=np.zeros(rows, dtype=np.float64),
        payables_history=np.zeros((rows, 2), dtype=np.float64),
        receivables_history=np.zeros((rows, 2), dtype=np.float64),
        settlement_default=np.zeros(rows, dtype=np.bool_),
        short_sale_collateral_history=(
            np.zeros((rows, symbol_count), dtype=np.float64)
            if execution_mode == "tw_cash"
            else None
        ),
        short_margin_collateral_history=(
            np.zeros((rows, symbol_count), dtype=np.float64)
            if execution_mode == "tw_cash"
            else None
        ),
    )
    trainer_module._save_deployment_test_artifacts(
        fold_dir,
        placeholder,
        np.asarray(dates, dtype="datetime64[D]"),
        symbols=symbols,
    )
    return _fold_result(fold_id)


def _disable_stitched_plots(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "plot_equity_curve",
        "plot_equity_curve_log",
        "plot_annual_performance",
    ):
        monkeypatch.setattr(trainer_module, name, lambda *_args, **_kwargs: None)


def test_stitched_replay_preserves_t2_claim_and_expands_dynamic_symbols(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_stitched_plots(monkeypatch)
    panel = _day_trade_panel(
        np.asarray(
            [
                [10.0, 11.0, 10.0],
                [10.0, 10.0, 10.0],
                [10.0, 10.0, 10.0],
            ]
        )
    )
    fold_one = _write_fold_requests(
        tmp_path,
        fold_id=1,
        dates=panel.dates[:1],
        symbols=("2330",),
        requests=np.asarray([[1.0]]),
    )
    fold_two = _write_fold_requests(
        tmp_path,
        fold_id=2,
        dates=panel.dates[1:],
        symbols=("2317", "2330"),
        requests=np.asarray([[1.0, 0.0], [0.0, 0.0]]),
    )

    stitched = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold_two, fold_one],
        panel=panel,
        config=_day_trade_config(),
    )

    assert stitched is not None
    np.testing.assert_allclose(
        stitched.requested_weights_history,
        np.asarray(
            [
                [0.0, 1.0, 0.0],
                [1.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
            ]
        ),
    )
    # The 2330 profit is born in fold 1, remains due at fold 2's first
    # session, and becomes settled cash only at T+2.
    np.testing.assert_allclose(
        stitched.receivables_history,
        np.asarray([[0.0, 10.0], [10.0, 0.0], [0.0, 0.0]]),
    )
    np.testing.assert_allclose(stitched.cash_history, [100.0, 100.0, 110.0])

    fold_one_replayed, _ = trainer_module._load_backtest_artifact(
        trainer_module._deployment_backtest_path(
            trainer_module._fold_dir(tmp_path, 1)
        )
    )
    fold_two_replayed, _ = trainer_module._load_backtest_artifact(
        trainer_module._deployment_backtest_path(
            trainer_module._fold_dir(tmp_path, 2)
        )
    )
    assert fold_one_replayed.final_integer_state is None
    assert fold_one_replayed.final_receivables is None
    assert fold_two_replayed.final_integer_state is not None
    np.testing.assert_allclose(fold_two_replayed.receivables_history[0], [10.0, 0.0])
    np.testing.assert_allclose(
        fold_two_replayed.requested_weights_history,
        [[1.0, 0.0, 0.0], [0.0, 0.0, 0.0]],
    )
    assert trainer_module._load_deployment_symbols(
        trainer_module._fold_dir(tmp_path, 1),
        3,
    ) == panel.symbols

    # The first replay rewrites each fold into global symbol order. A later
    # refresh must be idempotent when it consumes those rewritten sidecars.
    replayed_again = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold_one, fold_two],
        panel=panel,
        config=_day_trade_config(),
    )
    assert replayed_again is not None
    np.testing.assert_allclose(
        replayed_again.requested_weights_history,
        stitched.requested_weights_history,
    )
    np.testing.assert_allclose(replayed_again.cash_history, stitched.cash_history)
    np.testing.assert_allclose(
        replayed_again.receivables_history,
        stitched.receivables_history,
    )


def test_daily_no_default_stitched_replay_uses_same_fractional_tplus3_forward(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_stitched_plots(monkeypatch)
    panel = _day_trade_panel(
        np.asarray(
            [
                [10.0, 11.0, 10.0],
                [10.0, 10.0, 10.0],
                [10.0, 10.0, 10.0],
            ]
        )
    )
    fold_one = _write_fold_requests(
        tmp_path,
        fold_id=1,
        dates=panel.dates[:1],
        symbols=("2330",),
        requests=np.asarray([[1.0]]),
    )
    fold_two = _write_fold_requests(
        tmp_path,
        fold_id=2,
        dates=panel.dates[1:],
        symbols=("2317", "2330"),
        requests=np.asarray([[1.0, 0.0], [0.0, 0.0]]),
    )
    config = _day_trade_config()
    config.trading.tw_day_trade_unlimited_margin_conversion = True
    # This contract deliberately has no whole-lot rounding.  A 1,000-share
    # audit with only TWD 100 would be identically flat and is not allowed to
    # replace the canonical differentiable daily loss during stitched replay.
    config.trading.tw_day_trade_lot_size = 1000

    stitched = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold_two, fold_one],
        panel=panel,
        config=config,
    )

    assert stitched is not None
    assert stitched.settlement_ledger_unit == "nav_ratio"
    assert stitched.final_integer_state is None
    assert stitched.shares_history is None
    np.testing.assert_allclose(
        stitched.requested_weights_history,
        np.asarray(
            [
                [0.0, 1.0, 0.0],
                [1.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
            ]
        ),
    )
    np.testing.assert_allclose(
        stitched.strategy_returns[0], np.log(1.10), atol=2.0e-8
    )
    np.testing.assert_allclose(
        stitched.receivables_history,
        np.asarray([[0.0, 0.10], [0.10, 0.0], [0.0, 0.0]]),
        atol=1.0e-7,
    )
    np.testing.assert_allclose(stitched.cash_history, [1.0, 1.0, 1.10])
    assert np.count_nonzero(stitched.weights_history) == 2


def test_daily_no_default_stitched_replay_preserves_optional_action_mask(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_stitched_plots(monkeypatch)
    panel = _day_trade_panel(
        np.asarray(
            [
                [10.0, 11.0, 10.0],
                [10.0, 10.0, 10.0],
            ]
        )
    )
    panel.unresolved_corporate_action_mask = None
    panel.corporate_action_avoidance_mask = None
    fold = _write_fold_requests(
        tmp_path,
        fold_id=1,
        dates=panel.dates[:1],
        symbols=("2330",),
        requests=np.asarray([[1.0]]),
    )
    config = _day_trade_config()
    config.trading.tw_day_trade_unlimited_margin_conversion = True

    stitched = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold],
        panel=panel,
        config=config,
    )

    assert stitched is not None
    np.testing.assert_allclose(
        stitched.strategy_returns[0], np.log(1.10), atol=2.0e-8
    )


def test_futures_portfolio_stitched_replay_uses_fixed_fee_side_channel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_stitched_plots(monkeypatch)
    panel = _futures_portfolio_panel()
    fold = _write_fold_requests(
        tmp_path,
        fold_id=1,
        dates=panel.dates[1:],
        symbols=("TX_M01_L1", "MTX_M01_L1"),
        requests=np.asarray(
            [[0.5, -0.5], [0.25, -0.25], [0.0, 0.0]],
            dtype=np.float64,
        ),
        execution_mode="tw_futures_portfolio_day",
    )

    stitched = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold],
        panel=panel,
        config=_futures_portfolio_config(),
    )

    assert stitched is not None
    assert stitched.execution_mode == "tw_futures_portfolio_day"
    assert stitched.settlement_ledger_unit == "notional_weight"
    assert stitched.final_integer_state is None
    np.testing.assert_allclose(
        stitched.requested_weights_history,
        [[0.5, -0.5], [0.25, -0.25], [0.0, 0.0]],
    )
    # The paired long/short request is profitable before costs, while the
    # aligned fixed-fee side channel keeps its net log return below 10%.
    assert 0.0 < float(stitched.strategy_returns[0]) < 0.10
    assert np.count_nonzero(stitched.turnovers) >= 2


def test_crypto_stitched_replay_preserves_funding_state_and_volume_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_stitched_plots(monkeypatch)
    panel = _crypto_perpetual_panel()
    fold_one = _write_fold_requests(
        tmp_path,
        fold_id=1,
        dates=panel.dates[:1],
        symbols=("BTCUSDT",),
        requests=np.asarray([[1.0]], dtype=np.float64),
        execution_mode="crypto_perpetual",
    )
    fold_two = _write_fold_requests(
        tmp_path,
        fold_id=2,
        dates=panel.dates[1:3],
        symbols=("BTCUSDT", "ETHUSDT"),
        requests=np.asarray([[1.0, 0.0], [0.0, 0.0]], dtype=np.float64),
        execution_mode="crypto_perpetual",
    )

    stitched = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold_two, fold_one],
        panel=panel,
        config=_crypto_perpetual_config(),
    )

    assert stitched is not None
    assert stitched.execution_mode == "crypto_perpetual"
    assert stitched.settlement_ledger_unit == "notional_weight"
    assert stitched.final_integer_state is None
    np.testing.assert_allclose(
        stitched.requested_weights_history,
        [[1.0, 0.0], [1.0, 0.0], [0.0, 0.0]],
    )
    # The first entry is limited to 1% of the completed session's 10M USDT
    # turnover divided by the configured 1M NAV: at most 0.1 weight.
    assert stitched.turnovers[0] == pytest.approx(0.1, abs=1.0e-7)
    # Flat price plus a positive funding payment must still reduce NAV, and
    # the carried position must survive the fold-1 -> fold-2 model handoff.
    assert stitched.strategy_returns[0] < 0.0
    assert stitched.weights_history[1, 0] > 0.0
    assert stitched.final_alive is not None
    assert bool(np.asarray(stitched.final_alive).item())


def test_stitched_replay_keeps_ruin_absorbing_across_fold_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_stitched_plots(monkeypatch)
    panel = _day_trade_panel(
        np.asarray(
            [
                [10.0, 30.0, 10.0],
                [20.0, 10.0, 10.0],
                [20.0, 10.0, 10.0],
            ]
        )
    )
    fold_one = _write_fold_requests(
        tmp_path,
        fold_id=1,
        dates=panel.dates[:1],
        symbols=("2330",),
        requests=np.asarray([[-1.0]]),
    )
    fold_two = _write_fold_requests(
        tmp_path,
        fold_id=2,
        dates=panel.dates[1:],
        symbols=("2317",),
        requests=np.asarray([[1.0], [1.0]]),
    )

    stitched = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold_one, fold_two],
        panel=panel,
        config=_day_trade_config(),
    )

    assert stitched is not None
    np.testing.assert_array_equal(stitched.settlement_default, [True, False, False])
    np.testing.assert_allclose(stitched.cash_history, [0.0, 0.0, 0.0])
    np.testing.assert_allclose(stitched.weights_history, np.zeros((3, 3)))
    assert stitched.final_alive is not None
    assert not bool(np.asarray(stitched.final_alive).item())
    assert stitched.final_integer_state is not None
    assert not stitched.final_integer_state.alive

    fold_two_replayed, _ = trainer_module._load_backtest_artifact(
        trainer_module._deployment_backtest_path(
            trainer_module._fold_dir(tmp_path, 2)
        )
    )
    np.testing.assert_allclose(fold_two_replayed.cash_history, [0.0, 0.0])
    np.testing.assert_allclose(fold_two_replayed.weights_history, np.zeros((2, 3)))


def test_stitched_cash_replay_carries_pending_payable_across_fold_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_stitched_plots(monkeypatch)
    panel = _day_trade_panel(np.full((4, 3), 10.0, dtype=np.float32))
    fold_one = _write_fold_requests(
        tmp_path,
        fold_id=1,
        dates=panel.dates[1:2],
        symbols=("2330",),
        requests=np.asarray([[1.0]]),
        execution_mode="tw_cash",
    )
    fold_two = _write_fold_requests(
        tmp_path,
        fold_id=2,
        dates=panel.dates[2:],
        symbols=("2330",),
        requests=np.asarray([[1.0], [1.0]]),
        execution_mode="tw_cash",
    )

    stitched = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold_one, fold_two],
        panel=panel,
        config=_cash_config(),
    )

    assert stitched is not None
    np.testing.assert_allclose(
        stitched.payables_history,
        np.asarray([[0.0, 100.0], [100.0, 0.0], [0.0, 0.0]]),
    )
    np.testing.assert_allclose(stitched.cash_history, [100.0, 100.0, 0.0])
    assert stitched.final_integer_state is not None
    np.testing.assert_array_equal(stitched.final_integer_state.holdings, [0, 10, 0])
    assert stitched.final_integer_state.alive


def test_stitched_cash_short_replay_uses_point_in_time_capacity_and_margin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_stitched_plots(monkeypatch)
    panel = _day_trade_panel(np.full((3, 3), 1_000.0, dtype=np.float32))
    panel.short_capacity_shares = np.zeros((3, 3), dtype=np.int64)
    panel.short_capacity_shares[1, 1] = 2
    panel.short_margin_rate = np.full((3, 3), np.nan, dtype=np.float64)
    panel.short_margin_rate[1, 1] = 0.95
    fold = _write_fold_requests(
        tmp_path,
        fold_id=1,
        dates=panel.dates[1:2],
        symbols=("2330",),
        requests=np.asarray([[-1.0]]),
        execution_mode="tw_cash",
    )
    config = _cash_config()
    config.trading.long_only = False
    config.trading.volume_participation_equity = 10_000.0
    config.trading.tw_short_lot_size = 1

    stitched = trainer_module._replay_taiwan_stitched_deployment(
        tmp_path,
        [fold],
        panel=panel,
        config=config,
    )

    assert stitched is not None
    # A -100% request would target ten shares at this capital/price, but the
    # receipt-backed capacity for that session permits opening only two.
    np.testing.assert_array_equal(stitched.shares_history, [[0, -2, 0]])
    np.testing.assert_allclose(
        stitched.short_sale_collateral_history,
        [[0.0, 2_000.0, 0.0]],
    )
    # The exact oracle consumes the point-in-time 95% security rate (above the
    # statutory 90% floor), rounded upward to the next NTD 100 as required.
    np.testing.assert_allclose(
        stitched.short_margin_collateral_history,
        [[0.0, 1_900.0, 0.0]],
    )


def test_all_integer_audit_calls_supply_point_in_time_short_contract() -> None:
    source_path = REPO_ROOT / "stockagent/training/trainer.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_integer_execution_runtime_kwargs"
    ]

    assert calls
    for call in calls:
        keyword_names = {keyword.arg for keyword in call.keywords}
        assert {"short_capacity_shares", "short_margin_rate"} <= keyword_names, (
            f"integer audit call at line {call.lineno} must pass the aligned "
            "point-in-time short contract"
        )


def test_active_short_contract_rows_slice_symbols_and_apply_official_floor() -> None:
    panel = _day_trade_panel(np.full((3, 3), 10.0, dtype=np.float32))
    panel.dates = np.asarray(
        ["2025-04-06", "2025-04-07", "2025-05-26"],
        dtype="datetime64[D]",
    )
    panel.short_capacity_shares = np.asarray(
        [[0.0, 0.0, 0.0], [1.0, 2.0, 3.0], [4.0, np.nan, 6.0]],
        dtype=np.float64,
    )
    panel.short_margin_rate = np.asarray(
        [[np.nan, np.nan, np.nan], [0.95, np.nan, 1.50], [1.00, 1.20, np.nan]],
        dtype=np.float64,
    )

    capacity, margin = trainer_module._active_panel_short_contract_rows(
        panel,
        np.asarray([1, 2], dtype=np.int64),
        np.asarray([2, 0], dtype=np.int64),
        execution_mode="tw_cash",
    )

    assert capacity is not None
    assert margin is not None
    np.testing.assert_array_equal(capacity, [[3, 1], [6, 4]])
    # 2025-04-07 is inside the temporary 130% market-wide window; 2025-05-26
    # is the first restored 90% session. Higher security-specific rates win.
    np.testing.assert_allclose(margin, [[1.50, 1.30], [0.90, 1.00]])

    panel.short_capacity_shares = None
    missing_capacity, _ = trainer_module._active_panel_short_contract_rows(
        panel,
        np.asarray([1], dtype=np.int64),
        np.asarray([2, 0], dtype=np.int64),
        execution_mode="tw_cash",
    )
    np.testing.assert_array_equal(missing_capacity, [[0, 0]])


def test_all_production_walkforward_refresh_calls_supply_panel_and_config() -> None:
    source_path = REPO_ROOT / "stockagent/training/trainer.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_refresh_walkforward_artifacts"
    ]

    assert calls
    for call in calls:
        keyword_names = {keyword.arg for keyword in call.keywords}
        assert {"panel", "config"} <= keyword_names, (
            f"production refresh call at line {call.lineno} must pass panel/config"
        )
