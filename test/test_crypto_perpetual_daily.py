from __future__ import annotations

from datetime import date, datetime, timezone
import math
from pathlib import Path
import sys

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.crypto_perpetual import run_crypto_perpetual_torch
from stockagent.backtest.report import compute_metrics
from stockagent.backtest.simulator import (
    BacktestResult,
    run_backtest_integer_shares,
    run_backtest_torch,
)
from stockagent.config import external_panel_data_kwargs, load_config
from stockagent.data.panel import PanelData
from stockagent.data.panel import (
    _load_symbol_arrays_polars_lazy,
    _load_symbol_arrays_pyarrow,
)
from stockagent.models.transformer_base_portfolio import (
    action_channels_for_execution_mode,
)
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.loss import risk_aware_loss

DOWNLOADER_DIR = Path(__file__).resolve().parents[1] / "downloader"
if str(DOWNLOADER_DIR) not in sys.path:
    sys.path.insert(0, str(DOWNLOADER_DIR))

from materialize_bybit_perpetual_daily import (  # noqa: E402
    _attach_funding_total_return,
    _daily_bars,
)
from download_bybit_funding_history import (  # noqa: E402
    _quarantine_unmarked_launch_prefix,
)
from download_bybit_perp_daily import (  # noqa: E402
    BYBIT_WINDOW_SPAN_MS,
    CANDLE_INTERVAL_MS,
    _iter_windows,
    _load_existing_candle_info,
)
from download_fred_crypto_macro_vintages import (  # noqa: E402
    _realtime_windows,
    parse_initial_release_rows,
)
from repair_bybit_1m_gaps import (  # noqa: E402
    _missing_timestamp_ms,
    _request_windows,
)
from scripts.build_bybit_crypto_public_daily_features import (  # noqa: E402
    _binance_daily,
    _ensure_output_feature_schema,
    _okx_base_map,
    _bybit_funding_features,
    _okx_daily,
    _resolve_okx_mapping,
)
from scripts.rebase_bybit_funding_feature_slice import (  # noqa: E402
    rebase_bybit_funding_slice,
)
from stockagent.data.crypto_public_web import (  # noqa: E402
    coingecko_snapshot_rows,
    coinmetrics_vintage_rows,
    etf_issuer_snapshot_rows,
    fred_macro_rows,
    sec_etf_filing_rows,
)


def _ledger(
    target: torch.Tensor,
    effective_simple: torch.Tensor,
    price_simple: torch.Tensor,
    **kwargs: object,
):
    mask = torch.ones_like(target, dtype=torch.bool)
    return run_crypto_perpetual_torch(
        target,
        torch.log1p(effective_simple),
        torch.log1p(price_simple),
        mask,
        mask,
        mask,
        mask,
        torch.zeros_like(mask),
        buy_fee_rate=0.00055,
        sell_fee_rate=0.00055,
        long_only=False,
        maximum_gross=1.0,
        **kwargs,
    )


def test_funding_cash_changes_nav_but_only_price_changes_marked_notional() -> None:
    result = _ledger(
        torch.tensor([[1.0]]),
        # +10% contract price less +1% long funding cash payment.
        torch.tensor([[0.09]]),
        torch.tensor([[0.10]]),
    )
    expected_net = 0.10 - 0.01 - 0.00055
    assert result.strategy_simple_returns.item() == pytest.approx(expected_net)
    assert result.turnovers.item() == pytest.approx(1.0)
    assert result.final_weights.item() == pytest.approx(1.10 / (1.0 + expected_net))


def test_positive_funding_is_received_by_a_short() -> None:
    result = _ledger(
        torch.tensor([[-1.0]]),
        torch.tensor([[-0.01]]),
        torch.tensor([[0.0]]),
    )
    assert result.strategy_simple_returns.item() == pytest.approx(0.01 - 0.00055)
    assert result.final_weights.item() == pytest.approx(-1.0 / (1.0 + 0.01 - 0.00055))


def test_crypto_reporting_reuses_canonical_continuous_result_without_share_oracle() -> (
    None
):
    canonical = BacktestResult(
        strategy_returns=np.asarray([0.01], dtype=np.float32),
        benchmark_returns=np.asarray([0.0], dtype=np.float32),
        turnovers=np.asarray([0.25], dtype=np.float32),
        weights_history=np.asarray([[0.25]], dtype=np.float32),
        requested_weights_history=np.asarray([[0.25]], dtype=np.float32),
        final_weights=np.asarray([0.2525], dtype=np.float32),
        final_alive=np.asarray(True),
        execution_mode="crypto_perpetual",
        settlement_ledger_unit="notional_weight",
    )
    reported, holdings = run_backtest_integer_shares(
        weights=np.asarray([[0.25]], dtype=np.float32),
        future_returns=np.asarray([[0.01]], dtype=np.float32),
        tradable_mask=np.asarray([[True]]),
        benchmark_returns=np.asarray([0.0], dtype=np.float32),
        execution_mode="crypto_perpetual",
        precomputed_exact_backtest=canonical,
    )
    assert reported is canonical
    assert holdings == []


def test_crypto_daily_metrics_use_365_period_annualization() -> None:
    returns = np.asarray([0.01, -0.005], dtype=np.float64)
    result = BacktestResult(
        strategy_returns=returns,
        benchmark_returns=np.zeros_like(returns),
        turnovers=np.zeros_like(returns),
        weights_history=np.zeros((2, 1), dtype=np.float64),
        execution_mode="crypto_perpetual",
        settlement_ledger_unit="notional_weight",
    )
    metrics = compute_metrics(result)
    assert metrics["annualized_return"] == pytest.approx(
        np.expm1(returns.mean() * 365.0)
    )
    assert metrics["sharpe"] == pytest.approx(
        returns.mean() / returns.std(ddof=0) * np.sqrt(365.0)
    )


def test_gross_cap_scales_only_new_expansion_not_an_unchanged_position() -> None:
    result = _ledger(
        torch.tensor([[0.5, 1.0]]),
        torch.zeros((1, 2)),
        torch.zeros((1, 2)),
        initial_weights=torch.tensor([0.5, 0.0]),
    )
    assert result.executed_weights[0].tolist() == pytest.approx([0.5, 0.5])
    assert result.turnovers.item() == pytest.approx(0.5)


def test_mark_drift_above_the_gross_cap_forces_only_required_deleveraging() -> None:
    result = _ledger(
        torch.tensor([[0.7, 0.5]]),
        torch.zeros((1, 2)),
        torch.zeros((1, 2)),
        initial_weights=torch.tensor([0.7, 0.5]),
    )
    assert result.executed_weights[0].abs().sum().item() == pytest.approx(1.0)
    assert result.turnovers.item() == pytest.approx(0.2)


def test_sign_flip_charges_the_full_close_plus_open_turnover() -> None:
    result = _ledger(
        torch.tensor([[-0.5]]),
        torch.zeros((1, 1)),
        torch.zeros((1, 1)),
        initial_weights=torch.tensor([0.5]),
    )
    assert result.turnovers.item() == pytest.approx(1.0)
    assert result.strategy_simple_returns.item() == pytest.approx(-0.00055)


def test_crypto_proximal_allocator_derives_no_trade_region_from_exact_fee() -> None:
    previous = torch.tensor([0.5, -0.5])
    target = torch.tensor([[0.5004, -0.4996]], requires_grad=True)
    result = _ledger(
        target,
        torch.zeros((1, 2)),
        torch.zeros((1, 2)),
        initial_weights=previous,
        stateful_proximal_allocator=True,
        proximal_cost_multiplier=1.0,
    )

    torch.testing.assert_close(result.executed_weights[0], previous)
    assert result.turnovers.item() == pytest.approx(0.0)
    assert result.strategy_simple_returns.item() == pytest.approx(0.0)
    loss = result.executed_weights.square().sum()
    loss.backward()
    assert target.grad is not None
    torch.testing.assert_close(target.grad, torch.zeros_like(target.grad))


def test_crypto_proximal_allocator_keeps_model_freedom_outside_fee_band() -> None:
    previous = torch.tensor([0.5, -0.5])
    target = torch.tensor([[0.6, -0.4]], requires_grad=True)
    result = _ledger(
        target,
        torch.zeros((1, 2)),
        torch.zeros((1, 2)),
        initial_weights=previous,
        stateful_proximal_allocator=True,
        proximal_cost_multiplier=1.0,
    )

    expected = torch.tensor([0.59945, -0.40055])
    torch.testing.assert_close(result.executed_weights[0], expected)
    assert result.turnovers.item() == pytest.approx(0.1989)
    (-result.strategy_simple_returns.sum()).backward()
    assert target.grad is not None
    assert torch.isfinite(target.grad).all()
    assert float(target.grad.abs().sum()) > 0.0


def test_chunk_boundary_carries_marked_position_and_alive_state_exactly() -> None:
    target = torch.tensor(
        [[0.4, -0.2], [0.3, -0.3], [0.1, 0.2], [-0.2, 0.4], [0.0, 0.3], [0.2, -0.1]]
    )
    effective = torch.tensor(
        [
            [0.02, -0.01],
            [0.01, 0.03],
            [-0.02, 0.01],
            [0.04, -0.01],
            [0.01, 0.02],
            [-0.01, 0.03],
        ]
    )
    price = effective + 0.001
    full = _ledger(target, effective, price)
    first = _ledger(target[:3], effective[:3], price[:3])
    second = _ledger(
        target[3:],
        effective[3:],
        price[3:],
        initial_weights=first.final_weights,
        initial_alive=first.final_alive,
    )
    assert torch.cat(
        [first.strategy_simple_returns, second.strategy_simple_returns]
    ).tolist() == pytest.approx(full.strategy_simple_returns.tolist())
    assert torch.cat([first.turnovers, second.turnovers]).tolist() == pytest.approx(
        full.turnovers.tolist()
    )
    assert second.final_weights.tolist() == pytest.approx(full.final_weights.tolist())
    assert second.final_alive.item() == full.final_alive.item()


def test_padding_row_preserves_position_without_fee_pnl_or_valuation() -> None:
    target = torch.tensor([[0.4], [0.0]])
    effective = torch.tensor([[0.02], [float("nan")]])
    price = torch.tensor([[0.03], [float("nan")]])
    result = _ledger(
        target,
        effective,
        price,
        state_advance_mask=torch.tensor([True, False]),
    )
    assert result.strategy_simple_returns[1].item() == 0.0
    assert result.turnovers[1].item() == 0.0
    assert result.executed_weights[1].item() == pytest.approx(
        result.final_weights.item()
    )
    assert result.final_alive.item() is True


def test_nontradable_row_does_not_manufacture_a_close_but_missing_active_mark_ruins() -> (
    None
):
    target = torch.tensor([[0.5], [0.0]])
    effective = torch.tensor([[0.0], [float("nan")]])
    price = torch.tensor([[0.0], [float("nan")]])
    tradable = torch.tensor([[True], [False]])
    result = run_crypto_perpetual_torch(
        target,
        torch.log1p(effective),
        torch.log1p(price),
        tradable,
        tradable,
        tradable,
        tradable,
        torch.zeros_like(tradable),
        buy_fee_rate=0.00055,
        sell_fee_rate=0.00055,
        long_only=False,
        maximum_gross=1.0,
    )
    assert result.turnovers[1].item() == 0.0
    assert result.strategy_simple_returns[1].item() == pytest.approx(-0.999999)
    assert result.final_alive.item() is False
    assert result.final_weights.item() == 0.0


def test_crypto_simulator_dispatch_preserves_gradients() -> None:
    weights = torch.tensor([[0.3, -0.2], [0.25, -0.1]], requires_grad=True)
    effective = torch.log1p(torch.tensor([[0.02, -0.01], [0.01, 0.03]]))
    price = torch.log1p(torch.tensor([[0.021, -0.009], [0.011, 0.031]]))
    mask = torch.ones_like(weights, dtype=torch.bool)
    result = run_backtest_torch(
        weights,
        effective,
        mask,
        torch.zeros(2),
        execution_mode="crypto_perpetual",
        overnight_returns=price,
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        buy_fee_rate=0.00055,
        sell_fee_rate=0.00055,
        long_only=False,
        gross_leverage=1.0,
        portfolio_activation="identity",
    )
    loss = -result.strategy_returns.sum()
    loss.backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()
    assert result.execution_mode == "crypto_perpetual"


def test_log_utility_loss_uses_the_crypto_funding_and_price_paths() -> None:
    weights = torch.tensor([[0.3, -0.2], [0.25, -0.1]], requires_grad=True)
    effective = torch.log1p(torch.tensor([[0.02, -0.01], [0.01, 0.03]]))
    price = torch.log1p(torch.tensor([[0.021, -0.009], [0.011, 0.031]]))
    mask = torch.ones_like(weights, dtype=torch.bool)
    loss = risk_aware_loss(
        weights,
        effective,
        mask,
        benchmark_returns=torch.zeros(2),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        long_only=False,
        buy_fee_rate=0.00055,
        sell_fee_rate=0.00055,
        gross_leverage=1.0,
        portfolio_activation="pre_normalized",
        execution_mode="crypto_perpetual",
        objective="log_utility",
        overnight_log_returns=price,
    )
    loss.backward()
    assert torch.isfinite(loss)
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_crypto_training_log_utility_uses_configured_365_day_scale() -> None:
    weights = torch.tensor([[0.5]], requires_grad=True)
    effective = torch.log1p(torch.tensor([[0.01]]))
    mask = torch.ones_like(weights, dtype=torch.bool)
    loss_365 = risk_aware_loss(
        weights,
        effective,
        mask,
        benchmark_returns=torch.zeros(1),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        long_only=False,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        gross_leverage=1.0,
        portfolio_activation="pre_normalized",
        execution_mode="crypto_perpetual",
        objective="log_utility",
        overnight_log_returns=effective,
        log_utility_periods_per_year=365.0,
        gamma_turnover=0.0,
        concentration_weight=0.0,
    )
    loss_252 = risk_aware_loss(
        weights,
        effective,
        mask,
        benchmark_returns=torch.zeros(1),
        can_buy_mask=mask,
        can_sell_mask=mask,
        can_short_open_mask=mask,
        long_only=False,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        gross_leverage=1.0,
        portfolio_activation="pre_normalized",
        execution_mode="crypto_perpetual",
        objective="log_utility",
        overnight_log_returns=effective,
        log_utility_periods_per_year=252.0,
        gamma_turnover=0.0,
        concentration_weight=0.0,
    )
    assert loss_365.item() == pytest.approx(loss_252.item() * 365.0 / 252.0)


def test_funding_materialization_matches_event_level_cash_identity(
    tmp_path: Path,
) -> None:
    boundaries = [
        datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 0, 5, tzinfo=timezone.utc),
    ]
    daily = pl.DataFrame(
        {
            "__session_end_date": [
                datetime(2024, 1, 1).date(),
                datetime(2024, 1, 2).date(),
            ],
            "open": [100.0, 110.0],
            "max": [101.0, 111.0],
            "min": [99.0, 109.0],
            "close": [99.0, 109.0],
            "execution_price": [100.0, 110.0],
            "Trading_Volume": [10.0, 12.0],
            "execution_volume_equivalent": [20.0, 30.0],
            "source_minute_rows": [1440, 1440],
            "unique_minute_rows": [1440, 1440],
            "first_minute_utc": [boundaries[0], boundaries[0]],
            "last_minute_utc": [boundaries[0], boundaries[1]],
            "minute_grid_complete": [True, True],
            "__decision_cutoff_utc": [
                datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
            ],
            "__boundary_utc": boundaries,
        }
    )
    funding_path = tmp_path / "funding.parquet"
    pl.DataFrame(
        {
            "funding_time_utc": ["2024-01-01 08:00:00", "2024-01-01 16:00:00"],
            "funding_rate": [0.001, -0.0005],
            "funding_mark_price": [102.0, 104.0],
            "bybit_funding_contract_version": [3, 3],
        }
    ).write_parquet(funding_path)
    output, executable, events = _attach_funding_total_return(
        daily,
        funding_path,
        {
            "head_complete": True,
            "coverage_start_utc": "2024-01-01 00:00:00",
            "coverage_end_utc": "2024-01-02 01:00:00",
        },
    )
    coefficient = 102.0 / 100.0 * 0.001 + 104.0 / 100.0 * -0.0005
    expected = 0.10 - coefficient
    assert executable == 1
    assert events == 2
    assert output[0, "funding_cashflow_coefficient_to_next"] == pytest.approx(
        coefficient
    )
    assert output[0, "funding_adjusted_simple_return_to_next"] == pytest.approx(
        expected
    )
    assert output[1, "adjclose"] / output[0, "adjclose"] - 1.0 == pytest.approx(
        expected
    )
    assert output[1, "funding_rate_sum_previous_session"] == pytest.approx(0.0005)
    assert output[1, "funding_last_rate_previous_session"] == pytest.approx(-0.0005)

    materialized = tmp_path / "BTCUSDT_features.parquet"
    output.write_parquet(materialized)
    features = _bybit_funding_features(materialized, "BTCUSDT")
    assert features[1, "crypto_bybit_funding_rate_sum_1d"] == pytest.approx(0.0005)
    assert features[1, "crypto_bybit_funding_realized_annualized"] == pytest.approx(
        0.0005 * 365.0
    )
    assert features[1, "crypto_bybit_funding_available"] == 1.0


def test_midnight_execution_settles_boundary_funding_before_new_target(
    tmp_path: Path,
) -> None:
    boundaries = [
        datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc),
    ]
    daily = pl.DataFrame(
        {
            "__session_end_date": [
                datetime(2024, 1, 1).date(),
                datetime(2024, 1, 2).date(),
            ],
            "open": [100.0, 110.0],
            "max": [101.0, 111.0],
            "min": [99.0, 109.0],
            "close": [100.0, 110.0],
            "execution_price": [100.0, 110.0],
            "Trading_Volume": [10.0, 12.0],
            "execution_volume_equivalent": [20.0, 30.0],
            "source_minute_rows": [1440, 1440],
            "unique_minute_rows": [1440, 1440],
            "first_minute_utc": boundaries,
            "last_minute_utc": boundaries,
            "minute_grid_complete": [True, True],
            "__decision_cutoff_utc": boundaries,
            "__boundary_utc": boundaries,
        }
    )
    funding_path = tmp_path / "funding.parquet"
    pl.DataFrame(
        {
            "funding_time_utc": [
                "2024-01-01 00:00:00",
                "2024-01-01 08:00:00",
                "2024-01-02 00:00:00",
            ],
            "funding_rate": [0.01, 0.001, 0.02],
            "funding_mark_price": [100.0, 102.0, 110.0],
            "bybit_funding_contract_version": [3, 3, 3],
        }
    ).write_parquet(funding_path)

    output, executable, events = _attach_funding_total_return(
        daily,
        funding_path,
        {
            "head_complete": True,
            "coverage_start_utc": "2024-01-01 00:00:00",
            "coverage_end_utc": "2024-01-02 00:00:00",
        },
        execution_minutes_utc=0,
    )

    # The event at entry time is already settled; events in (start, end],
    # including the next boundary, belong to the carried position.
    expected_coefficient = 102.0 / 100.0 * 0.001 + 110.0 / 100.0 * 0.02
    assert executable == 1
    assert events == 2
    assert output[0, "funding_cashflow_coefficient_to_next"] == pytest.approx(
        expected_coefficient
    )
    assert output[0, "funding_rate_sum_to_next"] == pytest.approx(0.021)
    assert output[0, "bybit_perpetual_contract_version"] == 7
    assert output[0, "decision_cutoff_utc"] == "00:00"
    assert output[0, "daily_boundary_utc"] == "00:00"


def test_rebase_bybit_funding_slice_preserves_other_public_features() -> None:
    base = pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-01"],
            "symbol": ["BTCUSDT", "__MARKET__"],
            "crypto_bybit_funding_rate_sum_1d": [9.0, None],
            "crypto_public_fear_greed_index": [None, 42.0],
        }
    )
    funding = pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            **{
                name: [float(index + 1), float(index + 2)]
                for index, name in enumerate(
                    (
                        "crypto_bybit_funding_rate_sum_1d",
                        "crypto_bybit_funding_realized_annualized",
                        "crypto_bybit_funding_cashflow_coefficient_1d",
                        "crypto_bybit_funding_last_rate",
                        "crypto_bybit_funding_age_hours",
                        "crypto_bybit_funding_event_count_1d",
                        "crypto_bybit_funding_available",
                    )
                )
            },
        }
    )

    output = rebase_bybit_funding_slice(base, funding)

    btc = output.filter(
        (pl.col("date") == "2024-01-01") & (pl.col("symbol") == "BTCUSDT")
    )
    market = output.filter(pl.col("symbol") == "__MARKET__")
    assert btc[0, "crypto_bybit_funding_rate_sum_1d"] == pytest.approx(1.0)
    assert market[0, "crypto_public_fear_greed_index"] == pytest.approx(42.0)
    assert output.filter(pl.col("date") == "2024-01-02").height == 1


def test_daily_features_stop_five_minutes_before_execution_open(tmp_path: Path) -> None:
    timestamps = pl.datetime_range(
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 1, 2, 0, 5),
        interval="1m",
        eager=True,
    )
    values = np.arange(len(timestamps), dtype=np.float64) + 100.0
    frame = pl.DataFrame(
        {
            "date": timestamps.dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open": values,
            "max": values + 1.0,
            "min": values - 1.0,
            "close": values + 0.5,
            "adjclose": values + 0.5,
            "Trading_Volume": np.ones(len(timestamps)),
        }
    )
    path = tmp_path / "BTCUSDT_features.parquet"
    frame.write_parquet(path)
    daily, incomplete_retained, execution_excluded = _daily_bars(path)
    assert daily.height == 1
    assert incomplete_retained == 0
    assert execution_excluded == 1
    assert daily[0, "close"] == pytest.approx(values[-7] + 0.5)
    assert daily[0, "execution_price"] == pytest.approx(values[-1])
    assert daily[0, "last_minute_utc"] == datetime(
        2024, 1, 1, 23, 59, tzinfo=timezone.utc
    )


def test_daily_features_can_execute_at_midnight_without_using_current_bar(
    tmp_path: Path,
) -> None:
    timestamps = pl.datetime_range(
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 1, 2, 0, 0),
        interval="1m",
        eager=True,
    )
    values = np.arange(len(timestamps), dtype=np.float64) + 100.0
    path = tmp_path / "BTCUSDT_features.parquet"
    pl.DataFrame(
        {
            "date": timestamps.dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open": values,
            "max": values + 1.0,
            "min": values - 1.0,
            "close": values + 0.5,
            "Trading_Volume": np.ones(len(timestamps)),
        }
    ).write_parquet(path)

    daily, incomplete_retained, execution_excluded = _daily_bars(
        path,
        execution_minutes_utc=0,
    )

    assert daily.height == 1
    assert incomplete_retained == 0
    assert execution_excluded == 1
    assert daily[0, "close"] == pytest.approx(values[-2] + 0.5)
    assert daily[0, "execution_price"] == pytest.approx(values[-1])
    assert daily[0, "last_minute_utc"] == datetime(
        2024, 1, 1, 23, 59, tzinfo=timezone.utc
    )


def test_incomplete_feature_day_keeps_real_execution_mark_but_blocks_policy(
    tmp_path: Path,
) -> None:
    timestamps = pl.datetime_range(
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 1, 4, 0, 5),
        interval="1m",
        eager=True,
    )
    missing = datetime(2024, 1, 2, 5, 50)
    timestamps = timestamps.filter(timestamps != missing)
    values = np.arange(len(timestamps), dtype=np.float64) + 100.0
    path = tmp_path / "XTZUSDT_features.parquet"
    pl.DataFrame(
        {
            "date": timestamps.dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open": values,
            "max": values + 1.0,
            "min": values - 1.0,
            "close": values + 0.5,
            "Trading_Volume": np.ones(len(timestamps)),
        }
    ).write_parquet(path)

    daily, incomplete_retained, execution_excluded = _daily_bars(path)

    assert daily.height == 3
    assert incomplete_retained == 1
    assert execution_excluded == 1
    incomplete_row = daily.filter(
        pl.col("__session_end_date") == datetime(2024, 1, 3).date()
    )
    assert incomplete_row.height == 1
    assert incomplete_row[0, "minute_grid_complete"] is False
    assert incomplete_row[0, "execution_available"] is True
    assert incomplete_row[0, "policy_tradable"] is False


def test_panel_uses_execution_price_only_for_perpetual_valuation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BTCUSDT_features.parquet"
    pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "open": [100.0, 110.0],
            "max": [103.0, 114.0],
            "min": [99.0, 109.0],
            "close": [102.0, 112.0],
            "execution_price": [105.0, 121.0],
            "adjclose": [100.0, 1000.0],
            "Trading_Volume": [10.0, 12.0],
            "execution_volume_equivalent": [20.0, 30.0],
            "return_quarantined": [False, True],
            "policy_tradable": [False, True],
            "execution_available": [True, True],
            "bybit_perpetual_contract_version": [6, 6],
        }
    ).write_parquet(path)
    for loader in (_load_symbol_arrays_pyarrow, _load_symbol_arrays_polars_lazy):
        arrays = loader(
            path, tradable_mode="tradable", trading_volume_policy="required"
        )
        assert arrays.close_prices.tolist() == pytest.approx([105.0, 121.0])
        assert arrays.daily_volumes.tolist() == pytest.approx([20.0, 30.0])
        assert arrays.intraday_returns[0] == pytest.approx(math.log(102.0 / 100.0))
        assert arrays.returns_1d[0] == pytest.approx(math.log(10.0))
        assert arrays.tradable_mask.tolist() == [False, True]
        assert arrays.can_buy_mask.tolist() == [True, True]
        assert arrays.can_sell_mask.tolist() == [True, True]


def test_only_launch_prefix_mark_gaps_can_be_quarantined() -> None:
    rows = [{"funding_timestamp_ms": timestamp} for timestamp in (1, 2, 3, 4)]
    retained, cutoff, count = _quarantine_unmarked_launch_prefix(
        rows, {3: 10.0, 4: 11.0}
    )
    assert cutoff == 2
    assert count == 2
    assert [row["funding_timestamp_ms"] for row in retained] == [3, 4]
    with pytest.raises(RuntimeError, match="non-prefix gap"):
        _quarantine_unmarked_launch_prefix(rows, {1: 9.0, 3: 10.0, 4: 11.0})


def test_bybit_kline_windows_deliberately_overlap_at_boundaries() -> None:
    windows = _iter_windows(0, BYBIT_WINDOW_SPAN_MS * 2)
    assert windows == [
        (0, BYBIT_WINDOW_SPAN_MS),
        (BYBIT_WINDOW_SPAN_MS, BYBIT_WINDOW_SPAN_MS * 2),
    ]


def test_bybit_existing_contract_and_repair_scanner_detect_internal_gap(
    tmp_path: Path,
) -> None:
    path = tmp_path / "GAPUSDT_features.parquet"
    pl.DataFrame(
        {
            "date": [
                "2024-01-01 00:00:00",
                "2024-01-01 00:01:00",
                "2024-01-01 00:03:00",
            ],
            "close": [1.0, 1.1, 1.2],
        }
    ).write_parquet(path, statistics=True)
    info = _load_existing_candle_info(path)
    rows, missing = _missing_timestamp_ms(path)
    assert info.interval_ok is False
    assert rows == 3
    assert missing == [
        int(datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc).timestamp() * 1000)
    ]


def test_bybit_gap_requests_never_exceed_the_1000_candle_span() -> None:
    windows = _request_windows(
        [0, BYBIT_WINDOW_SPAN_MS, BYBIT_WINDOW_SPAN_MS + CANDLE_INTERVAL_MS]
    )
    assert [(start, end) for start, end, _ in windows] == [
        (0, BYBIT_WINDOW_SPAN_MS),
        (
            BYBIT_WINDOW_SPAN_MS + CANDLE_INTERVAL_MS,
            BYBIT_WINDOW_SPAN_MS + CANDLE_INTERVAL_MS,
        ),
    ]


def test_dataset_separates_funding_adjusted_label_from_raw_price_drift() -> None:
    rows = 4
    mask = np.ones((rows, 1), dtype=bool)
    effective = np.asarray(
        [math.log1p(0.09), math.log1p(-0.02), math.log1p(0.01), np.nan],
        dtype=np.float32,
    )
    panel = PanelData(
        dates=np.arange("2024-01-01", "2024-01-05", dtype="datetime64[D]"),
        symbols=["BTCUSDT"],
        feature_names=["x"],
        features=np.zeros((rows, 1, 1), dtype=np.float32),
        returns_1d=effective[:, None],
        tradable_mask=mask,
        can_buy_mask=mask,
        can_sell_mask=mask,
        alive_mask=mask,
        benchmark_returns=effective,
        close_prices=np.asarray([[100.0], [110.0], [99.0], [100.0]], dtype=np.float32),
    )
    dataset = CrossSectionalDataset(
        panel,
        np.arange(rows),
        lookback=1,
        execution_mode="crypto_perpetual",
    )
    assert dataset.future_log_returns_t[0, 0].item() == pytest.approx(effective[0])
    assert dataset.overnight_log_returns_t[0, 0].item() == pytest.approx(math.log(1.1))
    assert dataset.overnight_log_returns_t[1, 0].item() == pytest.approx(math.log(0.9))
    assert torch.isnan(dataset.overnight_log_returns_t[-1, 0])


def test_future_label_availability_never_changes_crypto_policy_mask() -> None:
    rows, symbols = 3, 2
    current = np.ones((rows, symbols), dtype=bool)
    returns = np.asarray(
        [[0.01, 0.02], [0.03, np.nan], [np.nan, np.nan]], dtype=np.float32
    )
    panel = PanelData(
        dates=np.arange("2024-01-01", "2024-01-04", dtype="datetime64[D]"),
        symbols=["BTCUSDT", "ETHUSDT"],
        feature_names=["x"],
        features=np.zeros((rows, symbols, 1), dtype=np.float32),
        returns_1d=returns,
        tradable_mask=current,
        can_buy_mask=current,
        can_sell_mask=current,
        alive_mask=current,
        benchmark_returns=np.asarray([0.01, 0.03, np.nan], dtype=np.float32),
        close_prices=np.asarray(
            [[100.0, 50.0], [101.0, 51.0], [102.0, np.nan]], dtype=np.float32
        ),
    )
    dataset = CrossSectionalDataset(
        panel,
        np.arange(rows),
        lookback=1,
        execution_mode="crypto_perpetual",
    )
    assert dataset.valid_indices.tolist() == [0, 1]
    assert dataset.tradable_mask_t[1].tolist() == [True, True]
    assert dataset.can_buy_mask_t[1].tolist() == [True, True]


def test_crypto_feature_gap_blocks_new_policy_but_keeps_close_execution() -> None:
    rows = 3
    policy = np.asarray([[False], [True], [True]], dtype=bool)
    execution = np.ones((rows, 1), dtype=bool)
    panel = PanelData(
        dates=np.arange("2024-01-01", "2024-01-04", dtype="datetime64[D]"),
        symbols=["XTZUSDT"],
        feature_names=["x"],
        features=np.zeros((rows, 1, 1), dtype=np.float32),
        returns_1d=np.asarray([[0.01], [0.02], [np.nan]], dtype=np.float32),
        tradable_mask=policy,
        can_buy_mask=execution,
        can_sell_mask=execution,
        alive_mask=execution,
        benchmark_returns=np.asarray([0.01, 0.02, np.nan], dtype=np.float32),
        close_prices=np.asarray([[100.0], [101.0], [102.0]], dtype=np.float32),
    )

    dataset = CrossSectionalDataset(
        panel,
        np.arange(rows),
        lookback=1,
        execution_mode="crypto_perpetual",
    )

    assert dataset.tradable_mask_t[0, 0].item() is False
    assert dataset.can_buy_mask_t[0, 0].item() is True
    assert dataset.can_sell_mask_t[0, 0].item() is True


def test_crypto_dataset_retains_an_unvalued_interior_day_for_absorbing_failure() -> (
    None
):
    rows = 4
    current = np.ones((rows, 1), dtype=bool)
    panel = PanelData(
        dates=np.arange("2024-01-01", "2024-01-05", dtype="datetime64[D]"),
        symbols=["BTCUSDT"],
        feature_names=["x"],
        features=np.zeros((rows, 1, 1), dtype=np.float32),
        returns_1d=np.asarray([[0.01], [np.nan], [0.02], [np.nan]], dtype=np.float32),
        tradable_mask=current,
        can_buy_mask=current,
        can_sell_mask=current,
        alive_mask=current,
        benchmark_returns=np.asarray([0.01, np.nan, 0.02, np.nan], dtype=np.float32),
        close_prices=np.asarray([[100.0], [101.0], [102.0], [103.0]], dtype=np.float32),
    )
    dataset = CrossSectionalDataset(
        panel,
        np.arange(rows),
        lookback=1,
        execution_mode="crypto_perpetual",
    )
    assert dataset.valid_indices.tolist() == [0, 1, 2]
    assert torch.isnan(dataset.future_log_returns_t[1, 0])


def test_okx_public_daily_requires_a_complete_causally_available_session(
    tmp_path: Path,
) -> None:
    complete = pl.datetime_range(
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 1, 1, 23, 45),
        interval="15m",
        eager=True,
    )
    incomplete = pl.datetime_range(
        datetime(2024, 1, 2, 0, 0),
        datetime(2024, 1, 2, 2, 15),
        interval="15m",
        eager=True,
    )
    timestamps = complete.append(incomplete)
    values = np.linspace(100.0, 120.0, len(timestamps))
    source = pl.DataFrame(
        {
            "date": timestamps.dt.strftime("%Y-%m-%d %H:%M:%S"),
            "okx_mark_open": values,
            "okx_mark_high": values + 1.0,
            "okx_mark_low": values - 1.0,
            "okx_mark_close": values + 0.2,
            "okx_index_open": values - 0.1,
            "okx_index_high": values + 0.8,
            "okx_index_low": values - 1.2,
            "okx_index_close": values + 0.1,
        }
    )
    path = tmp_path / "BTCUSDTSWAP_features.parquet"
    source.write_parquet(path)
    output = _okx_daily(path, "BTCUSDT")
    assert output["date"].to_list() == ["2024-01-02"]
    assert output[0, "crypto_okx_session_coverage"] == 1.0
    assert output[0, "crypto_okx_available"] == 1.0
    assert output[0, "crypto_okx_funding_available"] == 0.0
    assert output[0, "crypto_okx_positioning_available"] == 0.0
    assert output[0, "crypto_okx_taker_available"] == 0.0
    assert output[0, "crypto_okx_source_available_at_utc"] == datetime(
        2024, 1, 2, 0, 0, tzinfo=timezone.utc
    )


def test_okx_mapping_handles_only_explicit_contract_denomination_prefixes() -> None:
    mapping = {"PEPE": "PEPEUSDT-SWAP", "1INCH": "1INCHUSDT-SWAP"}
    assert _resolve_okx_mapping("1000PEPE", mapping) == (
        "PEPEUSDT-SWAP",
        "denomination_1000_to_1",
    )
    assert _resolve_okx_mapping("1INCH", mapping) == (
        "1INCHUSDT-SWAP",
        "exact_base_coin",
    )
    assert _resolve_okx_mapping("2Z", mapping) == (None, None)


def test_okx_base_map_accepts_legacy_symbol_receipt(tmp_path: Path) -> None:
    pl.DataFrame(
        {
            "code": ["BTCUSDTSWAP"],
            "okx_symbol": ["BTC-USDT-SWAP"],
            "settle_ccy": ["USDT"],
            "ct_type": ["linear"],
            "state": ["live"],
        }
    ).write_csv(tmp_path / "symbols.csv")
    (tmp_path / "BTCUSDTSWAP_features.parquet").touch()

    assert _okx_base_map(tmp_path) == {"BTC": "BTCUSDTSWAP"}


def test_public_feature_schema_retains_missing_optional_families() -> None:
    output = _ensure_output_feature_schema(
        pl.DataFrame(
            {
                "date": ["2024-01-02"],
                "symbol": ["BTCUSDT"],
                "crypto_bybit_funding_available": [1.0],
            }
        )
    )

    assert "crypto_binance_core_available" in output.columns
    assert "crypto_okx_available" in output.columns
    assert "crypto_public_macro_available_fraction" in output.columns
    assert output[0, "crypto_binance_core_available"] is None


def test_fred_macro_uses_only_values_available_before_midnight(tmp_path: Path) -> None:
    path = tmp_path / "observations.parquet"
    pl.DataFrame(
        {
            "series_id": ["DFF", "DFF"],
            "observation_date": ["2024-01-01", "2024-01-02"],
            "value": [5.25, 5.50],
            "available_at_utc": [
                "2024-01-02T00:00:00+00:00",
                "2024-01-03T00:00:01+00:00",
            ],
        }
    ).write_parquet(path)
    output, receipt = fred_macro_rows(
        path, ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
    )
    assert output[0, "crypto_public_macro_fed_funds_rate"] is None
    assert output[1, "crypto_public_macro_fed_funds_rate"] == pytest.approx(0.0525)
    assert output[2, "crypto_public_macro_fed_funds_rate"] == pytest.approx(0.0525)
    assert output[3, "crypto_public_macro_fed_funds_rate"] == pytest.approx(0.055)
    assert receipt["revision_backprojection"] is False


def test_fred_initial_release_parser_waits_until_next_utc_day() -> None:
    payload = (
        b'{"observations":[{"realtime_start":"2024-01-02",'
        b'"realtime_end":"2024-02-01","date":"2023-12-01","value":"3.5"}]}'
    )
    rows = parse_initial_release_rows(
        "NFCI",
        payload,
        retrieved_at_utc=datetime(2024, 2, 2, tzinfo=timezone.utc),
    )
    assert rows[0]["available_at_utc"] == "2024-01-03T00:00:00+00:00"
    windows = _realtime_windows("2000-01-01", "2010-01-01")
    assert all(
        (date.fromisoformat(end) - date.fromisoformat(start)).days <= 1824
        for start, end in windows
    )


def test_sec_etf_filings_wait_until_next_midnight(tmp_path: Path) -> None:
    sec = tmp_path / "sec" / "0001"
    sec.mkdir(parents=True)
    pl.DataFrame(
        {
            "accession_number": ["0001-24-000001"],
            "registered_assets": ["BTC"],
            "form": ["S-1"],
            "available_at_utc": ["2024-01-01T15:30:00+00:00"],
        }
    ).write_parquet(sec / "filings.parquet")
    output, receipt = sec_etf_filing_rows(
        tmp_path / "sec",
        ["2024-01-01", "2024-01-02"],
        {"BTCUSDT": "BTC"},
    )
    market = output.filter(pl.col("symbol") == "__MARKET__").sort("date")
    btc = output.filter(pl.col("symbol") == "BTCUSDT").sort("date")
    assert market[0, "crypto_public_sec_etf_filings_1d_log1p"] == 0.0
    assert market[1, "crypto_public_sec_etf_filings_1d_log1p"] == pytest.approx(
        math.log(2.0)
    )
    assert btc[0, "crypto_public_sec_asset_available"] == 0.0
    assert btc[1, "crypto_public_sec_asset_registration_30d_log1p"] == pytest.approx(
        math.log(2.0)
    )
    assert receipt["availability_contract"] == (
        "sec_acceptance_datetime_le_decision_cutoff"
    )


def test_coinmetrics_latest_view_never_precedes_retrieval_vintage(
    tmp_path: Path,
) -> None:
    vintages = tmp_path / "vintages"
    vintages.mkdir()
    pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-01"],
            "metric": ["AdrActCnt", "TxCnt"],
            "value": [99.0, 50.0],
            "available_at_utc": [
                "2024-01-02T00:00:01+00:00",
                "2024-01-02T00:00:01+00:00",
            ],
        }
    ).write_parquet(vintages / "btc_vintages.parquet")
    output, receipt = coinmetrics_vintage_rows(
        vintages,
        ["2024-01-02", "2024-01-03"],
        {"BTCUSDT": "BTC"},
    )
    assert output["date"].to_list() == ["2024-01-03"]
    assert output[0, "crypto_public_onchain_active_addresses_log1p"] == pytest.approx(
        math.log(100.0)
    )
    assert receipt["latest_view_history_backprojected"] is False


def test_coingecko_snapshot_rejects_ambiguous_symbol_mapping(tmp_path: Path) -> None:
    snapshot_dir = (
        tmp_path / "snapshots" / "coingecko_market_snapshot" / "2024" / "01" / "01"
    )
    snapshot_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["btc", "pepe", "pepe"],
            "market_cap": [100.0, 80.0, 20.0],
            "fully_diluted_valuation": [110.0, 90.0, 30.0],
            "total_volume": [10.0, 8.0, 2.0],
            "market_cap_rank": [1.0, 10.0, 500.0],
            "circulating_supply": [20.0, 1000.0, 200.0],
            "available_at_utc": [
                "2024-01-01T12:00:00+00:00",
                "2024-01-01T12:00:00+00:00",
                "2024-01-01T12:00:00+00:00",
            ],
        }
    ).write_parquet(snapshot_dir / "snapshot.parquet")
    output, receipt = coingecko_snapshot_rows(
        tmp_path,
        ["2024-01-01", "2024-01-02"],
        {"BTCUSDT": "BTC", "1000PEPEUSDT": "1000PEPE"},
    )
    assets = output.filter(pl.col("symbol") != "__MARKET__")
    assert assets.select("date", "symbol").to_dicts() == [
        {"date": "2024-01-02", "symbol": "BTCUSDT"}
    ]
    assert receipt["asset_mapping"].endswith("share_ge_0.90")


def test_etf_issuer_snapshot_is_prospective_only(tmp_path: Path) -> None:
    normalized = tmp_path / "normalized"
    normalized.mkdir()
    pl.DataFrame(
        {
            "source_id": ["issuer_btc"],
            "asset": ["BTC"],
            "available_at_utc": ["2024-01-01T12:00:00+00:00"],
            "holding_ticker": ["BTC"],
            "market_value_usd": [1_000_000.0],
            "quantity": [20.0],
        }
    ).write_parquet(normalized / "issuer_holdings_snapshots.parquet")
    output, receipt = etf_issuer_snapshot_rows(
        tmp_path,
        ["2024-01-01", "2024-01-02"],
        {"BTCUSDT": "BTC"},
    )
    assert output["date"].to_list() == ["2024-01-02"]
    assert output[0, "crypto_public_etf_issuer_available"] == 1.0
    assert receipt["historical_snapshot_backprojection"] is False


def test_binance_public_daily_delays_bars_and_requires_complete_positioning(
    tmp_path: Path,
) -> None:
    timestamps = pl.datetime_range(
        datetime(2024, 1, 1, 0, 0),
        datetime(2024, 1, 1, 23, 45),
        interval="15m",
        eager=True,
    )
    values = np.linspace(100.0, 110.0, len(timestamps))
    funding_age = (np.arange(len(timestamps)) % 32) * 0.25
    source = pl.DataFrame(
        {
            "date": timestamps.dt.strftime("%Y-%m-%d %H:%M:%S"),
            "binance_volume_quote": np.full(len(timestamps), 100.0),
            "binance_trade_count": np.full(len(timestamps), 10),
            "binance_taker_buy_quote_volume": np.full(len(timestamps), 60.0),
            "binance_mark_open": values,
            "binance_mark_high": values + 1.0,
            "binance_mark_low": values - 1.0,
            "binance_mark_close": values + 0.2,
            "binance_index_open": values - 0.1,
            "binance_index_high": values + 0.8,
            "binance_index_low": values - 1.2,
            "binance_index_close": values + 0.1,
            "binance_funding_rate": np.full(len(timestamps), 0.001),
            "binance_funding_age_hours": funding_age,
            "binance_open_interest_value_usd": np.linspace(
                1_000_000.0, 1_100_000.0, len(timestamps)
            ),
            "binance_contract_mark_basis_log": np.full(len(timestamps), 0.001),
            "binance_mark_index_basis_log": np.full(len(timestamps), 0.0005),
            "binance_global_long_short_account_ratio_log": np.full(
                len(timestamps), 0.1
            ),
            "binance_top_long_short_account_ratio_log": np.full(len(timestamps), 0.2),
            "binance_top_long_short_position_ratio_log": np.full(len(timestamps), 0.3),
        }
    )
    path = tmp_path / "BTCUSDT_features.parquet"
    source.write_parquet(path)
    output = _binance_daily(path, "BTCUSDT")
    assert output["date"].to_list() == ["2024-01-02"]
    assert output[0, "crypto_binance_session_coverage"] == 1.0
    assert output[0, "crypto_binance_core_available"] == 1.0
    assert output[0, "crypto_binance_positioning_available"] == 1.0
    assert output[0, "crypto_binance_funding_realized_rate"] == pytest.approx(0.002)
    assert output[0, "crypto_binance_taker_imbalance_1d"] == pytest.approx(0.2)
    assert output[0, "crypto_binance_source_available_at_utc"] == datetime(
        2024, 1, 2, 0, 0, tzinfo=timezone.utc
    )


def test_bybit_strategy_config_keeps_multi_basis_fee_and_external_contract() -> None:
    config = load_config(
        "configs/markets/bybit_perpetual_daily_multi_basis_projection_l1.yaml"
    )
    assert config.trading.execution_mode == "crypto_perpetual"
    assert config.trading.buy_fee_rate == pytest.approx(0.00055)
    assert config.trading.sell_fee_rate == pytest.approx(0.00055)
    assert config.trading.crypto_stateful_proximal_allocator is True
    assert config.trading.crypto_execution_minute_utc == 5
    assert config.trading.crypto_proximal_cost_multiplier == pytest.approx(1.0)
    assert config.trading.max_turnover_ratio == pytest.approx(0.0)
    assert config.evaluation.eval_log_utility_periods_per_year == 365.0
    assert config.evaluation.gamma_turnover == pytest.approx(0.0)
    assert config.evaluation.gamma_turnover_budget == pytest.approx(0.0)
    assert config.training.model_name == "financial_transformer"
    assert config.training.lookback == 32
    assert action_channels_for_execution_mode("crypto_perpetual") == ("target",)
    model = config.training.financial_transformer
    assert model.temporal_basis_input == "input_features"
    assert len(model.temporal_basis_families) == 18
    assert model.portfolio_output_mode == "projection_l1"
    assert model.projection_l1_scale_by_active_count is True
    assert len(config.data.feature_include) == 141
    assert "crypto_bybit_funding_available" in config.data.feature_include
    assert "crypto_bybit_*" in config.data.feature_zero_fill
    assert "crypto_binance_*" in config.data.feature_zero_fill
    assert "crypto_public_macro_available_fraction" in config.data.feature_include
    assert "crypto_public_onchain_available_fraction" in config.data.feature_include
    assert "crypto_public_coingecko_asset_available" in config.data.feature_include
    external = external_panel_data_kwargs(config.data)
    assert external["external_include_features"] is True
    assert external["external_include_rules"] is False


def test_bybit_22_effective_rank_carry_config_adapts_reference_without_tw_rules() -> None:
    config = load_config(
        "configs/markets/"
        "bybit_perpetual_daily_multi_basis_22_effective_rank_projection_l1_"
        "carry_syncthing_20260823.yaml"
    )

    assert config.runner.output_dir == (
        "artifacts/markets/"
        "bybit_perpetual_daily_0000_execution_multi_basis_22_effective_rank_"
        "projection_l1_carry_syncthing_20260823_v2"
    )
    assert config.training.pretrained_initialization_root is None
    assert config.data.parquet_root == "data_bybit_daily_0000_training_20260823"
    assert config.data.external_feature_path.endswith(
        "bybit_crypto_public_daily_0000.parquet"
    )
    assert config.training.epochs == 1000
    assert config.training.batch_size_train == 16
    assert config.training.batch_size_eval == 16
    assert config.training.compile_loss is False
    assert config.environment.amp_dtype == "bf16"

    model = config.training.financial_transformer
    assert len(model.temporal_basis_families) == 22
    assert sum(model.temporal_basis_components_by_family.values()) == 524
    assert model.temporal_basis_components_by_family["pca_klt"] == 31
    assert model.temporal_basis_input == "input_features"
    assert model.center_long_short_logits is False
    assert model.portfolio_output_mode == "projection_l1"
    assert model.projection_l1_scale_by_active_count is True

    assert config.trading.execution_mode == "crypto_perpetual"
    assert config.trading.frequency == "daily"
    assert config.trading.long_only is False
    assert config.trading.buy_fee_rate == pytest.approx(0.00055)
    assert config.trading.sell_fee_rate == pytest.approx(0.00055)
    assert config.trading.crypto_stateful_proximal_allocator is True
    assert config.trading.crypto_execution_minute_utc == 0
    assert config.trading.reporting_leverage == pytest.approx(1.0)
    assert config.trading.tw_day_trade_unlimited_margin_conversion is False
    assert config.data.use_tw_public_rules is False
    assert config.evaluation.eval_log_utility_periods_per_year == 365.0
