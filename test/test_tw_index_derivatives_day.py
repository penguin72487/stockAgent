from __future__ import annotations

import math

import numpy as np
import pytest
import torch
from torch import nn

from stockagent.backtest.simulator import run_backtest_integer_shares, run_backtest_torch
from stockagent.backtest.tw_index_derivatives_day import (
    OptionDayCostSchedule,
    run_tw_index_derivatives_day_continuous,
    run_tw_index_derivatives_day_integer,
)
from stockagent.backtest.tw_index_futures import FuturesCostSchedule
from stockagent.config import load_config
from stockagent.data.tw_index_derivatives_day import (
    TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
    TAIFEX_OPTION_CANDIDATE_CAPACITY,
    TAIFEX_OPTION_CANDIDATE_FEATURE_DIM,
    build_causal_derivative_day_candidates,
    load_taiex_opening_index,
)
from stockagent.data.tw_index_futures import TaiwanIndexFuturesDaySession
from stockagent.data.tw_index_options_daily import (
    TaiwanIndexOptionChainDaySession,
    option_slot_index,
)
from stockagent.models.factory import build_model
from stockagent.models.normalization import (
    masked_cash_entmax15_weights,
    masked_l1_projection_weights,
)
from stockagent.training import trainer as trainer_module
from stockagent.training.trainer import _evaluate_windowed_tensor_batch
from stockagent.training.windowed import WindowedSplitTensors


def _dates() -> np.ndarray:
    return np.asarray(
        ["2025-01-02", "2025-01-03", "2025-01-06"], dtype="datetime64[D]"
    )


def _futures_market() -> TaiwanIndexFuturesDaySession:
    rows = 3
    products = ("TX", "MTX", "TMF")
    opens = np.full((rows, 3), 23_000.0)
    closes = np.asarray(
        [
            [23_050.0] * 3,
            [23_100.0] * 3,
            [22_900.0] * 3,
        ]
    )
    tenor_months = np.full((rows, 6), "", dtype="U6")
    tenor_months[:, :2] = np.asarray(["202501", "202502"])
    tenor_open = np.full((rows, 6, 3), np.nan)
    tenor_close = np.full((rows, 6, 3), np.nan)
    tenor_open[:, :2] = 23_000.0
    tenor_close[:, 0] = closes
    tenor_close[:, 1] = closes + 25.0
    tenor_valid = np.isfinite(tenor_open) & np.isfinite(tenor_close)
    tenor_returns = np.full((rows, 6, 3), np.nan, dtype=np.float32)
    tenor_returns[tenor_valid] = np.log(
        tenor_close[tenor_valid] / tenor_open[tenor_valid]
    ).astype(np.float32)
    return TaiwanIndexFuturesDaySession(
        dates=_dates(),
        products=products,
        contract_months=np.asarray([["202501"] * 3] * rows),
        open_prices=opens,
        high_prices=np.maximum(opens, closes),
        low_prices=np.minimum(opens, closes),
        close_prices=closes,
        volumes=np.full((rows, 3), 100, dtype=np.int64),
        log_returns=np.log(closes / opens).astype(np.float32),
        tradable_mask=np.ones((rows, 3), dtype=bool),
        multipliers=np.asarray([200.0, 50.0, 10.0]),
        tenor_contract_months=tenor_months,
        tenor_open_prices=tenor_open,
        tenor_high_prices=np.maximum(tenor_open, tenor_close),
        tenor_low_prices=np.minimum(tenor_open, tenor_close),
        tenor_close_prices=tenor_close,
        tenor_volumes=np.where(tenor_valid, 100, 0).astype(np.int64),
        tenor_log_returns=tenor_returns,
        tenor_tradable_mask=tenor_valid,
    )


def _option_chain(*, cheap_first: bool = False) -> TaiwanIndexOptionChainDaySession:
    # A second monthly strike first appears on row 1. It must not enter the
    # model's row-1 candidate set because that listing fact is same-session.
    base = [
        ("202501", 23_000.0, "C"),
        ("202501", 23_000.0, "P"),
        ("202501W2", 23_000.0, "C"),
        ("202501W2", 23_000.0, "P"),
        ("202501F1", 23_000.0, "C"),
        ("202501F1", 23_000.0, "P"),
    ]
    added = ("202501", 24_000.0, "C")
    row_contracts = [base, [*base, added], [*base, added]]
    offsets = [0]
    slots: list[int] = []
    series: list[str] = []
    strikes: list[float] = []
    rights: list[str] = []
    opens: list[float] = []
    closes: list[float] = []
    for row, contracts in enumerate(row_contracts):
        row_items = []
        for contract_index, (contract_series, strike, right) in enumerate(contracts):
            scope = "monthly" if "W" not in contract_series and "F" not in contract_series else "weekly"
            expiry_rank = 1 if "F" in contract_series else 0
            moneyness_rank = 1 if strike == 24_000.0 else 0
            slot = option_slot_index(scope, expiry_rank, moneyness_rank, right)
            row_items.append((slot, contract_index, contract_series, strike, right))
        for slot, contract_index, contract_series, strike, right in sorted(row_items):
            slots.append(slot)
            series.append(contract_series)
            strikes.append(strike)
            rights.append(right)
            open_price = 0.1 if cheap_first and row == 1 and contract_index == 0 else 100.0
            close_price = 0.05 if cheap_first and row == 1 and contract_index == 0 else (
                120.0 if right == "C" else 80.0
            )
            opens.append(open_price)
            closes.append(close_price)
        offsets.append(len(series))
    return TaiwanIndexOptionChainDaySession(
        dates=_dates(),
        row_offsets=np.asarray(offsets, dtype=np.int64),
        slot_indices=np.asarray(slots, dtype=np.int32),
        option_series=np.asarray(series),
        strikes=np.asarray(strikes),
        option_rights=np.asarray(rights),
        open_prices=np.asarray(opens),
        close_prices=np.asarray(closes),
        volumes=np.full(len(series), 10, dtype=np.int64),
        executable=np.ones(len(series), dtype=bool),
    )


def _candidates(*, cheap_first: bool = False, allow_option_short: bool = False):
    return build_causal_derivative_day_candidates(
        _futures_market(),
        _option_chain(cheap_first=cheap_first),
        fixed_fee_per_contract_per_side_twd=22.0,
        transaction_tax_rate=0.0002,
        slippage_points_per_side=0.5,
        allow_option_short=allow_option_short,
        option_risk_margin_a_twd=187_000.0,
        option_risk_margin_b_twd=94_000.0,
        option_margin_schedule_as_of="2026-08-12",
        underlying_index_open_prices=(
            np.full(3, 23_000.0) if allow_option_short else None
        ),
        option_margin_underlying_source=(
            "synthetic_official_taiex_open" if allow_option_short else ""
        ),
    )


def test_dual_5090_config_resolves_complete_ordinary_basis_contract() -> None:
    config = load_config(
        "configs/markets/tw_index_derivatives_day_multi_basis_dual_5090.yaml"
    )
    assert config.trading.execution_mode == "tw_index_derivatives_day"
    assert config.trading.tw_index_futures_initial_capital == 100_000_000.0
    assert config.trading.tw_index_futures_total_fee_per_side_twd == [60.0, 24.0, 16.0]
    assert config.trading.tw_index_futures_sell_transaction_tax_rate == pytest.approx(0.0002)
    assert config.trading.tw_index_derivatives_day_option_fixed_fee_per_contract_per_side_twd == 22.0
    assert config.training.epochs == 1000
    assert config.training.eval_model_chunk_rows == 128
    assert config.training.financial_transformer.temporal_basis_input == "input_features"
    assert len(config.training.financial_transformer.temporal_basis_families) == 18
    assert config.training.financial_transformer.temporal_basis_components == 4
    assert config.training.financial_transformer.temporal_pooling == "last"
    assert config.training.financial_transformer.temporal_query_mode == "last_only"
    assert config.training.financial_transformer.portfolio_output_mode == "projection_l1"
    assert "relative_tenor_v5" in str(config.runner.output_dir)


def test_candidates_are_prior_session_known_and_separate_wed_fri_families() -> None:
    candidates = _candidates()
    assert candidates.action_count == 4102
    assert candidates.option_candidate_features.shape == (
        3,
        TAIFEX_OPTION_CANDIDATE_CAPACITY,
        TAIFEX_OPTION_CANDIDATE_FEATURE_DIM,
    )
    # Row 1 sees exactly row 0's six contracts, not the newly listed strike.
    assert int(candidates.option_candidate_mask[1].sum()) == 6
    # Row 2 sees the newly listed monthly strike. Friday F1 has expired by
    # Monday, so 7 prior rows minus 2 expired Friday legs = 5.
    assert int(candidates.option_candidate_mask[2].sum()) == 5
    families = candidates.option_candidate_features[1, :6, :3]
    assert set(map(tuple, families.tolist())) == {
        (1.0, 0.0, 0.0),
        (0.0, 1.0, 0.0),
        (0.0, 0.0, 1.0),
    }
    assert candidates.futures_contract_months[1, :2].tolist() == ["202501", "202502"]


def test_derivative_model_masks_invalid_candidates_before_sparse_allocation() -> None:
    torch.manual_seed(7)
    config = load_config(
        "configs/markets/tw_index_derivatives_day_multi_basis_dual_5090.yaml"
    )
    model = build_model(
        config=config,
        lookback=32,
        num_features=3,
        num_symbols=6,
        feature_names=["a", "b", "c"],
    )
    candidate_features = torch.randn(2, 4096, 9)
    candidate_mask = torch.zeros(2, 4102, dtype=torch.bool)
    candidate_mask[:, :2] = True
    candidate_mask[:, 6:14] = True
    actions, _, aux = model(
        torch.randn(2, 32, 6, 3),
        torch.ones(2, 6, dtype=torch.bool),
        return_aux=True,
        portfolio_context={
            "candidate_features": candidate_features,
            "candidate_mask": candidate_mask,
        },
    )
    assert actions.shape == (2, TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4)
    assert torch.all(actions[:, 6:] >= 0.0)
    assert torch.count_nonzero(actions.masked_select(~candidate_mask)) == 0
    gross = actions[:, :6].abs().sum(dim=-1) + actions[:, 6:].sum(dim=-1)
    assert torch.all(gross <= 0.98 + 1e-6)
    expected = masked_l1_projection_weights(
        aux["derivative_raw_actions"],
        candidate_mask,
        long_only=False,
        radius=0.98,
    )
    torch.testing.assert_close(actions, expected)
    assert model.temporal_pooling == "last"
    assert model.temporal_query_mode == "last_only"
    assert model.portfolio_output_mode == "projection_l1"
    assert not hasattr(model, "derivative_capital_head")
    assert model.candle_encoder.joint_input_dim == 219
    actions.square().sum().backward()
    gradients = [parameter.grad for parameter in model.parameters() if parameter.grad is not None]
    assert gradients
    assert all(torch.isfinite(gradient).all() for gradient in gradients)


def test_cash_entmax_v7_can_hold_cash_without_gate_or_option_cap() -> None:
    torch.manual_seed(11)
    config = load_config(
        "configs/markets/tw_index_derivatives_day_multi_basis_cash_entmax_v7.yaml"
    )
    assert config.training.financial_transformer.temporal_pooling == "last"
    assert config.training.financial_transformer.temporal_query_mode == "last_only"
    assert config.training.financial_transformer.portfolio_output_mode == "cash_entmax15"
    assert not config.trading.tw_index_derivatives_day_use_exposure_gate
    assert config.trading.tw_index_derivatives_day_option_maximum_capital_fraction == 0.98
    assert config.trading.tw_index_derivatives_day_allow_option_short
    assert config.trading.tw_index_derivatives_day_option_risk_margin_a_twd == 187_000.0
    assert config.trading.tw_index_derivatives_day_option_risk_margin_b_twd == 94_000.0
    assert config.trading.tw_index_derivatives_day_option_risk_margin_c_twd == 18_800.0
    assert config.trading.tw_index_derivatives_day_option_margin_schedule_as_of == "2026-08-12"
    assert config.trading.tw_index_derivatives_day_underlying_index_path == (
        "data_tw_public/twse_taiex_ohlc.parquet"
    )
    assert "cash_entmax_v7" in str(config.runner.output_dir)

    model = build_model(
        config=config,
        lookback=32,
        num_features=3,
        num_symbols=6,
        feature_names=["a", "b", "c"],
    )
    candidate_mask = torch.ones(2, 4102, dtype=torch.bool)
    actions, _, aux = model(
        torch.randn(2, 32, 6, 3),
        torch.ones(2, 6, dtype=torch.bool),
        return_aux=True,
        portfolio_context={
            "candidate_features": torch.randn(2, 4096, 9),
            "candidate_mask": candidate_mask,
        },
    )
    short_mask = candidate_mask.clone()
    expected = masked_cash_entmax15_weights(
        aux["derivative_allocation_logits"],
        candidate_mask,
        short_mask=short_mask,
        radius=0.98,
    )
    torch.testing.assert_close(actions, expected)
    assert "derivative_projected_actions" not in aux
    assert "derivative_capital_gate" not in aux
    assert not hasattr(model, "derivative_capital_head")
    assert model.allow_option_short
    option_gross = actions[:, 6:].abs().sum(dim=-1)
    total_gross = actions[:, :6].abs().sum(dim=-1) + option_gross
    assert torch.all(option_gross <= 0.98 + 1e-6)
    assert torch.all(total_gross <= 0.98 + 1e-6)
    torch.testing.assert_close(
        aux["cash_fraction"], (1.0 - total_gross).unsqueeze(-1)
    )
    actions.square().sum().backward()
    gradients = [parameter.grad for parameter in model.parameters() if parameter.grad is not None]
    assert gradients
    assert all(torch.isfinite(gradient).all() for gradient in gradients)


def test_gated_v6_path_is_compatibility_alias_for_cash_entmax_v7() -> None:
    config = load_config(
        "configs/markets/tw_index_derivatives_day_multi_basis_gated_v6.yaml"
    )
    assert config.training.financial_transformer.portfolio_output_mode == "cash_entmax15"
    assert not config.trading.tw_index_derivatives_day_use_exposure_gate
    assert config.trading.tw_index_derivatives_day_option_maximum_capital_fraction == 0.98
    assert config.trading.tw_index_derivatives_day_allow_option_short
    assert "cash_entmax_v7" in str(config.runner.output_dir)


def test_option_simple_return_is_not_clipped_per_leg_below_minus_one() -> None:
    candidates = _candidates(cheap_first=True)
    assert candidates.option_simple_returns[1, 0] < -1.0
    open_price = 0.1
    close_price = 0.05
    expected = (
        close_price / open_price
        - 1.0
        - 44.0 / (open_price * 50.0)
        - 0.0002 * close_price / open_price
        - 1.0 / open_price
    )
    assert candidates.option_simple_returns[1, 0] == pytest.approx(expected)


def test_short_option_uses_dated_naked_margin_and_directional_capital_return() -> None:
    candidates = _candidates(allow_option_short=True)
    assert candidates.allow_option_short
    assert candidates.option_margin_schedule_as_of == "2026-08-12"
    assert candidates.option_margin_underlying_source == (
        "synthetic_official_taiex_open"
    )
    # Row 1 / slot 0 is the prior-known 23,000 Call. At a 23,000 underlying
    # open its OTM deduction is zero: 100*50 premium + A = 192,000 TWD.
    assert candidates.option_short_initial_margins[1, 0] == pytest.approx(
        192_000.0
    )
    expected_short_pnl = (
        (100.0 - 120.0) * 50.0
        - 2.0 * 22.0
        - 0.0002 * 100.0 * 50.0
        - 2.0 * 0.5 * 50.0
    )
    assert candidates.option_short_simple_returns[1, 0] == pytest.approx(
        expected_short_pnl / 192_000.0
    )
    directional = candidates.simple_returns()
    assert directional.shape == (3, 4102, 2)

    actions = np.zeros((3, 4102), dtype=np.float64)
    actions[1, 6] = -192_000.0 / 100_000_000.0
    integer = run_tw_index_derivatives_day_integer(
        actions,
        _futures_market(),
        candidates,
        initial_capital=100_000_000.0,
        option_cost_schedule=OptionDayCostSchedule(
            fixed_fee_per_contract_per_side_twd=22.0,
            transaction_tax_rate=0.0002,
            slippage_points_per_side=0.5,
        ),
    )
    assert integer.option_contract_quantities[1, 0] == -1
    assert integer.executed_actions[1, 6] == pytest.approx(actions[1, 6])
    assert integer.net_pnl_twd[1] == pytest.approx(expected_short_pnl)

    continuous = run_tw_index_derivatives_day_continuous(
        torch.as_tensor(actions, dtype=torch.float32),
        torch.as_tensor(directional, dtype=torch.float32),
        futures_round_trip_cost_rate=0.0,
    )
    assert continuous.strategy_returns[1].item() == pytest.approx(
        expected_short_pnl / 100_000_000.0,
        abs=1e-9,
    )


def test_chunked_eval_preserves_directional_derivative_return_axis() -> None:
    rows, symbols = 5, 2
    derivative_returns = torch.zeros(
        rows,
        TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
        2,
        dtype=torch.float32,
    )
    derivative_returns[:, 0, 0] = 0.01
    derivative_returns[:, 0, 1] = -0.01
    split = WindowedSplitTensors(
        features=torch.zeros((rows, symbols, 1), dtype=torch.float32),
        valid_indices=torch.arange(1, rows),
        future_log_returns=torch.zeros((rows, symbols), dtype=torch.float32),
        tradable_mask=torch.ones((rows, symbols), dtype=torch.bool),
        can_buy_mask=torch.ones((rows, symbols), dtype=torch.bool),
        can_sell_mask=torch.ones((rows, symbols), dtype=torch.bool),
        benchmark=torch.zeros(rows, dtype=torch.float32),
        lookback=1,
        overnight_log_returns=derivative_returns,
        derivative_candidate_features=torch.zeros(
            rows,
            TAIFEX_OPTION_CANDIDATE_CAPACITY,
            TAIFEX_OPTION_CANDIDATE_FEATURE_DIM,
            dtype=torch.float32,
        ),
        derivative_candidate_mask=torch.ones(
            rows,
            TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
            dtype=torch.bool,
        ),
        execution_mode="tw_index_derivatives_day",
    )

    class _FixedDerivativeModel(nn.Module):
        def forward(self, x, mask, *, portfolio_context=None):
            del mask, portfolio_context
            actions = torch.zeros(
                int(x.size(0)),
                TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
                device=x.device,
                dtype=x.dtype,
            )
            actions[:, 0] = 0.5
            return actions

    runtime = trainer_module._ExecutionRuntime(
        mode="tw_index_derivatives_day",
        buy_fee_rates=None,
        sell_fee_rates=None,
        lot_sizes=None,
        settlement_lag_sessions=0,
    )
    backtest, _, _ = _evaluate_windowed_tensor_batch(
        _FixedDerivativeModel(),
        None,
        split,
        torch.device("cpu"),
        None,
        False,
        False,
        0.0,
        0.0,
        0.0,
        1.0,
        chunk_rows=3,
        backtest_chunk_rows=2,
        execution_runtime=runtime,
    )

    assert backtest.strategy_returns.shape == (len(split),)
    assert backtest.requested_weights_history is not None
    assert backtest.requested_weights_history.shape == (
        len(split),
        TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
    )
    torch.testing.assert_close(
        backtest.strategy_returns,
        torch.full((len(split),), math.log1p(0.005), dtype=torch.float32),
    )


def test_short_option_margin_requires_and_uses_official_taiex_open() -> None:
    common = {
        "fixed_fee_per_contract_per_side_twd": 22.0,
        "transaction_tax_rate": 0.0002,
        "slippage_points_per_side": 0.5,
        "allow_option_short": True,
        "option_risk_margin_a_twd": 187_000.0,
        "option_risk_margin_b_twd": 94_000.0,
        "option_margin_schedule_as_of": "2026-08-12",
    }
    with pytest.raises(ValueError, match="official positive TAIEX open"):
        build_causal_derivative_day_candidates(
            _futures_market(),
            _option_chain(),
            **common,
        )

    candidates = build_causal_derivative_day_candidates(
        _futures_market(),
        _option_chain(),
        underlying_index_open_prices=np.asarray([23_000.0, 22_800.0, 23_000.0]),
        option_margin_underlying_source="synthetic_official_taiex_open",
        **common,
    )
    # Row 1 slot 0 is a 23,000 Call. With TAIEX open at 22,800, its 200-point
    # OTM value is 10,000 TWD: 5,000 premium + (187,000 - 10,000).
    assert candidates.option_short_initial_margins[1, 0] == pytest.approx(
        182_000.0
    )


def test_official_taiex_open_loader_aligns_dates_and_fails_closed(tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "taiex.parquet"
    pq.write_table(
        pa.table(
            {
                "date": pa.array(
                    [date.item() for date in _dates()], type=pa.date32()
                ),
                "opening_index": [23_000.0, 22_800.0, 23_100.0],
            }
        ),
        path,
    )
    loaded = load_taiex_opening_index(path, panel_dates=_dates()[[2, 0]])
    np.testing.assert_allclose(loaded, [23_100.0, 23_000.0])
    with pytest.raises(ValueError, match="does not cover every panel session"):
        load_taiex_opening_index(
            path,
            panel_dates=np.asarray(["2025-01-07"], dtype="datetime64[D]"),
        )


def test_integer_costs_and_relative_tenor_mapping_finish_flat() -> None:
    candidates = _candidates()
    actions = np.zeros((3, 4102), dtype=np.float64)
    # Row 1: exact one-TX notional in E1 and one 100-point option contract.
    actions[1, 0] = 4_600_000.0 / 100_000_000.0
    actions[1, 6] = 5_000.0 / 100_000_000.0
    result = run_tw_index_derivatives_day_integer(
        actions,
        _futures_market(),
        candidates,
        initial_capital=100_000_000.0,
        futures_cost_schedule=FuturesCostSchedule(
            tax_rate=0.0002,
            exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
            broker_fee_per_side_twd=(40.0, 11.5, 8.0),
        ),
        option_cost_schedule=OptionDayCostSchedule(
            fixed_fee_per_contract_per_side_twd=22.0,
            transaction_tax_rate=0.0002,
            slippage_points_per_side=0.0,
        ),
    )
    assert result.futures_contract_quantities[1, 0].tolist() == [1, 0, 0]
    assert result.option_contract_quantities[1, 0] == 1
    assert result.fees_twd[1] == pytest.approx(2 * 60.0 + 2 * 22.0)
    expected_tax = 23_100.0 * 200.0 * 0.0002 + 120.0 * 50.0 * 0.0002
    assert result.tax_twd[1] == pytest.approx(expected_tax)
    assert result.terminal_flat.all()


def test_continuous_keeps_missing_leg_cash_and_ruin_is_portfolio_level() -> None:
    raw = torch.zeros(2, 4102, requires_grad=True)
    raw.data[:, 0] = 0.2
    raw.data[:, 6:9] = 0.1
    returns = torch.full((2, 4102), float("nan"))
    returns[:, 0] = torch.tensor([0.01, -0.01])
    returns[0, 6:8] = torch.tensor([0.02, -20.0])
    returns[1, 6:9] = torch.tensor([0.02, -0.03, 0.04])
    result = run_tw_index_derivatives_day_continuous(
        raw,
        returns,
        futures_round_trip_cost_rate=0.00009,
    )
    assert result.executed_actions[0, 8] == 0.0
    assert result.strategy_returns[0] == -1.0
    assert not bool(result.alive[0])
    assert torch.count_nonzero(result.executed_actions[1]) == 0


def test_continuous_nonruin_gradient_is_finite() -> None:
    raw = torch.zeros(2, 4102, requires_grad=True)
    raw.data[:, 0] = 0.2
    raw.data[:, 6:9] = 0.1
    returns = torch.full((2, 4102), float("nan"))
    returns[:, 0] = torch.tensor([0.01, -0.01])
    returns[:, 6:9] = torch.tensor([0.02, -0.03, 0.04])
    result = run_tw_index_derivatives_day_continuous(
        raw, returns, futures_round_trip_cost_rate=0.00009
    )
    (-torch.log1p(result.strategy_returns).mean()).backward()
    assert raw.grad is not None and torch.isfinite(raw.grad).all()


def test_integer_audit_trades_only_derivatives_and_finishes_flat() -> None:
    rows, symbols = 3, 8
    candidates = _candidates()
    weights = np.zeros((rows, 4102), dtype=np.float64)
    weights[1:, 0] = 0.046
    weights[1:, 6] = 0.01
    future_returns = np.zeros((rows, symbols), dtype=np.float32)
    result, holdings = run_backtest_integer_shares(
        weights,
        future_returns,
        np.ones((rows, symbols), dtype=bool),
        np.zeros(rows, dtype=np.float32),
        initial_capital=100_000_000.0,
        long_only=False,
        execution_mode="tw_index_derivatives_day",
        dates=_dates(),
        futures_market=_futures_market(),
        derivatives_day_candidates=candidates,
        option_day_cost_schedule=OptionDayCostSchedule(),
    )
    assert result.requested_weights_history is not None
    assert result.requested_weights_history.shape == (rows, 4102)
    assert result.final_weights is not None and not result.final_weights.any()
    assert bool(result.final_alive)
    assert holdings
    assert all(
        record.symbol.startswith(("TX_", "MTX_", "TMF_", "TXO_"))
        for record in holdings
    )


def test_canonical_tensor_adapter_keeps_direct_derivative_request() -> None:
    rows, symbols = 2, 8
    weights = torch.zeros(rows, 4102)
    weights[:, 0] = 0.2
    weights[:, 6:9] = 0.1
    returns = torch.full((rows, 4102), float("nan"))
    returns[:, 0] = 0.01
    returns[:, 6:9] = torch.tensor([0.02, -0.03, 0.04])
    result = run_backtest_torch(
        weights,
        torch.zeros((rows, symbols)),
        torch.ones(rows, symbols, dtype=torch.bool),
        torch.zeros(rows),
        0.000045,
        0.000245,
        long_only=False,
        gross_leverage=0.98,
        portfolio_activation="pre_normalized",
        execution_mode="tw_index_derivatives_day",
        overnight_returns=returns,
    )
    assert result.weights_history.shape == (rows, symbols)
    assert result.requested_weights_history is not None
    assert result.requested_weights_history.shape == (rows, 4102)
    assert torch.isfinite(result.strategy_returns).all()
