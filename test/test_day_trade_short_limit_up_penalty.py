"""The close stress charge owns residual shares, never NAV or target weights."""
from dataclasses import fields, replace

import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import compact_day_trade_carry_session
from stockagent.data.tw_day_trade_carry_source import build_prepared_day_trade_carry_source
from stockagent.training.day_trade_carry_bridge import PreparedDayTradeCarryBatch
from test_tw_day_trade_carry import session, run, v, assert_state
from test_day_trade_carry_source import _physical_source_inputs


@pytest.mark.parametrize("fees", [False, True])
@pytest.mark.parametrize("partial_cover", [False, True])
def test_charge_only_source_residual_before_unlimited_research_cover(fees, partial_cover):
    base = session(volume=6000, exits=False)
    prices, capacity = base.exit_prices.clone(), base.exit_capacity.clone()
    if partial_cover:
        prices[0, 264, 1], capacity[0, 264, 1] = 1100., 1000.
    original = replace(base, exit_prices=prices, exit_capacity=capacity,
                       terminal_liquidation_price=v(1100.))
    stressed = replace(original, short_limit_up_penalty_per_share=v(1100. * .07))
    weights = v(-.4).reshape(1, 1)  # Entry remains capped at 3,000 shares.
    baseline = run(weights, [original], fees=fees)
    actual = run(weights, [stressed], fees=fees)
    remaining = 2000 if partial_cover else 3000
    torch.testing.assert_close(baseline.final_state.last_nav - actual.final_state.last_nav,
                               v(remaining * 1100. * .07).squeeze(), rtol=0, atol=1e-8)
    torch.testing.assert_close(actual.turnovers, baseline.turnovers, rtol=0, atol=1e-12)
    assert actual.shares_history[0, 0] == 0
    # A close expense is not an overnight borrow/interest or fictitious trade.
    torch.testing.assert_close(actual.final_state.inventory.carry_cost,
                               baseline.final_state.inventory.carry_cost, rtol=0, atol=0)


@pytest.mark.parametrize("weight,exits,per_share", [(0.21, False, 77.),
                                                    (-.21, True, 77.),
                                                    (-.21, False, 0.)])
def test_long_fully_covered_and_non_limit_up_positions_pay_nothing(weight, exits, per_share):
    original = replace(session(volume=6000, exits=exits), terminal_liquidation_price=v(1100.))
    stressed = replace(original, short_limit_up_penalty_per_share=v(per_share))
    baseline = run(v(weight).reshape(1, 1), [original])
    actual = run(v(weight).reshape(1, 1), [stressed])
    assert_state(actual.final_state, baseline.final_state)
    torch.testing.assert_close(actual.strategy_returns, baseline.strategy_returns, rtol=0, atol=0)


def test_penalty_gradient_reaches_actions_and_sparse_path_preserves_it():
    original = replace(session(volume=6000, exits=False), terminal_liquidation_price=v(1100.))
    stressed = replace(original, short_limit_up_penalty_per_share=v(77.))
    results, grads = [], []
    for current in (original, stressed, compact_day_trade_carry_session(stressed)):
        weights = v(-.21).reshape(1, 1).requires_grad_()
        result = run(weights, [current], fees=False, event_compression=True)
        # NAV derivative is useful away from the discrete lot boundary under
        # the selected original straight-through sizing contract.
        grad, = torch.autograd.grad(result.final_state.last_nav, weights)
        results.append(result)
        grads.append(grad)
    torch.testing.assert_close(grads[1] - grads[0], v(770000.).reshape(1, 1), rtol=1e-10, atol=1e-6)
    torch.testing.assert_close(grads[2], grads[1], rtol=1e-10, atol=1e-8)
    assert_state(results[2].final_state, results[1].final_state)


def test_nonterminal_penalty_is_assessed_once_per_session_not_each_mark():
    stressed = replace(session(volume=6000, exits=False),
                       short_limit_up_penalty_per_share=v(70.))
    original = replace(stressed, short_limit_up_penalty_per_share=None)
    actual, baseline = (run(v(-.21).reshape(1, 1), [s], fees=False)
                        for s in (stressed, original))
    torch.testing.assert_close(baseline.final_state.last_nav - actual.final_state.last_nav,
                               v(140000.).squeeze(), rtol=0, atol=1e-8)
    assert actual.shares_history[0, 0] == -2000


def test_close_charge_can_trigger_absorbing_ruin_without_invalidating_source():
    held = run(v(-.9).reshape(1, 1), [session(volume=20000)], fees=False)
    # An extreme move leaves a solvent carried account with a small buffer.
    # Entry/exit capacity is zero; only the additional close expense ruins it.
    original = replace(session(day=held.final_state.last_session_day + 1,
                               price=2100., volume=0, exits=False),
                       terminal_liquidation_price=v(2100.))
    stressed = replace(original, short_limit_up_penalty_per_share=v(2100. * .07))
    weights = v(-.9).reshape(1, 1)
    baseline, actual = (run(weights, [s], state=held.final_state, fees=False)
                        for s in (original, stressed))
    assert baseline.final_state.alive
    assert not actual.final_state.alive
    assert actual.final_state.last_nav == 0
    assert actual.final_state.inventory.failed == 0
    later = run(v(.9).reshape(1, 1), [session(day=original.day + 1)],
                state=actual.final_state, fees=False)
    assert later.strategy_returns.item() == 0
    assert later.turnovers.item() == 0
    assert later.final_state.last_nav.item() == 0


def test_float32_stored_limit_close_uses_the_existing_exact_price_grid(tmp_path):
    import numpy as np
    import polars as pl
    panel, public, minute = _physical_source_inputs(tmp_path)
    official = public / "tpex_daily_ohlcv.parquet"
    pl.read_parquet(official).with_columns(
        pl.lit("55.1").alias("次日漲停價")
    ).write_parquet(official)
    panel.open_prices = panel.open_prices.astype(np.float32)
    panel.close_prices = panel.close_prices.astype(np.float32)
    panel.close_prices[1, 1] = np.float32(55.1)
    assert float(panel.close_prices[1, 1]) != 55.1
    source = build_prepared_day_trade_carry_source(
        panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache", allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close", corporate_action_mode="avoid",
        terminal_liquidation_unlimited_capacity=True, short_limit_up_penalty_rate=.07,
    )
    actual = source.packed_session_loader(1)
    assert actual.terminal_liquidation_price[1] == 55.1
    torch.testing.assert_close(actual.short_limit_up_penalty_per_share,
                               v(0., 55.1 * .07, 0.), rtol=0, atol=1e-12)


def test_source_uses_official_close_and_exact_limit_then_preserves_all_transports(tmp_path):
    panel, public, minute = _physical_source_inputs(tmp_path)
    # The published legal band is based on the observed opening reference.
    # Closing at 109 instead of 110 must not pay merely for an intraday touch.
    panel.close_prices[1, :2] = [110., 54.]
    source = build_prepared_day_trade_carry_source(
        panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache", allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close", corporate_action_mode="avoid",
        terminal_liquidation_unlimited_capacity=True, short_limit_up_penalty_rate=.07,
    )
    dense = source.session_at(1)
    torch.testing.assert_close(dense.short_limit_up_penalty_per_share, v(7.7, 0., 0.))
    packed = source.packed_session_loader(1)
    rebuilt = PreparedDayTradeCarryBatch.from_packed_sessions((packed,), 1,
        event_compression=True).sessions(torch.device("cpu"))[0]
    compact = source.compact_session_loader(1)
    for current in (rebuilt, compact):
        torch.testing.assert_close(current.short_limit_up_penalty_per_share,
                                   dense.short_limit_up_penalty_per_share, rtol=0, atol=0)
    assert source.audit_receipt['short_limit_up_close_penalty']['rate'] == .07


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
@pytest.mark.parametrize("parallel", ["0", "2"])
@pytest.mark.parametrize("fused", ["0", "1"])
def test_actual_cuda_graph_forward_and_vjp_match_eager(monkeypatch, parallel, fused):
    from stockagent.backtest.tw_day_trade_carry import run_day_trade_carry_sessions
    from stockagent.backtest.tw_day_trade_graph import get_explicit_carry_graph_stats
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", "0")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP", "0")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS", "2")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT", parallel)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_GRAPH_ADJOINT", "saved")
    days = [replace(session(day=739840 + i, volume=6000, exits=False),
                    terminal_liquidation_price=v(1100.),
                    short_limit_up_penalty_per_share=v(77.)) for i in range(2)]
    days = tuple(replace(s, **{f.name: getattr(s, f.name).cuda()
                 for f in fields(s) if isinstance(getattr(s, f.name), torch.Tensor)}) for s in days)
    weights = v(-.21, .21).reshape(2, 1).cuda().requires_grad_()
    common = dict(can_enter=torch.ones_like(weights), buy_fee_rate=v(0.).cuda(),
                  day_sell_fee_rate=v(0.).cuda(), normal_sell_fee_rate=v(0.).cuda(),
                  rebate_rate=v(0.).cuda(), initial_capital=10_000_000.,
                  event_compression=True, require_minute_nav_grad=False)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH", "0")
    reference = run_day_trade_carry_sessions(weights, days, **common, _allow_flat_terminal=False)
    expected, = torch.autograd.grad(reference.strategy_returns.sum(), weights)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH", "1")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP", fused)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", fused)
    before = get_explicit_carry_graph_stats()
    actual = run_day_trade_carry_sessions(weights, days, **common)
    observed, = torch.autograd.grad(actual.strategy_returns.sum(), weights)
    after = get_explicit_carry_graph_stats()
    assert sum(after.get(key, 0) - value for key, value in before.items()
               if key.endswith("replays")) > 0
    torch.testing.assert_close(actual.strategy_returns, reference.strategy_returns, rtol=1e-9, atol=1e-10)
    torch.testing.assert_close(observed, expected, rtol=1e-6, atol=1e-9)


def test_penalty_opt_in_owns_config_and_checkpoint_identity():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import (
        _configuration_fingerprint_snapshot, _trading_checkpoint_contract,
    )
    config = load_config("configs/markets/tw_day_trade_unit_mgda_capital_20261103_v1.yaml")
    enabled = _trading_checkpoint_contract(config)["taiwan_execution"]
    assert enabled["short_limit_up_close_penalty"]["rate"] == .07
    assert _configuration_fingerprint_snapshot(config)["trading"][
        "tw_day_trade_short_limit_up_penalty_rate"] == .07
    config.trading.tw_day_trade_short_limit_up_penalty_rate = 0.
    assert "short_limit_up_close_penalty" not in _trading_checkpoint_contract(config)["taiwan_execution"]
    assert "tw_day_trade_short_limit_up_penalty_rate" not in _configuration_fingerprint_snapshot(config)["trading"]
