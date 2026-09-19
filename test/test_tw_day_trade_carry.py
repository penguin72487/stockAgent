from dataclasses import fields, replace
from datetime import date

import numpy as np
import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import (
    DayTradeCarrySession, DayTradeCarryState, compact_day_trade_carry_session,
    run_day_trade_carry_sessions,
)
from stockagent.data.tw_day_trade_schedule import paper_minute_opportunities
from stockagent.data.tw_price_rules import limit_price_numpy
from stockagent.backtest.simulator import run_backtest_torch
from stockagent.training.loss import risk_aware_loss
from stockagent.backtest.tw_day_trade_inventory import (
    DayTradeInventoryState,
    accrue_inventory_interest,
    append_inventory_fill,
    apply_inventory_action,
    convert_inventory_to_margin,
    inventory_nav,
    reduce_inventory_fifo,
    release_inventory_stock_deliveries,
    validate_inventory_state,
)


DAY = date(2026, 8, 13).toordinal()


def v(*values):
    return torch.tensor(values, dtype=torch.float64)


def session(day=DAY, price=1000., volume=6000., exits=False):
    # Positive sub-lot observations establish price but no whole-lot capacity.
    # Zero-volume source padding must not stand in for a fresh trade.
    bars = np.tile([price, price, price, price, price, 6000. if exits else 200.], (1, 270, 1))
    bars[0, 0, 5] = volume or 200.
    lower, upper = (torch.from_numpy(limit_price_numpy(np.array([price]), ratio,
        np.datetime64(date.fromordinal(day)))) for ratio in (.9, 1.1))
    schedule = paper_minute_opportunities(bars, trading_date=np.datetime64(date.fromordinal(day)),
        lower_limit=lower.numpy(), upper_limit=upper.numpy(), security_types=np.array(['stock']))
    return DayTradeCarrySession(day, v(price), v(price), v(price), v(volume),
        lower, upper, v(0), torch.from_numpy(schedule.prices),
        torch.from_numpy(schedule.capacity_shares), torch.from_numpy(schedule.marks))


def run(weights, sessions, state=None, fees=True, event_compression=False):
    return run_day_trade_carry_sessions(weights, tuple(sessions), can_enter=torch.ones_like(weights),
        buy_fee_rate=v(.001425 if fees else 0).expand(weights.shape[-1]),
        day_sell_fee_rate=v(.002925 if fees else 0).expand(weights.shape[-1]),
        normal_sell_fee_rate=v(.004425 if fees else 0).expand(weights.shape[-1]),
        rebate_rate=v(.00114 if fees else 0).expand(weights.shape[-1]),
        initial_capital=10_000_000., initial_state=state,
        event_compression=event_compression)


def assert_state(a, b):
    torch.testing.assert_close(a.last_nav, b.last_nav, rtol=0, atol=1e-8)
    assert torch.equal(a.alive, b.alive)
    for name in a.inventory.__dataclass_fields__:
        torch.testing.assert_close(getattr(a.inventory, name), getattr(b.inventory, name), rtol=0, atol=1e-8)


def test_full_run_equals_chunked_physical_inventory_and_all_minute_marks():
    sessions = [session(), session(DAY + 1, 1010), session(DAY + 4, 1020, exits=True)]
    weights = v(.21, -.21, .1).reshape(-1, 1).requires_grad_()
    full = run(weights, sessions)
    first = run(weights[:1], sessions[:1])
    tail = run(weights[1:], sessions[1:], first.final_state.detached())
    torch.testing.assert_close(full.minute_nav, torch.cat((first.minute_nav, tail.minute_nav)), rtol=0, atol=1e-8)
    torch.testing.assert_close(full.strategy_returns, torch.cat((first.strategy_returns, tail.strategy_returns)), rtol=0, atol=1e-12)
    assert_state(full.final_state, tail.final_state)
    assert full.shares_history[0, 0] == 2000
    assert full.shares_history[1, 0] == -1000  # reduce 2000, open 1000; one 3000 capacity
    assert full.shares_history[2, 0] == 0
    assert full.minute_nav.shape == (3, 270)
    (-full.strategy_returns.mean()).backward()
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0


def test_event_compression_preserves_daily_return_state_turnover_and_gradient():
    sessions = [session(), session(DAY + 1, 1010), session(DAY + 4, 1020, exits=True)]
    full_weights = v(.21, -.21, .1).reshape(-1, 1).requires_grad_()
    compact_weights = full_weights.detach().clone().requires_grad_()
    full = run(full_weights, sessions)
    compact = run(compact_weights, sessions, event_compression=True)

    assert compact.minute_nav.shape == (3, 2)
    torch.testing.assert_close(compact.strategy_returns, full.strategy_returns, rtol=0, atol=1e-12)
    torch.testing.assert_close(compact.turnovers, full.turnovers, rtol=0, atol=1e-10)
    assert_state(compact.final_state, full.final_state)
    (-full.strategy_returns.mean()).backward()
    (-compact.strategy_returns.mean()).backward()
    torch.testing.assert_close(compact_weights.grad, full_weights.grad, rtol=1e-10, atol=1e-12)


def test_sparse_event_sufficient_statistic_preserves_endpoint_and_gradient():
    dense_sessions = [
        session(),
        session(DAY + 1, 1010),
        session(DAY + 4, 1020, exits=True),
    ]
    sparse_sessions = [compact_day_trade_carry_session(x) for x in dense_sessions]
    assert all(x.uses_sparse_events for x in sparse_sessions)
    assert sum(x.exit_prices.numel() for x in sparse_sessions) < 3 * 270 * 2

    dense_weights = v(.21, -.21, .1).reshape(-1, 1).requires_grad_()
    sparse_weights = dense_weights.detach().clone().requires_grad_()
    dense = run(dense_weights, dense_sessions, event_compression=True)
    sparse = run(sparse_weights, sparse_sessions, event_compression=True)

    torch.testing.assert_close(sparse.strategy_returns, dense.strategy_returns, rtol=0, atol=1e-12)
    torch.testing.assert_close(sparse.turnovers, dense.turnovers, rtol=0, atol=1e-10)
    # The sparse ABI stores [intraday certificate bound, exact close], whereas
    # the dense compressed ABI stores [exact minute minimum, exact close].
    # Compare the quantity consumed by the batch solvency certificate, not the
    # intentionally different first-slot representation.
    assert bool(sparse.minute_nav.amin(dim=-1).le(
        dense.minute_nav.amin(dim=-1) + 1e-8
    ).all())
    torch.testing.assert_close(
        sparse.minute_nav[:, -1], dense.minute_nav[:, -1], rtol=0, atol=1e-8
    )
    assert_state(sparse.final_state, dense.final_state)
    (-dense.strategy_returns.mean()).backward()
    (-sparse.strategy_returns.mean()).backward()
    torch.testing.assert_close(sparse_weights.grad, dense_weights.grad, rtol=1e-10, atol=1e-12)


@pytest.mark.parametrize("seed", range(8))
def test_sparse_event_multisymbol_multiday_matches_full_path_and_gradient(seed):
    """Exercise recurrent reversals/cohorts, not only a one-symbol endpoint."""
    generator = torch.Generator().manual_seed(seed)
    symbols, days = 7, 5
    sessions = []
    prior_close = 500 + 1000 * torch.rand(
        symbols, generator=generator, dtype=torch.float64
    )
    for offset in range(days):
        official_open = prior_close * (
            0.98
            + 0.04
            * torch.rand(symbols, generator=generator, dtype=torch.float64)
        )
        entry_price = official_open * (
            0.999
            + 0.002
            * torch.rand(symbols, generator=generator, dtype=torch.float64)
        )
        entry_volume = (
            50 + torch.randint(0, 100, (symbols,), generator=generator)
        ).double() * 1000
        marks = official_open[:, None] * (
            0.97
            + 0.06
            * torch.rand(
                (symbols, 270), generator=generator, dtype=torch.float64
            )
        )
        exit_prices = torch.full(
            (symbols, 270, 2), float("nan"), dtype=torch.float64
        )
        exit_capacity = torch.zeros_like(exit_prices)
        event_minutes = torch.tensor([1, 17, 83, 260, 264, 269])
        for event in event_minutes:
            exit_prices[:, event, :] = official_open[:, None] * (
                0.96
                    + 0.08
                    * torch.rand(
                        (symbols, 2), generator=generator, dtype=torch.float64
                    )
            )
            exit_capacity[:, event, :] = (
                torch.randint(0, 5, (symbols, 2), generator=generator).double()
                * 1000
            )
        # Preserve explicit searchsorted plateaus and a missing event cell.
        exit_capacity[:, event_minutes[1], :] = 0
        exit_prices[::2, event_minutes[2], 1] = float("nan")
        session_row = DayTradeCarrySession(
            DAY + offset,
            official_open,
            prior_close,
            entry_price,
            entry_volume,
            official_open * 0.8,
            official_open * 1.2,
            torch.zeros(symbols, dtype=torch.float64),
            exit_prices,
            exit_capacity,
            marks,
        )
        sessions.append(session_row)
        prior_close = marks[:, -1]

    raw_weights = 0.08 * (
        2
        * torch.rand(
            (days, symbols), generator=generator, dtype=torch.float64
        )
        - 1
    )
    # Guaranteed sign changes create reduction plus opening on the same budget.
    raw_weights[1] = -raw_weights[0]
    raw_weights[3] = -raw_weights[2]
    full_weights = raw_weights.clone().requires_grad_()
    sparse_weights = raw_weights.clone().requires_grad_()
    full = run(full_weights, sessions)
    sparse = run(
        sparse_weights,
        [compact_day_trade_carry_session(row) for row in sessions],
        event_compression=True,
    )
    torch.testing.assert_close(
        sparse.strategy_returns, full.strategy_returns, rtol=0, atol=1e-10
    )
    torch.testing.assert_close(sparse.turnovers, full.turnovers, rtol=0, atol=1e-8)
    torch.testing.assert_close(
        sparse.minute_nav[:, -1], full.minute_nav[:, -1], rtol=0, atol=1e-7
    )
    assert bool(sparse.minute_nav.amin(dim=-1).le(
        full.minute_nav.amin(dim=-1) + 1e-7
    ).all())
    assert_state(sparse.final_state, full.final_state)
    (-full.strategy_returns.mean()).backward()
    (-sparse.strategy_returns.mean()).backward()
    torch.testing.assert_close(
        sparse_weights.grad, full_weights.grad, rtol=1e-9, atol=1e-11
    )


def test_inconclusive_event_certificate_falls_back_to_full_minute_default():
    import stockagent.backtest.tw_day_trade_carry as carry

    carry.reset_day_trade_carry_compile_stats()
    first = run(v(-.9).reshape(1, 1), [session(volume=20_000)], fees=False)
    stress = session(DAY + 1, 1000, volume=0)
    stress.marks[0, 20] = 3000
    expected = run(v(-.9).reshape(1, 1), [stress], first.final_state, fees=False)
    compact = run(
        v(-.9).reshape(1, 1),
        [stress],
        first.final_state,
        fees=False,
        event_compression=True,
    )
    # The fallback computes the authoritative full curve but returns the fixed
    # compact ABI: exact worst NAV and exact closing NAV.
    assert compact.minute_nav.shape == (1, 2)
    torch.testing.assert_close(compact.strategy_returns, expected.strategy_returns)
    torch.testing.assert_close(compact.minute_nav[:, 0], expected.minute_nav.amin(-1))
    torch.testing.assert_close(compact.minute_nav[:, 1], expected.minute_nav[:, -1])
    assert_state(compact.final_state, expected.final_state)
    assert carry.get_day_trade_carry_compile_stats()["event_compression_fallback_batches"] == 1


def test_overnight_move_is_measured_from_previous_close_not_reset_open():
    weights = v(.21, .21).reshape(-1, 1)
    result = run(weights, [session(), session(DAY + 1, 1100, volume=0)], fees=False)
    expected = result.minute_nav[1, -1] / result.minute_nav[0, -1]
    assert expected > 1
    torch.testing.assert_close(result.strategy_returns[1].exp(), expected)


def test_physical_action_and_dated_cash_claim_survive_the_session_boundary():
    second = replace(session(DAY + 1, 2000, volume=0), action_mask=v(1), share_ratio=v(.5),
                     cash_per_old_share=v(1), payment_day=v(DAY + 4))
    result = run(v(.21, .21).reshape(-1, 1), [session(), second], fees=False)
    assert result.final_state.inventory.shares.item() == 1000
    assert result.final_state.inventory.corporate_action_receivable.item() == 2000
    third = run(v(.21).reshape(-1, 1), [session(DAY + 4, 2000, volume=0)], result.final_state)
    assert third.final_state.inventory.corporate_action_receivable.item() == 0
    assert third.final_state.inventory.corporate_action_cash.item() == 2000


@pytest.mark.parametrize("direction", [1.0, -1.0])
def test_pending_stock_entitlement_is_valued_but_not_executable_until_delivery(
    direction,
):
    state = DayTradeInventoryState.empty(1)
    zero = v(0)
    state = append_inventory_fill(
        state,
        signed_shares=v(direction * 2000),
        price=v(100),
        buy_fee_rate=zero,
        day_sell_fee_rate=zero,
        normal_sell_fee_rate=zero,
        rebate_rate=zero,
        day=DAY,
    )
    state = convert_inventory_to_margin(state, day=DAY)
    before_nav = inventory_nav(
        state, initial_capital=10_000_000.0, marks=v(100)
    )
    state = apply_inventory_action(
        state,
        event_day=DAY + 1,
        as_of_day=DAY + 1,
        event_mask=v(1),
        share_ratio=v(1.1),
        cash_per_old_share=zero,
        payment_day=zero,
        stock_delivery_day=v(DAY + 10),
    )
    after_nav = inventory_nav(
        state, initial_capital=10_000_000.0, marks=v(100 / 1.1)
    )
    torch.testing.assert_close(after_nav, before_nav, rtol=0, atol=1e-8)
    assert state.shares.item() == direction * 2200
    assert state.locked_shares.item() == direction * 200
    assert state.tradable_shares.item() == direction * 2000

    reduced = reduce_inventory_fifo(
        state,
        requested_shares=v(3000),
        price=v(100 / 1.1),
        capacity_shares=v(3000),
    )
    assert reduced.filled_shares.item() == 2000
    assert reduced.state.shares.item() == direction * 200
    assert reduced.state.locked_shares.item() == direction * 200
    delivered = release_inventory_stock_deliveries(
        reduced.state,
        day=DAY + 10,
    )
    delivered = accrue_inventory_interest(delivered, day=DAY + 10)
    assert delivered.locked_shares.item() == 0
    assert delivered.tradable_shares.item() == direction * 200
    validate_inventory_state(delivered, symbols=1)


def test_proven_halt_uses_separate_valuation_never_executable_open():
    first = run(v(.21).reshape(-1, 1), [session()])
    halted = replace(session(DAY + 1, volume=0), official_open=v(float('nan')), halted=v(1))
    second = run(v(-.21).reshape(-1, 1), [halted], first.final_state)
    assert second.turnovers.item() == 0
    assert second.shares_history.item() == 2000
    assert torch.isfinite(second.minute_nav).all()


def test_missing_held_minute_mark_is_source_failure_not_default_or_flat_cash():
    bad = session()
    bad.marks[0, 20] = float('nan')
    with pytest.raises(RuntimeError, match='minute valuation'):
        run(v(.21).reshape(-1, 1), [bad])


def test_exact_symbol_day_source_mask_preserves_peers_dates_and_no_reallocation():
    from dataclasses import fields
    one = session(exits=True)
    two = replace(one, **{f.name: torch.cat((getattr(one, f.name), getattr(one, f.name)), dim=0)
        for f in fields(one) if isinstance(getattr(one, f.name), torch.Tensor)})
    bad = replace(two, source_gap_mask=v(1, 0))
    # Leave missing prices missing; a flat excluded symbol needs no made-up NAV mark.
    bad.marks[0] = float('nan')
    bad.official_open[0] = float('nan')
    bad.entry_price[0] = float('nan')
    bad = replace(bad, action_mask=v(1, 0), share_ratio=v(float('nan'), 1),
                  cash_per_old_share=v(float('nan'), 0), payment_day=v(float('nan'), 0))
    weights = v(.21, .21).reshape(1, 2).requires_grad_()
    result = run(weights, [bad])
    healthy = run(v(.21).reshape(1, 1), [session(exits=True)])
    torch.testing.assert_close(result.minute_nav, healthy.minute_nav, rtol=0, atol=1e-8)
    assert result.minute_nav.shape == (1, 270)
    assert result.final_state.inventory.action_cursor[0] == 0
    (-result.strategy_returns.sum()).backward()
    assert weights.grad[0, 0] == 0
    # The same stock on another date is still eligible; the day list is retained.
    later = run(v(.21, .21).reshape(1, 2), [replace(two, day=DAY + 1,
        official_open=v(1000, 1000), entry_price=v(1000, 1000),
        marks=torch.full((2, 270), 1000., dtype=torch.float64))])
    assert later.turnovers.item() > result.turnovers.item()


@pytest.mark.parametrize('weight', [.21, -.21])
def test_source_mask_rejects_existing_long_or_short_without_mutating_state(weight):
    first = run(v(weight).reshape(1, 1), [session()])
    before = first.final_state.detached()
    with pytest.raises(RuntimeError, match='cannot mask away ownership'):
        run(v(0).reshape(1, 1), [replace(session(DAY + 1), source_gap_mask=v(1))], first.final_state)
    assert_state(first.final_state, before)


@pytest.mark.parametrize('amount', [100., -100.])
def test_minute_source_gap_preserves_exact_cash_claim_and_payment_without_quotes(amount):
    state = DayTradeCarryState.empty(1, 10_000_000., torch.device('cpu'))
    state = replace(state, inventory=replace(state.inventory,
        claims=v(amount, DAY + 3, 0).reshape(1, 1, 3)), last_nav=v(10_000_000 + amount).squeeze())
    before = state.detached()
    missing = replace(session(), source_gap_mask=v(1), official_open=v(float('nan')),
        entry_price=v(float('nan')), marks=torch.full((1, 270), float('nan'), dtype=torch.float64))
    first = run(v(.21).reshape(1, 1), [missing], state)
    assert first.turnovers.item() == 0
    assert first.final_state.inventory.shares.item() == 0
    assert first.final_state.inventory.corporate_action_net.item() == amount
    assert first.final_state.inventory.corporate_action_cash.item() == 0
    paid = run(v(.21).reshape(1, 1), [replace(missing, day=DAY + 3)], first.final_state)
    assert paid.final_state.inventory.corporate_action_cash.item() == amount
    assert paid.final_state.inventory.corporate_action_net.item() == amount
    assert paid.final_state.last_nav.item() == 10_000_000 + amount
    assert_state(state, before)


def test_unheld_unknown_action_does_not_block_same_day_entry_or_grant_entitlement():
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
    from datetime import datetime
    # Actual paper gate short-circuits before reading issuer terms when flat.
    engine = object.__new__(TwDayTradeSimulationEngine)
    assert engine._margin_corporate_action_gate({'positions': {}}, datetime(2026, 8, 13, 9))
    base = session()
    unknown = replace(base, action_mask=v(1), share_ratio=v(float('nan')),
                      cash_per_old_share=v(float('nan')), payment_day=v(float('nan')))
    expected = run(v(.21).reshape(1, 1), [base])
    actual = run(v(.21).reshape(1, 1), [unknown])
    torch.testing.assert_close(actual.minute_nav, expected.minute_nav, rtol=0, atol=0)
    assert actual.final_state.inventory.shares.item() == 2000
    assert actual.final_state.inventory.corporate_action_net.item() == 0
    assert actual.final_state.inventory.action_cursor.item() == 0


def test_missing_0901_with_later_prices_never_backdates_an_entry():
    base = session(exits=True)
    base.marks[0, :2] = float('nan')
    base = replace(base, entry_price=v(float('nan')), entry_volume=v(0))
    actual = run(v(.21).reshape(1, 1), [base])
    assert actual.turnovers.item() == 0
    assert actual.final_state.inventory.shares.item() == 0
    assert torch.equal(actual.minute_nav, torch.full((1, 270), 10_000_000., dtype=torch.float64))


def test_explicit_daily_proxy_accepts_official_open_without_fabricated_limits():
    base = session(exits=True)
    missing_limits = replace(
        base,
        lower_limit=v(float("nan")),
        upper_limit=v(float("nan")),
    )
    rejected = run(v(.21).reshape(1, 1), [missing_limits])
    accepted = run(
        v(.21).reshape(1, 1),
        [replace(missing_limits, daily_proxy_mask=v(1))],
    )
    assert rejected.turnovers.item() == 0
    assert accepted.turnovers.item() > 0
    assert accepted.final_state.inventory.failed.item() == 0


@pytest.mark.parametrize("value", [-1, .5, float("nan"), float("inf")])
def test_daily_proxy_mask_requires_binary_evidence_flags(value):
    with pytest.raises(RuntimeError, match="daily proxy mask"):
        run(
            v(.21).reshape(1, 1),
            [replace(session(), daily_proxy_mask=v(value))],
        )


@pytest.mark.parametrize('claim', [(100., float('nan'), 0), (100., 0, 0),
                                  (100., DAY + .5, 0), (100., DAY, .5),
                                  (float('nan'), DAY, 0)])
def test_minute_mask_does_not_bypass_exact_cash_claim_contract(claim):
    state = DayTradeCarryState.empty(1, 10_000_000., torch.device('cpu'))
    state = replace(state, inventory=replace(state.inventory, claims=v(*claim).reshape(1, 1, 3)))
    with pytest.raises(RuntimeError, match='claim'):
        run(v(0).reshape(1, 1), [replace(session(), source_gap_mask=v(1))], state)


@pytest.mark.parametrize('weight', [.21, -.21])
def test_held_unknown_action_still_blocks_without_mutating_other_account_state(weight):
    first = run(v(weight).reshape(1, 1), [session()])
    before = first.final_state.detached()
    unknown = replace(session(DAY + 1), action_mask=v(1), share_ratio=v(float('nan')),
                      cash_per_old_share=v(float('nan')), payment_day=v(float('nan')))
    with pytest.raises(RuntimeError, match='corporate-action terms'):
        run(v(0).reshape(1, 1), [unknown], first.final_state)
    assert_state(first.final_state, before)


@pytest.mark.parametrize('value', [-1, .5, float('nan'), float('inf')])
def test_source_mask_requires_binary_evidence_flags(value):
    with pytest.raises(RuntimeError, match='exact binary'):
        run(v(.21).reshape(1, 1), [replace(session(), source_gap_mask=v(value))])


@pytest.mark.skipif(not torch.cuda.is_available(), reason='CUDA runtime unavailable')
def test_cuda_source_mask_failure_keeps_physical_evidence_and_context_healthy():
    from dataclasses import fields
    first = run(v(-.21).reshape(1, 1), [session()])
    state = first.final_state.detached(device='cuda')
    bad = replace(session(DAY + 1), source_gap_mask=v(1))
    bad = replace(bad, **{f.name: getattr(bad, f.name).cuda()
        for f in fields(bad) if isinstance(getattr(bad, f.name), torch.Tensor)})
    result = run(v(.21).cuda().reshape(1, 1), [bad], state)
    assert torch.isnan(result.strategy_returns).all()
    assert result.final_state.inventory.failed.item() == 1
    assert not result.settlement_default.item()  # source error is not bankruptcy
    torch.testing.assert_close(result.final_state.inventory.shares, state.inventory.shares)
    torch.testing.assert_close(result.final_state.inventory.carry_cost, state.inventory.carry_cost)
    assert result.turnovers.item() == 0
    assert torch.arange(4, device='cuda').sum().item() == 6


@pytest.mark.skipif(not torch.cuda.is_available(), reason='CUDA runtime unavailable')
def test_cuda_unknown_held_action_is_atomic_and_does_not_poison_device():
    from dataclasses import fields
    first = run(v(.21).reshape(1, 1), [session()])
    state = first.final_state.detached(device='cuda')
    unknown = replace(session(DAY + 1), action_mask=v(1), share_ratio=v(float('nan')),
                      cash_per_old_share=v(float('nan')), payment_day=v(float('nan')))
    unknown = replace(unknown, **{f.name: getattr(unknown, f.name).cuda()
        for f in fields(unknown) if isinstance(getattr(unknown, f.name), torch.Tensor)})
    actual = run(v(.21).cuda().reshape(1, 1), [unknown], state)
    assert torch.isnan(actual.minute_nav).all()
    assert actual.final_state.inventory.failed.item() == 1
    assert actual.final_state.inventory.realized_net_pnl.item() == state.inventory.realized_net_pnl.item()
    torch.testing.assert_close(actual.final_state.inventory.shares, state.inventory.shares)
    assert actual.final_state.inventory.action_cursor.item() == 0
    assert not actual.settlement_default.item()
    assert torch.ones(3, device='cuda').sum().item() == 3


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA runtime unavailable")
def test_cuda_compiled_pending_stock_delivery_matches_cpu_and_gradient():
    from dataclasses import fields

    pending = replace(
        session(DAY + 1, 800, volume=0),
        action_mask=v(1),
        share_ratio=v(1.25),
        cash_per_old_share=v(0),
        payment_day=v(0),
        stock_delivery_day=v(DAY + 10),
    )
    sessions = [
        session(),
        pending,
        session(DAY + 2, 800, volume=6000, exits=True),
        session(DAY + 10, 800, volume=6000, exits=True),
    ]
    cpu_weights = v(.21, .21, 0, 0).reshape(-1, 1).requires_grad_()
    expected = run(cpu_weights, sessions, fees=False)
    cuda_sessions = [
        DayTradeCarrySession(
            **{
                field.name: (
                    getattr(row, field.name).cuda()
                    if isinstance(getattr(row, field.name), torch.Tensor)
                    else getattr(row, field.name)
                )
                for field in fields(row)
            }
        )
        for row in sessions
    ]
    cuda_weights = cpu_weights.detach().cuda().requires_grad_()
    actual = run_day_trade_carry_sessions(
        cuda_weights,
        tuple(cuda_sessions),
        can_enter=torch.ones_like(cuda_weights),
        buy_fee_rate=v(0).cuda(),
        day_sell_fee_rate=v(0).cuda(),
        normal_sell_fee_rate=v(0).cuda(),
        rebate_rate=v(0).cuda(),
        initial_capital=10_000_000.0,
    )

    torch.testing.assert_close(
        actual.strategy_returns.cpu(), expected.strategy_returns, rtol=0, atol=1e-12
    )
    torch.testing.assert_close(
        actual.final_state.inventory.cohorts.cpu(),
        expected.final_state.inventory.cohorts,
        rtol=0,
        atol=1e-8,
    )
    assert actual.shares_history[:, 0].cpu().tolist() == [2000, 2500, 500, 0]
    (-expected.strategy_returns.sum()).backward()
    (-actual.strategy_returns.sum()).backward()
    torch.testing.assert_close(
        cuda_weights.grad.cpu(), cpu_weights.grad, rtol=1e-8, atol=1e-10
    )


def test_financial_default_is_absorbing_and_does_not_invalidate_source():
    first = run(v(-.9).reshape(-1, 1), [session(volume=20_000)], fees=False)
    # An extreme source-backed move is a solvency stress, not a data NaN.
    second = run(v(.9).reshape(-1, 1), [session(DAY + 1, 3000)], first.final_state, fees=False)
    assert second.settlement_default.item()
    assert second.final_state.last_nav.item() == 0
    assert second.final_state.inventory.failed.item() == 0
    assert second.turnovers.item() == 0
    later = run(v(.9).reshape(-1, 1), [session(DAY + 2)], second.final_state, fees=False)
    assert later.strategy_returns.item() == 0
    assert later.turnovers.item() == 0
    assert later.final_state.last_nav.item() == 0
    torch.testing.assert_close(later.final_state.inventory.shares, first.final_state.inventory.shares)


def test_solvent_session_reuses_the_exact_first_fifo_path(monkeypatch):
    import stockagent.backtest.tw_day_trade_carry as carry

    calls = 0
    original = carry.reduce_inventory_fifo_path

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(carry, "reduce_inventory_fifo_path", counted)
    result = run(v(.21).reshape(1, 1), [session(exits=True)])
    assert result.final_state.alive
    assert calls == 1


def test_intraday_insolvency_keeps_the_capacity_cutoff_replay(monkeypatch):
    import stockagent.backtest.tw_day_trade_carry as carry

    first = run(v(-.9).reshape(1, 1), [session(volume=20_000)], fees=False)
    stress = session(DAY + 1, 1000, volume=0)
    # The account is solvent at the open, then a source-backed intraday spike
    # bankrupts the short inventory.  This is distinct from an opening default.
    stress.marks[0, 20] = 3000
    calls = 0
    original = carry.reduce_inventory_fifo_path

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(carry, "reduce_inventory_fifo_path", counted)
    result = run(
        v(-.9).reshape(1, 1),
        [stress],
        first.final_state,
        fees=False,
    )
    assert result.settlement_default.item()
    assert calls == 2


def test_source_precision_chronology_and_actions_fail_closed():
    first = session()
    with pytest.raises(ValueError, match='float64'):
        run(v(.2).reshape(-1, 1), [replace(first, official_open=first.official_open.float())])
    with pytest.raises(ValueError, match='chronological'):
        run(v(.2, .2).reshape(-1, 1), [first, first])
    with pytest.raises(ValueError, match='all exact'):
        run(v(.2).reshape(-1, 1), [replace(first, action_mask=v(1))])
    with pytest.raises(ValueError, match='initial capital'):
        DayTradeCarryState.empty(1, 0, torch.device('cpu'))


def canonical_kwargs(weights, sessions):
    return dict(execution_mode="tw_day_trade", day_trade_unlimited_margin_conversion=True,
        long_only=False, portfolio_activation="pre_normalized", buy_fee_rate=.001425,
        sell_fee_rate=.002925, normal_sell_fee_rates=v(.004425), commission_rebate_rates=v(.00114),
        day_trade_eligible_mask=torch.ones_like(weights, dtype=torch.bool),
        day_trade_can_buy_open_mask=torch.ones_like(weights, dtype=torch.bool),
        day_trade_can_sell_open_mask=torch.ones_like(weights, dtype=torch.bool),
        can_short_open_mask=torch.ones_like(weights, dtype=torch.bool),
        day_trade_execution_initial_capital=10_000_000., day_trade_carry_sessions=tuple(sessions))


def test_canonical_simulator_and_loss_share_exact_minute_ledger_and_chunk_state():
    sessions = [session(), session(DAY + 1, 1010), session(DAY + 4, 1020, exits=True)]
    weights = v(.21, -.21, .1).reshape(-1, 1).requires_grad_()
    expected = run(weights, sessions)
    result = run_backtest_torch(weights, torch.zeros_like(weights), torch.ones_like(weights),
        torch.zeros(3), **canonical_kwargs(weights, sessions))
    torch.testing.assert_close(result.strategy_returns, expected.strategy_returns)
    torch.testing.assert_close(result.minute_nav, expected.minute_nav)
    assert_state(result.day_trade_carry_state, expected.final_state)
    assert result.to_numpy().minute_nav.dtype == np.float64
    assert result.to_numpy().strategy_returns.dtype == np.float64
    assert result.to_numpy().day_trade_carry_state.last_nav.device.type == "cpu"
    aux = {}
    options = dict(objective="log_utility", gamma_turnover=0, concentration_weight=0,
                   aux_outputs=aux)
    loss = risk_aware_loss(weights, torch.zeros_like(weights), torch.ones_like(weights),
        **canonical_kwargs(weights, sessions), **options)
    torch.testing.assert_close(loss, -252 * expected.strategy_returns.mean())
    assert_state(aux["_final_day_trade_carry_state"], expected.final_state)
    loss.backward()
    assert torch.isfinite(weights.grad).all() and weights.grad.abs().sum() > 0
    # Only physical state continues. Legacy normalized weights are not enough.
    first_aux = {}
    risk_aware_loss(weights[:1], torch.zeros_like(weights[:1]), torch.ones_like(weights[:1]),
        **canonical_kwargs(weights[:1], sessions[:1]), **{**options, "aux_outputs": first_aux})
    tail_aux = {"initial_day_trade_carry_state": first_aux["_final_day_trade_carry_state"]}
    tail_loss = risk_aware_loss(weights[1:], torch.zeros_like(weights[1:]), torch.ones_like(weights[1:]),
        **canonical_kwargs(weights[1:], sessions[1:]), **{**options, "aux_outputs": tail_aux})
    torch.testing.assert_close(tail_loss, -252 * expected.strategy_returns[1:].mean())
    assert_state(tail_aux["_final_day_trade_carry_state"], expected.final_state)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a real CUDA device")
def test_power_of_two_compiled_fifo_path_matches_eager_forward_state_and_gradient(monkeypatch):
    import stockagent.backtest.tw_day_trade_carry as carry

    def on_cuda(value):
        return replace(value, **{
            field.name: getattr(value, field.name).cuda()
            for field in fields(value)
            if isinstance(getattr(value, field.name), torch.Tensor)
        })

    sessions = tuple(on_cuda(value) for value in (
        session(), session(DAY + 1, 1010), session(DAY + 4, 1020, exits=True)
    ))
    eager_weights = v(.21, -.21, .1).cuda().reshape(-1, 1).requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", "0")
    eager = run(eager_weights, sessions)
    (-eager.strategy_returns.mean()).backward()

    compiled_weights = eager_weights.detach().clone().requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    carry.reset_day_trade_carry_compile_stats(clear_cache=True)
    compiled = run(compiled_weights, sessions)
    (-compiled.strategy_returns.mean()).backward()

    torch.testing.assert_close(compiled.strategy_returns, eager.strategy_returns, rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(compiled.minute_nav, eager.minute_nav, rtol=1e-12, atol=1e-7)
    torch.testing.assert_close(compiled.turnovers, eager.turnovers, rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(compiled_weights.grad, eager_weights.grad, rtol=1e-7, atol=1e-10)
    assert_state(compiled.final_state, eager.final_state)
    assert compiled.final_state.inventory.cohorts.shape == eager.final_state.inventory.cohorts.shape
    stats = carry.get_day_trade_carry_compile_stats()
    assert stats["compiled_session_calls"] == len(sessions)
    assert stats["session_compile_constructors"] == 1
    assert stats["compile_failures"] == 0
    assert stats["eager_fallback_calls"] == 0


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a real CUDA device")
def test_compiled_event_compression_matches_eager_state_return_and_gradient(monkeypatch):
    import stockagent.backtest.tw_day_trade_carry as carry

    def on_cuda(value):
        return replace(value, **{
            field.name: getattr(value, field.name).cuda()
            for field in fields(value)
            if isinstance(getattr(value, field.name), torch.Tensor)
        })

    sessions = tuple(on_cuda(value) for value in (
        session(), session(DAY + 1, 1010), session(DAY + 4, 1020, exits=True)
    ))
    eager_weights = v(.21, -.21, .1).cuda().reshape(-1, 1).requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", "0")
    eager = run(eager_weights, sessions, event_compression=True)
    (-eager.strategy_returns.mean()).backward()

    compiled_weights = eager_weights.detach().clone().requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    carry.reset_day_trade_carry_compile_stats(clear_cache=True)
    compiled = run(compiled_weights, sessions, event_compression=True)
    (-compiled.strategy_returns.mean()).backward()

    assert compiled.minute_nav.shape == (3, 2)
    torch.testing.assert_close(compiled.strategy_returns, eager.strategy_returns, rtol=1e-12, atol=1e-12)
    torch.testing.assert_close(compiled.minute_nav, eager.minute_nav, rtol=1e-12, atol=1e-7)
    torch.testing.assert_close(compiled.turnovers, eager.turnovers, rtol=1e-12, atol=1e-10)
    torch.testing.assert_close(compiled_weights.grad, eager_weights.grad, rtol=1e-7, atol=1e-10)
    assert_state(compiled.final_state, eager.final_state)
    stats = carry.get_day_trade_carry_compile_stats()
    assert stats["compiled_session_calls"] == len(sessions)
    assert stats["session_compile_constructors"] == 1
    assert stats["compile_failures"] == 0


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a real CUDA device")
def test_compiled_sparse_event_stream_matches_dense_endpoint_and_gradient(monkeypatch):
    import stockagent.backtest.tw_day_trade_carry as carry

    def on_cuda(value):
        return replace(value, **{
            field.name: getattr(value, field.name).cuda()
            for field in fields(value)
            if isinstance(getattr(value, field.name), torch.Tensor)
        })

    dense_sessions = tuple(on_cuda(value) for value in (
        session(), session(DAY + 1, 1010), session(DAY + 4, 1020, exits=True)
    ))
    sparse_sessions = tuple(
        compact_day_trade_carry_session(value) for value in dense_sessions
    )
    dense_weights = v(.21, -.21, .1).cuda().reshape(-1, 1).requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", "0")
    dense = run(dense_weights, dense_sessions, event_compression=True)
    (-dense.strategy_returns.mean()).backward()

    sparse_weights = dense_weights.detach().clone().requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_COMPILE", "1")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    carry.reset_day_trade_carry_compile_stats(clear_cache=True)
    sparse = run(sparse_weights, sparse_sessions, event_compression=True)
    (-sparse.strategy_returns.mean()).backward()

    torch.testing.assert_close(sparse.strategy_returns, dense.strategy_returns, rtol=1e-12, atol=1e-12)
    assert bool(sparse.minute_nav.amin(dim=-1).le(
        dense.minute_nav.amin(dim=-1) + 1e-7
    ).all())
    torch.testing.assert_close(
        sparse.minute_nav[:, -1], dense.minute_nav[:, -1], rtol=1e-12, atol=1e-7
    )
    torch.testing.assert_close(sparse.turnovers, dense.turnovers, rtol=1e-12, atol=1e-10)
    torch.testing.assert_close(sparse_weights.grad, dense_weights.grad, rtol=1e-7, atol=1e-10)
    assert_state(sparse.final_state, dense.final_state)
    stats = carry.get_day_trade_carry_compile_stats()
    assert stats["compiled_session_calls"] == len(sparse_sessions)
    assert stats["session_compile_constructors"] == 1
    assert stats["compile_failures"] == 0


def test_checkpoint_binds_capital_release_universe_and_default_chronology(tmp_path):
    live = run(v(-.9).reshape(-1, 1), [session(volume=20_000)], fees=False)
    dead = run(v(.9).reshape(-1, 1), [session(DAY + 1, 3000)], live.final_state, fees=False)
    after = run(v(.9).reshape(-1, 1), [session(DAY + 2)], dead.final_state, fees=False)
    for state in (live.final_state, dead.final_state, after.final_state):
        payload = state.checkpoint_state(universe=("2330",), release_id="exact-public-and-minute-digest")
        path = tmp_path / "state.pt"
        torch.save(payload, path)
        restored = DayTradeCarryState.from_checkpoint_state(torch.load(path, weights_only=True),
            universe=("2330",), release_id="exact-public-and-minute-digest", initial_capital=10_000_000.)
        assert_state(restored, state)
        assert restored.last_session_day == state.last_session_day
        with pytest.raises(ValueError, match="capital"):
            DayTradeCarryState.from_checkpoint_state(payload, universe=("2330",),
                release_id="exact-public-and-minute-digest", initial_capital=1_000_000.)
        with pytest.raises(ValueError, match="universe/release"):
            DayTradeCarryState.from_checkpoint_state(payload, universe=("2330",),
                release_id="another-release", initial_capital=10_000_000.)
        with pytest.raises(ValueError, match="incompatible physical carry"):
            DayTradeCarryState.from_checkpoint_state(
                {**payload, 'abi': 'tw_day_trade_physical_fifo_sessions_v2'},
                universe=("2330",), release_id="exact-public-and-minute-digest",
                initial_capital=10_000_000.)
    with pytest.raises(ValueError, match="last committed"):
        run(v(.9).reshape(-1, 1), [session(DAY + 2)], after.final_state)
    with pytest.raises(RuntimeError, match="continuing carry"):
        run(v(.9).reshape(-1, 1), [session(DAY + 1)], replace(live.final_state, last_nav=v(0).squeeze()))


@pytest.mark.parametrize("extra", [
    {"initial_weights": torch.zeros(1)}, {"state_advance_mask": torch.tensor([False])},
    {"unresolved_corporate_action_mask": torch.tensor([[True]])},
    {"force_exit_mask": torch.tensor([[True]])}, {"overnight_returns": torch.zeros(1, 1)},
    {"day_trade_execution_volume_participation": 1.0},
])
def test_canonical_entry_refuses_silently_ignored_state_or_different_contract(extra):
    weights = v(.2).reshape(1, 1)
    with pytest.raises(ValueError, match="physical FIFO"):
        run_backtest_torch(weights, torch.zeros_like(weights), torch.ones_like(weights),
            torch.zeros(1), **{**canonical_kwargs(weights, [session()]), **extra})


def test_legacy_artifact_writer_cannot_silently_discard_physical_state(tmp_path):
    from stockagent.training.trainer import _save_backtest_artifact

    weights = v(.2).reshape(1, 1)
    result = run_backtest_torch(weights, torch.zeros_like(weights), torch.ones_like(weights),
        torch.zeros(1), **canonical_kwargs(weights, [session()])).to_numpy()
    target = tmp_path / "not_a_completed_training_artifact.npz"
    with pytest.raises(ValueError, match="legacy NPZ writer"):
        _save_backtest_artifact(target, result, np.array(["2026-08-13"], dtype="datetime64[D]"))
    assert not target.exists()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires a real CUDA device")
def test_cuda_source_failure_stays_nan_without_poisoning_context_or_financial_default():
    from dataclasses import fields
    weights = v(.2).reshape(1, 1).cuda().requires_grad_()
    bad = session()
    bad.marks[0, 20] = float("nan")
    bad = DayTradeCarrySession(**{f.name: getattr(bad, f.name).cuda()
        if isinstance(getattr(bad, f.name), torch.Tensor) else getattr(bad, f.name)
        for f in fields(bad)})
    options = canonical_kwargs(weights, [bad])
    options = {k: value.cuda() if isinstance(value, torch.Tensor) else value
               for k, value in options.items()}
    aux = {}
    loss = risk_aware_loss(weights, torch.zeros_like(weights), torch.ones_like(weights),
        **options, aux_outputs=aux, objective="log_utility", gamma_turnover=0, concentration_weight=0)
    assert torch.isnan(loss)
    state = aux["_final_day_trade_carry_state"]
    assert state.inventory.failed == 1
    assert state.alive  # bad data must not be labelled financial insolvency
    assert aux["_first_settlement_default_row"] == -1
    with pytest.raises((ValueError, RuntimeError), match="invalid|failed"):
        state.checkpoint_state(universe=("2330",), release_id="test-pinned-release")
    # A device-side assertion would also break this unrelated CUDA work.
    torch.testing.assert_close(torch.ones(2, device="cuda") * 3, torch.full((2,), 3., device="cuda"))
