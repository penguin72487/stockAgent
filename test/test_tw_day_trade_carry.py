from dataclasses import replace
from datetime import date

import numpy as np
import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import (
    DayTradeCarrySession, DayTradeCarryState, run_day_trade_carry_sessions,
)
from stockagent.data.tw_day_trade_schedule import paper_minute_opportunities
from stockagent.data.tw_price_rules import limit_price_numpy
from stockagent.backtest.simulator import run_backtest_torch
from stockagent.training.loss import risk_aware_loss


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


def run(weights, sessions, state=None, fees=True):
    return run_day_trade_carry_sessions(weights, tuple(sessions), can_enter=torch.ones_like(weights),
        buy_fee_rate=v(.001425 if fees else 0).expand(weights.shape[-1]),
        day_sell_fee_rate=v(.002925 if fees else 0).expand(weights.shape[-1]),
        normal_sell_fee_rate=v(.004425 if fees else 0).expand(weights.shape[-1]),
        rebate_rate=v(.00114 if fees else 0).expand(weights.shape[-1]),
        initial_capital=10_000_000., initial_state=state)


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
