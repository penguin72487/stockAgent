"""A daily order is frozen; its minute fills do not create new decisions."""

from datetime import date

import pytest
import torch

from stockagent.backtest.tw_day_trade_inventory import (
    CohortField as F,
    DayTradeInventoryState,
    convert_inventory_to_margin,
    rebalance_inventory_at_open,
)

DAY = date(2026, 9, 24).toordinal()


def assert_paper_minute_nav(engine, market, start, expected):
    """Compare the published 270 right-labelled marks, not only terminal NAV."""
    import json
    from datetime import timedelta
    import numpy as np
    rows = [json.loads(line) for line in engine.marks_path.read_text().splitlines()]
    actual = {row['minute']: row['total_equity_twd'] for row in rows
              if row['market'] == market and row['session_date'] == start.date().isoformat()}
    clock = [(start + timedelta(minutes=i)).isoformat(timespec='minutes') for i in range(270)]
    assert sorted(actual) == clock
    np.testing.assert_allclose([actual[minute] for minute in clock],
                               expected.detach().numpy(), rtol=1e-12, atol=1e-7)


def execute(state, *, price=100., volume=2000., day=DAY, target=None, nav=None):
    v = lambda x: torch.tensor([x], dtype=torch.float64)
    return rebalance_inventory_at_open(
        state, weights=v(.051), official_open=v(100), opening_marks=v(100),
        entry_price=v(price), entry_volume_shares=v(volume), lower_limit=v(90),
        upper_limit=v(110), can_enter=v(1), buy_fee_rate=v(.001425),
        day_sell_fee_rate=v(.002925), normal_sell_fee_rate=v(.004425),
        rebate_rate=v(.00114), initial_capital=10_000_000., day=day,
        frozen_target_shares=target, frozen_sizing_nav=nav,
    )


def test_continuation_freezes_target_and_keeps_one_weighted_cohort():
    first = execute(DayTradeInventoryState.empty(1))
    assert first.target_shares.item() == 5000
    assert first.state.shares.item() == 1000
    second = execute(first.state, price=101, target=first.target_shares,
                     nav=first.sizing_nav)
    third = execute(second.state, price=102, volume=6000,
                    target=first.target_shares, nav=first.sizing_nav)
    assert third.state.shares.item() == 5000
    assert third.state.cohorts.shape[0] == 1
    assert third.state.cohorts[0, 0, F.BASIS].item() == pytest.approx(101.4)
    assert third.state.cohorts[0, 0, F.ENTRY_COST].item() == pytest.approx(144.495)
    assert third.state.decision_day.item() == DAY
    assert third.state.carry_cost.item() == 0


def test_zero_volume_continuation_is_noop_and_no_second_decision():
    first = execute(DayTradeInventoryState.empty(1))
    second = execute(first.state, volume=0, target=first.target_shares,
                     nav=first.sizing_nav)
    torch.testing.assert_close(first.state.cohorts, second.state.cohorts, rtol=0, atol=0)
    with pytest.raises(RuntimeError, match="daily decision"):
        execute(first.state)
    with pytest.raises(ValueError, match="both frozen"):
        execute(first.state, target=first.target_shares)


def test_next_day_continuation_requires_new_decision():
    first = execute(DayTradeInventoryState.empty(1))
    overnight = convert_inventory_to_margin(first.state, day=DAY)
    with pytest.raises(RuntimeError, match="daily decision"):
        execute(overnight, day=DAY + 1, target=first.target_shares, nav=first.sizing_nav)


def test_failure_diagnostic_names_invariant_without_mutating_inventory():
    from stockagent.backtest.tw_day_trade_carry import (
        DayTradeCarryState, _failed_session_cpu_diagnostic,
    )
    state = DayTradeCarryState.empty(1, 10_000_000., torch.device('cpu'))
    vector = lambda value: torch.tensor([value], dtype=torch.float64)
    diagnosis = _failed_session_cpu_diagnostic(state, sweep_session(),
        weights=vector(.051), can_enter=vector(1), buy_fee_rate=vector(-1),
        day_sell_fee_rate=vector(.003), normal_sell_fee_rate=vector(.004),
        rebate_rate=vector(0), initial_capital=10_000_000.)
    assert 'invalid gross funding fee' in diagnosis
    assert 'invalid_indices=[[0]]' in diagnosis
    assert state.inventory.failed.item() == 0
    assert state.inventory.cohorts.shape[0] == 0


def test_continuation_keeps_finite_action_gradient():
    v = lambda x: torch.tensor([x], dtype=torch.float64)
    first = execute(DayTradeInventoryState.empty(1), volume=0)
    target = v(5000).requires_grad_()
    second = execute(first.state, volume=20000, target=target, nav=first.sizing_nav)
    second.state.cohorts[..., F.ENTRY_COST].sum().backward()
    assert target.grad is not None and torch.isfinite(target.grad).all()
    assert target.grad.item() > 0


def test_continuation_budget_is_proportional_and_matches_paper_allocator():
    import numpy as np
    from stockagent.backtest.tw_integer_execution import _scale_lot_buys_to_budget_with_fixed_fees
    vector = lambda values: torch.tensor(values, dtype=torch.float64)
    common = dict(weights=vector([.6, -.4]), official_open=vector([1000., 1000.]),
                  lower_limit=vector([900., 900.]), upper_limit=vector([1100., 1100.]),
                  can_enter=vector([1., 1.]), buy_fee_rate=vector([.001425]*2),
                  day_sell_fee_rate=vector([.002925]*2), normal_sell_fee_rate=vector([.004425]*2),
                  rebate_rate=vector([.00114]*2), initial_capital=10_000_000., day=DAY)
    first = rebalance_inventory_at_open(DayTradeInventoryState.empty(2),
        entry_price=vector([1000.]*2), entry_volume_shares=vector([0.]*2), **common)
    continued = rebalance_inventory_at_open(first.state,
        entry_price=vector([1010.]*2), entry_volume_shares=vector([20000.]*2),
        frozen_target_shares=first.target_shares, frozen_sizing_nav=first.sizing_nav, **common)
    expected = _scale_lot_buys_to_budget_with_fixed_fees(
        np.array([6000,4000]), np.array([1010.,1010.]), np.array([1000,1000]),
        np.array([.001425-.00114, .002925-.00114]), budget=10_000_000.,
        minimum_commission=0., commission_rounding="none")
    np.testing.assert_array_equal(continued.state.shares.abs().numpy(), expected)
    assert expected.tolist() == [5000, 3000]


def sweep_session(*, volume=2000., exit_volume=0., day=DAY):
    from dataclasses import replace
    from test_tw_day_trade_carry import session
    base = session(day=day, price=100., volume=volume, exits=False)
    path = torch.zeros((1, 270, 2), dtype=torch.float64)
    path[..., 0] = 100.
    path[:, :3, 1] = volume
    capacity = base.exit_capacity.clone()
    capacity[:, -1] = exit_volume * .5
    prices = base.exit_prices.clone()
    prices[:, -1] = 100.
    return replace(base, entry_path=path, stop_hits=torch.zeros_like(path),
                   exit_capacity=capacity, exit_prices=prices)


def test_full_session_sweeps_three_minutes_and_carries_without_fake_close():
    from test_tw_day_trade_carry import run
    weights = torch.tensor([[.051]], dtype=torch.float64, requires_grad=True)
    result = run(weights, [sweep_session()])
    assert result.shares_history[0, 0].item() == 3000
    assert result.minute_nav.shape == (1, 270)
    assert result.minute_nav[0, 1] < result.minute_nav[0, 0]
    assert result.minute_nav[0, 2] < result.minute_nav[0, 1]
    assert result.final_state.inventory.cohorts.shape[0] == 1
    (-result.strategy_returns.sum()).backward()
    assert weights.grad is not None and torch.isfinite(weights.grad).all()


def test_stop_priority_cancels_new_entry_and_does_not_double_spend_capacity():
    from dataclasses import replace
    from test_tw_day_trade_carry import run
    base = sweep_session()
    stops = base.stop_hits.clone()
    stops[:, 1, 0] = 1
    result = run(torch.tensor([[.051]], dtype=torch.float64),
                 [replace(base, stop_hits=stops)])
    assert result.shares_history[0, 0].item() == 0
    # 1,000 shares enter at 09:01 and exit at 09:02; no re-entry at 09:03.
    assert result.turnovers[0].item() == pytest.approx(.02)


def test_stop_before_first_fill_does_not_latch_without_a_position():
    from dataclasses import replace
    from test_tw_day_trade_carry import run
    base = sweep_session(volume=0)
    path = base.entry_path.clone()
    path[:, 2, 1] = 2000
    stops = base.stop_hits.clone()
    stops[:, 1, 0] = 1
    result = run(torch.tensor([[.051]], dtype=torch.float64),
                 [replace(base, entry_path=path, stop_hits=stops)])
    assert result.shares_history[0, 0].item() == 1000


def test_old_cohort_stop_does_not_cancel_new_opposite_target():
    from dataclasses import replace
    from test_tw_day_trade_carry import run
    first = sweep_session(volume=6000.)
    second = sweep_session(volume=0., day=DAY+1)
    path = second.entry_path.clone()
    path[:, 1:3, 1] = 4000.
    hits = second.stop_hits.clone()
    hits[:, 1, 0] = 1
    second = replace(second, entry_path=path, stop_hits=hits)
    result = run(torch.tensor([[.031],[-.031]], dtype=torch.float64), [first, second])
    # 3,000 old long shares reduce 2,000 + 1,000, leaving one 1,000-share
    # bucket for the new short. The old long's stop is not the short's stop.
    assert result.shares_history[:, 0].tolist() == [3000., -1000.]


def test_already_held_target_does_not_reopen_after_old_cohort_exit():
    from dataclasses import replace
    from test_tw_day_trade_carry import run
    first = sweep_session(volume=6000.)
    second = sweep_session(volume=20000., day=DAY+1)
    hits = second.stop_hits.clone()
    hits[:, 1, 0] = 1
    second = replace(second, stop_hits=hits)
    result = run(torch.tensor([[.031],[.031]], dtype=torch.float64), [first, second])
    assert result.shares_history[:, 0].tolist() == [3000., 0.]


@pytest.mark.parametrize('volume', [2000., 20000.])
def test_certified_suffix_matches_chronological_values_and_gradients(monkeypatch, volume):
    from dataclasses import replace
    from test_tw_day_trade_carry import run
    first = sweep_session(volume=volume)
    second = sweep_session(volume=volume, day=DAY+1)
    hits = second.stop_hits.clone()
    hits[:, 50, 1] = 1
    path = second.entry_path.clone()
    path[:, 51, 1] = 4000.
    second = replace(second, stop_hits=hits, entry_path=path)
    sessions = [first, second]
    results, gradients = [], []
    for enabled in ['0', '1']:
        monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SWEEP_SUFFIX_FASTPATH', enabled)
        weights = torch.tensor([[.051],[-.041]], dtype=torch.float64, requires_grad=True)
        result = run(weights, sessions)
        (-result.strategy_returns.sum()).backward()
        results.append(result)
        gradients.append(weights.grad)
    torch.testing.assert_close(results[0].minute_nav, results[1].minute_nav, rtol=1e-12, atol=1e-7)
    torch.testing.assert_close(results[0].shares_history, results[1].shares_history, rtol=0, atol=0)
    torch.testing.assert_close(gradients[0], gradients[1], rtol=1e-10, atol=1e-12)


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("first_volume_shares", [0., 2000.])
@pytest.mark.parametrize("terminal_unlimited", [False, True])
def test_minute_sweep_matches_independent_paper_account(tmp_path, direction, first_volume_shares, terminal_unlimited):
    from dataclasses import replace
    from datetime import timedelta
    import numpy as np
    from scripts.rebuild_tw_day_trade_open_price_replay import _replay_historical_intraday
    from stockagent.data.tw_day_trade_schedule import paper_minute_opportunities
    from stockagent.live.tw_day_trade_simulation import (
        TwDayTradeSimulationEngine, ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
    )
    from test_day_trade_margin_carry import action_reference, register
    from test_tw_day_trade_simulation import _spec, _now
    from test_tw_day_trade_carry import session, run

    engine = TwDayTradeSimulationEngine(tmp_path / "paper")
    spec = replace(_spec(tmp_path), residual_margin_conversion=True,
                   terminal_liquidation_unlimited_capacity=terminal_unlimited,
                   entry_sweep_funding_policy="proportional_net_reservation_v1",
                   margin_corporate_action_reference_path=action_reference(tmp_path),
                   entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE)
    assert register(engine, spec, 0, direction * .401,
                    volume=first_volume_shares / 1000) == "registered"
    base = session(day=_now(9, 1).date().toordinal(), price=1000,
                   volume=first_volume_shares, exits=False)
    raw = np.tile([1000.,1000.,1000.,1000.,1000.,200.], (1,270,1))
    raw[0, :3, 5] = 2000.
    raw[0, 0, 5] = first_volume_shares
    raw[0, 1, :5] = 1005.
    raw[0, 2, :5] = 1010.
    schedule = paper_minute_opportunities(
        raw, trading_date=np.datetime64(_now(9, 1).date()),
        lower_limit=base.lower_limit.numpy(), upper_limit=base.upper_limit.numpy(),
        security_types=np.array(['stock']), latch_stops=False)
    bars = {"2330": {(_now(9, 1) + timedelta(minutes=i)).isoformat(timespec="minutes"):
                    dict(zip(("open","high","low","close","vwap","volume_shares"), raw[0, i]))
                    for i in range(270)}}
    terminal_quotes = lambda: {"2330": {"session_close_evidence": {
        "session_date": _now(9, 1).date().isoformat(), "price": 1005.,
        "source": "twse_official_daily_close", "source_sha256": "a" * 64,
        "price_basis": "official_session_close"}}}
    _replay_historical_intraday(engine, markets=[spec.market], bars=bars,
                                trading_date=_now(9, 1).date(),
                                terminal_close_quote_provider=terminal_quotes if terminal_unlimited else None)
    mode = engine.state["modes"][spec.market]
    candidate = replace(base, exit_prices=torch.from_numpy(schedule.prices),
                        exit_capacity=torch.from_numpy(schedule.capacity_shares),
                        marks=torch.from_numpy(schedule.marks),
                        entry_path=torch.from_numpy(raw[...,4:6].copy()),
                        terminal_liquidation_price=torch.tensor([1005.], dtype=torch.float64) if terminal_unlimited else None,
                        stop_hits=torch.from_numpy(schedule.stop_hits.astype(np.float64)))
    result = run(torch.tensor([[direction * .401]], dtype=torch.float64), [candidate])
    assert result.shares_history[-1].sum().item() == sum(
        int(p["signed_shares"]) for p in mode["positions"].values())
    assert result.final_state.last_nav.item() == pytest.approx(mode["total_equity_twd"], abs=1e-7)
    assert_paper_minute_nav(engine, spec.market, _now(9, 1), result.minute_nav[0])
    if terminal_unlimited:
        assert mode['open_position_count'] == 0
        assert result.shares_history[-1].sum().item() == 0
        assert mode.get('cumulative_carry_cost_twd', 0) == 0


def test_two_day_flip_matches_paper_fifo_interest_and_minute_capacity(tmp_path):
    from dataclasses import replace
    from datetime import timedelta
    import numpy as np
    from scripts.rebuild_tw_day_trade_open_price_replay import _replay_historical_intraday
    from stockagent.data.tw_day_trade_schedule import paper_minute_opportunities
    from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine, ENTRY_FILL_POLICY_0901_MINUTE_PRICE
    from test_day_trade_margin_carry import action_reference, register
    from test_tw_day_trade_simulation import _spec, _now
    from test_tw_day_trade_carry import session, run

    engine = TwDayTradeSimulationEngine(tmp_path / "paper")
    spec = replace(_spec(tmp_path), residual_margin_conversion=True,
                   entry_sweep_funding_policy="proportional_net_reservation_v1",
                   margin_corporate_action_reference_path=action_reference(tmp_path),
                   entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE)
    sessions = []
    for offset, weight in enumerate((.401, -.401)):
        day = _now(9, 1) + timedelta(days=offset)
        assert register(engine, spec, offset, weight, volume=2) == "registered"
        base = session(day=day.date().toordinal(), price=1000., volume=2000., exits=False)
        raw = np.tile([1000.,1000.,1000.,1000.,1000.,0.], (1,270,1))
        raw[0, :3 if offset == 0 else 6, 5] = 2000.
        schedule = paper_minute_opportunities(raw, trading_date=np.datetime64(day.date()),
            lower_limit=base.lower_limit.numpy(), upper_limit=base.upper_limit.numpy(),
            security_types=np.array(['stock']), latch_stops=False)
        bars = {"2330": {(day + timedelta(minutes=i)).isoformat(timespec="minutes"):
                         dict(zip(("open","high","low","close","vwap","volume_shares"), raw[0, i]))
                         for i in range(270)}}
        _replay_historical_intraday(engine, markets=[spec.market], bars=bars, trading_date=day.date())
        sessions.append(replace(base, exit_prices=torch.from_numpy(schedule.prices),
            exit_capacity=torch.from_numpy(schedule.capacity_shares), marks=torch.from_numpy(schedule.marks),
            entry_path=torch.from_numpy(raw[...,4:6].copy()),
            stop_hits=torch.from_numpy(schedule.stop_hits.astype(np.float64))))
    result = run(torch.tensor([[.401],[-.401]], dtype=torch.float64), sessions)
    mode = engine.state["modes"][spec.market]
    assert result.shares_history[:, 0].tolist() == [3000., -3000.]
    assert result.final_state.inventory.shares.item() == sum(int(p["signed_shares"]) for p in mode["positions"].values())
    assert result.final_state.last_nav.item() == pytest.approx(mode["total_equity_twd"], abs=1e-7)
    for offset, minute_nav in enumerate(result.minute_nav):
        assert_paper_minute_nav(engine, spec.market, _now(9, 1) + timedelta(days=offset), minute_nav)


def test_competing_long_short_remainders_match_paper_budget(tmp_path):
    from dataclasses import fields, replace
    from datetime import timedelta
    import numpy as np
    from scripts.rebuild_tw_day_trade_open_price_replay import _replay_historical_intraday
    from stockagent.data.tw_day_trade_schedule import paper_minute_opportunities
    from stockagent.live.tw_day_trade_simulation import (
        TwDayTradeSimulationEngine, ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE,
    )
    from test_day_trade_margin_carry import action_reference
    from test_tw_day_trade_simulation import _spec, _now, _row, _quote, _summary, _eligibility
    from test_tw_day_trade_carry import session, run

    symbols = ['2330', '2317']
    engine = TwDayTradeSimulationEngine(tmp_path / 'paper')
    spec = replace(_spec(tmp_path), residual_margin_conversion=True,
        entry_sweep_funding_policy='proportional_net_reservation_v1',
        margin_corporate_action_reference_path=action_reference(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE)
    (spec.parquet_root / 'symbols.csv').write_text(
        'code,name,market,security_type,source\n2330,A,twse,stock,official\n2317,B,twse,stock,official\n')
    quotes = {s: _quote(bid=1000., ask=1000., last=1000., minute_volume_lots=2.) | dict(
        symbol=s, execution_price_0901=1000., execution_price_0901_method='minute_close',
        quote_at=_now(9,1).isoformat()) for s in symbols}
    assert engine.register_signal(spec=spec,
        summary=_summary() | dict(generated_at=_now(9,0).isoformat(), simulation_replay=True,
            entry_fill_contract=REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE),
        signal_rows=[_row(.6), _row(-.4) | {'symbol':'2317'}], quotes=quotes,
        eligibility={s: replace(_eligibility()['2330'], symbol=s) for s in symbols},
        eligibility_coverage={}, now=_now(9,1), counterfactual_open_replay=True) == 'registered'
    raw = np.tile([1010.,1010.,1010.,1010.,1010.,200.], (2,270,1))
    raw[:,0,:5], raw[:,0,5], raw[:,1:3,5] = 1000., 2000., 20000.
    base = session(day=_now(9,1).date().toordinal(), price=1000., volume=2000., exits=False)
    base = replace(base, **{f.name: value.repeat(2, *([1]*(value.ndim-1)))
        for f in fields(base) if isinstance(value := getattr(base,f.name), torch.Tensor)})
    schedule = paper_minute_opportunities(raw, trading_date=np.datetime64(_now(9,1).date()),
        lower_limit=base.lower_limit.numpy(), upper_limit=base.upper_limit.numpy(),
        security_types=np.array(['stock']*2), latch_stops=False)
    bars = {s: {(_now(9,1)+timedelta(minutes=i)).isoformat(timespec='minutes'):
        dict(zip(('open','high','low','close','vwap','volume_shares'), raw[j,i]))
        for i in range(270)} for j,s in enumerate(symbols)}
    _replay_historical_intraday(engine, markets=[spec.market], bars=bars, trading_date=_now(9,1).date())
    candidate = replace(base, exit_prices=torch.from_numpy(schedule.prices),
        exit_capacity=torch.from_numpy(schedule.capacity_shares), marks=torch.from_numpy(schedule.marks),
        entry_path=torch.from_numpy(raw[...,4:6].copy()), stop_hits=torch.from_numpy(schedule.stop_hits.astype(np.float64)))
    result = run(torch.tensor([[.6,-.4]], dtype=torch.float64), [candidate])
    mode = engine.state['modes'][spec.market]
    for i,s in enumerate(symbols):
        assert result.shares_history[-1,i].item() == sum(
            p['signed_shares'] for p in mode['positions'].values() if p['symbol'] == s)
    assert result.final_state.last_nav.item() == pytest.approx(mode['total_equity_twd'], abs=1e-7)
    assert_paper_minute_nav(engine, spec.market, _now(9, 1), result.minute_nav[0])


def test_nested_rematerialization_preserves_cross_day_values_and_gradients(monkeypatch):
    import stockagent.backtest.tw_day_trade_carry as carry
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SWEEP_SUFFIX_FASTPATH', '0')
    from test_tw_day_trade_carry import run
    days = [sweep_session(volume=20000.), sweep_session(volume=20000., day=DAY+1)]
    reference_weights = torch.tensor([[.051],[-.041]], dtype=torch.float64, requires_grad=True)
    monkeypatch.setattr(carry, '_sweep_checkpoint_enabled', lambda _: False)
    reference = run(reference_weights, days)
    (-reference.strategy_returns.sum()).backward()
    checked_weights = reference_weights.detach().clone().requires_grad_()
    monkeypatch.setattr(carry, '_sweep_checkpoint_enabled', lambda _: True)
    checked = run(checked_weights, days)
    (-checked.strategy_returns.sum()).backward()
    torch.testing.assert_close(checked.minute_nav, reference.minute_nav, rtol=0, atol=0)
    torch.testing.assert_close(checked_weights.grad, reference_weights.grad, rtol=0, atol=0)


def test_compiled_step_claim_padding_is_bounded_and_preserves_ledger(monkeypatch):
    """Test the compiler adapter, not CUDA compilation, on the CPU oracle."""
    from dataclasses import replace
    import stockagent.backtest.tw_day_trade_carry as carry
    constructors, shapes = [], []
    def fake_compile(function, **kwargs):
        constructors.append(function)
        def call(state, *args):
            shapes.append(state[carry._INVENTORY_FIELDS.index('claims')].shape[0])
            return function(state, *args)
        return call
    monkeypatch.setattr(torch, 'compile', fake_compile)
    monkeypatch.setattr(carry, '_carry_path_compile_enabled', lambda _: True)
    monkeypatch.setattr(carry, '_COMPILED_SWEEP_STEPS', {})
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_SWEEP_COMPILE', '1')
    vector = lambda value: torch.tensor([value], dtype=torch.float64)
    for count in [1, 7, 17, 31, 32, 33]:
        opened = execute(DayTradeInventoryState.empty(1), volume=0.)
        claims = vector(0.).new_zeros((count, 1, 3))
        claims[:, 0, 0] = torch.arange(1, count+1, dtype=torch.float64) * 1.25
        claims[:, 0, 1] = DAY + 5
        claims.requires_grad_()
        state = replace(opened.state, claims=claims)
        weights = vector(.051)
        common = dict(official_open=vector(100.), opening_marks=vector(100.),
            lower_limit=vector(90.), upper_limit=vector(110.), can_enter=vector(1.),
            buy_fee_rate=vector(.001425), day_sell_fee_rate=vector(.002925),
            normal_sell_fee_rate=vector(.004425), rebate_rate=vector(.00114),
            initial_capital=10_000_000., day=state.observed_day, state_already_advanced=True)
        args = (tuple(getattr(state, name) for name in carry._INVENTORY_FIELDS),
            (weights, opened.target_shares, opened.sizing_nav, vector(0.).bool(),
             vector(0.).bool(), weights.new_tensor(True, dtype=torch.bool)),
            (torch.full((1,2), float('nan')), torch.zeros((1,2)),
             torch.tensor([[101.,20000.]], dtype=torch.float64),
             torch.zeros((1,2)), vector(101.)), common)
        candidate = carry._sweep_step_function(weights, state, initial_capital=10_000_000., entry_phase=True)(*args)
        reference = carry._flat_sweep_step(*args, initial_capital=10_000_000., entry_phase=True)
        for actual, expected in zip(candidate, reference, strict=True):
            torch.testing.assert_close(actual, expected, rtol=1e-12, atol=1e-8)
        torch.testing.assert_close(torch.autograd.grad(candidate[-2], claims)[0],
                                   torch.autograd.grad(reference[-2], claims)[0], rtol=0, atol=0)
        assert candidate[carry._INVENTORY_FIELDS.index('claims')].shape[0] == count
    assert shapes == [32, 32, 32, 32, 32, 64]
    assert len(constructors) == 2
