from dataclasses import fields, replace

import pytest
import torch

from stockagent.backtest.tw_day_trade_graph import _Program
from stockagent.backtest.tw_day_trade_parallel import (
    ParallelDayAdjointEngine, affine_day_adjoint)
from stockagent.training.return_adjoint import trajectory_metric_loss
from test_tw_day_trade_graph import _inputs


OBJECTIVES = ['log_utility', 'trajectory_sharpe', 'trajectory_return',
              'trajectory_sortino', 'trajectory_mdd']


@pytest.mark.parametrize('initial_cost', [0., 2345.])
def test_recorded_canonical_states_and_parallel_local_vjp_preserve_general_state_adjoints(initial_cost):
    w, sessions, program, values, _ = _inputs(rows=8)
    program.event_compression = True
    # Corporate-action cursors and disabled entry must come from the exact
    # recorded state, rather than an independently guessed calendar/mask.
    sessions = tuple(replace(s, action_mask=torch.tensor([float(i == 2), 0.], dtype=torch.float64))
                     for i, s in enumerate(sessions))
    program = _Program(sessions[0], 8, 2, 10_000_000., event_compression=True)
    source = tuple(torch.stack(tuple(getattr(s, n) for s in sessions)) for n in program.names)
    enter = values[1].clone()
    enter[::2, 0] = False
    pnl = torch.tensor(123456., dtype=torch.float64, requires_grad=True)
    cost = torch.tensor(initial_cost, dtype=torch.float64, requires_grad=True)
    nav = torch.tensor(10_000_000.+123456.-initial_cost+777., dtype=torch.float64, requires_grad=True)
    values = (w, enter, *values[2:6], pnl, cost, *values[8:12], nav, *values[13:16], *source)
    with torch.no_grad():
        recorded, trace = program(values, record_states=True)
    reference = program(values)
    assert bool(recorded[-1])
    for actual, expected in zip(recorded, reference):
        torch.testing.assert_close(actual, expected, rtol=0, atol=0, equal_nan=True)
    entering = tuple(t.detach().clone() for t in trace)
    entering[0].requires_grad_(True)
    engine = object.__new__(ParallelDayAdjointEngine)
    engine.program = program
    maps = torch.compile(engine._day_maps, fullgraph=True, backend='eager')
    after_pnl, after_cost, after_nav, turnover, proof = maps(values, entering)
    assert bool(proof.all())
    torch.testing.assert_close(after_pnl, torch.cat((trace[0][1:], recorded[8][None])), rtol=0, atol=0)
    torch.testing.assert_close(after_cost, trace[1], rtol=0, atol=0)
    torch.testing.assert_close(turnover, recorded[1], rtol=0, atol=0)
    a, jacobian = torch.autograd.grad(after_pnl.sum(), (entering[0], w))
    for objective in [*OBJECTIVES, 'arbitrary_vjp']:
        # Fused canonical day maps may donate their saved buffers. Each
        # independent reference loss therefore gets its own one-use tape,
        # exactly as a production optimizer pass does.
        reference = program(values)
        if objective == 'arbitrary_vjp':
            beta = torch.tensor([.3, -.1, .17, -.4, .08, .12, -.11, .25], dtype=torch.float64)
            loss = (reference[0]*beta).sum()
        else:
            leaf = recorded[0].detach().requires_grad_()
            beta, = torch.autograd.grad(trajectory_metric_loss(leaf, objective), leaf)
            loss = trajectory_metric_loss(reference[0], objective)
        gp, gc, gv = (nav.new_tensor(v) for v in (3e-7, -2e-7, 1e-7))
        expected = torch.autograd.grad(loss+gp*reference[8]+gc*reference[9]+gv*reference[14],
                                      (w, pnl, cost, nav))
        actual, valid = affine_day_adjoint(beta, gp, gc, gv, a, jacobian, after_nav.detach(), nav.detach())
        assert bool(valid)
        for candidate, wanted in zip(actual, expected):
            torch.testing.assert_close(candidate, wanted, rtol=1e-5, atol=1e-8)


def test_parallel_adjoint_checkpoint_schedule_is_explicit_and_disabled_compatible(monkeypatch):
    from pathlib import Path
    import os
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import (
        _configuration_fingerprint_snapshot, _training_checkpoint_contract)
    config = load_config(Path(os.environ.get('STOCKAGENT_TEST_CONFIG_ROOT', 'configs'))/'experiment_baseline.yaml')
    config.trading.execution_mode = 'tw_day_trade'
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH', '1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '0')
    old = (_configuration_fingerprint_snapshot(config), _training_checkpoint_contract(config))
    for snapshot in old:
        contract = snapshot.get('training', snapshot)['physical_fifo_explicit_graph_contract']
        assert contract['positive_prefix'] == 'opaque_int64_nonpositive_prefix_v1'
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '1')
    new = (_configuration_fingerprint_snapshot(config), _training_checkpoint_contract(config))
    assert all(a != b for a, b in zip(old, new))
    for snapshot in new:
        contract = snapshot.get('training', snapshot)['physical_fifo_explicit_graph_contract']
        assert contract['parallel_state_adjoint'] == 'canonical_serial_forward_parallel_day_vjp_affine_prefix_v1'
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '2')
    fixed = (_configuration_fingerprint_snapshot(config), _training_checkpoint_contract(config))
    assert all(a != b for a, b in zip(new, fixed))
    for snapshot in fixed:
        contract = snapshot.get('training', snapshot)['physical_fifo_explicit_graph_contract']
        assert contract['parallel_state_adjoint'] == 'canonical_parallel_flat_funding_certificate_affine_v2'
        assert contract['parallel_forward_iterations'] == 4
        assert contract['parallel_forward_candidate'] == 'canonical_pnl_only_day_maps_v1'
        assert contract['parallel_funding_certificate'] == 'unfunded_target_and_funded_quantities_interval_v1'
        assert contract['parallel_financial_interval'] == 'solvency_and_unclamped_log_return_interval_v1'
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '0')
    assert old == (_configuration_fingerprint_snapshot(config), _training_checkpoint_contract(config))
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '2')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PREWARM_CANONICAL_FALLBACK', '1')
    prewarmed = (_configuration_fingerprint_snapshot(config), _training_checkpoint_contract(config))
    assert all(a != b for a, b in zip(fixed, prewarmed))
    for snapshot in prewarmed:
        contract = snapshot.get('training', snapshot)['physical_fifo_explicit_graph_contract']
        assert contract['canonical_fallback_warmup'] == 'forward_and_vjp_before_parallel_replay_v1'


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA graph runtime')
@pytest.mark.parametrize('sparse', [False, True])
@pytest.mark.parametrize('block_rows', [4, 16])
@pytest.mark.parametrize('mode', ['1', '2'])
def test_parallel_graph_full_state_pending_vjp_and_all_independent_objectives(monkeypatch, sparse, block_rows, mode):
    from stockagent.backtest.tw_day_trade_carry import (
        DayTradeCarryState, compact_day_trade_carry_session, run_day_trade_carry_sessions)
    from stockagent.backtest.tw_day_trade_graph import (
        _ENGINES, get_explicit_carry_graph_stats)
    from stockagent.training.day_trade_carry_bridge import PreparedDayTradeCarryBatch
    settings = dict(STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH='1', STOCKAGENT_DAY_TRADE_CUDAGRAPHS='0',
        STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP='1', STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS=str(block_rows),
        STOCKAGENT_DAY_TRADE_PREWARM_CANONICAL_FALLBACK='1',
        STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT=mode, STOCKAGENT_DAY_TRADE_PREFIX_BACKEND='triton',
        STOCKAGENT_DAY_TRADE_GRAPH_PACKED_COPY='triton', STOCKAGENT_DAY_TRADE_SPARSE_EVENT_SLOTS='16',
        STOCKAGENT_DAY_TRADE_SPARSE_EVENTS='1' if sparse else '0',
        STOCKAGENT_DAY_TRADE_SPARSE_COMPACT_TRANSPORT='1' if sparse else '0',
        STOCKAGENT_DAY_TRADE_SPARSE_CAPACITY_BUCKETS='1' if sparse else '0')
    for name, value in settings.items():
        monkeypatch.setenv(name, value)
    _ENGINES.clear()
    w, sessions, _, _, common = _inputs(rows=5, device='cuda')
    common['event_compression'] = True
    if sparse:
        cpu = tuple(replace(s, **{f.name: getattr(s, f.name).cpu() for f in fields(s)
            if isinstance(getattr(s, f.name), torch.Tensor)}) for s in sessions)
        packet = PreparedDayTradeCarryBatch.from_sessions(
            tuple(compact_day_trade_carry_session(s) for s in cpu), 5,
            event_compression=True, sparse_events=True)
        sessions = packet.sessions(w.device)
    pnl = torch.tensor(123456., device=w.device, dtype=torch.float64, requires_grad=True)
    cost = torch.tensor(2345., device=w.device, dtype=torch.float64, requires_grad=True)
    nav = torch.tensor(10_000_000.+123456.-2345.+777., device=w.device, dtype=torch.float64, requires_grad=True)
    state = DayTradeCarryState.empty(2, 10_000_000., w.device)
    state = replace(state, inventory=replace(state.inventory, realized_net_pnl=pnl, carry_cost=cost), last_nav=nav)
    common['initial_state'] = state
    reference = run_day_trade_carry_sessions(w, sessions, **common, _allow_flat_terminal=False)
    actual = run_day_trade_carry_sessions(w, sessions, **common)
    for name in ['strategy_returns', 'turnovers', 'shares_history', 'minute_nav', 'weights_history']:
        torch.testing.assert_close(getattr(actual, name), getattr(reference, name), rtol=1e-10, atol=1e-6)
    other_w = (w.detach()*.73).requires_grad_()
    other_reference = run_day_trade_carry_sessions(other_w, sessions, **common, _allow_flat_terminal=False)
    other = run_day_trade_carry_sessions(other_w, sessions, **common)
    with torch.inference_mode():
        inference = run_day_trade_carry_sessions(w.detach(), sessions, **common)
        torch.testing.assert_close(inference.strategy_returns, reference.strategy_returns, rtol=1e-10, atol=1e-9)
    before = get_explicit_carry_graph_stats().copy()
    for objective in OBJECTIVES:
        def scalar(result):
            return (trajectory_metric_loss(result.strategy_returns, objective)
                + 3e-7*result.final_state.inventory.realized_net_pnl
                - 2e-7*result.final_state.inventory.carry_cost+1e-7*result.final_state.last_nav)
        reference = run_day_trade_carry_sessions(w, sessions, **common, _allow_flat_terminal=False)
        expected = torch.autograd.grad(scalar(reference), (w, pnl, cost, nav))
        observed = torch.autograd.grad(scalar(actual), (w, pnl, cost, nav), retain_graph=True)
        for candidate, wanted in zip(observed, expected):
            torch.testing.assert_close(candidate, wanted, rtol=1e-5, atol=1e-8)
    gradient, = torch.autograd.grad(other.strategy_returns.sum(), other_w)
    reference_gradient, = torch.autograd.grad(other_reference.strategy_returns.sum(), other_w)
    torch.testing.assert_close(gradient, reference_gradient, rtol=1e-5, atol=1e-8)
    assert torch.isfinite(gradient).all() and gradient.abs().sum() > 0
    after = get_explicit_carry_graph_stats()
    assert after['parallel_adjoint_replays'] > before['parallel_adjoint_replays']
    assert after['parallel_adjoint_fallbacks'] == before['parallel_adjoint_fallbacks']
    # A real turnover penalty has its complete canonical gradient, rather than
    # silently taking the return-only affine adjoint.
    reference = run_day_trade_carry_sessions(w, sessions, **common, _allow_flat_terminal=False)
    expected, = torch.autograd.grad(reference.strategy_returns.sum()+reference.turnovers.sum()*.01, w)
    from torch._dynamo.utils import counters
    compiled_before_fallback = counters['stats']['unique_graphs']
    observed, = torch.autograd.grad(actual.strategy_returns.sum()+actual.turnovers.sum()*.01, w)
    torch.testing.assert_close(observed, expected, rtol=1e-5, atol=1e-8)
    assert get_explicit_carry_graph_stats()['parallel_adjoint_fallbacks'] > after['parallel_adjoint_fallbacks']
    assert counters['stats']['unique_graphs'] == compiled_before_fallback
    assert get_explicit_carry_graph_stats()['canonical_fallback_prewarms'] > 0


def _cpu_fixed_engine(values, program):
    from stockagent.backtest.tw_day_trade_parallel import ParallelFlatForwardEngine
    engine = object.__new__(ParallelFlatForwardEngine)
    engine.program, engine.inputs, engine.device = program, values, torch.device('cpu')
    engine.maps = torch.compile(engine._maps, fullgraph=True, backend='eager')
    engine.balance_maps = torch.compile(engine._balance_maps, fullgraph=True, backend='eager')
    engine.opening_maps = torch.compile(engine._opening_maps, fullgraph=True, backend='eager')
    return engine


def test_certified_parallel_forward_uses_consistent_whole_lots_and_general_state_vjp():
    w, sessions, program, values, _ = _inputs(rows=8)
    program.event_compression = True
    pnl = torch.tensor(123456., dtype=torch.float64, requires_grad=True)
    cost = torch.tensor(2345., dtype=torch.float64, requires_grad=True)
    nav = torch.tensor(10_000_000.+123456.-2345.+777., dtype=torch.float64, requires_grad=True)
    values = (w, *values[1:6], pnl, cost, *values[8:12], nav, *values[13:])
    engine = _cpu_fixed_engine(values, program)
    actual, a, jacobian, after_nav, residual = engine._proposal(True)
    assert bool(actual[-1]) and float(residual) <= 1e-6
    reference = program(values)
    for candidate, expected in zip(actual, reference):
        torch.testing.assert_close(candidate, expected, rtol=1e-10, atol=1e-6, equal_nan=True)
    gp, gc, gv = (nav.new_tensor(v) for v in (3e-7, -2e-7, 1e-7))
    for objective in OBJECTIVES:
        reference = program(values)
        leaf = actual[0].detach().requires_grad_()
        beta, = torch.autograd.grad(trajectory_metric_loss(leaf, objective), leaf)
        observed, valid = affine_day_adjoint(beta, gp, gc, gv, a, jacobian, after_nav, nav.detach())
        expected = torch.autograd.grad(trajectory_metric_loss(reference[0], objective)
            +gp*reference[8]+gc*reference[9]+gv*reference[14], (w, pnl, cost, nav))
        assert bool(valid)
        for candidate, wanted in zip(observed, expected):
            torch.testing.assert_close(candidate, wanted, rtol=1e-5, atol=1e-8)


def _unconverged_inputs(device='cpu'):
    w, original, _, values, common = _inputs(rows=8, device=device)
    sessions = tuple(replace(s, official_open=torch.full_like(s.official_open, 100.),
        opening_marks=torch.full_like(s.opening_marks, 100.), entry_price=torch.full_like(s.entry_price, 100.),
        entry_volume=torch.full_like(s.entry_volume, 1e12), lower_limit=torch.full_like(s.lower_limit, 50.),
        upper_limit=torch.full_like(s.upper_limit, 300.),
        marks=torch.full_like(s.marks, 150.),
        exit_prices=torch.where(torch.isfinite(s.exit_prices), 200., s.exit_prices),
        exit_capacity=torch.where(s.exit_capacity > 0,
            torch.full_like(s.exit_capacity, 1e12), torch.zeros_like(s.exit_capacity)),
        terminal_liquidation_price=torch.full_like(s.terminal_liquidation_price, 200.)) for s in original)
    w = w.detach().clone()
    w[:, 0] = .9
    w[:, 1] = 0.
    w.requires_grad_(True)
    program = _Program(sessions[0], 8, 2, 10_000_000., event_compression=True)
    fees = tuple(torch.zeros_like(f) for f in values[2:6])
    values = (w, values[1], *fees, *values[6:16],
        *(torch.stack(tuple(getattr(s, n) for s in sessions)) for n in program.names))
    common.update(buy_fee_rate=fees[0], day_sell_fee_rate=fees[1], normal_sell_fee_rate=fees[2],
                  rebate_rate=fees[3], event_compression=True)
    return w, sessions, program, values, common


def test_parallel_funding_certificate_rejects_a_nonconverged_lot_path():
    _, _, program, values, _ = _unconverged_inputs()
    candidate = _cpu_fixed_engine(values, program)._proposal(True)
    assert not bool(candidate[0][-1])
    assert float(candidate[4]) > 1e-6


def test_parallel_certificate_rejects_a_lot_boundary_even_with_zero_monetary_residual():
    w, sessions, _, values, _ = _unconverged_inputs()
    w = torch.full_like(w, .5)
    w[:, 1] = 0.
    w.requires_grad_(True)
    sessions = tuple(replace(s, exit_prices=torch.where(torch.isfinite(s.exit_prices), 100., s.exit_prices),
        marks=torch.full_like(s.marks, 100.), terminal_liquidation_price=torch.full_like(s.terminal_liquidation_price, 100.))
        for s in sessions)
    program = _Program(sessions[0], 8, 2, 10_000_000., event_compression=True)
    values = (w, *values[1:16],
        *(torch.stack(tuple(getattr(s, n) for s in sessions)) for n in program.names))
    candidate = _cpu_fixed_engine(values, program)._proposal(True)
    assert float(candidate[4]) == 0.
    assert not bool(candidate[0][-1])


def _capacity_limited_boundary_inputs(device='cpu'):
    w, sessions, _, values, common = _unconverged_inputs(device)
    w = torch.full_like(w, .5)
    w[:, 1] = 0.
    w.requires_grad_(True)
    sessions = tuple(replace(s, entry_volume=torch.full_like(s.entry_volume, 2000.),
        exit_prices=torch.where(torch.isfinite(s.exit_prices), 100., s.exit_prices),
        marks=torch.full_like(s.marks, 100.),
        terminal_liquidation_price=torch.full_like(s.terminal_liquidation_price, 100.))
        for s in sessions)
    program = _Program(sessions[0], 8, 2, 10_000_000., event_compression=True)
    values = (w, *values[1:16],
        *(torch.stack(tuple(getattr(s, n) for s in sessions)) for n in program.names))
    return w, sessions, program, values, common


def test_parallel_funding_signature_rejects_unfunded_boundary_hidden_by_capacity():
    _, _, program, values, _ = _capacity_limited_boundary_inputs()
    engine = _cpu_fixed_engine(values, program)
    candidate = engine._proposal(True)
    before = values[6].expand(8)
    left = engine.opening_maps(values, engine._trace(before-8e-6))
    right = engine.opening_maps(values, engine._trace(before+8e-6))
    torch.testing.assert_close(left[0], right[0], rtol=0, atol=0)
    torch.testing.assert_close(left[1], right[1], rtol=0, atol=0)
    assert (left[0][:, 0] == 1000).all()
    assert (left[2][:, 0] == 49000).all() and (right[2][:, 0] == 50000).all()
    assert float(candidate[4]) == 0.
    assert not bool(candidate[0][-1])


def _near_ruin_interval_inputs(device='cpu'):
    from stockagent.backtest.tw_day_trade_carry import DayTradeCarryState
    w, sessions, _, values, common = _unconverged_inputs(device)
    w = torch.zeros_like(w)
    w[:, 0] = -.50003
    w.requires_grad_(True)
    sessions = tuple(replace(s, exit_prices=torch.where(torch.isfinite(s.exit_prices), 100., s.exit_prices),
        marks=torch.full_like(s.marks, 300.),
        terminal_liquidation_price=torch.full_like(s.terminal_liquidation_price, 100.))
        for s in sessions)
    program = _Program(sessions[0], 8, 2, 10_000_000., event_compression=True)
    pnl = values[6].new_tensor(5e-6, requires_grad=True)
    nav = values[12].new_tensor(10_000_000.+5e-6, requires_grad=True)
    values = (w, *values[1:6], pnl, *values[7:12], nav, *values[13:16],
        *(torch.stack(tuple(getattr(s, n) for s in sessions)) for n in program.names))
    state = DayTradeCarryState.empty(2, 10_000_000., w.device)
    common['initial_state'] = replace(state, inventory=replace(state.inventory, realized_net_pnl=pnl), last_nav=nav)
    return w, sessions, program, values, common


def test_parallel_certificate_protects_intraday_solvency_across_the_money_error_interval():
    _, _, program, values, _ = _near_ruin_interval_inputs()
    canonical = program(values)
    assert bool(canonical[-1]) and (canonical[4] > 0).all()
    assert float(canonical[4].detach().min()) < 8e-6
    candidate = _cpu_fixed_engine(values, program)._proposal(True)
    assert float(candidate[4]) == 0.
    assert not bool(candidate[0][-1])
    lower_values = (*values[:6], values[6]-8e-6, *values[7:])
    assert not bool(program(lower_values)[-1])


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA graph runtime')
def test_graph_full_state_capacity_limited_target_boundary_uses_canonical_fallback(monkeypatch):
    from stockagent.backtest.tw_day_trade_carry import run_day_trade_carry_sessions
    from stockagent.backtest.tw_day_trade_graph import _ENGINES, get_explicit_carry_graph_stats
    for key, value in dict(STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH='1', STOCKAGENT_DAY_TRADE_CUDAGRAPHS='0',
            STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP='1', STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS='16',
            STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT='2', STOCKAGENT_DAY_TRADE_SPARSE_EVENTS='0').items():
        monkeypatch.setenv(key, value)
    _ENGINES.clear()
    for inputs in [_capacity_limited_boundary_inputs, _near_ruin_interval_inputs]:
        w, sessions, _, _, common = inputs('cuda')
        expected = run_day_trade_carry_sessions(w, sessions, **common, _allow_flat_terminal=False)
        before = get_explicit_carry_graph_stats()
        actual = run_day_trade_carry_sessions(w, sessions, **common)
        torch.testing.assert_close(actual.strategy_returns, expected.strategy_returns, rtol=0, atol=0)
        wanted, = torch.autograd.grad(expected.strategy_returns.sum(), w)
        observed, = torch.autograd.grad(actual.strategy_returns.sum(), w)
        torch.testing.assert_close(observed, wanted, rtol=1e-5, atol=1e-8)
        after = get_explicit_carry_graph_stats()
        assert after['parallel_flat_forward_fallbacks'] > before['parallel_flat_forward_fallbacks']
        assert after['parallel_adjoint_fallbacks'] > before['parallel_adjoint_fallbacks']


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA graph runtime')
def test_graph_full_state_rejected_fixed_point_executes_canonical_forward_and_backward(monkeypatch):
    from stockagent.backtest.tw_day_trade_carry import run_day_trade_carry_sessions
    from stockagent.backtest.tw_day_trade_graph import _ENGINES, get_explicit_carry_graph_stats
    for key, value in dict(STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH='1', STOCKAGENT_DAY_TRADE_CUDAGRAPHS='0',
            STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP='1', STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS='16',
            STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT='2', STOCKAGENT_DAY_TRADE_SPARSE_EVENTS='0').items():
        monkeypatch.setenv(key, value)
    _ENGINES.clear()
    w, sessions, _, _, common = _unconverged_inputs('cuda')
    expected = run_day_trade_carry_sessions(w, sessions, **common, _allow_flat_terminal=False)
    before = get_explicit_carry_graph_stats()
    actual = run_day_trade_carry_sessions(w, sessions, **common)
    torch.testing.assert_close(actual.strategy_returns, expected.strategy_returns, rtol=0, atol=0)
    wanted, = torch.autograd.grad(expected.strategy_returns.sum(), w)
    observed, = torch.autograd.grad(actual.strategy_returns.sum(), w)
    torch.testing.assert_close(observed, wanted, rtol=1e-5, atol=1e-8)
    assert get_explicit_carry_graph_stats()['parallel_flat_forward_fallbacks'] > before['parallel_flat_forward_fallbacks']
