from concurrent.futures import ThreadPoolExecutor
import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import run_day_trade_carry_sessions
from stockagent.backtest.tw_day_trade_graph import (
    DeferredLedgerValidation, DeferredLedgerValidationError,
    current_ledger_validation_gate, get_explicit_carry_graph_stats, ledger_predicate)
from stockagent.backtest.tw_day_trade_parallel import affine_day_adjoint
from stockagent.training.return_adjoint import trajectory_metric_loss
from test_tw_day_trade_graph import _inputs
from test_tw_day_trade_parallel import OBJECTIVES, _cpu_fixed_engine


@pytest.mark.parametrize('tail', [0, 2])
def test_recorded_candidate_preserves_all_account_adjoints_without_proposal_iterations(tail):
    w, _, program, values, _ = _inputs(rows=8, tail=tail)
    if tail:
        # Match _Engine.copy: padding is scheduling only, with zero actions
        # and entry permissions. It contributes no recorded physical date.
        with torch.no_grad():
            w[-tail:].zero_()
            values[1][-tail:].zero_()
    program.event_compression = True
    pnl = torch.tensor(123456., dtype=torch.float64, requires_grad=True)
    cost = torch.tensor(2345., dtype=torch.float64, requires_grad=True)
    nav = torch.tensor(10_000_000.+123456.-2345.+777., dtype=torch.float64, requires_grad=True)
    values = (w, *values[1:6], pnl, cost, *values[8:12], nav, *values[13:])
    with torch.no_grad():
        recorded = program(values)[0].detach().masked_fill(~values[15], 0.).clone()
    engine = _cpu_fixed_engine(values, program)
    engine.has_return_candidate = True
    engine.return_candidate = recorded

    def unexpected_iteration(*args, **kwargs):
        raise AssertionError('recorded replay must not repeat the four proposal maps')

    engine.balance_maps = unexpected_iteration
    actual, a, jacobian, after_nav, residual = engine._proposal(True)
    assert bool(actual[-1]) and float(residual) <= 1e-6
    reference = program(values)
    for observed, expected in zip(actual, reference):
        torch.testing.assert_close(observed, expected, rtol=1e-10, atol=1e-6, equal_nan=True)
    gp, gc, gv = (nav.new_tensor(v) for v in (3e-7, -2e-7, 1e-7))
    for objective in OBJECTIVES:
        reference = program(values)
        leaf = actual[0][:8-tail].detach().requires_grad_()
        beta, = torch.autograd.grad(trajectory_metric_loss(leaf, objective), leaf)
        if tail:
            beta = torch.cat((beta, beta.new_zeros(tail)))
        observed, valid = affine_day_adjoint(beta, gp, gc, gv, a, jacobian, after_nav, nav.detach())
        expected = torch.autograd.grad(trajectory_metric_loss(reference[0][:8-tail], objective)
            +gp*reference[8]+gc*reference[9]+gv*reference[14], (w, pnl, cost, nav))
        assert bool(valid)
        for candidate, wanted in zip(observed, expected):
            torch.testing.assert_close(candidate, wanted, rtol=1e-5, atol=1e-8)


@pytest.mark.parametrize('value', [0., float('nan'), 1000.])
def test_untrusted_recorded_candidates_cannot_bypass_full_financial_certificate(value):
    _, _, program, values, _ = _inputs(rows=8)
    program.event_compression = True
    engine = _cpu_fixed_engine(values, program)
    engine.has_return_candidate = True
    engine.return_candidate = torch.full((8,), value, dtype=torch.float64)
    candidate = engine._proposal(True)
    assert not bool(candidate[0][-1])


@pytest.mark.parametrize('shape,dtype', [((2,), torch.float64), ((3, 1), torch.float64),
                                        ((3,), torch.int64)])
def test_recorded_candidate_requires_exact_physical_rows(shape, dtype):
    w, sessions, _, _, common = _inputs()
    with pytest.raises(ValueError, match='align'):
        run_day_trade_carry_sessions(w, sessions, **common,
                                     replay_log_returns=torch.zeros(shape, dtype=dtype))


@pytest.mark.parametrize('device', ['cpu', 'cuda'])
def test_forward_owned_gate_snapshots_producer_stream_and_survives_an_autograd_thread(device):
    if device == 'cuda' and not torch.cuda.is_available():
        pytest.skip('requires CUDA')
    device = (torch.device('cuda', torch.cuda.current_device())
              if device == 'cuda' else torch.device(device))
    with pytest.raises(DeferredLedgerValidationError, match='worker_original_false'):
        with DeferredLedgerValidation(device) as gate:
            def worker():
                assert current_ledger_validation_gate() is None
                if device.type == 'cuda':
                    torch.cuda.set_device(device)
                    stream = torch.cuda.Stream(device=device)
                    with torch.cuda.stream(stream):
                        flag = torch.zeros((), dtype=torch.bool, device=device)
                        assert ledger_predicate(flag, 'worker_original_false', gate=gate)
                        flag.fill_(True)
                else:
                    flag = torch.tensor(False)
                    assert ledger_predicate(flag, 'worker_original_false', gate=gate)
                    flag.fill_(True)
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(worker).result(timeout=30)
            assert len(gate.predicates) == 1
            gate.finish()


def test_recorded_and_deferred_parallel_schedules_change_checkpoint_contract(monkeypatch):
    from stockagent.backtest.tw_day_trade_graph import explicit_carry_graph_contract
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT', '2')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_RECORDED_RETURN_CANDIDATE', '0')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_DEFER_PARALLEL_FORWARD_CERTIFICATE', '0')
    control = explicit_carry_graph_contract()
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_RECORDED_RETURN_CANDIDATE', '1')
    candidate = explicit_carry_graph_contract()
    assert candidate != control and candidate['parallel_replay_candidate'].endswith('_v1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_DEFER_PARALLEL_FORWARD_CERTIFICATE', '1')
    deferred = explicit_carry_graph_contract()
    assert deferred != candidate and deferred['parallel_forward_validation'].endswith('_v1')
    monkeypatch.setenv('STOCKAGENT_DAY_TRADE_RECORDED_RETURN_CANDIDATE', 'typo')
    with pytest.raises(ValueError, match='must be 0 or 1'):
        explicit_carry_graph_contract()


def _gpu_settings(monkeypatch):
    for name, value in dict(STOCKAGENT_DAY_TRADE_EXPLICIT_GRAPH='1',
            STOCKAGENT_DAY_TRADE_CUDAGRAPHS='0', STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP='1',
            STOCKAGENT_DAY_TRADE_GRAPH_BLOCK_ROWS='4', STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT='2',
            STOCKAGENT_DAY_TRADE_RECORDED_RETURN_CANDIDATE='1',
            STOCKAGENT_DAY_TRADE_DEFER_PARALLEL_FORWARD_CERTIFICATE='1',
            STOCKAGENT_DAY_TRADE_SPARSE_EVENTS='0').items():
        monkeypatch.setenv(name, value)


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA graphs')
def test_recorded_graph_tail_pending_tapes_and_candidate_reset_preserve_full_state_vjp(monkeypatch):
    from stockagent.backtest.tw_day_trade_graph import _ENGINES
    _gpu_settings(monkeypatch)
    _ENGINES.clear()
    w, sessions, _, _, common = _inputs(rows=5, device='cuda')
    common['event_compression'] = True
    reference = run_day_trade_carry_sessions(w, sessions, **common, _allow_flat_terminal=False)
    recorded = reference.strategy_returns.detach().clone()
    first = run_day_trade_carry_sessions(w, sessions, **common, replay_log_returns=recorded)
    other_w = (w.detach()*.73).requires_grad_()
    # A stale candidate must use canonical fallback, without corrupting the
    # pending first tape. Then omitting a candidate must reset the same owner.
    second = run_day_trade_carry_sessions(other_w, sessions, **common,
                                         replay_log_returns=recorded*0.)
    third = run_day_trade_carry_sessions(w, sessions, **common)
    for weights, actual in [(w, first), (other_w, second), (w, third)]:
        for objective in OBJECTIVES:
            expected = run_day_trade_carry_sessions(weights, sessions, **common,
                                                   _allow_flat_terminal=False)
            for name in ['strategy_returns', 'turnovers', 'shares_history', 'minute_nav']:
                torch.testing.assert_close(getattr(actual, name), getattr(expected, name),
                                           rtol=1e-10, atol=1e-6)
            loss = trajectory_metric_loss(actual.strategy_returns, objective)
            wanted = trajectory_metric_loss(expected.strategy_returns, objective)
            observed_grad, = torch.autograd.grad(loss, weights, retain_graph=True)
            expected_grad, = torch.autograd.grad(wanted, weights)
            torch.testing.assert_close(observed_grad, expected_grad, rtol=1e-5, atol=1e-8)
    with torch.inference_mode():
        actual = run_day_trade_carry_sessions(w.detach(), sessions, **common,
                                             replay_log_returns=recorded)
        torch.testing.assert_close(actual.strategy_returns, recorded, rtol=0, atol=1e-9)
    stats = get_explicit_carry_graph_stats()
    assert stats['recorded_return_candidate_replays'] > 0
    assert stats['recorded_return_candidate_fallbacks'] > 0


@pytest.mark.skipif(not torch.cuda.is_available(), reason='requires CUDA graphs')
def test_real_rejected_parallel_candidate_is_observed_before_update_then_retried_canonically(monkeypatch):
    from stockagent.backtest.tw_day_trade_graph import _ENGINES
    _gpu_settings(monkeypatch)
    _ENGINES.clear()
    w, sessions, _, _, common = _inputs(rows=5, device='cuda')
    common['event_compression'] = True
    bad_candidate = torch.zeros(5, dtype=torch.float64, device=w.device)
    before = get_explicit_carry_graph_stats()
    with pytest.raises(DeferredLedgerValidationError, match='before update'):
        with DeferredLedgerValidation(w.device) as gate:
            rejected = run_day_trade_carry_sessions(w, sessions, **common,
                                                   replay_log_returns=bad_candidate)
            torch.autograd.grad(rejected.strategy_returns.sum(), w)
            assert any('affine_adjoint' in label for label in gate.labels)
            gate.finish()
    after = get_explicit_carry_graph_stats()
    assert after['parallel_flat_deferred_certificates'] > before['parallel_flat_deferred_certificates']
    assert after['parallel_flat_forward_fallbacks'] == before['parallel_flat_forward_fallbacks']
    actual = run_day_trade_carry_sessions(w, sessions, **common, replay_log_returns=bad_candidate)
    expected = run_day_trade_carry_sessions(w, sessions, **common, _allow_flat_terminal=False)
    torch.testing.assert_close(actual.strategy_returns, expected.strategy_returns, rtol=0, atol=0)
    got, = torch.autograd.grad(actual.strategy_returns.sum(), w)
    wanted, = torch.autograd.grad(expected.strategy_returns.sum(), w)
    torch.testing.assert_close(got, wanted, rtol=1e-5, atol=1e-8)
