"""The fused close preserves physical ownership, default and adjoints."""
from dataclasses import fields, replace

import pytest
import torch

from stockagent.backtest import tw_day_trade_carry as carry
from test_tw_day_trade_carry import DAY, run, session, v
from test_tw_day_trade_carry_checkpoint_blocks import _cases, _assert_financially_close


def on_cuda(session):
    return replace(session, **{field.name: getattr(session, field.name).cuda()
        for field in fields(session) if isinstance(getattr(session, field.name), torch.Tensor)})


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires initialized CUDA")
@pytest.mark.parametrize("kind", ["ordinary", "claim", "locked", "terminal", "default"])
@pytest.mark.parametrize("compact", [False, True])
def test_cuda_close_block_preserves_all_state_and_cross_day_gradients(monkeypatch, kind, compact):
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL", "0")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_CHECKPOINT_BLOCK_ROWS", "0")
    sessions = tuple(on_cuda(row) for row in _cases(kind))
    weights = v(-.9, .2, -.1, .3, 0, -.2, .1).cuda().reshape(-1, 1).requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_FINALIZE_COMPILE", "0")
    expected = run(weights, sessions, event_compression=compact)
    objective = (expected.strategy_returns.sum() + expected.minute_nav.sum()*1e-11
                 + expected.final_state.last_nav*1e-7)
    expected_grad, = torch.autograd.grad(objective, weights)
    actual_weights = weights.detach().clone().requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_FINALIZE_COMPILE", "1")
    before = carry.get_day_trade_carry_compile_stats()["compiled_session_finalize_calls"]
    actual = run(actual_weights, sessions, event_compression=compact)
    _assert_financially_close(actual, expected)
    actual_objective = (actual.strategy_returns.sum() + actual.minute_nav.sum()*1e-11
                        + actual.final_state.last_nav*1e-7)
    actual_grad, = torch.autograd.grad(actual_objective, actual_weights)
    torch.testing.assert_close(actual_grad, expected_grad, rtol=2e-10, atol=1e-9)
    assert torch.isfinite(actual_grad).all()
    # A failed compression certificate replays the same physical sessions.
    assert carry.get_day_trade_carry_compile_stats()["compiled_session_finalize_calls"] - before >= len(sessions)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires initialized CUDA")
def test_cuda_close_source_failure_is_atomic_and_not_economic_default(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL", "0")
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_CARRY_CHECKPOINT_BLOCK_ROWS", "0")
    monkeypatch.setenv("STOCKAGENT_STRICT_NO_FALLBACK", "1")
    first = run(v(-.21).reshape(1, 1), [session()])
    state = first.final_state.detached(device="cuda")
    row = on_cuda(session(DAY + 1))
    row = replace(row, source_gap_mask=torch.ones_like(row.official_open))
    weights = v(.2).cuda().reshape(-1, 1).requires_grad_()
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_FINALIZE_COMPILE", "0")
    expected = run(weights, (row,), state, event_compression=True)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_FINALIZE_COMPILE", "1")
    actual = run(weights, (row,), state, event_compression=True)
    _assert_financially_close(actual, expected)
    assert actual.final_state.inventory.failed.item() == 1
    assert actual.final_state.alive.item()
    assert torch.isnan(actual.minute_nav).all()
