"""Exact CUDA replay, mutable inputs, autograd lifetime and three futures modes."""
from dataclasses import fields
import os

import pytest
import torch

from stockagent.backtest.futures_cuda_graph import (
    clear_futures_cuda_graph_cache, get_futures_cuda_graph_stats,
)
from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.backtest.tw_stock_futures_day_trade import run_tw_stock_futures_day_trade_integer_torch
from stockagent.data import tw_futures_margin as m
from stockagent.data.tw_stock_futures_minute import TAPE_FIELDS

requires_cuda = pytest.mark.skipif(
    os.environ.get("STOCKAGENT_TEST_CUDA_GRAPH") != "1" or not torch.cuda.is_available(),
    reason="explicit CUDA graph acceptance run required",
)


def case(mode, rows=17, slots=16):
    generator = torch.Generator().manual_seed(1941)
    weights = torch.randn(rows, slots, generator=generator)
    weights = .75 * weights / weights.abs().sum(-1, keepdim=True)
    options = dict(initial_capital=1e7, recoverable_backward=True)
    if mode == "intraday":
        tape = torch.zeros(rows, slots, 2, TAPE_FIELDS)
        tape[..., 0] = torch.tensor([2000., 100.])
        tape[..., 1] = 40
        tape[..., 2] = .00002
        bars = tape[..., 3:].reshape(rows, slots, 2, 12, 5)
        bars[..., :4] = 100
        bars[..., 4] = 1000
        bars[..., 1:, :4] = 101
        tape[2, 0, :, 3+2*5:] = 0  # unfilled exit must retain its failure
        options.update(scheduled_events=True, use_compile=False,
                       recovery_objective="execution_utility")
        function = run_tw_stock_futures_day_trade_integer_torch
    else:
        tape = torch.zeros(rows, slots, m.MARGIN_EXECUTION_WIDTH if mode == "margin" else 11)
        tape[..., 1] = 1
        tape[..., 3] = torch.linspace(1000, 30000, slots)
        tape[..., 4] = tape[..., 3] * (1 + torch.randn(rows, slots, generator=generator) * .01)
        tape[..., 0] = (tape[..., 4] / tape[..., 3]).log()
        tape[..., 5] = 40
        tape[..., 6:8] = 1
        tape[..., 8] = 500
        tape[..., 9] = (torch.arange(slots) // 2) * 2
        tape[..., 10] = torch.arange(slots) % 2
        tape[3, :, 2] = 1
        tape[4, :, 8] = 0
        if mode == "margin":
            tape[..., m.INITIAL] = tape[..., m.END_INITIAL] = tape[..., m.PREVIOUS_INITIAL] = tape[..., 3] * .2
            tape[..., m.MAINTENANCE] = tape[..., m.END_MAINTENANCE] = tape[..., m.PREVIOUS_MAINTENANCE] = tape[..., 3] * .15
            tape[..., m.PREVIOUS_MARK] = tape[..., 3]
            tape[1:, :, m.PREVIOUS_MARK] = tape[:-1, :, 4]
            tape[..., m.CAN_BUY] = tape[..., m.CAN_SELL] = 1
            tape[..., m.TERMINAL_CAN_BUY] = tape[..., m.TERMINAL_CAN_SELL] = 1
            tape[..., m.POSITION_GROUP] = torch.arange(slots)
            tape[..., m.POSITION_UNIT] = 1
            tape[..., m.POSITION_LIMIT] = 1000
            tape[..., m.LIQUIDATION_RATIO] = .25
            tape[..., m.TERMINAL_MARK] = tape[..., 4]
            tape[..., m.TERMINAL_CAPACITY] = 1000
            tape[1, 0, m.CAN_SELL] = 0
        function = run_tw_futures_portfolio_integer_torch
    options["state_advance_mask"] = torch.ones(rows, dtype=torch.bool, device="cuda")
    options["state_advance_mask"][-1] = False
    return function, weights.cuda(), tape.cuda(), options


def assert_result_equal(a, b):
    for field in fields(a):
        x, y = getattr(a, field.name), getattr(b, field.name)
        if isinstance(x, torch.Tensor):
            torch.testing.assert_close(x, y, rtol=0, atol=0, msg=field.name)
        else:
            assert x is y, field.name


@pytest.mark.parametrize("mode", ["general", "margin", "intraday"])
@pytest.mark.parametrize("grad", [False, True])
@pytest.mark.parametrize("history", [False, True])
@pytest.mark.parametrize("funding_compile", [False, True])
@requires_cuda
def test_replay_matches_all_account_fields_and_gradients(monkeypatch, mode, grad, history, funding_compile):
    clear_futures_cuda_graph_cache()
    function, weights, tape, options = case(mode)
    options["return_weights_history"] = history
    saved = None
    for scale in (1., -.4, 0.):
        results, gradients = [], []
        for graph in (False, True):
            monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
            monkeypatch.setenv("STOCKAGENT_FUTURES_CUDA_GRAPH", str(int(graph)))
            monkeypatch.setenv("STOCKAGENT_FUTURES_FUNDING_COMPILE", str(int(graph and funding_compile)))
            w = (weights * scale).requires_grad_(grad)
            with torch.set_grad_enabled(grad):
                result = function(w, tape, **options)
                if grad:
                    result.strategy_returns.sum().backward()
                    gradients.append(w.grad.clone())
            results.append(result)
        assert_result_equal(*results)
        if grad:
            torch.testing.assert_close(*gradients, rtol=0, atol=0)
        if saved is not None:
            assert_result_equal(*saved)
        saved = results
    assert get_futures_cuda_graph_stats()["calls"] > 0


@requires_cuda
def test_pending_and_retained_backward_cannot_be_overwritten(monkeypatch):
    clear_futures_cuda_graph_cache()
    function, weights, tape, options = case("general")
    expected = []
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    monkeypatch.setenv("STOCKAGENT_FUTURES_CUDA_GRAPH", "0")
    for scale in (1., -.4):
        w = (weights * scale).requires_grad_()
        expected.append(torch.autograd.grad(function(w, tape, **options).strategy_returns.sum(), w)[0])
    monkeypatch.setenv("STOCKAGENT_FUTURES_CUDA_GRAPH", "1")
    a, b = weights.clone().requires_grad_(), (-.4 * weights).requires_grad_()
    first, second = function(a, tape, **options), function(b, tape, **options)
    ga = torch.autograd.grad(first.strategy_returns.sum(), a, retain_graph=True)[0]
    gb = torch.autograd.grad(second.strategy_returns.sum(), b)[0]
    again = function(b, tape, **options)
    ga2 = torch.autograd.grad(first.strategy_returns.sum(), a)[0]
    torch.testing.assert_close(ga, expected[0], rtol=0, atol=0)
    torch.testing.assert_close(ga2, expected[0], rtol=0, atol=0)
    torch.testing.assert_close(gb, expected[1], rtol=0, atol=0)
    assert again.strategy_returns.shape == first.strategy_returns.shape
    assert get_futures_cuda_graph_stats()["busy_eager_calls"] >= 2


@pytest.mark.parametrize("mode", ["general", "margin"])
@requires_cuda
def test_graph_continues_from_supplied_integer_account_state(monkeypatch, mode):
    clear_futures_cuda_graph_cache()
    function, weights, tape, options = case(mode, rows=17)
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    results = []
    for graph in (False, True):
        monkeypatch.setenv("STOCKAGENT_FUTURES_CUDA_GRAPH", str(int(graph)))
        with torch.no_grad():
            first = function(weights, tape, **options)
            second = function(-weights, tape, **options,
                              initial_quantities=first.final_weights,
                              initial_equity_scale=first.final_equity_scale,
                              initial_alive=first.final_alive)
        results.append((first, second))
    for eager, graphed in zip(*results, strict=True):
        assert_result_equal(eager, graphed)


def test_cuda_graph_config_is_runtime_only_and_cpu_keeps_eager():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _training_checkpoint_contract
    from stockagent.backtest.futures_cuda_graph import futures_cuda_graph_enabled
    config = load_config("configs/markets/tw_futures_v8_general.yaml")
    before = _training_checkpoint_contract(config)
    config.training.futures_cuda_graph = not config.training.futures_cuda_graph
    config.training.futures_funding_compile = not config.training.futures_funding_compile
    assert _training_checkpoint_contract(config) == before
    assert not futures_cuda_graph_enabled(torch.zeros(1, 1))


@requires_cuda
def test_compiled_funding_keeps_native_reduction_at_fp32_boundary():
    from stockagent.backtest.tw_futures_portfolio import (
        _globally_funded_group_candidate_indices_impl, _compiled_funding_selector,
    )
    generator = torch.Generator().manual_seed(74203)
    for trial in range(32):
        cash = torch.rand(64, 32, generator=generator).cuda() * 2000
        exposure = (torch.rand(64, 32, generator=generator).cuda() - .5) * 2000
        target_cash = torch.rand(64, generator=generator).cuda() * 1800
        target = (torch.rand(64, generator=generator).cuda() - .5) * 2000
        capacity = torch.rand(64, 32, generator=generator).cuda() > .3
        cash[:, -2] = trial % 17 * 10
        capacity[:, -2] = True
        options = dict(cash_required=cash, candidate_exposure=exposure,
                       target_exposure=target, capacity_ok=capacity,
                       target_cash=target_cash, equity=cash.new_zeros(()), target_candidate_count=30)
        minimum, _ = _globally_funded_group_candidate_indices_impl(**options)
        total = cash.gather(1, minimum[:, None]).sum()
        # Funding is now a hard budget comparison. Probe the actual native
        # sum boundary; the old total/(1+64eps) cases no longer straddle it.
        for equity in (
            torch.nextafter(total, total.new_full((), -float("inf"))),
            total,
            torch.nextafter(total, total.new_full((), float("inf"))),
        ):
            options["equity"] = equity
            expected = _globally_funded_group_candidate_indices_impl(**options)
            actual = _compiled_funding_selector()(**options)
            for eager, fused in zip(expected, actual, strict=True):
                torch.testing.assert_close(eager, fused, atol=0, rtol=0)
            selected, fundable = actual
            assert bool(fundable) == bool(total <= equity)
            if bool(fundable):
                assert cash.gather(1, selected[:, None]).sum() <= equity
