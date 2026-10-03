"""Unrequested reporting work must not change the recurrent account or STE."""
from dataclasses import fields
import os

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.data import tw_futures_margin as m
from test_tw_futures_margin import tape


@pytest.mark.parametrize("history", [False, True])
@pytest.mark.parametrize("recover", [False, True])
@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_optional_audit_preserves_account_and_gradients(monkeypatch, history, recover, device):
    if device == "cuda" and (
        os.environ.get("STOCKAGENT_TEST_CUDA_GRAPH") != "1" or not torch.cuda.is_available()
    ):
        pytest.skip("explicit CUDA graph acceptance required")
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "0")
    monkeypatch.setenv("STOCKAGENT_FUTURES_CUDA_GRAPH", str(int(device == "cuda")))
    monkeypatch.setenv("STOCKAGENT_FUTURES_FUNDING_COMPILE", str(int(device == "cuda")))
    generator = torch.Generator().manual_seed(913)
    execution = tape(12, 3)
    execution[..., 3:5] += torch.randn(12, 3, 2, generator=generator) * 30
    execution[1:, :, m.PREVIOUS_MARK] = execution[:-1, :, 4]
    execution[..., 5:8] = 1
    execution[4, 0, m.CAN_BUY] = 0
    execution[5, 1, m.CAN_SELL] = 0
    execution[8, :, 2] = execution[8, :, m.CASH_SETTLEMENT] = 1
    execution[9, :, m.POSITION_LIMIT] = 1
    execution = execution.to(device)
    requested = (torch.rand(12, 3, generator=generator) - .5).to(device) * .5
    advance = torch.ones(12, dtype=torch.bool, device=device)
    advance[-1] = False
    results, gradients = [], []
    for audit in (True, False):
        weights = requested.clone().requires_grad_()
        result = run_tw_futures_portfolio_integer_torch(
            weights, execution, initial_capital=1000., state_advance_mask=advance,
            return_weights_history=history, recoverable_backward=recover,
            return_margin_audit=audit,
        )
        result.strategy_returns.sum().backward()
        results.append(result)
        gradients.append(weights.grad.clone())
    assert results[0].margin_audit_history.shape == (12, len(m.MARGIN_AUDIT_COLUMNS))
    assert results[1].margin_audit_history is None
    for field in fields(results[0]):
        if field.name == "margin_audit_history":
            continue
        a, b = (getattr(result, field.name) for result in results)
        if isinstance(a, torch.Tensor):
            torch.testing.assert_close(a, b, rtol=0, atol=0, msg=field.name)
        else:
            assert a is b
    torch.testing.assert_close(*gradients, rtol=0, atol=0)


def test_training_loss_omits_only_margin_report(monkeypatch):
    from stockagent.training import loss as loss_module
    canonical = loss_module.run_backtest_torch
    observed = []

    def record(*args, **kwargs):
        result = canonical(*args, **kwargs)
        observed.append(result.futures_margin_audit)
        return result

    monkeypatch.setattr(loss_module, "run_backtest_torch", record)
    weights = torch.zeros(2, 1936)
    weights[:, 0] = .5
    weights.requires_grad_()
    execution = torch.zeros(2, 1936, m.MARGIN_EXECUTION_WIDTH)
    execution[:, :1] = tape(2)
    execution[:, 0, 4] += 10
    loss = loss_module.risk_aware_loss(
        weights, torch.zeros_like(weights), torch.ones_like(weights, dtype=torch.bool),
        overnight_log_returns=execution, execution_mode="tw_stock_context_futures_portfolio",
        day_trade_execution_initial_capital=1000., long_only=False,
        portfolio_activation="pre_normalized", objective="log_utility",
        gamma_turnover=0., concentration_weight=0.,
    )
    loss.backward()
    assert observed == [None]
    assert torch.isfinite(weights.grad).all()
    assert weights.grad.abs().sum() > 0


@pytest.mark.parametrize("history", [False, True])
def test_simulator_keeps_other_histories_when_margin_audit_is_omitted(history):
    from stockagent.backtest.simulator import run_backtest_torch
    weights = torch.full((3, 2), .2)
    results = [run_backtest_torch(
        weights, torch.zeros_like(weights), torch.ones_like(weights, dtype=torch.bool),
        torch.zeros(3), buy_fee_rate=0., sell_fee_rate=0., long_only=False,
        portfolio_activation="pre_normalized", execution_mode="tw_stock_context_futures_portfolio",
        overnight_returns=tape(3, 2), day_trade_execution_initial_capital=1000.,
        return_weights_history=history, return_futures_margin_audit=audit,
    ) for audit in (True, False)]
    assert results[1].futures_margin_audit is None
    for field in fields(results[0]):
        if field.name == "futures_margin_audit":
            continue
        a, b = (getattr(result, field.name) for result in results)
        if isinstance(a, torch.Tensor):
            torch.testing.assert_close(a, b, rtol=0, atol=0, msg=field.name)
        else:
            assert a == b, field.name
