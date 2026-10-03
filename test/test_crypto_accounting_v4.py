"""Cash-unit oracles for the live-NAV crypto ledger; no training jobs."""
from __future__ import annotations

import os
from dataclasses import fields

import numpy as np
import pytest
import torch

from stockagent.backtest.crypto_perpetual import (
    CryptoPerpetualDataError, run_crypto_perpetual_torch,
)
from stockagent.backtest.simulator import BacktestResult, run_backtest_torch
from stockagent.training import trainer


def ledger(target, effective=None, price=None, **kw):
    mask = torch.ones_like(target, dtype=torch.bool)
    effective = torch.zeros_like(target) if effective is None else effective
    price = effective if price is None else price
    options = dict(buy_fee_rate=0.00055, sell_fee_rate=0.00055,
                   long_only=False, maximum_gross=1.0)
    options.update(kw)
    return run_crypto_perpetual_torch(
        target, torch.log1p(effective), torch.log1p(price),
        mask, options.pop("can_buy", mask), options.pop("can_sell", mask),
        options.pop("can_short", mask), torch.zeros_like(mask), **options,
    )


@pytest.mark.parametrize("nav", [0.5, 1.0, 2.0, 1e50])
def test_absolute_volume_capacity_is_invariant_to_live_nav(nav):
    result = ledger(torch.tensor([[0.8]]), initial_equity_scale=torch.tensor(nav, dtype=torch.float64),
                    volume_limit_weights=torch.tensor([[0.1]]))
    if nav < 1e40:
        assert result.executed_weights.item() * nav == pytest.approx(0.1, rel=2e-5, abs=1e-7)
    else:
        # A weight smaller than FP32's range must underfill, never overflow.
        assert result.executed_weights.item() == 0.0
    assert torch.isfinite(result.final_equity_scale)


def test_unlimited_capacity_stays_unlimited_at_large_nav_and_has_finite_gradient():
    action = torch.tensor([[0.8], [-0.6]], requires_grad=True)
    result = ledger(action, torch.tensor([[0.01], [0.02]]),
                    initial_equity_scale=torch.tensor(1e50, dtype=torch.float64))
    torch.testing.assert_close(result.executed_weights, action)
    result.final_equity_scale.log().backward()
    assert torch.isfinite(action.grad).all()


@pytest.mark.parametrize("short", [False, True])
@pytest.mark.parametrize("capacity", [0.0, 0.02])
def test_gross_risk_cannot_create_liquidity_or_bypass_side_permissions(short, capacity):
    sign = -1 if short else 1
    target = torch.tensor([[sign * 1.2]])
    cap = torch.full_like(target, capacity)
    result = ledger(target, initial_weights=target[0], volume_limit_weights=cap)
    assert result.turnovers.item() == pytest.approx(capacity, abs=1e-7)
    assert result.executed_weights.abs().item() == pytest.approx(1.2 - capacity)
    side = torch.zeros_like(target, dtype=torch.bool)
    result = ledger(target, initial_weights=target[0], volume_limit_weights=cap,
                    **({"can_buy": side} if short else {"can_sell": side}))
    assert result.turnovers.item() == 0
    assert result.executed_weights.item() == pytest.approx(sign * 1.2)


def _cash_oracle(target, prices, funding, cap, capital=1000.0):
    """Independent signed contract quantity and currency NAV recurrence."""
    nav, quantity = capital, np.zeros(prices.shape[1])
    returns, turnovers, weights, scales = [], [], [], []
    for row in range(len(target)):
        proposed_quantity = target[row] * nav / prices[row]
        change = np.clip(proposed_quantity - quantity,
                         -cap[row] * capital / prices[row], cap[row] * capital / prices[row])
        quantity += change
        traded_cash = np.abs(change * prices[row]).sum()
        weights.append(quantity * prices[row] / nav)
        pnl = (quantity * (prices[row + 1] - prices[row] - funding[row])).sum()
        next_nav = nav + pnl - traded_cash * 0.00055
        returns.append(next_nav / nav - 1)
        turnovers.append(traded_cash / nav)
        nav = next_nav
        scales.append(nav / capital)
    return np.array(returns), np.array(turnovers), np.array(weights), np.array(scales)


def _sample():
    rng = np.random.default_rng(12)
    target = rng.uniform(-0.3, 0.3, size=(9, 2)).astype(np.float32)
    prices = np.exp(np.cumsum(rng.normal(0, 0.08, size=(10, 2)), axis=0)) * 100
    funding = rng.uniform(-0.003, 0.003, size=(9, 2)) * prices[:-1]
    cap = rng.uniform(0.02, 0.15, size=(9, 2)).astype(np.float32)
    price = prices[1:] / prices[:-1] - 1
    effective = price - funding / prices[:-1]
    return target, prices, funding, cap, price, effective


def test_quantity_cash_oracle_and_chunked_equity_gradient_parity():
    target, prices, funding, cap, price, effective = _sample()
    action = torch.tensor(target, requires_grad=True)
    args = [torch.tensor(x, dtype=torch.float32) for x in (effective, price)]
    full = ledger(action, *args, volume_limit_weights=torch.tensor(cap))
    expected = _cash_oracle(target, prices, funding, cap)
    for actual, reference in zip((full.strategy_simple_returns, full.turnovers,
                                  full.executed_weights, full.equity_scale_history), expected):
        np.testing.assert_allclose(actual.detach(), reference, atol=3e-7, rtol=3e-5)
    chunk_action = action.detach().clone().requires_grad_(True)
    chunks, state = [], {}
    for start, end in [(0, 3), (3, 7), (7, 9)]:
        part = ledger(chunk_action[start:end], *(x[start:end] for x in args),
                      volume_limit_weights=torch.tensor(cap[start:end]), **state)
        # Deliberately keep graph here: compare full recurrence, not TBPTT.
        state = dict(initial_weights=part.final_weights, initial_alive=part.final_alive,
                     initial_equity_scale=part.final_equity_scale)
        chunks.append(part)
    for field in ["strategy_simple_returns", "turnovers", "executed_weights", "equity_scale_history"]:
        torch.testing.assert_close(torch.cat([getattr(p, field) for p in chunks]), getattr(full, field))
    full.final_equity_scale.log().backward()
    chunks[-1].final_equity_scale.log().backward()
    torch.testing.assert_close(action.grad, chunk_action.grad)
    # Independent cash oracle finite differences away from clipping boundaries.
    for row, symbol in [(0, 0), (4, 1), (8, 0)]:
        plus, minus = target.astype(float).copy(), target.astype(float).copy()
        plus[row, symbol] += 1e-4
        minus[row, symbol] -= 1e-4
        derivative = (np.log(_cash_oracle(plus, prices, funding, cap)[-1][-1])
                      - np.log(_cash_oracle(minus, prices, funding, cap)[-1][-1])) / 2e-4
        assert action.grad[row, symbol].item() == pytest.approx(derivative, abs=2e-5)


def test_padding_freezes_nav_and_missing_unused_labels_are_inert():
    action = torch.tensor([[0.2, 0.0], [1.0, 1.0]], requires_grad=True)
    result = ledger(action, torch.tensor([[0.1, float("nan")], [3.0, float("nan")]]),
                    state_advance_mask=torch.tensor([True, False]),
                    initial_equity_scale=torch.tensor(2.0), volume_limit_weights=torch.full_like(action, 0.1))
    assert result.strategy_simple_returns[1] == 0
    torch.testing.assert_close(result.equity_scale_history[0], result.equity_scale_history[1])
    result.final_equity_scale.backward()
    torch.testing.assert_close(action.grad[1], torch.zeros(2))


def test_true_insolvency_is_absorbing_but_unknown_valuation_is_an_error():
    result = ledger(torch.tensor([[-1.0], [1.0]]), torch.tensor([[2.0], [0.1]]))
    assert not result.final_alive.item()
    assert result.strategy_simple_returns[1] == 0
    assert result.turnovers[1] == 0
    with pytest.raises(CryptoPerpetualDataError, match="symbol_index=0"):
        ledger(torch.tensor([[1e-11]]), torch.tensor([[float("nan")]]))


def test_equity_survives_canonical_eval_chunks_and_artifact_round_trip(tmp_path):
    target, _, _, cap, price, effective = _sample()
    action = torch.tensor(target)
    mask, no = torch.ones_like(action, dtype=torch.bool), torch.zeros_like(action, dtype=torch.bool)
    runtime = trainer._ExecutionRuntime(mode="crypto_perpetual", buy_fee_rates=None,
        sell_fee_rates=None, lot_sizes=None, settlement_lag_sessions=0,
        crypto_stateful_proximal_allocator=True)
    direct = run_backtest_torch(action, torch.log1p(torch.tensor(effective, dtype=torch.float32)),
        mask, torch.zeros(len(action)), buy_fee_rate=.00055, sell_fee_rate=.00055,
        overnight_returns=torch.log1p(torch.tensor(price, dtype=torch.float32)),
        execution_mode="crypto_perpetual", portfolio_activation="pre_normalized", long_only=False,
        volume_limit_weights=torch.tensor(cap), crypto_stateful_proximal_allocator=True)
    results = []
    for chunk in [2, 4, 20]:
        result, _ = trainer._run_eval_backtest_from_weight_buffers(
            action, torch.log1p(torch.tensor(effective, dtype=torch.float32)),
            mask, mask, mask, mask, no, no, torch.zeros(len(action)),
            device=torch.device("cpu"), non_blocking=False, long_only=False,
            buy_fee_rate=.00055, sell_fee_rate=.00055, max_turnover_ratio=0.,
            gross_leverage=1., min_trade_weight=0., backtest_chunk_rows=chunk,
            compute_metrics_summary=True, return_weights_history=True,
            profile_timing=False, progress_label=None, timing=trainer.TimingBreakdown(),
            reset_at_rows=None, portfolio_activation="pre_normalized",
            overnight_log_returns_all=torch.log1p(torch.tensor(price, dtype=torch.float32)),
            volume_notional_all=torch.tensor(cap) * 1e8,
            max_volume_participation=.01, volume_participation_equity=1e6,
            execution_runtime=runtime,
        )
        torch.testing.assert_close(result.strategy_returns, direct.strategy_returns)
        torch.testing.assert_close(result.equity_scale_history, direct.equity_scale_history)
        torch.testing.assert_close(result.final_equity_scale, direct.final_equity_scale)
        results.append(result)
    payload = {f.name: (getattr(results[0], f.name).detach().numpy()
                       if isinstance(getattr(results[0], f.name, None), torch.Tensor)
                       else getattr(results[0], f.name, None)) for f in fields(BacktestResult)
               if hasattr(results[0], f.name)}
    result = BacktestResult(**payload)
    dates = np.arange("2024-01-01", "2024-01-10", dtype="datetime64[D]")
    path = tmp_path / "crypto.npz"
    trainer._save_backtest_artifact(path, result, dates)
    loaded = trainer._load_backtest_artifact(path)
    # Reader returns (result, dates).
    read_result = loaded[0] if isinstance(loaded, tuple) else loaded
    np.testing.assert_allclose(read_result.equity_scale_history, result.equity_scale_history)
    prefix = trainer._prefix_backtest_result(result, 3)
    trainer._save_backtest_artifact(tmp_path / "prefix.npz", prefix, dates[:3])
    assert prefix.final_weights is None


@pytest.mark.skipif(os.environ.get("STOCKAGENT_TEST_CRYPTO_CUDA") != "1" or not torch.cuda.is_available(),
                    reason="explicit bounded CUDA kernel test only")
@pytest.mark.filterwarnings("ignore:The .grad attribute of a Tensor that is not a leaf Tensor:UserWarning")
def test_compiled_crypto_block_matches_eager_values_and_gradients(monkeypatch):
    target, _, _, cap, price, effective = _sample()
    outputs, grads = [], []
    for compiled in ["0", "1"]:
        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", compiled)
        action = torch.tensor(target, device="cuda", requires_grad=True)
        result = ledger(action, torch.tensor(effective, device="cuda"), torch.tensor(price, device="cuda"),
                        volume_limit_weights=torch.tensor(cap, device="cuda"))
        result.final_equity_scale.log().backward()
        outputs.append(result)
        grads.append(action.grad)
    for field in ["strategy_simple_returns", "turnovers", "executed_weights", "equity_scale_history", "final_weights"]:
        torch.testing.assert_close(getattr(outputs[0], field), getattr(outputs[1], field), atol=2e-7, rtol=2e-5)
    torch.testing.assert_close(*grads, atol=2e-7, rtol=2e-5)
