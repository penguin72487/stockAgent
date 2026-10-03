"""Opt-in announced exits waive volume only; v5 remains the default oracle."""
from __future__ import annotations

from dataclasses import fields
import os
from types import SimpleNamespace

import pytest
import torch

from stockagent.backtest import crypto_perpetual as crypto


def _run(target, *, price=None, effective=None, mask=None, buy=None, sell=None,
         short=None, force=None, volume=None, **kwargs):
    yes = torch.ones_like(target, dtype=torch.bool)
    price = torch.zeros_like(target) if price is None else price
    return crypto.run_crypto_perpetual_torch(
        target, price if effective is None else effective, price,
        yes if mask is None else mask, yes if buy is None else buy,
        yes if sell is None else sell, yes if short is None else short,
        ~yes if force is None else force,
        volume_limit_weights=volume, buy_fee_rate=0.0008, sell_fee_rate=0.00055,
        long_only=False, maximum_gross=1.0, **kwargs,
    )


def _assert_bits_equal(lhs, rhs):
    for field in fields(lhs):
        a, b = getattr(lhs, field.name), getattr(rhs, field.name)
        assert torch.equal(a.reshape(-1).view(torch.uint8), b.reshape(-1).view(torch.uint8)), field.name


@pytest.mark.parametrize("sign", [-1, 1])
@pytest.mark.parametrize("target_value", [-0.8, 0.0, 0.8])
@pytest.mark.parametrize("proximal", [False, True])
def test_announced_exit_fully_closes_both_sides_without_opening_or_reversal(sign, target_value, proximal):
    target = torch.tensor([[target_value]])
    result = _run(
        target, force=torch.ones_like(target, dtype=torch.bool),
        mask=torch.zeros_like(target, dtype=torch.bool), volume=torch.zeros_like(target),
        initial_weights=torch.tensor([sign * 0.5]), initial_equity_scale=torch.tensor(4.0),
        effective=torch.full_like(target, 0.2), price=torch.full_like(target, -0.1),
        stateful_proximal_allocator=proximal, announced_exit_unlimited_volume=True,
    )
    fee = 0.00055 if sign > 0 else 0.0008
    assert result.executed_weights.item() == 0.0
    assert result.final_weights.item() == 0.0
    assert result.turnovers.item() == 0.5
    assert result.strategy_simple_returns.item() == pytest.approx(-0.5 * fee)
    assert result.final_equity_scale.item() == pytest.approx(4.0 * (1.0 - 0.5 * fee))


def test_only_announced_reductions_are_exempt_and_unused_capacity_is_not_redistributed():
    target = torch.tensor([[-0.8, -0.8, 0.8]])
    result = _run(
        target, force=torch.tensor([[True, False, True]]),
        initial_weights=torch.tensor([0.5, -0.3, 0.0]),
        volume=torch.full_like(target, 0.01), announced_exit_unlimited_volume=True,
    )
    torch.testing.assert_close(result.executed_weights, torch.tensor([[0.0, -0.31, 0.0]]))
    assert result.turnovers.item() == pytest.approx(0.51)
    assert result.strategy_simple_returns.item() == pytest.approx(-0.51 * 0.00055)


@pytest.mark.parametrize("sign", [-1, 1])
def test_announced_volume_exemption_does_not_bypass_side_permission(sign):
    target = torch.zeros((1, 1))
    yes = torch.ones_like(target, dtype=torch.bool)
    result = _run(
        target, force=yes, buy=yes if sign > 0 else ~yes, sell=yes if sign < 0 else ~yes,
        initial_weights=torch.tensor([sign * 0.5]), volume=torch.zeros_like(target),
        announced_exit_unlimited_volume=True,
    )
    assert result.executed_weights.item() == sign * 0.5
    assert result.turnovers.item() == 0.0


@pytest.mark.parametrize("sign", [-1, 1])
def test_announced_volume_exemption_keeps_turnover_cap_and_residual_valuation(sign):
    target = torch.zeros((1, 1))
    result = _run(
        target, force=torch.ones_like(target, dtype=torch.bool),
        initial_weights=torch.tensor([sign * 0.5]), volume=torch.zeros_like(target),
        max_turnover_ratio=0.1, announced_exit_unlimited_volume=True,
        effective=torch.log1p(torch.full_like(target, 0.02)),
    )
    assert result.executed_weights.item() == pytest.approx(sign * 0.4)
    assert result.turnovers.item() == pytest.approx(0.1)
    fee = 0.00055 if sign > 0 else 0.0008
    assert result.strategy_simple_returns.item() == pytest.approx(sign * 0.4 * 0.02 - 0.1 * fee)


def test_padding_and_dead_account_do_not_execute_an_announced_exit():
    target = torch.zeros((1, 1))
    common = dict(force=torch.ones_like(target, dtype=torch.bool),
                  initial_weights=torch.tensor([0.5]), volume=torch.zeros_like(target),
                  announced_exit_unlimited_volume=True)
    padded = _run(target, state_advance_mask=torch.tensor([False]), **common)
    assert padded.executed_weights.item() == 0.5 and padded.turnovers.item() == 0.0
    dead = _run(target, initial_alive=torch.tensor(False), **common)
    assert dead.executed_weights.item() == 0.0 and dead.turnovers.item() == 0.0
    assert not dead.final_alive.item()


@pytest.mark.parametrize("announced", [False, True])
def test_future_missingness_never_triggers_an_exit_or_erases_a_side_blocked_residual(announced):
    target = torch.zeros((1, 1))
    no = torch.zeros_like(target, dtype=torch.bool)
    with pytest.raises(crypto.CryptoPerpetualDataError) as caught:
        _run(
            target, mask=no, buy=~no, sell=no, force=torch.full_like(no, announced),
            initial_weights=torch.tensor([0.5]), volume=torch.zeros_like(target),
            effective=torch.full_like(target, float("nan")), announced_exit_unlimited_volume=True,
        )
    assert caught.value.evidence["remaining_executed_weight"] == 0.5
    assert caught.value.evidence["announced_exit_volume_exempt"] is announced


def test_completed_announced_exit_needs_no_future_held_valuation():
    target = torch.zeros((1, 1))
    result = _run(
        target, force=torch.ones_like(target, dtype=torch.bool),
        initial_weights=torch.tensor([0.5]), volume=torch.zeros_like(target),
        effective=torch.full_like(target, float("nan")), price=torch.full_like(target, float("nan")),
        announced_exit_unlimited_volume=True,
    )
    assert result.final_weights.item() == 0.0 and result.final_alive.item()


def _random_case(seed=8, device="cpu"):
    generator = torch.Generator().manual_seed(seed)
    target = torch.randn((11, 7), generator=generator) * 0.08
    price = torch.randn(target.shape, generator=generator) * 0.01
    data = dict(
        price=price.to(device), effective=(price - 0.0002).to(device),
        mask=(torch.rand(target.shape, generator=generator) > 0.1).to(device),
        buy=(torch.rand(target.shape, generator=generator) > 0.15).to(device),
        sell=(torch.rand(target.shape, generator=generator) > 0.15).to(device),
        short=(torch.rand(target.shape, generator=generator) > 0.15).to(device),
        force=(torch.rand(target.shape, generator=generator) > 0.7).to(device),
        volume=(torch.rand(target.shape, generator=generator) * 0.03).to(device),
        state_advance_mask=torch.tensor([True] * 10 + [False], device=device),
    )
    state = dict(initial_weights=torch.randn(7, generator=generator).to(device) * 0.03,
                 initial_equity_scale=torch.tensor(2.0, dtype=torch.float64, device=device))
    return target.to(device), data, state


@pytest.mark.parametrize("seed", [0, 3, 8])
def test_default_v5_is_bitwise_identical_and_opt_in_matches_existing_unlimited_capacity_oracle(seed):
    target, data, state = _random_case(seed)
    legacy = _run(target, **data, **state)
    explicit_false = _run(target, **data, **state, announced_exit_unlimited_volume=False)
    _assert_bits_equal(legacy, explicit_false)
    action, reference_action = target.clone().requires_grad_(), target.clone().requires_grad_()
    actual = _run(action, **data, **state, announced_exit_unlimited_volume=True, max_turnover_ratio=0.1)
    reference_data = dict(data)
    reference_data["volume"] = torch.where(
        data["force"] & data["state_advance_mask"][:, None], float("inf"), data["volume"],
    )
    reference = _run(reference_action, **reference_data, **state, max_turnover_ratio=0.1)
    _assert_bits_equal(actual, reference)
    actual.final_equity_scale.log().backward()
    reference.final_equity_scale.log().backward()
    torch.testing.assert_close(action.grad, reference_action.grad, atol=0.0, rtol=0.0)


def test_chunked_opt_in_preserves_state_values_and_gradients():
    target, data, state = _random_case()
    action, chunk_action = target.clone().requires_grad_(), target.clone().requires_grad_()
    whole = _run(action, **data, **state, announced_exit_unlimited_volume=True)
    chunks = []
    for start, end in [(0, 3), (3, 7), (7, 11)]:
        chunk = _run(chunk_action[start:end], **{k:v[start:end] for k,v in data.items()},
                     **state, announced_exit_unlimited_volume=True)
        chunks.append(chunk)
        state = dict(initial_weights=chunk.final_weights, initial_alive=chunk.final_alive,
                     initial_equity_scale=chunk.final_equity_scale)
    for name in ("strategy_simple_returns", "turnovers", "executed_weights", "equity_scale_history"):
        torch.testing.assert_close(getattr(whole, name), torch.cat([getattr(c, name) for c in chunks]), atol=0.0, rtol=0.0)
    torch.testing.assert_close(whole.final_equity_scale, chunks[-1].final_equity_scale, atol=0.0, rtol=0.0)
    whole.final_equity_scale.log().backward()
    chunks[-1].final_equity_scale.log().backward()
    torch.testing.assert_close(action.grad, chunk_action.grad, atol=1e-7, rtol=1e-6)


@pytest.mark.parametrize("sign", [-1, 1])
def test_full_exit_preserves_exact_fee_gradient_and_blocks_model_reentry_gradient(sign):
    target = torch.tensor([[0.8]], requires_grad=True)
    held = torch.tensor([sign * 0.5], requires_grad=True)
    result = _run(target, force=torch.ones_like(target, dtype=torch.bool),
                  initial_weights=held, volume=torch.zeros_like(target),
                  announced_exit_unlimited_volume=True)
    result.final_equity_scale.log().backward()
    fee = 0.00055 if sign > 0 else 0.0008
    assert held.grad.item() == pytest.approx(-sign * fee / (1.0 - 0.5 * fee), rel=1e-6)
    assert target.grad.item() == 0.0


def test_compiler_cache_and_contract_distinguish_opt_in_without_gpu_allocation(monkeypatch):
    monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", "1")
    monkeypatch.setattr(crypto, "_DAY_KERNEL_CACHE", {})
    compiled = []
    def fake_compile(function, **kwargs):
        compiled.append(function)
        return function
    monkeypatch.setattr(torch, "compile", fake_compile)
    example = SimpleNamespace(device=torch.device("cuda:0"), dtype=torch.float32, numel=lambda: 7)
    kwargs = dict(block_rows=4, buy_fee_rate=0.0008, sell_fee_rate=0.00055,
                  long_only=False, maximum_gross=1.0, max_turnover_ratio=0.0,
                  stateful_proximal_allocator=False, proximal_cost_multiplier=1.0)
    old, _ = crypto._resolve_block_kernel(example, **kwargs)
    new, _ = crypto._resolve_block_kernel(example, **kwargs, announced_exit_unlimited_volume=True)
    repeat, _ = crypto._resolve_block_kernel(example, **kwargs)
    assert old is repeat and old is not new and len(compiled) == 2
    assert crypto.CRYPTO_PERPETUAL_BACKTEST_CONTRACT_VERSION == 5
    assert crypto.CRYPTO_PERPETUAL_ANNOUNCED_EXIT_BACKTEST_CONTRACT_VERSION == 6


@pytest.mark.skipif(os.environ.get("STOCKAGENT_TEST_CRYPTO_CUDA") != "1" or not torch.cuda.is_available(),
                    reason="explicit bounded CUDA ledger check only")
@pytest.mark.filterwarnings("ignore:The .grad attribute of a Tensor that is not a leaf Tensor:UserWarning")
def test_compiled_cuda_opt_in_matches_eager_values_and_gradients(monkeypatch):
    outputs, gradients = [], []
    for enabled in ("0", "1"):
        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", enabled)
        target, data, state = _random_case(device="cuda")
        target.requires_grad_()
        result = _run(target, **data, **state, announced_exit_unlimited_volume=True)
        result.final_equity_scale.log().backward()
        outputs.append(result)
        gradients.append(target.grad.detach().clone())
    for field in fields(outputs[0]):
        torch.testing.assert_close(getattr(outputs[0], field.name), getattr(outputs[1], field.name), atol=1e-6, rtol=1e-6)
    torch.testing.assert_close(gradients[0], gradients[1], atol=2e-6, rtol=2e-5)
