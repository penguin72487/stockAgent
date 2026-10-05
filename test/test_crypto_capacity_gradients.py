"""The continuous crypto executor differentiates its actual signed clamp."""

from dataclasses import fields
import os

import pytest
import torch

import stockagent.backtest.crypto_perpetual as crypto


def _one_day(target, *, capacity=0.25, buy=True, sell=True, short=True,
             policy=True, announced=False, advance=True, alive=True,
             initial=0.0, long_only=False, proximal=True):
    yes = torch.ones_like(target, dtype=torch.bool)
    mask = lambda flag: yes if flag else ~yes
    return crypto.run_crypto_perpetual_torch(
        target, torch.log1p(torch.full_like(target, 0.05)),
        torch.log1p(torch.full_like(target, 0.05)),
        mask(policy), mask(buy), mask(sell), mask(short), mask(announced),
        buy_fee_rate=0.0, sell_fee_rate=0.0, long_only=long_only,
        maximum_gross=1.0, volume_limit_weights=torch.full_like(target, capacity),
        initial_weights=torch.full((target.shape[1],), initial),
        initial_alive=torch.tensor(alive),
        state_advance_mask=torch.full((target.shape[0],), advance),
        stateful_proximal_allocator=proximal,
    )


@pytest.mark.parametrize("capacity", [0.25, float("inf")])
@pytest.mark.parametrize("proximal", [False, True])
@pytest.mark.parametrize("rows", [1, 5])
def test_flat_cash_gradient_matches_true_local_derivative(capacity, proximal, rows):
    target = torch.zeros((rows, 1), requires_grad=True)
    value = _one_day(target, capacity=capacity, proximal=proximal).strategy_simple_returns.sum()
    value.backward()
    torch.testing.assert_close(target.grad, torch.full_like(target, 0.05))
    # Numerical derivative of the actual executed account, not a surrogate.
    epsilon = 1e-4
    for row in range(rows):
        change = torch.zeros_like(target)
        change[row, 0] = epsilon
        plus = _one_day(change, capacity=capacity, proximal=proximal).strategy_simple_returns.sum()
        minus = _one_day(-change, capacity=capacity, proximal=proximal).strategy_simple_returns.sum()
        assert target.grad[row, 0].item() == pytest.approx(
            ((plus - minus) / (2 * epsilon)).item(), rel=2e-5, abs=1e-7,
        )


@pytest.mark.parametrize("options", [
    {"capacity": 0.0},
    {"buy": False, "sell": False, "short": False},
    {"buy": False, "sell": True, "short": False},
    {"buy": False, "sell": True, "short": True, "long_only": True},
    {"policy": False},
    {"announced": True},
    {"advance": False},
    {"alive": False},
])
def test_constant_zero_execution_has_no_phantom_target_gradient(options):
    target = torch.zeros((1, 1), requires_grad=True)
    result = _one_day(target, **options)
    result.strategy_simple_returns.sum().backward()
    assert result.executed_weights.item() == 0.0
    assert target.grad.item() == 0.0


@pytest.mark.parametrize("side", ["buy", "short"])
def test_one_sided_zero_boundary_uses_a_legal_directional_derivative(side):
    options = dict(buy=side == "buy", sell=side == "short", short=side == "short")
    target = torch.zeros((1, 1), requires_grad=True)
    _one_day(target, **options).strategy_simple_returns.sum().backward()
    direction = 1.0 if side == "buy" else -1.0
    allowed = _one_day(torch.tensor([[direction * 1e-4]]), **options)
    forbidden = _one_day(torch.tensor([[-direction * 1e-4]]), **options)
    assert target.grad.item() == pytest.approx(0.05)
    assert allowed.strategy_simple_returns.item() / (direction * 1e-4) == pytest.approx(0.05)
    assert forbidden.executed_weights.item() == 0.0
    # No unique two-sided derivative is claimed at this permission kink.


@pytest.mark.parametrize("initial", [-0.2, 0.2])
def test_announcement_does_not_reenable_model_gradient_or_erase_residual(initial):
    target = torch.zeros((1, 1), requires_grad=True)
    result = _one_day(target, initial=initial, announced=True, policy=False, capacity=0.05)
    result.strategy_simple_returns.sum().backward()
    assert target.grad.item() == 0.0
    assert result.executed_weights.item() == pytest.approx(initial * 0.75)


def test_short_open_permission_cannot_differentiate_a_forbidden_long_reduction():
    # Exercise the day kernel directly as well: the public runner normally
    # intersects short-open with sell evidence, but the local permission rule
    # must not depend on that sanitization to distinguish a long from a short.
    day = crypto._day_kernel_factory(
        buy_fee_rate=0.0, sell_fee_rate=0.0, long_only=False, maximum_gross=1.0,
        max_turnover_ratio=0.0, stateful_proximal_allocator=False,
        proximal_cost_multiplier=1.0,
    )
    yes, no = torch.tensor([True]), torch.tensor([False])

    def value(target):
        return day(
            torch.tensor([0.2]), torch.tensor(True), torch.tensor(1.0, dtype=torch.float64),
            target, torch.tensor([0.05]), torch.tensor([0.05]), yes, yes,
            yes, no, no, yes, no, torch.tensor([0.25]), torch.tensor(True),
        )[3]

    target = torch.tensor([0.2], requires_grad=True)
    value(target).backward()
    assert target.grad.item() == 0.0
    assert value(torch.tensor([0.2001])).item() == value(torch.tensor([0.1999])).item()


@pytest.mark.parametrize("capacity", [0.0, 0.1, 1.0, float("inf")])
@pytest.mark.parametrize("buy,down", [(False, False), (False, True), (True, False), (True, True)])
def test_capacity_map_preserves_old_values_and_signed_zero(capacity, buy, down):
    delta = torch.tensor([-2.0, -0.1, -0.0, 0.0, 0.1, 2.0])
    # The helper receives orders after the existing side-permission gates.
    delta = torch.where(((delta > 0) & ~torch.tensor(buy)) |
                        ((delta < 0) & ~torch.tensor(down)), 0.0, delta)
    cap = torch.full_like(delta, capacity)
    actual = crypto._capacity_clamp(delta, cap, torch.full_like(delta, buy, dtype=torch.bool),
                                    torch.full_like(delta, down, dtype=torch.bool))
    legacy = delta.sign() * torch.minimum(delta.abs(), cap)
    assert torch.equal(actual, legacy)
    assert torch.equal(torch.signbit(actual), torch.signbit(legacy))


@pytest.mark.parametrize("proximal", [False, True])
@pytest.mark.parametrize("long_only", [False, True])
def test_random_account_is_bitwise_equal_to_legacy_capacity_forward(monkeypatch, proximal, long_only):
    generator = torch.Generator().manual_seed(617)
    shape = (11, 7)  # Compiled-size blocks plus an eager tail on the CPU oracle.
    target = torch.randn(shape, generator=generator) * 0.25
    target[0] = 0.0
    target[1, :2] = torch.tensor([-0.0, 0.0])
    price = torch.randn(shape, generator=generator) * 0.025
    effective = price - 0.0003
    policy = torch.rand(shape, generator=generator) > 0.15
    buy = torch.rand(shape, generator=generator) > 0.2
    sell = torch.rand(shape, generator=generator) > 0.2
    short = torch.rand(shape, generator=generator) > 0.2
    announced = torch.zeros(shape, dtype=torch.bool)
    announced[8:, 0] = True
    policy[8:, 0] = False
    capacity = torch.rand(shape, generator=generator) * 0.12
    capacity[:, 1] = 0.0
    capacity[:, 2] = float("inf")
    advance = torch.ones(shape[0], dtype=torch.bool)
    advance[7] = False

    def run():
        return crypto.run_crypto_perpetual_torch(
            target, torch.log1p(effective), torch.log1p(price),
            policy, buy, sell, short, announced,
            buy_fee_rate=0.00055, sell_fee_rate=0.0006, long_only=long_only,
            maximum_gross=0.8, max_turnover_ratio=0.15,
            stateful_proximal_allocator=proximal, volume_limit_weights=capacity,
            state_advance_mask=advance,
        )

    actual = run()
    monkeypatch.setattr(crypto, "_capacity_clamp",
                        lambda delta, capacity, *_: delta.sign() * torch.minimum(delta.abs(), capacity))
    legacy = run()
    for field in fields(actual):
        left, right = getattr(actual, field.name), getattr(legacy, field.name)
        assert torch.equal(left, right), field.name
        if left.dtype != torch.bool:
            assert torch.equal(torch.signbit(left), torch.signbit(right)), field.name


def test_capacity_derivative_matches_signed_clamp_away_from_boundaries():
    target = torch.tensor([[-0.2, 0.0, 0.2]], requires_grad=True)
    result = _one_day(target, capacity=0.1)
    result.strategy_simple_returns.sum().backward()
    torch.testing.assert_close(target.grad, torch.tensor([[0.0, 0.05, 0.0]]))


def test_gradient_contract_is_separate_from_unchanged_forward_contract():
    assert crypto.CRYPTO_PERPETUAL_BACKTEST_CONTRACT_VERSION == 5
    assert crypto.CRYPTO_PERPETUAL_BACKWARD_CONTRACT_VERSION >= 1


@pytest.mark.skipif(os.environ.get("STOCKAGENT_TEST_CRYPTO_CUDA") != "1" or not torch.cuda.is_available(),
                    reason="explicit bounded CUDA ledger check only")
@pytest.mark.filterwarnings("ignore:The .grad attribute of a Tensor that is not a leaf Tensor:UserWarning")
def test_compiled_zero_trade_gradient_matches_eager_and_analytic_derivative(monkeypatch):
    values, gradients = [], []
    for compiled in ("0", "1"):
        monkeypatch.setenv("STOCKAGENT_BACKTEST_COMPILE", compiled)
        target = torch.zeros((5, 3), device="cuda", requires_grad=True)
        yes = torch.ones_like(target, dtype=torch.bool)
        capacity = torch.tensor([0.25, 0.0, float("inf")], device="cuda").expand_as(target)
        result = crypto.run_crypto_perpetual_torch(
            target, torch.log1p(torch.full_like(target, 0.05)),
            torch.log1p(torch.full_like(target, 0.05)), yes, yes, yes, yes, ~yes,
            buy_fee_rate=0.0, sell_fee_rate=0.0, long_only=False,
            maximum_gross=1.0, volume_limit_weights=capacity,
            stateful_proximal_allocator=True,
        )
        result.final_equity_scale.log().backward()
        gradients.append(target.grad)
        values.append(result)
    expected = torch.tensor([0.05, 0.0, 0.05], device="cuda").expand_as(target)
    for gradient in gradients:
        assert torch.isfinite(gradient).all()
        torch.testing.assert_close(gradient, expected)
    for field in fields(values[0]):
        torch.testing.assert_close(getattr(values[0], field.name), getattr(values[1], field.name))
