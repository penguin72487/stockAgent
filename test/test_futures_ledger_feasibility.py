"""Whole-contract feasibility and boundary gradients of the daily ledger."""

from dataclasses import fields

import pytest
import torch

from stockagent.backtest.tw_futures_portfolio import (
    _absolute_cost_with_intent,
    _globally_funded_group_candidate_indices_impl,
    _positive_inventory_half_slope,
    _project_margin_position_limit_axes,
    run_tw_futures_portfolio_integer_torch,
)
from stockagent.data import tw_futures_margin as margin


def _execution(*, margin_mode=False, tier=0, capacity=1000, fee=0.0):
    width = margin.MARGIN_EXECUTION_WIDTH if margin_mode else 11
    tape = torch.zeros(1, 1, width)
    tape[..., 1] = 1
    tape[..., 3:5] = 1000
    tape[..., 5] = fee
    tape[..., 8] = capacity
    tape[..., 10] = tier
    if margin_mode:
        for field in (margin.INITIAL, margin.END_INITIAL, margin.PREVIOUS_INITIAL):
            tape[..., field] = 100
        for field in (
            margin.MAINTENANCE, margin.END_MAINTENANCE, margin.PREVIOUS_MAINTENANCE,
        ):
            tape[..., field] = 75
        tape[..., margin.PREVIOUS_MARK] = tape[..., margin.TERMINAL_MARK] = 1000
        for field in (
            margin.CAN_BUY, margin.CAN_SELL,
            margin.TERMINAL_CAN_BUY, margin.TERMINAL_CAN_SELL,
        ):
            tape[..., field] = 1
        tape[..., margin.POSITION_UNIT] = 1
        tape[..., margin.POSITION_LIMIT] = 10000
        tape[..., margin.LIQUIDATION_RATIO] = 0.25
        tape[..., margin.TERMINAL_CAPACITY] = 1000
    return tape


def _run(action, tape, *, capital=1_000_000.0, initial_quantity=0):
    return run_tw_futures_portfolio_integer_torch(
        torch.as_tensor(action, dtype=torch.float32).reshape(1, 1), tape,
        initial_capital=capital,
        initial_quantities=torch.tensor([initial_quantity]),
        recoverable_backward=True,
    )


@pytest.mark.parametrize("tier", [0, 1])
@pytest.mark.parametrize("capacity", [0, 1, 2, 5, 10, 20, 50, 500])
@pytest.mark.parametrize("direction", [-1, 1])
def test_small_capacity_executes_feasible_partial_target(tier, capacity, direction):
    # The old frontier had only 500..78/0 standard lots, or 500/0 mini lots.
    # A request for 500 lots must still fill the available 1..50 whole lots.
    result = _run(direction * 0.5, _execution(tier=tier, capacity=capacity))
    assert result.contract_quantities_history.item() == direction * capacity
    assert result.final_alive
    assert result.strategy_returns.item() == 0


@pytest.mark.parametrize("initial,target,capacity,expected", [
    (5, -0.5, 2, 3), (5, -0.5, 5, 0), (5, -0.5, 7, -2),
    (-5, 0.5, 2, -3), (-5, 0.5, 7, 2), (5, 0.5, 2, 7),
])
def test_partial_delta_preserves_carry_and_consumes_reversal_capacity(
    initial, target, capacity, expected,
):
    result = _run(
        target, _execution(capacity=capacity, fee=40), initial_quantity=initial,
    )
    assert result.contract_quantities_history.item() == expected
    assert result.final_alive
    assert result.final_equity_scale.item() == pytest.approx(
        1 - abs(expected - initial) * 40 / 1_000_000, abs=1e-7,
    )


def test_paired_standard_and_mini_use_their_own_capacity():
    tape = _execution().expand(1, 2, 11).clone()
    tape[0, :, 3:5] = torch.tensor([[10000.0, 10000.0], [1000.0, 1000.0]])
    tape[0, :, 8] = torch.tensor([1, 2])
    tape[0, :, 10] = torch.tensor([0, 1])
    result = run_tw_futures_portfolio_integer_torch(
        torch.tensor([[0.5, 0.0]]), tape, initial_capital=1_000_000,
    )
    assert result.contract_quantities_history.tolist() == [[1, 2]]
    assert result.final_alive


@pytest.mark.parametrize("advance,executable", [(False, True), (True, False)])
def test_padding_or_blocked_market_never_partially_trades(advance, executable):
    tape = _execution(capacity=2, fee=40)
    tape[..., 1] = executable
    result = run_tw_futures_portfolio_integer_torch(
        torch.tensor([[-0.5]]), tape, initial_capital=1_000_000,
        initial_quantities=torch.tensor([5]),
        state_advance_mask=torch.tensor([advance]),
    )
    assert result.contract_quantities_history.item() == 5
    assert result.final_equity_scale.item() == 1
    assert result.final_alive


@pytest.mark.parametrize("direction", [-1, 1])
def test_partial_margin_target_keeps_direction_gate_and_margin_budget(direction):
    tape = _execution(margin_mode=True, capacity=2)
    result = _run(direction * 0.9, tape, capital=1000)
    assert result.contract_quantities_history.item() == direction * 2
    assert result.margin_audit_history[0, 3].item() == 200
    blocked = tape.clone()
    blocked[..., margin.CAN_BUY if direction > 0 else margin.CAN_SELL] = 0
    denied = _run(direction * 0.9, blocked, capital=1000)
    assert denied.contract_quantities_history.item() == 0
    assert denied.final_alive


def test_margin_forced_close_remains_reduction_only_with_residual_carry():
    # Existing five lots require 375 maintenance but equity is only 300.
    # The new 90%-long target cannot override the forced close of two lots.
    result = _run(
        0.9, _execution(margin_mode=True, capacity=2),
        capital=300, initial_quantity=5,
    )
    assert result.contract_quantities_history.item() == 3
    assert result.residual_contract_quantities_history.item() == 3
    assert result.default_reason_history.item() == 0
    assert result.margin_audit_history[0, 10].item() == 3
    assert result.final_weights.item() == 3
    assert result.final_equity_scale.item() == 1
    assert result.final_alive


def test_margin_position_limit_is_checked_after_partial_fill():
    tape = _execution(margin_mode=True, capacity=8)
    tape[..., margin.POSITION_LIMIT] = 3
    result = _run(0.9, tape, capital=1000)
    assert result.contract_quantities_history.item() == 3
    assert result.margin_audit_history[0, 3].item() == 300
    assert result.final_alive


@pytest.mark.parametrize("capital", [1_000_000.0, 10_000_000.0])
@pytest.mark.parametrize("overspend", [0.0, 1.0])
def test_one_dollar_unaffordable_order_stays_cash_instead_of_default(capital, overspend):
    tape = _execution(fee=40)
    tape[..., 3:5] = capital - 80 + overspend
    result = _run(1.0, tape, capital=capital)
    assert result.contract_quantities_history.item() == (1 if overspend == 0 else 0)
    assert result.default_reason_history.item() == 0
    assert result.final_alive


def test_funding_selector_never_uses_numeric_epsilon_as_spendable_cash():
    budget = torch.tensor(1_000_000.0)
    cash = torch.tensor([[1_000_001.0, 1_000_000.0, 0.0, 0.0]])
    chosen, fundable = _globally_funded_group_candidate_indices_impl(
        cash_required=cash,
        candidate_exposure=torch.tensor([[1_000_000.0, 999_999.0, 0.0, 0.0]]),
        target_exposure=budget.reshape(1), capacity_ok=torch.ones_like(cash, dtype=torch.bool),
        target_cash=budget.reshape(1), equity=budget, target_candidate_count=2,
    )
    assert fundable
    assert chosen.item() == 1
    assert cash[0, chosen.item()] <= budget


def test_random_partial_groups_respect_capacity_and_all_costs():
    generator = torch.Generator().manual_seed(1843)
    for _ in range(64):
        groups = 5
        slots = groups * 2
        capital = 1_000_000
        tape = _execution(fee=40).expand(1, slots, 11).clone()
        prices = torch.randint(1000, 50000, (slots,), generator=generator).float()
        tape[0, :, 3] = tape[0, :, 4] = prices
        caps = torch.randint(0, 20, (slots,), generator=generator)
        tape[0, :, 8] = caps
        group_ids = torch.arange(groups).repeat_interleave(2)
        tape[0, :, 9] = group_ids
        tape[0, :, 10] = torch.arange(slots) % 2
        budget = torch.rand(groups, generator=generator)
        budget = budget / budget.sum()
        direction = torch.where(torch.rand(groups, generator=generator) > 0.5, 1, -1)
        action = torch.zeros(1, slots)
        action[0, ::2] = budget * direction
        result = run_tw_futures_portfolio_integer_torch(
            action, tape, initial_capital=capital,
        )
        quantity = result.contract_quantities_history[0]
        assert result.final_alive  # Staying flat is always feasible here.
        assert torch.all(quantity.abs() <= caps)
        cash = quantity.abs() * (prices + 80)  # entry + close reserve
        group_cash = torch.zeros(groups).scatter_add(0, group_ids, cash)
        assert torch.all(group_cash <= budget * capital)
        assert cash.sum() <= capital


@pytest.mark.parametrize("direction", [-1, 1])
@pytest.mark.parametrize("profit_per_lot", [1.0, 100.0])
def test_subcontract_gradient_includes_both_sides_of_cost(direction, profit_per_lot):
    tape = _execution(fee=40)
    tape[..., 2] = 1  # A completed round trip includes the real close fee.
    tape[..., 3] = 100000
    tape[..., 4] = 100000 + direction * profit_per_lot
    action = torch.tensor(direction * 0.005, requires_grad=True)
    result = _run(action, tape)
    (-result.strategy_returns.sum()).backward()
    assert result.contract_quantities_history.item() == 0
    assert result.strategy_returns.item() == 0
    # No trade forward; backward shrinks a first lot that loses after 80 fees,
    # and increases a first lot whose 100 gross profit exceeds those fees.
    signed_slope = action.grad.item() * direction
    assert signed_slope > 0 if profit_per_lot < 80 else signed_slope < 0


@pytest.mark.parametrize("profit_per_lot", [-10000.0, 0.0, 10000.0])
def test_exact_cash_action_has_unbiased_signed_recovery_gradient(profit_per_lot):
    tape = _execution(fee=40)
    tape[..., 2] = 1
    tape[..., 3] = 100000
    tape[..., 4] = 100000 + profit_per_lot
    action = torch.tensor(0.0, requires_grad=True)
    result = _run(action, tape, capital=10_000_000)
    (-result.strategy_returns.sum()).backward()
    assert result.strategy_returns.item() == 0
    assert result.contract_quantities_history.item() == 0
    if profit_per_lot == 0:
        assert action.grad.item() == 0
    else:
        assert action.grad.item() * profit_per_lot < 0


def test_zero_delta_fee_shadow_preserves_forward_and_nonzero_derivatives():
    executed = torch.tensor([-2.0, 0.0, 0.0, 0.0, 2.0], requires_grad=True)
    intent = torch.tensor([9.0, -1.0, 0.0, 1.0, -9.0])
    cost = _absolute_cost_with_intent(executed, intent)
    assert torch.equal(cost, executed.abs())
    cost.sum().backward()
    assert executed.grad.tolist() == [-1.0, -1.0, 0.0, 1.0, 1.0]


@pytest.mark.parametrize("margin_mode", [False, True])
def test_all_public_forward_account_fields_are_bitwise_exact_with_gradients(margin_mode):
    generator = torch.Generator().manual_seed(93814)
    for _ in range(24):
        rows, slots = 7, 12
        tape = _execution(margin_mode=margin_mode, fee=40).expand(rows, slots, -1).clone()
        prices = torch.randint(1000, 90000, (slots,), generator=generator).float()
        tape[..., 3] = prices
        tape[..., 4] = prices * (1 + 0.025 * torch.randn(rows, slots, generator=generator))
        tape[..., 0] = (tape[..., 4] / tape[..., 3]).log()
        tape[..., 6:8] = 2
        tape[..., 8] = torch.randint(0, 20, (rows, slots), generator=generator)
        tape[..., 9] = torch.arange(slots)
        if margin_mode:
            tape[..., margin.PREVIOUS_MARK] = tape[..., 3]
            tape[..., margin.TERMINAL_MARK] = tape[..., 4]
            tape[..., margin.POSITION_GROUP] = torch.arange(slots)
        weights = torch.randn(rows, slots, generator=generator)
        weights = 0.9 * weights / weights.abs().sum(-1, keepdim=True)
        exact = run_tw_futures_portfolio_integer_torch(
            weights, tape, initial_capital=1_000_000, recoverable_backward=True,
        )
        trained = run_tw_futures_portfolio_integer_torch(
            weights.requires_grad_(), tape, initial_capital=1_000_000,
            recoverable_backward=True,
        )
        for field in fields(exact):
            if field.name.startswith("_"):
                continue  # Internal differentiable shadow exists only in training.
            expected, actual = getattr(exact, field.name), getattr(trained, field.name)
            if isinstance(expected, torch.Tensor):
                assert torch.equal(expected, actual), field.name
            else:
                assert expected is actual, field.name
        (-trained.strategy_returns.sum()).backward()
        assert torch.isfinite(weights.grad).all()


@pytest.mark.parametrize("action_value", [float("nan"), float("inf"), -float("inf")])
def test_sanitized_nonfinite_actions_keep_exact_forward_and_finite_backward(action_value):
    tape = _execution(fee=40)
    tape[..., 4] = 1100
    action = torch.tensor(action_value, requires_grad=True)
    result = _run(action, tape)
    assert result.strategy_returns.item() == 0
    assert result.final_alive
    (-result.strategy_returns.sum()).backward()
    assert action.grad.item() == 0


def test_signed_inventory_split_is_identity_in_value_and_gradient():
    integer = torch.tensor([-50, 0, 50], dtype=torch.int64)
    assert _positive_inventory_half_slope(integer).dtype == integer.dtype
    assert torch.equal(_positive_inventory_half_slope(integer), integer.clamp_min(0))
    quantity = torch.tensor([-50., -1., 0., 1., 50.], requires_grad=True)
    positive = _positive_inventory_half_slope(quantity)
    negative = _positive_inventory_half_slope(-quantity)
    assert torch.equal(positive, quantity.clamp_min(0))
    assert torch.equal(negative, (-quantity).clamp_min(0))
    (positive - negative).sum().backward()
    assert torch.equal(quantity.grad, torch.ones_like(quantity))


@pytest.mark.parametrize('rows', [1, 128])
def test_unconstrained_zero_inventory_carry_has_unit_tangent(rows):
    # Carrying an unchanged zero position must not multiply its sensitivity
    # every day. The old dual-clamp split produced 2**rows (FP32 overflow).
    tape = torch.zeros(3, margin.MARGIN_GRANDFATHER_EXECUTION_WIDTH)
    group = torch.arange(3)
    for field in (margin.POSITION_GROUP, margin.SECOND_POSITION_GROUP):
        tape[:, field] = group
    for field in (margin.POSITION_UNIT, margin.SECOND_POSITION_UNIT):
        tape[:, field] = 1
    for field in (margin.POSITION_LIMIT, margin.SECOND_POSITION_LIMIT):
        tape[:, field] = 1000
    seed = torch.tensor([-1., 0., 1.], requires_grad=True)
    current = seed
    for _ in range(rows):
        current, failed, _ = _project_margin_position_limit_axes(
            current, current, execution_row=tape, position_group=group,
            position_units=torch.ones(3), group_limits=torch.full((3,), 1000.),
            close_capacity=torch.full((3,), 1000.), whole_contracts=False,
        )
        assert not failed.any()
    assert torch.equal(current, seed)
    current.sum().backward()
    assert torch.equal(seed.grad, torch.ones_like(seed))
