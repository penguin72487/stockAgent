from __future__ import annotations

import math

import pytest
import torch

from stockagent.backtest.tw_dual_session import (
    CLOSE,
    OPEN,
    OVERNIGHT_ENTRY_AT_CLOSE,
    OVERNIGHT_ENTRY_AT_OPEN,
    OVERNIGHT_EXIT_AT_OPEN,
    run_tw_cash_dual_session,
    run_tw_overnight_dual_session,
)


def _phase_mask(rows: int, symbols: int = 1) -> torch.Tensor:
    return torch.ones((rows, 2, symbols), dtype=torch.bool)


def _cash(
    actions: torch.Tensor,
    *,
    overnight: torch.Tensor | None = None,
    intraday: torch.Tensor | None = None,
    tradable: torch.Tensor | None = None,
    can_buy: torch.Tensor | None = None,
    can_sell: torch.Tensor | None = None,
    buy_fees: torch.Tensor | float = 0.0,
    sell_fees: torch.Tensor | float = 0.0,
    **kwargs: object,
):
    rows, _, symbols = actions.shape
    returns = torch.zeros((rows, symbols), dtype=actions.dtype)
    masks = _phase_mask(rows, symbols)
    return run_tw_cash_dual_session(
        actions,
        returns if overnight is None else overnight,
        returns if intraday is None else intraday,
        masks if tradable is None else tradable,
        masks if can_buy is None else can_buy,
        masks if can_sell is None else can_sell,
        buy_fees,
        sell_fees,
        **kwargs,
    )


def _overnight(
    actions: torch.Tensor,
    *,
    overnight: torch.Tensor | None = None,
    intraday: torch.Tensor | None = None,
    tradable: torch.Tensor | None = None,
    can_buy: torch.Tensor | None = None,
    can_sell: torch.Tensor | None = None,
    buy_fees: torch.Tensor | float = 0.0,
    sell_fees: torch.Tensor | float = 0.0,
    **kwargs: object,
):
    rows, _, symbols = actions.shape
    returns = torch.zeros((rows, symbols), dtype=actions.dtype)
    masks = _phase_mask(rows, symbols)
    return run_tw_overnight_dual_session(
        actions,
        returns if overnight is None else overnight,
        returns if intraday is None else intraday,
        masks if tradable is None else tradable,
        masks if can_buy is None else can_buy,
        masks if can_sell is None else can_sell,
        buy_fees,
        sell_fees,
        **kwargs,
    )


def test_overnight_rejects_opposite_open_close_entry_directions() -> None:
    actions = torch.tensor(
        [[[0.5], [0.75], [-0.25]]],
        dtype=torch.float64,
    )

    with pytest.raises(
        ValueError,
        match="may not reverse the same symbol within one session",
    ):
        _overnight(actions)


def test_cash_marks_overnight_before_open_and_intraday_after_open() -> None:
    # A carried half-NAV holding doubles before the open, is sold at the open,
    # and therefore earns exactly 50% at the account level.
    carried = torch.zeros((1, 2, 1), dtype=torch.float64)
    carried_result = _cash(
        carried,
        overnight=torch.tensor([[math.log(2.0)]], dtype=torch.float64),
        initial_weights=torch.tensor([0.5], dtype=torch.float64),
        initial_cash=torch.tensor(0.5, dtype=torch.float64),
    )
    torch.testing.assert_close(
        carried_result.strategy_returns,
        torch.tensor([math.log(1.5)], dtype=torch.float64),
    )
    torch.testing.assert_close(
        carried_result.executed_long_sell_weights[0, OPEN],
        torch.tensor([2.0 / 3.0], dtype=torch.float64),
    )

    # A new open purchase does not receive the already-finished overnight gap;
    # it receives only open-to-close return.
    opened = torch.zeros((1, 2, 1), dtype=torch.float64)
    opened[0, OPEN, 0] = 0.5
    opened_result = _cash(
        opened,
        overnight=torch.tensor([[math.log(9.0)]], dtype=torch.float64),
        intraday=torch.tensor([[math.log(1.1)]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        opened_result.strategy_returns,
        torch.tensor([math.log(1.05)], dtype=torch.float64),
    )


def test_cash_gap_normalizes_turnover_and_execution_audit_by_open_nav() -> None:
    actions = torch.zeros((2, 2, 1), dtype=torch.float64)
    actions[0, CLOSE, 0] = 0.5
    result = _cash(
        actions,
        overnight=torch.tensor(
            [[0.0], [math.log(2.0)]],
            dtype=torch.float64,
        ),
    )

    expected_exit_weight = torch.tensor(2.0 / 3.0, dtype=torch.float64)
    torch.testing.assert_close(
        result.turnovers,
        torch.tensor([0.5, 2.0 / 3.0], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.event_turnovers[1, OPEN],
        expected_exit_weight,
    )
    torch.testing.assert_close(
        result.executed_long_sell_weights[1, OPEN, 0],
        expected_exit_weight,
    )
    torch.testing.assert_close(
        result.strategy_returns,
        torch.tensor([0.0, math.log(1.5)], dtype=torch.float64),
    )


def test_cash_shifts_t_plus_two_once_and_enqueues_one_daily_net_claim() -> None:
    actions = torch.full((3, 2, 1), 0.2, dtype=torch.float64)
    actions[0, OPEN, 0] = 0.6
    result = _cash(actions)

    # Day zero buys 0.6 then sells 0.4.  The same-date gross orders net to one
    # 0.2 payable at T+2; it cannot become due on day one.
    torch.testing.assert_close(
        result.event_turnovers[0],
        torch.tensor([0.6, 0.4], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.payables_history,
        torch.tensor(
            [
                [0.0, 0.2],
                [0.2, 0.0],
                [0.0, 0.0],
            ],
            dtype=torch.float64,
        ),
    )
    assert not bool(result.settlement_default.any())
    torch.testing.assert_close(
        result.final_cash, torch.tensor(0.8, dtype=torch.float64)
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([0.2], dtype=torch.float64),
    )


def test_cash_charges_open_and_close_gross_fees_before_daily_netting() -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)
    actions[0, OPEN, 0] = 0.5
    result = _cash(
        actions,
        buy_fees=torch.tensor([[0.01], [0.0]], dtype=torch.float64),
        sell_fees=torch.tensor([[0.0], [0.02]], dtype=torch.float64),
    )

    torch.testing.assert_close(
        result.event_turnovers,
        torch.tensor([[0.5, 0.5]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.equity_scale_history,
        torch.tensor([0.985], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.strategy_returns,
        torch.tensor([math.log(0.985)], dtype=torch.float64),
    )
    # Normalized recurrent state: 0.015 payable divided by remaining 0.985 NAV.
    torch.testing.assert_close(
        result.final_payables,
        torch.tensor([0.0, 0.015 / 0.985], dtype=torch.float64),
    )
    assert torch.count_nonzero(result.final_receivables) == 0


@pytest.mark.parametrize("budget_kind", ["volume", "turnover"])
def test_cash_open_and_close_share_one_daily_budget(budget_kind: str) -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)
    actions[0, OPEN, 0] = 0.5
    kwargs: dict[str, object]
    if budget_kind == "volume":
        kwargs = {"volume_limit_weights": torch.tensor([[0.6]], dtype=torch.float64)}
    else:
        kwargs = {"max_turnover_ratio": 0.6}
    result = _cash(actions, **kwargs)

    # Open consumes 0.5 of the only daily 0.6 budget.  The close can sell just
    # 0.1, not a fresh phase-local 0.5.
    torch.testing.assert_close(
        result.event_turnovers,
        torch.tensor([[0.5, 0.1]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([0.4], dtype=torch.float64),
    )


def test_cash_share_volume_budget_revalues_at_each_auction() -> None:
    # 0.5 is fifty shares valued at the prior close relative to unit capital.
    # Buying them at OPEN exhausts the share budget; their notional doubles by
    # CLOSE but no additional shares become available.
    open_actions = torch.ones((1, 2, 1), dtype=torch.float64)
    opened = _cash(
        open_actions,
        intraday=torch.tensor([[math.log(2.0)]], dtype=torch.float64),
        volume_limit_weights=torch.tensor([[0.5]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        opened.event_turnovers,
        torch.tensor([[0.5, 0.0]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        opened.strategy_returns,
        torch.tensor([math.log(1.5)], dtype=torch.float64),
    )
    torch.testing.assert_close(
        opened.final_weights,
        torch.tensor([2.0 / 3.0], dtype=torch.float64),
    )

    # If the same fifty shares are bought only at CLOSE after the price
    # doubles, their executable notional is 1.0 rather than the stale 0.5.
    close_actions = torch.zeros((1, 2, 1), dtype=torch.float64)
    close_actions[0, CLOSE, 0] = 1.0
    closed = _cash(
        close_actions,
        intraday=torch.tensor([[math.log(2.0)]], dtype=torch.float64),
        volume_limit_weights=torch.tensor([[0.5]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        closed.event_turnovers,
        torch.tensor([[0.0, 1.0]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        closed.final_weights,
        torch.tensor([1.0], dtype=torch.float64),
    )


def test_cash_short_share_inventory_revalues_at_close() -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)
    actions[0, CLOSE, 0] = -1.0
    masks = _phase_mask(1)
    result = _cash(
        actions,
        intraday=torch.tensor([[math.log(2.0)]], dtype=torch.float64),
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        # Fifty shares valued at 0.5 on the prior-close reference become 1.0
        # notional at this closing auction after the price doubles.
        short_capacity_weights=torch.tensor([[0.5]], dtype=torch.float64),
    )

    torch.testing.assert_close(
        result.executed_short_open_weights[0, CLOSE],
        torch.tensor([1.0], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([-1.0], dtype=torch.float64),
    )


def test_cash_disabled_short_capacity_stays_unbounded_across_gap() -> None:
    actions = torch.full(
        (1, 2, 1),
        -0.8,
        dtype=torch.float64,
        requires_grad=True,
    )
    masks = _phase_mask(1)
    result = _cash(
        actions,
        overnight=torch.tensor([[math.log(0.5)]], dtype=torch.float64),
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.full(
            (1, 1),
            float("inf"),
            dtype=torch.float64,
        ),
    )

    torch.testing.assert_close(
        result.executed_short_open_weights[0, OPEN],
        torch.tensor([0.8], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([-0.8], dtype=torch.float64),
    )
    objective = (
        -result.strategy_returns.sum()
        + 0.1 * result.turnovers.sum()
        + 0.01 * result.final_weights.square().sum()
    )
    objective.backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()


@pytest.mark.parametrize(
    ("runner", "phases"),
    [
        (_cash, 2),
        (_overnight, 3),
    ],
)
def test_disabled_short_capacity_has_finite_recurrent_zero_action_gradient(
    runner,
    phases: int,
) -> None:
    actions = torch.zeros(
        (2, phases, 2),
        dtype=torch.float64,
        requires_grad=True,
    )
    masks = _phase_mask(2, symbols=2)
    result = runner(
        actions,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.full(
            (2, 2),
            float("inf"),
            dtype=torch.float64,
        ),
    )

    objective = (
        result.strategy_returns.sum()
        + result.turnovers.sum()
        + result.final_equity_scale
        + result.final_weights.square().sum()
    )
    objective.backward()

    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()


def test_disabled_short_capacity_has_finite_price_ratio_gradients() -> None:
    actions = torch.zeros((2, 3, 2), dtype=torch.float64)
    overnight = torch.zeros(
        (2, 2),
        dtype=torch.float64,
        requires_grad=True,
    )
    intraday = torch.zeros(
        (2, 2),
        dtype=torch.float64,
        requires_grad=True,
    )
    masks = _phase_mask(2, symbols=2)
    result = _overnight(
        actions,
        overnight=overnight,
        intraday=intraday,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.full(
            (2, 2),
            float("inf"),
            dtype=torch.float64,
        ),
    )

    objective = (
        result.strategy_returns.sum()
        + result.turnovers.sum()
        + result.final_equity_scale
        + result.final_weights.square().sum()
    )
    overnight_grad, intraday_grad = torch.autograd.grad(
        objective,
        (overnight, intraday),
    )

    assert torch.isfinite(overnight_grad).all()
    assert torch.isfinite(intraday_grad).all()


@pytest.mark.parametrize(
    ("runner", "phases"),
    [
        (_cash, 2),
        (_overnight, 3),
    ],
)
def test_nonfinite_open_nav_default_has_finite_action_gradients(
    runner,
    phases: int,
) -> None:
    actions = torch.zeros(
        (1, phases, 1),
        dtype=torch.float32,
        requires_grad=True,
    )
    # Every external state component and price return is finite.  The highly
    # levered short remains a valid normalized recurrent seed within FP32
    # accounting tolerance, then overflows only when the overnight price
    # doubles.  The account must default without evaluating zero model actions
    # against the resulting -inf opening NAV.
    result = runner(
        actions,
        overnight=torch.tensor(
            [[math.log(2.0)]],
            dtype=torch.float32,
        ),
        initial_weights=torch.tensor([-3.0e38], dtype=torch.float32),
        initial_cash=torch.tensor(1.0, dtype=torch.float32),
        initial_payables=torch.zeros(2, dtype=torch.float32),
        initial_receivables=torch.zeros(2, dtype=torch.float32),
        initial_alive=torch.tensor(True),
        initial_equity_scale=torch.tensor(1.0, dtype=torch.float32),
        initial_short_sale_collateral=torch.tensor(
            [1.5e38],
            dtype=torch.float32,
        ),
        initial_short_margin_collateral=torch.tensor(
            [1.5e38],
            dtype=torch.float32,
        ),
        short_maintenance_ratio=1.0,
    )

    assert bool(result.settlement_default[0])
    assert not bool(result.final_alive)
    assert torch.isfinite(result.strategy_returns).all()
    result.strategy_returns.sum().backward()

    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()


@pytest.mark.parametrize(
    ("runner", "phases"),
    [
        (_cash, 2),
        (_overnight, 3),
    ],
)
def test_nonfinite_close_nav_default_has_finite_action_gradients(
    runner,
    phases: int,
) -> None:
    actions = torch.zeros((1, phases, 1), dtype=torch.float32)
    if phases == 2:
        # Preserve the carried P2 long through the opening auction.
        actions[0, OPEN, 0] = 2.0
    actions.requires_grad_()

    result = runner(
        actions,
        intraday=torch.tensor(
            [[math.log(2.0e38)]],
            dtype=torch.float32,
        ),
        # The not-yet-due payable keeps this finite seed normalized at NAV=1.
        # It shifts at the open without becoming payable until the next session,
        # so the two-unit long reaches the intraday price move and overflows.
        initial_weights=torch.tensor([2.0], dtype=torch.float32),
        initial_cash=torch.tensor(0.0, dtype=torch.float32),
        initial_payables=torch.tensor([0.0, 1.0], dtype=torch.float32),
        initial_receivables=torch.zeros(2, dtype=torch.float32),
        initial_alive=torch.tensor(True),
        initial_equity_scale=torch.tensor(1.0, dtype=torch.float32),
    )

    assert bool(result.settlement_default[0])
    assert not bool(result.final_alive)
    assert torch.isfinite(result.strategy_returns).all()
    result.strategy_returns.sum().backward()

    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()


def test_cash_crossing_zero_reduces_existing_side_before_opening_opposite() -> None:
    masks = _phase_mask(2)

    long_to_short = torch.zeros((2, 2, 1), dtype=torch.float64)
    long_to_short[0, CLOSE, 0] = 0.4
    long_to_short[1, OPEN, 0] = -0.4
    long_to_short[1, CLOSE, 0] = 0.2
    long_result = _cash(
        long_to_short,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((2, 1), dtype=torch.float64),
        volume_limit_weights=torch.tensor(
            [[1.0], [0.2]],
            dtype=torch.float64,
        ),
    )
    torch.testing.assert_close(
        long_result.executed_long_sell_weights[1, OPEN],
        torch.tensor([0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        long_result.executed_short_open_weights[1, OPEN],
        torch.zeros(1, dtype=torch.float64),
    )
    torch.testing.assert_close(
        long_result.open_weights_history[1],
        torch.tensor([0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        long_result.final_weights,
        torch.tensor([0.2], dtype=torch.float64),
    )

    short_to_long = torch.zeros((2, 2, 1), dtype=torch.float64)
    short_to_long[0, CLOSE, 0] = -0.4
    short_to_long[1, OPEN, 0] = 0.4
    short_to_long[1, CLOSE, 0] = -0.2
    short_result = _cash(
        short_to_long,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((2, 1), dtype=torch.float64),
        volume_limit_weights=torch.tensor(
            [[1.0], [0.2]],
            dtype=torch.float64,
        ),
    )
    torch.testing.assert_close(
        short_result.executed_short_cover_weights[1, OPEN],
        torch.tensor([0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        short_result.executed_long_buy_weights[1, OPEN],
        torch.zeros(1, dtype=torch.float64),
    )
    torch.testing.assert_close(
        short_result.open_weights_history[1],
        torch.tensor([-0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        short_result.final_weights,
        torch.tensor([-0.2], dtype=torch.float64),
    )


def test_cash_shared_turnover_scales_signed_endpoints_before_leg_split() -> None:
    actions = torch.zeros((2, 2, 2), dtype=torch.float64)
    actions[0, CLOSE] = torch.tensor([0.4, 0.0], dtype=torch.float64)
    actions[1, OPEN] = torch.tensor([-0.4, 0.4], dtype=torch.float64)
    actions[1, CLOSE] = torch.tensor([0.0, 0.2], dtype=torch.float64)
    masks = _phase_mask(2, symbols=2)
    result = _cash(
        actions,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((2, 2), dtype=torch.float64),
        max_turnover_ratio=0.6,
    )

    # The common 0.6 turnover budget scales A's requested 0.8 signed
    # transition and B's 0.4 transition by one half.  A therefore reaches
    # exactly flat before any short can open; B receives the remaining 0.2.
    torch.testing.assert_close(
        result.open_weights_history[1],
        torch.tensor([0.0, 0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.executed_long_sell_weights[1, OPEN],
        torch.tensor([0.4, 0.0], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.executed_short_open_weights[1, OPEN],
        torch.zeros(2, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.executed_long_buy_weights[1, OPEN],
        torch.tensor([0.0, 0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([0.0, 0.2], dtype=torch.float64),
    )


def test_cash_phase_permission_cannot_bypass_reduction_by_crossing_zero() -> None:
    masks = _phase_mask(1)
    blocked_sell = masks.clone()
    blocked_sell[:, :, 0] = False
    long_actions = torch.full((1, 2, 1), -0.4, dtype=torch.float64)
    long_result = _cash(
        long_actions,
        can_sell=blocked_sell,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((1, 1), dtype=torch.float64),
        initial_weights=torch.tensor([0.4], dtype=torch.float64),
        initial_cash=torch.tensor(0.6, dtype=torch.float64),
    )
    torch.testing.assert_close(
        long_result.final_weights,
        torch.tensor([0.4], dtype=torch.float64),
    )
    torch.testing.assert_close(
        long_result.executed_long_sell_weights,
        torch.zeros_like(long_result.executed_long_sell_weights),
    )
    torch.testing.assert_close(
        long_result.executed_short_open_weights,
        torch.zeros_like(long_result.executed_short_open_weights),
    )

    blocked_buy = _phase_mask(2)
    blocked_buy[:, :, 0] = False
    short_actions = torch.full((2, 2, 1), -0.4, dtype=torch.float64)
    short_actions[1] = 0.4
    short_result = _cash(
        short_actions,
        can_buy=blocked_buy,
        can_short_open_mask=_phase_mask(2),
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((2, 1), dtype=torch.float64),
    )
    torch.testing.assert_close(
        short_result.final_weights,
        torch.tensor([-0.4], dtype=torch.float64),
    )
    torch.testing.assert_close(
        short_result.executed_short_cover_weights[1],
        torch.zeros_like(short_result.executed_short_cover_weights[1]),
    )
    torch.testing.assert_close(
        short_result.executed_long_buy_weights[1],
        torch.zeros_like(short_result.executed_long_buy_weights[1]),
    )


def _underwater_short_continuous_kwargs(rows: int) -> dict[str, object]:
    return {
        "can_short_open_mask": _phase_mask(rows),
        "short_margin_rate": 0.9,
        "short_capacity_weights": torch.ones(
            (rows, 1),
            dtype=torch.float64,
        ),
        "claim_queue_sessions": 3,
        "initial_weights": torch.tensor(
            [-800.0 / 300.0],
            dtype=torch.float64,
        ),
        "initial_cash": torch.tensor(100.0 / 300.0, dtype=torch.float64),
        "initial_payables": torch.zeros(2, dtype=torch.float64),
        "initial_receivables": torch.tensor(
            [0.0, 0.0, 800.0 / 300.0],
            dtype=torch.float64,
        ),
        "initial_short_sale_collateral": torch.tensor(
            [100.0 / 300.0],
            dtype=torch.float64,
        ),
        "initial_short_margin_collateral": torch.tensor(
            [100.0 / 300.0],
            dtype=torch.float64,
        ),
    }


def test_cash_maintenance_blocked_release_is_not_a_cover_funding_source() -> None:
    actions = torch.full((1, 2, 1), -1.0, dtype=torch.float64)
    result = _cash(
        actions,
        can_short_open_mask=_phase_mask(1),
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((1, 1), dtype=torch.float64),
        claim_queue_sessions=3,
        initial_weights=torch.tensor([-8.0 / 7.0], dtype=torch.float64),
        initial_cash=torch.tensor(0.0, dtype=torch.float64),
        initial_payables=torch.zeros(2, dtype=torch.float64),
        initial_receivables=torch.tensor(
            [0.0, 0.0, 540.0 / 700.0],
            dtype=torch.float64,
        ),
        initial_short_sale_collateral=torch.tensor(
            [480.0 / 700.0],
            dtype=torch.float64,
        ),
        initial_short_margin_collateral=torch.tensor(
            [480.0 / 700.0],
            dtype=torch.float64,
        ),
    )

    torch.testing.assert_close(
        result.executed_short_cover_weights,
        torch.zeros_like(result.executed_short_cover_weights),
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([-8.0 / 7.0], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_payables,
        torch.zeros_like(result.final_payables),
    )


def test_cash_voluntary_underwater_cover_uses_actual_releasable_collateral() -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)
    result = _cash(
        actions,
        **_underwater_short_continuous_kwargs(1),
    )

    # Maintenance locks both collateral pools.  The same-due receivable cannot
    # fund today's new payable, so only one third of NAV can be covered.
    torch.testing.assert_close(
        result.executed_short_cover_weights[0],
        torch.tensor([[1.0 / 3.0], [0.0]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([-7.0 / 3.0], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_payables,
        torch.tensor([0.0, 1.0 / 3.0], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_receivables,
        torch.tensor([0.0, 8.0 / 3.0, 0.0], dtype=torch.float64),
    )


def test_cash_profitable_cover_funds_heterogeneous_underwater_cover() -> None:
    nav = 1700.0
    actions = torch.tensor(
        [
            [
                [-450.0 / nav, -1250.0 / nav],
                [-450.0 / nav, -1250.0 / nav],
            ]
        ],
        dtype=torch.float64,
    ).requires_grad_()
    can_buy = _phase_mask(1, 2)
    can_buy[:, CLOSE] = False
    result = _cash(
        actions,
        can_buy=can_buy,
        can_short_open_mask=_phase_mask(1, 2),
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((1, 2), dtype=torch.float64),
        claim_queue_sessions=3,
        initial_weights=torch.tensor(
            [-500.0 / nav, -2500.0 / nav],
            dtype=torch.float64,
        ),
        initial_cash=torch.tensor(0.0, dtype=torch.float64),
        initial_payables=torch.zeros(2, dtype=torch.float64),
        initial_receivables=torch.tensor(
            [0.0, 0.0, 700.0 / nav],
            dtype=torch.float64,
        ),
        initial_short_sale_collateral=torch.tensor(
            [1500.0 / nav, 500.0 / nav],
            dtype=torch.float64,
        ),
        initial_short_margin_collateral=torch.tensor(
            [1500.0 / nav, 500.0 / nav],
            dtype=torch.float64,
        ),
    )

    # Covering TWD 50 of the first short releases enough collateral to fund
    # itself and TWD 115 of the second short's loss.  The second requested
    # cover is therefore funded to exactly one third; a basket-wide scalar
    # would incorrectly reject both covers because their combined request is
    # net cash-consuming.
    torch.testing.assert_close(
        result.executed_short_cover_weights[0],
        torch.tensor(
            [[50.0 / nav, (1250.0 / 3.0) / nav], [0.0, 0.0]],
            dtype=torch.float64,
        ),
        rtol=1.0e-10,
        atol=1.0e-10,
    )
    torch.testing.assert_close(
        result.open_weights_history[0],
        torch.tensor(
            [-450.0 / nav, (-2500.0 + 1250.0 / 3.0) / nav],
            dtype=torch.float64,
        ),
        rtol=1.0e-10,
        atol=1.0e-10,
    )
    torch.testing.assert_close(
        result.final_payables,
        torch.zeros_like(result.final_payables),
        rtol=0.0,
        atol=1.0e-10,
    )
    (
        result.strategy_returns.sum()
        + result.turnovers.sum()
        + result.final_weights.square().sum()
    ).backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()


def test_overnight_producer_first_cover_finds_late_feasible_island() -> None:
    # These collateral pools are reachable with 90% short margin rounded up to
    # TWD 100: 100 shares originally shorted at TWD 7 and TWD 2 produce
    # sale/margin pools [700, 200] + [700, 200].  Both now trade at TWD 10.
    # Covering half of the first name is a raw producer, but maintenance locks
    # its collateral until enough of the underwater second name is covered.
    nav = 190.0
    actions = torch.zeros((1, 3, 2), dtype=torch.float64)
    actions[0, OVERNIGHT_EXIT_AT_OPEN] = torch.tensor(
        [0.5, 1.0],
        dtype=torch.float64,
    )
    actions.requires_grad_()
    result = _overnight(
        actions,
        can_short_open_mask=_phase_mask(1, 2),
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((1, 2), dtype=torch.float64),
        initial_weights=torch.tensor(
            [-1000.0 / nav, -1000.0 / nav],
            dtype=torch.float64,
        ),
        initial_cash=torch.tensor(390.0 / nav, dtype=torch.float64),
        initial_payables=torch.zeros(2, dtype=torch.float64),
        initial_receivables=torch.zeros(2, dtype=torch.float64),
        initial_short_sale_collateral=torch.tensor(
            [700.0 / nav, 200.0 / nav],
            dtype=torch.float64,
        ),
        initial_short_margin_collateral=torch.tensor(
            [700.0 / nav, 200.0 / nav],
            dtype=torch.float64,
        ),
        gross_budget=20.0,
    )

    # For second-name cover x in TWD 0..1000:
    # release=min(700+0.4x, max(-150+1.3x, 0)).
    # Funding is feasible only for x in [866.66..., 983.33...], so both the
    # producer-only endpoint and the full request are infeasible.  The correct
    # global producer-first ray endpoint is nevertheless TWD 983.33....
    torch.testing.assert_close(
        result.executed_short_cover_weights[0, OPEN],
        torch.tensor(
            [500.0 / nav, (2950.0 / 3.0) / nav],
            dtype=torch.float64,
        ),
        rtol=1.0e-11,
        atol=1.0e-11,
    )

    result.executed_short_cover_weights[0, OPEN].sum().backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    assert bool((actions.grad.abs() > 0.0).any())


def test_cash_mandatory_underwater_cover_bypasses_cap_then_defaults() -> None:
    actions = torch.zeros((3, 2, 1), dtype=torch.float64)
    forced = torch.zeros((3, 2, 1), dtype=torch.bool)
    forced[0, OPEN, 0] = True
    result = _cash(
        actions,
        force_short_cover_mask=forced,
        **_underwater_short_continuous_kwargs(3),
    )

    torch.testing.assert_close(
        result.executed_short_cover_weights[0, OPEN],
        torch.tensor([8.0 / 3.0], dtype=torch.float64),
    )
    assert result.settlement_default.tolist() == [False, False, True]
    assert not bool(result.final_alive)


def test_cash_signed_short_round_trip_keeps_proceeds_restricted() -> None:
    actions = torch.tensor([[[-0.2], [0.0]]], dtype=torch.float64)
    masks = _phase_mask(1)
    result = _cash(
        actions,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((1, 1), dtype=torch.float64),
    )

    torch.testing.assert_close(
        result.executed_short_open_weights[0, OPEN],
        torch.tensor([0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.executed_short_cover_weights[0, CLOSE],
        torch.tensor([0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_weights, torch.zeros(1, dtype=torch.float64)
    )
    torch.testing.assert_close(
        result.final_short_sale_collateral,
        torch.zeros(1, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_short_margin_collateral,
        torch.zeros(1, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.strategy_returns, torch.zeros(1, dtype=torch.float64)
    )
    torch.testing.assert_close(
        result.final_payables,
        torch.zeros_like(result.final_payables),
        atol=1.0e-12,
        rtol=0.0,
    )
    torch.testing.assert_close(
        result.final_receivables,
        torch.zeros_like(result.final_receivables),
        atol=1.0e-12,
        rtol=0.0,
    )


@pytest.mark.parametrize(
    ("entry_phase", "exit_fraction", "expected_day0", "expected_day1"),
    [
        (OPEN, 1.0, (0.4, 0.0), (0.4, 0.0)),
        (OPEN, 0.0, (0.4, 0.0), (0.0, 0.4)),
        (CLOSE, 1.0, (0.0, 0.4), (0.4, 0.0)),
        (CLOSE, 0.0, (0.0, 0.4), (0.0, 0.4)),
    ],
)
def test_overnight_supports_all_entry_exit_phase_pairs(
    entry_phase: int,
    exit_fraction: float,
    expected_day0: tuple[float, float],
    expected_day1: tuple[float, float],
) -> None:
    actions = torch.zeros((2, 3, 1), dtype=torch.float64)
    actions[:, OVERNIGHT_EXIT_AT_OPEN, 0] = exit_fraction
    entry_channel = (
        OVERNIGHT_ENTRY_AT_OPEN if entry_phase == OPEN else OVERNIGHT_ENTRY_AT_CLOSE
    )
    actions[0, entry_channel, 0] = 0.4
    result = _overnight(actions)

    torch.testing.assert_close(
        result.event_turnovers,
        torch.tensor(
            [expected_day0, expected_day1],
            dtype=torch.float64,
        ),
    )
    torch.testing.assert_close(
        result.final_weights, torch.zeros(1, dtype=torch.float64)
    )
    assert result.final_due_weights is not None
    torch.testing.assert_close(
        result.final_due_weights,
        torch.zeros(1, dtype=torch.float64),
    )
    assert not bool(result.settlement_default.any())


def test_overnight_same_direction_roll_is_two_gross_orders() -> None:
    actions = torch.zeros((2, 3, 1), dtype=torch.float64)
    actions[:, OVERNIGHT_EXIT_AT_OPEN, 0] = 0.0
    actions[:, OVERNIGHT_ENTRY_AT_OPEN, 0] = 0.4
    result = _overnight(actions)

    # On day one the old 0.4 remains until close while a new 0.4 is bought at
    # open.  Closing the old cohort is a real gross sale despite unchanged
    # end-of-day net exposure.
    torch.testing.assert_close(
        result.event_turnovers[1],
        torch.tensor([0.4, 0.4], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.weights_history,
        torch.full((2, 1), 0.4, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.weights_history,
        result.close_weights_history,
    )
    assert result.due_weights_history is not None
    torch.testing.assert_close(
        result.due_weights_history,
        result.weights_history,
    )


def test_overnight_blocked_due_close_is_absorbing_default() -> None:
    actions = torch.zeros((2, 3, 1), dtype=torch.float64)
    actions[:, OVERNIGHT_EXIT_AT_OPEN, 0] = 0.0
    actions[0, OVERNIGHT_ENTRY_AT_CLOSE, 0] = 0.4
    can_sell = _phase_mask(2)
    can_sell[1, CLOSE, 0] = False
    result = _overnight(actions, can_sell=can_sell)

    assert result.settlement_default.tolist() == [False, True]
    assert not bool(result.final_alive)
    torch.testing.assert_close(
        result.final_weights, torch.zeros(1, dtype=torch.float64)
    )
    torch.testing.assert_close(
        result.final_cash, torch.tensor(0.0, dtype=torch.float64)
    )
    assert torch.isfinite(result.strategy_returns).all()
    assert result.strategy_returns[1] < -13.0


def test_overnight_short_can_enter_at_close_and_cover_next_open() -> None:
    actions = torch.zeros((2, 3, 1), dtype=torch.float64)
    actions[:, OVERNIGHT_EXIT_AT_OPEN, 0] = 1.0
    actions[0, OVERNIGHT_ENTRY_AT_CLOSE, 0] = -0.2
    masks = _phase_mask(2)
    result = _overnight(
        actions,
        can_short_open_mask=masks,
        short_margin_rate=0.9,
        short_capacity_weights=torch.ones((2, 1), dtype=torch.float64),
    )

    torch.testing.assert_close(
        result.executed_short_open_weights[0, CLOSE],
        torch.tensor([0.2], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.executed_short_cover_weights[1, OPEN],
        torch.tensor([0.2], dtype=torch.float64),
    )
    assert bool(result.final_alive)
    torch.testing.assert_close(
        result.final_weights, torch.zeros(1, dtype=torch.float64)
    )


def test_cash_terminal_exit_blocks_reentry_and_requires_physical_side() -> None:
    actions = torch.zeros((2, 2, 1), dtype=torch.float64)
    actions[0, CLOSE, 0] = 0.4
    actions[1, OPEN, 0] = 0.4
    actions[1, CLOSE, 0] = 0.4
    force_exit = torch.zeros((2, 2, 1), dtype=torch.bool)
    force_exit[1, CLOSE, 0] = True
    result = _cash(actions, force_exit_mask=force_exit)

    torch.testing.assert_close(
        result.executed_long_sell_weights[1, CLOSE],
        torch.tensor([0.4], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_weights, torch.zeros(1, dtype=torch.float64)
    )
    assert bool(result.final_alive)

    blocked_sell = _phase_mask(2)
    blocked_sell[1, CLOSE, 0] = False
    blocked = _cash(
        actions,
        force_exit_mask=force_exit,
        can_sell=blocked_sell,
    )
    assert blocked.settlement_default.tolist() == [False, True]
    assert not bool(blocked.final_alive)


def test_cash_exact_dividend_uses_extended_claim_queue_and_is_not_silent() -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)
    actions[0, CLOSE, 0] = 0.5
    result = _cash(
        actions,
        cash_dividend_yield=torch.tensor([[0.1]], dtype=torch.float64),
        cash_dividend_payment_delay_sessions=torch.tensor([[4]]),
        claim_queue_sessions=4,
    )

    assert tuple(result.final_payables.shape) == (2,)
    assert tuple(result.final_receivables.shape) == (4,)
    torch.testing.assert_close(
        result.final_receivables,
        torch.tensor(
            [0.0, 0.0, 0.0, 0.05 / 1.05],
            dtype=torch.float64,
        ),
    )
    torch.testing.assert_close(
        result.equity_scale_history,
        torch.tensor([1.05], dtype=torch.float64),
    )
    # The phase audit remains at the closing-auction execution boundary.  The
    # same-close entitlement subsequently raises NAV and drifts only recurrent
    # end-of-day state.
    torch.testing.assert_close(
        result.close_weights_history,
        torch.tensor([[0.5]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.weights_history,
        result.close_weights_history,
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([0.5 / 1.05], dtype=torch.float64),
    )

    with pytest.raises(ValueError, match="must be supplied together"):
        _cash(
            actions,
            cash_dividend_yield=torch.tensor([[0.1]], dtype=torch.float64),
        )
    with pytest.raises(ValueError, match="exceeds"):
        _cash(
            actions,
            cash_dividend_yield=torch.tensor([[0.1]], dtype=torch.float64),
            cash_dividend_payment_delay_sessions=torch.tensor([[3]]),
            claim_queue_sessions=2,
        )


def test_overnight_dividend_keeps_due_history_at_close_execution_boundary() -> None:
    actions = torch.zeros((1, 3, 1), dtype=torch.float64)
    actions[0, OVERNIGHT_ENTRY_AT_CLOSE, 0] = 0.5
    result = _overnight(
        actions,
        cash_dividend_yield=torch.tensor([[0.1]], dtype=torch.float64),
        cash_dividend_payment_delay_sessions=torch.tensor([[3]]),
        claim_queue_sessions=3,
    )

    torch.testing.assert_close(
        result.weights_history,
        torch.tensor([[0.5]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.close_weights_history,
        result.weights_history,
    )
    assert result.due_weights_history is not None
    torch.testing.assert_close(
        result.due_weights_history,
        result.weights_history,
    )
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([0.5 / 1.05], dtype=torch.float64),
    )
    assert result.final_due_weights is not None
    torch.testing.assert_close(
        result.final_due_weights,
        torch.tensor([0.5 / 1.05], dtype=torch.float64),
    )
    torch.testing.assert_close(result.final_due_weights, result.final_weights)


def test_dividend_execution_weight_is_stable_under_extreme_entitlement() -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)
    actions[0, CLOSE, 0] = 0.5
    result = _cash(
        actions,
        cash_dividend_yield=torch.tensor([[1.0e16]], dtype=torch.float64),
        cash_dividend_payment_delay_sessions=torch.tensor(
            [[3]],
            dtype=torch.int64,
        ),
        claim_queue_sessions=3,
    )

    torch.testing.assert_close(
        result.weights_history,
        torch.tensor([[0.5]], dtype=torch.float64),
    )
    expected_final_weight = torch.tensor(
        [0.5 / (1.0 + 0.5e16)],
        dtype=torch.float64,
    )
    torch.testing.assert_close(result.final_weights, expected_final_weight)
    torch.testing.assert_close(
        (
            result.final_cash
            + result.final_weights.sum()
            + result.final_receivables.sum()
            - result.final_payables.sum()
        ),
        torch.tensor(1.0, dtype=torch.float64),
    )


def test_overnight_monolithic_equals_split_state_continuation() -> None:
    rows = 4
    actions = torch.zeros((rows, 3, 1), dtype=torch.float64)
    actions[:, OVERNIGHT_EXIT_AT_OPEN, 0] = torch.tensor(
        [0.0, 1.0, 0.0, 1.0],
        dtype=torch.float64,
    )
    actions[0, OVERNIGHT_ENTRY_AT_CLOSE, 0] = 0.30
    actions[1, OVERNIGHT_ENTRY_AT_CLOSE, 0] = 0.25
    actions[2, OVERNIGHT_ENTRY_AT_OPEN, 0] = 0.10
    gap = torch.tensor([[0.0], [0.02], [-0.01], [0.03]], dtype=torch.float64)
    intraday = torch.tensor([[0.01], [-0.02], [0.015], [0.0]], dtype=torch.float64)
    mask = _phase_mask(rows)

    full = run_tw_overnight_dual_session(
        actions,
        gap,
        intraday,
        mask,
        mask,
        mask,
        0.001,
        0.002,
    )
    first = run_tw_overnight_dual_session(
        actions[:2],
        gap[:2],
        intraday[:2],
        mask[:2],
        mask[:2],
        mask[:2],
        0.001,
        0.002,
    )
    second = run_tw_overnight_dual_session(
        actions[2:],
        gap[2:],
        intraday[2:],
        mask[2:],
        mask[2:],
        mask[2:],
        0.001,
        0.002,
        initial_weights=first.final_weights,
        initial_cash=first.final_cash,
        initial_payables=first.final_payables,
        initial_receivables=first.final_receivables,
        initial_alive=first.final_alive,
        initial_equity_scale=first.final_equity_scale,
        initial_short_sale_collateral=first.final_short_sale_collateral,
        initial_short_margin_collateral=first.final_short_margin_collateral,
    )

    torch.testing.assert_close(
        torch.cat((first.strategy_returns, second.strategy_returns)),
        full.strategy_returns,
    )
    torch.testing.assert_close(
        torch.cat((first.event_turnovers, second.event_turnovers)),
        full.event_turnovers,
    )
    torch.testing.assert_close(second.final_weights, full.final_weights)
    torch.testing.assert_close(second.final_cash, full.final_cash)
    torch.testing.assert_close(second.final_payables, full.final_payables)
    torch.testing.assert_close(second.final_receivables, full.final_receivables)
    torch.testing.assert_close(
        second.final_short_sale_collateral,
        full.final_short_sale_collateral,
    )
    torch.testing.assert_close(
        second.final_short_margin_collateral,
        full.final_short_margin_collateral,
    )
    torch.testing.assert_close(second.final_equity_scale, full.final_equity_scale)
    assert bool(second.final_alive) == bool(full.final_alive)


def test_flat_invalid_returns_are_ignored_but_active_invalid_gap_defaults() -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)
    invalid = torch.tensor([[float("nan")]], dtype=torch.float64)
    flat = _cash(actions, overnight=invalid, intraday=invalid)
    assert not bool(flat.settlement_default.any())

    active = _cash(
        actions,
        overnight=invalid,
        initial_weights=torch.tensor([0.5], dtype=torch.float64),
        initial_cash=torch.tensor(0.5, dtype=torch.float64),
    )
    assert active.settlement_default.tolist() == [True]
    assert not bool(active.final_alive)


def test_overnight_fraction_api_is_resolved_not_a_logit() -> None:
    actions = torch.zeros((1, 3, 1), dtype=torch.float32)
    actions[0, OVERNIGHT_EXIT_AT_OPEN, 0] = 1.1
    with pytest.raises(ValueError, match="already-resolved"):
        _overnight(actions)


def test_ledger_promotes_bf16_actions_to_fp32_and_remains_differentiable() -> None:
    bf16_actions = torch.zeros((1, 2, 1), dtype=torch.bfloat16)
    bf16_actions[0, OPEN, 0] = 0.25
    promoted = _cash(
        bf16_actions,
        intraday=torch.tensor([[math.log(1.1)]], dtype=torch.bfloat16),
    )
    assert promoted.final_cash.dtype == torch.float32
    assert promoted.final_payables.dtype == torch.float32
    assert promoted.final_short_sale_collateral.dtype == torch.float32

    actions = torch.zeros((1, 2, 1), dtype=torch.float64, requires_grad=True)
    with torch.no_grad():
        actions[0, OPEN, 0] = 0.25
    differentiable = _cash(
        actions,
        intraday=torch.tensor([[math.log(1.1)]], dtype=torch.float64),
    )
    differentiable.strategy_returns.sum().backward()
    assert actions.grad is not None
    assert torch.isfinite(actions.grad).all()
    assert actions.grad[0, OPEN, 0] > 0


@pytest.mark.parametrize(
    ("runner", "phases"),
    [
        (_cash, 2),
        (_overnight, 3),
    ],
)
def test_disabling_history_drops_all_symbol_audit_graphs(
    runner,
    phases: int,
) -> None:
    actions = torch.zeros((2, phases, 3), dtype=torch.float64, requires_grad=True)
    result = runner(actions, return_weights_history=False)

    assert tuple(result.event_turnovers.shape) == (0, 2)
    assert tuple(result.weights_history.shape) == (0, 3)
    assert tuple(result.open_weights_history.shape) == (0, 3)
    assert tuple(result.close_weights_history.shape) == (0, 3)
    assert tuple(result.executed_buy_weights.shape) == (0, 2, 3)
    assert tuple(result.executed_sell_weights.shape) == (0, 2, 3)
    assert tuple(result.executed_long_buy_weights.shape) == (0, 2, 3)
    assert tuple(result.executed_long_sell_weights.shape) == (0, 2, 3)
    assert tuple(result.executed_short_open_weights.shape) == (0, 2, 3)
    assert tuple(result.executed_short_cover_weights.shape) == (0, 2, 3)
    assert result.due_weights_history is None
    assert result.strategy_returns.grad_fn is not None
