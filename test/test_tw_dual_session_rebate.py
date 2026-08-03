from __future__ import annotations

import math

import pytest
import torch

from stockagent.backtest.tw_dual_session import (
    CLOSE,
    OPEN,
    run_tw_cash_dual_session,
    run_tw_overnight_dual_session,
)
from stockagent.backtest.tw_dual_session_compiled import (
    run_tw_cash_dual_session_compiled,
)


def _phase_mask(rows: int, symbols: int = 1) -> torch.Tensor:
    return torch.ones((rows, 2, symbols), dtype=torch.bool)


def _cash(
    actions: torch.Tensor,
    *,
    commission_rebate_rates: torch.Tensor | float = 0.008,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: torch.Tensor | None = None,
    payment_eligible: torch.Tensor | None = None,
    buy_fees: torch.Tensor | float = 0.01,
    sell_fees: torch.Tensor | float = 0.01,
    **kwargs: object,
):
    rows, _, symbols = actions.shape
    returns = torch.zeros((rows, symbols), dtype=actions.dtype)
    masks = _phase_mask(rows, symbols)
    return run_tw_cash_dual_session(
        actions,
        returns,
        returns,
        masks,
        masks,
        masks,
        buy_fees,
        sell_fees,
        commission_rebate_rates=commission_rebate_rates,
        commission_rebate_timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        commission_rebate_payment_eligible_mask=payment_eligible,
        **kwargs,
    )


def test_daily_close_rebate_charges_gross_fees_then_pays_at_close() -> None:
    actions = torch.tensor([[[0.5], [0.0]]], dtype=torch.float64)

    result = _cash(actions)

    torch.testing.assert_close(
        result.event_commission_rebate_accrued,
        torch.tensor([[0.004, 0.004]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.commission_rebate_accrued_history,
        torch.tensor([0.008], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.commission_rebate_paid_history,
        torch.tensor([0.008], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.strategy_returns,
        torch.tensor([math.log(0.998)], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.commission_rebate_current_history,
        torch.zeros(1, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.commission_rebate_due_history,
        torch.zeros(1, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_commission_rebate_month_id,
        torch.tensor(0, dtype=torch.int64),
    )


def test_monthly_rebate_is_accrued_nav_without_early_cash_payment() -> None:
    actions = torch.tensor([[[0.5], [0.0]]], dtype=torch.float64)
    monthly = _cash(
        actions,
        commission_rebate_timing="monthly_15th",
        session_month_ids=torch.tensor([2026 * 12 + 1]),
        payment_eligible=torch.tensor([False]),
    )
    daily = _cash(actions)

    torch.testing.assert_close(monthly.strategy_returns, daily.strategy_returns)
    torch.testing.assert_close(
        monthly.commission_rebate_paid_history,
        torch.zeros(1, dtype=torch.float64),
    )
    assert float(monthly.commission_rebate_current_history[0]) > 0.0
    assert float(monthly.cash_history[0]) < float(daily.cash_history[0])
    torch.testing.assert_close(
        monthly.final_commission_rebate_month_id,
        torch.tensor(2026 * 12 + 1, dtype=torch.int64),
    )


def test_monthly_rolls_prior_month_then_pays_only_due_bucket() -> None:
    actions = torch.zeros((3, 2, 1), dtype=torch.float64)
    actions[0, OPEN, 0] = 0.5
    actions[2, OPEN, 0] = 0.25
    month_ids = torch.tensor(
        [2026 * 12 + 1, 2026 * 12 + 2, 2026 * 12 + 2],
        dtype=torch.int64,
    )

    result = _cash(
        actions,
        commission_rebate_timing="monthly_15th",
        session_month_ids=month_ids,
        payment_eligible=torch.tensor([False, False, True]),
    )

    assert float(result.commission_rebate_current_history[0]) > 0.0
    torch.testing.assert_close(
        result.commission_rebate_due_history[1],
        result.commission_rebate_current_history[0],
    )
    torch.testing.assert_close(
        result.commission_rebate_paid_history[2],
        result.commission_rebate_due_history[1],
    )
    torch.testing.assert_close(
        result.commission_rebate_due_history[2],
        torch.tensor(0.0, dtype=torch.float64),
    )
    # The payment-day commissions belong to the current month and cannot be
    # mixed into the prior-month payout.
    assert float(result.commission_rebate_accrued_history[2]) > 0.0
    assert float(result.commission_rebate_current_history[2]) > 0.0


def test_monthly_allows_zero_month_only_on_inactive_padding_rows() -> None:
    actions = torch.zeros((2, 2, 1), dtype=torch.float64)
    actions[0, OPEN, 0] = 0.5

    result = _cash(
        actions,
        commission_rebate_timing="monthly_15th",
        session_month_ids=torch.tensor([2026 * 12 + 1, 0]),
        payment_eligible=torch.tensor([False, False]),
        state_advance_mask=torch.tensor([True, False]),
    )

    torch.testing.assert_close(
        result.strategy_returns[1],
        torch.tensor(0.0, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_commission_rebate_month_id,
        torch.tensor(2026 * 12 + 1, dtype=torch.int64),
    )


def test_pending_monthly_rebate_cannot_size_or_fund_same_day_trades() -> None:
    actions = torch.ones((1, 2, 1), dtype=torch.float64)

    result = _cash(
        actions,
        commission_rebate_rates=0.0,
        commission_rebate_timing="monthly_15th",
        session_month_ids=torch.tensor([2026 * 12 + 1]),
        payment_eligible=torch.tensor([False]),
        buy_fees=0.0,
        sell_fees=0.0,
        initial_cash=torch.tensor(0.8, dtype=torch.float64),
        initial_commission_rebate_current=torch.tensor(
            0.2,
            dtype=torch.float64,
        ),
        initial_commission_rebate_month_id=torch.tensor(
            2026 * 12 + 1,
            dtype=torch.int64,
        ),
    )

    # Total accrual-basis NAV is one, but only the liquid 0.8 base NAV may
    # enter the auction.  A target of 1.0 therefore carries only 0.8 risky NAV.
    torch.testing.assert_close(
        result.final_weights,
        torch.tensor([0.8], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_commission_rebate_current,
        torch.tensor(0.2, dtype=torch.float64),
    )


def test_open_settlement_default_forfeits_pending_rebate() -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)

    result = _cash(
        actions,
        commission_rebate_rates=0.0,
        commission_rebate_timing="monthly_15th",
        session_month_ids=torch.tensor([2026 * 12 + 1]),
        payment_eligible=torch.tensor([True]),
        buy_fees=0.0,
        sell_fees=0.0,
        initial_cash=torch.tensor(0.0, dtype=torch.float64),
        initial_payables=torch.tensor([0.1, 0.0], dtype=torch.float64),
        initial_receivables=torch.tensor([0.9, 0.0], dtype=torch.float64),
        initial_commission_rebate_current=torch.tensor(
            0.2,
            dtype=torch.float64,
        ),
        initial_commission_rebate_month_id=torch.tensor(
            2026 * 12 + 1,
            dtype=torch.int64,
        ),
    )

    assert bool(result.settlement_default[0])
    assert not bool(result.final_alive)
    torch.testing.assert_close(
        result.final_commission_rebate_current,
        torch.tensor(0.0, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_commission_rebate_due,
        torch.tensor(0.0, dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.final_commission_rebate_month_id,
        torch.tensor(0, dtype=torch.int64),
    )


def test_overnight_rebate_counts_due_exit_and_both_entry_auctions() -> None:
    actions = torch.tensor([[[1.0], [0.3], [0.2]]], dtype=torch.float64)
    returns = torch.zeros((1, 1), dtype=torch.float64)
    masks = _phase_mask(1)

    result = run_tw_overnight_dual_session(
        actions,
        returns,
        returns,
        masks,
        masks,
        masks,
        0.01,
        0.01,
        commission_rebate_rates=0.008,
        initial_weights=torch.tensor([0.4], dtype=torch.float64),
        initial_cash=torch.tensor(0.6, dtype=torch.float64),
    )

    torch.testing.assert_close(
        result.event_commission_rebate_accrued,
        torch.tensor([[0.0056, 0.0015888]], dtype=torch.float64),
    )
    torch.testing.assert_close(
        result.commission_rebate_paid_history,
        torch.tensor([0.0071888], dtype=torch.float64),
    )


def test_monthly_eager_chunk_continuation_preserves_all_rebate_state() -> None:
    rows = 6
    actions = torch.zeros((rows, 2, 1), dtype=torch.float64)
    actions[:, OPEN, 0] = 0.4
    actions[:, CLOSE, 0] = 0.2
    month_ids = torch.tensor(
        [
            2026 * 12 + 1,
            2026 * 12 + 1,
            2026 * 12 + 2,
            2026 * 12 + 2,
            2026 * 12 + 2,
            2026 * 12 + 2,
        ],
        dtype=torch.int64,
    )
    eligible = torch.tensor([False, False, False, False, True, True])
    full = _cash(
        actions,
        commission_rebate_timing="monthly_15th",
        session_month_ids=month_ids,
        payment_eligible=eligible,
    )

    split = 3
    first = _cash(
        actions[:split],
        commission_rebate_timing="monthly_15th",
        session_month_ids=month_ids[:split],
        payment_eligible=eligible[:split],
    )
    second = _cash(
        actions[split:],
        commission_rebate_timing="monthly_15th",
        session_month_ids=month_ids[split:],
        payment_eligible=eligible[split:],
        initial_weights=first.final_weights,
        initial_cash=first.final_cash,
        initial_payables=first.final_payables,
        initial_receivables=first.final_receivables,
        initial_alive=first.final_alive,
        initial_equity_scale=first.final_equity_scale,
        initial_short_sale_collateral=first.final_short_sale_collateral,
        initial_short_margin_collateral=first.final_short_margin_collateral,
        initial_commission_rebate_current=(
            first.final_commission_rebate_current
        ),
        initial_commission_rebate_due=first.final_commission_rebate_due,
        initial_commission_rebate_month_id=(
            first.final_commission_rebate_month_id
        ),
    )

    for name in (
        "strategy_returns",
        "turnovers",
        "cash_history",
        "payables_history",
        "receivables_history",
        "commission_rebate_accrued_history",
        "commission_rebate_paid_history",
        "commission_rebate_current_history",
        "commission_rebate_due_history",
        "event_commission_rebate_accrued",
    ):
        torch.testing.assert_close(
            torch.cat((getattr(first, name), getattr(second, name)), dim=0),
            getattr(full, name),
        )
    for name in (
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_alive",
        "final_equity_scale",
        "final_commission_rebate_current",
        "final_commission_rebate_due",
        "final_commission_rebate_month_id",
    ):
        torch.testing.assert_close(getattr(second, name), getattr(full, name))


def test_compiled_cpu_wrapper_matches_monthly_eager_contract() -> None:
    rows = 35
    actions = torch.zeros((rows, 2, 2), dtype=torch.float64)
    actions[:, OPEN] = torch.tensor([0.25, -0.20], dtype=torch.float64)
    actions[:, CLOSE] = torch.tensor([0.15, -0.10], dtype=torch.float64)
    returns = torch.zeros((rows, 2), dtype=torch.float64)
    masks = _phase_mask(rows, 2)
    short_capacity = torch.ones((rows, 2), dtype=torch.float64)
    month_ids = torch.cat(
        (
            torch.full((16,), 2026 * 12 + 1, dtype=torch.int64),
            torch.full((19,), 2026 * 12 + 2, dtype=torch.int64),
        )
    )
    eligible = torch.zeros(rows, dtype=torch.bool)
    eligible[30:] = True
    kwargs = {
        "actions": actions,
        "overnight_log_returns": returns,
        "intraday_log_returns": returns,
        "tradable_mask": masks,
        "can_buy_mask": masks,
        "can_sell_mask": masks,
        "buy_fee_rates": 0.01,
        "sell_fee_rates": 0.01,
        "commission_rebate_rates": 0.008,
        "commission_rebate_timing": "monthly_15th",
        "session_month_ids": month_ids,
        "commission_rebate_payment_eligible_mask": eligible,
        "can_short_open_mask": masks,
        "short_margin_rate": 0.9,
        "short_capacity_weights": short_capacity,
    }

    expected = run_tw_cash_dual_session(**kwargs)
    actual = run_tw_cash_dual_session_compiled(**kwargs, strict_compile=True)

    for name in (
        "strategy_returns",
        "turnovers",
        "weights_history",
        "cash_history",
        "payables_history",
        "receivables_history",
        "commission_rebate_accrued_history",
        "commission_rebate_paid_history",
        "commission_rebate_current_history",
        "commission_rebate_due_history",
        "event_commission_rebate_accrued",
        "final_weights",
        "final_cash",
        "final_payables",
        "final_receivables",
        "final_commission_rebate_current",
        "final_commission_rebate_due",
        "final_commission_rebate_month_id",
    ):
        torch.testing.assert_close(getattr(actual, name), getattr(expected, name))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {
                "commission_rebate_rates": 0.011,
            },
            "cannot exceed gross buy or sell fee rates",
        ),
        (
            {
                "commission_rebate_timing": "monthly_15th",
            },
            "require session_month_ids",
        ),
    ],
)
def test_rebate_contract_rejects_invalid_low_level_inputs(
    kwargs: dict[str, object],
    message: str,
) -> None:
    actions = torch.zeros((1, 2, 1), dtype=torch.float64)

    with pytest.raises(ValueError, match=message):
        _cash(actions, **kwargs)
