"""Point-in-time Taiwan broker commission-rebate primitives.

The trading ledgers charge the full commission at execution.  The economic
discount is an earned receivable, while the configured payment timing controls
when that receivable becomes settled cash.  Keeping those two events separate
prevents a monthly rebate from being used before it is actually paid and keeps
the later cash transfer NAV-neutral.
"""

from __future__ import annotations

import torch

from stockagent.backtest.tw_commission_rebate_policy import (
    COMMISSION_REBATE_TIMINGS,
    commission_rebate_calendar,
    normalize_commission_rebate_timing,
)


def prepare_commission_rebate_calendar_tensors(
    *,
    reference: torch.Tensor,
    timing: object,
    session_month_ids: torch.Tensor | None,
    payment_eligible_mask: torch.Tensor | None,
    state_advance_mask: torch.Tensor | None = None,
) -> tuple[str, torch.Tensor, torch.Tensor]:
    """Validate row-aligned calendar tensors for a differentiable ledger."""

    canonical = normalize_commission_rebate_timing(timing)
    rows = int(reference.size(0))
    device = reference.device
    if canonical == "daily_close":
        month_ids = torch.zeros((rows,), device=device, dtype=torch.int64)
        payment = torch.zeros((rows,), device=device, dtype=torch.bool)
        return canonical, month_ids, payment
    if session_month_ids is None or payment_eligible_mask is None:
        raise ValueError(
            "monthly_15th commission rebates require session_month_ids and "
            "commission_rebate_payment_eligible_mask"
        )
    month_ids = torch.as_tensor(session_month_ids, device=device, dtype=torch.int64)
    payment = torch.as_tensor(
        payment_eligible_mask,
        device=device,
        dtype=torch.bool,
    )
    if tuple(month_ids.shape) != (rows,):
        raise ValueError(f"session_month_ids must have shape [{rows}]")
    if tuple(payment.shape) != (rows,):
        raise ValueError(
            "commission_rebate_payment_eligible_mask must have "
            f"shape [{rows}]"
        )
    if state_advance_mask is None:
        active_rows = torch.ones((rows,), device=device, dtype=torch.bool)
    else:
        active_rows = torch.as_tensor(
            state_advance_mask,
            device=device,
            dtype=torch.bool,
        )
        if tuple(active_rows.shape) != (rows,):
            raise ValueError(f"state_advance_mask must have shape [{rows}]")
    # Zero is the explicit padding sentinel used by fixed-shape eval chunks,
    # but every active exchange session must identify a real calendar month.
    if not torch.compiler.is_compiling():
        if bool((month_ids < 0).any().item()):
            raise ValueError("session_month_ids must contain non-negative integers")
        if bool((active_rows & (month_ids == 0)).any().item()):
            raise ValueError(
                "active session_month_ids must contain positive integers"
            )
    return canonical, month_ids, payment


def prepare_commission_rebate_state(
    *,
    reference: torch.Tensor,
    initial_current: torch.Tensor | None,
    initial_due: torch.Tensor | None,
    initial_month_id: torch.Tensor | None,
    alive: torch.Tensor,
    detach: bool,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Prepare normalized pending-rebate state at a public ledger boundary."""

    def amount(value: torch.Tensor | None, name: str) -> torch.Tensor:
        if value is None:
            result = torch.zeros((), device=reference.device, dtype=reference.dtype)
        else:
            source = value.detach() if detach else value
            if source.dim() != 0:
                raise ValueError(f"{name} must be a scalar tensor")
            result = source.clone(memory_format=torch.contiguous_format).to(
                device=reference.device,
                dtype=reference.dtype,
            )
        if not torch.compiler.is_compiling():
            if not bool(torch.isfinite(result).item()) or bool((result < 0.0).item()):
                raise ValueError(f"{name} must be finite and non-negative")
        return result.reshape(())

    current = amount(initial_current, "initial_commission_rebate_current")
    due = amount(initial_due, "initial_commission_rebate_due")
    if initial_month_id is None:
        month_id = torch.zeros((), device=reference.device, dtype=torch.int64)
    else:
        source_month = initial_month_id.detach() if detach else initial_month_id
        if source_month.dim() != 0:
            raise ValueError("initial_commission_rebate_month_id must be scalar")
        month_id = source_month.clone(memory_format=torch.contiguous_format).to(
            device=reference.device,
            dtype=torch.int64,
        ).reshape(())
    if not torch.compiler.is_compiling():
        if bool((month_id < 0).item()):
            raise ValueError(
                "initial_commission_rebate_month_id must be non-negative"
            )
        if bool(((current > 0.0) & (month_id == 0)).item()):
            raise ValueError(
                "a non-zero current commission rebate requires its month id"
            )
        if not bool(alive.item()) and bool(
            ((current > 0.0) | (due > 0.0) | (month_id != 0)).item()
        ):
            raise ValueError(
                "an absorbing dead account cannot retain commission rebates"
            )
    return current, due, month_id


def mask_commission_rebate_state(
    current: torch.Tensor,
    due: torch.Tensor,
    month_id: torch.Tensor,
    alive: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Forfeit all pending rebates when the account enters absorbing default."""

    return (
        torch.where(alive, current, torch.zeros_like(current)),
        torch.where(alive, due, torch.zeros_like(due)),
        torch.where(alive, month_id, torch.zeros_like(month_id)),
    )


def apply_commission_rebate_at_close(
    *,
    earned: torch.Tensor,
    cash: torch.Tensor,
    current: torch.Tensor,
    due: torch.Tensor,
    accrual_month_id: torch.Tensor,
    session_month_id: torch.Tensor,
    payment_eligible: torch.Tensor,
    timing: str,
    advance: torch.Tensor,
    alive: torch.Tensor,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    """Accrue the day's rebate and transfer only mature rebates to cash.

    Monthly payment happens after all closing executions.  On a month change,
    the previous month's current bucket first becomes due; if the first
    observed session is already on/after the 15th it is then paid immediately.
    The current session's newly earned amount is added only after that payout,
    so commissions earned on days 1--15 are never mixed into the prior month.
    """

    active = advance & alive
    earned_active = torch.where(active, earned.clamp_min(0.0), torch.zeros_like(earned))
    if timing == "daily_close":
        paid = earned_active
        return (
            cash + paid,
            torch.zeros_like(current),
            torch.zeros_like(due),
            torch.zeros_like(accrual_month_id),
            paid,
        )
    if timing != "monthly_15th":
        raise AssertionError(f"unsupported commission rebate timing: {timing}")

    initialized = accrual_month_id > 0
    month_changed = active & initialized & (session_month_id != accrual_month_id)
    rolled_due = torch.where(month_changed, due + current, due)
    rolled_current = torch.where(month_changed, torch.zeros_like(current), current)
    next_month_id = torch.where(
        active,
        session_month_id,
        accrual_month_id,
    )
    should_pay = active & payment_eligible
    paid = torch.where(should_pay, rolled_due, torch.zeros_like(rolled_due))
    next_due = rolled_due - paid
    next_current = rolled_current + earned_active
    return cash + paid, next_current, next_due, next_month_id, paid


def normalize_commission_rebate_state(
    *,
    current: torch.Tensor,
    due: torch.Tensor,
    month_id: torch.Tensor,
    nav_end: torch.Tensor,
    advance: torch.Tensor,
    alive: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Normalize pending asset buckets into the next day's NAV units."""

    live_advance = advance & alive
    safe_nav = torch.where(live_advance, nav_end, torch.ones_like(nav_end))
    next_current = torch.where(live_advance, current / safe_nav, current)
    next_due = torch.where(live_advance, due / safe_nav, due)
    return mask_commission_rebate_state(
        next_current,
        next_due,
        month_id,
        alive,
    )


__all__ = [
    "COMMISSION_REBATE_TIMINGS",
    "apply_commission_rebate_at_close",
    "commission_rebate_calendar",
    "mask_commission_rebate_state",
    "normalize_commission_rebate_state",
    "normalize_commission_rebate_timing",
    "prepare_commission_rebate_calendar_tensors",
    "prepare_commission_rebate_state",
]
