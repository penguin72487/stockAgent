"""Separate stock decision sessions from receipt-backed futures account days.

This module only constructs an explicit calendar and interleaves already
validated ledger rows.  It does not fetch observations, create execution
prices, attach data to a training panel, or authorize an experiment.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib

import numpy as np
import torch


@dataclass(frozen=True, slots=True)
class FuturesAccountCalendar:
    """Account nodes and their mapping to unchanged stock decision sessions."""

    dates: np.ndarray
    decision_dates: np.ndarray
    account_only_mask: np.ndarray
    decision_indices: np.ndarray
    account_only_indices: np.ndarray
    prior_decision_index: np.ndarray
    fingerprint: str
    training_trailing_event_credit: bool = False


@dataclass(frozen=True, slots=True)
class FuturesAccountCalendarBatch:
    """A chronological policy batch with its sparse original account tape."""

    calendar: FuturesAccountCalendar
    account_execution: torch.Tensor
    decision_start: int
    decision_stop: int
    account_event_start: int
    account_event_stop: int


def _calendar_from_nodes(dates: np.ndarray, decision_dates: np.ndarray,
                         account_only_mask: np.ndarray, *,
                         training_trailing_event_credit: bool = False) -> FuturesAccountCalendar:
    decision_indices = np.flatnonzero(~account_only_mask).astype(np.int64)
    event_indices = np.flatnonzero(account_only_mask).astype(np.int64)
    if not np.array_equal(dates[decision_indices], decision_dates):
        raise ValueError("calendar decision nodes do not match the decision dates")
    prior = np.searchsorted(decision_dates, dates, side="right").astype(np.int64) - 1
    digest = hashlib.sha256()
    digest.update(b"stock_decision_futures_account_calendar_v1\0")
    digest.update(dates.astype("<M8[D]").tobytes())
    digest.update(account_only_mask.tobytes())
    if training_trailing_event_credit:
        digest.update(b"\0training_trailing_event_credit_v1")
    return FuturesAccountCalendar(
        dates=dates, decision_dates=decision_dates, account_only_mask=account_only_mask,
        decision_indices=decision_indices, account_only_indices=event_indices,
        prior_decision_index=prior, fingerprint=digest.hexdigest(),
        training_trailing_event_credit=training_trailing_event_credit,
    )


def _checked_dates(values, name: str) -> np.ndarray:
    dates = np.asarray(values, dtype="datetime64[D]")
    if dates.ndim != 1 or not dates.size or np.isnat(dates).any():
        raise ValueError(f"{name} must be a non-empty finite date axis")
    if np.any(dates[1:] <= dates[:-1]):
        raise ValueError(f"{name} must be strictly increasing and unique")
    return dates


def build_futures_account_calendar(
    decision_dates, source_dates, source_accounting_only,
) -> FuturesAccountCalendar:
    """Include source-proven account-only dates within the decision horizon.

    A source market session missing from the stock context is still an error;
    it cannot be treated as a non-trading day. A stock decision without any
    corresponding futures source session is also unknown, not an empty market
    or a zero account mark. Source history before and
    after the specified decision horizon is neither consumed nor validated as
    part of that horizon. A futures-account-only day that is also a stock
    decision needs a separate market-observation mask and is rejected until
    that additional case is supported.
    """
    decisions = _checked_dates(decision_dates, "stock decision dates")
    source = _checked_dates(source_dates, "futures source dates")
    flags = np.asarray(source_accounting_only)
    if flags.ndim != 1 or flags.shape != source.shape or flags.dtype != np.bool_:
        raise ValueError("futures source accounting flags must be complete booleans")
    within = (source >= decisions[0]) & (source <= decisions[-1])
    source, flags = source[within], flags[within]
    is_decision = np.isin(source, decisions)
    if np.any(is_decision & flags):
        raise ValueError("a stock decision coincides with a futures-account-only source date")
    missing_source = decisions[~np.isin(decisions, source)]
    if missing_source.size:
        raise ValueError(
            f"futures source omits {len(missing_source)} stock decision sessions; "
            f"first={missing_source[0]} last={missing_source[-1]}; "
            "missing observations do not establish a closed futures market"
        )
    missing_market = source[~is_decision & ~flags]
    if missing_market.size:
        raise ValueError(
            f"stock context omits {len(missing_market)} futures market sessions; "
            f"first={missing_market[0]} last={missing_market[-1]}"
        )
    extra = source[~is_decision & flags]
    dates = np.union1d(decisions, extra)
    event_indices = np.searchsorted(dates, extra).astype(np.int64)
    mask = np.zeros(len(dates), dtype=bool)
    mask[event_indices] = True
    result = _calendar_from_nodes(dates, decisions, mask)
    if np.any(result.prior_decision_index < 0):
        raise ValueError("account-only nodes require a prior decision session")
    return result


def slice_futures_account_calendar(
    calendar: FuturesAccountCalendar, start: int, stop: int,
) -> FuturesAccountCalendar:
    """Slice decisions while retaining events just before the first decision.

    An event is owned by the first *following* decision. At a chunk or fold
    boundary its incoming account state belongs to the preceding slice; it
    must be passed to the ledger instead of resetting inventory or moving the
    event to another date. For such a first event ``prior_decision_index`` is
    -1, explicitly indicating that its preceding decision lies outside the
    slice.
    """
    n = len(calendar.decision_dates)
    if not 0 <= start < stop <= n:
        raise IndexError("account calendar decision slice must be nonempty and in range")
    first = 0 if start == 0 else int(calendar.decision_indices[start - 1]) + 1
    last = int(calendar.decision_indices[stop - 1]) + 1
    return _calendar_from_nodes(
        calendar.dates[first:last].copy(), calendar.decision_dates[start:stop].copy(),
        calendar.account_only_mask[first:last].copy(),
    )


def slice_futures_account_calendar_batch(
    calendar: FuturesAccountCalendar, account_execution: torch.Tensor,
    decision_indices,
) -> FuturesAccountCalendarBatch:
    """Select a contiguous decision batch and its original, dated events.

    ``decision_indices`` refer to the original stock decision axis, not to
    account-node positions. An event between batches belongs to the first
    following decision batch, which must receive the preceding account state.
    Reject skipped decisions: silently jumping over their market valuation or
    policy action would change the recurrent ledger. The event selection is a
    slice, retaining the original sparse tensor without a dense daily copy.
    """
    indices = np.asarray(decision_indices)
    if (indices.ndim != 1 or not len(indices)
            or not np.issubdtype(indices.dtype, np.integer)
            or np.any(indices[1:] != indices[:-1] + 1)):
        raise ValueError("account calendar batches require nonempty contiguous decision indices")
    if not isinstance(account_execution, torch.Tensor) or account_execution.ndim != 3:
        raise ValueError("account event tape must have shape [event, slot, field]")
    if account_execution.shape[0] != len(calendar.account_only_indices):
        raise ValueError("account event tape does not match the original account calendar")
    start, stop = int(indices[0]), int(indices[-1]) + 1
    sliced = slice_futures_account_calendar(calendar, start, stop)
    first = 0 if start == 0 else int(calendar.decision_indices[start - 1]) + 1
    last = int(calendar.decision_indices[stop - 1]) + 1
    event_start = int(np.searchsorted(calendar.account_only_indices, first, side="left"))
    event_stop = int(np.searchsorted(calendar.account_only_indices, last, side="left"))
    events = account_execution[event_start:event_stop]
    if events.shape[0] != len(sliced.account_only_indices):
        raise RuntimeError("account event slice and calendar disagree")
    return FuturesAccountCalendarBatch(sliced, events, start, stop, event_start, event_stop)


def next_decision_after_market_rows(
    calendar: FuturesAccountCalendar, account_indices,
) -> np.ndarray:
    """Map source market rows to the next policy session on a dual calendar.

    Account-only rows are not market features and return -1. A terminal
    market row with no following decision also returns -1. The mapping is
    independent of contract identity: the caller must retain the existing
    same-physical-contract admission and point-in-time feature checks.
    """
    indices = np.asarray(account_indices)
    if (indices.ndim != 1 or not np.issubdtype(indices.dtype, np.integer)
            or np.any(indices < 0) or np.any(indices >= len(calendar.dates))):
        raise ValueError("source calendar indices must be valid integer account nodes")
    positions = np.searchsorted(calendar.decision_indices, indices, side="right")
    valid = (positions < len(calendar.decision_indices)) & ~calendar.account_only_mask[indices]
    result = np.full(len(indices), -1, dtype=np.int64)
    result[valid] = calendar.decision_indices[positions[valid]]
    return result


def interleave_account_only_ledger_rows(
    calendar: FuturesAccountCalendar, decision_actions: torch.Tensor,
    decision_execution: torch.Tensor, account_execution: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Insert non-executable account rows without evaluating a new policy.

    Accounting metadata must already have passed the canonical margin-source
    validation.  Cash settlement can retire inventory without a trade; a
    required market close cannot be invented on an account-only date.
    """
    from stockagent.data import tw_futures_margin as margin

    n, e = len(calendar.decision_dates), len(calendar.account_only_indices)
    if decision_actions.ndim not in (2, 3) or decision_actions.shape[0] != n:
        raise ValueError("decision actions must have shape [decision, slot, ...]")
    if (decision_execution.ndim != 3 or decision_execution.shape[0] != n
            or decision_execution.shape[1] != decision_actions.shape[1]
            or decision_execution.shape[-1] not in margin.MARGIN_EXECUTION_WIDTHS):
        raise ValueError("decision execution must be the exact margin ledger tape")
    if (account_execution.ndim != 3 or account_execution.shape[0] != e
            or account_execution.shape[1:] != decision_execution.shape[1:]):
        raise ValueError("account execution must match the dated margin tape")
    if decision_execution.device != account_execution.device or decision_actions.device != decision_execution.device:
        raise ValueError("account, decision and policy tensors must share a device")
    if decision_execution.dtype != account_execution.dtype:
        raise ValueError("account and decision execution dtypes must match")
    # The account event source is exact: missing values cannot be interpreted
    # as zero order capacity or a nonterminal event.
    if e:
        executable = account_execution[..., 1]
        capacity = account_execution[..., 8]
        terminal = account_execution[..., 2]
        cash = account_execution[..., margin.CASH_SETTLEMENT]
        if (torch.any(~torch.isfinite(executable)) or torch.any(~torch.isfinite(capacity))
                or torch.any(executable != 0) or torch.any(capacity != 0)):
            raise ValueError("account-only dates cannot contain executable capacity")
        if (torch.any(~torch.isfinite(terminal)) or torch.any(~torch.isfinite(cash))
                or torch.any((terminal != 0) & (terminal != 1))
                or torch.any((cash != 0) & (cash != 1)) or torch.any(terminal != cash)):
            raise ValueError("account-only terminal obligations require cash settlement")
    order = np.empty(len(calendar.dates), dtype=np.int64)
    order[calendar.decision_indices] = np.arange(n)
    order[calendar.account_only_indices] = n + np.arange(e)
    index = torch.as_tensor(order, device=decision_actions.device, dtype=torch.long)
    empty_actions = decision_actions.new_zeros((e, *decision_actions.shape[1:]))
    actions = torch.cat((decision_actions, empty_actions), dim=0).index_select(0, index)
    execution = torch.cat((decision_execution, account_execution), dim=0).index_select(0, index)
    return actions, execution


def aggregate_account_log_returns_to_decisions(
    calendar: FuturesAccountCalendar, account_log_returns: torch.Tensor,
) -> torch.Tensor:
    """Attribute inter-session account P&L to the next decision session.

    This aggregation is for a market-session metric alongside the unmodified
    dated account ledger; it must not replace or relabel the event-day record.
    """
    if account_log_returns.ndim != 1 or account_log_returns.shape[0] != len(calendar.dates):
        raise ValueError("account log returns must align with every account date")
    indices = np.searchsorted(calendar.decision_dates, calendar.dates, side="left")
    if np.any(indices == len(calendar.decision_dates)):
        if not calendar.training_trailing_event_credit:
            raise ValueError("a trailing account event has no following decision session")
        # Only training chunks created for a boundary may attribute a real
        # trailing event to their last decision.  This keeps its gradient in
        # the chunk that chose the carried position. Formal dated reports use
        # the original calendar and next-decision attribution instead.
        indices = np.minimum(indices, len(calendar.decision_dates) - 1)
    result = account_log_returns.new_zeros((len(calendar.decision_dates),))
    return result.index_add(0, torch.as_tensor(indices, device=result.device), account_log_returns)
