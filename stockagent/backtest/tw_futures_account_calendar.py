"""Run the canonical whole-contract futures ledger on two dated calendars.

The policy is evaluated only on stock decision sessions. Account-only events
are source-backed tape rows; the full dated result remains available alongside
decision-session log returns. This helper is not a data loader or training
configuration and does not admit a source or a model checkpoint by itself.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch

from stockagent.backtest.tw_futures_portfolio import (
    FuturesPortfolioTensorResult, run_tw_futures_portfolio_integer_torch,
)
from stockagent.data.tw_futures_account_calendar import (
    FuturesAccountCalendar, aggregate_account_log_returns_to_decisions,
    interleave_account_only_ledger_rows,
)


@dataclass(slots=True)
class FuturesAccountCalendarResult:
    """Real dated ledger and market-session attribution of its log returns."""

    calendar: FuturesAccountCalendar
    account_ledger: FuturesPortfolioTensorResult
    decision_log_returns: torch.Tensor
    initial_capital: float
    initial_quantities: torch.Tensor
    initial_equity_scale: torch.Tensor
    initial_alive: torch.Tensor
    # Filled only by the evaluator that has checked the prepared source and
    # split. A low-level direct ledger call does not assert source provenance.
    event_source_fingerprint: str | None = None
    execution_release_fingerprint: str | None = None


def merge_futures_account_calendar_results(
    results: list[FuturesAccountCalendarResult], expected: FuturesAccountCalendar,
) -> FuturesAccountCalendarResult:
    """Merge consecutive evaluation chunks without dropping dated account rows.

    Evaluation attributes inter-session events to the next decision. Training's
    special trailing credit is deliberately rejected here. Callers must pin
    ``expected`` to their verified source slice before merging.
    """
    if not results or expected.training_trailing_event_credit:
        raise ValueError("evaluation requires a nonempty original account calendar")
    if any(item.calendar.training_trailing_event_credit for item in results):
        raise ValueError("training event attribution cannot be used for evaluation")
    for field in ("dates", "decision_dates", "account_only_mask"):
        actual = np.concatenate([getattr(item.calendar, field) for item in results])
        if not np.array_equal(actual, getattr(expected, field)):
            raise ValueError(f"account evaluation chunks changed source {field}")
    if any(item.initial_capital != results[0].initial_capital for item in results):
        raise ValueError("account evaluation chunks do not share exact incoming state")
    if any((item.event_source_fingerprint, item.execution_release_fingerprint) !=
           (results[0].event_source_fingerprint, results[0].execution_release_fingerprint)
           for item in results):
        raise ValueError("account evaluation chunks do not share source provenance")
    if len(results) > 1:
        previous = [item.account_ledger for item in results[:-1]]
        incoming = results[1:]
        # Check all boundaries together to avoid one device synchronization
        # for each evaluation chunk. Compare in the original numeric type so
        # a malformed fractional contract cannot be hidden by integer casting.
        quantities = torch.stack([item.final_weights for item in previous])
        expected_quantities = torch.stack([item.initial_quantities for item in incoming]).to(quantities)
        equities = torch.stack([item.final_equity_scale for item in previous])
        expected_equities = torch.stack([item.initial_equity_scale for item in incoming]).to(equities)
        alive = torch.stack([item.final_alive for item in previous])
        expected_alive = torch.stack([item.initial_alive for item in incoming]).to(alive)
        if (not torch.equal(quantities, expected_quantities)
                or not torch.equal(equities, expected_equities)
                or not torch.equal(alive, expected_alive)):
            raise ValueError("account evaluation chunks do not share exact incoming state")

    def combine(name: str) -> torch.Tensor | None:
        values = [getattr(item.account_ledger, name) for item in results]
        if all(value is None for value in values):
            return None
        if any(not isinstance(value, torch.Tensor) for value in values):
            raise ValueError(f"account evaluation chunks disagree on {name}")
        return torch.cat(values, dim=0)

    last = results[-1].account_ledger
    ledger = FuturesPortfolioTensorResult(
        strategy_returns=combine("strategy_returns"),
        turnovers=combine("turnovers"),
        weights_history=combine("weights_history"),
        final_weights=last.final_weights, final_alive=last.final_alive,
        equity_scale_history=combine("equity_scale_history"),
        final_equity_scale=last.final_equity_scale,
        contract_quantities_history=combine("contract_quantities_history"),
        default_history=combine("default_history"),
        default_reason_history=combine("default_reason_history"),
        margin_audit_history=combine("margin_audit_history"),
        residual_contract_quantities_history=combine("residual_contract_quantities_history"),
        requested_weights_history=combine("requested_weights_history"),
    )
    decisions = torch.cat([item.decision_log_returns for item in results], dim=0)
    if not torch.equal(decisions, aggregate_account_log_returns_to_decisions(expected, ledger.strategy_returns)):
        raise ValueError("account evaluation chunks changed event-return attribution")
    first = results[0]
    return FuturesAccountCalendarResult(
        expected, ledger, decisions, first.initial_capital, first.initial_quantities,
        first.initial_equity_scale, first.initial_alive, first.event_source_fingerprint,
        first.execution_release_fingerprint,
    )


def run_futures_account_calendar_torch(
    calendar: FuturesAccountCalendar,
    decision_actions: torch.Tensor,
    decision_execution: torch.Tensor,
    account_execution: torch.Tensor,
    *,
    initial_capital: float,
    decision_state_advance: torch.Tensor | None = None,
    account_state_advance: torch.Tensor | None = None,
    initial_quantities: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    return_weights_history: bool = True,
    return_turnovers: bool = True,
    return_margin_audit: bool = True,
    recoverable_backward: bool = False,
) -> FuturesAccountCalendarResult:
    """Evaluate every account event with the existing recurrent integer ledger.

    When a caller supplies an advance mask, it must also explicitly say which
    account-only events advance. A padded or dropped policy row cannot silently
    decide the fate of a real settlement between sessions.
    """
    n, e = len(calendar.decision_dates), len(calendar.account_only_indices)
    if (e and calendar.prior_decision_index[calendar.account_only_indices[0]] < 0
            and any(value is None for value in
                    (initial_quantities, initial_equity_scale, initial_alive))):
        raise ValueError(
            "a batch beginning with an account event requires explicit incoming "
            "quantities, equity scale and alive state"
        )
    actions, execution = interleave_account_only_ledger_rows(
        calendar, decision_actions, decision_execution, account_execution,
    )
    # Retain the effective incoming state for artifact identity.  The exact
    # ledger converts the account to FP32 and rounds supplied quantities; take
    # independent snapshots using precisely those conversions before it runs.
    incoming_quantities = (
        torch.zeros(actions.shape[1], device=actions.device, dtype=torch.int64)
        if initial_quantities is None else
        torch.round(initial_quantities.detach().to(device=actions.device, dtype=torch.float32))
        .to(torch.int64).clone()
    )
    incoming_scale = (
        torch.ones((), device=actions.device, dtype=torch.float32)
        if initial_equity_scale is None else
        initial_equity_scale.detach().to(device=actions.device, dtype=torch.float32).reshape(()).clone()
    )
    incoming_alive = (
        torch.ones((), device=actions.device, dtype=torch.bool)
        if initial_alive is None else
        initial_alive.detach().to(device=actions.device, dtype=torch.bool).reshape(()).clone()
    )
    advance = None
    if decision_state_advance is not None or account_state_advance is not None:
        if (decision_state_advance is None or account_state_advance is None
                or decision_state_advance.shape != (n,) or account_state_advance.shape != (e,)
                or decision_state_advance.dtype != torch.bool or account_state_advance.dtype != torch.bool
                or decision_state_advance.device != actions.device or account_state_advance.device != actions.device):
            raise ValueError("advance masks must explicitly cover decisions and account events on the action device")
        order = torch.empty(len(calendar.dates), dtype=torch.long, device=actions.device)
        order[torch.as_tensor(calendar.decision_indices, device=actions.device)] = torch.arange(n, device=actions.device)
        order[torch.as_tensor(calendar.account_only_indices, device=actions.device)] = n + torch.arange(e, device=actions.device)
        advance = torch.cat((decision_state_advance, account_state_advance)).index_select(0, order)
    result = run_tw_futures_portfolio_integer_torch(
        actions, execution, initial_capital=initial_capital, state_advance_mask=advance,
        initial_quantities=initial_quantities, initial_equity_scale=initial_equity_scale,
        initial_alive=initial_alive, return_weights_history=return_weights_history,
        return_turnovers=return_turnovers, return_margin_audit=return_margin_audit,
        recoverable_backward=recoverable_backward,
    )
    return FuturesAccountCalendarResult(
        calendar=calendar, account_ledger=result,
        decision_log_returns=aggregate_account_log_returns_to_decisions(calendar, result.strategy_returns),
        initial_capital=float(initial_capital), initial_quantities=incoming_quantities,
        initial_equity_scale=incoming_scale, initial_alive=incoming_alive,
    )
