"""Source-pinned, dated audit payload for the dual-calendar futures ledger.

The payload is an extension for the canonical backtest archive, not an
independent training or data-publication path.  Building it does not enable a
dual-calendar experiment: the trainer, checkpoint and reporting contracts must
all adopt the same source identity before the existing admission guards move.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import math
import re
from typing import Mapping

import numpy as np
import torch

from stockagent.backtest.tw_futures_account_calendar import FuturesAccountCalendarResult
from stockagent.data.tw_futures_account_calendar import _calendar_from_nodes
from stockagent.data.tw_futures_margin import MARGIN_AUDIT_COLUMNS
from stockagent.training.futures_account_artifact_contract import (
    FUTURES_ACCOUNT_ARTIFACT_ABI, FUTURES_ACCOUNT_CONTEXT_FIELDS,
    FUTURES_ACCOUNT_LEDGER_FIELDS, FUTURES_ACCOUNT_METRICS_ABI, FUTURES_ACCOUNT_PAYLOAD_FIELDS,
    FUTURES_ACCOUNT_PREFIX,
)


_PREFIX = FUTURES_ACCOUNT_PREFIX


@dataclass(frozen=True, slots=True)
class FuturesAccountArtifactContext:
    """The caller's accepted source and incoming physical account state."""

    universe: tuple[str, ...]
    event_source_fingerprint: str
    execution_release_fingerprint: str
    expected_calendar_fingerprint: str
    initial_capital: float
    initial_equity_scale: float
    initial_alive: bool
    initial_quantities: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class DatedFuturesAccountLedger:
    dates: np.ndarray
    decision_dates: np.ndarray
    account_only_mask: np.ndarray
    strategy_returns: np.ndarray
    decision_returns: np.ndarray
    turnovers: np.ndarray
    opening_quantities: np.ndarray
    residual_quantities: np.ndarray
    equity_scale: np.ndarray
    default: np.ndarray
    default_reason: np.ndarray
    margin_audit: np.ndarray
    final_quantities: np.ndarray
    final_equity_scale: np.ndarray
    final_alive: np.ndarray
    calendar_fingerprint: str


_LEDGER_FIELDS = tuple(DatedFuturesAccountLedger.__dataclass_fields__)
_CONTEXT_FIELDS = tuple(FuturesAccountArtifactContext.__dataclass_fields__)
if _LEDGER_FIELDS != FUTURES_ACCOUNT_LEDGER_FIELDS or _CONTEXT_FIELDS != FUTURES_ACCOUNT_CONTEXT_FIELDS:
    raise RuntimeError("dated futures artifact classes and container contract disagree")
_PAYLOAD_FIELDS = FUTURES_ACCOUNT_PAYLOAD_FIELDS


def _validate_context(context: FuturesAccountArtifactContext) -> None:
    if not isinstance(context, FuturesAccountArtifactContext):
        raise ValueError("dated futures account requires a pinned source context")
    if (not isinstance(context.universe, tuple) or not context.universe
            or any(not isinstance(s, str) or not s for s in context.universe)
            or len(set(context.universe)) != len(context.universe)):
        raise ValueError("dated futures account requires a unique ordered slot universe")
    for fingerprint in (context.event_source_fingerprint, context.execution_release_fingerprint,
                        context.expected_calendar_fingerprint):
        if not isinstance(fingerprint, str) or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None:
            raise ValueError("dated futures account source fingerprints must be SHA256")
    if (isinstance(context.initial_capital, bool) or not math.isfinite(context.initial_capital)
            or context.initial_capital <= 0 or isinstance(context.initial_equity_scale, bool)
            or not math.isfinite(context.initial_equity_scale) or context.initial_equity_scale < 0
            or not isinstance(context.initial_alive, bool)
            or context.initial_alive != (context.initial_equity_scale > 0)):
        raise ValueError("dated futures account has invalid incoming equity or alive state")
    if (not isinstance(context.initial_quantities, tuple)
            or len(context.initial_quantities) != len(context.universe)
            or any(isinstance(q, bool) or not isinstance(q, int) for q in context.initial_quantities)
            or (not context.initial_alive and any(context.initial_quantities))):
        raise ValueError("dated futures account requires whole incoming contract quantities")


def _validate_ledger(ledger: DatedFuturesAccountLedger, context: FuturesAccountArtifactContext) -> None:
    _validate_context(context)
    dates, decisions, event = ledger.dates, ledger.decision_dates, ledger.account_only_mask
    if (dates.dtype != np.dtype("datetime64[D]") or dates.ndim != 1 or not len(dates)
            or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1])
            or decisions.dtype != np.dtype("datetime64[D]") or decisions.ndim != 1
            or not len(decisions) or np.isnat(decisions).any()
            or np.any(decisions[1:] <= decisions[:-1])
            or event.dtype != np.bool_ or event.shape != dates.shape
            or not np.array_equal(dates[~event], decisions) or event[-1]):
        raise ValueError("dated futures account calendar and decision dates disagree")
    calendar = _calendar_from_nodes(dates, decisions, event)
    if calendar.fingerprint != ledger.calendar_fingerprint:
        raise ValueError("dated futures account calendar fingerprint changed")
    if ledger.calendar_fingerprint != context.expected_calendar_fingerprint:
        raise ValueError("dated futures account calendar differs from the pinned source slice")
    n, d, s = len(dates), len(decisions), len(context.universe)
    shapes = {
        "strategy_returns": ((n,), np.float32), "decision_returns": ((d,), np.float32),
        "turnovers": ((n,), np.float32), "opening_quantities": ((n, s), np.int64),
        "residual_quantities": ((n, s), np.int64), "equity_scale": ((n,), np.float32),
        "default": ((n,), np.bool_), "default_reason": ((n,), np.int64),
        "margin_audit": ((n, len(MARGIN_AUDIT_COLUMNS)), np.float32),
        "final_quantities": ((s,), np.int64), "final_equity_scale": ((), np.float32),
        "final_alive": ((), np.bool_),
    }
    for name, (shape, dtype) in shapes.items():
        value = getattr(ledger, name)
        if (not isinstance(value, np.ndarray) or value.shape != shape or value.dtype != dtype
                or not np.isfinite(value).all()):
            raise ValueError(f"dated futures account {name} has an invalid shape, dtype or value")
    if np.any(ledger.turnovers < 0) or np.any(ledger.equity_scale < 0):
        raise ValueError("dated futures account turnover and equity must be nonnegative")
    alive = ledger.equity_scale > 0
    previous_alive = np.concatenate(([context.initial_alive], alive[:-1]))
    expected_default = previous_alive & ~alive
    if (np.any(~previous_alive & alive) or not np.array_equal(ledger.default, expected_default)
            or np.any((ledger.default_reason != 0) != ledger.default)
            or np.any(ledger.default_reason < 0) or np.any(ledger.default_reason > 4)
            or bool(ledger.final_alive) != bool(alive[-1])
            or ledger.final_equity_scale != ledger.equity_scale[-1]):
        raise ValueError("dated futures account default must be absorbing and correctly attributed")
    expected_final = ledger.residual_quantities[-1] if alive[-1] else np.zeros(s, dtype=np.int64)
    if not np.array_equal(ledger.final_quantities, expected_final):
        raise ValueError("dated futures account final and residual contract quantities disagree")
    prior_equity = np.concatenate(([context.initial_equity_scale], ledger.equity_scale[:-1]))
    prior_equity = prior_equity * context.initial_capital
    if not np.allclose(ledger.margin_audit[:, 0], prior_equity, rtol=2e-6, atol=.02):
        raise ValueError("dated futures account prior equity is not continuous")
    if not np.allclose(ledger.margin_audit[alive, 2],
                       ledger.equity_scale[alive] * context.initial_capital, rtol=2e-6, atol=.02):
        raise ValueError("dated futures account marked and reported equity disagree")
    expected_returns = np.zeros(n, dtype=np.float32)
    continuing = previous_alive & alive
    with np.errstate(divide="ignore", invalid="ignore"):
        expected_returns[continuing] = np.log1p(
            ledger.margin_audit[continuing, 2] / ledger.margin_audit[continuing, 0] - np.float32(1))
    expected_returns[ledger.default] = np.log1p(np.float32(-1 + 1e-7))
    if not np.allclose(ledger.strategy_returns, expected_returns, rtol=2e-5, atol=2e-6):
        raise ValueError("dated futures account returns disagree with marked equity")
    expected_decisions = np.zeros(d, dtype=np.float32)
    np.add.at(expected_decisions, np.searchsorted(decisions, dates, side="left"), ledger.strategy_returns)
    if not np.allclose(ledger.decision_returns, expected_decisions, rtol=2e-6, atol=2e-6):
        raise ValueError("dated futures account decision returns omit or move an account event")


def futures_account_artifact_payload(
    result: FuturesAccountCalendarResult, context: FuturesAccountArtifactContext,
) -> dict[str, np.ndarray]:
    """Encode all real dated account nodes with the pinned incoming state."""
    if not isinstance(result, FuturesAccountCalendarResult):
        raise ValueError("dated futures account archive requires the original tensor ledger")
    source = result.account_ledger
    def array(value: torch.Tensor | None, name: str) -> np.ndarray:
        if not isinstance(value, torch.Tensor):
            raise ValueError(f"dated futures account requires recorded {name}")
        return value.detach().cpu().numpy().copy()
    _validate_context(context)
    if ((result.event_source_fingerprint is not None or result.execution_release_fingerprint is not None)
            and (result.event_source_fingerprint != context.event_source_fingerprint
                 or result.execution_release_fingerprint != context.execution_release_fingerprint)):
        raise ValueError("dated futures result provenance differs from its pinned context")
    if (result.initial_capital != context.initial_capital
            or not np.array_equal(array(result.initial_quantities, "incoming quantities"),
                                  np.asarray(context.initial_quantities, dtype=np.int64))
            or not np.array_equal(array(result.initial_equity_scale, "incoming equity"),
                                  np.asarray(context.initial_equity_scale, dtype=np.float32))
            or not np.array_equal(array(result.initial_alive, "incoming alive state"),
                                  np.asarray(context.initial_alive, dtype=np.bool_))):
        raise ValueError("dated futures account context differs from the actual incoming state")
    final_weights = array(source.final_weights, "final quantities")
    if (final_weights.ndim != 1 or not np.isfinite(final_weights).all()
            or not np.array_equal(final_weights, np.rint(final_weights))):
        raise ValueError("dated futures account final quantities must be whole contracts")
    ledger = DatedFuturesAccountLedger(
        dates=result.calendar.dates.copy(), decision_dates=result.calendar.decision_dates.copy(),
        account_only_mask=result.calendar.account_only_mask.copy(),
        strategy_returns=array(source.strategy_returns, "strategy returns"),
        decision_returns=array(result.decision_log_returns, "decision returns"),
        turnovers=array(source.turnovers, "turnovers"),
        opening_quantities=array(source.contract_quantities_history, "opening quantities"),
        residual_quantities=array(source.residual_contract_quantities_history, "residual quantities"),
        equity_scale=array(source.equity_scale_history, "equity history"),
        default=array(source.default_history, "default history"),
        default_reason=array(source.default_reason_history, "default reasons"),
        margin_audit=array(source.margin_audit_history, "margin audit"),
        final_quantities=final_weights.astype(np.int64),
        final_equity_scale=array(source.final_equity_scale, "final equity"),
        final_alive=array(source.final_alive, "final alive"),
        calendar_fingerprint=result.calendar.fingerprint,
    )
    return futures_account_ledger_payload(ledger, context)


def futures_account_ledger_payload(
    ledger: DatedFuturesAccountLedger, context: FuturesAccountArtifactContext,
) -> dict[str, np.ndarray]:
    """Encode an already materialized ledger after the same source checks."""
    if not isinstance(ledger, DatedFuturesAccountLedger):
        raise ValueError("dated futures account archive requires a complete ledger")
    _validate_ledger(ledger, context)
    payload = {f"{_PREFIX}{name}": np.asarray(getattr(ledger, name)) for name in _LEDGER_FIELDS}
    payload.update({f"{_PREFIX}{name}": np.asarray(getattr(context, name)) for name in _CONTEXT_FIELDS})
    payload[f"{_PREFIX}initial_quantities"] = np.asarray(context.initial_quantities, dtype=np.int64)
    payload[f"{_PREFIX}abi"] = np.asarray(FUTURES_ACCOUNT_ARTIFACT_ABI)
    payload[f"{_PREFIX}metrics_abi"] = np.asarray(FUTURES_ACCOUNT_METRICS_ABI)
    payload[f"{_PREFIX}margin_audit_columns"] = np.asarray(MARGIN_AUDIT_COLUMNS)
    return payload


def fresh_futures_account_context_for_dates(
    source,
    decision_dates,
    *,
    execution_release_fingerprint: str,
    initial_capital: float,
) -> FuturesAccountArtifactContext:
    """Pin a fresh flat account to a contiguous slice of an accepted source.

    This is for independent fold-test artifacts.  A carried deployment must
    instead derive its incoming state from a verified preceding ledger; the
    dates in an archive alone cannot establish that account state.
    """
    from stockagent.data.tw_futures_account_calendar import slice_futures_account_calendar
    from stockagent.training.futures_account_bridge import PreparedFuturesAccountSource

    if not isinstance(source, PreparedFuturesAccountSource) or source.decision_execution is None:
        raise ValueError("fresh dated futures account requires a prepared execution release")
    source.verify_identity()
    if execution_release_fingerprint != source.execution_release_fingerprint:
        raise ValueError("fresh dated futures account execution release differs from the prepared source")
    dates = np.asarray(decision_dates)
    if (dates.dtype != np.dtype("datetime64[D]") or dates.ndim != 1 or not len(dates)
            or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1])):
        raise ValueError("fresh dated futures account requires strictly increasing decision dates")
    source_dates = source.calendar.decision_dates
    indices = np.searchsorted(source_dates, dates)
    if (np.any(indices >= len(source_dates))
            or not np.array_equal(source_dates[indices], dates)
            or np.any(indices[1:] != indices[:-1] + 1)):
        raise ValueError("fresh dated futures account dates must be a contiguous source slice")
    calendar = slice_futures_account_calendar(source.calendar, int(indices[0]), int(indices[-1]) + 1)
    context = FuturesAccountArtifactContext(
        universe=source.universe,
        event_source_fingerprint=source.fingerprint,
        execution_release_fingerprint=execution_release_fingerprint,
        expected_calendar_fingerprint=calendar.fingerprint,
        initial_capital=initial_capital,
        initial_equity_scale=1., initial_alive=True,
        initial_quantities=(0,) * len(source.universe),
    )
    _validate_context(context)
    return context


def bind_futures_account_result_context(
    result: FuturesAccountCalendarResult,
    source,
    split,
    *,
    execution_release_fingerprint: str,
    initial_capital: float,
    incoming_context: FuturesAccountArtifactContext | None = None,
) -> FuturesAccountArtifactContext:
    """Bind an evaluated account to its exact source and incoming state.

    Without an incoming context this is a *fresh, flat* evaluation, never a
    continuation.  A stitched account must instead pass a context derived
    from its preceding verified ledger by ``continue_futures_account_context``.
    No result data is used to infer a missing incoming position.
    """
    from stockagent.training.futures_account_bridge import (
        PreparedFuturesAccountSource, verify_futures_account_binding,
    )

    if not isinstance(result, FuturesAccountCalendarResult):
        raise ValueError("account result binding requires the original dated tensor ledger")
    if not isinstance(source, PreparedFuturesAccountSource):
        raise ValueError("account result binding requires the prepared dated source")
    source.verify_identity()
    verify_futures_account_binding(split, source)
    source.validate_training_split(split)
    if (result.event_source_fingerprint != source.fingerprint
            or result.execution_release_fingerprint != execution_release_fingerprint):
        raise ValueError("account result provenance differs from the evaluated source or execution release")
    sample = getattr(split, "sample_mask", None)
    if sample is not None and not bool(torch.all(sample)):
        raise ValueError("account result binding cannot use padded decision rows")
    if (source.decision_execution is not None
            and execution_release_fingerprint != source.execution_release_fingerprint):
        raise ValueError("account result execution release differs from the prepared source")
    calendar = source.prepare_batch(split, 0, len(split._valid_indices_cpu)).calendar
    if result.calendar.fingerprint != calendar.fingerprint:
        raise ValueError("account result calendar differs from the prepared source slice")
    context = incoming_context
    if context is None:
        context = FuturesAccountArtifactContext(
            universe=source.universe,
            event_source_fingerprint=source.fingerprint,
            execution_release_fingerprint=execution_release_fingerprint,
            expected_calendar_fingerprint=calendar.fingerprint,
            initial_capital=initial_capital,
            initial_equity_scale=1., initial_alive=True,
            initial_quantities=(0,) * len(source.universe),
        )
    _validate_context(context)
    if (context.universe != source.universe
            or context.event_source_fingerprint != source.fingerprint
            or context.execution_release_fingerprint != execution_release_fingerprint
            or context.expected_calendar_fingerprint != calendar.fingerprint
            or context.initial_capital != initial_capital):
        raise ValueError("account result context differs from the prepared source or execution release")
    # Check only the small incoming state here; archive construction checks
    # the full dated ledger separately when it is materialized for reporting.
    def incoming(value: torch.Tensor, name: str) -> np.ndarray:
        if not isinstance(value, torch.Tensor):
            raise ValueError(f"account result requires recorded incoming {name}")
        return value.detach().cpu().numpy()

    if (result.initial_capital != context.initial_capital
            or not np.array_equal(incoming(result.initial_quantities, "quantities"),
                                  np.asarray(context.initial_quantities, dtype=np.int64))
            or not np.array_equal(incoming(result.initial_equity_scale, "equity"),
                                  np.asarray(context.initial_equity_scale, dtype=np.float32))
            or not np.array_equal(incoming(result.initial_alive, "alive state"),
                                  np.asarray(context.initial_alive, dtype=np.bool_))):
        raise ValueError("account result context differs from the actual incoming state")
    return context


def continue_futures_account_context(
    ledger: DatedFuturesAccountLedger,
    context: FuturesAccountArtifactContext,
    source,
    next_split,
    *,
    execution_release_fingerprint: str,
) -> FuturesAccountArtifactContext:
    """Bind a validated previous account to the immediately next decisions.

    No event or position is fabricated: the previous actual terminal state is
    carried into the next source slice, including account-only dates before its
    first decision. The release fingerprint must come from the caller's
    independently checked execution release, not from the stored ledger alone.
    """
    from stockagent.data.tw_futures_account_calendar import slice_futures_account_calendar
    from stockagent.training.futures_account_bridge import (
        PreparedFuturesAccountSource, verify_futures_account_binding,
    )

    if not isinstance(source, PreparedFuturesAccountSource):
        raise ValueError("account continuation requires the prepared dated source")
    source.verify_identity()
    verify_futures_account_binding(next_split, source)
    source.validate_training_split(next_split)
    _validate_ledger(ledger, context)
    if (context.event_source_fingerprint != source.fingerprint
            or context.universe != source.universe
            or context.execution_release_fingerprint != execution_release_fingerprint):
        raise ValueError("account continuation source, universe or execution release differs")
    source_dates = source.calendar.decision_dates
    previous_indices = np.searchsorted(source_dates, ledger.decision_dates)
    if (np.any(previous_indices >= len(source_dates))
            or not np.array_equal(source_dates[previous_indices], ledger.decision_dates)
            or np.any(previous_indices[1:] != previous_indices[:-1] + 1)):
        raise ValueError("previous account decisions are not a contiguous source slice")
    previous_calendar = slice_futures_account_calendar(
        source.calendar, int(previous_indices[0]), int(previous_indices[-1]) + 1,
    )
    if previous_calendar.fingerprint != ledger.calendar_fingerprint:
        raise ValueError("previous account dates differ from the prepared source calendar")
    next_indices = next_split._valid_indices_cpu
    sample = getattr(next_split, "sample_mask", None)
    if sample is not None and not bool(torch.all(sample)):
        raise ValueError("account continuation cannot use padded decision rows")
    if int(next_indices[0]) != int(previous_indices[-1]) + 1:
        raise ValueError("account continuation requires the immediately next source decision")
    next_calendar = source.prepare_batch(next_split, 0, len(next_indices)).calendar
    return replace(
        context, expected_calendar_fingerprint=next_calendar.fingerprint,
        initial_quantities=tuple(int(value) for value in ledger.final_quantities),
        initial_equity_scale=float(ledger.final_equity_scale),
        initial_alive=bool(ledger.final_alive),
    )


def slice_futures_account_ledger(
    ledger: DatedFuturesAccountLedger, context: FuturesAccountArtifactContext,
    start: int, stop: int,
) -> tuple[DatedFuturesAccountLedger, FuturesAccountArtifactContext]:
    """Slice decision rows while carrying real intervening events and state."""
    _validate_ledger(ledger, context)
    n = len(ledger.decision_dates)
    if (isinstance(start, bool) or isinstance(stop, bool)
            or not isinstance(start, int) or not isinstance(stop, int)
            or not 0 <= start < stop <= n):
        raise ValueError("dated futures account slice requires nonempty decision rows")
    decisions = np.flatnonzero(~ledger.account_only_mask)
    first = 0 if start == 0 else int(decisions[start - 1]) + 1
    last = int(decisions[stop - 1]) + 1
    dates = ledger.dates[first:last].copy()
    decision_dates = ledger.decision_dates[start:stop].copy()
    flags = ledger.account_only_mask[first:last].copy()
    calendar = _calendar_from_nodes(dates, decision_dates, flags)
    if start == 0:
        initial_equity = context.initial_equity_scale
        initial_alive = context.initial_alive
        initial_quantities = context.initial_quantities
    else:
        previous = int(decisions[start - 1])
        initial_equity = float(ledger.equity_scale[previous])
        initial_alive = initial_equity > 0
        initial_quantities = tuple(int(q) for q in ledger.residual_quantities[previous])
        if not initial_alive:
            initial_quantities = (0,) * len(context.universe)
    sliced_context = replace(
        context, expected_calendar_fingerprint=calendar.fingerprint,
        initial_equity_scale=initial_equity, initial_alive=initial_alive,
        initial_quantities=initial_quantities,
    )
    final_alive = bool(ledger.equity_scale[last - 1] > 0)
    final_quantities = (ledger.residual_quantities[last - 1].copy() if final_alive
                        else np.zeros(len(context.universe), dtype=np.int64))
    sliced = DatedFuturesAccountLedger(
        dates=dates, decision_dates=decision_dates, account_only_mask=flags,
        strategy_returns=ledger.strategy_returns[first:last].copy(),
        decision_returns=ledger.decision_returns[start:stop].copy(),
        turnovers=ledger.turnovers[first:last].copy(),
        opening_quantities=ledger.opening_quantities[first:last].copy(),
        residual_quantities=ledger.residual_quantities[first:last].copy(),
        equity_scale=ledger.equity_scale[first:last].copy(),
        default=ledger.default[first:last].copy(),
        default_reason=ledger.default_reason[first:last].copy(),
        margin_audit=ledger.margin_audit[first:last].copy(),
        final_quantities=final_quantities,
        final_equity_scale=np.asarray(ledger.equity_scale[last - 1], dtype=np.float32),
        final_alive=np.asarray(final_alive, dtype=np.bool_),
        calendar_fingerprint=calendar.fingerprint,
    )
    _validate_ledger(sliced, sliced_context)
    return sliced, sliced_context


def validate_dated_futures_backtest(result, dates, ledger: DatedFuturesAccountLedger) -> None:
    """Check that legacy decision rows are views of the full dated ledger."""
    if (getattr(result, "execution_mode", None) != "tw_stock_context_futures_portfolio"
            or getattr(result, "settlement_ledger_unit", None) != "contract_quantity"
            or not np.array_equal(np.asarray(dates, dtype="datetime64[D]"), ledger.decision_dates)):
        raise ValueError("dated futures backtest mode, unit or decision dates disagree")
    indices = ~ledger.account_only_mask
    checks = (
        ("strategy_returns", ledger.decision_returns),
        ("equity_scale_history", ledger.equity_scale[indices]),
        ("futures_margin_audit", ledger.margin_audit[indices]),
        ("futures_contract_quantities_history", ledger.opening_quantities[indices]),
        ("futures_residual_contract_quantities_history", ledger.residual_quantities[indices]),
        ("final_weights", ledger.final_quantities),
        ("final_equity_scale", ledger.final_equity_scale),
        ("final_alive", ledger.final_alive),
    )
    for name, expected in checks:
        actual = getattr(result, name, None)
        if actual is None or not np.array_equal(np.asarray(actual), expected):
            raise ValueError(f"dated futures backtest {name} disagrees with its account ledger")
    owner = np.searchsorted(ledger.decision_dates, ledger.dates, side="left")
    turnover = np.zeros(len(ledger.decision_dates), dtype=np.float32)
    defaults = np.zeros(len(ledger.decision_dates), dtype=np.int64)
    reasons = np.zeros(len(ledger.decision_dates), dtype=np.int64)
    np.add.at(turnover, owner, ledger.turnovers)
    np.add.at(defaults, owner, ledger.default.astype(np.int64))
    np.add.at(reasons, owner, ledger.default_reason)
    for name, expected in (("turnovers", turnover), ("settlement_default", defaults.astype(bool)),
                           ("default_reason_history", reasons)):
        actual = getattr(result, name, None)
        if actual is None or not np.array_equal(np.asarray(actual), expected):
            raise ValueError(f"dated futures backtest {name} disagrees with its account ledger")


def futures_account_artifact_from_payload(
    payload: Mapping[str, np.ndarray], context: FuturesAccountArtifactContext,
) -> DatedFuturesAccountLedger:
    """Read only a complete extension under the caller's exact source pins."""
    _validate_context(context)
    keys = {key for key in payload if key.startswith(_PREFIX)}
    if keys != _PAYLOAD_FIELDS:
        raise ValueError(f"dated futures account fields differ: missing={sorted(_PAYLOAD_FIELDS - keys)} "
                         f"unknown={sorted(keys - _PAYLOAD_FIELDS)}")
    if (np.asarray(payload[f"{_PREFIX}abi"]).shape != ()
            or str(np.asarray(payload[f"{_PREFIX}abi"]).item()) != FUTURES_ACCOUNT_ARTIFACT_ABI
            or np.asarray(payload[f"{_PREFIX}metrics_abi"]).shape != ()
            or str(np.asarray(payload[f"{_PREFIX}metrics_abi"]).item()) != FUTURES_ACCOUNT_METRICS_ABI
            or not np.array_equal(np.asarray(payload[f"{_PREFIX}margin_audit_columns"]),
                                  np.asarray(MARGIN_AUDIT_COLUMNS))):
        raise ValueError("dated futures account ABI or margin audit columns differ")
    for name in _CONTEXT_FIELDS:
        actual = np.asarray(payload[f"{_PREFIX}{name}"])
        expected = np.asarray(getattr(context, name))
        if name == "initial_quantities":
            expected = np.asarray(context.initial_quantities, dtype=np.int64)
        if actual.dtype != expected.dtype or not np.array_equal(actual, expected):
            raise ValueError(f"dated futures account pinned context differs: {name}")
    values = {name: np.array(payload[f"{_PREFIX}{name}"], copy=True) for name in _LEDGER_FIELDS}
    fingerprint = values["calendar_fingerprint"]
    if fingerprint.shape != () or fingerprint.dtype.kind != "U":
        raise ValueError("dated futures account calendar fingerprint must be a string")
    values["calendar_fingerprint"] = str(fingerprint.item())
    ledger = DatedFuturesAccountLedger(**values)
    _validate_ledger(ledger, context)
    return ledger
