"""Prepare dated, sparse futures-account events for chronological batches.

This adapter does not enable an experiment.  The dataset, evaluator, artifact
writer and checkpoint contract must bind the same identity before an attached
dual-calendar panel can be admitted to the trainer.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Callable

import numpy as np
import torch

from stockagent.data.tw_futures_account_calendar import (
    FuturesAccountCalendar,
    FuturesAccountCalendarBatch,
    _calendar_from_nodes,
    build_futures_account_calendar,
    slice_futures_account_calendar_batch,
)
from stockagent.data.tw_futures_margin import CASH_SETTLEMENT, MARGIN_EXECUTION_WIDTHS


def _identity(calendar: FuturesAccountCalendar, events: torch.Tensor,
              universe: tuple[str, ...]) -> str:
    """Hash calendar, ordered slot identity and event bytes, including nulls."""
    if events.device.type != "cpu" or not events.is_contiguous():
        raise ValueError("account source identity requires contiguous CPU events")
    digest = hashlib.sha256()
    digest.update(b"stockagent_futures_account_source_v2\0")
    digest.update(calendar.fingerprint.encode("ascii"))
    for symbol in universe:
        encoded = symbol.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "little"))
        digest.update(encoded)
    for value in (calendar.dates, calendar.decision_dates, calendar.account_only_mask,
                  calendar.decision_indices, calendar.account_only_indices,
                  calendar.prior_decision_index):
        digest.update(np.ascontiguousarray(value).tobytes())
    digest.update(str(tuple(events.shape)).encode("ascii"))
    digest.update(str(events.dtype).encode("ascii"))
    digest.update(events.numpy().tobytes(order="C"))
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class PreparedFuturesAccountSource:
    """CPU-owned event tape and, optionally, a shared decision execution tape."""

    calendar: FuturesAccountCalendar
    events: torch.Tensor
    universe: tuple[str, ...]
    fingerprint: str
    decision_execution: torch.Tensor | None = None
    execution_release_fingerprint: str | None = None
    _decision_execution_version: int | None = None
    decision_session_mask: np.ndarray | None = None
    _decision_session_mask_digest: str | None = None

    @classmethod
    def from_daily(cls, daily, decision_dates, *, prepare_execution: bool = False) -> "PreparedFuturesAccountSource":
        calendar = getattr(daily, "futures_account_calendar", None)
        raw = getattr(daily, "account_only_integer_execution", None)
        if not isinstance(calendar, FuturesAccountCalendar) or raw is None:
            raise ValueError("prepared futures account source requires both calendar and event tape")
        if not np.array_equal(np.asarray(decision_dates, dtype="datetime64[D]"), calendar.decision_dates):
            raise ValueError("prepared futures account decisions do not match panel dates")
        owned_calendar = build_futures_account_calendar(
            calendar.decision_dates.copy(), calendar.dates.copy(), calendar.account_only_mask.copy(),
        )
        if (owned_calendar.fingerprint != calendar.fingerprint or any(
                not np.array_equal(getattr(owned_calendar, name), getattr(calendar, name))
                for name in ("decision_indices", "account_only_indices", "prior_decision_index"))):
            raise ValueError("prepared futures account calendar identity is inconsistent")
        for name in ("dates", "decision_dates", "account_only_mask", "decision_indices",
                     "account_only_indices", "prior_decision_index"):
            getattr(owned_calendar, name).setflags(write=False)
        decision_tape = getattr(daily, "integer_execution", None)
        if decision_tape is None:
            raise ValueError("prepared futures account source requires the decision execution tape")
        values = np.asarray(raw)
        expected = np.asarray(decision_tape).shape
        if (len(expected) != 3 or values.ndim != 3 or values.shape[0] != len(calendar.account_only_indices)
                or values.shape[1:] != expected[1:] or values.shape[-1] not in MARGIN_EXECUTION_WIDTHS
                or expected[0] != len(calendar.decision_dates)):
            raise ValueError("account event and decision execution shapes do not match")
        if values.dtype != np.float32:
            raise ValueError("account event tape must preserve its prepared float32 ABI")
        universe = getattr(daily, "symbols", None)
        if (not isinstance(universe, tuple) or len(universe) != expected[1]
                or any(not isinstance(symbol, str) or not symbol for symbol in universe)
                or len(set(universe)) != len(universe)):
            raise ValueError("account event source requires its unique ordered slot universe")
        capacity = values[..., [1, 8]]
        if np.any(~np.isfinite(capacity)) or np.any(capacity != 0):
            raise ValueError("account-only events cannot contain executable capacity")
        terminal = values[..., [2, CASH_SETTLEMENT]]
        if (np.any(~np.isfinite(terminal)) or np.any((terminal != 0) & (terminal != 1))
                or np.any(terminal[..., 0] != terminal[..., 1])):
            raise ValueError("account-only terminal events require cash settlement")
        # The attached panel remains independently owned and mutable.  Keep a
        # private copy so a later source edit cannot silently alter a batch.
        events = torch.from_numpy(np.array(values, dtype=np.float32, order="C", copy=True))
        source = cls(owned_calendar, events, universe, _identity(owned_calendar, events, universe))
        if not prepare_execution:
            return source
        # A margin-account sample boundary must not add a synthetic liquidation.
        # Consequently its packed decision tape is identical across splits and
        # can be owned, fingerprinted and allocated once for the entire runtime.
        if not getattr(daily, "margin_contract_version", 0):
            raise ValueError("dated futures execution requires the cash-denominated margin account ABI")
        packed = _pack_futures_decision_execution(daily)
        session_mask = _copy_futures_decision_session_mask(daily, expected[0])
        # Keep version tracking even when a caller prepares a source inside
        # inference_mode: the shared tape must remain mutation-checkable.
        with torch.inference_mode(False):
            execution = torch.from_numpy(packed)
        return cls(owned_calendar, events, universe, source.fingerprint, execution,
                   _futures_execution_fingerprint(daily, source, packed, session_mask), execution._version,
                   session_mask, _session_mask_digest(session_mask))

    def verify_identity(self) -> None:
        if _identity(self.calendar, self.events, self.universe) != self.fingerprint:
            raise ValueError("prepared futures account event source identity changed")
        if self.decision_execution is not None and (
            self.execution_release_fingerprint is None
            or self._decision_execution_version != self.decision_execution._version
            or self.decision_execution.device.type != "cpu"
            or self.decision_execution.dtype != torch.float32
            or not self.decision_execution.is_contiguous()
        ):
            raise ValueError("prepared futures decision execution was modified")
        if self.decision_execution is not None:
            mask = self.decision_session_mask
            if ((mask is not None and (not isinstance(mask, np.ndarray) or mask.dtype != np.bool_
                    or mask.shape != (len(self.calendar.decision_dates),) or not mask.flags.c_contiguous))
                    or _session_mask_digest(mask) != self._decision_session_mask_digest):
                raise ValueError("prepared futures decision session mask was modified")

    def validate_training_split(self, split) -> None:
        """Reject omitted account intervals and anything except tail padding."""
        indices = split._valid_indices_cpu
        if (not isinstance(indices, torch.Tensor) or indices.device.type != "cpu"
                or indices.ndim != 1 or indices.dtype != torch.int64 or not len(indices)):
            raise ValueError("futures account split requires chronological CPU decision indices")
        sample = getattr(split, "sample_mask", None)
        if sample is None:
            valid = len(indices)
        else:
            if not isinstance(sample, torch.Tensor) or sample.dtype != torch.bool or sample.shape != indices.shape:
                raise ValueError("futures account split sample mask differs from its decision axis")
            sample = sample.detach().to(device="cpu")
            valid = int(sample.sum().item())
            if (valid == 0 or not bool(torch.all(sample[:valid])) or bool(torch.any(sample[valid:]))
                    or (valid < len(indices) and not bool(torch.all(indices[valid:] == indices[valid - 1])))):
                raise ValueError("futures account split only permits a duplicated padded tail")
        real = indices[:valid]
        if (bool(torch.any(real < 0)) or bool(torch.any(real >= len(self.calendar.decision_dates)))
                or (valid > 1 and not bool(torch.all(real[1:] == real[:-1] + 1)))):
            raise ValueError("futures account split cannot skip or reorder decision sessions")
        advance = getattr(split, "session_advance_mask", None)
        if advance is not None:
            if (not isinstance(advance, torch.Tensor) or advance.dtype != torch.bool or advance.ndim != 1
                    or len(advance) != len(self.calendar.decision_dates)
                    or not bool(torch.all(advance.detach().to(device="cpu")[real]))):
                raise ValueError("futures account split cannot skip a real account session")

    def prepare_batch(self, split, start: int, stop: int) -> FuturesAccountCalendarBatch:
        if not 0 <= start < stop <= len(split._valid_indices_cpu):
            raise IndexError("futures account batch lies outside the split")
        indices = split._valid_indices_cpu[start:stop].detach().cpu().numpy()
        return slice_futures_account_calendar_batch(self.calendar, self.events, indices)

    def prepare_training_batch(self, split, start: int, stop: int) -> FuturesAccountCalendarBatch:
        """Keep a boundary settlement with the action that created inventory.

        This is a loss-only attribution: physical dates and the event tape do
        not move. A following real decision owns no duplicate event, while the
        last actual decision in a split cannot consume an event beyond its end.
        Formal evaluation must use ``prepare_batch`` and the full dated ledger.
        """
        original = self.prepare_batch(split, start, stop)
        indices = split._valid_indices_cpu
        sample = getattr(split, "sample_mask", None)
        first_decision, stop_decision = original.decision_start, original.decision_stop
        if start and (int(indices[start - 1]) != first_decision - 1
                      or (sample is not None and not bool(sample[start - 1]))):
            raise ValueError("training event ownership requires the preceding real decision")
        next_real = stop < len(indices) and (sample is None or bool(sample[stop]))
        if next_real and int(indices[stop]) != stop_decision:
            raise ValueError("training event ownership requires the next chronological decision")
        calendar = self.calendar
        first = (int(calendar.decision_indices[first_decision]) if start else
                 (0 if first_decision == 0 else int(calendar.decision_indices[first_decision - 1]) + 1))
        last = (int(calendar.decision_indices[stop_decision]) if next_real else
                int(calendar.decision_indices[stop_decision - 1]) + 1)
        dates = calendar.dates[first:last].copy()
        flags = calendar.account_only_mask[first:last].copy()
        sliced = _calendar_from_nodes(
            dates, calendar.decision_dates[first_decision:stop_decision].copy(), flags,
            training_trailing_event_credit=bool(flags[-1]),
        )
        event_start = int(np.searchsorted(calendar.account_only_indices, first, side="left"))
        event_stop = int(np.searchsorted(calendar.account_only_indices, last, side="left"))
        events = self.events[event_start:event_stop]
        if events.shape[0] != len(sliced.account_only_indices):
            raise RuntimeError("training event ownership does not match the original tape")
        return FuturesAccountCalendarBatch(
            sliced, events, first_decision, stop_decision, event_start, event_stop,
        )


def _pack_futures_decision_execution(daily) -> np.ndarray:
    """Own the exact executor channels, including the source liquidation mask."""
    decision = np.asarray(daily.integer_execution)
    liquidation = np.asarray(getattr(daily, "must_liquidate_mask", None))
    if (decision.dtype != np.float32 or decision.ndim != 3
            or decision.shape[-1] not in MARGIN_EXECUTION_WIDTHS
            or liquidation.dtype != np.bool_
            or liquidation.shape != decision.shape[:2]):
        raise ValueError("execution identity requires the actual decision tape and liquidation mask")
    if getattr(daily, "intraday_execution", None) is not None:
        raise ValueError("dated futures account does not support a separate intraday execution ABI")
    packed = np.array(decision, dtype=np.float32, order="C", copy=True)
    packed[..., 2] = liquidation
    return packed


def _copy_futures_decision_session_mask(daily, rows: int) -> np.ndarray | None:
    value = getattr(daily, "margin_session_mask", None)
    if value is None:
        return None
    mask = np.asarray(value)
    if mask.dtype != np.bool_ or mask.shape != (rows,):
        raise ValueError("dated futures margin session mask must be a boolean decision axis")
    owned = np.array(mask, dtype=bool, order="C", copy=True)
    owned.setflags(write=False)
    return owned


def _session_mask_digest(mask: np.ndarray | None) -> str | None:
    return None if mask is None else hashlib.sha256(mask.tobytes(order="C")).hexdigest()


def _futures_execution_fingerprint(daily, source: PreparedFuturesAccountSource, packed: np.ndarray,
                                   session_mask: np.ndarray | None) -> str:
    from stockagent.data.panel_cache import array_content_fingerprint

    clock = getattr(daily, "decision_clock_contract", None)
    if clock is not None and not isinstance(clock, dict):
        raise ValueError("execution clock metadata must be a mapping")
    # The local path and model-only opening reference are not execution data.
    # The actual decision tape already binds the values from specifications.
    clock = {key: value for key, value in (clock or {}).items()
             if key not in {"source_path", "opening_reference", "entry_specifications"}}
    payload = {
        "contract": "dated_futures_execution_content_v3",
        "event_source_fingerprint": source.fingerprint,
        "decision_dates": array_content_fingerprint(source.calendar.decision_dates),
        "packed_integer_execution": array_content_fingerprint(packed),
        "margin_session_mask": (None if session_mask is None else array_content_fingerprint(session_mask)),
        "margin_contract_version": getattr(daily, "margin_contract_version", None),
        "futures_data_contract_version": getattr(daily, "futures_data_contract_version", None),
        "futures_backtest_contract_version": getattr(daily, "futures_backtest_contract_version", None),
        "decision_clock": clock,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def futures_account_execution_fingerprint(daily, source: PreparedFuturesAccountSource) -> str:
    """Hash the same packed decision tape the executor actually consumes.

    This is content identity, not a substitute for source admission. Runtime
    preparation packs and hashes once; this independent helper is for readback.
    """
    if not isinstance(source, PreparedFuturesAccountSource):
        raise ValueError("execution identity requires a prepared dated account source")
    source.verify_identity()
    attached = PreparedFuturesAccountSource.from_daily(daily, source.calendar.decision_dates)
    if attached.fingerprint != source.fingerprint:
        raise ValueError("execution identity and attached account source differ")
    packed = _pack_futures_decision_execution(daily)
    return _futures_execution_fingerprint(
        daily, source, packed, _copy_futures_decision_session_mask(daily, packed.shape[0]))


def verify_futures_account_binding(split, source: PreparedFuturesAccountSource | None) -> None:
    """Require the split and executor to consume the same sparse event tape."""
    identity = getattr(split, "futures_account_source_fingerprint", None)
    if source is None:
        if identity is not None:
            raise ValueError("dated futures split cannot run without its event source")
        return
    if not isinstance(source, PreparedFuturesAccountSource) or identity != source.fingerprint:
        raise ValueError("dated futures split and executor event sources differ")
    release = getattr(split, "futures_execution_release_fingerprint", None)
    if source.decision_execution is not None:
        if release != source.execution_release_fingerprint:
            raise ValueError("dated futures split and executor decision releases differ")
        tensor = getattr(split, "overnight_log_returns", None)
        if (not isinstance(tensor, torch.Tensor)
                or getattr(split, "_futures_execution_tensor_version", None) != tensor._version):
            raise ValueError("dated futures split decision execution was modified")
    elif release is not None:
        raise ValueError("dated futures split requires its prepared decision execution")


def bind_futures_account_loss(
    loss_fn: Callable,
    *,
    source: PreparedFuturesAccountSource,
    split,
    start: int,
    stop: int,
    device: torch.device,
    credit_boundary_events: bool = False,
) -> Callable:
    """Bind real events to an unpadded, contiguous chronological loss call.

    The caller owns recurrent state and cannot omit any decision between
    batches.  Fixed-shape padding is deliberately rejected until the trainer
    binds a separately checked padded-calendar contract.
    """
    batch = (source.prepare_training_batch(split, start, stop) if credit_boundary_events
             else source.prepare_batch(split, start, stop))
    count = stop - start
    events = batch.account_execution.to(device=device, non_blocking=True)
    decision_execution = (
        None if source.decision_execution is None else
        source.decision_execution.narrow(0, batch.decision_start, count).to(
            device=device, non_blocking=True,
        )
    )

    def call(weights, returns, mask, **kwargs):
        if any(not isinstance(value, torch.Tensor) or value.shape[0] != count
               for value in (weights, returns, mask)):
            raise ValueError("futures account loss requires exact unpadded decision rows")
        if "futures_account_calendar" in kwargs or "account_only_execution" in kwargs:
            raise ValueError("futures account event source cannot be rebound")
        execution_mode = kwargs.get("execution_mode", getattr(split, "execution_mode", None))
        if execution_mode != "tw_stock_context_futures_portfolio":
            raise ValueError("futures account loss requires the exact futures execution mode")
        # The canonical trainer often stores this argument in a partial rather
        # than the call's kwargs. Bind the checked split mode explicitly so the
        # exact account cannot inherit a different partial's default.
        kwargs["execution_mode"] = execution_mode
        if kwargs.get("objective") != "log_utility":
            raise ValueError("futures account loss currently supports chronological log utility")
        if kwargs.get("symbol_sharded_ledger", False):
            raise ValueError("futures account loss requires the replicated whole-contract ledger")
        sample = kwargs.get("sample_mask")
        if sample is not None and (sample.shape != (count,) or sample.dtype != torch.bool
                                   or not bool(torch.all(sample))):
            raise ValueError("futures account loss cannot omit a real decision sample")
        advance = kwargs.get("state_advance_mask")
        if advance is not None and (advance.shape != (count,) or advance.dtype != torch.bool
                                    or not bool(torch.all(advance))):
            raise ValueError("futures account loss cannot skip a real decision date")
        if (batch.calendar.account_only_mask[0]
                and any((kwargs.get("aux_outputs") or {}).get(key) is None for key in
                        ("initial_weights", "initial_equity_scale", "initial_alive"))):
            raise ValueError("account event at batch start requires explicit incoming account state")
        if decision_execution is not None:
            # The recorded release must identify the data actually used by
            # the loss, including after DDP gather or window/cache transforms.
            # A caller-supplied buffer cannot establish that identity.
            kwargs["overnight_log_returns"] = decision_execution
        return loss_fn(weights, returns, mask, futures_account_calendar=batch.calendar,
                       account_only_execution=events, **kwargs)

    return call


_PADDED_ROW_ARGUMENTS = frozenset({
    "benchmark_returns", "overnight_log_returns", "can_buy_mask", "can_sell_mask",
    "can_short_open_mask", "can_short_open_open_mask", "force_short_cover_mask",
    "force_exit_mask", "sample_mask", "state_advance_mask", "session_month_ids",
    "commission_rebate_payment_eligible_mask", "day_trade_eligible_mask",
    "day_trade_can_buy_open_mask", "day_trade_can_sell_open_mask",
    "unresolved_corporate_action_mask", "cash_dividend_yield",
    "cash_dividend_payment_delay_sessions", "volume_limit_weights", "short_capacity_weights",
    "log_return_weights", "day_trade_replay_log_returns",
})


def bind_padded_futures_account_loss(
    loss_fn: Callable, *, source: PreparedFuturesAccountSource, split,
    start: int, stop: int, device: torch.device, credit_boundary_events: bool = False,
) -> Callable:
    """Drop only a verified duplicate-padded suffix after global DDP gather.

    The model and collective retain their fixed global shape.  A synthetic
    repeated decision is removed before the dated account ledger, so it can
    neither execute an order nor consume a real account event.  This adapter
    alone does not enable DDP or change its checkpoint and artifact contracts.
    """
    indices = split._valid_indices_cpu
    if not 0 <= start < stop <= len(indices):
        raise IndexError("padded futures account batch lies outside the split")
    sample = getattr(split, "sample_mask", None)
    if sample is None:
        valid = stop - start
    else:
        if not isinstance(sample, torch.Tensor) or sample.dtype != torch.bool or sample.shape != indices.shape:
            raise ValueError("padded futures account sample mask must align with the split")
        local = sample[start:stop].detach().to(device="cpu")
        valid = int(local.sum().item())
        if (valid == 0 or not bool(torch.all(local[:valid]))
                or bool(torch.any(local[valid:]))):
            raise ValueError("padded futures account permits only a nonempty real prefix")
        if valid < len(local) and not bool(torch.all(indices[start + valid:stop] == indices[start + valid - 1])):
            raise ValueError("padded futures account suffix must duplicate the last real decision")
    real_stop = start + valid
    exact_loss = bind_futures_account_loss(loss_fn, source=source, split=split,
                                           start=start, stop=real_stop, device=device,
                                           credit_boundary_events=credit_boundary_events)
    total = stop - start
    if valid == total:
        return exact_loss

    def call(weights, returns, mask, **kwargs):
        if any(not isinstance(value, torch.Tensor) or value.shape[0] != total
               for value in (weights, returns, mask)):
            raise ValueError("padded futures account loss requires the complete gathered batch")
        supplied_sample = kwargs.get("sample_mask")
        if (not isinstance(supplied_sample, torch.Tensor) or supplied_sample.dtype != torch.bool
                or supplied_sample.shape != (total,) or not bool(torch.all(supplied_sample[:valid]))
                or bool(torch.any(supplied_sample[valid:]))):
            raise ValueError("padded futures account sample mask differs from the verified real prefix")
        forwarded = dict(kwargs)
        for name in _PADDED_ROW_ARGUMENTS:
            value = forwarded.get(name)
            if value is not None:
                if not isinstance(value, torch.Tensor) or value.ndim == 0 or value.shape[0] != total:
                    raise ValueError(f"padded futures account row argument {name} has an invalid axis")
                forwarded[name] = value[:valid]
        rate = forwarded.get("short_margin_rate")
        if isinstance(rate, torch.Tensor) and rate.ndim >= 2:
            if rate.shape[0] != total:
                raise ValueError("padded futures account row argument short_margin_rate has an invalid axis")
            forwarded["short_margin_rate"] = rate[:valid]
        output = exact_loss(weights[:valid], returns[:valid], mask[:valid], **forwarded)
        aux = kwargs.get("aux_outputs")
        if aux is not None and aux.get("_capture_daily_log_returns", False):
            captured = aux.get("_daily_log_returns")
            if not isinstance(captured, torch.Tensor) or captured.shape != (valid,):
                raise ValueError("padded futures account return capture has an invalid axis")
            aux["_daily_log_returns"] = torch.cat((captured, captured.new_zeros(total - valid)))
        return output

    return call

