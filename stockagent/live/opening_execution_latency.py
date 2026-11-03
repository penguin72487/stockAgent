"""Observe durable paper execution, without touching prices/orders/accounting.

Economic replay timestamps and signal registration are not completion clocks.
One record per changed quantity/source state follows the canonical state fsync;
unchanged minute marks do not generate another fsync or full-ledger scan.
"""
from __future__ import annotations

from datetime import datetime
import json
import math
import threading
import time
from typing import Any, Mapping

from stockagent.live.market_config import SIGNAL_UPDATE_PROGRESSIVE


_phase_thread_identity = threading.current_thread


def nonnegative_duration_ms(value: Any) -> float | None:
    """A measured zero is valid; an absent, invalid or negative span is unknown."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number >= 0.0 else None


def residual_duration_ms(total: Any, *parts: Any) -> float | None:
    """Subtract only complete, non-overlapping measured spans.

    Callers own the clock/overlap contract. Values are published with
    millisecond precision, so allow tiny negative rounding error, not a
    materially inconsistent set of clocks.
    """
    whole = nonnegative_duration_ms(total)
    measured = [nonnegative_duration_ms(part) for part in parts]
    if whole is None or not measured or any(part is None for part in measured):
        return None
    remainder = whole - sum(measured)
    if remainder < -0.01:
        return None
    return round(max(0.0, remainder), 3)


class OpeningPhaseTimer:
    """Wall spans plus CPU only where both observations share one thread.

    Shared input builders may notify a waiter's callback on another thread.
    Such CPU spans are unknown, not negative durations or evidence of provider
    wait. All elapsed spans use the one process clock. No timers write files,
    renew progress watchdogs, or create synthetic completion callbacks.
    """

    def __init__(self, stages, cpu_stages=None):
        self.stages, self.cpu_stages = stages, cpu_stages
        self._lock = threading.Lock()
        self._clock = time.perf_counter()
        self._cpu_clock = time.thread_time() if cpu_stages is not None else None
        self._thread = _phase_thread_identity()
        self._phase = None
        self._ambiguous_cpu = False
        self._closed = False

    def transition(self, phase):
        with self._lock:
            if self._closed:
                return
            thread = _phase_thread_identity()
            if phase == self._phase:
                self._ambiguous_cpu |= thread is not self._thread
                return
            observed = time.perf_counter()
            cpu = time.thread_time() if self.cpu_stages is not None else None
            self._close_span(observed, cpu, thread)
            self._phase, self._clock, self._cpu_clock = phase, observed, cpu
            self._thread, self._ambiguous_cpu = thread, False

    def _close_span(self, observed, cpu, thread):
        if self._phase is None:
            return
        phase = self._phase
        self.stages[phase] = self.stages.get(phase, 0.) + (observed - self._clock) * 1000
        if self.cpu_stages is not None:
            previous = self.cpu_stages.get(phase, 0.)
            duration = ((cpu - self._cpu_clock) * 1000
                        if not self._ambiguous_cpu and thread is self._thread else None)
            self.cpu_stages[phase] = (
                previous + duration if previous is not None and duration is not None
                and duration >= 0 else None)

    def finish(self):
        with self._lock:
            if self._closed:
                return
            self._close_span(time.perf_counter(),
                time.thread_time() if self.cpu_stages is not None else None, _phase_thread_identity())
            self._closed = True


def call_with_opening_stage_timing(function, kwargs, stages, cpu_stages=None):
    """Observe an unchanged synchronous call, retaining its callback and errors."""
    timer = OpeningPhaseTimer(stages, cpu_stages)
    original = kwargs.get("progress_callback")

    def progress(event):
        timer.transition(str(event.get("message") or ""))
        if original is not None:
            original(event)

    try:
        return function(**dict(kwargs, progress_callback=progress))
    finally:
        timer.finish()


def inventory_rebalance_quantities(mode):
    """Derived inventory completion, never an order or a synthetic fill."""
    targets, positions = mode.get("signal_inventory_targets"), mode.get("positions")
    valid = isinstance(targets, dict) and isinstance(positions, dict)
    targets = targets if isinstance(targets, dict) else {}
    positions = positions if isinstance(positions, dict) else {}
    held, unknown, remaining = {}, 0, 0
    for position in positions.values():
        if not isinstance(position, Mapping):
            valid = False
            continue
        shares = position.get("signed_shares")
        if type(shares) is not int:
            valid = False
            continue
        symbol = position.get("symbol")
        if not isinstance(symbol, str) or not symbol or held.get(symbol, 0) * shares < 0:
            valid = False
        held[symbol] = held.get(symbol, 0) + shares
    for symbol, target in targets.items():
        if target is None:
            unknown += 1
        elif type(target) is not int:
            valid = False
        else:
            remaining += abs(target - held.get(symbol, 0))
    if any(shares and symbol not in targets for symbol, shares in held.items()):
        valid = False
    return {"contract": "signed_target_minus_actual_inventory_v1", "contract_valid": valid,
            "unresolved_symbol_count": unknown, "remaining_delta_shares": remaining,
            "target_complete": valid and not unknown and not remaining}


def _executed_progress(mode):
    filled = mode.get("entry_filled_shares")
    if (type(filled) is int and mode.get("day_trade_signal_update_policy") == SIGNAL_UPDATE_PROGRESSIVE
            and type(mode.get("rebalance_reduction_fill_count")) is int
            and mode["rebalance_reduction_fill_count"] > 0):
        return filled + 1  # Presence token only; not reported as share quantity.
    return filled


def unsized_target_count(mode):
    if isinstance(mode.get("pending_entry_orders"), dict):
        # Cancellation at the exit window is not successful sizing/execution.
        return sum(order.get("sizing_pending") is True for order in mode["pending_entry_orders"].values())
    return mode.get("unsized_entry_target_count")


def signal_registration_committed(mode):
    if "processed_signal_ids" in mode:
        return bool(mode.get("signal_id") in (mode.get("processed_signal_ids") or ())
                    and str(mode.get("entry_completed_at") or "")[:10] == mode.get("session_date")
                    and not mode.get("pending_signal_id"))
    return mode.get("signal_registration_committed") is True


def execution_quantities(mode: Mapping[str, Any]) -> dict[str, Any]:
    fields = ("entry_requested_shares", "entry_filled_shares", "entry_unfilled_shares",
              "pending_entry_shares", "pending_entry_source_count")
    quantities = {name: mode.get(name) for name in fields}
    valid = all(isinstance(value, int) and not isinstance(value, bool) and value >= 0
                for value in quantities.values())
    reasons = dict(mode.get("signal_reason_counts") or {})
    unsized = unsized_target_count(mode)
    unsized_valid = isinstance(unsized, int) and not isinstance(unsized, bool) and unsized >= 0
    registered = signal_registration_committed(mode)
    source_blockers = {name: value for name, value in reasons.items()
                       if any(token in name for token in ("unavailable", "missing", "invalid", "quote_not_after"))
                       and isinstance(value, int) and value > 0}
    requested, filled = quantities["entry_requested_shares"], quantities["entry_filled_shares"]
    if valid:
        valid = filled <= requested and quantities["entry_unfilled_shares"] == requested - filled
    no_order = (valid and requested == 0 and mode.get("entry_fill_outcome") == "no_order"
                and not source_blockers and not quantities["pending_entry_source_count"]
                and not quantities["pending_entry_shares"])
    complete = (registered and valid and requested > 0 and filled == requested and unsized_valid and unsized == 0
                and not quantities["pending_entry_shares"]
                and not quantities["pending_entry_source_count"])
    no_order = no_order and registered and unsized_valid and unsized == 0
    revision_progress = {}
    if mode.get("day_trade_signal_update_policy") == SIGNAL_UPDATE_PROGRESSIVE:
        inventory = mode.get("inventory_rebalance")
        inventory = inventory if isinstance(inventory, Mapping) else {}
        reductions = mode.get("rebalance_reduction_fill_count")
        inventory_valid = (inventory.get("contract") == "signed_target_minus_actual_inventory_v1"
            and inventory.get("contract_valid") is True
            and type(inventory.get("remaining_delta_shares")) is int and inventory["remaining_delta_shares"] >= 0
            and type(inventory.get("unresolved_symbol_count")) is int and inventory["unresolved_symbol_count"] >= 0
            and type(reductions) is int and reductions >= 0)
        at_target = (inventory_valid and inventory.get("target_complete") is True
                     and inventory["remaining_delta_shares"] == 0 and inventory["unresolved_symbol_count"] == 0)
        valid = valid and inventory_valid
        no_order = no_order and at_target and reductions == 0
        complete = ((complete or (registered and valid and requested == 0 and reductions > 0
                                 and unsized_valid and unsized == 0)) and at_target)
        revision_progress = {"day_trade_signal_update_policy": SIGNAL_UPDATE_PROGRESSIVE,
            "inventory_rebalance": inventory, "rebalance_reduction_fill_count": reductions}
    return {**quantities, "quantity_contract_valid": valid,
            **revision_progress,
            "known_sized_target_complete": bool(valid and requested > 0 and filled == requested),
            "all_target_complete": bool(complete), "no_execution_required": bool(no_order),
            "source_blocking_reason_counts": source_blockers,
            "unsized_entry_target_count": unsized,
            "signal_registration_committed": registered,
            "source_reason_counts_are_registration_history": True,
            "entry_fill_outcome": mode.get("entry_fill_outcome"),
            "signal_reason_counts": reasons,
            "entry_fill_policy": mode.get("entry_fill_policy"),
            "simulation_replay": bool(mode.get("simulation_replay") or mode.get("counterfactual_0901_price_fill")
                                      or str(mode.get("entry_fill_policy") or "").startswith("official_open")),
            "live_execution_eligible": mode.get("entry_fill_policy") in {
                "causal_market_full_target_at_best_quote", "causal_best_quote"}
                and not mode.get("entry_fill_is_synthetic"),
            "ledger_state_divergence": bool(mode.get("ledger_state_divergence"))}


class OpeningExecutionObserver:
    """Process-local change detection; never backdate fills found after restart."""

    def __init__(self, modes: Mapping[str, Mapping[str, Any]]) -> None:
        self._seen = {market: self._signature(mode) for market, mode in modes.items()}
        self._prior_filled = {self._identity(market, mode): (
            _executed_progress(mode) if signal_registration_committed(mode) else 0)
                              for market, mode in modes.items()}
        self._restored_filled = {identity for identity, filled in self._prior_filled.items()
                                 if isinstance(filled, int) and not isinstance(filled, bool) and filled > 0}

    @staticmethod
    def _identity(market, mode):
        return market, mode.get("session_date"), mode.get("signal_id")

    @staticmethod
    def _signature(mode):
        return json.dumps({"session_date": mode.get("session_date"), "signal_id": mode.get("signal_id"),
                           **execution_quantities(mode)}, sort_keys=True, separators=(",", ":"))

    def samples(self, modes, *, committed_at: datetime, state_revision: int,
                engine_run_id: str, clock_basis: str):
        rows = []
        for market, mode in modes.items():
            signature = self._signature(mode)
            if self._seen.get(market) == signature or not mode.get("signal_id"):
                continue
            identity = self._identity(market, mode)
            prior_filled = self._prior_filled.get(identity, 0)
            quantities = execution_quantities(mode)
            rows.append({"schema_version": 1,
                "contract": "state_fsync_then_paper_target_progress_v1",
                "measurement_boundary": "paper_target_quantities_after_state_commit_not_broker_fill",
                "market": market, "session_date": mode.get("session_date"), "signal_id": mode.get("signal_id"),
                "recorded_at": committed_at.isoformat(timespec="microseconds"),
                "clock_basis": clock_basis, "state_revision": state_revision,
                "engine_run_id": engine_run_id, "simulation_only": True,
                "production_order_possible": False, **quantities,
                "first_fill_observed_this_commit": bool(quantities["signal_registration_committed"] and prior_filled == 0 and isinstance(
                    _executed_progress(mode), int) and _executed_progress(mode) > 0),
                "first_fill_clock_unknown_after_restore": identity in self._restored_filled,
            })
        return rows

    def acknowledge(self, modes):
        # Only acknowledge after the progress rows themselves have been fsynced.
        # A failed observer write can be retried; it cannot lose an execution.
        for market, mode in modes.items():
            self._seen[market] = self._signature(mode)
            self._prior_filled[self._identity(market, mode)] = (
                _executed_progress(mode) if signal_registration_committed(mode) else 0)


def opening_execution_slo(session_date, publication, current_modes, samples, *, observed_at):
    """All enabled modes, real durable commit clocks, pending targets retained.

    The caller supplies the immutable publication identity. A late registration,
    a 09:01 replay event or complete quantities discovered after restart cannot
    prove a live opening fill deadline. Cash-only modes are N/A, not fills.
    """
    gate = datetime.fromisoformat(session_date + "T09:00:00+08:00")
    if observed_at.tzinfo is None:
        raise ValueError("observer clock requires timezone")
    modes = {}
    for market, pointer in publication["modes"].items():
        current = current_modes.get(market, {})
        current_identity = (current.get("session_date") == session_date
                            and current.get("signal_id") == pointer.get("signal_id")
                            and bool(pointer.get("signal_id")))
        quantities = execution_quantities(current)
        first_fill, complete, no_order, invalid = [], [], [], 0
        for row in samples:
            if not isinstance(row, Mapping):
                invalid += 1
                continue
            if row.get("market") != market or row.get("session_date") != session_date:
                continue
            try:
                committed = datetime.fromisoformat(row["recorded_at"])
                published = datetime.fromisoformat(pointer["artifact_published_at"])
                if (row.get("contract") != "state_fsync_then_paper_target_progress_v1"
                        or row.get("signal_id") != pointer.get("signal_id")
                        or committed.tzinfo is None or published.tzinfo is None
                        or not gate <= published <= committed <= observed_at
                        or row.get("clock_basis") != "engine_publication_clock_not_economic_event"
                        or row.get("simulation_only") is not True
                        or row.get("production_order_possible") is not False
                        or not row.get("engine_run_id") or not isinstance(row.get("state_revision"), int)
                        or isinstance(row.get("state_revision"), bool) or row["state_revision"] <= 0):
                    raise ValueError("unverified execution identity or clock")
                row_quantities = execution_quantities(row)
                if not row_quantities["quantity_contract_valid"] or row_quantities["ledger_state_divergence"]:
                    raise ValueError("invalid execution quantities or divergent state")
                if row.get("first_fill_observed_this_commit") is True:
                    first_fill.append(committed)
                compatible = (current_identity and not quantities["ledger_state_divergence"]
                              and quantities["live_execution_eligible"] and row_quantities["live_execution_eligible"]
                              and not quantities["simulation_replay"] and not row_quantities["simulation_replay"]
                              and row_quantities["entry_requested_shares"] == quantities["entry_requested_shares"])
                if compatible and quantities["all_target_complete"] and row_quantities["all_target_complete"]:
                    complete.append(committed)
                if compatible and quantities["no_execution_required"] and row_quantities["no_execution_required"]:
                    no_order.append(committed)
            except (KeyError, TypeError, ValueError):
                invalid += 1
        completed_at = min(complete) if complete else None
        no_order_at = min(no_order) if no_order else None
        first_fill_at = min(first_fill) if first_fill else None
        modes[market] = {"signal_id": pointer.get("signal_id"), **quantities,
            "state": "identity_unavailable" if not current_identity else
                     "replay_not_live_execution" if quantities["simulation_replay"] else
                     "completed" if completed_at else "no_execution_required" if no_order_at else
                     "waiting_source_or_fill" if not quantities["all_target_complete"] and not quantities["no_execution_required"]
                     else "completion_clock_unavailable",
            "first_fill_committed_at": first_fill_at.isoformat() if first_fill_at else None,
            "all_targets_committed_at": completed_at.isoformat() if completed_at else None,
            "no_execution_required_committed_at": no_order_at.isoformat() if no_order_at else None,
            "all_target_completion_from_open_ms": round((completed_at - gate).total_seconds() * 1000, 3) if completed_at else None,
            "invalid_execution_records": invalid}
    finished = [row for row in modes.values() if row["state"] == "completed"]
    accounted = [row for row in modes.values() if row["state"] in {"completed", "no_execution_required"}]
    complete = len(accounted) == len(modes) and bool(finished)
    all_at = max(datetime.fromisoformat(row["all_targets_committed_at"]
                     or row["no_execution_required_committed_at"]) for row in accounted) if complete else None
    all_ms = round((all_at - gate).total_seconds() * 1000, 3) if all_at else None
    return {"contract": "all_enabled_mode_paper_full_target_completion_stages_v1",
            "measurement_boundary": "09:00_gate_to_all_paper_targets_durably_filled_not_registration_or_broker_fill",
            "state": "observed" if complete else "no_execution_required" if len(accounted) == len(modes) else "incomplete",
            "expected_mode_count": len(modes), "completed_fill_mode_count": len(finished),
            "no_execution_required_mode_count": sum(row["state"] == "no_execution_required" for row in modes.values()),
            "all_modes_complete_from_open_ms": all_ms,
            "stages": [{"target_ms": target, "passed": bool(complete and all_ms <= target),
                        "state": "incomplete" if not complete else "passed" if all_ms <= target else "missed"}
                       for target in (30000, 20000, 11000, 10000, 5000, 1000)], "modes": modes}
