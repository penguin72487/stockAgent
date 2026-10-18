"""Observe durable paper execution, without touching prices/orders/accounting.

Economic replay timestamps and signal registration are not completion clocks.
One record per changed quantity/source state follows the canonical state fsync;
unchanged minute marks do not generate another fsync or full-ledger scan.
"""
from __future__ import annotations

from datetime import datetime
import json
from typing import Any, Mapping


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
    return {**quantities, "quantity_contract_valid": valid,
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
            mode.get("entry_filled_shares") if signal_registration_committed(mode) else 0)
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
                    quantities["entry_filled_shares"], int) and quantities["entry_filled_shares"] > 0),
                "first_fill_clock_unknown_after_restore": identity in self._restored_filled,
            })
        return rows

    def acknowledge(self, modes):
        # Only acknowledge after the progress rows themselves have been fsynced.
        # A failed observer write can be retried; it cannot lose an execution.
        for market, mode in modes.items():
            self._seen[market] = self._signature(mode)
            self._prior_filled[self._identity(market, mode)] = (
                mode.get("entry_filled_shares") if signal_registration_committed(mode) else 0)


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
