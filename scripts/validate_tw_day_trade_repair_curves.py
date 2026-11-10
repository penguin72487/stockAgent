#!/usr/bin/env python3
"""Validate a composed paper curve without applying or inventing prices.

The calendar, source pins, candidate accounting and any exceptional delayed
auction evidence are separate required admissions. This module only validates
the resulting minute grid, provenance fields and the already-authorized event.
"""
from __future__ import annotations

from collections import Counter
from datetime import date
import math
from typing import Mapping, Sequence

from scripts.rebuild_tw_day_trade_minute_curves import (
    historical_minute_mark_has_source,
    validate_existing_strategy_marks,
)


def equivalent_legacy_outcome(current: Mapping, accepted: Mapping) -> bool:
    """Treat only absent optional nontrade fields as equivalent to null.

    Earlier accepted position receipts predate that optional field. A real
    settlement, any changed economic value or any other missing key is NOT
    downgraded to schema compatibility.
    """
    def exact_value(a, b):
        if isinstance(a, bool) or isinstance(b, bool):
            return type(a) is type(b) and a == b
        if isinstance(a, float) and not math.isfinite(a):
            return False
        if isinstance(b, float) and not math.isfinite(b):
            return False
        if isinstance(a, Mapping) and isinstance(b, Mapping):
            return a.keys() == b.keys() and all(exact_value(a[key], b[key]) for key in a)
        if isinstance(a, list) and isinstance(b, list):
            return len(a) == len(b) and all(exact_value(x, y) for x, y in zip(a, b))
        return a == b

    def matches_with_optional_nulls(left, right, names):
        a, b = dict(left), dict(right)
        for name in names:
            if name not in a and b.get(name, object()) is None:
                b.pop(name)
            if name not in b and a.get(name, object()) is None:
                a.pop(name)
        return exact_value(a, b)

    current_rest = {key: value for key, value in current.items() if key != "positions"}
    accepted_rest = {key: value for key, value in accepted.items() if key != "positions"}
    if not matches_with_optional_nulls(current_rest, accepted_rest, {
            "cumulative_nontrade_settlement_net_pnl_twd",
            "nontrade_settlement_count", "nontrade_settlement_shares"}):
        return False
    now_positions, old_positions = current.get("positions"), accepted.get("positions")
    if not isinstance(now_positions, Mapping) or not isinstance(old_positions, Mapping):
        return False
    if now_positions.keys() != old_positions.keys():
        return False
    for identity, now in now_positions.items():
        old = old_positions[identity]
        if not isinstance(now, Mapping) or not isinstance(old, Mapping):
            return False
        if not matches_with_optional_nulls(now, old, {"nontrade_settlement"}):
            return False
    return True


def validate_previously_accepted_mode(mode: Mapping, manifest: Mapping,
                                      acceptance: Mapping, *, day: str) -> dict:
    """Recheck a previously accepted mode without weakening accounting."""
    from scripts import repair_tw_day_trade_flat_opening as repair

    market = manifest.get("market")
    sessions = acceptance.get("sessions") or []
    if (not isinstance(market, str) or not market or manifest.get("status") != "applied"
            or manifest.get("simulation_only") is not True
            or manifest.get("production_order_possible") is not False
            or acceptance.get("status") not in {"accepted", "verified_with_data_gaps"}
            or acceptance.get("market") != market
            or acceptance.get("simulation_only") is not True
            or acceptance.get("production_order_possible") is not False
            or acceptance.get("original_journal_prefixes_sha256_verified") is not True
            or acceptance.get("candidate_live_economics_equal") is not True
            or len(sessions) != 1 or sessions[0].get("session_date") != day
            or sessions[0].get("minute_points") != 270
            or mode.get("session_date") != day):
        raise RuntimeError("previous acceptance does not match this paper account and session")
    if not equivalent_legacy_outcome(repair._outcome(dict(mode)), manifest["candidate_final_outcome"]):
        raise RuntimeError(f"previously accepted economic state changed: {market}")
    return {"market": market, "session_date": day,
            "prior_acceptance": acceptance["status"],
            "old_optional_nulls_only": True, "economic_state_matches": True,
            "original_journal_prefix_recheck_required": True,
            "source_pin_recheck_required": True, "simulation_only": True,
            "production_order_possible": False}


def validate_composed_curves(rows: Sequence[Mapping], *, sessions: Sequence[str],
                             markets: set[str], audited_delayed_events: set[tuple[str, str]]) -> dict:
    """Check 270 regular points plus separately audited 13:33 events.

    ``audited_delayed_events`` must come from independent source-pinned replay
    audits; this function deliberately does not infer it from the marks. A
    delayed event is never counted as a regular minute or a brokerage fill.
    """
    if (not sessions or list(sessions) != sorted(set(sessions)) or not markets
            or any(date.fromisoformat(day).isoformat() != day for day in sessions)):
        raise ValueError("exact ordered official session and market scope required")
    scope = set(sessions)
    selected, delayed, counts = [], [], Counter()
    for row in rows:
        day, market = str(row.get("session_date") or ""), str(row.get("market") or "")
        if day not in scope or market not in markets:
            continue
        minute = str(row.get("minute") or "")
        if minute == f"{day}T13:33+08:00":
            key = (day, market)
            try:
                late_equity = float(row["total_equity_twd"])
            except (KeyError, TypeError, ValueError) as error:
                raise RuntimeError("invalid delayed event equity") from error
            if (key not in audited_delayed_events
                    or row.get("valuation_source") != "source_pinned_delayed_auction_event_v1"
                    or row.get("historical_minute_replay") is not True
                    or row.get("valuation_executable") is not False
                    or row.get("broker_fill") is True or row.get("exchange_fill") is True
                    or row.get("simulation_only") is False or not math.isfinite(late_equity)
                    or int(row.get("open_position_count") or 0) != 0
                    or int(row.get("missing_price_position_count") or 0) != 0
                    or row.get("valuation_stale") is not False):
                raise RuntimeError(f"unverified delayed paper event: {day}/{market}")
            delayed.append(key)
            continue
        counts[(day, market, minute)] += 1
        if counts[(day, market, minute)] != 1:
            raise RuntimeError(f"duplicate regular minute: {day}/{market}/{minute}")
        try:
            equity = float(row["total_equity_twd"])
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError("missing or invalid curve equity") from error
        if not math.isfinite(equity):
            raise RuntimeError("nonfinite curve equity")
        if minute[11:16] not in {"09:01", "13:30"} and not historical_minute_mark_has_source(row):
            raise RuntimeError(f"interior minute lacks auditable source: {day}/{market}/{minute}")
        selected.append(dict(row))
    if Counter(delayed) != Counter({key: 1 for key in audited_delayed_events}):
        raise RuntimeError("audited delayed events do not match the composed curve")
    _, stats = validate_existing_strategy_marks(
        selected, start=date.fromisoformat(sessions[0]), end=date.fromisoformat(sessions[-1]))
    if stats["session_dates"] != list(sessions) or set(stats["markets"]) != markets:
        raise RuntimeError("curve sessions or markets differ from the official scope")
    expected = len(sessions) * len(markets) * 270
    if len(selected) != expected:
        raise RuntimeError("composed curve does not contain 270 regular points per mode/session")
    return {"status": "validated_component_only", "completed_sessions": len(sessions),
            "markets": sorted(markets), "regular_minute_points": expected,
            "separately_audited_delayed_events": [
                {"session_date": day, "market": market, "minute": f"{day}T13:33+08:00"}
                for day, market in sorted(delayed)],
            "duplicate_regular_points": 0, "linear_interpolation_used": False,
            "simulation_only": True, "production_order_possible": False,
            "source_and_accounting_validation_required": True}
