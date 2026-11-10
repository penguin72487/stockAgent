from datetime import datetime, timedelta

import pytest

from scripts.validate_tw_day_trade_repair_curves import (
    equivalent_legacy_outcome, validate_composed_curves, validate_previously_accepted_mode,
)
from scripts.rebuild_tw_day_trade_minute_curves import MINUTE_CONTRACT
from scripts.repair_tw_day_trade_flat_opening import _outcome


def regular_rows():
    day = "2026-11-04"
    start = datetime.fromisoformat(day + "T09:01:00+08:00")
    return [{"market": "m", "session_date": day,
             "minute": (start + timedelta(minutes=i)).isoformat(timespec="minutes"),
             "total_equity_twd": 100., "historical_minute_replay": True,
             "minute_valuation_contract": MINUTE_CONTRACT, "valuation_source": "observed",
             "fresh_trade_notional_coverage_ratio": 1., "fresh_trade_position_count": 0,
             "last_trade_carried_position_count": 0, "missing_price_position_count": 0,
             "valuation_executable": False} for i in range(270)]


def late_row():
    return {"market": "m", "session_date": "2026-11-04", "minute": "2026-11-04T13:33+08:00",
            "total_equity_twd": 100., "historical_minute_replay": True,
            "valuation_source": "source_pinned_delayed_auction_event_v1",
            "valuation_executable": False, "open_position_count": 0,
            "missing_price_position_count": 0, "valuation_stale": False}


def test_old_outcome_may_omit_only_null_nontrade_settlement():
    old = {"total_equity_twd": 100., "positions": {"p": {"filled_shares": 1000}}}
    current = {"total_equity_twd": 100., "positions": {"p": {"filled_shares": 1000,
                                                                   "nontrade_settlement": None}},
               "cumulative_nontrade_settlement_net_pnl_twd": None,
               "nontrade_settlement_count": None, "nontrade_settlement_shares": None}
    assert equivalent_legacy_outcome(current, old)
    assert equivalent_legacy_outcome(old, current)
    current["positions"]["p"]["nontrade_settlement"] = {"quantity": 1000}
    assert not equivalent_legacy_outcome(current, old)
    current["positions"]["p"]["nontrade_settlement"] = None
    current["positions"]["p"]["filled_shares"] = 0
    assert not equivalent_legacy_outcome(current, old)
    current["positions"]["p"]["filled_shares"] = 1000
    current["positions"]["p"]["other"] = None
    assert not equivalent_legacy_outcome(current, old)
    current["positions"]["p"].pop("other")
    current["nontrade_settlement_count"] = 1
    assert not equivalent_legacy_outcome(current, old)
    current["nontrade_settlement_count"] = None
    current["positions"]["p"]["filled_shares"] = True
    old["positions"]["p"]["filled_shares"] = 1
    assert not equivalent_legacy_outcome(current, old)


def test_previous_mode_compatibility_preserves_acceptance_and_other_gates():
    mode = {"market": "m", "session_date": "2026-11-04", "positions": {}}
    accepted = _outcome(mode)
    for key in ("nontrade_settlement_count", "nontrade_settlement_shares",
                "cumulative_nontrade_settlement_net_pnl_twd"):
        accepted.pop(key)
    manifest = {"market": "m", "status": "applied", "simulation_only": True,
                "production_order_possible": False, "candidate_final_outcome": accepted}
    acceptance = {"market": "m", "status": "verified_with_data_gaps", "simulation_only": True,
                  "production_order_possible": False, "original_journal_prefixes_sha256_verified": True,
                  "candidate_live_economics_equal": True,
                  "sessions": [{"session_date": "2026-11-04", "minute_points": 270}]}
    proof = validate_previously_accepted_mode(mode, manifest, acceptance, day="2026-11-04")
    assert proof["prior_acceptance"] == "verified_with_data_gaps"
    assert proof["original_journal_prefix_recheck_required"] is True
    mode["nontrade_settlement_count"] = 1
    with pytest.raises(RuntimeError, match="economic state changed"):
        validate_previously_accepted_mode(mode, manifest, acceptance, day="2026-11-04")
    mode["nontrade_settlement_count"] = None
    acceptance["original_journal_prefixes_sha256_verified"] = False
    with pytest.raises(RuntimeError, match="previous acceptance"):
        validate_previously_accepted_mode(mode, manifest, acceptance, day="2026-11-04")


def validate(rows, allowed=frozenset()):
    return validate_composed_curves(rows, sessions=["2026-11-04"], markets={"m"},
                                    audited_delayed_events=set(allowed))


def test_curve_component_requires_separate_audit_for_delayed_event():
    rows = [*regular_rows(), late_row()]
    with pytest.raises(RuntimeError, match="unverified delayed"):
        validate(rows)
    result = validate(rows, {("2026-11-04", "m")})
    assert result["regular_minute_points"] == 270
    assert len(result["separately_audited_delayed_events"]) == 1
    assert result["source_and_accounting_validation_required"] is True


@pytest.mark.parametrize("corruption", ["duplicate", "missing", "unsourced", "nonfinite", "late_missing", "late_stale", "late_broker_fill"])
def test_curve_component_rejects_bad_grid_and_fake_events(corruption):
    rows, allowed = regular_rows(), set()
    if corruption == "duplicate":
        rows.append(dict(rows[0]))
    elif corruption == "missing":
        rows.pop()
    elif corruption == "unsourced":
        rows[1]["valuation_source"] = None
    elif corruption == "nonfinite":
        rows[1]["total_equity_twd"] = float("nan")
    elif corruption == "late_missing":
        allowed.add(("2026-11-04", "m"))
    else:
        event = late_row()
        if corruption == "late_stale":
            event["valuation_stale"] = True
        else:
            event["broker_fill"] = True
        rows.append(event)
        allowed.add(("2026-11-04", "m"))
    with pytest.raises(RuntimeError):
        validate(rows, allowed)
