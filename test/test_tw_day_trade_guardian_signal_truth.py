from datetime import time

import pytest

from scripts import check_tw_day_trade_unattended_health as guardian


DAY = "2026-10-28"


def _modes(day=DAY):
    return {market: {"session_date": day, "signal_id": market,
                     "entry_completed_at": f"{day}T09:00:02+08:00",
                     "entry_fill_policy": "causal_market_full_target_at_best_quote",
                     "entry_requested_shares": 1000, "entry_filled_shares": 0}
            for market in guardian.EXPECTED_MARKETS}


def _receipt(*, passed=True):
    return {"contract": "daily_opening_original_clock_deadline_v1",
            "session_date": DAY, "observed_at": f"{DAY}T09:00:11+08:00",
            "target_ms": 1000, "scope_matches_preopen": True,
            "evidence_errors": [], "expected_markets": list(guardian.EXPECTED_MARKETS),
            "simulation_only": True, "broker_fill_claim": False,
            "passed": passed, "state": "passed" if passed else "missed_or_unverified"}


def _observe(wall, *, modes=None, receipt=None, session_state="open"):
    return guardian._signal_observation(
        session_state=session_state, session_date=DAY, wall=wall,
        modes=modes if modes is not None else _modes(),
        deadline_receipt=receipt if receipt is not None else {},
    )


def test_preopen_previous_session_is_not_a_current_commit_or_sla():
    row = _observe(time(8, 45), modes=_modes("2026-10-27"))
    assert row["state"] == "waiting_for_open"
    assert row["ready"] is None
    assert row["committed"] is False
    assert row["live_sla_met"] is None
    assert set(row["missing_markets"]) == set(guardian.EXPECTED_MARKETS)


def test_registration_does_not_prove_fill_or_one_second_sla():
    row = _observe(time(9, 0, 16))
    assert row["ready"] is True
    assert row["committed"] is True
    assert row["live_sla_met"] is None
    assert row["live_sla_evidence"]["state"] == "unverified"
    assert "not_full_fill" in row["registration_measurement_boundary"]


@pytest.mark.parametrize("passed", [True, False])
def test_one_second_sla_uses_only_the_canonical_deadline_receipt(passed):
    row = _observe(time(9, 0, 16), receipt=_receipt(passed=passed))
    assert row["live_sla_met"] is passed
    assert row["live_sla_evidence"]["state"] == ("passed" if passed else "missed_or_unverified")
    assert "not_browser_paint" in row["live_sla_evidence"]["measurement_boundary"]


@pytest.mark.parametrize("changes", [
    {"session_date": "2026-10-27"},
    {"target_ms": 11000},
    {"scope_matches_preopen": False},
    {"evidence_errors": ["bad_clock"]},
    {"evidence_errors": None},
    {"expected_markets": [["malformed"]]},
    {"expected_markets": []},
    {"simulation_only": False},
    {"broker_fill_claim": True},
    {"state": "missed_or_unverified"},
    {"observed_at": "2026-10-28T09:00:20+08:00"},
])
def test_bad_or_future_deadline_receipt_cannot_prove_sla(changes):
    row = _observe(time(9, 0, 16), receipt={**_receipt(), **changes})
    assert row["live_sla_met"] is None
    assert row["live_sla_evidence"]["state"] == "unverified"


@pytest.mark.parametrize("session_state, expected", [("closed", "not_applicable"),
                                                        ("unknown", "calendar_unverified")])
def test_non_session_is_not_a_current_commit(session_state, expected):
    row = _observe(time(10, 0), receipt=_receipt(), session_state=session_state)
    assert row["state"] == expected
    assert row["committed"] is None
    assert row["live_sla_met"] is None
