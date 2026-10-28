from copy import deepcopy
from datetime import date

import pytest

from scripts.audit_tw_day_trade_terminal_close_sources import compare, TERMINAL_CLOSE_UNLIMITED_CONTRACT


DAY = date(2026, 10, 28)
DIGEST = "a" * 64


def fill():
    return {
        "session_date": str(DAY), "symbol": "2330", "purpose": "13_30_unlimited_close_paper_settlement",
        "simulation_only": True, "fill_contract": TERMINAL_CLOSE_UNLIMITED_CONTRACT,
        "quantity": 1000, "requested_quantity": 1000, "remaining_quantity": 0,
        "price": 100.0, "fill_at": "2026-10-28T13:30:00+08:00",
        "terminal_close_receipt": {
            "contract": TERMINAL_CLOSE_UNLIMITED_CONTRACT, "broker_fill": False,
            "full_quantity_is_user_assumption": True, "effective_at": "2026-10-28T13:30:00+08:00",
            "recorded_at": "2026-10-28T13:30:03+08:00", "source_evidence": {
                "session_date": str(DAY), "price": 100.0, "price_basis": "official_session_close",
                "source": "twse_official_daily_close", "source_sha256": DIGEST,
            },
        },
    }


def official(price=100.0):
    return {"2330": {"price": price, "source": "twse_official_daily_close", "source_sha256": DIGEST,
                     "official_row": {"成交股數": "1,000"}}}


def test_checks_retained_official_evidence_and_current_price():
    result = compare([fill()], DAY, official(), lambda exchange, digest: official())
    assert result["errors"] == []
    assert result["row_counts"]["official_retained_digest_verified"] == 1


@pytest.mark.parametrize("current,pinned", [
    (official(101.0), official()), (official(), official(101.0)),
    ({"2330": {"price": 100.0, "official_row": {"成交股數": "0"}}}, official()),
    (official(), {"2330": {"price": 100.0, "source": "twse_official_daily_close",
                           "source_sha256": DIGEST, "official_row": {"成交股數": "0"}}}),
    (official(), {}),
])
def test_conflicting_or_missing_official_data_fail_closed(current, pinned):
    assert compare([fill()], DAY, current, lambda exchange, digest: pinned)["errors"]


def test_auction_metadata_not_a_broker_fill_proof():
    row = fill()
    row["terminal_close_receipt"]["source_evidence"] = {
        "session_date": str(DAY), "price": 100.0, "price_basis": "observed_regular_closing_auction",
        "source": "exchange_non_trial_quote_trade", "exchange_quote_at": "2026-10-28T13:30:00+08:00",
        "received_at": "2026-10-28T13:30:02+08:00", "observed_trade_volume_lots": 1,
    }
    result = compare([row], DAY, official(), lambda *args: {})
    assert result["errors"] == []
    assert result["row_counts"]["auction_metadata_only"] == 1
    row["terminal_close_receipt"]["source_evidence"]["received_at"] = "2026-10-28T13:30:04+08:00"
    assert compare([row], DAY, official(), lambda *args: {})["errors"]


def test_claimed_broker_fill_and_non_full_quantity_fail():
    row = fill()
    row["terminal_close_receipt"]["broker_fill"] = True
    assert compare([row], DAY, official(), lambda *args: official())["errors"]
    other = deepcopy(fill())
    other["remaining_quantity"] = 1000
    assert compare([other], DAY, official(), lambda *args: official())["errors"]
