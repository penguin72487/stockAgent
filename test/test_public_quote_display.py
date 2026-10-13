from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path

import pytest

from stockagent.live import public_dashboards as public
from stockagent.live import public_quote_display as quotes


NOW = datetime.fromisoformat("2026-10-13T13:00:00+08:00")
GRANT = {
    "source_id": "licensed_feed", "grant_id": "fixture-grant",
    "public_label": "已授權行情", "public_display": True,
    "license_reference": "fixture-contract-not-a-real-license",
    "valid_from": "2000-01-01", "valid_until": "2999-01-01",
    "venues": ["TWSE", "TPEX"],
}


def record(field="last_mark_price", *, public_value=112.5):
    return {
        "symbol": "2330", "session_date": "2020-10-13", field: 112.0,
        "filled_shares": 1000, "signed_shares": 1000,
        "reconciled_total_net_pnl_twd": 4500.25,
        "public_price_observations": {field: {
            "value": public_value, "source_id": "licensed_feed",
            "grant_id": "fixture-grant", "symbol": "2330", "venue": "TWSE",
            "price_role": field, "observed_at": "2020-10-13T09:00:01+08:00",
        }},
    }


@pytest.mark.parametrize("field", [
    "price", "entry_price", "last_mark_price", "exit_price", "execution_price",
    "bid", "ask", "upper_limit", "lower_limit", "sizing_open_price",
    "call_entry_ask", "put_entry_bid", "open", "Open", "last_prices",
])
def test_unlicensed_prices_are_withheld_but_private_facts_are_preserved(field):
    source = record(field)
    before = deepcopy(source)
    payload = public._scrub_public_value(source)
    assert payload[field] is None
    assert payload["public_price_display"]["withheld_fields"] == [field]
    assert "public_price_observations" not in payload
    assert payload["filled_shares"] == 1000
    assert payload["reconciled_total_net_pnl_twd"] == 4500.25
    assert source == before


@pytest.mark.parametrize("sanitizer", [public.sanitize_tw_status, public.sanitize_tw_signals,
                                        public.sanitize_tw_positions, public.sanitize_tw_events])
def test_every_tw_public_price_exit_is_covered(sanitizer):
    field = ("price" if sanitizer is public.sanitize_tw_events
             else "execution_price" if sanitizer is public.sanitize_tw_signals
             else "entry_price")
    row = record(field)
    payload = {"simulation_only": True, "production_order_possible": False,
               "rows": [row], "positions": [row]}
    output = sanitizer(payload)
    rows = output["positions"] if sanitizer is public.sanitize_tw_status else output["rows"]
    assert rows[0][field] is None
    assert "public_price_observations" not in json.dumps(output)
    assert row[field] == 112.0


def test_nested_quote_arrays_taifex_and_credentials_cannot_bypass_projection():
    source = {"simulation_only": True, "production_order_possible": False,
              "active_cycle": {"call_entry_ask": 15.5, "put_entry_ask": 16.2},
              "market": {"quotes": [{"bid": 125.0, "ask": "126.0", "api_key": "secret"}]},
              "debug": {"prices": [125.0, 126.0]},
              "cumulative_pnl_twd": 1234.56, "return_pct": 0.15}
    before = deepcopy(source)
    output = public.sanitize_taifex_status(source)
    assert output["active_cycle"]["call_entry_ask"] is None
    assert output["market"]["quotes"][0]["ask"] is None
    assert output["debug"]["prices"] is None
    assert "secret" not in json.dumps(output)
    assert output["cumulative_pnl_twd"] == 1234.56
    assert output["return_pct"] == 0.15
    assert output["price_display_policy"]["contract"] == quotes.CONTRACT
    assert source == before


def test_untrusted_permission_flags_do_not_authorize_publication():
    row = record()
    row["public_display_authorized"] = True
    row["public_price_observations"]["last_mark_price"]["public_display"] = True
    assert quotes.PublicQuoteDisplayPolicy().observation(row, "last_mark_price", 112.0, now=NOW) is None


def test_verified_public_quote_uses_its_own_value_and_clock_not_the_private_quote(monkeypatch):
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: quotes.PublicQuoteDisplayPolicy((GRANT,)))
    row = record()
    payload = {"simulation_only": True, "production_order_possible": False,
               "rows": [row]}
    for serializer in [public.sanitize_tw_positions, public.sanitize_tw_signals]:
        source = payload if serializer is public.sanitize_tw_positions else {
            **payload, "rows": [{"symbol": "2330", "position": row}]}
        output = serializer(source)
        disclosed = output["rows"][0] if serializer is public.sanitize_tw_positions else output["rows"][0]["position"]
        assert disclosed["last_mark_price"] == 112.5
        assert disclosed["public_price_display"]["visible_fields"]["last_mark_price"] == {
            "source": "已授權行情", "observed_at": "2020-10-13T09:00:01+08:00",
            "price_role": "last_mark_price",
        }
        assert disclosed["reconciled_total_net_pnl_twd"] == 4500.25
    assert row["last_mark_price"] == 112.0


@pytest.mark.parametrize("change", [
    {"source_id": "finmind"}, {"source_id": ""}, {"grant_id": ""},
    {"grant_id": "wrong"}, {"symbol": "0050"}, {"venue": "TAIFEX"},
    {"venue": "unknown"}, {"price_role": "entry_price"},
    {"observed_at": "2020-10-14T09:00:01+08:00"},
    {"observed_at": "2020-10-13T09:00:01"},
    {"value": float("nan")}, {"value": float("inf")}, {"value": 0}, {"value": True},
])
def test_wrong_role_symbol_session_provider_or_bad_price_remains_withheld(change):
    row = record()
    row["public_price_observations"]["last_mark_price"].update(change)
    assert quotes.PublicQuoteDisplayPolicy((GRANT,)).observation(row, "last_mark_price", 112.0, now=NOW) is None


@pytest.mark.parametrize("change", [
    {"valid_until": "2026-10-12"}, {"valid_from": "2026-10-14"},
    {"valid_until": "invalid"}, {"public_display": False},
    {"license_reference": ""}, {"public_label": ""}, {"venues": ["TAIFEX"]},
])
def test_expired_or_incomplete_grants_fail_closed(change):
    row = record()
    assert quotes.PublicQuoteDisplayPolicy(({**GRANT, **change},)).observation(row, "last_mark_price", 112.0, now=NOW) is None


def test_authorized_source_cannot_change_the_recorded_fill():
    row = record("entry_price")
    policy = quotes.PublicQuoteDisplayPolicy((GRANT,))
    assert policy.observation(row, "entry_price", 112.0, now=NOW) is None
    row["public_price_observations"]["entry_price"]["value"] = 112.0
    assert policy.observation(row, "entry_price", 112.0, now=NOW)[0] == 112.0


def test_future_price_is_not_displayed_even_with_a_grant():
    row = record()
    row["session_date"] = "2026-10-13"
    row["public_price_observations"]["last_mark_price"]["observed_at"] = "2026-10-13T13:00:01+08:00"
    assert quotes.PublicQuoteDisplayPolicy((GRANT,)).observation(row, "last_mark_price", 112.0, now=NOW) is None


def test_malformed_policy_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(quotes, "_POLICY_PATH", tmp_path / "missing.json")
    quotes.public_quote_display_policy.cache_clear()
    assert quotes.public_quote_display_policy().grants == ()
    quotes.public_quote_display_policy.cache_clear()


@pytest.mark.parametrize("field", ["strike_price", "entry_fee_twd", "net_pnl_twd", "return_pct",
                                    "opening_price_covered_count", "entry_price_source", "price_offset_ticks"])
def test_non_quote_accounting_metadata_is_not_removed(field):
    assert not quotes.is_quote_price_field(field)


def test_public_disclosure_notice_is_not_a_mobile_hidden_subtitle():
    root = Path(__file__).resolve().parents[1]
    for product in ("tw_day_trade_dashboard", "taifex_dashboard"):
        html = (root / "services" / product / "index.html").read_text(encoding="utf-8")
        assert 'id="quote-display-notice" class="quote-display-notice hidden"' in html
        assert 'id="quote-display-notice" class="subtitle' not in html
    shared = (root / "services/public_dashboards/dashboard-core.css").read_text(encoding="utf-8")
    assert ".quote-display-notice {" in shared
