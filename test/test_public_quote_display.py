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


def paper_position(**changes):
    return {
        "symbol": "2330", "market": "tw_day_trade", "session_date": "2026-10-13",
        "entry_at": "2026-10-13T09:01:00+08:00", "entry_price": 112.123456789,
        "inventory_basis_price": 112.123456789, "filled_shares": 3000, "signed_shares": 0,
        "exit_price": 115.5, "last_exit_price": 115.5,
        "exit_at": "2026-10-13T11:00:00+08:00", "last_exit_at": "2026-10-13T11:00:00+08:00",
        "status": "closed", "entry_price_source": "shioaji:historical_kbar_0901_minute_close",
        "last_mark_price": 116.0, "bid": 115.5, "ask": 116.0,
        "reconciled_total_net_pnl_twd": 9000.12345,
        **changes,
    }


@pytest.mark.parametrize("field", ["entry_price", "inventory_basis_price", "exit_price", "last_exit_price"])
def test_owner_reports_preserve_original_price_and_explicit_paper_evidence(field):
    row = paper_position()
    before = deepcopy(row)
    policy = quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True)
    price, proof = policy.transaction_report_price(row, field, row[field], record_kind="paper_position", now=NOW)
    assert price == row[field]
    assert proof["publication_basis"] == quotes.PAPER_REPORT_CONTRACT
    assert proof["evidence_kind"] == "paper_fill"
    assert proof["broker_execution_verified"] is False
    assert "license_reference" not in proof
    assert row == before


@pytest.mark.parametrize("change", [
    {"filled_shares": 0}, {"filled_shares": True}, {"filled_shares": 0.5},
    {"entry_at": None}, {"entry_at": "2026-10-13T09:01:00"},
    {"entry_at": "2026-10-13T13:00:01+08:00"}, {"session_date": "2026-10-12"},
    {"symbol": None}, {"entry_price": True}, {"entry_price": float("nan")},
    {"entry_price": float("inf")}, {"entry_price": -1},
])
def test_owner_reports_do_not_invent_fills_or_clocks(change):
    row = paper_position(**change)
    policy = quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True)
    assert policy.transaction_report_price(row, "entry_price", row["entry_price"], record_kind="paper_position", now=NOW) is None


@pytest.mark.parametrize("field", ["bid", "ask", "last_mark_price", "open", "close", "sizing_open_price",
                                    "take_profit_price", "order_limit_price", "price"])
def test_own_transaction_report_is_not_a_blanket_quote_grant(field):
    row = paper_position(**{field: 112.0, "public_display_authorized": True})
    policy = quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True)
    assert policy.transaction_report_price(row, field, 112.0, record_kind="paper_position", now=NOW) is None
    assert quotes.PublicQuoteDisplayPolicy().transaction_report_price(row, "entry_price", row["entry_price"], record_kind="paper_position", now=NOW) is None
    assert policy.transaction_report_price(row, "entry_price", row["entry_price"], record_kind=None, now=NOW) is None


def test_only_recorded_partial_exit_is_disclosed_before_position_is_closed():
    policy = quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True)
    row = paper_position(status="open", signed_shares=1000)
    assert policy.transaction_report_price(row, "last_exit_price", row["last_exit_price"], record_kind="paper_position", now=NOW)
    assert policy.transaction_report_price(row, "exit_price", row["exit_price"], record_kind="paper_position", now=NOW) is None
    for change in ({"last_exit_at": None}, {"last_exit_at": "2026-10-13T09:00:00+08:00"},
                   {"last_exit_at": "2026-10-13T13:00:01+08:00"}, {"signed_shares": 3000}):
        invalid = {**row, **change}
        assert policy.transaction_report_price(invalid, "last_exit_price", invalid["last_exit_price"], record_kind="paper_position", now=NOW) is None


@pytest.mark.parametrize("sanitizer", [public.sanitize_tw_status, public.sanitize_tw_positions, public.sanitize_tw_signals])
def test_public_endpoints_use_trusted_record_context_without_mutation_or_raw_quote_leak(monkeypatch, sanitizer):
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True))
    position = paper_position()
    row = position if sanitizer is not public.sanitize_tw_signals else {
        "symbol": position["symbol"], "market": position["market"], "session_date": position["session_date"],
        "filled_shares": position["filled_shares"], "execution_price": position["entry_price"], "position": position,
        "bid": 111.0, "ask": 112.0,
    }
    payload = {"simulation_only": True, "production_order_possible": False, "rows": [row], "positions": [row]}
    before = deepcopy(payload)
    out = sanitizer(payload)
    published = out["positions" if sanitizer is public.sanitize_tw_status else "rows"][0]
    if sanitizer is public.sanitize_tw_signals:
        assert published["execution_price"] == position["entry_price"]
        assert published["bid"] is None and published["ask"] is None
        published = published["position"]
    assert published["entry_price"] == position["entry_price"]
    assert published["exit_price"] == position["exit_price"]
    assert published["last_mark_price"] is None
    assert published["reconciled_total_net_pnl_twd"] == position["reconciled_total_net_pnl_twd"]
    assert payload == before
    # A direct arbitrary quote mapping cannot opt into the owner-report scope.
    assert public._scrub_public_value({**position, "record_kind": "paper_position"})["entry_price"] is None


def test_record_context_does_not_propagate_into_nested_quote_mappings(monkeypatch):
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True))
    row = {**paper_position(), "quotes": [paper_position()]}
    result = public._scrub_public_value(row, record_kind="paper_position")
    assert result["entry_price"] == row["entry_price"]
    assert result["quotes"][0]["entry_price"] is None


def test_signal_price_requires_matching_recorded_position_not_signal_clock_alone():
    position = paper_position()
    row = {"symbol": position["symbol"], "market": position["market"], "session_date": position["session_date"],
           "signal_at": "2026-10-13T09:00:00+08:00", "filled_shares": 1000,
           "execution_price": position["entry_price"], "position": position}
    policy = quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True)
    assert policy.transaction_report_price(row, "execution_price", row["execution_price"], record_kind="paper_signal", now=NOW)
    for change in ({"position": None}, {"symbol": "0050"}, {"market": "other"}, {"filled_shares": 0},
                   {"filled_shares": 4000}, {"execution_price": 112.0},
                   {"position": {**position, "filled_shares": 0}}):
        invalid = {**row, **change}
        assert policy.transaction_report_price(invalid, "execution_price", invalid["execution_price"], record_kind="paper_signal", now=NOW) is None


def test_order_prices_cannot_become_own_fills_and_fill_event_keeps_its_original_clock(monkeypatch):
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True))
    row = {"symbol": "2330", "event_kind": "fill", "simulation_only": True, "status": "filled",
           "quantity": 1000, "price": 112.123456789, "recorded_at": "2026-10-13T11:02:00+08:00"}
    out = public.sanitize_tw_events({"simulation_only": True, "production_order_possible": False,
                                   "rows": [row, {**row, "event_kind": "order"}]})["rows"]
    assert out[0]["price"] == row["price"] and out[1]["price"] is None
    assert out[0]["public_price_display"]["visible_fields"]["price"]["recorded_at"] == row["recorded_at"]
    policy = quotes.PublicQuoteDisplayPolicy(paper_transaction_reports=True)
    for change in ({"quantity": 0}, {"simulation_only": False}, {"status": "pending"}, {"recorded_at": None}):
        invalid = {**row, **change}
        assert policy.transaction_report_price(invalid, "price", invalid["price"], record_kind="paper_event", now=NOW) is None


@pytest.mark.parametrize("config", [None, {"enabled": True}, {"enabled": "true", "contract": quotes.PAPER_REPORT_CONTRACT},
                                   {"enabled": True, "contract": quotes.PAPER_REPORT_CONTRACT, "record_scope": "all_quotes"}])
def test_malformed_owner_report_config_cannot_expand_publication_scope(tmp_path, monkeypatch, config):
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps({"schema_version": 1, "contract": quotes.CONTRACT,
        "default_policy": "withhold_without_verified_public_display_grant", "authorized_sources": [],
        "owner_transaction_reports": config}))
    monkeypatch.setattr(quotes, "_POLICY_PATH", policy_path)
    quotes.public_quote_display_policy.cache_clear()
    try:
        assert quotes.public_quote_display_policy().paper_transaction_reports is False
    finally:
        quotes.public_quote_display_policy.cache_clear()


@pytest.mark.parametrize("field,clock_field", list(quotes.STRATEGY_POSITION_CLOCKS.items()))
@pytest.mark.parametrize("filled", [0, 3000])
def test_owner_strategy_level_does_not_require_a_fill_or_round_original_value(field, clock_field, filled):
    row = paper_position(**{field: 123.456789, clock_field: "2026-10-13T09:01:00+08:00",
                            "opening_exit_order_session_date": "2026-10-13", "filled_shares": filled})
    before = deepcopy(row)
    policy = quotes.PublicQuoteDisplayPolicy(paper_strategy_reports=True)
    value, proof = policy.strategy_report_price(row, field, row[field], record_kind="paper_position", now=NOW)
    assert value == 123.456789
    assert proof["publication_basis"] == quotes.STRATEGY_REPORT_CONTRACT
    assert proof["evidence_kind"] == "strategy_plan" and proof["broker_execution_verified"] is False
    assert proof["basis_at"] == row[clock_field]
    assert "license_reference" not in proof and "recorded_at" not in proof
    assert row == before


@pytest.mark.parametrize("change", [
    {"take_profit_price": True}, {"take_profit_price": float("nan")}, {"take_profit_price": float("inf")},
    {"take_profit_price": 0}, {"take_profit_price": -1}, {"market": None}, {"symbol": ""},
    {"entry_at": None}, {"entry_at": "2026-10-13T09:01:00"},
    {"entry_at": "2026-10-13T13:00:01+08:00"}, {"session_date": "2026-10-12"},
])
def test_owner_strategy_report_rejects_invalid_context_value_or_clock(change):
    row = paper_position(take_profit_price=125.0, **change) if "take_profit_price" not in change else paper_position(**change)
    policy = quotes.PublicQuoteDisplayPolicy(paper_strategy_reports=True)
    assert policy.strategy_report_price(row, "take_profit_price", row["take_profit_price"],
                                        record_kind="paper_position", now=NOW) is None


@pytest.mark.parametrize("field", ["entry_price", "execution_price", "sizing_open_price", "last_mark_price",
    "bid", "ask", "open", "close", "upper_limit", "lower_limit", "next_upper_limit", "price"])
def test_owner_strategy_report_does_not_authorize_inputs_quotes_exchange_limits_or_fills(field):
    row = paper_position(**{field: 123.0, "public_display_authorized": True})
    policy = quotes.PublicQuoteDisplayPolicy(paper_strategy_reports=True)
    assert policy.strategy_report_price(row, field, 123.0, record_kind="paper_position", now=NOW) is None
    assert policy.observation(row, field, 123.0, now=NOW) is None


@pytest.mark.parametrize("sanitizer", [public.sanitize_tw_status, public.sanitize_tw_positions, public.sanitize_tw_signals])
def test_strategy_reports_share_status_positions_and_signal_lifecycle_without_raw_quote_leak(monkeypatch, sanitizer):
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: quotes.PublicQuoteDisplayPolicy(paper_strategy_reports=True))
    pos = paper_position(take_profit_price=120.123456789, stop_trigger_price=105.05,
                         closing_auction_limit_price=103.0, closing_auction_order_submitted_at="2026-10-13T12:00:00+08:00")
    row = pos if sanitizer is not public.sanitize_tw_signals else {
        "symbol": pos["symbol"], "market": pos["market"], "session_date": pos["session_date"],
        "signal_at": "2026-10-13T09:00:00+08:00", "order_limit_price": 125.0, "position": pos,
        "filled_shares": 0, "execution_price": 123.0, "bid": 111.0, "ask": 112.0,
    }
    source = {"simulation_only": True, "production_order_possible": False, "rows": [row], "positions": [row]}
    before = deepcopy(source)
    out = sanitizer(source)
    published = out["positions" if sanitizer is public.sanitize_tw_status else "rows"][0]
    if sanitizer is public.sanitize_tw_signals:
        assert published["order_limit_price"] == 125.0  # A planned price, even with no fill.
        assert published["execution_price"] is None and published["bid"] is None and published["ask"] is None
        published = published["position"]
    for field in ("take_profit_price", "stop_trigger_price", "closing_auction_limit_price"):
        assert published[field] == pos[field]
        assert published["public_price_display"]["visible_fields"][field]["evidence_kind"] == "strategy_plan"
    assert published["entry_price"] is None and published["last_mark_price"] is None
    assert published["reconciled_total_net_pnl_twd"] == pos["reconciled_total_net_pnl_twd"]
    assert source == before
    assert public._scrub_public_value({**pos, "record_kind": "paper_position"})["take_profit_price"] is None
    nested = public._scrub_public_value({**pos, "quotes": [pos]}, record_kind="paper_position")
    assert nested["take_profit_price"] == pos["take_profit_price"]
    assert nested["quotes"][0]["take_profit_price"] is None


def test_next_open_order_plan_has_its_own_session_not_the_entry_date():
    row = paper_position(opening_exit_limit_price=120.0, opening_exit_order_session_date="2026-10-14",
                         opening_exit_submitted_at="2026-10-14T08:30:00+08:00")
    policy = quotes.PublicQuoteDisplayPolicy(paper_strategy_reports=True)
    now = datetime.fromisoformat("2026-10-14T09:00:00+08:00")
    assert policy.strategy_report_price(row, "opening_exit_limit_price", 120.0,
                                        record_kind="paper_position", now=now)[1]["session_date"] == "2026-10-14"
    row["opening_exit_order_session_date"] = "2026-10-15"
    assert policy.strategy_report_price(row, "opening_exit_limit_price", 120.0, record_kind="paper_position", now=now) is None


def test_own_order_event_is_plan_not_a_fake_fill(monkeypatch):
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: quotes.PublicQuoteDisplayPolicy(paper_strategy_reports=True))
    row = {"symbol": "2330", "market": "tw_overnight", "session_date": "2026-10-13", "event_kind": "order",
           "simulation_only": True, "order_type": "LMT_ROD", "status": "working", "quantity": 1000,
           "price": 123.456789, "recorded_at": "2026-10-13T12:00:00+08:00"}
    out = public.sanitize_tw_events({"simulation_only": True, "production_order_possible": False,
                                   "rows": [row, {**row, "event_kind": "fill"}, {**row, "order_type": "MKT"}]})["rows"]
    assert out[0]["price"] == row["price"]
    assert out[0]["public_price_display"]["visible_fields"]["price"]["evidence_kind"] == "strategy_plan"
    assert out[1]["price"] is None and out[2]["price"] is None


@pytest.mark.parametrize("config", [None, {"enabled": True},
    {"enabled": "true", "contract": quotes.STRATEGY_REPORT_CONTRACT},
    {"enabled": True, "contract": quotes.STRATEGY_REPORT_CONTRACT, "record_scope": "all_quotes"}])
def test_malformed_strategy_policy_cannot_expand_the_scope(tmp_path, monkeypatch, config):
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps({"schema_version": 1, "contract": quotes.CONTRACT,
        "default_policy": "withhold_without_verified_public_display_grant", "authorized_sources": [],
        "owner_strategy_reports": config}))
    monkeypatch.setattr(quotes, "_POLICY_PATH", policy_path)
    quotes.public_quote_display_policy.cache_clear()
    try:
        assert quotes.public_quote_display_policy().paper_strategy_reports is False
    finally:
        quotes.public_quote_display_policy.cache_clear()


def test_operator_strategy_request_is_enabled_separately_from_quote_grants():
    policy = quotes.public_quote_display_policy()
    assert policy.paper_strategy_reports is True and policy.paper_transaction_reports is True
    assert {grant["source_id"] for grant in policy.grants} == {"twse_daily_ohlcv", "tpex_daily_ohlcv"}


def test_loaded_disclosure_revision_changes_with_scope_or_grant_not_mapping_key_order():
    policy = quotes.PublicQuoteDisplayPolicy((GRANT,), paper_transaction_reports=True)
    revision = policy.revision
    assert "revision" in policy.__dict__  # computed once, not per field/row
    assert revision == quotes.PublicQuoteDisplayPolicy((dict(reversed(list(GRANT.items()))),),
                                                      paper_transaction_reports=True).revision
    assert revision != quotes.PublicQuoteDisplayPolicy((GRANT,), paper_transaction_reports=True,
                                                       paper_strategy_reports=True).revision
    assert revision != quotes.PublicQuoteDisplayPolicy(({**GRANT, "public_display": False},),
                                                       paper_transaction_reports=True).revision
