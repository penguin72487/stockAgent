from scripts.verify_public_price_projection import snapshot_contract_digest, verify_transaction_report, verify_official_reference_fields, verify_strategy_report, verify_entry_distances
from pathlib import Path
import re
import pytest


def test_archive_envelope_clock_is_distinct_from_financial_snapshot_contract():
    row = {"positions": [{"symbol": "2330", "entry_price": 123.45, "signed_shares": 1000}],
           "market": "paper", "archived_at": "2026-10-13T21:00:00+08:00"}
    assert snapshot_contract_digest(row) == snapshot_contract_digest({**row, "archived_at": "2026-10-13T21:01:00+08:00"})


def test_all_financial_fields_and_row_clocks_still_require_exact_parity():
    row = {"positions": [{"entry_price": 123.45, "exit_at": "2026-10-13T13:30:00+08:00"}]}
    before = snapshot_contract_digest(row)
    for key, value in (("entry_price", 123.46), ("exit_at", "2026-10-13T13:29:59+08:00"),
                       ("net_pnl_twd", 1000), ("signed_shares", 1000), ("entry_fee_twd", 1)):
        assert before != snapshot_contract_digest({"positions": [{**row["positions"][0], key: value}]})


def test_deployment_and_source_identity_changes_are_not_ignored():
    before = snapshot_contract_digest({"checkpoint_fingerprint": "first", "positions": []})
    assert before != snapshot_contract_digest({"checkpoint_fingerprint": "second", "positions": []})


def test_public_price_frontend_advances_both_immutable_asset_versions():
    root = Path(__file__).resolve().parents[1]
    html = (root / "services/tw_day_trade_dashboard/index.html").read_text()
    # v10/v100 predate the daily price producer and its shared renderer. Keep
    # the release floor, not a permanently pinned current frontend version.
    for asset, minimum in (("detail-components.js", 19), ("app.js", 103)):
        match = re.search(rf'src="{re.escape(asset)}\?v=(\d+)"', html)
        assert match is not None and int(match[1]) >= minimum


def test_independent_price_distance_oracle_rejects_wrong_bands_and_hidden_price_leaks():
    row = {"symbol": "2330", "entry_price": 999, "exit_price": 1005,
           "public_price_display": {"visible_fields": {"entry_price": {}, "exit_price": {}}},
           "entry_price_distances": {"exit_price": {"contract": "public_entry_price_distance_v1",
                "change_pct": 6 / 999 * 100, "ticks": 2, "tick_session_date": "2026-10-14"}}}
    assert verify_entry_distances(row) == 1
    row["entry_price_distances"]["exit_price"]["ticks"] = 6 / 5
    with pytest.raises(AssertionError, match="cross-band"):
        verify_entry_distances(row)
    row["entry_price_distances"]["exit_price"]["ticks"] = 2
    row["public_price_display"]["withheld_fields"] = ["exit_price"]
    with pytest.raises(AssertionError, match="withheld current"):
        verify_entry_distances(row)


def test_owner_report_acceptance_compares_exact_private_values_and_never_calls_it_a_license():
    original = {"entry_price": 112.123456789, "entry_at": "2026-10-13T09:01:00+08:00", "filled_shares": 1000}
    public = {**original, "public_price_display": {"visible_fields": {"entry_price": {
        "publication_basis": "owner_paper_transaction_report_v1", "evidence_kind": "paper_fill",
        "broker_execution_verified": False, "recorded_at": original["entry_at"],
    }}}}
    assert verify_transaction_report(public, original) == 1
    for change in ({"entry_price": 112.12}, {"filled_shares": 2000}):
        with pytest.raises(AssertionError):
            verify_transaction_report({**public, **change}, original)
    for change in ({"broker_execution_verified": True}, {"recorded_at": "2026-10-13T09:00:00+08:00"},
                   {"license_reference": "imaginary-license"}):
        invalid = {**public, "public_price_display": {"visible_fields": {
            "entry_price": {**public["public_price_display"]["visible_fields"]["entry_price"], **change},
        }}}
        with pytest.raises(AssertionError):
            verify_transaction_report(invalid, original)


def test_acceptance_checks_official_reference_units_and_does_not_authorize_twse_bid_ask():
    canonical = {"open": 28.2, "volume_shares": 18000, "next_upper_limit": 31.0}
    quote = {**canonical, "projection_contract": "official_daily_quote_v2", "venue": "TPEX",
             "session_date": "2026-09-21", "limit_scope": "next_exchange_session_published_bounds",
             "public_price_display": {"visible_fields": {key: {"grain": "daily", "session_date": "2026-09-21",
                 "source_field": key, "license_reference": "https://data.gov.tw/dataset/11370"}
                 for key in ("open", "next_upper_limit")}}}
    assert verify_official_reference_fields(quote, canonical) == 3
    with pytest.raises(AssertionError):
        verify_official_reference_fields({**quote, "volume_shares": 18000000}, canonical)
    with pytest.raises(AssertionError):
        verify_official_reference_fields({**quote, "venue": "TWSE", "last_bid": 28.2}, canonical)


def test_session_gap_and_date_range_remain_distinct_contracts():
    from stockagent.live.tw_day_trade_dashboard import DashboardSessionUnavailable, _select_session_date, _select_session_range
    with pytest.raises(DashboardSessionUnavailable, match="unavailable"):
        _select_session_date("2026-10-13", ["2026-10-14"])
    # Valid interval with no retained session is an honest empty query,
    # not a fake archived session or a failed service.
    assert _select_session_range(start_date="2026-10-13", end_date="2026-10-13",
        session_date=None, available=["2026-10-14"]) == ("2026-10-13", "2026-10-13", [])


def test_strategy_acceptance_checks_original_values_and_does_not_invent_feed_license_or_fill():
    original = {"entry_at": "2026-10-14T09:01:00+08:00", "take_profit_price": 1065.123456789,
                "stop_trigger_price": 1285.0}
    proof = {"publication_basis": "owner_paper_strategy_report_v1", "evidence_kind": "strategy_plan",
             "broker_execution_verified": False, "basis_at": original["entry_at"]}
    row = {**original, "public_price_display": {"visible_fields": {key: proof for key in (
        "take_profit_price", "stop_trigger_price")}}}
    assert verify_strategy_report(row, original) == 2
    for change in ({"take_profit_price": None}, {"take_profit_price": 1065.12}, {"stop_trigger_price": 0}):
        with pytest.raises(AssertionError):
            verify_strategy_report({**row, **change}, original)
    for change in ({"broker_execution_verified": True}, {"evidence_kind": "paper_fill"},
                   {"license_reference": "imaginary-feed-grant"}, {"basis_at": "2026-10-14T09:00:00+08:00"}):
        invalid = {**row, "public_price_display": {"visible_fields": {"take_profit_price": {**proof, **change},
                                                                  "stop_trigger_price": proof}}}
        with pytest.raises(AssertionError):
            verify_strategy_report(invalid, original)
