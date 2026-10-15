from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.live.public_daily_quotes import OfficialDailyQuoteSource
from stockagent.live import public_dashboards as public
from stockagent.live.public_quote_display import PublicQuoteDisplayPolicy, public_quote_display_policy


DAY = "2026-09-21"
NOW = datetime.fromisoformat("2026-10-13T20:00:00+08:00")


def official_row(**overrides):
    return {"date": DAY, "證券代號": "2330", "開盤價": "2,445.00",
            "最高價": "2,485.00", "最低價": "2,445.00", "收盤價": "2,480.00",
            "_dataset": "twse_daily_ohlcv", "_source": "TWSE",
            "_downloaded_at_utc": "2026-09-22T09:13:47+00:00",
            "_url": "https://wwwc.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date=20260921",
            **overrides}


def source_at(tmp_path, rows=None, **kwargs):
    pq.write_table(pa.Table.from_pylist(rows or [official_row()]), tmp_path / "twse_daily_ohlcv.parquet")
    return OfficialDailyQuoteSource(tmp_path, **kwargs)


def tpex_row(**overrides):
    return {"date": DAY, "代號": "6168", "開盤": "28.00", "最高": "29.20",
            "最低": "27.80", "收盤": "28.20", "均價": "28.1234",
            "最後買價": "28.15", "最後賣價": "28.20", "次日參考價": "28.20",
            "次日漲停價": "31.00", "次日跌停價": "25.40",
            "成交股數": "18,000", "成交金額(元)": "506,221", "成交筆數": "18",
            "_dataset": "tpex_daily_ohlcv", "_source": "TPEx",
            "_downloaded_at_utc": "2026-09-21T06:10:00+00:00",
            "_url": "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date=2026/09/21",
            **overrides}


@pytest.mark.parametrize("sanitize", [public.sanitize_tw_signals, public.sanitize_tw_positions,
                                      public.sanitize_tw_events, public.sanitize_tw_status])
def test_official_tpex_reference_fields_share_all_endpoints_with_exact_units(tmp_path, sanitize):
    pq.write_table(pa.Table.from_pylist([tpex_row()]), tmp_path / "tpex_daily_ohlcv.parquet")
    source = OfficialDailyQuoteSource(tmp_path)
    original = {"symbol": "6168", "session_date": DAY, "bid": 28.15, "ask": 28.2,
                "upper_limit": 31., "lower_limit": 25.4, "next_upper_limit": 31.,
                "reconciled_total_net_pnl_twd": 120.15, "filled_shares": 0}
    before = deepcopy(original)
    result = sanitize(payload(original), quote_source=source)
    out = result["positions" if sanitize is public.sanitize_tw_status else "rows"][0]
    quote = out["official_daily_quote"]
    assert quote["projection_contract"] == "official_daily_quote_v2"
    assert quote["quote_context"] == "official_after_close_not_realtime"
    assert quote["limit_scope"] == "next_exchange_session_published_bounds"
    assert {key: quote[key] for key in ("last_bid", "last_ask", "average_price", "next_reference_price",
                                     "next_upper_limit", "next_lower_limit")} == {
        "last_bid": 28.15, "last_ask": 28.2, "average_price": 28.1234,
        "next_reference_price": 28.2, "next_upper_limit": 31., "next_lower_limit": 25.4,
    }
    assert (quote["volume_shares"], quote["turnover"], quote["trade_count"]) == (18000, 506221, 18)
    for field in ("last_bid", "last_ask", "next_upper_limit", "next_lower_limit"):
        proof = quote["public_price_display"]["visible_fields"][field]
        assert proof["grain"] == "daily"
        assert proof["session_date"] == DAY
        assert proof["license_reference"] == "https://data.gov.tw/dataset/11370"
    # Identical numbers do not turn AFTER-CLOSE or NEXT-session facts into a
    # current session's executable bid/ask or limit.
    for field in ("bid", "ask", "upper_limit", "lower_limit", "next_upper_limit"):
        assert out.get(field) is None  # Endpoint allowlists may omit the field entirely.
        assert public_quote_display_policy().observation(original, field, original[field], now=NOW) is None
    assert original == before
    if sanitize in (public.sanitize_tw_positions, public.sanitize_tw_status):
        assert out["reconciled_total_net_pnl_twd"] == 120.15


def test_official_next_bounds_preserve_no_limit_sentinels_and_actual_zero_volume(tmp_path):
    pq.write_table(pa.Table.from_pylist([tpex_row(次日漲停價="9,999.95", 次日跌停價="0.01",
        成交股數="0", 成交筆數="0", **{"成交金額(元)": "0"})]), tmp_path / "tpex_daily_ohlcv.parquet")
    quote = OfficialDailyQuoteSource(tmp_path).session(DAY)["6168"]
    assert (quote["next_upper_limit"], quote["next_lower_limit"]) == (9999.95, 0.01)
    assert (quote["volume_shares"], quote["turnover"], quote["trade_count"]) == (0, 0, 0)


def test_optional_columns_do_not_authorize_unlicensed_twse_final_quotes(tmp_path):
    quote = source_at(tmp_path, [official_row(最後揭示買價="2475", 最後揭示賣價="2480",
        成交股數="1,000", 成交金額="2,480,000", 成交筆數="1")]).session(DAY)["2330"]
    assert "last_bid" not in quote and "last_ask" not in quote
    assert (quote["volume_shares"], quote["turnover"], quote["trade_count"]) == (1000, 2480000, 1)


@pytest.mark.parametrize("change", [
    {"_downloaded_at_utc": DAY + "T04:59:00Z"},
    {"_downloaded_at_utc": "2099-01-01T00:00:00Z"},
    {"_url": "https://tpex.org.tw.attacker.example/afterTrading/otc"},
])
def test_unverified_tpex_receipts_cannot_leak_extra_prices_or_measures(tmp_path, change):
    pq.write_table(pa.Table.from_pylist([tpex_row(**change)]), tmp_path / "tpex_daily_ohlcv.parquet")
    assert OfficialDailyQuoteSource(tmp_path).session(DAY) == {}


def test_missing_and_nonfinite_optional_values_are_not_fabricated(tmp_path):
    pq.write_table(pa.Table.from_pylist([tpex_row(均價="NaN", 最後買價="--", 最後賣價="0",
        次日參考價=None, 次日漲停價="inf", 次日跌停價="-1", 成交股數=None,
        成交筆數="-1", **{"成交金額(元)": "NaN"})]), tmp_path / "tpex_daily_ohlcv.parquet")
    quote = OfficialDailyQuoteSource(tmp_path).session(DAY)["6168"]
    for field in ("average_price", "last_bid", "last_ask", "next_reference_price", "next_upper_limit",
                  "next_lower_limit", "volume_shares", "turnover", "trade_count"):
        assert quote[field] is None
    assert quote["close"] == 28.2


def payload(row):
    return {"simulation_only": True, "production_order_possible": False,
            "session_date": DAY, "rows": [row], "positions": [row]}


@pytest.mark.parametrize("sanitize", [public.sanitize_tw_signals, public.sanitize_tw_positions,
                                      public.sanitize_tw_events, public.sanitize_tw_status])
@pytest.mark.parametrize("owner_reports", [False, True])
def test_all_tw_endpoints_share_daily_source_without_changing_paper_fills(tmp_path, sanitize, monkeypatch, owner_reports):
    policy = public_quote_display_policy()
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: PublicQuoteDisplayPolicy(
        policy.grants, paper_transaction_reports=owner_reports,
    ))
    source = source_at(tmp_path)
    row = {"symbol": "2330", "session_date": DAY, "sizing_open_price": 2445.,
           "entry_price": 2446., "execution_price": 2446., "price": 2446.,
           "last_mark_price": 2479., "entry_at": DAY + "T09:01:00+08:00",
           "filled_shares": 1000, "reconciled_total_net_pnl_twd": 32000.,
           "entry_price_source": "local_minute_parquet_0901_minute_vwap"}
    before = deepcopy(row)
    result = sanitize(payload(row), quote_source=source)
    out = result["positions" if sanitize is public.sanitize_tw_status else "rows"][0]
    assert out["official_daily_quote"]["open"] == 2445.
    assert out["official_daily_quote"]["close"] == 2480.
    assert out["official_daily_quote"]["received_at"].startswith("2026-09-22")
    assert out["official_daily_quote"]["session_date"] == DAY
    for field in ("entry_price", "execution_price", "last_mark_price", "price"):
        if field in out:
            if field == "entry_price" and owner_reports and sanitize in (public.sanitize_tw_positions, public.sanitize_tw_status):
                assert out[field] == row[field]
                assert out["public_price_display"]["visible_fields"][field]["publication_basis"] == "owner_paper_transaction_report_v1"
            else:
                assert out[field] is None
    assert row == before
    assert "public_price_observations" not in json.dumps(result)
    assert "_url" not in json.dumps(result)


def test_sizing_open_has_role_specific_public_proof_not_a_fake_tick_clock(tmp_path):
    source = source_at(tmp_path)
    out = public.sanitize_tw_signals(payload({"symbol": "2330", "session_date": DAY,
        "sizing_open_price": 2445., "exchange_quote_at": DAY + "T09:00:01+08:00"}), quote_source=source)["rows"][0]
    assert out["sizing_open_price"] == 2445.
    proof = out["public_price_display"]["visible_fields"]["sizing_open_price"]
    assert proof["grain"] == "daily"
    assert proof["session_date"] == DAY
    assert proof["observed_at"].startswith("2026-09-22")
    assert "data.gov.tw/dataset/11549" in proof["license_reference"]


def test_daily_close_cannot_replace_intraday_or_other_session_fill(tmp_path):
    source = source_at(tmp_path)
    row = {"symbol": "2330", "session_date": DAY, "entry_price": 2480.,
           "entry_at": "2026-09-18T13:30:00+08:00", "entry_price_source": "official_close"}
    out = public.sanitize_tw_positions(payload(row), quote_source=source)["rows"][0]
    assert out["entry_price"] is None
    row.update(entry_at=DAY + "T13:30:00+08:00", entry_price_source="historical_official_close_counterfactual")
    assert public.sanitize_tw_positions(payload(row), quote_source=source)["rows"][0]["entry_price"] == 2480.


@pytest.mark.parametrize("change", [
    {"_source": "Shioaji"}, {"_dataset": "other"},
    {"_url": "https://twse.com.tw.attacker.example/quotes"},
    {"_url": "https://evil.example/quotes"}, {"_downloaded_at_utc": "2026-09-22T09:00:00"},
    {"開盤價": "--", "收盤價": "----", "最高價": "NaN", "最低價": "0"},
])
def test_unproven_and_no_trade_rows_are_not_usable_prices(tmp_path, change):
    assert source_at(tmp_path, [official_row(**change)]).session(DAY) == {}


def test_conflicting_duplicates_are_not_silently_chosen(tmp_path):
    assert source_at(tmp_path, [official_row(), official_row(收盤價="2481")]).session(DAY) == {}


def test_future_receipt_and_wrong_grain_cannot_use_daily_grant(tmp_path):
    source = source_at(tmp_path)
    record = source.enrich({"symbol": "2330", "session_date": DAY})["official_daily_quote"]
    policy = public_quote_display_policy()
    candidate = record["public_price_observations"]["open"]
    candidate["observed_at"] = "2026-10-14T00:00:00+08:00"
    assert policy.observation(record, "open", 2445., now=NOW) is None
    candidate.update(observed_at=DAY + "T09:00:01+08:00", grain="daily")
    assert policy.observation(record, "open", 2445., now=NOW) is None
    candidate.update(observed_at=DAY + "T09:00:01+08:00", grain="tick")
    assert policy.observation(record, "open", 2445., now=NOW) is None


def test_daily_grant_cannot_authorize_bid_or_arbitrary_derived_fill(tmp_path):
    source = source_at(tmp_path)
    record = source.enrich({"symbol": "2330", "session_date": DAY})["official_daily_quote"]
    candidate = dict(record["public_price_observations"]["open"])
    candidate.update(price_role="bid")
    record["public_price_observations"]["bid"] = candidate
    assert public_quote_display_policy().observation(record, "bid", 2445., now=NOW) is None


def test_empty_source_recovers_after_canonical_atomic_replacement(tmp_path):
    source = OfficialDailyQuoteSource(tmp_path)
    assert source.session(DAY) == {}
    before = source.revision()
    source_at(tmp_path)
    assert source.revision() != before
    assert source.session(DAY)["2330"]["close"] == 2480.
    replacement = tmp_path / "replacement.parquet"
    pq.write_table(pa.Table.from_pylist([official_row(收盤價="2481")]), replacement)
    replacement.replace(tmp_path / "twse_daily_ohlcv.parquet")
    assert source.session(DAY)["2330"]["close"] == 2481.


def test_concurrent_modes_share_one_session_load_and_bound_cache(tmp_path, monkeypatch):
    source = source_at(tmp_path, maximum_sessions=2)
    load = source._load
    calls = []
    def counted(day):
        calls.append(day)
        return load(day)
    monkeypatch.setattr(source, "_load", counted)
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert all(pool.map(lambda _: source.session(DAY).get("2330") is not None, range(12)))
    assert calls == [DAY]
    source.session("2026-09-18")
    source.session("2026-09-17")
    assert len(source._sessions) == 2


def test_benchmark_uses_selected_session_without_overwriting_old_private_mark(tmp_path):
    source = source_at(tmp_path)
    row = {"symbol": "2330", "last_quote_at": "2026-09-18T09:01:00+08:00", "last_mark_price": 2400.}
    out = public.sanitize_tw_status({"simulation_only": True, "production_order_possible": False,
        "session_date": DAY, "benchmarks": [row]}, quote_source=source)["benchmarks"][0]
    assert out["last_mark_price"] is None
    assert out["last_quote_at"] == row["last_quote_at"]
    assert out["official_daily_quote"]["close"] == 2480.


def test_buy_hold_origin_uses_its_own_official_close_date_even_without_today_report(tmp_path):
    source = source_at(tmp_path)
    row = {"symbol": "2330", "instrument_type": "stock_buy_and_hold",
           "capital_basis": "one_board_lot_previous_close_notional",
           "replay_basis": "cross_session_buy_and_hold_from_previous_close",
           "entry_at": DAY + "T13:30:00+08:00", "entry_price": 2480.,
           "last_mark_price": 2600., "last_quote_at": "2026-10-13T09:05:00+08:00"}
    before = deepcopy(row)
    out = public.sanitize_tw_status({"simulation_only": True, "production_order_possible": False,
        "session_date": "2026-10-13", "benchmarks": [row]}, quote_source=source)["benchmarks"][0]
    assert out["entry_price"] == 2480.
    assert out["public_price_display"]["visible_fields"]["entry_price"]["session_date"] == DAY
    assert out["last_mark_price"] is None
    assert "official_daily_quote" not in out
    assert row == before
    for change in ({"replay_basis": "broker_fill"}, {"entry_price": 2479.},
                   {"instrument_type": "continuous_long_future"},
                   {"entry_at": DAY + "T09:01:00+08:00"}):
        record = source.enrich({**row, **change}, session_date="2026-10-13")
        assert public_quote_display_policy().observation(record, "entry_price", record["entry_price"], now=NOW) is None


def test_overnight_open_exit_verifies_next_session_open_not_entry_session_close(tmp_path):
    source = source_at(tmp_path)
    row = {"symbol": "2330", "session_date": "2026-09-18", "exit_session_date": DAY,
           "exit_reason": "next_session_open_auction_fill", "opening_exit_order_status": "filled",
           "exit_at": DAY + "T09:00:04+08:00", "exit_exchange_at": DAY + "T09:00:00+08:00",
           "exit_price": 2445., "last_exit_price": 2445., "filled_shares": 1000}
    out = public.sanitize_tw_positions(payload(row), quote_source=source)["rows"][0]
    assert out["exit_price"] == out["last_exit_price"] == 2445.
    proof = out["public_price_display"]["visible_fields"]["exit_price"]
    assert proof["session_date"] == DAY and proof["source_field"] == "open"
    for change in ({"exit_reason": "13_20_limit_filled"}, {"opening_exit_order_status": "working"},
                   {"exit_price": 2446.}, {"exit_session_date": "2026-09-18"}):
        record = source.enrich({**row, **change})
        assert public_quote_display_policy().observation(record, "exit_price", record["exit_price"], now=NOW) is None


def test_shioaji_minute_price_gap_is_unverified_permission_not_failed_acquisition(tmp_path):
    source = source_at(tmp_path)
    row = {"symbol": "2330", "session_date": DAY, "entry_at": DAY + "T09:01:00+08:00",
           "entry_price": 2445., "entry_price_source": "shioaji:historical_kbar_0901_minute_close"}
    out = public.sanitize_tw_positions(payload(row), quote_source=source)["rows"][0]
    # Numerical equality with an official open cannot change its minute role.
    assert out["entry_price"] is None
    assert out["official_daily_quote"]["open"] == 2445.
    assert out["public_price_display"]["withheld_reasons"]["entry_price"] == "public_display_permission_unverified"
    assert row["entry_price"] == 2445.


def test_terminal_paper_close_is_verified_but_a_same_price_intraday_exit_is_not(tmp_path):
    source = source_at(tmp_path)
    row = {"symbol": "2330", "session_date": DAY, "exit_price": 2480.,
           "last_exit_price": 2480., "last_mark_price": 2480.,
           "exit_reason": "13_30_unlimited_close_paper_settlement",
           "exit_at": DAY + "T13:30:00+08:00", "last_quote_at": DAY + "T13:30:00+08:00"}
    out = public.sanitize_tw_positions(payload(row), quote_source=source)["rows"][0]
    assert out["exit_price"] == out["last_mark_price"] == 2480.
    for change in ({"exit_at": DAY + "T13:20:00+08:00"}, {"exit_reason": "take_profit"}):
        out = public.sanitize_tw_positions(payload({**row, **change}), quote_source=source)["rows"][0]
        assert out["exit_price"] is None
    out = public.sanitize_tw_positions(payload({**row, "last_quote_at": DAY + "T09:01:00+08:00"}), quote_source=source)["rows"][0]
    assert out["exit_price"] == 2480.
    assert out["last_mark_price"] is None


def test_missing_one_venue_does_not_stop_other_venue(tmp_path):
    source = source_at(tmp_path)
    (tmp_path / "tpex_daily_ohlcv.parquet").write_bytes(b"broken fixture")
    assert source.session(DAY)["2330"]["close"] == 2480.


def test_invalid_policy_still_withholds_even_real_official_fact(tmp_path, monkeypatch):
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: PublicQuoteDisplayPolicy())
    source = source_at(tmp_path)
    out = public.sanitize_tw_positions(payload({"symbol": "2330", "session_date": DAY}), quote_source=source)["rows"][0]
    assert out["official_daily_quote"]["open"] is None
    assert out["official_daily_quote"]["close"] is None


def test_empty_strategy_still_has_reference_prices_without_any_fake_signal(tmp_path):
    source = source_at(tmp_path)
    out = public.sanitize_tw_status({"simulation_only": True, "production_order_possible": False,
        "session_date": DAY, "positions": [], "signals": []}, quote_source=source)
    assert out["positions"] == out["signals"] == []
    reference = out["official_price_reference"]
    assert reference["role"] == "reference_only"
    row = next(item for item in reference["quotes"] if item["symbol"] == "2330")
    assert row["official_daily_quote"]["close"] == 2480.


def test_live_view_retains_prior_official_reference_without_replacing_today_prices(tmp_path, monkeypatch):
    from stockagent.live import public_daily_quotes as module

    today = "2026-09-22"
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat(today + "T20:00:00+08:00").astimezone(tz)
    monkeypatch.setattr(module, "datetime", Clock)
    source = source_at(tmp_path)
    row = {"symbol": "2330", "session_date": today, "sizing_open_price": 2445.,
           "last_mark_price": 2480., "bid": 2480.}
    out = public.sanitize_tw_signals({**payload(row), "session_date": today}, quote_source=source)["rows"][0]
    daily = out["official_daily_quote"]
    assert daily["session_date"] == DAY
    assert daily["requested_session_date"] == today
    assert daily["prior_session_reference_only"] is True
    assert daily["close"] == 2480.
    assert out["sizing_open_price"] is None
    assert out.get("last_mark_price") is None  # Signal DTO intentionally omits valuation fields.
    assert out["bid"] is None
    # Historical absence cannot quietly inherit an unrelated prior day.
    assert "official_daily_quote" not in source.enrich({"symbol": "2330", "session_date": "2026-09-23"})


def test_status_reference_date_is_actual_source_date_not_requested_live_session(tmp_path):
    source = source_at(tmp_path)
    today = "2026-09-22"
    out = public.sanitize_tw_status({"simulation_only": True, "production_order_possible": False,
        "session_date": today, "positions": [], "signals": []}, quote_source=source)
    assert out["session_date"] == today
    ref = out["official_price_reference"]
    assert ref["session_date"] == DAY and ref["requested_session_date"] == today
    assert ref["quotes"][1]["official_daily_quote"]["session_date"] == DAY
    assert out["positions"] == out["signals"] == []


def test_prior_date_cache_is_bounded_and_invalidated_with_canonical_source(tmp_path):
    source = source_at(tmp_path, maximum_sessions=2)
    assert source.prior_completed_session("2026-09-22") == DAY
    assert source.prior_completed_session("2026-09-23") == DAY
    assert source.prior_completed_session("2026-09-24") == DAY
    assert len(source._prior_sessions) == 2
    replacement = tmp_path / "replacement.parquet"
    pq.write_table(pa.Table.from_pylist([official_row(date="2026-09-18")]), replacement)
    replacement.replace(tmp_path / "twse_daily_ohlcv.parquet")
    assert source.prior_completed_session("2026-09-22") == "2026-09-18"


def test_price_publication_changes_ui_revision_but_not_large_history_revision(tmp_path, monkeypatch):
    from scripts import serve_public_dashboards as gateway
    server = gateway.PublicDashboardServer(("127.0.0.1", 0), repo_root=tmp_path,
        public_static_root=tmp_path, taifex_static_root=tmp_path, tw_static_root=tmp_path,
        shioaji_static_root=tmp_path, openbb_static_root=tmp_path,
        data_monitor_static_root=tmp_path, traffic_static_root=tmp_path,
        taifex_upstream="http://127.0.0.1:1", tw_upstream="http://127.0.0.1:1")
    monkeypatch.setattr(gateway, "build_dashboard_revision", lambda **_: {"revision_token": "unchanged-ledger"})
    try:
        before = [json.loads(method().body) for method in (server.tw_revision, server.overnight_revision)]
        root = tmp_path / "data_tw_public"
        root.mkdir()
        source_at(root)
        after = [json.loads(method().body) for method in (server.tw_revision, server.overnight_revision)]
        for old, new in zip(before, after):
            assert old["revision_token"] != new["revision_token"]
            assert old["history_revision"] == new["history_revision"]
            assert old["public_quote_revision"] != new["public_quote_revision"]
        for path in server.public_quote_source.paths:
            assert path in server.update_hub.metadata_only_paths
            assert all(path in server.update_hub.paths[topic] for topic in ("tw", "overnight"))
    finally:
        server.server_close()


def test_owner_disclosure_scope_invalidates_both_detail_revisions_not_financial_history(tmp_path, monkeypatch):
    from scripts import serve_public_dashboards as gateway
    from stockagent.live.public_quote_display import PublicQuoteDisplayPolicy
    server = gateway.PublicDashboardServer(("127.0.0.1", 0), repo_root=tmp_path,
        public_static_root=tmp_path, taifex_static_root=tmp_path, tw_static_root=tmp_path,
        shioaji_static_root=tmp_path, openbb_static_root=tmp_path,
        data_monitor_static_root=tmp_path, traffic_static_root=tmp_path,
        taifex_upstream="http://127.0.0.1:1", tw_upstream="http://127.0.0.1:1")
    monkeypatch.setattr(gateway, "build_dashboard_revision", lambda **_: {"revision_token": "unchanged-ledger"})
    try:
        monkeypatch.setattr(gateway, "public_quote_display_policy", lambda: PublicQuoteDisplayPolicy())
        old = [json.loads(method().body) for method in (server.tw_revision, server.overnight_revision)]
        monkeypatch.setattr(gateway, "public_quote_display_policy",
                            lambda: PublicQuoteDisplayPolicy(paper_strategy_reports=True))
        new = [json.loads(method().body) for method in (server.tw_revision, server.overnight_revision)]
        for before, after in zip(old, new):
            assert before["revision_token"] != after["revision_token"]
            assert before["public_quote_revision"] != after["public_quote_revision"]
            assert before["history_revision"] == after["history_revision"]
    finally:
        server.server_close()
