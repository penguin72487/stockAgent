from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import hashlib
import json
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.data import tw_current_trading_universe as universe
from stockagent.live import opening_price_acquisition as acquisition
from stockagent.live import quote_provider as quotes, signal_engine as signals


DAY = "2026-10-13"


def registry(market, stocks, etfs=()):
    venue = "上市" if market == "listed" else "上櫃"
    rows = ["<tr><td>有價證券代號及名稱</td></tr>"]
    for section, records in (("股票", stocks), ("ETF", etfs)):
        rows.append(f"<tr><td>{section}</td></tr>")
        for code, name, listed in records:
            cells = [f"{code}　{name}", "TW0000000000", listed, venue, "電子", "ESVUFR", ""]
            rows.append("<tr>" + "".join(f"<td>{v}</td>" for v in cells) + "</tr>")
    return ("<html><body><table>" + "".join(rows) + "</table></body></html>").encode("cp950")


@pytest.fixture
def setup(monkeypatch, tmp_path):
    clock = [datetime.fromisoformat(DAY + "T08:30:00+08:00")]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz or universe.TAIPEI)

    for module in (universe, quotes, acquisition, signals):
        monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setenv("STOCKAGENT_TW_TRADING_UNIVERSE_ROOT", str(tmp_path / "universe"))
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "opens"))
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE_KEY", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE", {})
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_REQUESTED", set())
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_DIRTY", False)
    universe._read_receipt.cache_clear()
    quotes._opening_receipt_scope_fingerprint.cache_clear()
    raw = {
        "twse_registry": registry("listed", [("2330", "台積電", "1994/09/05"), ("2303", "聯電", "1985/07/16")], [("0050", "元大台灣５０", "2003/06/30")]),
        "tpex_registry": registry("otc", [("6488", "環球晶", "2011/10/18"), ("5285", "界霖", "2010/06/01")]),
        "twse_halts": json.dumps([{"Code": "2303", "TradingHaltDate": "20261013", "TradingResumptionDate": "", "TradingHaltTime": "090000"}]).encode(),
        "tpex_status": json.dumps([{"Date": "1151013", "SecuritiesCompanyCode": "5285", "SuspensionOfTrading": "Y"}]).encode(),
        "tpex_halts": b"[]",
        "twse_suspensions": json.dumps({"stat": "ok", "tables": [{"title": "115年10月13日 停止買賣", "fields": ["證券代號", "停止買賣開始日期"], "data": []}]}).encode(),
    }
    by_url = {universe.SOURCES[name]: value for name, value in raw.items()}
    root = tmp_path / "source" / "stocks"
    root.mkdir(parents=True)
    (root.parent / "twse_daily_ohlcv.parquet").touch()
    monkeypatch.setenv("STOCKAGENT_TW_REFERENCE_ROOT", str(root))
    import polars as pl
    reference = root.parent / "tw_share_replacement_reference.parquet"
    pl.DataFrame(schema={"symbol": pl.String, "suspension_date": pl.Date,
                         "resume_date": pl.Date, "historical_halt_evidence": pl.Boolean}).write_parquet(reference)
    manifest = root.parent / "raw" / "manifest.jsonl"
    manifest.parent.mkdir()
    manifest.write_bytes(b'{}\n')
    def receipt(path):
        content = path.read_bytes()
        return {"size": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    (root.parent / "tw_share_replacement_reference.summary.json").write_text(json.dumps({
        "generated_at_utc": "2026-10-13T00:00:00+00:00", "source_download_complete": True,
        "lifecycle_catalog_complete": True, "complete_all_markets": True,
        "coverage_end": DAY, "announced_resumption_query_end": "2026-11-13", "rows": 0,
        "output_receipt": receipt(reference),
        "raw_receipt_manifest": receipt(manifest) | {"relative_path": "raw/manifest.jsonl"}}))
    yield SimpleNamespace(clock=clock, raw=raw, by_url=by_url, root=root,
                          publish=lambda **kw: universe.refresh_current_universe(DAY, fetch=by_url.__getitem__, **kw))
    universe._read_receipt.cache_clear()
    quotes._opening_receipt_scope_fingerprint.cache_clear()


def test_official_membership_not_alive_mask_and_preopen_effective_halts(setup):
    receipt = setup.publish()
    assert (receipt["listed_count"], receipt["halted_count"], receipt["trading_count"]) == (5, 2, 3)
    assert receipt["observed_at_taipei"].startswith(DAY + "T08:30")
    assert receipt["evaluated_at_taipei"].startswith(DAY + "T09:00")
    assert receipt["historical_pit_claim"] is False
    panel = SimpleNamespace(symbols=["2303", "2330", "8888"], alive_mask=np.asarray([[True, False, True]]), close_prices=np.asarray([[50., 1000., 1.]]))
    request = signals._live_quote_request(config=SimpleNamespace(data=SimpleNamespace(parquet_root=str(setup.root))), panel=panel, panel_idx=0, execution_mode="tw_day_trade", observation="session_open", price_source="tw", prices_csv=None, yahoo_chunk_size=80, tw_latest_quote_cache_seconds=0, tw_latest_quote_force_refresh=False)
    np.testing.assert_array_equal(request["request_mask"], [False, True, False])
    assert request["symbols"] == panel.symbols  # Checkpoint/model shape unchanged.
    assert request["tw_trading_universe"]["symbols"] == ["0050", "2330", "6488"]
    assert request["tw_trading_universe"]["official_not_in_model_symbols"] == ["0050", "6488"]
    assert request["tw_trading_universe"]["model_official_trading_count"] == 1


def test_registered_root_never_falls_back_to_model_history(setup):
    with pytest.raises(universe.TWTradingUniversePending):
        universe.opening_scope(["2330"], DAY)
    assert universe.official_scope_required(setup.root)
    assert not universe.official_scope_required(setup.root / "offline" / "stocks")


def test_registry_filters_instruments_future_listings_and_other_venue(setup):
    raw = registry("listed", [("2330", "台積電", "1994/09/05"), ("2881A", "特別股", "2020/01/01"), ("040001", "權證", "2020/01/01"), ("7866", "明日上市", "2026/10/14")], [("0050", "元大台灣５０", "2003/06/30")])
    assert [r["symbol"] for r in universe.parse_registry(raw, "listed", setup.clock[0])] == ["2330", "0050"]
    with pytest.raises(ValueError, match="empty"):
        universe.parse_registry(raw, "otc", setup.clock[0])
    with pytest.raises(ValueError, match="truncated"):
        universe.parse_registry(raw[:raw.rfind(b"</table>")], "listed", setup.clock[0])
    with pytest.raises(ValueError, match="header"):
        universe.parse_registry(b"<html><table><tr><td>error</td></tr></table></html>", "listed", setup.clock[0])
    assert len(universe.parse_registry(raw[:raw.rfind(b"</body>")], "listed", setup.clock[0])) == 2


@pytest.mark.parametrize("at,halted", [("08:59:59", False), ("09:00:00", True), ("09:19:59", True), ("09:20:00", False)])
def test_halt_interval_start_inclusive_end_exclusive(setup, at, halted):
    payloads = {"twse_halts": [{"Code": "2330", "TradingHaltDate": "20261013", "TradingHaltTime": "090000", "TradingResumptionDate": "20261013", "TradingResumptionTime": "092000"}], "tpex_halts": [], "tpex_status": [{"Date": "1151013", "SecuritiesCompanyCode": "6488", "SuspensionOfTrading": "N"}]}
    assert ("2330" in universe.halted_symbols(payloads, datetime.fromisoformat(DAY + "T" + at + "+08:00"))) is halted


def test_no_volume_is_not_a_membership_gate(setup):
    setup.publish()
    mask, scope = universe.opening_scope(["6488", "0050", "2330"], DAY)
    assert mask.all() and scope["official_trading_count"] == 3
    assert "volume" not in scope
    mask[:] = False
    scope["symbols"].clear()
    scope["official_not_in_model_symbols"].append("forged")
    next_mask, next_scope = universe.opening_scope(["6488", "0050", "2330"], DAY)
    assert next_mask.all() and len(next_scope["symbols"]) == 3
    assert "forged" not in next_scope["official_not_in_model_symbols"]


def test_tpex_separate_resume_rows_close_previous_halts_regardless_of_order(setup):
    payloads = {"twse_halts": [], "tpex_status": [{"Date": "1151012", "SecuritiesCompanyCode": "1788", "SuspensionOfTrading": ""}], "tpex_halts": [
        {"SecuritiesCompanyCode": "1788", "DateOfSuspendedTrading": "", "DateOfResumedTrading": "1150622", "TimeOfResumedTrading": "080000"},
        {"SecuritiesCompanyCode": "1788", "DateOfSuspendedTrading": "1150618", "DateOfResumedTrading": "", "TimeOfSuspendedTrading": "080000"},
    ]}
    assert "1788" not in universe.halted_symbols(payloads, setup.clock[0])
    payloads["tpex_halts"].append({"SecuritiesCompanyCode": "1788", "DateOfSuspendedTrading": "1151013", "DateOfResumedTrading": "", "TimeOfSuspendedTrading": "090000"})
    assert "1788" in universe.halted_symbols(payloads, setup.clock[0].replace(hour=9))


def test_explicit_today_resume_can_supersede_yesterdays_status(setup):
    payloads = {"twse_halts": [], "tpex_status": [{"Date": "1151012", "SecuritiesCompanyCode": "6488", "SuspensionOfTrading": "Ｙ"}], "tpex_halts": [{"SecuritiesCompanyCode": "6488", "DateOfSuspendedTrading": "", "DateOfResumedTrading": "1151013", "TimeOfResumedTrading": "090000"}]}
    assert "6488" in universe.halted_symbols(payloads, setup.clock[0])
    assert "6488" not in universe.halted_symbols(payloads, setup.clock[0].replace(hour=9))


def test_current_twse_financial_suspension_is_separate_from_temporary_halts(setup):
    payload = {"stat": "ok", "tables": [{"title": "115年10月13日 停止買賣", "fields": ["證券代號", "停止買賣開始日期"], "data": [["1589", "115年04月07日"]]}, {"notes": ["nonfinancial stops use issuer announcements"]}]}
    assert universe.current_twse_suspensions(payload, setup.clock[0]) == {"1589"}
    payload["stat"] = "invalid"
    with pytest.raises(ValueError, match="status"):
        universe.current_twse_suspensions(payload, setup.clock[0])


def test_canonical_corporate_suspension_is_receipt_verified_and_not_redownloaded(setup):
    import polars as pl
    reference = setup.root.parent / "tw_share_replacement_reference.parquet"
    pl.DataFrame({"symbol": ["2330"], "suspension_date": [setup.clock[0].date()],
                  "resume_date": [(setup.clock[0] + timedelta(days=1)).date()],
                  "historical_halt_evidence": [True]}).write_parquet(reference)
    summary_path = reference.with_suffix(".summary.json")
    summary = json.loads(summary_path.read_text())
    raw = reference.read_bytes()
    summary.update(rows=1, output_receipt={"size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    summary_path.write_text(json.dumps(summary))
    assert setup.publish()["trading_count"] == 2
    reference.write_bytes(b"corrupt canonical reference")
    with pytest.raises(universe.TWTradingUniversePending):
        setup.publish(force=True)
    assert universe.load_current_universe(DAY)["trading_count"] == 2


def test_public_scope_is_current_dated_and_contains_no_local_paths(setup):
    assert universe.public_current_universe(DAY)["status"] == "waiting_source"
    setup.publish()
    public = universe.public_current_universe(DAY)
    assert public["trading_count"] == 3 and public["historical_pit_claim"] is False
    assert public["venue_trading_counts"] == {"twse": 2, "tpex": 1}
    assert "raw_path" not in json.dumps(public) and "sources" not in public


@pytest.mark.parametrize("fault", ["date", "count", "fingerprint", "schema", "duplicate", "raw"])
def test_bad_receipt_is_unknown_not_a_smaller_market(setup, fault):
    payload = setup.publish()
    if fault == "date":
        payload["observed_at_taipei"] = "2026-10-12T08:30:00+08:00"
    elif fault == "count":
        payload["trading_count"] = 2
    elif fault == "schema":
        payload["schema_version"] = 999
    elif fault == "duplicate":
        payload["records"].append(payload["records"][0].copy())
        payload["fingerprint"] = universe._identity(payload)
    elif fault == "raw":
        from pathlib import Path
        Path(payload["sources"]["twse_registry"]["raw_path"]).write_bytes(b"corrupted source")
    else:
        payload["fingerprint"] = "0" * 64
    universe.receipt_path(DAY).write_text(json.dumps(payload))
    with pytest.raises(universe.TWTradingUniversePending):
        universe.load_current_universe(DAY)


def test_current_registry_cannot_be_used_as_historical_pit(setup):
    setup.publish()
    setup.clock[0] += timedelta(days=1)
    with pytest.raises(universe.TWTradingUniversePending):
        universe.load_current_universe(DAY)
    with pytest.raises(ValueError, match="backdated"):
        universe.refresh_current_universe(DAY)


@pytest.mark.parametrize("failure", ["network", "schema"])
def test_failed_refresh_keeps_accepted_pointer_and_retains_raw_evidence(setup, failure):
    setup.publish()
    before = universe.receipt_path(DAY).read_bytes()
    def fetch(url):
        if url == universe.SOURCES["twse_halts"]:
            if failure == "network":
                raise TimeoutError("test timeout")
            return b'{"wrong_schema":true}'
        return setup.by_url[url]
    with pytest.raises(universe.TWTradingUniversePending):
        universe.refresh_current_universe(DAY, force=True, fetch=fetch)
    assert universe.receipt_path(DAY).read_bytes() == before
    status = json.loads((universe.receipt_path(DAY).parent / "refresh_status.json").read_text())
    assert status["status"] == ("waiting_source" if failure == "network" else "invalid_source")
    assert "twse_registry" in status["sources"]
    assert universe.load_current_universe(DAY)["trading_count"] == 3


def test_refresh_singleflight_and_runtime_never_performs_http(setup, monkeypatch):
    calls = []
    def fetch(url):
        calls.append(url)
        return setup.by_url[url]
    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(lambda _: universe.refresh_current_universe(DAY, max_age_seconds=900, fetch=fetch), range(4)))
    assert len(calls) == len(universe.SOURCES) and len({r["fingerprint"] for r in receipts}) == 1
    monkeypatch.setattr(universe, "_fetch", lambda _: pytest.fail("network in hot path"))
    for _ in range(5):
        assert universe.opening_scope(["2330"], DAY)[0].all()
    setup.clock[0] += timedelta(minutes=16)
    universe.refresh_current_universe(DAY, max_age_seconds=900, fetch=fetch)
    assert len(calls) == len(universe.SOURCES) + len(universe.SOURCES) - 2
    assert len(list((universe.receipt_path(DAY).parent / "generations").glob("*.json"))) == 2


def test_failed_aggregate_retries_only_missing_transport_preserving_success_clocks(setup):
    calls = []
    broken = [True]
    def fetch(url):
        calls.append(url)
        if url == universe.SOURCES["twse_suspensions"] and broken[0]:
            raise TimeoutError("isolated temporary outage")
        return setup.by_url[url]
    with pytest.raises(universe.TWTradingUniversePending):
        universe.refresh_current_universe(DAY, fetch=fetch)
    broken[0] = False
    setup.clock[0] += timedelta(seconds=5)
    result = universe.refresh_current_universe(DAY, fetch=fetch)
    assert len(calls) == len(universe.SOURCES) + 1
    assert result["sources"]["twse_registry"]["observed_at_taipei"].endswith("08:30:00+08:00")
    assert result["sources"]["twse_suspensions"]["observed_at_taipei"].endswith("08:30:05+08:00")


def test_invalid_schema_is_refetched_not_cached_until_ttl(setup):
    calls = []
    broken = [True]
    def fetch(url):
        calls.append(url)
        if url == universe.SOURCES["twse_suspensions"] and broken[0]:
            return b'{"stat":"bad"}'
        return setup.by_url[url]
    with pytest.raises(universe.TWTradingUniversePending):
        universe.refresh_current_universe(DAY, fetch=fetch)
    broken[0] = False
    universe.refresh_current_universe(DAY, fetch=fetch)
    assert len(calls) == len(universe.SOURCES) + 1


def snapshot(symbols):
    stamp = int(datetime.fromisoformat(DAY + "T09:01:00+08:00").timestamp() * 1000)
    return quotes.PriceSnapshot(prices=np.full(len(symbols), 100.), open_prices=np.full(len(symbols), 100.), source="twse_tpex:mis", available_mask=np.ones(len(symbols), dtype=bool), available_count=len(symbols), requested_count=len(symbols), timestamps_ms=np.full(len(symbols), stamp, dtype=np.int64))


def test_opening_fan_in_fetches_official_symbols_not_in_model(setup, monkeypatch):
    setup.publish()
    setup.clock[0] = setup.clock[0].replace(hour=9, minute=1)
    calls = []
    def mis(requested, fallback, **kwargs):
        calls.append(list(requested))
        return snapshot(requested)
    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", mis)
    monkeypatch.setattr(signals, "fetch_shared_day_trade_stock_snapshots", lambda symbols, fallback, **kw: snapshot(symbols))
    monkeypatch.setattr(acquisition, "optional_opening_providers", lambda: {})
    owner = acquisition.OpeningPriceFanIn()
    monkeypatch.setattr(acquisition, "OPENING_FAN_IN", owner)
    try:
        _, scope = universe.opening_scope(["2330"], DAY)
        result = signals._price_snapshot(source="tw", symbols=["2330"], fallback_prices=np.asarray([99.]), parquet_root=setup.root, prices_csv=None, yahoo_chunk_size=80, require_official_tw_session_open=True, tw_trading_universe=scope)
        assert result.open_prices.tolist() == [100.]
        assert result.requested_count == 1
        assert result.transport_timing["official_trading_universe"]["official_trading_count"] == 3
        assert calls == [["0050", "2330", "6488"]]
    finally:
        owner.close()


def test_receipt_counts_official_scope_and_preserves_excluded_observations(setup):
    setup.publish()
    setup.clock[0] = setup.clock[0].replace(hour=9, minute=1)
    quotes.seed_tw_opening_snapshot_cache(["2330", "0050", "6488", "8888"], snapshot(["2330", "0050", "6488", "8888"]), parquet_root=setup.root, session_date=DAY)
    path = quotes._tw_mis_opening_receipt_path(DAY)
    payload = json.loads(path.read_text())
    assert payload["requested_symbols"] == ["0050", "2330", "6488"]
    assert payload["valid_open_count"] == 3 and payload["row_count"] == 4
    assert payload["out_of_scope_observed_count"] == 1
    assert "8888" in payload["rows"]
    status = quotes.tw_mis_opening_receipt_status(parquet_root=setup.root, session_date=DAY)
    assert status["valid_open_count"] == 3 and status["complete"]
    before = path.read_bytes()
    quotes.persist_tw_opening_snapshot_cache(parquet_root=setup.root, session_date=DAY)
    assert path.read_bytes() == before
    # Changes to official membership republish scope even without new prices.
    setup.by_url[universe.SOURCES["twse_halts"]] = b"[]"
    setup.clock[0] += timedelta(seconds=1)
    setup.publish(force=True)
    quotes.persist_tw_opening_snapshot_cache(parquet_root=setup.root, session_date=DAY)
    assert json.loads(path.read_text())["requested_count"] == 4
    generations = list((path.parent / "scope_generations").glob("*.json"))
    assert len(generations) == 1 and generations[0].read_bytes() == before
