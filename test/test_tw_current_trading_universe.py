from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import stat
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


def twse_catalog_content(symbols=("0050",)):
    return json.dumps({"stat": "OK", "fields": ["上市日期", "證券代號", "證券簡稱", "發行人", "標的指數"],
        "data": [["2003.06.30", symbol, "測試ETF", "投信", "指數"] for symbol in symbols]}, ensure_ascii=False).encode()


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
        "twse_etf_catalog": twse_catalog_content(),
        **{"tpex_etf_" + name: catalog_content(()) for name in universe.TPEX_ETF_CATALOGS},
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


def test_official_opening_source_bootstraps_without_model_receipt_or_source_read_lock(setup, monkeypatch):
    import fcntl
    setup.publish()
    setup.clock[0] = setup.clock[0].replace(hour=9, minute=1)
    path = quotes._tw_mis_opening_receipt_path(DAY)
    assert not path.exists()
    calls = []
    def provider(names, values, progress):
        calls.append(list(names))
        return snapshot(names)
    monkeypatch.setattr(acquisition, "configured_opening_providers", lambda *_: {"official_mis": provider})
    owner = acquisition.OpeningPriceFanIn()
    monkeypatch.setattr(acquisition, "OPENING_FAN_IN", owner)
    lock = setup.root.parent.parent / ".locks" / "tw-public-refresh.lock"
    lock.parent.mkdir()
    try:
        with lock.open("a+") as writer:
            fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = quotes.repair_tw_opening_snapshot_receipt(parquet_root=setup.root, session_date=DAY)
        assert result["status"] == "complete" and result["requested_count"] == 3
        assert result["bootstrapped_from_official_universe"]
        assert calls == [["0050", "2330", "6488"]]
        assert owner.wait_idle(timeout_seconds=1)
        receipt = json.loads(path.read_bytes())
        assert receipt["requested_symbols"] == ["0050", "2330", "6488"]
        assert set(receipt["rows"]) == set(receipt["requested_symbols"])
    finally:
        owner.close()


@pytest.mark.parametrize("kind", ["foreign_root", "corrupt", "missing_authority", "preopen"])
def test_official_source_bootstrap_never_overwrites_foreign_corrupt_or_unqualified_receipt(setup, monkeypatch, kind):
    if kind != "missing_authority":
        setup.publish()
    setup.clock[0] = setup.clock[0].replace(hour=8 if kind == "preopen" else 9, minute=59 if kind == "preopen" else 1)
    path = quotes._tw_mis_opening_receipt_path(DAY)
    if kind in {"foreign_root", "corrupt"}:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"{" if kind == "corrupt" else json.dumps({"parquet_root": "foreign-root", "session_date": DAY}).encode())
    before = path.read_bytes() if path.exists() else None
    monkeypatch.setattr(acquisition, "configured_opening_providers", lambda *_: pytest.fail("unqualified scope cannot query"))
    if kind == "missing_authority":
        with pytest.raises(universe.TWTradingUniversePending):
            quotes.repair_tw_opening_snapshot_receipt(parquet_root=setup.root, session_date=DAY)
    else:
        result = quotes.repair_tw_opening_snapshot_receipt(parquet_root=setup.root, session_date=DAY)
        assert result["status"] == "not_started"
    assert (path.read_bytes() if path.exists() else None) == before


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


def test_scope_only_directory_failure_requires_a_new_durable_barrier(setup, monkeypatch):
    setup.publish()
    setup.clock[0] = setup.clock[0].replace(hour=9, minute=1)
    quotes.seed_tw_opening_snapshot_cache(["2330", "0050", "6488"],
        snapshot(["2330", "0050", "6488"]), parquet_root=setup.root, session_date=DAY)
    assert not quotes._TW_MIS_OPENING_DIRTY
    path = quotes._tw_mis_opening_receipt_path(DAY)
    before = path.read_bytes()
    setup.by_url[universe.SOURCES["twse_halts"]] = b"[]"
    setup.clock[0] += timedelta(seconds=1)
    authority = setup.publish(force=True)
    replaced_receipt = []
    original_replace, original_sync = os.replace, os.fsync
    timings = {}

    def replace(source, destination):
        original_replace(source, destination)
        if Path(destination) == path:
            replaced_receipt.append(True)

    def sync(fd):
        if replaced_receipt and stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("scope-only directory barrier failed after rename")
        original_sync(fd)

    with monkeypatch.context() as patcher:
        patcher.setattr(os, "replace", replace)
        patcher.setattr(os, "fsync", sync)
        with pytest.raises(OSError, match="scope-only directory barrier"):
            quotes.persist_tw_opening_snapshot_cache(parquet_root=setup.root,
                session_date=DAY, timings=timings)
    # The new fingerprint is already visible, but visibility is not durability.
    assert json.loads(path.read_bytes())["official_trading_universe"]["fingerprint"] == authority["fingerprint"]
    assert quotes._TW_MIS_OPENING_DIRTY
    assert not timings
    barriers = []

    def retry_sync(fd):
        barriers.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
        original_sync(fd)

    monkeypatch.setattr(os, "fsync", retry_sync)
    quotes.persist_tw_opening_snapshot_cache(parquet_root=setup.root, session_date=DAY, timings=timings)
    assert barriers == ["file", "directory"]
    assert timings["receipt_written"] and not quotes._TW_MIS_OPENING_DIRTY
    assert json.loads(path.read_bytes())["requested_count"] == 4
    assert set(json.loads(path.read_bytes())["rows"]) == {"2330", "0050", "6488"}
    generations = list((path.parent / "scope_generations").glob("*.json"))
    assert len(generations) == 1 and generations[0].read_bytes() == before


def catalog_content(symbols=("006201",), *, day=DAY):
    return json.dumps({"date": day.replace("-", ""),
        "stat": "ok" if symbols else "暫無相關商品",
        "tables": [{"fields": ["證券代號", "ETF簡稱", "上櫃日期"],
                    "totalCount": len(symbols),
                    "data": [[symbol, "測試ETF", "100/01/27"] for symbol in symbols]}]}, ensure_ascii=False).encode()


@pytest.fixture
def daily_setup(setup, monkeypatch):
    from scripts import refresh_tw_trading_universe as daily
    from stockagent.live import market_status
    monkeypatch.setattr(daily, "datetime", universe.datetime)
    monkeypatch.setattr(universe, "_fetch", setup.by_url.__getitem__)
    monkeypatch.setattr(daily, "_fetch_official", lambda url:
        setup.by_url[url] if url in setup.by_url else catalog_content())
    monkeypatch.setattr(market_status, "tw_stock_day_decision", lambda *args, **kwargs:
        SimpleNamespace(status="scheduled_open", reason="verified test calendar", is_session=True))
    setup.by_url[universe.SOURCES["tpex_registry"]] = registry("otc",
        [("6488", "環球晶", "2011/10/18"), ("5285", "界霖", "2010/06/01")],
        [("006201", "元大富櫃50", "2011/01/27")])
    setup.by_url.update({url: catalog_content() for url in daily.TPEX_ETF_CATALOGS.values()})
    return daily, setup


@pytest.mark.parametrize("status", ["closed", "unknown"])
def test_daily_calendar_gate_never_fetches_or_promotes(daily_setup, monkeypatch, status):
    from stockagent.live import market_status
    daily, setup = daily_setup
    monkeypatch.setattr(market_status, "tw_stock_day_decision", lambda *args, **kwargs:
        SimpleNamespace(status=status, reason="test source decision", is_session=False))
    monkeypatch.setattr(universe, "refresh_current_universe", lambda *args, **kwargs: pytest.fail("closed/unknown day fetch"))
    result = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    assert result["status"] == ("closed" if status == "closed" else "waiting_calendar")
    assert not universe.receipt_path(DAY).exists()
    assert result["broker_orders"] == 0 and result["ledger_modified"] is False
    assert daily.publish_update(result).is_file()


@pytest.mark.parametrize("clock", ["08:57:00", "08:58:00", "08:59:59", "09:00:00", "09:09:59"])
def test_boot_catchup_does_not_compete_with_opening(daily_setup, monkeypatch, clock):
    daily, setup = daily_setup
    monkeypatch.setattr(universe, "refresh_current_universe", lambda *args, **kwargs: pytest.fail("protected open fetch"))
    result = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root,
        now=datetime.fromisoformat(DAY + "T" + clock + "+08:00"))
    assert result["status"] == "deferred_protected_opening"
    assert not universe.receipt_path(DAY).exists()


def test_daily_update_reuses_canonical_receipts_and_source_cache(daily_setup, monkeypatch):
    daily, setup = daily_setup
    requests = []
    def fetch(url):
        if url in daily.TPEX_ETF_CATALOGS.values():
            requests.append(url)
        return setup.by_url[url]
    monkeypatch.setattr(daily, "_fetch_official", fetch)
    first = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    before = universe.receipt_path(DAY).read_bytes()
    second = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    assert first["status"] == second["status"] == "ready"
    assert len(requests) == len(daily.TPEX_ETF_CATALOGS)
    assert first["fingerprint"] == second["fingerprint"] == universe.load_current_universe(DAY)["fingerprint"]
    assert universe.receipt_path(DAY).read_bytes() == before
    assert first["catalog_audit"]["catalog_count"] == 1  # Categories overlap.
    setup.clock[0] += timedelta(minutes=16)
    third = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    assert third["status"] == "ready" and len(requests) == 2 * len(daily.TPEX_ETF_CATALOGS)


def test_daily_active_product_policy_corrects_stale_isin_with_full_receipts(daily_setup):
    daily, setup = daily_setup
    setup.by_url[universe.SOURCES["tpex_registry"]] = registry("otc",
        [("6488", "環球晶", "2011/10/18")],
        [("006201", "元大富櫃50", "2011/01/27"), ("00838B", "已終止但ISIN仍保留", "2019/06/13")])
    result = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    assert result["status"] == "ready"
    assert result["catalog_audit"]["status"] == "resolved"
    assert result["catalog_audit"]["only_isin"] == ["00838B"]
    assert result["catalog_audit"]["isin_count"] == 2 and result["catalog_audit"]["catalog_count"] == 1
    assert result["catalog_audit"]["resolved_excluded_symbols"] == ["00838B"]
    assert result["catalog_audit"]["only_catalog"] == []
    assert result["catalog_audit"]["runtime_membership_changed"] is False
    assert "00838B" not in universe.opening_scope([], DAY)[1]["symbols"]
    accepted = universe.load_current_universe(DAY)
    assert accepted["membership_contract"] == universe.MEMBERSHIP_CONTRACT
    assert accepted["excluded_count"] == 1 and result["excluded_count"] == 1
    policy = json.loads(Path(accepted["sources"][universe.MEMBERSHIP_SOURCE]["raw_path"]).read_bytes())
    assert [row["symbol"] for row in policy["excluded_records"]] == ["00838B"]


def test_daily_audit_reuses_accepted_policy_without_second_catalog_fetch_or_parse(daily_setup, monkeypatch):
    daily, setup = daily_setup
    first = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    monkeypatch.setattr(daily, "audit_tpex_etf_catalog", lambda *a, **kw: pytest.fail("redundant ingestion audit"))
    monkeypatch.setattr(daily, "_fetch_official", lambda *a, **kw: pytest.fail("redundant provider request"))
    second = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    assert second["status"] == "ready" and second["fingerprint"] == first["fingerprint"]
    assert second["catalog_audit"]["additional_provider_requests"] == 0


def test_daily_source_failure_keeps_accepted_pointer(daily_setup, monkeypatch):
    daily, setup = daily_setup
    daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    before = universe.receipt_path(DAY).read_bytes()
    setup.clock[0] += timedelta(minutes=16)
    def unavailable(_url):
        raise TimeoutError("unavailable upstream")
    monkeypatch.setattr(daily, "_fetch_official", unavailable)
    result = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    assert result["status"] == "waiting_source"
    assert universe.receipt_path(DAY).read_bytes() == before


def test_catalog_retry_preserves_success_and_never_reports_partial_union(daily_setup, monkeypatch):
    daily, setup = daily_setup
    calls = []
    broken = [True]
    def fetch(url):
        calls.append(url)
        if url == daily.TPEX_ETF_CATALOGS["bond"] and broken[0]:
            raise TimeoutError("test outage")
        return catalog_content()
    records = [{"symbol": "006201", "market": "tpex", "security_type": "etf"}]
    first = daily.audit_tpex_etf_catalog(records, DAY, fetch=fetch)
    assert first["status"] == "waiting_source" and first["catalog_count"] is None
    assert first["only_isin"] is None
    broken[0] = False
    second = daily.audit_tpex_etf_catalog(records, DAY, fetch=fetch)
    assert second["status"] == "matched" and second["catalog_count"] == 1
    assert len(calls) == len(daily.TPEX_ETF_CATALOGS) + 1


@pytest.mark.parametrize("fault", ["date", "schema", "count", "status", "empty_status", "duplicate", "missing_date", "wrong_type", "truncated"])
def test_catalog_parser_rejects_incomplete_or_ambiguous_sources(fault):
    from scripts import refresh_tw_trading_universe as daily
    p = json.loads(catalog_content())
    table = p["tables"][0]
    if fault == "date": p["date"] = "20261012"
    elif fault == "schema": table["fields"] = ["changed"]
    elif fault == "count": table["totalCount"] = 2
    elif fault == "status": p["stat"] = "bad"
    elif fault == "empty_status": table.update(data=[], totalCount=0)
    elif fault == "duplicate": table["data"] *= 2; table["totalCount"] = 2
    elif fault == "missing_date": table["data"][0][2] = ""
    elif fault == "wrong_type": table["data"][0][0] = "2330"
    else: table["data"][0].pop()
    with pytest.raises(ValueError):
        daily.parse_tpex_etf_catalog(json.dumps(p).encode(), DAY)


def test_catalog_accepts_explicit_empty_and_filters_future_listing():
    from scripts import refresh_tw_trading_universe as daily
    assert daily.parse_tpex_etf_catalog(catalog_content(()), DAY) == set()
    p = json.loads(catalog_content())
    p["tables"][0]["data"][0][2] = "115/10/14"
    assert daily.parse_tpex_etf_catalog(json.dumps(p).encode(), DAY) == set()


def test_catalog_cache_tamper_is_refetched_not_trusted(daily_setup):
    daily, _setup = daily_setup
    calls = []
    def fetch(url):
        calls.append(url)
        return catalog_content()
    records = [{"symbol": "006201", "market": "tpex", "security_type": "etf"}]
    daily.audit_tpex_etf_catalog(records, DAY, fetch=fetch)
    path = universe.receipt_path(DAY).parent / "source_state" / DAY / "tpex_etf_bond.json"
    source = json.loads(path.read_bytes())
    Path(source["raw_path"]).write_bytes(b"tampered source")
    assert daily.audit_tpex_etf_catalog(records, DAY, fetch=fetch)["status"] == "matched"
    assert len(calls) == len(daily.TPEX_ETF_CATALOGS) + 1


@pytest.mark.parametrize("status,code", [("ready", 0), ("closed", 0), ("waiting_source", 75),
    ("waiting_calendar", 75), ("degraded_catalog_waiting_source", 75), ("degraded_catalog_conflict", 65)])
def test_daily_cli_distinguishes_transient_failure_from_source_conflict(daily_setup, monkeypatch, status, code):
    daily, _setup = daily_setup
    monkeypatch.setattr(daily.sys, "argv", ["refresh_tw_trading_universe.py", "--scheduled", "--session-date", DAY])
    monkeypatch.setattr(daily, "scheduled_refresh", lambda **kwargs: {"session_date": DAY, "status": status})
    assert daily.main() == code


def test_daily_does_not_backdate_or_refresh_scope_after_open(daily_setup, monkeypatch):
    daily, setup = daily_setup
    first = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    before = universe.receipt_path(DAY).read_bytes()
    setup.clock[0] = setup.clock[0].replace(hour=9, minute=15)
    monkeypatch.setattr(universe, "_fetch", lambda url: pytest.fail("already accepted opening scope refetch"))
    later = daily.scheduled_refresh(session_date=DAY, parquet_root=setup.root)
    assert later["fingerprint"] == first["fingerprint"]
    assert universe.receipt_path(DAY).read_bytes() == before
    with pytest.raises(ValueError, match="backdated"):
        daily.scheduled_refresh(session_date="2026-10-12", parquet_root=setup.root)


def test_daily_universe_timer_is_independent_and_protects_opening():
    root = Path(__file__).resolve().parents[1]
    service = (root / "deploy/systemd/stockagent-tw-trading-universe.service.in").read_text()
    timer = (root / "deploy/systemd/stockagent-tw-trading-universe.timer.in").read_text()
    installer = (root / "scripts/install_tw_public_publication_services.sh").read_text()
    assert "--scheduled" in service and "--force" not in service
    assert "discord-bot.service" not in service and "simulation.service" not in service
    assert "RestartSec=30s" in service and "StartLimitBurst=3" in service
    assert "SuccessExitStatus=75" in service
    assert "RestartPreventExitStatus=65 75" in service
    assert "08:30:00 Asia/Taipei" in timer and "08:55:00 Asia/Taipei" in timer
    assert "09:00:" not in timer and "Persistent=true" in timer and "OnBootSec=90s" in timer
    assert "--universe-only" in installer and "scoped_universe_install=true" in installer


def test_current_catalog_membership_is_generic_for_both_venues(setup):
    setup.by_url[universe.SOURCES["twse_registry"]] = registry("listed",
        [("2330", "台積電", "1994/09/05"), ("2303", "聯電", "1985/07/16")],
        [("0050", "元大台灣50", "2003/06/30"), ("0054", "舊商品", "2003/06/30")])
    setup.by_url[universe.SOURCES["tpex_registry"]] = registry("otc",
        [("6488", "環球晶", "2011/10/18"), ("5285", "界霖", "2010/06/01")],
        [("006201", "富櫃50", "2011/01/27"), ("009999", "另一個不在現行目錄的商品", "2019/01/01")])
    setup.by_url.update({url: catalog_content() for url in universe.TPEX_ETF_CATALOGS.values()})
    accepted = setup.publish()
    policy = json.loads(Path(accepted["sources"][universe.MEMBERSHIP_SOURCE]["raw_path"]).read_bytes())
    assert (accepted["registry_count"], accepted["listed_count"], accepted["excluded_count"], accepted["trading_count"]) == (8, 6, 2, 4)
    assert [r["symbol"] for r in policy["excluded_records"]] == ["0054", "009999"]
    assert all(r["exclusion_reason"] == "not_in_complete_current_official_etf_catalog" for r in policy["excluded_records"])
    assert "006201" in universe.opening_scope([], DAY)[1]["symbols"]
    assert universe.public_current_universe(DAY)["membership_contract"] == universe.MEMBERSHIP_CONTRACT


@pytest.mark.parametrize("fault", ["network", "truncated", "stale_date"])
def test_missing_catalog_never_promotes_or_shrinks_and_retries_only_failed_source(setup, fault):
    accepted = setup.publish()
    before = universe.receipt_path(DAY).read_bytes()
    calls = []
    broken = [True]
    bad_url = universe.SOURCES["tpex_etf_bond"]
    def fetch(url):
        calls.append(url)
        if url == bad_url and broken[0]:
            if fault == "network": raise TimeoutError("bounded outage")
            if fault == "truncated": return b'{"date":'
            return catalog_content((), day="2026-10-12")
        return setup.by_url[url]
    with pytest.raises(universe.TWTradingUniversePending):
        universe.refresh_current_universe(DAY, force=True, fetch=fetch)
    assert universe.receipt_path(DAY).read_bytes() == before
    assert universe.load_current_universe(DAY)["fingerprint"] == accepted["fingerprint"]
    broken[0] = False
    universe.refresh_current_universe(DAY, fetch=fetch)
    # The forced failure refreshed all successful sources; only the invalid or
    # missing catalogue is fetched again, not two multi-MB registry pages.
    assert calls.count(bad_url) == 2
    assert len(calls) == len(universe.SOURCES) + 1


def test_new_catalog_product_forces_registry_retry_without_guessing_its_metadata(setup):
    setup.by_url[universe.SOURCES["twse_etf_catalog"]] = twse_catalog_content(("0050", "0051"))
    with pytest.raises(universe.TWTradingUniversePending):
        setup.publish()
    assert not universe.receipt_path(DAY).exists()
    setup.by_url[universe.SOURCES["twse_registry"]] = registry("listed",
        [("2330", "台積電", "1994/09/05"), ("2303", "聯電", "1985/07/16")],
        [("0050", "元大台灣50", "2003/06/30"), ("0051", "新同步商品", DAY)])
    calls = []
    def fetch(url):
        calls.append(url)
        return setup.by_url[url]
    result = universe.refresh_current_universe(DAY, fetch=fetch)
    assert set(calls) == {universe.SOURCES["twse_registry"], universe.SOURCES["tpex_registry"]}
    assert result["listed_count"] == 6 and "0051" in universe.opening_scope([], DAY)[1]["symbols"]


def test_twse_catalog_preserves_currency_counters_and_effective_listing_dates():
    payload = json.loads(twse_catalog_content())
    payload["data"].append(["2011.09.26(新臺幣)<br>2016.08.08(人民幣)",
        "006205(新臺幣)<br>00625K(人民幣)", "原幣ETF<br>人民幣ETF", "投信", "指數"])
    payload["data"].append(["2026.10.14", "00415A", "明日掛牌", "投信", "指數"])
    # Active ETFs need not track an index. Preserve the provider's NULL;
    # absence of an index name is not missing membership evidence.
    payload["data"].append(["2026.10.13", "00412A", "主動式ETF", "投信", None])
    assert universe.parse_twse_etf_catalog(json.dumps(payload).encode(), DAY) == {"0050", "006205", "00625K", "00412A"}


@pytest.mark.parametrize("fault", ["status", "schema", "empty", "duplicate", "mismatched_counters", "date", "wrong_type", "row_type", "root_type"])
def test_twse_catalog_rejects_bad_or_partial_product_tables(fault):
    payload = json.loads(twse_catalog_content())
    if fault == "status": payload["stat"] = "Error"
    elif fault == "schema": payload["fields"] = ["changed"]
    elif fault == "empty": payload["data"] = []
    elif fault == "duplicate": payload["data"] *= 2
    elif fault == "mismatched_counters": payload["data"][0][1] += "<br>00625K"
    elif fault == "date": payload["data"][0][0] = ""
    elif fault == "wrong_type": payload["data"][0][1] = "2330"
    elif fault == "row_type": payload["data"][0][0] = None
    else: payload = []
    with pytest.raises(ValueError):
        universe.parse_twse_etf_catalog(json.dumps(payload).encode(), DAY)


@pytest.mark.parametrize("fault", ["legacy_policy", "policy_binding", "omitted_stock", "wrong_exclusion_count",
    "source_clock", "policy_raw", "policy_root", "observation_clock", "effective_clock", "historical_claim"])
def test_reader_requires_current_policy_and_replays_membership_from_pinned_sources(setup, fault):
    payload = setup.publish()
    if fault == "legacy_policy": payload.pop("membership_contract")
    elif fault == "policy_binding": payload["sources"][universe.MEMBERSHIP_SOURCE]["contract"] = "old_policy"
    elif fault == "omitted_stock":
        payload["records"] = [r for r in payload["records"] if r["symbol"] != "2330"]
        payload["listed_count"] -= 1
        payload["trading_count"] -= 1
    elif fault == "wrong_exclusion_count": payload["excluded_count"] = 1
    elif fault == "source_clock": payload["sources"]["twse_etf_catalog"]["observed_at_taipei"] = "2026-10-12T08:00:00+08:00"
    elif fault == "observation_clock": payload["observed_at_taipei"] = "2026-10-13T08:20:00+08:00"
    elif fault == "effective_clock": payload["evaluated_at_taipei"] = "2026-10-13T09:20:00+08:00"
    elif fault == "historical_claim": payload["historical_pit_claim"] = True
    elif fault == "policy_root":
        source = payload["sources"][universe.MEMBERSHIP_SOURCE]
        Path(source["raw_path"]).write_bytes(b'[]')
        source.update(bytes=2, sha256=hashlib.sha256(b'[]').hexdigest())
    else: Path(payload["sources"][universe.MEMBERSHIP_SOURCE]["raw_path"]).write_bytes(b'{}')
    payload["fingerprint"] = universe._identity(payload)
    universe.receipt_path(DAY).write_text(json.dumps(payload))
    with pytest.raises(universe.TWTradingUniversePending):
        universe.load_current_universe(DAY)


def test_non_object_receipt_is_pending_not_an_unhandled_attribute_error(setup):
    setup.publish()
    universe.receipt_path(DAY).write_bytes(b'[]')
    with pytest.raises(universe.TWTradingUniversePending):
        universe.load_current_universe(DAY)


def test_membership_contract_is_bound_without_changing_existing_reader_envelope(setup):
    payload = setup.publish()
    envelope = {key: payload[key] for key in ("contract", "session_date", "records", "sources")}
    assert hashlib.sha256(json.dumps(envelope, sort_keys=True, ensure_ascii=False,
        separators=(",", ":")).encode()).hexdigest() == payload["fingerprint"]
    assert payload["sources"][universe.MEMBERSHIP_SOURCE]["contract"] == universe.MEMBERSHIP_CONTRACT


def test_runtime_reader_verifies_projection_without_reparsing_ingestion_html(setup, monkeypatch):
    payload = setup.publish()
    for name in ("parse_registry", "parse_twse_etf_catalog", "parse_tpex_etf_catalog", "reconcile_active_etfs"):
        monkeypatch.setattr(universe, name, lambda *a, **kw: pytest.fail("ingestion on runtime read path"))
    assert universe.load_current_universe(DAY)["fingerprint"] == payload["fingerprint"]
    scope = universe.opening_scope(["2330", "00838B"], DAY)[1]
    assert "2330" in scope["symbols"] and "00838B" not in scope["symbols"]


def test_empty_complete_catalog_cannot_erase_registered_etf_market(daily_setup):
    daily, setup = daily_setup
    setup.by_url.update({url: catalog_content(()) for url in universe.TPEX_ETF_CATALOGS.values()})
    with pytest.raises(universe.TWTradingUniversePending):
        setup.publish()
    assert not universe.receipt_path(DAY).exists()
