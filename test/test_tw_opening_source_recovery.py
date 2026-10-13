from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
import time
from zoneinfo import ZoneInfo
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.live import quote_provider as quotes, signal_engine as signals
from stockagent.live import opening_price_acquisition as acquisition
from stockagent.live.quote_provider import PriceSnapshot


@pytest.fixture(autouse=True)
def isolated_receipt(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts"))
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_NO_OPEN_RETRY_SECONDS", "0")
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE_KEY", None)
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE", {})
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_REQUESTED", set())
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_DIRTY", False)
    owner = acquisition.OpeningPriceFanIn()
    monkeypatch.setattr(acquisition, "OPENING_FAN_IN", owner)
    monkeypatch.setattr(acquisition, "optional_opening_providers", lambda: {})
    def no_engine(*args, **kwargs):
        raise ConnectionError("isolated test has no engine")
    monkeypatch.setattr(quotes, "fetch_shared_day_trade_stock_snapshots", no_engine)
    yield
    owner.close()


def snapshot(values, *, source="twse_tpex:mis", available=None, exchange_ms=None):
    values = np.asarray(values, dtype=np.float64)
    size = len(values)
    now_ms = int(datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=30).timestamp() * 1000)
    return PriceSnapshot(
        prices=np.full(size, 100.0), open_prices=values,
        available_mask=np.ones(size, dtype=bool) if available is None else np.asarray(available, dtype=bool),
        source=source, requested_count=size,
        available_count=size if available is None else int(np.count_nonzero(available)),
        timestamps_ms=np.full(size, now_ms, dtype=np.int64),
        exchange_timestamps_ms=np.full(size, now_ms if exchange_ms is None else exchange_ms, dtype=np.int64) if source.startswith("shioaji:") else None,
    )


def price_source(symbols, tmp_path):
    return signals._price_snapshot(
        source="tw", symbols=symbols, fallback_prices=np.full(len(symbols), 99.0),
        parquet_root=tmp_path, prices_csv=None, yahoo_chunk_size=80,
        require_official_tw_session_open=True,
    )


@pytest.mark.parametrize("size,first_opens", [(2302, 48), (2294, 42)])
def test_initial_race_requests_full_universe_from_both_sources_and_shares_success(monkeypatch, tmp_path, size, first_opens):
    symbols = [f"S{idx}" for idx in range(size)]
    broker_calls, mis_calls = [], []

    def broker(requested, fallback, **kwargs):
        broker_calls.append(requested)
        opens = np.full(size, np.nan)
        opens[:first_opens] = 100.0
        return snapshot(opens, source="shioaji:stock_snapshot")

    def mis(requested, fallback, **kwargs):
        mis_calls.append(requested)
        values = np.full(len(requested), 101.0)
        values[:first_opens] = 100.0
        return snapshot(values)

    monkeypatch.setattr(signals, "fetch_shared_day_trade_stock_snapshots", broker)
    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", mis)
    first = price_source(symbols, tmp_path)
    second = price_source(list(reversed(symbols)), tmp_path)
    assert broker_calls == [symbols]
    assert mis_calls == [symbols]
    assert first.available_count == size
    assert quotes.tw_session_open_observed_mask(first).sum() == size
    np.testing.assert_array_equal(first.open_prices[:first_opens], 100.0)
    np.testing.assert_array_equal(second.open_prices, first.open_prices[::-1])
    status = quotes.tw_mis_opening_receipt_status(parquet_root=tmp_path, session_date=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    assert status["complete"] and status["valid_open_count"] == size


@pytest.mark.parametrize("active,opens", [(2302, 48), (2294, 42)])
def test_transport_success_never_passes_opening_input_gate(active, opens):
    with pytest.raises(RuntimeError, match="model input coverage is not ready"):
        signals._require_day_trade_opening_coverage(
            live_session=True, active=active, opens=opens, responses=active,
            required=int(np.ceil(active * 0.9)), model_observation="session_open",
        )


def test_latest_quote_observation_keeps_its_own_input_contract():
    signals._require_day_trade_opening_coverage(
        live_session=True, active=10, opens=0, responses=10, required=9,
        model_observation="latest_quote",
    )


def test_explicit_no_print_resolves_input_without_becoming_a_price():
    values = snapshot([100.0, np.nan, np.nan])
    stamp = int(datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=0).timestamp() * 1000)
    values.no_open_print_timestamps_ms = np.asarray([stamp, stamp, 0])
    np.testing.assert_array_equal(quotes.tw_session_no_open_print_mask(values), [False, True, False])
    signals._require_day_trade_opening_coverage(
        live_session=True, active=3, opens=1, no_prints=1, responses=3,
        required=2, model_observation="session_open",
    )
    assert np.isnan(values.open_prices[1])
    with pytest.raises(RuntimeError, match="model input coverage is not ready"):
        signals._require_day_trade_opening_coverage(
            live_session=True, active=3, opens=1, no_prints=0, responses=3,
            required=2, model_observation="session_open",
        )


@pytest.mark.parametrize("hour,days", [(8, 0), (9, -1)])
def test_preopen_or_yesterday_no_print_cannot_resolve_input(hour, days):
    values = snapshot([np.nan])
    at = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=hour, minute=0) + timedelta(days=days)
    values.no_open_print_timestamps_ms = np.asarray([int(at.timestamp() * 1000)])
    assert not quotes.tw_session_no_open_print_mask(values).any()


def test_no_print_proof_round_trips_broker_payload():
    values = snapshot([np.nan])
    values.no_open_print_timestamps_ms = np.asarray([123456789], dtype=np.int64)
    restored = quotes._price_snapshot_from_payload(quotes._price_snapshot_payload(values))
    np.testing.assert_array_equal(restored.no_open_print_timestamps_ms, [123456789])


def test_ready_input_cache_does_not_wait_for_unrelated_unknown_name(monkeypatch, tmp_path):
    symbols = [f"S{i}" for i in range(10)]
    values = snapshot([100.0] * 9 + [np.nan], available=[True] * 9 + [False])
    quotes.seed_tw_opening_snapshot_cache(symbols, values, parquet_root=tmp_path)
    monkeypatch.setattr(signals, "fetch_shared_day_trade_stock_snapshots", lambda *a, **k: pytest.fail("ready models must not wait on a full-universe broker retry"))
    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", lambda *a, **k: pytest.fail("remaining source work belongs to the independent repair owner"))
    result = price_source(symbols, tmp_path)
    assert quotes.tw_session_open_observed_mask(result).sum() == 9
    assert np.isnan(result.open_prices[-1])


def test_successful_open_survives_expiry_failure_and_restart(monkeypatch, tmp_path):
    calls = []
    quotes.seed_tw_opening_snapshot_cache(["A", "B"], snapshot([100.0, np.nan]), parquet_root=tmp_path)
    stored, row = quotes._TW_MIS_OPENING_CACHE["A"]
    quotes._TW_MIS_OPENING_CACHE["A"] = (stored - 90000, row)

    def fail(requested, fallback, **kwargs):
        calls.append(requested)
        raise ConnectionError("failed batch")

    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", fail)
    with pytest.raises(ConnectionError):
        quotes.fetch_tw_mis_opening_snapshot(["A", "B"], np.array([99.0, 99.0]), parquet_root=tmp_path, cache_ttl_seconds=0)
    assert calls == [["B"]]
    reused = quotes.fetch_tw_mis_opening_snapshot(["A"], np.array([99.0]), parquet_root=tmp_path, cache_ttl_seconds=0, allow_network=False)
    assert reused.open_prices[0] == 100.0
    monkeypatch.setattr(quotes, "_TW_MIS_OPENING_CACHE_KEY", None)
    quotes._TW_MIS_OPENING_CACHE.clear()

    def success(requested, fallback, **kwargs):
        calls.append(requested)
        return snapshot([102.0])

    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", success)
    restored = quotes.fetch_tw_mis_opening_snapshot(["A", "B"], np.array([99.0, 99.0]), parquet_root=tmp_path)
    assert calls == [["B"], ["B"]]
    np.testing.assert_array_equal(restored.open_prices, [100.0, 102.0])


def test_broker_receipt_time_does_not_validate_previous_session_open(tmp_path):
    yesterday = int((datetime.now(ZoneInfo("Asia/Taipei")) - timedelta(days=1)).timestamp() * 1000)
    stale = snapshot([100.0], source="shioaji:stock_snapshot", exchange_ms=yesterday)
    assert not quotes.tw_session_open_observed_mask(stale).any()
    quotes.seed_tw_opening_snapshot_cache(["A"], stale, parquet_root=tmp_path)
    cached = quotes.fetch_tw_mis_opening_snapshot(["A"], np.array([99.0]), parquet_root=tmp_path, allow_network=False)
    assert cached.available_count == 1
    assert np.isnan(cached.open_prices[0])
    status = quotes.tw_mis_opening_receipt_status(parquet_root=tmp_path, session_date=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    assert not status["ready"] and status["missing_open_count"] == 1


def test_later_no_print_or_conflicting_snapshot_cannot_overwrite_open(tmp_path):
    for values in ([100.0], [np.nan], [101.0]):
        quotes.seed_tw_opening_snapshot_cache(["A"], snapshot(values), parquet_root=tmp_path)
    cached = quotes.fetch_tw_mis_opening_snapshot(["A"], np.array([99.0]), parquet_root=tmp_path, allow_network=False)
    np.testing.assert_array_equal(cached.open_prices, [100.0])


def test_concurrent_models_share_one_native_acquisition(monkeypatch, tmp_path):
    calls, mis_calls = [], []

    def broker(requested, fallback, **kwargs):
        calls.append(requested)
        time.sleep(0.01)
        return snapshot(np.full(len(requested), 100.0), source="shioaji:stock_snapshot")

    monkeypatch.setattr(signals, "fetch_shared_day_trade_stock_snapshots", broker)
    def mis(requested, fallback, **kwargs):
        mis_calls.append(requested)
        time.sleep(0.02)
        return snapshot(np.full(len(requested), 100.0))
    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", mis)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: price_source(["A", "B"], tmp_path), range(4)))
    assert calls == [["A", "B"]]
    assert mis_calls == [["A", "B"]]
    for result in results:
        np.testing.assert_array_equal(result.open_prices, [100.0, 100.0])


def test_durable_source_repair_retries_a_completely_missing_response(monkeypatch, tmp_path):
    quotes.seed_tw_opening_snapshot_cache(["A", "B"], snapshot([100.0, np.nan], available=[True, False]), parquet_root=tmp_path)
    requests = []

    def fetch(requested, fallback, **kwargs):
        requests.append(requested)
        return snapshot([np.nan] if len(requests) == 1 else [101.0])

    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", fetch)
    session = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    first = quotes.repair_tw_opening_snapshot_receipt(parquet_root=tmp_path, session_date=session)
    assert first["status"] == "waiting_source" and first["missing_open_symbols"] == ["B"]
    cooling = quotes.repair_tw_opening_snapshot_receipt(parquet_root=tmp_path, session_date=session)
    assert cooling["status"] == "waiting_source"
    assert requests == [["B"]]
    # Advance only this source's retry deadline, never freeze the process
    # clock or bypass the repair owner's durable missing-work list.
    owner_session = next(iter(acquisition.OPENING_FAN_IN._sessions.values()))
    owner_session.metrics["official_mis"]["retry_after_monotonic"] = 0.0
    second = quotes.repair_tw_opening_snapshot_receipt(parquet_root=tmp_path, session_date=session)
    assert second["status"] == "complete" and second["valid_open_count"] == 2
    assert requests == [["B"], ["B"]]
    receipt = json.loads(quotes._tw_mis_opening_receipt_path(session).read_text())
    assert receipt["requested_count"] == 2 and receipt["missing_open_symbols"] == []


def test_repair_cannot_backdate_current_quotes(tmp_path):
    with pytest.raises(ValueError, match="current Taipei session"):
        quotes.repair_tw_opening_snapshot_receipt(parquet_root=tmp_path, session_date="2026-10-06")


def test_same_date_premarket_snapshot_is_not_an_open(tmp_path):
    premarket_ms = int(datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=8, minute=59).timestamp() * 1000)
    early = snapshot([100.0], source="shioaji:stock_snapshot", exchange_ms=premarket_ms)
    assert not quotes.tw_session_open_observed_mask(early).any()
    quotes.seed_tw_opening_snapshot_cache(["A"], early, parquet_root=tmp_path)
    cached = quotes.fetch_tw_mis_opening_snapshot(["A"], np.array([99.0]), parquet_root=tmp_path, allow_network=False)
    assert np.isnan(cached.open_prices[0])


def test_mis_zero_volume_with_no_open_is_waiting_print_not_transport_failure(monkeypatch, tmp_path):
    no_print = snapshot([np.nan])
    no_print.volumes = np.zeros(1)
    quotes.seed_tw_opening_snapshot_cache(["A"], no_print, parquet_root=tmp_path)
    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", lambda *a, **k: no_print)
    session = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    result = quotes.repair_tw_opening_snapshot_receipt(parquet_root=tmp_path, session_date=session)
    assert result["status"] == "awaiting_first_print"
    assert result["reported_no_open_print_count"] == 1
    assert result["unresolved_source_count"] == 0
    assert result["valid_open_count"] == 0


@pytest.mark.parametrize("raw,expected", [(0, 0.0), ("0", 0.0), ("1,000", 1000.0), (None, None), ("-", None), (-1, None)])
def test_source_quantity_zero_is_not_unknown(raw, expected):
    assert quotes._quantity_or_none(raw) == expected


def test_latest_quote_uses_source_session_not_delivery_clock():
    today = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=30)
    yesterday = today - timedelta(days=1)
    panel = SimpleNamespace(
        dates=np.asarray([np.datetime64(yesterday.replace(tzinfo=None))]),
        features=np.zeros((1, 2, 1), dtype=np.float32), num_symbols=2,
        feature_names=[signals.DAY_TRADE_OPEN_GAP_FEATURE], close_prices=np.full((1, 2), 100.0),
    )
    observed = snapshot([100.0, 100.0])
    observed.prices = np.array([110.0, 120.0])
    observed.timestamp = today.isoformat()
    observed.exchange_timestamps_ms = np.array([int(today.timestamp()*1000), int(yesterday.timestamp()*1000)])
    window, _, _, live, mask = signals._day_trade_live_model_window(
        panel, panel_idx=0, lookback=1, price_snapshot=observed,
        resolved_asof=today.isoformat(), source_timezone="Asia/Taipei", model_observation="latest_quote",
    )
    assert live
    np.testing.assert_array_equal(mask, [True, False])
    assert window[-1, 0, 0] == pytest.approx(np.log(1.1))
    assert window[-1, 1, 0] == 0.0
