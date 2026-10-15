"""Native opening inputs, not broker/order execution or a real 09:00 SLA."""
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from datetime import datetime, timedelta
import threading
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from stockagent.live import quote_provider as quotes

TAIPEI = ZoneInfo("Asia/Taipei")


@pytest.fixture
def opening(monkeypatch, tmp_path):
    now = datetime.now(TAIPEI).replace(hour=9, minute=0, second=2, microsecond=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz or TAIPEI)

    monkeypatch.setattr(quotes, "datetime", Clock)
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(quotes, "_SHIOAJI_STOCK_CACHE", {})
    monkeypatch.setattr(quotes, "_SHIOAJI_STREAM_ROWS", {})
    monkeypatch.setattr(quotes, "_SHIOAJI_OPENING_CONDITIONS", set())
    monkeypatch.setattr(quotes, "_SHIOAJI_OPENING_REVISION", 0)
    monkeypatch.setattr(quotes, "_SHIOAJI_HISTORY_REQUEST_TIMES", deque())
    monkeypatch.setattr(quotes, "_load_prepared_tw_price_limits", lambda *_: ({}, None))
    monkeypatch.setenv("STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT", str(tmp_path / "traffic"))
    # The scoped clock changes this module's isinstance boundary too; retain
    # genuine datetime subclass values rather than replacing stdlib globally.
    return Clock.fromisoformat(now.isoformat())


def event(now, code="2330", **overrides):
    values = dict(code=code, datetime=(now - timedelta(seconds=1)).replace(tzinfo=None),
                  open=100., close=101., high=102., low=99., simtrade=False,
                  total_volume=5., volume=1., suspend=False,
                  bid_price=[100.5], ask_price=[101.], bid_volume=[8], ask_volume=[7])
    values.update(overrides)
    return SimpleNamespace(**values)


class API:
    def __init__(self, symbols, now):
        self.now, self.calls, self.pending = now, [], []
        self.contracts = {s: SimpleNamespace(code=s, reference=100., limit_up=110., limit_down=90.) for s in symbols}
        self.defer = False
        self.entered = threading.Event()

    def snapshots(self, contracts, timeout=0, cb=None):
        assert timeout == 0 and cb is not None
        self.calls.append([c.code for c in contracts])
        self.entered.set()
        if self.defer:
            self.pending.append((contracts, cb))
        else:
            cb(self.rows(contracts))
        return []

    def rows(self, contracts):
        return [SimpleNamespace(code=c.code, open=100., close=101., high=102., low=99.,
                                total_volume=5., buy_price=100.5, sell_price=101.,
                                buy_volume=8., sell_volume=7., simtrade=False,
                                ts=int(np.datetime64(self.now.replace(tzinfo=None), "ns").astype(np.int64)))
                for c in contracts]


def acquire(api, symbols, *, prefer=True, progress=None):
    return quotes._fetch_shioaji_stock_snapshots_once(
        symbols, np.full(len(symbols), 90.), api=api, cache_ttl_seconds=0.,
        prefer_opening_stream=prefer, on_progress=progress,
    )


def test_proven_stream_open_is_immediate_and_only_missing_names_are_queried(opening):
    quotes._record_stock_stream_event("quote", event(opening))
    api, progress = API(["2330", "2317"], opening), []
    result = acquire(api, ["2330", "2317"], progress=lambda names, value: progress.append(value))
    assert api.calls == [["2317"]]
    assert progress[0].source == "shioaji:stock_quote_stream"
    assert progress[0].available_mask.tolist() == [True, False]
    assert progress[0].exchange_timestamps_ms[0] == int((opening - timedelta(seconds=1)).timestamp() * 1000)
    assert progress[0].timestamps_ms[0] == int(opening.timestamp() * 1000)
    np.testing.assert_array_equal(result.open_prices, [100., 100.])
    np.testing.assert_array_equal(result.reference_prices, [100., 100.])
    np.testing.assert_array_equal(result.upper_limit_prices, [110., 110.])
    assert result.transport_timing["snapshot_queried_contract_count"] == 1
    assert result.transport_timing["snapshot_stream_open_count"] == 1
    assert result.transport_timing["snapshot_opening_input_policy"] == "proven_native_open_then_snapshot_gap_v1"


def test_all_native_openings_need_no_snapshot_request(opening):
    quotes._record_stock_stream_event("quote", event(opening))
    api = API(["2330"], opening)
    result = acquire(api, ["2330"])
    assert api.calls == []
    assert quotes.tw_session_open_observed_mask(result, session_date=opening.date().isoformat()).all()


def test_unchanged_trade_ticks_do_not_repeatedly_wake_full_market_open_workers(opening):
    quotes._record_stock_stream_event("quote", event(opening, simtrade=True))
    assert quotes._SHIOAJI_OPENING_REVISION == 0
    quotes._record_stock_stream_event("quote", event(opening))
    assert quotes._SHIOAJI_OPENING_REVISION == 1
    for number in range(100):
        quotes._record_stock_stream_event("quote", event(opening, close=100. + number * .01, total_volume=number + 5.))
    assert quotes._SHIOAJI_OPENING_REVISION == 1
    quotes._record_stock_stream_event("quote", event(opening, code="2317"))
    assert quotes._SHIOAJI_OPENING_REVISION == 2


@pytest.mark.parametrize("later", [{"simtrade": True}, {"open": None}, {"total_volume": 0.}])
def test_native_open_survives_later_unproven_rows_before_request(opening, later):
    first = event(opening)
    quotes._record_stock_stream_event("quote", first, received=opening)
    quotes._record_stock_stream_event("quote", event(opening, **later),
                                      received=opening + timedelta(milliseconds=100))
    api = API(["2330"], opening)
    result = acquire(api, ["2330"])
    assert api.calls == []
    assert result.open_prices.tolist() == [100.]
    # Preserved data is not re-stamped as a newly received executable book.
    assert result.timestamps_ms.tolist() == [int(opening.timestamp() * 1000)]
    assert quotes._SHIOAJI_STREAM_ROWS["2330"]["quote"].get("open") == later.get("open", 100.)
    assert quotes._SHIOAJI_OPENING_REVISION == 1


def test_trial_between_valid_updates_does_not_repeat_opening_wakeup(opening):
    quotes._record_stock_stream_event("quote", event(opening))
    quotes._record_stock_stream_event("quote", event(opening, simtrade=True))
    quotes._record_stock_stream_event("quote", event(opening, close=105., total_volume=20.))
    assert quotes._SHIOAJI_OPENING_REVISION == 1
    quotes._record_stock_stream_event("quote", event(opening, open=101.))
    assert quotes._SHIOAJI_OPENING_REVISION == 2
    assert acquire(API(["2330"], opening), ["2330"]).open_prices.tolist() == [101.]


def test_out_of_order_formal_open_is_retained_without_rolling_back_book(opening):
    latest = event(opening, simtrade=True, datetime=opening.replace(tzinfo=None))
    quotes._record_stock_stream_event("quote", latest)
    quotes._record_stock_stream_event("quote", event(opening))
    api = API(["2330"], opening)
    assert acquire(api, ["2330"]).open_prices.tolist() == [100.]
    assert api.calls == []
    stored = quotes._SHIOAJI_STREAM_ROWS["2330"]
    assert stored["quote"]["simtrade"] is True
    assert stored["quote"]["exchange_at"] == opening.isoformat()
    assert stored["opening"]["simtrade"] is False


def test_yesterday_native_open_never_eliminates_today_query(opening):
    quotes._record_stock_stream_event("quote", event(opening - timedelta(days=1)),
                                      received=opening - timedelta(days=1))
    api = API(["2330"], opening)
    acquire(api, ["2330"])
    assert api.calls == [["2330"]]


def test_rotation_and_reconnect_keep_opening_but_remove_old_live_books(opening, monkeypatch):
    quotes._record_stock_stream_event("quote", event(opening))
    api = API(["2317", "2330"], opening)
    api.set_on_quote_stk_v1_callback = lambda cb: None
    api.subscribe = lambda *a, **k: None
    api.unsubscribe = lambda *a, **k: None
    monkeypatch.setattr(quotes, "_shioaji_stock_api", lambda: api)
    monkeypatch.setattr(quotes, "_SHIOAJI_STREAM_API", api)
    monkeypatch.setattr(quotes, "_SHIOAJI_STREAM_SUBSCRIPTIONS", {("2330", "quote")})
    monkeypatch.setattr(quotes, "_SHIOAJI_STREAM_TARGET", ("2330",))
    monkeypatch.setattr(quotes, "_SHIOAJI_STREAM_TARGET_READY", True)
    monkeypatch.setattr(quotes, "_SHIOAJI_STREAM_ROTATION_AT", 0.)
    monkeypatch.setattr(quotes, "_SHIOAJI_STREAM_CURSOR", 0)
    live = quotes.fetch_shioaji_stock_live_quotes(["2317"], trading_date=opening.date())
    assert not live["2317"]["available"]
    assert set(quotes._SHIOAJI_STREAM_ROWS["2330"]) == {"opening"}
    assert quotes._shioaji_stream_opening_rows(["2330"], session_date=opening.date().isoformat())
    monkeypatch.setattr(quotes, "_SHIOAJI_STREAM_API", object())
    live = quotes.fetch_shioaji_stock_live_quotes(["2330"], trading_date=opening.date())
    assert not live["2330"]["available"]
    assert live["2330"]["ask"] is None
    assert acquire(api, ["2330"]).open_prices.tolist() == [100.]
    assert api.calls == []


def test_snapshot_burst_and_history_share_actual_provider_window_not_push(opening, monkeypatch):
    clock, waits = [100.], []
    monkeypatch.setattr(quotes.time, "monotonic", lambda: clock[0])

    def wait(delay):
        waits.append(delay)
        clock[0] += delay

    monkeypatch.setattr(quotes.time, "sleep", wait)
    for _ in range(40):
        quotes._wait_stock_history_request_slot()
    for _ in range(10):
        quotes._wait_stock_quote_query_slot()
    assert not waits
    assert len(quotes._SHIOAJI_HISTORY_REQUEST_TIMES) == 50
    quotes._record_stock_stream_event("quote", event(opening))
    assert len(quotes._SHIOAJI_HISTORY_REQUEST_TIMES) == 50
    quotes._wait_stock_quote_query_slot()
    assert waits == [pytest.approx(10.01)]
    assert len(quotes._SHIOAJI_HISTORY_REQUEST_TIMES) == 1


@pytest.mark.parametrize("overrides", [
    {"simtrade": True}, {"simtrade": None}, {"total_volume": 0.},
    {"total_volume": None}, {"open": 0.}, {"suspend": True},
    {"datetime": "yesterday"},
])
def test_unproven_native_open_never_suppresses_snapshot(opening, overrides):
    if overrides.get("datetime") == "yesterday":
        overrides = {"datetime": (opening - timedelta(days=1)).replace(tzinfo=None)}
    quotes._record_stock_stream_event("quote", event(opening, **overrides))
    api = API(["2330"], opening)
    result = acquire(api, ["2330"])
    assert api.calls == [["2330"]]
    assert result.transport_timing["snapshot_stream_open_count"] == 0


def test_opening_preference_is_opt_in_generic_snapshot_semantics_unchanged(opening):
    quotes._record_stock_stream_event("quote", event(opening, open=105.))
    api = API(["2330"], opening)
    result = acquire(api, ["2330"], prefer=False)
    assert api.calls == [["2330"]]
    assert result.open_prices.tolist() == [100.]
    assert "native_opening_stream" not in result.source
    assert result.transport_timing["snapshot_opening_input_policy"] == "snapshot_query_v1"


def test_native_arrival_wakes_progress_before_slow_snapshot_and_survives_later_trial(opening):
    api, progressed = API(["2330"], opening), threading.Event()
    api.defer = True
    received = []

    def progress(names, snapshot):
        received.append(snapshot)
        if quotes.tw_session_open_observed_mask(snapshot, session_date=opening.date().isoformat()).any():
            progressed.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(acquire, api, ["2330"], progress=progress)
        try:
            assert api.entered.wait(timeout=1)
            # The request can be between submission and condition registration;
            # cache replay plus revision fencing must cover both orders.
            quotes._record_stock_stream_event("quote", event(opening))
            assert progressed.wait(timeout=1)
            assert not future.done()
            quotes._record_stock_stream_event("quote", event(opening, simtrade=True, open=0.))
        finally:
            for contracts, cb in api.pending:
                cb([])
        result = future.result(timeout=1)
    assert received[0].open_prices.tolist() == [100.]
    assert result.open_prices.tolist() == [100.]
    assert result.timestamps_ms.tolist() == [int(opening.timestamp() * 1000)]
    assert not quotes._SHIOAJI_OPENING_CONDITIONS


def test_callback_rows_are_decoded_once_despite_cumulative_progress(opening, monkeypatch):
    symbols = [str(1000 + i) for i in range(1101)]
    api, parsed, samples = API(symbols, opening), [], []
    api.defer = True
    original = quotes._shioaji_snapshot_values

    def decode(row, contract, *, received_ms):
        parsed.append(row.code)
        return original(row, contract, received_ms=received_ms)

    monkeypatch.setattr(quotes, "_shioaji_snapshot_values", decode)
    progressed = threading.Event()
    def progress(names, snapshot):
        samples.append(snapshot)
        progressed.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(acquire, api, symbols, progress=progress)
        assert api.entered.wait(timeout=1)
        # Wait until all nonblocking batches are submitted (the worker must
        # not wait for one response before dispatching another).
        for _ in range(1000):
            if len(api.pending) == 3:
                break
            threading.Event().wait(.001)
        assert len(api.pending) == 3
        # Deliberately deliver the last batch first.
        for index in [2, 0, 1]:
            progressed.clear()
            contracts, cb = api.pending[index]
            cb(api.rows(contracts))
            assert progressed.wait(timeout=2)
        result = future.result(timeout=2)
    assert len(parsed) == len(symbols) == len(set(parsed))
    assert [s.available_count for s in samples] == [101, 601, 1101]
    assert [s.transport_timing["snapshot_raw_response_count"] for s in samples] == [101, 601, 1101]
    assert result.available_count == 1101
    assert result.transport_timing["snapshot_callback_normalized_rows_count"] == 1101
    np.testing.assert_array_equal(result.open_prices, np.full(1101, 100.))


def test_decoded_diagnostics_partition_every_requested_name_and_retain_unknown_flags(opening):
    stamp = int(opening.timestamp() * 1000)
    size = 8
    value = quotes.PriceSnapshot(
        prices=np.full(size, 100.), open_prices=np.array([100., np.nan, 100., 100., 100., np.nan, 100., 100.]),
        source="twse_tpex:mis", available_mask=np.array([True] * 7 + [False]),
        available_count=7, requested_count=size,
        timestamps_ms=np.full(size, stamp, dtype=np.int64),
        exchange_timestamps_ms=np.array([stamp, stamp, 0, stamp - 86400000, stamp, stamp, stamp, stamp]),
        volumes=np.array([1., np.nan, 1., 1., 1., 0., 1., 1.]),
        simtrade_flags=np.array([0, 0, 0, 0, 1, 0, -1, 0], dtype=np.int8),
    )
    summary, rows = quotes.tw_opening_observation_diagnostics(list("ABCDEFGH"), value, session_date=opening.date().isoformat())
    assert summary["valid_open_count"] == 2
    assert summary["observed_no_print_count"] == 1
    assert sum(summary["states"].values()) == size
    assert summary["unknown_trial_flag_count"] == 1
    assert [r["state"] for r in rows] == ["valid_open", "missing_open_price", "missing_market_time",
        "stale_or_preopen_time", "trial_matching", "observed_no_first_print", "valid_open", "no_source_observation"]
    assert rows[6]["simtrade"] is None
    assert rows[3]["market_timestamp_ms"] == stamp - 86400000
