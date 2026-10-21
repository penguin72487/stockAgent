from __future__ import annotations

import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import date, datetime, timedelta
import json
import sys
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlparse
from zoneinfo import ZoneInfo

import numpy as np
import pytest
import requests

from stockagent.live import quote_provider
from stockagent.live.quote_provider import (
    PriceSnapshot,
    fetch_tw_mis_last_prices,
    fetch_tw_mis_opening_snapshot,
    fetch_tw_mis_session_snapshot,
    load_symbol_yahoo_map,
    fetch_yahoo_last_prices,
    load_prices_csv,
    seed_tw_opening_snapshot_cache,
)


@pytest.fixture(autouse=True)
def _isolate_opening_quote_receipts(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "opening_receipts"))
    monkeypatch.setattr(quote_provider, "_SHIOAJI_HISTORY_REQUEST_TIMES", deque())


def test_symbol_yahoo_map_derives_suffix_from_official_venue(tmp_path) -> None:
    (tmp_path / "symbols.csv").write_text(
        "code,name,market,security_type,source\n"
        "2330,台積電,twse,stock,official\n"
        "6547,高端疫苗,tpex,stock,official\n",
        encoding="utf-8",
    )

    assert load_symbol_yahoo_map(tmp_path) == {
        "2330": "2330.TW",
        "6547": "6547.TWO",
    }


def test_shared_day_trade_quote_broker_reuses_serving_process_snapshot(
    tmp_path, monkeypatch
) -> None:
    expected = PriceSnapshot(
        prices=np.array([101.0, 202.0]),
        source="shioaji:stock_snapshot",
        timestamp="2026-09-02T09:45:00+08:00",
        available_count=2,
        requested_count=2,
        available_mask=np.array([True, True]),
        bid_prices=np.array([100.5, 201.5]),
        ask_prices=np.array([101.5, 202.5]),
        timestamps_ms=np.array([1_800_000_000_000, 1_800_000_000_001]),
        transport_timing={"snapshot_request_body_ms": 12.5, "snapshot_usage_before_ms": 0.0},
    )
    def snapshots(symbols, fallback_prices, *, cache_ttl_seconds, prefer_opening_stream=False):
        assert prefer_opening_stream
        return expected

    monkeypatch.setattr(quote_provider, "fetch_shioaji_stock_snapshots", snapshots)
    result: list[PriceSnapshot] = []
    errors: list[BaseException] = []

    def request() -> None:
        try:
            result.append(
                quote_provider.fetch_shared_day_trade_stock_snapshots(
                    ["2330", "2317"],
                    np.array([100.0, 200.0]),
                    state_dir=tmp_path,
                    timeout_seconds=2.0,
                    purpose="opening_signal",
                )
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(target=request)
    thread.start()
    requests_dir = quote_provider.day_trade_quote_broker_request_dir(tmp_path)
    deadline = time.monotonic() + 1.0
    while not list(requests_dir.glob("*.json")) and time.monotonic() < deadline:
        time.sleep(0.005)
    receipts = quote_provider.serve_shared_day_trade_quote_requests(
        state_dir=tmp_path,
        max_requests=1,
        allowed_purposes={"opening_signal"},
    )
    thread.join(timeout=2.0)

    assert not errors
    assert not thread.is_alive()
    assert receipts[0]["available_count"] == 2
    assert receipts[0]["purpose"] == "opening_signal"
    assert result[0].source == "shioaji:stock_snapshot+shared_day_trade_engine"
    np.testing.assert_allclose(result[0].prices, [101.0, 202.0])
    np.testing.assert_allclose(result[0].bid_prices, [100.5, 201.5])
    np.testing.assert_array_equal(result[0].available_mask, [True, True])
    timing = result[0].transport_timing
    assert timing is not None
    assert timing["clock"] == "monotonic_durations_and_utc_wall_timestamps"
    assert timing["server_pid"] > 0
    assert timing["server_receiver_thread"] == threading.current_thread().name
    assert timing["server_provider_fetch_ms"] >= 0.0
    assert timing["server_snapshot_serialize_ms"] >= 0.0
    assert timing["client_round_trip_ms"] >= 0.0
    assert timing["client_total_ms"] >= timing["client_round_trip_ms"]
    assert timing["snapshot_request_body_ms"] == 12.5
    assert timing["snapshot_usage_before_ms"] == 0.0


def test_price_snapshot_payload_preserves_exchange_time_and_simtrade() -> None:
    snapshot = PriceSnapshot(
        prices=np.array([101.0, 202.0]),
        source="shioaji:test",
        exchange_timestamps_ms=np.array([1_788_925_800_000, 1_788_925_800_001]),
        simtrade_flags=np.array([1, 0], dtype=np.int8),
    )

    restored = quote_provider._price_snapshot_from_payload(
        quote_provider._price_snapshot_payload(snapshot)
    )

    np.testing.assert_array_equal(
        restored.exchange_timestamps_ms, snapshot.exchange_timestamps_ms
    )
    np.testing.assert_array_equal(restored.simtrade_flags, [1, 0])


def test_shared_quote_broker_prioritizes_opening_request_in_protected_window(
    tmp_path, monkeypatch
) -> None:
    expected = PriceSnapshot(
        prices=np.array([101.0]),
        source="shioaji:fixture",
        available_count=1,
        requested_count=1,
        available_mask=np.array([True]),
    )
    monkeypatch.setattr(
        quote_provider,
        "fetch_shioaji_stock_snapshots",
        lambda symbols, fallback_prices, *, cache_ttl_seconds, prefer_opening_stream=False: expected,
    )
    requests_dir, responses_dir = quote_provider._day_trade_quote_broker_paths(
        tmp_path
    )
    deadline = time.time() + 10.0
    base = {
        "schema_version": 1,
        "requested_at_utc": datetime.now().astimezone().isoformat(),
        "deadline_epoch": deadline,
        "symbols": ["2330"],
        "fallback_prices": [100.0],
        "requester_pid": 1,
    }
    quote_provider._atomic_write_json(
        requests_dir / "a-interactive.json",
        {**base, "request_id": "a-interactive", "purpose": "interactive"},
    )
    quote_provider._atomic_write_json(
        requests_dir / "b-opening.json",
        {**base, "request_id": "b-opening", "purpose": "opening_signal"},
    )

    receipts = quote_provider.serve_shared_day_trade_quote_requests(
        state_dir=tmp_path,
        max_requests=1,
        allowed_purposes={"opening_signal"},
    )

    assert [row["request_id"] for row in receipts] == ["b-opening"]
    assert (requests_dir / "a-interactive.json").exists()
    assert not (requests_dir / "b-opening.json").exists()
    assert (responses_dir / "b-opening.json").exists()


def test_historical_0901_vwap_uses_only_0900_minute_ticks(monkeypatch) -> None:
    timestamps = np.asarray(
        [
            np.datetime64("2026-08-13T09:00:10", "ns").astype(np.int64),
            np.datetime64("2026-08-13T09:00:50", "ns").astype(np.int64),
            np.datetime64("2026-08-13T09:01:00", "ns").astype(np.int64),
        ],
        dtype=np.int64,
    )

    class FakeAPI:
        contracts = SimpleNamespace(get=lambda symbol: object())

        @staticmethod
        def usage():
            return SimpleNamespace(bytes=10, limit_bytes=1_000_000)

        @staticmethod
        def ticks(**_kwargs):
            return SimpleNamespace(
                ts=timestamps,
                close=[100.0, 110.0, 999.0],
                volume=[1.0, 3.0, 100.0],
            )

    @contextmanager
    def fake_query(*_args, **_kwargs):
        yield lambda _result: None

    monkeypatch.setattr(quote_provider, "_shioaji_stock_api", lambda: FakeAPI())
    monkeypatch.setattr(quote_provider, "shioaji_query", fake_query)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setitem(
        sys.modules,
        "shioaji",
        SimpleNamespace(TicksQueryType=SimpleNamespace(RangeTime="RangeTime")),
    )

    rows, receipt = quote_provider.fetch_shioaji_historical_stock_0901_vwaps(
        ["2330"],
        trading_date=date(2026, 8, 13),
        progress_every=0,
    )

    assert rows["2330"]["execution_price_0901"] == 107.5
    assert rows["2330"]["execution_price_0901_method"] == "minute_vwap"
    assert rows["2330"]["tick_count_0901"] == 2
    assert rows["2330"]["tick_volume_units_0901"] == 4.0
    assert rows["2330"]["observed_volume_unit_0901"] == "board_lots"
    assert rows["2330"]["session_open_price_0900"] == 100.0
    assert (
        rows["2330"]["session_open_price_source"]
        == "shioaji:historical_first_trade_session_open"
    )
    assert quote_provider.observed_0901_minute_volume_lots(rows["2330"]) == 4.0
    assert rows["2330"]["quote_at"] == "2026-08-13T09:01:00+08:00"
    assert receipt["resolved_symbols"] == 1
    assert receipt["right_label"] == "09:01:00 Asia/Taipei"


def test_historical_0901_price_uses_kbar_close_when_tick_query_is_empty(
    monkeypatch,
) -> None:
    minute = int(np.datetime64("2026-08-13T09:01:00", "ns").astype(np.int64))

    class FakeAPI:
        contracts = SimpleNamespace(get=lambda symbol: object())

        @staticmethod
        def usage():
            return SimpleNamespace(bytes=10, limit_bytes=1_000_000)

        @staticmethod
        def ticks(**_kwargs):
            return SimpleNamespace(ts=[], close=[], volume=[])

        @staticmethod
        def kbars(**_kwargs):
            return SimpleNamespace(
                ts=[minute],
                Open=[100.5],
                Close=[101.0],
                Low=[100.0],
                High=[102.0],
                Volume=[0.0],
                Amount=[0.0],
            )

    @contextmanager
    def fake_query(*_args, **_kwargs):
        yield lambda _result: None

    monkeypatch.setattr(quote_provider, "_shioaji_stock_api", lambda: FakeAPI())
    monkeypatch.setattr(quote_provider, "shioaji_query", fake_query)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setitem(
        sys.modules,
        "shioaji",
        SimpleNamespace(TicksQueryType=SimpleNamespace(RangeTime="RangeTime")),
    )

    rows, receipt = quote_provider.fetch_shioaji_historical_stock_0901_vwaps(
        ["2330"],
        trading_date=date(2026, 8, 13),
        progress_every=0,
    )

    assert rows["2330"]["execution_price_0901"] == 101.0
    assert rows["2330"]["execution_price_0901_method"] == "minute_close"
    assert rows["2330"]["tick_count_0901"] == 0
    assert rows["2330"]["observed_volume_unit_0901"] == "shares"
    assert rows["2330"]["session_open_price_0900"] is None
    assert rows["2330"]["session_open_price_source"] is None
    assert receipt["kbar_fallback_queries"] == 1
    assert receipt["kbar_fallback_resolved_symbols"] == 1
    assert receipt["source_empty_symbols"] == 0


def test_historical_0901_kbar_with_volume_retains_first_trade_open(
    monkeypatch,
) -> None:
    minute = int(np.datetime64("2026-08-13T09:01:00", "ns").astype(np.int64))

    class FakeAPI:
        contracts = SimpleNamespace(get=lambda symbol: object())

        @staticmethod
        def usage():
            return SimpleNamespace(bytes=10, limit_bytes=1_000_000)

        @staticmethod
        def ticks(**_kwargs):
            return SimpleNamespace(ts=[], close=[], volume=[])

        @staticmethod
        def kbars(**_kwargs):
            return SimpleNamespace(
                ts=[minute],
                Open=[100.5],
                Close=[101.0],
                Low=[100.0],
                High=[102.0],
                Volume=[2_000.0],
                Amount=[202_000.0],
            )

    @contextmanager
    def fake_query(*_args, **_kwargs):
        yield lambda _result: None

    monkeypatch.setattr(quote_provider, "_shioaji_stock_api", lambda: FakeAPI())
    monkeypatch.setattr(quote_provider, "shioaji_query", fake_query)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setitem(
        sys.modules,
        "shioaji",
        SimpleNamespace(TicksQueryType=SimpleNamespace(RangeTime="RangeTime")),
    )

    rows, receipt = quote_provider.fetch_shioaji_historical_stock_0901_vwaps(
        ["2330"],
        trading_date=date(2026, 8, 13),
        progress_every=0,
    )

    assert rows["2330"]["execution_price_0901"] == 101.0
    assert rows["2330"]["session_open_price_0900"] == 100.5
    assert (
        rows["2330"]["session_open_price_source"]
        == "shioaji:historical_kbar_0901_open_first_trade"
    )
    assert receipt["attempted_symbols"] == ["2330"]


def test_current_stock_minute_bars_are_cutoff_bounded_and_drop_zero_volume(
    monkeypatch,
) -> None:
    stamps = [
        int(np.datetime64(value, "ns").astype(np.int64))
        for value in (
            "2026-08-13T09:01:00",
            "2026-08-13T09:02:00",
            "2026-08-13T09:03:00",
        )
    ]

    class FakeAPI:
        contracts = SimpleNamespace(get=lambda symbol: object())

        @staticmethod
        def usage():
            return SimpleNamespace(bytes=10, limit_bytes=1_000_000)

        @staticmethod
        def kbars(**_kwargs):
            return SimpleNamespace(
                ts=stamps,
                Open=[100.0, 101.0, 102.0],
                High=[101.0, 102.0, 103.0],
                Low=[99.0, 100.0, 101.0],
                Close=[100.5, 101.5, 102.5],
                # Shioaji stock KBar deployments may expose regular-board
                # volume in lots. Amount/OHLC are the unit proof: 2 lots here
                # must normalize to 2,000 shares before capacity accounting.
                Volume=[2.0, 0.0, 3.0],
                Amount=[201_000.0, 0.0, 307_500.0],
            )

    @contextmanager
    def fake_query(*_args, **_kwargs):
        yield lambda _result: None

    monkeypatch.setattr(quote_provider, "_shioaji_stock_api", lambda: FakeAPI())
    monkeypatch.setattr(quote_provider, "shioaji_query", fake_query)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_CONTRACTS", {})

    rows, receipt = quote_provider.fetch_shioaji_current_stock_minute_bars(
        ["2330"],
        trading_date=date(2026, 8, 13),
        completed_through=datetime(
            2026, 8, 13, 9, 2, tzinfo=ZoneInfo("Asia/Taipei")
        ),
        progress_every=0,
    )

    assert len(rows) == 1
    assert rows[0]["ts"] == datetime(2026, 8, 13, 9, 1)
    assert rows[0]["volume_shares"] == 2_000.0
    assert receipt["attempted_symbols"] == ["2330"]
    assert receipt["zero_volume_rows_ignored"] == 1


def test_0901_minute_volume_legacy_receipts_keep_source_units() -> None:
    tick = {
        "source": "shioaji:historical_ticks_0900_090059_vwap_right_label_0901",
        "tick_volume_units_0901": 3_174.0,
    }
    kbar = {
        "source": "shioaji:historical_kbar_0901_minute_vwap",
        "tick_volume_units_0901": 3_174_000.0,
    }
    local = {
        "source": "local_minute_parquet_0901_minute_vwap:/some/partition.parquet",
        "tick_volume_units_0901": 3_174_000.0,
    }
    for row in (tick, kbar, local):
        assert quote_provider.observed_0901_minute_volume_lots(row) == 3_174.0
    assert quote_provider.observed_0901_minute_volume_lots(
        {"source": "unknown", "tick_volume_units_0901": 3_174_000.0}
    ) == 0.0


class _FakeResponse:
    def __init__(self, tickers: list[str]) -> None:
        self.tickers = tickers

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        rows = []
        for ticker in self.tickers:
            index = int(ticker.replace("SYM", ""))
            rows.append(
                {
                    "symbol": ticker,
                    "regularMarketPrice": 100.0 + index,
                    "regularMarketTime": 1_800_000_000 + index,
                }
            )
        return {
            "quoteResponse": {
                "result": rows,
            }
        }


class _FakeChartResponse:
    def __init__(self, ticker: str) -> None:
        self.ticker = ticker

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        index = int(self.ticker.replace("SYM", ""))
        return {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "regularMarketPrice": 200.0 + index,
                            "regularMarketTime": 1_900_000_000 + index,
                        },
                        "timestamp": [1_900_000_000 + index],
                        "indicators": {
                            "quote": [
                                {
                                    "close": [200.0 + index],
                                }
                            ]
                        },
                    }
                ]
            }
        }


class _FakeMisResponse:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"msgArray": self.rows}


def test_warm_tw_mis_probes_api_directly_without_browser_dependency(monkeypatch) -> None:
    class HealthyApiResponse(_FakeMisResponse):
        status_code = 200

    requested = []

    class FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}
            self.cookies = requests.cookies.RequestsCookieJar()

        def get(self, url: str, *, timeout: int, params=None, **kwargs):
            assert params is not None, "browser frontend must not delay the actual quote API"
            requested.append(params["ex_ch"])
            return HealthyApiResponse([{"c": "2330", "z": "100.0"}])

    monkeypatch.setattr(quote_provider.requests, "Session", FakeSession)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_AT", 0.0)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_COOKIES", {})

    result = quote_provider.warm_tw_mis_quote_client(force=True)

    assert result["ready"] is True
    assert requested == ["tse_2330.tw"]
    assert result["frontend_http_status"] is None
    assert result["frontend_performed"] is False
    assert result["api_http_status"] == 200
    assert result["api_http_total_ms"] >= 0
    assert result["readiness_contract"] == "quote_api_requested_row_validated_v2"


@pytest.mark.parametrize("rows", [[], [{"c": "0050"}], [None]])
def test_mis_empty_probe_invalidates_previous_readiness_and_closes_session(monkeypatch, rows):
    closed = []

    class Session:
        def __init__(self):
            self.headers = {}
            self.cookies = requests.cookies.RequestsCookieJar()

        def get(self, _url, **_kwargs):
            return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                                   json=lambda: {"msgArray": rows})

        def close(self):
            closed.append(True)

    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_AT", time.monotonic())
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_COOKIES", {"old": "stale"})
    with pytest.raises(RuntimeError, match="no requested probe row"):
        quote_provider.warm_tw_mis_quote_client(force=True)
    assert quote_provider._TW_MIS_BOOTSTRAP_AT == 0.0
    assert quote_provider._TW_MIS_BOOTSTRAP_COOKIES == {}
    assert closed == [True]


def test_optional_mis_warmup_never_holds_up_real_quote_workers(monkeypatch, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    import threading

    probe_started, release_probe = threading.Event(), threading.Event()
    sessions = []

    class Session:
        def __init__(self):
            self.headers = {}
            self.cookies = requests.cookies.RequestsCookieJar()
            self.is_probe = not sessions
            sessions.append(self)

        def get(self, _url, params=None, **_kwargs):
            if self.is_probe:
                probe_started.set()
                assert release_probe.wait(timeout=3)
            return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                json=lambda: {"msgArray": [{"c": "2330", "z": "101", "o": "100"}]})

        def close(self):
            pass

    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    with ThreadPoolExecutor(max_workers=2) as pool:
        probe = pool.submit(quote_provider.warm_tw_mis_quote_client, force=True)
        try:
            assert probe_started.wait(timeout=1)
            work = pool.submit(fetch_tw_mis_last_prices, ["2330"], np.array([np.nan]),
                parquet_root=tmp_path, empty_chunk_retry_attempts=0, on_progress=lambda *_args: None)
            result = work.result(timeout=1)
            assert not release_probe.is_set()
            assert result.open_prices[0] == 100
        finally:
            release_probe.set()
        assert probe.result(timeout=2)["ready"]


def test_mis_validated_probe_hands_connection_to_only_one_worker(monkeypatch, tmp_path, opening_clock):
    sessions, calls, response_closes, session_closes = [], [], [], []
    barrier = threading.Barrier(2)
    guard = threading.Lock()
    stamp = int(opening_clock.timestamp() * 1000)

    class Session:
        def __init__(self):
            self.index = len(sessions)
            sessions.append(self)
            self.headers = {}
            self.cookies = requests.cookies.RequestsCookieJar()
            self.active = False

        def get(self, _url, params, **_kwargs):
            with guard:
                assert not self.active, "requests.Session cannot have concurrent owners"
                self.active = True
                calls.append((self.index, params["ex_ch"]))
            try:
                if params["ex_ch"] != "tse_2330.tw":
                    # Both real workers must be able to reach the transport at
                    # once, but only one may use the warmed Session.
                    barrier.wait(timeout=3)
                else:
                    assert not response_closes
                code = params["ex_ch"].split("_")[1].split(".")[0]
                return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                    json=lambda: {"msgArray": [{"c": code, "z": "101", "o": "100", "tlong": str(stamp)}]},
                    close=lambda: response_closes.append(self.index))
            finally:
                with guard:
                    self.active = False

        def close(self):
            session_closes.append(self.index)

    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_SESSION", None)
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda *_: {"A": "A.TW", "B": "B.TW"})
    warm = quote_provider.warm_tw_mis_quote_client(force=True)
    assert warm["connection_retained"] is True
    assert response_closes == [0], "the probe response must be released before handing off its session"
    result = fetch_tw_mis_last_prices(["A", "B"], np.full(2, np.nan), parquet_root=tmp_path,
        chunk_size=1, max_parallel_requests=2, empty_chunk_retry_attempts=0, on_progress=lambda *_: None)
    assert result.available_count == 2
    assert len(sessions) == 2, "the probe socket must not be thrown away and recreated"
    assert {index for index, _ in calls[1:]} == {0, 1}
    assert {channel for _, channel in calls[1:]} == {"tse_A.tw", "tse_B.tw"}
    assert session_closes == [], "the probe cannot close a successfully transferred connection"
    assert quote_provider._TW_MIS_BOOTSTRAP_SESSION is None
    origins = [row["http_session_origin"] for row in result.transport_timing["mis_requests"]]
    assert sorted(origins) == ["new_worker_connection", "validated_prewarm_exclusive_handoff"]


def test_mis_failed_forced_probe_closes_unclaimed_connection_and_failed_session(monkeypatch):
    sessions, closed = [], []

    class Session:
        def __init__(self):
            self.index = len(sessions)
            sessions.append(self)
            self.headers = {}
            self.cookies = requests.cookies.RequestsCookieJar()

        def get(self, _url, **_kwargs):
            return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                json=lambda: {"msgArray": [{"c": "2330"}] if self.index == 0 else []})

        def close(self):
            closed.append(self.index)

    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_SESSION", None)
    quote_provider.warm_tw_mis_quote_client(force=True)
    assert closed == []
    with pytest.raises(RuntimeError, match="no requested probe row"):
        quote_provider.warm_tw_mis_quote_client(force=True)
    assert closed == [0, 1]
    assert quote_provider._TW_MIS_BOOTSTRAP_SESSION is None
    assert quote_provider._TW_MIS_BOOTSTRAP_AT == 0


@pytest.mark.parametrize("wrong_owner", ["factory", "pid"])
def test_mis_warm_connection_rejects_other_factory_or_process(monkeypatch, tmp_path, wrong_owner):
    closed = []
    class Session:
        def __init__(self):
            self.headers = {}
            self.cookies = requests.cookies.RequestsCookieJar()

        def get(self, _url, **_kwargs):
            return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                json=lambda: {"msgArray": [{"c": "2330", "z": "101", "o": "100"}]})

        def close(self):
            pass

    import os
    foreign = SimpleNamespace(close=lambda: closed.append(True))
    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_SESSION",
        (foreign, object() if wrong_owner == "factory" else Session, os.getpid() + (wrong_owner == "pid")))
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda *_: {"2330": "2330.TW"})
    result = fetch_tw_mis_last_prices(["2330"], np.array([np.nan]), parquet_root=tmp_path,
        empty_chunk_retry_attempts=0, on_progress=lambda *_: None)
    assert result.available_count == 1
    assert closed == [True]
    assert result.transport_timing["mis_requests"][0]["http_session_origin"] == "new_worker_connection"


def test_tw_mis_opening_snapshot_persists_same_session_observation(
    monkeypatch,
    tmp_path,
    opening_clock,
) -> None:
    session_timestamp_ms = int(opening_clock.timestamp() * 1000)

    class FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}
            self.cookies = requests.cookies.RequestsCookieJar()

        def get(self, url: str, *, timeout: int, params=None, **kwargs):
            return _FakeMisResponse(
                [
                    {
                        "c": "2330",
                        "z": "101.0",
                        "o": "100.0",
                        "tlong": str(session_timestamp_ms),
                    }
                ]
            )

    monkeypatch.setenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts"))
    monkeypatch.setattr(quote_provider.requests, "Session", FakeSession)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_AT", time.monotonic())
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_COOKIES", {})
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", None)
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE", {})

    first = quote_provider.fetch_tw_mis_opening_snapshot(
        ["2330"],
        np.asarray([99.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=21600,
    )
    assert first.available_count == 1
    np.testing.assert_allclose(first.open_prices, [100.0])

    class FailingSession:
        def __init__(self) -> None:
            raise AssertionError("persisted opening receipt should avoid HTTP")

    monkeypatch.setattr(quote_provider.requests, "Session", FailingSession)
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", None)
    quote_provider._TW_MIS_OPENING_CACHE.clear()
    second = quote_provider.fetch_tw_mis_opening_snapshot(
        ["2330"],
        np.asarray([99.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=21600,
    )

    assert second.available_count == 1
    np.testing.assert_allclose(second.open_prices, [100.0])
    status = quote_provider.tw_mis_opening_receipt_status(
        parquet_root=tmp_path,
        session_date=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat(),
    )
    assert status["ready"] is True
    assert status["row_count"] == 1


def test_mis_failed_requests_keep_safe_transport_evidence(monkeypatch, tmp_path) -> None:
    class FailedSession:
        def __init__(self):
            self.headers = {}
        def get(self, *args, **kwargs):
            raise ConnectionError("private credential and query must not persist")
    monkeypatch.setattr(quote_provider.requests, "Session", FailedSession)
    snapshot = fetch_tw_mis_last_prices(
        ["2330"], np.array([90.0]), parquet_root=tmp_path,
        empty_chunk_retry_attempts=0, on_progress=lambda *args: None,
    )
    assert snapshot.available_count == 0
    assert snapshot.transport_timing["mis_failed_request_count"] > 0
    assert snapshot.transport_timing["mis_failure_types"] == ["ConnectionError"]
    assert "private credential" not in str(snapshot.transport_timing)
    assert not np.isfinite(snapshot.open_prices).any()


def test_mis_timeout_burst_defers_unsent_scope_instead_of_waiting_every_wave(monkeypatch, tmp_path):
    symbols = [f"{index:04d}" for index in range(2301)]
    monkeypatch.setattr(quote_provider, "_tw_mis_candidates", lambda symbol, _yahoo: [f"tse_{symbol}.tw"])
    class TimeoutSession:
        def __init__(self):
            self.headers = {}
        def get(self, *_args, **_kwargs):
            raise quote_provider.requests.ReadTimeout("private URL must not persist")
        def close(self):
            pass
    monkeypatch.setattr(quote_provider.requests, "Session", TimeoutSession)
    snapshot = fetch_tw_mis_last_prices(symbols, np.full(2301, np.nan), parquet_root=tmp_path,
        max_parallel_requests=4, request_timeout_seconds=4, empty_chunk_retry_attempts=2,
        on_progress=lambda *_args: None)
    timing = snapshot.transport_timing
    assert timing["mis_dispatch_stopped"]
    assert timing["mis_dispatch_stop_reason"] == "consecutive_transport_failures"
    assert timing["mis_request_count"] == 4
    assert timing["mis_deferred_channel_count"] == 2301 - 4 * 80
    assert timing["mis_retry_rounds"] == 0
    assert snapshot.requested_count == 2301 and snapshot.available_count == 0
    assert "private URL" not in str(timing)
    assert all(row["failure_phase"] == "connect_tls_or_response_headers" for row in timing["mis_requests"])


def test_mis_isolated_timeout_restores_parallel_pipeline_on_healthy_peer(monkeypatch, tmp_path):
    import threading
    barrier = threading.Barrier(4)
    failed = threading.Event()
    symbols = [f"{index:04d}" for index in range(12)]
    monkeypatch.setattr(quote_provider, "_tw_mis_candidates", lambda symbol, _yahoo: [f"tse_{symbol}.tw"])

    class Session:
        def __init__(self):
            self.headers = {}

        def get(self, *_args, params, **_kwargs):
            code = params["ex_ch"].split("_")[1].split(".")[0]
            if int(code) < 4:
                barrier.wait(timeout=2)
            if code == "0000":
                failed.set()
                raise quote_provider.requests.ReadTimeout("isolated source failure")
            assert failed.wait(timeout=2)
            return _FakeMisResponse([{"c": code, "z": "101", "o": "100"}])

        def close(self):
            pass

    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    result = fetch_tw_mis_last_prices(symbols, np.full(12, np.nan), parquet_root=tmp_path,
        chunk_size=1, max_parallel_requests=4, empty_chunk_retry_attempts=0, on_progress=lambda *_: None)
    assert result.available_count == 11
    assert result.transport_timing["mis_request_count"] == 12
    assert not result.transport_timing["mis_dispatch_stopped"]
    assert result.transport_timing["mis_failed_request_count"] == 1


def test_mis_timeout_guard_keeps_partial_success_and_reports_remaining_scope(monkeypatch, tmp_path):
    symbols = [f"{index:04d}" for index in range(10)]
    monkeypatch.setattr(quote_provider, "_tw_mis_candidates", lambda symbol, _yahoo: [f"tse_{symbol}.tw"])
    class PartialSession:
        def __init__(self):
            self.headers = {}
        def get(self, *_args, params, **_kwargs):
            if params["ex_ch"] == "tse_0000.tw":
                return _FakeMisResponse([{"c": "0000", "z": "101", "o": "100"}])
            raise quote_provider.requests.ReadTimeout("unavailable source")
        def close(self):
            pass
    monkeypatch.setattr(quote_provider.requests, "Session", PartialSession)
    progress = []
    snapshot = fetch_tw_mis_last_prices(symbols, np.full(10, np.nan), parquet_root=tmp_path,
        chunk_size=1, max_parallel_requests=1, empty_chunk_retry_attempts=0,
        on_progress=lambda names, _snapshot: progress.extend(names))
    assert snapshot.requested_count == 10 and snapshot.available_count == 1
    assert snapshot.open_prices[0] == 100 and progress == ["0000"]
    assert not snapshot.available_mask[1:].any()
    assert snapshot.transport_timing["mis_request_count"] == 5
    assert snapshot.transport_timing["mis_deferred_channel_count"] == 5
    assert snapshot.transport_timing["mis_dispatch_stopped"]


@pytest.mark.parametrize("broken_body", [False, True])
def test_mis_stream_separates_body_failure_and_closes_response(monkeypatch, tmp_path, broken_body):
    from urllib3.exceptions import ReadTimeoutError
    closed = []
    class BodyResponse:
        status_code = 200
        def raise_for_status(self):
            pass
        @property
        def content(self):
            if broken_body:
                raise quote_provider.requests.ConnectionError(ReadTimeoutError(None, "private", "secret"))
            return b'{"msgArray":[{"c":"2330","o":"100","z":"101"}]}'
        def json(self):
            return json.loads(self.content)
        def close(self):
            closed.append(True)
    class BodySession:
        def __init__(self):
            self.headers = {}
        def get(self, *_args, stream, **_kwargs):
            assert stream is True
            return BodyResponse()
        def close(self):
            pass
    monkeypatch.setattr(quote_provider.requests, "Session", BodySession)
    snapshot = fetch_tw_mis_last_prices(["2330"], np.array([np.nan]), parquet_root=tmp_path,
        max_parallel_requests=1, empty_chunk_retry_attempts=0, on_progress=lambda *_args: None)
    row = snapshot.transport_timing["mis_requests"][0]
    assert closed == [True]
    assert row["http_headers_ms"] >= 0 and row["worker_queue_ms"] >= 0
    if broken_body:
        assert row["failure_phase"] == "body_read" and row["http_status"] == 200
        assert "ReadTimeoutError" in row["error_chain_types"]
        assert snapshot.available_count == 0 and "secret" not in str(row)
    else:
        assert row["body_read_ms"] >= 0 and row["json_decode_ms"] >= 0
        assert row["row_decode_ms"] >= 0
        assert row["request_to_decoded_rows_ms"] >= row["row_decode_ms"]
        assert row["body_bytes"] == len(BodyResponse().content)
        assert snapshot.available_count == 1 and snapshot.open_prices[0] == 100
        stages = snapshot.transport_timing["mis_progress_spans"]
        assert len(stages) == snapshot.transport_timing["mis_progress_batch_count"] == 1
        assert stages[0]["row_count"] == stages[0]["cohort_count"] == 1
        assert stages[0]["snapshot_assembly_ms"] >= 0 and stages[0]["callback_ms"] >= 0


def test_fetch_tw_mis_retries_empty_chunks_without_using_last_as_open(monkeypatch, tmp_path) -> None:
    attempts: dict[str, int] = {}

    class FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def get(self, url: str, *, timeout: int, params=None, **kwargs):
            if params is None:
                return _FakeMisResponse([])
            ex_ch = str(params["ex_ch"])
            attempts[ex_ch] = attempts.get(ex_ch, 0) + 1
            if attempts[ex_ch] == 1:
                raise RuntimeError("temporary MIS disconnect")
            code = ex_ch.split("_", 1)[1].split(".", 1)[0]
            return _FakeMisResponse(
                [
                    {
                        "c": code,
                        "z": "105.0",
                        "o": "101.0",
                        "h": "106.0",
                        "l": "100.0",
                        "v": "1234",
                        "tlong": "1800000000000",
                    }
                ]
            )

    monkeypatch.setenv("STOCKAGENT_TW_MIS_PARALLEL_REQUESTS", "1")
    monkeypatch.setenv("STOCKAGENT_TW_MIS_RETRY_ATTEMPTS", "1")
    monkeypatch.setenv("STOCKAGENT_TW_MIS_RETRY_DELAY_SECONDS", "0")
    monkeypatch.setattr("stockagent.live.quote_provider.requests.Session", FakeSession)

    snapshot = fetch_tw_mis_last_prices(
        ["2330", "2317"],
        np.array([90.0, 80.0]),
        parquet_root=tmp_path,
        chunk_size=2,
    )

    assert snapshot.available_count == 2
    np.testing.assert_allclose(snapshot.prices, [105.0, 105.0])
    np.testing.assert_allclose(snapshot.open_prices, [101.0, 101.0])
    chunk_attempts = {
        ex_ch: count for ex_ch, count in attempts.items() if "|" in ex_ch
    }
    assert chunk_attempts
    assert all(count == 2 for count in chunk_attempts.values())


def test_fetch_tw_mis_preserves_official_no_limit_band(monkeypatch, tmp_path) -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def get(self, url: str, *, timeout: int, params=None, **kwargs):
            if params is None:
                return _FakeMisResponse([])
            return _FakeMisResponse(
                [
                    {
                        "c": "00656R",
                        "z": "5.89",
                        "o": "5.84",
                        "u": "9999.9500",
                        "w": None,
                        "y": "5.87",
                        "tlong": "1800000000000",
                    }
                ]
            )

    monkeypatch.setenv("STOCKAGENT_TW_MIS_PARALLEL_REQUESTS", "1")
    monkeypatch.setattr("stockagent.live.quote_provider.requests.Session", FakeSession)

    snapshot = fetch_tw_mis_last_prices(
        ["00656R"],
        np.array([5.87]),
        parquet_root=tmp_path,
        chunk_size=1,
    )

    np.testing.assert_allclose(snapshot.upper_limit_prices, [9999.95])
    np.testing.assert_allclose(snapshot.lower_limit_prices, [0.01])
    np.testing.assert_allclose(snapshot.open_prices, [5.84])


def test_tw_opening_snapshot_is_single_flight_and_shared_across_models(
    monkeypatch, tmp_path, opening_clock
) -> None:
    calls: list[list[str]] = []
    now_ms = int(opening_clock.timestamp() * 1000)

    def fake_fetch(symbols, fallback_prices, **kwargs):
        del kwargs
        calls.append(list(symbols))
        size = len(symbols)
        values = np.asarray(fallback_prices, dtype=np.float64) + 1.0
        return PriceSnapshot(
            prices=values,
            source="twse_tpex:mis",
            available_count=size,
            requested_count=size,
            available_mask=np.ones((size,), dtype=bool),
            open_prices=values.copy(),
            timestamps_ms=np.full((size,), now_ms, dtype=np.int64),
        )

    monkeypatch.setattr(quote_provider, "fetch_tw_mis_last_prices", fake_fetch)
    monkeypatch.setenv(
        "STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts")
    )
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_NO_OPEN_RETRY_SECONDS", "0")
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", None)
    quote_provider._TW_MIS_OPENING_CACHE.clear()

    first = fetch_tw_mis_opening_snapshot(
        ["2330", "2317"],
        np.array([100.0, 200.0]),
        parquet_root=tmp_path,
    )
    second = fetch_tw_mis_opening_snapshot(
        ["2317"],
        np.array([200.0]),
        parquet_root=tmp_path,
    )

    assert calls == [["2330", "2317"]]
    np.testing.assert_allclose(first.open_prices, [101.0, 201.0])
    np.testing.assert_allclose(second.open_prices, [201.0])
    assert second.source.endswith("+cache_hit")


@pytest.mark.parametrize("repeated", [False, True])
@pytest.mark.parametrize("max_clocks", [False, True])
def test_mis_bulk_column_copy_matches_ordered_scalar_values_and_integer_clocks(repeated, max_clocks):
    rng = np.random.default_rng(2301)
    indices = [4, 2, 0, 3, 1] + ([2, 4] if repeated else [])
    rows, receipts = [], []
    for index, destination in enumerate(indices):
        values = rng.uniform(0.01, 200., len(quote_provider._PRICE_SNAPSHOT_ARRAY_FIELDS)).tolist()
        values[index % len(values)] = None
        values[3] = 0. if index % 2 else -0.
        rows.append((destination, np.nextafter(100. + index, np.inf),
            2 ** 53 + 17 + index if index % 2 else None, *values))
        receipts.append(2 ** 53 + 99 + index)

    def arrays():
        return (np.full(6, -0.), tuple(np.full(6, np.nan) for _ in quote_provider._PRICE_SNAPSHOT_ARRAY_FIELDS),
            np.arange(6, dtype=np.int64), np.arange(6, dtype=np.int64))

    actual, expected = arrays(), arrays()
    for row, receipt, destination in zip(rows, receipts, indices):
        price, fields, stamps, market = expected
        price[destination] = row[1]
        stamp = int(row[2] or 0)
        stamps[destination] = max(int(stamps[destination]), receipt) if max_clocks else receipt
        market[destination] = max(int(market[destination]), stamp) if max_clocks else stamp
        for target, value in zip(fields, row[3:]):
            if value is not None:
                target[destination] = value
    quote_provider._assign_tw_mis_rows(rows, receipts, indices, *actual, max_clocks=max_clocks)
    for received, wanted in zip((actual[0], *actual[1], *actual[2:]), (expected[0], *expected[1], *expected[2:])):
        # Check raw bits: preserve signed zero, missingness and >2**53 clocks.
        np.testing.assert_array_equal(received.view(np.uint64), wanted.view(np.uint64))
    before = [value.copy() for value in (actual[0], *actual[1], *actual[2:])]
    quote_provider._assign_tw_mis_rows([], [], [], *actual, max_clocks=max_clocks)
    for received, wanted in zip((actual[0], *actual[1], *actual[2:]), before):
        np.testing.assert_array_equal(received.view(np.uint64), wanted.view(np.uint64))


def test_opening_row_projection_reuses_only_immutable_rows_and_still_checks_ttl(monkeypatch, tmp_path, opening_clock):
    day = opening_clock.date().isoformat()
    stamp = int(opening_clock.timestamp() * 1000)
    rows = {"A": (1., {"available": True, "price": 101., "open_prices": 100.,
                        "timestamp_ms": stamp, "open_timestamp_ms": stamp, "source": "shioaji:fixture"}),
            "B": (18., {"available": True, "price": 99., "timestamp_ms": stamp,
                         "no_open_print_observed_ms": stamp, "source": "twse_tpex:mis"}),
            "C": (1., {"available": True, "price": 99., "timestamp_ms": stamp})}
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", (str(tmp_path.resolve()), day))
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE", rows)
    clock = SimpleNamespace(monotonic=lambda: 20.)
    monkeypatch.setattr(quote_provider, "time", clock)
    original = quote_provider._opening_row_has_open
    validations = []
    monkeypatch.setattr(quote_provider, "_opening_row_has_open", lambda row, date: (
        validations.append(id(row)), original(row, date))[1])

    def read():
        return fetch_tw_mis_opening_snapshot(list("ABCD"), np.full(4, 50.),
            parquet_root=tmp_path, allow_network=False, cache_ttl_seconds=15.)

    result = read()
    assert result.available_mask.tolist() == [True, True, False, False]
    assert result.prices.tolist() == [101., 99., 50., 50.]
    result.prices[0] = result.open_prices[0] = -999.
    assert read().open_prices[0] == 100.
    assert len(validations) == 3, "full-market wakes must not revalidate unchanged rows"
    clock.monotonic = lambda: 36.
    expired = read()
    assert expired.available_mask.tolist() == [True, False, False, False]
    assert expired.no_open_print_timestamps_ms.tolist() == [0, 0, 0, 0]
    rows["A"] = (36., {**rows["A"][1], "price": 102., "bid_prices": 101.5})
    changed = read()
    assert changed.prices[0] == 102. and changed.bid_prices[0] == 101.5
    assert len(validations) == 4


def test_opening_columns_keep_axis_fallback_removal_and_generation_isolated(monkeypatch, tmp_path, opening_clock):
    day = opening_clock.date().isoformat()
    stamp = int(opening_clock.timestamp() * 1000)
    root = str(tmp_path.resolve())
    row = {"available": True, "price": 101., "open_prices": 100.,
           "timestamp_ms": stamp, "open_timestamp_ms": stamp, "source": "shioaji:fixture"}
    rows = {"A": (1., row)}
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", (root, day))
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE", rows)
    monkeypatch.setattr(quote_provider, "time", SimpleNamespace(monotonic=lambda: 2.))
    def read(names, fallback=None):
        return fetch_tw_mis_opening_snapshot(names, np.full(len(names), 50.) if fallback is None else fallback,
            parquet_root=tmp_path, allow_network=False)
    first = read(["A", "missing"])
    np.testing.assert_array_equal(read(["missing", "A"], np.array([60., 70.])).prices, [60., 101.])
    first.open_prices[:] = first.bid_prices[:] = -100.
    first.timestamps_ms[:] = 0
    assert read(["A", "missing"]).open_prices[0] == 100.
    assert read(["A", "missing"]).timestamps_ms[0] == stamp
    del rows["A"]
    removed = read(["A", "missing"], np.array([80., 90.]))
    np.testing.assert_array_equal(removed.prices, [80., 90.])
    assert not removed.available_mask.any() and np.isnan(removed.open_prices).all()
    # Same key, new map object: none of the earlier row/view identities survive.
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE", {"A": (1., {**row, "open_prices": 102.})})
    assert read(["A", "missing"]).open_prices[0] == 102.
    for i in range(12):
        read(["A", f"axis-{i}"])
    assert len(quote_provider._TW_MIS_OPENING_COLUMNS) <= 8


def test_opening_column_block_updates_match_scalar_projection_bits(monkeypatch, opening_clock):
    fields = quote_provider._PRICE_SNAPSHOT_ARRAY_FIELDS
    stamp = int(opening_clock.timestamp() * 1000)
    day = opening_clock.date().isoformat()
    symbols = tuple(f"symbol-{i}" for i in range(37))
    rows = {symbol: (float(i), {"available": bool(i % 2), "price": 100. + i,
        "timestamp_ms": stamp, "open_timestamp_ms": stamp, "source": "shioaji:fixture",
        **{field: (None if (i + j) % 4 == 0 else -0. if (i + j) % 4 == 1 else 3.141592653589793)
           for j, field in enumerate(fields)}}) for i, symbol in enumerate(symbols)}
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE", rows)
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_ROW_VIEWS", {})
    columns = quote_provider._OpeningColumns(len(symbols))
    for revision in range(2):
        if revision:
            rows[symbols[3]] = (99., {**rows[symbols[3]][1], "open_prices": 101., "price": None})
            del rows[symbols[5]]
        columns.refresh(symbols, day)
        expected_fields = np.full_like(columns.fields, np.nan)
        expected_timestamps = np.zeros_like(columns.timestamps)
        expected_prices = np.full_like(columns.prices, np.nan)
        for i, symbol in enumerate(symbols):
            if symbol not in rows:
                assert not columns.present[i]
                continue
            view = quote_provider._opening_row_view(symbol, rows[symbol][1], day)
            expected_fields[:, i] = view[0]
            expected_timestamps[:, i] = view[4:7]
            expected_prices[i] = view[3] if view[3] is not None else np.nan
            assert (columns.immutable[i], columns.available[i], columns.has_price[i]) == (
                view[1], view[2], view[3] is not None)
        for actual, expected in ((columns.fields, expected_fields), (columns.timestamps, expected_timestamps),
                                 (columns.prices, expected_prices)):
            assert actual.tobytes() == expected.tobytes(), "preserve NaNs, signed zero, clocks and row order"


@pytest.mark.parametrize("field", ["open_prices", "volumes", "bid_volumes", "ask_volumes"])
@pytest.mark.parametrize("dtype", [np.float64, np.float32, object, str])
def test_opening_numeric_columns_preserve_scalar_normalization(field, dtype):
    values = np.array([1.1, 0., -0., -1., np.nan, np.inf, -np.inf, 3.141592653589793], dtype=dtype)
    expected = [quote_provider._snapshot_field_or_none(field, value) for value in values]
    assert quote_provider._opening_numeric_column(field, values) == expected
    if dtype is object:
        values = np.array([True, False, "1,234.50", None, "-", "nan", "inf"], dtype=object)
        assert quote_provider._opening_numeric_column(field, values) == [
            quote_provider._snapshot_field_or_none(field, value) for value in values]


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("body,encoding", [
    ('{"name":"台積電","large":123456789012345678901234567890}'.encode(), "utf-8"),
    (b'\xef\xbb\xbf{"open":100}', None),
    ('{"name":"台積電"}'.encode("utf-16"), None),
    (b'{"name":"caf\xe9"}', "iso-8859-1"),
    (b'{"name":"\xff"}', "utf-8"),
    (b'{"name":"\xff"}', None),
    (b'{"open":NaN,"high":Infinity}', "utf-8"),
])
def test_quote_response_decoder_preserves_requests_semantics(monkeypatch, body, encoding, native):
    if not native:
        monkeypatch.setattr(quote_provider.private_json, "_native", None)
    response = requests.Response()
    response._content, response.encoding, response.status_code = body, encoding, 200
    expected = response.json()
    actual = quote_provider._decode_quote_response(response, body)
    assert json.dumps(actual, sort_keys=True, allow_nan=True) == json.dumps(expected, sort_keys=True, allow_nan=True)


@pytest.mark.parametrize("conflicting_channels", [False, True])
def test_mis_only_coalesces_ready_disjoint_rows_preserving_conflicts(monkeypatch, tmp_path, opening_clock, conflicting_channels):
    from concurrent.futures import Future
    class ImmediatePool:
        def submit(self, function, *args):
            future = Future()
            future.set_result(function(*args))
            return future
    class Session:
        def __init__(self):
            self.headers = {}
        def get(self, _url, *, params, **_kwargs):
            channel = params["ex_ch"]
            code = channel.split("_", 1)[1].split(".", 1)[0]
            price = 101. if channel.startswith("otc_") else 100.
            return _FakeMisResponse([{"c": code, "z": str(price), "o": str(price),
                "v": "1", "tlong": str(int(opening_clock.timestamp() * 1000))}])
    monkeypatch.setattr(quote_provider, "_TW_MIS_HTTP_POOL", ImmediatePool())
    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda _: {} if conflicting_channels else {
        "2330": "2330.TW", "2317": "2317.TW"})
    names = ["2330"] if conflicting_channels else ["2330", "2317"]
    progress = []
    result = fetch_tw_mis_last_prices(names, np.zeros(len(names)), parquet_root=tmp_path,
        chunk_size=1, max_parallel_requests=2, empty_chunk_retry_attempts=0,
        on_progress=lambda scope, snapshot: progress.append((scope, snapshot)))
    assert result.transport_timing["mis_request_count"] == 2
    assert result.transport_timing["mis_progress_batch_count"] == (2 if conflicting_channels else 1)
    if conflicting_channels:
        assert sorted(snapshot.open_prices[0] for _, snapshot in progress) == [100., 101.]
        assert all(scope == ["2330"] for scope, _snapshot in progress)
    else:
        assert set(progress[0][0]) == set(names) and progress[0][1].available_count == 2
    assert all((snapshot.timestamps_ms > 0).all() for _scope, snapshot in progress)
    assert all((snapshot.exchange_timestamps_ms == int(opening_clock.timestamp() * 1000)).all()
               for _scope, snapshot in progress)


def test_mis_ready_cohort_never_waits_for_unfinished_peer(monkeypatch, tmp_path, opening_clock):
    delivered = threading.Event()
    class Session:
        def __init__(self):
            self.headers = {}
        def get(self, _url, *, params, **_kwargs):
            code = params["ex_ch"].split("_", 1)[1].split(".", 1)[0]
            if code == "2317":
                assert delivered.wait(2.), "the fast batch was withheld behind an unfinished peer"
            return _FakeMisResponse([{"c": code, "z": "100", "o": "100", "v": "1",
                                      "tlong": str(int(opening_clock.timestamp() * 1000))}])
    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda _: {"2330": "2330.TW", "2317": "2317.TW"})
    batches = []
    def progress(names, _snapshot):
        batches.append(names)
        if names == ["2330"]:
            delivered.set()
    result = fetch_tw_mis_last_prices(["2330", "2317"], np.zeros(2), parquet_root=tmp_path,
        chunk_size=1, max_parallel_requests=2, empty_chunk_retry_attempts=0, on_progress=progress)
    assert result.available_count == 2 and batches == [["2330"], ["2317"]]


def test_mis_refills_http_window_before_synchronous_progress(monkeypatch, tmp_path, opening_clock):
    """Journal/validation work must overlap, not serialize, the next HTTP job."""
    next_http = threading.Event()
    progress_done = threading.Event()
    calls, batches = [], []
    class Session:
        def __init__(self):
            self.headers = {}
        def get(self, _url, *, params, **_kwargs):
            code = params["ex_ch"].split("_", 1)[1].split(".", 1)[0]
            calls.append(code)
            if code == "2317":
                next_http.set()
                assert progress_done.wait(2.), "next job must not delay publishing the ready cohort"
            return _FakeMisResponse([{"c": code, "z": "101", "o": "100", "v": "1",
                                      "tlong": str(int(opening_clock.timestamp() * 1000))}])
    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda _: {"2330": "2330.TW", "2317": "2317.TW"})
    def progress(names, snapshot):
        batches.append(names)
        if names == ["2330"]:
            assert next_http.wait(2.), "synchronous progress left the next HTTP request idle"
            assert snapshot.open_prices.tolist() == [100.]
            progress_done.set()
    result = fetch_tw_mis_last_prices(["2330", "2317"], np.zeros(2), parquet_root=tmp_path,
        chunk_size=1, max_parallel_requests=1, empty_chunk_retry_attempts=0, on_progress=progress)
    assert calls == ["2330", "2317"] and batches == [["2330"], ["2317"]]
    assert result.available_count == 2
    assert result.transport_timing["mis_http_workers"] == 1
    assert result.transport_timing["mis_local_overlap_policy"] == "refill_bounded_http_before_synchronous_progress_v1"


def test_mis_keeps_refilling_while_previous_progress_is_blocked(monkeypatch, tmp_path, opening_clock):
    """A slow receipt must not idle the window after the next HTTP completes."""
    progress_entered = threading.Event()
    third_http = threading.Event()
    calls, batches = [], []

    class Session:
        def __init__(self):
            self.headers = {}

        def get(self, _url, *, params, **_kwargs):
            code = params["ex_ch"].split("_", 1)[1].split(".", 1)[0]
            calls.append(code)
            if code == "2317":
                assert progress_entered.wait(2.), "first completed batch was not published"
            if code == "2454":
                third_http.set()
            return _FakeMisResponse([{"c": code, "z": "101", "o": "100", "v": "1",
                                      "tlong": str(int(opening_clock.timestamp() * 1000))}])

    names = ["2330", "2317", "2454"]
    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda _: {code: code + ".TW" for code in names})

    def progress(scope, snapshot):
        batches.extend(scope)
        if scope == ["2330"]:
            progress_entered.set()
            assert third_http.wait(2.), "completed HTTP left its slot idle behind validation/journal"
        assert snapshot.open_prices.tolist() == [100.] * len(scope)

    result = fetch_tw_mis_last_prices(names, np.zeros(3), parquet_root=tmp_path,
        chunk_size=1, max_parallel_requests=1, empty_chunk_retry_attempts=0, on_progress=progress)
    assert calls == names and sorted(batches) == sorted(names)
    assert result.available_count == 3
    assert result.transport_timing["mis_http_workers"] == 1
    assert result.transport_timing["mis_network_schedule_policy"] == "bounded_window_independent_of_progress_v2"


def test_mis_progress_failure_drains_admitted_http_before_return(monkeypatch, tmp_path, opening_clock):
    second_started, release, second_finished = (threading.Event() for _ in range(3))

    class Session:
        def __init__(self):
            self.headers = {}

        def get(self, _url, *, params, **_kwargs):
            code = params["ex_ch"].split("_", 1)[1].split(".", 1)[0]
            if code == "2330":
                assert second_started.wait(2.)
            else:
                second_started.set()
                assert release.wait(2.), "progress did not release the already admitted request"
                second_finished.set()
            return _FakeMisResponse([{"c": code, "z": "101", "o": "100", "v": "1",
                                      "tlong": str(int(opening_clock.timestamp() * 1000))}])

    monkeypatch.setattr(quote_provider.requests, "Session", Session)
    monkeypatch.setattr(quote_provider, "load_symbol_yahoo_map", lambda _: {"2330": "2330.TW", "2317": "2317.TW"})

    def failed_progress(*_args):
        release.set()
        raise OSError("receipt fsync failed")

    with pytest.raises(OSError, match="receipt fsync failed"):
        fetch_tw_mis_last_prices(["2330", "2317"], np.zeros(2), parquet_root=tmp_path,
            chunk_size=1, max_parallel_requests=2, empty_chunk_retry_attempts=0, on_progress=failed_progress)
    assert second_finished.is_set()
    assert not any(thread.name == "tw-mis-window" for thread in threading.enumerate())
    assert not quote_provider._TW_MIS_INFLIGHT


def test_tw_opening_snapshot_does_not_cache_all_empty_preopen_observation(
    monkeypatch, tmp_path
) -> None:
    call_count = 0

    def fake_fetch(symbols, fallback_prices, **kwargs):
        nonlocal call_count
        del kwargs
        call_count += 1
        size = len(symbols)
        return PriceSnapshot(
            prices=np.asarray(fallback_prices, dtype=np.float64),
            source="twse_tpex:mis",
            available_count=0,
            requested_count=size,
            available_mask=np.zeros((size,), dtype=bool),
            open_prices=np.full((size,), np.nan),
            timestamps_ms=np.zeros((size,), dtype=np.int64),
        )

    monkeypatch.setattr(quote_provider, "fetch_tw_mis_last_prices", fake_fetch)
    monkeypatch.setenv(
        "STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts")
    )
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", None)
    quote_provider._TW_MIS_OPENING_CACHE.clear()

    for _ in range(2):
        result = fetch_tw_mis_opening_snapshot(
            ["2330"],
            np.array([100.0]),
            parquet_root=tmp_path,
        )
        assert result.available_count == 0

    assert call_count == 2


def test_tw_opening_snapshot_retries_only_symbols_whose_open_is_still_missing(
    monkeypatch, tmp_path, opening_clock
) -> None:
    calls: list[list[str]] = []
    now_ms = int(opening_clock.timestamp() * 1000)

    def fake_fetch(symbols, fallback_prices, **kwargs):
        calls.append(list(symbols))
        assert kwargs["request_timeout_seconds"] == 1.5
        size = len(symbols)
        opens = np.full((size,), np.nan, dtype=np.float64)
        timestamps = np.zeros((size,), dtype=np.int64)
        for idx, symbol in enumerate(symbols):
            if symbol == "2330" or len(calls) > 1:
                opens[idx] = float(fallback_prices[idx]) + 1.0
                timestamps[idx] = now_ms
        return PriceSnapshot(
            prices=np.asarray(fallback_prices, dtype=np.float64) + 1.0,
            source="twse_tpex:mis",
            available_count=size,
            requested_count=size,
            available_mask=np.ones((size,), dtype=bool),
            open_prices=opens,
            timestamps_ms=timestamps,
        )

    monkeypatch.setattr(quote_provider, "fetch_tw_mis_last_prices", fake_fetch)
    monkeypatch.setenv(
        "STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts")
    )
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_NO_OPEN_RETRY_SECONDS", "0")
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", None)
    quote_provider._TW_MIS_OPENING_CACHE.clear()

    first = fetch_tw_mis_opening_snapshot(
        ["2330", "2317"],
        np.array([100.0, 200.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=21600,
    )
    second = fetch_tw_mis_opening_snapshot(
        ["2330", "2317"],
        np.array([100.0, 200.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=21600,
    )

    assert calls == [["2330", "2317"], ["2317"]]
    assert np.isfinite(first.open_prices[0])
    assert np.isnan(first.open_prices[1])
    np.testing.assert_allclose(second.open_prices, [101.0, 201.0])


def test_tw_opening_snapshot_shares_shioaji_fallback_without_refetch(
    monkeypatch, tmp_path, opening_clock
) -> None:
    now_ms = int(opening_clock.timestamp() * 1000)
    monkeypatch.setenv(
        "STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts")
    )
    monkeypatch.setenv("STOCKAGENT_TW_OPENING_NO_OPEN_RETRY_SECONDS", "10")
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", None)
    quote_provider._TW_MIS_OPENING_CACHE.clear()
    snapshot = PriceSnapshot(
        prices=np.array([101.0, 202.0]),
        source="shioaji:stock_snapshot+prepared_limits",
        available_count=2,
        requested_count=2,
        available_mask=np.ones((2,), dtype=bool),
        open_prices=np.array([101.0, np.nan]),
        timestamps_ms=np.full((2,), now_ms, dtype=np.int64),
        exchange_timestamps_ms=np.full((2,), now_ms, dtype=np.int64),
    )

    seeded = seed_tw_opening_snapshot_cache(
        ["2330", "ILLIQUID"],
        snapshot,
        parquet_root=tmp_path,
    )
    monkeypatch.setattr(
        quote_provider,
        "fetch_tw_mis_last_prices",
        lambda *args, **kwargs: pytest.fail("shared opening batch refetched"),
    )
    reused = fetch_tw_mis_opening_snapshot(
        ["2330", "ILLIQUID"],
        np.array([100.0, 200.0]),
        parquet_root=tmp_path,
    )

    assert seeded["accepted_count"] == 2
    assert reused.available_count == 2
    assert reused.source == "shioaji:stock_snapshot+shared_opening_snapshot+cache_hit"
    assert reused.open_prices[0] == pytest.approx(101.0)
    assert np.isnan(reused.open_prices[1])


def test_tw_opening_snapshot_counts_causal_no_open_row_as_source_coverage(
    monkeypatch, tmp_path
) -> None:
    calls = 0
    now_ms = int(time.time() * 1000)

    def fake_fetch(symbols, fallback_prices, **kwargs):
        nonlocal calls
        del kwargs
        calls += 1
        return PriceSnapshot(
            prices=np.asarray(fallback_prices, dtype=np.float64) + 0.5,
            source="twse_tpex:mis",
            available_count=len(symbols),
            requested_count=len(symbols),
            available_mask=np.ones((len(symbols),), dtype=bool),
            open_prices=np.full((len(symbols),), np.nan),
            timestamps_ms=np.full((len(symbols),), now_ms, dtype=np.int64),
        )

    monkeypatch.setattr(quote_provider, "fetch_tw_mis_last_prices", fake_fetch)
    monkeypatch.setenv(
        "STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT", str(tmp_path / "receipts")
    )
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE_KEY", None)
    quote_provider._TW_MIS_OPENING_CACHE.clear()

    first = fetch_tw_mis_opening_snapshot(
        ["ILLIQUID"],
        np.array([100.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=21600,
    )
    second = fetch_tw_mis_opening_snapshot(
        ["ILLIQUID"],
        np.array([100.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=21600,
    )

    assert first.available_count == 1
    assert second.available_count == 1
    assert np.isnan(first.open_prices[0])
    assert np.isnan(second.open_prices[0])
    # One scheduled opening batch freezes source-response coverage briefly so
    # the next model does not repeat a full-market request for a no-print row.
    assert calls == 1


def test_tw_session_snapshot_forces_once_then_reuses_without_touching_opening_cache(
    monkeypatch, tmp_path
) -> None:
    calls: list[list[str]] = []
    now_ms = int(datetime.now(ZoneInfo("Asia/Taipei")).timestamp() * 1000)

    def fake_fetch(symbols, fallback_prices, **kwargs):
        del kwargs
        calls.append(list(symbols))
        bump = float(len(calls))
        size = len(symbols)
        return PriceSnapshot(
            prices=np.asarray(fallback_prices, dtype=np.float64) + bump,
            source="twse_tpex:mis",
            available_count=size,
            requested_count=size,
            available_mask=np.ones((size,), dtype=bool),
            timestamps_ms=np.full((size,), now_ms, dtype=np.int64),
        )

    opening_sentinel = {"KEEP": (time.monotonic(), {"available": True})}
    monkeypatch.setattr(quote_provider, "fetch_tw_mis_last_prices", fake_fetch)
    monkeypatch.setattr(quote_provider, "_TW_MIS_SESSION_CACHE_KEY", None)
    monkeypatch.setattr(quote_provider, "_TW_MIS_SESSION_CACHE", {})
    monkeypatch.setattr(quote_provider, "_TW_MIS_OPENING_CACHE", opening_sentinel)

    first = fetch_tw_mis_session_snapshot(
        ["2330", "2317"],
        np.array([100.0, 200.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=120.0,
        force_refresh=True,
    )
    reused = fetch_tw_mis_session_snapshot(
        ["2330"],
        np.array([100.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=120.0,
    )
    refreshed = fetch_tw_mis_session_snapshot(
        ["2330"],
        np.array([100.0]),
        parquet_root=tmp_path,
        cache_ttl_seconds=120.0,
        force_refresh=True,
    )

    assert calls == [["2330", "2317"], ["2330"]]
    np.testing.assert_allclose(first.prices, [101.0, 201.0])
    np.testing.assert_allclose(reused.prices, [101.0])
    np.testing.assert_allclose(refreshed.prices, [102.0])
    assert reused.source.endswith("+cache_hit")
    assert quote_provider._TW_MIS_OPENING_CACHE is opening_sentinel


def test_tw_session_forced_refresh_never_returns_an_older_cached_quote(
    monkeypatch, tmp_path
) -> None:
    now_ms = int(datetime.now(ZoneInfo("Asia/Taipei")).timestamp() * 1000)
    monkeypatch.setattr(
        quote_provider,
        "_TW_MIS_SESSION_CACHE_KEY",
        datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat(),
    )
    monkeypatch.setattr(
        quote_provider,
        "_TW_MIS_SESSION_CACHE",
        {
            "2330": (
                time.monotonic(),
                {
                    "available": True,
                    "price": 99.0,
                    "timestamp_ms": now_ms,
                    "source": "twse_tpex:mis",
                },
            )
        },
    )
    monkeypatch.setattr(
        quote_provider,
        "fetch_tw_mis_last_prices",
        lambda symbols, fallback_prices, **_kwargs: PriceSnapshot(
            prices=np.asarray(fallback_prices, dtype=np.float64),
            source="twse_tpex:mis",
            available_count=0,
            requested_count=len(symbols),
            available_mask=np.zeros((len(symbols),), dtype=bool),
            timestamps_ms=np.zeros((len(symbols),), dtype=np.int64),
        ),
    )

    result = fetch_tw_mis_session_snapshot(
        ["2330"],
        np.array([88.0]),
        parquet_root=tmp_path,
        force_refresh=True,
    )

    assert result.available_count == 0
    assert result.available_mask.tolist() == [False]
    np.testing.assert_allclose(result.prices, [88.0])


def test_load_prices_csv_preserves_explicit_open_snapshot(tmp_path) -> None:
    path = tmp_path / "session_open.csv"
    path.write_text(
        "symbol,price,open_price\n2330,101.0,101.0\n2317,205.0,205.0\n",
        encoding="utf-8",
    )

    snapshot = load_prices_csv(path, ["2330", "2317", "MISSING"], np.array([90.0, 190.0, 30.0]))

    np.testing.assert_allclose(snapshot.prices, [101.0, 205.0, 30.0])
    np.testing.assert_allclose(snapshot.open_prices[:2], [101.0, 205.0])
    assert np.isnan(snapshot.open_prices[2])
    assert snapshot.available_mask.tolist() == [True, True, False]
    assert snapshot.available_count == 2
    assert snapshot.timestamps_ms is None


def test_load_prices_csv_uses_only_explicit_observation_timestamps(tmp_path) -> None:
    path = tmp_path / 'history.csv'
    path.write_text(
        'symbol,price,effective_timestamp_ms\n'
        '2330,100,1771997100000\n2317,200,1771997400000\n'
        'BAD,10,-1\nFRACTION,20,12.5\n',
    )
    snapshot = load_prices_csv(
        path, ['2317', '2330', 'MISSING', 'BAD', 'FRACTION'], np.ones(5),
    )
    assert snapshot.timestamps_ms.tolist() == [1771997400000, 1771997100000, 0, 0, 0]
    assert snapshot.available_mask.tolist() == [True, True, False, True, True]
    assert snapshot.source.startswith('csv:')


def test_fetch_yahoo_last_prices_runs_requests_in_parallel(monkeypatch, tmp_path) -> None:
    active = 0
    max_active = 0
    request_count = 0
    lock = threading.Lock()

    def fake_get(url: str, timeout: int, **kwargs):
        nonlocal active, max_active, request_count
        with lock:
            active += 1
            request_count += 1
            max_active = max(max_active, active)
        time.sleep(0.03)
        try:
            query = parse_qs(urlparse(url).query)
            tickers = unquote(query["symbols"][0]).split(",")
            return _FakeResponse(tickers)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr("stockagent.live.quote_provider.requests.get", fake_get)

    symbols = [f"SYM{i}" for i in range(8)]
    fallback = np.ones((len(symbols),), dtype=np.float64)
    result = fetch_yahoo_last_prices(
        symbols,
        fallback,
        parquet_root=tmp_path,
        chunk_size=4,
    )

    assert result.available_count == len(symbols)
    assert np.allclose(result.prices, [100.0 + i for i in range(len(symbols))])
    assert max_active > 1
    assert max_active <= 2
    assert request_count == 2
    assert result.source == "yahoo:quote"
    assert result.timestamp is not None


def test_fetch_yahoo_last_prices_falls_back_to_parallel_chart(monkeypatch, tmp_path) -> None:
    active = 0
    max_active = 0
    request_count = 0
    lock = threading.Lock()

    def fake_get(url: str, timeout: int, **kwargs):
        nonlocal active, max_active, request_count
        if "/v7/finance/quote" in url:
            return _FakeResponse([])
        with lock:
            active += 1
            request_count += 1
            max_active = max(max_active, active)
        time.sleep(0.03)
        try:
            ticker = url.split("/chart/", 1)[1].split("?", 1)[0]
            return _FakeChartResponse(ticker)
        finally:
            with lock:
                active -= 1

    monkeypatch.setenv("STOCKAGENT_YAHOO_PARALLEL_REQUESTS", "4")
    monkeypatch.setenv("STOCKAGENT_YAHOO_CHART_FALLBACK_MAX_SYMBOLS", "20")
    monkeypatch.setattr("stockagent.live.quote_provider._yahoo_session_and_crumb", lambda: (None, None))
    monkeypatch.setattr("stockagent.live.quote_provider.requests.get", fake_get)

    symbols = [f"SYM{i}" for i in range(8)]
    fallback = np.ones((len(symbols),), dtype=np.float64)
    result = fetch_yahoo_last_prices(
        symbols,
        fallback,
        parquet_root=tmp_path,
        chunk_size=4,
    )

    assert result.available_count == len(symbols)
    assert np.allclose(result.prices, [200.0 + i for i in range(len(symbols))])
    assert max_active > 1
    assert max_active <= 4
    assert request_count == len(symbols)
    assert result.source == "yahoo:quote+chart"


def test_stock_quote_wakeup_is_pending_only_nontrial_and_coalesced():
    import select
    from stockagent.live.quote_provider import StockQuoteWakeup

    wakeup = StockQuoteWakeup()
    try:
        if wakeup.fd is None:
            pytest.skip("nonblocking pipe unavailable")
        wakeup.arm({"2330"})
        for symbol, flag in (("0050", False), ("2330", True), ("2330", None)):
            wakeup.notify(symbol, simtrade=flag)
        assert select.select([wakeup.fd], [], [], 0)[0] == []
        assert wakeup.consume() is False
        for _ in range(100):
            wakeup.notify("2330", simtrade=False)
        assert select.select([wakeup.fd], [], [], 0)[0] == [wakeup.fd]
        assert wakeup.consume() is True
        assert wakeup.consume() is False
        assert select.select([wakeup.fd], [], [], 0)[0] == []
        wakeup.arm(set())
        wakeup.notify("2330", simtrade=False)
        assert wakeup.consume() is False
    finally:
        wakeup.close()


def test_shared_quote_timeout_survives_frozen_wall_clock(monkeypatch, tmp_path):
    ticks = [0.0]

    def sleep(seconds):
        ticks[0] += seconds

    # Replace only this module's clock, not the stdlib clock used by pytest or
    # filesystem mtimes. This models a stalled/corrected WSL wall clock.
    monkeypatch.setattr(quote_provider, "time", SimpleNamespace(
        time=lambda: 1234.0, monotonic=lambda: ticks[0],
        perf_counter=lambda: ticks[0], sleep=sleep,
    ))
    with pytest.raises(TimeoutError):
        quote_provider.fetch_shared_day_trade_stock_snapshots(
            ["2330"], np.array([100.0]), state_dir=tmp_path,
            timeout_seconds=0.25,
        )
    assert 0.25 <= ticks[0] < 0.27


def test_native_book_wakeup_preserves_clock_and_ignores_older_events(monkeypatch):
    wakeup = quote_provider.StockQuoteWakeup()
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STOCK_WAKEUP", wakeup)
    monkeypatch.setattr(quote_provider, "_SHIOAJI_STREAM_ROWS", {})
    exchange = datetime(2026, 10, 12, 9, 0, 0, tzinfo=ZoneInfo("Asia/Taipei"))
    receipt = exchange + timedelta(milliseconds=4)
    event = SimpleNamespace(code="2330", datetime=exchange, simtrade=False,
                            bid_price=[100.0], ask_price=[101.0],
                            bid_volume=[10], ask_volume=[20])
    try:
        if wakeup.fd is None:
            pytest.skip("nonblocking pipe unavailable")
        wakeup.arm({"2330"})
        quote_provider._record_stock_stream_event("book", event, received=receipt)
        assert wakeup.consume() is True
        row = quote_provider._SHIOAJI_STREAM_ROWS["2330"]["book"]
        assert row["exchange_at"] == exchange.isoformat()
        assert row["received_at"] == receipt.isoformat()
        event.datetime = exchange - timedelta(milliseconds=1)
        quote_provider._record_stock_stream_event("book", event, received=receipt)
        assert wakeup.consume() is False
        assert quote_provider._SHIOAJI_STREAM_ROWS["2330"]["book"] == row
    finally:
        wakeup.close()
