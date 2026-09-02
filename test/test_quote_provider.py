from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import date, datetime
import sys
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlparse
from zoneinfo import ZoneInfo

import numpy as np
import requests

from stockagent.live import quote_provider
from stockagent.live.quote_provider import (
    PriceSnapshot,
    fetch_tw_mis_last_prices,
    fetch_tw_mis_opening_snapshot,
    load_symbol_yahoo_map,
    fetch_yahoo_last_prices,
    load_prices_csv,
)


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
    )
    monkeypatch.setattr(
        quote_provider,
        "fetch_shioaji_stock_snapshots",
        lambda symbols, fallback_prices, *, cache_ttl_seconds: expected,
    )
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
    )
    thread.join(timeout=2.0)

    assert not errors
    assert not thread.is_alive()
    assert receipts[0]["available_count"] == 2
    assert result[0].source == "shioaji:stock_snapshot+shared_day_trade_engine"
    np.testing.assert_allclose(result[0].prices, [101.0, 202.0])
    np.testing.assert_allclose(result[0].bid_prices, [100.5, 201.5])
    np.testing.assert_array_equal(result[0].available_mask, [True, True])


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
    assert rows["2330"]["tick_count_0901"] == 2
    assert rows["2330"]["quote_at"] == "2026-08-13T09:01:00+08:00"
    assert receipt["resolved_symbols"] == 1
    assert receipt["right_label"] == "09:01:00 Asia/Taipei"


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


def test_warm_tw_mis_uses_quote_api_when_browser_frontend_fails(monkeypatch) -> None:
    class FailedFrontendResponse:
        status_code = 502

        def raise_for_status(self) -> None:
            raise RuntimeError("browser frontend unavailable")

    class HealthyApiResponse(_FakeMisResponse):
        status_code = 200

    class FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}
            self.cookies = requests.cookies.RequestsCookieJar()

        def get(self, url: str, *, timeout: int, params=None, **kwargs):
            if params is None:
                return FailedFrontendResponse()
            return HealthyApiResponse([{"c": "2330", "z": "100.0"}])

    monkeypatch.setattr(quote_provider.requests, "Session", FakeSession)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_AT", 0.0)
    monkeypatch.setattr(quote_provider, "_TW_MIS_BOOTSTRAP_COOKIES", {})

    result = quote_provider.warm_tw_mis_quote_client(force=True)

    assert result["ready"] is True
    assert result["frontend_http_status"] == 502


def test_tw_mis_opening_snapshot_persists_same_session_observation(
    monkeypatch,
    tmp_path,
) -> None:
    session_timestamp_ms = int(
        datetime.now(ZoneInfo("Asia/Taipei")).timestamp() * 1000
    )

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
    monkeypatch, tmp_path
) -> None:
    calls: list[list[str]] = []
    now_ms = int(time.time() * 1000)

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
    monkeypatch, tmp_path
) -> None:
    calls: list[list[str]] = []
    now_ms = int(time.time() * 1000)

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
    assert calls == 2


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
