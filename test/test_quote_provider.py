from __future__ import annotations

import threading
import time
from urllib.parse import parse_qs, unquote, urlparse

import numpy as np

from stockagent.live.quote_provider import (
    fetch_tw_mis_last_prices,
    fetch_yahoo_last_prices,
    load_prices_csv,
)


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
    assert all(count == 2 for count in attempts.values())


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
