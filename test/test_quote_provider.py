from __future__ import annotations

import threading
import time
from urllib.parse import parse_qs, unquote, urlparse

import numpy as np

from stockagent.live.quote_provider import fetch_yahoo_last_prices


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
