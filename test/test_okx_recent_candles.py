"""Latest/history routing changes requests, never raw OHLCV or cursor semantics."""
from __future__ import annotations

from copy import deepcopy
from io import BytesIO
from urllib.error import HTTPError

import pytest

from downloader import download_okx_perp_daily as mod


MINUTE = mod.CANDLE_INTERVAL_MS
CLOSED = 30_000 * MINUTE
PARAMS = {"instId": "BTC-USDT-SWAP", "bar": "1m", "limit": "3", "after": str(CLOSED + 1)}


def page():
    return {"code": "0", "data": [[str(CLOSED - n * MINUTE), "100", "101", "99", "100",
                                   "10", "0.01", "1000", "1"] for n in range(3)]}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(mod, "_latest_closed_candle_start_ms", lambda: CLOSED)
    return mod.OkxClient(request_interval=None, max_retries=0, retry_base=0.1)


def test_complete_recent_page_reuses_identical_raw_values_and_reports_route(client, monkeypatch):
    payload, calls = page(), []
    def fetch(path, params):
        calls.append((path, dict(params)))
        return payload
    monkeypatch.setattr(client, "_get", fetch)
    assert client.get(mod.HISTORY_CANDLES_ENDPOINT, PARAMS) is payload
    assert calls == [(mod.RECENT_CANDLES_ENDPOINT, PARAMS)]
    assert client.candle_route_activity() == {"recent_pages": 1, "history_fallback_pages": 0}


@pytest.mark.parametrize("mutation", [
    lambda p: p.update(data=[]),
    lambda p: p["data"].pop(),
    lambda p: p["data"].append(p["data"][-1]),
    lambda p: p["data"][0].__setitem__(8, "0"),
    lambda p: p["data"][1].__setitem__(0, p["data"][0][0]),
    lambda p: p["data"][1].__setitem__(0, "invalid"),
    lambda p: p["data"].reverse(),
    lambda p: p["data"].__setitem__(1, ["invalid"]),
    lambda p: p.update(data=None),
])
def test_invalid_recent_page_falls_back_at_same_cursor_without_partial_acceptance(client, monkeypatch, mutation):
    recent, history, calls = page(), page(), []
    mutation(recent)
    def fetch(path, params):
        calls.append((path, dict(params)))
        return recent if path == mod.RECENT_CANDLES_ENDPOINT else history
    monkeypatch.setattr(client, "_get", fetch)
    before = deepcopy(PARAMS)
    assert client.get(mod.HISTORY_CANDLES_ENDPOINT, PARAMS) is history
    assert calls == [(mod.RECENT_CANDLES_ENDPOINT, before), (mod.HISTORY_CANDLES_ENDPOINT, before)]
    assert PARAMS == before
    assert client.candle_route_activity() == {"recent_pages": 0, "history_fallback_pages": 1}


@pytest.mark.parametrize("patch", [
    {"after": str(CLOSED - 2000 * MINUTE)}, {"after": str(CLOSED + 2 * MINUTE)},
    {"bar": "5m"}, {"limit": "301"}, {"limit": "invalid"}, {"after": "invalid"},
])
def test_old_unfinished_or_unsupported_pages_remain_on_history(client, monkeypatch, patch):
    calls = []
    monkeypatch.setattr(client, "_get", lambda path, params: calls.append(path) or page())
    client.get(mod.HISTORY_CANDLES_ENDPOINT, PARAMS | patch)
    assert calls == [mod.HISTORY_CANDLES_ENDPOINT]


@pytest.mark.parametrize("status", [403, 429, 503])
def test_recent_transport_failure_is_not_bypassed_through_history(client, monkeypatch, status):
    calls = []
    error = HTTPError("https://www.okx.com/fixture", status, "fixture", {}, BytesIO())
    def fail(path, params):
        calls.append(path)
        raise error
    monkeypatch.setattr(client, "_get", fail)
    with pytest.raises(HTTPError) as caught:
        client.get(mod.HISTORY_CANDLES_ENDPOINT, PARAMS)
    assert caught.value is error
    error.close()
    assert calls == [mod.RECENT_CANDLES_ENDPOINT]


def test_official_recent_bucket_is_shared_between_symbols_and_override_stays_slower(client):
    recent = client._limiter_for_request(mod.RECENT_CANDLES_ENDPOINT, {"instId": "BTC-USDT-SWAP"})
    assert recent.interval_seconds == 0.05
    assert recent.name == "okx_recent_candles"
    assert client._limiter_for_request(mod.RECENT_CANDLES_ENDPOINT, {"instId": "ETH-USDT-SWAP"}) is recent
    history = client._limiter_for_request(mod.HISTORY_CANDLES_ENDPOINT)
    assert history.interval_seconds == 0.1
    assert recent is not history
    slower = mod.OkxClient(request_interval=0.5, max_retries=0, retry_base=0.1)
    assert slower._limiter_for_request(mod.RECENT_CANDLES_ENDPOINT).interval_seconds == 0.5


def test_history_only_oracle_never_uses_recent_route(client, monkeypatch):
    calls = []
    client.prefer_recent_candles = False
    monkeypatch.setattr(client, "_get", lambda path, params: calls.append(path) or page())
    client.get(mod.HISTORY_CANDLES_ENDPOINT, PARAMS)
    assert calls == [mod.HISTORY_CANDLES_ENDPOINT]
