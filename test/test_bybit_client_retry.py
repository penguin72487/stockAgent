from __future__ import annotations

import io
import json

import pytest

from downloader import download_bybit_perp_daily as bybit


class FakeLimiter:
    def __init__(self):
        self.waits = 0
        self.deferrals = []

    def wait(self):
        self.waits += 1

    def defer(self, seconds):
        self.deferrals.append(seconds)


class Http200Response(io.StringIO):
    status = 200

    def __init__(self, payload):
        super().__init__(json.dumps(payload))
        self.headers = {"Retry-After": "4"}


def fake_client(monkeypatch, payloads, *, max_retries=2):
    limiter = FakeLimiter()
    requests = []
    retry_calls = []
    responses = iter(payloads)

    def urlopen(request, *, timeout):
        requests.append((request.full_url, timeout))
        return Http200Response(next(responses))

    def retry_delay(attempt, **kwargs):
        retry_calls.append((attempt, kwargs))
        return float(2**attempt)

    monkeypatch.setattr(bybit, "SharedRateLimiter", lambda *_a, **_kw: limiter)
    monkeypatch.setattr(bybit, "urlopen", urlopen)
    monkeypatch.setattr(bybit, "retry_delay_seconds", retry_delay)
    client = bybit.BybitClient(request_interval=None, max_retries=max_retries, retry_base=0.5)
    return client, limiter, requests, retry_calls


@pytest.mark.parametrize("code", [10016, "10016"])
def test_server_error_http200_retries_same_page_then_returns_success(monkeypatch, code):
    success = {"retCode": 0, "result": {"list": [["123", "100"]]}}
    client, limiter, requests, retries = fake_client(monkeypatch, [
        {"retCode": code, "retMsg": "svc error: Get kline failed", "result": {"list": []}},
        success,
    ])
    result = client.get(bybit.KLINE_ENDPOINT, {"category": "linear", "symbol": "XTZUSDT", "start": "1", "end": "2"})
    assert result == success
    assert len(requests) == limiter.waits == 2
    assert requests[0] == requests[1]
    assert requests[0][1] == 30
    assert limiter.deferrals == [1.0]
    assert retries == [(0, {"base": 0.5, "retry_after": "4"})]


@pytest.mark.parametrize("max_retries", [0, 1, 3])
def test_server_error_retry_budget_exhaustion_raises_final_error(monkeypatch, max_retries):
    responses = [{"retCode": 10016, "retMsg": f"server error attempt {i}"} for i in range(max_retries + 1)]
    client, limiter, requests, retries = fake_client(monkeypatch, responses, max_retries=max_retries)
    with pytest.raises(RuntimeError, match=f"retCode=10016 retMsg=server error attempt {max_retries}$"):
        client.get(bybit.KLINE_ENDPOINT, {"symbol": "BTCUSD"})
    assert len(requests) == limiter.waits == max_retries + 1
    assert len(set(requests)) == 1
    assert [attempt for attempt, _ in retries] == list(range(max_retries))
    assert limiter.deferrals == [float(2**i) for i in range(max_retries)]


@pytest.mark.parametrize("code", [10001, 10029, -1])
def test_nonretriable_http200_api_error_still_fails_without_retry(monkeypatch, code):
    client, limiter, requests, retries = fake_client(monkeypatch, [{"retCode": code, "retMsg": "not transient"}])
    with pytest.raises(RuntimeError, match=f"retCode={code} retMsg=not transient$"):
        client.get(bybit.KLINE_ENDPOINT, {"symbol": "ETHUSD"})
    assert len(requests) == limiter.waits == 1
    assert limiter.deferrals == retries == []


@pytest.mark.parametrize("code", [10000, 10006, 429])
def test_existing_retry_codes_keep_same_bounded_policy(monkeypatch, code):
    client, limiter, requests, retries = fake_client(monkeypatch, [
        {"retCode": code, "retMsg": "transient"},
        {"retCode": 0, "result": {"list": []}},
    ], max_retries=1)
    assert client.get(bybit.KLINE_ENDPOINT, {"symbol": "BTCUSD"})["retCode"] == 0
    assert len(requests) == limiter.waits == 2
    assert len(retries) == len(limiter.deferrals) == 1
