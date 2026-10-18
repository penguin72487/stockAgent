from http.client import IncompleteRead, RemoteDisconnected
import io
import json
from urllib.error import HTTPError, URLError

import pytest

from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader import download_binance_perp_15m as binance
from downloader.http_transport import HttpRequestPolicy, ResilientHttpTransport


class Limiter:
    def __init__(self):
        self.waits = 0
        self.deferrals = []

    def wait(self, **_kwargs):
        self.waits += 1

    def defer(self, seconds):
        self.deferrals.append(seconds)


class Response(io.BytesIO):
    status = 200
    headers = {}

    def __init__(self, body, error=None):
        super().__init__(body)
        self.error = error

    def read(self, *args):
        if self.error is not None:
            raise self.error
        return super().read(*args)


def client_for(monkeypatch, kind, failures, *, max_retries=1, phase="open", body=None):
    limiter = Limiter()
    requests = []
    remaining = iter(failures)
    payload = {"retCode": 0, "result": {"list": []}} if kind == "bybit" else {"code": "0", "data": []}
    body = json.dumps(payload).encode() if body is None else body

    def opener(request, *, timeout):
        requests.append((request.full_url, timeout))
        error = next(remaining, None)
        if error is not None and phase == "open":
            raise error
        return Response(body, error)

    if kind == "shared":
        sleeps = []
        client = ResilientHttpTransport(
            HttpRequestPolicy(provider="okx_history_candles", max_retries=max_retries),
            limiter=limiter, opener=opener, sleeper=sleeps.append,
            retry_delay=lambda *_args, **_kwargs: 0.0,
        )
        call = lambda: client.request_bytes("https://example.test/candles")
    else:
        module = bybit if kind == "bybit" else binance if kind == "binance" else okx
        monkeypatch.setattr(module, "archive_urlopen" if kind == "okx_archive" else "urlopen", opener)
        monkeypatch.setattr(module, "SharedRateLimiter", lambda *_args, **_kwargs: limiter)
        monkeypatch.setattr(module, "retry_delay_seconds", lambda *_args, **_kwargs: 0.0)
        if kind == "binance":
            client = binance.BinanceClient(
                requested_weight_per_minute=None, max_retries=max_retries, retry_base=0.1,
            )
        else:
            client_type = bybit.BybitClient if kind == "bybit" else okx.OkxClient
            client = client_type(request_interval=None, max_retries=max_retries, retry_base=0.1)
        if kind == "okx_archive":
            call = lambda: client.get_bytes("https://example.test/funding.zip")
        elif kind == "binance":
            call = lambda: client.get("/test", {"symbol": "PUBLIC", "after": "123"}, weight=2)
        else:
            call = lambda: client.get("/test", {"symbol": "PUBLIC", "after": "123"})
    return call, limiter, requests, payload


ERRORS = [
    lambda: RemoteDisconnected("remote closed the response"),
    lambda: IncompleteRead(b"partial", 100),
    lambda: TimeoutError("response timeout"),
    lambda: ConnectionResetError("connection reset"),
]


@pytest.mark.parametrize("kind", ["okx_json", "okx_archive", "bybit", "binance", "shared"])
@pytest.mark.parametrize("phase", ["open", "read"])
@pytest.mark.parametrize("error_factory", ERRORS)
def test_transient_disconnect_retries_identical_request_then_returns_observed_response(
    monkeypatch, kind, phase, error_factory,
):
    call, limiter, requests, payload = client_for(monkeypatch, kind, [error_factory()], phase=phase)
    result = call()
    expected = json.dumps(payload).encode()
    if kind == "shared":
        assert result.body == expected and result.attempts == 2
    elif kind == "okx_archive":
        assert result == expected
    else:
        assert result == payload
    assert limiter.waits == len(requests) == 2
    assert requests[0] == requests[1]


@pytest.mark.parametrize("kind", ["okx_json", "okx_archive", "bybit", "binance", "shared"])
@pytest.mark.parametrize("budget", [0, 1, 3])
def test_exhausted_disconnect_keeps_bounded_attempt_count_and_failure(monkeypatch, kind, budget):
    errors = [RemoteDisconnected(f"failure {i}") for i in range(budget + 1)]
    call, limiter, requests, _ = client_for(monkeypatch, kind, errors, max_retries=budget)
    with pytest.raises(URLError if kind == "shared" else RemoteDisconnected):
        call()
    assert limiter.waits == len(requests) == budget + 1
    assert len(set(requests)) == 1


@pytest.mark.parametrize("kind", ["okx_json", "okx_archive", "bybit", "binance"])
def test_nonretriable_http_error_still_fails_without_retry(monkeypatch, kind):
    error = HTTPError("https://example.test/invalid", 400, "bad request", {}, io.BytesIO(b"bad"))
    call, limiter, requests, _ = client_for(monkeypatch, kind, [error])
    with pytest.raises(HTTPError) as caught:
        call()
    assert caught.value is error
    assert limiter.waits == len(requests) == 1
    assert limiter.deferrals == []


@pytest.mark.parametrize("kind", ["okx_json", "bybit", "binance"])
def test_payload_decode_error_is_not_relabelled_as_transient_network_failure(monkeypatch, kind):
    call, limiter, requests, _ = client_for(monkeypatch, kind, [], body=b"not JSON")
    with pytest.raises(json.JSONDecodeError):
        call()
    assert limiter.waits == len(requests) == 1


@pytest.mark.parametrize("kind", ["okx_json", "okx_archive", "bybit", "binance", "shared"])
def test_exhausted_rate_limited_request_still_defers_other_workers(monkeypatch, kind):
    error = HTTPError("https://example.test/quota", 429, "limited", {"Retry-After": "7"}, io.BytesIO(b"limited"))
    call, limiter, requests, _ = client_for(monkeypatch, kind, [error], max_retries=0)
    with pytest.raises(Exception) as caught:
        call()
    assert getattr(caught.value, "code", getattr(caught.value, "status", None)) == 429
    assert limiter.waits == len(requests) == len(limiter.deferrals) == 1


def test_bybit_ban_closes_pool_and_preserves_ten_minute_cooldown_even_when_exhausted(monkeypatch):
    closed = []
    monkeypatch.setattr(bybit, "close_pooled_connections", lambda: closed.append(True))
    error = HTTPError("https://example.test/ban", 403, "too frequent", {}, io.BytesIO(b"ban"))
    call, limiter, requests, _ = client_for(monkeypatch, "bybit", [error], max_retries=0)
    with pytest.raises(HTTPError):
        call()
    assert closed == [True]
    assert limiter.deferrals == [600.0]
    assert len(requests) == 1
