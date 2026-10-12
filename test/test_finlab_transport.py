from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from email.utils import format_datetime
import hashlib
from io import BytesIO
import multiprocessing
from types import SimpleNamespace
import threading
import time

import pandas as pd
import pytest

from stockagent.data.finlab_transport import (
    HTTP_COUNTERS, PREFETCH_MAX_BYTES, RateGate, SignedMinutePrefetch,
    WorkerHTTPTransport, retry_after_seconds, validate_prefetch_sdk,
)


@pytest.mark.parametrize("value,expected", [(None, 60), ("invalid", 60), ("nan", 60),
    ("inf", 60), ("-1", 60), ("0", 1), ("2.5", 2.5), ("900", 900)])
def test_retry_after_is_bounded_by_source_not_a_hardcoded_short_sleep(value, expected):
    assert retry_after_seconds(value, now=1000) == expected


def test_retry_after_http_date():
    value = format_datetime(datetime.fromtimestamp(1300, UTC), usegmt=True)
    assert retry_after_seconds(value, now=1000) == 300


def test_rate_gate_blocks_all_workers_and_keeps_longer_cooldown():
    from finlab.exceptions import DataError
    shared = multiprocessing.get_context("spawn").Array("d", [0, 0])
    a, b = RateGate(shared), RateGate(shared)
    a.defer(900)
    original = a.deadline()
    b.defer(10)
    assert a.deadline() >= original and shared[1] == 2
    with pytest.raises(DataError, match="shared source cooldown"):
        b.check()


def test_transport_429_does_not_repeat_the_http_request_or_retain_url():
    from finlab.exceptions import DataError
    calls = []

    def get(*args, **kwargs):
        calls.append(True)
        return SimpleNamespace(status_code=429, headers={"Retry-After": "900"}, content=b"")

    shared = multiprocessing.get_context("spawn").Array("d", [0, 0])
    transport = WorkerHTTPTransport(SimpleNamespace(get=get), keepalive=False, rate_gate=shared)
    transport.get("https://fixture.invalid/private-signed-url", timeout=120)
    with pytest.raises(DataError):
        transport.get("https://fixture.invalid/private-signed-url", timeout=120)
    metrics = transport.metrics()
    assert len(calls) == metrics["http_429s"] == metrics["http_errors"] == 1
    assert not any("url" in key or "token" in key for key in metrics)
    assert metrics["source_backoff_until_utc"]


@pytest.mark.parametrize("mode", ["manifest", "download"])
@pytest.mark.parametrize("status", [200, 409])
def test_control_conflict_retry_preserves_exact_body_and_has_a_finite_bound(monkeypatch, mode, status):
    from unittest.mock import Mock
    calls = []
    monkeypatch.setattr("stockagent.data.finlab_transport.time.sleep", lambda _: None)
    response = SimpleNamespace(status_code=status, headers={},
        json=lambda: {"code": "request_conflict", "error": "fixture rejected transaction"})

    def post(*args, **kwargs):
        calls.append(kwargs["json"])
        return response

    gate = Mock()
    transport = WorkerHTTPTransport(SimpleNamespace(post=post), keepalive=False, authorization_gate=gate)
    body = {"intraday": {"dataset": "tw_minute:2330", "mode": mode}}
    if mode == "download":
        body["intraday"]["request_id"] = "fixture-id"
    assert transport.post("https://fixture.invalid", json=body, timeout=60) is response
    assert len(calls) == gate.acquire.call_count == gate.release.call_count == 3
    assert all(value is body for value in calls)
    assert transport.metrics()["http_conflicts"] == 3
    assert transport.metrics()["http_conflict_retries"] == 2
    assert transport.metrics()["http_manifests" if mode == "manifest" else "http_signs"] == 3


@pytest.mark.parametrize("code", ["quota_exhausted", "not_ready", "unauthenticated", "other_error"])
@pytest.mark.parametrize("mode", ["manifest", "download"])
def test_control_gate_does_not_add_retries_for_other_errors(code, mode):
    from unittest.mock import Mock
    response = SimpleNamespace(status_code=409, headers={}, json=lambda: {"code": code, "error": "fixture"})
    post, gate = Mock(return_value=response), Mock()
    transport = WorkerHTTPTransport(SimpleNamespace(post=post), keepalive=False, authorization_gate=gate)
    transport.post("https://fixture.invalid", json={"intraday": {"mode": mode}}, timeout=60)
    assert post.call_count == gate.acquire.call_count == gate.release.call_count == 1
    assert transport.metrics()["http_conflicts"] == 0


def test_explicit_json_source_rate_limit_also_stops_other_control_requests():
    from finlab.exceptions import DataError
    from unittest.mock import Mock
    shared = multiprocessing.get_context("spawn").Array("d", [0, 0])
    response = SimpleNamespace(status_code=200, headers={"Retry-After": "900"},
        json=lambda: {"error": "private message withheld", "code": "rate_limited"})
    post = Mock(return_value=response)
    first = WorkerHTTPTransport(SimpleNamespace(post=post), keepalive=False, rate_gate=shared)
    second = WorkerHTTPTransport(SimpleNamespace(post=post), keepalive=False, rate_gate=shared)
    first.post("https://fixture.invalid", json={"intraday": {"mode": "manifest"}})
    with pytest.raises(DataError, match="shared source cooldown"):
        second.post("https://fixture.invalid", json={"intraday": {"mode": "download"}})
    assert post.call_count == 1 and shared[0] > time.time()+899


def test_keepalive_session_is_thread_local_and_timeout_tls_are_unchanged():
    sessions, requests = [], []
    lock = threading.Lock()
    barrier = threading.Barrier(3)

    class Session:
        def __init__(self):
            self.cookies = SimpleNamespace(clear=lambda: None)
            sessions.append(self)

        def mount(self, *args):
            pass

        def get(self, *args, **kwargs):
            with lock:
                requests.append((id(self), args, kwargs))
            barrier.wait(timeout=3)
            return SimpleNamespace(status_code=200, headers={}, content=b"abc")

        def close(self):
            pass

    transport = WorkerHTTPTransport(SimpleNamespace(Session=Session,
        adapters=SimpleNamespace(HTTPAdapter=lambda **kwargs: None)))
    with ThreadPoolExecutor(3) as pool:
        jobs = [pool.submit(transport.get, "https://fixture.invalid", timeout=120, verify=True) for _ in range(3)]
        assert all(job.result().status_code == 200 for job in jobs)
    assert len({call[0] for call in requests}) == 3
    assert all(call[2] == {"timeout": 120, "verify": True} for call in requests)
    assert len(sessions) == 4  # Main-thread manifest session plus object threads.
    metrics = transport.metrics()
    assert metrics["http_peak_inflight"] == 3 and metrics["http_download_bytes"] == 9
    assert set(HTTP_COUNTERS) <= metrics.keys()
    transport.close()


@pytest.fixture
def native_sdk(monkeypatch):
    """Actual native get/download/SHA/schema/cache logic; fake network only."""
    from finlab import utils
    from finlab.data import context, intraday
    parts, bodies, calls, cache_values = [], {}, [], {}
    source = []
    for ordinal in range(5):
        day = f"2026-06-{ordinal+1:02d}"
        row = pd.DataFrame({"stock_id": ["2330"], "trade_date": [day],
            "timestamp": pd.to_datetime([f"{day}T09:00:00+08:00"]).tz_convert("Asia/Taipei"), "session": ["regular"],
            "open": [100.0], "high": [102.0], "low": [99.0], "close": [101.0],
            "volume": [1000], "tick_count": [1], "vwap": [101.0]})
        source.append(row)
        raw = row.to_parquet(index=False)
        part = {"key": f"fixture-{day}", "date": day, "generation": "1", "status": "ready",
                "size": len(raw), "rows": 1, "sha256": hashlib.sha256(raw).hexdigest()}
        parts.append(part)
        bodies[f"https://fixture.invalid/{day}"] = raw
    signed_transform = [lambda values: values]

    def post(query):
        calls.append(query["mode"])
        chosen = parts if query["mode"] == "manifest" else [
            {**part, "url": f"https://fixture.invalid/{part['date']}"} for part in parts
            if part["date"] in {p["date"] for p in query["partitions"]}]
        if query["mode"] == "download":
            chosen = signed_transform[0](chosen)
        return {"partitions": chosen, "volume_unit": "provider_native", "closed_dates": []}

    def get(url, **kwargs):
        time.sleep(0.01)
        return SimpleNamespace(status_code=200, headers={}, content=bodies[url])

    monkeypatch.setattr(intraday, "_post", post)
    monkeypatch.setattr(intraday, "_download", intraday._download)
    monkeypatch.setattr(intraday, "get_intraday", intraday.get_intraday)
    monkeypatch.setattr(context._default_context, "use_local_data_only", False)
    monkeypatch.setattr(context._default_context, "force_cloud_download", False)

    class Cache:
        def get_dataframe(self, key):
            return cache_values.get(key)

        def set_dataframe(self, key, value):
            cache_values[key] = value

    monkeypatch.setattr(context._default_context, "_storage", Cache())
    transport = WorkerHTTPTransport(SimpleNamespace(get=get), keepalive=False)
    monkeypatch.setattr(utils, "requests", transport)
    return SimpleNamespace(module=intraday, parts=parts, bodies=bodies, calls=calls,
        source=pd.concat(source, ignore_index=True), transform=signed_transform, transport=transport)


def native_get(sdk):
    return sdk.module.get_intraday("tw_minute:2330", start="2026-06-01", end="2026-06-05",
        force_download=False, save_to_storage=True, lazy=False, progress="silent", progress_callback=None)


def test_prefetch_preserves_native_data_checks_and_cache_without_extra_signing(native_sdk):
    sdk = native_sdk
    adapter = SignedMinutePrefetch(sdk.module, 5)
    adapter.install()
    try:
        got = native_get(sdk)
        pd.testing.assert_frame_equal(got, sdk.source)
        assert got.attrs["volume_unit"] == "provider_native"
        assert sdk.calls == ["manifest", "download"]
        metrics = sdk.transport.metrics()
        assert metrics["http_gets"] == 5 and metrics["http_peak_inflight"] >= 2
        pd.testing.assert_frame_equal(native_get(sdk), got)
        assert sdk.calls == ["manifest", "download", "manifest"]
        assert sdk.transport.metrics()["http_gets"] == 5  # Exact generation cache.
        assert adapter._futures == {}
    finally:
        adapter.close()


@pytest.mark.parametrize("field", ["generation", "sha256", "size", "rows", "key"])
def test_changed_signed_identity_is_rejected_before_any_prefetch(native_sdk, field):
    from finlab.exceptions import DataError
    sdk = native_sdk
    sdk.transform[0] = lambda values: [*values[:-1], {**values[-1], field: "wrong"}]
    adapter = SignedMinutePrefetch(sdk.module, 5)
    adapter.install()
    try:
        with pytest.raises(DataError, match="Publication changed"):
            native_get(sdk)
        assert sdk.transport.metrics()["http_gets"] == 0
    finally:
        adapter.close()


def test_native_checksum_error_is_not_a_success_and_pending_objects_are_released(native_sdk):
    from finlab.exceptions import DataError
    sdk = native_sdk
    sdk.bodies["https://fixture.invalid/2026-06-01"] = b"corrupt"
    adapter = SignedMinutePrefetch(sdk.module, 5)
    adapter.install()
    try:
        with pytest.raises(DataError, match="partition download failed"):
            native_get(sdk)
        assert adapter._futures == {}
        assert sdk.transport.metrics()["http_gets"] == 6  # Native bounded retry.
    finally:
        adapter.close()


def test_unknown_sdk_version_is_rejected_in_parent(monkeypatch):
    monkeypatch.setattr("stockagent.data.finlab_transport.version", lambda _: "unknown")
    with pytest.raises(RuntimeError, match="minute_download_threads=1"):
        validate_prefetch_sdk()


def test_large_signing_group_does_not_allocate_unbounded_prefetch(native_sdk):
    sdk = native_sdk
    for part in sdk.parts:
        part["size"] = PREFETCH_MAX_BYTES
    adapter = SignedMinutePrefetch(sdk.module, 5)
    try:
        adapter.post({"dataset": "tw_minute:2330", "start": "2026-06-01", "end": "2026-06-05", "mode": "manifest"})
        adapter.post({"dataset": "tw_minute:2330", "start": "2026-06-01", "end": "2026-06-05", "mode": "download",
                      "partitions": [{"date": p["date"], "generation": p["generation"]} for p in sdk.parts]})
        assert adapter._pool is None and sdk.transport.metrics()["http_gets"] == 0
    finally:
        adapter.close()
