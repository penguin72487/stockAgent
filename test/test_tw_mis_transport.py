from __future__ import annotations

from datetime import timedelta
from http.client import RemoteDisconnected
import threading

import numpy as np
import pytest
import requests

from stockagent.live import quote_provider as quotes


@pytest.fixture(autouse=True)
def isolated_transport(monkeypatch):
    monkeypatch.setattr(quotes, "_TW_MIS_HTTP_LOCAL", threading.local())
    monkeypatch.setattr(quotes, "_TW_MIS_BOOTSTRAP_COOKIES", {})
    monkeypatch.setattr(quotes, "_TW_MIS_BOOTSTRAP_AT", 0.)
    monkeypatch.setattr(quotes, "load_symbol_yahoo_map", lambda _: {})
    monkeypatch.setattr(quotes, "_tw_mis_candidates", lambda symbol, _: [f"tse_{symbol}.tw"])


def response(rows, *, rtcode="0000"):
    return type("Response", (), {
        "status_code": 200,
        "elapsed": timedelta(milliseconds=15),
        "raise_for_status": lambda self: None,
        "json": lambda self: {"msgArray": rows, "rtcode": rtcode},
    })()


def row(symbol):
    return {"c": symbol, "z": "101", "o": "100", "v": "123",
            "tlong": "1791853200000"}


def install_session(monkeypatch, get):
    class Session:
        def __init__(self):
            self.headers = {}
            self.cookies = requests.cookies.RequestsCookieJar()

        def get(self, url, **kwargs):
            return get(kwargs["params"], kwargs["timeout"])

        def close(self):
            pass

    monkeypatch.setattr(quotes.requests, "Session", Session)


@pytest.mark.parametrize("timeout,batch_limit", [(.25, 3), (1.5, 20), (2., 20), (4., 80), (8., 80)])
def test_batch_size_matches_deadline_without_reducing_scope(monkeypatch, tmp_path, timeout, batch_limit):
    calls, progress = [], []

    def get(params, actual_timeout):
        assert actual_timeout == timeout
        names = [channel.split("_")[1].split(".")[0] for channel in params["ex_ch"].split("|")]
        calls.append(names)
        return response([row(name) for name in names])

    install_session(monkeypatch, get)
    symbols = [str(1000 + i) for i in range(85)]
    snapshot = quotes.fetch_tw_mis_last_prices(symbols, np.ones(len(symbols)),
        parquet_root=tmp_path, chunk_size=80, request_timeout_seconds=timeout,
        max_parallel_requests=4, empty_chunk_retry_attempts=0,
        on_progress=lambda names, values: progress.append((names, values)))
    assert max(map(len, calls)) == batch_limit
    assert sorted(name for batch in calls for name in batch) == symbols
    assert snapshot.available_count == len(symbols)
    np.testing.assert_array_equal(snapshot.open_prices, np.full(len(symbols), 100.))
    assert sum(len(names) for names, _ in progress) == len(symbols)
    timing = snapshot.transport_timing
    assert timing["mis_effective_chunk_size"] == batch_limit
    assert timing["mis_requested_chunk_size"] == 80
    assert timing["mis_request_count"] == len(calls)
    assert all(request["headers_ms"] == 15. for request in timing["mis_requests"])


def test_partial_batch_retry_only_queries_missing_names_and_preserves_original_clock(monkeypatch, tmp_path):
    calls, observed = [], []

    def get(params, _timeout):
        names = [channel.split("_")[1].split(".")[0] for channel in params["ex_ch"].split("|")]
        calls.append(names)
        return response([row(name) for name in (names[:1] if len(calls) == 1 else names)])

    install_session(monkeypatch, get)
    snapshot = quotes.fetch_tw_mis_last_prices(["2330", "2317", "2454"], np.ones(3),
        parquet_root=tmp_path, max_parallel_requests=1, empty_chunk_retry_attempts=1,
        empty_chunk_retry_delay_seconds=0,
        on_progress=lambda names, values: observed.append((names, values)))
    assert calls == [["2330", "2317", "2454"], ["2317", "2454"]]
    assert snapshot.available_count == 3
    assert snapshot.timestamps_ms[0] == observed[0][1].timestamps_ms[0]
    assert observed[0][0] == ["2330"]
    assert observed[1][0] == ["2317", "2454"]
    assert snapshot.transport_timing["mis_retry_rounds"] == 1
    np.testing.assert_array_equal(snapshot.open_prices, [100., 100., 100.])


def test_reported_source_error_never_becomes_a_price_or_no_first_trade(monkeypatch, tmp_path):
    install_session(monkeypatch, lambda *_: response([row("2330")], rtcode="9999"))
    snapshot = quotes.fetch_tw_mis_last_prices(["2330"], np.array([99.]),
        parquet_root=tmp_path, empty_chunk_retry_attempts=0, on_progress=lambda *_: None)
    assert snapshot.available_count == 0
    assert np.isnan(snapshot.open_prices).all()
    assert not quotes.tw_session_no_open_print_mask(snapshot).any()
    assert snapshot.transport_timing["mis_failure_types"] == ["ValueError"]


@pytest.mark.parametrize("failed", [False, True])
def test_request_timeline_uses_one_clock_and_keeps_failure_boundary(monkeypatch, tmp_path, failed):
    def get(params, _timeout):
        if failed:
            raise requests.ReadTimeout("private endpoint must not be recorded")
        return response([row(params["ex_ch"].split("_")[1].split(".")[0])])

    install_session(monkeypatch, get)
    result = quotes.fetch_tw_mis_last_prices(["2330", "2317"], np.ones(2),
        parquet_root=tmp_path, chunk_size=1, max_parallel_requests=1,
        empty_chunk_retry_attempts=0, on_progress=lambda *_: None)
    timeline = sorted(result.transport_timing["mis_requests"], key=lambda item: item["request_index"])
    assert [item["request_index"] for item in timeline] == [0, 1]
    for item in timeline:
        assert 0 <= item["enqueued_offset_ms"] <= item["request_started_offset_ms"]
        assert item["worker_queue_ms"] >= 0
        if failed:
            assert item["failed_offset_ms"] >= item["request_started_offset_ms"]
            assert "headers_received_offset_ms" not in item
            assert "body_received_offset_ms" not in item
            assert "decoded_rows_ready_offset_ms" not in item
        else:
            assert (item["request_started_offset_ms"] <= item["headers_received_offset_ms"]
                <= item["body_received_offset_ms"] <= item["decoded_rows_ready_offset_ms"])
            assert "failed_offset_ms" not in item
    assert "private endpoint" not in str(result.transport_timing)


def test_source_timing_includes_venue_preparation(monkeypatch, tmp_path):
    clock, offset = quotes.time.perf_counter, [0.]

    def prepare(_root):
        offset[0] += .2
        return {"2330": "2330.TW"}

    monkeypatch.setattr(quotes.time, "perf_counter", lambda: clock() + offset[0])
    monkeypatch.setattr(quotes, "load_symbol_yahoo_map", prepare)
    install_session(monkeypatch, lambda *_: response([row("2330")]))
    result = quotes.fetch_tw_mis_last_prices(["2330"], np.ones(1),
        parquet_root=tmp_path, empty_chunk_retry_attempts=0)
    timing = result.transport_timing
    assert timing["mis_timing_boundary"] == "source_entry_including_venue_map_and_arrays_v2"
    assert timing["mis_total_ms"] >= timing["mis_source_prepare_ms"] >= 200
    assert timing["mis_requests"][0]["enqueued_offset_ms"] >= timing["mis_source_prepare_ms"]


def test_transport_timeout_resets_only_broken_session_and_retains_other_rows(monkeypatch, tmp_path):
    calls = []
    sessions, closed = [], []

    class Session:
        def __init__(self):
            self.headers = {}
            self.cookies = requests.cookies.RequestsCookieJar()
            sessions.append(self)

        def get(self, _url, **kwargs):
            channel = kwargs["params"]["ex_ch"]
            calls.append(channel)
            if len(calls) == 1:
                raise requests.ReadTimeout("must not persist secret query or cookie")
            return response([row(channel.split("_")[1].split(".")[0])])

        def close(self):
            closed.append(self)

    monkeypatch.setattr(quotes.requests, "Session", Session)
    progress = []
    snapshot = quotes.fetch_tw_mis_last_prices(["2330", "2317"], np.array([99., 99.]),
        parquet_root=tmp_path, chunk_size=1, max_parallel_requests=1,
        empty_chunk_retry_attempts=1, empty_chunk_retry_delay_seconds=0,
        on_progress=lambda names, values: progress.append((names, values)))
    assert calls == ["tse_2330.tw", "tse_2317.tw", "tse_2330.tw"]
    # The process-wide pool may dispatch successive jobs to different workers;
    # each owns a separate healthy keep-alive session.
    assert 2 <= len(sessions) <= len(calls)
    assert closed == [sessions[0]]
    assert snapshot.available_count == 2
    assert snapshot.timestamps_ms[1] == progress[0][1].timestamps_ms[0]
    assert snapshot.transport_timing["mis_failed_request_count"] == 1
    assert snapshot.transport_timing["mis_failure_types"] == ["ReadTimeout"]
    assert "must not persist" not in str(snapshot.transport_timing)


def test_malformed_response_rows_are_unknown_not_fabricated(monkeypatch, tmp_path):
    install_session(monkeypatch, lambda *_: response([None, "bad", row("2330")]))
    snapshot = quotes.fetch_tw_mis_last_prices(["2330", "2317"], np.array([99., 99.]),
        parquet_root=tmp_path, empty_chunk_retry_attempts=0, on_progress=lambda *_: None)
    assert snapshot.available_mask.tolist() == [True, False]
    assert np.isnan(snapshot.open_prices[1])
    assert not quotes.tw_session_no_open_print_mask(snapshot).any()


def test_repeated_peer_close_stops_sweep_without_erasing_success(monkeypatch, tmp_path):
    calls = []

    def get(params, _timeout):
        name = params["ex_ch"].split("_")[1].split(".")[0]
        calls.append(name)
        if len(calls) > 1:
            raise requests.ConnectionError(RemoteDisconnected("private text must not persist"))
        return response([row(name)])

    install_session(monkeypatch, get)
    symbols = [str(1000 + i) for i in range(30)]
    snapshot = quotes.fetch_tw_mis_last_prices(symbols, np.full(30, 99.),
        parquet_root=tmp_path, chunk_size=1, max_parallel_requests=1,
        empty_chunk_retry_attempts=2, empty_chunk_retry_delay_seconds=0,
        on_progress=lambda *_: None)
    assert len(calls) == 5  # one success, then four consecutive peer closes
    assert snapshot.available_count == 1
    assert snapshot.open_prices[0] == 100.
    assert np.isnan(snapshot.open_prices[1:]).all()
    timing = snapshot.transport_timing
    assert timing["mis_dispatch_stopped"]
    assert timing["mis_deferred_channel_count"] == 25
    assert timing["mis_retry_rounds"] == 0
    assert timing["mis_error_chain_types"] == ["ConnectionError", "RemoteDisconnected"]
    assert "private text" not in str(timing)


def test_explicit_rate_limit_stops_new_dispatch_and_does_not_retry(monkeypatch, tmp_path):
    calls = []

    def get(params, _timeout):
        calls.append(params["ex_ch"])
        denied = requests.Response()
        denied.status_code = 429
        raise requests.HTTPError(response=denied)

    install_session(monkeypatch, get)
    snapshot = quotes.fetch_tw_mis_last_prices(["2330", "2317", "2454"], np.full(3, 99.),
        parquet_root=tmp_path, chunk_size=1, max_parallel_requests=1,
        empty_chunk_retry_attempts=2, empty_chunk_retry_delay_seconds=0,
        on_progress=lambda *_: None)
    assert len(calls) == 1
    assert snapshot.available_count == 0
    assert snapshot.transport_timing["mis_dispatch_stopped"]
    assert snapshot.transport_timing["mis_http_error_statuses"] == [429]
    assert snapshot.transport_timing["mis_deferred_channel_count"] == 2


def test_opening_mis_budget_is_independent_of_the_model_quorum_wait(monkeypatch, tmp_path):
    from stockagent.live import opening_price_acquisition as acquisition
    monkeypatch.delenv("STOCKAGENT_TW_OPENING_REQUEST_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setattr(acquisition, "optional_opening_providers", lambda: {})
    seen = []

    def fetch(names, values, **kwargs):
        seen.append(kwargs)
        return quotes.PriceSnapshot(prices=values.copy(), source="twse_tpex:mis", available_count=0)

    monkeypatch.setattr(quotes, "fetch_tw_mis_last_prices", fetch)
    provider = acquisition.configured_opening_providers(tmp_path)["official_mis"]
    provider(["2330"], np.array([99.]), lambda *_: None)
    assert seen[0]["request_timeout_seconds"] == 4.
    assert seen[0]["empty_chunk_retry_attempts"] == 0
