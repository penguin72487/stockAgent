"""Deterministic source-price and whole-fetch timing integration, no broker I/O."""

from datetime import date
import sys
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.live import quote_provider as provider
from stockagent.live import shioaji_traffic_ledger as ledger


@pytest.mark.parametrize("mode", ["ticks", "fallback", "failed", "ledger_failed", "quota", "callback"])
def test_remote_price_receipt_attributes_full_context_and_preserves_source_rules(monkeypatch, mode):
    clock = [0.0]
    events = []
    calls = {"usage": 0, "ticks": 0, "kbars": 0}
    callbacks = []

    def advance(milliseconds):
        clock[0] += milliseconds / 1_000.0

    def contract(symbol):
        advance(1)
        return SimpleNamespace(code=symbol)

    class API:
        contracts = SimpleNamespace(get=contract)

        def usage(self):
            calls["usage"] += 1
            advance(3)
            return SimpleNamespace(bytes=900 if mode == "quota" else 100, limit_bytes=1_000)

        def ticks(self, **_kwargs):
            calls["ticks"] += 1
            advance(6)
            if mode == "failed":
                raise RuntimeError("fixture native query failed")
            if mode == "fallback":
                return SimpleNamespace(ts=[], close=[], volume=[])
            return SimpleNamespace(
                ts=[int(np.datetime64(t, "ns").astype(np.int64)) for t in (
                    "2026-08-13T09:00:10", "2026-08-13T09:00:50",
                )], close=[100.0, 110.0], volume=[1.0, 3.0],
            )

        def kbars(self, **_kwargs):
            calls["kbars"] += 1
            advance(8)
            return SimpleNamespace(
                ts=[int(np.datetime64("2026-08-13T09:01:00", "ns").astype(np.int64))],
                Open=[100.5], Close=[101.0], Low=[100.0], High=[102.0],
                Volume=[2.0], Amount=[202_000.0],
            )

    api = API()

    def acquire():
        advance(2)
        return api

    def record(event):
        advance(7)
        if mode == "ledger_failed":
            raise OSError("fixture journal full")
        events.append(event)

    def resolved_callback(symbol, row):
        advance(9)
        callbacks.append((symbol, row))

    monkeypatch.setattr(provider.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(provider, "_shioaji_stock_api", acquire)
    monkeypatch.setattr(provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(provider, "_wait_stock_history_request_slot", lambda: advance(4))
    monkeypatch.setattr(ledger, "record_traffic_event", record)
    monkeypatch.setitem(sys.modules, "shioaji", SimpleNamespace(
        TicksQueryType=SimpleNamespace(RangeTime="RangeTime"),
    ))

    prices, receipt = provider.fetch_shioaji_historical_stock_0901_vwaps(
        ["2330"], trading_date=date(2026, 8, 13), progress_every=0,
        resolved_callback=resolved_callback if mode == "callback" else None,
    )
    timing = receipt["timing"]
    assert timing["contract"] == "historical-0901-fetch-phases-v1"
    assert timing["api_ready_ms"] == 2.0
    assert timing["guard_usage_calls"] == (4 if mode == "fallback" else 3)
    assert timing["guard_usage_ms"] == (12.0 if mode == "fallback" else 9.0)
    assert timing["unmeasured_query_contexts"] == 0
    assert timing["remaining_processing_ms"] == 0.0
    assert timing["ledger_record_failures"] == int(mode == "ledger_failed")
    assert timing["resolved_callback_calls"] == int(mode == "callback")
    assert timing["resolved_callback_ms"] == (9.0 if mode == "callback" else 0.0)

    if mode == "quota":
        assert prices == {}
        assert receipt["stopped_for_traffic"] is True
        assert receipt["unqueried_symbols"] == 1
        assert timing["measured_query_contexts"] == 0
        assert all(v is None for v in timing["query_phase_totals_ms"].values())
        assert timing["total_fetch_ms"] == 11.0
        assert calls == {"usage": 3, "ticks": 0, "kbars": 0}
        return

    assert timing["measured_query_contexts"] == (2 if mode == "fallback" else 1)
    assert timing["contract_resolution_ms"] == 1.0
    assert timing["history_admission_ms"] == (8.0 if mode == "fallback" else 4.0)
    assert timing["query_phase_totals_ms"]["total_context_ms"] == (40.0 if mode == "fallback" else 19.0)
    assert timing["query_phase_totals_ms"]["ledger_record_ms"] == (14.0 if mode == "fallback" else 7.0)
    assert timing["total_fetch_ms"] == (63.0 if mode == "fallback" else 35.0) + (9.0 if mode == "callback" else 0.0)
    assert calls == {"usage": 8 if mode == "fallback" else 5, "ticks": 1, "kbars": int(mode == "fallback")}
    if mode == "failed":
        assert prices == {}
        assert receipt["retry_symbols"] == ["2330"]
        assert receipt["error_counts"] == {"RuntimeError": 1}
        assert events[0]["status"] == "failed"
    else:
        row = prices["2330"]
        assert row["execution_price_0901"] == (101.0 if mode == "fallback" else 107.5)
        assert row["quote_at"] == "2026-08-13T09:01:00+08:00"
        assert row["observed_volume_unit_0901"] == ("shares" if mode == "fallback" else "board_lots")
        assert provider.observed_0901_minute_volume_lots(row) == (2.0 if mode == "fallback" else 4.0)
        assert receipt["error_counts"] == {}
    if events:
        assert events[0]["rows"] == (0 if mode in {"fallback", "failed"} else 2)
    if mode == "callback":
        assert callbacks == [("2330", prices["2330"])]


def test_uninstrumented_legacy_wrapper_is_not_reported_as_zero_query_cost(monkeypatch):
    from contextlib import contextmanager

    @contextmanager
    def legacy_query(*_args, **_kwargs):
        yield lambda _result: None

    monkeypatch.setattr(provider, "shioaji_query", legacy_query)
    monkeypatch.setattr(provider, "_shioaji_stock_api", lambda: SimpleNamespace(
        usage=lambda: SimpleNamespace(bytes=100, limit_bytes=1_000),
        contracts=SimpleNamespace(get=lambda _: object()),
        ticks=lambda **_: SimpleNamespace(ts=[], close=[], volume=[]),
        kbars=lambda **_: SimpleNamespace(ts=[], Open=[], Close=[], Low=[], High=[], Volume=[], Amount=[]),
    ))
    monkeypatch.setattr(provider, "_SHIOAJI_STOCK_CONTRACTS", {})
    monkeypatch.setattr(provider, "_wait_stock_history_request_slot", lambda: None)
    monkeypatch.setitem(sys.modules, "shioaji", SimpleNamespace(
        TicksQueryType=SimpleNamespace(RangeTime="RangeTime"),
    ))
    _, receipt = provider.fetch_shioaji_historical_stock_0901_vwaps(
        ["2330"], trading_date=date(2026, 8, 13), progress_every=0,
    )
    timing = receipt["timing"]
    assert timing["measured_query_contexts"] == 0
    assert timing["unmeasured_query_contexts"] == 2
    assert timing["remaining_processing_ms"] is None
    assert all(v is None for v in timing["query_phase_totals_ms"].values())


def test_history_request_window_matches_canonical_profile_and_retains_live_reserve(monkeypatch):
    from collections import deque
    from dataclasses import replace
    from downloader import common

    profile = common.provider_rate_limit("shioaji_quote_query")
    assert (profile.requests, profile.seconds) == (50, 10)
    clock, starts = [100.0], []
    monkeypatch.setattr(provider, "_SHIOAJI_HISTORY_REQUEST_TIMES", deque())
    monkeypatch.setattr(provider.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(provider.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    for _ in range(100):
        provider._wait_stock_history_request_slot()
        starts.append(clock[0])
    assert starts[40] == pytest.approx(110.01)
    assert starts[80] == pytest.approx(120.02)
    assert max(sum(t - 10 < previous <= t for previous in starts) for t in starts) == 40

    # The rolling gate reads the shared source of truth, not another literal.
    monkeypatch.setitem(common.PROVIDER_RATE_LIMITS, "shioaji_quote_query", replace(
        profile, requests=10, seconds=2,
    ))
    monkeypatch.setattr(provider, "_SHIOAJI_HISTORY_REQUEST_TIMES", deque())
    clock[0] = 100.0
    for _ in range(9):
        provider._wait_stock_history_request_slot()
    assert clock[0] == pytest.approx(102.01)
