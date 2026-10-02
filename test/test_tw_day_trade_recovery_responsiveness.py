from datetime import datetime
import threading
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts import run_tw_day_trade_simulation as runner


NOW = datetime(2026, 10, 1, 9, 5, tzinfo=ZoneInfo("Asia/Taipei"))


def _local_empty(_roots, symbols, **_kwargs):
    return {}, {"requested_symbols": len(symbols), "resolved_symbols": 0}


def _fetched(symbols):
    return (
        {s: {"execution_price_0901": 100.0, "source": "fixture_0901"} for s in symbols},
        {"attempted_symbols": list(symbols), "queried_symbols": len(symbols),
         "resolved_symbols": len(symbols), "unqueried_symbols": 0,
         "failed_symbols": [], "error_counts": {}},
    )


def test_recovery_batches_are_durable_and_restart_skips_cached_prices(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(runner, "load_local_stock_0901_vwaps", _local_empty)

    def fetch(symbols, **_kwargs):
        calls.append(list(symbols))
        return _fetched(symbols)

    monkeypatch.setattr(runner, "fetch_shioaji_historical_stock_0901_vwaps", fetch)
    prices, receipt = runner._resolve_missed_opening_prices(
        tmp_path, NOW, {"1101", "2330", "4905"},
        max_remote_symbols=1, symbol_priority=("4905", "2330"),
    )
    assert set(prices) == {"4905"}
    assert receipt["attempted_symbols"] == ["4905"]
    assert receipt["unqueried_symbols"] == 2
    assert runner._load_missed_opening_prices(tmp_path, NOW) == (prices, receipt)
    recovered = runner._MissedOpeningPriceRecovery(tmp_path, batch_size=2)
    recovered.poll(NOW, {"1101", "2330", "4905"})
    recovered._thread.join(timeout=2)
    assert not recovered._thread.is_alive()
    assert calls == [["4905"], ["1101", "2330"]]
    assert set(recovered.poll(NOW, {"1101", "2330", "4905"})[0]) == {"1101", "2330", "4905"}


def test_network_wait_does_not_block_main_loop_or_spawn_duplicate_worker(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    calls = []
    monkeypatch.setattr(runner, "load_local_stock_0901_vwaps", _local_empty)

    def fetch(symbols, **_kwargs):
        calls.append(list(symbols))
        entered.set()
        assert release.wait(timeout=2)
        return _fetched(symbols)

    monkeypatch.setattr(runner, "fetch_shioaji_historical_stock_0901_vwaps", fetch)
    recovery = runner._MissedOpeningPriceRecovery(tmp_path, batch_size=1)
    try:
        assert recovery.poll(NOW, {"2330", "4905"}) == ({}, {})
        assert entered.wait(timeout=1)
        # The worker is still waiting on a broker reply; main remains usable.
        for _ in range(10):
            assert recovery.poll(NOW, {"2330", "4905"}) == ({}, {})
        assert calls == [["2330"]]
    finally:
        release.set()
        recovery._thread.join(timeout=2)


def test_local_mode_can_register_while_remote_mode_is_waiting(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(runner, "load_local_stock_0901_vwaps", lambda *_a, **_kw: (
        {"2330": {"execution_price_0901": 100.0, "source": "fixture_local"}},
        {"resolved_symbols": 1},
    ))

    def fetch(symbols, **_kwargs):
        entered.set()
        assert release.wait(timeout=2)
        return _fetched(symbols)

    monkeypatch.setattr(runner, "fetch_shioaji_historical_stock_0901_vwaps", fetch)
    recovery = runner._MissedOpeningPriceRecovery(tmp_path)
    try:
        recovery.poll(NOW, {"2330", "4905"})
        assert entered.wait(timeout=1)
        prices, receipt = recovery.poll(NOW, {"2330", "4905"})
        assert set(prices) == {"2330"}
        assert runner._load_missed_opening_prices(tmp_path, NOW) == (prices, receipt)
        assert not runner._missed_opening_query_pending(prices, receipt, {"2330"}, NOW)
        assert runner._missed_opening_query_pending(prices, receipt, {"4905"}, NOW)
    finally:
        release.set()
        recovery._thread.join(timeout=2)


def test_resolved_remote_mode_is_durable_before_next_brokers_reply(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(runner, "load_local_stock_0901_vwaps", _local_empty)

    def fetch(symbols, **kwargs):
        prices, receipt = _fetched(symbols)
        kwargs["resolved_callback"](symbols[0], prices[symbols[0]])
        entered.set()
        assert release.wait(timeout=2)
        return prices, receipt

    monkeypatch.setattr(runner, "fetch_shioaji_historical_stock_0901_vwaps", fetch)
    recovery = runner._MissedOpeningPriceRecovery(tmp_path)
    try:
        recovery.poll(NOW, {"2330", "4905"}, symbol_priority=("2330", "4905"))
        assert entered.wait(timeout=1)
        prices, receipt = recovery.poll(NOW, {"2330", "4905"})
        assert set(prices) == {"2330"}
        assert runner._load_missed_opening_prices(tmp_path, NOW) == (prices, receipt)
        assert not runner._missed_opening_query_pending(prices, receipt, {"2330"}, NOW)
    finally:
        release.set()
        recovery._thread.join(timeout=2)


def test_quota_unqueried_symbols_are_not_marked_attempted(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "load_local_stock_0901_vwaps", _local_empty)
    monkeypatch.setattr(
        runner, "fetch_shioaji_historical_stock_0901_vwaps",
        lambda *_args, **_kwargs: ({}, {
            "attempted_symbols": [], "unqueried_symbols": 2,
            "error_counts": {}, "stopped_for_traffic": True,
        }),
    )
    prices, receipt = runner._resolve_missed_opening_prices(tmp_path, NOW, {"2330", "4905"})
    assert receipt["attempted_symbols"] == []
    assert receipt["unqueried_symbols"] == 2
    assert runner._missed_opening_query_pending(prices, receipt, {"4905"}, NOW)


def test_one_mode_does_not_wait_for_another_modes_unqueried_or_failed_symbols():
    receipt = {"attempted_symbols": ["2330"], "retry_symbols": ["4905"],
               "error_counts": {"TimeoutError": 1}, "unqueried_symbols": 800}
    assert not runner._missed_opening_query_pending({}, receipt, {"2330"}, NOW)
    assert runner._missed_opening_query_pending({}, receipt, {"4905"}, NOW)
    assert runner._missed_opening_query_pending({}, receipt, {"1101"}, NOW)
    assert not runner._missed_opening_query_pending({"4905": {}}, receipt, {"4905"}, NOW)
    assert runner._missed_opening_query_pending(
        {}, receipt, {"2330"}, NOW.replace(hour=9, minute=1),
    )  # Empty data may still be published before 09:03.


def test_failed_symbols_are_retried_without_repeating_source_empty_symbols(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(runner, "load_local_stock_0901_vwaps", _local_empty)

    def fetch(symbols, **_kwargs):
        calls.append(list(symbols))
        if len(calls) == 1:
            return {}, {"attempted_symbols": list(symbols), "failed_symbols": ["4905"],
                        "unqueried_symbols": 0, "error_counts": {"TimeoutError": 1}}
        return _fetched(symbols)

    monkeypatch.setattr(runner, "fetch_shioaji_historical_stock_0901_vwaps", fetch)
    runner._resolve_missed_opening_prices(tmp_path, NOW, {"2330", "4905"})
    prices, receipt = runner._resolve_missed_opening_prices(tmp_path, NOW, {"2330", "4905"})
    assert calls == [["2330", "4905"], ["4905"]]
    assert set(prices) == {"4905"}
    assert receipt["retry_symbols"] == []


def _discovery(mode, *, price=50.0):
    return runner._missed_opening_quote_symbols(
        spec=SimpleNamespace(initial_capital_twd=10_000_000.0, lot_size=1000),
        rows=[{"symbol": "2330", "target_weight": 0.003, "open_price": price,
               "current_price": 50.0, "tradable": True, "can_buy": True}],
        eligibility={"2330": SimpleNamespace(covered=True, eligible=True, short_open=True)},
        mode=mode, observed=NOW,
    )[0]


def test_pruning_uses_actual_compounded_nav_not_initial_capital():
    mode = {"initial_capital_twd": 10_000_000.0,
            "cumulative_realized_net_pnl_twd": 10_000_000.0}
    assert _discovery(mode) == {"2330"}  # 1200 shares, despite initial-capital 600.
    assert _discovery({"initial_capital_twd": 10_000_000.0}) == set()
    assert _discovery({**mode, "sizing_session_date": "2026-10-01",
                       "session_sizing_nav_twd": 10_000_000.0}) == set()


@pytest.mark.parametrize("extra", [
    {"positions": {"held": {"signed_shares": 1000, "symbol": "2330"}}},
    {"corporate_action_ledger": [{"amount_twd": 1_000_000.0}]},
    {"cumulative_realized_net_pnl_twd": -10_000_000.0},
])
def test_pruning_keeps_inventory_unknown_accounting_and_invalid_nav(extra):
    assert _discovery({"initial_capital_twd": 10_000_000.0, **extra}) == {"2330"}


def test_pruning_keeps_unknown_official_open_even_when_current_price_is_known():
    assert _discovery({"initial_capital_twd": 10_000_000.0}, price=None) == {"2330"}


def test_large_quote_universe_reconciles_before_ttl_without_exceeding_subscription_cap(monkeypatch):
    from stockagent.live import quote_provider as provider

    clock = [100.0]
    subscribed = set()

    class API:
        contracts = SimpleNamespace(get=lambda code: SimpleNamespace(code=code))

        def set_on_quote_stk_v1_callback(self, _callback):
            pass

        def subscribe(self, contract, **_kwargs):
            subscribed.add(contract.code)
            assert len(subscribed) <= 200

        def unsubscribe(self, contract, **_kwargs):
            subscribed.remove(contract.code)

    api = API()
    monkeypatch.setattr(provider, "_shioaji_stock_api", lambda: api)
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_API", None)
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_SUBSCRIPTIONS", set())
    monkeypatch.setattr(provider, "_SHIOAJI_STREAM_ROWS", {})
    monkeypatch.setattr(provider, "_load_prepared_tw_price_limits", lambda _day: ({}, None))
    monkeypatch.setattr(provider.time, "monotonic", lambda: clock[0])
    symbols = [str(1000 + i) for i in range(366)]

    def reconcile():
        for _ in range(10):
            quotes = provider.fetch_shioaji_stock_live_quotes(symbols, trading_date=NOW.date())
            pending = any(q["stream_reconciliation_pending"] for q in quotes.values())
            if not pending:
                return
            clock[0] += runner._stream_rotation_interval_seconds(pending)
        pytest.fail("bounded subscription reconciliation did not complete")

    reconcile()
    first_target = set(subscribed)
    assert len(first_target) == 200
    clock[0] = provider._SHIOAJI_STREAM_ROTATION_AT + 0.01
    started = clock[0]
    reconcile()
    assert len(subscribed) == 200
    assert subscribed != first_target
    assert clock[0] - started < 1.0
    assert clock[0] - started + provider._SHIOAJI_STOCK_STREAM_DWELL_SECONDS < 10.0
    assert runner._stream_rotation_interval_seconds(False) == 1.0


def test_history_rate_window_is_shared_across_small_recovery_batches(monkeypatch):
    from collections import deque
    from stockagent.live import quote_provider as provider

    clock, sleeps = [100.0], []
    monkeypatch.setattr(provider, "_SHIOAJI_HISTORY_REQUEST_TIMES", deque())
    monkeypatch.setattr(provider.time, "monotonic", lambda: clock[0])

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    monkeypatch.setattr(provider.time, "sleep", sleep)
    for _batch in range(6):
        for _symbol in range(8):
            provider._wait_stock_history_request_slot()
    assert sleeps == [pytest.approx(10.01)]
    assert len(provider._SHIOAJI_HISTORY_REQUEST_TIMES) == 8


def test_failed_high_priority_symbol_does_not_starve_unqueried_symbols(tmp_path, monkeypatch):
    calls = []
    runner._persist_missed_opening_prices(tmp_path, NOW, prices={}, query_receipt={
        "attempted_symbols": ["1101"], "retry_symbols": ["1101"],
        "unqueried_symbols": 2, "error_counts": {"TimeoutError": 1},
    })
    monkeypatch.setattr(runner, "load_local_stock_0901_vwaps", _local_empty)

    def fetch(symbols, **_kwargs):
        calls.append(symbols)
        return _fetched(symbols)

    monkeypatch.setattr(runner, "fetch_shioaji_historical_stock_0901_vwaps", fetch)
    prices, receipt = runner._resolve_missed_opening_prices(
        tmp_path, NOW, {"1101", "2330", "4905"}, max_remote_symbols=1,
        symbol_priority=("1101", "2330", "4905"),
    )
    assert calls == [["2330"]]
    assert set(prices) == {"2330"}
    assert receipt["retry_symbols"] == ["1101"]
    assert receipt["unqueried_symbols"] == 1


@pytest.mark.parametrize("receipt,expected_delay", [
    ({"retry_symbols": ["1101"], "unqueried_symbols": 2}, 0.0),
    ({"retry_symbols": ["1101"], "unqueried_symbols": 0}, 5.0),
    ({"source_settling": True, "unqueried_symbols": 2}, 0.0),
    ({"source_settling": True, "unqueried_symbols": 0}, 5.0),
    ({"stopped_for_traffic": True, "unqueried_symbols": 2}, 60.0),
    ({"retry_symbols": [], "unqueried_symbols": 0}, 0.0),
])
def test_recovery_retry_cooldown_does_not_delay_independent_fresh_work(
    tmp_path, monkeypatch, receipt, expected_delay,
):
    monkeypatch.setattr(runner.time_module, "monotonic", lambda: 100.0)
    monkeypatch.setattr(runner, "_resolve_missed_opening_prices", lambda *_a, **_kw: ({}, receipt))
    recovery = runner._MissedOpeningPriceRecovery(tmp_path)
    recovery._run(NOW, {"2330"}, ())
    assert recovery._retry_after == 100.0 + expected_delay


@pytest.mark.parametrize("available,trial,day,expected", [
    (True, -1, "2026-10-01", True),
    (True, 0, "2026-10-01", True),
    (True, 1, "2026-10-01", False),
    (False, -1, "2026-10-01", False),
    (True, -1, "2026-09-30", False),
])
def test_snapshot_backup_preserves_execution_fields_and_requires_current_source(
    monkeypatch, available, trial, day, expected,
):
    import numpy as np
    from stockagent.live.quote_provider import PriceSnapshot

    engine = SimpleNamespace(state={"modes": {"one": {"positions": {"p": {
        "symbol": "2330", "signed_shares": 1000, "side": "long", "entry_price": 100.0,
    }}}}})
    snapshot = PriceSnapshot(
        prices=np.array([100.0]), source="shioaji:stock_snapshot",
        bid_prices=np.array([99.5]), ask_prices=np.array([100.0]),
        available_mask=np.array([available]), simtrade_flags=np.array([trial]),
        timestamps_ms=np.array([int(NOW.timestamp() * 1000)]),
        exchange_timestamps_ms=np.array([int(np.datetime64(f"{day}T09:04:00", "ms").astype(np.int64))]),
    )
    monkeypatch.setattr(runner, "fetch_shioaji_stock_snapshots", lambda *_a, **_kw: snapshot)
    original = {"source": "shioaji_stock_quote_stream", "bid": None, "ask": None,
                "last": None, "available": False, "simtrade": None, "quote_at": None}
    quotes = {"2330": dict(original)}
    runner._attach_snapshot_valuation_quotes(quotes, engine=engine, observed=NOW)
    assert {k:quotes["2330"][k] for k in original} == original
    assert (quotes["2330"].get("valuation_bid") == 99.5) is expected
    if expected:
        assert quotes["2330"]["valuation_quote_source"] == "shioaji:stock_snapshot"
        assert quotes["2330"]["valuation_simtrade"] is (None if trial == -1 else False)


def test_incomplete_legacy_receipt_does_not_turn_unqueried_prices_into_source_empty(tmp_path, monkeypatch):
    runner._persist_missed_opening_prices(tmp_path, NOW, prices={}, query_receipt={
        "attempted_symbols": ["2330", "4905"], "unqueried_symbols": 2,
        "stopped_for_traffic": True, "error_counts": {},
    })
    calls = []
    monkeypatch.setattr(runner, "load_local_stock_0901_vwaps", _local_empty)

    def fetch(symbols, **_kwargs):
        calls.append(list(symbols))
        return _fetched(symbols)

    monkeypatch.setattr(runner, "fetch_shioaji_historical_stock_0901_vwaps", fetch)
    prices, receipt = runner._resolve_missed_opening_prices(tmp_path, NOW, {"2330", "4905"})
    assert calls == [["2330", "4905"]]
    assert set(prices) == {"2330", "4905"}
    assert receipt["retry_symbols"] == []


def test_history_nonblocking_uses_received_callback_not_placeholder():
    from stockagent.live.quote_provider import _stock_history_result

    entered = threading.Event()
    callback = []
    actual, placeholder = object(), object()
    results = []

    def method(*, timeout, cb):
        assert timeout == 0
        callback.append(cb)
        entered.set()
        return placeholder

    worker = threading.Thread(target=lambda: results.append(
        _stock_history_result(method, timeout_ms=1000, nonblocking=True)
    ))
    worker.start()
    try:
        assert entered.wait(timeout=1)
        assert worker.is_alive()
        assert results == []
        callback[0](actual)
    finally:
        worker.join(timeout=2)
    assert results == [actual]


def test_timed_out_history_callback_cannot_publish_late_data():
    from stockagent.live.quote_provider import _stock_history_result

    callbacks = []
    def method(*, timeout, cb):
        callbacks.append(cb)
    with pytest.raises(TimeoutError, match="callback timed out"):
        _stock_history_result(method, timeout_ms=1, nonblocking=True)
    callbacks[0](object())  # The expired request is safely closed, not reused.
