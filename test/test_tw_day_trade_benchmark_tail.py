from copy import deepcopy
from datetime import date, datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from scripts import rebuild_tw_day_trade_benchmark_history as builder
from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine, TX_CONTINUOUS_BENCHMARK_ID

TAIPEI = ZoneInfo("Asia/Taipei")


def quote(price):
    return {"bid": price, "ask": price + 1, "previous_close": 43000.,
            "previous_close_date": "2026-11-02", "delivery_month": "202611",
            "last_trading_date": "2026-11-18", "source": "verified_fixture"}


def retained(tmp_path):
    engine = TwDayTradeSimulationEngine(tmp_path / "full")
    start = datetime(2026, 11, 3, 8, 45, tzinfo=TAIPEI)
    rows = []
    for offset in range(300):
        at = start + timedelta(minutes=offset)
        engine._mark_tx_continuous_benchmark(current_contract_code="TXFK6", current_quote=quote(43100. + offset),
            previous_contract_quote={}, now=at)
        rows.append(builder._compact_benchmark_mark(builder._tx_engine_history_mark(engine, observed=at)))
    payload = {"simulation_only": True, "production_order_possible": False,
               "marks": rows, "roll_history": [], "provenance": {"tx_history_receipts": {"retained": {"sha256": "proof"}}}}
    path = tmp_path / "history.json"
    path.write_text(json.dumps(payload))
    return engine, path, payload


def test_retained_same_contract_continuation_has_exact_financial_parity(tmp_path):
    full, path, payload = retained(tmp_path)
    seed, rows, provenance = builder._retained_tx_prefix(path, expected_sha256=builder._sha256(path),
        session_dates=[date(2026, 11, 3), date(2026, 11, 4)])
    resumed = TwDayTradeSimulationEngine(tmp_path / "resumed")
    resumed.state["benchmarks"][TX_CONTINUOUS_BENCHMARK_ID] = seed
    assert rows == payload["marks"]
    assert provenance == payload["provenance"]
    for offset in range(300):
        at = datetime(2026, 11, 4, 8, 45, tzinfo=TAIPEI) + timedelta(minutes=offset)
        for engine in (full, resumed):
            engine._mark_tx_continuous_benchmark(current_contract_code="TXFK6", current_quote=quote(43400. + offset),
                previous_contract_quote={}, now=at)
        actual = builder._compact_benchmark_mark(builder._tx_engine_history_mark(resumed, observed=at))
        expected = builder._compact_benchmark_mark(builder._tx_engine_history_mark(full, observed=at))
        assert actual == expected


@pytest.mark.parametrize("tamper", ["sha", "duplicate", "gap", "wealth", "roll", "production", "scope"])
def test_retained_prefix_rejects_unproven_continuation(tmp_path, tamper):
    _engine, path, payload = retained(tmp_path)
    expected = builder._sha256(path)
    sessions = [date(2026, 11, 3), date(2026, 11, 4)]
    if tamper == "duplicate":
        payload["marks"].append(deepcopy(payload["marks"][-1]))
    elif tamper == "gap":
        payload["marks"].pop(20)
    elif tamper == "wealth":
        payload["marks"][-1]["buy_hold_wealth_index"] += .1
    elif tamper == "roll":
        payload["marks"][-1]["roll_count"] = 1
    elif tamper == "production":
        payload["production_order_possible"] = True
    elif tamper == "scope":
        sessions.insert(0, date(2026, 11, 2))
    path.write_text(json.dumps(payload))
    if tamper != "sha":
        expected = builder._sha256(path)
    else:
        expected = "0" * 64
    with pytest.raises(ValueError):
        builder._retained_tx_prefix(path, expected_sha256=expected, session_dates=sessions)
