from __future__ import annotations

from datetime import date, datetime, time

import numpy as np
import pytest

from scripts.backtest_taifex_atm_straddle_rolling import (
    DayMarket,
    Fill,
    OptionContract,
    STRATEGY_CLASSIC,
    STRATEGY_ROLL_OTM,
    _ns,
    _simulate_variant,
    _trade_row,
)
from stockagent.data.tw_index_derivatives_tick import TAIPEI


def _event(second: int) -> int:
    return _ns(datetime(2026, 8, 6, 8, 50, second, tzinfo=TAIPEI))


def _at(hour: int, minute: int, second: int) -> int:
    return _ns(datetime(2026, 8, 6, hour, minute, second, tzinfo=TAIPEI))


def test_cached_chain_selection_is_point_in_time_and_series_scoped() -> None:
    market = DayMarket(
        trading_date=date(2026, 8, 6),
        tx_times_ns=np.asarray([_event(0)], dtype=np.int64),
        tx_prices=np.asarray([20_040.0]),
        option_events={},
        pair_availability=(
            (_event(1), date(2026, 8, 7), "202608F1", 20_000.0),
            (_event(3), date(2026, 8, 7), "202608F1", 20_050.0),
            (_event(1), date(2026, 8, 12), "202608W2", 20_050.0),
        ),
    )

    with pytest.raises(ValueError, match="no causal Call/Put pair"):
        market.select_atm_pair(decision_ns=_event(0), underlying_price=20_040.0)

    assert market.select_atm_pair(
        decision_ns=_event(2), underlying_price=20_040.0
    ) == ("202608F1", date(2026, 8, 7), 20_000.0)
    assert market.select_atm_pair(
        decision_ns=_event(3),
        underlying_price=20_040.0,
        required_series="202608F1",
    ) == ("202608F1", date(2026, 8, 7), 20_050.0)


def test_trade_costs_reconcile_fee_tax_slippage_and_direction() -> None:
    common = {
        "trading_date": date(2026, 8, 6),
        "strategy": STRATEGY_ROLL_OTM,
        "rolling_points": 50,
        "contract": OptionContract("202608F1", 20_000.0, "C"),
        "fill": Fill(event_ns=_event(2), price=100.0),
        "reason": "test",
        "decision_ns": _event(1),
        "multiplier": 50.0,
        "fee_per_side": 22.0,
        "tax_rate": 0.001,
        "slippage_points": 0.5,
    }
    buy = _trade_row(delta_contracts=1, **common)
    sell = _trade_row(delta_contracts=-1, **common)

    assert buy["fixed_fee_twd"] == pytest.approx(22.0)
    assert buy["transaction_tax_twd"] == pytest.approx(5.0)
    assert buy["slippage_cost_twd"] == pytest.approx(25.0)
    assert buy["net_cash_flow_twd"] == pytest.approx(-5_052.0)
    assert sell["net_cash_flow_twd"] == pytest.approx(4_948.0)
    assert buy["fill_delay_seconds"] == pytest.approx(1.0)


def test_classic_opening_straddle_reuses_open_and_terminal_path_without_rolls() -> None:
    call = OptionContract("202608W2", 20_000.0, "C")
    put = OptionContract("202608W2", 20_000.0, "P")
    option_times = np.asarray(
        [_at(8, 49, 59), _at(8, 50, 1), _at(13, 41, 0), _at(13, 44, 0)],
        dtype=np.int64,
    )
    market = DayMarket(
        trading_date=date(2026, 8, 6),
        tx_times_ns=np.asarray(
            [_at(8, 49, 59), _at(8, 50, 0), _at(9, 0, 0), _at(13, 44, 0)],
            dtype=np.int64,
        ),
        tx_prices=np.asarray([20_000.0, 20_000.0, 21_000.0, 21_000.0]),
        option_events={
            call: (option_times, np.asarray([100.0, 101.0, 140.0, 145.0])),
            put: (option_times, np.asarray([100.0, 99.0, 55.0, 50.0])),
        },
        pair_availability=(
            (_at(8, 49, 59), date(2026, 8, 12), "202608W2", 20_000.0),
        ),
    )

    daily, trades = _simulate_variant(
        market,
        strategy=STRATEGY_CLASSIC,
        rolling_points=0,
        selection_time=time(8, 50),
        close_decision_time=time(13, 40),
        session_end_time=time(13, 45),
        multiplier=50.0,
        fee_per_side=22.0,
        tax_rate=0.001,
        slippage_points=0.0,
    )

    assert daily["roll_count"] == 0
    assert daily["trade_sides"] == 4
    assert daily["slippage_cost_twd"] == pytest.approx(0.0)
    assert daily["net_after_fee_twd"] == pytest.approx(
        daily["gross_pnl_twd"] - 4 * 22.0
    )
    assert {row["reason"] for row in trades} == {
        "open_atm_straddle",
        "daily_terminal_last_trade_mark",
    }
    assert daily["strict_terminal_executable"] is True

    with pytest.raises(ValueError, match="no-roll threshold"):
        _simulate_variant(
            market,
            strategy=STRATEGY_CLASSIC,
            rolling_points=50,
            selection_time=time(8, 50),
            close_decision_time=time(13, 40),
            session_end_time=time(13, 45),
            multiplier=50.0,
            fee_per_side=22.0,
            tax_rate=0.001,
            slippage_points=0.0,
        )
