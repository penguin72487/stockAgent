from dataclasses import replace
from datetime import timedelta
import json

import numpy as np
import pytest
import torch

from scripts.rebuild_tw_day_trade_open_price_replay import _replay_historical_intraday
from stockagent.backtest.tw_day_trade_inventory import (
    DayTradeInventoryState,
    append_inventory_fill,
    convert_inventory_to_margin,
    inventory_nav,
    inventory_path_nav,
    reduce_inventory_fifo_path,
)
from stockagent.data.tw_day_trade_schedule import paper_minute_opportunities
from stockagent.live.tw_day_trade_simulation import (
    TwDayTradeSimulationEngine,
    ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
)
from test_day_trade_margin_carry import action_reference, register
from test_tw_day_trade_inventory import assert_paper, v, DAY
from test_tw_day_trade_simulation import _spec, _now


def schedule(bars):
    return paper_minute_opportunities(
        bars,
        trading_date=np.datetime64("2026-08-13"),
        lower_limit=np.array([900.0]),
        upper_limit=np.array([1100.0]),
        security_types=np.array(["stock"]),
    )


@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize(
    "scenario",
    ["passive", "late_quote", "stop_latch", "both_brackets", "auction_only", "no_exit", "limit_boundary"],
)
def test_entire_session_matches_actual_paper_replay(tmp_path, direction, scenario):
    spec = replace(
        _spec(tmp_path),
        residual_margin_conversion=True,
        margin_corporate_action_reference_path=action_reference(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        price_limit_offset_ticks=1,
    )
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    register(engine, spec, 0, 0.9 * direction)
    mode = engine.state["modes"][spec.market]
    # Canonical minute replay sets this on registration. Synthetic high/low
    # quotes test limit crossing; valuation still uses the observed bar Close.
    mode["historical_minute_valuation"] = True
    position = next(iter(mode["positions"].values()))
    state = append_inventory_fill(
        DayTradeInventoryState.empty(1),
        signed_shares=v(position["signed_shares"]),
        price=v(position["entry_price"]),
        buy_fee_rate=v(position["buy_fee_rate"]),
        day_sell_fee_rate=v(position["sell_fee_rate"]),
        normal_sell_fee_rate=v(position["cash_sell_fee_rate"]),
        rebate_rate=v(position["commission_rebate_rate"]),
        day=DAY,
    )
    bars = np.tile([1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 2000.0], (1, 270, 1))
    if scenario == "passive":
        bars[0, 260:264, 1] = 1010
        bars[0, 260:264, 2] = 990
    elif scenario == "late_quote":
        bars[0, 259, :5], bars[0, 259, 5] = np.nan, 0
        bars[0, 260, :5] = 1005
        bars[0, 261, :5] = 1000
    elif scenario in {"stop_latch", "both_brackets"}:
        bars[0, 5, 1:3] = (
            [1100, 900]
            if scenario == "both_brackets"
            else ([1000, 900] if direction > 0 else [1100, 1000])
        )
        bars[0, 5, 5] = 1000  # trigger but no executable whole-lot capacity
    elif scenario == "auction_only":
        bars[0, :265, 5] = 0
        bars[0, 269, 5] = 4000
    elif scenario == "no_exit":
        bars[0, :, 5] = 0
    elif scenario == "limit_boundary":
        bars[0, 259:264, :5] = 1100 if direction > 0 else 900
    # The accepted opening position has a real 09:01 source observation.
    bars[0, 0, 5] = 2000
    # Huge trial-matching volume cannot close anything at 13:26..13:29.
    bars[0, 265:269, 5] = 1_000_000
    opportunities = schedule(bars)
    assert np.count_nonzero(opportunities.capacity_shares[:, 265:269]) == 0
    side = 0 if direction > 0 else 1
    result = reduce_inventory_fifo_path(
        state,
        prices=torch.from_numpy(opportunities.prices[..., side]),
        capacity_shares=torch.from_numpy(opportunities.capacity_shares[..., side]),
    )
    marks = opportunities.marks.copy()
    if scenario == "late_quote":
        assert opportunities.mark_source_index[0, 259] == 258
    minute_nav = inventory_path_nav(
        state,
        prices=torch.from_numpy(opportunities.prices[..., side]),
        minute_filled_shares=result.minute_filled_shares,
        marks=torch.from_numpy(marks),
        initial_capital=10_000_000,
    )
    state = convert_inventory_to_margin(result.reduction.state, day=DAY)
    minute_nav[-1] = inventory_nav(state, initial_capital=10_000_000, marks=v(1000))
    series = {}
    for m, values in enumerate(bars[0]):
        if not np.isfinite(values[:5]).all() or values[5] <= 0:
            continue
        stamp = (_now(9, 1) + timedelta(minutes=m)).isoformat(timespec="minutes")
        series[stamp] = dict(
            zip(
                ("open", "high", "low", "close", "vwap", "volume_shares"),
                values,
                strict=True,
            )
        )
    _replay_historical_intraday(
        engine,
        markets=[spec.market],
        bars={"2330": series},
        trading_date=_now(9, 0).date(),
    )
    assert_paper(state, mode, 1000)
    rows = [json.loads(line) for line in engine.marks_path.read_text().splitlines()]
    assert len(rows) == 270
    np.testing.assert_allclose(
        minute_nav.numpy(), [row["total_equity_twd"] for row in rows], rtol=0, atol=1e-7
    )


def test_minute_schedule_uses_dated_etf_ticks_not_stock_ticks():
    bars = np.tile([50.0, 50.05, 49.99, 50.0, 50.012345, 2000.0], (1, 270, 1))
    result = paper_minute_opportunities(
        bars,
        trading_date=np.datetime64("2026-08-13"),
        lower_limit=np.array([45.0]),
        upper_limit=np.array([55.0]),
        security_types=np.array(["etf"]),
    )
    assert result.prices[0, 264, 0] == 50.012345
    assert result.prices[0, 260, 0] == 50.05


@pytest.mark.parametrize('side,extreme', [(0, 900.), (1, 1100.)])
def test_zero_volume_padding_never_latches_stop_or_refreshes_mark(side, extreme):
    bars = np.tile([1000., 1000., 1000., 1000., 1000., 2000.], (1, 270, 1))
    bars[0, 5, :5], bars[0, 5, 5] = extreme, 0
    result = schedule(bars)
    assert np.isnan(result.prices[0, 5:259, side]).all()
    assert result.marks[0, 5] == 1000
    assert result.mark_source_index[0, 5] == 4
    assert result.mark_source_index[0, 6] == 6
    assert result.capacity_shares[0, 5, side] == 0


def test_sparse_marks_never_backfill_leading_0901_or_become_liquidity():
    bars = np.full((2, 270, 6), np.nan)
    bars[0, 2] = [1000., 1000., 1000., 1000., 1000., 2000.]
    result = paper_minute_opportunities(bars, trading_date=np.datetime64('2026-08-13'),
        lower_limit=np.array([900., 900.]), upper_limit=np.array([1100., 1100.]),
        security_types=np.array(['stock', 'stock']))
    assert np.isnan(result.marks[0, :2]).all()
    assert np.all(result.marks[0, 2:] == 1000)
    assert np.all(result.mark_source_index[0, 2:] == 2)
    assert np.isnan(result.marks[1]).all()
    assert np.all(result.mark_source_index[1] == -1)
    assert not result.capacity_shares.any()
    assert np.isnan(bars[0, 269, 3])  # original observations remain unchanged


@pytest.mark.parametrize("field,value", [(0, 1000.03), (4, 3000), (5, -1), (5, 2000.5)])
def test_malformed_rows_are_not_liquidity(field, value):
    bars = np.tile([1000.0, 1010.0, 990.0, 1000.0, 1000.1, 2000.0], (1, 270, 1))
    bars[0, 4, field] = value
    with pytest.raises(ValueError):
        schedule(bars)


def test_three_day_composed_executor_matches_paper_delta_orders_and_810_marks(tmp_path):
    from test_tw_day_trade_carry import run, session

    spec = replace(_spec(tmp_path), residual_margin_conversion=True,
        margin_corporate_action_reference_path=action_reference(tmp_path),
        entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE, price_limit_offset_ticks=1)
    engine = TwDayTradeSimulationEngine(tmp_path / "state")
    days, prices, weights = (0, 1, 4), (1000., 1010., 1020.), (.21, -.21, .1)
    sessions = [session(DAY + day, price, exits=idx == 2)
                for idx, (day, price) in enumerate(zip(days, prices))]
    expected = run(torch.tensor(weights, dtype=torch.float64).reshape(-1, 1), sessions)
    for idx, (day, price, weight) in enumerate(zip(days, prices, weights)):
        register(engine, spec, day, weight, volume=6, price=price, quote_updates={
            "lower_limit": sessions[idx].lower_limit.item(),
            "upper_limit": sessions[idx].upper_limit.item(),
        })
        mode = engine.state["modes"][spec.market]
        mode["historical_minute_valuation"] = True
        series = {}
        for minute in range(270):
            stamp = (_now(9, 1) + timedelta(days=day, minutes=minute)).isoformat(timespec="minutes")
            series[stamp] = dict(open=price, high=price, low=price, close=price, vwap=price,
                                volume_shares=6000 if idx == 2 or minute == 0 else 0)
        _replay_historical_intraday(engine, markets=[spec.market], bars={"2330": series},
                                   trading_date=(_now(9, 0) + timedelta(days=day)).date())
        assert sum(int(p.get("signed_shares") or 0) for p in mode["positions"].values()) == expected.shares_history[idx, 0]
    rows = [json.loads(line) for line in engine.marks_path.read_text().splitlines()]
    assert len(rows) == 810
    np.testing.assert_allclose(expected.minute_nav.numpy().reshape(-1),
        [row["total_equity_twd"] for row in rows], rtol=0, atol=1e-7)
