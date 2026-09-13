from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from stockagent.data.tw_price_rules import (
    move_price_ticks_numpy,
    price_on_tick_grid_numpy,
    quantize_order_price_numpy,
    tick_size_numpy,
)
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.live.tw_day_trade_simulation import quote_map_from_snapshot


def test_stock_and_etf_price_buckets_are_not_interchangeable():
    prices = np.array([9.99, 10, 49.95, 50, 99.9, 100, 499.5, 500, 999, 1000])
    np.testing.assert_array_equal(tick_size_numpy(prices), [.01, .05, .05, .1, .1, .5, .5, 1, 1, 5])
    np.testing.assert_array_equal(tick_size_numpy(prices, security_types="etf"), [.01]*3 + [.05]*7)


@pytest.mark.parametrize("kind,price,down,up", [
    ("stock", 10, 9.99, 10.05), ("stock", 50, 49.95, 50.1),
    ("stock", 100, 99.9, 100.5), ("stock", 500, 499.5, 501),
    ("stock", 1000, 999, 1005), ("etf", 50, 49.99, 50.05),
])
def test_tick_crossing_each_bucket_recomputes_the_next_tick(kind, price, down, up):
    for count, expected in [(-1, down), (1, up)]:
        result = move_price_ticks_numpy(np.array([price]), count, security_types=kind)
        assert result[0] == expected
        assert price_on_tick_grid_numpy(result, security_types=kind).all()
    assert move_price_ticks_numpy(np.array([down]), 1, security_types=kind)[0] == price
    assert move_price_ticks_numpy(np.array([up]), -1, security_types=kind)[0] == price


def test_invalid_tick_cannot_be_accepted_by_rounding_to_two_decimals():
    prices = np.array([10.03, 49.91, 50.01, 100.1, 500.5, 1001, np.nan, -1, 0])
    assert not price_on_tick_grid_numpy(prices).any()
    assert price_on_tick_grid_numpy(np.array([49.91, 50.05]), security_types="etf").all()
    assert price_on_tick_grid_numpy(np.array([49.9], dtype=np.float32)).all()
    assert not price_on_tick_grid_numpy(np.array([49.91], dtype=np.float32)).any()


def test_only_calculated_order_bounds_are_directionally_quantized():
    values = np.array([49.999, 99.999, 999.999, .005])
    np.testing.assert_allclose(quantize_order_price_numpy(values, "up"), [50, 100, 1000, .01])
    np.testing.assert_allclose(quantize_order_price_numpy(values, "down"), [49.95, 99.9, 999, np.nan])
    np.testing.assert_allclose(quantize_order_price_numpy(values[:1], "down", security_types="etf"), [49.99])
    with pytest.raises(ValueError, match="rounding"):
        quantize_order_price_numpy(values, "nearest")
    with pytest.raises(ValueError, match="stock or etf"):
        tick_size_numpy(values, security_types="warrant")


def test_quote_gate_rejects_invalid_stock_prices_without_mutating_provider():
    # The exact same 50.05 is illegal for a stock and legal for an ETF.
    bid = np.array([50.05, 50.05])
    snapshot = PriceSnapshot(prices=np.array([50.037123, 50.037123]), source="test",
                             bid_prices=bid, ask_prices=np.array([50.1, 50.1]),
                             reference_prices=np.array([50., 50.]))
    rows = quote_map_from_snapshot(["2330", "0050"], snapshot, trading_date=date(2026, 9, 10))
    assert rows["2330"]["bid"] is None
    assert rows["2330"]["invalid_order_price_fields"] == ["bid"]
    assert rows["0050"]["bid"] == 50.05
    assert rows["0050"]["upper_limit"] is None  # An ETF is not always limited to 10%.
    assert rows["2330"]["upper_limit"] == 55
    assert rows["0050"]["last"] == 50.037123  # Mark/average is not an order price.
    np.testing.assert_array_equal(bid, [50.05, 50.05])


def test_source_float32_representation_noise_only_is_normalized():
    snapshot = PriceSnapshot(prices=np.array([49.9]), source="test",
                             bid_prices=np.array([49.9], dtype=np.float32))
    row = quote_map_from_snapshot(["2330"], snapshot, trading_date=date(2026, 9, 10))["2330"]
    assert row["bid"] == 49.9
    assert row["invalid_order_price_fields"] == []


@pytest.mark.parametrize("side,expected", [("long", 50.05), ("short", 49.99)])
def test_historical_etf_passive_quote_uses_its_own_tick(side, expected):
    from scripts.rebuild_tw_day_trade_open_price_replay import _eod_kbar_quotes

    observed = datetime(2026, 9, 10, 13, 20, tzinfo=ZoneInfo("Asia/Taipei"))
    position = dict(symbol="0050", security_type="etf", side=side,
                    lower_limit=45., upper_limit=55.,
                    signed_shares=751 if side == "long" else -751)
    bars = {"0050": {observed.isoformat(timespec="minutes"):
                    dict(close=50., open=50., high=50.1, low=49.9, vwap=50.037123,
                         volume_shares=10000., volume_lots=10.)}}
    row = _eod_kbar_quotes({"positions": {"p": position}}, bars, observed=observed)["0050"]
    assert row["ask" if side == "long" else "bid"] == expected
