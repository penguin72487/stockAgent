from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from stockagent.data.tw_price_rules import (
    move_price_ticks_numpy,
    price_on_tick_grid_numpy,
    price_on_explicit_tick_grid_numpy,
    quantize_order_price_numpy,
    taifex_index_future_tick_size_numpy,
    taifex_option_tick_size_numpy,
    tick_size_numpy,
)
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.live.tw_day_trade_simulation import quote_map_from_snapshot


def test_stock_and_etf_price_buckets_are_not_interchangeable():
    prices = np.array([9.99, 10, 49.95, 50, 99.9, 100, 499.5, 500, 999, 1000])
    np.testing.assert_array_equal(tick_size_numpy(prices), [.01, .05, .05, .1, .1, .5, .5, 1, 1, 5])
    np.testing.assert_array_equal(tick_size_numpy(prices, security_types="etf"), [.01]*3 + [.05]*7)


def test_stock_futures_2026_change_is_dated_and_does_not_rewrite_cash_stock():
    prices = np.array([999., 1000., 1001., 2499., 2500.])
    before = np.datetime64("2026-07-03")
    after = np.datetime64("2026-07-06")
    np.testing.assert_array_equal(
        tick_size_numpy(prices, before, security_types="stock_future"),
        [1., 5., 5., 5., 5.],
    )
    np.testing.assert_array_equal(
        tick_size_numpy(prices, after, security_types="stock_future"),
        [1., 1., 1., 1., 5.],
    )
    np.testing.assert_array_equal(
        tick_size_numpy(prices, after, security_types="stock"),
        [1., 5., 5., 5., 5.],
    )
    assert not price_on_tick_grid_numpy(
        np.array([1001.]), before, security_types="stock_future"
    ).item()
    assert price_on_tick_grid_numpy(
        np.array([1001.]), after, security_types="stock_future"
    ).item()
    assert quantize_order_price_numpy(
        np.array([1001.2]), "down", after, security_types="stock_future"
    ).item() == 1001.


def test_futures_require_real_listing_date_and_etf_future_has_own_grid():
    assert tick_size_numpy(
        np.array([50.]), np.datetime64("2014-10-06"), security_types="etf_future"
    ).item() == .05
    with pytest.raises(ValueError, match="not listed"):
        tick_size_numpy(np.array([10.]), np.datetime64("2010-01-22"), security_types="stock_future")
    with pytest.raises(ValueError, match="trading date"):
        tick_size_numpy(np.array([10.]), np.datetime64("NaT"), security_types="stock_future")


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
    with pytest.raises(ValueError, match="unsupported"):
        tick_size_numpy(values, security_types="unknown_product")


def test_cash_exchange_product_families_keep_their_own_dated_grids():
    assert tick_size_numpy(
        np.array([10.0]), np.datetime64("2005-02-28"), security_types="warrant"
    ).item() == 0.05
    assert tick_size_numpy(
        np.array([10.0]), np.datetime64("2005-03-01"), security_types="warrant"
    ).item() == 0.1
    assert tick_size_numpy(
        np.array([100.0]), np.datetime64("2006-03-03"), security_types="reit"
    ).item() == 0.5
    assert tick_size_numpy(
        np.array([100.0]), np.datetime64("2006-03-06"), security_types="reit"
    ).item() == 0.05
    assert tick_size_numpy(
        np.array([149.95, 150.0, 1000.0]), security_types="convertible_bond"
    ).tolist() == [0.05, 1.0, 5.0]


def test_emerging_stock_grid_changes_on_2020_03_23_and_listing_is_separate():
    from stockagent.data.tw_exchange_price_classification import (
        classify_tw_broker_security_on_date,
    )
    assert classify_tw_broker_security_on_date("tpex", "2743", date(2020, 3, 6)) == "emerging_stock"
    assert classify_tw_broker_security_on_date("tpex", "2743", date(2020, 3, 9)) == "stock"
    assert classify_tw_broker_security_on_date("tpex", "6716", date(2020, 3, 23)) == "emerging_stock"
    assert classify_tw_broker_security_on_date("tpex", "6716", date(2020, 3, 27)) == "stock"
    assert price_on_tick_grid_numpy(
        np.array([55.99]), np.datetime64("2020-03-20"), security_types="emerging_stock"
    ).item()
    assert not price_on_tick_grid_numpy(
        np.array([55.99]), np.datetime64("2020-03-23"), security_types="emerging_stock"
    ).item()
    with pytest.raises(ValueError, match="trading date"):
        tick_size_numpy(np.array([55.99]), security_types="emerging_stock")


def test_taifex_index_futures_and_option_premiums_are_distinct_from_strikes():
    future_prices = np.array([23456.0, 23456.5])
    ticks = taifex_index_future_tick_size_numpy(
        future_prices, product_codes=np.array(["TX", "TMF"])
    )
    assert price_on_explicit_tick_grid_numpy(future_prices, ticks).tolist() == [True, False]

    txo = np.array([9.9, 10.0, 49.5, 50.0, 499.0, 500.0, 1000.0])
    np.testing.assert_array_equal(
        taifex_option_tick_size_numpy(
            txo, np.datetime64("2026-09-18"), product_families="txo"
        ),
        [0.1, 0.5, 0.5, 1.0, 1.0, 5.0, 10.0],
    )
    assert taifex_option_tick_size_numpy(
        np.array([0.49, 0.5, 2.5, 25.0, 50.0]),
        np.datetime64("2025-12-05"),
        product_families="teo",
    ).tolist() == [0.005, 0.025, 0.05, 0.25, 0.5]
    assert taifex_option_tick_size_numpy(
        np.array([1.98, 2.0, 10.0, 100.0, 200.0]),
        np.datetime64("2025-12-08"),
        product_families="teo",
    ).tolist() == [0.02, 0.1, 0.2, 1.0, 2.0]


def test_txo_block_trade_2019_grid_does_not_change_ordinary_options():
    price = np.array([50.1])
    day = np.datetime64("2019-05-27")
    ordinary = taifex_option_tick_size_numpy(price, day, product_families="txo")
    block = taifex_option_tick_size_numpy(
        price, day, product_families="txo", trading_method="block"
    )
    assert not price_on_explicit_tick_grid_numpy(price, ordinary).item()
    assert price_on_explicit_tick_grid_numpy(price, block).item()
    with pytest.raises(ValueError, match="starts on 2019-05-27"):
        taifex_option_tick_size_numpy(
            price, np.datetime64("2019-05-24"),
            product_families="txo", trading_method="block",
        )


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
