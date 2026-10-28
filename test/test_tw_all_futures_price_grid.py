"""Historical product dispatch must not bless prices using another grid."""

import numpy as np
import pytest

from stockagent.data.tw_price_rules import (
    TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION,
    TW_DERIVATIVE_PRICE_CONTRACT_VERSION,
    price_on_taifex_futures_tick_grid_numpy,
    taifex_futures_day_session_minutes,
    taifex_futures_tick_size_numpy,
    taifex_index_future_tick_size_numpy,
)


@pytest.mark.parametrize("product,tick", [
    ("TX", 1), ("MTX", 1), ("TE", .05), ("TF", .2), ("T5F", 1),
    ("GTF", .05), ("XIF", 1), ("TJF", .25), ("UDF", 1), ("SPF", .25),
    ("UNF", 1), ("G2F", 1), ("E4F", 1), ("BTF", 1), ("F1F", 1),
    ("ZEF", .05), ("ZFF", .2), ("SOF", 1), ("SHF", .05), ("SXF", .5),
    ("TMF", 1), ("M1F", 1),
])
def test_all_selected_index_products_validate_their_own_grid(product, tick):
    day = "2022-07-19" if product == "T5F" else "2026-09-04"
    prices = np.array([100 + tick, 100 + tick / 2])
    actual = price_on_taifex_futures_tick_grid_numpy(
        prices, day, product_codes=product, asset_classes="index_future",
    )
    assert actual.tolist() == [True, False]
    assert taifex_futures_tick_size_numpy(
        prices, day, product_codes=product, asset_classes="index_future",
    ).tolist() == [tick, tick]


@pytest.mark.parametrize("product,start", [
    ("E4F", "2020-06-08"), ("BTF", "2020-06-08"), ("F1F", "2020-11-23"),
    ("ZEF", "2021-06-28"), ("ZFF", "2021-12-06"), ("SOF", "2022-06-27"),
    ("SHF", "2022-06-27"), ("SXF", "2023-12-18"), ("TMF", "2024-07-29"),
    ("M1F", "2024-12-09"),
])
def test_new_index_product_cannot_be_backfilled_before_launch(product, start):
    assert price_on_taifex_futures_tick_grid_numpy(
        np.array([100.]), start, product_codes=product, asset_classes="index_future",
    ).item()
    with pytest.raises(ValueError, match="product window"):
        taifex_futures_tick_size_numpy(
            np.array([100.]), np.datetime64(start) - np.timedelta64(1, "D"),
            product_codes=product, asset_classes="index_future",
        )


def test_t5f_delisting_and_registry_window_are_fail_closed():
    assert taifex_futures_day_session_minutes(
        "2022-09-21", product_code="T5F", asset_class="index_future",
        is_last_trading_day=True,
    ) == (525, 810)
    with pytest.raises(ValueError, match="product window"):
        taifex_futures_day_session_minutes(
            "2022-09-22", product_code="T5F", asset_class="index_future",
        )
    for day in ["2011-01-02", "2026-09-28"]:
        with pytest.raises(ValueError, match="historical window"):
            taifex_futures_day_session_minutes(day, product_code="TX", asset_class="index_future")
    for day in [None, "NaT"]:
        with pytest.raises(ValueError, match="trading date"):
            taifex_futures_day_session_minutes(day, product_code="TX", asset_class="index_future")


@pytest.mark.parametrize("product", [
    "NYF", "PFF", "RIF", "RYF", "SMF", "SNF", "SRF", "SSF", "SUF", "VHF",
    "NZF", "OAF", "OBF", "OCF", "OJF", "OKF", "OOF", "RXF", "RZF",
    "SGF", "SIF", "SQF", "UJF", "UKF", "URF", "USF",
])
def test_selected_etfs_keep_etf_bands_instead_of_stock_bands(product):
    assert price_on_taifex_futures_tick_grid_numpy(
        np.array([49.99, 50.00, 50.05, 50.01]), "2026-09-04",
        product_codes=product, asset_classes="etf_future",
    ).tolist() == [True, True, True, False]


@pytest.mark.parametrize('product', ['NY1', 'SR1'])
def test_0050_adjusted_etfs_inherit_only_dated_price_family(product):
    assert price_on_taifex_futures_tick_grid_numpy(
        np.array([49.99,50.05,50.01]),'2025-06-18',product_codes=product,
        asset_classes='etf_future').tolist() == [True,True,False]
    assert taifex_futures_day_session_minutes('2025-07-18',product_code=product,
        asset_class='etf_future') == (525,825)
    for day in ['2025-06-17','2026-04-01']:
        with pytest.raises(ValueError,match='adjusted ETF window'):
            taifex_futures_tick_size_numpy(np.array([50.]),day,
                product_codes=product,asset_classes='etf_future')
    with pytest.raises(ValueError,match='disagree'):
        taifex_futures_tick_size_numpy(np.array([50.]),'2025-07-18',
            product_codes=product,asset_classes='stock_future')


def test_mixed_product_array_broadcast_and_stock_amendment():
    prices = np.array([[1001., 50.05, 100.05], [1001., 50.05, 100.05]])
    actual = price_on_taifex_futures_tick_grid_numpy(
        prices, np.array(["2026-07-03", "2026-07-06"])[:, None],
        product_codes=["DFF", "NYF", "TE"],
        asset_classes=["stock_future", "etf_future", "index_future"],
    )
    assert actual.tolist() == [[False, True, True], [True, True, True]]
    assert not price_on_taifex_futures_tick_grid_numpy(
        np.array([100.037123, np.nan, 0.0]), "2026-09-04",
        product_codes="TE", asset_classes="index_future",
    ).any()


@pytest.mark.parametrize("product,kind,error", [
    ("UNKNOWN", "index_future", "unsupported"),
    ("UNKNOWN", "etf_future", "unsupported"),
    ("TX", "etf_future", "disagree"),
    ("OCF", "stock_future", "disagree"),
    ("TX", "stock", "asset class"),
    (None, "stock_future", "product code"),
])
def test_unknown_or_conflicting_metadata_is_never_accepted(product, kind, error):
    with pytest.raises(ValueError, match=error):
        price_on_taifex_futures_tick_grid_numpy(
            np.array([100.]), "2026-09-04", product_codes=product, asset_classes=kind,
        )


def test_day_session_product_exceptions_do_not_change_strategy_event_clock():
    # OCF's historical 16:12 prints are day-session evidence, not night trades.
    assert taifex_futures_day_session_minutes(
        "2020-03-23", product_code="OCF", asset_class="etf_future",
    ) == (525, 975)
    assert taifex_futures_day_session_minutes(
        "2020-03-23", product_code="TJF", asset_class="index_future",
    ) == (480, 975)
    for product, kind in [("NYF", "etf_future"), ("TX", "index_future"), ("DFF", "stock_future")]:
        assert taifex_futures_day_session_minutes(
            "2020-03-23", product_code=product, asset_class=kind,
        ) == (525, 825)
    for product in ["UDF", "SPF", "UNF", "F1F", "SXF"]:
        assert taifex_futures_day_session_minutes(
            "2026-09-04", product_code=product, asset_class="index_future",
            is_last_trading_day=True,
        ) == (525, 825)
    assert taifex_futures_day_session_minutes(
        "2020-04-15", product_code="OCF", asset_class="etf_future", is_last_trading_day=True,
    ) == (525, 810)


def test_float32_input_and_legacy_index_api_keep_existing_contract():
    assert price_on_taifex_futures_tick_grid_numpy(
        np.array([100.05], dtype=np.float32), "2020-03-23",
        product_codes="te", asset_classes="index_future",
    ).item()
    assert taifex_index_future_tick_size_numpy(
        np.array([12345.]), product_codes="TMF",
    ).item() == 1.
    assert TW_DERIVATIVE_PRICE_CONTRACT_VERSION == 2
    assert TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION == 7


def test_stock_forward_rule_is_bounded_and_does_not_extend_other_asset_classes():
    prices = np.array([1001., 2500., 2501.])
    assert price_on_taifex_futures_tick_grid_numpy(prices, '2026-10-27',
        product_codes='PK1', asset_classes='stock_future').tolist() == [True, True, False]
    assert taifex_futures_day_session_minutes('2026-10-27', product_code='PK1',
        asset_class='stock_future') == (525, 825)
    for product, kind, day in [('PK1', 'stock_future', '2026-10-28'),
                               ('TX', 'index_future', '2026-09-28'),
                               ('NYF', 'etf_future', '2026-09-28')]:
        with pytest.raises(ValueError, match='historical window'):
            taifex_futures_day_session_minutes(day, product_code=product, asset_class=kind)


def test_earlier_registry_extension_keeps_unreviewed_products_and_periods_closed():
    for product in ["TX", "MTX"]:
        assert taifex_futures_tick_size_numpy(np.array([9001.]), "2011-01-03",
            product_codes=product, asset_classes="index_future").item() == 1.
    for product, kind, day in [('TJF', 'index_future', '2019-01-03'),
                               ('NYF', 'etf_future', '2014-10-03')]:
        with pytest.raises(ValueError, match="historical window"):
            taifex_futures_tick_size_numpy(np.array([100.]), day,
                product_codes=product, asset_classes=kind)


@pytest.mark.parametrize('product,tick', [('TE', .05), ('TF', .2), ('T5F', 1.), ('GTF', .05), ('XIF', 1.)])
def test_2011_official_domestic_index_ticks_and_day_sessions(product, tick):
    assert taifex_futures_tick_size_numpy(np.array([100.]), '2011-02-18',
        product_codes=product, asset_classes='index_future').item() == tick
    assert taifex_futures_day_session_minutes('2011-02-18', product_code=product,
        asset_class='index_future') == (525, 825)
    with pytest.raises(ValueError, match='historical window'):
        taifex_futures_day_session_minutes('2010-12-31', product_code=product, asset_class='index_future')


def test_2014_etf_bands_and_2015_session_amendment_have_separate_effective_dates():
    assert taifex_futures_tick_size_numpy(np.array([49.99, 50.]), '2014-10-07',
        product_codes='NZF', asset_classes='etf_future').tolist() == [.01, .05]
    assert taifex_futures_day_session_minutes('2015-07-17', product_code='NZF', asset_class='etf_future') == (525, 825)
    assert taifex_futures_day_session_minutes('2015-07-20', product_code='NZF', asset_class='etf_future') == (525, 975)
    assert taifex_futures_day_session_minutes('2015-07-20', product_code='NZF', asset_class='etf_future',
        is_last_trading_day=True) == (525, 810)
    assert taifex_futures_day_session_minutes('2015-07-20', product_code='NYF', asset_class='etf_future') == (525, 825)


def test_2011_stock_futures_keep_historical_bands_and_dated_day_session():
    prices = np.array([9.99, 10.05, 50.1, 100.5, 501., 1005.])
    assert taifex_futures_tick_size_numpy(prices, "2011-01-03",
        product_codes="CDF", asset_classes="stock_future").tolist() == [.01, .05, .1, .5, 1., 5.]
    assert price_on_taifex_futures_tick_grid_numpy(np.array([1005., 1001.]), "2011-01-03",
        product_codes="CDF", asset_classes="stock_future").tolist() == [True, False]
    assert taifex_futures_day_session_minutes("2011-01-03", product_code="CDF",
        asset_class="stock_future") == (525, 825)
    assert taifex_futures_day_session_minutes("2011-01-19", product_code="CDF",
        asset_class="stock_future", is_last_trading_day=True) == (525, 810)
    with pytest.raises(ValueError, match="historical window"):
        taifex_futures_day_session_minutes("2010-12-31", product_code="CDF", asset_class="stock_future")


def test_g2f_launch_registry_does_not_inherit_a_later_archive_start():
    assert price_on_taifex_futures_tick_grid_numpy(np.array([5501., 5501.5]), "2019-09-30",
        product_codes="G2F", asset_classes="index_future").tolist() == [True, False]
    assert taifex_futures_day_session_minutes("2020-03-19", product_code="G2F",
        asset_class="index_future") == (525, 825)
    assert taifex_futures_day_session_minutes("2019-10-16", product_code="G2F",
        asset_class="index_future", is_last_trading_day=True) == (525, 810)
    with pytest.raises(ValueError, match="historical window"):
        taifex_futures_day_session_minutes("2019-09-27", product_code="G2F", asset_class="index_future")
