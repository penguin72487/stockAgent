from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime
import math

import numpy as np
import pytest

from stockagent.backtest.tw_execution import (
    TaiwanFeeSchedule,
    TaiwanMarginShortSchedule,
    commission_rebate_rate_vector,
    effective_fee_rate_vectors,
    gross_fee_rate_vectors,
    lot_size_vector,
    normalize_execution_mode,
    official_tw_short_initial_margin_rates,
    settlement_session_indices,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("naive", "naive"),
        (" legacy ", "naive"),
        ("tw_cash", "tw_cash"),
        ("TW-CASH", "tw_cash"),
        ("taiwan cash", "tw_cash"),
        ("現股", "tw_cash"),
        ("tw_day_trade", "tw_day_trade"),
        ("TW-DAY-TRADE", "tw_day_trade"),
        ("day trade", "tw_day_trade"),
        ("intraday", "tw_day_trade"),
        ("當沖", "tw_day_trade"),
    ],
)
def test_normalize_execution_mode_accepts_explicit_aliases(
    raw: str, expected: str
) -> None:
    assert normalize_execution_mode(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "unknown", "tw_margin", 1, object()])
def test_normalize_execution_mode_fails_closed(raw: object) -> None:
    with pytest.raises(ValueError, match="execution_mode"):
        normalize_execution_mode(raw)


def test_fee_schedule_defaults_are_exact_and_broker_discount_is_separate() -> None:
    schedule = TaiwanFeeSchedule()

    assert schedule.commission_rate == 0.001425
    assert schedule.commission_discount == 0.2
    assert schedule.commission_rebate_timing == "monthly_15th"
    assert schedule.stock_sell_tax == 0.003
    assert schedule.etf_sell_tax == 0.001
    assert schedule.day_trade_stock_sell_tax == 0.0015
    assert schedule.day_trade_etf_sell_tax == 0.001
    assert schedule.minimum_commission == 0.0
    assert schedule.commission_rounding == "none"
    assert schedule.tax_rounding == "none"
    assert schedule.settlement_lag_sessions == 2
    assert schedule.cash_lot_size == 1
    assert schedule.day_trade_default_lot_size == 1000
    assert math.isclose(schedule.effective_commission_rate, 0.000285)
    assert math.isclose(schedule.commission_rebate_rate, 0.00114)

    with pytest.raises(FrozenInstanceError):
        schedule.commission_discount = 1.0  # type: ignore[misc]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("monthly_15th", "monthly_15th"),
        (" monthly-15th ", "monthly_15th"),
        ("MONTHLY", "monthly_15th"),
        ("月退", "monthly_15th"),
        ("daily_close", "daily_close"),
        (" daily-close ", "daily_close"),
        ("DAILY", "daily_close"),
        ("日退", "daily_close"),
    ],
)
def test_fee_schedule_normalizes_commission_rebate_timing(
    raw: str,
    expected: str,
) -> None:
    schedule = TaiwanFeeSchedule(commission_rebate_timing=raw)

    assert schedule.commission_rebate_timing == expected


@pytest.mark.parametrize("bad_value", [None, "", "immediate", "weekly", 1, True])
def test_fee_schedule_rejects_unknown_commission_rebate_timing(
    bad_value: object,
) -> None:
    with pytest.raises(ValueError, match="commission_rebate_timing"):
        TaiwanFeeSchedule(commission_rebate_timing=bad_value)  # type: ignore[arg-type]


def test_margin_short_schedule_defaults_are_checkpoint_safe_and_broker_neutral() -> (
    None
):
    schedule = TaiwanMarginShortSchedule()

    assert schedule.initial_margin_rate == 0.9
    assert schedule.maintenance_ratio == 1.3
    assert schedule.lot_size == 1000
    assert schedule.handling_fee_rate == 0.0

    with pytest.raises(FrozenInstanceError):
        schedule.initial_margin_rate = 1.2  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("initial_margin_rate", 0.0),
        ("initial_margin_rate", -0.1),
        ("initial_margin_rate", math.nan),
        ("initial_margin_rate", math.inf),
        ("initial_margin_rate", True),
        ("maintenance_ratio", 1.0),
        ("maintenance_ratio", math.nan),
        ("handling_fee_rate", -0.01),
        ("handling_fee_rate", 1.01),
    ],
)
def test_margin_short_schedule_rejects_invalid_ratios_and_rates(
    field_name: str,
    bad_value: object,
) -> None:
    with pytest.raises(ValueError):
        replace(TaiwanMarginShortSchedule(), **{field_name: bad_value})


@pytest.mark.parametrize("bad_lot_size", [0, -1, 1000.5, math.inf, True, "1000"])
def test_margin_short_schedule_rejects_invalid_lot_size(bad_lot_size: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        TaiwanMarginShortSchedule(lot_size=bad_lot_size)  # type: ignore[arg-type]


def test_official_margin_rate_helper_covers_raise_and_restore_boundaries() -> None:
    dates = np.asarray(
        [
            "2015-08-12",
            "2015-08-13",
            "2015-10-15",
            "2015-10-16",
            "2016-01-07",
            "2016-01-08",
            "2016-02-29",
            "2016-03-01",
            "2022-10-11",
            "2022-10-12",
            "2023-02-23",
            "2023-02-24",
            "2025-04-06",
            "2025-04-07",
            "2025-05-25",
            "2025-05-26",
        ],
        dtype="datetime64[D]",
    )

    rates = official_tw_short_initial_margin_rates(dates)

    assert rates.dtype == np.float64
    assert rates.shape == dates.shape
    np.testing.assert_array_equal(
        rates,
        [
            0.9,
            1.2,
            1.2,
            0.9,
            0.9,
            1.2,
            1.2,
            0.9,
            0.9,
            1.2,
            1.2,
            0.9,
            0.9,
            1.3,
            1.3,
            0.9,
        ],
    )


@pytest.mark.parametrize(
    "date_like",
    [np.datetime64("2025-04-07"), date(2025, 4, 7), datetime(2025, 4, 7, 15, 30)],
)
def test_official_margin_rate_helper_accepts_scalar_date_like(
    date_like: object,
) -> None:
    rate = official_tw_short_initial_margin_rates(date_like)

    assert isinstance(rate, np.ndarray)
    assert rate.shape == ()
    assert rate.item() == 1.3


def test_configured_margin_floor_does_not_hide_official_temporary_increases() -> None:
    schedule = TaiwanMarginShortSchedule(initial_margin_rate=1.0)

    rates = schedule.marketwide_initial_margin_rates(
        np.asarray(["2024-01-02", "2025-04-07"], dtype="datetime64[D]")
    )

    np.testing.assert_array_equal(rates, [1.0, 1.3])


@pytest.mark.parametrize(
    "bad_dates",
    [np.datetime64("NaT"), ["2025-04-07", None], [1, 2], 1, "not-a-date"],
)
def test_official_margin_rate_helper_rejects_missing_numeric_or_invalid_dates(
    bad_dates: object,
) -> None:
    with pytest.raises(ValueError, match="date|NaT|missing"):
        official_tw_short_initial_margin_rates(bad_dates)


@pytest.mark.parametrize(
    "field_name",
    [
        "commission_rate",
        "commission_discount",
        "stock_sell_tax",
        "etf_sell_tax",
        "day_trade_stock_sell_tax",
        "day_trade_etf_sell_tax",
        "minimum_commission",
    ],
)
@pytest.mark.parametrize(
    "bad_value", [-0.0001, math.nan, math.inf, -math.inf, True, "0.1"]
)
def test_fee_schedule_rejects_invalid_rates(field_name: str, bad_value: object) -> None:
    with pytest.raises(ValueError):
        replace(TaiwanFeeSchedule(), **{field_name: bad_value})


def test_fee_schedule_rejects_discount_above_one() -> None:
    with pytest.raises(ValueError, match="commission_discount"):
        TaiwanFeeSchedule(commission_discount=1.000001)


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("commission_rounding", "bankers"),
        ("commission_rounding", 1),
        ("tax_rounding", "ceil"),
        ("tax_rounding", None),
    ],
)
def test_fee_schedule_rejects_unknown_rounding_rules(
    field_name: str,
    bad_value: object,
) -> None:
    with pytest.raises(ValueError, match="rounding"):
        replace(TaiwanFeeSchedule(), **{field_name: bad_value})


def test_fee_schedule_normalizes_explicit_rounding_aliases() -> None:
    schedule = TaiwanFeeSchedule(
        commission_rounding="truncate-to-twd",
        tax_rounding="round half up",
    )

    assert schedule.commission_rounding == "floor"
    assert schedule.tax_rounding == "half_up"


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("settlement_lag_sessions", 0),
        ("settlement_lag_sessions", -1),
        ("settlement_lag_sessions", 2.0),
        ("settlement_lag_sessions", True),
        ("cash_lot_size", 0),
        ("cash_lot_size", 1.5),
        ("day_trade_default_lot_size", -1000),
        ("day_trade_default_lot_size", math.inf),
    ],
)
def test_fee_schedule_rejects_non_positive_or_non_integer_schedule_values(
    field_name: str,
    bad_value: object,
) -> None:
    with pytest.raises(ValueError):
        replace(TaiwanFeeSchedule(), **{field_name: bad_value})


def test_tw_cash_fee_vectors_distinguish_stock_and_etf_tax() -> None:
    buy, sell = effective_fee_rate_vectors(["2330", "0050", "00631L"], "tw_cash")
    commission = 0.001425 * 0.2

    assert buy.dtype == np.float64
    assert sell.dtype == np.float64
    np.testing.assert_allclose(
        buy, [commission, commission, commission], rtol=0.0, atol=0.0
    )
    np.testing.assert_allclose(
        sell,
        [commission + 0.003, commission + 0.001, commission + 0.001],
        rtol=0.0,
        atol=0.0,
    )


def test_tw_day_trade_fee_vectors_use_reduced_stock_tax_and_etf_tax() -> None:
    buy, sell = effective_fee_rate_vectors(["2330", "0050"], "tw_day_trade")
    commission = 0.001425 * 0.2

    np.testing.assert_allclose(buy, [commission, commission], rtol=0.0, atol=0.0)
    np.testing.assert_allclose(
        sell,
        [commission + 0.0015, commission + 0.001],
        rtol=0.0,
        atol=0.0,
    )


def test_naive_fee_vectors_broadcast_legacy_scalars_without_tw_classification() -> None:
    buy, sell = effective_fee_rate_vectors(
        ["AAPL", "03001P"],
        "naive",
        naive_buy_fee_rate=0.01,
        naive_sell_fee_rate=0.02,
    )

    np.testing.assert_array_equal(buy, np.asarray([0.01, 0.01], dtype=np.float64))
    np.testing.assert_array_equal(sell, np.asarray([0.02, 0.02], dtype=np.float64))
    gross_buy, gross_sell = gross_fee_rate_vectors(
        ["AAPL", "03001P"],
        "naive",
        naive_buy_fee_rate=0.01,
        naive_sell_fee_rate=0.02,
    )
    rebate = commission_rebate_rate_vector(["AAPL", "03001P"], "naive")
    np.testing.assert_array_equal(gross_buy, buy)
    np.testing.assert_array_equal(gross_sell, sell)
    np.testing.assert_array_equal(rebate, np.zeros(2, dtype=np.float64))


def test_security_type_override_supports_explicit_stock_or_etf_only() -> None:
    commission = 0.001425 * 0.2
    _, sell = effective_fee_rate_vectors(
        ["FUND_X", "2330"],
        "tw_cash",
        security_types={"fund_x": "ETF", "2330": "stock"},
    )
    np.testing.assert_allclose(
        sell,
        [commission + 0.001, commission + 0.003],
        rtol=0.0,
        atol=0.0,
    )

    with pytest.raises(ValueError, match="stock.*etf"):
        effective_fee_rate_vectors(
            ["2330"],
            "tw_cash",
            security_types={"2330": "warrant"},
        )


@pytest.mark.parametrize("mode", ["tw_cash", "tw_day_trade"])
def test_tw_modes_reject_unknown_security_products(mode: str) -> None:
    with pytest.raises(ValueError, match="unsupported Taiwan security"):
        effective_fee_rate_vectors(["2330", "03001P"], mode)


def test_custom_fee_schedule_is_applied_without_discounting_tax() -> None:
    schedule = TaiwanFeeSchedule(
        commission_rate=0.002,
        commission_discount=0.5,
        stock_sell_tax=0.004,
        etf_sell_tax=0.002,
    )
    buy, sell = effective_fee_rate_vectors(
        ["2330", "0050"], "tw_cash", fee_schedule=schedule
    )

    np.testing.assert_allclose(buy, [0.001, 0.001], rtol=0.0, atol=0.0)
    np.testing.assert_allclose(sell, [0.005, 0.003], rtol=0.0, atol=0.0)


def test_gross_fee_and_rebate_vectors_separate_cash_timing_from_tax() -> None:
    schedule = TaiwanFeeSchedule(
        commission_rate=0.002,
        commission_discount=0.2,
        stock_sell_tax=0.004,
        etf_sell_tax=0.002,
    )

    effective_buy, effective_sell = effective_fee_rate_vectors(
        ["2330", "0050"],
        "tw_cash",
        fee_schedule=schedule,
    )
    gross_buy, gross_sell = gross_fee_rate_vectors(
        ["2330", "0050"],
        "tw_cash",
        fee_schedule=schedule,
    )
    rebate = commission_rebate_rate_vector(
        ["2330", "0050"],
        "tw_cash",
        fee_schedule=schedule,
    )

    np.testing.assert_allclose(gross_buy, [0.002, 0.002], rtol=0.0, atol=0.0)
    np.testing.assert_allclose(
        gross_sell,
        [0.002 + 0.004, 0.002 + 0.002],
        rtol=0.0,
        atol=0.0,
    )
    np.testing.assert_allclose(rebate, [0.0016, 0.0016], rtol=0.0, atol=0.0)
    # Only commission is rebated: product-specific sell tax is identical in
    # the gross and ultimate economic schedules.
    np.testing.assert_allclose(
        gross_buy - rebate,
        effective_buy,
        rtol=1e-15,
        atol=0.0,
    )
    np.testing.assert_allclose(
        gross_sell - rebate,
        effective_sell,
        rtol=1e-15,
        atol=0.0,
    )


def test_lot_size_vectors_cover_cash_day_trade_and_override() -> None:
    symbols = ["2330", "0050", "00631L"]

    cash = lot_size_vector(symbols, "tw_cash")
    day_trade = lot_size_vector(
        symbols,
        "tw_day_trade",
        per_symbol_lot_sizes={"0050": 500, "00631l": np.int64(2000)},
    )

    assert cash.dtype == np.int64
    assert day_trade.dtype == np.int64
    np.testing.assert_array_equal(cash, [1, 1, 1])
    np.testing.assert_array_equal(day_trade, [1000, 500, 2000])


@pytest.mark.parametrize("bad_lot_size", [0, -1, 1.5, math.nan, math.inf, True, "1000"])
def test_day_trade_lot_override_must_be_a_positive_integer(
    bad_lot_size: object,
) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        lot_size_vector(
            ["2330"],
            "tw_day_trade",
            per_symbol_lot_sizes={"2330": bad_lot_size},
        )


def test_lot_overrides_fail_closed_outside_day_trade() -> None:
    with pytest.raises(ValueError, match="only for tw_day_trade"):
        lot_size_vector(["2330"], "tw_cash", per_symbol_lot_sizes={"2330": 1000})


def test_tw_lot_vector_rejects_unknown_products_too() -> None:
    with pytest.raises(ValueError, match="unsupported Taiwan security"):
        lot_size_vector(["03001P"], "tw_day_trade")


def test_settlement_indices_count_sessions_and_preserve_tail_obligations() -> None:
    indices = settlement_session_indices(4, lag=2)

    assert indices.dtype == np.int64
    np.testing.assert_array_equal(indices, [2, 3, 4, 5])
    np.testing.assert_array_equal(
        settlement_session_indices(0), np.asarray([], dtype=np.int64)
    )


@pytest.mark.parametrize(
    ("num_sessions", "lag"),
    [(-1, 2), (2.5, 2), (True, 2), (3, 0), (3, -1), (3, 2.0), (3, True)],
)
def test_settlement_indices_validate_integer_session_arguments(
    num_sessions: object,
    lag: object,
) -> None:
    with pytest.raises(ValueError):
        settlement_session_indices(num_sessions, lag=lag)  # type: ignore[arg-type]
