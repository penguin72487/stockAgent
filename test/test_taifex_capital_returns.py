from __future__ import annotations

from datetime import date, datetime
import math

import polars as pl
import pytest

from stockagent.data.tw_index_derivatives_tick import TAIPEI
from scripts.render_taifex_option_capital_report import curve_group_for_variant
from stockagent.research.taifex_capital_returns import (
    TAIFEX_INITIAL_MARGIN_TWD,
    TAIFEX_INITIAL_MARGIN_TWD_2026_08_13,
    TXO_RISK_MARGIN_TWD,
    TXO_RISK_MARGIN_TWD_2026_08_13,
    build_capital_normalized_returns,
    compute_daily_required_capital,
    taifex_futures_margin_twd,
    taifex_initial_margin_twd,
    taifex_txo_risk_margin_twd,
)


def _trade(
    *,
    trading_date: date,
    variant_id: str,
    hour: int,
    minute: int,
    instrument_type: str,
    product: str,
    delta_contracts: int,
    gross_cash_flow_twd: float,
    fixed_fee_twd: float,
    transaction_tax_twd: float | None = None,
    series: str = "202608W1",
    strike: float | None = 20_000.0,
    option_right: str | None = "C",
) -> dict[str, object]:
    row = {
        "trading_date": trading_date,
        "benchmark_family": "synthetic",
        "variant_id": variant_id,
        "fill_ts": datetime(
            trading_date.year,
            trading_date.month,
            trading_date.day,
            hour,
            minute,
            tzinfo=TAIPEI,
        ),
        "instrument_type": instrument_type,
        "product": product,
        "series": series,
        "strike": strike,
        "option_right": option_right,
        "delta_contracts": delta_contracts,
        "gross_cash_flow_twd": gross_cash_flow_twd,
        "fixed_fee_twd": fixed_fee_twd,
    }
    if transaction_tax_twd is not None:
        row["transaction_tax_twd"] = transaction_tax_twd
    return row


def _option_day(
    trading_date: date, *, variant_id: str, closing_gross_twd: float
) -> list[dict[str, object]]:
    return [
        _trade(
            trading_date=trading_date,
            variant_id=variant_id,
            hour=8,
            minute=46,
            instrument_type="option",
            product="TXO",
            delta_contracts=1,
            gross_cash_flow_twd=-5_000.0,
            fixed_fee_twd=22.0,
            option_right="C",
        ),
        _trade(
            trading_date=trading_date,
            variant_id=variant_id,
            hour=8,
            minute=46,
            instrument_type="option",
            product="TXO",
            delta_contracts=1,
            gross_cash_flow_twd=-4_000.0,
            fixed_fee_twd=22.0,
            option_right="P",
        ),
        _trade(
            trading_date=trading_date,
            variant_id=variant_id,
            hour=13,
            minute=44,
            instrument_type="option",
            product="TXO",
            delta_contracts=-1,
            gross_cash_flow_twd=closing_gross_twd * 0.6,
            fixed_fee_twd=22.0,
            option_right="C",
        ),
        _trade(
            trading_date=trading_date,
            variant_id=variant_id,
            hour=13,
            minute=44,
            instrument_type="option",
            product="TXO",
            delta_contracts=-1,
            gross_cash_flow_twd=closing_gross_twd * 0.4,
            fixed_fee_twd=22.0,
            option_right="P",
        ),
    ]


def test_option_capital_is_peak_net_premium_debit_and_fees() -> None:
    rows = _option_day(
        date(2026, 8, 5), variant_id="long_option", closing_gross_twd=9_000.0
    )
    capital = compute_daily_required_capital(pl.DataFrame(rows))
    assert capital.height == 1
    assert capital.item(0, "daily_peak_required_capital_twd") == pytest.approx(
        9_044.0
    )
    assert capital.item(0, "peak_option_cash_requirement_twd") == pytest.approx(
        9_044.0
    )
    assert capital.item(0, "peak_futures_initial_margin_twd") == 0.0


def test_option_capital_includes_transaction_tax_when_ledger_provides_it() -> None:
    trading_date = date(2026, 8, 5)
    rows = [
        _trade(
            trading_date=trading_date,
            variant_id="taxed_long_option",
            hour=8,
            minute=46,
            instrument_type="option",
            product="TXO",
            delta_contracts=1,
            gross_cash_flow_twd=-5_000.0,
            fixed_fee_twd=22.0,
            transaction_tax_twd=5.0,
            option_right="C",
        ),
        _trade(
            trading_date=trading_date,
            variant_id="taxed_long_option",
            hour=13,
            minute=44,
            instrument_type="option",
            product="TXO",
            delta_contracts=-1,
            gross_cash_flow_twd=5_100.0,
            fixed_fee_twd=0.0,
            transaction_tax_twd=20.0,
            option_right="C",
        ),
    ]

    capital = compute_daily_required_capital(pl.DataFrame(rows))

    assert capital.item(0, "daily_peak_required_capital_twd") == pytest.approx(
        5_027.0
    )


def test_futures_capital_uses_official_initial_margin_not_notional() -> None:
    trading_date = date(2026, 8, 5)
    rows = [
        _trade(
            trading_date=trading_date,
            variant_id="mtx_two_lots",
            hour=8,
            minute=46,
            instrument_type="future",
            product="MTX",
            delta_contracts=2,
            gross_cash_flow_twd=-2_000_000.0,
            fixed_fee_twd=48.0,
            series="MTX_front_month",
            strike=None,
            option_right=None,
        ),
        _trade(
            trading_date=trading_date,
            variant_id="mtx_two_lots",
            hour=13,
            minute=44,
            instrument_type="future",
            product="MTX",
            delta_contracts=-2,
            gross_cash_flow_twd=2_001_000.0,
            fixed_fee_twd=48.0,
            series="MTX_front_month",
            strike=None,
            option_right=None,
        ),
    ]
    capital = compute_daily_required_capital(pl.DataFrame(rows))
    expected = 2.0 * TAIFEX_INITIAL_MARGIN_TWD["MTX"] + 48.0
    assert capital.item(0, "daily_peak_required_capital_twd") == pytest.approx(
        expected
    )
    assert capital.item(0, "peak_futures_initial_margin_twd") == pytest.approx(
        318_000.0
    )


def test_margin_schedule_switches_on_2026_08_13_trading_date() -> None:
    assert taifex_initial_margin_twd("TX", date(2026, 8, 12)) == pytest.approx(
        TAIFEX_INITIAL_MARGIN_TWD["TX"]
    )
    assert taifex_initial_margin_twd("TX", date(2026, 8, 13)) == pytest.approx(
        TAIFEX_INITIAL_MARGIN_TWD_2026_08_13["TX"]
    )
    assert taifex_txo_risk_margin_twd(date(2026, 8, 12)) == TXO_RISK_MARGIN_TWD
    assert (
        taifex_txo_risk_margin_twd(date(2026, 8, 13))
        == TXO_RISK_MARGIN_TWD_2026_08_13
    )
    assert taifex_futures_margin_twd(
        "TMF", date(2026, 8, 13), level="maintenance"
    ) == pytest.approx(26_900.0)
    assert taifex_futures_margin_twd(
        "TMF", date(2026, 8, 13), level="clearing"
    ) == pytest.approx(25_950.0)
    assert taifex_txo_risk_margin_twd(
        date(2026, 8, 13), level="maintenance"
    ) == {"A": 143_000.0, "B": 72_000.0, "C": 14_400.0}
    assert taifex_txo_risk_margin_twd(
        date(2026, 8, 13), level="clearing"
    ) == {"A": 138_000.0, "B": 69_000.0, "C": 13_800.0}


def test_futures_capital_uses_new_margin_from_2026_08_13() -> None:
    trading_date = date(2026, 8, 13)
    rows = [
        _trade(
            trading_date=trading_date,
            variant_id="tmf_new_margin",
            hour=8,
            minute=46,
            instrument_type="future",
            product="TMF",
            delta_contracts=1,
            gross_cash_flow_twd=0.0,
            fixed_fee_twd=16.0,
            series="TMF_front_month",
            strike=None,
            option_right=None,
        ),
        _trade(
            trading_date=trading_date,
            variant_id="tmf_new_margin",
            hour=13,
            minute=44,
            instrument_type="future",
            product="TMF",
            delta_contracts=-1,
            gross_cash_flow_twd=0.0,
            fixed_fee_twd=16.0,
            series="TMF_front_month",
            strike=None,
            option_right=None,
        ),
    ]
    capital = compute_daily_required_capital(pl.DataFrame(rows))
    assert capital.item(0, "peak_futures_initial_margin_twd") == pytest.approx(
        35_050.0
    )
    assert capital.item(0, "daily_peak_required_capital_twd") == pytest.approx(
        35_066.0
    )


def test_mixed_option_and_future_requirements_are_concurrent() -> None:
    trading_date = date(2026, 8, 5)
    rows = _option_day(
        trading_date, variant_id="gamma", closing_gross_twd=9_000.0
    )
    rows.extend(
        [
            _trade(
                trading_date=trading_date,
                variant_id="gamma",
                hour=8,
                minute=46,
                instrument_type="future",
                product="TMF",
                delta_contracts=1,
                gross_cash_flow_twd=-200_000.0,
                fixed_fee_twd=16.0,
                series="TMF_front_month",
                strike=None,
                option_right=None,
            ),
            _trade(
                trading_date=trading_date,
                variant_id="gamma",
                hour=13,
                minute=44,
                instrument_type="future",
                product="TMF",
                delta_contracts=-1,
                gross_cash_flow_twd=200_100.0,
                fixed_fee_twd=16.0,
                series="TMF_front_month",
                strike=None,
                option_right=None,
            ),
        ]
    )
    capital = compute_daily_required_capital(pl.DataFrame(rows))
    assert capital.item(0, "daily_peak_required_capital_twd") == pytest.approx(
        9_044.0 + 31_800.0 + 16.0
    )


def test_capital_replay_rejects_naked_option_short() -> None:
    trading_date = date(2026, 8, 5)
    row = _trade(
        trading_date=trading_date,
        variant_id="invalid",
        hour=8,
        minute=46,
        instrument_type="option",
        product="TXO",
        delta_contracts=-1,
        gross_cash_flow_twd=5_000.0,
        fixed_fee_twd=22.0,
    )
    with pytest.raises(ValueError, match="naked short option"):
        compute_daily_required_capital(pl.DataFrame([row]))


def test_normalized_returns_keep_one_lot_and_compounded_views_separate() -> None:
    variant_id = "long_option"
    first_date = date(2026, 8, 5)
    second_date = date(2026, 8, 6)
    rows = _option_day(
        first_date, variant_id=variant_id, closing_gross_twd=9_000.0
    ) + _option_day(
        second_date, variant_id=variant_id, closing_gross_twd=11_000.0
    )
    daily = pl.DataFrame(
        [
            {
                "trading_date": first_date,
                "benchmark_family": "synthetic",
                "variant_id": variant_id,
                "net_after_fee_twd": -88.0,
            },
            {
                "trading_date": second_date,
                "benchmark_family": "synthetic",
                "variant_id": variant_id,
                "net_after_fee_twd": 1_912.0,
            },
        ]
    )
    normalized, metrics = build_capital_normalized_returns(
        daily, pl.DataFrame(rows)
    )
    expected_returns = pl.Series([-88.0 / 9_044.0, 1_912.0 / 9_044.0])
    assert normalized["daily_return_on_capital"].to_list() == pytest.approx(
        expected_returns.to_list()
    )
    assert metrics.item(0, "cumulative_return_on_capital") == pytest.approx(
        expected_returns.sum()
    )
    assert metrics.item(0, "cumulative_compounded_return") == pytest.approx(
        math.prod(1.0 + value for value in expected_returns) - 1.0
    )
    expected_std = float(expected_returns.std(ddof=1))
    expected_sharpe = float(expected_returns.mean()) / expected_std * math.sqrt(252.0)
    assert metrics.item(0, "annualized_sharpe") == pytest.approx(expected_sharpe)


def test_expiry_carry_reuses_one_fixed_capital_across_sessions() -> None:
    variant_id = "carry_option"
    first_date = date(2026, 8, 4)
    expiry_date = date(2026, 8, 5)
    rows = [
        _trade(
            trading_date=first_date,
            variant_id=variant_id,
            hour=8,
            minute=50,
            instrument_type="option",
            product="TXO",
            delta_contracts=1,
            gross_cash_flow_twd=-5_000.0,
            fixed_fee_twd=22.0,
            transaction_tax_twd=5.0,
        ),
        _trade(
            trading_date=expiry_date,
            variant_id=variant_id,
            hour=13,
            minute=30,
            instrument_type="option",
            product="TXO",
            delta_contracts=-1,
            gross_cash_flow_twd=5_500.0,
            fixed_fee_twd=0.0,
            transaction_tax_twd=20.0,
        ),
    ]
    daily = pl.DataFrame(
        [
            {
                "trading_date": first_date,
                "benchmark_family": "synthetic",
                "variant_id": variant_id,
                "net_pnl_twd": 73.0,
            },
            {
                "trading_date": expiry_date,
                "benchmark_family": "synthetic",
                "variant_id": variant_id,
                "net_pnl_twd": 380.0,
            },
        ]
    )

    normalized, metrics = build_capital_normalized_returns(
        daily,
        pl.DataFrame(rows),
        carry_across_sessions=True,
        pnl_column="net_pnl_twd",
    )

    assert metrics.item(0, "capital_base_twd") == pytest.approx(5_027.0)
    assert metrics.item(0, "total_normalized_pnl_twd") == pytest.approx(453.0)
    assert normalized["capital_base_twd"].to_list() == pytest.approx(
        [5_027.0, 5_027.0]
    )


def test_historical_option_only_cycle_does_not_require_futures_margin_window() -> None:
    trading_date = date(2005, 1, 5)
    rows = _option_day(
        trading_date,
        variant_id="historical_long_option",
        closing_gross_twd=9_500.0,
    )

    capital = compute_daily_required_capital(pl.DataFrame(rows))

    assert capital.height == 1
    assert capital.item(0, "daily_peak_required_capital_twd") == pytest.approx(
        9_044.0
    )


def test_custom_periods_per_year_controls_sharpe_scaling() -> None:
    variant_id = "weekly_option"
    first_date = date(2026, 8, 5)
    second_date = date(2026, 8, 6)
    rows = _option_day(
        first_date, variant_id=variant_id, closing_gross_twd=9_000.0
    ) + _option_day(
        second_date, variant_id=variant_id, closing_gross_twd=11_000.0
    )
    daily = pl.DataFrame(
        [
            {
                "trading_date": first_date,
                "benchmark_family": "synthetic",
                "variant_id": variant_id,
                "net_after_fee_twd": -88.0,
            },
            {
                "trading_date": second_date,
                "benchmark_family": "synthetic",
                "variant_id": variant_id,
                "net_after_fee_twd": 1_912.0,
            },
        ]
    )

    _, daily_metrics = build_capital_normalized_returns(
        daily, pl.DataFrame(rows), periods_per_year=252.0
    )
    _, weekly_metrics = build_capital_normalized_returns(
        daily, pl.DataFrame(rows), periods_per_year=52.0
    )

    assert weekly_metrics.item(0, "annualized_sharpe") == pytest.approx(
        daily_metrics.item(0, "annualized_sharpe") * math.sqrt(52.0 / 252.0)
    )
    assert weekly_metrics.item(0, "periods_per_year") == pytest.approx(52.0)


@pytest.mark.parametrize(
    ("family", "variant_id", "expected"),
    [
        ("buy_hold_atm_straddle", "classic_opening_straddle", "classic"),
        (
            "single_leg_roll_candidate",
            "roll_itm_call_keep_otm_put__0050",
            "candidate_keep_otm",
        ),
        (
            "single_leg_roll_candidate",
            "roll_otm_put_keep_itm_call__0050",
            "candidate_keep_itm",
        ),
        (
            "random_roll_control",
            "random_control__roll_itm_call_keep_otm_put__0050",
            "random_keep_otm",
        ),
        (
            "random_roll_control",
            "random_control__roll_otm_put_keep_itm_call__0050",
            "random_keep_itm",
        ),
    ],
)
def test_curve_group_split_is_unambiguous(
    family: str, variant_id: str, expected: str
) -> None:
    assert curve_group_for_variant(family=family, variant_id=variant_id) == expected
