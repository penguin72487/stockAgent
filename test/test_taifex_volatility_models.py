from __future__ import annotations

from datetime import date, datetime, time
import math

import numpy as np

from scripts.backtest_taifex_atm_straddle_rolling import DayMarket, OptionContract, _ns
from stockagent.data.tw_index_derivatives_tick import TAIPEI
from stockagent.research.taifex_volatility_models import (
    BidAskSurfaceQuote,
    CausalVolatilitySurface,
    SurfacePoint,
    VOLATILITY_MODEL_IDS,
    black76_implied_volatility,
    black76_price,
    build_bidask_iv_surface,
    extract_causal_iv_surface,
    fit_volatility_model,
)


def test_black76_implied_volatility_round_trip() -> None:
    expected = 0.37
    price = black76_price(46_000.0, 45_500.0, 21.0 / 365.0, expected, "P")
    observed = black76_implied_volatility(
        forward=46_000.0,
        strike=45_500.0,
        years_to_expiry=21.0 / 365.0,
        price_points=price,
        option_right="P",
    )
    assert observed is not None
    assert math.isclose(observed, expected, rel_tol=0.0, abs_tol=1e-10)


def test_surface_uses_completed_second_and_official_expiry_override() -> None:
    trading_date = date(2026, 7, 13)
    decision = datetime.combine(trading_date, time(8, 55), tzinfo=TAIPEI)
    prior_second = _ns(decision) - 1_000_000_000
    decision_ns = _ns(decision)
    option_events = {}
    expiry_years = (
        datetime.combine(trading_date, time(13, 30), tzinfo=TAIPEI) - decision
    ).total_seconds() / (365.0 * 24.0 * 60.0 * 60.0)
    for strike in np.linspace(900.0, 1_100.0, 21):
        right = "C" if strike >= 1_000.0 else "P"
        contract = OptionContract(series="202607F2", strike=float(strike), right=right)
        price = black76_price(1_000.0, float(strike), expiry_years, 0.4, right)
        option_events[contract] = (
            np.asarray([prior_second], dtype=np.int64),
            np.asarray([price], dtype=np.float64),
        )
    market = DayMarket(
        trading_date=trading_date,
        tx_times_ns=np.asarray([prior_second, decision_ns], dtype=np.int64),
        tx_prices=np.asarray([1_000.0, 2_000.0], dtype=np.float64),
        option_events=option_events,
        pair_availability=(),
    )
    surface = extract_causal_iv_surface(
        market,
        calibration_decision_ns=decision_ns,
        maximum_abs_log_moneyness=0.12,
        expiry_overrides={"202607F2": trading_date},
    )
    assert surface.forward == 1_000.0
    assert surface.observable_through_ns == prior_second
    assert len(surface.points) >= 12
    assert {point.expiry for point in surface.points} == {trading_date}


def _synthetic_surface() -> CausalVolatilitySurface:
    points: list[SurfacePoint] = []
    forward = 46_000.0
    for series, expiry, years in (
        ("202608W1", date(2026, 8, 5), 7.0 / 365.0),
        ("202608", date(2026, 8, 19), 21.0 / 365.0),
        ("202609", date(2026, 9, 16), 49.0 / 365.0),
    ):
        for log_moneyness in np.linspace(-0.08, 0.08, 13):
            strike = forward * math.exp(float(log_moneyness))
            implied = 0.32 - 0.35 * log_moneyness + 1.2 * log_moneyness**2
            right = "C" if log_moneyness >= 0.0 else "P"
            points.append(
                SurfacePoint(
                    series=series,
                    expiry=expiry,
                    strike=strike,
                    option_right=right,
                    price_points=black76_price(forward, strike, years, implied, right),
                    years_to_expiry=years,
                    log_moneyness=float(log_moneyness),
                    implied_volatility=float(implied),
                    staleness_seconds=2.0,
                )
            )
    return CausalVolatilitySurface(
        calibration_decision_ns=1_000_000_000,
        observable_through_ns=0,
        forward=forward,
        points=tuple(points),
    )


def test_all_six_models_fit_and_return_finite_straddle_delta() -> None:
    surface = _synthetic_surface()
    for model_id in VOLATILITY_MODEL_IDS:
        fitted = fit_volatility_model(
            surface,
            model_id=model_id,
            held_series="202608W1",
        )
        delta = fitted.straddle_delta(
            forward=46_100.0,
            strike=46_000.0,
            years_to_expiry=6.5 / 365.0,
        )
        assert fitted.calibration_points >= 4
        assert math.isfinite(fitted.calibration_rmse_iv)
        assert math.isfinite(delta)
        assert -2.0 <= delta <= 2.0


def test_bidask_surface_is_causal_and_uses_mid_only_for_calibration() -> None:
    decision = datetime(2026, 8, 12, 13, 29, tzinfo=TAIPEI)
    decision_ns = _ns(decision)
    forward = 45_000.0
    expiry = date(2026, 8, 14)
    years = (
        datetime.combine(expiry, time(13, 30), tzinfo=TAIPEI) - decision
    ).total_seconds() / (365.0 * 24.0 * 60.0 * 60.0)
    quotes: list[BidAskSurfaceQuote] = []
    for log_moneyness in np.linspace(-0.08, 0.08, 17):
        strike = forward * math.exp(float(log_moneyness))
        right = "C" if strike >= forward else "P"
        midpoint = black76_price(forward, strike, years, 0.31, right)
        quotes.append(
            BidAskSurfaceQuote(
                series="TXV:202608:2026-08-14",
                expiry=expiry,
                strike=strike,
                option_right=right,
                bid_price=max(0.01, midpoint - 0.05),
                ask_price=midpoint + 0.05,
                receive_ts_ns=decision_ns - 1_000_000_000,
            )
        )
    # This future observation must be ignored rather than leaking into the fit.
    quotes.append(
        BidAskSurfaceQuote(
            series="TXV:202608:2026-08-14",
            expiry=expiry,
            strike=forward,
            option_right="C",
            bid_price=9_999.0,
            ask_price=10_000.0,
            receive_ts_ns=decision_ns + 1,
        )
    )
    surface = build_bidask_iv_surface(
        quotes,
        calibration_decision_ns=decision_ns,
        forward_bid=44_999.0,
        forward_ask=45_001.0,
        forward_receive_ts_ns=decision_ns - 500_000_000,
        maximum_staleness_seconds=5.0,
    )
    assert surface.forward == forward
    assert len(surface.points) == 17
    assert max(point.implied_volatility for point in surface.points) < 1.0


def test_bidask_surface_rejects_stale_forward() -> None:
    with np.testing.assert_raises_regex(ValueError, "forward Bid/Ask is stale"):
        build_bidask_iv_surface(
            [],
            calibration_decision_ns=10_000_000_000,
            forward_bid=100.0,
            forward_ask=101.0,
            forward_receive_ts_ns=0,
            maximum_staleness_seconds=1.0,
        )
