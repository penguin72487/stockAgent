"""Causal volatility-surface delta models for TAIFEX TXO research.

The public TAIFEX archive contains transaction prints, not synchronized quotes.
This module therefore exposes a deliberately narrow research contract:

* build one completed-second IV snapshot from prints observed strictly before a
  calibration decision;
* keep the fitted surface parameters fixed for the remainder of that session;
* return the one-lot Call+Put delta used by the shared TMF hedge ledger.

Black-Scholes and beta-one SABR use their direct formulas.  The remaining
families are explicitly named proxies: raw-SVI for Heston-like skew, a smooth
surface for local volatility, a local/stochastic blend for SLV, and a
power-law term-skew surface for rough volatility.  They are useful first-layer
delta comparisons, not production calibrations of the full latent processes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
import math
from typing import Any, Final, Mapping, Sequence

import numpy as np
from scipy.optimize import least_squares

from stockagent.data.tw_index_derivatives_tick import TAIPEI
from stockagent.data.tw_index_options_daily import taifex_option_expiry


MODEL_BLACK_SCHOLES: Final[str] = "black_scholes"
MODEL_HESTON_SVI: Final[str] = "heston_svi_proxy"
MODEL_SABR: Final[str] = "sabr_hagan_beta1"
MODEL_LOCAL_VOL: Final[str] = "local_vol_surface_proxy"
MODEL_SLV: Final[str] = "slv_blend_proxy"
MODEL_ROUGH_VOL: Final[str] = "rough_vol_power_law_proxy"

VOLATILITY_MODEL_IDS: Final[tuple[str, ...]] = (
    MODEL_BLACK_SCHOLES,
    MODEL_HESTON_SVI,
    MODEL_SABR,
    MODEL_LOCAL_VOL,
    MODEL_SLV,
    MODEL_ROUGH_VOL,
)

VOLATILITY_MODEL_LABELS: Final[dict[str, str]] = {
    MODEL_BLACK_SCHOLES: "Black-Scholes (flat IV)",
    MODEL_HESTON_SVI: "Heston (raw-SVI surface proxy)",
    MODEL_SABR: "SABR (Hagan beta=1)",
    MODEL_LOCAL_VOL: "Local Vol (smooth-surface delta proxy)",
    MODEL_SLV: "SLV (local/Heston surface blend proxy)",
    MODEL_ROUGH_VOL: "Rough Vol (power-law term-skew proxy)",
}

VOLATILITY_MODEL_IMPLEMENTATION: Final[dict[str, str]] = {
    MODEL_BLACK_SCHOLES: "direct_formula",
    MODEL_HESTON_SVI: "surface_proxy_not_heston_characteristic_function",
    MODEL_SABR: "direct_hagan_asymptotic_formula_beta_1",
    MODEL_LOCAL_VOL: "surface_delta_proxy_not_dupire_pde",
    MODEL_SLV: "surface_blend_proxy_not_particle_calibrated_slv",
    MODEL_ROUGH_VOL: "power_law_surface_proxy_not_rough_bergomi_monte_carlo",
}

SECONDS_PER_YEAR: Final[float] = 365.0 * 24.0 * 60.0 * 60.0
OBSERVABILITY_DELAY_NS: Final[int] = 1_000_000_000


@dataclass(frozen=True, slots=True)
class SurfacePoint:
    series: str
    expiry: object
    strike: float
    option_right: str
    price_points: float
    years_to_expiry: float
    log_moneyness: float
    implied_volatility: float
    staleness_seconds: float


@dataclass(frozen=True, slots=True)
class BidAskSurfaceQuote:
    """One causally received executable option book used for live fitting."""

    series: str
    expiry: date
    strike: float
    option_right: str
    bid_price: float
    ask_price: float
    receive_ts_ns: int


@dataclass(frozen=True, slots=True)
class CausalVolatilitySurface:
    calibration_decision_ns: int
    observable_through_ns: int
    forward: float
    points: tuple[SurfacePoint, ...]

    @property
    def maturity_count(self) -> int:
        return len({point.series for point in self.points})


def build_bidask_iv_surface(
    quotes: Sequence[BidAskSurfaceQuote],
    *,
    calibration_decision_ns: int,
    forward_bid: float,
    forward_ask: float,
    forward_receive_ts_ns: int,
    maximum_staleness_seconds: float = 120.0,
    maximum_abs_log_moneyness: float = 0.12,
) -> CausalVolatilitySurface:
    """Build a causal IV surface from already-received executable books.

    Midpoints are used only for model calibration.  Strategy fills remain at
    the opposing best price.  A quote received after the decision, a crossed
    or one-sided book, or an observation older than the configured boundary is
    excluded.  As in the historical extractor, one OTM observation is retained
    per series/strike so Call/Put parity does not double-count the same smile
    location.
    """

    if maximum_staleness_seconds <= 0.0:
        raise ValueError("maximum_staleness_seconds must be positive")
    if maximum_abs_log_moneyness <= 0.0:
        raise ValueError("maximum_abs_log_moneyness must be positive")
    if not all(
        math.isfinite(float(value)) for value in (forward_bid, forward_ask)
    ) or not (0.0 < float(forward_bid) <= float(forward_ask)):
        raise ValueError("forward Bid/Ask must be finite, positive, and uncrossed")
    if int(forward_receive_ts_ns) > int(calibration_decision_ns):
        raise ValueError("forward quote was received after the calibration decision")
    forward_age = (
        int(calibration_decision_ns) - int(forward_receive_ts_ns)
    ) / 1_000_000_000.0
    if forward_age > maximum_staleness_seconds:
        raise ValueError("forward Bid/Ask is stale at calibration")
    forward = (float(forward_bid) + float(forward_ask)) / 2.0
    calibration_dt = datetime.fromtimestamp(
        int(calibration_decision_ns) / 1_000_000_000.0,
        tz=TAIPEI,
    )
    candidates: dict[tuple[str, float], dict[str, SurfacePoint]] = {}
    for quote in quotes:
        receive_ns = int(quote.receive_ts_ns)
        if receive_ns > int(calibration_decision_ns):
            continue
        staleness = (int(calibration_decision_ns) - receive_ns) / 1_000_000_000.0
        if staleness > maximum_staleness_seconds:
            continue
        bid = float(quote.bid_price)
        ask = float(quote.ask_price)
        strike = float(quote.strike)
        right = str(quote.option_right).strip().upper()
        if (
            right not in {"C", "P"}
            or not all(math.isfinite(value) for value in (bid, ask, strike))
            or not (0.0 < bid <= ask)
            or strike <= 0.0
        ):
            continue
        expiry_dt = datetime.combine(quote.expiry, time(13, 30), tzinfo=TAIPEI)
        years = (expiry_dt - calibration_dt).total_seconds() / SECONDS_PER_YEAR
        if years <= 0.0:
            continue
        log_moneyness = math.log(strike / forward)
        if abs(log_moneyness) > maximum_abs_log_moneyness:
            continue
        midpoint = (bid + ask) / 2.0
        implied = black76_implied_volatility(
            forward=forward,
            strike=strike,
            years_to_expiry=years,
            price_points=midpoint,
            option_right=right,
        )
        if implied is None:
            continue
        point = SurfacePoint(
            series=str(quote.series),
            expiry=quote.expiry,
            strike=strike,
            option_right=right,
            price_points=midpoint,
            years_to_expiry=years,
            log_moneyness=log_moneyness,
            implied_volatility=implied,
            staleness_seconds=staleness,
        )
        candidates.setdefault((point.series, point.strike), {})[right] = point
    points: list[SurfacePoint] = []
    for (_series, strike), rights in candidates.items():
        preferred = "C" if strike >= forward else "P"
        selected = rights.get(preferred)
        if selected is None and rights:
            selected = min(
                rights.values(),
                key=lambda point: (point.staleness_seconds, point.option_right),
            )
        if selected is not None:
            points.append(selected)
    points.sort(key=lambda point: (point.years_to_expiry, point.strike))
    if len(points) < 12:
        raise ValueError(f"live Bid/Ask IV surface is too sparse: {len(points)} points")
    return CausalVolatilitySurface(
        calibration_decision_ns=int(calibration_decision_ns),
        observable_through_ns=int(calibration_decision_ns),
        forward=forward,
        points=tuple(points),
    )


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def black76_price(
    forward: float,
    strike: float,
    years_to_expiry: float,
    volatility: float,
    option_right: str,
) -> float:
    """Zero-discount Black-76 price in index points."""

    right = str(option_right).strip().upper()
    if right not in {"C", "P"}:
        raise ValueError(f"unsupported option right: {option_right}")
    if forward <= 0.0 or strike <= 0.0:
        raise ValueError("forward and strike must be positive")
    if years_to_expiry <= 0.0 or volatility <= 1e-12:
        return (
            max(forward - strike, 0.0) if right == "C" else max(strike - forward, 0.0)
        )
    sigma_sqrt_t = volatility * math.sqrt(years_to_expiry)
    d1 = (
        math.log(forward / strike) + 0.5 * volatility * volatility * years_to_expiry
    ) / sigma_sqrt_t
    d2 = d1 - sigma_sqrt_t
    if right == "C":
        return forward * _normal_cdf(d1) - strike * _normal_cdf(d2)
    return strike * _normal_cdf(-d2) - forward * _normal_cdf(-d1)


def black76_implied_volatility(
    *,
    forward: float,
    strike: float,
    years_to_expiry: float,
    price_points: float,
    option_right: str,
) -> float | None:
    """Invert a zero-discount Black-76 price with a bounded bisection."""

    if (
        not all(
            math.isfinite(value)
            for value in (forward, strike, years_to_expiry, price_points)
        )
        or forward <= 0.0
        or strike <= 0.0
        or years_to_expiry <= 0.0
        or price_points <= 0.0
    ):
        return None
    right = str(option_right).strip().upper()
    intrinsic = (
        max(forward - strike, 0.0) if right == "C" else max(strike - forward, 0.0)
    )
    if price_points < intrinsic - 1e-6:
        return None
    low = 1e-4
    high = 5.0
    low_price = black76_price(forward, strike, years_to_expiry, low, right)
    high_price = black76_price(forward, strike, years_to_expiry, high, right)
    if price_points <= low_price + 1e-10:
        return low
    if price_points > high_price + 1e-8:
        return None
    for _ in range(64):
        middle = (low + high) / 2.0
        candidate = black76_price(forward, strike, years_to_expiry, middle, right)
        if candidate < price_points:
            low = middle
        else:
            high = middle
    result = (low + high) / 2.0
    return result if 0.01 <= result <= 3.0 else None


def extract_causal_iv_surface(
    market: Any,
    *,
    calibration_decision_ns: int,
    maximum_abs_log_moneyness: float = 0.12,
    expiry_overrides: Mapping[str, date] | None = None,
) -> CausalVolatilitySurface:
    """Return one OTM observation per series/strike before the decision.

    A print stamped at second ``t`` is available only after that second.  The
    one-second subtraction makes the observable boundary explicit rather than
    relying on within-second ordering that the archive does not contain.
    """

    if maximum_abs_log_moneyness <= 0.0:
        raise ValueError("maximum_abs_log_moneyness must be positive")
    observable_through_ns = calibration_decision_ns - OBSERVABILITY_DELAY_NS
    forward = market.underlying_at_or_before(observable_through_ns)
    if forward is None or not math.isfinite(float(forward)) or float(forward) <= 0.0:
        raise ValueError("causal TX forward is unavailable at calibration")
    forward = float(forward)
    calibration_dt = datetime.fromtimestamp(
        calibration_decision_ns / 1_000_000_000.0,
        tz=TAIPEI,
    )
    candidates: dict[tuple[str, float], dict[str, SurfacePoint]] = {}
    for contract, (times, prices) in market.option_events.items():
        index = int(np.searchsorted(times, observable_through_ns, side="right")) - 1
        if index < 0:
            continue
        expiry = (expiry_overrides or {}).get(contract.series)
        if expiry is None:
            try:
                expiry = taifex_option_expiry(contract.series)
            except ValueError:
                continue
        expiry_dt = datetime.combine(expiry, time(13, 30), tzinfo=TAIPEI)
        years = (expiry_dt - calibration_dt).total_seconds() / SECONDS_PER_YEAR
        if years <= 0.0:
            continue
        log_moneyness = math.log(float(contract.strike) / forward)
        if abs(log_moneyness) > maximum_abs_log_moneyness:
            continue
        price = float(prices[index])
        implied = black76_implied_volatility(
            forward=forward,
            strike=float(contract.strike),
            years_to_expiry=years,
            price_points=price,
            option_right=contract.right,
        )
        if implied is None:
            continue
        point = SurfacePoint(
            series=str(contract.series),
            expiry=expiry,
            strike=float(contract.strike),
            option_right=str(contract.right),
            price_points=price,
            years_to_expiry=years,
            log_moneyness=log_moneyness,
            implied_volatility=implied,
            staleness_seconds=(observable_through_ns - int(times[index]))
            / 1_000_000_000.0,
        )
        candidates.setdefault((point.series, point.strike), {})[point.option_right] = (
            point
        )
    points: list[SurfacePoint] = []
    for (_series, strike), rights in candidates.items():
        preferred = "C" if strike >= forward else "P"
        selected = rights.get(preferred)
        if selected is None and rights:
            selected = min(
                rights.values(),
                key=lambda point: (point.staleness_seconds, point.option_right),
            )
        if selected is not None:
            points.append(selected)
    points.sort(key=lambda point: (point.years_to_expiry, point.strike))
    if len(points) < 12:
        raise ValueError(f"causal IV surface is too sparse: {len(points)} points")
    return CausalVolatilitySurface(
        calibration_decision_ns=calibration_decision_ns,
        observable_through_ns=observable_through_ns,
        forward=forward,
        points=tuple(points),
    )


def _weights(points: Sequence[SurfacePoint]) -> np.ndarray:
    weights = np.asarray(
        [
            math.exp(-abs(point.log_moneyness) / 0.06)
            / math.sqrt(1.0 + max(point.staleness_seconds, 0.0) / 60.0)
            for point in points
        ],
        dtype=np.float64,
    )
    total = float(weights.sum())
    if not math.isfinite(total) or total <= 0.0:
        return np.ones(len(points), dtype=np.float64)
    return weights / total * len(points)


def _sabr_beta_one_volatility(
    *,
    forward: float,
    strike: float,
    years_to_expiry: float,
    alpha: float,
    rho: float,
    nu: float,
) -> float:
    log_fk = math.log(forward / strike)
    correction = (
        1.0
        + (rho * alpha * nu / 4.0 + (2.0 - 3.0 * rho * rho) * nu * nu / 24.0)
        * years_to_expiry
    )
    if abs(log_fk) < 1e-10 or nu < 1e-10:
        return max(alpha * correction, 1e-4)
    z = (nu / alpha) * log_fk
    radical = max(1.0 - 2.0 * rho * z + z * z, 1e-14)
    numerator = math.sqrt(radical) + z - rho
    denominator = 1.0 - rho
    if numerator <= 0.0 or denominator <= 0.0:
        return max(alpha * correction, 1e-4)
    x_z = math.log(numerator / denominator)
    ratio = 1.0 if abs(x_z) < 1e-12 else z / x_z
    return float(np.clip(alpha * ratio * correction, 0.01, 3.0))


def _local_features(k: np.ndarray, years: np.ndarray) -> np.ndarray:
    sqrt_t = np.sqrt(np.maximum(years, 1e-10))
    return np.column_stack(
        (
            np.ones_like(k),
            sqrt_t,
            years,
            k,
            k * k,
            k * k * k,
            k * sqrt_t,
            k * years,
            k * k * sqrt_t,
        )
    )


@dataclass(frozen=True, slots=True)
class FittedVolatilityModel:
    model_id: str
    reference_forward: float
    reference_years: float
    parameters: Mapping[str, Any]
    calibration_points: int
    calibration_maturities: int
    calibration_rmse_iv: float
    maximum_staleness_seconds: float

    def implied_volatility(
        self,
        *,
        forward: float,
        strike: float,
        years_to_expiry: float,
    ) -> float:
        years = max(float(years_to_expiry), 1.0 / SECONDS_PER_YEAR)
        k = math.log(float(strike) / float(forward))
        p = self.parameters
        if self.model_id == MODEL_BLACK_SCHOLES:
            value = float(p["sigma"])
        elif self.model_id == MODEL_HESTON_SVI:
            a = float(p["a"])
            b = float(p["b"])
            rho = float(p["rho"])
            m = float(p["m"])
            sigma = float(p["sigma"])
            total_variance = a + b * (
                rho * (k - m) + math.sqrt((k - m) ** 2 + sigma * sigma)
            )
            value = math.sqrt(max(total_variance, 1e-10) / self.reference_years)
        elif self.model_id == MODEL_SABR:
            value = _sabr_beta_one_volatility(
                forward=forward,
                strike=strike,
                years_to_expiry=years,
                alpha=float(p["alpha"]),
                rho=float(p["rho"]),
                nu=float(p["nu"]),
            )
        elif self.model_id == MODEL_LOCAL_VOL:
            coefficients = np.asarray(p["coefficients"], dtype=np.float64)
            features = _local_features(
                np.asarray([k], dtype=np.float64),
                np.asarray([years], dtype=np.float64),
            )[0]
            value = math.exp(float(features @ coefficients))
        elif self.model_id == MODEL_SLV:
            local = FittedVolatilityModel(
                model_id=MODEL_LOCAL_VOL,
                reference_forward=self.reference_forward,
                reference_years=self.reference_years,
                parameters={"coefficients": p["local_coefficients"]},
                calibration_points=self.calibration_points,
                calibration_maturities=self.calibration_maturities,
                calibration_rmse_iv=self.calibration_rmse_iv,
                maximum_staleness_seconds=self.maximum_staleness_seconds,
            ).implied_volatility(
                forward=forward,
                strike=strike,
                years_to_expiry=years,
            )
            svi = FittedVolatilityModel(
                model_id=MODEL_HESTON_SVI,
                reference_forward=self.reference_forward,
                reference_years=self.reference_years,
                parameters=p["svi_parameters"],
                calibration_points=self.calibration_points,
                calibration_maturities=self.calibration_maturities,
                calibration_rmse_iv=self.calibration_rmse_iv,
                maximum_staleness_seconds=self.maximum_staleness_seconds,
            ).implied_volatility(
                forward=forward,
                strike=strike,
                years_to_expiry=years,
            )
            mixing = float(p["mixing_weight"])
            value = math.sqrt(
                max(mixing * local * local + (1.0 - mixing) * svi * svi, 1e-8)
            )
        elif self.model_id == MODEL_ROUGH_VOL:
            h = float(p["hurst"])
            atm = math.exp(
                float(p["atm_intercept"]) + float(p["atm_sqrt_time"]) * math.sqrt(years)
            )
            skew = float(p["skew_scale"]) * years ** (h - 0.5)
            value = atm + skew * k + float(p["curvature"]) * k * k
        else:
            raise ValueError(f"unsupported volatility model: {self.model_id}")
        return float(np.clip(value, 0.01, 3.0))

    def straddle_delta(
        self,
        *,
        forward: float,
        strike: float,
        years_to_expiry: float,
    ) -> float:
        """Central-difference sticky-parameter Call+Put delta."""

        bump = max(1.0, abs(float(forward)) * 1e-4)

        def value(candidate_forward: float) -> float:
            volatility = self.implied_volatility(
                forward=candidate_forward,
                strike=strike,
                years_to_expiry=years_to_expiry,
            )
            return black76_price(
                candidate_forward,
                strike,
                years_to_expiry,
                volatility,
                "C",
            ) + black76_price(
                candidate_forward,
                strike,
                years_to_expiry,
                volatility,
                "P",
            )

        delta = (value(forward + bump) - value(forward - bump)) / (2.0 * bump)
        return float(np.clip(delta, -2.0, 2.0))

    def diagnostics(self) -> dict[str, Any]:
        return {
            "volatility_model": self.model_id,
            "volatility_model_label": VOLATILITY_MODEL_LABELS[self.model_id],
            "implementation_level": VOLATILITY_MODEL_IMPLEMENTATION[self.model_id],
            "calibration_points": self.calibration_points,
            "calibration_maturities": self.calibration_maturities,
            "calibration_rmse_iv": self.calibration_rmse_iv,
            "maximum_calibration_staleness_seconds": self.maximum_staleness_seconds,
            "parameters": dict(self.parameters),
        }


def _fit_svi(
    points: Sequence[SurfacePoint],
    *,
    reference_years: float,
) -> tuple[dict[str, float], np.ndarray]:
    k = np.asarray([point.log_moneyness for point in points], dtype=np.float64)
    observed = np.asarray(
        [point.implied_volatility**2 * reference_years for point in points],
        dtype=np.float64,
    )
    weight = np.sqrt(_weights(points))
    minimum = max(float(np.quantile(observed, 0.05)) * 0.5, 1e-8)
    initial = np.asarray(
        [minimum, max(float(np.std(observed)) * 5.0, 1e-4), -0.2, 0.0, 0.05],
        dtype=np.float64,
    )

    def residual(parameters: np.ndarray) -> np.ndarray:
        a, b, rho, m, sigma = parameters
        fitted = a + b * (rho * (k - m) + np.sqrt((k - m) ** 2 + sigma * sigma))
        return (fitted - observed) * weight

    result = least_squares(
        residual,
        initial,
        bounds=(
            np.asarray([1e-10, 1e-10, -0.999, -0.5, 1e-4]),
            np.asarray([2.0, 10.0, 0.999, 0.5, 2.0]),
        ),
        max_nfev=500,
        x_scale="jac",
    )
    a, b, rho, m, sigma = result.x
    parameters = {
        "a": float(a),
        "b": float(b),
        "rho": float(rho),
        "m": float(m),
        "sigma": float(sigma),
    }
    fitted_variance = a + b * (rho * (k - m) + np.sqrt((k - m) ** 2 + sigma * sigma))
    fitted_iv = np.sqrt(np.maximum(fitted_variance, 1e-10) / reference_years)
    return parameters, fitted_iv


def _fit_local_coefficients(
    points: Sequence[SurfacePoint],
) -> tuple[np.ndarray, np.ndarray]:
    k = np.asarray([point.log_moneyness for point in points], dtype=np.float64)
    years = np.asarray([point.years_to_expiry for point in points], dtype=np.float64)
    target = np.log(
        np.asarray([point.implied_volatility for point in points], dtype=np.float64)
    )
    design = _local_features(k, years)
    weight = _weights(points)
    gram = design.T @ (weight[:, None] * design)
    penalty = np.eye(design.shape[1], dtype=np.float64) * 1e-3
    penalty[0, 0] = 1e-8
    coefficients = np.linalg.solve(
        gram + penalty,
        design.T @ (weight * target),
    )
    fitted = np.exp(design @ coefficients)
    return coefficients, fitted


def fit_volatility_model(
    surface: CausalVolatilitySurface,
    *,
    model_id: str,
    held_series: str,
) -> FittedVolatilityModel:
    normalized = str(model_id).strip().lower()
    if normalized not in VOLATILITY_MODEL_IDS:
        raise ValueError(f"unsupported volatility model: {model_id}")
    held = [point for point in surface.points if point.series == held_series]
    if len(held) < 8:
        raise ValueError(
            f"held-series IV surface is too sparse: series={held_series} points={len(held)}"
        )
    reference_years = float(np.median([point.years_to_expiry for point in held]))
    observed: np.ndarray
    fitted: np.ndarray
    parameters: dict[str, Any]
    fit_points: Sequence[SurfacePoint]
    if normalized == MODEL_BLACK_SCHOLES:
        near = [point for point in held if abs(point.log_moneyness) <= 0.025]
        fit_points = near if len(near) >= 4 else held
        weight = _weights(fit_points)
        observed = np.asarray(
            [point.implied_volatility for point in fit_points], dtype=np.float64
        )
        sigma = float(np.sum(weight * observed) / np.sum(weight))
        parameters = {"sigma": sigma}
        fitted = np.full_like(observed, sigma)
    elif normalized == MODEL_HESTON_SVI:
        fit_points = held
        parameters, fitted = _fit_svi(held, reference_years=reference_years)
        observed = np.asarray(
            [point.implied_volatility for point in held], dtype=np.float64
        )
    elif normalized == MODEL_SABR:
        fit_points = held
        observed = np.asarray(
            [point.implied_volatility for point in held], dtype=np.float64
        )
        strikes = np.asarray([point.strike for point in held], dtype=np.float64)
        years = np.asarray([point.years_to_expiry for point in held], dtype=np.float64)
        weight = np.sqrt(_weights(held))

        def residual(raw: np.ndarray) -> np.ndarray:
            alpha, rho, nu = raw
            values = np.asarray(
                [
                    _sabr_beta_one_volatility(
                        forward=surface.forward,
                        strike=float(strike),
                        years_to_expiry=float(year),
                        alpha=float(alpha),
                        rho=float(rho),
                        nu=float(nu),
                    )
                    for strike, year in zip(strikes, years, strict=True)
                ],
                dtype=np.float64,
            )
            return (values - observed) * weight

        initial_alpha = float(np.median(observed))
        result = least_squares(
            residual,
            np.asarray([initial_alpha, -0.2, 1.0]),
            bounds=(
                np.asarray([0.01, -0.999, 1e-4]),
                np.asarray([3.0, 0.999, 10.0]),
            ),
            max_nfev=500,
            x_scale="jac",
        )
        alpha, rho, nu = result.x
        parameters = {
            "alpha": float(alpha),
            "rho": float(rho),
            "nu": float(nu),
            "beta": 1.0,
        }
        fitted = observed + residual(result.x) / weight
    elif normalized == MODEL_LOCAL_VOL:
        fit_points = surface.points
        coefficients, fitted = _fit_local_coefficients(fit_points)
        observed = np.asarray(
            [point.implied_volatility for point in fit_points], dtype=np.float64
        )
        parameters = {"coefficients": coefficients.tolist()}
    elif normalized == MODEL_SLV:
        fit_points = surface.points
        local_coefficients, local_fitted = _fit_local_coefficients(fit_points)
        svi_parameters, _held_svi = _fit_svi(
            held,
            reference_years=reference_years,
        )
        observed = np.asarray(
            [point.implied_volatility for point in fit_points], dtype=np.float64
        )
        svi_model = FittedVolatilityModel(
            model_id=MODEL_HESTON_SVI,
            reference_forward=surface.forward,
            reference_years=reference_years,
            parameters=svi_parameters,
            calibration_points=len(held),
            calibration_maturities=1,
            calibration_rmse_iv=0.0,
            maximum_staleness_seconds=max(point.staleness_seconds for point in held),
        )
        svi_fitted = np.asarray(
            [
                svi_model.implied_volatility(
                    forward=surface.forward,
                    strike=point.strike,
                    years_to_expiry=point.years_to_expiry,
                )
                for point in fit_points
            ],
            dtype=np.float64,
        )
        mixing = 0.5
        fitted = np.sqrt(
            mixing * local_fitted * local_fitted
            + (1.0 - mixing) * svi_fitted * svi_fitted
        )
        parameters = {
            "mixing_weight": mixing,
            "local_coefficients": local_coefficients.tolist(),
            "svi_parameters": svi_parameters,
        }
    else:
        fit_points = surface.points
        observed = np.asarray(
            [point.implied_volatility for point in fit_points], dtype=np.float64
        )
        k = np.asarray([point.log_moneyness for point in fit_points], dtype=np.float64)
        years = np.asarray(
            [point.years_to_expiry for point in fit_points], dtype=np.float64
        )
        weight = np.sqrt(_weights(fit_points))

        def rough_values(raw: np.ndarray) -> np.ndarray:
            intercept, sqrt_time, skew_scale, curvature, hurst = raw
            atm = np.exp(intercept + sqrt_time * np.sqrt(years))
            return (
                atm + skew_scale * np.power(years, hurst - 0.5) * k + curvature * k * k
            )

        def residual(raw: np.ndarray) -> np.ndarray:
            return (rough_values(raw) - observed) * weight

        result = least_squares(
            residual,
            np.asarray([math.log(float(np.median(observed))), 0.0, -0.01, 0.5, 0.1]),
            bounds=(
                np.asarray([-6.0, -20.0, -5.0, -20.0, 0.03]),
                np.asarray([1.5, 20.0, 5.0, 20.0, 0.49]),
            ),
            max_nfev=800,
            x_scale="jac",
        )
        intercept, sqrt_time, skew_scale, curvature, hurst = result.x
        parameters = {
            "atm_intercept": float(intercept),
            "atm_sqrt_time": float(sqrt_time),
            "skew_scale": float(skew_scale),
            "curvature": float(curvature),
            "hurst": float(hurst),
        }
        fitted = rough_values(result.x)
    rmse = float(np.sqrt(np.mean(np.square(fitted - observed))))
    return FittedVolatilityModel(
        model_id=normalized,
        reference_forward=surface.forward,
        reference_years=reference_years,
        parameters=parameters,
        calibration_points=len(fit_points),
        calibration_maturities=len({point.series for point in fit_points}),
        calibration_rmse_iv=rmse,
        maximum_staleness_seconds=max(point.staleness_seconds for point in fit_points),
    )
