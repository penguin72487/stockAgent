"""Return dependence must use the right units, dates and physical action axis."""
from datetime import date

import numpy as np
import pyarrow as pa
import pytest

from scripts.analyze_tw_futures_benchmark_dependence import (
    aligned_comparison, dependence, product_scope,
)


def test_known_simple_return_beta_survives_log_artifact_storage():
    market = np.array([-.08, -.02, .01, .03, .06])
    strategy = .001 + .8 * market
    result = dependence(np.log1p(strategy), np.log1p(market))
    assert result['ols_beta_simple_returns_rf0'] == pytest.approx(.8)
    assert result['ols_intercept_daily_simple_rf0'] == pytest.approx(.001)
    assert result['ols_r_squared'] == pytest.approx(1.)
    assert result['ols_residual_daily_std'] < 1e-15
    assert result['strategy_cumulative_return'] == pytest.approx(np.prod(1 + strategy) - 1)
    assert result['benchmark_down']['days'] == 2


def test_cash_has_undefined_correlation_not_proven_independent_alpha():
    result = dependence(np.zeros(4), np.log1p([-.01, .02, -.02, .03]))
    assert result['correlation_daily_simple_returns'] is None
    assert result['ols_r_squared'] is None
    assert result['ols_beta_simple_returns_rf0'] == 0.
    assert result['strategy_cumulative_return'] == 0.
    assert dependence(np.arange(4) * .01, np.zeros(4))['ols_beta_simple_returns_rf0'] is None


def test_date_join_does_not_match_return_rows_by_position_or_cumulative_nav():
    days = np.array(['2026-01-02', '2026-01-05', '2026-01-06', '2026-01-07'], dtype='datetime64[D]')
    other_days = np.r_[np.datetime64('2025-12-31'), days[[0, 2, 3]]]
    market = np.log1p([-.02, .30, .01, .04])
    other = np.log1p([.50, -.02, .01, .04])
    result = aligned_comparison(days, market, other_days, other)
    assert result['rows'] == 3
    assert result['ols_beta_simple_returns_rf0'] == pytest.approx(1.)
    assert result['start'] == '2026-01-02'
    with pytest.raises(ValueError, match='unique'):
        aligned_comparison(days, market, days[[0, 0, 2, 3]], other)
    with pytest.raises(ValueError, match='lengths'):
        aligned_comparison(days, market[:-1], other_days, other)


@pytest.mark.parametrize('bad', [[0., np.nan, .1], [0., np.inf, .1]])
def test_nonfinite_returns_fail_instead_of_silently_filtering_dates(bad):
    with pytest.raises(ValueError, match='finite'):
        dependence(bad, [0., .01, .02])


def test_physical_slot_identity_is_one_based_and_reconciles_every_position():
    days = np.array(['2026-01-02', '2026-01-05'], dtype='datetime64[D]')
    rows = [dict(date=date(2026, 1, d), portfolio_slot=slot, product=product,
                 product_name=product, executable=True)
            for d in (2, 5) for slot, product in ((2, 'MTX'), (1, 'TX'))]
    table = pa.Table.from_pylist(rows)
    q = np.array([[2, 0], [0, -4]])
    result = product_scope(days, q, table)
    assert result['held_products']['TX']['long_days'] == 1
    assert result['held_products']['MTX']['short_days'] == 1
    assert result['available_products_in_selected_dates'] == ['MTX', 'TX']
    with pytest.raises(ValueError, match='no date/slot source'):
        product_scope(days, q, pa.Table.from_pylist(rows[:1] + rows[2:]))
    with pytest.raises(ValueError, match='duplicate'):
        product_scope(days, q, pa.Table.from_pylist(rows + rows[:1]))
