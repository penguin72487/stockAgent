"""Expiry PnL uses actual settlement, without adding the calendar spread."""
from datetime import date

import numpy as np
import polars as pl
import pytest

from scripts.build_tx_final_settlement_reference import build_reference


def inputs():
    dates = np.array(['2012-01-18', '2012-01-30'], dtype='datetime64[D]')
    ref = {'dates': dates, 'contract_months': np.array(['201201', '201202']),
           'source_covered': np.array([True, True]), 'front_month_roll_mask': np.array([False, True]),
           'prior_same_contract_close': np.array([7000., 7200.]),
           'front_month_close': np.array([7207., 7500.])}
    daily = pl.DataFrame({
        'date': [date(2012, 1, 18), date(2012, 1, 30)], 'product': ['TX', 'TX'],
        'contract': ['201201', '201202'], 'accounting_only_date': [False, False],
        'official_expiry': [date(2012, 1, 18), date(2012, 2, 15)],
        'terminal_event': ['cash_settlement', 'mark_only'], 'final_settlement_price': [7208., 7600.],
        'observed_close': [7207., 7500.], 'source_row_observed': [True, True],
        'official_row_observed': [True, True],
    })
    return ref, daily


def test_final_settlement_mark_and_new_contract_own_prior_close():
    ref, daily = inputs()
    result = build_reference(ref, daily)
    assert result['research_pnl_mark'].to_list() == [7208., 7500.]
    np.testing.assert_allclose(result['gross_log_return'].to_numpy(),
                               np.log(np.array([7208 / 7000, 7500 / 7200])))
    assert result['cash_settlement_event'].to_list() == [True, False]


def test_missing_or_wrong_dated_final_fixing_is_rejected():
    ref, daily = inputs()
    missing = daily.with_columns(pl.when(pl.col('contract') == '201201').then(None)
                                 .otherwise(pl.col('final_settlement_price')).alias('final_settlement_price'))
    with pytest.raises(ValueError, match='own dated positive final'):
        build_reference(ref, missing)
    mismatched = daily.with_columns(pl.when(pl.col('contract') == '201201').then(date(2012, 1, 30))
                                    .otherwise(pl.col('official_expiry')).alias('official_expiry'))
    with pytest.raises(ValueError, match='own dated positive final'):
        build_reference(ref, mismatched)
