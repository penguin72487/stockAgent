"""A roll audit observes both actual contracts and never invents a fill."""
from datetime import date

import polars as pl
import pytest

from scripts.audit_tx_one_contract_sources import audit


def test_roll_uses_each_contract_on_previous_market_session_and_flags_expiry():
    days = [date(2012, 1, 17), date(2012, 1, 18), date(2012, 1, 30)]
    ref = pl.DataFrame({'date': days, 'product': ['TX'] * 3, 'contract': ['201201', '201201', '201202'],
                        'tenor_rank': [1] * 3})
    daily = pl.DataFrame({
        'date': [days[1], days[1], days[2]], 'product': ['TX'] * 3,
        'contract': ['201201', '201202', '201202'], 'accounting_only_date': [False] * 3,
        'official_expiry': [days[1], date(2012, 2, 15), date(2012, 2, 15)],
        'terminal_event': ['cash_settlement', 'mark_only', 'mark_only'],
        'final_settlement_price': [7208., 7400., 7400.],
        'close': [7207., 7200., 7500.], 'observed_close': [7207., 7200., 7500.],
        'official_close': ['7207', '7200', '7500'], 'outright_volume': [10, 20, 30],
        'source_row_observed': [True] * 3, 'official_row_observed': [True] * 3,
    })
    rules = pl.DataFrame({
        'date': [days[1], days[2]], 'product': ['TX'] * 2, 'contract': ['201201', '201202'],
        'initial': [83000., 92000.], 'settlement_initial': [83000., 92000.],
        'margin_kind': ['fixed_twd'] * 2, 'contract_multiplier': [200.] * 2,
        'known_at': ['observed'] * 2, 'settlement_known_at': ['observed'] * 2,
    })
    result = audit(ref, daily, rules, '2012-01-18', '2012-01-30')
    assert result['market_sessions'] == 2
    assert result['rolls'] == 1
    assert result['rolls_with_both_prior_day_observed_closes_and_positive_outright'] == 1
    assert result['rolls_where_old_expiry_is_prior_session'] == 1
    assert result['rolls_where_old_expiry_close_differs_from_final_settlement'] == 1
    assert result['expiry_close_final_difference_examples'][0]['prior_date'] == days[1]
    assert result['one_contract_benchmark_admitted'] is False

    # A source row without an observed close cannot be counted as evidence for
    # the new roll leg, even when that contract has other daily records.
    missing = daily.with_columns(pl.when(pl.col('contract') == '201202')
                                 .then(None).otherwise(pl.col('observed_close')).alias('observed_close'))
    result = audit(ref, missing, rules, '2012-01-18', '2012-01-30')
    assert result['rolls_with_both_prior_day_observed_closes_and_positive_outright'] == 0

    with pytest.raises(ValueError, match='ambiguous TX identity'):
        audit(ref, pl.concat([daily, daily.head(1)]), rules, '2012-01-18', '2012-01-30')
