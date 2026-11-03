from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from scripts.audit_finmind_option_boundary_daily import summarize


def test_daily_key_presence_is_separate_from_expiry_and_absence():
    day = date(2011, 2, 17)
    rows = pl.DataFrame([
        {'decision_date': day, 'decision_time': '08:45', 'product_raw': 'TXO',
         'series_raw': '201102', 'strike_decimal': '8000', 'right_raw': 'C'},
        {'decision_date': day, 'decision_time': '08:45', 'product_raw': 'TXO',
         'series_raw': '201103', 'strike_decimal': '8000', 'right_raw': 'C'},
        {'decision_date': day, 'decision_time': '09:00', 'product_raw': 'TXO',
         'series_raw': '201103', 'strike_decimal': '8000', 'right_raw': 'C'},
    ])
    daily = {(day, 'TXO', '201103', Decimal('8000'), 'C'):
             {'sessions': {'day'}, 'expiry_texts': {None}}}
    result = summarize(rows, daily)
    assert result['by_clock']['08:45'] == {
        'reference_rows': 2, 'daily_key_absent': 1, 'expiry_missing': 1,
        'daily_key_present': 1, 'daily_day_session_key_present': 1}
    assert result['by_clock']['09:00']['daily_key_present'] == 1
    assert any(row['series'] == '201102' and row['state'] == 'daily_key_absent'
               for row in result['by_day_clock_product_series_state'])


def test_malformed_contract_and_unexpected_clock_are_not_admitted():
    row = {'decision_date': date(2011, 2, 17), 'decision_time': '08:45',
           'product_raw': 'TXO', 'series_raw': 'unknown', 'strike_decimal': '8000', 'right_raw': 'C'}
    assert summarize(pl.DataFrame([row]), {})['by_clock']['08:45']['unsupported_reference_key'] == 1
    row['decision_time'] = '09:01'
    with pytest.raises(ValueError, match='unexpected'):
        summarize(pl.DataFrame([row]), {})
