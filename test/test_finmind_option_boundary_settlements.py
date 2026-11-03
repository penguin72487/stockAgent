from datetime import date

import polars as pl

from scripts.audit_finmind_option_boundary_settlements import classify


def test_same_day_future_and_multiple_generations_are_not_treated_as_expired():
    day = date(2011, 2, 17)
    references = pl.DataFrame([
        {'decision_date': day, 'decision_time': '08:45', 'product_raw': product, 'series_raw': '201102'}
        for product in ['TXO', 'TEO', 'TFO', 'GTO', 'XIO']])
    settlements = pl.DataFrame([
        {'product_raw': 'TXO', 'series_raw': '201102', 'settlement_date': date(2011, 2, 16), 'positive_numeric_price': True},
        {'product_raw': 'TEO', 'series_raw': '201102', 'settlement_date': day, 'positive_numeric_price': True},
        {'product_raw': 'TFO', 'series_raw': '201102', 'settlement_date': date(2011, 2, 18), 'positive_numeric_price': True},
        {'product_raw': 'GTO', 'series_raw': '201102', 'settlement_date': date(2011, 2, 16), 'positive_numeric_price': True},
        {'product_raw': 'GTO', 'series_raw': '201102', 'settlement_date': day, 'positive_numeric_price': True},
        {'product_raw': 'XIO', 'series_raw': '201102', 'settlement_date': day, 'positive_numeric_price': False},
    ])
    result = classify(references, settlements)
    states = {row['product']: row['state'] for row in result['by_day_clock_product_series_state']}
    assert states == {'TXO': 'single_source_settlement_before_decision_day',
                      'TEO': 'single_source_settlement_on_decision_day',
                      'TFO': 'single_source_settlement_after_decision_day',
                      'GTO': 'multiple_source_rows_generation_unknown',
                      'XIO': 'no_source_settlement_key'}
