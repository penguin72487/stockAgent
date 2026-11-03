from datetime import date
from decimal import Decimal

import polars as pl

from scripts.audit_taifex_option_txo_settlement_dates import compare


def test_scheduled_date_does_not_override_actual_settlement_or_missing_evidence():
    day = date(2026, 10, 27)
    rows = [dict(decision_date=day, decision_time='09:00', product_raw='TXO', series_raw=series,
                 strike_decimal='1000', right_raw='C') for series in ('202610F4', '202610F3', '202610W4')]
    daily = {(day, 'TXO', series, Decimal('1000'), 'C'): {'expiry_texts': {'20261023'}, 'sessions': {'day'}}
             for series in ('202610F4', '202610F3', '202610W4')}
    result = {row['series']: row for row in compare(pl.DataFrame(rows), daily,
              {'202610F4': day, '202610F3': date(2026, 10, 26)})}
    assert result['202610F4']['settlement_relation'] == 'settled_on_decision_day'
    assert result['202610F3']['settlement_relation'] == 'settled_before_decision_day'
    assert result['202610W4']['settlement_relation'] == 'missing_final_settlement'
