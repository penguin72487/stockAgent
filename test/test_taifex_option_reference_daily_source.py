from datetime import date

import polars as pl

from scripts.audit_taifex_option_reference_daily_source import compare, daily_index


def test_daily_source_comparison_keeps_absence_and_conflicts_unresolved():
    day = date(2026, 10, 27)
    common = {'date': day, 'series': '202610W4', 'right': 'C', 'session': 'day'}
    rows = [dict(common, product='TXO', strike_raw='1,000.0', contract_expiry_raw='20261028'),
            dict(common, product='TXO', strike_raw='1000', contract_expiry_raw='20261028'),
            dict(common, product='TEO', strike_raw='200', contract_expiry_raw='20261027'),
            dict(common, product='TFO', strike_raw='300', contract_expiry_raw='20261026'),
            dict(common, product='CAO', strike_raw='400', contract_expiry_raw='20261028'),
            dict(common, product='CAO', strike_raw='400', contract_expiry_raw='20261104'),
            dict(common, product='CBO', strike_raw='500', contract_expiry_raw=None),
            dict(common, product='CCO', strike_raw='bad', contract_expiry_raw='20261028')]
    index, unsupported = daily_index([pl.DataFrame(rows)])
    assert unsupported == 1
    refs = [dict(decision_date=day, decision_time='09:00', product_raw=p, series_raw='202610W4',
                 strike_decimal=s, right_raw='C') for p, s in [('TXO', '1000'), ('TEO', '200'),
                 ('TFO', '300'), ('CAO', '400'), ('CBO', '500'), ('DDO', '600')]]
    result = compare(pl.DataFrame(refs), index)['09:00']
    assert result == {'reference_rows': 6, 'daily_key_present': 5, 'daily_day_session_key_present': 5,
                      'scheduled_date_after': 1, 'scheduled_date_on': 1, 'scheduled_date_before': 1,
                      'multiple_expiry_texts': 1, 'expiry_missing': 1, 'daily_key_absent': 1}
