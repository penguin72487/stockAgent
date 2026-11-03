from datetime import date

import pytest

from stockagent.data.tw_option_settlement_collisions import settlement_collisions


def row(product, series, day, source_row, price):
    return {'product_raw': product, 'series_raw': series, 'settlement_date': day,
            'query_month': '2011-03', 'source_row': source_row,
            'settlement_price_raw': price, 'underlying_raw': '2357'}


def test_colliding_month_preserves_each_source_and_date():
    records = [row('DJO', '201103', date(2010, 5, 17), 45, '49.55'),
               row('DJO', '201103', date(2011, 3, 16), 17, '245'),
               row('DJO', '201104', date(2011, 4, 20), 18, '250')]
    result = settlement_collisions(records)
    assert len(result) == 1
    assert result[0]['distinct_settlement_dates'] == ['2010-05-17', '2011-03-16']
    assert [r['settlement_price_raw'] for r in result[0]['records']] == ['49.55', '245']
    assert result[0]['state'] == 'generation_or_duplicate_ambiguous'


def test_same_day_duplicates_remain_ambiguous():
    records = [row('A', '202501', date(2025, 1, 15), 1, '1'),
               row('A', '202501', date(2025, 1, 15), 2, '1')]
    assert len(settlement_collisions(records)[0]['records']) == 2


@pytest.mark.parametrize('field,value', [('product_raw', ''), ('series_raw', None),
                                         ('settlement_date', '2011-03-16'),
                                         ('source_row', True), ('query_month', '')])
def test_invalid_identity_fails_closed(field, value):
    record = row('A', '201103', date(2011, 3, 16), 1, '1')
    record[field] = value
    with pytest.raises(ValueError):
        settlement_collisions([record])
