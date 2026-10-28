from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_futures_holiday_night_observations import last_reviewed_holiday_night_ticks


def test_gold_regular_afternoon_is_not_misclassified_as_night():
    rows = pl.DataFrame({
        'futures_id': ['TGF', 'TGF', 'TGF', 'TX', 'TX'],
        'contract_date': ['202302'] * 5,
        'date': ['2023-01-17 15:32:28', '2023-01-17 16:10:00', '2023-01-17 17:25:01',
                 '2023-01-17 15:00:00', '2023-01-18 04:59:59'],
        'price': [7011.0, 7012.0, 7013.0, 14000.0, 14001.0],
        'volume': [1, 1, 1, 1, 1],
    })
    result = last_reviewed_holiday_night_ticks(rows, evening=date(2023, 1, 17),
                                                morning=date(2023, 1, 18))
    assert result['TGF', '202302']['timestamp'] == '2023-01-17 17:25:01'
    assert result['TGF', '202302']['prices'] == [7013.0]
    assert result['TX', '202302']['timestamp'] == '2023-01-18 04:59:59'
    only_regular = rows.filter((pl.col('futures_id') == 'TGF') & (pl.col('date') < '2023-01-17 17:25:00'))
    assert last_reviewed_holiday_night_ticks(only_regular, evening=date(2023, 1, 17),
                                             morning=date(2023, 1, 18)) == {}


def test_last_timestamp_ties_remain_unordered_and_unknown_products_fail_closed():
    rows = pl.DataFrame({'futures_id': ['UDF', 'UDF'], 'contract_date': ['202303', '202303'],
                         'date': ['2023-01-18 04:59:49'] * 2, 'price': [33994.0, 34000.0],
                         'volume': [1, 1]})
    result = last_reviewed_holiday_night_ticks(rows, evening=date(2023, 1, 17),
                                                morning=date(2023, 1, 18))
    assert result['UDF', '202303']['prices'] == [33994.0, 34000.0]
    assert result['UDF', '202303']['unique_price_at_last_timestamp'] is False
    with pytest.raises(ValueError, match='unreviewed'):
        last_reviewed_holiday_night_ticks(rows.with_columns(pl.lit('XYZ').alias('futures_id')),
                                          evening=date(2023, 1, 17), morning=date(2023, 1, 18))
    with pytest.raises(ValueError, match='unsupported'):
        last_reviewed_holiday_night_ticks(rows, evening=date(2023, 1, 17), morning=date(2023, 1, 19))
