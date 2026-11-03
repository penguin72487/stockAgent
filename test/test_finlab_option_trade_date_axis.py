from datetime import date

import polars as pl

from scripts.audit_finlab_option_trade_date_axis import KEY, compare, vendor_keys


def _row(price=1.0, quantity=1.0):
    return {'product': 'TXO', 'series': '202612', 'strike': 25200.0, 'right': 'P',
            'time': '225948', 'price': price, 'quantity': quantity, 'auction': ''}


def test_duplicate_rows_resolve_only_with_unique_official_date_and_enough_count():
    vendor = pl.DataFrame([_row(), _row(), _row(2), _row(3), _row(3), _row(4)])
    official = pl.DataFrame([
        {**_row(), 'event_date': date(2026, 10, 22)},
        {**_row(), 'event_date': date(2026, 10, 22)},
        {**_row(2), 'event_date': date(2026, 10, 22)},
        {**_row(2), 'event_date': date(2026, 10, 23)},
        {**_row(3), 'event_date': date(2026, 10, 22)},
    ])
    result = compare(vendor, official)
    assert result['vendor_side_rows'] == 6
    assert result['matched_side_rows_upper_bound'] == 4
    assert result['unique_physical_date_and_sufficient_count_vendor_side_rows'] == 2
    assert result['unique_physical_date_by_vendor_side_rows'] == [
        {'official_event_date': '2026-10-22', 'vendor_side_rows': 2}]
    assert result['ambiguous_multiple_physical_dates_vendor_side_rows'] == 1
    assert result['insufficient_official_multiplicity_vendor_side_rows'] == 2
    assert result['no_official_key_vendor_side_rows'] == 1


def test_vendor_right_mapping_keeps_unknown_unmatched():
    original = pl.DataFrame({'symbol': ['TXO'], '到期月份(週別)': ['202612'],
                             '履約價格': [25200.0], '買賣權': ['?'], '成交時間': ['225948'],
                             '成交價格': [1.0], '成交數量': [1.0], '開盤集合競價': ['']})
    vendor = vendor_keys(original)
    assert vendor.columns == KEY
    assert vendor['right'][0] is None
    official = pl.DataFrame([{**_row(), 'event_date': date(2026, 10, 22)}])
    result = compare(vendor, official)
    assert result['invalid_vendor_side_rows'] == 1
    assert result['matched_side_rows_upper_bound'] == 0
