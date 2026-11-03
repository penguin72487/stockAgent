from datetime import date, datetime

import polars as pl

from scripts.request_finmind_futures_evening_event_checks import compare


def test_event_gap_is_not_misreported_as_missing_contract():
    official = pl.DataFrame([
        {'product': 'TX', 'contract': '202612', 'event_ts': datetime(2026, 10, 21, 15, 1), 'price': 100.0},
        {'product': 'TX', 'contract': '202612', 'event_ts': datetime(2026, 10, 21, 15, 2), 'price': 101.0},
        {'product': 'TX', 'contract': '202612', 'event_ts': datetime(2026, 10, 21, 13, 0), 'price': 99.0},
    ])
    vendor = pl.DataFrame([
        {'futures_id': 'TX', 'contract_date': '202612', 'date': '2026-10-21 13:00:00', 'price': 99.0, 'volume': 1},
        {'futures_id': 'TX', 'contract_date': '202612.0', 'date': '2026-10-21 15:01:00', 'price': 100.0, 'volume': 1},
    ])
    result = compare(official, vendor, date(2026, 10, 21), ('TX',))
    assert result['official_event_keys'] == 2
    assert result['vendor_event_keys'] == 1
    assert result['official_keys_absent_in_vendor_capture'] == 1
    assert result['by_product'][0]['examples'][0]['event_ts'] == '2026-10-21T15:02:00'
