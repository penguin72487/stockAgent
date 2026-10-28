import hashlib
import json

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.audit_taifex_option_market_daily_contracts import audit_day, vendor_keys
from scripts.audit_taifex_option_market_daily_presence import audit_day as audit_products

OFFICIAL = {'date': '2011-01-03', 'day_positive_products': 'TEO|TXO',
            'unreported_session_positive_products': 'CAO|TEO',
            'night_positive_products': '', 'unknown_session_positive_products': ''}


def test_vendor_identity_normalization_and_positive_volume():
    vendor = pl.DataFrame({'option_id': [' txo ', 'TXO', 'CAO'],
                           'contract_date': ['202610W1', '202610', 'other'],
                           'strike_price': [20000., 19900., 5.],
                           'call_put': ['call', 'P', 'put'], 'volume': [3, 0, 7]})
    assert set(vendor_keys(vendor).iter_rows()) == {
        ('TXO', '202610W1', 20000., 'C'), ('TXO', '202610', 19900., 'P')}
    assert set(vendor_keys(vendor, positive=True).iter_rows()) == {('TXO', '202610W1', 20000., 'C')}


def test_audit_distinguishes_missing_contract_and_zero_volume(tmp_path):
    path = tmp_path / 'parquet/TaiwanOptionDaily/all/2011-01-03/rows.parquet'
    path.parent.mkdir(parents=True)
    pq.write_table(pa.table({'date': [OFFICIAL['date']] * 2, 'option_id': ['TEO', 'TXO'],
                             'contract_date': ['201101', '201101'], 'strike_price': [100., 200.],
                             'call_put': ['call', 'put'], 'volume': [3, 0],
                             'trading_session': ['position', 'position']}), path)
    receipt = {'dataset': 'TaiwanOptionDaily', 'data_id': '', 'kind': 'day',
               'partition': OFFICIAL['date'], 'status': 'complete', 'rows': 2,
               'parquet_path': str(path.relative_to(tmp_path)), 'parquet_size_bytes': path.stat().st_size,
               'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    receipt_path = tmp_path / 'receipts/TaiwanOptionDaily/all' / f"{OFFICIAL['date']}.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(json.dumps(receipt))
    product = audit_products(OFFICIAL, tmp_path)
    official = pl.DataFrame({'product': ['TEO', 'TXO', 'CAO'], 'series': ['201101'] * 3,
                             'strike': pl.Series([100., 200., 50.], dtype=pl.Float32),
                             'right': ['C', 'P', 'C'], 'session': ['day', 'day', 'unreported']})
    row, details = audit_day(product, tmp_path, official)
    assert row['source_status'] == 'daily_contract_readback_verified'
    assert row['missing_keys'] == 1
    assert row['missing_positive_volume_keys'] == 2
    assert row['missing_day_keys'] == row['missing_unreported_keys'] == 1
    assert {(x['product'], x['missing_entirely']) for x in details} == {('TXO', False), ('CAO', True)}

    path.write_bytes(path.read_bytes() + b'tamper')
    row, _ = audit_day(product, tmp_path, official)
    assert row['source_status'] == 'daily_contract_readback_failed'
