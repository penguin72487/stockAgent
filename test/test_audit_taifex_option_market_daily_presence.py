import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.audit_taifex_option_market_daily_presence import audit_day


DAY = '2011-01-01'
OFFICIAL = {'date': DAY, 'day_positive_products': 'TEO|TXO',
            'unreported_session_positive_products': 'CAO|TEO',
            'night_positive_products': 'TXO', 'unknown_session_positive_products': ''}


def write_receipt(root: Path, status='complete', wrong_date=False):
    path = root / 'receipts/TaiwanOptionDaily/all' / f'{DAY}.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    receipt = {'dataset': 'TaiwanOptionDaily', 'data_id': '', 'kind': 'day',
               'partition': DAY, 'status': status, 'rows': 0}
    if status == 'complete':
        parquet = root / 'parquet/TaiwanOptionDaily/all' / DAY / 'rows.parquet'
        parquet.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table({'date': ['2011-01-02' if wrong_date else DAY, DAY],
                          'option_id': ['TEO', 'TXO'], 'volume': [3, 0],
                          'trading_session': ['position', 'after_market']})
        pq.write_table(table, parquet)
        receipt.update(rows=2, parquet_path=str(parquet.relative_to(root)),
                       parquet_size_bytes=parquet.stat().st_size,
                       sha256=hashlib.sha256(parquet.read_bytes()).hexdigest())
    path.write_text(json.dumps(receipt))
    return receipt


def test_verified_products_preserve_official_sessions_and_positive_volume(tmp_path):
    write_receipt(tmp_path)
    row = audit_day(OFFICIAL, tmp_path)
    assert row['source_status'] == 'daily_readback_verified'
    assert row['vendor_products'] == 'TEO|TXO'
    assert row['vendor_positive_volume_products'] == 'TEO'
    assert row['vendor_trading_sessions'] == 'after_market|position'
    assert row['missing_unreported_session_products'] == 'CAO'
    assert row['missing_day_positive_volume_products'] == 'TXO'


def test_missing_and_observed_empty_are_distinct(tmp_path):
    assert audit_day(OFFICIAL, tmp_path)['source_status'] == 'missing_receipt'
    write_receipt(tmp_path, 'observed_empty')
    row = audit_day(OFFICIAL, tmp_path)
    assert row['source_status'] == 'observed_empty'
    assert row['official_day_positive_products'] == 'TEO|TXO'
    assert row['vendor_products'] == ''


def test_rejects_tampered_payload_and_wrong_partition(tmp_path):
    receipt = write_receipt(tmp_path)
    path = tmp_path / receipt['parquet_path']
    path.write_bytes(path.read_bytes() + b'tamper')
    row = audit_day(OFFICIAL, tmp_path)
    assert row['source_status'] == 'daily_readback_failed'
    assert row['error'] == 'parquet_size_mismatch'
    write_receipt(tmp_path, wrong_date=True)
    row = audit_day(OFFICIAL, tmp_path)
    assert row['source_status'] == 'daily_readback_failed'
    assert row['error'] == 'invalid row date or product identity'


def test_legacy_month_receipt_is_only_an_observation_of_its_first_day(tmp_path):
    receipt = write_receipt(tmp_path)
    receipt.update(kind='month', source_first_date=DAY, source_last_date=DAY)
    path = tmp_path / 'receipts/TaiwanOptionDaily/all' / f'{DAY}.json'
    path.write_text(json.dumps(receipt))
    row = audit_day(OFFICIAL, tmp_path)
    assert row['source_status'] == 'daily_readback_verified'
    assert row['source_kind'] == 'month'
    receipt['source_last_date'] = '2011-01-02'
    path.write_text(json.dumps(receipt))
    assert audit_day(OFFICIAL, tmp_path)['error'] == 'unsupported source query shape'
