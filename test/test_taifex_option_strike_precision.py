import hashlib
import json
import zipfile
from datetime import date

import polars as pl
import pytest

from scripts.audit_taifex_option_strike_precision import audit_source, collision_groups, exact_strike, read_inputs


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_decimal_normalization_preserves_value_and_all_digits():
    values = ['0003.000', '003.00500', '0000.00', '10000', '10000.00001',
              '1.00000000000000000001', '000.1', '100.010', '+00300.001']
    out = pl.DataFrame({'value': values}).select(exact_strike(pl.col('value'))).to_series().to_list()
    assert out == ['3', '3.005', '0', '10000', '10000.00001',
                   '1.00000000000000000001', '0.1', '100.01', '300.001']


def test_collisions_keep_date_product_expiry_and_right_boundaries():
    frame = pl.DataFrame({
        'date': [date(2020, 1, 1)] * 5 + [date(2020, 1, 2)],
        'product': ['TXO'] * 6,
        'series': ['202001', '202001', '202001', '202001W1', '202001', '202001'],
        'right': ['C', 'C', 'P', 'C', 'C', 'C'],
        'strike_decimal': ['10000', '10000.00001', '10000.00001', '10000.00001', '10000', '10000.00001'],
    }).with_columns(pl.col('strike_decimal').cast(pl.Float32).alias('strike_float32'))
    groups = collision_groups(frame).to_dicts()
    assert len(groups) == 1
    assert groups[0]['official_decimal_strikes'] == ['10000', '10000.00001']
    assert groups[0]['right'] == 'C' and groups[0]['series'] == '202001'


def test_raw_archive_cross_batch_identity_and_nonpositive_scope(tmp_path):
    path = tmp_path / '2020_opt.zip'
    text = ('交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,成交量,交易時段\n'
            '2020/01/02,TXO,202001,10000,買權,1,1,一般\n'
            '2020/01/02,TXO,202001,10000.00001,買權,1,1,一般\n'
            '2020/01/02,TXO,202001,010000.0,買權,1,1,一般\n'
            '2020/01/02,TXO,202001,10000.00001,賣權,1,1,一般\n'
            '2020/01/02,TXO,202001,1e6,買權,1,1,一般\n'
            '2020/01/02,TXO,202001,11000,買權,-,1,一般\n'
            '2020/01/02,TXO,202001,12000,買權,1,1,盤後\n')
    with zipfile.ZipFile(path, 'w') as archive:
        archive.writestr('daily.csv', text.encode('cp950'))
    result, groups = audit_source(path, digest(path), date(2020, 1, 1), date(2020, 12, 31))
    assert result == {'positive_open_rows': 5, 'unsupported_rows': 1,
                      'official_decimal_contract_day_keys': 3, 'official_float32_contract_day_keys': 2,
                      'supported_dates': 1, 'collision_groups': 1, 'collision_decimal_keys': 2}
    assert groups[0]['official_decimal_strikes'] == ['10000', '10000.00001']
    assert groups[0]['date'] == '2020-01-02'
    with pytest.raises(ValueError, match='digest mismatch'):
        audit_source(path, '0' * 64, date(2020, 1, 1), date(2020, 12, 31))


def test_source_receipts_and_ranges_are_bound(tmp_path):
    manifest = tmp_path / 'manifest.json'
    manifest.write_text('{}')
    annual = tmp_path / 'annual.json'
    annual.write_text(json.dumps({'contract': 'taifex_option_annual_positive_days_finmind_receipts_v1',
                                  'source_manifest': str(manifest), 'source_manifest_sha256': digest(manifest),
                                  'annual_sources': [{'path': str(tmp_path / '2020_opt.zip'), 'sha256': 'a' * 64}]}))
    raw = tmp_path / 'monthly.csv'
    raw.write_text('source')
    receipt = tmp_path / 'receipt.json'
    receipt.write_text(json.dumps({'contract': 'taifex_option_all_products_daily_raw_v1',
                                   'range_start': '2021-01-01', 'range_end': '2021-01-31',
                                   'point_in_time_snapshot': False, 'path': str(raw),
                                   'sha256': digest(raw), 'bytes': raw.stat().st_size}))
    monthly = tmp_path / 'monthly.json'
    monthly.write_text(json.dumps({'contract': 'tw_option_all_product_monthly_range_readback_v1',
                                   'start': '2021-01-01', 'end': '2021-01-31', 'months_verified': 1,
                                   'sources': [{'start': '2021-01-01', 'end': '2021-01-31', 'path': str(raw),
                                                'receipt': str(receipt), 'sha256': digest(raw),
                                                'bytes': raw.stat().st_size}]}))
    sources, _ = read_inputs(annual, monthly, [2020])
    assert len(sources) == 2 and sources[1]['receipt_sha256'] == digest(receipt)
    receipt.write_text(receipt.read_text().replace('2021-01-31', '2021-01-30'))
    with pytest.raises(ValueError, match='receipt identity mismatch'):
        read_inputs(annual, monthly, [2020])
