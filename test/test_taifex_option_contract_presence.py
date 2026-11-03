import hashlib
import json
import zipfile

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.audit_taifex_option_contract_presence import audit_day, compare_contracts, official_year


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_contract_comparison_does_not_merge_right_expiry_or_strike():
    official = pl.DataFrame({'product': ['TXO'] * 4, 'series': ['202610', '202610', '202610W1', '202610'],
                             'strike': [27000.0, 27000.0, 27000.0, 27100.0], 'right': ['C', 'P', 'C', 'C']},
                            schema_overrides={'strike': pl.Float32})
    vendor = pl.DataFrame({'option_id': ['TXO', 'TXO'], 'contract_date': ['202610', '202610'],
                            'ExercisePrice': [27000.0, 27000.0], 'PutCall': ['C', 'C']})
    assert compare_contracts(official, vendor).to_dicts() == [
        {'product': 'TXO', 'series': '202610', 'strike': 27000.0, 'right': 'P'},
        {'product': 'TXO', 'series': '202610', 'strike': 27100.0, 'right': 'C'},
        {'product': 'TXO', 'series': '202610W1', 'strike': 27000.0, 'right': 'C'},
    ]


def test_monthly_decimal_label_is_normalized_without_merging_weeks_or_other_decimals():
    official = pl.DataFrame({'product': ['TXO'] * 6,
                             'series': ['202006', '202006W1', '202007', '202008', '202009', '202010'],
                             'strike': [100.] * 6, 'right': ['C'] * 6},
                            schema_overrides={'strike': pl.Float32})
    vendor = pl.DataFrame({'option_id': ['TXO'] * 6,
                           'contract_date': ['202006.0', '202006W1.0', '202007.1',
                                             '202008.00', '202009/202010', '202010W1'],
                           'ExercisePrice': [100.] * 6, 'PutCall': ['C'] * 6})
    missing = compare_contracts(official, vendor)
    assert set(missing['series']) == {'202006W1', '202007', '202008', '202009', '202010'}
    assert vendor['contract_date'][0] == '202006.0'


def test_ordinary_open_source_and_receipt_verified_without_fabricating_fills(tmp_path):
    archive = tmp_path / '2022_opt.zip'
    csv_data = ('交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,成交量,交易時段\n'
                '2022/07/05,TXO,202207,14000,買權,5.0,1,一般\n'
                '2022/07/05,CAO,202207,24.5,賣權,0.44,1,一般\n'
                '2022/07/05,TXO,202207,14100,買權,-,5,一般\n'
                '2022/07/05,TXO,202207,14200,買權,6,1,盤後\n'
                '2022/07/05,TXO,202207/202208,14300,買權,7,1,一般\n')
    with zipfile.ZipFile(archive, 'w') as file:
        file.writestr('daily.csv', csv_data.encode('cp950'))
    daily, stats = official_year(archive, digest(archive), 2022)
    assert stats['unsupported_rows'] == 1
    assert daily['2022-07-05'].height == 2
    parquet = tmp_path / 'ticks.parquet'
    pq.write_table(pa.table({'option_id': ['TXO'], 'contract_date': ['202207'],
                             'ExercisePrice': pa.array([14000.0], type=pa.float32()),
                             'PutCall': ['C']}), parquet)
    receipt = tmp_path / 'receipt.json'
    receipt.write_text(json.dumps({'dataset': 'TaiwanOptionTick', 'data_id': '', 'partition': '2022-07-05',
                                   'status': 'complete', 'rows': 1, 'parquet_path': parquet.name,
                                   'parquet_size_bytes': parquet.stat().st_size, 'sha256': digest(parquet)}))
    row = {'date': '2022-07-05', 'finmind_status': 'complete_payload_sha_verified',
           'finmind_receipt_path': str(receipt), 'finmind_receipt_sha256': digest(receipt),
           'finmind_parquet_path': str(parquet), 'finmind_parquet_sha256': digest(parquet)}
    result, missing = audit_day(row, tmp_path.resolve(), daily['2022-07-05'])
    assert result['source_status'] == 'contract_key_readback_verified'
    assert result['missing_day_session_contracts'] == 1
    assert missing == [{'date': '2022-07-05', 'product': 'CAO', 'series': '202207',
                        'strike': 24.5, 'right': 'P', 'session': 'day'}]
    parquet.write_bytes(parquet.read_bytes() + b'corrupt')
    result, missing = audit_day(row, tmp_path.resolve(), daily['2022-07-05'])
    assert result['source_status'] == 'contract_key_readback_failed' and not missing


def test_unknown_session_and_empty_object_do_not_claim_completeness(tmp_path):
    archive = tmp_path / '2011_opt.zip'
    csv_data = ('交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,成交量\n'
                '2011/02/16,TXO,201102,9000,買權,10,1\n')
    with zipfile.ZipFile(archive, 'w') as file:
        file.writestr('daily.csv', csv_data.encode('cp950'))
    daily, _ = official_year(archive, digest(archive), 2011)
    result, details = audit_day({'date': '2011-02-16', 'finmind_status': 'observed_empty'},
                                tmp_path.resolve(), daily['2011-02-16'])
    assert result['unreported_session_contracts'] == 1
    assert result['source_status'] == 'observed_empty' and not details


def test_friday_weekly_option_series_is_preserved(tmp_path):
    archive = tmp_path / '2025_opt.zip'
    csv_data = ('交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,成交量,交易時段\n'
                '2025/11/14,TXO,202511F4,25100,賣權,52,90,一般\n')
    with zipfile.ZipFile(archive, 'w') as file:
        file.writestr('daily.csv', csv_data.encode('cp950'))
    daily, stats = official_year(archive, digest(archive), 2025)
    assert stats['unsupported_rows'] == 0
    assert daily['2025-11-14']['series'].to_list() == ['202511F4']
