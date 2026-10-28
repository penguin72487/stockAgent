from datetime import date
import hashlib
import json

import polars as pl
import pytest

from scripts.audit_taifex_option_monthly_contract_presence import audit
from scripts.download_taifex_option_daily_history import TAIFEX_OPTION_DOWNLOAD_URL


def _range(root, first, last, rows):
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{first.isoformat()}_{last.isoformat()}_all.csv'
    header = '交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,收盤價,結算價,成交量,交易時段\n'
    path.write_bytes((header + rows).encode('cp950'))
    identity = {'contract': 'taifex_option_all_products_daily_raw_v1', 'url': TAIFEX_OPTION_DOWNLOAD_URL,
                'request': {'down_type': '1', 'queryStartDate': first.strftime('%Y/%m/%d'),
                            'queryEndDate': last.strftime('%Y/%m/%d'),
                            'commodity_id': 'all', 'commodity_id2': ''},
                'range_start': first.isoformat(), 'range_end': last.isoformat(),
                'point_in_time_snapshot': False}
    path.with_suffix('.intent.json').write_text(json.dumps(identity), encoding='utf-8')
    path.with_suffix('.receipt.json').write_text(json.dumps(
        {**identity, 'path': str(path), 'bytes': path.stat().st_size,
         'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}), encoding='utf-8')
    return path


def _fixture(tmp_path):
    source = _range(tmp_path / 'source', date(2026, 1, 1), date(2026, 1, 31),
                    '2026/01/03,TXO,202601,23000,買權,1,2,2,5,一般\n'
                    '2026/01/03,CAO,202601,23.50,賣權,1,2,2,2,一般\n'
                    '2026/01/04,TGO,202601F1,68,買權,1,2,2,2,一般\n')
    receipt = json.loads(source.with_suffix('.receipt.json').read_text())
    summary = {'contract': 'tw_option_all_product_monthly_range_readback_v1',
               'start': '2026-01-01', 'end': '2026-01-31', 'months_verified': 1,
               'sources': [{'start': '2026-01-01', 'end': '2026-01-31',
                            'path': str(source), 'receipt': str(source.with_suffix('.receipt.json')),
                            'sha256': receipt['sha256'], 'bytes': receipt['bytes']}]}
    summary_path = tmp_path / 'summary.json'
    summary_path.write_text(json.dumps(summary), encoding='utf-8')
    root = tmp_path / 'finmind'
    data = root / 'objects/2026-01-03.parquet'
    data.parent.mkdir(parents=True)
    pl.DataFrame({'option_id': ['TXO'], 'contract_date': ['202601'],
                  'ExercisePrice': [23000.0], 'PutCall': ['C']}).write_parquet(data)
    receipts = root / 'receipts/TaiwanOptionTick/all'
    receipts.mkdir(parents=True)
    native = {'dataset': 'TaiwanOptionTick', 'data_id': '', 'partition': '2026-01-03',
              'status': 'complete', 'parquet_path': 'objects/2026-01-03.parquet',
              'sha256': hashlib.sha256(data.read_bytes()).hexdigest(),
              'parquet_size_bytes': data.stat().st_size, 'rows': 1}
    (receipts / '2026-01-03.json').write_text(json.dumps(native))
    empty = {'dataset': 'TaiwanOptionTick', 'data_id': '', 'partition': '2026-01-04',
             'status': 'observed_empty', 'rows': 0}
    (receipts / '2026-01-04.json').write_text(json.dumps(empty))
    return summary_path, source, root


def test_current_month_audit_distinguishes_missing_contracts_from_empty_day(tmp_path):
    summary, _, root = _fixture(tmp_path)
    days, missing, result = audit(summary, root, workers=1)
    assert result['date_count'] == 2
    assert result['source_states'] == {'contract_key_readback_verified': 1, 'observed_empty': 1}
    assert result['verified_missing_contract_day_keys'] == 1
    assert days[0]['missing_contracts'] == 1
    assert missing == [{'date': '2026-01-03', 'product': 'CAO', 'series': '202601',
                        'strike': 23.5, 'right': 'P', 'session': 'day'}]
    assert days[1]['source_status'] == 'observed_empty'  # not an asserted no-trade day


@pytest.mark.parametrize('defect', ['source_bytes', 'receipt_identity', 'range_gap'])
def test_current_month_audit_fails_closed_on_source_defects(tmp_path, defect):
    summary, source, root = _fixture(tmp_path)
    if defect == 'source_bytes':
        with source.open('ab') as out:
            out.write(b'changed')
    elif defect == 'receipt_identity':
        path = source.with_suffix('.receipt.json')
        receipt = json.loads(path.read_text())
        receipt['point_in_time_snapshot'] = True
        path.write_text(json.dumps(receipt))
    else:
        reference = json.loads(summary.read_text())
        reference['sources'][0]['end'] = '2026-01-30'
        summary.write_text(json.dumps(reference))
    with pytest.raises(ValueError):
        audit(summary, root, workers=1)
