import hashlib
import json
from pathlib import Path
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.audit_taifex_option_product_presence import audit_day
from scripts.audit_taifex_option_missing_open import audit


def test_daily_product_presence_distinguishes_day_and_unknown_session_and_detects_mutation(tmp_path):
    root = tmp_path.resolve()
    file = root / 'ticks.parquet'
    pq.write_table(pa.table({'option_id': ['TXO', 'TXO', 'TGO']}), file)
    digest = hashlib.sha256(file.read_bytes()).hexdigest()
    receipt = root / 'receipt.json'
    receipt.write_text(json.dumps({'dataset': 'TaiwanOptionTick', 'data_id': '', 'partition': '2011-01-03',
                                   'status': 'complete', 'rows': 3, 'parquet_path': file.name,
                                   'parquet_size_bytes': file.stat().st_size, 'sha256': digest}))
    row = {'date': '2011-01-03', 'day_positive_products': 'TXO|CAO',
           'unreported_session_positive_products': 'CCO|TGO', 'finmind_status': 'complete_payload_sha_verified',
           'finmind_receipt_path': str(receipt), 'finmind_receipt_sha256': hashlib.sha256(receipt.read_bytes()).hexdigest(),
           'finmind_parquet_path': str(file), 'finmind_parquet_sha256': digest}
    result = audit_day(row, root)
    assert result['source_status'] == 'product_readback_verified'
    assert result['finmind_products'] == 'TGO|TXO'
    assert result['missing_day_products'] == 'CAO'
    assert result['missing_unreported_products'] == 'CCO'
    file.write_bytes(file.read_bytes() + b'changed')
    assert audit_day(row, root)['source_status'] == 'product_readback_failed'


def test_missing_object_is_not_classified_as_product_level_complete(tmp_path):
    row = {'date': '2011-01-03', 'day_positive_products': 'TXO',
           'unreported_session_positive_products': '', 'finmind_status': 'observed_empty'}
    result = audit_day(row, Path(tmp_path))
    assert result['source_status'] == 'observed_empty'
    assert result['missing_day_products'] == ''


def test_open_evidence_separates_block_only_and_uses_day_session(tmp_path):
    archive = tmp_path / '2022_opt.zip'
    source = ('交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,成交量,交易時段\n'
              '2022/07/05,OJO,202207,24.5,買權,0.44,1,一般\n'
              '2022/07/05,CAO,202207,45,賣權,-,100,一般\n'
              '2022/07/05,CAO,202207,45,賣權,2.00,1,盤後\n')
    with zipfile.ZipFile(archive, 'w') as z:
        z.writestr('daily.csv', source.encode('cp950'))
    sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    coverage = tmp_path / 'coverage.json'
    coverage.write_text(json.dumps({'annual_sources': [{'path': str(archive), 'sha256': sha(archive)}]}))
    inventory = tmp_path / 'inventory.csv'
    inventory.write_text('date,source_status,missing_day_products\n2022-07-05,product_readback_verified,CAO|OJO\n')
    presence = tmp_path / 'presence.json'
    presence.write_text(json.dumps({'contract': 'taifex_option_daily_vs_finmind_product_presence_v1',
                                    'day_inventory': str(inventory), 'day_inventory_sha256': sha(inventory),
                                    'coverage_summary_sha256': sha(coverage)}))
    rows, summary = audit(coverage, presence)
    result = {row['product']: row for row in rows}
    assert result['OJO']['classification'] == 'official_open_missing_from_object'
    assert result['OJO']['example_open_price'] == '0.44'
    assert result['CAO']['classification'] == 'daily_volume_only_unresolved'
    assert summary['by_year'][0]['ordinary_open_product_days'] == 1
