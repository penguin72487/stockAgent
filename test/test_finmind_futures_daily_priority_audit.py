import hashlib
import json

import polars as pl
import pytest

from scripts import audit_finmind_futures_daily_priority as auditor


def test_daily_priority_audit_checks_exact_identity_and_payload(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    root.mkdir()
    key = ('TaiwanFuturesDaily', 'GDF', 'history')
    plan = tmp_path / 'plan.json'
    plan.write_text(json.dumps({'contract': 'finmind-finite-product-query-verification-v1',
                                'keys': [{'dataset': key[0], 'data_id': key[1], 'partition': key[2]}]}))
    payload = root / 'data.parquet'
    pl.DataFrame({'date': ['2021-01-04', '2021-01-05'], 'futures_id': ['GDF', 'GDF'],
                  'contract_date': ['202101', '202102'], 'volume': [1, 0],
                  'open': [100., 101.], 'trading_session': ['position', 'position']}).write_parquet(payload)
    digest = hashlib.sha256(payload.read_bytes()).hexdigest()
    receipt = root / 'receipt.json'
    receipt.write_text(json.dumps({'dataset': key[0], 'data_id': key[1], 'partition': key[2],
                                   'status': 'complete', 'rows': 2, 'parquet_path': payload.name, 'sha256': digest}))
    native = {'key': json.dumps(key), 'state': 'complete', 'rows': 2, 'receipt_path': receipt.name}
    monkeypatch.setattr(auditor, 'inspect', lambda *a, **kw: {'provider': 'finmind', 'request_id': 'id', 'items': [native]})
    result = auditor.audit(root, plan, 'id')
    item = result['results'][0]
    assert (item['rows'], item['positive_volume_rows'], item['first_date'], item['last_date']) == (2, 1, '2021-01-04', '2021-01-05')
    assert item['status'] == 'source_payload_verified' and result['training_ready'] is False
    payload.write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='payload differs'):
        auditor.audit(root, plan, 'id')


def test_daily_priority_audit_rejects_unrequested_scope(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    root.mkdir()
    plan = tmp_path / 'plan.json'
    plan.write_text(json.dumps({'contract': 'finmind-finite-product-query-verification-v1',
                                'keys': [{'dataset': 'TaiwanFuturesDaily', 'data_id': 'GDF', 'partition': 'history'}]}))
    monkeypatch.setattr(auditor, 'inspect', lambda *a, **kw: {'provider': 'finmind', 'request_id': 'id', 'items': []})
    with pytest.raises(ValueError, match='differ from source plan'):
        auditor.audit(root, plan, 'id')
