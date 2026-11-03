from datetime import UTC, datetime
import hashlib
import json

import polars as pl
import pytest

from downloader import download_finmind_complement as owner
from scripts import audit_finmind_futures_tick_product_checks as readback
from scripts import request_finmind_futures_tick_product_checks as checks
from scripts import run_finmind_option_tick_product_priority_wait as waiter

NOW = datetime(2026, 10, 28, 17, tzinfo=UTC)
DAY = '2016-11-16'
PRODUCT = 'I5F'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def setup(tmp_path, monkeypatch):
    root = tmp_path / 'data_finmind' / 'complement'
    root.mkdir(parents=True)
    with owner._db(root / 'queue.sqlite3') as con:
        owner._add_tasks(con, [(checks.DATASET, '', DAY, 'id_day', 10)])
        con.execute("UPDATE tasks SET state='complete',rows=1 WHERE dataset=? AND data_id=''", (checks.DATASET,))
    baseline = root / 'original.parquet'
    pl.DataFrame({'date': [DAY + ' 09:00:01'], 'futures_id': [PRODUCT], 'contract_date': ['201611'],
                  'price': [12.], 'volume': [2]}).write_parquet(baseline)
    receipt = root / 'receipts' / checks.DATASET / 'all' / f'{DAY}.json'
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({'dataset': checks.DATASET, 'data_id': '', 'partition': DAY,
                                   'status': 'complete', 'rows': 1, 'parquet_path': baseline.name,
                                   'sha256': digest(baseline)}))
    sidecar = root / checks.BATCHES / 'one.json'
    sidecar.parent.mkdir()
    sidecar.write_text(json.dumps({'dates': [DAY], 'expires_at_utc': '2026-11-02T17:00:00+00:00'}))
    request = {'start_date': DAY, 'expected_source_futures_contracts': [[PRODUCT, '201612']],
               'original_parquet_path': baseline.name, 'original_parquet_sha256': digest(baseline)}
    monkeypatch.setattr(checks, 'load_futures_tick_conflicts', lambda *_a, **_kw: ({'requests': [request]}, 'pinned'))
    return root, baseline


def test_futures_tick_product_request_is_exact_and_owner_backed(tmp_path, monkeypatch):
    root, _ = setup(tmp_path, monkeypatch)
    path = tmp_path / 'source.json'
    registration = checks.register_checks(root, [(DAY, PRODUCT)], NOW, path)
    plan, status = waiter.checked_inputs(path, root, registration['request_id'], NOW)
    assert plan['contract'] == checks.CONTRACT
    assert plan['checks'][0]['audited_missing_contracts'] == [[PRODUCT, '201612']]
    assert waiter.ready_keys(status, NOW) == ((checks.DATASET, PRODUCT, DAY),)
    calls = []
    def fetch(_session, _limiter, _root, dataset, _token, params, **kwargs):
        calls.append((dataset, params, kwargs['endpoint']))
        return [{'date': DAY + ' 09:00:02', 'futures_id': PRODUCT}]
    monkeypatch.setattr(owner, '_fetch_rows', fetch)
    task = owner.Task(checks.DATASET, PRODUCT, DAY, 'id_day', 10, 'pending')
    _, metadata = owner._request_independent(None, None, root, task, 'not-a-real-token', today=NOW.date())
    assert metadata['query_shape'] == 'per_futures_day'
    assert calls[0][1] == {'data_id': PRODUCT, 'dataset': checks.DATASET, 'start_date': DAY}
    assert registration['provider_calls_by_registration'] == 0


def test_futures_native_readback_checks_keys_receipts_and_payload(tmp_path, monkeypatch):
    root, baseline = setup(tmp_path, monkeypatch)
    path = tmp_path / 'source.json'
    registration = checks.register_checks(root, [(DAY, PRODUCT)], NOW, path)
    parquet = root / 'product.parquet'
    pl.DataFrame({'date': [DAY + ' 09:00:02'], 'futures_id': [PRODUCT], 'contract_date': ['201612'],
                  'price': [11.], 'volume': [1]}).write_parquet(parquet)
    receipt = root / 'product.json'
    receipt.write_text(json.dumps({'dataset': checks.DATASET, 'data_id': PRODUCT, 'partition': DAY,
                                   'status': 'complete', 'rows': 1, 'parquet_path': parquet.name,
                                   'sha256': digest(parquet)}))
    import sqlite3
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute("UPDATE tasks SET state='complete',rows=1,receipt_path=? WHERE data_id=?", (receipt.name, PRODUCT))
    report = readback.audit(path, root, registration['request_id'], NOW)
    assert report['results'][0]['matched_contracts'] == 1
    assert report['results'][0]['status'] == 'candidate_keys_present'
    assert report['training_ready'] is False and report['provider_calls'] == 0
    baseline.write_bytes(b'changed')
    with pytest.raises(ValueError, match='evidence changed'):
        readback.audit(path, root, registration['request_id'], NOW)


def test_do_not_request_contract_already_in_current_whole_market_source(tmp_path, monkeypatch):
    root, baseline = setup(tmp_path, monkeypatch)
    pl.DataFrame({'date': [DAY + ' 09:00:01'], 'futures_id': [PRODUCT], 'contract_date': ['201612'],
                  'price': [12.], 'volume': [2]}).write_parquet(baseline)
    receipt = root / 'receipts' / checks.DATASET / 'all' / f'{DAY}.json'
    contents = json.loads(receipt.read_text())
    contents['sha256'] = digest(baseline)
    receipt.write_text(json.dumps(contents))
    monkeypatch.setattr(checks, 'load_futures_tick_conflicts', lambda *_a, **_kw: (
        {'requests': [{'start_date': DAY, 'expected_source_futures_contracts': [[PRODUCT, '201612']],
                       'original_parquet_path': baseline.name, 'original_parquet_sha256': digest(baseline)}]}, 'pinned'))
    with pytest.raises(ValueError, match='already has'):
        checks.register_checks(root, [(DAY, PRODUCT)], NOW, tmp_path / 'source.json')
