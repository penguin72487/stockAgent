from datetime import UTC, datetime
import hashlib
import json

import polars as pl
import pytest

from downloader import download_finmind_complement as owner
from downloader.provider_priority import inspect
from scripts import audit_finmind_option_tick_product_checks as readback
from scripts import request_finmind_option_tick_product_checks as checks
from scripts import request_finmind_option_tick_corrected_product_checks as corrected_checks
from scripts import run_finmind_option_corrected_product_backlog as backlog
from scripts import run_finmind_option_tick_product_priority_wait as waiter


NOW = datetime(2026, 10, 28, 17, tzinfo=UTC)
DAY = '2020-11-17'
PRODUCT = 'CDO'


def fixture(tmp_path, monkeypatch):
    root = tmp_path / 'data_finmind' / 'complement'
    root.mkdir(parents=True)
    with owner._db(root / 'queue.sqlite3') as con:
        owner._add_tasks(con, [(checks.DATASET, '', DAY, 'id_day', 10)])
        con.execute("UPDATE tasks SET state='complete',rows=2 WHERE dataset=? AND data_id=''", (checks.DATASET,))
    sidecar = root / 'option_contract_conflict_batches' / 'one.json'
    sidecar.parent.mkdir()
    sidecar.write_text(json.dumps({'dates': [DAY], 'expires_at_utc': '2026-11-02T17:00:00+00:00'}))
    receipt = root / 'receipts' / checks.DATASET / 'all' / f'{DAY}.json'
    receipt.parent.mkdir(parents=True)
    receipt.write_text('{"original":true}')
    parquet = root / 'parquet' / checks.DATASET / 'all' / DAY / 'original.parquet'
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b'original-provider-payload')
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    request = {'start_date': DAY, 'expected_source_contracts': [[PRODUCT, '202012', 20., 'C']],
               'original_parquet_path': str(parquet.relative_to(root)), 'original_parquet_sha256': digest(parquet),
               'original_receipt_sha256': digest(receipt)}
    monkeypatch.setattr(checks, 'load_option_contract_conflicts',
                        lambda *_a, **_kw: ({'requests': [request]}, 'verified-sidecar'))
    return root, receipt, parquet


def test_register_keeps_whole_market_task_and_adds_only_exact_product_day(tmp_path, monkeypatch):
    root, _, _ = fixture(tmp_path, monkeypatch)
    output = tmp_path / 'source-evidence.json'
    result = checks.register_checks(root, [(DAY, PRODUCT)], NOW, output)
    assert result['provider_calls_by_registration'] == 0
    assert result['source_complete'] is False and result['training_ready'] is False
    status = inspect(root, result['request_id'], now=NOW)
    assert status['priority_active'] and status['items'][0]['state'] == 'pending'
    with sqlite3_connect(root) as con:
        rows = con.execute('SELECT data_id,partition,kind,state,rows FROM tasks ORDER BY data_id').fetchall()
        assert rows == [('', DAY, 'id_day', 'complete', 2), (PRODUCT, DAY, 'id_day', 'pending', 0)]
        selected = owner._next_task(con, NOW, required_keys=((checks.DATASET, PRODUCT, DAY),))
        assert selected is not None and selected.data_id == PRODUCT
    saved = json.loads(output.read_text())
    assert saved['checks'][0]['audited_missing_contracts'] == [[PRODUCT, '202012', 20.0, 'C']]


def sqlite3_connect(root):
    import sqlite3
    return sqlite3.connect(root / 'queue.sqlite3')


def test_product_task_uses_the_existing_per_id_provider_contract(tmp_path, monkeypatch):
    root, _, _ = fixture(tmp_path, monkeypatch)
    checks.register_checks(root, [(DAY, PRODUCT)], NOW, tmp_path / 'source.json')
    calls = []
    def fetch(_session, _limiter, _traffic_root, dataset, _token, params, **kwargs):
        calls.append((dataset, params, kwargs['endpoint']))
        return [{'date': DAY + ' 09:00:02', 'option_id': PRODUCT}]
    monkeypatch.setattr(owner, '_fetch_rows', fetch)
    task = owner.Task(checks.DATASET, PRODUCT, DAY, 'id_day', 10, 'pending')
    rows, metadata = owner._request_independent(None, None, root, task, 'not-a-real-token', today=NOW.date())
    assert rows[0]['option_id'] == PRODUCT
    assert calls == [(checks.DATASET, {'data_id': PRODUCT, 'dataset': checks.DATASET,
                                      'start_date': DAY}, 'https://api.finmindtrade.com/api/v4/data')]
    assert metadata['query_shape'] == 'per_options_day'


@pytest.mark.parametrize('mutation', ['receipt', 'parquet', 'wrong_product', 'duplicate'])
def test_source_mismatch_fails_before_creating_tasks(tmp_path, monkeypatch, mutation):
    root, receipt, parquet = fixture(tmp_path, monkeypatch)
    selected = [(DAY, PRODUCT)]
    if mutation == 'receipt':
        receipt.write_text('changed')
    elif mutation == 'parquet':
        parquet.write_bytes(b'changed')
    elif mutation == 'wrong_product':
        selected = [(DAY, 'OTHER')]
    else:
        selected *= 2
    with pytest.raises(ValueError):
        checks.register_checks(root, selected, NOW, tmp_path / 'source.json')
    with sqlite3_connect(root) as con:
        assert con.execute('SELECT count(*) FROM tasks').fetchone()[0] == 1


def test_existing_terminal_product_is_not_reopened(tmp_path, monkeypatch):
    root, _, _ = fixture(tmp_path, monkeypatch)
    with sqlite3_connect(root) as con:
        con.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,rows) "
                    "VALUES (?,?,?,'id_day',10,'observed_empty',0)", (checks.DATASET, PRODUCT, DAY))
    result = checks.register_checks(root, [(DAY, PRODUCT)], NOW, tmp_path / 'source.json')
    assert result['items'][0]['state'] == 'observed_empty'
    with sqlite3_connect(root) as con:
        assert con.execute('SELECT state FROM tasks WHERE data_id=?', (PRODUCT,)).fetchone()[0] == 'observed_empty'


def test_waiter_requires_the_same_source_and_priority_receipt(tmp_path, monkeypatch):
    root, receipt, _ = fixture(tmp_path, monkeypatch)
    path = tmp_path / 'source.json'
    result = checks.register_checks(root, [(DAY, PRODUCT)], NOW, path)
    _, status = waiter.checked_inputs(path, root, result['request_id'], NOW)
    key = (checks.DATASET, PRODUCT, DAY)
    assert waiter.ready_keys(status, NOW) == (key,)
    assert waiter.owner_command(root, result['request_id'], (key,))[-4:] == ['--task-key', *key]
    status['items'][0]['next_attempt_at_utc'] = '2026-10-29T17:00:00+00:00'
    assert waiter.ready_keys(status, NOW) == ()
    receipt.write_text('changed')
    with pytest.raises(ValueError, match='evidence changed'):
        waiter.checked_inputs(path, root, result['request_id'], NOW)


def test_waiter_only_shortens_sampling_retry_for_a_fresh_disk_guard(tmp_path):
    status = tmp_path / 'status.json'
    started = datetime(2026, 10, 28, 17, tzinfo=UTC)
    result = {'state': 'disk_guard', 'observed_at_utc': '2026-10-28T17:00:01+00:00',
              'capacity_admission': {'reason': 'capacity_sample_expired'}}
    status.write_text(json.dumps(result))
    assert waiter.retry_delay('disk_guard', tmp_path, started) == (5, 'capacity_sample_expired')
    result['capacity_admission']['reason'] = 'physical_backing_reserve'
    status.write_text(json.dumps(result))
    assert waiter.retry_delay('disk_guard', tmp_path, started) == (60, 'physical_backing_reserve')
    result['observed_at_utc'] = '2026-10-28T16:59:59+00:00'
    status.write_text(json.dumps(result))
    assert waiter.retry_delay('disk_guard', tmp_path, started) == (60, None)
    status.write_text('malformed')
    assert waiter.retry_delay('disk_guard', tmp_path, started) == (60, None)
    assert waiter.retry_delay('batch_complete', tmp_path, started) == (10, None)
    assert waiter.retry_delay('rate_limited', tmp_path, started) == (300, None)


def test_waiter_observes_canonical_lock_without_starting_an_owner(tmp_path):
    import fcntl
    import threading
    import time

    lock = tmp_path / 'worker.lock'
    lock.write_text('')
    with lock.open('r') as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert not waiter.wait_until_owner_unlock(tmp_path, timeout_seconds=.03, poll_seconds=.01)
        thread = threading.Thread(target=lambda: (time.sleep(.03), fcntl.flock(handle.fileno(), fcntl.LOCK_UN)))
        thread.start()
        assert waiter.wait_until_owner_unlock(tmp_path, timeout_seconds=1, poll_seconds=.01)
        thread.join()
    assert not waiter.wait_until_owner_unlock(tmp_path / 'missing', timeout_seconds=.01, poll_seconds=.01)


def test_priority_wait_wakes_on_verified_local_budget_without_dispatch(tmp_path, monkeypatch):
    elapsed = [0.0]
    checks = []
    monkeypatch.setattr(waiter, 'priority_dispatch_ready', lambda root, now: checks.append((root, now)) or len(checks) == 2)
    def advance(seconds):
        elapsed[0] += seconds
    assert waiter.wait_for_priority_dispatch(tmp_path, timeout_seconds=60, poll_seconds=5,
        monotonic=lambda: elapsed[0], sleep=advance, clock=lambda: NOW)
    assert elapsed[0] == 10 and len(checks) == 2


def test_priority_wait_keeps_backoff_when_local_budget_is_unverified(tmp_path, monkeypatch):
    elapsed = [0.0]
    monkeypatch.setattr(waiter, 'priority_dispatch_ready', lambda *_a: False)
    def advance(seconds):
        elapsed[0] += seconds
    assert not waiter.wait_for_priority_dispatch(tmp_path, timeout_seconds=12, poll_seconds=5,
        monotonic=lambda: elapsed[0], sleep=advance, clock=lambda: NOW)
    assert elapsed[0] == 12


def test_corrected_priority_uses_only_still_missing_keys_and_pins_audit(tmp_path, monkeypatch):
    root, receipt, parquet = fixture(tmp_path, monkeypatch)
    original_key = [PRODUCT, '202012', 20., 'C']
    false_old_key = [PRODUCT, '202012', 21., 'C']
    old = {'start_date': DAY, 'expected_source_contracts': [original_key, false_old_key],
           'original_parquet_path': str(parquet.relative_to(root)),
           'original_parquet_sha256': hashlib.sha256(parquet.read_bytes()).hexdigest(),
           'original_receipt_sha256': hashlib.sha256(receipt.read_bytes()).hexdigest()}
    monkeypatch.setattr(checks, 'load_option_contract_conflicts', lambda *_a, **_kw: ({'requests': [old]}, 'x'))
    monkeypatch.setattr(corrected_checks, 'load_option_contract_conflicts', lambda *_a, **_kw: ({'requests': [old]}, 'x'))
    script = tmp_path / 'corrected.py'
    script.write_text('corrected')
    monkeypatch.setattr(corrected_checks, '__file__', str(script))
    audit = tmp_path / 'readback.json'
    audit.write_text('verified')
    digest = hashlib.sha256(audit.read_bytes()).hexdigest()
    monkeypatch.setattr(corrected_checks, 'corrected_contract_readbacks', lambda *_a: (
        {DAY: {'verified': True, 'receipt': old['original_receipt_sha256'],
               'parquet': old['original_parquet_sha256'], 'keys': {tuple(original_key)}}},
        {str(audit): digest}))
    path = tmp_path / 'source.json'
    result = corrected_checks.register_corrected(root, [audit], path, NOW)
    assert result['distinct_products'] == 1 and result['audited_missing_contract_keys'] == 1
    saved = json.loads(path.read_text())
    assert saved['checks'][0]['audited_missing_contracts'] == [original_key]
    waiter.checked_inputs(path, root, result['request_id'], NOW)
    audit.write_text('tampered')
    with pytest.raises(ValueError, match='additional source evidence changed'):
        waiter.checked_inputs(path, root, result['request_id'], NOW)


def test_backlog_keeps_date_product_scope_and_unknowns_separate():
    keys = {('TXO', '202001', 12000., 'C'), ('TXO', '202001W1', 12000., 'P'),
            ('TGO', '202001', 1000., 'C')}
    request = {'start_date': '2020-01-02', 'original_receipt_sha256': 'r',
               'original_parquet_sha256': 'p', 'expected_source_contracts': [list(k) for k in keys]}
    readbacks = {'2020-01-02': {'verified': True, 'receipt': 'r', 'parquet': 'p', 'keys': keys}}
    assert backlog.product_days([request], readbacks, {('2020-01-02', 'TGO')}) == [('2020-01-02', 'TXO')]
    assert backlog.product_days([request], {**readbacks, '2020-01-02': {**readbacks['2020-01-02'],
                                                                      'verified': False}}, set()) == []
    assert backlog.product_days([{**request, 'original_receipt_sha256': 'different'}], readbacks, set()) == []
    with pytest.raises(ValueError, match='differ from original'):
        backlog.product_days([{**request, 'expected_source_contracts': []}], readbacks, set())


def test_backlog_retries_only_unstarted_registration_on_owner_lock_contention(tmp_path, monkeypatch):
    path = tmp_path / 'batch.json'
    def locked(*_args, **_kwargs):
        raise BlockingIOError('native owner lock busy')
    monkeypatch.setattr(backlog, 'register_evidenced', locked)
    assert backlog.register_if_owner_idle(tmp_path, [], NOW, path, {}) is None
    # Never retry past partial evidence, which must be reconciled explicitly.
    path.write_text('{}')
    with pytest.raises(BlockingIOError):
        backlog.register_if_owner_idle(tmp_path, [], NOW, path, {})


def test_readback_distinguishes_contract_presence_from_source_completeness(tmp_path, monkeypatch):
    root, _, _ = fixture(tmp_path, monkeypatch)
    path = tmp_path / 'source.json'
    result = checks.register_checks(root, [(DAY, PRODUCT)], NOW, path)
    parquet = root / 'per-product.parquet'
    pl.DataFrame({'date': [DAY + ' 09:00:01'], 'option_id': [PRODUCT], 'contract_date': ['202012.0'],
                  'ExercisePrice': [20.], 'PutCall': ['C'], 'price': [1.25], 'volume': [1]}).write_parquet(parquet)
    receipt = root / 'per-product.json'
    receipt.write_text(json.dumps({'dataset': checks.DATASET, 'data_id': PRODUCT, 'partition': DAY,
                                   'status': 'complete', 'rows': 1, 'parquet_path': parquet.name,
                                   'sha256': hashlib.sha256(parquet.read_bytes()).hexdigest()}))
    with sqlite3_connect(root) as con:
        con.execute("UPDATE tasks SET state='complete',rows=1,receipt_path=? WHERE data_id=?",
                    (receipt.name, PRODUCT))
    report = readback.audit(path, root, result['request_id'], NOW)
    row = report['results'][0]
    assert row['status'] == 'candidate_keys_present' and row['matched_contracts'] == 1
    assert report['training_ready'] is False and report['provider_calls'] == 0
    parquet.write_bytes(b'changed')
    with pytest.raises(ValueError, match='payload differs'):
        readback.audit(path, root, result['request_id'], NOW)


def test_empty_product_response_requires_matching_native_receipt(tmp_path, monkeypatch):
    root, _, _ = fixture(tmp_path, monkeypatch)
    path = tmp_path / 'source.json'
    result = checks.register_checks(root, [(DAY, PRODUCT)], NOW, path)
    receipt = root / 'empty.json'
    body = {'dataset': checks.DATASET, 'data_id': PRODUCT, 'partition': DAY,
            'status': 'observed_empty', 'rows': 0}
    receipt.write_text(json.dumps(body))
    with sqlite3_connect(root) as con:
        con.execute("UPDATE tasks SET state='observed_empty',rows=0,receipt_path=? WHERE data_id=?",
                    (receipt.name, PRODUCT))
    row = readback.audit(path, root, result['request_id'], NOW)['results'][0]
    assert row['status'] == 'provider_observed_empty_receipt_verified'
    body['data_id'] = 'WRONG'
    receipt.write_text(json.dumps(body))
    with pytest.raises(ValueError, match='empty receipt identity'):
        readback.audit(path, root, result['request_id'], NOW)
