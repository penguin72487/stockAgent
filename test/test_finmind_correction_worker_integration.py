"""Correction hooks must preserve last-good evidence and remain retryable.

Only temporary queues/files and mocked provider responses are used here.
"""

from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from types import SimpleNamespace

import pytest
import requests

from downloader import download_finmind_complement as complement
from downloader import download_finmind_free as free
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_corrections as corrections
from downloader.artifact_io import atomic_write_json


NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
OLD = NOW - timedelta(days=2)
DAY = date(2020, 3, 9)
DATASET = 'TaiwanStockPrice'


@pytest.fixture(autouse=True)
def no_provider_calls(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('correction integration tests must never call a provider')
    monkeypatch.setattr(requests.Session, 'request', forbidden)
    monkeypatch.setattr(requests.Session, 'send', forbidden)


def _plan(root, owner, dataset, *, allow_empty=False, derived=False, snapshot=False):
    request = {
        'correction_id': 'reviewed-correction-1', 'owner': owner, 'dataset': dataset,
        'start_date': DAY.isoformat(), 'end_date': DAY.isoformat(), 'data_ids': None,
        'required_after_utc': (NOW - timedelta(days=1)).isoformat(), 'allow_empty': allow_empty,
        'reason': 'reviewed exact scope', 'source_url': 'https://finmind.github.io/WhatIsNew/#2026-09-26',
    }
    if derived:
        request['derived_datasets'] = [complement.WIDE_INSTITUTIONAL]
    if snapshot:
        request['scope_kind'] = 'snapshot'
    path = (root if owner == 'free' else root.parent) / 'announcements' / 'repair_plan.json'
    atomic_write_json(path, {'schema_version': 1, 'plan_id': 'test-reviewed-plan', 'requests': [request]})
    return request


def _seed(root, owner, *, dataset=DATASET, data_id='', kind='day', partition=None, rows=None):
    partition = partition or DAY.isoformat()
    root.mkdir(parents=True, exist_ok=True)
    task = complement.Task(dataset, data_id, partition, kind, 2, 'pending')
    with complement._db(root / 'queue.sqlite3') as conn:
        complement._add_tasks(conn, [(dataset, data_id, partition, kind, 2)])
        rows = rows if rows is not None else [{'date': DAY.isoformat(), 'stock_id': '2330', 'close': 100}]
        if owner == 'sponsor':
            receipt = sponsor._finish(conn, root, task, rows, OLD)
        else:
            receipt = complement._store(root, task, rows, OLD)
            complement._save_result(conn, task, receipt, OLD)
    return task, receipt


def _activate(conn, root, owner):
    result = corrections.apply_worker_corrections(conn, root, owner, NOW)
    conn.commit()
    return result


def _assert_old_proof(root, old_receipt, old_bytes):
    assert (root / old_receipt['parquet_path']).is_file()
    assert hashlib.sha256((root / old_receipt['parquet_path']).read_bytes()).hexdigest() == old_receipt['sha256']
    assert old_bytes in [path.read_bytes() for path in (root / 'receipt_history').rglob('*.json')]


@pytest.mark.parametrize('owner', ['sponsor', 'complement'])
def test_nonempty_refresh_archives_old_proof_and_repairs_only_after_real_new_receipt(tmp_path, owner):
    root = tmp_path / owner
    task, old = _seed(root, owner, data_id='2330' if owner == 'complement' else '',
                      kind='id_history' if owner == 'complement' else 'day',
                      partition='history' if owner == 'complement' else None)
    old_bytes = (root / old['receipt_path']).read_bytes()
    _plan(root, owner, task.dataset)
    with complement._db(root / 'queue.sqlite3') as conn:
        assert _activate(conn, root, owner)['applied'] == 1
        assert corrections.reconcile_worker_corrections(conn, root, owner, NOW)['repaired'] == 0
        rows = [{'date': DAY.isoformat(), 'stock_id': '2330', 'close': 200}]
        if owner == 'sponsor':
            receipt = sponsor._finish(conn, root, task, rows, NOW)
        else:
            receipt = complement._store(root, task, rows, NOW, correction=corrections.correction_context(conn, task))
            complement._save_result(conn, task, receipt, NOW)
        assert receipt['fetched_at_utc'] == NOW.isoformat()
        assert receipt['finmind_corrections']['correction_ids'] == ['reviewed-correction-1']
        assert corrections.reconcile_worker_corrections(conn, root, owner, NOW)['repaired'] == 1
        assert conn.execute('SELECT state FROM finmind_correction_tasks').fetchone() == ('repaired',)
    _assert_old_proof(root, old, old_bytes)


@pytest.mark.parametrize('allow_empty', [False, True])
def test_sponsor_empty_override_requires_exact_reviewed_day_and_preserves_old_proof(tmp_path, allow_empty):
    root = tmp_path / 'sponsor'
    task, old = _seed(root, 'sponsor')
    old_bytes = (root / old['receipt_path']).read_bytes()
    _plan(root, 'sponsor', task.dataset, allow_empty=allow_empty)
    with complement._db(root / 'queue.sqlite3') as conn:
        _activate(conn, root, 'sponsor')
        if allow_empty:
            receipt = sponsor._finish(conn, root, task, [], NOW)
            assert receipt['status'] == 'complete' and receipt['rows'] == 0
            assert receipt['finmind_corrections']['authoritative_empty'] is True
            assert not receipt.get('parquet_path')
            assert corrections.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 1
            _assert_old_proof(root, old, old_bytes)
        else:
            with pytest.raises(complement.SourceError, match='unexpected_empty_after_nonempty') as caught:
                sponsor._finish(conn, root, task, [], NOW)
            sponsor._fail(conn, task, caught.value, NOW)
            assert (root / old['receipt_path']).read_bytes() == old_bytes
            assert conn.execute('SELECT state,rows,error_code FROM tasks').fetchone() == (
                'failed', 1, 'unexpected_empty_after_nonempty')
            assert corrections.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 0


def test_complement_worker_correction_empty_history_keeps_last_good_head_and_short_retry(tmp_path, monkeypatch):
    root = tmp_path / 'complement'
    task, old = _seed(root, 'complement', data_id='2330', kind='id_history', partition='history')
    old_bytes = (root / old['receipt_path']).read_bytes()
    _plan(root, 'complement', task.dataset)
    monkeypatch.setenv('FINMIND_TOKEN', 'unit-test-not-a-live-token')
    monkeypatch.setattr(complement, 'load_env_file', lambda *args, **kwargs: None)
    monkeypatch.setattr(complement, '_now', lambda: NOW)
    monkeypatch.setattr(complement, 'verified_account', lambda *args: {'tier': 'Free', 'official_requests_per_hour': 600})
    monkeypatch.setattr(complement, 'rate_limiter', lambda *args: object())
    monkeypatch.setattr(complement, '_populate', lambda *args, **kwargs: None)
    monkeypatch.setattr(complement, '_status', lambda *args, **kwargs: kwargs)
    monkeypatch.setattr(complement.shutil, 'disk_usage', lambda *args: SimpleNamespace(free=complement.MIN_FREE_BYTES + 1))
    monkeypatch.setattr(complement, 'backfill_budget', lambda *args, **kwargs: {'allowed': True})
    monkeypatch.setattr(complement, '_request', lambda *args, **kwargs: [])
    complement.run_once(root, max_requests=1, datasets=(task.dataset,))
    assert (root / old['receipt_path']).read_bytes() == old_bytes
    with complement._db(root / 'queue.sqlite3') as conn:
        row = conn.execute('SELECT state,rows,error_code,next_attempt_at_utc FROM tasks').fetchone()
        assert row[:3] == ('failed', 1, 'unexpected_empty_after_nonempty')
        assert NOW < datetime.fromisoformat(row[3]) <= NOW + timedelta(hours=1)
        assert conn.execute('SELECT state FROM finmind_correction_tasks').fetchone() == ('queued',)


def test_named_id_deletion_does_not_authorize_empty_whole_market_partition(tmp_path):
    root = tmp_path / 'sponsor'
    task, old = _seed(root, 'sponsor')
    old_bytes = (root / old['receipt_path']).read_bytes()
    request = _plan(root, 'sponsor', task.dataset, allow_empty=True)
    request['data_ids'] = ['2330']
    atomic_write_json(root.parent / 'announcements/repair_plan.json', {
        'schema_version': 1, 'plan_id': 'test-reviewed-plan', 'requests': [request]})
    with complement._db(root / 'queue.sqlite3') as conn:
        _activate(conn, root, 'sponsor')
        assert corrections.correction_context(conn, task)['allow_empty'] is False
        with pytest.raises(complement.SourceError, match='unexpected_empty_after_nonempty'):
            sponsor._finish(conn, root, task, [], NOW)
        assert (root / old['receipt_path']).read_bytes() == old_bytes


def _index_rows(value=100):
    return [{'date': f'{DAY} {seconds // 3600:02}:{seconds % 3600 // 60:02}:{seconds % 60:02}', 'TAIEX': value}
            for seconds in range(9 * 3600, 13 * 3600 + 30 * 60 + 1, 5)]


def test_free_non_order_book_empty_response_keeps_proof_chain_and_can_recover(tmp_path):
    dataset = 'TaiwanVariousIndicators5Seconds'
    old = free._record_session(tmp_path, dataset, DAY, _index_rows(), now=OLD)
    assert old['status'] == 'complete'
    old_data = (tmp_path / old['parquet_path']).read_bytes()
    _plan(tmp_path, 'free', dataset)
    with pytest.raises(free.ProviderError, match='unexpected_empty_after_nonempty') as caught:
        free._record_session(tmp_path, dataset, DAY, [], now=NOW)
    empty = free._record_failure(tmp_path, dataset, DAY, caught.value, now=NOW)
    assert empty['status'] == 'failed'
    assert empty['previous_source_receipt']['sha256'] == old['sha256']
    assert (tmp_path / old['parquet_path']).read_bytes() == old_data
    later = NOW + timedelta(hours=7)
    refreshed = free._record_session(tmp_path, dataset, DAY, _index_rows(200), now=later)
    assert refreshed['status'] == 'complete'
    assert corrections.free_correction_due(tmp_path, dataset, DAY, refreshed, later)['due'] is False
    archived = list((tmp_path / 'receipt_history' / dataset / str(DAY)).glob('*.parquet'))
    assert old_data in [path.read_bytes() for path in archived]


def test_free_partial_and_transient_failure_are_never_repair_proofs_and_preserve_retries(tmp_path):
    dataset = 'TaiwanVariousIndicators5Seconds'
    _plan(tmp_path, 'free', dataset)
    partial = free._record_session(tmp_path, dataset, DAY, _index_rows()[:-1], now=NOW)
    assert partial['status'] == 'partial'
    assert not corrections.free_correction_due(tmp_path, dataset, DAY, partial, NOW)['due']
    assert corrections.free_correction_due(tmp_path, dataset, DAY, partial, NOW + timedelta(hours=7))['due']
    failure = free._record_failure(tmp_path, dataset, DAY, free.ProviderError('http_503', retry_after=900), now=NOW)
    assert failure['status'] == 'failed'
    assert not corrections.free_correction_due(tmp_path, dataset, DAY, failure, NOW)['due']
    assert corrections.free_correction_due(tmp_path, dataset, DAY, failure, NOW + timedelta(minutes=16))['due']
    complete = free._record_session(tmp_path, dataset, DAY, _index_rows(200), now=NOW + timedelta(minutes=16))
    assert complete['status'] == 'complete'
    assert not corrections.free_correction_due(tmp_path, dataset, DAY, complete, NOW + timedelta(minutes=16))['due']


def test_free_master_correction_forces_fresh_snapshot_and_archives_old_head(tmp_path):
    # A snapshot is keyed by local date, so install a pre-notice proof for the
    # current date rather than pretending a previous-day snapshot is current.
    today = NOW.astimezone(free.TAIPEI).date()
    earlier_today = NOW - timedelta(hours=2)
    old = free._record_master(tmp_path, [{'stock_id': '2330', 'name': 'before'}], now=earlier_today)
    old_path = tmp_path / old['parquet_path']
    old_data = old_path.read_bytes()
    request = _plan(tmp_path, 'free', free.MASTER_DATASET, snapshot=True)
    request['required_after_utc'] = (NOW - timedelta(hours=1)).isoformat()
    atomic_write_json(tmp_path / 'announcements/repair_plan.json', {
        'schema_version': 1, 'plan_id': 'test-reviewed-plan', 'requests': [request]})
    assert free._master_due(tmp_path, NOW)
    fresh = free._record_master(tmp_path, [{'stock_id': '2330', 'name': 'after'}], now=NOW)
    assert not corrections.free_correction_due(tmp_path, free.MASTER_DATASET, today, fresh, NOW)['due']
    assert old_data in [p.read_bytes() for p in (tmp_path / 'receipt_history').rglob('*.parquet')]


@pytest.mark.parametrize('dataset', ['TaiwanVariousIndicators5Seconds', free.MASTER_DATASET])
def test_free_new_notice_cannot_claim_inflight_result_without_prefetch_context(tmp_path, dataset):
    snapshot = dataset == free.MASTER_DATASET
    day = NOW.astimezone(free.TAIPEI).date() if snapshot else DAY
    request = _plan(tmp_path, 'free', dataset, snapshot=snapshot)
    request['watermark_basis'] = 'first_detection_of_new_or_edited_notice'
    atomic_write_json(tmp_path / 'announcements/repair_plan.json', {
        'schema_version': 1, 'plan_id': 'test-reviewed-plan', 'requests': [request]})
    # The worker captured no notice before sending this request.  Arrival of a
    # new plan during HTTP may not attach a new correction marker after return.
    if snapshot:
        receipt = free._record_master(tmp_path, [{'stock_id': '2330'}], now=NOW, correction_context={})
    else:
        receipt = free._record_session(tmp_path, dataset, day, _index_rows(), now=NOW, correction_context={})
    assert 'finmind_corrections' not in receipt
    due = corrections.free_correction_due(tmp_path, dataset, day, receipt, NOW)
    assert due['due'] is True
    if snapshot:
        fresh = free._record_master(tmp_path, [{'stock_id': '2330'}], now=NOW + timedelta(seconds=1),
                                    correction_context=due['context'])
    else:
        fresh = free._record_session(tmp_path, dataset, day, _index_rows(), now=NOW + timedelta(seconds=1),
                                     correction_context=due['context'])
    assert fresh['finmind_corrections']['correction_ids'] == ['reviewed-correction-1']
    assert corrections.free_correction_due(tmp_path, dataset, day, fresh, NOW + timedelta(seconds=1))['due'] is False


def test_free_calendar_correction_bypasses_fresh_cache_and_preserves_old_calendar(tmp_path, monkeypatch):
    before = {'schema_version': 1, 'source_dataset': free.CALENDAR_DATASET,
              'observed_at_utc': (NOW - timedelta(hours=2)).isoformat(), 'dates': [DAY.isoformat()]}
    atomic_write_json(tmp_path / 'calendar.json', before)
    old_bytes = (tmp_path / 'calendar.json').read_bytes()
    request = _plan(tmp_path, 'free', free.CALENDAR_DATASET, snapshot=True)
    request['required_after_utc'] = (NOW - timedelta(hours=1)).isoformat()
    atomic_write_json(tmp_path / 'announcements/repair_plan.json', {
        'schema_version': 1, 'plan_id': 'test-reviewed-plan', 'requests': [request]})
    calls = []
    def fetch(*args, **kwargs):
        calls.append(args)
        return [{'date': DAY.isoformat()}, {'date': '2020-03-10'}]
    monkeypatch.setattr(free, '_request', fetch)
    dates, request_count = free._load_calendar(tmp_path, object(), object(), '', NOW)
    assert len(calls) == request_count == 1 and len(dates) == 2
    assert old_bytes in [p.read_bytes() for p in (tmp_path / 'versions' / free.CALENDAR_DATASET).glob('*.json')]
    current = json.loads((tmp_path / 'calendar.json').read_bytes())
    assert current['finmind_corrections']['correction_ids'] == ['reviewed-correction-1']
    assert not corrections.free_correction_due(tmp_path, free.CALENDAR_DATASET, NOW.date(), current, NOW)['due']


def test_sponsor_derived_correction_waits_for_parent_repair_before_rebuilding(tmp_path):
    root = tmp_path / 'sponsor'
    long_rows = [{'date': DAY.isoformat(), 'stock_id': '2330', 'name': 'Foreign_Investor', 'buy': 100, 'sell': 20}]
    parent, _ = _seed(root, 'sponsor', dataset=complement.LONG_INSTITUTIONAL, rows=long_rows)
    child_rows = sponsor._derive_wide(root, str(DAY))
    child, _ = _seed(root, 'sponsor', dataset=complement.WIDE_INSTITUTIONAL, kind='derived', rows=child_rows)
    _plan(root, 'sponsor', complement.LONG_INSTITUTIONAL, derived=True)
    with complement._db(root / 'queue.sqlite3') as conn:
        assert _activate(conn, root, 'sponsor')['applied'] == 2
        assert sponsor._next(conn, NOW, datasets=(child.dataset,)) is None
        sponsor._finish(conn, root, parent, [{**long_rows[0], 'buy': 200}], NOW)
        selected = sponsor._next(conn, NOW, datasets=(child.dataset,))
        assert selected is not None and selected.dataset == child.dataset
        refreshed_rows = sponsor._derive_wide(root, str(DAY))
        assert refreshed_rows[0]['Foreign_Investor_buy'] == 200
        sponsor._finish(conn, root, selected, refreshed_rows, NOW)
        assert corrections.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 2


def test_complement_derived_correction_waits_through_parent_error_then_rebuilds(tmp_path):
    root = tmp_path / 'complement'
    long_rows = [{'date': DAY.isoformat(), 'stock_id': '2330', 'name': 'Foreign_Investor', 'buy': 100, 'sell': 20}]
    parent, _ = _seed(root, 'complement', dataset=complement.LONG_INSTITUTIONAL, data_id='2330',
                      kind='id_history', partition='history', rows=long_rows)
    with complement._db(root / 'queue.sqlite3') as conn:
        child = complement.Task(complement.WIDE_INSTITUTIONAL, '2330', 'history', 'derived', 2, 'pending')
        child_rows = complement._derive_wide(root, conn, child)
    child, _ = _seed(root, 'complement', dataset=child.dataset, data_id=child.data_id,
                     kind=child.kind, partition=child.partition, rows=child_rows)
    _plan(root, 'complement', complement.LONG_INSTITUTIONAL, derived=True)
    with complement._db(root / 'queue.sqlite3') as conn:
        assert _activate(conn, root, 'complement')['applied'] == 2
        assert complement._next_task(conn, NOW, datasets=(child.dataset,)) is None
        complement._save_failure(conn, parent, complement.SourceError('http_503', retry_after=900), NOW)
        assert complement._next_task(conn, NOW, datasets=(child.dataset,)) is None
        later = NOW + timedelta(minutes=16)
        receipt = complement._store(root, parent, [{**long_rows[0], 'buy': 200}], later,
                                     correction=corrections.correction_context(conn, parent))
        complement._save_result(conn, parent, receipt, later)
        selected = complement._next_task(conn, later, datasets=(child.dataset,))
        assert selected is not None and selected.dataset == child.dataset
        refreshed_rows = complement._derive_wide(root, conn, selected)
        assert refreshed_rows[0]['Foreign_Investor_buy'] == 200
        receipt = complement._store(root, selected, refreshed_rows, later,
                                     correction=corrections.correction_context(conn, selected))
        complement._save_result(conn, selected, receipt, later)
        assert corrections.reconcile_worker_corrections(conn, root, 'complement', later)['repaired'] == 2


def test_sponsor_derived_exact_empty_correction_propagates_parent_without_stale_parquet(tmp_path):
    root = tmp_path / 'sponsor'
    long_rows = [{'date': DAY.isoformat(), 'stock_id': '2330', 'name': 'Foreign_Investor', 'buy': 100, 'sell': 20}]
    parent, _ = _seed(root, 'sponsor', dataset=complement.LONG_INSTITUTIONAL, rows=long_rows)
    child, _ = _seed(root, 'sponsor', dataset=complement.WIDE_INSTITUTIONAL, kind='derived',
                     rows=sponsor._derive_wide(root, str(DAY)))
    _plan(root, 'sponsor', complement.LONG_INSTITUTIONAL, derived=True, allow_empty=True)
    with complement._db(root / 'queue.sqlite3') as conn:
        _activate(conn, root, 'sponsor')
        receipt = sponsor._finish(conn, root, parent, [], NOW)
        assert receipt['status'] == 'complete' and receipt['rows'] == 0
        selected = sponsor._next(conn, NOW, datasets=(child.dataset,))
        assert selected is not None
        assert sponsor._derive_wide(root, str(DAY)) == []
        sponsor._finish(conn, root, selected, [], NOW)
        assert corrections.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 2
