from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import sqlite3
from copy import deepcopy

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import finmind_corrections as c


NOW = datetime(2026, 9, 27, 12, tzinfo=UTC)
DATASET = 'TaiwanStockPrice'


def _conn():
    conn = sqlite3.connect(':memory:')
    conn.execute('''CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,kind TEXT,priority INTEGER,
        state TEXT,next_attempt_at_utc TEXT,last_attempt_at_utc TEXT,rows INTEGER DEFAULT 0,bytes INTEGER DEFAULT 0,
        first_data_date TEXT,last_data_date TEXT,receipt_path TEXT,error_code TEXT,
        PRIMARY KEY(dataset,data_id,partition))''')
    return conn


def _task(conn, partition='2024-10-02', *, dataset=DATASET, data_id='', kind='day', state='complete', priority=8,
          retry=None, error=None):
    conn.execute('INSERT INTO tasks(dataset,data_id,partition,kind,priority,state,next_attempt_at_utc,error_code) '
                 'VALUES (?,?,?,?,?,?,?,?)', (dataset, data_id, partition, kind, priority, state, retry, error))
    conn.commit()
    return {'dataset': dataset, 'data_id': data_id, 'partition': partition, 'kind': kind}


def _request(**kw):
    return {'correction_id': 'notice-hash-1', 'owner': 'sponsor', 'dataset': DATASET,
            'start_date': '2024-10-02', 'end_date': '2024-10-02', 'data_ids': None,
            'required_after_utc': (NOW - timedelta(days=1)).isoformat(), 'allow_empty': False,
            'reason': 'reviewed official correction', 'source_url': 'https://finmind.github.io/tutor/WhatIsNew/', **kw}


def _plan(root, requests, *, owner='sponsor', plan_id='plan-1'):
    base = root if owner == 'free' else root.parent
    path = base / 'announcements/repair_plan.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'schema_version': 1, 'plan_id': plan_id, 'requests': requests}))
    return path


def _receipt(root, task, *, fetched=NOW, rows=1, status='complete', **kw):
    receipt = {**task, 'status': status, 'rows': rows, 'fetched_at_utc': fetched.isoformat(), **kw}
    if rows:
        relative = f"parquet/{task['dataset']}/{task['data_id'] or 'all'}/{task['partition']}.parquet"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist([{'date': task['partition'], 'value': 1}] * rows), path)
        receipt.update(parquet_path=relative, parquet_size_bytes=path.stat().st_size,
                       sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    relative = f"receipts/{task['dataset']}/{task['data_id'] or 'all'}/{task['partition']}.json"
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(receipt))
    return receipt, relative


def _install(conn, root, task, **kw):
    receipt, path = _receipt(root, task, **kw)
    conn.execute('UPDATE tasks SET state=?,rows=?,receipt_path=? WHERE dataset=? AND data_id=? AND partition=?',
                 (receipt['status'], receipt['rows'], path, task['dataset'], task['data_id'], task['partition']))
    conn.commit()
    return receipt, path


def test_only_existing_exact_owner_overlap_promoted_and_audited_idempotently(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn)
    _task(conn, '2024-10-03')
    old = conn.execute("SELECT * FROM tasks WHERE partition='2024-10-02'").fetchone()
    other = conn.execute("SELECT * FROM tasks WHERE partition='2024-10-03'").fetchone()
    _plan(root, [_request(), _request(correction_id='other-owner', owner='complement'),
                 _request(dataset='UnscheduledDataset')])
    summary = c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    assert summary['applied'] == 1 and summary['owner_skipped'] == 1 and summary['unmatched_requests'] == 1
    assert conn.execute("SELECT priority,state FROM tasks WHERE partition='2024-10-02'").fetchone() == (1, 'pending')
    assert conn.execute("SELECT * FROM tasks WHERE partition='2024-10-03'").fetchone() == other
    audit = json.loads(conn.execute('SELECT prior_task_json FROM finmind_correction_tasks').fetchone()[0])
    columns = [row[1] for row in conn.execute('PRAGMA table_info(tasks)')]
    assert audit == dict(zip(columns, old))
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['unchanged'] == 1
    assert conn.execute('SELECT COUNT(*) FROM tasks').fetchone() == (2,)
    assert c.correction_context(conn, task)['allow_empty'] is False


def test_changed_same_identity_rejected_atomically_and_new_identity_can_repair_again(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    _task(conn)
    _plan(root, [_request()])
    c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    _plan(root, [_request(reason='edited scope')])
    with pytest.raises(ValueError, match='new content identity'):
        c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    _plan(root, [_request(correction_id='new-hash', reason='edited scope')])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['applied'] == 1


@pytest.mark.parametrize('state,error,retry', [
    ('inflight', None, None), ('blocked', 'invalid_token', None), ('not_entitled', None, None),
    ('invalid_request', None, None), ('failed', 'rate_limited', (NOW + timedelta(hours=1)).isoformat()),
    ('failed', 'ip_banned', (NOW + timedelta(minutes=30)).isoformat()),
    ('pending', None, (NOW + timedelta(hours=1)).isoformat()),
])
def test_existing_cooldowns_and_blockers_untouched(tmp_path, state, error, retry):
    root = tmp_path / 'sponsor'
    conn = _conn()
    _task(conn, state=state, error=error, retry=retry)
    old = conn.execute('SELECT * FROM tasks').fetchone()
    _plan(root, [_request()])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['deferred'] == 1
    assert conn.execute('SELECT * FROM tasks').fetchone() == old


def test_late_seed_and_finished_inflight_are_discovered_on_next_apply(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    _plan(root, [_request(end_date='2024-10-03')])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['unmatched_requests'] == 1
    _task(conn, state='inflight')
    c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    conn.execute("UPDATE tasks SET state='complete'")
    conn.commit()
    _task(conn, '2024-10-03')
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['applied'] == 2


def test_year_and_per_id_history_match_only_requested_range_and_ids(tmp_path):
    root = tmp_path / 'complement'
    conn = _conn()
    _task(conn, '2023', kind='year')
    _task(conn, '2024', kind='year')
    _task(conn, 'history', kind='id_history', data_id='2330')
    _task(conn, 'history', kind='id_history', data_id='2317')
    _plan(root, [_request(owner='complement', data_ids=['2330'])])
    assert c.apply_worker_corrections(conn, root, 'complement', NOW)['applied'] == 2
    assert conn.execute("SELECT partition,data_id FROM tasks WHERE state='pending' ORDER BY partition").fetchall() == [
        ('2024', ''), ('history', '2330')]


def test_outer_transaction_rollback_restores_tasks_and_audit(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    _task(conn)
    _plan(root, [_request()])
    conn.execute('BEGIN')
    c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    assert conn.in_transaction
    conn.rollback()
    assert conn.execute('SELECT state FROM tasks').fetchone() == ('complete',)
    assert not c._has_schema(conn)


@pytest.mark.parametrize('damage', ['sha', 'size', 'rows', 'outside_path', 'not_complete'])
def test_fresh_receipt_requires_hash_size_footer_and_complete_status(tmp_path, damage):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn)
    receipt, relative = _install(conn, root, task)
    if damage == 'sha':
        receipt['sha256'] = '0' * 64
    elif damage == 'size':
        receipt['parquet_size_bytes'] += 1
    elif damage == 'rows':
        receipt['rows'] += 1
    elif damage == 'outside_path':
        receipt['parquet_path'] = '../outside.parquet'
    else:
        receipt['status'] = 'observed_empty'
    (root / relative).write_text(json.dumps(receipt))
    _plan(root, [_request()])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['applied'] == 1
    conn.execute("UPDATE tasks SET state='complete'")
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 0


def test_fresh_verified_receipt_not_requeued_and_later_completion_reconciles(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn)
    _install(conn, root, task)
    _plan(root, [_request()])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['already_refreshed'] == 1
    _plan(root, [_request(correction_id='new', required_after_utc=NOW.isoformat())])
    _install(conn, root, task, fetched=NOW - timedelta(seconds=1))
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['applied'] == 1
    _install(conn, root, task, fetched=NOW + timedelta(seconds=1))
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 1
    assert conn.execute("SELECT receipt_evidence_json FROM finmind_correction_tasks WHERE correction_id='new'").fetchone()[0]


def test_empty_authorization_exact_day_token_and_status_are_all_required(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn, state='non_session')
    _plan(root, [_request(allow_empty=True)])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['applied'] == 1
    context = c.correction_context(conn, task)
    assert context['allow_empty'] is True
    _install(conn, root, task, rows=0, status='observed_empty',
             **c.correction_receipt_metadata(context, authoritative_empty=True))
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 0
    _install(conn, root, task, rows=0, **c.correction_receipt_metadata(context, authoritative_empty=True))
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 1
    context['partition'] = '2024-10-03'
    assert c._verified_receipt(root, task, _request(allow_empty=True), 'sponsor',
                               {**task, 'status': 'complete', 'rows': 0, 'fetched_at_utc': NOW.isoformat(),
                                **c.correction_receipt_metadata(context, authoritative_empty=True)}) is None


@pytest.mark.parametrize('state', ['non_session', 'not_observation_date'])
def test_restart_seed_exclusion_can_only_recover_or_finish_exact_empty_notice(tmp_path, state):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn)
    _plan(root, [_request(allow_empty=True)])
    c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    conn.execute('UPDATE tasks SET state=?', (state,))
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['applied'] == 1
    assert conn.execute('SELECT state FROM tasks').fetchone() == ('pending',)
    context = c.correction_context(conn, task)
    _install(conn, root, task, rows=0, **c.correction_receipt_metadata(context, authoritative_empty=True))
    conn.execute('UPDATE tasks SET state=?', (state,))
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 1
    event = json.loads(conn.execute('SELECT prior_task_json FROM finmind_correction_events ORDER BY id DESC').fetchone()[0])
    assert event['state'] == state


def test_unapproved_excluded_day_never_reopened(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    _task(conn, state='non_session')
    _plan(root, [_request()])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['deferred'] == 1
    assert conn.execute('SELECT state FROM tasks').fetchone() == ('non_session',)


@pytest.mark.parametrize('kind,partition,ids,end', [
    ('year', '2024', None, '2024-10-02'), ('id_history', 'history', None, '2024-10-02'),
    ('day', '2024-10-02', ['2330'], '2024-10-02'), ('day', '2024-10-02', None, '2024-10-03')])
def test_empty_notice_never_clears_whole_year_history_or_broader_scope(tmp_path, kind, partition, ids, end):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn, partition, kind=kind)
    _plan(root, [_request(allow_empty=True, data_ids=ids, end_date=end)])
    c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    context = c.correction_context(conn, task)
    assert context['allow_empty'] is False
    with pytest.raises(ValueError, match='exact day'):
        c.correction_receipt_metadata(context, authoritative_empty=True)


def test_reconciler_does_not_starve_finished_tasks_behind_pending_or_invalid_proofs(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    for day in ['2024-10-01', '2024-10-02', '2024-10-03']:
        _task(conn, day)
    _plan(root, [_request(start_date='2024-10-01', end_date='2024-10-03')])
    c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    conn.execute("UPDATE tasks SET state='complete' WHERE partition='2024-10-02'")
    conn.commit()
    _install(conn, root, {'dataset': DATASET, 'data_id': '', 'partition': '2024-10-03', 'kind': 'day'})
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW, max_receipts=1)['repaired'] == 0
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW + timedelta(seconds=1), max_receipts=1)['repaired'] == 1


def test_long_notice_marks_only_existing_wide_derived_tasks_dirty(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    long = 'TaiwanStockInstitutionalInvestorsBuySell'
    wide = long + 'Wide'
    _task(conn, dataset=long)
    task = _task(conn, dataset=wide, kind='derived')
    _install(conn, root, task)
    _plan(root, [_request(dataset=long, derived_datasets=[wide])])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['applied'] == 2
    assert conn.execute('SELECT COUNT(*) FROM tasks').fetchone() == (2,)
    assert conn.execute("SELECT state FROM tasks WHERE dataset=?", (wide,)).fetchone() == ('pending',)


def test_free_day_notice_is_read_only_respects_retry_and_reuses_cached_plan(tmp_path, monkeypatch):
    dataset = 'TaiwanVariousIndicators5Seconds'
    day = date(2024, 10, 2)
    task = {'dataset': dataset, 'data_id': '', 'partition': str(day), 'kind': 'day'}
    receipt, _ = _receipt(tmp_path, task, fetched=NOW - timedelta(days=2), date=str(day))
    path = _plan(tmp_path, [_request(owner='free', dataset=dataset)], owner='free')
    before = path.read_bytes()
    assert c.free_correction_due(tmp_path, dataset, day, receipt, NOW)['due']
    cached = c._parse_plan.cache_info().hits
    assert not c.free_correction_due(tmp_path, dataset, day - timedelta(days=1), receipt, NOW)['due']
    assert c._parse_plan.cache_info().hits > cached
    failed = {**receipt, 'status': 'failed', 'error_code': 'rate_limited', 'retry_at_utc': (NOW + timedelta(hours=1)).isoformat()}
    assert not c.free_correction_due(tmp_path, dataset, day, failed, NOW)['due']
    assert path.read_bytes() == before
    assert not (tmp_path / 'queue.sqlite3').exists()


def test_free_provider_empty_after_retry_keeps_notice_context_until_verified_complete(tmp_path):
    dataset = 'TaiwanVariousIndicators5Seconds'
    day = date(2024, 10, 2)
    _plan(tmp_path, [_request(owner='free', dataset=dataset)], owner='free')
    receipt = {'dataset': dataset, 'date': str(day), 'status': 'provider_empty', 'rows': 0,
               'fetched_at_utc': (NOW - timedelta(hours=1)).isoformat(),
               'retry_at_utc': (NOW + timedelta(hours=1)).isoformat()}
    assert not c.free_correction_due(tmp_path, dataset, day, receipt, NOW)['due']
    due = c.free_correction_due(tmp_path, dataset, day, receipt, NOW + timedelta(hours=2))
    assert due['due'] and due['context']['correction_ids'] == ['notice-hash-1']
    assert due['context']['allow_empty'] is False


def test_free_calendar_snapshot_uses_observed_time_and_exact_local_calendar(tmp_path):
    request = _request(owner='free', dataset=c.CALENDAR, scope_kind='snapshot', start_date=None, end_date=None)
    _plan(tmp_path, [request], owner='free')
    calendar = {'schema_version': 1, 'source_dataset': c.CALENDAR, 'observed_at_utc': NOW.isoformat(),
                'dates': ['2026-09-25', '2026-09-28']}
    (tmp_path / 'calendar.json').write_text(json.dumps(calendar))
    assert not c.free_correction_due(tmp_path, c.CALENDAR, NOW.date(), calendar, NOW)['due']
    assert c.free_correction_due(tmp_path, c.CALENDAR, NOW.date(), {**calendar, 'dates': []}, NOW)['due']


def test_free_master_snapshot_proves_whole_table_and_footer(tmp_path):
    request = _request(owner='free', dataset=c.MASTER, scope_kind='snapshot', start_date=None, end_date=None)
    _plan(tmp_path, [request], owner='free')
    day = NOW.astimezone(c.TAIPEI).date()
    relative = f'snapshots/{c.MASTER}/snapshot={day}-full.parquet'
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{'stock_id': '2330'}]), path)
    receipt = {'dataset': c.MASTER, 'status': 'complete', 'query_scope': 'full_table_snapshot',
               'snapshot_date_taipei': str(day), 'rows': 1, 'fetched_at_utc': NOW.isoformat(),
               'parquet_path': relative, 'parquet_size_bytes': path.stat().st_size,
               'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    assert not c.free_correction_due(tmp_path, c.MASTER, day, receipt, NOW)['due']
    receipt['rows'] = 2
    assert c.free_correction_due(tmp_path, c.MASTER, day, receipt, NOW)['due']


def test_new_notice_needs_post_notice_request_not_only_post_notice_finish(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn)
    receipt, path = _install(conn, root, task)
    request = _request(watermark_basis='first_detection_of_new_or_edited_notice')
    _plan(root, [request])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['applied'] == 1
    # The pre-notice in-flight result ended late but has no causal request proof.
    conn.execute("UPDATE tasks SET state='complete'")
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 0
    receipt['request_started_at_utc'] = (NOW - timedelta(days=2)).isoformat()
    (root / path).write_text(json.dumps(receipt))
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 0
    receipt.update(c.correction_receipt_metadata(c.correction_context(conn, task)))
    (root / path).write_text(json.dumps(receipt))
    assert c.reconcile_worker_corrections(conn, root, 'sponsor', NOW)['repaired'] == 1


def test_explicit_post_notice_start_can_prove_new_request_without_marker(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn)
    _install(conn, root, task, request={'request_started_at_utc': (NOW - timedelta(hours=1)).isoformat()})
    _plan(root, [_request(watermark_basis='first_detection_of_new_or_edited_notice')])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['already_refreshed'] == 1


def test_new_calendar_notice_requires_marker_even_if_old_request_finishes_late(tmp_path):
    request = _request(owner='free', dataset=c.CALENDAR, scope_kind='snapshot', start_date=None, end_date=None,
                       watermark_basis='first_detection_of_new_or_edited_notice')
    _plan(tmp_path, [request], owner='free')
    calendar = {'schema_version': 1, 'source_dataset': c.CALENDAR, 'observed_at_utc': NOW.isoformat(),
                'dates': ['2026-09-25', '2026-09-28']}
    path = tmp_path / 'calendar.json'
    path.write_text(json.dumps(calendar))
    decision = c.free_correction_due(tmp_path, c.CALENDAR, NOW.date(), calendar, NOW)
    assert decision['due']
    calendar.update(c.correction_receipt_metadata(decision['context']))
    path.write_text(json.dumps(calendar))
    assert not c.free_correction_due(tmp_path, c.CALENDAR, NOW.date(), calendar, NOW)['due']
    assert not c.free_correction_due(tmp_path, c.CALENDAR, NOW.date() + timedelta(days=1), calendar, NOW)['due']


def test_partial_first_year_partition_does_not_claim_uncovered_earlier_dates(tmp_path):
    root = tmp_path / 'sponsor'
    conn = _conn()
    _task(conn, '2024-06-22', kind='year')
    _plan(root, [_request(start_date='2024-03-01', end_date='2024-03-01')])
    assert c.apply_worker_corrections(conn, root, 'sponsor', NOW)['unmatched_requests'] == 1


def test_real_reviewed_planner_envelope_is_accepted_without_scope_inference(tmp_path):
    from downloader.finmind_correction_plans import build_repair_plan, load_scope_registry

    registry = load_scope_registry()
    entries = [{'entry_id': key, 'revision_family_id': item['revision_family_id'],
                'notice_date': item['notice_date'], 'text': item['evidence_text'],
                'leading_text': item['evidence_text'], 'datasets': item['datasets'],
                'is_correction': True, 'nested_items': [], 'known_unavailable': False,
                'source_url': 'https://finmind.github.io/WhatIsNew/'}
               for key, item in registry['scopes'].items()]
    plan = build_repair_plan(entries, now=NOW)
    path = tmp_path / 'announcements/repair_plan.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(plan))
    actual, digest = c._load_plan(tmp_path / 'sponsor', 'sponsor')
    assert actual['requests'] == plan['requests'] and len(digest) == 64
    assert len(actual['requests']) > 0


@pytest.mark.parametrize('damage', [None, 'token', 'owner', 'day', 'dataset', 'rows', 'status', 'payload', 'ids', 'kind'])
def test_preserved_authoritative_empty_verifier_binds_day_owner_id_and_token(tmp_path, damage):
    root = tmp_path / 'sponsor'
    conn = _conn()
    task = _task(conn)
    _plan(root, [_request(allow_empty=True)])
    c.apply_worker_corrections(conn, root, 'sponsor', NOW)
    receipt, _ = _receipt(root, task, rows=0,
                          **c.correction_receipt_metadata(c.correction_context(conn, task), authoritative_empty=True))
    altered = deepcopy(receipt)
    marker = altered['finmind_corrections']
    if damage == 'token':
        marker['empty_authorization_tokens']['notice-hash-1'] = '0' * 64
    elif damage == 'owner':
        marker['owner'] = 'complement'
    elif damage == 'day':
        marker['partition'] = '2024-10-03'
    elif damage == 'dataset':
        marker['dataset'] = 'TaiwanStockPriceAdj'
    elif damage == 'rows':
        altered['rows'] = 1
    elif damage == 'status':
        altered['status'] = 'observed_empty'
    elif damage == 'payload':
        altered['parquet_path'] = 'old.parquet'
    elif damage == 'ids':
        marker['correction_ids'] = []
    elif damage == 'kind':
        altered['kind'] = 'year'
    assert c.verified_authoritative_empty(altered, owner='sponsor', dataset=DATASET,
                                         data_id='', partition='2024-10-02') is (damage is None)
    assert not c.verified_authoritative_empty(receipt, owner='sponsor', dataset=DATASET,
                                             data_id='2330', partition='2024-10-02')
