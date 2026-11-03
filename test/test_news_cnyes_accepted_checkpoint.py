"""Accepted-batch recovery keeps the source fence and exact checkpoint evidence."""
from contextlib import closing
import json
import sqlite3
import threading

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_queue import NewsQueue
from test_news_cnyes_history_evidence import (  # noqa: F401
    cfg, head, queue, seed_accepted_batch_for_history_page, seed_retained_work,
    seed_unindexed_nonzero_pages, source_payload,
)


def _started(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    payload = source_payload([61], epoch=1388534400, page=3, total=61)
    for tag in ('a', 'b'):
        seed_accepted_batch_for_history_page(queue, urls[2], payload, batch_tag=tag)
    source = queue.registry.sources['cnyes']
    first = history.seed_accepted_body_batch_candidates(queue, limit=1, max_seconds=10)
    assert first['checked'] == first['indexed'] == 1 and not first['complete']
    return urls, source, history.accepted_batch_scan_key(source)


@pytest.mark.parametrize('version', [2, '2', True, None])
def test_unknown_version_keeps_cursor_candidates_and_source_jobs(queue, version):
    _, source, marker = _started(queue)
    newer = json.dumps({'version': version, 'path': 'newer private cursor'})
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (newer, marker))
    candidates = [tuple(row) for row in queue.db.execute(
        'SELECT * FROM cnyes_history_accepted_batch_candidates ORDER BY parquet_path')]
    jobs = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert result['blocked'] == 'unsupported_accepted_batch_checkpoint_version'
    assert not result['complete'] and not history.accepted_batch_inventory_complete(queue, source)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == newer
    assert [tuple(row) for row in queue.db.execute(
        'SELECT * FROM cnyes_history_accepted_batch_candidates ORDER BY parquet_path')] == candidates
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs
    status = history.progress(queue.db, source)['accepted_body_batch_scan']
    assert status['scan_state'] == 'unknown' and status['checkpoint_problem'] == 'unsupported_version'
    assert status['scanned_batches'] is None and 'newer private cursor' not in json.dumps(status)


@pytest.mark.parametrize('damaged', [b'\xff\xfe', 'broken', '{"version":1,"attempts":-1}'])
def test_damaged_checkpoint_is_archived_before_rescan_and_restart(queue, damaged):
    urls, source, marker = _started(queue)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (damaged, marker))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    assert history.progress(queue.db, source)['accepted_body_batch_scan']['checkpoint_problem'] == 'invalid_checkpoint'
    result = history.seed_accepted_body_batch_candidates(queue, limit=1, max_seconds=10)
    assert result['restarted_invalid_checkpoint'] and not result['complete']
    assert [row[0] for row in queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?',
            (history.ACCEPTED_BATCH_SCAN_PREFIX + 'invalid:%',))] == [damaged]
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(12):
            result = history.seed_accepted_body_batch_candidates(resumed, limit=1, max_seconds=10)
            if result['complete']:
                break
        assert result['complete'] and history.accepted_batch_inventory_complete(resumed, source)
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == 2
        assert [tuple(row) for row in resumed.db.execute('SELECT * FROM discovery ORDER BY url')] == before
        for _ in range(12):
            history.backfill(resumed, limit=8, max_seconds=10)
            current = resumed.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=?',
                                         (history.cnyes_scope(urls[0]),)).fetchone()
            if current and current[0] == 'list_complete':
                break
        assert head(resumed, urls[0])['state'] == 'list_complete'
        assert [tuple(row) for row in resumed.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_conflicting_archive_cannot_erase_original_or_release_fence(queue):
    _, source, marker = _started(queue)
    audit_key = history.ACCEPTED_BATCH_SCAN_PREFIX + 'invalid:' + history.digest((marker, {'value': 'broken'}))
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('broken', marker))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (audit_key, 'other evidence'))
    result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert result['blocked'] == 'accepted_batch_checkpoint_audit_conflict'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()[0] == 'other evidence'
    assert not history.accepted_batch_inventory_complete(queue, source)
    assert history.progress(queue.db, source)['accepted_body_batch_scan']['checkpoint_problem'] == 'audit_conflict'


def test_complete_marker_with_remaining_registered_batch_resumes_after_restart(queue):
    urls, source, marker = _started(queue)
    assert history.retry_invalid_accepted_batches(queue, limit=8, max_seconds=10)['migration']['complete']
    with queue.db:
        state = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0])
        state['state'] = 'complete'
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(state), marker))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    assert not history.accepted_batch_inventory_complete(queue, source)
    assert history.progress(queue.db, source)['accepted_body_batch_scan']['scan_state'] == 'scanning'
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        result = history.seed_accepted_body_batch_candidates(resumed, limit=8, max_seconds=10)
        assert result['complete'] and result['checked'] == result['indexed'] == 1
        assert history.accepted_batch_inventory_complete(resumed, source)
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == 2
        assert [tuple(row) for row in resumed.db.execute('SELECT * FROM discovery ORDER BY url')] == before
        assert resumed.db.execute('SELECT error_code FROM discovery WHERE url=?', (urls[2],)).fetchone()[0] == 'http_429'


def test_cursor_past_its_frozen_endpoint_is_preserved_before_rescan(queue):
    _, source, marker = _started(queue)
    with queue.db:
        state = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0])
        state.update(state='complete', path='z-outside', end_path='a-end')
        raw = json.dumps(state)
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (raw, marker))
    assert not history.accepted_batch_inventory_complete(queue, source)
    assert history.progress(queue.db, source)['accepted_body_batch_scan']['checkpoint_problem'] == 'invalid_checkpoint'
    result = history.seed_accepted_body_batch_candidates(queue, limit=1, max_seconds=10)
    assert result['restarted_invalid_checkpoint'] and not result['complete']
    assert [row[0] for row in queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?',
            (history.ACCEPTED_BATCH_SCAN_PREFIX + 'invalid:%',))] == [raw]


def test_archive_and_cursor_deletion_rollback_together(queue):
    _, source, marker = _started(queue)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('broken', marker))
        queue.db.execute("CREATE TEMP TRIGGER checkpoint_delete_fail BEFORE DELETE ON queue_settings "
                         "WHEN OLD.value='broken' BEGIN SELECT RAISE(ABORT,'simulated crash'); END")
    with pytest.raises(sqlite3.IntegrityError, match='simulated crash'):
        history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key LIKE ?',
                                (history.ACCEPTED_BATCH_SCAN_PREFIX + 'invalid:%',)).fetchone()
    assert not history.accepted_batch_inventory_complete(queue, source)


def test_stop_during_decode_does_not_rewrite_checkpoint(queue, monkeypatch):
    _, _, marker = _started(queue)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('broken', marker))
    stop, original = threading.Event(), json.loads
    def stop_then_decode(value):
        stop.set()
        return original(value)
    with monkeypatch.context() as patch:
        patch.setattr(history.json, 'loads', stop_then_decode)
        result = history.seed_accepted_body_batch_candidates(queue, stop=stop, limit=8, max_seconds=10)
    assert not result['restarted_invalid_checkpoint']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'


def test_blocked_index_allows_other_local_work_and_normal_idle(queue):
    source = queue.registry.sources['cnyes']
    marker = history.accepted_batch_scan_key(source)
    url, _ = seed_retained_work(queue, failure='http_429')
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({'version': 2})))
    for _ in range(20):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if result['exhausted']:
            break
    assert result['accepted_body_batch_scan']['blocked'] == 'unsupported_accepted_batch_checkpoint_version'
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'restored'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert result['exhausted'] and history.maintenance_delay(result) == 300


def test_blocked_index_keeps_missing_original_behind_fence_without_spinning(queue):
    source = queue.registry.sources['cnyes']
    marker = history.accepted_batch_scan_key(source)
    url, _ = seed_retained_work(queue, failure='http_429', valid_raw=False)
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({'version': 2})))
    for _ in range(20):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if result['exhausted']:
            break
    assert result['exhausted'] and history.maintenance_delay(result) == 300
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (url,)).fetchone()[0] == 'local_retry'
    assert queue.db.execute('SELECT error_code FROM cnyes_history_retained_local_retries WHERE request_url=?',
                            (url,)).fetchone()[0] == 'accepted_batch_scan_pending'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_new_exact_candidate_wakes_only_its_waiting_local_retry_before_global_scan_finishes(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    payload = source_payload([61], epoch=1388534400, page=3, total=61)
    seed_accepted_batch_for_history_page(queue, urls[2], payload, batch_tag='a')
    seed_accepted_batch_for_history_page(queue, urls[2], payload, batch_tag='z')
    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()
    evidence = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                                (history.cnyes_scope(urls[0]),)).fetchone()
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (urls[2], history.cnyes_scope(urls[0]), 3, job['response_sha256'],
                                       evidence['total'], evidence['ids_sha256'], evidence['observed_at_utc']))
    work = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()
    history._defer_retained_work(queue, work, 'pending', 'accepted_batch_scan_pending', candidate_errors=['old_mismatch'])
    before_job = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    before_work = dict(queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone())
    before_retry = dict(queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?', (urls[2],)).fetchone())
    indexed = history.seed_accepted_body_batch_candidates(queue, limit=1, max_seconds=10)
    assert indexed['indexed'] == 1 and not indexed['complete']
    retry = queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?', (urls[2],)).fetchone()
    assert retry['error_code'] == 'new_accepted_body_batch_candidate' and retry['not_before_epoch'] == 0
    archived = [json.loads(r[0]) for r in queue.db.execute('SELECT original_work_json FROM '
        'cnyes_history_retained_revision_audit WHERE request_url=?', (urls[2],))]
    assert any(entry.get('work') == before_work and entry.get('local_retry') == before_retry for entry in archived)
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before_job

    # An unsupported global checkpoint cannot make this already-found exact
    # local candidate wait or release unrelated publisher fences.
    marker = history.accepted_batch_scan_key(source)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps({'version': 2}), marker))
    for _ in range(12):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'restored':
            break
    assert result['accepted_body_batch_scan']['blocked'] == 'unsupported_accepted_batch_checkpoint_version'
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'restored'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before_job


@pytest.mark.parametrize('different_hash,different_retry', [(True, False), (False, True)])
def test_candidate_does_not_wake_a_different_revision_or_unrelated_retry(queue, different_hash, different_retry):
    url, _ = seed_retained_work(queue, valid_raw=False)
    work = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    history._defer_retained_work(queue, work, 'pending',
        'local_io_unavailable' if different_retry else 'accepted_batch_scan_pending')
    retry_before = tuple(queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?',
                                          (url,)).fetchone())
    with queue.db:
        history._register_accepted_batch_candidate(queue, url, 'a' * 64 if different_hash else work['response_sha256'],
                                                   'bodies/example.parquet', 'receipts/example.json')
    assert tuple(queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?',
                                  (url,)).fetchone()) == retry_before
    assert queue.db.execute('SELECT 1 FROM cnyes_history_retained_revision_audit WHERE request_url=?', (url,)).fetchone() is None


def test_candidate_wake_and_its_audit_roll_back_with_registration(queue):
    url, _ = seed_retained_work(queue, valid_raw=False)
    work = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    history._defer_retained_work(queue, work, 'pending', 'accepted_batch_scan_pending')
    retry_before = tuple(queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?',
                                          (url,)).fetchone())
    with pytest.raises(RuntimeError), queue.db:
        history._register_accepted_batch_candidate(queue, url, work['response_sha256'],
                                                   'bodies/example.parquet', 'receipts/example.json')
        raise RuntimeError('injected rollback')
    assert tuple(queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?',
                                  (url,)).fetchone()) == retry_before
    assert queue.db.execute('SELECT 1 FROM cnyes_history_retained_revision_audit WHERE request_url=?', (url,)).fetchone() is None
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE request_url=?', (url,)).fetchone() is None


@pytest.mark.parametrize('invalid', ['receipt', 'authorization'])
def test_unverified_late_candidate_returns_to_scan_fence_without_source_retry(queue, invalid):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    data = source_payload([61], epoch=1388534400, page=3, total=61)
    _, receipt = seed_accepted_batch_for_history_page(queue, urls[2], data, batch_tag='a',
        authorization='b' * 64 if invalid == 'authorization' else None)
    seed_accepted_batch_for_history_page(queue, urls[2], data, batch_tag='z')
    if invalid == 'receipt':
        path = queue.root / receipt
        value = json.loads(path.read_bytes())
        value['sha256'] = '0' * 64
        path.write_text(json.dumps(value))
    job = queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()
    before_job = tuple(job)
    evidence = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                                (history.cnyes_scope(urls[0]),)).fetchone()
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (urls[2], history.cnyes_scope(urls[0]), 3, job['response_sha256'],
                                       evidence['total'], evidence['ids_sha256'], evidence['observed_at_utc']))
    work = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()
    history._defer_retained_work(queue, work, 'pending', 'accepted_batch_scan_pending')
    assert history.seed_accepted_body_batch_candidates(queue, limit=1, max_seconds=10)['indexed'] == 1
    marker = history.accepted_batch_scan_key(source)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps({'version': 2}), marker))
    for _ in range(12):
        history.backfill(queue, limit=8, max_seconds=10)
        retry = queue.db.execute('SELECT error_code FROM cnyes_history_retained_local_retries WHERE request_url=?',
                                 (urls[2],)).fetchone()
        if retry and retry[0] == 'accepted_batch_scan_pending':
            break
    assert retry and retry[0] == 'accepted_batch_scan_pending'
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'local_retry'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (history.cnyes_scope(urls[0]),)).fetchone() is None
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before_job
