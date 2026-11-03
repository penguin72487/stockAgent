"""Legacy nonzero-page scans must not erase newer or damaged cursors."""

from contextlib import closing
import json
import threading

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_queue import NewsQueue
from test_news_cnyes_history_evidence import (  # noqa: F401
    cfg, queue, seed_retained_work, seed_unindexed_nonzero_pages,
)


def _started(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    assert history.seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['queued'] == 1
    source = queue.registry.sources['cnyes']
    return urls, source, history.nonzero_scan_key(source)


@pytest.mark.parametrize('version', [2, '2', True, None])
def test_unknown_version_preserves_cursor_fences_and_source_jobs(queue, version):
    urls, source, marker = _started(queue)
    unknown = json.dumps({'version': version, 'state': 'scanning', 'scope_url': '', 'page': 30,
                          'checked': 0, 'queued': 0})
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (unknown, marker))
        queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET authorization_fingerprint='old'")
    fence = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes')]
    work = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_history_retained_work')]
    jobs = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = history.seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)
    assert result['blocked'] == 'unsupported_nonzero_scan_checkpoint_version'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == unknown
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes')] == fence
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_history_retained_work')] == work
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs
    assert history.progress(queue.db, source)['unobserved_nonzero_retained_scan']['checkpoint_problem'] == 'unsupported_version'


def test_damaged_blob_cursor_is_preserved_and_safely_rescanned_after_restart(queue):
    urls, source, marker = _started(queue)
    damaged = b'\xff\xfe'
    jobs = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (damaged, marker))
    assert history.progress(queue.db, source)['unobserved_nonzero_retained_scan']['checkpoint_problem'] == 'invalid_checkpoint'
    assert history.seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['restarted_invalid_checkpoint']
    audit = queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?',
        (history.LEGACY_NONZERO_SCAN_PREFIX + 'invalid:%',)).fetchall()
    assert [row[0] for row in audit] == [damaged]
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            result = history.seed_unobserved_nonzero_pages(resumed, limit=1, max_seconds=10)
            if result['complete']:
                break
        assert result['complete']
        assert [row[0] for row in resumed.db.execute(
            'SELECT request_url FROM cnyes_history_retained_work ORDER BY request_url')] == sorted(urls[:2])
        assert [tuple(row) for row in resumed.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs
        assert resumed.db.execute('SELECT scan_state FROM cnyes_history_local_inventory_scopes').fetchone()[0] == 'ready'


def test_nonzero_audit_conflict_keeps_cursor_and_fences(queue):
    _, source, marker = _started(queue)
    audit_key = history.LEGACY_NONZERO_SCAN_PREFIX + 'invalid:' + history.digest((marker, {'value': 'broken'}))
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('broken', marker))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (audit_key, 'other evidence'))
        queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET authorization_fingerprint='old'")
    fence = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes')]
    result = history.seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)
    assert result['blocked'] == 'nonzero_checkpoint_audit_conflict'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()[0] == 'other evidence'
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes')] == fence
    assert history.progress(queue.db, source)['unobserved_nonzero_retained_scan']['checkpoint_problem'] == 'audit_conflict'


def test_stop_during_nonzero_cursor_decode_preserves_cursor_and_fences(queue, monkeypatch):
    _, _, marker = _started(queue)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('broken', marker))
        queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET authorization_fingerprint='old'")
    fence = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes')]
    stop, original = threading.Event(), json.loads
    def stop_then_decode(value):
        stop.set()
        return original(value)
    with monkeypatch.context() as patch:
        patch.setattr(history.json, 'loads', stop_then_decode)
        result = history.seed_unobserved_nonzero_pages(queue, stop=stop, limit=8, max_seconds=10)
    assert not result['restarted_invalid_checkpoint']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes')] == fence


def test_stop_during_stale_fence_reconciliation_does_not_start_another(queue, monkeypatch):
    _, source, _ = _started(queue)
    url = history.cnyes_url(source.cnyes_apis[0], start=1388707200, end=1388707300)
    other_scope = history.cnyes_scope(url)
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET authorization_fingerprint='old'")
        queue.db.execute('INSERT INTO cnyes_history_local_inventory_scopes VALUES (?,?,?,?,?)',
                         (other_scope, history.cnyes_bulk_fingerprint(source), 'old', 'scanning',
                          '2020-01-01T00:00:00+00:00'))
    stop = threading.Event()
    calls = []
    def stop_after_one(*args):
        calls.append(args[1])
        stop.set()
    with monkeypatch.context() as patch:
        patch.setattr(history, 'reconcile_scope', stop_after_one)
        result = history.seed_unobserved_nonzero_pages(queue, stop=stop, limit=8, max_seconds=10)
    assert result['stale_fences_reconciled'] == 1 and len(calls) == 1
    assert [row[0] for row in queue.db.execute(
        'SELECT scan_state FROM cnyes_history_local_inventory_scopes ORDER BY scan_state')] == ['ready', 'scanning']


def test_blocked_nonzero_version_allows_normal_idle_cadence(queue):
    source = queue.registry.sources['cnyes']
    marker = history.nonzero_scan_key(source)
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({'version': 2})))
    for _ in range(8):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if result['exhausted']:
            break
    assert result['unobserved_nonzero_page_scan']['blocked'] == 'unsupported_nonzero_scan_checkpoint_version'
    assert result['exhausted'] and history.maintenance_delay(result) == 300


def test_blocked_nonzero_version_does_not_spin_on_fenced_work(queue):
    urls, _, marker = _started(queue)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps({'version': 2}), marker))
    for _ in range(12):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if result['exhausted']:
            break
    assert queue.db.execute('SELECT scan_state FROM cnyes_history_local_inventory_scopes').fetchone()[0] == 'scanning'
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[0],)).fetchone()[0] == 'pending'
    assert result['unobserved_nonzero_page_scan']['blocked'] == 'unsupported_nonzero_scan_checkpoint_version'
    assert result['exhausted'], {key: result[key] for key in (
        'repair_sweep_pending', 'repair_priority_pending', 'evidence_repair', 'next_local_retry_epoch',
        'overflow_retained_scan', 'unobserved_nonzero_page_scan')}
    assert history.maintenance_delay(result) == 300


@pytest.mark.parametrize('error_code', ['local_io_unavailable', 'accepted_batch_scan_pending'])
def test_blocked_nonzero_version_does_not_spin_on_fenced_retry(queue, error_code):
    urls, _, marker = _started(queue)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps({'version': 2}), marker))
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='local_retry' WHERE request_url=?", (urls[0],))
        queue.db.execute('INSERT INTO cnyes_history_retained_local_retries VALUES (?,?,?,?,?,?)',
                         (urls[0], 'pending', 0, 1, error_code, '2020-01-01T00:00:00+00:00'))
    for _ in range(12):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if result['exhausted']:
            break
    assert result['exhausted'] and history.maintenance_delay(result) == 300
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[0],)).fetchone()[0] == 'local_retry'
    assert queue.db.execute('SELECT error_code FROM cnyes_history_retained_local_retries WHERE request_url=?', (urls[0],)).fetchone()[0] == error_code


@pytest.mark.parametrize('retry', [False, True])
def test_blocked_nonzero_version_keeps_other_local_work_runnable(queue, retry):
    _, _, marker = _started(queue)
    url, _ = seed_retained_work(queue, failure='http_429', low=1388707200)
    job_before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps({'version': 2}), marker))
        if retry:
            queue.db.execute("UPDATE cnyes_history_retained_work SET state='local_retry' WHERE request_url=?", (url,))
            queue.db.execute('INSERT INTO cnyes_history_retained_local_retries VALUES (?,?,?,?,?,?)',
                             (url, 'pending', 0, 1, 'local_io_unavailable', '2020-01-01T00:00:00+00:00'))
    for _ in range(12):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'restored':
            break
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'restored'
    assert result['unobserved_nonzero_page_scan']['blocked'] == 'unsupported_nonzero_scan_checkpoint_version'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == job_before
