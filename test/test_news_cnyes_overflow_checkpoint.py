"""Overflow recovery keeps original evidence across a damaged local cursor."""

from contextlib import closing
import json
import sqlite3
import threading

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_queue import NewsQueue
from test_news_cnyes_history_evidence import (  # noqa: F401
    cfg, queue, seed_legacy_overflow, seed_retained_work,
)


@pytest.mark.parametrize('damaged', [
    'not-json',
    json.dumps({'version': 1, 'state': 'scanning', 'scope_url': '', 'page': 31}),
    b'\xff\xfe',
])
def test_corrupt_overflow_cursor_restarts_with_evidence_and_preserves_jobs(queue, damaged):
    urls, _ = seed_legacy_overflow(queue)
    source = queue.registry.sources['cnyes']
    marker = history.overflow_scan_key(source)
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=2 WHERE url=?", (urls[0],))
    jobs_before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    assert history.seed_overflow_retained(queue, limit=1, max_seconds=10)['queued'] == 1
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (damaged, marker))
    assert history.progress(queue.db, source)['overflow_retained_recovery']['checkpoint_problem'] == 'invalid_checkpoint'

    recovered = history.seed_overflow_retained(queue, limit=1, max_seconds=10)
    assert recovered['restarted_invalid_checkpoint'] and not recovered['complete']
    evidence = queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?',
        (history.OVERFLOW_RETAINED_SCAN_PREFIX + 'invalid:%',)).fetchall()
    assert [row[0] for row in evidence] == [damaged]
    assert history.progress(queue.db, source)['overflow_retained_recovery']['checkpoint_problem'] is None
    # The recovery did not bypass the all-scopes-indexed fence.
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
    queue.close()

    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(12):
            result = history.backfill(resumed, limit=1, max_seconds=10)
            if result['exhausted']:
                break
        assert result['overflow_retained_scan']['complete']
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references').fetchone()[0] == 1
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 1
        assert [tuple(row) for row in resumed.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs_before
        assert resumed.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


@pytest.mark.parametrize('version', [2, '2', True, None])
def test_unrecognized_overflow_version_remains_blocked_without_rewriting(queue, version):
    seed_legacy_overflow(queue)
    marker = history.overflow_scan_key(queue.registry.sources['cnyes'])
    original = json.dumps({'version': version, 'state': 'scanning', 'scope_url': '', 'page': 30})
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, original))
    result = history.seed_overflow_retained(queue, limit=1, max_seconds=10)
    assert result['blocked'] == 'unsupported_overflow_scan_checkpoint_version'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == original
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 0
    public_state = history.progress(queue.db, queue.registry.sources['cnyes'])['overflow_retained_recovery']
    assert public_state['scan_state'] == 'unknown'
    assert public_state['checkpoint_problem'] == 'unsupported_version'


def test_stopped_overflow_scan_does_not_rewrite_damaged_cursor(queue):
    seed_legacy_overflow(queue)
    marker = history.overflow_scan_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, 'broken'))
    stop = threading.Event()
    stop.set()
    assert not history.seed_overflow_retained(queue, stop=stop)['complete']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 0


def test_stop_during_cursor_decode_does_not_rewrite_damaged_cursor(queue, monkeypatch):
    seed_legacy_overflow(queue)
    marker = history.overflow_scan_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, 'broken'))
    stop = threading.Event()
    original = json.loads

    def stop_then_decode(value):
        stop.set()
        return original(value)

    with monkeypatch.context() as patch:
        patch.setattr(history.json, 'loads', stop_then_decode)
        result = history.seed_overflow_retained(queue, stop=stop, limit=1, max_seconds=10)
    assert not result['restarted_invalid_checkpoint']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT count(*) FROM queue_settings WHERE key LIKE ?',
        (history.OVERFLOW_RETAINED_SCAN_PREFIX + 'invalid:%',)).fetchone()[0] == 0


def test_cursor_evidence_write_failure_keeps_original_for_retry(queue):
    seed_legacy_overflow(queue)
    marker = history.overflow_scan_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, 'broken'))
        queue.db.execute("CREATE TEMP TRIGGER reject_overflow_audit BEFORE INSERT ON queue_settings "
            "WHEN NEW.key LIKE 'cnyes_history_overflow_retained_scan_v1:invalid:%' "
            "BEGIN SELECT RAISE(ABORT, 'simulated audit failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='simulated audit failure'):
        history.seed_overflow_retained(queue, limit=1, max_seconds=10)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 0


def test_conflicting_archived_cursor_does_not_destroy_original(queue):
    seed_legacy_overflow(queue)
    marker = history.overflow_scan_key(queue.registry.sources['cnyes'])
    audit_key = history.OVERFLOW_RETAINED_SCAN_PREFIX + 'invalid:' + history.digest((marker, {'value': 'broken'}))
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, 'broken'))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (audit_key, 'different evidence'))
    result = history.seed_overflow_retained(queue, limit=1, max_seconds=10)
    assert result['blocked'] == 'overflow_checkpoint_audit_conflict'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()[0] == 'different evidence'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 0
    public_state = history.progress(queue.db, queue.registry.sources['cnyes'])['overflow_retained_recovery']
    assert public_state['scan_state'] == 'unknown' and public_state['checkpoint_problem'] == 'audit_conflict'


def test_unsupported_overflow_version_does_not_block_other_local_lanes(queue):
    seed_legacy_overflow(queue)
    marker = history.overflow_scan_key(queue.registry.sources['cnyes'])
    unknown = json.dumps({'version': 2, 'state': 'scanning', 'scope_url': '', 'page': 30})
    url, _ = seed_retained_work(queue, failure='http_429', low=1388620800)
    job_before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, unknown))
    for _ in range(8):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'restored':
            break
    assert result['overflow_retained_scan']['blocked'] == 'unsupported_overflow_scan_checkpoint_version'
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'restored'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == job_before
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == unknown
