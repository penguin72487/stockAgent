"""Idempotent local reconciliation survives damaged checkpoints without source requests."""

from contextlib import closing
import json
import sqlite3
import threading

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_bulk import cnyes_scope, cnyes_url
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import BASE
from test_news_cnyes_history_evidence import (  # noqa: F401
    bounds, capture, cfg, queue, seed_retained_work, source_payload,
)


def scan_settings(queue, kind):
    source = queue.registry.sources['cnyes']
    if kind == 'mismatch':
        return history.LEGACY_MISMATCH_SCAN_KEY, history.LEGACY_MISMATCH_SCAN_KEY + ':', 'request_url', history.reconcile_legacy_request_mismatches
    if kind == 'unavailable':
        return history.LEGACY_UNAVAILABLE_SCAN_KEY, history.LEGACY_UNAVAILABLE_SCAN_KEY + ':', 'request_url', history.reconcile_legacy_unavailable
    marker = history.LEGACY_EMPTY_SCAN_PREFIX + history.digest((
        source.provider, cnyes_bulk_fingerprint(source), source.authorization_fingerprint))
    return marker, history.LEGACY_EMPTY_SCAN_PREFIX, 'base_url', history.seed_unobserved_empty_scopes


def seed(queue, kind):
    urls = []
    for index in range(3):
        low = 1388534400 + index * 86400
        if kind == 'empty':
            url = cnyes_url(BASE, start=low, end=low + 100)
            with queue.db:
                queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',0,1,'old-clock')", (cnyes_scope(url),))
                queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts) "
                                 "VALUES (?,'cnyes_news','cnyes',9e9,?,?)",
                                 (url, 'http_429' if index == 2 else None, int(index == 2)))
        else:
            url, _ = seed_retained_work(queue, failure='http_429', valid_raw=False, low=low)
            capture(queue, url, source_payload([1, 2], epoch=bounds(url)[0]))
            outcome = json.dumps({'error': 'retained_original_request_differs_not_relabelled', 'old': index})
            with queue.db:
                queue.db.execute('UPDATE cnyes_history_retained_work SET state=?,outcome_json=? WHERE request_url=?',
                                 ('retained_invalid' if kind == 'mismatch' else 'retained_unavailable', outcome, url))
                queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=2,due=9e9 WHERE url=?", (url,))
        urls.append(url)
    return urls


@pytest.mark.parametrize('kind', ['mismatch', 'unavailable', 'empty'])
@pytest.mark.parametrize('damaged', ['not-json', b'\xff\xfe', json.dumps({'version': 1, 'state': 'broken'})])
def test_damaged_cursor_is_archived_then_reconciles_in_bounded_restartable_slices(queue, kind, damaged):
    urls = seed(queue, kind)
    marker, prefix, _, scan = scan_settings(queue, kind)
    jobs_before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM discovery')}
    outcomes = {row['request_url']: row['outcome_json'] for row in queue.db.execute('SELECT * FROM cnyes_history_retained_work')}
    first = scan(queue, limit=1, max_seconds=10)
    assert not first['complete']
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (damaged, marker))
    result = scan(queue, limit=1, max_seconds=10)
    assert result['restarted_invalid_checkpoint'] and not result['complete']
    assert [row[0] for row in queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?',
                                                (prefix + 'invalid:%',))] == [damaged]
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(8):
            result = scan(resumed, limit=1, max_seconds=10)
            if result['complete']:
                break
        assert result['complete']
        jobs_after = {row['url']: dict(row) for row in resumed.db.execute('SELECT * FROM discovery')}
        if kind == 'empty':
            # The pre-existing empty-scope migration schedules unfailed old
            # successes for verification, but must not bypass a 429 deadline.
            assert jobs_after[urls[2]] == jobs_before[urls[2]]
            for url in urls[:2]:
                after, before = jobs_after[url].copy(), jobs_before[url].copy()
                assert 0 < after.pop('due') <= before.pop('due')
                assert after == before
            assert resumed.db.execute('SELECT count(*) FROM cnyes_history_scope_heads').fetchone()[0] == 2
            assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
        else:
            assert jobs_after == jobs_before
            rows = resumed.db.execute('SELECT request_url,state,outcome_json FROM cnyes_history_retained_work').fetchall()
            assert all(row['state'] == 'forward_evidence_available' and row['outcome_json'] == outcomes[row['request_url']] for row in rows)
            assert resumed.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] == len(urls)
        assert scan(resumed, limit=1, max_seconds=10)['complete']


@pytest.mark.parametrize('kind', ['mismatch', 'unavailable', 'empty'])
@pytest.mark.parametrize('version', [2, '2', True, None])
def test_unknown_version_preserves_cursor_and_reports_scoped_problem(queue, kind, version):
    marker, prefix, field, scan = scan_settings(queue, kind)
    original = json.dumps({'version': version, 'state': 'scanning', field: ''})
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, original))
    result = scan(queue, limit=1, max_seconds=10)
    assert result['blocked'] == 'unsupported_legacy_checkpoint_version'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == original
    assert queue.db.execute('SELECT count(*) FROM queue_settings WHERE key LIKE ?', (prefix + 'invalid:%',)).fetchone()[0] == 0
    status = history.progress(queue.db, queue.registry.sources['cnyes'])['legacy_metadata_scans'][kind]
    assert status == {'scan_state': 'unknown', 'checkpoint_problem': 'unsupported_version'}


@pytest.mark.parametrize('kind', ['mismatch', 'unavailable', 'empty'])
def test_archive_conflict_and_write_failure_preserve_original(queue, kind):
    marker, prefix, _, scan = scan_settings(queue, kind)
    audit_key = prefix + 'invalid:' + history.digest((marker, {'value': 'broken'}))
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, 'broken'))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (audit_key, 'different evidence'))
    result = scan(queue, limit=1, max_seconds=10)
    assert result['blocked'] == 'legacy_checkpoint_audit_conflict'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (audit_key,)).fetchone()[0] == 'different evidence'
    status = history.progress(queue.db, queue.registry.sources['cnyes'])['legacy_metadata_scans'][kind]
    assert status == {'scan_state': 'unknown', 'checkpoint_problem': 'audit_conflict'}
    with queue.db:
        queue.db.execute('DELETE FROM queue_settings WHERE key=?', (audit_key,))
        queue.db.execute("CREATE TEMP TRIGGER reject_legacy_audit BEFORE INSERT ON queue_settings "
                         "WHEN NEW.key LIKE '%:invalid:%' BEGIN SELECT RAISE(ABORT, 'simulated audit failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='simulated audit failure'):
        scan(queue, limit=1, max_seconds=10)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'


@pytest.mark.parametrize('kind', ['mismatch', 'unavailable', 'empty'])
def test_stop_during_decode_does_not_archive_cursor(queue, kind, monkeypatch):
    marker, prefix, _, scan = scan_settings(queue, kind)
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, 'broken'))
    stop = threading.Event()
    original = json.loads
    def stop_then_decode(value):
        stop.set()
        return original(value)
    with monkeypatch.context() as patch:
        patch.setattr(history.json, 'loads', stop_then_decode)
        result = scan(queue, stop=stop, limit=1, max_seconds=10)
    assert not result['restarted_invalid_checkpoint']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT count(*) FROM queue_settings WHERE key LIKE ?', (prefix + 'invalid:%',)).fetchone()[0] == 0


@pytest.mark.parametrize('kind', ['mismatch', 'unavailable', 'empty'])
def test_unsupported_legacy_scan_does_not_starve_other_local_work_or_spin(queue, kind):
    marker, _, field, _ = scan_settings(queue, kind)
    original = json.dumps({'version': 2, 'state': 'scanning', field: ''})
    url, _ = seed_retained_work(queue, failure='http_429')
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, original))
    for _ in range(30):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if result['exhausted']:
            break
    assert result['exhausted'] and history.maintenance_delay(result) == 300
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'restored'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == original
