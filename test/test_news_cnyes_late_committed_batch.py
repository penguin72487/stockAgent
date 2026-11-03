"""Newly committed Cnyes batches must wake already-passed local inventory."""
from contextlib import closing
import hashlib
import json
import sqlite3

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_bulk import cnyes_records, cnyes_scope, cnyes_url
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg, registry_http, response  # noqa: F401
from test_news_cnyes_history_evidence import (  # noqa: F401
    queue, seed_accepted_batch_for_history_page, seed_unindexed_nonzero_pages, source_payload,
)


def _accepted_records(queue, url, data):
    source = queue.registry.sources['cnyes']
    raw = response(data, url)
    records, errors = cnyes_records(raw, type('HTTP', (), {'registry': queue.registry})(), source)
    assert len(records) == 1 and not errors
    queue.add_articles([{'url': records[0]['requested_url'], 'title': records[0]['title'], 'date': None}])
    records[0]['raw_response_body'] = raw.body
    records[0]['source_response_article_id'] = records[0]['article_id']
    return records


def _old_inventory_complete(queue, source):
    nonzero = history.seed_unobserved_nonzero_pages(queue, limit=100, max_seconds=10)
    assert nonzero['complete']
    batch = history.seed_accepted_body_batch_candidates(queue, limit=100, max_seconds=10)
    assert batch['complete']
    assert history.accepted_batch_inventory_complete(queue, source)


def test_live_committed_batch_after_inventory_wakes_local_page_without_changing_source(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    _old_inventory_complete(queue, source)
    old_source = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()
    data = source_payload([61], epoch=1388534400, page=3, total=61)
    accepted = queue.accept(_accepted_records(queue, urls[2], data), return_batch=True)
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates '
                            'WHERE request_url=? AND parquet_path=?', (urls[2], accepted['parquet_path'])).fetchone()
    work = queue.db.execute('SELECT state,response_sha256 FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()
    assert work and work['state'] == 'pending'
    assert work['response_sha256'] == hashlib.sha256(response(data, urls[2]).body).hexdigest()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == old_source
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                (cnyes_scope(urls[0]),)).fetchone()
    assert getattr(queue, '_cnyes_original_priority_needs_wake', False)
    for _ in range(12):
        history.backfill(queue, limit=16, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    observation = json.loads(queue.db.execute('SELECT o.observation_json FROM cnyes_history_page_heads h '
        'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
        'WHERE h.scope_url=? AND h.page=3', (cnyes_scope(urls[0]),)).fetchone()[0])
    assert observation['source_observed_at_utc'] == _accepted_capture_clock(queue, accepted['parquet_path'])
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == old_source


def _accepted_capture_clock(queue, relative):
    import pyarrow.parquet as pq
    return pq.read_table(queue.root / relative, columns=['captured_at_utc']).to_pylist()[0]['captured_at_utc']


def test_startup_orphan_before_completed_cursor_is_revisited_after_recovery(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    _old_inventory_complete(queue, source)
    old_source = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    records = _accepted_records(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    # Simulate the existing real crash boundary: immutable batch and receipt
    # are written but the SQLite acceptance transaction does not commit.
    with monkeypatch.context() as patch:
        patch.setattr(queue, '_accept_records', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('crash')))
        with pytest.raises(RuntimeError, match='crash'):
            queue.accept(records)
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        accepted = resumed.db.execute('SELECT path FROM accepted_batches').fetchone()
        assert accepted
        for _ in range(12):
            history.backfill(resumed, limit=16, max_seconds=10)
            if resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                  (cnyes_scope(urls[0]),)).fetchone():
                break
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                  (cnyes_scope(urls[0]),)).fetchone()
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == old_source


def test_pre_migration_batch_retry_survives_another_restart_and_keeps_existing_deadline(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    _old_inventory_complete(queue, source)
    old_source = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    data = source_payload([61], epoch=1388534400, page=3, total=61)
    raw = response(data, urls[2])
    relative, receipt = seed_accepted_batch_for_history_page(queue, urls[2], data)
    marker = history.accepted_batch_scan_key(source)
    # Simulate the pre-migration stage of acceptance recovery. A pre-existing
    # local retry is never accelerated or reset by a repeated registration.
    queue._cnyes_history_fully_initialized = False
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_accepted_batch_local_retries VALUES (?,?,?,?,?)',
                         (marker, relative, receipt, 9000000000, 3))
        history.register_accepted_body_batch(queue, [{
            'provider': 'cnyes', 'source_request_url': urls[2], 'raw_response_body': raw.body,
            'response_sha256': hashlib.sha256(raw.body).hexdigest(),
        }], relative, receipt)
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        retry = resumed.db.execute('SELECT not_before_epoch,local_attempts FROM '
            'cnyes_history_accepted_batch_local_retries WHERE scan_key=? AND parquet_path=?',
            (marker, relative)).fetchone()
        assert tuple(retry) == (9000000000, 3)
        assert not history.accepted_batch_inventory_complete(resumed, source)
        with resumed.db:
            resumed.db.execute('UPDATE cnyes_history_accepted_batch_local_retries '
                               'SET not_before_epoch=0 WHERE scan_key=? AND parquet_path=?', (marker, relative))
    with closing(NewsQueue(queue.root, queue.registry)) as resumed_again:
        for _ in range(12):
            history.backfill(resumed_again, limit=16, max_seconds=10)
            if resumed_again.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                        (cnyes_scope(urls[0]),)).fetchone():
                break
        assert resumed_again.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                        (cnyes_scope(urls[0]),)).fetchone()
        assert dict(resumed_again.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == old_source


def test_wrong_authorization_candidate_never_becomes_page_proof(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    _old_inventory_complete(queue, source)
    old_source = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    records = _accepted_records(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    records[0]['authorization_fingerprint'] = 'f' * 64
    queue.accept(records)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()[0] == 'pending'
    for _ in range(12):
        history.backfill(queue, limit=16, max_seconds=10)
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                (cnyes_scope(urls[0]),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == old_source


def test_identical_bytes_from_another_request_do_not_wake_the_target(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    _old_inventory_complete(queue, source)
    old_source = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    records = _accepted_records(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    records[0]['source_request_url'] = cnyes_url(source.cnyes_apis[0], 3, start=1388534501, end=1388534600)
    queue.accept(records)
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE request_url=?',
                                (urls[2],)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == old_source


def test_late_work_failure_rolls_back_acceptance_then_orphan_recovery_retries(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    _old_inventory_complete(queue, source)
    old_source = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    records = _accepted_records(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    with monkeypatch.context() as patch:
        patch.setattr(history, '_queue_late_accepted_candidate',
                      lambda *a, **k: (_ for _ in ()).throw(OSError('simulated local failure')))
        with pytest.raises(OSError, match='simulated local failure'):
            queue.accept(records)
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates').fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()
    queue.recover_acceptance()
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()[0] == 'pending'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == old_source


@pytest.mark.parametrize('position,expected_retry', [
    ('unstarted', False), ('inside_frontier', False), ('behind_cursor', True),
    ('beyond_frontier', True), ('unknown_version', True),
])
def test_pre_migration_retry_depends_on_frozen_frontier(queue, position, expected_retry):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    assert history.seed_unobserved_nonzero_pages(queue, limit=100, max_seconds=10)['complete']
    data = source_payload([61], epoch=1388534400, page=3, total=61)
    raw = response(data, urls[2])
    relative, receipt = seed_accepted_batch_for_history_page(queue, urls[2], data)
    marker = history.accepted_batch_scan_key(source)
    states = {
        'inside_frontier': {'version': 1, 'state': 'scanning', 'path': 'a', 'end_path': 'z',
                            'checked': 0, 'indexed': 0, 'retry_after': 0, 'attempts': 0},
        'behind_cursor': {'version': 1, 'state': 'scanning', 'path': 'z', 'end_path': 'z',
                          'checked': 1, 'indexed': 0, 'retry_after': 0, 'attempts': 0},
        'beyond_frontier': {'version': 1, 'state': 'scanning', 'path': '', 'end_path': 'a',
                            'checked': 0, 'indexed': 0, 'retry_after': 0, 'attempts': 0},
        'unknown_version': {'version': 2, 'state': 'opaque'},
    }
    queue._cnyes_history_fully_initialized = False
    with queue.db:
        if position in states:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, json.dumps(states[position])))
        history.register_accepted_body_batch(queue, [{
            'provider': 'cnyes', 'source_request_url': urls[2], 'raw_response_body': raw.body,
            'response_sha256': hashlib.sha256(raw.body).hexdigest(),
        }], relative, receipt)
    retry = queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries '
                             'WHERE scan_key=? AND parquet_path=?', (marker, relative)).fetchone()
    assert bool(retry) is expected_retry
    queue._cnyes_history_fully_initialized = True
    scan = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    if position == 'unknown_version':
        assert scan['blocked'] == 'unsupported_accepted_batch_checkpoint_version'
        assert json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]) == states[position]
        assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries '
                                'WHERE scan_key=? AND parquet_path=?', (marker, relative)).fetchone()
    else:
        assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                (urls[2],)).fetchone()[0] == 'pending'


def test_orphan_retry_commits_before_full_history_migration_and_survives_second_crash(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    source = queue.registry.sources['cnyes']
    _old_inventory_complete(queue, source)
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    records = _accepted_records(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    with monkeypatch.context() as patch:
        patch.setattr(queue, '_accept_records', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('first crash')))
        with pytest.raises(RuntimeError, match='first crash'):
            queue.accept(records)
    # The paused production snapshot still has the older schema without this
    # table. Recovery must create it before consuming an orphan receipt.
    with queue.db:
        queue.db.execute('DROP TABLE cnyes_history_accepted_batch_local_retries')
    root, registry = queue.root, queue.registry
    queue.close()
    with monkeypatch.context() as patch:
        patch.setattr(history, 'initialize', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('second crash')))
        with pytest.raises(RuntimeError, match='second crash'):
            NewsQueue(root, registry)
    # The failed constructor closes the owner; inspect the committed state
    # independently before allowing another full initialization.
    with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True)) as snapshot:
        assert snapshot.execute('SELECT 1 FROM accepted_batches').fetchone()
        assert snapshot.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries').fetchone()
    with closing(NewsQueue(root, registry)) as resumed:
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries').fetchone()
        for _ in range(12):
            history.backfill(resumed, limit=16, max_seconds=10)
            if resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                  (cnyes_scope(urls[0]),)).fetchone():
                break
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                  (cnyes_scope(urls[0]),)).fetchone()
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == original
