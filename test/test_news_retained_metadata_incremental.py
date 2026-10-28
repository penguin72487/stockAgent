"""Incremental mobile indexing follows durable acceptance and preserves evidence."""
from contextlib import closing
import json
import sqlite3
from types import SimpleNamespace

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metadata_index import advance_metadata_index_scan, begin_metadata_index_scan, register_metadata_head
from downloader.news_fulltext_metadata_replay import _ensure_cohort, replay_retained_cnyes_metadata, retained_metadata_next_wake_delay
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_cnyes_news_bulk import cfg
from test_news_cnyes_canonical_alias import mobile_page


CAPTURED = '2026-10-01T00:00:00+00:00'


def captured(queue, number, *, incomplete=True, accept=True):
    number = str(number)
    url = 'https://m.cnyes.com/news/id/' + number
    queue.add_articles([{'url': url}])
    head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    payload = mobile_page().replace(b'42', number.encode())
    http = SimpleNamespace(registry=queue.registry,
        get=lambda *_a, **_kw: Response(url, 200, {'content-type': 'text/html'}, payload))
    record = fetch_article(http, head)
    record.update(captured_at_utc=CAPTURED, data_fingerprint='retained-test-original')
    if incomplete:
        record.update(published_at_raw=None, authors_json='[]', body_completeness='article_container_unverified')
    if accept:
        queue.accept([record])
    return head['id'], record


def current(queue, key):
    return dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (key,)).fetchone())


def checkpoint(queue, key):
    return dict(queue.db.execute('SELECT * FROM retained_metadata_replays WHERE article_id=?', (key,)).fetchone())


def test_new_incomplete_head_is_registered_atomically_after_fixed_snapshot(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        _ensure_cohort(queue, queue.registry.sources['cnyes'])
        complete, _ = captured(queue, 101, incomplete=False)
        assert queue.db.execute('SELECT 1 FROM retained_metadata_replays WHERE article_id=?', (complete,)).fetchone() is None
        key, _ = captured(queue, 102)
        old = current(queue, key)
        assert checkpoint(queue, key)['original_path'] == old['parquet_path']
        assert checkpoint(queue, key)['state'] == 'pending'
        assert queue.db.execute('SELECT 1 FROM accepted_batches WHERE path=?', (old['parquet_path'],)).fetchone()
        digest = sha256_file(tmp_path / old['parquet_path'])
        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert result['http_requests'] == 0 and result['repaired_versions'] == 1
        assert checkpoint(queue, key)['state'] == 'verified_repair'
        assert sha256_file(tmp_path / old['parquet_path']) == digest
        assert current(queue, key)['last_checked_at_utc'] == old['last_checked_at_utc']
        assert verify_bodies(tmp_path)['accepted']


def test_new_version_archives_prior_checkpoint_without_reopening_same_head(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        key, _ = captured(queue, 103)
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        prior = checkpoint(queue, key)
        old_path = prior['original_path']
        digest = sha256_file(tmp_path / old_path)
        captured(queue, 103)
        updated = checkpoint(queue, key)
        assert updated['original_path'] != prior['original_path'] and updated['state'] == 'pending'
        archive = queue.db.execute('''SELECT original_path,original_body_sha256,state,error_code,reason
            FROM retained_metadata_checkpoint_history WHERE article_id=?''', (key,)).fetchall()
        assert [tuple(row) for row in archive] == [(prior['original_path'], prior['original_body_sha256'],
                                                    'verified_repair', None, 'acceptance')]
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation',error_code='actual_proof_failure' WHERE article_id=?", (key,))
            assert not register_metadata_head(queue.db, queue.registry.sources['cnyes'].data_fingerprint,
                key, updated['original_path'], updated['original_body_sha256'], reason='same_head', now=CAPTURED)
        assert checkpoint(queue, key)['error_code'] == 'actual_proof_failure'
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_checkpoint_history').fetchone()[0] == 1
        assert sha256_file(tmp_path / old_path) == digest and verify_bodies(tmp_path)['accepted']


def test_interrupted_acceptance_recovers_checkpoint_and_source_receipt_together(cfg, tmp_path):
    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        key, _ = captured(queue, 104)
        _ensure_cohort(queue, queue.registry.sources['cnyes'])
        _, record = captured(queue, 104, accept=False)
        old, old_checkpoint = current(queue, key), checkpoint(queue, key)
        with queue.db:
            queue.db.execute('''CREATE TRIGGER inject_metadata_archive_failure BEFORE INSERT
                ON retained_metadata_checkpoint_history BEGIN SELECT RAISE(ABORT, 'injected archive failure'); END''')
        with pytest.raises(sqlite3.IntegrityError, match='injected archive failure'):
            queue.accept([record])
        assert current(queue, key) == old and checkpoint(queue, key) == old_checkpoint
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_checkpoint_history').fetchone()[0] == 0
        with queue.db:
            queue.db.execute('DROP TRIGGER inject_metadata_archive_failure')
    with closing(NewsQueue(tmp_path, registry)) as queue:
        assert current(queue, key)['parquet_path'] != old['parquet_path']
        assert checkpoint(queue, key)['original_path'] == current(queue, key)['parquet_path']
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_checkpoint_history').fetchone()[0] == 1
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        assert verify_bodies(tmp_path)['accepted']


def test_legacy_scan_resumes_and_catches_state_movement_behind_cursor(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext_queue as queue_module

    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        old, _ = captured(queue, 105)
        _ensure_cohort(queue, registry.sources['cnyes'])
        prior = checkpoint(queue, old)
        with monkeypatch.context() as legacy:
            legacy.setattr(queue_module, 'register_accepted_metadata_head', lambda *_a, **_kw: False)
            missing, _ = captured(queue, 106)
            moving, _ = captured(queue, 107)
            captured(queue, 105)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry' WHERE id=?", (moving,))
            queue.db.execute('DELETE FROM retained_metadata_incremental_scans')
            queue.db.execute('DELETE FROM retained_metadata_incremental_current')
        _ensure_cohort(queue, registry.sources['cnyes'])
        assert retained_metadata_next_wake_delay(queue, 60) == 1
        first = advance_metadata_index_scan(queue.db, registry.sources['cnyes'].data_fingerprint,
                                            now=CAPTURED, scan_rows=1, dirty_rows=1)
        assert first['rows_scanned'] == 1
        with queue.db:
            # This transition puts the unscanned row before the scan cursor.
            # The migration trigger durably journals it in the same transaction.
            queue.db.execute("UPDATE articles SET state='a_moved' WHERE id=?", (moving,))
        assert queue.db.execute('SELECT 1 FROM retained_metadata_incremental_dirty WHERE article_id=?', (moving,)).fetchone()
    with closing(NewsQueue(tmp_path, registry)) as queue:
        source = registry.sources['cnyes']
        for _ in range(20):
            advance_metadata_index_scan(queue.db, source.data_fingerprint, now=CAPTURED, scan_rows=1, dirty_rows=1)
            state = queue.db.execute('SELECT state FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                                     (source.data_fingerprint,)).fetchone()[0]
            if state == 'complete':
                break
        assert state == 'complete'
        assert checkpoint(queue, missing)['original_path'] == current(queue, missing)['parquet_path']
        assert checkpoint(queue, moving)['original_path'] == current(queue, moving)['parquet_path']
        assert checkpoint(queue, old)['original_path'] == current(queue, old)['parquet_path']
        history = queue.db.execute('SELECT original_path,state,reason FROM retained_metadata_checkpoint_history WHERE article_id=?', (old,)).fetchone()
        assert tuple(history) == (prior['original_path'], prior['state'], 'legacy_scan')
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_incremental_dirty').fetchone()[0] == 0
        with queue.db:
            queue.db.execute("UPDATE articles SET state='complete' WHERE id=?", (moving,))
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_incremental_dirty').fetchone()[0] == 0
        assert verify_bodies(tmp_path)['accepted']


def test_schema_change_or_vacuum_restarts_rowid_cursor_without_missing_heads(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        keys = [captured(queue, number)[0] for number in (108, 109, 110)]
        source = queue.registry.sources['cnyes']
        _ensure_cohort(queue, source)
        with queue.db:
            queue.db.execute('DELETE FROM retained_metadata_replays WHERE article_id IN (?,?)', keys[1:])
            queue.db.execute('DELETE FROM retained_metadata_incremental_scans')
            queue.db.execute('DELETE FROM retained_metadata_incremental_current')
        _ensure_cohort(queue, source)
        result = advance_metadata_index_scan(queue.db, source.data_fingerprint,
                                             now=CAPTURED, scan_rows=1, dirty_rows=1)
        assert result['rows_scanned'] == 1
        old_rowid = queue.db.execute('SELECT rowid FROM articles WHERE id=?', (keys[1],)).fetchone()[0]
        with queue.db:
            # Fixture-only rowid rewrite simulates a database rebuild that
            # moves a not-yet-scanned head behind the persisted cursor.
            queue.db.execute('DELETE FROM articles WHERE id=?', (keys[0],))
            queue.db.execute('UPDATE articles SET rowid=1 WHERE id=?', (keys[1],))
        queue.db.execute('VACUUM')
        new_rowid = queue.db.execute('SELECT rowid FROM articles WHERE id=?', (keys[1],)).fetchone()[0]
        assert new_rowid < old_rowid
        advance_metadata_index_scan(queue.db, source.data_fingerprint, now=CAPTURED, scan_rows=1, dirty_rows=1)
        assert checkpoint(queue, keys[1])['original_path'] == current(queue, keys[1])['parquet_path']
        assert queue.db.execute('SELECT cursor_resets FROM retained_metadata_incremental_scans').fetchone()[0] == 1


def test_large_initial_cohort_uses_bounded_scan_instead_of_stopping(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_replay as replay

    monkeypatch.setattr(replay, 'METADATA_INITIAL_COHORT_LIMIT', 2)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        keys = [captured(queue, number, incomplete=False)[0] for number in (111, 112, 113)]
        source = queue.registry.sources['cnyes']
        _ensure_cohort(queue, source)
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_replays').fetchone()[0] == 0
        assert queue.db.execute('SELECT state FROM retained_metadata_incremental_scans').fetchone()[0] == 'scanning'
        marker = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                  ('cnyes_mobile_metadata_cohort_v1:' + source.data_fingerprint,)).fetchone()[0]
        assert json.loads(marker)['heads'] is None and json.loads(marker)['initial_index_mode'] == 'incremental_scan'
        for _ in range(8):
            advance_metadata_index_scan(queue.db, source.data_fingerprint, now=CAPTURED, scan_rows=1, dirty_rows=1)
            if queue.db.execute('SELECT state FROM retained_metadata_incremental_scans').fetchone()[0] == 'complete':
                break
        assert queue.db.execute('SELECT state FROM retained_metadata_incremental_scans').fetchone()[0] == 'complete'
        assert {row[0] for row in queue.db.execute('SELECT article_id FROM retained_metadata_replays')} == set(keys)
        assert verify_bodies(tmp_path)['accepted']


def test_large_cohort_incrementally_reuses_only_matching_chart_checkpoints(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_replay as replay

    monkeypatch.setattr(replay, 'METADATA_INITIAL_COHORT_LIMIT', 1)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        keys = [captured(queue, number, incomplete=False)[0] for number in range(311, 320)]
        source = queue.registry.sources['cnyes']
        previous, fingerprint = source.parser_repair_prior_capture_fingerprint, source.data_fingerprint
        heads = {key: current(queue, key) for key in keys}
        cases = [
            ('current_metadata_complete', None),
            ('not_mobile_html_candidate', None),
            ('failed_validation', 'article_identity_not_verified'),
            ('failed_validation', 'body_not_found'),
            ('current_metadata_complete', None),  # Different original digest.
            ('current_metadata_complete', None),  # Different policy fingerprint.
            ('current_metadata_complete', None),  # Existing pending checkpoint.
            ('current_metadata_complete', None),  # Existing current failure.
            ('failed_validation', 'retained_metadata_batch_head_not_unique'),  # Old batch-wide failure.
        ]
        with queue.db:
            for index, (key, (state, error)) in enumerate(zip(keys, cases)):
                head = heads[key]
                digest = 'different' if index == 4 else head['body_sha256']
                prior = 'another-policy' if index == 5 else previous
                queue.db.execute('INSERT INTO retained_metadata_replays VALUES (?,?,?,?,?,?,?)',
                    (prior, key, head['parquet_path'], digest, state, error, CAPTURED))
            for index, state, error in ((6, 'pending', None), (7, 'failed_validation', 'new_failure')):
                head = heads[keys[index]]
                queue.db.execute('INSERT INTO retained_metadata_replays VALUES (?,?,?,?,?,?,?)',
                    (fingerprint, keys[index], head['parquet_path'], head['body_sha256'], state, error, CAPTURED))
        _ensure_cohort(queue, source)
        for _ in range(20):
            result = advance_metadata_index_scan(queue.db, fingerprint, now=CAPTURED, scan_rows=2, dirty_rows=1,
                                                 prior_fingerprint=previous)
            assert result['rows_scanned'] <= 2
            if queue.db.execute('SELECT state FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                                (fingerprint,)).fetchone()[0] == 'complete':
                break
        assert queue.db.execute('SELECT state FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                                (fingerprint,)).fetchone()[0] == 'complete'
        outcomes = [tuple(queue.db.execute('SELECT state,error_code FROM retained_metadata_replays '
                    'WHERE source_fingerprint=? AND article_id=?', (fingerprint, key)).fetchone()) for key in keys]
        assert outcomes == [
            ('current_metadata_complete', None), ('not_mobile_html_candidate', None),
            ('failed_validation', 'article_identity_not_verified'), ('pending', None),
            ('pending', None), ('pending', None), ('current_metadata_complete', None),
            ('failed_validation', 'new_failure'), ('pending', None),
        ]
        assert {key: current(queue, key) for key in keys} == heads
        assert verify_bodies(tmp_path)['accepted']


def test_resident_large_cohort_does_not_reread_matching_prior_captures(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_replay as replay

    monkeypatch.setattr(replay, 'METADATA_INITIAL_COHORT_LIMIT', 1)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        keys = [captured(queue, number, incomplete=False)[0] for number in (321, 322, 323)]
        source = queue.registry.sources['cnyes']
        with queue.db:
            for key in keys:
                head = current(queue, key)
                queue.db.execute('INSERT INTO retained_metadata_replays VALUES (?,?,?,?,?,?,?)',
                    (source.parser_repair_prior_capture_fingerprint, key, head['parquet_path'],
                     head['body_sha256'], 'current_metadata_complete', None, CAPTURED))
        # This is a negative I/O assertion: unchanged proven heads must not
        # open their captures merely because the cohort exceeds the bound.
        monkeypatch.setattr(replay.pq, 'ParquetFile', lambda *_a, **_kw: pytest.fail('unexpected original read'))
        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert result['http_requests'] == result['heads_checked'] == result['batches_read'] == 0
        states = [row[0] for row in queue.db.execute('SELECT state FROM retained_metadata_replays '
                  'WHERE source_fingerprint=?', (source.data_fingerprint,))]
        assert states == ['current_metadata_complete'] * len(keys)


@pytest.mark.parametrize('old_scan_complete', [False, True])
def test_pre_upgrade_incremental_cursor_is_reconciled_once(cfg, tmp_path, monkeypatch, old_scan_complete):
    import downloader.news_fulltext_metadata_replay as replay

    monkeypatch.setattr(replay, 'METADATA_INITIAL_COHORT_LIMIT', 1)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        keys = [captured(queue, number, incomplete=False)[0] for number in (331, 332, 333)]
        source = queue.registry.sources['cnyes']
        fp = source.data_fingerprint
        heads = {key: current(queue, key) for key in keys}
        with queue.db:
            for key in keys:
                head = heads[key]
                queue.db.execute('INSERT INTO retained_metadata_replays VALUES (?,?,?,?,?,?,?)',
                    (source.parser_repair_prior_capture_fingerprint, key, head['parquet_path'],
                     head['body_sha256'], 'current_metadata_complete', None, CAPTURED))
        _ensure_cohort(queue, source)
        # Model the previous implementation: it registered heads but did not
        # pass the prior policy to its incremental scan.
        for _ in range(10 if old_scan_complete else 1):
            advance_metadata_index_scan(queue.db, fp, now=CAPTURED, scan_rows=1, dirty_rows=1)
            if queue.db.execute('SELECT state FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                                (fp,)).fetchone()[0] == 'complete':
                break
        with queue.db:
            queue.db.execute('DELETE FROM queue_settings WHERE key=?',
                             ('retained_mobile_chart_incremental_reuse_v1:' + fp,))
        _ensure_cohort(queue, source)
        scan = queue.db.execute('SELECT state,cursor_state,cursor_rowid FROM retained_metadata_incremental_scans '
                                'WHERE source_fingerprint=?', (fp,)).fetchone()
        assert tuple(scan) == ('scanning', None, 0)
        advance_metadata_index_scan(queue.db, fp, now=CAPTURED, scan_rows=1, dirty_rows=1,
                                    prior_fingerprint=source.parser_repair_prior_capture_fingerprint)
        before = tuple(queue.db.execute('SELECT state,cursor_state,cursor_rowid FROM retained_metadata_incremental_scans '
                                        'WHERE source_fingerprint=?', (fp,)).fetchone())
        _ensure_cohort(queue, source)
        assert tuple(queue.db.execute('SELECT state,cursor_state,cursor_rowid FROM retained_metadata_incremental_scans '
                                      'WHERE source_fingerprint=?', (fp,)).fetchone()) == before
        for _ in range(10):
            advance_metadata_index_scan(queue.db, fp, now=CAPTURED, scan_rows=1, dirty_rows=1,
                                        prior_fingerprint=source.parser_repair_prior_capture_fingerprint)
            if queue.db.execute('SELECT state FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                                (fp,)).fetchone()[0] == 'complete':
                break
        assert queue.db.execute('SELECT state FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                                (fp,)).fetchone()[0] == 'complete'
        assert [row[0] for row in queue.db.execute('SELECT state FROM retained_metadata_replays '
                'WHERE source_fingerprint=?', (fp,))] == ['current_metadata_complete'] * len(keys)
        assert {key: current(queue, key) for key in keys} == heads


def test_initial_cohort_bound_counts_urls_without_saved_bodies(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_replay as replay

    monkeypatch.setattr(replay, 'METADATA_INITIAL_COHORT_LIMIT', 2)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        queue.add_articles([{'url': 'https://m.cnyes.com/news/id/' + str(n)} for n in (212, 213, 214)])
        source = queue.registry.sources['cnyes']
        _ensure_cohort(queue, source)
        marker = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                  ('cnyes_mobile_metadata_cohort_v1:' + source.data_fingerprint,)).fetchone()[0]
        assert json.loads(marker)['initial_index_mode'] == 'incremental_scan'
        assert queue.db.execute('SELECT state FROM retained_metadata_incremental_scans').fetchone()[0] == 'scanning'
        assert advance_metadata_index_scan(queue.db, source.data_fingerprint,
                                           now=CAPTURED, scan_rows=2, dirty_rows=1)['rows_scanned'] == 2
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_replays').fetchone()[0] == 0


def test_owner_snapshot_reports_index_scope_without_source_identifiers(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        assert queue.status(publish=False)['cnyes_metadata_index_progress'] is None
        captured(queue, 114)
        source = queue.registry.sources['cnyes']
        _ensure_cohort(queue, source)
        with queue.db:
            queue.db.execute('DELETE FROM retained_metadata_incremental_scans')
            queue.db.execute('DELETE FROM retained_metadata_incremental_current')
        _ensure_cohort(queue, source)
        advance_metadata_index_scan(queue.db, source.data_fingerprint, now=CAPTURED, scan_rows=1, dirty_rows=1)
        result = queue.status(publish=False)['cnyes_metadata_index_progress']
        assert result == {'contract_version': 1, 'current_source_contract_only': True,
                          'counts_are_article_completion': False, 'state': 'scanning',
                          'rows_seen': 1, 'registered_heads': 0, 'dirty_processed': 0,
                          'cursor_resets': 0, 'invalid_heads_observed': 0, 'updated_at_utc': CAPTURED}
        assert source.data_fingerprint not in json.dumps(result)


def test_baseline_slice_bounds_rows_without_bodies_and_reaches_later_head(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        urls = ['https://m.cnyes.com/news/id/' + str(n) for n in range(200, 210)]
        queue.add_articles([{'url': url} for url in urls])
        key, _ = captured(queue, 210, incomplete=False)
        source = queue.registry.sources['cnyes']
        _ensure_cohort(queue, source)
        with queue.db:
            queue.db.execute('DELETE FROM retained_metadata_replays')
            queue.db.execute('DELETE FROM retained_metadata_incremental_scans')
            queue.db.execute('DELETE FROM retained_metadata_incremental_current')
            queue.db.execute("UPDATE articles SET state='same' WHERE host='m.cnyes.com'")
        _ensure_cohort(queue, source)
        observations = []
        for _ in range(10):
            result = advance_metadata_index_scan(queue.db, source.data_fingerprint,
                                                 now=CAPTURED, scan_rows=2, dirty_rows=1)
            observations.append(result['rows_scanned'])
            assert result['rows_scanned'] <= 2
            if queue.db.execute('SELECT state FROM retained_metadata_incremental_scans').fetchone()[0] == 'complete':
                break
        assert observations == [2, 2, 2, 2, 2, 1]
        assert checkpoint(queue, key)['original_path'] == current(queue, key)['parquet_path']
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_replays').fetchone()[0] == 1
        assert verify_bodies(tmp_path)['accepted']


def test_missing_source_path_is_visible_and_cleared_by_later_acceptance(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        key, record = captured(queue, 211, incomplete=False)
        with queue.db:
            queue.db.execute('UPDATE articles SET parquet_path=NULL WHERE id=?', (key,))
        source = queue.registry.sources['cnyes']
        _ensure_cohort(queue, source)
        assert queue.db.execute('SELECT state FROM retained_metadata_incremental_scans').fetchone()[0] == 'scanning'
        for _ in range(3):
            advance_metadata_index_scan(queue.db, source.data_fingerprint, now=CAPTURED, scan_rows=1, dirty_rows=1)
            if queue.db.execute('SELECT state FROM retained_metadata_incremental_scans').fetchone()[0] == 'complete':
                break
        invalid = queue.db.execute('SELECT article_id,reason,body_sha256 FROM retained_metadata_incremental_invalid').fetchone()
        assert tuple(invalid) == (key, 'missing_parquet_path', record['body_sha256'])
        assert queue.status(publish=False)['cnyes_metadata_index_progress']['invalid_heads_observed'] == 1
        queue.accept([record])
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_incremental_invalid').fetchone()[0] == 0
        assert queue.status(publish=False)['cnyes_metadata_index_progress']['invalid_heads_observed'] == 0
        assert verify_bodies(tmp_path)['accepted']


def test_switching_back_to_a_source_version_rescans_its_inactive_interval(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        source = queue.registry.sources['cnyes']
        captured(queue, 115, incomplete=False)
        _ensure_cohort(queue, source)
        with queue.db:
            begin_metadata_index_scan(queue.db, 'other-source-version', initial_cohort_seeded=True, now=CAPTURED)
        new_key, _ = captured(queue, 116, incomplete=False)
        assert queue.db.execute('SELECT 1 FROM retained_metadata_replays WHERE article_id=?', (new_key,)).fetchone() is None
        with queue.db:
            begin_metadata_index_scan(queue.db, source.data_fingerprint, initial_cohort_seeded=False, now=CAPTURED)
        scan = queue.db.execute('SELECT state,cursor_resets FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                                (source.data_fingerprint,)).fetchone()
        assert tuple(scan) == ('scanning', 1)
        for _ in range(5):
            advance_metadata_index_scan(queue.db, source.data_fingerprint, now=CAPTURED, scan_rows=1, dirty_rows=1)
            if queue.db.execute('SELECT state FROM retained_metadata_incremental_scans WHERE source_fingerprint=?',
                                (source.data_fingerprint,)).fetchone()[0] == 'complete':
                break
        assert checkpoint(queue, new_key)['original_path'] == current(queue, new_key)['parquet_path']


def test_idle_owner_advances_legacy_scan_without_any_publisher_request(cfg, tmp_path):
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cfg.update(cached_cnyes_metadata_replay_enabled=True, cached_cnyes_replay_enabled=False,
               retained_cnyes_failure_replay_enabled=False, adaptive_rate={'enabled': False},
               service_idle_max_seconds=30)
    http, session = http_fixture(cfg, tmp_path, {})
    with closing(NewsQueue(tmp_path, http.registry)) as queue:
        try:
            captured(queue, 117, incomplete=False)
            source = queue.registry.sources['cnyes']
            _ensure_cohort(queue, source)
            assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['heads_checked'] == 1
            new_key, _ = captured(queue, 118, incomplete=False)
            with queue.db:
                queue.db.execute('DELETE FROM retained_metadata_incremental_scans')
                queue.db.execute('DELETE FROM retained_metadata_incremental_current')
            _ensure_cohort(queue, source)
            assert retained_metadata_next_wake_delay(queue, 60) == 1
            http.defer('cnyes', 120)
            run_service(queue, http, cfg, max_articles=0, max_discovery_requests=0, duration_seconds=4)
            assert queue.db.execute('SELECT state FROM retained_metadata_incremental_scans').fetchone()[0] == 'complete'
            assert checkpoint(queue, new_key)['state'] == 'current_metadata_complete'
            assert not session.calls and verify_bodies(tmp_path)['accepted']
        finally:
            http.close()
