"""One bad retained metadata head must not poison a verified batch peer."""

from contextlib import closing
import json
import sqlite3

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_metadata_replay import replay_retained_cnyes_metadata
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_cnyes_news_bulk import cfg
from test_news_retained_metadata_checkpoint_batch import native_heads


@pytest.mark.parametrize('bad_position', [0, 1, 3])
def test_mismatched_one_head_does_not_block_repairs_in_same_batch(cfg, tmp_path, bad_position):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        heads = native_heads(queue, 4, repair_count=4)
        bad = sorted(heads)[bad_position]
        old_path = heads[bad]['parquet_path']
        old_sha = sha256_file(tmp_path / old_path)
        with queue.db:
            # Model a bad current head independently of a valid, unchanged
            # receipt and source file; the peer heads remain verifiable.
            queue.db.execute('UPDATE articles SET body_sha256=? WHERE id=?', ('f' * 64, bad))
            queue.db.execute('UPDATE retained_metadata_replays SET original_body_sha256=? WHERE article_id=?',
                             ('f' * 64, bad))
        before_bad = dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (bad,)).fetchone())

        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)

        assert result['http_requests'] == 0
        assert result['heads_checked'] == 4 and result['repaired_versions'] == 3
        rows = {row['article_id']: (row['state'], row['error_code']) for row in queue.db.execute(
            'SELECT article_id,state,error_code FROM retained_metadata_replays')}
        assert rows[bad] == ('failed_validation', 'retained_metadata_batch_head_not_unique')
        assert all(rows[key] == ('verified_repair', None) for key in heads if key != bad)
        assert dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (bad,)).fetchone()) == before_bad
        assert sha256_file(tmp_path / old_path) == old_sha
        for key in heads:
            if key == bad:
                continue
            current = queue.db.execute('SELECT * FROM articles WHERE id=?', (key,)).fetchone()
            assert current['parquet_path'] != old_path
            stored = pq.read_table(tmp_path / current['parquet_path']).to_pylist()
            record = next(row for row in stored if row['article_id'] == key.hex())
            assert record['body_sha256'] == current['body_sha256']
            assert current['captured_at_utc'] == heads[key]['captured_at_utc']
            assert current['last_checked_at_utc'] == heads[key]['last_checked_at_utc']


def test_malformed_metadata_isolated_from_valid_peers(cfg, tmp_path, monkeypatch):
    import test_news_retained_metadata_checkpoint_batch as fixture

    original_fetch = fixture.fetch_article

    def malformed_one(http, head):
        record = original_fetch(http, head)
        if head['url'].endswith('/100002'):
            record['authors_json'] = '{'
        return record

    monkeypatch.setattr(fixture, 'fetch_article', malformed_one)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        heads = native_heads(queue, 4, repair_count=2)
        bad = next(key for key, head in heads.items() if head['url'].endswith('/100002'))
        before_bad = dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (bad,)).fetchone())

        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)

        assert result['http_requests'] == 0
        assert result['heads_checked'] == 4 and result['repaired_versions'] == 2
        rows = {row['article_id']: (row['state'], row['error_code']) for row in queue.db.execute(
            'SELECT article_id,state,error_code FROM retained_metadata_replays')}
        assert rows[bad][0] == 'failed_validation'
        assert rows[bad][1].startswith('Expecting property name')
        assert sorted(state for key, (state, _) in rows.items() if key != bad) == [
            'current_metadata_complete', 'verified_repair', 'verified_repair']
        assert dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (bad,)).fetchone()) == before_bad
        assert verify_bodies(tmp_path)['accepted']


def test_upgrade_reconsiders_only_old_batch_mismatch_once_after_restart(cfg, tmp_path):
    from downloader.news_fulltext_metadata_replay import _ensure_cohort

    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        heads = native_heads(queue, 5, repair_count=5)
        source = registry.sources['cnyes']
        bad, *peers, unrelated = sorted(heads)
        old_path = heads[bad]['parquet_path']
        old_sha = sha256_file(tmp_path / old_path)
        marker = 'retained_mobile_head_isolation_v1:' + source.data_fingerprint
        with queue.db:
            queue.db.execute('UPDATE articles SET body_sha256=? WHERE id=?', ('f' * 64, bad))
            queue.db.execute('UPDATE retained_metadata_replays SET original_body_sha256=? WHERE article_id=?',
                             ('f' * 64, bad))
            for key in [bad, *peers]:
                queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation',"
                                 "error_code='retained_metadata_batch_head_not_unique',checked_at_utc='old-check' "
                                 'WHERE article_id=?', (key,))
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation',"
                             "error_code='article_identity_not_verified',checked_at_utc='old-check' "
                             'WHERE article_id=?', (unrelated,))
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (marker,))
        before = {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        _ensure_cohort(queue, source)
        archived = queue.db.execute('SELECT article_id,state,error_code,checked_at_utc '
            "FROM retained_metadata_checkpoint_history WHERE reason='batch_head_isolation_upgrade'").fetchall()
        assert {row['article_id'] for row in archived} == {bad, *peers}
        assert all(tuple(row)[1:] == ('failed_validation', 'retained_metadata_batch_head_not_unique', 'old-check')
                   for row in archived)
        note = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0])
        assert note['reconsidered'] == 4 and note['http_requests'] == 0
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == before

    with closing(NewsQueue(tmp_path, registry)) as queue:
        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert result['http_requests'] == 0
        assert result['heads_checked'] == 4 and result['repaired_versions'] == 3
        _ensure_cohort(queue, source)
        states = {row['article_id']: (row['state'], row['error_code']) for row in queue.db.execute(
            'SELECT article_id,state,error_code FROM retained_metadata_replays')}
        assert states[bad] == ('failed_validation', 'retained_metadata_batch_head_not_unique')
        assert states[unrelated] == ('failed_validation', 'article_identity_not_verified')
        assert all(states[key] == ('verified_repair', None) for key in peers)
        assert queue.db.execute("SELECT count(*) FROM retained_metadata_checkpoint_history "
                                "WHERE reason='batch_head_isolation_upgrade'").fetchone()[0] == 4
        assert sha256_file(tmp_path / old_path) == old_sha


def test_upgrade_checkpoint_and_archive_are_atomic_on_failure(cfg, tmp_path):
    from downloader.news_fulltext_metadata_replay import _ensure_cohort

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        heads = native_heads(queue, 1)
        source = queue.registry.sources['cnyes']
        marker = 'retained_mobile_head_isolation_v1:' + source.data_fingerprint
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation',"
                             "error_code='retained_metadata_batch_head_not_unique',checked_at_utc='old-check'")
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (marker,))
            queue.db.execute("CREATE TRIGGER injected_archive_failure BEFORE INSERT ON "
                             "retained_metadata_checkpoint_history BEGIN "
                             "SELECT RAISE(ABORT,'injected archive failure'); END")
        before = {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        with pytest.raises(sqlite3.IntegrityError, match='injected archive failure'):
            _ensure_cohort(queue, source)
        assert queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone() is None
        assert tuple(queue.db.execute('SELECT state,error_code,checked_at_utc FROM retained_metadata_replays').fetchone()) == (
            'failed_validation', 'retained_metadata_batch_head_not_unique', 'old-check')
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_checkpoint_history').fetchone()[0] == 0
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == before == heads
        with queue.db:
            queue.db.execute('DROP TRIGGER injected_archive_failure')
        _ensure_cohort(queue, source)
        assert tuple(queue.db.execute('SELECT state,error_code,checked_at_utc FROM retained_metadata_replays').fetchone()) == (
            'pending', None, None)
        assert queue.db.execute("SELECT count(*) FROM retained_metadata_checkpoint_history "
                                "WHERE reason='batch_head_isolation_upgrade'").fetchone()[0] == 1
