"""Cooperative original-batch ingestion, exact associations and crash recovery."""
from contextlib import closing
import json
import sqlite3
import threading

import pyarrow as pa
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.news_fulltext import run_service
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config as config, http_fixture, metadata_receipt


def queue_at(tmp_path, config, name='queue'):
    return NewsQueue(tmp_path / name, PolicyRegistry(config, base=tmp_path))


def one_batch_clock(queue, monkeypatch):
    clock = [0.0]
    original = queue.add_articles
    def add(rows, **kwargs):
        result = original(rows, **kwargs)
        clock[0] += 1.0  # One logical batch exceeds the cooperative commit window.
        return result
    monkeypatch.setattr(queue, 'add_articles', add)
    monkeypatch.setattr('downloader.news_fulltext_metadata_ingest.time.monotonic', lambda: clock[0])
    return clock


def rows(count):
    return [{'url': f'https://publisher.test/article/{i}', 'title': f'title{i}', 'date': '2026-10-01'}
            for i in range(count)]


def semantic_tables(queue):
    return {name: [tuple(r) for r in queue.db.execute(f'SELECT * FROM {name} ORDER BY 1,2')]
            for name in ('articles', 'origins', 'source_links', 'hosts', 'receipt_heads', 'origin_errors')}


@pytest.mark.parametrize('count', [0, 1, 4096, 4097, 9234])
def test_slices_keep_full_receipt_output_and_logical_batch_boundaries(config, tmp_path, monkeypatch, count):
    values = rows(count)
    if count > 4096:
        # The first duplicate wins WITHIN one original4k batch, but the next
        # logical batch can update the title. Cutting at arbitrary smaller
        # row groups would silently change this original behavior.
        values[10] = dict(values[0], title='ignored within first logical batch')
        values[4096] = dict(values[0], title='next logical batch title', date='2026-10-02')
    receipt = metadata_receipt(tmp_path, values)
    if not values:
        # A zero-row receipt must still declare its original link schema.
        atomic_write_parquet(tmp_path / 'metadata.parquet', pa.table({'url': pa.array([], type=pa.string())}))
        atomic_write_json(receipt, {'parquet_path': 'metadata.parquet', 'rows': 0,
                                   'sha256': sha256_file(tmp_path / 'metadata.parquet')})
    monkeypatch.setattr('downloader.news_fulltext_queue.utc_now', lambda: '2026-10-14T00:00:00+00:00')
    with closing(queue_at(tmp_path, config, 'reference')) as reference:
        assert reference.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        expected = semantic_tables(reference)
    with closing(queue_at(tmp_path, config, 'sliced')) as queue:
        one_batch_clock(queue, monkeypatch)
        completed, calls = False, 0
        while not completed:
            calls += 1
            completed = queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
            assert calls < 6
            if not completed:
                assert queue.metadata_receipt_pending
                assert not queue.db.execute('SELECT 1 FROM receipt_heads').fetchone()
                status = queue.status()
                assert status['metadata_association_rows'] == 0
                assert status['metadata_inventory']['partial_partitions'] == 1
        assert semantic_tables(queue) == expected
        assert not queue.db.execute('SELECT 1 FROM metadata_ingest_progress').fetchone()
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')


def test_durable_cursor_resumes_after_owner_restart_without_duplicate_associations(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(8300))
    queue = queue_at(tmp_path, config)
    one_batch_clock(queue, monkeypatch)
    assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
    assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 4096
    queue.close()
    with closing(queue_at(tmp_path, config)) as restarted:
        assert restarted.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert restarted.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 8300
        assert restarted.db.execute('SELECT rows FROM origins').fetchone()[0] == 8300
        assert not restarted.db.execute('SELECT 1 FROM metadata_ingest_progress').fetchone()


def test_stop_yields_without_complete_head_or_lost_cursor(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(4500))
    stop = threading.Event()
    with closing(queue_at(tmp_path, config)) as queue:
        original = queue.add_articles
        def add(values, **kwargs):
            result = original(values, **kwargs)
            stop.set()
            return result
        monkeypatch.setattr(queue, 'add_articles', add)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', stop=stop)
        assert queue.metadata_receipt_pending
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 4096
        assert not queue.db.execute('SELECT 1 FROM receipt_heads').fetchone()
        stop.clear()
        monkeypatch.setattr(queue, 'add_articles', original)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', stop=stop)
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4500


def test_already_stopped_owner_does_not_create_partial_origin(config, tmp_path):
    stop = threading.Event()
    stop.set()
    receipt = metadata_receipt(tmp_path, rows(1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', stop=stop)
        assert queue.metadata_receipt_pending
        assert not queue.db.execute('SELECT 1 FROM origins').fetchone()


def test_one_sql_failure_rolls_back_batch_cursor_and_same_owner_retry(config, tmp_path):
    receipt = metadata_receipt(tmp_path, rows(4500))
    with closing(queue_at(tmp_path, config)) as queue:
        original_cache = queue.db.execute('PRAGMA cache_size').fetchone()[0]
        queue.db.executescript("CREATE TRIGGER fail_import BEFORE INSERT ON source_links BEGIN SELECT RAISE(ABORT,'failed_batch'); END;")
        with pytest.raises(sqlite3.IntegrityError, match='failed_batch'):
            queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 0
        assert not queue.db.execute('SELECT 1 FROM articles').fetchone()
        assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == original_cache
        queue.db.execute('DROP TRIGGER fail_import')
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4500
        assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == original_cache


def test_full_checksum_is_revalidated_after_restart(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(4500))
    queue = queue_at(tmp_path, config)
    one_batch_clock(queue, monkeypatch)
    assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
    queue.close()
    proof = json.loads(receipt.read_text())
    proof['sha256'] = '0' * 64
    atomic_write_json(receipt, proof)
    with closing(queue_at(tmp_path, config)) as restarted:
        assert not restarted.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert not restarted.db.execute('SELECT 1 FROM receipt_heads').fetchone()
        assert restarted.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 4096
        assert 'metadata_sha256_mismatch' in restarted.db.execute('SELECT error_code FROM origin_errors').fetchone()[0]


def test_revised_receipt_supersedes_partial_version_without_calling_it_complete(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(4500))
    with closing(queue_at(tmp_path, config)) as queue:
        one_batch_clock(queue, monkeypatch)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
        metadata_receipt(tmp_path, rows(1))
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT state FROM metadata_ingest_progress').fetchone()[0] == 'superseded'
        status = queue.status()
        assert status['metadata_association_rows'] == 1
        assert status['metadata_partitions'] == 1 and status['metadata_partition_versions_retained'] == 2
        assert status['metadata_inventory']['partial_partitions'] == 0


def test_invalid_urls_and_existing_body_leases_are_preserved(config, tmp_path, monkeypatch):
    values = rows(4500)
    values[0]['url'], values[4097]['url'] = None, 'javascript:invalid'
    receipt = metadata_receipt(tmp_path, values)
    with closing(queue_at(tmp_path, config)) as queue:
        with queue.db:
            queue.add_articles([values[2]])
            queue.db.execute("UPDATE articles SET state='inflight',due=12345,lease_owner='other-fixture',lease_until=8888")
        one_batch_clock(queue, monkeypatch)
        while not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05):
            assert queue.metadata_receipt_pending
        row = queue.db.execute("SELECT state,due,lease_owner,lease_until FROM articles WHERE url=?", (values[2]['url'],)).fetchone()
        assert tuple(row) == ('inflight', 12345, 'other-fixture', 8888)
        assert queue.status()['invalid_metadata_urls'] == 2
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4498


def test_local_scan_resumes_same_receipt_then_other_providers(config, tmp_path, monkeypatch):
    root = tmp_path / 'finlab'
    root.mkdir()
    receipt = metadata_receipt(root, rows(4500))
    receipts = root / 'receipts'
    receipts.mkdir()
    receipt.rename(receipts / 'tw_news.json')
    with closing(queue_at(tmp_path, config)) as queue:
        one_batch_clock(queue, monkeypatch)
        result = queue.ingest_local_slice(root, tmp_path / 'finmind', maximum_wall_seconds=.05)
        assert result == {'imported': 0, 'complete': False}
        assert queue.metadata_inventory_pending
        result = queue.ingest_local_slice(root, tmp_path / 'finmind')
        assert result == {'imported': 1, 'complete': True}
        assert not queue.metadata_inventory_pending
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4500


def test_pending_inventory_keeps_idle_service_wake_bounded_and_honors_duration(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    queue = queue_at(tmp_path, config)
    calls = []
    def refresh():
        calls.append(1)
        queue.metadata_inventory_pending = True
    try:
        result = run_service(queue, http, config, max_articles=0, max_discovery_requests=0,
                             refresh_metadata=refresh, duration_seconds=1.2)
        assert len(calls) >= 2
        assert result['activity'] == 'service_duration_complete'
    finally:
        http.close()
        queue.close()


def test_metadata_cache_is_one_bounded_temporary_lease_for_two_receipts(config, tmp_path, monkeypatch):
    roots = [tmp_path / 'first', tmp_path / 'second']
    for root in roots:
        root.mkdir()
    receipts = [metadata_receipt(root, rows(4500)) for root in roots]
    with closing(queue_at(tmp_path, config)) as queue:
        original = queue.db.execute('PRAGMA cache_size').fetchone()[0]
        one_batch_clock(queue, monkeypatch)
        for root, receipt in zip(roots, receipts):
            assert not queue.ingest_receipt(root, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
            assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == -8192
        assert queue.ingest_receipt(roots[0], receipts[0], provider='finlab', link_column='url')
        assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == -8192
        assert queue.ingest_receipt(roots[1], receipts[1], provider='finlab', link_column='url')
        assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == original
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 9000


def test_explicit_larger_sqlite_cache_is_not_reduced_by_metadata_lease(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(4500))
    with closing(queue_at(tmp_path, config)) as queue:
        queue.db.execute('PRAGMA cache_size=-32768')
        one_batch_clock(queue, monkeypatch)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
        assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == -32768
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == -32768


def test_original_batches_coalesce_without_changing_dedup_boundary(config, tmp_path, monkeypatch):
    values = rows(20600)
    values[10] = dict(values[0], title='first batch duplicate must not overwrite')
    values[4096] = dict(values[0], title='next logical batch must overwrite', date='2026-10-02')
    receipt = metadata_receipt(tmp_path, values)
    with closing(queue_at(tmp_path, config)) as queue:
        clock, original, commits = [0.0], queue.add_articles, []
        def add(items, **kwargs):
            result = original(items, **kwargs)
            clock[0] += .2
            return result
        monkeypatch.setattr(queue, 'add_articles', add)
        monkeypatch.setattr('downloader.news_fulltext_metadata_ingest.time.monotonic', lambda: clock[0])
        queue.db.set_trace_callback(lambda sql: commits.append(sql) if sql == 'COMMIT' else None)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 16384
        assert len(commits) == 2  # Initial origin, then four logical batches in ONE commit.
        assert queue.db.execute('SELECT title FROM articles WHERE url=?', (values[0]['url'],)).fetchone()[0] == values[4096]['title']
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 20600


def test_group_rollback_discards_every_uncommitted_logical_batch(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(20600))
    with closing(queue_at(tmp_path, config)) as queue:
        clock, original = [0.0], queue.add_articles
        def add(items, **kwargs):
            result = original(items, **kwargs)
            clock[0] += .2
            return result
        monkeypatch.setattr(queue, 'add_articles', add)
        monkeypatch.setattr('downloader.news_fulltext_metadata_ingest.time.monotonic', lambda: clock[0])
        queue.db.executescript("CREATE TRIGGER fail_third_batch BEFORE INSERT ON articles "
                              "WHEN new.url='https://publisher.test/article/8192' BEGIN SELECT RAISE(ABORT,'third_batch'); END;")
        original_cache = queue.db.execute('PRAGMA cache_size').fetchone()[0]
        with pytest.raises(sqlite3.IntegrityError, match='third_batch'):
            queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 0
        assert not queue.db.execute('SELECT 1 FROM articles').fetchone()
        assert not queue.db.execute('SELECT 1 FROM source_links').fetchone()
        assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == original_cache
        queue.db.execute('DROP TRIGGER fail_third_batch')
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 20600


def test_stop_between_logical_batches_commits_only_exact_processed_prefix(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(20600))
    stop = threading.Event()
    with closing(queue_at(tmp_path, config)) as queue:
        clock, calls, original = [0.0], [0], queue.add_articles
        def add(items, **kwargs):
            result = original(items, **kwargs)
            calls[0] += 1
            clock[0] += .2
            if calls[0] == 2:
                stop.set()
            return result
        monkeypatch.setattr(queue, 'add_articles', add)
        monkeypatch.setattr('downloader.news_fulltext_metadata_ingest.time.monotonic', lambda: clock[0])
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', stop=stop)
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 8192
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 8192
        assert not queue.db.execute('SELECT 1 FROM receipt_heads').fetchone()
        stop.clear()
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', stop=stop)
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 20600
