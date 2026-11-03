"""Cooperative original-batch ingestion, exact associations and crash recovery."""
from contextlib import closing
import hashlib
import json
import os
import sqlite3
import threading
import time
import zlib

import pyarrow as pa
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.news_fulltext import _coalesce_inventory_heartbeats, run_service
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, config as config, http_fixture, metadata_receipt


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
        assert not getattr(queue, 'metadata_inventory_made_progress', False)
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


def test_same_size_and_mtime_replacement_imports_new_completed_receipt(config, tmp_path):
    receipt = metadata_receipt(tmp_path, rows(1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        old = receipt.stat()
        receipt = metadata_receipt(tmp_path, [rows(2)[1]])
        assert receipt.stat().st_size == old.st_size
        os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
        assert receipt.stat().st_mtime_ns == old.st_mtime_ns
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')


def test_in_place_receipt_change_with_restored_mtime_is_not_skipped(config, tmp_path):
    receipt = metadata_receipt(tmp_path, rows(1))
    original = receipt.read_text()
    proof = json.loads(original)
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        old = receipt.stat()
        receipt.write_text(original.replace(proof['sha256'], '0' * 64))
        os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
        assert receipt.stat().st_ino == old.st_ino and receipt.stat().st_size == old.st_size
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert 'metadata_sha256_mismatch' in queue.db.execute('SELECT error_code FROM origin_errors').fetchone()[0]
        receipt.write_text(original)
        os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert not queue.db.execute('SELECT 1 FROM origin_errors').fetchone()
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1


def test_receipt_scope_root_retarget_is_not_skipped(config, tmp_path):
    first, second = tmp_path / 'first', tmp_path / 'second'
    first.mkdir()
    second.mkdir()
    original_receipt = metadata_receipt(first, rows(1))
    receipt = tmp_path / 'outside.json'
    receipt.write_text(original_receipt.read_text())
    root = tmp_path / 'source_root'
    root.symlink_to(first, target_is_directory=True)
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_receipt(root, receipt, provider='finlab', link_column='url')
        root.unlink()
        root.symlink_to(second, target_is_directory=True)
        assert not queue.ingest_receipt(root, receipt, provider='finlab', link_column='url')
        assert 'metadata_receipt_scope_mismatch' in queue.db.execute('SELECT error_code FROM origin_errors').fetchone()[0]
        root.unlink()
        root.symlink_to(first, target_is_directory=True)
        assert not queue.ingest_receipt(root, receipt, provider='finlab', link_column='url')
        assert not queue.db.execute('SELECT 1 FROM origin_errors').fetchone()
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1


def test_same_size_and_mtime_replacement_revalidates_inflight_receipt(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(4500))
    with closing(queue_at(tmp_path, config)) as queue:
        one_batch_clock(queue, monkeypatch)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 4096
        old = receipt.stat()
        proof = json.loads(receipt.read_text())
        proof['sha256'] = '0' * 64
        atomic_write_json(receipt, proof)
        assert receipt.stat().st_size == old.st_size
        os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert not queue.db.execute('SELECT 1 FROM receipt_heads').fetchone()
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 4096
        assert 'metadata_sha256_mismatch' in queue.db.execute('SELECT error_code FROM origin_errors').fetchone()[0]


def test_receipt_replaced_during_batch_rolls_back_and_resumes(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(2))
    proof = json.loads(receipt.read_text())
    old = receipt.stat()
    with closing(queue_at(tmp_path, config)) as queue:
        original = queue.add_articles
        def replace_during_batch(batch, **kwargs):
            result = original(batch, **kwargs)
            atomic_write_json(receipt, dict(proof, sha256='0' * 64))
            os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
            return result
        monkeypatch.setattr(queue, 'add_articles', replace_during_batch)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.metadata_receipt_pending
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 0
        assert not queue.db.execute('SELECT 1 FROM receipt_heads').fetchone()
        monkeypatch.setattr(queue, 'add_articles', original)
        atomic_write_json(receipt, proof)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2


def test_legacy_completed_receipt_is_revalidated_without_reimport_or_write(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    with closing(queue_at(tmp_path, config)) as restarted:
        monkeypatch.setattr(restarted, 'add_articles', lambda *_args, **_kwargs: pytest.fail('reimported completed receipt'))
        before = restarted.db.total_changes
        assert not restarted.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert restarted.db.total_changes == before
        assert restarted.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 1


def test_legacy_completed_receipt_replacement_is_detected_after_restart(config, tmp_path):
    receipt = metadata_receipt(tmp_path, rows(1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    old = receipt.stat()
    receipt = metadata_receipt(tmp_path, [rows(2)[1]])
    assert receipt.stat().st_size == old.st_size
    os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
    with closing(queue_at(tmp_path, config)) as restarted:
        assert restarted.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert restarted.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert restarted.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2


def _versioned_metadata_receipt(root, name, values):
    directory = root / name
    directory.mkdir()
    original = metadata_receipt(directory, values)
    proof = json.loads(original.read_text())
    proof['parquet_path'] = f'{name}/metadata.parquet'
    return proof


def test_completed_receipt_reversion_selects_existing_version_in_status_and_dataset(config, tmp_path):
    from stockagent.data.news_dataset_sources import freeze_inputs
    first = _versioned_metadata_receipt(tmp_path, 'first', rows(1))
    second = _versioned_metadata_receipt(tmp_path, 'second', rows(2))
    receipt = tmp_path / 'source.json'
    with closing(queue_at(tmp_path, config)) as queue:
        atomic_write_json(receipt, first)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        atomic_write_json(receipt, second)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.status()['metadata_association_rows'] == 2
        atomic_write_json(receipt, first)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.status()['metadata_association_rows'] == 1
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2
        frozen = tmp_path / 'frozen'
        freeze_inputs(tmp_path / 'queue', frozen)
        with closing(sqlite3.connect(frozen / 'snapshot.sqlite3')) as snapshot:
            assert snapshot.execute('SELECT sha256,rows FROM metadata_inputs').fetchall() == [(first['sha256'], 1)]
    with closing(queue_at(tmp_path, config)) as restarted:
        assert restarted.status()['metadata_association_rows'] == 1
        before = restarted.db.total_changes
        assert not restarted.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert restarted.db.total_changes == before


def test_completed_receipt_reversion_supersedes_partial_and_resumes_if_returned(config, tmp_path, monkeypatch):
    first = _versioned_metadata_receipt(tmp_path, 'first', rows(1))
    second = _versioned_metadata_receipt(tmp_path, 'second', rows(4500))
    receipt = tmp_path / 'source.json'
    with closing(queue_at(tmp_path, config)) as queue:
        atomic_write_json(receipt, first)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        one_batch_clock(queue, monkeypatch)
        atomic_write_json(receipt, second)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 4096
        atomic_write_json(receipt, first)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.status()['metadata_inventory']['partial_partitions'] == 0
        assert queue.db.execute('SELECT state FROM metadata_ingest_progress').fetchone()[0] == 'superseded'
        atomic_write_json(receipt, second)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.status()['metadata_association_rows'] == 4500
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4501


def test_legacy_inferred_receipt_reversion_becomes_explicit_without_reimport(config, tmp_path, monkeypatch):
    first = _versioned_metadata_receipt(tmp_path, 'first', rows(1))
    second = _versioned_metadata_receipt(tmp_path, 'second', rows(2))
    receipt = tmp_path / 'source.json'
    with closing(queue_at(tmp_path, config)) as queue:
        for proof in (first, second):
            atomic_write_json(receipt, proof)
            assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        with queue.db:
            queue.db.execute('DELETE FROM metadata_current_origins')
        atomic_write_json(receipt, first)
    with closing(queue_at(tmp_path, config)) as queue:
        before = queue.status()
        assert before['metadata_association_rows'] == 2
        assert before['metadata_inventory']['legacy_inferred_origins'] == 1
        monkeypatch.setattr(queue, 'add_articles', lambda *_a, **_kw: pytest.fail('completed origin was reimported'))
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        after = queue.status()
        assert after['metadata_association_rows'] == 1
        assert after['metadata_inventory']['explicit_current_origins'] == 1
        assert after['metadata_inventory']['legacy_inferred_origins'] == 0
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2


def test_legacy_explicit_complete_cursor_does_not_reimport(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        origin_id = queue.db.execute('SELECT id FROM origins').fetchone()[0]
        with queue.db:
            queue.db.execute("INSERT INTO metadata_ingest_progress VALUES (?,?,?,?,'complete')",
                             (origin_id, 1, 1, 'url'))
    with closing(queue_at(tmp_path, config)) as queue:
        before = queue.db.total_changes
        monkeypatch.setattr(queue, 'add_articles', lambda *_a, **_kw: pytest.fail('completed origin was reimported'))
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.total_changes == before
        assert queue.status()['metadata_association_rows'] == 1


def test_new_partial_does_not_supersede_legacy_complete_cursor(config, tmp_path, monkeypatch):
    first = _versioned_metadata_receipt(tmp_path, 'first', rows(1))
    second = _versioned_metadata_receipt(tmp_path, 'second', rows(4500))
    receipt = tmp_path / 'source.json'
    with closing(queue_at(tmp_path, config)) as queue:
        atomic_write_json(receipt, first)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        first_id = queue.db.execute('SELECT id FROM origins').fetchone()[0]
        with queue.db:
            queue.db.execute("INSERT INTO metadata_ingest_progress VALUES (?,?,?,?,'complete')",
                             (first_id, 1, 1, 'url'))
        one_batch_clock(queue, monkeypatch)
        atomic_write_json(receipt, second)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
        assert queue.db.execute('SELECT state FROM metadata_ingest_progress WHERE origin_id=?',
                                (first_id,)).fetchone()[0] == 'complete'
        assert queue.status()['metadata_association_rows'] == 1
        assert queue.status()['metadata_inventory']['partial_partitions'] == 1


@pytest.mark.parametrize('corruption', ['missing', 'different_receipt', 'partial'])
def test_invalid_current_metadata_pointer_is_rejected_by_status_and_snapshot(config, tmp_path, corruption):
    from stockagent.data.news_dataset_sources import freeze_inputs
    first = _versioned_metadata_receipt(tmp_path, 'first', rows(1))
    second = _versioned_metadata_receipt(tmp_path, 'second', rows(2))
    receipt, other = tmp_path / 'source.json', tmp_path / 'other.json'
    with closing(queue_at(tmp_path, config)) as queue:
        atomic_write_json(receipt, first)
        atomic_write_json(other, second)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.ingest_receipt(tmp_path, other, provider='finlab', link_column='url')
        with queue.db:
            if corruption == 'partial':
                origin_id = queue.db.execute('SELECT origin_id FROM metadata_current_origins WHERE receipt_path=?',
                                             (str(receipt),)).fetchone()[0]
                queue.db.execute("INSERT INTO metadata_ingest_progress VALUES (?,?,?,?,'pending')",
                                 (origin_id, 1, 0, 'url'))
            else:
                origin_id = (999 if corruption == 'missing' else queue.db.execute(
                    'SELECT origin_id FROM metadata_current_origins WHERE receipt_path=?', (str(other),)).fetchone()[0])
                queue.db.execute('UPDATE metadata_current_origins SET origin_id=? WHERE receipt_path=?',
                                 (origin_id, str(receipt)))
        with pytest.raises(ValueError, match='invalid_current_metadata_origin'):
            queue.status()
        frozen = tmp_path / 'frozen'
        with pytest.raises(ValueError, match='invalid_current_metadata_origin'):
            freeze_inputs(tmp_path / 'queue', frozen)
        assert not (frozen / 'snapshot.json').exists()


def test_receipt_change_during_current_selection_rolls_back(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_metadata_ingest as ingest
    first = _versioned_metadata_receipt(tmp_path, 'first', rows(1))
    second = _versioned_metadata_receipt(tmp_path, 'second', rows(2))
    receipt = tmp_path / 'source.json'
    with closing(queue_at(tmp_path, config)) as queue:
        for proof in (first, second):
            atomic_write_json(receipt, proof)
            assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        old_head = tuple(queue.db.execute('SELECT * FROM receipt_heads').fetchone())
        old_selection = tuple(queue.db.execute('SELECT * FROM metadata_current_origins').fetchone())
        original = ingest._publish_current_origin
        def changed_source(*args):
            original(*args)
            atomic_write_json(receipt, second)
        monkeypatch.setattr(ingest, '_publish_current_origin', changed_source)
        atomic_write_json(receipt, first)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.metadata_receipt_pending
        assert tuple(queue.db.execute('SELECT * FROM receipt_heads').fetchone()) == old_head
        assert tuple(queue.db.execute('SELECT * FROM metadata_current_origins').fetchone()) == old_selection


def test_changed_completed_receipt_scope_records_error_and_clears_after_recovery(config, tmp_path):
    receipt = metadata_receipt(tmp_path, rows(1))
    proof = json.loads(receipt.read_text())
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        old = receipt.stat()
        atomic_write_json(receipt, dict(proof, rows=2))
        assert receipt.stat().st_size == old.st_size
        os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert 'metadata_completed_receipt_scope_mismatch' in queue.db.execute(
            'SELECT error_code FROM origin_errors').fetchone()[0]
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1
        atomic_write_json(receipt, proof)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert not queue.db.execute('SELECT 1 FROM origin_errors').fetchone()
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 1


def test_replacement_during_checksum_yields_before_creating_origin(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_ingest as ingest
    receipt = metadata_receipt(tmp_path, rows(1))
    proof = json.loads(receipt.read_text())
    old = receipt.stat()
    original = ingest.sha256_file
    def replace_during_checksum(path):
        digest = original(path)
        atomic_write_json(receipt, dict(proof, sha256='0' * 64))
        os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
        return digest
    monkeypatch.setattr(ingest, 'sha256_file', replace_during_checksum)
    with closing(queue_at(tmp_path, config)) as queue:
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.metadata_receipt_pending
        assert not queue.db.execute('SELECT 1 FROM origins').fetchone()
        assert not queue.db.execute('SELECT 1 FROM receipt_heads').fetchone()


def test_finmind_replacement_with_same_size_and_mtime_imports_new_link(config, tmp_path):
    receipt = metadata_receipt(tmp_path, [{'link': 'https://publisher.test/article/0'}])
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_receipt(tmp_path, receipt, provider='finmind', link_column='link')
        old = receipt.stat()
        receipt = metadata_receipt(tmp_path, [{'link': 'https://publisher.test/article/1'}])
        assert receipt.stat().st_size == old.st_size
        os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
        assert queue.ingest_receipt(tmp_path, receipt, provider='finmind', link_column='link')
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2


def test_replacement_during_later_batch_keeps_prior_cursor_and_resumes(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(4500))
    proof = json.loads(receipt.read_text())
    old = receipt.stat()
    with closing(queue_at(tmp_path, config)) as queue:
        one_batch_clock(queue, monkeypatch)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', maximum_wall_seconds=.05)
        original = queue.add_articles
        def replace_during_batch(batch, **kwargs):
            result = original(batch, **kwargs)
            atomic_write_json(receipt, dict(proof, sha256='0' * 64))
            os.utime(receipt, ns=(old.st_atime_ns, old.st_mtime_ns))
            return result
        monkeypatch.setattr(queue, 'add_articles', replace_during_batch)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.metadata_receipt_pending
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 4096
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 4096
        assert not queue.db.execute('SELECT 1 FROM receipt_heads').fetchone()
        monkeypatch.setattr(queue, 'add_articles', original)
        atomic_write_json(receipt, proof)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 4500
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4500


@pytest.mark.parametrize('broken', [None, [], {'sha256': [], 'rows': 1, 'parquet_path': 'metadata.parquet'},
                                   {'sha256': '0' * 64, 'rows': True, 'parquet_path': 'metadata.parquet'},
                                   {'sha256': '0' * 64, 'rows': 1, 'parquet_path': []}])
def test_malformed_receipt_isolated_and_retried_after_source_repair(config, tmp_path, broken):
    receipt = metadata_receipt(tmp_path, rows(1))
    proof = json.loads(receipt.read_text())
    atomic_write_json(receipt, broken)
    with closing(queue_at(tmp_path, config)) as queue:
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT 1 FROM origin_errors').fetchone()
        assert not queue.db.execute('SELECT 1 FROM receipt_heads').fetchone()
        atomic_write_json(receipt, proof)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert not queue.db.execute('SELECT 1 FROM origin_errors').fetchone()
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1


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
        result = queue.ingest_local_slice(root, tmp_path / 'finmind', maximum_wall_seconds=0)
        assert result == {'imported': 0, 'complete': False}
        assert not queue.metadata_inventory_made_progress
        result = queue.ingest_local_slice(root, tmp_path / 'finmind', maximum_wall_seconds=.05)
        assert result == {'imported': 0, 'complete': False}
        assert queue.metadata_inventory_pending
        assert queue.metadata_inventory_made_progress
        result = queue.ingest_local_slice(root, tmp_path / 'finmind')
        assert result == {'imported': 1, 'complete': True}
        assert not queue.metadata_inventory_pending
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4500


def test_lost_finlab_catalog_keeps_finmind_running_and_recovers(config, tmp_path):
    finlab = tmp_path / 'finlab'
    (finlab / 'receipts').mkdir(parents=True)
    metadata_receipt(finlab, rows(1)).rename(finlab / 'receipts' / 'tw_news.json')
    finmind = tmp_path / 'finmind'

    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': True}
        moved = tmp_path / 'unmounted-finlab'
        finlab.rename(moved)
        finmind.mkdir()
        receipt = metadata_receipt(finmind, [{'link': 'https://publisher.test/article/1'}])
        with closing(sqlite3.connect(finmind / 'queue.sqlite3')) as source, source:
            source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
            source.execute('INSERT INTO tasks VALUES (?,?,?,?)',
                           ('TaiwanStockNews', 'complete', receipt.name, 1))

        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': False}
        status = queue.status()
        assert status['metadata_inventory']['finlab_catalog_errors'] == 1
        assert status['metadata_partition_errors'] == 1
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2

        moved.rename(finlab)
        # Trigger the resident owner's next scheduled local retry without a
        # wall-clock sleep; the actual service path keeps its bounded interval.
        queue._metadata_local_inventory.finlab_retry_at = 0
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 0, 'complete': True}
        assert queue.status()['metadata_inventory']['finlab_catalog_errors'] == 0
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2


def test_both_catalog_failures_recover_independently(config, tmp_path):
    finlab = tmp_path / 'finlab'
    finlab.mkdir()
    finmind = tmp_path / 'finmind'
    finmind.mkdir()
    source_db = finmind / 'queue.sqlite3'
    source_db.write_bytes(b'broken')
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 0, 'complete': False}
        status = queue.status()
        assert status['metadata_partition_errors'] == 2
        assert status['metadata_inventory']['finlab_catalog_errors'] == 1
        assert status['metadata_inventory']['finmind_catalog_errors'] == 1

        (finlab / 'receipts').mkdir()
        metadata_receipt(finlab, rows(1)).rename(finlab / 'receipts' / 'tw_news.json')
        queue._metadata_local_inventory.finlab_retry_at = 0
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': False}
        status = queue.status()
        assert status['metadata_inventory']['finlab_catalog_errors'] == 0
        assert status['metadata_inventory']['finmind_catalog_errors'] == 1
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1

        receipt = metadata_receipt(finmind, [{'link': 'https://publisher.test/article/1'}])
        source_db.unlink()
        with closing(sqlite3.connect(source_db)) as source, source:
            source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
            source.execute('INSERT INTO tasks VALUES (?,?,?,?)',
                           ('TaiwanStockNews', 'complete', receipt.name, 1))
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': True}
        assert queue.status()['metadata_partition_errors'] == 0
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2


def test_both_unavailable_catalogs_do_not_stop_ready_publisher_work(config, tmp_path, monkeypatch):
    from downloader import news_fulltext

    finlab = tmp_path / 'finlab'
    finlab.mkdir()
    finmind = tmp_path / 'finmind'
    finmind.mkdir()
    (finmind / 'queue.sqlite3').write_bytes(b'broken')
    url = 'https://publisher.test/story'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, _ = http_fixture(config, tmp_path, routes)
    queue = queue_at(tmp_path, config)
    monkeypatch.setattr(news_fulltext, 'refresh_disk_admission', lambda *_: False)

    def refresh():
        queue.ingest_local_slice(finlab, finmind, maximum_wall_seconds=.05, stop=http.stop)
        if queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (url,)).fetchone()[0]:
            http.stop.set()

    try:
        with queue.db:
            queue.add_articles([{'url': url}])
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=5)
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (url,)).fetchone()[0]
        assert verify_bodies(queue.root)['accepted']
        status = queue.status()
        assert status['metadata_partition_errors'] == 2
        assert status['metadata_inventory']['finlab_catalog_errors'] == 1
        assert status['metadata_inventory']['finmind_catalog_errors'] == 1
        assert queue.metadata_inventory_pending
    finally:
        http.close()
        queue.close()


def test_missing_finlab_catalog_stays_pending_across_owner_restart(config, tmp_path):
    finlab = tmp_path / 'finlab'
    finlab.mkdir()
    finmind = tmp_path / 'finmind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(finlab, finmind)['complete'] is False
        assert queue.status()['metadata_inventory']['finlab_catalog_errors'] == 1
        before = queue.db.total_changes
        queue._metadata_local_inventory.finlab_retry_at = 0
        assert queue.ingest_local_slice(finlab, finmind)['complete'] is False
        assert queue.db.total_changes == before
    finlab.rmdir()
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(finlab, finmind)['complete'] is False
        assert queue.status()['metadata_inventory']['finlab_catalog_errors'] == 1
        (finlab / 'receipts').mkdir(parents=True)
        queue._metadata_local_inventory.finlab_retry_at = 0
        assert queue.ingest_local_slice(finlab, finmind)['complete'] is True
        assert queue.status()['metadata_inventory']['finlab_catalog_errors'] == 0


def test_finlab_catalog_io_failure_does_not_stop_finmind(config, tmp_path, monkeypatch):
    finlab = tmp_path / 'finlab'
    (finlab / 'receipts').mkdir(parents=True)
    metadata_receipt(finlab, rows(1)).rename(finlab / 'receipts' / 'tw_news.json')
    finmind = tmp_path / 'finmind'
    finmind.mkdir()
    receipt = metadata_receipt(finmind, [{'link': 'https://publisher.test/article/1'}])
    with closing(sqlite3.connect(finmind / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)',
                       ('TaiwanStockNews', 'complete', receipt.name, 1))
    original_scandir = os.scandir

    def fail_finlab(path):
        if os.fspath(path) == os.fspath(finlab / 'receipts'):
            raise PermissionError('simulated source catalog I/O failure')
        return original_scandir(path)

    with closing(queue_at(tmp_path, config)) as queue:
        with monkeypatch.context() as patch:
            patch.setattr(os, 'scandir', fail_finlab)
            assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': False}
            assert queue.status()['metadata_inventory']['finlab_catalog_errors'] == 1
        queue._metadata_local_inventory.finlab_retry_at = 0
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': True}
        assert queue.status()['metadata_partition_errors'] == 0
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2


def test_recovered_finlab_catalog_has_priority_between_finmind_receipts(config, tmp_path, monkeypatch):
    finlab = tmp_path / 'finlab'
    finlab.mkdir()
    finmind = tmp_path / 'finmind'
    finmind.mkdir()
    with closing(sqlite3.connect(finmind / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        for n in (1, 2):
            folder = finmind / str(n)
            folder.mkdir()
            receipt = metadata_receipt(folder, [{'link': f'https://publisher.test/article/{n}'}])
            proof = json.loads(receipt.read_text())
            proof['parquet_path'] = f'{n}/metadata.parquet'
            atomic_write_json(receipt, proof)
            source.execute('INSERT INTO tasks VALUES (?,?,?,?)',
                           ('TaiwanStockNews', 'complete', f'{n}/{receipt.name}', n))
    with closing(queue_at(tmp_path, config)) as queue:
        one_batch_clock(queue, monkeypatch)
        assert queue.ingest_local_slice(finlab, finmind, maximum_wall_seconds=.05)['complete'] is False
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1
        (finlab / 'receipts').mkdir()
        metadata_receipt(finlab, rows(1)).rename(finlab / 'receipts' / 'tw_news.json')
        queue._metadata_local_inventory.finlab_retry_at = 0
        assert queue.ingest_local_slice(finlab, finmind, maximum_wall_seconds=.05)['complete'] is False
        urls = {row[0] for row in queue.db.execute('SELECT url FROM articles')}
        assert urls == {'https://publisher.test/article/0', 'https://publisher.test/article/2'}
        for _ in range(10):
            if queue.ingest_local_slice(finlab, finmind, maximum_wall_seconds=.05)['complete']:
                break
        else:
            pytest.fail('the resumed inventory did not finish in ten original-batch slices')
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 3


def test_finmind_catalog_failure_isolated_and_recovers_without_reimport(config, tmp_path):
    finlab = tmp_path / 'finlab'
    (finlab / 'receipts').mkdir(parents=True)
    metadata_receipt(finlab, rows(1)).rename(finlab / 'receipts' / 'tw_news.json')
    # URI-special characters must not change which local database is opened.
    finmind = tmp_path / 'finmind #?'
    finmind.mkdir()
    receipt = metadata_receipt(finmind, [{'link': 'https://publisher.test/article/1'}])
    source_db = finmind / 'queue.sqlite3'
    source_db.write_bytes(b'not a sqlite database')

    with closing(queue_at(tmp_path, config)) as queue:
        first = queue.ingest_local_slice(finlab, finmind)
        assert first == {'imported': 1, 'complete': False}
        assert queue.metadata_inventory_pending
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1
        status = queue.status()
        assert status['metadata_partition_errors'] == 1
        assert status['metadata_inventory']['finmind_catalog_errors'] == 1
        error = queue.db.execute('SELECT error_code FROM origin_errors').fetchone()[0]
        assert error.startswith('finmind_source_db:DatabaseError:')

        # Failed retries stay pending without repeatedly writing the same error.
        before = queue.db.total_changes
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 0, 'complete': False}
        assert queue.db.total_changes == before
        source_db.unlink()
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 0, 'complete': False}
        assert queue.metadata_inventory_pending

        with closing(sqlite3.connect(source_db)) as source, source:
            source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
            source.execute('INSERT INTO tasks VALUES (?,?,?,?)',
                           ('TaiwanStockNews', 'complete', receipt.name, 1))
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': True}
        assert not queue.metadata_inventory_pending
        status = queue.status()
        assert status['metadata_partition_errors'] == 0
        assert status['metadata_inventory']['finmind_catalog_errors'] == 0
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2


def test_missing_finmind_catalog_remains_pending_after_owner_restart(config, tmp_path):
    finmind = tmp_path / 'finmind'
    finmind.mkdir()
    source_db = finmind / 'queue.sqlite3'
    source_db.write_bytes(b'broken')
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(tmp_path / 'finlab', finmind)['complete'] is False
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 1
    source_db.unlink()
    with closing(queue_at(tmp_path, config)) as restarted:
        assert restarted.ingest_local_slice(tmp_path / 'finlab', finmind)['complete'] is False
        assert restarted.metadata_inventory_pending
        assert restarted.status()['metadata_inventory']['finmind_catalog_errors'] == 1
        with closing(sqlite3.connect(source_db)) as source, source:
            source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        assert restarted.ingest_local_slice(tmp_path / 'finlab', finmind)['complete'] is True
        assert restarted.status()['metadata_inventory']['finmind_catalog_errors'] == 0


def test_missing_or_unmounted_finmind_catalog_is_not_silently_complete(config, tmp_path):
    finmind = tmp_path / 'finmind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(tmp_path / 'finlab', finmind)['complete'] is True
        finmind.mkdir()
        assert queue.ingest_local_slice(tmp_path / 'finlab', finmind)['complete'] is False
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 1

        receipt = metadata_receipt(finmind, [{'link': 'https://publisher.test/article/1'}])
        with closing(sqlite3.connect(finmind / 'queue.sqlite3')) as source, source:
            source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
            source.execute('INSERT INTO tasks VALUES (?,?,?,?)',
                           ('TaiwanStockNews', 'complete', receipt.name, 1))
        assert queue.ingest_local_slice(tmp_path / 'finlab', finmind)['complete'] is True
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 1

        # Simulate the previously used source mount disappearing completely.
        moved = tmp_path / 'moved-finmind'
        finmind.rename(moved)
        assert queue.ingest_local_slice(tmp_path / 'finlab', finmind)['complete'] is False
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 1
        moved.rename(finmind)
        assert queue.ingest_local_slice(tmp_path / 'finlab', finmind)['complete'] is True
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 0
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 1


def test_finmind_catalog_lock_does_not_block_finlab_or_publisher_then_recovers(config, tmp_path, monkeypatch):
    from downloader import news_fulltext

    url = 'https://publisher.test/story'
    finlab = tmp_path / 'finlab'
    (finlab / 'receipts').mkdir(parents=True)
    metadata_receipt(finlab, [{'url': url}]).rename(finlab / 'receipts' / 'tw_news.json')
    finmind = tmp_path / 'finmind'
    finmind.mkdir()
    receipt = metadata_receipt(finmind, [{'link': 'https://publisher.test/article/1'}])
    source = sqlite3.connect(finmind / 'queue.sqlite3')
    source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
    source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', receipt.name, 1))
    source.commit()
    source.execute('BEGIN EXCLUSIVE')

    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = queue_at(tmp_path, config)
    seen = []
    released = False

    def refresh():
        nonlocal released
        if not released and any(target == url for target, _ in session.calls):
            source.rollback()
            released = True
        started = time.monotonic()
        result = queue.ingest_local_slice(finlab, finmind, maximum_wall_seconds=.05, stop=http.stop)
        seen.append((result, time.monotonic() - started))
        if result['complete']:
            http.stop.set()

    monkeypatch.setattr(news_fulltext, 'refresh_disk_admission', lambda *_: False)
    try:
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=5)
        assert released and any(result['complete'] for result, _ in seen)
        assert all(elapsed < 2 for _, elapsed in seen)
        assert any(result == {'imported': 1, 'complete': False} for result, _ in seen)
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (url,)).fetchone()[0]
        assert verify_bodies(queue.root)['accepted']
        assert queue.status()['metadata_partition_errors'] == 0
    finally:
        source.close()
        http.close()
        queue.close()


def test_invalid_finmind_task_does_not_block_publisher_and_recovers(config, tmp_path, monkeypatch):
    from downloader import news_fulltext
    url = 'https://publisher.test/story'
    finlab = tmp_path / 'finlab'
    (finlab / 'receipts').mkdir(parents=True)
    metadata_receipt(finlab, [{'url': url}]).rename(finlab / 'receipts' / 'tw_news.json')
    finmind = tmp_path / 'finmind'
    finmind.mkdir()
    receipt = metadata_receipt(finmind, [{'link': 'https://publisher.test/article/1'}])
    source = sqlite3.connect(finmind / 'queue.sqlite3')
    source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
    source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', None, 1))
    source.commit()
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = queue_at(tmp_path, config)
    seen = []
    repaired = False

    def refresh():
        nonlocal repaired
        if not repaired and any(target == url for target, _ in session.calls):
            source.execute('UPDATE tasks SET receipt_path=? WHERE partition=1', (receipt.name,))
            source.commit()
            queue._metadata_local_inventory.finmind_retry_at = 0
            repaired = True
        result = queue.ingest_local_slice(finlab, finmind, maximum_wall_seconds=.05, stop=http.stop)
        seen.append(result)
        if result['complete']:
            http.stop.set()

    monkeypatch.setattr(news_fulltext, 'refresh_disk_admission', lambda *_: False)
    try:
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=5)
        assert repaired and any(not result['complete'] for result in seen)
        assert any(result['complete'] for result in seen)
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (url,)).fetchone()[0]
        assert verify_bodies(queue.root)['accepted']
        assert queue.status()['metadata_partition_errors'] == 0
    finally:
        source.close()
        http.close()
        queue.close()


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


def test_productive_inventory_continues_without_idle_delay(config, tmp_path, monkeypatch):
    from downloader import news_fulltext

    http, _ = http_fixture(config, tmp_path, {})
    queue = queue_at(tmp_path, config)
    calls, cycles = [], []
    original_cycle = news_fulltext.run_cycle

    def cycle(*args, **kwargs):
        cycles.append(1)
        return original_cycle(*args, **kwargs)

    monkeypatch.setattr(news_fulltext, 'run_cycle', cycle)

    def refresh():
        calls.append(1)
        queue.metadata_inventory_pending = len(calls) < 5
        queue.metadata_inventory_made_progress = True
        if len(calls) == 5:
            http.stop.set()

    try:
        run_service(queue, http, config, max_articles=0, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=10)
        assert len(calls) == 5
        assert len(cycles) == 1  # No one-second idle transition between committed batches.
    finally:
        http.close()
        queue.close()


def test_stalled_inventory_keeps_bounded_retry(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    queue = queue_at(tmp_path, config)
    calls = []

    def refresh():
        calls.append(1)
        queue.metadata_inventory_pending = True
        queue.metadata_inventory_made_progress = False

    try:
        run_service(queue, http, config, max_articles=0, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=.25)
        assert len(calls) == 1
    finally:
        http.close()
        queue.close()


def test_productive_inventory_gives_ready_article_a_dispatch_turn(config, tmp_path, monkeypatch):
    url = 'https://publisher.test/story'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = queue_at(tmp_path, config)
    body_counts = []
    started = threading.Event()
    original_get = session.get

    def observed_get(target, **kwargs):
        started.set()
        return original_get(target, **kwargs)

    monkeypatch.setattr(session, 'get', observed_get)

    def refresh():
        body_counts.append(queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0])
        if len(body_counts) > 1:
            assert started.wait(4), 'a ready article must be dispatched before the next slice'
            time.sleep(.001)  # Give the synthetic worker time to finish its in-memory response.
        queue.metadata_inventory_pending = not body_counts[-1]
        queue.metadata_inventory_made_progress = not body_counts[-1]
        if body_counts[-1]:
            http.stop.set()

    try:
        with queue.db:
            queue.add_articles([{'url': url}])
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=8)
        assert body_counts[0] == 0 and body_counts[-1] == 1
    finally:
        http.close()
        queue.close()


def test_completed_response_is_handed_off_before_the_next_fast_slice(config, tmp_path, monkeypatch):
    from concurrent.futures import Future
    from downloader import news_fulltext

    class ImmediateExecutor:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def submit(self, fn, *args, **kwargs):
            future = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except BaseException as error:
                future.set_exception(error)
            return future

    url = 'https://publisher.test/story'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, _ = http_fixture(config, tmp_path, routes)
    queue = queue_at(tmp_path, config)
    bodies = []

    def refresh():
        bodies.append(queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0])
        queue.metadata_inventory_pending = len(bodies) < 2
        queue.metadata_inventory_made_progress = True
        if len(bodies) == 2:
            http.stop.set()

    monkeypatch.setattr(news_fulltext, 'ThreadPoolExecutor', ImmediateExecutor)
    monkeypatch.setattr(news_fulltext, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr('downloader.news_fulltext_metadata_ingest.METADATA_SLICE_INTERVAL_SECONDS', 60)
    try:
        with queue.db:
            queue.add_articles([{'url': url}])
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=8)
        assert bodies == [0, 1]
    finally:
        http.close()
        queue.close()


def test_productive_inventory_continues_while_source_response_is_inflight(config, tmp_path, monkeypatch):
    url = 'https://publisher.test/story'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = queue_at(tmp_path, config)
    entered, release = threading.Event(), threading.Event()
    original_get = session.get
    calls = []

    def blocked_get(target, **kwargs):
        if target == url:
            entered.set()
            if not release.wait(8):
                raise RuntimeError('fixture response was not released')
        return original_get(target, **kwargs)

    def refresh():
        calls.append(1)
        if len(calls) in (2, 3):
            assert entered.wait(4), 'the ready article must be dispatched before another slice'
        if len(calls) == 3:
            release.set()
        queue.metadata_inventory_pending = len(calls) < 3
        queue.metadata_inventory_made_progress = len(calls) < 3

    monkeypatch.setattr(session, 'get', blocked_get)
    # The original periodic timer cannot explain progress in this test.
    monkeypatch.setattr('downloader.news_fulltext_metadata_ingest.METADATA_SLICE_INTERVAL_SECONDS', 60)
    try:
        with queue.db:
            queue.add_articles([{'url': url}])
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=4)
        assert len(calls) >= 3
        assert queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 1
        assert verify_bodies(queue.root)['accepted']
    finally:
        release.set()
        http.close()
        queue.close()


def test_resident_fast_inventory_preserves_original_batch_semantics(config, tmp_path, monkeypatch):
    from downloader import news_fulltext

    root = tmp_path / 'finlab'
    (root / 'receipts').mkdir(parents=True)
    values = rows(8200)
    values[10] = dict(values[0], title='ignored duplicate')
    values[4096] = dict(values[0], title='later original batch', date='2026-10-02')
    receipt = metadata_receipt(root, values)
    receipt.rename(root / 'receipts' / 'tw_news.json')
    receipt = root / 'receipts' / 'tw_news.json'
    monkeypatch.setattr('downloader.news_fulltext_queue.utc_now', lambda: '2026-10-14T00:00:00+00:00')
    monkeypatch.setattr('downloader.news_fulltext_metadata_ingest.METADATA_COMMIT_MAX_BATCHES', 1)
    with closing(queue_at(tmp_path, config, 'reference')) as reference:
        assert reference.ingest_receipt(root, receipt, provider='finlab', link_column='url')
        expected = semantic_tables(reference)

    queue = queue_at(tmp_path, config, 'resident')
    http, _ = http_fixture(config, tmp_path, {})
    cycles, progress = [], []
    original_cycle = news_fulltext.run_cycle

    def cycle(*args, **kwargs):
        cycles.append(1)
        return original_cycle(*args, **kwargs)

    def refresh():
        result = queue.ingest_local_slice(root, tmp_path / 'finmind', maximum_wall_seconds=.05, stop=http.stop)
        progress.append((result['complete'], queue.metadata_inventory_made_progress))
        if result['complete']:
            http.stop.set()

    monkeypatch.setattr(news_fulltext, 'run_cycle', cycle)
    try:
        run_service(queue, http, config, max_articles=0, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=30)
        assert progress[-1][0] and len(progress) >= 3
        assert len(cycles) == 1
        assert semantic_tables(queue) == expected
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 8200
    finally:
        http.close()
        queue.close()


def test_coalesced_finmind_inventory_preserves_actual_receipts_and_associations(config, tmp_path, monkeypatch):
    root = tmp_path / 'finmind'
    root.mkdir()
    source = sqlite3.connect(root / 'queue.sqlite3')
    source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
    for index in range(2):
        directory = root / str(index)
        directory.mkdir()
        receipt = metadata_receipt(directory, [
            {'link': f'https://publisher.test/article/{index}', 'title': f'title{index}', 'date': '2026-10-01'},
            {'link': 'https://publisher.test/article/shared', 'title': 'shared', 'date': '2026-10-01'},
        ])
        metadata = json.loads(receipt.read_text())
        metadata['parquet_path'] = f'{index}/metadata.parquet'
        atomic_write_json(receipt, metadata)
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', f'{index}/source.json', index))
    source.commit()
    source.close()
    clock = [0.0]
    monkeypatch.setattr('downloader.news_fulltext_metadata_ingest.time.monotonic', lambda: clock[0])
    monkeypatch.setattr('downloader.news_fulltext_queue.utc_now', lambda: '2026-10-14T00:00:00+00:00')

    def run(queue, *, coalesced):
        statuses = []
        original = queue.ingest_receipt
        def receipt(*args, **kwargs):
            result = original(*args, **kwargs)
            clock[0] += 20
            return result
        monkeypatch.setattr(queue, 'ingest_receipt', receipt)
        monkeypatch.setattr(queue, 'status', lambda **kwargs: statuses.append(kwargs['activity']))
        if coalesced:
            with _coalesce_inventory_heartbeats(queue):
                outcome = queue.ingest_local_slice(tmp_path / 'finlab', root)
        else:
            outcome = queue.ingest_local_slice(tmp_path / 'finlab', root)
        return outcome, statuses

    with closing(queue_at(tmp_path, config, 'reference')) as reference, \
            closing(queue_at(tmp_path, config, 'coalesced')) as coalesced:
        baseline, baseline_statuses = run(reference, coalesced=False)
        blocked, blocked_statuses = run(coalesced, coalesced=True)
        assert baseline == blocked == {'imported': 2, 'complete': True}
        assert baseline_statuses == ['inventorying', 'inventorying'] and blocked_statuses == []
        assert semantic_tables(reference) == semantic_tables(coalesced)
        assert coalesced.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4
        assert coalesced.db.execute('SELECT count(*) FROM receipt_heads').fetchone()[0] == 2
        assert coalesced.db.execute("SELECT value FROM counters WHERE key='latest_inventory_finmind_receipts_checked'").fetchone()[0] == 2


@pytest.mark.parametrize('bad_path', [None, sqlite3.Binary(b'\x00bad'), ''])
def test_finmind_invalid_completed_path_preserves_siblings_and_retries(config, tmp_path, bad_path):
    root = tmp_path / 'finmind'
    root.mkdir()
    good = metadata_receipt(root, [{'link': 'https://publisher.test/article/1'}])
    repaired_dir = root / 'repaired'
    repaired_dir.mkdir()
    repaired = metadata_receipt(repaired_dir, [{'link': 'https://publisher.test/article/2'}])
    proof = json.loads(repaired.read_text())
    proof['parquet_path'] = 'repaired/metadata.parquet'
    atomic_write_json(repaired, proof)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', bad_path, 2))
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', good.name, 1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 1, 'complete': False}
        assert queue.metadata_inventory_pending
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 1
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 1
        changes = queue.db.total_changes
        queue._metadata_local_inventory.finmind_retry_at = 0
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 0, 'complete': False}
        assert queue.db.total_changes == changes
        with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
            source.execute("UPDATE tasks SET receipt_path=? WHERE partition=2", ('repaired/source.json',))
        queue._metadata_local_inventory.finmind_retry_at = 0
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 1, 'complete': True}
        assert not queue.metadata_inventory_pending
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 0
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2


def test_finmind_individual_path_io_error_preserves_siblings_and_recovers(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_queue
    root = tmp_path / 'finmind'
    root.mkdir()
    good = metadata_receipt(root, [{'link': 'https://publisher.test/article/1'}])
    second = root / 'second'
    second.mkdir()
    receipt = metadata_receipt(second, [{'link': 'https://publisher.test/article/2'}])
    proof = json.loads(receipt.read_text())
    proof['parquet_path'] = 'second/metadata.parquet'
    atomic_write_json(receipt, proof)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', 'second/source.json', 2))
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', good.name, 1))
    original = news_fulltext_queue.source_path
    def inaccessible(root_arg, relative):
        if relative == 'second/source.json':
            raise PermissionError('simulated source file unavailable')
        return original(root_arg, relative)
    with closing(queue_at(tmp_path, config)) as queue:
        with monkeypatch.context() as patch:
            patch.setattr(news_fulltext_queue, 'source_path', inaccessible)
            assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 1, 'complete': True}
            assert queue.status()['metadata_partition_errors'] == 1
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 1, 'complete': True}
        assert queue.status()['metadata_partition_errors'] == 0
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 2


def test_finlab_rechecks_while_invalid_finmind_catalog_is_pending(config, tmp_path):
    finlab = tmp_path / 'finlab'
    (finlab / 'receipts').mkdir(parents=True)
    receipt = metadata_receipt(finlab, [{'url': 'https://publisher.test/article/1'}])
    receipt.rename(finlab / 'receipts' / 'tw_news.json')
    finmind = tmp_path / 'finmind'
    finmind.mkdir()
    with closing(sqlite3.connect(finmind / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', None, 1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': False}
        new_receipt = metadata_receipt(finlab, [{'url': 'https://publisher.test/article/2'}])
        new_receipt.replace(finlab / 'receipts' / 'tw_news.json')
        queue._metadata_local_inventory.finmind_retry_at = 0
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': False}
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 1


def test_invalid_catalog_does_not_rehash_unchanged_failed_receipt_and_sees_repair(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_ingest as ingest
    root = tmp_path / 'finmind'
    root.mkdir()
    receipt = metadata_receipt(root, [{'link': 'https://publisher.test/article/1'}])
    parquet = root / 'metadata.parquet'
    original_bytes = parquet.read_bytes()
    parquet.write_bytes(b'wrong data')
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', None, 2))
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', receipt.name, 1))
    real_hash = ingest.sha256_file
    hashes = []
    def counted(path):
        hashes.append(str(path))
        return real_hash(path)
    monkeypatch.setattr(ingest, 'sha256_file', counted)
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 0, 'complete': False}
        assert len(hashes) == 1
        inventory = queue._metadata_local_inventory
        fingerprint, _, delay = inventory.finmind_validation_retries[receipt.name]
        inventory.finmind_validation_retries[receipt.name] = (fingerprint, float('inf'), delay)
        inventory.finmind_retry_at = 0
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 0, 'complete': False}
        assert len(hashes) == 1
        parquet.write_bytes(original_bytes)
        inventory.finmind_retry_at = 0
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 1, 'complete': False}
        assert len(hashes) == 2
        with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
            source.execute('DELETE FROM tasks WHERE partition=2')
        inventory.finmind_retry_at = 0
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 0, 'complete': True}
        assert queue.status()['metadata_partition_errors'] == 0
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 1


def test_invalid_finmind_catalog_stays_visible_across_owner_restart(config, tmp_path):
    root = tmp_path / 'finmind'
    root.mkdir()
    receipt = metadata_receipt(root, [{'link': 'https://publisher.test/article/1'}])
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', None, 2))
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', receipt.name, 1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 1, 'complete': False}
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 1
    with closing(queue_at(tmp_path, config)) as restarted:
        assert restarted.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 0, 'complete': False}
        assert restarted.status()['metadata_inventory']['finmind_catalog_errors'] == 1
        with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
            source.execute('DELETE FROM tasks WHERE partition=2')
        restarted._metadata_local_inventory.finmind_retry_at = 0
        assert restarted.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 0, 'complete': True}
        assert restarted.status()['metadata_partition_errors'] == 0
        assert restarted.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 1


def test_pending_invalid_catalog_detects_incremental_and_unannounced_receipt_changes(config, tmp_path, monkeypatch):
    root = tmp_path / 'finmind'
    root.mkdir()
    for name, article in [('changing', 1), ('stable', 2)]:
        directory = root / name
        directory.mkdir()
        receipt = metadata_receipt(directory, [{'link': f'https://publisher.test/article/{article}'}])
        proof = json.loads(receipt.read_text())
        proof['parquet_path'] = f'{name}/metadata.parquet'
        atomic_write_json(receipt, proof)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition,last_attempt_at_utc)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?,?)', ('TaiwanStockNews', 'complete', None, 3, 'one'))
        source.execute('INSERT INTO tasks VALUES (?,?,?,?,?)', ('TaiwanStockNews', 'complete', 'changing/source.json', 2, 'one'))
        source.execute('INSERT INTO tasks VALUES (?,?,?,?,?)', ('TaiwanStockNews', 'complete', 'stable/source.json', 1, 'one'))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 2, 'complete': False}
        calls = []
        original = queue.ingest_receipt
        def record(*args, **kwargs):
            calls.append(args[1])
            return original(*args, **kwargs)
        monkeypatch.setattr(queue, 'ingest_receipt', record)
        receipt = metadata_receipt(root / 'changing', [{'link': 'https://publisher.test/article/3'}])
        proof = json.loads(receipt.read_text())
        proof['parquet_path'] = 'changing/metadata.parquet'
        atomic_write_json(receipt, proof)
        with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
            source.execute("UPDATE tasks SET last_attempt_at_utc='two' WHERE partition=2")
        inventory = queue._metadata_local_inventory
        inventory.finmind_retry_at = 0
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 1, 'complete': False}
        assert calls == [receipt.resolve()]
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 3
        calls.clear()
        receipt = metadata_receipt(root / 'changing', [{'link': 'https://publisher.test/article/4'}])
        proof = json.loads(receipt.read_text())
        proof['parquet_path'] = 'changing/metadata.parquet'
        atomic_write_json(receipt, proof)
        inventory.finmind_retry_at = 0
        inventory.finmind_full_rescan_at = 0
        assert queue.ingest_local_slice(tmp_path / 'finlab', root) == {'imported': 1, 'complete': False}
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 4
        assert queue.status()['metadata_inventory']['finmind_catalog_errors'] == 1


@pytest.mark.parametrize('provider', ['finlab', 'finmind'])
def test_receipt_path_error_clears_after_repair_to_valid_symlink(config, tmp_path, provider):
    root = tmp_path / provider
    root.mkdir()
    target_dir = root / 'target'
    target_dir.mkdir()
    column = 'url' if provider == 'finlab' else 'link'
    target = metadata_receipt(target_dir, [{column: 'https://publisher.test/article/1'}])
    proof = json.loads(target.read_text())
    proof['parquet_path'] = 'target/metadata.parquet'
    atomic_write_json(target, proof)
    if provider == 'finlab':
        (root / 'receipts').mkdir()
        alias = root / 'receipts' / 'tw_news_a.json'
    else:
        alias = root / 'broken.json'
        with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
            source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
            source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', alias.name, 1))
    alias.symlink_to(alias.name)
    finlab, finmind = (root, tmp_path / 'other') if provider == 'finlab' else (tmp_path / 'other', root)
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(finlab, finmind)['complete']
        assert queue.status()['metadata_partition_errors'] == 1
        alias.unlink()
        alias.symlink_to(os.path.relpath(target, alias.parent))
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': True}
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 1
        assert queue.status()['metadata_partition_errors'] == 0


@pytest.mark.parametrize('provider', ['finlab', 'finmind'])
@pytest.mark.parametrize('shared', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_retargeted_logical_receipt_selects_only_referenced_origins(config, tmp_path, provider, shared, restart):
    from downloader.news_fulltext_metadata_ingest import read_alias_census_proof
    from stockagent.data.news_dataset_sources import freeze_inputs

    root = tmp_path / provider
    root.mkdir()
    column = 'url' if provider == 'finlab' else 'link'
    receipts = []
    for name, count in [('old', 1), ('new', 2)]:
        directory = root / name
        directory.mkdir()
        source = metadata_receipt(directory, [{column: f'https://publisher.test/{name}/{i}'} for i in range(count)])
        proof = json.loads(source.read_text())
        proof['parquet_path'] = f'{name}/metadata.parquet'
        atomic_write_json(source, proof)
        receipts.append((source, proof))
    old, new = receipts
    if provider == 'finlab':
        aliases = root / 'receipts'
        aliases.mkdir()
        alias = aliases / 'tw_news_a.json'
        other = aliases / 'tw_news_b.json'
    else:
        alias, other = root / 'alias.json', root / 'other.json'
        with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
            source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
            source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', alias.name, 2))
            if shared:
                source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', other.name, 1))
    alias.symlink_to(os.path.relpath(old[0], alias.parent))
    if shared:
        other.symlink_to(os.path.relpath(old[0], other.parent))
    finlab, finmind = (root, tmp_path / 'other-provider') if provider == 'finlab' else (tmp_path / 'other-provider', root)
    queue = queue_at(tmp_path, config)
    try:
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': True}
        assert queue.status()['metadata_association_rows'] == 1
        if restart:
            queue.close()
            queue = queue_at(tmp_path, config)
        alias.unlink()
        alias.symlink_to(os.path.relpath(new[0], alias.parent))
        assert queue.ingest_local_slice(finlab, finmind) == {'imported': 1, 'complete': True}
        assert queue.status()['metadata_association_rows'] == (3 if shared else 2)
        digest = queue.db.execute('SELECT census_sha256 FROM metadata_receipt_alias_events').fetchone()[0]
        witness = read_alias_census_proof(queue.db, digest)
        assert witness['finlab_catalog' if provider == 'finlab' else 'finmind_catalog'] == sorted(
            ([str(alias.absolute())] + ([str(other.absolute())] if shared else [])) if provider == 'finlab'
            else ([alias.name] + ([other.name] if shared else [])))
        assert witness['retired_targets'] == ([] if shared else [str(old[0].resolve())])
        frozen = tmp_path / 'frozen'
        freeze_inputs(queue.root, frozen)
        with closing(sqlite3.connect(frozen / 'snapshot.sqlite3')) as snapshot:
            hashes = {r[0] for r in snapshot.execute('SELECT sha256 FROM metadata_inputs')}
        assert hashes == ({old[1]['sha256'], new[1]['sha256']} if shared else {new[1]['sha256']})
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2
    finally:
        queue.close()


def _aliased_finlab_sources(tmp_path, sizes):
    root = tmp_path / 'finlab'
    aliases = root / 'receipts'
    aliases.mkdir(parents=True)
    sources = []
    for index, size in enumerate(sizes):
        directory = root / str(index)
        directory.mkdir()
        receipt = metadata_receipt(directory, [
            {'url': f'https://publisher.test/{index}/{row}'} for row in range(size)])
        proof = json.loads(receipt.read_text())
        proof['parquet_path'] = f'{index}/metadata.parquet'
        atomic_write_json(receipt, proof)
        sources.append((receipt, proof))
    alias = aliases / 'tw_news_a.json'
    alias.symlink_to(os.path.relpath(sources[0][0], alias.parent))
    return root, alias, sources


@pytest.mark.parametrize('shared', [False, True])
def test_alias_census_retains_a_reconstructible_reference_witness(config, tmp_path, shared):
    from downloader.news_fulltext_metadata_ingest import read_alias_census_proof

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    other = tmp_path / 'optional-mind'
    shared_alias = alias.parent / 'tw_news_shared.json'
    if shared:
        shared_alias.symlink_to(os.path.relpath(sources[0][0], shared_alias.parent))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        assert queue.ingest_local_slice(root, other)['complete']
        event = queue.db.execute(
            'SELECT old_target,new_target,census_sha256 FROM metadata_receipt_alias_events').fetchone()
        digest = event[2]
        version, raw_size, compressed = queue.db.execute(
            'SELECT version,raw_bytes,payload_zlib FROM metadata_receipt_alias_census_proofs '
            'WHERE census_sha256=?', (digest,)).fetchone()
        raw = zlib.decompress(compressed)
        assert version == 1 and len(raw) == raw_size
        assert hashlib.sha256(raw).hexdigest() == digest
        captured = json.loads(raw)
        assert read_alias_census_proof(queue.db, digest) == captured
        references = {(r[0], r[1]): r[2] for r in captured['references']}
        assert references[('finlab', str(alias.absolute()))] == event[1]
        assert captured['finlab_catalog'] == sorted(
            [str(alias.absolute())] + ([str(shared_alias.absolute())] if shared else []))
        assert captured['retired_targets'] == ([] if shared else [event[0]])
        # The proof is the earlier observation, not a later filesystem lookup.
        alias.unlink()
        assert read_alias_census_proof(queue.db, digest) == captured
        assert [r[0] for r in queue.db.execute('SELECT evidence_sha256 FROM metadata_retired_receipt_targets')] == (
            [] if shared else [digest])


def test_alias_census_proof_detects_corruption_and_distinguishes_legacy(config, tmp_path):
    from downloader.news_fulltext_metadata_ingest import read_alias_census_proof

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, tmp_path / 'mind')['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        assert queue.ingest_local_slice(root, tmp_path / 'mind')['complete']
        digest = queue.db.execute('SELECT census_sha256 FROM metadata_receipt_alias_events').fetchone()[0]
        assert read_alias_census_proof(queue.db, '0' * 64) is None
        original = read_alias_census_proof(queue.db, digest)
        # A self-consistent checksum cannot make an internally false retirement valid.
        original['retired_targets'] = []
        forged = json.dumps(original, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()
        forged_digest = hashlib.sha256(forged).hexdigest()
        with queue.db:
            queue.db.execute('INSERT INTO metadata_receipt_alias_census_proofs VALUES (?,?,?,?,?)',
                             (forged_digest, 1, len(forged), zlib.compress(forged), 'clock'))
            queue.db.execute('UPDATE metadata_receipt_alias_census_proofs SET payload_zlib=? '
                             'WHERE census_sha256=?', (b'corrupted', digest))
        with pytest.raises(ValueError, match='alias_census_proof'):
            read_alias_census_proof(queue.db, forged_digest)
        with pytest.raises(ValueError, match='alias_census_proof'):
            read_alias_census_proof(queue.db, digest)
        with queue.db:
            queue.db.execute('DROP TABLE metadata_receipt_alias_census_proofs')
        assert read_alias_census_proof(queue.db, digest) is None


def test_alias_census_witness_and_retirement_roll_back_together(config, tmp_path):
    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    other = tmp_path / 'optional-mind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        with queue.db:
            queue.db.execute("CREATE TRIGGER fail_alias_event BEFORE INSERT ON metadata_receipt_alias_events "
                             "BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='synthetic failure'):
            queue.ingest_local_slice(root, other)
        assert queue.db.execute('SELECT count(*) FROM metadata_receipt_alias_pending').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM metadata_retired_receipt_targets').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM metadata_receipt_alias_census_proofs').fetchone()[0] == 0
        with queue.db:
            queue.db.execute('DROP TRIGGER fail_alias_event')
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.db.execute('SELECT count(*) FROM metadata_receipt_alias_pending').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM metadata_retired_receipt_targets').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM metadata_receipt_alias_census_proofs').fetchone()[0] == 1


def test_alias_census_never_overwrites_conflicting_existing_proof(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_ingest as metadata_ingest

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    other = tmp_path / 'optional-mind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        assert queue.ingest_local_slice(root, other)['complete']
        digest, original = queue.db.execute(
            'SELECT census_sha256,payload_zlib FROM metadata_receipt_alias_census_proofs').fetchone()
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[0][0], alias.parent))
        build = metadata_ingest._alias_census_proof

        def force_collision(*args):
            _, raw = build(*args)
            return digest, raw

        with monkeypatch.context() as patch:
            patch.setattr(metadata_ingest, '_alias_census_proof', force_collision)
            assert not queue.ingest_local_slice(root, other)['complete']
        assert queue.db.execute('SELECT count(*) FROM metadata_receipt_alias_pending').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM metadata_receipt_alias_events').fetchone()[0] == 1
        assert queue.db.execute('SELECT payload_zlib FROM metadata_receipt_alias_census_proofs '
                                'WHERE census_sha256=?', (digest,)).fetchone()[0] == original
        queue._metadata_local_inventory.alias_retry_at = 0
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.db.execute('SELECT count(*) FROM metadata_receipt_alias_events').fetchone()[0] == 2


def test_retarget_partial_receipt_defers_snapshot_and_resumes_after_restart(config, tmp_path, monkeypatch):
    from stockagent.data.news_dataset_sources import freeze_inputs

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 4500])
    other = tmp_path / 'optional-mind'
    queue = queue_at(tmp_path, config)
    try:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        stop = threading.Event()
        original = queue.add_articles
        def add(values, **kwargs):
            result = original(values, **kwargs)
            stop.set()
            return result
        with monkeypatch.context() as patch:
            patch.setattr(queue, 'add_articles', add)
            assert not queue.ingest_local_slice(root, other, stop=stop)['complete']
        status = queue.status()
        assert status['metadata_inventory']['receipt_alias_retargets_pending'] == 1
        assert status['metadata_association_rows'] is None
        with pytest.raises(ValueError, match='unresolved_metadata_receipt_alias_retarget'):
            freeze_inputs(queue.root, tmp_path / 'pending-snapshot')
        assert not (tmp_path / 'pending-snapshot/snapshot.json').exists()
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2
        queue.close()
        queue = queue_at(tmp_path, config)
        assert queue.status()['metadata_association_rows'] is None
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_association_rows'] == 4500
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 2
        assert queue.db.execute('SELECT count(*) FROM metadata_receipt_alias_events').fetchone()[0] == 1
    finally:
        queue.close()


def test_retarget_again_while_partial_keeps_only_latest_and_preserves_origins(config, tmp_path, monkeypatch):
    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 4500, 2])
    other = tmp_path / 'optional-mind'
    queue = queue_at(tmp_path, config)
    try:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        stop = threading.Event()
        original = queue.add_articles
        def add(values, **kwargs):
            result = original(values, **kwargs)
            stop.set()
            return result
        with monkeypatch.context() as patch:
            patch.setattr(queue, 'add_articles', add)
            assert not queue.ingest_local_slice(root, other, stop=stop)['complete']
        queue.close()
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[2][0], alias.parent))
        queue = queue_at(tmp_path, config)
        assert queue.ingest_local_slice(root, other)['complete']
        status = queue.status()
        assert status['metadata_association_rows'] == 2
        assert status['metadata_inventory']['partial_partitions'] == 0
        assert queue.db.execute("SELECT state FROM metadata_ingest_progress p JOIN origins o ON o.id=p.origin_id "
                                "WHERE o.receipt_path=?", (str(sources[1][0].resolve()),)).fetchone()[0] == 'superseded'
        assert queue.db.execute('SELECT count(*) FROM origins').fetchone()[0] == 3
        retired = {r[0] for r in queue.db.execute('SELECT receipt_path FROM metadata_retired_receipt_targets')}
        assert retired == {str(sources[0][0].resolve()), str(sources[1][0].resolve())}
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_association_rows'] == 4500
        assert queue.status()['metadata_inventory']['partial_partitions'] == 0
    finally:
        queue.close()


def test_retargeted_inflight_reader_releases_resources_and_can_resume(config, tmp_path, monkeypatch):
    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 4500, 2])
    other = tmp_path / 'optional-mind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        stop = threading.Event()
        original = queue.add_articles
        def add(values, **kwargs):
            result = original(values, **kwargs)
            stop.set()
            return result
        with monkeypatch.context() as patch:
            patch.setattr(queue, 'add_articles', add)
            assert not queue.ingest_local_slice(root, other, stop=stop)['complete']
        old_target = str(sources[1][0].resolve())
        assert old_target in queue._metadata_receipt_readers
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[2][0], alias.parent))
        stop.clear()
        assert queue.ingest_local_slice(root, other, stop=stop)['complete']
        assert old_target not in queue._metadata_receipt_readers
        assert queue.status()['metadata_inventory']['partial_partitions'] == 0
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        assert queue.ingest_local_slice(root, other, stop=stop)['complete']
        assert queue.status()['metadata_association_rows'] == 4500


def test_retired_target_error_is_not_current_but_reappears_when_referenced(config, tmp_path):
    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 3, 2])
    other = tmp_path / 'optional-mind'
    invalid = {**sources[1][1], 'sha256': '0' * 64}
    atomic_write_json(sources[1][0], invalid)
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        assert not queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_partition_errors'] == 1
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[2][0], alias.parent))
        queue._metadata_local_inventory.alias_retry_at = 0
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_partition_errors'] == 0
        target = str(sources[1][0].resolve())
        assert queue.db.execute('SELECT 1 FROM origin_errors WHERE receipt_path=?', (target,)).fetchone()
        another = alias.parent / 'tw_news_b.json'
        another.symlink_to(os.path.relpath(sources[1][0], another.parent))
        assert not queue.ingest_local_slice(root, other)['complete']
        status = queue.status()
        assert status['metadata_partition_errors'] == 1
        assert status['metadata_association_rows'] is None
        assert status['metadata_inventory']['receipt_alias_retargets_pending'] == 1
        atomic_write_json(sources[1][0], sources[1][1])
        queue._metadata_local_inventory.alias_retry_at = 0
        assert queue.ingest_local_slice(root, other)['complete']
        status = queue.status()
        assert status['metadata_partition_errors'] == 0
        assert status['metadata_association_rows'] == 5


def test_pending_alias_does_not_reactivate_unrelated_retired_errors(config, tmp_path):
    with closing(queue_at(tmp_path, config)) as queue:
        with queue.db:
            for target in ('/retired-one', '/retired-two'):
                queue.db.execute('INSERT INTO origin_errors VALUES (?,?,?)', (target, 'old-error', 'clock'))
                queue.db.execute('INSERT INTO metadata_retired_receipt_targets VALUES (?,?,?)',
                                 (target, 'clock', 'digest'))
        assert queue.status()['metadata_partition_errors'] == 0
        with queue.db:
            queue.db.execute('INSERT INTO metadata_receipt_alias_pending VALUES (?,?,?,?,?)',
                             ('finlab', '/logical', '/retired-one', '', 'clock'))
        assert queue.status()['metadata_partition_errors'] == 1


def test_census_failure_retains_pending_and_retries_after_repair(config, tmp_path, monkeypatch):
    from downloader.news_fulltext_metadata_ingest import LocalInventory

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    other = tmp_path / 'optional-mind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        def unavailable(_self):
            raise OSError('catalog unavailable')
        with monkeypatch.context() as patch:
            patch.setattr(LocalInventory, '_alias_catalog', unavailable)
            assert not queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_association_rows'] is None
        assert queue.db.execute('SELECT count(*) FROM metadata_retired_receipt_targets').fetchone()[0] == 0
        queue._metadata_local_inventory.alias_retry_at = 0
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_association_rows'] == 2


def test_alias_changed_during_census_retries_with_new_target(config, tmp_path, monkeypatch):
    from downloader.news_fulltext_metadata_ingest import LocalInventory

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2, 3])
    other = tmp_path / 'optional-mind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        original = LocalInventory._alias_catalog
        calls = []
        def change_after_catalog(owner):
            result = original(owner)
            calls.append(1)
            if len(calls) == 1:
                alias.unlink()
                alias.symlink_to(os.path.relpath(sources[2][0], alias.parent))
            return result
        with monkeypatch.context() as patch:
            patch.setattr(LocalInventory, '_alias_catalog', change_after_catalog)
            assert not queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_association_rows'] is None
        assert queue.db.execute('SELECT count(*) FROM metadata_retired_receipt_targets').fetchone()[0] == 0
        queue._metadata_local_inventory.alias_retry_at = 0
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_association_rows'] == 3
        retired = {r[0] for r in queue.db.execute('SELECT receipt_path FROM metadata_retired_receipt_targets')}
        assert retired == {str(sources[0][0].resolve()), str(sources[1][0].resolve())}


def test_alias_census_yields_and_resumes_without_losing_references(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_ingest as metadata_ingest

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    for index in range(10):
        another = alias.parent / f'tw_news_shared_{index}.json'
        another.symlink_to(os.path.relpath(sources[0][0], another.parent))
    other = tmp_path / 'optional-mind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        stop = threading.Event()
        original = metadata_ingest._signature
        def stop_during_census(path):
            result = original(path)
            inventory = queue._metadata_local_inventory
            if inventory is not None and inventory.alias_census is not None:
                stop.set()
            return result
        with monkeypatch.context() as patch:
            patch.setattr(metadata_ingest, '_signature', stop_during_census)
            assert not queue.ingest_local_slice(root, other, stop=stop)['complete']
        assert queue._metadata_local_inventory.alias_census is not None
        assert queue.status()['metadata_association_rows'] is None
        stop.clear()
        assert queue.ingest_local_slice(root, other, stop=stop)['complete']
        assert queue.status()['metadata_association_rows'] == 3


@pytest.mark.parametrize('change', ['catalog_generation', 'unrelated_receipt_mtime'])
def test_alias_census_tolerates_unrelated_nonmembership_updates(config, tmp_path, monkeypatch, change):
    from downloader.news_fulltext_metadata_ingest import LocalInventory

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    mind = tmp_path / 'mind'
    mind.mkdir()
    other_receipt = metadata_receipt(mind, [{'link': 'https://publisher.test/other'}])
    with closing(sqlite3.connect(mind / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition,last_attempt_at_utc)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?,?)',
                       ('TaiwanStockNews', 'complete', other_receipt.name, 1, 'generation-0'))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, mind)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        original = LocalInventory._alias_catalog
        calls = []
        def update_unrelated(owner):
            result = original(owner)
            calls.append(1)
            if change == 'catalog_generation':
                with closing(sqlite3.connect(mind / 'queue.sqlite3')) as source, source:
                    source.execute('UPDATE tasks SET last_attempt_at_utc=?', (f'generation-{len(calls)}',))
            else:
                current = other_receipt.stat()
                os.utime(other_receipt, ns=(current.st_atime_ns, current.st_mtime_ns + 1_000_000_000))
            return result
        monkeypatch.setattr(LocalInventory, '_alias_catalog', update_unrelated)
        assert queue.ingest_local_slice(root, mind)['complete']
        assert len(calls) >= 3
        assert queue.status()['metadata_association_rows'] == 3


def test_alias_census_retries_when_completed_catalog_membership_changes(config, tmp_path, monkeypatch):
    from downloader.news_fulltext_metadata_ingest import LocalInventory

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    mind = tmp_path / 'mind'
    mind.mkdir()
    receipt = metadata_receipt(mind, [{'link': 'https://publisher.test/other'}])
    with closing(sqlite3.connect(mind / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', receipt.name, 1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, mind)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        original = LocalInventory._alias_catalog
        calls = []
        def add_member(owner):
            result = original(owner)
            calls.append(1)
            if len(calls) == 1:
                with closing(sqlite3.connect(mind / 'queue.sqlite3')) as source, source:
                    source.execute('INSERT INTO tasks VALUES (?,?,?,?)',
                                   ('TaiwanStockNews', 'complete', receipt.name, 2))
            return result
        with monkeypatch.context() as patch:
            patch.setattr(LocalInventory, '_alias_catalog', add_member)
            assert not queue.ingest_local_slice(root, mind)['complete']
        assert queue.status()['metadata_association_rows'] is None
        assert queue.db.execute('SELECT count(*) FROM metadata_retired_receipt_targets').fetchone()[0] == 0
        queue._metadata_local_inventory.alias_retry_at = 0
        assert queue.ingest_local_slice(root, mind)['complete']
        assert queue.status()['metadata_association_rows'] == 3


def test_previously_retired_receipt_is_restored_when_independently_referenced(config, tmp_path):
    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    other = tmp_path / 'optional-mind'
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_association_rows'] == 2
        another = alias.parent / 'tw_news_b.json'
        another.symlink_to(os.path.relpath(sources[0][0], another.parent))
        assert queue.ingest_local_slice(root, other)['complete']
        assert queue.status()['metadata_association_rows'] == 3
        assert queue.db.execute('SELECT count(*) FROM metadata_retired_receipt_targets').fetchone()[0] == 0


def test_finmind_direct_receipt_reference_keeps_old_target_selected(config, tmp_path):
    root = tmp_path / 'finmind'
    root.mkdir()
    receipts = []
    for name, size in [('old', 1), ('new', 2)]:
        directory = root / name
        directory.mkdir()
        receipt = metadata_receipt(directory, [
            {'link': f'https://publisher.test/{name}/{i}'} for i in range(size)])
        proof = json.loads(receipt.read_text())
        proof['parquet_path'] = f'{name}/metadata.parquet'
        atomic_write_json(receipt, proof)
        receipts.append(receipt)
    alias = root / 'alias.json'
    alias.symlink_to(os.path.relpath(receipts[0], alias.parent))
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', alias.name, 2))
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)',
                       ('TaiwanStockNews', 'complete', str(receipts[0].relative_to(root)), 1))
    with closing(queue_at(tmp_path, config)) as queue:
        assert queue.ingest_local_slice(tmp_path / 'optional-lab', root)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(receipts[1], alias.parent))
        assert queue.ingest_local_slice(tmp_path / 'optional-lab', root)['complete']
        assert queue.status()['metadata_association_rows'] == 3
        assert queue.db.execute('SELECT count(*) FROM metadata_retired_receipt_targets').fetchone()[0] == 0


def test_alias_census_failure_does_not_block_publisher_and_recovers(config, tmp_path, monkeypatch):
    from downloader import news_fulltext
    from downloader.news_fulltext_metadata_ingest import LocalInventory

    root, alias, sources = _aliased_finlab_sources(tmp_path, [1, 2])
    other = tmp_path / 'optional-mind'
    url = 'https://publisher.test/story'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = queue_at(tmp_path, config)
    original = LocalInventory._alias_catalog
    seen = []
    try:
        assert queue.ingest_local_slice(root, other)['complete']
        alias.unlink()
        alias.symlink_to(os.path.relpath(sources[1][0], alias.parent))
        with queue.db:
            queue.add_articles([{'url': url}])
        def census(owner):
            if not any(target == url for target, _ in session.calls):
                raise OSError('simulated transient catalog failure')
            return original(owner)
        def refresh():
            if any(target == url for target, _ in session.calls):
                queue._metadata_local_inventory.alias_retry_at = 0
            result = queue.ingest_local_slice(root, other, maximum_wall_seconds=.05, stop=http.stop)
            seen.append(result)
            if result['complete'] and any(target == url for target, _ in session.calls):
                http.stop.set()
        monkeypatch.setattr(LocalInventory, '_alias_catalog', census)
        monkeypatch.setattr(news_fulltext, 'refresh_disk_admission', lambda *_: False)
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0,
                    refresh_metadata=refresh, duration_seconds=8)
        assert any(not result['complete'] for result in seen)
        assert any(result['complete'] for result in seen)
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (url,)).fetchone()[0]
        assert verify_bodies(queue.root)['accepted']
        assert queue.status()['metadata_association_rows'] == 2
    finally:
        http.close()
        queue.close()


def test_finmind_inventory_isolates_symlink_cycle_and_recovers_on_next_scan(config, tmp_path):
    root = tmp_path / 'finmind'
    root.mkdir()
    good = metadata_receipt(root, [{'link': 'https://publisher.test/article/1'}])
    second = root / 'second'
    second.mkdir()
    held = metadata_receipt(second, [{'link': 'https://publisher.test/article/2'}])
    contents = json.loads(held.read_text())
    contents['parquet_path'] = 'second/metadata.parquet'
    atomic_write_json(held, contents)
    held = held.rename(root / 'broken-held.json')
    broken = root / 'broken.json'
    broken.symlink_to(broken.name)
    with closing(sqlite3.connect(root / 'queue.sqlite3')) as source, source:
        source.execute('CREATE TABLE tasks(dataset,state,receipt_path,partition)')
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', broken.name, 2))
        source.execute('INSERT INTO tasks VALUES (?,?,?,?)', ('TaiwanStockNews', 'complete', good.name, 1))
    with closing(queue_at(tmp_path, config)) as queue:
        first = queue.ingest_local_slice(tmp_path / 'finlab', root)
        assert first == {'imported': 1, 'complete': True}
        assert queue.db.execute('SELECT count(*) FROM origin_errors WHERE receipt_path=?', (str(broken),)).fetchone()[0] == 1
        broken.unlink()
        held.rename(broken)
        second_result = queue.ingest_local_slice(tmp_path / 'finlab', root)
        assert second_result == {'imported': 1, 'complete': True}
        assert queue.db.execute('SELECT count(*) FROM origin_errors WHERE receipt_path=?', (str(broken),)).fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2


def test_finlab_inventory_isolates_symlink_cycle_and_recovers_on_next_scan(config, tmp_path):
    root = tmp_path / 'finlab'
    receipts = root / 'receipts'
    receipts.mkdir(parents=True)
    good = metadata_receipt(receipts, [{'url': 'https://publisher.test/article/1'}])
    contents = json.loads(good.read_text())
    contents['parquet_path'] = 'receipts/metadata.parquet'
    atomic_write_json(good, contents)
    good.rename(receipts / 'tw_news_b.json')
    second = root / 'second'
    second.mkdir()
    held = metadata_receipt(second, [{'url': 'https://publisher.test/article/2'}])
    contents = json.loads(held.read_text())
    contents['parquet_path'] = 'second/metadata.parquet'
    atomic_write_json(held, contents)
    broken = receipts / 'tw_news_a.json'
    broken.symlink_to(broken.name)
    with closing(queue_at(tmp_path, config)) as queue:
        first = queue.ingest_local_slice(root, tmp_path / 'finmind')
        assert first == {'imported': 1, 'complete': True}
        assert queue.db.execute('SELECT count(*) FROM origin_errors WHERE receipt_path=?', (str(broken),)).fetchone()[0] == 1
        broken.unlink()
        held.rename(broken)
        second_result = queue.ingest_local_slice(root, tmp_path / 'finmind')
        assert second_result == {'imported': 1, 'complete': True}
        assert queue.db.execute('SELECT count(*) FROM origin_errors WHERE receipt_path=?', (str(broken),)).fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2


def test_receipt_cycle_closes_partial_reader_and_resumes_cursor(config, tmp_path, monkeypatch):
    receipt = metadata_receipt(tmp_path, rows(4500))
    with closing(queue_at(tmp_path, config)) as queue:
        original_cache = queue.db.execute('PRAGMA cache_size').fetchone()[0]
        one_batch_clock(queue, monkeypatch)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url',
                                        maximum_wall_seconds=.05)
        assert queue.db.execute('SELECT processed_rows FROM metadata_ingest_progress').fetchone()[0] == 4096
        held = receipt.with_suffix('.held')
        receipt.rename(held)
        receipt.symlink_to(receipt.name)
        assert not queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('PRAGMA cache_size').fetchone()[0] == original_cache
        assert queue.db.execute('SELECT state FROM metadata_ingest_progress').fetchone()[0] == 'error'
        receipt.unlink()
        held.rename(receipt)
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert queue.db.execute('SELECT sum(association_rows) FROM source_links').fetchone()[0] == 4500
        assert not queue.db.execute('SELECT 1 FROM metadata_ingest_progress').fetchone()
        assert not queue.db.execute('SELECT 1 FROM origin_errors').fetchone()


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
