"""A repaired local batch must become discoverable without a publisher request."""
from contextlib import closing
import json
import time

import pyarrow as pa
import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg  # noqa: F401
from test_news_cnyes_history_evidence import (  # noqa: F401
    queue, seed_accepted_batch_for_history_page, seed_unindexed_nonzero_pages, source_payload,
)


@pytest.mark.parametrize('damage', ['invalid_parquet', 'symlink_loop'])
def test_changed_invalid_accepted_batch_is_revisited_without_source_mutation(queue, monkeypatch, damage):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    content = path.read_bytes()
    if damage == 'invalid_parquet':
        path.write_bytes(b'not a parquet file')
    else:
        path.unlink()
        path.symlink_to(path.name)
    with queue.db:
        queue.db.executemany("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?",
                             [(url,) for url in urls])
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    first = history.seed_accepted_body_batch_candidates(queue, limit=100, max_seconds=30)
    assert first['complete']
    assert queue.db.execute("SELECT state FROM cnyes_history_accepted_batch_outcomes WHERE parquet_path=?",
                            (relative,)).fetchone()[0] == 'invalid'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                                (relative,)).fetchone()
    if damage == 'symlink_loop':
        path.unlink()
    path.write_bytes(content)
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3601)
    for _ in range(4):
        history.backfill(queue, limit=20, max_seconds=30)
        if queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                            (relative,)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                            (relative,)).fetchone()
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_unchanged_invalid_batch_skips_decode_after_restart_and_recovers_on_change(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    content = path.read_bytes()
    path.write_bytes(b'invalid parquet')
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    history.seed_accepted_body_batch_candidates(queue, limit=100, max_seconds=30)
    reads = []
    original = history._accepted_batch_metadata_pairs

    def counted(*args, **kwargs):
        reads.append(1)
        yield from original(*args, **kwargs)

    monkeypatch.setattr(history, '_accepted_batch_metadata_pairs', counted)
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3)
    assert history.retry_invalid_accepted_batches(queue, limit=100, max_seconds=30)['processed'] == 1
    assert len(reads) == 1
    retry = queue.db.execute('SELECT confirmed,not_before_epoch FROM cnyes_history_accepted_invalid_retries').fetchone()
    assert retry['confirmed'] == 1 and retry['not_before_epoch'] == epoch + 3 + history.ACCEPTED_INVALID_RECHECK_SECONDS
    assert history.progress(queue.db, queue.registry.sources['cnyes'])['accepted_body_batch_scan']['local_invalid_retry_batches'] == 1
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        monkeypatch.setattr(history.time, 'time', lambda: epoch + 3604)
        result = history.retry_invalid_accepted_batches(resumed, limit=100, max_seconds=30)
        assert result['unchanged'] == 1 and len(reads) == 1
        path.write_bytes(content)
        monkeypatch.setattr(history.time, 'time', lambda: epoch + 7205)
        result = history.retry_invalid_accepted_batches(resumed, limit=100, max_seconds=30)
        assert result['indexed'] == 1 and len(reads) == 2
        assert not resumed.db.execute('SELECT 1 FROM cnyes_history_accepted_invalid_retries').fetchone()
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_invalid_batch_progress_distinguishes_migration_wait_and_recovery(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    original = path.read_bytes()
    path.write_bytes(b'invalid parquet')
    source = queue.registry.sources['cnyes']
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (urls[2],))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    assert history.seed_accepted_body_batch_candidates(queue, limit=100, max_seconds=30)['complete']
    scan = history.progress(queue.db, source)['accepted_body_batch_scan']
    assert (scan['scan_state'], scan['local_invalid_migration_state'], scan['local_invalid_retry_batches']) == (
        'scanning', 'not_started', 1)

    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3)
    assert history.retry_invalid_accepted_batches(queue, limit=100, max_seconds=30)['processed'] == 1
    scan = history.progress(queue.db, source)['accepted_body_batch_scan']
    assert (scan['scan_state'], scan['local_invalid_migration_state'], scan['local_invalid_retry_batches']) == (
        'local_retry_wait', 'complete', 1)

    path.write_bytes(original)
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3604)
    assert history.retry_invalid_accepted_batches(queue, limit=100, max_seconds=30)['indexed'] == 1
    scan = history.progress(queue.db, source)['accepted_body_batch_scan']
    assert (scan['scan_state'], scan['local_invalid_migration_state'], scan['local_invalid_retry_batches']) == (
        'complete', 'complete', 0)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_invalid_migration_checkpoint_is_unknown_until_repaired(queue):
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    source = queue.registry.sources['cnyes']
    assert history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)['complete']
    key = history.ACCEPTED_INVALID_SCAN_PREFIX + history.digest(history.accepted_batch_scan_key(source))
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings(key,value) VALUES (?,?)', (key, 'PRIVATE_INVALID_JSON'))
    scan = history.progress(queue.db, source)['accepted_body_batch_scan']
    assert scan['scan_state'] == scan['local_invalid_migration_state'] == 'unknown'
    assert scan['scanned_batches'] == scan['indexed_candidates'] == 1
    assert 'PRIVATE_INVALID_JSON' not in json.dumps(scan)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['migration']['complete']
    scan = history.progress(queue.db, source)['accepted_body_batch_scan']
    assert scan['scan_state'] == scan['local_invalid_migration_state'] == 'complete'


def test_legacy_schema_keeps_invalid_batch_progress_unknown(queue):
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    source = queue.registry.sources['cnyes']
    assert history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)['complete']
    with queue.db:
        queue.db.execute('DROP TABLE cnyes_history_accepted_invalid_retries')
    scan = history.progress(queue.db, source)['accepted_body_batch_scan']
    assert scan['scan_state'] == 'unknown'
    assert scan['local_invalid_migration_state'] is scan['local_invalid_retry_batches'] is None


def test_progress_without_source_keeps_unqueried_local_counts_unknown(queue):
    scan = history.progress(queue.db)['accepted_body_batch_scan']
    assert scan['scan_state'] == 'unknown'
    assert scan['local_retry_batches'] is scan['local_missing_retry_batches'] is None
    assert scan['local_invalid_retry_batches'] is scan['local_invalid_migration_state'] is None


@pytest.mark.parametrize('previously_recovered', [False, True])
def test_previous_invalid_outcome_is_migrated_and_recovered(queue, monkeypatch, previously_recovered):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, receipt = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                         (relative, receipt, 'invalid', 'ArrowInvalid', '2020-01-01T00:00:00+00:00'))
        if previously_recovered:
            queue.db.execute('INSERT INTO cnyes_history_accepted_batch_outcomes VALUES (?,?,?,?,?)',
                             (relative, receipt, 'recovered', 'earlier_recovery', '2021-01-01T00:00:00+00:00'))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    migrated = history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)
    assert migrated['migration']['complete'] and migrated['migration']['checked'] == 1
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_invalid_retries').fetchone()
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['indexed'] == 1
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                            (relative,)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


@pytest.mark.parametrize('previous_candidate', [False, True])
def test_changed_registered_receipt_is_revisited_without_source_mutation(queue, monkeypatch, previous_candidate):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, receipt = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    row = queue.db.execute('SELECT path,receipt_path FROM accepted_batches WHERE path=?', (relative,)).fetchone()
    if previous_candidate:
        assert history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)['indexed'] == 1
    new_receipt = receipt.replace('.json', '-replacement.json')
    (queue.root / new_receipt).write_bytes((queue.root / receipt).read_bytes())
    source = queue.registry.sources['cnyes']
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    with queue.db:
        history._defer_invalid_accepted_batch(queue, history.accepted_batch_scan_key(source), row)
        queue.db.execute('UPDATE accepted_batches SET receipt_path=? WHERE path=?', (new_receipt, relative))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (urls[2],))
        if previous_candidate:
            sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (urls[2],)).fetchone()[0]
            evidence = queue.db.execute('SELECT total,ids_sha256,observed_at_utc FROM bulk_page_evidence '
                                        'WHERE scope_url=? AND page=3', (history.cnyes_scope(urls[2]),)).fetchone()
            queue.db.execute('INSERT INTO cnyes_history_retained_work '
                '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state,outcome_json) '
                "VALUES (?,?,?,?,?,?,?,'retained_invalid','{}')",
                (urls[2], history.cnyes_scope(urls[2]), 3, sha, *evidence))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['processed'] == 1
    retry = queue.db.execute('SELECT receipt_path,not_before_epoch FROM cnyes_history_accepted_invalid_retries').fetchone()
    assert retry['receipt_path'] == new_receipt and retry['not_before_epoch'] == epoch + 5
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 6)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['indexed'] == (0 if previous_candidate else 1)
    candidate = queue.db.execute('SELECT receipt_path FROM cnyes_history_accepted_batch_candidates '
                                 'WHERE parquet_path=?', (relative,)).fetchone()
    assert candidate['receipt_path'] == new_receipt
    if previous_candidate:
        revision = queue.db.execute('SELECT old_receipt_path,new_receipt_path FROM '
                                    'cnyes_history_candidate_receipt_revisions').fetchone()
        assert tuple(revision) == (receipt, new_receipt)
        assert (queue.root / receipt).is_file()
        assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                (urls[2],)).fetchone()[0] == 'pending'
        # An older scan must not replace the current registered receipt again.
        with queue.db:
            assert not history._register_accepted_batch_candidate(queue, urls[2], sha, relative, receipt)
        assert queue.db.execute('SELECT receipt_path FROM cnyes_history_accepted_batch_candidates '
                                'WHERE parquet_path=?', (relative,)).fetchone()[0] == new_receipt
        assert queue.db.execute('SELECT count(*) FROM cnyes_history_candidate_receipt_revisions').fetchone()[0] == 1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_changed_batch_during_retry_rolls_back_candidate_and_retries(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    row = queue.db.execute('SELECT path,receipt_path FROM accepted_batches WHERE path=?', (relative,)).fetchone()
    source = queue.registry.sources['cnyes']
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    with queue.db:
        history._defer_invalid_accepted_batch(queue, history.accepted_batch_scan_key(source), row)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3)
    witnesses = iter(('before', 'after'))
    with monkeypatch.context() as patch:
        patch.setattr(history, '_accepted_invalid_witness', lambda *args: next(witnesses))
        result = history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)
    assert result['indexed'] == 0
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates').fetchone()
    retry = queue.db.execute('SELECT local_attempts FROM cnyes_history_accepted_invalid_retries').fetchone()
    assert retry['local_attempts'] == 1
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 5)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['indexed'] == 1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_reappeared_missing_batch_with_invalid_parquet_becomes_invalid_retry(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    original = path.read_bytes()
    path.unlink()
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (urls[2],))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    assert history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)['complete']
    path.write_bytes(b'not a parquet file')
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3)
    result = history.retry_missing_accepted_batches(queue, limit=20, max_seconds=30)
    assert result['processed'] == 1 and result['indexed'] == 0
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_missing_retries').fetchone()
    assert queue.db.execute('SELECT confirmed FROM cnyes_history_accepted_invalid_retries').fetchone()[0] == 0
    assert queue.db.execute("SELECT reason FROM cnyes_history_accepted_batch_outcomes WHERE state='invalid'").fetchone()[0] == 'ArrowInvalid'
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 6)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['processed'] == 1
    retry = queue.db.execute('SELECT confirmed,not_before_epoch FROM cnyes_history_accepted_invalid_retries').fetchone()
    assert retry['confirmed'] == 1 and retry['not_before_epoch'] == epoch + 6 + history.ACCEPTED_INVALID_RECHECK_SECONDS
    path.write_bytes(original)
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3607)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['indexed'] == 1
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_invalid_retries').fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


@pytest.mark.parametrize('lane', ['initial', 'missing'])
@pytest.mark.parametrize('failure', ['invalid', 'io', 'memory'])
def test_partial_batch_index_failure_does_not_commit_candidates_or_count(queue, monkeypatch, lane, failure):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='a')
    other, _ = seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61), batch_tag='b')
    path = queue.root / relative
    held = path.with_suffix('.held')
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (urls[2],))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    if lane == 'missing':
        path.rename(held)
        assert history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)['indexed'] == 1
        held.rename(path)
        monkeypatch.setattr(history.time, 'time', lambda: epoch + 3)
    original = history._accepted_batch_metadata_pairs
    error = {'invalid': pa.ArrowInvalid('invalid after candidate'),
             'io': OSError(5, 'transient after candidate'),
             'memory': MemoryError('pressure after candidate')}[failure]
    def fail_after_first(file_path, source):
        for pair in original(file_path, source):
            yield pair
            if file_path == path:
                raise error
    with monkeypatch.context() as patch:
        patch.setattr(history, '_accepted_batch_metadata_pairs', fail_after_first)
        result = (history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)
                  if lane == 'initial' else history.retry_missing_accepted_batches(queue, limit=20, max_seconds=30))
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?', (relative,)).fetchone()
    expected_other = lane == 'missing' or failure != 'memory'
    assert bool(queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                                 (other,)).fetchone()) == expected_other
    assert result['indexed'] == (1 if lane == 'initial' and failure != 'memory' else 0)
    assert bool(queue.db.execute('SELECT 1 FROM cnyes_history_accepted_invalid_retries WHERE parquet_path=?',
                                 (relative,)).fetchone()) == (failure == 'invalid')
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 6)
    if failure == 'invalid':
        assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['indexed'] == 1
    elif lane == 'missing':
        assert history.retry_missing_accepted_batches(queue, limit=20, max_seconds=30)['indexed'] == 1
    else:
        assert history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)['indexed'] >= 1
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?', (relative,)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


@pytest.mark.parametrize('lane', ['missing', 'local_io'])
def test_accepted_retry_uses_current_registered_receipt(queue, monkeypatch, lane):
    from downloader import news_fulltext_queue as queue_module
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, receipt = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    receipt_path = queue.root / receipt
    new_receipt = receipt.replace('.json', '-replacement.json')
    (queue.root / new_receipt).write_bytes(receipt_path.read_bytes())
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (urls[2],))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    if lane == 'missing':
        receipt_path.unlink()
        history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)
        retry_table = 'cnyes_history_accepted_missing_retries'
    else:
        original = queue_module.replay_source_path
        def unavailable(root, path):
            if path == relative:
                raise OSError(5, 'temporary local I/O')
            return original(root, path)
        with monkeypatch.context() as patch:
            patch.setattr(queue_module, 'replay_source_path', unavailable)
            history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)
        receipt_path.unlink()
        retry_table = 'cnyes_history_accepted_batch_local_retries'
    with queue.db:
        queue.db.execute('UPDATE accepted_batches SET receipt_path=? WHERE path=?', (new_receipt, relative))
        queue.db.execute(f'UPDATE {retry_table} SET not_before_epoch=0 WHERE parquet_path=?', (relative,))
    result = (history.retry_missing_accepted_batches(queue, limit=20, max_seconds=30) if lane == 'missing'
              else history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30))
    assert result['indexed'] == 1
    candidate = queue.db.execute('SELECT receipt_path FROM cnyes_history_accepted_batch_candidates '
                                 'WHERE parquet_path=?', (relative,)).fetchone()
    assert candidate['receipt_path'] == new_receipt
    assert not queue.db.execute(f'SELECT 1 FROM {retry_table} WHERE parquet_path=?', (relative,)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_interrupted_initial_batch_slice_rolls_back_candidates_and_checkpoint(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61), batch_tag='a')
    last, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='b')
    source = queue.registry.sources['cnyes']
    marker = history.accepted_batch_scan_key(source)
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = history._accepted_batch_metadata_pairs
    def interrupt(path, source):
        if path == queue.root / last:
            raise RuntimeError('unexpected interruption')
        yield from original(path, source)
    with monkeypatch.context() as patch:
        patch.setattr(history, '_accepted_batch_metadata_pairs', interrupt)
        with pytest.raises(RuntimeError, match='unexpected interruption'):
            history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates').fetchone()
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone()
    assert history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)['indexed'] == 2
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


@pytest.mark.parametrize('lane', ['missing', 'local_io'])
def test_retry_waits_for_batch_registration_to_return(queue, monkeypatch, lane):
    from downloader import news_fulltext_queue as queue_module
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, receipt = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    if lane == 'missing':
        path = queue.root / relative
        held = path.with_suffix('.held')
        path.rename(held)
        history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)
        held.rename(path)
        retry_table = 'cnyes_history_accepted_missing_retries'
    else:
        original = queue_module.replay_source_path
        def unavailable(root, path):
            if path == relative:
                raise OSError(5, 'temporary local I/O')
            return original(root, path)
        with monkeypatch.context() as patch:
            patch.setattr(queue_module, 'replay_source_path', unavailable)
            history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)
        retry_table = 'cnyes_history_accepted_batch_local_retries'
    with queue.db:
        queue.db.execute('DELETE FROM accepted_batches WHERE path=?', (relative,))
        queue.db.execute(f'UPDATE {retry_table} SET not_before_epoch=0 WHERE parquet_path=?', (relative,))
    result = (history.retry_missing_accepted_batches(queue, limit=20, max_seconds=30) if lane == 'missing'
              else history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30))
    assert result['indexed'] == 0
    assert queue.db.execute(f'SELECT 1 FROM {retry_table} WHERE parquet_path=?', (relative,)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates').fetchone()
    with queue.db:
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (relative, receipt))
        queue.db.execute(f'UPDATE {retry_table} SET not_before_epoch=0 WHERE parquet_path=?', (relative,))
    result = (history.retry_missing_accepted_batches(queue, limit=20, max_seconds=30) if lane == 'missing'
              else history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30))
    assert result['indexed'] == 1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_unchanged_invalid_batch_is_rechecked_after_decoder_upgrade(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    path.write_bytes(b'invalid parquet')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    epoch = time.time()
    monkeypatch.setattr(history.time, 'time', lambda: epoch)
    history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)
    reads = []
    original = history._accepted_batch_metadata_pairs
    def counted(*args, **kwargs):
        reads.append(1)
        yield from original(*args, **kwargs)
    monkeypatch.setattr(history, '_accepted_batch_metadata_pairs', counted)
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['processed'] == 1
    assert len(reads) == 1
    monkeypatch.setattr(history.pa, '__version__', str(history.pa.__version__) + '-updated')
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 3604)
    result = history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)
    assert result['processed'] == 1 and result['unchanged'] == 0 and len(reads) == 2
    monkeypatch.setattr(history.time, 'time', lambda: epoch + 7205)
    assert history.retry_invalid_accepted_batches(queue, limit=20, max_seconds=30)['unchanged'] == 1
    assert len(reads) == 2
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_invalid_reader_fingerprint_changes_with_loaded_reader_code(monkeypatch):
    history._accepted_invalid_reader_code_fingerprint.cache_clear()
    before = history._accepted_invalid_reader_code_fingerprint()
    original = history._accepted_batch_metadata_pairs
    try:
        with monkeypatch.context() as patch:
            def changed(*args, **kwargs):
                yield from original(*args, **kwargs)
            patch.setattr(history, '_accepted_batch_metadata_pairs', changed)
            history._accepted_invalid_reader_code_fingerprint.cache_clear()
            assert history._accepted_invalid_reader_code_fingerprint() != before
    finally:
        history._accepted_invalid_reader_code_fingerprint.cache_clear()


@pytest.mark.parametrize('lane', ['initial', 'missing'])
def test_partial_batch_failure_does_not_wake_prior_invalid_work(queue, monkeypatch, lane):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (urls[2],)).fetchone()[0]
    evidence = queue.db.execute('SELECT total,ids_sha256,observed_at_utc FROM bulk_page_evidence '
                                'WHERE scope_url=? AND page=3', (history.cnyes_scope(urls[2]),)).fetchone()
    original_outcome = '{"error":"old_invalid"}'
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state,outcome_json) '
            "VALUES (?,?,?,?,?,?,?,'retained_invalid',?)",
            (urls[2], history.cnyes_scope(urls[2]), 3, sha, *evidence, original_outcome))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    if lane == 'missing':
        held = path.with_suffix('.held')
        path.rename(held)
        history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30)
        held.rename(path)
        table = 'cnyes_history_accepted_missing_retries'
        with queue.db:
            queue.db.execute(f'UPDATE {table} SET not_before_epoch=0')
    else:
        table = 'cnyes_history_accepted_batch_local_retries'
    original = history._accepted_batch_metadata_pairs
    def fail_after_candidate(*args, **kwargs):
        yield from original(*args, **kwargs)
        raise OSError(5, 'temporary failure after candidate')
    with monkeypatch.context() as patch:
        patch.setattr(history, '_accepted_batch_metadata_pairs', fail_after_candidate)
        result = (history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30) if lane == 'initial'
                  else history.retry_missing_accepted_batches(queue, limit=20, max_seconds=30))
    assert result['indexed'] == 0
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates').fetchone()
    work = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()
    assert tuple(work) == ('retained_invalid', original_outcome)
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_outcome_resolutions').fetchone()
    with queue.db:
        queue.db.execute(f'UPDATE {table} SET not_before_epoch=0')
    result = (history.seed_accepted_body_batch_candidates(queue, limit=20, max_seconds=30) if lane == 'initial'
              else history.retry_missing_accepted_batches(queue, limit=20, max_seconds=30))
    assert result['indexed'] == 1
    work = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()
    assert tuple(work) == ('pending', original_outcome)
    resolution = queue.db.execute('SELECT original_state,original_outcome_json,evidence_kind '
                                  'FROM cnyes_history_retained_outcome_resolutions').fetchone()
    assert tuple(resolution) == ('retained_invalid', original_outcome, 'new_accepted_body_batch_candidate')
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
