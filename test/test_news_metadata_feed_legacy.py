"""Legacy RSS originals are indexed locally without changing source heads."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metadata_feeds import LTN_FEEDS, _parse, compatible_fingerprints
from downloader.news_fulltext import run_cycle, run_service
from downloader.news_fulltext_metadata_legacy import backfill, progress
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_metadata_feed_scope import config, setup, xml
from test_news_fulltext import FakeResponse


def one_feed(config, tmp_path):
    cfg = deepcopy(config)
    cfg['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    registry = PolicyRegistry(cfg, base=tmp_path)
    return registry, NewsQueue(tmp_path, registry)


def test_older_legacy_queue_schema_is_extended_without_changing_existing_work():
    from downloader.news_fulltext_metadata_legacy import initialize
    with closing(sqlite3.connect(':memory:')) as db:
        db.executescript("""CREATE TABLE metadata_feed_legacy_originals(
            provider TEXT NOT NULL,feed_url TEXT NOT NULL,fingerprint TEXT NOT NULL,
            response_sha256 TEXT NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
            not_before REAL NOT NULL DEFAULT 0,outcome TEXT,
            PRIMARY KEY(provider,feed_url,fingerprint,response_sha256)) WITHOUT ROWID;
            INSERT INTO metadata_feed_legacy_originals(provider,feed_url,fingerprint,response_sha256,state)
            VALUES ('ltn','feed','fingerprint','digest','pending');""")
        initialize(SimpleNamespace(db=db))
        assert db.execute('SELECT state,original_observed_at_utc,invalid_file_stamp FROM '
                          'metadata_feed_legacy_originals').fetchone() == ('pending', None, None)
        initialize(SimpleNamespace(db=db))


def test_older_completed_directory_schema_keeps_its_state_until_rechecked():
    from downloader.news_fulltext_metadata_legacy import initialize
    with closing(sqlite3.connect(':memory:')) as db:
        db.executescript("""CREATE TABLE metadata_feed_legacy_directories(
            provider TEXT NOT NULL,feed_url TEXT NOT NULL,fingerprint TEXT NOT NULL,
            state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
            not_before REAL NOT NULL DEFAULT 0,outcome TEXT,
            PRIMARY KEY(provider,feed_url,fingerprint)) WITHOUT ROWID;
            INSERT INTO metadata_feed_legacy_directories
            VALUES ('ltn','feed','fingerprint','complete',0,0,'scanned');""")
        initialize(SimpleNamespace(db=db))
        assert db.execute('SELECT state,outcome,not_before,directory_stamp FROM '
                          'metadata_feed_legacy_directories').fetchone() == ('complete', 'scanned', 0, None)
        initialize(SimpleNamespace(db=db))


def test_read_only_audit_refuses_report_symlink_inside_source_root(tmp_path, monkeypatch):
    from scripts import audit_news_metadata_feed_originals as audit_module

    root = tmp_path / 'source'
    root.mkdir()
    outside = tmp_path / 'outside.json'
    outside.write_text('keep')
    output = root / 'report.json'
    output.symlink_to(outside)
    monkeypatch.setattr(sys, 'argv', ['audit', '--root', str(root), '--output', str(output)])
    monkeypatch.setattr(audit_module, 'load_config', lambda _: (None, object(), None))
    monkeypatch.setattr(audit_module, 'audit', lambda *_, **__: {
        'scope_complete': True, 'input_snapshot_stable': True, 'issue_count': 0})

    with pytest.raises(SystemExit) as exc:
        audit_module.main()
    assert exc.value.code == 2
    assert output.is_symlink() and outside.read_text() == 'keep'


def retain(queue, fingerprint, link, *, original_clock='2026-10-13T12:01:00+00:00'):
    source = queue.registry.sources['ltn']
    url = LTN_FEEDS[0]
    body = xml(link=link)
    rows, preservation = _parse(Response(url, 200, {'content-type': 'application/rss+xml'}, body), source, url)
    digest = hashlib.sha256(body).hexdigest()
    relative = f'metadata_feeds/{fingerprint}/{hashlib.sha256(url.encode()).hexdigest()}/{digest}.parquet'
    path = queue.root / relative
    atomic_write_parquet(path, table_from_records([{'feed_url': url, 'provider': source.provider,
        'raw_response_body': body, 'items_json': json.dumps(rows, ensure_ascii=False, sort_keys=True),
        'response_sha256': digest, 'metadata_fingerprint': fingerprint,
        'authorization_json': json.dumps(source.metadata_feed_authorization, ensure_ascii=False, sort_keys=True),
        'observed_at_utc': original_clock, 'content_kind': 'rss_information_not_fulltext',
        'article_bodies_accepted': 0, 'publish': False, 'remote_training_export': False}]), durable=True)
    version = 2 if fingerprint == source.metadata_feed_fingerprint else 1
    receipt = {'contract_version': version, 'parquet_path': relative, 'sha256': sha256_file(path),
        'response_sha256': digest, 'feed_url': url, 'provider': source.provider,
        'metadata_fingerprint': fingerprint, 'rows': len(rows), 'response_bytes': len(body),
        'article_bodies_accepted': 0, 'publish': False, 'remote_training_export': False}
    if version == 2:
        receipt['xml_value_preservation'] = preservation
    atomic_write_json(path.with_suffix('.parquet.receipt.json'), receipt, durable=True)
    return path


def drain(queue, *, limit=100):
    for _ in range(limit):
        result = backfill(queue, queue.registry.sources['ltn'], scan_limit=2, item_limit=2, max_seconds=1)
        if not result['ready']:
            return result
    raise AssertionError('legacy backfill did not drain')


@pytest.mark.parametrize('old_version', [False, True])
def test_legacy_owner_reads_receipt_once_and_preserves_verified_observation(config, tmp_path, monkeypatch, old_version):
    from downloader import news_fulltext_metadata_checkpoint as checkpoint
    from downloader import news_fulltext_metadata_legacy as legacy

    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        fingerprint = compatible_fingerprints(source)[int(old_version)]
        path = retain(queue, fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1905')
        before = (sha256_file(path), sha256_file(path.with_suffix('.parquet.receipt.json')))
        original_reader = checkpoint._receipt
        calls = []

        def counted(*args, **kwargs):
            calls.append((args[1], kwargs.get('required')))
            return original_reader(*args, **kwargs)

        monkeypatch.setattr(checkpoint, '_receipt', counted)
        monkeypatch.setattr(legacy, '_receipt', counted, raising=False)
        assert not drain(queue)['ready']
        assert len(calls) == 1 and calls[0][1] is True
        row = queue.db.execute('SELECT state,original_observed_at_utc FROM metadata_feed_legacy_originals').fetchone()
        assert tuple(row) == ('complete', '2026-10-13T12:01:00+00:00')
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0
        assert before == (sha256_file(path), sha256_file(path.with_suffix('.parquet.receipt.json')))
    finally:
        queue.close()


@pytest.mark.parametrize('change', ['edit', 'replace_same_bytes', 'remove'])
def test_legacy_owner_refuses_receipt_changed_while_original_is_verified(config, tmp_path, monkeypatch, change):
    from downloader import news_fulltext_metadata_feeds as feeds

    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint,
                      'https://news.ltn.com.tw/news/business/breakingnews/1906')
        receipt_path = path.with_suffix('.parquet.receipt.json')
        good_receipt = receipt_path.read_bytes()
        original_parse = feeds._parse
        changed = []

        def change_receipt_during_parse(*args, **kwargs):
            result = original_parse(*args, **kwargs)
            if not changed:
                if change == 'edit':
                    with receipt_path.open('ab') as receipt:
                        receipt.write(b' ')
                elif change == 'replace_same_bytes':
                    replacement = receipt_path.with_name(receipt_path.name + '.replacement')
                    replacement.write_bytes(good_receipt)
                    replacement.replace(receipt_path)
                else:
                    receipt_path.unlink()
                changed.append(True)
            return result

        monkeypatch.setattr(feeds, '_parse', change_receipt_during_parse)
        drain(queue)
        assert changed
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 0
        assert tuple(queue.db.execute('SELECT state,outcome FROM metadata_feed_legacy_originals').fetchone()) == ('retry', 'OSError')
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
        if not receipt_path.exists():
            receipt_path.write_bytes(good_receipt)
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='retry'")
        assert not drain(queue)['ready']
        assert tuple(queue.db.execute('SELECT state,original_observed_at_utc FROM metadata_feed_legacy_originals').fetchone()) == (
            'complete', '2026-10-13T12:01:00+00:00')
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
    finally:
        queue.close()


@pytest.mark.parametrize('size', [None, True, -1, '3', (32 << 20) + 1])
def test_legacy_owner_rejects_invalid_receipt_size_before_reading_parquet(config, tmp_path, monkeypatch, size):
    from downloader import news_fulltext_metadata_checkpoint as checkpoint

    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint,
                      'https://news.ltn.com.tw/news/business/breakingnews/1907')
        receipt_path = path.with_suffix('.parquet.receipt.json')
        receipt = json.loads(receipt_path.read_text())
        receipt['response_bytes'] = size
        atomic_write_json(receipt_path, receipt)

        def unexpected_reader(*args, **kwargs):
            raise AssertionError('invalid response size must be rejected before reading original')

        monkeypatch.setattr(checkpoint.pq, 'ParquetFile', unexpected_reader)
        drain(queue)
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'invalid'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        queue.close()


def test_read_only_original_audit_checks_both_versions_and_refuses_incomplete_or_bad_receipts(config, tmp_path):
    from scripts.audit_news_metadata_feed_originals import audit
    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        current, old = compatible_fingerprints(source)
        first = retain(queue, current, 'https://news.ltn.com.tw/news/business/breakingnews/1901')
        second = retain(queue, old, 'https://news.ltn.com.tw/news/business/breakingnews/1902')
        changes = queue.db.total_changes
        result = audit(tmp_path, registry, 'ltn')
        assert result['scope_complete'] and result['input_snapshot_stable']
        assert result['counts']['verified_originals'] == 2 and result['issue_count'] == 0
        assert result['counts']['metadata_rows'] == 2
        assert result['publisher_requests'] == result['production_queue_writes'] == 0
        assert queue.db.total_changes == changes
        assert not audit(tmp_path, registry, 'ltn', max_files=1)['scope_complete']
        receipt_path = second.with_suffix('.parquet.receipt.json')
        receipt = json.loads(receipt_path.read_text())
        receipt['rows'] += 1
        atomic_write_json(receipt_path, receipt)
        bad = audit(tmp_path, registry, 'ltn')
        assert bad['scope_complete'] and bad['counts']['verified_originals'] == 1
        assert bad['issue_count'] == 1 and bad['issues'][0]['reason'] == 'invalid_metadata_receipt'
        assert first.exists() and second.exists() and queue.db.total_changes == changes
        receipt_path.unlink()
        missing = audit(tmp_path, registry, 'ltn')
        assert not missing['scope_complete']
        assert any(issue['reason'] == 'missing_receipt' for issue in missing['issues'])
    finally:
        queue.close()


def test_read_only_original_audit_enforces_owner_response_size_before_reading(config, tmp_path, monkeypatch):
    from downloader.news_fulltext_metadata_checkpoint import MAX_BODY_BYTES
    from scripts import audit_news_metadata_feed_originals as audit_module
    registry, queue = one_feed(config, tmp_path)
    try:
        path = retain(queue, registry.sources['ltn'].metadata_feed_fingerprint,
                      'https://news.ltn.com.tw/news/business/breakingnews/1904')
        receipt_path = path.with_suffix('.parquet.receipt.json')
        receipt = json.loads(receipt_path.read_text())
        receipt['response_bytes'] = MAX_BODY_BYTES + 1
        atomic_write_json(receipt_path, receipt)
        def unexpected_reader(*args, **kwargs):
            raise AssertionError('oversize original should not be read')
        monkeypatch.setattr(audit_module, '_original', unexpected_reader)
        result = audit_module.audit(tmp_path, registry, 'ltn')
        assert result['counts']['inspected_originals'] == 1
        assert result['issue_count'] == 1
        assert result['issues'][0]['reason'] == 'invalid_metadata_legacy_response_size'
    finally:
        queue.close()


def test_read_only_original_audit_detects_a_receipt_changed_after_validation(config, tmp_path, monkeypatch):
    from scripts import audit_news_metadata_feed_originals as audit_module
    registry, queue = one_feed(config, tmp_path)
    try:
        path = retain(queue, registry.sources['ltn'].metadata_feed_fingerprint,
                      'https://news.ltn.com.tw/news/business/breakingnews/1903')
        original_reader = audit_module._original
        def change_after_read(*args, **kwargs):
            result = original_reader(*args, **kwargs)
            with path.with_suffix('.parquet.receipt.json').open('ab') as receipt:
                receipt.write(b' ')
            return result
        monkeypatch.setattr(audit_module, '_original', change_after_read)
        result = audit_module.audit(tmp_path, registry, 'ltn')
        assert result['counts']['verified_originals'] == 1
        assert result['changed_files'] == 1 and not result['scope_complete']
        assert not result['input_snapshot_stable']
    finally:
        queue.close()


def test_indexes_both_native_versions_without_changing_existing_heads_or_states(config, tmp_path):
    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        current, legacy = compatible_fingerprints(source)
        existing = 'https://news.ltn.com.tw/news/business/breakingnews/123'
        new = 'https://news.ltn.com.tw/news/business/breakingnews/456'
        originals = [retain(queue, legacy, existing), retain(queue, current, new)]
        with queue.db:
            queue.add_articles([{'url': existing, 'title': '較新的標題'}])
            queue.db.execute("UPDATE articles SET error_code='http_429',attempts=3,due=12345 WHERE url=?", (existing,))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts) VALUES (?, 'metadata_feed','ltn',12345,'http_429',3)", (LTN_FEEDS[0],))
        before_head = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (LTN_FEEDS[0],)).fetchone())
        before_article = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (existing,)).fetchone())
        hashes = [sha256_file(path) for path in originals]
        recovery_start = datetime.now(timezone.utc)
        assert not drain(queue)['ready']
        recovery_end = datetime.now(timezone.utc)
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 2
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (LTN_FEEDS[0],)).fetchone()) == before_head
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (existing,)).fetchone()) == before_article
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (new,)).fetchone()[0] == 'awaiting_authorization'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_heads').fetchone()[0] == 0
        assert queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0
        assert all(row[0] == '2026-10-13T12:01:00+00:00' for row in
                   queue.db.execute('SELECT original_observed_at_utc FROM metadata_feed_legacy_originals'))
        assert all(recovery_start <= datetime.fromisoformat(row[0].replace('Z', '+00:00')) <= recovery_end for row in
                   queue.db.execute('SELECT processed_at_utc FROM metadata_feed_observations'))
        assert [sha256_file(path) for path in originals] == hashes
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 2
    finally:
        queue.close()


def test_scan_resumes_after_restart_without_skipping_unfinished_files(config, tmp_path):
    registry, queue = one_feed(config, tmp_path)
    current = registry.sources['ltn'].metadata_feed_fingerprint
    for identifier in range(5):
        retain(queue, current, f'https://news.ltn.com.tw/news/business/breakingnews/{identifier + 1000}')
    first = backfill(queue, registry.sources['ltn'], scan_limit=1, item_limit=1, max_seconds=1)
    assert first['ready'] and first['scanned'] <= 1 and first['processed'] <= 1
    queue.close()
    with closing(NewsQueue(tmp_path, registry)) as restored:
        assert not drain(restored)['ready']
        assert restored.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 5
        assert restored.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 5


def test_changed_directory_is_rescanned_and_new_original_is_not_missed(config, tmp_path):
    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        current = source.metadata_feed_fingerprint
        retain(queue, current, 'https://news.ltn.com.tw/news/business/breakingnews/1201')
        for _ in range(5):
            result = backfill(queue, source, scan_limit=1, item_limit=1, max_seconds=1)
            scan = getattr(queue, '_metadata_legacy_scan', None)
            if scan is not None and scan['row']['fingerprint'] == current and result['scanned']:
                break
        else:
            raise AssertionError('current directory iterator did not start')
        assert result['ready']
        retain(queue, current, 'https://news.ltn.com.tw/news/business/breakingnews/1202')
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 2
    finally:
        queue.close()


@pytest.mark.parametrize('had_directory', [False, True])
@pytest.mark.parametrize('restart', [False, True])
def test_completed_directory_discovers_later_restored_original_without_reprocessing(
        config, tmp_path, monkeypatch, had_directory, restart):
    from downloader import news_fulltext_metadata_legacy as legacy

    registry, queue = one_feed(config, tmp_path)
    source = registry.sources['ltn']
    first = 'https://news.ltn.com.tw/news/business/breakingnews/1211'
    added = 'https://news.ltn.com.tw/news/business/breakingnews/1212'
    try:
        if had_directory:
            retain(queue, source.metadata_feed_fingerprint, first)
        result = drain(queue)
        initial_recheck = result['next_retry_epoch']
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == int(had_directory)

        if restart:
            queue.close()
            queue = NewsQueue(tmp_path, registry)
        retain(queue, source.metadata_feed_fingerprint, added)

        calls = []
        original = legacy._recover_candidate

        def counted_recovery(queue_arg, row, source_arg):
            calls.append(row['response_sha256'])
            return original(queue_arg, row, source_arg)

        monkeypatch.setattr(legacy, '_recover_candidate', counted_recovery)
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_directories SET not_before=0 WHERE state='complete'")
        result = drain(queue)
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1 + int(had_directory)
        assert queue.db.execute('SELECT count(*) FROM articles WHERE url=?', (added,)).fetchone()[0] == 1
        assert len(calls) == 1  # Already indexed originals are not parsed again.
        assert progress(queue.db, source)['discovery_complete']
        assert initial_recheck is not None and initial_recheck > time.time()
        assert result['next_retry_epoch'] is not None and result['next_retry_epoch'] > time.time()

        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_directories SET not_before=0 WHERE state='complete'")
        assert not drain(queue)['ready']
        assert len(calls) == 1  # An unchanged directory only needs a local metadata check.
    finally:
        queue.close()


def test_old_completed_directory_without_stamp_is_rescanned_once(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_legacy as legacy

    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1221')
        drain(queue)
        retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1222')
        with queue.db:
            queue.db.execute('UPDATE metadata_feed_legacy_directories SET directory_stamp=NULL,not_before=0 '
                             'WHERE fingerprint=?', (source.metadata_feed_fingerprint,))
        calls = []
        original = legacy._recover_candidate
        def counted_recovery(queue_arg, row, source_arg):
            calls.append(row['response_sha256'])
            return original(queue_arg, row, source_arg)
        monkeypatch.setattr(legacy, '_recover_candidate', counted_recovery)
        assert not drain(queue)['ready']
        assert len(calls) == 1
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 2
        assert queue.db.execute('SELECT directory_stamp FROM metadata_feed_legacy_directories WHERE fingerprint=?',
                                (source.metadata_feed_fingerprint,)).fetchone()[0] is not None
    finally:
        queue.close()


def test_directory_recheck_failure_retries_without_overwriting_indexed_history(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_legacy as legacy

    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1231')
        drain(queue)
        with queue.db:
            queue.db.execute('UPDATE metadata_feed_legacy_directories SET not_before=0 WHERE fingerprint=?',
                             (source.metadata_feed_fingerprint,))
        original = legacy._directory_stamp
        attempts = []
        def temporary_failure(path):
            attempts.append(1)
            if len(attempts) == 1:
                raise OSError('temporary local directory failure')
            return original(path)
        monkeypatch.setattr(legacy, '_directory_stamp', temporary_failure)
        result = backfill(queue, source, max_seconds=1)
        row = queue.db.execute('SELECT state,attempts,not_before,outcome FROM metadata_feed_legacy_directories '
                               'WHERE fingerprint=?', (source.metadata_feed_fingerprint,)).fetchone()
        assert row['state'] == 'retry' and row['attempts'] == 1 and row['outcome'] == 'OSError'
        assert row['not_before'] > time.time() and result['next_retry_epoch'] == row['not_before']
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
        with queue.db:
            queue.db.execute('UPDATE metadata_feed_legacy_directories SET not_before=0 WHERE fingerprint=?',
                             (source.metadata_feed_fingerprint,))
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_directories WHERE fingerprint=?',
                                (source.metadata_feed_fingerprint,)).fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
    finally:
        queue.close()


@pytest.mark.parametrize('already_completed', [False, True])
def test_directory_symlink_loop_is_retryable_without_stopping_owner(config, tmp_path, already_completed):
    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        current = source.metadata_feed_fingerprint
        if already_completed:
            drain(queue)
            with queue.db:
                queue.db.execute('UPDATE metadata_feed_legacy_directories SET not_before=0 WHERE fingerprint=?', (current,))
        parent = queue.root / 'metadata_feeds'
        parent.mkdir(exist_ok=True)
        loop = parent / current
        loop.symlink_to(current)

        assert not drain(queue)['ready']
        failed = queue.db.execute('SELECT state,outcome,not_before FROM metadata_feed_legacy_directories '
                                  'WHERE fingerprint=?', (current,)).fetchone()
        assert failed['state'] == 'retry' and failed['outcome'] in {'ValueError', 'OSError'}
        assert failed['not_before'] > time.time()
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 0
        loop.unlink()
        with queue.db:
            queue.db.execute('UPDATE metadata_feed_legacy_directories SET not_before=0 WHERE fingerprint=?', (current,))
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_directories '
                                'WHERE fingerprint=?', (current,)).fetchone()[0] == 'complete'
    finally:
        queue.close()


def test_outdated_feed_scope_does_not_process_or_count_old_candidates(config, tmp_path):
    registry, queue = one_feed(config, tmp_path)
    try:
        old = registry.sources['ltn']
        path = retain(queue, old.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1601')
        with queue.db:
            queue.db.execute('INSERT INTO metadata_feed_legacy_originals '
                             '(provider,feed_url,fingerprint,response_sha256,state) VALUES (?,?,?,?,?)',
                             ('ltn', LTN_FEEDS[0], old.metadata_feed_fingerprint, path.stem, 'pending'))
        new = replace(old, metadata_feeds=(LTN_FEEDS[1],))
        registry.sources['ltn'] = new
        for _ in range(10):
            result = backfill(queue, new, max_seconds=1)
            if not result['ready']:
                break
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'pending'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 0
        snapshot = progress(queue.db, new)
        assert snapshot['candidate_originals'] == 0 and snapshot['discovery_complete']
    finally:
        queue.close()


def test_receipt_symlink_outside_store_is_invalid_and_never_imported(config, tmp_path):
    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1701')
        receipt = path.with_suffix('.parquet.receipt.json')
        outside = tmp_path.parent / (tmp_path.name + '-outside-receipt.json')
        outside.write_bytes(receipt.read_bytes())
        receipt.unlink()
        receipt.symlink_to(outside)
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'invalid'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 0
    finally:
        queue.close()


def test_resident_owner_indexes_without_http_and_exposes_bounded_progress(config, tmp_path):
    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    queue, http, session = setup(config, tmp_path, {})
    try:
        source = queue.registry.sources['ltn']
        retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1801')
        snapshot = run_cycle(queue, http, config, max_articles=0, max_discovery_requests=0,
                             continuous_budgets=True, max_run_seconds=8)
        assert session.calls == []
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1
        history = snapshot['providers']['ltn']['metadata_feed_progress']['legacy_original_index']
        assert history['discovery_complete'] and history['examined_directories'] == history['expected_directories'] == 2
        assert history['candidate_originals'] == history['verified_originals'] == 1
        assert history['pending_originals'] == history['retry_originals'] == history['invalid_originals'] == 0
        assert snapshot['providers']['ltn']['metadata_feed_progress']['article_bodies_accepted'] == 0
    finally:
        http.close()
        queue.close()


def test_resident_owner_wakes_for_local_retry_without_a_publisher_request(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_legacy as legacy
    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    queue, http, session = setup(config, tmp_path, {})
    original = legacy._recover_candidate
    attempts = []
    def transient_read_failure(*args):
        attempts.append(1)
        if len(attempts) == 1:
            raise OSError('temporary local read failure')
        return original(*args)
    monkeypatch.setattr(legacy, '_recover_candidate', transient_read_failure)
    try:
        source = queue.registry.sources['ltn']
        retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1802')
        run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=3.5)
        assert len(attempts) == 2 and session.calls == []
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
    finally:
        http.close()
        queue.close()


def test_resident_owner_wakes_to_discover_a_later_local_archive(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_legacy as legacy

    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    queue, http, session = setup(config, tmp_path, {})
    monkeypatch.setattr(legacy, 'DIRECTORY_RECHECK_SECONDS', .25)
    try:
        source = queue.registry.sources['ltn']
        drain(queue)
        retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1803')
        run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=2)
        assert session.calls == []
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1
    finally:
        http.close()
        queue.close()


def test_slow_legacy_backfill_does_not_start_error_rollup_after_deadline(config, tmp_path, monkeypatch):
    from downloader import news_fulltext as worker, news_fulltext_metadata_legacy as legacy
    from test_news_provider_cooperation import AdvancingCycleClock

    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    queue, http, session = setup(config, tmp_path, {})
    clock, stages = AdvancingCycleClock(), []

    def slow_backfill(*args, **kwargs):
        stages.append('legacy')
        clock.advance += 10
        return {'next_retry_epoch': None, 'ready': False}

    monkeypatch.setattr(worker, 'time', clock)
    monkeypatch.setattr(worker, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(worker, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr(legacy, 'backfill', slow_backfill)
    monkeypatch.setattr(queue, 'advance_discovery_error_rollup', lambda: stages.append('rollup'))
    try:
        result = run_cycle(queue, http, config, max_articles=0, max_discovery_requests=0,
                           continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert stages == ['legacy']
        assert not session.calls and result['cycle']['http_requests'].get('ltn', 0) == 0
    finally:
        http.close()
        queue.close()


def test_legacy_rss_recovery_hands_off_other_provider_body_before_next_slice(config, tmp_path, monkeypatch):
    """Real retained RSS recovery stays local and cannot delay a received body."""
    from concurrent.futures import Future
    from downloader import news_fulltext as worker, news_fulltext_metadata_legacy as legacy
    from downloader.news_fulltext_queue import verify_bodies
    from test_news_fulltext import article_html, config as publisher_config, http_fixture

    class ImmediateExecutor:
        def submit(self, function, *args):
            future = Future()
            try:
                future.set_result(function(*args))
            except Exception as error:
                future.set_exception(error)
            return future

    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    config['providers'].append(deepcopy(publisher_config.__wrapped__()['providers'][0]))
    config.update(batch_articles=64, batch_text_bytes=4 << 20, batch_raw_response_bytes=16 << 20,
                  batch_flush_seconds=3600)
    url = 'https://publisher.test/article/1'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        url: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(worker, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr(worker, 'seed_discovery', lambda *_: None)
    try:
        source = queue.registry.sources['ltn']
        originals = [retain(queue, source.metadata_feed_fingerprint,
                            f'https://news.ltn.com.tw/news/business/breakingnews/{number}')
                     for number in (1901, 1902, 1903)]
        before = [(sha256_file(path), sha256_file(path.with_suffix('.parquet.receipt.json'))) for path in originals]
        with queue.db:
            queue.add_articles([{'url': url}])
        calls, observed = [], []
        real_backfill = legacy.backfill

        def one_original_slice(q, current_source, **kwargs):
            if calls:
                observed.append((q.db.execute('SELECT state,body_sha256 FROM articles WHERE url=?', (url,)).fetchone(),
                                 q.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]))
            result = real_backfill(q, current_source, scan_limit=256, item_limit=1, max_seconds=1, **kwargs)
            calls.append(result)
            if len(calls) == 2:
                http.stop.set()
            return result

        monkeypatch.setattr(legacy, 'backfill', one_original_slice)
        worker.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                         executor=ImmediateExecutor(), provider_ids={'ltn', 'publisher'},
                         continuous_budgets=True, max_run_seconds=5, defer_final_status=True)
        assert len(calls) == 2 and calls[0]['ready']
        head, batches = observed[0]
        assert head['state'] == 'complete' and head['body_sha256'] and batches == 1
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 2
        assert queue.db.execute("SELECT count(*) FROM articles WHERE provider='ltn' AND body_sha256 IS NOT NULL").fetchone()[0] == 0
        assert before == [(sha256_file(path), sha256_file(path.with_suffix('.parquet.receipt.json'))) for path in originals]
        assert [called for called, _ in session.calls] == ['https://publisher.test/robots.txt', url]
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('scanned,processed,expect_pause', [(1, 0, False), (0, 1, False), (0, 0, True)])
def test_legacy_ready_slice_continues_after_progress_but_backs_off_without_progress(
        config, tmp_path, monkeypatch, scanned, processed, expect_pause):
    from downloader import news_fulltext as worker, news_fulltext_metadata_legacy as legacy
    from test_news_provider_cooperation import AdvancingCycleClock

    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    queue, http, session = setup(config, tmp_path, {})
    clock, calls, waits = AdvancingCycleClock(), [], []

    def local_slice(*args, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            http.stop.set()
        return {'ready': True, 'next_retry_epoch': 0, 'scanned': scanned, 'processed': processed}

    def measured_wait(delay):
        waits.append(delay)
        clock.advance += delay
        return False

    monkeypatch.setattr(worker, 'time', clock)
    monkeypatch.setattr(worker, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr(worker, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(http.stop, 'wait', measured_wait)
    monkeypatch.setattr(legacy, 'backfill', local_slice)
    try:
        run_cycle(queue, http, config, max_articles=0, max_discovery_requests=0,
                  provider_ids={'ltn'}, continuous_budgets=True, max_run_seconds=5, defer_final_status=True)
        assert len(calls) == 2 and waits
        if expect_pause:
            assert waits[0] >= .045
        else:
            assert waits[0] == 0
        assert not session.calls
    finally:
        http.close()
        queue.close()


def test_ready_legacy_slices_leave_next_dispatch_open_for_another_provider(config, tmp_path, monkeypatch):
    from concurrent.futures import Future
    from downloader import news_fulltext as worker, news_fulltext_metadata_legacy as legacy
    from downloader.news_fulltext_queue import verify_bodies
    from test_news_fulltext import article_html, config as publisher_config, http_fixture

    class ImmediateExecutor:
        def submit(self, function, *args):
            future = Future()
            try:
                future.set_result(function(*args))
            except Exception as error:
                future.set_exception(error)
            return future

    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    config['providers'].append(deepcopy(publisher_config.__wrapped__()['providers'][0]))
    url = 'https://publisher.test/article/1'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        url: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    calls = []

    def productive_slice(q, source, **kwargs):
        calls.append(source.provider)
        if len(calls) == 1:
            with q.db:
                q.add_articles([{'url': url}])
        else:
            assert [called for called, _ in session.calls] == ['https://publisher.test/robots.txt', url]
            http.stop.set()
        return {'ready': True, 'next_retry_epoch': 0, 'scanned': 1, 'processed': 0}

    monkeypatch.setattr(worker, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr(worker, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(legacy, 'backfill', productive_slice)
    try:
        run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                  executor=ImmediateExecutor(), provider_ids={'ltn', 'publisher'},
                  continuous_budgets=True, max_run_seconds=5, defer_final_status=True)
        assert calls == ['ltn', 'ltn']
        head = queue.db.execute('SELECT state,body_sha256 FROM articles WHERE url=?', (url,)).fetchone()
        assert head['state'] == 'complete' and head['body_sha256']
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        http.close()
        queue.close()


def test_one_shot_metadata_request_waits_instead_of_spinning_on_resident_legacy_work(config, tmp_path, monkeypatch):
    from downloader import news_fulltext as worker
    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    routes = {'https://news.ltn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
              LTN_FEEDS[0]: FakeResponse(xml(), headers={'Content-Type': 'application/rss+xml'})}
    queue, http, session = setup(config, tmp_path, routes)
    original_get, original_wait = session.get, worker.wait
    waits = []
    def slow_get(url, **kwargs):
        if url == LTN_FEEDS[0]:
            time.sleep(.08)
        return original_get(url, **kwargs)
    def measured_wait(futures, *, timeout, return_when):
        waits.append(timeout)
        return original_wait(futures, timeout=timeout, return_when=return_when)
    monkeypatch.setattr(session, 'get', slow_get)
    monkeypatch.setattr(worker, 'wait', measured_wait)
    try:
        run_cycle(queue, http, config, max_articles=0, max_discovery_requests=1)
        assert any(timeout > 0 for timeout in waits)
        assert len(waits) < 20
    finally:
        http.close()
        queue.close()


def test_bad_receipt_is_recorded_without_preventing_other_originals(config, tmp_path):
    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        current = source.metadata_feed_fingerprint
        bad = retain(queue, current, 'https://news.ltn.com.tw/news/business/breakingnews/1401')
        retain(queue, current, 'https://news.ltn.com.tw/news/business/breakingnews/1402')
        receipt = bad.with_suffix('.parquet.receipt.json')
        value = json.loads(receipt.read_text())
        value['sha256'] = '0' * 64
        receipt.write_text(json.dumps(value))
        assert not drain(queue)['ready']
        states = dict(queue.db.execute('SELECT state,count(*) FROM metadata_feed_legacy_originals GROUP BY state'))
        assert states == {'complete': 1, 'invalid': 1}
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1
    finally:
        queue.close()


@pytest.mark.parametrize('restart', [False, True])
def test_invalid_receipt_repaired_in_place_is_rechecked_without_reparsing_unchanged(config, tmp_path, monkeypatch, restart):
    from downloader import news_fulltext_metadata_legacy as legacy

    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1451')
        receipt = path.with_suffix('.parquet.receipt.json')
        good = receipt.read_bytes()
        value = json.loads(good)
        value['sha256'] = '0' * 64
        receipt.write_text(json.dumps(value))
        calls = []
        original = legacy._recover_candidate
        def counted_recovery(queue_arg, row, source_arg):
            calls.append(row['response_sha256'])
            return original(queue_arg, row, source_arg)
        monkeypatch.setattr(legacy, '_recover_candidate', counted_recovery)

        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'invalid'
        assert len(calls) == 1
        # Confirm the original failure against an unchanged pair once; after
        # that, unchanged files should require only a metadata comparison.
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='invalid'")
        assert not drain(queue)['ready']
        assert len(calls) == 2
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'invalid'
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='invalid'")
        assert not drain(queue)['ready']
        assert len(calls) == 2

        # A different feed original appearing in the same directory is not a
        # change to this invalid original/receipt pair.
        retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1453')
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='invalid'")
        assert not drain(queue)['ready']
        assert len(calls) == 2
        if restart:
            queue.close()
            queue = NewsQueue(tmp_path, registry)
        before_directory = legacy._directory_stamp(path.parent)
        receipt.write_bytes(good)  # In-place repair does not change the directory entry.
        assert legacy._directory_stamp(path.parent) == before_directory
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='invalid'")
        assert not drain(queue)['ready']
        assert len(calls) == 3
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_legacy_invalid_history').fetchone()[0] == 2
    finally:
        queue.close()


def test_old_invalid_outcome_is_preserved_without_inventing_a_timestamp(config, tmp_path):
    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1454')
        receipt = path.with_suffix('.parquet.receipt.json')
        good = receipt.read_bytes()
        value = json.loads(good)
        value['sha256'] = '0' * 64
        receipt.write_text(json.dumps(value))
        drain(queue)
        with queue.db:
            # An old schema had a single invalid state but no event history.
            queue.db.execute('DELETE FROM metadata_feed_legacy_invalid_history')
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0,invalid_file_stamp=NULL WHERE state='invalid'")
        receipt.write_bytes(good)
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'complete'
        record = queue.db.execute('SELECT observed_at_utc,reason,event_kind FROM metadata_feed_legacy_invalid_history').fetchone()
        assert tuple(record) == (None, 'ValueError', 'legacy_state')
    finally:
        queue.close()


def test_invalid_receipt_recheck_preserves_a_racing_repair_as_retry(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_legacy as legacy

    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1452')
        receipt = path.with_suffix('.parquet.receipt.json')
        good = receipt.read_bytes()
        value = json.loads(good)
        value['sha256'] = '0' * 64
        receipt.write_text(json.dumps(value))
        drain(queue)
        original = legacy._recover_candidate
        def repaired_during_read(*args):
            receipt.write_bytes(good)
            raise ValueError('invalid_metadata_receipt')
        monkeypatch.setattr(legacy, '_recover_candidate', repaired_during_read)
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='invalid'")
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'retry'
        monkeypatch.setattr(legacy, '_recover_candidate', original)
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='retry'")
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_legacy_invalid_history').fetchone()[0] == 2
    finally:
        queue.close()


def test_invalid_file_metadata_errors_back_off_without_erasing_failure(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_legacy as legacy

    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1456')
        receipt = path.with_suffix('.parquet.receipt.json')
        good = receipt.read_bytes()
        value = json.loads(good)
        value['sha256'] = '0' * 64
        receipt.write_text(json.dumps(value))
        drain(queue)
        original = legacy._candidate_file_marker
        def unavailable(*args):
            raise OSError('temporary local metadata error')
        monkeypatch.setattr(legacy, '_candidate_file_marker', unavailable)
        for attempts in (1, 2):
            with queue.db:
                queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='invalid'")
            before = time.time()
            result = backfill(queue, source, max_seconds=1)
            row = queue.db.execute('SELECT state,attempts,not_before,outcome FROM metadata_feed_legacy_originals').fetchone()
            assert row['state'] == 'invalid' and row['attempts'] == attempts and row['outcome'] == 'ValueError'
            assert row['not_before'] >= before + 2**attempts and result['next_retry_epoch'] == row['not_before']
        monkeypatch.setattr(legacy, '_candidate_file_marker', original)
        receipt.write_bytes(good)
        with queue.db:
            queue.db.execute("UPDATE metadata_feed_legacy_originals SET not_before=0 WHERE state='invalid'")
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_legacy_invalid_history').fetchone()[0] == 1
    finally:
        queue.close()


def test_resident_owner_wakes_after_invalid_receipt_is_repaired(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_legacy as legacy

    config = deepcopy(config)
    config['providers'][0]['metadata_feed_policy']['urls'] = [LTN_FEEDS[0]]
    queue, http, session = setup(config, tmp_path, {})
    invalid_seen = threading.Event()
    original = legacy._recover_candidate

    def signal_invalid(*args):
        try:
            return original(*args)
        except ValueError:
            invalid_seen.set()
            raise

    monkeypatch.setattr(legacy, '_recover_candidate', signal_invalid)
    thread = None
    try:
        source = queue.registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1455')
        receipt = path.with_suffix('.parquet.receipt.json')
        good = receipt.read_bytes()
        value = json.loads(good)
        value['sha256'] = '0' * 64
        receipt.write_text(json.dumps(value))
        def restore():
            assert invalid_seen.wait(timeout=3)
            receipt.write_bytes(good)
        thread = threading.Thread(target=restore)
        thread.start()
        run_service(queue, http, config, max_articles=0, max_discovery_requests=0, duration_seconds=3.5)
        thread.join(timeout=1)
        assert not thread.is_alive() and invalid_seen.is_set()
        assert session.calls == []
        assert queue.db.execute('SELECT state FROM metadata_feed_legacy_originals').fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_legacy_invalid_history').fetchone()[0] == 1
    finally:
        if thread is not None:
            thread.join(timeout=3)
        http.close()
        queue.close()


def test_temporary_missing_receipt_retries_without_redownloading(config, tmp_path):
    registry, queue = one_feed(config, tmp_path)
    try:
        source = registry.sources['ltn']
        path = retain(queue, source.metadata_feed_fingerprint, 'https://news.ltn.com.tw/news/business/breakingnews/1501')
        receipt = path.with_suffix('.parquet.receipt.json')
        saved = receipt.read_bytes()
        receipt.unlink()
        result = drain(queue)
        assert result['next_retry_epoch'] is not None
        assert queue.db.execute("SELECT state FROM metadata_feed_legacy_originals").fetchone()[0] == 'retry'
        receipt.write_bytes(saved)
        with queue.db:
            queue.db.execute('UPDATE metadata_feed_legacy_originals SET not_before=0')
        assert not drain(queue)['ready']
        assert queue.db.execute('SELECT count(*) FROM metadata_feed_observations').fetchone()[0] == 1
    finally:
        queue.close()
