"""Bounded receipt lookup preserves exact crash-replay validation and order."""
import json
import shutil

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_queue import sorted_acceptance_receipt_paths
from test_news_fulltext import config, http_fixture, FakeResponse, article_html


URL = 'https://publisher.test/news/1'


def test_receipt_names_keep_exact_glob_set_and_path_order_including_invalid_children(tmp_path):
    directory = tmp_path / 'receipts'
    directory.mkdir()
    for name in ['.hidden.json', 'a.json', 'A.json', '中.json', 'é.json', 'e\u0301.json',
                 '0.json', 'z.json', 'upper.JSON', 'not-json']:
        (directory / name).write_text('{}')
    (directory / 'directory.json').mkdir()
    (directory / 'link.json').symlink_to(directory / 'a.json')
    (directory / 'broken.json').symlink_to(directory / 'absent')
    actual = sorted_acceptance_receipt_paths(tmp_path)
    assert not isinstance(actual, list)
    assert list(actual) == sorted(directory.glob('*.json'))


@pytest.mark.parametrize('as_file', [False, True])
def test_absent_or_non_directory_receipt_root_has_legacy_empty_frontier(tmp_path, as_file):
    if as_file:
        (tmp_path / 'receipts').write_text('not a directory')
    assert list(sorted_acceptance_receipt_paths(tmp_path)) == list((tmp_path / 'receipts').glob('*.json')) == []


def test_invalid_json_directory_is_not_filtered_out_as_success(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        (tmp_path / 'receipts' / 'zz-invalid.json').mkdir()
        with pytest.raises(IsADirectoryError):
            queue.recover_acceptance()
    finally:
        queue.close()
        http.close()


def accepted_queue(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    queue.add_articles([{'url': URL}])
    queue.accept([fetch_article(http, queue.claim_url(URL, 300))])
    return queue, http


def recover_with_benchmark_readers(queue, readers):
    if readers:
        from scripts.benchmark_news_acceptance_recovery import _prefetch_recovery
        _prefetch_recovery(queue, sorted((queue.root / 'receipts').glob('*.json')), readers)
    else:
        queue.recover_acceptance()


def test_committed_receipts_are_still_parsed_but_use_one_covering_lookup(config, tmp_path, monkeypatch):
    queue, http = accepted_queue(config, tmp_path)
    try:
        reads, statements = [], []
        original_read = type(tmp_path).read_bytes
        def read(path, *args, **kwargs):
            reads.append(path)
            return original_read(path, *args, **kwargs)
        monkeypatch.setattr(type(tmp_path), 'read_bytes', read)
        before = queue.db.total_changes
        queue.db.set_trace_callback(statements.append)
        queue.recover_acceptance()
        assert queue.db.total_changes == before
        assert len(reads) == 1
        assert len([s for s in statements if s.startswith('SELECT receipt_path,path')]) == 1
        assert not any(s.startswith('SELECT 1 FROM accepted_batches WHERE path=') for s in statements)
        plan = queue.db.execute('EXPLAIN QUERY PLAN SELECT receipt_path,path FROM accepted_batches '
                               'INDEXED BY accepted_batches_receipt_path WHERE receipt_path IN (?)',
                               (str(reads[0].relative_to(tmp_path)),)).fetchall()
        assert any('COVERING INDEX accepted_batches_receipt_path' in row[-1] for row in plan)
    finally:
        queue.db.set_trace_callback(None)
        queue.close()
        http.close()


def test_malformed_committed_receipt_still_fails_closed(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        receipt = next((tmp_path / 'receipts').glob('*.json'))
        # Generated corruption fixture, not a production observation.
        atomic_write_json(receipt, {'not_the_receipt_identity': True})
        with pytest.raises(KeyError, match='parquet_path'):
            queue.recover_acceptance()
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('readers', [0, 2, 4])
@pytest.mark.parametrize('corruption', ['', 'digest', 'rows', 'contract'])
def test_uncommitted_receipt_keeps_full_digest_row_and_contract_validation(config, tmp_path, corruption, readers):
    queue, http = accepted_queue(config, tmp_path)
    try:
        path, relative_receipt = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        receipt_path = tmp_path / relative_receipt
        receipt = json.loads(receipt_path.read_text())
        if corruption == 'digest':
            receipt['sha256'] = '0' * 64
        if corruption == 'rows':
            receipt['rows'] += 1
        if corruption == 'contract':
            receipt['contract_version'] = -1
        atomic_write_json(receipt_path, receipt)
        with queue.db:
            queue.db.execute('DELETE FROM accepted_batches WHERE path=?', (path,))
            queue.db.execute("UPDATE articles SET state='pending',body_sha256=NULL,body_bytes=NULL,parquet_path=NULL")
        if corruption:
            with pytest.raises(ValueError, match='news_acceptance_recovery_'):
                recover_with_benchmark_readers(queue, readers)
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        else:
            recover_with_benchmark_readers(queue, readers)
            assert tuple(queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()) == (path, relative_receipt)
            assert queue.db.execute('SELECT state,body_sha256 FROM articles').fetchone()['state'] == 'complete'
            recover_with_benchmark_readers(queue, readers)  # An exact no-op.
    finally:
        queue.close()
        http.close()


def test_duplicate_receipt_falls_back_to_current_committed_path_without_replay(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        receipt = next((tmp_path / 'receipts').glob('*.json'))
        shutil.copyfile(receipt, tmp_path / 'receipts' / 'z-copy.json')
        before = queue.db.total_changes
        queue.recover_acceptance()
        assert queue.db.total_changes == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    finally:
        queue.close()
        http.close()


def test_large_committed_history_uses_bounded_covering_queries(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    statements = []
    try:
        # Isolated historical index fixtures; recovery is not a full body audit.
        for number in range(140):
            path = f'bodies/fixture-{number}.parquet'
            receipt = f'receipts/fixture-{number}.json'
            atomic_write_json(tmp_path / receipt, {'parquet_path': path})
            queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (path, receipt))
        queue.db.commit()
        before = queue.db.total_changes
        queue.db.set_trace_callback(statements.append)
        queue.recover_acceptance()
        assert queue.db.total_changes == before
        assert len([s for s in statements if s.startswith('SELECT receipt_path,path')]) == 2
        assert not any(s.startswith('SELECT 1 FROM accepted_batches WHERE path=') for s in statements)
    finally:
        queue.db.set_trace_callback(None)
        queue.close()
        http.close()


@pytest.mark.parametrize('readers', [0, 2, 4])
def test_valid_orphan_is_replayed_before_a_later_bad_receipt_in_same_chunk(config, tmp_path, readers):
    queue, http = accepted_queue(config, tmp_path)
    try:
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.commit()
        atomic_write_json(tmp_path / 'receipts' / 'zz-invalid.json', {'invalid': True})
        with pytest.raises(KeyError, match='parquet_path'):
            recover_with_benchmark_readers(queue, readers)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert queue.db.execute('SELECT state FROM articles').fetchone()[0] == 'complete'
    finally:
        queue.close()
        http.close()


def test_benchmark_is_readonly_and_never_claims_source_throughput(config, tmp_path):
    from scripts.benchmark_news_acceptance_recovery import benchmark
    queue, http = accepted_queue(config, tmp_path)
    try:
        before = queue.db.total_changes
        report = benchmark(tmp_path, maximum_receipts=1, cycles=1)
        assert report['accepted'] and report['cohort_receipts'] == 1
        assert report['publisher_requests'] == 0 and not report['production_database_written']
        assert report['source_receipts_unchanged'] and not report['all_history_complete']
        assert queue.db.total_changes == before
        assert len(report['results']) == 4
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('native_disabled', [False, True])
@pytest.mark.parametrize('extra_json', [
    'null', 'true', '["台灣", "\\uD834\\uDD1E", "\\uD800"]',
    '1234567890123456789012345678901234567890', '-0.0',
    '[NaN, Infinity, -Infinity, 1e309]', '{"same":1,"same":2}',
])
def test_private_receipt_decoder_preserves_every_legacy_json_value(
        config, tmp_path, monkeypatch, native_disabled, extra_json):
    from downloader import news_fulltext_queue as module
    from stockagent import private_json
    queue, http = accepted_queue(config, tmp_path)
    try:
        if native_disabled:
            monkeypatch.setattr(private_json, '_native', None)
        path, relative_receipt = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        receipt_path = tmp_path / relative_receipt
        raw = '{"parquet_path":' + json.dumps(path) + ',"unchanged_extra":' + extra_json + '}'
        # Generated fixture only. Original default-text encoding/newline
        # semantics persist; no untyped value is narrowed or newly encoded.
        receipt_path.write_text(raw)
        seen = []
        original_decode = module.decode_private_json
        def decode(text):
            value = original_decode(text)
            seen.append(json.dumps(value, ensure_ascii=True))
            return value
        monkeypatch.setattr(module, 'decode_private_json', decode)
        before = queue.db.total_changes
        queue.recover_acceptance()
        assert seen == [json.dumps(json.loads(raw), ensure_ascii=True)]
        assert queue.db.total_changes == before
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('suffix', [',"invalid":', ',"tail":1} garbage'])
def test_committed_receipt_malformed_after_path_is_not_skipped(config, tmp_path, suffix):
    queue, http = accepted_queue(config, tmp_path)
    try:
        path, relative = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        (tmp_path / relative).write_text('{"parquet_path":' + json.dumps(path) + suffix)
        before = queue.db.total_changes
        with pytest.raises(json.JSONDecodeError):
            queue.recover_acceptance()
        assert queue.db.total_changes == before
    finally:
        queue.close()
        http.close()


def test_decode_only_benchmark_checks_full_receipts_and_keeps_owner_decoder(config, tmp_path):
    from downloader import news_fulltext_queue as module
    from scripts.benchmark_news_acceptance_recovery import benchmark
    queue, http = accepted_queue(config, tmp_path)
    try:
        original_decode = module.decode_private_json
        report = benchmark(tmp_path, maximum_receipts=1, cycles=1, json_decode_only=True)
        assert report['accepted'] and report['contract_version'] == 2
        assert report['decode_parity']['all_fields_equal']
        assert report['decode_parity']['mismatches'] == []
        assert report['implementation_unchanged']
        assert {row['mode'] for row in report['results']} == {'stdlib_json', 'shared_private_json'}
        assert all(row['committed_lookup_statements'] == 1 for row in report['results'])
        assert module.decode_private_json is original_decode
    finally:
        queue.close()
        http.close()


def test_decode_benchmark_restores_decoder_after_failed_recovery(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_queue as module
    from scripts.benchmark_news_acceptance_recovery import benchmark
    queue, http = accepted_queue(config, tmp_path)
    try:
        original_decode = module.decode_private_json
        def fail(*args, **kwargs):
            raise ValueError('generated_replay_failure')
        monkeypatch.setattr(module.NewsQueue, 'recover_acceptance', fail)
        with pytest.raises(ValueError, match='generated_replay_failure'):
            benchmark(tmp_path, maximum_receipts=1, cycles=1, json_decode_only=True)
        assert module.decode_private_json is original_decode
    finally:
        queue.close()
        http.close()


def test_io_prefetch_is_ordered_and_never_runs_sqlite_or_decoder_in_readers(config, tmp_path, monkeypatch):
    import threading
    from downloader import news_fulltext_queue as module
    queue, http = accepted_queue(config, tmp_path)
    try:
        owner_thread = threading.get_ident()
        original_decode = module.decode_private_json
        def decode(text):
            assert threading.get_ident() == owner_thread
            return original_decode(text)
        monkeypatch.setattr(module, 'decode_private_json', decode)
        before = queue.db.total_changes
        recover_with_benchmark_readers(queue, 4)
        assert queue.db.total_changes == before
        assert not any(t.name.startswith('news-receipt-read') for t in threading.enumerate())
    finally:
        queue.close()
        http.close()


def test_oversized_prefetch_receipt_is_fully_read_in_owner_not_truncated(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_queue as module
    from scripts import benchmark_news_acceptance_recovery as benchmark_module
    queue, http = accepted_queue(config, tmp_path)
    try:
        monkeypatch.setattr(benchmark_module, '_RECEIPT_PREFETCH_CHARACTERS', 32)
        path, relative = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        raw = json.dumps({'parquet_path': path, 'unicode_and_null': ['台灣' * 128, None]}, ensure_ascii=False)
        (tmp_path / relative).write_text(raw)
        assert benchmark_module._bounded_receipt_text(tmp_path / relative) is None
        seen, original_decode = [], module.decode_private_json
        def decode(text):
            seen.append(text)
            return original_decode(text)
        monkeypatch.setattr(module, 'decode_private_json', decode)
        before = queue.db.total_changes
        recover_with_benchmark_readers(queue, 2)
        assert seen == [raw] and queue.db.total_changes == before
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('invalid', [-1, 9, True, 1.5, '2'])
def test_unbounded_or_noninteger_reader_pool_is_rejected(config, tmp_path, invalid):
    from scripts.benchmark_news_acceptance_recovery import _prefetch_recovery
    queue, http = accepted_queue(config, tmp_path)
    try:
        with pytest.raises(ValueError, match='bounded_receipt_read_ahead_workers_required'):
            _prefetch_recovery(queue, sorted((tmp_path / 'receipts').glob('*.json')), invalid)
    finally:
        queue.close()
        http.close()


def test_io_prefetch_benchmark_keeps_exact_committed_receipts_and_no_source_writes(config, tmp_path):
    from scripts.benchmark_news_acceptance_recovery import benchmark
    queue, http = accepted_queue(config, tmp_path)
    try:
        report = benchmark(tmp_path, maximum_receipts=1, cycles=1, read_ahead_workers=2)
        assert report['accepted'] and report['contract_version'] == 3
        assert report['candidate_read_ahead_workers'] == 2
        assert {row['mode'] for row in report['results']} == {'serial_read', 'prefetch_read'}
        assert all(row['sqlite_changes'] == 0 for row in report['results'])
        assert report['source_receipts_unchanged'] and report['publisher_requests'] == 0
    finally:
        queue.close()
        http.close()
