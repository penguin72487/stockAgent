"""The optional CNYES hint never replaces the ordered source import."""
from contextlib import closing
from datetime import datetime
import hashlib
import json
import os
import threading

import pyarrow as pa
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_url_metadata import _load, consume_urls as _consume_urls
from test_news_fulltext import config as config, http_fixture


def version(root, name, values):
    path = root / (name + '.parquet')
    atomic_write_parquet(path, pa.Table.from_pylist(values))
    receipt = root / 'receipt.json'
    atomic_write_json(receipt, {'dataset': 'tw_news_cnyes', 'parquet_path': path.name,
                                'sha256': sha256_file(path), 'rows': len(values)})
    return receipt


def values(count):
    return [{'url': f'https://publisher.test/article/{i}', 'title': str(i), 'date': '2026-10-01'}
            for i in range(count)]


@pytest.fixture
def news_queue(config, tmp_path, monkeypatch):
    config['providers'][0]['id'] = 'cnyes'
    monkeypatch.setattr('downloader.news_fulltext_finlab_priority._MIN_REMAINING_ROWS', 1)
    with closing(NewsQueue(tmp_path / 'news', PolicyRegistry(config, base=tmp_path))) as queue:
        yield queue


def pause_after_batch(queue, root, receipt, monkeypatch):
    stop = threading.Event()
    original = queue.add_articles
    def add(rows, **kwargs):
        result = original(rows, **kwargs)
        stop.set()
        return result
    with monkeypatch.context() as context:
        context.setattr(queue, 'add_articles', add)
        assert not queue.ingest_receipt(root, receipt, provider='finlab', link_column='url', stop=stop)
    reader = queue._metadata_receipt_readers[str(receipt.resolve())]
    assert reader.priority_hint is not None
    reader.priority_hint.future.result(timeout=30)
    return reader


def inbox(queue):
    directory = queue.root / 'reviewed_repairs/url_metadata/pending'
    return sorted(directory.glob('*.json')) if directory.exists() else []


def consume_urls(queue, **kwargs):
    """Wait deterministically for the optional background source-row check."""
    for _ in range(4):
        result = _consume_urls(queue, **kwargs)
        pending = result['outcomes'].pop('finlab_row_verification_pending', 0)
        if not pending or result['processed']:
            return result
        verifier = queue._finlab_hint_row_verifier
        if verifier.active is not None:
            try:
                verifier.active[1].result(timeout=30)
            except Exception:
                pass  # The owner classifies a failed verification as unverified.
        queue._url_metadata_retry_at = 0
    raise AssertionError('source row verification did not complete')


def test_delta_hint_precedes_last_batch_and_preserves_full_associations(news_queue, tmp_path, monkeypatch):
    queue = news_queue
    old = values(8200)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    new_url = 'https://publisher.test/article/new'
    existing_url = 'https://publisher.test/article/already-known'
    queue.add_articles([{'url': existing_url}])
    existing_key = hashlib.sha256(existing_url.encode()).digest()
    queue.db.execute("UPDATE articles SET state='retry_cooldown',due=12345 WHERE id=?", (existing_key,))
    queue.db.commit()
    rows = old + [{'url': new_url, 'title': 'new', 'date': '2026-10-02'},
                  {'url': existing_url, 'title': 'existing', 'date': '2026-10-02'}]
    receipt = version(tmp_path, 'new', rows)
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    assert reader.processed_rows == 4096
    reader.priority_hint.poll()
    paths = inbox(queue)
    assert len(paths) == 1
    request = _load(paths[0])
    assert {item['url'] for item in request['targets']} == {new_url, existing_url}
    assert {item['source_date'] for item in request['targets']} == {'2026-10-02'}
    result = consume_urls(queue, limit=512, max_seconds=.1)
    assert result['outcomes'] == {'metadata_registered': 1, 'existing_head_preserved': 1}
    assert tuple(queue.db.execute('SELECT state,due FROM articles WHERE id=?', (existing_key,)).fetchone()) == ('retry_cooldown', 12345)
    new_key = hashlib.sha256(new_url.encode()).digest()
    assert tuple(queue.db.execute('SELECT title,body_sha256,metadata_date_raw,metadata_order_day '
                                  'FROM articles WHERE id=?', (new_key,)).fetchone()) == (
                                      '', None, '2026-10-02', '2026-10-02')
    assert not queue.db.execute('SELECT 1 FROM source_links WHERE article_id=? AND origin_id=?',
                                (new_key, reader.origin_id)).fetchone()
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    assert queue.db.execute('SELECT sum(association_rows) FROM source_links WHERE origin_id=?',
                            (reader.origin_id,)).fetchone()[0] == len(rows)
    assert tuple(queue.db.execute('SELECT title,body_sha256 FROM articles WHERE id=?', (new_key,)).fetchone()) == ('new', None)
    marker = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
        ('finlab_cnyes_delta_hint_v1:' + str(reader.origin_id),)).fetchone()[0])
    assert marker['targets'] == 2 and not marker['body_verified'] and not marker['source_associations_complete']


def test_hint_date_enters_existing_dated_dispatch_without_claiming_publication(news_queue, tmp_path, monkeypatch):
    queue = news_queue
    receipt = version(tmp_path, 'old', values(4100))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    new_url = 'https://publisher.test/article/newest'
    receipt = version(tmp_path, 'new', values(4100) + [
        {'url': new_url, 'date': '2026-10-02 12:34:56', 'title': 'newest'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
    row = queue.claim('cnyes', 300)
    assert row['url'] == new_url
    assert row['metadata_date_raw'] == '2026-10-02 12:34:56'
    assert row['metadata_order_day'] == '2026-10-02'
    assert row['body_sha256'] is None and row['parquet_path'] is None
    assert not queue.db.execute('SELECT 1 FROM source_links WHERE article_id=? AND origin_id=?',
                                (row['id'], reader.origin_id)).fetchone()


@pytest.mark.parametrize('damage', ['date', 'row_index'])
def test_hint_does_not_admit_date_that_disagrees_with_original_row(news_queue, tmp_path, monkeypatch, damage):
    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/row-evidence-mismatch'
    receipt = version(tmp_path, 'new', old + [{'url': url, 'date': '2026-10-02'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    result = reader.priority_hint.future.result(timeout=30)
    if damage == 'date':
        result['raw_dates'][0] = '2027-12-31'
    else:
        result['raw_indices'][0] = 1
    reader.priority_hint.poll()
    outcome = consume_urls(queue, limit=512, max_seconds=.1)['outcomes']
    assert outcome == {'metadata_registered_unverified_origin': 1}
    head = queue.db.execute('SELECT metadata_date_raw,metadata_order_day,body_sha256 FROM articles WHERE url=?',
                            (url,)).fetchone()
    assert tuple(head) == ('', '', None)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    head = queue.db.execute('SELECT metadata_date_raw,metadata_order_day FROM articles WHERE url=?', (url,)).fetchone()
    assert tuple(head) == ('2026-10-02', '2026-10-02')


def test_recovered_hint_row_check_does_not_block_other_mailbox_work(news_queue, tmp_path, monkeypatch):
    from downloader import news_fulltext_url_metadata as intake

    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/recovered-background'
    receipt = version(tmp_path, 'new', old + [{'url': url, 'date': '2026-10-02'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    queue._finlab_hint_row_verifier.completed.clear()  # Lost on owner restart.
    started, release = threading.Event(), threading.Event()
    verify = intake._verify_finlab_hint_rows

    def slow(*args):
        started.set()
        assert release.wait(10)
        return verify(*args)

    monkeypatch.setattr(intake, '_verify_finlab_hint_rows', slow)
    monkeypatch.setattr(intake, 'FINLAB_ROW_VERIFY_TIMEOUT_SECONDS', 30)
    try:
        first = _consume_urls(queue, limit=512, max_seconds=.1)
        assert first['processed'] == 0 and first['outcomes'] == {'finlab_row_verification_pending': 1}
        assert started.wait(5)
        other = 'https://publisher.test/article/unrelated-mailbox'
        intake.submit_urls(queue.root, queue.registry, [other])
        next_result = _consume_urls(queue, limit=512, max_seconds=.1)
        assert next_result['outcomes']['metadata_registered'] == 1
        assert queue.db.execute('SELECT 1 FROM articles WHERE url=?', (other,)).fetchone()
        assert not queue.db.execute('SELECT 1 FROM articles WHERE url=?', (url,)).fetchone()
    finally:
        release.set()
    queue._finlab_hint_row_verifier.active[1].result(timeout=30)
    queue._url_metadata_retry_at = 0
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
    assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (url,)).fetchone()[0] == '2026-10-02'


def test_source_generation_changed_during_background_row_read_is_unverified(news_queue, tmp_path, monkeypatch):
    from downloader import news_fulltext_url_metadata as intake

    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/changed-during-row-read'
    receipt = version(tmp_path, 'new', old + [{'url': url, 'date': '2026-10-02'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    queue._finlab_hint_row_verifier.completed.clear()
    started, release = threading.Event(), threading.Event()
    verify = intake._verify_finlab_hint_rows

    def slow(*args):
        started.set()
        assert release.wait(10)
        return verify(*args)

    monkeypatch.setattr(intake, '_verify_finlab_hint_rows', slow)
    monkeypatch.setattr(intake, 'FINLAB_ROW_VERIFY_TIMEOUT_SECONDS', 30)
    try:
        assert _consume_urls(queue, limit=512, max_seconds=.1)['processed'] == 0
        assert started.wait(5)
        current = reader.parquet.stat()
        os.utime(reader.parquet, ns=(current.st_atime_ns, current.st_mtime_ns + 1_000_000))
    finally:
        release.set()
    queue._finlab_hint_row_verifier.active[1].result(timeout=30)
    queue._url_metadata_retry_at = 0
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered_unverified_origin': 1}
    assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (url,)).fetchone()[0] == ''


def test_primed_new_hint_bypasses_older_blocked_row_check(news_queue, tmp_path, monkeypatch):
    from downloader import news_fulltext_url_metadata as intake

    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    earlier = 'https://publisher.test/article/earlier-hint'
    receipt = version(tmp_path, 'first', old + [{'url': earlier, 'date': '2026-10-02'}])
    first = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    first.priority_hint.poll()
    queue._finlab_hint_row_verifier.completed.clear()
    started, release = threading.Event(), threading.Event()
    original = intake._verify_finlab_hint_rows

    def slow(*args):
        started.set()
        assert release.wait(30)
        return original(*args)

    monkeypatch.setattr(intake, '_verify_finlab_hint_rows', slow)
    monkeypatch.setattr(intake, 'FINLAB_ROW_VERIFY_TIMEOUT_SECONDS', 60)
    try:
        assert _consume_urls(queue, limit=512, max_seconds=.1)['processed'] == 0
        assert started.wait(5)
        newer = 'https://publisher.test/article/newer-primed-hint'
        receipt = version(tmp_path, 'second', old + [{'url': newer, 'date': '2026-10-03'}])
        second = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
        second.priority_hint.poll()
        result = _consume_urls(queue, limit=512, max_seconds=.1)
        assert result['outcomes']['metadata_registered'] == 1
        assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (newer,)).fetchone()[0] == '2026-10-03'
        assert not queue.db.execute('SELECT 1 FROM articles WHERE url=?', (earlier,)).fetchone()
    finally:
        release.set()
    queue._finlab_hint_row_verifier.active[1].result(timeout=30)
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}


def test_stuck_recovered_row_check_falls_back_without_date(news_queue, tmp_path, monkeypatch):
    from downloader import news_fulltext_url_metadata as intake

    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/recovered-timeout'
    receipt = version(tmp_path, 'new', old + [{'url': url, 'date': '2026-10-02'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    queue._finlab_hint_row_verifier.completed.clear()
    started, release = threading.Event(), threading.Event()

    def slow(*args):
        started.set()
        assert release.wait(10)
        return None

    monkeypatch.setattr(intake, '_verify_finlab_hint_rows', slow)
    monkeypatch.setattr(intake, 'FINLAB_ROW_VERIFY_TIMEOUT_SECONDS', 0)
    try:
        first = _consume_urls(queue, limit=512, max_seconds=.1)
        assert first['outcomes'] == {'finlab_row_verification_pending': 1}
        assert started.wait(5)
        second = _consume_urls(queue, limit=512, max_seconds=.1)
        assert second['outcomes'] == {'metadata_registered_unverified_origin': 1}
        assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (url,)).fetchone()[0] == ''
    finally:
        release.set()
    queue._finlab_hint_row_verifier.active[1].result(timeout=30)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (url,)).fetchone()[0] == '2026-10-02'


def test_recovered_row_worker_failure_keeps_other_paths_and_uses_no_date(news_queue, tmp_path, monkeypatch):
    from downloader import news_fulltext_url_metadata as intake

    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/recovered-worker-error'
    receipt = version(tmp_path, 'new', old + [{'url': url, 'date': '2026-10-02'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    queue._finlab_hint_row_verifier.completed.clear()

    def fail(*args):
        raise RuntimeError('injected background read failure')

    monkeypatch.setattr(intake, '_verify_finlab_hint_rows', fail)
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered_unverified_origin': 1}
    assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (url,)).fetchone()[0] == ''


def test_row_worker_cannot_start_registers_undated_without_crashing(news_queue, tmp_path, monkeypatch):
    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/recovered-worker-unavailable'
    receipt = version(tmp_path, 'new', old + [{'url': url, 'date': '2026-10-02'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    verifier = queue._finlab_hint_row_verifier
    verifier.completed.clear()
    def fail_submit(*args, **kwargs):
        raise RuntimeError('injected executor failure')
    monkeypatch.setattr(verifier.pool, 'submit', fail_submit)
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered_unverified_origin': 1}
    assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (url,)).fetchone()[0] == ''


def test_completed_hint_archive_retry_does_not_reverify_source(news_queue, tmp_path, monkeypatch):
    from downloader import news_fulltext_url_metadata as intake

    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/archive-retry'
    receipt = version(tmp_path, 'new', old + [{'url': url, 'date': '2026-10-02'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    with monkeypatch.context() as context:
        def fail_archive(*args):
            raise OSError('injected archive failure')
        context.setattr(intake, '_archive', fail_archive)
        assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
        assert inbox(queue)
    def forbidden_check(*args):
        raise AssertionError('completed request must not reverify')
    monkeypatch.setattr(queue._finlab_hint_row_verifier, 'check', forbidden_check)
    assert consume_urls(queue, limit=512, max_seconds=.1)['processed'] == 0
    assert not inbox(queue)


def test_source_date_lookup_uses_first_exact_delta_row_and_preserves_null(tmp_path):
    from downloader.news_fulltext_finlab_priority import _compare, _stamp

    old = tmp_path / 'old.parquet'
    current = tmp_path / 'current.parquet'
    atomic_write_parquet(old, pa.Table.from_pylist([{'url': 'https://publisher.test/old', 'date': None}]))
    rows = [
        {'url': 'https://publisher.test/old', 'date': datetime(2026, 10, 1, 9)},
        {'url': 'https://publisher.test/new', 'date': datetime(2026, 10, 2, 10, 11, 12)},
        {'url': 'https://publisher.test/new', 'date': datetime(2026, 10, 3, 10)},
        {'url': 'https://publisher.test/unknown', 'date': None},
    ]
    import pyarrow.parquet as pq
    pq.write_table(pa.Table.from_pylist(rows), current, row_group_size=1, use_dictionary=True)
    result = _compare(old, sha256_file(old), 1, current, _stamp(current), len(rows), threading.Event())
    assert result['raw_hints'] == ['https://publisher.test/new', 'https://publisher.test/unknown']
    assert result['raw_dates'] == ['2026-10-02 10:11:12', '']


@pytest.mark.parametrize('source_date', ['x' * 129, 'date\x00hidden', None])
def test_optional_unusable_date_does_not_prevent_url_hint(tmp_path, source_date):
    from downloader.news_fulltext_finlab_priority import _compare, _stamp

    old, current = tmp_path / 'old.parquet', tmp_path / 'new.parquet'
    atomic_write_parquet(old, pa.Table.from_pylist([{'url': 'https://publisher.test/old'}]))
    atomic_write_parquet(current, pa.Table.from_pylist([{'url': 'https://publisher.test/new', 'date': source_date}]))
    result = _compare(old, sha256_file(old), 1, current, _stamp(current), 1, threading.Event())
    assert result['raw_hints'] == ['https://publisher.test/new']
    assert result['raw_dates'] == ['']


def test_missing_or_unreadable_date_column_degrades_to_undated_hint(tmp_path, monkeypatch):
    import downloader.news_fulltext_finlab_priority as priority

    path = tmp_path / 'missing-date.parquet'
    atomic_write_parquet(path, pa.Table.from_pylist([{'url': 'https://publisher.test/new'}]))
    assert priority._read_finlab_hint_source_rows(path, list(priority._stamp(path)), 1, [0]) == (
        ('https://publisher.test/new', ''),)
    def fail(*args, **kwargs):
        raise OSError('transient optional date read failure')
    monkeypatch.setattr(priority.pq, 'ParquetFile', fail)
    assert priority._read_finlab_hint_source_rows(path, list(priority._stamp(path)), 1, [0]) is None


def test_priority_progress_uses_durable_owner_receipt_not_mailbox_submission(config, tmp_path, monkeypatch):
    from downloader.news_fulltext_finlab_priority import priority_progress

    config['providers'][0]['id'] = 'cnyes'
    monkeypatch.setattr('downloader.news_fulltext_finlab_priority._MIN_REMAINING_ROWS', 1)
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path / 'news', registry)
    receipt = version(tmp_path, 'old', values(4100))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    existing = 'https://publisher.test/article/already'
    queue.add_articles([{'url': existing}])
    queue.db.commit()
    receipt = version(tmp_path, 'new', values(4100) + [
        {'url': 'https://publisher.test/article/new'}, {'url': existing}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    assert priority_progress(queue) is None
    reader.priority_hint.poll()
    pending = priority_progress(queue)
    assert pending['recorded_at_utc'].endswith('+00:00')
    assert pending == {'contract_version': 1, 'raw_unique_delta': 2, 'raw_hints_examined': 2,
        'targets': 2, 'request_state': 'unobserved', 'owner_checked': None, 'outcomes': None,
        'unverified_origin': None,
        'recorded_at_utc': pending['recorded_at_utc'],
        'body_completion_proven': False, 'association_completion_proven': False}
    consume_urls(queue, limit=1, max_seconds=.1)
    partial = priority_progress(queue)
    assert partial['request_state'] == 'processing' and partial['owner_checked'] == 1
    assert sum(partial['outcomes'].values()) == 1
    consume_urls(queue, limit=512, max_seconds=.1)
    complete = priority_progress(queue)
    assert complete['request_state'] == 'checked' and complete['owner_checked'] == 2
    assert complete['outcomes'] == {'metadata_registered': 1, 'existing_head_preserved': 1,
                                    'source_contract_changed': 0, 'other': 0}
    assert queue.status(publish=False)['finlab_cnyes_priority'] == complete
    assert 'https://' not in json.dumps(complete)
    queue.close()
    with closing(NewsQueue(tmp_path / 'news', registry)) as reopened:
        assert priority_progress(reopened) == complete


def test_priority_progress_invalid_job_cursor_reports_unknown(news_queue, tmp_path, monkeypatch):
    from downloader.news_fulltext_finlab_priority import priority_progress

    receipt = version(tmp_path, 'old', values(4100))
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(4101))
    reader = pause_after_batch(news_queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    consume_urls(news_queue, limit=512, max_seconds=.1)
    news_queue.db.execute('UPDATE url_metadata_jobs SET next_index=0')
    news_queue.db.commit()
    progress = priority_progress(news_queue)
    assert progress['request_state'] == 'unknown'
    assert progress['owner_checked'] is None and progress['outcomes'] is None


def test_priority_progress_rejects_bad_marker_and_handles_no_eligible_targets(news_queue, tmp_path, monkeypatch):
    from downloader.news_fulltext_finlab_priority import priority_progress

    receipt = version(tmp_path, 'old', values(4100))
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(4100) + [{'url': 'https://other.test/a'}])
    reader = pause_after_batch(news_queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    progress = priority_progress(news_queue)
    assert progress['targets'] == 0 and progress['request_state'] == 'not_applicable'
    assert progress['owner_checked'] == 0 and sum(progress['outcomes'].values()) == 0
    marker = json.loads(news_queue.db.execute(
        "SELECT value FROM queue_settings WHERE key='finlab_cnyes_delta_hint_latest_v1'").fetchone()[0])
    bad_clock = {**marker, 'recorded_at_utc': 'not-a-clock'}
    original_key = 'finlab_cnyes_delta_hint_v1:' + str(marker['origin_id'])
    with news_queue.db:
        news_queue.db.execute('UPDATE queue_settings SET value=? WHERE key IN (?,?)',
            (json.dumps(bad_clock, sort_keys=True), 'finlab_cnyes_delta_hint_latest_v1', original_key))
    assert priority_progress(news_queue) is None
    with news_queue.db:
        news_queue.db.execute('UPDATE queue_settings SET value=? WHERE key IN (?,?)',
            (json.dumps(marker, sort_keys=True), 'finlab_cnyes_delta_hint_latest_v1', original_key))
    marker['raw_unique_delta'] = marker['raw_hints_examined'] = 2
    news_queue.db.execute("UPDATE queue_settings SET value=? WHERE key='finlab_cnyes_delta_hint_latest_v1'",
                          (json.dumps(marker, sort_keys=True),))
    news_queue.db.commit()
    assert priority_progress(news_queue) is None  # pointer conflicts with the per-origin record
    news_queue.db.execute("UPDATE queue_settings SET value='{}' WHERE key='finlab_cnyes_delta_hint_latest_v1'")
    news_queue.db.commit()
    assert priority_progress(news_queue) is None


def test_changed_current_receipt_does_not_publish_a_hint(news_queue, tmp_path, monkeypatch):
    receipt = version(tmp_path, 'old', values(4100))
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(4101))
    reader = pause_after_batch(news_queue, tmp_path, receipt, monkeypatch)
    atomic_write_json(receipt, {'dataset': 'changed'})
    reader.priority_hint.poll()
    assert inbox(news_queue) == []


def test_unavailable_baseline_only_skips_hint_not_ordered_import(news_queue, tmp_path, monkeypatch):
    receipt = version(tmp_path, 'old', values(4100))
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(4101))
    (tmp_path / 'old.parquet').unlink()
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    assert inbox(news_queue) == []
    assert news_queue.db.execute('SELECT max(rows) FROM origins').fetchone()[0] == 4101


def test_closed_source_or_non_cnyes_urls_are_not_priority_targets(news_queue, tmp_path, monkeypatch):
    receipt = version(tmp_path, 'old', values(4100))
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    rows = values(4100) + [{'url': 'https://another.test/story', 'title': 'x', 'date': ''},
                           {'url': 'file:///private', 'title': 'x', 'date': ''}]
    receipt = version(tmp_path, 'new', rows)
    reader = pause_after_batch(news_queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    assert inbox(news_queue) == []
    marker = json.loads(news_queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
        ('finlab_cnyes_delta_hint_v1:' + str(reader.origin_id),)).fetchone()[0])
    assert marker['raw_unique_delta'] == 2 and marker['targets'] == 0


def test_crash_after_mailbox_write_reuses_same_request(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_finlab_priority as priority

    config['providers'][0]['id'] = 'cnyes'
    monkeypatch.setattr(priority, '_MIN_REMAINING_ROWS', 1)
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path / 'news', registry)
    receipt = version(tmp_path, 'old', values(8200))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(8201))
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    submit = priority._submit
    def write_then_crash(*args, **kwargs):
        submit(*args, **kwargs)
        raise OSError('injected crash before SQL marker')
    with monkeypatch.context() as context:
        context.setattr(priority, '_submit', write_then_crash)
        reader.priority_hint.poll()
    first = inbox(queue)
    assert len(first) == 1
    first_name = first[0].name
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
        ('finlab_cnyes_delta_hint_v1:' + str(reader.origin_id),)).fetchone()
    queue.close()
    with closing(NewsQueue(tmp_path / 'news', registry)) as restarted:
        resumed = pause_after_batch(restarted, tmp_path, receipt, monkeypatch)
        resumed.priority_hint.poll()
        assert [p.name for p in inbox(restarted)] == [first_name]
        assert restarted.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
            ('finlab_cnyes_delta_hint_v1:' + str(resumed.origin_id),)).fetchone()
        assert consume_urls(restarted, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
        assert restarted.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert restarted.db.execute('SELECT sum(association_rows) FROM source_links WHERE origin_id=?',
                                    (resumed.origin_id,)).fetchone()[0] == 8201


def test_owner_can_check_mailbox_before_marker_retry_and_restart(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_finlab_priority as priority

    config['providers'][0]['id'] = 'cnyes'
    monkeypatch.setattr(priority, '_MIN_REMAINING_ROWS', 1)
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path / 'news', registry)
    receipt = version(tmp_path, 'old', values(8200))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(8201))
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    submit = priority._submit
    def write_then_fail(*args, **kwargs):
        submit(*args, **kwargs)
        raise OSError('injected interruption after mailbox write')
    with monkeypatch.context() as context:
        context.setattr(priority, '_submit', write_then_fail)
        reader.priority_hint.poll()
    assert priority.priority_progress(queue) is None
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
    progress = priority.priority_progress(queue)
    assert progress['request_state'] == 'checked' and progress['owner_checked'] == 1
    reader.priority_hint.retry_at = 0  # Advance the local retry gate without sleeping.
    reader.priority_hint.poll()
    assert reader.priority_hint.done
    assert not inbox(queue)
    queue.close()
    with closing(NewsQueue(tmp_path / 'news', registry)) as reopened:
        progress = priority.priority_progress(reopened)
        assert progress['request_state'] == 'checked' and progress['owner_checked'] == 1
        assert progress['outcomes']['metadata_registered'] == 1
        consume_urls(reopened, limit=512, max_seconds=.1)
        assert reopened.db.execute('SELECT count(*) FROM url_metadata_items').fetchone()[0] == 1
        assert reopened.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert reopened.db.execute('SELECT sum(association_rows) FROM source_links WHERE origin_id=?',
                                   (reader.origin_id,)).fetchone()[0] == 8201


def test_hint_survives_crash_and_receipt_replacement_with_exact_original_row(config, tmp_path, monkeypatch):
    import downloader.news_fulltext_finlab_priority as priority
    import pyarrow.parquet as pq

    config['providers'][0]['id'] = 'cnyes'
    monkeypatch.setattr(priority, '_MIN_REMAINING_ROWS', 1)
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path / 'news', registry)
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/from-replaced-receipt'
    receipt = version(tmp_path, 'interrupted', old + [{'url': url, 'date': '2026-10-12'}])
    interrupted_sha = sha256_file(tmp_path / 'interrupted.parquet')
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    origin_id = reader.origin_id
    submit = priority._submit
    def write_then_fail(*args, **kwargs):
        submit(*args, **kwargs)
        raise OSError('crash after mailbox write, before origin marker')
    with monkeypatch.context() as context:
        context.setattr(priority, '_submit', write_then_fail)
        reader.priority_hint.poll()
    path, = inbox(queue)
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
        ('finlab_cnyes_delta_hint_v1:' + str(origin_id),)).fetchone()
    queue.close()

    # The newer receipt no longer contains the hinted row. The old origin is
    # retained as superseded, but its lost marker cannot supply provenance.
    receipt = version(tmp_path, 'replacement', old + [
        {'url': 'https://publisher.test/article/other', 'date': '2026-10-13'}])
    with closing(NewsQueue(tmp_path / 'news', registry)) as reopened:
        with monkeypatch.context() as context:
            context.setattr(priority.FinlabCnyesPriority, 'start', lambda *args: None)
            assert reopened.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        request = _load(path)
        assert request['source_origin']['origin_id'] == origin_id
        assert request['source_origin']['sha256'] == interrupted_sha
        target, = request['targets']
        assert target['source_row_index'] == 4100
        original, = pq.read_table(tmp_path / 'interrupted.parquet', columns=['url', 'date']).slice(
            target['source_row_index'], 1).to_pylist()
        assert (target['url'], target['source_date']) == (original['url'], original['date'])
        assert consume_urls(reopened, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
        progress = priority.priority_progress(reopened)
        assert progress is not None and progress['request_state'] == 'checked' and progress['owner_checked'] == 1
        key = hashlib.sha256(url.encode()).digest()
        assert not reopened.db.execute('SELECT 1 FROM source_links WHERE article_id=? AND origin_id=?',
                                       (key, origin_id)).fetchone()
        stored = reopened.db.execute('SELECT metadata_date_raw FROM articles WHERE id=?', (key,)).fetchone()
        assert stored[0] == original['date']
        archived = reopened.root / 'reviewed_repairs/url_metadata/completed' / (path.stem + '.request.json')
        assert hashlib.sha256(archived.read_bytes()).hexdigest() == path.stem
        assert json.loads(archived.read_text())['source_origin'] == request['source_origin']


def test_late_recovered_hint_never_overwrites_newer_progress(news_queue, tmp_path, monkeypatch):
    import downloader.news_fulltext_finlab_priority as priority

    queue = news_queue
    old = values(4100)
    receipt = version(tmp_path, 'old', old)
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'interrupted', old + [
        {'url': 'https://publisher.test/article/interrupted', 'date': '2026-10-12'}])
    first = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    submit = priority._submit
    def write_then_fail(*args, **kwargs):
        submit(*args, **kwargs)
        raise OSError('injected interruption')
    with monkeypatch.context() as context:
        context.setattr(priority, '_submit', write_then_fail)
        first.priority_hint.poll()
    first_id = inbox(queue)[0].stem
    receipt = version(tmp_path, 'newer', old + [
        {'url': 'https://publisher.test/article/newer', 'date': '2026-10-13'}])
    newer = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    newer.priority_hint.poll()
    latest_key = 'finlab_cnyes_delta_hint_latest_v1'
    latest = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (latest_key,)).fetchone()[0])
    assert latest['origin_id'] == newer.origin_id > first.origin_id
    for _ in range(4):
        consume_urls(queue, limit=1, max_seconds=.1)
        if queue.db.execute('SELECT 1 FROM url_metadata_items WHERE request_id=?', (first_id,)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM url_metadata_items WHERE request_id=?', (first_id,)).fetchone()
    recovered = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
        ('finlab_cnyes_delta_hint_v1:' + str(first.origin_id),)).fetchone()[0])
    assert recovered['recovered_from_owner_request'] and recovered['request_id'] == first_id
    assert json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (latest_key,)).fetchone()[0]) == latest


def test_bad_latest_pointer_cannot_hide_a_new_valid_marker(news_queue, tmp_path, monkeypatch):
    from downloader.news_fulltext_finlab_priority import priority_progress

    queue = news_queue
    receipt = version(tmp_path, 'old', values(4100))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(4101))
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                         ('finlab_cnyes_delta_hint_latest_v1', json.dumps({'origin_id': 999999})))
    reader.priority_hint.poll()
    progress = priority_progress(queue)
    assert progress['targets'] == 1 and progress['request_state'] == 'unobserved'


def test_orphan_with_unverifiable_baseline_never_fabricates_progress(news_queue, tmp_path, monkeypatch):
    import downloader.news_fulltext_finlab_priority as priority

    queue = news_queue
    receipt = version(tmp_path, 'old', values(4100))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(4101))
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    submit = priority._submit
    def write_then_fail(*args, **kwargs):
        submit(*args, **kwargs)
        raise OSError('injected interruption')
    with monkeypatch.context() as context:
        context.setattr(priority, '_submit', write_then_fail)
        reader.priority_hint.poll()
    with queue.db:
        queue.db.execute('UPDATE origins SET sha256=? WHERE id=?', ('a' * 64, reader.priority_hint.old_id))
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
    assert priority.priority_progress(queue) is None
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
        ('finlab_cnyes_delta_hint_v1:' + str(reader.origin_id),)).fetchone()


@pytest.mark.parametrize('change', ['modified', 'missing'])
def test_unavailable_source_keeps_url_but_never_uses_unverified_date(news_queue, tmp_path, monkeypatch, change):
    queue = news_queue
    receipt = version(tmp_path, 'old', values(4100))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/source-changed'
    receipt = version(tmp_path, 'new', values(4100) + [{'url': url, 'date': '2026-10-12'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    source = tmp_path / 'new.parquet'
    if change == 'missing':
        source.unlink()
    else:
        info = source.stat()
        os.utime(source, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered_unverified_origin': 1}
    key = hashlib.sha256(url.encode()).digest()
    assert tuple(queue.db.execute('SELECT metadata_date_raw,metadata_order_day FROM articles WHERE id=?',
                                  (key,)).fetchone()) == ('', '')
    assert not queue.db.execute('SELECT 1 FROM source_links WHERE article_id=? AND origin_id=?',
                                (key, reader.origin_id)).fetchone()
    if change == 'modified':
        # A fresh checksum and the ordinary ordered import can recover the
        # source date and its real association, without replaying the hint.
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        assert tuple(queue.db.execute('SELECT metadata_date_raw,metadata_order_day FROM articles WHERE id=?',
                                      (key,)).fetchone()) == ('2026-10-12', '2026-10-12')
        assert queue.db.execute('SELECT 1 FROM source_links WHERE article_id=? AND origin_id=?',
                                (key, reader.origin_id)).fetchone()


def test_source_change_during_admission_rolls_back_and_retries_undated(news_queue, tmp_path, monkeypatch):
    queue = news_queue
    receipt = version(tmp_path, 'old', values(4100))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/source-changes-during-write'
    receipt = version(tmp_path, 'new', values(4100) + [{'url': url, 'date': '2026-10-12'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    original_add = queue.add_articles
    def modify_source(rows, **kwargs):
        result = original_add(rows, **kwargs)
        info = reader.parquet.stat()
        os.utime(reader.parquet, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000_000))
        return result
    with monkeypatch.context() as context:
        context.setattr(queue, 'add_articles', modify_source)
        assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'url_metadata_storage_deferred': 1}
    key = hashlib.sha256(url.encode()).digest()
    assert not queue.db.execute('SELECT 1 FROM articles WHERE id=?', (key,)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM url_metadata_jobs').fetchone()
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered_unverified_origin': 1}
    assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE id=?', (key,)).fetchone()[0] == ''


@pytest.mark.parametrize('damage', ['digest', 'provider', 'rows', 'column'])
def test_hint_origin_record_mismatch_never_admits_date(news_queue, tmp_path, monkeypatch, damage):
    from downloader.news_fulltext_finlab_priority import priority_progress

    queue = news_queue
    receipt = version(tmp_path, 'old', values(4100))
    assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    url = 'https://publisher.test/article/source-record-mismatch'
    receipt = version(tmp_path, 'new', values(4100) + [{'url': url, 'date': '2026-10-12'}])
    reader = pause_after_batch(queue, tmp_path, receipt, monkeypatch)
    reader.priority_hint.poll()
    with queue.db:
        if damage == 'digest':
            queue.db.execute('UPDATE origins SET sha256=? WHERE id=?', ('a' * 64, reader.origin_id))
        elif damage == 'provider':
            queue.db.execute('UPDATE origins SET provider=? WHERE id=?', ('other', reader.origin_id))
        elif damage == 'rows':
            queue.db.execute('UPDATE metadata_ingest_progress SET expected_rows=expected_rows+1 WHERE origin_id=?',
                             (reader.origin_id,))
        else:
            queue.db.execute('UPDATE metadata_ingest_progress SET link_column=? WHERE origin_id=?',
                             ('other', reader.origin_id))
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered_unverified_origin': 1}
    assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (url,)).fetchone()[0] == ''
    progress = priority_progress(queue)
    assert progress['request_state'] == 'checked' and progress['outcomes']['other'] == 1
    assert progress['unverified_origin'] == 1
    assert progress['outcomes']['metadata_registered'] == 0


def test_legacy_hint_remains_compatible_but_date_is_not_admitted(news_queue):
    import downloader.news_fulltext_url_metadata as intake

    queue = news_queue
    url = 'https://publisher.test/article/legacy-hint'
    intake._submit(queue.root, {'contract_version': intake.LEGACY_FINLAB_CNYES_HINT_VERSION,
        'kind': intake.FINLAB_CNYES_HINT_KIND, 'created_at_utc': intake.utc_now(),
        'targets': [{'url': url, 'source': intake._source_identity(queue.registry, url), 'source_date': '2026-10-12'}]})
    assert consume_urls(queue, limit=512, max_seconds=.1)['outcomes'] == {'metadata_registered_unverified_origin': 1}
    assert queue.db.execute('SELECT metadata_date_raw FROM articles WHERE url=?', (url,)).fetchone()[0] == ''


def test_baseline_changed_after_comparison_never_publishes_hint(news_queue, tmp_path, monkeypatch):
    receipt = version(tmp_path, 'old', values(4100))
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(4101))
    reader = pause_after_batch(news_queue, tmp_path, receipt, monkeypatch)
    with (tmp_path / 'old.parquet').open('ab') as file:
        file.write(b'x')
    reader.priority_hint.poll()
    assert inbox(news_queue) == []
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')


def test_optional_worker_exception_cannot_stop_original_import(news_queue, tmp_path, monkeypatch):
    import downloader.news_fulltext_finlab_priority as priority

    receipt = version(tmp_path, 'old', values(4100))
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new', values(4101))
    def fail(*args):
        raise RuntimeError('injected local acceleration failure')
    monkeypatch.setattr(priority, '_compare', fail)
    stop = threading.Event()
    original = news_queue.add_articles
    def add(rows, **kwargs):
        result = original(rows, **kwargs)
        stop.set()
        return result
    with monkeypatch.context() as context:
        context.setattr(news_queue, 'add_articles', add)
        assert not news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', stop=stop)
    reader = news_queue._metadata_receipt_readers[str(receipt.resolve())]
    with pytest.raises(RuntimeError):
        reader.priority_hint.future.result(timeout=30)
    reader.priority_hint.poll()
    assert reader.priority_hint.done and inbox(news_queue) == []
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')


def test_replaced_receipt_does_not_spawn_overlapping_comparisons(news_queue, tmp_path, monkeypatch):
    import downloader.news_fulltext_finlab_priority as priority

    receipt = version(tmp_path, 'old', values(4100))
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
    receipt = version(tmp_path, 'new-one', values(4101))
    started, release = threading.Event(), threading.Event()
    calls = []
    def blocked(*args):
        calls.append(1)
        started.set()
        release.wait(30)
        return None
    monkeypatch.setattr(priority, '_compare', blocked)
    stop = threading.Event()
    add = news_queue.add_articles
    def one_batch(rows, **kwargs):
        result = add(rows, **kwargs)
        stop.set()
        return result
    try:
        with monkeypatch.context() as context:
            context.setattr(news_queue, 'add_articles', one_batch)
            assert not news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', stop=stop)
        assert started.wait(5) and len(calls) == 1
        receipt = version(tmp_path, 'new-two', values(4102))
        stop.clear()
        with monkeypatch.context() as context:
            context.setattr(news_queue, 'add_articles', one_batch)
            assert not news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url', stop=stop)
        reader = news_queue._metadata_receipt_readers[str(receipt.resolve())]
        assert reader.priority_hint is None and len(calls) == 1
    finally:
        release.set()
    assert news_queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')


def test_resident_owner_processes_hint_before_receipt_end(config, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_url_metadata as mailbox
    import downloader.news_fulltext_finlab_priority as priority

    config['providers'][0]['id'] = 'cnyes'
    monkeypatch.setattr(priority, '_MIN_REMAINING_ROWS', 1)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    http, session = http_fixture(config, tmp_path, {})
    with closing(NewsQueue(tmp_path / 'news', http.registry)) as queue:
        # At least nine original batches: even two maximum-size commit groups
        # cannot consume the final URL before the background hint is polled.
        receipt = version(tmp_path, 'old', values(32768))
        assert queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url')
        receipt = version(tmp_path, 'new', values(32769))
        observed, events, refreshes = [], [], []
        consume = mailbox.consume_urls
        def inspect(*args, **kwargs):
            result = consume(*args, **kwargs)
            if result['outcomes']:
                events.append(result['outcomes'])
            if result['outcomes'].get('metadata_registered'):
                row = queue.db.execute('SELECT processed_rows,expected_rows FROM metadata_ingest_progress').fetchone()
                observed.append(tuple(row) if row else None)
                http.stop.set()
            return result
        monkeypatch.setattr(mailbox, 'consume_urls', inspect)
        def refresh():
            complete = queue.ingest_receipt(tmp_path, receipt, provider='finlab', link_column='url',
                                            maximum_wall_seconds=.05, stop=http.stop)
            queue.metadata_inventory_pending = not complete
            reader = getattr(queue, '_metadata_receipt_readers', {}).get(str(receipt.resolve()))
            hint = reader.priority_hint if reader else None
            refreshes.append((complete, reader.processed_rows if reader else None,
                              hint.future.done() if hint else None, hint.done if hint else None))
        try:
            main.run_service(queue, http, config, max_articles=0, max_discovery_requests=0,
                             refresh_metadata=refresh, duration_seconds=15)
            assert observed and observed[0] is not None and observed[0][0] < observed[0][1], {
                'events': events, 'refreshes': refreshes[:30], 'pending': len(inbox(queue)),
                'markers': queue.db.execute("SELECT count(*) FROM queue_settings WHERE key LIKE 'finlab_cnyes_delta_hint_v1:%'").fetchone()[0],
                'progress': [tuple(row) for row in queue.db.execute('SELECT processed_rows,expected_rows FROM metadata_ingest_progress')],
            }
            assert not session.calls
        finally:
            http.close()


