"""Bounded receipt lookup preserves exact crash-replay validation and order."""
from contextlib import closing
import json
import shutil
import sqlite3
import threading
import time

import pytest

from downloader.artifact_io import atomic_write_json
from downloader import news_fulltext as fulltext_module
from downloader import news_fulltext_queue as queue_module
from downloader.news_fulltext import fetch_article, run_cycle
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_queue import sorted_acceptance_receipt_paths
from stockagent.data.news_worker_lifecycle import NewsWorkerLifecycle
from stockagent.live.news_dashboard import build_news_public_status
from test_news_fulltext import config, http_fixture, FakeResponse, article_html


URL = 'https://publisher.test/news/1'


def test_live_article_recovers_a_real_sqlite_busy_commit_without_refetch(config, tmp_path, monkeypatch):
    """A locked commit reuses its exact durable receipt within the same owner."""
    config['batch_articles'] = 1
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    queue.db.execute('PRAGMA busy_timeout=10')
    original = queue._accept_records
    attempts = []

    def commit_with_one_real_lock(*args, **kwargs):
        attempts.append(args[1])
        if len(attempts) > 1:
            return original(*args, **kwargs)
        locker = sqlite3.connect(tmp_path / 'queue.sqlite3', timeout=.01)
        try:
            locker.execute('BEGIN IMMEDIATE')
            return original(*args, **kwargs)
        finally:
            locker.rollback()
            locker.close()

    monkeypatch.setattr(queue, '_accept_records', commit_with_one_real_lock)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *args: False)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        status = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert len(attempts) == 2 and attempts[0] == attempts[1]
        assert status['performance']['providers']['publisher']['counts']['local_sqlite_busy_commit_events'] == 1
        from stockagent.live.news_dashboard import build_news_public_status
        web_root = tmp_path / 'web-root'
        atomic_write_json(web_root / 'data_news_fulltext/status.json', status)
        atomic_write_json(web_root / 'configs/news_fulltext.json', {
            'providers': [{'id': 'publisher', 'name': '測試來源', 'enabled': True}],
        })
        public = build_news_public_status(web_root)
        provider = next(row for row in public['providers'] if row['id'] == 'publisher')
        assert provider['owner_local_sqlite_busy_commit_events'] == 1
        assert [called for called, _ in session.calls].count(URL) == 1
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert len(list((tmp_path / 'receipts').glob('*.json'))) == 1
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        queue.close()
        http.close()


def test_live_busy_wait_does_not_rehash_body_while_writer_remains_locked(config, tmp_path, monkeypatch):
    """Repeated write contention must not reread the same durable Parquet."""
    config['batch_articles'] = 1
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    queue.db.execute('PRAGMA busy_timeout=1')
    locker = sqlite3.connect(tmp_path / 'queue.sqlite3', timeout=.001)
    original_commit = queue._accept_records
    original_verify = queue_module.verify_immutable_body_receipt
    original_wait = http.stop.wait
    attempts, waits, verifications = [], [], []

    def commit_with_real_lock(*args, **kwargs):
        attempts.append(args[1])
        if len(attempts) == 1:
            locker.execute('BEGIN IMMEDIATE')
        return original_commit(*args, **kwargs)

    def controlled_wait(seconds):
        if locker.in_transaction:
            waits.append(seconds)
            if len(waits) == 4:
                locker.rollback()
            return False
        return original_wait(seconds)

    def verify(*args, **kwargs):
        verifications.append((args[1]['parquet_path'], locker.in_transaction))
        return original_verify(*args, **kwargs)

    monkeypatch.setattr(queue, '_accept_records', commit_with_real_lock)
    monkeypatch.setattr(queue_module, 'verify_immutable_body_receipt', verify)
    monkeypatch.setattr(http.stop, 'wait', controlled_wait)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *args: False)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        status = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert len(waits) == 4
        # Check once immediately, then again when the writer becomes available.
        assert len(verifications) == 2
        assert [locked for _, locked in verifications] == [True, False]
        assert len(set(attempts)) == 1 and len(attempts) == 2
        assert status['performance']['providers']['publisher']['counts']['local_sqlite_busy_commit_events'] == 1
        assert [called for called, _ in session.calls].count(URL) == 1
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert len(list((tmp_path / 'receipts').glob('*.json'))) == 1
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        if locker.in_transaction:
            locker.rollback()
        locker.close()
        queue.close()
        http.close()


def test_live_busy_publishes_current_wait_then_requires_a_new_snapshot(config, tmp_path, monkeypatch):
    """The dashboard must not present a locked owner as currently downloading."""
    config['batch_articles'] = 1
    root = tmp_path / 'data_news_fulltext'
    root.mkdir()
    http, session = http_fixture(config, root, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    lifecycle = NewsWorkerLifecycle(root)
    queue = NewsQueue(root, http.registry, lifecycle=lifecycle)
    queue.db.execute('PRAGMA busy_timeout=1')
    locker = sqlite3.connect(root / 'queue.sqlite3', timeout=.001)
    original_commit, original_wait = queue._accept_records, http.stop.wait
    original_accept = queue.accept_live_article_batch
    original_phase = lifecycle.phase
    waits, attempts, recovery, phases = [], [], [], []

    class Clock:
        offset = 0

        def monotonic(self):
            return time.monotonic() + self.offset

        def __getattr__(self, name):
            return getattr(time, name)

    clock = Clock()
    atomic_write_json(tmp_path / 'configs/news_fulltext.json', {
        'providers': [{'id': 'publisher', 'name': '測試來源', 'enabled': True}],
    })

    def commit_with_real_lock(*args, **kwargs):
        attempts.append(args[1])
        if len(attempts) == 1:
            locker.execute('BEGIN IMMEDIATE')
        return original_commit(*args, **kwargs)

    def wait(seconds):
        if not locker.in_transaction:
            return original_wait(seconds)
        current = build_news_public_status(tmp_path)
        waits.append(current)
        assert current['health'] == 'waiting_local_database'
        assert current['worker']['lifecycle']['phase'] == 'waiting_local_sqlite'
        assert current['acquisition']['eligible_eta_seconds'] is None
        assert all(p['conditional_eta_seconds'] is None for p in current['providers'])
        clock.offset += 16  # Cross one heartbeat interval while still locked.
        if len(waits) == 2:
            locker.rollback()
        return False

    def accept_then_observe(*args, **kwargs):
        result = original_accept(*args, **kwargs)
        recovery.append(build_news_public_status(tmp_path))
        return result

    def phase(name, **kwargs):
        phases.append(name)
        return original_phase(name, **kwargs)

    monkeypatch.setattr(queue_module, 'time', clock)
    monkeypatch.setattr(queue, '_accept_records', commit_with_real_lock)
    monkeypatch.setattr(queue, 'accept_live_article_batch', accept_then_observe)
    monkeypatch.setattr(lifecycle, 'phase', phase)
    monkeypatch.setattr(http.stop, 'wait', wait)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *args: False)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        queue.status(activity='running')
        assert build_news_public_status(tmp_path)['health'] == 'running'
        status = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert len(waits) == 2 and len(set(attempts)) == 1 and len(attempts) == 2
        assert phases.count('waiting_local_sqlite') == 2
        assert recovery[0]['health'] == 'waiting_local_snapshot'
        assert recovery[0]['worker']['lifecycle']['phase'] == 'local_sqlite_retry_finished'
        assert recovery[0]['acquisition']['eligible_eta_seconds'] is None
        assert build_news_public_status(tmp_path)['health'] == 'waiting'
        assert status['cycle']['durably_accepted_versions'] == 1
        assert [called for called, _ in session.calls].count(URL) == 1
        assert verify_bodies(root)['body_versions_verified'] == 1
    finally:
        if locker.in_transaction:
            locker.rollback()
        locker.close()
        queue.close()
        http.close()


def test_live_busy_lifecycle_write_failure_never_discards_saved_batch(config, tmp_path, monkeypatch):
    from downloader import artifact_io

    config['batch_articles'] = 1
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    lifecycle = NewsWorkerLifecycle(tmp_path)
    queue = NewsQueue(tmp_path, http.registry, lifecycle=lifecycle)
    queue.db.execute('PRAGMA busy_timeout=1')
    locker = sqlite3.connect(tmp_path / 'queue.sqlite3', timeout=.001)
    original_commit, original_wait, original_write = queue._accept_records, http.stop.wait, artifact_io.atomic_write_json
    attempts, waits = [], []

    def commit_with_real_lock(*args, **kwargs):
        attempts.append(args[1])
        if len(attempts) == 1:
            locker.execute('BEGIN IMMEDIATE')
        return original_commit(*args, **kwargs)

    def wait(seconds):
        if not locker.in_transaction:
            return original_wait(seconds)
        waits.append(seconds)
        if len(waits) == 2:
            locker.rollback()
        return False

    def write(path, *args, **kwargs):
        if path == lifecycle.path:
            raise OSError('telemetry_only_failure')
        return original_write(path, *args, **kwargs)

    monkeypatch.setattr(queue, '_accept_records', commit_with_real_lock)
    monkeypatch.setattr(http.stop, 'wait', wait)
    monkeypatch.setattr(artifact_io, 'atomic_write_json', write)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *args: False)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert lifecycle.write_errors >= 2
        assert len(attempts) == 2 and len(set(attempts)) == 1
        assert [called for called, _ in session.calls].count(URL) == 1
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        if locker.in_transaction:
            locker.rollback()
        locker.close()
        queue.close()
        http.close()


def test_live_busy_detects_changed_receipt_before_locked_writer_is_released(config, tmp_path, monkeypatch):
    config['batch_articles'] = 1
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    queue.db.execute('PRAGMA busy_timeout=1')
    locker = sqlite3.connect(tmp_path / 'queue.sqlite3', timeout=.001)
    original = queue._accept_records
    original_wait = http.stop.wait
    waits = []

    def corrupt_and_lock(*args, **kwargs):
        locker.execute('BEGIN IMMEDIATE')
        next((tmp_path / 'receipts').glob('*.json')).write_text('{}')
        return original(*args, **kwargs)

    def wait(seconds):
        if not locker.in_transaction:
            return original_wait(seconds)
        waits.append(seconds)
        if len(waits) == 4:
            locker.rollback()  # Ensure a faulty retry cannot hang this test.
        return False

    monkeypatch.setattr(queue, '_accept_records', corrupt_and_lock)
    monkeypatch.setattr(http.stop, 'wait', wait)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *args: False)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        with pytest.raises(ValueError, match='live_acceptance_receipt_changed'):
            run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert len(waits) == 1 and locker.in_transaction
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert [called for called, _ in session.calls].count(URL) == 1
    finally:
        if locker.in_transaction:
            locker.rollback()
        locker.close()
        queue.close()
        http.close()


def test_live_busy_reads_committed_batch_even_when_another_writer_is_locked(config, tmp_path, monkeypatch):
    config['batch_articles'] = 1
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    queue.db.execute('PRAGMA busy_timeout=1')
    locker = sqlite3.connect(tmp_path / 'queue.sqlite3', timeout=.001)
    original_commit, original_accept = queue._accept_records, queue.accept
    original_wait = http.stop.wait
    attempts, waits = [], []

    def committed_then_uncertain(*args, **kwargs):
        attempts.append(args[1])
        original_commit(*args, **kwargs)
        locker.execute('BEGIN IMMEDIATE')
        raise sqlite3.OperationalError('database is locked')

    def wait(seconds):
        if not locker.in_transaction:
            return original_wait(seconds)
        waits.append(seconds)
        if len(waits) == 2 and locker.in_transaction:
            locker.rollback()  # An unnecessary write probe would reach this.
        return False

    def accept_then_release(*args, **kwargs):
        try:
            return original_accept(*args, **kwargs)
        finally:
            if locker.in_transaction:
                locker.rollback()

    monkeypatch.setattr(queue, '_accept_records', committed_then_uncertain)
    monkeypatch.setattr(queue, 'accept', accept_then_release)
    monkeypatch.setattr(http.stop, 'wait', wait)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *args: False)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        status = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
        assert len(waits) == len(attempts) == 1
        assert status['performance']['providers']['publisher']['counts']['local_sqlite_busy_commit_events'] == 1
        assert [called for called, _ in session.calls].count(URL) == 1
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        if locker.in_transaction:
            locker.rollback()
        locker.close()
        queue.close()
        http.close()


@pytest.mark.parametrize('run_mode', ['cycle', 'service'])
@pytest.mark.parametrize('batch_articles', [1, 2])
def test_live_busy_finishes_leased_article_after_admission_deadline(config, tmp_path, monkeypatch, batch_articles, run_mode):
    """The soft cycle deadline rejects new work, not an already durable batch."""
    config['workers'] = 1
    config['batch_articles'] = batch_articles
    second_url = 'https://publisher.test/news/2'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
        second_url: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    queue.db.execute('PRAGMA busy_timeout=1')
    locker = sqlite3.connect(tmp_path / 'queue.sqlite3', timeout=.001)
    original_commit, original_wait = queue._accept_records, http.stop.wait
    attempts, waits = [], []

    class Clock:
        offset = 0

        def monotonic(self):
            return time.monotonic() + self.offset

        def __getattr__(self, name):
            return getattr(time, name)

    clock = Clock()

    def commit_after_deadline(*args, **kwargs):
        attempts.append(args[1])
        if len(attempts) == 1:
            locker.execute('BEGIN IMMEDIATE')
            clock.offset = 10
        return original_commit(*args, **kwargs)

    def controlled_wait(seconds):
        if not locker.in_transaction:
            return original_wait(seconds)
        waits.append(seconds)
        if len(waits) == 2:
            locker.rollback()
        return False

    monkeypatch.setattr(fulltext_module, 'time', clock)
    monkeypatch.setattr(queue, '_accept_records', commit_after_deadline)
    monkeypatch.setattr(http.stop, 'wait', controlled_wait)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *args: False)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}, *([{'url': second_url}] if run_mode == 'cycle' else [])])
        if run_mode == 'service':
            status = fulltext_module.run_service(queue, http, config, max_articles=1,
                                                 max_discovery_requests=0, duration_seconds=1)
            assert status['activity'] == 'service_duration_complete'
        else:
            status = run_cycle(queue, http, config, max_articles=2 if batch_articles == 1 else 1,
                               max_discovery_requests=0, max_run_seconds=1)
        assert len(waits) == 2 and len(attempts) == 2 and len(set(attempts)) == 1
        article_calls = [called for called, _ in session.calls if called in {URL, second_url}]
        assert len(article_calls) == 1
        expected_states = ['complete', 'pending'] if run_mode == 'cycle' else ['complete']
        assert sorted(row[0] for row in queue.db.execute('SELECT state FROM articles')) == expected_states
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert len(list((tmp_path / 'receipts').glob('*.json'))) == 1
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
        assert status['cycle']['durably_accepted_versions'] == 1
        assert not http.stop.is_set()
    finally:
        if locker.in_transaction:
            locker.rollback()
        locker.close()
        queue.close()
        http.close()


@pytest.mark.parametrize('failure', ['after_commit', 'newer_refusal', 'busy_twice', 'stop',
                                      'stop_during_wait', 'not_busy', 'receipt_changed', 'parquet_changed'])
def test_live_busy_retry_preserves_commit_identity_and_failure_boundaries(config, tmp_path, monkeypatch, failure):
    config['batch_articles'] = 1
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    registry = http.registry
    original = queue._accept_records
    attempts = []
    timers = []

    def uncertain_commit(*args, **kwargs):
        attempts.append(args[1])
        if len(attempts) > 1 and failure not in {'busy_twice', 'stop_during_wait'}:
            return original(*args, **kwargs)
        if failure == 'busy_twice' and len(attempts) > 2:
            return original(*args, **kwargs)
        if failure == 'after_commit':
            original(*args, **kwargs)
        elif failure == 'newer_refusal':
            with queue.db:
                queue.db.execute("UPDATE articles SET state='http_403', error_code='http_403', "
                                 "lease_owner=NULL, lease_until=NULL, lease_token=NULL WHERE url=?", (URL,))
        elif failure == 'stop':
            http.stop.set()
        elif failure == 'stop_during_wait' and len(attempts) == 1:
            timer = threading.Timer(.1, http.stop.set)
            timers.append(timer)
            timer.start()
        elif failure == 'receipt_changed':
            next((tmp_path / 'receipts').glob('*.json')).write_text('{}')
        elif failure == 'parquet_changed':
            receipt = json.loads(next((tmp_path / 'receipts').glob('*.json')).read_text())
            with (tmp_path / receipt['parquet_path']).open('ab') as output:
                output.write(b'changed')
        error = sqlite3.OperationalError('injected sqlite failure')
        error.sqlite_errorcode = sqlite3.SQLITE_ERROR if failure == 'not_busy' else sqlite3.SQLITE_BUSY
        raise error

    monkeypatch.setattr(queue, '_accept_records', uncertain_commit)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *args: False)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
        if failure in {'stop', 'stop_during_wait', 'not_busy', 'receipt_changed', 'parquet_changed'}:
            expected = ValueError if failure.endswith('_changed') else sqlite3.OperationalError
            with pytest.raises(expected):
                run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
            if failure != 'stop_during_wait':
                assert len(attempts) == 1
            else:
                assert attempts
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        else:
            run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0)
            assert len(attempts) == (1 if failure == 'after_commit' else 3 if failure == 'busy_twice' else 2)
            assert len(set(attempts)) == 1
            head = queue.db.execute('SELECT state, error_code FROM articles WHERE url=?', (URL,)).fetchone()
            assert tuple(head) == (('http_403', 'http_403') if failure == 'newer_refusal' else ('complete', None))
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
            assert verify_bodies(tmp_path)['body_versions_verified'] == 1
            counts = queue.metrics.snapshot()['providers']['publisher']['counts']
            assert counts['durably_accepted_versions'] == 1
            assert counts['local_sqlite_busy_commit_events'] == (2 if failure == 'busy_twice' else 1)
        assert len(list((tmp_path / 'receipts').glob('*.json'))) == 1
        assert [called for called, _ in session.calls].count(URL) == 1
    finally:
        for timer in timers:
            timer.cancel()
            timer.join()
        queue.close()
        http.close()
    if failure in {'stop', 'stop_during_wait', 'not_busy'}:
        with closing(NewsQueue(tmp_path, registry)) as resumed:
            assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
            assert resumed.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
            assert verify_bodies(tmp_path)['body_versions_verified'] == 1


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


@pytest.mark.parametrize('replacement', ['missing', 'file'])
def test_unavailable_committed_receipt_directory_fails_then_recovers(config, tmp_path, replacement):
    queue, http = accepted_queue(config, tmp_path)
    directory = tmp_path / 'receipts'
    saved = tmp_path / 'saved_receipts'
    try:
        batches_before = [tuple(row) for row in queue.db.execute(
            'SELECT path,receipt_path FROM accepted_batches')]
        body_before = [tuple(row) for row in queue.db.execute(
            'SELECT state,parquet_path,body_sha256 FROM articles')]
        directory.rename(saved)
        if replacement == 'file':
            directory.write_text('not a directory')
        expected = FileNotFoundError if replacement == 'missing' else NotADirectoryError
        with pytest.raises(expected):
            queue.recover_acceptance()
        assert queue.acceptance_receipt_io['completed'] is False
        assert [tuple(row) for row in queue.db.execute(
            'SELECT path,receipt_path FROM accepted_batches')] == batches_before
        assert [tuple(row) for row in queue.db.execute(
            'SELECT state,parquet_path,body_sha256 FROM articles')] == body_before
        if replacement == 'file':
            directory.unlink()
        saved.rename(directory)
        queue.recover_acceptance()
        assert queue.acceptance_receipt_io['completed'] is True
        assert queue.acceptance_receipt_io['receipts_decoded'] == 1
        assert [tuple(row) for row in queue.db.execute(
            'SELECT path,receipt_path FROM accepted_batches')] == batches_before
        assert [tuple(row) for row in queue.db.execute(
            'SELECT state,parquet_path,body_sha256 FROM articles')] == body_before
    finally:
        queue.close()
        http.close()


def test_startup_releases_owner_after_unavailable_committed_receipts(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    registry = http.registry
    queue.close()
    directory = tmp_path / 'receipts'
    saved = tmp_path / 'saved_receipts'
    directory.rename(saved)
    try:
        # Keep the first traceback alive; a new attempt must not depend on GC
        # to release the failed constructor's SQLite connection or owner lock.
        with pytest.raises(FileNotFoundError) as first_failure:
            NewsQueue(tmp_path, registry)
        assert first_failure.value is not None
        with pytest.raises(FileNotFoundError):
            NewsQueue(tmp_path, registry)
        saved.rename(directory)
        restarted = NewsQueue(tmp_path, registry)
        try:
            assert restarted.acceptance_receipt_io['completed'] is True
            assert restarted.acceptance_receipt_io['receipts_decoded'] == 1
            assert restarted.db.execute('SELECT COUNT(*) FROM accepted_batches').fetchone()[0] == 1
        finally:
            restarted.close()
    finally:
        http.close()


@pytest.mark.parametrize('error_type', [OSError, KeyboardInterrupt])
@pytest.mark.parametrize('module_name,initializer', [
    ('downloader.news_fulltext_cnyes_history', 'initialize_accepted_batch_candidates'),
    ('downloader.news_fulltext_cnyes_history', 'initialize'),
    ('downloader.news_fulltext_cnyes_index', 'initialize'),
])
def test_startup_initializer_failure_releases_owner_with_traceback_retained(
        config, tmp_path, monkeypatch, module_name, initializer, error_type):
    import importlib

    http, _ = http_fixture(config, tmp_path, {})
    module = importlib.import_module(module_name)

    def fail(_queue):
        raise error_type('injected startup failure')

    try:
        with monkeypatch.context() as patch:
            patch.setattr(module, initializer, fail)
            with pytest.raises(error_type, match='injected startup failure') as first_failure:
                NewsQueue(tmp_path, http.registry)
        # Keep the constructor traceback alive; cleanup cannot rely on GC.
        assert first_failure.value is not None
        restarted = NewsQueue(tmp_path, http.registry)
        try:
            assert restarted.db.execute('SELECT 1').fetchone()[0] == 1
        finally:
            restarted.close()
    finally:
        http.close()


def test_startup_database_open_failure_releases_owner_with_traceback_retained(config, tmp_path, monkeypatch):
    import sqlite3

    http, _ = http_fixture(config, tmp_path, {})
    try:
        def fail(*_args, **_kwargs):
            raise sqlite3.OperationalError('injected database open failure')

        with monkeypatch.context() as patch:
            patch.setattr(queue_module.sqlite3, 'connect', fail)
            with pytest.raises(sqlite3.OperationalError, match='injected database open failure') as first_failure:
                NewsQueue(tmp_path, http.registry)
        assert first_failure.value is not None
        restarted = NewsQueue(tmp_path, http.registry)
        try:
            assert restarted.db.execute('SELECT 1').fetchone()[0] == 1
        finally:
            restarted.close()
    finally:
        http.close()


def test_startup_failure_rolls_back_uncommitted_work_and_preserves_accepted_receipts(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    queue, http = accepted_queue(config, tmp_path)
    batch_count = queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]
    body_count = queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0]
    queue.close()

    def fail(queue):
        queue.db.execute("INSERT INTO queue_settings VALUES ('injected_uncommitted_startup', 'pending')")
        raise OSError('injected migration failure')

    try:
        with monkeypatch.context() as patch:
            patch.setattr(history, 'initialize', fail)
            with pytest.raises(OSError, match='injected migration failure') as first_failure:
                NewsQueue(tmp_path, http.registry)
        assert first_failure.value is not None
        restarted = NewsQueue(tmp_path, http.registry)
        try:
            assert restarted.db.execute("SELECT 1 FROM queue_settings WHERE key='injected_uncommitted_startup'").fetchone() is None
            assert restarted.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == batch_count
            assert restarted.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == body_count
        finally:
            restarted.close()
    finally:
        http.close()


def test_startup_cleanup_error_does_not_mask_failure_or_retain_owner(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history, news_fulltext_metadata_legacy as metadata

    http, _ = http_fixture(config, tmp_path, {})

    def fail_startup(_queue):
        raise OSError('injected startup failure')

    def fail_cleanup(_queue):
        raise ValueError('injected cleanup failure')

    try:
        with monkeypatch.context() as patch:
            patch.setattr(history, 'initialize', fail_startup)
            patch.setattr(metadata, 'close_scanner', fail_cleanup)
            with pytest.raises(OSError, match='injected startup failure') as first_failure:
                NewsQueue(tmp_path, http.registry)
        assert any('startup cleanup failed: ValueError' in note for note in first_failure.value.__notes__)
        restarted = NewsQueue(tmp_path, http.registry)
        restarted.close()
    finally:
        http.close()


def test_close_releases_later_resources_when_scanner_cleanup_fails(monkeypatch):
    from downloader import news_fulltext_metadata_ingest as ingest, news_fulltext_metadata_legacy as metadata

    calls = []

    class Resource:
        def __init__(self, name):
            self.name = name

        def close(self):
            calls.append(self.name)

    def fail_scanner(_queue):
        calls.append('scanner')
        raise ValueError('injected scanner close failure')

    def close_readers(_queue):
        calls.append('readers')

    queue = NewsQueue.__new__(NewsQueue)
    queue.owner_lock = Resource('owner_lock')
    queue.db = Resource('database')
    queue._cached_cnyes_inventory_entries = Resource('entries')
    queue._large_metadata_job = {'job': Resource('job')}
    monkeypatch.setattr(metadata, 'close_scanner', fail_scanner)
    monkeypatch.setattr(ingest, 'close_readers', close_readers)
    with pytest.raises(ValueError, match='injected scanner close failure'):
        queue.close()
    assert calls == ['scanner', 'job', 'readers', 'entries', 'database', 'owner_lock']


def test_startup_lifecycle_failure_before_database_open_releases_owner(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})

    class FailingLifecycle:
        def started(self, _owner):
            raise OSError('injected lifecycle failure')

    try:
        with pytest.raises(OSError, match='injected lifecycle failure') as first_failure:
            NewsQueue(tmp_path, http.registry, lifecycle=FailingLifecycle())
        assert first_failure.value is not None
        restarted = NewsQueue(tmp_path, http.registry)
        restarted.close()
    finally:
        http.close()


def test_empty_legacy_queue_allows_absent_receipt_directory_but_records_it(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        assert not (tmp_path / 'receipts').exists()
        assert queue.acceptance_receipt_io['completed'] is True
        assert queue.acceptance_receipt_io['receipt_directory_state'] == 'absent'
        queue.recover_acceptance(receipt_paths=[])
        assert queue.acceptance_receipt_io['receipt_directory_state'] == 'explicit_paths'
    finally:
        queue.close()
        http.close()


def test_receipt_root_file_rejects_even_an_empty_queue_and_releases_owner(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    (tmp_path / 'receipts').write_text('not a directory')
    try:
        with pytest.raises(NotADirectoryError) as first_failure:
            NewsQueue(tmp_path, http.registry)
        assert first_failure.value is not None
        with pytest.raises(NotADirectoryError):
            NewsQueue(tmp_path, http.registry)
        (tmp_path / 'receipts').unlink()
        queue = NewsQueue(tmp_path, http.registry)
        queue.close()
    finally:
        http.close()


@pytest.mark.parametrize('state', ['retry', 'source_not_found', 'paywall_or_login'])
def test_verifier_checks_retained_body_independently_of_latest_state(config, tmp_path, state):
    queue, http = accepted_queue(config, tmp_path)
    try:
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?', (state,))
        proof = verify_bodies(tmp_path)
        assert proof['accepted']
        assert proof['complete_queue_heads_verified'] == 0
        assert proof['stored_body_queue_heads_checked'] == 1
        assert proof['stored_body_queue_heads_verified'] == 1
        assert proof['noncomplete_stored_body_queue_heads_checked'] == 1
        assert proof['noncomplete_stored_body_queue_heads_verified'] == 1
        with queue.db:
            queue.db.execute("UPDATE articles SET body_sha256=?", ('0' * 64,))
        proof = verify_bodies(tmp_path)
        assert not proof['accepted']
        assert proof['stored_body_queue_heads_verified'] == 0
        assert any(error['error'] == 'queue_head_missing_verified_body' for error in proof['errors'])
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('state,missing', [
    ('complete', 'both'), ('complete', 'path'), ('complete', 'digest'),
    ('retry', 'path'), ('retry', 'digest'),
])
def test_verifier_rejects_missing_or_partial_head_pointer(config, tmp_path, state, missing):
    queue, http = accepted_queue(config, tmp_path)
    try:
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?', (state,))
            if missing in {'both', 'path'}:
                queue.db.execute('UPDATE articles SET parquet_path=NULL')
            if missing in {'both', 'digest'}:
                queue.db.execute('UPDATE articles SET body_sha256=NULL')
        proof = verify_bodies(tmp_path)
        assert not proof['accepted']
        assert any(error['error'] == 'queue_head_missing_verified_body' for error in proof['errors'])
    finally:
        queue.close()
        http.close()


def test_verifier_requires_exact_committed_receipt_for_current_head(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        with queue.db:
            queue.db.execute('DELETE FROM accepted_batches')
        proof = verify_bodies(tmp_path)
        assert not proof['accepted']
        assert any(error['error'] == 'queue_head_missing_verified_body' for error in proof['errors'])
        assert proof['uncommitted_receipts_observed'] == 1
    finally:
        queue.close()
        http.close()


def test_verifier_detects_missing_committed_receipt_even_if_head_is_not_current(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        receipt = next((tmp_path / 'receipts').glob('*.json'))
        receipt.unlink()
        with queue.db:
            queue.db.execute("UPDATE articles SET state='pending',parquet_path=NULL,body_sha256=NULL,body_bytes=0")
        proof = verify_bodies(tmp_path)
        assert not proof['accepted']
        assert any(error['error'] == 'committed_receipt_missing_or_invalid' for error in proof['errors'])
    finally:
        queue.close()
        http.close()


def test_verifier_allows_duplicate_receipt_without_confusing_commit_identity(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        receipt = next((tmp_path / 'receipts').glob('*.json'))
        shutil.copyfile(receipt, tmp_path / 'receipts' / 'z-copy.json')
        proof = verify_bodies(tmp_path)
        assert proof['accepted']
        assert proof['committed_batches_verified'] == 1
        assert proof['uncommitted_receipts_observed'] == 1
        assert proof['body_versions_verified'] == 2
        assert proof['committed_body_versions_verified'] == 1
        assert proof['uncommitted_body_versions_verified'] == 1
    finally:
        queue.close()
        http.close()


def test_duplicate_receipt_cannot_replace_missing_committed_original(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        receipt = next((tmp_path / 'receipts').glob('*.json'))
        shutil.copyfile(receipt, tmp_path / 'receipts' / 'z-copy.json')
        receipt.unlink()
        proof = verify_bodies(tmp_path)
        assert not proof['accepted']
        assert proof['complete_queue_heads_checked'] == 1
        assert proof['complete_queue_heads_verified'] == 0
        assert {error['error'] for error in proof['errors']} >= {
            'queue_head_missing_verified_body', 'committed_receipt_missing_or_invalid'}
    finally:
        queue.close()
        http.close()


def test_bad_receipt_bytes_cannot_validate_a_head(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        receipt_path = next((tmp_path / 'receipts').glob('*.json'))
        receipt = json.loads(receipt_path.read_text())
        receipt['text_bytes'] += 1
        atomic_write_json(receipt_path, receipt)
        proof = verify_bodies(tmp_path)
        assert not proof['accepted']
        assert proof['body_versions_verified'] == 0
        assert proof['complete_queue_heads_verified'] == 0
        assert any(error['error'] == 'queue_head_missing_verified_body' for error in proof['errors'])
    finally:
        queue.close()
        http.close()


def test_verifier_receipt_lookup_is_bounded_and_supports_old_unindexed_snapshot(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        for number in range(260):
            (tmp_path / 'receipts' / f'copy-{number:03}.json').write_text('{}')
        statements = []
        queue.db.set_trace_callback(statements.append)
        inputs = list(queue_module._verification_receipt_inputs(tmp_path, queue.db))
        assert len(inputs) == 261
        assert len([sql for sql in statements if sql.startswith('SELECT receipt_path,path')]) == 3
        assert sum(relative == next(iter(committed))[0] for _, relative, committed in inputs if committed) == 1
        queue.db.set_trace_callback(None)
        queue.db.execute('DROP INDEX accepted_batches_receipt_path')
        inputs = list(queue_module._verification_receipt_inputs(tmp_path, queue.db))
        assert len(inputs) == 261
        assert sum(relative == next(iter(committed))[0] for _, relative, committed in inputs if committed) == 1
    finally:
        queue.db.set_trace_callback(None)
        queue.close()
        http.close()


def test_verifier_reconciles_stored_body_progress_counter(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        with queue.db:
            queue.db.execute('UPDATE state_metrics SET stored_body_urls=0')
        proof = verify_bodies(tmp_path)
        assert not proof['accepted']
        assert any(error['error'] == 'incremental_progress_metrics_mismatch' for error in proof['errors'])
    finally:
        queue.close()
        http.close()


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
        original_read = queue_module.read_acceptance_receipt_input
        def read(path, *args, **kwargs):
            reads.append(path)
            return original_read(path, *args, **kwargs)
        monkeypatch.setattr(queue_module, 'read_acceptance_receipt_input', read)
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
            # This synthetic reset is not an abandoned lease: it discards the
            # old head but leaves a later check clock. Retain the validated
            # receipt without inventing an entitlement to overwrite that head.
            head = queue.db.execute('SELECT state,body_sha256 FROM articles').fetchone()
            assert head['state'] == 'pending' and head['body_sha256'] is None
            assert queue.db.execute("SELECT value FROM counters WHERE key='article_capture_superseded_at_commit'").fetchone()[0] == 1
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
