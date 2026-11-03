"""Resident URL registration keeps one writer and ordinary source admission."""
from collections import Counter
from datetime import UTC, datetime, timedelta
import errno
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_wake import OwnerMailboxWake, OwnerStopEvent
from downloader import news_fulltext_url_metadata as intake
from test_news_fulltext import config


URL = 'https://publisher.test/story/1'


@pytest.fixture
def queue(config, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        yield owner
    finally:
        owner.close()


def submit(queue, urls=(URL,)):
    return intake.submit_urls(queue.root, queue.registry, iter(urls))


def head(queue, url=URL):
    row = queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
    return dict(row) if row else None


def consume(queue, **kwargs):
    return intake.consume_urls(queue, max_seconds=.1, **kwargs)


@pytest.mark.parametrize('slow_step', ['discovery', 'validation'])
def test_slow_url_inbox_still_registers_one_per_slice(queue, monkeypatch, slow_step):
    submit(queue, (URL, URL + '2'))
    now = [0.0]
    monkeypatch.setattr(intake, 'time', SimpleNamespace(monotonic=lambda: now[0], time=time.time))
    name = '_pending' if slow_step == 'discovery' else '_load'
    original = getattr(intake, name)

    def delayed(*args):
        result = original(*args)
        now[0] += 1.0
        return result

    monkeypatch.setattr(intake, name, delayed)
    for _ in range(2):
        assert intake.consume_urls(queue, limit=1, max_seconds=.05)['outcomes'] == {'metadata_registered': 1}
    assert head(queue, URL)['state'] == head(queue, URL + '2')['state'] == 'pending'


def test_slow_url_requests_rotate_between_slices(queue, monkeypatch):
    submissions = [submit(queue, tuple(f'{URL}-{group}-{i}' for i in range(2))) for group in ('a', 'b')]
    now = [0.0]
    monkeypatch.setattr(intake, 'time', SimpleNamespace(monotonic=lambda: now[0], time=time.time))
    original = intake._load

    def delayed(path):
        request = original(path)
        now[0] += 1.0
        return request

    monkeypatch.setattr(intake, '_load', delayed)
    for _ in range(2):
        assert intake.consume_urls(queue, limit=1, max_seconds=.05)['processed'] == 1
    admitted = dict(queue.db.execute('SELECT request_id,count(*) FROM url_metadata_items GROUP BY request_id'))
    assert admitted == {request['request_id']: 1 for request in submissions}


def test_stop_during_url_validation_does_not_register(queue, monkeypatch):
    submit(queue)
    stop = threading.Event()
    original = intake._load

    def interrupted(path):
        request = original(path)
        stop.set()
        return request

    monkeypatch.setattr(intake, '_load', interrupted)
    assert intake.consume_urls(queue, limit=1, stop=stop)['processed'] == 0
    assert head(queue) is None
    assert not queue.db.execute("SELECT 1 FROM sqlite_master WHERE name='url_metadata_jobs'").fetchone()


def test_transient_url_request_read_failure_is_deferred_and_other_request_runs(queue, monkeypatch):
    submitted = [submit(queue, (url,)) for url in (URL, URL + '2')]
    paths = sorted(Path(item['pending_path']) for item in submitted)
    original = intake._load
    unavailable = [True]

    def transient(path):
        if path == paths[0] and unavailable[0]:
            raise OSError(errno.EIO, 'temporary request read error')
        return original(path)

    monkeypatch.setattr(intake, '_load', transient)
    assert intake.consume_urls(queue, limit=1, max_seconds=.1)['outcomes'] == {'request_io_deferred': 1}
    assert paths[0].exists() and head(queue, URL) is None and head(queue, URL + '2') is None
    assert intake.consume_urls(queue, limit=1, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
    unavailable[0] = False
    assert intake.consume_urls(queue, limit=1, max_seconds=.1)['outcomes'] == {'metadata_registered': 1}
    assert head(queue, URL)['state'] == head(queue, URL + '2')['state'] == 'pending'


def test_storage_failure_rolls_back_whole_url_slice_and_retries(queue, monkeypatch):
    result = submit(queue, (URL, URL + '2'))
    add = queue.add_articles
    calls = [0]

    def interrupted(rows):
        add(rows)
        calls[0] += 1
        if calls[0] == 2:
            raise OSError(errno.EIO, 'temporary write failure')

    monkeypatch.setattr(queue, 'add_articles', interrupted)
    monkeypatch.setattr(intake, 'time', SimpleNamespace(monotonic=lambda: 0.0, time=time.time))
    first = intake.consume_urls(queue, limit=2, max_seconds=.1)
    assert first['outcomes'] == {'url_metadata_storage_deferred': 1}
    assert head(queue, URL) is None and head(queue, URL + '2') is None
    assert not queue.db.execute('SELECT 1 FROM url_metadata_items').fetchone()
    assert Path(result['pending_path']).exists()
    monkeypatch.setattr(queue, 'add_articles', add)
    assert intake.consume_urls(queue, limit=2, max_seconds=.1)['outcomes'] == {'metadata_registered': 2}
    assert head(queue, URL)['state'] == head(queue, URL + '2')['state'] == 'pending'


def rewrite(path, request=None, *, raw=None):
    payload = raw if raw is not None else intake._bytes(request)
    result = path.with_name(hashlib.sha256(payload).hexdigest() + '.json')
    path.unlink()
    atomic_write_bytes(result, payload, durable=True, mode=0o600)
    return result


def test_submission_never_opens_http_or_mutates_live_queue(queue, monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
                        lambda *a, **k: pytest.fail('registration cannot construct HTTP'))
    changes = queue.db.total_changes
    assert intake.has_live_owner(queue.root)
    result = submit(queue)
    assert result['admission_pending'] and result['submitted_urls'] == 1
    assert result['http_requests'] == result['queue_writes'] == 0
    assert not result['owner_restarted'] and not result['body_acceptance_proven']
    assert head(queue) is None and queue.db.total_changes == changes
    path = Path(result['pending_path'])
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert intake._load(path)['targets'][0]['url'] == URL
    assert not queue.db.execute("SELECT 1 FROM sqlite_master WHERE name='url_metadata_jobs'").fetchall()


def test_inbox_progress_reports_unexamined_work_and_reuses_validated_counts(queue, monkeypatch):
    submit(queue, (URL, URL + '2'))
    submit(queue, (URL,))  # A second request may repeat an already submitted URL.
    original_load = intake._load
    calls = []
    monkeypatch.setattr(intake, '_load', lambda path: calls.append(path) or original_load(path))
    before = queue.db.total_changes
    first = queue.status(publish=False)['url_metadata_inbox']
    assert first == {'contract_version': 1, 'available': True, 'request_files': 2,
                     'pending_requests': 2, 'targets_unexamined': 3,
                     'completed_unarchived': 0, 'invalid_or_unreadable': 0}
    assert queue.db.total_changes == before and len(calls) == 2
    assert queue.status(publish=False)['url_metadata_inbox'] == first and len(calls) == 2
    assert consume(queue, limit=1)['processed'] == 1
    remaining = queue.status(publish=False)['url_metadata_inbox']
    assert remaining['targets_unexamined'] == 2
    while intake.has_pending(queue):
        consume(queue)
    done = queue.status(publish=False)['url_metadata_inbox']
    assert done['request_files'] == done['targets_unexamined'] == 0


def test_inbox_progress_distinguishes_unreadable_and_completed_unarchived(queue, monkeypatch):
    first = submit(queue)
    path = Path(first['pending_path'])
    assert intake.inbox_progress(queue)['targets_unexamined'] == 1
    path.chmod(0o644)  # File identity changed; cached validation must not survive it.
    broken = intake.inbox_progress(queue)
    assert broken['invalid_or_unreadable'] == 1 and broken['targets_unexamined'] == 0
    path.chmod(0o600)
    monkeypatch.setattr(intake, '_archive', lambda *args: (_ for _ in ()).throw(OSError('full')))
    assert consume(queue)['processed'] == 1
    archived = intake.inbox_progress(queue)
    assert archived['completed_unarchived'] == 1 and archived['pending_requests'] == 0
    assert archived['targets_unexamined'] == 0 and path.exists()


def test_resident_registration_is_durable_and_idempotent(queue):
    result = submit(queue, (URL, URL + '#fragment', URL))
    assert result['submitted_urls'] == 1
    assert intake.has_pending(queue)
    assert consume(queue) == {'processed': 1, 'outcomes': {'metadata_registered': 1}}
    before = head(queue)
    assert before['state'] == 'pending' and not before['body_sha256'] and not before['metadata_date_raw']
    assert before['attempts'] == 0
    base = queue.root/'reviewed_repairs/url_metadata/completed'
    assert hashlib.sha256((base/(result['request_id']+'.request.json')).read_bytes()).hexdigest() == result['request_id']
    receipt = json.loads((base/(result['request_id']+'.receipt.json')).read_bytes())
    assert receipt['outcomes'] == {'metadata_registered': 1}
    assert receipt['http_requests'] == 0 and not receipt['body_acceptance_proven']
    assert not intake.has_pending(queue) and consume(queue)['processed'] == 0
    submit(queue)
    assert consume(queue)['outcomes'] == {'existing_head_preserved': 1}
    assert head(queue) == before


@pytest.mark.parametrize('field,value', [
    ('source_date', 'x' * (intake.MAX_SOURCE_DATE_CHARS + 1)),
    ('source_date', '2026-01-01\x00hidden'),
    ('source_date', None),
    ('provider', 'other'),
    ('allowed', False),
])
def test_cnyes_date_hint_rejects_invalid_scope_or_date(queue, field, value):
    identity = intake._source_identity(queue.registry, URL)
    identity['provider'] = 'cnyes'
    target = {'url': URL, 'source': identity, 'source_date': '2026-01-01'}
    if field in identity:
        identity[field] = value
    else:
        target[field] = value
    submitted = intake._submit(queue.root, {'contract_version': intake.LEGACY_FINLAB_CNYES_HINT_VERSION,
        'kind': intake.FINLAB_CNYES_HINT_KIND, 'created_at_utc': intake.utc_now(), 'targets': [target]})
    with pytest.raises(ValueError, match='invalid_finlab_cnyes_source_date_hint'):
        intake._load(Path(submitted['pending_path']))
    assert consume(queue)['outcomes'] == {'invalid_request': 1}
    assert head(queue) is None


@pytest.mark.parametrize('field,value', [
    ('origin_id', 0), ('origin_id', True), ('sha256', 'x' * 64),
    ('expected_rows', 0), ('expected_rows', True),
    ('file_identity', [1, 2, 3, 4]), ('file_identity', [1, 2, 3, 4, True]),
    ('source_row_index', -1), ('source_row_index', 2), ('source_row_index', True),
])
def test_bound_cnyes_hint_rejects_invalid_origin_and_row(queue, field, value):
    identity = intake._source_identity(queue.registry, URL)
    identity['provider'] = 'cnyes'
    origin = {'origin_id': 1, 'sha256': 'a' * 64, 'expected_rows': 2, 'file_identity': [1, 2, 3, 4, 5]}
    target = {'url': URL, 'source': identity, 'source_date': '2026-01-01', 'source_row_index': 1}
    (target if field == 'source_row_index' else origin)[field] = value
    submitted = intake._submit(queue.root, {'contract_version': intake.FINLAB_CNYES_HINT_VERSION,
        'kind': intake.FINLAB_CNYES_HINT_KIND, 'created_at_utc': intake.utc_now(),
        'source_origin': origin, 'priority_evidence': {'baseline_origin_id': 2, 'baseline_sha256': 'b' * 64,
            'raw_unique_delta': 2, 'raw_hints_examined': 2}, 'targets': [target]})
    with pytest.raises(ValueError, match='invalid_finlab_cnyes_source_(origin|row)'):
        intake._load(Path(submitted['pending_path']))
    assert consume(queue)['outcomes'] == {'invalid_request': 1}
    assert head(queue) is None


def test_bound_cnyes_hint_rejects_duplicate_source_rows(queue):
    identity = intake._source_identity(queue.registry, URL)
    identity['provider'] = 'cnyes'
    targets = [{'url': URL, 'source': identity, 'source_date': '', 'source_row_index': 0},
               {'url': URL + '/second', 'source': identity, 'source_date': '', 'source_row_index': 0}]
    submitted = intake._submit(queue.root, {'contract_version': intake.FINLAB_CNYES_HINT_VERSION,
        'kind': intake.FINLAB_CNYES_HINT_KIND, 'created_at_utc': intake.utc_now(),
        'source_origin': {'origin_id': 1, 'sha256': 'a' * 64, 'expected_rows': 2,
                          'file_identity': [1, 2, 3, 4, 5]},
        'priority_evidence': {'baseline_origin_id': 2, 'baseline_sha256': 'b' * 64,
                              'raw_unique_delta': 2, 'raw_hints_examined': 2}, 'targets': targets})
    with pytest.raises(ValueError, match='invalid_finlab_cnyes_source_row'):
        intake._load(Path(submitted['pending_path']))


@pytest.mark.parametrize('field,value', [
    ('baseline_origin_id', 0), ('baseline_origin_id', 1), ('baseline_origin_id', True),
    ('baseline_sha256', 'invalid'), ('raw_unique_delta', 0), ('raw_unique_delta', True),
    ('raw_hints_examined', 0), ('raw_hints_examined', True),
])
def test_bound_cnyes_hint_rejects_invalid_priority_evidence(queue, field, value):
    identity = intake._source_identity(queue.registry, URL)
    identity['provider'] = 'cnyes'
    evidence = {'baseline_origin_id': 2, 'baseline_sha256': 'b' * 64,
                'raw_unique_delta': 1, 'raw_hints_examined': 1}
    evidence[field] = value
    submitted = intake._submit(queue.root, {'contract_version': intake.FINLAB_CNYES_HINT_VERSION,
        'kind': intake.FINLAB_CNYES_HINT_KIND, 'created_at_utc': intake.utc_now(),
        'source_origin': {'origin_id': 1, 'sha256': 'a' * 64, 'expected_rows': 2,
                          'file_identity': [1, 2, 3, 4, 5]}, 'priority_evidence': evidence,
        'targets': [{'url': URL, 'source': identity, 'source_date': '', 'source_row_index': 0}]})
    with pytest.raises(ValueError, match='invalid_finlab_cnyes_priority_evidence'):
        intake._load(Path(submitted['pending_path']))


@pytest.mark.parametrize('state', ['stored', 'retry', 'http_429', 'waiting_cohort', 'robots_disallowed'])
def test_every_existing_head_field_and_deadline_is_preserved(queue, state):
    with queue.db:
        queue.add_articles([{'url': URL, 'title': 'Retained title', 'date': '2001-02-03'}])
        queue.db.execute('UPDATE articles SET state=?,attempts=7,due=?,body_sha256=?,body_bytes=13,'
                         'parquet_path=?,lease_owner=?,lease_until=? WHERE url=?',
                         (state, time.time()+50000, 'a'*64, 'original.parquet', 'owner', time.time()+600, URL))
    before = head(queue)
    submit(queue)
    assert consume(queue)['outcomes'] == {'existing_head_preserved': 1}
    assert head(queue) == before


@pytest.mark.parametrize('policy, url, state', [
    ({}, 'https://unknown.test/story/1', 'awaiting_authorization'),
    ({'enabled': False}, URL, 'awaiting_authorization'),
    ({'article_path_pattern': '^/allowed/'}, URL, 'unsupported_article_path'),
    ({'acquisition_mode': 'finite'}, URL, 'waiting_cohort'),
])
def test_new_heads_use_canonical_source_and_finite_scope(config, tmp_path, policy, url, state):
    config['providers'][0].update(policy)
    owner = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        if state == 'waiting_cohort':
            with owner.db:
                owner.add_articles([{'url': 'https://publisher.test/already-known'}])
                owner.sync_finite_cohorts()
        submit(owner, (url,))
        assert consume(owner)['outcomes'] == {'metadata_registered': 1}
        assert head(owner, url)['state'] == state
        if state == 'waiting_cohort':
            assert not owner.db.execute('SELECT 1 FROM finite_cohort_members WHERE url=?', (url,)).fetchall()
    finally:
        owner.close()


def test_source_contract_drift_does_not_register_or_fetch(queue, config):
    submit(queue)
    config['providers'][0]['enabled'] = False
    queue.registry = PolicyRegistry(config, base=queue.root)
    assert consume(queue)['outcomes'] == {'source_contract_changed': 1}
    assert head(queue) is None


def test_failed_registration_rolls_back_head_cursor_and_receipt(queue):
    result = submit(queue)
    intake._schema(queue)
    queue.db.execute("CREATE TRIGGER reject_intake BEFORE INSERT ON url_metadata_items "
                     "BEGIN SELECT raise(ABORT,'synthetic receipt failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='synthetic receipt failure'):
        consume(queue)
    assert head(queue) is None
    assert not queue.db.execute('SELECT 1 FROM url_metadata_jobs').fetchall()
    assert Path(result['pending_path']).exists()
    queue.db.execute('DROP TRIGGER reject_intake')
    assert consume(queue)['outcomes'] == {'metadata_registered': 1}


def test_cursor_survives_owner_reopen_without_duplicate_registration(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    owner = NewsQueue(tmp_path, registry)
    urls = [f'https://publisher.test/story/{i}' for i in range(4)]
    result = submit(owner, urls)
    assert consume(owner, limit=1)['processed'] == 1
    first = head(owner, urls[0])
    assert intake.has_pending(owner)
    owner.close()
    assert not intake.has_live_owner(tmp_path)
    owner = NewsQueue(tmp_path, registry)
    try:
        assert consume(owner)['processed'] == 3
        assert head(owner, urls[0]) == first
        assert owner.db.execute('SELECT count(*) FROM url_metadata_items').fetchone()[0] == 4
        assert owner.db.execute('SELECT completed_at_utc FROM url_metadata_jobs WHERE request_id=?',
                                (result['request_id'],)).fetchone()[0]
        assert not intake.has_pending(owner)
    finally:
        owner.close()


def test_archive_io_failure_is_recovered_without_repeating_admission(queue, monkeypatch):
    result = submit(queue)
    archive = intake._archive
    def fail(*args):
        raise OSError('synthetic full archive device')
    monkeypatch.setattr(intake, '_archive', fail)
    assert consume(queue)['outcomes'] == {'metadata_registered': 1}
    before = head(queue)
    assert Path(result['pending_path']).exists()
    assert not intake.has_pending(queue)  # Completed archive retries must not busy-loop.
    monkeypatch.setattr(intake, '_archive', archive)
    assert consume(queue)['processed'] == 0
    assert not Path(result['pending_path']).exists() and head(queue) == before


@pytest.mark.parametrize('damage', ['digest', 'public', 'symlink', 'future', 'naive', 'duplicate_keys', 'extra_field'])
def test_invalid_input_is_retained_and_cannot_block_the_next_valid_request(queue, damage):
    result = submit(queue)
    path = Path(result['pending_path'])
    request = json.loads(path.read_bytes())
    if damage == 'digest':
        path.write_bytes(b'{}')
    elif damage == 'public':
        path.chmod(0o644)
    elif damage == 'symlink':
        external = queue.root/'outside-request'
        path.replace(external)
        path.symlink_to(external)
    elif damage == 'duplicate_keys':
        path = rewrite(path, raw=b'{"kind":1,"kind":2}')
    else:
        if damage == 'future':
            request['created_at_utc'] = (datetime.now(UTC)+timedelta(days=1)).isoformat()
        elif damage == 'naive':
            request['created_at_utc'] = '2026-01-01T00:00:00'
        else:
            request['body'] = 'Cannot inject article text'
        path = rewrite(path, request)
    submit(queue, ('https://publisher.test/story/2',))
    outcomes = Counter()
    # Durable quarantine can consume a slow I/O slice; a later owner turn
    # must process the valid request without weakening the soft deadline.
    for _ in range(8):
        outcomes.update(consume(queue)['outcomes'])
        if outcomes == {'invalid_request': 1, 'metadata_registered': 1}:
            break
    assert outcomes == {'invalid_request': 1, 'metadata_registered': 1}
    assert head(queue) is None and head(queue, 'https://publisher.test/story/2')['state'] == 'pending'
    assert not path.exists() and list((queue.root/'reviewed_repairs/rejected').glob('*.input'))


def test_archive_symlink_never_follows_or_overwrites_external_file(queue):
    result = submit(queue)
    completed = intake._directory(queue.root, 'url_metadata', 'completed')
    external = queue.root/'outside-archive'
    external.write_bytes(b'preserve')
    archived = completed/(result['request_id']+'.request.json')
    archived.symlink_to(external)
    assert consume(queue)['outcomes'] == {'metadata_registered': 1}
    before = head(queue)
    assert external.read_bytes() == b'preserve' and archived.is_symlink()
    assert not intake.has_pending(queue)
    archived.unlink()
    assert consume(queue)['processed'] == 0 and head(queue) == before
    assert not Path(result['pending_path']).exists()


def test_stop_and_bounded_batches_keep_unprocessed_requests(queue):
    urls = [f'https://publisher.test/story/{i}' for i in range(8)]
    submit(queue, urls)
    stop = threading.Event(); stop.set()
    assert consume(queue, stop=stop)['processed'] == 0
    assert not queue.db.execute('SELECT 1 FROM articles').fetchall()
    stop.clear()
    assert consume(queue, limit=2, stop=stop)['processed'] == 2
    assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 2
    assert intake.has_pending(queue)
    with pytest.raises(ValueError, match='slice'):
        consume(queue, limit=0)


def test_large_or_empty_submissions_are_rejected_without_partial_request(queue, monkeypatch):
    with pytest.raises(ValueError, match='limit_512'):
        submit(queue, (f'https://publisher.test/{i}' for i in range(513)))
    with pytest.raises(ValueError, match='requires_urls'):
        submit(queue, ())
    monkeypatch.setattr(intake, 'MAX_REQUEST_BYTES', 1)
    with pytest.raises(ValueError, match='too_large'):
        submit(queue)
    assert not (queue.root/'reviewed_repairs/url_metadata').exists()
    assert not queue.db.execute('SELECT 1 FROM articles').fetchall()


def test_full_inbox_has_explicit_backpressure(queue):
    directory = intake._directory(queue.root, 'url_metadata', 'pending')
    for i in range(intake.MAX_PENDING):
        (directory/f'{i:064x}.json').write_bytes(b'{}')
    with pytest.raises(ValueError, match='inbox_full'):
        submit(queue)
    assert len(list(directory.iterdir())) == intake.MAX_PENDING


def test_metadata_wakes_existing_owner_without_threads_and_respects_disk_wait(queue):
    stop = OwnerStopEvent()
    with OwnerMailboxWake(queue, stop) as wake:
        if wake.backend != 'linux_inotify':
            pytest.skip('inotify unavailable')
        threads = threading.active_count()
        submit(queue)
        assert wake.has_pending()
        assert not wake.wait(.02, enabled=False)
        assert wake.wait(1) and threading.active_count() == threads
        assert consume(queue)['processed'] == 1
        assert not wake.has_pending() and not wake.wait(.02)


def test_cli_enqueue_hands_off_before_lifecycle_queue_or_http(queue, config, tmp_path, monkeypatch, capsys):
    from scripts import download_news_fulltext as cli
    config_path = tmp_path/'config.json'
    config_path.write_text(json.dumps(config))
    owner = queue.owner
    before = queue.db.total_changes
    monkeypatch.setattr(cli, 'NewsQueue', lambda *a, **k: pytest.fail('second queue owner'))
    monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **k: pytest.fail('second HTTP collector'))
    monkeypatch.setattr('stockagent.data.news_worker_lifecycle.NewsWorkerLifecycle',
                        lambda *a, **k: pytest.fail('registration overwrote lifecycle'))
    assert cli.main(['enqueue', '--config', str(config_path), '--root', str(tmp_path), '--url', URL]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['admission_pending'] and result['http_requests'] == 0
    assert queue.db.total_changes == before and queue.owner == owner and head(queue) is None
