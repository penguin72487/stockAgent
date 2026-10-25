"""Resident URL registration keeps one writer and ordinary source admission."""
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time

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
    outcome = consume(queue)
    assert outcome['outcomes'] == {'invalid_request': 1, 'metadata_registered': 1}
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
