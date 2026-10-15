"""An observer never fetches, rewrites a due clock or replaces its producer."""
from datetime import datetime
import json
from types import SimpleNamespace

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
import scripts.watch_news_formal_articles as watcher
from test_news_fulltext import config as config


URL = 'https://publisher.test/a'
SHA = 'a' * 64
STAMP = '2026-10-11T20:00:00+00:00'
STARTED = datetime.fromisoformat(STAMP).timestamp()


def row():
    return {'url': URL, 'state': 'complete', 'body_sha256': SHA, 'lease_owner': None,
            'captured_at_utc': STAMP, 'last_checked_at_utc': STAMP}


@pytest.mark.parametrize('value', [None, {}, [], {URL: 'short'}, {URL: 'A' * 64},
    {URL: True}, {URL + '#fragment': SHA}, {True: SHA},
    {f'https://publisher.test/{i}': SHA for i in range(17)}])
def test_expected_heads_are_bounded_exact_url_and_sha(value):
    with pytest.raises(ValueError):
        watcher.expected_heads(value)


def test_query_equals_is_not_a_url_sha_separator():
    url = URL + '?exp=a'
    assert watcher.expected_heads({url: SHA}) == {url: SHA}


@pytest.mark.parametrize('mutation', ['pending', 'wrong_sha', 'leased', 'missing',
    'old_capture', 'old_check', 'naive_capture', 'invalid_check', 'missing_url'])
def test_only_this_owner_complete_original_is_ready(mutation):
    value = row()
    rows = {URL: value}
    if mutation == 'pending':
        value['state'] = 'retry_cooldown'
    elif mutation == 'wrong_sha':
        value['body_sha256'] = 'b' * 64
    elif mutation == 'leased':
        value['lease_owner'] = 'other'
    elif mutation == 'missing':
        rows[URL] = None
    elif mutation in {'old_capture', 'old_check'}:
        value['captured_at_utc' if mutation == 'old_capture' else 'last_checked_at_utc'] = '2020-01-01T00:00:00Z'
    elif mutation == 'naive_capture':
        value['captured_at_utc'] = '2026-10-11T20:00:00'
    elif mutation == 'invalid_check':
        value['last_checked_at_utc'] = None
    else:
        rows = {}
    assert not watcher.heads_ready(rows, {URL: SHA}, STARTED)


def test_indexed_reads_do_not_take_writer_lock_or_change_clock(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': URL}])
        changes = queue.db.total_changes
        before = queue.db.execute('SELECT state,due,attempts FROM articles').fetchone()
        fingerprint = registry.require(URL).data_fingerprint
        with queue.db:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings(key,value) VALUES (?,?)',
                             ('source_capture_fingerprint:publisher', fingerprint))
        changes = queue.db.total_changes
        result = watcher.read_heads(tmp_path, {URL: SHA}, queue.owner, {'publisher': fingerprint})
        assert result[URL]['state'] == 'pending'
        assert queue.db.total_changes == changes
        assert tuple(queue.db.execute('SELECT state,due,attempts FROM articles').fetchone()) == tuple(before)
        with pytest.raises(ValueError, match='owner_changed'):
            watcher.read_heads(tmp_path, {URL: SHA}, 'different', {'publisher': fingerprint})
        with pytest.raises(ValueError, match='capture_contract_changed'):
            watcher.read_heads(tmp_path, {URL: SHA}, queue.owner, {'publisher': 'wrong'})
    finally:
        queue.close()


def baseline(files):
    return {'owner_id': 'owner', 'status_owner_id': 'owner', 'service': {'MainPID': '1'},
        **dict.fromkeys(('status_database_owner_consistent', 'loaded_provider_policy_matches_config',
                        'source_files_unchanged_since_owner_start', 'metrics_snapshot_clock_verified'), True),
        'owner_process_clock_check': {'accepted': True, 'owner_metric_started_at_epoch': STARTED},
        'source_code_sha256': {'source.py': sha256_file(files / 'source.py')},
        'configuration_files_sha256': {'config.json': sha256_file(files / 'config.json')}}


@pytest.mark.parametrize('changed', ['owner', 'status_database_owner_consistent',
    'loaded_provider_policy_matches_config', 'source_files_unchanged_since_owner_start',
    'metrics_snapshot_clock_verified', 'process_clock', 'missing_hash', 'changed_file'])
def test_baseline_pins_verified_owner_and_exact_files(tmp_path, changed):
    (tmp_path / 'source.py').write_text('source fixture')
    (tmp_path / 'config.json').write_text('{}')
    proof = baseline(tmp_path)
    if changed == 'owner':
        proof['status_owner_id'] = 'other'
    elif changed == 'process_clock':
        proof['owner_process_clock_check']['accepted'] = False
    elif changed == 'missing_hash':
        proof['source_code_sha256'] = {}
    elif changed == 'changed_file':
        (tmp_path / 'source.py').write_text('changed fixture')
    else:
        proof[changed] = False
    with pytest.raises(ValueError):
        watcher.pinned_files(tmp_path, proof)


def test_valid_baseline_also_pins_current_shared_acceptance_code(tmp_path):
    (tmp_path / 'source.py').write_text('source fixture')
    (tmp_path / 'config.json').write_text('{}')
    files = watcher.pinned_files(tmp_path, baseline(tmp_path))
    assert len(files) == 4
    assert all(sha256_file(path) == digest for path, digest in files.items())


@pytest.fixture
def fake_watch(tmp_path, monkeypatch):
    source = tmp_path / 'source.py'
    source.write_text('source fixture')
    (tmp_path / 'config.json').write_text('{}')
    proof = baseline(tmp_path)
    clock = {'now': 0.0, 'sleeps': []}
    monkeypatch.setattr(watcher, 'pinned_files', lambda repo, base: {source: sha256_file(source)})
    monkeypatch.setattr(watcher, 'verify_owner', lambda root, base: {'accepted': True})
    monkeypatch.setattr(watcher.time, 'monotonic', lambda: clock['now'])
    def sleep(seconds):
        clock['sleeps'].append(seconds)
        clock['now'] += seconds
    monkeypatch.setattr(watcher.time, 'sleep', sleep)
    registry = SimpleNamespace(require=lambda url: SimpleNamespace(provider='publisher', data_fingerprint='capture'))
    return tmp_path, proof, clock, registry, source


def run_watch(fake_watch):
    root, proof, clock, registry, _ = fake_watch
    return watcher.watch(root, registry, root, proof, {URL: SHA}, root / 'final.json',
                         maximum_seconds=60, poll_seconds=20)


def test_timeout_is_observer_deadline_not_producer_failure(fake_watch, monkeypatch):
    import sqlite3
    calls = []
    def transient(*args):
        calls.append(args)
        raise sqlite3.OperationalError('read side locked')
    monkeypatch.setattr(watcher, 'read_heads', transient)
    monkeypatch.setattr(watcher, 'audit', lambda *a, **k: pytest.fail('no complete head to audit'))
    result = run_watch(fake_watch)
    assert result['state'] == 'observation_deadline_reached'
    assert result['accepted'] is False and result['producer_failed'] is False
    assert result['publisher_requests'] == 0 and result['queue_writes'] is False
    assert result['all_history_complete'] is False
    assert len(calls) == 4 and fake_watch[2]['sleeps'] == [20, 20, 20]
    assert json.loads((fake_watch[0] / 'final.pending.json').read_bytes()) == result


@pytest.mark.parametrize('matched_contract', [False, True])
def test_ready_head_uses_canonical_source_and_durable_batch_verifier(fake_watch, monkeypatch, matched_contract):
    monkeypatch.setattr(watcher, 'read_heads', lambda *a: {URL: row()})
    calls = []
    def verify(root, urls, *, registry):
        calls.append((root, urls, registry))
        return {'accepted': True, 'articles': [{'url': URL, 'body_sha256': SHA,
                'capture_contract_matches_current_source': matched_contract}]}
    monkeypatch.setattr(watcher, 'audit', verify)
    result = run_watch(fake_watch)
    assert result['accepted'] is matched_contract
    assert len(calls) == 1 and calls[0][1] == [URL]
    assert fake_watch[2]['sleeps'] == []
    assert result['publisher_requests'] == 0 and result['queue_writes'] is False
    assert not result['all_history_complete']
    with pytest.raises(ValueError, match='already_exists'):
        run_watch(fake_watch)


def test_code_change_stops_observer_without_acceptance(fake_watch, monkeypatch):
    monkeypatch.setattr(watcher, 'read_heads', lambda *a: {URL: None})
    def change(seconds):
        fake_watch[4].write_text('changed source fixture')
    monkeypatch.setattr(watcher.time, 'sleep', change)
    with pytest.raises(ValueError, match='source_configuration_or_verifier_changed'):
        run_watch(fake_watch)
    assert not (fake_watch[0] / 'final.json').exists()


def test_change_during_final_verification_is_rejected(fake_watch, monkeypatch):
    monkeypatch.setattr(watcher, 'read_heads', lambda *a: {URL: row()})
    def change(*a, **k):
        fake_watch[4].write_text('changed during acceptance')
        return {'accepted': True, 'articles': [{'url': URL, 'body_sha256': SHA,
                'capture_contract_matches_current_source': True}]}
    monkeypatch.setattr(watcher, 'audit', change)
    with pytest.raises(ValueError, match='bytes_changed_during_acceptance'):
        run_watch(fake_watch)
    assert not (fake_watch[0] / 'final.json').exists()


@pytest.mark.parametrize('maximum,poll', [(59, 20), (7201, 20), (60, 0), (60, 61)])
def test_unbounded_observation_is_rejected(fake_watch, maximum, poll):
    root, proof, _, registry, _ = fake_watch
    with pytest.raises(ValueError, match='bounded_observation'):
        watcher.watch(root, registry, root, proof, {URL: SHA}, root / 'final.json',
                      maximum_seconds=maximum, poll_seconds=poll)
