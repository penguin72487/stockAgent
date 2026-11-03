"""Finite original-response reuse shares durable acceptance, never HTTP."""
from contextlib import closing
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_failure_focus import configure_focus
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_replay import replay_retained_cnyes_failures
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL, short_page
from test_news_failure_diagnostics import fail, reports
from test_news_failure_focus import spec
from test_news_fulltext import config as publisher_config  # noqa: F401


def prepare(queue, monkeypatch, *, payload=None, advance=True):
    configure_focus(queue, spec())
    fail(queue, Response(URL, 200, {'content-type': 'text/html'}, payload or short_page()))
    original = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    ledger = queue.failure_diagnostic_focus.progress()
    if advance:
        original_time = time.time
        monkeypatch.setattr(time, 'time', lambda: original_time() + 4000)
    return original, ledger


def test_due_original_is_materialized_once_without_http_or_new_diagnostic_budget(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        original, ledger = prepare(queue, monkeypatch)
        paths, evidence = reports(tmp_path)
        immutable = {str(path): sha256_file(path) for path in (*paths, *(p.with_suffix('.receipt.json') for p in paths))}
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == result['responses_checked'] == 1 and result['http_requests'] == 0
        head = queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()
        assert head['state'] == 'complete' and head['captured_at_utc'] == evidence[0]['captured_at_utc']
        assert head['last_checked_at_utc'] == original['last_checked_at_utc']
        record = pq.read_table(tmp_path / head['parquet_path']).to_pylist()[0]
        assert record['acquisition_transport'] == 'retained_raw_reparse'
        assert record['source_observed_at_utc'] == evidence[0]['captured_at_utc']
        assert record['http_duration_seconds'] is None and not record['historical_point_in_time']
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert all(sha256_file(path) == digest for path, digest in immutable.items())
        assert replay_retained_cnyes_failures(queue)['accepted_heads'] == 0
        status = queue.status()
        assert status['provider_stored_body_counter_contract_version'] == 1
        assert status['providers']['cnyes']['stored_body_urls'] == status['articles_with_stored_body'] == 1
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error',due=? WHERE url=?", (time.time() + 4000, URL))
        assert queue.status()['providers']['cnyes']['stored_body_urls'] == 1
        assert queue.status()['providers']['cnyes']['states'] == {'retry': 1}
        assert verify_bodies(tmp_path)['accepted']
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        configure_focus(restarted, spec())
        assert replay_retained_cnyes_failures(restarted)['accepted_heads'] == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1


def test_future_due_is_not_reset_or_checkpointed(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        original, ledger = prepare(queue, monkeypatch, advance=False)
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert queue.db.execute('SELECT count(*) FROM retained_failure_replays').fetchone()[0] == 0


def test_superseded_failure_replay_is_not_counted_as_an_accepted_head(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        original = queue._accept_records
        def newer_claim(records, relative, receipt, **kwargs):
            with queue.db:
                queue.db.execute("UPDATE articles SET lease_token='new-token' WHERE url=?", (URL,))
            return original(records, relative, receipt, **kwargs)
        monkeypatch.setattr(queue, '_accept_records', newer_claim)
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 0 and result['superseded_heads'] == 1
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()[0] is None
        assert not queue.db.execute('SELECT 1 FROM retained_failure_replays').fetchone()
        monkeypatch.setattr(queue, '_accept_records', original)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,"
                             "lease_token=NULL WHERE url=?", (URL,))
        recovered = replay_retained_cnyes_failures(queue)
        assert recovered['accepted_heads'] == 1 and recovered['http_requests'] == 0
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()[0]


@pytest.mark.parametrize('state', ['inflight', 'source_not_found', 'paywall_or_login', 'article_identity_not_verified'])
def test_newer_lease_or_terminal_source_outcome_is_never_overwritten(cnyes_config, tmp_path, monkeypatch, state):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, URL))
        original = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert replay_retained_cnyes_failures(queue)['accepted_heads'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original


def test_newer_http_check_blocks_stale_original_even_after_due(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        original = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 0 and result['responses_checked'] == 1
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        stored = queue.db.execute('SELECT state,error_code FROM retained_failure_replays').fetchone()
        assert tuple(stored) == ('failed_validation', 'retained_failure_head_clock_outside_window')


def test_validation_failure_is_not_checkpointed_if_head_changes_during_read(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        original, ledger = prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        read = queue.failure_diagnostic_focus.read_retained_response

        def correct_head_during_read(job):
            result = read(job)
            with queue.db:
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?',
                                 (original['last_checked_at_utc'], URL))
            return result

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', correct_head_during_read)
        first = replay_retained_cnyes_failures(queue)
        assert first['accepted_heads'] == 0 and first['http_requests'] == 0
        assert not queue.db.execute('SELECT 1 FROM retained_failure_replays').fetchone()
        assert first['local_commit_deferrals'] == 1
        assert 0 < replay.retained_failure_next_wake_delay(queue, 60) <= 1
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', read)
        now = time.time
        monkeypatch.setattr(time, 'time', lambda: now() + 2)
        recovered = replay_retained_cnyes_failures(queue)
        assert recovered['accepted_heads'] == 1 and recovered['http_requests'] == 0
        assert queue.failure_diagnostic_focus.progress() == ledger


def test_failed_checkpoint_is_revisited_when_head_is_corrected(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        original, ledger = prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        first = replay_retained_cnyes_failures(queue)
        assert first['accepted_heads'] == 0 and first['responses_checked'] == 1
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        with queue.db:
            queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?',
                             (original['last_checked_at_utc'], URL))
        assert replay.retained_failure_next_wake_delay(queue, 60) == 0
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        configure_focus(restarted, spec())
        recovered = replay_retained_cnyes_failures(restarted)
        assert recovered['accepted_heads'] == 1 and recovered['http_requests'] == 0
        assert restarted.failure_diagnostic_focus.progress() == ledger


@pytest.mark.parametrize('new_condition', ['future_due', 'terminal', 'body'])
def test_changed_failure_head_preserves_other_workers_state(cnyes_config, tmp_path, monkeypatch, new_condition):
    from downloader import news_fulltext_replay as replay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        read = queue.failure_diagnostic_focus.read_retained_response
        newer = []

        def update_during_read(job):
            result = read(job)
            with queue.db:
                if new_condition == 'future_due':
                    queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + 600, URL))
                elif new_condition == 'terminal':
                    queue.db.execute("UPDATE articles SET state='source_not_found' WHERE url=?", (URL,))
                else:
                    queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url=?", (b'x' * 32, URL))
            newer.append(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()))
            return result

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', update_during_read)
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 0 and result['superseded_heads'] == 1
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == newer[0]
        assert not queue.db.execute('SELECT 1 FROM retained_failure_replays').fetchone()
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()
        assert replay.retained_failure_next_wake_delay(queue, 60) == 60


def test_legacy_failure_checkpoint_without_witness_is_rechecked(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        _, ledger = prepare(queue, monkeypatch)
        source = queue.registry.sources['cnyes']
        receipt = queue.db.execute('SELECT receipt_path FROM failure_evidence_focus_attempts').fetchone()[0]
        with queue.db:
            queue.db.execute('''CREATE TABLE retained_failure_replays(
                receipt_path TEXT NOT NULL,parser_fingerprint TEXT NOT NULL,state TEXT NOT NULL,
                error_code TEXT,checked_at_utc TEXT NOT NULL,
                PRIMARY KEY(receipt_path,parser_fingerprint)) WITHOUT ROWID''')
            queue.db.execute('INSERT INTO retained_failure_replays VALUES (?,?,?,?,?)',
                             (receipt, source.parser_repair_fingerprint, 'failed_validation',
                              'retained_failure_head_clock_outside_window', '2000-01-01T00:00:00+00:00'))
        replay._ensure_retained_failure_retry_schema(queue)
        old_columns = [r[1] for r in queue.db.execute('PRAGMA table_info(retained_failure_replays)')]
        queue.db.execute('PRAGMA query_only=ON')
        try:
            assert replay.retained_failure_next_wake_delay(queue, 60) == 0
        finally:
            queue.db.execute('PRAGMA query_only=OFF')
        assert [r[1] for r in queue.db.execute('PRAGMA table_info(retained_failure_replays)')] == old_columns
        first = replay_retained_cnyes_failures(queue)
        assert first['accepted_heads'] == 1 and first['http_requests'] == 0
        checkpoint = queue.db.execute('SELECT state,head_witness_json FROM retained_failure_replays').fetchone()
        assert tuple(checkpoint) == ('accepted', None)
        assert queue.failure_diagnostic_focus.progress() == ledger


def test_changed_validation_head_does_not_block_another_original(cnyes_config, tmp_path, monkeypatch):
    second_url = URL.replace('/42', '/43')
    second_page = short_page(canonical=second_url, change={'newsId': 43, 'url': second_url},
                             schema_change={'url': second_url, 'mainEntityOfPage': {'@id': second_url}})
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec(urls=[URL, second_url]))
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, short_page()))
        fail(queue, Response(second_url, 200, {'content-type': 'text/html'}, second_page))
        now = [time.time() + 4000]
        monkeypatch.setattr(time, 'time', lambda: now[0])
        selected = queue.db.execute('''SELECT f.url_sha256,a.last_checked_at_utc
            FROM failure_evidence_focus_attempts f JOIN articles a ON a.id=f.url_sha256
            WHERE f.outcome='raw_saved' ORDER BY f.attempt_number DESC LIMIT 1''').fetchone()
        with queue.db:
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2099-01-01T00:00:00+00:00' WHERE id=?",
                             (selected[0],))
        read = queue.failure_diagnostic_focus.read_retained_response

        def correct_during_read(job):
            result = read(job)
            if job[0] == selected[0]:
                with queue.db:
                    queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE id=?', (selected[1], job[0]))
            return result

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', correct_during_read)
        first = replay_retained_cnyes_failures(queue, maximum_responses=1, maximum_wall_seconds=2)
        assert first['local_commit_deferrals'] == 1 and first['accepted_heads'] == 0
        second = replay_retained_cnyes_failures(queue, maximum_responses=1, maximum_wall_seconds=2)
        assert second['accepted_heads'] == 1 and second['http_requests'] == 0
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE id=?', (selected[0],)).fetchone()[0] is None
        now[0] += 2
        third = replay_retained_cnyes_failures(queue, maximum_responses=1, maximum_wall_seconds=2)
        assert third['accepted_heads'] == 1 and third['http_requests'] == 0
        assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize(('state', 'error_code', 'checked_at'), [
    ('source_not_found', 'source_not_found', None),
    ('retry', 'newer_transport_error', '2099-01-01T00:00:00+00:00'),
])
def test_changed_head_during_local_validation_is_not_claimed(
        cnyes_config, tmp_path, monkeypatch, state, error_code, checked_at):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        _, ledger = prepare(queue, monkeypatch)
        read = queue.failure_diagnostic_focus.read_retained_response
        newer = []

        def read_then_change_head(job):
            result = read(job)
            with queue.db:
                queue.db.execute('UPDATE articles SET state=?,error_code=?,'
                                 'last_checked_at_utc=coalesce(?,last_checked_at_utc) WHERE url=?',
                                 (state, error_code, checked_at, URL))
            newer.append(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()))
            return result

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', read_then_change_head)
        result = replay_retained_cnyes_failures(queue)
        assert result['responses_checked'] == 1 and result['accepted_heads'] == 0
        assert result['http_requests'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == newer[0]
        assert queue.db.execute('SELECT count(*) FROM retained_failure_replays').fetchone()[0] == 0
        retry = queue.db.execute('SELECT last_error FROM retained_failure_local_retries').fetchone()
        if state == 'retry':
            assert retry[0] == 'retained_failure_claim_changed_head'
            now = time.time()
            monkeypatch.setattr(time, 'time', lambda: now + 2)
            assert replay_retained_cnyes_failures(queue)['accepted_heads'] == 0
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == newer[0]
            checked = queue.db.execute('SELECT state,error_code FROM retained_failure_replays').fetchone()
            assert tuple(checked) == ('failed_validation', 'retained_failure_head_clock_outside_window')
        else:
            assert retry is None
        assert queue.failure_diagnostic_focus.progress() == ledger


@pytest.mark.parametrize('new_condition', ['future_due', 'leased'])
def test_changed_head_already_waiting_is_not_given_local_retry(cnyes_config, tmp_path, monkeypatch, new_condition):
    from downloader import news_fulltext_replay as replay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        _, ledger = prepare(queue, monkeypatch)
        read = queue.failure_diagnostic_focus.read_retained_response
        newer = []

        def change_during_read(job):
            result = read(job)
            with queue.db:
                if new_condition == 'future_due':
                    queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + 600, URL))
                else:
                    queue.db.execute("UPDATE articles SET state='inflight',lease_owner='other',lease_until=? WHERE url=?",
                                     (time.time() + 600, URL))
            newer.append(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()))
            return result

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', change_during_read)
        assert replay_retained_cnyes_failures(queue)['accepted_heads'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == newer[0]
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()
        assert replay.retained_failure_next_wake_delay(queue, 60) == 60
        assert queue.failure_diagnostic_focus.progress() == ledger


def test_unchanged_body_not_found_head_can_be_replayed(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found' WHERE url=?", (URL,))
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 1 and result['http_requests'] == 0
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'


def test_body_not_found_claim_requires_explicit_failure_replay_fence(cnyes_config, tmp_path, monkeypatch):
    from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS

    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found' WHERE url=?", (URL,))
        head = queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()
        expected = {key: head[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS}
        with pytest.raises(ValueError):
            queue.claim_url(URL, 600, expected_missing_head=expected)
        with pytest.raises(ValueError):
            queue.claim_url(URL, 600, retained_failure_replay=True)
        with pytest.raises(ValueError):
            queue.claim_url(URL, 600, expected_missing_head=expected, retained_failure_replay='true')
        expected['state'] = 'source_not_found'
        with pytest.raises(ValueError):
            queue.claim_url(URL, 600, expected_missing_head=expected, retained_failure_replay=True)
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == dict(head)


def test_unresolved_local_parser_does_not_add_retry_attempt_or_reset_error(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        original, _ = prepare(queue, monkeypatch, payload=short_page(visible='source text does not match schema'))
        result = replay_retained_cnyes_failures(queue)
        assert result['responses_checked'] == 1 and result['accepted_heads'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        assert queue.db.execute('SELECT state FROM retained_failure_replays').fetchone()[0] == 'parse_unresolved'


def test_corrupt_pair_keeps_original_proof_and_failure_head(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        original, _ = prepare(queue, monkeypatch)
        path = reports(tmp_path)[0][0].with_suffix('.receipt.json')
        value = json.loads(path.read_bytes())
        value['sha256'] = '0' * 64
        path.write_text(json.dumps(value))  # Synthetic corruption injection only.
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 0 and result['responses_checked'] == 1
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        stored = queue.db.execute('SELECT state,error_code FROM retained_failure_replays').fetchone()
        assert tuple(stored) == ('failed_validation', 'retained_failure_pair_unverified')


def test_local_read_failure_defers_and_recovers_after_restart_without_new_http(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        original, ledger = prepare(queue, monkeypatch)
        paths, _ = reports(tmp_path)
        immutable = {str(path): sha256_file(path) for path in (*paths, *(p.with_suffix('.receipt.json') for p in paths))}
        def unavailable(_job):
            raise OSError('synthetic local storage outage; not a source response')
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', unavailable)
        first = replay_retained_cnyes_failures(queue)
        assert first['local_io_deferrals'] == 1 and first['accepted_heads'] == 0 and first['http_requests'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.db.execute('SELECT count(*) FROM retained_failure_replays').fetchone()[0] == 0
        assert 0 < replay.retained_failure_next_wake_delay(queue, 60) <= 1
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
    finally:
        queue.close()
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        configure_focus(restarted, spec())
        assert 0 < replay.retained_failure_next_wake_delay(restarted, 60) <= 1
        assert replay_retained_cnyes_failures(restarted)['responses_checked'] == 0
        current_time = time.time
        monkeypatch.setattr(time, 'time', lambda: current_time() + 2)
        assert replay.retained_failure_next_wake_delay(restarted, 60) == 0
        recovered = replay_retained_cnyes_failures(restarted)
        assert recovered['accepted_heads'] == 1 and recovered['http_requests'] == 0
        assert restarted.db.execute('SELECT count(*) FROM retained_failure_local_retries').fetchone()[0] == 0
        assert restarted.failure_diagnostic_focus.progress() == ledger
        assert all(sha256_file(path) == digest for path, digest in immutable.items())
        assert verify_bodies(tmp_path)['accepted']


def test_temporarily_missing_retained_pair_remains_scheduled(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        original, _ = prepare(queue, monkeypatch)
        data_path = reports(tmp_path)[0][0]
        backup = data_path.with_suffix('.local-fixture-backup')
        digest = sha256_file(data_path)
        data_path.rename(backup)  # Only the synthetic fixture is temporarily moved.
        try:
            first = replay_retained_cnyes_failures(queue)
            assert first['local_io_deferrals'] == 1 and first['accepted_heads'] == 0
            assert queue.db.execute('SELECT count(*) FROM retained_failure_replays').fetchone()[0] == 0
            assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        finally:
            backup.rename(data_path)
        assert sha256_file(data_path) == digest
        current_time = time.time
        monkeypatch.setattr(time, 'time', lambda: current_time() + 2)
        assert replay_retained_cnyes_failures(queue)['accepted_heads'] == 1


def test_local_retry_does_not_wake_for_a_newer_terminal_head(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        def unavailable(_job):
            raise OSError('synthetic local storage outage')
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', unavailable)
        assert replay_retained_cnyes_failures(queue)['local_io_deferrals'] == 1
        with queue.db:
            queue.db.execute("UPDATE articles SET state='source_not_found',error_code='source_not_found' WHERE url=?", (URL,))
        original = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert replay.retained_failure_next_wake_delay(queue, 60) == 60
        current_time = time.time
        monkeypatch.setattr(time, 'time', lambda: current_time() + 5)
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original


def test_repeated_local_outages_back_off_and_never_hot_loop(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        original, ledger = prepare(queue, monkeypatch)
        now = [time.time()]
        monkeypatch.setattr(time, 'time', lambda: now[0])

        def unavailable(_job):
            raise OSError('synthetic persistent local storage outage')

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', unavailable)
        for attempt, delay in enumerate((1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300), 1):
            result = replay_retained_cnyes_failures(queue)
            assert result['local_io_deferrals'] == 1 and result['http_requests'] == 0
            row = queue.db.execute('SELECT local_attempts,not_before_epoch FROM retained_failure_local_retries').fetchone()
            assert row[0] == attempt and row[1] == now[0] + delay
            assert replay.retained_failure_next_wake_delay(queue, 500) == delay
            assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
            now[0] += delay
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert queue.db.execute('SELECT count(*) FROM retained_failure_replays').fetchone()[0] == 0


def test_oversized_local_retry_does_not_cause_an_idle_wake_loop(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        original_read = queue.failure_diagnostic_focus.read_retained_response

        def unavailable(_job):
            raise OSError('synthetic local storage outage')

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', unavailable)
        assert replay_retained_cnyes_failures(queue)['local_io_deferrals'] == 1
        current_time = time.time
        monkeypatch.setattr(time, 'time', lambda: current_time() + 2)
        size = queue.db.execute('SELECT reserved_bytes FROM failure_evidence_focus_attempts').fetchone()[0]
        with queue.db:
            queue.db.execute('UPDATE failure_evidence_focus_attempts SET reserved_bytes=?',
                             (replay.RETAINED_FAILURE_DEFAULT_RAW_BYTES + 1,))
        assert replay.retained_failure_next_wake_delay(queue, 60) == 60
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        with queue.db:
            queue.db.execute('UPDATE failure_evidence_focus_attempts SET reserved_bytes=?', (size,))
        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', original_read)
        assert replay.retained_failure_next_wake_delay(queue, 60) == 0
        assert replay_retained_cnyes_failures(queue)['accepted_heads'] == 1


def test_deferred_local_item_does_not_block_another_eligible_item(cnyes_config, tmp_path, monkeypatch):
    second_url = URL.replace('/42', '/43')
    second_page = short_page(canonical=second_url, change={'newsId': 43, 'url': second_url},
                             schema_change={'url': second_url, 'mainEntityOfPage': {'@id': second_url}})
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec(urls=[URL, second_url]))
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, short_page()))
        fail(queue, Response(second_url, 200, {'content-type': 'text/html'}, second_page))
        simulated_now = time.time() + 4000
        monkeypatch.setattr(time, 'time', lambda: simulated_now)
        original_read = queue.failure_diagnostic_focus.read_retained_response
        blocked = []

        def first_item_unavailable(job):
            if not blocked:
                blocked.append(job[0])
                raise OSError('synthetic item-specific local outage')
            return original_read(job)

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', first_item_unavailable)
        first = replay_retained_cnyes_failures(queue, maximum_responses=1, maximum_wall_seconds=2)
        assert first['local_io_deferrals'] == 1 and first['accepted_heads'] == 0
        second = replay_retained_cnyes_failures(queue, maximum_responses=1, maximum_wall_seconds=2)
        assert second['accepted_heads'] == 1 and second['http_requests'] == 0
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE id=?', (blocked[0],)).fetchone()[0] is None
        assert queue.db.execute('SELECT count(*) FROM articles WHERE state="complete"').fetchone()[0] == 1
        assert replay_retained_cnyes_failures(queue, maximum_responses=1, maximum_wall_seconds=2)['responses_checked'] == 0


def test_untried_retained_original_wakes_after_bounded_replay(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    urls = [URL.replace('/42', f'/{number}') for number in range(42, 47)]
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec(urls=urls))
        for number, url in zip(range(42, 47), urls):
            page = short_page(canonical=url, change={'newsId': number, 'url': url},
                              schema_change={'url': url, 'mainEntityOfPage': {'@id': url}})
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, page))
        ledger = queue.failure_diagnostic_focus.progress()
        now = [time.time() + 4000]
        monkeypatch.setattr(time, 'time', lambda: now[0])

        first = replay_retained_cnyes_failures(queue, maximum_responses=4, maximum_wall_seconds=2)
        assert first['accepted_heads'] == first['responses_checked'] == 4
        remaining = queue.db.execute("SELECT url FROM articles WHERE body_sha256 IS NULL").fetchone()[0]
        assert replay.retained_failure_next_wake_delay(queue, 60) == 0

        # A source due, lease and the local byte budget still gate the hint.
        with queue.db:
            queue.db.execute('UPDATE articles SET due=? WHERE url=?', (now[0] + 12, remaining))
        assert replay.retained_failure_next_wake_delay(queue, 60) == 12
        with queue.db:
            queue.db.execute("UPDATE articles SET due=0,lease_owner='other',lease_until=? WHERE url=?",
                             (now[0] + 60, remaining))
        assert replay.retained_failure_next_wake_delay(queue, 60) == 60
        with queue.db:
            queue.db.execute('UPDATE articles SET lease_owner=NULL,lease_until=NULL WHERE url=?', (remaining,))
            size = queue.db.execute('SELECT reserved_bytes FROM failure_evidence_focus_attempts '
                                    'WHERE url_sha256=(SELECT id FROM articles WHERE url=?)', (remaining,)).fetchone()[0]
            queue.db.execute('UPDATE failure_evidence_focus_attempts SET reserved_bytes=? '
                             'WHERE url_sha256=(SELECT id FROM articles WHERE url=?)',
                             (replay.RETAINED_FAILURE_DEFAULT_RAW_BYTES + 1, remaining))
        assert replay.retained_failure_next_wake_delay(queue, 60) == 60
        with queue.db:
            queue.db.execute('UPDATE failure_evidence_focus_attempts SET reserved_bytes=? '
                             'WHERE url_sha256=(SELECT id FROM articles WHERE url=?)', (size, remaining))
        assert replay.retained_failure_next_wake_delay(queue, 60) == 0
        second = replay_retained_cnyes_failures(queue, maximum_responses=4, maximum_wall_seconds=2)
        assert second['accepted_heads'] == second['responses_checked'] == 1
        assert first['http_requests'] == second['http_requests'] == 0
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert replay.retained_failure_next_wake_delay(queue, 60) == 60
        assert verify_bodies(tmp_path)['accepted']


def test_oversized_candidates_do_not_hide_an_eligible_original(cnyes_config, tmp_path, monkeypatch):
    """The bounded candidate window must not fill with items outside the byte budget."""
    oversized_urls = [URL.replace('/42', f'/{number}') for number in range(43, 171)]
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        configure_focus(queue, spec(urls=[URL, *oversized_urls]))
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, short_page()))
        actual_size = queue.db.execute(
            'SELECT reserved_bytes FROM failure_evidence_focus_attempts WHERE outcome=\'raw_saved\'').fetchone()[0]
        with queue.db:
            queue.add_articles([{'url': url} for url in oversized_urls])
            rows = queue.db.execute('SELECT id,url FROM articles WHERE url<>?', (URL,)).fetchall()
            attempts = []
            for article_id, url in rows:
                # Attempt 1 failed to write; the retained second attempt is beyond this
                # test's byte budget. Its path must never be opened by this slice.
                for number, outcome, size in ((1, 'write_failed', 0), (2, 'raw_saved', actual_size + 1)):
                    attempts.append(('cnyes', article_id, number, queue.failure_diagnostic_focus.id,
                                     size, 'f' * 64, f'fixture/{url.rsplit("/", 1)[-1]}-{number}.receipt.json', outcome))
            queue.db.executemany('INSERT INTO failure_evidence_focus_attempts VALUES (?,?,?,?,?,?,?,?)', attempts)
            queue.db.execute('UPDATE failure_evidence_focus_cohorts SET reserved_responses=reserved_responses+?, '
                             'reserved_raw_bytes=reserved_raw_bytes+? WHERE cohort_id=?',
                             (len(attempts), len(rows) * (actual_size + 1), queue.failure_diagnostic_focus.id))
            queue.db.execute('UPDATE articles SET due=0')
        ledger = queue.failure_diagnostic_focus.progress()
        original_time = time.time
        monkeypatch.setattr(time, 'time', lambda: original_time() + 4000)

        result = replay_retained_cnyes_failures(queue, maximum_responses=1, maximum_wall_seconds=2,
                                               maximum_raw_bytes=actual_size)
        assert result['responses_checked'] == result['accepted_heads'] == 1
        assert result['http_requests'] == 0 and result['local_io_deferrals'] == 0
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert verify_bodies(tmp_path)['accepted']


def test_service_wakes_for_local_retry_while_provider_is_cooling_down(cnyes_config, tmp_path, monkeypatch):
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, service_idle_max_seconds=30,
                        adaptive_rate={'enabled': False})
    http, session = http_fixture(cnyes_config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        _, ledger = prepare(queue, monkeypatch)
        http.defer('cnyes', 120)
        original_read = queue.failure_diagnostic_focus.read_retained_response
        attempts = []

        def fail_once(job):
            attempts.append(time.monotonic())
            if len(attempts) == 1:
                raise OSError('synthetic temporary local outage')
            return original_read(job)

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', fail_once)
        run_service(queue, http, cnyes_config, max_articles=0, max_discovery_requests=0, duration_seconds=3)
        assert len(attempts) == 2 and attempts[1] - attempts[0] >= 1
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
        assert queue.db.execute('SELECT count(*) FROM retained_failure_local_retries').fetchone()[0] == 0
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert not session.calls and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_service_drains_untried_retained_originals_while_provider_is_cooling_down(cnyes_config, tmp_path, monkeypatch):
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, service_idle_max_seconds=30,
                        adaptive_rate={'enabled': False})
    http, session = http_fixture(cnyes_config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        urls = [URL.replace('/42', f'/{number}') for number in range(42, 47)]
        configure_focus(queue, spec(urls=urls))
        for number, url in zip(range(42, 47), urls):
            page = short_page(canonical=url, change={'newsId': number, 'url': url},
                              schema_change={'url': url, 'mainEntityOfPage': {'@id': url}})
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, page))
        ledger = queue.failure_diagnostic_focus.progress()
        current_time = time.time
        monkeypatch.setattr(time, 'time', lambda: current_time() + 4000)
        http.defer('cnyes', 120)
        deadline = http.provider_not_before['cnyes']
        run_service(queue, http, cnyes_config, max_articles=0, max_discovery_requests=0, duration_seconds=5)
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 5
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert http.provider_not_before['cnyes'] == deadline
        assert not session.calls and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_one_idle_owner_cycle_continues_durable_retained_backlog(cnyes_config, tmp_path, monkeypatch):
    """No publisher work must not force a new owner cycle between local batches."""
    from downloader import news_fulltext as owner
    from downloader import news_fulltext_cnyes_history as history
    from downloader import news_fulltext_cnyes_work as work
    from downloader import news_fulltext_urls as original_urls
    from test_news_fulltext import http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, adaptive_rate={'enabled': False},
                        cnyes_original_audit_priority_plan=None, cnyes_accepted_batch_priority_plan=None)
    http, session = http_fixture(cnyes_config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        urls = [URL.replace('/42', f'/{number}') for number in range(42, 47)]
        configure_focus(queue, spec(urls=urls))
        for number, url in zip(range(42, 47), urls):
            page = short_page(canonical=url, change={'newsId': number, 'url': url},
                              schema_change={'url': url, 'mainEntityOfPage': {'@id': url}})
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, page))
        ledger = queue.failure_diagnostic_focus.progress()
        original_time = time.time
        monkeypatch.setattr(time, 'time', lambda: original_time() + 4000)
        http.defer('cnyes', 120)
        deadline = http.provider_not_before['cnyes']
        # Isolate this lane from otherwise valid CNYES/history work so another
        # local continuation cannot mask the owner's own replay wake.
        monkeypatch.setattr(history, 'backfill', lambda *a, **k: {'exhausted': True})
        monkeypatch.setattr(work, 'backfill', lambda *a, **k: {'checked_heads': 0})
        monkeypatch.setattr(work, 'has_lookup_rechecks', lambda *a, **k: False)
        monkeypatch.setattr(original_urls, 'has_original_url_work', lambda *a, **k: False)
        owner.run_cycle(queue, http, cnyes_config, max_articles=0, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=3)
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 5
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert http.provider_not_before['cnyes'] == deadline
        assert not session.calls and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_future_local_retry_does_not_hold_other_provider_wake(
        cnyes_config, publisher_config, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner
    from downloader import news_fulltext_cnyes_history as history
    from downloader import news_fulltext_cnyes_work as work
    from downloader import news_fulltext_urls as original_urls
    from test_news_fulltext import FakeResponse, article_html, http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, adaptive_rate={'enabled': False},
                        cnyes_original_audit_priority_plan=None, cnyes_accepted_batch_priority_plan=None)
    cnyes_config['providers'].append(publisher_config['providers'][0])
    publisher_url = 'https://publisher.test/story'
    http, session = http_fixture(cnyes_config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        publisher_url: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    events = []
    try:
        _, ledger = prepare(queue, monkeypatch)
        with queue.db:
            queue.add_articles([{'url': publisher_url}])
        http.defer('cnyes', 120)
        http.defer('publisher', .25)
        deadline = http.provider_not_before['cnyes']
        monkeypatch.setattr(history, 'backfill', lambda *a, **k: {'exhausted': True})
        monkeypatch.setattr(work, 'backfill', lambda *a, **k: {'checked_heads': 0})
        monkeypatch.setattr(work, 'has_lookup_rechecks', lambda *a, **k: False)
        monkeypatch.setattr(original_urls, 'has_original_url_work', lambda *a, **k: False)
        original_read = queue.failure_diagnostic_focus.read_retained_response
        original_fetch = owner.fetch_article

        def read_with_one_local_failure(job):
            events.append('local')
            if events.count('local') == 1:
                raise OSError('synthetic local storage interruption')
            return original_read(job)

        def record_fetch(client, row):
            events.append('publisher')
            return original_fetch(client, row)

        monkeypatch.setattr(queue.failure_diagnostic_focus, 'read_retained_response', read_with_one_local_failure)
        monkeypatch.setattr(owner, 'fetch_article', record_fetch)
        owner.run_service(queue, http, cnyes_config, max_articles=1, max_discovery_requests=0, duration_seconds=2.5)
        assert events == ['local', 'publisher', 'local']
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 2
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert http.provider_not_before['cnyes'] == deadline
        assert {call[0] for call in session.calls} == {'https://publisher.test/robots.txt', publisher_url}
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_idle_retained_continuation_hands_off_online_body_first(
        cnyes_config, publisher_config, tmp_path, monkeypatch):
    from concurrent.futures import Future
    from downloader import news_fulltext as owner
    from downloader import news_fulltext_cnyes_history as history
    from downloader import news_fulltext_cnyes_work as work
    from downloader import news_fulltext_replay as replay
    from downloader import news_fulltext_urls as original_urls
    from test_news_fulltext import FakeResponse, article_html, http_fixture

    class ImmediateExecutor:
        def submit(self, function, *args):
            future = Future()
            try:
                future.set_result(function(*args))
            except Exception as error:
                future.set_exception(error)
            return future

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, adaptive_rate={'enabled': False},
                        cnyes_original_audit_priority_plan=None, cnyes_accepted_batch_priority_plan=None)
    cnyes_config['providers'].append(publisher_config['providers'][0])
    publisher_url = 'https://publisher.test/story'
    http, session = http_fixture(cnyes_config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        publisher_url: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    events = []
    try:
        urls = [URL.replace('/42', f'/{number}') for number in range(42, 47)]
        configure_focus(queue, spec(urls=urls))
        for number, url in zip(range(42, 47), urls):
            page = short_page(canonical=url, change={'newsId': number, 'url': url},
                              schema_change={'url': url, 'mainEntityOfPage': {'@id': url}})
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, page))
        with queue.db:
            queue.add_articles([{'url': publisher_url}])
        ledger = queue.failure_diagnostic_focus.progress()
        original_time = time.time
        monkeypatch.setattr(time, 'time', lambda: original_time() + 4000)
        http.defer('cnyes', 120)
        deadline = http.provider_not_before['cnyes']
        monkeypatch.setattr(history, 'backfill', lambda *a, **k: {'exhausted': True})
        monkeypatch.setattr(work, 'backfill', lambda *a, **k: {'checked_heads': 0})
        monkeypatch.setattr(work, 'has_lookup_rechecks', lambda *a, **k: False)
        monkeypatch.setattr(original_urls, 'has_original_url_work', lambda *a, **k: False)
        original_replay = replay.replay_retained_cnyes_failures
        original_accept = queue.accept_live_article_batch

        def record_replay(*args, **kwargs):
            result = original_replay(*args, **kwargs)
            events.append(('local_replay', result['accepted_heads']))
            return result

        def record_handoff(records, *args, **kwargs):
            result = original_accept(records, *args, **kwargs)
            events.append(('online_handoff', len(records)))
            return result

        monkeypatch.setattr(replay, 'replay_retained_cnyes_failures', record_replay)
        monkeypatch.setattr(queue, 'accept_live_article_batch', record_handoff)
        owner.run_cycle(queue, http, cnyes_config, max_articles=1, max_discovery_requests=0,
                        executor=ImmediateExecutor(), continuous_budgets=True, max_run_seconds=3)
        assert events[:3] == [('local_replay', 4), ('online_handoff', 1), ('local_replay', 1)]
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 6
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert http.provider_not_before['cnyes'] == deadline
        assert {call[0] for call in session.calls} == {'https://publisher.test/robots.txt', publisher_url}
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_unchanged_head_claim_failure_is_deferred_before_service_retries(cnyes_config, tmp_path, monkeypatch):
    """A claim with no progress must not wake the resident owner in a hot loop."""
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, adaptive_rate={'enabled': False})
    http, session = http_fixture(cnyes_config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        original, ledger = prepare(queue, monkeypatch, advance=False)
        now = [time.time() + 4000]
        monkeypatch.setattr(time, 'time', lambda: now[0])
        http.defer('cnyes', 120)
        failed_claims = []

        def unchanged_fence_failure(*args, **kwargs):
            failed_claims.append(1)
            return None

        monkeypatch.setattr(queue, 'claim_url', unchanged_fence_failure)
        run_service(queue, http, cnyes_config, max_articles=0, max_discovery_requests=0, duration_seconds=.8)
        assert len(failed_claims) == 1
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == original
        retry = queue.db.execute('SELECT not_before_epoch,last_error FROM retained_failure_local_retries').fetchone()
        assert retry[0] == now[0] + 1 and retry[1] == 'retained_failure_claim_no_progress'
        assert queue.failure_diagnostic_focus.progress() == ledger and not session.calls
        from stockagent.live.news_dashboard import _cnyes_local_replay_retries
        projection = _cnyes_local_replay_retries(queue.status(publish=False)['cnyes_local_replay_retries'])
        assert projection['entries']['diagnostic_originals']['records'] == 1
        assert projection['entries']['diagnostic_originals']['earliest_not_before_utc'] is not None

        queue.close()
        queue = NewsQueue(tmp_path, http.registry)
        configure_focus(queue, spec())
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        now[0] += 2
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 1 and result['http_requests'] == 0
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()
        assert queue.failure_diagnostic_focus.progress() == ledger and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_changed_but_ready_head_cannot_spin_or_block_other_originals(cnyes_config, tmp_path, monkeypatch):
    """Repeated claim races must defer only that original, not the whole owner."""
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, adaptive_rate={'enabled': False})
    http, session = http_fixture(cnyes_config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        second = URL.replace('/42', '/43')
        configure_focus(queue, spec(urls=[URL, second]))
        for number, url in ((42, URL), (43, second)):
            page = short_page(canonical=url, change={'newsId': number, 'url': url},
                              schema_change={'url': url, 'mainEntityOfPage': {'@id': url}})
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, page))
        ledger = queue.failure_diagnostic_focus.progress()
        now = [time.time() + 4000]
        monkeypatch.setattr(time, 'time', lambda: now[0])
        http.defer('cnyes', 120)
        claim = queue.claim_url
        raced_heads = []

        def changed_head_claim(url, *args, **kwargs):
            if url != URL:
                return claim(url, *args, **kwargs)
            with queue.db:
                queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (url,))
            raced_heads.append(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()))
            return None

        monkeypatch.setattr(queue, 'claim_url', changed_head_claim)
        run_service(queue, http, cnyes_config, max_articles=0, max_discovery_requests=0, duration_seconds=.8)
        assert len(raced_heads) == 1
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == raced_heads[0]
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (second,)).fetchone()[0] == 'complete'
        retry = queue.db.execute('SELECT last_error FROM retained_failure_local_retries').fetchone()
        assert retry[0] == 'retained_failure_claim_changed_head'
        assert queue.failure_diagnostic_focus.progress() == ledger
        monkeypatch.setattr(queue, 'claim_url', claim)
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        queue.close()
        queue = NewsQueue(tmp_path, http.registry)
        configure_focus(queue, spec(urls=[URL, second]))
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        now[0] += 2
        run_service(queue, http, cnyes_config, max_articles=0, max_discovery_requests=0, duration_seconds=.5)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert not session.calls and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_superseded_commit_with_ready_head_cannot_spin_or_block_other_originals(
        cnyes_config, tmp_path, monkeypatch):
    """A superseded durable commit must not continuously rewrite the same response."""
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, adaptive_rate={'enabled': False})
    http, session = http_fixture(cnyes_config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        second = URL.replace('/42', '/43')
        configure_focus(queue, spec(urls=[URL, second]))
        for number, url in ((42, URL), (43, second)):
            page = short_page(canonical=url, change={'newsId': number, 'url': url},
                              schema_change={'url': url, 'mainEntityOfPage': {'@id': url}})
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, page))
        ledger = queue.failure_diagnostic_focus.progress()
        now = [time.time() + 4000]
        monkeypatch.setattr(time, 'time', lambda: now[0])
        http.defer('cnyes', 120)
        accept_records = queue._accept_records
        raced_heads = []

        def changed_head_during_commit(records, relative, receipt, **kwargs):
            if any(record['requested_url'] == URL for record in records):
                with queue.db:
                    queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,"
                                     "lease_token=NULL,attempts=attempts+1 WHERE url=?", (URL,))
                raced_heads.append(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()))
            return accept_records(records, relative, receipt, **kwargs)

        monkeypatch.setattr(queue, '_accept_records', changed_head_during_commit)
        run_service(queue, http, cnyes_config, max_articles=0, max_discovery_requests=0, duration_seconds=.8)
        assert len(raced_heads) == 1
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == raced_heads[0]
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (second,)).fetchone()[0] == 'complete'
        retry = queue.db.execute('SELECT not_before_epoch,last_error FROM retained_failure_local_retries').fetchone()
        assert retry[:] == (now[0] + 1, 'retained_failure_commit_changed_head')
        assert queue.failure_diagnostic_focus.progress() == ledger and not session.calls
        monkeypatch.setattr(queue, '_accept_records', accept_records)
        queue.close()
        queue = NewsQueue(tmp_path, http.registry)
        configure_focus(queue, spec(urls=[URL, second]))
        assert replay_retained_cnyes_failures(queue)['responses_checked'] == 0
        now[0] += 2
        result = replay_retained_cnyes_failures(queue)
        assert result['accepted_heads'] == 1 and result['http_requests'] == 0
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()
        assert queue.failure_diagnostic_focus.progress() == ledger and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('new_condition', ['future_due', 'leased', 'terminal'])
def test_superseded_commit_with_nonready_head_does_not_add_local_retry(
        cnyes_config, tmp_path, monkeypatch, new_condition):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        _, ledger = prepare(queue, monkeypatch)
        accept_records = queue._accept_records
        newer = []

        def changed_head_during_commit(records, relative, receipt, **kwargs):
            with queue.db:
                if new_condition == 'future_due':
                    queue.db.execute("UPDATE articles SET state='retry',due=?,lease_owner=NULL,lease_until=NULL,"
                                     "lease_token=NULL WHERE url=?", (time.time() + 120, URL))
                elif new_condition == 'leased':
                    queue.db.execute("UPDATE articles SET lease_owner='new-owner',lease_until=?,lease_token='new-token' "
                                     "WHERE url=?", (time.time() + 120, URL))
                else:
                    queue.db.execute("UPDATE articles SET state='source_not_found',error_code='source_not_found',"
                                     "lease_owner=NULL,lease_until=NULL,lease_token=NULL WHERE url=?", (URL,))
            newer.append(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()))
            return accept_records(records, relative, receipt, **kwargs)

        monkeypatch.setattr(queue, '_accept_records', changed_head_during_commit)
        result = replay_retained_cnyes_failures(queue)
        assert result['superseded_heads'] == 1 and result['accepted_heads'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == newer[0]
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()
        assert not queue.db.execute('SELECT 1 FROM retained_failure_replays').fetchone()
        assert queue.failure_diagnostic_focus.progress() == ledger


def test_changed_head_before_acceptance_is_retryable_without_stopping_owner(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        _, ledger = prepare(queue, monkeypatch)
        accept = queue.accept
        newer = []

        def changed_before_accept(records, **kwargs):
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,"
                                 "lease_token=NULL,attempts=attempts+1 WHERE url=?", (URL,))
            newer.append(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()))
            return accept(records, **kwargs)

        monkeypatch.setattr(queue, 'accept', changed_before_accept)
        result = replay_retained_cnyes_failures(queue)
        assert result['superseded_heads'] == 1 and result['local_commit_deferrals'] == 1
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == newer[0]
        retry = queue.db.execute('SELECT last_error FROM retained_failure_local_retries').fetchone()
        assert retry[0] == 'retained_failure_accept_changed_head'
        assert not queue.db.execute('SELECT 1 FROM retained_failure_replays').fetchone()
        assert queue.failure_diagnostic_focus.progress() == ledger


def test_changed_head_after_claim_before_witness_does_not_abort_replay(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        _, ledger = prepare(queue, monkeypatch)
        claim = queue.claim_url
        newer = []

        def changed_after_claim(*args, **kwargs):
            row = claim(*args, **kwargs)
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,"
                                 "lease_token=NULL,attempts=attempts+1 WHERE url=?", (URL,))
            newer.append(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()))
            return row

        monkeypatch.setattr(queue, 'claim_url', changed_after_claim)
        result = replay_retained_cnyes_failures(queue)
        assert result['superseded_heads'] == 1 and result['local_commit_deferrals'] == 1
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == newer[0]
        retry = queue.db.execute('SELECT last_error FROM retained_failure_local_retries').fetchone()
        assert retry[0] == 'retained_failure_post_claim_changed_head'
        assert not queue.db.execute('SELECT 1 FROM retained_failure_replays').fetchone()
        assert queue.failure_diagnostic_focus.progress() == ledger


def test_unrelated_acceptance_value_error_remains_visible(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        def invalid_record(*args, **kwargs):
            raise ValueError('invalid source record')
        monkeypatch.setattr(queue, 'accept', invalid_record)
        with pytest.raises(ValueError, match='invalid source record'):
            replay_retained_cnyes_failures(queue)
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()


def test_unchanged_head_with_acceptance_contract_rejection_remains_visible(cnyes_config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        monkeypatch.setattr(queue, '_failure_replay_head_matches', lambda record: False)
        with pytest.raises(ValueError, match='retained_failure_head_changed_before_acceptance'):
            replay_retained_cnyes_failures(queue)
        assert not queue.db.execute('SELECT 1 FROM retained_failure_local_retries').fetchone()


def test_commit_deferrals_keep_later_original_batches_moving(cnyes_config, tmp_path, monkeypatch):
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, adaptive_rate={'enabled': False})
    http, session = http_fixture(cnyes_config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        urls = [URL.replace('/42', f'/{number}') for number in range(42, 48)]
        configure_focus(queue, spec(urls=urls))
        for number, url in zip(range(42, 48), urls):
            page = short_page(canonical=url, change={'newsId': number, 'url': url},
                              schema_change={'url': url, 'mainEntityOfPage': {'@id': url}})
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, page))
        ledger = queue.failure_diagnostic_focus.progress()
        now = time.time() + 4000
        monkeypatch.setattr(time, 'time', lambda: now)
        http.defer('cnyes', 120)
        accept_records = queue._accept_records
        superseded = []

        def first_four_superseded(records, relative, receipt, **kwargs):
            url = records[0]['requested_url']
            if len(superseded) < 4:
                with queue.db:
                    queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,"
                                     "lease_token=NULL,attempts=attempts+1 WHERE url=?", (url,))
                superseded.append(url)
            return accept_records(records, relative, receipt, **kwargs)

        monkeypatch.setattr(queue, '_accept_records', first_four_superseded)
        run_service(queue, http, cnyes_config, max_articles=0, max_discovery_requests=0, duration_seconds=.8)
        assert len(superseded) == 4 and len(set(superseded)) == 4
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 2
        assert queue.db.execute('SELECT count(*) FROM retained_failure_local_retries').fetchone()[0] == 4
        assert queue.failure_diagnostic_focus.progress() == ledger and not session.calls
    finally:
        http.close()
        queue.close()


def test_live_slow_publisher_does_not_delay_retained_original_continuation(
        cnyes_config, publisher_config, tmp_path, monkeypatch):
    """A slow unrelated response must not hold a ready local replay batch."""
    import threading
    from downloader import news_fulltext as owner
    from downloader import news_fulltext_replay as replay
    from test_news_fulltext import FakeResponse, article_html, http_fixture

    cnyes_config.update(retained_cnyes_failure_replay_enabled=True, cached_cnyes_replay_enabled=False,
                        cached_cnyes_metadata_replay_enabled=False, adaptive_rate={'enabled': False})
    cnyes_config['providers'].append(publisher_config['providers'][0])
    publisher_url = 'https://publisher.test/story'
    http, session = http_fixture(cnyes_config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        publisher_url: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    slow_started, release_slow = threading.Event(), threading.Event()
    slow_released_by_replay, slices = [], []
    try:
        urls = [URL.replace('/42', f'/{number}') for number in range(42, 47)]
        configure_focus(queue, spec(urls=urls))
        for number, url in zip(range(42, 47), urls):
            page = short_page(canonical=url, change={'newsId': number, 'url': url},
                              schema_change={'url': url, 'mainEntityOfPage': {'@id': url}})
            fail(queue, Response(url, 200, {'content-type': 'text/html'}, page))
        with queue.db:
            queue.add_articles([{'url': publisher_url}])
        ledger = queue.failure_diagnostic_focus.progress()
        original_time = time.time
        monkeypatch.setattr(time, 'time', lambda: original_time() + 4000)
        http.defer('cnyes', 120)
        deadline = http.provider_not_before['cnyes']
        original_fetch = owner.fetch_article
        original_replay = replay.replay_retained_cnyes_failures

        def wait_for_local_continuation(http, row):
            if row['provider'] == 'publisher':
                slow_started.set()
                slow_released_by_replay.append(release_slow.wait(2.5))
            return original_fetch(http, row)

        def record_slices(*args, **kwargs):
            result = original_replay(*args, **kwargs)
            slices.append(result['accepted_heads'])
            if sum(slices) >= 5 and slow_started.is_set():
                release_slow.set()
            return result

        monkeypatch.setattr(owner, 'fetch_article', wait_for_local_continuation)
        monkeypatch.setattr(replay, 'replay_retained_cnyes_failures', record_slices)
        owner.run_cycle(queue, http, cnyes_config, max_articles=1, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=5)
        assert slow_started.is_set() and slow_released_by_replay == [True]
        assert sum(slices) == 5 and len(slices) >= 2
        assert queue.db.execute("SELECT count(*) FROM articles WHERE provider='cnyes' AND state='complete'").fetchone()[0] == 5
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (publisher_url,)).fetchone()[0] == 'complete'
        assert queue.failure_diagnostic_focus.progress() == ledger
        assert http.provider_not_before['cnyes'] == deadline
        assert {call[0] for call in session.calls} == {'https://publisher.test/robots.txt', publisher_url}
        assert verify_bodies(tmp_path)['accepted']
    finally:
        release_slow.set()
        http.close()
        queue.close()


def test_durable_receipt_recovers_after_commit_failure_under_same_head(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        prepare(queue, monkeypatch)
        def fail_commit(*args):
            raise OSError('synthetic SQLite commit interruption')
        monkeypatch.setattr(queue, '_accept_records', fail_commit)
        with pytest.raises(OSError):
            replay_retained_cnyes_failures(queue)
        assert queue.db.execute('SELECT count(*) FROM retained_failure_replays').fetchone()[0] == 0
        assert queue.db.execute('SELECT state,body_sha256 FROM articles').fetchone()[:] == ('inflight', None)
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        assert restarted.db.execute('SELECT state FROM articles').fetchone()[0] == 'complete'
        assert verify_bodies(tmp_path)['accepted']


def test_recovered_receipt_cannot_overwrite_later_source_removal(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        prepare(queue, monkeypatch)
        def fail_commit(*args):
            raise OSError('synthetic SQLite commit interruption')
        monkeypatch.setattr(queue, '_accept_records', fail_commit)
        with pytest.raises(OSError):
            replay_retained_cnyes_failures(queue)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='source_body_removed',error_code='source_body_removed',lease_owner=NULL,lease_until=NULL")
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        row = restarted.db.execute('SELECT state,body_sha256 FROM articles').fetchone()
        assert row[:] == ('source_body_removed', None)
        assert dict(restarted.db.execute('SELECT key,value FROM counters'))['retained_failure_superseded_at_commit'] == 1


@pytest.mark.parametrize('options', [{'maximum_responses': True}, {'maximum_responses': 17},
    {'maximum_wall_seconds': float('nan')}, {'maximum_wall_seconds': True}, {'maximum_raw_bytes': True},
    {'maximum_raw_bytes': 17 << 20}])
def test_replay_has_explicit_resource_guards(cnyes_config, tmp_path, monkeypatch, options):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))) as queue:
        prepare(queue, monkeypatch)
        with pytest.raises(ValueError):
            replay_retained_cnyes_failures(queue, **options)


@pytest.mark.parametrize('value', [True, False, 'true', None, 1])
def test_runtime_opt_in_is_typed_and_bound_to_reviewed_focus(cnyes_config, tmp_path, value):
    cnyes_config.pop('provider_catalog', None)
    cnyes_config.update(failure_diagnostic_focus=spec(), failure_diagnostic_metadata_identity_upgrade=False,
                        retained_cnyes_failure_replay_enabled=value)
    path = tmp_path / 'candidate.json'
    atomic_write_json(path, cnyes_config)
    if type(value) is bool:
        config, _, _ = load_config(path)
        assert config['retained_cnyes_failure_replay_enabled'] is value
    else:
        with pytest.raises(ValueError, match='retained_cnyes_failure_replay_enabled'):
            load_config(path)
    if value is True:
        cnyes_config['failure_diagnostic_focus'] = None
        atomic_write_json(path, cnyes_config)
        with pytest.raises(ValueError, match='retained failure replay requires'):
            load_config(path)
