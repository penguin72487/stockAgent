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
