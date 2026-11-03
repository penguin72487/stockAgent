"""Cached recovery shares source proof and canonical crash-safe acceptance."""
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_replay import replay_cached_cnyes
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from test_cnyes_news_bulk import cfg, registry_http, payload, response


CAPTURED = '2026-10-08T00:00:00+00:00'


def test_local_replay_status_is_scoped_read_only_and_unknown_before_schema(cfg, tmp_path):
    from downloader.news_fulltext_metadata_replay import _ensure_cohort
    from downloader.news_fulltext_replay import ensure_replay_schema, _ensure_retained_failure_retry_schema
    from downloader.news_fulltext_status import cnyes_local_replay_retries

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    source = registry.sources['cnyes']
    try:
        entries = queue.status(publish=False)['cnyes_local_replay_retries']['entries']
        assert entries == {'api_pages': None, 'metadata_batches': None, 'diagnostic_originals': None}
        ensure_replay_schema(queue)
        _ensure_cohort(queue, source)
        _ensure_retained_failure_retry_schema(queue)
        queue.failure_diagnostic_focus = SimpleNamespace(id='current-cohort', progress=lambda: {})
        empty = queue.status(publish=False)['cnyes_local_replay_retries']['entries']
        assert all(row == {'records': 0, 'earliest_not_before_utc': None} for row in empty.values())
        with queue.db:
            for fingerprint, digest, due in [(cnyes_bulk_fingerprint(source), 'a', 1800000120),
                                             ('old-contract', 'b', 1)]:
                queue.db.execute('INSERT INTO cached_bulk_local_retries VALUES (?,?,?,?,?,?)',
                    (fingerprint, digest, due, 1, CAPTURED, 'PRIVATE_ERROR'))
            for fingerprint, path, due in [(source.data_fingerprint, 'PRIVATE_PATH_A', 1800000040),
                                            ('old-contract', 'PRIVATE_PATH_B', 1)]:
                queue.db.execute('INSERT INTO retained_metadata_local_retries VALUES (?,?,?,?,?,?)',
                    (fingerprint, path, due, 1, CAPTURED, 'PRIVATE_ERROR'))
            for cohort, path, due in [('current-cohort', 'PRIVATE_RECEIPT_A', 1800000060),
                                      ('old-cohort', 'PRIVATE_RECEIPT_B', 1)]:
                queue.db.execute('INSERT INTO retained_failure_local_retries VALUES (?,?,?,?,?,?,?,?,?)',
                    (path, source.parser_repair_fingerprint, cohort, b'url', 1, due, 1, CAPTURED, 'PRIVATE_ERROR'))
        changes = queue.db.total_changes
        snapshot = queue.status(publish=False)['cnyes_local_replay_retries']
        entries = snapshot['entries']
        assert [entries[key]['records'] for key in ('api_pages', 'metadata_batches', 'diagnostic_originals')] == [1, 1, 1]
        assert entries['api_pages']['earliest_not_before_utc'] == '2027-01-15T08:02:00+00:00'
        assert entries['metadata_batches']['earliest_not_before_utc'] == '2027-01-15T08:00:40+00:00'
        assert entries['diagnostic_originals']['earliest_not_before_utc'] == '2027-01-15T08:01:00+00:00'
        assert queue.db.total_changes == changes and not (tmp_path/'status.json').exists()
        assert 'PRIVATE_' not in json.dumps(snapshot)
        assert cnyes_local_replay_retries(queue.db, source, None)['entries']['diagnostic_originals'] is None
        with queue.db:
            queue.db.execute('UPDATE retained_metadata_local_retries SET not_before_epoch=? WHERE source_fingerprint=?',
                             ('invalid', source.data_fingerprint))
        broken = cnyes_local_replay_retries(queue.db, source, queue.failure_diagnostic_focus)['entries']
        assert broken['metadata_batches'] is None and broken['api_pages']['records'] == 1
    finally:
        queue.close()


def retained(queue, data=None, *, clock=CAPTURED, patch=None):
    r = response(data or payload(ids=(1,), content='來源完整短快訊，原始數值 12.5，沒有摘要替換也沒有虛構發布時間。'))
    digest = hashlib.sha256(r.body).hexdigest()
    relative = 'quarantine/' + digest + '.parquet'
    path = queue.root / relative
    source = queue.registry.sources['cnyes']
    row = {'request_url': r.url, 'raw_response_body': r.body, 'response_sha256': digest,
           'captured_at_utc': clock, 'authorization_fingerprint': source.authorization_fingerprint,
           'authorization_json': json.dumps(source.authorization, sort_keys=True), 'errors_json': '[]', 'publish': False}
    row.update(patch or {})
    atomic_write_parquet(path, table_from_records([row]), compression='zstd', durable=True)
    receipt = {'parquet_path': relative, 'sha256': sha256_file(path), 'response_sha256': digest,
               'article_count': len(data['items']['data']) if data else 1, 'accepted_body_count': 0, 'publish': False}
    atomic_write_json(Path(str(path) + '.receipt.json'), receipt)
    return path


def inventory_fixture_pages(queue, expected):
    """Let the bounded inventory resume if one slice runs out under load."""
    from downloader.news_fulltext_replay import _inventory

    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        _inventory(queue, queue.registry.sources['cnyes'])
        count = queue.db.execute('SELECT count(*) FROM cached_bulk_replays').fetchone()[0]
        if count >= expected:
            return
        time.sleep(.005)
    raise AssertionError('cached fixture inventory did not finish within the test deadline')


def test_cached_recovery_preserves_source_time_and_has_no_http_or_duplicate_head(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        result = replay_cached_cnyes(queue)
        assert result['accepted_heads'] == 1 and result['http_requests'] == 0
        row = queue.db.execute('SELECT * FROM articles').fetchone()
        assert row['state'] == 'complete' and row['captured_at_utc'] == CAPTURED
        verified = verify_bodies(tmp_path)
        assert verified['accepted'] and verified['body_versions_verified'] == 1
        import pyarrow.parquet as pq
        body = pq.read_table(tmp_path / row['parquet_path']).to_pylist()[0]
        assert body['source_observed_at_utc'] == CAPTURED and body['reparsed_at_utc'] > CAPTURED
        assert body['acquisition_transport'] == 'retained_raw_reparse'
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        counts = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert counts['durably_accepted_cached_new_url_heads'] == 1
        assert counts['durable_workload_cached_source_reparse_new_heads'] == 1
    finally:
        queue.close()
    restarted = NewsQueue(tmp_path, registry)
    try:
        assert replay_cached_cnyes(restarted)['pages_checked'] == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        restarted.close()


@pytest.mark.parametrize('checked,preserve_failure', [
    ('2026-10-07T00:00:00+00:00', False),
    (CAPTURED, True),
    ('2026-10-09T00:00:00+00:00', True),
])
def test_cached_older_body_preserves_the_newer_source_failure_and_check_clock(
        cfg, tmp_path, checked, preserve_failure):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    url = 'https://news.cnyes.com/news/id/1'
    try:
        retained(queue)
        with queue.db:
            queue.add_articles([{'url': url}])
            queue.db.execute("UPDATE articles SET state='retry',due=0,error_code='transport_error',"
                             "last_checked_at_utc=?,attempts=2 WHERE url=?", (checked, url))
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
        head = queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        if preserve_failure:
            assert head['state'] == 'retry' and head['error_code'] == 'transport_error'
            assert head['last_checked_at_utc'] == checked
        else:
            assert head['state'] == 'complete' and head['error_code'] is None
            assert head['last_checked_at_utc'] == CAPTURED
        assert head['attempts'] == 2
        assert head['captured_at_utc'] == CAPTURED and head['body_sha256']
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()


def test_cached_older_body_preserves_source_failure_after_receipt_crash_restart(cfg, tmp_path, monkeypatch):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    url = 'https://news.cnyes.com/news/id/1'
    checked = '2026-10-09T00:00:00+00:00'
    try:
        retained(queue)
        with queue.db:
            queue.add_articles([{'url': url}])
            queue.db.execute("UPDATE articles SET state='retry',due=0,error_code='transport_error',"
                             "last_checked_at_utc=?,attempts=2 WHERE url=?", (checked, url))

        def crash_before_commit(*args, **kwargs):
            raise RuntimeError('simulated crash after durable receipt')

        monkeypatch.setattr(queue, '_accept_records', crash_before_commit)
        with pytest.raises(RuntimeError, match='simulated crash'):
            replay_cached_cnyes(queue)
    finally:
        queue.close()
    restarted = NewsQueue(tmp_path, registry)
    try:
        head = restarted.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
        assert head['state'] == 'retry' and head['error_code'] == 'transport_error'
        assert head['attempts'] == 2 and head['last_checked_at_utc'] == checked
        assert head['captured_at_utc'] == CAPTURED and head['body_sha256']
        assert verify_bodies(tmp_path)['accepted']
    finally:
        restarted.close()


def test_cached_replay_cannot_override_a_newer_cooldown_during_commit(cfg, tmp_path, monkeypatch):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    url = 'https://news.cnyes.com/news/id/1'
    deadline = time.time() + 3600
    checked = '2026-10-09T00:00:00+00:00'
    original = queue._accept_records
    def newer_cooldown(records, relative, receipt, **kwargs):
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry_cooldown',due=?,error_code='http_429',"
                             "last_checked_at_utc=? WHERE url=?", (deadline, checked, url))
        return original(records, relative, receipt, **kwargs)
    monkeypatch.setattr(queue, '_accept_records', newer_cooldown)
    try:
        retained(queue)
        with queue.db:
            queue.add_articles([{'url': url}])
        result = replay_cached_cnyes(queue)
        head = queue.db.execute('SELECT state,due,error_code,last_checked_at_utc,body_sha256 FROM articles WHERE url=?',
                                (url,)).fetchone()
        assert tuple(head) == ('retry_cooldown', deadline, 'http_429', checked, None)
        assert result['accepted_heads'] == 0
        assert queue.db.execute('SELECT state FROM cached_bulk_replays').fetchone()[0] == 'deferred'
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replay_waits').fetchone()[0] == 1
        monkeypatch.setattr(queue, '_accept_records', original)
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        with queue.db:
            queue.db.execute('UPDATE articles SET due=0 WHERE url=?', (url,))
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
    finally:
        queue.close()


@pytest.mark.parametrize('patch', [
    {'publish': True}, {'authorization_fingerprint': 'wrong'}, {'authorization_json': '{}'},
    {'response_sha256': 'wrong'}, {'captured_at_utc': '2026-01-01'},
    {'captured_at_utc': '2099-01-01T00:00:00+00:00'},
    {'request_url': 'https://api.cnyes.com/media/api/v1/newslist/category/unregistered'},
])
def test_invalid_cache_proof_is_visible_and_never_promoted(cfg, tmp_path, patch, monkeypatch):
    # This assertion covers source-proof admission; cooperative slice expiry
    # is tested separately and must not make this test depend on host load.
    from downloader import news_fulltext_replay as replay
    monkeypatch.setattr(replay, 'time', SimpleNamespace(monotonic=lambda: 1.0, time=time.time))
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue, patch=patch)
        assert replay_cached_cnyes(queue).get('accepted_heads', 0) == 0
        row = queue.db.execute('SELECT state,error_code FROM cached_bulk_replays').fetchone()
        assert row['state'] in {'failed_validation', 'other_source'}
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        queue.close()


@pytest.mark.parametrize('payment', [1, True, None])
def test_cached_paid_or_unproven_rows_are_not_accepted(cfg, tmp_path, payment, monkeypatch):
    # This tests public-body admission, not whether a 50 ms cooperative
    # inventory slice survives scheduler/DDL latency on a loaded test host.
    # Keep only this module's budget clock deterministic; production budgets
    # and actual source/observation clocks remain unchanged.
    from downloader import news_fulltext_replay as replay
    monkeypatch.setattr(replay, 'time', SimpleNamespace(monotonic=lambda: 1.0, time=time.time))
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    data = payload(ids=(1,))
    data['items']['data'][0]['payment'] = payment
    try:
        retained(queue, data)
        assert replay_cached_cnyes(queue)['accepted_heads'] == 0
        assert queue.db.execute('SELECT error_code FROM cached_bulk_replays').fetchone()[0] == 'bulk_post_not_public'
    finally:
        queue.close()


@pytest.mark.parametrize('payment', [1, True, None])
def test_exhausted_inventory_quantum_resumes_and_still_rejects_paid_or_unproven_rows(cfg, tmp_path, payment, monkeypatch):
    from downloader import news_fulltext_replay as replay
    ticks = iter((1.0, 1.0, 1.1))  # replay start, inventory start, expired slice
    clock = SimpleNamespace(monotonic=lambda: next(ticks, 1.1), time=time.time)
    monkeypatch.setattr(replay, 'time', clock)
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    data = payload(ids=(1,))
    data['items']['data'][0]['payment'] = payment
    try:
        retained(queue, data)
        first = replay_cached_cnyes(queue)
        assert first['pages_checked'] == first['accepted_heads'] == first['http_requests'] == 0
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replays').fetchone()[0] == 0
        assert queue._cached_cnyes_inventory_entries is not None
        assert not hasattr(queue, '_cached_cnyes_inventory_at')
        clock.monotonic = lambda: 1.2
        second = replay_cached_cnyes(queue)
        assert second['pages_checked'] == 1 and second['accepted_heads'] == second['http_requests'] == 0
        row = queue.db.execute('SELECT state,error_code FROM cached_bulk_replays').fetchone()
        assert tuple(row) == ('failed_validation', 'bulk_post_not_public')
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        queue.close()


@pytest.mark.parametrize('state', ['inflight', 'paywall_or_login', 'source_not_found', 'retry', 'retry_cooldown'])
def test_cached_source_never_overrides_lease_refusal_or_future_retry(cfg, tmp_path, state):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute('UPDATE articles SET state=?,due=?', (state, time.time() + 10000))
        assert replay_cached_cnyes(queue)['accepted_heads'] == 0
        assert queue.db.execute('SELECT state FROM articles').fetchone()[0] == state
    finally:
        queue.close()


def test_storage_failure_keeps_replay_pending_and_recovers_once(cfg, tmp_path, monkeypatch):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        accept = queue.accept
        monkeypatch.setattr(queue, 'accept', lambda *_, **_kwargs: (_ for _ in ()).throw(OSError('fsync failed')))
        with pytest.raises(OSError, match='fsync failed'):
            replay_cached_cnyes(queue)
        assert queue.db.execute('SELECT state FROM cached_bulk_replays').fetchone()[0] == 'pending'
        monkeypatch.setattr(queue, 'accept', accept)
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()


@pytest.mark.parametrize('unavailable_part', ['envelope', 'receipt'])
def test_cached_local_source_outage_is_retryable_across_restart(cfg, tmp_path, unavailable_part):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue)
        inventory_fixture_pages(queue, 1)
        target = path if unavailable_part == 'envelope' else Path(str(path) + '.receipt.json')
        original = target.read_bytes()
        target.unlink()
        result = replay_cached_cnyes(queue)
        assert result['local_io_deferrals'] == 1 and result['pages_checked'] == 0
        assert result['http_requests'] == 0
        assert queue.db.execute('SELECT state,error_code FROM cached_bulk_replays').fetchone()[:] == ('pending', None)
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
        assert queue.db.execute('SELECT local_attempts FROM cached_bulk_local_retries').fetchone()[0] == 1
        target.write_bytes(original)
    finally:
        queue.close()

    restarted = NewsQueue(tmp_path, registry)
    try:
        assert replay_cached_cnyes(restarted)['pages_checked'] == 0
        with restarted.db:
            restarted.db.execute('UPDATE cached_bulk_local_retries SET not_before_epoch=0')
        recovered = replay_cached_cnyes(restarted)
        assert recovered['accepted_heads'] == 1 and recovered['http_requests'] == 0
        assert restarted.db.execute('SELECT count(*) FROM cached_bulk_local_retries').fetchone()[0] == 0
        assert restarted.db.execute('SELECT captured_at_utc FROM articles').fetchone()[0] == CAPTURED
        assert verify_bodies(tmp_path)['accepted']
    finally:
        restarted.close()


def test_cached_local_outage_backs_off_without_hot_loop(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        inventory_fixture_pages(queue, 1)
        clock = [time.time()]
        monkeypatch.setattr(time, 'time', lambda: clock[0])

        def unavailable(*_args):
            raise OSError('synthetic local outage')

        monkeypatch.setattr(replay, 'replay_source_path', unavailable)
        for attempt, delay in enumerate((1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300), 1):
            assert replay_cached_cnyes(queue)['local_io_deferrals'] == 1
            row = queue.db.execute('SELECT local_attempts,not_before_epoch FROM cached_bulk_local_retries').fetchone()
            assert row[:] == (attempt, clock[0] + delay)
            assert replay.cached_bulk_local_retry_next_wake_delay(queue, 500) == delay
            assert replay_cached_cnyes(queue)['local_io_deferrals'] == 0
            clock[0] += delay
        assert queue.db.execute('SELECT state,error_code FROM cached_bulk_replays').fetchone()[:] == ('pending', None)
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        queue.close()


def test_unavailable_cached_page_does_not_starve_another(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        unavailable = retained(queue)
        available = retained(queue, payload(ids=(2,), content='第二則來源完整正文。' * 12))
        inventory_fixture_pages(queue, 2)
        with queue.db:
            queue.db.execute('UPDATE cached_bulk_replays SET priority=0 WHERE response_sha256=?', (unavailable.stem,))
            queue.db.execute('UPDATE cached_bulk_replays SET priority=1 WHERE response_sha256=?', (available.stem,))
        now = time.time()
        monkeypatch.setattr(time, 'time', lambda: now)
        original_path = replay.replay_source_path

        def fail_one(root, relative):
            if relative == str(unavailable.relative_to(root)):
                raise OSError('synthetic per-page outage')
            return original_path(root, relative)

        monkeypatch.setattr(replay, 'replay_source_path', fail_one)
        assert replay_cached_cnyes(queue, maximum_pages=1)['local_io_deferrals'] == 1
        next_result = replay_cached_cnyes(queue, maximum_pages=1)
        assert next_result['accepted_heads'] == 1 and next_result['http_requests'] == 0
        assert queue.db.execute('SELECT state FROM cached_bulk_replays WHERE response_sha256=?',
                                (unavailable.stem,)).fetchone()[0] == 'pending'
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()


def test_cached_retry_wakes_only_when_deferred_head_is_eligible(cfg, tmp_path):
    from downloader import news_fulltext_replay as replay

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue)
        inventory_fixture_pages(queue, 1)
        queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=?", (time.time() + 120,))
        assert replay_cached_cnyes(queue)['accepted_heads'] == 0
        with queue.db:
            queue.db.execute('INSERT INTO cached_bulk_local_retries VALUES (?,?,?,?,?,?)',
                (cnyes_bulk_fingerprint(registry.sources['cnyes']), path.stem,
                 time.time()-10, 1, CAPTURED, 'cached_bulk_local_io_unavailable'))
        assert replay.cached_bulk_local_retry_next_wake_delay(queue, 60) == 60
        with queue.db:
            queue.db.execute('UPDATE articles SET due=0')
        assert replay.cached_bulk_local_retry_next_wake_delay(queue, 60) == 1
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
    finally:
        queue.close()


def test_cached_retry_wake_uses_deadline_index(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        inventory_fixture_pages(queue, 1)
        monkeypatch.setattr(replay, 'replay_source_path',
                            lambda *_args: (_ for _ in ()).throw(OSError('synthetic local outage')))
        assert replay_cached_cnyes(queue)['local_io_deferrals'] == 1
        statements = []
        queue.db.set_trace_callback(statements.append)
        assert replay.cached_bulk_local_retry_next_wake_delay(queue, 60) <= 1
        queue.db.set_trace_callback(None)
        statement, = [sql for sql in statements if sql.startswith('SELECT min(l.not_before_epoch)')]
        plan = [row[3] for row in queue.db.execute('EXPLAIN QUERY PLAN ' + statement)]
        assert any('cached_bulk_local_retry_due' in row for row in plan)
        assert not any('TEMP B-TREE' in row for row in plan)
    finally:
        queue.close()


def test_idle_service_recovers_cached_page_during_publisher_cooldown(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_replay as replay
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cfg.update(cached_cnyes_replay_enabled=True, cached_cnyes_metadata_replay_enabled=False,
               retained_cnyes_failure_replay_enabled=False, adaptive_rate={'enabled': False},
               service_idle_max_seconds=30)
    http, session = http_fixture(cfg, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        path = retained(queue)
        original_path = replay.replay_source_path
        attempts = []

        def fail_once(root, relative):
            if relative == str(path.relative_to(root)):
                attempts.append(time.monotonic())
                if len(attempts) == 1:
                    raise OSError('synthetic local outage')
            return original_path(root, relative)

        monkeypatch.setattr(replay, 'replay_source_path', fail_once)
        http.defer('cnyes', 120)
        run_service(queue, http, cfg, max_articles=0, max_discovery_requests=0, duration_seconds=5)
        assert len(attempts) == 2 and attempts[1] - attempts[0] >= 1
        assert queue.db.execute('SELECT state,captured_at_utc FROM articles').fetchone()[:] == ('complete', CAPTURED)
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_local_retries').fetchone()[0] == 0
        assert not session.calls and verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('state', ['inflight', 'retry', 'retry_cooldown'])
def test_temporarily_blocked_cache_is_revisited_after_article_becomes_ready(cfg, tmp_path, state):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute('UPDATE articles SET state=?,due=?', (state, time.time() + 10000))
        assert replay_cached_cnyes(queue)['accepted_heads'] == 0
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL")
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()


def test_deferred_pages_do_not_reparse_while_original_retry_is_future(cfg, tmp_path, monkeypatch):
    import downloader.news_fulltext_replay as replay
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='retry',due=?", (time.time() + 10000,))
        assert replay_cached_cnyes(queue)['pages_checked'] == 1
        assert queue.db.execute('SELECT state FROM cached_bulk_replays').fetchone()[0] == 'deferred'
        monkeypatch.setattr(replay.pq, 'read_table', lambda *_args, **_kwargs: pytest.fail('blocked cache reread'))
        for _ in range(4):
            assert replay_cached_cnyes(queue)['pages_checked'] == 0
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replay_waits').fetchone()[0] == 1
    finally:
        queue.close()


def test_partial_deferred_acceptance_preserves_cumulative_heads_after_restart(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue, payload(ids=(1, 2)))
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='retry_cooldown',due=?", (time.time() + 10000,))
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
        row = queue.db.execute('SELECT state,accepted_heads FROM cached_bulk_replays').fetchone()
        assert tuple(row) == ('deferred', 1)
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, registry)
    try:
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        with queue.db:
            queue.db.execute("UPDATE articles SET due=0 WHERE state='retry_cooldown'")
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
        assert tuple(queue.db.execute('SELECT state,accepted_heads FROM cached_bulk_replays').fetchone()) == ('checked', 2)
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replay_waits').fetchone()[0] == 0
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        verified = verify_bodies(tmp_path)
        assert verified['accepted'] and verified['body_versions_verified'] == 2
    finally:
        queue.close()


@pytest.mark.parametrize('terminal', ['paywall_or_login', 'source_not_found', 'policy_blocked'])
def test_deferred_wakeup_clears_wait_but_preserves_terminal_refusal(cfg, tmp_path, terminal):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='retry',due=?", (time.time() + 10000,))
        assert replay_cached_cnyes(queue)['accepted_heads'] == 0
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?', (terminal,))
        assert replay_cached_cnyes(queue)['pages_checked'] == 1
        assert tuple(queue.db.execute('SELECT state,body_sha256 FROM articles').fetchone()) == (terminal, None)
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replay_waits').fetchone()[0] == 0
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
    finally:
        queue.close()


def legacy_checkpoint(queue, path, state='checked'):
    with queue.db:
        queue.db.execute('''CREATE TABLE cached_bulk_replays(
            response_sha256 TEXT NOT NULL,source_fingerprint TEXT NOT NULL,
            priority REAL NOT NULL,state TEXT NOT NULL DEFAULT 'pending',
            error_code TEXT,accepted_heads INTEGER NOT NULL DEFAULT 0,checked_at_utc TEXT,
            PRIMARY KEY(response_sha256,source_fingerprint)) WITHOUT ROWID''')
        queue.db.execute('INSERT INTO cached_bulk_replays '
            '(response_sha256,source_fingerprint,priority,state,checked_at_utc) VALUES (?,?,0,?,?)',
            (path.stem, cnyes_bulk_fingerprint(queue.registry.sources['cnyes']), state, CAPTURED))


def test_legacy_checked_schema_is_reexamined_once_without_new_source_or_reset(cfg, tmp_path):
    from scripts.audit_news_cached_replay import audit
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue)
        legacy_checkpoint(queue, path)
        before = audit(tmp_path, registry, [], sample_legacy_pages=1)
        assert before['accepted'] and before['legacy_checked_response_files'] == before['pending_response_files'] == 1
        assert before['examined_response_files'] == 0
        assert before['source_proofs'][0]['head_readiness'] == {'not_yet_registered_body': 1}
        result = replay_cached_cnyes(queue)
        assert result['accepted_heads'] == 1 and result['http_requests'] == 0
        assert queue.db.execute('SELECT checkpoint_contract_version FROM cached_bulk_replays').fetchone()[0] == 2
        after = audit(tmp_path, registry, [str(path.relative_to(tmp_path))])
        assert after['legacy_checked_response_files'] == after['pending_response_files'] == 0
        assert after['examined_response_files'] == 1
        assert after['source_proofs'][0]['head_readiness'] == {'already_stored_body': 1}
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()


@pytest.mark.parametrize('state', ['failed_validation', 'other_source', 'no_replayable_body'])
def test_old_terminal_checkpoints_are_not_reopened_by_schema_migration(cfg, tmp_path, state):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue)
        legacy_checkpoint(queue, path, state)
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        assert tuple(queue.db.execute('SELECT state,checked_at_utc FROM cached_bulk_replays').fetchone()) == (state, CAPTURED)
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        queue.close()


def test_readonly_progress_does_not_claim_deferred_pages_examined_complete(cfg, tmp_path):
    from scripts.audit_news_cached_replay import audit
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue)
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='retry',due=?", (time.time() + 10000,))
        replay_cached_cnyes(queue)
        result = audit(tmp_path, registry, [str(path.relative_to(tmp_path))])
        assert result['accepted'] and result['deferred_response_files'] == result['pending_response_files'] == 1
        assert result['examined_response_files'] == 0
        assert result['source_proofs'][0]['head_readiness'] == {'temporarily_blocked_missing_body': 1}
        assert not result['all_history_complete']
    finally:
        queue.close()


def test_readonly_audit_exposes_source_title_echo_without_promoting_it(cfg, tmp_path):
    from scripts.audit_news_cached_replay import audit
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        data = payload(ids=(1,))
        post = data['items']['data'][0]
        post['content'] = '<p>' + post['title'] + '</p>'
        path = retained(queue, data)
        replay_cached_cnyes(queue)
        result = audit(tmp_path, registry, [str(path.relative_to(tmp_path))])
        assert not result['accepted'] and result['publisher_http_requests'] == 0
        outcome = result['source_proofs'][0]['bounded_item_outcomes'][0]
        assert outcome['error'] == 'body_not_found' and outcome['source_content_equals_title']
        assert 'body' not in outcome and 'source_content' not in outcome
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        queue.close()


@pytest.mark.parametrize('patch,code', [
    ({'authorization_fingerprint': 'wrong'}, 'authorization_changed'),
    ({'captured_at_utc': '2026-01-01'}, 'observation_clock_invalid'),
    ({'captured_at_utc': '2099-01-01T00:00:00+00:00'}, 'observation_clock_invalid'),
])
def test_readonly_audit_rejects_invalid_authorization_or_source_clock(cfg, tmp_path, patch, code):
    from scripts.audit_news_cached_replay import audit
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue, patch=patch)
        replay_cached_cnyes(queue)
        with pytest.raises(ValueError, match=code):
            audit(tmp_path, registry, [str(path.relative_to(tmp_path))])
    finally:
        queue.close()


def test_pending_and_legacy_selection_use_bounded_ordered_indices(cfg, tmp_path):
    from downloader.news_fulltext_replay import ensure_replay_schema, _ready_replays
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        ensure_replay_schema(queue)
        statements = []
        queue.db.set_trace_callback(statements.append)
        assert _ready_replays(queue, cnyes_bulk_fingerprint(registry.sources['cnyes']), 4) == []
        queue.db.set_trace_callback(None)
        scans = [sql for sql in statements if sql.startswith('SELECT response_sha256,priority FROM cached_bulk_replays')]
        assert len(scans) == 2
        for sql in scans:
            assert sql.strip().endswith('LIMIT 4')
            plan = [r[3] for r in queue.db.execute('EXPLAIN QUERY PLAN ' + sql)]
            assert any('SEARCH cached_bulk_replays USING COVERING INDEX' in r for r in plan)
            assert not any('TEMP B-TREE' in r for r in plan)
    finally:
        queue.close()


def test_replay_is_bounded_and_missing_content_is_not_a_body(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        data = payload(ids=(2,))
        data['items']['data'][0]['content'] = None
        retained(queue, data)
        retained(queue)
        first = replay_cached_cnyes(queue, maximum_pages=1)
        assert first['pages_checked'] == 1
        second = replay_cached_cnyes(queue, maximum_pages=1)
        assert second['pages_checked'] == 1
        assert first['accepted_heads'] + second['accepted_heads'] == 1
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        queue.close()


def test_readonly_audit_proves_metadata_fallback_without_claiming_full_body(cfg, tmp_path):
    from scripts.audit_news_cached_replay import audit
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        data = payload(ids=(2,))
        data['items']['data'][0]['content'] = '&lt;p&gt;null&lt;/p&gt;'
        path = retained(queue, data)
        # Inventory and replay share bounded wall-time slices. Under load the
        # first slice may only establish the scan; resume it on later ticks.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            replay_cached_cnyes(queue)
            row = queue.db.execute('SELECT state FROM cached_bulk_replays WHERE response_sha256=?',
                                   (path.stem,)).fetchone()
            if row and row[0] != 'pending':
                break
            time.sleep(.005)
        result = audit(tmp_path, registry, [str(path.relative_to(tmp_path))])
        assert result['accepted'] and result['publisher_http_requests'] == 0
        assert result['source_proofs'][0]['metadata_only_html_fallback']
        assert result['source_proofs'][0]['validated_full_bodies'] == 0
        assert result['source_proofs'][0]['bulk_body_page_classification'] == 'bulk_metadata_only_html_fallback_pages'
        assert result['examined_response_files'] == 1
        assert not result['all_history_complete']
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        queue.close()


def test_html_only_contract_does_not_reparse_unchanged_api_or_reset_discovery(cfg, tmp_path):
    from downloader.news_fulltext import discovery_fingerprint
    registry, _ = registry_http(cfg, tmp_path)
    source = registry.sources['cnyes']
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
        previous = discovery_fingerprint(source)
        registry.sources['cnyes'] = replace(source, data_fingerprint='changed-html-only')
        assert discovery_fingerprint(registry.sources['cnyes']) == previous
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        queue.close()


def test_api_configuration_change_invalidates_cache_within_same_inventory_hour(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    source = registry.sources['cnyes']
    queue = NewsQueue(tmp_path, registry)
    try:
        retained(queue)
        assert replay_cached_cnyes(queue)['pages_checked'] == 1
        cfg['providers'][0]['bulk_text_permission_basis'] += '; updated API permission contract'
        changed, _ = registry_http(cfg, tmp_path)
        assert cnyes_bulk_fingerprint(changed.sources['cnyes']) != cnyes_bulk_fingerprint(source)
        registry.sources['cnyes'] = changed.sources['cnyes']
        assert replay_cached_cnyes(queue)['pages_checked'] == 1
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        queue.close()


@pytest.mark.parametrize('state', ['checked', 'failed_validation', 'other_source'])
def test_exact_html_api_checkpoints_migrate_without_reopening_examined_sources(cfg, tmp_path, state):
    from downloader.news_fulltext_replay import ensure_replay_schema
    registry, _ = registry_http(cfg, tmp_path)
    source = registry.sources['cnyes']
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue)
        ensure_replay_schema(queue)
        with queue.db:
            queue.db.execute('INSERT INTO cached_bulk_replays '
                '(response_sha256,source_fingerprint,priority,state,error_code,checked_at_utc) VALUES (?,?,0,?,?,?)',
                (path.stem, source.data_fingerprint, state, 'original-proof', CAPTURED))
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        migrated = queue.db.execute('SELECT * FROM cached_bulk_replays WHERE source_fingerprint=?',
                                   (cnyes_bulk_fingerprint(source),)).fetchone()
        assert migrated['state'] == state and migrated['checked_at_utc'] == CAPTURED
        assert migrated['error_code'] == 'original-proof'
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
    finally:
        queue.close()


def test_checkpoint_migration_does_not_replace_existing_destination_failure(cfg, tmp_path):
    from downloader.news_fulltext_replay import ensure_replay_schema
    registry, _ = registry_http(cfg, tmp_path)
    source = registry.sources['cnyes']
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue)
        ensure_replay_schema(queue)
        with queue.db:
            for fingerprint, state, error in [(source.data_fingerprint, 'checked', None),
                    (cnyes_bulk_fingerprint(source), 'failed_validation', 'preserve-destination-error')]:
                queue.db.execute('INSERT INTO cached_bulk_replays '
                    '(response_sha256,source_fingerprint,priority,state,error_code) VALUES (?,?,0,?,?)',
                    (path.stem, fingerprint, state, error))
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        outcome = queue.db.execute('SELECT state,error_code FROM cached_bulk_replays WHERE source_fingerprint=?',
                                   (cnyes_bulk_fingerprint(source),)).fetchone()
        assert tuple(outcome) == ('failed_validation', 'preserve-destination-error')
    finally:
        queue.close()


def test_live_source_empty_page_avoids_immediate_replay_without_body_acceptance(cfg, tmp_path):
    from downloader.news_fulltext import accept_discovery
    from downloader.news_fulltext_bulk import cnyes_records
    registry, http = registry_http(cfg, tmp_path)
    source = registry.sources['cnyes']
    queue = NewsQueue(tmp_path, registry)
    try:
        data = payload(ids=(2,))
        data['items']['data'][0]['content'] = None
        raw = response(data)
        task = {'url': raw.url, 'provider': 'cnyes', 'kind': 'cnyes_news'}
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,provider,kind) VALUES (?,?,?)',
                             (raw.url, 'cnyes', 'cnyes_news'))
        records, errors = cnyes_records(raw, http, source)
        links = [(e['url'], e['title'], e['metadata_date_raw']) for e in errors]
        assert accept_discovery(queue, task, raw, links, records, errors) == 0
        assert queue.db.execute('SELECT state FROM articles').fetchone()[0] == 'pending'
        assert queue.db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0] == 1
        assert queue.db.execute('SELECT state FROM cached_bulk_replays').fetchone()[0] == 'no_replayable_body'
        assert replay_cached_cnyes(queue)['pages_checked'] == 0
        assert list((tmp_path / 'quarantine').glob('*.receipt.json'))
        assert verify_bodies(tmp_path)['body_versions_verified'] == 0
        assert queue.metrics.snapshot()['providers']['cnyes']['counts']['cached_bulk_no_body_replays_avoided'] == 1
    finally:
        queue.close()


def test_mixed_source_page_with_valid_body_is_not_no_body_checkpointed(cfg, tmp_path):
    from downloader.news_fulltext import accept_discovery
    from downloader.news_fulltext_bulk import cnyes_records
    registry, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        data = payload(ids=(1, 2))
        data['items']['data'][1]['content'] = None
        raw = response(data)
        task = {'url': raw.url, 'provider': 'cnyes', 'kind': 'cnyes_news'}
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,provider,kind) VALUES (?,?,?)',
                             (raw.url, 'cnyes', 'cnyes_news'))
        records, errors = cnyes_records(raw, http, registry.sources['cnyes'])
        links = [(r['requested_url'], r['title'], r['metadata_date_raw']) for r in records]
        links.extend((e['url'], e['title'], e['metadata_date_raw']) for e in errors)
        assert accept_discovery(queue, task, raw, links, records, errors) == 1
        assert replay_cached_cnyes(queue)['pages_checked'] == 1
        assert queue.db.execute('SELECT state FROM cached_bulk_replays').fetchone()[0] == 'checked'
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1
    finally:
        queue.close()


def _insert_crosscheck_observation(queue, data, url, artifact):
    from downloader.news_fulltext_bulk import cnyes_pagination
    from downloader.news_fulltext_cnyes_history import CONTRACT_VERSION, digest, encoded

    source = queue.registry.sources['cnyes']
    r = response(data, url)
    info, posts = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    observation = {'provider': 'cnyes', 'contract_version': CONTRACT_VERSION,
        'scope_url': info['scope'], 'page': info['page'], 'request_url': r.url,
        'response_sha256': sha, 'source_bulk_fingerprint': cnyes_bulk_fingerprint(source),
        'authorization_fingerprint': source.authorization_fingerprint,
        'pagination': info, 'source_day_offset_seconds': 0, 'source_observed_at_utc': CAPTURED,
        'evidence_recorded_at_utc': CAPTURED, 'origin': 'normal_source_response',
        'body_completeness_verified': False,
        'identities': [{'newsId': p['newsId'], 'publishAt': p['publishAt'], 'source_post_sha256': digest(p)}
                       for p in posts], 'artifact': artifact}
    observation_sha = digest(observation)
    queue.db.execute('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
        (info['scope'], info['page'], sha, CONTRACT_VERSION, cnyes_bulk_fingerprint(source),
         source.authorization_fingerprint, observation_sha, encoded(observation)))
    return observation_sha


def test_missing_crosscheck_binds_original_request_per_observation(cfg, tmp_path):
    from downloader.news_fulltext_bulk import cnyes_url
    from downloader.news_fulltext_cnyes_history import initialize
    from downloader.news_fulltext_replay import ensure_replay_schema
    from scripts.audit_news_cnyes_retained_missing import audit
    from test_cnyes_news_bulk import BASE

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    data = payload(ids=(1,))
    try:
        path = retained(queue, data)
        initialize(queue)
        ensure_replay_schema(queue)
        artifact = {'kind': 'canonical_quarantine', 'parquet_path': 'quarantine/' + path.name,
                    'receipt_path': 'quarantine/' + path.name + '.receipt.json'}
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='source_article_empty_template'")
            exact = _insert_crosscheck_observation(queue, data, response(data).url, artifact)
            wrong = _insert_crosscheck_observation(queue, data,
                cnyes_url(BASE, start=1388534400, end=1388534500), artifact)
    finally:
        queue.close()
    result = audit(tmp_path, registry, {})
    assert result['contract'] == 'cnyes_retained_missing_crosscheck_v4'
    assert result['request_mismatched_observations'] == 1
    assert result['unresolved_matching_observations'] == 1
    assert result['target_outcomes'][0]['observation_sha256'] == [exact]
    assert result['target_outcomes'][0]['evidence_state'] == 'verified'
    assert result['request_mismatches'][0]['observation_sha256'] == wrong
    resumed = audit(tmp_path, registry, result)
    assert resumed['candidate_pairs'] == resumed['request_unverified_pairs'] == 1
    assert resumed['target_outcomes'] == [] and resumed['request_mismatched_observations'] == 1


def _request_scoped_history_fixture(cfg, tmp_path, monkeypatch, data=None):
    from downloader.news_fulltext_bulk import cnyes_url
    from downloader import news_fulltext_cnyes_history as history
    from downloader.news_fulltext_replay import ensure_replay_schema
    from test_cnyes_news_bulk import BASE

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    source = registry.sources['cnyes']
    if data is None:
        data = payload(ids=(1,))
        data['items']['data'][0]['content'] = None
    urls = [response(data).url, cnyes_url(BASE, start=1388534400, end=1388534500)]
    clocks = [CAPTURED, '2026-10-09T00:00:00+00:00']
    try:
        history.initialize(queue)
        ensure_replay_schema(queue)
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='source_article_empty_template'")
            artifacts, observations = [], []
            for url, clock in zip(urls, clocks):
                monkeypatch.setattr(history, 'now', lambda clock=clock: clock)
                artifact = history.retain_response(queue, response(data, url), source, accepted=[], restricted=False)
                artifacts.append(artifact)
                observations.append(_insert_crosscheck_observation(queue, data, url, artifact))
    finally:
        queue.close()
    return registry, data, urls, clocks, artifacts, observations


def test_missing_crosscheck_tracks_identical_bytes_as_distinct_request_envelopes(cfg, tmp_path, monkeypatch):
    from scripts.audit_news_cnyes_retained_missing import audit

    registry, data, urls, clocks, artifacts, observations = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    assert artifacts[0]['parquet_path'] != artifacts[1]['parquet_path']
    assert all(item['kind'] == 'cnyes_history_source_envelope' for item in artifacts)
    result = audit(tmp_path, registry, {})
    assert result['candidate_responses'] == result['responses_verified'] == 1
    assert result['candidate_source_envelopes'] == result['source_envelopes_verified'] == 2
    assert result['candidate_pairs'] == 2 and result['candidate_heads'] == 1
    assert not result['response_failures'] and not result['unresolved_matching_observations']
    assert result['pair_outcome_counts'] == {'bulk_body_not_provided': 2}
    by_path = {row['source_artifact_path']: row for row in result['target_outcomes']}
    for artifact, url, clock, observation in zip(artifacts, urls, clocks, observations):
        item = by_path[artifact['parquet_path']]
        assert item['source_request_sha256'] == hashlib.sha256(url.encode()).hexdigest()
        assert item['source_observed_at_utc'] == clock and item['observation_sha256'] == [observation]
        assert item['source_artifact_kind'] == artifact['kind'] and item['original_request_identity_checked']
    assert result['publisher_http_requests'] == result['production_queue_writes'] == 0
    partial = audit(tmp_path, registry, {}, max_responses=1)
    assert partial['unfinished_source_envelopes'] == partial['unfinished_responses'] == 1
    remainder = audit(tmp_path, registry, partial)
    assert remainder['candidate_source_envelopes'] == remainder['source_envelopes_verified'] == 1
    assert not remainder['unfinished_source_envelopes'] and not remainder['response_failures']
    assert {row['source_artifact_path'] for row in partial['target_outcomes'] + remainder['target_outcomes']} == set(by_path)
    complete = audit(tmp_path, registry, [partial, remainder],
                     only_responses=[hashlib.sha256(response(data).body).hexdigest()])
    assert complete['candidate_source_envelopes'] == 0
    assert complete['selected_responses_previously_verified'] == [hashlib.sha256(response(data).body).hexdigest()]
    assert complete['selected_responses_without_candidate'] == []


def test_reviewed_v3_proof_is_reused_only_for_unchanged_originals_and_reader(cfg, tmp_path, monkeypatch):
    from scripts import audit_news_cnyes_retained_missing as module

    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    result = module.audit(tmp_path, registry, {})
    prior = {**result, 'contract': 'cnyes_retained_missing_crosscheck_v3',
             'parser_identity': module._V3_REVIEWED_PARSER,
             'target_outcomes': [{key: value for key, value in item.items() if key != 'source_receipt_path'}
                                 for item in result['target_outcomes']]}
    reused = module.audit(tmp_path, registry, prior)
    assert reused['candidate_source_envelopes'] == 0 and reused['previously_verified_observations'] == 2
    incompatible = {**prior, 'parser_identity': {**prior['parser_identity'], 'downloader/news_fulltext_bulk.py': '0' * 64}}
    assert module.audit(tmp_path, registry, incompatible)['candidate_source_envelopes'] == 2
    path = tmp_path / artifacts[0]['receipt_path']
    atomic_write_json(path, {**json.loads(path.read_bytes()), 'sha256': '0' * 64})
    changed = module.audit(tmp_path, registry, prior)
    assert changed['candidate_source_envelopes'] == 1 and len(changed['response_failures']) == 1
    with monkeypatch.context() as patch:
        patch.setattr(module, '_V3_COMPAT_SELF_SHA256', '0' * 64)
        assert module.audit(tmp_path, registry, prior)['candidate_source_envelopes'] == 2


@pytest.mark.parametrize('path', [
    'scripts/audit_news_cached_replay.py',
    'downloader/news_fulltext_bulk.py',
    'downloader/news_fulltext_cnyes_history.py',
    'downloader/news_fulltext_policy.py',
])
def test_reviewed_reader_rejects_any_unreviewed_current_dependency(path):
    from scripts import audit_news_cnyes_retained_missing as module

    identity = {name: sha256_file(Path(name)) for name in module._V3_REVIEWED_CURRENT}
    assert module._reviewed_current_reader(identity)
    assert not module._reviewed_current_reader({**identity, path: '0' * 64})


def test_reviewed_historical_proofs_keep_their_original_policy_identity():
    from scripts import audit_news_cnyes_retained_missing as module

    key = 'downloader/news_fulltext_policy.py'
    old_policy = 'd4ee857f04aec54c3e17299a3ddd7cde0d492e11d8f65ccc6c4dbafcd62f26f1'
    assert module._V3_REVIEWED_CURRENT[key] != old_policy
    assert all(identity[key] == old_policy for identity in (
        module._V3_REVIEWED_PARSER, module._V4_REVIEWED_PARSER,
        module._V4_REVIEWED_RECENT_PARSER, module._V4_REVIEWED_PRE_PRIORITY_PARSER))


@pytest.mark.parametrize('script_hash,history_hash', [
    ('2a70776d3ac32383a9fc3263bc874f38f99a024e0995b7600efd0c65a0928126',
     'efa5c5d4aa9227b3df865ba2fde3a404ae7ff8e9b555b60c1663f1c5d044bfff'),
    ('c046261b751da208bd0c67d3feb2e28252900004236ed4f16582daea54f78379',
     '0d41bf62ae44dcf24c01a675a5be30dd70eff4f29208ffffc22bac17c0fe8997'),
])
def test_reviewed_v4_proof_survives_writer_only_change_but_not_reader_or_original_changes(
        cfg, tmp_path, monkeypatch, script_hash, history_hash):
    from scripts import audit_news_cnyes_retained_missing as module

    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    result = module.audit(tmp_path, registry, {})
    # Both exact reviewed versions retain the same per-original proof reader.
    previous_parser = {
        **module._V3_REVIEWED_CURRENT,
        'downloader/news_fulltext_policy.py': module._V3_REVIEWED_PARSER['downloader/news_fulltext_policy.py'],
        'scripts/audit_news_cnyes_retained_missing.py': script_hash,
        'downloader/news_fulltext_cnyes_history.py': history_hash,
    }
    prior = {**result, 'parser_identity': previous_parser}
    reused = module.audit(tmp_path, registry, prior)
    assert reused['candidate_source_envelopes'] == 0 and reused['previously_verified_observations'] == 2
    incompatible = {**prior, 'parser_identity': {**previous_parser, 'downloader/news_fulltext_bulk.py': '0' * 64}}
    assert module.audit(tmp_path, registry, incompatible)['candidate_source_envelopes'] == 2
    path = tmp_path / artifacts[0]['receipt_path']
    atomic_write_json(path, {**json.loads(path.read_bytes()), 'sha256': '0' * 64})
    changed = module.audit(tmp_path, registry, prior)
    assert changed['candidate_source_envelopes'] == 1 and len(changed['response_failures']) == 1
    with monkeypatch.context() as patch:
        patch.setattr(module, '_V3_COMPAT_SELF_SHA256', '0' * 64)
        assert module.audit(tmp_path, registry, prior)['candidate_source_envelopes'] == 2


@pytest.mark.parametrize('change', [
    {'contract': 'untrusted_contract'}, {'raw_response_bytes': -1}, {'article_bodies_accepted': True},
])
def test_missing_crosscheck_rejects_damaged_history_envelope_receipt(cfg, tmp_path, monkeypatch, change):
    from scripts.audit_news_cnyes_retained_missing import audit

    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    path = tmp_path / artifacts[0]['receipt_path']
    receipt = json.loads(path.read_bytes())
    atomic_write_json(path, {**receipt, **change})
    result = audit(tmp_path, registry, {})
    assert result['source_envelopes_verified'] == 1 and len(result['response_failures']) == 1
    assert result['response_failures'][0]['source_artifact_path'] == artifacts[0]['parquet_path']
    assert 'cached_history_envelope_receipt_invalid' in result['response_failures'][0]['error']
    assert len(result['target_outcomes']) == 1


@pytest.mark.parametrize('part', ['parquet_path', 'receipt_path'])
def test_missing_crosscheck_rechecks_changed_original_after_prior_proof(cfg, tmp_path, monkeypatch, part):
    from scripts.audit_news_cnyes_retained_missing import audit

    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    verified = audit(tmp_path, registry, {})
    assert not verified['response_failures'] and len(verified['target_outcomes']) == 2
    path = tmp_path / artifacts[0][part]
    damaged = bytearray(path.read_bytes())
    damaged[0] ^= 1
    path.write_bytes(damaged)
    resumed = audit(tmp_path, registry, verified)
    assert resumed['candidate_source_envelopes'] == 1 and len(resumed['response_failures']) == 1
    assert resumed['response_failures'][0]['source_artifact_path'] == artifacts[0]['parquet_path']
    assert resumed['target_outcomes'] == []


def test_missing_crosscheck_does_not_attribute_another_request_history_envelope(cfg, tmp_path, monkeypatch):
    from downloader.news_fulltext_bulk import cnyes_url
    from scripts.audit_news_cnyes_retained_missing import audit
    from test_cnyes_news_bulk import BASE

    registry, data, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            wrong = _insert_crosscheck_observation(queue, data,
                cnyes_url(BASE, start=1388534400, end=1388534600), artifacts[0])
    finally:
        queue.close()
    result = audit(tmp_path, registry, {})
    assert len(result['target_outcomes']) == 2 and result['source_envelopes_verified'] == 2
    assert result['unresolved_matching_observations'] == 1
    assert result['request_mismatches'][0]['observation_sha256'] == wrong
    assert result['request_mismatches'][0]['source_artifact_path'] == artifacts[0]['parquet_path']


def test_request_scoped_history_proofs_flow_into_issuer_comparison(cfg, tmp_path, monkeypatch):
    from datetime import datetime
    from scripts.audit_news_cnyes_retained_missing import audit as crosscheck
    from scripts.audit_news_cnyes_issuer_overlap import audit as issuer_audit
    from test_audit_news_issuer_overlap import inputs

    data = payload(ids=(1,))
    data['items']['data'][0].update(source='台灣證券交易所', stock=['1234'],
        market=[{'code': '1234', 'name': '測試公司'}], title='測試公司:公告董事會決議', content=None)
    news_root = tmp_path / 'actual_news'
    news_root.mkdir()
    registry, _, _, clocks, artifacts, _ = _request_scoped_history_fixture(cfg, news_root, monkeypatch, data)
    finlab_root = tmp_path / 'fixtures'
    finlab_root.mkdir()
    _, finlab, _, _, _ = inputs(finlab_root, records=[{'source_index': '17', 'symbol': '1234',
        'date': datetime(2014, 1, 1, 18), 'title': '公告董事會決議', 'info': '非新聞正文'}])
    checked = crosscheck(news_root, registry, {})
    result = issuer_audit(news_root, finlab, registry, checked, {})
    assert result['contract'] == 'cnyes_finlab_issuer_overlap_v3' and result['accepted']
    assert result['counts']['available_source_envelopes'] == 2
    assert result['counts']['source_envelopes_attempted'] == 1
    row, = result['rows']
    assert row['match_state'] == 'single_local_nonempty_info'
    assert {item['source_artifact_path'] for item in row['source_observations']} == {
        artifact['parquet_path'] for artifact in artifacts}
    assert {item['source_observed_at_utc'] for item in row['source_observations']} == set(clocks)
    assert row['selected_source_artifact_path'] in {artifact['parquet_path'] for artifact in artifacts}
    assert result['publisher_requests'] == result['queue_writes'] == 0
    assert not result['body_equivalence_verified'] and not result['all_history_complete']
    broken = {**checked, 'unfinished_source_envelopes': 1}
    with pytest.raises(ValueError, match='invalid_cnyes_proof_scope'):
        issuer_audit(news_root, finlab, registry, broken, {})


def _accepted_batch_crosscheck_fixture(cfg, root):
    """Two real request identities share bytes and an accepted batch, not clocks."""
    from downloader.news_fulltext_bulk import cnyes_url
    from downloader.news_fulltext_cnyes_history import initialize
    from downloader.news_fulltext_replay import ensure_replay_schema
    from test_cnyes_news_bulk import BASE

    registry, _ = registry_http(cfg, root)
    queue = NewsQueue(root, registry)
    data = payload(ids=(1, 2))
    data['items']['data'][0].update(source='台灣證券交易所', stock=['1234'],
        market=[{'code': '1234', 'name': '測試公司'}], title='測試公司:依證券交易法第43條之1公告', content=None)
    urls = [cnyes_url(BASE, start=1388534400, end=1388620800),
            cnyes_url(BASE, start=1388620800, end=1388707200)]
    clocks = [CAPTURED, '2026-10-09T00:00:00+00:00']
    relative, receipt_relative = 'bodies/fixture.parquet', 'receipts/fixture.json'
    artifact = {'kind': 'accepted_body_batch', 'parquet_path': relative, 'receipt_path': receipt_relative}
    try:
        initialize(queue)
        ensure_replay_schema(queue)
        raw = response(data).body
        sha = hashlib.sha256(raw).hexdigest()
        rows = [{'provider': 'cnyes', 'source_request_url': url, 'raw_response_body': raw,
                 'response_sha256': sha, 'captured_at_utc': clock,
                 'authorization_fingerprint': registry.sources['cnyes'].authorization_fingerprint,
                 'publish': False} for url, clock in zip(urls, clocks)]
        path = root / relative
        atomic_write_parquet(path, table_from_records(rows), durable=True)
        atomic_write_json(root / receipt_relative, {'contract_version': 1, 'parquet_path': relative,
            'sha256': sha256_file(path), 'rows': len(rows), 'publish': False}, durable=True)
        with queue.db:
            queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (relative, receipt_relative))
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='source_article_empty_template'")
            observations = [_insert_crosscheck_observation(queue, data, url, artifact) for url in urls]
    finally:
        queue.close()
    return registry, data, urls, clocks, artifact, observations


def test_accepted_batch_missing_proof_keeps_request_receipt_and_clock_through_issuer(cfg, tmp_path):
    from datetime import datetime
    from scripts.audit_news_cnyes_retained_missing import audit as crosscheck
    from scripts.audit_news_cnyes_issuer_overlap import audit as issuer_audit
    from scripts.audit_news_cnyes_finlab_disclosure_leads import candidates as lead_candidates
    from scripts.plan_news_cnyes_disclosure_candidates import plan, valid_request_scoped_provenance
    from test_audit_news_issuer_overlap import inputs

    root = tmp_path / 'actual_news'
    root.mkdir()
    registry, _, urls, clocks, artifact, observations = _accepted_batch_crosscheck_fixture(cfg, root)
    before = {path: sha256_file(path) for path in (root / 'queue.sqlite3', root / artifact['parquet_path'],
                                                     root / artifact['receipt_path'])}
    partial = crosscheck(root, registry, {}, max_responses=1)
    remainder = crosscheck(root, registry, partial)
    assert partial['candidate_source_envelopes'] == 2 and partial['unfinished_source_envelopes'] == 1
    assert remainder['candidate_source_envelopes'] == 1 and remainder['unfinished_source_envelopes'] == 0
    checked = crosscheck(root, registry, {})
    assert checked['candidate_responses'] == 1 and checked['source_envelopes_verified'] == 2
    assert checked['candidate_source_request_groups'] == 2 and checked['candidate_original_artifacts'] == 1
    assert checked['candidate_pairs'] == 2 and checked['candidate_heads'] == 1
    assert checked['pair_outcome_counts'] == {'bulk_body_not_provided': 2}
    by_request = {row['source_request_sha256']: row for row in checked['target_outcomes']}
    for url, clock, observation in zip(urls, clocks, observations):
        row = by_request[hashlib.sha256(url.encode()).hexdigest()]
        assert row['source_observed_at_utc'] == clock and row['observation_sha256'] == [observation]
        assert row['source_receipt_path'] == artifact['receipt_path'] and row['original_request_identity_checked']
    finlab_root = tmp_path / 'finlab_fixture'
    finlab_root.mkdir()
    _, finlab, _, _, _ = inputs(finlab_root, records=[{'source_index': '17', 'symbol': '1234',
        'date': datetime(2014, 1, 1, 18), 'title': '不同的公告', 'info': '非新聞正文'}])
    result = issuer_audit(root, finlab, registry, checked, {})
    assert result['contract'] == 'cnyes_finlab_issuer_overlap_v3' and result['accepted']
    assert result['counts']['available_source_envelopes'] == 2
    row, = result['rows']
    assert row['match_state'] == 'no_local_match' and len(row['source_observations']) == 2
    assert valid_request_scoped_provenance(row['source_observations'], row['selected_response_sha256'],
        row['selected_source_artifact_path'], selected_receipt=row['selected_source_receipt_path'],
        selected_request=row['selected_source_request_sha256'], require_details=True)
    old = {'contract_version': 1, 'source_audit_sha256': 'f' * 64, 'candidate_heads': 0,
           'candidate_days': 0, 'groups': [], 'source_requests': 0, 'production_queue_writes': 0,
           'historical_gaps_retired': False}
    result['input_audit_sha256'] = ['e' * 64, old['source_audit_sha256']]
    planned = plan(result, old)
    planned['input_sha256'] = ['a' * 64, 'b' * 64, 'd' * 64]
    assert planned['contract'] == 'cnyes_43_1_incremental_candidates_v3'
    assert len(lead_candidates(old, planned, 'd' * 64)) == 1
    assert {path: sha256_file(path) for path in before} == before
    assert checked['publisher_http_requests'] == checked['production_queue_writes'] == 0
    assert result['publisher_requests'] == result['queue_writes'] == 0
    assert not result['body_equivalence_verified'] and not result['all_history_complete']


@pytest.mark.parametrize('failure', ['uncommitted', 'missing_catalog', 'receipt', 'request', 'authorization', 'response'])
def test_accepted_batch_missing_proof_fails_closed(cfg, tmp_path, failure):
    from scripts.audit_news_cnyes_retained_missing import audit
    import pyarrow.parquet as pq

    registry, _, _, _, artifact, _ = _accepted_batch_crosscheck_fixture(cfg, tmp_path)
    path, receipt = tmp_path / artifact['parquet_path'], tmp_path / artifact['receipt_path']
    if failure in {'uncommitted', 'missing_catalog'}:
        with sqlite3.connect(tmp_path / 'queue.sqlite3') as db:
            db.execute('DELETE FROM accepted_batches' if failure == 'uncommitted' else 'DROP TABLE accepted_batches')
        db.close()
    elif failure == 'receipt':
        atomic_write_json(receipt, {**json.loads(receipt.read_bytes()), 'sha256': '0' * 64})
    else:
        rows = pq.read_table(path).to_pylist()
        for row in rows:
            if failure == 'request':
                row['source_request_url'] += '&wrong=1'
            elif failure == 'authorization':
                row['authorization_fingerprint'] = 'wrong'
            else:
                row['raw_response_body'] += b'changed'
        atomic_write_parquet(path, table_from_records(rows), durable=True)
        atomic_write_json(receipt, {**json.loads(receipt.read_bytes()), 'sha256': sha256_file(path)})
    result = audit(tmp_path, registry, {})
    assert result['candidate_source_envelopes'] == len(result['response_failures']) == 2
    assert not result['target_outcomes'] and result['source_envelopes_verified'] == 0


@pytest.mark.parametrize('reviewed_old_reader', [False, True])
def test_accepted_batch_prior_proof_rechecks_committed_membership(cfg, tmp_path, reviewed_old_reader):
    from scripts import audit_news_cnyes_retained_missing as module

    registry, _, _, _, _, _ = _accepted_batch_crosscheck_fixture(cfg, tmp_path)
    prior = module.audit(tmp_path, registry, {})
    assert len(prior['target_outcomes']) == 2
    if reviewed_old_reader:
        prior['parser_identity'] = module._V4_REVIEWED_PARSER
    assert module.audit(tmp_path, registry, prior)['candidate_source_envelopes'] == 0
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as db:
        db.execute('DELETE FROM accepted_batches')
    db.close()
    resumed = module.audit(tmp_path, registry, prior)
    assert resumed['candidate_source_envelopes'] == len(resumed['response_failures']) == 2
    assert not resumed['target_outcomes']


def test_missing_crosscheck_wrong_request_cannot_poison_verified_post_hash(cfg, tmp_path):
    from downloader.news_fulltext_bulk import cnyes_url
    from downloader.news_fulltext_cnyes_history import digest, encoded, initialize
    from downloader.news_fulltext_replay import ensure_replay_schema
    from scripts.audit_news_cnyes_retained_missing import audit
    from test_cnyes_news_bulk import BASE

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    data = payload(ids=(1,))
    try:
        path = retained(queue, data)
        initialize(queue)
        ensure_replay_schema(queue)
        artifact = {'kind': 'canonical_quarantine', 'parquet_path': 'quarantine/' + path.name,
                    'receipt_path': 'quarantine/' + path.name + '.receipt.json'}
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='source_article_empty_template'")
            exact = _insert_crosscheck_observation(queue, data, response(data).url, artifact)
            wrong = _insert_crosscheck_observation(queue, data,
                cnyes_url(BASE, start=1388534400, end=1388534500), artifact)
            row = queue.db.execute('SELECT * FROM cnyes_history_page_observations WHERE observation_sha256=?',
                                   (wrong,)).fetchone()
            observation = json.loads(row['observation_json'])
            observation['identities'][0]['source_post_sha256'] = '0' * 64
            queue.db.execute('UPDATE cnyes_history_page_observations SET observation_sha256=?,observation_json=? '
                             'WHERE observation_sha256=?', (digest(observation), encoded(observation), wrong))
    finally:
        queue.close()
    result = audit(tmp_path, registry, {})
    assert result['response_failures'] == [] and result['request_mismatched_observations'] == 1
    assert result['target_outcomes'][0]['observation_sha256'] == [exact]
    assert result['target_outcomes'][0]['outcome'] == 'validated_source_body'


@pytest.mark.parametrize('wrong_post_hash', [False, True])
def test_missing_crosscheck_does_not_verify_a_wrong_request_only(cfg, tmp_path, wrong_post_hash):
    from downloader.news_fulltext_bulk import cnyes_url
    from downloader.news_fulltext_cnyes_history import digest, encoded, initialize
    from downloader.news_fulltext_replay import ensure_replay_schema
    from scripts.audit_news_cnyes_retained_missing import audit
    from test_cnyes_news_bulk import BASE

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    data = payload(ids=(1,))
    try:
        path = retained(queue, data)
        initialize(queue)
        ensure_replay_schema(queue)
        artifact = {'kind': 'canonical_quarantine', 'parquet_path': 'quarantine/' + path.name,
                    'receipt_path': 'quarantine/' + path.name + '.receipt.json'}
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='source_article_empty_template'")
            wrong = _insert_crosscheck_observation(queue, data,
                cnyes_url(BASE, start=1388534400, end=1388534500), artifact)
            if wrong_post_hash:
                row = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                       'WHERE observation_sha256=?', (wrong,)).fetchone()
                observation = json.loads(row[0])
                observation['identities'][0]['source_post_sha256'] = '0' * 64
                queue.db.execute('UPDATE cnyes_history_page_observations SET observation_sha256=?,observation_json=? '
                                 'WHERE observation_sha256=?', (digest(observation), encoded(observation), wrong))
    finally:
        queue.close()
    result = audit(tmp_path, registry, {})
    assert result['candidate_pairs'] == result['request_unverified_pairs'] == 1
    assert result['request_mismatched_observations'] == 1
    assert result['target_outcomes'] == []
    assert result['unresolved_matching_observations'] == 1
    raw_only, = result['source_only_target_outcomes']
    assert raw_only['evidence_state'] == 'verified_raw_source_only'
    assert raw_only['observation_reference_verified'] is False
    assert raw_only['outcome'] == 'validated_source_body'
    assert raw_only['matches_observed_post_hash'] is not wrong_post_hash
    assert raw_only['source_observed_at_utc'] == CAPTURED
    assert raw_only['source_request_sha256'] == hashlib.sha256(response(data).url.encode()).hexdigest()
    assert 'body' not in raw_only and len(raw_only['mismatched_observation_sha256']) == 1


def test_missing_crosscheck_binds_exact_post_and_skips_previously_verified_pairs(cfg, tmp_path, monkeypatch):
    from downloader.news_fulltext_cnyes_history import initialize
    from downloader.news_fulltext_replay import ensure_replay_schema
    from scripts.audit_news_cnyes_retained_missing import audit

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    data = payload(ids=(1, 2))
    data['items']['data'][1]['content'] = None
    try:
        path = retained(queue, data)
        initialize(queue)
        ensure_replay_schema(queue)
        artifact = {'kind': 'canonical_quarantine', 'parquet_path': 'quarantine/' + path.name,
                    'receipt_path': 'quarantine/' + path.name + '.receipt.json'}
        with queue.db:
            queue.add_articles([{'url': f'https://news.cnyes.com/news/id/{i}'} for i in (1, 2)])
            queue.db.execute("UPDATE articles SET state='source_article_empty_template'")
            _insert_crosscheck_observation(queue, data, response(data).url, artifact)
    finally:
        queue.close()
    legacy = {'rows': [{'news_id': 2, 'observations': [
        {'source_response_sha256': path.stem, 'evidence_state': 'verified', 'content_class': 'content_missing'}]}]}
    all_pairs = audit(tmp_path, registry, {})
    assert all_pairs['pair_outcome_counts'] == {'validated_source_body': 1, 'bulk_body_not_provided': 1}
    # Legacy proofs did not bind each observation to the original request.
    assert audit(tmp_path, registry, legacy)['candidate_pairs'] == 2
    prior = {**all_pairs, 'target_outcomes': [row for row in all_pairs['target_outcomes'] if row['news_id'] == 2]}
    result = audit(tmp_path, registry, prior)
    assert result['publisher_http_requests'] == result['production_queue_writes'] == 0
    assert result['unresolved_matching_observations'] == 0
    assert result['candidate_pairs'] == result['responses_verified'] == 1
    assert result['pair_outcome_counts'] == {'validated_source_body': 1}
    assert result['target_outcomes'][0]['news_id'] == 1
    assert result['target_outcomes'][0]['head']['state'] == 'source_article_empty_template'
    assert result['target_outcomes'][0]['source_observed_at_utc'] == CAPTURED
    assert 'body' not in result['target_outcomes'][0]
    assert audit(tmp_path, registry, [prior, result])['candidate_pairs'] == 0
    stale = {**result, 'parser_identity': {'changed': 'parser'}}
    assert audit(tmp_path, registry, [prior, stale])['candidate_pairs'] == 1
    selected = audit(tmp_path, registry, {}, only_responses=[path.stem])
    assert selected['selected_responses_requested'] == 1 and selected['candidate_pairs'] == 2
    absent = audit(tmp_path, registry, {}, only_responses=['f' * 64])
    assert absent['selected_responses_without_candidate'] == ['f' * 64]
    with pytest.raises(ValueError, match='invalid_selected_responses'):
        audit(tmp_path, registry, {}, only_responses=['not-a-digest'])
    from scripts import audit_news_cnyes_retained_missing as module
    actual_sha256 = module.sha256_file
    policy_reads = []

    def changed_policy(path):
        digest = actual_sha256(path)
        if str(path).endswith('downloader/news_fulltext_policy.py'):
            policy_reads.append(1)
            return digest if len(policy_reads) == 1 else 'changed'
        return digest

    with monkeypatch.context() as context:
        context.setattr(module, 'sha256_file', changed_policy)
        with pytest.raises(ValueError, match='parser_changed_during_audit'):
            audit(tmp_path, registry, {})
    db = sqlite3.connect((tmp_path / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)
    try:
        assert db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0
    finally:
        db.close()
    db = sqlite3.connect(tmp_path / 'queue.sqlite3')
    try:
        with db:
            row = db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()
            broken = json.loads(row[0])
            broken['pagination']['ids_sha256'] = '0' * 64
            # Leave the old observation SHA untouched, as happens with a
            # damaged row that still has an otherwise reusable prior proof.
            db.execute('UPDATE cnyes_history_page_observations SET observation_json=?', (json.dumps(broken),))
    finally:
        db.close()
    invalid = audit(tmp_path, registry, all_pairs)
    assert invalid['unresolved_matching_observations'] == 1 and invalid['candidate_pairs'] == 0


@pytest.mark.parametrize('journal_suffix', ['-wal', '-journal'])
def test_missing_crosscheck_rejects_a_post_hash_mismatch_and_uncheckpointed_journal(cfg, tmp_path, journal_suffix):
    from downloader.news_fulltext_cnyes_history import digest
    from downloader.news_fulltext_replay import ensure_replay_schema
    from scripts.audit_news_cached_replay import _source_proof
    from scripts.audit_news_cnyes_retained_missing import audit

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    data = payload(ids=(1,))
    try:
        path = retained(queue, data)
        ensure_replay_schema(queue)
        source = registry.sources['cnyes']
        with pytest.raises(ValueError, match='post_identity_mismatch'):
            _source_proof(tmp_path, registry, source, queue.db, str(path.relative_to(tmp_path)),
                          cnyes_bulk_fingerprint(source), target_hashes={1: {digest({'wrong': 1})}})
    finally:
        queue.close()
    (tmp_path / ('queue.sqlite3' + journal_suffix)).write_bytes(b'uncheckpointed')
    with pytest.raises(ValueError, match='requires_empty_wal_and_journal'):
        audit(tmp_path, registry, {})


@pytest.mark.parametrize('changed_file', ['parquet', 'receipt'])
def test_cached_source_proof_rejects_original_changed_during_read(cfg, tmp_path, monkeypatch, changed_file):
    from scripts import audit_news_cached_replay as module
    from downloader.news_fulltext_replay import ensure_replay_schema

    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        path = retained(queue, payload(ids=(1,)))
        ensure_replay_schema(queue)
        receipt = Path(str(path) + '.receipt.json')
        actual_read = module.pq.read_table

        def read_and_change(*args, **kwargs):
            table = actual_read(*args, **kwargs)
            target = path if changed_file == 'parquet' else receipt
            data = bytearray(target.read_bytes())
            data[0] ^= 1
            target.write_bytes(data)
            return table

        monkeypatch.setattr(module.pq, 'read_table', read_and_change)
        with pytest.raises(ValueError, match='cached_source_changed_during_proof'):
            module._source_proof(tmp_path, registry, registry.sources['cnyes'], queue.db,
                                 str(path.relative_to(tmp_path)), cnyes_bulk_fingerprint(registry.sources['cnyes']))
    finally:
        queue.close()
