"""Cached recovery shares source proof and canonical crash-safe acceptance."""
import hashlib
import json
from pathlib import Path
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


@pytest.mark.parametrize('patch', [
    {'publish': True}, {'authorization_fingerprint': 'wrong'}, {'authorization_json': '{}'},
    {'response_sha256': 'wrong'}, {'captured_at_utc': '2026-01-01'},
    {'captured_at_utc': '2099-01-01T00:00:00+00:00'},
    {'request_url': 'https://api.cnyes.com/media/api/v1/newslist/category/unregistered'},
])
def test_invalid_cache_proof_is_visible_and_never_promoted(cfg, tmp_path, patch):
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
        monkeypatch.setattr(queue, 'accept', lambda _: (_ for _ in ()).throw(OSError('fsync failed')))
        with pytest.raises(OSError, match='fsync failed'):
            replay_cached_cnyes(queue)
        assert queue.db.execute('SELECT state FROM cached_bulk_replays').fetchone()[0] == 'pending'
        monkeypatch.setattr(queue, 'accept', accept)
        assert replay_cached_cnyes(queue)['accepted_heads'] == 1
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()


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
        replay_cached_cnyes(queue)
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
