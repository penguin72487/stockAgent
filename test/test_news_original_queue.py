"""Routing/provenance/restart contracts on the canonical queue, no live HTTP."""

from copy import deepcopy
import hashlib
import json
import sqlite3
import threading
import time

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_urls import backfill_original_urls, original_url_progress, sync_original_url_routes
from scripts.audit_news_original_urls import inventory
from test_news_fulltext import FakeResponse, article_html, config, http_fixture
from test_news_original_urls import field, redirect


def alias(url, **kwargs):
    return {'url': redirect(b'\x08\x13' + field(4, url.encode())), 'date': '2014-02-03 08:00:00',
            'title': 'Retained metadata, not publisher-verified', **kwargs}


@pytest.fixture
def queue(config, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        yield value
    finally:
        value.close()


def test_literal_alias_is_not_a_body_or_copied_stock_association(queue):
    item = alias('https://publisher.test/a')
    with queue.db:
        queue.add_articles([item, item], origin_id=27)
    assert backfill_original_urls(queue)['literal_links'] == 1
    rows = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    assert rows[item['url']]['state'] == 'awaiting_authorization'
    assert rows['https://publisher.test/a']['state'] == 'pending'
    assert rows['https://publisher.test/a']['body_sha256'] is None
    links = list(queue.db.execute('SELECT article_id,origin_id,association_rows FROM source_links'))
    assert len(links) == 1 and links[0][:] == (rows[item['url']]['id'], 27, 2)
    proof = queue.db.execute('SELECT * FROM original_url_resolutions').fetchone()
    assert proof['original_id'] == rows['https://publisher.test/a']['id']
    assert proof['target_queued'] == 1 and proof['metadata_identity_verified'] == 0
    assert original_url_progress(queue)['body_acceptance_proven'] is False


def test_different_redirects_deduplicate_the_same_original(queue):
    first = alias('https://publisher.test/a')
    second = {**first, 'url': first['url'] + '&hl=zh-TW'}
    with queue.db:
        queue.add_articles([first, second])
    result = backfill_original_urls(queue)
    assert result['examined'] == 2 and result['literal_links'] == 2
    assert queue.db.execute("SELECT count(*) FROM articles WHERE provider='publisher'").fetchone()[0] == 1
    progress = original_url_progress(queue)
    assert progress['literal_links_to_queued_targets'] == 2  # Not two originals/bodies.
    assert progress['examined_urls'] == 2 and progress['counts'] == {'literal_original': 2}


@pytest.mark.parametrize('state', ['complete', 'retry_cooldown', 'inflight', 'paywall'])
def test_routing_preserves_existing_body_due_failure_and_lease(queue, state):
    deadline = time.time() + 3600
    with queue.db:
        queue.add_articles([{'url': 'https://publisher.test/a'}, alias('https://publisher.test/a')])
        queue.db.execute("UPDATE articles SET state=?,due=?,error_code='source_refusal',lease_owner='other',lease_until=?,body_sha256=?,body_bytes=9 WHERE provider='publisher'",
                         (state, deadline, deadline, 'a' * 64))
    before = queue.db.execute("SELECT state,due,error_code,lease_owner,lease_until,body_sha256,body_bytes FROM articles WHERE provider='publisher'").fetchone()[:]
    backfill_original_urls(queue)
    after = queue.db.execute("SELECT state,due,error_code,lease_owner,lease_until,body_sha256,body_bytes FROM articles WHERE provider='publisher'").fetchone()[:]
    assert after == before


def test_unreviewed_and_disabled_originals_remain_metadata_only(queue):
    with queue.db:
        queue.add_articles([alias('https://unreviewed.test/a')])
    backfill_original_urls(queue)
    assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 1
    proof = queue.db.execute('SELECT * FROM original_url_resolutions').fetchone()
    assert proof['original_url'] == 'https://unreviewed.test/a' and proof['target_queued'] == 0


def test_new_permission_routes_cached_literals_without_redecoding(config, tmp_path, monkeypatch):
    disabled = deepcopy(config)
    disabled['providers'][0]['enabled'] = False
    q = NewsQueue(tmp_path, PolicyRegistry(disabled, base=tmp_path))
    try:
        with q.db:
            q.add_articles([alias('https://publisher.test/a')])
        backfill_original_urls(q)
        proof = dict(q.db.execute('SELECT * FROM original_url_resolutions').fetchone())
        assert proof['target_queued'] == 0
    finally:
        q.close()
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        monkeypatch.setattr('downloader.news_fulltext_urls.resolve_google_literal_url', lambda _: pytest.fail('cached proof redecoded'))
        assert sync_original_url_routes(q) == 1
        assert q.db.execute("SELECT state FROM articles WHERE provider='publisher'").fetchone()[0] == 'pending'
        now = q.db.execute('SELECT * FROM original_url_resolutions').fetchone()
        assert now['envelope_sha256'] == proof['envelope_sha256'] and now['target_queued'] == 1
        assert original_url_progress(q)['examined_urls'] == 1
    finally:
        q.close()


def test_failure_rolls_back_proof_target_and_cursor_together(queue, monkeypatch):
    with queue.db:
        queue.add_articles([alias('https://publisher.test/a')])
    original = queue.add_articles
    def fail(rows, **kwargs):
        original(rows, **kwargs)
        raise OSError('injected storage failure')
    monkeypatch.setattr(queue, 'add_articles', fail)
    with pytest.raises(OSError, match='injected'):
        backfill_original_urls(queue)
    assert queue.db.execute('SELECT count(*) FROM original_url_resolutions').fetchone()[0] == 0
    assert queue.db.execute("SELECT count(*) FROM articles WHERE provider='publisher'").fetchone()[0] == 0
    assert queue.db.execute("SELECT value FROM queue_settings WHERE key='google_literal_url_cursor_v1'").fetchone() is None
    assert original_url_progress(queue)['examined_urls'] == 0
    monkeypatch.setattr(queue, 'add_articles', original)
    assert backfill_original_urls(queue)['examined'] == 1


def test_restart_resumes_and_future_metadata_updates_original_sort_only(config, tmp_path):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    item = alias('https://publisher.test/a')
    try:
        with q.db:
            q.add_articles([item, alias('https://publisher.test/b')])
        assert backfill_original_urls(q, limit=1)['examined'] == 1
    finally:
        q.close()
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert backfill_original_urls(q, limit=1)['examined'] == 1
        assert backfill_original_urls(q)['examined'] == 0
        with q.db:
            q.add_articles([{**item, 'date': '2026-10-10 08:00:00'}])
        assert q.db.execute("SELECT metadata_date_raw FROM articles WHERE url='https://publisher.test/a'").fetchone()[0] == '2026-10-10 08:00:00'
        with q.db:
            q.add_articles([alias('https://publisher.test/c')])
        assert backfill_original_urls(q)['examined'] == 1
        assert original_url_progress(q)['examined_urls'] == 3
    finally:
        q.close()


def test_all_states_participate_and_opaque_never_becomes_body_success(queue):
    opaque = {'url': redirect(b'\x08\x13' + field(4, b'AU_yqLopaque')), 'date': '2026-10-10'}
    with queue.db:
        queue.add_articles([opaque, alias('https://publisher.test/a')])
        queue.db.execute("UPDATE articles SET state='body_not_found' WHERE url=?", (opaque['url'],))
    for _ in range(3):
        backfill_original_urls(queue)
    progress = original_url_progress(queue)
    assert progress['examined_urls'] == 2 and progress['counts']['opaque_original'] == 1
    assert progress['progress_ratio'] == 1 and progress['body_acceptance_proven'] is False
    assert queue.db.execute('SELECT state,body_sha256 FROM articles WHERE url=?', (opaque['url'],)).fetchone()[:] == ('body_not_found', None)


def test_stop_bounded_batch_and_index_seek(queue):
    with queue.db:
        queue.add_articles([alias(f'https://publisher.test/{i}') for i in range(20)])
    stopped = threading.Event()
    stopped.set()
    assert backfill_original_urls(queue, stop=stopped)['examined'] == 0
    assert backfill_original_urls(queue, limit=3)['examined'] == 3
    plan = queue.db.execute('EXPLAIN QUERY PLAN SELECT rowid,id,url FROM articles INDEXED BY host_state WHERE host=? AND state=? AND rowid>? ORDER BY rowid LIMIT ?', ('news.google.com', 'awaiting_authorization', 0, 256)).fetchall()
    assert any('rowid>?' in r[3] for r in plan) and all('TEMP B-TREE' not in r[3] for r in plan)


@pytest.mark.parametrize('limit,seconds', [(0, .04), (-1, .04), (True, .04), (2049, .04), (1, 0), (1, float('nan')), (1, 1)])
def test_unbounded_slices_are_rejected(queue, limit, seconds):
    with pytest.raises(ValueError, match='invalid_original'):
        backfill_original_urls(queue, limit=limit, max_seconds=seconds)


def test_readonly_whole_cohort_inventory_accounts_for_dates_duplicates_and_targets(config, tmp_path):
    q = NewsQueue(tmp_path / 'source', PolicyRegistry(config, base=tmp_path))
    item = alias('https://publisher.test/a')
    try:
        with q.db:
            q.add_articles([item, {**item, 'url': item['url'] + '&hl=zh-TW'}, {'url': redirect(b'\x08\x13' + field(4, b'AU_yqLopaque'))}])
    finally:
        q.close()
    cfg = tmp_path / 'config.json'
    atomic_write_json(cfg, config)
    before = (tmp_path / 'source/queue.sqlite3').read_bytes()
    result = inventory(cfg, workspace=tmp_path / 'audit', root=tmp_path / 'source')
    assert result['examined_urls'] == 3 and result['whole_local_cohort_accounted_for']
    assert result['unique_original_urls'] == 1 and result['counts']['literal_original'] == 2
    assert result['provider_counts']['publisher']['earliest_metadata_day'] == '2014-02-03'
    assert result['cost']['publisher_requests'] == 0
    assert (tmp_path / 'source/queue.sqlite3').read_bytes() == before


@pytest.mark.parametrize('raw', ['{broken', '[]', '{"pending":true}', '{"pending":-1}', '{"pending":9223372036854775808}'])
def test_malformed_cursor_recovers_cached_proofs_without_resetting_source_state(queue, monkeypatch, raw):
    with queue.db:
        queue.add_articles([alias('https://publisher.test/a')])
    backfill_original_urls(queue)
    deadline = time.time() + 3600
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry_cooldown',due=?,error_code='http_403' WHERE provider='publisher'", (deadline,))
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', ('google_literal_url_cursor_v1', raw))
    monkeypatch.setattr('downloader.news_fulltext_urls.resolve_google_literal_url', lambda _: pytest.fail('cached proof redecoded'))
    assert backfill_original_urls(queue)['examined'] == 1
    assert original_url_progress(queue)['examined_urls'] == 1
    assert original_url_progress(queue)['checkpoint_recovery_events'] == 1
    recovery = queue.db.execute('SELECT original_value,reason FROM original_url_checkpoint_recoveries').fetchone()
    assert recovery[0] == raw
    assert queue.db.execute("SELECT state,due,error_code FROM articles WHERE provider='publisher'").fetchone()[:] == ('retry_cooldown', deadline, 'http_403')
    assert backfill_original_urls(queue)['examined'] == 0


@pytest.mark.parametrize('raw', ['{broken', '[]', '{"policy_fingerprint":"old","after":"not hex"}'])
def test_malformed_routing_checkpoint_recovers_on_restart(config, tmp_path, raw):
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.add_articles([alias('https://publisher.test/a')])
        backfill_original_urls(q)
        with q.db:
            q.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', ('google_literal_url_routing_v1', raw))
    finally:
        q.close()
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert sync_original_url_routes(q) == 1
        assert original_url_progress(q)['examined_urls'] == 1
        assert original_url_progress(q)['checkpoint_recovery_events'] == 1
        assert q.db.execute('SELECT original_value FROM original_url_checkpoint_recoveries').fetchone()[0] == raw
    finally:
        q.close()


def test_recovery_archive_and_cursor_replacement_rollback_together(queue):
    raw = '{broken'
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', ('google_literal_url_cursor_v1', raw))
        queue.db.execute("CREATE TRIGGER injected_recovery_failure BEFORE INSERT ON queue_settings WHEN NEW.key='google_literal_url_cursor_v1' BEGIN SELECT RAISE(ABORT,'injected recovery storage failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='injected recovery'):
        backfill_original_urls(queue)
    assert queue.db.execute('SELECT count(*) FROM original_url_checkpoint_recoveries').fetchone()[0] == 0
    assert queue.db.execute("SELECT value FROM queue_settings WHERE key='google_literal_url_cursor_v1'").fetchone()[0] == raw


def test_continuous_metadata_only_backlog_does_not_wait_for_next_service_cycle(config, queue, tmp_path):
    from downloader.news_fulltext import run_cycle
    http, session = http_fixture(config, tmp_path, {})
    config['original_url_resolution_batch'] = 1
    config['original_url_resolution_interval_seconds'] = .05
    deadline = time.time() + 3600
    http.provider_not_before['publisher'] = deadline
    try:
        with queue.db:
            queue.add_articles([alias(f'https://unreviewed.test/{i}') for i in range(3)])
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                           continuous_budgets=True, max_run_seconds=1)
        assert result['original_url_resolution']['examined_urls'] == 3
        assert result['cycle']['article_attempts_started'] == 0
        assert .08 <= result['cycle']['wall_seconds'] < 1  # Bounded interruptible waits, no spin.
        assert session.calls == [] and http.provider_not_before['publisher'] == deadline
    finally:
        http.close()


def test_metadata_only_continuation_flushes_completed_body_before_backlog_finishes(config, queue, tmp_path, monkeypatch):
    from downloader.news_fulltext import run_cycle
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/a': FakeResponse(article_html())}
    http, _ = http_fixture(config, tmp_path, routes)
    config['original_url_resolution_batch'] = 1
    config['original_url_resolution_interval_seconds'] = .05
    config['batch_articles'] = 64
    remaining_at_flush = []
    accept = queue.accept
    def observed_accept(records):
        remaining_at_flush.append(original_url_progress(queue)['remaining_urls'])
        return accept(records)
    monkeypatch.setattr(queue, 'accept', observed_accept)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/a'},
                                *[alias(f'https://unreviewed.test/{i}') for i in range(6)]])
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                           continuous_budgets=True, max_run_seconds=2)
        assert result['cycle']['durably_accepted_versions'] == 1
        assert remaining_at_flush and remaining_at_flush[0] > 0
        assert result['original_url_resolution']['examined_urls'] == 6
    finally:
        http.close()
