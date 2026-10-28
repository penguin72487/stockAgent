"""A cached empty CNYES revision selection must never hide newly ready work."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import random
import sqlite3
import time

import pytest

from downloader.news_fulltext_bulk import cnyes_scope
from downloader.news_fulltext_cnyes_index import ENDPOINT, request_url
from downloader.news_fulltext_policy import NewsError
from test_news_cnyes_public_index import DAY, cfg, queue
from test_news_fulltext import FakeResponse, article_html, config, http_fixture


@pytest.fixture
def blocked(queue):
    source = queue.registry.sources['cnyes']
    first, second = (request_url(source, DAY, page) for page in (1, 2))
    with queue.db:
        queue.db.executemany('INSERT INTO discovery(url,kind,provider,due) VALUES (?,\'index\',\'cnyes\',?)',
                             [(first, time.time() + 600), (second, 0)])
        queue.db.execute('INSERT INTO cnyes_index_revision_rounds VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (cnyes_scope(first), source.cnyes_index_data_fingerprint, source.authorization_fingerprint,
             '1' * 64, '2' * 64, 'anchor', None, None, None, None, '2026-10-28T00:00:00+00:00'))
    return queue, source, first, second


def traced_selection(queue, source, **options):
    traced = []
    queue.db.set_trace_callback(traced.append)
    try:
        rows = [dict(r) for r in queue.discovery_candidates(source, 17, **options)]
    finally:
        queue.db.set_trace_callback(None)
    scans = [sql for sql in traced if 'SELECT * FROM discovery' in sql]
    return rows, scans


def test_empty_revision_is_reused_across_unrelated_provider_writes(blocked):
    queue, source, _, _ = blocked
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES ('https://other.test/rss','feed','other')")
        queue.db.execute("INSERT INTO queue_settings VALUES ('unrelated','test')")
    assert traced_selection(queue, source) == ([], [])


def test_unrelated_cnyes_diagnostics_do_not_invalidate_empty_proof(blocked):
    queue, source, first, _ = blocked
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    with queue.db:
        queue.db.execute("UPDATE discovery SET attempts=attempts+1,priority=priority-1,"
                         "error_code='diagnostic',due=due WHERE url=?", (first,))
    assert traced_selection(queue, source) == ([], [])


@pytest.mark.parametrize('change', ['insert', 'due', 'phase', 'round_delete', 'external'])
def test_changed_revision_or_new_work_invalidates_immediately(blocked, change):
    queue, source, first, second = blocked
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    if change == 'external':
        with closing(sqlite3.connect(queue.root / 'queue.sqlite3')) as db:
            with db:
                db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
        expected = first
    else:
        with queue.db:
            if change == 'insert':
                expected = 'https://news.cnyes.com/rss'
                queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','cnyes')", (expected,))
            elif change == 'due':
                expected = first
                queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
            elif change == 'phase':
                expected = second
                queue.db.execute("UPDATE cnyes_index_revision_rounds SET phase='pages',anchor_total=31,"
                                 "anchor_pages=2,anchor_ids_sha256=?,anchor_response_sha256=?", ('a'*64, 'b'*64))
            else:
                expected = second
                queue.db.execute('DELETE FROM cnyes_index_revision_rounds')
    rows, scans = traced_selection(queue, source)
    assert scans and expected in {row['url'] for row in rows}


def test_first_page_deadline_invalidates_without_database_change(blocked, monkeypatch):
    queue, source, first, _ = blocked
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=100 WHERE url=?', (first,))
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 99)
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 100)
    assert traced_selection(queue, source)[0][0]['url'] == first


def test_endpoint_circuit_deadline_cannot_hide_a_ready_first_page(blocked, monkeypatch):
    queue, source, first, _ = blocked
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
            ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, 100, 'transport', first))
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 99)
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 100)
    assert traced_selection(queue, source)[0][0]['url'] == first


def test_wall_clock_step_during_proof_cannot_hide_newly_due_work(blocked, monkeypatch):
    queue, source, first, _ = blocked
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=100.15 WHERE url=?', (first,))
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
            ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, 100, 'transport', first))
    clock = [100.1]
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: clock[0])
    original = queue.discovery_dispatch_filter

    def stepped(source, bulk_allowed=True, *, include_circuits=True, now=None):
        clock[0] = 99.9
        try:
            if now is None:
                return original(source, bulk_allowed, include_circuits=include_circuits)
            return original(source, bulk_allowed, include_circuits=include_circuits, now=now)
        finally:
            clock[0] = 100.2

    # Only proof construction uses the instance attribute; the actual initial
    # selection still sees 100.1. The wall clock steps during proof creation.
    monkeypatch.setattr(queue, 'discovery_dispatch_filter', stepped)
    assert queue.discovery_candidates(source, 17) == []
    assert queue.discovery_candidates(source, 17)[0]['url'] == first


def test_idle_wake_uses_proof_and_respects_endpoint_expiry(blocked, monkeypatch):
    queue, source, first, _ = blocked
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
            ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, 100, 'transport', first))
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 99)
    assert queue.discovery_candidates(source, 17) == []
    traced = []
    queue.db.set_trace_callback(traced.append)
    try:
        assert queue.next_wake_delay({}, maximum=60) == 1
    finally:
        queue.db.set_trace_callback(None)
    assert not any('SELECT min(due) FROM discovery' in sql for sql in traced)
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 100)
    assert queue.discovery_candidates(source, 17)[0]['url'] == first


def test_retired_page_reactivation_invalidates_empty_selection(blocked):
    queue, source, first, second = blocked
    with queue.db:
        queue.db.execute("UPDATE cnyes_index_revision_rounds SET phase='pages',anchor_total=31,"
                         "anchor_pages=2,anchor_ids_sha256=?,anchor_response_sha256=?", ('a'*64, 'b'*64))
        queue.db.execute('INSERT INTO cnyes_index_revision_retired VALUES (?,?,?,?,?,?,?,?,?)',
                         (cnyes_scope(first), 2, second, source.cnyes_index_data_fingerprint,
                          source.authorization_fingerprint, '1'*64, '[]', '2026-10-28T00:00:00+00:00', 1))
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    with queue.db:
        queue.db.execute('UPDATE cnyes_index_revision_retired SET active=0')
    assert traced_selection(queue, source)[0][0]['url'] == second


def test_changed_failure_scope_and_endpoint_circuit_invalidate(blocked):
    queue, source, first, _ = blocked
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',
            ('cnyes_index_request_failure_scope_v1:' + source.cnyes_index_data_fingerprint, '{}'))
    assert traced_selection(queue, source)[1]
    assert traced_selection(queue, source) == ([], [])
    with queue.db:
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
            ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, time.time()+30, 'transport', first))
    assert traced_selection(queue, source)[1]


def test_removed_endpoint_circuit_releases_due_page_immediately(blocked):
    queue, source, first, _ = blocked
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
            ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, time.time()+900, 'transport', first))
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    with queue.db:
        queue.db.execute('DELETE FROM bulk_endpoint_cooldowns')
    assert traced_selection(queue, source)[0][0]['url'] == first


def test_bulk_scope_and_policy_identity_do_not_share_proof(blocked):
    queue, source, _, _ = blocked
    base = source.cnyes_apis[0]
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (base+'?page=1',))
    assert traced_selection(queue, source, bulk_allowed=False)[0] == []
    assert traced_selection(queue, source, bulk_allowed=False) == ([], [])
    assert traced_selection(queue, source, bulk_allowed=True)[0][0]['url'] == base+'?page=1'
    assert queue.discovery_candidates(replace(source, allowed=False), 17) == []


def test_temp_schema_change_disables_optimization_without_hiding_work(blocked):
    queue, source, first, _ = blocked
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    queue.db.execute('CREATE TEMP TABLE unrelated_schema_change(x)')
    assert traced_selection(queue, source)[1]
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
    assert traced_selection(queue, source)[0][0]['url'] == first


def test_cache_setup_failure_falls_back_to_canonical_query(blocked):
    queue, source, first, _ = blocked
    queue.db.execute('CREATE TEMP TABLE cnyes_revision_dispatch_cache_generation(x)')
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source)[1]
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
    assert traced_selection(queue, source)[0][0]['url'] == first


def test_cache_proof_does_not_write_durable_queue_rows(blocked):
    queue, source, _, _ = blocked
    tables = ('discovery', 'cnyes_index_revision_rounds', 'cnyes_index_revision_retired',
              'bulk_endpoint_cooldowns', 'queue_settings')
    before = {table: [tuple(row) for row in queue.db.execute('SELECT * FROM ' + table)] for table in tables}
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    assert {table: [tuple(row) for row in queue.db.execute('SELECT * FROM ' + table)] for table in tables} == before


def test_proof_is_bounded_even_when_no_deadline_is_near(blocked, monkeypatch):
    queue, source, first, _ = blocked
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=1000 WHERE url=?', (first,))
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 99)
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 159)
    assert traced_selection(queue, source)[1]


def test_proof_expires_on_monotonic_time_even_if_wall_clock_stalls(blocked, monkeypatch):
    queue, source, first, _ = blocked
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=1000 WHERE url=?', (first,))
    wall, monotonic = [99.0], [time.monotonic()]
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: wall[0])
    monkeypatch.setattr('downloader.news_fulltext_queue.time.monotonic', lambda: monotonic[0])
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    monotonic[0] += 59.5
    assert queue.next_wake_delay({}, maximum=60) == 0.5
    monotonic[0] += 0.5
    assert traced_selection(queue, source)[1]


def test_cache_not_used_inside_transaction_and_rollback_does_not_hide_work(blocked):
    queue, source, first, _ = blocked
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
    assert traced_selection(queue, source)[0][0]['url'] == first
    queue.db.rollback()
    assert traced_selection(queue, source)[0] == []
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
    assert traced_selection(queue, source)[0][0]['url'] == first


def test_source_scope_limits_and_invalid_origins_are_still_checked(blocked):
    queue, source, first, _ = blocked
    assert traced_selection(queue, source)[0] == []
    assert traced_selection(queue, source) == ([], [])
    with pytest.raises(NewsError, match='invalid_url'):
        queue.discovery_candidates(source, 17, excluded_origins=('bad-origin',))
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
    assert queue.discovery_candidates(source, 0) == []
    assert queue.discovery_candidates(source, 1, excluded_origins=('https://api.cnyes.com',)) == []
    assert queue.discovery_candidates(source, 1)[0]['url'] == first


def test_randomized_cache_selection_matches_canonical_sql(blocked):
    queue, source, first, second = blocked
    rng = random.Random(1402)
    for _ in range(50):
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (rng.choice([0, time.time()+900]), first))
            queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (rng.choice([0, time.time()+900]), second))
            queue.db.execute("UPDATE cnyes_index_revision_rounds SET phase=?", (rng.choice(['anchor', 'verify', 'bad']),))
        allowed = rng.choice([True, False])
        excluded = rng.choice([(), ('https://api.cnyes.com',)])
        sql, binds = queue.discovery_candidates_query(source, 17, bulk_allowed=allowed, excluded_origins=excluded)
        expected = [dict(row) for row in queue.db.execute(sql, binds)]
        for _ in range(2):
            actual = [dict(row) for row in queue.discovery_candidates(source, 17, bulk_allowed=allowed, excluded_origins=excluded)]
            assert actual == expected


def test_owner_continues_other_publisher_while_cnyes_revision_waits(cfg, config, tmp_path, monkeypatch):
    """Real executor, queue and durable body receipts with a fake publisher."""
    from downloader.news_fulltext import run_cycle
    from downloader.news_fulltext_queue import NewsQueue, verify_bodies

    config['providers'] = [deepcopy(cfg['providers'][0]), config['providers'][0]]
    config.update(workers=2, batch_articles=1)
    urls = [f'https://publisher.test/article/{n}' for n in range(8)]
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')}
    routes.update({url: FakeResponse(article_html()) for url in urls})
    http, session = http_fixture(config, tmp_path, routes)
    owner = NewsQueue(tmp_path, http.registry)
    source = http.registry.sources['cnyes']
    first, second = (request_url(source, DAY, page) for page in (1, 2))
    monkeypatch.setattr('downloader.news_fulltext.seed_discovery', lambda *_: None)
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *_: False)
    try:
        with owner.db:
            owner.add_articles([{'url': url} for url in urls])
            owner.db.executemany("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'index','cnyes',?)",
                                 [(first, time.time()+600), (second, 0)])
            owner.db.execute('INSERT INTO cnyes_index_revision_rounds VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (cnyes_scope(first), source.cnyes_index_data_fingerprint, source.authorization_fingerprint,
                 '1'*64, '2'*64, 'anchor', None, None, None, None, '2026-10-28T00:00:00+00:00'))
        result = run_cycle(owner, http, config, max_articles=len(urls), max_discovery_requests=1)
        assert result['cycle']['durably_accepted_versions'] == len(urls)
        assert verify_bodies(tmp_path)['accepted']
        assert all(owner.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'complete'
                   for url in urls)
        assert owner.metrics.snapshot()['providers']['cnyes']['counts']['discovery_revision_empty_cache_hits'] > 0
        assert all('cnyes' not in url for url, _ in session.calls)
    finally:
        http.close()
        owner.close()
