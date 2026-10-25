import json
import time
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import pytest

from downloader.news_fulltext import seed_discovery
from downloader.news_fulltext_bulk import cnyes_history_priority, cnyes_url
from downloader.news_fulltext_cnyes_seed import (
    PLANNED_QUERY, PLANNED_SCOPE_INDEX_NAME, PLANNED_SCOPE_INDEX_SQL, planned_intervals,
)
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts.benchmark_news_cnyes_scope_maintenance import benchmark, legacy_intervals
from scripts.benchmark_news_discovery_maintenance import benchmark as maintenance_benchmark
from test_cnyes_news_bulk import BASE, cfg


@pytest.fixture
def queue(cfg, tmp_path):
    cfg['providers'][0]['cnyes_history_partition_policy'] = 'adaptive_days_v1'
    value = NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))
    yield value
    value.close()


def test_planned_ranges_preserve_refusals_superseded_jobs_and_noncanonical_queries(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388620799)
    parts = urlsplit(url)
    variants = [cnyes_url(BASE, page, start=1388534400, end=1388620799) for page in range(1, 31)]
    # Never discard earlier duplicate values or unusual spelling when reducing
    # pagination. Non-generated prefixes retain the old query parser.
    variants += [urlunsplit((*parts[:3], query, '')) for query in (
        'limit=30&page=1&endAt=1389000000&startAt=1388620800&endAt=1389200000',
        'limit=30&page=01&startAt=1388620800&endAt=1388707199',
        'page=2&limit=30&startAt=1388707200&endAt=1388793599',
        'limit=30&page=3&startAt=1388793600&endAt=1388879999&endAt=1389999999',
        'limit=30&page=4&startAt=%31%33%38%38%38%38%30%30%30%30&endAt=1388966399',
        'limit=30&page=5&startAt=-10&endAt=-1',
        'limit=30&page=6&startAt=0&endAt=0&token=retained',
    )]
    with queue.db:
        queue.db.executemany("INSERT INTO discovery(url,kind,provider,due,error_code,superseded_by_windows_json) VALUES (?,'cnyes_news','cnyes',999,'discovery_http_403','{}')", [(u,) for u in variants])
    before = queue.db.total_changes
    stats = {}
    assert planned_intervals(queue.db, BASE, statistics=stats) == legacy_intervals(queue.db, BASE, {})
    assert stats['queries_parsed'] == 8
    assert queue.db.total_changes == before


def test_scope_reader_seeks_exact_endpoint_not_neighbor_category(queue):
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (cnyes_url(BASE + '_other', start=1, end=2),))
    assert planned_intervals(queue.db, BASE) == []
    prefix = BASE + '?limit=30&page='
    plan = [r[3] for r in queue.db.execute('EXPLAIN QUERY PLAN ' + PLANNED_QUERY,
            {'prefix': prefix, 'prefix_length': len(prefix), 'lower': BASE + '?', 'upper': BASE + '@'})]
    assert any('SEARCH discovery' in p for p in plan)


def test_covering_scope_index_preserves_archived_refused_and_unusual_ranges(queue):
    variants = [cnyes_url(BASE, page, start=1, end=2) for page in (1, 2, 3)]
    variants += [BASE + '?page=01&limit=30&startAt=3&endAt=4&endAt=5']
    with queue.db:
        queue.db.executemany("INSERT INTO discovery(url,kind,provider,due,attempts,error_code,"
                             "superseded_by_windows_json) VALUES (?,'cnyes_news','cnyes',12345,4,"
                             "'discovery_http_403','{}')", [(url,) for url in variants])
        queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'index','cnyes')",
                         (BASE + '?startAt=100&endAt=200',))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    expected = legacy_intervals(queue.db, BASE, {})
    with queue.db:
        queue.db.execute(PLANNED_SCOPE_INDEX_SQL)
        queue.db.execute(PLANNED_SCOPE_INDEX_SQL)  # Reopening an owner is idempotent.
    prefix = BASE + '?limit=30&page='
    plan = [row[3] for row in queue.db.execute('EXPLAIN QUERY PLAN ' + PLANNED_QUERY,
            {'prefix': prefix, 'prefix_length': len(prefix), 'lower': BASE + '?', 'upper': BASE + '@'})]
    assert any('COVERING INDEX ' + PLANNED_SCOPE_INDEX_NAME in item for item in plan)
    assert planned_intervals(queue.db, BASE) == expected == [(1, 2), (3, 4)]
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_covering_scope_benchmark_compares_complete_maintenance_without_source_writes(queue, monkeypatch):
    seed_discovery(queue)
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due,attempts,error_code,"
                         "superseded_by_windows_json) VALUES (?,'cnyes_news','cnyes',12345,4,"
                         "'discovery_http_403','{}')", (cnyes_url(BASE, 2, start=1, end=2),))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.get',
                        lambda *a, **k: pytest.fail('benchmark issued HTTP'))
    result = maintenance_benchmark(queue.root, queue.registry, repetitions=2, max_seconds=10,
                                   index_axis='cnyes_planned_scope')
    assert result['accepted'] and result['exact_complete_maintenance_results_equal']
    assert [row['method'] for row in result['samples']] == ['legacy', 'indexed', 'indexed', 'legacy']
    assert not any(PLANNED_SCOPE_INDEX_NAME in item for item in result['query_plans']['legacy'])
    assert any('COVERING INDEX ' + PLANNED_SCOPE_INDEX_NAME in item
               for item in result['query_plans']['indexed'])
    assert not result['source_states_mutated'] and not result['source_schema_mutated']
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_priority_migration_is_once_per_contract_and_keeps_retry_state(queue):
    url = cnyes_url(BASE, 2, start=1388534400, end=1388620799)
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,priority) VALUES (?,'cnyes_news','cnyes',999,'discovery_http_403',100)", (url,))
    seed_discovery(queue)
    row = queue.db.execute('SELECT due,error_code,priority FROM discovery WHERE url=?', (url,)).fetchone()
    assert row[:] == (999, 'discovery_http_403', cnyes_history_priority(url))
    before = queue.db.total_changes
    # Exercise the next owner/day path, not the per-owner memory shortcut.
    queue._cnyes_seed_signatures = {}
    stages = queue.metrics.snapshot()['stages']['cnyes_seed_priority_migration']['count']
    seed_discovery(queue)
    assert queue.db.total_changes == before
    assert queue.metrics.snapshot()['stages']['cnyes_seed_priority_migration']['count'] == stages
    with queue.db:
        queue.db.execute("UPDATE queue_settings SET value='superseded-contract' WHERE key='cnyes_seed_priority_contract:cnyes'")
    queue._cnyes_seed_signatures = {}
    seed_discovery(queue)
    assert queue.metrics.snapshot()['stages']['cnyes_seed_priority_migration']['count'] > stages


def test_native_benchmark_is_readonly_and_compares_identical_scope_and_new_roots(queue, monkeypatch):
    seed_discovery(queue)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.get', lambda *a, **k: pytest.fail('benchmark issued HTTP'))
    before = queue.db.total_changes
    result = benchmark(queue.root, queue.registry.sources['cnyes'])
    assert result['accepted'] and result['exact_planned_ranges_and_new_roots_equal']
    assert len(result['samples']) == 4
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert not result['history_completeness_verified']
    assert queue.db.total_changes == before


@pytest.mark.parametrize('value', [0, 5, True, 1.5])
def test_benchmark_limits_are_validated_before_database_access(tmp_path, value):
    with pytest.raises(ValueError):
        benchmark(tmp_path, None, repetitions=value)
