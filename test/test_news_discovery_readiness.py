"""Exact empty-frontier proof; no cached deadline, scope or priority changes."""
from contextlib import closing
from copy import deepcopy
from pathlib import Path
import json
import time

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import (
    DISCOVERY_DUE_LOOKAHEAD_ROWS, DISCOVERY_READINESS_PROBE_SQL, NewsQueue,
)
from scripts.benchmark_news_discovery_dispatch import benchmark
from test_news_fulltext import config
from test_news_finite_cohorts import finite


def add(queue, url, kind='sitemap', *, due=0, priority=100, superseded=None):
    queue.db.execute('INSERT INTO discovery(url,kind,provider,due,priority,superseded_by_windows_json) '
                     'VALUES (?,?,?,?,?,?)', (url, kind, 'publisher', due, priority, superseded))


def test_empty_due_frontier_skips_old_scan_not_validation(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['publisher']
        with queue.db:
            queue.db.executemany('INSERT INTO discovery(url,kind,provider,due) VALUES (?,\'sitemap\',\'publisher\',?)',
                [('https://publisher.test/map/' + str(i), time.time() + 900) for i in range(2048)])
            add(queue, 'https://publisher.test/superseded', superseded='[]')
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES ('https://other.test/map','sitemap','other')")
        before = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
        traced = []
        queue.db.set_trace_callback(traced.append)
        assert queue.discovery_candidates(source, 17) == []
        queue.db.set_trace_callback(None)
        assert len(traced) == 1 and 'discovery_due_frontier' in traced[0]
        assert queue.metrics.snapshot()['providers']['publisher']['counts']['discovery_idle_index_skips'] == 1
        assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
        plan = ' '.join(r[-1] for r in queue.db.execute('EXPLAIN QUERY PLAN ' + DISCOVERY_READINESS_PROBE_SQL,
                                                       ('publisher', time.time())))
        assert 'SEARCH discovery USING INDEX discovery_due_frontier' in plan
        assert 'TEMP B-TREE' not in plan and 'due<?' in plan
        with pytest.raises(NewsError, match='invalid_url'):
            queue.discovery_candidates(source, 1, excluded_origins=('not-an-origin',))


@pytest.mark.parametrize('bulk_allowed', [True, False])
@pytest.mark.parametrize('limit', [1, 3, 17])
@pytest.mark.parametrize('excluded', [(), ('https://publisher.test',)])
def test_ready_rows_keep_exact_old_order_scope_limit_and_deadlines(config, tmp_path, bulk_allowed, limit, excluded):
    config['providers'][0]['fulltext_feeds'] = ['https://publisher.test/full']
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['publisher']
        with queue.db:
            add(queue, 'https://publisher.test/map/z', priority=-20, due=-1)
            add(queue, 'https://publisher.test/map/a', priority=-20)
            add(queue, 'https://publisher.test/rss/z', 'feed', priority=1000)
            add(queue, 'https://publisher.test/rss/a', 'feed', priority=1000)
            add(queue, 'https://publisher.test/full?page=2', 'rss_fulltext', priority=-30)
            add(queue, 'https://publisher.test/unregistered?page=1', 'rss_fulltext', priority=-100)
            add(queue, 'https://publisher.test/future', 'feed', due=time.time() + 900, priority=-100)
            add(queue, 'http://publisher.test/map', priority=-40)
            add(queue, 'https://publisher.test/superseded', 'feed', superseded='[]', priority=-100)
        before = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
        sql, binds = queue.discovery_candidates_query(source, limit, bulk_allowed=bulk_allowed, excluded_origins=excluded)
        expected = [dict(r) for r in queue.db.execute(sql, binds)]
        assert [dict(r) for r in queue.discovery_candidates(source, limit, bulk_allowed=bulk_allowed,
                                                           excluded_origins=excluded)] == expected
        assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
        assert all(r['url'] != 'https://publisher.test/future' for r in expected)
        if not excluded:
            assert expected[0]['url'] == 'https://publisher.test/rss/a'
        assert all('/unregistered' not in r['url'] for r in expected)
        if not bulk_allowed:
            assert all(r['kind'] != 'rss_fulltext' for r in expected)


def test_probe_is_not_negative_cached_after_insert_or_deadline_update(config, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['publisher']
        assert queue.discovery_candidates(source, 1) == []
        with queue.db:
            add(queue, 'https://publisher.test/map', due=42)
        monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 41)
        assert queue.discovery_candidates(source, 1) == []
        monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 42)
        assert queue.discovery_candidates(source, 1)[0]['due'] == 42
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=1000')
        assert queue.discovery_candidates(source, 1) == []
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=-3')
        assert queue.discovery_candidates(source, 1)[0]['due'] == -3


def test_finite_scope_never_crashes_or_grants_stale_discovery_permission(config, tmp_path):
    finite(config)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            add(queue, 'https://publisher.test/old-rss', 'feed')
        source = queue.registry.sources['publisher']
        sql, binds = queue.discovery_candidates_query(source, 17)
        assert 'INDEXED BY discovery_ready_order' not in sql
        assert queue.discovery_candidates(source, 17) == []
        result = benchmark(tmp_path, queue.registry, ['publisher'], cycles=1)
        assert result['accepted'] and result['providers']['publisher']['selected_rows'] == 0


@pytest.mark.parametrize('count', [1, DISCOVERY_DUE_LOOKAHEAD_ROWS, DISCOVERY_DUE_LOOKAHEAD_ROWS + 1, 2048])
def test_sparse_and_dense_plans_match_full_reference_not_probe_subset(config, tmp_path, count):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.db.executemany('INSERT INTO discovery(url,kind,provider,due,priority) '
                'VALUES (?,\'sitemap\',\'publisher\',?,?)',
                [('https://publisher.test/map/' + str(i), i % 4, -i) for i in range(count)])
        source = queue.registry.sources['publisher']
        sql, binds = queue.discovery_candidates_query(source, 17)
        expected = [dict(r) for r in queue.db.execute(sql, binds)]
        traced = []
        queue.db.set_trace_callback(traced.append)
        observed = [dict(r) for r in queue.discovery_candidates(source, 17)]
        queue.db.set_trace_callback(None)
        assert observed == expected and len(traced) == 2
        selected_index = 'discovery_due_frontier' if count <= DISCOVERY_DUE_LOOKAHEAD_ROWS else 'discovery_ready_order'
        assert 'INDEXED BY ' + selected_index in traced[1]
        # The most important row is deliberately beyond the bounded probe's
        # due order. Dense mode must still run the full priority query.
        assert observed[0]['priority'] == -(count - 1)
        result = benchmark(tmp_path, queue.registry, ['publisher'], cycles=1)
        assert result['accepted'] and result['providers']['publisher']['exact_row_order_parity']


def test_due_cnyes_endpoint_circuit_is_not_a_permission_grant(config, tmp_path):
    entry = deepcopy(next(p for p in json.loads(Path('configs/news_fulltext.json').read_text())['providers'] if p['id'] == 'cnyes'))
    entry.pop('cnyes_endpoint_inventory', None)
    base = entry['cnyes_apis'][0]
    entry['cnyes_apis'] = [base]
    config['providers'] = [entry]
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['cnyes']
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (base + '?page=1',))
            queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
                ('cnyes', base, source.data_fingerprint, time.time() + 900, 'bulk_all_items_unusable', base))
        assert queue.db.execute(DISCOVERY_READINESS_PROBE_SQL, ('cnyes', time.time())).fetchone()
        assert queue.discovery_candidates(source, 17) == []
        with queue.db:
            queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=0')
        assert queue.discovery_candidates(source, 17)[0]['url'] == base + '?page=1'


def test_schema_upgrade_and_readonly_paired_benchmark_preserve_original_rows(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    with queue.db:
        add(queue, 'https://publisher.test/map', due=time.time() + 900)
        queue.db.execute('DROP INDEX discovery_due_frontier')
    before = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    queue.close()
    with closing(NewsQueue(tmp_path, registry)) as queue:
        assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
        changes = queue.db.total_changes
        result = benchmark(tmp_path, registry, ['publisher'], cycles=1)
        assert result['accepted'] and result['publisher_requests'] == 0
        assert not result['production_database_written'] and result['fixed_single_wal_snapshot']
        assert result['providers']['publisher']['raw_due_rows'] == 0
        assert len(result['providers']['publisher']['measurements']) == 4
        assert queue.db.total_changes == changes
        assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


@pytest.mark.parametrize('cycles', [0, 4, True, 1.2])
def test_benchmark_rejects_unbounded_cycles(config, tmp_path, cycles):
    with pytest.raises(ValueError, match='bounded_ab_ba_cycles_required'):
        benchmark(tmp_path, PolicyRegistry(config, base=tmp_path), ['publisher'], cycles=cycles)


def test_readonly_benchmark_never_installs_missing_index(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        queue.db.execute('DROP INDEX discovery_due_frontier')
        queue.db.commit()
        with pytest.raises(ValueError, match='canonical_discovery_due_frontier_index_required'):
            benchmark(tmp_path, registry, ['publisher'], cycles=1)
        assert queue.db.execute("SELECT 1 FROM sqlite_master WHERE name='discovery_due_frontier'").fetchone() is None
