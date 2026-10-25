"""Keep complete maintenance parity independent of its physical query plan."""
import pytest

from downloader.news_fulltext import discovery_fingerprint, seed_discovery
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts.benchmark_news_discovery_maintenance import benchmark
from test_news_fulltext import config


def test_partial_parser_index_covers_archived_successes_but_preserves_refusals(config, tmp_path):
    config['providers'][0]['index_pages'] = ['https://publisher.test/latest']
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        source = q.registry.sources['publisher']
        rows = [('archived', 'old', None, '{}'), ('current', discovery_fingerprint(source), None, None),
                ('refused', 'old', 'discovery_http_403', None), ('unchecked', None, None, None)]
        with q.db:
            q.db.executemany("INSERT INTO discovery(url,kind,provider,due,attempts,parser_fingerprint,error_code,superseded_by_windows_json) "
                             "VALUES (?,'index','publisher',12345,4,?,?,?)",
                             [('https://publisher.test/'+name, fp, error, archived) for name, fp, error, archived in rows])
        plan = [row[3] for row in q.db.execute("EXPLAIN QUERY PLAN UPDATE discovery SET due=0 WHERE provider=? AND kind='index' "
                "AND error_code IS NULL AND parser_fingerprint IS NOT NULL AND parser_fingerprint!=?", ('publisher', 'new'))]
        assert any('SEARCH discovery USING INDEX discovery_parser_contract' in item for item in plan)
        seed_discovery(q)
        assert q.db.execute("SELECT due FROM discovery WHERE url='https://publisher.test/archived'").fetchone()[0] == 0
        for name, *_ in rows[1:]:
            assert q.db.execute('SELECT due,attempts FROM discovery WHERE url=?', ('https://publisher.test/'+name,)).fetchone()[:] == (12345, 4)
        assert q.metrics.snapshot()['stages']['discovery_seed_maintenance']['count'] == 1
    finally:
        q.close()


def test_full_maintenance_replay_uses_scratch_and_restores_every_input(config, tmp_path, monkeypatch):
    config['providers'][0]['index_pages'] = ['https://publisher.test/latest']
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        seed_discovery(q)
        with q.db:
            q.db.execute("UPDATE discovery SET due=12345,parser_fingerprint='stale',last_checked_at_utc='2026-10-08T00:00:00+00:00'")
        before = [tuple(row) for row in q.db.execute('SELECT * FROM discovery')]
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.get', lambda *a, **k: pytest.fail('benchmark issued HTTP'))
        result = benchmark(q.root, q.registry, repetitions=2, max_seconds=10)
        assert result['accepted'] and result['exact_complete_maintenance_results_equal']
        assert [r['method'] for r in result['samples']] == ['legacy', 'indexed', 'indexed', 'legacy']
        assert result['query_plans']['legacy'] == ['SCAN discovery']
        assert any('discovery_parser_contract' in plan for plan in result['query_plans']['indexed'])
        assert not result['source_states_mutated'] and not result['source_schema_mutated']
        assert [tuple(row) for row in q.db.execute('SELECT * FROM discovery')] == before
    finally:
        q.close()


@pytest.mark.parametrize('repetitions', [0, 4, True, 1.5])
def test_repetition_budget_checked_before_database_access(tmp_path, repetitions):
    with pytest.raises(ValueError, match='bounded_discovery_maintenance_benchmark_required'):
        benchmark(tmp_path, None, repetitions=repetitions)
