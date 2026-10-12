"""A small exact-state index, not configured-provider or gauge shortcuts."""
import time

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, ABANDONED_WORK_RECOVERY_SQL
from scripts.benchmark_news_abandoned_recovery import benchmark
from test_news_fulltext import config


def test_restart_finds_every_actual_inflight_row_without_scanning_history(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    urls = ['https://publisher.test/news/' + str(i) for i in range(12)]
    queue.add_articles([{'url': url} for url in urls])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='inflight',lease_owner='old-owner',lease_until=?,"
                         "due=?,attempts=7,error_code='old-error' WHERE url IN (?,?,?)",
                         (time.time() + 9999, time.time() + 9999, *urls[:3]))
        # Recovery must not depend on registration or a trusted metrics gauge.
        queue.db.execute("UPDATE articles SET provider='not-configured' WHERE url=?", (urls[2],))
        queue.db.execute("UPDATE articles SET state='retry',due=?,error_code='http_429' WHERE url=?",
                         (time.time() + 9999, urls[3]))
    before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    queue.close()
    queue = NewsQueue(tmp_path, registry)
    try:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        changed = {'state', 'due', 'lease_owner', 'lease_until', 'error_code'}
        for url in urls[:3]:
            assert after[url]['state'] == 'retry'
            assert after[url]['due'] == 0 and after[url]['error_code'] == 'owner_restart'
            assert after[url]['lease_owner'] is None and after[url]['lease_until'] is None
            assert {k: v for k, v in after[url].items() if k not in changed} == {
                k: v for k, v in before[url].items() if k not in changed}
        for url in urls[3:]:
            assert after[url] == before[url]
        plan = ' '.join(r[-1] for r in queue.db.execute('EXPLAIN QUERY PLAN ' + ABANDONED_WORK_RECOVERY_SQL))
        assert 'abandoned_work' in plan
        assert queue.db.execute("SELECT count(*) FROM articles INDEXED BY abandoned_work WHERE state='inflight'").fetchone()[0] == 0
        actual = [tuple(r) for r in queue.db.execute('SELECT provider,state,count(*) FROM articles GROUP BY provider,state')]
        metrics = [tuple(r) for r in queue.db.execute('SELECT provider,state,articles FROM state_metrics WHERE articles>0 ORDER BY provider,state')]
        assert actual == metrics
    finally:
        queue.close()


def test_index_upgrade_and_readonly_benchmark_keep_raw_heads_and_source_deadlines(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    queue.add_articles([{'url': 'https://publisher.test/news/' + str(i)} for i in range(140)])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='complete',body_sha256='raw-sha',body_bytes=123,error_code=NULL")
        queue.db.execute('DROP INDEX abandoned_work')
    before = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')] == before
        changes = queue.db.total_changes
        result = benchmark(tmp_path, cycles=1)
        assert result['accepted'] and result['fixed_single_wal_snapshot']
        assert result['inflight_rows'] == 0 and result['state_summary_articles'] == 140
        assert result['publisher_requests'] == 0 and not result['production_database_written']
        assert not result['all_history_complete'] and queue.db.total_changes == changes
        assert len(result['results']) == 4
    finally:
        queue.close()


@pytest.mark.parametrize('cycles', [0, 4, True, 1.1])
def test_benchmark_rejects_unbounded_or_untyped_cycles(tmp_path, cycles):
    with pytest.raises(ValueError, match='bounded_ab_ba_cycles_required'):
        benchmark(tmp_path, cycles=cycles)


def test_readonly_benchmark_does_not_install_missing_production_index(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    queue.db.execute('DROP INDEX abandoned_work')
    queue.db.commit()
    try:
        with pytest.raises(ValueError, match='canonical_abandoned_work_index_required'):
            benchmark(tmp_path, cycles=1)
        assert queue.db.execute("SELECT 1 FROM sqlite_master WHERE name='abandoned_work'").fetchone() is None
    finally:
        queue.close()
