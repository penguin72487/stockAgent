"""Local routing is unfinished work even after all Google envelopes decode."""
import time

import pytest

from downloader.news_fulltext import run_cycle
from downloader.news_fulltext_urls import (
    backfill_original_urls, has_original_url_work, original_url_progress, sync_original_url_routes,
)
from test_news_fulltext import config, http_fixture
from test_news_original_queue import alias, queue


def decoded(queue, count=6):
    with queue.db:
        queue.add_articles([alias(f'https://unreviewed.test/{i}') for i in range(count)])
    while backfill_original_urls(queue)['examined']:
        pass
    assert original_url_progress(queue)['remaining_urls'] == 0


def test_continuous_routing_finishes_without_publisher_future_or_service_cycle(config, queue, tmp_path):
    """Before the fix only the first route slice ran, then the cycle exited."""
    decoded(queue)
    config['original_url_resolution_batch'] = 1
    config['original_url_resolution_interval_seconds'] = .05
    http, session = http_fixture(config, tmp_path, {})
    deadline = time.time() + 60
    http.provider_not_before['publisher'] = deadline
    try:
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                           continuous_budgets=True, max_run_seconds=2)
        progress = result['original_url_resolution']
        assert progress['examined_urls'] == 6 and progress['remaining_urls'] == 0
        assert progress['routing_work']['completion_proven'] is True
        assert progress['routing_work']['completed_hosts'] == 1
        assert .15 <= result['cycle']['wall_seconds'] < 2
        assert result['cycle']['article_attempts_started'] == 0
        assert session.calls == [] and http.provider_not_before['publisher'] == deadline
    finally:
        http.close()


def test_noncontinuous_cycle_remains_bounded_without_running_local_backlog(config, queue, tmp_path):
    decoded(queue)
    http, session = http_fixture(config, tmp_path, {})
    try:
        result = run_cycle(queue, http, config, max_articles=0, max_discovery_requests=0)
        assert result['original_url_resolution']['routing_work']['completion_proven'] is False
        assert session.calls == []
    finally:
        http.close()


def finish_routing(queue):
    for _ in range(20):
        sync_original_url_routes(queue, limit=2)
        if not has_original_url_work(queue):
            return
    pytest.fail('local routing did not finish')


def test_scheduling_hint_separates_decoding_and_routing_without_scanning_articles_or_hosts(queue):
    with queue.db:
        queue.add_articles([alias('https://unreviewed.test/one')])
    assert has_original_url_work(queue) is True
    while backfill_original_urls(queue)['examined']:
        pass
    assert has_original_url_work(queue) is True  # No undecoded envelope remains.
    finish_routing(queue)
    statements = []
    queue.db.set_trace_callback(statements.append)
    assert has_original_url_work(queue) is False
    queue.db.set_trace_callback(None)
    assert len(statements) == 2
    assert all(s.startswith('SELECT ') for s in statements)
    assert not any('FROM articles' in s or 'original_url_routing_hosts' in s
                   or 'original_url_routing_inputs' in s for s in statements)


@pytest.mark.parametrize('key,value', [
    ('google_literal_url_routing_v1', None),
    ('google_literal_url_routing_v1', 'not-json'),
    ('google_literal_url_routing_v1', 'null'),
    ('google_literal_url_routing_v1', '{"policy_fingerprint":"legacy","after":"done"}'),
    ('google_literal_url_routing_v1', '{"contract_version":2,"routing_fingerprint":"' + 'c' * 64 + '","after":"done"}'),
    ('original_url_routing_new_hosts_v1', '1'),
    ('original_url_routing_new_hosts_v1', None),
    ('original_url_routing_geometry_v1', 'unknown'),
])
def test_unknown_or_dirty_owner_markers_keep_local_work_runnable_without_repair_writes(queue, key, value):
    decoded(queue, count=1)
    finish_routing(queue)
    with queue.db:
        if value is None:
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (key,))
        else:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, value))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings ORDER BY key')]
    changes = queue.db.total_changes
    assert has_original_url_work(queue) is True
    assert queue.db.total_changes == changes
    assert before == [tuple(row) for row in queue.db.execute('SELECT * FROM queue_settings ORDER BY key')]
