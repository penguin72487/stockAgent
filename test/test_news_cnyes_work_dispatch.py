import json
import threading
import time

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_cnyes_work import backfill, canonical_candidate_id, statement
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as config


@pytest.fixture
def queue(config, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    yield value
    value.close()


def seed(queue, *, count=16):
    with queue.db:
        queue.add_articles([{'url': f'https://news.cnyes.com/news/id/{i}', 'date': '2014-01-01'} for i in range(count)])
        queue.add_articles([{'url': f'https://news.cnyes.com/news/id/{100+i}', 'date': '2026-10-09'} for i in range(4)])
        queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url LIKE 'https://news.cnyes.com/news/id/10_'",
                         ('a' * 64,))
        queue.add_articles([{'url': f'https://m.cnyes.com/news/id/{100+i}', 'date': '2026-10-09'} for i in range(4)])


def test_missing_originals_first_and_candidate_views_keep_bounded_service(queue):
    seed(queue)
    rows = [queue.claim('cnyes', 300) for _ in range(20)]
    known = [r['cnyes_known_body_candidate'] for r in rows]
    assert known == [0] * 8 + [1] + [0] * 8 + [1, 1, 1]
    assert rows[0]['metadata_order_day'] == '2014-01-01'
    assert rows[8]['metadata_order_day'] == '2026-10-09'
    counts = queue.metrics.snapshot()['providers']['cnyes']['counts']
    assert counts['frontier_no_known_body_candidate_dispatches'] == 16
    assert counts['frontier_existing_body_candidate_dispatches'] == 4
    assert not any(r['body_sha256'] for r in rows)


def test_candidate_trigger_tracks_new_body_without_changing_mobile_observations(queue):
    with queue.db:
        queue.add_articles([{'url': 'https://m.cnyes.com/news/id/1', 'title': 'old mobile title'},
                            {'url': 'https://news.cnyes.com/news/id/1'}])
        row = queue.db.execute("SELECT * FROM articles WHERE host='m.cnyes.com'").fetchone()
        assert row['cnyes_known_body_candidate'] == 0
        queue.db.execute("UPDATE articles SET body_sha256=?,state='complete' WHERE host='news.cnyes.com'", ('a' * 64,))
        current = queue.db.execute('SELECT * FROM articles WHERE id=?', (row['id'],)).fetchone()
        assert current['cnyes_known_body_candidate'] == 1
        assert all(current[k] == row[k] for k in row.keys() if k != 'cnyes_known_body_candidate')
        queue.db.execute("UPDATE articles SET body_sha256=NULL WHERE host='news.cnyes.com'")
        assert queue.db.execute('SELECT cnyes_known_body_candidate FROM articles WHERE id=?', (row['id'],)).fetchone()[0] == 0


def test_future_retries_refusals_and_other_worker_leases_are_never_reset(queue):
    seed(queue)
    deadline = time.time() + 3600
    with queue.db:
        queue.db.execute("UPDATE articles SET due=?,state='retry_cooldown',error_code='http_403' WHERE host='m.cnyes.com'", (deadline,))
        queue.db.execute("UPDATE articles SET state='inflight',lease_owner='other',lease_until=? WHERE url LIKE '%/0'", (deadline,))
        queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403' WHERE url LIKE '%/1'")
    for _ in range(14):
        assert queue.claim('cnyes', 300)['host'] != 'm.cnyes.com'
    assert queue.claim('cnyes', 300) is None
    assert queue.db.execute("SELECT due FROM articles WHERE host='m.cnyes.com' LIMIT 1").fetchone()[0] == deadline
    assert queue.db.execute("SELECT lease_owner FROM articles WHERE url LIKE '%/0'").fetchone()[0] == 'other'


@pytest.mark.parametrize('url', ['https://m.cnyes.com/news/id/1?exp=c', 'https://m.cnyes.com/news/id/1?exp=%61',
                               'https://m.cnyes.com/news/id/1?exp=a&token=x', 'https://m.cnyes.com/news/id/1#x',
                               'http://m.cnyes.com/news/id/1', 'https://m.cnyes.com.evil.test/news/id/1'])
def test_unproven_url_views_remain_unclassified(url):
    assert canonical_candidate_id(url) is None


@pytest.mark.parametrize('url', ['https://news.cnyes.com/news/id/1?exp=b',
                               'https://news.cnyes.com/news/id/1?exp=%61',
                               'https://news.cnyes.com/news/id/1?exp=a&token=x',
                               'https://news.cnyes.com/news/id/1?exp=a#x',
                               'http://news.cnyes.com/news/id/1?exp=a',
                               'https://news.cnyes.com.evil.test/news/id/1?exp=a',
                               'https://news.cnyes.com/news/id/1'])
def test_desktop_primary_and_unobserved_views_are_not_alias_candidates(url):
    assert canonical_candidate_id(url) is None


def test_desktop_alias_import_and_backfill_prioritize_missing_bodies_without_acceptance(queue):
    from downloader.news_fulltext_cnyes_work import DESKTOP_CURSOR
    primary = 'https://news.cnyes.com/news/id/1'
    variant = primary + '?exp=a'
    with queue.db:
        queue.add_articles([{'url': primary}])
        queue.db.execute('UPDATE articles SET state=\'complete\',body_sha256=? WHERE url=?', ('a' * 64, primary))
        queue.add_articles([{'url': variant, 'date': '2026-10-11', 'title': 'retained raw title'},
                            {'url': 'https://news.cnyes.com/news/id/2', 'date': '2014-01-01'}])
        row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())
        assert row['cnyes_known_body_candidate'] == 1
        assert row['cnyes_canonical_candidate_id'] == canonical_candidate_id(variant)
        queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=NULL,cnyes_known_body_candidate=0 WHERE url=?', (variant,))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())
    result = backfill(queue, limit=1)
    assert result['checked_heads'] == result['existing_body_candidates'] == 1
    assert result['native_lookup_candidates_added'] == result['publisher_requests'] == 0
    after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())
    assert all(after[k] == before[k] for k in before if not k.startswith('cnyes_'))
    assert after['state'] == 'pending' and after['body_sha256'] is None
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_candidates').fetchone()[0] == 0
    assert queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (DESKTOP_CURSOR,)).fetchone()
    from scripts.audit_news_cnyes_work_dispatch import desktop_inventory
    audit = desktop_inventory(queue.db)
    assert audit['supported_variant_urls'] == audit['matching_candidate_ids'] == audit['available_primary_body_hints'] == 1
    assert audit['classification_complete_for_current_frontier'] and not audit['classification_is_source_acceptance']
    assert audit['availability_hint_mismatches'] == 0
    assert queue.claim('cnyes', 300)['url'].endswith('/2')
    assert queue.claim('cnyes', 300)['url'] == variant


def assert_work_metrics(queue):
    from downloader.news_fulltext_cnyes_work import progress
    actual = [dict(row) for row in queue.db.execute("SELECT state,cnyes_known_body_candidate AS candidate,count(*) AS urls "
        "FROM articles WHERE provider='cnyes' AND body_sha256 IS NULL "
        "AND state IN ('pending','retry','retry_cooldown','inflight') "
        "GROUP BY state,cnyes_known_body_candidate ORDER BY state,candidate")]
    result = progress(queue.db)
    assert result['states'] == actual
    assert result['active_missing_body_urls'] == sum(row['urls'] for row in actual)
    assert result['active_missing_body_urls'] == result['existing_primary_body_candidates'] + result['no_known_primary_body_urls']
    assert not result['candidate_views_accepted_by_hint'] and not result['candidate_body_equivalence_verified']
    return result


def test_work_metrics_follow_primary_changes_claim_retry_acceptance_and_rollback(queue):
    with queue.db:
        queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'},
                            {'url': 'https://news.cnyes.com/news/id/1?exp=a'},
                            {'url': 'https://m.cnyes.com/news/id/1'},
                            {'url': 'https://news.cnyes.com/news/id/2'}])
    assert assert_work_metrics(queue)['no_known_primary_body_urls'] == 4
    row = queue.claim('cnyes', 300)
    assert_work_metrics(queue)
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry_cooldown',due=?,error_code='http_403' WHERE id=?",
                         (time.time() + 3600, row['id']))
    assert_work_metrics(queue)
    with queue.db:
        queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url='https://news.cnyes.com/news/id/1'", ('a' * 64,))
    before = assert_work_metrics(queue)
    assert before['existing_primary_body_candidates'] == 2
    with pytest.raises(RuntimeError), queue.db:
        queue.db.execute("DELETE FROM articles WHERE provider='cnyes'")
        assert_work_metrics(queue)
        raise RuntimeError('rollback all rows and trigger changes')
    assert assert_work_metrics(queue) == before
    with queue.db:
        queue.db.execute("UPDATE articles SET body_sha256=NULL WHERE url='https://news.cnyes.com/news/id/1'")
        queue.db.execute("UPDATE articles SET state='body_not_found' WHERE url='https://m.cnyes.com/news/id/1'")
    assert assert_work_metrics(queue)['existing_primary_body_candidates'] == 0
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry',body_sha256=? WHERE url LIKE '%?exp=a'", ('b' * 64,))
        queue.db.execute("DELETE FROM articles WHERE url='https://news.cnyes.com/news/id/2'")
        queue.db.execute("UPDATE articles SET provider='unregistered:other.test' WHERE url='https://news.cnyes.com/news/id/1'")
    assert_work_metrics(queue)


def test_progress_migration_restart_and_heartbeat_are_constant_table_reads(queue, tmp_path):
    from downloader.news_fulltext_cnyes_work import initialize_progress, progress
    seed(queue)
    queue.claim('cnyes', 300)
    expected = assert_work_metrics(queue)
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_body_work_metrics')
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_body_work_metrics_version'")
    initialize_progress(queue)
    assert assert_work_metrics(queue) == expected
    before = queue.db.total_changes
    progress(queue.db)
    initialize_progress(queue)
    assert queue.db.total_changes == before
    queries = []
    queue.db.set_trace_callback(queries.append)
    progress(queue.db)
    queue.db.set_trace_callback(None)
    assert len(queries) == 1 and 'FROM cnyes_body_work_metrics' in queries[0]
    queue.close()
    reopened = NewsQueue(tmp_path, queue.registry)
    try:
        assert_work_metrics(reopened)
        current = progress(reopened.db)
        assert current['active_missing_body_urls'] == expected['active_missing_body_urls']
        assert reopened.db.execute("SELECT count(*) FROM articles WHERE state='inflight'").fetchone()[0] == 0
        assert reopened.status(activity='running')['providers']['cnyes']['body_work_progress'] == current
    finally:
        reopened.close()


def test_bounded_backfill_is_indexed_resumable_and_cancellable(queue, tmp_path):
    seed(queue)
    with queue.db:
        queue.db.execute("UPDATE articles SET cnyes_canonical_candidate_id=NULL,cnyes_known_body_candidate=0 WHERE host='m.cnyes.com'")
    before = [dict(r) for r in queue.db.execute("SELECT * FROM articles WHERE host='m.cnyes.com' ORDER BY id")]
    first = backfill(queue, limit=2)
    assert first['checked_heads'] == first['existing_body_candidates'] == 2
    stopped = threading.Event()
    stopped.set()
    assert backfill(queue, stop=stopped)['checked_heads'] == 0
    assert backfill(queue, limit=2)['checked_heads'] == 2
    assert backfill(queue)['checked_heads'] == 0
    after = [dict(r) for r in queue.db.execute("SELECT * FROM articles WHERE host='m.cnyes.com' ORDER BY id")]
    for left, right in zip(before, after):
        assert all(left[k] == right[k] for k in left if not k.startswith('cnyes_'))
    saved = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key='cnyes_body_candidate_cursor_v1'").fetchone()[0])
    queue.close()
    reopened = NewsQueue(tmp_path, queue.registry)
    try:
        assert backfill(reopened)['checked_heads'] == 0
        assert json.loads(reopened.db.execute("SELECT value FROM queue_settings WHERE key='cnyes_body_candidate_cursor_v1'").fetchone()[0]) == saved
    finally:
        reopened.close()


def test_exhausted_and_stopped_backfill_do_not_write_a_checkpoint(queue):
    from downloader.news_fulltext_cnyes_work import maintenance_delay
    seed(queue)
    backfill(queue)
    before = queue.db.total_changes
    assert maintenance_delay(backfill(queue)) == 5
    stopped = threading.Event()
    stopped.set()
    assert backfill(queue, stop=stopped)['checked_heads'] == 0
    assert queue.db.total_changes == before
    assert maintenance_delay({'checked_heads': 1}) == 0.25


def test_idle_backfill_does_not_delay_new_import_or_changed_state_claim(queue):
    seed(queue)
    backfill(queue)
    assert backfill(queue)['checked_heads'] == 0
    with queue.db:
        queue.add_articles([{'url': 'https://news.cnyes.com/news/id/999', 'date': '2026-10-10'}])
        queue.db.execute("UPDATE articles SET body_sha256=?,state='complete' WHERE url LIKE '%/999'", ('a' * 64,))
        queue.add_articles([{'url': 'https://m.cnyes.com/news/id/999', 'date': '2026-10-10'}])
        row = queue.db.execute("SELECT cnyes_known_body_candidate FROM articles WHERE url='https://m.cnyes.com/news/id/999'").fetchone()
        assert row[0] == 1
        queue.db.execute("UPDATE articles SET state='pending',cnyes_canonical_candidate_id=NULL,cnyes_known_body_candidate=0 WHERE url='https://m.cnyes.com/news/id/999'")
    claimed = queue.claim('cnyes', 300)
    assert claimed and claimed['cnyes_known_body_candidate'] == 0
    assert queue.db.execute("SELECT cnyes_known_body_candidate FROM articles WHERE url='https://m.cnyes.com/news/id/999'").fetchone()[0] == 1


@pytest.mark.parametrize('known', [0, 1])
def test_value_lane_uses_partial_index_without_full_scan_or_sort(queue, known):
    details = [r[3] for r in queue.db.execute('EXPLAIN QUERY PLAN ' + statement(), ('cnyes', 'pending', time.time(), known))]
    assert any('SEARCH articles USING INDEX dispatch_cnyes_body_value' in r for r in details)
    assert not any('TEMP B-TREE' in r or 'SCAN articles' in r for r in details)


@pytest.mark.parametrize('value', [0, 129, True, '8', 1.5])
def test_invalid_fairness_configuration_rejected(config, tmp_path, value):
    config['cnyes_primary_body_burst'] = value
    target = tmp_path / 'config.json'
    target.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        load_config(target, repo=tmp_path)


def test_native_dispatch_auditor_is_readonly_and_exposes_candidates_not_unique_stories(queue, monkeypatch):
    from scripts.audit_news_cnyes_work_dispatch import audit
    seed(queue)
    queue.claim('cnyes', 300)
    queue.status(activity='running')
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.get',
                        lambda *a, **k: pytest.fail('readonly audit attempted HTTP'))
    before = queue.db.total_changes
    result = audit(queue.root, repetitions=2)
    assert result['accepted'] and result['owner_status_coherent']
    assert result['owner_html_dispatches']['no_known_body_candidate'] == 1
    assert not result['unique_story_inventory_verified'] and not result['candidate_body_equivalence_verified']
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert queue.db.total_changes == before
    assert len(result['lookup_measurements']) == 6


@pytest.mark.parametrize('value', [0, 257, True, 1.5])
def test_dispatch_auditor_work_is_bounded(tmp_path, value):
    from scripts.audit_news_cnyes_work_dispatch import audit
    with pytest.raises(ValueError):
        audit(tmp_path, repetitions=value)
