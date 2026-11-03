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


def test_repeated_candidate_classification_does_not_rewrite_unchanged_indexes(queue):
    from downloader.news_fulltext_cnyes_work import classify

    mobile = 'https://m.cnyes.com/news/id/123'
    primary = 'https://news.cnyes.com/news/id/123'
    with queue.db:
        queue.add_articles([{'url': mobile}, {'url': primary}])
        row = queue.db.execute('SELECT id,url,provider FROM articles WHERE url=?', (mobile,)).fetchone()
        before = queue.db.total_changes
        assert not classify(queue, row)
        assert queue.db.total_changes == before

        # The existing body trigger changes the hint, but does not turn this
        # mobile view into an accepted body or a proven equivalent alias.
        queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a' * 64, primary))
        before = queue.db.total_changes
        assert classify(queue, row)
        assert queue.db.total_changes == before
        assert queue.db.execute('SELECT body_sha256 FROM articles WHERE url=?', (mobile,)).fetchone()[0] is None

        # An actually stale classification must still be repaired.
        queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=NULL,cnyes_known_body_candidate=0 WHERE url=?', (mobile,))
        before = queue.db.total_changes
        assert classify(queue, row)
        assert queue.db.total_changes > before
        restored = queue.db.execute('SELECT cnyes_canonical_candidate_id,cnyes_known_body_candidate FROM articles WHERE url=?', (mobile,)).fetchone()
        assert restored[0] == canonical_candidate_id(mobile) and restored[1] == 1


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


def test_primary_provider_change_updates_hints_and_metrics_transactionally(queue):
    primary = 'https://news.cnyes.com/news/id/1'
    variants = ('https://m.cnyes.com/news/id/1', primary + '?exp=a')
    with queue.db:
        queue.add_articles([{'url': primary}, *({'url': url, 'title': 'retained title'} for url in variants)])
        queue.db.execute('UPDATE articles SET state=\'complete\',body_sha256=? WHERE url=?', ('a' * 64, primary))
        queue.db.execute("UPDATE articles SET state='retry_cooldown',due=?,error_code='http_403' WHERE url=?",
                         (time.time() + 3600, variants[0]))
    before = [dict(r) for r in queue.db.execute('SELECT * FROM articles WHERE url IN (?,?) ORDER BY url', variants)]
    assert [r['cnyes_known_body_candidate'] for r in before] == [1, 1]
    metrics = assert_work_metrics(queue)
    with pytest.raises(RuntimeError), queue.db:
        queue.db.execute('UPDATE articles SET provider=? WHERE url=?', ('unregistered:changed', primary))
        assert assert_work_metrics(queue)['existing_primary_body_candidates'] == 0
        raise RuntimeError('rollback primary reassignment and hints')
    assert assert_work_metrics(queue) == metrics
    with queue.db:
        queue.db.execute('UPDATE articles SET provider=? WHERE url=?', ('unregistered:changed', primary))
    after = [dict(r) for r in queue.db.execute('SELECT * FROM articles WHERE url IN (?,?) ORDER BY url', variants)]
    assert [r['cnyes_known_body_candidate'] for r in after] == [0, 0]
    assert all(all(old[key] == new[key] for key in old if key != 'cnyes_known_body_candidate')
               for old, new in zip(before, after))
    assert assert_work_metrics(queue)['no_known_primary_body_urls'] == 2
    with queue.db:
        queue.db.execute('UPDATE articles SET provider=? WHERE url=?', ('cnyes', primary))
    assert assert_work_metrics(queue)['existing_primary_body_candidates'] == 2


@pytest.mark.parametrize('old_provider,old_body,new_provider,new_body,expected', [
    ('unregistered:changed', None, 'cnyes', 'a' * 64, 1),
    ('cnyes', 'a' * 64, 'unregistered:changed', None, 0),
    ('unregistered:changed', 'a' * 64, 'cnyes', None, 0),
    ('cnyes', None, 'unregistered:changed', 'a' * 64, 0),
])
def test_provider_and_body_change_together_have_current_hint(queue, old_provider, old_body,
                                                            new_provider, new_body, expected):
    primary = 'https://news.cnyes.com/news/id/1'
    mobile = 'https://m.cnyes.com/news/id/1'
    with queue.db:
        queue.add_articles([{'url': primary}, {'url': mobile}])
        queue.db.execute("UPDATE articles SET state='complete',provider=?,body_sha256=? WHERE url=?",
                         (old_provider, old_body, primary))
        queue.db.execute('UPDATE articles SET provider=?,body_sha256=? WHERE url=?',
                         (new_provider, new_body, primary))
    row = queue.db.execute('SELECT body_sha256,cnyes_known_body_candidate FROM articles WHERE url=?', (mobile,)).fetchone()
    assert row['body_sha256'] is None and row['cnyes_known_body_candidate'] == expected
    assert assert_work_metrics(queue)['existing_primary_body_candidates'] == expected


@pytest.mark.parametrize('stale_positive', [False, True])
@pytest.mark.parametrize('through_owner', [False, True])
def test_dispatch_heals_preexisting_hint_before_selecting_a_lane(queue, stale_positive, through_owner):
    from downloader.news_fulltext_cnyes_work import candidate
    primary = 'https://news.cnyes.com/news/id/1'
    variant = 'https://m.cnyes.com/news/id/1'
    alternative_primary = 'https://news.cnyes.com/news/id/2'
    alternative = 'https://m.cnyes.com/news/id/2' if stale_positive else alternative_primary
    with queue.db:
        queue.add_articles([{'url': primary}, {'url': variant, 'date': '2026-10-20'},
                            {'url': alternative_primary, 'date': '2014-01-01'}])
        if stale_positive:
            queue.db.execute('UPDATE articles SET state=\'complete\',body_sha256=? WHERE url=?',
                             ('b' * 64, alternative_primary))
            queue.add_articles([{'url': alternative, 'date': '2014-01-01'}])
        else:
            queue.db.execute('UPDATE articles SET state=\'complete\',body_sha256=? WHERE url=?', ('a' * 64, primary))
        # Reproduce a hint left by older code without rewriting observations.
        queue.db.execute('UPDATE articles SET cnyes_known_body_candidate=? WHERE url=?', (int(stale_positive), variant))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())
        if through_owner:
            queue._cnyes_primary_body_burst = 8 if stale_positive else 0
            selected = queue.claim('cnyes', 300)
        else:
            selected = candidate(queue, 'pending', time.time(), prefer_known=stale_positive)
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())
    assert selected['url'] == alternative
    assert after['cnyes_known_body_candidate'] == int(not stale_positive)
    assert all(before[key] == after[key] for key in before if key != 'cnyes_known_body_candidate')
    assert_work_metrics(queue)


def test_owner_repairs_large_stale_frontier_in_bounded_slices_without_stalling(queue):
    with queue.db:
        for number in range(1, 13):
            primary = f'https://news.cnyes.com/news/id/{number}'
            variant = f'https://m.cnyes.com/news/id/{number}'
            queue.add_articles([{'url': primary}, {'url': variant, 'date': '2026-10-20'}])
            queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url=?", ('a' * 64, primary))
            queue.db.execute('UPDATE articles SET cnyes_known_body_candidate=0 WHERE url=?', (variant,))
        missing = 'https://news.cnyes.com/news/id/99'
        queue.add_articles([{'url': missing, 'date': '2014-01-01'}])
    first = queue.claim('cnyes', 300)
    assert first and first['host'] == 'm.cnyes.com'
    assert queue.db.execute("SELECT count(*) FROM articles WHERE host='m.cnyes.com' AND cnyes_known_body_candidate=1").fetchone()[0] == 8
    assert queue.claim('cnyes', 300)['url'] == missing
    assert_work_metrics(queue)


@pytest.mark.parametrize('primary_has_body', [False, True])
def test_unchanged_variant_hint_recheck_uses_index_and_does_not_write(queue, primary_has_body):
    from downloader.news_fulltext_cnyes_work import candidate
    primary = 'https://news.cnyes.com/news/id/1'
    variant = 'https://m.cnyes.com/news/id/1'
    with queue.db:
        queue.add_articles([{'url': primary}, {'url': variant}])
        queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url=?",
                         ('a' * 64 if primary_has_body else None, primary))
        before = queue.db.total_changes
        queries = []
        queue.db.set_trace_callback(queries.append)
        selected = candidate(queue, 'pending', time.time(), prefer_known=primary_has_body)
        queue.db.set_trace_callback(None)
        assert selected['url'] == variant
        assert queue.db.total_changes == before
        assert not any(query.lstrip().upper().startswith('UPDATE ARTICLES') for query in queries)
        plan = [row['detail'] for row in queue.db.execute(
            "EXPLAIN QUERY PLAN SELECT body_sha256 FROM articles WHERE id=? AND provider='cnyes'",
            (canonical_candidate_id(variant),))]
        assert any('SEARCH articles USING INDEX' in item for item in plan)
        assert not any('SCAN articles' in item for item in plan)


def test_unsupported_import_classification_does_not_issue_a_database_update(queue):
    from downloader.news_fulltext_cnyes_work import classify
    url = 'https://m.cnyes.com/news/id/1?exp=c'
    with queue.db:
        queue.add_articles([{'url': url}])
        row = queue.db.execute('SELECT id,url,provider FROM articles WHERE url=?', (url,)).fetchone()
        queries = []
        queue.db.set_trace_callback(queries.append)
        assert not classify(queue, row)
        queue.db.set_trace_callback(None)
    assert not any(query.lstrip().upper().startswith('UPDATE ARTICLES') for query in queries)


def test_dispatch_clears_unsupported_stale_hint_and_still_serves_work(queue):
    from downloader.news_fulltext_cnyes_work import candidate
    primary = 'https://news.cnyes.com/news/id/1'
    with queue.db:
        queue.add_articles([{'url': primary}])
        queue.db.execute('UPDATE articles SET cnyes_known_body_candidate=1,cnyes_canonical_candidate_id=? WHERE url=?',
                         (b'x' * 32, primary))
        selected = candidate(queue, 'pending', time.time(), prefer_known=True)
    assert selected['url'] == primary
    assert selected['cnyes_known_body_candidate'] == 0
    assert selected['cnyes_canonical_candidate_id'] is None
    assert_work_metrics(queue)


@pytest.mark.parametrize('case', ['unsupported', 'mobile_missing_body', 'mobile_known_body'])
@pytest.mark.parametrize('through_owner', [False, True])
def test_dispatch_returns_repaired_target_even_when_priority_lane_is_unchanged(queue, case, through_owner):
    from downloader.news_fulltext_cnyes_work import candidate

    primary = 'https://news.cnyes.com/news/id/1'
    variant = primary if case == 'unsupported' else 'https://m.cnyes.com/news/id/1'
    known = case == 'mobile_known_body'
    with queue.db:
        queue.add_articles([{'url': primary}])
        if case != 'unsupported':
            queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url=?",
                             ('a' * 64 if known else None, primary))
            queue.add_articles([{'url': variant}])
        queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=?,cnyes_known_body_candidate=? WHERE url=?',
                         (b'\xa5' * 32, int(known), variant))
        original = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())

    selected = queue.claim('cnyes', 300) if through_owner else candidate(queue, 'pending', time.time(), prefer_known=known)
    current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())
    expected = canonical_candidate_id(variant)
    assert selected['id'] == current['id'] == original['id']
    assert selected['cnyes_canonical_candidate_id'] == current['cnyes_canonical_candidate_id'] == expected
    assert selected['cnyes_known_body_candidate'] == current['cnyes_known_body_candidate'] == int(known)
    assert all(current[key] == original[key] for key in ('url', 'provider', 'due', 'error_code',
        'body_sha256', 'parquet_path', 'title', 'metadata_order_day'))
    assert_work_metrics(queue)


@pytest.mark.parametrize('lane', ['retry', 'undated', 'stored'])
def test_nonregular_claim_repairs_stale_cnyes_hint_before_returning(queue, lane):
    primary = 'https://news.cnyes.com/news/id/1'
    variant = 'https://m.cnyes.com/news/id/1'
    with queue.db:
        queue.add_articles([{'url': primary}])
        queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url=?", ('a' * 64, primary))
        queue.add_articles([{'url': variant, **({} if lane == 'undated' else {'date': '2026-10-09'})}])
        queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=?,cnyes_known_body_candidate=0 WHERE url=?',
                         (b'\xa5' * 32, variant))
        if lane in {'retry', 'stored'}:
            queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error',due=0,body_sha256=? WHERE url=?",
                             ('b' * 64 if lane == 'stored' else None, variant))
        original = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())
    if lane == 'retry':
        queue._pending_body_dispatch_bursts = {'cnyes': 8}
    elif lane == 'undated':
        queue._dated_body_dispatch_bursts = {'cnyes': 8}
    else:
        queue._missing_body_dispatch_bursts = {'cnyes': 8}

    selected = queue.claim('cnyes', 300)
    current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone())
    assert selected['id'] == current['id'] == original['id']
    assert selected['cnyes_canonical_candidate_id'] == current['cnyes_canonical_candidate_id'] == canonical_candidate_id(variant)
    assert selected['cnyes_known_body_candidate'] == current['cnyes_known_body_candidate'] == 1
    assert all(current[key] == original[key] for key in ('url', 'provider', 'due', 'error_code',
        'body_sha256', 'parquet_path', 'title', 'metadata_order_day'))
    if lane != 'stored':
        counts = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert counts['frontier_existing_body_candidate_dispatches'] == 1
    assert_work_metrics(queue)


def test_nonregular_claim_does_not_use_a_changed_head_after_hint_repair(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_work as work

    primary = 'https://news.cnyes.com/news/id/1'
    variant = 'https://m.cnyes.com/news/id/1'
    with queue.db:
        queue.add_articles([{'url': primary}, {'url': variant}])
        queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url=?", ('a' * 64, primary))
        queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error',due=0,"
                         'cnyes_canonical_candidate_id=?,cnyes_known_body_candidate=0 WHERE url=?', (b'\xa5' * 32, variant))
    queue._pending_body_dispatch_bursts = {'cnyes': 8}
    original = work.classify

    def change_after_repair(owner, row, **kwargs):
        result = original(owner, row, **kwargs)
        if row['url'] == variant and result is None:
            owner.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE id=?", ('b' * 64, row['id']))
        return result

    monkeypatch.setattr(work, 'classify', change_after_repair)
    assert queue.claim('cnyes', 300) is None
    row = queue.db.execute('SELECT state,body_sha256,lease_owner FROM articles WHERE url=?', (variant,)).fetchone()
    assert tuple(row) == ('complete', 'b' * 64, None)
    assert_work_metrics(queue)


@pytest.mark.parametrize('variant', ['https://m.cnyes.com/news/id/1', 'https://news.cnyes.com/news/id/1'])
def test_hint_repair_does_not_write_if_snapshot_provider_has_changed(queue, variant):
    from downloader.news_fulltext_cnyes_work import classify

    with queue.db:
        queue.add_articles([{'url': variant}])
        queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=? WHERE url=?', (b'\xa5' * 32, variant))
        old = queue.db.execute('SELECT * FROM articles WHERE url=?', (variant,)).fetchone()
        queue.db.execute("UPDATE articles SET provider='unregistered:changed' WHERE url=?", (variant,))
        before = queue.db.total_changes
        assert classify(queue, old, current_fields_trusted=True, signal_stale=True) is None
        assert queue.db.total_changes == before
    after = queue.db.execute('SELECT provider,cnyes_canonical_candidate_id FROM articles WHERE url=?', (variant,)).fetchone()
    assert tuple(after) == ('unregistered:changed', b'\xa5' * 32)


def test_existing_queue_installs_provider_hint_trigger_on_restart(queue, tmp_path):
    primary = 'https://news.cnyes.com/news/id/1'
    mobile = 'https://m.cnyes.com/news/id/1'
    with queue.db:
        queue.add_articles([{'url': primary}, {'url': mobile}])
        queue.db.execute('UPDATE articles SET state=\'complete\',body_sha256=? WHERE url=?', ('a' * 64, primary))
        queue.db.execute('DROP TRIGGER cnyes_candidate_body_provider_update')
    registry = queue.registry
    queue.close()
    reopened = NewsQueue(tmp_path, registry)
    try:
        with reopened.db:
            reopened.db.execute('UPDATE articles SET provider=? WHERE url=?', ('unregistered:changed', primary))
        assert reopened.db.execute('SELECT cnyes_known_body_candidate FROM articles WHERE url=?', (mobile,)).fetchone()[0] == 0
        assert_work_metrics(reopened)
    finally:
        reopened.close()


def test_restarted_owner_policy_reconciliation_updates_primary_provider_hint(queue, config, tmp_path):
    from copy import deepcopy
    primary = 'https://news.cnyes.com/news/id/1'
    mobile = 'https://m.cnyes.com/news/id/1'
    with queue.db:
        queue.add_articles([{'url': primary}, {'url': mobile, 'title': 'original metadata'}])
        queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url=?", ('a' * 64, primary))
    assert queue.db.execute('SELECT cnyes_known_body_candidate FROM articles WHERE url=?', (mobile,)).fetchone()[0] == 1
    queue.close()
    changed = deepcopy(config)
    changed['providers'][0]['domains'] = ['m.cnyes.com', 'api.cnyes.com']
    reopened = NewsQueue(tmp_path, PolicyRegistry(changed, base=tmp_path))
    try:
        source = reopened.db.execute('SELECT provider,body_sha256 FROM articles WHERE url=?', (primary,)).fetchone()
        view = reopened.db.execute('SELECT provider,body_sha256,title,cnyes_known_body_candidate FROM articles WHERE url=?',
                                   (mobile,)).fetchone()
        assert source['provider'] == 'unregistered:news.cnyes.com' and source['body_sha256'] == 'a' * 64
        assert view['provider'] == 'cnyes' and view['body_sha256'] is None and view['title'] == 'original metadata'
        assert view['cnyes_known_body_candidate'] == 0
        assert_work_metrics(reopened)
    finally:
        reopened.close()


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
        status_work = reopened.status(activity='running')['providers']['cnyes']['body_work_progress']
        assert status_work.pop('request_scope_progress')['available'] is False
        assert status_work == current
    finally:
        reopened.close()


def test_request_scope_progress_reads_only_indexed_missing_heads_and_never_mutates(queue):
    from downloader.news_fulltext_cnyes_work import progress, request_scope_progress
    now = time.time()
    with queue.db:
        queue.add_articles([{'url': f'https://amp-news.cnyes.com/news/id/{i}'} for i in (1, 2, 3)])
        queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'},
                            {'url': 'https://m.cnyes.com/news/id/4'},
                            {'url': 'https://news.cnyes.com/news/id/5'}])
        queue.db.execute("UPDATE articles SET body_sha256=?,state='retry' WHERE host='news.cnyes.com'", ('a' * 64,))
        queue.db.execute("UPDATE articles SET state='inflight',lease_owner='owner',lease_until=? WHERE url LIKE '%/3'", (now + 300,))
        queue.db.execute("UPDATE articles SET state='retry_cooldown',due=? WHERE url LIKE '%/2'", (now + 3600,))
    before = [tuple(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    work = progress(queue.db)
    control = {'temporarily_paused_robots_origins': {'https://amp-news.cnyes.com': now + 30},
               'temporarily_paused_gateway_hosts': {'amp-news.cnyes.com': now + 60},
               'unverified_robots_origins': {'https://amp-news.cnyes.com': now + 30},
               'unverified_gateway_hosts': {'amp-news.cnyes.com': now + 60},
               'unverified_robots_origins_overflow': False}
    queries = []; changes = queue.db.total_changes
    queue.db.set_trace_callback(queries.append)
    actual = request_scope_progress(queue.db, work, control, now_epoch=now)
    queue.db.set_trace_callback(None)
    assert actual['available'] and actual['active_missing_body_urls'] == 4
    assert actual['source_paused_missing_body_urls'] == 2
    assert actual['policy_paused_missing_body_urls'] == actual['gateway_paused_missing_body_urls'] == 2
    assert actual['other_missing_body_urls'] == 2  # mobile plus already in-flight AMP
    from datetime import UTC, datetime
    assert actual['next_source_probe_at_utc'] == datetime.fromtimestamp(now + 60, UTC).isoformat()
    assert actual['pending_source_rechecks'] == {'contract_version': 1, 'available': True,
        'unverified_missing_body_urls': 3, 'due_missing_body_urls': 0,
        'next_probe_at_utc': datetime.fromtimestamp(now + 60, UTC).isoformat()}
    assert not actual['source_recovery_verified'] and 'cnyes.com' not in json.dumps(actual)
    assert queue.db.total_changes == changes
    assert before == [tuple(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    assert len(queries) == 2
    for query in queries:
        plan = ' '.join(r['detail'] for r in queue.db.execute('EXPLAIN QUERY PLAN ' + query))
        assert 'SEARCH articles USING INDEX dispatch' in plan and 'SCAN articles' not in plan


@pytest.mark.parametrize('control', [None, {}, {'temporarily_paused_robots_origins': {}},
    {'temporarily_paused_robots_origins': {}, 'temporarily_paused_gateway_hosts': {'x': True}},
    {'temporarily_paused_robots_origins': {'x': float('nan')}, 'temporarily_paused_gateway_hosts': {}},
    {'temporarily_paused_robots_origins': {'x': '300'}, 'temporarily_paused_gateway_hosts': {}}])
def test_request_scope_missing_or_invalid_controls_stay_unknown(queue, control):
    from downloader.news_fulltext_cnyes_work import progress, request_scope_progress
    result = request_scope_progress(queue.db, progress(queue.db), control, now_epoch=time.time())
    assert not result['available'] and result['source_paused_missing_body_urls'] is None
    assert result['other_missing_body_urls'] is None and result['next_source_probe_at_utc'] is None


def test_request_scope_large_frontier_is_unknown_without_an_article_scan(queue):
    from downloader.news_fulltext_cnyes_work import MAX_REQUEST_SCOPE_URLS, request_scope_progress
    queries = []
    queue.db.set_trace_callback(queries.append)
    result = request_scope_progress(queue.db, {'active_missing_body_urls': MAX_REQUEST_SCOPE_URLS + 1}, {},
                                    now_epoch=time.time())
    queue.db.set_trace_callback(None)
    assert not result['available'] and queries == []


def test_request_scope_expired_policy_is_not_a_new_body_or_provider_recovery(queue):
    from downloader.news_fulltext_cnyes_work import progress, request_scope_progress
    now = time.time()
    with queue.db:
        queue.add_articles([{'url': 'https://amp-news.cnyes.com/news/id/1'}])
    result = request_scope_progress(queue.db, progress(queue.db), {
        'temporarily_paused_robots_origins': {'https://amp-news.cnyes.com': now - 1},
        'temporarily_paused_gateway_hosts': {},
        'unverified_robots_origins': {'https://amp-news.cnyes.com': now - 1},
        'unverified_gateway_hosts': {}, 'unverified_robots_origins_overflow': False}, now_epoch=now)
    assert result['available'] and result['source_paused_missing_body_urls'] == 0
    # Registering AMP also discovers the unverified desktop lookup in this
    # same queue. Neither URL has a body; an expired gate accepts neither.
    assert result['active_missing_body_urls'] == result['other_missing_body_urls'] == 2
    assert result['next_source_probe_at_utc'] is None
    assert result['pending_source_rechecks']['unverified_missing_body_urls'] == 1
    assert result['pending_source_rechecks']['due_missing_body_urls'] == 1
    assert not result['source_recovery_verified'] and queue.db.execute('SELECT body_sha256 FROM articles').fetchone()[0] is None


@pytest.mark.parametrize('patch', [
    {'unverified_robots_origins': None}, {'unverified_gateway_hosts': None},
    {'unverified_robots_origins_overflow': True}, {'unverified_robots_origins_overflow': None},
    {'unverified_robots_origins': {'https://amp-news.cnyes.com': None}},
    {'unverified_gateway_hosts': {'amp-news.cnyes.com': float('nan')}},
    {'unverified_robots_origins': {}},
])
def test_unreliable_or_inconsistent_recheck_evidence_is_unknown(queue, patch):
    from downloader.news_fulltext_cnyes_work import progress, request_scope_progress
    now = time.time()
    with queue.db:
        queue.add_articles([{'url': 'https://amp-news.cnyes.com/news/id/1'}])
    control = {'temporarily_paused_robots_origins': {'https://amp-news.cnyes.com': now + 60},
        'temporarily_paused_gateway_hosts': {},
        'unverified_robots_origins': {'https://amp-news.cnyes.com': now + 60},
        'unverified_gateway_hosts': {}, 'unverified_robots_origins_overflow': False, **patch}
    result = request_scope_progress(queue.db, progress(queue.db), control, now_epoch=now)
    assert result['available'] and result['source_paused_missing_body_urls'] == 1
    assert not result['pending_source_rechecks']['available']
    assert result['pending_source_rechecks']['unverified_missing_body_urls'] is None


@pytest.mark.parametrize('gated,paused', [(None, None), ('https://amp-news.cnyes.com', 1),
                                        ('https://unrelated.example', 0)])
def test_owner_status_does_not_price_paused_or_unknown_scope_at_healthy_ingestion_rate(queue, gated, paused):
    with queue.db:
        queue.add_articles([{'url': 'https://amp-news.cnyes.com/news/id/1'}])
    if gated is not None:
        deadline = time.time() + 300
        queue.http_capacity_snapshot = lambda: {'providers': {'cnyes': {
            'temporarily_paused_robots_origins': {gated: deadline},
            'temporarily_paused_gateway_hosts': {}, 'unverified_robots_origins': {gated: deadline},
            'unverified_gateway_hosts': {}, 'unverified_robots_origins_overflow': False}}}
    queue.metrics.started -= 120
    queue.metrics.count('cnyes', 'durably_accepted_new_url_heads', 10)
    snapshot = queue.status(activity='running')
    current = snapshot['providers']['cnyes']
    assert current['observed_owner_articles_per_second'] > 0
    scope = current['body_work_progress']['request_scope_progress']
    assert scope['source_paused_missing_body_urls'] == paused
    assert current['body_work_progress']['active_missing_body_urls'] == 2
    assert not scope['source_recovery_verified']
    if paused == 0:
        assert current['remaining_eligible_estimate_seconds'] > 0
    else:
        assert current['remaining_eligible_estimate_seconds'] is None
        assert snapshot['eligible_eta_seconds'] is None


def test_owner_status_does_not_price_expired_unverified_origin_at_other_lane_rate(queue):
    with queue.db:
        queue.add_articles([{'url': 'https://amp-news.cnyes.com/news/id/1'}])
    queue.metrics.started -= 120
    queue.metrics.count('cnyes', 'durably_accepted_new_url_heads', 10)
    deadline = time.time() - 10
    control = {'temporarily_paused_robots_origins': {}, 'temporarily_paused_gateway_hosts': {},
        'unverified_robots_origins': {'https://amp-news.cnyes.com': deadline},
        'unverified_gateway_hosts': {}, 'unverified_robots_origins_overflow': False}
    queue.http_capacity_snapshot = lambda: {'providers': {'cnyes': control}}
    snapshot = queue.status(activity='running')
    current = snapshot['providers']['cnyes']
    scope = current['body_work_progress']['request_scope_progress']
    assert current['observed_owner_articles_per_second'] > 0
    assert scope['source_paused_missing_body_urls'] == 0
    assert scope['pending_source_rechecks']['unverified_missing_body_urls'] == 1
    assert scope['pending_source_rechecks']['due_missing_body_urls'] == 1
    assert current['remaining_eligible_estimate_seconds'] is None
    assert snapshot['eligible_eta_seconds'] is None

    # Only a real successful probe clears the HTTP owner's evidence. The
    # queue consumes that cleared state without imposing a new deadline.
    control['unverified_robots_origins'] = {}
    recovered = queue.status(activity='running')['providers']['cnyes']
    assert recovered['remaining_eligible_estimate_seconds'] > 0


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


def test_slow_checkpoint_seek_eventually_classifies_later_state_without_source_request(queue, monkeypatch):
    from types import SimpleNamespace
    from downloader import news_fulltext_cnyes_work as work, news_fulltext_urls as urls

    mobile = 'https://m.cnyes.com/news/id/4123456'
    with queue.db:
        queue.add_articles([{'url': mobile}])
        queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error',due=?,"
                         "cnyes_canonical_candidate_id=NULL,cnyes_known_body_candidate=0 WHERE url=?",
                         (1234567890, mobile))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (mobile,)).fetchone())
    clock = [0.0]
    read = urls._read_checkpoint
    def slow(*args, **kwargs):
        result = read(*args, **kwargs)
        clock[0] += 1.0
        return result
    monkeypatch.setattr(work, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(urls, '_read_checkpoint', slow)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
                        lambda *args, **kwargs: pytest.fail('local backfill must not request a publisher'))
    results = [backfill(queue, max_seconds=.04) for _ in range(12)]
    assert sum(result['checked_heads'] for result in results) == 1
    after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (mobile,)).fetchone())
    assert after['cnyes_canonical_candidate_id'] == canonical_candidate_id(mobile)
    assert all(after[key] == before[key] for key in before if not key.startswith('cnyes_'))
    assert all(result['publisher_requests'] == 0 and not result['source_observations_merged'] for result in results)


def test_slow_empty_seek_sweeps_each_state_once_then_returns_to_idle(queue, monkeypatch):
    from types import SimpleNamespace
    from downloader import news_fulltext_cnyes_work as work, news_fulltext_urls as urls

    clock = [0.0]
    read = urls._read_checkpoint
    def slow(*args, **kwargs):
        result = read(*args, **kwargs)
        clock[0] += 1.0
        return result
    monkeypatch.setattr(work, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(urls, '_read_checkpoint', slow)
    before = queue.db.total_changes
    results = [backfill(queue, max_seconds=.04) for _ in range(9)]
    assert all(result['checked_heads'] == 0 for result in results)
    assert [work.maintenance_delay(result) for result in results] == [.25] * 8 + [5.0]
    assert queue.db.total_changes == before


def test_stopped_candidate_backfill_never_spends_a_slow_seek_turn(queue, monkeypatch):
    from downloader import news_fulltext_urls as urls

    stopped = threading.Event()
    stopped.set()
    monkeypatch.setattr(urls, '_read_checkpoint',
                        lambda *args, **kwargs: pytest.fail('a stopped owner must not seek'))
    before = queue.db.total_changes
    assert backfill(queue, stop=stopped)['checked_heads'] == 0
    assert queue.db.total_changes == before


def test_slow_seek_resumes_after_restart_from_durable_candidate_cursors(config, tmp_path, monkeypatch):
    from types import SimpleNamespace
    from downloader import news_fulltext_cnyes_work as work, news_fulltext_urls as urls

    owner = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    mobile = 'https://m.cnyes.com/news/id/4789012'
    try:
        with owner.db:
            owner.add_articles([{'url': mobile}])
            owner.db.execute("UPDATE articles SET state='retry',cnyes_canonical_candidate_id=NULL WHERE url=?", (mobile,))
        clock = [0.0]
        read = urls._read_checkpoint
        def slow(*args, **kwargs):
            result = read(*args, **kwargs)
            clock[0] += 1.0
            return result
        monkeypatch.setattr(work, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
        monkeypatch.setattr(urls, '_read_checkpoint', slow)
        assert backfill(owner, max_seconds=.04)['checked_heads'] == 0
        registry = owner.registry
        owner.close()
        owner = NewsQueue(tmp_path, registry)
        results = [backfill(owner, max_seconds=.04) for _ in range(9)]
        assert sum(result['checked_heads'] for result in results) == 1
        row = owner.db.execute('SELECT cnyes_canonical_candidate_id,state FROM articles WHERE url=?', (mobile,)).fetchone()
        assert row[0] == canonical_candidate_id(mobile) and row[1] == 'retry'
    finally:
        owner.close()


def test_candidate_backfill_rolls_back_cursor_and_turn_when_classification_fails(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_work as work

    mobile = 'https://m.cnyes.com/news/id/4567890'
    with queue.db:
        queue.add_articles([{'url': mobile}])
        queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=NULL WHERE url=?', (mobile,))
    queue._cnyes_candidate_seek_turn = 3  # mobile pending
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (mobile,)).fetchone())
    real = work.classify
    def interrupted(*args):
        real(*args)
        raise OSError('interrupted local classification')
    monkeypatch.setattr(work, 'classify', interrupted)
    with pytest.raises(OSError, match='interrupted local classification'):
        backfill(queue, limit=1)
    assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (mobile,)).fetchone()) == before
    assert queue._cnyes_candidate_seek_turn == 3
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (work.CURSOR,)).fetchone()
    monkeypatch.setattr(work, 'classify', real)
    assert backfill(queue, limit=1)['checked_heads'] == 1
    assert queue.db.execute('SELECT cnyes_canonical_candidate_id FROM articles WHERE url=?', (mobile,)).fetchone()[0] == canonical_candidate_id(mobile)


def test_candidate_backfill_does_not_starve_retry_behind_busy_pending(queue):
    pending = [f'https://m.cnyes.com/news/id/{i}' for i in range(300, 320)]
    retry = 'https://m.cnyes.com/news/id/1234567'
    with queue.db:
        queue.add_articles([{'url': url} for url in pending + [retry]])
        queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=NULL WHERE host=?', ('m.cnyes.com',))
        queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error',due=? WHERE url=?",
                         (1234567890, retry))
    before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (retry,)).fetchone())
    first, second = (backfill(queue, limit=1) for _ in range(2))
    assert first['checked_heads'] == second['checked_heads'] == 1
    after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (retry,)).fetchone())
    assert after['cnyes_canonical_candidate_id'] == canonical_candidate_id(retry)
    assert all(after[key] == before[key] for key in before if not key.startswith('cnyes_'))


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
