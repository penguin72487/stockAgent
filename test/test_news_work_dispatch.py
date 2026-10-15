"""Missing-body fairness without changing source deadlines, leases or values."""
import time

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, ARTICLE_DISPATCH_CONTRACT_VERSION
from downloader.news_fulltext_policy import NewsError
from test_news_fulltext import config


def queued_work(queue, *, missing=16, rechecks=4):
    with queue.db:
        queue.add_articles([
            {'url': f'https://publisher.test/missing/{i}', 'date': '2014-01-01'} for i in range(missing)
        ] + [{'url': f'https://publisher.test/recheck/{i}', 'date': '2026-10-09'} for i in range(rechecks)])
        queue.db.execute("UPDATE articles SET state='retry',body_sha256=?,body_bytes=12 "
                         "WHERE url LIKE '%/recheck/%'", ('a' * 64,))


def test_missing_older_bodies_are_not_starved_by_newer_stored_rechecks(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue)
        kinds = []
        for _ in range(18):
            row = queue.claim('publisher', 300)
            kinds.append('missing' if row['body_sha256'] is None else 'recheck')
            assert queue.db.execute('SELECT state,lease_owner FROM articles WHERE id=?', (row['id'],)).fetchone()[:] == ('inflight', queue.owner)
        assert kinds == (['missing'] * 8 + ['recheck']) * 2
    finally:
        queue.close()


def test_future_recheck_deadline_is_not_cleared_to_fill_fairness_slot(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=12, rechecks=1)
        deadline = time.time() + 3600
        with queue.db:
            queue.db.execute("UPDATE articles SET due=?,error_code='source_access_denied' WHERE body_sha256 IS NOT NULL", (deadline,))
        for _ in range(12):
            assert queue.claim('publisher', 300)['body_sha256'] is None
        assert queue.claim('publisher', 300) is None
        row = queue.db.execute('SELECT due,state,error_code FROM articles WHERE body_sha256 IS NOT NULL').fetchone()
        assert row[:] == (deadline, 'retry', 'source_access_denied')
    finally:
        queue.close()


def test_missing_lane_preserves_due_and_current_worker_leases(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=3, rechecks=1)
        deadline = time.time() + 3600
        with queue.db:
            queue.db.execute("UPDATE articles SET due=?,state='retry_cooldown' WHERE url LIKE '%/missing/0'", (deadline,))
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner='other',lease_until=? WHERE url LIKE '%/missing/1'", (deadline,))
        assert queue.claim('publisher', 300)['url'].endswith('/missing/2')
        assert queue.claim('publisher', 300)['body_sha256'] is not None
        assert queue.claim('publisher', 300) is None
        assert queue.db.execute("SELECT lease_owner FROM articles WHERE url LIKE '%/missing/1'").fetchone()[0] == 'other'
        assert queue.db.execute("SELECT due FROM articles WHERE url LIKE '%/missing/0'").fetchone()[0] == deadline
    finally:
        queue.close()


def test_dispatch_stays_latest_first_within_missing_lane_across_retry_states(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/old', 'date': '2014-01-01'},
                                {'url': 'https://publisher.test/new', 'date': '2026-10-09'}])
            queue.db.execute("UPDATE articles SET state='retry_cooldown',due=0,error_code='timeout' WHERE url LIKE '%/new'")
        assert queue.claim('publisher', 300)['url'].endswith('/new')
        assert queue.claim('publisher', 300)['url'].endswith('/old')
    finally:
        queue.close()


def test_each_publisher_has_an_independent_missing_burst(config, tmp_path):
    config['providers'].append({**config['providers'][0], 'id': 'peer', 'domains': ['peer.test']})
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=8, rechecks=1)
        with queue.db:
            queue.add_articles([{'url': 'https://peer.test/missing', 'date': '2014-01-01'},
                                {'url': 'https://peer.test/recheck', 'date': '2026-10-09'}])
            queue.db.execute("UPDATE articles SET state='retry',body_sha256=? WHERE url='https://peer.test/recheck'", ('b' * 64,))
        for _ in range(8):
            assert queue.claim('publisher', 300)['body_sha256'] is None
        assert queue.claim('peer', 300)['body_sha256'] is None
        assert queue.claim('publisher', 300)['body_sha256'] is not None
    finally:
        queue.close()


@pytest.mark.parametrize('stored_only,undated_only', [(False, False), (True, False), (False, True)])
def test_work_kind_query_uses_ready_partial_index_without_sorting(config, tmp_path, stored_only, undated_only):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        plan = queue.db.execute('EXPLAIN QUERY PLAN ' + queue.article_dispatch_statement(stored_only=stored_only,
                                undated_only=undated_only),
            ('publisher', 'pending', time.time())).fetchall()
        assert all('TEMP B-TREE' not in row[3] for row in plan)
        assert any('dispatch_work_kind' in row[3] for row in plan)
        if undated_only:
            searches = [r[3] for r in plan if 'SEARCH articles' in r[3]]
            assert len(searches) == 2 and all('metadata_order_day=? AND due<?' in r for r in searches)
    finally:
        queue.close()


def test_undated_missing_bodies_are_not_starved_and_never_backdated(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=32, rechecks=4)
        with queue.db:
            queue.add_articles([{'url': f'https://publisher.test/undated/{i}'} for i in range(2)])
        lanes = []
        for _ in range(20):
            row = queue.claim('publisher', 300)
            lanes.append('recheck' if row['body_sha256'] else 'dated' if row['metadata_order_day'] else 'undated')
            if row['url'].find('/undated/') != -1:
                assert row['metadata_date_raw'] == row['metadata_order_day'] == ''
        # Undated work also counts toward the independent eight-missing-body
        # recheck bound; the two fairness clocks are not reset together.
        assert lanes == ['dated'] * 8 + ['recheck', 'undated'] + ['dated'] * 7 + ['recheck', 'dated', 'undated']
        assert queue.metrics.snapshot()['providers']['publisher']['counts']['frontier_undated_missing_body_dispatches'] == 2
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='article_dispatch_contract_version'").fetchone()[0] == str(ARTICLE_DISPATCH_CONTRACT_VERSION)
        assert queue.status(activity='running')['article_dispatch_contract_version'] == ARTICLE_DISPATCH_CONTRACT_VERSION
    finally:
        queue.close()


@pytest.mark.parametrize('unknown_day', ['', None])
def test_undated_lane_keeps_retry_deadlines_and_worker_leases(config, tmp_path, unknown_day):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=12, rechecks=0)
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/undated/future'},
                                {'url': 'https://publisher.test/undated/leased'},
                                {'url': 'https://publisher.test/undated/ready'}])
            deadline = time.time() + 3600
            queue.db.execute("UPDATE articles SET metadata_order_day=? WHERE url LIKE '%/undated/%'", (unknown_day,))
            queue.db.execute("UPDATE articles SET due=?,state='retry_cooldown' WHERE url LIKE '%/future'", (deadline,))
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner='peer',lease_until=? WHERE url LIKE '%/leased'", (deadline,))
        for _ in range(8):
            assert queue.claim('publisher', 300)['metadata_order_day']
        assert queue.claim('publisher', 300)['url'].endswith('/undated/ready')
        for _ in range(4):
            assert queue.claim('publisher', 300)['metadata_order_day']
        assert queue.claim('publisher', 300) is None
        assert queue.db.execute("SELECT due,state FROM articles WHERE url LIKE '%/future'").fetchone()[:] == (deadline, 'retry_cooldown')
        assert queue.db.execute("SELECT lease_owner FROM articles WHERE url LIKE '%/leased'").fetchone()[0] == 'peer'
    finally:
        queue.close()


def test_undated_service_does_not_borrow_another_publishers_burst(config, tmp_path):
    config['providers'].append({**config['providers'][0], 'id': 'peer', 'domains': ['peer.test']})
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=10, rechecks=0)
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/undated'},
                                {'url': 'https://peer.test/dated', 'date': '2026-10-09'},
                                {'url': 'https://peer.test/undated'}])
        for _ in range(8):
            assert queue.claim('publisher', 300)['metadata_order_day']
        assert queue.claim('peer', 300)['url'].endswith('/dated')
        assert queue.claim('publisher', 300)['url'].endswith('/undated')
    finally:
        queue.close()


@pytest.mark.parametrize('value', [True, 0, -1, 1.5, 129, '8'])
def test_invalid_undated_configuration_is_rejected(config, tmp_path, value):
    config['dated_body_undated_burst'] = value
    path = tmp_path / 'config.json'
    atomic_write_json(path, config)
    with pytest.raises(ValueError):
        load_config(path, repo=tmp_path)


@pytest.mark.parametrize('value', [True, 0, -1, 1.5, 129, '8'])
def test_direct_queue_api_rejects_unsafe_undated_burst(config, tmp_path, value):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with pytest.raises(ValueError, match='invalid dated-body undated burst'):
            queue.claim('publisher', 300, dated_body_burst=value)
    finally:
        queue.close()


@pytest.mark.parametrize('value', [True, 0, -1, 1.5, 129, '8'])
def test_invalid_fairness_configuration_is_rejected(config, tmp_path, value):
    config['missing_body_recheck_burst'] = value
    path = tmp_path / 'config.json'
    atomic_write_json(path, config)
    with pytest.raises(ValueError):
        load_config(path, repo=tmp_path)


@pytest.mark.parametrize('value', [True, 0, -1, 1.5, 129, '8'])
def test_direct_queue_api_rejects_unsafe_fairness_burst(config, tmp_path, value):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with pytest.raises(ValueError, match='invalid missing-body recheck burst'):
            queue.claim('publisher', 300, missing_body_burst=value)
    finally:
        queue.close()


def test_old_due_missing_retry_gets_bounded_service_behind_new_pending(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=32, rechecks=2)
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/retry-old', 'date': '2009-01-01'},
                                {'url': 'https://publisher.test/retry-new', 'date': '2026-10-09'}])
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_502',due=? WHERE url LIKE '%/retry-old'",
                             (time.time() - 300,))
            queue.db.execute("UPDATE articles SET state='retry_cooldown',error_code='transport_error',due=? WHERE url LIKE '%/retry-new'",
                             (time.time() + 3600,))
        rows = [queue.claim('publisher', 300) for _ in range(10)]
        assert rows[8]['body_sha256'] is not None  # Existing recheck lane keeps its service.
        assert rows[9]['url'].endswith('/retry-old')
        assert rows[9]['error_code'] == 'http_502'  # Original error is retained until an outcome.
        assert queue.db.execute("SELECT state,lease_owner FROM articles WHERE url LIKE '%/retry-old'").fetchone()[:] == ('inflight', queue.owner)
        assert queue.db.execute("SELECT state FROM articles WHERE url LIKE '%/retry-new'").fetchone()[0] == 'retry_cooldown'
        assert queue.metrics.snapshot()['providers']['publisher']['counts']['frontier_due_missing_retry_dispatches'] == 1
    finally:
        queue.close()


def test_newer_failures_cannot_starve_older_first_downloads(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=32, rechecks=0)
        with queue.db:
            queue.add_articles([{'url': f'https://publisher.test/failure/{i}', 'date': '2026-10-09'}
                                for i in range(32)])
            queue.db.execute("UPDATE articles SET state='retry',error_code='body_not_found',due=0 "
                             "WHERE url LIKE '%/failure/%'")
        rows = [queue.claim('publisher', 300) for _ in range(19)]
        assert all('/missing/' in r['url'] for r in rows[:8])
        assert '/failure/' in rows[8]['url']
        assert all('/missing/' in r['url'] for r in rows[9:17])
        assert '/failure/' in rows[17]['url']
        assert '/missing/' in rows[18]['url']
        assert queue.metrics.snapshot()['providers']['publisher']['counts']['frontier_due_missing_retry_dispatches'] == 2
    finally:
        queue.close()


def test_regular_network_recovery_is_latest_first_and_excludes_parser_repairs_without_scan(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/first', 'date': '2014-01-01'},
                                {'url': 'https://publisher.test/parser', 'date': '2026-10-09'},
                                {'url': 'https://publisher.test/network', 'date': '2026-10-08'}])
            queue.db.execute("UPDATE articles SET state='retry',error_code='body_not_found',due=0 WHERE url LIKE '%/parser'")
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_502',due=0 WHERE url LIKE '%/network'")
        assert queue.claim('publisher', 300)['url'].endswith('/network')
        assert queue.claim('publisher', 300)['url'].endswith('/first')
        assert queue.claim('publisher', 300)['url'].endswith('/parser')
        plan = queue.db.execute('EXPLAIN QUERY PLAN ' + queue.article_dispatch_statement(exclude_parser_repairs=True),
                                ('publisher', 'retry', time.time())).fetchall()
        assert any('dispatch_nonparser_retry' in row[3] for row in plan)
        assert all('TEMP B-TREE' not in row[3] for row in plan)
    finally:
        queue.close()


@pytest.mark.parametrize('network_state', ['retry', 'retry_cooldown'])
@pytest.mark.parametrize('old_error', ['body_not_found', 'http_502'])
@pytest.mark.parametrize('burst', [2, 8])
def test_newer_regular_retries_cannot_reset_oldest_due_service_credit(config, tmp_path, network_state, old_error, burst):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/old-due', 'date': '2010-01-01'},
                                {'url': 'https://publisher.test/new-network', 'date': '2026-10-09'},
                                {'url': 'https://publisher.test/pending', 'date': '2014-01-01'}])
            queue.db.execute("UPDATE articles SET state='retry',error_code=?,due=?,attempts=3,last_checked_at_utc='2026-10-10T00:00:00+00:00' "
                             "WHERE url LIKE '%/old-due'", (old_error, time.time() - 300))
            queue.db.execute('UPDATE articles SET state=?,error_code=?,due=? WHERE url LIKE ?',
                             (network_state, 'http_502', time.time() - 100, '%/new-network'))
        original = dict(queue.db.execute("SELECT * FROM articles WHERE url LIKE '%/old-due'").fetchone())
        selected = []
        for _ in range(burst + 1):
            row = queue.claim('publisher', 300, pending_body_burst=burst)
            selected.append(row['url'])
            if row['url'].endswith('/new-network'):
                # Synthetic repeated due network work: no HTTP, deadline
                # override or immediate retry is performed in production.
                with queue.db:
                    queue.db.execute('UPDATE articles SET state=?,lease_owner=NULL,lease_until=NULL WHERE id=?',
                                     (network_state, row['id']))
        assert all(url.endswith('/new-network') for url in selected[:burst])
        assert selected[burst].endswith('/old-due')
        current = dict(queue.db.execute("SELECT * FROM articles WHERE url LIKE '%/old-due'").fetchone())
        for field in ('due', 'error_code', 'last_checked_at_utc', 'body_sha256', 'metadata_date_raw'):
            assert current[field] == original[field]
        assert current['attempts'] == 4 and current['lease_owner'] == queue.owner
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['frontier_oldest_due_retry_dispatches'] == 1
        assert counts['frontier_due_missing_retry_dispatches'] == burst + 1
    finally:
        queue.close()


def test_legacy_pending_repair_migration_preserves_evidence_and_deadlines(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        urls = [f'https://publisher.test/repair/{i}' for i in range(6)]
        with queue.db:
            queue.add_articles([{'url': u} for u in urls])
            queue.db.execute("UPDATE articles SET state='pending',error_code='body_not_found',attempts=2,"
                             "due=12345678900,last_checked_at_utc='2026-10-09T00:00:00+00:00'")
            queue.db.execute("UPDATE articles SET error_code='article_attribution_not_verified',body_sha256='old-head',body_bytes=12 WHERE url=?", (urls[1],))
            queue.db.execute("UPDATE articles SET error_code='source_access_denied' WHERE url=?", (urls[2],))
            queue.db.execute("UPDATE articles SET last_checked_at_utc=NULL WHERE url=?", (urls[3],))
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner=?,lease_until=12345678900 WHERE url=?", (queue.owner, urls[4]))
            queue.db.execute("UPDATE articles SET error_code='paywall_or_login' WHERE url=?", (urls[5],))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        # The prior owner already wrote its one-time empty marker before these
        # legacy rows were restored. A new dispatch ABI must inspect them once.
        with queue.db:
            queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'parser_repair_retry_lane_v1:%'")
        queue.migrate_parser_repair_lanes()
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        for i, url in enumerate(urls):
            expected = {**before[url], 'state': 'retry'} if i in (0, 1) else before[url]
            assert after[url] == expected
        marker = queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'parser_repair_retry_lane_v1:%'").fetchone()[0]
        assert '"rows_moved": 2' in marker
        statements = []
        queue.db.set_trace_callback(statements.append)
        queue.reconcile_policy()
        assert not any('UPDATE articles' in s for s in statements)
        assert queue.claim('publisher', 300) is None  # Migration never erases a future deadline.
    finally:
        queue.close()


def test_changed_parser_real_failures_are_scheduled_as_bounded_repairs(config, tmp_path):
    from dataclasses import replace
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/known-failure'}])
        row = queue.claim_url('https://publisher.test/known-failure', 300)
        queue.failed(row, NewsError('body_not_found'), 6)
        prior_clock = queue.db.execute('SELECT last_checked_at_utc FROM articles').fetchone()[0]
        queue.registry.sources['publisher'] = replace(queue.registry.sources['publisher'], data_fingerprint='new-parser')
        queue.reconcile_policy()
        row = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert row['state'] == 'retry' and row['error_code'] == 'body_not_found'
        assert row['last_checked_at_utc'] == prior_clock
        assert row['due'] == 0 and row['attempts'] == 0
        assert row['body_sha256'] is None and row['lease_owner'] is None
    finally:
        queue.close()


def test_retry_lane_is_oldest_due_indexed_and_provider_local(config, tmp_path):
    config['providers'].append({**config['providers'][0], 'id': 'peer', 'domains': ['peer.test']})
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=8, rechecks=0)
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/old-deadline', 'date': '2010-01-01'},
                                {'url': 'https://publisher.test/new-deadline', 'date': '2011-01-01'},
                                {'url': 'https://peer.test/pending', 'date': '2026-10-09'},
                                {'url': 'https://peer.test/retry', 'date': '2009-01-01'}])
            now = time.time()
            for suffix, due in (('old-deadline', now - 200), ('new-deadline', now - 100), ('retry', now - 300)):
                queue.db.execute("UPDATE articles SET state='retry',error_code='transport_error',due=? WHERE url LIKE ?",
                                 (due, '%/' + suffix))
        for _ in range(8):
            assert queue.claim('publisher', 300)['url'].find('/missing/') != -1
        assert queue.claim('peer', 300)['url'].endswith('/pending')
        assert queue.claim('publisher', 300)['url'].endswith('/old-deadline')
        plan = queue.db.execute('EXPLAIN QUERY PLAN ' + queue.retry_dispatch_statement(), ('publisher', now)).fetchall()
        assert any('dispatch_missing_retry' in row[3] for row in plan)
        assert all('TEMP B-TREE' not in row[3] for row in plan)
    finally:
        queue.close()


@pytest.mark.parametrize('key', ['pending_body_retry_burst', 'bulk_article_burst'])
@pytest.mark.parametrize('value', [True, 0, -1, 1.5, 129, '8'])
def test_new_dispatch_bursts_reject_unsafe_configuration(config, tmp_path, key, value):
    config[key] = value
    path = tmp_path / 'config.json'
    atomic_write_json(path, config)
    with pytest.raises(ValueError):
        load_config(path, repo=tmp_path)


@pytest.mark.parametrize('value', [True, 0, -1, 1.5, 129, '8'])
def test_direct_queue_api_rejects_unsafe_retry_burst(config, tmp_path, value):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with pytest.raises(ValueError, match='invalid pending-body retry burst'):
            queue.claim('publisher', 300, pending_body_burst=value)
    finally:
        queue.close()


def test_dispatch_benchmark_pins_same_scope_without_leases_or_source_calls(config, tmp_path):
    from scripts.benchmark_news_dispatch import benchmark
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queued_work(queue, missing=12, rechecks=2)
    finally:
        queue.close()
    result = benchmark(tmp_path, ['publisher'], repetitions=3)
    assert result['accepted'] and result['queue_known_urls'] == 14
    assert result['all_history_complete'] is False and result['publish'] is False
    assert result['dispatch_index_physical_bytes'] > 0
    assert result['contract_version'] == 2
    assert {case['lane'] for case in result['cases']} == {'missing_preferred', 'stored_recheck', 'undated_missing'}
    assert all(case['indexed_no_temporary_sort'] and case['ordering_and_eligibility_match'] for case in result['cases'])
