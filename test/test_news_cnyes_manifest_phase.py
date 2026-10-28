"""Finite metadata-phase counts/ETA never become full-text completeness."""
from datetime import datetime, timezone
from dataclasses import replace
import sqlite3
import time

import pytest

from downloader.news_fulltext_cnyes_index import PublicIndexRowsCache, progress
from downloader.news_fulltext import fail_discovery
from downloader.news_fulltext_policy import NewsError
from test_cnyes_news_bulk import BASE, cfg
from test_news_cnyes_public_index import DAY, accept, index_payload, queue
from test_news_cnyes_index_manifest_schedule import add_gap


def page(queue, number=1, *, day=DAY, total=3000):
    data = index_payload(ids=range((number - 1) * 30 + 1, number * 30 + 1), page=number, total=total)
    for post in data['items']['data']:
        post['publishAt'] = day * 86400 - 28800 + 42
    return accept(queue, data, page=number, day=day)


def phase(queue, **kwargs):
    return progress(queue.db, queue.registry.sources['cnyes'], **kwargs)['manifest_phase']


def test_unseen_dates_are_not_extrapolated_and_read_only_report_preserves_all_rows(queue):
    with queue.db:
        add_gap(queue, DAY)
        add_gap(queue, DAY - 1)
    page(queue, total=61)
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    changes = queue.db.total_changes
    result = phase(queue)
    assert result['planned_source_dates'] == 2 and result['first_pages_observed'] == 1
    assert result['first_pages_remaining'] == 1 and not result['totals_known']
    assert result['source_reported_total_pages'] is result['source_reported_total_metadata_rows'] is None
    assert result['progress_ratio'] is result['remaining_pages'] is None
    assert result['forecast']['state'] == 'unknown_denominator'
    assert not result['phase_complete'] and not result['body_completion_claimed']
    assert queue.db.total_changes == changes
    assert before == [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]


def test_same_real_date_set_deduplicates_categories_and_excludes_unplanned_heads(queue):
    with queue.db:
        add_gap(queue, DAY)
        add_gap(queue, DAY - 1)
        queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'foreign','test','clock')", ('unreviewed',))
    page(queue, total=61)
    page(queue, day=DAY - 1, total=92)
    page(queue, day=DAY - 2, total=123)
    result = phase(queue)
    assert result['planned_source_dates'] == result['first_pages_observed'] == 2
    assert result['source_reported_total_pages'] == 3 + 4
    assert result['source_reported_total_metadata_rows'] == 61 + 92
    assert result['verified_pages'] == 2 and result['observed_metadata_rows'] == 60
    assert result['remaining_pages'] == 5 and result['progress_ratio'] == 2 / 7
    assert result['metadata_only'] and not result['all_history_complete']


def test_changed_source_total_suppresses_phase_denominator_not_original_evidence(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, total=61)
    page(queue, number=2, total=62)
    result = phase(queue)
    assert result['dates_with_changed_source_total'] == 1
    assert not result['totals_known'] and result['source_reported_total_pages'] is None
    assert result['forecast']['middle_seconds'] is None
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == 2


def test_empty_scope_cannot_claim_all_history_complete(queue):
    result = phase(queue)
    assert result['planned_source_dates'] == 0 and not result['totals_known']
    assert result['progress_ratio'] is None and not result['phase_complete']


def observed_bins(queue, bins, now):
    with queue.db:
        add_gap(queue, DAY)
    number = 0
    for position, count in enumerate(bins):
        for _ in range(count):
            number += 1
            page(queue, number)
            stamp = datetime.fromtimestamp(now - len(bins) * 60 + position * 60 + 30, timezone.utc).isoformat()
            with queue.db:
                queue.db.execute('UPDATE cnyes_public_index_observations SET source_observed_at_utc=? WHERE page=?',
                    (stamp, number))


def test_whole_bins_include_zero_wait_time_and_derive_conditional_scenarios(queue):
    now, bins = time.time(), [2, 0, 4, 3, 5]
    observed_bins(queue, bins, now)
    result = phase(queue, owner_start_epoch=now - 600, now_epoch=now, activity='running')
    rate = result['forecast']
    assert rate['state'] == 'recent_measured' and rate['window_page_counts'] == bins
    assert rate['sample_seconds'] == 300 and rate['verified_pages'] == 14
    assert rate['pages_per_second'] == 14 / 300
    assert rate['middle_seconds'] == (100 - 14) / (14 / 300)
    assert rate['fastest_seconds'] == (100 - 14) * 60 / 5
    assert rate['slowest_seconds'] is None  # No fabricated finite upper bound.
    assert rate['scenario_not_confidence_interval'] and not rate['all_history_complete']


def test_no_latest_progress_does_not_keep_counting_down_from_older_fast_work(queue):
    now = time.time()
    observed_bins(queue, [3, 3, 3, 3, 0], now)
    result = phase(queue, owner_start_epoch=now - 600, now_epoch=now, activity='running')['forecast']
    assert result['state'] == 'no_recent_progress'
    assert result['pages_per_second'] is result['middle_seconds'] is result['middle_finish_at_utc'] is None


@pytest.mark.parametrize('activity', ['stopped', 'draining', 'waiting_disk', 'configuration_reload_requested'])
def test_inactive_owner_preserves_counts_but_no_live_eta(queue, activity):
    with queue.db:
        add_gap(queue, DAY)
    page(queue)
    now = time.time()
    result = phase(queue, owner_start_epoch=now - 600, now_epoch=now, activity=activity)
    assert result['verified_pages'] == 1 and result['totals_known']
    assert result['forecast']['state'] == 'inactive' and result['forecast']['middle_seconds'] is None


def test_new_owner_cannot_borrow_previous_owner_window(queue):
    now = time.time()
    observed_bins(queue, [3, 3, 3, 3, 3], now)
    result = phase(queue, owner_start_epoch=now - 30, now_epoch=now, activity='running')['forecast']
    assert result['state'] == 'warming' and result['middle_seconds'] is None


def test_reported_page_completion_is_only_metadata_not_body_completion(queue):
    with queue.db:
        add_gap(queue, DAY)
    task, _, accepted_bodies = page(queue, total=30)
    result = phase(queue)
    assert accepted_bodies == 0 and result['phase_complete'] and result['progress_ratio'] == 1
    assert result['forecast']['state'] == 'phase_complete'
    assert result['metadata_only'] and not result['body_completion_claimed'] and not result['all_history_complete']
    assert queue.db.execute('SELECT count(*) FROM articles WHERE body_sha256 IS NOT NULL').fetchone()[0] == 0


def test_final_missing_page_failure_suspends_eta_until_its_own_response_succeeds(queue):
    with queue.db:
        add_gap(queue, DAY)
    for number in range(1, 12):
        page(queue, number=number, total=360)
    from test_news_cnyes_public_index import register
    failed = register(queue, page=12)
    fail_discovery(queue, failed, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    now = time.time()
    original = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (failed['url'],)).fetchone())
    result = phase(queue, owner_start_epoch=now - 600, now_epoch=now, activity='running')
    assert result['remaining_pages'] == 1
    assert result['forecast']['state'] == 'last_page_waiting'
    assert result['last_page_gate']['state'] == 'waiting'
    assert result['forecast']['middle_finish_at_utc'] is None
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (failed['url'],)).fetchone()) == original
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (now - 1, failed['url']))
        queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=? WHERE request_url=?', (now - 1, failed['url']))
    result = phase(queue, owner_start_epoch=now - 600, now_epoch=now, activity='running')
    assert result['last_page_gate']['state'] == 'rechecking'
    assert result['forecast']['state'] == 'last_page_rechecking'
    assert result['forecast']['middle_finish_at_utc'] is None
    page(queue, number=12, total=360)
    result = phase(queue, owner_start_epoch=now - 600, now_epoch=now, activity='running')
    assert result['phase_complete'] and result['forecast']['state'] == 'phase_complete'
    assert not result['body_completion_claimed'] and not result['all_history_complete']


def test_sole_missing_page_is_proved_even_when_it_is_not_the_highest_page(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, number=1, total=90)
    page(queue, number=3, total=90)
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['remaining_pages'] == 1 and result['last_page_gate']['state'] == 'ready'
    source = queue.registry.sources['cnyes']
    from downloader.news_fulltext_cnyes_index import request_url
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (time.time() + 90, request_url(source, DAY, 2)))
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['last_page_gate']['state'] == 'waiting'


def test_last_page_gate_keeps_later_matching_circuit_deadline(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, number=1, total=60)
    from test_news_cnyes_public_index import register
    job = register(queue, page=2)
    fail_discovery(queue, job, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    now = time.time()
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (now - 1, job['url']))
    result = phase(queue, now_epoch=now, activity='running')
    assert result['last_page_gate']['state'] == 'waiting'
    deadline = queue.db.execute('SELECT not_before FROM bulk_endpoint_cooldowns WHERE request_url=?', (job['url'],)).fetchone()[0]
    assert datetime.fromisoformat(result['last_page_gate']['next_probe_at_utc']).timestamp() == pytest.approx(deadline)


@pytest.mark.parametrize('change', ['missing', 'foreign_kind', 'foreign_provider', 'attempt_mismatch',
                                  'invalid_due', 'superseded'])
def test_unproven_last_page_job_does_not_invent_a_ready_state(queue, change):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, number=1, total=60)
    from downloader.news_fulltext_cnyes_index import request_url
    url = request_url(queue.registry.sources['cnyes'], DAY, 2)
    with queue.db:
        if change == 'missing':
            queue.db.execute('DELETE FROM discovery WHERE url=?', (url,))
        else:
            column, value = {
                'foreign_kind': ('kind', 'sitemap'), 'foreign_provider': ('provider', 'other'),
                'attempt_mismatch': ('attempts', 1), 'invalid_due': ('due', float('inf')),
                'superseded': ('superseded_by_windows_json', '[]'),
            }[change]
            queue.db.execute('UPDATE discovery SET ' + column + '=? WHERE url=?', (value, url))
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['last_page_gate']['state'] == 'unknown'
    assert result['forecast']['state'] == 'last_page_scope_unknown'


def test_failed_page_with_healthy_work_does_not_add_a_phase_wide_gate(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, number=1, total=90)
    from test_news_cnyes_public_index import register
    bad, healthy = register(queue, page=2), register(queue, page=3)
    fail_discovery(queue, bad, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['remaining_pages'] == 2 and 'last_page_gate' not in result
    assert healthy['url'] in [job['url'] for job in queue.discovery_candidates(queue.registry.sources['cnyes'], 20)]


def test_multiple_remaining_pages_keep_retry_evidence_in_phase_forecast(queue):
    with queue.db:
        add_gap(queue, DAY)
    for number in range(1, 13):
        page(queue, number=number, total=420)
    now = time.time()
    with queue.db:
        queue.db.execute('UPDATE cnyes_public_index_observations SET source_observed_at_utc=?',
                         (datetime.fromtimestamp(now - 30, timezone.utc).isoformat(),))
    from test_news_cnyes_public_index import register
    failed, healthy = register(queue, page=13), register(queue, page=14)
    fail_discovery(queue, failed, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = phase(queue, owner_start_epoch=now - 600, now_epoch=now, activity='running')
    assert result['remaining_pages'] == 2
    assert result['forecast']['state'] == 'partial_page_waiting'
    assert result['forecast']['middle_finish_at_utc'] is None
    assert result['partial_page_gate']['failed_pages'] == 1
    assert result['partial_page_gate']['next_probe_at_utc']
    assert healthy['url'] in [job['url'] for job in queue.discovery_candidates(queue.registry.sources['cnyes'], 20)]
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_multiple_page_failures_remain_independent_through_recheck_and_success(queue):
    with queue.db:
        add_gap(queue, DAY)
    for number in range(1, 13):
        page(queue, number=number, total=450)
    from test_news_cnyes_public_index import register
    first, second, healthy = (register(queue, page=n) for n in (13, 14, 15))
    for job in (first, second):
        fail_discovery(queue, job, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    now = time.time()
    result = phase(queue, owner_start_epoch=now - 600, now_epoch=now, activity='running')
    gate = result['partial_page_gate']
    assert gate['state'] == 'waiting' and gate['failed_pages'] == 2
    assert healthy['url'] in [job['url'] for job in queue.discovery_candidates(queue.registry.sources['cnyes'], 20)]
    retry = datetime.fromisoformat(gate['next_probe_at_utc']).timestamp() + 1
    result = phase(queue, owner_start_epoch=retry - 600, now_epoch=retry, activity='running')
    assert result['partial_page_gate']['state'] == 'rechecking'
    assert result['forecast']['state'] == 'partial_page_rechecking'
    page(queue, number=13, total=450)
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['partial_page_gate']['state'] == 'waiting'
    assert result['partial_page_gate']['failed_pages'] == 1
    page(queue, number=14, total=450)
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['remaining_pages'] == 1 and result['last_page_gate']['state'] == 'ready'
    page(queue, number=15, total=450)
    assert phase(queue, now_epoch=time.time(), activity='running')['phase_complete']


def test_partial_phase_reuses_older_endpoint_witness_when_per_page_key_is_absent(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, number=1, total=120)
    from test_news_cnyes_public_index import register
    from downloader.news_fulltext_cnyes_index import request_failure_scope_key
    job = register(queue, page=2)
    fail_discovery(queue, job, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    source = queue.registry.sources['cnyes']
    with queue.db:
        queue.db.execute('DELETE FROM queue_settings WHERE key=?', (request_failure_scope_key(source, job['url']),))
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['partial_page_gate']['state'] == 'waiting'
    assert result['partial_page_gate']['failed_pages'] == 1
    with queue.db:
        queue.db.execute('DELETE FROM queue_settings WHERE key=?', (request_failure_scope_key(source),))
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['partial_page_gate']['state'] == 'unknown'
    assert result['forecast']['state'] == 'partial_page_scope_unknown'


def test_partial_phase_does_not_adopt_failure_from_an_unplanned_source_date(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, number=1, total=120)
    from test_news_cnyes_public_index import register
    unrelated = register(queue, page=2, day=DAY - 1)
    fail_discovery(queue, unrelated, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['remaining_pages'] == 3
    assert result['partial_page_gate']['state'] == 'none_observed'
    assert result['partial_page_gate']['failed_pages'] == 0
    assert result['forecast']['state'] != 'partial_page_waiting'


@pytest.mark.parametrize('change', ['bad_witness', 'oversized_witness', 'changed_job', 'missing_job', 'too_many_witnesses'])
def test_partial_page_gate_does_not_invent_recovery_from_ambiguous_evidence(queue, change):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, number=1, total=120)
    from test_news_cnyes_public_index import register
    from downloader.news_fulltext_cnyes_index import request_failure_scope_key
    job = register(queue, page=2)
    fail_discovery(queue, job, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    source = queue.registry.sources['cnyes']
    key = request_failure_scope_key(source, job['url'])
    with queue.db:
        if change == 'bad_witness':
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('{}', key))
        elif change == 'oversized_witness':
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('x' * 8193, key))
        elif change == 'changed_job':
            queue.db.execute('UPDATE discovery SET due=due+10 WHERE url=?', (job['url'],))
        elif change == 'missing_job':
            queue.db.execute('DELETE FROM discovery WHERE url=?', (job['url'],))
        else:
            prefix = request_failure_scope_key(source) + ':'
            queue.db.executemany('INSERT INTO queue_settings VALUES (?,?)',
                                 ((prefix + f'{i:064x}', '{}') for i in range(129)))
    result = phase(queue, now_epoch=time.time(), activity='running')
    assert result['partial_page_gate']['state'] == 'unknown'
    assert result['forecast']['state'] == 'partial_page_scope_unknown'
    assert result['forecast']['middle_finish_at_utc'] is None


def test_index_rows_cache_reuses_unchanged_view_and_invalidates_on_owner_writes(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, total=61)
    source = queue.registry.sources['cnyes']
    cache = PublicIndexRowsCache()
    now = time.time()
    kwargs = dict(owner_start_epoch=now - 600, now_epoch=now, activity='waiting_disk')
    statements = []
    queue.db.set_trace_callback(statements.append)
    try:
        original = progress(queue.db, source, **kwargs)
        statements.clear()
        first = progress(queue.db, source, rows_cache=cache, **kwargs)
        second = progress(queue.db, source, rows_cache=cache, **kwargs)
        scans = [sql for sql in statements if sql.startswith('SELECT o.scope_url,count(*)')]
        assert first == second == original and len(scans) == 1
        page(queue, number=2, total=61)
        changed = progress(queue.db, source, rows_cache=cache, **kwargs)
        assert changed == progress(queue.db, source, **kwargs)
        assert changed['verified_pages'] == 2
        assert len([sql for sql in statements if sql.startswith('SELECT o.scope_url,count(*)')]) == 3
    finally:
        queue.db.set_trace_callback(None)


def test_index_rows_cache_does_not_leak_rolled_back_observations(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, total=61)
    source = queue.registry.sources['cnyes']
    cache = PublicIndexRowsCache()
    kwargs = dict(now_epoch=1_800_000_000, activity='waiting_disk')
    committed = progress(queue.db, source, rows_cache=cache, **kwargs)
    queue.db.execute('UPDATE cnyes_public_index_observations SET total=999 WHERE page=1')
    assert queue.db.in_transaction
    pending = progress(queue.db, source, rows_cache=cache, **kwargs)
    assert pending == progress(queue.db, source, **kwargs) and pending != committed
    queue.db.rollback()
    assert progress(queue.db, source, rows_cache=cache, **kwargs) == committed


def test_index_rows_cache_fences_other_connection_commit_and_read_snapshot(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, total=61)
    source = queue.registry.sources['cnyes']
    cache = PublicIndexRowsCache()
    kwargs = dict(now_epoch=1_800_000_000, activity='waiting_disk')
    original = progress(queue.db, source, rows_cache=cache, **kwargs)
    queue.db.execute('BEGIN')
    assert progress(queue.db, source, rows_cache=cache, **kwargs) == original
    with sqlite3.connect(queue.root / 'queue.sqlite3') as other:
        other.execute('UPDATE cnyes_public_index_observations SET total=99 WHERE page=1')
    assert progress(queue.db, source, rows_cache=cache, **kwargs) == original
    queue.db.commit()
    changed = progress(queue.db, source, rows_cache=cache, **kwargs)
    assert changed == progress(queue.db, source, **kwargs) and changed != original


def test_index_rows_cache_fences_schema_and_unusual_factories(tmp_path):
    db = sqlite3.connect(tmp_path / 'cache.sqlite')
    try:
        db.execute('CREATE TABLE items(value TEXT)')
        db.execute("INSERT INTO items VALUES ('original')")
        db.commit()
        cache = PublicIndexRowsCache()
        query = 'SELECT value FROM items'
        assert cache.fetch(db, query, ()) == [('original',)]
        db.execute('CREATE TEMP TABLE items(value TEXT)')
        db.execute("INSERT INTO temp.items VALUES ('temporary')")
        db.commit()
        assert cache.fetch(db, query, ()) == [('temporary',)]
        db.execute('DROP TABLE temp.items')
        assert cache.fetch(db, query, ()) == [('original',)]
        db.text_factory = bytes
        assert cache.fetch(db, query, ()) == [(b'original',)]
        db.text_factory = str
        assert cache.fetch(db, query, ()) == [('original',)]
        db.execute('DROP TABLE main.items')
        with pytest.raises(sqlite3.OperationalError, match='no such table'):
            cache.fetch(db, query, ())
    finally:
        db.close()


def test_index_rows_cache_falls_back_for_connection_wrappers(queue):
    with queue.db:
        add_gap(queue, DAY)
    page(queue, total=61)

    class ExecuteOnly:
        def execute(self, sql, parameters=()):
            return queue.db.execute(sql, parameters)

    proxy = ExecuteOnly()
    source = queue.registry.sources['cnyes']
    cache = PublicIndexRowsCache()
    kwargs = dict(now_epoch=1_800_000_000, activity='waiting_disk')
    assert progress(proxy, source, rows_cache=cache, **kwargs) == progress(proxy, source, **kwargs)
    assert progress(proxy, source, rows_cache=cache, **kwargs) == progress(proxy, source, **kwargs)


def test_index_rows_cache_does_not_assume_attached_database_revision(tmp_path):
    db = sqlite3.connect(tmp_path / 'main.sqlite')
    attached = tmp_path / 'attached.sqlite'
    try:
        db.execute('ATTACH DATABASE ? AS other', (str(attached),))
        db.execute('CREATE TABLE other.items(value TEXT)')
        db.execute("INSERT INTO other.items VALUES ('original')")
        db.commit()
        cache = PublicIndexRowsCache()
        query = 'SELECT value FROM other.items'
        assert cache.fetch(db, query, ()) == [('original',)]
        with sqlite3.connect(attached) as other:
            other.execute("UPDATE items SET value='changed'")
        assert cache.fetch(db, query, ()) == [('changed',)]
    finally:
        db.close()


def test_index_rows_cache_keeps_source_identity_and_forecast_live(queue):
    now = time.time()
    observed_bins(queue, [3, 3, 3, 3, 3], now)
    source = queue.registry.sources['cnyes']
    cache = PublicIndexRowsCache()
    first = progress(queue.db, source, rows_cache=cache,
        owner_start_epoch=now - 600, now_epoch=now, activity='running')
    assert first['manifest_phase']['forecast']['state'] == 'recent_measured'
    later = progress(queue.db, source, rows_cache=cache,
        owner_start_epoch=now - 600, now_epoch=now + 300, activity='running')
    assert later['manifest_phase']['forecast']['state'] == 'no_recent_progress'
    different = replace(source, authorization_fingerprint='different-authorization')
    assert progress(queue.db, different, rows_cache=cache, now_epoch=now) == progress(
        queue.db, different, now_epoch=now)
    assert progress(queue.db, different, rows_cache=cache, now_epoch=now)['verified_pages'] == 0
