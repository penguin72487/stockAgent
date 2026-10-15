"""Finite metadata-phase counts/ETA never become full-text completeness."""
from datetime import datetime, timezone
import time

import pytest

from downloader.news_fulltext_cnyes_index import progress
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
