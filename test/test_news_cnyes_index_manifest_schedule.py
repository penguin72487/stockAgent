"""Inspect each actual gap date once before deep metadata-only pagination.

The independent all-list must not change current-release/bulk-body priority,
retry deadlines, permissions, source day bounds, or any stored body evidence.
"""
from dataclasses import replace
import time

import pytest

from downloader.news_fulltext_cnyes_index import discovery_priority, request_url, seed_gap_dates
from downloader.news_fulltext_bulk import cnyes_scope, cnyes_url
from test_cnyes_news_bulk import BASE, cfg
from test_news_cnyes_public_index import DAY, accept, index_payload, queue


def add_gap(queue, day):
    low = day * 86400 - 28800
    scope = cnyes_scope(cnyes_url(BASE, start=low, end=low + 86399))
    queue.db.execute("INSERT OR IGNORE INTO bulk_window_gaps VALUES (?,'cnyes',"
                     "'cnyes_single_day_page_cap','actual-source-clock')", (scope,))


def actual_candidates(queue):
    return [dict(row) for row in queue.discovery_candidates(queue.registry.sources['cnyes'], 64)]


def test_unseen_older_source_date_precedes_newer_deep_index_pagination(queue):
    source = queue.registry.sources['cnyes']
    with queue.db:
        add_gap(queue, DAY)
        add_gap(queue, DAY - 1)
        seed_gap_dates(queue, source)
    # A real metadata-only page schedules its advertised next page.
    accept(queue, index_payload(ids=range(1, 31), total=10844))
    rows = actual_candidates(queue)
    assert [row['url'] for row in rows] == [request_url(source, DAY - 1), request_url(source, DAY, 2)]


def test_manifest_reprioritization_preserves_all_nonpriority_fields(queue):
    source = queue.registry.sources['cnyes']
    with queue.db:
        add_gap(queue, DAY)
        seed_gap_dates(queue, source)
        url = request_url(source, DAY)
        queue.db.execute('UPDATE discovery SET priority=999,due=?,attempts=7,error_code=?,etag=?,last_modified=? WHERE url=?',
                         (time.time() + 3600, 'http_429', 'original-etag', 'original-last-modified', url))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    queue._cnyes_index_seed_after = 0
    with queue.db:
        seed_gap_dates(queue, source)
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert {key: value for key, value in before.items() if key != 'priority'} == {
        key: value for key, value in after.items() if key != 'priority'}
    assert actual_candidates(queue) == []


def test_latest_bulk_and_current_release_priority_not_displaced_by_manifest(queue):
    source = queue.registry.sources['cnyes']
    with queue.db:
        add_gap(queue, DAY - 1)
        seed_gap_dates(queue, source)
        queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'cnyes_news','cnyes',0)",
                         (cnyes_url(BASE),))
    assert actual_candidates(queue)[0]['kind'] == 'cnyes_news'


def test_successful_page_deadline_raw_clocks_and_no_body_acceptance_unchanged(queue):
    task, _, count = accept(queue, index_payload(ids=range(1, 31), total=10844))
    assert count == 0
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
    heads = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    evidence = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_observations')]
    with queue.db:
        add_gap(queue, DAY)
        seed_gap_dates(queue, queue.registry.sources['cnyes'])
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (task['url'],)).fetchone())
    assert {key: value for key, value in before.items() if key != 'priority'} == {
        key: value for key, value in after.items() if key != 'priority'}
    assert after['due'] > time.time() + 86400
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == heads
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_observations')] == evidence
    assert [row['url'] for row in actual_candidates(queue)] == [
        request_url(queue.registry.sources['cnyes'], DAY, 2)]


def test_repeated_manifest_seed_is_exact_row_and_durable_evidence_idempotent(queue):
    source = queue.registry.sources['cnyes']
    with queue.db:
        add_gap(queue, DAY)
        add_gap(queue, DAY - 1)
        seed_gap_dates(queue, source)
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    for _ in range(3):
        queue._cnyes_index_seed_after = 0
        with queue.db:
            seed_gap_dates(queue, source)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


@pytest.mark.parametrize('disabled', ['endpoint', 'source'])
def test_disabled_manifest_does_not_migrate_or_insert_jobs(queue, disabled):
    source = queue.registry.sources['cnyes']
    with queue.db:
        add_gap(queue, DAY)
        seed_gap_dates(queue, source)
        queue.db.execute('UPDATE discovery SET priority=999')
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    source = replace(source, **({'cnyes_index_api': ''} if disabled == 'endpoint' else {'allowed': False}))
    queue._cnyes_index_seed_after = 0
    with queue.db:
        seed_gap_dates(queue, source)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


@pytest.mark.parametrize('collision', ['foreign_provider', 'foreign_kind'])
def test_manifest_URL_collision_preserves_entire_foreign_work(queue, collision):
    source = queue.registry.sources['cnyes']
    url = request_url(source, DAY)
    with queue.db:
        add_gap(queue, DAY)
        queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,?,?,999)",
                         (url, 'index' if collision == 'foreign_provider' else 'sitemap',
                          'other' if collision == 'foreign_provider' else 'cnyes'))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        seed_gap_dates(queue, source)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_manifest_dates_newest_first_pagination_newest_first_without_unadvertised_pages(queue):
    source = queue.registry.sources['cnyes']
    with queue.db:
        for day in (DAY - 1000, DAY - 1, DAY):
            add_gap(queue, day)
        seed_gap_dates(queue, source)
    assert [row['url'] for row in actual_candidates(queue)] == [
        request_url(source, day) for day in (DAY, DAY - 1, DAY - 1000)]
    assert all(discovery_priority(day) < discovery_priority(DAY, 2) for day in (DAY, DAY - 1, DAY - 1000))
    assert discovery_priority(DAY, 2) < discovery_priority(DAY - 1, 2)
    assert queue.db.execute('SELECT count(*) FROM discovery').fetchone()[0] == 3
