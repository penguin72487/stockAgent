"""Measured source-date boundaries, durable repair and refusal preservation."""
from dataclasses import replace
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from downloader.news_fulltext import accept_discovery, discovery_fingerprint
from downloader.news_fulltext_bulk import (
    CNYES_QUERY_DAY_OFFSET_SECONDS, CNYES_WINDOW_PARTITION_CONTRACT_VERSION,
    cnyes_records, cnyes_scope, cnyes_url, cnyes_window_partitions,
)
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import CNYES_WINDOW_DISPATCH_CONTRACT_VERSION, NewsQueue, verify_bodies
from test_cnyes_news_bulk import BASE, cfg, payload, registry_http, response


def bounds(url):
    query = parse_qs(urlsplit(url).query)
    return int(query['startAt'][0]), int(query['endAt'][0])


def source_dates(url):
    low, high = bounds(url)
    return ((low + 28800) // 86400, (high + 28800) // 86400)


@pytest.mark.parametrize('low,high,pages', [
    (0, 86399, 31), (57600, 143999, 31), (57599, 57600, 31),
    (0, 2 * 86400 - 1, 300), (1234, 731 * 86400 - 2345, 10000),
    (-86400, -1, 31), (0, 86399, 30), (1, 2, 31),
])
def test_partition_preserves_requested_seconds_and_all_source_dates(low, high, pages):
    parent = cnyes_url(BASE, start=low, end=high)
    children = cnyes_window_partitions(parent, pages)
    first, last = source_dates(parent)
    if pages <= 30 or first == last:
        assert children == []
        return
    assert len(children) == min(last - first + 1, (pages + 29) // 30)
    intervals = [bounds(child) for child in children]
    assert intervals[0][0] == low and intervals[-1][1] == high
    assert all(left <= right for left, right in intervals)
    assert all(left[1] + 1 == right[0] for left, right in zip(intervals, intervals[1:]))
    assert all((start + 28800) % 86400 == 0 for start, _ in intervals[1:])
    days = [source_dates(child) for child in children]
    assert days[0][0] == first and days[-1][1] == last
    assert all(left[1] + 1 == right[0] for left, right in zip(days, days[1:]))
    assert all(int(parse_qs(urlsplit(child).query)['page'][0]) == 1 for child in children)


def test_versioned_calendar_scheduler_does_not_change_body_or_discovery_abi(monkeypatch):
    assert CNYES_WINDOW_PARTITION_CONTRACT_VERSION == 2
    assert CNYES_WINDOW_DISPATCH_CONTRACT_VERSION == 3
    assert CNYES_QUERY_DAY_OFFSET_SECONDS == 28800
    from downloader.news_fulltext import load_config
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['cnyes']
    monkeypatch.setattr('downloader.news_fulltext_bulk.CNYES_WINDOW_PARTITION_CONTRACT_VERSION', 3)
    _, changed, _ = load_config(Path('configs/news_fulltext.json'))
    assert changed.sources['cnyes'].data_fingerprint == source.data_fingerprint
    assert changed.sources['cnyes'].cnyes_bulk_data_fingerprint == source.cnyes_bulk_data_fingerprint
    assert discovery_fingerprint(changed.sources['cnyes']) == discovery_fingerprint(source)


@pytest.fixture
def queue(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    value = NewsQueue(tmp_path, registry)
    yield value
    value.close()


def seed_legacy(queue, *, first_page=True, failure='discovery_http_429', extra_child=False):
    first = cnyes_url(BASE, start=1388534400, end=1388620799)
    second = cnyes_url(BASE, 2, start=1388534400, end=1388620799)
    old31 = first.replace('page=1&', 'page=31&')
    scope = cnyes_scope(first)
    children = cnyes_window_partitions(first, 34)
    with queue.db:
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_window_dispatch_v3:cnyes'")
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',1000,34,'real-old-source-clock')", (scope,))
        queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','cnyes_single_day_page_cap','real-old-gap-clock')", (scope,))
        if first_page:
            queue.db.execute("INSERT INTO bulk_page_evidence VALUES (?,1,1000,?,'real-old-page-clock')", (scope, 'a' * 64))
        queue.db.executemany("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,last_checked_at_utc) "
            "VALUES (?,'cnyes_news','cnyes',?,?,?,?,'old-response-clock')", [
                (first, 9e9, None, 0, 'old-validator'),
                (second, 5e9, failure, 4, 'failed-validator'),
                (old31, 8e9, 'discovery_http_422', 1, 'invalid-page-validator')])
        if extra_child:
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts) "
                "VALUES (?,'cnyes_news','cnyes',6e9,'discovery_http_403',2)", (children[0],))
        queue.db.execute("INSERT INTO bulk_endpoint_cooldowns VALUES ('cnyes',?,?,7e9,'bulk_all_items_unusable',?)",
                         (BASE, queue.registry.sources['cnyes'].data_fingerprint, second))
    return first, second, old31, scope, children


@pytest.mark.parametrize('failure', ['discovery_http_429', 'discovery_http_403', 'transport_retry', 'bulk_all_items_unusable', None])
def test_retained_first_page_splits_directly_no_parent_refetch_and_no_refusal_reset(queue, failure):
    first, second, old31, scope, children = seed_legacy(queue, failure=failure)
    columns = 'url,due,error_code,attempts,etag,last_checked_at_utc'
    prior = [tuple(row) for row in queue.db.execute('SELECT ' + columns + ' FROM discovery ORDER BY url')]
    old_pages = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_page_evidence')]
    old_gaps = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_window_gaps')]
    circuits = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns')]
    queue.migrate_cnyes_window_dispatch()
    assert [tuple(queue.db.execute('SELECT ' + columns + ' FROM discovery WHERE url=?', (row[0],)).fetchone()) for row in prior] == prior
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_page_evidence')] == old_pages
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_window_gaps')] == old_gaps
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns')] == circuits
    assert json.loads(queue.db.execute('SELECT child_urls_json FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()[0]) == children
    for child in children:
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (child,)).fetchone()[0] == (5e9 if failure else 0)
    assert all(json.loads(queue.db.execute('SELECT superseded_by_windows_json FROM discovery WHERE url=?', (url,)).fetchone()[0]) == children for url in (first, second, old31))
    changes = queue.db.total_changes
    queue.migrate_cnyes_window_dispatch()
    assert queue.db.total_changes == changes
    assert not queue.status()['historical_completeness_verified']


def test_preexisting_failed_child_deadline_and_error_are_not_overwritten(queue):
    *_, children = seed_legacy(queue, extra_child=True)
    queue.migrate_cnyes_window_dispatch()
    row = queue.db.execute('SELECT due,error_code,attempts FROM discovery WHERE url=?', (children[0],)).fetchone()
    assert tuple(row) == (6e9, 'discovery_http_403', 2)


def test_missing_source_page_evidence_requires_real_envelope_not_invented_split(queue):
    first, _, _, scope, _ = seed_legacy(queue, first_page=False)
    queue.migrate_cnyes_window_dispatch()
    assert queue.db.execute('SELECT count(*) FROM bulk_window_splits').fetchone()[0] == 0
    assert queue.db.execute('SELECT etag FROM discovery WHERE url=?', (first,)).fetchone()[0] is None
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (first,)).fetchone()[0] == 5e9
    assert queue.db.execute('SELECT observed_at_utc FROM bulk_window_gaps WHERE scope_url=?', (scope,)).fetchone()[0] == 'real-old-gap-clock'


def test_html_capture_change_does_not_repeat_bulk_calendar_migration(queue):
    seed_legacy(queue)
    queue.migrate_cnyes_window_dispatch()
    queue.registry.sources['cnyes'] = replace(queue.registry.sources['cnyes'], data_fingerprint='new-html-only-capture')
    changes = queue.db.total_changes
    queue.migrate_cnyes_window_dispatch()
    assert queue.db.total_changes == changes


def test_live_UTC_day_split_stores_body_and_retains_old_cap_evidence(cfg, tmp_path):
    registry, http = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    first = cnyes_url(BASE, start=1388534400, end=1388620799)
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (first,))
            queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','cnyes_single_day_page_cap','original-clock')", (cnyes_scope(first),))
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (first,)).fetchone())
        source_response = response(payload(ids=tuple(range(1, 31)), total=901), first)
        records, errors = cnyes_records(source_response, http, registry.sources['cnyes'])
        assert accept_discovery(queue, task, source_response, [(row['requested_url'], row['title'], row['metadata_date_raw']) for row in records], records, errors) == 30
        assert verify_bodies(tmp_path)['accepted']
        assert queue.db.execute('SELECT observed_at_utc FROM bulk_window_gaps').fetchone()[0] == 'original-clock'
        assert queue.db.execute('SELECT count(*) FROM discovery WHERE superseded_by_windows_json IS NULL').fetchone()[0] == 2
        assert queue.db.execute('SELECT parser_fingerprint FROM discovery WHERE url=?', (first,)).fetchone()[0] == discovery_fingerprint(registry.sources['cnyes'])
    finally:
        queue.close()


def test_repair_never_changes_an_existing_different_partition_tree(queue):
    first, _, _, scope, _ = seed_legacy(queue)
    with queue.db:
        queue.db.execute("INSERT INTO bulk_window_splits VALUES (?,'cnyes','[\"retained-other-child\"]','old-tree-clock')", (scope,))
        with pytest.raises(NewsError, match='cnyes_window_partition_conflict'):
            queue.partition_cnyes_window(first, 34, queue.registry.sources['cnyes'])
    assert queue.db.execute('SELECT observed_at_utc FROM bulk_window_splits').fetchone()[0] == 'old-tree-clock'
