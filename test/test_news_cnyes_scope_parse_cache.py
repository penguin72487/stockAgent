"""Cache immutable scope arithmetic, not live acquisition observations."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import re
from urllib.parse import parse_qs

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_bulk import wordpress_base
from downloader.news_fulltext_cnyes_index import gap_dates, progress
from downloader.artifact_io import sha256_file
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_queue import NewsQueue
from scripts.benchmark_news_status_projection import profile_current_status
from test_cnyes_news_bulk import BASE, cfg
from test_news_cnyes_public_index import DAY, queue
from test_news_cnyes_index_manifest_schedule import add_gap


@pytest.fixture(autouse=True)
def isolated_parse_cache():
    history._cached_scope_key.cache_clear()
    yield
    history._cached_scope_key.cache_clear()


def original_scope_key(url):
    # The original complete parser, including first-value and exception rules.
    query = parse_qs(url.split('?', 1)[1]) if '?' in url else {}
    if not {'startAt', 'endAt'} <= query.keys():
        return None
    low, high = int(query['startAt'][0]), int(query['endAt'][0])
    if high < low or query.get('limit') != ['30']:
        return None
    first, last = ((value + history.CNYES_QUERY_DAY_OFFSET_SECONDS) // 86400 for value in (low, high))
    return wordpress_base(url), first, last


@pytest.mark.parametrize('query', [
    '', 'limit=30', 'startAt=0', 'endAt=9', 'startAt=0&endAt=9',
    'limit=30&startAt=0&endAt=9', 'limit=30&startAt=-28800&endAt=57599',
    'limit=30&startAt=57599&endAt=57600', 'limit=30&startAt=9&endAt=0',
    'limit=30&startAt=0&endAt=0', 'limit=30&startAt=-100000&endAt=-90000',
    'limit=30&limit=30&startAt=0&endAt=9', 'limit=30&limit=31&startAt=0&endAt=9',
    'limit=&startAt=0&endAt=9', 'limit=30&startAt=&endAt=9',
    'limit=30&startAt=bad&endAt=9', 'limit=30&startAt=1.0&endAt=9',
    'limit=30&startAt=0&endAt=bad', 'limit=30&startAt=+10&endAt=20',
    'limit=30&startAt=%2B10&endAt=20', 'limit=30&startAt=0&startAt=99&endAt=9',
    'limit=30&startAt=0&endAt=9&endAt=bad', 'limit=30&startAt=0&endAt=9&page=7',
    'LIMIT=30&startAt=0&endAt=9', 'limit=030&startAt=0&endAt=9',
    'limit=30;startAt=0;endAt=9', 'limit=30&startAt=0&endAt=9#original',
    'limit=30&startAt=0&endAt=9&x=%00',
])
def test_values_none_and_original_exceptions_are_exact(query):
    url = BASE + ('?' + query if query else '')
    try:
        expected = original_scope_key(url)
    except Exception as error:
        for _ in range(2):
            with pytest.raises(type(error), match='^' + re.escape(str(error)) + '$'):
                history.scope_key(url)
        assert history._cached_scope_key.cache_info().currsize == 0
    else:
        assert history.scope_key(url) == history.scope_key(url) == expected
        assert history._cached_scope_key.cache_info().hits == 1


@pytest.mark.parametrize('value', [None, 1, {}, [], ['?'], b'url'])
def test_unhashable_or_nonstring_inputs_keep_original_values_or_errors(value):
    try:
        expected = original_scope_key(value)
    except Exception as old:
        with pytest.raises(type(old)) as new:
            history.scope_key(value)
        assert str(new.value) == str(old)
    else:
        assert history.scope_key(value) == expected
    assert history._cached_scope_key.cache_info().currsize == 0


def test_cached_tuple_is_immutable_and_date_offset_is_part_of_key(monkeypatch):
    url = BASE + '?limit=30&startAt=-28800&endAt=57599'
    assert history.scope_key(url) == (BASE, 0, 0)
    monkeypatch.setattr(history, 'CNYES_QUERY_DAY_OFFSET_SECONDS', 0)
    assert history.scope_key(url) == original_scope_key(url) == (BASE, -1, 0)
    assert history._cached_scope_key.cache_info().misses == 2
    monkeypatch.setattr(history, 'CNYES_QUERY_DAY_OFFSET_SECONDS', 28800)
    assert history.scope_key(url) == (BASE, 0, 0)
    assert history._cached_scope_key.cache_info().hits == 1
    with pytest.raises(TypeError):
        history.scope_key(url)[1] = 999


@pytest.mark.parametrize('offset', [True, 28800.0])
def test_unusual_offset_uses_original_arithmetic_without_caching(monkeypatch, offset):
    monkeypatch.setattr(history, 'CNYES_QUERY_DAY_OFFSET_SECONDS', offset)
    url = BASE + '?limit=30&startAt=-28800&endAt=57599'
    assert history.scope_key(url) == original_scope_key(url)
    assert history._cached_scope_key.cache_info().currsize == 0


def test_oversized_input_is_not_retained_but_is_not_rejected_or_truncated():
    url = BASE + '?limit=30&startAt=0&endAt=9&x=' + 'x' * history.SCOPE_KEY_CACHE_MAX_URL_CHARS
    assert history.scope_key(url) == original_scope_key(url)
    assert history._cached_scope_key.cache_info().currsize == 0


def test_capacity_is_bounded_and_eviction_does_not_change_values():
    first = BASE + '?limit=30&startAt=0&endAt=9'
    expected = history.scope_key(first)
    for number in range(1, history.SCOPE_KEY_CACHE_SIZE + 20):
        assert history.scope_key(BASE + f'?limit=30&startAt={number}&endAt={number}') == (BASE, 0, 0)
    assert history._cached_scope_key.cache_info().currsize == history.SCOPE_KEY_CACHE_SIZE
    missed = history._cached_scope_key.cache_info().misses
    assert history.scope_key(first) == expected
    assert history._cached_scope_key.cache_info().misses == missed + 1


def test_parallel_readers_return_same_values_and_cannot_mutate_source_evidence():
    urls = [BASE + f'?limit=30&startAt={n * 86400 - 28800}&endAt={n * 86400 + 57599}' for n in range(-3, 20)]
    with ThreadPoolExecutor(max_workers=4) as executor:
        actual = list(executor.map(history.scope_key, urls * 8))
    assert actual == [original_scope_key(url) for url in urls * 8]
    assert history._cached_scope_key.cache_info().currsize == len(urls)


def test_gap_rows_and_config_are_fresh_after_cache_warms(queue):
    source = queue.registry.sources['cnyes']
    with queue.db:
        add_gap(queue, DAY)
    changes = queue.db.total_changes
    assert gap_dates(queue.db, source) == {DAY}
    assert gap_dates(queue.db, source) == {DAY}
    assert queue.db.total_changes == changes
    assert gap_dates(queue.db, replace(source, cnyes_apis=())) == set()
    with queue.db:
        add_gap(queue, DAY - 1)
    assert gap_dates(queue.db, source) == {DAY, DAY - 1}
    queue.db.execute('DELETE FROM bulk_window_gaps')
    assert gap_dates(queue.db, source) == set()
    queue.db.rollback()
    assert gap_dates(queue.db, source) == {DAY, DAY - 1}


def test_manifest_phase_reads_new_and_removed_rows_not_a_cached_denominator(queue):
    source = queue.registry.sources['cnyes']
    with queue.db:
        add_gap(queue, DAY)
    assert progress(queue.db, source)['manifest_phase']['planned_source_dates'] == 1
    with queue.db:
        add_gap(queue, DAY - 1)
    assert progress(queue.db, source)['manifest_phase']['planned_source_dates'] == 2
    with queue.db:
        queue.db.execute('DELETE FROM bulk_window_gaps')
    phase = progress(queue.db, source)['manifest_phase']
    assert phase['planned_source_dates'] == 0 and not phase['totals_known']
    assert phase['forecast']['state'] == 'unknown_denominator' and not phase['all_history_complete']


def test_whole_status_replay_byte_equal_and_no_live_writes_or_http(queue, monkeypatch):
    with queue.db:
        add_gap(queue, DAY)
        add_gap(queue, DAY - 1)
        queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1', 'date': '2020-01-01'}])
    queue.status(activity='fixture')
    before = sha256_file(queue.root / 'status.json')
    changes = queue.db.total_changes
    rows = [tuple(r) for r in queue.db.execute('SELECT * FROM bulk_window_gaps ORDER BY scope_url')]
    def no_owner(*args, **kwargs):
        raise AssertionError('must use the existing read-only benchmark')
    monkeypatch.setattr(NewsQueue, '__init__', no_owner)
    monkeypatch.setattr(NewsHTTP, '__init__', no_owner)
    result = profile_current_status(queue.root, queue.registry, 3, compare_scope_cache=True)
    assert result['accepted'] and result['deterministic_projection_sha256']['legacy'] == result['deterministic_projection_sha256']['summary']
    assert result['scope_parse_cache']['first_summary_trial_includes_cold_cache']
    assert not result['scope_parse_cache']['source_rows_cached']
    assert result['measurements']['summary'][0]['scope_cache_before']['currsize'] == 0
    assert result['measurements']['summary'][0]['scope_cache_after']['misses'] == 2
    assert result['publisher_requests'] == 0 and not result['source_states_mutated'] and not result['status_published']
    assert sha256_file(queue.root / 'status.json') == before
    assert queue.db.total_changes == changes
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM bulk_window_gaps ORDER BY scope_url')] == rows
    with pytest.raises(ValueError, match='one_projection_comparison'):
        profile_current_status(queue.root, queue.registry, 3, compare_scope_cache=True, compare_discovery_summary=True)
