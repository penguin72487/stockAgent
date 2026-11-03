"""A partial large review must not monopolize every owner admission slice."""
from collections import Counter
from copy import deepcopy
import errno
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from downloader import news_fulltext_repairs as repair
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_reviewed_repairs import queue, failure, submit, head


def two_reviews(owner):
    groups = [[f'https://publisher.test/story/{group}-{number}' for number in range(4)]
        for group in ('large-a', 'large-b')]
    before = {url:failure(owner, url) for group in groups for url in group}
    submitted = [submit(owner, group) for group in groups]
    return before, submitted


def admissions(owner):
    return dict(owner.db.execute('SELECT request_id,count(*) FROM reviewed_repair_items GROUP BY request_id'))


def test_partial_reviews_rotate_between_slices_without_touching_financial_or_source_facts(queue, monkeypatch):
    before, submitted = two_reviews(queue)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *args, **kwargs: pytest.fail('admission must not create HTTP'))
    assert repair.consume_repairs(queue, limit=1)['outcomes'] == {'enqueued':1}
    assert repair.consume_repairs(queue, limit=1)['outcomes'] == {'enqueued':1}
    assert admissions(queue) == {row['request_id']:1 for row in submitted}
    outcomes = Counter(enqueued=2)
    for _ in range(6):
        outcomes.update(repair.consume_repairs(queue, limit=1)['outcomes'])
    assert outcomes == {'enqueued':8}
    for url, previous in before.items():
        assert head(queue, url) == dict(previous, state='retry')
    assert all(not Path(row['request_path']).exists() for row in submitted)
    assert repair.consume_repairs(queue, limit=1)['processed'] == 0


def test_round_robin_survives_owner_restart(config, tmp_path):
    registry = PolicyRegistry(config, base=tmp_path)
    owner = NewsQueue(tmp_path, registry)
    try:
        before, submitted = two_reviews(owner)
        assert repair.consume_repairs(owner, limit=1)['processed'] == 1
        first = admissions(owner)
        owner.close()
        owner = NewsQueue(tmp_path, registry)
        assert repair.consume_repairs(owner, limit=1)['processed'] == 1
        assert admissions(owner) == {row['request_id']:1 for row in submitted}
        assert len(first) == 1
        for url, previous in before.items():
            current = head(owner, url)
            assert current == dict(previous, state=current['state'])
            assert current['state'] in {previous['state'], 'retry'}
    finally:
        owner.close()


def test_stop_does_not_spend_the_next_review_turn(queue):
    _, submitted = two_reviews(queue)
    repair.consume_repairs(queue, limit=1)
    before = dict(queue.db.execute('SELECT key,value FROM queue_settings'))
    stopped = threading.Event()
    stopped.set()
    assert repair.consume_repairs(queue, limit=1, stop=stopped)['processed'] == 0
    assert dict(queue.db.execute('SELECT key,value FROM queue_settings')) == before
    repair.consume_repairs(queue, limit=1)
    assert admissions(queue) == {row['request_id']:1 for row in submitted}


@pytest.mark.parametrize('slow_step', ['discovery', 'validation'])
def test_slow_inbox_still_admits_one_target_per_slice_and_rotates(queue, monkeypatch, slow_step):
    before, submitted = two_reviews(queue)
    now = [0.0]
    monkeypatch.setattr(repair, 'time', SimpleNamespace(monotonic=lambda: now[0], time=time.time))
    name = '_pending' if slow_step == 'discovery' else '_load_for_admission'
    original = getattr(repair, name)

    def delayed(*args):
        result = original(*args)
        now[0] += 1.0
        return result

    monkeypatch.setattr(repair, name, delayed)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *args, **kwargs: pytest.fail('admission must not create HTTP'))
    for _ in range(2):
        assert repair.consume_repairs(queue, limit=1, max_seconds=.05)['outcomes'] == {'enqueued': 1}
    assert admissions(queue) == {row['request_id']: 1 for row in submitted}
    for url, previous in before.items():
        current = head(queue, url)
        assert current == dict(previous, state=current['state'])
        assert current['state'] in {previous['state'], 'retry'}


def test_stop_during_slow_discovery_does_not_admit_or_spend_turn(queue, monkeypatch):
    before, _ = two_reviews(queue)
    settings = dict(queue.db.execute('SELECT key,value FROM queue_settings'))
    stopped = threading.Event()
    original = repair._pending

    def interrupted(directory):
        result = original(directory)
        stopped.set()
        return result

    monkeypatch.setattr(repair, '_pending', interrupted)
    assert repair.consume_repairs(queue, limit=1, stop=stopped)['processed'] == 0
    assert dict(queue.db.execute('SELECT key,value FROM queue_settings')) == settings
    assert not queue.db.execute("SELECT 1 FROM sqlite_master WHERE name='reviewed_repair_items'").fetchone()
    assert all(head(queue, url) == previous for url, previous in before.items())


def test_transient_request_read_failure_is_deferred_and_later_review_runs(queue, monkeypatch):
    before, submitted = two_reviews(queue)
    paths = sorted(Path(row['request_path']) for row in submitted)
    original = repair._load_for_admission
    unavailable = [True]

    def transient(owner, path):
        if path == paths[0] and unavailable[0]:
            raise OSError(errno.EIO, 'temporary request read error')
        return original(owner, path)

    monkeypatch.setattr(repair, '_load_for_admission', transient)
    assert repair.consume_repairs(queue, limit=1, max_seconds=.1)['outcomes'] == {'request_io_deferred': 1}
    assert paths[0].exists() and all(head(queue, url) == previous for url, previous in before.items())
    assert repair.consume_repairs(queue, limit=1, max_seconds=.1)['outcomes'] == {'enqueued': 1}
    unavailable[0] = False
    assert repair.consume_repairs(queue, limit=1, max_seconds=.1)['outcomes'] == {'enqueued': 1}
    assert admissions(queue) == {row['request_id']: 1 for row in submitted}


def test_independent_providers_both_reach_the_original_dispatcher(config, tmp_path):
    other = deepcopy(config['providers'][0])
    other.update(id='other', domains=['other.test'])
    config['providers'].append(other)
    owner = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        submitted = []
        for host in ('publisher.test', 'other.test'):
            urls = [f'https://{host}/story/{number}' for number in range(16)]
            for url in urls:
                failure(owner, url)
            submitted.append(submit(owner, urls))
        repair.consume_repairs(owner, limit=1)
        repair.consume_repairs(owner, limit=1)
        assert admissions(owner) == {row['request_id']:1 for row in submitted}
        providers = {row[0] for row in owner.db.execute("SELECT provider FROM articles WHERE state='retry'")}
        assert providers == {'publisher', 'other'}
    finally:
        owner.close()


def test_admission_failure_rolls_back_cursor_with_head_and_once_only_record(queue, monkeypatch):
    before, submitted = two_reviews(queue)
    settings = dict(queue.db.execute('SELECT key,value FROM queue_settings'))
    counter = queue.counter
    def interrupted(*args, **kwargs):
        raise RuntimeError('simulated interruption')
    monkeypatch.setattr(queue, 'counter', interrupted)
    with pytest.raises(RuntimeError, match='simulated interruption'):
        repair.consume_repairs(queue, limit=1)
    assert not queue.reviewed_repair_pending and not queue.reviewed_repair_continuation_pending
    assert dict(queue.db.execute('SELECT key,value FROM queue_settings')) == settings
    assert admissions(queue) == {}
    assert all(head(queue, url) == previous for url,previous in before.items())
    monkeypatch.setattr(queue, 'counter', counter)
    repair.consume_repairs(queue, limit=1)
    repair.consume_repairs(queue, limit=1)
    assert admissions(queue) == {row['request_id']:1 for row in submitted}


@pytest.mark.parametrize('cursor', ['../outside.json', 'not-a-request', 'x'*1024])
def test_invalid_scheduling_cursor_is_never_a_path_or_permission(queue, cursor, monkeypatch):
    before = failure(queue)
    result = submit(queue)
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (repair.ROUND_ROBIN_CURSOR_KEY, cursor))
    # Cursor validation is independent of a busy host's 50 ms I/O slice.
    with monkeypatch.context() as clock:
        clock.setattr(repair.time, 'monotonic', lambda: 0.0)
        assert repair.consume_repairs(queue, limit=1)['outcomes'] == {'enqueued':1}
    assert head(queue) == dict(before, state='retry')
    assert admissions(queue) == {result['request_id']:1}


def test_archived_cursor_wraps_to_remaining_work_without_repeating_admission(queue):
    _, submitted = two_reviews(queue)
    for _ in range(8):
        repair.consume_repairs(queue, limit=1)
    assert admissions(queue) == {row['request_id']:4 for row in submitted}
    for row in submitted:
        assert not Path(row['request_path']).exists()
        path = queue.root/'reviewed_repairs'/'processed'/(row['request_id']+'.outcome.json')
        proof = json.loads(path.read_bytes())
        assert proof['admitted_targets'] == 4
        assert not proof['accepted_as_bodies']
    assert repair.consume_repairs(queue)['processed'] == 0


def test_immutable_shape_validation_is_reused_but_every_file_read_is_rechecked(queue, monkeypatch):
    urls = [f'https://publisher.test/story/cached-{i}' for i in range(3)]
    before = {url:failure(queue, url) for url in urls}
    result = submit(queue, urls)
    counts = Counter()
    read, validate = repair._read_private_request, repair._validate_request
    def counted_read(path):
        counts['private_read'] += 1
        return read(path)
    def counted_validation(request):
        counts['shape_validation'] += 1
        return validate(request)
    monkeypatch.setattr(repair, '_read_private_request', counted_read)
    monkeypatch.setattr(repair, '_validate_request', counted_validation)
    for _ in urls:
        assert repair.consume_repairs(queue, limit=1, max_seconds=.1)['outcomes'] == {'enqueued':1}
    assert counts == {'private_read':3, 'shape_validation':1}
    assert all(head(queue, url) == dict(previous, state='retry') for url,previous in before.items())
    assert not queue.reviewed_repair_pending and not queue.reviewed_repair_continuation_pending


@pytest.mark.parametrize('change', ['digest', 'public', 'symlink'])
def test_cached_request_never_bypasses_current_private_file_integrity(queue, monkeypatch, change):
    urls = ['https://publisher.test/story/cache-first', 'https://publisher.test/story/cache-second']
    for url in urls:
        failure(queue, url)
    result = submit(queue, urls)
    assert repair.consume_repairs(queue, limit=1, max_seconds=.1)['outcomes'] == {'enqueued':1}
    path = Path(result['request_path'])
    previous = head(queue, urls[1])
    if change == 'digest':
        path.write_bytes(b'{}')
    elif change == 'public':
        path.chmod(0o644)
    else:
        outside = queue.root/'private-original'
        outside.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(outside)
    assert repair.consume_repairs(queue, max_seconds=.1)['outcomes'] == {'invalid_request':1}
    assert head(queue, urls[1]) == previous
    assert not queue.reviewed_repair_pending and not queue.reviewed_repair_continuation_pending


def test_clock_rollback_revalidates_cached_observation_times(queue, monkeypatch):
    urls = ['https://publisher.test/story/clock-first', 'https://publisher.test/story/clock-second']
    for url in urls:
        failure(queue, url)
    result = submit(queue, urls)
    created = repair._clock(repair._load(Path(result['request_path']))['created_at_utc'])
    assert repair.consume_repairs(queue, limit=1, max_seconds=.1)['outcomes'] == {'enqueued':1}
    previous = head(queue, urls[1])
    monkeypatch.setattr(repair.time, 'time', lambda: created-1)
    assert repair.consume_repairs(queue, max_seconds=.1)['outcomes'] == {'invalid_request':1}
    assert head(queue, urls[1]) == previous


@pytest.mark.parametrize('change', ['lease', 'deadline', 'body', 'head', 'source'])
def test_cached_request_rechecks_current_heads_and_source_on_every_target(queue, monkeypatch, change):
    urls = ['https://publisher.test/story/guard-first', 'https://publisher.test/story/guard-second']
    for url in urls:
        failure(queue, url)
    submit(queue, urls)
    assert repair.consume_repairs(queue, limit=1, max_seconds=.1)['outcomes'] == {'enqueued':1}
    with queue.db:
        if change == 'lease':
            queue.db.execute('UPDATE articles SET lease_owner=?,lease_until=? WHERE url=?', ('other', time.time()+60, urls[1]))
        elif change == 'deadline':
            queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time()+60, urls[1]))
        elif change == 'body':
            queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a'*64, urls[1]))
        elif change == 'head':
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (urls[1],))
    if change == 'source':
        current_source = repair._source
        def changed_source(*args):
            source, identity = current_source(*args)
            return source, dict(identity, parser_fingerprint='f'*64)
        monkeypatch.setattr(repair, '_source', changed_source)
    previous = head(queue, urls[1])
    result = repair.consume_repairs(queue, max_seconds=.1)
    assert result['processed'] == 1 and 'enqueued' not in result['outcomes']
    assert head(queue, urls[1]) == previous
    assert not queue.reviewed_repair_continuation_pending


def test_shape_cache_has_explicit_entry_and_byte_bounds(queue, monkeypatch):
    _, submitted = two_reviews(queue)
    monkeypatch.setattr(repair, 'MAX_VALIDATED_REQUESTS', 1)
    for result in submitted:
        repair._load_for_admission(queue, Path(result['request_path']))
    assert len(queue.reviewed_repair_validated_requests) == 1
    monkeypatch.setattr(repair, 'MAX_VALIDATED_REQUEST_BYTES', 1)
    repair._load_for_admission(queue, Path(submitted[0]['request_path']))
    assert not queue.reviewed_repair_validated_requests


def test_failed_archive_or_quarantine_does_not_request_fast_continuation(queue, monkeypatch):
    failure(queue)
    submit(queue)
    def unavailable(*args, **kwargs):
        raise OSError('synthetic unavailable archive')
    monkeypatch.setattr(repair, '_archive', unavailable)
    assert repair.consume_repairs(queue, max_seconds=.1)['outcomes'] == {'enqueued':1}
    assert not queue.reviewed_repair_continuation_pending and not queue.reviewed_repair_pending
    assert repair.consume_repairs(queue, max_seconds=.1)['processed'] == 0
    assert not queue.reviewed_repair_continuation_pending
    path, = list((queue.root/'reviewed_repairs/pending').glob('*.json'))
    path.write_bytes(b'{}')
    monkeypatch.setattr(repair, '_quarantine', unavailable)
    assert repair.consume_repairs(queue, max_seconds=.1)['outcomes'] == {'invalid_request':1}
    assert not queue.reviewed_repair_continuation_pending and not queue.reviewed_repair_pending


def test_partial_repair_continues_through_actual_owner_without_accelerating_other_pumps(config, tmp_path, monkeypatch):
    from downloader.news_fulltext import run_service
    from downloader.news_fulltext_queue import verify_bodies
    from test_news_fulltext import http_fixture, FakeResponse, article_html
    from downloader import news_fulltext_url_metadata as urls
    from downloader import news_fulltext_candidate_diagnostics as candidates
    targets = [f'https://publisher.test/story/continuation-{i}' for i in range(4)]
    routes = {'https://publisher.test/robots.txt':FakeResponse(b'User-agent: *\nAllow: /')}
    routes.update({url:FakeResponse(article_html(body=(url+' 完整原始正文。')*24)) for url in targets})
    http, session = http_fixture(config, tmp_path, routes)
    owner = NewsQueue(tmp_path, http.registry)
    calls = Counter()
    consume, url_consume, pump = repair.consume_repairs, urls.consume_urls, candidates.pump_candidates
    def one_at_a_time(*args, **kwargs):
        calls['repair'] += 1
        return consume(*args, limit=1, **kwargs)
    def consume_urls(*args, **kwargs):
        calls['urls'] += 1
        return url_consume(*args, **kwargs)
    def pump_candidates(*args, **kwargs):
        calls['candidates'] += 1
        return pump(*args, **kwargs)
    monkeypatch.setattr(repair, 'consume_repairs', one_at_a_time)
    monkeypatch.setattr(urls, 'consume_urls', consume_urls)
    monkeypatch.setattr(candidates, 'pump_candidates', pump_candidates)
    try:
        for url in targets:
            failure(owner, url)
        submit(owner, targets)
        result = run_service(owner, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=2)
        assert result['states'] == {'complete':4}
        assert calls == {'repair':4, 'urls':1, 'candidates':1}
        assert Counter(url for url,_ in session.calls) == Counter({url:1 for url in routes})
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        owner.close()
