"""Independent publisher work must not wait for another publisher's response.

Exercise the real shared queue/HTTP/executor/parser/fsync/receipt path with
synthetic source responses. No production owner, publisher request or policy
relaxation; the engineering order must not become a global downloader gate.
"""
from copy import deepcopy
from threading import Event
import time

import pytest

from downloader.news_fulltext import run_cycle
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, config, http_fixture


class AdvancingCycleClock:
    """Only the scheduler clock advances; HTTP fixtures keep the real clock."""
    def __init__(self):
        self.advance = 0

    def monotonic(self):
        return time.monotonic() + self.advance

    def __getattr__(self, name):
        return getattr(time, name)


def cooperative_config(config):
    foreground = config['providers'][0]
    foreground.update(parallelism=1)
    background = deepcopy(foreground)
    background.update(id='background', domains=['background.test'])
    disabled = deepcopy(foreground)
    disabled.update(id='disabled', domains=['disabled.test'], enabled=False)
    # Background first deliberately tests independence, not lucky priority order.
    config['providers'] = [background, foreground, disabled]
    config.update(workers=2, batch_articles=1)
    return config


@pytest.mark.parametrize('priority', ['publisher', ['missing'], ['publisher', 'publisher'], [{}], None])
def test_provider_priority_rejects_invalid_or_unknown_ids(config, tmp_path, priority):
    from downloader.news_fulltext import validated_provider_priority

    config = cooperative_config(config)
    config['provider_priority'] = priority
    with pytest.raises(ValueError, match='provider_priority'):
        validated_provider_priority(config, PolicyRegistry(config, base=tmp_path))


def test_provider_priority_accepts_registered_disabled_sources_without_enabling_them(config, tmp_path):
    from downloader.news_fulltext import validated_provider_priority

    config = cooperative_config(config)
    config['provider_priority'] = ['publisher', 'disabled']
    registry = PolicyRegistry(config, base=tmp_path)
    assert validated_provider_priority(config, registry) == ['publisher', 'disabled']
    assert not registry.sources['disabled'].request_enabled


def test_repository_priority_retains_user_order_without_enabling_sources():
    from pathlib import Path
    from downloader.news_fulltext import load_config

    config, registry, _ = load_config(Path('configs/news_fulltext.json'))
    assert config['provider_priority'] == [
        'cnyes', 'cmoney', 'udn', 'globalvoices', 'wantmedia',
        'money_link', 'cmnews', 'moneydj', 'ltn',
    ]
    assert not registry.sources['udn'].request_enabled
    assert not registry.sources['moneydj'].request_enabled


@pytest.mark.parametrize('priority_cooling', [False, True])
def test_explicit_priority_uses_next_worker_but_yields_during_cooldown(config, tmp_path, monkeypatch, priority_cooling):
    """A priority source gets a free slot; its refusal never blocks a peer."""
    from downloader import news_fulltext as main

    config = cooperative_config(config)
    config.update(workers=1, provider_priority=['publisher'])
    routes = {
        'https://background.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://background.test/story': FakeResponse(article_html()),
        'https://publisher.test/story': FakeResponse(article_html()),
    }
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    original_accept = queue.accept

    def accept_and_stop(records, **kwargs):
        result = original_accept(records, **kwargs)
        http.stop.set()
        return result

    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr(queue, 'accept', accept_and_stop)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://background.test/story'}, {'url': 'https://publisher.test/story'}])
        if priority_cooling:
            http.defer('publisher', 120)
        run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                  continuous_budgets=True, max_run_seconds=4)
        started = [url for url, _ in session.calls if url.endswith('/story')]
        assert started == ['https://background.test/story' if priority_cooling else 'https://publisher.test/story']
        assert verify_bodies(tmp_path)['errors'] == []
        remaining = queue.db.execute("SELECT state,attempts FROM articles WHERE url=?",
            ('https://publisher.test/story' if priority_cooling else 'https://background.test/story',)).fetchone()
        assert tuple(remaining) == ('pending', 0)
    finally:
        http.close()
        queue.close()


def test_priority_fills_available_parallel_slots_before_other_history(config, tmp_path, monkeypatch):
    from protego import Protego
    from downloader import news_fulltext as main

    config = cooperative_config(config)
    config['providers'][1].update(parallelism=2, interval_seconds=.00001)
    config.update(workers=2, provider_priority=['publisher'])
    priority_urls = ['https://publisher.test/story/1', 'https://publisher.test/story/2']
    peer_url = 'https://background.test/story'
    http, session = http_fixture(config, tmp_path, {url: FakeResponse(article_html())
                                                   for url in [*priority_urls, peer_url]})
    queue = NewsQueue(tmp_path, http.registry)
    starts, priority_started, received = [], Event(), [0]
    original_get, original_accept = session.get, queue.accept

    def observe_get(url, **kwargs):
        starts.append(url)
        if url in priority_urls:
            if sum(value in priority_urls for value in starts) == 2:
                priority_started.set()
            priority_started.wait(.8)
        return original_get(url, **kwargs)

    def accept(records, **kwargs):
        result = original_accept(records, **kwargs)
        received[0] += len(records)
        if received[0] == 3:
            http.stop.set()
        return result

    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr(session, 'get', observe_get)
    monkeypatch.setattr(queue, 'accept', accept)
    parser = Protego.parse('User-agent: *\nAllow: /')
    http.robots_parsers.update({origin: (time.time(), parser)
                                for origin in ('https://publisher.test', 'https://background.test')})
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in [peer_url, *priority_urls]])
        run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                  continuous_budgets=True, max_run_seconds=5)
        assert set(starts[:2]) == set(priority_urls) and starts[2:] == [peer_url]
        assert received == [3] and verify_bodies(tmp_path)['errors'] == []
    finally:
        http.close()
        queue.close()


def test_slow_recent_recheck_does_not_start_discovery_seed_after_deadline(config, tmp_path, monkeypatch):
    import downloader.news_fulltext as main

    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    clock, stages = AdvancingCycleClock(), []
    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)

    def slow_recheck(*args, **kwargs):
        stages.append('recheck')
        clock.advance += 10

    monkeypatch.setattr(queue, 'schedule_recent_rechecks', slow_recheck)
    monkeypatch.setattr(main, 'seed_discovery', lambda *_: stages.append('seed'))
    try:
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=1,
                           continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert stages == ['recheck']
        assert not session.calls and result['cycle']['article_attempts_started'] == 0
        assert queue.db.execute('SELECT COUNT(*) FROM discovery').fetchone()[0] == 0
    finally:
        http.close()
        queue.close()


def test_slow_url_repair_admission_does_not_start_remaining_inbox_slices(config, tmp_path, monkeypatch):
    import downloader.news_fulltext as main
    import downloader.news_fulltext_candidate_diagnostics as candidates
    import downloader.news_fulltext_repairs as repairs
    import downloader.news_fulltext_url_metadata as url_metadata

    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    clock, stages = AdvancingCycleClock(), []
    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)

    def slow_urls(*args, **kwargs):
        stages.append('urls')
        clock.advance += 10

    monkeypatch.setattr(url_metadata, 'consume_urls', slow_urls)
    monkeypatch.setattr(repairs, 'consume_repairs', lambda *args, **kwargs: stages.append('repairs'))
    monkeypatch.setattr(candidates, 'pump_candidates', lambda *args, **kwargs: stages.append('candidates'))
    try:
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                           continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert stages == ['urls']
        assert not session.calls and result['cycle']['article_attempts_started'] == 0
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('continuous_budgets', [False, True])
def test_slow_namespace_maintenance_cannot_start_work_after_cycle_deadline(
        config, tmp_path, monkeypatch, continuous_budgets):
    import downloader.news_fulltext as main

    foreground = config['providers'][0]
    foreground.update(id='cmoney', domains=['cmoney.tw'], parallelism=1,
                      article_path_pattern=r'^/forum/article/[0-9]+$')
    peer = deepcopy(foreground)
    peer.update(id='publisher', domains=['publisher.test'], article_path_pattern=r'^/story$')
    config['providers'] = [foreground, peer]
    config.update(workers=2, batch_articles=1)
    routes = {'https://www.cmoney.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://www.cmoney.tw/forum/article/55': FakeResponse(article_html()),
              'https://publisher.test/story': FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    clock, slices = AdvancingCycleClock(), []
    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)

    def slow_namespace(*, stop=None):
        slices.append(True)
        clock.advance += 10  # The local slice started within the two-second budget.
        return {'completed': True}

    monkeypatch.setattr(queue, 'backfill_cmoney_note_detail_exclusions', slow_namespace)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://www.cmoney.tw/forum/article/55'},
                                {'url': 'https://publisher.test/story'}])
        result = run_cycle(queue, http, config, max_articles=2, max_discovery_requests=0,
                           continuous_budgets=continuous_budgets, max_run_seconds=2,
                           defer_final_status=True)
        assert slices == [True]
        assert not session.calls and result['cycle']['article_attempts_started'] == 0
        assert [tuple(row) for row in queue.db.execute('SELECT state,attempts FROM articles ORDER BY url')] == [
            ('pending', 0), ('pending', 0)]
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('queued_discovery', [False, True])
def test_deadline_during_slow_discovery_lookup_drains_existing_peer_without_new_claim(
        config, tmp_path, monkeypatch, queued_discovery):
    import downloader.news_fulltext as main

    config = cooperative_config(config)
    if queued_discovery:
        config['providers'][1]['feeds'] = ['https://publisher.test/feed']
    routes = {'https://background.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://background.test/story': FakeResponse(article_html()),
              'https://publisher.test/story': FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    clock = AdvancingCycleClock()
    original = queue.discovery_candidates
    slowed = []
    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)

    def slow_second(source, *args, **kwargs):
        candidates = original(source, *args, **kwargs)
        if source.provider == 'publisher':
            slowed.append(True)
            clock.advance += 10
        return candidates

    monkeypatch.setattr(queue, 'discovery_candidates', slow_second)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://background.test/story'},
                                {'url': 'https://publisher.test/story'}])
        result = run_cycle(queue, http, config, max_articles=2, max_discovery_requests=2,
                           continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert slowed == [True]
        assert result['cycle']['article_attempts_started'] == 1
        assert queue.db.execute("SELECT state,attempts FROM articles WHERE provider='background'").fetchone()[:] == ('complete', 1)
        assert queue.db.execute("SELECT state,attempts FROM articles WHERE provider='publisher'").fetchone()[:] == ('pending', 0)
        assert any(url == 'https://background.test/story' for url, _ in session.calls)
        assert not any('publisher.test' in url for url, _ in session.calls)
        if queued_discovery:
            head = queue.db.execute("SELECT attempts,error_code,last_checked_at_utc FROM discovery "
                                    "WHERE url='https://publisher.test/feed'").fetchone()
            assert head[:] == (0, None, None)
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('stop_kind', ['deadline', 'signal'])
def test_stop_during_discovery_preparation_does_not_dispatch_unclaimed_request(
        config, tmp_path, monkeypatch, stop_kind):
    import downloader.news_fulltext as main
    import downloader.news_fulltext_cnyes_revision as revision

    feed = 'https://publisher.test/feed'
    config['providers'][0]['feeds'] = [feed]
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    clock, prepared = AdvancingCycleClock(), []
    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)

    def slow_prepare(q, task):
        prepared.append(task['url'])
        if stop_kind == 'signal':
            http.stop.set()
        else:
            clock.advance += 130

    monkeypatch.setattr(revision, 'prepare_dispatch', slow_prepare)
    try:
        result = run_cycle(queue, http, config, max_articles=0, max_discovery_requests=1,
                           continuous_budgets=True, max_run_seconds=120, defer_final_status=True)
        assert prepared == [feed]
        assert result['cycle']['discovery_attempts_started'] == 0 and not session.calls
        head = queue.db.execute('SELECT attempts,error_code,last_checked_at_utc FROM discovery WHERE url=?',
                                (feed,)).fetchone()
        assert head[:] == (0, None, None)
    finally:
        http.close()
        queue.close()


def test_slow_original_route_sync_does_not_start_a_second_local_slice_or_http(config, tmp_path, monkeypatch):
    import downloader.news_fulltext as main
    import downloader.news_fulltext_urls as urls

    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/story': FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    clock, stages = AdvancingCycleClock(), []
    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)

    def slow_sync(*args, **kwargs):
        stages.append('sync')
        clock.advance += 10

    monkeypatch.setattr(urls, 'sync_original_url_routes', slow_sync)
    monkeypatch.setattr(urls, 'backfill_original_urls', lambda *args, **kwargs: stages.append('backfill'))
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/story'}])
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                           continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert stages == ['sync']
        assert not session.calls and result['cycle']['article_attempts_started'] == 0
        assert queue.db.execute('SELECT state,attempts FROM articles').fetchone()[:] == ('pending', 0)
    finally:
        http.close()
        queue.close()


def test_claim_started_before_deadline_is_drained_and_committed_after_deadline(config, tmp_path, monkeypatch):
    import downloader.news_fulltext as main

    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/story': FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    clock, original = AdvancingCycleClock(), queue.claim
    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)

    def slow_claim(*args, **kwargs):
        row = original(*args, **kwargs)
        assert row is not None
        clock.advance += 10
        return row

    monkeypatch.setattr(queue, 'claim', slow_claim)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/story'}])
        result = run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                           continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert result['cycle']['article_attempts_started'] == 1
        assert queue.db.execute('SELECT state,attempts,lease_owner FROM articles').fetchone()[:] == ('complete', 1, None)
        assert any(url == 'https://publisher.test/story' for url, _ in session.calls)
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('continuous_budgets', [False, True])
def test_cmoney_pristine_path_sweep_waits_for_disk_and_other_provider_continues(
        config, tmp_path, monkeypatch, continuous_budgets):
    cmoney = config['providers'][0]
    cmoney.update(id='cmoney', domains=['cmoney.tw'], parallelism=1,
                  article_path_pattern=r'^/(forum/article/[0-9]+|notes/note[.]aspx)')
    other = deepcopy(cmoney)
    other.update(id='publisher', domains=['publisher.test'], article_path_pattern=r'^/story$')
    config['providers'] = [cmoney, other]
    config.update(workers=2, batch_articles=1)
    forum = 'https://www.cmoney.tw/forum/article/55'
    focus = 'https://publisher.test/story'
    notes = [f'https://www.cmoney.tw/notes/note-detail.aspx?nid={i}' for i in range(3)]
    routes = {'https://www.cmoney.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              forum: FakeResponse(article_html()), focus: FakeResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    slices = []
    original = queue.backfill_cmoney_note_detail_exclusions

    def slice_one(*, stop=None):
        result = original(limit=1, stop=stop)
        slices.append((result, tuple(url for url, _ in session.calls)))
        return result

    monkeypatch.setattr(queue, 'backfill_cmoney_note_detail_exclusions', slice_one)
    blocked = [True]
    monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *_: blocked[0])
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in notes + [forum, focus]])
            queue.db.executemany("UPDATE articles SET state='pending',error_code=NULL WHERE url=?",
                                 ((url,) for url in notes))
        before = [dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) for url in notes]
        result = run_cycle(queue, http, config, max_articles=4, max_discovery_requests=0,
                           continuous_budgets=continuous_budgets)
        assert result['activity'] == 'waiting_disk' and not slices and not session.calls
        assert [dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) for url in notes] == before
        blocked[0] = False
        result = run_cycle(queue, http, config, max_articles=4, max_discovery_requests=0,
                           continuous_budgets=continuous_budgets)
        assert slices and slices[-1][0]['completed']
        assert any(focus in calls and forum not in calls and not item['completed'] for item, calls in slices)
        assert all(queue.db.execute('SELECT state,attempts FROM articles WHERE url=?', (url,)).fetchone()[:]
                   == ('unsupported_article_path', 0) for url in notes)
        assert all(queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'complete'
                   for url in (forum, focus))
        assert not any('note-detail.aspx' in url for url, _ in session.calls)
        assert result['cycle']['durably_accepted_versions'] == 2
        assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


def test_priority_body_is_durable_while_background_response_still_waits(config, tmp_path, monkeypatch):
    config = cooperative_config(config)
    background_started, focus_committed = Event(), Event()

    class WaitingBackground(FakeResponse):
        def iter_content(self, **kwargs):
            background_started.set()
            assert focus_committed.wait(5), 'background must not occupy every available worker'
            yield self.body

    class FocusResponse(FakeResponse):
        def iter_content(self, **kwargs):
            assert background_started.wait(5), 'both publishers must actually overlap'
            yield self.body

    routes = {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://background.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://publisher.test/story': FocusResponse(article_html()),
        'https://background.test/story': WaitingBackground(article_html()),
    }
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    original_accept = queue.accept
    commits = []

    def accept(records):
        original_accept(records)
        commits.extend(record['provider'] for record in records)
        if 'publisher' in commits:
            focus_committed.set()  # Only after the real durable writer returns.

    monkeypatch.setattr(queue, 'accept', accept)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://' + host + '/story'} for host in
                                ('publisher.test', 'background.test', 'disabled.test')])
        status = run_cycle(queue, http, config, max_articles=4, max_discovery_requests=0)
        assert background_started.is_set() and focus_committed.is_set()
        assert commits == ['publisher', 'background']
        assert status['states'] == {'complete': 2, 'awaiting_authorization': 1}
        proof = verify_bodies(tmp_path)
        assert proof['accepted'] and proof['body_versions_verified'] == 2
        assert not any('disabled.test' in url for url, _ in session.calls)
        assert http.parallelism == {'background': 1, 'publisher': 1}
        assert 'disabled' not in http.limiters
    finally:
        focus_committed.set()
        http.close()
        queue.close()


@pytest.mark.parametrize(('provider', 'continuous_budgets'), [('publisher', True), ('cnyes', False)])
def test_unused_cnyes_priority_wake_does_not_busy_poll(config, tmp_path, monkeypatch, provider, continuous_budgets):
    import downloader.news_fulltext as main

    class SlowResponse(FakeResponse):
        def iter_content(self, **kwargs):
            Event().wait(.25)
            yield self.body

    config['providers'][0]['id'] = provider
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              'https://publisher.test/story': SlowResponse(article_html())}
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    queue._cnyes_original_priority_needs_wake = True
    timeouts, original_wait = [], main.wait

    def measured_wait(futures, *, timeout, return_when):
        timeouts.append(timeout)
        return original_wait(futures, timeout=timeout, return_when=return_when)

    monkeypatch.setattr(main, 'wait', measured_wait)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda *_: False)
    try:
        queue.add_articles([{'url': 'https://publisher.test/story'}])
        result = run_cycle(queue, http, config, max_articles=2, max_discovery_requests=0,
                           continuous_budgets=continuous_budgets, max_run_seconds=3, defer_final_status=True)
        assert any(timeout > 0 for timeout in timeouts)
        assert timeouts.count(0) <= 2, f'unused wake caused repeated polling: {len(timeouts)} waits'
        if provider == 'publisher':
            assert result['cycle']['durably_accepted_versions'] == 1
        assert any(url == 'https://publisher.test/story' for url, _ in session.calls)
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('refused', ['publisher', 'background'])
def test_source_refusal_pauses_only_its_provider_while_other_body_commits(config, tmp_path, refused):
    config = cooperative_config(config)
    sources = {'publisher': 'publisher.test', 'background': 'background.test'}
    routes = {f'https://{host}/robots.txt': FakeResponse(b'User-agent: *\nAllow: /')
              for host in sources.values()}
    routes.update({f'https://{host}/story': FakeResponse(status=403) if provider == refused
                   else FakeResponse(article_html()) for provider, host in sources.items()})
    http, session = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': f'https://{host}/story'} for host in sources.values()])
        status = run_cycle(queue, http, config, max_articles=4, max_discovery_requests=0)
        assert status['states'] == {'complete': 1, 'retry': 1}
        refused_head = queue.db.execute('SELECT state,error_code,due FROM articles WHERE provider=?',
                                       (refused,)).fetchone()
        assert refused_head['state'] == 'retry' and refused_head['error_code'] == 'source_access_denied'
        assert refused_head['due'] > time.time()
        assert http.provider_not_before[refused] > time.time()
        healthy = next(provider for provider in sources if provider != refused)
        assert healthy not in http.provider_not_before
        assert sum(url == f'https://{sources[refused]}/story' for url, _ in session.calls) == 1
        proof = verify_bodies(tmp_path)
        assert proof['accepted'] and proof['body_versions_verified'] == 1
    finally:
        http.close()
        queue.close()
