"""A failed host must not consume another discovery endpoint's dispatch slots."""
from contextlib import closing
from dataclasses import replace
import random
import time

import pytest

from downloader.news_fulltext_host_circuit import discovery_request_host
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, config, http_fixture
from test_news_cnyes_public_index import cfg, queue
from test_news_cnyes_revision_dispatch_cache import blocked


BAD = 'bad.publisher.test'
GOOD = 'https://good.publisher.test/feed'


def test_host_pause_filters_before_limit_and_preserves_discovery_evidence(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['publisher']
        with queue.db:
            queue.db.executemany(
                "INSERT INTO discovery(url,kind,provider,priority,due,attempts,error_code) "
                "VALUES (?,'feed','publisher',0,0,3,'prior_failure')",
                [(f'https://{BAD}/feed/{number}',) for number in range(30)])
            queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'feed','publisher',100)", (GOOD,))
        before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
        rows = queue.discovery_candidates(source, 2, excluded_hosts=(BAD,))
        assert [row['url'] for row in rows] == [GOOD]
        assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
        assert len(queue.discovery_candidates(source, 2)) == 2


def test_idle_wake_observes_host_gate_without_spinning(config, tmp_path, monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: 100.0)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','publisher')",
                             (f'https://{BAD}/feed',))
        scopes = {'publisher': {'hosts': {BAD: 120}, 'origins': {}}}
        assert queue.next_wake_delay({}, maximum=60, request_scope_cooldowns=scopes) == 20
        assert queue.next_wake_delay({'publisher': 125}, maximum=60, request_scope_cooldowns=scopes) == 25


@pytest.mark.parametrize('url', [
    'http://bad.publisher.test/feed',
    ' HTTPS://BAD.PUBLISHER.TEST:443/feed ',
    'http://bad.publisher.test.:80?x=1',
])
def test_host_filter_matches_http_request_normalization(config, tmp_path, url):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['publisher']
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','publisher')", (url,))
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','publisher')", (GOOD,))
        rows = queue.discovery_candidates(source, 5, excluded_hosts=(BAD,))
        assert [row['url'] for row in rows] == [GOOD]


def test_host_filter_matches_http_host_for_canonical_and_unusual_urls(config, tmp_path):
    hosts = (BAD, 'xn--fiq228c.tw')
    urls = [
        f'https://{BAD}/feed', f'http://{BAD}/feed', f'https://{BAD}.evil.test/feed',
        f'https://good.publisher.test/path/{BAD}', f'HTTPS://{BAD.upper()}:443/feed',
        f'http://{BAD}.:80?feed=1', 'https://中文.tw/feed',
        f'https://{BAD}/bad\nurl', f'https://{BAD}/bad\\url',
        f'https://{BAD}/' + 'x' * 8192, 'https://good.publisher.test/新聞',
    ]
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['publisher']
        with queue.db:
            queue.db.executemany("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','publisher')",
                                 [(url,) for url in urls])
        actual = {row['url'] for row in queue.discovery_candidates(source, 100, excluded_hosts=hosts)}
        assert actual == {url for url in urls if discovery_request_host(url) not in hosts}


def test_host_filter_matches_http_host_on_varied_url_frontier(config, tmp_path):
    hosts = (BAD, 'xn--fiq228c.tw')
    roots = [f'https://{BAD}/feed', f'http://{BAD}/feed', f'HTTPS://{BAD.upper()}:443/feed',
             f'http://{BAD}.:80', f'https://{BAD}.evil.test/feed', f'https://{BAD}@else.test/feed',
             f'https://good.publisher.test/{BAD}', 'https://中文.tw/feed',
             'https://xn--fiq228c.tw/feed', 'https://good.publisher.test/新聞',
             f'https://{BAD}/bad\\path', f'https://{BAD}/bad\npath']
    rng = random.Random(613021)
    urls = [root + ('&' if '?' in root else '?') + f'n={index}'
            for index in range(500) for root in (rng.choice(roots),)]
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['publisher']
        with queue.db:
            queue.db.executemany("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','publisher')",
                                 [(url,) for url in urls])
        actual = {row['url'] for row in queue.discovery_candidates(source, 1000, excluded_hosts=hosts)}
        assert actual == {url for url in urls if discovery_request_host(url) not in hosts}


def test_metadata_only_scope_keeps_its_permissions_and_host_isolation(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        bad_feed = f'https://{BAD}/metadata'
        source = replace(queue.registry.sources['publisher'], allowed=False, metadata_feeds=(bad_feed, GOOD))
        queue.registry.sources['publisher'] = source
        with queue.db:
            queue.db.executemany("INSERT INTO discovery(url,kind,provider) VALUES (?,?, 'publisher')",
                [(bad_feed, 'metadata_feed'), (GOOD, 'metadata_feed'), ('https://good.publisher.test/unapproved', 'feed')])
        assert [row['url'] for row in queue.discovery_candidates(source, 20, excluded_hosts=(BAD,))] == [GOOD]
        assert {row['url'] for row in queue.discovery_candidates(source, 20)} == {bad_feed, GOOD}


def test_host_and_robots_scopes_compose_without_changing_provider_selection(config, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        source = queue.registry.sources['publisher']
        third = 'https://third.publisher.test/feed'
        with queue.db:
            queue.db.executemany("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','publisher')",
                                 [(f'https://{BAD}/feed',), (GOOD,), (third,)])
        hosts = (BAD,) + tuple(f'host{i}.publisher.test' for i in range(15))
        origins = ('https://good.publisher.test',) + tuple(f'https://origin{i}.publisher.test' for i in range(31))
        assert [row['url'] for row in queue.discovery_candidates(
            source, 20, excluded_hosts=hosts, excluded_origins=origins)] == [third]
        with pytest.raises(ValueError, match='bounded_excluded_hosts_required'):
            queue.discovery_candidates(source, 20, excluded_hosts=hosts + ('overflow.publisher.test',))


def test_cnyes_empty_revision_cache_is_bound_to_host_scope(blocked):
    queue, source, first, _ = blocked
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (first,))
    assert queue.discovery_candidates(source, 17, excluded_hosts=('api.cnyes.com',)) == []
    assert queue.discovery_candidates(source, 17, excluded_hosts=('api.cnyes.com',)) == []
    scopes = {'cnyes': {'hosts': {'api.cnyes.com': time.time() + 20}, 'origins': {}}}
    assert 0 < queue.next_wake_delay({}, maximum=60, request_scope_cooldowns=scopes) <= 20
    assert queue.discovery_candidates(source, 17)[0]['url'] == first


def test_cnyes_host_pause_reuses_empty_proof_even_without_revision(queue):
    from downloader.news_fulltext_cnyes_index import request_url
    from test_news_cnyes_public_index import DAY

    source = queue.registry.sources['cnyes']
    first = request_url(source, DAY)
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'index','cnyes')", (first,))
    assert queue.db.execute('SELECT count(*) FROM cnyes_index_revision_rounds').fetchone()[0] == 0
    traced = []
    queue.db.set_trace_callback(traced.append)
    try:
        assert queue.discovery_candidates(source, 17, excluded_hosts=('api.cnyes.com',)) == []
        traced.clear()
        assert queue.discovery_candidates(source, 17, excluded_hosts=('api.cnyes.com',)) == []
    finally:
        queue.db.set_trace_callback(None)
    assert not any('SELECT * FROM discovery' in sql for sql in traced)
    assert not queue._cnyes_revision_empty_selection.hit(source, 17, True, ())
    assert queue.discovery_candidates(source, 17)[0]['url'] == first


def test_cnyes_host_pause_proof_invalidates_for_new_other_host_work(queue):
    from downloader.news_fulltext_cnyes_index import request_url
    from test_news_cnyes_public_index import DAY

    source = queue.registry.sources['cnyes']
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'index','cnyes')", (request_url(source, DAY),))
    assert queue.discovery_candidates(source, 17, excluded_hosts=('api.cnyes.com',)) == []
    healthy = 'https://news.cnyes.com/rss'
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'feed','cnyes')", (healthy,))
    assert [row['url'] for row in queue.discovery_candidates(
        source, 17, excluded_hosts=('api.cnyes.com',))] == [healthy]


def test_owner_serves_healthy_discovery_without_mutating_blocked_heads(config, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner

    config.update(workers=1, cached_cnyes_replay_enabled=False,
                  cached_cnyes_metadata_replay_enabled=False, retained_cnyes_failure_replay_enabled=False)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    xml = b'<rss version="2.0"><channel><title>fixture</title></channel></rss>'
    http, session = http_fixture(config, tmp_path, {
        'https://good.publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        GOOD: FakeResponse(xml, headers={'Content-Type': 'application/rss+xml'}),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.db.executemany(
                "INSERT INTO discovery(url,kind,provider,priority,due,attempts,error_code) "
                "VALUES (?,'feed','publisher',0,0,3,'prior_failure')",
                [(f'https://{BAD}/feed/{number}',) for number in range(30)])
            queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'feed','publisher',100)", (GOOD,))
        http.host_circuits.paused['publisher'] = {BAD: time.time() + 30}
        before = [tuple(row) for row in queue.db.execute(
            "SELECT * FROM discovery WHERE url LIKE ? ORDER BY url", (f'https://{BAD}/%',))]
        result = owner.run_cycle(queue, http, config, max_articles=0, max_discovery_requests=1,
                                 max_run_seconds=5, defer_final_status=True)
        assert [url for url, _ in session.calls if url == GOOD] == [GOOD]
        assert result['cycle']['discovery_attempts_started'] == 1
        assert [tuple(row) for row in queue.db.execute(
            "SELECT * FROM discovery WHERE url LIKE ? ORDER BY url", (f'https://{BAD}/%',))] == before
        assert http.host_circuits.remaining('publisher', BAD) > 0


def test_resident_owner_fetches_healthy_body_then_recovers_paused_host(config, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner

    config.update(workers=2, batch_articles=1, cached_cnyes_replay_enabled=False,
                  cached_cnyes_metadata_replay_enabled=False, retained_cnyes_failure_replay_enabled=False)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    bad_feed = f'https://{BAD}/feed'
    good_article = 'https://good.publisher.test/news/1'
    good_xml = (f'<rss version="2.0"><channel><title>fixture</title><item><title>news</title>'
                f'<link>{good_article}</link></item></channel></rss>').encode()
    empty_xml = b'<rss version="2.0"><channel><title>fixture</title></channel></rss>'
    http, session = http_fixture(config, tmp_path, {
        'https://good.publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://bad.publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        GOOD: FakeResponse(good_xml, headers={'Content-Type': 'application/rss+xml'}),
        bad_feed: FakeResponse(empty_xml, headers={'Content-Type': 'application/rss+xml'}),
        good_article: FakeResponse(article_html()),
    })
    with closing(http), closing(NewsQueue(tmp_path, http.registry)) as queue:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'feed','publisher',0)", (bad_feed,))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'feed','publisher',100)", (GOOD,))
        deadline = time.time() + 2
        http.host_circuits.paused['publisher'] = {BAD: deadline}
        observed = []
        original_get = session.get
        def timed_get(url, **kwargs):
            observed.append((url, time.time()))
            return original_get(url, **kwargs)
        session.get = timed_get
        result_flags = {'bad_done': False, 'body_done': False}
        original_discovery = owner.accept_discovery
        original_accept = queue.accept
        def stop_if_done():
            if all(result_flags.values()):
                http.stop.set()
        def accept_discovery(q, task, response, links, records, errors):
            result = original_discovery(q, task, response, links, records, errors)
            if task['url'] == bad_feed:
                result_flags['bad_done'] = True
            stop_if_done()
            return result
        def accept(records):
            result = original_accept(records)
            if any(record['requested_url'] == good_article for record in records):
                result_flags['body_done'] = True
            stop_if_done()
            return result
        monkeypatch.setattr(owner, 'accept_discovery', accept_discovery)
        monkeypatch.setattr(queue, 'accept', accept)
        result = owner.run_service(queue, http, config, max_articles=1, max_discovery_requests=1, duration_seconds=10)
        assert result['activity'] == 'stopped' and all(result_flags.values())
        assert [when for url, when in observed if url == bad_feed] and all(
            when >= deadline for url, when in observed if url == bad_feed)
        assert min(when for url, when in observed if url == good_article) < min(
            when for url, when in observed if url == bad_feed)
        audit = verify_bodies(tmp_path)
        assert audit['accepted'] and audit['body_versions_verified'] == audit['complete_queue_heads_verified'] == 1
