"""Idle wakes must include durable adapter/endpoint cooldown boundaries."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import time

import pyarrow.parquet as pq
import pytest

from downloader.news_fulltext_bulk import cnyes_scope, cnyes_url, wordpress_url
from downloader.news_fulltext_cnyes_index import ENDPOINT, request_url
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_cnyes_public_index import DAY, cfg, index_payload, queue
from test_news_fulltext import FakeResponse, config, http_fixture


@pytest.fixture
def clock(monkeypatch):
    now = [100.0]
    monkeypatch.setattr('downloader.news_fulltext_queue.time.time', lambda: now[0])
    return now


@pytest.mark.parametrize('kind', ['index', 'cnyes_news'])
def test_cnyes_circuit_wakes_at_deadline_without_revision(queue, clock, kind):
    source = queue.registry.sources['cnyes']
    endpoint = ENDPOINT if kind == 'index' else source.cnyes_apis[0]
    url = request_url(source, DAY) if kind == 'index' else cnyes_url(endpoint)
    fingerprint = source.cnyes_index_data_fingerprint if kind == 'index' else source.data_fingerprint
    with queue.db:
        queue.db.execute('INSERT INTO discovery(url,kind,provider,due) VALUES (?,?,?,0)', (url, kind, 'cnyes'))
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
                         ('cnyes', endpoint, fingerprint, 105, 'transport_error', url))
    assert queue.discovery_candidates(source, 10) == []
    assert queue.db.execute('SELECT count(*) FROM cnyes_index_revision_rounds').fetchone()[0] == 0
    assert queue.next_wake_delay({}, maximum=60) == 5
    assert queue.next_wake_delay({'cnyes': 108}, maximum=60) == 8
    clock[0] = 105
    assert [row['url'] for row in queue.discovery_candidates(source, 10)] == [url]


@pytest.mark.parametrize('kind', ['wordpress_posts', 'rss_fulltext'])
def test_bulk_adapter_wakes_at_deadline_without_delaying_other_publishers(config, tmp_path, clock, kind):
    base = ('https://publisher.test/wp-json/wp/v2/posts' if kind == 'wordpress_posts'
            else 'https://publisher.test/feed')
    config['providers'][0]['wordpress_apis' if kind == 'wordpress_posts' else 'fulltext_feeds'] = [base]
    other = deepcopy(config['providers'][0])
    other.update(id='other', domains=['other.test'], wordpress_apis=[], fulltext_feeds=[])
    config['providers'].append(other)
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        source = q.registry.sources['publisher']
        url = wordpress_url(base) if kind == 'wordpress_posts' else base
        with q.db:
            q.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,?,'publisher',0)", (url, kind))
            q.db.execute("INSERT INTO bulk_adapter_cooldowns VALUES (?,?,?,?)",
                         ('publisher', source.data_fingerprint, 105, 'bulk_all_items_unusable'))
        assert not q.bulk_allowed(source)
        assert q.discovery_candidates(source, 10, bulk_allowed=False) == []
        assert q.next_wake_delay({}, maximum=60) == 5
        with q.db:
            q.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'feed','other',103)",
                         ('https://other.test/feed',))
        assert q.next_wake_delay({}, maximum=60) == 3
        clock[0] = 103
        assert q.discovery_candidates(q.registry.sources['other'], 10)[0]['url'] == 'https://other.test/feed'
        with q.db:
            q.db.execute("DELETE FROM discovery WHERE provider='other'")
        clock[0] = 105
        assert q.bulk_allowed(source)
        assert [row['url'] for row in q.discovery_candidates(source, 10)] == [url]
    finally:
        q.close()


def test_origin_pause_and_provider_pause_remain_authoritative(queue, clock):
    source = queue.registry.sources['cnyes']
    url = request_url(source, DAY)
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'index','cnyes',0)", (url,))
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
                         ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, 105, 'transport_error', url))
    before = tuple(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone())
    scopes = {'cnyes': {'hosts': {}, 'origins': {'https://api.cnyes.com': 110}}}
    assert queue.next_wake_delay({}, maximum=60, request_scope_cooldowns=scopes) == 10
    assert queue.next_wake_delay({'cnyes': 112}, maximum=60, request_scope_cooldowns=scopes) == 12
    assert tuple(queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns').fetchone()) == before


@pytest.mark.parametrize('condition', ['different_fingerprint', 'removed_endpoint', 'finite_scope', 'known_urls', 'no_permission'])
def test_unrelated_or_unpermitted_circuit_does_not_change_wake(queue, clock, condition):
    source = queue.registry.sources['cnyes']
    endpoint = ENDPOINT if condition != 'removed_endpoint' else 'https://api.cnyes.com/unregistered'
    fingerprint = '0' * 64 if condition == 'different_fingerprint' else source.cnyes_index_data_fingerprint
    with queue.db:
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
                         ('cnyes', endpoint, fingerprint, 105, 'transport_error', endpoint + '?page=1'))
    if condition == 'finite_scope':
        queue.registry.sources['cnyes'] = replace(source, acquisition_mode='finite')
    elif condition == 'known_urls':
        queue.registry.sources['cnyes'] = replace(source, known_url_only=True)
    elif condition == 'no_permission':
        queue.registry.sources['cnyes'] = replace(source, allowed=False, metadata_feeds=())
    assert queue.next_wake_delay({}, maximum=60) == 60


def test_cooldown_for_a_future_job_wakes_then_rechecks_its_own_due(queue, clock):
    source = queue.registry.sources['cnyes']
    url = request_url(source, DAY)
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'index','cnyes',108)", (url,))
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
                         ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, 105, 'transport_error', url))
    assert queue.next_wake_delay({}, maximum=60) == 5
    clock[0] = 105
    assert queue.discovery_candidates(source, 10) == []
    assert queue.next_wake_delay({}, maximum=60) == 3
    clock[0] = 108
    assert queue.discovery_candidates(source, 10)[0]['url'] == url


def test_stale_circuit_can_wake_once_but_never_spin(queue, clock):
    source = queue.registry.sources['cnyes']
    url = request_url(source, DAY)
    with queue.db:
        queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
                         ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, 105, 'transport_error', url))
    assert queue.next_wake_delay({}, maximum=60) == 5
    clock[0] = 105
    assert queue.next_wake_delay({}, maximum=60) == 60


def test_inflight_dispatch_deadlines_filter_lanes_providers_and_old_work(config, tmp_path, clock):
    other = deepcopy(config['providers'][0])
    other.update(id='other', domains=['other.test'])
    config['providers'].append(other)
    q = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with q.db:
            q.add_articles([{'url': 'https://publisher.test/old'},
                            {'url': 'https://other.test/old'}, {'url': 'https://other.test/future'}])
            q.db.execute("UPDATE articles SET due=104 WHERE url='https://other.test/future'")
            q.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'feed','other',102)",
                         ('https://other.test/feed',))
        common = dict(provider_ids={'other'}, include_local=False, dispatch_after=100)
        assert q.next_wake_delay({}, maximum=60, include_discovery=False, **common) == 4
        assert q.next_wake_delay({}, maximum=60, include_articles=False, **common) == 2
        assert q.next_wake_delay({'other': 108}, maximum=60, include_discovery=False, **common) == 8
        scopes = {'other': {'hosts': {'other.test': 106}, 'origins': {}}}
        assert q.next_wake_delay({}, maximum=60, include_discovery=False,
                                 request_scope_cooldowns=scopes, **common) == 6
        # A deadline that passed since the dispatch attempt needs one retry.
        clock[0] = 103
        assert q.next_wake_delay({}, maximum=60, include_articles=False, **common) == 0
        # An already offered (but unclaimable) row cannot spin forever or hide
        # a later deadline in the same provider's indexed queue.
        assert q.next_wake_delay({}, maximum=60, include_discovery=False,
                                 provider_ids={'other'}, include_local=False, dispatch_after=103) == 1
        clock[0] = 105
        assert q.next_wake_delay({}, maximum=60, provider_ids={'other'}, include_local=False,
                                 dispatch_after=105) == 60
        assert q.next_wake_delay({}, maximum=60, provider_ids={'publisher'}, include_local=False,
                                 dispatch_after=105) == 60
    finally:
        q.close()


def test_article_wake_many_sources_preserves_deadlines_with_bounded_queries(config, tmp_path, clock):
    """Multiple sources must agree with a row-level oracle without per-state SQL calls."""
    template = config['providers'][0]
    config['providers'] = []
    for number in range(132):
        source = deepcopy(template)
        source.update(id=f'source_{number:03}',
                      domains=[f'p{number:03}.test', f'alt{number:03}.test'])
        config['providers'].append(source)
    registry = PolicyRegistry(config, base=tmp_path)
    q = NewsQueue(tmp_path, registry)
    rows = []
    for number in range(132):
        for index, state in enumerate(('pending', 'retry', 'retry_cooldown', 'complete')):
            host = f'{"alt" if index % 2 else "p"}{number:03}.test'
            url = f'https://{host}/story/{index}'
            due = (90, 104 + number % 6, 105 + number % 8, 101)[index]
            rows.append((f'source_{number:03}', url, host, state, due))
    try:
        with q.db:
            q.add_articles([{'url': url} for _, url, _, _, _ in rows])
            q.db.executemany('UPDATE articles SET state=?,due=? WHERE url=?',
                             [(state, due, url) for _, url, _, state, due in rows])
        registry.sources['source_005'] = replace(registry.sources['source_005'], allowed=False, metadata_feeds=())
        registry.sources['source_006'] = replace(registry.sources['source_006'], allowed=False,
            metadata_feeds=('https://p006.test/feed',))
        scopes = {
            'source_003': {'hosts': {'p003.test': 104}, 'origins': {'https://alt003.test': 106}},
            'source_006': {'hosts': {'p006.test': 107}, 'origins': {}},
            'source_007': {'hosts': {'p007.test': 99}, 'origins': {}},
        }
        provider_cooldowns = {'source_000': 109, 'source_003': 108}

        def oracle(selected, after):
            deadlines = []
            for provider, source in registry.sources.items():
                if not source.request_enabled or selected is not None and provider not in selected:
                    continue
                scope = scopes.get(provider, {})
                hosts = {host: due for host, due in scope.get('hosts', {}).items() if due > clock[0]}
                origins = {origin: due for origin, due in scope.get('origins', {}).items() if due > clock[0]}
                earliest = [*hosts.values(), *origins.values()]
                if source.allowed:
                    earliest.extend(due for row_provider, url, host, state, due in rows
                        if row_provider == provider and state in {'pending', 'retry', 'retry_cooldown'}
                        and (after is None or due > after) and host not in hosts
                        and not any(url.startswith(origin + '/') for origin in origins))
                if earliest:
                    deadlines.append(max(min(earliest), provider_cooldowns.get(provider, 0)))
            return min(60, max(0, min(deadlines) - clock[0])) if deadlines else 60

        cases = [(None, None), (None, 100), (None, 107), ({'source_003', 'source_004'}, 100),
                 ({'source_003'}, 100), ({'source_005'}, None), ({'source_006'}, None), (set(), 100)]
        statements = []
        for selected, after in cases:
            q.db.set_trace_callback(statements.append)
            try:
                actual = q.next_wake_delay(provider_cooldowns, maximum=60,
                    request_scope_cooldowns=scopes, provider_ids=selected, include_discovery=False,
                    include_local=False, dispatch_after=after)
            finally:
                q.db.set_trace_callback(None)
            assert actual == oracle(selected, after)
        # Query count should scale by bounded provider batches, not by three
        # statements per provider. Scoped hosts retain the ordinary filter.
        statements.clear()
        q.db.set_trace_callback(statements.append)
        try:
            q.next_wake_delay(provider_cooldowns, maximum=60, request_scope_cooldowns=scopes,
                              include_discovery=False, include_local=False, dispatch_after=100)
        finally:
            q.db.set_trace_callback(None)
        article_queries = [sql for sql in statements if 'FROM ARTICLES' in sql.upper()]
        assert len(article_queries) < 10
    finally:
        q.close()


def test_resident_owner_wakes_and_accepts_original_after_cnyes_cooldown(cfg, tmp_path, monkeypatch):
    """Exercise the resident loop, HTTP owner, parser and durable receipt offline."""
    from downloader import news_fulltext as owner

    cfg.update(workers=2, cached_cnyes_replay_enabled=False,
               cached_cnyes_metadata_replay_enabled=False, retained_cnyes_failure_replay_enabled=False,
               adaptive_rate={'enabled': False})
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    registry = PolicyRegistry(cfg, base=tmp_path)
    source = registry.sources['cnyes']
    url = request_url(source, DAY)
    raw = json.dumps(index_payload(ids=(6789,)), ensure_ascii=False).encode()
    http, session = http_fixture(cfg, tmp_path, {
        'https://api.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(raw, headers={'Content-Type': 'application/json'}),
    })
    queue = NewsQueue(tmp_path, http.registry)
    observed, original_get = [], session.get
    def checked_get(request_url, **kwargs):
        if request_url == url:
            observed.append(time.time())
        return original_get(request_url, **kwargs)
    session.get = checked_get
    original_accept = owner.accept_discovery
    def stop_after_accept(q, task, response, links, records, errors):
        result = original_accept(q, task, response, links, records, errors)
        if task['url'] == url:
            http.stop.set()
        return result
    monkeypatch.setattr(owner, 'accept_discovery', stop_after_accept)
    try:
        deadline = time.time() + 2.0
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'index','cnyes',0)", (url,))
            queue.db.execute('INSERT INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)',
                             ('cnyes', ENDPOINT, source.cnyes_index_data_fingerprint, deadline, 'transport_error', url))
        result = owner.run_service(queue, http, cfg, max_articles=0, max_discovery_requests=1, duration_seconds=12)
        assert result['activity'] == 'stopped'
        assert len(observed) == 1 and observed[0] >= deadline
        evidence = queue.db.execute('SELECT evidence_path FROM cnyes_public_index_observations WHERE scope_url=? AND page=1',
                                    (cnyes_scope(url),)).fetchone()
        assert evidence is not None
        path = tmp_path / evidence['evidence_path']
        receipt = json.loads(path.with_suffix(path.suffix + '.receipt.json').read_bytes())
        assert receipt['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
        row = pq.read_table(path).to_pylist()[0]
        assert row['raw_response_body'] == raw and row['metadata_only'] and row['accepted_body_count'] == 0
        assert not list(tmp_path.glob('articles/**/*.parquet'))
    finally:
        http.close()
        queue.close()
