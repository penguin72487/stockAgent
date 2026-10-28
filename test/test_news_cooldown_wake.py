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
