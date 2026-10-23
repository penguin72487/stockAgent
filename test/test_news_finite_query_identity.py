"""Synthetic legacy query-ID articles: finite identity, not URL enumeration."""
from copy import deepcopy
import hashlib
from urllib.parse import urlsplit

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import FakeResponse, config, http_fixture


URL = 'https://publisher.test/Reading/IndepArticle.aspx?id=ARTL003020265'
OTHER = URL.replace('ARTL003020265', '6022715')


def finite(config, *, urls=(URL,), keys=('id',)):
    source = config['providers'][0]
    source.update(acquisition_mode='finite', finite_article_urls=sorted(urls),
        finite_identity_query_keys=list(keys),
        article_path_pattern=r'^/(?:Reading/IndepArticle\.aspx|public/[1-9][0-9]*)$')
    return config


def test_query_identity_requires_explicit_keys_and_keeps_exact_literal_URLs(config, tmp_path):
    current = finite(config)
    source = PolicyRegistry(current, base=tmp_path).sources['publisher']
    assert source.finite_article_urls == (URL,)
    altered = deepcopy(current)
    altered['providers'][0].pop('finite_identity_query_keys')
    with pytest.raises(ValueError, match='outside reviewed identity scope'):
        PolicyRegistry(altered, base=tmp_path)
    altered['providers'][0]['finite_identity_query_keys'] = ['id']
    altered['providers'][0]['finite_article_urls'] = [OTHER]
    changed = PolicyRegistry(altered, base=tmp_path).sources['publisher']
    assert changed.data_fingerprint != source.data_fingerprint
    assert changed.authorization_fingerprint == source.authorization_fingerprint


@pytest.mark.parametrize('query', [
    'id=', 'id', 'id=1&id=2', 'ID=1', 'id=1&ID=1', 'id=1&extra=2',
    'id=1&', 'id=%00', 'id=a%2Fb', 'id=https%3A%2F%2Fother.test',
    'id='+'a'*129,
])
def test_missing_duplicate_ambiguous_or_nonidentity_query_is_not_a_scope(config, tmp_path, query):
    with pytest.raises(ValueError, match='finite (article|query)'):
        PolicyRegistry(finite(config, urls=(URL.split('?')[0]+'?'+query,)), base=tmp_path)


@pytest.mark.parametrize('keys', [None, 'id', ['id','id'], ['unused'], ['id','extra'], [1], ['bad-key']])
def test_identity_key_declaration_is_bounded_used_and_canonical(config, tmp_path, keys):
    current = finite(config)
    current['providers'][0]['finite_identity_query_keys'] = keys
    with pytest.raises(ValueError, match='finite (article|query)'):
        PolicyRegistry(current, base=tmp_path)


def test_queryless_finite_scope_does_not_acquire_unused_query_permission(config, tmp_path):
    with pytest.raises(ValueError, match='unused explicit finite query'):
        PolicyRegistry(finite(config, urls=('https://publisher.test/public/123',)), base=tmp_path)
    with pytest.raises(ValueError, match='invalid explicit finite query'):
        PolicyRegistry(finite(config, urls=()), base=tmp_path)


def test_literal_request_retains_query_and_home_extra_parameter_or_changed_value_is_zero_network(config, tmp_path):
    current = finite(config)
    robots = 'https://publisher.test/robots.txt'
    http, session = http_fixture(current, tmp_path, {
        robots: FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(b'public original response'),
    })
    try:
        for url in (OTHER, URL+'&extra=1', URL.replace('?id=', '?ID='),
                    URL.replace('ARTL003020265', '%41RTL003020265'),
                    'https://publisher.test/', URL.split('?')[0], robots+'?id=1'):
            with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
                http.get(url)
        assert session.calls == []
        assert http.get(URL).body == b'public original response'
        assert [row[0] for row in session.calls] == [robots, URL]
    finally:
        http.close()


def test_query_redirect_cannot_change_the_reviewed_identity(config, tmp_path):
    http, session = http_fixture(finite(config), tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(status=302, headers={'Location': OTHER}),
    })
    try:
        with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
            http.get(URL)
        assert [row[0] for row in session.calls] == ['https://publisher.test/robots.txt', URL]
    finally:
        http.close()


@pytest.mark.parametrize('status,body,error', [
    (200, b'User-agent: *\nDisallow: /Reading/\n', 'robots_disallowed'),
    (403, b'', 'source_access_denied'), (429, b'', 'http_429'),
])
def test_query_identity_does_not_override_actual_rep_or_refusal(config, tmp_path, status, body, error):
    robots = 'https://publisher.test/robots.txt'
    http, session = http_fixture(finite(config), tmp_path, {robots: FakeResponse(body,status)})
    try:
        with pytest.raises(NewsError, match=error):
            http.get(URL)
        assert [row[0] for row in session.calls] == [robots]
    finally:
        http.close()


def test_queue_freezes_full_query_identities_and_does_not_claim_another_id(config, tmp_path):
    registry = PolicyRegistry(finite(config, urls=(URL, OTHER)), base=tmp_path)
    queue = NewsQueue(tmp_path/'queue',registry)
    outside = URL.replace('ARTL003020265','unreviewed')
    try:
        with queue.db:
            queue.add_articles([{'url':url} for url in (URL,OTHER,outside)])
        queue.sync_finite_cohorts()
        members = {r[0] for r in queue.db.execute('SELECT url FROM finite_cohort_members')}
        assert members == {URL,OTHER}
        assert queue.claim_url(outside,300) is None
        for url in (URL,OTHER):
            row = queue.claim_url(url,300)
            assert row['url'] == url and row['id'] == hashlib.sha256(url.encode()).digest()
            assert urlsplit(row['url']).query
    finally:
        queue.close()
