"""Source-witnessed magazine and legacy routes retain normal article checks."""
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_moneyweekly import (
    LEGACY_ARTICLE_PATTERN, NATIVE_ARTICLE_PATTERN, moneyweekly_article_id,
    validate_moneyweekly_native_response,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_news_reviewed_author_scope import page, reviewed_moneyweekly_config

MAGAZINE = 'https://www.moneyweekly.com.tw/Magazine/Info/%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A/257127'
LEGACY = 'https://www.moneyweekly.com.tw/_Article?AID=246465'
DESTINATION = 'https://www.moneyweekly.com.tw/ArticleData/Info/%e7%90%86%e8%b2%a1%e5%91%a8%e5%88%8a/246465'


def native_config():
    config = deepcopy(reviewed_moneyweekly_config())
    config.pop('provider_catalog', None)
    next(p for p in config['providers'] if p['id'] == 'moneyweekly')['article_path_pattern'] = NATIVE_ARTICLE_PATTERN
    return config


@pytest.fixture
def source():
    return PolicyRegistry(native_config(), base=Path.cwd()).sources['moneyweekly']


def magazine_page(**kwargs):
    payload, text = page(article_id='257127', **kwargs)
    payload = payload.replace(b'https://www.moneyweekly.com.tw/ArticleData/Info/Article/257127', MAGAZINE.encode())
    return payload.replace(b'</head>', f'<meta property="og:url" content="{MAGAZINE}"></head>'.encode()), text


def redirect_response(destination=DESTINATION, **changes):
    payload, _ = page(article_id='246465')
    hop = RedirectHop(LEGACY, 302, destination, destination, '2026-10-26T00:00:00+00:00', 'a'*64, 178)
    return replace(Response(destination, 200, {'content-type':'text/html'}, payload, (hop,)), **changes)


def test_route_parser_versions_only_the_reviewed_geometry_and_keeps_http_history(source, monkeypatch):
    config = reviewed_moneyweekly_config()
    entry = next(p for p in config['providers'] if p['id'] == 'moneyweekly')
    entry['article_path_pattern'] = LEGACY_ARTICLE_PATTERN
    with monkeypatch.context() as predecessor:
        predecessor.setattr('downloader.news_fulltext_moneyweekly_outcomes.CONTRACT_VERSION', 0)
        old = PolicyRegistry(config, base=Path.cwd()).sources['moneyweekly']
        native_predecessor = PolicyRegistry(native_config(), base=Path.cwd()).sources['moneyweekly']
    assert old.data_fingerprint == '4ba474c2bac6f94e5d9475680a63fa1244bb5630a6e792ab4c08d199890598e1'
    assert source.data_fingerprint != old.data_fingerprint
    assert source.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert native_predecessor.data_fingerprint in source.parser_repair_capture_history
    assert PolicyRegistry(config, base=Path.cwd()).sources['moneyweekly'].data_fingerprint == old.data_fingerprint
    assert old.data_fingerprint in source.parser_repair_capture_history
    assert source.parser_repair_fingerprint == native_predecessor.parser_repair_fingerprint
    assert 'ae39f183bffb6a696815319a3ecc17b0d2b19418d30d51a16c5a81fa1a9d053d' in source.parser_repair_capture_history
    assert source.authorization_fingerprint == old.authorization_fingerprint
    assert source.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert source.http_capacity_authorization_fingerprint == old.http_capacity_authorization_fingerprint
    assert source.interval_seconds == old.interval_seconds == 2


@pytest.mark.parametrize('field,value', [
    ('interval_seconds', 1), ('capacity_max_parallelism', 3),
    ('domains', ['www.moneyweekly.com.tw']), ('permitted_author_names', ['任意作者']),
    ('article_path_pattern', NATIVE_ARTICLE_PATTERN + '|/other'),
])
def test_unreviewed_changes_cannot_inherit_transport_or_repair_witnesses(field, value):
    config = native_config()
    next(p for p in config['providers'] if p['id'] == 'moneyweekly')[field] = value
    source = PolicyRegistry(config, base=Path.cwd()).sources['moneyweekly']
    assert not source.http_capacity_source_fingerprint
    assert not source.http_capacity_authorization_fingerprint


def test_magazine_accepts_the_literal_canonical_body_without_changing_raw_values(source):
    payload, text = magazine_page()
    result = extract_article(payload, source, page_url=MAGAZINE)
    assert result['body'] == text and '-12.5' in result['body']
    assert json.loads(result['authors_json']) == ['理周投研部']
    assert result['published_at_raw'] == '2026/10/8'
    assert moneyweekly_article_id(MAGAZINE) is None
    assert moneyweekly_article_id(MAGAZINE, version=4) == '257127'


@pytest.mark.parametrize('change', ['different_id', 'borrowed_canonical', 'duplicate_canonical', 'duplicate_body'])
def test_magazine_keeps_identity_and_whole_body_guards(source, change):
    payload, _ = magazine_page()
    if change == 'different_id':
        payload = payload.replace(b'/257127', b'/257128')
    elif change == 'borrowed_canonical':
        payload = payload.replace(MAGAZINE.encode(), b'https://www.moneyweekly.com.tw/ArticleData/Info/Article/257127')
    elif change == 'duplicate_canonical':
        payload = payload.replace(b'</head>', f'<link rel="canonical" href="{MAGAZINE}"></head>'.encode())
    else:
        payload = payload.replace(b'<h2>', '<div class="div_Article_Info" itemprop="articleBody">'.encode()+b'<h2>')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, source, page_url=MAGAZINE)


def test_magazine_member_preview_is_not_a_complete_body(source):
    payload, _ = magazine_page()
    payload = payload.replace(b'<p>', '<div class="mt-5">》本文限會員登入瀏覽，請先登入 登入會員</div><p>'.encode(), 1)
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract_article(payload, source, page_url=MAGAZINE)


@pytest.mark.parametrize('author', ['', '第三方新聞社', '理周投研部 第三方作者'])
def test_magazine_does_not_borrow_a_permitted_author_from_schema(source, author):
    payload, _ = magazine_page(author=author, schema_author='理周投研部')
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(payload, source, page_url=MAGAZINE)


def test_literal_legacy_redirect_is_fetched_once_and_retained_as_provenance(source):
    response = redirect_response()
    calls = []
    def get(url, *, headers):
        calls.append((url, headers))
        return response
    transport = SimpleNamespace(registry=SimpleNamespace(require=lambda url: source), get=get)
    result = fetch_article(transport, {'id':hashlib.sha256(LEGACY.encode()).digest(), 'url':LEGACY})
    assert calls == [(LEGACY, {})]
    assert result['requested_url'] == LEGACY and result['final_url'] == DESTINATION
    assert result['raw_response_body'] == response.body
    assert result['url_resolution_basis'].startswith('observed_moneyweekly_same_id_source_redirect_v5')
    assert json.loads(result['transport_redirects_json'])[0]['location'] == DESTINATION


def test_actual_source_relative_location_is_resolved_without_rewriting_or_extra_requests(source):
    response = redirect_response()
    relative = urlsplit(DESTINATION).path
    assert hashlib.sha256(relative.encode()).hexdigest() == '0e8da1fff598a662605a922561cf1249524a22ea3bf8fd7a3f9769e950721ad0'
    response = replace(response, redirects=(replace(response.redirects[0], location=relative),))
    calls = []
    def get(url, *, headers):
        calls.append(url)
        return response
    transport = SimpleNamespace(registry=SimpleNamespace(require=lambda url: source), get=get)
    result = fetch_article(transport, {'id':hashlib.sha256(LEGACY.encode()).digest(), 'url':LEGACY})
    assert calls == [LEGACY] and result['requested_url'] == LEGACY and result['final_url'] == DESTINATION
    assert json.loads(result['transport_redirects_json'])[0]['location'] == relative


def test_failed_native_redirect_transfers_the_received_response_for_bounded_diagnosis(source):
    response = replace(redirect_response(), redirects=())
    transport = SimpleNamespace(registry=SimpleNamespace(require=lambda url: source), get=lambda *a, **k:response)
    with pytest.raises(NewsError, match='article_identity_not_verified') as failure:
        fetch_article(transport, {'url':LEGACY})
    assert failure.value.failure_response.response is response
    assert failure.value.failure_response.requested_url == LEGACY


def test_native_failure_metadata_retains_relative_hop_facts_without_prose_or_new_http(tmp_path):
    from downloader.news_fulltext_diagnostics import retain_identity_metadata
    from downloader.news_fulltext_queue import NewsQueue
    config = native_config()
    config.update(root=str(tmp_path), exclusive_http_owner=False, min_free_disk_bytes=1)
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=Path.cwd()))
    try:
        queue.add_articles([{'url':LEGACY}])
        row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(LEGACY,)).fetchone())
        response = redirect_response(DESTINATION.replace('246465','246466'))
        relative = urlsplit(response.url).path
        response = replace(response, redirects=(replace(response.redirects[0],location=relative),))
        calls = []
        def get(url, *, headers):
            calls.append(url)
            return response
        transport = SimpleNamespace(registry=queue.registry,get=get)
        with pytest.raises(NewsError) as failure:
            fetch_article(transport,row)
        retain_identity_metadata(queue,row,failure.value,failure.value.failure_response)
        paths = list((tmp_path/'identity_metadata').glob('*/moneyweekly/*.json'))
        assert len(paths) == 1 and calls == [LEGACY]
        value = json.loads(paths[0].read_bytes())
        hop = value['facts']['moneyweekly_route_transport']['hops'][0]
        assert hop['location_is_relative'] and hop['location_sha256'] == hashlib.sha256(relative.encode()).hexdigest()
        assert hop['to_url'] == response.url and not value['raw_response_retained']
        assert not value['accepted_as_article'] and not value['publish']
        assert '原始營收' not in json.dumps(value,ensure_ascii=False)
    finally:
        queue.close()


@pytest.mark.parametrize('change', ['wrong_id', 'foreign_host', 'query', 'fragment', 'http', 'no_hop', 'multi_hop', '301', 'wrong_location'])
def test_legacy_redirect_cannot_guess_or_accept_other_identities(source, change):
    response = redirect_response()
    if change in {'wrong_id','foreign_host','query','fragment','http'}:
        destination = {'wrong_id':DESTINATION.replace('246465','246466'),
            'foreign_host':DESTINATION.replace('www.moneyweekly.com.tw','evil.test'),
            'query':DESTINATION+'?source=other', 'fragment':DESTINATION+'#other',
            'http':DESTINATION.replace('https:', 'http:')}[change]
        response = redirect_response(destination)
    elif change == 'no_hop': response = replace(response, redirects=())
    elif change == 'multi_hop': response = replace(response, redirects=response.redirects*2)
    elif change == '301': response = replace(response, redirects=(replace(response.redirects[0],status=301),))
    else: response = replace(response, redirects=(replace(response.redirects[0],location=LEGACY),))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        validate_moneyweekly_native_response(source, LEGACY, response)


@pytest.mark.parametrize('url', [LEGACY+'&extra=1', LEGACY.replace('AID=', 'aid='), MAGAZINE+'?extra=1'])
def test_unobserved_routes_fail_before_any_http(source, url):
    transport = SimpleNamespace(registry=SimpleNamespace(require=lambda u: source),
        get=lambda *a, **k: pytest.fail('unsupported route must not request'))
    with pytest.raises(NewsError, match='unsupported_article_path'):
        fetch_article(transport, {'url':url})
