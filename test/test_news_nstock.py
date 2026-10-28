"""Finite nStock source identity, public-body and transport contracts."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_nstock import NSTOCK_REVIEWED_URLS, nstock_article_id
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


URL = NSTOCK_REVIEWED_URLS[0]
HEADLINE = '【公告】測試公司：第一則測試公告'
CLOCK = '2024-05-10 16:17:32'
BODY = ("<hr style='height:0.7rem;border:0'/>"
        "<div style='display:inline-block;background-color: #eeeeee;padding:0.2rem;"
        "padding-right:0.7rem;margin:-0.2rem;padding-left:0.7rem;"
        "border: 0.5px solid #9e9e9e'>公司代號</div>1234"
        "<hr style='height:0.7rem;border:0'/>公告內容：" + '來源公開公告文字。' * 24)


def sample(*, native=None, visible_title=HEADLINE, visible_clock=CLOCK,
           datetime_value='2024-05-10T16:17:32+08:00', byline='nStock快訊', body=BODY):
    values = {'id': '169138', 'title': HEADLINE, 'img': 'https://example.org/example.jpg',
              'category': '公司公告查詢', 'pubDate': CLOCK, 'author': 'nStock快訊',
              'summary': None, 'content': body, 'stock': '1234(TW)', 'keyword': None,
              'insteadKey': 'mops解析公告查詢_20240510_1234_第一則測試公告'}
    values.update(native or {})
    fields = ','.join(key + ':' + ('a' if value is None else json.dumps(value, ensure_ascii=False))
                      for key, value in values.items())
    return (f'<html><body><h1>{visible_title}</h1><time datetime="{datetime_value}">'
            f'發布時間：{visible_clock}</time><div class="text-gray-500">作者：{byline}</div>'
            f'<script>window.__NUXT__=(function(){{return {{article:{{{fields}}}}}}}())</script>'
            '</body></html>').encode()


@pytest.fixture(scope='module')
def installed():
    config, registry, repo = load_config(Path('configs/news_fulltext.json'))
    return config, registry, repo


def test_native_body_and_source_clock_are_kept_separate(installed):
    _, registry, _ = installed
    article = extract_article(sample(), registry.sources['nstock'], page_url=URL)
    assert article['title'] == HEADLINE
    assert article['published_at_raw'] == '2024-05-10T16:17:32+08:00'
    assert 'not_verified_MOPS_or_CNYES_release' in article['published_time_basis']
    assert article['extraction_method'] == 'nstock_literal_public_announcement_v1'
    assert article['authors_json'] == '["nStock快訊"]'
    assert article['body'].count('來源公開公告文字。') == 24
    assert article['historical_point_in_time'] is False
    assert json.loads(article['author_attribution_detail_json'])['claimed_MOPS_origin_not_independently_verified']


@pytest.mark.parametrize('bad_url', [
    'https://www.nstock.tw/news/article_m?id=169139',
    'https://www.nstock.tw/news/article_m?id=169138&utm_source=x',
    'https://nstock.tw/news/article_m?id=169138',
    'https://www.nstock.tw/news/article_m?id=0169138',
])
def test_only_literal_reviewed_urls_are_admitted(bad_url):
    assert nstock_article_id(bad_url) is None


@pytest.mark.parametrize('changes', [
    {'native': {'id': '455676'}},
    {'native': {'title': '另一則'}},
    {'native': {'category': '其他'}},
    {'native': {'pubDate': '2024-05-11 16:17:32'}},
    {'native': {'author': '第三方'}},
    {'native': {'stock': '5678(TW)'}},
    {'native': {'insteadKey': 'mops解析公告查詢_20240510_1234_別的公告'}},
    {'visible_title': '不一致'},
    {'visible_clock': '2024-05-10 16:17:33'},
    {'datetime_value': '2024-05-10T16:17:32+00:00'},
    {'byline': '第三方'},
    {'body': ''},
    {'body': BODY + '<div style="display:none">隱藏</div>'},
    {'body': BODY + '<script>malicious()</script>'},
    {'body': BODY + '<!--隱藏-->'},
])
def test_conflicting_identity_or_unreviewed_content_is_not_accepted(installed, changes):
    with pytest.raises(NewsError):
        extract_article(sample(**changes), installed[1].sources['nstock'], page_url=URL)


def test_duplicate_or_nonliteral_native_object_is_rejected(installed):
    source = installed[1].sources['nstock']
    raw = sample()
    duplicated = raw.replace(b'category:', b'id:"169138",category:', 1)
    computed = raw.replace(b'id:"169138"', b'id:someFunction()', 1)
    for payload in (duplicated, computed):
        with pytest.raises(NewsError):
            extract_article(payload, source, page_url=URL)


@pytest.mark.parametrize('mutation', [
    lambda p: p.update(acquisition_mode='continuous'),
    lambda p: p.update(finite_article_urls=[*p['finite_article_urls'],
                                            'https://www.nstock.tw/news/article_m?id=999999']),
    lambda p: p.update(finite_identity_query_keys=[]),
    lambda p: p.update(sitemaps=['https://www.nstock.tw/api/sitemap-news.xml']),
    lambda p: p.update(require_author_attribution=False),
])
def test_registry_rejects_unreviewed_scope_expansion(installed, mutation):
    config, _, repo = installed
    config = deepcopy(config)
    provider = next(p for p in config['providers'] if p['id'] == 'nstock')
    mutation(provider)
    with pytest.raises(ValueError):
        PolicyRegistry(config, base=repo)


class _HTTP:
    def __init__(self, registry, response):
        self.registry, self.response, self.calls = registry, response, []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def test_successful_transport_preserves_raw_and_source_provenance(installed):
    registry = installed[1]
    raw = sample()
    http = _HTTP(registry, Response(URL, 200, {'content-type': 'text/html'}, raw))
    article = fetch_article(http, {'url': URL, 'id': bytes.fromhex('01' * 16)})
    assert len(http.calls) == 1 and http.calls[0][1]['follow_redirects'] is False
    assert article['requested_url'] == article['final_url'] == URL
    assert article['provider'] == 'nstock' and article['raw_response_body'] == raw
    assert article['published_at_raw'] == '2024-05-10T16:17:32+08:00'
    assert article['url_resolution_basis'] == 'requested_public_url'
    assert article['publish'] is False and article['remote_training_export'] is False


def test_redirects_are_not_followed_or_accepted(installed):
    registry = installed[1]
    http = _HTTP(registry, Response(URL, 302, {'location': NSTOCK_REVIEWED_URLS[1]}, b''))
    with pytest.raises(NewsError, match='http_302'):
        fetch_article(http, {'url': URL})
    assert len(http.calls) == 1 and http.calls[0][1]['follow_redirects'] is False
    http = _HTTP(registry, Response(NSTOCK_REVIEWED_URLS[1], 200,
                                    {'content-type': 'text/html'}, sample()))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': URL})


def test_nonreviewed_request_is_rejected_before_http(installed):
    registry = installed[1]
    http = _HTTP(registry, None)
    with pytest.raises(NewsError, match='unsupported_article_path'):
        fetch_article(http, {'url': 'https://www.nstock.tw/news/article_m?id=999999'})
    assert http.calls == []
