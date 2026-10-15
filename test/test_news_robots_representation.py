from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import time

import pytest
from protego import Protego

from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_robots import robots_representation
from test_news_fulltext import config, http_fixture, FakeResponse


@pytest.fixture
def source(config, tmp_path):
    return replace(PolicyRegistry(config,base=tmp_path).sources["publisher"],
                   index_pages=("https://publisher.test/",), article_path_pattern=r"^/article/[0-9]+$")


def home(*, title="Publisher", links="", canonical="https://publisher.test/", extra=""):
    links = links or '<a href="/article/1">one</a><a href="/article/2">two</a>'
    return f'<html><head><title>{title}</title><link rel="canonical" href="{canonical}"></head><body>{links}{extra}</body></html>'.encode()


def representation(body,source,*,url="https://publisher.test/",headers=None,origin="https://publisher.test"):
    return robots_representation(Response(url,200,headers or {"content-type":"text/html"},body),source,origin)


def test_plain_utf8_bom_retains_original_rule_meaning(source):
    result = representation(b'\xef\xbb\xbfUser-agent: *\r\nDisallow: /private\r\nCrawl-delay: 2.5',source,url='https://publisher.test/robots.txt')
    parser = Protego.parse(result['body'])
    assert result['representation_kind'] == 'plain_utf8'
    assert not parser.can_fetch('https://publisher.test/private','StockAgent')
    assert parser.crawl_delay('StockAgent') == 2.5


@pytest.mark.parametrize('body',[
    b'<html><body><pre>\nUser-agent: *\nDisallow: /private\nAllow: /private/public\nCrawl-delay: 2.5\n</pre></body></html>',
    b'<html><body><pre>User-agent: *<br>Disallow: /private<br>Allow: /private/public<br>Crawl-delay: 2.5</pre></body></html>',
])
def test_html_rules_are_parsed_not_ignored(source,body):
    result = representation(body,source,url='https://publisher.test/robots.txt')
    parser = Protego.parse(result['body'])
    assert result['representation_kind']=='html_valid_rule_lines'
    assert not parser.can_fetch('https://publisher.test/private','StockAgent')
    assert parser.can_fetch('https://publisher.test/private/public/1','StockAgent')
    assert parser.crawl_delay('StockAgent')==2.5
    assert '<html' not in result['body']


def test_html_rule_groups_keep_distinct_agents(source):
    body=b'<pre>User-agent: *<br>Disallow: /a<br>User-agent: StockAgentNewsResearch<br>Disallow: /b</pre>'
    parser=Protego.parse(representation(body,source)['body'])
    assert not parser.can_fetch('https://publisher.test/b','StockAgentNewsResearch')
    assert parser.can_fetch('https://publisher.test/a','StockAgentNewsResearch')


def test_registered_ordinary_home_redirect_is_not_a_prohibition(source):
    result=representation(home(),source)
    assert result['body']==''
    assert result['representation_kind']=='registered_home_without_rep_groups'
    assert result['raw_response_sha256'] and result['raw_response_bytes']>0
    assert 'Publisher' not in json.dumps({k:v for k,v in result.items() if k!='content_type'})


@pytest.mark.parametrize('body,url',[
    (home(title='Just a moment'),'https://publisher.test/'),
    (home(extra='<div id="challenge-form"></div>'),'https://publisher.test/'),
    (home(extra='<div data-sitekey="test"></div>'),'https://publisher.test/'),
    (home(extra='<input type="password">'),'https://publisher.test/'),
    (home(extra='<form action="/login"></form>'),'https://publisher.test/'),
    (home(extra='<meta http-equiv="refresh" content="1;url=/">'),'https://publisher.test/'),
    (home(),'https://publisher.test/robots.txt'),
    (home(),'https://publisher.test/?login=true'),
    (home(canonical='https://other.test/'),'https://publisher.test/'),
    (home(links='<a href="https://other.test/article/1">one</a><a href="https://other.test/article/2">two</a>'),'https://publisher.test/'),
    (home(links='<a href="/article/1">one</a><a href="/article/1">same</a>'),'https://publisher.test/'),
    (home(title='系統異常'),'https://publisher.test/'),
    (home(title='Forbidden'),'https://publisher.test/'),
    (b'<?xml version="1.0"?><sitemap><url>https://publisher.test/</url></sitemap>','https://publisher.test/robots.txt'),
])
def test_challenge_login_error_and_unregistered_html_are_not_permission(source,body,url):
    with pytest.raises(NewsError,match='robots_'):
        representation(body,source,url=url)


def test_header_challenge_not_treated_as_a_missing_policy(source):
    with pytest.raises(NewsError,match='robots_access_challenge'):
        representation(home(),source,headers={'content-type':'text/html','cf-mitigated':'challenge'})


@pytest.mark.parametrize('body', [b'User-agent: *\nAllow: /', b'Verify you are human'])
def test_plain_text_challenge_header_is_not_rep_permission(source, body):
    with pytest.raises(NewsError, match='robots_access_challenge'):
        representation(body, source, headers={'content-type': 'text/plain', 'cf-mitigated': 'challenge'})


def test_registered_https_home_handles_actual_same_host_http_upgrade(source):
    result = representation(home(), source, origin='http://publisher.test')
    assert result['representation_kind'] == 'registered_home_without_rep_groups'
    assert result['initial_origin'] == 'http://publisher.test'
    assert result['registered_home_url'] == 'https://publisher.test/'
    assert result['body'] == ''


@pytest.mark.parametrize('origin,url', [
    ('https://publisher.test', 'http://publisher.test/'),
    ('http://other.test', 'https://publisher.test/'),
])
def test_home_representation_does_not_allow_host_change_or_https_downgrade(source, origin, url):
    with pytest.raises(NewsError, match='robots_not_plain_rules'):
        representation(home(canonical=url), source, origin=origin, url=url)


def test_actual_http_upgrade_keeps_both_origin_robots_and_original_article_identity(config, tmp_path):
    config['providers'][0].update(index_pages=['https://publisher.test/'], article_path_pattern='^/article/[0-9]+$')
    original, final = 'http://publisher.test/article/1', 'https://publisher.test/article/1'
    http, session = http_fixture(config, tmp_path, {
        'http://publisher.test/robots.txt': FakeResponse(status=301, headers={'Location': 'https://publisher.test/robots.txt'}),
        'https://publisher.test/robots.txt': FakeResponse(status=302, headers={'Location': '/'}),
        'https://publisher.test/': FakeResponse(home()),
        original: FakeResponse(status=301, headers={'Location': final}),
        final: FakeResponse(b'<p>body</p>'),
    })
    try:
        assert http.get(original).url == final
        caches = [json.loads(path.read_text()) for path in (tmp_path / 'robots').glob('*.json')]
        assert {c['origin'] for c in caches} == {'http://publisher.test', 'https://publisher.test'}
        assert all(c['registered_home_url'] == 'https://publisher.test/' and c['body'] == '' for c in caches)
        assert [r[0] for r in session.calls] == [
            'http://publisher.test/robots.txt', 'https://publisher.test/robots.txt', 'https://publisher.test/',
            original, 'https://publisher.test/robots.txt', 'https://publisher.test/', final,
        ]
        assert not http.provider_not_before
    finally:
        http.close()


def test_unregistered_home_remains_blocked(source):
    with pytest.raises(NewsError,match='robots_not_plain_rules'):
        representation(home(),replace(source,index_pages=()))


def test_registered_home_without_rules_is_cached_without_home_text(config,tmp_path):
    config['providers'][0].update(index_pages=['https://publisher.test/'],article_path_pattern='^/article/[0-9]+$',title_selector='')
    url='https://publisher.test/article/1'
    http,session=http_fixture(config,tmp_path,{
        'https://publisher.test/robots.txt':FakeResponse(status=302,headers={'Location':'/'}),
        'https://publisher.test/':FakeResponse(home()),url:FakeResponse(b'<p>body</p>'),
    })
    try:
        assert http.get(url).status==200
        assert http.get(url).status==200
        cache=json.loads(next((tmp_path/'robots').glob('*.json')).read_text())
        assert cache['representation_kind']=='registered_home_without_rep_groups' and cache['body']==''
        assert '<html' not in json.dumps(cache)
        assert [r[0] for r in session.calls]==['https://publisher.test/robots.txt','https://publisher.test/',url,url]
        assert not http.provider_not_before
    finally:
        http.close()


def test_html_disallow_still_prevents_article_http(config,tmp_path):
    url='https://publisher.test/article/1'
    http,session=http_fixture(config,tmp_path,{'https://publisher.test/robots.txt':FakeResponse(b'<pre>User-agent: *<br>Disallow: /</pre>')})
    try:
        with pytest.raises(NewsError,match='robots_disallowed'):
            http.get(url)
        assert len(session.calls)==1
    finally:
        http.close()


def test_five_robots_redirects_are_followed(config,tmp_path):
    routes={}
    start='https://publisher.test/robots.txt'
    for i in range(5):
        routes[start if i==0 else f'https://publisher.test/r{i}']=FakeResponse(status=302,headers={'Location':f'/r{i+1}'})
    routes['https://publisher.test/r5']=FakeResponse(b'User-agent: *\nDisallow: /')
    http,session=http_fixture(config,tmp_path,routes)
    try:
        with pytest.raises(NewsError,match='robots_disallowed'):
            http.get('https://publisher.test/article/1')
        assert len(session.calls)==6
    finally:
        http.close()
