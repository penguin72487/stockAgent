"""FTNN whole-report identity and private known-URL incremental acquisition."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import load_config, fetch_article
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, http_fixture


URL = 'https://www.ftnn.com.tw/news/587339'
TITLE = '原始報導保留-12.5%、股數123456與0.75'
PROSE = '負值-12.5%、123456股與比率0.75為報導原值，不是廣告或摘要。' * 5
NOTICE = '◎《FTNN新聞網》提醒您：本資料僅供參考，投資人應獨立判斷。'


def recipe():
    return {'id': 'ftnn', 'domains': ['ftnn.com.tw'], 'enabled': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-15',
            'evidence_url': 'https://www.ftnn.com.tw/copyright',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'Known public originals for private personal noncommercial reading; no redistribution or training'},
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 2, 'parallelism': 1, 'capacity_max_parallelism': 2,
        'html_parser': 'lxml', 'embedded_article_layout': 'ftnn_visible_article_v1',
        'article_path_pattern': '^/news/[1-9][0-9]*$',
        'article_selectors': [':scope > .news-body'], 'exclude_selectors': [],
        'title_selector': 'h1', 'require_configured_container': True, 'require_author_attribution': True,
        'feeds': [], 'sitemaps': [], 'wordpress_apis': [], 'fulltext_feeds': []}


def policy_config(parser='lxml'):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    return {**cfg, 'providers': [{**recipe(), 'html_parser': parser}]}


def policy(parser='lxml'):
    return PolicyRegistry(policy_config(parser), base=Path.cwd()).sources['ftnn']


def test_enabled_candidate_passes_the_whole_installed_source_contract(monkeypatch):
    # Exercise the post-cutover branch before any production configuration
    # write; a disabled FTNN registry cannot validate that branch.
    import test_news_fulltext as shared
    cfg, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(cfg)
    candidate['providers'] = [
        {**entry, **recipe()} if entry['id'] == 'ftnn' else entry
        for entry in candidate['providers']]
    registry = PolicyRegistry(candidate, base=repo)
    assert registry.sources['ftnn'].allowed
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


def page(parser='lxml', url=URL):
    declared = {'@context': 'http://schema.org/', '@type': 'NewsArticle',
        'headline': TITLE, 'description': '摘要不得冒充正文',
        'datePublished': '2026-10-14T22:00:15+08:00', 'dateModified': '2026-10-14T22:01:00+08:00',
        'author': {'@type': 'Person', 'name': '黃詩雯', 'url': 'https://www.ftnn.com.tw/search?author=a100103'},
        'publisher': {'@type': 'Organization', 'name': 'FTNN 新聞網'},
        'isAccessibleForFree': 'http://schema.org/True'}
    crumbs = {'@type': 'BreadcrumbList', 'itemListElement': [
        {'@type': 'ListItem', 'position': 3, 'item': {'@id': url, 'name': TITLE}}]}
    return BeautifulSoup(f'''<html><head><meta charset="utf-8">
        <link rel="canonical" href="{url}"><meta property="og:url" content="{url}">
        <meta property="og:title" content="{TITLE} | FTNN 新聞網">
        <script type="application/ld+json">{json.dumps(declared, ensure_ascii=False)}</script>
        <script type="application/ld+json">{json.dumps(crumbs, ensure_ascii=False)}</script></head><body>
        <div class="news-page-all"><div class="news-show"><div class="news-page-top">
        <div class="news-main-title"><h1>{TITLE}</h1></div>
        <div class="news-detail-text"><div class="ndt-left"><p>發布時間：2026/10/14 22:00</p></div></div></div>
        <div class="news-box"><div class="news-body"><p>記者黃詩雯／綜合報導</p><p>{PROSE}</p>
        <figure class="image"><picture><img src="https://media.invalid/photo.jpg"></picture><figcaption>獨立照片說明／原始出處</figcaption></figure>
        <p style="margin-bottom: 0px; text-align: center;">廣告 更多內容請繼續往下閱讀</p>
        <div id="div-gpt-ad-123-0" class="d-pc-show"></div><p>{NOTICE}</p></div></div></div>
        <div class="news-page-right"><h1>別篇標題</h1><p>別篇完整正文不可混入</p><time>2026/10/15 05:00</time></div></div>
        </body></html>''', parser)


def extract(soup, parser='lxml', url=URL):
    return extract_article(str(soup).encode(), policy(parser), page_url=url)


def update_schema(soup, field, value):
    node = soup.select('script[type="application/ld+json"]')[0]
    data = json.loads(node.string)
    data[field] = value
    node.string = json.dumps(data, ensure_ascii=False)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_whole_visible_report_native_author_clock_and_risk_notice(parser):
    soup = page(parser)
    before = str(soup)
    result = extract(soup, parser)
    assert PROSE in result['body'] and NOTICE in result['body']
    assert not any(value in result['body'] for value in ('摘要不得', '更多內容', '別篇完整', '記者黃詩雯', '獨立照片說明'))
    assert result['authors_json'] == '["黃詩雯"]'
    assert result['published_at_raw'] == '2026-10-14T22:00:15+08:00'
    assert result['modified_at_raw'] == '2026-10-14T22:01:00+08:00'
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert json.loads(result['verified_image_captions_json']) == ['獨立照片說明／原始出處']
    assert '2026/10/14 22:00' in result['verified_article_notices_json']
    assert 'whole_native_report' in result['body_completeness']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert str(soup) == before


@pytest.mark.parametrize('selector,attribute,value', [
    ('link[rel=canonical]', 'href', URL+'0'), ('meta[property="og:url"]', 'content', URL+'?a=1'),
    ('meta[property="og:title"]', 'content', '別篇 | FTNN 新聞網'),
    ('.news-show', 'hidden', ''), ('.news-body', 'style', 'display:none'),
    ('.news-body p:first-child', 'aria-hidden', 'true'), ('.ndt-left p', 'hidden', ''),
    ('.news-body p:nth-child(2)', 'style', 'visibility: hidden'),
])
def test_competing_identity_or_hidden_current_prose_is_rejected(selector, attribute, value):
    soup = page()
    soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('field,value', [
    ('headline', '其他文章'), ('url', URL+'0'), ('@id', URL+'0'), ('mainEntityOfPage', URL+'0'),
    ('publisher', 'bad type'), ('publisher', {'name': '其他新聞網'}),
    ('author', None), ('author', ['黃詩雯']), ('author', {'@type': 'Organization', 'name': '黃詩雯'}),
    ('author', {'@type': 'Person', 'name': '中央社', 'url': 'https://www.ftnn.com.tw/search?author=a100103'}),
    ('author', {'@type': 'Person', 'name': '黃詩雯', 'url': 'https://foreign.test/author'}),
    ('datePublished', '2026-10-14T22:02:00+08:00'), ('datePublished', '2026-10-14T22:00:00'),
    ('dateModified', '2026-10-14T21:00:00+08:00'), ('datePublished', '2026-02-30T22:00:00+08:00'),
    ('articleBody', PROSE), ('isAccessibleForFree', False), ('isAccessibleForFree', 'false'),
    ('isAccessibleForFree', 1), ('isAccessibleForFree', None),
])
def test_schema_is_not_a_loose_body_or_permission_fallback(field, value):
    soup = page()
    update_schema(soup, field, value)
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('change', ['schema_duplicate', 'breadcrumb_duplicate', 'body_duplicate', 'body_absent',
    'byline_absent', 'external_byline', 'script', 'unmatched_ad', 'ad_contains_prose', 'date_label', 'breadcrumb'])
def test_scope_and_content_changes_need_a_new_verified_representation(change):
    soup = page()
    if change.endswith('_duplicate'):
        selector = {'schema_duplicate': 'script', 'breadcrumb_duplicate': 'script:nth-of-type(2)',
                    'body_duplicate': '.news-body'}[change]
        node = soup.select_one(selector)
        node.insert_after(deepcopy(node))
    elif change == 'body_absent':
        soup.select_one('.news-body').decompose()
    elif change == 'byline_absent':
        soup.select_one('.news-body p').decompose()
    elif change == 'external_byline':
        soup.select_one('.news-body p').string = '中央社記者黃詩雯／綜合報導'
    elif change == 'script':
        soup.select_one('.news-body').append(soup.new_tag('script'))
    elif change == 'unmatched_ad':
        soup.select_one('[id^=div-gpt-ad]').decompose()
    elif change == 'ad_contains_prose':
        soup.select_one('[id^=div-gpt-ad]').string = '新加入的未知文字不得默默刪掉'
    elif change == 'date_label':
        soup.select_one('.ndt-left p').string = '更新時間：2026/10/14 22:00'
    else:
        node = soup.select('script')[1]
        node.string = json.dumps({'@type': 'BreadcrumbList', 'itemListElement': []})
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('url', ['https://www.ftnn.com.tw/news/0', URL+'/', URL+'?x=1',
    URL+'#body', 'https://ftnn.com.tw/news/587339', 'https://app.ftnn.com.tw/news/587339',
    'http://www.ftnn.com.tw/news/587339'])
def test_only_reviewed_normal_news_identity(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(), url=url)


@pytest.mark.parametrize('field,value', [
    ('id', 'other'), ('domains', ['other.test']), ('article_path_pattern', '^/'),
    ('require_configured_container', False), ('require_author_attribution', False),
    ('article_selectors', ['.news-body']), ('exclude_selectors', ['p']),
    ('acquisition_mode', 'finite'), ('private_known_url_scope', ''),
    ('feeds', ['https://www.ftnn.com.tw/feed']), ('wordpress_apis', ['https://www.ftnn.com.tw/wp-json']),
])
def test_native_mode_does_not_grant_a_different_host_or_discovery_scope(field, value):
    cfg = policy_config()
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


def test_actual_fetch_accept_receipt_restart_and_new_known_url(tmp_path):
    cfg = policy_config()
    second = 'https://www.ftnn.com.tw/news/587332'
    http, _ = http_fixture(cfg, tmp_path, {
        'https://www.ftnn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(str(page()).encode()), second: FakeResponse(str(page(url=second)).encode())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{'url': URL}])
        queue.accept([fetch_article(http, queue.claim_url(URL, 300))])
        queue.close()
        queue = NewsQueue(tmp_path, http.registry)
        queue.add_articles([{'url': second}])
        queue.accept([fetch_article(http, queue.claim_url(second, 300))])
        assert queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 2
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 2
        assert not queue.db.execute('SELECT 1 FROM discovery').fetchone()
        assert verify_bodies(tmp_path)['accepted']
    finally:
        queue.close()
        http.close()
