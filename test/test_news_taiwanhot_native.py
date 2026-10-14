"""Native visible news/focus/legacy identity, not summaries or inferred clocks."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article, load_config, seed_discovery
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue


URL = 'https://www.taiwanhot.net/news/907503/%E6%9C%AC%E7%AF%87%E6%96%B0%E8%81%9E'
FOCUS = URL.replace('/news/', '/news/focus/') + '/70/%E7%94%9F%E6%B4%BB'
LEGACY = 'http://www.taiwanhot.net/?p=907503'
TEXT = '這是本篇原始正文：交易量12345股、負值-12.5與比率3.25%，不得改寫數值或抓入側欄。' * 4


def config():
    cfg = load_config(Path('configs/news_fulltext.json'))[0]
    # No dependency on ignored private probes, retained source text or secrets.
    entry = {
        'id': 'taiwanhot', 'name': '台灣好新聞', 'domains': ['taiwanhot.net'],
        'enabled': True, 'acquisition_mode': 'continuous',
        'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'html_parser': 'lxml', 'embedded_article_layout': 'taiwanhot_visible_article_v1',
        'article_path_pattern': '^(?:/news/(?:focus/)?[1-9][0-9]*/|/$)',
        'article_selectors': ['article > .news_content'], 'title_selector': '.top_title > h2.news_title',
        'author_selectors': ['.top_title > h5.txt_gray2 > span.reporter_name'],
        'publication_selectors': ['.top_title > h5.txt_gray2 > span.post_time'],
        'exclude_selectors': ['.photo_wrap'], 'require_configured_container': True,
        'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'evidence_url': 'https://www.taiwanhot.net/',
            'scope': 'Known public pages privately retained with actual source credits, not a reuse license.',
            'reviewed_on': '2026-10-14', 'automated_access_review': 'no_explicit_prohibition_identified'},
    }
    return {**cfg, 'providers': [entry]}


@pytest.fixture(params=['html.parser', 'lxml'])
def source(request):
    cfg = config()
    cfg['providers'][0]['html_parser'] = request.param
    return PolicyRegistry(cfg, base=Path.cwd()).sources['taiwanhot']


def page(*, og_url=URL, title='本篇新聞', author='理財周刊／新聞中心', clock='2026-10-14 15:30',
         body=TEXT, extra_head='', extra_body='', related=True, body_attributes='', author_attributes=''):
    tail = ('<h3>【延伸閱讀】</h3><ul><li><a href="https://third.test/news/1">不是本篇正文</a></li></ul><br>'
            if related else '')
    return f'''<html><head><meta charset="utf-8"><title>{title} | 台灣好新聞</title>
      <meta name="og:url" content="{og_url}"><meta name="og:title" content="{title}">
      <meta property="og:title" content="{title}">{extra_head}</head><body>
      <h1>網站標題不是文章</h1><div class="main_content"><div class="content_wrapper">
      <div class="top_title"><h2 class="news_title">{title}</h2><h5 class="txt_gray2">
      <span class="reporter_name" {author_attributes}>{author}</span>
      <span class="post_time"><span class="glyphicon glyphicon-time"></span> {clock}</span></h5></div>
      <article><div class="news_content" {body_attributes}><div class="photo_wrap"><img src="/photo.jpg">第三方圖說</div>
      <h2>原始小標</h2><p>{body}</p>{tail}</div></article>{extra_body}</div>
      <aside><p>側欄正文{TEXT}</p><span class="reporter_name">錯誤作者</span><time>2099-01-01</time></aside></div>
      <footer><span class="post_time">2099-01-01 23:59</span>All Rights Reserved</footer></body></html>'''.encode()


def test_news_and_focus_same_native_id_preserve_real_byline_and_precision(source):
    normal = extract_article(page(), source, page_url=URL)
    focus = extract_article(page(), source, page_url=FOCUS)
    assert normal == focus
    assert normal['body'] == '原始小標\n' + TEXT
    assert json.loads(normal['authors_json']) == ['理財周刊／新聞中心']
    assert normal['published_at_raw'] == '2026-10-14 15:30'
    assert 'no_timezone_or_missing_precision_inferred' in normal['published_time_basis']
    assert normal['modified_at_raw'] is None and normal['historical_point_in_time'] is False
    assert normal['body_completeness'] == 'verified_current_article_container'
    detail = json.loads(normal['author_attribution_detail_json'])
    assert detail['byline_raw'] == '理財周刊／新聞中心'
    assert detail['rights_owner_inferred'] is False
    assert not normal['per_article_license_verified']


def test_editorial_organization_is_not_split_into_invented_people(source):
    result = extract_article(page(author='政治中心／綜合報導'), source, page_url=URL)
    assert json.loads(result['authors_json']) == ['政治中心／綜合報導']


def test_unknown_related_reading_shape_is_not_silently_deleted(source):
    html = page(related=False).replace(b'</div></article>',
        '<h3>【延伸閱讀】</h3><p>後續是原始段落，不是已驗證的連結清单。</p></div></article>'.encode())
    result = extract_article(html, source, page_url=URL)
    assert '後續是原始段落' in result['body']


def test_actual_unclosed_block_paragraph_wrapper_preserves_prose_in_both_parsers(source):
    html = page().replace('<h2>原始小標</h2>'.encode(), '<p><h2>原始小標</h2>'.encode())
    html = html.replace(b'</div></article>', b'</p></div></article>')
    result = extract_article(html, source, page_url=URL)
    assert result['body'] == '原始小標\n' + TEXT


@pytest.mark.parametrize('mutation', [
    lambda html: html.replace('name="og:url"', 'name="missing:url"'),
    lambda html: html.replace('property="og:title" content="本篇新聞"', 'property="og:title" content="另一篇"'),
    lambda html: html.replace('</head>', f'<meta property="og:url" content="{URL}"></head>'),
    lambda html: html.replace('</head>', f'<link rel="canonical" href="{URL.replace("907503", "907502")}"></head>'),
    lambda html: html.replace('</head>', f'<link rel="canonical" href="https://evil.test/news/907503/test"></head>'),
    lambda html: html.replace('class="news_title"', 'class="missing_title"'),
    lambda html: html.replace('class="news_title"', 'class="news_title" hidden'),
    lambda html: html.replace('</div></article>', '</div><div class="news_content">第二正文</div></article>'),
])
def test_conflicting_missing_duplicate_or_hidden_current_identity_is_rejected(source, mutation):
    with pytest.raises(NewsError, match='article_identity_not_verified|body_not_found'):
        extract_article(mutation(page().decode()).encode(), source, page_url=URL)


@pytest.mark.parametrize('url', [
    URL.replace('907503', '907502'), URL.replace('www.taiwanhot.net', 'evil.test'),
    URL.replace('https:', 'http:'), URL + '?p=907503', LEGACY,
    URL.replace('/news/907503/', '/news/907503/%2F'), 'https://www.taiwanhot.net/news/907503/',
])
def test_final_response_requires_verified_https_native_article_not_legacy_query(source, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(page(), source, page_url=url)


@pytest.mark.parametrize('author,attributes', [('', ''), (' ', ''), ('政治中心', 'hidden'),
    ('政治中心', 'aria-hidden="true"'), ('政治中心', 'style="display: none"')])
def test_source_author_must_be_visible_nonempty_without_footer_fallback(source, author, attributes):
    with pytest.raises(NewsError, match='article_attribution_not_verified|article_identity_not_verified'):
        extract_article(page(author=author, author_attributes=attributes), source, page_url=URL)


@pytest.mark.parametrize('clock', ['', '2026-02-30 15:30', '2026-10-14', '2026-10-14 15:30:00',
    '2026-10-14 15:30+08:00', '2026-1-4 5:30', '2026-10-14 15:30 更新2026-10-14 18:00'])
def test_clock_does_not_invent_publication_precision_or_use_metadata_day(source, clock):
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract_article(page(clock=clock), source, page_url=URL)


def test_optional_declared_publication_must_match_visible_minute(source):
    result = extract_article(page(extra_head='<meta property="article:published_time" content="2026-10-14T15:30:45+08:00">'), source, page_url=URL)
    assert result['published_at_raw'] == '2026-10-14T15:30:45+08:00'
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract_article(page(extra_head='<meta property="article:published_time" content="2026-10-14T16:30:45+08:00">'), source, page_url=URL)


@pytest.mark.parametrize('extra_head,extra_body,body', [
    ('', '<div class="paywall">付費</div>', TEXT),
    (f'<script type="application/ld+json">{{"@type":"NewsArticle","url":"{URL}","isAccessibleForFree":false}}</script>', '', TEXT),
    ('', '', '本文禁止轉載。' + TEXT),
])
def test_restricted_body_and_current_paid_declaration_are_not_accepted(source, extra_head, extra_body, body):
    with pytest.raises(NewsError, match='paywall_or_login|article_rights_or_scope_excluded'):
        extract_article(page(extra_head=extra_head, extra_body=extra_body, body=body), source, page_url=FOCUS)


@pytest.mark.parametrize('attribute', ['hidden', 'aria-hidden="true"', 'style="visibility: hidden"'])
def test_hidden_body_is_not_recovered_from_page_or_schema(source, attribute):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(page(body_attributes=attribute), source, page_url=URL)


def test_native_paid_neighbor_schema_is_not_the_current_article(source):
    other = json.dumps({'@type': 'NewsArticle', 'url': URL.replace('907503', '907504'), 'isAccessibleForFree': False})
    result = extract_article(page(extra_head=f'<script type="application/ld+json">{other}</script>'), source, page_url=URL)
    assert TEXT in result['body']


@pytest.mark.parametrize('request_url', [LEGACY, LEGACY.replace('http:', 'https:'),
    LEGACY.replace('www.', ''), LEGACY.replace('www.', 'web.'), URL, FOCUS])
def test_native_source_redirect_reuses_one_transport_and_retains_original_url(request_url):
    registry = PolicyRegistry(config(), base=Path.cwd())
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return Response(URL, 200, {'content-type': 'text/html'}, page())
    record = fetch_article(SimpleNamespace(registry=registry, get=get),
                           {'url': request_url, 'id': hashlib.sha256(request_url.encode()).digest()})
    assert calls == [request_url]
    assert record['requested_url'] == request_url and record['final_url'] == URL
    assert record['raw_response_body'] == page() and record['published_at_raw'] == '2026-10-14 15:30'
    assert record['publish'] is False and record['remote_training_export'] is False


@pytest.mark.parametrize('url', ['https://www.taiwanhot.net/', LEGACY + '&p=907502', LEGACY + '&extra=1',
    LEGACY.replace('907503', '0'), LEGACY.replace('907503', '%39%30%37%35%30%33'),
    'https://www.taiwanhot.net/account', 'https://www.taiwanhot.net/news/907503/',
    'https://api.taiwanhot.net/?p=907503'])
def test_unknown_request_identity_rejected_before_transport(url):
    registry = PolicyRegistry(config(), base=Path.cwd())
    def get(*args, **kwargs):
        pytest.fail('unreviewed URL sent to source')
    with pytest.raises(NewsError, match='unsupported_article_path'):
        fetch_article(SimpleNamespace(registry=registry, get=get), {'url': url})


def test_legacy_redirect_to_different_article_cannot_become_a_success():
    registry = PolicyRegistry(config(), base=Path.cwd())
    response = Response(URL.replace('907503', '907502'), 200, {'content-type': 'text/html'}, page(og_url=URL.replace('907503', '907502')))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(SimpleNamespace(registry=registry, get=lambda *a, **kw: response), {'url': LEGACY})


@pytest.mark.parametrize('field,value', [
    ('domains', ['other.test']), ('require_configured_container', False),
    ('require_author_attribution', False), ('article_selectors', ['body']),
    ('article_path_pattern', '^/'), ('title_selector', 'h1'),
    ('author_selectors', ['footer']), ('publication_selectors', ['time']),
    ('private_known_url_scope', ''), ('exclude_selectors', ['p']),
])
def test_native_layout_contract_does_not_grant_generic_or_page_wide_capture(field, value):
    cfg = config()
    cfg['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('field', ['feeds', 'sitemaps', 'index_pages', 'wordpress_apis', 'metadata_feeds'])
def test_known_url_scope_cannot_be_expanded_to_a_discovery_endpoint(field):
    cfg = config()
    cfg['providers'][0][field] = ['https://www.taiwanhot.net/']
    if field == 'wordpress_apis':
        cfg['providers'][0]['bulk_text_permission_basis'] = 'not_a_known_URL_discovery_grant'
    with pytest.raises(ValueError, match='known-URL-only'):
        PolicyRegistry(cfg, base=Path.cwd())


def test_every_current_and_later_locally_known_url_is_admitted_without_discovery(tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config(), base=Path.cwd()))
    try:
        queue.add_articles([{'url': URL, 'date': '2026-10-14'}, {'url': LEGACY, 'date': '2021-02-23'}])
        seed_discovery(queue)
        assert queue.db.execute('SELECT COUNT(*) FROM discovery').fetchone()[0] == 0
        assert set(r[0] for r in queue.db.execute('SELECT state FROM articles')) == {'pending'}
        later = URL.replace('907503', '907504')
        queue.add_articles([{'url': later}])
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (later,)).fetchone()[0] == 'pending'
        assert queue.db.execute('SELECT COUNT(*) FROM finite_cohorts').fetchone()[0] == 0
    finally:
        queue.close()
