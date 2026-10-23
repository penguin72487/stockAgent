"""Public native text and declared credits must not widen collection rights."""
from copy import deepcopy
import json

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_stockfeel import (
    STOCKFEEL_BODY_SELECTOR, STOCKFEEL_ORIGIN, STOCKFEEL_PATH_PATTERN, stockfeel_url_key,
)
from test_news_fulltext import config


URL = STOCKFEEL_ORIGIN + '/%E6%B8%AC%E8%A9%A6-2026q2/'
CANONICAL = STOCKFEEL_ORIGIN + '/測試-2026q2/'
TITLE = '合成測試 2026Q2'
AUTHOR = '股感時事特派員'
PROFILE = STOCKFEEL_ORIGIN + '/author/news'


@pytest.fixture
def settings(config):
    result = deepcopy(config)
    result['providers'] = [{'id': 'stockfeel', 'name': 'Synthetic native source',
        'domains': ['www.stockfeel.com.tw'], 'enabled': True, 'acquisition_mode': 'continuous',
        'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'embedded_article_layout': 'stockfeel_visible_original_v1', 'html_parser': 'lxml',
        'article_path_pattern': STOCKFEEL_PATH_PATTERN, 'article_selectors': [STOCKFEEL_BODY_SELECTOR],
        'title_selector': 'h1', 'exclude_selectors': [], 'author_selectors': [],
        'require_author_attribution': True, 'require_configured_container': True,
        'authorization': {'kind': 'personal_research',
            'evidence_url': STOCKFEEL_ORIGIN + '/member/?view=copyright',
            'reviewed_on': '2026-10-22', 'scope': 'Synthetic private known original; not a reuse grant.',
            'automated_access_review': 'no_explicit_prohibition_identified'}}]
    return result


@pytest.fixture
def html():
    """Synthetic prose only; no downloaded publisher article is a Git fixture."""
    title = TITLE + ' - StockFeel 股感'
    credit = lambda label: f'<div data-aid="427" style="visibility: hidden;"><span>{label}</span><br><span><a href="{PROFILE}">{AUTHOR}</a></span></div>'
    return f'''<html><head><title>{title}</title><meta property="og:title" content="{title}">
    <meta property="og:title" content="{title}"><meta property="og:type" content="article">
    <meta property="og:type" content="article"><link rel="canonical" href="{CANONICAL}">
    <meta property="og:url" content="{CANONICAL}"><meta name="author" content="{AUTHOR}">
    <meta property="article:published_time" content="2026-10-15T07:07:51+00:00">
    <meta property="article:modified_time" content="2026-10-15T07:07:55+00:00"></head><body>
    <div class="single-content"><div class="row"><div class="col-md-12"><div class="sin-content-wrapper">
    <div class="sin-content-header"><h1>{TITLE}</h1><p class="text-gray-3">最近更新時間： 15 October, 2026</p>
    <div class="flex"><a class="user-avatar" href="{PROFILE}"><img src="/not-requested.jpg"></a>
    <div class="flex-wrap">{credit('作者')}{credit('文章來源')}</div></div></div>
    <article class="sin-content-body"><div class="sin-content"><div class="sin-content-original">
    <p>{'原始合成段落。' * 25}</p><table><tr><th>單位</th><th>數值</th></tr>
    <tr><td>股</td><td>-1,234.50</td></tr></table><p><a href="/another-story/">延伸連結仍屬原文</a></p>
    <div class="swiper article-bottom"><div></div></div></div><aside>不屬於正文</aside></div></article>
    </div></div></div></div><footer><div class="copyright"><span>© Stockfeel. All rights reserved 股感服務之軟體開發、營運及作業環境通過 ISO/IEC 27001:2022 驗證</span></div></footer>
    </body></html>'''


def extract(settings, html, page_url=URL):
    source = PolicyRegistry(settings, base='.').sources['stockfeel']
    return extract_article(html.encode(), source, page_url=page_url)


def test_native_original_preserves_whole_prose_units_and_declared_credit(settings, html):
    record = extract(settings, html)
    assert record['title'] == TITLE
    assert record['authors_json'] == json.dumps([AUTHOR], ensure_ascii=False)
    assert '原始合成段落。' * 25 in record['body']
    assert '-1,234.50' in record['body'] and '股' in record['body']
    assert '延伸連結仍屬原文' in record['body'] and '不屬於正文' not in record['body']
    assert record['published_at_raw'] == '2026-10-15T07:07:51+00:00'
    assert record['modified_at_raw'] == '2026-10-15T07:07:55+00:00'
    assert '2026Q2' in record['title'] and '2026-10' in record['published_at_raw']
    detail = json.loads(record['author_attribution_detail_json'])
    assert not detail['rendered_byline_visibility_verified'] and not detail['human_reporter_identity_verified']
    assert all(row['initial_inline_visibility_hidden'] for row in detail['source_header_declarations'])
    assert not record['historical_point_in_time']
    assert record['attribution_status'] == 'source_declared_header_credit_not_render_verified'
    notices = json.loads(record['verified_article_notices_json'])
    assert notices[0]['text'] == '最近更新時間： 15 October, 2026'
    assert not notices[0]['timezone_declared'] and notices[0]['not_a_publication_or_PIT_observation']


@pytest.mark.parametrize('mutation', [
    {'domains': ['stockfeel.com.tw']}, {'id': 'other'}, {'acquisition_mode': 'finite'},
    {'article_selectors': ['body']}, {'article_path_pattern': '.*'}, {'title_selector': 'h2'},
    {'exclude_selectors': ['p']}, {'author_selectors': ['footer']},
    {'permitted_author_names': [AUTHOR]}, {'allow_short_configured_body': True},
    {'require_author_attribution': False}, {'require_configured_container': False},
])
def test_unreviewed_source_contract_cannot_select_native_adapter(settings, mutation):
    settings['providers'][0].update(mutation)
    with pytest.raises(ValueError, match='unreviewed embedded article parser|known-URL-only scope.*'):
        PolicyRegistry(settings, base='.')


@pytest.mark.parametrize('old,new,code', [
    ('<h1>', '<h1 hidden>', 'article_identity_not_verified'),
    (f'content="{TITLE} - StockFeel 股感"', 'content="其他標題"', 'article_identity_not_verified'),
    ('data-aid="427"', 'data-aid="999"', 'article_rights_or_scope_excluded'),
    (PROFILE, 'https://other.example/author/news', 'article_rights_or_scope_excluded'),
    ('<span>文章來源</span>', '<span>轉載</span>', 'article_rights_or_scope_excluded'),
    ('name="author"', 'name="not-author"', 'article_attribution_not_verified'),
    ('visibility: hidden;', 'display: none;', 'article_attribution_not_verified'),
    ('2026-10-15T07:07:51+00:00', '2026-10-15T07:07:51', 'invalid_bulk_publication_clock'),
    ('2026-10-15T07:07:55+00:00', '2026-10-14T07:07:55+00:00', 'invalid_bulk_publication_clock'),
    ('15 October, 2026', '31 February, 2026', 'invalid_bulk_publication_clock'),
    ('class="sin-content-original"', 'class="sin-content-original" hidden', 'article_identity_not_verified'),
    ('</table>', '</table><script>not executed</script>', 'article_identity_not_verified'),
    ('</table>', '</table><iframe src="/not-requested"></iframe>', 'paywall_or_login'),
    ('</table>', '</table><p>本文轉載自其他來源</p>', 'article_rights_or_scope_excluded'),
    ('© Stockfeel. All rights reserved', '其他著作權', 'article_rights_not_verified'),
])
def test_unknown_identity_access_scope_and_clocks_fail_closed(settings, html, old, new, code):
    with pytest.raises(NewsError) as caught:
        extract(settings, html.replace(old, new))
    assert caught.value.code == code


def test_both_recorded_and_rendered_credit_modes_are_declared_not_assumed(settings, html):
    record = extract(settings, html.replace(' style="visibility: hidden;"', ''))
    detail = json.loads(record['author_attribution_detail_json'])
    assert not any(row['initial_inline_visibility_hidden'] for row in detail['source_header_declarations'])
    assert not detail['rendered_byline_visibility_verified']


def test_native_revision_layout_whitespace_preserves_raw_calendar_declaration(settings, html):
    raw = '最近更新時間：\n                        15 October, 2026'
    record = extract(settings, html.replace('最近更新時間： 15 October, 2026', raw))
    notice = json.loads(record['verified_article_notices_json'])[0]
    assert notice['text'] == raw
    assert not notice['timezone_declared']
    assert record['published_at_raw'] == '2026-10-15T07:07:51+00:00'


@pytest.mark.parametrize('url', [
    'http://www.stockfeel.com.tw/a/', 'https://stockfeel.com.tw/a/',
    STOCKFEEL_ORIGIN + '/a/?x=1', STOCKFEEL_ORIGIN + '/a/?', STOCKFEEL_ORIGIN + '/a/#',
    STOCKFEEL_ORIGIN + '/a%2fb/', STOCKFEEL_ORIGIN + '/a%5cb/', STOCKFEEL_ORIGIN + '/%252F/',
    STOCKFEEL_ORIGIN + '/%GG/', STOCKFEEL_ORIGIN + '/%FF/', STOCKFEEL_ORIGIN + '/a/b/',
    STOCKFEEL_ORIGIN + '/a\nb/', STOCKFEEL_ORIGIN + '/%00/', STOCKFEEL_ORIGIN + '/./',
])
def test_url_key_is_not_a_host_query_or_encoding_alias_resolver(url):
    assert stockfeel_url_key(url) is None


def test_unicode_and_percent_spellings_only_compare_same_native_path():
    assert stockfeel_url_key(URL) == stockfeel_url_key(CANONICAL) == '/測試-2026q2/'
    assert stockfeel_url_key(URL.lower()) == stockfeel_url_key(CANONICAL)
