"""The small native source keeps its own date/byline/footnotes, never sidebars."""
from copy import deepcopy
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_independent import EVENTS_REVIEWED_URL, EVENTS_BODY_SELECTOR
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


TITLE = '農地光電目標8GW，-12.5%、0.75及2025年'
PROSE = '原始報導保留8GW、2.4GW、4GW、0.6GW、-12.5%與0.75，不取頁尾日期。' * 5
NOTES = '其它包括0.6GW、埤塘0.4GW及低地力農地0.6GW，不能遺漏註解。'


def recipe():
    return {'id': 'eventsinfocus', 'domains': ['eventsinfocus.org'], 'enabled': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-15',
            'evidence_url': 'https://eventsinfocus.org/pages/55',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'One reviewed own article only; no third party, training or redistribution'},
        'acquisition_mode': 'finite', 'interval_seconds': 10, 'parallelism': 1,
        'embedded_article_layout': 'eventsinfocus_visible_news_v1',
        'html_parser': 'lxml', 'article_path_pattern': '^/news/7145971$',
        'article_selectors': [EVENTS_BODY_SELECTOR], 'exclude_selectors': [],
        'title_selector': 'h1', 'require_configured_container': True, 'require_author_attribution': True}


def policy(parser='lxml'):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    return PolicyRegistry({**cfg, 'providers': [{**recipe(), 'html_parser': parser}]}, base=Path.cwd()).sources['eventsinfocus']


def page(parser='lxml'):
    return BeautifulSoup(f'''<html><head><meta charset="utf-8">
        <link rel="canonical" href="{EVENTS_REVIEWED_URL}">
        <meta property="og:url" content="{EVENTS_REVIEWED_URL}">
        <meta property="og:title" content="{TITLE}">
        <meta property="og:description" content="只有摘要，不能當完整正文"></head><body>
        <main id="content" role="main"><div id="block-events2020-page-title"><h1 class="title">
        <span class="field--name-title">{TITLE}</span></h1></div>
        <div id="block-events2020-content"><div class="content"><article class="node--type-news node--view-mode-full"
        data-history-node-id="7145971" about="/news/7145971"><div class="node__content">
        <div class="field--name-field-time"><time class="datetime" datetime="2020-10-07T12:00:00Z">2020-10-07</time></div>
        <div class="field--name-body"><p>焦點事件記者王子豪報導</p><p>{PROSE}</p></div>
        <div class="field--name-field-footnote"><div class="field__label">註解</div><div>{NOTES}</div></div>
        </div></article></div></div></main>
        <aside><article class="node--type-page"><div class="field--name-body">募款電話捐款人名單</div></article>
        <time datetime="2026-10-15T00:00:00Z">最後修改2026-10-15</time></aside></body></html>''', parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_exact_native_body_notes_reporter_and_distinct_clock_precision(parser):
    soup = page(parser)
    original = str(soup)
    result = extract_article(original.encode(), policy(parser), page_url=EVENTS_REVIEWED_URL)
    assert PROSE in result['body'] and NOTES in result['body']
    assert all(text not in result['body'] for text in ['募款電話', '只有摘要', '焦點事件記者王子豪報導', '2026-10-15'])
    assert result['authors_json'] == '["王子豪"]' and result['published_at_raw'] == '2020-10-07'
    assert result['modified_at_raw'] is None and result['historical_point_in_time'] is False
    assert 'no_exact_historical_release_time_inferred' in result['published_time_basis']
    notice = json.loads(result['verified_article_notices_json'])[0]
    assert notice['text'] == '2020-10-07T12:00:00Z' and not notice['exact_historical_release_time_verified']
    assert 'separate_footnotes' in result['body_completeness'] and str(soup) == original
    assert result['declared_license_urls_json'] == '[]' and not result['per_article_license_verified']


@pytest.mark.parametrize('selector,attribute,value', [
    ('link[rel=canonical]', 'href', 'https://foreign.test/news/7145971'),
    ('meta[property="og:url"]', 'content', EVENTS_REVIEWED_URL+'?view=1'),
    ('meta[property="og:title"]', 'content', '另一篇文章'),
    ('main article', 'data-history-node-id', '7145972'),
    ('main article', 'about', '/news/7145972'),
    ('main', 'hidden', ''), ('main article', 'aria-hidden', 'true'),
    ('.field--name-body', 'style', 'display:none'),
    ('.field--name-field-footnote', 'style', 'visibility:hidden'),
    ('.field--name-body p', 'hidden', ''),
])
def test_foreign_ambiguous_or_hidden_identity_and_prose_rejected(selector, attribute, value):
    soup = page()
    soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), policy(), page_url=EVENTS_REVIEWED_URL)


@pytest.mark.parametrize('selector', [
    'link[rel=canonical]', 'meta[property="og:url"]', 'meta[property="og:title"]',
    '#block-events2020-content', '.field--name-field-time', '.field--name-field-footnote',
])
@pytest.mark.parametrize('operation', ['missing', 'duplicate'])
def test_required_native_witnesses_cannot_be_missing_or_competing(selector, operation):
    soup = page()
    node = soup.select_one(selector)
    if operation == 'missing':
        node.decompose()
    else:
        node.insert_after(deepcopy(node))
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), policy(), page_url=EVENTS_REVIEWED_URL)


@pytest.mark.parametrize('credit', ['中央社記者王子豪', '焦點事件編輯王子豪', '焦點事件記者別人報導', '', '焦點事件記者王子豪報導（轉載）'])
def test_unknown_or_third_party_byline_not_inferred_from_name_or_sidebar(credit):
    soup = page()
    soup.select_one('.field--name-body p').string = credit
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(str(soup).encode(), policy(), page_url=EVENTS_REVIEWED_URL)


@pytest.mark.parametrize('day,declared', [
    ('2020-02-30', '2020-02-30T12:00:00Z'), ('2020-10-07', '2020-10-08T12:00:00Z'),
    ('2020/10/07', '2020-10-07T12:00:00Z'), ('2020-10-07', '2020-10-07'),
    ('2020-10-07', '2020-10-07T12:00:00'), ('2020-10-07', '2020-10-07T25:00:00Z'),
])
def test_invalid_or_competing_date_not_taken_from_event_title_or_sidebar(day, declared):
    soup = page()
    soup.time.string, soup.time['datetime'] = day, declared
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract_article(str(soup).encode(), policy(), page_url=EVENTS_REVIEWED_URL)


@pytest.mark.parametrize('suffix', ['?q=1', '#article', '/'])
def test_one_known_url_not_alias_generation(suffix):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(page()).encode(), policy(), page_url=EVENTS_REVIEWED_URL+suffix)


@pytest.mark.parametrize('changes', [
    {'acquisition_mode': 'continuous'}, {'article_path_pattern': '^/news/[0-9]+$'},
    {'exclude_selectors': ['aside']}, {'article_selectors': ['.field--name-body']},
    {'domains': ['eventsinfocus.org', 'www.eventsinfocus.org']}, {'require_author_attribution': False},
])
def test_exact_review_cannot_become_site_wide_scope(changes):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry({**cfg, 'providers': [{**recipe(), **changes}]}, base=Path.cwd())
