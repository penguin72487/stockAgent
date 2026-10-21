"""Synthetic four-URL MCU contract; no publisher prose or live requests."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_mcu import MCU_BODY_SELECTOR, MCU_IDS, MCU_PATH_PATTERN, MCU_EDITOR, MCU_PROFILE
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from stockagent.data.news_source_catalog import load_source_review_ledger


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'mcu_week', 'name': '銘傳一週', 'enabled': True,
        'domains': ['www.week.mcu.edu.tw'], 'acquisition_mode': 'finite',
        'interval_seconds': 2, 'parallelism': 1, 'html_parser': 'lxml',
        'embedded_article_layout': 'mcu_visible_news_v1',
        'article_path_pattern': MCU_PATH_PATTERN, 'article_selectors': [MCU_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1.cm-entry-title',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'evidence_url': 'https://www.week.mcu.edu.tw/about',
            'scope': 'Four locally known own-publication private texts only; no media or export.',
            'reviewed_on': '2026-10-20', 'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def page(identifier='43881'):
    url = f'https://www.week.mcu.edu.tw/{identifier}/'
    title = '合成校友產業報導'
    first = '【本刊訊】' if identifier != '45536' else ''
    prose = ''.join(f'<p>{first if i == 0 else ""}第{i}段' + '合成測試原始值與<strong>細節</strong>。' * 9 + '</p>'
                    for i in range(4))
    return f'''<html><head><title>{title} – Ming Chuan Weekly 銘傳一週</title>
      <link rel="canonical" href="{url}"><meta property="og:url" content="{url}">
      <meta property="og:title" content="{title}">
      <meta property="article:published_time" content="2022-11-25T09:17:49+08:00">
      <meta property="article:modified_time" content="2022-11-26T09:17:49+08:00"></head>
      <body><nav>導航不是正文</nav><article id="post-{identifier}">
      <div class="cm-post-content cm-single-post-content"><div class="cm-featured-image">主圖不是正文</div>
      <header class="cm-entry-header"><h1 class="cm-entry-title">{title}</h1></header>
      <div class="cm-below-entry-meta"><span class="cm-post-date"><a rel="bookmark" href="{url}">
        <time class="entry-date" datetime="2022-11-25T09:17:49+08:00">2022-11-25</time></a></span>
        <span class="cm-author"><a href="{MCU_PROFILE}">{MCU_EDITOR}</a></span></div>
      <div class="cm-entry-summary">{prose}<figure class="wp-caption"><img src="/not-downloaded.png">
        <figcaption>照片說明不是正文</figcaption></figure></div></div>
      <meta itemprop="dateModified" content="2022-11-26T09:17:49+08:00"></article>
      <aside><time datetime="2026-10-20T12:00:00+08:00">2026-10-20</time>別篇文章不是正文</aside>
      </body></html>'''


def parse(settings, html, identifier='43881', url=None):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['mcu_week']
    return extract_article(html.encode(), source,
        page_url=url or f'https://www.week.mcu.edu.tw/{identifier}/')


@pytest.mark.parametrize('identifier', MCU_IDS)
def test_native_prose_identity_clocks_and_editor_role(settings, identifier):
    result = parse(settings, page(identifier), identifier)
    assert result['published_at_raw'] == '2022-11-25T09:17:49+08:00'
    assert result['modified_at_raw'] == '2022-11-26T09:17:49+08:00'
    assert 'historical_release_observation_not_verified' in result['published_time_basis']
    assert not result['historical_point_in_time']
    assert not result['per_article_license_verified']
    assert json.loads(result['authors_json']) == ([] if identifier == '45536' else ['本刊訊']) + [MCU_EDITOR]
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['editor_is_not_inferred_reporter'] and detail['named_reporter'] is None
    for i in range(4):
        assert f'第{i}段' in result['body']
    assert result['body'].count('細節') == 36
    for excluded in ('主圖', '照片說明', '導航', '別篇文章', '2026-10-20', MCU_EDITOR):
        assert excluded not in result['body']


@pytest.mark.parametrize('url', [
    'https://www.week.mcu.edu.tw/43882/', 'https://www.week.mcu.edu.tw/43881/?copy=1',
    'https://www.week.mcu.edu.tw/43881/#other', 'https://week.mcu.edu.tw/43881/',
    'http://www.week.mcu.edu.tw/43881/',
])
def test_no_guessed_new_id_or_alias(settings, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, page(), url=url)


@pytest.mark.parametrize('old,new,code', [
    ('id="post-43881"', 'id="post-43882"', 'article_identity_not_verified'),
    ('property="og:url"', 'property="unverified:url"', 'article_identity_not_verified'),
    ('property="og:title"', 'property="unverified:title"', 'article_identity_not_verified'),
    ('<link rel="canonical"', '<link rel="other"', 'article_identity_not_verified'),
    (' – Ming Chuan Weekly 銘傳一週', ' – 別的網站', 'article_identity_not_verified'),
    ('class="cm-entry-title"', 'class="cm-entry-title" hidden', 'article_identity_not_verified'),
    ('class="cm-post-content cm-single-post-content"', 'class="cm-post-content cm-single-post-content" style="display:none"', 'article_identity_not_verified'),
    (MCU_PROFILE, 'https://elsewhere.test/author/', 'article_attribution_not_verified'),
    (MCU_EDITOR, '記者｜未知作者', 'article_attribution_not_verified'),
    ('class="cm-author"', 'class="cm-author" aria-hidden="true"', 'article_identity_not_verified'),
    ('2022-11-25T09:17:49+08:00', '2022-02-30T09:17:49+08:00', 'invalid_bulk_publication_clock'),
    ('2022-11-26T09:17:49+08:00', '2022-11-24T09:17:49+08:00', 'invalid_bulk_publication_clock'),
    ('2022-11-25T09:17:49+08:00', '2022-11-25T09:17:49', 'invalid_bulk_publication_clock'),
    ('>2022-11-25</time>', '>2022-11-24</time>', 'invalid_bulk_publication_clock'),
    ('【本刊訊】', '【中央社】', 'article_rights_or_scope_excluded'),
    ('<strong>細節</strong>', '<em>未核對的結構</em>', 'article_identity_not_verified'),
    ('第2段', '<span hidden>第2段</span>', 'article_identity_not_verified'),
    ('class="wp-caption"', 'class="unknown-media"', 'article_identity_not_verified'),
    ('<figcaption>', '<p>不能丟棄的正文</p><figcaption>', 'article_identity_not_verified'),
])
def test_source_drift_and_third_party_fail_closed(settings, old, new, code):
    with pytest.raises(NewsError, match=code):
        parse(settings, page().replace(old, new))


@pytest.mark.parametrize('declaration', [
    '<link rel="canonical" href="https://other.test/">',
    '<meta property="og:url" content="https://www.week.mcu.edu.tw/43881/">',
])
def test_duplicate_identity_declarations_not_fallback(settings, declaration):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, page().replace('</head>', declaration + '</head>'))


def test_editor_only_attribution_never_hides_a_syndication_prefix(settings):
    html = page('45536').replace('<p>第0段', '<p>轉載自其他媒體第0段')
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        parse(settings, html, '45536')


@pytest.mark.parametrize('field,value', [
    ('acquisition_mode', 'continuous'), ('domains', ['mcu.edu.tw']),
    ('article_path_pattern', '^/[0-9]+/$'), ('article_selectors', ['article']),
    ('require_author_attribution', False), ('exclude_selectors', ['p']),
    ('title_selector', 'h1'), ('feeds', ['https://www.week.mcu.edu.tw/feed/']),
    ('sitemaps', ['https://www.week.mcu.edu.tw/sitemap.xml']),
    ('index_pages', ['https://www.week.mcu.edu.tw/']),
])
def test_reviewed_parser_cannot_expand_source_scope(settings, field, value):
    settings['providers'][0][field] = value
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry(settings, base=Path.cwd())


def test_installed_scope_reuses_finite_private_owner_without_discovery_or_export():
    config, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['mcu_week']
    assert source.allowed and source.acquisition_mode == 'finite'
    assert source.domains == ('www.week.mcu.edu.tw',)
    assert source.selectors == (MCU_BODY_SELECTOR,)
    assert source.authorization['kind'] == 'personal_research'
    assert source.authorization.get('license') is None
    assert not any((source.feeds, source.sitemaps, source.wordpress_apis, source.fulltext_feeds,
                    source.historical_fulltext_feeds, source.index_pages, source.incremental_index_pages))
    assert not config['publish'] and not config['remote_training_export']


def test_installed_review_alias_keeps_the_old_unfinished_observation():
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    current = reviews['mcu_week']
    assert current == reviews['unregistered:www.week.mcu.edu.tw']
    assert current['capture_decision'] == 'reviewed_existing_scope'
    assert current['prior_reviews'][0]['capture_decision'] == 'review_incomplete'
    assert current['prior_reviews'][0]['reviewed_at_utc'] == '2026-10-12T02:57:00+00:00'
