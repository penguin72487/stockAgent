"""Native two-original parsing must not relax CMNews or source access rules."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.news_fulltext_cmoneylearning import (
    CMONEY_LEARNING_BODY_SELECTOR, CMONEY_LEARNING_ORIGIN,
    CMONEY_LEARNING_PATH_PATTERN, CMONEY_LEARNING_PATHS,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_news_fulltext import config


@pytest.fixture
def settings(config):
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'cmoneylearning', 'name': 'Synthetic public originals',
        'domains': ['cmoneylearning.com.tw'], 'enabled': True, 'acquisition_mode': 'finite',
        'interval_seconds': 10, 'parallelism': 1,
        'embedded_article_layout': 'cmoneylearning_visible_article_v1',
        'html_parser': 'lxml', 'article_path_pattern': CMONEY_LEARNING_PATH_PATTERN,
        'article_selectors': [CMONEY_LEARNING_BODY_SELECTOR], 'exclude_selectors': [],
        'title_selector': 'h1.articleContent__title',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research',
            'evidence_url': 'https://www.cmoney.tw/member/tos.aspx',
            'reviewed_on': '2026-10-20', 'scope': 'Two private originals only; no aliases, discovery or export.',
            'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def test_two_original_candidate_keeps_the_closed_installed_source_contract(settings, monkeypatch):
    """Exercise admission before deployment, even while the recipe is disabled."""
    import test_news_fulltext as shared
    from downloader.news_fulltext import load_config

    installed, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(installed)
    candidate['providers'] = [p for p in candidate['providers'] if p['id'] != 'cmoneylearning'] + settings['providers']
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, registry, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


def page(path=CMONEY_LEARNING_PATHS[0]):
    url = CMONEY_LEARNING_ORIGIN + path
    origin = CMONEY_LEARNING_ORIGIN
    person, organization = origin + '/author/rowan_lin/#person', origin + '/#organization'
    declarations = [
        {'@type': 'BlogPosting', '@id': url + '#article',
         'mainEntityOfPage': {'@type': 'WebPage', '@id': url}, 'headline': '合成財經報導',
         'datePublished': '2026-10-19T17:11:20+00:00', 'dateModified': '2026-10-19T17:11:44+00:00',
         'isAccessibleForFree': True, 'author': {'@id': person},
         'publisher': {'@id': organization}, 'copyrightHolder': {'@id': organization}},
        {'@type': 'Person', '@id': person, 'name': '理財內容編輯林序安',
         'url': origin + '/author/rowan_lin/', 'worksFor': {'@id': organization}},
        {'@type': 'Organization', '@id': organization, 'name': '實習大人', 'url': origin + '/'},
    ]
    scripts = ''.join('<script type="application/ld+json">' + json.dumps(v, ensure_ascii=False) + '</script>'
                      for v in declarations)
    prose = ''.join(f'<p>第{i}段' + '合成原始數值<strong>不取對數</strong>。' * 8 + '</p>' for i in range(4))
    return f'''<html><head><title>合成財經報導 | 實習大人</title>
      <link rel="canonical" href="{url}"><meta property="og:type" content="article">
      <meta property="og:title" content="合成財經報導 | 實習大人">{scripts}</head>
      <body><nav>不是正文的導航</nav><main class="site-main"><article class="articleContent">
      <div class="articleContent__body"><div class="articleContent__content--main">
      <div class="articleContent__content--titleRow"><h1 class="articleContent__title">合成財經報導</h1></div>
      <div class="articleContent__meta"><div class="articleContent__metaLine">
      <span class="articleContent__author">理財內容編輯林序安</span>
      <time class="articleContent__date" datetime="2026-10-20T01:11:20+08:00">2026年10月20日</time>
      </div></div><figure>不是正文的主圖</figure>
      <div class="articleContent__textContent"><div>{prose}
      <table><thead><tr><th>欄名</th></tr></thead><tbody><tr><td>-12.50</td></tr><tr><td>0</td></tr></tbody></table>
      <article id="readmo-ai-article-shortcode-1" class="readmo-ai-article-shortcode"></article>
      <script>throw new Error('不得執行的輪詢程式');</script></div></div>
      <footer>不是正文的頁尾</footer></div></div></article></main>
      <aside><h1>另一篇新聞</h1><span class="articleContent__author">別篇作者</span></aside></body></html>'''


def parse(settings, html, path=CMONEY_LEARNING_PATHS[0], *, url=None):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['cmoneylearning']
    return extract_article(html.encode(), source, page_url=url or CMONEY_LEARNING_ORIGIN + path)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('path', CMONEY_LEARNING_PATHS)
def test_native_body_table_and_equivalent_declared_clocks(settings, parser, path):
    settings['providers'][0]['html_parser'] = parser
    result = parse(settings, page(path), path)
    assert result['published_at_raw'] == '2026-10-20T01:11:20+08:00'
    assert result['modified_at_raw'] == '2026-10-19T17:11:44+00:00'
    assert json.loads(result['authors_json']) == ['理財內容編輯林序安']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert json.loads(result['author_attribution_detail_json'])['declared_editor_role_not_inferred_reporter']
    for i in range(4):
        assert f'第{i}段' in result['body']
    for value in ['欄名', '-12.50', '0']:
        assert value in result['body']
    assert result['body'].count('不取對數') == 32
    for value in ['合成財經報導', '導航', '主圖', '頁尾', '另一篇', '別篇作者', '輪詢程式', '理財內容編輯']:
        assert value not in result['body']
    assert 'not_CMNews_alias_equivalence' in result['article_identity_basis']


@pytest.mark.parametrize('old,new,code', [
    ('rel="canonical"', 'rel="alternate"', 'article_identity_not_verified'),
    ('property="og:type" content="article"', 'property="og:type" content="website"', 'article_identity_not_verified'),
    ('class="articleContent__title"', 'class="articleContent__title" hidden', 'article_identity_not_verified'),
    ('class="articleContent__body"', 'class="articleContent__body" aria-hidden="true"', 'article_identity_not_verified'),
    ('第2段', '<span class="hidden">第2段</span>', 'article_identity_not_verified'),
    ('理財內容編輯林序安</span>', '其他作者</span>', 'article_attribution_not_verified'),
    ('"name": "理財內容編輯林序安"', '"name": "另一位作者"', 'article_rights_or_scope_excluded'),
    ('"copyrightHolder":', '"unverifiedHolder":', 'article_rights_or_scope_excluded'),
    ('"isAccessibleForFree": true', '"isAccessibleForFree": false', 'paywall_or_login'),
    ('2026年10月20日', '2026年10月19日', 'invalid_bulk_publication_clock'),
    ('2026-10-20T01:11:20+08:00', '2026-10-20T01:11:20', 'invalid_bulk_publication_clock'),
    ('2026-10-19T17:11:20+00:00', '2026-10-19T17:11:21+00:00', 'invalid_bulk_publication_clock'),
    ('2026-10-19T17:11:44+00:00', '2026-10-19T17:11:19+00:00', 'invalid_bulk_publication_clock'),
    ('2026-10-19T17:11:44+00:00', '2026-02-30T17:11:44+00:00', 'invalid_bulk_publication_clock'),
    ('class="readmo-ai-article-shortcode"></article>', 'class="readmo-ai-article-shortcode">生成內容</article>', 'article_identity_not_verified'),
    ('<table>', '<iframe src="/unreviewed"></iframe><table>', 'paywall_or_login'),
])
def test_drift_and_access_never_fall_back(settings, old, new, code):
    with pytest.raises(NewsError, match=code):
        parse(settings, page().replace(old, new))


@pytest.mark.parametrize('selector', ['canonical', 'scope', 'date', 'schema'])
def test_ambiguous_current_article_is_not_chosen_by_first_match(settings, selector):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(page(), 'lxml')
    node = soup.select_one({'canonical': 'link[rel=canonical]', 'scope': 'article.articleContent',
                           'date': 'time', 'schema': 'script[type="application/ld+json"]'}[selector])
    node.insert_after(deepcopy(node))
    with pytest.raises(NewsError, match='article_identity_not_verified|invalid_bulk_publication_clock'):
        parse(settings, str(soup))


@pytest.mark.parametrize('url', [
    'https://cmoneylearning.com.tw/not-reviewed/', 'http://cmoneylearning.com.tw/',
    'https://cmnews.com.tw/article/other',
    CMONEY_LEARNING_ORIGIN + CMONEY_LEARNING_PATHS[0] + '?other=1',
    CMONEY_LEARNING_ORIGIN + CMONEY_LEARNING_PATHS[0] + '#other',
    CMONEY_LEARNING_ORIGIN + CMONEY_LEARNING_PATHS[0].upper(),
])
def test_no_guessed_alias_or_percent_case_expansion(settings, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, page(), url=url)


@pytest.mark.parametrize('field,value', [
    ('acquisition_mode', 'continuous'), ('domains', ['cmnews.com.tw']),
    ('article_path_pattern', '^/.*$'), ('article_selectors', ['article']),
    ('exclude_selectors', ['p']), ('require_author_attribution', False),
    ('require_configured_container', False), ('title_selector', 'h1'),
    ('feeds', [CMONEY_LEARNING_ORIGIN + '/feed/']),
    ('sitemaps', [CMONEY_LEARNING_ORIGIN + '/sitemap.xml']),
    ('index_pages', [CMONEY_LEARNING_ORIGIN + '/']),
])
def test_two_private_originals_do_not_grant_sitewide_capture(settings, field, value):
    settings['providers'][0][field] = value
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry(settings, base=Path.cwd())
