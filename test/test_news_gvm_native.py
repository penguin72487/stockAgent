"""Finite GVM native candidate; synthetic prose, never downloaded fixtures."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_gvm import (
    GVM_AUTHOR_SELECTOR, GVM_BODY_SELECTOR, GVM_EXCLUSIONS, GVM_LAYOUT,
    GVM_PATH_PATTERN, GVM_REVIEWED_AUTHORS,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


URL = 'https://www.gvm.com.tw/article/12345'
ORIGIN = 'https://www.gvm.com.tw'
PUBLISHED = '2020-12-10T00:00:00+08:00'
MODIFIED = '2024-11-07T12:01:33+08:00'
TEXT = '這是自行撰寫的測試文章，不是來源新聞。保留股數1000、價格12.34與負值-5。' * 5


def entry():
    return {'id': 'gvm', 'domains': ['www.gvm.com.tw'], 'enabled': True,
        'acquisition_mode': 'finite', 'finite_article_urls': [URL], 'html_parser': 'lxml', 'interval_seconds': 10,
        'parallelism': 1, 'embedded_article_layout': GVM_LAYOUT,
        'article_selectors': [GVM_BODY_SELECTOR], 'article_path_pattern': GVM_PATH_PATTERN,
        'title_selector': 'h1', 'require_configured_container': True,
        'require_author_attribution': True, 'author_selectors': [GVM_AUTHOR_SELECTOR],
        'permitted_author_names': list(GVM_REVIEWED_AUTHORS), 'exclude_selectors': list(GVM_EXCLUSIONS),
        'authorization': {'kind': 'personal_research', 'evidence_url': ORIGIN + '/copyright',
            'reviewed_on': '2026-10-22', 'scope': 'Finite private public native diagnostic only; not a license.',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'original_author_scope_evidence_urls': [URL]}}


def policy(value=None):
    return PolicyRegistry({'contract_version': 1, 'publish': False,
        'remote_training_export': False, 'providers': [value or entry()]}, base=Path('.')).sources['gvm']


def page(*, advertorial=False):
    name = GVM_REVIEWED_AUTHORS[int(advertorial)]
    kind, route, identifier = ('廣編', 'blog', '599') if advertorial else ('網路文章', 'author', '6813')
    declared = {'@type': 'NewsArticle', 'mainEntityOfPage': {'@type': 'WebPage', '@id': URL},
        'headline': '合成測試標題', 'publisher': {'@id': ORIGIN + '/#publisher'},
        'isAccessibleForFree': True, 'author': [{'@type': 'Person', 'name': name,
            'url': f'{ORIGIN}/{route}/{identifier}'}], 'datePublished': PUBLISHED, 'dateModified': PUBLISHED}
    return BeautifulSoup(f'''<html><head><title>合成測試標題 | 遠見雜誌</title>
      <link rel="canonical" href="{URL}"><meta property="og:url" content="{URL}">
      <meta property="og:title" content="合成測試標題 | 遠見雜誌">
      <meta property="article:published_time" content="{PUBLISHED}">
      <meta property="article:modified_time" content="{MODIFIED}">
      <script type="application/ld+json">{json.dumps({'@graph': [declared]}, ensure_ascii=False)}</script>
      </head><body><nav>NOT_ARTICLE_NAVIGATION</nav><div class="article-left"><div class="article-block">
      <input id="article_id" type="hidden" value="12345"><div class="article-block">
      <div class="article-head" aid="12345" id="article_12345" title="合成測試標題"
        author="{name}" author_id="{identifier}" article_type="{kind}" start_time="2026/10/22 10:56:39"
        status="不限制" data-is-accessible="1" pub_date="2020-12-10">
        <h1 class="article-head_h1">合成測試標題</h1><div class="article-head_box">
        <div class="article-head_inner"><a class="article-head_author" href="{ORIGIN}/{route}/{identifier}">{name}</a></div>
        <p class="article-head_grey">2020-12-10</p><p class="article-head_grey js-article-click">瀏覽數</p></div></div>
      <div class="article-content article-content_level-0"><div id="article_float_article"></div>
        <blockquote class="article-head_blockquote">合成導言不能當整篇正文。</blockquote><p>{TEXT}</p>
        <div id="div-gpt-ad-1738661051687-0-12345"><script> googletag.cmd.push(function () {{ googletag.display('div-gpt-ad-1738661051687-0-12345'); }}); </script></div>
        <section class="newsletter"><input type="text">NOT_ARTICLE_NEWSLETTER</section>
        <p>合成結尾。</p><p><img src="https://example.org/image"><figcaption>可見圖片說明。</figcaption></p>
        <input type="hidden" id="tracker[]" key="banners" value="13247">
        <iframe title="Firstory" src="https://open.firstory.me/embed/story/abc123"></iframe>
      </div><aside>NOT_ARTICLE_RECOMMENDATIONS</aside></div></div></div>
      <div id="login-popup">收藏功能登入，不是文章付費正文。</div></body></html>''', 'lxml')


def extract(soup, source=None, url=URL):
    return extract_article(str(soup).encode(), source or policy(), page_url=url)


def schema(soup, mutate):
    script = soup.select_one('script[type="application/ld+json"]')
    value = json.loads(script.get_text())
    mutate(value['@graph'][0])
    script.string = json.dumps(value, ensure_ascii=False)


@pytest.mark.parametrize('advertorial', [False, True])
def test_public_written_body_credit_roles_and_distinct_clocks(advertorial):
    result = extract(page(advertorial=advertorial))
    assert TEXT in result['body'] and '合成結尾。' in result['body'] and '可見圖片說明。' in result['body']
    assert 'NOT_ARTICLE_' not in result['body'] and 'googletag' not in result['body']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    detail, notices = (json.loads(result[key]) for key in ('author_attribution_detail_json', 'verified_article_notices_json'))
    assert detail['source_credit_role'] == ('publisher_declared_advertorial' if advertorial else 'publisher_declared_reporter')
    assert not detail['human_reporter_identity_verified'] and not detail['original_ownership_verified']
    assert not notices['modified_declarations_agree'] and notices['source_schema_modified_raw'] == PUBLISHED
    assert not notices['native_start_time_is_publication'] and not notices['media_or_scripts_requested_or_executed']


@pytest.mark.parametrize('selector,attribute,value,error', [
    ('.article-head', 'aid', '999', 'article_identity_not_verified'),
    ('input#article_id', 'value', '999', 'article_identity_not_verified'),
    ('.article-head', 'data-is-accessible', '0', 'paywall_or_login'),
    ('.article-head', 'status', '會員限定', 'paywall_or_login'),
    ('.article-content', 'class', ['article-content', 'article-content_level-1'], 'article_identity_not_verified'),
    ('.article-content', 'hidden', '', 'article_identity_not_verified'),
    ('.article-head_author', 'href', ORIGIN + '/author/999', 'article_rights_or_scope_excluded'),
    ('meta[property="article:modified_time"]', 'content', '2024-11-07', 'invalid_bulk_publication_clock'),
])
def test_native_identity_access_and_clock_fail_closed(selector, attribute, value, error):
    soup = page()
    soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError, match=error):
        extract(soup)


@pytest.mark.parametrize('mutate,error', [
    (lambda a: a.update(isAccessibleForFree=False), 'paywall_or_login'),
    (lambda a: a.update(mainEntityOfPage={'@type': 'WebPage', '@id': ORIGIN + '/article/999'}), 'article_identity_not_verified'),
    (lambda a: a.update(headline='別篇標題'), 'article_identity_not_verified'),
    (lambda a: a.update(datePublished='2020-12-10'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(author=[{'@type': 'Person', 'name': '中央社', 'url': ORIGIN + '/author/6813'}]), 'article_rights_or_scope_excluded'),
])
def test_schema_does_not_override_native_article(mutate, error):
    soup = page()
    schema(soup, mutate)
    with pytest.raises(NewsError, match=error):
        extract(soup)


def test_jsonld_hidden_body_and_lead_are_not_a_full_body_fallback():
    soup = page()
    schema(soup, lambda a: a.update(articleBody=TEXT * 20))
    for node in soup.select('.article-content > p'):
        node.decompose()
    with pytest.raises(NewsError, match='body_not_found'):
        extract(soup)


def test_unknown_script_and_hidden_written_prose_are_not_discarded_to_pass():
    for markup in ('<script>window.unknown = "full body";</script>', '<p hidden>secret</p>'):
        soup = page()
        fragment = BeautifulSoup('<div>' + markup + '</div>', 'lxml')
        soup.select_one('.article-content').append(fragment.div.contents[0])
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract(soup)


@pytest.mark.parametrize('field,value', [
    ('domains', ['gvm.com.tw']), ('acquisition_mode', 'continuous'),
    ('exclude_selectors', []), ('require_author_attribution', False),
    ('article_selectors', ['body']), ('feeds', [ORIGIN + '/feed']),
    ('permitted_author_names', ['unreviewed']),
    ('finite_article_urls', []),
])
def test_finite_candidate_cannot_expand_into_site_wide_or_unreviewed_capture(field, value):
    candidate = deepcopy(entry())
    candidate[field] = value
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        policy(candidate)


@pytest.mark.parametrize('url', [ORIGIN + '/article/999', URL + '?member=1', 'https://esg.gvm.com.tw/article/12345'])
def test_literal_url_identity_is_not_guessed_or_cross_subsite(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(), url=url)
