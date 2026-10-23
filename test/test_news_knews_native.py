"""Knews native structure uses synthetic prose; no copyrighted body fixture."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_knews import (
    KNEWS_AUTHOR_SELECTOR, KNEWS_BODY_SELECTOR, KNEWS_EXCLUSIONS, KNEWS_LAYOUT,
    KNEWS_PATH_PATTERN, KNEWS_REVIEWED_HEADER_AUTHORS, KNEWS_REVIEWED_URLS,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


PUBLISHED, MODIFIED = '2026-06-17T16:30:00+08:00', '2026-06-17T17:12:29+08:00'
TEXT = '這是自行撰寫的測試內容，保留原始股數1000、價格12.34、負數-5，不是來源新聞。' * 4
INTRO = '【記者合成作者／台北報導】這一段是完整正文的導言，不能遺失，也不能把它當作全部正文。'


def entry():
    return {'id': 'knews', 'domains': ['www.knews.com.tw'], 'enabled': True,
        'acquisition_mode': 'finite', 'finite_article_urls': list(KNEWS_REVIEWED_URLS),
        'html_parser': 'lxml', 'interval_seconds': 10, 'parallelism': 1,
        'embedded_article_layout': KNEWS_LAYOUT, 'article_selectors': [KNEWS_BODY_SELECTOR],
        'article_path_pattern': KNEWS_PATH_PATTERN, 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'author_selectors': [KNEWS_AUTHOR_SELECTOR], 'exclude_selectors': list(KNEWS_EXCLUSIONS),
        'permitted_author_names': list(KNEWS_REVIEWED_HEADER_AUTHORS),
        'authorization': {'kind': 'personal_research', 'evidence_url': 'https://www.knews.com.tw/',
            'reviewed_on': '2026-10-22', 'scope': 'Only two reviewed private personal public originals; no license.',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'original_author_scope_evidence_urls': list(KNEWS_REVIEWED_URLS)}}


def policy(value=None):
    return PolicyRegistry({'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'providers': [value or entry()]}, base=Path('.')).sources['knews']


def page(index=0):
    url, name = KNEWS_REVIEWED_URLS[index], KNEWS_REVIEWED_HEADER_AUTHORS[index]
    profile = 'https://www.knews.com.tw/reporter/' + ('renee', 'cliff')[index]
    declared = {'@type': 'NewsArticle', 'mainEntityOfPage': {'@type': 'WebPage', '@id': url},
        'headline': '合成測試標題', 'publisher': {'@type': 'Organization', 'name': '知新聞'},
        'author': [{'@type': 'Person', 'name': name, 'url': profile}],
        'datePublished': PUBLISHED, 'dateModified': MODIFIED, 'description': INTRO}
    return BeautifulSoup(f'''<html><head><link rel="canonical" href="{url}">
      <meta property="og:url" content="{url}"><meta property="og:title" content="合成測試標題｜知新聞">
      <meta name="author" content="{name}">
      <script type="application/ld+json">{json.dumps({'@graph': [declared]}, ensure_ascii=False)}</script>
      </head><body><nav>NOT_ARTICLE_NAV</nav><article id="post-detail">
      <div id="adm-wrapper" class="ad">廣告</div><style>.example {{ color: red; }}</style>
      <div class="knews-ticker-root">NOT_ARTICLE_TICKER</div><script>window.ticker = 1;</script>
      <h1>合成測試標題</h1><h2></h2><div class="article-meta-panel abc-style"><div class="abc-meta-info">
      <div class="abc-byline"><span class="label">記者</span><a href="{profile}">{name}</a></div>
      <div class="abc-timestamp"><span class="label">發布</span><time>2026/06/17 16:30</time></div></div></div>
      <figure><a href="https://example.org/image"><img src="https://example.org/image" alt="ALT_NOT_PROSE"></a>
      <figcaption>合成照片說明與攝影署名。</figcaption></figure>
      <div class="post-share sharetop">NOT_ARTICLE_SHARE</div><p class="post-intro">{INTRO}</p>
      <div class="article-body-wrapper"><div id="article-left-toc" class="article-left-toc">
      <div class="toc-title">本文大綱</div><ul class="toc-list"><li><a class="toc-link" href="#subhead-0">合成小標</a></li></ul></div>
      <div class="article-content-inner"><h3 id="subhead-0">合成小標</h3><p>{TEXT}</p>
      <div class="in-article-ad-wrapper"><div id="ad1-wrapper"><div id="knews_pc_article_inarticle01"></div></div></div>
      <p class="extend-reading"><span>🔗 延伸閱讀</span><a href="{KNEWS_REVIEWED_URLS[1]}">NOT_ARTICLE_EXTEND</a></p>
      <p class="extend-reading" id="norss"></p><p>合成結尾。</p>
      <div class="related-news-card" id="norss"><a href="{KNEWS_REVIEWED_URLS[1]}"><div><img src="https://example.org/related"></div>
      <div><div><span>編輯推薦</span></div><h5>NOT_ARTICLE_RELATED</h5></div></a></div>
      <p><figure><a href="https://event.knews.com.tw/2026election"><img src="https://example.org/banner"></a></figure></p>
      <p class="app-promo-wrapper"><a class="app-promo-link" href="https://event.knews.com.tw/app">⭐️ 即刻下載！無廣告《知新聞》App</a></p>
      </div></div></article><aside>NOT_ARTICLE_NEXT_STORY</aside></body></html>''', 'lxml')


def extract(soup, index=0):
    return extract_article(str(soup).encode(), policy(), page_url=KNEWS_REVIEWED_URLS[index])


def schema(soup, mutate):
    script = soup.select_one('script[type="application/ld+json"]')
    value = json.loads(script.get_text())
    mutate(value['@graph'][0])
    script.string = json.dumps(value, ensure_ascii=False)


@pytest.mark.parametrize('index', [0, 1])
def test_whole_editor_intro_caption_credit_roles_and_clock_precision(index):
    result = extract(page(index), index)
    assert TEXT in result['body'] and INTRO in result['body'] and '合成結尾。' in result['body']
    assert result['body'].count('合成小標') == 1
    assert result['body'].index('合成照片說明') < result['body'].index(INTRO) < result['body'].index(TEXT)
    assert 'NOT_ARTICLE_' not in result['body'] and 'ALT_NOT_PROSE' not in result['body']
    assert '即刻下載' not in result['body'] and '本文大綱' not in result['body']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert json.loads(result['authors_json']) == [KNEWS_REVIEWED_HEADER_AUTHORS[index]]
    roles = json.loads(result['author_attribution_detail_json'])
    assert roles['printed_reporter_name'] == '合成作者'
    assert not roles['different_names_are_verified_aliases'] and not roles['original_ownership_verified']
    clocks = json.loads(result['verified_article_notices_json'])
    assert clocks['visible_publication_precision'] == 'minute' and clocks['schema_clock_precision'] == 'second'
    assert not clocks['visible_timezone_declared'] and not clocks['historical_first_release_verified']


@pytest.mark.parametrize('selector,attr,value,error', [
    ('link[rel=canonical]', 'href', KNEWS_REVIEWED_URLS[1], 'article_identity_not_verified'),
    ('meta[property="og:title"]', 'content', '另一篇標題', 'article_identity_not_verified'),
    ('.post-intro', 'hidden', '', 'article_identity_not_verified'),
    ('.article-content-inner', 'style', 'display: none', 'article_identity_not_verified'),
    ('.abc-byline a', 'href', 'https://unreviewed.example/reporter/renee', 'article_attribution_not_verified'),
    ('h3', 'id', 'wrong', 'article_identity_not_verified'),
])
def test_identity_visibility_and_credit_do_not_fall_back(selector, attr, value, error):
    soup = page(); soup.select_one(selector)[attr] = value
    with pytest.raises(NewsError, match=error):
        extract(soup)


@pytest.mark.parametrize('mutate,error', [
    (lambda a: a.update(datePublished='2026-06-17'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(dateModified='2026-06-16T16:30:00+08:00'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(datePublished='2026-06-17T17:30:00+08:00'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(isAccessibleForFree=False), 'paywall_or_login'),
    (lambda a: a.update(isAccessibleForFree='false'), 'paywall_or_login'),
    (lambda a: a.update(publisher=None), 'article_identity_not_verified'),
    (lambda a: a.update(author=[{'name': '中央社'}]), 'article_attribution_not_verified'),
    (lambda a: a.update(mainEntityOfPage={'@id': KNEWS_REVIEWED_URLS[1]}), 'article_identity_not_verified'),
])
def test_schema_cannot_override_native_identity_clocks_or_access(mutate, error):
    soup = page(); schema(soup, mutate)
    with pytest.raises(NewsError, match=error):
        extract(soup)


def test_intro_and_schema_body_cannot_replace_missing_editor_prose():
    soup = page()
    schema(soup, lambda a: a.update(articleBody=TEXT * 10))
    for node in soup.select('.article-content-inner > p:not([class])'):
        node.decompose()
    with pytest.raises(NewsError, match='body_not_found'):
        extract(soup)


@pytest.mark.parametrize('selector,markup', [
    ('.article-content-inner', '<p hidden>不可丟棄以通過驗證的隱藏段落。</p>'),
    ('.article-content-inner', '<script>window.unknown = 1;</script>'),
    ('.in-article-ad-wrapper', '<p>不是空白廣告，不能悄悄刪除正文。</p>'),
    ('.related-news-card > a', '<p>不是推薦卡標題，不能遺失。</p>'),
    ('#post-detail', '<p>不能漏掉的新正文區。</p>'),
])
def test_unknown_editor_content_is_not_discarded_for_success(selector, markup):
    soup = page(); fragment = BeautifulSoup('<div>' + markup + '</div>', 'lxml')
    soup.select_one(selector).append(fragment.div.contents[0])
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('field,value', [
    ('domains', ['knews.com.tw']), ('acquisition_mode', 'continuous'),
    ('finite_article_urls', [KNEWS_REVIEWED_URLS[0]]), ('allow_reviewed_supplements', True),
    ('exclude_selectors', []), ('article_selectors', ['article']), ('require_author_attribution', False),
    ('permitted_author_names', ['中央社']), ('feeds', ['https://www.knews.com.tw/feed']),
])
def test_candidate_does_not_expand_into_unreviewed_source_or_history(field, value):
    candidate = deepcopy(entry()); candidate[field] = value
    with pytest.raises(ValueError, match='unreviewed embedded article parser|finite article scope'):
        policy(candidate)


@pytest.mark.parametrize('url', [KNEWS_REVIEWED_URLS[0] + '?member=1',
    KNEWS_REVIEWED_URLS[0].replace('www.', ''), 'https://www.knews.com.tw/search?q=股票'])
def test_literal_identity_never_guesses_other_hosts_or_routes(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(page()).encode(), policy(), page_url=url)


def test_exact_reviewed_identity_alone_keeps_the_previous_http_owner(monkeypatch):
    source = policy()
    monkeypatch.setattr('downloader.news_fulltext_knews.KNEWS_FINITE_HTTP_BINDING',
        (source.data_fingerprint, source.authorization_fingerprint, '1'*64, '2'*64))
    bound = policy()
    assert bound.http_capacity_source_fingerprint == '1'*64
    assert bound.http_capacity_authorization_fingerprint == '2'*64
    altered = entry(); altered['interval_seconds'] = 11
    assert not policy(altered).http_capacity_source_fingerprint
    altered = entry(); altered['authorization']['scope'] += ' Changed scope.'
    assert not policy(altered).http_capacity_authorization_fingerprint


def test_knews_retained_finite_admission_once_no_http_and_original_roles_remain(tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    from test_news_retained_native_replay import approved, head
    config = {'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'collection_purpose': 'personal_research', 'providers': [entry()]}
    auth = deepcopy(config['providers'][0]['authorization'])
    auth.pop('original_author_scope_evidence_urls')
    auth['scope'] = 'Original private diagnostics; not yet formal article admission.'
    queue, registry, path, manifest = approved(tmp_path, config, urls=KNEWS_REVIEWED_URLS,
        provider='knews', original_auth=auth,
        raw_factory=lambda url: str(page(KNEWS_REVIEWED_URLS.index(url))).encode())
    try:
        queue.sync_finite_cohorts()
        directory = path.parent/'private-source-diagnostics'
        directory.chmod(0o700)
        for member in directory.iterdir():
            member.chmod(0o600)
        manifest.update(contract_version=3, scope_kind='reviewed_finite_native_materialization_v3',
            original_author_scope_evidence_addition_reviewed=True, diagnostic_directory=directory.name)
        atomic_write_json(path, manifest, durable=True)
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
            lambda *a, **k: pytest.fail('retained originals cannot create an HTTP owner'))
        result = materialize_reviewed_native(queue, KNEWS_REVIEWED_URLS, path, lease_seconds=300)
        assert result['materialized_bodies']==2 and result['publisher_requests_this_command']==0
        heads = [head(queue,url) for url in KNEWS_REVIEWED_URLS]
        assert all(row['state']=='complete' for row in heads)
        for selected in heads:
            saved, = [row for row in pq.ParquetFile(queue.root/selected['parquet_path']).read().to_pylist()
                      if row['requested_url']==selected['url']]
            assert json.loads(saved['original_diagnostic_authorization_json'])==auth
            assert saved['acquisition_transport']=='retained_raw_reparse'
            assert saved['published_at_raw']==PUBLISHED and saved['modified_at_raw']==MODIFIED
            assert json.loads(saved['author_attribution_detail_json'])['printed_reporter_name']=='合成作者'
        assert materialize_reviewed_native(queue,KNEWS_REVIEWED_URLS,path,lease_seconds=300)['materialized_bodies']==0
        assert heads==[head(queue,url) for url in KNEWS_REVIEWED_URLS]
    finally:
        queue.close()
