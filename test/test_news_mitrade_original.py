"""Synthetic native Mitrade DOM; no downloaded article text in Git fixtures."""
from copy import deepcopy
from datetime import datetime, timedelta, UTC
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_mitrade import (
    MITRADE_BODY_SELECTOR, MITRADE_ORIGIN, MITRADE_PATH_PATTERN, mitrade_article_id,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


URL = MITRADE_ORIGIN + '/zh/insights/shares-analysis/more/20200101S01'
TITLE = '合成數據 -12.50 與 0%'
PUBLISHED = '2014-10-21T01:02:03Z'  # Deliberately different from the URL date.
MODIFIED = '2025-09-21T01:02:03Z'


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'mitrade', 'domains': ['www.mitrade.com'], 'enabled': True,
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 10, 'parallelism': 1, 'html_parser': 'lxml',
        'embedded_article_layout': 'mitrade_visible_article_v1',
        'article_path_pattern': MITRADE_PATH_PATTERN, 'article_selectors': [MITRADE_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-21',
            'evidence_url': MITRADE_ORIGIN + '/zh',
            'scope': 'Private known public native text; no third-party, media, export or training grant.',
            'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def page(*, url=URL, author='投資指南針', profile='compass', declaration_edit=None):
    locale = url.split('/')[3]
    profile_url = MITRADE_ORIGIN + f'/{locale}/insights/profile/{profile}'
    brand = '投资慧眼' if locale == 'cn' else '投資慧眼'
    declaration = {'@context': 'https://schema.org', '@type': 'NewsArticle',
        'url': url, 'mainEntityOfPage': {'@type': 'WebPage', '@id': url}, 'headline': TITLE,
        'description': '這個摘要絕不是完整正文', 'isAccessibleForFree': True,
        'author': {'@type': 'Person', 'name': author, 'url': profile_url},
        'publisher': {'@type': 'Organization', 'name': brand, 'logo': {'@type': 'ImageObject',
            'url': MITRADE_ORIGIN + f'/insights/image/logo/logo_top_{locale}.png'}},
        'datePublished': PUBLISHED, 'dateModified': MODIFIED}
    if declaration_edit:
        declaration_edit(declaration)
    prose = ''.join(f'<p>第{i}段 -12.50 與 0%。' + '合成原始值保留股數與比率。' * 6 + '</p>' for i in range(3))
    return f'''<html><head><title>{TITLE}</title><link rel="canonical" href="{url}">
      <meta property="og:type" content="article"><meta property="og:title" content="{TITLE}">
      <meta property="og:url" content="{url}"><meta name="og:url" content="{url}">
      <script type="application/ld+json">{json.dumps(declaration, ensure_ascii=False)}</script></head><body>
      <div id="articleDetail"><div class="article-content__articleTitle__fbEXV">
        <h1 class="article-content__title__5GvQ6">{TITLE}</h1>
        <div class="article-content__wrap__FGxnV"><div class="article-content__flex1__W6hv6"><div>
          <a class="article-content__newsAuthor__k-zC2" href="{profile_url}"><span>作者<!-- -->:</span> <!-- -->{author}</a>
          <span>更新於</span></div></div></div></div>
        <div class="article-content__coverImgBox__DzR+9"><img src="/cover.jpg">來源 : 照片權利人不是作者</div>
        <div id="articleCon"><div><a href="{MITRADE_ORIGIN}/{locale}/insights">Mitrade.com {brand}</a></div>
          <div class="article-content__articleDetailCon__kmMNZ" id="12345">
            <div class="article-content__contextC__QMEJJ">{prose}<p>圖片前原始值<img src="/inline.jpg">圖片後原始值</p></div></div>
          <div><p>{'阅读更多' if locale == 'cn' else '閱讀更多'}</p><div class="article-content__readBox__DRJfg">推薦不是本文，更新2026-10-21</div></div>
          <div>合成轉載說明<a href="">《{TITLE}》</a>保留原鏈結<a href="{MITRADE_ORIGIN}/{locale}/insights">{brand}</a>
            <a href="{MITRADE_ORIGIN}/en">www.mitrade.com</a></div>
          <div class="article-content__affirms__QFojR"><p>合成風險提示與槓桿損失警告</p></div></div>
      </div><div>其他來源文章與頁尾</div></body></html>'''


def parse(settings, html=None, *, url=URL):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['mitrade']
    return extract_article((html or page()).encode(), source, page_url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('locale,author,profile', [('zh', '投資指南針', 'compass'),
    ('zh', '財富進化論', 'wealth'), ('cn', 'Insights', 'insights')])
def test_literal_native_body_raw_values_credits_clocks_and_notices(settings, parser, locale, author, profile):
    settings['providers'][0]['html_parser'] = parser
    url = URL.replace('/zh/', f'/{locale}/')
    result = parse(settings, page(url=url, author=author, profile=profile), url=url)
    assert result['title'] == TITLE and json.loads(result['authors_json']) == [author]
    assert result['declared_canonical_url'] == url
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert 'not_observed_historical_PIT' in result['published_time_basis']
    assert 'no_URL_date_inference' in result['published_time_basis']
    assert json.loads(result['author_attribution_detail_json'])['role'].endswith('not_verified_person')
    for i in range(3):
        assert result['body'].count(f'第{i}段') == 1
    for value in ('-12.50', '0%', '圖片前原始值', '圖片後原始值'):
        assert value in result['body']
    for value in ('摘要', '推薦不是本文', '更新2026', '照片權利人', '風險提示', '轉載說明', '其他來源文章'):
        assert value not in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert [n['kind'] for n in notices] == ['publisher_reuse_notice', 'publisher_financial_risk_notice',
        'publisher_current_machine_clock', 'unfetched_inline_image']
    assert '保留原鏈結' in notices[0]['text'] and not notices[0]['open_license_or_bulk_permission_inferred']
    assert '槓桿損失' in notices[1]['text'] and not notices[2]['visible_article_clock_present']
    assert not notices[3]['media_requested']


@pytest.mark.parametrize('edit,code', [
    (lambda s: s.replace('rel="canonical" href="' + URL, 'rel="canonical" href="' + URL + '0'), 'article_identity_not_verified'),
    (lambda s: s.replace('name="og:url" content="' + URL, 'name="og:url" content="' + URL + '0'), 'article_identity_not_verified'),
    (lambda s: s.replace('<title>' + TITLE, '<title>其他報導'), 'article_identity_not_verified'),
    (lambda s: s.replace('id="articleDetail"', 'id="articleDetail" hidden'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>第0段', '<p style="display:none">第0段'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>第0段', '<p class="d-none">第0段'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>第0段', '<iframe></iframe><p>第0段'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>第0段', '<script>unknown()</script><p>第0段'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>第0段', '<p>轉載自其他網站 第0段'), 'article_rights_or_scope_excluded'),
    (lambda s: s.replace('id="articleCon">', 'id="articleCon"><div>未知新增正文</div>'), 'article_identity_not_verified'),
    (lambda s: s.replace('id="articleCon">', 'id="articleCon">容器的未知直接文字'), 'article_identity_not_verified'),
    (lambda s: s.replace('id="12345">', 'id="12345">未被包含的正文'), 'article_identity_not_verified'),
    (lambda s: s.replace('id="12345"', 'id="unknown"'), 'article_identity_not_verified'),
    (lambda s: s.replace('更新於</span>', '更新於</span><h1>推薦標題</h1>'), 'article_identity_not_verified'),
    (lambda s: s.replace('href="">《' + TITLE, 'href="">《另一篇'), 'article_identity_not_verified'),
    (lambda s: s.replace('合成風險提示與槓桿損失警告', ''), 'article_identity_not_verified'),
    (lambda s: s.replace('href="' + MITRADE_ORIGIN + '/zh/insights/profile/compass',
                        'href="' + MITRADE_ORIGIN + '/zh/insights/profile/wealth'), 'article_attribution_not_verified'),
])
def test_unknown_identity_hidden_content_rights_and_shape_do_not_fall_back(settings, edit, code):
    with pytest.raises(NewsError, match='^' + code + '$'):
        parse(settings, edit(page()))


@pytest.mark.parametrize('key,value,code', [
    ('isAccessibleForFree', False, 'paywall_or_login'),
    ('isAccessibleForFree', 'true', 'article_identity_not_verified'),
    ('url', URL + '0', 'article_identity_not_verified'),
    ('mainEntityOfPage', {'@type': 'WebPage', '@id': URL + '0'}, 'article_identity_not_verified'),
    ('author', {'@type': 'Person', 'name': ['投資指南針']}, 'article_attribution_not_verified'),
    ('publisher', None, 'article_identity_not_verified'),
    ('articleBody', '摘要不能替代原文', 'article_identity_not_verified'),
    ('datePublished', '2014-02-30T01:02:03Z', 'invalid_bulk_publication_clock'),
    ('datePublished', '2014-10-21', 'invalid_bulk_publication_clock'),
    ('dateModified', '2014-10-21T01:02:03', 'invalid_bulk_publication_clock'),
    ('dateModified', '2013-10-21T01:02:03Z', 'invalid_bulk_publication_clock'),
])
def test_source_declarations_cannot_substitute_identity_body_or_clock(settings, key, value, code):
    with pytest.raises(NewsError, match='^' + code + '$'):
        parse(settings, page(declaration_edit=lambda d: d.update({key: value})))


def test_unknown_native_profile_is_not_arbitrarily_promoted_to_own_author(settings):
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        parse(settings, page(author='未審核共同作者', profile='other'))


def test_embedded_null_in_notice_is_not_numeric_zero_or_body_and_engines_agree(settings):
    raw = page().replace('合成風險提示與槓桿損失警告', '合成風險\x00提示與槓桿損失警告 0%')
    results = []
    for parser in ['lxml', 'html.parser']:
        config = deepcopy(settings)
        config['providers'][0]['html_parser'] = parser
        results.append(parse(config, raw))
    assert results[0] == results[1] and '\x00' in raw
    notice = json.loads(results[0]['verified_article_notices_json'])[1]
    assert notice['text'] == '合成風險提示與槓桿損失警告 0%'
    assert 'NUL_controls_omitted' in notice['text_projection_basis']
    assert '0%' in results[0]['body'] and '-12.50' in results[0]['body']


@pytest.mark.parametrize('url', [URL.replace('www.', ''), URL + '?x=1', URL + '#x', URL + '/',
    URL.replace('https:', 'http:'), URL.replace('/zh/', '/zh-au/'), URL.replace('/more/', '/preview/'),
    URL.replace('/more/', '/more%2f'), URL.rsplit('/', 1)[0] + '/' + 'x' * 129, 42, None])
def test_no_guessed_alias_locale_date_or_query(url):
    assert mitrade_article_id(url) is None


@pytest.mark.parametrize('slug', ['0250829CF02', '1785-gen-20251223', '20250829', 'literal-ID_not-a-date'])
def test_identity_is_literal_not_an_invented_url_calendar(settings, slug):
    url = URL.rsplit('/', 1)[0] + '/' + slug
    assert mitrade_article_id(url) == url.removeprefix(MITRADE_ORIGIN)
    result = parse(settings, page(url=url), url=url)
    assert result['declared_canonical_url'] == url and result['published_at_raw'] == PUBLISHED


@pytest.mark.parametrize('field,value', [('domains', ['mitrade.com']), ('article_selectors', ['body']),
    ('exclude_selectors', ['p']), ('feeds', [MITRADE_ORIGIN + '/feed']), ('require_author_attribution', False),
    ('require_configured_container', False), ('title_selector', 'h2'),
    ('private_known_url_scope', 'all_articles'), ('article_path_pattern', '^/.*$')])
def test_configuration_cannot_broaden_reviewed_native_scope(settings, field, value):
    settings['providers'][0][field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(settings, base=Path.cwd())


def test_fetch_rejects_different_literal_final_identity(settings):
    registry = PolicyRegistry(settings, base=Path.cwd())
    http = SimpleNamespace(registry=registry, get=lambda url, **kwargs:
        Response(URL.replace('/zh/', '/cn/'), 200, {'content-type': 'text/html'}, page().encode()))
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        fetch_article(http, {'url': URL})


def test_fetch_rejects_unreviewed_path_before_http(settings):
    registry = PolicyRegistry(settings, base=Path.cwd())
    def unexpected_get(*args, **kwargs):
        pytest.fail('unsupported paths must not consume a source request')
    http = SimpleNamespace(registry=registry, get=unexpected_get)
    with pytest.raises(NewsError, match='^unsupported_article_path$'):
        fetch_article(http, {'url': URL.replace('/more/', '/preview/')})


@pytest.mark.parametrize('route', ['shares-analysis/more', 'shares-analysis/weekly',
    'shares-analysis/outlook-of-month', 'commodity-analysis/oil', 'commodity-analysis/xau',
    'crypto-analysis/eth', 'forex-analysis/eur', 'forex-analysis/more',
    'more-analysis/industry-analysis', 'more-analysis/more'])
def test_other_locally_observed_routes_still_require_the_same_original_identity(settings, route):
    url = MITRADE_ORIGIN+'/zh/insights/'+route+'/literal-existing-ID'
    assert mitrade_article_id(url) is not None
    assert parse(settings, page(url=url), url=url)['declared_canonical_url'] == url
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        parse(settings, page(url=url).replace('profile/compass', 'profile/other'), url=url)


def test_failure_metadata_retains_bounded_native_evidence_not_unverified_body_or_access_scope():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_mitrade import mitrade_validation_metadata
    raw = page(author='尚未審核的原生署名', profile='unreviewed')
    soup = BeautifulSoup(raw, 'lxml')
    before = str(soup)
    result = mitrade_validation_metadata(soup, page_url=URL)
    assert str(soup) == before
    assert result['native_credits']['values'][0]['value'] == '作者 : 尚未審核的原生署名'
    assert result['native_profiles']['values'][0]['value'].endswith('/profile/unreviewed')
    assert result['native_body_count'] == 1 and result['native_body_structures'][0]['tag_counts']['p'] == 4
    assert not result['article_identity_or_scope_granted'] and not result['body_text_retained']
    assert result['additional_requests'] == 0 and not result['raw_response_retained']
    assert '第0段' not in json.dumps(result, ensure_ascii=False)


def test_failure_metadata_does_not_export_queries_credentials_or_an_unbounded_byline():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_mitrade import mitrade_validation_metadata
    raw = page(author='長'*2000).replace('/profile/compass', '/profile/compass?token=secret#secret')
    result = mitrade_validation_metadata(BeautifulSoup(raw, 'lxml'), page_url=URL)
    credit = result['native_credits']['values'][0]
    assert credit['truncated'] and len(credit['value']) == 512
    assert 'secret' not in json.dumps(result)


def test_explicit_editorial_metadata_preserves_default_abi_and_contains_no_prose():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_mitrade import mitrade_validation_metadata
    raw = page(author='Mitrade', profile='mitrade-team', declaration_edit=lambda d: d.update(
        reviewedBy={'@type': 'Organization', 'name': 'Mitrade',
                    'url': MITRADE_ORIGIN+'/zh/insights/profile/mitrade-team?token=secret'}))
    soup = BeautifulSoup(raw, 'lxml')
    before = str(soup)
    default = mitrade_validation_metadata(soup, page_url=URL)
    result = mitrade_validation_metadata(soup, page_url=URL, include_editorial=True)
    editorial = result.pop('editorial_structure')
    assert result == default and str(soup) == before
    assert editorial['jsonld'][0]['author'][0]['name'] == 'Mitrade'
    assert editorial['jsonld'][0]['reviewedBy'][0]['type'] == 'Organization'
    assert editorial['jsonld'][0]['reviewedBy'][0]['profile_url'].endswith('/mitrade-team')
    assert len(editorial['body_containers'][0]['children']) == 5
    assert not editorial['scope_granted'] and not editorial['attribution_verified']
    serialized = json.dumps(editorial, ensure_ascii=False)
    for value in ('第0段', '這個摘要', '合成轉載說明', '合成風險', 'secret'):
        assert value not in serialized

    with_notice = raw.replace('<div class="article-content__affirms__QFojR">',
        '<div>以上內容在AI的輔助下完成，並經過編輯校對。</div><div class="article-content__affirms__QFojR">')
    bounded = mitrade_validation_metadata(BeautifulSoup(with_notice, 'lxml'), page_url=URL,
                                          include_editorial=True)['editorial_structure']
    assert bounded['body_containers'][0]['children'][-2]['matches_reviewed_AI_notice']
    assert '以上內容' not in json.dumps(bounded, ensure_ascii=False)


def test_editorial_metadata_bounds_untrusted_schema_header_and_siblings():
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_mitrade import mitrade_validation_metadata
    raw = page(author='長'*2000).replace('/profile/compass', '/profile/compass?token=secret')
    raw = raw.replace('<span>更新於', ('<i class="'+'x'*200+'"></i>')*200+'<span>更新於')
    raw = raw.replace('id="articleCon">', 'id="articleCon">'+'<div>不可保留</div>'*100)
    raw = raw.replace('</head>', '<script type="application/ld+json">'+'x'*1_048_577+'</script></head>')
    result = mitrade_validation_metadata(BeautifulSoup(raw, 'lxml'), page_url=URL, include_editorial=True)
    editorial = result['editorial_structure']
    assert editorial['headers'][0]['truncated'] and len(editorial['headers'][0]['elements']) == 96
    assert editorial['body_containers'][0]['truncated']
    assert len(editorial['body_containers'][0]['children']) == 32
    assert editorial['jsonld'][1]['oversized'] and 'keys' not in editorial['jsonld'][1]
    assert '不可保留' not in json.dumps(editorial) and 'secret' not in json.dumps(editorial)


def test_canonical_probe_observes_company_schema_without_accepting_or_retaining_body(settings, tmp_path, capsys):
    from scripts.probe_news_sources import probe_provider
    registry = PolicyRegistry(settings, base=Path.cwd())
    calls = []
    def get(url):
        calls.append(url)
        return Response(url, 200, {'content-type': 'text/html'},
                        page(author='Mitrade', profile='mitrade-team').encode())
    http = SimpleNamespace(registry=registry, get=get, provider_not_before={}, robot_delays={})
    result = probe_provider(http, [URL], output_root=tmp_path)[0]
    assert calls == [URL] and not list(tmp_path.iterdir())
    assert result['extraction_error'] == 'article_rights_or_scope_excluded'
    editorial = result['mitrade_native_validation']['editorial_structure']
    assert editorial['jsonld'][0]['author'][0]['name'] == 'Mitrade'
    assert not editorial['scope_granted'] and not editorial['attribution_verified']
    assert '第0段' not in capsys.readouterr().out


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v2_adds_only_the_official_publisher_declared_analysis_team(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    raw = page(author='Mitrade 分析師', profile='mitrade-editor')
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        parse(settings, raw)
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v2'
    result = parse(settings, raw)
    assert json.loads(result['authors_json']) == ['Mitrade 分析師']
    assert result['body'] == parse(settings)['body']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['role'] == 'publisher_declared_analysis_team_not_verified_person'
    assert detail['profile_url'].endswith('/zh/insights/profile/mitrade-editor')
    assert not detail['real_identity_independently_verified']


@pytest.mark.parametrize('author,profile', [('Mitrade 團隊', 'mitrade-editor'),
    ('Mitrade 分析師、別人', 'mitrade-editor'), ('中央社', 'cna'), ('投資指南針', 'mitrade-editor')])
def test_v2_does_not_promote_other_team_labels_or_mismatched_profiles(settings, author, profile):
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v2'
    with pytest.raises(NewsError):
        parse(settings, page(author=author, profile=profile))


@pytest.mark.parametrize('obstruction', [None, 'body', 'parquet', 'lease_owner', 'lease_until',
    'missing_clock', 'naive_clock', 'future_clock', 'http_403', 'unsupported_path',
    'different_predecessor', 'changed_new_authorization'])
@pytest.mark.parametrize('version', [2, 4, 5, 6, 7])
def test_pinned_reviewed_profile_scope_migration_preserves_deadlines_and_protected_heads(settings, tmp_path, monkeypatch, obstruction, version):
    from downloader.news_fulltext_queue import NewsQueue
    if version >= 4:
        settings['providers'][0]['embedded_article_layout'] = f'mitrade_visible_article_v{version-1}'
    old = PolicyRegistry(settings, base=tmp_path)
    current = deepcopy(settings)
    current['providers'][0]['embedded_article_layout'] = f'mitrade_visible_article_v{version}'
    current['providers'][0]['authorization']['scope'] += f' Explicitly reviewed one native profile pair v{version}.'
    new = PolicyRegistry(current, base=tmp_path)
    pair = (old.sources['mitrade'].data_fingerprint, new.sources['mitrade'].data_fingerprint)
    monkeypatch.setattr(f'downloader.news_fulltext_mitrade.MITRADE_V{version}_SCOPE_REPAIR_IDENTITY', pair)
    if version in {6, 7}:
        monkeypatch.setattr(f'downloader.news_fulltext_mitrade.MITRADE_V{version}_REPAIR_COHORT', frozenset({URL}))
    url = URL.replace('/zh/', '/zh-au/') if obstruction == 'unsupported_path' else URL
    queue = NewsQueue(tmp_path/'news', old)
    try:
        with queue.db:
            queue.add_articles([{'url': url}])
            queue.db.execute("UPDATE articles SET state='article_rights_or_scope_excluded',"
                "error_code='article_rights_or_scope_excluded',attempts=3,due=9000000000,last_checked_at_utc=? WHERE url=?",
                ((datetime.now(UTC)-timedelta(seconds=10)).isoformat(), url))
            fields = {'body': ('body_sha256', 'stored-body'), 'parquet': ('parquet_path', 'stored.parquet'),
                'lease_owner': ('lease_owner', 'active-owner'), 'lease_until': ('lease_until', 9000000000),
                'missing_clock': ('last_checked_at_utc', None), 'naive_clock': ('last_checked_at_utc', '2026-10-01T00:00:00'),
                'future_clock': ('last_checked_at_utc', (datetime.now(UTC)+timedelta(days=1)).isoformat())}
            if obstruction in fields:
                field, value = fields[obstruction]
                queue.db.execute('UPDATE articles SET '+field+'=? WHERE url=?', (value, url))
            elif obstruction == 'http_403':
                queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403' WHERE url=?", (url,))
            elif obstruction == 'different_predecessor':
                queue.db.execute("UPDATE queue_settings SET value='unreviewed predecessor' WHERE key='source_capture_fingerprint:mitrade'")
            queue.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('failure_validation_metadata_attempts:v1:mitrade:article_rights_or_scope_excluded:20261020','2')")
            if version in {6, 7}:
                neighbor = URL+'-not-reviewed'
                queue.add_articles([{'url': neighbor}])
                queue.db.execute("UPDATE articles SET state='article_rights_or_scope_excluded',"
                    "error_code='article_rights_or_scope_excluded',last_checked_at_utc=? WHERE url=?",
                    ((datetime.now(UTC)-timedelta(seconds=10)).isoformat(), neighbor))
                neighbor_before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (neighbor,)).fetchone())
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    finally:
        queue.close()
    if obstruction == 'changed_new_authorization':
        current['providers'][0]['authorization']['scope'] += ' A later unreviewed change.'
        new = PolicyRegistry(current, base=tmp_path)
    queue = NewsQueue(tmp_path/'news', new)
    try:
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        expected = {**before, 'state': 'retry'} if obstruction is None else before
        assert after == expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_validation_metadata_attempts:v1:mitrade:article_rights_or_scope_excluded:20261020'").fetchone()[0] == '2'
        if version in {6, 7}:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (neighbor,)).fetchone()) == neighbor_before
        queue.recheck_mitrade_reviewed_profile_scope(pair[0])
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
    finally:
        queue.close()


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v4_adds_only_the_reviewed_zh_insights_profile_without_changing_text_or_clocks(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    raw = page(author='Insights', profile='insights')
    for version in (1, 2, 3):
        settings['providers'][0]['embedded_article_layout'] = f'mitrade_visible_article_v{version}'
        with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
            parse(settings, raw)
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v4'
    result = parse(settings, raw)
    assert result['body'] == parse(settings)['body']
    assert json.loads(result['authors_json']) == ['Insights']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['profile_url'] == MITRADE_ORIGIN+'/zh/insights/profile/insights'
    assert detail['role'] == 'publisher_declared_profile_not_verified_person'
    assert not detail['real_identity_independently_verified']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert result['article_identity_basis'].endswith('_v4')


@pytest.mark.parametrize('author,profile', [('Insight', 'insights'), ('Insights、其他', 'insights'),
    ('中央社', 'insights'), ('Insights', 'mitrade-editor'), ('Insights', 'INSIGHTS')])
def test_v4_never_promotes_other_labels_or_guessed_profile_aliases(settings, author, profile):
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v4'
    with pytest.raises(NewsError):
        parse(settings, page(author=author, profile=profile))


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('author,profile', [('Alison Ho', 'alison-ho'), ('Tony Chou', 'tony')])
def test_v5_adds_only_reviewed_native_writer_pairs_preserving_original_values_and_clocks(settings, parser, author, profile):
    settings['providers'][0]['html_parser'] = parser
    raw = page(author=author, profile=profile)
    for version in (1, 2, 3, 4):
        settings['providers'][0]['embedded_article_layout'] = f'mitrade_visible_article_v{version}'
        with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
            parse(settings, raw)
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v5'
    result = parse(settings, raw)
    assert result['body'] == parse(settings)['body']
    assert json.loads(result['authors_json']) == [author]
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['profile_url'] == MITRADE_ORIGIN+'/zh/insights/profile/'+profile
    assert detail['role'] == 'publisher_declared_profile_not_verified_person'
    assert not detail['real_identity_independently_verified']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert result['article_identity_basis'].endswith('_v5')
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        parse(settings, page(url=URL.replace('/zh/', '/cn/'), author=author, profile=profile),
              url=URL.replace('/zh/', '/cn/'))


@pytest.mark.parametrize('author,profile', [('Alison Ho', 'tony'), ('Tony Chou', 'alison-ho'),
    ('Alison', 'alison-ho'), ('Tony Chou、別人', 'tony'), ('中央社', 'tony')])
def test_v5_keeps_ambiguous_mismatched_or_unreviewed_writer_pairs_unaccepted(settings, author, profile):
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v5'
    with pytest.raises(NewsError):
        parse(settings, page(author=author, profile=profile))


@pytest.mark.parametrize('mutation', [None, 'interval', 'parallelism', 'scope', 'review_date', 'unreviewed_new_contract'])
def test_v5_http_state_inheritance_requires_the_complete_reviewed_source_identity(settings, tmp_path, monkeypatch, mutation):
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v4'
    old = PolicyRegistry(settings, base=tmp_path).sources['mitrade']
    current = deepcopy(settings)
    current['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v5'
    current['providers'][0]['authorization']['scope'] += ' Two reviewed native writer pairs.'
    candidate = PolicyRegistry(current, base=tmp_path).sources['mitrade']
    monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V5_SCOPE_REPAIR_IDENTITY',
        (old.data_fingerprint, candidate.data_fingerprint))
    monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V5_PRIOR_HTTP_AUTHORIZATION_FINGERPRINT',
        old.authorization_fingerprint)
    entry = current['providers'][0]
    if mutation == 'interval': entry['interval_seconds'] = 20
    if mutation == 'parallelism': entry['parallelism'] = 2
    if mutation == 'scope': entry['authorization']['scope'] += ' Different scope.'
    if mutation == 'review_date': entry['authorization']['reviewed_on'] = '2026-10-22'
    if mutation == 'unreviewed_new_contract':
        monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V5_SCOPE_REPAIR_IDENTITY',
            (old.data_fingerprint, '0'*64))
    source = PolicyRegistry(current, base=tmp_path).sources['mitrade']
    assert source.authorization_fingerprint != old.authorization_fingerprint
    assert source.http_capacity_source_fingerprint == (old.data_fingerprint if mutation is None else '')
    assert source.http_capacity_authorization_fingerprint == (old.authorization_fingerprint if mutation is None else '')


@pytest.mark.parametrize('mutation', [None, 'interval', 'parallelism', 'scope', 'review_date', 'unreviewed_new_contract'])
@pytest.mark.parametrize('rate_refused', [False, True])
def test_v5_real_checkpoint_retains_only_reviewed_http_measurements_and_never_resets_refusals(
        settings, tmp_path, monkeypatch, mutation, rate_refused):
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_capacity import PublisherCapacity
    from test_news_fulltext import http_fixture

    settings['providers'][0].update(embedded_article_layout='mitrade_visible_article_v4', capacity_max_parallelism=2)
    previous = PolicyRegistry(settings, base=tmp_path).sources['mitrade']
    current = deepcopy(settings)
    current['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v5'
    current['providers'][0]['authorization']['scope'] += ' Two reviewed native writer pairs.'
    candidate = PolicyRegistry(current, base=tmp_path).sources['mitrade']
    monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V5_SCOPE_REPAIR_IDENTITY',
        (previous.data_fingerprint, candidate.data_fingerprint))
    monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V5_PRIOR_HTTP_AUTHORIZATION_FINGERPRINT',
        previous.authorization_fingerprint)
    entry = current['providers'][0]
    if mutation == 'interval': entry['interval_seconds'] = 20
    if mutation == 'parallelism': entry['parallelism'] = 2
    if mutation == 'scope': entry['authorization']['scope'] += ' Different scope.'
    if mutation == 'review_date': entry['authorization']['reviewed_on'] = '2026-10-22'
    if mutation == 'unreviewed_new_contract':
        monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V5_SCOPE_REPAIR_IDENTITY',
            (previous.data_fingerprint, '0'*64))
    old_http, old_session = http_fixture(settings, tmp_path, {})
    try:
        old = PublisherCapacity(old_http, settings)
        old._set('mitrade', 2, 1, 'verifying', 'synthetic saved learned capacity')
        old.states['mitrade']['history'] = [{'test_witness': 'retained measured window'}]
        saved_fingerprint = old.provider_fingerprints['mitrade']
        atomic_write_json(old.path, old.snapshot())
        if rate_refused:
            with pytest.raises(NewsError, match='^http_429$'):
                old_http._response_status('mitrade', 429, {'retry-after': '120'})
            refused_deadline = old_http.provider_not_before['mitrade']
        assert not old_session.calls
    finally:
        old_http.close()
    http, session = http_fixture(current, tmp_path, {})
    try:
        restored = PublisherCapacity(http, current)
        source = http.registry.sources['mitrade']
        actual_authorization = hashlib.sha256(json.dumps(entry['authorization'], sort_keys=True).encode()).hexdigest()
        assert source.authorization_fingerprint == actual_authorization
        assert source.authorization_fingerprint != previous.authorization_fingerprint
        assert (restored.provider_fingerprints['mitrade'] == saved_fingerprint) == (mutation is None)
        if mutation is None:
            assert restored.states['mitrade']['history'] == [{'test_witness': 'retained measured window'}]
            assert http.effective_interval('mitrade') == (4 if rate_refused else 2)
        else:
            assert not restored.states['mitrade']['history']
            assert http.client_intervals['mitrade'] == entry['interval_seconds']
        if rate_refused:
            assert http.provider_not_before['mitrade'] == refused_deadline
            assert http.rate_refusal_intervals['mitrade'] == 4
            http.set_client_capacity('mitrade', .1, 2)
            assert http.effective_interval('mitrade') == 4
        # A prior owner's reporting windows are not a current ETA sample.
        assert not restored.completion_observations['mitrade']['history']
        assert not session.calls
    finally:
        http.close()


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v6_adds_the_exact_case_sensitive_native_pen_name_without_changing_raw_values(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    raw = page(author='投資-槓把子', profile='Invest')
    for version in range(1, 6):
        settings['providers'][0]['embedded_article_layout'] = f'mitrade_visible_article_v{version}'
        with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
            parse(settings, raw)
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v6'
    result = parse(settings, raw)
    assert result['body'] == parse(settings)['body']
    assert json.loads(result['authors_json']) == ['投資-槓把子']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['profile_url'] == MITRADE_ORIGIN+'/zh/insights/profile/Invest'
    assert detail['role'] == 'publisher_declared_profile_not_verified_person'
    assert not detail['real_identity_independently_verified']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert result['article_identity_basis'].endswith('_v6')
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        parse(settings, page(url=URL.replace('/zh/', '/cn/'), author='投資-槓把子', profile='Invest'),
              url=URL.replace('/zh/', '/cn/'))


@pytest.mark.parametrize('author,profile', [('投資-槓把子', 'invest'), ('投資-槓把子', 'INVEST'),
    ('投資槓把子', 'Invest'), ('投資-槓把子、別人', 'Invest'), ('中央社', 'Invest'),
    ('Mitrade', 'mitrade-team')])
def test_v6_keeps_guessed_case_aliases_company_and_other_credits_excluded(settings, author, profile):
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v6'
    with pytest.raises(NewsError):
        parse(settings, page(author=author, profile=profile))


def company_page():
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(page(author='Mitrade', profile='mitrade-team'), 'html.parser')
    reviewer = BeautifulSoup('''<div class="article-content__articleDetailTitle__Kjfdx">
      <div class="article-content__reviewer__vvt-C"><div class="article-content__authorTag__RVK1L">審核人</div>
      <div class="article-content__hover__YR39T"><a class="article-content__name__l8-t8"
      href="https://www.mitrade.com/zh/insights/profile/mitrade-team">Mitrade</a></div></div></div>''', 'html.parser')
    soup.select_one('.article-content__articleTitle__fbEXV').append(reviewer.div)
    ai = soup.new_tag('p')
    ai.string = '以上內容在AI的輔助下完成，並經過編輯校對。'
    soup.select_one('.article-content__affirms__QFojR').insert(0, ai)
    return str(soup)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v7_keeps_company_author_reviewer_and_AI_notice_distinct_from_prose(settings, parser):
    settings['providers'][0].update(html_parser=parser, embedded_article_layout='mitrade_visible_article_v7')
    result = parse(settings, company_page())
    assert result['body'] == parse(settings)['body']
    assert json.loads(result['authors_json']) == ['Mitrade']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['publisher_schema_type'] == 'Person'  # Literal source declaration, not our inference.
    assert detail['role'] == 'publisher_declared_company_author_not_verified_person'
    assert not detail['real_identity_independently_verified'] and detail['AI_assistance_declared'] is True
    reviewer, = detail['reviewers']
    assert reviewer['native_role'] == '審核人' and reviewer['native_credit'] == 'Mitrade'
    assert reviewer['role'] == 'publisher_declared_company_reviewer_not_verified_person'
    assert not reviewer['independent_person_or_fact_check_verified']
    assert 'no_JSONLD_reviewedBy' in reviewer['basis']
    notices = json.loads(result['verified_article_notices_json'])
    ai, = [n for n in notices if n['kind'] == 'publisher_AI_assisted_editorial_notice']
    assert ai['text'] == '以上內容在AI的輔助下完成，並經過編輯校對。'
    assert not ai['human_author_or_fact_accuracy_independently_verified']
    assert not ai['open_license_or_bulk_permission_inferred']
    risk, = [n for n in notices if n['kind'] == 'publisher_financial_risk_notice']
    assert '合成風險' in risk['text'] and ai['text'] not in risk['text'] and ai['text'] not in result['body']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('edit,code', [
    (lambda s: s.replace('article-content__reviewer__vvt-C', 'unknown-reviewer'), 'article_attribution_not_verified'),
    (lambda s: s.replace('article-content__reviewer__vvt-C"', 'article-content__reviewer__vvt-C" hidden'), 'article_identity_not_verified'),
    (lambda s: s.replace('審核人</div>', '作者</div>'), 'article_attribution_not_verified'),
    (lambda s: s.replace('>Mitrade</a></div>', '>另一位審核人</a></div>'), 'article_attribution_not_verified'),
    (lambda s: s.replace('/profile/mitrade-team', '/profile/other'), 'article_attribution_not_verified'),
    (lambda s: s.replace('/profile/mitrade-team', '/profile/mitrade-team?token=x'), 'article_attribution_not_verified'),
    (lambda s: s.replace('以上內容在AI的輔助下完成，並經過編輯校對。', ''), 'article_identity_not_verified'),
    (lambda s: s.replace('以上內容在AI的輔助下完成，並經過編輯校對。', '已經真人核實所有事實'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>以上內容', '<p hidden>以上內容'), 'article_identity_not_verified'),
    (lambda s: s.replace('<p>以上內容', '<p>額外未分類聲明</p><p>以上內容'), 'article_identity_not_verified'),
    (lambda s: s.replace('"datePublished":', '"reviewedBy": null, "datePublished":'), 'article_attribution_not_verified'),
    (lambda s: s.replace('"@type": "Person"', '"@type": "Organization"'), 'article_attribution_not_verified'),
])
def test_v7_never_infers_human_author_or_drops_unknown_reviewer_or_AI_notice(settings, edit, code):
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v7'
    with pytest.raises(NewsError, match='^'+code+'$'):
        parse(settings, edit(company_page()))


def _v6_pinned_transition(settings, tmp_path, monkeypatch):
    """A real two-hop measurement identity, not a raw v5 capture identity."""
    settings['providers'][0].update(embedded_article_layout='mitrade_visible_article_v4', capacity_max_parallelism=2)
    v4 = PolicyRegistry(settings, base=tmp_path).sources['mitrade']
    previous_config = deepcopy(settings)
    previous_config['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v5'
    previous_config['providers'][0]['authorization']['scope'] += ' Two reviewed native writers.'
    v5 = PolicyRegistry(previous_config, base=tmp_path).sources['mitrade']
    monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V5_SCOPE_REPAIR_IDENTITY',
        (v4.data_fingerprint, v5.data_fingerprint))
    monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V5_PRIOR_HTTP_AUTHORIZATION_FINGERPRINT',
        v4.authorization_fingerprint)
    previous = PolicyRegistry(previous_config, base=tmp_path).sources['mitrade']
    assert previous.http_capacity_source_fingerprint == v4.data_fingerprint != previous.data_fingerprint
    assert previous.http_capacity_authorization_fingerprint == v4.authorization_fingerprint != previous.authorization_fingerprint
    current = deepcopy(previous_config)
    current['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v6'
    current['providers'][0]['authorization']['scope'] += ' Exact reviewed zh pen name/Invest.'
    candidate = PolicyRegistry(current, base=tmp_path).sources['mitrade']
    monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V6_SCOPE_REPAIR_IDENTITY',
        (previous.data_fingerprint, candidate.data_fingerprint))
    monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V6_PRIOR_HTTP_MEASUREMENT_IDENTITY',
        (previous.http_capacity_source_fingerprint, previous.http_capacity_authorization_fingerprint))
    return previous_config, previous, current


@pytest.mark.parametrize('target_version', [6, 7])
@pytest.mark.parametrize('mutation', [None, 'interval', 'parallelism', 'scope', 'review_date', 'unreviewed_new_contract'])
@pytest.mark.parametrize('rate_refused', [False, True])
def test_v6_two_hop_checkpoint_inherits_effective_measurement_identity_not_v5_capture(
        settings, tmp_path, monkeypatch, mutation, rate_refused, target_version):
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_capacity import PublisherCapacity
    from test_news_fulltext import http_fixture

    previous_config, previous, current = _v6_pinned_transition(settings, tmp_path, monkeypatch)
    if target_version == 7:
        previous_config = current
        previous = PolicyRegistry(previous_config, base=tmp_path).sources['mitrade']
        current = deepcopy(previous_config)
        current['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v7'
        current['providers'][0]['authorization']['scope'] += ' Exact company/reviewer/AI notice scope.'
        candidate = PolicyRegistry(current, base=tmp_path).sources['mitrade']
        monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V7_SCOPE_REPAIR_IDENTITY',
            (previous.data_fingerprint, candidate.data_fingerprint))
        monkeypatch.setattr('downloader.news_fulltext_mitrade.MITRADE_V7_PRIOR_HTTP_MEASUREMENT_IDENTITY',
            (previous.http_capacity_source_fingerprint, previous.http_capacity_authorization_fingerprint))
    entry = current['providers'][0]
    if mutation == 'interval': entry['interval_seconds'] = 20
    if mutation == 'parallelism': entry['parallelism'] = 2
    if mutation == 'scope': entry['authorization']['scope'] += ' Different scope.'
    if mutation == 'review_date': entry['authorization']['reviewed_on'] = '2026-10-22'
    if mutation == 'unreviewed_new_contract':
        monkeypatch.setattr(f'downloader.news_fulltext_mitrade.MITRADE_V{target_version}_SCOPE_REPAIR_IDENTITY',
            (previous.data_fingerprint, '0'*64))
    old_http, old_session = http_fixture(previous_config, tmp_path, {})
    try:
        old = PublisherCapacity(old_http, previous_config)
        old._set('mitrade', 2, 1, 'verifying', 'synthetic saved v4 measurement retained through v5')
        old.states['mitrade']['history'] = [{'test_witness': 'v4 effective measurement'}]
        saved_fingerprint = old.provider_fingerprints['mitrade']
        atomic_write_json(old.path, old.snapshot())
        if rate_refused:
            with pytest.raises(NewsError, match='^http_429$'):
                old_http._response_status('mitrade', 429, {'retry-after': '120'})
            refused_deadline = old_http.provider_not_before['mitrade']
        assert not old_session.calls
    finally:
        old_http.close()
    http, session = http_fixture(current, tmp_path, {})
    try:
        restored = PublisherCapacity(http, current)
        source = http.registry.sources['mitrade']
        assert source.authorization_fingerprint == hashlib.sha256(json.dumps(entry['authorization'], sort_keys=True).encode()).hexdigest()
        assert source.authorization_fingerprint != previous.authorization_fingerprint
        assert (restored.provider_fingerprints['mitrade'] == saved_fingerprint) == (mutation is None)
        if mutation is None:
            assert source.http_capacity_source_fingerprint == previous.http_capacity_source_fingerprint
            assert source.http_capacity_source_fingerprint != previous.data_fingerprint
            assert source.http_capacity_authorization_fingerprint == previous.http_capacity_authorization_fingerprint
            assert restored.states['mitrade']['history'] == [{'test_witness': 'v4 effective measurement'}]
            assert http.effective_interval('mitrade') == (4 if rate_refused else 2)
        else:
            assert not restored.states['mitrade']['history']
            assert not source.http_capacity_authorization_fingerprint and not source.http_capacity_source_fingerprint
            assert http.client_intervals['mitrade'] == entry['interval_seconds']
        if rate_refused:
            assert http.provider_not_before['mitrade'] == refused_deadline
            assert http.rate_refusal_intervals['mitrade'] == 4
            http.set_client_capacity('mitrade', .1, 2)
            assert http.effective_interval('mitrade') == 4
        assert not restored.completion_observations['mitrade']['history']
        assert not session.calls
    finally:
        http.close()


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v3_preserves_complete_numeric_tables_with_native_column_presentation(settings, parser):
    settings['providers'][0]['html_parser'] = parser
    table = '<table><colgroup><col style="width:40%"><col style="width:60%"></colgroup><tbody><tr><td>原始值</td><td>-12.50</td></tr><tr><td>比率</td><td>0%</td></tr></tbody></table>'
    raw = page().replace('<p>第0段', table+'<p>第0段')
    for version in (1, 2):
        settings['providers'][0]['embedded_article_layout'] = f'mitrade_visible_article_v{version}'
        with pytest.raises(NewsError, match='^article_identity_not_verified$'):
            parse(settings, raw)
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v3'
    result = parse(settings, raw)
    assert result['body'].splitlines()[:4] == ['原始值', '-12.50', '比率', '0%']
    assert all(f'第{i}段' in result['body'] for i in range(3))
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert result['article_identity_basis'].endswith('_v3')


@pytest.mark.parametrize('table', ['<colgroup><col></colgroup>', '<col>',
    '<table><colgroup>hidden text<col></colgroup></table>',
    '<table><colgroup style="display:none"><col></colgroup></table>',
    '<table><colgroup><span>not a column</span></colgroup></table>',
    '<table><colgroup><col></colgroup><script>not prose</script></table>'])
def test_v3_column_support_does_not_weaken_structure_visibility_or_active_content(settings, table):
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v3'
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        parse(settings, page().replace('<p>第0段', table+'<p>第0段'))


@pytest.mark.parametrize('obstruction', [None, 'body', 'lease', 'http_403', 'rights', 'missing_clock',
    'naive_clock', 'future_clock', 'changed_authorization', 'different_predecessor'])
def test_v3_table_only_migration_keeps_http_scope_budgets_and_future_retry_deadlines(settings, tmp_path, obstruction):
    from downloader.news_fulltext_queue import NewsQueue, native_text_parser_repair
    settings['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v2'
    old = PolicyRegistry(settings, base=tmp_path)
    current = deepcopy(settings)
    current['providers'][0]['embedded_article_layout'] = 'mitrade_visible_article_v3'
    new = PolicyRegistry(current, base=tmp_path)
    previous = old.sources['mitrade']
    source = new.sources['mitrade']
    assert source.http_capacity_source_fingerprint == previous.data_fingerprint
    assert source.authorization_fingerprint == previous.authorization_fingerprint
    assert native_text_parser_repair(source, previous.data_fingerprint)
    assert not native_text_parser_repair(source, source.data_fingerprint)
    queue = NewsQueue(tmp_path/'news', old)
    try:
        with queue.db:
            queue.add_articles([{'url': URL}])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
                "error_code='article_identity_not_verified',attempts=3,due=9000000000,last_checked_at_utc=? WHERE url=?",
                ((datetime.now(UTC)-timedelta(seconds=10)).isoformat(), URL))
            if obstruction == 'body': queue.db.execute("UPDATE articles SET body_sha256='protected body' WHERE url=?", (URL,))
            if obstruction == 'lease': queue.db.execute("UPDATE articles SET lease_owner='other-owner' WHERE url=?", (URL,))
            if obstruction in {'http_403', 'rights'}:
                code = 'http_403' if obstruction == 'http_403' else 'article_rights_or_scope_excluded'
                queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (code, code, URL))
            if obstruction in {'missing_clock', 'naive_clock', 'future_clock'}:
                clock = {'missing_clock': None, 'naive_clock': '2026-10-01T00:00:00',
                    'future_clock': (datetime.now(UTC)+timedelta(days=1)).isoformat()}[obstruction]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (clock, URL))
            if obstruction == 'different_predecessor':
                queue.db.execute("UPDATE queue_settings SET value='unknown source' WHERE key='source_capture_fingerprint:mitrade'")
            queue.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:mitrade:20261020','2')")
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    finally:
        queue.close()
    if obstruction == 'changed_authorization':
        current['providers'][0]['authorization']['scope'] += ' changed author scope'
        new = PolicyRegistry(current, base=tmp_path)
    queue = NewsQueue(tmp_path/'news', new)
    try:
        expected = {**before, 'state': 'retry'} if obstruction is None else before
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_identity_metadata_attempts:v1:mitrade:20261020'").fetchone()[0] == '2'
    finally:
        queue.close()
