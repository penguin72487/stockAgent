"""Native ETtoday anatomy, clocks, literal routing and original-value guards.

Synthetic regression fixtures reproduce observed markup, never claim to be
source articles. Actual original-byte replay has separate private receipts.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_ettoday import (
    ETTODAY_BODY_SELECTOR, ETTODAY_PATH_PATTERN, _native_json, ettoday_request_identity,
    ettoday_validation_metadata,
)


URLS = ('https://finance.ettoday.net/news/3254747', 'https://www.ettoday.net/news/20261013/3254110.htm')
TITLE = '原始數值：-12.5%、123456股與比率0.75'
PROSE = '記者查證原始-12.5%、123456股、0.75及引用中央社報導；數值不轉換、不是摘要或推薦新聞。' * 4
NOTICE = '本段風險提示與原始資料不得刪除；並非保證收益。'
RIGHTS = '※本文版權所有，非經授權，不得轉載。[ETtoday著作權聲明]※'
PUB = '2026-10-13T22:18:00+08:00'
MOD = '2026-10-13T22:25:02+08:00'


def recipe(parser='lxml'):
    return {'id': 'ettoday', 'name': 'ETtoday', 'domains': ['ettoday.net'], 'enabled': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-15',
            'evidence_url': 'https://member.ettoday.net/clause_copyright.php7',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'Exact locally known own-reporters; private noncommercial reading, no export/training grant'},
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'interval_seconds': 2, 'parallelism': 1, 'capacity_max_parallelism': 2,
        'html_parser': parser, 'embedded_article_layout': 'ettoday_visible_article_v1',
        'article_path_pattern': ETTODAY_PATH_PATTERN, 'article_selectors': [ETTODAY_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'feeds': [], 'sitemaps': [], 'wordpress_apis': [], 'fulltext_feeds': []}


def config(parser='lxml'):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    cfg.pop('provider_catalog', None)
    return {**cfg, 'providers': [recipe(parser)]}


def policy(parser='lxml'):
    return PolicyRegistry(config(parser), base=Path.cwd()).sources['ettoday']


def page(host='finance', parser='lxml'):
    url = URLS[host == 'www']
    names = ['研究記者'] if host == 'finance' else ['原始記者', '研究記者']
    author = ({'@type': 'Organization', 'name': 'ETtoday新聞雲', 'url': 'https://www.ettoday.net/'}
        if host == 'finance' else {'@type': 'Person', 'name': names[-1]})
    schema = {'@context': 'https://schema.org', '@type': 'NewsArticle', 'url': url,
        'mainEntityOfPage': url, 'headline': TITLE, 'datePublished': PUB, 'dateModified': MOD,
        'author': author, 'publisher': {'@type': 'Organization', 'name': ' ETtoday新聞雲'},
        'description': '摘要不得冒充完整正文'}
    suffix = ' | ETtoday財經雲 | ETtoday新聞雲' if host == 'finance' else ' | ETtoday社會新聞 | ETtoday新聞雲'
    pubprop = 'itemprop="dateCreated"' if host == 'finance' else 'itemprop="dateCreated" itemprop="datePublished"'
    head = '<html itemscope itemtype="https://schema.org/NewsArticle"><head><meta charset="utf-8">'
    head += f'<link rel="canonical" itemprop="mainEntityOfPage" href="{url}"><meta property="og:url" content="{url}">'
    head += f'<meta property="og:title" content="{TITLE + suffix}"><meta name="subsection" itemprop="articleSection" content="社會">'
    head += f'<meta name="pubdate" {pubprop} content="{PUB}"><meta name="lastmod" itemprop="dateModified" content="{MOD}">'
    head += '<script id="report" type="application/ld+json">' + json.dumps(schema, ensure_ascii=False, indent=2) + '</script></head><body>'
    time = ('<time class="date" datetime="31" itemprop="datePublished">2026-10-13 22:18</time>'
        if host == 'finance' else '<time class="date" datetime="' + PUB + '" itemprop="datePublished">2026年10月13日 22:18</time>')
    subject = '<div class="subject_article"><header><h1 class="title" itemprop="headline">' + TITLE + '</h1></header>'
    subject += '<div class="story" itemprop="articleBody"><p><img alt="alt不是正文" src="//cdn2.ettoday.net/images/1/test.jpg"></p>'
    subject += '<p><strong>▲原始圖片出處與數值123456股。</strong></p><p id="credit">記者' + '、'.join(names) + '／台北報導</p>'
    subject += '<p id="prose">' + PROSE + '</p><div class="ad_in_news"><div class="ad_readmore">[廣告]請繼續往下閱讀...</div><script>廣告不是正文</script></div>'
    subject += '<h2>原始小標題</h2><p>' + NOTICE + '</p></div><p class="note">' + RIGHTS + '</p></div>'
    if host == 'finance':
        document = '<div class="c1"><div class="breadcrumb_box">' + time + '</div>' + subject + '</div>'
    else:
        document = '<div class="part_area_1"><div class="part_breadcrumb_2">' + time + '</div><article>' + subject + '</article></div>'
    return BeautifulSoup(head + document + '<aside><time>2040-01-01</time><h1>鄰篇標題不能補值</h1>鄰篇不是正文</aside></body></html>', parser)


def extract(soup, host='finance', parser='lxml'):
    return extract_article(str(soup).encode(), policy(parser), page_url=URLS[host == 'www'])


def update_schema(soup, **changes):
    node = soup.select_one('script#report')
    value, _ = _native_json(node.get_text())
    value.update(changes)
    node.string = json.dumps(value, ensure_ascii=False, indent=2)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_native_entire_prose_values_own_ordered_reporters_clocks_and_source_notices(host, parser):
    soup = page(host, parser)
    before = str(soup)
    result = extract(soup, host, parser)
    assert PROSE in result['body'] and NOTICE in result['body'] and '原始小標題' in result['body']
    assert not any(word in result['body'] for word in ('摘要不得', '鄰篇', '廣告', 'alt不是正文', '圖片出處', RIGHTS))
    assert result['authors_json'] == json.dumps(['研究記者'] if host == 'finance' else ['原始記者', '研究記者'], ensure_ascii=False)
    assert result['published_at_raw'] == PUB and result['modified_at_raw'] == MOD
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    notices = json.loads(result['verified_article_notices_json'])
    assert any(n['kind'] == 'native_article_rights_notice' and n['text'] == RIGHTS for n in notices)
    assert bool([n for n in notices if n['kind'] == 'source_invalid_datetime_attribute_preserved']) == (host == 'finance')
    assert json.loads(result['verified_image_captions_json']) == ['▲原始圖片出處與數值123456股。']
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('change', ['canonical', 'og_url', 'og_title', 'headline', 'missing_body', 'second_story',
    'header_hidden', 'body_hidden', 'prose_hidden', 'credit_hidden', 'caption_hidden', 'unknown_inline_script',
    'unknown_ad_prose', 'foreign_byline', 'publisher_as_byline', 'no_byline', 'duplicate_byline',
    'duplicate_reporter_name', 'body_before_credit', 'schema_foreign_author', 'schema_foreign_publisher',
    'schema_wrong_url', 'schema_wrong_headline', 'schema_extra_article', 'schema_full_body', 'schema_paid',
    'schema_unknown_free', 'schema_bad_syntax', 'missing_pubdate', 'duplicate_pubdate', 'pub_bad_alias',
    'invalid_pubdate', 'naive_pubdate', 'wrong_offset', 'missing_modified', 'modification_epoch_placeholder',
    'revision_before_release', 'visible_minute', 'datetime_unreviewed', 'clock_hidden', 'clock_extra',
    'schema_pubdate_conflict', 'schema_modified_conflict', 'explicit_download_denial'])
def test_missing_hidden_conflicting_foreign_or_unreviewed_evidence_is_rejected(host, parser, change):
    soup = page(host, parser)
    if change in {'canonical', 'og_url'}:
        node = soup.select_one('link[rel=canonical]' if change == 'canonical' else 'meta[property="og:url"]')
        node['href' if change == 'canonical' else 'content'] = URLS[host == 'www'] + '0'
    elif change == 'og_title':
        soup.select_one('meta[property="og:title"]')['content'] = '鄰篇不是正文'
    elif change == 'headline':
        soup.select_one('.subject_article h1').string = '鄰篇不是正文'
    elif change == 'missing_body':
        soup.select_one('.story').decompose()
    elif change == 'second_story':
        soup.body.append(deepcopy(soup.select_one('.story')))
    elif change.endswith('_hidden'):
        selector = {'header_hidden': '.subject_article header', 'body_hidden': '.story',
            'prose_hidden': '#prose', 'credit_hidden': '#credit', 'caption_hidden': '.story strong',
            'clock_hidden': 'time[itemprop=datePublished]'}[change]
        soup.select_one(selector)['hidden'] = ''
    elif change == 'unknown_inline_script':
        node = soup.new_tag('script'); node.string = '不是已審閱廣告'; soup.select_one('#prose').append(node)
    elif change == 'unknown_ad_prose':
        node = soup.new_tag('p'); node.string = '不可刪除原始123456股'; soup.select_one('.ad_in_news').append(node)
    elif change in {'foreign_byline', 'publisher_as_byline', 'no_byline', 'duplicate_reporter_name', 'body_before_credit'}:
        node = soup.select_one('#credit')
        if change == 'foreign_byline': node.string = '中央社記者／台北報導'
        elif change == 'publisher_as_byline': node.string = 'ETtoday新聞雲'
        elif change == 'no_byline': node.decompose()
        elif change == 'duplicate_reporter_name': node.string = '記者研究記者、研究記者／台北報導'
        else: node.insert_before(BeautifulSoup('<p>真正正文先出現，不能借後方的署名</p>', parser).p)
    elif change == 'duplicate_byline':
        soup.select_one('#credit').insert_after(deepcopy(soup.select_one('#credit')))
    elif change.startswith('schema_'):
        field, value = {'schema_foreign_author': ('author', {'@type': 'Person', 'name': '他人'}),
            'schema_foreign_publisher': ('publisher', {'@type': 'Organization', 'name': '中央社'}),
            'schema_wrong_url': ('url', URLS[host == 'www'] + '0'), 'schema_wrong_headline': ('headline', '鄰篇'),
            'schema_full_body': ('articleBody', PROSE), 'schema_paid': ('isAccessibleForFree', False),
            'schema_unknown_free': ('isAccessibleForFree', 'unknown'), 'schema_pubdate_conflict': ('datePublished', MOD),
            'schema_modified_conflict': ('dateModified', PUB)}.get(change, (None, None))
        if field: update_schema(soup, **{field: value})
        elif change == 'schema_extra_article': soup.head.append(deepcopy(soup.select_one('#report')))
        else: soup.select_one('#report').string = '{invalid JSON cannot be silently ignored}'
    elif change in {'missing_pubdate', 'duplicate_pubdate', 'pub_bad_alias', 'invalid_pubdate', 'naive_pubdate', 'wrong_offset'}:
        node = soup.select_one('meta[name=pubdate]')
        if change == 'missing_pubdate': node.decompose()
        elif change == 'duplicate_pubdate': node.insert_after(deepcopy(node))
        elif change == 'pub_bad_alias': node['itemprop'] = 'dateOfUnrelatedNews'
        else: node['content'] = {'invalid_pubdate': '2026-99-99T22:18:00+08:00',
            'naive_pubdate': '2026-10-13T22:18:00', 'wrong_offset': '2026-10-13T22:18:00+00:00'}[change]
    elif change in {'missing_modified', 'modification_epoch_placeholder', 'revision_before_release'}:
        node = soup.select_one('meta[name=lastmod]')
        if change == 'missing_modified': node.decompose()
        else: node['content'] = '1970-01-01T08:00:00+08:00' if change == 'modification_epoch_placeholder' else '2026-10-12T22:18:00+08:00'
    elif change in {'visible_minute', 'datetime_unreviewed', 'clock_extra'}:
        node = soup.select_one('time[itemprop=datePublished]')
        if change == 'visible_minute': node.string = '2026-10-13 22:19' if host == 'finance' else '2026年10月13日 22:19'
        elif change == 'datetime_unreviewed': node['datetime'] = '32'
        else: node.insert_after(deepcopy(node))
    else:
        soup.select_one('p.note').string = '禁止下載與抓取'
    with pytest.raises(NewsError):
        extract(soup, host, parser)


@pytest.mark.parametrize('defect', ['description_lf', 'description_crlf', 'exact_author_comment'])
def test_lossless_observed_json_syntax_normalization_not_new_source_values(defect):
    value = {'@type': 'NewsArticle', 'author': {'@type': 'Person', 'name': '研究記者'},
        'description': '摘要\r\n\r\n' if defect == 'description_crlf' else '摘要\n\n', 'headline': TITLE}
    text = json.dumps(value, ensure_ascii=False, indent=2)
    if defect.startswith('description_'):
        text = text.replace('\\r\\n', '\r\n').replace('\\n', '\n')
    else:
        text = text.replace('"name": "研究記者"', '"name": "研究記者"\n//如果有筆名，才會有url')
    parsed, anomalies = _native_json(text)
    assert parsed == value and len(anomalies) == 1


@pytest.mark.parametrize('text', ['{"headline":"壞\n標題"}', '{"description":"壞\x00字元"}',
    '{"name":"研究記者"\n//其他註解不能刪除\n}', '{"name":"研究記者" /*不能eval*/}',
    '{"description":"缺少引號}', '{"description":"abc",}', '{"description":"abc"}eval(1)'])
def test_unreviewed_syntax_or_controls_cannot_be_forgiven(text):
    with pytest.raises(ValueError):
        _native_json(text)


@pytest.mark.parametrize('url', ['https://house.ettoday.net/news/3254747',
    URLS[0] + '?foo=1', 'https://finance.ettoday.net/news/03254747', 'https://www.ettoday.net/news/3254747',
    'https://www.ettoday.net/news/20261013/3254110.htm/extra'])
def test_unknown_host_route_or_query_is_rejected_before_any_http(url):
    calls = []
    http = SimpleNamespace(registry=PolicyRegistry(config(), base=Path.cwd()),
        get=lambda *a, **kw: calls.append(a))
    with pytest.raises(NewsError, match='unsupported_article_path'):
        fetch_article(http, {'url': url, 'id': b'1' * 16})
    assert not calls and ettoday_request_identity(url, requested=True) is None


@pytest.mark.parametrize('final', [URLS[0] + '0', URLS[1]])
def test_actual_redirect_cannot_replace_article_or_host(final):
    http = SimpleNamespace(registry=PolicyRegistry(config(), base=Path.cwd()),
        get=lambda *a, **kw: Response(final, 200, {'content-type': 'text/html'}, str(page()).encode()))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': URLS[0], 'id': b'1' * 16})


def test_only_actual_same_host_path_https_upgrade_preserves_literal_request():
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return Response(URLS[0], 200, {'content-type': 'text/html'}, str(page()).encode())
    http = SimpleNamespace(registry=PolicyRegistry(config(), base=Path.cwd()), get=get)
    requested = URLS[0].replace('https:', 'http:')
    result = fetch_article(http, {'url': requested, 'id': b'1' * 16})
    assert calls == [requested] and result['requested_url'] == requested and result['final_url'] == URLS[0]
    assert result['url_resolution_basis'] == 'verified_ettoday_same_host_path_source_redirect_v1; original_frontier_url_retained'
    assert result['raw_response_body'] and result['published_at_raw'] == PUB


@pytest.mark.parametrize('change', ['domain', 'evidence', 'discovery', 'scope', 'selectors', 'container', 'authors', 'path', 'purpose', 'export'])
def test_native_parser_does_not_grant_other_source_or_discovery_scope(change):
    cfg = config()
    entry = cfg['providers'][0]
    if change == 'domain': entry['domains'] = ['other.invalid']
    elif change == 'evidence': entry['authorization']['evidence_url'] = 'https://other.invalid/'
    elif change == 'discovery': entry['feeds'] = ['https://www.ettoday.net/feed']
    elif change == 'scope': entry['private_known_url_scope'] = ''
    elif change == 'selectors': entry['article_selectors'] = ['body']
    elif change == 'container': entry['require_configured_container'] = False
    elif change == 'authors': entry['require_author_attribution'] = False
    elif change == 'path': entry['article_path_pattern'] = '.*'
    elif change == 'purpose': cfg['collection_purpose'] = 'commercial'
    else: cfg['remote_training_export'] = True
    if change == 'purpose':
        assert not PolicyRegistry(cfg, base=Path.cwd()).sources['ettoday'].allowed
    else:
        with pytest.raises(ValueError): PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_native_failure_metadata_has_shapes_not_prose_names_or_capture_grants(host, parser):
    soup = page(host, parser)
    facts = ettoday_validation_metadata(soup, page_url=URLS[host == 'www'])
    assert facts['native_story_count'] == 1 and facts['native_release_node_count'] == 1
    assert facts['declared_publication'] == [PUB] and facts['declared_modification'] == [MOD]
    assert facts['native_credit_shapes'][0]['native_reporter_signature_shape']
    assert facts['native_credit_shapes'][0]['reporter_count'] == (1 if host == 'finance' else 2)
    text = json.dumps(facts, ensure_ascii=False)
    assert not any(value in text for value in (TITLE, PROSE, '研究記者', '原始記者', RIGHTS))
    assert not facts['body_text_retained'] and not facts['raw_scope_or_article_acceptance_granted']
    assert len(text) < 4096


def test_bad_source_clock_and_schema_are_diagnostic_not_silently_repaired():
    soup = page()
    soup.select_one('meta[name=lastmod]')['content'] = '1970-01-01T08:00:00+08:00'
    soup.select_one('#report').string = '{invalid JSON}'
    facts = ettoday_validation_metadata(soup, page_url=URLS[0])
    assert facts['schema_error'] and facts['declared_modification'] == ['1970-01-01T08:00:00+08:00']
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract(soup)


def test_unknown_route_does_not_export_metadata_or_authority():
    facts = ettoday_validation_metadata(page(), page_url='https://house.ettoday.net/news/3254747')
    assert facts == {'contract_version': 1, 'literal_route_verified': False,
        'body_text_retained': False, 'raw_scope_or_article_acceptance_granted': False}
