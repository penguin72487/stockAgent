"""Literal native Videoland originals, captions, clocks and private scope."""
from copy import deepcopy
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, native_text_parser_repair, utc_now
from downloader.news_fulltext_videoland import (
    VIDEOLAND_BODY_SELECTOR, VIDEOLAND_PATH_PATTERN, VIDEOLAND_POLICY_EVIDENCE_URL, videoland_article_id,
    videoland_validation_metadata, videoland_description_repair_metadata, _decode_schema,
)
from scripts.probe_news_sources import videoland_candidate_source, probe_provider


URL = 'https://news.videoland.com.tw/article/12345678-1234-1234-1234-123456789abc.html'
TITLE = '合成測試新聞：金額與單位'
CREDIT = '記者測試員／台北報導'
PROSE = '【緯來新聞網】' + '這是合成測試正文，保留原始營收123.45億元、成交量987股與負值-2.5。' * 5
CAPTION = '合成圖說，必須保留；不可當作已下載圖片。'
RELEASE = '2026-10-16T17:53:00+08:00'
REVISION = '2026-10-16T18:00:00+08:00'
COPYRIGHT = '未經同意禁止任何形式之轉載 © COPYRIGHT VIDEOLAND INC. ALL RIGHTS RESERVED.'


def entry():
    return {
        'id': 'videoland', 'name': 'synthetic test', 'domains': ['news.videoland.com.tw'],
        'enabled': True, 'interval_seconds': 10, 'parallelism': 1,
        'article_path_pattern': VIDEOLAND_PATH_PATTERN, 'article_selectors': [VIDEOLAND_BODY_SELECTOR],
        'embedded_article_layout': 'videoland_visible_article_v1',
        'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'acquisition_mode': 'continuous', 'title_selector': 'h1', 'exclude_selectors': [],
        'require_author_attribution': True, 'require_configured_container': True,
        'authorization': {'kind': 'personal_research', 'reviewed_on': '2026-10-21',
            'evidence_url': VIDEOLAND_POLICY_EVIDENCE_URL,
            'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'Synthetic private native-original test; no network or media requests.'},
    }


def config(source=None):
    settings, _, _ = load_config(Path('configs/news_fulltext.json'))
    settings = deepcopy(settings)
    settings['providers'] = [entry() if source is None else source]
    return settings


def versioned_config(version):
    source = entry()
    source['embedded_article_layout'] = f'videoland_visible_article_v{version}'
    return config(source)


def page():
    schema = {'@context': 'https://schema.org', '@type': 'NewsArticle',
        'url': URL, 'mainEntityOfPage': {'@type': 'WebPage', '@id': URL},
        'headline': TITLE, 'isAccessibleForFree': True,
        'author': {'@type': 'Person', 'name': CREDIT},
        'publisher': {'@type': 'Organization', 'name': '緯來新聞網'},
        'datePublished': RELEASE, 'dateModified': REVISION, 'articleBody': PROSE}
    return f'''<html><head><title>{TITLE}｜緯來新聞網</title>
        <link rel="canonical" href="{URL}">
        <meta property="og:url" content="{URL}"><meta property="og:title" content="{TITLE}">
        <meta property="og:site_name" content="緯來新聞網"><meta property="og:type" content="article">
        <meta property="article:published_time" content="{RELEASE}">
        <meta property="article:modified_time" content="{REVISION}">
        <script type="application/ld+json">{json.dumps(schema, ensure_ascii=False)}</script></head>
        <body><div id="root"><main><div>
        <header><h1><span id="NewsTitle">{TITLE}</span></h1>
        <span>發佈時間:<span id="NewsRealDate">2026/10/16 17:53</span>
        更新時間:<span id="NewsModDate">2026/10/16 18:00</span></span>
        <a id="author" href="https://news.videoland.com.tw/search?q={CREDIT}">{CREDIT}</a></header>
        <article><div class="news-content"><span id="NewsBody"><p>{PROSE}</p>
        <div><img src="https://unrequested.invalid/image.jpg"><figcaption>{CAPTION}</figcaption></div>
        </span></div></article><aside>不應擷取的推薦文章</aside></div></main>
        <footer><p>{COPYRIGHT}</p></footer></div></body></html>'''.encode()


def changed(transform):
    soup = BeautifulSoup(page(), 'lxml')
    transform(soup)
    return str(soup).encode()


def change_schema(soup, **fields):
    script = soup.find('script', type='application/ld+json')
    data = json.loads(script.get_text())
    data.update(fields)
    script.string = json.dumps(data, ensure_ascii=False)


def omit_schema_body(soup, **fields):
    script = soup.find('script', type='application/ld+json')
    data = json.loads(script.get_text())
    data.update(fields)
    del data['articleBody']
    data['description'] = '合成摘要不是正文；不得用它取代完整原生文章。'
    script.string = json.dumps(data, ensure_ascii=False)


def literal_lf_schema(schema):
    """Synthetic publisher defect; do not damage escaped backslash-n values."""
    description = schema['description']
    literal = '"' + '\n'.join(json.dumps(part, ensure_ascii=False)[1:-1]
                             for part in description.split('\n')) + '"'
    return json.dumps(schema, ensure_ascii=False).replace(
        json.dumps(description, ensure_ascii=False), literal, 1)


def description_defect_page(transform=None, *, absent_body=False):
    soup = BeautifulSoup(page(), 'lxml')
    if transform:
        transform(soup)
        soup = BeautifulSoup(str(soup), 'lxml')  # Restore a parsed Script node after synthetic mutation.
    script = soup.find('script', type='application/ld+json')
    schema = json.loads(script.get_text())
    schema['description'] = '合成摘要，保留引號"及字面\\n\n第二行不是正文。'
    if absent_body:
        schema.pop('articleBody')
    script.string = literal_lf_schema(schema)
    return str(soup).encode()


@pytest.mark.parametrize('lf_count', [1, 3, 16])
@pytest.mark.parametrize('escaped_key', [False, True])
def test_description_LF_syntax_repair_preserves_all_decoded_values(lf_count, escaped_key):
    expected = {'@type': 'NewsArticle', 'description': '\n'.join(
        ['引號"、反斜線\\、字面\\n、負值-2.5、股數987'] * (lf_count+1)),
        'headline': TITLE, 'author': {'@type': 'Person', 'name': CREDIT},
        'other': {'unknown_metadata_retained': ['\t', '\r', 0, -2.5, None, False]},
        'datePublished': RELEASE, 'url': URL}
    raw = literal_lf_schema(expected)
    if escaped_key:
        raw = raw.replace('"description"', '"desc\\u0072iption"', 1)
    with pytest.raises(json.JSONDecodeError):
        _decode_schema(raw)
    decoded, proof = _decode_schema(raw, allow_description_lf=True)
    assert decoded == expected
    assert proof['candidate_decoded'] and proof['description_value_preserved']
    assert proof['literal_lf_count'] == lf_count
    assert proof['only_root_description_syntax_changed']
    assert not proof['raw_HTML_modified'] and not proof['accepted_as_article']
    assert all(raw[pos] == '\n' for pos in proof['script_character_positions'])
    assert all(value not in json.dumps(proof, ensure_ascii=False) for value in (TITLE, CREDIT, URL))


@pytest.mark.parametrize('raw', [
    '{"@type":"NewsArticle","description":"first\nsecond\tthird"}',
    '{"@type":"NewsArticle","description":"first\r\nsecond"}',
    '{"@type":"NewsArticle","description":"first\nsecond\u0000third"}',
    '{"@type":"NewsArticle","description":"first\nsecond\\q"}',
    '{"@type":"NewsArticle","description":"first\nsecond","headline":"bad\nfield"}',
    '{"@type":"NewsArticle","description":"first\nsecond","other":NaN}',
    '{"@type":"NewsArticle","description":"first\nsecond","other":Infinity}',
    '{"@type":"NewsArticle","description":"first\nsecond","other":1,"other":2}',
    '{"@type":"NewsArticle","description":"first\nsecond","@type":"NewsArticle"}',
    '{"@type":"NewsArticle","description":"first\nsecond","description":"other"}',
    '{"@type":"NewsArticle","description":"first\nsecond",}',
    '{"@type":"NewsArticle","description":"first\nsecond"}{}',
    '{"description":"first\nsecond","@type":"NewsArticle"}',
    '{"@type":"Organization","description":"first\nsecond"}',
    '[{"@type":"NewsArticle","description":"first\nsecond"}]',
    '{"@type":"NewsArticle","other":{"@type":"NewsArticle","description":"first\nsecond"}}',
    '{"@type":"NewsArticle","description":["first\nsecond"]}',
    '{"@type":"NewsArticle","description\n":"first"}',
    '{"@type":"NewsArticle","headline":"first\nsecond"}',
    '{"@type":"NewsArticle","articleBody":"first\nsecond"}',
    '{"@type":"NewsArticle","author":{"name":"first\nsecond"}}',
    '{"@type":"NewsArticle","url":"first\nsecond"}',
    '{"@type":"NewsArticle","datePublished":"first\nsecond"}',
    '{"@type":"NewsArticle","unknown":"first\nsecond"}',
    literal_lf_schema({'@type': 'NewsArticle', 'description': '\n'.join(['part'] * 18)}),
])
def test_description_repair_cannot_repair_other_fields_or_invalid_remainder(raw):
    with pytest.raises(json.JSONDecodeError):
        _decode_schema(raw, allow_description_lf=True)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('absent_body', [False, True])
def test_v4_LF_repair_keeps_native_article_and_all_other_gates(tmp_path, engine, absent_body):
    settings = versioned_config(3)
    current = replace(PolicyRegistry(settings, base=tmp_path).sources['videoland'], html_parser=engine)
    candidate = replace(videoland_candidate_source(settings, PolicyRegistry(settings, base=tmp_path),
        tmp_path, 'videoland_visible_article_v4'), html_parser=engine)
    raw = description_defect_page(absent_body=absent_body)
    original_raw = bytes(raw)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(raw, current, page_url=URL)
    result = extract_article(raw, candidate, page_url=URL)
    oracle = extract_article(page(), candidate, page_url=URL)
    for field in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw',
                  'historical_point_in_time'):
        assert result[field] == oracle[field]
    assert raw == original_raw
    assert CAPTION in result['body'] and '第二行不是正文' not in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    repair = next(item for item in notices if item['kind'] == 'schema_description_literal_LF_syntax_repair')
    assert repair['literal_lf_count'] == 1 and repair['description_value_preserved']
    assert repair['syntax_repair_does_not_grant_attribution_or_rights']


@pytest.mark.parametrize('transform,code', [
    (lambda s: s.select_one('#author').decompose(), 'article_attribution_not_verified'),
    (lambda s: change_schema(s, author={'@type': 'Organization', 'name': '緯來新聞網'}), 'article_attribution_not_verified'),
    (lambda s: change_schema(s, isAccessibleForFree=False), 'paywall_or_login'),
    (lambda s: change_schema(s, articleBody='只是摘要'), 'body_not_found'),
    (lambda s: change_schema(s, headline='其他標題'), 'article_identity_not_verified'),
    (lambda s: s.select_one('#NewsModDate').string.replace_with('2026/10/17 18:00'), 'invalid_bulk_publication_clock'),
    (lambda s: s.select_one('footer p').decompose(), 'article_rights_not_verified'),
])
def test_LF_syntax_repair_never_grants_author_body_clock_or_rights(tmp_path, transform, code):
    source = PolicyRegistry(versioned_config(4), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match=code):
        extract_article(description_defect_page(transform), source, page_url=URL)


@pytest.mark.parametrize('author_type', ['Person', 'Organization', [], {}])
def test_repaired_schema_preview_is_bounded_private_evidence_not_attribution(tmp_path, author_type):
    def transform(soup):
        change_schema(soup, author={'@type': author_type, 'name': '緯來新聞網'})
        soup.select_one('#author').name = 'span'
        soup.select_one('#author').attrs.pop('href')
        soup.select_one('#author').string = '緯來新聞網'
    raw = description_defect_page(transform, absent_body=True)
    facts = videoland_description_repair_metadata(BeautifulSoup(raw, 'lxml'), page_url=URL)
    assert facts['candidate_schema_decoded'] and facts['schema_author_is_single_object']
    assert facts['schema_author_type'] == (author_type if isinstance(author_type, str) else 'other')
    assert facts['schema_author_is_publisher_organization'] == (author_type == 'Organization')
    assert facts['schema_body_field_present'] is False
    assert facts['native_title_matches_schema'] and facts['native_release_update_clock_check'] == 'matches'
    assert facts['native_header_name_matches_count'] == 1
    assert facts['native_header_name_matches'] == [{'tag': 'span', 'id_kind': 'author',
        'visible': True, 'has_href': False, 'native_credit_search_query_agrees': False}]
    assert not facts['accepted_as_article'] and not facts['raw_JSON_or_names_or_body_retained']
    assert all(value not in json.dumps(facts, ensure_ascii=False) for value in (TITLE, CREDIT, PROSE, CAPTION, URL, '緯來新聞網'))
    source = PolicyRegistry(versioned_config(4), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(raw, source, page_url=URL)


def test_candidate_uses_one_envelope_with_unchanged_auth_and_capacity(tmp_path):
    settings = versioned_config(3)
    registry = PolicyRegistry(settings, base=tmp_path)
    prior = registry.sources['videoland']
    candidate = videoland_candidate_source(settings, registry, tmp_path, 'videoland_visible_article_v4')
    assert candidate.parser_repair_prior_capture_fingerprint == prior.data_fingerprint
    assert candidate.authorization_fingerprint == prior.authorization_fingerprint
    assert candidate.http_capacity_source_fingerprint == prior.http_capacity_source_fingerprint
    assert settings['providers'][0]['embedded_article_layout'] == 'videoland_visible_article_v3'
    calls = []
    def get(url):
        calls.append(url)
        return Response(url, 200, {'content-type': 'text/html'}, description_defect_page())
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={}, get=get)
    item, = probe_provider(http, [URL], candidate_source=candidate)
    assert calls == [URL] and item['extraction_error'] == 'article_identity_not_verified'
    candidate_facts = item['candidate_parser']
    assert candidate_facts['parse_success'] and candidate_facts['published_at_raw'] == RELEASE
    assert 'authors_json' not in candidate_facts
    assert item['videoland_description_repair_preview']['candidate_schema_decoded']
    assert all(not candidate_facts[key] for key in ('accepted_as_article', 'raw_response_retained',
        'body_text_exported', 'production_queue_writes', 'additional_publisher_requests'))
    assert all(value not in json.dumps(item, ensure_ascii=False) for value in (TITLE, CREDIT, PROSE, CAPTION))
    with pytest.raises(ValueError, match='metadata_only_same_source_candidate_required'):
        probe_provider(http, [URL], candidate_source=candidate, retain=True)
    assert calls == [URL]


@pytest.mark.parametrize('version,layout', [(2, 'videoland_visible_article_v4'),
    (4, 'videoland_visible_article_v4'), (3, 'videoland_visible_article_v3'),
    (3, 'videoland_visible_article_v5'), (3, 'ettoday_visible_article_v39')])
def test_candidate_requires_exact_reviewed_capture_predecessor(tmp_path, version, layout):
    settings = versioned_config(version)
    with pytest.raises(ValueError, match='reviewed_videoland_v3_predecessor_candidate_required'):
        videoland_candidate_source(settings, PolicyRegistry(settings, base=tmp_path), tmp_path, layout)


def test_candidate_cannot_change_source_scope_or_request_identity(tmp_path):
    settings = versioned_config(3)
    registry = PolicyRegistry(settings, base=tmp_path)
    settings['providers'][0]['authorization']['scope'] += '; changed authorization'
    with pytest.raises(ValueError, match='candidate_must_preserve_source_auth_and_http_identity'):
        videoland_candidate_source(settings, registry, tmp_path, 'videoland_visible_article_v4')


@pytest.mark.parametrize('author_list', [False, True])
def test_bounded_metadata_diagnoses_credit_container_without_retaining_text(author_list):
    soup = BeautifulSoup(page(), 'lxml')
    if author_list:
        change_schema(soup, author=[{'@type': 'Person', 'name': CREDIT}])
        soup = BeautifulSoup(str(soup), 'lxml')  # Parse a received script node, not a synthetic generic string.
    facts = videoland_validation_metadata(soup, page_url=URL)
    assert facts['declared_author_container'] == ('list' if author_list else 'object')
    assert facts['single_declared_author'] and facts['schema_author_name_equals_native_credit']
    assert facts['native_byline_href_shape'] == 'exact_native_credit_search'
    assert facts['native_prose_without_captions_equals_schema'] and facts['native_caption_count'] == 1
    assert facts['body_text_retained'] is False and facts['accepted_as_article'] is False
    encoded = json.dumps(facts, ensure_ascii=False)
    assert all(value not in encoded for value in (TITLE, CREDIT, PROSE, CAPTION, URL, 'unrequested.invalid'))


@pytest.mark.parametrize('schema_name,query,agrees', [
    ('測試員', '測試員', True), ('其他人', '其他人', False),
    ('測試員', '其他人', False), ('其他人', '測試員', False),
])
def test_metadata_distinguishes_reporter_name_representation_from_conflict(schema_name, query, agrees):
    def transform(soup):
        change_schema(soup, author={'@type': 'Person', 'name': schema_name})
        soup.select_one('#author')['href'] = 'https://news.videoland.com.tw/search?q=' + query
    facts = videoland_validation_metadata(BeautifulSoup(changed(transform), 'lxml'), page_url=URL)
    assert facts['contract_version'] == 5
    assert facts['schema_author_name_equals_native_reporter_component'] == (schema_name == '測試員')
    assert facts['native_byline_same_origin_search_query_matches_reporter_component'] == (query == '測試員')
    assert (facts['schema_author_name_equals_native_reporter_component']
            and facts['native_byline_same_origin_search_query_matches_reporter_component']) == agrees
    assert facts['accepted_as_article'] is False
    assert all(value not in json.dumps(facts, ensure_ascii=False) for value in
               (schema_name, query, CREDIT, TITLE, PROSE, CAPTION))


@pytest.mark.parametrize('href', [
    'https://news.videoland.com.tw/search?q=測試員&q=測試員',
    'https://news.videoland.com.tw/search?q=測試員&other=測試員',
    'https://news.videoland.com.tw/search?q=測試員#測試員',
    'https://news.videoland.com.tw.invalid/search?q=測試員',
    'https://user:secret@news.videoland.com.tw/search?q=測試員',
    '/search?q=測試員',
    'https://news.videoland.com.tw/search?q=測試員&other=',
    'https://news.videoland.com.tw/search?q=測試員\n',
])
def test_metadata_reporter_component_does_not_infer_a_query_or_origin(href):
    soup = BeautifulSoup(changed(lambda s: s.select_one('#author').__setitem__('href', href)), 'lxml')
    facts = videoland_validation_metadata(soup, page_url=URL)
    assert facts['native_byline_same_origin_search_query_matches_reporter_component'] is False
    assert facts['native_byline_same_origin_search_query_matches_schema_name'] is False
    assert facts['author_names_or_hrefs_retained'] is False


@pytest.mark.parametrize('native_markup,contiguous_matches', [
    ('<span>記者</span><span>測試員</span><span>／台北報導</span>', True),
    ('記者 測試員 ／ 台北報導', False),
])
@pytest.mark.parametrize('schema_name,query', [('測試員', '測試員'), ('其他人', '其他人')])
def test_metadata_separates_joined_node_spaces_from_actual_native_spaces(
        tmp_path, native_markup, contiguous_matches, schema_name, query):
    def transform(soup):
        byline = soup.select_one('#author')
        byline.clear()
        fragment = BeautifulSoup(native_markup, 'html.parser')
        for node in list(fragment.contents):
            byline.append(node.extract())
        byline['href'] = 'https://news.videoland.com.tw/search?q=' + query
        change_schema(soup, author={'@type': 'Person', 'name': schema_name})
    html = changed(transform)
    facts = videoland_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    assert facts['native_credit_is_reporter_signature'] is False
    assert facts['native_contiguous_credit_is_reporter_signature'] == contiguous_matches
    assert facts['native_compact_credit_is_reporter_signature'] is True
    assert facts['native_byline_contiguous_equals_joined_text'] == (not contiguous_matches)
    assert facts['schema_name_matches_contiguous_reporter_component'] == (
        contiguous_matches and schema_name == '測試員')
    assert facts['search_query_matches_contiguous_reporter_component'] == (
        contiguous_matches and query == '測試員')
    assert facts['schema_name_matches_compact_reporter_component'] == (schema_name == '測試員')
    assert facts['search_query_matches_compact_reporter_component'] == (query == '測試員')
    # A diagnostic equality must not silently broaden the current capture ABI.
    source = PolicyRegistry(versioned_config(2), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(html, source, page_url=URL)
    assert facts['accepted_as_article'] is False
    assert all(value not in json.dumps(facts, ensure_ascii=False) for value in
               (schema_name, query, CREDIT, TITLE, PROSE, CAPTION, native_markup))


@pytest.mark.parametrize('mutation', [None, 'native_title', 'canonical', 'schema_url', 'og_url',
    'main_entity', 'document_title', 'schema_title', 'free_access'])
def test_metadata_identity_equalities_are_diagnostic_only(mutation):
    def transform(soup):
        if mutation == 'native_title':
            soup.select_one('#NewsTitle').string = '合成測試新聞： 金額與單位'
        elif mutation == 'canonical':
            soup.select_one('link[rel=canonical]')['href'] = URL + '?different=1'
        elif mutation == 'schema_url':
            change_schema(soup, url=URL + '?different=1')
        elif mutation == 'og_url':
            soup.select_one('meta[property="og:url"]')['content'] = URL + '?different=1'
        elif mutation == 'main_entity':
            change_schema(soup, mainEntityOfPage={'@type': 'WebPage', '@id': URL + '?different=1'})
        elif mutation == 'document_title':
            soup.title.string = TITLE
        elif mutation == 'schema_title':
            change_schema(soup, headline='不同的合成標題')
        elif mutation == 'free_access':
            change_schema(soup, isAccessibleForFree=False)
    facts = videoland_validation_metadata(BeautifulSoup(changed(transform), 'lxml'), page_url=URL)
    identity = facts['native_identity_comparisons']
    assert identity['canonical_count'] == identity['native_title_count'] == 1
    assert identity['canonical_url_agrees'] == (mutation != 'canonical')
    assert identity['og_url_agrees'] == (mutation != 'og_url')
    assert identity['schema_url_agrees'] == (mutation != 'schema_url')
    assert identity['schema_main_entity_agrees'] == (mutation != 'main_entity')
    assert identity['native_title_equals_og'] == (mutation != 'native_title')
    assert identity['native_title_equals_schema'] == (mutation not in {'native_title', 'schema_title'})
    assert identity['native_title_compact_equals_og'] is True
    assert identity['native_title_compact_equals_schema'] == (mutation != 'schema_title')
    assert identity['native_title_plus_brand_equals_document_title'] == (
        mutation not in {'native_title', 'document_title'})
    assert identity['free_news_article_as_declared'] == (mutation != 'free_access')
    assert facts['native_body_active_tags'] == {}
    assert facts['accepted_as_article'] is False
    assert all(value not in json.dumps(facts, ensure_ascii=False) for value in
               (CREDIT, TITLE, PROSE, CAPTION, URL, '不同的合成標題'))


def test_metadata_active_body_structure_retains_only_fixed_tag_counts():
    def transform(soup):
        body = soup.select_one('#NewsBody')
        active = BeautifulSoup('<iframe src="https://never-request.invalid/private"></iframe>'
            '<script>const privateText = "不得保存的診斷文字";</script><svg><text>不得保存</text></svg>', 'html.parser')
        for node in list(active.contents):
            body.append(node.extract())
    facts = videoland_validation_metadata(BeautifulSoup(changed(transform), 'lxml'), page_url=URL)
    assert facts['native_body_active_tags'] == {'iframe': 1, 'script': 1, 'svg': 1}
    assert facts['body_text_retained'] is False and facts['accepted_as_article'] is False
    assert all(value not in json.dumps(facts, ensure_ascii=False) for value in
               ('never-request.invalid', 'privateText', '不得保存', CREDIT, TITLE, PROSE, CAPTION))


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('raw,kind,control', [
    ('{"private":"未保存的私有名字\n未保存的內文"}', 'unescaped_control_character', 10),
    ('{"private":"未保存的私有名字\t未保存的內文"}', 'unescaped_control_character', 9),
    ('{"private":"未保存的私有名字', 'unterminated_string', None),
    ('{"未保存的私有名字" 1}', 'expected_colon', None),
    ('{"private":1 "未保存的私有名字":2}', 'expected_comma', None),
    ('{"private": 未保存的私有名字}', 'expected_value', None),
    ('{未保存的私有名字:1}', 'expected_quoted_property', None),
    ('{"private":1} 未保存的私有名字', 'extra_data', None),
    ('{"private":"\\q未保存的私有名字"}', 'invalid_escape', None),
    ('{"private":"\\uXXXX未保存的私有名字"}', 'invalid_unicode_escape', None),
])
def test_json_diagnostic_classifies_syntax_without_retaining_source_or_repairing(tmp_path, engine, raw, kind, control):
    html = changed(lambda soup: setattr(soup.find('script', type='application/ld+json'), 'string', raw))
    facts = videoland_validation_metadata(BeautifulSoup(html, engine), page_url=URL)
    process = facts['schema_processing']
    error = process['first_error']
    assert facts['contract_version'] == 5 and facts['schema_bounded_or_json_error']
    assert process['contract_version'] == 2
    assert process['scripts_seen'] == process['scripts_examined'] == 1
    assert process['scripts_parsed'] == 0 and process['objects_examined'] == 0
    assert process['complete'] is process['syntax_repair_attempted'] is False
    assert process['raw_json_or_exception_message_retained'] is False
    assert error['kind'] == kind and error['control_codepoint'] == control
    with pytest.raises(json.JSONDecodeError) as caught:
        json.loads(raw)
    assert (error['line'], error['column'], error['position']) == (
        caught.value.lineno, caught.value.colno, caught.value.pos)
    assert error['characters'] == len(raw) and error['script_index'] == 0
    encoded = json.dumps(facts, ensure_ascii=False)
    assert all(value not in encoded for value in ('未保存的私有名字', '未保存的內文', TITLE, CREDIT, PROSE, CAPTION, URL))
    source = PolicyRegistry(versioned_config(3), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(html, source, page_url=URL)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('field', ['description', 'articleBody', 'headline', 'name',
    'datePublished', 'dateModified', 'url', '@id', '@type', '不得保存的私有欄位'])
def test_control_context_reports_only_fixed_field_enum_not_a_repaired_declaration(engine, field):
    key = json.dumps(field, ensure_ascii=True)
    raw = '{"@type":"NewsArticle",' + key + ':"不得保存的值\n後續原文"}'
    html = changed(lambda soup: setattr(soup.find('script', type='application/ld+json'), 'string', raw))
    facts = videoland_validation_metadata(BeautifulSoup(html, engine), page_url=URL)
    process = facts['schema_processing']
    context = process['first_error']['string_context']
    assert context['contract_version'] == 1 and context['proved']
    assert context['field'] == ('other' if field == '不得保存的私有欄位' else field)
    assert context['role'] == 'object_value' and context['nesting_depth'] == 1
    assert context['owning_object_newsarticle_type_in_valid_prefix'] is True
    assert not facts['schema_count'] and not facts['accepted_as_article']
    assert process['scripts_parsed'] == 0 and process['syntax_repair_attempted'] is False
    assert all(value not in json.dumps(facts, ensure_ascii=False) for value in
               ('不得保存的私有欄位', '不得保存的值', '後續原文', raw, TITLE, CREDIT, URL))


@pytest.mark.parametrize('raw,role,field,owner,depth', [
    ('{"@graph":[{"@type":"NewsArticle","description":"a\nb"}]}', 'object_value', 'description', True, 3),
    ('{"@type":"NewsArticle","author":{"@type":"Person","name":"a\nb"}}', 'object_value', 'name', False, 2),
    ('{"@type":"NewsArticle","author":["a\nb"]}', 'array_value', None, False, 2),
    ('{"@type":"NewsArticle","bad\nkey":1}', 'object_key', None, True, 1),
    ('"a\nb"', 'root_value', None, False, 0),
    ('[{"description":"a\nb"}]', 'object_value', 'description', False, 2),
    ('{"@type":"NewsArticle","@type":"Thing","description":"a\nb"}', 'object_value', 'description', False, 1),
    ('{"@type":["NewsArticle"],"description":"a\nb"}', 'object_value', 'description', False, 1),
    ('{"description":"a\nb","@type":"NewsArticle"}', 'object_value', 'description', False, 1),
    ('{"prior":[1,-2.5,true,false,null,{},[]],"description":"a\nb"}', 'object_value', 'description', False, 1),
    (' \n {"prior":"escaped \\n \\t \\\\ \\" quote","description":"a\nb"}', 'object_value', 'description', False, 1),
])
def test_control_context_tracks_owning_container_not_any_preceding_article(raw, role, field, owner, depth):
    from downloader.news_fulltext_videoland import _json_control_context
    with pytest.raises(json.JSONDecodeError) as caught:
        json.loads(raw)
    context = _json_control_context(raw, caught.value)
    assert context['proved'] and context['reason'] == 'matched_first_string_error'
    assert (context['role'], context['field'], context['nesting_depth']) == (role, field, depth)
    assert context['owning_object_newsarticle_type_in_valid_prefix'] is owner


@pytest.mark.parametrize('raw', [
    '{"prior":,"description":"a\nb"}',
    '{"prior":1 "description":"a\nb"}',
    '{"prior":{"x":1,},"description":"a\nb"}',
    '{"prior":[1,],"description":"a\nb"}',
    '{"prior":1] ,"description":"a\nb"}',
    '{"prior":"\\q","description":"a\nb"}',
    '{} {"description":"a\nb"}',
])
def test_control_context_does_not_trust_a_forged_later_error_or_skip_invalid_prefix(raw):
    from downloader.news_fulltext_videoland import _json_control_context
    forged = json.JSONDecodeError('Invalid control character at', raw, raw.index('\n'))
    facts = _json_control_context(raw, forged)
    assert facts['proved'] is False and facts['field'] is None and facts['role'] is None
    assert facts['reason'] in {'different_prefix_decode_error', 'prefix_grammar_or_position_mismatch'}


def test_control_context_fails_closed_on_mismatched_input_and_resource_limits(monkeypatch):
    import downloader.news_fulltext_videoland as videoland

    raw = '{"@type":"NewsArticle","description":"a\nb"}'
    with pytest.raises(json.JSONDecodeError) as caught:
        json.loads(raw)
    for source, error in [(None, caught.value), (raw+' ', caught.value), (raw, ValueError('不得保存')),
            (raw, json.JSONDecodeError('Invalid control character at', raw, 0)),
            (raw, json.JSONDecodeError('不得保存的錯誤文字', raw, raw.index('\n')))]:
        facts = videoland._json_control_context(source, error)
        assert not facts['proved'] and facts['reason'] == 'unsupported_error'
        assert '不得保存' not in json.dumps(facts, ensure_ascii=False)
    with monkeypatch.context() as patch:
        patch.setattr(videoland, '_JSON_CONTEXT_MAX_TOKENS', 5)
        facts = videoland._json_control_context(raw, caught.value)
        assert not facts['proved'] and facts['reason'] == 'prefix_token_limit'
        assert facts['tokens_examined'] == facts['maximum_tokens'] == 5
    raw = '{"prior":[{"description":"a\nb"}]}'
    with pytest.raises(json.JSONDecodeError) as caught:
        json.loads(raw)
    with monkeypatch.context() as patch:
        patch.setattr(videoland, '_JSON_CONTEXT_MAX_DEPTH', 2)
        facts = videoland._json_control_context(raw, caught.value)
        assert not facts['proved'] and facts['reason'] == 'prefix_depth_limit'
        assert facts['nesting_depth'] == facts['maximum_depth'] == 2
    huge = '{"description":"' + 'x' * (1 << 20) + '\n"}'
    with pytest.raises(json.JSONDecodeError) as caught:
        json.loads(huge)
    facts = videoland._json_control_context(huge, caught.value)
    assert not facts['proved'] and facts['tokens_examined'] == 0


@pytest.mark.parametrize('count,maximum,kind', [(16, 16, None), (17, 16, 'script_count_limit'),
                                             (64, 64, None), (65, 64, 'object_count_limit')])
def test_json_diagnostic_distinguishes_bounds_from_malformed_source(count, maximum, kind):
    def transform(soup):
        script = soup.find('script', type='application/ld+json')
        if maximum == 16:
            script.string = '{}'
            for _ in range(count-1):
                clone = soup.new_tag('script', type='application/ld+json')
                clone.string = '{}'
                soup.head.append(clone)
        else:
            script.string = json.dumps({'@graph': [{}] * (count-1)})
    facts = videoland_validation_metadata(BeautifulSoup(changed(transform), 'lxml'), page_url=URL)
    process = facts['schema_processing']
    assert facts['schema_bounded_or_json_error'] == (kind is not None)
    assert process['complete'] == (kind is None)
    assert (process['first_error']['kind'] if kind else None) == kind
    assert process['script_count_is_lower_bound'] == (count == 17)
    assert process['syntax_repair_attempted'] is False
    assert facts['accepted_as_article'] is False


def test_json_diagnostic_keeps_size_and_recursion_errors_bounded_and_private(monkeypatch):
    import downloader.news_fulltext_videoland as videoland

    oversized = '"' + 'x' * ((1 << 20) - 1) + '"'
    html = changed(lambda soup: setattr(soup.find('script', type='application/ld+json'), 'string', oversized))
    facts = videoland_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
    assert facts['schema_processing']['first_error'] == {'kind': 'script_size_limit',
        'script_index': 0, 'characters': len(oversized), 'maximum': 1 << 20}
    soup = BeautifulSoup(page(), 'lxml')
    for phase in ('json_decode', 'object_walk'):
        for cls, kind in [(RecursionError, 'recursion'), (TypeError, 'type'), (ValueError, 'value')]:
            def fail(*args, **kwargs):
                raise cls('未保存的私有錯誤文字')
            with monkeypatch.context() as patch:
                patch.setattr(videoland.json if phase == 'json_decode' else videoland,
                              'loads' if phase == 'json_decode' else '_objects', fail)
                facts = videoland_validation_metadata(soup, page_url=URL)
            process = facts['schema_processing']
            assert process['first_error']['kind'] == phase + '_' + kind
            assert process['scripts_parsed'] == (phase == 'object_walk')
            assert process['complete'] is False
            assert '未保存的私有錯誤文字' not in json.dumps(facts, ensure_ascii=False)

    def unrecognized_error(*args, **kwargs):
        raise json.JSONDecodeError('未保存的私有例外訊息', '未保存的私有原文', 2)
    with monkeypatch.context() as patch:
        patch.setattr(videoland.json, 'loads', unrecognized_error)
        facts = videoland_validation_metadata(soup, page_url=URL)
    assert facts['schema_processing']['first_error']['kind'] == 'other_json_decode_error'
    assert '未保存的私有' not in json.dumps(facts, ensure_ascii=False)


def test_valid_json_metadata_neither_repairs_nor_relabels_missing_declarations():
    for raw, schema_count in [('{}', 0), ('{"@type":"Thing"}', 0), ('[]', 0)]:
        html = changed(lambda soup: setattr(soup.find('script', type='application/ld+json'), 'string', raw))
        facts = videoland_validation_metadata(BeautifulSoup(html, 'lxml'), page_url=URL)
        process = facts['schema_processing']
        assert not facts['schema_bounded_or_json_error']
        assert process['complete'] and process['scripts_parsed'] == 1
        assert process['first_error'] is None and process['syntax_repair_attempted'] is False
        assert facts['schema_count'] == schema_count and facts['accepted_as_article'] is False


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('encoded_query', [False, True])
def test_v2_observed_name_component_preserves_native_signature_and_declared_author(tmp_path, engine, encoded_query):
    from urllib.parse import quote
    name = '測試員'
    def transform(soup):
        change_schema(soup, author={'@type': 'Person', 'name': name})
        soup.select_one('#author')['href'] = 'https://news.videoland.com.tw/search?q=' + (quote(name) if encoded_query else name)
    html = changed(transform)
    old = PolicyRegistry(versioned_config(1), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(html, old, page_url=URL)
    new = replace(PolicyRegistry(versioned_config(2), base=tmp_path).sources['videoland'], html_parser=engine)
    result = extract_article(html, new, page_url=URL)
    assert json.loads(result['authors_json']) == [name]
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['native_credit'] == CREDIT and detail['schema_author_as_declared'] == {'@type': 'Person', 'name': name}
    assert detail['native_reporter_component_matches_schema_and_search_query'] is True
    assert detail['reporter_real_identity_independently_verified'] is False
    assert result['published_at_raw'] == RELEASE and result['modified_at_raw'] == REVISION
    assert PROSE in result['body'] and CAPTION in result['body'] and result['body'].count(CAPTION) == 1


@pytest.mark.parametrize('schema_name,query', [
    ('其他人', '其他人'), ('測試員', '其他人'), ('其他人', '測試員'),
    ('測試員', '測試員&q=測試員'), ('測試員', '測試員&other='),
    ('測試員', '測試員#fragment'), (CREDIT, '測試員'), ('測試員', CREDIT),
])
def test_v2_does_not_accept_an_inconsistent_or_ambiguous_name_component(tmp_path, schema_name, query):
    def transform(soup):
        change_schema(soup, author={'@type': 'Person', 'name': schema_name})
        soup.select_one('#author')['href'] = 'https://news.videoland.com.tw/search?q=' + query
    source = PolicyRegistry(versioned_config(2), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(changed(transform), source, page_url=URL)


@pytest.mark.parametrize('obstruction', [None, 'body_sha256', 'parquet_path', 'lease_owner', 'lease_until',
    'missing_observation', 'future_observation', 'unknown_prior', 'unreviewed_scope', 'changed_rate'])
def test_v2_capture_delta_only_requeues_missing_attribution_once_preserving_evidence(tmp_path, obstruction):
    old = PolicyRegistry(versioned_config(1), base=tmp_path)
    settings = versioned_config(2)
    if obstruction == 'unreviewed_scope':
        settings['providers'][0]['authorization']['scope'] += '; changed scope'
    elif obstruction == 'changed_rate':
        settings['providers'][0]['interval_seconds'] = 5
    new = PolicyRegistry(settings, base=tmp_path)
    prior, current = old.sources['videoland'], new.sources['videoland']
    if obstruction not in {'unreviewed_scope', 'changed_rate'}:
        assert native_text_parser_repair(current, prior.data_fingerprint)
        assert (current.http_capacity_source_fingerprint or current.data_fingerprint) == (
            prior.http_capacity_source_fingerprint or prior.data_fingerprint)
        assert (current.http_capacity_authorization_fingerprint or current.authorization_fingerprint) == (
            prior.http_capacity_authorization_fingerprint or prior.authorization_fingerprint)
    else:
        assert not native_text_parser_repair(current, prior.data_fingerprint)
    before = {}
    budget = 'failure_validation_metadata_attempts:v1:videoland:article_attribution_not_verified:20261021'
    with closing(NewsQueue(tmp_path, old)) as queue:
        for i, state in enumerate(('article_attribution_not_verified', 'article_identity_not_verified',
                'article_rights_or_scope_excluded', 'forbidden', 'rate_limited', 'complete')):
            url = URL.replace('789abc', f'{i:06x}')
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, utc_now(), url))
            if obstruction in {'body_sha256', 'parquet_path', 'lease_owner', 'lease_until'}:
                queue.db.execute(f'UPDATE articles SET {obstruction}=? WHERE url=?',
                    (9000000001 if obstruction == 'lease_until' else 'protected', url))
            elif obstruction in {'missing_observation', 'future_observation'}:
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?',
                    (None if obstruction == 'missing_observation' else '2099-01-01T00:00:00+00:00', url))
            before[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        if obstruction == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:videoland'", ('f' * 64,))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (budget, '2'))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, row in before.items():
            repair = obstruction is None and row['state'] == 'article_attribution_not_verified'
            expected = {**row, 'state': 'retry'} if repair else row
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (budget,)).fetchone()[0] == '2'
        # The stored one-time marker must not requeue the same failures again.
        for url, row in before.items():
            queue.db.execute('UPDATE articles SET state=? WHERE url=?', (row['state'], url))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert all(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == row
                   for url, row in before.items())


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('name_component', [False, True])
def test_v3_optional_schema_body_absence_retains_complete_native_prose_and_captions(
        tmp_path, engine, name_component):
    def transform(soup):
        if name_component:
            omit_schema_body(soup, author={'@type': 'Person', 'name': '測試員'})
            soup.select_one('#author')['href'] = 'https://news.videoland.com.tw/search?q=測試員'
        else:
            omit_schema_body(soup)
    raw = changed(transform)
    old = replace(PolicyRegistry(versioned_config(2), base=tmp_path).sources['videoland'], html_parser=engine)
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(raw, old, page_url=URL)
    source = replace(PolicyRegistry(versioned_config(3), base=tmp_path).sources['videoland'], html_parser=engine)
    result = extract_article(raw, source, page_url=URL)
    assert PROSE in result['body'] and result['body'].count(CAPTION) == 1
    assert '-2.5' in result['body'] and '123.45億元' in result['body'] and '987股' in result['body']
    assert '合成摘要不是正文' not in result['body']
    assert result['published_at_raw'] == RELEASE and result['modified_at_raw'] == REVISION
    assert json.loads(result['authors_json']) == ['測試員' if name_component else CREDIT]
    assert 'optional_schema_articleBody_absent_as_declared' in result['article_identity_basis']
    notices = json.loads(result['verified_article_notices_json'])
    absence = next(item for item in notices if item['kind'] == 'schema_article_body_optional_field_absent')
    assert absence['body_source'] == 'whole_visible_native_article_container'
    assert absence['schema_description_or_summary_used_as_body'] is False
    assert result['body_sha256'] == extract_article(page(), source, page_url=URL)['body_sha256']


@pytest.mark.parametrize('value', [None, '', False, 13, {}, [], PROSE[:-3], PROSE + CAPTION, '這只是合成摘要'])
@pytest.mark.parametrize('version', [3, 4])
def test_optional_schema_body_does_not_hide_present_invalid_or_conflicting_values(tmp_path, value, version):
    source = PolicyRegistry(versioned_config(version), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(changed(lambda s: change_schema(s, articleBody=value)), source, page_url=URL)


@pytest.mark.parametrize('obstruction', [None, 'body_sha256', 'parquet_path', 'lease_owner', 'lease_until',
    'missing_observation', 'future_observation', 'unknown_prior', 'unreviewed_scope', 'changed_rate', 'v1_prior'])
@pytest.mark.parametrize('old_version,new_version,failure', [
    (2, 3, 'body_not_found'), (3, 4, 'article_identity_not_verified')])
def test_capture_only_requeues_exact_predecessor_failure_once(
        tmp_path, obstruction, old_version, new_version, failure):
    old = PolicyRegistry(versioned_config(1 if obstruction == 'v1_prior' else old_version), base=tmp_path)
    settings = versioned_config(new_version)
    if obstruction == 'unreviewed_scope':
        settings['providers'][0]['authorization']['scope'] += '; changed scope'
    elif obstruction == 'changed_rate':
        settings['providers'][0]['interval_seconds'] = 5
    new = PolicyRegistry(settings, base=tmp_path)
    prior, current = old.sources['videoland'], new.sources['videoland']
    assert native_text_parser_repair(current, prior.data_fingerprint) == (
        obstruction not in {'v1_prior', 'unreviewed_scope', 'changed_rate'})
    if obstruction not in {'unreviewed_scope', 'changed_rate'}:
        assert (current.http_capacity_source_fingerprint or current.data_fingerprint) == (
            prior.http_capacity_source_fingerprint or prior.data_fingerprint)
        assert (current.http_capacity_authorization_fingerprint or current.authorization_fingerprint) == (
            prior.http_capacity_authorization_fingerprint or prior.authorization_fingerprint)
    budget = f'failure_validation_metadata_attempts:v1:videoland:{failure}:20261021'
    before = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for i, state in enumerate(('body_not_found', 'article_attribution_not_verified',
                'article_identity_not_verified', 'article_rights_or_scope_excluded',
                'forbidden', 'rate_limited', 'complete')):
            url = URL.replace('789abc', f'{i:06x}')
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, utc_now(), url))
            if obstruction in {'body_sha256', 'parquet_path', 'lease_owner', 'lease_until'}:
                queue.db.execute(f'UPDATE articles SET {obstruction}=? WHERE url=?',
                    (9000000001 if obstruction == 'lease_until' else 'protected', url))
            elif obstruction in {'missing_observation', 'future_observation'}:
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?',
                    (None if obstruction == 'missing_observation' else '2099-01-01T00:00:00+00:00', url))
            before[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        if obstruction == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:videoland'", ('f' * 64,))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (budget, '2'))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, row in before.items():
            repair = obstruction is None and row['state'] == failure
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == (
                {**row, 'state': 'retry'} if repair else row)
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (budget,)).fetchone()[0] == '2'
        for url, row in before.items():
            queue.db.execute('UPDATE articles SET state=? WHERE url=?', (row['state'], url))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert all(dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == row
                   for url, row in before.items())


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('version', [1, 2, 3, 4])
def test_native_body_preserves_captions_raw_values_credit_and_source_clocks(engine, tmp_path, version):
    source = replace(PolicyRegistry(versioned_config(version), base=tmp_path).sources['videoland'], html_parser=engine)
    result = extract_article(page(), source, page_url=URL)
    assert PROSE in result['body'] and CAPTION in result['body']
    assert result['body'].count(CAPTION) == 1 and '不應擷取' not in result['body']
    assert json.loads(result['authors_json']) == [CREDIT]
    assert result['published_at_raw'] == RELEASE and result['modified_at_raw'] == REVISION
    assert result['historical_point_in_time'] is False
    notices = json.loads(result['verified_article_notices_json'])
    assert next(n for n in notices if n['kind'] == 'publisher_copyright')['text'] == COPYRIGHT
    assert next(n for n in notices if n['kind'] == 'unfetched_inline_images')['count'] == 1
    assert 'not_observed_historical_PIT' in result['published_time_basis']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['native_credit'] == CREDIT and detail['reporter_real_identity_independently_verified'] is False
    assert result['extraction_method'] == 'css:' + VIDEOLAND_BODY_SELECTOR


@pytest.mark.parametrize('url', [
    URL + '?view=1', URL + '#x', URL.replace('https:', 'http:'),
    URL.replace('news.videoland.com.tw', 'news.videoland.com.tw.invalid'),
    URL.replace('news.videoland.com.tw', 'user:secret@news.videoland.com.tw'),
    URL.replace('article/', 'article/%2f'), URL.replace('789abc', '789ABC'),
    'https://[invalid', None,
])
def test_unknown_routes_are_not_aliases_or_supported_originals(url):
    assert videoland_article_id(url) is None


@pytest.mark.parametrize('transform,code', [
    (lambda s: s.select_one('link[rel=canonical]').__setitem__('href', URL.replace('789abc', '789abd')), 'article_identity_not_verified'),
    (lambda s: s.select_one('meta[property="og:url"]').__setitem__('content', URL + '?different'), 'article_identity_not_verified'),
    (lambda s: s.select_one('#NewsTitle').append('另一篇'), 'article_identity_not_verified'),
    (lambda s: s.select_one('#NewsBody').__setitem__('hidden', ''), 'article_identity_not_verified'),
    (lambda s: s.select_one('#NewsBody p').__setitem__('class', ['hidden']), 'article_identity_not_verified'),
    (lambda s: change_schema(s, isAccessibleForFree=False), 'paywall_or_login'),
    (lambda s: change_schema(s, author={'@type': 'Person', 'name': '其他作者'}), 'article_attribution_not_verified'),
    (lambda s: s.select_one('#author').__setitem__('href', 'https://unreviewed.invalid/person'), 'article_attribution_not_verified'),
    (lambda s: change_schema(s, headline='另外一篇'), 'article_identity_not_verified'),
    (lambda s: change_schema(s, publisher={'@type': 'Organization', 'name': '其他出版社'}), 'article_identity_not_verified'),
    (lambda s: change_schema(s, articleBody=PROSE + '另外一段'), 'body_not_found'),
    (lambda s: s.select_one('#NewsBody').append(s.new_tag('iframe')), 'body_not_found'),
    (lambda s: s.select_one('#NewsModDate').string.replace_with('2026/10/17 18:00'), 'invalid_bulk_publication_clock'),
    (lambda s: s.select_one('#NewsRealDate').string.replace_with('2026/02/30 17:53'), 'invalid_bulk_publication_clock'),
    (lambda s: change_schema(s, datePublished='2026-10-16T17:53:00'), 'invalid_bulk_publication_clock'),
    (lambda s: s.select_one('footer p').decompose(), 'article_rights_not_verified'),
])
@pytest.mark.parametrize('version', [1, 2, 3, 4])
def test_conflicts_hidden_prose_unreviewed_media_and_invented_clocks_fail(tmp_path, transform, code, version):
    source = PolicyRegistry(versioned_config(version), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError) as error:
        extract_article(changed(transform), source, page_url=URL)
    assert error.value.code == code


@pytest.mark.parametrize('version', [1, 2, 3, 4])
def test_caption_only_body_is_not_promoted_to_written_prose(tmp_path, version):
    def transform(soup):
        soup.select_one('#NewsBody p').string = '【緯來新聞網】'
        change_schema(soup, articleBody='【緯來新聞網】')
        soup.select_one('figcaption').string = CAPTION * 20
    source = PolicyRegistry(versioned_config(version), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(changed(transform), source, page_url=URL)


@pytest.mark.parametrize('credit', ['中央社', '記者中央社／台北報導', '編輯某人／台北報導'])
@pytest.mark.parametrize('version', [1, 2, 3, 4])
def test_foreign_or_unreviewed_credit_scope_is_not_inferred(tmp_path, credit, version):
    def transform(soup):
        byline = soup.select_one('#author')
        byline.string = credit
        byline['href'] = 'https://news.videoland.com.tw/search?q=' + credit
        change_schema(soup, author={'@type': 'Person', 'name': credit})
    source = PolicyRegistry(versioned_config(version), base=tmp_path).sources['videoland']
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_article(changed(transform), source, page_url=URL)


@pytest.mark.parametrize('field,value', [
    ('domains', ['videoland.com.tw']), ('article_selectors', ['article']),
    ('article_path_pattern', '.*'), ('exclude_selectors', ['figcaption']),
    ('require_author_attribution', False), ('title_selector', 'h2'),
    ('wordpress_apis', ['https://news.videoland.com.tw/unreviewed-api']),
])
def test_native_source_scope_cannot_be_silently_expanded(tmp_path, field, value):
    source = entry()
    source[field] = value
    with pytest.raises(ValueError):
        PolicyRegistry(config(source), base=tmp_path)


@pytest.mark.parametrize('change', ['redirect', 'different_url', 'query'])
def test_fetch_uses_only_literal_request_and_observed_response(tmp_path, change):
    source = PolicyRegistry(config(), base=tmp_path)
    target = URL + '?view=1' if change == 'query' else URL
    response = Response(URL if change != 'different_url' else URL.replace('789abc', '789abd'),
        200, {'content-type': 'text/html'}, page())
    if change == 'redirect':
        response = replace(response, redirects=(SimpleNamespace(from_url=URL, to_url=URL),))
    calls = []
    http = SimpleNamespace(registry=source, get=lambda url, **kwargs: calls.append(url) or response)
    with pytest.raises(NewsError) as error:
        fetch_article(http, {'url': target})
    assert error.value.code == ('unsupported_article_path' if change == 'query' else 'article_identity_not_verified')
    assert calls == ([] if change == 'query' else [URL])
