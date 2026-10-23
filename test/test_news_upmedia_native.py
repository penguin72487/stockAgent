"""Modern UpMedia contracts with synthetic prose, no publisher news payload."""
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_upmedia import (
    UPMEDIA_AUTHOR_SELECTOR, UPMEDIA_BODY_SELECTOR, UPMEDIA_EXCLUSIONS, UPMEDIA_LAYOUT, UPMEDIA_PATH_PATTERN,
)


URL = 'https://www.upmedia.mg/tw/focus/finance/123456'
CLOCK = '2026-10-09T10:47:00+08:00'
TEXT = '自行撰寫測試內容：股數1000、價格12.34、負值-5與零值0都必須保留，不是新聞原文。' * 4


def entry():
    return {'id': 'upmedia', 'name': '上報', 'domains': ['www.upmedia.mg'], 'enabled': True,
        'acquisition_mode': 'continuous', 'private_known_url_scope': 'personal_noncommercial_originals_v1',
        'html_parser': 'lxml', 'interval_seconds': 10, 'parallelism': 1,
        'embedded_article_layout': UPMEDIA_LAYOUT, 'article_path_pattern': UPMEDIA_PATH_PATTERN,
        'article_selectors': [UPMEDIA_BODY_SELECTOR], 'author_selectors': [UPMEDIA_AUTHOR_SELECTOR],
        'exclude_selectors': list(UPMEDIA_EXCLUSIONS), 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'authorization': {'kind': 'personal_research', 'evidence_url': 'https://www.upmedia.mg/',
            'additional_evidence_urls': ['https://www.upmedia.mg/about', 'https://www.upmedia.mg/privacy'],
            'reviewed_on': '2026-10-22', 'automated_access_review': 'no_explicit_prohibition_identified',
            'scope': 'Private known public originals, not a sitewide content or export license.'}}


def settings(value=None):
    return {'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'collection_purpose': 'personal_research', 'providers': [value or entry()]}


def policy(value=None):
    return PolicyRegistry(settings(value), base=Path('.')).sources['upmedia']


def page(*, nested=False, staff=False, url=URL):
    names = ['朱明'] if staff else ['上報快訊', '合成測試']
    name = '／'.join(names)
    declared = {'@type': 'NewsArticle', 'url': url,
        'mainEntityOfPage': {'@type': 'WebPage', '@id': url},
        'headline': '合成標題 -- 上報', 'datePublished': CLOCK,
        'author': {'@type': 'Person', 'name': name, 'url': 'https://www.upmedia.mg/author/' + name},
        'publisher': {'@type': 'Organization', 'name': '上報', 'url': 'https://www.upmedia.mg'},
        'description': 'NOT_ARTICLE_SUMMARY'}
    byline = ''.join(f'<a href="/author/{n}">{n}</a>' for n in names)
    prose = f'<div><div>{TEXT}</div><h2>合成小標。</h2><div>合成結尾與責任編輯署名。</div></div>' if nested else f'<p>{TEXT}<br>合成結尾與責任編輯署名。</p>'
    return BeautifulSoup(f'''<html><head><link rel="canonical" href="{url}">
      <meta property="og:url" content="{url}"><meta property="og:type" content="article">
      <meta property="og:title" content="合成標題 -- 上報">
      <meta property="article:author" content="https://www.upmedia.mg/author/{name}">
      <script type="application/ld+json">{json.dumps(declared,ensure_ascii=False)}</script></head>
      <body><header><nav>NOT_ARTICLE_NAV</nav><div class="news-box"><h1>合成標題</h1>
      <div class="publish"><div class="publish-text"><span class="author au2">{byline}</span>
      <span class="pub-date">2026年10月09日 10:47:00</span></div>
      <div class="publish-news" style="display:none">NOT_ARTICLE_AI_TOOL</div></div>
      <div class="news-box-text"><!-- Native comment. -->
      <img alt="ALT_NOT_PROSE" src="https://example.org/image">
      <div class="mbt-text"><span>合成圖說與攝影署名。</span></div>
      <div class="ad-detail" data-ad="c02"><div data-cad="adc2"></div></div>{prose}
      <div><h3 class="rss_close"><strong>（熱門點閱：<a href="https://example.org/related">NOT_ARTICLE_RELATED</a>）</strong></h3></div>
      <table><tbody><tr><td>原始數值</td><td>-12.50</td><td>0</td></tr></tbody></table>
      <div class="news-foot clearfix"><div class="news-label"><a href="/search/合成"><span>合成</span></a></div></div>
      </div></div></header><footer>NOT_ARTICLE_FOOTER</footer></body></html>''', 'lxml')


def extract(soup, *, url=URL):
    return extract_article(str(soup).encode(), policy(), page_url=url)


def schema(soup, change):
    node = soup.select_one('script[type="application/ld+json"]')
    value = json.loads(str(node.string)); change(value); node.string = json.dumps(value, ensure_ascii=False)


def v2_entry():
    from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V2, UPMEDIA_AUTHOR_SELECTOR_V2
    return {**entry(), 'embedded_article_layout': UPMEDIA_LAYOUT_V2,
            'author_selectors': [UPMEDIA_AUTHOR_SELECTOR_V2]}


def single_byline_page(name='合成署名'):
    """Source-observed anchor/span geometry, entirely synthetic article/name."""
    soup = page()
    span = soup.select_one('.publish-text > .author')
    span.clear(); span.string = name
    link = soup.new_tag('a', href='/author/' + name)
    span.wrap(link)
    profile = 'https://www.upmedia.mg/author/' + name
    schema(soup, lambda value: value['author'].update(name=name, url=profile))
    soup.select_one('meta[property="article:author"]')['content'] = profile
    return soup


def extract_v2(soup):
    return extract_article(str(soup).encode(), policy(v2_entry()), page_url=URL)


def v3_entry():
    from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V3, UPMEDIA_AUTHOR_SELECTOR_V3
    return {**entry(), 'embedded_article_layout': UPMEDIA_LAYOUT_V3,
        'author_selectors': [UPMEDIA_AUTHOR_SELECTOR_V3]}


def extract_v3(soup):
    return extract_article(str(soup).encode(), policy(v3_entry()), page_url=URL)


def v4_entry():
    from downloader.news_fulltext_upmedia import UPMEDIA_LAYOUT_V4
    return {**v3_entry(), 'embedded_article_layout': UPMEDIA_LAYOUT_V4}


def extract_v4(soup):
    return extract_article(str(soup).encode(), policy(v4_entry()), page_url=URL)


@pytest.mark.parametrize('label', ['（延伸閱讀：)', '(延伸閱讀：）', '延伸閱讀：（）', '延伸閱讀:()'])
def test_v4_observed_navigation_punctuation_is_not_body_or_a_changed_value(label):
    soup = single_byline_page()
    related = soup.select_one('.rss_close strong')
    anchor = related.select_one('a').extract()
    related.clear(); related.append(label[:-1]); related.append(anchor); related.append(label[-1])
    with pytest.raises(NewsError, match='article_identity_not_verified'): extract_v3(soup)
    result = extract_v4(soup)
    assert result['body'].count(TEXT) == 1 and 'NOT_ARTICLE_RELATED' not in result['body']
    assert json.loads(result['verified_article_notices_json'])['source_related_navigation_labels_raw'] == [label]
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == ['合成署名']


@pytest.mark.parametrize('label', ['延伸閱讀：（額外數值1000）', '延伸閱讀：（未連結正文不可以丟棄）',
    '（延伸閱讀：))', '延伸閱讀：（）私自附加句子', '延伸閱讀：（）-5', '延伸閱讀：（）2026'])
def test_v4_navigation_repair_does_not_silently_drop_unlinked_prose_or_numbers(label):
    soup = single_byline_page(); related = soup.select_one('.rss_close strong')
    anchor = related.select_one('a').extract(); related.clear(); related.append(label); related.append(anchor)
    with pytest.raises(NewsError, match='article_identity_not_verified'): extract_v4(soup)


def test_v4_house_desk_profile_navigation_is_not_an_author_identity_requirement_or_guessed_profile():
    soup = single_byline_page('上報快訊'); soup.select_one('.author')['class'] = ['author']
    href = '/author/不同的導覽連結'
    soup.select_one('.publish-text > a')['href'] = href
    with pytest.raises(NewsError, match='article_attribution_not_verified'): extract_v3(soup)
    original = str(soup); result = extract_v4(soup)
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['source_visible_author_profile_hrefs_DOM'] == [href] and str(soup) == original
    assert detail['source_schema_profile_url_raw'] == 'https://www.upmedia.mg/author/上報快訊'
    assert detail['visible_profile_matches_written_credit'] == [False]
    assert detail['house_desk_nonmatching_profile_navigation']
    assert detail['profile_link_is_not_an_author_identity_witness']
    assert not detail['source_profile_navigation_corrected_or_requested']
    assert not detail['original_ownership_verified'] and not detail['human_identity_verified']
    assert result['body'].count(TEXT) == 1 and json.loads(result['authors_json']) == ['上報快訊']


@pytest.mark.parametrize('href', ['#', '', '/author/', '/author/ ', '/author/../different', '//[invalid',
    'https://other.example/author/上報快訊', '/author/上報快訊?x=1', '/author/%E4%B8%8A%E5%A0%B1',
    '/different/上報快訊', '/author/name#other', '/author/a\\b'])
def test_v4_does_not_make_unknown_or_external_profile_routes_into_desk_attribution(href):
    soup = single_byline_page('上報快訊'); soup.select_one('.publish-text > a')['href'] = href
    with pytest.raises(NewsError, match='article_attribution_not_verified'): extract_v4(soup)


@pytest.mark.parametrize('change', ['named_person', 'schema_mismatch', 'head_mismatch', 'hidden', 'explicit_external_credit'])
def test_v4_navigation_exception_cannot_override_written_credit_or_schema_head_visibility(change):
    soup = single_byline_page('合成署名' if change=='named_person' else '中央社' if change=='explicit_external_credit' else '上報快訊')
    soup.select_one('.publish-text > a')['href'] = '/author/different-navigation'
    if change == 'schema_mismatch': schema(soup, lambda value: value['author'].update(name='不同'))
    elif change == 'head_mismatch': soup.select_one('meta[property="article:author"]')['content'] = 'https://www.upmedia.mg/author/不同'
    elif change == 'hidden': soup.select_one('.author')['hidden'] = ''
    with pytest.raises(NewsError): extract_v4(soup)


@pytest.mark.parametrize('control', ['\t', '\r', '\x00'])
def test_v4_does_not_expand_the_proven_description_LF_repair(control):
    soup = single_byline_page('上報快訊')
    schema(soup, lambda value: value.update(description='PRIVATE_DESCRIPTION'))
    payload = str(soup).encode().replace(b'PRIVATE_DESCRIPTION', ('PRIVATE'+control+'DESCRIPTION').encode())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, policy(v4_entry()), page_url=URL)


def test_v4_old_body_values_authors_clocks_and_all_v3_notices_are_preserved():
    soup = single_byline_page(); old, new = extract_v3(soup), extract_v4(soup)
    for key in ('body','body_sha256','title','authors_json','published_at_raw','modified_at_raw','published_time_basis','body_completeness'):
        assert old[key] == new[key]
    assert old['verified_article_notices_json'] == new['verified_article_notices_json']
    a,b=(json.loads(row['author_attribution_detail_json']) for row in (old,new))
    assert all(b[k]==v for k,v in a.items() if k!='contract_version')
    assert b['contract_version']==4 and b['visible_profile_matches_written_credit']==[True]


def test_v4_exact_http_transition_keeps_prior_control_identity_and_native_body_contract(monkeypatch):
    from dataclasses import replace
    from downloader.news_fulltext_queue import native_body_only_parser_repair
    before, preview = policy(v3_entry()), policy(v4_entry())
    binding=(before.data_fingerprint, preview.data_fingerprint, preview.authorization_fingerprint,
        before.http_capacity_source_fingerprint,before.http_capacity_authorization_fingerprint)
    monkeypatch.setattr('downloader.news_fulltext_upmedia.UPMEDIA_V4_HTTP_BINDING', binding)
    selected=policy(v4_entry())
    assert selected.http_capacity_source_fingerprint == before.http_capacity_source_fingerprint
    assert selected.http_capacity_authorization_fingerprint == before.http_capacity_authorization_fingerprint
    assert selected.parser_repair_prior_capture_fingerprint == before.data_fingerprint
    assert native_body_only_parser_repair(selected,before.data_fingerprint)
    assert not native_body_only_parser_repair(selected,'f'*64)
    assert not native_body_only_parser_repair(replace(selected,allowed=False),before.data_fingerprint)


@pytest.mark.parametrize('whitespace', [' ', '  ', '\t', '\r\n'])
def test_v3_author_link_outer_ASCII_whitespace_only_is_comparison_not_rewritten_source(whitespace):
    soup = page()
    link = soup.select('.author > a')[1]
    original = link['href']; link['href'] = whitespace + original + whitespace
    raw = str(soup)
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_v2(soup)
    result = extract_v3(soup)
    detail = json.loads(result['author_attribution_detail_json'])
    DOM_href = BeautifulSoup(raw, 'lxml').select('.author > a')[1]['href']
    assert str(soup) == raw and detail['source_visible_author_profile_hrefs_DOM'][1] == DOM_href
    assert detail['profile_attribute_representation'].startswith('HTML_DOM;')
    assert detail['profile_comparison_only_outer_ASCII_whitespace'] and not detail['profile_percent_decoding_used']
    assert result['body'] == extract_v2(page())['body'] and result['published_at_raw'] == CLOCK


def test_v3_single_author_span_without_au2_is_the_same_observed_visible_byline():
    soup = single_byline_page('上報快訊')
    soup.select_one('.publish-text > a > span')['class'] = ['author']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_v2(soup)
    result = extract_v3(soup)
    assert json.loads(result['authors_json']) == ['上報快訊']
    assert not json.loads(result['author_attribution_detail_json'])['human_identity_verified']


@pytest.mark.parametrize('tag', ['div', 'h2', 'h3', 'h4'])
@pytest.mark.parametrize('label', ['（延伸閱讀：）', '（推薦閱讀：）', '（ 延伸閱讀 : ）', '【相關閱讀】', '熱門點閱：'])
def test_v3_related_navigation_role_preserves_body_without_hardcoding_one_heading_or_label(tag, label):
    soup = page(); node = soup.select_one('.rss_close')
    node.name = tag; link = node.select_one('a').extract(); node.clear()
    prefix, suffix = (label[:-1], label[-1]) if label[-1] in '）】' else (label, '')
    node.append(prefix); node.append(link); node.append(suffix)
    result = extract_v3(soup)
    assert result['body'] == extract_v2(page())['body'] and 'NOT_ARTICLE_RELATED' not in result['body']
    assert json.loads(result['verified_article_notices_json'])['source_related_navigation_labels_raw'] == [label]


@pytest.mark.parametrize('script_type', ['', 'text/javascript', 'application/javascript'])
@pytest.mark.parametrize('external', [False, True])
def test_v3_only_passive_scripts_within_the_exact_empty_ad_slot_are_not_prose(script_type, external):
    soup = page(); script = soup.new_tag('script')
    if script_type: script['type'] = script_type
    if external: script['src'] = 'https://example.org/never-requested.js'
    else: script.string = 'window.syntheticAdvertisingOnly = true;'
    soup.select_one('.ad-detail > div').append(script)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_v2(soup)
    result = extract_v3(soup)
    assert result['body'] == extract_v2(page())['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_ad_slot_scripts_are_excluded_without_execution']
    assert not notices['media_or_scripts_requested_or_executed']


@pytest.mark.parametrize('change', [
    lambda soup: soup.select_one('.news-box-text').append(soup.new_tag('script')),
    lambda soup: soup.select_one('.ad-detail').append(soup.new_tag('script', type='application/ld+json')),
    lambda soup: soup.select_one('.ad-detail').append(soup.new_tag('iframe')),
    lambda soup: soup.select_one('.ad-detail').append('Must not silently discard this written value1000'),
    lambda soup: soup.select_one('.rss_close').append('Unlinked sentence is not related navigation.'),
    lambda soup: soup.select_one('.rss_close').append('額外數值1000'),
    lambda soup: soup.select('.author > a')[1].__setitem__('href', ' /author/another '),
    lambda soup: soup.select('.author > a')[1].__setitem__('href', '/author/合成測試?query=1'),
    lambda soup: soup.select('.author > a')[1].__setitem__('href', '/author/%E5%90%88%E6%88%90%E6%B8%AC%E8%A9%A6'),
    lambda soup: soup.select_one('.author').__setitem__('style', 'display:none'),
    lambda soup: schema(soup, lambda value: value.update(isAccessibleForFree=False)),
])
def test_v3_does_not_turn_unknown_body_or_profile_routes_into_accepted_prose(change):
    soup = page(); change(soup)
    with pytest.raises(NewsError): extract_v3(soup)


def test_v3_reuses_strict_root_description_LF_repair_without_changing_values_or_raw_HTML():
    soup = page(); script = soup.select_one('script[type="application/ld+json"]')
    value = json.loads(script.get_text()); value['description'] = 'Synthetic summary\noriginal LF value'
    script.string = json.dumps(value, ensure_ascii=False).replace('\\n', '\n')
    raw = str(soup)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_v2(soup)
    result = extract_v3(soup)
    assert str(soup) == raw and result['body'] == extract_v2(page())['body']
    proof, = json.loads(result['verified_article_notices_json'])['source_description_LF_syntax_notices']
    assert proof['description_value_preserved'] and proof['only_root_description_syntax_changed']
    assert proof['literal_lf_count'] == 1 and not proof['raw_HTML_modified']
    assert proof['original_UTF8_HTML_script_verified']
    assert result['published_at_raw'] == CLOCK and not result['historical_point_in_time']


@pytest.mark.parametrize('field,value', [('headline', 'headline\ninvalid'), ('url', URL + '\n'),
    ('datePublished', CLOCK + '\n'), ('description', 'description\tinvalid'),
    ('description', 'description\x00invalid'), ('description', 'description\rinvalid'),
    ('description', 'line\n' * 17)])
def test_v3_JSON_fallback_never_repairs_other_fields_controls_or_excessive_LF(field, value):
    soup = page(); script = soup.select_one('script[type="application/ld+json"]')
    data = json.loads(script.get_text()); data[field] = value
    script.string = json.dumps(data, ensure_ascii=False).replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r').replace('\\u0000', '\x00')
    with pytest.raises(NewsError): extract_v3(soup)


def test_v3_description_repair_requires_matching_original_bytes_not_only_a_DOM():
    from downloader.news_fulltext_upmedia import upmedia_public_modern_article
    soup = page(); script = soup.select_one('script[type="application/ld+json"]')
    script.string = script.get_text().replace('NOT_ARTICLE_SUMMARY', 'summary\noriginal LF')
    raw = str(soup).encode(); parsed = BeautifulSoup(raw, 'lxml')
    for original in (None, raw.replace(b'original LF', b'different LF'), b'\xff' + raw):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            upmedia_public_modern_article(parsed, page_url=URL, adapter_contract_version=3, raw_payload=original)
    article, _ = upmedia_public_modern_article(parsed, page_url=URL, adapter_contract_version=3, raw_payload=raw)
    assert json.loads(article['verified_article_notices_json'])['source_description_LF_syntax_notices']


@pytest.mark.parametrize('control', ['\x01', '\x08', '\x0b', '\x0c', '\x0e', '\x1f'])
def test_v3_never_accepts_literal_non_HTML_controls_after_DOM_loss(control):
    soup = page(); node = soup.select_one('.news-box-text p'); node.append(control)
    with pytest.raises(NewsError, match='article_identity_not_verified'): extract_v3(soup)


def test_v3_original_CRLF_between_JSON_fields_is_not_a_changed_description_value():
    soup = page(); script = soup.select_one('script[type="application/ld+json"]')
    data = json.loads(script.get_text()); data['description'] = 'summary\noriginal LF'
    script.string = json.dumps(data, ensure_ascii=False, indent=2).replace('\n', '\r\n').replace('\\n', '\n')
    result = extract_v3(soup)
    proof, = json.loads(result['verified_article_notices_json'])['source_description_LF_syntax_notices']
    assert proof['literal_lf_count'] == 1 and proof['original_UTF8_HTML_script_verified']


@pytest.mark.parametrize('name,role', [('上報快訊', 'publisher_branded_desk'),
    ('合成署名', 'publisher_declared_named_byline'), ('朱明', 'current_about_declared_editorial_staff')])
def test_v2_observed_single_anchor_span_credit_is_attribution_not_employment(name, role):
    soup = single_byline_page(name)
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup)
    result = extract_v2(soup)
    assert json.loads(result['authors_json']) == [name] and result['body'].count(TEXT) == 1
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['contract_version'] == 2 and detail['source_credit_role'] == role
    assert detail['source_visible_byline_layout'] == 'anchor_contains_written_author_span'
    assert not detail['historical_employment_verified'] and not detail['human_identity_verified']
    assert not detail['original_ownership_verified'] and not result['per_article_license_verified']
    assert result['published_at_raw'] == CLOCK and result['modified_at_raw'] is None
    assert not result['historical_point_in_time']
    assert not settings()['publish'] and not settings()['remote_training_export']


@pytest.mark.parametrize('staff', [False, True])
def test_v2_retained_v1_values_body_and_clock_contract_are_unchanged(staff):
    soup = page(staff=staff)
    old, new = extract(soup), extract_v2(soup)
    for field in ('body', 'body_sha256', 'title', 'authors_json', 'published_at_raw', 'modified_at_raw',
                  'published_time_basis', 'body_completeness', 'verified_article_notices_json'):
        assert new[field] == old[field]
    assert policy().data_fingerprint != policy(v2_entry()).data_fingerprint
    assert policy().parser_repair_fingerprint != policy(v2_entry()).parser_repair_fingerprint


@pytest.mark.parametrize('change', [
    lambda soup: soup.select_one('.publish-text > a').__setitem__('href', '/author/other'),
    lambda soup: soup.select_one('.publish-text > a').__setitem__('hidden', ''),
    lambda soup: soup.select_one('.publish-text > a > span').__setitem__('style', 'display:none'),
    lambda soup: soup.select_one('.publish-text > a > span').__setitem__('class', ['author']),
    lambda soup: soup.select_one('.publish-text > a > span').append(soup.new_tag('strong')),
    lambda soup: soup.select_one('.publish-text').append(soup.select_one('.publish-text > a').__copy__()),
    lambda soup: schema(soup, lambda value: value['author'].update(name='不同署名')),
    lambda soup: schema(soup, lambda value: value.update(isAccessibleForFree=False)),
    lambda soup: soup.select_one('.pub-date').__setattr__('string', '2026年10月09日 10:47:01'),
    lambda soup: soup.select_one('.news-box-text').append(soup.new_tag('iframe')),
])
def test_v2_does_not_relax_profile_visibility_access_clock_or_whole_editor(change):
    soup = single_byline_page(); change(soup)
    with pytest.raises(NewsError):
        extract_v2(soup)


@pytest.mark.parametrize('name', ['中央社', '路透社', '美聯社', '工商時報'])
def test_v2_explicit_external_credit_is_not_made_into_original(name):
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract_v2(single_byline_page(name))


def test_v2_policy_reconciliation_does_not_reset_any_existing_head(tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    urls = [URL, URL+'1', URL+'2', URL+'3', URL+'4']
    queue = NewsQueue(tmp_path, PolicyRegistry(settings(), base=tmp_path))
    with queue.db:
        queue.add_articles([{'url': url} for url in urls])
        for url, state in zip(urls, ['article_attribution_not_verified', 'article_identity_not_verified',
                                    'http_403', 'paywall_or_login', 'complete']):
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=123 WHERE url=?', (state, state, url))
        queue.db.execute('UPDATE articles SET body_sha256=?,parquet_path=? WHERE url=?', ('retained', 'retained.parquet', urls[-1]))
    before = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(settings(v2_entry()), base=tmp_path))
    try:
        assert [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
    finally:
        queue.close()


@pytest.mark.parametrize('nested', [False, True])
@pytest.mark.parametrize('staff', [False, True])
def test_complete_editor_under_header_preserves_values_captions_credit_and_clocks(nested, staff):
    soup = page(nested=nested, staff=staff); original = str(soup)
    result = extract(soup)
    assert str(soup) == original
    assert result['body'].count(TEXT) == result['body'].count('合成圖說與攝影署名。') == 1
    assert result['body'].index('合成圖說') < result['body'].index(TEXT) < result['body'].index('原始數值')
    assert '-12.50' in result['body'] and 'NOT_ARTICLE_' not in result['body'] and 'ALT_NOT_PROSE' not in result['body']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert result['published_at_raw'] == CLOCK and result['modified_at_raw'] is None
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert json.loads(result['authors_json']) == (['朱明'] if staff else ['上報快訊／合成測試'])
    detail = json.loads(result['author_attribution_detail_json'])
    assert not detail['original_ownership_verified'] and not detail['historical_employment_verified']
    clocks = json.loads(result['verified_article_notices_json'])
    assert clocks['source_declared_timezone_offset_raw'] == '+08:00'
    assert not clocks['exact_PIT_eligible'] and not clocks['effective_clock_precision_verified']


@pytest.mark.parametrize('selector,attribute,value,error', [
    ('link[rel=canonical]', 'href', URL+'0', 'article_identity_not_verified'),
    ('meta[property="og:url"]', 'content', URL+'0', 'article_identity_not_verified'),
    ('meta[property="og:type"]', 'content', 'website', 'article_identity_not_verified'),
    ('meta[property="og:title"]', 'content', '不同標題', 'article_identity_not_verified'),
    ('.news-box', 'hidden', '', 'article_identity_not_verified'),
    ('.news-box-text', 'style', 'display: none', 'article_identity_not_verified'),
    ('.mbt-text', 'aria-hidden', 'true', 'article_identity_not_verified'),
    ('.author > a', 'href', '/author/other', 'article_attribution_not_verified'),
    ('meta[property="article:author"]', 'content', 'other', 'article_attribution_not_verified'),
])
def test_identity_visibility_and_attribution_are_current_article_bound(selector, attribute, value, error):
    soup = page(); soup.select_one(selector)[attribute] = value
    with pytest.raises(NewsError, match=error): extract(soup)


@pytest.mark.parametrize('change,error', [
    (lambda a: a.update(url=URL+'0'), 'article_identity_not_verified'),
    (lambda a: a.update(mainEntityOfPage={'@id': URL+'0'}), 'article_identity_not_verified'),
    (lambda a: a.update(headline='不符的標題 -- 上報'), 'article_identity_not_verified'),
    (lambda a: a['publisher'].update(name='other'), 'article_identity_not_verified'),
    (lambda a: a['author'].update(name='中央社'), 'article_attribution_not_verified'),
    (lambda a: a['author'].update(url='https://example.org/author'), 'article_attribution_not_verified'),
    (lambda a: a['author'].update({'@type': 'Organization'}), 'article_attribution_not_verified'),
    (lambda a: a.update(datePublished='2026-02-30T10:47:00+08:00'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(datePublished='2026-10-09T10:47:00Z'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(datePublished='2026-10-09T10:47+08:00'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(datePublished='2026-10-09T10:47:01+08:00'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(dateModified='2026-10-08T10:47:00+08:00'), 'invalid_bulk_publication_clock'),
    (lambda a: a.update(isAccessibleForFree=False), 'paywall_or_login'),
    (lambda a: a.update(isAccessibleForFree='false'), 'paywall_or_login'),
])
def test_schema_never_overrides_visible_identity_clocks_or_access(change, error):
    soup = page(); schema(soup, change)
    with pytest.raises(NewsError, match=error): extract(soup)


@pytest.mark.parametrize('selector,markup', [
    ('.news-box', '<p>容器外不可遺失正文。</p>'),
    ('.news-box-text', '<iframe src="https://example.org/member"></iframe>'),
    ('.news-box-text', '<p hidden>不可刪掉以宣稱完整。</p>'),
    ('.news-box-text', '<script>window.unreviewed = true;</script>'),
    ('.ad-detail', '<p>不是廣告空槽，不能悄悄排除。</p>'),
    ('.rss_close', '<span>不是延伸閱讀，不能悄悄排除。</span>'),
    ('.news-foot', '<p>不是標籤，不能悄悄排除。</p>'),
])
def test_unknown_or_hidden_editor_content_is_not_silently_dropped(selector, markup):
    soup = page(); fragment = BeautifulSoup('<div>'+markup+'</div>', 'lxml')
    soup.select_one(selector).append(fragment.div.contents[0])
    with pytest.raises(NewsError, match='article_identity_not_verified'): extract(soup)


def test_explicit_reprint_is_not_turned_into_publisher_original():
    soup = page(); soup.select_one('.news-box-text > p').append('本文轉載自合成來源。')
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'): extract(soup)


def test_parser_reparented_paragraph_outside_related_heading_still_keeps_whole_prose():
    soup = page()
    extra = soup.new_tag('p'); extra.string = '解析器移回正文的段落仍須完整保留。'
    soup.select_one('.rss_close').append(extra)
    assert extract(soup)['body'].count(extra.string) == 1


@pytest.mark.parametrize('url', [URL+'?member=1', URL+'#body', URL.replace('www.', ''),
    'https://www.upmedia.mg/news_info.php?SerialNo=123456',
    'https://www.upmedia.mg/author/合成測試', URL.replace('/finance/', '/Finance/')])
def test_no_legacy_conversion_unobserved_alias_or_new_namespace(url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(url=url), url=url)


@pytest.mark.parametrize('field,value', [
    ('domains', ['upmedia.mg']), ('acquisition_mode', 'finite'),
    ('private_known_url_scope', ''), ('require_author_attribution', False),
    ('article_selectors', ['article']), ('author_selectors', ['a']),
    ('exclude_selectors', ['.news-box-text']), ('html_parser', 'html.parser'),
    ('permitted_author_names', ['上報']), ('sitemaps', ['https://www.upmedia.mg/sitemapnews']),
])
def test_known_url_scope_does_not_grant_sitewide_discovery_or_loose_parsing(field, value):
    candidate = entry(); candidate[field] = value
    with pytest.raises(ValueError, match='known-URL|embedded article|author scope'): policy(candidate)


def test_only_reviewed_capture_and_authorization_preserve_diagnostic_http_identity(monkeypatch):
    source = policy()
    monkeypatch.setattr('downloader.news_fulltext_upmedia.UPMEDIA_HTTP_BINDING',
        (source.data_fingerprint, source.authorization_fingerprint, '1'*64, '2'*64))
    assert policy().http_capacity_source_fingerprint == '1'*64
    candidate = entry(); candidate['interval_seconds'] = 11
    assert not policy(candidate).http_capacity_source_fingerprint
    candidate = entry(); candidate['authorization']['scope'] += ' Different.'
    assert not policy(candidate).http_capacity_authorization_fingerprint


def test_v3_only_exact_reviewed_capture_inherits_prior_HTTP_and_missing_only_repair(monkeypatch):
    from dataclasses import replace
    from downloader.news_fulltext_queue import native_body_only_parser_repair
    before, selected = policy(v2_entry()), policy(v3_entry())
    monkeypatch.setattr('downloader.news_fulltext_upmedia.UPMEDIA_V3_HTTP_BINDING',
        (before.data_fingerprint, selected.data_fingerprint, selected.authorization_fingerprint, '1'*64, '2'*64))
    selected = policy(v3_entry())
    assert selected.parser_repair_prior_capture_fingerprint == before.data_fingerprint
    assert selected.http_capacity_source_fingerprint == '1'*64 and selected.http_capacity_authorization_fingerprint == '2'*64
    assert native_body_only_parser_repair(selected, before.data_fingerprint)
    assert not native_body_only_parser_repair(selected, 'f'*64)
    assert not native_body_only_parser_repair(replace(selected, allowed=False), before.data_fingerprint)
    for field in ('interval_seconds', 'authorization'):
        candidate = v3_entry()
        if field == 'authorization': candidate[field]['scope'] += ' A different scope.'
        else: candidate[field] += 1
        changed = policy(candidate)
        assert not changed.http_capacity_source_fingerprint
        assert not changed.http_capacity_authorization_fingerprint
        assert not native_body_only_parser_repair(changed, before.data_fingerprint)


def test_existing_diagnostic_directory_is_reused_once_without_copy_or_http(tmp_path, monkeypatch):
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    from test_news_retained_native_replay import approved, head
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
        lambda *args, **kwargs: pytest.fail('retained acceptance must not create HTTP'))
    queue, _, path, manifest = approved(tmp_path, settings(), provider='upmedia', urls=(URL,),
        raw_factory=lambda **kwargs: str(page()).encode())
    try:
        old = path.parent/'private-source-diagnostics'; directory = path.parent/'upmedia-retained-originals'
        old.rename(directory); directory.chmod(0o700)
        for item in directory.iterdir(): item.chmod(0o600)
        report_path = path.parent/manifest['observation_report']
        report = json.loads(report_path.read_text())
        for probe in report['probes']:
            probe['private_diagnostic_path'] = str(directory/Path(probe['private_diagnostic_path']).name)
        atomic_write_json(report_path, report, durable=True)
        manifest.update(diagnostic_directory=directory.name, diagnostic_directory_contract_version=1,
            observation_report_sha256=hashlib.sha256(report_path.read_bytes()).hexdigest())
        for item in manifest['entries']:
            item['diagnostic_path'] = str(Path(directory.name)/Path(item['diagnostic_path']).name)
        atomic_write_json(path, manifest, durable=True)
        result = materialize_reviewed_native(queue, (URL,), path, lease_seconds=300)
        assert result['materialized_bodies'] == 1 and result['publisher_requests_this_command'] == 0
        before = head(queue, URL)
        assert before['state'] == 'complete' and not old.exists()
        assert materialize_reviewed_native(queue, (URL,), path, lease_seconds=300)['materialized_bodies'] == 0
        assert head(queue, URL) == before
    finally: queue.close()


@pytest.mark.parametrize('directory,version', [('..', 1), ('../outside', 1), ('/outside', 1),
    ('private-source-diagnostics', 2), ('private-source-diagnostics', True), ('private-source-diagnostics', None)])
def test_unreviewed_directory_never_claims_body(tmp_path, directory, version):
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    from test_news_retained_native_replay import approved, head
    queue, _, path, manifest = approved(tmp_path, settings(), provider='upmedia', urls=(URL,),
        raw_factory=lambda **kwargs: str(page()).encode())
    try:
        manifest.update(diagnostic_directory=directory, diagnostic_directory_contract_version=version)
        atomic_write_json(path, manifest, durable=True); before = head(queue, URL)
        with pytest.raises(ValueError, match='diagnostic directory'):
            materialize_reviewed_native(queue, (URL,), path, lease_seconds=300)
        assert head(queue, URL) == before
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    finally: queue.close()
