"""Native report ABI: public prose versus UI, named reporter versus CMS login."""
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_fact_checks import TFC_BODY_SELECTOR
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


URL = 'https://tfc-taiwan.org.tw/fact-check-reports/example-financial-report/'
LEGAL_NAME = '財團法人新北市台灣事實查核教育基金會'
BODY = '原始股數1000、價格-12.5、比例3.25%，不作數值變換。'


@pytest.fixture
def source():
    return load_config(Path('configs/news_fulltext.json'))[1].sources['tfc_taiwan']


def page():
    graph = {'@graph': [
        {'@type': ['Article', 'Report'], '@id': URL + '#article',
         'isPartOf': {'@id': URL}, 'mainEntityOfPage': {'@id': URL},
         'headline': '本篇金融查核', 'author': {'name': 'cms-login', '@id': 'https://tfc-taiwan.org.tw/#/person/one'},
         'datePublished': '2026-07-28T07:05:01+00:00', 'dateModified': '2026-07-28T07:07:55+00:00',
         'publisher': {'@id': 'https://tfc-taiwan.org.tw/#organization'}, 'wordCount': 16},
        {'@type': 'Organization', '@id': 'https://tfc-taiwan.org.tw/#organization',
         'name': LEGAL_NAME, 'alternateName': '台灣事實查核中心', 'url': 'https://tfc-taiwan.org.tw/'},
    ]}

    def column(text, classes=''):
        return f'<div class="wp-block-kadence-column {classes}"><div class="kt-inside-inner-col">{text}</div></div>'

    hero = ('<div class="wp-block-kadence-rowlayout"><div class="kt-row-column-wrap">'
            + column('') + column('<p>上方短標不是完整正文</p>') + column('') + '</div></div>')
    metadata = '''<div class="wp-block-kadence-column kvs-sm-false">
      <p>發佈： 2026-07-28</p><p>更新： 2026-07-28</p><p>報告編號： 4245</p>
      <p>查核記者： 曾慧雯</p><p>責任編輯： 陳偉婷</p></div>
      <div class="wp-block-kadence-column kvs-lg-false kvs-md-false">
      <p>發佈： 2026-07-28</p><p>報告編號： 4245</p><p>記者： 曾慧雯、 責任編輯： 陳偉婷</p></div>
      <div><span>內容 背景 查核 Share on Facebook</span></div>'''
    body = (f'<p>{BODY * 5}</p><h2>背景</h2><p>{BODY}</p>'
            '<div class="wp-block-kadence-image"><figure><img src="never-download.jpg">'
            '<figcaption>排除的第三方截圖說明</figcaption></figure></div>'
            f'<h2>查核</h2><h3>第一查核點</h3><p>{BODY}</p>')
    main = ('<div class="wp-block-kadence-rowlayout"><div class="kt-row-column-wrap">'
            + column(metadata, 'kb-section-is-sticky') + column(body) + column('') + '</div></div>')
    return f'''<html><head><meta charset="utf-8"><link rel="canonical" href="{URL}">
      <meta property="og:url" content="{URL}"><meta property="og:title" content="本篇金融查核 - 台灣事實查核中心">
      <meta property="article:modified_time" content="2026-07-28T07:07:55+00:00">
      <script type="application/ld+json">{json.dumps(graph, ensure_ascii=False)}</script></head><body>
      <article id="post-153277" class="entry single-entry type-fact-check-reports post-153277">
      <div class="entry-content-wrap"><div class="entry-content single-content">{hero}{main}<p></p></div></div></article>
      <footer>發佈：2099-12-31 作者：側欄人物 捐款贊助我們</footer></body></html>'''


def extract(source, text=None, url=URL):
    return extract_article((page() if text is None else text).encode(), source, page_url=url)


def test_report_body_person_and_original_clock(source):
    result = extract(source)
    assert json.loads(result['authors_json']) == ['曾慧雯']
    provenance = json.loads(result['author_attribution_detail_json'])
    assert provenance['responsible_editor_raw'] == '陳偉婷'
    assert provenance['schema_cms_author_raw']['name'] == 'cms-login'
    assert provenance['report_number_raw'] == '4245'
    assert result['published_at_raw'] == '2026-07-28T07:05:01+00:00'
    assert result['modified_at_raw'] == '2026-07-28T07:07:55+00:00'
    assert result['title'] == '本篇金融查核'
    assert result['body'].count(BODY) == 7
    for excluded in ('上方短標', 'Share on Facebook', '責任編輯', 'cms-login', '側欄人物', '第三方截圖', '捐款贊助'):
        assert excluded not in result['body']
    assert result['article_identity_basis'] == 'tfc_same_requested_canonical_og_schema_report_v1'
    assert result['body_completeness'].startswith('verified_current_fact_check_report_text;')
    assert result['historical_point_in_time'] is False


@pytest.mark.parametrize('old,new,code', [
    ('href="' + URL + '"', 'href="https://tfc-taiwan.org.tw/fact-check-reports/other/"', 'article_identity_not_verified'),
    ('property="og:url" content="' + URL, 'property="og:url" content="https://tfc-taiwan.org.tw/other', 'article_identity_not_verified'),
    ('"mainEntityOfPage": {"@id": "' + URL, '"mainEntityOfPage": {"@id": "https://tfc-taiwan.org.tw/other/', 'article_identity_not_verified'),
    ('"Report"', '"Unknown"', 'article_identity_not_verified'),
    (LEGAL_NAME, '不明公司', 'article_identity_not_verified'),
    ('post-153277" class', 'post-153278" class', 'article_identity_not_verified'),
    ('type-fact-check-reports', 'type-post', 'article_identity_not_verified'),
    ('本篇金融查核 - 台灣事實查核中心', '別篇標題 - 台灣事實查核中心', 'article_identity_not_verified'),
    ('<p>查核記者： 曾慧雯</p>', '', 'article_attribution_not_verified'),
    ('<p>查核記者： 曾慧雯</p>', '<p>查核記者： cms-login</p>', 'article_attribution_not_verified'),
    ('記者： 曾慧雯、 責任編輯： 陳偉婷', '記者： 陳偉婷、 責任編輯： 陳偉婷', 'article_attribution_not_verified'),
    ('<p>責任編輯： 陳偉婷</p>', '', 'article_attribution_not_verified'),
    ('<p>報告編號： 4245</p>', '<p>報告編號： unknown</p>', 'article_identity_not_verified'),
    ('<p>發佈： 2026-07-28</p>', '<p>發佈： 2026-07-29</p>', 'invalid_bulk_publication_clock'),
    ('2026-07-28T07:05:01+00:00', '2026-07-28T07:05:01', 'invalid_bulk_publication_clock'),
    ('2026-07-28T07:05:01+00:00', '2026-07-29T07:05:01+00:00', 'invalid_bulk_publication_clock'),
    ('2026-07-28T07:07:55+00:00', '2026-07-28T07:04:59+00:00', 'invalid_bulk_publication_clock'),
    ('<h2>查核</h2>', '', 'body_not_found'),
    ('kb-section-is-sticky', 'unknown-sidebar', 'body_not_found'),
    ('<h3>第一查核點</h3>', '<div>未審查的內容模組</div>', 'body_not_found'),
    ('<p>查核記者： 曾慧雯</p>', '<p hidden>查核記者： 曾慧雯</p>', 'article_attribution_not_verified'),
    ('<h2>背景</h2>', '<h2 aria-hidden="true">背景</h2>', 'body_not_found'),
])
def test_no_fallback_for_conflicts_missingness_or_unreviewed_representation(source, old, new, code):
    original = page()
    assert old in original
    with pytest.raises(NewsError, match=code):
        extract(source, original.replace(old, new))


@pytest.mark.parametrize('suffix', ['?paid=1', '?exp=a', '#other'])
def test_unknown_request_parameters_never_sanitized(source, suffix):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(source, url=URL + suffix)


def test_source_declared_nonfree_report_rejected(source):
    text = page().replace('"wordCount": 16', '"isAccessibleForFree": false, "wordCount": 16')
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract(source, text)


@pytest.mark.parametrize('patch', [
    {'domains': ['example.com']}, {'id': 'other_source'}, {'acquisition_mode': 'continuous'},
    {'article_selectors': ['.entry-content']}, {'exclude_selectors': []},
    {'require_author_attribution': False}, {'require_configured_container': False},
    {'authorization': {'kind': 'open_license', 'license': 'unknown', 'scope': 'not verified',
                       'evidence_url': 'https://tfc-taiwan.org.tw/copyright-notice/'}},
])
def test_native_capture_contract_cannot_be_transferred_or_broadened(patch, tmp_path):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    entry = next(deepcopy(item) for item in cfg['providers'] if item['id'] == 'tfc_taiwan')
    entry.update(patch)
    cfg['providers'] = [entry]
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry(cfg, base=tmp_path)


def test_production_recipe_has_no_unverified_discovery_or_parallel_limit(source):
    assert source.acquisition_mode == 'finite'
    assert source.selectors == (TFC_BODY_SELECTOR,)
    assert not source.feeds and not source.sitemaps and not source.index_pages
    assert not source.wordpress_apis and not source.fulltext_feeds
    assert source.interval_seconds == 2 and source.parallelism == 1


def test_all_original_provider_contracts_unchanged_when_new_provider_added():
    cfg, registry, repo = load_config(Path('configs/news_fulltext.json'))
    cfg['providers'] = [item for item in cfg['providers'] if item['id'] != 'tfc_taiwan']
    previous = PolicyRegistry(cfg, base=repo)
    assert len(registry.sources) == len(previous.sources) + 1
    assert all(asdict(registry.sources[key]) == asdict(value) for key, value in previous.sources.items())


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_offline_benchmark_keeps_real_finite_claim_and_durable_acceptance(tmp_path, parser):
    from scripts.benchmark_news_fulltext import pipeline
    cfg, _, repo = load_config(Path('configs/news_fulltext.json'))
    cfg['providers'] = [entry for entry in cfg['providers'] if entry['id'] == 'tfc_taiwan']
    registry = PolicyRegistry(cfg, base=repo)
    rows = [
        {'provider': 'tfc_taiwan', 'requested_url': URL, 'final_url': URL,
         'raw_response_body': page().encode()},
        {'provider': 'tfc_taiwan', 'requested_url': URL.replace('example-financial-report', 'wrong-identity'),
         'final_url': URL.replace('example-financial-report', 'wrong-identity'),
         'raw_response_body': page().encode()},
    ]
    result = pipeline(rows, registry, cfg, parser, tmp_path)
    assert result['accepted_records'] == result['verified_body_versions'] == 1
    assert result['verified'] and len(result['body_hashes']) == 1
    assert registry.sources['tfc_taiwan'].acquisition_mode == 'finite'
    assert not list(tmp_path.iterdir())

