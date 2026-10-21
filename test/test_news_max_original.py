"""Synthetic MAX current-post contracts; no publisher prose or source calls."""
from copy import deepcopy
from dataclasses import replace
import json
import hashlib
from pathlib import Path
import re
import time

import pytest
from bs4 import BeautifulSoup

from downloader.news_fulltext import discovery_links, load_config
from downloader.news_fulltext_max import (
    MAX_AUTHORS, MAX_AUTHOR_SELECTOR, MAX_BODY_SELECTOR, MAX_ORIGIN, MAX_PATH_PATTERN, MAX_SITEMAP,
    max_native_clock_metadata,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader import news_fulltext_max as native
from downloader.news_fulltext_queue import NewsQueue, max_native_clock_parser_repair
from stockagent.data.news_source_catalog import load_source_review_ledger
from scripts.probe_news_sources import compare_native_candidate, max_candidate_source, probe_provider
from downloader.news_fulltext_http import Response
from types import SimpleNamespace


URL = MAX_ORIGIN + '/synthetic-company/'
PUBLISHED = '2023-02-14T15:33:03+08:00'
MODIFIED = '2026-10-01T10:42:08+08:00'


def test_clock_metadata_observes_declared_fields_not_body_or_repairs(settings):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    soup = BeautifulSoup(page(), 'lxml')
    before = str(soup)
    result = max_native_clock_metadata(soup, source)
    assert result['head_clock_count'] == result['header_time_count'] == result['schema_clock_count'] == 2
    assert [r['content'] for r in result['head_clocks']] == [PUBLISHED, MODIFIED]
    assert [r['datetime'] for r in result['header_times']] == [PUBLISHED, MODIFIED]
    assert result['schema_clocks'][0]['datePublished'] == PUBLISHED
    assert result['schema_clocks'][1]['dateModified'] == MODIFIED
    assert all(not result[k] for k in ('body_text_exported', 'raw_response_retained',
        'source_clock_inferred', 'queue_mutation_possible', 'article_durably_accepted'))
    assert '完整合成研究細節' not in json.dumps(result, ensure_ascii=False)
    assert str(soup) == before


def test_clock_metadata_uses_exact_counts_bounded_samples_and_fields(settings):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    soup = BeautifulSoup(page(), 'lxml')
    for _ in range(20):
        node = soup.new_tag('time', datetime='x' * 500)
        node['class'] = ['y' * 500] * 12
        soup.select_one('.single-header').append(node)
    result = max_native_clock_metadata(soup, source)
    assert result['header_time_count'] == 22 and len(result['header_times']) == 8
    assert all(len(r['ancestors']) <= 6 and len(r['classes']) <= 8
               and all(len(c) <= 80 for c in r['classes']) for r in result['header_times'])
    assert all(len(r['datetime']) <= 160 for r in result['header_times'])


def test_clock_metadata_ignores_footer_time_and_unrelated_multi_type_schema(settings):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    soup = BeautifulSoup(page(), 'lxml')
    soup.body.append(BeautifulSoup('<footer><time datetime="secret">not a clock</time></footer>', 'lxml').footer)
    node = soup.new_tag('script', type='application/ld+json')
    node.string = json.dumps({'@type': ['Person', 'Organization'], 'datePublished': 'not an article'})
    soup.head.append(node)
    result = max_native_clock_metadata(soup, source)
    assert result['header_time_count'] == 2 and result['schema_clock_count'] == 2
    assert 'secret' not in json.dumps(result) and 'not an article' not in json.dumps(result)
    with pytest.raises(ValueError, match='max_native_clock_metadata_scope_required'):
        max_native_clock_metadata(soup, replace(source, provider='unreviewed'))


@pytest.fixture
def settings():
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    config = deepcopy(config)
    config['providers'] = [{
        'id': 'maxfinanciallife', 'name': 'MAX 的金融生活', 'enabled': True,
        'domains': ['maxfinanciallife.com'], 'acquisition_mode': 'continuous',
        'interval_seconds': 2, 'parallelism': 1, 'html_parser': 'lxml',
        'embedded_article_layout': 'max_visible_post_v1',
        'article_path_pattern': MAX_PATH_PATTERN, 'article_selectors': [MAX_BODY_SELECTOR],
        'exclude_selectors': [], 'title_selector': 'h1.post-title.single-post-title.entry-title',
        'require_configured_container': True, 'require_author_attribution': True,
        'author_selectors': [MAX_AUTHOR_SELECTOR], 'permitted_author_names': list(MAX_AUTHORS),
        'sitemaps': [MAX_SITEMAP],
        'authorization': {'kind': 'personal_research',
            'evidence_url': MAX_ORIGIN + '/editorial-policy/',
            'scope': 'Private current own-post text, not old historical versions or a reuse grant.',
            'original_author_scope_evidence_urls': [MAX_ORIGIN + '/about/'],
            'reviewed_on': '2026-10-20', 'automated_access_review': 'no_explicit_prohibition_identified'},
    }]
    return config


def page(*, wrapped=False, author='MAX 投資研究團隊', profile='/author/ping/'):
    title = '合成原生可見標題'
    machine_title = '合成但不同的社群與機器標題'
    graph = {'@graph': [
        {'@type': 'Person', '@id': MAX_ORIGIN + '/#person', 'name': '不可拿來代替作者'},
        {'@type': 'WebPage', '@id': URL + '#webpage', 'url': URL, 'name': machine_title,
         'datePublished': PUBLISHED, 'dateModified': MODIFIED},
        {'@type': 'BlogPosting', '@id': URL + '#richSnippet', 'headline': machine_title,
         'author': {'@id': MAX_ORIGIN + profile, 'name': author},
         'datePublished': PUBLISHED, 'dateModified': MODIFIED,
         'mainEntityOfPage': {'@id': URL + '#webpage'}, 'isPartOf': {'@id': URL + '#webpage'}},
    ]}
    prose = ''.join(f'<p>第{i}段原稿，' + '完整合成研究細節與風險揭露。' * 12 + '</p>' for i in range(4))
    prose += '''<figure><table><tbody><tr><th>來源原始值</th><td>-987.65</td><td>6.50%</td>
        <td>1,234</td></tr></tbody></table></figure><div>來源聲明不是實際報酬</div>
        <figure><img src="/never-fetch-photo.png"><figcaption>獨立照片說明</figcaption></figure>'''
    if wrapped:
        prose = f'<div class="wb-s1-article"><h2>公司指南小標</h2>{prose}</div>'
    else:
        prose = '''<div class="no_bullets" id="toc_container"><p class="toc_title">內容目錄</p>
          <ul class="toc_list"><li><a href="#section-one">測試目錄</a></li></ul></div>
          <h1>正文另有原生標題</h1><h2 id="section-one">正文小標</h2>''' + prose
        prose += '''<h2><span>延伸推薦閱讀</span></h2><ul><li><a href="/different-story/">別篇連結</a>：另篇簡介</li></ul>'''
    return f'''<html><head><title>{machine_title}</title><link rel="canonical" href="{URL}">
        <meta property="og:title" content="{machine_title}"><meta property="og:url" content="{URL}">
        <meta property="article:published_time" content="{PUBLISHED}">
        <meta property="article:modified_time" content="{MODIFIED}">
        <script type="application/ld+json">{json.dumps(graph, ensure_ascii=False)}</script></head>
        <body><nav>頁首導覽不是文章</nav><article id="post-64438" class="post type-post status-publish">
        <div class="header-standard header-classic single-header"><h1 class="post-title single-post-title entry-title">{title}</h1>
        <div class="post-box-meta-single"><span class="author-post byline"><span class="author vcard">
        作者 <a class="author-url url fn n" href="{MAX_ORIGIN + profile}">{author}</a></span></span>
        <span><time class="entry-date published" datetime="{PUBLISHED}">14 2 月, 2023</time>
        <strong class="updated-label">上次更新時間</strong>
        <time class="entry-date modified" datetime="{MODIFIED}">1 10 月, 2026</time></span></div></div>
        <div class="post-entry blockquote-style-1"><div id="penci-post-entry-inner">{prose}
        <div class="penci-single-link-pages"> </div>
        <div class="post-tags"><a rel="tag" href="/tag/stock/">不是正文的標籤</a></div></div></div>
        <div class="post-author">不能代替當篇作者的介紹</div><div class="post-related">推薦故事</div></article>
        <aside><h1>別篇標題</h1><time>2026-10-20</time></aside></body></html>'''


def parse(settings, html, url=URL):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    return extract_article(html.encode(), source, page_url=url)


def resource_post(*, chart=False, video=False):
    """Synthetic article plus connection hints and optional literal references."""
    soup = BeautifulSoup(page(), 'lxml')
    body = soup.select_one('#penci-post-entry-inner')
    anchor = body.select(':scope > h2')[-1]
    for rel, href in sorted(native.MAX_NATIVE_RESOURCE_HINTS):
        anchor.insert_before(soup.new_tag('link', rel=rel, href=href))
    anchor.insert_before(soup.new_tag('meta', attrs={'http-equiv': 'x-dns-prefetch-control', 'content': 'on'}))
    if chart:
        paragraph = soup.new_tag('p')
        widget = soup.new_tag('script', type='text/javascript', src=native.MAX_CHART_SCRIPT, attrs={'async': ''})
        widget.string = json.dumps({'symbol': 'TEST:SYNTHETIC', 'interval': 'D', 'studies': ['STD;SMA']})
        paragraph.append(widget)
        anchor.insert_before(paragraph)
    if video:
        paragraph = soup.new_tag('p')
        paragraph.append(soup.new_tag('iframe', attrs={
            'src': 'https://www.youtube.com/embed/A1b2C3d4E5F?feature=oembed',
            'title': '合成影片引用，不是新聞正文或轉錄', 'width': '1170', 'height': '658',
            'frameborder': '0', 'referrerpolicy': 'strict-origin-when-cross-origin', 'allowfullscreen': '',
            'allow': 'accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share'}))
        anchor.insert_before(paragraph)
    # Parsed Script strings match the real HTTP parser; an unparsed synthetic
    # NavigableString is intentionally excluded by BeautifulSoup.get_text().
    return BeautifulSoup(str(soup), 'lxml')


@pytest.mark.parametrize('prior', range(1, 9))
def test_connection_hints_require_reviewed_v9_without_relaxing_earlier_captures(settings, prior):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(prior)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(resource_post()))


@pytest.mark.parametrize('chart,video', [(False, False), (True, False), (False, True), (True, True)])
def test_v9_reference_metadata_preserves_prose_numbers_and_never_fetches_media(settings, monkeypatch, chart, video):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v9'
    reference = parse(settings, page())
    soup = resource_post(chart=chart, video=video)
    widget = soup.select_one('#penci-post-entry-inner script')
    if widget:
        digest = hashlib.sha256(widget.get_text().encode()).hexdigest()
        monkeypatch.setattr(native, 'MAX_CHART_OPTIONS_SHA256', frozenset({digest}))
    original = str(soup)
    result = parse(settings, original)
    assert result['body'] == reference['body'] and result['body_sha256'] == reference['body_sha256']
    for key in ('published_at_raw', 'modified_at_raw', 'title', 'authors_json', 'author_attribution_detail_json'):
        assert result[key] == reference[key]
    notices = [n for n in json.loads(result['verified_article_notices_json']) if 'external_resource_requests' in n]
    assert len(notices) == 7 + chart + video
    assert all(not n['external_resource_requests'] and not n['resource_executed']
               and n['original_response_unchanged'] for n in notices)
    assert len([n for n in notices if n['kind'] == 'native_connection_hint_not_article_prose']) == 7
    if chart:
        notice = next(n for n in notices if n['kind'] == 'native_external_chart_reference_not_article_prose')
        assert notice['source_widget_options'] == json.loads(widget.get_text())
        assert notice['source_options_sha256'] == digest and not notice['market_series_observed']
        assert 'TEST:SYNTHETIC' not in result['body']
    if video:
        notice = next(n for n in notices if n['kind'] == 'native_external_video_reference_not_article_prose')
        assert notice['source_attributes'] == soup.iframe.attrs and not notice['video_or_transcript_captured']
        assert '合成影片引用' not in result['body']
    assert ('no_media_payload_transcript_or_market_series_captured' in result['body_completeness']) == bool(chart or video)
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert str(soup) == original  # Extractor never mutates the caller's raw HTML.
    settings['providers'][0]['html_parser'] = 'html.parser'
    assert parse(settings, original) == result


@pytest.mark.parametrize('mutation', ['href', 'rel', 'event', 'refresh', 'content', 'nested', 'duplicate', 'hidden'])
def test_v9_connection_hint_is_not_arbitrary_metadata_or_resource_bypass(settings, mutation):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v9'
    soup = resource_post()
    body = soup.select_one('#penci-post-entry-inner')
    node = body.link
    if mutation == 'href':
        node['href'] = 'https://unreviewed.invalid/content'
    elif mutation == 'rel':
        node['rel'] = ['stylesheet']
    elif mutation == 'event':
        node['onload'] = 'execute()'
    elif mutation == 'refresh':
        body.meta['http-equiv'] = 'refresh'
    elif mutation == 'content':
        body.meta['content'] = 'unreviewed'
    elif mutation == 'nested':
        node.wrap(soup.new_tag('div'))
    elif mutation == 'duplicate':
        body.append(deepcopy(node))
    else:
        body['hidden'] = ''
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(soup))


@pytest.mark.parametrize('mutation', ['src', 'type', 'extra', 'changed_options', 'not_json', 'json_array', 'hidden'])
def test_v9_chart_reference_never_accepts_unknown_or_executable_inline_content(settings, monkeypatch, mutation):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v9'
    soup = resource_post(chart=True)
    node = soup.select_one('#penci-post-entry-inner script')
    digest = hashlib.sha256(node.get_text().encode()).hexdigest()
    monkeypatch.setattr(native, 'MAX_CHART_OPTIONS_SHA256', frozenset({digest}))
    if mutation == 'src':
        node['src'] += '?unreviewed=1'
    elif mutation == 'type':
        node['type'] = 'application/ld+json'
    elif mutation == 'extra':
        node['data-extra'] = 'unreviewed'
    elif mutation == 'changed_options':
        node.string = node.get_text() + ' '
    elif mutation in {'not_json', 'json_array'}:
        node.string = 'alert(1)' if mutation == 'not_json' else '[]'
        monkeypatch.setattr(native, 'MAX_CHART_OPTIONS_SHA256', frozenset({hashlib.sha256(node.get_text().encode()).hexdigest()}))
    else:
        node.parent['aria-hidden'] = 'true'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(soup))


@pytest.mark.parametrize('field,value', [('src', 'https://unreviewed.invalid/embed/A1b2C3d4E5F'),
    ('src', 'https://www.youtube.com/embed/A1b2C3d4E5F?autoplay=1'), ('srcdoc', 'not article prose'),
    ('allow', 'unreviewed'), ('title', ''), ('height', '0'), ('hidden', ''), ('onload', 'execute()')])
def test_v9_video_reference_does_not_enable_arbitrary_frames(settings, field, value):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v9'
    soup = resource_post(video=True)
    soup.iframe[field] = value
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(soup))


def test_v9_reference_only_page_is_not_a_complete_body(settings):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v9'
    soup = resource_post(video=True)
    body = soup.select_one('#penci-post-entry-inner')
    frame = soup.iframe.extract()
    body.clear()
    paragraph = soup.new_tag('p')
    paragraph.append(frame)
    body.append(paragraph)
    with pytest.raises(NewsError, match='body_not_found'):
        parse(settings, str(soup))


@pytest.mark.parametrize('wrapped', [False, True])
@pytest.mark.parametrize('author,profile', [('MAX', '/author/ping/'), ('MAX 投資研究團隊', '/author/max-research-team/')])
def test_native_text_tables_values_roles_and_revision(settings, wrapped, author, profile):
    html = page(wrapped=wrapped, author=author, profile=profile)
    result = parse(settings, html)
    assert result['title'] == '合成原生可見標題'
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert 'not_original_historical_body' in result['published_time_basis']
    assert json.loads(result['authors_json']) == [author]
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['native_profile_url'] == MAX_ORIGIN + profile
    assert detail['operator_is_not_inferred_author'] and not detail['named_reporter_independently_verified']
    assert result['body'].count('完整合成研究細節與風險揭露。') == 48
    for value in ('-987.65', '6.50%', '1,234', '來源聲明不是實際報酬'):
        assert value in result['body']
    for furniture in ('照片說明', '別篇連結', '測試目錄', '標籤', '推薦故事', '頁首導覽'):
        assert furniture not in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices[0]['visible_header'] != notices[0]['schema_headline']
    assert json.loads(result['verified_image_captions_json'])[0]['text'] == '獨立照片說明'
    assert '/never-fetch-photo.png' in html  # Raw source is retained independently.


def test_supported_html_engines_preserve_values(settings):
    reference = parse(settings, page())
    settings['providers'][0]['html_parser'] = 'html.parser'
    assert parse(settings, page()) == reference


def unchanged_post():
    html = page().replace(MODIFIED, PUBLISHED)
    return re.sub(r'<strong class="updated-label">.*?</strong>\s*<time class="entry-date modified"[^>]*>.*?</time>', '', html)


def schema_only_post():
    soup = BeautifulSoup(unchanged_post(), 'lxml')
    soup.select_one('meta[property="article:modified_time"]').decompose()
    hidden = BeautifulSoup('<div class="penci-hide-tagupdated"><time class="entry-date updated" '
        'datetime="2022-10-01T12:34:56+08:00">1 10 月, 2022</time></div>', 'lxml').div
    soup.select_one('.single-header').insert(0, hidden)
    return str(soup)


def auxiliary_header_post():
    soup = BeautifulSoup(schema_only_post(), 'lxml')
    node = soup.new_tag('span')
    node.string = '隱藏模板輔助標記不是正文'
    soup.select_one('.penci-hide-tagupdated').insert(0, node)
    return str(soup)


def duplicate_publication_template_post():
    soup = BeautifulSoup(auxiliary_header_post(), 'lxml')
    hidden = soup.select_one('.penci-hide-tagupdated > time')
    hidden['class'] = ['entry-date', 'published']
    hidden['datetime'] = PUBLISHED
    hidden.string = '14 2 月, 2023'
    return str(soup)


def presentation_post(css='.synthetic-source-table{border-collapse:collapse;color:#111827}'):
    soup = BeautifulSoup(duplicate_publication_template_post(), 'lxml')
    style = soup.new_tag('style', type='text/css')
    style.string = css
    soup.select_one('#penci-post-entry-inner').insert(0, style)
    return str(soup)


def test_v6_presentation_is_separate_metadata_not_prose_or_removed_financial_values(settings, monkeypatch):
    css = '.synthetic-source-table{border-collapse:collapse;color:#111827}'
    digest = hashlib.sha256(css.encode()).hexdigest()
    monkeypatch.setattr(native, 'MAX_PRESENTATION_CSS_SHA256', frozenset({digest}))
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v5'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, presentation_post(css))
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v6'
    result = parse(settings, presentation_post(css))
    assert css not in result['body'] and 'border-collapse' not in result['body']
    assert all(value in result['body'] for value in ('-987.65', '6.50%', '1,234'))
    assert result['published_at_raw'] == result['modified_at_raw'] == PUBLISHED
    notice = next(n for n in json.loads(result['verified_article_notices_json'])
                  if n['kind'] == 'native_presentation_stylesheet_not_article_prose')
    assert notice['source_css_sha256'] == digest and notice['original_response_unchanged']
    assert not notice['style_executed'] and notice['style_external_resource_requests'] == 0
    settings['providers'][0]['html_parser'] = 'html.parser'
    assert parse(settings, presentation_post(css)) == result


@pytest.mark.parametrize('change', ['unknown_sheet', 'changed_sheet', 'unexpected_attribute',
                                  'wrong_mime', 'hidden_ancestor', 'script', 'too_many', 'short_body'])
def test_v6_no_generic_style_skip_or_metadata_padded_body(settings, monkeypatch, change):
    css = '.synthetic-source-table{color:#111827}'
    digest = hashlib.sha256(css.encode()).hexdigest()
    monkeypatch.setattr(native, 'MAX_PRESENTATION_CSS_SHA256', frozenset({digest}))
    soup = BeautifulSoup(presentation_post(css), 'lxml')
    style = soup.style
    if change == 'unknown_sheet':
        style.string = '.story{display:none} .story:before{content:"not source prose"}'
    elif change == 'changed_sheet':
        style.string = css + ' '
    elif change == 'unexpected_attribute':
        style['data-unreviewed'] = 'x'
    elif change == 'wrong_mime':
        style['type'] = 'text/unknown'
    elif change == 'hidden_ancestor':
        parent = soup.new_tag('div', hidden=True)
        style.wrap(parent)
    elif change == 'script':
        script = soup.new_tag('script')
        script.string = 'not prose or an accepted stylesheet'
        style.insert_after(script)
    elif change == 'too_many':
        for _ in range(8):
            style.insert_after(BeautifulSoup(str(style), 'lxml').style)
    else:
        body = soup.select_one('#penci-post-entry-inner')
        body.clear()
        body.append(style)
        p = soup.new_tag('p')
        p.string = '真正正文太短'
        body.append(p)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v6'
    with pytest.raises(NewsError):
        parse(settings, str(soup))


@pytest.mark.parametrize('prior,fixture', [(1, page), (2, unchanged_post), (3, schema_only_post),
                                         (4, auxiliary_header_post), (5, duplicate_publication_template_post)])
def test_v6_does_not_rewrite_prior_body_metadata_or_source_scope(settings, prior, fixture):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(prior)
    old = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    result = parse(settings, fixture())
    candidate = max_candidate_source(settings, PolicyRegistry(settings, base=Path.cwd()), Path.cwd(), 'max_visible_post_v6')
    assert max_native_clock_parser_repair(candidate, old.data_fingerprint)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v6'
    assert parse(settings, fixture()) == result
    settings['providers'][0]['authorization']['scope'] += ' Changed unrelated entitlement.'
    assert not max_native_clock_parser_repair(PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife'], old.data_fingerprint)


@pytest.mark.parametrize('version', [7, 8])
def test_native_body_header_is_prose_only_after_reviewed_v8(settings, version):
    soup = BeautifulSoup(page(), 'lxml')
    introduction = soup.new_tag('header')
    introduction.append(soup.select_one('#penci-post-entry-inner > h1').extract())
    paragraph = soup.new_tag('p')
    paragraph.string = '來源正文序言與原始數值 -12.34%、5,678 股，不能當成網站導覽刪除。'
    introduction.append(paragraph)
    soup.select_one('#penci-post-entry-inner').insert(0, introduction)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(version)
    if version == 7:
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            parse(settings, str(soup))
    else:
        result = parse(settings, str(soup))
        assert paragraph.get_text() in result['body'] and '正文另有原生標題' in result['body']
        assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
        settings['providers'][0]['html_parser'] = 'html.parser'
        assert parse(settings, str(soup)) == result
        introduction['hidden'] = ''
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            parse(settings, str(soup))


@pytest.mark.parametrize('tag', ['script', 'iframe', 'meta', 'link', 'footer', 'form'])
def test_v8_native_header_is_not_a_generic_embed_or_metadata_bypass(settings, tag):
    soup = BeautifulSoup(page(), 'lxml')
    introduction = soup.new_tag('header')
    introduction.append(soup.new_tag(tag))
    soup.select_one('#penci-post-entry-inner').insert(0, introduction)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v8'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(soup))


@pytest.mark.parametrize('scoped', [False, True])
def test_v7_reviewed_presentation_stays_metadata_and_keeps_values(settings, monkeypatch, scoped):
    css = '.synthetic-native{font-size:17px;line-height:1.9}'
    digest = hashlib.sha256(css.encode()).hexdigest()
    monkeypatch.setattr(native, 'MAX_V7_SCOPED_PRESENTATION_CSS_SHA256' if scoped else
                        'MAX_V7_PRESENTATION_CSS_SHA256', frozenset({digest}))
    soup = BeautifulSoup(presentation_post(css), 'lxml')
    if scoped:
        soup.style.attrs = {'scoped': ''}
    html = str(soup)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v6'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v7'
    result = parse(settings, html)
    assert all(value in result['body'] for value in ('-987.65', '6.50%', '1,234'))
    assert css not in result['body'] and result['published_at_raw'] == PUBLISHED
    notice = next(n for n in json.loads(result['verified_article_notices_json'])
                  if n['kind'] == 'native_presentation_stylesheet_not_article_prose')
    assert notice['source_css_sha256'] == digest and not notice['style_executed']
    settings['providers'][0]['html_parser'] = 'html.parser'
    assert parse(settings, html) == result
    soup.style['scoped' if not scoped else 'data-unreviewed'] = 'not-a-boolean'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(soup))


@pytest.mark.parametrize('prior,fixture,next_version', [
    (prior, fixture, next_version)
    for next_version in (7, 8, 9)
    for prior, fixture in ((1, page), (2, unchanged_post), (3, schema_only_post),
                           (4, auxiliary_header_post), (5, duplicate_publication_template_post),
                           (6, duplicate_publication_template_post), (7, duplicate_publication_template_post),
                           (8, duplicate_publication_template_post))
    if prior < next_version])
def test_v7_preserves_previous_bodies_notices_clocks_auth_and_http(settings, prior, fixture, next_version):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(prior)
    old = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    result = parse(settings, fixture())
    candidate = max_candidate_source(settings, PolicyRegistry(settings, base=Path.cwd()), Path.cwd(), 'max_visible_post_v' + str(next_version))
    assert max_native_clock_parser_repair(candidate, old.data_fingerprint)
    assert candidate.authorization_fingerprint == old.authorization_fingerprint
    assert candidate.http_capacity_source_fingerprint == (old.http_capacity_source_fingerprint or old.data_fingerprint)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(next_version)
    assert parse(settings, fixture()) == result
    settings['providers'][0]['authorization']['scope'] += ' Independent changed scope.'
    assert not max_native_clock_parser_repair(PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife'], old.data_fingerprint)


def test_v7_preserves_literal_schema_title_variants_without_guessing_equivalence(settings):
    soup = BeautifulSoup(page(), 'lxml')
    script = soup.select_one('script[type="application/ld+json"]')
    graph = json.loads(script.get_text())
    graph['@graph'][1]['name'] = '來源另列 WebPage 名稱'
    graph['@graph'][2]['headline'] = '來源另列 BlogPosting 標題'
    script.string = json.dumps(graph, ensure_ascii=False)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v6'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(soup))
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v7'
    result = parse(settings, str(soup))
    notice = json.loads(result['verified_article_notices_json'])[0]
    assert notice['schema_webpage_name'] == graph['@graph'][1]['name']
    assert notice['schema_headline'] == graph['@graph'][2]['headline']
    assert notice['machine_headline_variants_present'] and not notice['semantic_title_equivalence_inferred']
    assert result['title'] == notice['visible_header']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    assert not result['historical_point_in_time'] and '-987.65' in result['body']
    settings['providers'][0]['html_parser'] = 'html.parser'
    assert parse(settings, str(soup)) == result


@pytest.mark.parametrize('field,value', [('name', None), ('name', ' '), ('name', {}),
                                      ('headline', 'x' * 2001), ('headline', []),
                                      ('url', MAX_ORIGIN + '/different/'),
                                      ('@id', URL + '#wrong-webpage'),
                                      ('mainEntityOfPage', {'@id': URL + '#different'}),
                                      ('isPartOf', {'@id': MAX_ORIGIN + '/other/'})])
def test_v7_variants_never_relax_url_graph_or_title_presence(settings, field, value):
    soup = BeautifulSoup(page(), 'lxml')
    script = soup.select_one('script[type="application/ld+json"]')
    graph = json.loads(script.get_text())
    graph['@graph'][1 if field in {'name', 'url', '@id'} else 2][field] = value
    script.string = json.dumps(graph)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v7'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, str(soup))


@pytest.mark.parametrize('version', [7, 8, 9])
def test_v7_v8_defer_eight_heads_but_v9_retries_the_reviewed_resource_repair(settings, tmp_path, version):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(version - 1)
    old = PolicyRegistry(settings, base=tmp_path)
    queue = NewsQueue(tmp_path, old)
    urls = [MAX_ORIGIN + path for path in sorted(native.MAX_V7_DEFERRED_EMBED_PATHS)]
    assert len(urls) == 8
    eligible = MAX_ORIGIN + '/eligible-css-repair/'
    try:
        queue.add_articles([{'url': url} for url in [*urls, eligible]])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                             "attempts=6,due=0,last_checked_at_utc='2026-10-20T11:27:32+00:00'")
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    finally:
        queue.close()
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(version)
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        expected = deepcopy(before)
        expected[eligible]['state'] = 'retry'
        if version == 9:
            for url in urls:
                expected[url]['state'] = 'retry'
        assert after == expected
        key = 'max_native_clock_parser_repair:v' + str(version) + ':' + queue.registry.sources['maxfinanciallife'].data_fingerprint
        marker = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
        assert marker['url_count'] == (9 if version == 9 else 1)
        assert marker.get('known_unimplemented_embed_heads_deferred', 0) == (0 if version == 9 else 8)
        assert not marker['source_refusals_stored_heads_future_deadlines_and_budgets_reset']
    finally:
        queue.close()


def test_v5_hidden_duplicate_publication_is_not_a_fictional_update(settings):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v4'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, duplicate_publication_template_post())
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v5'
    result = parse(settings, duplicate_publication_template_post())
    assert result['published_at_raw'] == result['modified_at_raw'] == PUBLISHED
    notice = next(n for n in json.loads(result['verified_article_notices_json'])
                  if n['kind'] == 'native_hidden_update_template_not_a_publication_clock')
    assert notice['duplicate_publication_not_a_separate_update']
    assert not notice['used_as_published_or_modified_time']
    assert not result['historical_point_in_time']
    assert '隱藏模板輔助標記' not in result['body']
    assert '-987.65' in result['body'] and '6.50%' in result['body']


@pytest.mark.parametrize('change', ['duplicate', 'nested', 'different_datetime', 'different_text',
                                  'mixed_role', 'unknown_role', 'schema_conflict'])
def test_v5_duplicate_template_still_requires_exact_source_clocks_and_shape(settings, change):
    soup = BeautifulSoup(duplicate_publication_template_post(), 'lxml')
    hidden = soup.select_one('.penci-hide-tagupdated > time')
    if change == 'duplicate':
        hidden.insert_after(BeautifulSoup(str(hidden), 'lxml').time)
    elif change == 'nested':
        hidden.wrap(soup.new_tag('span'))
    elif change == 'different_datetime':
        hidden['datetime'] = MODIFIED
    elif change == 'different_text':
        hidden.string = '1 10 月, 2026'
    elif change == 'mixed_role':
        hidden['class'] = ['entry-date', 'published', 'updated']
    elif change == 'unknown_role':
        hidden['class'] = ['entry-date', 'unknown']
    else:
        node = soup.select_one('script[type="application/ld+json"]')
        graph = json.loads(node.string)
        graph['@graph'][2]['dateModified'] = MODIFIED
        node.string = json.dumps(graph, ensure_ascii=False)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v5'
    with pytest.raises(NewsError):
        parse(settings, str(soup))


@pytest.mark.parametrize('prior,fixture', [(1, page), (2, unchanged_post), (3, schema_only_post),
                                         (4, auxiliary_header_post)])
def test_v5_keeps_every_prior_native_record_unchanged(settings, prior, fixture):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(prior)
    original = parse(settings, fixture())
    old = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    candidate = max_candidate_source(settings, PolicyRegistry(settings, base=Path.cwd()), Path.cwd(), 'max_visible_post_v5')
    assert max_native_clock_parser_repair(candidate, old.data_fingerprint)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v5'
    assert parse(settings, fixture()) == original


def test_v4_auxiliary_hidden_header_markup_is_not_a_body_or_release_clock(settings):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v3'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, auxiliary_header_post())
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v4'
    result = parse(settings, auxiliary_header_post())
    assert result['published_at_raw'] == result['modified_at_raw'] == PUBLISHED
    assert not result['historical_point_in_time'] and '隱藏模板輔助標記' not in result['body']
    notice = next(n for n in json.loads(result['verified_article_notices_json'])
                  if n['kind'] == 'native_hidden_update_template_not_a_publication_clock')
    assert notice['auxiliary_header_elements'] == 1 and not notice['used_as_published_or_modified_time']


@pytest.mark.parametrize('prior_version', [1, 2, 3])
def test_v4_predecessors_preserve_exact_http_scope_and_cannot_inherit_changed_rights(settings, prior_version):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(prior_version)
    registry = PolicyRegistry(settings, base=Path.cwd())
    old = registry.sources['maxfinanciallife']
    candidate = max_candidate_source(settings, registry, Path.cwd(), 'max_visible_post_v4')
    assert max_native_clock_parser_repair(candidate, old.data_fingerprint)
    assert candidate.http_capacity_source_fingerprint == (old.http_capacity_source_fingerprint or old.data_fingerprint)
    changed = deepcopy(settings)
    changed['providers'][0]['embedded_article_layout'] = 'max_visible_post_v4'
    changed['providers'][0]['authorization']['scope'] += ' Changed independent rights.'
    assert not max_native_clock_parser_repair(PolicyRegistry(changed, base=Path.cwd()).sources['maxfinanciallife'], old.data_fingerprint)


@pytest.mark.parametrize('prior,candidate_layout,fixture', [(3, 'max_visible_post_v4', auxiliary_header_post),
                                                         (4, 'max_visible_post_v5', duplicate_publication_template_post)])
def test_candidate_uses_one_received_envelope_and_never_accepts_or_exports_body(settings, tmp_path, prior, candidate_layout, fixture):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(prior)
    registry = PolicyRegistry(settings, base=Path.cwd())
    source = registry.sources['maxfinanciallife']
    candidate = max_candidate_source(settings, registry, Path.cwd(), candidate_layout)
    payload = fixture().encode()
    calls = []

    def get(url):
        calls.append(url)
        return Response(url, 200, {'content-type': 'text/html'}, payload)

    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={}, get=get)
    results = probe_provider(http, [URL], candidate_source=candidate)
    assert calls == [URL] and len(results) == 1
    assert results[0]['extraction_error'] == 'article_identity_not_verified'
    result = results[0]['candidate_parser']
    assert result['parse_success'] and result['published_at_raw'] == PUBLISHED
    assert not result['accepted_as_article'] and not result['raw_response_retained']
    assert not result['body_text_exported'] and not result['production_queue_writes']
    assert result['additional_publisher_requests'] == 0
    assert '完整合成研究細節' not in json.dumps(result, ensure_ascii=False)
    assert not list(tmp_path.iterdir())
    conflict = payload.replace(PUBLISHED.encode(), b'not-a-source-clock', 1)
    assert not compare_native_candidate(conflict, source, candidate, page_url=URL)['parse_success']


@pytest.mark.parametrize('bad_layout', ['max_visible_post_v1', 'max_visible_post_v3', 'unreviewed'])
def test_unreviewed_candidate_layout_is_not_a_live_source_or_request_grant(settings, bad_layout):
    with pytest.raises(ValueError, match='reviewed_max_predecessor_candidate_required'):
        max_candidate_source(settings, PolicyRegistry(settings, base=Path.cwd()), Path.cwd(), bad_layout)


def test_v3_exact_schema_revision_without_head_tag_does_not_backdate_hidden_theme_clock(settings):
    html = schema_only_post()
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html)
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v3'
    result = parse(settings, html)
    assert result['published_at_raw'] == result['modified_at_raw'] == PUBLISHED
    assert not result['historical_point_in_time']
    notices = json.loads(result['verified_article_notices_json'])
    revision = next(n for n in notices if n['kind'] == 'native_revision_from_two_equal_schema_clocks')
    assert not revision['head_modified_meta_present'] and not revision['visible_modified_label_present']
    hidden = next(n for n in notices if n['kind'] == 'native_hidden_update_template_not_a_publication_clock')
    assert hidden['datetime_attribute'] == '2022-10-01T12:34:56+08:00'
    assert not hidden['used_as_published_or_modified_time'] and not hidden['public_visibility_verified']
    assert '1 10 月, 2022' not in result['body']
    shape = max_native_clock_metadata(BeautifulSoup(html, 'lxml'), PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife'])
    assert shape['contract_version'] == 2 and shape['head_clock_count'] == 1
    assert shape['header_times'][0]['in_native_hidden_update_template']
    assert not shape['public_visibility_inferred']


@pytest.mark.parametrize('change', ['schema_mismatch', 'schema_later_revision', 'missing_template',
                                  'template_extra_prose', 'duplicate_published', 'bad_publication'])
def test_v3_schema_only_revision_still_requires_exact_verified_native_shape_and_clocks(settings, change):
    soup = BeautifulSoup(schema_only_post(), 'lxml')
    if change.startswith('schema_'):
        script = soup.select_one('script[type="application/ld+json"]')
        graph = json.loads(script.string)
        graph['@graph'][2]['dateModified'] = MODIFIED
        if change == 'schema_later_revision':
            graph['@graph'][1]['dateModified'] = MODIFIED
        script.string = json.dumps(graph, ensure_ascii=False)
    elif change == 'missing_template':
        soup.select_one('.penci-hide-tagupdated').decompose()
    elif change == 'template_extra_prose':
        soup.select_one('.penci-hide-tagupdated').append('不能忽略的文字')
    elif change == 'duplicate_published':
        soup.select_one('.post-box-meta-single').append(BeautifulSoup(str(soup.select_one('time.published')), 'lxml').time)
    else:
        soup.select_one('time.published')['datetime'] = '2022-10-01T12:34:56+08:00'
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v3'
    with pytest.raises(NewsError):
        parse(settings, str(soup))


@pytest.mark.parametrize('prior_version', [1, 2])
def test_v3_clock_repair_uses_exact_v1_or_v2_predecessor_and_same_capacity(settings, prior_version):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(prior_version)
    old = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v3'
    new = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    assert max_native_clock_parser_repair(new, old.data_fingerprint)
    assert new.http_capacity_source_fingerprint == (old.http_capacity_source_fingerprint or old.data_fingerprint)
    assert parse(settings, page()) == parse({**settings, 'providers': [{**settings['providers'][0],
        'embedded_article_layout': 'max_visible_post_v' + str(prior_version)}]}, page())
    settings['providers'][0]['authorization']['scope'] += ' Changed independent policy.'
    changed = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    assert not max_native_clock_parser_repair(changed, old.data_fingerprint)


def test_v2_unmodified_native_post_needs_no_invented_visible_update(settings):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, unchanged_post())  # Preserve the original v1 evidence.
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v2'
    result = parse(settings, unchanged_post())
    assert result['published_at_raw'] == result['modified_at_raw'] == PUBLISHED
    notices = json.loads(result['verified_article_notices_json'])
    assert notices[-1]['kind'] == 'native_no_separate_revision_label'
    assert 'no_update_label_or_time_invented' in notices[-1]['basis']
    assert '-987.65' in result['body'] and not result['historical_point_in_time']


def test_no_update_label_cannot_hide_a_real_revision_or_clock_conflict(settings):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v2'
    html = re.sub(r'<time class="entry-date modified"[^>]*>.*?</time>', '', page())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, html)
    html = unchanged_post().replace('"dateModified": "' + PUBLISHED, '"dateModified": "' + MODIFIED, 1)
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(settings, html)


def test_v2_keeps_old_layout_values_and_http_identity(settings):
    old = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    reference = parse(settings, page())
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v2'
    new = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    assert parse(settings, page()) == reference
    assert new.data_fingerprint != old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert max_native_clock_parser_repair(new, old.data_fingerprint)
    assert not max_native_clock_parser_repair(new, 'f' * 64)


@pytest.mark.parametrize('prior_version,new_version', [(1, 2), (1, 3), (2, 3), (1, 4), (2, 4), (3, 4),
                                                    (1, 5), (2, 5), (3, 5), (4, 5),
                                                    (1, 6), (2, 6), (3, 6), (4, 6), (5, 6),
                                                    (1, 7), (2, 7), (3, 7), (4, 7), (5, 7), (6, 7),
                                                    (1, 8), (2, 8), (3, 8), (4, 8), (5, 8), (6, 8), (7, 8),
                                                    *[(prior, 9) for prior in range(1, 9)]])
def test_clock_repair_requeues_only_due_missing_predecessor_failures_without_erasing_evidence(settings, tmp_path, prior_version, new_version):
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(prior_version)
    old = PolicyRegistry(settings, base=tmp_path)
    queue = NewsQueue(tmp_path, old)
    names = ('eligible', 'future', 'refused', 'stored', 'body-failure', 'clock-failure', 'leased')
    try:
        queue.add_articles([{'url': MAX_ORIGIN + '/' + name + '/'} for name in names])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                             "attempts=2,due=0,last_checked_at_utc='2026-10-20T10:06:23+00:00'")
            queue.db.execute("UPDATE articles SET due=? WHERE url=?", (time.time() + 86400, MAX_ORIGIN + '/future/'))
            queue.db.execute("UPDATE articles SET state='robots_disallowed',error_code='robots_disallowed' WHERE url=?", (MAX_ORIGIN + '/refused/',))
            queue.db.execute("UPDATE articles SET body_sha256=? WHERE url=?", ('a' * 64, MAX_ORIGIN + '/stored/'))
            for name, state in [('body-failure', 'body_not_found'), ('clock-failure', 'invalid_bulk_publication_clock')]:
                queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (state, state, MAX_ORIGIN + '/' + name + '/'))
            queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=? WHERE url=?", (time.time() + 86400, MAX_ORIGIN + '/leased/'))
            queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:maxfinanciallife:20261020','2')")
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT url,state,attempts,error_code,due,body_sha256,last_checked_at_utc,lease_owner,lease_until FROM articles')}
    finally:
        queue.close()
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v' + str(new_version)
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT url,state,attempts,error_code,due,body_sha256,last_checked_at_utc,lease_owner,lease_until FROM articles')}
        expected = deepcopy(before)
        expected[MAX_ORIGIN + '/eligible/']['state'] = 'retry'
        assert after == expected
        marker_key = 'max_native_clock_parser_repair:v' + str(new_version) + ':' + queue.registry.sources['maxfinanciallife'].data_fingerprint
        marker = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker_key,)).fetchall()
        assert len(marker) == 1 and json.loads(marker[0][0])['url_count'] == 1
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_identity_metadata_attempts:v1:maxfinanciallife:20261020'").fetchone()[0] == '2'
    finally:
        queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        resumed = {r['url']: dict(r) for r in queue.db.execute('SELECT url,state,attempts,error_code,due,body_sha256,last_checked_at_utc,lease_owner,lease_until FROM articles')}
        assert resumed == after
        assert queue.db.execute('SELECT count(*) FROM queue_settings WHERE key=?', (marker_key,)).fetchone()[0] == 1
    finally:
        queue.close()


def test_changed_rights_never_inherits_v1_clock_recovery(settings):
    old = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    settings['providers'][0]['embedded_article_layout'] = 'max_visible_post_v2'
    settings['providers'][0]['authorization']['scope'] += ' An independent changed policy.'
    new = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    assert not max_native_clock_parser_repair(new, old.data_fingerprint)


@pytest.mark.parametrize('old,new,code', [
    ('id="post-64438"', 'id="unknown-64438"', 'article_identity_not_verified'),
    ('class="post type-post status-publish"', 'class="type-page"', 'article_identity_not_verified'),
    ('rel="canonical"', 'rel="other"', 'article_identity_not_verified'),
    ('property="og:url"', 'property="unverified:url"', 'article_identity_not_verified'),
    ('class="single-header"', 'class="single-header" hidden', 'article_identity_not_verified'),
    ('class="post-entry blockquote-style-1"', 'class="post-entry blockquote-style-1" hidden', 'article_identity_not_verified'),
    ('id="penci-post-entry-inner"', 'id="penci-post-entry-inner" style="display:none"', 'article_identity_not_verified'),
    ('/author/ping/', '/author/unreviewed/', 'article_attribution_not_verified'),
    ('MAX 投資研究團隊', '外部轉載作者', 'article_attribution_not_verified'),
    ('>14 2 月, 2023<', '>13 2 月, 2023<', 'invalid_bulk_publication_clock'),
    (PUBLISHED, '2023-02-30T15:33:03+08:00', 'invalid_bulk_publication_clock'),
    (PUBLISHED, '2023-02-14T15:33:03', 'invalid_bulk_publication_clock'),
    (MODIFIED, '2022-10-01T10:42:08+08:00', 'invalid_bulk_publication_clock'),
    ('第2段原稿', '<span hidden>第2段原稿</span>', 'article_identity_not_verified'),
    ('<td>-987.65</td>', '<td><iframe>不可丟棄內容</iframe></td>', 'article_identity_not_verified'),
    ('<p>第0段原稿', '<p>轉載自外部媒體：第0段原稿', 'article_rights_or_scope_excluded'),
    ('href="#section-one"', 'href="/not-a-toc/"', 'article_identity_not_verified'),
    ('<figcaption>', '<p>不能當照片丟棄的正文</p><figcaption>', 'article_identity_not_verified'),
    ('<div class="penci-single-link-pages"> ', '<div class="penci-single-link-pages"><a href="/page/2/">第二頁</a>', 'body_not_found'),
])
def test_drift_no_silent_truncation_or_inferred_credit(settings, old, new, code):
    html = page()
    if old == 'class="single-header"':
        old, new = 'header-classic single-header"', 'header-classic single-header" hidden'
    assert old in html
    with pytest.raises(NewsError, match=code):
        parse(settings, html.replace(old, new))


@pytest.mark.parametrize('url', [URL + '?guess=1', URL + '#other', URL.replace('https:', 'http:'),
    URL.replace('maxfinanciallife.com', 'www.maxfinanciallife.com'), MAX_ORIGIN + '/other-title/',
    MAX_ORIGIN + '/a%2Fb/', MAX_ORIGIN + '/about/'])
def test_literal_url_identity_no_guessed_alias(settings, url):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(settings, page(), url=url)


@pytest.mark.parametrize('target', ['WebPage', 'BlogPosting'])
def test_schema_clock_must_match_native_header(settings, target):
    html = page()
    marker = f'"@type": "{target}"'
    start = html.index(marker)
    before, after = html[:start], html[start:]
    after = after.replace(PUBLISHED, '2023-02-13T15:33:03+08:00', 1)
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(settings, before + after)


def test_sitemap_only_native_post_leaves_no_theme_page_or_taxonomy_polling(settings):
    source = PolicyRegistry(settings, base=Path.cwd()).sources['maxfinanciallife']
    urls = [MAX_ORIGIN + '/' + name for name in ('post-sitemap1.xml', 'post-sitemap4.xml',
        'page-sitemap.xml', 'penci-block-sitemap.xml', 'category-sitemap.xml', 'post-sitemap1.xml?retry=1')]
    xml = '<sitemapindex>' + ''.join(f'<sitemap><loc>{url}</loc></sitemap>' for url in urls) + '</sitemapindex>'
    assert discovery_links(xml.encode(), 'sitemap', source) == [(url, 'sitemap', '') for url in urls[:2]]
    leaf = f'<urlset><url><loc>{MAX_ORIGIN}/</loc></url><url><loc>{URL}</loc><lastmod>{MODIFIED}</lastmod></url></urlset>'
    assert discovery_links(leaf.encode(), 'sitemap', source) == [(URL, '', MODIFIED)]


@pytest.mark.parametrize('field,value', [
    ('domains', ['financiallife.com']), ('acquisition_mode', 'finite'),
    ('article_selectors', ['article']), ('article_path_pattern', '^/.*$'),
    ('exclude_selectors', ['table']), ('author_selectors', ['a']),
    ('permitted_author_names', ['MAX', '外部作者']), ('require_author_attribution', False),
    ('sitemaps', [MAX_ORIGIN + '/category-sitemap.xml']),
    ('feeds', [MAX_ORIGIN + '/feed/']), ('index_pages', [MAX_ORIGIN + '/']),
])
def test_reviewed_adapter_does_not_expand_to_unreviewed_endpoints(settings, field, value):
    settings['providers'][0][field] = value
    with pytest.raises(ValueError, match='unreviewed embedded article parser'):
        PolicyRegistry(settings, base=Path.cwd())


def test_installed_scope_is_private_native_posts_with_old_review_preserved():
    config, registry, _ = load_config(Path('configs/news_fulltext.json'))
    source = registry.sources['maxfinanciallife']
    assert source.allowed and source.acquisition_mode == 'continuous'
    assert source.domains == ('maxfinanciallife.com',) and source.selectors == (MAX_BODY_SELECTOR,)
    assert source.sitemaps == (MAX_SITEMAP,) and source.permitted_author_names == MAX_AUTHORS
    assert source.authorization['kind'] == 'personal_research' and not source.authorization.get('license')
    assert not any((source.feeds, source.wordpress_apis, source.fulltext_feeds, source.index_pages))
    assert not config['publish'] and not config['remote_training_export']
    reviews = load_source_review_ledger(Path('configs/news/source_reviews_20261009.json'))
    review = reviews['maxfinanciallife']
    assert review == reviews['unregistered:maxfinanciallife.com']
    assert review['capture_decision'] == 'reviewed_existing_scope'
    assert review['prior_reviews'][0]['capture_decision'] == 'review_incomplete'
    assert review['prior_reviews'][0]['reviewed_at_utc'] == '2026-10-20T07:53:00+00:00'
