"""Same-response Cnyes canonical metadata, not an alternate fetch route."""
import html
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_news_cnyes_html import BODY, CLOCK, TITLE, URL, short_page, source
from test_cnyes_news_bulk import cfg as cnyes_config


MOBILE = URL.replace('news.cnyes.com', 'm.cnyes.com')
AUTHOR = '來源宣告的測試作者'

# Exact renderer observed in the independently retained public responses, not
# an executable fixture. A different/truncated/error instruction is rejected.
FIZZ_COMPLETION = (
    '$RC=function(b,c,e){c=document.getElementById(c);c.parentNode.removeChild(c);'
    'var a=document.getElementById(b);if(a){b=a.previousSibling;if(e)b.data="$!",'
    'a.setAttribute("data-dgst",e);else{e=b.parentNode;a=b.nextSibling;var f=0;'
    'do{if(a&&8===a.nodeType){var d=a.data;if("/$"===d)if(0===f)break;else f--;'
    'else"$"!==d&&"$?"!==d&&"$!"!==d||f++}d=a.nextSibling;e.removeChild(a);'
    'a=d}while(a);for(;c.firstChild;)e.insertBefore(c.firstChild,a);b.data="$"}'
    'b._reactRetry&&b._reactRetry()}};$RC("B:0","S:0")'
)


def mobile_page(*, body=BODY * 8, visible=None, changes=None, schema_changes=None):
    data = {'author': AUTHOR, **(changes or {})}
    payload = short_page(body=body, visible=visible, change=data, schema_change=schema_changes)
    payload = payload.replace(b'</head>', f'<meta property="og:url" content="{URL}"></head>'.encode())
    header = f'<p><span class="signature">鉅亨網記者{AUTHOR} 台北</span><time datetime="{CLOCK}">2014-01-01 08:00</time></p>'
    return payload.replace(b'</section><div', ('</section>' + header + '<div').encode())


def streamed_mobile_page(**options):
    return mobile_page(**options).replace(
        b'<body><article>',
        b'<body><main><div><!--$?--><template id="B:0"></template>'
        b'<div>loading</div><!--/$--></div></main><div hidden id="S:0"><article>'
    ).replace(b'</article>', ('</article></div><script>' + FIZZ_COMPLETION + '</script>').encode())


@pytest.mark.parametrize('prefix,suffix', [('', '\u00a0'), ('\u00a0', ''), (' ', '\n')])
def test_native_title_outer_whitespace_is_comparison_only(cnyes_config, tmp_path, prefix, suffix):
    title = prefix + TITLE + suffix
    payload = streamed_mobile_page(changes={'title': title}, schema_changes={'headline': title})
    result = extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['title'] == title
    assert result['body'] == BODY * 8
    assert result['published_at_raw'] == CLOCK
    assert 'source_visible_title_outer_whitespace_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('title', [TITLE[:2] + ' ' + TITLE[2:], TITLE + '\u200b', TITLE + '\ufeff', '不同的標題'])
def test_native_title_repair_does_not_rewrite_words_internal_spaces_or_hidden_controls(cnyes_config, tmp_path, title):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(mobile_page(changes={'title': title}, schema_changes={'headline': title}),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('body', [BODY, BODY * 8])
def test_completed_source_stream_is_public_body_not_a_hidden_payload(cnyes_config, tmp_path, body):
    result = extract_article(streamed_mobile_page(body=body), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == body
    assert result['published_at_raw'] == CLOCK
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert 'verified_completed_fizz_boundary' in result['article_identity_basis']


@pytest.mark.parametrize('old,new', [
    (FIZZ_COMPLETION, ''),
    (FIZZ_COMPLETION, FIZZ_COMPLETION + ';alert(1)'),
    ('$RC("B:0","S:0")', '$RC("B:0","S:0","error")'),
    ('$RC("B:0","S:0")', '$RC("B:1","S:0")'),
    ('<!--$?-->', '<!--$!-->'),
    ('<!--/$-->', ''),
    ('<template id="B:0"></template>', '<template id="B:0"></template><template id="B:0"></template>'),
    ('<main>', '<main hidden>'),
    ('<div hidden id="S:0">', '<div hidden id="S:0" style="display:none">'),
    ('<article>', '<article hidden>'),
    ('<span class="signature">', '<span class="signature" hidden>'),
    ('<div id="article-container">', '<div id="article-container" hidden>'),
])
def test_only_exact_completed_visible_destination_may_reveal_a_stream(cnyes_config, tmp_path, old, new):
    payload = streamed_mobile_page().replace(old.encode(), new.encode())
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


def test_revealed_stream_cannot_accept_paid_or_conflicting_declared_article(cnyes_config, tmp_path):
    for changes in ({'payment': 1}, {'newsId': 43}, {'author': '另一篇作者'}):
        with pytest.raises(NewsError):
            extract_article(streamed_mobile_page(changes=changes), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_destination_not_yet_parsed_when_reveal_runs_is_not_public(cnyes_config, tmp_path):
    payload = streamed_mobile_page().replace(b'<template id="B:0"></template>', b'')
    payload = payload.replace(b'</body>', b'<div><!--$?--><template id="B:0"></template><!--/$--></div></body>')
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


def test_reveal_after_interleaved_flight_frames_and_other_segments(cnyes_config, tmp_path):
    payload = streamed_mobile_page().replace(
        ('<script>' + FIZZ_COMPLETION).encode(),
        ('<script>self.__next_f.push([1,"0:[]\\n"])</script>'
         '<div hidden id="S:1"><span>unrelated segment</span></div>'
         '<script>' + FIZZ_COMPLETION).encode())
    result = extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 8
    assert 'unrelated segment' not in result['body']
    assert 'verified_completed_fizz_boundary' in result['article_identity_basis']


def test_completion_before_its_content_exists_cannot_reveal_it(cnyes_config, tmp_path):
    payload = streamed_mobile_page().replace(('<script>' + FIZZ_COMPLETION + '</script>').encode(), b'')
    payload = payload.replace(b'<body>', ('<body><script>' + FIZZ_COMPLETION + '</script>').encode())
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('body', [BODY, BODY * 8])
def test_source_declared_mobile_alias_recovers_exact_current_clock_and_byline(cnyes_config, tmp_path, body):
    result = extract_article(mobile_page(body=body), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == body
    assert result['published_at_raw'] == CLOCK
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert result['declared_canonical_url'] == URL
    assert result['extraction_method'] == 'cnyes_visible_canonical_alias_v1'
    assert result['body_completeness'].startswith('verified_current_article_container')
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['visible_byline_raw'] == f'鉅亨網記者{AUTHOR} 台北'


@pytest.mark.parametrize('base,query', [(MOBILE, 'exp=a'), (MOBILE, 'exp=b'), (URL, 'exp=a')])
@pytest.mark.parametrize('body', [BODY, BODY * 8])
def test_observed_native_uri_requires_the_same_full_source_proof(cnyes_config, tmp_path, base, query, body):
    result = extract_article(mobile_page(body=body), source(cnyes_config, tmp_path), page_url=base + '?' + query)
    assert result['body'] == body and result['published_at_raw'] == CLOCK
    assert f'observed_{query.replace("=", "_")}_request_variant' in result['article_identity_basis']
    for changes in ({'payment': 1}, {'newsId': 43}, {'author': 'another source author'}):
        with pytest.raises(NewsError):
            extract_article(mobile_page(body=body, changes=changes), source(cnyes_config, tmp_path), page_url=base + '?' + query)


@pytest.mark.parametrize('query', ['exp=c', 'exp=a&other=1', 'exp=b&other=1', 'newsId=43', 'token=unknown', 'exp=%61', 'exp=%62'])
def test_unobserved_queries_are_not_silently_normalized_to_an_article(cnyes_config, tmp_path, query):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(mobile_page(), source(cnyes_config, tmp_path), page_url=MOBILE + '?' + query)


@pytest.mark.parametrize('replace_old,replace_new', [
    (f'href="{URL}"', f'href="{URL}0"'),
    (f'content="{URL}"', f'content="{URL}0"'),
    (f'datetime="{CLOCK}"', 'datetime="2014-01-02T00:00:00.000Z"'),
    ('<article>', '<article hidden>'),
    ('<span class="signature">', '<span class="signature" hidden>'),
])
def test_alias_cannot_borrow_identity_time_or_hidden_credits(cnyes_config, tmp_path, replace_old, replace_new):
    payload = mobile_page().replace(replace_old.encode(), replace_new.encode())
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('changes', [
    {'newsId': 43}, {'payment': False}, {'payment': 1}, {'publishAt': True},
    {'publishAt': 1388534401}, {'author': '另一篇作者'}, {'content': '&lt;p&gt;摘要不是正文。&lt;/p&gt;'},
])
def test_alias_requires_literal_current_free_body_declarations(cnyes_config, tmp_path, changes):
    with pytest.raises(NewsError):
        extract_article(mobile_page(changes=changes), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema_changes', [
    {'isAccessibleForFree': False}, {'author': []}, {'datePublished': '2014-01-01T00:00:00'},
    {'datePublished': '2014-01-02T00:00:00.000Z'}, {'headline': '另一篇標題'},
])
def test_alias_never_substitutes_missing_or_conflicting_public_metadata(cnyes_config, tmp_path, schema_changes):
    with pytest.raises(NewsError):
        extract_article(mobile_page(schema_changes=schema_changes), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_alias_never_accepts_visible_teaser_or_unmatched_extra_body(cnyes_config, tmp_path):
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(mobile_page(visible=BODY), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_original_host_keeps_its_existing_fast_body_contract(cnyes_config, tmp_path):
    result = extract_article(mobile_page(), source(cnyes_config, tmp_path), page_url=URL)
    assert result['body'] == BODY * 8
    assert result['extraction_method'] == 'jsonld_article_body'


def test_source_leading_paragraph_marker_is_preserved_despite_schema_omission(cnyes_config, tmp_path):
    body = '\ufeff' + BODY * 8
    result = extract_article(mobile_page(body=body, schema_changes={'articleBody': BODY * 8}),
                             source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == body
    assert 'source_paragraph_format_marker_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('body', [BODY + '\ufeff' + BODY, '\ufeff\ufeff' + BODY])
def test_schema_cannot_drop_in_word_or_repeated_markers(cnyes_config, tmp_path, body):
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(mobile_page(body=body, schema_changes={'articleBody': body.replace('\ufeff', '')}),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


def test_visible_marker_must_still_exist_in_current_complete_content(cnyes_config, tmp_path):
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(mobile_page(body='\ufeff' + BODY, visible=BODY, schema_changes={'articleBody': BODY}),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


FACTSET_PREFIXES = ['根據FactSet最新調查，共8位分析師，對', '綜合評級 - 共有7位分析師給予']


def factset_markup(prefix=FACTSET_PREFIXES[0]):
    return ('<p>' + prefix + '\ufeff測試股(<a href="https://www.cnyes.com/twstock/2353">2353-TW</a>)'
            '原始預估 -12.50；8 位分析師，22 億。</p>')


def factset_page(markup, *, current=None, schema=None, streamed=False):
    current = markup if current is None else current
    schema = markup.replace('\ufeff', '') if schema is None else schema
    builder = streamed_mobile_page if streamed else mobile_page
    payload = builder(body=BODY, changes={'content': html.escape(current)},
                      schema_changes={'articleBody': schema})
    return payload.replace(('<div id="article-container"><p>' + BODY + '</p>').encode(),
                           ('<div id="article-container">' + markup).encode())


@pytest.mark.parametrize('prefix', FACTSET_PREFIXES)
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_factset_comparison_preserves_original_company_marker_and_every_number(cnyes_config, tmp_path, prefix, url, streamed):
    result = extract_article(factset_page(factset_markup(prefix), streamed=streamed),
                             source(cnyes_config, tmp_path), page_url=url)
    assert prefix + '\ufeff測試股(' in result['body']
    assert all(value in result['body'] for value in ['2353-TW', '-12.50', '22 億'])
    assert 'source_factset_company_prefix_marker_comparison_v1' in result['article_identity_basis']
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]


@pytest.mark.parametrize('mutation', ['unknown_prefix', 'in_word', 'repeated', 'wrong_href', 'missing_link',
    'current_link_conflict', 'current_marker_missing', 'schema_number_conflict', 'duplicate_paragraph', 'hidden_link'])
def test_factset_exception_never_repairs_words_numbers_unmatched_current_content_or_hidden_links(cnyes_config, tmp_path, mutation):
    markup = factset_markup()
    current = None
    schema = None
    if mutation == 'unknown_prefix':
        markup = markup.replace('根據FactSet最新調查', '根據其他調查')
    elif mutation == 'in_word':
        markup = markup.replace('\ufeff測試股', '測\ufeff試股')
    elif mutation == 'repeated':
        markup = markup.replace('\ufeff', '\ufeff\ufeff')
    elif mutation == 'wrong_href':
        markup = markup.replace('www.cnyes.com', 'other.test')
    elif mutation == 'missing_link':
        markup = markup.replace('<a href="https://www.cnyes.com/twstock/2353">2353-TW</a>', '2353-TW')
    elif mutation == 'current_link_conflict':
        current = markup.replace('/twstock/2353', '/twstock/2354')
    elif mutation == 'current_marker_missing':
        current = markup.replace('\ufeff', '')
    elif mutation == 'schema_number_conflict':
        schema = markup.replace('\ufeff', '').replace('-12.50', '-12.51')
    elif mutation == 'duplicate_paragraph':
        markup *= 2
    elif mutation == 'hidden_link':
        markup = markup.replace('<a ', '<a hidden ')
    with pytest.raises(NewsError, match='body_not_found|article_identity_not_verified'):
        extract_article(factset_page(markup, current=current, schema=schema),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


def test_matching_factset_schema_needs_no_formatting_relaxation(cnyes_config, tmp_path):
    markup = factset_markup()
    result = extract_article(factset_page(markup, schema=markup), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert '\ufeff' in result['body']
    assert 'source_factset_company_prefix_marker_comparison_v1' not in result['article_identity_basis']


@pytest.mark.parametrize('mutation', ['', 'in_word', 'repeated', 'unmatched_href', 'unmatched_text', 'hidden', 'not_list'])
@pytest.mark.parametrize('prefix', ['盤中速報 - ', '盤後速報 - ', '鉅亨速報 - Factset 最新調查：'])
def test_only_observed_current_source_list_anchor_format_marker_can_be_compared(cnyes_config, tmp_path, mutation, prefix):
    title = prefix + '\ufeff測試股價原始值 -12.50 與營收22億'
    if mutation == 'in_word':
        title = '測試股\ufeff價 -12.50'
    if mutation == 'repeated':
        title = title.replace('\ufeff', '\ufeff\ufeff')
    link = '<ul><li><a href="https://news.cnyes.com/news/id/43">' + title + '</a></li></ul>'
    visible = link
    if mutation == 'unmatched_href':
        visible = visible.replace('/43', '/44')
    if mutation == 'unmatched_text':
        visible = visible.replace('22億', '23億')
    if mutation == 'hidden':
        visible = visible.replace('<a ', '<a hidden ')
    if mutation == 'not_list':
        visible = visible.replace('<ul><li>', '<div>').replace('</li></ul>', '</div>')
    payload = mobile_page(body=title, changes={'content': html.escape(link)},
                          schema_changes={'articleBody': title.replace('\ufeff', '')})
    payload = payload.replace(('<p>' + html.escape(title) + '</p>').encode(), visible.encode())
    if mutation:
        with pytest.raises(NewsError):
            extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)
    else:
        result = extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)
        assert result['body'] == title  # Every native value/marker survives.
        assert 'source_anchor_title_format_marker_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('cells', [('22', '184'), ('2', '2184')])
def test_alias_table_cell_boundaries_are_not_lost_in_compact_text(cnyes_config, tmp_path, cells):
    table = '<table><tr><td>22</td><td>184</td></tr></table>'
    visible = '<table><tr>' + ''.join(f'<td>{cell}</td>' for cell in cells) + '</tr></table>'
    payload = mobile_page(body='22184', changes={'content': html.escape(table)})
    payload = payload.replace(b'<p>22184</p>', visible.encode())
    # Add Chinese text so this is an actual tiny data report, not a five-digit
    # number that the generic nontrivial-text gate correctly rejects.
    payload = payload.replace(b'<td>22</td>', '<td>營收22</td>'.encode())
    payload = payload.replace(b'<td>2</td>', '<td>營收2</td>'.encode())
    payload = payload.replace(b'"articleBody": "22184"', '"articleBody": "營收22184"'.encode())
    payload = payload.replace(html.escape(table).encode(), html.escape(table.replace('22</td>', '營收22</td>')).encode())
    if cells == ('22', '184'):
        result = extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)
        assert '營收22\n184' in result['body']
    else:
        with pytest.raises(NewsError, match='body_not_found'):
            extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)
