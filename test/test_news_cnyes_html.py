"""Synthetic source-contract cases, not saved publisher text or HTTP probes."""
from __future__ import annotations

from copy import deepcopy
import html
import json
import re

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config


URL = 'https://news.cnyes.com/news/id/42'
BODY = '公開的完整數據快訊，公布原始數值、變化量及預估值。'
TITLE = '測試完整短訊'
CLOCK = '2014-01-01T00:00:00.000Z'


def short_page(*, body=BODY, change=None, schema_change=None, visible=None,
               canonical=URL, related=False, conflict=False, split=False, duplicate_key=False,
               video_conflict=False):
    article = {'@type': 'NewsArticle', 'url': URL, 'mainEntityOfPage': {'@id': URL},
               'headline': TITLE, 'articleBody': body, 'datePublished': CLOCK,
               'author': [{'name': '來源宣告的測試作者'}]}
    article.update(schema_change or {})
    data = {'newsId': 42, 'title': TITLE, 'content': '&lt;p&gt;' + html.escape(body) + '&lt;/p&gt;',
            'publishAt': 1388534400, 'payment': 0, 'url': URL}
    data.update(change or {})
    nodes = [['$', '$L2', None, {'data': data}]]
    if related:
        neighbor = {**data, 'newsId': 43, 'content': '&lt;p&gt;別篇不得提供正文。&lt;/p&gt;',
                    'url': URL.replace('/42', '/43')}
        nodes.insert(0, ['$', '$L2', None, {'data': neighbor}])
    if conflict:
        nodes.append(['$', '$L2', None, {'data': {**data, 'content': 'conflicting body'}}])
    if video_conflict:
        nodes.append(['$', '$L2', None, {'data': {**data, 'video': 'another video declaration'}}])
    stream = '16:' + json.dumps(nodes, ensure_ascii=False) + '\n'
    if duplicate_key:
        stream = stream.replace('"payment": 0', '"payment": 1, "payment": 0')
    parts = [stream[:len(stream) // 2], stream[len(stream) // 2:]] if split else [stream]
    scripts = ''.join('<script>self.__next_f.push(' + json.dumps([1, p], ensure_ascii=False) + ')</script>'
                      for p in parts)
    text = body if visible is None else visible
    return (f'<html><head><link rel="canonical" href="{html.escape(canonical, quote=True)}">'
            '<script type="application/ld+json">' + json.dumps({'@graph': [article]}, ensure_ascii=False)
            + f'</script></head><body><article><section><h1>{html.escape(TITLE)}</h1></section><div id="article-container">'
            + f'<p>{html.escape(text)}</p></div></article>{scripts}</body></html>').encode()


def source(cnyes_config, tmp_path):
    return PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']


@pytest.mark.parametrize('body', ['(OTT) 2.39 :', '(GLCH) 14.04 :',
                                '18.87  -2.21(-10.48%)：Youku Tudou',
                                '18.87-2.21(-10.48%)：YoukuTudou', '(PLCM) 11.78 :',
                                '(THG)51.01+0.40:', '(LIVE)3.25+0.42:',
                                '(THG) 51.01 +0.40 :', '(LIVE) 3.25 -0.42 :'])
def test_verified_native_quote_stub_is_source_outcome_not_complete_news(cnyes_config, tmp_path, body):
    with pytest.raises(NewsError, match='source_body_quote_only') as caught:
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)
    assert not caught.value.retry


@pytest.mark.parametrize('options', [
    {'change': {'payment': 1}}, {'change': {'content': '&lt;p&gt;different original&lt;/p&gt;'}},
    {'visible': 'visible original disagrees'}, {'canonical': URL.replace('/42', '/43')},
    {'change': {'publishAt': 1388534401}}, {'conflict': True}, {'duplicate_key': True},
    {'schema_change': {'articleBody': 'different schema original'}},
])
def test_native_quote_outcome_requires_original_identity_free_clock_and_text(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError) as caught:
        extract_article(short_page(body='(OTT) 2.39 :', **options), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_quote_only'


@pytest.mark.parametrize('body', ['(OTT) 2.39 : actual prose', '(OTT) 2.39 : 來源敘述',
                                '(OTT) 2.39', 'Useful brief financial news report', '(1) 2.39 :',
                                '(THG)51.01+0.40: actual prose', '(LIVE)3.25+0.42:中文正文',
                                '(THG)51.01+0.40-0.50:', '(LIVE)3.25+0.42(10%):',
                                '(THG)51.01+:', '(THG)51.01+-0.40:'])
def test_arbitrary_short_body_cannot_be_called_verified_quote_only(cnyes_config, tmp_path, body):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_publishers import cnyes_verified_quote_only_body
    soup = BeautifulSoup(short_page(body=body), 'html.parser')
    article = {'@type': 'NewsArticle', 'url': URL, 'headline': TITLE, 'datePublished': CLOCK}
    assert not cnyes_verified_quote_only_body(soup, source(cnyes_config, tmp_path), article, body, page_url=URL)


@pytest.mark.parametrize('media', ['img', 'video', 'audio', 'iframe', 'object', 'table'])
def test_native_quote_classification_cannot_discard_another_visible_representation(cnyes_config, tmp_path, media):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_publishers import cnyes_verified_quote_only_body
    markup = short_page(body='(OTT) 2.39 :').replace(b'</p></div>', f'</p><{media}></{media}></div>'.encode())
    soup = BeautifulSoup(markup, 'html.parser')
    article = {'@type': 'NewsArticle', 'url': URL, 'headline': TITLE, 'datePublished': CLOCK}
    assert not cnyes_verified_quote_only_body(soup, source(cnyes_config, tmp_path), article, '(OTT) 2.39 :', page_url=URL)


def test_quote_outcome_changes_html_fingerprint_not_bulk_api(cnyes_config, tmp_path, monkeypatch):
    import hashlib
    new = source(cnyes_config, tmp_path)
    original_sha256 = hashlib.sha256

    def previous_outcome_fingerprint(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original_sha256(value)
        if isinstance(payload, dict) and 'verified_native_quote_only_outcome_contract_version' in payload:
            payload.pop('verified_native_quote_only_outcome_contract_version')
            return original_sha256(json.dumps(payload, sort_keys=True).encode())
        return original_sha256(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', previous_outcome_fingerprint)
    previous = source(cnyes_config, tmp_path)
    assert new.data_fingerprint != previous.data_fingerprint
    assert (new.cnyes_bulk_data_fingerprint
            and new.cnyes_bulk_data_fingerprint == previous.cnyes_bulk_data_fingerprint)


def test_signed_quote_outcome_upgrade_preserves_bulk_checkpoint_identity(cnyes_config, tmp_path, monkeypatch):
    import hashlib
    new = source(cnyes_config, tmp_path)
    original_sha256 = hashlib.sha256

    def v1_outcome_fingerprint(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original_sha256(value)
        if isinstance(payload, dict) and 'verified_native_quote_only_outcome_contract_version' in payload:
            assert payload['verified_native_quote_only_outcome_contract_version'] == 6
            payload['verified_native_quote_only_outcome_contract_version'] = 1
            return original_sha256(json.dumps(payload, sort_keys=True).encode())
        return original_sha256(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', v1_outcome_fingerprint)
    previous = source(cnyes_config, tmp_path)
    assert new.data_fingerprint != previous.data_fingerprint
    assert new.cnyes_bulk_data_fingerprint == previous.cnyes_bulk_data_fingerprint


def replace_flight(page, stream, *, split=False):
    page = re.sub(rb'<script>self[.]__next_f[.]push\(.*?</script>', b'', page, flags=re.S)
    fragments = [stream[:len(stream) // 2], stream[len(stream) // 2:]] if split else [stream]
    scripts = ''.join('<script>self.__next_f.push(' + json.dumps([1, part], ensure_ascii=False).replace('<', r'\u003c')
                      + ')</script>' for part in fragments).encode()
    return page.replace(b'</body>', scripts + b'</body>')


def text_frame(marker, text, *, size=None):
    return marker + ':T' + format(len(text.encode()) if size is None else size, 'x') + ',' + text


def referenced_page(*, body=BODY, mutate=None, split=False, first=False):
    content = '&lt;p&gt;' + html.escape(body) + '&lt;/p&gt;'
    data = {'newsId': 42, 'title': TITLE, 'content': '$3f', 'publishAt': 1388534400, 'payment': 0, 'url': URL}
    model = '16:' + json.dumps([['$', '$L2', None, {'data': data}]], ensure_ascii=False) + '\n'
    text = text_frame('3f', content)
    stream = text + model if first else model + text
    return replace_flight(short_page(body=body), mutate(stream, model, content) if mutate else stream, split=split)


@pytest.mark.parametrize('options', [{}, {'split': True}, {'first': True}, {'body': BODY + '🐧'}])
def test_flight_text_reference_corroborates_only_visible_short_body(cnyes_config, tmp_path, options):
    result = extract_article(referenced_page(**options), source(cnyes_config, tmp_path), page_url=URL)
    assert result['body'] == options.get('body', BODY)
    assert result['published_at_raw'] == CLOCK
    assert result['extraction_method'] == 'jsonld_article_body'


@pytest.mark.parametrize('mutate', [
    lambda s, m, c: m,
    lambda s, m, c: m + text_frame('3f', c, size=len(c)),  # Character count is not the byte length.
    lambda s, m, c: m + text_frame('3f', c, size=len(c.encode()) + 1),
    lambda s, m, c: s + text_frame('3f', 'conflicting text'),
    lambda s, m, c: m + '3f:' + json.dumps(c) + '\n',  # Only a literal T frame is supported.
    lambda s, m, c: m + text_frame('3f', '$3f'),
    lambda s, m, c: text_frame('3f', '\n' + m),  # Never parse a model embedded in a text frame.
    lambda s, m, c: '0:Hnull\n' * 4097 + s,
])
def test_malformed_missing_conflicting_and_injected_flight_reference_is_not_body(cnyes_config, tmp_path, mutate):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(referenced_page(mutate=mutate), source(cnyes_config, tmp_path), page_url=URL)


def table_page(*, declaration=None, visible_change=None, extra='', nested=False):
    from bs4 import BeautifulSoup
    fragment = ('<p>營收資料(單位:千元)</p><table><tbody><tr><th>項目</th><th>9月</th><th>累計</th></tr>'
                '<tr><td>本年度</td><td>22</td><td>184</td></tr>'
                '<tr><td>去年同期</td><td>22</td><td>169</td></tr>'
                '<tr><td>增減金額</td><td>0</td><td>15</td></tr></tbody></table>')
    body = BeautifulSoup(fragment, 'lxml').get_text('', strip=True)
    rendered = visible_change(fragment) if visible_change else fragment
    if nested:
        rendered = rendered.replace('<table>', '<table><tr><td><table>').replace('</table>', '</table></td></tr></table>')
    page = short_page(body=body, change={'content': html.escape(declaration or fragment)})
    return page.replace(('<p>' + html.escape(body) + '</p>').encode(), (rendered + extra).encode())


def test_short_table_preserves_separate_raw_values_and_public_clock(cnyes_config, tmp_path):
    result = extract_article(table_page(extra='<div id="div-gpt-ad-cnyes_news_article_middle_300*250">‌</div>'),
                             source(cnyes_config, tmp_path), page_url=URL)
    assert '本年度\n22\n184\n去年同期\n22\n169\n增減金額\n0\n15' in result['body']
    assert '22184' not in result['body'] and '22169' not in result['body']
    assert result['extraction_method'] == 'cnyes_visible_short_table_v1'
    assert result['published_at_raw'] == CLOCK


@pytest.mark.parametrize('options', [
    {'declaration': '<p>營收資料(單位:千元)</p><table><tbody><tr><th>項目</th><th>9月</th><th>累計</th></tr>'
                    '<tr><td>本年度</td><td>2</td><td>2184</td></tr><tr><td>去年同期</td><td>22</td><td>169</td></tr>'
                    '<tr><td>增減金額</td><td>0</td><td>15</td></tr></tbody></table>'},
    {'extra': '<div>未宣告但確實存在的正文，不得當作摘要忽略。</div>'},
    {'extra': '<img src="third-party-media">'},
    {'visible_change': lambda f: f.replace('<table>', '<table hidden>')},
    {'visible_change': lambda f: f.replace('<td>22</td>', '<td style="display:none">22</td>')},
    {'nested': True},
])
def test_short_table_cannot_merge_cells_hide_body_or_ignore_real_text(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(table_page(**options), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('options', [{}, {'related': True}, {'split': True},
    {'body': 'The complete original short flash report has more than eight words.'},
    {'body': TITLE}])
def test_exact_short_public_body_is_not_rejected_by_length(cnyes_config, tmp_path, options):
    policy = source(cnyes_config, tmp_path)
    result = extract_article(short_page(**options), policy, page_url=URL)
    assert result['body'] == options.get('body', BODY)
    assert result['published_at_raw'] == CLOCK
    assert result['authors_json'] == '["來源宣告的測試作者"]'
    assert result['extraction_method'] == 'jsonld_article_body'
    assert result['body_completeness'].startswith('verified_publisher_short_body;')
    assert 'payment0' in result['article_identity_basis']
    assert not policy.allow_short_configured_body


@pytest.mark.parametrize('fragment', [
    '<section>完整的原始短訊<br>保留數值 123<p>後續段落 456</p>段落以外仍是正文 789</section>',
    '<div>完整的原始短訊，保留數值 123 與 456。</div>',
    '完整的原始短訊，保留數值 123<br>後續數值 456。',
])
def test_whole_current_short_body_does_not_require_paragraph_wrapping(cnyes_config, tmp_path, fragment):
    from downloader.news_fulltext_policy import _plain_text
    body = _plain_text(fragment, parser='lxml')
    payload = short_page(body=body, change={'content': html.escape(fragment)})
    payload = payload.replace(('<p>' + html.escape(body) + '</p>').encode(), fragment.encode())
    result = extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert result['body'] == body
    assert result['published_at_raw'] == CLOCK
    assert 'whole_visible_container_match_full_content' in result['article_identity_basis']


@pytest.mark.parametrize('hidden', [
    '<section hidden>', '<section aria-hidden="true">', '<section style="display:none">',
])
def test_direct_short_body_text_cannot_be_hidden_or_partial(cnyes_config, tmp_path, hidden):
    fragment = '<section>完整原始短訊，數值 123 與 456。</section>'
    body = '完整原始短訊，數值 123 與 456。'
    payload = short_page(body=body, change={'content': html.escape(fragment)})
    payload = payload.replace(('<p>' + html.escape(body) + '</p>').encode(), fragment.replace('<section>', hidden).encode())
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('mutation', [None, 'missing_completion', 'error_completion', 'hidden_section', 'hidden_body', 'duplicate_wrapper'])
def test_original_host_direct_short_text_requires_the_actual_completed_public_stream(cnyes_config, tmp_path, mutation):
    from downloader.news_fulltext_policy import _plain_text
    from test_news_cnyes_canonical_alias import FIZZ_COMPLETION, streamed_mobile_page
    fragment = '<section>完整原始短訊<br>數值 123<p>另一欄 456</p>未包段落的結尾 789。</section>'
    body = _plain_text(fragment, parser='lxml')
    payload = streamed_mobile_page(body=body, changes={'content': html.escape(fragment)})
    payload = payload.replace(('<p>' + html.escape(body) + '</p>').encode(), fragment.encode())
    if mutation == 'missing_completion':
        payload = payload.replace(FIZZ_COMPLETION.encode(), b'')
    elif mutation == 'error_completion':
        payload = payload.replace(b'$RC("B:0","S:0")', b'$RC("B:0","S:0","error")')
    elif mutation == 'hidden_section':
        payload = payload.replace(fragment.encode(), fragment.replace('<section>', '<section hidden>').encode())
    elif mutation == 'hidden_body':
        payload = payload.replace(b'<div id="article-container">', b'<div id="article-container" hidden>')
    elif mutation == 'duplicate_wrapper':
        payload = payload.replace(b'</body>', b'<div hidden id="S:0"></div></body>')
    if mutation:
        with pytest.raises(NewsError):
            extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    else:
        result = extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
        assert result['body'] == body
        assert result['published_at_raw'] == CLOCK
        assert 'whole_visible_container_match_full_content' in result['article_identity_basis']


@pytest.mark.parametrize('options', [
    {'change': {'payment': 1}}, {'change': {'payment': False}}, {'change': {'payment': '0'}},
    {'change': {'newsId': True}}, {'change': {'newsId': 43}}, {'change': {'publishAt': True}},
    {'change': {'publishAt': 1388534401}}, {'change': {'title': 'related headline'}},
    {'change': {'url': URL.replace('/42', '/43')}}, {'canonical': URL.replace('/42', '/43')},
    {'change': {'content': None}}, {'change': {'content': ''}},
    {'change': {'content': '&lt;p&gt;只有摘要，不是這篇全文。&lt;/p&gt;'}},
    {'visible': ''}, {'visible': '只有摘要，不是這篇全文。'},
    {'schema_change': {'datePublished': '2014-01-01T00:00:00'}},
    {'schema_change': {'datePublished': 'unverified clock'}},
    {'schema_change': {'@type': 'BlogPosting'}}, {'conflict': True}, {'duplicate_key': True},
])
def test_no_short_exception_for_unverified_or_hidden_text(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(short_page(**options), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('body, outcome', [('', 'source_article_empty_template'), ('tiny', 'body_not_found')])
def test_exact_empty_paragraph_and_unverified_tiny_text_are_not_articles(cnyes_config, tmp_path, body, outcome):
    with pytest.raises(NewsError, match='^' + outcome + '$'):
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)


def test_verified_literal_null_is_source_placeholder_not_four_character_body(cnyes_config, tmp_path):
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(short_page(body='null'), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('options', [
    {'visible': ''}, {'visible': 'real article text'}, {'visible': 'NULL'},
    {'change': {'content': None}}, {'change': {'content': ''}},
    {'change': {'content': '&lt;p&gt;NULL&lt;/p&gt;'}},
    {'change': {'content': '&lt;p&gt;null&lt;/p&gt;&lt;script&gt;hidden body&lt;/script&gt;'}},
    {'change': {'video': 'video body'}}, {'change': {'payment': 1}},
    {'change': {'payment': False}}, {'change': {'publishAt': 1388534401}},
    {'change': {'url': URL.replace('/42', '/43')}}, {'conflict': True},
    {'duplicate_key': True}, {'video_conflict': True},
    {'canonical': URL.replace('/42', '/43')},
    {'schema_change': {'datePublished': '2014-01-01T00:00:00'}},
])
def test_unverified_literal_null_remains_parser_gap(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(short_page(body='null', **options), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('media', [b'<img src="x">', b'<video></video>', b'<iframe></iframe>'])
def test_null_with_media_is_not_a_verified_empty_template(cnyes_config, tmp_path, media):
    payload = short_page(body='null').replace(b'<div id="article-container">', b'<div id="article-container">' + media)
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


def test_nontrivial_guard_avoids_redundant_short_body_stream_parse(cnyes_config, tmp_path, monkeypatch):
    import downloader.news_fulltext_publishers as publishers
    monkeypatch.setattr(publishers, 'cnyes_verified_short_body',
                        lambda *a, **k: pytest.fail('nontrivial text rejection parsed stream again'))
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(short_page(body='null'), source(cnyes_config, tmp_path), page_url=URL)
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(short_page(body='tiny'), source(cnyes_config, tmp_path), page_url=URL)


def empty_page(**options):
    # Model the observed empty article container, not an empty <p> or teaser.
    return short_page(body='', change={'content': '', **options.pop('change', {})},
                      **options).replace(b'<div id="article-container"><p></p>', b'<div id="article-container">')


def test_verified_empty_publisher_body_has_its_own_source_outcome(cnyes_config, tmp_path):
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(empty_page(), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('content', ['<div></div>', '&lt;div&gt;&lt;/div&gt;',
    '<div><br></div>', '&lt;div&gt;&lt;br&gt;&lt;/div&gt;', '<p></p>', '&lt;p&gt;&lt;/p&gt;'])
@pytest.mark.parametrize('schema_body_present', [False, True])
def test_observed_exact_empty_div_is_source_absence_not_a_body(cnyes_config, tmp_path, content, schema_body_present):
    payload = empty_page(change={'content': content, 'video': ''})
    if not schema_body_present:
        payload = payload.replace(b'"articleBody": "", ', b'')
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('content', ['<div> </div>', '<div hidden></div>', '<div><p></p></div>',
    '<div><!--unknown--></div>', '<div><script>unknown</script></div>', '<div><img src="x"></div>',
    '<div>real text</div>', '<div><br hidden></div>', '<div><br><br></div>', '<div> <br></div>',
    '<div><br><script>unknown</script></div>', '<div><br><img src="x"></div>',
    '&amp;lt;div&amp;gt;&amp;lt;/div&amp;gt;', None])
def test_empty_div_repair_does_not_classify_arbitrary_textless_markup(cnyes_config, tmp_path, content):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(empty_page(change={'content': content, 'video': ''}),
                        source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('changes', [{'payment': 1}, {'payment': False}, {'publishAt': 1388534401},
    {'video': 'source video'}, {'title': 'another title'}, {'url': URL.replace('/42', '/43')}])
def test_empty_div_still_requires_exact_free_current_identity_clock_and_no_video(cnyes_config, tmp_path, changes):
    with pytest.raises(NewsError):
        extract_article(empty_page(change={'content': '&lt;div&gt;&lt;/div&gt;', 'video': '', **changes}),
                        source(cnyes_config, tmp_path), page_url=URL)
@pytest.mark.parametrize('options', [
    {'schema_change': {'headline': ''}, 'change': {'title': ''}},
    {'related': True}, {'split': True},
])
def test_verified_empty_source_variants(cnyes_config, tmp_path, options):
    payload = empty_page(**options)
    if options.get('schema_change', {}).get('headline') == '':
        payload = payload.replace(('<h1>' + TITLE + '</h1>').encode(), b'<h1></h1>')
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


def test_empty_article_with_later_loading_header(cnyes_config, tmp_path):
    payload = empty_page().replace(b'</body>', b'<section><h1>...</h1></section></body>')
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


def test_unrelated_leading_header_does_not_prove_empty_source(cnyes_config, tmp_path):
    payload = empty_page().replace(b'<article>', b'<article><h1>another article</h1>')
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('empty', [False, True])
def test_streamed_article_after_leading_sidebar_header(cnyes_config, tmp_path, empty):
    payload = (empty_page() if empty else short_page()).replace(
        b'<body>', b'<body><aside><h1>...</h1></aside><div id="S:0">').replace(b'</article>', b'</article></div>')
    if empty:
        with pytest.raises(NewsError, match='^source_article_empty_template$'):
            extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    else:
        result = extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
        assert result['body'] == BODY
        assert result['published_at_raw'] == CLOCK


def test_header_outside_body_article_is_not_an_identity_witness(cnyes_config, tmp_path):
    payload = short_page().replace(b'<article><section>', b'<section>').replace(b'</div></article>', b'</div>')
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('options', [
    {'change': {'payment': 1}}, {'change': {'payment': False}},
    {'change': {'content': None}}, {'change': {'content': ' '}},
    {'change': {'video': 'a video declaration'}},
    {'change': {'newsId': 43}}, {'change': {'newsId': True}},
    {'change': {'publishAt': 1388534401}}, {'change': {'publishAt': True}},
    {'change': {'title': 'another headline'}},
    {'change': {'url': URL.replace('/42', '/43')}},
    {'canonical': URL.replace('/42', '/43')},
    {'schema_change': {'datePublished': 'unknown'}},
    {'schema_change': {'datePublished': '2014-01-01T00:00:00'}},
    {'schema_change': {'@type': 'BlogPosting'}},
    {'visible': 'tiny'}, {'conflict': True}, {'duplicate_key': True}, {'video_conflict': True},
])
def test_unverified_missing_text_is_not_declared_source_empty(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(empty_page(**options), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('media', [b'<img src="x">', b'<video></video>', b'<iframe></iframe>'])
def test_media_only_body_is_not_empty_source_template(cnyes_config, tmp_path, media):
    payload = empty_page().replace(b'<div id="article-container">', b'<div id="article-container">' + media)
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


def test_no_short_exception_on_other_provider(cnyes_config, tmp_path):
    cfg = deepcopy(cnyes_config)
    cfg['providers'][0].update(id='other', domains=['example.test'], cnyes_apis=[])
    cfg['providers'][0].pop('cnyes_history_start')
    result = PolicyRegistry(cfg, base=tmp_path).sources['other']
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(short_page(), result, page_url=URL)


def test_long_declared_body_does_not_parse_streaming_metadata(cnyes_config, tmp_path, monkeypatch):
    import downloader.news_fulltext_publishers as publishers
    monkeypatch.setattr(publishers, 'cnyes_verified_short_body',
                        lambda *a, **k: pytest.fail('normal long article did redundant stream parsing'))
    body = BODY * 10
    result = extract_article(short_page(body=body, change={'content': ''}),
                             source(cnyes_config, tmp_path), page_url=URL)
    assert result['body'] == body
    assert result['body_completeness'] == 'declared_article_body'


@pytest.mark.parametrize('change,expected', [
    ({}, True), ({'title': 'valid title'}, False), ({'content': 'real body'}, False),
    ({'content': None}, False), ({'video': 'published video'}, False),
    ({'payment': False}, False), ({'payment': 1}, False), ({'newsId': True}, False),
    ({'publishAt': True}, False),
])
def test_source_empty_diagnostic_requires_exact_fields(change, expected):
    from scripts.verify_news_source_diagnostics import cnyes_empty_item_outcome
    post = {'newsId': 42, 'title': '', 'content': '', 'video': '', 'payment': 0, 'publishAt': 1388534400}
    post.update(change)
    assert cnyes_empty_item_outcome(post, {'url': URL, 'error': 'invalid_bulk_post_fields'}) is expected
    assert not cnyes_empty_item_outcome(post, {'url': URL, 'error': 'bulk_post_not_public'})
    assert not cnyes_empty_item_outcome(post, {'url': URL.replace('/42', '/43'), 'error': 'invalid_bulk_post_fields'})
