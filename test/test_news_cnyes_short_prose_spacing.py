"""Whole visible/current prose may restore spaces lost by JSON-LD, not words."""
import html
import json
import re

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page, source


PROSE = 'Report: Original traders index was up 138.1% in 2021'
FLAT = ''.join(PROSE.split())


def page(*, fragment=None, schema=FLAT, visible=None, **options):
    fragment = fragment if fragment is not None else '<p>' + html.escape(PROSE) + '</p>'
    payload = short_page(body=PROSE, schema_change={'articleBody': schema},
        change={'content': html.escape(fragment), **options.pop('change', {})}, **options)
    return payload.replace(('<p>' + html.escape(PROSE) + '</p>').encode(),
        (visible if visible is not None else fragment).encode())


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('fragment', [PROSE, '<p>' + PROSE + '</p>', '<div>' + PROSE + '</div>',
    '<p>Report: Original traders index<br>was up 138.1% in 2021</p>'])
def test_visible_and_complete_current_content_restore_only_actual_spaces(cnyes_config, tmp_path, parser, fragment):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(page(fragment=fragment), source(cnyes_config, tmp_path), page_url=URL)
    assert re.sub(r'\s+', ' ', result['body']).strip() == PROSE
    assert result['body'] != FLAT
    assert result['extraction_method'] == 'cnyes_visible_short_prose_spacing_v1'
    assert result['published_at_raw'] == CLOCK
    assert json.loads(result['authors_json']) == ['來源宣告的測試作者']
    assert result['body_completeness'].startswith('verified_publisher_short_body;')
    assert 'whole_visible_container_match_full_content' in result['article_identity_basis']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('options', [
    {'change': {'payment': 1}}, {'change': {'publishAt': 1388534401}},
    {'change': {'newsId': 43}}, {'change': {'url': URL.replace('/42', '/43')}},
    {'change': {'content': '<p>' + PROSE.replace('138.1', '139.1') + '</p>'}},
    {'change': {'title': 'another source title'}}, {'conflict': True}, {'duplicate_key': True},
    {'canonical': URL.replace('/42', '/43')}, {'schema': FLAT.replace('2021', '2022')},
    {'visible': '<p>' + PROSE.replace('138.1', '139.1') + '</p>'},
    {'visible': '<p>' + PROSE + '</p><p>another source observation 7.25</p>'},
    {'fragment': '<p hidden>' + PROSE + '</p>'},
    {'fragment': '<p aria-hidden="true">' + PROSE + '</p>'},
    {'fragment': '<p style="display:none">' + PROSE + '</p>'},
])
def test_spacing_recovery_cannot_supply_other_text_hide_body_or_change_identity(cnyes_config, tmp_path, parser, options):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError):
        extract_article(page(**options), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('extra', [
    '<img src="https://source.example/image">', '<video></video>', '<audio></audio>',
    '<iframe src="https://source.example/video"></iframe>', '<object></object>', '<embed>',
    '<table><tr><td>12</td><td>138.1</td></tr></table>', '<figure><figcaption>another body</figcaption></figure>',
])
def test_spacing_recovery_does_not_ignore_media_tables_or_figure_text(cnyes_config, tmp_path, parser, extra):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError):
        extract_article(page(fragment='<p>' + PROSE + '</p>' + extra, schema=FLAT),
            source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('body', ['tiny', 'b', 'XXXX', '1234', '23456789',
    'Report: Originaltradersindexwasup138.1%in2021', '(AB) 20.19 1.08(5.65%) :'])
def test_unobserved_word_boundaries_never_infer_missing_words(cnyes_config, tmp_path, body):
    with pytest.raises(NewsError):
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_already_spaced_short_prose_stays_exact_same_contract(cnyes_config, tmp_path, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(short_page(body=PROSE), source(cnyes_config, tmp_path), page_url=URL)
    assert result['body'] == PROSE
    assert result['extraction_method'] == 'jsonld_article_body'


def test_empty_source_headline_is_not_reconstructed_from_table_or_metadata(cnyes_config, tmp_path):
    payload = page().replace(html.escape(TITLE).encode(), b'')
    # Every source declaration and the visible header are empty. Do not
    # invent a headline even when independent identity/body clocks agree.
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
