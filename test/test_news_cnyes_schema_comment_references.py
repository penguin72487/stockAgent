"""Schema-only source image-comment residue must not become visible prose."""
import html
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page, streamed_mobile_page
from test_news_cnyes_html import BODY, CLOCK, URL, source

LABELS = ('20131004_214_0.jpg', '20131209_824_0.jpg')
TAIL = ' 原始成交量2330股，年增率-12.5%，價格29.290元。'


def current_comment(label):
    return '<article_image>' + label + '</article_image> '


def renderer_comment(label):
    return ('</section><section style="margin-top:30px"><article_image>' + label
        + '</article_image></section><section style="margin-top:30px"> ')


def reference_page(*, labels=LABELS, current_comments=None, visible_comments=None,
                   visible_body=None, schema=None, streamed=False, visible_attrs=None,
                   current_parent=False, extra_current='', extra_visible='', empty=False,
                   changes=None, schema_changes=None):
    body = '' if empty else BODY * 4
    tail = '' if empty else TAIL
    current_comments = current_comments if current_comments is not None else [current_comment(label) for label in labels]
    visible_comments = visible_comments if visible_comments is not None else [renderer_comment(label) for label in labels]
    current_markup = ''.join('<!--' + value + '-->' for value in current_comments)
    if current_parent:
        current_markup = '<p>' + current_markup + '</p>'
    current = '<p>' + html.escape(body) + '</p>' + current_markup + extra_current + '<p>' + html.escape(tail) + '</p>'
    attrs = visible_attrs if visible_attrs is not None else 'style="margin-top:30px"'
    visible = ('<section ' + attrs + '><p>' + html.escape(visible_body if visible_body is not None else body)
        + '</p>' + ''.join('<!--' + value + '-->' for value in visible_comments) + '</section>'
        + extra_visible + '<section style="margin-top:30px"><p>' + html.escape(tail) + '</p></section>')
    schema = schema if schema is not None else body + ' '.join(label + ' -->' for label in labels) + tail
    builder = streamed_mobile_page if streamed else mobile_page
    payload = builder(body=body + tail,
        changes={'content': html.escape(current), **(changes or {})},
        schema_changes={'articleBody': schema, **(schema_changes or {})})
    original = '<p>' + html.escape(body + tail) + '</p></div></article>'
    if original.encode() not in payload:
        raise ValueError('fixture_original_article_scope_missing')
    return payload.replace(original.encode(), (visible + '</div></article>').encode())


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('labels', [LABELS[:1], LABELS])
def test_real_visible_prose_and_numeric_values_preserved_not_schema_residue(cnyes_config, tmp_path, parser, url, streamed, labels):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(reference_page(streamed=streamed, labels=labels), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 4 + '\n' + TAIL.strip()
    assert not any(label in result['body'] for label in labels)
    assert '2330股' in result['body'] and '-12.5%' in result['body'] and '29.290元' in result['body']
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert result['published_at_raw'] == CLOCK and result['historical_point_in_time'] is False
    assert result['verified_image_captions_json'] is None
    assert json.loads(result['verified_article_notices_json']) == [{'text': label + ' -->',
        'kind': 'same_article_schema_legacy_comment_reference', 'marker_kind': 'article_image',
        'source_reference': label, 'referenced_asset_or_values_reconstructed': False,
        'visible_as_narrative': False} for label in labels]
    assert 'source_current_schema_legacy_comment_reference_comparison_v1' in result['article_identity_basis']
    assert 'source_current_figure_notice_schema_comparison_v1' not in result['article_identity_basis']
    assert 'schema_only_references_preserved_without_hidden_text_asset_or_value_reconstruction' in result['body_completeness']


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('change', [
    {'current_comments': [current_comment('20130101_999_0.jpg'), current_comment(LABELS[1])]},
    {'current_comments': [current_comment(LABELS[1]), current_comment(LABELS[0])]},
    {'visible_comments': [renderer_comment(LABELS[1]), renderer_comment(LABELS[0])]},
    {'visible_comments': [renderer_comment(LABELS[0])]},
    {'visible_comments': [renderer_comment('20130101_999_0.jpg'), renderer_comment(LABELS[1])]},
    {'current_comments': [current_comment(label).rstrip() for label in LABELS]},
    {'visible_comments': [renderer_comment(label) + 'new value123.45' for label in LABELS]},
    {'current_comments': ['omitted prose123.45', current_comment(LABELS[1])]},
    {'extra_current': '<!--unknown-->'},
    {'extra_visible': '<!--unknown-->'},
    {'extra_current': '<p>only current123.45</p>'},
    {'extra_visible': '<p>only visible123.45</p>'},
    {'visible_body': BODY * 4 + ' altered123.45'},
    {'schema': BODY * 4 + '99.table -->' + TAIL},
    {'schema': BODY * 4 + ' '.join(label + ' -->' for label in reversed(LABELS)) + TAIL},
    {'schema': ' '.join(label + ' -->' for label in LABELS) + BODY * 4 + TAIL},
    {'schema': BODY * 4 + ' '.join(label + ' -->' for label in LABELS) + TAIL + ' schema only123.45'},
    {'visible_attrs': 'style="margin-top:30px" hidden'},
    {'visible_attrs': 'style="margin-top:30px;display:none"'},
    {'visible_attrs': 'style="margin-top:30px" aria-hidden="true"'},
    {'visible_attrs': 'style="margin-top: 30px"'},
    {'current_parent': True},
    {'labels': (LABELS[0], LABELS[0])},
    {'labels': (LABELS[0].replace('.jpg', '.png'),)},
    {'labels': ('https://example.org/x.jpg',)},
    {'labels': tuple(f'20131004_{n}_0.jpg' for n in range(17))},
    {'empty': True},
    {'changes': {'payment': 1}},
    {'schema_changes': {'isAccessibleForFree': False}},
    {'schema_changes': {'url': 'https://news.cnyes.com/news/id/999'}},
    {'schema_changes': {'datePublished': '2020-01-01T01:01:01+08:00'}},
    {'schema_changes': {'author': {'@type': 'Person', 'name': 'wrong source author'}}},
])
def test_complete_source_identity_order_rights_and_comment_structure_required(cnyes_config, tmp_path, parser, change):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError):
        extract_article(reference_page(**change), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_healthy_body_skips_schema_comment_reparse(cnyes_config, tmp_path, monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_publishers._cnyes_schema_legacy_comment_witness',
        lambda *args: pytest.fail('healthy body entered mismatch-only schema reference path'))
    result = extract_article(mobile_page(), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['verified_article_notices_json'] is None


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_schema_without_residue_retains_existing_body_match_fast_path(cnyes_config, tmp_path, parser, monkeypatch):
    cnyes_config['providers'][0]['html_parser'] = parser
    monkeypatch.setattr('downloader.news_fulltext_publishers._cnyes_schema_legacy_comment_witness',
        lambda *args: pytest.fail('matching schema was incorrectly required to contain legacy references'))
    result = extract_article(reference_page(schema=BODY * 4 + TAIL), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 4 + '\n' + TAIL.strip()
    assert result['verified_article_notices_json'] is None
