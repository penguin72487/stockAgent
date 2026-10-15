"""Exact visible/current em-format comparison preserves the original marker."""
import html
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_publishers import _cnyes_remove_verified_em_paragraph_markers
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page, streamed_mobile_page
from test_news_cnyes_captions import FIGURE, caption_page
from test_news_cnyes_html import BODY, CLOCK, URL


TEXT = '\ufeff一、完整原文標題：成交量123股，價格-12.50，殖利率2.39%。'
EM = '<p><em>' + TEXT + '</em></p>'


def page(*, markup=EM, current=None, schema=None, streamed=False, figures=False):
    current = markup if current is None else current
    schema = BODY * 8 + TEXT[1:] if schema is None else schema
    if figures:
        # Reuse the actual caption/current/visible corroboration fixture too.
        payload = caption_page(schema_body=schema, streamed=streamed)
        content = '<p>' + BODY * 8 + '</p>' + current + FIGURE
        body_at = ('<p>' + BODY * 8 + '</p>').encode()
        return payload.replace(body_at, body_at + markup.encode(), 1).replace(
            html.escape('<p>' + BODY * 8 + '</p>' + FIGURE).encode(), html.escape(content).encode())
    builder = streamed_mobile_page if streamed else mobile_page
    payload = builder(changes={'content': html.escape('<p>' + BODY * 8 + '</p>' + current)},
                      schema_changes={'articleBody': schema})
    return payload.replace(b'</p></div></article>', ('</p>' + markup + '</div></article>').encode())


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('figures', [False, True])
def test_verified_single_em_marker_preserves_whole_source_body(cnyes_config, tmp_path, parser, url, streamed, figures):
    cnyes_config['providers'][0]['html_parser'] = parser
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    value = extract_article(page(streamed=streamed, figures=figures), source, page_url=url)
    assert BODY * 8 in value['body'] and TEXT in value['body']
    assert value['published_at_raw'] == CLOCK and json.loads(value['authors_json']) == [AUTHOR]
    assert 'source_current_em_paragraph_format_marker_comparison_v1' in value['article_identity_basis']


@pytest.mark.parametrize('mutation', ['in_word', 'repeat', 'other_wrapper', 'nested', 'attribute',
                                    'hidden', 'comment', 'duplicate', 'current_missing_marker',
                                    'current_markup_differs', 'schema_number_conflict'])
def test_em_marker_does_not_relax_source_text_visibility_or_current_structure(cnyes_config, tmp_path, mutation):
    markup, current, schema = EM, None, None
    if mutation == 'in_word':
        markup = EM.replace(TEXT, '一、\ufeff' + TEXT[3:])
    elif mutation == 'repeat':
        markup = EM.replace(TEXT, '\ufeff' + TEXT)
    elif mutation == 'other_wrapper':
        markup = EM.replace('<em>', '<strong>').replace('</em>', '</strong>')
    elif mutation == 'nested':
        markup = EM.replace(TEXT, '<span>' + TEXT + '</span>')
    elif mutation == 'attribute':
        markup = EM.replace('<em>', '<em class="unreviewed">')
    elif mutation == 'hidden':
        markup = EM.replace('<em>', '<em hidden>')
    elif mutation == 'comment':
        markup = EM.replace('<em>', '<em><!--unknown-->')
    elif mutation == 'duplicate':
        markup *= 2
        schema = BODY * 8 + TEXT[1:] * 2
    elif mutation == 'current_missing_marker':
        current = EM.replace('\ufeff', '')
    elif mutation == 'current_markup_differs':
        current = EM.replace('<em>', '<strong>').replace('</em>', '</strong>')
    elif mutation == 'schema_number_conflict':
        schema = BODY * 8 + TEXT[1:].replace('123股', '456股')
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    with pytest.raises(NewsError):
        extract_article(page(markup=markup, current=current, schema=schema), source, page_url=MOBILE)


def test_comparison_changes_only_copy_never_original_current_or_retained_value(cnyes_config, tmp_path):
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    original = BeautifulSoup(EM, source.html_parser)
    before = str(original)
    compare = BeautifulSoup(str(original), source.html_parser)
    assert _cnyes_remove_verified_em_paragraph_markers(compare, html.escape(EM), source) == 1
    assert str(original) == before and str(compare.find('p')) == EM.replace('\ufeff', '')


def test_ordinary_native_match_does_not_parse_marker_comparisons(cnyes_config, tmp_path, monkeypatch):
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    def forbidden(*args):
        pytest.fail('exact ordinary match parsed another comparison view')
    monkeypatch.setattr('downloader.news_fulltext_publishers._cnyes_remove_verified_em_paragraph_markers', forbidden)
    assert extract_article(mobile_page(), source, page_url=MOBILE)['body'] == BODY * 8
