"""Original text-only figure credit needs full source proof, not an author guess."""
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_structured_figure_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import caption_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


TEXT = '文．測試雜誌'
FIGURE = '<figure><strong style="letter-spacing:0.7px; text-align:justify">' + TEXT + '</strong></figure>'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_credit_is_original_notice_not_inferred_author_or_narrative(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    value = extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE,
        schema_caption=TEXT, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert value['body'] == BODY * 8 and value['published_at_raw'] == CLOCK
    assert json.loads(value['authors_json']) == [AUTHOR] and value['modified_at_raw'] is None
    assert value['verified_image_captions_json'] is None
    assert json.loads(value['verified_article_notices_json']) == [{
        'text': TEXT, 'kind': 'same_article_figure_source_byline', 'image_sources': [], 'source_links': [],
    }]
    assert 'source_current_figure_source_byline_comparison_v1' in value['article_identity_basis']
    assert 'source_figure_byline_preserved_without_author_reassignment' in value['body_completeness']
    assert value['historical_point_in_time'] is False


MUTATIONS = [
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>'), id='hidden-root'),
    pytest.param(lambda f: f.replace('<figure>', '<figure class="unknown">'), id='unknown-root'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unknown-->'), id='comment'),
    pytest.param(lambda f: f.replace('<strong ', '<strong hidden '), id='hidden-credit'),
    pytest.param(lambda f: f.replace('<strong ', '<strong onclick="unknown()" '), id='event'),
    pytest.param(lambda f: f.replace('text-align:justify', 'display:none'), id='hidden-style'),
    pytest.param(lambda f: f.replace('0.7px', '2px'), id='unknown-style'),
    pytest.param(lambda f: f.replace('文．', '未知．'), id='unknown-prefix'),
    pytest.param(lambda f: f.replace(TEXT, '<span>' + TEXT + '</span>'), id='nested-label'),
    pytest.param(lambda f: f.replace(TEXT, '<a href="https://example.org">' + TEXT + '</a>'), id='link'),
    pytest.param(lambda f: f.replace(TEXT, '文．'), id='missing-name'),
    pytest.param(lambda f: f.replace(TEXT, '文．' + '測' * 65), id='oversized-name'),
    pytest.param(lambda f: f.replace('</figure>', '<p>additional original value -12.5%</p></figure>'), id='extra-prose'),
    pytest.param(lambda f: f.replace('</figure>', '<img src="https://example.org/x.jpg"></figure>'), id='unexpected-image'),
]


@pytest.mark.parametrize('mutate', MUTATIONS)
@pytest.mark.parametrize('representation', ['visible', 'current', 'both'])
def test_schema_repetition_does_not_authorize_unknown_credit_grammar(cnyes_config, tmp_path, mutate, representation):
    changed = mutate(FIGURE)
    text = BeautifulSoup(changed, 'html.parser').figure.get_text(' ', strip=True) if representation == 'both' else TEXT
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=changed if representation in {'current', 'both'} else FIGURE,
            visible_figure=changed if representation in {'visible', 'both'} else FIGURE, schema_caption=text),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [TEXT[:-1], TEXT[::-1], TEXT + ' -12.5%', ''])
def test_whole_schema_required_except_unchanged_original_narrative_fastpath(cnyes_config, tmp_path, schema):
    payload = caption_page(current_figure=FIGURE, visible_figure=FIGURE, schema_caption=schema)
    if schema:
        with pytest.raises(NewsError):
            extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)
    else:
        value = extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)
        assert value['body'] == BODY * 8 and value['verified_article_notices_json'] is None
        assert 'source_current_figure_source_byline_comparison_v1' not in value['article_identity_basis']


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_byline_witness_is_exact_nonmutating_ordered_dom(parser):
    node = BeautifulSoup(FIGURE, parser).figure
    before = str(node)
    assert _cnyes_structured_figure_identity(node) == (
        TEXT, ((), (), hashlib.sha256(before.encode()).hexdigest()), 'same_article_figure_source_byline')
    assert str(node) == before


def test_credit_cannot_manufacture_a_body_or_reorder_duplicate_figures(cnyes_config, tmp_path):
    for current, visible, declared, body in ((FIGURE, FIGURE, TEXT, ''),
            (FIGURE + FIGURE, FIGURE + FIGURE, TEXT + TEXT, BODY * 8),
            (FIGURE, '', TEXT, BODY * 8), ('', FIGURE, TEXT, BODY * 8)):
        with pytest.raises(NewsError):
            extract_article(caption_page(body=body, current_figure=current, visible_figure=visible,
                schema_caption=declared), source(cnyes_config, tmp_path), page_url=MOBILE)
