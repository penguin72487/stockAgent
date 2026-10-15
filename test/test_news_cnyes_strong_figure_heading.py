"""The exact same-image numbered heading is text, not an inferred caption."""
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_structured_figure_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import caption_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


IMAGE = 'https://cimg.cnyes.cool/owned-heading-image.jpg'
STYLE = 'letter-spacing:0.7px; text-align:justify'
HEADING = '成交量123股，價格-12.50，殖利率2.39%。'
TEXT = '1.\n' + HEADING
FIGURE = f'<figure><img src="{IMAGE}"><strong style="{STYLE}">1.</strong><strong style="{STYLE}">{HEADING}</strong></figure>'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_exact_current_visible_schema_heading_is_preserved_separately(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    value = extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE,
        schema_caption=TEXT, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert value['body'] == BODY * 8 and value['published_at_raw'] == CLOCK
    assert json.loads(value['authors_json']) == [AUTHOR]
    assert value['verified_image_captions_json'] is None
    assert json.loads(value['verified_article_notices_json']) == [{
        'text': TEXT, 'kind': 'same_article_image_figure_numbered_heading',
        'image_sources': [IMAGE], 'source_links': [],
    }]
    assert 'source_current_image_figure_numbered_heading_comparison_v1' in value['article_identity_basis']


@pytest.mark.parametrize('mutation', [
    lambda f: f.replace('1.</strong>', '2.</strong>'),
    lambda f: f.replace('123股', '456股'),
    lambda f: f.replace(IMAGE, 'https://cimg.cnyes.cool/different-image.jpg'),
    lambda f: f.replace('<figure>', '<figure hidden>'),
    lambda f: f.replace('<figure>', '<figure class="unknown">'),
    lambda f: f.replace('<figure>', '<figure><!--unknown-->'),
    lambda f: f.replace('<img ', '<img hidden '),
    lambda f: f.replace('<strong ', '<strong hidden ', 1),
    lambda f: f.replace(STYLE, STYLE + '; display:none'),
    lambda f: f.replace(STYLE, 'letter-spacing:1px; text-align:justify'),
    lambda f: f.replace('<strong', '<em').replace('</strong>', '</em>'),
    lambda f: f.replace(HEADING, '<span>' + HEADING + '</span>'),
    lambda f: f.replace('</figure>', '<p>未知附註789</p></figure>'),
    lambda f: f.replace('</figure>', '<strong style="' + STYLE + '">額外標題</strong></figure>'),
    lambda f: f.replace('<strong ', '<strong data-source="unknown" ', 1),
    lambda f: f.replace('1.</strong>', '0.</strong>'),
    lambda f: f.replace(IMAGE, 'javascript:unknown()'),
    lambda f: f.replace(IMAGE, 'http://[invalid'),
])
def test_unknown_hidden_or_conflicting_heading_is_not_a_schema_waiver(cnyes_config, tmp_path, mutation):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE, visible_figure=mutation(FIGURE),
            schema_caption=TEXT), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [TEXT.replace('123', '456'), TEXT + '未知文字', TEXT[::-1]])
def test_matching_heading_still_requires_whole_schema(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE,
            schema_caption=schema), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_schema_omits_figure_keeps_existing_narrative_scope_without_new_heading_claim(cnyes_config, tmp_path):
    # The established ordinary-body path excludes figures. A schema containing
    # only that same narrative is not an excuse to invent a verified heading
    # witness or to change all previously accepted image-adjacent articles.
    value = extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE,
        schema_caption=''), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert value['body'] == BODY * 8 and value['verified_article_notices_json'] is None
    assert 'source_current_image_figure_numbered_heading_comparison_v1' not in value['article_identity_basis']


def test_heading_witness_does_not_mutate_either_tree_or_invent_notice_kind():
    original = BeautifulSoup(FIGURE, 'html.parser').figure
    before = str(original)
    key = _cnyes_structured_figure_identity(original)
    assert str(original) == before
    assert key == (TEXT, ((IMAGE,), ()), 'same_article_image_figure_numbered_heading')

