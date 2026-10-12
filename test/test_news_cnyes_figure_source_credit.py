"""A source credit is kept, but cannot supply a new author or arbitrary prose."""
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_structured_figure_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import caption_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


IMAGE = 'https://cimg.cnyes.cool/owned-source-credit-image.jpg'
CREDIT = '文章來源: 測試投顧 - 測試分析師'
FIGURE = f'<figure><img src="{IMAGE}">\n<p><em><strong>{CREDIT}</strong></em></p>\n</figure>'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_source_credit_whole_text_and_identity_without_author_inference(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    value = extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE,
        schema_caption=CREDIT, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert value['body'] == BODY * 8 and value['published_at_raw'] == CLOCK
    assert json.loads(value['authors_json']) == [AUTHOR]
    assert value['verified_image_captions_json'] is None
    assert json.loads(value['verified_article_notices_json']) == [{
        'text': CREDIT, 'kind': 'same_article_image_figure_source_credit', 'image_sources': [IMAGE], 'source_links': [],
    }]
    assert 'source_current_image_figure_source_credit_comparison_v1' in value['article_identity_basis']


@pytest.mark.parametrize('mutate', [
    lambda f: f.replace('測試分析師', '不同分析師'),
    lambda f: f.replace(IMAGE, 'https://cimg.cnyes.cool/different-image.jpg'),
    lambda f: f.replace('<figure>', '<figure hidden>'),
    lambda f: f.replace('<figure>', '<figure class="unknown">'),
    lambda f: f.replace('<figure>', '<figure><!--unknown-->'),
    lambda f: f.replace('<p>', '<p hidden>'),
    lambda f: f.replace('<p>', '<p class="unknown">'),
    lambda f: f.replace('<em>', '<em hidden>'),
    lambda f: f.replace('<em>', '<em style="display:none">'),
    lambda f: f.replace('<strong>', '<strong aria-hidden="true">'),
    lambda f: f.replace('<strong>', '<strong class="unknown">'),
    lambda f: f.replace('<strong>', '<b>').replace('</strong>', '</b>'),
    lambda f: f.replace('<em>', '<span>').replace('</em>', '</span>'),
    lambda f: f.replace(CREDIT, '<span>' + CREDIT + '</span>'),
    lambda f: f.replace(CREDIT, CREDIT + '\n其他來源'),
    lambda f: f.replace('</figure>', '<p>額外正文123.45</p></figure>'),
    lambda f: f.replace(CREDIT, '不是来源署名'),
    lambda f: f.replace(IMAGE, 'javascript:unknown()'),
])
def test_unknown_credit_shape_or_changed_content_never_waives_schema(cnyes_config, tmp_path, mutate):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE, visible_figure=mutate(FIGURE),
            schema_caption=CREDIT), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [CREDIT.replace('測試', '不同'), CREDIT + '原數字456', CREDIT[::-1]])
def test_same_credit_still_requires_entire_schema(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE,
            schema_caption=schema), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_source_credit_witness_is_nonmutating_and_not_article_attribution():
    node = BeautifulSoup(FIGURE, 'lxml').figure
    original = str(node)
    assert _cnyes_structured_figure_identity(node) == (CREDIT, ((IMAGE,), ()), 'same_article_image_figure_source_credit')
    assert str(node) == original
