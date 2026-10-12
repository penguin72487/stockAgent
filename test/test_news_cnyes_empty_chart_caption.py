"""A declared chart image with NBSP caption is a reference, not a caption."""
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import CAPTION, FIGURE, IMAGE, caption_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


CHART = 'https://cimg.cnyes.cool/prod/news/3947637/o/951b4327d0cb415b4940cd4071c1e78d.jpg'
EMPTY = f'<figure><img data-cnyeschart="cnyeschart" src="{CHART}">\n<figcaption>\xa0</figcaption>\n</figure>'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('first', [False, True])
def test_exact_visible_current_chart_keeps_narrative_and_real_caption(
        cnyes_config, tmp_path, parser, url, streamed, first):
    cnyes_config['providers'][0]['html_parser'] = parser
    figures = EMPTY + FIGURE if first else FIGURE + EMPTY
    value = extract_article(caption_page(current_figure=figures, visible_figure=figures,
        streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert value['body'] == BODY * 8
    assert value['published_at_raw'] == CLOCK
    assert json.loads(value['authors_json']) == [AUTHOR]
    assert json.loads(value['verified_image_captions_json']) == [{'text': CAPTION, 'image_src': IMAGE}]
    assert json.loads(value['verified_article_notices_json']) == [{
        'text': '', 'kind': 'same_article_empty_chart_caption_reference',
        'image_sources': [CHART], 'source_links': [],
    }]
    assert 'source_current_empty_chart_caption_reference_comparison_v1' in value['article_identity_basis']
    assert value['historical_point_in_time'] is False


@pytest.mark.parametrize('mutate', [
    lambda f: '',
    lambda f: f + f,
    lambda f: f.replace('951b4327', '951b4328'),
    lambda f: f.replace('<figure>', '<figure hidden>'),
    lambda f: f.replace('<figure>', '<figure class="unknown">'),
    lambda f: f.replace('<figure>', '<figure><!--unknown-->'),
    lambda f: f.replace('<img ', '<img hidden '),
    lambda f: f.replace('<img ', '<img aria-hidden="true" '),
    lambda f: f.replace('<figcaption>', '<figcaption hidden>'),
    lambda f: f.replace('<figcaption>', '<figcaption class="unknown">'),
    lambda f: f.replace('\xa0', '-123.45'),
    lambda f: f.replace('\xa0', ' '),
    lambda f: f.replace('\xa0', '\n\xa0\n'),
    lambda f: f.replace('\xa0', '<span>\xa0</span>'),
    lambda f: f.replace('data-cnyeschart="cnyeschart"', 'data-cnyeschart="unknown"'),
    lambda f: f.replace('data-cnyeschart="cnyeschart"', ''),
    lambda f: f.replace(CHART, 'javascript:unknown()'),
    lambda f: f.replace('</figure>', '<p>額外數值123.45</p></figure>'),
])
def test_chart_reference_requires_exact_visible_current_membership(cnyes_config, tmp_path, mutate):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE + EMPTY, visible_figure=FIGURE + mutate(EMPTY)),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('changed', [
    EMPTY.replace('data-cnyeschart="cnyeschart"', 'data-cnyeschart="unknown"'),
    EMPTY.replace('data-cnyeschart="cnyeschart"', ''),
    EMPTY.replace('\xa0', ' '),
    EMPTY.replace('\xa0', '\xa0\xa0'),
    EMPTY.replace('\xa0', '<span>\xa0</span>'),
    EMPTY.replace('<img ', '<img data-unreviewed="unknown" '),
    EMPTY.replace(CHART, 'https://cimg.cnyes.cool/unreviewed.png'),
    EMPTY.replace(CHART, 'https://user:secret@cimg.cnyes.cool/prod/news/3947637/o/951b4327d0cb415b4940cd4071c1e78d.jpg'),
])
def test_unreviewed_empty_image_shapes_do_not_become_accepted_placeholders(cnyes_config, tmp_path, changed):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE + changed, visible_figure=FIGURE + changed),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [CAPTION.replace('123', '124'), CAPTION + '額外值', CAPTION[::-1]])
def test_chart_never_waives_the_entire_schema(cnyes_config, tmp_path, schema):
    figures = FIGURE + EMPTY
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=figures, visible_figure=figures, schema_caption=schema),
            source(cnyes_config, tmp_path), page_url=MOBILE)
