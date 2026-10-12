"""Typed chart forests retain labels/references, never synthesize market data."""
import html
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_typed_chart_caption_reference
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import CAPTION, FIGURE, IMAGE, caption_page
from test_news_cnyes_empty_chart_caption import CHART
from test_news_cnyes_html import BODY, CLOCK, URL, source


LABEL = '當篇公司日 K 線圖。'
LIVE_LABEL = '當篇公司走勢圖'
LIVE = ('https://so.cnyes.com/mobilechart/MobileChart_TodayLive.aspx?markettype=twstock'
        '&charttype1=linechart&stockcode=8105&imgwidth=580&imgheight=500&bgcolor=2')
KLINE = LIVE.replace('MobileChart_TodayLive.aspx', 'MobileChart.aspx').replace('linechart', 'kchart')
KIND = 'same_article_typed_chart_caption_reference'
BASIS = 'same_article_typed_chart_label_and_source_image'


def image(src=CHART):
    return f'<img data-cnyeschart="cnyeschart" src="{html.escape(src, quote=True)}">'


def nested(label=LABEL, src=CHART):
    return f'<figure>\n<figure>{image(src)}<figcaption>{label}</figcaption></figure>\n</figure>'


PARAGRAPH = f'<figure>{image()}<p>{LABEL}</p><p>\xa0</p><p>\xa0</p></figure>'
BEFORE = f'<figure><p>\xa0</p>{image()}<p>{LABEL}</p><p>\xa0</p></figure>'
EMPTY_WRAPPED = nested('\xa0').replace('</figure>\n</figure>', '</figure><figcaption>\xa0</figcaption>\n</figure>')
CASES = [(PARAGRAPH, LABEL, CHART), (BEFORE, LABEL, CHART),
         (nested(), LABEL, CHART), (nested(src=KLINE), LABEL, KLINE),
         (nested(LIVE_LABEL, LIVE), LIVE_LABEL, LIVE), ('<figure>' + nested() + '</figure>', LABEL, CHART),
         (EMPTY_WRAPPED, '', CHART)]


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('first', [False, True])
@pytest.mark.parametrize('chart,label,src', CASES)
def test_complete_typed_chart_forests_preserve_source_labels_body_and_clocks(
        cnyes_config, tmp_path, parser, url, streamed, first, chart, label, src):
    cnyes_config['providers'][0]['html_parser'] = parser
    figures = chart + FIGURE if first else FIGURE + chart
    labels = '\n'.join(filter(None, [label, CAPTION] if first else [CAPTION, label]))
    value = extract_article(caption_page(current_figure=figures, visible_figure=figures,
        schema_caption=labels, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert value['body'] == BODY * 8
    assert value['published_at_raw'] == CLOCK
    assert json.loads(value['authors_json']) == [AUTHOR]
    typed = [{'text': label, 'image_src': src, 'structure_basis': BASIS}] if label else []
    ordinary = [{'text': CAPTION, 'image_src': IMAGE}]
    assert json.loads(value['verified_image_captions_json']) == (typed + ordinary if first else ordinary + typed)
    assert json.loads(value['verified_article_notices_json']) == [
        {'text': '', 'kind': KIND, 'image_sources': [src], 'source_links': []}]
    assert 'source_current_typed_chart_caption_reference_comparison_v1' in value['article_identity_basis']
    assert 'source_current_nested_figure_schema_comparison_v1' not in value['article_identity_basis']
    assert value['historical_point_in_time'] is False


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('change', [
    lambda f: '', lambda f: f + f, lambda f: f.replace('951b4327', '951b4328'),
    lambda f: f.replace('<figure>', '<figure hidden>'),
    lambda f: f.replace('<figure>', '<figure class="unknown">'),
    lambda f: f.replace('<figure>', '<figure><!--unknown-->'),
    lambda f: f.replace('<img ', '<img hidden '),
    lambda f: f.replace('<img ', '<img aria-hidden="true" '),
    lambda f: f.replace('<figcaption>', '<figcaption hidden>'),
    lambda f: f.replace(LABEL, LABEL.replace('公司', '異文公司')),
    lambda f: f.replace('</figure>', '<p>額外 -123.45</p></figure>'),
])
def test_chart_requires_exact_whole_visible_current_membership(cnyes_config, tmp_path, parser, change):
    cnyes_config['providers'][0]['html_parser'] = parser
    chart = nested()
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE + chart, visible_figure=FIGURE + change(chart),
            schema_caption=CAPTION + '\n' + LABEL), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('changed', [
    PARAGRAPH.replace('data-cnyeschart="cnyeschart"', ''),
    PARAGRAPH.replace('cnyeschart"', 'unknown"'),
    PARAGRAPH.replace('\xa0', ' '), PARAGRAPH.replace('\xa0', '\xa0\xa0'),
    PARAGRAPH.replace('\xa0', '<span>\xa0</span>'),
    PARAGRAPH.replace(LABEL, '真的正文 -12.50 不能假稱圖說'),
    nested().replace('<img ', '<img data-unreviewed="unknown" '),
    nested().replace(CHART, 'https://cimg.cnyes.cool/unreviewed.png'),
    nested().replace(CHART, CHART + '?unknown=1'),
    nested().replace(CHART, 'javascript:unknown()'),
    nested().replace(CHART, CHART.replace('https://', 'https://user:secret@')),
    nested().replace('<figcaption>', '<figcaption><span>').replace('</figcaption>', '</span></figcaption>'),
    nested().replace('<figure>', '<figure><!--unknown-->'),
    '<figure><figure>' + nested() + '</figure></figure>',
    nested(src=KLINE + '&stockcode=2330'), nested(src=KLINE.replace('bgcolor=2', 'bgcolor=3')),
    EMPTY_WRAPPED.replace('\xa0', ' '), EMPTY_WRAPPED.replace('\xa0', '不存在的圖說'),
])
def test_unknown_shapes_are_not_reclassified_as_declared_chart(cnyes_config, tmp_path, changed):
    from bs4 import BeautifulSoup
    assert _cnyes_typed_chart_caption_reference(BeautifulSoup(changed, 'lxml').find('figure')) is None
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE + changed, visible_figure=FIGURE + changed,
            schema_caption=CAPTION + '\n' + LABEL), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [CAPTION, CAPTION + '\n' + LABEL + '-123.45', LABEL + '\n' + CAPTION])
def test_chart_never_waives_complete_schema(cnyes_config, tmp_path, schema):
    figures = FIGURE + nested()
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=figures, visible_figure=figures, schema_caption=schema),
            source(cnyes_config, tmp_path), page_url=MOBILE)


def test_typed_chart_does_not_change_existing_ordinary_caption_contract(cnyes_config, tmp_path):
    from bs4 import BeautifulSoup
    ordinary = f'<figure>{image()}<figcaption>{LABEL}</figcaption></figure>'
    assert _cnyes_typed_chart_caption_reference(BeautifulSoup(ordinary, 'lxml').find('figure')) is None
    value = extract_article(caption_page(current_figure=ordinary, visible_figure=ordinary, schema_caption=LABEL),
        source(cnyes_config, tmp_path), page_url=MOBILE)
    assert json.loads(value['verified_image_captions_json']) == [{'text': LABEL, 'image_src': CHART}]
    assert value['verified_article_notices_json'] is None
