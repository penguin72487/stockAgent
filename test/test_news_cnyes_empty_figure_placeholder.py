"""Bare NBSP and NBSP-caption source figures are distinct, ordered witnesses."""
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import MOBILE
from test_news_cnyes_captions import CAPTION, FIGURE, IMAGE, caption_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


BARE = '<figure>\xa0</figure>'
EMPTY_CAPTION = '<figure>\n<figcaption>\xa0</figcaption>\n</figure>'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('with_caption_placeholder', [False, True])
def test_same_current_visible_bare_template_does_not_drop_chart_caption_or_source_text(
        cnyes_config, tmp_path, parser, url, streamed, with_caption_placeholder):
    cnyes_config['providers'][0]['html_parser'] = parser
    figures = FIGURE + BARE + (EMPTY_CAPTION if with_caption_placeholder else '')
    result = extract_article(caption_page(current_figure=figures, visible_figure=figures,
        streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8 and result['published_at_raw'] == CLOCK
    assert json.loads(result['verified_image_captions_json']) == [{'text': CAPTION, 'image_src': IMAGE}]
    assert result['verified_article_notices_json'] is None
    assert 'source_current_empty_figure_placeholder_comparison_v1' in result['article_identity_basis']
    assert ('source_current_empty_caption_placeholder_comparison_v1' in result['article_identity_basis']) == with_caption_placeholder


@pytest.mark.parametrize('mutation', [
    'visible_missing', 'current_missing', 'order_conflict', 'duplicate', 'visible_extra',
    'text', 'number', 'img', 'nested', 'hidden', 'style', 'class', 'comment',
    'space', 'newline', 'two_nbsp', 'zero_width', 'unknown_wrapper', 'current_unknown',
])
def test_only_the_same_exact_one_nbsp_template_can_enter_schema_comparison(cnyes_config, tmp_path, mutation):
    current = visible = FIGURE + BARE + EMPTY_CAPTION
    alternatives = {
        'text': BARE.replace('\xa0', '來源敘事'),
        'number': BARE.replace('\xa0', '-123.45'),
        'img': BARE.replace('\xa0', '<img src="https://cimg.cnyes.cool/unknown.jpg">'),
        'nested': BARE.replace('\xa0', '<figure>\xa0</figure>'),
        'hidden': BARE.replace('<figure>', '<figure hidden>'),
        'style': BARE.replace('<figure>', '<figure style="display:none">'),
        'class': BARE.replace('<figure>', '<figure class="unknown">'),
        'comment': BARE.replace('\xa0', '<!--unknown-->\xa0'),
        'space': BARE.replace('\xa0', ' '),
        'newline': BARE.replace('\xa0', '\n\xa0\n'),
        'two_nbsp': BARE.replace('\xa0', '\xa0\xa0'),
        'zero_width': BARE.replace('\xa0', '\u200c'),
        'unknown_wrapper': BARE.replace('\xa0', '<span>\xa0</span>'),
    }
    if mutation == 'visible_missing':
        visible = FIGURE + EMPTY_CAPTION
    elif mutation == 'current_missing':
        current = FIGURE + EMPTY_CAPTION
    elif mutation == 'order_conflict':
        visible = FIGURE + EMPTY_CAPTION + BARE
    elif mutation == 'duplicate':
        current = visible = FIGURE + BARE + BARE + EMPTY_CAPTION
    elif mutation == 'visible_extra':
        current = FIGURE + EMPTY_CAPTION
    elif mutation == 'current_unknown':
        current = current.replace(BARE, '<figure>未驗證文字</figure>')
    else:
        visible = visible.replace(BARE, alternatives[mutation])
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=current, visible_figure=visible),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [CAPTION.replace('123', '124'), CAPTION + '額外文字', CAPTION[::-1]])
def test_templates_never_waive_entire_caption_or_schema_match(cnyes_config, tmp_path, schema):
    figures = FIGURE + BARE + EMPTY_CAPTION
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=figures, visible_figure=figures,
            schema_caption=schema), source(cnyes_config, tmp_path), page_url=MOBILE)
