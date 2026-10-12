"""Observed same-image one-paragraph warning, not waived body disagreement."""
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_figure_notice_text
from test_news_cnyes_captions import (
    CAPTION_FREE_IMAGE, NOTICE_FIGURE, NOTICE_TEXT, caption_page,
)
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_html import BODY, CLOCK, URL, source
from test_cnyes_news_bulk import cfg as cnyes_config


SINGLE_NOTICE = NOTICE_FIGURE.replace('<p>&nbsp;</p>', '')


@pytest.mark.parametrize('url', [MOBILE, URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_observed_single_warning_retains_notice_and_whole_narrative(cnyes_config, tmp_path, url, streamed):
    result = extract_article(caption_page(current_figure=SINGLE_NOTICE, visible_figure=SINGLE_NOTICE,
        schema_caption=NOTICE_TEXT, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert result['published_at_raw'] == CLOCK
    assert result['verified_image_captions_json'] is None
    assert json.loads(result['verified_article_notices_json']) == [{
        'text': NOTICE_TEXT, 'image_src': CAPTION_FREE_IMAGE,
        'kind': 'same_article_investment_risk_disclaimer',
    }]
    assert 'source_current_figure_notice_schema_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('figure', [NOTICE_FIGURE, SINGLE_NOTICE])
def test_notice_matcher_does_not_mutate_either_observed_structure(parser, figure):
    node = BeautifulSoup(figure, parser).find('figure')
    before = str(node)
    assert _cnyes_figure_notice_text(node) == NOTICE_TEXT
    assert str(node) == before


@pytest.mark.parametrize('change', [
    lambda f: f.replace('推薦分析', '未知財務數字 789'),
    lambda f: f.replace('warning', 'warning other'),
    lambda f: f.replace('<p class="warning">', '<p class="warning" hidden>'),
    lambda f: f.replace('<br>', '<br style="display:none">', 1),
    lambda f: f.replace('投資人', '<span>投資人</span>'),
    lambda f: f.replace('</figure>', '<p>真正價格 123.45 不可排除</p></figure>'),
    lambda f: f.replace('</figure>', '<figcaption>未知圖說</figcaption></figure>'),
    lambda f: f.replace('without-caption', 'different-image'),
    lambda f: f.replace('<p class="warning">', '<!--未知內容--><p class="warning">'),
    lambda f: f.replace('</figure>', '<div>真正財務數字 100 不能消除</div></figure>'),
    lambda f: f.replace('<br>', '', 1),
])
def test_single_warning_keeps_visibility_structure_and_source_identity_guards(cnyes_config, tmp_path, change):
    payload = caption_page(current_figure=SINGLE_NOTICE, visible_figure=change(SINGLE_NOTICE),
                           schema_caption=NOTICE_TEXT)
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('body', [BODY * 8 + ' 未知真實價格123.45', BODY * 7])
def test_single_warning_does_not_hide_whole_body_or_number_disagreement(cnyes_config, tmp_path, body):
    payload = caption_page(current_figure=SINGLE_NOTICE, visible_figure=SINGLE_NOTICE,
                           schema_caption=NOTICE_TEXT, schema_body=body)
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


def test_matching_unknown_financial_paragraph_is_not_a_warning(cnyes_config, tmp_path):
    different = SINGLE_NOTICE.replace('本公司所推薦分析之個別有價證券', '本年度營收123元、發行股數456股')
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=different, visible_figure=different,
            schema_caption=NOTICE_TEXT.replace('本公司所推薦分析之個別有價證券', '本年度營收123元、發行股數456股')),
            source(cnyes_config, tmp_path), page_url=MOBILE)
