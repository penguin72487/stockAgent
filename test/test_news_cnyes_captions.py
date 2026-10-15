"""A same-article figure caption is metadata, not missing narrative text."""
import html
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_news_cnyes_canonical_alias import MOBILE, mobile_page, streamed_mobile_page
from test_news_cnyes_html import BODY, CLOCK, URL, source
from test_cnyes_news_bulk import cfg as cnyes_config


CAPTION = '當篇公司日 K 線圖，成交量 123 股。'
IMAGE = 'https://so.cnyes.com/mobilechart/chart.png?stockcode=2330'
FIGURE = f'<figure><img src="{html.escape(IMAGE, quote=True)}"><figcaption>{CAPTION}</figcaption></figure>'
CAPTION_FREE_IMAGE = 'https://cimg.cnyes.cool/content-without-caption.png'
CAPTION_FREE_FIGURE = f'<figure><img src="{CAPTION_FREE_IMAGE}"></figure>'
NOTICE_LINES = (
    '本公司所推薦分析之個別有價證券',
    '無不當之財務利益關係 以往之績效不保證未來獲利',
    '投資人應獨立判斷 審慎評估並自負投資風險',
)
NOTICE_TEXT = '\n'.join(NOTICE_LINES)
NOTICE_FIGURE = (f'<figure><img src="{CAPTION_FREE_IMAGE}"><p>&nbsp;</p>'
                 '<p class="warning">' + '<br>'.join(NOTICE_LINES) + '</p></figure>')


def caption_page(*, body=BODY * 8, current_figure=FIGURE, visible_figure=FIGURE,
                 schema_caption=CAPTION, schema_body=None, cover=True, streamed=False):
    content = f'<p>{html.escape(body)}</p>' + current_figure
    declared = (body if schema_body is None else schema_body) + '\n' + schema_caption
    page = streamed_mobile_page if streamed else mobile_page
    payload = page(body=body, changes={'content': html.escape(content)},
                   schema_changes={'articleBody': declared})
    payload = payload.replace(b'</p></div></article>', ('</p>' + visible_figure + '</div></article>').encode())
    if cover:
        lead = '<figure><img src="https://cimg.cnyes.cool/cover.jpg"><figcaption>首圖說明不是當篇 content 圖說。</figcaption></figure>'
        payload = payload.replace(b'<div id="article-container">', ('<div id="article-container">' + lead).encode())
    return payload


def test_source_caption_is_separately_preserved_without_changing_narrative(cnyes_config, tmp_path):
    result = extract_article(caption_page(), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 8
    assert json.loads(result['verified_image_captions_json']) == [{'text': CAPTION, 'image_src': IMAGE}]
    assert 'source_current_figure_caption_schema_comparison_v1' in result['article_identity_basis']
    assert '首圖' not in result['body'] and '首圖' not in result['verified_image_captions_json']
    assert result['historical_point_in_time'] is False


@pytest.mark.parametrize('url', [MOBILE, URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_exact_current_empty_caption_placeholder_does_not_invalidate_real_caption(cnyes_config, tmp_path, url, streamed):
    placeholder = '<figure>\n<figcaption>\xa0</figcaption>\n</figure>'
    result = extract_article(caption_page(current_figure=FIGURE + placeholder, visible_figure=FIGURE + placeholder,
                                         streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8
    assert json.loads(result['verified_image_captions_json']) == [{'text': CAPTION, 'image_src': IMAGE}]
    assert 'source_current_empty_caption_placeholder_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('mutation', [
    'visible_missing', 'current_missing', 'order_conflict', 'duplicate', 'text', 'image',
    'nested', 'hidden', 'comment', 'unknown_whitespace', 'visible_extra',
])
def test_placeholder_still_requires_exact_empty_visible_current_ordered_membership(cnyes_config, tmp_path, mutation):
    placeholder = '<figure>\n<figcaption>\xa0</figcaption>\n</figure>'
    current = visible = FIGURE + placeholder
    if mutation == 'visible_missing':
        visible = FIGURE
    elif mutation == 'current_missing':
        current = FIGURE
    elif mutation == 'order_conflict':
        visible = placeholder + FIGURE
    elif mutation == 'duplicate':
        current = visible = FIGURE + placeholder * 2
    elif mutation in {'text', 'image', 'nested', 'hidden', 'comment', 'unknown_whitespace'}:
        changed = {
            'text': placeholder.replace('\xa0', '真實數字 -12.50'),
            'image': placeholder.replace('<figcaption>', '<img src="https://public.test/chart.png"><figcaption>'),
            'nested': placeholder.replace('<figcaption>', '<div><figcaption>').replace('</figcaption>', '</figcaption></div>'),
            'hidden': placeholder.replace('<figure>', '<figure hidden>'),
            'comment': placeholder.replace('<figcaption>', '<!-- incomplete --><figcaption>'),
            'unknown_whitespace': placeholder.replace('\xa0', ' '),
        }[mutation]
        current = visible = FIGURE + changed
    elif mutation == 'visible_extra':
        visible += placeholder
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=current, visible_figure=visible),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('first', [False, True])
def test_matching_caption_free_image_does_not_reject_an_independent_real_caption(cnyes_config, tmp_path, url, streamed, first):
    figures = CAPTION_FREE_FIGURE + FIGURE if first else FIGURE + CAPTION_FREE_FIGURE
    payload = caption_page(current_figure=figures, visible_figure=figures, streamed=streamed)
    result = extract_article(payload, source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8
    assert result['published_at_raw'] == CLOCK
    assert json.loads(result['verified_image_captions_json']) == [{'text': CAPTION, 'image_src': IMAGE}]
    assert 'source_current_figure_caption_schema_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('current_extra,visible_extra', [
    (CAPTION_FREE_FIGURE, ''),
    (CAPTION_FREE_FIGURE, CAPTION_FREE_FIGURE.replace('without-caption', 'different-image')),
    (CAPTION_FREE_FIGURE, CAPTION_FREE_FIGURE.replace('<figure>', '<figure hidden>')),
    (CAPTION_FREE_FIGURE, CAPTION_FREE_FIGURE.replace('<img ', '<img hidden ')),
    (CAPTION_FREE_FIGURE.replace('<img ', '<img style="display:none" '), CAPTION_FREE_FIGURE),
    (CAPTION_FREE_FIGURE, CAPTION_FREE_FIGURE.replace('</figure>', '<p>價格 123.45 不能省略</p></figure>')),
    (CAPTION_FREE_FIGURE.replace('</figure>', '<p>來源額外的財務數字 789</p></figure>'), CAPTION_FREE_FIGURE),
    (CAPTION_FREE_FIGURE + CAPTION_FREE_FIGURE, CAPTION_FREE_FIGURE + CAPTION_FREE_FIGURE),
    (CAPTION_FREE_FIGURE, CAPTION_FREE_FIGURE + CAPTION_FREE_FIGURE),
    (CAPTION_FREE_FIGURE, CAPTION_FREE_FIGURE.replace('</figure>', '<figcaption></figcaption></figure>')),
    (CAPTION_FREE_FIGURE.replace('https://', 'https://user:password@'),
     CAPTION_FREE_FIGURE.replace('https://', 'https://user:password@')),
])
def test_caption_free_image_still_requires_exact_visible_unique_textless_source_membership(cnyes_config, tmp_path, current_extra, visible_extra):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE + current_extra, visible_figure=FIGURE + visible_extra),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


def test_caption_free_images_cannot_hide_a_conflicting_source_figure_order(cnyes_config, tmp_path):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE + CAPTION_FREE_FIGURE,
                                     visible_figure=CAPTION_FREE_FIGURE + FIGURE),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('options', [
    {'visible_figure': FIGURE.replace('stockcode=2330', 'stockcode=2317')},
    {'visible_figure': FIGURE.replace(CAPTION, '別篇 caption 不能借用')},
    {'visible_figure': FIGURE.replace('<figure>', '<figure hidden>')},
    {'visible_figure': FIGURE.replace('<figure>', '<figure style="display:none">')},
    {'visible_figure': FIGURE.replace('<figcaption>', '<figcaption hidden>')},
    {'visible_figure': FIGURE.replace(CAPTION, '<span hidden>' + CAPTION + '</span>')},
    {'current_figure': FIGURE.replace('<figcaption>', '<figcaption style="display:none">')},
    {'visible_figure': FIGURE + FIGURE},
    {'visible_figure': '<figure>' + FIGURE + '</figure>'},
    {'current_figure': FIGURE + FIGURE, 'visible_figure': FIGURE + FIGURE, 'schema_caption': CAPTION * 2},
    {'visible_figure': FIGURE.replace('<figcaption>', '<div><figcaption>').replace('</figcaption>', '</figcaption></div>')},
    {'current_figure': FIGURE.replace('</figure>', '<p>真正財務數字不能當作圖說消除</p></figure>')},
    {'schema_caption': CAPTION + ' 未知的 schema 內容不能忽略'},
    {'current_figure': '', 'visible_figure': FIGURE},
    {'visible_figure': ''},
])
def test_unknown_hidden_ambiguous_or_conflicting_caption_is_not_waived(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError):
        extract_article(caption_page(**options), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_caption_comparison_also_preserves_actual_leading_format_marker(cnyes_config, tmp_path):
    body = '\ufeff' + BODY * 8
    result = extract_article(caption_page(body=body, schema_body=BODY * 8),
                             source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == body
    assert 'source_paragraph_format_marker_comparison_v1' in result['article_identity_basis']
    assert json.loads(result['verified_image_captions_json'])[0]['text'] == CAPTION


def test_unchanged_narrative_fast_path_does_no_caption_reparse(cnyes_config, tmp_path, monkeypatch):
    import downloader.news_fulltext_publishers as publishers
    monkeypatch.setattr(publishers, '_cnyes_caption_schema_view',
                        lambda *_a, **_k: pytest.fail('healthy matching body did redundant figure parsing'))
    result = extract_article(mobile_page(), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 8


def test_caption_materialization_has_an_explicit_source_grain_bound(cnyes_config, tmp_path):
    figures = ''.join(FIGURE.replace('stockcode=2330', f'stockcode={i}').replace(CAPTION, CAPTION + str(i))
                      for i in range(33))
    captions = '\n'.join(CAPTION + str(i) for i in range(33))
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=figures, visible_figure=figures, schema_caption=captions),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('url', [MOBILE, URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_same_source_figure_disclaimer_is_preserved_as_notice_not_a_caption(cnyes_config, tmp_path, url, streamed):
    payload = caption_page(current_figure=NOTICE_FIGURE, visible_figure=NOTICE_FIGURE,
                           schema_caption=NOTICE_TEXT, streamed=streamed)
    result = extract_article(payload, source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8
    assert result['verified_image_captions_json'] is None
    assert json.loads(result['verified_article_notices_json']) == [{
        'text': NOTICE_TEXT, 'image_src': CAPTION_FREE_IMAGE,
        'kind': 'same_article_investment_risk_disclaimer',
    }]
    assert 'source_current_figure_notice_schema_comparison_v1' in result['article_identity_basis']
    assert result['published_at_raw'] == CLOCK


@pytest.mark.parametrize('change', [
    lambda f: f.replace('推薦分析', '未知財務數字 789'),
    lambda f: f.replace('warning', 'other'),
    lambda f: f.replace('<p class="warning">', '<p class="warning" hidden>'),
    lambda f: f.replace('<br>', '<br style="display:none">', 1),
    lambda f: f.replace('投資人', '<span>投資人</span>'),
    lambda f: f.replace('<p>&nbsp;</p>', '<p>真正價格 123.45 不可排除</p>'),
    lambda f: f.replace('</figure>', '<p>真正財務數字 100 不能消除</p></figure>'),
    lambda f: f.replace('</figure>', '<figcaption>未知圖說</figcaption></figure>'),
    lambda f: f.replace('without-caption', 'different-image'),
])
def test_unknown_hidden_conflicting_figure_notice_never_waives_body_equality(cnyes_config, tmp_path, change):
    payload = caption_page(current_figure=NOTICE_FIGURE, visible_figure=change(NOTICE_FIGURE),
                           schema_caption=NOTICE_TEXT)
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


def test_matching_current_and_visible_financial_text_is_not_reclassified_as_risk_notice(cnyes_config, tmp_path):
    unrelated = NOTICE_FIGURE.replace(NOTICE_LINES[0], '本年度營收 123 股數 456 不可丟掉')
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=unrelated, visible_figure=unrelated,
                                     schema_caption=NOTICE_TEXT.replace(NOTICE_LINES[0], '本年度營收 123 股數 456 不可丟掉')),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('url', [MOBILE, URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('with_caption', [False, True])
def test_jsonld_literal_angle_text_requires_identical_current_visible_complete_body(cnyes_config, tmp_path, url, streamed, with_caption):
    body = BODY * 8 + '\n<Line@測試即點即加，一對一諮詢>：100 股\n<P1234> 不能被當標籤刪掉'
    if with_caption:
        payload = caption_page(body=body, streamed=streamed)
    else:
        page = streamed_mobile_page if streamed else mobile_page
        # Native content is encoded HTML, with angle text escaped inside it;
        # an actual <Line...> element would not be visible article text.
        payload = page(body=body, changes={'content': html.escape('<p>' + html.escape(body) + '</p>')})
    result = extract_article(payload, source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == body
    assert 'source_declared_article_body_literal_text_comparison_v1' in result['article_identity_basis']
    assert result['published_at_raw'] == CLOCK


@pytest.mark.parametrize('options', [
    {'visible': BODY * 8 + '<Line@價格 100>'},
    {'schema_changes': {'articleBody': BODY * 8 + '<Line@價格 101>'}},
    {'changes': {'content': html.escape('<p>' + html.escape(BODY * 8 + '<Line@價格 101>') + '</p>')}},
    {'changes': {'payment': 1}},
    {'changes': {'author': '另一篇作者'}},
])
def test_literal_schema_comparison_cannot_bypass_current_body_numbers_or_rights(cnyes_config, tmp_path, options):
    body = BODY * 8 + '<Line@價格 100> 真正原文不可忽略'
    with pytest.raises(NewsError):
        extract_article(mobile_page(body=body, **options), source(cnyes_config, tmp_path), page_url=MOBILE)


def test_matching_literal_text_fast_path_does_not_reparse_figures(cnyes_config, tmp_path, monkeypatch):
    import downloader.news_fulltext_publishers as publishers
    monkeypatch.setattr(publishers, '_cnyes_caption_schema_view',
                        lambda *_a, **_k: pytest.fail('literal body did redundant figure parsing'))
    body = BODY * 8 + '<Line@價格 100> 原始值'
    payload = mobile_page(body=body, changes={'content': html.escape('<p>' + html.escape(body) + '</p>')})
    assert extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)['body'] == body
