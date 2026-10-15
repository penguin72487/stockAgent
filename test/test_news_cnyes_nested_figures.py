"""Synthetic nested source forests; no publisher body or live HTTP."""
import html
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import MOBILE
from test_news_cnyes_captions import caption_page, NOTICE_LINES, NOTICE_TEXT
from test_news_cnyes_html import BODY, CLOCK, URL, source


IMAGES = [f'https://cimg.cnyes.cool/synthetic-image-{n}.png' for n in range(4)]
LEAVES = ''.join(f'<figure>\n<img src="{src}">\n</figure>' for src in IMAGES[1:])
WARNING = '<p class="warning">' + '<br>'.join(NOTICE_LINES) + '</p>'
CAPTION = '當篇原始資料圖說，成交量 123 股。'
CAPTION_FOREST = f'<figure><figure><img src="{IMAGES[1]}"></figure><figcaption>{CAPTION}</figcaption></figure>'
LINK = 'https://www.example.org/original-source?id=123'
SUPPLEMENT = ('<p>&lt;當篇作者附註&gt;</p>'
              f'<p>原始參考：<a href="{html.escape(LINK, quote=True)}">保留數值 456</a></p>')
SUPPLEMENT_TEXT = '<當篇作者附註>\n原始參考：\n保留數值 456'
BUNDLE = '<figure>' + LEAVES + WARNING + '</figure>'
SUPPLEMENT_BUNDLE = f'<figure><img src="{IMAGES[0]}">' + SUPPLEMENT + LEAVES + WARNING + '</figure>'
DIRECT_SUPPLEMENT = f'<figure><img src="{IMAGES[0]}">' + SUPPLEMENT + WARNING + '</figure>'


@pytest.mark.parametrize('url', [MOBILE, URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_one_nested_leaf_caption_has_exact_ordered_source_witness(cnyes_config, tmp_path, url, streamed):
    result = extract_article(caption_page(current_figure=CAPTION_FOREST, visible_figure=CAPTION_FOREST,
                                         schema_caption=CAPTION, streamed=streamed),
                             source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8
    assert result['published_at_raw'] == CLOCK
    assert json.loads(result['verified_image_captions_json']) == [{
        'text': CAPTION, 'image_src': IMAGES[1],
        'structure_basis': 'same_article_nested_image_figure_and_caption',
    }]
    assert 'source_current_nested_figure_schema_comparison_v1' in result['article_identity_basis']
    assert result['verified_article_notices_json'] is None


@pytest.mark.parametrize('url', [MOBILE, URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('supplement', [False, True])
def test_nested_images_and_complete_source_supplement_are_preserved_separately(cnyes_config, tmp_path, url, streamed, supplement):
    fragment = SUPPLEMENT_BUNDLE if supplement else BUNDLE
    text = SUPPLEMENT_TEXT + '\n' + NOTICE_TEXT if supplement else NOTICE_TEXT
    result = extract_article(caption_page(current_figure=fragment, visible_figure=fragment,
                                         schema_caption=text, streamed=streamed),
                             source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8
    assert result['verified_image_captions_json'] is None
    assert json.loads(result['verified_article_notices_json']) == [{
        'text': text, 'kind': 'same_article_nested_figure_text',
        'image_sources': IMAGES if supplement else IMAGES[1:],
        'source_links': [{'href': LINK, 'text': '保留數值 456'}] if supplement else [],
    }]
    assert 'source_current_nested_figure_schema_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('change', [
    lambda f: f.replace('image-1', 'another-image'),
    lambda f: f.replace('image-2', 'image-1'),
    lambda f: f.replace('<figure>\n', '<figure hidden>\n', 1),
    lambda f: f.replace('<img ', '<img hidden ', 1),
    lambda f: f.replace('<figure>\n', '<figure class="unreviewed">\n', 1),
    lambda f: f.replace('<figure>\n', '<figure><figure>\n', 1),
    lambda f: f.replace('<figure>\n', '<figure><!--unknown-->\n', 1),
    lambda f: f.replace('<figure>\n', '<figure>未宣告數值 789\n', 1),
    lambda f: f.replace('保留數值 456', '保留數值 789'),
    lambda f: f.replace(LINK, 'https://www.example.org/different-source'),
    lambda f: f.replace(LINK, 'https://user:password@example.org/private-source'),
    lambda f: f.replace('<p>&lt;', '<p hidden>&lt;'),
    lambda f: f.replace('<p>&lt;', '<p><!--unknown-->&lt;'),
    lambda f: f.replace('<p class="warning">', '<p class="other">'),
    lambda f: f.replace('<br>', '<br style="display:none">', 1),
    lambda f: f.replace(NOTICE_LINES[0], '不明財務數據 100 不得假裝為風險聲明'),
    lambda f: f.replace('</figure>', '<figcaption>額外圖說</figcaption></figure>', 1),
    lambda f: f.replace('<figure>\n<img src="' + IMAGES[1] + '">\n</figure>', ''),
    lambda f: f.replace(LEAVES, LEAVES + LEAVES),
    lambda f: f.replace(IMAGES[1], 'javascript:unknown()'),
    lambda f: f.replace(IMAGES[1], 'http://[invalid'),
    lambda f: f.replace(LINK, 'http://[invalid'),
])
def test_conflicting_hidden_unknown_nested_figures_are_not_a_schema_waiver(cnyes_config, tmp_path, change):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=SUPPLEMENT_BUNDLE,
                                     visible_figure=change(SUPPLEMENT_BUNDLE),
                                     schema_caption=SUPPLEMENT_TEXT + '\n' + NOTICE_TEXT),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('mutation', ['schema_value', 'schema_missing', 'schema_extra', 'unknown_warning', 'too_many_images'])
def test_matching_nested_layout_still_requires_complete_schema_and_known_warning(cnyes_config, tmp_path, mutation):
    fragment = SUPPLEMENT_BUNDLE
    text = SUPPLEMENT_TEXT + '\n' + NOTICE_TEXT
    if mutation == 'schema_value':
        text = text.replace('456', '789')
    elif mutation == 'schema_missing':
        text = NOTICE_TEXT
    elif mutation == 'schema_extra':
        text += '\n額外未公開資料 789'
    elif mutation == 'unknown_warning':
        fragment = fragment.replace(NOTICE_LINES[0], '未知財務數字 789 不得丟掉')
        text = text.replace(NOTICE_LINES[0], '未知財務數字 789 不得丟掉')
    else:
        leaves = ''.join(f'<figure><img src="https://cimg.cnyes.cool/image-{n}.png"></figure>' for n in range(33))
        fragment = '<figure>' + leaves + WARNING + '</figure>'
        text = NOTICE_TEXT
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=fragment, visible_figure=fragment, schema_caption=text),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


def test_nested_caption_comments_do_not_supply_a_known_template(cnyes_config, tmp_path):
    fragment = CAPTION_FOREST.replace('<figcaption>', '<figcaption><!--unreviewed-->')
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=fragment, visible_figure=fragment, schema_caption=CAPTION),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('url', [MOBILE, URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_direct_image_supplement_retains_all_text_and_links_as_distinct_kind(cnyes_config, tmp_path, url, streamed):
    text = SUPPLEMENT_TEXT + '\n' + NOTICE_TEXT
    result = extract_article(caption_page(current_figure=DIRECT_SUPPLEMENT, visible_figure=DIRECT_SUPPLEMENT,
                                         schema_caption=text, streamed=streamed),
                             source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8
    assert result['published_at_raw'] == CLOCK
    assert result['verified_image_captions_json'] is None
    assert json.loads(result['verified_article_notices_json']) == [{
        'text': text, 'kind': 'same_article_image_figure_text', 'image_sources': [IMAGES[0]],
        'source_links': [{'href': LINK, 'text': '保留數值 456'}],
    }]
    assert 'source_current_image_figure_supplement_schema_comparison_v1' in result['article_identity_basis']
    assert 'source_current_nested_figure_schema_comparison_v1' not in result['article_identity_basis']


@pytest.mark.parametrize('change', [
    lambda f: f.replace('image-0', 'other-image'),
    lambda f: f.replace('456', '789'),
    lambda f: f.replace(LINK, 'https://www.example.org/other-source'),
    lambda f: f.replace(LINK, 'https://user:secret@example.org/source'),
    lambda f: f.replace(LINK, 'http://[invalid'),
    lambda f: f.replace('<img ', '<img hidden ', 1),
    lambda f: f.replace('<p>&lt;', '<p hidden>&lt;'),
    lambda f: f.replace('<p>&lt;', '<p><!--unreviewed-->&lt;'),
    lambda f: f.replace('<p>&lt;', '<p class="unknown">&lt;'),
    lambda f: f.replace('<p class="warning">', '<p class="other">'),
    lambda f: f.replace('<br>', '<br hidden>', 1),
    lambda f: f.replace(NOTICE_LINES[0], '未知財務數值 789 不得直接略過'),
    lambda f: f.replace('<img ', '<div><img ', 1).replace('<p>&lt;', '</div><p>&lt;', 1),
    lambda f: f.replace('</figure>', '<figcaption>額外圖說</figcaption></figure>'),
    lambda f: f.replace('</figure>', f'<img src="{IMAGES[1]}"></figure>'),
    lambda f: f.replace('<a ', '<span><a ', 1).replace('</a>', '</a></span>', 1),
])
def test_direct_supplement_does_not_waive_identity_visibility_or_unknown_markup(cnyes_config, tmp_path, change):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=DIRECT_SUPPLEMENT, visible_figure=change(DIRECT_SUPPLEMENT),
                                     schema_caption=SUPPLEMENT_TEXT + '\n' + NOTICE_TEXT),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [NOTICE_TEXT, SUPPLEMENT_TEXT + '\n' + NOTICE_TEXT + '\n未公開資料 789'])
def test_direct_supplement_requires_whole_declared_body(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=DIRECT_SUPPLEMENT, visible_figure=DIRECT_SUPPLEMENT,
                                     schema_caption=schema), source(cnyes_config, tmp_path), page_url=MOBILE)
