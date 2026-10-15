"""Observed image forests use the shared classifier, never synthetic bodies."""

import pytest

from downloader import news_fulltext_publishers as publishers
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL
from test_news_cnyes_canonical_alias import MOBILE
from test_news_cnyes_image_outcomes import IMAGE, page


WIDTH = f'<p align="center"><img src="{IMAGE}" alt="" width="440"></p>'
STYLE = (f'<p align="center"><img style="height: 1420px; width: 300px; filter: none; border: #aaaaaa 1px solid;" '
         f'src="{IMAGE}" border="0" alt="已觀測圖表標籤" hspace="0" width="300" height="1420"></p>')
GALLERY = '<p align="center">' + ' '.join(
    f'<img src="{IMAGE.replace("0" * 32, str(n) * 32)}" alt="">' for n in range(3)) + '</p>'
MEASURED = (f'<div>\n<p class="p_center"><img data-height="546" data-ratio="1.098901098901099" '
            f'data-width="600" src="{IMAGE}"></p>\n</div>\n\n<p>&nbsp;</p>')
SHAPES = [WIDTH, STYLE, GALLERY, MEASURED]


def policy(cnyes_config, tmp_path, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    return PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']


@pytest.mark.parametrize('markup', SHAPES)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [URL, URL + '?exp=a', MOBILE, MOBILE + '?exp=a', MOBILE + '?exp=b'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('schema_missing', [False, True])
@pytest.mark.parametrize('desk', [False, True])
def test_matching_whole_observed_forest_remains_nonbody(cnyes_config, tmp_path, markup, parser, url, streamed, schema_missing, desk):
    with pytest.raises(NewsError, match='^source_body_image_only$') as caught:
        extract_article(page(current=markup, visible=markup, streamed=streamed,
            schema_missing=schema_missing, desk=desk), policy(cnyes_config, tmp_path, parser), page_url=url)
    assert caught.value.retry is False


@pytest.mark.parametrize('markup', SHAPES)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('mutate', [
    lambda text: text.replace(IMAGE, IMAGE.replace('/42/', '/43/')),
    lambda text: text.replace('0' * 32, 'f' * 32),
    lambda text: text.replace('<img ', '<img hidden '),
    lambda text: text.replace('<p ', '<p hidden '),
    lambda text: text.replace('<img ', '<!--not-reviewed--><img '),
    lambda text: text + '<p>不可以丟棄的正文。</p>',
    lambda text: text + '<iframe src="https://unreviewed.test/video"></iframe>',
    lambda text: '<div>' + text + '</div>',
    lambda text: text.replace('<img ', '<img data-unreviewed="true" '),
    lambda text: text.replace(IMAGE, IMAGE + '?private-token=unknown'),
    lambda text: text.replace(IMAGE, IMAGE.replace('cimg.cnyes.cool', 'other.test')),
    lambda text: text + text,
])
def test_changed_hidden_text_media_or_whole_forest_is_not_discarded(cnyes_config, tmp_path, markup, parser, mutate):
    with pytest.raises(NewsError) as caught:
        extract_article(page(current=markup, visible=mutate(markup)),
            policy(cnyes_config, tmp_path, parser), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


@pytest.mark.parametrize('markup', SHAPES)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('changes', [
    {'payment': 1}, {'payment': False}, {'newsId': 99}, {'publishAt': 1388534401},
    {'author': '不相符作者'}, {'video': 'external-video'}, {'url': URL.replace('/42', '/43')},
])
def test_forest_does_not_replace_original_identity_and_rights_guard(cnyes_config, tmp_path, markup, parser, changes):
    with pytest.raises(NewsError) as caught:
        extract_article(page(current=markup, visible=markup, changes=changes),
            policy(cnyes_config, tmp_path, parser), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


@pytest.mark.parametrize('markup', SHAPES)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('schema', [
    {'articleBody': '不可丟棄的來源正文'}, {'articleBody': None},
    {'datePublished': '2014-01-01T00:00:00'}, {'datePublished': '2014-01-01T00:00:01.000Z'},
    {'isAccessibleForFree': False}, {'author': [{'name': '不相符作者'}]},
    {'mainEntityOfPage': {'@id': URL.replace('/42', '/43')}},
])
def test_conflicting_schema_stays_unresolved(cnyes_config, tmp_path, markup, parser, schema):
    with pytest.raises(NewsError) as caught:
        extract_article(page(current=markup, visible=markup, schema=schema),
            policy(cnyes_config, tmp_path, parser), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


@pytest.mark.parametrize('markup', SHAPES)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_observed_taipei_desk_credit_does_not_invent_visible_named_author(cnyes_config, tmp_path, markup, parser):
    payload = page(current=markup, visible=markup, desk=True).replace(
        '鉅亨網新聞中心'.encode(), '鉅亨台北資料中心'.encode())
    with pytest.raises(NewsError, match='^source_body_image_only$') as caught:
        extract_article(payload, policy(cnyes_config, tmp_path, parser), page_url=URL)
    assert caught.value.retry is False


@pytest.mark.parametrize('byline', ['鉅亨台北資料中心 其他來源', '台北資料中心', '無法確認的作者', ''])
def test_unknown_desk_credit_not_generalized(cnyes_config, tmp_path, byline):
    payload = page(current=MEASURED, visible=MEASURED, desk=True).replace(
        '鉅亨網新聞中心'.encode(), byline.encode())
    with pytest.raises(NewsError) as caught:
        extract_article(payload, policy(cnyes_config, tmp_path, 'lxml'), page_url=URL)
    assert caught.value.code != 'source_body_image_only'
