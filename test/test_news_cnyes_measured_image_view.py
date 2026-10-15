"""Measured native image forests are source non-prose, never accepted bodies."""
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import URL, source
from test_news_cnyes_canonical_alias import MOBILE
from test_news_cnyes_image_outcomes import IMAGE, page


def measured(*, width='600', height='358', ratio='1.675977653631285', path='l'):
    return ('<div>\n<p class="p_center"><img data-height="' + height
            + '" data-ratio="' + ratio + '" data-width="' + width + '" src="'
            + IMAGE.replace('/o/', '/' + path + '/') + '"></p>\n</div>\n\n<p>&nbsp;</p>')


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url,streamed,missing,desk', [
    (URL, False, True, True), (URL + '?exp=a', True, False, False),
    (MOBILE, False, False, True), (MOBILE + '?exp=b', True, True, False),
])
def test_actual_source_large_asset_shape_is_nonbody_not_a_parser_retry(cnyes_config, tmp_path, parser, url, streamed, missing, desk):
    cnyes_config['providers'][0]['html_parser'] = parser
    markup = measured()
    with pytest.raises(NewsError, match='^source_body_image_only$') as caught:
        extract_article(page(current=markup, visible=markup, streamed=streamed,
            schema_missing=missing, desk=desk), source(cnyes_config, tmp_path), page_url=url)
    assert caught.value.retry is False


@pytest.mark.parametrize('width,height', [(1, 1), (1920, 1080), (358, 600), (32768, 32768)])
@pytest.mark.parametrize('path', ['o', 'l'])
def test_bounded_source_measurements_are_not_hard_coded_to_one_chart(cnyes_config, tmp_path, width, height, path):
    markup = measured(width=str(width), height=str(height), ratio=str(width / height), path=path)
    with pytest.raises(NewsError, match='^source_body_image_only$'):
        extract_article(page(current=markup, visible=markup), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('changes', [
    {'width': '0'}, {'height': '0'}, {'width': '-600'}, {'height': '-358'},
    {'width': '0600'}, {'height': '0358'}, {'width': '600.0'}, {'height': '358.0'},
    {'width': ' 600'}, {'height': '358 '}, {'width': '32769'}, {'height': '32769'},
    {'width': '100000000000000000000000000000000'}, {'height': '1e100'},
    {'ratio': 'NaN'}, {'ratio': 'Infinity'}, {'ratio': '-1.675977653631285'},
    {'ratio': '1.675977653631286'}, {'ratio': '1'}, {'ratio': '1.6759776536312850'},
    {'ratio': '1.675977653631285 '}, {'path': 'x'}, {'path': 'original'},
])
def test_invalid_or_unknown_same_view_measurements_remain_unresolved(cnyes_config, tmp_path, changes):
    markup = measured(**changes)
    with pytest.raises(NewsError) as caught:
        extract_article(page(current=markup, visible=markup), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


@pytest.mark.parametrize('mutate', [
    lambda t: t.replace('data-height="358"', 'data-height="359"'),
    lambda t: t.replace('/l/', '/o/'),
    lambda t: t.replace('0' * 32, '1' * 32),
    lambda t: t.replace('/42/', '/43/'),
    lambda t: t.replace('cimg.cnyes.cool', 'unreviewed.test'),
    lambda t: t.replace('.jpg', '.jpg?access_key=unreviewed'),
    lambda t: t.replace('<img ', '<img hidden '),
    lambda t: t.replace('<img ', '<img data-other="unknown" '),
    lambda t: t.replace('<img ', '<img alt="a new unsupported attribute" '),
    lambda t: t.replace('<p class=', '<p aria-hidden="true" class='),
    lambda t: t + '<p>不能遺失的正文123</p>',
    lambda t: t + '<iframe src="https://other.test/video"></iframe>',
    lambda t: t + '<!--not source formatting-->',
    lambda t: t + t,
])
def test_entire_native_forest_and_all_original_attributes_must_still_match(cnyes_config, tmp_path, mutate):
    markup = measured()
    with pytest.raises(NewsError) as caught:
        extract_article(page(current=markup, visible=mutate(markup)), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


@pytest.mark.parametrize('changes', [{'payment': 1}, {'payment': False}, {'newsId': 43},
    {'publishAt': 1388534401}, {'author': 'unverified author'}, {'video': 'other-video'}])
def test_measured_shape_does_not_waive_source_identity_author_clock_or_access(cnyes_config, tmp_path, changes):
    markup = measured()
    with pytest.raises(NewsError) as caught:
        extract_article(page(current=markup, visible=markup, changes=changes), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


def test_stale_request_view_is_not_relabelled_from_a_primary_body(cnyes_config, tmp_path):
    markup = measured()
    with pytest.raises(NewsError) as caught:
        extract_article(page(current=markup, visible=markup), source(cnyes_config, tmp_path),
                        page_url=URL + '?unknown=1')
    assert caught.value.code != 'source_body_image_only'
