"""Visible source exclusion precedes attribution dispatch, never accepts prose."""
from dataclasses import replace
from pathlib import Path

import pytest

from downloader.news_fulltext_ettoday import _finance_third_party_prelude
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house
import test_news_ettoday_native_credit_v29 as credit
import test_news_ettoday_desk_dispatch_v33 as dispatch


def source(version=34, parser='lxml'):
    return replace(PolicyRegistry(house.config(version), base=Path.cwd()).sources['ettoday'], html_parser=parser)


def page(parser='lxml'):
    soup = native.page(parser=parser)
    soup.select_one('.story > p strong').string = '▲原生圖片圖說。（圖／CTWANT提供）'
    soup.select_one('#credit').string = '圖文／CTWANT'
    return soup


def extract(soup, parser='lxml', version=34):
    return extract_article(str(soup).encode(), source(version, parser), page_url=native.URLS[0])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_native_finance_source_is_excluded_before_attribution_error(parser):
    soup = page(parser)
    before = str(soup)
    assert _finance_third_party_prelude(soup.select_one('.story'))
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract(soup, parser)
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract(soup, parser, version=33)
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['hidden_credit', 'hidden_caption', 'hidden_image', 'nested_credit',
    'nested_caption', 'wrong_literal', 'own_reporter', 'credit_in_caption', 'prose_before_credit',
    'own_credit_before_prefix', 'nonparagraph_prefix', 'source_link', 'extra_credit_text', 'mixed_image_text'])
def test_unobserved_hidden_quoted_or_mixed_layout_cannot_use_source_exclusion(parser, change):
    soup = page(parser)
    body, node = soup.select_one('.story'), soup.select_one('#credit')
    if change.startswith('hidden_'):
        target = {'hidden_credit': node, 'hidden_caption': body.select_one('strong'),
                  'hidden_image': body.select_one('img')}[change]
        target['hidden'] = ''
    elif change == 'nested_credit': node.string.wrap(soup.new_tag('strong'))
    elif change == 'nested_caption': body.select_one('strong').string.wrap(soup.new_tag('span'))
    elif change == 'wrong_literal': node.string = '圖文／其他來源'
    elif change == 'own_reporter': node.string = '記者研究記者／台北報導'
    elif change == 'credit_in_caption':
        body.select_one('strong').string = '▲引用字面「圖文／CTWANT」不是署名。'
        node.string = '記者研究記者／台北報導'
    elif change == 'prose_before_credit':
        paragraph = soup.new_tag('p'); paragraph.string = native.PROSE
        node.insert_before(paragraph)
    elif change == 'own_credit_before_prefix':
        paragraph = soup.new_tag('p'); paragraph.string = '記者研究記者／台北報導'
        body.insert(0, paragraph)
    elif change == 'nonparagraph_prefix':
        paragraph = soup.new_tag('div'); paragraph.string = '不能略過的來源節點'
        body.insert(0, paragraph)
    elif change == 'source_link': node.string.wrap(soup.new_tag('a', href='https://example.test/'))
    elif change == 'extra_credit_text': node.append('並非純署名')
    elif change == 'mixed_image_text': body.select_one('img').insert_after('原生內文不得刪除')
    else: raise AssertionError(change)
    before = str(soup)
    try:
        assert not _finance_third_party_prelude(body)
    except NewsError as error:
        assert change.startswith('hidden_') and error.code != 'article_rights_or_scope_excluded'
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_same_body_on_www_is_not_a_finance_scope_rule(parser):
    soup = native.page('www', parser)
    soup.select_one('#credit').string = '圖文／CTWANT'
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract_article(str(soup).encode(), source(parser=parser), page_url=native.URLS[1])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_existing_reporter_values_and_house_desk_dispatch_unchanged(parser):
    soup = native.page(parser=parser)
    assert extract(soup, parser) == extract(soup, parser, version=33)
    for kind in credit.KINDS:
        raw = str(credit.page(kind, parser)).encode()
        assert extract_article(raw, source(parser=parser), page_url=house.URL) == extract_article(
            raw, source(33, parser), page_url=house.URL)
    raw = str(dispatch.page(parser=parser)).encode()
    assert extract_article(raw, source(parser=parser), page_url=house.URL) == extract_article(
        raw, source(33, parser), page_url=house.URL)


def test_capture_version_changes_without_auth_or_request_identity_change():
    old, new = source(33), source()
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint


def test_both_engines_reject_visible_third_party_without_returning_a_body():
    raw = str(page()).encode()
    for parser in ('lxml', 'html.parser'):
        with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
            extract_article(raw, source(parser=parser), page_url=native.URLS[0])
