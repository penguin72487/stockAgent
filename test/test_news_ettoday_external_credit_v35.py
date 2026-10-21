"""Explicit supplied-source prefixes never become own-reporter body grants."""
from dataclasses import replace
from pathlib import Path

import pytest

from downloader.news_fulltext_ettoday import _finance_third_party_prelude
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house
import test_news_ettoday_native_credit_v29 as credits
import test_news_ettoday_desk_dispatch_v33 as dispatch
import test_news_ettoday_third_party_prelude_v34 as prior


KINDS = ('supplied', 'company', 'weekly_plain', 'weekly_strong')


def source(version=35, parser='lxml'):
    return replace(PolicyRegistry(house.config(version), base=Path.cwd()).sources['ettoday'], html_parser=parser)


def page(kind, parser='lxml'):
    soup = native.page(parser=parser)
    body, credit = soup.select_one('.story'), soup.select_one('#credit')
    if kind == 'supplied':
        body.select_one('p').decompose()
        body.select_one('p').decompose()
        credit.string = '圖、文／基金醫生提供'
    elif kind in {'company', 'weekly_plain'}:
        credit.string = '圖、文／大同公司提供' if kind == 'company' else '圖、文／鏡週刊'
    elif kind == 'weekly_strong':
        caption = body.select_one('strong')
        caption.string = '原站不含圖片符號的獨立圖說。'
        caption.append(soup.new_tag('br'))
        caption.append('\xa0')
        credit.clear()
        strong = soup.new_tag('strong'); strong.string = '圖文／鏡週刊'
        credit.append(strong)
    else:
        raise AssertionError(kind)
    return soup


def extract(soup, version=35, parser='lxml', url=native.URLS[0]):
    return extract_article(str(soup).encode(), source(version, parser), page_url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind', KINDS)
def test_observed_explicit_credit_is_classified_without_mutation_or_body(kind, parser):
    soup = page(kind, parser)
    before = str(soup)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract(soup, parser=parser)
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract(soup, version=34, parser=parser)
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind', KINDS)
@pytest.mark.parametrize('change', ['hidden_credit', 'hidden_body', 'nested_credit', 'linked_credit',
    'quoted_credit', 'mixed_credit', 'own_reporter', 'prose_before_prefix'])
def test_hidden_quoted_nested_or_nonprefix_credit_cannot_use_new_rule(kind, change, parser):
    soup = page(kind, parser)
    body, node = soup.select_one('.story'), soup.select_one('#credit')
    if change == 'hidden_credit': node['hidden'] = ''
    elif change == 'hidden_body': body['aria-hidden'] = 'true'
    elif change == 'nested_credit':
        target = node.find('strong') or node
        target.string.wrap(soup.new_tag('span'))
    elif change == 'linked_credit': node.wrap(soup.new_tag('a', href='https://example.test/'))
    elif change == 'quoted_credit':
        node.string = '記者在正文引用「' + node.get_text() + '」，不是署名。'
    elif change == 'mixed_credit': node.append('這一行另有原始正文，不得刪去。')
    elif change == 'own_reporter': node.string = '記者研究記者／台北報導'
    elif change == 'prose_before_prefix':
        paragraph = soup.new_tag('p'); paragraph.string = native.PROSE
        body.insert(0, paragraph)
    else: raise AssertionError(change)
    before = str(soup)
    try:
        assert not _finance_third_party_prelude(body, reviewed_external_credits=True)
    except NewsError as error:
        assert change.startswith('hidden_') and error.code != 'article_rights_or_scope_excluded'
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['hidden_image', 'hidden_caption', 'linked_caption', 'nonterminal_break',
    'break_attributes', 'nested_caption', 'mixed_image', 'caption_prose_after_break'])
def test_image_caption_shape_is_not_guessed(parser, change):
    soup = page('weekly_strong', parser)
    body, caption = soup.select_one('.story'), soup.select_one('.story strong')
    if change == 'hidden_image': body.select_one('img')['style'] = 'display:none'
    elif change == 'hidden_caption': caption['hidden'] = ''
    elif change == 'linked_caption': caption.wrap(soup.new_tag('a', href='https://example.test/'))
    elif change == 'nonterminal_break': caption.insert(0, soup.new_tag('br'))
    elif change == 'break_attributes': caption.select_one('br')['class'] = ['unexpected']
    elif change == 'nested_caption': caption.contents[0].wrap(soup.new_tag('em'))
    elif change == 'mixed_image': body.select_one('img').insert_after('正文不得移除')
    elif change == 'caption_prose_after_break': caption.append('這是正文而非空白')
    else: raise AssertionError(change)
    try:
        assert not _finance_third_party_prelude(body, reviewed_external_credits=True)
    except NewsError as error:
        assert change.startswith('hidden_') and error.code != 'article_rights_or_scope_excluded'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_source_typo_remains_unverified_and_prior_valid_values_do_not_change(parser):
    soup = native.page(parser=parser)
    soup.select_one('#credit').string = '記埋研究記者／台北報導'
    for version in (34, 35):
        with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
            extract(soup, version, parser)
    assert extract(native.page(parser=parser), 35, parser) == extract(native.page(parser=parser), 34, parser)
    for kind in credits.KINDS:
        raw = str(credits.page(kind, parser)).encode()
        assert extract_article(raw, source(35, parser), page_url=house.URL) == extract_article(
            raw, source(34, parser), page_url=house.URL)
    raw = str(dispatch.page(parser=parser)).encode()
    assert extract_article(raw, source(35, parser), page_url=house.URL) == extract_article(
        raw, source(34, parser), page_url=house.URL)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract(prior.page(parser), 35, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_www_scope_does_not_inherit_finance_credit_rule(parser):
    soup = native.page('www', parser)
    soup.select_one('#credit').string = '圖、文／基金醫生提供'
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract(soup, 35, parser, native.URLS[1])


def test_capture_version_changes_but_not_auth_or_http_identity():
    old, new = source(34), source()
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
