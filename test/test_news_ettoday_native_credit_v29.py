"""Observed native roles and caption boundaries, without account-derived names."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house
import test_news_ettoday_native_report_v28 as previous


CAPTION = '▲保留圖片出處與123456股。（圖／原始提供）'
KINDS = ('caption_only', 'adjacent_fragments', 'separate_caption_credit', 'intern_caption', 'named_team')


def source(version=29, parser='lxml'):
    return replace(PolicyRegistry(house.config(version), base=Path.cwd()).sources['ettoday'], html_parser=parser)


def page(kind, parser='lxml'):
    soup = house.page(parser)
    body = soup.select_one('.story')
    image, caption, credit = body.find_all('p', recursive=False)[:3]
    caption.find('strong').string = CAPTION
    image['id'], caption['id'] = 'prefix-image', 'prefix-caption'
    if kind == 'caption_only':
        image.append(soup.new_tag('br'))
        image.append(caption.find('strong').extract())
        caption.decompose()
    elif kind == 'adjacent_fragments':
        caption.find('strong').string = '▲保留圖片出處與123456股。'
        image.append(soup.new_tag('br'))
        image.append(caption.find('strong').extract())
        part = soup.new_tag('strong'); part.string = '（圖／原始提供）'; image.append(part)
        image.append(soup.new_tag('br')); image.append(credit.get_text())
        caption.decompose(); credit.decompose()
    elif kind in ('separate_caption_credit', 'intern_caption'):
        text = ('實習記者研究記者／台北報導' if kind == 'intern_caption' else credit.get_text())
        if kind == 'intern_caption': caption.find('strong').append(soup.new_tag('br'))
        caption.append(soup.new_tag('br')); caption.append(text)
        credit.decompose()
    elif kind == 'named_team':
        credit.string = '網搜小組／研究記者報導'
    else:
        raise AssertionError('unknown synthetic native layout')
    native.update_schema(soup, creator=['1940-研究記者', '1940'])
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=29):
    return extract_article(str(soup).encode(), source(version, parser), page_url=house.URL)


@pytest.mark.parametrize('kind', KINDS)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_complete_values_caption_names_clocks_and_original_dom_survive(kind, parser):
    soup = page(kind, parser)
    original = str(soup)
    result = extract(soup, parser)
    assert original == str(soup)
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    assert '-12.5%、123456股、0.75' in result['body'] and '原始小標題' in result['body']
    assert json.loads(result['authors_json']) == ['研究記者']
    assert '網搜小組' not in result['authors_json'] and '實習記者' not in result['authors_json']
    assert (result['published_at_raw'], result['modified_at_raw']) == (native.PUB, native.MOD)
    captions = json.loads(result['verified_image_captions_json'])
    assert len(captions) == 1 and '▲保留圖片出處與123456股。' in captions[0] and '（圖／原始提供）' in captions[0]
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    if kind in ('intern_caption', 'named_team'):
        notices = json.loads(result['verified_article_notices_json'])
        role, = [n for n in notices if n['kind'] == 'native_house_role_named_reporter_with_creator_witness']
        assert role['native_reporter_names'] == ['研究記者']
        assert role['same_article_creator_name_and_identifier_required']
        assert not role['role_promoted_to_person_or_source_values_rewritten']
    with pytest.raises(NewsError): extract(soup, parser, version=28)


@pytest.mark.parametrize('kind', KINDS)
def test_both_html_engines_agree_on_every_field(kind):
    raw = str(page(kind)).encode()
    assert extract_article(raw, source(parser='lxml'), page_url=house.URL) == extract_article(
        raw, source(parser='html.parser'), page_url=house.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['extra_prose', 'extra_image', 'unknown_tag', 'nested_caption', 'byline_in_caption',
    'hidden_image', 'hidden_caption', 'hidden_prefix', 'missing_reporter', 'nonplain_reporter', 'duplicate_reporter',
    'attributed_break'])
def test_caption_only_does_not_drop_prose_or_infer_a_reporter(parser, change):
    soup = page('caption_only', parser)
    prefix, credit = soup.select_one('#prefix-image'), soup.select_one('#credit')
    if change == 'extra_prose': prefix.append('不能刪掉的原始正文。')
    elif change == 'extra_image': prefix.append(deepcopy(prefix.find('img')))
    elif change == 'unknown_tag': prefix.insert(1, soup.new_tag('span'))
    elif change == 'nested_caption': prefix.find('strong').wrap(soup.new_tag('span'))
    elif change == 'byline_in_caption': prefix.find('strong').string = '▲記者其他作者／台北報導。（圖／提供）'
    elif change == 'hidden_image': prefix.find('img')['hidden'] = ''
    elif change == 'hidden_caption': prefix.find('strong')['style'] = 'display:none'
    elif change == 'hidden_prefix': prefix['aria-hidden'] = 'true'
    elif change == 'missing_reporter': credit.decompose()
    elif change == 'nonplain_reporter': credit.string.wrap(soup.new_tag('b'))
    elif change == 'duplicate_reporter': credit.insert_after(deepcopy(credit))
    elif change == 'attributed_break': prefix.find('br')['type'] = 'unreviewed'
    with pytest.raises(NewsError): extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind', ['intern_caption', 'named_team'])
@pytest.mark.parametrize('change', ['creator_missing', 'creator_name', 'creator_id', 'creator_number',
    'creator_extra', 'unknown_role', 'account_without_visible_name', 'credit_markup', 'credit_prose',
    'credit_hidden', 'wrong_schema_author', 'missing_schema', 'duplicate_schema', 'wrong_clock',
    'missing_rights', 'paid', 'unknown_script'])
def test_named_role_requires_literal_visible_name_and_matching_creator(parser, kind, change):
    soup = page(kind, parser)
    credit = soup.select_one('#credit' if kind == 'named_team' else '#prefix-caption')
    creators = {'creator_missing': None, 'creator_name': ['1940-其他作者', '1940'],
        'creator_id': ['1940-研究記者', '1941'], 'creator_number': ['1940-研究記者', 1940],
        'creator_extra': ['1940-研究記者', '1940', '1940']}
    if change in creators: native.update_schema(soup, creator=creators[change])
    elif change == 'unknown_role': credit.contents[-1].replace_with('未知小組／研究記者報導')
    elif change == 'account_without_visible_name': credit.contents[-1].replace_with('網搜小組／報導')
    elif change == 'credit_markup': credit.contents[-1].wrap(soup.new_tag('b'))
    elif change == 'credit_prose': credit.contents[-1].replace_with(str(credit.contents[-1]) + ' 不可刪除的正文。')
    elif change == 'credit_hidden': credit['hidden'] = ''
    elif change == 'wrong_schema_author': native.update_schema(soup, author={'@type':'Person', 'name':'其他作者'})
    elif change == 'missing_schema': soup.select_one('#report').decompose()
    elif change == 'duplicate_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'wrong_clock': native.update_schema(soup, datePublished=native.MOD)
    elif change == 'missing_rights': soup.select_one('p.note').decompose()
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'unknown_script': soup.select_one('.story').append(soup.new_tag('script', src='/unknown.js'))
    with pytest.raises(NewsError): extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['separated_fragments', 'fragment_prose', 'nested_credit',
    'hidden_second_fragment', 'additional_text', 'missing_separator'])
def test_adjacent_caption_fragments_have_explicit_closed_boundaries(parser, change):
    soup = page('adjacent_fragments', parser)
    prefix = soup.select_one('#prefix-image')
    first, second = prefix.find_all('strong', recursive=False)
    if change == 'separated_fragments': second.insert_before(soup.new_tag('br'))
    elif change == 'fragment_prose': second.append('此處是正文。')
    elif change == 'nested_credit': second.string.wrap(soup.new_tag('span'))
    elif change == 'hidden_second_fragment': second['hidden'] = ''
    elif change == 'additional_text': first.insert_after('此處是正文。')
    elif change == 'missing_separator': second.find_next_sibling('br').decompose()
    with pytest.raises(NewsError): extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('marker_text', ['【廣編特輯】', '【企劃特輯】'])
def test_native_advertorial_label_is_scope_excluded_not_a_missing_person(parser, marker_text):
    soup = house.page(parser)
    marker = soup.new_tag('p'); marker.string = marker_text
    soup.select_one('.story').insert(0, marker)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'): extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('padding_type', ['_moz', None])
def test_house_editor_padding_does_not_turn_a_caption_into_a_credit_split(parser, padding_type):
    soup = page('named_team', parser)
    caption = soup.select_one('#prefix-caption')
    caption.attrs.clear()
    br = soup.new_tag('br', **({'type': padding_type} if padding_type else {}))
    caption.find('strong').append(br)
    caption.find('strong').append('\n')
    result = extract(BeautifulSoup(str(soup), parser), parser)
    assert json.loads(result['authors_json']) == ['研究記者']
    assert json.loads(result['verified_image_captions_json']) == [CAPTION]
    br['type'] = 'unreviewed'
    with pytest.raises(NewsError): extract(BeautifulSoup(str(soup), parser), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('factory', [house.page, lambda parser: previous.page('caption_break', parser),
    lambda parser: previous.page('native_report', parser)])
def test_completed_old_layout_values_are_unchanged_without_replaying_downloads(parser, factory):
    soup = factory(parser)
    assert extract(soup, parser) == extract(soup, parser, version=28)


def test_revision_changes_only_capture_identity_not_authorization_or_capacity():
    old, new = source(28), source()
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
