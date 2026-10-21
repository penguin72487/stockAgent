"""Lossless observed house prefixes and literal paired bare reporters."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house
import test_news_ettoday_house_native_v25 as bare
import test_news_ettoday_native_credit_v29 as prior


def source(version=30, parser='lxml'):
    return replace(PolicyRegistry(house.config(version), base=Path.cwd()).sources['ettoday'], html_parser=parser)


def page(kind, parser='lxml'):
    soup = bare.bare_page(parser) if kind == 'pair' else house.page(parser)
    if kind == 'pair':
        soup.select_one('#credit').string = '研究記者、另一記者／綜合報導'
    elif kind == 'mixed':
        prefix, caption, credit, prose = soup.select_one('.story').find_all('p', recursive=False)[:4]
        caption.find('strong').string = prior.CAPTION
        prefix['id'] = 'mixed-prefix'
        prefix.append(soup.new_tag('br')); prefix.append(soup.new_tag('br'))
        prefix.append(caption.find('strong').extract())
        prefix.append(soup.new_tag('br')); prefix.append(soup.new_tag('br'))
        prefix.append(credit.get_text())
        prefix.append(soup.new_tag('br')); prefix.append(soup.new_tag('br'))
        for child in list(prose.contents): prefix.append(child.extract())
        caption.decompose(); credit.decompose(); prose.decompose()
    else:
        raise AssertionError('unknown synthetic source layout')
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=30):
    return extract_article(str(soup).encode(), source(version, parser), page_url=house.URL)


@pytest.mark.parametrize('kind', ['mixed', 'pair'])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_native_nodes_names_values_captions_and_clocks_preserved(kind, parser):
    soup = page(kind, parser)
    original = str(soup)
    result = extract(soup, parser)
    reference_page = bare.bare_page(parser) if kind == 'pair' else house.page(parser)
    if kind == 'mixed': reference_page.select_one('.story > p:nth-of-type(2) strong').string = prior.CAPTION
    reference = extract(reference_page, parser, 29)
    for key in ('body', 'body_sha256', 'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert result[key] == reference[key], key
    assert json.loads(result['authors_json']) == (['研究記者', '另一記者'] if kind == 'pair' else ['研究記者'])
    assert str(soup) == original
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    if kind == 'pair':
        note, = [n for n in json.loads(result['verified_article_notices_json'])
            if n['kind'] == 'native_bare_reporter_pair_with_first_photo_and_creator_witness']
        assert not note['second_name_creator_identity_inferred']
    with pytest.raises(NewsError): extract(soup, parser, 29)


@pytest.mark.parametrize('kind', ['mixed', 'pair'])
def test_both_engines_agree_on_every_field(kind):
    raw = str(page(kind)).encode()
    assert extract_article(raw, source(parser='lxml'), page_url=house.URL) == extract_article(
        raw, source(parser='html.parser'), page_url=house.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['creator_missing', 'creator_name', 'creator_id', 'creator_type',
    'creator_extra', 'last_name_creator_only', 'photo_other_name', 'photo_role_absent', 'alt_only',
    'hidden_photo', 'hidden_credit', 'credit_markup', 'duplicate_name', 'third_name', 'center',
    'credit_prose', 'missing_schema', 'wrong_schema_author', 'paid', 'missing_rights', 'unknown_script'])
def test_pair_requires_visible_plain_literal_names_first_photo_and_exact_creator(parser, change):
    soup = page('pair', parser)
    credit, caption = soup.select_one('#credit'), soup.select_one('.story > p:nth-of-type(2) strong')
    creators = {'creator_missing': None, 'creator_name': ['1940-其他記者', '1940'],
        'creator_id': ['1940-研究記者', '1941'], 'creator_type': ['1940-研究記者', 1940],
        'creator_extra': ['1940-研究記者', '1940', '1940'], 'last_name_creator_only': ['1940-另一記者', '1940']}
    if change in creators: native.update_schema(soup, creator=creators[change])
    elif change == 'photo_other_name': caption.string = '▲照片。（圖／記者其他記者攝）'
    elif change == 'photo_role_absent': caption.string = '▲照片。（圖／研究記者攝）'
    elif change == 'alt_only':
        soup.select_one('.story img')['alt'] = caption.get_text(); caption.string = '▲照片。'
    elif change == 'hidden_photo': caption['hidden'] = ''
    elif change == 'hidden_credit': credit['style'] = 'display:none'
    elif change == 'credit_markup': credit.string.wrap(soup.new_tag('b'))
    elif change == 'duplicate_name': credit.string = '研究記者、研究記者／綜合報導'
    elif change == 'third_name': credit.string = '研究記者、另一記者、第三記者／綜合報導'
    elif change == 'center': credit.string = '研究記者、房產中心／綜合報導'
    elif change == 'credit_prose': credit.append(' 不可切除的正文。')
    elif change == 'missing_schema': soup.select_one('#report').decompose()
    elif change == 'wrong_schema_author': native.update_schema(soup, author={'@type':'Person','name':'其他作者'})
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'missing_rights': soup.select_one('p.note').decompose()
    elif change == 'unknown_script': soup.select_one('.story').append(soup.new_tag('script', src='/unknown.js'))
    with pytest.raises(NewsError): extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['hidden_prefix', 'hidden_image', 'unknown_break', 'extra_image',
    'prefix_prose', 'nested_caption', 'caption_prose', 'credit_markup', 'missing_separator',
    'credit_prose', 'second_credit', 'foreign_credit', 'unknown_script'])
def test_mixed_prefix_never_discards_unknown_nodes_or_guesses_a_boundary(parser, change):
    soup = page('mixed', parser)
    prefix = soup.select_one('#mixed-prefix')
    credit = next(c for c in prefix.contents if type(c).__name__ == 'NavigableString' and '記者研究記者／' in str(c))
    if change == 'hidden_prefix': prefix['hidden'] = ''
    elif change == 'hidden_image': prefix.find('img')['aria-hidden'] = 'true'
    elif change == 'unknown_break': prefix.find('br')['type'] = 'unreviewed'
    elif change == 'extra_image': prefix.insert(1, deepcopy(prefix.find('img')))
    elif change == 'prefix_prose': prefix.insert(1, '不能移除的正文。')
    elif change == 'nested_caption': prefix.find('strong').wrap(soup.new_tag('span'))
    elif change == 'caption_prose': prefix.find('strong').append('不能移除的正文。')
    elif change == 'credit_markup': credit.wrap(soup.new_tag('b'))
    elif change == 'missing_separator':
        for child in list(credit.next_siblings):
            if getattr(child, 'name', None) == 'br': child.decompose()
    elif change == 'credit_prose': credit.replace_with(str(credit) + ' 不能切除的正文。')
    elif change == 'second_credit': prefix.append(soup.new_tag('br')); prefix.append('記者其他作者／台北報導')
    elif change == 'foreign_credit': credit.replace_with('中央社／綜合報導')
    elif change == 'unknown_script': prefix.append(soup.new_tag('script', src='/unknown.js'))
    with pytest.raises(NewsError): extract(soup, parser)


@pytest.mark.parametrize('kind', prior.KINDS)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_completed_old_synthetic_layout_values_unchanged(kind, parser):
    soup = prior.page(kind, parser)
    assert extract(soup, parser) == extract(soup, parser, 29)


def test_capture_revision_preserves_authorization_and_capacity():
    old, new = source(29), source()
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
