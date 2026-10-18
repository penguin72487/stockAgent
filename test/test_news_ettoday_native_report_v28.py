"""Source-proved native report signatures; no account-only authors or prose loss."""
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
import test_news_ettoday_image_reporter_v27 as combined
import test_news_ettoday_newsroom_v26 as desk


def registry(version=28):
    return PolicyRegistry(house.config(version), base=Path.cwd())


def page(kind, parser='lxml'):
    if kind == 'caption_break':
        soup = combined.page(parser)
        prefix = soup.select_one('#desk-prelude')
        caption = prefix.find('strong')
        # Same literal image/caption/BR/reporter firstP as source903852.
        for child in list(prefix.contents[prefix.contents.index(caption) + 1:-1]):
            child.extract()
        caption.append(soup.new_tag('br'))
    else:
        soup = house.page(parser)
        soup.select_one('#credit').string = '研究記者／台北報導'
        native.update_schema(soup, creator=['1940-研究記者', '1940'])
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=28, url=house.URL):
    return extract_article(str(soup).encode(), replace(registry(version).sources['ettoday'], html_parser=parser),
        page_url=url)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind', ['caption_break', 'native_report'])
def test_literal_native_prose_authorship_clocks_and_captions_are_preserved(parser, kind):
    soup = page(kind, parser)
    before = str(soup)
    actual = extract(soup, parser)
    reference = extract(house.page(parser), parser, version=27)
    for key in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw',
                'verified_image_captions_json'):
        assert actual[key] == reference[key], key
    assert str(soup) == before
    detail, = json.loads(actual['author_attribution_detail_json'])
    assert detail['native_credit'] == ('記者研究記者／台北報導' if kind == 'caption_break' else '研究記者／台北報導')
    notes = json.loads(actual['verified_article_notices_json'])
    if kind == 'caption_break':
        assert any(n['kind'] == 'native_house_caption_trailing_break_reporter_separator' for n in notes)
    else:
        note, = [n for n in notes if n['kind'] == 'native_report_signature_with_matching_creator_witness']
        assert note['same_article_creator_name_and_identifier_required']
        assert note['role_basis'] == 'literal_visible_name_place_report_signature'
        assert not note['prefix_added_or_source_values_rewritten']
    assert not actual['historical_point_in_time'] and not actual['per_article_license_verified']
    with pytest.raises(NewsError):
        extract(soup, parser, version=27)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['no_break', 'break_attributes', 'caption_prose', 'missing_image',
    'duplicate_image', 'credit_markup', 'credit_prose', 'hidden_caption', 'hidden_credit', 'foreign_desk',
    'duplicate_credit', 'no_following_prose'])
def test_caption_break_is_a_structural_separator_not_permission_to_slice(parser, change):
    soup = page('caption_break', parser)
    prefix = soup.select_one('#desk-prelude')
    caption = prefix.find('strong')
    if change == 'no_break':
        for br in caption.find_all('br'): br.decompose()
    elif change == 'break_attributes': caption.find_all('br')[-1]['type'] = 'unreviewed'
    elif change == 'caption_prose': caption.append('不可刪除的額外正文。')
    elif change == 'missing_image': prefix.find('img').decompose()
    elif change == 'duplicate_image': prefix.insert(1, deepcopy(prefix.find('img')))
    elif change == 'credit_markup':
        tagged = soup.new_tag('span')
        tagged.string = str(prefix.contents[-1])
        prefix.contents[-1].replace_with(tagged)
    elif change == 'credit_prose': prefix.contents[-1].replace_with('記者研究記者／台北報導 不可刪除的正文。')
    elif change == 'hidden_caption': caption['hidden'] = ''
    elif change == 'hidden_credit': prefix['aria-hidden'] = 'true'
    elif change == 'foreign_desk': prefix.contents[-1].replace_with('財經中心／台北報導')
    elif change == 'duplicate_credit':
        node = soup.new_tag('p'); node.string = '記者研究記者／台北報導'; prefix.insert_after(node)
    elif change == 'no_following_prose':
        for node in list(prefix.find_next_siblings()): node.decompose()
    with pytest.raises(NewsError): extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['creator_missing', 'creator_name', 'creator_id', 'creator_number',
    'duplicate_creator', 'organization_only', 'account_only', 'credit_markup', 'credit_prose', 'credit_center',
    'credit_hidden', 'duplicate_credit', 'schema_missing', 'schema_duplicate', 'schema_author', 'schema_clock',
    'schema_url', 'canonical', 'rights', 'paid', 'unknown_script'])
def test_report_signature_never_gets_its_name_from_an_account_or_unknown_scope(parser, change):
    soup = page('native_report', parser)
    credit = soup.select_one('#credit')
    creator_cases = {'creator_missing': None, 'creator_name': ['1940-不同作者', '1940'],
        'creator_id': ['1940-研究記者', '1941'], 'creator_number': ['1940-研究記者', 1940],
        'duplicate_creator': ['1940-研究記者', '1940', '1940']}
    if change in creator_cases: native.update_schema(soup, creator=creator_cases[change])
    elif change == 'organization_only': native.update_schema(soup, creator=None)
    elif change == 'account_only': credit.string = ''
    elif change == 'credit_markup':
        node = soup.new_tag('b'); node.string = credit.get_text(); credit.clear(); credit.append(node)
    elif change == 'credit_prose': credit.string = '研究記者／台北報導 此處是正文。'
    elif change == 'credit_center': credit.string = '未知中心／台北報導'
    elif change == 'credit_hidden': credit['style'] = 'display:none'
    elif change == 'duplicate_credit':
        node = soup.new_tag('p'); node.string = credit.get_text(); credit.insert_after(node)
    elif change == 'schema_missing': soup.select_one('#report').decompose()
    elif change == 'schema_duplicate': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'schema_author': native.update_schema(soup, author={'@type': 'Person', 'name': '其他作者'})
    elif change == 'schema_clock': native.update_schema(soup, datePublished=native.MOD)
    elif change == 'schema_url': native.update_schema(soup, url=house.URL + '0')
    elif change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '0'
    elif change == 'rights': soup.select_one('p.note').decompose()
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'unknown_script': soup.select_one('.story').append(soup.new_tag('script', src='/unknown.js'))
    with pytest.raises(NewsError): extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('credit', ['圖文／鏡週刊', '圖文／CTWANT', '圖、文／業者提供'])
@pytest.mark.parametrize('tag', [None, 'b'])
def test_supplied_credit_is_not_missing_attribution_or_publisher_editor_authorship(parser, credit, tag):
    soup = house.page(parser)
    node = soup.select_one('#credit')
    if tag:
        wrapped = soup.new_tag(tag); wrapped.string = credit; node.clear(); node.append(wrapped)
    else: node.string = credit
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup, parser)
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(soup, parser, version=27)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('factory', [house.page, bare.bare_page, combined.page, desk.page])
def test_old_accepted_complete_values_are_not_changed(parser, factory):
    soup = factory(parser)
    assert extract(soup, parser) == extract(soup, parser, version=27)


def test_native_report_revision_changes_capture_not_authorization_or_http_capacity():
    old, new = registry(27).sources['ettoday'], registry().sources['ettoday']
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.data_fingerprint != old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
