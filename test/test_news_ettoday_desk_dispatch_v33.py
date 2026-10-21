"""Pure desk prefixes cannot be destructively sent through a reporter/prose parser."""
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house
import test_news_ettoday_newsroom_v26 as desk
import test_news_ettoday_native_prefix_v30 as mixed
import test_news_ettoday_reviewed_desks_v32 as reviewed


SIGNATURES = (*reviewed.DESKS, '房產中心／綜合報導', '財經中心／台北報導')


def source(version=33, parser='lxml'):
    return replace(PolicyRegistry(house.config(version), base=Path.cwd()).sources['ettoday'], html_parser=parser)


def page(signature=reviewed.DESKS[0], parser='lxml', double_breaks=True):
    soup = reviewed.page(signature, parser, style='combined')
    caption = soup.select_one('#desk-prelude strong')
    # The actual failed prefix has a plain-text STRONG, not v26's nested BRs.
    caption.string = '▲合成建物。（圖／合成資料）'
    if double_breaks:
        caption.insert_before(soup.new_tag('br'))
        caption.insert_after(soup.new_tag('br'))
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=33):
    return extract_article(str(soup).encode(), source(version, parser), page_url=house.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('signature', SIGNATURES)
def test_pure_desk_with_extra_empty_separators_keeps_whole_prose_and_literal_org(parser, signature):
    soup = page(signature, parser)
    original = str(soup)
    actual, reference = extract(soup, parser), extract(page(signature, parser, double_breaks=False), parser, version=32)
    assert actual == reference
    assert json.loads(actual['authors_json']) == [signature.split('／')[0]]
    attribution, = json.loads(actual['author_attribution_detail_json'])
    assert attribution['role'] == 'newsroom' and attribution['native_credit'] == signature
    assert str(soup) == original
    with pytest.raises(NewsError) as failure:
        extract(soup, parser, version=32)
    assert getattr(failure.value, 'ettoday_attribution_failure_stage', None) == 'caption_credit_boundaries'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['unknown_desk', 'unreviewed_signature', 'desk_plus_prose',
    'tagged_desk', 'hidden_desk', 'hidden_image', 'hidden_caption', 'nested_caption_prose',
    'extra_precredit_prose', 'caption_comment', 'no_following_prose', 'foreign_source',
    'person_author', 'foreign_author', 'duplicate_credit', 'caption_embeds_reporter', 'script'])
def test_dispatch_does_not_relax_pure_prefix_ownership_or_prose_boundaries(parser, change):
    soup = page(parser=parser)
    prefix, caption = soup.select_one('#desk-prelude'), soup.select_one('#desk-prelude strong')
    if change == 'unknown_desk': prefix.contents[-1].replace_with('其他中心／綜合報導')
    elif change == 'unreviewed_signature': prefix.contents[-1].replace_with('財經中心／外電報導')
    elif change == 'desk_plus_prose': prefix.contents[-1].replace_with(reviewed.DESKS[0] + ' 不能丟掉的正文。')
    elif change == 'tagged_desk':
        node = soup.new_tag('b')
        node.string = str(prefix.contents[-1])
        prefix.contents[-1].replace_with(node)
    elif change == 'hidden_desk': prefix['hidden'] = ''
    elif change == 'hidden_image': prefix.find('img')['hidden'] = ''
    elif change == 'hidden_caption': caption['hidden'] = ''
    elif change == 'nested_caption_prose':
        node = soup.new_tag('span')
        node.string = '不能混入圖說移除的正文。'
        caption.append(node)
    elif change == 'extra_precredit_prose': prefix.insert(1, '不能丟掉的正文。')
    elif change == 'caption_comment': caption.append(BeautifulSoup('<!-- unknown -->', parser).contents[0])
    elif change == 'no_following_prose':
        for node in list(prefix.next_siblings): node.extract()
    elif change == 'foreign_source':
        node = soup.new_tag('p')
        node.string = '來源：中央社'
        soup.select_one('.story').append(node)
    elif change == 'person_author': native.update_schema(soup, author={'@type': 'Person', 'name': '財經中心'})
    elif change == 'foreign_author': native.update_schema(soup, author={'@type': 'Organization', 'name': '其他媒體'})
    elif change == 'duplicate_credit':
        node = soup.new_tag('p')
        node.string = reviewed.DESKS[0]
        soup.select_one('.story').append(node)
    elif change == 'caption_embeds_reporter': caption.string = '▲圖說 記者其他記者／台北報導'
    elif change == 'script': prefix.insert(1, soup.new_tag('script', src='/unknown.js'))
    else: raise AssertionError(change)
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind', ['mixed', 'pair'])
def test_real_reporter_and_prose_dispatch_keeps_existing_contract(parser, kind):
    soup = mixed.page(kind, parser)
    assert extract(soup, parser) == extract(soup, parser, version=32)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_existing_supported_desk_and_static_video_keep_all_fields(parser):
    for soup in (desk.page(parser), desk.page(parser, 'combined'), reviewed.youtube_page(parser)):
        assert extract(soup, parser) == extract(soup, parser, version=32)


def test_native_pure_prefix_all_fields_equal_between_both_html_engines():
    raw = str(page()).encode()
    assert extract_article(raw, source(parser='lxml'), page_url=house.URL) == extract_article(
        raw, source(parser='html.parser'), page_url=house.URL)


def test_capture_dispatch_version_is_not_new_source_permission_or_quota():
    old, new = source(32), source()
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_caption_literal_is_retained_not_sliced_by_a_guessed_text_boundary(parser):
    soup = page(parser=parser)
    caption = soup.select_one('#desk-prelude strong')
    caption.append(' 原始圖說的其他文字。')
    original = str(soup)
    actual = extract(soup, parser)
    assert actual == extract(soup, parser, version=32)
    assert json.loads(actual['verified_image_captions_json']) == [caption.get_text(' ', strip=True)]
    assert str(soup) == original
