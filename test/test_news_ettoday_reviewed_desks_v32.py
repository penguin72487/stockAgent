"""Reviewed desk literals cannot become people, inferred authors or a scope waiver."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house
import test_news_ettoday_newsroom_v26 as desk
import test_news_ettoday_native_credit_v29 as credit
import test_news_ettoday_static_player_v31 as player


DESKS = ('財經中心／綜合報導', '消費中心／綜合報導', '房產中心／台北報導')
YOUTUBE = 'https://www.youtube.com/embed/Abcde_f-G12'


def source(version=32, parser='lxml'):
    return replace(PolicyRegistry(house.config(version), base=Path.cwd()).sources['ettoday'], html_parser=parser)


def page(signature, parser='lxml', style='standalone'):
    soup = desk.page(parser, style)
    node = soup.select_one('#credit' if style == 'standalone' else '#desk-prelude')
    if style == 'standalone':
        node.string = signature
    else:
        node.contents[-1].replace_with(signature)
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=32):
    return extract_article(str(soup).encode(), source(version, parser), page_url=house.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('signature', DESKS)
@pytest.mark.parametrize('style', ['standalone', 'combined'])
def test_reviewed_literals_preserve_whole_story_and_actual_organizational_attribution(parser, signature, style):
    soup = page(signature, parser, style)
    original = str(soup)
    actual, reference = extract(soup, parser), extract(desk.page(parser, style), parser, version=31)
    for key in ('body', 'body_sha256', 'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert actual[key] == reference[key], key
    name = signature.split('／')[0]
    assert json.loads(actual['authors_json']) == [name]
    attribution, = json.loads(actual['author_attribution_detail_json'])
    assert attribution['native_credit'] == signature and attribution['role'] == 'newsroom'
    assert attribution['name'] == name and '不同的帳號人名' not in actual['authors_json']
    notice, = [n for n in json.loads(actual['verified_article_notices_json'])
        if n['kind'] == 'native_publisher_newsroom_signature']
    assert not notice['person_identity_inferred'] and not notice['schema_creator_promoted_to_reporter']
    assert not actual['per_article_license_verified'] and not actual['historical_point_in_time']
    assert str(soup) == original
    with pytest.raises(NewsError):
        extract(soup, parser, version=31)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('signature', DESKS)
@pytest.mark.parametrize('change', ['unreviewed_desk', 'unreviewed_signature', 'person_author',
    'foreign_organization', 'missing_schema', 'duplicate_schema', 'foreign_source', 'hidden_credit',
    'nested_credit', 'duplicate_credit', 'paid', 'missing_rights', 'clock_mismatch', 'no_prose'])
def test_new_desk_does_not_waive_native_identity_ownership_or_visible_boundaries(parser, signature, change):
    soup = page(signature, parser)
    node = soup.select_one('#credit')
    if change == 'unreviewed_desk': node.string = '未核中心／綜合報導'
    elif change == 'unreviewed_signature': node.string = signature.split('／')[0] + '／外電報導'
    elif change == 'person_author': native.update_schema(soup, author={'@type': 'Person', 'name': signature.split('／')[0]})
    elif change == 'foreign_organization': native.update_schema(soup, author={'@type': 'Organization', 'name': '其他媒體'})
    elif change == 'missing_schema': soup.select_one('#report').decompose()
    elif change == 'duplicate_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'foreign_source':
        override = soup.new_tag('p')
        override.string = '來源：中央社'
        soup.select_one('.story').append(override)
    elif change == 'hidden_credit': node['hidden'] = ''
    elif change == 'nested_credit': node.string.wrap(soup.new_tag('b'))
    elif change == 'duplicate_credit': soup.select_one('.story').append(deepcopy(node))
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'missing_rights': soup.select_one('p.note').decompose()
    elif change == 'clock_mismatch': native.update_schema(soup, datePublished=native.MOD)
    elif change == 'no_prose':
        for paragraph in list(node.next_siblings): paragraph.extract()
    else: raise AssertionError(change)
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser)


@pytest.mark.parametrize('signature', DESKS)
def test_both_html_engines_agree_on_all_fields(signature):
    raw = str(page(signature)).encode()
    assert extract_article(raw, source(parser='lxml'), page_url=house.URL) == extract_article(
        raw, source(parser='html.parser'), page_url=house.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('signature', DESKS)
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_reviewed_house_desks_do_not_admit_another_brand_route(parser, signature, host):
    soup = native.page(host, parser)
    soup.select_one('#credit').string = signature
    native.update_schema(soup, author={'@type': 'Organization', 'name': 'ETtoday新聞雲',
        'url': 'https://www.ettoday.net/'})
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), source(parser=parser), page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind', credit.KINDS)
def test_previous_reporter_layouts_keep_all_values(parser, kind):
    soup = credit.page(kind, parser)
    assert extract(soup, parser) == extract(soup, parser, version=31)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_previous_desk_and_native_player_layouts_keep_all_values(parser):
    for soup in (desk.page(parser), desk.page(parser, 'combined'), player.page(parser)):
        assert extract(soup, parser) == extract(soup, parser, version=31)


def test_only_capture_contract_changes_not_authority_or_http_identity():
    old, new = source(31), source()
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint


def youtube_page(parser='lxml', preview=True):
    soup = page(DESKS[-1], parser)
    image, caption = soup.select('.story > p')[:2]
    caption.decompose()
    if preview:
        image.clear()
        image.append(soup.new_tag('iframe', src=YOUTUBE, width='560', height='315',
            title='YouTube video player', frameborder='0',
            allow='accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture',
            allowfullscreen=''))
    else:
        image.decompose()
    return BeautifulSoup(str(soup), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_native_youtube_declaration_without_caption_preserves_desk_and_all_prose(parser):
    soup = youtube_page(parser)
    original = str(soup)
    actual, reference = extract(soup, parser), extract(youtube_page(parser, preview=False), parser)
    for key in ('body', 'body_sha256', 'authors_json', 'author_attribution_detail_json',
            'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert actual[key] == reference[key], key
    notice, = [n for n in json.loads(actual['verified_article_notices_json'])
        if n['kind'] == 'native_house_static_youtube_desk_prefix']
    assert notice['source_iframe_attributes'] == dict(soup.find('iframe').attrs)
    assert not notice['native_caption_present']
    assert not notice['media_requested_or_executed'] and not notice['media_content_accepted']
    assert not notice['media_title_promoted_to_prose_or_attribution']
    assert 'no_caption_or_transcript_inferred' in actual['body_completeness']
    assert json.loads(actual['verified_image_captions_json']) == []
    assert str(soup) == original
    with pytest.raises(NewsError):
        extract(soup, parser, version=31)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['foreign_host', 'http', 'credentials', 'port', 'query', 'fragment',
    'invalid_id', 'extra_attr', 'hidden_frame', 'fallback_text', 'second_frame', 'preview_text',
    'preview_comment', 'preview_attrs', 'intervening_node', 'missing_credit', 'credit_markup',
    'unknown_credit', 'person_author', 'foreign_source', 'no_prose'])
def test_static_video_is_not_general_media_or_attribution_permission(parser, change):
    soup = youtube_page(parser)
    frame, preview, node = soup.find('iframe'), soup.select('.story > p')[0], soup.select_one('#credit')
    urls = {'foreign_host': YOUTUBE.replace('www.youtube.com', 'other.example'),
        'http': YOUTUBE.replace('https:', 'http:'),
        'credentials': YOUTUBE.replace('www.youtube.com', 'user@www.youtube.com'),
        'port': YOUTUBE.replace('www.youtube.com', 'www.youtube.com:443'),
        'query': YOUTUBE + '?autoplay=1', 'fragment': YOUTUBE + '#fragment', 'invalid_id': YOUTUBE + 'x'}
    if change in urls: frame['src'] = urls[change]
    elif change == 'extra_attr': frame['onload'] = 'unknown()'
    elif change == 'hidden_frame': frame['hidden'] = ''
    elif change == 'fallback_text': frame.append('不能當成正文或刪除的替代文字。')
    elif change == 'second_frame': preview.append(deepcopy(frame))
    elif change == 'preview_text': preview.append('不能隨預覽刪除的正文。')
    elif change == 'preview_comment': preview.append(BeautifulSoup('<!-- unknown -->', parser).contents[0])
    elif change == 'preview_attrs': preview['aria-hidden'] = 'true'
    elif change == 'intervening_node': node.insert_before(soup.new_tag('div'))
    elif change == 'missing_credit': node.decompose()
    elif change == 'credit_markup': node.string.wrap(soup.new_tag('b'))
    elif change == 'unknown_credit': node.string = '其他中心／台北報導'
    elif change == 'person_author': native.update_schema(soup, author={'@type': 'Person', 'name': '房產中心'})
    elif change == 'foreign_source':
        node.string = '中央社／台北報導'
    elif change == 'no_prose':
        for paragraph in list(node.next_siblings): paragraph.extract()
    else: raise AssertionError(change)
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser)


def test_static_youtube_all_fields_equal_between_html_engines():
    raw = str(youtube_page()).encode()
    assert extract_article(raw, source(parser='lxml'), page_url=house.URL) == extract_article(
        raw, source(parser='html.parser'), page_url=house.URL)
