"""Observed publisher desks are organizations, never inferred named reporters."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house


def registry(version=26):
    return PolicyRegistry(house.config(version), base=Path.cwd())


def extract(soup, parser='lxml', version=26, url=house.URL):
    return extract_article(str(soup).encode(), replace(registry(version).sources['ettoday'], html_parser=parser),
        page_url=url)


def page(parser='lxml', style='standalone'):
    soup = house.page(parser)
    credit = '房產中心／綜合報導' if style == 'standalone' else '財經中心／台北報導'
    native.update_schema(soup, author={'@type': 'Organization', 'name': 'ETtoday新聞雲',
        'url': 'https://www.ettoday.net/'}, creator=['1940-不同的帳號人名', '1940'])
    if style == 'standalone':
        soup.select_one('#credit').string = credit
    else:
        paragraphs = soup.select('.story > p')
        caption = paragraphs[1].get_text(' ', strip=True)
        prelude = BeautifulSoup('<p id="desk-prelude"><img src="//cdn2.ettoday.net/images/original.jpg"/>'
            '<br/>\n<strong><br/>\n' + caption + '\n<br/></strong><br/>\n' + credit + '</p>', parser).find('p')
        paragraphs[0].insert_before(prelude)
        for node in paragraphs[:3]:
            node.decompose()
    return BeautifulSoup(str(soup), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('style', ['standalone', 'combined'])
def test_exact_native_desk_preserves_whole_story_numbers_clocks_caption_and_rights(parser, style):
    soup = page(parser, style)
    original = str(soup)
    result = extract(soup, parser)
    old = extract(house.page(parser), parser, version=25)
    for key in ('body', 'body_sha256', 'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert result[key] == old[key], key
    name = '房產中心' if style == 'standalone' else '財經中心'
    assert json.loads(result['authors_json']) == [name]
    detail, = json.loads(result['author_attribution_detail_json'])
    assert detail['name'] == name and detail['role'] == 'newsroom'
    assert detail['basis'] == 'visible_current_story_publisher_newsroom_signature'
    assert detail['native_credit'] == name + ('／綜合報導' if style == 'standalone' else '／台北報導')
    assert '不同的帳號人名' not in result['authors_json']
    notes = json.loads(result['verified_article_notices_json'])
    signature, = [n for n in notes if n['kind'] == 'native_publisher_newsroom_signature']
    assert not signature['person_identity_inferred'] and not signature['schema_creator_promoted_to_reporter']
    assert any(n['kind'] == 'native_article_rights_notice' for n in notes)
    assert not any(n['kind'] == 'native_reporter_credit' for n in notes)
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert str(soup) == original
    with pytest.raises(NewsError):
        extract(soup, parser, version=25)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('style', ['standalone', 'combined'])
@pytest.mark.parametrize('change', ['unknown_desk', 'unobserved_signature', 'foreign_credit',
    'duplicate_credit', 'hidden_credit', 'caption_only', 'source_override', 'person_schema',
    'other_publisher_author', 'missing_schema', 'duplicate_schema', 'schema_url', 'schema_clock',
    'schema_publisher', 'paid', 'canonical', 'release', 'rights', 'extra_script'])
def test_desk_does_not_waive_scope_identity_visible_authorship_or_clocks(parser, style, change):
    soup = page(parser, style)
    credit = soup.select_one('#credit' if style == 'standalone' else '#desk-prelude')
    if change in {'unknown_desk', 'unobserved_signature', 'foreign_credit'}:
        replacement = {'unknown_desk': '未知中心／綜合報導', 'unobserved_signature': '房產中心／外電報導',
            'foreign_credit': '中央社／台北報導'}[change]
        if style == 'standalone':
            credit.string = replacement
        else:
            credit.contents[-1].replace_with(replacement)
    elif change == 'duplicate_credit':
        duplicate = soup.new_tag('p')
        duplicate.string = '房產中心／綜合報導'
        soup.select_one('.story').append(duplicate)
    elif change == 'hidden_credit':
        credit['hidden'] = ''
    elif change == 'caption_only':
        if style == 'standalone':
            credit.decompose()
        else:
            credit.contents[-1].extract()
    elif change == 'source_override':
        override = soup.new_tag('p')
        override.string = '來源：中央社'
        soup.select_one('.story').append(override)
    elif change == 'person_schema':
        native.update_schema(soup, author={'@type': 'Person', 'name': '房產中心'})
    elif change == 'other_publisher_author':
        native.update_schema(soup, author={'@type': 'Organization', 'name': '其他媒體', 'url': 'https://other.example/'})
    elif change == 'missing_schema':
        soup.select_one('#report').decompose()
    elif change == 'duplicate_schema':
        soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'schema_url':
        native.update_schema(soup, url=house.URL + '0')
    elif change == 'schema_clock':
        native.update_schema(soup, datePublished=native.MOD)
    elif change == 'schema_publisher':
        native.update_schema(soup, publisher={'@type': 'Organization', 'name': '其他媒體'})
    elif change == 'paid':
        native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'canonical':
        soup.select_one('link[rel=canonical]')['href'] += '0'
    elif change == 'release':
        soup.select_one('time').string = '2026-10-13 22:19'
    elif change == 'rights':
        soup.select_one('p.note').decompose()
    elif change == 'extra_script':
        soup.select_one('.story').append(soup.new_tag('script', src='/unknown.js'))
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['hidden_image', 'hidden_caption', 'nested_caption', 'extra_caption_prose',
    'extra_prose', 'before_caption', 'no_image', 'duplicate_image', 'no_following_prose', 'tagged_credit',
    'embedded_credit', 'caption_reporter'])
def test_image_prefix_has_exact_native_boundaries_and_cannot_delete_prose(parser, change):
    soup = page(parser, 'combined')
    prefix = soup.select_one('#desk-prelude')
    caption = prefix.find('strong')
    if change == 'hidden_image':
        prefix.find('img')['hidden'] = ''
    elif change == 'hidden_caption':
        caption['hidden'] = ''
    elif change == 'nested_caption':
        nested = soup.new_tag('span')
        nested.string = '▲原始圖說'
        caption.append(nested)
    elif change == 'extra_caption_prose':
        caption.append('這是不能刪除的實際正文。')
    elif change == 'extra_prose':
        prefix.insert(2, '不能刪除的實際正文。')
    elif change == 'before_caption':
        prefix.insert(0, '署名前的實際正文。')
    elif change == 'no_image':
        prefix.find('img').decompose()
    elif change == 'duplicate_image':
        prefix.insert(1, deepcopy(prefix.find('img')))
    elif change == 'no_following_prose':
        for node in list(prefix.find_next_siblings()):
            node.decompose()
    elif change == 'tagged_credit':
        tagged = soup.new_tag('strong')
        tagged.string = str(prefix.contents[-1])
        prefix.contents[-1].replace_with(tagged)
    elif change == 'embedded_credit':
        prefix.contents[-1].replace_with('財經中心／台北報導 不能併入署名的實際正文。')
    elif change == 'caption_reporter':
        caption.string = '▲圖說 記者其他記者／台北報導'
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_newsroom_scope_not_inferred_for_another_brand_route(parser, host):
    soup = native.page(host, parser)
    soup.select_one('#credit').string = '房產中心／綜合報導'
    native.update_schema(soup, author={'@type': 'Organization', 'name': 'ETtoday新聞雲',
        'url': 'https://www.ettoday.net/'})
    with pytest.raises(NewsError):
        extract(soup, parser, url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_previously_verified_named_house_output_unchanged(parser):
    assert extract(house.page(parser), parser) == extract(house.page(parser), parser, version=25)


def test_parser_ancestry_and_explicit_authority_change_remain_separate():
    old, parsed = registry(25).sources['ettoday'], registry().sources['ettoday']
    assert old.data_fingerprint == parsed.parser_repair_prior_capture_fingerprint
    assert old.authorization_fingerprint == parsed.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == parsed.http_capacity_source_fingerprint
    cfg = house.config(26)
    cfg['providers'][0]['authorization']['scope'] += '; explicitly reviewed native publisher desks only'
    reviewed = PolicyRegistry(cfg, base=Path.cwd()).sources['ettoday']
    assert reviewed.authorization_fingerprint != old.authorization_fingerprint
    assert reviewed.http_capacity_source_fingerprint != old.http_capacity_source_fingerprint
    assert reviewed.parser_repair_prior_capture_fingerprint != old.data_fingerprint
