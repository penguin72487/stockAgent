"""Static publisher previews do not supply text, people or media permissions."""
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
import test_news_ettoday_native_credit_v29 as prior


CAPTION = '▲合成建物預覽。（影、圖／合成業者提供，下同）'
PLAYER = 'https://www.ettoday.net/tools/player/12345-6789?title=%E5%90%88%E6%88%90&bid=boba_preroll_web&show_ad=1&uccu=3'


def source(version=31, parser='lxml'):
    return replace(PolicyRegistry(house.config(version), base=Path.cwd()).sources['ettoday'], html_parser=parser)


def page(parser='lxml', *, preview=True):
    soup = desk.page(parser)
    paragraphs = soup.select('.story > p')
    paragraphs[1].find('strong').string = CAPTION
    if preview:
        paragraphs[0].clear()
        frame = soup.new_tag('iframe', src=PLAYER, width='560', height='315', frameborder='0', scrolling='no',
            allowfullscreen='true', mozallowfullscreen='true', webkitallowfullscreen='true')
        paragraphs[0].append(frame)
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=31):
    return extract_article(str(soup).encode(), source(version, parser), page_url=house.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_whole_prose_native_caption_desk_clocks_and_preview_declaration_preserved(parser):
    soup = page(parser)
    original = str(soup)
    result = extract(soup, parser)
    reference = extract(page(parser, preview=False), parser, version=30)
    for key in ('body', 'body_sha256', 'authors_json', 'author_attribution_detail_json',
            'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert result[key] == reference[key], key
    notice, = [n for n in json.loads(result['verified_article_notices_json'])
        if n['kind'] == 'native_house_static_player_caption_prefix']
    assert notice['source_iframe_attributes'] == dict(soup.find('iframe').attrs)
    assert notice['native_caption'] == CAPTION
    assert not notice['media_requested_or_executed'] and not notice['media_content_accepted']
    assert not notice['media_title_promoted_to_prose_or_attribution']
    assert json.loads(result['authors_json']) == ['房產中心']
    assert json.loads(result['verified_image_captions_json']) == [CAPTION]
    assert 'media_not_requested_executed_or_transcribed' in result['body_completeness']
    assert str(soup) == original
    with pytest.raises(NewsError):
        extract(soup, parser, version=30)


def test_both_html_engines_agree_on_all_fields():
    raw = str(page()).encode()
    assert extract_article(raw, source(parser='lxml'), page_url=house.URL) == extract_article(
        raw, source(parser='html.parser'), page_url=house.URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', [
    'foreign_host', 'foreign_path', 'http', 'port', 'credentials', 'fragment', 'extra_query',
    'empty_title', 'duplicate_title', 'extra_frame_attr', 'hidden_frame', 'preview_attrs',
    'preview_prose', 'preview_comment', 'frame_fallback', 'second_frame', 'frame_script',
    'caption_prose', 'caption_markup', 'caption_attr', 'caption_container_attr', 'caption_comment',
    'caption_not_native', 'missing_caption', 'intervening_node', 'missing_credit',
    'credit_markup', 'unknown_desk', 'hidden_credit', 'person_schema', 'missing_schema',
    'missing_rights', 'paid', 'script_elsewhere', 'media_only',
])
def test_ambiguous_unsafe_or_incomplete_preludes_remain_rejected(parser, change):
    soup = page(parser)
    preview, caption, credit = soup.select('.story > p')[:3]
    frame = preview.find('iframe')
    urls = {
        'foreign_host': PLAYER.replace('www.ettoday.net', 'unreviewed.example'),
        'foreign_path': PLAYER.replace('/tools/player/', '/tools/other/'),
        'http': PLAYER.replace('https:', 'http:'),
        'port': PLAYER.replace('www.ettoday.net', 'www.ettoday.net:443'),
        'credentials': PLAYER.replace('www.ettoday.net', 'user@www.ettoday.net'),
        'fragment': PLAYER + '#fragment', 'extra_query': PLAYER + '&extra=1',
        'empty_title': PLAYER.replace('%E5%90%88%E6%88%90', ''),
        'duplicate_title': PLAYER.replace('&bid=', '&title=other&bid='),
    }
    if change in urls: frame['src'] = urls[change]
    elif change == 'extra_frame_attr': frame['onload'] = 'unreviewed()'
    elif change == 'hidden_frame': frame['hidden'] = ''
    elif change == 'preview_attrs': preview['style'] = 'display:none'
    elif change == 'preview_prose': preview.append('不可遺失的正文。')
    elif change == 'preview_comment': preview.append(BeautifulSoup('<!-- comment -->', parser).contents[0])
    elif change == 'frame_fallback': frame.append('不可遺失的替代正文。')
    elif change == 'second_frame': preview.append(deepcopy(frame))
    elif change == 'frame_script': preview.append(soup.new_tag('script', src='/unreviewed.js'))
    elif change == 'caption_prose': caption.find('strong').append('不可遺失的正文。')
    elif change == 'caption_markup': caption.find('strong').string.wrap(soup.new_tag('span'))
    elif change == 'caption_attr': caption.find('strong')['hidden'] = ''
    elif change == 'caption_container_attr': caption['aria-hidden'] = 'true'
    elif change == 'caption_comment': caption.find('strong').append(BeautifulSoup('<!-- comment -->', parser).contents[0])
    elif change == 'caption_not_native': caption.find('strong').string = '不能推定為圖說。'
    elif change == 'missing_caption': caption.decompose()
    elif change == 'intervening_node': caption.insert_before(soup.new_tag('div'))
    elif change == 'missing_credit': credit.decompose()
    elif change == 'credit_markup': credit.string.wrap(soup.new_tag('b'))
    elif change == 'unknown_desk': credit.string = '其他中心／綜合報導'
    elif change == 'hidden_credit': credit['hidden'] = ''
    elif change == 'person_schema': native.update_schema(soup, author={'@type': 'Person', 'name': '其他作者'})
    elif change == 'missing_schema': soup.select_one('#report').decompose()
    elif change == 'missing_rights': soup.select_one('p.note').decompose()
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'script_elsewhere': soup.select_one('.story').append(soup.new_tag('script', src='/other.js'))
    elif change == 'media_only':
        for node in list(credit.next_siblings): node.extract()
    else: raise AssertionError(change)
    with pytest.raises(NewsError):
        extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind', prior.KINDS)
def test_previously_supported_layouts_keep_every_value(parser, kind):
    soup = prior.page(kind, parser)
    assert extract(soup, parser) == extract(soup, parser, version=30)


def test_capture_version_changes_without_changing_source_scope_or_pacing():
    old, new = source(30), source()
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
