"""Visible name/place signatures need the same article's creator witness."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
import test_news_ettoday as native


def source(host='finance', parser='lxml', version=37):
    config = native.config(parser)
    config['providers'][0]['embedded_article_layout'] = f'ettoday_visible_article_v{version}'
    return replace(PolicyRegistry(config, base=Path.cwd()).sources['ettoday'], html_parser=parser)


def page(host='finance', parser='lxml'):
    soup = native.page(host, parser)
    soup.select_one('#credit').string = '研究記者／台北報導'
    native.update_schema(soup, creator=['1940-研究記者', '1940'])
    return BeautifulSoup(str(soup), parser)


def extract(soup, host='finance', parser='lxml', version=37):
    return extract_article(str(soup).encode(), source(host, parser, version), page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_bare_signature_preserves_prose_values_clocks_and_actual_byline(host, parser):
    soup = page(host, parser)
    original = str(soup)
    actual = extract(soup, host, parser)
    reference = deepcopy(soup)
    reference.select_one('#credit').string = '記者研究記者／台北報導'
    old = extract(reference, host, parser, version=36)
    for key in ('body', 'body_sha256', 'published_at_raw', 'modified_at_raw', 'authors_json',
                'verified_image_captions_json'):
        assert actual[key] == old[key]
    assert str(soup) == original
    detail, = json.loads(actual['author_attribution_detail_json'])
    assert detail['native_credit'] == '研究記者／台北報導'
    notes = json.loads(actual['verified_article_notices_json'])
    note, = [n for n in notes if n['kind'] == 'native_report_signature_with_matching_creator_witness']
    assert note['same_article_creator_name_and_identifier_required']
    assert not note['prefix_added_or_source_values_rewritten']
    assert not actual['historical_point_in_time'] and not actual['per_article_license_verified']
    with pytest.raises(NewsError):
        extract(soup, host, parser, version=36)


@pytest.mark.parametrize('host', ['finance', 'www'])
def test_dual_engines_agree_on_every_field(host):
    raw = str(page(host)).encode()
    assert extract_article(raw, source(host), page_url=native.URLS[host == 'www']) == extract_article(
        raw, source(host, 'html.parser'), page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('change', ['creator_missing', 'creator_name', 'creator_id', 'creator_type',
    'creator_extra', 'tagged_credit', 'hidden_credit', 'credit_prose', 'credit_prefix', 'desk',
    'duplicate_credit', 'late_credit', 'no_visible_name', 'foreign_author', 'missing_schema',
    'second_schema', 'wrong_clock', 'paid', 'before_credit', 'hidden_prose'])
def test_native_signature_never_comes_from_an_account_or_relaxes_scope(host, parser, change):
    soup = page(host, parser)
    credit = soup.select_one('#credit')
    creators = {'creator_missing': None, 'creator_name': ['1940-其他作者', '1940'],
        'creator_id': ['1940-研究記者', '1941'], 'creator_type': ['1940-研究記者', 1940],
        'creator_extra': ['1940-研究記者', '1940', 'extra']}
    if change in creators:
        native.update_schema(soup, creator=creators[change])
    elif change == 'tagged_credit': credit.string.wrap(soup.new_tag('b'))
    elif change == 'hidden_credit': credit['hidden'] = ''
    elif change == 'credit_prose': credit.append('不能刪掉的正文。')
    elif change == 'credit_prefix': credit.string = '來源：研究記者／台北報導'
    elif change == 'desk': credit.string = '其他中心／台北報導'
    elif change == 'duplicate_credit': credit.insert_after(deepcopy(credit))
    elif change == 'late_credit':
        for _ in range(4): credit.insert_before(soup.new_tag('p'))
    elif change == 'no_visible_name': credit.string = '／台北報導'
    elif change == 'foreign_author': native.update_schema(soup, author={'@type':'Person', 'name':'其他作者'})
    elif change == 'missing_schema': soup.select_one('#report').decompose()
    elif change == 'second_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'wrong_clock': native.update_schema(soup, datePublished=native.MOD)
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'before_credit':
        node = soup.new_tag('p'); node.string = '原始正文不能因為晚來的署名而被刪除。'
        credit.insert_before(node)
    elif change == 'hidden_prose': soup.select_one('#prose')['hidden'] = ''
    else: raise AssertionError(change)
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), host, parser)


@pytest.mark.parametrize('host', ['finance', 'www'])
def test_old_native_reporter_contract_does_not_change(host):
    soup = native.page(host)
    assert extract(soup, host) == extract(soup, host, version=36)


def test_only_capture_identity_changes():
    old, new = source(version=36), source()
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
