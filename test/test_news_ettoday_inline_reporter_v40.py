"""Source-witnessed inline prefix, with complete prose and unchanged admission."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_candidates import ettoday_candidate_source
import test_news_ettoday as native
import test_news_ettoday_named_author_v39 as previous


def source(parser='lxml', version=40):
    return previous.source(parser=parser, version=version)


def page(parser='lxml', names=('研究記者',)):
    soup = native.page('www', parser)
    body = soup.select_one('.story'); body.clear()
    first = soup.new_tag('p')
    image = soup.new_tag('img', src='https://cdn2.ettoday.net/images/92/test.jpg', alt='圖片不是作者或正文')
    first.append(image); first.append(soup.new_tag('br')); first.append('\n')
    credit = soup.new_tag('strong'); credit.string = '記者'+'、'.join(names)+'／台北報導'
    first.append(credit); first.append(soup.new_tag('br')); first.append('\n'); first.append(soup.new_tag('br'))
    first.append(native.PROSE); body.append(first)
    paragraph = soup.new_tag('p'); paragraph.string = native.NOTICE; body.append(paragraph)
    native.update_schema(soup, author=[{'@type': 'Person', 'name': name} for name in names])
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=40):
    return extract_article(str(soup).encode(), source(parser, version), page_url=native.URLS[1])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('names', [('研究記者',), ('研究記者', '共同記者')])
def test_inline_prefix_keeps_complete_prose_raw_values_native_names_clocks_and_dom(parser, names):
    soup = page(parser, names); before = str(soup)
    actual = extract(soup, parser)
    assert actual['body'] == native.PROSE+'\n'+native.NOTICE
    assert json.loads(actual['authors_json']) == list(names)
    assert actual['published_at_raw'] == native.PUB and actual['modified_at_raw'] == native.MOD
    assert str(soup) == before
    assert not actual['historical_point_in_time'] and not actual['per_article_license_verified']
    notes = json.loads(actual['verified_article_notices_json'])
    note, = [n for n in notes if n['kind'] == 'native_image_break_strong_reporter_and_prose_same_paragraph']
    assert note['following_native_prose_nodes_preserved'] and note['image_preserved_in_shared_original']
    assert not note['source_values_rewritten'] and not note['image_alt_promoted_to_reporter']
    assert [d['native_credit'] for d in json.loads(actual['author_attribution_detail_json'])] == [note['native_credit']]*len(names)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract(soup, parser, 39)


def test_dual_html_engines_match_every_inline_field():
    raw = str(page()).encode()
    assert extract_article(raw, source(), page_url=native.URLS[1]) == extract_article(
        raw, source('html.parser'), page_url=native.URLS[1])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('mutation', [
    'image_alt_credit', 'hidden_credit', 'hidden_prose', 'missing_separator', 'caption_not_credit',
    'prefixed_prose', 'byline_contains_prose', 'no_prose', 'reprint', 'duplicate_credit',
    'multiple_images', 'nested_credit', 'unknown_script', 'publisher', 'blank_person',
    'missing_schema', 'duplicate_schema', 'wrong_identity', 'wrong_clock', 'paid',
])
def test_inline_syntax_never_supplies_missing_attribution_scope_identity_or_access(parser, mutation):
    soup = page(parser); first = soup.select_one('.story > p'); credit = first.find('strong')
    if mutation == 'image_alt_credit': first.img['alt'] = credit.get_text(); credit.decompose()
    elif mutation == 'hidden_credit': credit['hidden'] = ''
    elif mutation == 'hidden_prose': first['style'] = 'display:none'
    elif mutation == 'missing_separator':
        for br in first.find_all('br'): br.decompose()
    elif mutation == 'caption_not_credit': credit.string = '▲圖／記者研究記者攝'
    elif mutation == 'prefixed_prose': first.insert(0, '原始正文不能當作空白刪掉。')
    elif mutation == 'byline_contains_prose': credit.append(native.NOTICE)
    elif mutation == 'no_prose':
        for node in list(credit.next_siblings): node.extract()
        first.append(soup.new_tag('br'))
        for paragraph in list(first.find_next_siblings()): paragraph.decompose()
    elif mutation == 'reprint': soup.select_one('.story > p:last-child').string = '本文來源：其他媒體'
    elif mutation == 'duplicate_credit':
        extra = soup.new_tag('p'); extra.string = credit.get_text(); first.insert_after(extra)
    elif mutation == 'multiple_images': first.insert(0, deepcopy(first.img))
    elif mutation == 'nested_credit': credit.string.wrap(soup.new_tag('span'))
    elif mutation == 'unknown_script':
        script = soup.new_tag('script'); script.string = '任意腳本'; first.insert(0, script)
    elif mutation == 'publisher': native.update_schema(soup, publisher={'@type': 'Organization', 'name': '其他單位'})
    elif mutation == 'blank_person': native.update_schema(soup, author={'@type': 'Person', 'name': ''})
    elif mutation == 'missing_schema': soup.select_one('#report').decompose()
    elif mutation == 'duplicate_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif mutation == 'wrong_identity': native.update_schema(soup, url=native.URLS[0])
    elif mutation == 'wrong_clock': native.update_schema(soup, datePublished=native.MOD)
    else: native.update_schema(soup, isAccessibleForFree=False)
    with pytest.raises(NewsError): extract(BeautifulSoup(str(soup), parser), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_previous_plain_reporters_and_named_authors_are_unchanged(parser):
    for soup in (native.page('www', parser), previous.page(parser)):
        raw = str(soup).encode()
        assert extract_article(raw, source(parser, 40), page_url=native.URLS[1]) == extract_article(
            raw, source(parser, 39), page_url=native.URLS[1])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_standalone_credit_with_separate_prose_keeps_its_existing_contract(parser):
    soup = page(parser); first = soup.select_one('.story > p')
    for node in list(first.strong.next_siblings): node.extract()
    raw = str(soup).encode()
    assert extract_article(raw, source(parser, 40), page_url=native.URLS[1]) == extract_article(
        raw, source(parser, 39), page_url=native.URLS[1])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_one_schema_person_disagreement_is_preserved_without_resolving_author_or_editor_role(parser):
    soup = page(parser)
    declaration = {'@type': 'Person', 'name': '來源另列的作者', 'url': 'https://www.ettoday.net/'}
    native.update_schema(soup, author=declaration)
    original = str(soup)
    actual = extract(BeautifulSoup(original, parser), parser)
    assert actual['body'] == native.PROSE+'\n'+native.NOTICE
    assert json.loads(actual['authors_json']) == ['研究記者']
    note, = [n for n in json.loads(actual['verified_article_notices_json'])
        if n['kind'] == 'native_reporter_and_schema_person_disagree']
    assert note['visible_reporters'] == ['研究記者']
    assert note['schema_declared_person_authors'] == [declaration]
    assert not note['same_person_or_editor_role_inferred'] and not note['source_values_rewritten']
    detail, = json.loads(actual['author_attribution_detail_json'])
    assert not detail['schema_person_declaration_agrees'] and not detail['employment_independently_verified']
    assert not detail['same_person_or_editor_role_inferred'] and str(soup) == original
    # A plain/named/byline-free page cannot use this observed prefix contract
    # to clear a different source attribution shape or an arbitrary account.
    soup = native.page('www', parser); native.update_schema(soup, author=declaration)
    with pytest.raises(NewsError): extract(BeautifulSoup(str(soup), parser), parser)


def test_candidate_requires_exact_v39_predecessor_preserving_authorization_and_capacity():
    config = native.config(); config['providers'][0]['embedded_article_layout'] = 'ettoday_visible_article_v39'
    registry = PolicyRegistry(config, base=Path.cwd())
    candidate = ettoday_candidate_source(config, registry, Path.cwd(), 'ettoday_visible_article_v40')
    current = registry.sources['ettoday']
    assert candidate.allowed and candidate.authorization_fingerprint == current.authorization_fingerprint
    assert candidate.http_capacity_source_fingerprint == current.http_capacity_source_fingerprint
    assert candidate.parser_repair_prior_capture_fingerprint == current.data_fingerprint
    assert candidate.data_fingerprint != current.data_fingerprint
    for version in (38, 40):
        invalid = deepcopy(config); invalid['providers'][0]['embedded_article_layout'] = f'ettoday_visible_article_v{version}'
        with pytest.raises(ValueError):
            ettoday_candidate_source(invalid, PolicyRegistry(invalid, base=Path.cwd()), Path.cwd(), 'ettoday_visible_article_v40')
