"""A visible named signature needs this article's exact Person witness.

Synthetic fixtures exercise observed structure, not licensed source bodies.
Mirror credits only repair exclusion classification; they never admit prose.
"""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ettoday import (
    _finance_third_party_prelude, ettoday_credit_anatomy, ettoday_validation_metadata,
)
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from scripts.probe_news_sources import ettoday_candidate_source, probe_provider
import test_news_ettoday as native
import test_news_ettoday_bare_signature_v37 as previous
import test_news_ettoday_house_v24 as house
import test_news_ettoday_native_credit_v29 as house_credit
import test_news_ettoday_third_party_prelude_v34 as external


def source(parser='lxml', version=39):
    return previous.source(parser=parser, version=version)


def page(parser='lxml', host='www'):
    soup = native.page(host, parser)
    soup.select_one('#credit').string = '網搜小組／研究記者報導'
    native.update_schema(soup, author={'@type': 'Person', 'name': '研究記者'})
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=39, host='www'):
    return extract_article(str(soup).encode(), source(parser, version), page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_named_author_keeps_original_values_and_does_not_infer_a_reporter_title(parser):
    soup = page(parser)
    before = str(soup)
    result = extract(soup, parser)
    reference = deepcopy(soup)
    reference.select_one('#credit').string = '記者研究記者／台北報導'
    old = extract(reference, parser, 38)
    for key in ('body', 'body_sha256', 'published_at_raw', 'modified_at_raw', 'authors_json',
                'verified_image_captions_json'):
        assert result[key] == old[key]
    assert str(soup) == before
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    detail, = json.loads(result['author_attribution_detail_json'])
    assert detail['name'] == '研究記者' and detail['role'] == 'named_report_author'
    assert detail['native_role'] == '網搜小組' and not detail['employment_or_reporter_title_inferred']
    assert detail['native_credit'] == '網搜小組／研究記者報導'
    notes = json.loads(result['verified_article_notices_json'])
    note, = [n for n in notes if n['kind'] == 'native_search_team_named_author_with_person_schema_witness']
    assert note['same_article_exact_person_schema_required']
    assert not any(note[k] for k in ('source_values_rewritten', 'publisher_or_creator_promoted_to_person',
                                    'employment_or_reporter_title_inferred'))
    assert any(n['kind'] == 'native_named_author_credit' for n in notes)
    assert not any(n['kind'] == 'native_reporter_credit' for n in notes)
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract(soup, parser, 38)


def test_dual_engines_agree_on_every_named_author_field():
    raw = str(page()).encode()
    assert extract_article(raw, source(), page_url=native.URLS[1]) == extract_article(
        raw, source('html.parser'), page_url=native.URLS[1])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['no_person', 'organization', 'wrong_name', 'creator_only',
    'second_author', 'duplicate_author', 'no_schema', 'second_schema', 'wrong_clock', 'paid',
    'wrong_url', 'tagged_credit', 'hidden_credit', 'mixed_prose', 'duplicate_credit', 'late_credit',
    'before_credit', 'missing_name', 'another_role', 'no_role', 'spaced_credit', 'quoted_credit',
    'hidden_prose'])
def test_accounts_or_fuzzy_hidden_late_signatures_do_not_supply_missing_identity(parser, change):
    soup = page(parser)
    credit = soup.select_one('#credit')
    authors = {'no_person': None,
        'organization': {'@type': 'Organization', 'name': 'ETtoday新聞雲', 'url': 'https://www.ettoday.net/'},
        'wrong_name': {'@type': 'Person', 'name': '其他作者'},
        'second_author': [{'@type': 'Person', 'name': '研究記者'}, {'@type': 'Person', 'name': '其他作者'}],
        'duplicate_author': [{'@type': 'Person', 'name': '研究記者'}] * 2}
    if change in authors:
        native.update_schema(soup, author=authors[change])
    elif change == 'creator_only':
        native.update_schema(soup, author=None, creator=['1940-研究記者', '1940'])
    elif change == 'no_schema': soup.select_one('#report').decompose()
    elif change == 'second_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change == 'wrong_clock': native.update_schema(soup, datePublished=native.MOD)
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'wrong_url': native.update_schema(soup, url=native.URLS[0])
    elif change == 'tagged_credit': credit.string.wrap(soup.new_tag('strong'))
    elif change == 'hidden_credit': credit['hidden'] = ''
    elif change == 'mixed_prose': credit.append('原始正文不得因署名而刪去。')
    elif change == 'duplicate_credit': credit.insert_after(deepcopy(credit))
    elif change == 'late_credit':
        for _ in range(4): credit.insert_before(soup.new_tag('p'))
    elif change == 'before_credit':
        node = soup.new_tag('p'); node.string = '原始正文先出現，不能略去。'
        credit.insert_before(node)
    elif change in {'missing_name', 'another_role', 'no_role', 'spaced_credit', 'quoted_credit'}:
        credit.string = {'missing_name': '網搜小組／報導', 'another_role': '其他小組／研究記者報導',
            'no_role': '研究記者報導', 'spaced_credit': '網搜小組 ／ 研究記者報導',
            'quoted_credit': '引用「網搜小組／研究記者報導」不是署名'}[change]
    elif change == 'hidden_prose': soup.select_one('#prose')['hidden'] = ''
    else: raise AssertionError(change)
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_www_only_new_signature_does_not_expand_hosts_or_change_old_native_contracts(parser):
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract(page(parser, 'finance'), parser, host='finance')
    for host in ('www', 'finance'):
        for specimen in (native.page(host, parser), previous.page(host, parser)):
            assert extract(specimen, parser, host=host) == extract(specimen, parser, 38, host)
    for kind in house_credit.KINDS:
        raw = str(house_credit.page(kind, parser)).encode()
        old = replace(PolicyRegistry(house.config(38), base=Path.cwd()).sources['ettoday'], html_parser=parser)
        new = replace(PolicyRegistry(house.config(39), base=Path.cwd()).sources['ettoday'], html_parser=parser)
        assert extract_article(raw, old, page_url=house.URL) == extract_article(raw, new, page_url=house.URL)


def mirror_page(parser='lxml'):
    soup = external.page(parser)
    soup.select_one('#credit').string = '圖文／鏡週刊'
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_exact_plain_mirror_credit_repairs_only_the_exclusion_classification(parser):
    soup = mirror_page(parser)
    before = str(soup)
    with pytest.raises(NewsError, match='^article_rights_or_scope_excluded$'):
        extract(soup, parser, host='finance')
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract(soup, parser, 38, 'finance')
    assert str(soup) == before
    www = native.page('www', parser)
    www.select_one('#credit').string = '圖文／鏡週刊'
    with pytest.raises(NewsError, match='^article_attribution_not_verified$'):
        extract(www, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['hidden_credit', 'linked_credit', 'nested_credit', 'extra_text',
    'wrong_literal', 'later_credit', 'hidden_caption', 'hidden_image'])
def test_mirror_exclusion_does_not_guess_unreviewed_source_shapes(parser, change):
    soup = mirror_page(parser)
    credit = soup.select_one('#credit')
    if change.startswith('hidden_'):
        soup.select_one({'hidden_credit': '#credit', 'hidden_caption': '.story strong',
            'hidden_image': '.story img'}[change])['hidden'] = ''
    elif change == 'linked_credit': credit.string.wrap(soup.new_tag('a', href='https://example.test/'))
    elif change == 'nested_credit': credit.string.wrap(soup.new_tag('strong'))
    elif change == 'extra_text': credit.append('不能略去的正文。')
    elif change == 'wrong_literal': credit.string = '圖文／鏡週刊以外'
    elif change == 'later_credit':
        node = soup.new_tag('p'); node.string = native.PROSE; credit.insert_before(node)
    else: raise AssertionError(change)
    before = str(soup)
    try:
        assert not _finance_third_party_prelude(soup.select_one('.story'),
            reviewed_external_credits=True, reviewed_editor_padding=True,
            reviewed_ctwant_punctuation=True, reviewed_plain_mirror_credit=True)
    except NewsError as error:
        assert change.startswith('hidden_') and error.code != 'article_rights_or_scope_excluded'
    assert str(soup) == before


def test_anatomy_reports_a_named_signature_lead_without_exporting_a_name_or_acceptance():
    soup = page()
    facts = ettoday_validation_metadata(soup, page_url=native.URLS[1], adapter_contract_version=39)
    assert facts['contract_version'] == 4
    anatomy = ettoday_credit_anatomy(soup.select_one('.story'))
    assert anatomy['contract_version'] == 2
    row, = [r for r in anatomy['paragraph_shapes'] if r['native_search_team_named_author_signature']]
    assert row['direct_child'] and row['visible'] and row['reporter_count'] is None
    assert not row['exact_reporter_signature'] and not anatomy['attribution_verified_by_diagnostic']
    encoded = json.dumps(facts, ensure_ascii=False)
    assert all(text not in encoded for text in ('研究記者', native.PROSE, '網搜小組／'))


def candidate_config(version=38):
    config = native.config()
    config['providers'][0]['embedded_article_layout'] = f'ettoday_visible_article_v{version}'
    return config


def test_candidate_is_exact_prior_capture_with_same_auth_capacity_and_one_received_envelope():
    config = candidate_config()
    registry = PolicyRegistry(config, base=Path.cwd())
    old = registry.sources['ettoday']
    candidate = ettoday_candidate_source(config, registry, Path.cwd(), 'ettoday_visible_article_v39')
    assert candidate.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert candidate.authorization_fingerprint == old.authorization_fingerprint
    assert candidate.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert config['providers'][0]['embedded_article_layout'] == 'ettoday_visible_article_v38'
    calls = []
    raw = str(page()).encode()
    def get(url):
        calls.append(url)
        return Response(url, 200, {'content-type': 'text/html'}, raw)
    http = SimpleNamespace(registry=registry, provider_not_before={}, robot_delays={}, get=get)
    result, = probe_provider(http, [native.URLS[1]], candidate_source=candidate)
    assert calls == [native.URLS[1]] and result['extraction_error'] == 'article_attribution_not_verified'
    observed = result['candidate_parser']
    assert observed['parse_success'] and observed['published_at_raw'] == native.PUB
    assert not any(observed[k] for k in ('accepted_as_article', 'raw_response_retained',
        'body_text_exported', 'production_queue_writes', 'additional_publisher_requests'))
    assert native.PROSE not in json.dumps(result, ensure_ascii=False)
    with pytest.raises(ValueError, match='metadata_only_same_source_candidate_required'):
        probe_provider(http, [native.URLS[1]], candidate_source=candidate, retain=True)
    assert calls == [native.URLS[1]]


@pytest.mark.parametrize('version,layout', [(37, 'ettoday_visible_article_v39'),
    (39, 'ettoday_visible_article_v39'), (38, 'ettoday_visible_article_v38'),
    (38, 'ettoday_visible_article_v40'), (38, 'max_visible_post_v9')])
def test_candidate_does_not_grant_arbitrary_upgrades(version, layout):
    config = candidate_config(version)
    with pytest.raises(ValueError, match='reviewed_ettoday_v38_predecessor_candidate_required'):
        ettoday_candidate_source(config, PolicyRegistry(config, base=Path.cwd()), Path.cwd(), layout)


def test_candidate_cannot_inherit_changed_auth_from_different_config():
    config = candidate_config()
    registry = PolicyRegistry(config, base=Path.cwd())
    config['providers'][0]['authorization']['scope'] += ' Changed independent rights.'
    with pytest.raises(ValueError, match='candidate_must_preserve_source_auth_and_http_identity'):
        ettoday_candidate_source(config, registry, Path.cwd(), 'ettoday_visible_article_v39')
