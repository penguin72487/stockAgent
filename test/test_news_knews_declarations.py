"""Source-declared credit/clock roles and complete native recommendation trees."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from downloader import news_fulltext_knews as native
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_knews_editor import editor_entry, editor_page
from test_news_knews_known_urls import URL, authored
from test_news_knews_native import MODIFIED, PUBLISHED, TEXT, policy


def declarations_entry():
    value = editor_entry()
    value.update(embedded_article_layout=native.KNEWS_DECLARATIONS_LAYOUT,
        adapter_verification=native.KNEWS_V4_VERIFICATION)
    return value


def extract(soup):
    return extract_article(str(soup).encode(), policy(declarations_entry()), page_url=URL)


def schema(soup, **updates):
    node = soup.select_one('script[type="application/ld+json"]')
    value = json.loads(node.string)
    article = value['@graph'][0]
    article.update(updates)
    node.string = json.dumps(value, ensure_ascii=False)


def desk_page():
    soup = authored('合成編輯')
    soup.select_one('p.post-intro').string = '【財經中心╱台北報導】合成導言，數量1000股與比率12.34%保持原值。'
    return soup


def test_printed_editorial_desk_is_retained_without_inventing_a_human_reporter():
    result = extract(desk_page())
    roles = json.loads(result['author_attribution_detail_json'])
    assert roles['printed_editorial_desk_name'] == '財經中心'
    assert roles['printed_reporter_name'] is None and roles['printed_credit_role'] == 'editorial_desk'
    assert roles['printed_reporting_location'] == '台北'
    assert not roles['human_reporter_identity_verified'] and not roles['original_ownership_verified']
    assert roles['source_header_credit'] == '合成編輯'
    assert '財經中心╱台北報導' in result['body'] and TEXT in result['body']


def test_source_modified_before_publication_does_not_replace_the_verified_publication_clock():
    soup = authored()
    modified = '2026-06-17T01:12:02+08:00'
    schema(soup, dateModified=modified)
    result = extract(soup)
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == modified
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['schema_modified_precedes_publication']
    assert not notices['source_modified_clock_rewritten']
    assert not notices['modified_clock_order_used_to_infer_publication']
    assert not result['historical_point_in_time']
    soup.select_one('.abc-timestamp > time').string = '2026/06/18 16:30'
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract(soup)


def profile_page(slug='r2.chen'):
    soup = authored()
    link = 'https://www.knews.com.tw/reporter/' + slug
    soup.select_one('.abc-byline > a')['href'] = link
    name = soup.select_one('.abc-byline > a').get_text(strip=True)
    schema(soup, author=[{'@type': 'Person', 'name': name, 'url': link}])
    return soup


def test_profile_slug_is_syntax_not_an_alias_or_a_fixed_name_whitelist():
    result = extract(profile_page())
    roles = json.loads(result['author_attribution_detail_json'])
    assert roles['literal_profile_url'].endswith('/r2.chen')
    assert not roles['different_names_are_verified_aliases']


@pytest.mark.parametrize('slug', ['r2..chen', '../news', 'r2.chen?secret=1', '%72%32.chen', 'r2.chen#fragment', 'x' * 100])
def test_malformed_or_ambiguous_profiles_remain_rejected(slug):
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(profile_page(slug))


def headings_page():
    soup = authored()
    soup.select_one('.article-content-inner > h3')['id'] = 'subhead-1'
    soup.select_one('.toc-link')['href'] = '#subhead-1'
    return soup


def test_heading_identity_comes_from_the_complete_ordered_toc_not_zero_based_numbering():
    assert TEXT in extract(headings_page())['body']
    soup = headings_page()
    soup.select_one('.toc-link')['href'] = '#subhead-0'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


def nested_page():
    soup = authored()
    card = soup.select_one('.related-news-card')
    del card['id']
    card['style'] = 'border: 1px solid #e0e0e0;'
    card.select_one('a')['href'] += '?utm_source=editor&utm_medium=article'
    for _ in range(2):
        child = deepcopy(card)
        card.append(child)
        card = child
    return soup


def test_nested_recommendations_exclude_only_complete_cards_and_keep_full_original_prose():
    result = extract(nested_page())
    assert TEXT in result['body'] and 'NOT_ARTICLE_RELATED' not in result['body']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED


@pytest.mark.parametrize('kind', ['swallowed_prose', 'free_text', 'extra_link', 'unknown_class',
    'external_link', 'missing_heading', 'extra_label', 'event', 'script'])
def test_unaccounted_card_descendants_cannot_silently_remove_article_text(kind):
    soup = nested_page()
    card = soup.select('.related-news-card')[-1]
    if kind == 'swallowed_prose':
        node = soup.new_tag('p'); node.string = '合成原文不可被推薦卡吞掉。'; card.append(node)
    elif kind == 'free_text':
        card.append('合成原文不可遺失')
    elif kind == 'extra_link':
        card.append(deepcopy(card.select_one('a')))
    elif kind == 'unknown_class':
        card['class'] = ['unreviewed']
    elif kind == 'external_link':
        card.select_one('a')['href'] = 'https://example.org/news/' + 'A' * 32
    elif kind == 'missing_heading':
        card.select_one('h5').decompose()
    elif kind == 'extra_label':
        card.select_one('a').append(deepcopy(card.select_one('span')))
    elif kind == 'event':
        card['onclick'] = 'unreviewed()'
    else:
        card.append(soup.new_tag('script'))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('factory', [desk_page, profile_page, headings_page, nested_page])
def test_previous_v3_contract_is_not_silently_rewritten(factory):
    with pytest.raises(NewsError):
        extract_article(str(factory()).encode(), policy(editor_entry()), page_url=URL)


def test_original_v3_values_credits_and_clocks_have_exact_parity():
    soup = editor_page()
    before = extract_article(str(soup).encode(), policy(editor_entry()), page_url=URL)
    after = extract(soup)
    for key in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert after[key] == before[key]
    for key in ('author_attribution_detail_json', 'verified_article_notices_json'):
        old, new = json.loads(before[key]), json.loads(after[key])
        old['contract_version'] = 4
        assert old == new


def test_exact_v3_predecessor_keeps_http_owner_but_changed_pacing_and_rights_do_not(monkeypatch):
    previous = policy(editor_entry())
    monkeypatch.setattr(native, 'KNEWS_EDITOR_HTTP_BINDING',
        (previous.data_fingerprint, previous.authorization_fingerprint, '1' * 64, '2' * 64))
    current = policy(declarations_entry())
    assert current.parser_repair_prior_capture_fingerprint == previous.data_fingerprint
    assert current.http_capacity_source_fingerprint == '1' * 64
    for field in ('interval_seconds', 'authorization'):
        value = declarations_entry()
        if field == 'authorization':
            value[field]['scope'] += ' changed purpose'
        else:
            value[field] = 11
        assert not policy(value).http_capacity_source_fingerprint


def test_full_installed_candidate_keeps_other_sources_and_the_shared_policy_contract(monkeypatch):
    import test_news_fulltext as shared

    config, registry, repo = shared.load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config); candidate.pop('provider_catalog', None)
    next(p for p in candidate['providers'] if p['id'] == 'knews').update(
        embedded_article_layout=native.KNEWS_DECLARATIONS_LAYOUT, adapter_verification=native.KNEWS_V4_VERIFICATION)
    approved = PolicyRegistry(candidate, base=repo)
    for provider, old in registry.sources.items():
        new = approved.sources[provider]
        assert (new.authorization_fingerprint, new.http_capacity_source_fingerprint,
                new.http_capacity_authorization_fingerprint) == (old.authorization_fingerprint,
                old.http_capacity_source_fingerprint, old.http_capacity_authorization_fingerprint)
        if provider != 'knews':
            assert new.data_fingerprint == old.data_fingerprint
    monkeypatch.setattr(shared, 'load_config', lambda _: (candidate, approved, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


def test_parser_upgrade_preserves_all_previous_heads_attempts_and_due_clocks(tmp_path):
    config = {'contract_version': 1, 'publish': False, 'remote_training_export': False, 'providers': [editor_entry()]}
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=?',
                ('article_attribution_not_verified', 'article_attribution_not_verified', time.time() + 3600))
        before = [dict(row) for row in queue.db.execute('SELECT * FROM articles')]
    with closing(NewsQueue(tmp_path, PolicyRegistry({**config, 'providers': [declarations_entry()]}, base=tmp_path))) as queue:
        queue.reconcile_policy()
        assert [dict(row) for row in queue.db.execute('SELECT * FROM articles')] == before
