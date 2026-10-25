"""Complete native short editors and source-declared title/credit variants."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from downloader import news_fulltext_knews as native
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_knews_declarations import declarations_entry, schema
from test_news_knews_editor import editor_page
from test_news_knews_known_urls import URL, authored
from test_news_knews_native import TEXT, policy


def credits_entry():
    value = declarations_entry()
    value.update(embedded_article_layout=native.KNEWS_CREDITS_LAYOUT, adapter_verification=native.KNEWS_V5_VERIFICATION)
    return value


def extract(soup):
    return extract_article(str(soup).encode(), policy(credits_entry()), page_url=URL)


@pytest.mark.parametrize('edge', [' ', '\u3000', '\n\t', '\u00a0'])
def test_title_comparison_trims_edges_only_and_preserves_source_declarations(edge):
    soup = authored()
    title = soup.select_one('h1').get_text(' ', strip=True)
    declared = edge + title + edge
    schema(soup, headline=declared)
    soup.select_one('meta[property="og:title"]')['content'] = declared + '｜知新聞'
    result = extract(soup)
    notices = json.loads(result['verified_article_notices_json'])
    assert result['title'] == title and TEXT in result['body']
    assert notices['source_schema_headline_raw'] == declared
    assert notices['source_og_title_raw'] == declared + '｜知新聞'
    assert notices['title_edge_whitespace_used_for_comparison'] and not notices['internal_title_characters_rewritten']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(soup).encode(), policy(declarations_entry()), page_url=URL)


@pytest.mark.parametrize('case', ['internal_space', 'changed_character', 'wrong_brand', 'nonstring', 'empty'])
def test_title_edges_do_not_allow_conflicting_content_or_publisher(case):
    soup = authored()
    if case == 'wrong_brand':
        soup.select_one('meta[property="og:title"]')['content'] = '合成測試標題｜別站'
    else:
        schema(soup, headline={'internal_space': '合成 測試標題', 'changed_character': '不同測試標題',
                               'nonstring': ['合成測試標題'], 'empty': '　'}[case])
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('separator', ['／', '╱'])
def test_printed_reporter_separator_is_retained_without_alias_inference(separator):
    soup = authored()
    intro = '【記者合成作者' + separator + '台北報導】合成導言保留1000股與-5原值。'
    soup.select_one('p.post-intro').string = intro
    result = extract(soup)
    roles = json.loads(result['author_attribution_detail_json'])
    assert roles['printed_byline_raw'] == intro[:intro.index('】') + 1]
    assert roles['printed_reporter_name'] == '合成作者' and intro in result['body']
    assert roles['source_header_credit'] != roles['printed_reporter_name']
    assert not roles['different_names_are_verified_aliases']


def test_translator_is_a_separate_printed_role_not_a_reporter_or_ownership_claim():
    soup = authored('合成編輯')
    soup.select_one('p.post-intro').string = '【編譯合成譯者／綜合外電】合成導言，來源數值12.34%不改。'
    result = extract(soup)
    roles = json.loads(result['author_attribution_detail_json'])
    assert roles['printed_translator_name'] == '合成譯者' and roles['printed_reporter_name'] is None
    assert roles['printed_credit_role'] == 'translator' and roles['printed_reporting_location'] == '綜合外電'
    assert 'source_printed_translator_signature' in roles['source_credit_roles']
    assert not roles['original_ownership_verified'] and not roles['human_reporter_identity_verified']
    assert not roles['different_names_are_verified_aliases']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(str(soup).encode(), policy(declarations_entry()), page_url=URL)


@pytest.mark.parametrize('name', ['中央社', 'Reuters', 'AFP'])
def test_translator_role_does_not_override_external_credit_exclusions(name):
    soup = authored()
    soup.select_one('p.post-intro').string = '【編譯' + name + '／綜合外電】合成導言。'
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup)


def short_page(text):
    soup = authored()
    soup.select_one('#article-left-toc').decompose()
    body = soup.select_one('.article-content-inner')
    body.clear()
    paragraph = soup.new_tag('p')
    paragraph.string = text
    body.append(paragraph)
    return soup


def test_complete_short_editor_beyond_intro_is_not_replaced_with_a_summary():
    text = '合成短訊正文，原始數值-5與1000股完整保留。' * 3
    assert len(text) < 100
    soup = short_page(text)
    result = extract(soup)
    assert text in result['body'] and result['body'].count(text) == 1
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['short_native_editor_characters'] == len(text)
    assert notices['short_native_editor_beyond_introduction_verified']
    assert not notices['schema_description_used_as_full_body']
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(str(soup).encode(), policy(declarations_entry()), page_url=URL)


@pytest.mark.parametrize('text', ['', '　 \n'])
def test_intro_and_caption_without_real_editor_prose_remain_missing(text):
    with pytest.raises(NewsError, match='body_not_found'):
        extract(short_page(text))


def test_prior_v4_body_values_roles_and_clock_notices_have_exact_parity():
    soup = editor_page()
    before = extract_article(str(soup).encode(), policy(declarations_entry()), page_url=URL)
    after = extract(soup)
    for field in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert before[field] == after[field]
    for field in ('author_attribution_detail_json', 'verified_article_notices_json'):
        prior, current = json.loads(before[field]), json.loads(after[field])
        prior['contract_version'] = 5
        assert prior == current


def test_exact_v4_http_binding_does_not_mint_rate_or_permission_budget(monkeypatch):
    previous = policy(declarations_entry())
    monkeypatch.setattr(native, 'KNEWS_DECLARATIONS_HTTP_BINDING',
        (previous.data_fingerprint, previous.authorization_fingerprint, '1' * 64, '2' * 64))
    current = policy(credits_entry())
    assert current.parser_repair_prior_capture_fingerprint == previous.data_fingerprint
    assert current.http_capacity_source_fingerprint == '1' * 64
    for field in ('interval_seconds', 'authorization'):
        value = credits_entry()
        if field == 'authorization':
            value[field]['scope'] += ' changed purpose'
        else:
            value[field] = 11
        assert not policy(value).http_capacity_source_fingerprint


def test_upgrade_keeps_all_old_heads_attempts_deadlines_and_errors(tmp_path):
    config = {'contract_version': 1, 'publish': False, 'remote_training_export': False,
              'providers': [declarations_entry()]}
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=?',
                ('article_attribution_not_verified', 'article_attribution_not_verified', time.time() + 3600))
        before = [dict(r) for r in queue.db.execute('SELECT * FROM articles')]
    with closing(NewsQueue(tmp_path, PolicyRegistry({**config, 'providers': [credits_entry()]}, base=tmp_path))) as queue:
        queue.reconcile_policy()
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles')] == before


def test_full_installed_candidate_preserves_every_other_provider(monkeypatch):
    import test_news_fulltext as shared

    config, registry, repo = shared.load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    candidate.pop('provider_catalog', None)
    next(p for p in candidate['providers'] if p['id'] == 'knews').update(
        embedded_article_layout=native.KNEWS_CREDITS_LAYOUT, adapter_verification=native.KNEWS_V5_VERIFICATION)
    approved = PolicyRegistry(candidate, base=repo)
    for provider, old in registry.sources.items():
        current = approved.sources[provider]
        for field in ('authorization_fingerprint', 'http_capacity_source_fingerprint', 'http_capacity_authorization_fingerprint'):
            assert getattr(current, field) == getattr(old, field)
        if provider != 'knews':
            assert current.data_fingerprint == old.data_fingerprint
    monkeypatch.setattr(shared, 'load_config', lambda _: (candidate, approved, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()
