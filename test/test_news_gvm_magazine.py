"""Public periodicals: same-page issue identity, literal authors and clocks."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_gvm import GVM_MAGAZINE_LAYOUT, GVM_V6_VERIFICATION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_gvm_inline_text import WRAPPERS, inline_entry
from test_news_gvm_known_urls import extract
from test_news_gvm_native import MODIFIED, ORIGIN, PUBLISHED, TEXT, URL, policy, schema
from test_news_gvm_tables import TABLE, table_page


def magazine_entry():
    return {**inline_entry(), 'embedded_article_layout': GVM_MAGAZINE_LAYOUT,
        'adapter_verification': GVM_V6_VERIFICATION}


def magazine_page():
    soup = table_page(TABLE + ''.join(WRAPPERS))
    head, body = soup.select_one('.article-head'), soup.select_one('.article-content')
    head['article_type'] = '期刊'
    del head['data-is-accessible']
    issue_url = ORIGIN + '/magazine/published/803'
    link = soup.new_tag('a', attrs={'class': 'article-head_magazine', 'href': issue_url,
        'title': '合成期刊標題'})
    link.string = '2015年9月號 第351期 合成期刊標題'
    head.append(link)
    main = soup.new_tag('main', attrs={'class': 'main-wrapper'})
    section, container, block = (soup.new_tag(name) for name in ('section', 'div', 'div'))
    container['class'] = ['container', 'container-article']
    block['class'] = ['article-block']
    block.extend([head.extract(), body.extract()])
    container.extend([block, soup.new_tag('div', id='article_end_12345')])
    section.append(container)
    main.append(section)
    soup.select_one('.article-left').replace_with(main)
    schema(soup, lambda a: a.update(isPartOf={
        '@type': 'PublicationIssue', '@id': issue_url + '#issue', 'url': issue_url,
        'name': '合成期刊標題', 'alternateName': '2015年9月號', 'issueNumber': '351',
        'isPartOf': {'@type': 'Periodical', '@id': ORIGIN + '/magazine#periodical',
            'name': '遠見雜誌', 'url': ORIGIN + '/magazine',
            'publisher': {'@id': ORIGIN + '/#publisher'}}}))
    return BeautifulSoup(str(soup), 'lxml')


def test_periodical_retains_whole_written_body_issue_attribution_and_distinct_clocks():
    soup = magazine_page()
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, policy(inline_entry()))
    result = extract(soup, policy(magazine_entry()))
    assert result['body'] == extract(table_page(TABLE + ''.join(WRAPPERS)), policy(inline_entry()))['body']
    assert TEXT in result['body'] and 'NOT_ARTICLE_' not in result['body']
    assert result['published_at_raw'] == PUBLISHED and result['modified_at_raw'] == MODIFIED
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['source_schema_modified_raw'] == PUBLISHED
    assert not notices['modified_declarations_agree']
    assert notices['native_online_access_attribute_absent']
    assert not notices['metadata_midnight_is_verified_first_release_time']
    assert not notices['native_start_time_is_publication']
    issue = notices['source_declared_periodical_issue']
    assert issue['source_issue_number'] == '351' and not issue['issue_requested']
    detail = json.loads(result['author_attribution_detail_json'])
    assert detail['source_credit_role'] == 'publisher_declared_periodical_author'
    assert not detail['human_reporter_identity_verified'] and not detail['original_ownership_verified']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


def test_online_v5_results_remain_identical_except_named_parser_identity():
    soup = table_page(TABLE + ''.join(WRAPPERS))
    old, new = extract(soup, policy(inline_entry())), extract(soup, policy(magazine_entry()))
    assert {k: v for k, v in old.items() if k != 'article_identity_basis'} == {
        k: v for k, v in new.items() if k != 'article_identity_basis'}
    assert new['article_identity_basis'].endswith('_v6')


@pytest.mark.parametrize('mutate,error', [
    (lambda s: s.select_one('.article-head').__setitem__('data-is-accessible', '0'), 'paywall_or_login'),
    (lambda s: s.select_one('.article-head').__setitem__('status', '會員限定'), 'paywall_or_login'),
    (lambda s: schema(s, lambda a: a.update(isAccessibleForFree=False)), 'paywall_or_login'),
    (lambda s: schema(s, lambda a: a.pop('isAccessibleForFree')), 'paywall_or_login'),
    (lambda s: s.select_one('#article_end_12345').decompose(), 'article_identity_not_verified'),
    (lambda s: s.select_one('#article_end_12345').append('other prose'), 'article_identity_not_verified'),
    (lambda s: s.select_one('.article-head').__setitem__('aid', '999'), 'article_identity_not_verified'),
    (lambda s: s.select_one('.article-head').__setitem__('article_type', '網路文章'), 'article_identity_not_verified'),
    (lambda s: s.select_one('.article-block').append('orphan prose'), 'article_identity_not_verified'),
    (lambda s: s.select_one('.article-block').append(s.new_tag('aside')), 'article_identity_not_verified'),
    (lambda s: s.select_one('.article-content').append(s.new_tag('div')), 'article_identity_not_verified'),
    (lambda s: s.select_one('link[rel=canonical]').__setitem__('href', URL + '9'), 'article_identity_not_verified'),
    (lambda s: s.select_one('.article-head_magazine').__setitem__('hidden', ''), 'article_identity_not_verified'),
    (lambda s: s.select_one('.article-head_magazine').__setitem__('href', ORIGIN + '/magazine/published/999'), 'article_identity_not_verified'),
    (lambda s: s.select_one('.article-head_magazine').__setitem__('title', 'another issue'), 'article_identity_not_verified'),
    (lambda s: schema(s, lambda a: a['isPartOf'].update(**{'@type': 'WebPage'})), 'article_identity_not_verified'),
    (lambda s: schema(s, lambda a: a['isPartOf'].update(issueNumber='0')), 'article_identity_not_verified'),
    (lambda s: schema(s, lambda a: a['isPartOf']['isPartOf'].update(name='another publisher')), 'article_identity_not_verified'),
    (lambda s: schema(s, lambda a: a['author'][0].update(name='different')), 'article_rights_or_scope_excluded'),
    (lambda s: s.select_one('.article-content > p').append('本文轉載自別家媒體'), 'article_rights_or_scope_excluded'),
    (lambda s: s.select_one('meta[property="article:published_time"]').__setitem__('content', 'unknown'), 'invalid_bulk_publication_clock'),
])
def test_periodical_still_requires_access_identity_whole_prose_credit_and_clocks(mutate, error):
    soup = magazine_page()
    mutate(soup)
    with pytest.raises(NewsError, match=error):
        extract(soup, policy(magazine_entry()))


def test_duplicate_periodical_head_is_not_an_arbitrary_first_match():
    soup = magazine_page()
    soup.select_one('.article-block').append(deepcopy(soup.select_one('.article-head')))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, policy(magazine_entry()))


def test_only_exact_v5_predecessor_and_http_scope_inherit_capacity():
    old, new = policy(inline_entry()), policy(magazine_entry())
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    for changed in ({'interval_seconds': 5}, {'parallelism': 2},
                    {'authorization': {**magazine_entry()['authorization'], 'scope': 'different'}}):
        candidate = policy({**magazine_entry(), **changed})
        assert candidate.http_capacity_source_fingerprint != old.http_capacity_source_fingerprint
        assert candidate.parser_repair_prior_capture_fingerprint != old.data_fingerprint


def test_parser_upgrade_preserves_completed_and_failed_queue_heads(config, tmp_path):
    settings = deepcopy(config)
    settings['providers'] = [inline_entry()]
    with closing(NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))) as queue:
        queue.add_articles([{'url': URL}, {'url': URL + '9'}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',attempts=3,due=0")
            queue.db.execute("UPDATE articles SET state='complete',body_sha256='retained-body',parquet_path='retained.parquet' WHERE url=?", (URL + '9',))
        before = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    settings['providers'] = [magazine_entry()]
    with closing(NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))) as queue:
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before


def test_registered_defaults_with_periodical_candidate(monkeypatch):
    import test_news_fulltext as checks
    from downloader.news_fulltext import load_config
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(settings)
    candidate.pop('provider_catalog', None)
    entry = next(p for p in candidate['providers'] if p['id'] == 'gvm')
    entry.update(embedded_article_layout=GVM_MAGAZINE_LAYOUT, adapter_verification=GVM_V6_VERIFICATION)
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(checks, 'load_config', lambda _: (candidate, registry, repo))
    checks.test_default_configuration_separates_personal_capture_from_licenses()
