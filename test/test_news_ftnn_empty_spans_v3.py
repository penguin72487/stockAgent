"""Native empty spans and exact editor sections; no hidden-payload fallback."""
from copy import deepcopy
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import (
    FTNN_NATIVE_V3_REPAIR_COHORT, NewsQueue, ftnn_native_v3_parser_repair, utc_now, verify_bodies,
)
from test_news_ftnn import URL, PROSE, NOTICE, page
from test_news_ftnn_native_v2 import candidate_config as prior_config


def candidate_config(parser='lxml'):
    cfg = prior_config(parser)
    cfg['providers'][0]['embedded_article_layout'] = 'ftnn_visible_article_v3'
    return cfg


def policy(parser='lxml'):
    return PolicyRegistry(candidate_config(parser), base=Path.cwd()).sources['ftnn']


def extract(soup, parser='lxml'):
    return extract_article(str(soup).encode(), policy(parser), page_url=URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('style', ['display:none', 'display: none;', 'DISPLAY: NONE', ' display : none ; '])
@pytest.mark.parametrize('content', ['', ' ', '\u00a0', '<!-- source editor marker -->'])
def test_only_empty_leaf_span_is_discarded_without_changing_any_article_value(parser, style, content):
    soup = page(parser)
    expected = extract(soup, parser)
    soup.select_one('.news-body > p:nth-of-type(2)').append(
        BeautifulSoup(f'<span style="{style}">{content}</span>', parser).find('span'))
    before = str(soup)
    result = extract(soup, parser)
    assert result == expected
    assert PROSE in result['body'] and NOTICE in result['body']
    assert str(soup) == before


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('markup', [
    '<span style="display:none">不能刪除的原始正文</span>',
    '<span style="display:none"><img src="https://invalid.test/media"></span>',
    '<span style="display:none"><a href="https://invalid.test/source"></a></span>',
    '<span style="display:none"><span>不能刪除的子節點文字</span></span>',
    '<span style="display:none"><span></span></span>',
    '<span style="display:none" data-source="attribution"></span>',
    '<span style="display:none" hidden></span>', '<span style="visibility:hidden"></span>',
    '<span style="display:none;color:red"></span>', '<span aria-hidden="true"></span>',
    '<span style="display:none"><span style="background-image:url(https://invalid.test/media)"></span></span>',
    '<span style="display:none" lang="not a language"></span>',
    '<span style="display:none"><span data-source="original"></span></span>',
    '<div style="display:none"></div>',
])
def test_hidden_prose_payloads_unknown_styles_or_semantic_attributes_still_fail(parser, markup):
    soup = page(parser)
    soup.select_one('.news-body > p:nth-of-type(2)').append(BeautifulSoup(markup, parser).find(['span', 'div']))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, parser)


@pytest.mark.parametrize('selector', ['.news-body', '.news-box', '.news-body > p:nth-of-type(2)'])
def test_empty_span_never_reveals_or_waives_a_hidden_ancestor(selector):
    soup = page()
    soup.select_one('.news-body > p:nth-of-type(2)').append(
        BeautifulSoup('<span style="display:none"></span>', 'lxml').span)
    soup.select_one(selector)['hidden'] = ''
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


def test_old_adapter_is_reproducible_and_both_new_parsers_match():
    soup = page()
    soup.select_one('.news-body > p:nth-of-type(2)').append(
        BeautifulSoup('<span style="display:none"></span>', 'lxml').span)
    old = PolicyRegistry(prior_config(), base=Path.cwd()).sources['ftnn']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(soup).encode(), old, page_url=URL)
    assert extract(soup) == extract(soup, 'html.parser')


def section_page(parser='lxml'):
    soup = page(parser)
    body = soup.select_one('.news-body')
    attributes = {'data-scroll-anchor': 'true', 'data-testid': 'turn-1', 'data-turn': 'assistant',
                  'data-turn-id': 'source-editor', 'data-turn-id-container': '', 'dir': 'auto'}
    empty = soup.new_tag('section', attrs=attributes)
    flow = soup.new_tag('section', attrs=attributes)
    notice = body.select(':scope > p')[-1]
    for child in list(body.children):
        if child is not notice:
            flow.append(child.extract())
    body.insert(0, flow)
    body.insert(0, empty)
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_exact_editor_sections_preserve_whole_body_outer_notice_captions_and_native_credit(parser):
    soup = section_page(parser)
    before = str(soup)
    expected = extract(page(parser), parser)
    assert extract(soup, parser) == expected
    assert str(soup) == before
    old = PolicyRegistry(prior_config(parser), base=Path.cwd()).sources['ftnn']
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract_article(str(soup).encode(), old, page_url=URL)


@pytest.mark.parametrize('change', ['prefix_prose', 'prefix_image', 'prefix_quote', 'prefix_hidden',
    'unknown_attributes', 'unknown_dir', 'nested_section', 'report_hidden', 'extra_section',
    'missing_outer_paragraph', 'literal_prefix', 'later_credit', 'third_party_credit', 'hidden_prose'])
def test_sections_are_not_a_general_descendant_byline_or_hidden_content_fallback(change):
    soup = section_page()
    body = soup.select_one('.news-body')
    empty, flow = body.select(':scope > section')
    if change == 'prefix_prose':
        empty.string = '不能略過的前文'
    elif change == 'prefix_image':
        empty.append(soup.new_tag('img'))
    elif change == 'prefix_quote':
        empty.append(soup.new_tag('p'))
    elif change == 'prefix_hidden':
        empty['hidden'] = ''
    elif change == 'unknown_attributes':
        flow['data-other-source'] = 'unknown'
    elif change == 'unknown_dir':
        flow['dir'] = 'unknown'
    elif change == 'nested_section':
        flow.append(soup.new_tag('section'))
    elif change == 'report_hidden':
        flow['style'] = 'display:none'
    elif change == 'extra_section':
        flow.insert_after(deepcopy(flow))
    elif change == 'missing_outer_paragraph':
        body.select_one(':scope > p').decompose()
    elif change == 'literal_prefix':
        body.insert(0, '不能略過的前文')
    elif change == 'later_credit':
        flow.insert(0, BeautifulSoup('<p>前文不是署名</p>', 'lxml').p)
    elif change == 'third_party_credit':
        flow.select_one(':scope > p').string = '中央社記者黃詩雯／綜合報導'
    else:
        flow.select(':scope > p')[1]['hidden'] = ''
    with pytest.raises(NewsError):
        extract(soup)


def test_capture_changes_only_parser_not_original_authorization_or_capacity():
    old = PolicyRegistry(prior_config(), base=Path.cwd()).sources['ftnn']
    new = policy()
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    changed = deepcopy(candidate_config())
    changed['providers'][0]['interval_seconds'] = 3
    other = PolicyRegistry(changed, base=Path.cwd()).sources['ftnn']
    assert other.parser_repair_prior_capture_fingerprint != old.data_fingerprint


def test_only_exact_previous_native_capture_can_reopen_missing_residuals():
    old = PolicyRegistry(prior_config(), base=Path.cwd()).sources['ftnn']
    new = policy()
    assert ftnn_native_v3_parser_repair(new, old.data_fingerprint)
    for previous in (new.data_fingerprint, 'b' * 64, None, old.http_capacity_source_fingerprint):
        assert not ftnn_native_v3_parser_repair(new, previous)
    assert not ftnn_native_v3_parser_repair(old, old.data_fingerprint)
    assert not ftnn_native_v3_parser_repair(replace(new, allowed=False), old.data_fingerprint)


def seed_failure(queue, url, error):
    queue.add_articles([{'url': url}])
    queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=4102444800,'
        'last_checked_at_utc=? WHERE url=?', (error, error, utc_now(), url))


def test_exact_two_url_error_pairs_reopen_once_without_replaying_accepted_or_other_failures(tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(prior_config(), base=tmp_path))) as queue:
        for url, error in FTNN_NATIVE_V3_REPAIR_COHORT.items():
            seed_failure(queue, url, error)
        seed_failure(queue, 'https://www.ftnn.com.tw/news/520952', 'article_identity_not_verified')
        seed_failure(queue, 'https://www.ftnn.com.tw/news/587768', 'complete')
        seed_failure(queue, 'https://www.ftnn.com.tw/news/512507', 'article_rights_or_scope_excluded')
        seed_failure(queue, 'https://www.ftnn.com.tw/news/42', 'article_attribution_not_verified')
        queue.db.commit()
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    registry = PolicyRegistry(candidate_config(), base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for row in queue.db.execute('SELECT * FROM articles'):
            expected = before[row['url']]
            assert dict(row) == ({**expected, 'state': 'retry'} if row['url'] in FTNN_NATIVE_V3_REPAIR_COHORT else expected)
        marker = 'ftnn_native_layout_recheck_v3:' + registry.sources['ftnn'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert {r['url'] for r in proof['rows']} == set(FTNN_NATIVE_V3_REPAIR_COHORT)
        assert proof['attempts_due_error_and_observation_preserved'] and not proof['accepted_as_bodies']
        assert not proof['all_failures_assumed_repairable']
        for url in FTNN_NATIVE_V3_REPAIR_COHORT:
            queue.db.execute('UPDATE articles SET state=error_code WHERE url=?', (url,))
        queue.db.commit()
        after = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    with closing(NewsQueue(tmp_path, registry)) as queue:
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')] == after
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('change', ['stored_hash', 'stored_path', 'lease_owner', 'lease_until',
    'missing_observation', 'naive_observation', 'future_observation', 'wrong_error',
    'unknown_capture', 'changed_http', 'query_url', 'wrong_url_error_pair'])
def test_unproved_or_in_use_residual_is_never_reopened_or_generic_reset(tmp_path, change):
    native = 'https://www.ftnn.com.tw/news/561059'
    url = native + ('?x=1' if change == 'query_url' else '')
    with closing(NewsQueue(tmp_path, PolicyRegistry(prior_config(), base=tmp_path))) as queue:
        seed_failure(queue, url, 'article_identity_not_verified')
        columns = {'stored_hash': ('body_sha256', 'a' * 64), 'stored_path': ('parquet_path', 'bodies/old.parquet'),
            'lease_owner': ('lease_owner', 'other-owner'), 'lease_until': ('lease_until', 4102444800),
            'missing_observation': ('last_checked_at_utc', None),
            'naive_observation': ('last_checked_at_utc', '2026-10-17T00:00:00'),
            'future_observation': ('last_checked_at_utc', '2099-10-17T00:00:00+00:00'),
            'wrong_error': ('error_code', 'http_403')}
        if change in columns:
            column, value = columns[change]
            queue.db.execute('UPDATE articles SET ' + column + '=? WHERE url=?', (value, url))
        elif change == 'unknown_capture':
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',
                             ('b' * 64, 'source_capture_fingerprint:ftnn'))
        elif change == 'wrong_url_error_pair':
            queue.db.execute("UPDATE articles SET state='article_attribution_not_verified',"
                             "error_code='article_attribution_not_verified' WHERE url=?", (url,))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    cfg = candidate_config()
    if change == 'changed_http':
        cfg['providers'][0]['interval_seconds'] = 3
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


def test_section_failure_metadata_uses_native_reporter_not_footer_and_has_no_prose(tmp_path):
    from downloader.news_fulltext_http import Response
    from test_news_failure_diagnostics import fail
    from test_news_failure_identity_metadata import evidence
    soup = section_page()
    with closing(NewsQueue(tmp_path, PolicyRegistry(candidate_config(), base=tmp_path))) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()),
             code='article_identity_not_verified', received_at_utc='2026-10-17T20:30:00+00:00')
        paths, records = evidence(tmp_path)
        assert len(records) == 1
        record = records[0]
        facts = record['facts']['ftnn_native_validation']
        assert facts['contract_version'] == 4 and facts['byline_count'] == 1
        assert facts['declared_author_in_visible_byline'] and facts['native_reporter_byline_shape']
        assert not record['raw_response_retained'] and not record['accepted_as_article']
        assert PROSE not in paths[0].read_text() and '黃詩雯' not in paths[0].read_text()


@pytest.mark.parametrize('sections', [False, True])
def test_new_native_body_fetch_reaches_shared_durable_acceptance_and_restart(tmp_path, sections):
    from downloader.news_fulltext import fetch_article
    from test_news_fulltext import FakeResponse, http_fixture
    soup = section_page() if sections else page()
    if not sections:
        soup.select_one('.news-body > p:nth-of-type(2)').append(
            BeautifulSoup('<span style="display:none"></span>', 'lxml').span)
    http, _ = http_fixture(candidate_config(), tmp_path, {
        'https://www.ftnn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(str(soup).encode())})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': URL}])
            queue.accept([fetch_article(http, queue.claim_url(URL, 300))])
            head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            assert head['state'] == 'complete' and head['body_sha256'] and head['parquet_path']
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == head
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
            assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
