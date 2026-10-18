"""Literal adjacent DOM text, not arbitrary whitespace/name reconstruction."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

from bs4 import BeautifulSoup, Comment
import pytest

import downloader.news_fulltext_ettoday as adapter
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import ETTODAY_LAYOUT_VERSIONS, NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_split_reporter_parser_repair, utc_now, verify_bodies
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import FakeResponse, http_fixture
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_break_credit_v8 as breaks
import test_news_ettoday_static_imgur_v9 as imgur
import test_news_ettoday_static_threads_v10 as threads
import test_news_ettoday_caption_credit_only_v11 as caption
import test_news_ettoday_two_threads_v12 as pairs


@pytest.fixture(autouse=True)
def synthetic_decorative_icon(monkeypatch):
    monkeypatch.setattr(adapter, '_THREADS_ICON_SHA256', hashlib.sha256(threads.ICON.encode()).hexdigest())


def page(parser='lxml', host='finance'):
    soup = native.page(host, parser)
    credit = soup.select_one('#credit'); credit.attrs.clear(); credit.clear()
    first = soup.new_tag('strong'); first.string = '記'
    credit.append(first); credit.append('者研究記者／台北報導')
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', *, version=13, host='finance'):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_source_adjacent_text_preserves_literal_credit_and_all_values(parser):
    soup = page(parser); original = str(soup)
    paragraph = soup.select_one('.story').find_all('p', recursive=False)[2]
    assert paragraph.get_text(' ', strip=True) == '記 者研究記者／台北報導'
    assert paragraph.get_text('', strip=False) == '記者研究記者／台北報導'
    result = extract(soup, parser)
    old = extract(native.page('finance', parser), parser, version=12)
    assert str(soup) == original
    for key in result:
        if key != 'verified_article_notices_json':
            assert result[key] == old[key], key
    notices = json.loads(result['verified_article_notices_json'])
    added = [n for n in notices if n['kind'] == 'native_strong_split_reporter_word']
    assert added == [{'kind': 'native_strong_split_reporter_word',
        'source_dom_text_credit': '記者研究記者／台北報導',
        'literal_adjacent_text_nodes_without_added_separator': True,
        'source_whitespace_removed_or_names_invented': False}]
    assert [n for n in notices if n not in added] == json.loads(old['verified_article_notices_json'])
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('version', range(1, 13))
def test_old_versions_do_not_silently_change_attribution(parser, version):
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(page(parser), parser, version=version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('variant', ['finance', 'www', 'break', 'imgur', 'threads', 'caption', 'pairs'])
def test_completed_positive_values_and_previous_contracts_stay_identical(parser, variant):
    soup = (native.page(variant, parser) if variant in {'finance', 'www'} else
        {'break': breaks.page, 'imgur': imgur.page, 'threads': threads.page,
            'caption': caption.page, 'pairs': pairs.page}[variant](parser))
    host = 'www' if variant == 'www' else 'finance'
    assert extract(soup, parser, version=12, host=host) == extract(soup, parser, host=host)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('case', ['b', 'span', 'strong_attribute', 'strong_hidden', 'paragraph_attribute',
    'paragraph_hidden', 'comment', 'nested', 'other_first_character', 'source_gap', 'source_newline',
    'source_nbsp', 'leading_space', 'name_gap', 'duplicate_names', 'extra_child', 'late', 'duplicate',
    'www', 'identity', 'clock', 'schema', 'paid', 'wire', 'hidden_prose', 'script'])
def test_unknown_shapes_source_whitespace_and_unrelated_guards_are_not_relaxed(parser, case):
    host = 'www' if case == 'www' else 'finance'
    soup = page(parser, host); story = soup.select_one('.story')
    credit = story.find_all('p', recursive=False)[2]; first = credit.strong
    if case in {'b', 'span'}:
        first.name = case
    elif case == 'strong_attribute': first['id'] = 'unproved'
    elif case == 'strong_hidden': first['hidden'] = ''
    elif case == 'paragraph_attribute': credit['id'] = 'unproved'
    elif case == 'paragraph_hidden': credit['hidden'] = ''
    elif case == 'comment': credit.append(Comment('unproved'))
    elif case == 'nested':
        child = soup.new_tag('span'); child.string = '記'; first.clear(); first.append(child)
    elif case == 'other_first_character': first.string = '紀'
    elif case in {'source_gap', 'source_newline', 'source_nbsp', 'name_gap', 'duplicate_names'}:
        credit.contents[-1].replace_with({'source_gap': ' 者研究記者／台北報導',
            'source_newline': '\n者研究記者／台北報導', 'source_nbsp': '\xa0者研究記者／台北報導',
            'name_gap': '者研究 記者／台北報導', 'duplicate_names': '者研究記者、研究記者／台北報導'}[case])
    elif case == 'leading_space': credit.insert(0, ' ')
    elif case == 'extra_child': credit.append(soup.new_tag('img', src='unproved.jpg'))
    elif case == 'late':
        for _ in range(4):
            paragraph = soup.new_tag('p'); paragraph.append(soup.new_tag('img', src='prelude.jpg')); credit.insert_before(paragraph)
    elif case == 'duplicate': story.append(BeautifulSoup(str(credit), parser).p)
    elif case == 'identity': soup.select_one('link[rel=canonical]')['href'] = native.URLS[0] + '1'
    elif case == 'clock': soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif case == 'schema': native.update_schema(soup, headline='其他文章')
    elif case == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif case == 'wire':
        paragraph = soup.new_tag('p'); paragraph.string = '文／中央社'; credit.insert_before(paragraph)
    elif case == 'hidden_prose': soup.select_one('#prose')['style'] = 'display:none'
    elif case == 'script': story.append(soup.new_tag('script', src='https://example.test/unknown.js'))
    with pytest.raises(NewsError):
        extract(soup, parser, host=host)


def test_canonical_fetch_acceptance_keeps_source_bytes_and_only_requests_robots_and_article(tmp_path):
    config = clocks.config(13); config['exclusive_http_owner'] = False
    config['providers'][0]['interval_seconds'] = 0.001
    url = native.URLS[0]; raw = str(page()).encode()
    http, session = http_fixture(config, tmp_path, {
        'https://finance.ettoday.net/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(raw)})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': url}])
            row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
            record = fetch_article(http, row)
            assert record['raw_response_body'] == raw
            assert not record['publish'] and not record['remote_training_export']
            queue.accept([record])
            assert verify_bodies(tmp_path)['accepted']
            assert audit(tmp_path, [url], registry=http.registry)['accepted']
            assert [call[0] for call in session.calls] == ['https://finance.ettoday.net/robots.txt', url]
    finally:
        http.close()


def test_closed_versions_and_exact_ancestry_preserve_auth_and_http_capacity():
    assert set(ETTODAY_LAYOUT_VERSIONS.values()) == set(range(1, clocks.future_version()))
    assert f'ettoday_visible_article_v{clocks.future_version()}' not in ETTODAY_LAYOUT_VERSIONS
    first, old, new = (clocks.registry(v).sources['ettoday'] for v in (1, 12, 13))
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint == first.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint == first.authorization_fingerprint
    assert ettoday_split_reporter_parser_repair(new, old.data_fingerprint)
    assert not ettoday_split_reporter_parser_repair(new, clocks.registry(11).sources['ettoday'].data_fingerprint)


@pytest.mark.parametrize('boundary', ['eligible', 'body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'state', 'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior', 'skipped_v12', 'different_scope'])
def test_only_due_missing_unleased_observed_head_requeues_once_without_evidence_resets(tmp_path, boundary):
    old = clocks.registry(11 if boundary == 'skipped_v12' else 12, tmp_path)
    if boundary == 'different_scope':
        config = clocks.config(12); config['providers'][0]['authorization']['scope'] += '; other scope'
        old = type(old)(config, base=tmp_path)
    url = adapter.ETTODAY_V13_PROVED_SPLIT_REPORTER_URLS[0]
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': item} for item in (url, *adapter.ETTODAY_V12_PROVED_TWO_THREADS_URLS,
            *adapter.ETTODAY_V11_PROVED_CAPTION_CREDIT_URLS, 'https://finance.ettoday.net/news/3219089')])
        queue.db.execute("UPDATE articles SET state='article_attribution_not_verified',error_code='article_attribution_not_verified',"
            'attempts=4,due=1234,last_checked_at_utc=?', (utc_now(),))
        changes = {'body': ('body_sha256', 'b' * 64), 'parquet': ('parquet_path', 'saved.parquet'),
            'lease_owner': ('lease_owner', 'other'), 'lease_until': ('lease_until', 9000000000),
            'future_due': ('due', 9000000000), 'state': ('state', 'http_403'), 'error': ('error_code', 'http_429'),
            'missing_clock': ('last_checked_at_utc', None), 'naive_clock': ('last_checked_at_utc', '2026-10-17T00:00:00'),
            'future_clock': ('last_checked_at_utc', (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()),
            'invalid_clock': ('last_checked_at_utc', 'invalid')}
        if boundary == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value='unknown' WHERE key='source_capture_fingerprint:ettoday'")
        elif boundary in changes:
            field, value = changes[boundary]; queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ettoday:20261018','2')")
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        queue.db.commit()
    with closing(NewsQueue(tmp_path, clocks.registry(13, tmp_path))) as queue:
        expected = {key: {**row, 'state': 'retry'} if key == url and boundary == 'eligible' else row for key, row in before.items()}
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0] == '2'
        marker = 'ettoday_split_reporter_recheck_v13:' + queue.registry.sources['ettoday'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if boundary in {'unknown_prior', 'skipped_v12', 'different_scope'}:
            assert receipt is None
            return
        proof = json.loads(receipt[0])
        assert proof['rows'] == ([{key: before[url][key] for key in (
            'url', 'state', 'error_code', 'attempts', 'due', 'last_checked_at_utc')}] if boundary == 'eligible' else [])
        if boundary == 'eligible':
            queue.db.execute('UPDATE articles SET state=? WHERE url=?', (before[url]['state'], url)); queue.db.commit()
    with closing(NewsQueue(tmp_path, clocks.registry(13, tmp_path))) as queue:
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt[0]
