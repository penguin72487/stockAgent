"""Native caption/byline-only paragraph; separate real prose, no invented line."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_ettoday import ETTODAY_V11_PROVED_CAPTION_CREDIT_URLS
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_caption_credit_only_parser_repair, utc_now, verify_bodies
from scripts.audit_news_selected_articles import audit
from scripts.verify_news_recent_batches import verify_recent_batches
from test_news_fulltext import FakeResponse, http_fixture
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_break_credit_v8 as breaks
import test_news_ettoday_static_imgur_v9 as imgur
import test_news_ettoday_static_threads_v10 as threads


def page(parser='lxml', host='finance'):
    soup = native.page(host, parser)
    caption = soup.select_one('.story').find_all('p', recursive=False)[1]
    credit = soup.select_one('#credit')
    node = BeautifulSoup('<p id="caption-credit"><strong>' + breaks.CAPTION + '</strong><br/>\n<br/>'
        + credit.get_text(' ', strip=True) + '</p>', parser).find('p')
    caption.replace_with(node)
    credit.decompose()
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', host='finance', version=11):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_real_separate_prose_source_values_caption_byline_clocks_and_dom_survive(parser):
    soup = page(parser)
    before = str(soup)
    value = extract(soup, parser)
    assert str(soup) == before and native.PROSE in value['body'] and native.NOTICE in value['body']
    assert '原始小標題' in value['body'] and '-12.5%、123456股、0.75' in value['body']
    assert breaks.CAPTION not in value['body'] and breaks.CREDIT not in value['body']
    assert json.loads(value['verified_image_captions_json']) == [breaks.CAPTION]
    assert json.loads(value['authors_json']) == ['研究記者']
    assert json.loads(value['author_attribution_detail_json'])[0]['native_credit'] == breaks.CREDIT
    assert (value['published_at_raw'], value['modified_at_raw']) == (native.PUB, native.MOD)
    proof = [n for n in json.loads(value['verified_article_notices_json'])
        if n['kind'] == 'native_caption_and_reporter_only_paragraph']
    assert len(proof) == 1 and proof[0]['contract_version'] == 11
    assert proof[0]['nonempty_lines'] == 2 and not proof[0]['source_values_rewritten']
    assert not value['historical_point_in_time'] and not value['per_article_license_verified']


@pytest.mark.parametrize('version', range(1, 11))
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_v1_through_v10_do_not_gain_new_caption_credit_acceptance(version, parser):
    error = 'article_attribution_not_verified' if version >= 8 else 'article_rights_or_scope_excluded'
    with pytest.raises(NewsError, match='^' + error + '$'):
        extract(page(parser), parser, version=version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('case', ['www', 'no_image', 'no_caption', 'embedded_byline', 'tagged_credit',
    'duplicate_names', 'hidden_credit', 'nested_br', 'quote', 'no_prose', 'empty_prose',
    'next_caption', 'hidden_next_prose', 'next_wire_credit', 'later_wire_credit', 'duplicate_reporter',
    'identity', 'clock', 'schema', 'hidden_prose'])
def test_new_shape_never_uses_missing_hidden_quoted_conflicting_or_wrong_article_prose(parser, case):
    host = 'www' if case == 'www' else 'finance'
    soup = page(parser, host)
    node = soup.select_one('#caption-credit')
    if case == 'no_image':
        soup.select_one('.story img').decompose()
    elif case == 'no_caption':
        node.strong.string = '這不是圖說。'
    elif case in {'embedded_byline', 'tagged_credit', 'duplicate_names', 'nested_br'}:
        credit = breaks.CREDIT
        replacement = {'embedded_byline': credit + '與同段正文', 'tagged_credit': '<b>' + credit + '</b>',
            'duplicate_names': '記者研究記者、研究記者／台北報導',
            'nested_br': '<span>' + credit + '<br/></span>'}[case]
        node.replace_with(BeautifulSoup(str(node).replace(credit, replacement), parser).find('p'))
    elif case == 'hidden_credit':
        node['style'] = 'display:none'
    elif case == 'quote':
        node.wrap(soup.new_tag('blockquote'))
    elif case == 'no_prose':
        soup.select_one('#prose').decompose()
    elif case == 'empty_prose':
        soup.select_one('#prose').clear()
    elif case == 'next_caption':
        soup.select_one('#prose').string = '▲另張圖片說明，不能充當獨立正文。'
    elif case in {'hidden_next_prose', 'hidden_prose'}:
        soup.select_one('#prose')['hidden'] = ''
    elif case in {'next_wire_credit', 'duplicate_reporter'}:
        soup.select_one('#prose').string = '來源：中央社' if case == 'next_wire_credit' else breaks.CREDIT
    elif case == 'later_wire_credit':
        soup.select_one('.story').append(BeautifulSoup('<p>來源：中央社</p>', parser).p)
    elif case == 'identity':
        soup.select_one('meta[property="og:url"]')['content'] += '0'
    elif case == 'clock':
        soup.select_one('meta[name="pubdate"]')['content'] = native.MOD
    elif case == 'schema':
        native.update_schema(soup, author={'@type': 'Person', 'name': '別位記者'})
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser, host)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_old_accepted_native_caption_prose_and_static_media_values_do_not_change(parser, monkeypatch):
    monkeypatch.setattr(threads.adapter, '_THREADS_ICON_SHA256', hashlib.sha256(threads.ICON.encode()).hexdigest())
    for soup in (native.page('finance', parser), native.page('www', parser), breaks.page(parser),
                 imgur.page(parser), threads.page(parser)):
        host = 'www' if 'www.ettoday.net/news/' in soup.select_one('link[rel=canonical]')['href'] else 'finance'
        assert extract(soup, parser, host, 10) == extract(soup, parser, host, 11)


def test_canonical_capture_verification_and_source_audit_read_the_same_current_batch(tmp_path):
    config = clocks.config(11)
    config['exclusive_http_owner'] = False
    config['providers'][0]['interval_seconds'] = 0.001
    url = native.URLS[0]
    raw = str(page()).encode()
    http, session = http_fixture(config, tmp_path, {
        'https://finance.ettoday.net/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'), url: FakeResponse(raw)})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': url}])
            row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
            record = fetch_article(http, row)
            assert record['raw_response_body'] == raw and not record['publish'] and not record['remote_training_export']
            queue.accept([record])
            assert verify_bodies(tmp_path)['accepted'] and verify_recent_batches(tmp_path, 1)['accepted']
            assert audit(tmp_path, [url], registry=http.registry)['accepted']
            assert [call[0] for call in session.calls] == ['https://finance.ettoday.net/robots.txt', url]
    finally:
        http.close()


def test_only_capture_abi_changes_with_exact_v10_prior_unchanged_authorization_and_http():
    old, previous, new = (clocks.registry(v).sources['ettoday'] for v in (1, 10, 11))
    assert new.data_fingerprint != previous.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == previous.data_fingerprint
    assert new.http_capacity_source_fingerprint == previous.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == previous.authorization_fingerprint == old.authorization_fingerprint
    assert ettoday_caption_credit_only_parser_repair(new, previous.data_fingerprint)
    assert not ettoday_caption_credit_only_parser_repair(new, clocks.registry(9).sources['ettoday'].data_fingerprint)


@pytest.mark.parametrize('boundary', ['eligible', 'body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'state', 'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior', 'skipped_v10', 'different_scope'])
def test_requeue_exactly_one_missing_due_head_once_preserving_all_other_columns_and_budgets(tmp_path, boundary):
    old = clocks.registry(9 if boundary == 'skipped_v10' else 10, tmp_path)
    if boundary == 'different_scope':
        config = clocks.config(10)
        config['providers'][0]['authorization']['scope'] += '; other grant'
        old = type(old)(config, base=tmp_path)
    url = ETTODAY_V11_PROVED_CAPTION_CREDIT_URLS[0]
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': item} for item in (url, *threads.adapter.ETTODAY_V10_PROVED_THREADS_URLS,
            'https://finance.ettoday.net/news/3254171')])
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
            field, value = changes[boundary]
            queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ettoday:20261018','2')")
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        queue.db.commit()
    with closing(NewsQueue(tmp_path, clocks.registry(11, tmp_path))) as queue:
        expected = {key: {**row, 'state': 'retry'} if key == url and boundary == 'eligible' else row for key, row in before.items()}
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0] == '2'
        marker = 'ettoday_caption_credit_only_recheck_v11:' + queue.registry.sources['ettoday'].data_fingerprint
        proof = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if boundary in {'unknown_prior', 'skipped_v10', 'different_scope'}:
            assert proof is None
            return
        receipt = json.loads(proof[0])
        assert receipt['rows'] == ([{key: before[url][key] for key in (
            'url', 'state', 'error_code', 'attempts', 'due', 'last_checked_at_utc')}] if boundary == 'eligible' else [])
        if boundary == 'eligible':
            queue.db.execute('UPDATE articles SET state=? WHERE url=?', (before[url]['state'], url))
            queue.db.commit()
    with closing(NewsQueue(tmp_path, clocks.registry(11, tmp_path))) as queue:
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == proof[0]
