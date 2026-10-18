"""Two native preview pairs, not two fetched posts or permission to strip prose."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

from bs4 import BeautifulSoup, Comment
import pytest

import downloader.news_fulltext_ettoday as adapter
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_two_threads_parser_repair, utc_now, verify_bodies
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import FakeResponse, http_fixture
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_break_credit_v8 as breaks
import test_news_ettoday_static_imgur_v9 as imgur
import test_news_ettoday_static_threads_v10 as threads
import test_news_ettoday_caption_credit_only_v11 as caption

SECOND = 'https://www.threads.com/@synthetic_two/post/Qr67St89Uv0'


@pytest.fixture(autouse=True)
def synthetic_decorative_icon(monkeypatch):
    monkeypatch.setattr(adapter, '_THREADS_ICON_SHA256', hashlib.sha256(threads.ICON.encode()).hexdigest())


def page(parser='lxml', *, host='finance', between=False):
    soup = threads.page(parser, host=host)
    story = soup.select_one('.story'); first = story.blockquote
    widget = deepcopy(first); script = deepcopy(first.find_next_sibling('script'))
    widget['id'] = 'ig-tp-Qr67St89Uv0'
    widget['data-text-post-permalink'] = SECOND; widget.a['href'] = SECOND
    if between:
        paragraph = soup.new_tag('p'); paragraph.string = '兩組介面之間的原始正文 -12.5%、123456股。'
        story.append(paragraph)
    story.append(widget); story.append(script)
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', *, version=12, host='finance'):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('between', [False, True])
def test_all_prose_source_values_and_distinct_ui_declarations_survive(parser, between):
    soup = page(parser, between=between); original = str(soup)
    result = extract(soup, parser)
    baseline = threads.page(parser)
    if between:
        paragraph = baseline.new_tag('p'); paragraph.string = soup.select_one('.story').find_all('p', recursive=False)[-1].get_text()
        baseline.select_one('.story').append(paragraph)
    old = extract(baseline, parser, version=11)
    assert str(soup) == original
    for key in ('body', 'body_sha256', 'title', 'authors_json', 'published_at_raw', 'modified_at_raw',
            'verified_image_captions_json', 'author_attribution_detail_json'):
        assert result[key] == old[key]
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    assert '在 Threads 查看' not in result['body'] and 'synthetic_two' not in result['body']
    notices = [n for n in json.loads(result['verified_article_notices_json'])
        if n['kind'] == 'native_static_threads_preview_ui_declaration']
    assert len(notices) == 2
    assert [n['source_blockquote_attributes'] for n in notices] == [dict(w.attrs) for w in soup.select('.text-post-media')]
    assert all(n['raw_declarations_preserved'] and not any(n[k] for k in (
        'external_request_sent', 'script_executed', 'embedded_post_content_accepted')) for n in notices)
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('version', range(1, 12))
def test_previous_abis_do_not_silently_accept_two_widgets(parser, version):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(parser), parser, version=version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('variant', ['finance', 'www', 'break', 'imgur', 'threads', 'caption'])
def test_all_previously_accepted_values_are_identical(parser, variant):
    if variant in {'finance', 'www'}:
        soup = native.page(variant, parser)
    else:
        soup = {'break': breaks.page, 'imgur': imgur.page, 'threads': threads.page, 'caption': caption.page}[variant](parser)
    host = 'www' if variant == 'www' else 'finance'
    assert extract(soup, parser, version=11, host=host) == extract(soup, parser, host=host)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('case', ['third', 'duplicate_post', 'duplicate_id_other_user', 'nested', 'hidden',
    'extra_attribute', 'wrong_style', 'foreign_host', 'query', 'wrong_id', 'version', 'anchor_href',
    'embedded_prose', 'hidden_label', 'label_comment', 'icon_path', 'iframe', 'script_inline', 'script_src',
    'script_extra', 'script_missing', 'extra_script', 'intervening_comment', 'intervening_text', 'quoted_pair',
    'www', 'identity', 'clock', 'reporter', 'wire', 'schema', 'hidden_prose'])
def test_unknown_pairs_and_source_failures_are_not_blanket_script_exceptions(parser, case):
    host = 'www' if case == 'www' else 'finance'
    soup = page(parser, host=host); story = soup.select_one('.story')
    first, widget = soup.select('.text-post-media'); script = widget.find_next_sibling('script')
    if case == 'third':
        story.append(deepcopy(widget)); story.append(deepcopy(script))
    elif case == 'duplicate_post':
        widget.replace_with(deepcopy(first))
    elif case == 'duplicate_id_other_user':
        widget['id'] = first['id']; widget['data-text-post-permalink'] = threads.PERMALINK.replace('synthetic_user', 'different_user')
        widget.a['href'] = widget['data-text-post-permalink']
    elif case == 'nested':
        wrapper = soup.new_tag('div'); widget.wrap(wrapper); wrapper.append(script.extract())
    elif case == 'hidden':
        widget['hidden'] = ''
    elif case == 'extra_attribute':
        widget['data-other'] = 'unproved'
    elif case == 'wrong_style':
        widget['style'] += 'color:red;'
    elif case in {'foreign_host', 'query'}:
        value = SECOND.replace('www.threads.com', 'www.example.com') if case == 'foreign_host' else SECOND + '?unknown=1'
        widget['data-text-post-permalink'] = value; widget.a['href'] = value
    elif case == 'wrong_id':
        widget['id'] = 'different'
    elif case == 'version':
        widget['data-text-post-version'] = '1'
    elif case == 'anchor_href':
        widget.a['href'] = threads.PERMALINK
    elif case == 'embedded_prose':
        widget.a.div.append('不能遺失的第三方正文')
    elif case == 'hidden_label':
        widget.a.div.find_all('div', recursive=False)[1]['hidden'] = ''
    elif case == 'label_comment':
        widget.a.div.find_all('div', recursive=False)[1].append(Comment('unproved'))
    elif case == 'icon_path':
        widget.path['d'] = 'M1 1L2 2'
    elif case == 'iframe':
        widget.append(soup.new_tag('iframe', src='https://www.example.com/embed'))
    elif case == 'script_inline':
        script.string = 'unexpected()'
    elif case == 'script_src':
        script['src'] = 'https://www.example.com/embed.js'
    elif case == 'script_extra':
        script['defer'] = ''
    elif case == 'script_missing':
        script.decompose()
    elif case == 'extra_script':
        story.append(soup.new_tag('script', src='https://www.threads.com/embed.js'))
    elif case == 'intervening_comment':
        widget.insert_after(Comment('unproved'))
    elif case == 'intervening_text':
        widget.insert_after('不能刪掉的原始文字')
    elif case == 'quoted_pair':
        wrapper = soup.new_tag('blockquote'); widget.wrap(wrapper); wrapper.append(script.extract())
    elif case == 'identity':
        soup.select_one('link[rel=canonical]')['href'] = native.URLS[0] + '1'
    elif case == 'clock':
        soup.select_one('meta[name=pubdate]')['content'] = 'invalid'
    elif case == 'reporter':
        soup.select_one('#credit').decompose()
    elif case == 'wire':
        soup.select_one('#credit').string = '中央社記者／台北報導'
    elif case == 'schema':
        native.update_schema(soup, author={'@type': 'Person', 'name': '別位記者'})
    elif case == 'hidden_prose':
        story.find_all('p', recursive=False)[-1]['style'] = 'display:none'
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser, host=host)


def test_failed_second_pair_does_not_partially_remove_first_pair():
    soup = page(); story = soup.select_one('.story')
    soup.select('.text-post-media')[1].a['target'] = '_self'
    original = str(story)
    with pytest.raises(NewsError):
        adapter._static_threads_declaration(story, allow_multiple=True)
    assert str(story) == original


def test_canonical_fetch_acceptance_keeps_raws_and_sends_no_embedded_request(tmp_path):
    config = clocks.config(12); config['exclusive_http_owner'] = False
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


def test_only_capture_version_changes_not_source_scope_authentication_or_capacity():
    first, old, new = (clocks.registry(v).sources['ettoday'] for v in (1, 11, 12))
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint == first.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint == first.authorization_fingerprint
    assert ettoday_two_threads_parser_repair(new, old.data_fingerprint)
    assert not ettoday_two_threads_parser_repair(new, clocks.registry(10).sources['ettoday'].data_fingerprint)


@pytest.mark.parametrize('boundary', ['eligible', 'body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'state', 'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior', 'skipped_v11', 'different_scope'])
def test_requeue_only_exact_due_missing_identity_once_without_changing_failure_evidence(tmp_path, boundary):
    old = clocks.registry(10 if boundary == 'skipped_v11' else 11, tmp_path)
    if boundary == 'different_scope':
        config = clocks.config(11); config['providers'][0]['authorization']['scope'] += '; other scope'
        old = type(old)(config, base=tmp_path)
    url = adapter.ETTODAY_V12_PROVED_TWO_THREADS_URLS[0]
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': item} for item in (url, *adapter.ETTODAY_V11_PROVED_CAPTION_CREDIT_URLS,
            *adapter.ETTODAY_V10_PROVED_THREADS_URLS, 'https://finance.ettoday.net/news/635788')])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
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
    with closing(NewsQueue(tmp_path, clocks.registry(12, tmp_path))) as queue:
        expected = {key: {**row, 'state': 'retry'} if key == url and boundary == 'eligible' else row for key, row in before.items()}
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0] == '2'
        marker = 'ettoday_two_threads_recheck_v12:' + queue.registry.sources['ettoday'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if boundary in {'unknown_prior', 'skipped_v11', 'different_scope'}:
            assert receipt is None
            return
        proof = json.loads(receipt[0])
        assert proof['rows'] == ([{key: before[url][key] for key in (
            'url', 'state', 'error_code', 'attempts', 'due', 'last_checked_at_utc')}] if boundary == 'eligible' else [])
        if boundary == 'eligible':
            queue.db.execute('UPDATE articles SET state=? WHERE url=?', (before[url]['state'], url)); queue.db.commit()
    with closing(NewsQueue(tmp_path, clocks.registry(12, tmp_path))) as queue:
        assert {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt[0]
