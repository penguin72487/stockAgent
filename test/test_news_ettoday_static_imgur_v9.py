"""Empty native media declarations are not missing publisher prose or a script grant."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ettoday import ETTODAY_V9_PROVED_STATIC_IMGUR_URLS
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_static_imgur_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_break_credit_v8 as breaks
import test_news_ettoday_social_headline_v7 as social


def page(parser='lxml', pairs=1, host='finance'):
    soup = native.page(host, parser)
    for identifier in ('Ab12Cd3', 'Ef45Gh6')[:pairs]:
        pair = BeautifulSoup('<blockquote class="imgur-embed-pub" lang="en" data-id="' + identifier
            + '" data-context="false"></blockquote><script async src="//s.imgur.com/min/embed.js" '
            'charset="utf-8"></script>', parser)
        soup.select_one('.story').extend(list(pair.select('blockquote, script')))
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=9, host='finance'):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('pairs', [1, 2])
def test_complete_prose_and_source_declarations_are_preserved_without_external_requests(parser, pairs):
    soup = page(parser, pairs)
    original = str(soup)
    result = extract(soup, parser)
    baseline = extract(native.page('finance', parser), parser, version=8)
    assert str(soup) == original
    for field in ('body', 'body_sha256', 'title', 'authors_json', 'published_at_raw', 'modified_at_raw',
            'author_attribution_detail_json', 'verified_image_captions_json'):
        assert result[field] == baseline[field]
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    embeds = [n for n in notices if n['kind'] == 'native_empty_static_imgur_declaration']
    assert len(embeds) == pairs
    for node, notice in zip(soup.select('.imgur-embed-pub'), embeds):
        assert notice['source_blockquote_attributes'] == node.attrs
        assert notice['source_script_attributes'] == node.find_next_sibling('script').attrs
        assert notice['raw_declarations_preserved']
        assert not any(notice[key] for key in ('external_request_sent', 'script_executed', 'media_content_accepted'))
    assert 'external_media_not_requested_or_transcribed' in result['body_completeness']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('version', range(1, 9))
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_previous_versions_still_reject_the_observed_external_script(version, parser):
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract(page(parser), parser, version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('variant', ['native', 'social', 'break'])
def test_healthy_v8_outputs_are_not_rewritten(parser, host, variant):
    if variant == 'break' and host == 'www':
        for version in (8, 9):
            with pytest.raises(NewsError):
                extract(breaks.page(parser, host), parser, version, host)
        return  # This observed prefix remains finance-only in both versions.
    soup = native.page(host, parser) if variant == 'native' else (
        social.page(parser, host) if variant == 'social' else breaks.page(parser, host))
    assert extract(soup, parser, 8, host) == extract(soup, parser, 9, host)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('case', ['text', 'whitespace', 'nested', 'unknown_class', 'hidden',
    'extra_attribute', 'context_true', 'language', 'unsafe_id', 'duplicate_id', 'third_pair',
    'no_pair', 'no_script', 'intervening_text', 'intervening_comment', 'nested_script',
    'script_body', 'script_src', 'script_charset', 'script_extra_attribute', 'script_no_async',
    'unknown_script', 'hidden_story', 'foreign_identity', 'foreign_clock', 'no_reporter',
    'wire_credit', 'paywall', 'www', 'media_only'])
def test_no_arbitrary_stripping_media_prose_or_identity_waiver(parser, case):
    soup = page(parser, 2 if case == 'duplicate_id' else 1, 'www' if case == 'www' else 'finance')
    widget = soup.select_one('.imgur-embed-pub')
    script = widget.find_next_sibling('script')
    if case in {'text', 'whitespace'}:
        widget.string = 'unverified fallback prose' if case == 'text' else ' '
    elif case == 'nested':
        widget.append(BeautifulSoup('<span></span>', parser).find('span'))
    elif case == 'unknown_class':
        widget['class'].append('unverified')
    elif case in {'hidden', 'extra_attribute'}:
        widget['hidden' if case == 'hidden' else 'data-other'] = ''
    elif case == 'context_true':
        widget['data-context'] = 'true'
    elif case == 'language':
        widget['lang'] = 'unknown'
    elif case == 'unsafe_id':
        widget['data-id'] = 'a/Ab12Cd3?private=1'
    elif case == 'duplicate_id':
        soup.select('.imgur-embed-pub')[1]['data-id'] = widget['data-id']
    elif case == 'third_pair':
        soup.select_one('.story').append(BeautifulSoup(str(widget) + str(script), parser))
    elif case == 'no_pair':
        widget.decompose()
    elif case == 'no_script':
        script.decompose()
        # Unknown media declarations alone must not acquire a special media
        # completeness notice. Existing script-free empty HTML stays unchanged.
        result = extract(soup, parser)
        assert 'native_empty_static_imgur_declaration' not in result['verified_article_notices_json']
        return
    elif case == 'intervening_text':
        widget.insert_after('unverified media fallback')
    elif case == 'intervening_comment':
        from bs4 import Comment
        widget.insert_after(Comment('unknown boundary'))
    elif case == 'nested_script':
        holder = soup.new_tag('div')
        script.wrap(holder)
    elif case == 'script_body':
        script.string = 'unverifiedJavascript()'
    elif case in {'script_src', 'script_charset', 'script_extra_attribute', 'script_no_async'}:
        if case == 'script_no_async':
            del script['async']
        else:
            field, value = {'script_src': ('src', 'https://unreviewed.test/embed.js'),
                'script_charset': ('charset', 'unknown'), 'script_extra_attribute': ('data-other', '')}[case]
            script[field] = value
    elif case == 'unknown_script':
        soup.select_one('.story').append(soup.new_tag('script', src='https://unreviewed.test/script.js'))
    elif case == 'hidden_story':
        soup.select_one('.story')['hidden'] = ''
    elif case == 'foreign_identity':
        soup.select_one('link[rel=canonical]')['href'] += '0'
    elif case == 'foreign_clock':
        soup.select_one('meta[name=pubdate]')['content'] = native.MOD
    elif case == 'no_reporter':
        soup.select_one('#credit').decompose()
    elif case == 'wire_credit':
        soup.select_one('#credit').string = '來源：中央社'
    elif case == 'paywall':
        native.update_schema(soup, isAccessibleForFree=False)
    elif case == 'media_only':
        for node in soup.select('.story > p, .story > h2'):
            node.decompose()
    with pytest.raises(NewsError):
        extract(BeautifulSoup(str(soup), parser), parser, host='www' if case == 'www' else 'finance')


@pytest.mark.parametrize('prior_version', range(1, 9))
def test_only_capture_changes_with_exact_prior_version_for_repair(prior_version):
    old, new = clocks.registry(prior_version).sources['ettoday'], clocks.registry(9).sources['ettoday']
    assert old.data_fingerprint != new.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert (old.http_capacity_source_fingerprint or old.data_fingerprint) == new.http_capacity_source_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == clocks.registry(8).sources['ettoday'].data_fingerprint
    assert ettoday_static_imgur_parser_repair(new, old.data_fingerprint) == (prior_version == 8)


@pytest.mark.parametrize('ancestry', ['exact', 'unknown', 'changed_scope'])
@pytest.mark.parametrize('case', ['eligible', 'complete', 'saved_body', 'saved_path', 'lease_owner', 'lease_until',
    'unobserved', 'bad_clock', 'naive_clock', 'future_clock', 'wrong_error', 'http_403', 'http_429',
    'paywall_or_login', 'article_attribution_not_verified', 'retry'])
def test_only_two_proved_missing_heads_reopen_once_with_full_history_preserved(tmp_path, ancestry, case):
    old, new = clocks.registry(8, tmp_path), clocks.registry(9, tmp_path)
    if ancestry == 'unknown':
        old.sources['ettoday'] = replace(old.sources['ettoday'], data_fingerprint='f' * 64,
            parser_repair_capture_fingerprint='f' * 64)
    elif ancestry == 'changed_scope':
        cfg = clocks.config(8)
        cfg['providers'][0]['authorization']['scope'] += '; changed scope'
        old = type(old)(cfg, base=tmp_path)
    url = ETTODAY_V9_PROVED_STATIC_IMGUR_URLS[0]
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': value} for value in (*ETTODAY_V9_PROVED_STATIC_IMGUR_URLS,
            'https://finance.ettoday.net/news/3254171', *breaks.ETTODAY_V8_PROVED_BREAK_CREDIT_URLS)])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            'attempts=4,due=9000000000,last_checked_at_utc=?', (utc_now(),))
        if case in {'complete', 'http_403', 'http_429', 'paywall_or_login', 'article_attribution_not_verified', 'retry'}:
            queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (case, case, url))
        elif case in {'saved_body', 'saved_path', 'lease_owner', 'lease_until'}:
            field, value = {'saved_body': ('body_sha256', 'a' * 64), 'saved_path': ('parquet_path', 'old.parquet'),
                'lease_owner': ('lease_owner', 'old-owner'), 'lease_until': ('lease_until', 9000000000)}[case]
            queue.db.execute('UPDATE articles SET ' + field + '=? WHERE url=?', (value, url))
        elif case in {'unobserved', 'bad_clock', 'naive_clock', 'future_clock'}:
            value = {'unobserved': None, 'bad_clock': 'unknown', 'naive_clock': '2026-01-01T00:00:00',
                'future_clock': (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()}[case]
            queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (value, url))
        elif case == 'wrong_error':
            queue.db.execute("UPDATE articles SET error_code='http_403' WHERE url=?", (url,))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ettoday:20261017','2')")
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        queue.db.commit()
    marker = 'ettoday_static_imgur_recheck_v9:' + new.sources['ettoday'].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        eligible = {ETTODAY_V9_PROVED_STATIC_IMGUR_URLS[1]}
        if case == 'eligible':
            eligible.add(url)
        for item, previous in before.items():
            expected = {**previous, 'state': 'retry'} if item in eligible and ancestry == 'exact' else previous
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (item,)).fetchone()) == expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_identity_metadata_attempts:v1:ettoday:20261017'").fetchone()[0] == '2'
        proof = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if ancestry != 'exact':
            assert proof is None
            return
        journal = json.loads(proof[0])
        assert len(journal['rows']) == len(eligible)
        assert journal['attempts_due_error_and_observation_preserved'] and not journal['accepted_as_bodies']
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified'")
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert all(row[0] == 'article_identity_not_verified' for row in queue.db.execute('SELECT state FROM articles'))


def test_closed_version_set_rejects_unknown_adapter():
    from downloader.news_fulltext_ettoday import ettoday_visible_article
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        ettoday_visible_article(page(), page_url=native.URLS[0], adapter_contract_version=clocks.future_version())
