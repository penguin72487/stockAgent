"""Source-declared static quote is not newsroom prose, a fill or a tweet fetch."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

from bs4 import BeautifulSoup, Comment
import pytest

from downloader.news_fulltext_ettoday import ETTODAY_V19_PROVED_STATIC_TWITTER_URLS
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_static_twitter_quote_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks


QUOTE = ('<blockquote class="twitter-tweet"><p lang="en" dir="ltr">'
    'External post -12.5%, 123456 shares and ratio 0.75.<br><br>Original quoted sentence.'
    '<br><a href="https://t.co/ABC123">pic.twitter.com/ABC123</a></p>'
    '\n— Public Researcher (@source_test) <a href="https://twitter.com/source_test/status/1657538027487653888'
    '?ref_src=twsrc%5Etfw">May 13, 2023</a>\n</blockquote>')
SCRIPT = '<script async="" src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>'


def page(parser='lxml', *, host='finance'):
    soup = native.page(host, parser)
    for node in list(BeautifulSoup(QUOTE + '\n' + SCRIPT, 'html.parser').contents):
        soup.select_one('.story').append(node.extract())
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=19, host='finance'):
    return extract_article(str(soup).encode(), replace(clocks.registry(version).sources['ettoday'], html_parser=parser),
        page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_whole_original_journalism_values_and_attribution_are_not_external_post_text(parser):
    soup = page(parser)
    unchanged = str(soup)
    old = extract(native.page('finance', parser), parser, version=18)
    actual = extract(soup, parser)
    assert str(soup) == unchanged
    for key in ('title', 'body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw',
                'verified_image_captions_json', 'author_attribution_detail_json'):
        assert actual[key] == old[key], key
    assert native.PROSE in actual['body'] and 'External post' not in actual['body']
    assert 'Public Researcher' not in actual['authors_json']
    notes = json.loads(actual['verified_article_notices_json'])
    quote, = (item for item in notes if item['kind'] == 'native_static_twitter_quote_declaration')
    assert quote['source_visible_attribution'] == '\n— Public Researcher (@source_test) '
    assert quote['source_post_date_label'] == 'May 13, 2023'
    assert quote['source_quoted_dom_lf_text_sha256'] == hashlib.sha256(soup.blockquote.get_text().encode()).hexdigest()
    assert quote['quoted_post_preserved_in_shared_original'] and quote['excluded_from_newsroom_body']
    assert all(not quote[key] for key in ('external_request_sent', 'script_executed',
        'post_publication_time_inferred', 'post_credit_is_reporter_attribution'))
    assert [n for n in notes if n['kind'] != quote['kind']] == json.loads(old['verified_article_notices_json'])
    assert 'quoted_post_preserved_in_raw_not_newsroom_body' in actual['body_completeness']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, parser, version=18)


def test_html_input_line_endings_have_same_explicit_dom_witness_not_fake_original_byte_digest():
    raw = str(page()).replace('\n', '\r\n').encode()
    values = [extract_article(raw, replace(clocks.registry(19).sources['ettoday'], html_parser=p),
        page_url=native.URLS[0]) for p in ('lxml', 'html.parser')]
    assert values[0] == values[1]
    quote, = (n for n in json.loads(values[0]['verified_article_notices_json'])
        if n['kind'] == 'native_static_twitter_quote_declaration')
    assert quote['text_witness_basis'] == 'HTML_input_CRLF_CR_to_LF; exact_bytes_in_shared_original'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['inline_script', 'script_attrs', 'nested_script', 'second_script',
    'missing_script', 'second_quote', 'quote_attrs', 'nested_quote', 'hidden_quote', 'hidden_paragraph',
    'unknown_tag', 'comment', 'extra_text', 'changed_credit', 'other_user', 'other_host', 'http_link',
    'extra_query', 'bad_date', 'missing_date', 'empty_quote', 'quote_image', 'other_link',
    'nested_link', 'second_paragraph', 'missing_newsroom_prose', 'missing_reporter', 'foreign_reporter',
    'paid', 'wrong_canonical', 'wrong_clock', 'intervening_prose', 'tweet_iframe'])
def test_unobserved_markup_identity_and_newsroom_guards_remain_closed(parser, change):
    soup = page(parser)
    story, widget = soup.select_one('.story'), soup.blockquote
    script = story.select_one('script[src*="twitter"]')
    paragraph = widget.p
    date = widget.find('a', recursive=False)
    if change == 'inline_script': script.string = 'alert(1)'
    elif change == 'script_attrs': script['defer'] = ''
    elif change == 'nested_script': script.wrap(soup.new_tag('p'))
    elif change == 'second_script': story.append(BeautifulSoup(SCRIPT, parser).find('script'))
    elif change == 'missing_script': script.decompose()
    elif change == 'second_quote': story.append(BeautifulSoup(QUOTE, parser).blockquote)
    elif change == 'quote_attrs': widget['data-theme'] = 'dark'
    elif change == 'nested_quote': widget.wrap(soup.new_tag('div'))
    elif change == 'hidden_quote': widget['hidden'] = ''
    elif change == 'hidden_paragraph': paragraph['style'] = 'display:none'
    elif change == 'unknown_tag': paragraph.append(soup.new_tag('b'))
    elif change == 'comment': paragraph.append(Comment('not visible'))
    elif change == 'extra_text': widget.append('unattributed extra material')
    elif change == 'changed_credit': widget.contents[-3].replace_with('unknown attribution')
    elif change == 'other_user': date['href'] = date['href'].replace('source_test', 'different')
    elif change == 'other_host': date['href'] = date['href'].replace('twitter.com', 'example.org')
    elif change == 'http_link': date['href'] = date['href'].replace('https:', 'http:')
    elif change == 'extra_query': date['href'] += '&unverified=1'
    elif change == 'bad_date': date.string = 'February 30, 2023'
    elif change == 'missing_date': date.decompose()
    elif change == 'empty_quote': paragraph.clear()
    elif change == 'quote_image': paragraph.append(soup.new_tag('img', src='https://example.org/image.jpg'))
    elif change == 'other_link': paragraph.a['href'] = 'https://example.org/'
    elif change == 'nested_link': paragraph.a.append(soup.new_tag('b'))
    elif change == 'second_paragraph': widget.append(soup.new_tag('p'))
    elif change == 'missing_newsroom_prose':
        for node in list(story.find_all('p', recursive=False)):
            if node.get('id') not in {'credit'} and not node.find('img') and not node.strong:
                node.decompose()
    elif change == 'missing_reporter': story.select_one('#credit').decompose()
    elif change == 'foreign_reporter': story.select_one('#credit').string = '中央社記者報導'
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'wrong_canonical': soup.select_one('link[rel=canonical]')['href'] += '1'
    elif change == 'wrong_clock': native.update_schema(soup, datePublished='2026-10-13T22:19:00+08:00')
    elif change == 'intervening_prose': widget.insert_after(soup.new_tag('p'))
    elif change == 'tweet_iframe': story.append(soup.new_tag('iframe', src='https://twitter.com/embed'))
    with pytest.raises(NewsError):
        extract(soup, parser)


@pytest.mark.parametrize('version', range(1, 20))
def test_previously_supported_plain_articles_have_identical_values(version):
    soup = native.page()
    assert extract(soup, version=version) == extract(soup, version=1)


def test_unknown_version_and_unobserved_www_quote_stay_closed():
    with pytest.raises(ValueError):
        clocks.registry(clocks.future_version())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(host='www'), host='www')


def test_capture_ancestry_keeps_authorization_and_pacing_identity():
    old, new = clocks.registry(18).sources['ettoday'], clocks.registry(19).sources['ettoday']
    assert ettoday_static_twitter_quote_parser_repair(new, old.data_fingerprint)
    assert not ettoday_static_twitter_quote_parser_repair(new, clocks.registry(17).sources['ettoday'].data_fingerprint)
    assert old.data_fingerprint != new.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
    assert new.parser_repair_capture_history[-1] == old.data_fingerprint


@pytest.mark.parametrize('excluded', ['none', 'future_due', 'unobserved', 'future_observation', 'naive_observation',
    'leased', 'has_body', 'has_partition', 'other_url', 'other_failure', 'wrong_error', 'complete'])
def test_once_only_retry_is_one_proved_missing_due_unleased_head(tmp_path, excluded):
    old, new = clocks.registry(18), clocks.registry(19)
    url = ETTODAY_V19_PROVED_STATIC_TWITTER_URLS[0]
    if excluded == 'other_url': url = url.replace('2498959', '2498960')
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}])
        values = {'state': 'article_identity_not_verified', 'error_code': 'article_identity_not_verified',
            'attempts': 3, 'due': 99, 'last_checked_at_utc': utc_now()}
        if excluded == 'future_due': values['due'] = (datetime.now(timezone.utc) + timedelta(days=1)).timestamp()
        elif excluded == 'unobserved': values['last_checked_at_utc'] = None
        elif excluded == 'future_observation': values['last_checked_at_utc'] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        elif excluded == 'naive_observation': values['last_checked_at_utc'] = '2026-10-18T00:00:00'
        elif excluded == 'leased': values.update(lease_owner='other_owner', lease_until=1)
        elif excluded == 'has_body': values['body_sha256'] = 'a' * 64
        elif excluded == 'has_partition': values['parquet_path'] = 'source.parquet'
        elif excluded == 'other_failure': values['state'] = 'article_attribution_not_verified'
        elif excluded == 'wrong_error': values['error_code'] = 'different_error'
        elif excluded == 'complete': values['state'] = 'complete'
        with queue.db:
            queue.db.execute('UPDATE articles SET ' + ','.join(k + '=?' for k in values) + ' WHERE url=?', (*values.values(), url))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        assert after == {**before, 'state': 'retry' if excluded == 'none' else before['state']}
        marker = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('ettoday_static_twitter_quote_recheck_v19:' + new.sources['ettoday'].data_fingerprint,)).fetchone()[0])
        assert len(marker['rows']) == (excluded == 'none') and not marker['accepted_as_bodies']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (url,))
        queue.recheck_ettoday_static_twitter_quote(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_identity_not_verified'
