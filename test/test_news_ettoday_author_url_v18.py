"""Exact observed non-value comment; no requests, invented values or retries."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json

import pytest

from downloader.news_fulltext_ettoday import (
    _native_json, ETTODAY_V18_PROVED_AUTHOR_URL_COMMENT_URLS,
)
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_author_url_comment_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks


COMMENT = '//如果有筆名，才會有url'
AUTHOR_URL = 'https://www.ettoday.net/reporter/sample.php7?pid=677'


def schema(*, author_list=False, graph=False):
    author = {'@type': 'Person', 'name': '研究記者', 'url': AUTHOR_URL}
    article = {'@type': 'NewsArticle', 'author': [author] if author_list else author,
               'description': '原始-12.5%、123456股、0.75及字串//不是註解'}
    return {'@graph': [article]} if graph else article


def commented(value):
    return json.dumps(value, ensure_ascii=False, indent=2).replace(
        json.dumps(AUTHOR_URL), json.dumps(AUTHOR_URL) + ' ' + COMMENT)


@pytest.mark.parametrize('author_list', [False, True])
@pytest.mark.parametrize('graph', [False, True])
def test_exact_person_url_comment_preserves_every_decoded_value_and_original_text(author_list, graph):
    value = schema(author_list=author_list, graph=graph)
    text = commented(value)
    original = text
    decoded, notices = _native_json(text, allow_author_url_comment=True)
    assert decoded == value and text == original
    assert notices == ['source_non_json_author_url_comment']
    with pytest.raises(ValueError):
        _native_json(text, allow_literal_value_controls=True)
    assert _native_json(json.dumps(value)) == (value, [])


@pytest.mark.parametrize('bad', ['publisher', 'nested', 'organization', 'root_author', 'not_article',
    'extra_member', 'comma_before', 'different_comment', 'block_comment', 'duplicate_comment',
    'no_newline', 'trailing_comma', 'marker_collision', 'escaped_marker_collision'])
def test_unreviewed_comment_grammar_or_object_position_stays_closed(bad):
    value = schema()
    author = value['author']
    if bad == 'publisher': value['publisher'] = value.pop('author')
    elif bad == 'nested': value['author'] = {'extra': author}
    elif bad == 'organization': author['@type'] = 'Organization'
    elif bad == 'root_author': value = author
    elif bad == 'not_article': value['@type'] = 'Organization'
    elif bad == 'extra_member': author['extra'] = 'source value'
    elif bad in {'marker_collision', 'escaped_marker_collision'}:
        value['__stockagent_author_url_comment_position__'] = 'original source member'
    text = commented(value)
    if bad == 'comma_before': text = text.replace(' ' + COMMENT, ', ' + COMMENT)
    elif bad == 'different_comment': text = text.replace(COMMENT, COMMENT + ' other')
    elif bad == 'block_comment': text = text.replace(COMMENT, '/*' + COMMENT + '*/')
    elif bad == 'duplicate_comment': text = text.replace(COMMENT, COMMENT + '\n' + COMMENT)
    elif bad == 'no_newline': text = text[:text.index(COMMENT)] + COMMENT
    elif bad == 'trailing_comma': text = text.replace(COMMENT + '\n', COMMENT + '\n,')
    elif bad == 'escaped_marker_collision':
        text = text.replace('__stockagent_author_url_comment_position__', '\\u005f_stockagent_author_url_comment_position__')
    with pytest.raises(ValueError):
        _native_json(text, allow_literal_value_controls=True, allow_author_url_comment=True)


def test_url_comment_and_literal_controls_are_lossless_but_bad_keys_stay_invalid():
    value = schema()
    value['description'] += '\x02\x08\n'
    text = commented(value).replace('\\u0002', '\x02').replace('\\b', '\x08')
    result, notices = _native_json(text, allow_literal_value_controls=True, allow_author_url_comment=True)
    assert result == value
    assert set(notices) == {'source_non_json_author_url_comment', 'source_unescaped_json_value_control'}
    with pytest.raises(ValueError):
        _native_json(text.replace('"description"', '"descr\x02iption"'),
                     allow_literal_value_controls=True, allow_author_url_comment=True)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_full_native_semantics_unchanged_except_observed_syntax_notice(parser, host):
    soup = native.page(host, parser)
    source_schema = json.loads(soup.select_one('#report').get_text())
    person = {'@type': 'Person', 'name': '研究記者', 'url': AUTHOR_URL}
    source_schema['author'] = person
    text = json.dumps(source_schema, ensure_ascii=False, indent=2)
    soup.select_one('#report').string = text
    raw_before = str(soup).encode()
    soup.select_one('#report').string = commented(source_schema)
    raw = str(soup).encode()
    url = native.URLS[host == 'www']
    expected = extract_article(raw_before, clocks.registry(17).sources['ettoday'], page_url=url)
    result = extract_article(raw, clocks.registry(18).sources['ettoday'], page_url=url)
    notice = json.loads(result.pop('verified_article_notices_json'))
    old_notices = json.loads(expected.pop('verified_article_notices_json'))
    assert result == expected
    assert [item for item in notice if item['kind'] == 'source_non_json_author_url_comment'] == [
        {'kind': 'source_non_json_author_url_comment', 'raw_evidence': 'shared_original_response_receipt'}]
    assert [item for item in notice if item['kind'] != 'source_non_json_author_url_comment'] == old_notices
    for version in range(1, 18):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract_article(raw, clocks.registry(version).sources['ettoday'], page_url=url)


def test_capture_ancestry_changes_without_access_or_capacity_reset():
    old, new = clocks.registry(17).sources['ettoday'], clocks.registry(18).sources['ettoday']
    assert ettoday_author_url_comment_parser_repair(new, old.data_fingerprint)
    assert not ettoday_author_url_comment_parser_repair(new, clocks.registry(16).sources['ettoday'].data_fingerprint)
    assert old.data_fingerprint != new.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
    assert new.parser_repair_capture_history[-1] == old.data_fingerprint
    with pytest.raises(ValueError):
        clocks.registry(clocks.future_version())


@pytest.mark.parametrize('excluded', ['none', 'future_due', 'unobserved', 'future_observation', 'naive_observation',
    'leased', 'has_body', 'has_partition', 'other_url', 'other_failure', 'wrong_error', 'complete'])
def test_once_only_retry_is_one_proved_missing_due_unleased_head(tmp_path, excluded):
    old, new = clocks.registry(17), clocks.registry(18)
    url = ETTODAY_V18_PROVED_AUTHOR_URL_COMMENT_URLS[0]
    if excluded == 'other_url': url = url.replace('3174135', '3174136')
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
            ('ettoday_author_url_comment_recheck_v18:' + new.sources['ettoday'].data_fingerprint,)).fetchone()[0])
        assert len(marker['rows']) == (excluded == 'none') and not marker['accepted_as_bodies']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (url,))
        queue.recheck_ettoday_author_url_comment(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_identity_not_verified'
