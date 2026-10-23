"""Declared names, publisher groups and missing writer profiles stay separate."""
from copy import deepcopy
import json
import time

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_stockfeel import (
    STOCKFEEL_V6_CREDIT_NOTE, STOCKFEEL_V7_CREDIT_NOTE, STOCKFEEL_V7_VERIFICATION, STOCKFEEL_V7_HTTP_BINDING,
)
from downloader import news_fulltext_candidate_diagnostics as diagnostic
from test_news_fulltext import config
from test_news_stockfeel_original import html, settings
from test_news_stockfeel_roles import extract, role_html
from test_news_stockfeel_title_roles import titles, v6_settings
from test_news_stockfeel_written_roles import writer_html, with_head, chart_html


PUBLISHER = '股感知識庫'
PROFILE = 'https://www.stockfeel.com.tw/author/stockfeel'


def unlinked_html(html, name='合成未連結作者'):
    soup = BeautifulSoup(writer_html(html), 'lxml')
    writer = soup.select('.sin-content-header div[data-aid]')[0]
    writer['data-aid'] = ''
    writer.find_all('span', recursive=False)[1].attrs = {'class': ['f5', 'tet-gray-2', 'b']}
    writer.find_all('span', recursive=False)[1].string = ' ' + name + ' '
    soup.select_one('head > meta[name=author]')['content'] = PUBLISHER
    soup.select_one('.user-avatar')['href'] = PROFILE
    return str(soup)


def v7_settings():
    settings, repo = v6_settings()
    entry = settings['providers'][0]
    entry['embedded_article_layout'] = 'stockfeel_visible_original_v7'
    entry['adapter_verification'] = STOCKFEEL_V7_VERIFICATION
    entry['note'] = entry['note'].replace(STOCKFEEL_V6_CREDIT_NOTE, STOCKFEEL_V7_CREDIT_NOTE)
    return settings, repo


def test_named_unlinked_writer_keeps_missing_ID_profile_and_publisher_role(settings, html):
    raw = titles(unlinked_html(html))
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(settings, raw, 6)
    row = extract(settings, raw, 7)
    assert json.loads(row['authors_json']) == ['合成未連結作者']
    detail = json.loads(row['author_attribution_detail_json'])
    writer, publisher = detail['source_header_declarations']
    assert writer['name_text_raw'] == ' 合成未連結作者 '
    assert writer['name'] == '合成未連結作者' and writer['data_aid'] == '' and writer['profile_url'] is None
    assert not writer['numeric_id_declared'] and not writer['profile_declared']
    assert publisher['data_aid'] == '67' and publisher['name'] == PUBLISHER
    assert detail['meta_author_raw'] == PUBLISHER
    assert detail['head_declared_names'] == [PUBLISHER]
    assert not detail['native_writer_profile_declared'] and not detail['native_writer_numeric_id_declared']
    assert detail['avatar_role'] == 'declared_publisher_group_not_native_writer'
    assert detail['avatar_profile_url_raw'] == PROFILE
    assert not detail['human_reporter_identity_verified'] and not detail['rendered_byline_visibility_verified']
    assert 'missing_writer_ID_profile_stay_missing' in row['article_identity_basis']
    assert '-1,234.50' in row['body']
    assert row['published_at_raw'] == '2026-10-15T07:07:51+00:00'
    assert row['modified_at_raw'] == '2026-10-15T07:07:55+00:00'
    assert not row['historical_point_in_time']


def test_linked_writer_is_not_replaced_by_publisher_head_group(settings, html):
    raw = with_head(html, PUBLISHER)
    with pytest.raises(NewsError, match='article_attribution_not_verified'):
        extract(settings, raw, 6)
    row = extract(settings, raw, 7)
    detail = json.loads(row['author_attribution_detail_json'])
    assert json.loads(row['authors_json']) == ['合成作者甲']
    assert detail['meta_author_raw'] == PUBLISHER and detail['head_declared_names'] == [PUBLISHER]
    assert detail['native_writer_profile_declared'] and detail['native_writer_numeric_id_declared']
    assert detail['avatar_role'] == 'declared_native_writer'
    assert detail['avatar_profile_url_raw'] == 'https://www.stockfeel.com.tw/author/synthetic.writer'
    assert detail['publisher_head_group_is_not_native_writer']


@pytest.mark.parametrize('name', ['', ' ', '股感知識庫', '股感時事特派員', '股感產業研究員',
    'x'*129, 'a\x00b', 'a\nb', 'a\tb', 'a\u200bb'])
def test_unlinked_writer_requires_bounded_plain_name_not_a_guessed_group(settings, html, name):
    with pytest.raises(NewsError):
        extract(settings, unlinked_html(html, name), 7)


@pytest.mark.parametrize('mutation', [
    'missing-aid', 'nonempty-aid', 'nested-name', 'comment', 'duplicate-value', 'hidden-value',
    'style-value', 'event-value', 'unknown-class', 'missing-name', 'publisher-avatar-mismatch',
    'head-writer-instead-of-publisher', 'unknown-head', 'unknown-source-ID', 'unknown-source-name',
    'external-source-profile', 'wrong-role-label', 'hidden-credit',
])
def test_unlinked_credit_never_waives_source_avatar_markup_or_visibility(settings, html, mutation):
    soup = BeautifulSoup(unlinked_html(html), 'lxml')
    author, publisher = soup.select('.sin-content-header div[data-aid]')
    value = author.find_all('span', recursive=False)[1]
    if mutation == 'missing-aid':
        del author['data-aid']
    elif mutation == 'nonempty-aid':
        author['data-aid'] = '215'
    elif mutation == 'nested-name':
        value.clear(); value.append(soup.new_tag('span')); value.span.string = '合成未連結作者'
    elif mutation == 'comment':
        from bs4 import Comment
        value.append(Comment('not a declared name'))
    elif mutation == 'duplicate-value':
        author.append(soup.new_tag('span'))
    elif mutation in {'hidden-value', 'style-value', 'event-value', 'unknown-class'}:
        key, val = {'hidden-value': ('hidden', ''), 'style-value': ('style', 'display:none'),
            'event-value': ('onclick', 'unknown()'), 'unknown-class': ('class', ['unknown'])}[mutation]
        value[key] = val
    elif mutation == 'missing-name':
        value.clear()
    elif mutation == 'publisher-avatar-mismatch':
        soup.select_one('.user-avatar')['href'] = 'https://www.stockfeel.com.tw/author/guessed-writer'
    elif mutation in {'head-writer-instead-of-publisher', 'unknown-head'}:
        soup.select_one('head > meta[name=author]')['content'] = '合成未連結作者' if mutation.startswith('head-writer') else '其他團隊'
    elif mutation == 'unknown-source-ID':
        publisher['data-aid'] = '368'
    elif mutation == 'unknown-source-name':
        publisher.a.string = '其他來源'
    elif mutation == 'external-source-profile':
        publisher.a['href'] = 'https://other.example/author/stockfeel'
    elif mutation == 'wrong-role-label':
        author.find_all('span', recursive=False)[0].string = '編輯'
    elif mutation == 'hidden-credit':
        author['hidden'] = ''
    with pytest.raises(NewsError):
        extract(settings, str(soup), 7)


@pytest.mark.parametrize('old,new', [
    ('</table>', '</table><iframe src="/not-requested"></iframe>'),
    ('</table>', '</table><form></form>'),
    ('<h1>', '<h1 hidden>'),
    ('© Stockfeel. All rights reserved', 'unreviewed copyright'),
    ('2026-10-15T07:07:51+00:00', '2026-10-15T07:07:51'),
])
def test_missing_writer_profile_never_waives_other_article_gates(settings, html, old, new):
    with pytest.raises(NewsError):
        extract(settings, unlinked_html(html).replace(old, new), 7)


def test_old_v6_fields_do_not_change_when_new_credit_roles_are_unused(settings, html):
    for raw in (html, role_html(html), writer_html(html), with_head(html, '股感商品研究室'),
            with_head(html, '合成作者甲, 合成作者乙'), titles(writer_html(html)), chart_html(writer_html(html))):
        assert extract(settings, raw, 7) == extract(settings, raw, 6)


def test_exact_v7_capture_preserves_HTTP_refusal_and_authorization():
    settings, repo = v7_settings()
    source = PolicyRegistry(settings, base=repo).sources['stockfeel']
    assert (source.data_fingerprint, source.authorization_fingerprint) == STOCKFEEL_V7_HTTP_BINDING[1:3]
    assert (source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint) == STOCKFEEL_V7_HTTP_BINDING[3:]
    assert source.parser_repair_prior_capture_fingerprint == STOCKFEEL_V7_HTTP_BINDING[0]
    for field, value in [('interval_seconds', 3), ('parallelism', 2), ('capacity_max_parallelism', 2)]:
        changed = deepcopy(settings); changed['providers'][0][field] = value
        other = PolicyRegistry(changed, base=repo).sources['stockfeel']
        assert not other.http_capacity_source_fingerprint and not other.http_capacity_authorization_fingerprint


def test_v7_upgrade_does_not_refetch_healthy_heads_reset_failures_or_repeat_originals(tmp_path):
    settings, repo = v6_settings()
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=repo))
    try:
        states = ['complete', 'article_identity_not_verified', 'article_attribution_not_verified',
            'article_rights_or_scope_excluded', 'paywall_or_login', 'awaiting_authorization', 'http_403', 'http_429']
        for index, state in enumerate(states):
            url = 'https://www.stockfeel.com.tw/synthetic-'+str(index)+'/'
            with queue.db:
                queue.add_articles([{'url': url}])
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=? WHERE url=?',
                    (state, state, time.time()-60, url))
                if state == 'complete':
                    queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a'*64, url))
        old = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        with queue.db:
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', ('failure_attempts:stockfeel', '17'))
        following, _ = v7_settings()
        queue.registry = PolicyRegistry(following, base=repo)
        queue.reconcile_policy()
        assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == old
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', ('failure_attempts:stockfeel',)).fetchone()[0] == '17'
        for version in (14, 15):
            target = {'provider': 'stockfeel', 'id': 'a'*64}
            assert diagnostic._key(target, 'a'*64, evidence_contract_version=version) == diagnostic._key(target, 'b'*64, evidence_contract_version=version)
    finally:
        queue.close()
