"""Measured native TVBS v2 fixes, raw clocks, ordered credits and exact retries."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import CONTRACT_VERSION, NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, tvbs_native_v2_parser_repair, utc_now
from downloader.news_fulltext_tvbs import tvbs_validation_metadata
from scripts.audit_news_selected_articles import audit
from test_news_failure_diagnostics import fail
from test_news_failure_identity_metadata import evidence
import test_news_tvbs as native


def config(entry):
    return {'contract_version': CONTRACT_VERSION, 'publish': False, 'remote_training_export': False,
        'providers': [entry], 'collection_purpose': 'personal_research', 'user_agent': 'StockAgentNewsResearch/1.0'}


def registry(version=2, *, parser='lxml'):
    entry = native.recipe(parser)
    entry['embedded_article_layout'] = f'tvbs_visible_article_v{version}'
    return PolicyRegistry(config(entry), base=Path.cwd())


def extract(soup, *, version=2, parser='lxml', url=native.URLS[0]):
    return extract_article(str(soup).encode(), registry(version, parser=parser).sources['tvbs'], page_url=url)


def update_schema(soup, **changes):
    # Repeated fixture edits retain the exact script string; BeautifulSoup's
    # Script-vs-NavigableString filtering must not manufacture empty JSON.
    node = soup.select_one('script#report')
    value = json.loads(str(node.string))
    value.update(changes)
    node.string = json.dumps(value, ensure_ascii=False)


def credits(soup, *, role='記者', secondary=None):
    schema_author = {'@type': 'Person', 'name': '研究記者'}
    authors = [schema_author, {'@type': 'Person', 'name': secondary}] if secondary else schema_author
    update_schema(soup, author=authors)
    label = role + '：研究記者' + ('｜責任編輯：' + secondary if secondary else '')
    for node in soup.select('header ul > li:first-child'):
        node.string = label


def skew(soup, modified='2026-10-14T08:29:25.000Z'):
    update_schema(soup, dateModified=modified)
    soup.select_one('meta[property="article:modified_time"]')['content'] = modified
    for node in soup.select('header li'):
        if node.get_text().startswith('最後更新時間'):
            node.string = '最後更新時間：2026.10.14 16:29'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('role,secondary', [('記者', None), ('記者', '編輯組'), ('編輯', '責任編輯名'),
    ('編譯', None), ('編譯', '責任編輯名')])
def test_native_translator_and_ordered_responsible_editor_are_not_dropped(parser, role, secondary):
    soup = native.page(parser=parser)
    credits(soup, role=role, secondary=secondary)
    result = extract(soup, parser=parser)
    names = ['研究記者', secondary] if secondary else ['研究記者']
    assert json.loads(result['authors_json']) == names
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    assert not result['historical_point_in_time']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    detail = json.loads(result['author_attribution_detail_json'])
    assert not detail['author_name_invented']
    if secondary:
        assert [a['name'] for a in detail['publisher_ordered_authors_with_responsible_editor']] == names
    if role == '編譯' or secondary:
        with pytest.raises(NewsError):
            extract(soup, version=1, parser=parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('secondary', [None, '編輯組'])
def test_exact_same_minute_source_one_second_anomaly_is_preserved_not_corrected(parser, secondary):
    soup = native.page(parser=parser)
    credits(soup, secondary=secondary)
    skew(soup)
    result = extract(soup, parser=parser)
    assert result['published_at_raw'] == '2026-10-14T08:29:26.000Z'
    assert result['modified_at_raw'] == '2026-10-14T08:29:25.000Z'
    assert 'source_declared_revision_precedes_release_exactly_1_second_same_visible_minute' in result['published_time_basis']
    assert 'raw_clocks_preserved_not_corrected' in result['published_time_basis']
    assert not result['historical_point_in_time']
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract(soup, version=1, parser=parser)


@pytest.mark.parametrize('change', ['two_seconds', 'previous_day', 'cross_minute', 'meta_disagrees',
    'visible_disagrees', 'missing_publication', 'missing_modification', 'naive', 'foreign_host'])
def test_source_clock_anomaly_exception_never_becomes_an_arbitrary_clock_repair(change):
    soup = native.page()
    skew(soup)
    url = native.URLS[0]
    if change == 'two_seconds':
        skew(soup, '2026-10-14T08:29:24.000Z')
    elif change == 'previous_day':
        skew(soup, '2026-10-13T08:29:25.000Z')
    elif change == 'cross_minute':
        update_schema(soup, datePublished='2026-10-14T08:29:00.000Z')
        soup.select_one('meta[property="article:published_time"]')['content'] = '2026-10-14T08:29:00.000Z'
        skew(soup, '2026-10-14T08:28:59.000Z')
    elif change == 'meta_disagrees':
        soup.select_one('meta[property="article:modified_time"]')['content'] = '2026-10-14T08:29:24.000Z'
    elif change == 'visible_disagrees':
        soup.select('header li')[2].string = '最後更新時間：2026.10.14 16:28'
    elif change.startswith('missing_'):
        update_schema(soup, **{'datePublished' if change.endswith('publication') else 'dateModified': None})
    elif change == 'naive':
        skew(soup, '2026-10-14T08:29:25')
    else:
        soup = native.page('cars.tvbs.com.tw')
        url = native.URLS[1]
        skew(soup)
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        extract(soup, url=url)


@pytest.mark.parametrize('change', ['unknown_role', 'external_name', 'missing_editor', 'reordered_schema',
    'different_editor', 'duplicate_names', 'hidden_credit', 'responsive_conflict', 'third_author',
    'name_delimiter', 'long_name'])
def test_competing_unknown_external_hidden_or_incomplete_credits_still_fail(change):
    soup = native.page()
    credits(soup, secondary='責任編輯名')
    if change == 'unknown_role':
        credits(soup, role='作者', secondary='責任編輯名')
    elif change == 'external_name':
        credits(soup, secondary='品觀點')
    elif change == 'missing_editor':
        for node in soup.select('header ul > li:first-child'):
            node.string = '記者：研究記者'
    elif change == 'reordered_schema':
        update_schema(soup, author=[{'@type': 'Person', 'name': '責任編輯名'}, {'@type': 'Person', 'name': '研究記者'}])
    elif change == 'different_editor':
        soup.select('header ul > li:first-child')[1].string = '記者：研究記者｜責任編輯：另一位'
    elif change == 'duplicate_names':
        credits(soup, secondary='研究記者')
    elif change == 'hidden_credit':
        soup.select_one('header li')['hidden'] = ''
    elif change == 'responsive_conflict':
        soup.select('header ul > li:first-child')[1].string = '編輯：研究記者｜責任編輯：責任編輯名'
    elif change == 'third_author':
        update_schema(soup, author=[{'@type': 'Person', 'name': n} for n in ('研究記者', '責任編輯名', '第三位')])
    elif change == 'name_delimiter':
        credits(soup, secondary='某人｜責任編輯：別人')
    else:
        credits(soup, secondary='人' * 81)
    with pytest.raises(NewsError):
        extract(soup)


def test_all_original_single_author_outputs_are_exactly_equal_across_versions():
    for host, url in zip(native.TVBS_HOSTS, native.URLS):
        soup = native.page(host)
        assert extract(soup, version=1, url=url) == extract(soup, version=2, url=url)


@pytest.mark.parametrize('change', ['scope', 'authorization', 'domain', 'parser', 'path', 'cadence'])
def test_native_parser_identity_cannot_hide_any_other_capture_change(change):
    old = registry(1).sources['tvbs']
    new = registry(2).sources['tvbs']
    assert new.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.data_fingerprint != old.data_fingerprint
    assert tvbs_native_v2_parser_repair(new, old.data_fingerprint)
    entry = native.recipe()
    entry['embedded_article_layout'] = 'tvbs_visible_article_v2'
    if change in {'scope', 'authorization'}:
        entry['authorization']['scope' if change == 'scope' else 'reviewed_on'] += '-changed'
    elif change == 'domain':
        entry['domains'].append('www.tvbs.com.tw')
    elif change == 'parser':
        entry['html_parser'] = 'html.parser'
    elif change == 'path':
        entry['article_path_pattern'] = '^/money/[0-9]+$'
    else:
        entry['interval_seconds'] = 3
    try:
        changed = PolicyRegistry(config(entry), base=Path.cwd()).sources['tvbs']
    except ValueError:
        return  # Strict layout scope rejected the change before any owner.
    assert not tvbs_native_v2_parser_repair(changed, old.data_fingerprint)


@pytest.mark.parametrize('code', ['invalid_bulk_publication_clock', 'article_attribution_not_verified', 'article_rights_or_scope_excluded'])
def test_owner_failure_metadata_uses_existing_budget_and_preserves_source_clock_and_credit_shape(tmp_path, code):
    soup = native.page()
    credits(soup, secondary='責任編輯名')
    skew(soup)
    raw = Response(native.URLS[0], 200, {'content-type': 'text/html'}, str(soup).encode())
    policy = registry()
    with closing(NewsQueue(tmp_path, policy)) as queue:
        for _ in range(4):
            fail(queue, raw, code=code, received_at_utc='2026-10-15T00:05:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(paths) == 2
        facts = rows[0]['facts']['tvbs_native_validation']
        assert facts['declared_publication'] == '2026-10-14T08:29:26.000Z'
        assert facts['declared_modification'] == '2026-10-14T08:29:25.000Z'
        assert facts['declared_author_count'] == 2
        assert all(c['responsible_editor_present'] for c in facts['native_credit_shapes'])
        assert facts['native_original_body_container_count'] == 1
        assert not facts['body_text_retained'] and not facts['raw_scope_or_article_acceptance_granted']
        assert all('研究記者' not in p.read_text() and '責任編輯名' not in p.read_text() and native.PROSE not in p.read_text() for p in paths)
        assert not list((tmp_path / 'diagnostics').rglob('*.parquet'))
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    policy.sources['tvbs'] = replace(policy.sources['tvbs'], data_fingerprint='upgraded-parser')
    with closing(NewsQueue(tmp_path, policy)) as queue:
        fail(queue, raw, code=code, received_at_utc='2026-10-15T00:05:00+00:00')
        assert len(evidence(tmp_path)[0]) == 2


@pytest.mark.parametrize('invalid', ['broken_schema', 'nested_author_type', 'nested_name', 'many_authors', 'hidden_credit'])
def test_native_diagnostics_cannot_accept_or_export_malformed_or_private_source_fields(invalid):
    soup = native.page()
    if invalid == 'broken_schema':
        soup.select_one('script#report').string = 'private-broken-json'
    elif invalid == 'nested_author_type':
        update_schema(soup, author={'@type': ['private-type'], 'name': 'private-name'})
    elif invalid == 'nested_name':
        update_schema(soup, author={'@type': 'Person', 'name': ['private-name']})
    elif invalid == 'many_authors':
        update_schema(soup, author=[{'@type': 'Person', 'name': 'private-name'}] * 20)
    else:
        soup.select_one('header li')['hidden'] = ''
    facts = tvbs_validation_metadata(BeautifulSoup(str(soup), 'lxml'), page_url=native.URLS[0])
    assert not facts['body_text_retained'] and not facts['raw_scope_or_article_acceptance_granted']
    assert 'private-' not in json.dumps(facts) and len(json.dumps(facts)) < 8192


def test_v2_retry_cohort_preserves_all_evidence_and_is_one_shot_after_restart(tmp_path):
    old = registry(1)
    originals = {}
    cases = [
        ('https://news.tvbs.com.tw/money/11', 'invalid_bulk_publication_clock', True),
        ('https://news.tvbs.com.tw/money/12', 'article_attribution_not_verified', True),
        ('https://news.tvbs.com.tw/money/4033506', 'article_rights_or_scope_excluded', True),
        ('https://news.tvbs.com.tw/money/4030694', 'article_rights_or_scope_excluded', False),
        ('https://news.tvbs.com.tw/money/15', 'article_identity_not_verified', False),
        ('https://news.tvbs.com.tw/money/16', 'body_not_found', False),
        ('https://news.tvbs.com.tw/money/17', 'http_403', False),
        ('https://news.tvbs.com.tw/money/18', 'http_429', False),
        ('https://news.tvbs.com.tw/money/19', 'paywall_or_login', False),
        ('https://news.tvbs.com.tw/money/20', 'source_not_found', False),
        ('https://news.tvbs.com.tw/money/21', 'complete', False),
        ('https://cars.tvbs.com.tw/car-news/22', 'invalid_bulk_publication_clock', False),
    ]
    with closing(NewsQueue(tmp_path, old)) as queue:
        for url, state, _ in cases:
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, utc_now(), url))
            originals[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        queue.db.commit()
    new = registry(2)
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, _, retry in cases:
            current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
            assert current == ({**originals[url], 'state': 'retry'} if retry else originals[url])
        marker = 'tvbs_native_credit_clock_recheck_v2:' + new.sources['tvbs'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert len(proof['rows']) == 3
        assert all(row['attempts'] == 3 and row['due'] == 9000000000 for row in proof['rows'])
        assert not proof['accepted_as_bodies']
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock' WHERE url=?", (cases[0][0],))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (cases[0][0],)).fetchone()[0] == 'invalid_bulk_publication_clock'
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('change', ['stored_hash', 'stored_path', 'lease_owner', 'lease_until',
    'missing_observation', 'different_error', 'unknown_prior_capture'])
def test_v2_migration_does_not_touch_saved_leased_unobserved_or_unproved_head(tmp_path, change):
    old = registry(1)
    url = 'https://news.tvbs.com.tw/money/11'
    columns = {'stored_hash': ('body_sha256', 'a' * 64), 'stored_path': ('parquet_path', 'bodies/old.parquet'),
        'lease_owner': ('lease_owner', 'another-live-owner'), 'lease_until': ('lease_until', 9000000000),
        'missing_observation': ('last_checked_at_utc', None), 'different_error': ('error_code', 'http_403')}
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}])
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock',error_code='invalid_bulk_publication_clock',"
            'attempts=4,due=9000000000,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        if change in columns:
            column, value = columns[change]
            queue.db.execute('UPDATE articles SET ' + column + '=? WHERE url=?', (value, url))
        else:
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('b' * 64, 'source_capture_fingerprint:tvbs'))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, registry(2))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('kind', ['translator', 'responsible_editor', 'negative_second'])
def test_native_v2_worker_receipt_persistence_and_independent_restart_replay(tmp_path, kind):
    soup = native.page()
    if kind == 'translator':
        credits(soup, role='編譯')
    else:
        credits(soup, secondary='編輯組')
        if kind == 'negative_second':
            skew(soup)
    policy = registry(2)
    raw = Response(native.URLS[0], 200, {'content-type': 'text/html'}, str(soup).encode())
    calls = []
    def get(url, **_kwargs):
        calls.append(url)
        return raw
    http = SimpleNamespace(registry=policy, get=get)
    with closing(NewsQueue(tmp_path, policy)) as queue:
        queue.add_articles([{'url': raw.url}])
        row = queue.claim_url(raw.url, 300)
        record = fetch_article(http, row)
        assert calls == [raw.url]
        assert record['raw_response_body'] == raw.body and not record['historical_point_in_time']
        queue.accept([record])
    with closing(NewsQueue(tmp_path, policy)) as queue:
        head = queue.db.execute('SELECT state,body_sha256 FROM articles WHERE url=?', (raw.url,)).fetchone()
        assert head[:] == ('complete', record['body_sha256'])
    audited = audit(tmp_path, [raw.url], registry=policy)
    assert audited['accepted'] and audited['articles'][0]['current_source_replay_verified']
