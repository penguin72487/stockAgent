"""Candidate work is independent of source observations and body acceptance."""
from dataclasses import replace
import hashlib
import sqlite3
import threading
import time

import pytest

from downloader import news_fulltext_cnyes_work as work
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as config


@pytest.fixture
def queue(config, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    yield value
    value.close()


def read(queue, url):
    row = queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()
    return dict(row) if row else None


AMP = 'https://amp-news.cnyes.com/news/id/123'
NATIVE = 'https://news.cnyes.com/news/id/123'


def add_legacy(queue, monkeypatch, url=AMP):
    with monkeypatch.context() as patch:
        patch.setattr(work, 'admit_native_lookup', lambda *args: False)
        with queue.db:
            queue.add_articles([{'url': url, 'title': 'AMP observation', 'date': '2026-10-10'}])
            queue.db.execute('UPDATE articles SET cnyes_canonical_candidate_id=NULL,cnyes_known_body_candidate=0 WHERE url=?', (url,))
    return read(queue, url)


def test_missing_native_admission_is_unverified_atomic_and_idempotent(queue, monkeypatch):
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.get', lambda *a, **k: pytest.fail('discovery made HTTP'))
    with queue.db:
        assert queue.add_articles([{'url': AMP, 'title': 'AMP title', 'date': '2026-10-10'}]) == (1, 0)
    parent, native = read(queue, AMP), read(queue, NATIVE)
    assert parent['state'] == native['state'] == 'pending'
    assert parent['title'] == 'AMP title' and parent['metadata_date_raw'] == '2026-10-10'
    assert native['title'] == native['metadata_date_raw'] == native['metadata_order_day'] == ''
    assert native['body_sha256'] is native['captured_at_utc'] is None
    journal = queue.db.execute('SELECT * FROM cnyes_native_lookup_candidates').fetchone()
    assert journal['basis'] == 'unverified_same_id_public_lookup_not_alias'
    assert journal['parent_article_id'] == parent['id'] and journal['candidate_article_id'] == native['id']
    assert journal['discovery_contract_version'] == work.LOOKUP_DISCOVERY_VERSION
    assert journal['authorization_fingerprint'] == queue.registry.sources['cnyes'].authorization_fingerprint
    assert queue.db.execute('SELECT count(*) FROM source_links').fetchone()[0] == 0
    with queue.db:
        queue.add_articles([{'url': AMP}])
    assert read(queue, NATIVE) == native
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_candidates').fetchone()[0] == 1


@pytest.mark.parametrize('suffix', ['?exp=a', '?token=x', '#x', '/', 'x'])
def test_unsupported_amp_view_is_not_a_native_lookup(suffix):
    assert work.native_lookup_url(AMP + suffix) is None
    assert work.canonical_candidate_id(AMP + suffix) is None


@pytest.mark.parametrize('url', ['http://amp-news.cnyes.com/news/id/123', 'https://amp-news.cnyes.com:443/news/id/123',
                              'https://u:p@amp-news.cnyes.com/news/id/123', 'https://amp-news.cnyes.com.evil.test/news/id/123',
                              'https://amp-news.cnyes.com/news/id/0123', 'https://amp-news.cnyes.com/news/id/0'])
def test_lookup_namespace_has_exact_origin_and_path(url):
    assert work.native_lookup_url(url) is None


@pytest.mark.parametrize('state,error,body', [('retry', 'http_403', None), ('retry', 'http_429', None),
    ('retry', 'source_access_denied', None), ('retry', 'paywall', None), ('retry', 'worker_ValueError', None),
    ('inflight', None, None), ('awaiting_authorization', None, None), ('waiting_cohort', None, None),
    ('complete', None, 'a' * 64), ('retry', None, 'a' * 64)])
def test_no_route_discovery_from_refusals_leases_or_retained_bodies(queue, monkeypatch, state, error, body):
    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=?,body_sha256=?,due=?,lease_owner=? WHERE id=?',
                         (state, error, body, time.time() + 3600, 'original', row['id']))
        before = read(queue, AMP)
        assert not work.admit_native_lookup(queue, before)
    assert read(queue, AMP) == before and read(queue, NATIVE) is None


@pytest.mark.parametrize('state', ['pending', 'retry', 'complete', 'inflight', 'awaiting_authorization', 'source_access_denied'])
def test_existing_native_target_is_never_changed_or_requeued(queue, state):
    with queue.db:
        queue.add_articles([{'url': NATIVE, 'title': 'native observation', 'date': '2014-01-01'}])
        queue.db.execute('UPDATE articles SET state=?,due=?,lease_owner=?,error_code=? WHERE url=?',
                         (state, time.time() + 3600, 'other', 'source_access_denied', NATIVE))
        before = read(queue, NATIVE)
        queue.add_articles([{'url': AMP}])
    assert read(queue, NATIVE) == before
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_candidates').fetchone()[0] == 0


@pytest.mark.parametrize('change', ['disabled', 'expired', 'finite', 'different_rights', 'different_provider', 'excluded_path'])
def test_discovery_cannot_expand_source_authorization(queue, monkeypatch, change):
    row = add_legacy(queue, monkeypatch)
    source = queue.registry.sources['cnyes']
    if change == 'disabled':
        queue.registry.sources['cnyes'] = replace(source, allowed=False)
    elif change == 'expired':
        queue.registry.sources['cnyes'] = replace(source, authorization={**source.authorization, 'valid_until': '2000-01-01T00:00:00+00:00'})
    elif change == 'finite':
        queue.registry.sources['cnyes'] = replace(source, acquisition_mode='finite')
    elif change == 'excluded_path':
        queue.registry.sources['cnyes'] = replace(source, article_path_pattern=r'^/other/')
    else:
        original = queue.registry.require
        other = replace(source, **({'authorization_fingerprint': 'different'} if change == 'different_rights' else {'provider': 'other'}))
        monkeypatch.setattr(queue.registry, 'require', lambda url: other if url == NATIVE else original(url))
    with queue.db:
        assert not work.admit_native_lookup(queue, row)
    assert read(queue, AMP) == row and read(queue, NATIVE) is None


def test_candidate_and_provenance_roll_back_together(queue):
    queue.db.execute("CREATE TEMP TRIGGER reject_lookup BEFORE INSERT ON cnyes_native_lookup_candidates "
                     "BEGIN SELECT RAISE(ABORT,'fixture journal unavailable'); END")
    with pytest.raises(sqlite3.IntegrityError, match='journal unavailable'), queue.db:
        queue.add_articles([{'url': AMP}])
    assert read(queue, AMP) is read(queue, NATIVE) is None
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_candidates').fetchone()[0] == 0


def test_amp_backfill_resumes_without_touching_parent_errors_or_http(queue, monkeypatch, tmp_path):
    for number in (300, 301, 302):
        add_legacy(queue, monkeypatch, AMP.rsplit('/', 1)[0] + '/' + str(number))
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry_cooldown',due=?,error_code='host_gateway_cooldown' WHERE host='amp-news.cnyes.com'", (time.time()+3600,))
    before = [dict(r) for r in queue.db.execute("SELECT * FROM articles WHERE host='amp-news.cnyes.com' ORDER BY id")]
    result = work.backfill(queue, limit=2)
    assert result['checked_heads'] == result['native_lookup_candidates_added'] == 2
    assert result['publisher_requests'] == 0 and not result['source_observations_merged']
    stopped = threading.Event()
    stopped.set()
    assert work.backfill(queue, stop=stopped)['checked_heads'] == 0
    queue.close()
    reopened = NewsQueue(tmp_path, queue.registry)
    try:
        assert work.backfill(reopened)['native_lookup_candidates_added'] == 1
        assert work.backfill(reopened)['checked_heads'] == 0
        after = [dict(r) for r in reopened.db.execute("SELECT * FROM articles WHERE host='amp-news.cnyes.com' ORDER BY id")]
        for left, right in zip(before, after):
            assert all(left[key] == right[key] for key in left if not key.startswith('cnyes_'))
        plan = [r[3] for r in reopened.db.execute('EXPLAIN QUERY PLAN SELECT rowid,id FROM articles INDEXED BY host_state '
            'WHERE host=? AND state=? AND rowid>? ORDER BY rowid LIMIT ?', ('amp-news.cnyes.com', 'pending', 0, 2))]
        assert any('SEARCH articles USING INDEX host_state' in p for p in plan)
        assert not any('SCAN articles' in p or 'TEMP B-TREE' in p for p in plan)
    finally:
        reopened.close()


def test_amp_with_known_desktop_body_is_only_a_lower_priority_hint(queue):
    with queue.db:
        queue.add_articles([{'url': NATIVE}])
        queue.db.execute('UPDATE articles SET body_sha256=?,state=\'complete\' WHERE url=?', ('a'*64, NATIVE))
        queue.add_articles([{'url': AMP, 'date': '2026-10-10'}])
        queue.add_articles([{'url': f'https://news.cnyes.com/news/id/{i}', 'date': '2014-01-01'} for i in range(1, 17)])
    parent = read(queue, AMP)
    assert parent['body_sha256'] is None and parent['state'] == 'pending' and parent['cnyes_known_body_candidate'] == 1
    assert work.canonical_candidate_id(AMP) == hashlib.sha256(NATIVE.encode()).digest()
    assert [queue.claim('cnyes', 300)['cnyes_known_body_candidate'] for _ in range(9)] == [0]*8+[1]
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_candidates').fetchone()[0] == 0


def test_native_body_is_fetched_and_accepted_independently_of_failed_amp(queue, config, tmp_path):
    from downloader.news_fulltext import fetch_article
    from downloader.news_fulltext_queue import verify_bodies
    from test_news_fulltext import FakeResponse, http_fixture
    from test_news_cnyes_html import short_page, URL

    parent_url = URL.replace('news.cnyes.com', 'amp-news.cnyes.com')
    with queue.db:
        queue.add_articles([{'url': parent_url, 'title': 'unverified AMP metadata', 'date': '2026-10-10'}])
        queue.db.execute('UPDATE articles SET state=\'retry\',error_code=\'http_502\',due=? WHERE url=?',
                         (time.time() + 300, parent_url))
    before = read(queue, parent_url)
    http, session = http_fixture(config, tmp_path, {
        'https://news.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nDisallow:\n'),
        URL: FakeResponse(short_page())})
    try:
        row = queue.claim_url(URL, 300)
        record = fetch_article(http, row)
        queue.accept([record])
        assert record['requested_url'] == record['final_url'] == record['source_request_url'] == URL
        assert record['metadata_date_raw'] == '' and record['published_at_raw'] == '2014-01-01T00:00:00.000Z'
        assert verify_bodies(tmp_path)['accepted']
        native, after = read(queue, URL), read(queue, parent_url)
        assert native['state'] == 'complete' and native['body_sha256'] and native['metadata_order_day'] == '2014-01-01'
        assert after['cnyes_known_body_candidate'] == 1 and after['body_sha256'] is None
        assert all(before[key] == after[key] for key in before if key != 'cnyes_known_body_candidate')
        assert [url for url, _ in session.calls] == ['https://news.cnyes.com/robots.txt', URL]
    finally:
        http.close()


def test_audit_requires_loaded_work_version_and_labels_pending_lookups(queue):
    from scripts.audit_news_cnyes_work_dispatch import audit
    with queue.db:
        queue.add_articles([{'url': AMP}])
    queue.claim('cnyes', 300)
    queue.status(activity='running')
    result = audit(queue.root, repetitions=1)
    assert result['accepted'] and result['loaded_work_contracts_match']
    assert result['lookup_targets_require_independent_source_acceptance']
    assert result['native_lookup_work_by_state'] == [{'state': 'pending', 'candidates': 1,
        'stored_native_heads': 0, 'nonempty_raw_metadata_dates': 0}]
    with queue.db:
        queue.db.execute("UPDATE queue_settings SET value='2' WHERE key='cnyes_body_value_dispatch_contract'")
    result = audit(queue.root, repetitions=1)
    assert not result['accepted'] and not result['loaded_work_contracts_match']
    assert result['native_lookup_work_by_state'] == []
