"""Candidate work is independent of source observations and body acceptance."""
from dataclasses import replace
import hashlib
from itertools import product
import re
import sqlite3
import threading
import time
from urllib.parse import urlsplit

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
    assert work.canonical_candidate_id(url) is None


def test_candidate_hint_preserves_url_admission_across_views():
    # Reference the original admission rules independently of the candidate
    # implementation: AMP lookup and observed alias shapes are distinct.
    from downloader.news_fulltext_publishers import cnyes_supported_native_alias_view

    schemes = ('https', 'http', 'HTTPS')
    hosts = ('amp-news.cnyes.com', 'm.cnyes.com', 'news.cnyes.com',
             'amp-news.cnyes.com:443', 'm.cnyes.com.evil.test', 'M.CNYES.COM')
    paths = ('/news/id/1', '/news/id/0', '/news/id/0123', '/news/id/123',
             '/news/id/١٢٣', '/news/id/%31', '/news/id/123/', '/news/print/123')
    suffixes = ('', '?', '?exp=a', '?exp=b', '?exp=%61', '?defer_action=auth',
                '?amp;utm_medium=RSS&amp;utm_campaign=relate', '?token=x', '#x', '?exp=a#x')
    for scheme, host, path, suffix in product(schemes, hosts, paths, suffixes):
        url = f'{scheme}://{host}{path}{suffix}'
        lookup = work.native_lookup_url(url)
        if lookup is not None:
            expected = hashlib.sha256(lookup.encode()).digest()
        else:
            parts = urlsplit(url)
            match = re.fullmatch(r'/news/id/([0-9]+)', parts.path)
            expected = (hashlib.sha256(('https://news.cnyes.com/news/id/' + match[1]).encode()).digest()
                        if match and cnyes_supported_native_alias_view(parts) else None)
        assert work.canonical_candidate_id(url) == expected, url


@pytest.mark.parametrize('state,error,body', [('retry', 'http_403', None), ('retry', 'http_429', None),
    ('retry', 'source_access_denied', None), ('retry', 'paywall', None), ('retry', 'worker_ValueError', None),
    ('inflight', None, None), ('awaiting_authorization', None, None), ('waiting_cohort', None, None),
    ('complete', None, 'a' * 64), ('retry', None, 'a' * 64), ('retry', None, '')])
def test_no_route_discovery_from_refusals_leases_or_retained_bodies(queue, monkeypatch, state, error, body):
    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute('UPDATE articles SET state=?,error_code=?,body_sha256=?,due=?,lease_owner=? WHERE id=?',
                         (state, error, body, time.time() + 3600, 'original', row['id']))
        before = read(queue, AMP)
        assert not work.admit_native_lookup(queue, before)
    assert read(queue, AMP) == before and read(queue, NATIVE) is None


@pytest.mark.parametrize('provider', ['unregistered:amp-news.cnyes.com', 'other'])
def test_reimport_does_not_make_native_lookup_for_a_differently_owned_parent(queue, monkeypatch, provider):
    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute('UPDATE articles SET provider=? WHERE id=?', (provider, row['id']))
        before = read(queue, AMP)
        # The registry maps this input to CNYES, but the ON CONFLICT import
        # deliberately leaves an existing head's provider unchanged.
        queue.add_articles([{'url': AMP}])
    after = read(queue, AMP)
    assert after['provider'] == provider
    assert all(after[key] == before[key] for key in before if key != 'last_seen_utc')
    assert read(queue, NATIVE) is None
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_candidates').fetchone()[0] == 0
    # Once the canonical owner reconciles the actual parent provider, a
    # later ordinary import may register the lookup with normal provenance.
    with queue.db:
        queue.db.execute("UPDATE articles SET provider='cnyes' WHERE id=?", (row['id'],))
        queue.add_articles([{'url': AMP}])
    assert read(queue, NATIVE)['provider'] == 'cnyes'
    journal = queue.db.execute('SELECT parent_article_id,parent_url FROM cnyes_native_lookup_candidates').fetchone()
    assert journal[:] == (row['id'], AMP)


def test_stale_parent_url_cannot_admit_native_lookup(queue, monkeypatch):
    row = add_legacy(queue, monkeypatch)
    changed = AMP + '?view=changed'
    with queue.db:
        queue.db.execute('UPDATE articles SET url=? WHERE id=?', (changed, row['id']))
        before = read(queue, changed)
        assert not work.admit_native_lookup(queue, row)
    assert read(queue, changed) == before and read(queue, NATIVE) is None
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_candidates').fetchone()[0] == 0


def test_backfill_does_not_admit_lookup_after_parent_provider_changes(queue, monkeypatch):
    row = add_legacy(queue, monkeypatch)
    original = work.classify

    def reattribute_before_classifying(owner, candidate, **options):
        owner.db.execute("UPDATE articles SET provider='unregistered:amp-news.cnyes.com' WHERE id=?",
                         (candidate['id'],))
        return original(owner, candidate, **options)

    monkeypatch.setattr(work, 'classify', reattribute_before_classifying)
    result = work.backfill(queue, limit=1)
    assert result['checked_heads'] == 1 and result['native_lookup_candidates_added'] == 0
    assert read(queue, AMP)['provider'] == 'unregistered:amp-news.cnyes.com'
    assert read(queue, NATIVE) is None
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_candidates').fetchone()[0] == 0


def test_backfill_revisits_amp_after_canonical_provider_reconciliation(queue, monkeypatch):
    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute("UPDATE articles SET provider='unregistered:amp-news.cnyes.com' WHERE id=?", (row['id'],))
        queue.db.execute("UPDATE hosts SET provider='unregistered:amp-news.cnyes.com' WHERE host='amp-news.cnyes.com'")
        queue.db.execute("DELETE FROM queue_settings WHERE key='policy_fingerprint'")
    assert work.backfill(queue, limit=1)['checked_heads'] == 1
    assert read(queue, NATIVE) is None

    # The registry now assigns this existing host to CNYES. The normal owner
    # reconciles it without importing the URL again.
    queue.reconcile_policy()
    parent = read(queue, AMP)
    assert parent['provider'] == 'cnyes' and parent['state'] == 'pending'
    for _ in range(20):
        work.backfill(queue, limit=1)
        if read(queue, NATIVE) is not None:
            break
    assert read(queue, NATIVE)['provider'] == 'cnyes'
    assert all(read(queue, AMP)[key] == value for key, value in parent.items()
               if not key.startswith('cnyes_'))
    journal = queue.db.execute('SELECT parent_article_id,parent_url FROM cnyes_native_lookup_candidates').fetchone()
    assert journal[:] == (row['id'], AMP)


def test_backfill_revisits_amp_after_a_real_retry_outcome(queue, monkeypatch):
    from downloader.news_fulltext_policy import NewsError

    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry',error_code='source_access_denied',due=0 WHERE id=?", (row['id'],))
    for _ in range(12):
        if work.backfill(queue, limit=1)['checked_heads']:
            break
    assert read(queue, NATIVE) is None
    claimed = queue.claim('cnyes', 300)
    assert claimed['id'] == row['id']
    assert queue.failed(claimed, NewsError('http_502', retry=True, delay=5), 3)
    parent = read(queue, AMP)
    assert parent['state'] == 'retry' and parent['error_code'] == 'http_502'
    for _ in range(20):
        work.backfill(queue, limit=1)
        if read(queue, NATIVE) is not None:
            break
    assert read(queue, NATIVE)['provider'] == 'cnyes'
    assert all(read(queue, AMP)[key] == value for key, value in parent.items()
               if not key.startswith('cnyes_'))


def test_recheck_retains_refusals_and_rolls_back_with_candidate(queue, monkeypatch, tmp_path):
    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry',error_code='http_403',due=123 WHERE id=?", (row['id'],))
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_rechecks').fetchone()[0] == 0
    with queue.db:
        queue.db.execute("UPDATE articles SET error_code='http_502' WHERE id=?", (row['id'],))
        queue.db.execute("UPDATE articles SET error_code='http_403' WHERE id=?", (row['id'],))
    before = read(queue, AMP)
    queue._cnyes_lookup_rechecks_first = True
    assert work.backfill(queue, limit=1)['rechecked_heads'] == 1
    assert all(read(queue, AMP)[key] == value for key, value in before.items()
               if not key.startswith('cnyes_'))
    assert read(queue, NATIVE) is None
    with queue.db:
        queue.db.execute("UPDATE articles SET error_code='http_502' WHERE id=?", (row['id'],))
        queue.db.execute("CREATE TEMP TRIGGER reject_lookup BEFORE INSERT ON cnyes_native_lookup_candidates "
                         "BEGIN SELECT RAISE(ABORT,'fixture journal unavailable'); END")
    queue._cnyes_lookup_rechecks_first = True
    with pytest.raises(sqlite3.IntegrityError, match='journal unavailable'):
        work.backfill(queue, limit=1)
    assert queue.db.execute('SELECT count(*) FROM cnyes_native_lookup_rechecks').fetchone()[0] == 1
    assert read(queue, NATIVE) is None
    with queue.db:
        queue.db.execute('DROP TRIGGER reject_lookup')
    registry = queue.registry
    queue.close()
    reopened = NewsQueue(tmp_path, registry)
    try:
        reopened._cnyes_lookup_rechecks_first = True
        assert work.backfill(reopened, limit=1)['native_lookup_candidates_added'] == 1
        assert reopened.db.execute('SELECT count(*) FROM cnyes_native_lookup_rechecks').fetchone()[0] == 0
        assert read(reopened, AMP)['error_code'] == 'http_502'
    finally:
        reopened.close()


def test_native_lookup_recheck_status_tracks_transactional_journal_without_http(queue, monkeypatch):
    from stockagent.live.news_dashboard import build_news_public_status
    from test_news_dashboard import NOW, _save, _seed

    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute("UPDATE articles SET state='retry',error_code='http_502' WHERE id=?", (row['id'],))
    sample = queue.status(activity='running')['providers']['cnyes']['native_lookup_recheck_progress']
    assert sample == {'contract_version': 1, 'pending_records_lower_bound': 1,
                      'count_exact': True, 'records_are_article_completion': False,
                      'source_lookup_performed': False}
    status = _seed(queue.root / 'public_fixture')
    status['providers']['cnyes']['native_lookup_recheck_progress'] = sample
    _save(queue.root / 'public_fixture', status)
    projected = build_news_public_status(queue.root / 'public_fixture', now=NOW)
    assert next(p for p in projected['providers'] if p['id'] == 'cnyes')['native_lookup_recheck_progress'] == sample
    with pytest.raises(RuntimeError, match='rollback'):
        with queue.db:
            queue.db.execute('DELETE FROM cnyes_native_lookup_rechecks')
            raise RuntimeError('rollback')
    assert queue.status(activity='running')['providers']['cnyes']['native_lookup_recheck_progress'] == sample
    queue._cnyes_lookup_rechecks_first = True
    assert work.backfill(queue, limit=1)['rechecked_heads'] == 1
    assert queue.status(activity='running')['providers']['cnyes']['native_lookup_recheck_progress']['pending_records_lower_bound'] == 0


def test_native_lookup_recheck_status_has_a_bounded_read(queue):
    with queue.db:
        queue.db.executemany('INSERT INTO cnyes_native_lookup_rechecks(parent_article_id) VALUES (?)',
                             ((str(i).encode(),) for i in range(2050)))
    queries = []
    queue.db.set_trace_callback(queries.append)
    sample = work.lookup_recheck_progress(queue.db)
    queue.db.set_trace_callback(None)
    assert sample['pending_records_lower_bound'] == 2049 and sample['count_exact'] is False
    assert len(queries) == 1 and 'LIMIT 2049' in queries[0] and 'articles' not in queries[0]
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_native_lookup_rechecks WHERE seq > 2048')
    sample = work.lookup_recheck_progress(queue.db)
    assert sample['pending_records_lower_bound'] == 2048 and sample['count_exact'] is True


def test_changed_amp_rechecks_and_legacy_scans_both_make_progress(queue, monkeypatch):
    for number in range(200, 208):
        row = add_legacy(queue, monkeypatch, AMP.rsplit('/', 1)[0] + '/' + str(number))
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_403' WHERE id=?", (row['id'],))
    for _ in range(12):
        work.backfill(queue)
    with queue.db:
        queue.db.execute("UPDATE articles SET error_code='http_502' WHERE host='amp-news.cnyes.com'")
        queue.add_articles([{'url': f'https://m.cnyes.com/news/id/{number}'} for number in range(300, 308)])
    results = [work.backfill(queue, limit=1) for _ in range(8)]
    assert sum(result['rechecked_heads'] for result in results) >= 3
    assert sum(result['checked_heads'] - result['rechecked_heads'] for result in results) >= 3
    assert all(result['publisher_requests'] == 0 for result in results)


def test_changed_source_contract_revisits_a_previously_skipped_amp(queue, monkeypatch):
    source = queue.registry.sources['cnyes']
    queue.registry.sources['cnyes'] = replace(source, acquisition_mode='finite', data_fingerprint='finite-before')
    queue.reconcile_policy()
    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute("UPDATE articles SET state='pending',error_code=NULL WHERE id=?", (row['id'],))
    for _ in range(12):
        work.backfill(queue, limit=1)
    assert read(queue, NATIVE) is None

    queue.registry.sources['cnyes'] = replace(source, data_fingerprint='continuous-after')
    queue.reconcile_policy()
    for _ in range(20):
        work.backfill(queue, limit=1)
        if read(queue, NATIVE) is not None:
            break
    assert read(queue, NATIVE)['provider'] == 'cnyes'


def test_recheck_upgrade_replays_legacy_cursor_once(queue, monkeypatch, tmp_path):
    row = add_legacy(queue, monkeypatch)
    with queue.db:
        queue.db.execute("UPDATE articles SET provider='other' WHERE id=?", (row['id'],))
    assert work.backfill(queue, limit=1)['checked_heads'] == 1
    with queue.db:
        queue.db.execute('DROP TRIGGER cnyes_native_lookup_recheck_update_v1')
        queue.db.execute("UPDATE articles SET provider='cnyes' WHERE id=?", (row['id'],))
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_native_lookup_recheck_contract'")
    registry = queue.registry
    queue.close()
    reopened = NewsQueue(tmp_path, registry)
    try:
        assert work.backfill(reopened, limit=1)['native_lookup_candidates_added'] == 1
        saved = reopened.db.execute('SELECT value FROM queue_settings WHERE key=?', (work.AMP_CURSOR,)).fetchone()[0]
        reopened.close()
        reopened = NewsQueue(tmp_path, registry)
        assert reopened.db.execute('SELECT value FROM queue_settings WHERE key=?', (work.AMP_CURSOR,)).fetchone()[0] == saved
        assert work.backfill(reopened)['native_lookup_candidates_added'] == 0
    finally:
        reopened.close()


def test_owner_drains_a_recheck_created_after_its_last_slice(config, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture

    config['adaptive_rate'] = {'enabled': False}
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(history, 'backfill', lambda *a, **k: {'exhausted': True, 'next_local_retry_epoch': None})
    try:
        row = add_legacy(queue, monkeypatch)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_403',due=? WHERE id=?",
                             (time.time() + 3600, row['id']))
        for _ in range(12):
            work.backfill(queue)
        calls, real_backfill = [], work.backfill

        def changed_after_slice(*args, **kwargs):
            result = real_backfill(*args, **kwargs)
            calls.append(result)
            if len(calls) == 1:
                with queue.db:
                    queue.db.execute("UPDATE articles SET error_code='http_502' WHERE id=?", (row['id'],))
            elif read(queue, NATIVE) is not None:
                http.stop.set()
            return result

        monkeypatch.setattr(work, 'backfill', changed_after_slice)
        owner.run_cycle(queue, http, config, max_articles=0, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert len(calls) >= 2 and read(queue, NATIVE)['provider'] == 'cnyes'
        assert session.calls == []
        assert read(queue, AMP)['due'] > time.time()
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize(('maintenance_delay', 'journaled'), [(0, True), (3600, True), (0, False)],
                         ids=['due-slice', 'journal-wake', 'due-scan-without-journal'])
def test_owner_commits_received_body_before_continuing_cnyes_lookup_work(config, tmp_path, monkeypatch,
                                                                       maintenance_delay, journaled):
    from concurrent.futures import Future
    from downloader import news_fulltext as owner, news_fulltext_cnyes_history as history
    from downloader.news_fulltext_queue import verify_bodies
    from test_news_fulltext import FakeResponse, article_html, config as publisher_config, http_fixture

    class ImmediateExecutor:
        def submit(self, fn, *args, **kwargs):
            result = Future()
            try:
                result.set_result(fn(*args, **kwargs))
            except Exception as error:
                result.set_exception(error)
            return result

    config['providers'].append(publisher_config.__wrapped__()['providers'][0])
    config.update(adaptive_rate={'enabled': False}, batch_articles=64, batch_text_bytes=4 << 20,
                  batch_raw_response_bytes=16 << 20, batch_flush_seconds=3600,
                  cached_cnyes_replay_enabled=False, cached_cnyes_metadata_replay_enabled=False,
                  retained_cnyes_failure_replay_enabled=False)
    url = 'https://publisher.test/a'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        url: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(history, 'backfill', lambda *a, **k: {'exhausted': True, 'next_local_retry_epoch': None})
    monkeypatch.setattr(work, 'admit_native_lookup', lambda *a: False)
    # Exercise both an already-due slice and a durable recheck wake. Neither
    # must defer a received body while continuing local-only work.
    monkeypatch.setattr(work, 'maintenance_delay', lambda _: maintenance_delay)
    try:
        with queue.db:
            queue.add_articles([{'url': url}])
        if journaled:
            for article_id in (200, 201, 202):
                row = add_legacy(queue, monkeypatch, url=f'https://amp-news.cnyes.com/news/id/{article_id}')
                with queue.db:
                    queue.db.execute("UPDATE articles SET state='retry',error_code='http_502',due=? WHERE id=?",
                                     (time.time() + 3600, row['id']))
        queue._cnyes_lookup_rechecks_first = True
        calls, observed, real_backfill = [], [], work.backfill

        def single_lookup_slice(*args, **kwargs):
            if calls:
                observed.append((read(queue, url), queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0]))
                http.stop.set()
            result = real_backfill(*args, limit=1, **kwargs)
            calls.append(result)
            return result

        monkeypatch.setattr(work, 'backfill', single_lookup_slice)
        owner.run_cycle(queue, http, config, max_articles=1, max_discovery_requests=0,
                        executor=ImmediateExecutor(), continuous_budgets=True,
                        max_run_seconds=5, defer_final_status=True)
        assert len(calls) >= 2
        if journaled:
            assert calls[0]['rechecked_heads'] == 1
        else:
            assert calls[0]['checked_heads'] == 0 and not calls[0]['seek_continuation']
        assert [entry[0] for entry in session.calls] == ['https://publisher.test/robots.txt', url]
        head, accepted_batches = observed[0]
        assert head['state'] == 'complete' and head['body_sha256']
        assert accepted_batches == 1 and verify_bodies(tmp_path)['accepted']
    finally:
        http.close()
        queue.close()


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
