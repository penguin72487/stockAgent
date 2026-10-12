"""Pinned native parser repair, not a blanket retry/refusal reset."""
import json

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_publishers import (
    CNYES_MOBILE_CANONICAL_V1_CAPTURE_FINGERPRINT,
    CNYES_MOBILE_CANONICAL_V2_CAPTURE_FINGERPRINT,
    CNYES_MOBILE_CANONICAL_V3_CAPTURE_FINGERPRINT,
    CNYES_MOBILE_CANONICAL_V4_CAPTURE_FINGERPRINT,
    CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT,
    CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT,
    CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT,
)
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as cnyes_config


PINNED_CONTRACTS = [CNYES_MOBILE_CANONICAL_V1_CAPTURE_FINGERPRINT, CNYES_MOBILE_CANONICAL_V2_CAPTURE_FINGERPRINT]


@pytest.mark.parametrize('previous', [CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT, 'unknown-title-contract'])
def test_v7_title_recheck_is_pinned_once_only_and_preserves_refusals_and_all_evidence(cnyes_config, tmp_path, previous):
    queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
    eligible = ['https://m.cnyes.com/news/id/1', 'https://m.cnyes.com/news/id/2?exp=b',
                'https://news.cnyes.com/news/id/3?exp=a']
    protected = ['https://news.cnyes.com/news/id/4', 'https://news.cnyes.com/news/id/5?exp=b',
                 'https://m.cnyes.com/news/id/6?token=x', *[f'https://m.cnyes.com/news/id/{i}' for i in range(7, 15)]]
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in eligible + protected])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',attempts=4,"
                             "last_checked_at_utc='2026-10-10T08:12:40+00:00'")
            queue.db.execute("UPDATE articles SET body_sha256='retained' WHERE url=?", (protected[3],))
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=9999999999 WHERE url=?", (protected[4],))
            queue.db.execute("UPDATE articles SET state='retry',error_code='source_access_denied',due=9999999999 WHERE url=?", (protected[5],))
            queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=9999999999 WHERE url=?", (protected[6],))
            queue.db.execute("UPDATE articles SET last_checked_at_utc=NULL WHERE url=?", (protected[7],))
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found' WHERE url=?", (protected[8],))
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login' WHERE url=?", (protected[9],))
            queue.db.execute("UPDATE articles SET state='source_article_empty_template',error_code='source_article_empty_template' WHERE url=?", (protected[10],))
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', ('source_capture_fingerprint:cnyes', previous))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        queue.reconcile_policy()
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        for url in eligible:
            assert after[url] == ({**before[url], 'state': 'retry', 'due': 0}
                                 if previous == CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT else before[url])
        for url in protected:
            assert after[url] == before[url]
        if previous == CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT:
            marker = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'cnyes_native_v7_title_repair_v1:%'").fetchone()[0])
            assert marker['url_count'] == len(eligible) and not marker['accepted_as_bodies']
            assert marker['attempts_errors_and_last_http_checks_preserved'] and not marker['source_refusals_and_stored_heads_reset']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',attempts=5 WHERE url=?", (eligible[0],))
        queue.recheck_cnyes_native_v7_titles(previous)
        assert queue.db.execute('SELECT state,attempts FROM articles WHERE url=?', (eligible[0],)).fetchone()[0] == 'article_identity_not_verified'
    finally:
        queue.close()


@pytest.mark.parametrize('invalid', ['', 'missing_legacy', 'wrong_legacy', 'wrong_current', 'missing_current',
                                    'naive_clock', 'future_clock', 'refusal_reset', 'dispatch_version'])
def test_missed_v7_rollout_requires_both_durable_proofs_and_pre_rollout_failure_clocks(cnyes_config, tmp_path, monkeypatch, invalid):
    from downloader import news_fulltext_publishers as publisher
    from downloader.news_fulltext_queue import ARTICLE_DISPATCH_CONTRACT_VERSION

    queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
    current_fp = queue.registry.sources['cnyes'].data_fingerprint
    monkeypatch.setattr(publisher, 'CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT', current_fp)
    urls = [f'https://m.cnyes.com/news/id/{i}?exp=b' for i in range(1, 5)]
    legacy = {'old_source_fingerprint': CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT,
              'new_source_fingerprint': CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT,
              'stored_heads_or_source_refusals_reset': False, 'accepted_as_bodies': False,
              'observed_at_utc': '2026-10-10T02:01:30+00:00'}
    current = {'source_fingerprint': current_fp, 'dispatch_contract_version': ARTICLE_DISPATCH_CONTRACT_VERSION,
               'observed_at_utc': '2026-10-10T06:03:35+00:00',
               'original_error_clock_attempts_due_lease_and_body_preserved': True,
               'http_refusals_or_unobserved_first_downloads_changed': False}
    if invalid == 'wrong_legacy':
        legacy['new_source_fingerprint'] = 'unknown'
    if invalid == 'wrong_current':
        current['source_fingerprint'] = 'unknown'
    if invalid == 'naive_clock':
        current['observed_at_utc'] = '2026-10-10T06:03:35'
    if invalid == 'future_clock':
        current['observed_at_utc'] = '2099-10-10T06:03:35+00:00'
    if invalid == 'refusal_reset':
        current['http_refusals_or_unobserved_first_downloads_changed'] = True
    if invalid == 'dispatch_version':
        current['dispatch_contract_version'] = -1
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                             "attempts=4,last_checked_at_utc='2026-10-10T04:57:22+00:00'")
            queue.db.execute("UPDATE articles SET last_checked_at_utc='2026-10-10T06:03:36+00:00' WHERE url=?", (urls[1],))
            queue.db.execute("UPDATE articles SET last_checked_at_utc=NULL WHERE url=?", (urls[2],))
            queue.db.execute("UPDATE articles SET state='retry_cooldown',error_code='http_403',due=9999999999 WHERE url=?", (urls[3],))
            if invalid != 'missing_legacy':
                queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (
                    'cnyes_mobile_query_view_repair_v1:' + CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT, json.dumps(legacy)))
            if invalid == 'missing_current':
                queue.db.execute('DELETE FROM queue_settings WHERE key=?', (
                    'parser_repair_retry_lane_v1:cnyes:' + current_fp + ':' + str(ARTICLE_DISPATCH_CONTRACT_VERSION),))
            else:
                queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (
                    'parser_repair_retry_lane_v1:cnyes:' + current_fp + ':' + str(ARTICLE_DISPATCH_CONTRACT_VERSION), json.dumps(current)))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        queue.recheck_cnyes_native_v6_views(current_fp)
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after[urls[0]] == ({**before[urls[0]], 'state': 'retry', 'due': 0} if not invalid else before[urls[0]])
        for url in urls[1:]:
            assert after[url] == before[url]
        if not invalid:
            marker = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'cnyes_native_v6_view_repair_v1:%'").fetchone()[0])
            assert marker['url_count'] == 1 and marker['recovered_missed_same_contract_rollout']
            with queue.db:
                queue.db.execute("UPDATE articles SET state='article_identity_not_verified',attempts=5 WHERE url=?", (urls[0],))
            queue.recheck_cnyes_native_v6_views(current_fp)
            head = queue.db.execute('SELECT state,attempts FROM articles WHERE url=?', (urls[0],)).fetchone()
            assert tuple(head) == ('article_identity_not_verified', 5)
    finally:
        queue.close()


@pytest.mark.parametrize('previous', [CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT, 'unknown-native-contract'])
def test_v6_native_repair_is_once_only_with_original_source_outcomes_preserved(cnyes_config, tmp_path, previous):
    queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
    eligible = ['https://m.cnyes.com/news/id/1', 'https://m.cnyes.com/news/id/2?exp=b',
                'https://m.cnyes.com/news/id/3?exp=a', 'https://news.cnyes.com/news/id/4?exp=a']
    protected = ['https://news.cnyes.com/news/id/5', 'https://news.cnyes.com/news/id/6?exp=b',
                 'https://m.cnyes.com/news/id/7?exp=b&token=x',
                 *[f'https://m.cnyes.com/news/id/{i}' for i in range(8, 15)]]
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in eligible + protected])
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',"
                             "attempts=4,last_checked_at_utc='2026-10-10T05:47:41+00:00'")
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified' WHERE url=?", (eligible[1],))
            queue.db.execute("UPDATE articles SET body_sha256='retained' WHERE url=?", (protected[3],))
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login' WHERE url=?", (protected[4],))
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=9999999999 WHERE url=?", (protected[5],))
            queue.db.execute("UPDATE articles SET state='source_access_denied',error_code='source_access_denied' WHERE url=?", (protected[6],))
            queue.db.execute("UPDATE articles SET lease_owner='other',lease_until=9999999999 WHERE url=?", (protected[7],))
            queue.db.execute("UPDATE articles SET state='source_article_empty_template',error_code='source_article_empty_template' WHERE url=?", (protected[8],))
            queue.db.execute("UPDATE articles SET last_checked_at_utc=NULL WHERE url=?", (protected[9],))
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', ('source_capture_fingerprint:cnyes', previous))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        queue.reconcile_policy()
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        known = previous == CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT
        for url in eligible:
            assert after[url] == ({**before[url], 'state': 'retry', 'due': 0} if known else before[url])
        for url in protected:
            assert after[url] == before[url]
        if known:
            marker = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'cnyes_native_v6_view_repair_v1:%'").fetchone()[0])
            assert marker['url_count'] == 4 and not marker['accepted_as_bodies']
            assert marker['attempts_errors_and_last_http_checks_preserved']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',attempts=5 WHERE url=?", (eligible[0],))
        queue.close()
        queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
        head = queue.db.execute('SELECT state,attempts,error_code FROM articles WHERE url=?', (eligible[0],)).fetchone()
        assert tuple(head) == ('body_not_found', 5, 'body_not_found')
    finally:
        queue.close()


@pytest.mark.parametrize('old_contract', [*PINNED_CONTRACTS, 'unreviewed-old-parser'])
def test_only_pinned_missing_mobile_identity_failure_is_requeued_once(cnyes_config, tmp_path, old_contract):
    queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
    mobile = [f'https://m.cnyes.com/news/id/{i}' for i in range(1, 7)]
    original = 'https://news.cnyes.com/news/id/7'
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in mobile + [original]])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',"
                             "error_code='article_identity_not_verified',attempts=1,"
                             "last_checked_at_utc='2026-10-10T00:33:00+00:00'")
            queue.db.execute("UPDATE articles SET body_sha256='stored-head' WHERE url=?", (mobile[1],))
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login' WHERE url=?", (mobile[2],))
            queue.db.execute("UPDATE articles SET state='retry',error_code='source_access_denied',due=9999999999 WHERE url=?", (mobile[3],))
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner=?,lease_until=9999999999 WHERE url=?", (queue.owner, mobile[4]))
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=9999999999 WHERE url=?", (mobile[5],))
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
                             ('source_capture_fingerprint:cnyes', old_contract))
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        queue.reconcile_policy()
        after = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        repaired = old_contract in PINNED_CONTRACTS
        assert after[mobile[0]] == ({**before[mobile[0]], 'state': 'pending', 'due': 0} if repaired else before[mobile[0]])
        for url in mobile[1:] + [original]:
            assert after[url] == before[url]
        if repaired:
            marker = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'cnyes_completed_mobile_stream_repair_v1:%'").fetchone()[0])
            assert marker['url_count'] == 1 and marker['accepted_as_bodies'] is False
            # A repeat failure is not silently retried again by owner restart.
            with queue.db:
                queue.db.execute("UPDATE articles SET state='article_identity_not_verified',attempts=2 WHERE url=?", (mobile[0],))
            queue.close()
            queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
            row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (mobile[0],)).fetchone())
            assert row['state'] == 'article_identity_not_verified' and row['attempts'] == 2
            assert row['error_code'] == 'article_identity_not_verified'
    finally:
        queue.close()


@pytest.mark.parametrize('previous', [CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT, 'unreviewed-query-contract'])
def test_query_view_repair_is_pinned_once_and_never_changes_other_outcomes(cnyes_config, tmp_path, previous):
    queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
    urls = [f'https://m.cnyes.com/news/id/{i}?exp=a' for i in range(1, 10)]
    urls[5] = urls[5].replace('exp=a', 'exp=b')
    urls[6] = urls[6].split('?')[0]
    urls[7] = urls[7].replace('m.cnyes.com', 'news.cnyes.com')
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                             "attempts=4,last_checked_at_utc='2026-10-10T01:50:36+00:00'")
            queue.db.execute("UPDATE articles SET body_sha256='stored' WHERE url=?", (urls[1],))
            queue.db.execute("UPDATE articles SET state='retry',error_code='source_access_denied',due=9999999999 WHERE url=?", (urls[2],))
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=9999999999 WHERE url=?", (urls[3],))
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner=?,lease_until=9999999999 WHERE url=?", (queue.owner, urls[4]))
            queue.db.execute("UPDATE articles SET lease_owner=?,lease_until=9999999999 WHERE url=?", (queue.owner, urls[8]))
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', ('source_capture_fingerprint:cnyes', previous))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        queue.reconcile_policy()
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        repaired = previous == CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT
        assert after[urls[0]] == ({**before[urls[0]], 'state': 'pending', 'due': 0} if repaired else before[urls[0]])
        for url in urls[1:]:
            assert after[url] == before[url]
        if repaired:
            marker = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'cnyes_mobile_query_view_repair_v1:%'").fetchone()[0])
            assert marker['url_count'] == 1 and marker['accepted_as_bodies'] is False
            with queue.db:
                queue.db.execute("UPDATE articles SET state='article_identity_not_verified',attempts=5 WHERE url=?", (urls[0],))
            queue.close()
            queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
            head = queue.db.execute('SELECT state,attempts,error_code FROM articles WHERE url=?', (urls[0],)).fetchone()
            assert tuple(head) == ('article_identity_not_verified', 5, 'article_identity_not_verified')
    finally:
        queue.close()


@pytest.mark.parametrize('previous', [CNYES_MOBILE_CANONICAL_V3_CAPTURE_FINGERPRINT,
                                     CNYES_MOBILE_CANONICAL_V4_CAPTURE_FINGERPRINT, 'unreviewed-capture'])
def test_paragraph_marker_repair_never_blanket_resets_attempts_or_retries(cnyes_config, tmp_path, previous):
    queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
    urls = [f'https://m.cnyes.com/news/id/{i}' for i in range(1, 6)]
    try:
        with queue.db:
            queue.add_articles([{'url': url} for url in urls])
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',attempts=4,"
                             "last_checked_at_utc='2026-10-10T00:57:56+00:00'")
            queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=9999999999 WHERE url=?", (urls[1],))
            queue.db.execute("UPDATE articles SET body_sha256='stored' WHERE url=?", (urls[2],))
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='paywall_or_login' WHERE url=?", (urls[3],))
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner=?,lease_until=9999999999 WHERE url=?", (queue.owner, urls[4]))
            queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', ('source_capture_fingerprint:cnyes', previous))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        queue.reconcile_policy()
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        known = {CNYES_MOBILE_CANONICAL_V3_CAPTURE_FINGERPRINT, CNYES_MOBILE_CANONICAL_V4_CAPTURE_FINGERPRINT}
        expected = {**before[urls[0]], 'state': 'pending', 'due': 0} if previous in known else before[urls[0]]
        assert after[urls[0]] == expected
        for url in urls[1:]:
            assert after[url] == before[url]
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',attempts=5 WHERE url=?", (urls[0],))
        queue.close()
        queue = NewsQueue(tmp_path, PolicyRegistry(cnyes_config, base=tmp_path))
        head = queue.db.execute('SELECT state,attempts,error_code FROM articles WHERE url=?', (urls[0],)).fetchone()
        assert tuple(head) == ('body_not_found', 5, 'body_not_found')
    finally:
        queue.close()
