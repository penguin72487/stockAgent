"""Unknown bodies can recover; genuine absence, scope and deadlines cannot reset."""
from dataclasses import replace
from datetime import datetime, timezone
import json
import sqlite3
import time

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import (NewsQueue, CNYES_UNKNOWN_BODY_RECHECK_SECONDS,
                                          CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION)
from test_cnyes_news_bulk import cfg as config


URL = 'https://news.cnyes.com/news/id/123'
RECHECK_KEY_PREFIX = f'unknown_cnyes_body_rechecks_v{CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION}:'


@pytest.fixture
def queue(config, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    yield value
    value.close()


def row(queue, url=URL):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())


def legacy(queue, *, url=URL, state='body_not_found', error='body_not_found', age=1000, due=0, body=None, lease=None):
    clock = datetime.fromtimestamp(time.time() - age, timezone.utc).isoformat()
    with queue.db:
        queue.add_articles([{'url': url}])
        queue.db.execute('UPDATE articles SET state=?,error_code=?,last_checked_at_utc=?,attempts=3,due=?,body_sha256=?,lease_owner=?,lease_until=? WHERE url=?',
                         (state, error, clock, due, body, lease, time.time() + 600 if lease else None, url))
        queue.db.execute('DELETE FROM queue_settings WHERE key LIKE ?', (RECHECK_KEY_PREFIX + '%',))
    return row(queue, url)


@pytest.mark.parametrize('url', [URL, URL.replace('news.cnyes.com', 'm.cnyes.com'), URL + '?exp=a',
    URL.replace('news.cnyes.com', 'm.cnyes.com') + '?exp=a',
    URL.replace('news.cnyes.com', 'm.cnyes.com') + '?exp=b'])
def test_native_unknown_missing_body_is_deferred_not_completed(queue, url):
    with queue.db:
        queue.add_articles([{'url': url}])
    claimed = queue.claim_url(url, 300)
    error = NewsError('body_not_found')
    now = time.time()
    queue.failed(claimed, error, 6)
    result = row(queue, url)
    assert result['state'] == 'retry' and result['error_code'] == 'body_not_found'
    assert result['due'] >= now + CNYES_UNKNOWN_BODY_RECHECK_SECONDS
    assert result['body_sha256'] is None and result['lease_owner'] is None
    assert error.retry is False  # Do not mutate a worker's original observation.
    assert queue.claim_url(url, 300) is None


def test_exhausted_burst_keeps_recoverability_but_hourly_cooldown(queue):
    legacy(queue)
    claimed = queue.claim_url(URL, 300)
    now = time.time()
    queue.failed(claimed, NewsError('body_not_found', delay=5000), 1)
    result = row(queue)
    assert result['state'] == 'retry_cooldown' and result['due'] >= now + 5000
    assert result['attempts'] == 0 and result['error_code'] == 'body_not_found'


@pytest.mark.parametrize('error', ['source_not_found', 'source_article_empty_template',
    'article_attribution_not_verified', 'paywall_or_login', 'article_rights_or_scope_excluded',
    'article_license_not_verified', 'access_challenge', 'http_403', 'http_429'])
def test_other_outcomes_are_not_converted_to_unknown_body_retries(queue, error):
    with queue.db:
        queue.add_articles([{'url': URL}])
    claimed = queue.claim_url(URL, 300)
    queue.failed(claimed, NewsError(error), 6)
    assert row(queue)['state'] == error and row(queue)['due'] == 0


@pytest.mark.parametrize('url', ['https://amp-news.cnyes.com/news/id/123', URL + '?exp=unknown',
    URL + '?exp=b', URL + '?exp=a&x=1', URL + '?exp=%61', URL + '?exp=a&exp=a', URL + '#x',
    'http://news.cnyes.com/news/id/123', URL + '/', URL + 'x', URL.replace('/123', '/0123'),
    URL.replace('/123', '/0'), 'https://u:p@news.cnyes.com/news/id/123',
    'https://news.cnyes.com:443/news/id/123', 'https://news.cnyes.com.evil.test/news/id/123'])
def test_unreviewed_routes_are_not_recovery_scope(queue, url):
    assert not queue.unknown_cnyes_body_recheck_allowed(queue.registry.sources['cnyes'],
                                                      {'provider': 'cnyes', 'url': url, 'body_sha256': None})


@pytest.mark.parametrize('change', [{'allowed': False}, {'acquisition_mode': 'finite'}, {'provider': 'other'}])
def test_disabled_finite_or_other_provider_cannot_expand_scope(queue, change):
    source = replace(queue.registry.sources['cnyes'], **change)
    assert not queue.unknown_cnyes_body_recheck_allowed(source, {'provider': 'cnyes', 'url': URL})


def test_old_failure_recovery_preserves_evidence_due_attempts_and_unrelated_work(queue):
    deadline = time.time() + 6000
    before = legacy(queue, age=5, due=deadline)
    with queue.db:
        queue.add_articles([{'url': 'https://news.cnyes.com/news/id/456'}])
    other = row(queue, 'https://news.cnyes.com/news/id/456')
    queue.migrate_unknown_cnyes_body_rechecks()
    after = row(queue)
    assert after['state'] == 'retry_cooldown' and after['due'] == deadline
    for key in ('attempts', 'error_code', 'last_checked_at_utc', 'body_sha256', 'captured_at_utc', 'metadata_date_raw'):
        assert after[key] == before[key]
    assert row(queue, 'https://news.cnyes.com/news/id/456') == other
    proof = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?', (RECHECK_KEY_PREFIX + '%',)).fetchone()[0])
    assert proof['rows'][0]['previous_last_checked_at_utc'] == before['last_checked_at_utc']
    assert proof['rows'][0]['attempts_preserved'] == 3
    assert proof['accepted_as_bodies'] is False


def test_recent_failure_gets_minimum_wait_not_immediate_loop(queue):
    before = legacy(queue, age=5)
    queue.migrate_unknown_cnyes_body_rechecks()
    assert row(queue)['due'] == datetime.fromisoformat(before['last_checked_at_utc']).timestamp() + 300


@pytest.mark.parametrize('overrides', [{'state': 'http_403', 'error': 'http_403'},
    {'state': 'source_article_empty_template'}, {'error': 'paywall_or_login'}, {'body': 'a' * 64},
    {'lease': 'other'}, {'state': 'inflight'}, {'state': 'awaiting_authorization'}, {'state': 'waiting_cohort'}])
def test_migration_does_not_reset_refusals_leases_or_saved_heads(queue, overrides):
    before = legacy(queue, **overrides)
    queue.migrate_unknown_cnyes_body_rechecks()
    assert row(queue) == before


@pytest.mark.parametrize('clock', [None, '2026-10-10', '2026-10-10T00:00:00', 'invalid'])
def test_unobserved_or_unaware_failure_clock_is_not_invented(queue, clock):
    legacy(queue)
    with queue.db:
        queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (clock, URL))
    before = row(queue)
    queue.migrate_unknown_cnyes_body_rechecks()
    assert row(queue) == before


def test_unchanged_owner_start_is_idempotent_without_frontier_rescan(queue):
    legacy(queue)
    queue.migrate_unknown_cnyes_body_rechecks()
    before = row(queue)
    traced = []
    queue.db.set_trace_callback(traced.append)
    queue.migrate_unknown_cnyes_body_rechecks()
    queue.db.set_trace_callback(None)
    assert row(queue) == before and len(traced) == 1
    assert 'queue_settings' in traced[0] and 'articles' not in traced[0]


def test_migration_and_evidence_commit_atomically(queue):
    before = legacy(queue)
    queue.db.execute(f"CREATE TRIGGER deny_migration_receipt BEFORE INSERT ON queue_settings WHEN NEW.key LIKE '{RECHECK_KEY_PREFIX}%' BEGIN SELECT RAISE(ABORT,'test sink failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='test sink failure'):
        queue.migrate_unknown_cnyes_body_rechecks()
    assert row(queue) == before
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key LIKE ?', (RECHECK_KEY_PREFIX + '%',)).fetchone()


@pytest.mark.parametrize('url', [URL + '?exp=a', URL.replace('news.cnyes.com', 'm.cnyes.com') + '?exp=b'])
def test_supported_view_migration_preserves_prior_contract_journal(queue, url):
    before = legacy(queue, url=url)
    old_key = 'unknown_cnyes_body_rechecks_v1:' + queue.registry.sources['cnyes'].data_fingerprint
    old_proof = json.dumps({'contract_version': 1, 'rows': [], 'accepted_as_bodies': False})
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (old_key, old_proof))
    queue.migrate_unknown_cnyes_body_rechecks()
    assert row(queue, url)['state'] == 'retry_cooldown'
    assert row(queue, url)['last_checked_at_utc'] == before['last_checked_at_utc']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (old_key,)).fetchone()[0] == old_proof


@pytest.mark.parametrize('url', [URL, URL + '?exp=a', URL.replace('news.cnyes.com', 'm.cnyes.com') + '?exp=b'])
def test_full_policy_rollout_cannot_reset_the_bounded_native_recheck_evidence(queue, url):
    deadline = time.time() + 6000
    before = legacy(queue, url=url, age=5, due=deadline)
    with queue.db:
        queue.db.execute("UPDATE queue_settings SET value='previous-policy' WHERE key='policy_fingerprint'")
        queue.db.execute("UPDATE queue_settings SET value='previous-source' WHERE key='source_capture_fingerprint:cnyes'")
    queue.reconcile_policy()
    after = row(queue, url)
    assert after['state'] == 'retry_cooldown'
    for field in ('due', 'attempts', 'last_checked_at_utc', 'error_code', 'body_sha256', 'lease_owner'):
        assert after[field] == before[field]
    proof = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?',
                                       (RECHECK_KEY_PREFIX + '%',)).fetchone()[0])
    assert proof['rows'][0]['previous_due'] == deadline


def test_recovery_uses_failure_state_index_not_full_pending_scan(queue):
    plan = ' '.join(r[3] for r in queue.db.execute("EXPLAIN QUERY PLAN SELECT id,url FROM articles INDEXED BY dispatch WHERE provider='cnyes' AND state='body_not_found' AND body_sha256 IS NULL"))
    assert 'SEARCH articles USING INDEX dispatch' in plan
    assert 'SCAN articles' not in plan and 'TEMP B-TREE' not in plan


def test_read_only_recheck_audit_requires_loaded_contract_and_preserved_observation(queue):
    from scripts.audit_news_body_rechecks import audit
    legacy(queue)
    queue.migrate_unknown_cnyes_body_rechecks()
    queue.status(activity='running')
    result = audit(queue.root, queue.registry)
    assert result['accepted'] and result['original_failure_cohort_urls'] == 1
    assert result['publisher_requests'] == 0 and not result['body_durability_verified']
    assert result['current_cohort_states'] == {'retry_cooldown': 1}
    with queue.db:
        queue.db.execute('UPDATE articles SET due=0 WHERE url=?', (URL,))
    assert not audit(queue.root, queue.registry)['accepted']


def test_read_only_recheck_audit_does_not_accept_old_loaded_status(queue):
    from downloader.artifact_io import atomic_write_json
    from scripts.audit_news_body_rechecks import audit
    legacy(queue)
    queue.migrate_unknown_cnyes_body_rechecks()
    status = queue.status(activity='running')
    status.pop('cnyes_unknown_body_recheck_contract_version')
    atomic_write_json(queue.root / 'status.json', status)
    assert not audit(queue.root, queue.registry)['accepted']


def test_recheck_audit_keeps_original_cohort_when_new_parser_journal_is_empty(queue):
    from scripts.audit_news_body_rechecks import audit
    legacy(queue)
    queue.migrate_unknown_cnyes_body_rechecks()
    queue.status(activity='running')
    original = audit(queue.root, queue.registry)['rows'][0]['original_observation']
    queue.registry.sources['cnyes'] = replace(queue.registry.sources['cnyes'], data_fingerprint='a' * 64)
    queue.reconcile_policy()
    queue.status(activity='running')
    before = [tuple(value) for value in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    result = audit(queue.root, queue.registry)
    assert result['accepted'] and result['contract_version'] == 2
    assert result['original_failure_cohort_urls'] == 1
    assert result['rows'][0]['original_observation'] == original
    assert sorted(journal['original_urls'] for journal in result['retained_recheck_journals']) == [0, 1]
    assert result['rows'][0]['current_last_checked_at_utc'] == original['previous_last_checked_at_utc']
    assert not result['stored_body_heads_observed'] and not result['body_durability_verified']
    assert result['publisher_requests'] == 0 and not result['source_states_mutated']
    assert before == [tuple(value) for value in queue.db.execute('SELECT * FROM articles ORDER BY id')]


def test_duplicate_journals_do_not_double_count_the_same_original_url(queue):
    from scripts.audit_news_body_rechecks import audit
    legacy(queue)
    queue.migrate_unknown_cnyes_body_rechecks()
    key, payload = queue.db.execute('SELECT key,value FROM queue_settings WHERE key LIKE ?', (RECHECK_KEY_PREFIX + '%',)).fetchone()
    journal = json.loads(payload)
    journal['source_fingerprint'] = 'b' * 64
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (RECHECK_KEY_PREFIX + 'b' * 64, json.dumps(journal)))
    queue.status(activity='running')
    result = audit(queue.root, queue.registry)
    assert result['accepted'] and result['original_failure_cohort_urls'] == 1
    assert len(result['retained_recheck_journals']) == 2


def test_invalid_retained_journal_is_not_erased_by_a_valid_current_journal(queue):
    from scripts.audit_news_body_rechecks import audit
    legacy(queue)
    queue.migrate_unknown_cnyes_body_rechecks()
    payload = queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?', (RECHECK_KEY_PREFIX + '%',)).fetchone()[0]
    journal = json.loads(payload)
    journal['source_fingerprint'] = 'b' * 64
    journal['accepted_as_bodies'] = True
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (RECHECK_KEY_PREFIX + 'b' * 64, json.dumps(journal)))
    queue.status(activity='running')
    result = audit(queue.root, queue.registry)
    assert not result['accepted'] and result['original_failure_cohort_urls'] == 1
    assert result['errors'][0]['error'] == 'original_recheck_journal_invalid'
