"""Saved-body presence must neither stop recovery nor assert source freshness."""
from dataclasses import replace
from datetime import datetime, timezone
import json
import sqlite3
import time

import pytest

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import (
    CNYES_RETAINED_VALIDATION_RECHECK_CONTRACT_VERSION,
    CNYES_UNKNOWN_BODY_RECHECK_SECONDS, NewsQueue,
)
from downloader.news_fulltext_repairs import STORED_HEAD_FIELDS
from test_news_empty_response_rechecks import config, queue, saved_body, head, URL


PREFIX = f'retained_cnyes_validation_rechecks_v{CNYES_RETAINED_VALIDATION_RECHECK_CONTRACT_VERSION}:'
ERRORS = ('body_not_found', 'article_identity_not_verified')


def terminal(queue, error='body_not_found', *, due=0, **changes):
    saved_body(queue)
    values = dict(state=error, error_code=error, attempts=3, due=due,
        last_checked_at_utc=datetime.now(timezone.utc).isoformat())
    values.update(changes)
    with queue.db:
        queue.db.execute('UPDATE articles SET ' + ','.join(k + '=?' for k in values) + ' WHERE url=?',
                         (*values.values(), URL))
        # Emulate a pre-ABI database, not a production repair or deadline reset.
        queue.db.execute('DELETE FROM queue_settings WHERE key LIKE ?', (PREFIX + '%',))
    return head(queue)


def migration_proof(queue):
    return json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?', (PREFIX + '%',)).fetchone()[0])


@pytest.mark.parametrize('error', ERRORS)
def test_new_retained_validation_failure_defers_and_keeps_exact_old_representation(queue, error):
    saved_body(queue)
    claimed = queue.claim_url(URL, 300, refresh=True)
    before = head(queue)
    observation, now = NewsError(error), time.time()
    queue.failed(claimed, observation, 6)
    after = head(queue)
    assert after['state'] == 'retry' and after['error_code'] == error
    assert after['due'] >= now + CNYES_UNKNOWN_BODY_RECHECK_SECONDS
    assert after['last_checked_at_utc'] >= before['last_checked_at_utc']
    assert after['lease_owner'] is after['lease_until'] is None
    assert not observation.retry and queue.claim_url(URL, 300) is None
    for key in (*STORED_HEAD_FIELDS, 'authorization_fingerprint', 'metadata_date_raw'):
        assert after[key] == before[key]
    status = queue.status()
    assert status['cnyes_retained_validation_recheck_contract_version'] == 1
    coverage = status['providers']['cnyes']
    assert coverage['stored_body_urls'] == status['eligible_stored_body_recheck_articles'] == 1
    assert coverage['body_state_counts'] == {'missing': {}, 'stored': {'retry': 1}}
    assert status['eligible_missing_body_articles'] == 0
    assert queue.metrics.snapshot()['providers']['cnyes']['counts']['retained_validation_failure_rechecks_deferred'] == 1


@pytest.mark.parametrize('error', ERRORS)
def test_exhausted_saved_burst_preserves_old_body_and_longer_source_wait(queue, error):
    saved_body(queue)
    claimed = queue.claim_url(URL, 300, refresh=True)
    before, now = head(queue), time.time()
    queue.failed(claimed, NewsError(error, delay=7200), 1)
    after = head(queue)
    assert after['state'] == 'retry_cooldown' and after['attempts'] == 0
    assert after['due'] >= now + 7200 and after['error_code'] == error
    assert all(after[k] == before[k] for k in STORED_HEAD_FIELDS)


@pytest.mark.parametrize('error', ['source_not_found', 'source_article_empty_template', 'source_article_test_only',
    'paywall_or_login', 'article_rights_or_scope_excluded', 'article_attribution_not_verified',
    'article_license_not_verified', 'access_challenge', 'http_403', 'http_429', 'unsupported_article_path'])
def test_old_body_does_not_turn_a_real_source_outcome_into_parser_recovery(queue, error):
    saved_body(queue)
    claimed = queue.claim_url(URL, 300, refresh=True)
    before = head(queue)
    queue.failed(claimed, NewsError(error), 6)
    after = head(queue)
    assert after['state'] == error and after['due'] == 0
    assert all(after[k] == before[k] for k in STORED_HEAD_FIELDS)


@pytest.mark.parametrize('error', ERRORS)
@pytest.mark.parametrize('future_due', [False, True])
def test_terminal_saved_recovery_is_not_body_acceptance_and_preserves_negative_clocks(queue, error, future_due):
    deadline = time.time() + 9000 if future_due else 0
    before = terminal(queue, error, due=deadline)
    queue.add_articles([{'url': URL.replace('/news/id/', '/news/id/8')}])
    unrelated = [tuple(r) for r in queue.db.execute('SELECT * FROM articles WHERE url<>?', (URL,))]
    queue.migrate_cnyes_retained_validation_rechecks()
    after = head(queue)
    assert after['state'] == 'retry_cooldown'
    expected_due = max(deadline, datetime.fromisoformat(before['last_checked_at_utc']).timestamp() + 300)
    assert after['due'] == expected_due
    assert {k: after[k] for k in before if k not in {'state', 'due'}} == {k: v for k, v in before.items() if k not in {'state', 'due'}}
    assert unrelated == [tuple(r) for r in queue.db.execute('SELECT * FROM articles WHERE url<>?', (URL,))]
    proof = migration_proof(queue)
    assert len(proof['rows']) == 1 and proof['rows'][0]['previous_state'] == error
    assert proof['rows'][0]['attempts_preserved'] == before['attempts']
    assert proof['rows'][0]['stored_head']['body_sha256'] == before['body_sha256']
    assert not proof['accepted_as_bodies'] and not proof['source_freshness_verified']
    assert not proof['body_payloads_references_and_original_clocks_changed']
    assert not proof['source_refusals_leases_and_finite_cohorts_changed']


@pytest.mark.parametrize('field,value', [
    ('body_sha256', None), ('body_sha256', 'unverified'), ('body_bytes', 0),
    ('parquet_path', None), ('parquet_path', '../outside.parquet'),
    ('authorization_fingerprint', '0' * 64), ('captured_at_utc', None),
    ('captured_at_utc', '2026-01-01T00:00:00'), ('last_checked_at_utc', 'invalid'),
    ('last_checked_at_utc', '2999-01-01T00:00:00+00:00'),
    ('attempts', -1),
])
def test_partial_untrusted_or_future_saved_reference_is_not_recovery_evidence(queue, field, value):
    before = terminal(queue, **{field: value})
    queue.migrate_cnyes_retained_validation_rechecks()
    assert head(queue) == before and migration_proof(queue)['rows'] == []


@pytest.mark.parametrize('changes', [
    {'state': 'complete'}, {'state': 'retry'}, {'state': 'http_403'}, {'error_code': 'http_429'},
    {'lease_owner': 'other'}, {'lease_until': 9999999999}, {'due': -1}, {'due': float('inf')},
])
def test_migration_does_not_overwrite_other_states_live_leases_or_invalid_deadlines(queue, changes):
    before = terminal(queue, **changes)
    queue.migrate_cnyes_retained_validation_rechecks()
    assert head(queue) == before and not migration_proof(queue)['rows']


@pytest.mark.parametrize('change', [{'allowed': False}, {'acquisition_mode': 'finite'}, {'provider': 'other'}])
def test_retained_recovery_does_not_expand_disabled_finite_or_other_provider_scope(queue, change):
    before = terminal(queue)
    source = replace(queue.registry.sources['cnyes'], **change)
    assert not queue.cnyes_retained_validation_recheck_allowed(source, before)


@pytest.mark.parametrize('url', ['http://news.cnyes.com/news/id/1', 'https://amp-news.cnyes.com/news/id/1',
    'https://news.cnyes.com/news/print/1', 'https://m.cnyes.com/news/id/1?unreviewed=1',
    'https://news.cnyes.com.evil.test/news/id/1'])
def test_no_old_body_can_create_a_new_route_or_alias_permission(queue, url):
    before = terminal(queue)
    assert not queue.cnyes_retained_validation_recheck_allowed(queue.registry.sources['cnyes'], {**before, 'url': url})


def test_retry_migration_commits_with_its_evidence_and_never_scans_healthy_frontier_twice(queue):
    before = terminal(queue)
    queue.db.execute(f"CREATE TRIGGER deny_retained_recheck BEFORE INSERT ON queue_settings WHEN NEW.key LIKE '{PREFIX}%' BEGIN SELECT RAISE(ABORT,'test receipt failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='test receipt failure'):
        queue.migrate_cnyes_retained_validation_rechecks()
    assert head(queue) == before
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key LIKE ?', (PREFIX + '%',)).fetchone()
    queue.db.execute('DROP TRIGGER deny_retained_recheck')
    queue.migrate_cnyes_retained_validation_rechecks()
    first, proof = head(queue), migration_proof(queue)
    traced = []
    queue.db.set_trace_callback(traced.append)
    queue.migrate_cnyes_retained_validation_rechecks()
    queue.db.set_trace_callback(None)
    assert len(traced) == 1 and 'queue_settings' in traced[0] and 'articles' not in traced[0]
    assert head(queue) == first and migration_proof(queue) == proof
    plan = ' '.join(r[3] for r in queue.db.execute("EXPLAIN QUERY PLAN SELECT * FROM articles INDEXED BY dispatch "
        "WHERE provider='cnyes' AND state IN ('body_not_found','article_identity_not_verified') AND state=error_code AND body_sha256 IS NOT NULL"))
    assert 'SEARCH articles USING INDEX dispatch' in plan and 'SCAN articles' not in plan and 'TEMP B-TREE' not in plan


def test_full_policy_rollout_preserves_saved_failure_attempts_and_deadline_until_migration(queue):
    before = terminal(queue, due=time.time() + 9000)
    with queue.db:
        queue.db.execute("UPDATE queue_settings SET value='previous-policy' WHERE key='policy_fingerprint'")
        queue.db.execute("UPDATE queue_settings SET value='previous-source' WHERE key='source_capture_fingerprint:cnyes'")
    queue.reconcile_policy()
    after = head(queue)
    assert after == {**before, 'state': 'retry_cooldown'}
    assert len(migration_proof(queue)['rows']) == 1


def test_new_owner_resumes_old_terminal_check_once_without_republishing_the_saved_body(queue):
    original = terminal(queue, 'article_identity_not_verified')
    root, registry = queue.root, queue.registry
    queue.close()
    resumed = NewsQueue(root, registry)
    try:
        after, proof = head(resumed), migration_proof(resumed)
        assert after['state'] == 'retry_cooldown' and len(proof['rows']) == 1
        assert all(after[k] == original[k] for k in (*STORED_HEAD_FIELDS, 'last_checked_at_utc', 'attempts'))
    finally:
        resumed.close()
    again = NewsQueue(root, registry)
    try:
        assert head(again) == after and migration_proof(again) == proof
    finally:
        again.close()


def test_old_review_mail_cannot_enqueue_a_second_copy_after_automatic_recovery(queue):
    from test_news_empty_response_rechecks import legacy_empty_failure, submit
    from downloader.news_fulltext_repairs import consume_repairs
    reference = legacy_empty_failure(queue)
    assert submit(queue, reference)['submitted']
    with queue.db:
        queue.db.execute('DELETE FROM queue_settings WHERE key LIKE ?', (PREFIX + '%',))
    before = head(queue)
    queue.migrate_cnyes_retained_validation_rechecks()
    automatic = head(queue)
    assert automatic['state'] == 'retry_cooldown'
    assert consume_repairs(queue, max_seconds=0.1)['outcomes'] == {'not_semantic_parser_failure': 1}
    assert head(queue) == automatic and automatic['attempts'] == before['attempts']
