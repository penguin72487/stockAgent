"""Identity failures may recover, but an unverified response never is a body."""
from dataclasses import replace
import json
import time

import pytest

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import CNYES_UNKNOWN_BODY_RECHECK_SECONDS
from test_news_cnyes_unknown_body_rechecks import config, legacy, queue, row, URL


ERROR = 'article_identity_not_verified'


@pytest.mark.parametrize('url', [URL, URL + '?exp=a', URL.replace('news.', 'm.'),
    URL.replace('news.', 'm.') + '?exp=a', URL.replace('news.', 'm.') + '?exp=b'])
def test_native_identity_failure_defers_without_accepting_or_changing_original_error(queue, url):
    queue.add_articles([{'url': url}])
    claimed = queue.claim_url(url, 300)
    error, now = NewsError(ERROR), time.time()
    queue.failed(claimed, error, 6)
    after = row(queue, url)
    assert after['state'] == 'retry' and after['error_code'] == ERROR
    assert after['due'] >= now + CNYES_UNKNOWN_BODY_RECHECK_SECONDS
    assert after['body_sha256'] is None and after['lease_owner'] is None
    assert queue.claim_url(url, 300) is None and error.retry is False
    assert queue.metrics.snapshot()['providers']['cnyes']['counts']['unknown_identity_failure_rechecks_deferred'] == 1
    assert queue.status()['cnyes_unknown_identity_recheck_contract_version'] == 1


def test_exhausted_burst_preserves_identity_error_and_longer_source_deadline(queue):
    legacy(queue, state=ERROR, error=ERROR)
    claimed = queue.claim_url(URL, 300)
    now = time.time()
    queue.failed(claimed, NewsError(ERROR, delay=7200), 1)
    after = row(queue)
    assert after['state'] == 'retry_cooldown' and after['due'] >= now + 7200
    assert after['attempts'] == 0 and after['body_sha256'] is None and after['error_code'] == ERROR


@pytest.mark.parametrize('change', [
    {'url': URL.replace('news.', 'amp-news.')}, {'url': URL + '?exp=unknown'},
    {'body': 'existing-raw-sha'}, {'error': 'http_429'}, {'lease': 'live-owner'},
    {'state': 'http_403'}, {'state': 'source_article_empty_template'},
])
def test_migration_never_overwrites_other_source_outcomes_saved_heads_or_live_leases(queue, change):
    options = {'state': ERROR, 'error': ERROR, **change}
    before = legacy(queue, **options)
    queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'unknown_cnyes_identity_rechecks_v1:%'")
    queue.db.commit()
    queue.migrate_unknown_cnyes_identity_rechecks()
    assert row(queue, options.get('url', URL)) == before


@pytest.mark.parametrize('change', [{'allowed': False}, {'acquisition_mode': 'finite'}])
def test_identity_retries_never_expand_disabled_or_finite_source_scope(queue, change):
    queue.registry.sources['cnyes'] = replace(queue.registry.sources['cnyes'], **change)
    before = legacy(queue, state=ERROR, error=ERROR)
    queue.failed(before, NewsError(ERROR), 6)
    assert row(queue)['state'] == ERROR
    before = row(queue)
    queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'unknown_cnyes_identity_rechecks_v1:%'")
    queue.db.commit()
    queue.migrate_unknown_cnyes_identity_rechecks()
    assert row(queue) == before


def test_old_terminal_identity_rechecks_once_preserving_negative_evidence_and_future_deadline(queue):
    due = time.time() + 9000
    before = legacy(queue, state=ERROR, error=ERROR, age=5, due=due)
    queue.db.execute("DELETE FROM queue_settings WHERE key LIKE 'unknown_cnyes_identity_rechecks_v1:%'")
    queue.db.commit()
    queue.migrate_unknown_cnyes_identity_rechecks()
    after = row(queue)
    assert after['state'] == 'retry_cooldown' and after['due'] == due
    for key in ('error_code', 'attempts', 'last_checked_at_utc', 'body_sha256', 'captured_at_utc', 'metadata_date_raw'):
        assert after[key] == before[key]
    marker = queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'unknown_cnyes_identity_rechecks_v1:%'").fetchone()
    proof = json.loads(marker[0])
    assert proof['original_error_code'] == ERROR and proof['rows'][0]['previous_due'] == due
    assert not proof['accepted_as_bodies'] and not proof['source_refusals_leases_stored_heads_and_finite_cohorts_changed']
    changes = queue.db.total_changes
    queue.migrate_unknown_cnyes_identity_rechecks()
    assert queue.db.total_changes == changes
