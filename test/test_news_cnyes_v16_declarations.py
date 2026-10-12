"""A pinned negative-declaration recheck is not a deadline or rights waiver."""
from dataclasses import replace
import json
import time

import pytest

from downloader.news_fulltext_publishers import CNYES_MOBILE_CANONICAL_V16_CAPTURE_FINGERPRINT
from test_news_cnyes_unknown_body_rechecks import config, legacy, queue, row


URL = 'https://m.cnyes.com/news/id/5418903'


@pytest.mark.parametrize('state', ['body_not_found', 'article_identity_not_verified'])
def test_only_due_negative_declarations_requeue_once_with_original_evidence(queue, state):
    before = legacy(queue, url=URL, state=state, error=state)
    queue.recheck_cnyes_native_v16_declarations(CNYES_MOBILE_CANONICAL_V16_CAPTURE_FINGERPRINT)
    after = row(queue, URL)
    assert after['state'] == 'retry' and after['due'] == 0
    for key in ('error_code', 'attempts', 'last_checked_at_utc', 'body_sha256', 'url'):
        assert after[key] == before[key]
    marker = queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'cnyes_native_v16_declaration_repair_v1:%'").fetchone()
    proof = json.loads(marker[0])
    assert proof['url_count'] == 1 and proof['accepted_as_bodies'] is False
    assert not proof['source_refusals_stored_heads_and_future_deadlines_reset']
    changes = queue.db.total_changes
    queue.recheck_cnyes_native_v16_declarations(CNYES_MOBILE_CANONICAL_V16_CAPTURE_FINGERPRINT)
    assert queue.db.total_changes == changes


@pytest.mark.parametrize('change', [
    {'state': 'http_403', 'error': 'http_403'}, {'state': 'paywall', 'error': 'paywall'},
    {'state': 'retry', 'error': 'body_not_found'}, {'error': 'http_429'},
    {'due': time.time() + 10000}, {'lease': 'another-owner'}, {'body': 'stored-sha'},
    {'url': URL.replace('m.', 'news.')}, {'url': URL + '?exp=unknown'},
    {'url': URL.replace('m.', 'amp-news.')},
])
def test_repair_preserves_stored_heads_refusals_paid_routes_leases_and_future_dates(queue, change):
    options = {'url': URL, 'state': 'article_identity_not_verified', 'error': 'article_identity_not_verified', **change}
    before = legacy(queue, **options)
    queue.recheck_cnyes_native_v16_declarations(CNYES_MOBILE_CANONICAL_V16_CAPTURE_FINGERPRINT)
    assert row(queue, options['url']) == before


@pytest.mark.parametrize('reason', ['unrelated_contract', 'same_contract', 'source_disabled'])
def test_recheck_requires_exact_pinned_allowed_changed_source(queue, reason):
    before = legacy(queue, url=URL, state='article_identity_not_verified', error='article_identity_not_verified')
    source = queue.registry.sources['cnyes']
    previous = CNYES_MOBILE_CANONICAL_V16_CAPTURE_FINGERPRINT
    if reason == 'unrelated_contract':
        previous = '0' * 64
    elif reason == 'same_contract':
        queue.registry.sources['cnyes'] = replace(source, data_fingerprint=previous)
    else:
        queue.registry.sources['cnyes'] = replace(source, allowed=False)
    queue.recheck_cnyes_native_v16_declarations(previous)
    assert row(queue, URL) == before
