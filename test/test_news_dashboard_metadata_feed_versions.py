"""Native RSS metadata generations must reach the same read-only dashboard."""
import json
from datetime import timedelta

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import NOW, _metadata_feed_snapshot


def _row(tmp_path, status):
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    public = build_news_public_status(tmp_path, now=NOW)
    return public, next(row for row in public['providers'] if row['id'] == 'ltn')


@pytest.mark.parametrize('version', [1, 2])
def test_native_metadata_generation_preserves_counts_clocks_and_no_body_scope(tmp_path, version):
    status = _metadata_feed_snapshot(tmp_path)
    raw = status['providers']['ltn']['metadata_feed_progress']
    raw['contract_version'] = version
    raw['failure_diagnostic_contract_version'] = 1
    public, row = _row(tmp_path, status)
    progress = row['metadata_feed_progress']
    assert progress is not None
    assert progress['contract_version'] == version
    for key in ('registered_feeds', 'verified_feeds', 'latest_feed_item_observations',
                'latest_feed_response_bytes', 'feeds_with_errors', 'poll_interval_seconds',
                'next_check_at_utc', 'last_check_at_utc'):
        assert progress[key] == raw[key]
    assert row['state'] == 'metadata_polling'
    assert not row['automatic_fulltext_allowed'] and row['complete_urls'] == 0
    assert row['conditional_eta_seconds'] is None and row['completion_forecast'] is None
    assert progress['article_bodies_accepted'] == 0 and progress['finish_eta_utc'] is None
    for secret in ('PRIVATE_XML', 'PRIVATE_SUMMARY', 'PRIVATE_TOKEN', 'PRIVATE_HTML', 'PRIVATE_BODY'):
        assert secret not in json.dumps(public)


@pytest.mark.parametrize('version', [1, 2])
@pytest.mark.parametrize('cooling,errors,expected', [
    (False, 0, 'metadata_polling'), (False, 1, 'attention'),
    (True, 0, 'cooldown'), (True, 1, 'cooldown'),
])
def test_both_metadata_generations_cannot_hide_errors_or_original_due(tmp_path, version, cooling, errors, expected):
    status = _metadata_feed_snapshot(tmp_path)
    status['providers']['ltn']['metadata_feed_progress'].update(contract_version=version, feeds_with_errors=errors)
    if cooling:
        status['provider_next_attempt_utc']['ltn'] = (NOW + timedelta(minutes=5)).isoformat()
    _, row = _row(tmp_path, status)
    assert row['metadata_feed_progress'] is not None
    assert row['state'] == expected
    assert row['conditional_eta_seconds'] is None and row['completion_forecast'] is None


@pytest.mark.parametrize('version', [0, 3, None, True, False, 1.0, 2.0, '1', '2'])
def test_unrecognized_or_coerced_generation_is_not_metadata_evidence(tmp_path, version):
    status = _metadata_feed_snapshot(tmp_path)
    status['providers']['ltn']['metadata_feed_progress']['contract_version'] = version
    _, row = _row(tmp_path, status)
    assert row['metadata_feed_progress'] is None
    assert row['complete_urls'] == 0


@pytest.mark.parametrize('changes', [
    {'enabled': False}, {'verified_feeds': 15}, {'registered_feeds': 15},
    {'feeds_with_errors': 15}, {'latest_feed_item_observations': -1},
    {'latest_feed_response_bytes': True}, {'poll_interval_seconds': 59},
    {'article_bodies_accepted': 1}, {'article_bodies_accepted': False},
    {'all_history_complete': True}, {'overlapping_feed_items_not_unique_articles': False},
    {'finish_eta_utc': '2026-10-14T00:00:00+00:00'},
])
def test_metadata_generation_two_keeps_all_original_scope_and_quantity_guards(tmp_path, changes):
    status = _metadata_feed_snapshot(tmp_path)
    raw = status['providers']['ltn']['metadata_feed_progress']
    raw.update(contract_version=2)
    raw.update(changes)
    _, row = _row(tmp_path, status)
    assert row['metadata_feed_progress'] is None
    assert row['complete_urls'] == 0 and not row['automatic_fulltext_allowed']
