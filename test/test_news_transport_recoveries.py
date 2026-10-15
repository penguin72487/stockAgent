"""A pool reset/old body cannot be mistaken for later durable recovery."""
from contextlib import closing
from copy import deepcopy
import json

import pytest
import requests

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_transport_observations import audit as audit_transport
from scripts.audit_news_transport_recoveries import audit, later_complete_head
from test_news_fulltext import FakeResponse, article_html, config, http_fixture
from test_news_fulltext_performance import FailedResponse
from test_news_transport_observations_audit import publish_status


@pytest.mark.parametrize('patch,expected', [
    ({}, True), ({'state': 'retry'}, False), ({'body_sha256': None}, False),
    ({'captured_at_utc': None}, False), ({'captured_at_utc': '2026-10-11T01:00:00+00:00'}, False),
    ({'captured_at_utc': '2026-10-11T00:59:59+00:00'}, False),
])
def test_forward_time_is_strict_and_requires_complete_body(patch, expected):
    row = {'state': 'complete', 'body_sha256': 'digest', 'captured_at_utc': '2026-10-11T01:00:01+00:00'}
    row.update(patch)
    assert later_complete_head(row, '2026-10-11T01:00:00+00:00') is expected


def test_naive_time_never_fills_a_missing_source_offset():
    with pytest.raises(ValueError, match='timezone_aware_forward_capture_required'):
        later_complete_head({'state': 'complete', 'body_sha256': 'digest',
                             'captured_at_utc': '2026-10-11T01:00:01'}, '2026-10-11T01:00:00+00:00')


@pytest.mark.parametrize('capture', [False, True])
def test_actual_error_then_canonical_capture_and_receipt(config, tmp_path, capture):
    url = 'https://publisher.test/a'
    # The synthetic generic recipe has no article route by default, so its
    # HTTP observer correctly labels /a as metadata unless explicitly scoped.
    config['providers'][0]['article_path_pattern'] = r'^/a$'
    routes = {'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
              url: FailedResponse(requests.exceptions.ReadTimeout())}
    http, session = http_fixture(config, tmp_path, routes)
    with closing(NewsQueue(tmp_path, http.registry)) as queue, closing(http):
        http.metrics = queue.metrics
        with queue.db:
            queue.add_articles([{'url': url}])
        with pytest.raises(NewsError):
            http.get(url)
        publish_status(queue, http)
        transport = audit_transport(tmp_path, http.registry, ['publisher'])
        assert transport['accepted']
        assert transport['providers']['publisher']['observed_failure_count'] == 1
        if capture:
            session.routes[url] = FakeResponse(article_html())
            queue.accept([fetch_article(http, queue.claim_url(url, config['lease_seconds']))])
        settings = list(queue.db.execute('SELECT key,value FROM queue_settings ORDER BY key'))
        changes, calls = queue.db.total_changes, len(session.calls)
        result = audit(tmp_path, http.registry, transport)
        assert result['accepted'] and result['sample_article_failure_events'] == 1
        assert result['verified_article_recoveries'] == int(capture)
        assert result['all_sample_article_failures_recovered'] is capture
        assert len(session.calls) == calls and queue.db.total_changes == changes
        assert list(queue.db.execute('SELECT key,value FROM queue_settings ORDER BY key')) == settings
        assert not result['publisher_requests'] and not result['queue_writes']
        assert not result['all_history_complete']
        wrong_owner = deepcopy(transport)
        wrong_owner['owner_id'] = 'another-owner'
        with pytest.raises(ValueError, match='failure_snapshot_owner_changed'):
            audit(tmp_path, http.registry, wrong_owner)
        for path in (tmp_path / 'transport_problems/publisher').glob('*/*.json'):
            event = json.loads(path.read_bytes())
            event['requested_url'] = 'https://publisher.test/other'
            path.write_text(json.dumps(event))
        with pytest.raises(ValueError):
            audit(tmp_path, http.registry, transport)
