"""Source cooldowns remain mandatory; a due refusal cannot starve first work.

Fixtures use isolated queues and no publisher requests. Selecting another URL
after a source cooldown is ordinary scheduling, not overriding that cooldown.
"""
import time

import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config


def seed(queue, *, pending=16, state='retry', error='source_access_denied', due=None):
    with queue.db:
        queue.add_articles([
            {'url': f'https://publisher.test/first/{i}', 'date': '2014-01-01'}
            for i in range(pending)
        ] + [{'url': 'https://publisher.test/refused-newer', 'date': '2026-10-14'}])
        queue.db.execute(
            'UPDATE articles SET state=?,error_code=?,due=?,attempts=2,'
            "last_checked_at_utc='2026-10-14T12:16:47+00:00' WHERE url=?",
            (state, error, time.time() - 30 if due is None else due,
             'https://publisher.test/refused-newer'),
        )
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?',
                                ('https://publisher.test/refused-newer',)).fetchone())


@pytest.mark.parametrize('state', ['retry', 'retry_cooldown'])
@pytest.mark.parametrize('burst', [1, 2, 8])
def test_due_newer_refusal_yields_to_first_download_but_keeps_bounded_retry(config, tmp_path, state, burst):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        original = seed(queue, state=state)
        for _ in range(burst):
            row = queue.claim('publisher', 300, pending_body_burst=burst)
            assert '/first/' in row['url']
            assert dict(queue.db.execute('SELECT * FROM articles WHERE id=?',
                                         (original['id'],)).fetchone()) == original
        retried = queue.claim('publisher', 300, pending_body_burst=burst)
        assert retried['id'] == original['id']
        current = dict(queue.db.execute('SELECT * FROM articles WHERE id=?',
                                        (original['id'],)).fetchone())
        for field in ('due', 'error_code', 'last_checked_at_utc', 'metadata_date_raw', 'body_sha256'):
            assert current[field] == original[field]
        assert current['attempts'] == 3 and current['lease_owner'] == queue.owner
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts['frontier_oldest_due_retry_dispatches'] == 1
        assert counts['frontier_access_refusal_fairness_deferrals'] == burst
    finally:
        queue.close()


@pytest.mark.parametrize('state', ['retry', 'retry_cooldown'])
def test_future_refusal_deadline_and_evidence_never_reset(config, tmp_path, state):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        original = seed(queue, pending=12, state=state, due=time.time() + 3600)
        for _ in range(12):
            assert '/first/' in queue.claim('publisher', 300, pending_body_burst=2)['url']
        assert queue.claim('publisher', 300, pending_body_burst=2) is None
        assert dict(queue.db.execute('SELECT * FROM articles WHERE id=?',
                                     (original['id'],)).fetchone()) == original
    finally:
        queue.close()


@pytest.mark.parametrize('error', ['http_502', 'transport_error', 'timeout'])
def test_transient_network_recovery_retains_latest_first_service(config, tmp_path, error):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        original = seed(queue, error=error)
        assert queue.claim('publisher', 300)['id'] == original['id']
        counts = queue.metrics.snapshot()['providers']['publisher']['counts']
        assert counts.get('frontier_access_refusal_fairness_deferrals', 0) == 0
    finally:
        queue.close()


def test_no_fresh_work_does_not_idle_a_due_refusal(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        original = seed(queue, pending=0)
        assert queue.claim('publisher', 300)['id'] == original['id']
        assert queue.claim('publisher', 300) is None
    finally:
        queue.close()
