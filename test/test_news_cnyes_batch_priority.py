"""Finite local hints accelerate the canonical scanner without source requests."""
from contextlib import closing
import hashlib
import json
from threading import Event

import pytest

from downloader import news_fulltext_cnyes_batch_priority as priority
from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_cnyes_priority import plan_bytes
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import NewsQueue
from test_news_cnyes_history_evidence import (  # noqa: F401
    cfg, queue, seed_accepted_batch_for_history_page, seed_unindexed_nonzero_pages, source_payload,
)


def _plan(queue, tmp_path, paths):
    source = queue.registry.sources['cnyes']
    plan = {'contract': 'cnyes_accepted_batch_priority_v1',
            'source_bulk_fingerprint': cnyes_bulk_fingerprint(source),
            'authorization_fingerprint': source.authorization_fingerprint,
            'inventory_sha256': 'a' * 64, 'batches': sorted(paths)}
    data = plan_bytes(plan)
    path = tmp_path / 'priority.json'
    path.write_bytes(data)
    return {'path': str(path), 'sha256': hashlib.sha256(data).hexdigest()}


def _two_batches(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    data = source_payload([61], epoch=1388534400, page=3, total=61)
    first, _ = seed_accepted_batch_for_history_page(queue, urls[2], data, batch_tag='a')
    last, _ = seed_accepted_batch_for_history_page(queue, urls[2], data, batch_tag='z')
    return urls, first, last


def test_priority_batch_runs_before_frontier_and_resumes_without_source_changes(queue, tmp_path):
    urls, first, last = _two_batches(queue)
    spec = _plan(queue, tmp_path, [last])
    jobs = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    seeded = priority.seed(queue, spec, max_seconds=10)
    assert seeded['state'] == 'complete' and seeded['enqueued'] == 1
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates').fetchone() is None
    scanned = history.seed_accepted_body_batch_candidates(queue, limit=1, max_seconds=10)
    assert not scanned['complete'] and scanned['checked'] == 0 and scanned['indexed'] == 1
    assert scanned['retried'] == 1
    assert history.maintenance_delay({'exhausted': False, 'accepted_body_batch_scan': scanned}) == 0
    assert queue.db.execute('SELECT parquet_path FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == last
    assert first != last and [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert priority.seed(resumed, spec, max_seconds=10)['state'] == 'complete'
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries').fetchone() is None
        assert [tuple(row) for row in resumed.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs


def test_priority_admission_preserves_existing_local_retry_and_skips_unregistered(queue, tmp_path):
    _, first, last = _two_batches(queue)
    marker = history.accepted_batch_scan_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_accepted_batch_local_retries VALUES (?,?,?,?,?)',
                         (marker, last, 'receipts/existing.json', 12345678900, 4))
        queue.db.execute('DELETE FROM accepted_batches WHERE path=?', (first,))
    before = tuple(queue.db.execute('SELECT * FROM cnyes_history_accepted_batch_local_retries').fetchone())
    result = priority.seed(queue, _plan(queue, tmp_path, [first, last]), max_seconds=10)
    assert (result['existing'], result['stale'], result['enqueued']) == (1, 1, 0)
    assert tuple(queue.db.execute('SELECT * FROM cnyes_history_accepted_batch_local_retries').fetchone()) == before


@pytest.mark.parametrize('problem', ['tampered', 'different_authorization', 'traversal', 'duplicate', 'checkpoint'])
def test_bad_plan_or_checkpoint_cannot_admit_priority_or_change_source(queue, tmp_path, problem):
    _, _, last = _two_batches(queue)
    spec = _plan(queue, tmp_path, [last])
    path = tmp_path / 'priority.json'
    if problem == 'tampered':
        path.write_bytes(path.read_bytes() + b' ')
    elif problem in ('different_authorization', 'traversal', 'duplicate'):
        plan = json.loads(path.read_bytes())
        if problem == 'different_authorization':
            plan['authorization_fingerprint'] = 'b' * 64
        elif problem == 'traversal':
            plan['batches'] = ['bodies/../../other.parquet']
        else:
            plan['batches'] = [last, last]
        data = plan_bytes(plan)
        path.write_bytes(data)
        spec['sha256'] = hashlib.sha256(data).hexdigest()
    else:
        scan = history.accepted_batch_scan_key(queue.registry.sources['cnyes'])
        marker = priority._PREFIX + history.digest((spec['sha256'], scan))
        with queue.db:
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, '{"version":2}'))
    jobs = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = priority.seed(queue, spec, max_seconds=10)
    assert result['state'] == 'blocked'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries').fetchone() is None
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs


def test_stop_and_restart_resume_priority_without_duplicate_admission(queue, tmp_path):
    _, first, last = _two_batches(queue)
    spec = _plan(queue, tmp_path, [first, last])
    stop = Event()
    stop.set()
    assert priority.seed(queue, spec, stop=stop, max_seconds=10)['slice_steps'] == 0
    assert priority.seed(queue, spec, limit=1, max_seconds=10)['state'] == 'scanning'
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        result = priority.seed(resumed, spec, limit=1, max_seconds=10)
        assert result['state'] == 'complete' and result['enqueued'] == 2
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_accepted_batch_local_retries').fetchone()[0] == 2


def test_malformed_runtime_spec_blocks_only_priority(queue, tmp_path):
    path = tmp_path / 'plan.json'
    path.write_text('{}')
    result = priority.seed(queue, {'path': str(path)})
    assert result['state'] == 'blocked' and result['problem'] == 'invalid_or_unavailable_plan'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries').fetchone() is None


def test_canonical_cycle_admits_and_scans_priority_without_publisher_calls(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner
    from test_news_fulltext import http_fixture

    cfg['adaptive_rate'] = {'enabled': False}
    http, session = http_fixture(cfg, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    try:
        _, _, last = _two_batches(queue)
        cfg['cnyes_accepted_batch_priority_plan'] = _plan(queue, tmp_path, [last])
        jobs = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
        original_backfill = history.backfill
        calls = []

        def local_backfill(*args, **kwargs):
            result = original_backfill(*args, **kwargs)
            calls.append(result)
            http.stop.set()
            return result

        monkeypatch.setattr(history, 'backfill', local_backfill)
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=4, defer_final_status=True)
        assert calls and calls[0]['accepted_body_batch_scan']['retried'] == 1
        assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                                (last,)).fetchone()
        assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs
        assert session.calls == []
    finally:
        http.close()
        queue.close()
