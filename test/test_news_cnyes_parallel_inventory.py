"""Local history recovery progresses during a bounded accepted-batch scan."""
from contextlib import closing
import threading
import time

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg  # noqa: F401
from test_news_cnyes_history_evidence import (  # noqa: F401
    head, queue, seed_accepted_batch_for_history_page, seed_retained_work,
    seed_unindexed_nonzero_pages, source_payload,
)


def _jobs(queue, urls):
    return [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
            for url in urls]


@pytest.mark.parametrize('failed_last', [False, True])
def test_local_originals_are_validated_before_batch_frontier_finishes(queue, failed_last):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=failed_last)
    page2 = source_payload(range(31, 61), epoch=1388534400, page=2, total=61)
    for number in range(20):
        seed_accepted_batch_for_history_page(queue, urls[1], page2, batch_tag=f'a{number:02}')
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='z')
    before = _jobs(queue, urls)

    first = history.backfill(queue, limit=1, max_seconds=10)
    assert not first['accepted_body_batch_scan']['frontier_complete']
    assert first['unobserved_nonzero_page_scan']['queued'] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 1
    assert _jobs(queue, urls) == before

    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(20):
            history.backfill(resumed, limit=1, max_seconds=10)
            if head(resumed, urls[0])['state'] == 'list_complete':
                break
        assert head(resumed, urls[0])['state'] == 'list_complete'
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 3
        assert _jobs(resumed, urls) == before
        assert resumed.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                  (urls[2],)).fetchone()[0] == 'restored'


def test_forward_scan_wakes_candidate_after_page_inventory_passed_it(queue):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = _jobs(queue, urls)
    assert history.seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['complete']
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()

    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                            (relative,)).fetchone()
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'pending'
    for _ in range(5):
        history.backfill(queue, limit=8, max_seconds=10)
        if head(queue, urls[0])['state'] == 'list_complete':
            break
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert _jobs(queue, urls) == before


def test_forward_candidate_wakeup_rolls_back_with_batch_on_io_failure(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = _jobs(queue, urls)
    assert history.seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['complete']
    original = history._queue_late_accepted_candidate

    def fail_after_wakeup(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError('simulated interruption after candidate wake')

    with monkeypatch.context() as patch:
        patch.setattr(history, '_queue_late_accepted_candidate', fail_after_wakeup)
        result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert not result['complete']
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE parquet_path=?',
                                (relative,)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_local_retries WHERE parquet_path=?',
                            (relative,)).fetchone()
    assert _jobs(queue, urls) == before
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=0')
    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'pending'


def test_one_ready_local_validation_survives_slow_scan_but_stop_is_immediate(queue, monkeypatch):
    url, _ = seed_retained_work(queue, failure='http_429')
    before = _jobs(queue, [url])
    original = history.seed_accepted_body_batch_candidates

    def slow_scan(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_accepted_body_batch_candidates', slow_scan)
        result = history.backfill(queue, limit=8, max_seconds=.005)
    assert result['processed'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (url,)).fetchone()[0] == 'restored'
    assert _jobs(queue, [url]) == before

    another, _ = seed_retained_work(queue, failure='http_429', low=1388620800)
    stop = threading.Event()
    stop.set()
    assert history.backfill(queue, stop=stop, limit=8, max_seconds=.005)['processed'] == 0
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (another,)).fetchone()[0] == 'pending'


def test_old_queue_gets_retained_scope_index_on_owner_initialization(queue):
    url, _ = seed_retained_work(queue, failure='http_429')
    before = _jobs(queue, [url])
    with queue.db:
        queue.db.execute('DROP INDEX cnyes_history_retained_scope_page')
    sql = 'EXPLAIN QUERY PLAN SELECT 1 FROM cnyes_history_retained_work WHERE scope_url=? LIMIT 1'
    plan = [row[3] for row in queue.db.execute(sql, ('scope',))]
    assert any('SCAN cnyes_history_retained_work' in item for item in plan)

    history.initialize(queue)
    plan = [row[3] for row in queue.db.execute(sql, ('scope',))]
    assert any('SEARCH cnyes_history_retained_work' in item
               and 'cnyes_history_retained_scope_page' in item for item in plan)
    assert _jobs(queue, [url]) == before
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        plan = [row[3] for row in resumed.db.execute(sql, ('scope',))]
        assert any('cnyes_history_retained_scope_page' in item for item in plan)
        assert _jobs(resumed, [url]) == before
