"""A slow local inventory must not indefinitely starve scope repair."""
from contextlib import closing
from types import SimpleNamespace
import threading
import time

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader.news_fulltext_bulk import cnyes_scope, cnyes_url
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import BASE, cfg  # noqa: F401
from test_news_cnyes_history_evidence import (  # noqa: F401
    capture, legacy_missing_page, queue, seed_unindexed_nonzero_pages, source_payload,
)


@pytest.mark.parametrize('failure', [None, 'http_429'])
def test_slow_scans_cannot_starve_repair_across_restart(queue, monkeypatch, failure):
    _, missing = legacy_missing_page(queue, failure=failure)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    original = history.seed_accepted_body_batch_candidates

    def slow_scan(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_accepted_body_batch_candidates', slow_scan)
        first = history.backfill(queue, max_seconds=.005)
        assert first['repair_sweep_pending'] and first['evidence_repair']['scopes_checked'] == 0
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone()) == before
        queue.close()
        with closing(NewsQueue(queue.root, queue.registry)) as resumed:
            results = [history.backfill(resumed, max_seconds=.005) for _ in range(3)]
            assert any(result['evidence_repair']['scopes_checked'] > 0 for result in results)
            after = dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
            if failure:
                assert after == before
            else:
                assert after['due'] < before['due'] and after['etag'] is None
                assert after['error_code'] is None and after['attempts'] == 0


def test_prioritized_repair_makes_one_step_after_slow_seek(queue, monkeypatch):
    _, missing = legacy_missing_page(queue)
    clock = iter((0.0, 2.0, 3.0, 4.0))
    fake_time = SimpleNamespace(monotonic=lambda: next(clock, 5.0), time=time.time)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'time', fake_time)
        result = history.repair_incomplete_scopes(queue, max_seconds=1, allow_one_after_timeout=True)
    assert result['scopes_checked'] == 1
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (missing,)).fetchone()[0] < 9e9


def test_stop_and_zero_budget_do_not_advance_a_prioritized_repair(queue, monkeypatch):
    _, missing = legacy_missing_page(queue)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    original = history.seed_accepted_body_batch_candidates

    def slow_scan(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_accepted_body_batch_candidates', slow_scan)
        assert history.backfill(queue, max_seconds=.005)['repair_sweep_pending']
    stop = threading.Event()
    stop.set()
    assert history.backfill(queue, stop=stop, max_seconds=.005)['evidence_repair']['scopes_checked'] == 0
    assert history.backfill(queue, max_seconds=0)['evidence_repair']['scopes_checked'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone()) == before
    assert history.backfill(queue, max_seconds=10)['evidence_repair']['scopes_checked'] >= 1
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (missing,)).fetchone()[0] < before['due']


def test_unchanged_failed_scope_does_not_spin_during_repeated_slow_scans(queue, monkeypatch):
    _, missing = legacy_missing_page(queue, failure='http_429')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    for seed in (history.reconcile_legacy_request_mismatches, history.reconcile_legacy_unavailable,
                 history.seed_unobserved_empty_scopes, history.seed_accepted_body_batch_candidates,
                 history.seed_unobserved_nonzero_pages, history.seed_overflow_retained):
        assert seed(queue, limit=128, max_seconds=10)['complete']
    original = history.seed_accepted_body_batch_candidates

    def slow_scan(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_accepted_body_batch_candidates', slow_scan)
        results = [history.backfill(queue, max_seconds=.005) for _ in range(8)]
    assert sum(result['evidence_repair']['scopes_checked'] for result in results) == 1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone()) == before


def test_repair_revisits_scope_when_last_local_candidate_settles(queue, monkeypatch):
    urls = seed_unindexed_nonzero_pages(queue)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    original = history.seed_accepted_body_batch_candidates

    def slow_scan(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_accepted_body_batch_candidates', slow_scan)
        for _ in range(10):
            history.backfill(queue, limit=1, max_seconds=.005)
            if queue.db.execute('SELECT due FROM discovery WHERE url=?', (urls[2],)).fetchone()[0] < before['due']:
                break
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    assert after['due'] < before['due'] and after['etag'] is None
    assert after['error_code'] is None and after['attempts'] == 0


def test_new_incomplete_scope_invalidates_finished_repair_sweep(queue, monkeypatch):
    _, old_missing = legacy_missing_page(queue, failure='http_429')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (old_missing,)).fetchone())
    for seed in (history.reconcile_legacy_request_mismatches, history.reconcile_legacy_unavailable,
                 history.seed_unobserved_empty_scopes, history.seed_accepted_body_batch_candidates,
                 history.seed_unobserved_nonzero_pages, history.seed_overflow_retained):
        assert seed(queue, limit=128, max_seconds=10)['complete']
    original = history.seed_accepted_body_batch_candidates

    def slow_scan(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_accepted_body_batch_candidates', slow_scan)
        initial = [history.backfill(queue, max_seconds=.005) for _ in range(4)]
        assert sum(result['evidence_repair']['scopes_checked'] for result in initial) == 1
        low = 1388620800
        first = cnyes_url(BASE, start=low, end=low + 100)
        missing = cnyes_url(BASE, 2, start=low, end=low + 100)
        capture(queue, first, source_payload(range(1, 31), epoch=low, total=31))
        with queue.db:
            queue.db.execute("UPDATE discovery SET due=9e9,etag='old',error_code=NULL,attempts=0 WHERE url=?",
                             (missing,))
        for _ in range(4):
            history.backfill(queue, max_seconds=.005)
            if queue.db.execute('SELECT due FROM discovery WHERE url=?', (missing,)).fetchone()[0] < 9e9:
                break
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (missing,)).fetchone()[0] < 9e9
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (old_missing,)).fetchone()) == before


def test_restart_resweeps_scopes_changed_behind_durable_cursor(queue):
    first, missing = legacy_missing_page(queue)
    source = queue.registry.sources['cnyes']
    low = 1388620800
    second = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, second, source_payload(range(1, 31), epoch=low, total=31))
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_local_inventory_scopes VALUES (?,?,?,?,?)',
            (cnyes_scope(first), cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
             'scanning', '2020-01-01T00:00:00+00:00'))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    assert history.repair_incomplete_scopes(queue, limit=1, max_seconds=10)['scopes_checked'] == 1
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_local_inventory_scopes SET scan_state='ready' WHERE scope_url=?",
                         (cnyes_scope(first),))
    queue.close()

    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(12):
            history.backfill(resumed, limit=1, max_seconds=10)
            if resumed.db.execute('SELECT due FROM discovery WHERE url=?', (missing,)).fetchone()[0] < before['due']:
                break
        after = dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
        assert after['due'] < before['due'] and after['etag'] is None
        assert after['error_code'] is None and after['attempts'] == 0


def test_completed_sweep_is_revisited_after_existing_idle_interval(queue, monkeypatch):
    _, missing = legacy_missing_page(queue, failure='http_429')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    for seed in (history.reconcile_legacy_request_mismatches, history.reconcile_legacy_unavailable,
                 history.seed_unobserved_empty_scopes, history.seed_accepted_body_batch_candidates,
                 history.seed_unobserved_nonzero_pages, history.seed_overflow_retained):
        assert seed(queue, limit=128, max_seconds=10)['complete']
    original = history.seed_accepted_body_batch_candidates
    offset = [0.0]
    clock = SimpleNamespace(time=time.time, monotonic=lambda: time.monotonic() + offset[0])

    def slow_scan(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'time', clock)
        patch.setattr(history, 'seed_accepted_body_batch_candidates', slow_scan)
        first = [history.backfill(queue, max_seconds=.005) for _ in range(4)]
        assert sum(result['evidence_repair']['scopes_checked'] for result in first) == 1
        offset[0] += 301
        second = [history.backfill(queue, max_seconds=.005) for _ in range(4)]
    assert sum(result['evidence_repair']['scopes_checked'] for result in second) == 1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone()) == before


def test_explicit_new_sweep_invalidates_cached_completion(queue):
    legacy_missing_page(queue, failure='http_429')
    low = 1388620800
    first = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, first, source_payload(range(1, 31), epoch=low, total=31))
    for _ in range(5):
        history.backfill(queue, limit=128, max_seconds=10)
        if getattr(queue, '_cnyes_history_repair_covered', None):
            break
    assert queue._cnyes_history_repair_covered
    assert history.repair_incomplete_scopes(queue, limit=1, max_seconds=10)['scopes_checked'] == 1
    assert queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (history.REPAIR_CURSOR,)).fetchone()
    for _ in range(5):
        history.backfill(queue, limit=1, max_seconds=10)
        if not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (history.REPAIR_CURSOR,)).fetchone():
            break
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (history.REPAIR_CURSOR,)).fetchone()
