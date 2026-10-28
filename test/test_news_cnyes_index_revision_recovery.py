"""Offline cases for mutable publisher pagination and concurrent requests."""
from datetime import datetime
import time

from downloader.news_fulltext import accept_discovery
from downloader.news_fulltext_bulk import cnyes_scope
from downloader.news_fulltext_cnyes_index import progress, request_url
from test_cnyes_news_bulk import cfg, registry_http
from test_news_cnyes_public_index import DAY, accept, index_payload, queue, register, response


def complete(queue):
    accept(queue, index_payload(ids=range(1, 31), total=61))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=61), page=2)
    accept(queue, index_payload(ids=(61,), page=3, total=61), page=3)


def job(queue, page):
    url = request_url(queue.registry.sources['cnyes'], DAY, page)
    return dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())


def test_same_total_new_ids_rechecks_all_pages_then_bookends_first(queue):
    complete(queue)
    source = queue.registry.sources['cnyes']
    before = queue.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0]
    accept(queue, index_payload(ids=range(2, 32), total=61))
    status = progress(queue.db, source)
    assert status['dates_all_reported_pages_observed'] == 0
    assert status['dates_reconciling_page_membership'] == 1
    assert job(queue, 2)['due'] <= time.time() and job(queue, 3)['due'] <= time.time()
    accept(queue, index_payload(ids=range(32, 62), page=2, total=61), page=2)
    accept(queue, index_payload(ids=(62,), page=3, total=61), page=3)
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 0
    assert job(queue, 1)['due'] <= time.time()
    accept(queue, index_payload(ids=range(2, 32), total=61))
    status = progress(queue.db, source)
    assert status['dates_all_reported_pages_observed'] == 1
    assert status['dates_reconciling_page_membership'] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] >= before + 3


def test_earlier_inflight_result_cannot_confirm_new_revision(queue):
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch
    from downloader.news_fulltext_cnyes_index import parse_index
    complete(queue)
    source = queue.registry.sources['cnyes']
    task = register(queue, 2)
    prepare_dispatch(queue, task)
    old_head = tuple(queue.db.execute('SELECT * FROM cnyes_public_index_heads WHERE page=2').fetchone())
    accept(queue, index_payload(ids=range(2, 32), total=61))
    stale = response(index_payload(ids=range(31, 61), page=2, total=61), task['url'])
    accept_discovery(queue, task, stale, parse_index(stale, source)[2], [], [])
    assert tuple(queue.db.execute('SELECT * FROM cnyes_public_index_heads WHERE page=2').fetchone()) == old_head
    assert job(queue, 2)['due'] <= time.time()
    assert progress(queue.db, source)['dates_reconciling_page_membership'] == 1


def test_later_page_change_rechecks_first_before_other_pages(queue):
    complete(queue)
    accept(queue, index_payload(ids=range(32, 62), page=2, total=61), page=2)
    assert job(queue, 1)['due'] <= time.time()
    assert progress(queue.db, queue.registry.sources['cnyes'])['dates_all_reported_pages_observed'] == 0
    accept(queue, index_payload(ids=range(2, 32), total=61))
    assert job(queue, 2)['due'] <= time.time() and job(queue, 3)['due'] <= time.time()


def test_shrunk_list_retires_heads_only_after_full_checked_round(queue):
    complete(queue)
    source = queue.registry.sources['cnyes']
    original = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_observations WHERE page=3')]
    accept(queue, index_payload(ids=range(1, 31), total=60))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=60), page=2)
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 0
    accept(queue, index_payload(ids=range(1, 31), total=60))
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads WHERE page=3').fetchone()[0] == 0
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_observations WHERE page=3')] == original
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (job(queue, 3)['url'],))
    assert job(queue, 3)['url'] not in {row['url'] for row in queue.discovery_candidates(source, 64)}


def test_round_and_retry_deadlines_survive_restart(cfg, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    registry, _ = registry_http(cfg, tmp_path)
    owner = NewsQueue(tmp_path, registry)
    complete(owner)
    source = registry.sources['cnyes']
    url = request_url(source, DAY, 2)
    with owner.db:
        owner.db.execute('UPDATE discovery SET due=?,attempts=3,error_code=? WHERE url=?',
                         (time.time() + 7200, 'discovery_http_429', url))
    before = job(owner, 2)
    accept(owner, index_payload(ids=range(2, 32), total=61))
    assert job(owner, 2) == before
    owner.close()
    restored = NewsQueue(tmp_path, registry)
    try:
        assert job(restored, 2) == before
        assert progress(restored.db, source)['dates_reconciling_page_membership'] == 1
        scope = cnyes_scope(request_url(source, DAY))
        assert restored.db.execute('SELECT phase FROM cnyes_index_revision_rounds WHERE scope_url=?', (scope,)).fetchone()
    finally:
        restored.close()


def test_verify_waits_for_other_same_scope_inflight_and_keeps_retry_budget(queue):
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch
    from downloader.news_fulltext_cnyes_index import parse_index
    complete(queue)
    source = queue.registry.sources['cnyes']
    accept(queue, index_payload(ids=range(1, 31), total=60))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=60), page=2)
    task = register(queue, 1)
    prepare_dispatch(queue, task)
    task['_cnyes_revision_scope_idle'] = False
    final = response(index_payload(ids=range(1, 31), total=60), task['url'])
    accept_discovery(queue, task, final, parse_index(final, source)[2], [], [])
    assert progress(queue.db, source)['dates_reconciling_page_membership'] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads WHERE page=3').fetchone()[0] == 1
    assert job(queue, 1)['due'] > time.time() + source.discovery_interval_seconds - 5
    accept(queue, index_payload(ids=range(1, 31), total=60))
    assert progress(queue.db, source)['dates_reconciling_page_membership'] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads WHERE page=3').fetchone()[0] == 0


def test_shrunk_page_reopens_when_first_page_later_advertises_it(queue, monkeypatch):
    complete(queue)
    source = queue.registry.sources['cnyes']
    accept(queue, index_payload(ids=range(1, 31), total=60))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=60), page=2)
    accept(queue, index_payload(ids=range(1, 31), total=60))
    assert queue.db.execute('SELECT count(*) FROM cnyes_index_revision_retired WHERE page=3 AND active=1').fetchone()[0] == 1
    checked = datetime.fromisoformat(job(queue, 3)['last_checked_at_utc']).timestamp()
    start_monotonic = time.monotonic()
    before = time.time()
    accept(queue, index_payload(ids=range(1, 31), total=61))
    after = time.time()
    elapsed = time.monotonic() - start_monotonic
    assert queue.db.execute('SELECT count(*) FROM cnyes_index_revision_retired WHERE page=3 AND active=1').fetchone()[0] == 0
    reopened = job(queue, 3)
    # The first retry uses max(current wall clock, the earlier successful
    # check), not the publisher cadence. Allow only the time actually spent
    # executing this acceptance plus timestamp rounding; a wall-clock step
    # can leave the earlier successful check slightly in the future.
    assert reopened['due'] <= max(before, after, checked) + elapsed + 1e-6
    with monkeypatch.context() as clock:
        clock.setattr(time, 'time', lambda: max(after, reopened['due']))
        assert reopened['url'] in {row['url'] for row in queue.discovery_candidates(source, 64)}
    accept(queue, index_payload(ids=range(31, 61), page=2, total=61), page=2)
    accept(queue, index_payload(ids=(61,), page=3, total=61), page=3)
    accept(queue, index_payload(ids=range(1, 31), total=61))
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_heads WHERE page=3').fetchone()[0] == 1


def test_failed_old_page_is_never_reset_when_retired_or_reactivated(queue):
    complete(queue)
    source = queue.registry.sources['cnyes']
    url = request_url(source, DAY, 3)
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=?,attempts=3,error_code=? WHERE url=?',
                         (time.time() + 7200, 'discovery_http_429', url))
    before = job(queue, 3)
    accept(queue, index_payload(ids=range(1, 31), total=60))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=60), page=2)
    accept(queue, index_payload(ids=range(1, 31), total=60))
    assert job(queue, 3) == before
    accept(queue, index_payload(ids=range(1, 31), total=61))
    assert job(queue, 3) == before
    assert progress(queue.db, source)['dates_reconciling_page_membership'] == 1


def test_anchor_phase_does_not_dispatch_due_other_pages_or_spin(queue):
    complete(queue)
    source = queue.registry.sources['cnyes']
    accept(queue, index_payload(ids=range(32, 62), page=2, total=61), page=2)
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=? WHERE url=?', (time.time() + 120, job(queue, 1)['url']))
        queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (job(queue, 3)['url'],))
        queue.db.execute('UPDATE articles SET due=?', (time.time() + 120,))
    assert job(queue, 3)['url'] not in {row['url'] for row in queue.discovery_candidates(source, 64)}
    assert queue.next_wake_delay({}, maximum=60) > 1


def test_duplicate_current_pages_do_not_finish_round(queue):
    complete(queue)
    source = queue.registry.sources['cnyes']
    accept(queue, index_payload(ids=range(2, 32), total=61))
    # A moved page can temporarily equal another current head; its original
    # evidence is retained but it cannot make the completed round valid.
    accept(queue, index_payload(ids=range(2, 32), page=2, total=61), page=2)
    accept(queue, index_payload(ids=(62,), page=3, total=61), page=3)
    accept(queue, index_payload(ids=range(2, 32), total=61))
    assert progress(queue.db, source)['dates_reconciling_page_membership'] == 1
    assert job(queue, 2)['due'] > time.time() + source.discovery_interval_seconds - 5
    accept(queue, index_payload(ids=range(32, 62), page=2, total=61), page=2)
    accept(queue, index_payload(ids=range(2, 32), total=61))
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 1


def test_prior_revision_signature_not_current_duplicate_does_not_reject_moved_page(queue):
    complete(queue)
    accept(queue, index_payload(ids=range(101, 131), total=61))
    accept(queue, index_payload(ids=range(1, 31), page=2, total=61), page=2)
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_observations WHERE page=2').fetchone()[0] == 2


def test_first_page_change_during_verify_starts_a_fenced_new_round(queue):
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch
    from downloader.news_fulltext_cnyes_index import parse_index
    complete(queue)
    source = queue.registry.sources['cnyes']
    accept(queue, index_payload(ids=range(2, 32), total=61))
    old_task = register(queue, 3)
    prepare_dispatch(queue, old_task)
    accept(queue, index_payload(ids=range(32, 62), page=2, total=61), page=2)
    accept(queue, index_payload(ids=(62,), page=3, total=61), page=3)
    accept(queue, index_payload(ids=range(3, 33), total=61))
    stale = response(index_payload(ids=(62,), page=3, total=61), old_task['url'])
    accept_discovery(queue, old_task, stale, parse_index(stale, source)[2], [], [])
    assert progress(queue.db, source)['dates_reconciling_page_membership'] == 1
    # The new round retains the previous successful-check retry budget.
    assert job(queue, 2)['due'] > time.time() and job(queue, 3)['due'] > time.time()


def test_final_check_revalidates_each_confirmed_head(queue):
    complete(queue)
    source = queue.registry.sources['cnyes']
    old_sha = queue.db.execute('SELECT response_sha256 FROM cnyes_public_index_heads WHERE page=2').fetchone()[0]
    accept(queue, index_payload(ids=range(2, 32), total=61))
    accept(queue, index_payload(ids=range(32, 62), page=2, total=61), page=2)
    accept(queue, index_payload(ids=(62,), page=3, total=61), page=3)
    with queue.db:
        queue.db.execute('UPDATE cnyes_public_index_heads SET response_sha256=? WHERE page=2', (old_sha,))
    accept(queue, index_payload(ids=range(2, 32), total=61))
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 0
    assert progress(queue.db, source)['dates_reconciling_page_membership'] == 1
    assert job(queue, 2)['due'] > time.time()  # Previous retry budget is retained.


def test_legacy_same_total_membership_change_is_recovered_once_on_startup(cfg, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    registry, _ = registry_http(cfg, tmp_path)
    owner = NewsQueue(tmp_path, registry)
    complete(owner)
    source = registry.sources['cnyes']
    accept(owner, index_payload(ids=range(2, 32), total=61))
    with owner.db:
        owner.db.execute('DELETE FROM cnyes_index_revision_rounds')
        owner.db.execute('DELETE FROM cnyes_index_revision_checks')
        owner.db.execute("DELETE FROM queue_settings WHERE key LIKE 'cnyes_index_revision_migration_v1:%'")
        for page in (1, 2, 3):
            owner.db.execute('UPDATE discovery SET due=? WHERE url=?', (time.time() + 86400, job(owner, page)['url']))
    owner.close()
    restored = NewsQueue(tmp_path, registry)
    try:
        assert progress(restored.db, source)['dates_reconciling_page_membership'] == 1
        assert job(restored, 1)['due'] <= time.time()
        accept(restored, index_payload(ids=range(2, 32), total=61))
        accept(restored, index_payload(ids=range(32, 62), page=2, total=61), page=2)
        accept(restored, index_payload(ids=(62,), page=3, total=61), page=3)
        accept(restored, index_payload(ids=range(2, 32), total=61))
        assert progress(restored.db, source)['dates_reconciling_page_membership'] == 0
    finally:
        restored.close()
    reopened = NewsQueue(tmp_path, registry)
    try:
        assert progress(reopened.db, source)['dates_reconciling_page_membership'] == 0
    finally:
        reopened.close()


def test_malformed_revision_blocks_dispatch_and_cannot_forge_completion(queue):
    complete(queue)
    source = queue.registry.sources['cnyes']
    accept(queue, index_payload(ids=range(2, 32), total=61))
    with queue.db:
        queue.db.execute("UPDATE cnyes_index_revision_rounds SET anchor_ids_sha256='invalid'")
        queue.db.execute('UPDATE articles SET due=?', (time.time() + 120,))
    assert not {job(queue, page)['url'] for page in (1, 2, 3)}.intersection(
        row['url'] for row in queue.discovery_candidates(source, 64))
    assert queue.next_wake_delay({}, maximum=60) > 1
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 0
    assert progress(queue.db, source)['dates_reconciling_page_membership'] == 1


def test_owner_cycle_dispatches_parallel_page_checks_then_final_first(cfg, tmp_path, monkeypatch):
    """Real queue/owner/executor, with a local in-memory publisher only."""
    import threading
    from urllib.parse import parse_qs, urlsplit
    from downloader.news_fulltext import run_cycle
    from downloader.news_fulltext_cnyes_index import parse_index
    from downloader.news_fulltext_queue import NewsQueue
    from test_news_fulltext import http_fixture

    cfg['workers'] = 3
    http, session = http_fixture(cfg, tmp_path, {})
    owner = NewsQueue(tmp_path, http.registry)
    barrier = threading.Barrier(2, timeout=5)
    seen = []
    source = http.registry.sources['cnyes']
    try:
        complete(owner)
        accept(owner, index_payload(ids=range(2, 32), total=61))
        http.parallelism['cnyes'] = 3
        monkeypatch.setattr('downloader.news_fulltext.seed_discovery', lambda *_: None)
        monkeypatch.setattr('downloader.news_fulltext.refresh_disk_admission', lambda *_: False)

        def fake_fetch(_http, task):
            page = int(parse_qs(urlsplit(task['url']).query)['page'][0])
            seen.append((page, task.get('_cnyes_revision_round'), task.get('_cnyes_revision_phase')))
            if page in (2, 3):
                barrier.wait()
            ids = range(2, 32) if page == 1 else range(32, 62) if page == 2 else (62,)
            r = response(index_payload(ids=ids, page=page, total=61), task['url'])
            return r, parse_index(r, source)[2], [], []

        monkeypatch.setattr('downloader.news_fulltext.fetch_discovery', fake_fetch)
        run_cycle(owner, http, cfg, max_articles=0, max_discovery_requests=3, max_run_seconds=10)
        assert sorted(page for page, _, _ in seen) == [1, 2, 3]
        assert {phase for page, _, phase in seen if page > 1} == {'pages'}
        assert next(phase for page, _, phase in seen if page == 1) == 'verify'
        assert len({round_id for _, round_id, _ in seen}) == 1
        assert progress(owner.db, source)['dates_all_reported_pages_observed'] == 1
        assert not session.calls
    finally:
        http.close()
        owner.close()
