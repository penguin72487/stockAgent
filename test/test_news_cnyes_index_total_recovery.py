"""A changed all-index total must not strand previously completed pages."""
from dataclasses import replace
import json
import time

import pytest

from downloader.news_fulltext import accept_discovery
from downloader.news_fulltext_bulk import cnyes_scope
from downloader.news_fulltext_cnyes_index import _reconcile_index_totals, progress, request_url
from test_cnyes_news_bulk import cfg, registry_http
from test_news_cnyes_public_index import DAY, accept, index_payload, queue, response


def _job(queue, page):
    url = request_url(queue.registry.sources['cnyes'], DAY, page)
    return dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())


def _complete_three_pages(queue):
    accept(queue, index_payload(ids=range(1, 31), total=61))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=61), page=2)
    accept(queue, index_payload(ids=(61,), page=3, total=61), page=3)


def test_first_page_total_change_rechecks_each_previously_observed_page(queue):
    _complete_three_pages(queue)
    source = queue.registry.sources['cnyes']
    with queue.db:
        for page in (2, 3):
            queue.db.execute('UPDATE discovery SET etag=?,last_modified=? WHERE url=?',
                             ('old-etag', 'old-clock', request_url(source, DAY, page)))
    old3 = _job(queue, 3)
    old_evidence = queue.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0]
    accept(queue, index_payload(ids=range(1, 31), total=62))
    second = _job(queue, 2)
    assert second['due'] <= time.time()
    assert second['etag'] is None and second['last_modified'] is None
    assert second['attempts'] == 0 and second['error_code'] is None
    assert _job(queue, 3)['due'] <= time.time()
    assert _job(queue, 3)['etag'] is None and _job(queue, 3)['last_modified'] is None
    assert _job(queue, 3)['attempts'] == old3['attempts']
    assert second['url'] in {row['url'] for row in queue.discovery_candidates(source, 64)}
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 0
    accept(queue, index_payload(ids=range(31, 61), page=2, total=62), page=2)
    assert _job(queue, 3)['due'] <= time.time()
    accept(queue, index_payload(ids=(61, 62), page=3, total=62), page=3)
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 0
    accept(queue, index_payload(ids=range(1, 31), total=62))
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == old_evidence + 3


def test_changed_total_never_shortens_existing_failed_page_deadline(queue):
    _complete_three_pages(queue)
    source = queue.registry.sources['cnyes']
    url = request_url(source, DAY, 2)
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=?,attempts=3,error_code=?,etag=? WHERE url=?',
                         (time.time() + 7200, 'discovery_http_429', 'retained-etag', url))
    before = _job(queue, 2)
    accept(queue, index_payload(ids=range(1, 31), total=62))
    assert _job(queue, 2) == before
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 0


def test_smaller_last_page_preserves_contradictory_head_and_rechecks_first(queue):
    _complete_three_pages(queue)
    source = queue.registry.sources['cnyes']
    old = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_observations WHERE page=3')]
    old_head = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_heads WHERE page=3')]
    accept(queue, index_payload(ids=range(1, 31), total=60))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=60), page=2)
    assert progress(queue.db, source)['dates_all_reported_pages_observed'] == 0
    assert progress(queue.db, source)['dates_with_changed_source_total'] == 1
    assert _job(queue, 1)['due'] <= time.time()
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_observations WHERE page=3')] == old
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_heads WHERE page=3')] == old_head


def test_changed_later_page_rechecks_first_then_other_pages(queue):
    _complete_three_pages(queue)
    accept(queue, index_payload(ids=range(31, 61), page=2, total=62), page=2)
    assert _job(queue, 1)['due'] <= time.time()
    assert _job(queue, 3)['due'] > time.time() + 86400
    accept(queue, index_payload(ids=range(1, 31), total=62))
    assert _job(queue, 3)['due'] <= time.time()


def test_identical_total_refresh_does_not_requeue_other_pages(queue):
    _complete_three_pages(queue)
    before = [_job(queue, page) for page in (2, 3)]
    accept(queue, index_payload(ids=range(1, 31), total=61))
    assert [_job(queue, page) for page in (2, 3)] == before


def test_repeated_conflict_uses_persisted_capped_backoff(queue):
    _complete_three_pages(queue)
    source = queue.registry.sources['cnyes']
    accept(queue, index_payload(ids=range(1, 31), total=62))
    assert _job(queue, 2)['due'] <= time.time()
    # The exact next page remains on the old publisher total, so first page
    # must be rechecked; an identical first response does not reset its budget.
    accept(queue, index_payload(ids=range(31, 61), page=2, total=61), page=2)
    assert _job(queue, 1)['due'] <= time.time()
    accept(queue, index_payload(ids=range(1, 31), total=62))
    assert _job(queue, 2)['due'] > time.time() + source.discovery_interval_seconds - 5
    rows = queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'cnyes_index_total_recheck_v1:%'").fetchall()
    assert any(json.loads(row[0])['attempts'] == 2 for row in rows)


def test_reconciliation_is_idempotent_and_survives_queue_reopen(cfg, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    registry, _ = registry_http(cfg, tmp_path)
    owner = NewsQueue(tmp_path, registry)
    _complete_three_pages(owner)
    source = registry.sources['cnyes']
    accept(owner, index_payload(ids=range(1, 31), total=62))
    before = _job(owner, 2)
    scope = cnyes_scope(request_url(source, DAY))
    with owner.db:
        _reconcile_index_totals(owner, source, scope, 1)
    assert _job(owner, 2) == before
    owner.close()
    other = NewsQueue(tmp_path, registry)
    try:
        assert _job(other, 2) == before
        accept(other, index_payload(ids=range(31, 61), page=2, total=62), page=2)
        assert _job(other, 3)['due'] <= time.time()
    finally:
        other.close()


def test_not_modified_page_can_continue_an_existing_reconciliation(queue):
    from downloader.news_fulltext_cnyes_revision import prepare_dispatch
    _complete_three_pages(queue)
    source = queue.registry.sources['cnyes']
    accept(queue, index_payload(ids=range(1, 31), total=62))
    task = _job(queue, 2)
    prepare_dispatch(queue, task)
    task['_cnyes_revision_scope_idle'] = True
    not_modified = replace(response(index_payload(), task['url']), status=304, body=b'')
    accept_discovery(queue, task, not_modified, [], [], [])
    assert _job(queue, 1)['due'] <= time.time()


def test_new_proved_total_change_gets_new_round_after_old_round_completes(queue):
    _complete_three_pages(queue)
    accept(queue, index_payload(ids=range(1, 31), total=62))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=62), page=2)
    accept(queue, index_payload(ids=(61, 62), page=3, total=62), page=3)
    accept(queue, index_payload(ids=range(1, 31), total=62))
    assert _job(queue, 2)['due'] > time.time() + 86400
    accept(queue, index_payload(ids=range(1, 31), total=63))
    assert _job(queue, 2)['due'] <= time.time()


def test_startup_detects_preexisting_mixed_totals_without_resetting_other_work(cfg, tmp_path):
    from downloader.news_fulltext_queue import NewsQueue
    registry, _ = registry_http(cfg, tmp_path)
    owner = NewsQueue(tmp_path, registry)
    _complete_three_pages(owner)
    old_page2 = _job(owner, 2)
    old_page3 = _job(owner, 3)
    accept(owner, index_payload(ids=range(1, 31), total=62))
    # Simulate the old implementation: inconsistent heads, successful old
    # page with its original deadline, and no new repair markers.
    with owner.db:
        owner.db.execute("DELETE FROM queue_settings WHERE key LIKE 'cnyes_index_total_reconcil%'")
        owner.db.execute("DELETE FROM queue_settings WHERE key LIKE 'cnyes_index_total_recheck_v1:%'")
        owner.db.execute('DELETE FROM cnyes_index_revision_rounds')
        owner.db.execute('DELETE FROM cnyes_index_revision_checks')
        for old in (old_page2, old_page3):
            owner.db.execute('UPDATE discovery SET due=?,etag=?,last_modified=? WHERE url=?',
                             (old['due'], old['etag'], old['last_modified'], old['url']))
    observations = [tuple(row) for row in owner.db.execute('SELECT * FROM cnyes_public_index_observations')]
    other = _job(owner, 3)
    owner.close()
    restored = NewsQueue(tmp_path, registry)
    try:
        assert _job(restored, 2)['due'] <= time.time()
        assert _job(restored, 3) == other
        assert [tuple(row) for row in restored.db.execute('SELECT * FROM cnyes_public_index_observations')] == observations
    finally:
        restored.close()


def test_invalid_persisted_retry_budget_fails_closed(queue):
    _complete_three_pages(queue)
    accept(queue, index_payload(ids=range(1, 31), total=62))
    accept(queue, index_payload(ids=range(31, 61), page=2, total=61), page=2)
    with queue.db:
        rows = queue.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'cnyes_index_total_recheck_v1:%'").fetchall()
        second_key = next(row['key'] for row in rows if row['key'].count(':') == 1
                          and json.loads(row['value'])['url'] == _job(queue, 2)['url'])
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('{}', second_key))
    accept(queue, index_payload(ids=range(1, 31), total=62))
    assert _job(queue, 2)['due'] > time.time() + 86400


def test_repair_and_index_head_commit_atomically_after_exception(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_index as index
    _complete_three_pages(queue)
    old_heads = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_heads')]
    old_observations = [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_observations')]
    old_page2 = _job(queue, 2)
    original = index._schedule_index_total_recheck
    def stop(*args, **kwargs):
        raise RuntimeError('injected_failure')
    monkeypatch.setattr(index, '_schedule_index_total_recheck', stop)
    with pytest.raises(RuntimeError, match='injected_failure'):
        accept(queue, index_payload(ids=range(1, 31), total=62))
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_heads')] == old_heads
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM cnyes_public_index_observations')] == old_observations
    assert _job(queue, 2) == old_page2
    monkeypatch.setattr(index, '_schedule_index_total_recheck', original)
    # The retained original/receipt from the failed transaction is reused.
    accept(queue, index_payload(ids=range(1, 31), total=62))
    assert _job(queue, 2)['due'] <= time.time()


def test_total_reconciliation_does_not_change_endpoint_cooldown(queue):
    from downloader.news_fulltext import fail_discovery
    from downloader.news_fulltext_policy import NewsError
    _complete_three_pages(queue)
    accept(queue, index_payload(ids=range(1, 31), total=62))
    fail_discovery(queue, _job(queue, 2), NewsError('transport_error', retry=True, delay=7200))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns')]
    before_job = _job(queue, 2)
    source = queue.registry.sources['cnyes']
    with queue.db:
        _reconcile_index_totals(queue, source, cnyes_scope(request_url(source, DAY)), 1)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns')] == before
    assert _job(queue, 2) == before_job
