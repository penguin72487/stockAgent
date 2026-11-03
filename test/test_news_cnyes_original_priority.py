"""Finite original-audit hints stay inside the canonical owner and retry rules."""
from contextlib import closing
import hashlib
import json
import sqlite3
from threading import Event

import pytest

from downloader import news_fulltext_cnyes_history as history
from downloader import news_fulltext_cnyes_priority as priority
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_scope, cnyes_url
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import BASE, cfg, response  # noqa: F401
from test_news_cnyes_history_evidence import bounds, capture, queue, seed_equivalent, source_payload  # noqa: F401


def fixture_plan(queue, path, observations, *, source=None):
    source = source or queue.registry.sources['cnyes']
    plan = {'contract': 'cnyes_original_audit_priority_v1',
            'source_bulk_fingerprint': cnyes_bulk_fingerprint(source),
            'authorization_fingerprint': source.authorization_fingerprint,
            'prior_plan_sha256': 'b' * 64, 'observations': sorted(observations)}
    raw = priority.plan_bytes(plan)
    path.write_bytes(raw)
    return {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}


def capture_urls(queue, starts):
    urls = [cnyes_url(BASE, start=start, end=start + 99) for start in starts]
    for url in urls:
        capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    return urls


def test_priority_discovers_an_alias_created_after_the_original_plan_completed(queue, tmp_path):
    low = 1388534400
    exemplar = cnyes_url(BASE, start=low, end=low + 100)
    data = source_payload(range(1, 31), epoch=low, total=31)
    capture(queue, exemplar, data)
    row = queue.db.execute('SELECT observation_sha256,last_confirmed_at_utc '
                           'FROM cnyes_history_page_heads WHERE scope_url=? AND page=1',
                           (cnyes_scope(exemplar),)).fetchone()
    spec = fixture_plan(queue, tmp_path / 'priority.json', [row['observation_sha256']])
    assert priority.seed(queue, spec, limit=8, max_seconds=10)['state'] == 'complete'
    assert history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)['checked'] == 1
    assert priority.progress(queue)['outside_pages'] == 1

    _, alias = seed_equivalent(queue)
    info, _ = cnyes_pagination(response(data, exemplar))
    with queue.db:
        history.share_matching_page_IDs(queue, cnyes_scope(exemplar), row['observation_sha256'],
                                        info, queue.registry.sources['cnyes'], row['last_confirmed_at_utc'])
    assert queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads '
                            'WHERE scope_url=? AND page=1', (cnyes_scope(alias),)).fetchone()[0] == row['observation_sha256']
    # The new page version requires its own original-file audit; reusing IDs
    # and a page observation must not silently imply original request equality.
    for _ in range(8):
        priority.seed(queue, spec, limit=8, max_seconds=10)
        tracked = queue.db.execute('SELECT last_result FROM cnyes_history_original_priority_pages '
                                  'WHERE scope_url=? AND page=1', (cnyes_scope(alias),)).fetchone()
        if tracked:
            break
    assert tracked and tracked[0] == 'awaiting'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries '
                            'WHERE scope_url=? AND page=1', (cnyes_scope(alias),)).fetchone()
    assert priority.progress(queue)['tracking_complete']
    assert priority.progress(queue)['tracked_pages'] == 2


def _new_head(queue, scope, observation, *, notify=True):
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO cnyes_history_page_heads VALUES (?,?,?,?)',
                         (scope, 1, observation, '2026-01-01T00:00:00+00:00'))
        if notify:
            priority.notify_head(queue, scope, 1, observation)


def test_late_priority_head_survives_restart_and_keeps_existing_retry_and_429(queue, tmp_path):
    url = capture_urls(queue, (1388534400,))[0]
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    alias = cnyes_scope(cnyes_url(BASE, start=1388534600, end=1388534699))
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,2000,4)',
                         (key, alias, 1, observation))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    _new_head(queue, alias, observation)
    assert queue._cnyes_original_priority_needs_wake and priority.progress(queue) is None
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_original_priority_pending').fetchone()[0] == 1
    root, registry = queue.root, queue.registry
    queue.close()
    with closing(NewsQueue(root, registry)) as resumed:
        result = priority.seed(resumed, spec, limit=8, max_seconds=10)
        assert result['state'] == 'complete' and result['existing_pages'] == 1
        assert priority.progress(resumed)['tracking_complete'] and priority.progress(resumed)['tracked_pages'] == 2
        retry = resumed.db.execute('SELECT not_before_epoch,attempts FROM cnyes_history_original_file_retries '
                                   'WHERE scope_url=? AND page=1', (alias,)).fetchone()
        assert tuple(retry) == (2000, 4)
        assert tuple(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
        assert not resumed.db.execute('SELECT 1 FROM cnyes_history_original_priority_pending').fetchone()


def test_late_priority_admission_rolls_back_with_its_journal(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    alias = cnyes_scope(cnyes_url(BASE, start=1388534600, end=1388534699))
    _new_head(queue, alias, observation)
    marker = priority._PREFIX + spec['sha256']
    checkpoint = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
    with queue.db:
        queue.db.execute("CREATE TEMP TRIGGER fail_late_priority BEFORE INSERT ON cnyes_history_original_priority_pages "
                         "BEGIN SELECT RAISE(ABORT,'late admission failed'); END")
    with pytest.raises(sqlite3.IntegrityError, match='late admission failed'):
        priority.seed(queue, spec, limit=8, max_seconds=10)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == checkpoint
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_original_priority_pending').fetchone()[0] == 1
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries WHERE scope_url=?', (alias,)).fetchone()
    with queue.db:
        queue.db.execute('DROP TRIGGER fail_late_priority')
    assert priority.seed(queue, spec, limit=8, max_seconds=10)['state'] == 'complete'
    assert priority.progress(queue)['tracking_complete'] and priority.progress(queue)['tracked_pages'] == 2


def test_reenabled_priority_rescans_heads_created_while_disabled(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    assert history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)['checked'] == 1
    assert priority.progress(queue)['verified_pages'] == 1
    priority.seed(queue, None)
    alias = cnyes_scope(cnyes_url(BASE, start=1388534600, end=1388534699))
    _new_head(queue, alias, observation)
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pending').fetchone()
    result = priority.seed(queue, spec, limit=8, max_seconds=10)
    assert result['state'] == 'complete'
    assert priority.progress(queue)['tracking_complete'] and priority.progress(queue)['verified_pages'] == 1
    assert priority.progress(queue)['awaiting_pages'] == 1
    assert queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries WHERE scope_url=?', (alias,)).fetchone()


def test_late_pending_head_replaced_before_admission_is_not_fabricated(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    alias = cnyes_scope(cnyes_url(BASE, start=1388534600, end=1388534699))
    _new_head(queue, alias, observation)
    _new_head(queue, alias, 'e' * 64)
    assert priority.seed(queue, spec, limit=8, max_seconds=10)['state'] == 'complete'
    assert priority.progress(queue)['tracking_complete'] and priority.progress(queue)['tracked_pages'] == 1
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries WHERE scope_url=?', (alias,)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pending').fetchone()


def test_new_priority_heads_are_time_sliced_and_keep_progress_incomplete(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    for start in (1388534600, 1388534800, 1388535000):
        _new_head(queue, cnyes_scope(cnyes_url(BASE, start=start, end=start + 99)), observation)
    first = priority.seed(queue, spec, limit=1, max_seconds=10)
    assert first['state'] == 'scanning' and first['processed_targets'] == first['targets'] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_original_priority_pending').fetchone()[0] == 2
    assert priority.progress(queue)['tracked_pages'] == 2
    assert priority.seed(queue, spec, limit=1, max_seconds=10)['state'] == 'scanning'
    assert priority.seed(queue, spec, limit=1, max_seconds=10)['state'] == 'complete'
    assert priority.progress(queue)['tracking_complete'] and priority.progress(queue)['tracked_pages'] == 4


def test_alias_inserted_behind_partial_cursor_is_still_admitted(queue, tmp_path):
    capture_urls(queue, (1388534800,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    assert priority.seed(queue, spec, limit=1, max_seconds=10)['state'] == 'scanning'
    earlier = cnyes_scope(cnyes_url(BASE, start=1388534400, end=1388534499))
    _new_head(queue, earlier, observation)
    for _ in range(8):
        result = priority.seed(queue, spec, limit=1, max_seconds=10)
        if result['state'] == 'complete':
            break
    assert result['state'] == 'complete'
    assert priority.progress(queue)['tracking_complete'] and priority.progress(queue)['tracked_pages'] == 2
    assert queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries WHERE scope_url=?', (earlier,)).fetchone()


def test_initially_stale_target_can_later_enter_priority_without_claiming_original(queue, tmp_path):
    observation = '0' * 64
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    initial = priority.seed(queue, spec, limit=8, max_seconds=10)
    assert initial['state'] == 'complete' and initial['stale_targets'] == 1
    scope = cnyes_scope(cnyes_url(BASE, start=1388534600, end=1388534699))
    _new_head(queue, scope, observation)
    resumed = priority.seed(queue, spec, limit=8, max_seconds=10)
    assert resumed['state'] == 'complete' and resumed['stale_targets'] == 1  # As of the scan.
    assert priority.progress(queue)['awaiting_pages'] == 1
    assert not resumed['accepted_as_originals'] and not resumed['all_history_complete']


def test_old_priority_schema_rescans_current_heads_without_erasing_audit(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    assert history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)['checked'] == 1
    marker = priority._PREFIX + spec['sha256']
    prior = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
    alias = cnyes_scope(cnyes_url(BASE, start=1388534600, end=1388534699))
    with queue.db:
        queue.db.execute('DROP TRIGGER cnyes_history_original_priority_new_head')
        queue.db.execute('DROP TRIGGER cnyes_history_original_priority_changed_head')
        queue.db.execute('DROP TABLE cnyes_history_original_priority_pending')
        queue.db.execute('DROP TABLE cnyes_history_original_priority_watch')
        queue.db.execute('DELETE FROM queue_settings WHERE key IN (?,?)',
            (priority._ACTIVE, priority._PREFIX + 'watch_v1:' + spec['sha256'] + ':' + key))
        queue.db.execute('INSERT INTO cnyes_history_page_heads VALUES (?,?,?,?)',
                         (alias, 1, observation, '2026-01-01T00:00:00+00:00'))
    root, registry = queue.root, queue.registry
    queue.close()
    with closing(NewsQueue(root, registry)) as resumed:
        assert priority.seed(resumed, spec, limit=8, max_seconds=10)['state'] == 'complete'
        progress = priority.progress(resumed)
        assert progress['tracking_complete'] and progress['verified_pages'] == progress['awaiting_pages'] == 1
        archive = (priority._PREFIX + 'watch_upgrade:' + spec['sha256'] + ':'
                   + hashlib.sha256(prior.encode()).hexdigest())
        assert resumed.db.execute('SELECT value FROM queue_settings WHERE key=?', (archive,)).fetchone()[0] == prior


def test_priority_missing_watch_is_rebuilt_on_restart(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_original_priority_watch')
    root, registry = queue.root, queue.registry
    queue.close()
    with closing(NewsQueue(root, registry)) as resumed:
        result = priority.seed(resumed, spec, limit=8, max_seconds=10)
        assert result['state'] == 'complete' and priority.progress(resumed)['tracking_complete']
        assert resumed.db.execute('SELECT observation_sha256 FROM cnyes_history_original_priority_watch').fetchone()[0] == observation


@pytest.mark.parametrize('table', ['watch', 'pending'])
def test_priority_unknown_watch_or_pending_is_preserved_and_blocked(queue, tmp_path, table):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    with queue.db:
        if table == 'watch':
            queue.db.execute('INSERT INTO cnyes_history_original_priority_watch VALUES (?,?,?)',
                             (spec['sha256'], key, 'f' * 64))
        else:
            queue.db.execute('INSERT INTO cnyes_history_original_priority_pending VALUES (?,?,?,?,?)',
                             (spec['sha256'], key, 'unknown', 1, 'f' * 64))
    root, registry = queue.root, queue.registry
    queue.close()
    with closing(NewsQueue(root, registry)) as resumed:
        assert priority.seed(resumed, spec, limit=8, max_seconds=10)['problem'] == 'invalid_checkpoint'
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_original_priority_' + table +
                                  ' WHERE observation_sha256=?', ('f' * 64,)).fetchone()


def test_new_priority_head_wakes_the_idle_canonical_owner(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner
    from test_news_fulltext import http_fixture

    cfg['adaptive_rate'] = {'enabled': False}
    http, session = http_fixture(cfg, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    try:
        url = capture_urls(queue, (1388534400,))[0]
        observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
        cfg['cnyes_original_audit_priority_plan'] = fixture_plan(queue, tmp_path / 'priority.json', [observation])
        with queue.db:
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
        before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        alias = cnyes_scope(cnyes_url(BASE, start=1388534600, end=1388534699))
        calls, waits = [], []
        wait = http.stop.wait

        def record_wait(delay):
            waits.append(delay)
            return wait(delay)

        monkeypatch.setattr(http.stop, 'wait', record_wait)

        def local_backfill(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                # A retained page may be attached inside this very slice,
                # after its priority scan. No HTTP future keeps the loop open.
                _new_head(queue, alias, observation)
            elif len(calls) == 2:
                http.stop.set()
            return {'exhausted': True, 'next_local_retry_epoch': None}

        monkeypatch.setattr(history, 'backfill', local_backfill)
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=4, defer_final_status=True)
        assert len(calls) == 2
        assert priority.progress(queue)['tracked_pages'] == 2
        assert queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries WHERE scope_url=?', (alias,)).fetchone()
        assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
        assert all(delay == 0 for delay in waits), f'priority wake was delayed by local idle wait: {waits}'
        assert session.calls == []
    finally:
        http.close()
        queue.close()


def test_rolled_back_priority_head_wakes_once_without_busy_polling(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner
    from test_news_fulltext import http_fixture

    cfg['adaptive_rate'] = {'enabled': False}
    http, session = http_fixture(cfg, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    try:
        capture_urls(queue, (1388534400,))
        observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
        cfg['cnyes_original_audit_priority_plan'] = fixture_plan(queue, tmp_path / 'priority.json', [observation])
        alias = cnyes_scope(cnyes_url(BASE, start=1388534600, end=1388534699))
        calls = []

        def local_backfill(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                try:
                    with queue.db:
                        queue.db.execute('INSERT INTO cnyes_history_page_heads VALUES (?,?,?,?)',
                                         (alias, 1, observation, '2026-01-01T00:00:00+00:00'))
                        priority.notify_head(queue, alias, 1, observation)
                        raise RuntimeError('rollback the new head and its durable journal')
                except RuntimeError:
                    pass
            return {'exhausted': True, 'next_local_retry_epoch': None}

        monkeypatch.setattr(history, 'backfill', local_backfill)
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert len(calls) == 2
        assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=?', (alias,)).fetchone()
        assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pending').fetchone()
        assert not getattr(queue, '_cnyes_original_priority_needs_wake', False)
        assert priority.progress(queue)['tracked_pages'] == 1
        assert session.calls == []
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('resume_before_owner', [False, True])
def test_owner_admits_entire_initial_priority_scan_before_general_original_audit(
        cfg, tmp_path, monkeypatch, resume_before_owner):
    from downloader import news_fulltext as owner
    from test_news_fulltext import http_fixture

    cfg['adaptive_rate'] = {'enabled': False}
    http, session = http_fixture(cfg, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    try:
        urls = capture_urls(queue, (1388534400, 1388534600))
        rows = list(queue.db.execute('SELECT scope_url,page,observation_sha256 '
                                     'FROM cnyes_history_page_heads ORDER BY observation_sha256'))
        late = rows[-1]
        cfg['cnyes_original_audit_priority_plan'] = fixture_plan(
            queue, tmp_path / 'priority.json', [row['observation_sha256'] for row in rows])
        with queue.db:
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (urls[0],))
        before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone())
        original_seed = priority.seed
        if resume_before_owner:
            partial = original_seed(queue, cfg['cnyes_original_audit_priority_plan'], limit=1, max_seconds=10)
            assert partial['state'] == 'scanning'
            queue.close()
            queue = NewsQueue(tmp_path, http.registry)
        audit_observations = []

        def one_step_seed(queue, spec, **kwargs):
            kwargs.update(limit=1, max_seconds=10)
            return original_seed(queue, spec, **kwargs)

        def general_audit(queue, **kwargs):
            tracked = bool(queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pages '
                'WHERE scope_url=? AND page=? AND observation_sha256=?', tuple(late)).fetchone())
            audit_observations.append(tracked)
            http.stop.set()
            return {'complete': True, 'checked': 0, 'mismatched': 0, 'unverified': 0,
                    'equal_day_page_ID_recoveries': 0, 'next_epoch': None, 'blocked': None}

        monkeypatch.setattr(priority, 'seed', one_step_seed)
        monkeypatch.setattr(history, 'audit_original_envelopes', general_audit)
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=4, defer_final_status=True)
        assert audit_observations == [True]
        assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone()) == before
        assert session.calls == []
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('plan_state', ['absent', 'invalid'])
def test_owner_does_not_defer_general_audit_for_an_absent_or_invalid_plan(cfg, tmp_path, monkeypatch, plan_state):
    from downloader import news_fulltext as owner
    from test_news_fulltext import http_fixture

    cfg['adaptive_rate'] = {'enabled': False}
    if plan_state == 'absent':
        cfg.pop('cnyes_original_audit_priority_plan', None)
    else:
        cfg['cnyes_original_audit_priority_plan'] = {'path': str(tmp_path / 'missing.json'), 'sha256': '0' * 64}
    http, session = http_fixture(cfg, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    called = []

    def general_audit(queue, **kwargs):
        called.append(True)
        http.stop.set()
        return {'complete': True, 'checked': 0, 'mismatched': 0, 'unverified': 0,
                'equal_day_page_ID_recoveries': 0, 'next_epoch': None, 'blocked': None}

    monkeypatch.setattr(history, 'audit_original_envelopes', general_audit)
    try:
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=4, defer_final_status=True)
        assert called == [True]
        if plan_state == 'invalid':
            assert queue._cnyes_original_priority_view['state'] == 'blocked'
        assert not session.calls
    finally:
        http.close()
        queue.close()


def test_deferred_original_audit_keeps_other_local_history_lanes_running(queue, monkeypatch):
    original_sweep = history.sweep_completed_scopes
    sweeps, audits = [], []

    def sweep(*args, **kwargs):
        sweeps.append(True)
        return original_sweep(*args, **kwargs)

    def general_audit(queue, **kwargs):
        audits.append(True)
        return {'complete': True, 'checked': 0, 'mismatched': 0, 'unverified': 0,
                'equal_day_page_ID_recoveries': 0, 'next_epoch': None, 'blocked': None}

    monkeypatch.setattr(history, 'sweep_completed_scopes', sweep)
    monkeypatch.setattr(history, 'audit_original_envelopes', general_audit)
    first = history.backfill(queue, limit=8, max_seconds=10, defer_original_audit=True)
    assert sweeps == [True] and not audits
    assert first['original_envelope_audit']['deferred_for_priority'] is True
    assert first['exhausted'] is False
    second = history.backfill(queue, limit=8, max_seconds=10)
    assert len(sweeps) == 2 and audits == [True]
    assert second['original_envelope_audit']['complete'] is True


def test_other_provider_commits_while_initial_priority_scan_is_incomplete(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner
    from downloader.news_fulltext_queue import verify_bodies
    from test_news_fulltext import FakeResponse, article_html, config as publisher_config, http_fixture

    cfg['providers'].append(publisher_config.__wrapped__()['providers'][0])
    cfg.update(workers=2, batch_articles=1, adaptive_rate={'enabled': False})
    routes = {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://publisher.test/story': FakeResponse(article_html()),
    }
    http, session = http_fixture(cfg, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    try:
        capture_urls(queue, (1388534400, 1388534600))
        observations = [row[0] for row in queue.db.execute(
            'SELECT observation_sha256 FROM cnyes_history_page_heads ORDER BY observation_sha256')]
        cfg['cnyes_original_audit_priority_plan'] = fixture_plan(queue, tmp_path / 'priority.json', observations)
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/story'}])
        original_seed, original_accept = priority.seed, queue.accept_live_article_batch
        seed_calls, commits = [], []

        def slow_initial_scan(queue, spec, **kwargs):
            seed_calls.append(True)
            if len(seed_calls) == 1:
                kwargs.update(limit=1, max_seconds=10)
                return original_seed(queue, spec, **kwargs)
            # Model a still-running finite scan until the peer is durably accepted.
            return {**queue._cnyes_original_priority_view, 'slice_steps': 1}

        def accept(records, **kwargs):
            original_accept(records, **kwargs)
            view = queue._cnyes_original_priority_view
            commits.append((view['processed_targets'], view['targets']))
            http.stop.set()

        monkeypatch.setattr(priority, 'seed', slow_initial_scan)
        monkeypatch.setattr(queue, 'accept_live_article_batch', accept)
        owner.run_cycle(queue, http, cfg, max_articles=1, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=5, defer_final_status=True)
        assert commits and commits[0][0] < commits[0][1]
        assert verify_bodies(tmp_path)['accepted']
        assert any(url == 'https://publisher.test/story' for url, _ in session.calls)
    finally:
        http.close()
        queue.close()


def test_priority_resumes_aliases_and_preserves_existing_retry_deadline(queue, tmp_path):
    first, alias = capture_urls(queue, (1388534400, 1388534600))
    rows = list(queue.db.execute('SELECT scope_url,observation_sha256 FROM cnyes_history_page_heads ORDER BY scope_url'))
    sha, replaced = rows[0][1], rows[1][1]
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? WHERE scope_url=?',
                         (sha, rows[1][0]))
        queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,?,?)',
                         (key, rows[0][0], 1, sha, 9999999999, 4))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9")
    jobs = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [sha, replaced, '0' * 64])
    result = priority.seed(queue, spec, limit=1, max_seconds=10)
    assert result['state'] == 'scanning'
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(12):
            result = priority.seed(resumed, spec, limit=1, max_seconds=10)
            if result['state'] == 'complete':
                break
        assert result['state'] == 'complete' and result['processed_targets'] == result['targets'] == 3
        assert result['stale_targets'] == 2 and result['enqueued_pages'] == result['existing_pages'] == 1
        retries = [tuple(row) for row in resumed.db.execute(
            'SELECT scope_url,not_before_epoch,attempts FROM cnyes_history_original_file_retries ORDER BY scope_url')]
        assert retries == [(rows[0][0], 9999999999, 4), (rows[1][0], 0, 0)]
        assert [tuple(row) for row in resumed.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs
        repeat = priority.seed(resumed, spec, limit=1, max_seconds=10)
        assert {**repeat, 'slice_steps': result['slice_steps']} == result and repeat['slice_steps'] == 0
        assert not result['accepted_as_originals'] and not result['all_history_complete']


def test_priority_is_a_hint_and_auditor_checks_real_original_without_resetting_http(queue, tmp_path):
    url = capture_urls(queue, (1388534400,))[0]
    row = queue.db.execute('SELECT observation_sha256,observation_json FROM cnyes_history_page_observations').fetchone()
    original = queue.root / json.loads(row['observation_json'])['artifact']['parquet_path']
    original.rename(original.with_suffix('.held'))
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    spec = fixture_plan(queue, tmp_path / 'priority.json', [row['observation_sha256']])
    result = priority.seed(queue, spec, limit=8, max_seconds=10)
    assert result['enqueued_pages'] == 1
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_unproven_pages').fetchone()
    audit = history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)
    assert audit['checked'] == audit['unverified'] == 1
    retry = queue.db.execute('SELECT not_before_epoch,attempts FROM cnyes_history_original_file_retries').fetchone()
    assert tuple(retry) == (4600, 1)
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_priority_retry_overtakes_older_due_work_and_preserves_general_progress(queue, tmp_path):
    capture_urls(queue, (1388534400, 1388534600, 1388534800))
    rows = list(queue.db.execute('SELECT scope_url,page,observation_sha256 '
                                 'FROM cnyes_history_page_heads ORDER BY scope_url,page'))
    source = queue.registry.sources['cnyes']
    key = history.original_envelope_audit_key(source)
    # Complete the regular scan first so this test observes only retry slices.
    assert history.audit_original_envelopes(queue, limit=10, max_seconds=10, now_epoch=1000)['complete']
    with queue.db:
        for row in rows[:-1]:
            queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,0,0)',
                             (key, *row))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9")
    prior_jobs = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [rows[-1]['observation_sha256']])
    priority.seed(queue, spec, limit=8, max_seconds=10)

    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=1001)['checked'] == 1
    assert priority.progress(queue)['verified_pages'] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_original_file_retries').fetchone()[0] == 2
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=1001)['checked'] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_original_file_retries').fetchone()[0] == 1
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == prior_jobs


def test_priority_retry_turn_survives_restart_and_services_other_due_work(queue, tmp_path):
    capture_urls(queue, (1388534400, 1388534600, 1388534800, 1388535000))
    rows = list(queue.db.execute('SELECT scope_url,page,observation_sha256 '
                                 'FROM cnyes_history_page_heads ORDER BY scope_url,page'))
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    assert history.audit_original_envelopes(queue, limit=10, max_seconds=10, now_epoch=1000)['complete']
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,0,0)',
                         (key, *rows[0]))
    spec = fixture_plan(queue, tmp_path / 'priority.json', [row['observation_sha256'] for row in rows[1:]])
    priority.seed(queue, spec, limit=10, max_seconds=10)
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=1001)['checked'] == 1
    assert priority.progress(queue)['verified_pages'] == 1
    marker = key + ':priority_retry_next:' + spec['sha256']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == 'general'

    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        priority.seed(resumed, spec, limit=10, max_seconds=10)
        assert history.audit_original_envelopes(resumed, limit=1, max_seconds=10, now_epoch=1001)['checked'] == 1
        assert priority.progress(resumed)['verified_pages'] == 1
        assert not resumed.db.execute('SELECT 1 FROM cnyes_history_original_file_retries '
            'WHERE audit_key=? AND scope_url=? AND page=? AND observation_sha256=?', (key, *rows[0])).fetchone()
        assert history.audit_original_envelopes(resumed, limit=1, max_seconds=10, now_epoch=1001)['checked'] == 1
        assert priority.progress(resumed)['verified_pages'] == 2


def test_priority_retry_never_bypasses_its_local_deadline(queue, tmp_path):
    capture_urls(queue, (1388534400, 1388534600))
    rows = list(queue.db.execute('SELECT scope_url,page,observation_sha256 '
                                 'FROM cnyes_history_page_heads ORDER BY scope_url,page'))
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    assert history.audit_original_envelopes(queue, limit=10, max_seconds=10, now_epoch=1000)['complete']
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,0,0)',
                         (key, *rows[0]))
        queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,2000,4)',
                         (key, *rows[1]))
    spec = fixture_plan(queue, tmp_path / 'priority.json', [rows[1]['observation_sha256']])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=1001)['checked'] == 1
    assert priority.progress(queue)['awaiting_pages'] == 1
    retry = queue.db.execute('SELECT not_before_epoch,attempts FROM cnyes_history_original_file_retries '
        'WHERE audit_key=? AND scope_url=? AND page=? AND observation_sha256=?', (key, *rows[1])).fetchone()
    assert tuple(retry) == (2000, 4)
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=2000)['checked'] == 1
    assert priority.progress(queue)['verified_pages'] == 1


def test_priority_retries_do_not_starve_the_full_original_sweep(queue, tmp_path):
    capture_urls(queue, (1388534400, 1388534600, 1388534800))
    rows = list(queue.db.execute('SELECT scope_url,page,observation_sha256 '
                                 'FROM cnyes_history_page_heads ORDER BY scope_url,page'))
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    spec = fixture_plan(queue, tmp_path / 'priority.json', [rows[-1]['observation_sha256']])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    # The original full sweep has not started; one retry slice is followed by
    # a full slice even though another due retry is present.
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,0,0)',
                         (key, *rows[1]))
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=1000)['checked'] == 1
    assert priority.progress(queue)['verified_pages'] == 1
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=1000)['checked'] == 1
    state = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    assert state['state'] == 'scanning' and state['checked'] == 1
    assert state['cursor_scope'] == rows[0]['scope_url']


def test_future_priority_retry_wait_is_reused_and_new_due_hint_invalidates_it(queue, tmp_path):
    capture_urls(queue, (1388534400, 1388534600))
    rows = list(queue.db.execute('SELECT scope_url,page,observation_sha256 '
                                 'FROM cnyes_history_page_heads ORDER BY observation_sha256'))
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,2000,4)',
                         (key, *rows[0]))
    spec = fixture_plan(queue, tmp_path / 'priority.json', [row['observation_sha256'] for row in rows])
    priority.seed(queue, spec, limit=1, max_seconds=10)
    assert priority.select_priority_retries(queue, key, 1000, 1)[0] is None
    traced = []
    queue.db.set_trace_callback(traced.append)
    try:
        assert priority.select_priority_retries(queue, key, 1001, 1)[0] is None
    finally:
        queue.db.set_trace_callback(None)
    assert not any('FROM cnyes_history_original_priority_pages p' in sql for sql in traced)

    for _ in range(4):
        priority.seed(queue, spec, limit=1, max_seconds=10)
        if priority.progress(queue)['tracked_pages'] == 2:
            break
    assert priority.progress(queue)['tracked_pages'] == 2
    selected, _ = priority.select_priority_retries(queue, key, 1002, 1)
    assert len(selected) == 1 and selected[0]['retry_sha'] == rows[1]['observation_sha256']


def test_invalid_plan_source_and_cursor_defer_without_queue_mutation(queue, tmp_path):
    url = capture_urls(queue, (1388534400,))[0]
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads WHERE scope_url=?',
                           (cnyes_scope(url),)).fetchone()[0]
    path = tmp_path / 'priority.json'
    spec = fixture_plan(queue, path, [sha])
    raw = path.read_bytes()
    path.write_bytes(raw + b' ')
    assert priority.seed(queue, spec)['problem'] == 'plan_unavailable_or_invalid'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()
    path.write_bytes(raw)
    plan = json.loads(raw)
    plan['authorization_fingerprint'] = '0' * 64
    changed = priority.plan_bytes(plan)
    path.write_bytes(changed)
    changed_spec = {'path': str(path), 'sha256': hashlib.sha256(changed).hexdigest()}
    assert priority.seed(queue, changed_spec)['problem'] == 'source_identity_changed'
    path.write_bytes(raw)
    marker = priority._PREFIX + spec['sha256']
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, '{"version":2}'))
    assert priority.seed(queue, spec)['problem'] == 'invalid_checkpoint'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == '{"version":2}'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()


@pytest.mark.parametrize(('next_index', 'stale', 'after_scope', 'after_page'), [
    (1, 0, '', 0),
    (2, 1, '', 0),
    (0, 0, 'https://news.cnyes.com/scope', 1),
])
def test_impossible_priority_checkpoint_is_archived_and_rebuilt_from_hints(
        queue, tmp_path, next_index, stale, after_scope, after_page):
    capture_urls(queue, (1388534400, 1388534600))
    observations = [row[0] for row in queue.db.execute(
        'SELECT observation_sha256 FROM cnyes_history_page_heads ORDER BY scope_url,page')]
    spec = fixture_plan(queue, tmp_path / 'priority.json', observations)
    marker = priority._PREFIX + spec['sha256']
    state = {'version': 1, 'next_index': next_index, 'after_scope': after_scope,
             'after_page': after_page, 'heads_seen': 0, 'added': 0,
             'existing': 0, 'stale': stale}
    raw = json.dumps(state, sort_keys=True)
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, raw))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9")
    jobs_before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]

    result = priority.seed(queue, spec, limit=8, max_seconds=10)

    assert result['state'] == 'complete' and result['checkpoint_recovered']
    archive = priority._PREFIX + 'invalid:' + spec['sha256'] + ':' + hashlib.sha256(raw.encode()).hexdigest()
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (archive,)).fetchone()[0] == raw
    assert result['processed_targets'] == result['enqueued_pages'] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_original_file_retries').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_original_priority_pages').fetchone()[0] == 2
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == jobs_before
    assert priority.progress(queue)['awaiting_pages'] == 2


def test_priority_checkpoint_recovery_reuses_completed_audits_and_survives_restart(queue, tmp_path):
    capture_urls(queue, (1388534400, 1388534600))
    observations = [row[0] for row in queue.db.execute(
        'SELECT observation_sha256 FROM cnyes_history_page_heads ORDER BY scope_url,page')]
    spec = fixture_plan(queue, tmp_path / 'priority.json', observations)
    priority.seed(queue, spec, limit=8, max_seconds=10)
    for _ in range(8):
        history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)
        if priority.progress(queue)['verified_pages'] == 2:
            break
    assert priority.progress(queue)['verified_pages'] == 2
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()
    marker = priority._PREFIX + spec['sha256']
    invalid = {'version': 1, 'next_index': 2, 'after_scope': '', 'after_page': 0,
               'heads_seen': 0, 'added': 0, 'existing': 0, 'stale': 0}
    raw = json.dumps(invalid, sort_keys=True)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (raw, marker))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9")
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    first = priority.seed(queue, spec, limit=1, max_seconds=10)
    assert first['checkpoint_recovered'] and first['state'] == 'scanning'
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(8):
            result = priority.seed(resumed, spec, limit=1, max_seconds=10)
            if result['state'] == 'complete':
                break
        assert result['state'] == 'complete' and result['enqueued_pages'] == 0
        assert result['existing_pages'] == 2 and priority.progress(resumed)['verified_pages'] == 2
        assert not resumed.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()
        assert [tuple(row) for row in resumed.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_priority_checkpoint_recovery_rolls_back_archive_and_requeue_on_failure(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    marker = priority._PREFIX + spec['sha256']
    invalid = {'version': 1, 'next_index': 1, 'after_scope': '', 'after_page': 0,
               'heads_seen': 0, 'added': 0, 'existing': 0, 'stale': 0}
    raw = json.dumps(invalid, sort_keys=True)
    archive = priority._PREFIX + 'invalid:' + spec['sha256'] + ':' + hashlib.sha256(raw.encode()).hexdigest()
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, raw))
        queue.db.execute("CREATE TEMP TRIGGER fail_recovery BEFORE INSERT ON cnyes_history_original_priority_pages "
                         "BEGIN SELECT RAISE(ABORT,'simulated recovery failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='simulated recovery failure'):
        priority.seed(queue, spec, limit=8, max_seconds=10)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == raw
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (archive,)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()


def test_priority_checkpoint_recovery_preserves_existing_retry_deadline(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    row = queue.db.execute('SELECT scope_url,page,observation_sha256 FROM cnyes_history_page_heads').fetchone()
    spec = fixture_plan(queue, tmp_path / 'priority.json', [row['observation_sha256']])
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_original_file_retries VALUES (?,?,?,?,2000,4)', (key, *row))
    priority.seed(queue, spec, limit=8, max_seconds=10)
    marker = priority._PREFIX + spec['sha256']
    invalid = {'version': 1, 'next_index': 1, 'after_scope': '', 'after_page': 0,
               'heads_seen': 0, 'added': 0, 'existing': 0, 'stale': 0}
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(invalid), marker))
    result = priority.seed(queue, spec, limit=8, max_seconds=10)
    retry = queue.db.execute('SELECT not_before_epoch,attempts FROM cnyes_history_original_file_retries '
                             'WHERE audit_key=? AND scope_url=? AND page=? AND observation_sha256=?',
                             (key, *row)).fetchone()
    assert result['checkpoint_recovered'] and tuple(retry) == (2000, 4)
    assert priority.select_priority_retries(queue, key, 1999, 1)[0] is None
    assert len(priority.select_priority_retries(queue, key, 2000, 1)[0]) == 1


def test_priority_checkpoint_recovery_rechecks_a_returned_superseded_head(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    marker = priority._PREFIX + spec['sha256']
    invalid = {'version': 1, 'next_index': 1, 'after_scope': '', 'after_page': 0,
               'heads_seen': 0, 'added': 0, 'existing': 0, 'stale': 0}
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_original_priority_pages SET last_result='superseded'")
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(invalid), marker))
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    queue._cnyes_original_priority_wait = ((spec['sha256'], key), 9000)
    result = priority.seed(queue, spec, limit=8, max_seconds=10)
    assert result['checkpoint_recovered'] and priority.progress(queue)['awaiting_pages'] == 1
    assert len(priority.select_priority_retries(queue, key, 1000, 1)[0]) == 1


def test_priority_checkpoint_recovery_restores_missing_unverified_retry(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    marker = priority._PREFIX + spec['sha256']
    invalid = {'version': 1, 'next_index': 1, 'after_scope': '', 'after_page': 0,
               'heads_seen': 0, 'added': 0, 'existing': 0, 'stale': 0}
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_original_priority_pages SET last_result='unavailable'")
        queue.db.execute('DELETE FROM cnyes_history_original_file_retries')
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(invalid), marker))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9")
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = priority.seed(queue, spec, limit=8, max_seconds=10)
    assert result['checkpoint_recovered'] and result['enqueued_pages'] == 1
    assert priority.progress(queue)['unverified_pages'] == 1
    retry = queue.db.execute('SELECT not_before_epoch,attempts FROM cnyes_history_original_file_retries').fetchone()
    assert tuple(retry) == (0, 0)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_priority_checkpoint_recovery_keeps_conflicting_archive_and_stop(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    observation = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [observation])
    marker = priority._PREFIX + spec['sha256']
    invalid = {'version': 1, 'next_index': 1, 'after_scope': '', 'after_page': 0,
               'heads_seen': 0, 'added': 0, 'existing': 0, 'stale': 0}
    raw = json.dumps(invalid, sort_keys=True)
    archive = priority._PREFIX + 'invalid:' + spec['sha256'] + ':' + hashlib.sha256(raw.encode()).hexdigest()
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, raw))
    stopped = Event()
    stopped.set()
    assert priority.seed(queue, spec, stop=stopped)['problem'] == 'invalid_checkpoint'
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (archive,)).fetchone()
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (archive, 'different evidence'))
    assert priority.seed(queue, spec)['problem'] == 'checkpoint_archive_conflict'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == raw
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (archive,)).fetchone()[0] == 'different evidence'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()


def test_priority_all_stale_targets_complete_without_claiming_pages(queue, tmp_path):
    spec = fixture_plan(queue, tmp_path / 'priority.json', ['0' * 64, '1' * 64])
    first = priority.seed(queue, spec, limit=1, max_seconds=10)
    assert first['state'] == 'scanning' and first['stale_targets'] == 1
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        result = priority.seed(resumed, spec, limit=8, max_seconds=10)
        assert result['state'] == 'complete' and result['processed_targets'] == 2
        assert result['stale_targets'] == 2 and result['enqueued_pages'] == result['existing_pages'] == 0
        stage = priority.progress(resumed)
        assert stage['tracking_complete'] and stage['tracked_pages'] == stage['verified_pages'] == 0
        assert not resumed.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()


def test_priority_cursor_and_hints_rollback_together_on_failure(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [sha])
    with queue.db:
        queue.db.execute("CREATE TEMP TRIGGER fail_priority BEFORE INSERT ON queue_settings "
            "WHEN NEW.key LIKE 'cnyes_original_audit_priority_v1:%' "
            "BEGIN SELECT RAISE(ABORT,'priority checkpoint failed'); END")
    with pytest.raises(sqlite3.IntegrityError, match='priority checkpoint failed'):
        priority.seed(queue, spec, limit=8, max_seconds=10)
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_priority_pages').fetchone()
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (priority._PREFIX + spec['sha256'],)).fetchone()


def test_priority_tracks_audit_results_and_changed_heads_separately(queue, tmp_path, monkeypatch):
    first = cnyes_url(BASE, start=1388534400, end=1388534499)
    mismatch = cnyes_url(BASE, start=1388534600, end=1388534699)
    missing = cnyes_url(BASE, start=1388534800, end=1388534899)
    capture(queue, first, source_payload([], epoch=bounds(first)[0]))
    first_artifact = json.loads(queue.db.execute(
        'SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
        (cnyes_scope(first),)).fetchone()[0])['artifact']
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: first_artifact)
        capture(queue, mismatch, source_payload([], epoch=bounds(first)[0]))
    capture(queue, missing, source_payload([], epoch=bounds(missing)[0]))
    rows = list(queue.db.execute('SELECT scope_url,observation_sha256,observation_json '
        'FROM cnyes_history_page_observations ORDER BY scope_url'))
    missing_row = next(row for row in rows if row['scope_url'] == cnyes_scope(missing))
    original = queue.root / json.loads(missing_row['observation_json'])['artifact']['parquet_path']
    original.rename(original.with_suffix('.held'))
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9")
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [row['observation_sha256'] for row in rows])
    assert priority.seed(queue, spec, limit=20, max_seconds=10)['state'] == 'complete'
    staged = priority.progress(queue)
    assert staged['tracking_complete'] and staged['tracked_pages'] == staged['awaiting_pages'] == 3
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    assert result['checked'] == 3 and result['mismatched'] == result['unverified'] == 1
    audited = priority.progress(queue)
    assert {k: audited[k] for k in ('awaiting_pages', 'verified_pages', 'mismatched_pages',
            'unverified_pages', 'outside_pages', 'changed_pages')} == {
                'awaiting_pages': 0, 'verified_pages': 1, 'mismatched_pages': 1,
                'unverified_pages': 1, 'outside_pages': 0, 'changed_pages': 0}
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? WHERE scope_url=?',
                         ('f' * 64, cnyes_scope(first)))
    changed = priority.progress(queue)
    assert changed['verified_pages'] == 0 and changed['changed_pages'] == 1
    assert changed['mismatched_pages'] == changed['unverified_pages'] == 1
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    assert changed['local_raw_only'] and not changed['all_history_complete'] and not changed['publisher_requests']


def test_priority_audit_result_rolls_back_with_original_retry(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [sha])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    with queue.db:
        queue.db.execute("CREATE TEMP TRIGGER fail_priority_result BEFORE UPDATE ON cnyes_history_original_priority_pages "
                         "BEGIN SELECT RAISE(ABORT,'priority result failed'); END")
    with pytest.raises(sqlite3.IntegrityError, match='priority result failed'):
        history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)
    assert priority.progress(queue)['awaiting_pages'] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_original_file_retries').fetchone()[0] == 1
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
        (history.original_envelope_audit_key(queue.registry.sources['cnyes']),)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
        (history.original_envelope_audit_key(queue.registry.sources['cnyes'])
         + ':priority_retry_next:' + spec['sha256'],)).fetchone()


def test_priority_older_checkpoint_without_tracking_does_not_claim_full_accounting(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [sha])
    assert priority.seed(queue, spec, limit=8, max_seconds=10)['state'] == 'complete'
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_original_priority_pages')
    assert priority.seed(queue, spec, limit=8, max_seconds=10)['state'] == 'complete'
    tracking = priority.progress(queue)
    assert not tracking['tracking_complete'] and tracking['tracked_pages'] == 0


def test_priority_audit_accounting_is_durable_across_owner_restart(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    queue.add_articles([{'url': 'https://news.cnyes.com/news/id/70000002'}])
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [sha])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)
    prior = priority.progress(queue)
    assert prior['tracking_complete'] and prior['verified_pages'] == 1
    head = queue.db.execute('SELECT scope_url,page FROM cnyes_history_page_heads').fetchone()
    changes = queue.db.total_changes
    with queue.db:
        priority.record_audit_result(queue, history.original_envelope_audit_key(queue.registry.sources['cnyes']),
                                     head[0], head[1], sha, 'verified')
    assert queue.db.total_changes == changes  # Repeated identical checks do not rewrite the tracker.
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert priority.progress(resumed) is None
        assert priority.seed(resumed, spec, limit=8, max_seconds=10)['slice_steps'] == 0
        assert priority.progress(resumed) == prior
        assert resumed.status(publish=False)['providers']['cnyes']['history_list_progress']['original_priority_audit'] == prior


def test_priority_audit_is_recorded_while_plan_is_temporarily_disabled(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [sha])
    priority.seed(queue, spec, limit=8, max_seconds=10)
    assert priority.seed(queue, None) is None
    assert priority.progress(queue) is None
    assert history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)['checked'] == 1
    # The plan may have been inactive while new page references arrived, so
    # re-enabling it rechecks the finite target set without losing old results.
    assert priority.seed(queue, spec, limit=8, max_seconds=10)['state'] == 'complete'
    assert priority.progress(queue)['verified_pages'] == 1


def test_original_audit_updates_each_matching_priority_plan(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    one_path, two_path = tmp_path / 'one.json', tmp_path / 'two.json'
    one = fixture_plan(queue, one_path, [sha])
    plan = json.loads(one_path.read_bytes())
    plan['prior_plan_sha256'] = 'c' * 64
    raw = priority.plan_bytes(plan)
    two_path.write_bytes(raw)
    two = {'path': str(two_path), 'sha256': hashlib.sha256(raw).hexdigest()}
    priority.seed(queue, one, limit=8, max_seconds=10)
    priority.seed(queue, two, limit=8, max_seconds=10)
    priority.seed(queue, None)
    history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)
    for spec in (one, two):
        priority.seed(queue, spec, limit=8, max_seconds=10)
        assert priority.progress(queue)['verified_pages'] == 1


def test_stop_and_spec_validation_do_not_mutate_queue(queue, tmp_path):
    capture_urls(queue, (1388534400,))
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    spec = fixture_plan(queue, tmp_path / 'priority.json', [sha])
    stop = Event()
    stop.set()
    assert priority.seed(queue, spec, stop=stop)['processed_targets'] == 0
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()
    assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (priority._PREFIX + spec['sha256'],)).fetchone()
    with pytest.raises(ValueError, match='not_in_artifacts'):
        priority.validate_spec({'path': '../outside', 'sha256': 'a' * 64}, tmp_path)


def test_running_owner_adopts_priority_and_reuses_audit_without_publisher_calls(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner
    from test_news_fulltext import http_fixture

    cfg['adaptive_rate'] = {'enabled': False}
    http, session = http_fixture(cfg, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    try:
        url = capture_urls(queue, (1388534400,))[0]
        row = queue.db.execute('SELECT observation_sha256,observation_json FROM cnyes_history_page_observations').fetchone()
        original = queue.root / json.loads(row['observation_json'])['artifact']['parquet_path']
        original.rename(original.with_suffix('.held'))
        with queue.db:
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/70000001'}])
        before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        cfg['cnyes_original_audit_priority_plan'] = fixture_plan(queue, tmp_path / 'priority.json', [row['observation_sha256']])
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=0,
                        continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        view = queue._cnyes_original_priority_view
        assert view['state'] == 'complete' and view['processed_targets'] == 1 and view['enqueued_pages'] == 1
        assert queue.status(publish=False)['providers']['cnyes']['history_list_progress']['original_priority_admission'] == view
        retry = queue.db.execute('SELECT attempts FROM cnyes_history_original_file_retries').fetchone()
        assert retry and retry[0] == 1  # Existing auditor consumed the hint and deferred the missing file.
        stage = queue.status(publish=False)['providers']['cnyes']['history_list_progress']['original_priority_audit']
        assert stage['tracking_complete'] and stage['unverified_pages'] == 1 and stage['awaiting_pages'] == 0
        assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
        assert session.calls == []
    finally:
        http.close()
        queue.close()


def test_recreate_priority_artifact_requires_exact_report_and_preserves_conflicts(queue, tmp_path, monkeypatch):
    from scripts import prepare_news_cnyes_original_priority as prepare

    source = queue.registry.sources['cnyes']
    report = {'contract': 'cnyes_unchanged_mismatch_plan_v2', 'state': 'unresolved_evidence_plan',
              'all_history_complete': False, 'source_fingerprint': cnyes_bulk_fingerprint(source),
              'unchanged_mismatch_envelopes': 1,
              'unchanged_mismatches': [{'observation_sha256': ['a' * 64]}]}
    report_path = tmp_path / 'report.json'
    report_path.write_text(json.dumps(report))
    raw = priority.plan_bytes(priority.make_plan(report_path.read_bytes(), source))
    artifact = tmp_path / 'artifact.json'
    spec = {'path': str(artifact), 'sha256': hashlib.sha256(raw).hexdigest()}
    monkeypatch.setattr(prepare, 'load_config', lambda _: ({'cnyes_original_audit_priority_plan': spec},
                                                           queue.registry, tmp_path))
    result = prepare.prepare(tmp_path / 'config.json', report_path)
    assert result['targets'] == 1 and not result['production_queue_writes'] and not result['publisher_requests']
    assert artifact.read_bytes() == raw
    assert prepare.prepare(tmp_path / 'config.json', report_path) == result
    report_path.write_text(json.dumps({**report, 'unchanged_mismatches': [{'observation_sha256': ['c' * 64]}]}))
    with pytest.raises(ValueError, match='configured_priority_digest'):
        prepare.prepare(tmp_path / 'config.json', report_path)
    assert artifact.read_bytes() == raw
    report_path.write_text(json.dumps(report))
    artifact.write_bytes(b'other evidence')
    with pytest.raises(ValueError, match='artifact_conflict'):
        prepare.prepare(tmp_path / 'config.json', report_path)
    assert artifact.read_bytes() == b'other evidence'


@pytest.mark.parametrize('version', [2, '2', None])
def test_original_audit_unknown_version_preserves_cursor_and_other_work(queue, version):
    from test_news_cnyes_history_evidence import seed_retained_work

    url, _ = seed_retained_work(queue, failure='http_429')
    source = queue.registry.sources['cnyes']
    key = history.original_envelope_audit_key(source)
    saved = json.dumps({'version': version, 'private_cursor': 'do not overwrite'})
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, saved))
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=8, max_seconds=10)
    assert result['blocked'] == 'unsupported_original_audit_checkpoint_version'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == saved
    status = history.progress(queue.db, source)['original_envelope_audit']
    assert status['scan_state'] == 'unknown' and status['checkpoint_problem'] == 'unsupported_version'
    for _ in range(30):
        result = history.backfill(queue, limit=8, max_seconds=10)
        if result['exhausted']:
            break
    assert result['exhausted'] and history.maintenance_delay(result) == 300
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'restored'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == saved


def test_original_audit_checkpoint_conflict_does_not_erase_original(queue):
    source = queue.registry.sources['cnyes']
    key = history.original_envelope_audit_key(source)
    audit_sha, _ = history._original_audit_archive_identity(key, 'broken')
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, 'broken'))
        queue.db.execute('INSERT INTO cnyes_history_integrity_checkpoint_audit VALUES (?,?,?,?)',
                         (audit_sha, key, 'different evidence', '2026-01-01T00:00:00+00:00'))
    assert history.progress(queue.db, source)['original_envelope_audit']['checkpoint_problem'] == 'audit_conflict'
    result = history.audit_original_envelopes(queue, limit=8, max_seconds=10)
    assert result['blocked'] == 'original_audit_checkpoint_audit_conflict'
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == 'broken'
    assert queue.db.execute('SELECT original_value FROM cnyes_history_integrity_checkpoint_audit WHERE checkpoint_sha256=?',
                            (audit_sha,)).fetchone()[0] == 'different evidence'


def test_original_audit_checkpoint_stop_and_archive_failure_keep_cursor(queue, monkeypatch):
    source = queue.registry.sources['cnyes']
    key = history.original_envelope_audit_key(source)
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, 'broken'))
    stop = Event()
    def interrupted(_):
        stop.set()
        raise ValueError('broken')
    with monkeypatch.context() as patch:
        patch.setattr(history, '_decode_original_audit_checkpoint', interrupted)
        assert not history.audit_original_envelopes(queue, stop=stop)['complete']
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_integrity_checkpoint_audit').fetchone()
    with queue.db:
        queue.db.execute("CREATE TEMP TRIGGER fail_original_audit BEFORE INSERT ON cnyes_history_integrity_checkpoint_audit "
                         "BEGIN SELECT RAISE(ABORT,'archive failed'); END")
    with pytest.raises(sqlite3.IntegrityError, match='archive failed'):
        history.audit_original_envelopes(queue)
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == 'broken'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_integrity_checkpoint_audit').fetchone()


def test_original_audit_complete_cursor_with_remaining_heads_resumes(queue):
    url = capture_urls(queue, (1388534400,))[0]
    source = queue.registry.sources['cnyes']
    key = history.original_envelope_audit_key(source)
    first = history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1000)
    assert first['complete'] and first['checked'] == 1
    state = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    state.update(cursor_scope='', cursor_page=0)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(state), key))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert history.progress(queue.db, source)['original_envelope_audit']['scan_state'] == 'scanning'
    result = history.audit_original_envelopes(queue, limit=8, max_seconds=10, now_epoch=1001)
    assert result['complete'] and result['checked'] == 1
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
