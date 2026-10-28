"""Finite candidate requests share the live owner, not another downloader."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import threading
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext import run_service
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, REQUEST_DEFERRAL_CODES
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_news_fulltext import FakeResponse, article_html, config as generic_config, http_fixture
from test_news_videoland_original import (
    URL, TITLE, CREDIT, PROSE, CAPTION, RELEASE, description_defect_page, versioned_config,
)


LAYOUT = 'videoland_visible_article_v4'


@pytest.fixture
def settings(tmp_path):
    result = versioned_config(3)
    result.update(root=str(tmp_path), min_free_disk_bytes=1, workers=2, exclusive_http_owner=False)
    result['adaptive_rate'] = {'enabled': False}
    result['providers'][0]['interval_seconds'] = .001
    return result


@pytest.fixture
def queue(settings, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    fail_head(owner)
    yield owner
    owner.close()


def head(queue):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())


def fail_head(queue, **changes):
    values = dict(state='article_identity_not_verified', error_code='article_identity_not_verified',
        attempts=7, due=time.time()-60, last_checked_at_utc='2026-01-01T00:00:00+00:00')
    values.update(changes)
    with queue.db:
        queue.add_articles([{'url': URL}])
        queue.db.execute('UPDATE articles SET '+','.join(key+'=?' for key in values)+' WHERE url=?',
            (*values.values(), URL))
    return head(queue)


def submit(queue, settings, *, reason='Reviewed native candidate; no acceptance'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, URL, LAYOUT, reason=reason)


def request_path(queue, submitted):
    return queue.root/'reviewed_repairs/candidate_diagnostics/pending'/(submitted['request_id']+'.json')


def http_stub(queue, *, response=None, error=None):
    calls = []
    def get(url):
        calls.append(url)
        if error:
            raise error
        return response or Response(url, 200, {'content-type': 'text/html'}, description_defect_page())
    return SimpleNamespace(registry=queue.registry, get=get, calls=calls, provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


def admit(queue, settings):
    submitted = submit(queue, settings)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        diagnostic.pump_candidates(queue)
        if getattr(queue, 'native_candidate_schema_ready', False):
            row = queue.db.execute('SELECT state FROM native_candidate_jobs WHERE request_id=?',
                                   (submitted['request_id'],)).fetchone()
            if row and row['state'] == 'ready':
                return submitted
        time.sleep(.005)  # An exhausted bounded pump slice is retried on a later tick.
    raise AssertionError('candidate was not admitted within the bounded test deadline')


def pump_until(queue, predicate):
    """Follow bounded owner slices until the specific observable effect occurs."""
    deadline = time.monotonic() + 10
    while True:
        diagnostic.pump_candidates(queue)
        if predicate():
            return
        if time.monotonic() >= deadline:
            raise AssertionError('candidate operation did not complete within the test deadline')
        time.sleep(.005)


def seal_request(path, data=None, *, raw=None):
    payload = diagnostic._bytes(data) if raw is None else raw
    sealed = path.with_name(hashlib.sha256(payload).hexdigest()+'.json')
    path.unlink()
    atomic_write_bytes(sealed, payload, mode=0o600)
    return sealed


def test_no_input_has_no_new_schema_or_writes(queue):
    total = queue.db.total_changes
    diagnostic.pump_candidates(queue)
    assert not diagnostic.has_unadmitted_candidates(queue)
    assert queue.db.total_changes == total
    assert not queue.db.execute("SELECT 1 FROM sqlite_master WHERE name='native_candidate_jobs'").fetchone()
    assert not (queue.root/'reviewed_repairs').exists()


def test_expired_admission_slice_leaves_pending_input_for_next_tick(queue, settings, monkeypatch):
    from downloader.news_fulltext_wake import OwnerMailboxWake

    submitted = submit(queue, settings)
    mailbox = OwnerMailboxWake(queue, threading.Event())
    original_schema = diagnostic._schema

    def slow_first_schema(owner):
        original_schema(owner)
        time.sleep(.06)  # Exhaust the first 50 ms pump slice after schema initialization.

    monkeypatch.setattr(diagnostic, '_schema', slow_first_schema)
    diagnostic.pump_candidates(queue)
    assert diagnostic.has_unadmitted_candidates(queue)
    assert mailbox.has_pending()
    assert request_path(queue, submitted).exists()
    assert queue.db.execute('SELECT COUNT(*) FROM native_candidate_jobs').fetchone()[0] == 0
    monkeypatch.setattr(diagnostic, '_schema', original_schema)
    deadline = time.monotonic() + 3
    while diagnostic.has_unadmitted_candidates(queue) and time.monotonic() < deadline:
        diagnostic.pump_candidates(queue)
        time.sleep(.005)
    assert not diagnostic.has_unadmitted_candidates(queue)
    assert not mailbox.has_pending()
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'ready'


def test_candidate_progress_separates_unadmitted_registered_and_reserved_work(queue, settings):
    before = head(queue)
    total = queue.db.total_changes
    empty = diagnostic.candidate_progress(queue)
    assert empty['available'] and empty['request_files'] == 0 and empty['providers'] == {}
    assert queue.db.total_changes == total and not (queue.root/'reviewed_repairs').exists()
    submitted = submit(queue, settings, reason='PRIVATE_REASON')
    progress = diagnostic.candidate_progress(queue)
    assert progress['request_files'] == progress['pending_unadmitted'] == 1
    assert progress['providers']['videoland']['pending_unadmitted'] == 1
    assert progress['jobs'] == {'ready': 0, 'reserved': 0, 'orphaned_reservation': 0, 'uncertain_outcome': 0}
    assert all(secret not in json.dumps(progress) for secret in ('PRIVATE_REASON', URL, submitted['request_id']))
    assert queue.status(activity='waiting_disk', publish=False)['candidate_diagnostic_progress'] == progress
    # Admission is deliberately time-sliced; the first slice may expire before
    # processing a file under I/O or scheduler load.
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        diagnostic.pump_candidates(queue)
        if diagnostic.candidate_progress(queue)['already_registered'] == 1:
            break
        time.sleep(.005)
    progress = diagnostic.candidate_progress(queue)
    assert progress['request_files'] == progress['already_registered'] == 1
    assert progress['pending_unadmitted'] == 0 and progress['jobs']['ready'] == 1
    assert progress['providers']['videoland']['ready'] == 1
    job = diagnostic.claim_candidate(queue, http_stub(queue), settings, 'videoland')
    assert diagnostic.candidate_progress(queue)['jobs'] == {'ready': 0, 'reserved': 1, 'orphaned_reservation': 0, 'uncertain_outcome': 0}
    diagnostic.finish_candidate(queue, job, error=RuntimeError('PRIVATE_ERROR'))
    done = diagnostic.candidate_progress(queue)
    assert done['already_registered'] == 1 and not any(done['jobs'].values())
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        diagnostic.pump_candidates(queue)
        if diagnostic.candidate_progress(queue)['request_files'] == 0:
            break
        time.sleep(.005)
    assert diagnostic.candidate_progress(queue)['request_files'] == 0
    assert head(queue) == before


def test_candidate_progress_deduplicates_registered_observations_without_merging_submissions(queue, settings):
    submitted = submit(queue, settings)
    path = request_path(queue, submitted)
    request = json.loads(path.read_bytes())
    request['reason'] = 'A different private submission'
    payload = diagnostic._bytes(request)
    other = path.with_name(hashlib.sha256(payload).hexdigest()+'.json')
    atomic_write_bytes(other, payload, mode=0o600)
    before = diagnostic.candidate_progress(queue)
    assert before['request_files'] == before['pending_unadmitted'] == 2
    pump_until(queue, lambda: diagnostic.candidate_progress(queue)['already_registered'] == 2)
    after = diagnostic.candidate_progress(queue)
    assert after['request_files'] == after['already_registered'] == 2
    assert after['pending_unadmitted'] == 0 and after['jobs']['ready'] == 1
    assert after['providers']['videoland']['ready'] == 1


def test_candidate_progress_cache_rechecks_file_identity_and_keeps_invalid_unknown(queue, settings, monkeypatch):
    submitted = submit(queue, settings)
    path = request_path(queue, submitted)
    original = diagnostic._load
    calls = []
    def counted(value):
        calls.append(value)
        return original(value)
    monkeypatch.setattr(diagnostic, '_load', counted)
    before = queue.db.total_changes
    assert diagnostic.candidate_progress(queue)['pending_unadmitted'] == 1
    assert diagnostic.candidate_progress(queue)['pending_unadmitted'] == 1
    assert len(calls) == 1
    atomic_write_bytes(path, path.read_bytes()+b' ', mode=0o600)
    result = diagnostic.candidate_progress(queue)
    assert result['request_files'] == result['invalid_or_unreadable'] == 1
    assert result['pending_unadmitted'] == 0 and len(calls) == 2
    assert queue.db.total_changes == before and path.exists()


def test_candidate_progress_unreadable_directory_does_not_invent_zero(queue, settings):
    submitted = submit(queue, settings)
    path = request_path(queue, submitted)
    path.parent.chmod(0o755)
    try:
        assert diagnostic.candidate_progress(queue) == {'contract_version': 1, 'available': False}
        assert path.exists()
    finally:
        path.parent.chmod(0o700)


def test_live_submission_and_once_only_same_envelope_never_change_heads_or_settings(queue, settings, monkeypatch):
    before, owner = head(queue), queue.owner
    budget = list(queue.db.execute('SELECT * FROM queue_settings'))
    total = queue.db.total_changes
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no second HTTP owner'))
    submitted = submit(queue, settings)
    assert submitted['submitted'] and submitted['publisher_requests'] == 0
    assert queue.db.total_changes == total and head(queue) == before and queue.owner == owner
    path = request_path(queue, submitted)
    assert path.stat().st_mode & 0o777 == 0o600 and path.parent.stat().st_mode & 0o777 == 0o700
    assert diagnostic.has_unadmitted_candidates(queue)
    assert submit(queue, settings, reason='Different prose cannot grant another observation')['request_id'] == submitted['request_id']
    pump_until(queue, lambda: not diagnostic.has_unadmitted_candidates(queue))
    assert not diagnostic.has_unadmitted_candidates(queue)
    http = http_stub(queue)
    job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'reserved'
    result = diagnostic.fetch_candidate(http, job)
    assert http.calls == [URL] and result['current_error'] == 'article_identity_not_verified'
    assert result['candidate_parser']['parse_success']
    assert result['candidate_parser']['published_at_raw'] == RELEASE
    assert result['timing']['total_seconds'] >= result['timing']['http_seconds']
    private = json.dumps(result, ensure_ascii=False)
    assert all(text not in private for text in (TITLE, CREDIT, PROSE, CAPTION))
    diagnostic.finish_candidate(queue, job, result=result)
    pump_until(queue, lambda: not path.exists())
    state = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert state['state'] == 'observed' and state['accepted_as_article'] is False and state['publish'] is False
    assert head(queue) == before and queue.owner == owner and list(queue.db.execute('SELECT * FROM queue_settings')) == budget
    assert not path.exists()
    archived = path.parent.parent/'processed'/path.name
    assert hashlib.sha256(archived.read_bytes()).hexdigest() == submitted['request_id']
    assert json.loads(archived.with_suffix('.outcome.json').read_bytes())['owner_id'] == owner
    assert submit(queue, settings)['submitted'] is False
    assert diagnostic.claim_candidate(queue, http, settings, 'videoland') is None
    assert http.calls == [URL] and not (queue.root/'batches').exists()


@pytest.mark.parametrize('changes', [
    {'body_sha256': 'a'*64}, {'parquet_path': 'original.parquet'}, {'body_bytes': 1},
    {'captured_at_utc': RELEASE}, {'lease_owner': 'other'}, {'lease_until': 1},
    {'due': time.time()+86400}, {'due': float('inf')}, {'attempts': -1},
    {'last_checked_at_utc': '2099-01-01T00:00:00Z'}, {'last_checked_at_utc': '2026-01-01'},
    {'state': 'http_403', 'error_code': 'http_403'}, {'state': 'robots_denied', 'error_code': 'robots_denied'},
    {'state': 'body_not_found', 'error_code': 'body_not_found'},
])
def test_protected_heads_never_submit(queue, settings, changes):
    before = fail_head(queue, **changes)
    with pytest.raises((ValueError, NewsError)):
        submit(queue, settings)
    assert head(queue) == before and not (queue.root/'reviewed_repairs').exists()


@pytest.mark.parametrize('change', ['head', 'source', 'candidate'])
def test_owner_rechecks_head_and_source_without_http(queue, settings, change):
    submitted = admit(queue, settings)
    cfg = settings
    if change == 'head':
        with queue.db:
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
    elif change == 'source':
        source = queue.registry.sources['videoland']
        queue.registry.sources['videoland'] = replace(source, authorization_fingerprint='f'*64)
    else:
        cfg = deepcopy(settings)
        cfg['providers'][0]['require_author_attribution'] = False
    before = head(queue)
    http = http_stub(queue)
    assert diagnostic.claim_candidate(queue, http, cfg, 'videoland') is None
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'skipped'
    assert http.calls == [] and head(queue) == before


@pytest.mark.parametrize('boundary', ['provider', 'host', 'robots'])
def test_refusal_deadlines_remain_ready_without_reservation_or_idle_spin(queue, settings, boundary):
    submitted = admit(queue, settings)
    http = http_stub(queue)
    if boundary == 'provider':
        http.provider_not_before['videoland'] = time.time()+600
    elif boundary == 'host':
        http.blocked_gateway_hosts = lambda p: ('news.videoland.com.tw',)
    else:
        http.blocked_robots_origins = lambda p: ('https://news.videoland.com.tw',)
    assert diagnostic.claim_candidate(queue, http, settings, 'videoland') is None
    assert not diagnostic.has_unadmitted_candidates(queue)
    assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'ready'
    assert not http.calls


def test_unsent_candidate_after_robots_gate_retries_without_losing_observation(queue, settings, tmp_path, monkeypatch):
    submitted, before = admit(queue, settings), head(queue)
    http, session = http_fixture(settings, tmp_path, {
        'https://news.videoland.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(description_defect_page()),
    })
    try:
        job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
        original_raw = http._raw

        def gate_candidate(url, *args, **kwargs):
            if url == URL:
                http.defer('videoland', 3)
            return original_raw(url, *args, **kwargs)

        monkeypatch.setattr(http, '_raw', gate_candidate)
        with pytest.raises(NewsError) as blocked:
            diagnostic.fetch_candidate(http, job)
        assert blocked.value.request_deferred and blocked.value.initial_request_not_sent is True
        assert [url for url, _ in session.calls] == ['https://news.videoland.com.tw/robots.txt']
        assert http.requests_by_provider['videoland'] == 1
        diagnostic.finish_candidate(queue, job, error=blocked.value)
        status = diagnostic.candidate_status(queue.root, submitted['request_id'])
        assert status['state'] == 'ready' and status['last_request_deferral']['waiting']
        assert status['last_request_deferral']['last_gate_code'] == 'provider_cooldown'
        assert status['last_request_deferral']['deferral_count'] == 1
        assert request_path(queue, submitted).exists() and head(queue) == before
        assert diagnostic.claim_candidate(queue, http, settings, 'videoland') is None
        monkeypatch.setattr(http, '_raw', original_raw)
        clock = time.time
        monkeypatch.setattr(time, 'time', lambda: clock() + 4)
        job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
        diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
        diagnostic.pump_candidates(queue)
        assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'
        assert [url for url, _ in session.calls] == ['https://news.videoland.com.tw/robots.txt', URL]
        assert http.requests_by_provider['videoland'] == 2 and head(queue) == before
    finally:
        http.close()


@pytest.mark.parametrize('code', sorted(REQUEST_DEFERRAL_CODES))
def test_each_proven_presend_gate_keeps_candidate_ready_with_deadline(queue, settings, tmp_path, monkeypatch, code):
    submitted = admit(queue, settings)
    http, session = http_fixture(settings, tmp_path, {
        'https://news.videoland.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(description_defect_page()),
    })
    try:
        job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
        original_check = http._check_request_gates

        def hold_initial(provider, url, **kwargs):
            if url == URL:
                raise NewsError(code, retry=True, delay=3, request_deferred=True)
            return original_check(provider, url, **kwargs)

        monkeypatch.setattr(http, '_check_request_gates', hold_initial)
        with pytest.raises(NewsError) as blocked:
            diagnostic.fetch_candidate(http, job)
        assert blocked.value.initial_request_not_sent is True
        diagnostic.finish_candidate(queue, job, error=blocked.value)
        status = diagnostic.candidate_status(tmp_path, submitted['request_id'])
        assert status['state'] == 'ready' and status['last_request_deferral']['last_gate_code'] == code
        assert 2 < diagnostic.candidate_next_wake_delay(queue, {}, {}, 60) <= 3
        assert diagnostic.claim_candidate(queue, http, settings, 'videoland') is None
        assert [url for url, _ in session.calls] == ['https://news.videoland.com.tw/robots.txt']
    finally:
        http.close()


def test_candidate_presend_gate_survives_restart_and_wakes_for_latest_source_deadline(queue, settings, tmp_path, monkeypatch):
    submitted, before = admit(queue, settings), head(queue)
    http, session = http_fixture(settings, tmp_path, {
        'https://news.videoland.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(description_defect_page()),
    })
    try:
        job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
        http.defer('videoland', 3)
        with pytest.raises(NewsError) as blocked:
            diagnostic.fetch_candidate(http, job)
        assert blocked.value.initial_request_not_sent is True and not session.calls
        diagnostic.finish_candidate(queue, job, error=blocked.value)
        queue.close()
        reopened = NewsQueue(tmp_path, http.registry)
        try:
            diagnostic.pump_candidates(reopened)
            status = diagnostic.candidate_status(tmp_path, submitted['request_id'])
            assert status['state'] == 'ready' and status['last_request_deferral']['waiting']
            assert 2 < diagnostic.candidate_next_wake_delay(reopened, {}, {}, 60) <= 3
            now = time.time()
            scopes = {'videoland': {'hosts': {'news.videoland.com.tw': now+5},
                                    'origins': {'https://news.videoland.com.tw': now+7}}}
            assert 6 < diagnostic.candidate_next_wake_delay(reopened, {'videoland': now+6}, scopes, 60) <= 7
            assert diagnostic.claim_candidate(reopened, http, settings, 'videoland') is None
            clock = time.time
            monkeypatch.setattr(time, 'time', lambda: clock() + 8)
            assert diagnostic.candidate_next_wake_delay(reopened, {}, {}, 60) == 60  # No due=0 spin.
            job = diagnostic.claim_candidate(reopened, http, settings, 'videoland')
            diagnostic.finish_candidate(reopened, job, result=diagnostic.fetch_candidate(http, job))
            diagnostic.pump_candidates(reopened)
            status = diagnostic.candidate_status(tmp_path, submitted['request_id'])
            assert status['state'] == 'observed' and not status['last_request_deferral']['waiting']
            assert [url for url, _ in session.calls] == ['https://news.videoland.com.tw/robots.txt', URL]
            assert head(reopened) == before
        finally:
            reopened.close()
    finally:
        http.close()


def test_candidate_deadline_wake_uses_bounded_provider_index(settings, generic_config, tmp_path):
    settings['providers'].extend(generic_config['providers'])
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        diagnostic._schema(queue)
        now = time.time()
        with queue.db:
            for provider, request_id, due in [('videoland', 'v', now + 8), ('publisher', 'p', now + 2)]:
                target = URL if provider == 'videoland' else 'https://publisher.test/story'
                queue.db.execute('INSERT INTO native_candidate_jobs '
                    '(request_id,observation_key,provider,state,request_json,created_at_utc,owner_id) '
                    "VALUES (?,?,?,'ready',?,?,?)",
                    (request_id, request_id, provider, json.dumps({'target': {'url': target}}),
                     '2026-01-01T00:00:00+00:00', queue.owner))
                queue.db.execute('INSERT INTO native_candidate_wait VALUES (?,?,?,?,1)',
                    (request_id, due, 'provider_cooldown', '2026-01-01T00:00:00+00:00'))
        queue.native_candidate_ready_providers = {'videoland', 'publisher'}
        assert 1 < diagnostic.candidate_next_wake_delay(queue, {}, {}, 60) <= 2
        plan = queue.db.execute('EXPLAIN QUERY PLAN SELECT j.request_json,w.not_before_epoch '
            'FROM native_candidate_jobs AS j LEFT JOIN native_candidate_wait AS w ON w.request_id=j.request_id '
            "WHERE j.state='ready' AND j.provider=? ORDER BY j.created_at_utc LIMIT ?", ('videoland', 16)).fetchall()
        details = ' '.join(row[3] for row in plan)
        assert 'native_candidate_ready' in details and 'TEMP B-TREE' not in details
    finally:
        queue.close()


def test_gate_shaped_observer_error_after_send_does_not_rearm_candidate(queue, settings, tmp_path):
    submitted = admit(queue, settings)
    http, session = http_fixture(settings, tmp_path, {
        'https://news.videoland.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(description_defect_page()),
    })
    try:
        def fail_after_headers(stage, response):
            if stage == 'headers' and response.url == URL:
                error = NewsError('provider_cooldown', retry=True, delay=3, request_deferred=True)
                error.raw_request_not_sent = True  # Even a stale annotation cannot undo an actual send.
                raise error

        http.access_observer = fail_after_headers
        job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
        with pytest.raises(NewsError) as blocked:
            diagnostic.fetch_candidate(http, job)
        assert blocked.value.request_deferred and not blocked.value.initial_request_not_sent
        diagnostic.finish_candidate(queue, job, error=blocked.value)
        diagnostic.pump_candidates(queue)
        assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed_error'
        assert diagnostic.claim_candidate(queue, http, settings, 'videoland') is None
        assert [url for url, _ in session.calls] == ['https://news.videoland.com.tw/robots.txt', URL]
    finally:
        http.close()


def test_candidate_gate_after_initial_redirect_does_not_rearm_observation(queue, settings, tmp_path, monkeypatch):
    submitted = admit(queue, settings)
    target = URL.replace('123456789abc', '123456789abd')
    http, session = http_fixture(settings, tmp_path, {
        'https://news.videoland.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(status=302, headers={'Location': target}),
        target: FakeResponse(description_defect_page()),
    })
    try:
        job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
        original_raw = http._raw

        def gate_after_first_request(url, *args, **kwargs):
            result = original_raw(url, *args, **kwargs)
            if url == URL:
                http.defer('videoland', 3)
            return result

        monkeypatch.setattr(http, '_raw', gate_after_first_request)
        with pytest.raises(NewsError) as blocked:
            diagnostic.fetch_candidate(http, job)
        assert blocked.value.request_deferred and not getattr(blocked.value, 'initial_request_not_sent', False)
        assert [url for url, _ in session.calls] == ['https://news.videoland.com.tw/robots.txt', URL]
        diagnostic.finish_candidate(queue, job, error=blocked.value)
        diagnostic.pump_candidates(queue)
        assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed_error'
        clock = time.time
        monkeypatch.setattr(time, 'time', lambda: clock() + 4)
        assert diagnostic.claim_candidate(queue, http, settings, 'videoland') is None
        assert [url for url, _ in session.calls] == ['https://news.videoland.com.tw/robots.txt', URL]
    finally:
        http.close()


def test_resident_candidate_wait_wakes_after_presend_gate(settings, tmp_path, monkeypatch):
    settings.update(service_idle_max_seconds=10)
    http, session = http_fixture(settings, tmp_path, {
        'https://news.videoland.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n'),
        URL: FakeResponse(description_defect_page()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        before = fail_head(queue)
        submitted = submit(queue, settings)
        original_fetch = diagnostic.fetch_candidate
        gated = []

        def gated_once(client, job):
            if not gated:
                gated.append(time.monotonic())
                client.defer('videoland', .25)
            return original_fetch(client, job)

        monkeypatch.setattr(diagnostic, 'fetch_candidate', gated_once)
        run_service(queue, http, settings, max_articles=0, max_discovery_requests=0, duration_seconds=1.5)
        assert diagnostic.candidate_status(tmp_path, submitted['request_id'])['state'] == 'observed'
        assert len(gated) == 1 and [url for url, _ in session.calls] == [
            'https://news.videoland.com.tw/robots.txt', URL]
        assert dict(queue.db.execute('SELECT key,value FROM counters'))['native_candidate_gate_deferrals'] == 1
        assert head(queue) == before
    finally:
        http.close()
        queue.close()


def test_restart_never_retries_a_reserved_unknown_outcome(queue, settings):
    submitted = admit(queue, settings)
    assert diagnostic.claim_candidate(queue, http_stub(queue), settings, 'videoland')
    before, previous = head(queue), queue.owner
    queue.close()
    reopened = NewsQueue(queue.root, queue.registry)
    try:
        assert reopened.owner != previous
        before_recovery = diagnostic.candidate_progress(reopened)
        assert before_recovery['jobs']['reserved'] == 0
        assert before_recovery['jobs']['orphaned_reservation'] == 1
        assert before_recovery['providers']['videoland']['orphaned_reservation'] == 1
        diagnostic.pump_candidates(reopened)
        result = diagnostic.candidate_status(reopened.root, submitted['request_id'])
        assert result['state'] == 'uncertain_outcome' and result['result_json'] is None
        progress = diagnostic.candidate_progress(reopened)
        assert progress['jobs']['uncertain_outcome'] == 1
        assert progress['providers']['videoland']['uncertain_outcome'] == 1
        assert diagnostic.claim_candidate(reopened, http_stub(reopened), settings, 'videoland') is None
        assert head(reopened) == before
    finally:
        reopened.close()


def test_different_pending_identity_and_processed_replay_still_cannot_buy_second_get(queue, settings):
    submitted = admit(queue, settings)
    path = request_path(queue, submitted)
    request = json.loads(path.read_bytes())
    request['reason'] = 'A second reason cannot re-arm an observation'
    payload = diagnostic._bytes(request)
    other = path.with_name(hashlib.sha256(payload).hexdigest()+'.json')
    atomic_write_bytes(other, payload, mode=0o600)
    diagnostic.pump_candidates(queue)
    assert queue.db.execute('SELECT COUNT(*) FROM native_candidate_jobs').fetchone()[0] == 1
    http = http_stub(queue)
    job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    diagnostic.pump_candidates(queue)
    outcome = json.loads((other.parent.parent/'processed'/(other.stem+'.outcome.json')).read_bytes())
    assert outcome['duplicate_of'] == submitted['request_id'] and not other.exists()
    archived = path.parent.parent/'processed'/path.name
    atomic_write_bytes(path, archived.read_bytes(), mode=0o600)
    diagnostic.pump_candidates(queue)
    assert not path.exists() and diagnostic.claim_candidate(queue, http, settings, 'videoland') is None
    assert http.calls == [URL]


def test_unsafe_mailbox_does_not_turn_idle_owner_into_a_busy_loop(queue, settings):
    submitted = submit(queue, settings)
    folder = request_path(queue, submitted).parent
    folder.chmod(0o755)
    assert not diagnostic.has_unadmitted_candidates(queue)
    diagnostic.pump_candidates(queue)
    assert not getattr(queue, 'native_candidate_schema_ready', False)
    folder.chmod(0o700)
    assert diagnostic.has_unadmitted_candidates(queue)


def test_commit_then_archive_failure_resumes_with_zero_get(queue, settings, monkeypatch):
    submitted = admit(queue, settings)
    http = http_stub(queue)
    job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(http, job))
    archive = diagnostic._archive
    monkeypatch.setattr(diagnostic, '_archive', lambda *a, **k: (_ for _ in ()).throw(OSError('synthetic')))
    diagnostic.pump_candidates(queue)
    assert request_path(queue, submitted).exists()
    monkeypatch.setattr(diagnostic, '_archive', archive)
    queue.close()
    reopened = NewsQueue(queue.root, queue.registry)
    try:
        diagnostic.pump_candidates(reopened)
        assert not request_path(reopened, submitted).exists()
        assert diagnostic.candidate_status(reopened.root, submitted['request_id'])['state'] == 'observed'
        assert diagnostic.claim_candidate(reopened, http, settings, 'videoland') is None
        assert http.calls == [URL]
    finally:
        reopened.close()


@pytest.mark.parametrize('error', [RuntimeError('PRIVATE TRACE AND SOURCE'), NewsError('http_429', retry=True, delay=700)])
def test_worker_failure_is_finite_private_and_never_resets_article(queue, settings, error):
    submitted, before = admit(queue, settings), head(queue)
    job = diagnostic.claim_candidate(queue, http_stub(queue), settings, 'videoland')
    diagnostic.finish_candidate(queue, job, error=error)
    diagnostic.pump_candidates(queue)
    state = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert state['state'] == 'observed_error' and not state['accepted_as_article']
    assert 'PRIVATE' not in json.dumps(state) and head(queue) == before
    assert not submit(queue, settings)['submitted']


@pytest.mark.parametrize('bad', [None, {}, {'accepted_as_article': True},
    {'accepted_as_article': False, 'raw_or_names_or_body_retained': False, 'n': float('nan')}])
def test_invalid_worker_result_does_not_kill_other_work(queue, settings, bad):
    submitted = admit(queue, settings)
    job = diagnostic.claim_candidate(queue, http_stub(queue), settings, 'videoland')
    diagnostic.finish_candidate(queue, job, result=bad)
    state = diagnostic.candidate_status(queue.root, submitted['request_id'])
    assert state['state'] == 'observed_error' and state['result_json']['error'] == 'invalid_candidate_result'


@pytest.mark.parametrize('malform', ['permissions', 'symlink', 'hash', 'keys', 'provider', 'state', 'nan', 'source'])
def test_bad_mailbox_is_quarantined_without_http_or_head_changes(queue, settings, tmp_path, malform):
    submitted, before = submit(queue, settings), head(queue)
    path = request_path(queue, submitted)
    request = json.loads(path.read_bytes())
    if malform == 'permissions':
        path.chmod(0o644)
    elif malform == 'symlink':
        outside = tmp_path/'outside.private'
        outside.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(outside)
    elif malform == 'hash':
        atomic_write_bytes(path, path.read_bytes()+b' ', mode=0o600)
    elif malform == 'keys':
        path = seal_request(path, raw=b'{"contract_version":1,"contract_version":1}')
    else:
        if malform == 'provider':
            request['target']['provider'] = 'not_registered'
            request['observation_key'] = diagnostic._key(request['target'], request['candidate_fingerprint'])
        elif malform == 'state':
            request['target']['state'] = 'retry'
        elif malform == 'source':
            request['source']['authorization_fingerprint'] = 'not-a-hash'
        else:
            path = seal_request(path, raw=path.read_bytes().replace(b'"attempts":7', b'"attempts":NaN'))
        if malform != 'nan':
            path = seal_request(path, request)
    diagnostic.pump_candidates(queue)
    assert not path.exists() and head(queue) == before
    assert queue.db.execute('SELECT COUNT(*) FROM native_candidate_jobs').fetchone()[0] == 0
    assert not diagnostic.has_unadmitted_candidates(queue)
    if malform == 'symlink':
        assert outside.exists()


def test_stop_and_soft_slice_defer_without_reserving(queue, settings, monkeypatch):
    submitted = submit(queue, settings)
    stop = threading.Event()
    stop.set()
    diagnostic.pump_candidates(queue, stop=stop)
    assert request_path(queue, submitted).exists()
    assert queue.db.execute('SELECT COUNT(*) FROM native_candidate_jobs').fetchone()[0] == 0
    ticks = iter([0, .1])
    monkeypatch.setattr(diagnostic.time, 'monotonic', lambda: next(ticks))
    diagnostic.pump_candidates(queue, max_seconds=.05)
    assert queue.db.execute('SELECT COUNT(*) FROM native_candidate_jobs').fetchone()[0] == 0


@pytest.mark.parametrize('kind', ['redirect', 'mime', 'http'])
def test_response_contract_is_not_replaced_by_candidate_parsing(queue, settings, kind):
    admit(queue, settings)
    response = Response(URL, 200, {'content-type': 'text/html'}, description_defect_page())
    if kind == 'redirect':
        response = replace(response, url=URL.replace('.html', 'different.html'))
    elif kind == 'mime':
        response = replace(response, headers={'content-type': 'application/json'})
    else:
        response = replace(response, status=503)
    http = http_stub(queue, response=response)
    job = diagnostic.claim_candidate(queue, http, settings, 'videoland')
    with pytest.raises(NewsError):
        diagnostic.fetch_candidate(http, job)
    assert http.calls == [URL]


def test_cli_enqueue_and_readback_never_construct_http_or_other_owner(queue, settings, monkeypatch, tmp_path):
    from scripts import probe_news_sources as cli

    monkeypatch.setattr(cli, 'load_config', lambda p: (settings, queue.registry, tmp_path))
    monkeypatch.setattr(cli, 'NewsHTTP', lambda *a, **k: pytest.fail('resident submission must not create HTTP'))
    output = tmp_path/'private/submission.json'
    before = head(queue)
    args = ['--config', str(tmp_path/'config.json'), '--output', str(output), '--current-failure-provider', 'videoland',
        '--maximum-failure-probes', '1', '--failure-code', 'article_identity_not_verified',
        '--candidate-embedded-layout', LAYOUT, '--enqueue-candidate']
    assert cli.main(args) == 0
    state = json.loads(output.read_bytes())
    assert state['submitted'] and state['publisher_requests'] == 0 and head(queue) == before
    assert output.stat().st_mode & 0o777 == 0o600
    assert cli.main(['--config', str(tmp_path/'config.json'), '--output', str(output),
        '--candidate-request-status', state['request_id']]) == 0
    assert json.loads(output.read_bytes())['state'] == 'pending' and head(queue) == before


def test_resident_ordinary_work_has_first_slot_and_runs_while_candidate_is_inflight(settings, generic_config, tmp_path, monkeypatch):
    healthy = 'https://publisher.test/story/ordinary'
    settings['providers'].extend(generic_config['providers'])
    ordinary_started, candidate_started = threading.Event(), threading.Event()
    dispatched = []

    class RecordingExecutor(ThreadPoolExecutor):
        def submit(self, function, *args, **kwargs):
            dispatched.append(function.__name__)
            return super().submit(function, *args, **kwargs)

    monkeypatch.setattr('downloader.news_fulltext.ThreadPoolExecutor', RecordingExecutor)

    class Ordinary(FakeResponse):
        def iter_content(self, **kwargs):
            ordinary_started.set()
            assert candidate_started.wait(2), 'another provider must not wait for ordinary global quiescence'
            yield self.body

    class Candidate(FakeResponse):
        def iter_content(self, **kwargs):
            assert ordinary_started.wait(2), 'ordinary work must have the first worker slot'
            candidate_started.set()
            yield self.body

    http, session = http_fixture(settings, tmp_path, {
        'https://news.videoland.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: Candidate(description_defect_page()), healthy: Ordinary(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        before, owner = fail_head(queue), queue.owner
        with queue.db:
            queue.add_articles([{'url': healthy}])
        submitted = submit(queue, settings)
        result = run_service(queue, http, settings, max_articles=1, max_discovery_requests=0, duration_seconds=.2)
        assert ordinary_started.is_set() and candidate_started.is_set()
        assert head(queue) == before and queue.owner == owner
        assert diagnostic.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'
        assert result['states']['complete'] == 1 and verify_bodies(tmp_path)['accepted']
        urls = [url for url, _ in session.calls]
        assert urls.count(URL) == urls.count(healthy) == 1
        # Priority owns dispatch slots, not another origin's REP/transport
        # completion time. Two independent robots responses may race.
        assert dispatched.index('fetch_article') < dispatched.index('fetch_candidate')
        assert result['performance']['providers']['videoland']['counts']['native_candidate_observed'] == 1
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('condition', ['stopped', 'low_disk'])
def test_service_defers_candidate_without_busy_wake_during_stop_or_low_disk(settings, tmp_path, monkeypatch, condition):
    http, session = http_fixture(settings, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        before, submitted = fail_head(queue), submit(queue, settings)
        if condition == 'stopped':
            http.stop.set()
        else:
            monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda p: SimpleNamespace(free=0))
            monkeypatch.setattr(diagnostic, 'has_unadmitted_candidates', lambda q: pytest.fail('no hot wake while waiting_disk'))
        run_service(queue, http, settings, max_articles=1, max_discovery_requests=0, duration_seconds=.03)
        assert request_path(queue, submitted).exists() and head(queue) == before and not session.calls
    finally:
        http.close()
        queue.close()
