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
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
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
    diagnostic.pump_candidates(queue)
    return submitted


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
    diagnostic.pump_candidates(queue)
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
    diagnostic.pump_candidates(queue)
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


def test_restart_never_retries_a_reserved_unknown_outcome(queue, settings):
    submitted = admit(queue, settings)
    assert diagnostic.claim_candidate(queue, http_stub(queue), settings, 'videoland')
    before, previous = head(queue), queue.owner
    queue.close()
    reopened = NewsQueue(queue.root, queue.registry)
    try:
        assert reopened.owner != previous
        diagnostic.pump_candidates(reopened)
        result = diagnostic.candidate_status(reopened.root, submitted['request_id'])
        assert result['state'] == 'uncertain_outcome' and result['result_json'] is None
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
