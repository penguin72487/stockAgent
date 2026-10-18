"""Real local owner generation, initialization handoff and honest snapshot UI."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
import threading

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from stockagent.data.news_worker_lifecycle import NewsWorkerLifecycle, read_news_worker_lifecycle
from stockagent.live.news_dashboard import build_news_public_status, news_snapshot_revision
from test_news_fulltext import config as config
from test_news_dashboard import _seed, _save
from test_news_dashboard_cnyes_list_progress import browser


@pytest.mark.parametrize('audience,cache,valid', [
    ('owner', 'private, no-store', True), ('owner', 'no-store', False),
    ('public', 'no-store', False), ('public', 'private, no-store', False),
    (None, 'private, no-store', False), ('owner', 'public, max-age=15', False),
])
def test_live_verifier_requires_actual_owner_no_cache_contract(audience, cache, valid):
    from scripts.verify_news_dashboard import require_owner_transport
    headers = {'x-stockagent-audience': audience, 'cache-control': cache}
    if valid:
        require_owner_transport(headers)
    else:
        with pytest.raises(AssertionError):
            require_owner_transport(headers)


def seed(root, *, now=None):
    now = now or datetime.now(UTC)
    status = _seed(root, now=now - timedelta(seconds=10))
    status.update(owner_id='a' * 32, activity='stopped')
    _save(root, status)
    lifecycle = NewsWorkerLifecycle(root / 'data_news_fulltext')
    lifecycle.started('b' * 32)
    return status, lifecycle


def test_actual_generation_proves_startup_without_promoting_old_counts_or_speed(tmp_path):
    old, lifecycle = seed(tmp_path)
    lifecycle.phase('acceptance_recovery', receipts_decoded=128)
    result = build_news_public_status(tmp_path)
    assert result['health'] == result['worker']['activity'] == 'initializing'
    assert result['worker']['source_activity'] == 'stopped' and not result['worker']['fresh']
    assert result['worker']['requests'] is result['worker']['owner_cpu_seconds'] is None
    assert result['acquisition']['stored_body_articles'] == 2
    assert result['acquisition']['eligible_eta_seconds'] is None
    assert result['providers'][0]['conditional_eta_seconds'] is None
    assert result['providers'][0]['owner_requests'] is None
    assert result['providers'][0]['owner_accepted_versions'] is None
    assert result['stages'] == []
    assert result['providers'][0]['state'] == 'initializing'
    public = result['worker']['lifecycle']
    assert public['process_alive'] and public['supersedes_snapshot'] and public['receipts_decoded'] == 128
    assert not public['snapshot_owner_matches']
    assert not {'process_owner', 'owner_id', 'pid', 'boot_id', 'start_ticks'} & public.keys()
    encoded = json.dumps(result)
    assert 'PRIVATE_TOKEN' not in encoded and 'PRIVATE_BODY' not in encoded


def test_long_startup_is_progress_stale_not_stopped_or_live_forecast(tmp_path):
    _, lifecycle = seed(tmp_path)
    future = datetime.now(UTC) + timedelta(seconds=300)
    result = build_news_public_status(tmp_path, now=future)
    assert result['health'] == 'initializing_stale' and result['worker']['lifecycle']['process_alive']
    assert result['acquisition']['stored_body_articles'] == 2
    assert result['acquisition']['eligible_eta_seconds'] is None


def test_initialization_keeps_old_manifest_counts_but_never_its_forecast(tmp_path):
    from test_news_dashboard_cnyes_manifest_phase import fixture
    old, _ = fixture(tmp_path, now=datetime.now(UTC))
    _save(tmp_path, old)
    lifecycle = NewsWorkerLifecycle(tmp_path / 'data_news_fulltext')
    lifecycle.started('b' * 32)
    result = build_news_public_status(tmp_path)
    phase = next(row for row in result['providers'] if row['id'] == 'cnyes')['manifest_phase_progress']
    assert result['health'] == 'initializing'
    assert phase['verified_pages'] == 340 and phase['progress_ratio'] == .34
    assert phase['forecast']['state'] == 'inactive'
    assert phase['forecast']['middle_seconds'] is phase['forecast']['middle_finish_at_utc'] is None


def test_lifecycle_generation_invalidates_cached_old_snapshot_without_queue_scan(tmp_path):
    _seed(tmp_path, now=datetime.now(UTC))
    before = news_snapshot_revision(tmp_path)
    lifecycle = NewsWorkerLifecycle(tmp_path / 'data_news_fulltext')
    lifecycle.started('b' * 32)
    started = news_snapshot_revision(tmp_path)
    assert started != before
    lifecycle.phase('acceptance_recovery', receipts_decoded=128)
    assert news_snapshot_revision(tmp_path) != started


@pytest.mark.parametrize('width', [1280, 320])
def test_actual_owner_page_initializes_without_old_rate_then_adopts_snapshot(tmp_path, browser, width):
    from test_news_dashboard import _server
    old, lifecycle = seed(tmp_path)
    lifecycle.phase('acceptance_recovery', receipts_decoded=128)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f'http://127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': width, 'height': 900})
    errors, remote = [], []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.on('request', lambda request: remote.append(request.url) if not request.url.startswith(base) else None)
    try:
        page.goto(base + '/news/', wait_until='domcontentloaded', timeout=15000)
        page.locator('#news-health').get_by_text('啟動中，恢復本機資料').wait_for(timeout=10000)
        assert page.locator('#body-count').text_content() == '2'
        assert '數量沿用前次快照' in page.locator('#news-freshness').text_content()
        assert '128 份收據；不是新增新聞篇數' in page.locator('#alert-copy').text_content()
        assert page.locator('#capture-rate').text_content() == '等待有效量測'
        assert page.locator('#eligible-eta').text_content() == '目前階段暫無有效估時'
        assert page.evaluate('!currentSnapshot(state.latest) && !live(state.latest)')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        screenshot = tmp_path / f'news-owner-initialization-{width}.png'
        page.screenshot(path=str(screenshot), full_page=False)
        print(f'lifecycle_screenshot={screenshot}')
        old.update(owner_id='b' * 32, activity='running', updated_at_utc=datetime.now(UTC).isoformat())
        _save(tmp_path, old)
        lifecycle.snapshot_written()
        page.evaluate('refresh()')
        page.locator('#news-health').get_by_text('正在抓取').wait_for(timeout=10000)
        assert page.locator('#body-count').text_content() == '2'
        assert page.evaluate('live(state.latest) && state.latest.worker.lifecycle.snapshot_owner_matches')
        assert not errors and not remote
    finally:
        page.close()
        server.shutdown(); server.server_close(); thread.join(timeout=2)
        lifecycle.finish()


@pytest.mark.parametrize('width', [1280, 320])
def test_long_source_alert_defaults_compact_and_native_toggle_survives_refresh(tmp_path, browser, width):
    from test_news_dashboard import _server
    status = _seed(tmp_path, now=datetime.now(UTC))
    status['performance']['robots_origin_policy_contract_version'] = 1
    status['http_capacity'] = {'providers': {'cnyes': {'effective_interval_seconds': .5,
        'parallelism': 3, 'temporarily_paused_robots_origins': {
            'https://amp-news.cnyes.com': datetime.now(UTC).timestamp() + 120}}}}
    config_path = tmp_path / 'configs/news_fulltext.json'
    config = json.loads(config_path.read_bytes())
    for index in range(26):
        key = f'additional_{index}'
        config['providers'].append({'id': key, 'name': f'完整保留的來源 {index}', 'enabled': True})
        status['providers'][key] = {'states': {'article_identity_not_verified': 1},
            'automatic_collection_enabled': True}
    status['unique_articles'] += 26
    status['states']['article_identity_not_verified'] = 26
    atomic_write_json(config_path, config)
    _save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    page = browser.new_page(viewport={'width': width, 'height': 900})
    try:
        page.goto(f'http://127.0.0.1:{server.server_address[1]}/news/', wait_until='domcontentloaded')
        page.locator('#news-health').get_by_text('正在抓取').wait_for(timeout=10000)
        assert '27 個' in page.locator('#alert-title').text_content()
        details = page.locator('#alert-details')
        assert details.is_visible() and not details.evaluate('(element) => element.open')
        assert page.locator('#pipeline-alert').bounding_box()['height'] < 400
        details.locator('summary').click()
        assert details.evaluate('(element) => element.open')
        assert '完整保留的來源 25' in page.locator('#alert-source-copy').text_content()
        assert '局部端點暫停 1 個 origin' in page.locator('#alert-source-copy').text_content()
        page.evaluate('refresh()')
        page.wait_for_function('!state.inFlight')
        assert details.evaluate('(element) => element.open')
        details.locator('summary').focus()
        page.keyboard.press('Enter')
        assert not details.evaluate('(element) => element.open')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    finally:
        page.close()
        server.shutdown(); server.server_close(); thread.join(timeout=2)


@pytest.mark.parametrize('reason', ['dead', 'closed', 'failed'])
def test_dead_or_closed_owner_cannot_leave_fresh_running_snapshot(tmp_path, monkeypatch, reason):
    old, lifecycle = seed(tmp_path)
    old.update(owner_id='b' * 32, activity='running', updated_at_utc=datetime.now(UTC).isoformat())
    _save(tmp_path, old)
    lifecycle.snapshot_written()
    if reason == 'dead':
        monkeypatch.setattr('stockagent.data.news_worker_lifecycle.process_owner_alive', lambda *a, **k: False)
    else:
        lifecycle.finish(failed=reason == 'failed')
    result = build_news_public_status(tmp_path)
    assert result['health'] == 'stopped' and not result['worker']['fresh']
    assert result['acquisition']['stored_body_articles'] == 2
    assert result['acquisition']['eligible_eta_seconds'] is None


def test_first_same_owner_snapshot_enables_only_its_current_rates(tmp_path):
    old, lifecycle = seed(tmp_path)
    lifecycle.snapshot_written()
    assert build_news_public_status(tmp_path)['health'] == 'initializing'
    old.update(owner_id='b' * 32, activity='running', updated_at_utc=datetime.now(UTC).isoformat())
    _save(tmp_path, old)
    result = build_news_public_status(tmp_path)
    assert result['health'] == 'running' and result['worker']['fresh']
    assert result['worker']['lifecycle']['snapshot_owner_matches']
    assert result['acquisition']['eligible_eta_seconds'] is not None


def test_old_sidecar_does_not_override_a_later_different_snapshot_owner(tmp_path):
    old, lifecycle = seed(tmp_path)
    lifecycle.finish()
    old.update(owner_id='c' * 32, activity='running', updated_at_utc=(datetime.now(UTC) + timedelta(seconds=1)).isoformat())
    _save(tmp_path, old)
    result = build_news_public_status(tmp_path)
    assert not result['worker']['lifecycle']['supersedes_snapshot']
    assert result['health'] == 'running'


@pytest.mark.parametrize('case', ['oversized', 'not_json', 'version', 'boolean_version', 'owner', 'phase',
    'state_phase', 'naive', 'future', 'reversed_clock', 'process_reused', 'negative_count'])
def test_malformed_missing_and_stale_identity_cannot_grant_liveness(tmp_path, case):
    old, lifecycle = seed(tmp_path)
    record = dict(lifecycle.record)
    if case == 'oversized': record['unused'] = 'x' * 4096
    elif case == 'version': record['contract_version'] = 2
    elif case == 'boolean_version': record['contract_version'] = True
    elif case == 'owner': record['owner_id'] = 'PRIVATE_OWNER_TEXT'
    elif case == 'phase': record['phase'] = 'PRIVATE_PHASE_TEXT'
    elif case == 'state_phase': record['state'] = 'ready'
    elif case == 'naive': record['started_at_utc'] = '2026-10-01T01:00:00'
    elif case == 'future': record['updated_at_utc'] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    elif case == 'reversed_clock': record['started_at_utc'] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    elif case == 'process_reused': record['process_owner'] = {**record['process_owner'], 'start_ticks': '0'}
    elif case == 'negative_count': record['receipts_decoded'] = -1
    atomic_write_json(lifecycle.path, 'not_json' if case == 'not_json' else record)
    result = build_news_public_status(tmp_path)
    value = result['worker']['lifecycle']
    if case == 'process_reused':
        assert value['available'] and not value['process_alive'] and result['health'] == 'stopped'
    elif case == 'negative_count':
        assert value['receipts_decoded'] is None
    else:
        assert not value['available'] and result['health'] == 'stopped'


def test_no_prior_snapshot_initializes_with_unknown_not_zero_counts(tmp_path):
    lifecycle = NewsWorkerLifecycle(tmp_path / 'data_news_fulltext')
    lifecycle.started('b' * 32)
    result = build_news_public_status(tmp_path)
    assert result['health'] == 'initializing'
    assert result['acquisition']['known_urls'] is result['acquisition']['stored_body_articles'] is None


def test_observer_publishes_only_after_exclusive_lock_and_never_replaces_real_owner(config, tmp_path, monkeypatch):
    registry = PolicyRegistry(config, base=tmp_path)
    lifecycle = NewsWorkerLifecycle(tmp_path)
    observed = []
    original = NewsQueue.recover_acceptance

    def recovery(queue, *args, **kwargs):
        value = read_news_worker_lifecycle(tmp_path, snapshot_owner_id=None, snapshot_at=None, now=datetime.now(UTC))
        observed.append(value)
        assert value['process_alive'] and value['phase'] == 'acceptance_recovery'
        return original(queue, *args, **kwargs)

    monkeypatch.setattr(NewsQueue, 'recover_acceptance', recovery)
    with closing(NewsQueue(tmp_path, registry, lifecycle=lifecycle)) as queue:
        before = lifecycle.path.read_bytes()
        rejected = NewsWorkerLifecycle(tmp_path)
        with pytest.raises(RuntimeError, match='owner_already_running'):
            NewsQueue(tmp_path, registry, lifecycle=rejected)
        rejected.finish(failed=True)
        assert rejected.record is None and lifecycle.path.read_bytes() == before
        assert lifecycle.record['receipts_decoded'] == queue.acceptance_receipt_io['receipts_decoded']
        queue.status(activity='running')
        assert json.loads(lifecycle.path.read_bytes())['state'] == 'ready'
    lifecycle.finish()
    assert observed and json.loads(lifecycle.path.read_bytes())['state'] == 'closed'


def test_observation_write_error_does_not_trigger_body_retry_or_abort_owner(config, tmp_path, monkeypatch):
    def failed(*args, **kwargs):
        raise OSError('telemetry-only failure')
    lifecycle = NewsWorkerLifecycle(tmp_path)
    monkeypatch.setattr('downloader.artifact_io.atomic_write_json', failed)
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path), lifecycle=lifecycle)) as queue:
        assert lifecycle.write_errors > 0
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    lifecycle.finish()


def test_cli_failure_marks_failed_and_releases_canonical_lock(config, tmp_path, monkeypatch, capsys):
    from scripts import download_news_fulltext as cli
    from test_news_fulltext import http_fixture
    path, root = tmp_path / 'config.json', tmp_path / 'queue'
    atomic_write_json(path, config)
    http, session = http_fixture(config, tmp_path, {})
    monkeypatch.setattr(cli, 'NewsHTTP', lambda *args: http)

    def fail(*args, **kwargs):
        raise RuntimeError('deliberate_service_failure')

    monkeypatch.setattr(cli, 'run_service', fail)
    with pytest.raises(RuntimeError, match='deliberate_service_failure'):
        cli.main(['run', '--continuous', '--config', str(path), '--root', str(root), '--skip-local-ingest'])
    saved = json.loads((root / 'owner_lifecycle.json').read_bytes())
    assert saved['state'] == saved['phase'] == 'failed'
    assert not session.calls
    with closing(NewsQueue(root, PolicyRegistry(config, base=tmp_path))) as queue:
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
