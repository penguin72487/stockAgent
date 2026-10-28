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


@pytest.mark.parametrize('reason', ['dead', 'closed', 'failed'])
def test_proven_stopped_owner_overrides_expired_snapshot_for_enabled_sources(tmp_path, monkeypatch, reason):
    old, lifecycle = seed(tmp_path)
    old.update(owner_id='b' * 32, activity='running', updated_at_utc=datetime.now(UTC).isoformat())
    _save(tmp_path, old)
    lifecycle.snapshot_written()
    if reason == 'dead':
        monkeypatch.setattr('stockagent.data.news_worker_lifecycle.process_owner_alive', lambda *a, **k: False)
    else:
        lifecycle.finish(failed=reason == 'failed')
    result = build_news_public_status(tmp_path, now=datetime.now(UTC) + timedelta(seconds=181))
    cnyes = next(row for row in result['providers'] if row['id'] == 'cnyes')
    unregistered = next(row for row in result['providers'] if row['id'] == 'unregistered:example.org')
    assert result['health'] == result['worker']['activity'] == cnyes['state'] == 'stopped'
    assert result['provider_state_owner_lifecycle_contract_version'] == 1
    assert not result['worker']['fresh']
    assert cnyes['conditional_eta_seconds'] is None
    assert result['acquisition']['stored_body_articles'] == 2
    assert unregistered['state'] == 'not_enabled'


def test_proven_stopped_reload_wait_does_not_turn_into_stale_or_running(tmp_path):
    old, lifecycle = seed(tmp_path)
    old.update(owner_id='b' * 32, activity='configuration_reload_requested',
               updated_at_utc=datetime.now(UTC).isoformat())
    _save(tmp_path, old)
    lifecycle.snapshot_written()
    lifecycle.finish()
    result = build_news_public_status(tmp_path, now=datetime.now(UTC) + timedelta(seconds=181))
    assert result['health'] == 'waiting_restart'
    assert next(row for row in result['providers'] if row['id'] == 'cnyes')['state'] == 'waiting_restart'
    assert result['acquisition']['eligible_eta_seconds'] is None


def test_stale_snapshot_without_matching_owner_evidence_stays_stale(tmp_path):
    status = _seed(tmp_path, now=datetime.now(UTC) - timedelta(seconds=181))
    status['activity'] = 'stopped'
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path)
    assert not result['worker']['lifecycle']['available']
    assert result['health'] == 'stale'
    assert next(row for row in result['providers'] if row['id'] == 'cnyes')['state'] == 'stale'


@pytest.mark.parametrize('mode,width', [('stopped', 1280), ('stopped', 390), ('stale', 1280), ('running', 1280)])
def test_metadata_feed_card_distinguishes_stopped_stale_and_running(tmp_path, browser, mode, width):
    from playwright.sync_api import expect
    from scripts.verify_news_dashboard import required_provider_card
    from test_news_dashboard import _metadata_feed_snapshot, _server

    now = datetime.now(UTC)
    status = _metadata_feed_snapshot(tmp_path)
    status.update(owner_id='b' * 32, activity='running',
                  updated_at_utc=(now - timedelta(seconds=181) if mode != 'running' else now).isoformat())
    status['providers']['ltn']['metadata_feed_progress'].update(
        last_check_at_utc=(now - timedelta(minutes=5)).isoformat(),
        next_check_at_utc=(now - timedelta(seconds=1)).isoformat())
    if mode == 'stopped':
        status['provider_next_attempt_utc']['ltn'] = (now + timedelta(minutes=30)).isoformat()
    observed = datetime.fromisoformat(status['updated_at_utc'])
    probe = observed + timedelta(seconds=30 if mode != 'running' else -1)
    status['providers']['cnyes']['body_work_progress'] = {
        'contract_version': 1, 'active_missing_body_urls': 4,
        'existing_primary_body_candidates': 3, 'no_known_primary_body_urls': 1,
        'candidate_views_accepted_by_hint': False, 'candidate_body_equivalence_verified': False,
        'request_scope_progress': {'contract_version': 1, 'available': True,
            'active_missing_body_urls': 4, 'source_paused_missing_body_urls': 2,
            'other_missing_body_urls': 2, 'policy_paused_missing_body_urls': 2,
            'gateway_paused_missing_body_urls': 0, 'source_recovery_verified': False,
            'next_source_probe_at_utc': probe.isoformat(),
            'pending_source_rechecks': {'contract_version': 1, 'available': True,
                'unverified_missing_body_urls': 2, 'due_missing_body_urls': 2 if mode == 'running' else 0,
                'next_probe_at_utc': probe.isoformat()}}}
    _save(tmp_path, status)
    lifecycle = None
    if mode != 'stale':
        lifecycle = NewsWorkerLifecycle(tmp_path / 'data_news_fulltext')
        lifecycle.started('b' * 32)
        lifecycle.snapshot_written()
        if mode == 'stopped':
            lifecycle.finish()
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
        # Poll the rendered state rather than evaluating a string inside the
        # page: its CSP correctly disallows unsafe-eval, and Playwright's
        # wait_for_function can otherwise race with CSP enforcement.
        health_label = {'stopped': '下載器已停止', 'stale': '觀測已過期', 'running': '正在抓取'}[mode]
        expect(page.locator('#news-health')).to_contain_text(health_label, timeout=10000)
        assert not errors, errors
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('ltn')
        card = required_provider_card(page, 'ltn')
        feed = card.locator('[data-news-metadata-feed]')
        expect(feed).to_have_count(1)
        if mode == 'stopped':
            expect(card).to_contain_text('下載器已停止')
            expect(feed).to_contain_text('下載器顯示已停止，恢復服務後才會繼續')
            expect(feed).to_contain_text('上次記錄排程 已到期，不表示已經執行')
            expect(card).to_contain_text('上次記錄來源冷卻最早截止')
            expect(card).to_contain_text('不表示到時已執行')
            # A still-running older API may expose stale source states despite
            # already reporting the verified stopped owner globally.
            page.evaluate("state.latest.providers.filter(s => s.enabled).forEach(s => s.state = 'stale'); renderSources(state.latest)")
            expect(card).to_contain_text('下載器已停止')
        elif mode == 'stale':
            expect(card).to_contain_text('觀測已過期')
            expect(feed).to_contain_text('目前階段無有效追新執行觀測')
            assert '下載器顯示已停止' not in feed.inner_text()
        else:
            expect(card).to_contain_text('RSS 資訊追新，非全文')
            expect(feed).to_contain_text('下一次 已到期，等待排程')
        if mode != 'running':
            assert '下一次 已到期，等待排程' not in feed.inner_text()
        page.locator('#source-search').fill('cnyes')
        scope = required_provider_card(page, 'cnyes').locator('[data-news-request-scope]')
        expect(scope).to_have_count(1)
        if mode == 'running':
            expect(scope).to_contain_text('已到期，等待排程')
        else:
            expect(scope).to_contain_text('上次記錄已到期，實際執行情況待確認')
            expect(scope).to_contain_text('目前是否執行待確認')
            assert '已到期，等待排程' not in scope.inner_text()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors and not remote
    finally:
        page.close()
        server.shutdown(); server.server_close(); thread.join(timeout=2)
        if lifecycle is not None and mode == 'running':
            lifecycle.finish()


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
