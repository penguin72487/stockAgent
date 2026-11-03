"""Fixture-only source-card identity guard, not a live dashboard acceptance."""
from playwright.sync_api import expect, sync_playwright
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sys
import threading
from urllib.parse import urlsplit

import pytest

from scripts.probe_browser_runtime import browser_profile_launch_options
from scripts.verify_news_dashboard import required_provider_card


@pytest.mark.parametrize('width', [320, 1440])
@pytest.mark.parametrize('source,version,label', [('finmind', 5, 'FinMind'), ('finlab', 6, 'FinLab')])
def test_finmind_catalog_error_renders_as_stopped_snapshot(tmp_path, width, source, version, label):
    from downloader.artifact_io import atomic_write_json
    from test_news_dashboard import _seed, _server

    status = _seed(tmp_path, now=datetime.now(UTC))
    status['activity'] = 'stopped'
    status['metadata_partition_errors'] = 1
    status['metadata_inventory'] = {'contract_version': version, source + '_catalog_errors': 1,
                                    'private_path': '/PRIVATE_FINMIND_PATH'}
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    try:
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
            try:
                page = browser.new_page(viewport={'width': width, 'height': 900})
                errors, external = [], []
                page.on('pageerror', lambda exc: errors.append(str(exc)))
                page.on('request', lambda req: external.append(req.url) if urlsplit(req.url).netloc != origin else None)
                page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
                page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
                expect(page.locator('#error-count')).to_contain_text(f'{label} 新聞清單讀取異常（上次快照）')
                assert '/PRIVATE_FINMIND_PATH' not in page.content()
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                assert not errors and not external
                page.close()
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize('width', [320, 1440])
def test_pending_metadata_alias_renders_unknown_counts(tmp_path, width):
    from downloader.artifact_io import atomic_write_json
    from test_news_dashboard import _seed, _server

    status = _seed(tmp_path, now=datetime.now(UTC))
    status['activity'] = 'stopped'
    status['metadata_partitions'] = 100
    status['metadata_association_rows'] = 200
    status['metadata_inventory'] = {'contract_version': 7, 'receipt_alias_retargets_pending': 2}
    atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    try:
        with sync_playwright() as runtime:
            browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
            try:
                page = browser.new_page(viewport={'width': width, 'height': 900})
                errors, external = [], []
                page.on('pageerror', lambda exc: errors.append(str(exc)))
                page.on('request', lambda req: external.append(req.url) if urlsplit(req.url).netloc != origin else None)
                page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
                page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
                expect(page.locator('#metadata-count')).to_contain_text('來源選取待核對 2 個，分區數未知')
                expect(page.locator('#association-count')).to_contain_text('— 筆')
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                assert not errors and not external
                page.close()
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize('base_url,expected', [
    (None, 'http://127.0.0.1:8771/news/'),
    ('http://127.0.0.1:8871/news', 'http://127.0.0.1:8871/news/'),
])
def test_cli_uses_owner_listener_by_default_and_preserves_explicit_target(monkeypatch, tmp_path, base_url, expected):
    from scripts import verify_news_dashboard as verifier

    calls = []

    def fixture_verify(url, output, **kwargs):
        calls.append((url, output))
        return {'state': 'accepted', 'viewports': []}

    argv = ['verify_news_dashboard', '--output-dir', str(tmp_path)]
    if base_url is not None:
        argv += ['--base-url', base_url]
    monkeypatch.setattr(sys, 'argv', argv)
    monkeypatch.setattr(verifier, 'verify', fixture_verify)
    assert verifier.main() == 0
    assert calls == [(expected, tmp_path)]
    assert json.loads((tmp_path / 'acceptance.json').read_text())['state'] == 'accepted'


def test_owner_transport_still_rejects_public_audience_and_public_caching():
    from scripts.verify_news_dashboard import require_owner_transport

    require_owner_transport({'x-stockagent-audience': 'owner', 'cache-control': 'private, no-store'})
    for headers in ({'x-stockagent-audience': 'public', 'cache-control': 'private, no-store'},
                    {'x-stockagent-audience': 'owner', 'cache-control': 'public, max-age=60'}):
        with pytest.raises(AssertionError):
            require_owner_transport(headers)


def scope_card():
    return {'body_request_scope_progress': {'contract_version': 1, 'available': True,
        'active_missing_body_urls': 3, 'source_paused_missing_body_urls': 2,
        'other_missing_body_urls': 1, 'source_recovery_verified': False},
        'body_work_progress': {'active_missing_body_urls': 3},
        'displayed_request_scope_counts': [2, 1], 'request_scope_note_present': True,
        'conditional_eta_seconds': None, 'conditional_finish_at_utc': None,
        'conditional_eta_unavailable_reason': 'source_request_scope_paused'}


def test_request_scope_acceptance_checks_rendered_counts_and_withdrawn_countdown():
    from scripts.verify_news_dashboard import request_scope_progress_check

    assert request_scope_progress_check(scope_card())['accepted']
    assert request_scope_progress_check({}) is None
    assert request_scope_progress_check({'body_request_scope_progress': {'available': False}}) is None


def test_request_scope_acceptance_checks_expired_but_unverified_source():
    from scripts.verify_news_dashboard import request_scope_progress_check

    checked = scope_card()
    scope = checked['body_request_scope_progress']
    scope.update(source_paused_missing_body_urls=0, other_missing_body_urls=3,
                 pending_source_rechecks={'contract_version': 1, 'available': True,
                                          'unverified_missing_body_urls': 2, 'due_missing_body_urls': 2})
    checked.update(displayed_request_scope_counts=[0, 3], displayed_pending_source_rechecks=2,
                   pending_source_recheck_note_present=True)
    assert request_scope_progress_check(checked)['unverified_missing_body_urls'] == 2
    for field, value in [('displayed_pending_source_rechecks', 0),
                         ('pending_source_recheck_note_present', False),
                         ('conditional_eta_seconds', 30)]:
        changed = {**checked, field: value}
        with pytest.raises(AssertionError):
            request_scope_progress_check(changed)


def test_request_scope_acceptance_requires_unknown_legacy_evidence_to_suppress_eta():
    from scripts.verify_news_dashboard import request_scope_progress_check

    checked = scope_card()
    checked['body_request_scope_progress'].update(source_paused_missing_body_urls=0, other_missing_body_urls=3)
    checked.update(displayed_request_scope_counts=[0, 3],
                   conditional_eta_unavailable_reason='source_request_scope_unknown')
    assert request_scope_progress_check(checked)['unverified_missing_body_urls'] is None
    checked['conditional_eta_unavailable_reason'] = None
    with pytest.raises(AssertionError):
        request_scope_progress_check(checked)


@pytest.mark.parametrize('field,value', [
    ('displayed_request_scope_counts', [3, 0]), ('request_scope_note_present', False),
    ('conditional_eta_seconds', 30), ('conditional_finish_at_utc', '2026-10-24T00:00:00+00:00'),
    ('conditional_eta_unavailable_reason', None),
])
def test_request_scope_acceptance_rejects_stale_or_inconsistent_card(field, value):
    from scripts.verify_news_dashboard import request_scope_progress_check

    checked = scope_card(); checked[field] = value
    with pytest.raises(AssertionError):
        request_scope_progress_check(checked)


def test_required_source_card_is_exact_when_provider_names_or_prefixes_collide():
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            page = browser.new_page()
            page.set_content('''<section id="pipeline-grid">
                <article class="pipeline-card"><div class="news-provider-identity"><strong>CMoney</strong><small>cmoney</small></div></article>
                <article class="pipeline-card"><div class="news-provider-identity"><strong>CMoney 新聞</strong><small>cmnews</small></div></article>
                <article class="pipeline-card"><div class="news-provider-identity"><strong>同名</strong><small>cmoney.api</small></div></article>
                <article class="pipeline-card"><div class="news-provider-identity"><strong>同名</strong><small>cmoney.api2</small></div></article>
                </section>''')
            for provider in ('cmoney', 'cmnews', 'cmoney.api', 'cmoney.api2'):
                card = required_provider_card(page, provider)
                expect(card).to_have_count(1)
                expect(card.locator('.news-provider-identity small')).to_have_text(provider)
            expect(required_provider_card(page, 'missing')).to_have_count(0)
            page.close()
        finally:
            browser.close()


@pytest.mark.parametrize('width,height', [(1440, 1000), (390, 844)])
@pytest.mark.parametrize('scope_case', ['precise_recheck', 'unscoped_gate'])
def test_unverified_source_is_visible_without_a_false_eta(tmp_path, width, height, scope_case):
    from stockagent.live.news_dashboard import build_news_public_status
    from test_news_dashboard import NOW, _origin_pause_status, _request_scope_status, _save

    if scope_case == 'precise_recheck':
        status = _request_scope_status(tmp_path, paused=0)
        status['providers']['cnyes']['body_work_progress']['request_scope_progress']['pending_source_rechecks'] = {
            'contract_version': 1, 'available': True, 'unverified_missing_body_urls': 3,
            'due_missing_body_urls': 3, 'next_probe_at_utc': (NOW - timedelta(seconds=20)).isoformat()}
    else:
        status = _origin_pause_status(tmp_path, {'https://private-source.test': NOW.timestamp() + 30})
    _save(tmp_path, status)
    payload = build_news_public_status(tmp_path, now=NOW)
    payload['observed_at_utc'] = datetime.now(UTC).isoformat()
    services = Path(__file__).resolve().parents[1] / 'services'
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            page = browser.new_page(viewport={'width': width, 'height': height})
            errors, forbidden = [], []
            page.on('pageerror', lambda error: errors.append(str(error)))

            def route(request):
                parts = urlsplit(request.request.url)
                if parts.netloc != 'news-preview.test':
                    forbidden.append(parts.netloc)
                    request.abort()
                elif parts.path == '/news/api/status':
                    request.fulfill(content_type='application/json', body=json.dumps(payload))
                else:
                    mounts = {'news': 'news_dashboard', 'shioaji': 'shioaji_api_dashboard',
                              'finlab': 'finlab_dashboard', 'finmind': 'finmind_dashboard'}
                    prefix, _, suffix = parts.path.removeprefix('/').partition('/')
                    relative = (mounts[prefix] + '/' + (suffix or 'index.html') if prefix in mounts
                                else 'public_dashboards/' + parts.path.removeprefix('/'))
                    target = (services / relative).resolve()
                    if not target.is_relative_to(services) or not target.is_file():
                        request.fulfill(status=404, body='')
                    else:
                        mime = {'.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html',
                                '.svg': 'image/svg+xml'}.get(target.suffix, 'application/octet-stream')
                        request.fulfill(content_type=mime, body=target.read_bytes())

            page.route('**/*', route)
            page.goto('http://news-preview.test/news/', wait_until='networkidle', timeout=15000)
            page.locator('a[href="#acq-catalog"]').click(timeout=10000)
            card = required_provider_card(page, 'cnyes')
            if scope_case == 'precise_recheck':
                expect(card.locator('[data-news-scope-recheck-pending]')).to_have_text('3')
                expect(card).to_contain_text('最近核對時其中 3 個已到重新核對時間')
                expect(card).to_contain_text('等待來源重新核對，完成時間未知')
            else:
                expect(card).to_contain_text('缺文端點狀態尚未核對，完成時間未知')
                assert 'private-source.test' not in page.content()
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            assert not errors and not forbidden
            page.close()
        finally:
            browser.close()


@pytest.mark.parametrize('width,height', [(1280, 900), (320, 844)])
@pytest.mark.parametrize('causes_available', [True, False])
def test_stopped_local_retries_render_as_snapshot_not_article_completion(tmp_path, width, height, causes_available):
    from downloader.artifact_io import atomic_write_json
    from stockagent.live.news_dashboard import build_news_public_status
    from test_news_dashboard import _seed, _lookup_recheck_snapshot, local_replay_sample, metadata_resource_sample, metadata_index_sample, metadata_large_sample
    from test_news_dashboard_cnyes_list_progress import history_snapshot

    now = datetime.now(UTC)
    status = _seed(tmp_path, now=now)
    status['activity'] = 'stopped'
    status['providers']['cnyes']['native_lookup_recheck_progress'] = _lookup_recheck_snapshot()
    status['cnyes_local_replay_retries'] = local_replay_sample()
    status['cnyes_metadata_resource_waits'] = metadata_resource_sample()
    status['cnyes_metadata_large_replay'] = metadata_large_sample()
    if causes_available:
        status['cnyes_metadata_large_replay'].update(retry_causes_contract_version=1,
            retry_cause_counts={'progress_unknown_timeout': 1, 'counters_unchanged_timeout': 1, 'other': 0})
    status['cnyes_metadata_index_progress'] = metadata_index_sample()
    status['providers']['cnyes']['history_list_progress'] = history_snapshot()
    status['providers']['cnyes']['history_list_progress']['original_priority_admission'] = {
        'contract_version': 1, 'state': 'complete' if causes_available else 'scanning', 'problem': None,
        'targets': 854, 'processed_targets': 854 if causes_available else 100, 'stale_targets': 2,
        'enqueued_pages': 99, 'existing_pages': 1, 'publisher_requests': 0,
        'accepted_as_originals': False, 'all_history_complete': False, 'private_path': 'PRIVATE_PATH'}
    status['providers']['cnyes']['history_list_progress']['original_priority_audit'] = {
        'contract_version': 1, 'tracking_complete': causes_available, 'tracked_pages': 100 if causes_available else 99,
        'awaiting_pages': 20 if causes_available else 19, 'verified_pages': 40, 'mismatched_pages': 15,
        'unverified_pages': 10, 'outside_pages': 5, 'changed_pages': 10,
        'local_raw_only': True, 'publisher_requests': 0, 'all_history_complete': False,
        'private_path': 'PRIVATE_PATH'}
    status['providers']['cnyes']['history_list_progress']['original_envelope_audit'] = {
        'contract_version': 1, 'scan_state': 'complete' if causes_available else 'scanning',
        'next_audit_at_utc': '2026-10-30T12:00:00+00:00' if causes_available else None,
        'checked_pages': 40, 'verified_pages': 30,
        'out_of_scope_pages': 5, 'mismatched_pages': 3, 'unverified_pages': 2,
        'retry_pending_pages': 1 if causes_available else None, 'local_raw_only': True,
        'all_history_complete': False, 'publisher_requests': 0, 'private_path': 'PRIVATE_PATH'}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    payload = build_news_public_status(tmp_path, now=now)
    assert payload['health'] == 'stopped'
    assert payload['acquisition']['eligible_eta_seconds'] is None
    services = Path(__file__).resolve().parents[1] / 'services'
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            page = browser.new_page(viewport={'width': width, 'height': height})
            errors, forbidden = [], []
            page.on('pageerror', lambda error: errors.append(str(error)))

            def route(request):
                parts = urlsplit(request.request.url)
                if parts.netloc != 'news-preview.test':
                    forbidden.append(parts.netloc)
                    request.abort()
                elif parts.path == '/news/api/status':
                    request.fulfill(content_type='application/json', body=json.dumps(payload))
                else:
                    mounts = {'news': 'news_dashboard', 'shioaji': 'shioaji_api_dashboard',
                              'finlab': 'finlab_dashboard', 'finmind': 'finmind_dashboard'}
                    prefix, _, suffix = parts.path.removeprefix('/').partition('/')
                    relative = (mounts[prefix] + '/' + (suffix or 'index.html') if prefix in mounts
                                else 'public_dashboards/' + parts.path.removeprefix('/'))
                    target = (services / relative).resolve()
                    if not target.is_relative_to(services) or not target.is_file():
                        request.fulfill(status=404, body='')
                    else:
                        mime = {'.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html',
                                '.svg': 'image/svg+xml'}.get(target.suffix, 'application/octet-stream')
                        request.fulfill(content_type=mime, body=target.read_bytes())

            page.route('**/*', route)
            page.goto('http://news-preview.test/news/', wait_until='networkidle', timeout=15000)
            page.locator('a[href="#acq-catalog"]').click(timeout=10000)
            card = required_provider_card(page, 'cnyes')
            lookup = card.locator('[data-news-lookup-rechecks]')
            expect(lookup).to_contain_text('上次快照')
            expect(lookup).to_contain_text('3 筆')
            expect(lookup).to_contain_text('不是正文完成數')
            note = card.locator('[data-news-local-retries]')
            expect(note).to_contain_text('上次快照的本機原件重試紀錄')
            expect(note).to_contain_text('API 原件 3 頁')
            expect(note).to_contain_text('手機欄位原件 0 批')
            expect(note).to_contain_text('診斷原件未知')
            expect(note).to_contain_text('這些不是文章或正文完成數')
            resource = card.locator('[data-news-metadata-resource]')
            expect(resource).to_contain_text('上次快照的手機欄位容量觀測紀錄：2 批')
            expect(resource).to_contain_text('其中 1 批超過主迴圈單次讀取限制')
            expect(resource).to_contain_text('紀錄可能過期，不是文章、正文或完成數')
            large = card.locator('[data-news-metadata-large]')
            expect(large).to_contain_text('上次快照的大原件隔離讀取：已派發，等待回報')
            expect(large).to_contain_text('本機重試紀錄 2 筆')
            if causes_available:
                expect(large).to_contain_text('無法觀測逾時 1')
                expect(large).to_contain_text('計數器無變化逾時 1')
            else:
                expect(large).to_contain_text('重試原因分類未知')
            expect(large).to_contain_text('不是文章完成數或保證派發時間')
            metadata_index = card.locator('[data-news-metadata-index]')
            expect(metadata_index).to_contain_text('上次快照的手機原件增量清冊：基線核對中')
            expect(metadata_index).to_contain_text('累計掃描 123 列')
            expect(metadata_index).to_contain_text('不是正文或欄位補件完成數')
            original = card.locator('[data-news-original-envelope-audit]')
            priority = card.locator('[data-news-original-priority]')
            expect(priority).to_contain_text(f'上次快照：優先處理已知原件問題：{854 if causes_available else 100} / 854')
            expect(priority).to_contain_text('這是觀測掃描進度，不是已修復或下載完成')
            expect(priority.locator('progress')).to_have_attribute('max', '854')
            expect(priority.locator('progress')).to_have_attribute('value', '854' if causes_available else '100')
            priority_audit = card.locator('[data-news-original-priority-audit]')
            expect(priority_audit).to_contain_text(f'上次快照：優先清單本機核驗：追蹤 {100 if causes_available else 99} 個頁面版本')
            expect(priority_audit).to_contain_text('請求或授權不符 15')
            expect(priority_audit).to_contain_text('頁首已變更 10')
            expect(priority_audit).to_contain_text('已建立頁面 ID 證據 —')
            expect(priority_audit).to_contain_text('統計時不重讀原始檔或完整觀察')
            expect(priority_audit).to_contain_text('不能當作已修復')
            if causes_available:
                expect(priority_audit.locator('progress')).to_have_attribute('max', '100')
                expect(priority_audit.locator('progress')).to_have_attribute('value', '65')
            else:
                expect(priority_audit.locator('progress')).to_have_count(0)  # Admission is still in progress.
                expect(priority_audit).to_contain_text('追蹤紀錄不完整或不一致')
            expect(original).to_contain_text('上次快照的歷史清單原件身分盤點')
            expect(original).to_contain_text('已檢視 40 個頁面記錄')
            expect(original).to_contain_text('30 個原件核驗通過')
            expect(original).to_contain_text('3 個請求或授權不符')
            expect(original).to_contain_text('2 個中繼資料或本機原件當時無法驗證')
            expect(original).to_contain_text('5 個未由這輪原件身分核驗')
            if causes_available:
                expect(original).to_contain_text('目前已記錄 1 個頁面待本機重驗')
                expect(original).to_contain_text('上次快照預定下次本機巡檢')
                expect(original).to_contain_text('到期不代表已執行')
            else:
                expect(original).to_contain_text('目前待重驗筆數未知')
                expect(original).not_to_contain_text('下次本機巡檢')
            expect(card).to_contain_text('完成時點尚未知')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            assert not errors and not forbidden and 'PRIVATE_' not in (note.inner_text() + resource.inner_text()
                + large.inner_text() + metadata_index.inner_text() + original.inner_text() + priority.inner_text())
            if causes_available:
                status['providers']['cnyes']['history_list_progress']['original_priority_audit'].update(
                    contract_version=2, changed_pages=5, recovered_id_pages=5)
                atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
                payload = build_news_public_status(tmp_path, now=now)
                page.reload(wait_until='networkidle', timeout=15000)
                page.locator('a[href="#acq-catalog"]').click(timeout=10000)
                updated = required_provider_card(page, 'cnyes').locator('[data-news-original-priority-audit]')
                expect(updated).to_contain_text('已建立頁面 ID 證據 5')
                expect(updated).to_contain_text('頁首已變更 5')
                expect(updated.locator('progress')).to_have_attribute('value', '70')
                expect(updated).to_contain_text('不代表全文完成')
                assert page.evaluate('document.documentElement.scrollWidth') <= width
            status['providers']['cnyes']['history_list_progress']['original_priority_admission'].update(
                state='scanning', processed_targets=854)
            atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
            payload = build_news_public_status(tmp_path, now=now)
            page.reload(wait_until='networkidle', timeout=15000)
            page.locator('a[href="#acq-catalog"]').click(timeout=10000)
            changed = required_provider_card(page, 'cnyes')
            expect(changed.locator('[data-news-original-priority]')).to_contain_text('仍有後續頁面變更待安排')
            expect(changed.locator('[data-news-original-priority-audit] progress')).to_have_count(0)
            status['providers']['cnyes']['history_list_progress']['original_priority_admission'].update(
                state='blocked', problem='checkpoint_archive_conflict', targets=None, processed_targets=None,
                stale_targets=None, enqueued_pages=None, existing_pages=None)
            atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
            payload = build_news_public_status(tmp_path, now=now)
            page.reload(wait_until='networkidle', timeout=15000)
            page.locator('a[href="#acq-catalog"]').click(timeout=10000)
            blocked = required_provider_card(page, 'cnyes').locator('[data-news-original-priority]')
            expect(blocked).to_contain_text('本機游標封存證據衝突')
            expect(blocked.locator('progress')).to_have_count(0)
            cnyes = next(row for row in payload['providers'] if row['id'] == 'cnyes')
            cnyes['native_lookup_recheck_progress'].update(pending_records_lower_bound=2049, count_exact=False)
            page.evaluate('(data) => render(data)', payload)
            lookup = required_provider_card(page, 'cnyes').locator('[data-news-lookup-rechecks]')
            expect(lookup).to_contain_text('至少 2,049 筆')
            cnyes['native_lookup_recheck_progress'] = None
            page.evaluate('(data) => render(data)', payload)
            lookup = required_provider_card(page, 'cnyes').locator('[data-news-lookup-rechecks]')
            expect(lookup).to_contain_text('狀態未知')
            expect(lookup).not_to_contain_text('0 筆')
            assert not errors and not forbidden
            page.close()
        finally:
            browser.close()
