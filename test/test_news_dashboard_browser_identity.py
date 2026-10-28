"""Fixture-only source-card identity guard, not a live dashboard acceptance."""
from playwright.sync_api import expect, sync_playwright
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sys
from urllib.parse import urlsplit

import pytest

from scripts.probe_browser_runtime import browser_profile_launch_options
from scripts.verify_news_dashboard import required_provider_card


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
def test_expired_unverified_source_is_visible_without_a_false_eta(tmp_path, width, height):
    from stockagent.live.news_dashboard import build_news_public_status
    from test_news_dashboard import NOW, _request_scope_status, _save

    status = _request_scope_status(tmp_path, paused=0)
    status['providers']['cnyes']['body_work_progress']['request_scope_progress']['pending_source_rechecks'] = {
        'contract_version': 1, 'available': True, 'unverified_missing_body_urls': 3,
        'due_missing_body_urls': 3, 'next_probe_at_utc': (NOW - timedelta(seconds=20)).isoformat()}
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
            expect(card.locator('[data-news-scope-recheck-pending]')).to_have_text('3')
            expect(card).to_contain_text('最近核對時其中 3 個已到重新核對時間')
            expect(card).to_contain_text('等待來源重新核對，完成時間未知')
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
    from test_news_dashboard import _seed, local_replay_sample, metadata_resource_sample, metadata_index_sample, metadata_large_sample

    now = datetime.now(UTC)
    status = _seed(tmp_path, now=now)
    status['activity'] = 'stopped'
    status['cnyes_local_replay_retries'] = local_replay_sample()
    status['cnyes_metadata_resource_waits'] = metadata_resource_sample()
    status['cnyes_metadata_large_replay'] = metadata_large_sample()
    if causes_available:
        status['cnyes_metadata_large_replay'].update(retry_causes_contract_version=1,
            retry_cause_counts={'progress_unknown_timeout': 1, 'counters_unchanged_timeout': 1, 'other': 0})
    status['cnyes_metadata_index_progress'] = metadata_index_sample()
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
            expect(card).to_contain_text('完成時點尚未知')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            assert not errors and not forbidden and 'PRIVATE_' not in note.inner_text() + resource.inner_text() + large.inner_text() + metadata_index.inner_text()
            page.close()
        finally:
            browser.close()
