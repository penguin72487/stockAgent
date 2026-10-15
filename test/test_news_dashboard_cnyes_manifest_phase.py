"""Typed phase bars and bounded ETA on the actual existing reader."""
from datetime import UTC, datetime, timedelta
import json
import threading
from urllib.parse import urlsplit

import pytest

from test_news_dashboard import NOW, _server
from test_news_dashboard_cnyes_list_progress import setup, save, cnyes, browser


def fixture(root, *, now=NOW):
    status = setup(root)
    owner = 'a' * 32
    status['owner_id'] = owner
    status['updated_at_utc'] = now.isoformat()
    status['performance'].update(owner_id=owner, snapshot_at_utc=now.isoformat(),
        snapshot_clock_contract_version=1, owner_elapsed_seconds=600)
    bins, remaining = [1, 0, 3, 5, 11], 660
    rate = sum(bins) / 300
    phase = {'contract_version': 1, 'scope': 'actual_category_overflow_source_dates',
        'scope_fingerprint': 'b' * 64, 'planned_source_dates': 11, 'first_pages_observed': 11,
        'first_pages_remaining': 0, 'totals_known': True, 'source_reported_total_pages': 1000,
        'source_reported_total_metadata_rows': 29993, 'verified_pages': 340,
        'observed_metadata_rows': 10200, 'remaining_pages': remaining, 'progress_ratio': .34,
        'dates_with_changed_source_total': 0, 'phase_complete': False, 'metadata_only': True,
        'body_completion_claimed': False, 'all_history_complete': False,
        'forecast': {'contract_version': 1, 'state': 'recent_measured', 'sample_seconds': 300,
            'sample_count': 5, 'verified_pages': sum(bins), 'window_page_counts': bins,
            'pages_per_second': rate, 'fastest_seconds': remaining * 60 / max(bins),
            'middle_seconds': remaining / rate, 'slowest_seconds': None,
            'middle_finish_at_utc': (now + timedelta(seconds=remaining / rate)).isoformat(),
            'observed_at_utc': now.isoformat(), 'scenario_not_confidence_interval': True,
            'metadata_only': True, 'all_history_complete': False,
            'private_path': '/PRIVATE_PATH', 'raw_response_body': 'PRIVATE_BODY'},
        'url': 'https://private.test/?key=PRIVATE_TOKEN'}
    status['providers']['cnyes']['public_index_progress']['manifest_phase'] = phase
    return status, phase


def test_actual_ratio_and_all_wall_time_rate_project_without_private_fields(tmp_path):
    status, phase = fixture(tmp_path)
    result = save(tmp_path, status)
    public = cnyes(result)['manifest_phase_progress']
    assert result['cnyes_manifest_phase_projection_contract_version'] == 1
    assert public['progress_ratio'] == 340 / 1000 and public['remaining_pages'] == 660
    assert public['forecast']['pages_per_second'] == 20 / 300
    assert public['forecast']['fastest_seconds'] == 3600
    assert public['forecast']['middle_seconds'] == 9900
    assert public['forecast']['slowest_seconds'] is None
    assert public['forecast']['state'] == 'recent_measured'
    assert result['acquisition']['all_sources_eta_seconds'] is None
    assert cnyes(result)['public_index_progress']['progress_ratio'] is None
    assert 'PRIVATE_' not in json.dumps(result)


@pytest.mark.parametrize('patch', [
    {'planned_source_dates': True}, {'first_pages_remaining': 1}, {'verified_pages': 346},
    {'observed_metadata_rows': 10300}, {'source_reported_total_pages': 330},
    {'source_reported_total_metadata_rows': 30001}, {'remaining_pages': 0}, {'progress_ratio': .99},
    {'phase_complete': True}, {'all_history_complete': True}, {'body_completion_claimed': True},
    {'metadata_only': False}, {'scope': 'all_history'}, {'scope_fingerprint': 'PRIVATE_TOKEN'},
])
def test_inconsistent_phase_is_unknown_but_existing_body_and_list_counts_survive(tmp_path, patch):
    status, phase = fixture(tmp_path)
    phase.update(patch)
    source = cnyes(save(tmp_path, status))
    assert source['manifest_phase_progress'] is None
    assert source['public_index_progress']['verified_pages'] == 345
    assert source['known_urls'] == 6 and source['complete_urls'] == 2


@pytest.mark.parametrize('patch', [
    {'pages_per_second': .2}, {'verified_pages': 19}, {'sample_seconds': 299}, {'sample_count': 4},
    {'window_page_counts': [1, True, 3, 5, 11]}, {'fastest_seconds': 1}, {'middle_seconds': 1},
    {'slowest_seconds': 10000}, {'middle_finish_at_utc': NOW.isoformat()},
    {'observed_at_utc': (NOW - timedelta(seconds=1)).isoformat()}, {'all_history_complete': True},
])
def test_bad_rate_does_not_invent_a_finish_or_hide_the_verified_phase(tmp_path, patch):
    status, phase = fixture(tmp_path)
    phase['forecast'].update(patch)
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['progress_ratio'] == .34
    assert result['forecast']['state'] == 'unavailable'
    assert result['forecast']['middle_seconds'] is result['forecast']['middle_finish_at_utc'] is None


@pytest.mark.parametrize('activity', ['stopped', 'draining', 'waiting_disk'])
def test_nonlive_snapshots_have_counts_not_an_active_forecast(tmp_path, activity):
    status, _ = fixture(tmp_path)
    status['activity'] = activity
    public = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert public['verified_pages'] == 340 and public['forecast']['state'] == 'inactive'
    assert public['forecast']['middle_seconds'] is None


def test_previous_owner_and_stale_snapshots_do_not_supply_rates(tmp_path):
    status, _ = fixture(tmp_path)
    status['owner_id'] = 'c' * 32
    assert cnyes(save(tmp_path, status))['manifest_phase_progress']['forecast']['state'] == 'unavailable'
    status['updated_at_utc'] = (NOW - timedelta(seconds=181)).isoformat()
    assert cnyes(save(tmp_path, status))['manifest_phase_progress']['forecast']['state'] == 'inactive'


@pytest.mark.parametrize('width', [1440, 768, 390, 320])
def test_actual_reader_phase_bars_scenarios_and_native_controls(tmp_path, browser, width):
    from playwright.sync_api import expect
    from scripts.verify_news_dashboard import BODY_PROGRESS_SELECTOR, required_provider_card
    status, _ = fixture(tmp_path, now=datetime.now(UTC))
    save(tmp_path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': width, 'height': 900})
    errors, remote = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: remote.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('cnyes')
        detail = page.locator('[data-news-list-provider="cnyes"]')
        detail.locator('summary').click(timeout=10000)
        expect(detail.locator('[data-news-manifest-progress]')).to_have_count(2)
        expect(detail.locator('[data-news-manifest-progress="pages"]')).to_have_attribute('value', '0.34')
        expect(detail.locator('[data-news-manifest-progress="first_pages"]')).to_have_attribute('value', '1')
        # The shared body inspector must not read the newly added first-page
        # or metadata-page bars, including while the details are collapsed.
        body_progress = required_provider_card(page, 'cnyes').locator(BODY_PROGRESS_SELECTOR)
        expect(body_progress).to_have_count(1)
        expected_body_ratio = page.evaluate("() => state.latest.providers.find(row => row.id === 'cnyes').progress_ratio")
        assert body_progress.evaluate('(node) => node.value') == expected_body_ratio == 2 / 6
        expect(detail).to_contain_text('340 / 1,000 頁（34%）')
        expect(detail).to_contain_text('不是全庫或唯一新聞篇數')
        expect(detail.locator('[data-news-manifest-eta]')).to_contain_text('近 5 分鐘實際保存 20 頁')
        expect(detail.locator('[data-news-manifest-eta]')).to_contain_text('無有限較慢估時')
        expect(detail.locator('[data-news-list-eta]')).to_contain_text('完成時間未知')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert all(page.locator('[data-news-manifest-progress]').nth(i).bounding_box()['width'] > 0 for i in range(2))
        page.locator('#source-search').fill('absent-source')
        page.locator('#source-search').fill('cnyes')
        expect(detail).to_have_attribute('open', '')
        detail.locator('summary').focus()
        page.keyboard.press('Enter')
        expect(detail).not_to_have_attribute('open', '')
        assert body_progress.evaluate('(node) => node.value') == expected_body_ratio
        assert not errors and not remote
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_browser_status_loss_withdraws_phase_forecast_and_native_retry_restores_it(tmp_path, browser):
    """Exercise the actual timed refresh, not synthetic payload/DOM edits."""
    from playwright.sync_api import expect
    from downloader.artifact_io import sha256_file
    status, _ = fixture(tmp_path, now=datetime.now(UTC))
    save(tmp_path, status)
    original_status_sha = sha256_file(tmp_path / 'data_news_fulltext/status.json')
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 1440, 'height': 900})
    errors, remote = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: remote.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('cnyes')
        detail = page.locator('[data-news-list-provider="cnyes"]')
        detail.locator('summary').click(timeout=10000)
        phase = detail.locator('[data-news-manifest-state]')
        expect(phase).to_have_attribute('data-news-manifest-state', 'recent_measured')
        page.route('**/api/status', lambda route: route.abort())
        page.wait_for_function("() => state.latest?.health === 'stale' && !state.inFlight", timeout=30000)
        expect(phase).to_have_attribute('data-news-manifest-state', 'snapshot_stale')
        expect(phase.locator('[data-news-manifest-eta]')).to_contain_text('畫面更新暫停')
        expect(phase.locator('[data-news-manifest-eta]')).not_to_contain_text('基準預計')
        expect(phase.locator('[data-news-manifest-progress="pages"]')).to_have_attribute('value', '0.34')
        expect(phase.locator('[data-news-manifest-progress="first_pages"]')).to_have_attribute('value', '1')
        expect(detail).to_have_attribute('open', '')
        assert page.locator('#source-search').input_value() == 'cnyes'
        page.screenshot(path=str(tmp_path / 'status-loss.png'), full_page=True)
        page.unroute('**/api/status')
        page.wait_for_function("() => state.latest?.health !== 'stale' && !state.inFlight", timeout=30000)
        expect(phase).to_have_attribute('data-news-manifest-state', 'recent_measured')
        expect(phase.locator('[data-news-manifest-eta]')).to_contain_text('基準預計')
        expect(detail).to_have_attribute('open', '')
        assert page.locator('#source-search').input_value() == 'cnyes'
        assert sha256_file(tmp_path / 'data_news_fulltext/status.json') == original_status_sha
        assert not errors and not remote
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
