"""Offline rendered contract checks; no publisher or live queue requests."""
from datetime import UTC, datetime
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from playwright.sync_api import expect, sync_playwright

from scripts.probe_browser_runtime import browser_profile_launch_options
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import _seed, _save
from test_news_dashboard_cnyes_list_progress import history_snapshot, index_snapshot


@pytest.mark.parametrize('width,height', [(1440, 1000), (390, 844)])
def test_real_page_keeps_all_known_body_ratio_separate_from_finished_cohort(tmp_path, width, height):
    now = datetime.now(UTC)
    status = _seed(tmp_path, now=now)
    source = status['providers']['cnyes']
    source.update(states={'complete': 2, 'waiting_cohort': 4}, acquisition_mode='finite',
                  finite_cohort={'urls': 2, 'stored_bodies': 2}, stored_body_urls=3)
    history = history_snapshot()
    history['current_source_contract_scope_states'].update(
        inconsistent_source_snapshot=1, waiting_original_page_recheck=2,
        waiting_child_evidence=4, source_page_cap_exceeded=5)
    source.update(public_index_progress=index_snapshot(), history_list_progress=history)
    status.update(states={'complete': 2, 'waiting_cohort': 4, 'awaiting_authorization': 4},
                  provider_stored_body_counter_contract_version=1, articles_with_stored_body=3)
    status['providers']['unregistered:example.org']['stored_body_urls'] = 0
    _save(tmp_path, status)
    payload = build_news_public_status(tmp_path, now=now)
    assert payload['snapshot_consistent']
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
                    path = parts.path.removeprefix('/')
                    prefix, _, suffix = path.partition('/')
                    relative = (mounts[prefix] + '/' + (suffix or 'index.html')
                                if prefix in mounts else 'public_dashboards/' + path)
                    target = (services / relative).resolve()
                    if not target.is_relative_to(services) or not target.is_file():
                        request.fulfill(status=404, body='')
                    else:
                        mime = {'.js': 'text/javascript', '.css': 'text/css', '.html': 'text/html', '.svg': 'image/svg+xml'}.get(target.suffix, 'application/octet-stream')
                        request.fulfill(content_type=mime, body=target.read_bytes())
            page.route('**/*', route)
            page.goto('http://news-preview.test/news/', wait_until='networkidle', timeout=15000)
            expect(page.locator('#body-count')).to_have_text('3')
            page.locator('a[href="#acq-catalog"]').click(timeout=10000)
            card = page.locator('#pipeline-grid .pipeline-card').filter(has=page.locator('.news-provider-identity small').filter(has_text='cnyes'))
            expect(card).to_have_count(1)
            expect(card.locator('.pipeline-status')).to_have_text('新文章等待納入批次')
            expect(card.locator('.news-provider-facts').first.locator('dd').first).to_have_text('3 / 6')
            expect(card.locator('.mini-progress-track')).to_have_attribute('value', '0.5')
            expect(card).to_contain_text('原固定批次 2 / 2')
            card.locator('[data-news-list-provider="cnyes"] > summary').click()
            expect(card.locator('[data-news-history-count="verified"]')).to_have_text('4,007')
            expect(card.locator('[data-news-history-count="waiting"]')).to_have_text('9')
            expect(card.locator('[data-news-history-count="inconsistent"]')).to_have_text('1')
            expect(card.locator('[data-news-history-count="page_cap"]')).to_have_text('5')
            card.locator('[data-news-outcome-provider="cnyes"] > summary').click()
            expect(card.locator('[data-news-outcome="waiting_cohort"]')).to_have_text('4')
            expect(card).to_contain_text('原站不存在不代表所有可用替代都已找過')
            page.locator('[data-news-filter="not_enabled"]').click()
            expect(page.locator('#pipeline-grid .pipeline-card')).to_have_count(1)
            page.locator('[data-news-filter="all"]').click()
            expect(page.locator('#pipeline-grid .pipeline-card')).to_have_count(2)
            page.locator('a[href="#acq-progress"]').click()
            page.locator('#outcome-panel > summary').click()
            expect(page.locator('[data-news-outcome-total="registration_required"]')).to_have_text('4')
            expect(page.locator('[data-news-outcome-total="complete_state"]')).to_have_text('2')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            assert not errors and not forbidden
            page.close()
        finally:
            browser.close()
