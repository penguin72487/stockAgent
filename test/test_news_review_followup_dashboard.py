"""Recorded follow-ups are visible without leaking or scheduling private work."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
import threading
from urllib.parse import urlsplit

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.live.news_dashboard import build_news_public_status
from test_news_dashboard import NOW, _seed, _server, candidate_work_sample
from test_news_dashboard_cnyes_list_progress import browser  # noqa: F401
from test_news_source_reviews import _configure, _ledger


@pytest.mark.parametrize(('scope', 'expected'), [
    ({'completed_questions': ['done'], 'remaining_questions': ['PRIVATE_QUESTION']}, 'open'),
    ({'completed_questions': ['done'], 'remaining_questions': []}, 'none_recorded'),
    ({'completed_questions': [], 'remaining_questions': []}, 'unknown'),
    ({'completed_questions': ['done']}, 'unknown'),
    ({'remaining_questions': ['PRIVATE_QUESTION']}, 'open'),
    ({'remaining_questions': [True]}, 'unknown'),
    ({'remaining_questions': ['same', 'same']}, 'unknown'),
    ({'completed_questions': ['same'], 'remaining_questions': ['same']}, 'unknown'),
    ({'remaining_questions': ['PRIVATE_QUESTION'], 'recorded_at_utc': 'bad'}, 'unknown'),
    ({'remaining_questions': ['PRIVATE_QUESTION'],
      'recorded_at_utc': (NOW + timedelta(seconds=6)).isoformat()}, 'unknown'),
])
def test_followup_projection_is_private_bounded_and_independent_of_capture(tmp_path, scope, expected):
    ledger = _ledger()
    review = ledger['providers']['moneydj']
    review['capture_decision'] = 'reviewed_existing_scope'
    review['current_scope_review'] = scope
    original = deepcopy(ledger)
    _configure(tmp_path, ledger=ledger)
    result = build_news_public_status(tmp_path, now=NOW)
    rows = {row['id']: row for row in result['providers']}
    alias = rows['unregistered:example.org']
    assert alias['source_review_followup'] == expected
    assert alias['source_policy_review'] == 'reviewed_existing_scope'
    assert not alias['enabled'] and alias['conditional_eta_seconds'] is None
    assert rows['cnyes']['source_review_followup'] == 'unknown'
    assert result['source_review_followup_contract_version'] == 1
    assert sum(result['source_review_followup_state_counts'].values()) == len(rows)
    assert result['acquisition']['stored_body_articles'] == 2
    assert not result['scope']['source_reviews_grant_capture_permission']
    assert not result['scope']['publisher_calls_on_page_view']
    assert 'PRIVATE_' not in json.dumps(result) and 'https://example.org/terms' not in json.dumps(result)
    assert ledger == original


def test_invalid_review_clock_and_broken_ledger_leave_followups_unknown(tmp_path):
    ledger = _ledger()
    ledger['providers']['moneydj'].update(reviewed_at_utc='not-a-clock',
        current_scope_review={'remaining_questions': ['PRIVATE_QUESTION']})
    _, path = _configure(tmp_path, ledger=ledger)
    result = build_news_public_status(tmp_path, now=NOW)
    alias = next(row for row in result['providers'] if row['id'] == 'unregistered:example.org')
    assert alias['source_review_followup'] == 'unknown'
    assert alias['source_policy_review'] == 'review_unverified'
    atomic_write_json(path, {'contract_version': True})
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['source_reviews_state'] == 'unavailable'
    assert result['source_review_followup_state_counts'] == {'unknown': 2}
    assert result['acquisition']['stored_body_articles'] == 2


def test_browser_filters_followups_and_recovers_when_ledger_becomes_unavailable(tmp_path, browser):
    from playwright.sync_api import expect

    ledger = _ledger()
    ledger['providers']['moneydj'].update(capture_decision='reviewed_existing_scope',
        current_scope_review={'completed_questions': ['PRIVATE_DONE'],
                              'remaining_questions': ['PRIVATE_QUESTION']})
    _, path = _configure(tmp_path, ledger=ledger)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 320, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        button = page.locator('[data-news-filter="followup"]')
        expect(button).to_be_enabled()
        expect(page.locator('#source-review-summary')).to_contain_text('有已記錄後續問題 1 個來源鍵')
        button.click()
        expect(button).to_have_attribute('aria-pressed', 'true')
        expect(page.locator('.pipeline-card')).to_have_count(1)
        expect(page.locator('[data-news-review-followup="open"]')).to_contain_text('有已記錄的後續問題')
        expect(page.locator('.pipeline-card')).to_contain_text('已核對現有範圍')
        assert 'PRIVATE_' not in page.locator('body').inner_text()
        atomic_write_json(path, {'contract_version': True})
        page.evaluate('() => refresh()')
        expect(button).to_be_disabled()
        expect(page.locator('[data-news-filter="all"]')).to_have_attribute('aria-pressed', 'true')
        expect(page.locator('.pipeline-card')).to_have_count(2)
        expect(page.locator('#source-review-summary')).to_contain_text('暫無法讀取')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_browser_displays_candidate_work_without_private_payload_or_extra_requests(tmp_path, browser):
    from playwright.sync_api import expect

    status = _seed(tmp_path)
    status['candidate_diagnostic_progress'] = {**candidate_work_sample(),
        'local_retry': {'request_io': 1, 'quarantine': 0, 'archive': 1, 'path': 'PRIVATE_RETRY_PATH'}}
    path = tmp_path/'data_news_fulltext/status.json'
    atomic_write_json(path, status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 320, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1', timeout=15000)
        expect(page.locator('#candidate-inbox')).to_contain_text('待檢查提交 1 個')
        expect(page.locator('#candidate-inbox')).to_contain_text('前次預留待確認 1')
        expect(page.locator('#candidate-inbox')).to_contain_text('本機讀取 1、隔離 0、封存 1 個待重新處理')
        expect(page.locator('#candidate-inbox')).to_contain_text('這不是來源錯誤')
        page.evaluate('() => render({...state.latest, health: "stopped", observed_at_utc: new Date().toISOString()})')
        expect(page.locator('#candidate-inbox')).to_contain_text('這是上次快照')
        page.evaluate('() => render({...state.latest, health: "running", observed_at_utc: new Date().toISOString()})')
        expect(page.locator('#candidate-inbox')).not_to_contain_text('這是上次快照')
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        expect(page.locator('[data-news-candidate-work]')).to_contain_text('這些不是已存正文或文章數')
        assert 'PRIVATE_' not in page.locator('body').inner_text()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        del status['candidate_diagnostic_progress']
        atomic_write_json(path, status)
        page.evaluate('() => refresh()')
        expect(page.locator('#candidate-inbox')).to_contain_text('狀態未知')
        expect(page.locator('[data-news-candidate-work]')).to_have_count(0)
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


@pytest.mark.parametrize('failure', ['transport', 'invalid_snapshot'])
def test_browser_lost_snapshot_withdraws_live_claims_and_restores_last_good(tmp_path, browser, failure):
    from playwright.sync_api import expect

    status = _seed(tmp_path, now=datetime.now(UTC))
    status['candidate_diagnostic_progress'] = {**candidate_work_sample(),
        'local_retry': {'request_io': 1, 'quarantine': 0, 'archive': 1}}
    atomic_write_json(tmp_path/'data_news_fulltext/status.json', status)
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 320, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1 && !state.inFlight', timeout=15000)
        page.evaluate('''() => render({...state.latest, health: 'running', observed_at_utc: new Date().toISOString(),
            acquisition: {...state.latest.acquisition, eligible_eta_seconds: 120},
            providers: state.latest.providers.map(source => source.enabled
                ? {...source, state: 'running', owner_articles_per_second: 2, forecast_rate_basis: 'new_stored_url_heads'}
                : source)})''')
        expect(page.locator('#capture-next')).to_contain_text('就緒正文立即續抓')
        expect(page.locator('#capture-rate')).to_contain_text('120 新增正文／分')
        expect(page.locator('#candidate-inbox')).not_to_contain_text('這是上次快照')
        previous_count = page.locator('#body-count').inner_text()
        page.locator('a[href="#acq-catalog"]').click(timeout=10000)
        page.locator('#source-search').fill('鉅亨')

        def fail(route):
            if failure == 'transport':
                route.abort()
            else:
                route.fulfill(status=200, content_type='application/json', body=json.dumps({
                    'schema_version': 1, 'providers': [None], 'worker': {},
                    'acquisition': {'stored_body_articles': 98765, 'known_urls': 100000}}))

        page.route('**/api/status', fail)
        page.evaluate('() => refresh()')
        assert page.evaluate('state.latest.health') == 'stale'
        expect(page.locator('#news-health')).to_contain_text('更新暫停')
        expect(page.locator('#capture-next')).not_to_contain_text('就緒正文立即續抓')
        expect(page.locator('#capture-rate')).to_have_text('等待有效量測')
        expect(page.locator('#candidate-inbox')).to_contain_text('這是上次快照')
        expect(page.locator('#eligible-eta')).to_have_text('更新失敗，暫無估時')
        assert page.locator('#body-count').inner_text() == previous_count
        assert page.locator('#source-search').input_value() == '鉅亨'

        page.unroute('**/api/status')
        page.evaluate('''() => {if (state.retry) clearTimeout(state.retry); state.retry = null; return refresh();}''')
        assert page.evaluate('state.latest.health') != 'stale'
        expect(page.locator('#news-health')).not_to_contain_text('更新暫停')
        assert page.locator('#source-search').input_value() == '鉅亨'
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_browser_first_invalid_snapshot_does_not_show_partial_data(tmp_path, browser):
    from playwright.sync_api import expect

    _seed(tmp_path, now=datetime.now(UTC))
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 320, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.route('**/api/status', lambda route: route.fulfill(status=200, content_type='application/json',
            body=json.dumps({'schema_version': 1, 'providers': [None], 'worker': {},
                             'acquisition': {'stored_body_articles': 98765, 'known_urls': 100000}})))
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.retry != null && !state.inFlight', timeout=5000)
        assert page.evaluate('state.latest') is None
        expect(page.locator('#news-health')).to_contain_text('面板暫時無法讀取')
        expect(page.locator('#alert-copy')).to_contain_text('尚無可顯示的有效資料')
        for section in ('acq-overview', 'backfill', 'traffic', 'pipelines', 'capture'):
            expect(page.locator(f'#{section}')).to_be_hidden()
        assert '98,765' not in page.locator('body').inner_text()

        # Let the mirrored overview observe the failed partial render before
        # recovering; it must never become visible with that obsolete value.
        page.evaluate('''() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))''')
        page.unroute('**/api/status')
        moment = page.evaluate('''async () => {if (state.retry) clearTimeout(state.retry); state.retry = null;
            await refresh(); const overview = document.getElementById('acq-overview');
            return {hidden: overview.hidden, body: overview.querySelector('[data-acq-source="body-count"]').textContent};}''')
        assert moment['hidden'] or '98,765' not in moment['body']
        expect(page.locator('#acq-overview')).to_be_visible()
        page.locator('a[href="#acq-progress"]').click(timeout=10000)
        expect(page.locator('#backfill')).to_be_visible()
        expect(page.locator('#body-count')).to_have_text('2')
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_browser_withdraws_expired_live_claims_before_slow_status_response(tmp_path, browser):
    from playwright.sync_api import expect

    _seed(tmp_path, now=datetime.now(UTC))
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 320, 'height': 900})
    errors, nonlocal_calls = [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.goto(f'http://{origin}/news/', wait_until='domcontentloaded', timeout=15000)
        page.wait_for_function('() => state.latest?.schema_version === 1 && !state.inFlight', timeout=15000)
        page.evaluate('''() => render({...state.latest, health: 'running', observed_at_utc: new Date().toISOString(),
            providers: state.latest.providers.map(source => source.enabled
                ? {...source, state: 'running', owner_articles_per_second: 2, forecast_rate_basis: 'new_stored_url_heads'}
                : source)})''')
        expect(page.locator('#capture-next')).to_contain_text('就緒正文立即續抓')
        expect(page.locator('#capture-rate')).to_contain_text('120 新增正文／分')

        # The tab wakes after its last observation has expired, but the next
        # status request has not returned yet. Do not call an external source.
        page.evaluate('''() => {const clock = Date.now; Date.now = () => clock() + 181000;
            window.fetch = () => new Promise((resolve, reject) => {window.rejectPendingStatus = reject;});
            refresh();}''')
        assert page.evaluate('state.inFlight') is True
        expect(page.locator('#capture-next')).not_to_contain_text('就緒正文立即續抓')
        expect(page.locator('#capture-rate')).to_have_text('等待有效量測')
        expect(page.locator('#news-health')).to_contain_text('觀測已過期')
        page.evaluate('() => window.rejectPendingStatus(new Error("synthetic offline failure"))')
        page.wait_for_function("() => state.latest?.health === 'stale' && !state.inFlight", timeout=5000)
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_browser_first_failure_before_summary_mount_never_shows_partial_data(tmp_path, browser):
    from playwright.sync_api import expect

    _seed(tmp_path, now=datetime.now(UTC))
    server = _server(tmp_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'127.0.0.1:{server.server_address[1]}'
    page = browser.new_page(viewport={'width': 320, 'height': 900})
    delayed, errors, nonlocal_calls = [], [], []
    page.on('pageerror', lambda exc: errors.append(type(exc).__name__))
    page.on('request', lambda req: nonlocal_calls.append(req.url) if urlsplit(req.url).netloc != origin else None)
    try:
        page.route('**/dashboard-acquisition.js*', lambda route: delayed.append(route))
        page.route('**/api/status', lambda route: route.fulfill(status=200, content_type='application/json',
            body=json.dumps({'schema_version': 1, 'providers': [None], 'worker': {},
                             'acquisition': {'stored_body_articles': 98765, 'known_urls': 100000}})))
        page.goto(f'http://{origin}/news/', wait_until='commit', timeout=15000)
        page.wait_for_function("() => document.querySelector('#news-health')?.textContent.includes('面板暫時無法讀取')", timeout=10000)
        page.wait_for_timeout(200)
        assert len(delayed) == 1
        page.evaluate('''() => {if (state.retry) clearTimeout(state.retry); state.retry = null;}''')
        delayed[0].continue_()
        page.wait_for_selector('#acq-overview', state='attached', timeout=10000)
        expect(page.locator('#acq-overview')).to_be_hidden()
        assert '98,765' not in page.locator('body').inner_text()

        page.unroute('**/api/status')
        page.evaluate('() => refresh()')
        expect(page.locator('#acq-overview')).to_be_visible()
        expect(page.locator('[data-acq-source="body-count"]')).to_have_text('2')
        assert not errors and not nonlocal_calls
    finally:
        page.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
