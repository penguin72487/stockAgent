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
from test_news_dashboard import NOW, _metadata_feed_snapshot
from test_news_dashboard_cnyes_list_progress import history_snapshot, index_snapshot
from test_news_body_state_partition import partition_snapshot


@pytest.mark.parametrize('width,height', [(1440, 1000), (390, 844), (320, 768)])
@pytest.mark.parametrize('migration', ['scanning', None, 'no_source'])
def test_real_page_keeps_all_known_body_ratio_separate_from_finished_cohort(tmp_path, width, height, migration):
    now = datetime.now(UTC)
    status = _seed(tmp_path, now=now)
    status['url_metadata_inbox'] = {'contract_version': 1, 'available': True,
        'request_files': 2, 'pending_requests': 2, 'targets_unexamined': 3,
        'completed_unarchived': 0, 'invalid_or_unreadable': 0}
    status['finlab_cnyes_priority'] = {'contract_version': 1, 'raw_unique_delta': 182,
        'raw_hints_examined': 182, 'targets': 182, 'request_state': 'checked', 'owner_checked': 182,
        'recorded_at_utc': now.isoformat(),
        'outcomes': {'metadata_registered': 180, 'existing_head_preserved': 1,
                     'source_contract_changed': 0, 'other': 1}, 'unverified_origin': 1,
        'body_completion_proven': False, 'association_completion_proven': False}
    source = status['providers']['cnyes']
    source.update(states={'complete': 2, 'waiting_cohort': 4}, acquisition_mode='finite',
                  finite_cohort={'urls': 2, 'stored_bodies': 2}, stored_body_urls=3)
    history = history_snapshot()
    history['current_source_contract_scope_states'].update(
        inconsistent_source_snapshot=1, waiting_original_page_recheck=2,
        waiting_child_evidence=4, source_page_cap_exceeded=5, invalid_page_evidence=6, invalid_scope_evidence=2,
        inconsistent_original_page_ID_reference=7)
    history['completed_integrity_scan'] = {'contract_version': 1, 'scan_state': 'scanning',
        'checked_scopes': 40, 'affected_scopes': 3, 'local_metadata_only': True,
        'all_history_complete': False, 'publisher_requests': 0}
    history['overflow_retained_recovery'] = {'contract_version': 1, 'scan_state': 'scanning',
        'pending_retained_pages': 17, 'local_retry_pages': 4, 'local_raw_only': True,
        'category_gap_completion_claimed': False, 'publisher_requests': 0}
    history['unobserved_nonzero_retained_scan'] = {'contract_version': 1, 'scan_state': 'scanning',
        'checked_steps': 40, 'queued_candidate_pages': 17, 'local_raw_only': True,
        'all_history_complete': False, 'publisher_requests': 0}
    history['accepted_body_batch_scan'] = {'contract_version': 1, 'scan_state': 'scanning',
        'scanned_batches': 120, 'indexed_candidates': 17, 'local_raw_only': True,
        'local_retry_batches': 2, 'local_missing_retry_batches': 3, 'local_invalid_retry_batches': 4,
        'remaining_frontier_batches': 80, 'estimated_frontier_batches': 200,
        'local_invalid_migration_state': migration,
        'all_history_complete': False, 'publisher_requests': 0}
    if migration == 'no_source':
        history['accepted_body_batch_scan'].update(scan_state='unknown', scanned_batches=None,
            indexed_candidates=None, local_retry_batches=None, local_missing_retry_batches=None,
            remaining_frontier_batches=None, estimated_frontier_batches=None,
            local_invalid_retry_batches=None, local_invalid_migration_state=None)
    source.update(public_index_progress=index_snapshot(), history_list_progress=history)
    status.update(states={'complete': 2, 'waiting_cohort': 4, 'awaiting_authorization': 4},
                  provider_stored_body_counter_contract_version=1, articles_with_stored_body=3)
    status['providers']['unregistered:example.org']['stored_body_urls'] = 0
    _save(tmp_path, status)
    payload = build_news_public_status(tmp_path, now=now)
    assert payload['snapshot_consistent']
    payload['retained_link_handoff'] = {
        'plan': {'provider': 'cmnews', 'candidates': 772, 'requests': 2,
                 'source_and_policy_revalidation_required': True, 'body_verified': False},
        'runtime': {'provider': 'cmnews', 'state': 'owner_checked', 'candidates': 772, 'requests': 2,
                    'submitted_requests': 2, 'reason': None, 'body_verified': False,
                    'outcomes': {'metadata_registered': 770, 'existing_head_preserved': 1,
                                 'source_contract_changed': 1, 'other': 0}}}
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
            page.locator('a[href="#acq-progress"]').click(timeout=10000)
            expect(page.locator('#url-inbox')).to_be_visible()
            expect(page.locator('#url-inbox')).to_contain_text('3 項提交（2 個請求）尚待 owner 檢查')
            expect(page.locator('#finlab-cnyes-priority')).to_contain_text('owner 已檢查 182 / 182 個')
            expect(page.locator('#finlab-cnyes-priority')).to_contain_text('新增網址 180、保留既有 1')
            expect(page.locator('#finlab-cnyes-priority')).to_contain_text('1 個來源檔案或原始列無法核對，已保留網址但未採用日期')
            expect(page.locator('#finlab-cnyes-priority')).to_contain_text('正文下載或完整歷史進度')
            expect(page.locator('#finlab-cnyes-priority')).to_contain_text('中斷後可能為恢復時間')
            expect(page.locator('#retained-link-handoff')).to_contain_text('772 個候選網址、2 批')
            expect(page.locator('#retained-link-handoff')).to_contain_text('新增 770、保留既有 1、來源政策變動 1')
            expect(page.locator('#retained-link-handoff')).to_contain_text('候選不代表已取得正文')
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
            expect(card.locator('[data-news-history-count="reference"]')).to_have_text('7')
            expect(card.locator('[data-news-history-count="invalid_evidence"]')).to_have_text('6')
            expect(card.locator('[data-news-history-count="invalid_scope"]')).to_have_text('2')
            expect(card.locator('[data-news-integrity-scan]')).to_contain_text('本輪已檢查 40 個範圍，發現 3 個受影響範圍')
            expect(card.locator('[data-news-history-count="page_cap"]')).to_have_text('5')
            expect(card.locator('[data-news-overflow-recovery]')).to_contain_text('盤點中；已排入待核驗 17 頁')
            expect(card.locator('[data-news-overflow-recovery]')).to_contain_text('歷史清單原件待本機重試：4 頁')
            expect(card.locator('[data-news-nonzero-local-recovery]')).to_contain_text('本輪已檢視 40 個區間或頁面步驟')
            expect(card.locator('[data-news-nonzero-local-recovery]')).to_contain_text('發現 17 頁待核驗候選')
            if migration == 'no_source':
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('待本機重試批次數未知')
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('待再檢查缺檔批次數未知')
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('待復原異常批次數未知')
                expect(card.locator('[data-news-accepted-frontier]')).to_contain_text('估計分母未知')
                expect(card.locator('[data-news-accepted-frontier-progress]')).to_have_count(0)
            else:
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('本輪已嘗試盤點 120 個批次')
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('2 個批次待本機重試')
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('3 個缺檔批次待再檢查')
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('已盤點的全來源批次有 4 個異常批次等待本機復原')
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('索引 17 個原件候選')
                expect(card.locator('[data-news-accepted-frontier]')).to_contain_text('120 / 200 已嘗試，剩餘 80 個尚未嘗試')
                expect(card.locator('[data-news-accepted-frontier-progress]')).to_have_attribute('value', '120')
                expect(card.locator('[data-news-accepted-frontier-progress]')).to_have_attribute('max', '200')
                expect(card.locator('[data-news-accepted-frontier-progress]')).to_have_attribute('aria-label', '本輪已登記本機批次的已嘗試比例，非新聞下載完成率')
                expect(card.locator('[data-news-accepted-frontier]')).to_contain_text('這不是全文下載進度，完成時間未知')
            expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text(
                '舊異常紀錄補登：補登中' if migration == 'scanning' else '舊異常紀錄補登：狀態未知')
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
            if width == 390 and migration == 'scanning':
                payload['health'] = 'stopped'
                page.reload(wait_until='networkidle', timeout=15000)
                page.locator('a[href="#acq-catalog"]').click(timeout=10000)
                card = page.locator('#pipeline-grid .pipeline-card').filter(has=page.locator('.news-provider-identity small').filter(has_text='cnyes'))
                card.locator('[data-news-list-provider="cnyes"] > summary').click()
                expect(card.locator('[data-news-accepted-frontier]')).to_contain_text('上次快照：')
                expect(card.locator('[data-news-accepted-batch-scan]')).to_contain_text('上次快照：當時')
                del payload['retained_link_handoff']
                page.reload(wait_until='networkidle', timeout=15000)
                page.locator('a[href="#acq-progress"]').click(timeout=10000)
                expect(page.locator('#retained-link-handoff')).to_contain_text('面板服務尚未載入候選交接資訊')
                expect(page.locator('#finlab-cnyes-priority')).to_contain_text('上次快照')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            assert not errors and not forbidden
            page.close()
        finally:
            browser.close()


@pytest.mark.parametrize('width,height', [(1440, 1000), (390, 844)])
@pytest.mark.parametrize('legacy', [False, True])
def test_joint_body_state_counts_render_reconcile_and_survive_filters(tmp_path, width, height, legacy):
    now = datetime.now(UTC)
    status = partition_snapshot(tmp_path)
    status['updated_at_utc'] = now.isoformat()
    if legacy:
        del status['provider_body_state_counter_contract_version']
        for source in status['providers'].values():
            del source['body_state_counts']
    _save(tmp_path, status)
    payload = build_news_public_status(tmp_path, now=now)
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
            expect(page.locator('#body-count')).to_have_text('17')
            expect(page.locator('#remaining-count')).to_have_text('4' if legacy else '2')
            expect(page.locator('#acq-overview .acq-metric').filter(has=page.locator('[data-acq-source="remaining-count"]')).locator('.acq-metric-label')).to_have_text('待排網址（含重查）' if legacy else '待排缺正文')
            page.locator('a[href="#acq-progress"]').click(timeout=10000)
            page.locator('#outcome-panel > summary').click()
            expect(page.locator('[data-news-outcome-total="repair_required"]')).to_have_text('13')
            expect(page.locator('[data-news-missing-total="repair_required"]')).to_have_text('—' if legacy else '0')
            expect(page.locator('[data-news-retained-total="repair_required"]')).to_have_text('—' if legacy else '13')
            page.locator('a[href="#acq-catalog"]').click(timeout=10000)
            card = page.locator('#pipeline-grid .pipeline-card').filter(has=page.locator('.news-provider-identity small').filter(has_text='cnyes'))
            expect(card.locator('[data-news-missing-body-urls]')).to_have_text('4' if legacy else '2')
            if legacy:
                expect(card.locator('[data-news-body-partition-unknown]')).to_be_visible()
            else:
                expect(card.locator('[data-news-queued-missing]')).to_have_text('2')
                expect(card.locator('[data-news-queued-retained]')).to_have_text('2')
                expect(card.locator('[data-news-retained-noncomplete]')).to_have_text('15')
            detail = card.locator('[data-news-outcome-provider="cnyes"]')
            # Native details toggle events can arrive after a subsequent input
            # replaces the card. Force both actions into one browser task.
            page.evaluate('''() => {
                document.querySelector('[data-news-outcome-provider="cnyes"] > summary').click();
                const input = document.querySelector('#source-search');
                input.value = 'cnyes';
                input.dispatchEvent(new Event('input', {bubbles: true}));
            }''')
            expect(detail).to_have_attribute('open', '')
            expect(detail.locator('[data-news-retained="repair_required"]')).to_have_text('—' if legacy else '13')
            page.locator('#source-search').fill('')
            page.locator('[data-news-filter="not_enabled"]').click()
            expect(page.locator('#pipeline-grid .pipeline-card')).to_have_count(1)
            page.locator('[data-news-filter="all"]').click()
            expect(page.locator('#pipeline-grid .pipeline-card')).to_have_count(2)
            expect(detail).to_have_attribute('open', '')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            assert not errors and not forbidden
            page.close()
        finally:
            browser.close()


@pytest.mark.parametrize('width,height,activity', [(1440, 1000, 'running'), (390, 844, 'stopped')])
@pytest.mark.parametrize('complete', [False, True])
def test_metadata_original_history_renders_scope_and_stopped_snapshot(tmp_path, width, height, activity, complete):
    status = _metadata_feed_snapshot(tmp_path)
    status['activity'] = activity
    status['providers']['ltn']['metadata_feed_progress']['legacy_original_index'] = {
        'contract_version': 1, 'expected_directories': 28, 'registered_directories': 28,
        'examined_directories': 28 if complete else 10, 'candidate_originals': 12, 'verified_originals': 4,
        'pending_originals': 5, 'retry_originals': 1, 'invalid_originals': 2,
        'other_originals': 0, 'discovery_complete': complete}
    _save(tmp_path, status)
    payload = build_news_public_status(tmp_path, now=NOW)
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
            card = page.locator('#pipeline-grid .pipeline-card').filter(
                has=page.locator('.news-provider-identity small').filter(has_text='ltn'))
            note = card.locator('[data-news-metadata-feed]')
            expect(note).to_contain_text(f'舊原件目錄已檢查 {28 if complete else 10} / 28')
            expect(note).to_contain_text('發現待核驗原件 12 份，已驗 4，待驗 5，重試 1，異常 2')
            expect(note).to_contain_text('這些不是文章數或全文完成率')
            if complete:
                expect(note).to_contain_text('最近一輪已掃描，之後新增的原件仍需巡檢')
            else:
                expect(note).to_contain_text('仍在清點，候選原件數會增加')
            if activity == 'stopped':
                expect(note).to_contain_text('上次快照')
                expect(note).to_contain_text('下載器顯示已停止')
            assert page.evaluate('document.documentElement.scrollWidth') <= width
            assert not errors and not forbidden
            page.close()
        finally:
            browser.close()


def test_inconsistent_state_snapshot_never_renders_a_false_completed_count(tmp_path):
    now = datetime.now(UTC)
    status = _seed(tmp_path, now=now)
    status['providers']['cnyes']['states'] = {'complete': 6}
    _save(tmp_path, status)
    payload = build_news_public_status(tmp_path, now=now)
    assert not payload['snapshot_consistent']
    services = Path(__file__).resolve().parents[1] / 'services'
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            page = browser.new_page(viewport={'width': 1440, 'height': 1000})
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
            card = page.locator('#pipeline-grid .pipeline-card').filter(
                has=page.locator('.news-provider-identity small').filter(has_text='cnyes'))
            expect(card.locator('.pipeline-status')).to_have_text('尚無有效快照')
            expect(card.locator('.news-provider-facts').first.locator('dd').first).to_have_text('— / —')
            assert not card.locator('.mini-progress-track').evaluate('(node) => node.hasAttribute("value")')
            expect(card).to_contain_text('快照狀態無法核對')
            expect(card).to_contain_text('正文入庫狀態待核對')
            expect(card).not_to_contain_text('已知待辦已清')
            expect(card).not_to_contain_text('尚無已發現網址')
            assert not errors and not forbidden
            page.close()
        finally:
            browser.close()
