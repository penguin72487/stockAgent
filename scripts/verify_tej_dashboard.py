#!/usr/bin/env python3
"""Read-only, rendered TEJ acceptance. Explicit software compositor, no GUI query.

Ordinary browser interactions only; no force clicks, DOM-click substitution or
provider requests. This is HTML/2D acceptance, not a GPU/WebGL claim.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.live.tej_dashboard import SCHEMA_VERSION
from scripts.probe_browser_runtime import browser_profile_launch_options


def open_acquisition_sections(page, *keys: str) -> None:
    """Use real disclosure clicks before checking lazily loaded details."""
    for key in keys:
        details=page.locator('#acq-'+key)
        if details.count() and details.get_attribute('open') is None:
            details.locator(':scope > summary').click(timeout=10000)


def scroll_section_into_view(page, selector: str) -> None:
    """Allow deferred upstream sections to lay out before visibility checks."""
    target = page.locator(selector)
    for _ in range(3):
        target.scroll_into_view_if_needed(timeout=10000)
        page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
        if target.evaluate('(node) => {const r=node.getBoundingClientRect();return r.top>=0 && r.bottom<=innerHeight;}'):
            return
    from playwright.sync_api import expect
    expect(target).to_be_in_viewport(timeout=10000)


def verify_progress_binding(browser, url: str, baseline: dict, output: Path) -> dict:
    """Isolated HTTP-response fixtures, never changes the production queue.

    Live/source-backed checks run separately below. These fixtures exercise
    polling and state transitions that a safely paused downloader cannot emit.
    """
    from playwright.sync_api import expect
    page = browser.new_page(viewport={'width':1280,'height':900})
    source = baseline['activity']['last_successful_download']
    assert source, 'Dynamic binding acceptance needs an existing registered table'
    fixture = deepcopy(baseline)
    fixture['state'] = 'running'
    fixture['worker'].update(alive=True, state='running', kind='download')
    fixture['scheduler'].update(alive=True, state='executing', paused_reason=None)
    fixture['activity']['current_task'] = dict(task_id='a'*24, table_id=source['table_id'],
        table_name=source['table_name'], phase='P1', kind='download', query_work_rows=4,
        stage='awaiting_preview', elapsed_seconds=1, readback_ratio=None,
        readback_scanned_row_slots=None, readback_total_row_slots=None)
    current = {'payload':fixture, 'http_status':200}
    calls = {'status':0,'features':0}
    errors = []
    page.on('pageerror',lambda error: errors.append(type(error).__name__))
    page.on('request',lambda request: calls.__setitem__('features',calls['features']+1)
            if '/tej/api/features?' in request.url else None)

    def respond(route):
        calls['status'] += 1
        body = deepcopy(current['payload'])
        clock = datetime.now(UTC).isoformat()
        body['observed_at_utc'] = clock
        if body['activity']['current_task']:
            body['activity']['current_task']['started_at_utc'] = clock
            body['activity']['current_task']['progress_observed_at_utc'] = clock
        route.fulfill(status=current['http_status'],json=body)

    page.route('**/tej/api/status',respond)
    try:
        page.goto(url,wait_until='domcontentloaded',timeout=30000)
        open_acquisition_sections(page,'progress','catalog')
        expect(page.locator('#active-stage')).to_have_text('等待來源回覆')
        expect(page.locator('#active-readback-card')).to_be_hidden()
        initial = baseline['workload']['exported_rows']
        expect(page.locator('#exported-rows')).to_have_text(format(initial,','))
        page.locator('#table-search').fill('Price')
        reading = fixture['activity']['current_task']
        reading.update(stage='reading_preview',readback_ratio=.5,readback_scanned_row_slots=2,readback_total_row_slots=4)
        expect(page.locator('#active-readback')).to_have_attribute('value','0.5',timeout=12000)
        expect(page.locator('#active-readback-label')).to_contain_text('尚未驗證入庫')
        expect(page.locator('#exported-rows')).to_have_text(format(initial,','))
        expect(page.locator('#table-search')).to_have_value('Price')
        expect(page.locator('#table-search')).to_be_focused()
        fixture['activity'].update(current_task=None,completion_revision='isolated-fixture-commit',
            completed_download_tasks=baseline['activity']['completed_download_tasks']+1,
            last_successful_download={**source,'actual_rows':4,'actual_bytes':100,
                                      'completed_at_utc':datetime.now(UTC).isoformat()})
        fixture['workload']['exported_rows'] = initial+4
        fixture['workload']['recorded_bytes'] += 100
        fixture['worker'].update(alive=False,state='completed_task')
        fixture['scheduler']['state'] = 'between_tasks'
        fixture['state'] = 'automatic_waiting'
        expect(page.locator('#exported-rows')).to_have_text(format(initial+4,','),timeout=12000)
        expect(page.locator('#active-readback-card')).to_be_hidden()
        expect(page.locator('#last-success-detail')).to_contain_text('4 分片列')
        expect(page.locator('#table-search')).to_have_value('Price')
        current['http_status'] = 503
        expect(page.locator('#tej-health')).to_have_text('更新暫停 · 上次觀測',timeout=12000)
        expect(page.locator('#exported-rows')).to_have_text(format(initial+4,','))
        expect(page.locator('#active-readback-card')).to_be_hidden()
        current['http_status'] = 200
        fixture['state'] = 'needs_review'
        fixture['scheduler'].update(state='waiting_recovery',paused_reason='source_validation_failed')
        expect(page.locator('#active-stage')).to_have_text('等待安全恢復',timeout=12000)
        expect(page.locator('#table-search')).to_have_value('Price')
        fixture['scheduler'].update(state='replaying_authorized', authorized_unknown_replay={
            'enabled':True,'max_replays_per_task_hour':2,'max_replays_per_hour':6,
            'state':'checking_authorized_replay','next_check_at_utc':None})
        expect(page.locator('#active-stage')).to_contain_text('依使用者授權核對／重排',timeout=12000)
        expect(page.locator('#tej-automation')).to_contain_text('已授權有界自動重排')
        expect(page.locator('#exported-rows')).to_have_text(format(initial+4,','))
        expect(page.locator('#active-readback-card')).to_be_hidden()
        page.locator('#activity').scroll_into_view_if_needed()
        page.screenshot(path=str(output/'isolated-polling-fixture.png'))
        assert not errors
        return {'basis':'isolated_browser_response_fixture_not_a_TEJ_download',
                'production_queue_modified':False,'provider_queries_sent':0,
                'automatic_poll_transitions':['awaiting_preview','reading_preview','committed','http_error','waiting_recovery','replaying_authorized'],
                'preserved_filter_and_focus':True,'readback_not_counted_as_committed':True,
                'requests':calls,'errors':errors,'accepted':True}
    finally:
        page.close()


def verify(url: str, output: Path) -> dict:
    from playwright.sync_api import expect, sync_playwright
    if output.exists():
        raise FileExistsError("Do not overwrite rendered acceptance evidence")
    output.mkdir(parents=True)
    results = []
    polling = None
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            for name, width in (('desktop',1440),('mobile',390)):
                page = browser.new_page(viewport={'width':width,'height':1000},device_scale_factor=1)
                errors, provider_requests = [], []
                status_responses = []
                def record_status_response(reply):
                    if urlsplit(reply.url).path=='/tej/api/status' and reply.status==200:
                        # Event callbacks must not block for a body while a
                        # different greenlet closes this page. Read completed
                        # bodies explicitly below, before leaving its lifetime.
                        status_responses.append(reply)
                page.on('response',record_status_response)
                page.on('pageerror',lambda exc: errors.append(type(exc).__name__))
                page.on('request',lambda req: provider_requests.append(urlsplit(req.url).netloc)
                        if urlsplit(req.url).netloc and urlsplit(req.url).netloc != urlsplit(url).netloc else None)
                response = page.goto(url,wait_until='domcontentloaded',timeout=30000)
                expect(page.locator('#catalog-fields')).to_have_text('45,826',timeout=30000)
                expect(page.locator('#catalog-tables')).to_have_text('255')
                # The shared acquisition layout defaults to collapsed details.
                # A hidden feature list intentionally makes no API calls.
                open_acquisition_sections(page,'progress','quota','catalog')
                scroll_section_into_view(page,'#features h2')
                expect(page.locator('#feature-rows tr')).to_have_count(50,timeout=15000)
                # Wait for the complete page render, not an intermediate DOM
                # with rows inserted but its pagination metadata still pending.
                expect(page.locator('#feature-page')).to_have_text(re.compile(r'^1–50'),timeout=15000)
                expect(page.locator('#feature-next')).to_be_enabled(timeout=15000)
                expect(page.locator('#quota-day-calls')).to_have_text('未知')
                # Use the source response actually rendered in this frame.
                # A second HTTP call can be a newer completed generation while
                # the page is correctly still showing its previous five-second
                # sample. Never freeze production or compare different clocks.
                rendered_sample=page.evaluate("""() => ({status:state.latest,
                    rowProgress:document.querySelector('#result-row-progress').getAttribute('value'),
                    byteProgress:document.querySelector('#byte-progress').getAttribute('value')})""")
                status=rendered_sample['status'];forecast=status['eta']['forecast']
                assert status in [reply.json() for reply in tuple(status_responses)], 'Rendered state must match an actual source HTTP response'
                assert status['schema_version']==SCHEMA_VERSION and forecast['contract']=='tej_staged_query_scenarios_v1'
                activity=status['activity']
                assert activity['contract']=='tej_download_activity_v1' and activity['refresh_seconds']==5
                expect(page.locator('#completed-downloads')).to_have_text(format(activity['completed_download_tasks'],','))
                last=activity['last_successful_download']
                if last:
                    expect(page.locator('#last-success-detail')).to_contain_text(last['table_name'])
                    expect(page.locator('#last-success-detail')).to_contain_text(format(last['actual_rows'],','))
                if activity['current_task'] is None:
                    expect(page.locator('#active-table')).to_have_text('目前沒有正在下載的資料表')
                    expect(page.locator('#active-readback-card')).to_be_hidden()
                expect(page.locator('#activity-sync')).to_contain_text('每 5 秒原位更新')
                bounds=status.get('planning',{})
                expect(page.locator('#local-capacity')).to_contain_text(format(bounds['local_max_cells'],','))
                expect(page.locator('#local-capacity')).to_contain_text('不是官方配額')
                if activity['blocked_tasks_total'] or activity.get('deferred_tasks_total',0):
                    expect(page.locator('#blocker-summary')).to_contain_text(str(activity['blocked_tasks_total'])+' 個工作')
                    page.locator('#blocker-summary').click(timeout=10000)
                    for task in activity['blocked_tasks']:
                        expect(page.locator('#blocker-list')).to_contain_text(task['table_name'])
                    for task in activity.get('deferred_tasks',[]):
                        expect(page.locator('#blocker-list')).to_contain_text(task['table_name'])
                    if activity.get('deferred_tasks_total',0):
                        expect(page.locator('#blocker-list')).to_contain_text('下次嘗試')
                    page.locator('#blocker-summary').click(timeout=10000)
                for rendered,done,remaining in (
                    (rendered_sample['rowProgress'],status['workload']['exported_rows'],forecast['global_scenarios']['middle']['remaining_export_rows']),
                    (rendered_sample['byteProgress'],status['workload']['recorded_bytes'],forecast['global_scenarios']['middle']['remaining_local_bytes'])):
                    if remaining is None:assert rendered is None
                    elif done+remaining>0:assert abs(float(rendered)-done/(done+remaining))<1e-12
                assert forecast['scheduled_complete_at_utc'] is None
                assert len(forecast['phases'])==3
                for phase in forecast['phases']:
                    assert phase['scheduled_complete_at_utc'] is None
                    durations=[phase['scenarios'][s]['dependency_remaining_seconds'] for s in ('fast','middle','slow')]
                    assert all(v is None or v>=0 for v in durations)
                    if all(v is not None for v in durations):assert durations==sorted(durations)
                    if phase['bundled_into_earlier_tables']:
                        assert phase['scenarios']['middle']['additional_queries']==0
                        assert phase['scenarios']['middle']['dependency_remaining_seconds']>0
                assert forecast['phases'][2]['cross_source_validation_remaining_seconds'] is None
                expect(page.locator('#forecast-known')).to_contain_text('次已核實剩餘查詢')
                expect(page.locator('#forecast-progress-label')).to_contain_text('不是歷史完整率')
                expect(page.locator('#phase-rows tr')).to_have_count(3)
                expect(page.locator('#phase-rows tr').last).to_contain_text('真正多源校驗工時未知')
                expect(page.locator('#forecast-execution')).to_contain_text(
                    '非保證完成日' if forecast['execution_state']=='automatic_running' else
                    '未排定' if forecast['execution_state']=='finite_batch_running' else '尚未排定')
                expect(page.locator('#tej-automation')).to_contain_text('自動排程存活' if status.get('scheduler',{}).get('alive') else '自動排程未運行')
                startup=status.get('desktop_startup',{})
                if startup.get('enabled'):
                    assert startup['requires_windows_user_logon'] is True and startup['windows_autologin_changed'] is False
                    expect(page.locator('#tej-automation')).to_contain_text('Windows 登入後每 60 秒核對')
                replay=status.get('scheduler',{}).get('authorized_unknown_replay',{})
                if replay.get('enabled'):
                    expect(page.locator('#tej-automation')).to_contain_text('已授權有界自動重排')
                    expect(page.locator('#tej-automation')).to_contain_text('非官方配額')
                predicted_value=page.locator('#forecast-progress').get_attribute('value')
                assert predicted_value is not None and 0<=float(predicted_value)<1
                stacks=page.locator('#phase-rows .tej-scenario-stack')
                expect(stacks).to_have_count(6)
                if width<900:
                    page.locator('#phase-rows').scroll_into_view_if_needed(timeout=10000)
                    page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
                    positions=stacks.evaluate_all("""nodes=>nodes.map(node=>({
                        column:getComputedStyle(node).gridColumnStart,
                        lines:Array.from(node.children,child=>{const r=child.getBoundingClientRect();return {x:r.x,y:r.y,right:r.right};})
                    }))""")
                    for position in positions:
                        assert position['column']=='2'
                        assert len(position['lines'])==3
                        assert max(r['x'] for r in position['lines'])-min(r['x'] for r in position['lines'])<1
                        assert all(r['right']<=width for r in position['lines'])
                        assert all(r['right']>r['x'] for r in position['lines'])
                        assert all(a['y']<b['y'] for a,b in zip(position['lines'],position['lines'][1:]))
                value = page.locator('#row-progress').get_attribute('value')
                if value is None:
                    expect(page.locator('#row-progress-label')).to_contain_text('未知')
                else:
                    assert 0 <= float(value) < 1, 'A partial history cannot be rendered as 100%'
                page.locator('#feature-next').click(timeout=10000)
                expect(page.locator('#feature-page')).to_have_text(re.compile(r'^51'),timeout=15000)
                page.locator('#feature-phase').select_option('P2')
                expect(page.locator('#feature-count')).to_have_text(re.compile('811'),timeout=15000)
                page.locator('#feature-phase').select_option('all')
                page.locator('#feature-search').fill('Volume')
                expect(page.locator('#feature-rows tr')).not_to_have_count(0)
                expect(page.locator('#feature-rows tr').first).to_contain_text('Volume',timeout=10000)
                facts = page.evaluate("""() => ({viewport:innerWidth,document_width:document.documentElement.scrollWidth,
                    types:document.querySelector('#catalog-types').textContent,
                    fields:document.querySelector('#catalog-fields').textContent,
                    health:document.querySelector('#tej-health').textContent,
                    active_table:document.querySelector('#active-table').textContent,
                    active_stage:document.querySelector('#active-stage').textContent,
                    last_success:document.querySelector('#last-success').textContent,
                    completed_downloads:document.querySelector('#completed-downloads').textContent,
                    automation:document.querySelector('#tej-automation').textContent,
                    activity_sync:document.querySelector('#activity-sync').textContent,
                    result_row_progress:document.querySelector('#result-row-progress').getAttribute('value'),
                    byte_progress:document.querySelector('#byte-progress').getAttribute('value'),
                    quota:document.querySelector('#quota-day-calls').textContent,
                    global_eta:document.querySelector('#global-eta').textContent,
                    search_count:document.querySelector('#feature-count').textContent,
                    query_grid_target:document.querySelector('#total-rows').textContent,
                    row_progress_value:document.querySelector('#row-progress').getAttribute('value'),
                    forecast_execution:document.querySelector('#forecast-execution').textContent,
                    forecast_known:document.querySelector('#forecast-known').textContent,
                    forecast_progress:document.querySelector('#forecast-progress').getAttribute('value'),
                    forecast_rows:document.querySelector('#forecast-total-rows').textContent,
                    forecast_capacity:document.querySelector('#forecast-total-bytes').textContent,
                    phase_text:Array.from(document.querySelectorAll('#phase-rows tr'),r=>r.textContent),
                    phase_rows:document.querySelectorAll('#phase-rows tr').length})""")
                assert response and response.status == 200
                assert facts['document_width'] <= width, facts
                assert facts['types'] == '30' and facts['phase_rows'] == 3
                assert not errors and not provider_requests, (errors,provider_requests)
                # Shared sections use content-visibility:auto. Bring the
                # header into the viewport and wait for actual frames rather
                # than capturing an offscreen intrinsic-size placeholder.
                for section in ('activity','overview','progress','features'):
                    page.locator('#'+section+' h2').scroll_into_view_if_needed(timeout=10000)
                    page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
                    page.screenshot(path=str(output/(name+'-'+section+'.png')),timeout=10000)
                    if section=='progress':
                        # Capture the whole estimate section too: bringing
                        # just its heading onscreen need not show the stage
                        # rows near the bottom of a tall mobile section.
                        page.locator('#progress').screenshot(path=str(output/(name+'-forecast-section.png')),timeout=10000)
                        page.locator('#phase-rows').scroll_into_view_if_needed(timeout=10000)
                        page.evaluate('() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))')
                        page.locator('.tej-stage-scroll').screenshot(path=str(output/(name+'-stage-table.png')),timeout=10000)
                results.append({'profile':name,'http_status':response.status,'errors':errors,
                                'external_provider_requests':len(provider_requests),
                                'eta_contract':forecast['contract'],'eta_input_sha256':forecast['input_sha256'],**facts})
                page.remove_listener('response',record_status_response)
                page.close()
            polling = verify_progress_binding(browser,url,status,output)
        finally:
            browser.close()
    receipt = {'observed_at_utc':datetime.now(UTC).isoformat(),'url':url,
               'profile':'explicit_cpu_2d_no_gpu_webgl_coverage','accepted':True,'viewports':results,
               'isolated_polling_acceptance':polling}
    atomic_write_json(output/'rendered_acceptance.json',receipt)
    return receipt


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='https://penguin72487.ddnsgeek.com/tej/')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    print(json.dumps(verify(args.url,args.output),ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
