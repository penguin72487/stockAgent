#!/usr/bin/env python3
"""Read-only, rendered TEJ acceptance. Explicit software compositor, no GUI query.

Ordinary browser interactions only; no force clicks, DOM-click substitution or
provider requests. This is HTML/2D acceptance, not a GPU/WebGL claim.
"""
from __future__ import annotations

import argparse
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
from scripts.probe_browser_runtime import browser_profile_launch_options


def verify(url: str, output: Path) -> dict:
    from playwright.sync_api import expect, sync_playwright
    if output.exists():
        raise FileExistsError("Do not overwrite rendered acceptance evidence")
    output.mkdir(parents=True)
    results = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            for name, width in (('desktop',1440),('mobile',390)):
                page = browser.new_page(viewport={'width':width,'height':1000},device_scale_factor=1)
                errors, provider_requests = [], []
                page.on('pageerror',lambda exc: errors.append(type(exc).__name__))
                page.on('request',lambda req: provider_requests.append(urlsplit(req.url).netloc)
                        if urlsplit(req.url).netloc and urlsplit(req.url).netloc != urlsplit(url).netloc else None)
                response = page.goto(url,wait_until='domcontentloaded',timeout=30000)
                expect(page.locator('#catalog-fields')).to_have_text('45,826',timeout=30000)
                expect(page.locator('#catalog-tables')).to_have_text('255')
                expect(page.locator('#feature-rows tr')).to_have_count(50)
                # Wait for the complete page render, not an intermediate DOM
                # with rows inserted but its pagination metadata still pending.
                expect(page.locator('#feature-page')).to_have_text(re.compile(r'^1–50'),timeout=15000)
                expect(page.locator('#feature-next')).to_be_enabled(timeout=15000)
                expect(page.locator('#quota-day-calls')).to_have_text('未知')
                forecast_response = page.request.get(url.rstrip('/')+'/api/status',timeout=15000)
                assert forecast_response.status==200
                status=forecast_response.json();forecast=status['eta']['forecast']
                assert status['schema_version']==8 and forecast['contract']=='tej_staged_query_scenarios_v1'
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
                for section in ('overview','progress','features'):
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
                page.close()
        finally:
            browser.close()
    receipt = {'observed_at_utc':datetime.now(UTC).isoformat(),'url':url,
               'profile':'explicit_cpu_2d_no_gpu_webgl_coverage','accepted':True,'viewports':results}
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
