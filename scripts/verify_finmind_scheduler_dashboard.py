"""Read-only acceptance of the deployed FinMind page with the shared browser profile."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

from downloader.artifact_io import atomic_write_json
from scripts.probe_browser_runtime import browser_profile_launch_options
from downloader.finmind_history_order import HISTORY_STAGES, STAGES, metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='https://penguin72487.ddnsgeek.com')
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    profiles = []
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options('cpu-2d'), timeout=10000)
        try:
            for width in (1440, 1024, 390):
                page = browser.new_page(viewport={'width': width, 'height': 1000})
                errors, api_statuses, provider_calls = [], [], []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.on('response', lambda response: api_statuses.append(response.status)
                        if response.url.endswith('/finmind/api/status') else None)
                page.on('request', lambda request: provider_calls.append(request.url)
                        if 'finmindtrade.com' in request.url else None)
                with page.expect_response(lambda response: response.url.endswith('/finmind/api/status'), timeout=20000) as response:
                    page.goto(args.base_url.rstrip('/') + '/finmind/', wait_until='domcontentloaded', timeout=20000)
                payload = response.value.json()
                info = payload['acquisition']
                page.wait_for_function("document.querySelector('#download-progress-detail').textContent.includes('未建歷史搜尋候選')", timeout=15000)
                assert payload['read_only'] and not payload['production_control_possible']
                ordered = info.get('history_order', {}).get('applied') is True
                if ordered:
                    assert info['history_order'] == {**metadata(), 'applied': True}
                    assert [row['id'] for row in payload['datasets'] if row.get('history_rank')] == [stage.dataset for stage in HISTORY_STAGES]
                assert info['materialized_tasks'] + info['unseeded_candidate_tasks'] == info['total_tasks']
                assert info['materialized_pending_tasks'] + info['unseeded_candidate_tasks'] == info['pending_tasks']
                if payload['health'] == 'updating':
                    assert info['state'] == 'running'
                expected = f"{info['checked_tasks']:,}／{info['total_tasks']:,}"
                assert page.locator('#download-count').text_content() == expected
                assert '整體' in page.locator('#capture-state').text_content()
                page.evaluate("window.StockAgentAcquisition?.reveal('pipelines')")
                page.locator('[data-pipeline-filter="session_history"]').click()
                assert page.locator('#pipeline-grid .pipeline-card').count() == sum(
                    row['kind'] == 'session_history' for row in payload['datasets'])
                page.locator('[data-pipeline-filter="all"]').click()
                assert page.locator('#pipeline-grid .pipeline-card').count() == len(payload['datasets'])
                page.evaluate("window.StockAgentAcquisition?.reveal('download-global-eta')")
                page.locator('#download-global-eta').scroll_into_view_if_needed()
                page.evaluate('new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
                if ordered:
                    assert page.locator('#download-eta-milestones tr').count() == len(STAGES)
                    assert page.locator('.estimate-stage-progress').count() == len(HISTORY_STAGES)
                    assert page.locator('#download-eta-milestones tr[data-history-rank]').evaluate_all(
                        '(els) => els.map(el => el.dataset.sourceDataset)') == [stage.dataset for stage in HISTORY_STAGES]
                    assert '期貨價差 tick → 美股分鐘 K' in page.locator('#history-sequence').text_content()
                    assert '下載器已套用' in page.locator('#history-sequence').text_content()
                    bars = page.locator('.estimate-stage-progress').evaluate_all(
                        '(els) => els.map(el => el.hasAttribute("value") ? el.value : null)')
                    ranked = [row for row in payload['datasets'] if row.get('history_rank')]
                    for value, dataset in zip(bars, ranked):
                        expected_ratio = dataset['checked_partitions'] / dataset['target_partitions'] if dataset['target_partitions'] else None
                        assert value is None if expected_ratio is None else abs(value - expected_ratio) < 1e-9
                estimate = info['completion_estimate']
                now = datetime.now(UTC)
                verified_scenarios = []
                # A successful fetch and 12 rows do not prove forecasts were
                # rendered. Regress the version/capability integration itself.
                for key, scenario in estimate['scenarios'].items():
                    modeled = estimate.get('retry_condition', {}).get('scenarios', {}).get(key, {})
                    selected = modeled if scenario['state'] != 'estimated' and modeled.get('state') == 'estimated' else scenario
                    if selected.get('state') != 'estimated' or not selected.get('estimated_complete_at_utc'):
                        continue
                    if datetime.fromisoformat(selected['estimated_complete_at_utc']) <= now:
                        continue
                    if estimate['state'] in ('stale', 'unavailable'):
                        continue
                    value = page.locator(f'#download-eta-{key}').text_content()
                    assert '約 ' in value and '未知' not in value, (key, estimate['schema_version'], value)
                    assert '預計 ' in page.locator(f'#download-eta-{key}-complete').text_content()
                    verified_scenarios.append(key)
                own_work_verified = 0
                schedule_labels = page.locator('.estimate-stage-schedule').all_text_contents()
                if ordered:
                    assert len(schedule_labels) == len(HISTORY_STAGES)
                    for index, stage in enumerate(estimate['stages']):
                        central = stage['scenarios']['central']
                        cell = page.locator('#download-eta-milestones tr').nth(index).locator('td').nth(2)
                        current = stage['workload']['planned_requests'] == 0 and not any(
                            stage['workload'].get(key) for key in ('inflight_tasks', 'local_derived_tasks', 'blocked_tasks',
                            'unknown_datasets', 'unscheduled_datasets', 'retry_tasks', 'retry_exhausted_tasks'))
                        if stage.get('dataset') and current and stage['state'] in ('current', 'estimated', 'conditional'):
                            assert '目前候選已查驗' in cell.text_content()
                            assert '已超過估計' not in cell.text_content()
                        if (central.get('standalone_active_work_seconds', 0) or 0) > 0 and central['state'] == 'unknown':
                            assert '本階段工時' in cell.text_content(), (stage['key'], cell.text_content())
                            assert '完成日期尚無法估算' in cell.text_content()
                            own_work_verified += 1
                condition = estimate.get('retry_condition')
                conditional_verified = False
                if estimate['state'] == 'waiting_retry' and condition:
                    assert condition['is_guaranteed'] is False and condition['retry_tasks'] > 0
                    assert '尚未完成：等待成功重試' in page.locator('#download-global-eta').text_content()
                    for key, scenario in condition['scenarios'].items():
                        if scenario['state'] != 'estimated':
                            continue
                        assert page.locator(f'#download-eta-{key}').text_content().startswith('條件試算')
                        assert '若下次重試成功' in page.locator(f'#download-eta-{key}-complete').text_content()
                        assert '不是可靠倒數' in page.locator(f'#download-eta-{key}-detail').text_content()
                    for index, stage in enumerate(estimate['stages']):
                        modeled = stage.get('retry_condition', {}).get('scenarios', {}).get('central', {})
                        if modeled.get('state') != 'estimated' or stage['scenarios']['central']['state'] == 'estimated':
                            continue
                        cell = page.locator('#download-eta-milestones tr').nth(index).locator('td').nth(2)
                        no_work = stage['workload']['planned_requests'] == 0 and (stage.get('dataset') or modeled.get('forecast_arrival_requests') == 0)
                        no_work = no_work and not any(stage['workload'].get(key) for key in (
                            'inflight_tasks', 'local_derived_tasks', 'blocked_tasks', 'unknown_datasets',
                            'unscheduled_datasets', 'retry_tasks', 'retry_exhausted_tasks'))
                        if no_work and stage['state'] in ('current', 'estimated', 'conditional'):
                            assert '目前無已知待發請求' in cell.text_content()
                            continue
                        assert '若下次重試成功' in cell.text_content()
                        if modeled.get('stage_start_at_utc'):
                            assert '階段開始' in cell.text_content()
                    conditional_verified = True
                exhausted = info.get('retry_exhausted_tasks', 0)
                if exhausted:
                    assert f'重試耗盡 {exhausted:,} 個' in page.locator('#download-pending').text_content()
                    assert f'重試耗盡 {exhausted:,} 個' in page.locator('#download-eta-exclusions').text_content()
                    for row in payload['datasets']:
                        count = row.get('retry_exhausted_partitions', 0)
                        if not count:
                            continue
                        card = page.locator('#pipeline-grid .pipeline-card').filter(
                            has=page.get_by_text(row['id'], exact=True)).first
                        assert f'重試耗盡 {count:,}' in card.locator('.pipeline-detail').text_content()
                        assert '重試耗盡' in card.locator('.pipeline-status').text_content()
                overflow = page.evaluate('Math.max(0,document.documentElement.scrollWidth-innerWidth)')
                assert overflow == 0
                assert not errors and api_statuses and all(value == 200 for value in api_statuses)
                assert not provider_calls
                page.screenshot(path=str(output / f'finmind-stages-{width}.png'))
                page.locator('.estimate-stage-table').screenshot(
                    path=str(output / f'finmind-history-sequence-{width}.png'))
                if ordered and width == 390:
                    page.locator('#download-eta-milestones tr[data-history-rank="9"]').screenshot(
                        path=str(output / 'finmind-last-history-mobile.png'))
                profiles.append({'width': width, 'horizontal_overflow': overflow, 'javascript_errors': errors,
                                 'api_statuses': api_statuses, 'browser_provider_calls': len(provider_calls),
                                 'health': payload['health'], 'state': info['state'], 'free_state': info['free_state'],
                                 'quota_allocation': payload['quota']['backfill_allocation'],
                                 'generated_at_utc': payload['generated_at_utc'],
                                 'checked_tasks': info['checked_tasks'], 'materialized_tasks': info['materialized_tasks'],
                                 'unseeded_candidate_tasks': info['unseeded_candidate_tasks'],
                                 'eta_state': info['completion_estimate']['state'],
                                 'numeric_scenarios_verified': verified_scenarios,
                                 'standalone_stage_work_verified': own_work_verified,
                                 'history_schedule_labels': schedule_labels,
                                 'retry_exhausted_tasks': exhausted, 'retained_rows': info.get('retained_rows'),
                                 'retry_condition_verified': conditional_verified,
                                 'central_estimate_text': page.locator('#download-eta-central').text_content(),
                                 'central_complete_text': page.locator('#download-eta-central-complete').text_content(),
                                 'stage_rows': page.locator('#download-eta-milestones tr').count(),
                                 'history_progress_bars': page.locator('.estimate-stage-progress').count(),
                                 'history_sequence_applied': ordered,
                                 'history_sequence': page.locator('#history-sequence').text_content(),
                                 'progress_text': page.locator('#download-progress-label').text_content(),
                                 'progress_detail': page.locator('#download-progress-detail').text_content(),
                                 'capture_text': page.locator('#capture-state').text_content(),
                                 'latest_result': info['latest_result'], 'filter_verified': True})
                page.close()
        finally:
            browser.close()
    receipt = {'schema_version': 1, 'observed_at_utc': datetime.now(UTC).isoformat(), 'base_url': args.base_url,
               'passed': True, 'profiles': profiles, 'all_history_complete_claim': False}
    atomic_write_json(output / 'browser_acceptance.json', receipt)
    print(f"FinMind deployed browser acceptance: {len(profiles)} viewports passed; output={output}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
