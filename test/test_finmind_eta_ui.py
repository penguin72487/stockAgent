"""FinMind forecasts must be rendered as bounded-scope scenarios, not promises."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-09-27T04:00:00+00:00"


def _view(payload: dict | None, *, now: str = NOW) -> dict:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node runtime not installed")
    script = r"""
const fs = require('fs'), vm = require('vm');
const source = fs.readFileSync(process.argv[1], 'utf8').split('function svgNode')[0];
const context = {window: {StockAgentDashboard: {createJsonFetcher: () => () => {}, byId: () => {}}}};
vm.createContext(context);
vm.runInContext(source + '\nglobalThis.forecast = completionEstimateView;', context);
process.stdout.write(JSON.stringify(context.forecast(JSON.parse(process.argv[2]), Date.parse(process.argv[3]))));
"""
    result = subprocess.run(
        [node, "-e", script, str(ROOT / "services/finmind_dashboard/app.js"), json.dumps(payload), now],
        check=True, capture_output=True, text=True, timeout=10,
    )
    return json.loads(result.stdout)


def _estimate() -> dict:
    return {
        "state": "conditional",
        "observed_at_utc": NOW,
        "valid_until_utc": "2026-09-27T04:05:00+00:00",
        "scope_label": "FinMind 已排程可執行工作（含次要校驗）",
        "scenarios": {
            "fastest": {
                "state": "estimated", "remaining_seconds": 3600,
                "estimated_complete_at_utc": "2026-09-27T05:00:00+00:00",
                "request_count": 500, "effective_requests_per_hour": 500, "basis": "可用額度樂觀情境",
            },
            "central": {
                "state": "estimated", "remaining_seconds": 86400,
                "estimated_complete_at_utc": "2026-09-28T04:00:00+00:00",
                "request_count": 600, "effective_requests_per_hour": 25, "basis": "近期實測速度情境",
            },
            "slowest": {
                "state": "estimated", "remaining_seconds": 172800,
                "estimated_complete_at_utc": "2026-09-29T04:00:00+00:00",
                "request_count": 700, "effective_requests_per_hour": 14.6, "basis": "保守速度情境",
            },
        },
        "workload": {"required_requests": 480, "validation_requests": 20, "planned_requests": 500,
                     "blocked_tasks": 7, "unscheduled_datasets": 17, "unknown_datasets": 2},
        "basis": "本機工作清單与速度觀測",
        "assumptions": ["背景工作持續執行", "保留固定增量配額"],
        "blockers": [{"code": "not_entitled", "count": 7, "reason": "來源權限未滿足"}],
    }


def test_three_scenarios_preserve_backend_numbers_and_taipei_completion_dates() -> None:
    view = _view(_estimate())
    assert view["stateLabel"] == "條件式三情境估算"
    assert [row["key"] for row in view["scenarios"]] == ["fastest", "central", "slowest"]
    assert [row["value"] for row in view["scenarios"]] == ["約 1 小時", "約 24 小時", "約 2 天"]
    assert "2026/09/27 13:00" in " ".join(view["scenarios"][0]["complete"].split())
    assert "（台北）" in view["scenarios"][0]["complete"]
    assert "500 次請求" in view["scenarios"][0]["detail"]
    assert "阻塞 7 個任務" in view["exclusions"]
    assert "未排程 17 類" in view["exclusions"]
    assert "佇列未能盤點 2 類" in view["exclusions"]


def test_retry_condition_is_labeled_and_never_claims_success_or_reliable_countdown():
    payload = _estimate()
    modeled = payload['scenarios']
    payload.update(schema_version=7, state='waiting_retry',
                   scenarios={key: {'state': 'waiting_retry', 'remaining_seconds': None} for key in modeled},
                   retry_condition={'basis': 'next_retry_succeeds_no_additional_failures',
                                    'retry_tasks': 131, 'scenarios': modeled})
    payload['workload']['retry_tasks'] = 131
    view = _view(payload)
    assert view['stateLabel'] == '尚未完成：等待成功重試'
    assert all(row['value'].startswith('條件試算') for row in view['scenarios'])
    assert all(row['conditional'] for row in view['scenarios'])
    assert all('若下次重試成功' in row['complete'] for row in view['scenarios'])
    assert all('不是可靠倒數' in row['detail'] for row in view['scenarios'])
    stale = _view(payload, now='2026-09-27T04:06:00+00:00')
    assert all(row['value'] == '觀測已過期' for row in stale['scenarios'])
    assert not any(row['conditional'] for row in stale['scenarios'])
    assert "未清點的歷史代號與未來新增工作另計" in view["exclusions"]
    assert view["blockers"][0]["reason"] == "來源權限未滿足"
    assert "共 500 次" in view["workload"]


@pytest.mark.parametrize("state", ["warming_up", "unavailable", "stale", "unrecognized"])
def test_unavailable_estimate_never_renders_existing_numeric_scenarios(state: str) -> None:
    payload = _estimate()
    payload["state"] = state
    view = _view(payload)
    assert all(row["value"] in {"未知", "觀測已過期"} for row in view["scenarios"])
    assert all(row["complete"] == "完成日期尚無法估算" for row in view["scenarios"])
    assert "7 個任務" in view["exclusions"]


def test_expired_snapshot_is_not_presented_as_a_current_countdown() -> None:
    view = _view(_estimate(), now="2026-09-27T04:05:00+00:00")
    assert view["stateLabel"] == "估算觀測已過期"
    assert all(row["value"] == "觀測已過期" for row in view["scenarios"])
    assert "2026/09/27 12:00" in " ".join(view["observed"].split())


@pytest.mark.parametrize('version', [6, 9, 10, 11, 12])
def test_nine_stage_contract_preserves_valid_scenarios_when_one_rate_is_unknown(version):
    payload = _estimate()
    payload.update(schema_version=version, state='warming_up')
    payload['scenarios']['slowest'].update(state='unknown', remaining_seconds=None,
                                          estimated_complete_at_utc=None)
    view = _view(payload)
    assert view['scenarios'][0]['value'] == '約 1 小時'
    assert view['scenarios'][1]['value'] == '約 24 小時'
    assert view['scenarios'][2]['value'] == '未知'


def test_projection_capability_does_not_require_a_frontend_version_bump():
    payload = _estimate()
    payload.update(schema_version=99, scenario_projection_contract=1, state='warming_up')
    assert _view(payload)['scenarios'][0]['value'] == '約 1 小時'
    payload['scenario_projection_contract'] = None
    assert _view(payload)['scenarios'][0]['value'] == '未知'


def test_own_stage_work_is_not_an_absolute_deadline_or_global_countdown():
    payload = _estimate()
    payload.update(schema_version=12, state='warming_up')
    for scenario in payload['scenarios'].values():
        scenario.update(state='unknown', remaining_seconds=None, estimated_complete_at_utc=None,
                        active_work_seconds=None, standalone_active_work_seconds=3600)
    view = _view(payload)
    assert all(row['value'] == '本階段工時 約 1 小時' for row in view['scenarios'])
    assert all(row['complete'] == '完成日期尚無法估算' for row in view['scenarios'])
    assert all('前置等待' in row['detail'] for row in view['scenarios'])


def test_actual_history_schedule_is_independent_of_eta_and_rejects_stale_task():
    node = shutil.which('node')
    if node is None:
        pytest.skip('Node runtime not installed')
    script = r"""
const fs = require('fs'), vm = require('vm');
const source = fs.readFileSync(process.argv[1], 'utf8').split('function svgNode')[0];
const context = {window: {StockAgentDashboard: {createJsonFetcher: () => () => {}, byId: () => {}}}};
vm.createContext(context);
vm.runInContext(source + '\nglobalThis.schedule = historyScheduleLabel;', context);
const now = Date.parse('2026-09-27T04:00:00+00:00');
const item = {dataset:'TaiwanFuturesKBar', state:'warming_up', workload:{planned_requests:100}};
const info = {complement_observed_at_utc:'2026-09-27T04:00:00+00:00', complement_state:'running',
  complement_active_task:{dataset:item.dataset}};
process.stdout.write(JSON.stringify([
 context.schedule(item, {acquisition:info}, now),
 context.schedule(item, {acquisition:info}, now+180001),
 context.schedule(item, {acquisition:{...info, complement_state:'waiting_retry'}}, now),
 context.schedule(item, {acquisition:{...info, complement_active_task:null, complement_next_task:{dataset:item.dataset}}}, now)
]));
"""
    result = subprocess.run([node, '-e', script, str(ROOT / 'services/finmind_dashboard/app.js')],
                            check=True, capture_output=True, text=True, timeout=10)
    assert json.loads(result.stdout) == ['正在下載（工作取樣）', '實際排程觀測待更新',
                                       '接續待抓；依歷史順序／追新插隊', '下一筆已排程（尚非完成）']


def test_candidate_requests_are_separate_from_materialized_work():
    payload = _estimate()
    payload['workload']['candidate_requests'] = 300
    assert '歷史搜尋候選 300 次（尚未建入佇列，非缺漏筆數）' in _view(payload)['workload']


def test_exhausted_only_stage_is_a_gap_not_an_instant_completion():
    payload = _estimate()
    payload['workload'].update(planned_requests=0, retry_tasks=0, retry_exhausted_tasks=131)
    for scenario in payload['scenarios'].values():
        scenario.update(remaining_seconds=0, estimated_complete_at_utc=NOW, request_count=0)
    view = _view(payload)
    assert '重試耗盡 131 個' in view['exclusions']
    assert all(row['value'] == '重試耗盡，待人工修復' for row in view['scenarios'])
    assert all(row['complete'] == '完成日期尚無法估算' for row in view['scenarios'])


def test_zero_work_stage_never_claims_current_when_expired_or_unscheduled():
    node = shutil.which('node')
    if node is None:
        pytest.skip('Node runtime not installed')
    script = r"""
const fs = require('fs'), vm = require('vm');
const source = fs.readFileSync(process.argv[1], 'utf8').split('function svgNode')[0];
const context = {window: {StockAgentDashboard: {createJsonFetcher: () => () => {}, byId: () => {}}}};
vm.createContext(context);
vm.runInContext(source + '\nglobalThis.clearStage = stageHasNoPendingWork;', context);
const item = {state:'current', valid_until_utc:'2026-09-27T04:05:00+00:00', workload:{planned_requests:0}};
const raw = {forecast_arrival_requests:0}, now = Date.parse('2026-09-27T04:00:00+00:00');
const outputs = [context.clearStage(item, raw, now),
  context.clearStage(item, raw, now + 300000),
  context.clearStage({...item, state:'stale'}, raw, now),
  context.clearStage({...item, valid_until_utc:null}, raw, now),
  ...['retry_tasks','blocked_tasks','unknown_datasets','unscheduled_datasets','inflight_tasks']
    .map(key => context.clearStage({...item, workload:{planned_requests:0, [key]:1}}, raw, now)),
  context.clearStage({...item, dataset:'TaiwanStockKBar'}, {forecast_arrival_requests:1}, now)];
process.stdout.write(JSON.stringify(outputs));
"""
    result = subprocess.run([node, '-e', script, str(ROOT / 'services/finmind_dashboard/app.js')],
                            check=True, capture_output=True, text=True, timeout=10)
    assert json.loads(result.stdout) == [True] + [False] * 8 + [True]


def test_deadline_passes_before_snapshot_expiry_and_stops_one_minute_display():
    payload = _estimate()
    payload['scenarios']['fastest'].update(remaining_seconds=60,
        estimated_complete_at_utc='2026-09-27T04:01:00+00:00')
    view = _view(payload, now='2026-09-27T04:02:00+00:00')
    assert view['scenarios'][0]['value'] == '已超過估計，尚未完成'
    assert view['scenarios'][0]['complete'] == '完成日期尚無法估算'
    assert '估計期限不是成功收據' in view['scenarios'][0]['detail']


def test_retry_does_not_render_short_active_service_time_as_completion_countdown():
    payload = _estimate()
    payload['state'] = 'waiting_retry'
    payload['workload']['retry_tasks'] = 159
    for row in payload['scenarios'].values():
        row['active_work_seconds'] = 60
    view = _view(payload)
    assert view['stateLabel'] == '尚未完成：等待成功重試'
    assert all(row['value'] == '待重試，完成時間未定' for row in view['scenarios'])
    assert all(row['complete'] == '完成日期尚無法估算' for row in view['scenarios'])
    assert all('159 個' in row['detail'] and '不是倒數' in row['detail'] for row in view['scenarios'])
    assert '不是成功完成數' in view['rateBridge']


def test_estimate_does_not_decrease_without_a_new_measurement() -> None:
    first = _view(_estimate())
    later = _view(_estimate(), now="2026-09-27T04:04:00+00:00")
    assert first == later


def test_rate_bridge_distinguishes_rolling_starts_future_load_and_weighted_total():
    payload = _estimate()
    payload['rate_evidence'] = dict(rolling_complete_window=True, rolling_requests_60m=5307,
                                    gross_requests_per_hour=5307, future_recurring_requests_per_hour=1400,
                                    effective_requests_per_hour=3907, stage_label='指定優先回補',
                                    overall_rate_basis='stage_weighted', rolling_window_end_at_utc=NOW)
    view = _view(payload)
    assert '5,307' in view['rateBridge']
    assert '1,400' in view['rateBridge']
    assert '3,907' in view['rateBridge']
    assert '指定優先回補' in view['rateBridge']
    assert '全程加權' in view['scenarios'][0]['detail']
    assert '每分鐘快照' in view['rateWindow']
    expired = _view(payload, now='2026-09-27T04:06:00+00:00')
    assert '5,307' not in expired['rateBridge']


def test_release_clock_bridge_does_not_describe_daily_average_as_hourly_reserve():
    payload = _estimate()
    payload['rate_evidence'] = dict(rolling_complete_window=True, rolling_requests_60m=5307,
        gross_requests_per_hour=5307, future_recurring_requests_per_hour=0,
        effective_requests_per_hour=5100, current_reserved_requests=19,
        scheduling_basis='release_clock_events', stage_label='指定優先回補')
    bridge = _view(payload)['rateBridge']
    assert '此刻實際保留 19 次' in bridge
    assert '未到期不扣容量' in bridge
    assert '扣除未來追新模型' not in bridge


@pytest.mark.parametrize('state', ['waiting_admission', 'waiting_quota'])
def test_waiting_keeps_active_work_duration_but_never_a_calendar_finish(state):
    payload = _estimate()
    payload['state'] = state
    for row in payload['scenarios'].values():
        row['active_work_seconds'] = 3600
    view = _view(payload)
    assert all(row['value'] == '放行後 約 1 小時' for row in view['scenarios'])
    assert all(row['complete'] == '完成日期尚無法估算' for row in view['scenarios'])
    assert all('未知等待時間' in row['detail'] for row in view['scenarios'])


def test_expired_waiting_does_not_leave_active_work_numbers_live():
    payload = _estimate()
    payload['state'] = 'waiting_admission'
    payload['scenarios']['fastest']['active_work_seconds'] = 3600
    view = _view(payload, now='2026-09-27T04:06:00+00:00')
    assert all(row['value'] == '觀測已過期' for row in view['scenarios'])


def test_long_horizon_keeps_duration_without_inventing_a_completion_day():
    payload = _estimate()
    payload['scenarios']['slowest'].update(state='unknown', remaining_seconds=None,
                                          estimated_complete_at_utc=None, active_work_seconds=400000000)
    view = _view(payload)
    assert view['scenarios'][0]['value'] == '約 1 小時'
    assert view['scenarios'][2]['value'].startswith('有效工時')
    assert view['scenarios'][2]['complete'] == '完成日期尚無法估算'


def test_failed_refresh_rechecks_expiry_and_clears_old_numeric_estimates() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node runtime not installed")
    script = r"""
const fs = require('fs'), vm = require('vm');
const source = fs.readFileSync(process.argv[1], 'utf8').split('document.querySelectorAll("[data-pipeline-filter]")')[0];
const elements = new Map();
const element = () => ({textContent: '', className: '', lastChild: {textContent: ''}, replaceChildren() {}, append() {}});
const context = {
  window: {StockAgentDashboard: {
    createJsonFetcher: () => async () => { throw new Error('test-only network failure'); },
    byId: id => { if (!elements.has(id)) elements.set(id, element()); return elements.get(id); }
  }, setTimeout: () => 1, clearTimeout: () => {}},
  document: {hidden: false, createElement: element}, console: {warn() {}}, payload: JSON.parse(process.argv[2])
};
vm.createContext(context);
vm.runInContext(source + `
Date.now = () => Date.parse('2026-09-27T04:00:00+00:00');
state.latest = {generated_at_utc: payload.observed_at_utc, acquisition: {completion_estimate: payload}};
renderCompletionEstimate(payload);
globalThis.before = $('download-eta-fastest').textContent;
Date.now = () => Date.parse('2026-09-27T04:06:00+00:00');
globalThis.failedRefresh = refresh;
`, context);
(async () => {
  await context.failedRefresh();
  process.stdout.write(JSON.stringify({
    before: context.before, after: elements.get('download-eta-fastest').textContent,
    complete: elements.get('download-eta-fastest-complete').textContent,
    globalState: elements.get('download-global-eta').textContent,
    health: elements.get('finmind-health').className
  }));
})().catch(error => { process.stderr.write(String(error)); process.exitCode = 1; });
"""
    result = subprocess.run(
        [node, "-e", script, str(ROOT / "services/finmind_dashboard/app.js"), json.dumps(_estimate())],
        capture_output=True, text=True, check=True, timeout=10,
    )
    values = json.loads(result.stdout)
    assert values["before"] == "約 1 小時"
    assert values["after"] == "觀測已過期"
    assert values["complete"] == "完成日期尚無法估算"
    assert values["globalState"] == "估算觀測已過期"
    assert values["health"] == "status stale"


@pytest.mark.parametrize("payload", [None, {}, {"minimum_network_seconds_remaining": 60}])
def test_missing_forecast_does_not_reinterpret_legacy_projection(payload: dict | None) -> None:
    view = _view(payload)
    assert view["stateLabel"] == "尚無全域估算"
    assert all(row["value"] == "未知" for row in view["scenarios"])
    assert "阻塞 — 個任務" in view["exclusions"]


@pytest.mark.parametrize("value", [None, -1, "not numeric"])
def test_invalid_duration_is_unknown_not_zero(value: object) -> None:
    payload = _estimate()
    payload["scenarios"]["central"]["remaining_seconds"] = value
    view = _view(payload)
    assert view["scenarios"][1]["value"] == "未知"
    assert view["scenarios"][0]["value"] == "約 1 小時"


def test_zero_backlog_is_not_claimed_as_all_sources_complete() -> None:
    payload = _estimate()
    payload["state"] = "current"
    payload["scenarios"]["fastest"]["remaining_seconds"] = 0
    view = _view(payload)
    assert view["stateLabel"] == "本輪可執行工作已查驗"
    assert view["scenarios"][0]["value"] == "本輪可執行工作已查驗"
    assert "這些不算完成" in view["exclusions"]


def test_scenario_card_and_important_caveats_are_visible_in_markup() -> None:
    html = (ROOT / "services/finmind_dashboard/index.html").read_text(encoding="utf-8")
    for key in ("fastest", "central", "slowest"):
        assert f'id="download-eta-{key}"' in html
        assert f'id="download-eta-{key}-complete"' in html
    assert "最慢」不是硬上界" in html
    assert "不是保證完成期限或統計信賴區間" in html
    assert 'id="download-eta-exclusions" class="estimate-exclusions"' in html
    assert 'styles.css?v=3' in html
    assert 'app.js?v=22' in html
    assert 'href="styles.css?v=10"' in html
    assert '流量與估時對帳' in html
    assert '階段／已知剩餘請求' in html


@pytest.mark.parametrize("width", [390, 1440])
def test_three_estimates_render_without_overflow_in_real_browser(width: int) -> None:
    """Use local static assets plus a synthetic, test-only DTO; no provider API."""
    playwright = pytest.importorskip("playwright.sync_api")
    from scripts.probe_browser_runtime import browser_profile_launch_options

    estimate = _estimate()
    estimate["valid_until_utc"] = "2099-01-01T00:00:00+00:00"
    # Exercise the actual new stage table, not an empty container that can hide
    # mobile overflow or a renderer regression.
    from test_finmind_eta_stages import run
    staged = run()
    estimate['stages'] = staged['stages']
    estimate['rate_evidence'] = staged['rate_evidence']
    for item in estimate['stages']:
        item['valid_until_utc'] = '2099-01-01T00:00:00+00:00'
    from downloader.finmind_history_order import HISTORY_STAGES, metadata
    payload = {
        "read_only": True, "production_control_possible": False, "health": "waiting",
        "generated_at_utc": NOW,
        "datasets": [dict(id=stage.dataset, label=stage.label, kind='symbol_history', state='backfilling',
                          history_rank=rank, checked_partitions=50, complete_partitions=40,
                          target_partitions=100, unseeded_partition_candidates=25, rows=400,
                          local_bytes=1000, first_data_date='2021-06-30', last_data_date='2026-09-25')
                     for rank, stage in enumerate(HISTORY_STAGES, 1)],
        "acquisition": {"completion_estimate": estimate, 'history_order': {**metadata(), 'applied': True}},
    }
    asset_roots = {
        "finmind": ROOT / "services/finmind_dashboard",
        "shioaji": ROOT / "services/shioaji_api_dashboard",
        "finlab": ROOT / "services/finlab_dashboard",
    }

    def respond(route) -> None:
        from urllib.parse import urlparse
        request_path = urlparse(route.request.url).path.strip("/")
        if request_path == "finmind/api/status":
            route.fulfill(json=payload)
            return
        parts = request_path.split("/")
        if len(parts) == 1 and parts[0] in asset_roots:
            asset = asset_roots[parts[0]] / "index.html"
        elif len(parts) == 2 and parts[0] in asset_roots:
            asset = asset_roots[parts[0]] / parts[1]
        else:
            asset = ROOT / "services/public_dashboards" / request_path
        if asset.is_file():
            route.fulfill(path=asset)
        else:
            route.fulfill(status=404, body="Not found")

    with playwright.sync_playwright() as runtime:
        if not Path(runtime.chromium.executable_path).is_file():
            pytest.skip("Chromium is not installed")
        browser = runtime.chromium.launch(**browser_profile_launch_options("cpu-2d"), timeout=5000)
        try:
            page = browser.new_page(viewport={"width": width, "height": 1000})
            # The fixture is intentionally dated; test against its own clock,
            # not a later wall clock that should correctly mark it overdue.
            from datetime import datetime
            page.clock.set_fixed_time(datetime.fromisoformat(NOW))
            errors: list[str] = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.route("**/*", respond)
            page.goto("http://finmind-eta.test/finmind/", wait_until="networkidle", timeout=10000)
            page.evaluate("window.StockAgentAcquisition.reveal('download-global-eta')")
            page.locator("#download-global-eta").scroll_into_view_if_needed(timeout=5000)
            page.evaluate("new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))")
            playwright.expect(page.locator("#download-global-eta")).to_have_text("條件式三情境估算")
            playwright.expect(page.locator("#download-eta-central")).to_have_text("約 24 小時")
            playwright.expect(page.locator("#download-eta-fastest-complete")).to_contain_text("2026/09/27")
            playwright.expect(page.locator("#download-eta-exclusions")).to_contain_text("阻塞 7 個任務")
            playwright.expect(page.locator("#download-eta-blockers")).to_contain_text("來源權限未滿足")
            assert page.locator(".completion-estimate").count() == 3
            assert page.locator('#download-eta-milestones tr').count() == 12
            assert page.locator('.estimate-stage-progress').count() == 9
            assert page.locator('.estimate-stage-progress').evaluate_all('(els) => els.every(el => el.value === 0.5)')
            assert page.locator('#download-eta-milestones tr[data-history-rank]').evaluate_all(
                '(els) => els.map(el => el.dataset.sourceDataset)') == [stage.dataset for stage in HISTORY_STAGES]
            playwright.expect(page.locator('#history-sequence')).to_contain_text('期貨價差 tick → 美股分鐘 K')
            playwright.expect(page.locator('#history-sequence')).to_contain_text('下載器已套用')
            playwright.expect(page.locator('#download-eta-rate-bridge')).to_contain_text('5,600')
            playwright.expect(page.locator('#download-eta-milestones')).to_contain_text('本階段')
            playwright.expect(page.locator('#download-eta-milestones')).to_contain_text('新增日分區模型')
            assert page.locator('#backfill .progress-card').evaluate('(el) => getComputedStyle(el).contentVisibility') == 'visible'
            assert page.evaluate("document.documentElement.scrollWidth-document.documentElement.clientWidth") <= 1
            page.get_by_text("估算依據與條件", exact=True).click(timeout=5000)
            assert page.locator(".estimate-method").get_attribute("open") is not None
            playwright.expect(page.locator("#download-eta-assumptions")).to_contain_text("保留固定增量配額")
            assert not errors
        finally:
            browser.close()
