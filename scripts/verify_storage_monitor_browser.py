#!/usr/bin/env python3
"""Exercise the member storage explorer in real Chromium without GPU work."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from playwright.sync_api import sync_playwright
from scripts.probe_browser_runtime import browser_profile_launch_options
from stockagent.data_sync.storage_monitor import public_status
from stockagent.data_sync.storage_progress import CONTRACT as OPERATIONS_CONTRACT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:8772")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    cases = []
    with urlopen(args.base + "/data-monitor/api/storage", timeout=20) as response:
        actual = json.load(response)
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options("cpu-2d"))
        try:
            for width in (1440, 390):
                page = browser.new_page(viewport={"width": width, "height": 960})
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                # Pin the actual API observation while checking responsive
                # layouts. A later minute must not change the DOM halfway
                # through comparisons with this snapshot's capacity clocks.
                # The real polling timer is exercised separately below.
                page.route('**/data-monitor/api/storage', lambda route: route.fulfill(json=actual))
                with page.expect_response(lambda response: response.url.endswith('/data-monitor/api/storage')) as response:
                    page.goto(args.base + "/data-monitor/#storage-inventory", wait_until="domcontentloaded")
                native = response.value.json()
                expected_volume_ids = [volume['id'] for node in native['nodes'] for volume in node['volumes']]
                assert {'penguin-wsl', 'penguin-e', 'penguin-workspace', 'nas-disk'} <= set(expected_volume_ids)
                page.wait_for_function("expected => document.querySelectorAll('.storage-disk').length === expected",
                                       arg=len(expected_volume_ids), timeout=30000)
                assert set(page.locator('.storage-disk').evaluate_all(
                    "rows => rows.map(row => row.dataset.volume)")) == set(expected_volume_ids)
                page.locator("#storage-inventory").scroll_into_view_if_needed()
                assert page.locator("#storage-error").is_hidden()
                assert page.locator(".storage-node").count() == 4
                assert native.get("operations", {}).get("contract") == OPERATIONS_CONTRACT
                assert native.get("runtime", {}).get("contract") == "stockagent.storage_runtime.v1"
                assert page.locator("[data-runtime-node]").count() == 3
                assert page.locator('[data-runtime-node="lab203"]').count() == 1
                assert "三端運行狀況" in page.locator("#storage-inventory").inner_text()
                assert page.locator(".storage-operation-card").count() > 0
                assert page.locator('#storage-operation-tasks > .storage-operation-card').count() <= 6
                more = page.locator('#storage-operations-more')
                if more.is_visible():
                    assert more.get_attribute('open') is None
                    page.locator('#storage-operations-more > summary').click()
                    cases.append({'case': 'native_additional_work_expands_from_summary', 'width': width})
                receiver_rows = [row for row in native['operations']['tasks']
                                 if row.get('kind') == 'receiver'
                                 and row.get('reason_code') == 'receiver_readiness_lock_diagnosis']
                for row in receiver_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1
                    assert '最近一次診斷曾遇到 Restic 排他鎖' in card.inner_text()
                    assert row.get('diagnosis_at_utc') and '最近一次鎖診斷' in card.inner_text()
                    assert row['diagnosis_at_utc'] != row['observed_at_utc']
                    assert row['complete_evidence'] is False
                if receiver_rows:
                    cases.append({'case': 'native_lab203_owner_and_historical_lock_clocks',
                                  'width': width, 'diagnostic_is_not_nas_acceptance': True})
                assert len(native['operations']['schedules']) >= 20
                assert len(native['operations']['flow']) == 6
                assert page.locator('#storage-operation-attention').is_visible()
                cases.append({"case": "native_storage_cards", "width": width,
                              "volumes": len(expected_volume_ids), "nodes": 4})
                cases.append({"case": "native_runtime_cards", "width": width, "nodes": 3})
                spool_rows = [row for row in native['operations']['tasks']
                              if row.get('kind') == 'vast_spool']
                for row in spool_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1
                    assert row['label'] in card.inner_text()
                    assert row['complete_evidence'] is False
                    assert 'NAS' in row['scope']
                if spool_rows:
                    cases.append({'case': 'native_speculative_vast_compute',
                                  'width': width, 'cards': len(spool_rows),
                                  'source_retirement_authorized': False})
                    for row in spool_rows:
                        if row.get('phase') == 'waiting_new_sealed_packs':
                            assert row['state'] == 'waiting' and row['eta']['state'] != 'finalizing'
                            text = page.locator(f'[data-operation="{row["id"]}"]').inner_text()
                            assert '等候新封包' in text and '等待正式驗收' not in text
                        assert 'NAS 管線目前' not in page.locator(f'[data-operation="{row["id"]}"]').inner_text()
                    assert page.locator('#storage-operations-focus').inner_text()
                    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                    page.locator('#storage-operation-tasks').screenshot(
                        path=args.output/f'native-operations-{width}.png')
                capacity_rows = [row for row in native['operations']['tasks']
                                 if row.get('phase') == 'capacity-preflight']
                for row in capacity_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1
                    text = card.text_content()
                    assert '等待實體容量足夠後自動開始' in text
                    assert row['complete_evidence'] is False
                    if row.get('metrics', {}).get('planned_reclaim_allocated_bytes') is not None:
                        assert '清冊預計回收' in text and '尚未釋放' in text
                cases.append({'case': 'native_preflight_capacity_cards', 'width': width,
                              'cards': len(capacity_rows)})
                wsl = next(volume for node in native['nodes'] for volume in node['volumes']
                           if volume['id'] == 'penguin-wsl')
                assert "WSL 檔案系統可用" in page.locator('[data-volume="penguin-wsl"] .storage-size').inner_text()
                accounting = wsl.get('host_storage_accounting', {})
                if accounting.get('state') == 'measured':
                    text = page.locator('[data-volume="penguin-wsl"]').inner_text()
                    assert 'Windows／其他檔案' in text and '含目前占用' in text
                    assert '主機可分配給此 WSL 的總空間' in text
                    cases[-1]['host_accounting_state'] = accounting['state']
                headroom = wsl.get('writable_headroom', {})
                if headroom.get('state') == 'measured':
                    assert headroom['conservative'] is True
                    cases[-1]['conservative_writable_bytes'] = headroom['available_bytes']
                group = wsl['classification']
                if type(group.get('roots_completed')) is int and type(group.get('roots_total')) is int:
                    page.locator('[data-storage-select="penguin-wsl"]').click()
                    text = page.locator('#storage-classification-state').inner_text()
                    assert f"已完成 {group['roots_completed']}／{group['roots_total']} 個根目錄" in text
                    if group.get('layout_changed'):
                        assert "搬移前清冊" in text
                    cases[-1]['native_WSL_root_coverage'] = {
                        key: group[key] for key in ('roots_completed', 'roots_total', 'state', 'observed_at_utc')}
                    page.locator('#storage-inventory').screenshot(path=args.output / f'WSL-progress-{width}.png')
                button = page.locator('[data-storage-select="vastai1T-disk"]')
                button.click()
                assert button.get_attribute("aria-expanded") == "true"
                page.wait_for_function("() => document.querySelectorAll('#storage-honeycomb button:not(:disabled)').length > 0", timeout=30000)
                before = page.locator("#storage-honeycomb").inner_text()
                page.locator("#storage-honeycomb button:not(:disabled)").first.click()
                assert page.locator("#storage-breadcrumbs button").count() == 2
                assert page.locator("#storage-honeycomb").inner_text() != before
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                page.locator(".storage-tree-details > summary").click()
                page.wait_for_function("() => document.querySelectorAll('#storage-tree details').length > 0", timeout=10000)
                page.locator("#storage-tree details > summary").first.click()
                page.wait_for_function("() => document.querySelector('#storage-tree .storage-tree-children')?.children.length > 0")
                page.locator(".storage-tree-details > summary").click()
                page.locator("#storage-inventory").screenshot(path=args.output / f"storage-{width}.png")
                page.locator("#storage-close").click()
                assert page.locator("#storage-explorer").is_hidden()
                assert button.get_attribute("aria-expanded") == "false"
                page.locator('[data-storage-select="nas-disk"]').click()
                nas = next(volume for node in native["nodes"] for volume in node["volumes"]
                           if volume["id"] == "nas-disk")
                if not nas["classification"]["tree"]:
                    assert "未取得分類" in page.locator("#storage-classification-state").inner_text()
                    assert "尚未取得目錄分類" in page.locator("#storage-direct").inner_text()
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                cases.append({"case": "honeycomb_drill_collapse_and_lazy_tree", "width": width, "horizontal_overflow": False})
                assert not errors, errors
                page.close()
            for mode in ("missing", "stale", "markup", "partial", "scanning", "cold_tiers", "backing_missing"):
                data = public_status({}) if mode == "missing" else json.loads(json.dumps(actual))
                if mode == "stale":
                    data["nodes"][0]["volumes"][0]["capacity"]["state"] = "stale"
                if mode == "backing_missing":
                    data["nodes"][0]["volumes"][0]["writable_headroom"].update(state="unknown", available_bytes=None)
                if mode == "cold_tiers":
                    data["cold_tiers"] = {"state": "outside_daily_window", "stale": False,
                        "schedule": {"start_hour": 3, "end_hour": 7, "stable_after_hours": 6},
                        "last_work_cycle": {"observed_at_utc": actual["generated_at_utc"],
                            "objects_completed": 2, "logical_bytes_processed": 25644291,
                            "allocation_bytes_saved": 19451139}}
                if mode in {"markup", "partial", "scanning"}:
                    data["nodes"][1]["volumes"][0]["classification"] = {
                        "state": "scanning" if mode == "scanning" else "partial" if mode == "partial" else "complete", "tree": [
                        {"id": "x", "label": "已完成子目錄" if mode in {"partial", "scanning"} else '<img src=x onerror="alert(1)">',
                         "allocated_bytes": 10, "allocated_bytes_is_lower_bound": mode in {"partial", "scanning"},
                         "kind": "source", "children": []}], "allocated_bytes": 10,
                        "allocated_bytes_is_lower_bound": mode in {"partial", "scanning"},
                        "roots_total": 4, "roots_completed": 2}
                page = browser.new_page(viewport={"width": 390, "height": 960})
                page.route("**/data-monitor/api/storage", lambda route, request, payload=data: route.fulfill(json=payload))
                page.goto(args.base + "/data-monitor/#storage-inventory", wait_until="domcontentloaded")
                expected_count = sum(len(node['volumes']) for node in data['nodes'])
                page.wait_for_function("expected => document.querySelectorAll('.storage-disk').length === expected",
                                       arg=expected_count, timeout=30000)
                if mode == "missing":
                    assert page.locator(".storage-size").all_inner_texts()[0].startswith("尚未量測")
                    assert page.locator(".storage-bar-fill").count() == 0
                elif mode == "stale":
                    assert "保留上次實測值" in page.locator('[data-volume="penguin-wsl"]').inner_text()
                elif mode == "markup":
                    page.locator('[data-storage-select="vastai1T-disk"]').click()
                    assert page.locator("#storage-honeycomb img").count() == 0
                    assert "<img" in page.locator("#storage-honeycomb").inner_text()
                elif mode == "partial":
                    page.locator('[data-storage-select="vastai1T-disk"]').click()
                    assert "≥ " in page.locator("#storage-honeycomb").inner_text()
                    assert "未掃描不當作零" in page.locator("#storage-classification-state").inner_text()
                elif mode == "scanning":
                    page.locator('[data-storage-select="vastai1T-disk"]').click()
                    text = page.locator("#storage-classification-state").inner_text()
                    assert "背景清點中" in text and "已完成 2／4 個根目錄" in text
                    assert "未掃描不當作零" in text
                elif mode == "backing_missing":
                    text = page.locator('[data-volume="penguin-wsl"] .storage-size').inner_text()
                    assert "WSL 檔案系統可用" in text
                    assert "保守寫入餘量未知" in page.locator('[data-volume="penguin-wsl"]').inner_text()
                else:
                    text = page.locator("#storage-tiers").inner_text()
                    assert "03:00–07:00" in text and "2 個物件" in text
                    assert "非全庫壓縮率" in text
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                cases.append({"case": mode + "_fixture", "width": 390, "fixture_is_not_live_evidence": True})
                page.close()
            for width in (1440, 390):
                payload = json.loads(json.dumps(actual))
                runtime = payload["runtime"]["nodes"]
                runtime[0].update(state="measured", observed_at_utc=actual["generated_at_utc"])
                runtime[0]["services"][0]["state"] = "active"
                runtime[2].update(state="unavailable")
                runtime[2]["metrics"].update(gpu_percent=0, gpu_count=2)
                operations = payload["operations"]
                stamp = datetime.fromisoformat(actual['generated_at_utc']).timestamp()
                operations['overall_eta'] = {'state': 'partial', 'estimated_tasks': 2, 'unestimated_tasks': 3,
                    'blocked_tasks': 1, 'known_completion_at_utc': datetime.fromtimestamp(stamp+7200, timezone.utc).isoformat()}
                operations['attention'] = [
                    {'id': 'failure', 'level': 'blocked', 'label': '待處理的原排程',
                     'message': '<img src=x onerror=alert(1)> 原服務失敗，原排程仍啟用。'},
                    {'id': 'backup_owner', 'level': 'waiting', 'label': '備份寫入與收據對帳',
                     'message': '1 個程序正在等待原執行者釋放鎖；尚不能認定死鎖。'}]
                operations['schedules'] = [
                    {'id': 'private_backup', 'label': '私有冷庫備份', 'registered': True,
                     'timer_active': True, 'owner_state': 'failed', 'exit_code': 7},
                    {'id': 'lakehouse', 'label': '冷庫發布、還原與來源複寫', 'registered': True,
                     'mode': 'continuous', 'owner_state': 'active', 'owner_running': True},
                    {'id': 'transport_gc', 'label': '已驗收傳輸暫存回收', 'registered': True,
                     'timer_active': True, 'check_due': True,
                     'scheduled_check_at_utc': '2020-01-02T00:00:00+00:00'}]
                assert len(operations["tasks"]) >= 1
                template = operations["tasks"][0]
                operations["tasks"] = []
                for i in range(8):
                    row = json.loads(json.dumps(template))
                    row.update(id=f"{i:024x}", kind="retirement", state="running", complete_evidence=False,
                        stale=False, progress_stale=False, owner_state='active',
                        observed_at_utc=actual['generated_at_utc'], checked_at_utc=actual['generated_at_utc'],
                        label="<img src=x onerror=alert(1)>" if i == 0 else f"驗證測試 {i}",
                        phase_label="測試中的目前階段", progress={"completed_bytes": 50, "total_bytes": 100,
                        "completed_items": 5, "total_items": 10, "metric": "bytes", "percent": 50.0},
                        eta={"state": "sampling", "message": "同一階段的速度取樣中"}, metrics={})
                    if i in (1, 2):
                        row.update(checked_at_utc=actual['generated_at_utc'],
                                   observed_at_utc='2020-01-02T00:00:00+00:00',
                                   progress_stale=i == 1, owner_state='active')
                        if i == 1:
                            row['eta'] = {'state': 'stale', 'message': '本次已檢查；等待原工作回報新計數，完成時間暫停估算'}
                        else:
                            row.update(state='completed', complete_evidence=True)
                            row['progress'].update(completed_bytes=100, completed_items=10, percent=100)
                            row['eta'] = {'state': 'completed'}
                    if i == 4:
                        stamp = datetime.fromisoformat(actual['generated_at_utc']).timestamp()
                        row.update(state='waiting', label='回收等待測試',
                            reason='原始來源世代與舊收據不同，需完整 SHA 核對後再回收。',
                            next_attempt_at_utc=datetime.fromtimestamp(stamp+600, timezone.utc).isoformat(),
                            next_observation_at_utc=datetime.fromtimestamp(stamp+60, timezone.utc).isoformat(),
                            continuation={'automatic': True, 'owner_running': False,
                                'next_check_at_utc': datetime.fromtimestamp(stamp+30, timezone.utc).isoformat()})
                    if i == 3:
                        row.update(label='完成時間測試', completion_forecast={
                            'state': 'estimated', 'completion_at_utc': datetime.fromtimestamp(stamp+3600, timezone.utc).isoformat(),
                            'remaining_seconds': 3600, 'fastest_seconds': 1800, 'slowest_seconds': 7200,
                            'sample_count': 2, 'confidence': 'low'},
                            queue_forecast={'state': 'estimated',
                                'completion_at_utc': datetime.fromtimestamp(stamp+7200, timezone.utc).isoformat(),
                                'remaining_seconds': 7200, 'fastest_seconds': 3600, 'slowest_seconds': 14400,
                                'sample_count': 2, 'confidence': 'low', 'estimated_jobs': 3, 'retry_jobs': 1})
                    if i == 5:
                        row.update(label='預估超時測試', completion_forecast={
                            'state': 'overdue', 'previous_completion_at_utc': '2020-01-02T00:00:00+00:00'})
                    if i == 6:
                        row.update(kind='migration_publication', phase='packing_changed_small_files',
                            label='封存進度測試', phase_label='壓縮新增或變更的小檔案',
                            progress={'completed_bytes': None, 'total_bytes': None,
                                      'completed_items': 9, 'total_items': 64, 'metric': 'items', 'percent': 14.06},
                            metrics={'processed_source_files': 773763, 'total_source_files': 4684151,
                                     'processed_source_bytes': 326139910831, 'total_source_bytes': 476478228752})
                    if i == 7:
                        row.update(kind='migration_publication', phase='censusing_source',
                            label='來源清點測試', phase_label='清點來源檔案與大小；尚未驗證內容',
                            progress={'completed_bytes': 123456, 'total_bytes': None,
                                      'completed_items': 1234, 'total_items': None,
                                      'metric': 'items', 'percent': None},
                            eta={'state': 'unknown', 'message': '原程序尚未提供這個階段的完整工作量'})
                    operations["tasks"].append(row)
                page = browser.new_page(viewport={"width": width, "height": 960})
                page.clock.install()
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.route("**/data-monitor/api/storage", lambda route: route.fulfill(json=payload))
                page.goto(args.base + "/data-monitor/#storage-inventory", wait_until="domcontentloaded")
                page.wait_for_function("() => document.querySelectorAll('.storage-operation-card').length === 8")
                runtime_detail = page.locator('[data-runtime-detail="penguin"]')
                runtime_detail.locator("summary").click()
                assert "當沖服務：執行中" in runtime_detail.inner_text()
                vast = page.locator('[data-runtime-node="vastai1T"]')
                assert "本次無法讀取" in vast.inner_text() and "GPU 0.0%" in vast.inner_text()
                assert "保留上次量測" in vast.inner_text()
                assert page.locator("#storage-operations img").count() == 0
                assert page.locator('.storage-operation-notice-blocked').count() == 1
                assert '不能認定死鎖' in page.locator('#storage-operation-attention').inner_text()
                assert '<img' in page.locator('#storage-operation-attention').inner_text()
                assert "<img" in page.locator("#storage-operation-tasks").inner_text()
                assert '來源檔已處理 773,763／4,684,151' in page.locator(f'[data-operation="{6:024x}"]').inner_text()
                source_card = page.locator(f'[data-operation="{7:024x}"]')
                # This deliberately lower-priority fixture is in the closed
                # overflow details until the existing expansion check below.
                source_text = source_card.text_content()
                assert '已清點' in source_text and '總量待清點' in source_text, source_text
                assert '1,234' in source_text and '總數待清點' in source_text
                assert '100.00%' not in source_text and '尚未驗證內容' in source_text
                cases.append({'case': 'unknown_source_census_denominator', 'width': width,
                              'fixture_is_not_live_evidence': True})
                pending = page.locator(f'[data-operation="{1:024x}"]')
                accepted = page.locator(f'[data-operation="{2:024x}"]')
                assert '等待新進度計數' in pending.text_content()
                assert '本次檢查' in pending.text_content() and '2020/01/02' in pending.text_content()
                assert '收據已過期' not in pending.text_content() and '監測尚未更新' not in pending.text_content()
                assert '驗收完成' in accepted.text_content() and '2020/01/02' in accepted.text_content()
                assert '收據已過期' not in accepted.text_content()
                waiting = page.locator(f'[data-operation="{4:024x}"]')
                # Waiting rows can start in the intentionally closed overflow;
                # verify their DOM text before the later expansion exercise.
                assert '完整 SHA 核對' in waiting.locator('.storage-operation-reason').text_content()
                forecast_card = page.locator(f'[data-operation="{3:024x}"]')
                forecast_text = forecast_card.text_content()
                assert '目前批次預估完成' in forecast_text and '目前可執行佇列預估完成' in forecast_text
                assert '近期 2 批完整流程' in forecast_text and '可信度低' in forecast_text
                assert '另有 1 批等待重試' in forecast_text
                assert '可估工作目前最晚預估' in page.locator('#storage-operations-eta').text_content()
                assert '已超時' in page.locator(f'[data-operation="{5:024x}"]').text_content()
                cases.append({'case': 'phase_batch_queue_eta_and_overdue', 'width': width,
                              'fixture_is_not_live_evidence': True})
                assert '自動接續已啟用' in waiting.locator('.storage-operation-continuation').text_content()
                assert '每 60 秒更新' in waiting.text_content() and '原程序預定重試' in waiting.text_content()
                cases.append({'case': 'independent_check_progress_and_fixed_acceptance_clocks',
                              'width': width, 'fixture_is_not_live_evidence': True})
                # Foreground polling continues when this section is offscreen;
                # advancing virtual time exercises the real 60-second timer.
                page.evaluate('window.scrollTo(0, 0)')
                operations['tasks'][0]['phase_label'] = '每分鐘自動讀回的新階段'
                operations['tasks'][4].update(state='running', reason=None,
                    phase_label='等待條件更新後已自動接續',
                    continuation={'automatic': True, 'owner_running': True})
                with page.expect_response(lambda response: response.url.endswith('/data-monitor/api/storage')):
                    page.clock.fast_forward(60000)
                page.wait_for_function("() => document.querySelector('#storage-operation-tasks').textContent.includes('每分鐘自動讀回的新階段')")
                assert '等待條件更新後已自動接續' in waiting.text_content()
                assert waiting.locator('.storage-operation-reason').count() == 0
                assert '原排程本輪工作中' in waiting.locator('.storage-operation-continuation').inner_text()
                cases.append({'case': 'foreground_minute_refresh_outside_storage_viewport',
                              'width': width, 'fixture_is_not_live_evidence': True})
                cases.append({'case': 'waiting_reason_and_automatic_continuation_refresh_each_minute',
                              'width': width, 'fixture_is_not_live_evidence': True})
                first = page.locator("[data-operation-detail]").first
                first.locator("summary").click()
                page.locator("#storage-operations-more > summary").click()
                page.locator("#storage-operation-schedules > summary").click()
                assert "下次檢查不保證立即開始" in page.locator("#storage-operation-schedules").inner_text()
                assert '原服務失敗' in page.locator('#storage-operation-schedule-list').inner_text()
                assert '上次退出碼 7' in page.locator('#storage-operation-schedule-list').inner_text()
                assert '原生命週期常駐執行' in page.locator('#storage-operation-schedule-list').inner_text()
                assert '已到預定檢查' in page.locator('#storage-operation-schedule-list').inner_text()
                assert page.locator('.storage-operation-stage').count() == 6
                cases.append({'case': 'complete_schedule_blockers_and_lock_waits', 'width': width,
                              'safe_text_only': True, 'stages': 6})
                operations["tasks"][0]["phase_label"] = "取樣後更新的階段"
                page.locator("#storage-operations").scroll_into_view_if_needed()
                first.locator("summary").focus()
                with page.expect_response(lambda response: response.url.endswith('/data-monitor/api/storage')):
                    page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
                page.wait_for_function("() => document.querySelector('#storage-operation-tasks').textContent.includes('取樣後更新的階段')")
                assert page.locator("[data-operation-detail]").first.get_attribute("open") is not None
                assert page.locator("#storage-operations-more").get_attribute("open") is not None
                assert page.locator("#storage-operation-schedules").get_attribute("open") is not None
                assert runtime_detail.get_attribute("open") is not None
                assert page.locator("[data-operation-detail]").first.locator("summary").evaluate("node => node === document.activeElement")
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                page.locator("#storage-operations").screenshot(path=args.output / f"operations-{width}.png")
                page.unroute("**/data-monitor/api/storage")
                page.route("**/data-monitor/api/storage", lambda route: route.abort("failed"))
                page.locator("#storage-operations").scroll_into_view_if_needed()
                page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
                # Earlier synthetic tasks can already have paused ETA text.
                # Wait for this request's failed observation render as well.
                page.wait_for_function("() => document.querySelector('#storage-operations-observed').textContent.includes('觀測已過期')")
                page.wait_for_function("() => document.querySelector('#storage-operation-tasks').textContent.includes('完成時間暫停估算')")
                assert "觀測已過期" in page.locator("#storage-operations-observed").inner_text()
                assert '目前無法更新運行與排程觀測' in page.locator('#storage-operation-attention').inner_text()
                assert '不能認定死鎖' not in page.locator('#storage-operation-attention').inner_text()
                assert '原生命週期常駐執行' not in page.locator('#storage-operation-schedule-list').inner_text()
                assert '排程快照已過期' in page.locator('#storage-operation-schedule-list').inner_text()
                assert '流程觀測已過期' in page.locator('#storage-operation-flow').inner_text()
                assert '無法確認自動接續狀況' in waiting.locator('.storage-operation-continuation').inner_text()
                assert '原排程本輪工作中' not in waiting.text_content()
                assert waiting.locator('.storage-operation-next-observation').count() == 0
                assert not errors, errors
                cases.append({"case": "operations_refresh_schedule_scope_and_expand_state", "width": width,
                              "horizontal_overflow": False, "fixture_is_not_live_evidence": True})
                page.close()
        finally:
            browser.close()
    receipt = {"checked_at_utc": datetime.now(timezone.utc).isoformat(), "state": "passed",
        "renderer_profile": "cpu-2d", "cases": cases, "provider_calls": 0,
        "native_source_snapshot_generated_at_utc": actual['generated_at_utc'],
        "native_layout_source": "fixed_actual_API_observation",
        "polling_scope": "real_foreground_timer_with_fixture_and_virtual_clock"}
    (args.output / "browser_acceptance.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
