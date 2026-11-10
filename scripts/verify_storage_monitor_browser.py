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
                if (native.get('cold_tiers') or {}).get('state') == 'waiting_migration_priority':
                    assert '優先處理 D 空間回收' in page.locator('#storage-tiers').inner_text()
                    cases.append({'case': 'cold_tier_yields_to_active_migration', 'width': width})
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
                    # The live queue can exceed the first lazy page. Expand
                    # all pages before asserting every provider's card; do
                    # not require the production UI to render them eagerly.
                    page.wait_for_function("() => document.querySelector('#storage-operations-more').open && "
                        "document.querySelector('#storage-operation-other-tasks').childElementCount > 0")
                    more_rows = page.locator('#storage-operation-other-tasks > button')
                    while more_rows.count():
                        more_rows.click()
                    cases.append({'case': 'native_additional_work_expands_from_summary', 'width': width})
                vast_cleanup = [row for row in native['operations']['tasks']
                                if row.get('kind') == 'vast_testing_cleanup' and row['complete_evidence']]
                for row in vast_cleanup:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['node_id'] == 'vastai1T'
                    text = card.text_content() or ''
                    assert '回收 Vast 配置' in text and '不代表 D 或 NAS 驗收' in text
                if vast_cleanup:
                    cases.append({'case': 'vast_disposable_cleanup_is_scoped_to_vast', 'width': width})
                rescue_rows = [row for row in native['operations']['tasks']
                               if row.get('kind') == 'tw_rescue_original_manifests'
                               and row.get('reason_code') == 'rescue_original_manifest_missing']
                for row in rescue_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and not row['complete_evidence']
                    assert row['progress']['completed_items'] < row['progress']['total_items']
                    text = card.text_content() or ''
                    assert '原始續訓規格' in text and '固定版本清冊' in text
                    assert '不檢查內容物' in text and 'runtime' in text
                if rescue_rows:
                    cases.append({'case': 'original_rescue_manifest_gap_is_not_training_readiness', 'width': width})
                rescue_capture = [row for row in native['operations']['tasks']
                                  if row.get('kind') == 'training_rescue_capture'
                                  and row.get('reason_code') == 'rescue_capture_D_capacity']
                for row in rescue_capture:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    assert card.count() == 1 and not row['complete_evidence']
                    assert '保留門檻' in text and '不代表遠端已停止' in text
                if rescue_capture:
                    cases.append({'case': 'rescue_capacity_wait_is_not_a_capture', 'width': width})
                rescue_manifests = [row for row in native['operations']['tasks']
                                    if row.get('kind') == 'training_rescue_manifests'
                                    and row.get('reason_code') == 'rescue_registered_manifest_missing']
                for row in rescue_manifests:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    assert card.count() == 1 and not row['complete_evidence']
                    assert row['progress']['completed_items'] < row['progress']['total_items']
                    assert '已登錄續訓來源' in text and 'E／NAS' in text
                if rescue_manifests:
                    cases.append({'case': 'registered_rescue_manifest_gap_is_not_recovery', 'width': width})
                rescue_references = [row for row in native['operations']['tasks']
                                     if row.get('kind') == 'training_rescue_references'
                                     and row.get('metrics', {}).get('rescue_protected_bindings') is not None]
                for row in rescue_references:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    assert card.count() == 1 and '固定來源身分受保護' in text
                    assert '不代表資料物件' in text
                if rescue_references:
                    cases.append({'case': 'rescue_reference_protection_is_not_object_availability', 'width': width})
                e_capacity_rows = [row for row in native['operations']['tasks']
                                   if row.get('kind') == 'workspace_e_resize'
                                   and row.get('phase') == 'windows_storage_capacity_observed']
                for row in e_capacity_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert row['state'] == 'completed' and card.count() == 1
                    assert row['metrics'].get('guest_e_filesystem_capacity_bytes')
                    text = card.text_content() or ''
                    assert 'E 區塊裝置' in text and '檔案系統' in text
                    assert '先前 Windows 維護與服務恢復另計' in text
                if e_capacity_rows:
                    cases.append({'case': 'native_e_capacity_separate_from_past_windows_maintenance',
                                  'width': width})
                return_rows = [row for row in native['operations']['tasks'] if row.get('kind') == 'remote_return']
                for row in return_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['phase_label'] in card.inner_text()
                    assert not row['complete_evidence'] and row['progress']['percent'] is None
                    counts = row.get('metrics', {})
                    if all(isinstance(counts.get(k), int) for k in
                           ('remote_observed', 'remote_eligible', 'remote_published', 'remote_cycle_deferred')):
                        assert '最近完成一輪' in card.inner_text() and '當時未輪到' in card.inner_text()
                    if isinstance(counts.get('remote_deferred_recovery'), int):
                        text = card.text_content() or ''
                        assert '暫緩額外還原' in text and '未執行遠端刪除' in text
                    if isinstance(counts.get('remote_cache_available_bytes'), int):
                        text = card.text_content() or ''
                        assert 'Vast 暫存清理' in text and '下載快取' in text
                if return_rows:
                    cases.append({'case': 'remote_return_cycle_is_not_full_history_completion', 'width': width})
                bulk_rows = [row for row in native['operations']['tasks'] if row.get('kind') == 'bulk_return']
                for row in bulk_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    counts = row.get('metrics', {})
                    assert card.count() == 1
                    if counts.get('D_bulk_carriers_reclaimed'):
                        assert '舊回傳整包已回收' in card.inner_text()
                    if counts.get('D_bulk_landing_parts_reclaimed'):
                        assert '舊回傳分段已回收' in card.inner_text()
                        assert '不代表 NAS 驗收' in card.inner_text()
                if bulk_rows:
                    cases.append({'case': 'bulk_reclaimed_D_carriers_and_parts_are_distinct', 'width': width})
                mount_rows = [row for row in native['operations']['tasks'] if row.get('kind') == 'd_mount_repair']
                for row in mount_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and '掛載' in card.inner_text()
                    assert not row['complete_evidence']
                if mount_rows:
                    cases.append({'case': 'periodic_D_mount_observation_is_not_data_acceptance', 'width': width})
                export_rows = [row for row in native['operations']['tasks'] if row.get('kind') == 'catalog_export']
                for row in export_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['phase_label'] in card.inner_text()
                    assert not row['complete_evidence'] and row['eta']['completion_at_utc'] is None
                    if row['previous_source_failure']:
                        assert row['previous_source_failure']['message'] in card.inner_text()
                    if row['metrics'].get('attempt_number') is not None:
                        assert f"第 {row['metrics']['attempt_number']} 次嘗試" in card.inner_text()
                if export_rows:
                    cases.append({'case': 'native_catalog_export_retry_is_not_acceptance', 'width': width})
                provider_rows = [row for row in native['operations']['tasks']
                                 if row.get('kind') == 'provider_workspace']
                for row in provider_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1
                    assert row['label'] in card.inner_text()
                    assert row['phase_label'] in card.inner_text()
                    assert '不代表 C 原件已回收' in row['scope']
                    if row['phase'] == 'workspace_handoff':
                        assert row['progress']['completed_bytes'] is None
                        assert row['progress']['total_bytes'] is None
                        assert row['eta']['completion_at_utc'] is None
                if provider_rows:
                    cases.append({'case': 'native_provider_e_copy_handoff_not_retirement',
                                  'width': width, 'providers': len({row['label'] for row in provider_rows})})
                generation_rows = [row for row in native['operations']['tasks']
                                   if row.get('kind') == 'handoff_index_D'
                                   and row.get('metrics', {}).get('source_conflict_items') is not None]
                for row in generation_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    expected_status = ('已選用較新來源；舊世代數值尚未逐筆比較'
                        if row['phase'] == 'handoff_index_public_latest_selected' else
                        '公開世代已整合，本機限定來源另保留' if row['complete_evidence'] else '差異尚未合併')
                    assert '世代清冊' in text and expected_status in text
                    assert f"內容不同 {row['metrics']['source_conflict_items']:,} 項" in text
                if generation_rows:
                    cases.append({'case': 'native_source_generations_show_conflicts_without_merge',
                                  'width': width, 'rows': len(generation_rows)})
                attribution_rows = [row for row in native['operations']['tasks']
                                    if row.get('kind') == 'staged_cold_attribution']
                for row in attribution_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    assert '既有清冊引用' in text and '不代表已還原或可刪除' in text
                    assert row['complete_evidence'] is False
                if attribution_rows:
                    cases.append({'case': 'native_staged_object_attribution_is_not_reconstruction',
                                  'width': width, 'rows': len(attribution_rows)})
                capacity_rows = [row for row in native['operations']['tasks']
                                 if row.get('kind') in {'source_local_repack_queue', 'handoff_index_D_storage'}
                                 and row.get('metrics', {}).get('D_capacity_observed_epoch') is not None]
                for row in capacity_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    assert '需寫入' in text and '尚缺' in text and '實際寫入前會重新檢查' in text
                    assert row['complete_evidence'] is False
                    if row.get('phase') in {'source_local_E_prepared', 'source_local_E_prepared_ready'} \
                            and row.get('progress', {}).get('percent') == 100:
                        assert '本階段 100.00% · 等待寫入 D' in text and '等待正式驗收' not in text
                if capacity_rows:
                    cases.append({'case': 'native_prepared_source_shows_measured_D_shortfall',
                                  'width': width, 'rows': len(capacity_rows)})
                history_rows = [row for row in native['operations']['tasks']
                                if row.get('kind') == 'source_repack_retirement'
                                and row.get('metrics', {}).get('history_cumulative_reclaimed_bytes') is not None
                                and row.get('metrics', {}).get('history_cumulative_versions') is not None]
                for row in history_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    assert '累計估計釋放 D' in text and '非目前執行進度' in text
                if history_rows:
                    cases.append({'case': 'native_history_cumulative_separate_from_current_batch',
                                  'width': width, 'rows': len(history_rows)})
                missing_anchor_rows = [row for row in native['operations']['tasks']
                                       if row.get('metrics', {}).get('history_anchor_missing') == 1]
                for row in missing_anchor_rows:
                    text = page.locator(f'[data-operation="{row["id"]}"]').text_content() or ''
                    assert '中繼版本已不存在' in text and '未證明世代後繼或舊資料可還原' in text
                if missing_anchor_rows:
                    cases.append({'case': 'native_history_anchor_warning',
                                  'width': width, 'rows': len(missing_anchor_rows)})
                head_wait_rows = [row for row in native['operations']['tasks']
                                  if row.get('state') == 'blocked' and not row.get('stale')
                                  and row.get('reason_code') in {'repack_foreign_head_wait', 'retirement_current_head'}
                                  and row.get('continuation', {}).get('automatic') is True]
                for row in head_wait_rows[:3]:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.locator('.storage-operation-continuation').text_content() or ''
                    if row.get('reason_code') == 'retirement_current_head':
                        assert '需檢查遠端狀態與交接' in text and '尚未完成的版本仍保留' in text
                    else:
                        assert '仍需完成新 D 版本及跨節點交接' in text and '不會直接回收舊版' in text
                if head_wait_rows:
                    cases.append({'case': 'native_head_handoff_not_confused_with_retry',
                                  'width': width, 'rows': len(head_wait_rows)})
                handoff_rows = [row for row in native['operations']['tasks']
                                if row.get('kind') == 'foreign_head_handoff'
                                and row.get('phase') == 'foreign_head_queue']
                for row in handoff_rows:
                    text = page.locator(f'[data-operation="{row["id"]}"]').text_content() or ''
                    assert '本輪候選' in text and '非全系統總數' in text
                    assert '指標交接完成' in text and '不代表 D 物件已回收或 NAS 驗收' in text
                    assert not row['complete_evidence']
                if handoff_rows:
                    cases.append({'case': 'native_peer_handoff_is_not_D_retirement',
                                  'width': width, 'rows': len(handoff_rows)})
                activation_rows = [row for row in native['operations']['tasks']
                                   if row.get('kind') == 'v3_reader_activation']
                for row in activation_rows:
                    text = page.locator(f'[data-operation="{row["id"]}"]').text_content() or ''
                    assert 'E 候選' in text and '不表示 D 已安裝' in text
                    assert not row['complete_evidence']
                    if row.get('metrics', {}).get('v3_activated_last') is not None:
                        assert '最近一次啟用' in text
                if activation_rows:
                    cases.append({'case': 'native_v3_activation_is_not_D_publication',
                                  'width': width, 'rows': len(activation_rows)})
                reader_rows = [row for row in native['operations']['tasks']
                               if row.get('kind') == 'edge_reader_recovery']
                for row in reader_rows:
                    text = page.locator(f'[data-operation="{row["id"]}"]').text_content() or ''
                    assert '固定封裝讀取器' in text and '不下載或驗證市場內容' in text
                    assert not row['complete_evidence']
                if reader_rows:
                    cases.append({'case': 'native_reader_recovery_is_not_payload_recovery',
                                  'width': width, 'rows': len(reader_rows)})
                scan_rows = [row for row in native['operations']['tasks']
                             if row.get('kind') == 'cold_scan_notification']
                for row in scan_rows:
                    text = page.locator(f'[data-operation="{row["id"]}"]').text_content() or ''
                    assert '通知待處理' in text and '不表示 Vast 已同步內容' in text
                    assert not row['complete_evidence']
                if scan_rows:
                    cases.append({'case': 'native_scan_notice_is_not_peer_convergence',
                                  'width': width, 'rows': len(scan_rows)})
                lab_head_rows = [row for row in native['operations']['tasks']
                                 if row.get('reason_code') == 'repack_lab203_head_wait']
                for row in lab_head_rows:
                    text = page.locator(f'[data-operation="{row["id"]}"]').text_content() or ''
                    assert 'lab203 指標引用' in text and 'E 封裝已就緒' in text
                    assert row.get('metrics', {}).get('D_shortfall_bytes') is not None
                    assert not row['complete_evidence']
                if lab_head_rows:
                    cases.append({'case': 'native_lab203_head_and_D_capacity_are_visible',
                                  'width': width, 'rows': len(lab_head_rows)})
                foreign_E_rows = [row for row in head_wait_rows
                                  if row.get('kind') == 'source_local_repack_queue'
                                  and row.get('phase') == 'source_local_E_local_ready']
                for row in foreign_E_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    assert 'E 暫時資料集已就緒' in text and '標準來源尚未切換' in text
                    assert not row['complete_evidence']
                if foreign_E_rows:
                    cases.append({'case': 'foreign_head_E_view_is_not_a_D_handoff',
                                  'width': width, 'rows': len(foreign_E_rows)})
                install_rows = [row for row in native['operations']['tasks']
                                if row.get('kind') == 'source_local_repack_queue'
                                and row.get('state') == 'running'
                                and row.get('phase') == 'source_local_D_installing']
                for row in install_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    text = card.text_content() or ''
                    assert '將 E 新封包寫入 D' in text
                    assert '尚缺' not in text and row['complete_evidence'] is False
                    assert row['continuation']['owner_running'] is True
                    if row['progress'].get('total_bytes'):
                        assert row['progress']['metric'] == 'bytes'
                        assert row['eta']['unit'] == 'bytes'
                if install_rows:
                    cases.append({'case': 'native_parallel_prepared_install_has_live_owner_without_old_blocker',
                                  'width': width, 'rows': len(install_rows)})
                staging_rows = [row for row in native['operations']['tasks']
                                if row.get('kind') == 'workspace_source_staging']
                for row in staging_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1
                    assert row['label'] in card.inner_text() and row['phase_label'] in card.inner_text()
                    assert '不代表原路徑已切換' in row['scope']
                    if row['phase'] == 'workspace_seed_copy':
                        assert row['progress']['completed_bytes'] is None
                        assert not row['complete_evidence']
                        if row['progress']['completed_items'] is None:
                            assert row['eta']['completion_at_utc'] is None
                        else:
                            assert '尚未讀回驗收' in card.locator('.storage-bar').get_attribute('aria-label')
                            assert row['eta']['scope'] == 'rsync_filelist_estimate'
                            assert '傳送量不等於已驗證量' in card.inner_text()
                            if row['eta']['completion_at_utc']:
                                assert '清單檢查粗估完成' in card.inner_text()
                    if row['phase'] == 'workspace_seed_recheck' and row['progress']['completed_items'] is not None:
                        assert row['progress']['metric'] == 'items'
                        if row['eta']['state'] == 'estimated':
                            assert '本階段預估完成' in card.inner_text()
                    if row['state'] == 'waiting':
                        assert row['continuation']['owner_running'] is False
                    if row.get('metrics', {}).get('cftc_raw_restored_files') == 2:
                        assert 'D 固定版本的原始 ZIP' in card.inner_text()
                        assert 'NAS 驗收另計' in card.inner_text()
                if staging_rows:
                    cases.append({'case': 'native_source_staging_scope_and_live_phase', 'width': width,
                                  'rows': len(staging_rows)})
                flow = native['operations'].get('flow', [])
                assert {'e_copy', 'e_links', 'e_handoff', 'recover', 'nas'} <= {row['id'] for row in flow}
                assert page.locator('.storage-stage-estimate').count() == len(flow)
                assert '後續步驟待估' in (page.locator('#storage-operation-flow').text_content() or '') or not any(
                    row.get('known_current_step_completion_at_utc') for row in flow)
                if any(row.get('id') == 'd_cold_tier_capacity' for row in native['operations'].get('attention', [])):
                    notice = page.locator('#storage-operation-attention').inner_text()
                    assert 'D 冷區整理容量不足' in notice and '不代表已開始的 D 讀取還原停止' in notice
                coverage = next(row for row in flow if row['id'] == 'e_handoff')
                if coverage.get('unassessed_source_count'):
                    retirement_stage = next(row for row in flow if row['id'] == 'retire')
                    assert retirement_stage['state'] != 'completed'
                    assert retirement_stage['unassessed_source_count'] == coverage['unassessed_source_count']
                    # The retirement explanation is inside the initially
                    # collapsed schedule; text_content includes that detail.
                    assert '回收範圍待確認' in (page.locator('#storage-operation-flow').text_content() or '')
                    page.locator('#storage-operation-schedules > summary').click()
                    detail = page.locator('details[data-stage-coverage="e_handoff"]')
                    detail.locator('summary').click()
                    assert all(label in detail.inner_text() for label in coverage['unassessed_source_labels'])
                    assert detail.locator('.storage-handoff-sources > li').count() == len(coverage['unassessed_source_labels'])
                    assert '不代表 C 原件已可回收' in detail.inner_text()
                    assert '來源交接待核對，完成時間待估' in detail.inner_text()
                    assert any('E 固定副本' in item for item in detail.locator('.storage-handoff-sources > li').all_inner_texts())
                    with page.expect_response(lambda response: response.url.endswith('/data-monitor/api/storage')):
                        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
                    assert page.locator('details[data-stage-coverage="e_handoff"]').get_attribute('open') is not None
                    page.locator('#storage-operation-schedules > summary').click()
                    cases.append({'case': 'unregistered_handoffs_visible_and_expand_state_survives_refresh',
                                  'width': width, 'sources': coverage['unassessed_source_count']})
                handoff_rows = [row for row in native['operations']['tasks']
                                if row.get('kind') == 'workspace_source_handoff']
                for row in handoff_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['label'] in card.inner_text()
                    assert '回收收據' in row['scope'] and '不代表 D／NAS 已驗收' in row['scope']
                    assert row['continuation']['automatic'] is (row['phase'] not in {
                        'source_handoff_unenrolled', 'source_handoff_root_changed'})
                    if row['phase'] == 'source_handoff_unenrolled':
                        assert '不等於目前 C 槽佔用或待回收量' in card.inner_text()
                    metrics = row.get('metrics', {})
                    if metrics.get('namespace_pass') is not None:
                        assert (f"第 {metrics['namespace_pass']}／{metrics['namespace_passes_total']} 輪"
                                in card.inner_text())
                if handoff_rows:
                    cases.append({'case': 'native_source_handoff_distinct_from_backup', 'width': width,
                                  'rows': len(handoff_rows)})
                active_archives = [row for row in native['operations']['tasks']
                                   if row.get('kind') == 'handoff_index_archive' and row.get('state') == 'running']
                for row in active_archives:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['phase_label'] in card.inner_text()
                    if row.get('owner_deadline_at_utc'):
                        assert '舊版本輪時限' in card.inner_text()
                        assert row['reason_code'] == 'handoff_archive_legacy_timeout'
                        if row['eta']['state'] == 'unknown':
                            assert row['eta']['completion_at_utc'] is None
                if active_archives:
                    cases.append({'case':'native_parallel_archives_and_legacy_deadline',
                                  'width':width, 'rows':len(active_archives)})
                coverage_rows = [row for row in native['operations']['tasks']
                                 if row.get('kind') == 'handoff_index_coverage']
                for row in coverage_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['phase_label'] in card.inner_text()
                    assert '不是 D／NAS 獨立讀回' in row['scope']
                    assert row['scope'] in card.text_content()
                    if row['phase'] == 'handoff_index_coverage_parent_scan':
                        assert row['progress']['total_items'] is None and row['progress']['percent'] is None
                if coverage_rows:
                    cases.append({'case':'native_futures_D_inventory_comparison_scope',
                                  'width':width, 'rows':len(coverage_rows)})
                scheduled_binance = [row for row in handoff_rows if row.get('state') == 'waiting'
                                     and row.get('next_attempt_at_utc')
                                     and row['label'].endswith(('Binance 市場與衍生資料', 'Binance 公開歷史'))]
                for row in scheduled_binance:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert '台股保護時段結束後，最早' in card.inner_text()
                    assert '完成時間待估' in card.inner_text()
                    assert '不是完成時間' in card.inner_text()
                if scheduled_binance:
                    cases.append({'case': 'scheduled_binance_boundary_not_completion', 'width': width})
                closure_rows = [row for row in native['operations']['tasks']
                                if row.get('kind') == 'workspace_symlink_closure']
                for row in closure_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['label'] in card.inner_text()
                    assert '不是新的原始內容讀回' in row['scope']
                    assert row['continuation']['automatic'] is True
                if closure_rows:
                    cases.append({'case': 'native_symlink_closure_is_not_backup', 'width': width,
                                  'rows': len(closure_rows)})
                comparison_rows = [row for row in native['operations']['tasks']
                                   if row.get('kind') == 'workspace_cold_inventory']
                for row in comparison_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['label'] in card.inner_text()
                    assert '不是 D 實際原值讀回' in row['scope']
                    assert row['continuation']['automatic'] is True
                if comparison_rows:
                    cases.append({'case': 'native_cold_inventory_is_not_restore', 'width': width,
                                  'rows': len(comparison_rows)})
                snapshot_rows = [row for row in native['operations']['tasks']
                                 if row.get('kind') == 'workspace_cold_inventory_snapshot']
                for row in snapshot_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and '整檔邏輯量' in card.inner_text()
                    assert '去重後實際容量待估' in card.inner_text()
                    assert '不是 D 讀回' in row['scope'] and not row['continuation']['automatic']
                if snapshot_rows:
                    cases.append({'case': 'native_binance_fixed_inventory_is_not_recovery',
                                  'width': width, 'rows': len(snapshot_rows)})
                partial_rows = [row for row in native['operations']['tasks']
                                if row.get('kind') == 'workspace_partial_cold_readback']
                for row in partial_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert row['complete_evidence'] and card.count() == 1
                    assert 'D 正本已完整讀回' in card.inner_text() and '暫存回收與 NAS 驗收另計' in card.inner_text()
                    assert '不授權刪除' in row['scope']
                if partial_rows:
                    cases.append({'case': 'native_partial_cold_readback_is_not_retirement',
                                  'width': width, 'retirement_authorized': False})
                name_rows = [row for row in native['operations']['tasks']
                             if row.get('kind') == 'workspace_partial_name_restore']
                for row in name_rows:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and row['phase_label'] in card.inner_text()
                    assert '不表示 D 回收完成或 NAS 已驗收' in row['scope']
                    assert row['progress']['total_items'] == 159
                    if row['complete_evidence']:
                        assert row['progress']['completed_items'] == 159
                if name_rows:
                    cases.append({'case': 'native_partial_name_restore_is_not_d_retirement',
                                  'width': width, 'nas_acceptance': False})
                retained_rows = [row for row in native['operations']['tasks']
                                 if row.get('kind') == 'provider_originals']
                for row in retained_rows:
                    assert row['state'] == 'waiting' and not row['complete_evidence']
                    assert row['progress']['completed_bytes'] is None
                    assert row['progress']['total_bytes'] is None
                    assert row['eta']['completion_at_utc'] is None
                    assert row['reason_code'] == 'provider_originals_pending'
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1 and '不是 C 可回收量' in card.inner_text()
                    assert 'C 檔案系統仍有' in card.inner_text()
                    assert '待驗收回收' in card.inner_text()
                    assert '完成時間：尚無可用估計' in card.inner_text()
                    assert card.inner_text().count(row['reason']) == 1
                if retained_rows:
                    cases.append({'case': 'native_retained_c_originals_need_separate_acceptance',
                                  'width': width, 'retirement_authorized': False})
                lake_failures = [row for row in native['operations']['tasks']
                                 if row.get('kind') == 'lake_relay'
                                 and row.get('reason_code') == 'lake_relay_memory_exhausted'
                                 and not row.get('stale')]
                for row in lake_failures:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert row['state'] == 'blocked' and not row['complete_evidence']
                    assert row['eta']['completion_at_utc'] is None and row['diagnosis_at_utc']
                    assert '記憶體不足' in card.inner_text() and '最近一次驗證失敗' in card.inner_text()
                    assert '接收端已嘗試' in card.inner_text()
                    assert next(stage for stage in flow if stage['id'] == 'nas')['state'] == 'blocked'
                if lake_failures:
                    cases.append({'case': 'native_lake_relay_memory_failure_is_not_acceptance',
                                  'width': width, 'nas_acceptance': False})
                provider_failures = [row for row in provider_rows if row.get('reason_code') in {
                    'provider_source_drift', 'provider_namespace_registry'}]
                for row in provider_failures:
                    assert row['state'] == 'blocked' and not row['complete_evidence']
                    assert row['failure_observed_at_utc']
                    assert row['eta']['completion_at_utc'] is None
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert '例外狀態觀測' in card.inner_text()
                    assert row['reason'] in card.inner_text()
                if provider_failures:
                    cases.append({'case': 'native_provider_exception_does_not_renew_work_clock',
                                  'width': width, 'failed_phases': len(provider_failures)})
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
                source_candidates = [row for row in native['operations']['tasks']
                                     if row.get('kind') == 'source_local_candidate']
                for row in source_candidates:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1
                    text = card.text_content()
                    assert row['phase_label'] in text and row['scope'] in text
                    assert '不是 D 新版本' in text and 'NAS 驗收' in text
                    if row['complete_evidence']:
                        assert row['progress']['completed_items'] == row['progress']['total_items']
                    else:
                        assert row['state'] in {'waiting', 'blocked'}
                if source_candidates:
                    cases.append({'case': 'native_E_source_local_candidate_scope', 'width': width,
                                  'candidates': len(source_candidates), 'D_or_NAS_accepted': False})
                cold_handoffs = [row for row in native['operations']['tasks']
                                 if row.get('kind') in {'d_cold_e_handoff', 'd_cold_e_total'}]
                for row in cold_handoffs:
                    card = page.locator(f'[data-operation="{row["id"]}"]')
                    assert card.count() == 1
                    text = card.text_content()
                    assert row['phase_label'] in text and row['scope'] in text
                    assert '不是全部資料集' in text
                if cold_handoffs:
                    cases.append({'case': 'native_D_to_E_handoff_has_narrow_scope', 'width': width,
                                  'rows': len(cold_handoffs), 'all_datasets_claimed': False})
                assert len(native['operations']['schedules']) >= 20
                assert len(native['operations']['flow']) >= 9
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
                    if group.get('deferred_reason') == 'source_local_migration_active':
                        assert '清點暫讓路給封裝與遷移' in text and '仍每分鐘更新' in text
                        cases.append({'case': 'native_census_yields_to_migration_without_suppressing_capacity',
                                      'width': width})
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
                page.locator("#storage-explorer > .storage-tree-details > summary").click()
                page.wait_for_function("() => document.querySelectorAll('#storage-tree details').length > 0", timeout=10000)
                page.locator("#storage-tree details > summary").first.click()
                page.wait_for_function("() => document.querySelector('#storage-tree .storage-tree-children')?.children.length > 0")
                page.locator("#storage-explorer > .storage-tree-details > summary").click()
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
                for i in range(10):
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
                    if i == 8:
                        row.update(kind='staged_cold_attribution', phase='staged_cold_attributed',
                            state='blocked', label='部分清冊測試',
                            reason='部分搬移批次尚未核對，總數仍未知。',
                            progress={'completed_bytes': 200, 'total_bytes': None,
                                      'completed_items': 53, 'total_items': None,
                                      'metric': 'items', 'percent': None},
                            metrics={'staged_deferred_batches': 8, 'staged_known_subset_objects': 525},
                            eta={'state': 'blocked', 'message': '總數仍未知'})
                    if i == 9:
                        row.update(kind='source_repack_retirement', state='blocked', label='現行版本交接測試',
                            reason_code='retirement_current_head', reason='舊版本仍是現行 head',
                            continuation={'automatic': True, 'owner_running': True})
                    operations["tasks"].append(row)
                page = browser.new_page(viewport={"width": width, "height": 960})
                page.clock.install()
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.route("**/data-monitor/api/storage", lambda route: route.fulfill(json=payload))
                page.goto(args.base + "/data-monitor/#storage-inventory", wait_until="domcontentloaded")
                page.wait_for_function("() => document.querySelectorAll('.storage-operation-card').length === 6")
                assert page.locator('#storage-operations-more').get_attribute('open') is None
                page.locator('#storage-operations-more > summary').click()
                page.wait_for_function("() => document.querySelectorAll('.storage-operation-card').length === 10")
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
                # Lower-priority fixtures are rendered only after expansion.
                source_text = source_card.text_content()
                assert '已清點' in source_text and '總量待清點' in source_text, source_text
                assert '1,234' in source_text and '總數待清點' in source_text
                assert '100.00%' not in source_text and '尚未驗證內容' in source_text
                cases.append({'case': 'unknown_source_census_denominator', 'width': width,
                              'fixture_is_not_live_evidence': True})
                partial_text = page.locator(f'[data-operation="{8:024x}"]').text_content()
                assert '既有清冊引用 53 個物件' in partial_text and '總數待清點' in partial_text
                assert '已清點 525 個物件' in partial_text and '另有 8 批未完成清點' in partial_text
                assert '不代表已還原或可刪除' in partial_text and '100.00%' not in partial_text
                cases.append({'case': 'partial_staged_catalog_has_unknown_denominator', 'width': width,
                              'fixture_is_not_live_evidence': True})
                handoff = page.locator(f'[data-operation="{9:024x}"]').locator('.storage-operation-continuation')
                assert '需檢查遠端狀態與交接' in handoff.text_content()
                assert '尚未完成的版本仍保留' in handoff.text_content()
                assert '原排程本輪工作中' not in handoff.text_content()
                cases.append({'case': 'head_handoff_retry_is_not_automatic_handoff', 'width': width,
                              'fixture_is_not_live_evidence': True})
                pending = page.locator(f'[data-operation="{1:024x}"]')
                accepted = page.locator(f'[data-operation="{2:024x}"]')
                assert '等待新進度計數' in pending.text_content()
                assert '本次檢查' in pending.text_content() and '2020/01/02' in pending.text_content()
                assert '收據已過期' not in pending.text_content() and '監測尚未更新' not in pending.text_content()
                assert '驗收完成' in accepted.text_content() and '2020/01/02' in accepted.text_content()
                assert '收據已過期' not in accepted.text_content()
                waiting = page.locator(f'[data-operation="{4:024x}"]')
                # The overflow was expanded above before inspecting this row.
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
                assert page.locator("#storage-operations-more").get_attribute("open") is not None
                page.locator("#storage-operation-schedules > summary").click()
                assert "下次檢查不保證立即開始" in page.locator("#storage-operation-schedules").inner_text()
                assert '原服務失敗' in page.locator('#storage-operation-schedule-list').inner_text()
                assert '上次退出碼 7' in page.locator('#storage-operation-schedule-list').inner_text()
                assert '原生命週期常駐執行' in page.locator('#storage-operation-schedule-list').inner_text()
                assert '已到預定檢查' in page.locator('#storage-operation-schedule-list').inner_text()
                assert page.locator('.storage-operation-stage').count() == len(operations['flow'])
                cases.append({'case': 'complete_schedule_blockers_and_lock_waits', 'width': width,
                              'safe_text_only': True, 'stages': len(operations['flow'])})
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
