"""Bounded operational receipts for the storage observer, never a work owner.

Only small metadata and enrolled receipt directories are read. No source walk,
payload hash, backup catalogue/ledger, SSH, or workflow mutation belongs here.
Rate history is scoped to an exact generation, phase and counter denominator.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path, PureWindowsPath
import re
import sqlite3
import stat
import subprocess
import time
from zoneinfo import ZoneInfo

from stockagent.service_layout import canonical_service_path

CONTRACT = "stockagent.storage_operations.v1"
HISTORY_CONTRACT = "stockagent.storage_operations_history.v1"
MAX_METADATA_BYTES = 1024 * 1024
STALE_SECONDS = 300
# Keep the complete current migration queue plus its separate retirement and
# handoff receipts visible. 256 already truncated real enrolled work. The
# observer and gateway still apply the same bounded whitelist and report any
# further omissions explicitly. The browser creates the long list lazily, so
# increasing this bounded metadata budget does not create 1,024 DOM cards at
# every one-minute refresh.
MAX_TASKS = 1024
RECEIVER_LOCK_DIAGNOSTIC = '5623bb85bc595d58b57ed3f6fa950bf8747558818a84e1bfa360891da17b4191'
OPENING_ROUNDS_POLICY_SHA256 = '06050a3f40d7f2b5a62dcb459db88bdfceea97f079a45d410e213778e62e4288'
OPENING_REVIEWED_POLICY_SHA256 = '5a259a23ad96d6bcf3fec6d31969fed1b230e861df915efbc3b3488fa5a87ba7'
MONITOR_IMAGES_POLICY_SHA256 = 'e7a63f6df082fbea61bd7a42ca601526bf6d8f0a26b792a1f5eb88f443f114d1'
VAST_CAPACITY_TEST_POLICY_SHA256 = '7274ef8ebd3bfe288a8457799f1c99342e7160eac07863191466d717c0a3675d'
PROVIDER_FAILURE_REASONS = {'provider_source_drift', 'provider_namespace_registry'}
# The already-running owner was started before the inactivity watchdog. Its
# exact queue implementation had a 105-minute total child timeout. A new
# owner has a different implementation fingerprint and is not subject to it.
LEGACY_ARCHIVE_OWNER_WALL_LIMITS = {
    '3ba6b0d940b2eae39ac63cd0989f2b74b937eac2cb4be353b77e52d178a289ee': 105*60,
}
LABELS = {
    "ordinary_archive": "D／E · 每日增量合併與研究版本保留",
    "packed_archive_total": "D → E · 已交接封存累計",
    "packed_archive_placement": "D → E · 常用資料集封存分流",
    "d_mount_repair": "D 冷庫 · 掛載狀態與自動修復",
    "minute_validation_cache_rebind": "永豐分鐘 · 既有驗證快取續用",
    "bulk_return": "Vast 歷史封存回傳與原件驗證",
    "remote_return": "Vast → D · 訓練成果自動回傳",
    "vast_sealed_e_handoff": "WSL → E · Vast 固定救援封包搬移",
    "workspace_private_scratch": "WSL → E · 固定 SQLite 準備快照",
    "workspace_controls": "WSL → E · 固定準備副本",
    "provider_workspace": "WSL → E · 下載來源工作區",
    "workspace_source_staging": "WSL → E · 已登錄原始資料複製",
    "workspace_source_handoff": "WSL → E · 來源路徑交接",
    "source_local_candidate": "E 工作區 · 依來源目錄封裝候選",
    "source_local_promotion": "E → D · 資料集重新封裝發布",
    "source_local_repack_queue": "C／D → E → D · 資料集重新封裝排程",
    "source_local_dedup_legacy_cleanup": "E 工作區 · 已取代的未完成舊封裝回收",
    "source_repack_retirement": "D 冷庫 · 新封裝發布後的舊物件回收",
    "foreign_head_handoff": "Penguin ↔ Vast · 舊版本指標交接",
    "v3_reader_activation": "E 工作區 · 去重封裝讀取能力與啟用",
    "edge_reader_recovery": "Vast · 固定封裝讀取器監控與恢復",
    "cold_scan_notification": "D 冷庫 → Vast · 索引同步通知",
    "source_object_dedup": "D 冷庫 · 同內容封包去重",
    "private_subset_reuse": "D 冷庫 · 同來源資料集共用封包",
    "source_replication_pause": "來源備份複寫 · 搬遷期間暫停",
    "catalog_maintenance": "D 冷庫 · 資料集清冊登錄",
    "whole_root_refresh": "E → D · 資料來源每日增量追新",
    "handoff_source_repack": "C → E · 已交接來源封裝及舊副本回收",
    "handoff_private_d": "E → D · 私有來源新封裝發布",
    "handoff_private_retirement": "D 冷庫 · 私有舊封裝回收",
    "handoff_index_archive": "C → E · 不同世代來源封裝及舊副本回收",
    "offline_repair_archive": "C → E · 當沖歷史資料封裝",
    "offline_replay_pair": "C → E → D · 已確認舊當沖回放",
    "offline_repair_handoff": "WSL → E · 當沖歷史資料交接與回收",
    "offline_repair_D": "E → D · 當沖歷史資料冷庫發布",
    "handoff_index_coverage": "E → D · 既有期貨版本清冊比對",
    "handoff_index_D_storage": "E → D · 固定來源世代封裝",
    "handoff_index_D": "E → D · 不同世代來源整合",
    "fred_pin_recovery": "E → D · FRED 原始固定版本修復",
    "d_cold_e_handoff": "D → E · 已封裝物件分批搬移",
    "d_cold_e_total": "D → E · 暫無 release 引用物件的整體進度",
    "staged_cold_attribution": "E 暫存 · 舊冷庫物件來源比對",
    "workspace_symlink_closure": "WSL → E · 複製後連結目標核對",
    "workspace_cold_inventory": "E → D · 固定清冊差異比對",
    "futures_725_projection": "E 工作區 · 期貨 725 子資料集清冊",
    "futures_765_projection": "E 工作區 · 期貨 765 子資料集清冊",
    "workspace_cold_inventory_snapshot": "E → D · 固定時間清冊差異",
    "provider_originals": "WSL · E 交接後的 C 回復副本",
    "workspace_e_resize": "E 資料卷 · 擴容至 3 TiB",
    "workspace_c_compaction": "C 服務卷 · 前次 VHDX 離線維護",
    "workspace_transport": "D → E · 備份傳輸暫存搬移",
    "workspace_transport_comparison": "D → E · 已複製檔案差異比對",
    "workspace_transport_delta": "D → E · 舊版差異封包保全",
    "workspace_transport_restore": "D → E · 差異檔案還原演練",
    "workspace_partial_cold_readback": "D 舊傳輸暫存 · 冷庫正本讀回",
    "workspace_partial_name_restore": "E · 舊傳輸暫存同名準備",
    "workspace_transport_retirement": "D → E · 舊傳輸檔案逐檔回收",
    "workspace_worker_handoff": "E 工作區 · 儲存 worker 安全交接",
    "vast_spool": "Penguin → Vast · 封閉壓縮包預驗證",
    "registered_backup": "已登錄冷庫 · NAS 檔案驗證",
    "private_backup": "私有冷庫 · NAS 檔案驗證",
    "transport": "Penguin → lab203 同步通知",
    "transport_retirement": "已驗收傳輸副本回收",
    "lake_transport_retirement": "冷庫複寫 · 已驗收副本回收",
    "receiver": "lab203 → NAS 接收與獨立讀回",
    "lake_relay": "lab203 · 冷庫資料表還原驗證",
    "catalog_export": "D 冷庫 · 控制清冊封裝與交付",
    "migration": "D 槽冷庫 · 原始資料還原驗證",
    "migration_publication": "D 槽冷庫 · 來源清點與封存",
    "migration_dependency": "D 槽冷庫 · 待處理來源",
    "cooperative": "Penguin／Vast · 分工驗證 packs",
    "retained_verification": "沿用固定版本的驗證證據",
    "retirement": "WSL 安全回收",
    "encoding_stage_retirement": "D 槽 · 已退休版本的編碼副本回收",
    "lifecycle_backup": "控制資料庫 · 加密 NAS 備份",
    "local_testing_cleanup": "WSL · 已授權刪除的舊效能測試副本",
    "d_testing_cleanup": "D 槽 · 已授權刪除的舊效能試驗副本",
    "vast_testing_cleanup": "Vast · 已授權刪除的舊容量測試副本",
    "tw_rescue_original_manifests": "D 冷庫 · 台股原始續訓來源清冊",
    "training_rescue_capture": "Vast → D · 續訓 checkpoint 保全",
    "training_rescue_references": "續訓方案 · 固定來源保護登錄",
    "training_rescue_manifests": "D 冷庫 · 已登錄續訓來源清冊",
}
PHASES = {
    "ordinary_archive_wait": "等待每日 03:00–07:00 合併時段",
    "ordinary_archive_check": "核對原清冊、研究引用與可回收的舊版本",
    "archive_tier_totals": "累計已完成交接的封包，其他批次繼續排程",
    "archive_tier_check": "正在核對歷史封包清冊，尚未開始本批複製",
    "archive_tier_copy": "循序複製封包至 E，同時計算來源 SHA",
    "archive_tier_handoff": "登錄 E 位置並回收原 D 名稱",
    "archive_tier_wait": "等待資料集 owner、容量或讀取程式就緒",
    "archive_tier_done": "本批 E 封存交接完成",
    "offline_replay_hash": "核對固定舊回放並計算來源 SHA",
    "offline_replay_pack": "依原資料夾順序封裝至 E",
    "offline_replay_publish": "將新封包寫入 D 冷庫",
    "offline_replay_retire": "依固定清冊回收 C 上兩個硬連結名稱",
    "offline_replay_wait": "等待下一次排程或必要條件",
    "offline_replay_done": "D 已發布，C 舊回放副本已回收",
    "d_mount_monitoring": "每分鐘檢查 D 槽掛載及容量入口",
    "foreign_head_queue": "逐批核對遠端使用狀態與新舊版本",
    "foreign_head_peer_wait": "舊版本已受保護，等待 Vast 同步確認",
    "foreign_head_recovery": "等待原交接程序處理中斷交易",
    "foreign_head_release_pin": "遠端已確認，等待解除舊版保護",
    "v3_reader_check": "檢查 Vast 讀取程式並啟用固定 E 候選",
    "v3_reader_idle": "目前沒有新的去重封裝候選",
    "edge_reader_monitoring": "檢查固定讀取器與 v3 清冊",
    "edge_reader_recovery_waiting": "等待固定讀取器恢復",
    "cold_scan_notify": "依序通知同步器檢查固定版本",
    "remote_return_discovery": "搜尋已完成的遠端成果",
    "remote_return_existing": "檢查既有成果與回收條件",
    "remote_return_transfer": "傳送目前成果並準備封裝",
    "remote_return_publish": "將目前成果發布至 D 冷庫",
    "remote_return_confirmation": "檢查已發布成果的傳輸確認",
    "remote_return_e_prestage": "傳送固定成果至 E 暫存",
    "remote_return_e_check": "檢查固定成果的 E 暫存內容",
    "remote_return_waiting": "等待下一次成果回傳排程",
    "remote_return_failed": "成果回傳失敗，等待原排程重試",
    "spool_paused_user": "依目前遷移設定暫停額外遠端預驗證",
    "spool_removed_user": "已移除 Vast 舊備份及預驗證服務",
    "d_e_object_copy": "順序寫入 E；來源 SHA 同步計算",
    "d_e_object_handoff": "移除已交接的 D 舊名稱",
    "d_e_next_wave": "等待下一批現存無引用物件",
    "staged_cold_attributed": "比對既有清冊中的物件引用",
    "source_local_candidate_ready": "固定版本封裝並獨立還原完成",
    "source_local_candidate_building": "固定版本候選封裝與還原",
    "source_local_D_published": "以資料集為單位封裝並發布至 D",
    "source_local_repack_working": "封裝來源並提交新 D 版本",
    "source_local_E_packing": "將來源資料串流封裝到 E",
    "source_local_D_installing": "將 E 新封包寫入 D",
    "source_local_D_validating": "串流檢查新版本清冊",
    "source_local_source_loading": "載入原版本清冊",
    "source_local_source_validating": "檢查原版本清冊",
    "source_local_source_indexing": "建立封裝索引",
    "source_local_E_inventory": "寫入 E 封包清冊",
    "source_local_dedup_preparing": "準備固定版本的內容去重候選",
    "source_local_dedup_indexing": "核對內容索引與原版本清冊",
    "source_local_dedup_packing": "每份唯一內容讀取一次並封裝至 E",
    "source_local_dedup_fallback": "原生選取讀取暫時不可用，改用既有封包讀取繼續",
    "source_local_dedup_inventory": "寫入保留全部路徑的候選清冊",
    "source_local_dedup_compressing": "壓縮並封閉候選清冊",
    "vast_sealed_copy": "依固定清冊串流複製並核對來源 SHA",
    "vast_sealed_copy_ready": "E 已複製，等待原路徑交接與 C 回收",
    "vast_sealed_retirement": "E 路徑已交接，回收原 C 封包",
    "vast_sealed_completed": "E 路徑交接與原 C 封包回收完成",
    "source_local_dedup_sealed": "E 去重候選已封裝，等待讀取與發布契約",
    "source_local_dedup_v3_validating": "核對去重封包的完整路徑清冊",
    "source_local_dedup_v3_ready": "去重格式清冊已準備，等待正式交接",
    "source_local_dedup_legacy_cleanup": "回收 E 上被完整封包取代的未完成舊封裝",
    "recovery_deferred_user_migration": "已固定版本；依目前遷移設定暫緩額外還原",
    "source_local_prepared_history": "E 封裝完成，檢查並回收未使用的舊版本",
    "source_local_D_committing": "寫入新版本清冊並切換 D head",
    "source_local_E_prepared": "E 封裝完成，等待 D 空間後寫入並切換",
    "source_local_E_local_ready": "E 暫時資料集已就緒；標準來源尚未切換",
    "source_local_E_first_preparing": "E 已封裝；正在核對舊 D 版本的引用與使用狀態",
    "source_local_E_first_retiring": "E 暫存中，逐批回收舊 D 物件以便寫回",
    "source_local_E_first_install_waiting": "舊 D 物件已回收；等待將 E 封裝寫回 D",
    "source_local_E_prepared_ready": "E 封裝完成，已排隊寫入 D",
    "source_replication_pause_waiting": "等待原 worker 載入暫停設定",
    "source_replication_paused": "新來源備份複寫已暫停",
    "catalog_maintenance_check": "清點並登錄目前冷庫清冊",
    "whole_root_refresh_check": "依來源根目錄安排增量發布",
    "handoff_source_packing": "讀取固定 C 舊來源並封裝到 E",
    "handoff_source_retiring": "回收 C 舊來源，保留服務目錄與鎖",
    "handoff_source_prepared": "E 封裝完成，等待 C 來源回收",
    "handoff_source_retired": "C 舊來源已回收，E 封裝保留",
    "handoff_source_queued": "等待排程封裝及回收",
    "handoff_source_D_waiting": "E 新封裝等待發布到 D 冷庫",
    "handoff_source_D_installing": "順序寫入 D 冷庫並更新資料集版本",
    "handoff_source_D_validating": "串流檢查新版本清冊",
    "handoff_source_D_cleanup": "D 新版已發布，回收 E 封裝暫存",
    "handoff_source_D_done": "D 新版已發布，E 封裝暫存已回收",
    "handoff_source_D_reused": "D 既有版本已覆蓋固定來源；E 封裝暫存已回收",
    "handoff_source_D_direct_reused": "D 既有版本已覆蓋固定來源，無須另建 E 封包",
    "handoff_private_old_waiting": "等待舊版引用與研究版本檢查",
    "handoff_private_old_retiring": "檢查引用並回收私人舊封裝",
    "handoff_private_old_done": "未使用的私人舊封裝已回收",
    "handoff_index_D_waiting": "固定 C 世代已封裝到 E，等待與 D 資料集整合",
    "handoff_index_D_archived": "固定來源世代已保存 D，等待與現行資料集整合",
    "handoff_index_comparing": "正在比較固定來源世代與現行資料集清冊",
    "handoff_index_integration_waiting": "等待固定來源比對或封裝整合排程",
    "handoff_index_D_parent_scan": "掃描 D 父版本清冊，尋找固定子資料集",
    "handoff_index_reading_fixed_private": "依封裝順序讀取 D 固定私人版本",
    "handoff_index_reading_fixed_public": "依封裝順序讀取 D 固定公開版本",
    "handoff_index_comparing_fixed_members": "比較固定版本中的資料差異",
    "fred_pin_building": "由既有固定資料樹重建原封包",
    "fred_pin_installing": "將摘要相同的原封包寫回 D",
    "fred_pin_restored": "原固定版本已可由原 pin 解析",
    "fred_pin_waiting": "等待原發布鎖或下一次修復",
    "handoff_index_compared": "清冊比較完成，仍待處理資料差異",
    "handoff_index_equal_pending": "固定清冊內容一致；仍待版本整合與舊物件回收",
    "handoff_index_public_integrated": "公開資料世代已整合；私人來源仍另行保留",
    "handoff_index_public_exact_equal": "兩份清冊及檔案摘要一致，已採用共用封裝",
    "handoff_index_public_fixed_covered": "現行公開版涵蓋固定來源，並另有新增資料",
    "handoff_index_public_latest_selected": "依快速遷移設定採用較新來源；舊資料差異未逐筆比較",
    "handoff_index_C_retired_D": "C 舊來源已回收，固定來源世代已保存 D",
    "source_local_repack_waiting": "等待固定來源封裝執行者",
    "source_repack_retirement_working": "核對版本引用與執行逐物件回收",
    "source_repack_retirement_unlink": "依固定計畫回收舊名稱",
    "source_repack_history_unlink": "依固定計畫回收本批歷史名稱",
    "source_repack_retirement_wait_writer": "已保留下個回收時段，等待原寫入完成",
    "source_shared_object_working": "等待或執行固定封包共用",
    "source_shared_object_completed": "同內容封包已共用",
    "private_subset_checking_target": "核對原資料集清冊",
    "private_subset_checking_donor": "核對共用來源清冊",
    "private_subset_comparing": "比較路徑、原始雜湊、權限及連結",
    "private_subset_preparing": "等待原 owner 或準備清冊",
    "private_subset_published": "共用封包的新資料集版本已發布；舊版本回收另計",
    "source_latest_retirement_working": "核對引用並回收較舊版本；歷史值差異尚未比較",
    "source_repack_history_working": "清理未保留的歷史版本與未共用物件",
    "scratch_copy": "複製固定快照至 E",
    "scratch_readback": "獨立讀回 E 並核對 SHA",
    "scratch_binding": "核對引用、綁定並回收 WSL 準備副本",
    "scratch_completed": "固定準備快照已移至 E",
    "workspace_inventory": "建立固定搬移清冊；總量待清點",
    "workspace_copy": "複製並獨立讀回完整 SHA",
    "workspace_source_stream_copy": "來源單次讀取並寫入 E；進度可含寫入中資料，未提交不算完成",
    "workspace_source_finalizing": "檔案已串流寫入，等待命名空間及持久化檢查完成",
    "workspace_directory_preparing": "建立 E 端完整資料夾集合",
    "workspace_source_stream_staged": "來源串流及 E 寫入完成；尚未獨立讀回目的端",
    "workspace_seed_copy": "複製原始檔至 E；精確讀回計數尚未開始",
    "workspace_seed_recheck": "重新核對先前已複製檔案",
    "workspace_source_namespace": "核對固定來源的完整檔案集合",
    "workspace_directory_metadata": "套用目標資料夾的權限與時間",
    "workspace_destination_namespace": "核對 E 目的地的完整檔案集合",
    "workspace_source_copy_verified": "這個固定來源世代已完整複製及讀回",
    "workspace_link_wait": "等待固定來源完成複製；再核對連結目標",
    "workspace_link_verified": "固定來源內的連結目標已對應至 E 副本",
    "workspace_cold_wait": "等待 E 完整複製後比對固定 D 清冊",
    "workspace_cold_compare": "比對 E 固定清冊與 D 固定版本",
    "workspace_cold_compare_tail": "已掃描 E 清冊，繼續核對 D 其餘名稱",
    "workspace_cold_compared": "固定世代的清冊差異已比對",
    "futures_725_indexing": "從既有固定索引分批建立子資料集清冊",
    "futures_725_rechecking": "續跑前核對已提交的清冊批次",
    "futures_725_sealing": "固定子資料集索引的身分",
    "futures_725_projected": "固定子資料集索引已建立",
    "workspace_handoff": "核對原 owner／引用並切換原服務路徑",
    "source_handoff_initial": "首次逐檔核對 C 來源及 E 副本 SHA",
    "source_handoff_preexchange": "交接前再次逐檔核對 C 來源及 E 副本 SHA",
    "source_handoff_namespace": "核對來源及 E 副本的完整檔案清冊",
    "source_handoff_fence_install": "建立來源與 E 資料夾變動監測",
    "workspace_link_no_links": "固定來源清冊無符號連結；無須連結目標核對",
    "source_handoff_generation": "依既有完整複本收據核對來源與 E 檔案世代",
    "source_handoff_unenrolled": "固定 E 複本已備妥；來源交接尚未登錄",
    "source_handoff_root_changed": "來源根目錄自 E 複製後已改變，交接尚未登錄",
    "handoff_source_namespace": "核對 C 來源檔案清冊",
    "handoff_source_indexing": "整理 E 封裝清冊",
    "handoff_source_inventory": "寫入 E 封裝清冊",
    "handoff_source_E_prepared": "E 封裝完成，等待原排程回收 C 副本",
    "offline_repair_E_prepared": "E 封裝已完成；C 回收與 D 發布另列",
    "offline_repair_C_retired": "C 副本已回收，歷史路徑指向 E",
    "handoff_index_coverage_check": "比對 E 封裝清冊與 D 固定版本",
    "handoff_index_coverage_parent_scan": "掃描 D 父版本清冊，尋找固定子資料集",
    "handoff_index_coverage_ready": "固定 D 版本清冊比對完成；後續發布另列",
    "handoff_index_coverage_wait": "等待 E 封裝或原清冊比對程序",
    "handoff_source_retirement_plan": "建立或核對 C 舊檔回收清冊",
    "provider_workspace_completed": "已完成 E 複製、讀回及原路徑交接；C 原件仍保留",
    "source_handoff_retired": "原路徑已交接至 E；C 舊資料已封裝並回收",
    "provider_originals_pending": "E 交接已完成；C 副本的回收須另行驗收",
    "lake_relay_semantic_hash": "原始資料表語義讀回與完整列雜湊",
    "windows_storage_pending": "等待 Windows 管理員離線維護",
    "windows_storage_running": "原生磁碟維護與原服務恢復檢查",
    "windows_storage_verified": "磁碟維護及原生讀回完成",
    "windows_storage_failed": "前次磁碟維護未通過完整核對",
    "windows_storage_capacity_observed": "E 現行容量已量測；舊維護收據仍待核對",
    "minute_validation_cache_rebinding": "依固定清冊續用舊驗證快取",
    "minute_validation_cache_rebound": "本世代可續用的快取已處理",
    "workspace_retirement": "回收已核對 E 副本的 D 舊傳輸暫存",
    "workspace_comparison": "讀取 E 副本並與固定清冊逐檔比對 SHA",
    "workspace_comparison_done": "本輪逐檔差異比對完成",
    "workspace_delta_copy": "將固定差異檔順序封裝至 E",
    "workspace_delta_readback": "獨立讀回 E 差異封包並逐檔驗證",
    "workspace_delta_done": "固定差異檔的 E 封包已驗證",
    "workspace_delta_restore": "在 E 順序重建差異檔並獨立讀回",
    "workspace_delta_restore_done": "差異檔內容與中繼資料還原演練完成",
    "partial_cold_verified": "固定暫存所對應的 D 物件已完整 SHA 讀回",
    "partial_name_pending": "等待原傳輸執行者取得鎖",
    "partial_name_restoring": "從已驗證封包逐檔恢復 E 暫存並讀回",
    "partial_name_restored": "E 暫存名稱與內容已恢復並讀回",
    "workspace_retirement_done": "固定清冊的舊 D 傳輸檔案已回收",
    "workspace_completed": "E 傳輸路徑切換及 D 舊副本回收完成",
    "workspace_transport_files_retired": "E 傳輸路徑已接手；舊檔案已回收，空目錄保留",
    "workspace_worker_wait": "等待原發布工作釋放完整交接鎖",
    "workspace_worker_ready": "新版本 worker 已載入，原工作流程保留",
    "checking_exact_targets": "清點固定效能測試副本",
    "checking_consumers": "檢查現行程序、服務及引用",
    "waiting_artifact_owner": "等待原產物回收執行者",
    "discarding_exact_target": "隔離並刪除已授權的測試副本",
    "waiting_protected_targets": "其餘副本保留；等待引用解除或錯誤處理",
    "disposable_cleanup_completed": "固定效能測試副本清理完成",
    "rescue_original_manifest_audit": "檢查原始續訓固定版本清冊",
    "rescue_capture_waiting_capacity": "D 容量未達 checkpoint 保全預留",
    "rescue_capture_cycle_observed": "觀察原 checkpoint 保全排程",
    "rescue_references_reconciled": "登錄原始及歷史續訓固定來源",
    "opening_perf_cleanup_working": "逐檔檢查引用並回收舊效能測試輸出",
    "waiting_accepted_transport": "目前沒有待回收的已驗收副本",
    "waiting_retirement_retry": "保留上次驗證失敗原因；等待原排程重試",
    "admit_paired_nas_recovery": "核對配對 NAS 獨立還原收據",
    "verify_and_retire_transport": "核對來源、副本與引用後回收傳輸副本",
    "enumerate_transport": "清點固定傳輸副本",
    "hash_transport_native": "批次計算大型傳輸檔案 SHA",
    "hash_transport": "逐檔核對傳輸內容 SHA",
    "observe_primary_sources": "讀取原始來源檔案世代",
    "hash_primary_sources": "批次計算原始來源 SHA",
    "verify_primary_sources": "逐檔核對原始來源 SHA 與世代",
    "inspect_transport_members": "核對精確副本集合與檔案世代",
    "check_process_references": "檢查程序及檔案引用",
    "unlink_verified_transport": "刪除已通過原驗收的傳輸副本",
    "remove_transport_directories": "清理已回收副本的空目錄",
    "sealed_pack_staging": "傳輸封閉壓縮包並預先核對原值",
    "waiting_new_sealed_packs": "目前候選已預驗證；等待來源封閉下一包",
    "preparing_source": "準備來源與固定控制資料",
    "source_semantic_validation": "來源語義檢查：已核對或可重用的分鐘分區",
    "bootstrapping_public_objects": "準備可重用的公開來源物件",
    "discovering_sqlite_states": "清點需要固定的 SQLite 控制狀態",
    "snapshotting_sqlite_state": "固定使用中的 SQLite 狀態",
    "finalizing_sqlite_snapshot": "整理 SQLite 快照",
    "checking_sqlite_snapshot": "檢查 SQLite 快照結構",
    "hashing_sqlite_snapshot": "計算 SQLite 快照識別",
    "publishing_sqlite_state": "封裝並發布 SQLite 快照",
    "freezing_mutable_controls": "固定仍可變更的控制資料",
    "censusing_source": "清點來源檔案與大小；尚未驗證內容",
    "loading_previous_inventory": "讀取前一版本的固定清冊",
    "validating_previous_inventory": "核對前一版本的固定清冊",
    "routing_and_hashing": "核對來源內容並重用既有物件",
    "packing_changed_small_files": "壓縮新增或變更的小檔案",
    "verifying_complete_file_storage": "核對完整檔案的儲存綁定",
    "computing_portable_cohort_fingerprint": "整理來源清冊與計算版本識別",
    "counting_inventory_object_references": "整理清冊中的封包引用",
    "committing_complete_inventory": "提交完整來源清冊",
    "verifying_observation_cohort_stability": "核對固定觀測範圍的穩定性",
    "verifying_source_stability": "核對來源在處理期間保持一致",
    "source_fences_verified": "來源檢查完成；等待版本提交及冷庫還原",
    "nas_file_readback": "NAS 配對檔案讀回與驗證",
    "scan_notification": "等待同步器處理封閉批次",
    "relay_readback": "接收、備份與獨立還原並行處理",
    "catalog_export": "固定清冊封裝與原生檔案核對",
    "catalog_copy": "複製固定清冊資料並核對可重用檔案",
    "dataset_reconstruction": "逐資料集驗證原始資料可還原",
    "verify-owned-preparation-scratch": "核對本次作業的準備暫存檔",
    "check-owned-scratch-process-references": "核對準備暫存是否仍有程序使用",
    "unlink-owned-preparation-scratch": "回收已驗證的本次準備暫存檔",
    "owned-preparation-scratch-retired": "準備暫存已回收；等待還原收據",
    "owned-preparation-scratch-waiting": "準備暫存仍有程序使用",
    "source_dependency": "等待來源條件或修復",
    "source_refresh_trading_wait": "來源追新暫緩，等待當沖優先時段結束",
    "source_capacity_recheck": "等待原排程重新檢查空間與來源",
    "encode-and-check-receipts": "壓縮原檔並核對逐檔收據",
    "pack_reconstruction": "驗證封閉 pack 的原始內容",
    "waiting_preflight": "等待原回收程序與前置檢查",
    "capacity-preflight": "等待實體容量足夠後自動開始",
    "independent-D-original-recovery-for-quarantine": "由 D 槽獨立還原並核對原檔",
    "full-quarantine-resume-audit": "核對隔離原檔與封存內容",
    "verify-materialized-files": "逐檔核對還原內容",
    "decode-originals": "解碼並核對原始檔案",
    "load-packed-inventory": "讀取固定封存清冊",
    "validate-packed-inventory": "核對固定封存清冊的各項成員",
    "hash-cold-blobs": "核對冷庫原始物件的 SHA",
    "capture-cold-object-generations": "記錄冷庫物件驗證前的檔案世代",
    "capture-cold-chunk-generations": "記錄冷庫區塊驗證前的檔案世代",
    "capture-cold-blob-generations": "記錄冷庫原始物件驗證前的檔案世代",
    "recheck-cold-chunk-generations": "再次核對冷庫區塊的檔案世代",
    "capture-retained-pack-generations": "記錄已驗證封閉包的目前檔案世代",
    "revalidate-completed-pack-bytes": "重新讀取已驗證封閉包並核對 SHA",
    "recheck-retained-pack-generations": "再次核對封閉包的檔案世代",
    "materialize-cold-archive": "從封存重建原始檔案",
    "reconstruct-original-pack-files": "解碼封閉包並逐檔核對原值",
    "verify-cold-objects": "核對冷庫物件",
    "reconstruct-original-files": "還原原始檔案",
    "retire-verified-originals": "回收已驗證原檔",
    "retire-verified-transport": "回收已有 NAS 驗收及保留來源的傳輸副本",
    "prune-verified-encoding-stage": "回收已驗證編碼暫存",
    "retired": "已通過原回收程序驗收",
    "working": "原程序處理中；等待階段收據",
    "wait-for-transport-convergence": "等待同步收斂；完整驗證有效期限仍持續計時",
    "wait-for-retirement-owner": "等待原回收執行者釋放鎖",
    "wait-for-publication-owner": "等待原冷庫發布執行者釋放鎖",
    "wait-for-artifact-process-references": "等待程序解除檔案引用",
    "verify-and-retire-hot-source": "核對來源世代、引用與回收條件後回收",
    "quarantine-before-independent-audit": "隔離來源，準備獨立完整驗證",
    "prepare-exact-source-archive": "整理固定原始來源封存",
    "publish-to-D": "發布固定版本到 D 槽冷庫",
    "publish-under-shared-owner": "由原發布執行者寫入固定版本",
    "independent-D-original-recovery": "由 D 槽獨立還原原始資料",
    "full-source-mirror-D-retirement-audit": "核對來源、鏡像與 D 槽版本的回收條件",
    "plan-encoding-stage-prune": "核對編碼暫存回收清冊",
    "apply-encoding-stage-prune": "回收已驗收的編碼暫存",
    "verify-encoding-stage-files": "逐檔核對編碼暫存內容",
    "reconstruct-encoding-stage-files": "從冷庫還原並核對編碼暫存",
    "verify-encoding-stage-receipts": "核對編碼暫存的逐檔收據",
    "verify-encoding-stage-metadata": "核對編碼暫存的檔案身分與引用",
    "retirement-proof-reconciliation": "核對固定版本與原完成收據",
    "control_sql_dump": "固定資料庫交易快照並備份",
    "control_sql_delivery": "等待控制資料庫備份的 NAS 配對驗收",
}
PRUNE_PHASES = {
    "plan-initial-stage-fingerprint": "verify-encoding-stage-metadata",
    "plan-verify-archive": "verify-encoding-stage-files",
    "plan-reconstruct-cold": "reconstruct-encoding-stage-files",
    "plan-check-receipts": "verify-encoding-stage-receipts",
    "plan-final-stage-fingerprint": "verify-encoding-stage-metadata",
    "apply-before-rename-fingerprint": "verify-encoding-stage-metadata",
    "apply-after-rename-fingerprint": "verify-encoding-stage-metadata",
    "apply-verify-quarantine-archive": "verify-encoding-stage-files",
    "apply-reconstruct-cold": "reconstruct-encoding-stage-files",
    "apply-reconstruct-expired-cold-proof": "reconstruct-encoding-stage-files",
    "apply-final-stage-fingerprint": "verify-encoding-stage-metadata",
    "apply-unlink-quarantine": "apply-encoding-stage-prune",
}
REASONS = {
    'ordinary_archive_window': '一般資料每日 03:00–07:00 合併；研究固定版本與使用中的來源繼續保留。',
    'ordinary_archive_protected': '舊版本含目前版本沒有的原始路徑，或仍有研究／服務引用；保留並等待原來源補齊。',
    'ordinary_archive_retry': '原回收執行者保留錯誤證據；此資料集等待重試，其他資料集繼續輪流處理。',
    'ordinary_archive_owner_busy': '原封裝／備份執行者仍持有引用保護；回收器等待其提交，其他資料集繼續輪轉，不重掃負載或強制解鎖。',
    'archive_retirement_turn': '已讓原回收執行者優先取得下一輪；既有波次保留原進度，回收輪次結束後自動繼續搬移。',
    'archive_reader_rollout': '固定讀取程式尚未載入，保留 D 原物件並自動重試',
    'archive_tier_wait': '等待原資料集 owner 或下一批排程；原件與中斷紀錄保留',
    'archive_tier_failed': '本批分流尚有錯誤，原始原因保留於本機收據，由原排程重試',
    'archive_tier_capacity': 'E 槽容量或保留空間不足，保留原件並等待空間回收',
    'catalog_owner_busy': '清冊原執行者仍在工作，稍後自動重試',
    'catalog_cold_capacity': 'D 槽空間不足，保留上次清冊並自動重試',
    'catalog_refresh_failed': '清冊登錄失敗，原錯誤保留在服務日誌並由原排程重試',
    'catalog_cycle_complete': '上次清冊登錄完成，等待下一次檢查',
    'root_refresh_due': '已有來源到達每日追新時間，原資料集 owner 會依序處理',
    'root_refresh_scheduled': '目前沒有已到期的來源，依各來源完成時間安排下一次追新',
    'root_refresh_ambiguous': '部分來源沒有唯一的完整根目錄 owner；保留原資料並繼續其他來源',
    'incremental_source_incomplete': 'E 工作區缺少冷庫既有檔案；保留原冷庫版本，等待補齊或以增量合併方式處理',
    'incremental_base_missing': '原固定冷庫版本不存在或已改變；保留增量，等待核對來源後重試',
    'latest_successor_history_not_compared': '已依原 owner 的交接紀錄續用目前冷庫版本；舊歷史值未逐項比較，不能認定相同',
    "d_mount_healthy": "D 入口與冷庫主掛載的身分及 8 KiB 設定正常；持續觀察。",
    "d_mount_recurring": "每分鐘持續檢查掛載狀態；這是週期性工作，沒有整體完成時間。",
    "d_mount_busy": "D 入口仍被程序使用或原 owner 忙碌；不強制卸載，排程會再試。",
    "d_mount_trading": "偵測到掛載待修復，當沖優先時段結束後再處理。",
    "d_mount_deferred": "掛載核對或修復尚未完成；保留冷庫主掛載並等待原程序重試。",
    "edge_reader_monitoring": "固定讀取器可讀取 v3 清冊；下一輪持續觀察。",
    "edge_reader_disabled": "固定讀取器恢復排程目前停用。",
    "edge_reader_remote_unavailable": "目前無法確認 Vast 讀取器，原排程會再觀察。",
    "edge_reader_base_missing": "Vast 尚未具備原本的 edge owner 或狀態，先恢復該既有基礎服務。",
    "edge_reader_identity_changed": "遠端程式、控制指標或版本身分不符；不自動覆蓋其他版本。",
    "edge_reader_runtime_wait": "遠端程式檔完整，但 v3 清冊或執行環境目前無法確認；持續重試觀察。",
    "edge_reader_trading_deferred": "固定讀取器需要恢復；當沖優先時段持續觀察，恢復作業於時段結束後再執行。",
    "edge_reader_bundle_invalid": "E 端固定程式包不可用或身分不符；恢復前保留現有遠端狀態。",
    "edge_reader_retry": "讀取器恢復尚未完成；固定版本與原 owner 保留，下一輪重試。",
    "repack_lab203_head_wait": "舊版本目前由 lab203 指標引用；E 封裝已就緒，等待該節點核對並交接後再進行 D 容量回收。",
    "repack_exclusive_space": "E 封裝已就緒，但本批扣除共用或仍有其他連結的物件後，可回收的 D 空間不足；其他合格版本會繼續回收，容量改變後自動重試。",
    "cold_scan_retry_error": "部分固定版本的同步通知失敗，原排程保留佇列並自動重試。",
    "cold_scan_owner_busy": "同步器正在處理上一輪，原排程稍後再檢查。",
    'foreign_head_peer_pending': '舊版仍有保護 pin；確認遠端已同步後才交給原回收程序',
    'foreign_head_recovery_pending': '原交接程序會核對交易與舊版保護後繼續',
    'foreign_head_release_pin_pending': '遠端確認已持久記錄，原程序完成交接後再交給回收程序',
    'v3_reader_unavailable': 'Vast 讀取程式或固定版本清冊無法確認，原排程會重試',
    'v3_reader_no_pending': '目前沒有可啟用的新候選；後續來源仍由原排程檢查',
    'handoff_archive_legacy_timeout': '目前這輪使用舊版固定總時限；到期會由原排程續跑，完成時間需重新估算。',
    'handoff_index_coverage_owner_busy': 'D 的原發布程序正在使用共用鎖；清冊比對會自動重試。',
    'remote_return_connection_failed': '遠端連線或遠端作業失敗；請檢查原回傳服務的私有診斷，排程會再重試',
    'remote_return_failed': '成果回傳工作失敗；請檢查原回傳服務的私有診斷，排程會再重試',
    'remote_return_release_unavailable': '有待確認成果的固定版本及其可驗證後繼版本無法取得；保留該筆並輪詢其他工作',
    'remote_return_capacity': '成果回傳暫存空間不足，等待容量恢復後重試',
    'remote_return_confirmation_capacity': '待確認的成果已達原佇列上限，先處理既有確認',
    'user_paused_extra_verification': '依目前遷移設定暫停此預驗證；舊的工作計數僅供參考，重新啟用後才可估算',
    'user_removed_vast_verification': '已依要求移除 Vast 舊備份及預驗證排程；訓練及現行成果回傳保留',
    'source_or_head_mismatch': '固定來源與原資料集版本不一致，已跳過此資料集並繼續其他工作',
    'source_head_advanced': '已封裝的舊世代保留在 E；資料集已有新世代，先排新世代並等待舊世代釐清',
    'repack_source_inventory_missing': 'D 原始版本缺少清冊，暫停此版本並稍後重試，其他資料集繼續',
    'repack_foreign_head_wait': 'E 已封裝；舊版本仍是另一發布節點的現行 head，等待來源交接，避免 D 暫時存兩份',
    'rescue_original_manifest_missing': '原始續訓規格的一個或多個固定版本清冊目前不在本機 D；來源尚待查找，不能以新版本代替',
    'rescue_original_manifest_mismatch': '原始續訓規格的固定版本清冊身分不符；保留現有資料並核對來源',
    'rescue_registered_manifest_missing': '已登錄續訓來源中有固定版本清冊目前不在本機 D；來源身分受保護，仍需查找副本',
    'rescue_registered_manifest_mismatch': '已登錄續訓來源的固定版本清冊身分不符，需核對原始版本',
    'rescue_capture_D_capacity': 'D 的可用空間低於 checkpoint 保全設定的預留量；原排程會重試，既有遠端成果保留',
    'repack_temporary_failure': '封裝暫時無法執行，排程稍後重試',
    'repack_D_capacity': 'D 冷庫不足以容納這個資料集的新封裝及寫入保留空間，排程先處理其他資料集',
    'repack_retirement_turn': 'D 空間吃緊，先讓舊版本回收取得工作時段；E 封裝保留，之後自動繼續寫回。',
    'retirement_turn_waiting_writer': '目前的 D 寫入可繼續；本輪最多等待 10 分鐘，這不是回收完成時間。',
    'retirement_turn_interrupted': '回收時段的執行程序已不在，保留原 journal 並由原排程重新檢查。',
    'source_duplicate_bloat': '重複內容已交由原排程建立去重封裝，完成後自動接回 D 安裝；目前來源與 E 封裝持續保留，其他資料集繼續',
    'subset_pending_turn': '等待這個資料集的下一個排程時段。',
    'subset_owner_busy': '原資料集或版本 owner 正在執行，稍後自動重試。',
    'subset_lifecycle_wait': '原資料集的日更版本尚未對上現行 head；先保留所有版本並重試。',
    'subset_donor_unproven': '共用來源封包的目前檔案世代與原收據不同；保留原版並持續核對。',
    'subset_capacity_wait': 'E 工作區或 D 冷庫的寫入條件尚未滿足；稍後自動重試。',
    'subset_source_mismatch': '固定版本或來源清冊不符；保留原版，其他資料集繼續。',
    'subset_not_covered': '供應版本未完整涵蓋目標的所有路徑或內容；等待任一固定版本更新後再比對。',
    'subset_error': '清冊比較或來源身分暫時無法核對；保留原版並重試。',
    'dedup_candidate_reader_pending': 'E 去重候選已封裝；正式讀取、還原與發布契約尚未接通，原 D 來源保留',
    'dedup_candidate_handoff_pending': 'E 去重格式清冊已核對；正式安裝、遠端讀取與來源回收尚待交接，原 D 來源保留',
    'dedup_candidate_interrupted': '去重候選工作目前未執行；已封存分包保留，排程會重新嘗試',
    'dedup_candidate_error': '去重候選暫時無法處理；原資料與已封存分包保留，排程會重新嘗試',
    'repack_E_first_install_waiting': '原 D 版本暫時不完整；已封裝的 E 資料保留，原排程將寫回 D',
    'repack_E_first_owner_busy': 'E 封裝保留；另一項原工作正使用 D 寫入鎖，完成後排程會優先寫回',
    'repack_E_first_interrupted': '舊 D 物件回收尚未結束；E 封裝保留，排程會先續跑回收再寫回 D',
    'relief_pinned': '這個版本仍被研究或服務固定；排程保留資料並稍後重試',
    'private_authority_unavailable': '私有冷庫暫時無法讀取；其他資料集會繼續，排程稍後重查',
    'private_head_unavailable': '私有資料集的版本身分暫時無法讀取；來源保留，排程稍後重查',
    'private_dependency_contract_unknown': '發現尚不認識的私有版本依賴，保留來源並等待修正',
    'relief_reader_busy': '原 D 物件仍被程序使用；排程會稍後重試',
    'insufficient_exclusive_space': '可回收的專屬 D 空間仍不足以容納新封裝；E 封裝保留',
    'waiting_E_only_priority': '先讓暫時只在 E 的資料集寫回 D；本工作會在其交接完成後自動繼續',
    'repack_source_integrity': '舊來源在串流處理時的內容雜湊不符，保留來源並讓其他資料集繼續',
    'source_replication_pause_pending': '已要求暫停新備份複寫；原 worker 尚在等待安全交接，未確認已暫停',
    'handoff_source_waiting': 'E 已封裝；原交接或使用程序仍忙碌，排程會再次嘗試回收 C 舊來源',
    'handoff_source_queued': '等待前一個已登錄資料集完成，或等待排程再次嘗試',
    'handoff_source_retry': '上一輪處理未完成；排程會等待後重試，其他資料集仍可先處理',
    'offline_replay_consumer_busy': '舊回放仍被程式、服務或研究使用；保留原檔並自動重試',
    'offline_replay_capacity': 'E 或 D 可用容量不足；保留來源與已封裝內容，排程會重試',
    'handoff_source_owner_busy': '原 owner 或來源鎖正忙；保留封包及原檔，下一次排程會繼續回收',
    'handoff_source_external_hardlinks': '來源檔案有範圍外 hardlink；封包與原路徑均保留，需先確認共用連結的回收方式',
    'handoff_source_D_unpublished': 'E 已封裝、C 舊副本已回收；新版尚未發布到 D，原 D 版本仍保留',
    'handoff_private_D_retry': 'D 發布上一輪未完成，排程會重試；容量不足或鎖忙時，E 封裝與原 D 版本會保留',
    'handoff_private_old_owner_busy': '私人冷庫原有工作正在使用共用鎖；排程會等待鎖釋放後重試',
    'handoff_private_old_retry': '舊封裝的引用或固定版本檢查尚未完成，排程會再次嘗試',
    'handoff_index_D_waiting': 'E 封裝的來源世代不同於 D 現行版本；D 整合流程尚未完成，不可把它視為 D 新版',
    'handoff_index_D_archived': '固定來源世代已存入 D 的獨立私人區；尚未與公開資料集現行版本合併',
    'handoff_index_compared': '清冊差異已列入工作清單；同路徑內容不同的資料仍需依資料集規則合併，尚未完成整合',
    'handoff_index_binance_union_required': '兩個固定版本各含不同資料；需依資料集規則合併後才能整合，已停止重試無法保留全部路徑的發布方式',
    'handoff_index_equal_pending': '固定清冊的名稱及內容摘要一致；版本及物件引用仍需原 owner 處理',
    'handoff_index_generation_changed': '公開版本或資料集設定已變動；停止重選舊來源，等待重新釐清差異。',
    'handoff_index_binding_unavailable': '目前無法核對 D 現行版本或資料集設定；保留既有收據，稍後自動再查。',
    'staged_cold_unresolved': '僅辨識既有清冊引用；其餘物件及資料集身分仍待確認，E 暫存持續保留',
    'staged_cold_partial': '部分搬移批次尚未核對；下列數字只是已清點範圍，總數仍未知，E 暫存持續保留',
    'retirement_current_head': '舊版本仍是現行 head；暫時保留並繼續其他批次',
    'handoff_index_D_capacity': '上次嘗試時 D 可用空間不足；E 封裝持續保留，排程會再嘗試',
    'handoff_index_D_owner_busy': 'D 封裝寫入 owner 正在執行其他資料集；本批會自動重試',
    'handoff_index_integration_owner_busy': '來源比對或整合 owner 正在處理其他工作；本批會自動重試',
    'retirement_pinned': '舊版本仍被研究或服務保留，暫不回收此版本',
    'retirement_metadata_retry': '舊版本引用或回收記錄尚無法核對，排程保留資料並稍後重試',
    'repack_timeout': '封裝超時，來源保留並等待重試',
    'source_local_candidate_unverified': '候選尚未通過固定版本及獨立還原核對；不授權 D 切換或回收',
    'd_e_handoff_interrupted': '此批中斷或條件不符；保留 E 與現有 D 名稱，其他獨立批次可繼續',
    'd_e_owner_busy': '原發布或搬移執行者忙碌，排程會再次嘗試',
    'retirement_source_replication_busy': '來源複寫 owner 仍在使用冷庫；回收排程會再次嘗試',
    'retirement_backup_cycle_busy': '原備份週期 owner 仍在使用冷庫；回收排程會再次嘗試',
    'staging_queued': '已排入搬移佇列，等待工作槽位或下一輪；開始時間以原程序回報為準',
    'original_owner_or_reader': '原複製擁有者或來源讀取者仍在使用；保留原檔並自動重試',
    'workspace_native_bridge_unavailable': 'Windows 容量檢查暫時無法連線；保留原檔並自動重試',
    'workspace_capacity_or_reservation': 'E 工作區容量或原保留鎖尚未允許寫入；原排程會重試',
    'workspace_capacity': 'E 實體或工作卷可用空間不足；保留來源並等待空間恢復',
    'workspace_reservation_evidence_invalid': 'E 容量保留記錄無法確認，保留來源並等待核對',
    'workspace_identity_or_source_invalid': 'E 工作卷或來源身分未通過檢查，保留來源並等待核對',
    'source_or_copy_recheck_required': '來源世代、複製或既有證據需重新核對；保留原檔',
    'futures_D_head_changed': 'D 現行版本已改變；保留歷史比對紀錄，排程將比對新版本',
    'futures_D_head_unavailable': '目前無法確認 D 現行版本；歷史比對不能當成目前覆蓋證據',
    'futures_symlink_comparison_pending': '正補做符號連結目標的清冊比對，原有檔案比對紀錄仍保留',
    'namespace_readback_mismatch': '來源跨程序掛載讀回不一致；保留原件，由原排程重新核對',
    'bulk_return_failed': '原回傳程序保留來源並等待錯誤排查',
    'scratch_owner_busy': '原資料集作業仍在執行，原鎖釋放後重試',
    'scratch_capacity': 'E 實體或工作區可用容量不足，保留原副本並重試',
    'scratch_reference': '原準備副本仍有程序引用，解除後重試',
    'scratch_error': '固定快照核對或搬移失敗，保留來源並由原排程重試',
    'worker_reload_failed': '儲存 worker 交接或版本驗收失敗；保留原錯誤並由原排程有限重試',
    'heterogeneous_workloads': '已處理與剩餘項目的大小差異過大，暫時無法可靠外推完成時間',
    "retirement_source_changed": "來源檔案世代與原收據不同；原回收程序會核對可重驗的 metadata 差異，內容不符時保留副本",
    "retirement_file_missing": "回收所需的來源或傳輸檔案缺失；保留副本並等待原流程補齊",
    "retirement_process_references": "仍有程序引用這批傳輸副本；引用解除後由原排程接續",
    "retirement_transport_changed": "傳輸副本的內容、世代或檔案集合與原驗證不符；保留副本等待核對",
    "retirement_validation": "原回收驗證未通過；已保留詳細失敗收據，由原排程重試",
    "transport_not_converged": "同步尚未收斂或有錯誤；原排程會重新檢查，條件通過後自動接續",
    "deletion_scan_pending": "副本回收後的刪除通知等待同步器；原排程會補送，不會重新刪除原始資料",
    "retirement_retry": "原回收程序已排定重試；每分鐘更新等待狀態，尚未到實際重試時間",
    "metadata_unavailable": "目前無法讀取有效的原程序收據",
    "receipt_stale": "保留上次收據；等待新的進度觀測",
    "progress_pending": "本次已檢查；等待原工作回報新計數，完成時間暫停估算",
    "clock_ahead": "來源時鐘超前，暫不計算速度與完成時間",
    "sampling": "同一階段的速度取樣中",
    "no_counter": "原程序尚未提供這個階段的完整工作量",
    "no_recent_progress": "近期未觀測到進度增加，等待原程序回報",
    "forecast_overdue": "原預估時間已過，等待新的實際進度重新估算；不能僅憑此判斷作業停滯",
    "dependency": "仍有前置條件，無法可靠預估開始或完成時間",
    "recovery_deferred_user_migration": "依使用者目前的快速遷移設定，保留固定版本並暫緩額外還原；這不是還原完成。",
    "vast_sealed_handoff_pending": "E 複製已完成；原路徑尚未交接、C 原件尚未回收。",
    "vast_sealed_copy_retry": "固定封包複製尚未完成；保留原件並由排程重試。",
    "source_link": "原始來源連結無效，等待原產生器修復",
    "source_changed": "上次來源在封裝時發生變更；保留副本並等待原程序重新核對",
    "source_coordination_previous": "上次嘗試等待來源寫入鎖逾時；原排程會重試，舊紀錄不能證明目前仍鎖定",
    "source_native_bridge_previous": "上次 Windows 與 WSL 的 E 槽容量查詢失敗；原資料保留並等待重試，舊紀錄不代表目前掛載失效",
    "source_exact_pin_missing": "原固定版本清冊目前找不到；保留版本引用並等待查找原始副本",
    "source_exact_pin_repaired_retry": "原固定版本修復收據已對上來源，等待原排程重新嘗試；這不代表來源發布已完成",
    "source_sqlite_binding_previous": "上次發布引用的 SQLite 固定版本已換代；原排程將核對精確封裝收據後修復引用",
    "source_dependencies_repaired_retry": "固定版本引用已修復，等待原來源排程接續；上次失敗紀錄保留，尚未完成新的來源發布",
    "refresh_priority_boundary": "預估跨過當沖優先時段；來源更新將先讓出，待接續後重新估算",
    "fred_pin_retry": "原固定版本尚未完整恢復；保留來源與既有 pin，排程將自動重試",
    "source_access_during_handoff": "交接核對期間偵測到來源或 E 檔案被開啟或變動；保留原檔並由原排程重試",
    "source_workspace_mount": "上次來源工作區掛載未通過檢查；原程序需重新確認目前掛載後重試",
    "source_predecessor": "等待前一份來源完成原遷移程序",
    "source_publication_owner_busy": "上次嘗試時冷庫發布 owner 正忙；原排程會在工作空檔重試，記載的時間不是保證開始時間。",
    "upstream_publication": "目前已發現的封閉包已完成預驗證；新包封閉後由原排程自動接續，整份來源是否完成仍以原驗收為準",
    "original_owner": "由原程序執行；下次檢查時間不是開始時間",
    "tw_day_trade_priority_window": "大型來源核對配合當沖優先時段；盤後才開始，開始時間不是完成時間",
    "transport_placement_priority": "原傳輸路徑或工作者版本正在等待安全交接；先接收既有收據，交接後由原排程接續新批次，舊 D 副本回收另計",
    "retirement_identity": "固定清冊、冷庫版本或完成收據不符，保留原失敗證據等待修復",
    "owner_retry_limit": "原程序已達重試上限，保留失敗證據；修復後由原排程接續",
    "receiver_readiness": "接收端就緒回執已過期，等待原 owner 更新；保留有效期限及容量檢查",
    "receiver_readiness_lock_diagnosis": "接收端就緒回執仍待原程序更新；最近一次診斷曾遇到 Restic 排他鎖，目前鎖狀態仍待接收端確認",
    "lake_relay_memory_exhausted": "接收端回報 DuckDB 記憶體不足，固定資料表的完整語義雜湊尚未通過；保留原收據並等待原 owner 修復",
    "provider_workspace_handoff": "複製與獨立讀回已完成，等待原搬移作業的路徑／namespace 交接收據；C 原件仍保留，原服務恢復另驗",
    "source_handoff_unenrolled": "已有固定 E 複本，但此來源尚未納入交接器；需核對目前來源、寫入者與服務引用，現有計時器不會自動切換。",
    "source_handoff_root_changed": "E 複本完成後來源根目錄的檔案世代已改變；需更新固定來源並協調寫入者後才能交接，現有計時器不會自動切換。原複製量不是尚未交接的剩餘量，此觀測也不判定每個檔案的差異。",
    "provider_originals_pending": "目前只確認 C 回復入口的存在狀態，尚無本輪 C 回收驗收；由原資料 owner 核對還原、引用與刪除資格，不套用 E 複製完成時間",
    "windows_storage_administrator": "固定維護入口已準備；須在 Windows 管理員 PowerShell 執行，期間暫停已登錄 WSL，完成後恢復原服務。分鐘觀測不會自行停機或代替維護驗收。",
    "windows_storage_failed": "原 Windows 維護尚未通過完整容量／服務恢復讀回；保留原狀態，需核對原生維護收據。",
    "windows_storage_e_capacity_observed": "目前已量到固定 E 資料卷為 3 TiB；先前 Windows 維護與服務恢復的收據仍待核對，兩者分開計算。",
    "provider_source_drift": "原搬移工作未通過來源清冊或世代核對；檔案計數達標不代表驗收通過。既有副本保留，等待原接手器重核差異",
    "provider_namespace_registry": "原搬移子程序在舊 namespace 讀取登錄資訊失敗；複製收據保留，等待已排定的新接手器完成路徑交接",
    "transport_capacity": "傳輸暫存已達在途容量邊界，等待原 ACK 與暫存回收",
    "transport_retirement_unverified": "E 傳輸路徑已切換；D 舊副本尚未全部完成回收，請查看逐檔回收進度與保留原因",
    "transport_empty_root_pending": "固定清冊的舊 D 檔案已回收、空目錄保留；等待新的路徑狀態觀測。這不是 NAS 驗收。",
    "transport_stale_mount_namespace": "部分既有服務仍保留舊 D 傳輸掛載；原排程每分鐘重新檢查，尚未回收舊副本",
    "transport_namespace_unavailable": "無法完整檢查舊傳輸路徑或程序掛載，保留舊副本並等待下次檢查",
    "transport_consumer_alias": "Syncthing 的實際傳輸掛載尚未確認指向 E，保留 D 舊副本",
    "staging_capacity": "編碼／複製暫存空間不足，等待原暫存處理及回收",
    "physical_capacity": "實體磁碟寫入餘量不足，保留原服務及驗證空間",
    "capacity_recheck_due": "最新 D 槽可用空間已高於上次檢查門檻；等待原排程重新檢查其他條件。",
    "finalizing": "計數已到目標，等待這個階段的正式驗收",
    "scope_incomplete": "只涵蓋已登錄固定版本；不代表全部專案歷史",
    "remaining_phases": "後續階段尚無完整工作量或速度，總完成時間待估",
}
PREVIOUS_SOURCE_FAILURES = {
    'io_error': '上次來源處理曾遇到 I/O 錯誤；空間恢復後仍需重新檢查，不能認定錯誤已解除。',
    'native_command_rejected': '上次控制清冊封裝的 Windows 原生讀取遭拒；目前仍需成功的封裝收據。',
    'catalog_export_other': '上次控制清冊封裝未完成；原程序保留失敗證據，仍需成功的封裝收據。',
}
BOOT_AUDIT_FAILURES = {
    'finmind_inactive': 'FinMind 下載服務有未運行項目',
    'finmind_disabled': '原稽核列出的 FinMind 常駐服務目前停用；有限任務的執行狀態需另看原排程',
    'lakehouse_receipts_stale': '儲存控制回執過期',
    'lakehouse_worker_version_unverified': '儲存工作者載入版本尚未核對一致',
    'lakehouse_queries_unverified': '儲存工作流程查詢尚未驗證',
}
PUBLICATION_FAILURES = {
    'stale_official_symbol_build_receipt': '官方逐檔價格建置收據與來源不一致，需由原建置器更新後再發布',
    'stale_feature_build_receipt': '特徵建置收據與來源不一致，需由原建置器更新後再發布',
}
SCOPES = {
    'ordinary_archive': '只整理已登錄完整來源目錄的本機普通歷史；保留研究固定版本、原引用與現行服務。完成一次檢查不代表全部遷移或 NAS 驗收。',
    'packed_archive_total': '依原位置索引顯示目前仍在 E 的已交接封包，以及歷來 D 名稱配置回收量。E 舊副本後續回收不會扣回已釋放的 D 空間；不是整個資料集已遷移或 NAS 驗收。物件按最初交接資料集計一次。',
    'packed_archive_placement': '只搬移已封存的常用資料集；本批進度不代表全部遷移完成。D 保留原清冊，E 保存原 SHA 封包；不解壓、不宣稱 NAS 驗收。',
    'd_mount_repair': '僅觀察並在空閒時修復 D 入口掛載；不重新啟動當沖、不執行 TRIM，也不代表資料已驗收。',
    'edge_reader_recovery': '只核對並在缺失時恢復固定程式與 v3 清冊讀取能力；不下載或驗證市場內容，也不代表 D 還原或 NAS 驗收。',
    'cold_scan_notification': '這是 D 端掃描通知佇列及最近一輪數量；通知被接受不表示 Vast 已同步內容或 NAS 已驗收。新增版本會使佇列增加。',
    'foreign_head_handoff': '只交接已在 D 發布的新舊固定版本指標；等待期間保護舊版。指標交接不代表 D 物件已回收或 NAS 驗收。',
    'v3_reader_activation': '核對遠端固定讀取程式及 v3 清冊後，啟用 E 已封好的候選；不表示 D 已安裝、內容已讀回或 NAS 已驗收。',
    'minute_validation_cache_rebind': '只調整固定來源驗證快取的裝置編號；原驗證器仍核對目前檔案世代。這不是分鐘資料完成、來源交接或冷庫驗收。',
    'remote_return': '僅顯示原成果回傳 owner 的最近一次掃描及當前階段；發布 D、遠端原件保留及 NAS 驗收各自獨立。',
    "source_replication_pause": "只反映來源複寫的暫停要求及 worker 回報；不代表其他備份工作或既有 NAS 收據已變更。",
    "catalog_maintenance": "只顯示原 worker 的清冊執行及等待狀態；心跳不是已處理資料量、獨立還原或 NAS 驗收。",
    "whole_root_refresh": "每個已登錄 data 根目錄選一個不含排除範圍的原 owner；統計是本輪嘗試前的排程觀測，不是全部資料完整、NAS 驗收或已刪除。",
    "handoff_source_repack": "只處理已交接的固定 C 舊來源；E 封裝時核對原始 SHA，保留服務目錄及鎖。尚不代表新 D 封裝或 NAS 驗收。",
    "handoff_private_d": "私人資料集由 E 封裝寫入 D 的原有私人儲存區，成功發布後回收 E 封裝；不代表 NAS 驗收。",
    "handoff_private_retirement": "僅回收已有新版的私人 D 舊編碼；保留研究固定版本及其他清冊仍使用的物件。",
    "handoff_index_archive": "依固定原始清冊將 C 世代封裝到 E，並保留現行服務的來源鎖。",
    "offline_repair_archive": "封裝固定歷史修復候選並串流核對來源 SHA；這不代表 WSL 原件已回收、D 已發布或 NAS 已驗收。",
    "offline_replay_pair": "僅處理已確認無現行服務或研究引用的固定舊回放；保留現行當沖帳本，D 與 E 寫入時核對來源，未新增獨立 D 讀回或 NAS 驗收。",
    "offline_repair_handoff": "原歷史路徑改指向 E，並逐檔回收原 C 副本；不是當沖現行帳本，也不代表 NAS 已驗收。",
    "offline_repair_D": "固定 E 封包經原有私人冷庫發布器寫入 D；不另做目的端獨立讀回或聲稱 NAS 驗收。",
    "handoff_index_coverage": "只比對 E 與固定 D 版本的名稱、模式、大小、SHA 和連結；不解壓資料、不刪來源，也不是 D／NAS 獨立讀回。",
    "handoff_index_D_storage": "將固定來源世代保存至 D；若既有版本清冊相符則重用該版本，否則發布私人封裝。完成後回收 E 暫存，不會變更公開資料集 head；清冊比對不是 D／NAS 獨立讀回。",
    "handoff_index_D": "固定來源世代與 D 公開資料按資料集整合；本機限定來源及舊封包回收另列。",
    "fred_pin_recovery": "僅從既有固定資料樹重建與原 pin 摘要完全一致的封包及清冊；不改現行版本或刪除來源，也不是 NAS 驗收。",
    "staged_cold_attribution": "只讀既有清冊，比對 E 暫存物件身分；不讀取來源資料內容、不代表完整資料集或授權刪除。",
    "source_local_candidate": "僅限指定固定版本的 E 候選；不是 D 新版本、NAS 驗收或來源回收授權。",
    "source_local_promotion": "依發布收據顯示此固定資料集；未獨立讀回 D 或 NAS，舊版可能仍被服務或研究使用。",
    "source_local_repack_queue": "包含已登錄來源與分批輪巡的其他 D 資料集；本批不是整庫清冊。來源與新封裝在處理時核對，未獨立讀回 D 或 NAS；失敗項目不阻塞其他資料集。",
    "source_local_dedup_legacy_cleanup": "只計算被同一固定世代的完整 E 封包取代的未完成舊 E 封裝；不代表 WSL、D 或 NAS 已回收，也不代表整個資料集遷移完成。",
    "source_repack_retirement": "回收已串流發布資料集的舊 D 名稱及未保留歷史版本；歷史修訂可能不再可還原。保留現行版本、研究 pin 與共用物件，未獨立讀回 D 或 NAS。",
    "source_object_dedup": "依既有來源雜湊與固定版本收據共用 D 封包，保留原始收據與交易紀錄；未另行讀回 D，也不是 NAS 驗收。",
    "private_subset_reuse": "僅共用同一來源資料夾且清冊逐項一致的私人封包；維持各資料集身分。發布與舊檔回收分開，未另行讀回 D 或驗收 NAS。",
    "d_cold_e_handoff": "僅計算清冊中目前沒有 release 引用的物件；E 保留原封裝。未做額外備份、解壓或 E 獨立讀回，這不是全部資料集的遷移進度。",
    "d_cold_e_total": "依本輪清冊與已交接紀錄估計；可能隨清冊變動。僅涵蓋目前無 release 引用的物件，不是全部資料集的遷移進度。",
    "bulk_return": "固定歷史封存回傳的原程序；狀態不代表全部 Vast 資料或 NAS 已驗收",
    "vast_sealed_e_handoff": "只處理已封閉的 Vast 救援資料；複製時核對來源 SHA，完成後才計 WSL 原件回收，不代表 Windows C 實體空間增加或新的 D／NAS 讀回。",
    "workspace_private_scratch": "只遷移一份固定 SQLite 準備快照；不代表 D 冷庫或 NAS 已驗收",
    "workspace_controls": "只搬移原作業的固定控制及準備副本；不代表 D 冷庫或 NAS 已驗收",
    "provider_workspace": "只含本輪清冊選定的來源工作區；排除子樹不計入。E 完整複製與原路徑交接不代表 C 原件已回收、全部歷史齊全或 NAS 驗收；原服務恢復另驗。",
    "workspace_source_staging": "依各來源指定契約複製固定世代；部分僅在串流時計算來源 SHA。此處不代表原路徑已切換、C 已回收或 D／NAS 已備份。",
    "workspace_source_handoff": "依已登錄來源的路徑及回收收據，分別顯示 E 交接與 C 舊副本狀態；不代表 D／NAS 已驗收。",
    "workspace_symlink_closure": "只核對固定複製世代的連結文字和內部目標映射；不是新的原始內容讀回、來源切換、D／NAS 驗收或回收授權。",
    "workspace_cold_inventory": "只對照固定 E 與 D 清冊內的檔案名稱、大小、SHA 及已列入範圍的連結目標；不是 D 實際原值讀回、NAS 驗收或來源回收授權。",
    "futures_725_projection": "僅投影既有固定副本的清冊，不讀原始資料內容、不切換來源路徑、不刪 C，也不表示 D／NAS 驗收。",
    "futures_765_projection": "僅投影既有固定副本的清冊，保留已登錄連結；不讀原始資料內容、不切換來源路徑、不刪 C，也不表示 D／NAS 驗收。",
    "workspace_cold_inventory_snapshot": "僅對照該時間點固定 E 與 D 清冊的名稱、大小及 SHA；整檔差異不是區塊去重後容量，也不是 D 讀回、NAS 驗收或 C 回收授權。",
    "provider_originals": "只檢查已登錄 C 回復入口，不掃描內容、不授權刪除。E 複製邏輯量不是 C 實際配置量或可回收量；排除子樹仍依原來源合約保存。",
    "workspace_e_resize": "核對固定 E 資料卷目前的裝置及檔案系統容量；不代表先前 Windows 維護、服務恢復或來源遷移已完成。",
    "workspace_c_compaction": "顯示前次 C VHDX 離線維護的原始收據；這是歷史維護狀態，不表示目前資料遷移被它阻塞或需要執行線上 TRIM。",
    "workspace_transport": "只搬移備份傳輸暫存到 E；D 主冷庫、來源觀測與 NAS 原件不刪除，不代表全部來源已遷移或 NAS 驗收",
    "workspace_transport_comparison": "固定搬移清冊與 E 副本的讀取時比對；差異與缺檔保留，結果不授權刪除，也不是 NAS 驗收",
    "workspace_transport_delta": "只保全固定清冊中不同或缺少的舊 D 檔案，E 封包不是 D 冷庫或 NAS 驗收，亦不授權刪除",
    "workspace_transport_restore": "逐個重建 E 差異封包中的檔案並讀回驗證；不代表整棵資料樹、D 冷庫或 NAS 已驗收",
    "workspace_partial_cold_readback": "對固定舊傳輸暫存所對應的 D 物件進行完整 SHA；歷史讀回不授權刪除暫存，也不是 NAS 驗收。",
    "workspace_partial_name_restore": "只準備固定 E 暫存供原回收程序重新檢查；不表示 D 回收完成或 NAS 已驗收。",
    "workspace_transport_retirement": "只回收固定清冊中的舊 D 傳輸副本；保留舊目錄、來源觀測、D 冷庫和 NAS，與 WSL 釋放量分開計算",
    "workspace_worker_handoff": "只核對 E 切換後的 worker 載入版本與持久工作流程；等待觀測不會更新資料進度或 NAS 驗收時間",
    "local_testing_cleanup": "使用者明確授權直接刪除的固定舊測試副本；保留精簡結果，不代表冷庫或 NAS 驗收，外部連結目標及正式資料不刪除",
    "d_testing_cleanup": "只刪固定舊效能試驗的工作副本；比較結果、正式冷庫物件與 NAS 保留，這不是備份驗收。",
    "vast_testing_cleanup": "只計算三組指定 Vast 舊容量測試副本的實際回收；這不是 WSL、D 或 NAS 的回收或驗收。",
    "tw_rescue_original_manifests": "僅檢查這份原始台股續訓規格的固定 D manifest 及其 SHA；不檢查內容物、其他版本、E／NAS 副本、runtime 或模型能否續訓。",
    "training_rescue_capture": "只顯示原續訓保全 owner 的單次排程及 D 容量限制；不代表遠端已停止、最新 checkpoint 已回傳或可成功續訓。",
    "training_rescue_references": "保護已登錄原始規格與歷史修訂的固定來源身分；來源可能仍然缺失，不代表資料物件、還原或 NAS 已驗收。",
    "training_rescue_manifests": "僅核對累積登錄的續訓來源在本機 D 的固定 manifest 與 SHA；不檢查物件、E／NAS 副本或是否可續訓。",
    "vast_spool": "已封閉物件的暫存與原值計算；完整清冊待原驗證程序綁定，不代表 NAS 驗收或可刪除來源",
    "transport_retirement": "本輪固定已驗收傳輸副本的 logical bytes；不是 WSL 原始資料釋放量",
    "lake_transport_retirement": "原冷庫複寫回收程序的即時階段與重試；與備份封套回收分開，不代表 WSL 原檔已刪除",
    "registered_backup": "已登錄固定冷庫檔案；不含尚未發布來源，不代表完整歷史還原",
    "private_backup": "已登錄私有冷庫版本；與其他冷庫清冊可能重疊，不加總",
    "transport": "同步通知與待掃描批次；不代表接收或 NAS 驗收",
    "receiver": "lab203 當次管線與接收佇列；空佇列不代表完整備份",
    "lake_relay": "lab203 原冷庫 relay 的固定工作失敗觀測；先前檔案讀回與資料表語義驗收分開，不代表 NAS 完整驗收或來源可刪除。",
    "catalog_export": "每分鐘查詢原工作流程的控制清冊封裝；狀態不是內容進度、D 還原或 NAS 驗收。",
    "migration": "已登錄固定 D 槽版本的原始資料還原；不代表 NAS 或來源已刪除",
    "migration_publication": "本次來源清點、內容核對與封存；清點大小不是傳輸量，不代表冷庫還原完成或可刪除來源",
    "migration_dependency": "已登錄來源的前置問題；沿用原遷移程序重試",
    "cooperative": "pack 原始內容驗證；與資料集還原進度重疊，不加總",
    "retained_verification": "原還原收據保留的固定版本驗證證據；不是本次重新計算，不代表 NAS 或來源已刪除",
    "retirement": "原回收程序的固定清冊；只計已驗收的 WSL 實際釋放量",
    "encoding_stage_retirement": "固定退休版本的臨時編碼副本；由原程序完整核對後回收，不是 WSL 原檔或冷庫正本釋放量",
    "lifecycle_backup": "原 SQL 備份執行者的容量准入及交付狀態；交付不代表 NAS 已驗收，不授權刪除來源",
}
TIMERS = {
    "stockagent-wsl-retirement-queue.timer": ("retirement", "WSL 回收佇列"),
    "stockagent-private-cold-backup.timer": ("private_backup", "私有冷庫備份"),
    "stockagent-backup-stream.timer": ("registered_backup", "冷庫備份與收據對帳"),
    "stockagent-backup-notify.timer": ("transport", "封閉批次同步通知"),
    "stockagent-cold-tiers.timer": ("cold_tiers", "增量區併入冷區"),
    "stockagent-lake-transport-gc.timer": ("transport_gc", "已驗收傳輸暫存回收"),
    "stockagent-remote-cold-artifact-ingress.timer": ("remote_return", "Vast 產物回傳"),
    "stockagent-lifecycle-control-backup.timer": ("lifecycle_backup", "生命週期控制與 SQL 備份"),
    "stockagent-control-backup.timer": ("control_backup", "程式、設定與控制資料備份"),
    "stockagent-control-release-verification.timer": ("release_verification", "已登錄程式版本還原驗證"),
    "stockagent-d-cold-scan-retry.timer": ("cold_scan", "D 槽封閉版本同步重試"),
    "stockagent-packed-transport.timer": ("packed_transport", "封閉冷庫傳輸與對帳"),
    "stockagent-data-cache-gc.timer": ("data_cache_gc", "已登錄工作快取回收"),
    "stockagent-storage-pressure.timer": ("storage_pressure", "儲存壓力與安全容量檢查"),
    "stockagent-enrolled-artifact-retirement.timer": ("artifact_retirement", "已登錄產物安全回收"),
    "stockagent-vast-bulk-return.timer": ("bulk_return", "Vast 固定歷史封存回傳"),
    "stockagent-d-root-mount-repair.timer": ("d_mount_repair", "D 入口掛載每分鐘檢查"),
    "stockagent-legacy-return@main.timer": ("legacy_main_return", "Vast 歷史主資料回傳"),
    "stockagent-legacy-return@partitions.timer": ("legacy_partition_return", "Vast 歷史分區回傳"),
    "stockagent-legacy-return@reviewed-main-20261006.timer": ("reviewed_main_return", "Vast 已清點主資料回傳"),
    "stockagent-legacy-return@reviewed-panel-20261006.timer": ("reviewed_panel_return", "Vast 已清點 panel 回傳"),
    "stockagent-tw-public-cold-publish.timer": ("public_cold_publish", "台股公開來源冷庫發布"),
    "stockagent-shioaji-storage-monitor.timer": ("shioaji_storage", "永豐來源儲存與缺口觀測"),
    "stockagent-boot-recovery-audit.timer": ("boot_recovery", "開機恢復與原服務就緒稽核"),
    "stockagent-training-rescue.timer": ("training_rescue", "Vast 訓練中斷救援與 checkpoint 保全"),
    "stockagent-artifact-dedup.timer": ("artifact_dedup", "已登錄產物去重檢查"),
    "stockagent-openbb-l1-compaction.timer": ("openbb_compaction", "OpenBB 已登錄來源分區整理"),
    "stockagent-legacy-us-cold-publication-20261001.timer": ("legacy_us_publish", "已固定美股歷史冷庫發布"),
    "stockagent-pack-compute-spool.timer": ("vast_spool", "Vast 封閉壓縮包暫存與預驗證"),
    'stockagent-transport-relocation.timer': ('workspace_transport', 'D 傳輸暫存搬移 E；原鎖空閒時續作'),
    'stockagent-private-scratch-relocation.timer': ('workspace_private_scratch', '固定 SQLite 準備快照搬移 E'),
    'stockagent-private-controls-relocation.timer': ('workspace_controls', '固定控制及準備副本搬移 E'),
    'stockagent-workspace-source-staging.timer': ('workspace_source_staging', '已登錄來源複製至 E'),
    'stockagent-shioaji-validation-cache-rebind.timer': ('minute_validation_cache_rebind', '永豐分鐘固定驗證快取續用'),
    'stockagent-offline-repair-e-archive.timer': ('offline_repair_archive', '歷史修復候選依來源封裝至 E'),
    'stockagent-offline-replay-pair-migration.timer': ('offline_replay_pair', '已確認舊回放依來源封裝與回收'),
    'stockagent-offline-repair-c-handoff.timer': ('offline_repair_handoff', '歷史修復候選交接 E 及 C 回收'),
    'stockagent-opening-performance-cleanup.timer': ('opening_perf_cleanup', '舊效能測試副本回收'),
    'stockagent-workspace-seed-observation.timer': ('workspace_seed_observation', 'E 複製清單每分鐘取樣'),
    'stockagent-workspace-source-handoff.timer': ('workspace_source_handoff', '已登錄來源路徑交接及恢復'),
    'stockagent-workspace-symlink-closure.timer': ('workspace_symlink_closure', '固定來源複製後連結目標核對'),
    'stockagent-futures-e-cold-inventory.timer': ('workspace_cold_inventory', '期貨固定 E／D 清冊差異比對'),
    'stockagent-futures-725-index-projection.timer': ('futures_725_projection', '期貨 725 子資料集索引投影及續跑'),
    'stockagent-futures-765-index-projection.timer': ('futures_765_projection', '期貨 765 子資料集索引投影及續跑'),
    'stockagent-catalog-export-observer.timer': ('catalog_export_observer', '控制清冊封裝狀態每分鐘觀測'),
    'stockagent-d-benchmark-discard.timer': ('d_testing_cleanup', 'D 槽固定舊效能試驗副本回收'),
    "stockagent-d-cold-e-handoff.timer": ("d_cold_e_handoff", "D → E 冷物件分批交接"),
    "stockagent-source-local-repack.timer": ("source_local_repack_queue", "E → D 資料集封裝排程"),
    "stockagent-packed-object-placement.timer": ("packed_archive_placement", "常用封存分批搬移至 E"),
    "stockagent-prepared-stream-install.timer": ("prepared_stream_install", "已封裝資料優先寫入 D 排程"),
    "stockagent-dedup-candidate.timer": ("dedup_candidate", "E 資料集內容去重候選排程"),
    "stockagent-vast-sealed-e-handoff.timer": ("vast_sealed_e_handoff", "Vast 救援封包 C → E 排程"),
    "stockagent-source-local-retirement.timer": ("source_repack_retirement", "D 舊封裝回收排程"),
    "stockagent-private-subset-reuse.timer": ("private_subset_reuse", "同來源私人封包共用排程"),
    "stockagent-foreign-head-handoff.timer": ("foreign_head_handoff", "Vast 固定版本指標交接排程"),
    "stockagent-foreign-head-recovery.timer": ("foreign_head_recovery", "固定版本交接確認與中斷復原"),
    "stockagent-v3-reader-activation.timer": ("v3_reader_activation", "去重封裝讀取能力及候選啟用"),
    "stockagent-edge-reader-recovery.timer": ("edge_reader_recovery", "Vast 固定讀取器監控及缺失恢復"),
    "stockagent-handoff-source-repack.timer": ("handoff_source_repack", "C 舊來源封裝及回收排程"),
    "stockagent-handoff-private-d.timer": ("handoff_private_d", "E 私人封裝發布至 D 排程"),
    "stockagent-handoff-private-retirement.timer": ("handoff_private_retirement", "D 私人舊封裝回收排程"),
    "stockagent-handoff-index-archive.timer": ("handoff_index_archive", "C 不同世代來源封裝排程"),
    "stockagent-handoff-index-prepack.timer": ("handoff_index_prepack", "C 不同世代來源並行封裝排程"),
    "stockagent-handoff-index-coverage.timer": ("handoff_index_coverage", "既有期貨 D 版本清冊比對排程"),
    "stockagent-handoff-index-d.timer": ("handoff_index_D_storage", "固定來源世代 E → D 排程"),
    "stockagent-handoff-index-integration.timer": ("handoff_index_D", "固定來源世代差異比較排程"),
    "stockagent-fred-exact-pin-recovery.timer": ("fred_pin_recovery", "FRED 原固定版本修復排程"),
    "stockagent-staged-cold-attribution.timer": ("staged_cold_attribution", "E 舊冷庫物件清冊比對"),
}
CONTINUOUS = {"stockagent-storage-lifecycle.service": ("lakehouse", "冷庫發布、還原與來源複寫")}
SCHEDULE_LABELS = dict((*TIMERS.values(), *CONTINUOUS.values()))
# These installed owners use the same 120-minute runway and 13:30 gate.
# Each observation also verifies the exact current ExecCondition and calendar.
PRIORITY_GUARDED_SCHEDULES = frozenset({
    'offline_repair_archive', 'offline_repair_handoff', 'opening_perf_cleanup', 'd_cold_e_handoff',
    'source_local_repack_queue', 'prepared_stream_install', 'dedup_candidate', 'vast_sealed_e_handoff',
    'source_repack_retirement', 'private_subset_reuse', 'foreign_head_handoff', 'foreign_head_recovery', 'v3_reader_activation',
    'handoff_source_repack', 'handoff_private_d', 'handoff_private_retirement', 'handoff_index_archive',
    'handoff_index_prepack', 'handoff_index_coverage', 'handoff_index_D_storage', 'handoff_index_D',
    'staged_cold_attribution', 'fred_pin_recovery',
})
TASK_SCHEDULES = {
    'ordinary_archive': ('source_repack_retirement',),
    'packed_archive_total': ('packed_archive_placement',),
    'packed_archive_placement': ('packed_archive_placement',),
    'catalog_maintenance': ('lakehouse',),
    'whole_root_refresh': ('lakehouse',),
    'd_mount_repair': ('d_mount_repair',),
    'edge_reader_recovery': ('edge_reader_recovery',),
    'cold_scan_notification': ('cold_scan',),
    'foreign_head_handoff': ('foreign_head_handoff', 'foreign_head_recovery'),
    'v3_reader_activation': ('v3_reader_activation',),
    'training_rescue_capture': ('training_rescue',),
    'training_rescue_references': ('training_rescue',),
    'remote_return': ('remote_return',),
    'd_cold_e_handoff': ('d_cold_e_handoff',),
    'd_cold_e_total': ('d_cold_e_handoff',),
    'source_local_repack_queue': ('prepared_stream_install', 'dedup_candidate', 'source_local_repack_queue'),
    'source_local_dedup_legacy_cleanup': ('dedup_candidate',),
    'source_repack_retirement': ('source_repack_retirement', 'source_local_repack_queue'),
    'source_object_dedup': ('source_repack_retirement', 'source_local_repack_queue'),
    'private_subset_reuse': ('private_subset_reuse',),
    'handoff_source_repack': ('handoff_source_repack',),
    'handoff_private_d': ('handoff_private_d',),
    'handoff_private_retirement': ('handoff_private_retirement',),
    'handoff_index_archive': ('handoff_index_archive', 'handoff_index_prepack'),
    'offline_repair_archive': ('offline_repair_archive',),
    'offline_replay_pair': ('offline_replay_pair',),
    'offline_repair_handoff': ('offline_repair_handoff',),
    'offline_repair_D': ('handoff_index_D_storage',),
    'handoff_index_coverage': ('handoff_index_coverage',),
    'handoff_index_D_storage': ('handoff_index_D_storage',),
    'handoff_index_D': ('handoff_index_D',),
    'fred_pin_recovery': ('fred_pin_recovery',),
    'staged_cold_attribution': ('staged_cold_attribution',),
    'workspace_source_staging': ('workspace_source_staging',),
    'minute_validation_cache_rebind': ('minute_validation_cache_rebind',),
    'workspace_source_handoff': ('workspace_source_handoff',),
    'workspace_symlink_closure': ('workspace_symlink_closure',),
    'workspace_cold_inventory': ('workspace_cold_inventory',),
    'futures_725_projection': ('futures_725_projection',),
    'futures_765_projection': ('futures_765_projection',),
    'bulk_return': ('bulk_return',),
    'vast_sealed_e_handoff': ('vast_sealed_e_handoff',),
    'workspace_private_scratch': ('workspace_private_scratch',),
    'workspace_controls': ('workspace_controls',),
    'workspace_transport': ('workspace_transport',),
    'workspace_transport_comparison': ('workspace_transport',),
    'workspace_transport_delta': ('workspace_transport',),
    'workspace_transport_restore': ('workspace_transport',),
    'workspace_partial_name_restore': ('workspace_transport',),
    'workspace_transport_retirement': ('workspace_transport',),
    'workspace_worker_handoff': ('workspace_transport',),
    'registered_backup': ('registered_backup',), 'private_backup': ('private_backup',),
    'transport': ('transport',), 'transport_retirement': ('registered_backup', 'private_backup'),
    'lake_transport_retirement': ('transport_gc',),
    'migration': ('lakehouse',), 'migration_publication': ('lakehouse',),
    'catalog_export': ('lakehouse', 'catalog_export_observer'),
    'migration_dependency': ('lakehouse',), 'cooperative': ('lakehouse',),
    'retained_verification': ('lakehouse',), 'retirement': ('retirement',),
    'local_testing_cleanup': ('retirement',),
    'd_testing_cleanup': ('d_testing_cleanup',),
    'vast_spool': ('vast_spool',),
    'lifecycle_backup': ('lifecycle_backup',),
    'encoding_stage_retirement': ('artifact_retirement',),
}
OWNER_STATES = {"active", "activating", "inactive", "failed", "deactivating"}
LOCK_LABELS = {"backup_owner": "備份寫入與收據對帳", "dataset_owner": "來源發布與完整還原",
               "catalog_owner": "冷庫清冊登錄", "replication_owner": "來源複寫"}
FOLLOW_UP = {
    "edge_reader_base_missing": "由原 Vast 節點流程恢復既有 edge owner、索引與連線；固定讀取器排程才會接續。",
    "edge_reader_identity_changed": "核對遠端現行程式、控制檔與登錄版本，不自動覆寫未辨識的狀態。",
    "edge_reader_runtime_wait": "確認遠端索引與 Python 執行環境；程式完整時排程只觀察，不重複安裝。",
    "edge_reader_bundle_invalid": "核對 E 端固定程式包與原始版本清冊，修復前保留遠端現況。",
    "repack_lab203_head_wait": "請 lab203 原 owner 核對固定版本的現行使用與索引，沿既有回執通道回報可否交接；其他資料集仍由原排程繼續。",
    "repack_exclusive_space": "等待其他可回收版本釋出空間；原排程會依最新 D 容量重新檢查，不刪除仍被引用的物件。",
    "source_head_advanced": "原排程繼續處理新世代；舊封裝留在 E，核對來源版本與引用後再決定其後續處置。",
    "source_link": "保留上次來源連結失敗；由原遷移程序重試修復後的來源，尚未重新通過前不回收。",
    "source_predecessor": "先完成前一來源的封存及完整還原，原遷移程序再自動接續。",
    "source_handoff_unenrolled": "核對這個來源的現有版本、寫入鎖及所有使用者，建立可恢復的來源交接後再交給排程。",
    "source_handoff_root_changed": "先盤點固定 E 世代後的來源變更，協調寫入者並更新來源副本，再登錄交接排程。",
    "dependency": "先完成此工作的前置驗證；尚無可靠的開始時間。",
    "recovery_deferred_user_migration": "其他可封裝的來源可繼續；恢復原還原排程時會沿用同一固定版本。",
    "retirement_identity": "需核對固定清冊與原收據的身分差異，修復前保留來源。",
    "owner_retry_limit": "已達原重試上限，需修復失敗原因後交回原排程，其他獨立工作繼續。",
    "receiver_readiness": "等待 lab203 沿既有回執通道回報新鮮就緒證據。",
    "receiver_readiness_lock_diagnosis": "請 lab203 原 owner 核對最近一次 Restic 排他鎖的持有者、備份活動及目前狀態，並回傳新的就緒證據；不要只依 PID 判定鎖已失效。",
    "lake_relay_memory_exhausted": "請 lab203 原 owner 核對失敗資料表、載入版本、可用記憶體與暫存空間；修復後重新完成同一固定版本的完整列雜湊，回傳實際驗收收據。",
    "transport_capacity": "等待 NAS 驗收及傳輸暫存回收釋出在途額度，原排程會再檢查。",
    "transport_retirement_unverified": "由原執行者逐檔核對並回收舊 D 副本；內容不同或仍有引用的項目保留，另列實際阻擋原因。",
    "transport_stale_mount_namespace": "由原服務逐一正常交接舊掛載；未確認所有使用者已切換前，不回收 D 副本。",
    "transport_namespace_unavailable": "恢復程序及磁碟掛載的檢查權限後，由原排程重新檢查。",
    "transport_consumer_alias": "由原同步服務恢復 E 傳輸路徑後，再核對舊副本。",
    "staging_capacity": "先回收已有驗收的暫存；容量足夠後由原排程接續。",
    "physical_capacity": "等待實體磁碟提供足夠工作空間；虛擬磁碟可用量不能替代此條件。",
    "progress_pending": "原程序尚未回報新的階段計數，完成時間暫停估算；下次仍會檢查。",
    "metadata_unavailable": "檢查原工作收據是否產生及可讀；本次未取得有效進度。",
    "windows_storage_administrator": "在 Windows 管理員 PowerShell 執行已準備的磁碟維護入口；原觀測器只讀取狀態，不會自行停機。",
    "windows_storage_failed": "核對原 Windows 維護收據中的容量、檔案系統與服務恢復結果；不要只重寫完成時間。",
    "windows_storage_e_capacity_observed": "目前容量量測會繼續更新；若要驗收先前的 Windows 維護與服務恢復，仍需核對原生收據。",
}
FLOW = (
    ("e_copy", "C → E：已登錄來源工作區", (), "workspace_source_staging",
     "沿用各來源已存在的固定複製與核對證據；不新增解壓或獨立讀回作為本次搬移前置。"),
    ("e_links", "WSL → E：已登錄來源連結核對", ("e_copy",), "workspace_symlink_closure",
     "只核對已登錄且需要連結檢查的來源；每個固定版本可獨立接續。"),
    ("e_handoff", "WSL → E：來源路徑交接與覆蓋", ("e_copy",), "workspace_source_handoff",
     "逐來源顯示已登錄交接及尚未登錄交接的固定 E 複本；複製完成不代表原路徑已切換。"),
    ("publish", "來源追新與首次封存", (), "migration_publication",
     "逐資料集由原下載與發布執行者固定來源；這裡的任務數不是全庫資料量。"),
    ("d_old_to_e", "D → E：優先移出舊封裝", (), "packed_archive_placement",
     "每批來源 SHA、E 持久位置及原引用檢查通過後回收 D 名稱；目前涵蓋已登錄資料集，其他舊包沿原重新封裝排程處理。"),
    ("repack", "E：同資料集去重與循序封裝", ("d_old_to_e",), "source_local_repack_queue",
     "保留來源資料夾邊界，重用相同內容；來源已在 E 的版本可直接處理，不必等整個 D 搬空。"),
    ("d_new", "E → D：提交新封裝", ("repack",), "source_local_promotion",
     "新封裝持久提交到 D；空間不足時，已登錄的 E 固定封裝保留原位置並自動等待。"),
    ("old_gc", "D／E：回收無引用的舊編碼與版本", ("d_new",), "source_repack_retirement",
     "沿原回收輪替檢查引用與日誌；保留研究版本、服務依賴及最新版缺少的舊資料，不依資料夾名稱直接刪除。"),
    ("daily_merge", "每日增量合併與研究版本保留", (), "whole_root_refresh",
     "按目前台北日曆時段開始合併；完整清冊共用未變物件，只為新內容增加負載。一般中間版本另由原回收器清理。"),
    ("retire", "C：回收已封存的非服務副本", ("e_handoff", "publish"), "retirement",
     "只清理各列原 owner 已授權的固定副本；保留帳本、服務工作集、研究引用及各來源尚未完成的讀取接線。"),
    ("recover", "Penguin：獨立還原核對（本次可暫緩）", ("publish",), "migration",
     "本次遷移不新增獨立還原前置；仍保留未完成的原驗收紀錄。Vast 備份驗證協助已取消。"),
    ("send", "Penguin → lab203：封閉批次同步", ("publish",), "transport",
     "與本機遷移平行，送達不等於 NAS 驗收；不作本次舊包搬移的前置。"),
    ("nas", "lab203 → NAS：獨立讀回與配對 ACK", ("send",), "receiver",
     "沿原接收 owner 執行；遠端完整排程未回傳時，開始時間維持未知。"),
    ("transport_gc", "Penguin：已驗收傳輸副本回收", ("nas",), "transport_retirement",
     "逐批核對配對 ACK、保留來源及引用；釋放 D 暫存和 WSL 原檔分開計量。"),
)
METRICS = {"whole_root_registered", "whole_root_eligible", "whole_root_due", "whole_root_ambiguous", "whole_root_closed", "edge_reader_last_recovered_epoch", "cold_scan_pending", "cold_scan_last_acks", "cold_scan_last_elapsed_seconds", "v3_candidates", "v3_activated_last", "v3_activation_errors", "v3_last_activation_epoch", "foreign_head_candidates", "foreign_head_completed", "foreign_head_pending", "discarded_allocated_bytes", "estimated_reclaimed_allocated_bytes", "history_cumulative_reclaimed_bytes", "history_cumulative_versions", "history_cumulative_observed_epoch", "history_anchor_missing", "head_transition_recheck", "pending_batches", "pending_bytes", "active_jobs", "admission_available_bytes", "blocked_items", "attempt_number", "dedup_native_fallbacks", "E_legacy_reclaimed_allocated_bytes", "rescue_protected_bindings", "rescue_last_reconciled_epoch",
           "remote_observed", "remote_eligible", "remote_published", "remote_cycle_deferred",
           "remote_pending_confirmation", "remote_confirmation_limit", "remote_deferred_recovery", "remote_summary_epoch",
           "remote_cache_available_bytes", "remote_cache_threshold_bytes", "remote_cache_reclaimed_bytes", "remote_cache_observed_epoch", "remote_cache_profile_issues", "remote_cache_failure_epoch",
           "namespace_pass", "namespace_passes_total",
           "staging_e_write_bytes", "seed_transferred_bytes", "seed_source_logical_bytes",
           "seed_checkpoint_files", "seed_checkpoint_bytes", "seed_checkpoint_epoch",
           "seed_checkpoint_total_files", "seed_checkpoint_total_bytes",
           "inventory_exact_files", "inventory_changed_files", "inventory_e_only_files", "inventory_d_only_files",
           "inventory_exact_symlinks", "inventory_changed_symlinks", "inventory_e_only_symlinks", "inventory_d_only_symlinks",
           "source_equal_items", "source_public_only_items", "source_private_only_items",
           "source_excluded_items", "source_conflict_items", "uncompared_changed_members", "duplicate_encoded_bytes_pending",
           "attributed_objects", "unattributed_objects", "attributed_bytes", "attribution_errors",
           "staged_deferred_batches", "staged_known_subset_objects",
           "staged_index_matched_objects", "staged_index_matched_bytes", "staged_index_new_objects",
           "staged_index_new_bytes", "staged_index_observed_epoch", "staged_index_unavailable",
           "archive_prefix_candidates", "archive_mixed_or_unknown", "archive_errors",
           "D_required_bytes", "D_observed_free_bytes", "D_reserve_bytes", "D_shortfall_bytes", "D_capacity_observed_epoch", "D_capacity_live",
           "D_exclusive_allocated_bytes", "D_relief_shortfall_bytes", "relief_observed_at_epoch",
           "D_old_head_temporarily_unavailable", "D_bulk_carriers_reclaimed",
           "D_bulk_carrier_reclaimed_allocated_bytes", "D_bulk_carrier_summary_incomplete",
           "D_bulk_landing_parts_reclaimed", "D_bulk_landing_parts_reclaimed_allocated_bytes",
           "inventory_exact_bytes", "inventory_changed_bytes", "inventory_e_only_bytes",
           "partial_exact_full_files", "partial_empty_files", "partial_cold_bytes",
           "cftc_raw_restored_files", "cftc_raw_restored_bytes", "cftc_raw_restored_epoch",
           "retained_original_roots", "total_original_roots", "selected_copy_bytes",
           "stale_mount_namespaces", "reachable_stale_mount_namespaces", "files_held",
           "same_files", "different_files", "missing_files", "unstable_files", "unavailable_files",
           "reclaimed_allocated_bytes", "lab_reclaimed_allocated_bytes", "penguin_packs", "vast_packs", "retained_packs",
           "queued_jobs", "last_cycle_accepted_jobs", "retry_jobs", "rejected_jobs",
           "planned_reclaim_allocated_bytes", "planned_source_files",
           "processed_source_files", "total_source_files", "processed_source_bytes", "total_source_bytes",
           "sqlite_state_index", "sqlite_states_total",
           "source_free_bytes", "source_reserve_bytes", "inflight_streamed_bytes", "durable_streamed_bytes",
           "guest_e_capacity_bytes", "guest_e_filesystem_capacity_bytes", "guest_e_capacity_observed_epoch", "validation_cache_rebound",
           "validation_cache_raced"}
SUBJECTS = {"legacy-artifact-markets-crypto": "加密貨幣研究產物",
            "offline-replay-multibasis22": "當沖多基底 22 舊回放",
            "offline-replay-multibasis22-final": "當沖多基底 22 最終舊回放",
            "offline-replay-multibasis22-postclose": "當沖多基底 22 舊盤後回放",
            "tw-daytrade-offline-repair": "當沖歷史修復候選（非現行帳本）",
            "tw-daytrade-close-rollback": "當沖收盤修復回復副本（非現行帳本）",
            "other-cold-dataset": "其他冷庫資料集",
            "tw-forward-research": "台股研究回傳產物",
            "tw-daytrade-research-artifact": "台股當沖研究產物",
            "binance-public-archive": "Binance 公開歷史",
            "binance-market": "Binance 市場與衍生資料",
            "bybit-market": "Bybit 市場與衍生資料",
            "bybit": "Bybit 市場來源封裝",
            "okx-public-archive": "OKX 歷史行情",
            "tw-minute-source": "台股分鐘原始資料",
            "tw-minute-source-workspace": "永豐分鐘原始資料",
            "tw-minute-train": "台股分鐘研究資料",
            "tw-minute-train-workspace": "台股分鐘研究來源封裝",
            "tw-microstructure-train": "台股微結構研究資料",
            "tw-microstructure-train-workspace": "台股微結構研究來源封裝",
            "tw-futures-prepared": "台灣期貨訓練準備資料",
            "tw-futures-universe725-workspace": "台灣期貨 725 標的訓練來源",
            "tw-futures-universe765-workspace": "台灣期貨 765 標的訓練來源",
            "legacy-parquet": "台股舊版 Parquet 資料",
            "public-economic-history": "公開經濟歷史資料（本機）",
            "tw-index-options-daily": "臺指選擇權日資料",
            "tw-index-options-raw": "臺指選擇權原始歷史檔",
            "tw-index-options-raw-workspace": "臺指選擇權原始歷史來源封裝",
            "forex-frankfurter": "Frankfurter 外匯資料",
            "news-training-local": "新聞訓練資料（本機）",
            "news-training-workspace": "新聞訓練完整來源封裝（本機）",
            "news-fulltext-bodies": "新聞全文來源內容（本機）",
            "news-fulltext-bodies-workspace": "新聞全文來源封裝（本機）",
            "taifex-public-history": "期交所公開歷史及規則來源（本機）",
            "yahoo-market": "Yahoo 市場資料",
            "free-public-context": "免費公開背景資料",
            "forex-pepperstone": "Pepperstone 外匯資料",
            "openbb-compact": "OpenBB 公開歷史",
            "tw-public-research-wide-2014": "台股公開資料研究集（2014）",
            "tw-public-research-all-observed-2014-v3": "台股公開資料全觀測研究集",
            "tw-daytrade-physical-20261004-v1": "當沖實體來源研究資料",
            "tw-daytrade-mixed-sources-20261004-v1": "當沖多來源研究資料",
            "training-dataset-recipes": "訓練資料集設定與紀錄",
            "tw-futures-rule-recovery-20261027-v11": "台灣期貨歷史規則資料",
            "tw-daytrade-panel-sources-20261004-v3": "當沖面板來源資料",
            "tw-daytrade-panel-sources-20261004-gaprepair-v2": "當沖面板補缺來源 v2",
            "tw-daytrade-panel-sources-20261004-gaprepair-v4": "當沖面板補缺來源 v4",
            "tw-shioaji-history": "永豐台股歷史",
            "tw-minute-source-cold": "台股分鐘原始資料冷庫",
            "tw-microstructure-captures-cold": "台股微結構來源冷庫",
            "tw-futures": "台灣期貨歷史",
            "tw-public": "台股公開資料",
            "tw-index-derivatives-ticks": "台灣指數期權逐筆來源",
            "binance": "Binance 行情歷史",
            "okx": "OKX 行情歷史",
            "crypto-etf-history": "加密貨幣 ETF 歷史（本機）",
            "keyed-public-catalogs": "公開來源目錄（本機）",
            "crypto-reference": "加密貨幣參考資料（本機）",
            "coinmetrics-community": "Coin Metrics 鏈上資料（本機）",
            "crypto-public-context": "加密貨幣公開背景資料（本機）",
            "crypto-historical-public": "加密貨幣公開背景資料（本機）",
            "dune-crypto": "Dune 加密貨幣資料（本機）",
            "fred-crypto-macro": "FRED 加密貨幣總經資料（本機）",
            "fred-crypto-macro-workspace": "FRED 加密貨幣總經工作資料",
            "taifex-public-workspace": "期交所公開歷史及規則工作資料",
            "provider-originals": "FinLab／FinMind／TEJ",
            "source-originals": "已交接的一般來源",
            "finlab": "FinLab", "finmind": "FinMind", "tej": "TEJ",
            "finlab-research": "FinLab 研究資料", "finmind-free": "FinMind 免費來源",
            "local-tw-index-derivatives-ticks": "台灣期權逐筆與即時報價",
            "legacy-artifact-markets-us": "美股研究產物",
            "local-openbb": "OpenBB 本機歷史", "cftc-legacy-pre2000": "CFTC 2000 年以前歷史",
            "cftc-legacy-pre2000-workspace": "CFTC 2000 年以前工作資料",
            "tw-futures-margin-sources": "台灣期貨保證金來源資料",
            "local-tw-public": "台股公開資訊本機歷史", "local-tw-futures": "台灣期貨本機歷史",
            "local-tw-index-futures": "台灣指數期貨本機歷史", "tw-index-futures": "台灣指數期貨歷史",
            "local-bybit": "Bybit 本機完整來源", "local-forex-frankfurter": "Frankfurter 本機完整來源",
            "forex-frankfurter-official": "官方外匯來源（本機）", "local-free-public": "免費公開資料本機來源",
            "local-public-economic": "公開經濟資料本機來源", "local-taifex-public-history": "期交所本機完整來源",
            "local-tw-minute": "台股分鐘本機來源", "local-tw-microstructure": "台股微結構本機來源",
            "news-fulltext-local": "新聞全文資料（本機）", "openbb-task-shards-local": "OpenBB 任務原始資料（本機）",
            "taifex-rule-history": "期交所規則歷史（本機）", "fred-history-copy": "FRED 舊版來源",
            "openbb-state": "OpenBB 狀態快照", "taifex-state": "期交所狀態快照",
            "futures-state": "期貨狀態快照", "news-fulltext-state": "新聞全文狀態快照",
            "news-training-state": "新聞訓練狀態快照"}


METRICS.update({'archive_head_bytes', 'archive_placed_bytes', 'archive_remaining_bytes',
                'archive_shared_D_objects', 'archive_D_reclaimed_bytes',
                'archive_selected_historical',
                'ordinary_history_versions_removed', 'ordinary_history_reclaimed_bytes',
                'ordinary_history_coverage_protected'})


def number(value):
    return value if type(value) is int and 0 <= value <= 2**53 - 1 else None


def source_subject(name):
    if not isinstance(name, str):
        return None
    if name in SUBJECTS:
        return name
    state = re.fullmatch(r'(.+)-state-[a-f0-9]{12}', name)
    if state:
        return {'local-openbb': 'openbb-state', 'local-taifex-public-history': 'taifex-state',
                'taifex-rule-history': 'taifex-state', 'local-tw-index-futures': 'futures-state',
                'news-fulltext-local': 'news-fulltext-state',
                'news-training-local': 'news-training-state'}.get(state.group(1), 'other-cold-dataset')
    if re.fullmatch(r'fred-(?:eager|retained|unretired)-20261026-[a-e]', name):
        return 'fred-history-copy'
    # These fixed public families reveal only a broad research category. The
    # underlying snapshot names and model identifiers remain private.
    if re.fullmatch(r'completed-tw-forward-[a-f0-9]{64}', name):
        return 'tw-forward-research'
    if name in {
        'artifact-tw-day-trade-multi-basis-22-effective-rank-projection-l1-capital10m-v1',
        'artifact-tw-day-trade-v8-combined-annual-log-cash-v5',
    }:
        return 'tw-daytrade-research-artifact'
    return 'other-cold-dataset' if re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', name) else None


def choice(value, allowed, default=None):
    return value if isinstance(value, str) and value in allowed else default


def finite(value):
    return value if type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 2**53 - 1 else None


def timestamp(value):
    if type(value) in (int, float):
        return finite(value)
    if isinstance(value, str):
        try:
            date = datetime.fromisoformat(value)
            return finite(date.timestamp()) if date.tzinfo else None
        except (ValueError, OverflowError):
            pass
    return None


def iso(value):
    if finite(value) is not None:
        try:
            return datetime.fromtimestamp(value, timezone.utc).isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    return None


def clock(row):
    return next((stamp for key in ("observed_at_utc", "updated_at_utc", "checked_at_epoch",
                                   "observed_at_epoch", "finished_at_epoch", "started_at_epoch")
                 if (stamp := timestamp(row.get(key))) is not None), None)


def metadata(path: Path, *, limit=MAX_METADATA_BYTES) -> dict:
    """No aliases except the explicit service-layout map; no unbounded JSON."""
    try:
        path = canonical_service_path(path)
        if any(part.is_symlink() for part in (path, *path.parents)):
            return {}
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return {}
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            return {}
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError, RecursionError):
        return {}


def children(path: Path, limit=128) -> list[Path]:
    """Immediate metadata only, never recurse or follow a directory symlink."""
    path = canonical_service_path(path)
    try:
        if any(part.is_symlink() for part in (path, *path.parents)):
            return []
        entries = []
        for item in path.iterdir():
            if len(entries) >= limit:
                return []  # An unexpectedly broad root needs explicit enrollment.
            entries.append(item)
        return [item for item in entries if not item.is_symlink() and item.is_dir()]
    except OSError:
        return []


def key(value) -> str:
    return hashlib.sha256(str(value).encode()).hexdigest()[:24]


def task(kind, identity, *, node="penguin", state="unknown", phase="working", observed=None,
         generation="", done_bytes=None, total_bytes=None, done_items=None, total_items=None,
         proof=False, reason=None, activity=None, metrics=None, batch_number=None):
    return {"id": key((kind, identity)), "kind": kind, "node_id": node, "state": state,
            "phase": phase, "observed_at_utc": iso(observed), "activity_at_utc": iso(activity),
            "generation": key(generation), "completed_bytes": done_bytes, "total_bytes": total_bytes,
            "completed_items": done_items, "total_items": total_items, "complete_evidence": proof,
            "reason": reason, "metrics": metrics or {}, "batch_number": batch_number}


def publication_counters(value: dict) -> dict:
    """Map a source publication's counters to the phase that produced them."""
    if (value.get('stage') == 'packing_changed_small_files'
            or value.get('stage') == 'publishing_sqlite_state'
            and value.get('sqlite_substage') == 'packing_changed_small_files'):
        # Older producers reported a bucket complete after each sub-pack.
        # Source byte/member work is counted independently and stays usable
        # throughout the final bucket, including before that producer reloads.
        return {'done_items': value.get('files_hashed', value.get('files_completed')),
                'total_items': value.get('files_total'),
                'done_bytes': value.get('logical_bytes_processed'),
                'total_bytes': value.get('logical_bytes_total')}
    if number(value.get('phase_completed_items')) is not None:
        return {'done_items': value['phase_completed_items'], 'total_items': value.get('phase_total_items')}
    return {'done_items': (value.get('files_routed') if value.get('stage') == 'routing_and_hashing'
                           else value.get('files_completed')),
            'total_items': value.get('files_total'),
            'done_bytes': value.get('logical_bytes_processed'),
            'total_bytes': value.get('logical_bytes_total')}


def publication_eta_work(value: dict):
    """A bucket can take hours; its owner already counts each hashed member.

    Keep the source-work bar. Estimate remaining source-content work from actual
    member/byte deltas in this packing phase; later publication/recovery is
    outside this ETA. Earlier routing observations are not speed samples.
    """
    total, done = number(value.get('files_total')), number(value.get('files_hashed'))
    packing = (value.get('stage') == 'packing_changed_small_files'
               or value.get('stage') == 'publishing_sqlite_state'
               and value.get('sqlite_substage') == 'packing_changed_small_files')
    if (packing and total and done is not None
            and done <= total and number(value.get('files_routed')) == total):
        return {'completed_items': done, 'total_items': total,
                'completed_bytes': number(value.get('logical_bytes_processed')),
                'total_bytes': number(value.get('logical_bytes_total'))}
    return None


def cold_scan_notification_tasks(retry_path: Path) -> list[dict]:
    """Project the latest source-side notification, not remote convergence."""
    value = metadata(retry_path)
    accepted = {'idle_no_pending', 'scan_request_acknowledged', 'concurrent_scan_completed',
                'scan_batch_processed', 'deferred_existing_folder_scan', 'retry_failed',
                'retry_partial_failure'}
    status = choice(value.get('status'), accepted)
    if value.get('schema_version') != 1 or status is None:
        return []
    pending = number(value.get('pending_after'))
    acks = number(value.get('acknowledged_count'))
    if acks is None and status == 'scan_request_acknowledged':
        acks = 1
    elapsed = finite(value.get('elapsed_seconds'))
    metrics = {'cold_scan_pending': pending, 'cold_scan_last_acks': acks,
               'cold_scan_last_elapsed_seconds': round(elapsed) if elapsed is not None else None}
    failed = status in {'retry_failed', 'retry_partial_failure'}
    busy = status == 'deferred_existing_folder_scan'
    return [task('cold_scan_notification', 'current', state='blocked' if failed else 'waiting',
                 phase='cold_scan_notify', observed=clock(value),
                 reason='cold_scan_retry_error' if failed else 'cold_scan_owner_busy' if busy else None,
                 metrics=metrics, proof=False)]


def backup_tasks(root: Path, retry_path: Path | None = None) -> list[dict]:
    status = metadata(root / "status.json")
    combined = status.get("combined_nas_coverage", {})
    if not isinstance(combined, dict) or combined.get("contract") != "deduplicated_nas_file_coverage_v1":
        combined = {}
    private = status.get("private_history_backup", {})
    if not isinstance(private, dict) or private.get("contract") != "stockagent.private_cold_NAS_enrollment.v1":
        private = {}
    activity = metadata(root / "private-status.json")
    # An active writer starts a fresh small status; don't erase its last coverage
    # or borrow the new heartbeat to make an old coverage receipt look fresh.
    if activity.get("contract") != "stockagent.private_cold_NAS_enrollment.v1":
        activity = {}
    if (activity.get("catalog_identity_sha256") != private.get("catalog_identity_sha256")
            or (number(activity.get("nas_covered_bytes")) is not None
                and (clock(activity) or 0) >= (clock(private) or 0))):
        private = activity
    retained = metadata(root / "private-coverage.json")
    matching_retained = (retained.get("contract") == "stockagent.private_cold_NAS_coverage.v1"
            and isinstance(retained.get("catalog_identity_sha256"), str)
            and retained["catalog_identity_sha256"] == private.get("catalog_identity_sha256")
            and number(retained.get("nas_covered_bytes")) is not None)
    if matching_retained and (number(private.get("nas_covered_bytes")) is None
                             or (clock(retained) or 0) >= (clock(private) or 0)):
        private = retained
    private_reason = "receiver_readiness" if activity.get("state") == "private_backup_waiting_receiver_readiness" else "scope_incomplete"
    refresh = status.get('catalog_refresh')
    capacity_wait = (isinstance(refresh, dict)
        and refresh.get('contract') == 'stockagent.backup_catalog_refresh.v1'
        and refresh.get('state') == 'deferred_physical_capacity'
        and refresh.get('coverage_recomputed') is False)
    placement_wait = (isinstance(refresh, dict)
        and refresh.get('contract') == 'stockagent.backup_catalog_refresh.v1'
        and refresh.get('state') == 'deferred_transport_placement'
        and refresh.get('coverage_recomputed') is False)
    if activity.get("state") in {"private_backup_waiting_capacity", "private_backup_waiting_publication_owner"}:
        budget = activity.get("wave_budget")
        detail = budget.get("transport_detail_bounds_bytes") if isinstance(budget, dict) else None
        if isinstance(detail, dict) and all(type(detail.get(key)) is int for key in ("staging", "retained_transport", "physical")):
            limiting = min(detail, key=lambda key: detail[key]) if set(detail) == {"staging", "retained_transport", "physical"} else None
            private_reason = {"staging": "staging_capacity", "retained_transport": "transport_capacity",
                              "physical": "physical_capacity"}.get(limiting, "scope_incomplete")
    rows = []
    for kind, row, stamp in (("registered_backup", combined, clock(status)),
                             ("private_backup", private, clock(private))):
        proof = (bool(number(row.get("available_bytes")))
            and row.get("nas_covered_bytes") == row.get("available_bytes")
            and row.get("nas_covered_file_count") == row.get("available_file_count")
            and row.get("all_available_cold_files_verified" if kind == "registered_backup"
                        else "registered_private_closure_NAS_verified") is True)
        rows.append(task(kind, kind, node="nas", state="completed" if proof else "waiting" if row else "unknown",
            phase="nas_file_readback", observed=stamp, generation=row.get("catalog_identity_sha256"),
            done_bytes=row.get("nas_covered_bytes"), total_bytes=row.get("available_bytes"),
            done_items=row.get("nas_covered_file_count"), total_items=row.get("available_file_count"),
            proof=proof, reason=(private_reason if kind == "private_backup" else
                                'physical_capacity' if capacity_wait else
                                'transport_placement_priority' if placement_wait else 'scope_incomplete'),
            activity=((timestamp(activity.get("activity_at_utc")) or clock(activity)) if kind == "private_backup"
                      else timestamp(status.get('activity_at_utc'))),
            metrics={k: refresh.get(k) for k in ('source_free_bytes', 'source_reserve_bytes')}
                    if kind == 'registered_backup' and capacity_wait else None))
        if kind == 'private_backup':
            # The original owner validates the paired lease. Only its bounded
            # clock diagnosis is projected; no observer renews write admission.
            rows[-1]['receiver_readiness'] = activity.get('receiver_readiness')
    scan = metadata(root / "transport-scan-status.json")
    if scan.get("contract") != "backup_transport_scan_notification_v1":
        scan = {}
    # A new retry is an observation of the current owner/folder, not a fresh
    # scan acknowledgement. Never carry an older pending count into it.
    retry = metadata(retry_path) if retry_path is not None else {}
    retry_status = retry.get('backup_transport')
    retry_policy = retry.get('requested_scan_policy')
    retry_policy = retry_policy if isinstance(retry_policy, dict) else {}
    retry_state = retry_status.get('state') if isinstance(retry_status, dict) else None
    waiting_retry = {'existing_source_owner_busy', 'deferred_existing_folder_scan',
                     'deferred_invalid_folder_state'}
    failed_retry = {'notification_retry_failed', 'retryable_scan_error'}
    use_retry = (retry.get('schema_version') == 1
        and retry_policy.get('backup_transport') is True
        and retry_state in waiting_retry | failed_retry
        and (clock(retry) or 0) > (clock(scan) or 0))
    transport = task("transport", "transport", phase="scan_notification",
        observed=clock(retry) if use_retry else clock(scan),
        state=('blocked' if retry_state in failed_retry else 'waiting') if use_retry else "waiting" if scan else "unknown",
        generation=(scan.get("envelope"), 'retry_observation') if use_retry else scan.get("envelope"),
        metrics={"pending_batches": None if use_retry else scan.get("pending_batch_scans")},
        reason='dependency' if use_retry and retry_state in failed_retry else 'original_owner')
    transport['last_scan_observed_at_utc'] = iso(clock(scan))
    transport['notification_observation'] = 'retry' if use_retry else 'scan'
    rows.append(transport)
    cache = metadata(root / 'transport-cache-progress.json')
    if (cache.get('contract') == 'stockagent.transport_cache_retirement_progress.v1'
            and cache.get('scope') == 'fixed_accepted_transport_candidates'
            and isinstance(cache.get('generation_sha256'), str)
            and re.fullmatch(r'[a-f0-9]{64}', cache['generation_sha256'])
            and number(cache.get('source_objects_deleted')) == 0 and number(cache.get('NAS_snapshots_deleted')) == 0
            and cache.get('logical_transport_bytes_not_allocated_WSL_reclamation') is True):
        proof = (cache.get('all_candidate_retirements_verified') is True
                 and cache.get('state') == 'completed'
                 and bool(number(cache.get('total_items')))
                 and bool(number(cache.get('total_bytes')))
                 and number(cache.get('completed_items')) == number(cache.get('total_items'))
                 and number(cache.get('completed_bytes')) == number(cache.get('total_bytes')))
        rows.append(task('transport_retirement', 'transport-cache',
            state='completed' if proof else choice(cache.get('state'), {'running', 'waiting', 'blocked'}, 'unknown'),
            phase='retire-verified-transport', observed=clock(cache), generation=cache['generation_sha256'],
            done_items=cache.get('completed_items'), total_items=cache.get('total_items'),
            done_bytes=cache.get('completed_bytes'), total_bytes=cache.get('total_bytes'), proof=proof,
            reason=choice(cache.get('reason_code'), REASONS),
            metrics={'blocked_items': cache.get('blocked_items')}))
    else:
        rows.append(task('transport_retirement', 'transport-cache', phase='retire-verified-transport',
                         reason='metadata_unavailable'))
    return rows


def lifecycle_backup_tasks(root: Path) -> list[dict]:
    """Observe the original SQL backup's small receipts, never its dumps."""
    capacity = metadata(root / 'control-backup-capacity.json')
    if capacity.get('contract') != 'stockagent.lifecycle_sql_capacity.v1':
        capacity = {}
    delivery = metadata(root / 'traditional-backup-status.json')
    delivery_clock, capacity_clock = clock(delivery), clock(capacity)
    capacity_wait = (capacity.get('state') == 'waiting_capacity'
                     and (capacity_clock or 0) >= (delivery_clock or 0))
    if capacity_wait:
        return [task('lifecycle_backup', 'lifecycle-backup', state='blocked',
            phase='capacity-preflight', observed=capacity_clock, reason='physical_capacity')]
    if delivery.get('state') in {'publication_prepared', 'waiting_encrypted_nas_backup'}:
        dispatch = delivery.get('dispatch')
        dispatch = dispatch if isinstance(dispatch, dict) else {}
        return [task('lifecycle_backup', 'lifecycle-backup', state='waiting',
            phase='control_sql_delivery', observed=delivery_clock,
            generation=dispatch.get('envelope_identity_sha256'),
            total_bytes=dispatch.get('complete_bytes'), total_items=dispatch.get('complete_files'),
            reason='original_owner')]
    return [task('lifecycle_backup', 'lifecycle-backup',
        state='running' if capacity.get('state') == 'admitted' else 'unknown',
        phase='control_sql_dump', observed=capacity_clock,
        reason='no_counter' if capacity else 'metadata_unavailable')]


def lake_transport_tasks(root: Path) -> list[dict]:
    """The immutable-lake GC is distinct from BackupStream envelope GC."""
    value = metadata(root / 'transport-gc-status.json')
    if value.get('contract') != 'stockagent.lake_transport_gc_status.v1':
        return []
    state = value.get('state')
    retirement = value.get('retirement')
    proof = (state == 'retired' and isinstance(retirement, dict)
             and retirement.get('state') == 'retired'
             and retirement.get('primary_source_files_deleted') == 0
             and retirement.get('nas_archive_files_deleted') == 0)
    progress = value.get('work_progress')
    progress = progress if isinstance(progress, dict) else {}
    reason = choice(value.get('reason_code'), REASONS)
    row = task('lake_transport_retirement', 'lake-transport',
        state='completed' if proof else 'running' if state == 'retiring_exact_accepted_transport' else
              'blocked' if state == 'retirement_deferred' else 'waiting',
        phase='retired' if proof else 'waiting_accepted_transport' if state == 'no_accepted_transport_candidates'
              else choice(value.get('phase'), PHASES, 'retire-verified-transport'), observed=clock(value),
        generation=(value.get('delivery_identity_sha256', 'lake-transport'),
                    value.get('work_pass'), value.get('phase_iteration')),
        done_items=progress.get('completed_items'), total_items=progress.get('total_items'),
        done_bytes=progress.get('completed_bytes'), total_bytes=progress.get('total_bytes'), proof=proof,
        reason=reason or 'original_owner',
        metrics={'retry_jobs': value.get('retry_wait_count'),
                 'queued_jobs': value.get('ready_candidate_count')})
    row['next_attempt_at_utc'] = iso(timestamp(value.get('next_retry_at_epoch')))
    row['completion_forecast'] = value.get('completion_forecast')
    row['queue_forecast'] = value.get('queue_forecast')
    row['started_at_utc'] = value.get('workflow_started_at_utc')
    return [row]


def _live_tw_minute_auditor(repo: Path, owner: dict):
    """Identify the original auditor below the exact live lifecycle owner."""
    if not _process_identity_matches(owner):
        return None
    proc = Path('/proc')
    try:
        uptime = float((proc/'uptime').read_text().split()[0])
        boot_epoch = time.time() - uptime
        ticks = os.sysconf('SC_CLK_TCK')
        pending = [(int(owner['pid']), 0)]
        visited = set()
        while pending and len(visited) < 128:
            pid, depth = pending.pop()
            if pid in visited or depth > 6:
                continue
            visited.add(pid)
            try:
                # A subprocess created by a non-main worker thread appears in
                # that thread's children file, although its PPID is the process.
                thread_root = proc/str(pid)/'task'
                threads = list(thread_root.iterdir())
                if len(threads) > 256:
                    return None
                children = []
                for thread in threads:
                    try:
                        children.extend((thread/'children').read_text().split())
                    except OSError:
                        continue
            except OSError:
                continue
            for text in children:
                child = int(text)
                try:
                    fields = (proc/str(child)/'stat').read_text().rsplit(')', 1)[1].split()
                    if int(fields[1]) != pid:
                        continue
                    start_ticks = int(fields[19])
                    raw = (proc/str(child)/'cmdline').read_bytes()
                    if len(raw) > 32768:
                        continue
                    args = [os.fsdecode(value) for value in raw.split(b'\0') if value]
                    def option(name):
                        return args[args.index(name)+1] if name in args and args.index(name)+1 < len(args) else None
                    if (len(args) > 1 and args[1] == str(repo/'scripts/audit_shioaji_tw_minute_dataset.py')
                            and '--all-partitions' in args
                            and option('--dataset-root') == str(repo/'data_tw_minute/research_dataset')
                            and option('--verification-cache') == str(repo/'artifacts/operations/source_validation/tw-minute-train/files.sqlite3')):
                        return {'pid': child, 'start_ticks': start_ticks,
                                'started_at_epoch': boot_epoch + start_ticks/ticks}
                except (OSError, ValueError, IndexError):
                    continue
                pending.append((child, depth+1))
    except (OSError, ValueError, TypeError, KeyError):
        pass
    return None


def _tw_minute_validation_progress(repo: Path, owner: dict, *, now: float):
    """Observe matching cached partitions, never claim the audit has passed.

    The currently running auditor predates this observer. Its existing SQLite
    cache is read without writes, and each counted row must match the fixed
    manifest SHA, exact source generation and the running validator contract.
    """
    repo = Path(repo).resolve()
    auditor = _live_tw_minute_auditor(repo, owner)
    if auditor is None:
        return None
    deadline = time.monotonic() + 3.0
    dependencies = ('scripts/audit_shioaji_tw_minute_dataset.py',
                    'stockagent/data/tw_price_rules.py', 'downloader/stock_volume_units.py',
                    'scripts/build_shioaji_tw_minute_dataset.py',
                    'stockagent/data/tw_exchange_price_classification.py',
                    'stockagent/data_sync/source_validation.py')
    def same_generation(first, second):
        # Reading metadata can update atime; it does not change the generation.
        return all(getattr(first, key) == getattr(second, key) for key in
                   ('st_dev', 'st_ino', 'st_mode', 'st_size', 'st_mtime_ns', 'st_ctime_ns'))
    try:
        code_hashes = []
        for name in dependencies:
            path = repo/name
            before = path.stat()
            if (path.is_symlink() or not stat.S_ISREG(before.st_mode)
                    or before.st_size > 4*1024**2
                    or before.st_mtime > auditor['started_at_epoch']):
                return None
            raw = path.read_bytes()
            if not same_generation(path.stat(), before):
                return None
            code_hashes.append(hashlib.sha256(raw).hexdigest())
        contract = hashlib.sha256(''.join(code_hashes).encode()).hexdigest()
        root = repo/'data_tw_minute/research_dataset'
        manifest = root/'manifest.json'
        before_manifest = manifest.stat()
        if (manifest.is_symlink() or not stat.S_ISREG(before_manifest.st_mode)
                or before_manifest.st_size > 16*1024**2):
            return None
        manifest_raw = manifest.read_bytes()
        value = json.loads(manifest_raw)
        partitions = value.get('partitions') if isinstance(value, dict) else None
        if not isinstance(partitions, list) or not 0 < len(partitions) <= 5000:
            return None
        expected = {}
        for part in partitions:
            day = part.get('trade_date') if isinstance(part, dict) else None
            sha = part.get('output_sha256') if isinstance(part, dict) else None
            if (not isinstance(day, str) or re.fullmatch(r'\d{4}-\d{2}-\d{2}', day) is None
                    or not isinstance(sha, str) or re.fullmatch(r'[a-f0-9]{64}', sha) is None
                    or part.get('output') != f'trade_date={day}/data.parquet'):
                return None
            path = root/f'trade_date={day}'/'data.parquet'
            if str(path) in expected:
                return None
            expected[str(path)] = sha
        cache = repo/'artifacts/operations/source_validation/tw-minute-train/files.sqlite3'
        if cache.is_symlink() or not cache.is_file():
            return None
        connection = sqlite3.connect(f'file:{cache}?mode=ro', uri=True, timeout=0.1)
        try:
            done = 0
            for name, signature, sha in connection.execute(
                    'SELECT path,signature,sha256 FROM verified WHERE contract=?', (contract,)):
                if time.monotonic() > deadline:
                    return None
                if name not in expected or sha != expected[name]:
                    continue
                path = Path(name)
                if path.is_symlink() or path.parent.is_symlink():
                    continue
                try:
                    current = path.stat()
                    saved = json.loads(signature)
                except (OSError, ValueError, TypeError):
                    continue
                if (stat.S_ISREG(current.st_mode) and saved == [current.st_dev, current.st_ino,
                        current.st_size, current.st_mtime_ns, current.st_ctime_ns]):
                    done += 1
        finally:
            connection.close()
        if not same_generation(manifest.stat(), before_manifest) or not _process_identity_matches(owner):
            return None
        fields = (Path('/proc')/str(auditor['pid'])/'stat').read_text().rsplit(')', 1)[1].split()
        if int(fields[19]) != auditor['start_ticks']:
            return None
        return {'completed': done, 'total': len(expected), 'observed_at_epoch': now,
                'manifest_sha256': hashlib.sha256(manifest_raw).hexdigest(),
                'validator_sha256': contract, 'auditor_start_ticks': auditor['start_ticks']}
    except (OSError, ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
        return None


def _dependency_reference_repair_applies(operation: Path, dataset: str, data: dict) -> bool:
    repair = data.get('dependency_reference_repair')
    if not isinstance(repair, dict):
        return False
    bindings = repair.get('fixed_bindings')
    if (repair.get('contract') != 'stockagent.migration_dependency_reference_repair.v1'
            or repair.get('dataset') != dataset or repair.get('state') != 'references_reconciled'
            or not isinstance(data.get('attempt_id'), str) or repair.get('attempt_id') != data['attempt_id']
            or repair.get('source_publication_complete') is not False or repair.get('source_payload_read') is not False
            or repair.get('original_failure_preserved') is not True or not isinstance(bindings, dict)
            or not 1 <= len(bindings) <= 4096
            or any(not isinstance(name, str) or re.fullmatch('[a-z0-9][a-z0-9-]{0,127}', name) is None
                   or not isinstance(sha, str) or re.fullmatch('[a-f0-9]{64}', sha) is None
                   for name, sha in bindings.items())):
        return False
    if dataset in bindings:
        refresh = data.get('freshness_refresh')
        return (len(bindings) == 1 and isinstance(refresh, dict)
                and refresh.get('previous_manifest_sha256') == bindings[dataset])
    preparation = metadata(operation/'preparations'/(dataset+'.json'), limit=32*1024**2)
    states = preparation.get('sqlite_states')
    return (preparation.get('contract') == 'all_catalog_incremental_migration_v1'
        and preparation.get('dataset') == dataset and isinstance(states, list) and len(states) == len(bindings)
        and all(isinstance(row, dict) and isinstance(row.get('dataset'), str) and row.get('dataset') in bindings
                and row.get('manifest_sha256') == bindings[row['dataset']] for row in states)
        and len({row['dataset'] for row in states}) == len(bindings))


def migration_tasks(root: Path, *, repo: Path | None = None, recovery_workspace: Path | None = None, now=None) -> list[dict]:
    current = metadata(root / "current.json")
    directory = current.get("operation_directory")
    operation = Path(directory) if isinstance(directory, str) else root / "missing"
    # The canonical pointer may only select one operation under its own root.
    allowed = root / "operations"
    if operation.parent != allowed or not re.fullmatch(r"[a-f0-9-]{16,96}", operation.name):
        operation = allowed / "missing"
    progress = metadata(operation / "progress.json", limit=4 * MAX_METADATA_BYTES)
    if progress.get("contract") != "all_catalog_incremental_migration_v1":
        progress = {}
    datasets = progress.get("datasets", {})
    datasets = datasets if isinstance(datasets, dict) and len(datasets) <= 256 else {}
    total = number(progress.get("source_count"))
    verified = sum(isinstance(v, dict) and v.get("state") == "verified" for v in datasets.values())
    complete = bool(total and len(datasets) == total and verified == total)
    owner = progress.get("runtime_owner") or {}
    active = owner.get("active_dataset") if isinstance(owner, dict) else None
    remaining = [v for v in datasets.values() if isinstance(v, dict) and v.get("state") != "verified"]
    waiting = bool(remaining) and all(v.get("state") in {
        "waiting_retry", "recovery_deferred_user_migration"} for v in remaining)
    recovery_active = (isinstance(active, str) and isinstance(datasets.get(active), dict)
                       and datasets[active].get('state') == 'recovery')
    deferred = any(v.get('state') == 'recovery_deferred_user_migration' for v in remaining)
    # Source packaging uses the same parent operation as independent recovery.
    # Its processing state is not evidence that a recovery is running.
    deferred_wait = deferred and not recovery_active
    rows = [task("migration", "migration", state="completed" if complete else "running" if recovery_active
        else "waiting" if deferred_wait else "blocked" if waiting else "waiting" if progress else "unknown",
        phase="recovery_deferred_user_migration" if deferred_wait else "dataset_reconstruction",
        observed=clock(progress) or clock(current),
        generation=current.get("plan_sha256"), done_items=verified if progress else None,
        total_items=total, proof=complete, reason="recovery_deferred_user_migration" if deferred_wait
        else "dependency" if waiting else "remaining_phases")]
    refresh = progress.get('whole_root_refresh')
    priority_resume = (_storage_priority_window_end(time.time() if now is None else now)
                       if isinstance(refresh, dict) and refresh.get('contract') == 'stockagent.whole_root_refresh_queue.v1'
                       and refresh.get('window_allowed') is False else None)
    if (isinstance(refresh, dict) and refresh.get('contract') == 'stockagent.whole_root_refresh_queue.v1'
            and refresh.get('scope') == 'one_unexcluded_owner_per_registered_data_root'
            and refresh.get('source_freshness_claimed') is False
            and type(refresh.get('window_allowed')) is bool
            and all(type(refresh.get(k)) is int and 0 <= refresh[k] <= 256 for k in
                    ('registered_roots', 'eligible_roots', 'ambiguous_roots', 'due_roots'))
            and refresh['due_roots'] <= refresh['eligible_roots'] <= refresh['registered_roots']
            and type(refresh.get('closed_roots', 0)) is int
            and 0 <= refresh.get('closed_roots', 0) <= refresh['registered_roots']
            and refresh['ambiguous_roots'] + refresh['eligible_roots'] + refresh.get('closed_roots', 0)
                <= refresh['registered_roots']):
        ambiguous, due = refresh['ambiguous_roots'], refresh['due_roots']
        reason = ('root_refresh_ambiguous' if ambiguous else
                  'ordinary_archive_window' if refresh.get('daily_merge_window_allowed') is False else
                  'tw_day_trade_priority_window' if due and not refresh['window_allowed'] else
                  'root_refresh_due' if due else 'root_refresh_scheduled')
        row = task('whole_root_refresh', 'registered-data-roots', state='blocked' if ambiguous else 'waiting',
                   phase='whole_root_refresh_check', observed=clock(progress), reason=reason,
                   generation=current.get('plan_sha256'), metrics={
                       'whole_root_registered': refresh['registered_roots'],
                       'whole_root_eligible': refresh['eligible_roots'],
                       'whole_root_due': due, 'whole_root_ambiguous': ambiguous,
                       'whole_root_closed': refresh.get('closed_roots', 0)})
        next_due = timestamp(refresh.get('next_due_epoch'))
        if refresh.get('daily_merge_window_allowed') is False:
            next_due = timestamp(refresh.get('next_daily_merge_epoch'))
            row['next_attempt_at_utc'] = iso(next_due)
        if not due and next_due is not None:
            row['next_attempt_at_utc'] = iso(next_due)
        rows.append(row)
    for name, data in datasets.items():
        if not isinstance(data, dict) or not re.fullmatch(r"[a-z0-9_-]{1,96}", name):
            continue
        refresh_state = data.get('freshness_refresh')
        rebinding = refresh_state.get('base_rebinding') if isinstance(refresh_state, dict) else None
        if (isinstance(rebinding, dict) and
                rebinding.get('contract') == 'stockagent.refresh_latest_successor_binding.v1' and
                rebinding.get('historical_recovery_claimed') is False and
                rebinding.get('changed_member_values_compared') is False and
                rebinding.get('old_historical_value_coverage') == 'not_compared'):
            history = task('migration_dependency', name + '-latest-uncompared', state='waiting',
                phase='source_dependency', observed=clock(progress) or clock(current),
                generation=rebinding.get('manifest_sha256'), reason='latest_successor_history_not_compared')
            history['subject'] = name if name in SUBJECTS else None
            rows.append(history)
        if data.get('state') == 'recovery_deferred_user_migration':
            row = task('migration_dependency', name, state='waiting', phase='recovery_deferred_user_migration',
                observed=clock(progress) or clock(current), generation=data.get('manifest_sha256'),
                reason='recovery_deferred_user_migration')
            row['subject'] = name if name in SUBJECTS else None
            rows.append(row)
        if data.get("state") == "waiting_retry":
            error = data.get("error", "")
            dependency = data.get('dependency_wait')
            capacity_wait = data.get('capacity_wait')
            owner_busy = (data.get('error_type') == 'PackedPublicationBusy'
                          and isinstance(error, str) and error.startswith('publication owner busy:'))
            reason = ('physical_capacity' if isinstance(capacity_wait, dict)
                      and capacity_wait.get('contract') == 'source_publication_capacity_v1' else
                      'source_publication_owner_busy' if owner_busy else
                      "source_predecessor" if isinstance(dependency, dict)
                      and dependency.get('dataset') == 'local-tw-index-futures' else
                      'incremental_source_incomplete' if isinstance(error, str) and
                      'incremental workspace omits previously published files' in error else
                      'incremental_base_missing' if isinstance(error, str) and
                      ('incremental base head is missing' in error or 'incremental base head changed' in error) else
                      'source_exact_pin_missing' if isinstance(error, str) and 'cannot resolve exact packed pin' in error else
                      'source_sqlite_binding_previous' if data.get('error_type') == 'FileNotFoundError'
                      and isinstance(error, str) and '/manifests/' in error and '-state-' in error
                      and '.json' in error else
                      'source_coordination_previous' if data.get('error_type') == 'SourceCoordinationLockTimeout' else
                      'source_native_bridge_previous' if isinstance(error, str) and
                      'native E workspace operation failed' in error else
                      "source_link" if isinstance(error, str) and "symlink" in error else
                      "source_changed" if isinstance(error, str) and any(marker in error for marker in
                          ("source file changed while packing", "published mutable source changed during capture",
                           "control file changed during bounded freeze")) else
                      "source_workspace_mount" if isinstance(error, str) and
                      "E workspace is not its enrolled shared ext4 mount" in error else
                      "source_predecessor" if isinstance(error, str) and "original migration" in error else "dependency")
            repaired = _dependency_reference_repair_applies(operation, name, data)
            if repaired:
                reason = 'source_dependencies_repaired_retry'
            if (reason == 'source_exact_pin_missing' and name == 'fred-crypto-macro'
                    and repo is not None and recovery_workspace is not None):
                repair_root = recovery_workspace/'migration/fred-exact-pin-recovery-v1'
                repair = metadata(repair_root/'status.json')
                binding = metadata(repo/'data_fred_crypto_macro/state/retained-raw.json')
                repair_rows = fred_pin_recovery_tasks(recovery_workspace, workspace_checked=True, now=now)
                snapshot = repair.get('original_snapshot_id')
                repaired = (bool(repair_rows) and repair_rows[0]['state'] == 'completed'
                    and binding.get('contract') == 'bounded_raw_history_v1'
                    and binding.get('dataset') == name and binding.get('manifest_sha256') == repair.get('original_manifest_sha256')
                    and binding.get('snapshot_id') == snapshot and isinstance(snapshot, str)
                    and isinstance(error, str) and snapshot in error
                    and timestamp(data.get('started_at_utc')) is not None and clock(repair) is not None
                    and clock(repair) > timestamp(data['started_at_utc']))
                if repaired:
                    reason = 'source_exact_pin_repaired_retry'
            trading_deferred = priority_resume is not None and bool(data.get('freshness_refresh')) and 'snapshot_id' not in data
            row = task("migration_dependency", name, state="waiting" if owner_busy or repaired or trading_deferred else "blocked",
                phase='source_refresh_trading_wait' if trading_deferred else "source_dependency",
                observed=clock(progress) or clock(current), generation=data.get("attempt_id"), reason=reason)
            if reason == 'physical_capacity' and not error and not isinstance(dependency, dict):
                row['_capacity_recheck_minimum'] = number(capacity_wait.get('minimum_write_headroom_bytes'))
                row['_capacity_recheck_observed'] = timestamp(capacity_wait.get('observed_at_utc'))
            retry_at = timestamp(data.get('retry_at_unix'))
            if trading_deferred:
                retry_at = max(priority_resume, retry_at or 0)
            if repaired and retry_at is not None and retry_at <= (time.time() if now is None else now):
                retry_at = None  # An expired backoff is not an actual future start time.
            row['next_attempt_at_utc'] = iso(retry_at)
            if (data.get('error_type') == 'OSError' and isinstance(error, str)
                    and re.match(r'^\[Errno 5\](?:\s|$)', error)):
                # This is the previous attempt, not proof of a current I/O
                # failure. Never publish the original error or private path.
                row['previous_source_failure'] = 'io_error'
            row["subject"] = name if name in SUBJECTS else None
            rows.append(row)
        if name == active:
            publication = metadata(operation / 'publication' / (name + '.json'))
            publication_stage = publication.get('stage')
            if ('snapshot_id' not in data and data.get('state') == 'processing'
                    and isinstance(data.get('attempt_id'), str) and data['attempt_id']
                    and publication.get('attempt_id') == data.get('attempt_id')
                    and isinstance(publication_stage, str) and publication_stage in PHASES):
                counters = publication_counters(publication)
                # New small files are routed now and hashed while packing in
                # the next phase. files_completed counts only files already
                # hashed; its denominator here is NOT the whole source tree.
                # Version the counter interpretation so old ETA samples reset.
                publishing = task('migration_publication', name, state='running', phase=publication_stage,
                    observed=clock(publication), generation=(data.get('attempt_id'), 'source_phase_counters_v4',
                        publication.get('sqlite_state_identity'), publication.get('sqlite_substage')),
                    metrics={'processed_source_files': publication.get('files_completed'),
                             'total_source_files': publication.get('files_total'),
                             'processed_source_bytes': publication.get('logical_bytes_processed'),
                             'total_source_bytes': publication.get('logical_bytes_total'),
                             'sqlite_state_index': publication.get('sqlite_state_index'),
                             'sqlite_states_total': publication.get('sqlite_states_total')}, **counters)
                publishing['_whole_root_refresh_yields'] = bool(data.get('freshness_refresh'))
                publishing['subject'] = name if name in SUBJECTS else None
                publishing['eta_work'] = publication_eta_work(publication)
                if name == 'tw-minute-train' and publication_stage == 'preparing_source' and repo is not None:
                    observed = _tw_minute_validation_progress(repo, owner,
                        now=time.time() if now is None else now)
                    if observed is not None:
                        publishing.update(phase='source_semantic_validation',
                            observed_at_utc=iso(observed['observed_at_epoch']),
                            generation=key((data['attempt_id'], observed['manifest_sha256'],
                                            observed['validator_sha256'], observed['auditor_start_ticks'])),
                            completed_items=observed['completed'], total_items=observed['total'],
                            completed_bytes=None, total_bytes=None)
                rows.append(publishing)
            p = metadata(operation / "recovery" / (name + ".progress.json"))
            if p and p.get("attempt_id") == data.get("attempt_id"):
                nodes = p.get('worker_nodes') if isinstance(p.get('worker_nodes'), dict) else {}
                cooperative = p.get('stage') == 'parallel-pack-reconstruction'
                remote_work = cooperative and ((number(nodes.get('vast')) or 0) > 0
                    or (number(p.get('pending_remote_packs')) or 0) > 0)
                pending = [number(p.get(key)) for key in ('pending_local_packs', 'pending_remote_packs')]
                rows.append(task("migration", "active-recovery", state="running",
                    node='penguin_vast' if remote_work else 'penguin',
                    phase='pack_reconstruction' if cooperative else
                          p.get("stage") if p.get("stage") in PHASES else "working", observed=clock(p),
                    generation=(data.get("attempt_id"), data.get("manifest_sha256")),
                    done_bytes=None if p.get('stage') == 'check-owned-scratch-process-references'
                               else p.get("completed_bytes"),
                    total_bytes=None if p.get('stage') == 'check-owned-scratch-process-references'
                                else p.get("total_bytes"),
                    # This stage inherits the preceding file counter; it does
                    # not measure the independent process-reference census.
                    done_items=None if p.get('stage') == 'check-owned-scratch-process-references'
                               else p.get("completed_items"),
                    total_items=None if p.get('stage') == 'check-owned-scratch-process-references'
                                else p.get("total_items"),
                    metrics={'penguin_packs': nodes.get('local'), 'vast_packs': nodes.get('vast'),
                             'active_jobs': sum(pending) if None not in pending else None} if cooperative else {}))
    # A full reconstruction receipt binds the cooperative counts. Do not infer
    # success from process liveness, an empty queue or a pack counter alone.
    for name, data in datasets.items():
        if not isinstance(data, dict) or not re.fullmatch(r"[a-z0-9_-]{1,96}", name):
            continue
        receipt = metadata(operation / "recovery" / (name + ".json"))
        pack = receipt.get("pack_reconstruction_execution")
        if not isinstance(pack, dict) or pack.get("contract") != "exact_pack_reconstruction_lanes_v1":
            continue
        nodes = pack.get("nodes") if isinstance(pack.get("nodes"), dict) else {}
        # The verifier records only lanes that executed work (a local fallback
        # may have no vast key). Missing known lanes are zero; malformed or
        # unknown lanes cannot contribute to a closed reconstruction proof.
        local, remote, retained = (number(nodes.get(key, 0)) for key in ("local", "vast", "retained"))
        jobs = number(pack.get("jobs"))
        closed_nodes = (bool(nodes) and set(nodes) <= {"local", "vast", "retained"}
                        and None not in (local, remote, retained)
                        and local + remote + retained == jobs)
        retained_only = closed_nodes and retained == jobs and jobs is not None and jobs > 0
        proof = (receipt.get("full_original_file_reconstruction_verified") is True
                 and receipt.get("manifest_sha256") == data.get("manifest_sha256")
                 and jobs is not None and jobs > 0
                 and closed_nodes)
        row = task("retained_verification" if retained_only else "cooperative", name, node="penguin" if retained_only else "penguin_vast", state="completed" if proof else "unknown",
            phase="pack_reconstruction", observed=timestamp(data.get("verified_at_utc")),
            generation=data.get("manifest_sha256"), done_items=jobs if proof else None,
            total_items=jobs, proof=proof, metrics={"penguin_packs": local, "vast_packs": remote, "retained_packs": retained})
        row["subject"] = name if name in SUBJECTS else None
        rows.append(row)
    return rows


def retirement_tasks(repo: Path, config_path: Path) -> list[dict]:
    config = metadata(config_path)
    state_root = Path(config.get("state_root", "service_data/storage-retirement"))
    if not state_root.is_absolute():
        state_root = repo / state_root
    queue = metadata(state_root / "status.json")
    states = queue.get("cohorts", {})
    states = states if isinstance(states, dict) else {}
    cohorts = config.get("cohorts", [])
    rows = []
    for index, enrolled in enumerate(cohorts[:32] if isinstance(cohorts, list) else []):
        if not isinstance(enrolled, dict) or not isinstance(enrolled.get("id"), str):
            continue
        status = states.get(enrolled["id"], {})
        status = status if isinstance(status, dict) else {}
        relative = enrolled.get("receipt_directory")
        if not isinstance(relative, str) or Path(relative).is_absolute() or ".." in Path(relative).parts:
            continue
        directory = repo / relative
        progress = metadata(directory / "progress.json")
        acceptance = status.get("acceptance") or {}
        accepted = (status.get("state") == "accepted" and isinstance(acceptance, dict)
            and acceptance.get("inventory_sha256") == enrolled.get("inventory_sha256")
            and (not enrolled.get("manifest_sha256")
                 or acceptance.get("manifest_sha256") == enrolled["manifest_sha256"])
            and progress.get("inventory_sha256") == enrolled.get("inventory_sha256")
            and progress.get("all_selected_retired") is True
            and number(acceptance.get("reclaimed_allocated_bytes")) is not None
            and acceptance.get("reclaimed_allocated_bytes") == progress.get("reclaimed_allocated_bytes"))
        phase, counter = {}, {}
        bound = progress.get("inventory_sha256") == enrolled.get("inventory_sha256") and isinstance(enrolled.get("inventory_sha256"), str)
        if not accepted and bound:
            for child in children(directory):
                candidate = metadata(child / "phase.json")
                if (clock(candidate) or 0) <= (clock(phase) or 0):
                    continue
                phase, counter = candidate, {}
                for filename in ("recovery-progress.json", "quarantine-audit-progress.json",
                                 "publication-progress.json", "preparation-progress.json"):
                    value = metadata(child / filename)
                    if (clock(value) or 0) >= (clock(phase) or math.inf) and (clock(value) or 0) > (clock(counter) or 0):
                        counter = value
                # Stage observations are not acceptance. Admit them only for
                # this enrolled cold release and the current owner's phase.
                if phase.get("phase") in {"plan-encoding-stage-prune", "apply-encoding-stage-prune",
                                          "prune-verified-encoding-stage"}:
                    prune = metadata(child / "stage-prune-progress.json")
                    retired = metadata(child / "retirement.json")
                    if (prune.get("contract") == "manual-stage-prune-progress-v1"
                        and prune.get("acceptance_proof") is False
                        and isinstance(enrolled.get("manifest_sha256"), str)
                        and prune.get("manifest_sha256") == enrolled["manifest_sha256"] == retired.get("manifest_sha256")
                        and prune.get("dataset") == retired.get("dataset") == child.name
                        and isinstance(prune.get("snapshot_id"), str)
                        and prune["snapshot_id"] == retired.get("snapshot_id")
                        and retired.get("deleted") is True and retired.get("cold_primary_verified") is True
                        and isinstance(prune.get("prune_step"), str) and prune["prune_step"] in PRUNE_PHASES
                        and (phase["phase"] == "prune-verified-encoding-stage"
                             or prune["prune_step"].startswith(phase["phase"].split("-", 1)[0] + "-"))
                        and (clock(prune) or 0) >= (clock(phase) or math.inf)
                        and (clock(prune) or 0) > (clock(counter) or 0)):
                        counter = prune
        running = phase.get("state") == "running" or status.get("state") == "running"
        queue_state = status.get("state") if isinstance(status.get("state"), str) else ""
        # Before the owner starts there is no execution progress receipt. A
        # queue observation may describe waiting, never completion, only when
        # it matches the complete current enrollment, inventory and target.
        queue_bound = (queue.get("contract") == "stockagent.frozen_retirement_queue.v1"
            and status.get("enrollment_sha256") == hashlib.sha256(json.dumps(enrolled, sort_keys=True).encode()).hexdigest()
            and status.get("inventory_sha256") == enrolled.get("inventory_sha256")
            and status.get("receipt_directory") == relative)
        preflight = not accepted and queue_bound and queue_state in {
            "enrolled", "ready", "waiting_preflight", "waiting_capacity"}
        capacity_wait = preflight and queue_state == "waiting_capacity"
        source_summary = status.get("source_summary") if queue_bound else None
        source_summary = source_summary if isinstance(source_summary, dict) else {}
        identity_block = queue_state.startswith("blocked_")
        exhausted = queue_state in {"needs_attention", "retry_exhausted"}
        blocked = identity_block or exhausted or queue_state in {"blocked", "error", "waiting_retry"}
        pruning = counter.get("contract") == "manual-stage-prune-progress-v1"
        blocked = blocked or (pruning and counter.get("state") in {"failed", "interrupted"})
        p = counter.get("stage") or phase.get("phase") or "waiting_preflight"
        if pruning:
            p = PRUNE_PHASES[counter["prune_step"]]
        if accepted:
            p = "retired"
        if identity_block or exhausted:
            p, counter = "retirement-proof-reconciliation", {}
        if preflight:
            p, counter, running = "capacity-preflight" if capacity_wait else "waiting_preflight", {}, False
        publishing = counter.get('contract') == 'manual-retirement-publication-progress-v1'
        counters = (publication_counters(counter) if publishing and not accepted else {
            'done_bytes': counter.get('completed_bytes', counter.get('source_bytes_completed')),
            'total_bytes': counter.get('total_bytes', counter.get('source_bytes_total')),
            'done_items': acceptance.get('items') if accepted else counter.get('completed_items', counter.get('files_completed')),
            'total_items': acceptance.get('items') if accepted else counter.get('total_items', counter.get('files_total'))})
        waiting = isinstance(p, str) and p.startswith("wait-for-")
        match = re.fullmatch(r"workflow-round(\d{1,4})-\d{8}", enrolled["id"])
        row = task("retirement", enrolled["id"], state="completed" if accepted else "blocked" if blocked else "running" if running and not waiting else "waiting",
            phase=p if p in PHASES else "working", observed=clock(progress) if accepted else
                clock(queue) if identity_block or exhausted or preflight else clock(counter) or clock(phase) or clock(queue),
            activity=clock(queue), generation=(enrolled.get("inventory_sha256"), phase.get("started_at_epoch"), counter.get("attempt"),
                counter.get("prune_step"), counter.get("stage"), counter.get("item_scope"), counter.get("metadata_only"),
                'source_phase_counters_v2' if publishing else None), **counters,
            proof=accepted, reason="physical_capacity" if capacity_wait else
                "retirement_identity" if identity_block else "owner_retry_limit" if exhausted else
                "dependency" if blocked else "original_owner" if (waiting or not running) and not accepted else None,
            metrics={"reclaimed_allocated_bytes": acceptance.get("reclaimed_allocated_bytes") if accepted else None,
                "planned_reclaim_allocated_bytes": source_summary.get("allocated_unique_file_bytes_frozen") if not accepted else None,
                "planned_source_files": source_summary.get("files_frozen") if not accepted else None},
            batch_number=int(match[1]) if match else index + 1)
        row["next_attempt_at_utc"] = iso(timestamp(status.get("next_attempt_epoch")))
        if publishing and not accepted:
            row['eta_work'] = publication_eta_work(counter)
        if not bound and not preflight:
            row.update(state="unknown", reason="metadata_unavailable")
        rows.append(row)
    return rows


def local_testing_cleanup_tasks(repo: Path, workspace: Path | None = None) -> list[dict]:
    from stockagent.data_sync.testing_disposition import LOCAL_TESTING_CONTRACT, identity
    rows = []
    # Explicit generations only. A newer work-copy decision must not overwrite
    # or reclassify the earlier policy's completion and reclaimed bytes.
    for generation in ('20261102', '20261103', '20261106', '20261107', '20261107b', '20261108', '20261108_v2', '20261108_r21', '20261108_rounds', '20261108_reviewed', '20261108_monitor_images'):
        if generation in {'20261107', '20261107b'}:
            if repo.resolve() != Path(__file__).resolve().parents[2]:
                continue  # A fixture or another checkout must not observe this host's policy.
            base = Path('/var/lib/stockagent')/('cleanup-user-'+generation)
            policy, value = metadata(base/'policy.json'), metadata(base/'state/status.json')
        elif generation == '20261108_rounds':
            policy = metadata(repo/'configs/data_sync/testing_discard_opening_rounds_20261108.json')
            value = metadata(workspace/'verify/testing-discard-opening-rounds-20261108-v1/status.json') if workspace else {}
        elif generation == '20261108_reviewed':
            policy = metadata(repo/'configs/data_sync/testing_discard_opening_reviewed_20261108.json')
            value = metadata(workspace/'verify/testing-discard-opening-reviewed-20261108-v1/status.json') if workspace else {}
        elif generation == '20261108_monitor_images':
            policy = metadata(repo/'configs/data_sync/testing_discard_monitor_images_20261108.json')
            value = metadata(workspace/'verify/testing-discard-monitor-images-20261108-v1/status.json') if workspace else {}
        elif generation == '20261108_r21':
            policy = metadata(repo/'configs/data_sync/testing_discard_opening_r21_20261108.json')
            value = metadata(workspace/'verify/testing-discard-opening-r21-20261108-v1/status.json') if workspace else {}
        elif generation in {'20261108', '20261108_v2'}:
            policy = metadata(repo / f'configs/data_sync/local_testing_artifact_disposition_{generation}.json')
            suffix = 'v2' if generation.endswith('_v2') else 'v1'
            value = metadata(workspace/f'migration/local-testing-cleanup-20261108-{suffix}/status.json') if workspace else {}
        else:
            policy = metadata(repo / f'configs/data_sync/local_testing_artifact_disposition_{generation}.json')
            value = metadata(repo / f'service_data/storage-discard/performance-{generation}/status.json')
        if not policy and not value:
            continue
        targets = policy.get('targets')
        binding = (policy.get('contract') == LOCAL_TESTING_CONTRACT
            and policy.get('identity_sha256') == identity({k:v for k,v in policy.items() if k != 'identity_sha256'})
            and value.get('contract') == 'stockagent.local_testing_cleanup.v1'
            and value.get('policy_identity_sha256') == policy.get('identity_sha256')
            and value.get('apply') is True and value.get('preservation_waived_by_user') is True
            and value.get('recovery_claimed') is False
            and value.get('cold_objects_deleted') is False and value.get('nas_objects_deleted') is False
            and isinstance(targets, list) and number(value.get('targets_total')) == len(targets))
        if not binding:
            row = task('local_testing_cleanup', f'local-performance-{generation}', reason='metadata_unavailable')
            if generation in {'20261108', '20261108_v2', '20261108_r21'}:
                row['_one_off_without_timer'] = True
            if generation in {'20261108_rounds', '20261108_reviewed', '20261108_monitor_images'}:
                row['_opening_perf_timer'] = True
            rows.append(row)
            continue
        completed = value.get('state') == 'completed' and number(value.get('targets_completed')) == len(targets)
        waiting = value.get('state') == 'waiting_protected_targets'
        exact_rounds = generation == '20261108_rounds' and policy.get('identity_sha256') == OPENING_ROUNDS_POLICY_SHA256
        exact_reviewed = generation == '20261108_reviewed' and policy.get('identity_sha256') == OPENING_REVIEWED_POLICY_SHA256
        exact_monitor_images = generation == '20261108_monitor_images' and policy.get('identity_sha256') == MONITOR_IMAGES_POLICY_SHA256
        phase = ('disposable_cleanup_completed' if completed else 'opening_perf_cleanup_working'
                 if exact_rounds else value.get('state'))
        row = task('local_testing_cleanup', policy['identity_sha256'],
            state='completed' if completed else 'blocked' if waiting else 'running',
            phase=phase if phase in PHASES else 'working', observed=clock(value),
            generation=(policy['identity_sha256'], 'disposable_workcopies_v1'),
            done_items=number(value.get('targets_completed')), total_items=len(targets), proof=completed,
            reason='dependency' if waiting else None,
            metrics={'discarded_allocated_bytes': number(value.get('reclaimed_allocated_bytes')),
                     'blocked_items': number(value.get('targets_blocked'))})
        if generation in {'20261108', '20261108_v2', '20261108_r21'}:
            row['_one_off_without_timer'] = True
        if exact_rounds or exact_reviewed or exact_monitor_images:
            row['_opening_perf_timer'] = True
        rows.append(row)
    return rows


def vast_testing_cleanup_tasks(workspace: Path | None) -> list[dict]:
    """Expose only the fixed, successfully applied Vast cleanup receipt."""
    if workspace is None:
        return []
    from stockagent.data_sync.testing_disposition import CONTRACT as POLICY_CONTRACT, identity

    root = workspace/'verify/vast-old-capacity-bench-20261108-v1'
    policy, receipt = metadata(root/'policy.json'), metadata(root/'apply.json')
    if not policy and not receipt:
        return []
    targets, results = policy.get('targets'), receipt.get('results')
    bound = (policy.get('contract') == POLICY_CONTRACT
             and policy.get('authority_node_id') == 'penguin'
             and policy.get('origin_node_id') == 'vastai1T'
             and policy.get('decision') == 'discard_without_preservation'
             and policy.get('identity_sha256') == VAST_CAPACITY_TEST_POLICY_SHA256
             and identity({k: v for k, v in policy.items() if k != 'identity_sha256'})
                 == VAST_CAPACITY_TEST_POLICY_SHA256
             and receipt.get('policy_identity_sha256') == VAST_CAPACITY_TEST_POLICY_SHA256
             and isinstance(targets, list) and len(targets) == 3
             and isinstance(results, list) and len(results) == 3
             and receipt.get('targets') == receipt.get('deleted_targets') == 3
             and receipt.get('failed_or_protected_targets') == 0
             and receipt.get('cold_deletion_claimed') is False
             and clock(receipt) is not None
             and all(isinstance(row, dict) and row.get('target') == target
                     and row.get('policy_identity_sha256') == VAST_CAPACITY_TEST_POLICY_SHA256
                     and row.get('state') == 'discarded-without-preservation'
                     and row.get('deleted') is True and row.get('recovery_claimed') is False
                     and row.get('external_shared_names_deleted') is False
                     and number(row.get('reclaimed_allocated_bytes')) is not None
                     for target, row in zip(targets, results))
             and number(receipt.get('reclaimed_allocated_bytes')) is not None)
    if bound:
        bound = sum(row['reclaimed_allocated_bytes'] for row in results) == receipt['reclaimed_allocated_bytes']
    if not bound:
        return [task('vast_testing_cleanup', 'vast-capacity-bench-20261108', node='vastai1T',
                     reason='metadata_unavailable')]
    return [task('vast_testing_cleanup', VAST_CAPACITY_TEST_POLICY_SHA256, node='vastai1T',
                 state='completed', phase='disposable_cleanup_completed', observed=clock(receipt),
                 generation=(VAST_CAPACITY_TEST_POLICY_SHA256, 'vast_disposable_workcopies_v1'),
                 done_items=3, total_items=3, proof=True,
                 metrics={'discarded_allocated_bytes': receipt['reclaimed_allocated_bytes']})]


def _rescue_manifest_counts(sources, cold: Path, *, mount_check=None, resolve=None):
    """Count bounded, exact D metadata while rejecting mount or generation drift."""
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync.cold_primary import _check_d_primary_mount

    if not isinstance(sources, list) or not 0 < len(sources) <= 64:
        raise ValueError('rescue source metadata exceeds monitor bound')
    check, resolve_one = mount_check or _check_d_primary_mount, resolve or packed.resolve_packed_snapshot_id
    check(cold)
    found, missing, mismatch, seen = 0, 0, 0, set()
    for binding in sources:
        if not isinstance(binding, dict):
            raise ValueError('invalid rescue source metadata')
        dataset = packed.validate_slug(binding.get('dataset'), 'rescue source dataset')
        snapshot = packed.validate_slug(binding.get('snapshot_id'), 'rescue source snapshot')
        digest = binding.get('manifest_sha256')
        if (not isinstance(digest, str) or re.fullmatch(r'[0-9a-f]{64}', digest) is None
                or (dataset, snapshot) in seen):
            raise ValueError('duplicate or invalid rescue source identity')
        seen.add((dataset, snapshot))
        manifest = cold/'manifests'/dataset/(snapshot+'.json')
        if any(part.is_symlink() for part in (manifest, *manifest.parents)):
            raise ValueError('rescue manifest path is redirected')
        try:
            before = manifest.lstat()
            if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_METADATA_BYTES:
                raise ValueError('rescue manifest is not bounded regular metadata')
            fixed = resolve_one(cold, dataset, snapshot, require_objects=False)
        except FileNotFoundError:
            missing += 1
            continue
        after_manifest = manifest.lstat()
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after_manifest.st_dev, after_manifest.st_ino, after_manifest.st_size,
                    after_manifest.st_mtime_ns)):
            raise ValueError('rescue manifest changed during observation')
        if fixed.manifest_sha256 == digest:
            found += 1
        else:
            mismatch += 1
    check(cold)  # An unavailable D mount must never look like a missing manifest.
    return found, missing, mismatch, seen


def tw_rescue_original_manifest_tasks(*, now: float, state_root=Path('/var/lib/stockagent-training-rescue'),
                                      cold=Path('/srv/stockagent-packed'), mount_check=None,
                                      resolve=None) -> list[dict]:
    """Observe original TW rescue manifests; never decode payloads or change pins."""
    run = 'tw-v6-embedding-d128'
    spec_path = state_root/'runs'/run/'spec.json'
    try:
        original = spec_path.lstat()
    except OSError:
        original = None
    value = metadata(spec_path, limit=128*1024)
    sources = value.get('data_sources')
    unknown = lambda: [task('tw_rescue_original_manifests', run, reason='metadata_unavailable')]
    if (value.get('contract') != 'stockagent.training_rescue.v1' or value.get('schema_version') != 1
            or value.get('run_id') != run or not isinstance(sources, list) or not 0 < len(sources) <= 16):
        return unknown()
    try:
        found, missing, mismatch, seen = _rescue_manifest_counts(sources, cold, mount_check=mount_check,
                                                                   resolve=resolve)
        after = spec_path.lstat()
        if (original is None or (original.st_dev, original.st_ino, original.st_size, original.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
            return unknown()
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        return unknown()
    reason = ('rescue_original_manifest_mismatch' if mismatch else
              'rescue_original_manifest_missing' if missing else None)
    return [task('tw_rescue_original_manifests', run, state='blocked' if reason else 'completed',
                 phase='rescue_original_manifest_audit', observed=now,
                 generation=(run, tuple(sorted(seen))), done_items=found, total_items=len(sources),
                 proof=reason is None, reason=reason, metrics={'blocked_items': missing+mismatch})]


def training_rescue_registered_manifest_tasks(*, now: float,
        retention_state=Path('/var/lib/stockagent-packed-retention'), cold=Path('/srv/stockagent-packed'),
        mount_check=None, resolve=None) -> list[dict]:
    """Report exact manifest availability for the monotonic rescue reference set."""
    from stockagent.data_sync import rescue_source_references
    try:
        sources = rescue_source_references.load(retention_state)
        if not sources:
            return []
        found, missing, mismatch, seen = _rescue_manifest_counts(sources, cold, mount_check=mount_check,
                                                                   resolve=resolve)
        if rescue_source_references.load(retention_state) != sources:
            raise ValueError('rescue reference registry changed during observation')
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        return [task('training_rescue_manifests', 'rescue-registered', reason='metadata_unavailable')]
    reason = ('rescue_registered_manifest_mismatch' if mismatch else
              'rescue_registered_manifest_missing' if missing else None)
    return [task('training_rescue_manifests', 'rescue-registered',
                 state='blocked' if reason else 'completed', phase='rescue_original_manifest_audit',
                 observed=now, generation=('rescue-registered', tuple(sorted(seen))),
                 done_items=found, total_items=len(sources), proof=reason is None, reason=reason,
                 metrics={'blocked_items': missing+mismatch})]


def training_rescue_operational_tasks(state_root=Path('/var/lib/stockagent-training-rescue'),
                                      retention_state=Path('/var/lib/stockagent-packed-retention')) -> list[dict]:
    """Project the original owner's durable control observations only."""
    from stockagent.data_sync import rescue_source_references
    from stockagent.data_sync.desync_snapshots import SnapshotError

    rows = []
    value = metadata(state_root/'rescue-cycle-status.json', limit=16384)
    if value:
        valid = (value.get('contract') == 'stockagent.rescue_capture_cycle.v1'
                 and value.get('state') in {'waiting_d_capacity', 'cycle_observed'}
                 and value.get('source_retirement') is False
                 and value.get('recovery_or_nas_acceptance_claimed') is False and clock(value) is not None)
        if valid:
            blocked = value['state'] == 'waiting_d_capacity'
            row = task('training_rescue_capture', 'rescue-owner', state='blocked' if blocked else 'waiting',
                       phase='rescue_capture_waiting_capacity' if blocked else 'rescue_capture_cycle_observed',
                       observed=clock(value), reason='rescue_capture_D_capacity' if blocked else 'progress_pending',
                       metrics={'D_observed_free_bytes': number(value.get('D_available_bytes')),
                                'D_reserve_bytes': number(value.get('D_reserve_bytes')),
                                'D_capacity_observed_epoch': int(clock(value)) if blocked else None})
        else:
            row = task('training_rescue_capture', 'rescue-owner', reason='metadata_unavailable')
        rows.append(row)
    value = metadata(state_root/'rescue-source-reference-status.json')
    if value:
        try:
            bindings = rescue_source_references.load(retention_state)
            count = len(bindings)
            proof = (value.get('contract') == 'stockagent.rescue_source_reconciliation.v1'
                     and value.get('state') == 'completed' and value.get('protected_unique_bindings') == count
                     and value.get('fixed_bindings') == bindings
                     and value.get('source_objects_checked') is False
                     and value.get('recovery_or_nas_acceptance_claimed') is False and clock(value) is not None)
        except (OSError, ValueError, TypeError, SnapshotError):
            bindings, count, proof = None, None, False
        waiting = (bindings is not None and value.get('contract') == 'stockagent.rescue_source_reconciliation.v1'
                   and value.get('state') == 'waiting' and clock(value) is not None)
        completed = metadata(state_root/'rescue-source-reference-completed.json')
        fixed = completed.get('fixed_bindings')
        def binding_key(row):
            return (row['dataset'], row['snapshot_id'], row['manifest_sha256'])
        try:
            last = clock(completed) if (bindings is not None
                and completed.get('contract') == 'stockagent.rescue_source_reconciliation.v1'
                and completed.get('state') == 'completed' and isinstance(fixed, list)
                and completed.get('protected_unique_bindings') == len(fixed)
                and completed.get('source_objects_checked') is False
                and completed.get('recovery_or_nas_acceptance_claimed') is False
                and len({binding_key(item) for item in fixed}) == len(fixed)
                and {binding_key(item) for item in fixed}.issubset({binding_key(item) for item in bindings})) else None
        except (KeyError, TypeError):
            last = None
        row = task('training_rescue_references', 'rescue-owner',
                   state='completed' if proof else 'waiting' if waiting else 'unknown',
                   phase='rescue_references_reconciled' if proof else 'working', observed=clock(value),
                   done_items=count if proof else None, total_items=count if proof else None,
                   proof=proof, reason=None if proof else 'original_owner' if waiting else 'metadata_unavailable',
                   metrics={'rescue_protected_bindings': count,
                            'rescue_last_reconciled_epoch': int(last) if last is not None else None})
        rows.append(row)
    return rows


def source_replication_pause_tasks(state_root: Path) -> list[dict]:
    marker = metadata(state_root/'source-replication-migration-pause.json', limit=4096)
    if not marker:
        return []
    if (marker.get('contract') != 'stockagent.source_replication_migration_pause.v1'
            or marker.get('paused') is not True):
        return [task('source_replication_pause', 'source-wave', state='blocked', reason='metadata_unavailable')]
    requested = timestamp(marker.get('requested_at_epoch'))
    status = metadata(state_root/'source-replication-status.json')
    observed = clock(status)
    paused = (requested is not None and observed is not None and observed >= requested
              and status.get('state') == 'paused_for_user_migration')
    return [task('source_replication_pause', 'source-wave', state='completed' if paused else 'waiting',
                 phase='source_replication_paused' if paused else 'source_replication_pause_waiting',
                 observed=observed if paused else requested, proof=paused,
                 reason=None if paused else 'source_replication_pause_pending')]


def catalog_maintenance_tasks(state_root: Path) -> list[dict]:
    value = metadata(state_root/'catalog-activity-observation.json', limit=4096)
    if value.get('contract') != 'stockagent.catalog_activity_observation.v1':
        return []
    state = value.get('state')
    reason = value.get('reason')
    valid = ((state == 'running' and reason is None)
             or (state == 'waiting' and reason in {'tw_day_trade_priority_window', 'catalog_owner_busy',
                                                    'catalog_cold_capacity', 'catalog_cycle_complete'})
             or (state == 'blocked' and reason == 'catalog_refresh_failed'))
    if not valid or value.get('catalog_acceptance_claimed') is not False or clock(value) is None:
        return []
    row = task('catalog_maintenance', 'current-catalog', state=state, phase='catalog_maintenance_check',
               observed=clock(value), activity=clock(value) if state == 'running' else None,
               reason=reason)
    if state == 'waiting' and reason in {'tw_day_trade_priority_window', 'catalog_owner_busy',
                                        'catalog_cold_capacity'}:
        row['next_attempt_at_utc'] = iso(clock(value) + (15 if reason == 'catalog_owner_busy' else 60))
    return [row]


def _D_progress_counters(progress: dict) -> dict:
    if progress.get('stage') == 'comparing_D_existing_inventory':
        return {'done_items': number(progress.get('files_completed')),
                'total_items': number(progress.get('files_total')),
                'done_bytes': number(progress.get('logical_bytes_processed')),
                'total_bytes': number(progress.get('logical_bytes_total'))}
    if progress.get('stage') == 'validating_D_inventory':
        return {'done_items': number(progress.get('inventory_items_completed')),
                'total_items': number(progress.get('inventory_items_total'))}
    return {'done_items': number(progress.get('objects_completed')),
            'total_items': number(progress.get('objects_total')),
            'done_bytes': number(progress.get('encoded_bytes_streamed', progress.get('encoded_bytes_installed'))),
            'total_bytes': number(progress.get('encoded_bytes_total'))}


def _inflight_E_inventory_clock(workspace: Path, name: str, expected: str, *,
                                queue: str = 'stream-source-repack-v1'):
    """Recognize an older worker's actual inventory write without inventing a counter."""
    if (queue not in {'stream-source-repack-v1', 'source-local-dedup-candidate-v1'}
            or source_subject(name) is None or not isinstance(expected, str)
            or re.fullmatch(r'[a-f0-9]{64}', expected) is None):
        return None
    stage = workspace/'migration'/queue/name/expected
    try:
        if stage.is_symlink() or not stage.is_dir() or stage.resolve(strict=True) != stage:
            return None
        candidates = [path for path in stage.glob('inventory-*.partial')
                      if re.fullmatch(r'inventory-[a-f0-9]{32}\.partial', path.name)]
        if len(candidates) != 1:
            return None
        path = candidates[0]
        info = path.lstat()
        if path.is_symlink() or not path.is_file() or info.st_nlink != 1:
            return None
        return finite(info.st_mtime)
    except (OSError, ValueError, RuntimeError):
        return None


def vast_sealed_e_handoff_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    if not workspace_checked:
        return []
    value = metadata(workspace/'migration/vast-pre-stop-sealed-e-v1/status.json')
    if (value.get('contract') != 'stockagent.vast_sealed_e_handoff.v1'
            or value.get('destination_readback') is not False
            or type(value.get('source_deleted')) is not bool
            or type(value.get('mount_handoff_completed')) is not bool):
        return []
    state, observed = value.get('state'), clock(value)
    if state in {'copying', 'retiring'} and _process_identity_matches(value.get('owner_identity')):
        event = value.get('progress') if isinstance(value.get('progress'), dict) else {}
        return [task('vast_sealed_e_handoff', 'fixed-vast-rescue', state='running',
            phase='vast_sealed_retirement' if state == 'retiring' else 'vast_sealed_copy',
            observed=observed, generation='vast-rescue-fixed-v1',
            done_bytes=number(event.get('bytes_completed')), total_bytes=number(event.get('bytes_total')),
            done_items=number(event.get('files_completed')), total_items=number(event.get('files_total')))]
    if state == 'copied_waiting_handoff':
        count, size = number(value.get('files_completed')), number(value.get('bytes_completed'))
        return [task('vast_sealed_e_handoff', 'fixed-vast-rescue', state='waiting',
            phase='vast_sealed_copy_ready', observed=observed, generation='vast-rescue-fixed-v1',
            done_bytes=size, total_bytes=size, done_items=count, total_items=count,
            reason='vast_sealed_handoff_pending')]
    if state == 'completed' and value['source_deleted'] and value['mount_handoff_completed']:
        count, size = number(value.get('files_completed')), number(value.get('bytes_completed'))
        return [task('vast_sealed_e_handoff', 'fixed-vast-rescue', state='completed',
            phase='vast_sealed_completed', observed=observed, generation='vast-rescue-fixed-v1',
            done_bytes=size, total_bytes=size, done_items=count, total_items=count, proof=True)]
    if state in {'copying', 'retiring', 'waiting_retry'}:
        return [task('vast_sealed_e_handoff', 'fixed-vast-rescue', state='blocked',
            phase='vast_sealed_retirement' if value['mount_handoff_completed'] else 'vast_sealed_copy',
            observed=observed, generation='vast-rescue-fixed-v1',
            reason='vast_sealed_copy_retry')]
    return []


def handoff_source_repack_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    if not workspace_checked:
        return []
    root = workspace/'migration/handoff-source-repack-v1'
    names = ('coinmetrics-community', 'keyed-public-catalogs', 'crypto-reference',
             'dune-crypto', 'crypto-etf-history', 'crypto-historical-public')
    queue = metadata(root/'queue-status.json')
    d_queue = metadata(root/'D-queue-status.json')
    retirement_queue = metadata(root/'D-retirement-queue-status.json')
    queued = {}
    if queue.get('contract') == 'stockagent.handoff_source_repack_queue.v1':
        entries = queue.get('entries')
        if isinstance(entries, list) and len(entries) <= len(names):
            queued = {entry['dataset']: entry for entry in entries
                      if isinstance(entry, dict) and entry.get('dataset') in names}
    d_queued = {}
    if d_queue.get('contract') == 'stockagent.handoff_private_D_queue.v1':
        entries = d_queue.get('entries')
        if isinstance(entries, list) and len(entries) <= len(names):
            d_queued = {entry['dataset']: entry for entry in entries
                        if isinstance(entry, dict) and entry.get('dataset') in names}
    retirement_queued = {}
    if retirement_queue.get('contract') == 'stockagent.handoff_private_D_retirement_queue.v1':
        entries = retirement_queue.get('entries')
        if isinstance(entries, list) and len(entries) <= len(names):
            retirement_queued = {entry['dataset']: entry for entry in entries
                                 if isinstance(entry, dict) and entry.get('dataset') in names}
    rows = []
    for name in names:
        value = metadata(root/(name+'.status.json'))
        valid = value.get('contract') == 'stockagent.handoff_source_repack.v1' and value.get('dataset') == name
        pending = queued.get(name, {})
        if not valid and not pending:
            continue
        state, progress = value.get('state'), value.get('progress')
        progress = progress if isinstance(progress, dict) else {}
        if valid and state == 'running':
            phase = ('handoff_source_retiring' if progress.get('stage') == 'retiring_C_payload'
                     else 'handoff_source_packing')
            counters = {'done_items': number(progress.get('files_completed')),
                        'total_items': number(progress.get('files_total', value.get('files_total'))),
                        'done_bytes': number(progress.get('logical_bytes_processed')),
                        'total_bytes': number(progress.get('logical_bytes_total', value.get('logical_bytes_total')))}
            public_state, reason, proof = 'running', None, False
        elif valid and state in {'E_source_hashed_prepared', 'C_retirement_waiting'}:
            phase, counters = 'handoff_source_prepared', {}
            public_state = 'blocked' if state == 'C_retirement_waiting' else 'waiting'
            reason, proof = 'handoff_source_waiting', False
        elif valid and state == 'C_payload_retired_root_and_lock_preserved' and value.get('C_payload_deleted') is True:
            result = value.get('C_retirement') if isinstance(value.get('C_retirement'), dict) else {}
            if result.get('state') != state or result.get('prepared_sha256') != value.get('prepared_sha256'):
                continue
            phase = 'handoff_source_retired'
            counters = {'done_items': number(result.get('files_removed')), 'total_items': number(result.get('files_removed')),
                        'done_bytes': number(result.get('logical_bytes_removed')),
                        'total_bytes': number(result.get('logical_bytes_removed')),
                        'metrics': {'reclaimed_allocated_bytes': number(result.get('estimated_allocated_bytes_reclaimed'))}}
            public_state, reason, proof = 'completed', None, True
        else:
            phase, counters, public_state, reason, proof = ('handoff_source_queued', {}, 'waiting',
                                                           'handoff_source_queued', False)
        stale_owner = (pending.get('state') == 'running' and
                       (clock(queue) is None or time.time() - clock(queue) > 180))
        if not proof and (pending.get('state') in {'waiting', 'retrying'} or stale_owner):
            public_state = 'blocked' if pending.get('state') == 'retrying' or stale_owner else 'waiting'
            reason = 'handoff_source_retry' if public_state == 'blocked' else 'handoff_source_queued'
        row = task('handoff_source_repack', name, state=public_state, phase=phase,
                   observed=clock(value) if valid else clock(queue),
                   activity=clock(queue) if pending.get('state') == 'running' else None,
                   generation=str(value.get('original_manifest_sha256', '')),
                   proof=proof, reason=reason, **counters)
        row['subject'] = name
        rows.append(row)
        if queued and proof:
            publication = value.get('D_publication')
            committed = (value.get('D_new_head_committed') is True and isinstance(publication, dict)
                         and publication.get('dataset') == name
                         and publication.get('old_manifest_sha256') == value.get('original_manifest_sha256')
                         and publication.get('state') == 'new_D_head_committed_from_exact_E_source')
            cleaned = committed and value.get('D_E_stage_cleanup_complete') is True
            dpending = d_queued.get(name, {})
            dprogress = value.get('D_progress') if isinstance(value.get('D_progress'), dict) else {}
            live = (dpending.get('state') == 'running' and clock(d_queue) is not None
                    and time.time()-clock(d_queue) <= 180)
            if cleaned:
                dstate, dphase, dreason = 'completed', 'handoff_source_D_done', None
            elif live:
                dstate = 'running'
                dphase = ('handoff_source_D_cleanup' if committed else 'handoff_source_D_validating'
                          if dprogress.get('stage') == 'validating_D_inventory' else 'handoff_source_D_installing')
                dreason = None
            elif dpending.get('state') == 'waiting':
                dstate, dphase, dreason = 'waiting', 'handoff_source_D_waiting', 'handoff_source_D_unpublished'
            elif dpending.get('state') == 'waiting_priority':
                dstate, dphase, dreason = 'waiting', 'handoff_source_D_waiting', 'waiting_E_only_priority'
            else:
                dstate, dphase, dreason = 'blocked', 'handoff_source_D_waiting', 'handoff_private_D_retry'
            pending_d = task('handoff_private_d', name, state=dstate, phase=dphase,
                observed=clock(value), activity=clock(d_queue) if live else None, proof=cleaned,
                reason=dreason, **_D_progress_counters(dprogress))
            pending_d['subject'] = name
            rows.append(pending_d)
            if cleaned and name in retirement_queued:
                retirement = value.get('D_retirement')
                retired = (value.get('D_old_release_removed') is True and isinstance(retirement, dict)
                           and retirement.get('state') == 'completed' and retirement.get('apply') is True
                           and retirement.get('old_manifest_sha256') == value.get('original_manifest_sha256')
                           and retirement.get('new_manifest_sha256') == publication.get('new_manifest_sha256'))
                prior = retirement_queued[name]
                live = (prior.get('state') == 'running' and clock(retirement_queue) is not None
                        and time.time()-clock(retirement_queue) <= 180)
                if retired:
                    rstate, rphase, rreason = 'completed', 'handoff_private_old_done', None
                elif live:
                    rstate, rphase, rreason = 'running', 'handoff_private_old_retiring', None
                elif prior.get('state') == 'waiting':
                    rstate, rphase, rreason = 'waiting', 'handoff_private_old_waiting', 'handoff_private_old_retry'
                else:
                    rstate, rphase = 'blocked', 'handoff_private_old_waiting'
                    rreason = 'handoff_private_old_owner_busy' if prior.get('error') == 'owner_busy' else 'handoff_private_old_retry'
                metrics = {'estimated_reclaimed_allocated_bytes': number(retirement.get('estimated_allocated_bytes_reclaimed'))} if retired else {}
                row = task('handoff_private_retirement', name, state=rstate, phase=rphase,
                    observed=clock(value), activity=clock(retirement_queue) if live else None,
                    proof=retired, reason=rreason, metrics=metrics)
                row['subject'] = name
                rows.append(row)
    return rows


def staged_cold_attribution_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    if not workspace_checked:
        return []
    value = metadata(workspace/'migration/d-cold-object-staging-v1/orphan-attribution-v1.json')
    if (value.get('contract') != 'stockagent.staged_cold_orphan_attribution.v1'
            or value.get('state') != 'metadata_attributed' or value.get('payloads_read') is not False
            or value.get('complete_dataset_reconstructed') is not False or value.get('deletion_authorized') is not False):
        return []
    total, matched = number(value.get('object_count')), number(value.get('attributed_objects'))
    total_bytes, matched_bytes = number(value.get('object_bytes')), number(value.get('attributed_bytes'))
    if (total is None or matched is None or matched > total or total_bytes is None
            or matched_bytes is None or matched_bytes > total_bytes):
        return []
    errors = value.get('inventory_errors')
    deferred = value.get('deferred_batches')
    # A deferred batch can contain an unknown number of objects. The accepted
    # batches are a useful lower bound, but cannot be a whole-scope denominator.
    if not isinstance(deferred, list):
        return []
    deferred_count = len(deferred)
    error_count = (len(errors) if isinstance(errors, list) else 0) + deferred_count
    archive_errors = value.get('archive_errors')
    index_metrics = {}
    base = workspace/'migration/d-cold-object-staging-v1'
    candidate = metadata(base/'orphan-recovery-index-candidates-v1.json')
    index_status = metadata(base/'orphan-recovery-index-status-v1.json')
    try:
        from scripts.attribute_staged_recovery_index import CONTRACT as index_contract, _result_identity
        index_count = number(candidate.get('historical_index_matched_objects'))
        index_bytes = number(candidate.get('historical_index_matched_bytes'))
        new_count = number(candidate.get('not_locally_referenced_objects'))
        new_bytes = number(candidate.get('not_locally_referenced_bytes'))
        index_observed = timestamp(candidate.get('observed_at_epoch'))
        if (candidate.get('contract') == index_contract and candidate.get('state') == 'metadata_index_matched'
                and candidate.get('identity_sha256') == _result_identity(candidate)
                and candidate.get('primary_input_identity_sha256') == value.get('input_identity_sha256')
                and candidate.get('objects_scanned') == total and candidate.get('deferred_batches') == deferred_count
                and candidate.get('payloads_read') is False and candidate.get('current_NAS_readback') is False
                and candidate.get('complete_dataset_reconstructed') is False and candidate.get('deletion_authorized') is False
                and index_count is not None and 0 <= index_count <= total
                and index_bytes is not None and 0 <= index_bytes <= total_bytes
                and new_count is not None and 0 <= new_count <= index_count
                and new_bytes is not None and 0 <= new_bytes <= index_bytes and index_observed is not None
                and number(int(index_observed)) is not None):
            available = (index_status.get('contract') == index_contract+'.status'
                         and index_status.get('state') == 'available'
                         and index_status.get('candidate_identity_sha256') == candidate['identity_sha256'])
            index_metrics = {'staged_index_matched_objects': index_count, 'staged_index_matched_bytes': index_bytes,
                'staged_index_new_objects': new_count, 'staged_index_new_bytes': new_bytes,
                'staged_index_observed_epoch': int(index_observed), 'staged_index_unavailable': 0 if available else 1}
    except (ImportError, OSError, ValueError, TypeError, KeyError):
        pass
    row = task('staged_cold_attribution', 'staged-objects', state='blocked',
               phase='staged_cold_attributed', observed=timestamp(value.get('last_checked_at_epoch')) or clock(value),
               done_items=matched, total_items=None if deferred_count else total,
               done_bytes=matched_bytes, total_bytes=None if deferred_count else total_bytes,
               reason='staged_cold_partial' if deferred_count else 'staged_cold_unresolved',
               metrics={'attributed_objects': matched,
                   'unattributed_objects': total-matched, 'attributed_bytes': matched_bytes,
                   'attribution_errors': error_count, 'staged_deferred_batches': deferred_count,
                   'staged_known_subset_objects': total,
                   'archive_prefix_candidates': number(value.get('archive_prefix_candidates')),
                   'archive_mixed_or_unknown': number(value.get('archive_mixed_or_unknown')),
                   'archive_errors': len(archive_errors) if isinstance(archive_errors, list) else None,
                   **index_metrics})
    return [row]


def _index_archive_remaining(stage: Path, dataset: str, expected: str, prepared_sha: str):
    """Estimate remaining install bytes from a sealed E generation and journal."""
    from stockagent.data_sync import stream_repack
    if (not isinstance(expected, str) or re.fullmatch(r'[a-f0-9]{64}', expected) is None
            or not isinstance(prepared_sha, str) or re.fullmatch(r'[a-f0-9]{64}', prepared_sha) is None
            or stage.is_symlink() or stage.resolve(strict=True) != stage):
        return None
    prepared = stream_repack._prepared_metadata(stage, dataset, expected)
    if (prepared.get('identity_sha256') != prepared_sha
            or prepared.get('source_mode') != 'retained_handoff_C_index'
            or not isinstance(prepared.get('inventory'), dict)):
        return None
    members = [*prepared['packages'], prepared['inventory']]
    if not all(isinstance(item, dict) and re.fullmatch(r'[a-f0-9]{64}', str(item.get('sha256', '')))
               and number(item.get('bytes')) is not None for item in members):
        return None
    sizes = {item['sha256']: item['bytes'] for item in members}
    if len(sizes) != len(members):
        return None
    journal_path = stage/'install.json'
    if not os.path.lexists(journal_path):
        return sum(sizes.values())
    if journal_path.is_symlink():
        return None
    journal = metadata(journal_path)
    if (journal.get('contract') != 'stockagent.stream_source_local_repack.v1.install'
            or journal.get('dataset') != dataset or journal.get('old_manifest_sha256') != expected
            or journal.get('prepared_sha256') != prepared_sha
            or journal.get('state') not in {'installing', 'manifest_planned', 'head_pending'}
            or not isinstance(journal.get('installed'), list)):
        return None
    installed = [item.get('sha256') for item in journal['installed'] if isinstance(item, dict)]
    if (len(installed) != len(journal['installed']) or len(set(installed)) != len(installed)
            or not set(installed).issubset(sizes)):
        return None
    return sum(sizes[digest] for digest in sizes.keys()-set(installed))


def handoff_index_coverage_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    """Observe a source-bound D inventory comparison, never D acceptance."""
    if not workspace_checked:
        return []
    from scripts import run_handoff_index_archive_queue as archive_queue, run_handoff_index_coverage_queue as coverage
    from stockagent.data_sync import index_archive_existing as existing, stream_repack
    root = workspace/'migration/handoff-index-archive-v1'
    current = metadata(root/'coverage-precheck-status.json')
    current_time = clock(current)
    active = (current.get('contract') == coverage.CONTRACT and current.get('state') == 'running'
              and current_time is not None and -60 <= time.time()-current_time <= 180
              and type(current.get('owner_pid')) is int and current['owner_pid'] > 0
              and archive_queue.lock_owner_pid(root/'coverage-precheck-owner.lock') == current['owner_pid'])
    rows = []
    for dataset in coverage.DATASETS:
        source = metadata(root/(dataset+'.status.json'))
        value = metadata(root/(dataset+'.coverage-status.json'))
        if not source and not value:
            continue
        live = active and current.get('dataset') == dataset
        if live:
            value = current
        identity = value.get('source_identity')
        valid = (value.get('contract') == coverage.CONTRACT and value.get('dataset') == dataset
                 and isinstance(identity, str) and re.fullmatch(r'[0-9a-f]{64}', identity) is not None
                 and source.get('source_identity') == identity)
        proof = False
        if valid and value.get('state') == 'completed':
            try:
                stage = root/dataset/identity
                prepared = stream_repack._prepared_metadata(stage, dataset, identity)
                match = metadata(stage/'existing-D-match.json')
                receipt = {'state':'existing_D_head_matches_source_inventory', 'dataset':dataset,
                    'old_manifest_sha256':identity, 'new_snapshot_id':match.get('snapshot_id'),
                    'new_manifest_sha256':match.get('manifest_sha256'), 'files':match.get('files'),
                    'logical_bytes':match.get('logical_bytes'), 'existing_match':match}
                proof = (value.get('comparison_identity') in (None, match.get('identity_sha256'))
                         and value.get('matched_files') == match.get('files')
                         and value.get('matched_logical_bytes') == match.get('logical_bytes')
                         and value.get('D_or_NAS_readback') is False and value.get('source_files_deleted') == 0
                         and existing.receipt_matches(receipt, prepared['identity_sha256']))
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                proof = False
        progress = value.get('progress') if isinstance(value.get('progress'), dict) else {}
        owner_busy = valid and value.get('state') == 'retrying' and value.get('reason') == 'PackedPublicationBusy'
        state = ('completed' if proof else 'running' if valid and live else 'waiting' if owner_busy else
                 'blocked' if valid and value.get('state') in {'retrying', 'completed'} else 'waiting')
        scanning_parent = (state == 'running' and (progress.get('stage') == 'scanning_D_parent_inventory'
                           or progress.get('stage') == 'comparing_D_existing_inventory'
                           and progress.get('files_completed') == 0))
        phase = ('handoff_index_coverage_ready' if proof else 'handoff_index_coverage_parent_scan' if scanning_parent
                 else 'handoff_index_coverage_check' if state == 'running' else 'handoff_index_coverage_wait')
        row = task('handoff_index_coverage', dataset, state=state, phase=phase,
            observed=clock(value), activity=current_time if live else None,
            generation=(dataset, identity, value.get('owner_pid'), value.get('started_at_epoch')),
            done_items=number(value.get('matched_files')) if proof else number(progress.get('inventory_items_completed'))
                       if scanning_parent else number(progress.get('files_completed')),
            total_items=number(value.get('matched_files')) if proof else None if scanning_parent else number(progress.get('files_total')),
            done_bytes=number(value.get('matched_logical_bytes')) if proof else None if scanning_parent else number(progress.get('logical_bytes_processed')),
            total_bytes=number(value.get('matched_logical_bytes')) if proof else None if scanning_parent else number(progress.get('logical_bytes_total')),
            proof=proof, reason=None if proof or live else 'handoff_index_coverage_owner_busy' if owner_busy
                else 'source_or_copy_recheck_required' if state == 'blocked' else 'staging_queued')
        row['subject'] = dataset
        rows.append(row)
    return rows


def offline_replay_pair_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    """Project explicitly enrolled replay transactions without reading payloads."""
    if not workspace_checked:
        return []
    from stockagent.data_sync import offline_replay_policy as worker
    root = workspace/'migration/offline-replay-pairs-v1'
    rows = []
    for dataset in worker.ENROLLED:
        value = metadata(root/dataset/'status.json')
        if not value:
            row = task('offline_replay_pair', dataset, state='waiting', phase='offline_replay_wait',
                       reason='staging_queued')
        elif value.get('contract') != worker.CONTRACT or value.get('dataset') != dataset:
            row = task('offline_replay_pair', dataset, state='blocked', phase='offline_replay_wait',
                       reason='source_or_copy_recheck_required')
        else:
            progress = value.get('progress') if isinstance(value.get('progress'), dict) else {}
            stage = progress.get('stage')
            complete = (value.get('state') == 'completed' and value.get('D_archive_committed') is True
                        and value.get('C_payload_retired') is True
                        and isinstance(value.get('D_manifest_sha256'), str)
                        and re.fullmatch('[0-9a-f]{64}', value['D_manifest_sha256']) is not None
                        and isinstance(value.get('C_retirement'), dict)
                        and value['C_retirement'].get('state') == 'paired_payload_retired'
                        and isinstance(value.get('E_archive_cleanup'), dict)
                        and value['E_archive_cleanup'].get('state') == 'completed')
            phase = ('offline_replay_done' if complete else
                     'offline_replay_hash' if stage == 'hashing_source' else
                     'offline_replay_pack' if stage in {'streaming_source_into_E_packs', 'checking_source_namespace',
                         'indexing_source_inventory', 'rechecking_source_namespace', 'writing_E_inventory'} else
                     'offline_replay_publish' if stage in {'installing_D_objects', 'streaming_D_object',
                         'validating_D_inventory', 'committing_D_head'} else
                     'offline_replay_retire' if stage == 'retiring_C_pair' else 'offline_replay_wait')
            state = ('completed' if complete else 'blocked' if value.get('state') == 'waiting_retry'
                     else 'running' if value.get('state') in {'running', 'indexing', 'indexed',
                                                               'E_archive_sealed', 'D_archive_committed',
                                                               'C_payload_retired'} else 'waiting')
            if complete:
                counters = {'done_items': number(value['C_retirement'].get('files_removed')),
                            'total_items': number(value['C_retirement'].get('files_removed'))}
            elif stage == 'hashing_source':
                counters = {'done_items': number(progress.get('files_completed')),
                            'total_items': number(progress.get('files_total')),
                            'done_bytes': number(progress.get('logical_bytes_processed')),
                            'total_bytes': number(progress.get('logical_bytes_total'))}
            elif stage == 'streaming_source_into_E_packs':
                count = number(value.get('files'))
                counters = {'done_items': number(progress.get('files_completed')),
                            'total_items': count+1 if count is not None else None,
                            'done_bytes': number(progress.get('logical_bytes_processed')),
                            'total_bytes': number(value.get('logical_bytes'))}
            elif stage in {'installing_D_objects', 'streaming_D_object'}:
                counters = {'done_items': number(progress.get('objects_completed')),
                            'total_items': number(progress.get('objects_total')),
                            'done_bytes': number(progress.get('encoded_bytes_streamed', progress.get('encoded_bytes_installed'))),
                            'total_bytes': number(progress.get('encoded_bytes_total'))}
            elif stage == 'retiring_C_pair':
                counters = {'done_items': number(progress.get('files_completed')),
                            'total_items': number(progress.get('files_total'))}
            elif stage in {'checking_source_namespace', 'indexing_source_inventory',
                           'rechecking_source_namespace', 'writing_E_inventory', 'validating_D_inventory'}:
                counters = {'done_items': number(progress.get('inventory_items_completed')),
                            'total_items': number(progress.get('inventory_items_total'))}
            else:
                counters = {}
            error = str(value.get('error', '')).lower()
            blocked_reason = ('offline_replay_consumer_busy' if 'active ' in error and 'reference' in error
                              or 'active process consumer' in error else
                              'offline_replay_capacity' if 'capacity' in error or 'free space' in error
                              or 'reserve reached' in error else 'handoff_source_retry')
            row = task('offline_replay_pair', dataset, state=state, phase=phase, observed=clock(value),
                       generation=(value.get('source_identity'), stage), proof=complete, **counters,
                       reason=None if state in {'running', 'completed'} else
                              blocked_reason if state == 'blocked' else 'staging_queued',
                       metrics={'reclaimed_allocated_bytes': number(value.get('C_retirement', {}).get(
                           'estimated_allocated_bytes_reclaimed'))} if complete else {})
        row['subject'] = dataset
        rows.append(row)
    return rows


def offline_repair_archive_tasks(workspace: Path, *, workspace_checked=False, dataset=None) -> list[dict]:
    """Observe one enrolled offline source; never traverse its payload."""
    if not workspace_checked:
        return []
    from scripts import prepare_offline_repair_archive as worker, run_handoff_index_d_queue as d_queue
    from stockagent.data_sync import stream_repack
    from stockagent.data_sync.desync_snapshots import SnapshotError
    dataset = worker.DATASET if dataset is None else dataset
    if dataset not in worker.DATASET_SCOPES:
        return []
    subject = 'tw-daytrade-close-rollback' if dataset == worker.ROLLBACK_DATASET else 'tw-daytrade-offline-repair'
    root = workspace/'migration/handoff-index-archive-v1'
    value = metadata(root/(dataset+'.status.json'))
    if not value:
        row = task('offline_repair_archive', dataset, state='waiting',
                   phase='handoff_source_queued', reason='handoff_source_queued')
        row['subject'] = subject
        return [row]
    identity = value.get('source_identity')
    valid = (value.get('contract') == worker.CONTRACT and value.get('dataset') == dataset
             and isinstance(identity, str) and re.fullmatch(r'[0-9a-f]{64}', identity) is not None
             and type(value.get('C_payload_deleted')) is bool and type(value.get('D_archive_committed')) is bool)
    prepared = False
    if valid and value.get('state') in {'E_index_source_hashed_prepared', 'C_payload_retired'}:
        try:
            stage = root/dataset/identity
            result = (stream_repack._prepared_metadata(stage, dataset, identity)
                      if d_queue._done(value, dataset)
                      else stream_repack._read_prepared(stage, dataset, identity))
            prepared = (result.get('identity_sha256') == value.get('prepared_sha256')
                        and result.get('files') == value.get('files')
                        and result.get('logical_bytes') == value.get('logical_bytes'))
        except (OSError, ValueError, TypeError, KeyError, SnapshotError):
            prepared = False
    progress = value.get('progress') if isinstance(value.get('progress'), dict) else {}
    phase = {'checking_source_namespace': 'handoff_source_namespace',
             'rechecking_source_namespace': 'handoff_source_namespace',
             'indexing_source_inventory': 'handoff_source_indexing',
             'writing_E_inventory': 'handoff_source_inventory'}.get(progress.get('stage'), 'handoff_source_packing')
    metadata_phase = phase in {'handoff_source_namespace', 'handoff_source_indexing', 'handoff_source_inventory'}
    state = ('completed' if prepared else 'running' if valid and value.get('state') == 'running' else 'blocked')
    row = task('offline_repair_archive', dataset, state=state,
        phase='offline_repair_E_prepared' if prepared else phase, observed=clock(value),
        generation=(identity, progress.get('stage')),
        done_items=number(value.get('files')) if prepared else
                   number(progress.get('inventory_items_completed') if metadata_phase else progress.get('files_completed')),
        total_items=number(value.get('files')) if prepared else
                    number(progress.get('inventory_items_total') if metadata_phase else value.get('files_total')),
        done_bytes=number(value.get('logical_bytes')) if prepared else
                   None if metadata_phase else number(progress.get('logical_bytes_processed')),
        total_bytes=number(value.get('logical_bytes')) if prepared else
                    None if metadata_phase else number(value.get('logical_bytes_total')),
        proof=prepared, reason=None if state in {'completed', 'running'} else 'source_or_copy_recheck_required')
    row['subject'] = subject
    return [row]


def offline_repair_followup_tasks(workspace: Path, *, workspace_checked=False, dataset=None) -> list[dict]:
    """Project an exact offline source's C retirement and private D install."""
    if not workspace_checked:
        return []
    from scripts import prepare_offline_repair_archive as archive, retire_offline_repair_source as retire
    from scripts import run_handoff_index_d_queue as d_queue
    dataset = archive.DATASET if dataset is None else dataset
    if dataset not in archive.DATASET_SCOPES:
        return []
    subject = 'tw-daytrade-close-rollback' if dataset == archive.ROLLBACK_DATASET else 'tw-daytrade-offline-repair'
    root = workspace/'migration/handoff-index-archive-v1'
    value = metadata(root/(dataset+'.status.json'))
    progress = metadata(root/(dataset+'.retirement-status.json'))
    valid = retire.retired_status_matches(value, dataset)
    record = value.get('C_retirement') if valid else {}
    current = progress.get('progress') if isinstance(progress.get('progress'), dict) else {}
    live = (progress.get('contract') == retire.CONTRACT and progress.get('state') == 'running'
            and progress.get('source_identity') == value.get('source_identity'))
    handoff = task('offline_repair_handoff', dataset,
        state='completed' if valid else 'running' if live else 'waiting',
        phase='offline_repair_C_retired' if valid else
              'handoff_source_retiring' if current.get('stage') == 'retiring_C_payload' else
              'handoff_source_retirement_plan' if live else 'handoff_source_queued',
        observed=clock(value) if valid else clock(progress), generation=(value.get('source_identity'), current.get('stage')),
        done_items=number(record.get('files_removed')) if valid else number(current.get('files_completed')),
        total_items=number(record.get('files_removed')) if valid else number(current.get('files_total')),
        done_bytes=number(record.get('logical_bytes_removed')) if valid else number(current.get('logical_bytes_processed')),
        total_bytes=number(record.get('logical_bytes_removed')) if valid else number(current.get('logical_bytes_total')),
        proof=valid, reason=None if valid or live else 'handoff_source_queued',
        metrics={'reclaimed_allocated_bytes': number(record.get('estimated_allocated_bytes_reclaimed'))} if valid else {})
    handoff['subject'] = subject
    queued = metadata(root/'D-queue-status.json')
    entries = queued.get('entries') if queued.get('contract') == d_queue.CONTRACT else []
    selected = next((x for x in entries if isinstance(x, dict) and x.get('dataset') == dataset), {}) \
        if isinstance(entries, list) and len(entries) <= 64 else {}
    installed = d_queue._done(value, dataset)
    running = valid and selected.get('state') == 'running'
    dprogress = value.get('D_progress') if isinstance(value.get('D_progress'), dict) else {}
    phase = ('handoff_source_D_done' if installed else 'handoff_source_D_validating'
             if dprogress.get('stage') == 'validating_D_inventory' else 'handoff_source_D_installing'
             if running else 'handoff_source_D_waiting')
    publication = value.get('D_publication') if isinstance(value.get('D_publication'), dict) else {}
    counters = (_D_progress_counters(dprogress) if running else
        {'done_bytes': number(dprogress.get('encoded_bytes_total')),
         'total_bytes': number(dprogress.get('encoded_bytes_total')),
         'done_items': number(publication.get('files')),
         'total_items': number(publication.get('files'))} if installed else {})
    drow = task('offline_repair_D', dataset,
        state='completed' if installed else 'running' if running else 'blocked' if selected.get('state') == 'retrying' else 'waiting',
        phase=phase, observed=clock(value), generation=(value.get('source_identity'), dprogress.get('stage')),
        proof=installed, reason=None if installed or running else 'handoff_source_retry'
        if selected.get('state') == 'retrying' else 'handoff_source_queued', **counters)
    drow['subject'] = subject
    return [handoff, drow]


class _IntegrationGenerationObserver:
    """Check the current D head and catalog, without reading archive payloads.

    Queue timestamps are worker liveness, not the expiry of a committed fixed
    generation. A single collection shares its mount and catalog checks; a
    missing or changed authority never upgrades a historical receipt.
    """
    def __init__(self, *, cold=Path('/srv/stockagent-packed'), catalog=None,
                 mount_check=None, resolve=None):
        self.cold = Path(cold)
        self.catalog = Path(catalog) if catalog is not None else (
            Path(__file__).resolve().parents[2]/'configs/data_sync/packed_datasets.json')
        self.mount_check = mount_check
        self.resolve = resolve
        self._initialized = False
        self._available = False
        self._catalog_signature = None
        self._catalog_sha = None

    def _signature(self):
        info = self.catalog.lstat()
        if not stat.S_ISREG(info.st_mode) or self.catalog.is_symlink():
            raise ValueError('catalog is not a regular file')
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    def _initialize(self):
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        from stockagent.data_sync.packed_member_stream import read_stable_bytes
        try:
            (self.mount_check or _check_d_primary_mount)(self.cold)
            before = self._signature()
            raw = read_stable_bytes(self.catalog, maximum=MAX_METADATA_BYTES)
            if self._signature() != before:
                raise ValueError('catalog changed during observation')
            self._catalog_signature = before
            self._catalog_sha = hashlib.sha256(raw).hexdigest()
            self._available = True
        except (OSError, ValueError, TypeError, RuntimeError):
            self._available = False
        self._initialized = True

    def __call__(self, dataset: str, comparison: dict) -> str:
        from stockagent.data_sync.packed_snapshots import resolve_latest_packed
        expected_head = comparison.get('public_manifest_sha256')
        expected_catalog = comparison.get('catalog_sha256')
        if not all(isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value)
                   for value in (expected_head, expected_catalog)):
            return 'unavailable'
        if not self._initialized:
            self._initialize()
        if not self._available:
            return 'unavailable'
        try:
            if self._signature() != self._catalog_signature:
                self._available = False
                return 'unavailable'
            if expected_catalog != self._catalog_sha:
                return 'changed'
            current = (self.resolve or resolve_latest_packed)(self.cold, dataset, require_objects=False)
            if self._signature() != self._catalog_signature:
                self._available = False
                return 'unavailable'
            return 'current' if current.manifest_sha256 == expected_head else 'changed'
        except (OSError, ValueError, TypeError, RuntimeError):
            return 'unavailable'


def handoff_index_archive_tasks(workspace: Path, *, workspace_checked=False,
                                current_generation=None) -> list[dict]:
    if not workspace_checked:
        return []
    root = workspace/'migration/handoff-index-archive-v1'
    # The executor is the single source of truth for this bounded list.
    from scripts.prepare_handoff_index_archive import ENROLLED, PRIVATE_ONLY_DATASETS, DIRECT_PUBLIC_COVERAGE_DATASETS
    from scripts.prepare_offline_repair_archive import DATASET_SCOPES as OFFLINE_DATASET_SCOPES
    from scripts import run_handoff_index_archive_queue as archive_queue, run_handoff_index_prepack_queue as prepack_queue
    from stockagent.data_sync import index_archive_existing, semantic_link_promotion
    names = tuple(dataset for dataset, _locks, _supplemental in ENROLLED.values())
    queue = metadata(root/'queue-status.json')
    prepack = metadata(root/'prepack-queue-status.json')
    prepack_time = clock(prepack)
    prepack_source = prepack.get('source')
    prepack_dataset = prepack.get('dataset')
    prepack_live = (prepack.get('contract') == prepack_queue.CONTRACT and prepack.get('state') == 'running'
        and prepack_time is not None and -60 <= time.time()-prepack_time <= 180
        and type(prepack.get('owner_pid')) is int and prepack['owner_pid'] > 0
        and isinstance(prepack_source, str) and isinstance(prepack_dataset, str)
        and ENROLLED.get(prepack_source, (None,))[0] == prepack_dataset
        and archive_queue.lock_owner_pid(root/'prepack-owner.lock') == prepack['owner_pid'])
    d_queue = metadata(root/'D-queue-status.json')
    integration_queue = metadata(root/'integration-queue-status.json')
    entries = queue.get('entries') if queue.get('contract') == 'stockagent.handoff_index_archive_queue.v1' else []
    queued = {item['dataset']: item for item in entries if isinstance(item, dict) and item.get('dataset') in names} \
        if isinstance(entries, list) and len(entries) <= len(names) else {}
    queue_pid = queue.get('owner_pid')
    queue_owner_matches = (queue_pid is None or type(queue_pid) is int and queue_pid > 0
                           and archive_queue.lock_owner_pid(root/'queue-owner.lock') == queue_pid)
    entries = d_queue.get('entries') if d_queue.get('contract') == 'stockagent.handoff_index_private_D_queue.v1' else []
    d_queued = {item['dataset']: item for item in entries if isinstance(item, dict) and item.get('dataset') in names} \
        if isinstance(entries, list) and len(entries) <= len(names) + len(OFFLINE_DATASET_SCOPES) else {}
    entries = (integration_queue.get('entries') if integration_queue.get('contract')
               == 'stockagent.handoff_index_integration_queue.v1' else [])
    integration_queued = {item['dataset']: item for item in entries if isinstance(item, dict) and item.get('dataset') in names} \
        if isinstance(entries, list) and len(entries) <= len(names) else {}
    current_generation = current_generation or _IntegrationGenerationObserver()
    rows = []
    for name in names:
        value, pending = metadata(root/(name+'.status.json')), queued.get(name, {})
        valid = value.get('contract') == 'stockagent.handoff_index_archive.v1' and value.get('dataset') == name
        if not valid and not pending:
            continue
        result = value.get('C_retirement') if isinstance(value.get('C_retirement'), dict) else {}
        done = (valid and value.get('state') == 'C_payload_retired_root_and_locks_preserved'
                and value.get('C_payload_deleted') is True
                and result.get('state') == 'C_payload_retired_root_and_lock_preserved'
                and result.get('prepared_sha256') == value.get('prepared_sha256'))
        progress = value.get('progress') if isinstance(value.get('progress'), dict) else {}
        main_live = (pending.get('state') == 'running' and clock(queue) is not None
                     and -60 <= time.time()-clock(queue) <= 180 and queue_owner_matches)
        sidecar_live = prepack_live and prepack_dataset == name
        live = main_live or sidecar_live
        if done:
            state = 'completed'
            phase = 'handoff_source_retired'
            reason = None
        elif live:
            state = 'running'
            phase = {'retiring_C_payload': 'handoff_source_retiring',
                     'checking_source_namespace': 'handoff_source_namespace',
                     'rechecking_source_namespace': 'handoff_source_namespace',
                     'retirement_source_namespace': 'handoff_source_namespace',
                     'retirement_planning': 'handoff_source_retirement_plan',
                     'retirement_plan_recheck': 'handoff_source_retirement_plan',
                     'comparing_existing_D_inventory': 'handoff_index_comparing',
                     'indexing_source_inventory': 'handoff_source_indexing',
                     'writing_E_inventory': 'handoff_source_inventory'}.get(progress.get('stage'), 'handoff_source_packing')
            reason = None
        elif pending.get('state') == 'retrying' and pending.get('error') == 'owner_busy':
            state, phase, reason = 'waiting', 'handoff_source_queued', 'handoff_source_owner_busy'
        elif pending.get('state') == 'retrying' and pending.get('error') == 'external_hardlinks':
            state, phase, reason = 'blocked', 'handoff_source_queued', 'handoff_source_external_hardlinks'
        elif pending.get('state') == 'retrying':
            state, phase, reason = 'blocked', 'handoff_source_queued', 'handoff_source_retry'
        elif valid and prepack_queue.prepared(root, name, value):
            state, phase, reason = 'waiting', 'handoff_source_E_prepared', 'original_owner'
        elif (prepack_dataset == name and prepack.get('contract') == prepack_queue.CONTRACT
              and prepack.get('state') == 'retrying'):
            state, phase, reason = 'blocked', 'handoff_source_queued', 'handoff_source_retry'
        else:
            state, phase, reason = 'waiting', 'handoff_source_queued', 'handoff_source_queued'
        counters = ({'done_items': number(result.get('files_removed')), 'total_items': number(result.get('files_removed')),
                     'done_bytes': number(result.get('logical_bytes_removed')), 'total_bytes': number(result.get('logical_bytes_removed')),
                     'metrics': {'reclaimed_allocated_bytes': number(result.get('estimated_allocated_bytes_reclaimed'))}}
                    if done else {'done_items': number(progress.get('inventory_items_completed'))
                         if phase in {'handoff_source_namespace', 'handoff_source_indexing', 'handoff_source_inventory', 'handoff_source_retirement_plan'}
                         else number(progress.get('files_completed')),
                         'total_items': number(progress.get('inventory_items_total'))
                         if phase in {'handoff_source_namespace', 'handoff_source_indexing', 'handoff_source_inventory', 'handoff_source_retirement_plan'}
                         else number(progress.get('files_total', value.get('files_total'))),
                         'done_bytes': None if phase in {'handoff_source_namespace', 'handoff_source_indexing', 'handoff_source_inventory', 'handoff_source_retirement_plan'}
                         else number(progress.get('logical_bytes_processed')),
                         'total_bytes': None if phase in {'handoff_source_namespace', 'handoff_source_indexing', 'handoff_source_inventory', 'handoff_source_retirement_plan'}
                         else number(progress.get('logical_bytes_total', value.get('logical_bytes_total')))})
        owner_generation = ((prepack.get('owner_pid'), prepack.get('started_at_epoch')) if sidecar_live and not main_live
                            else (queue_pid, pending.get('started_at_epoch')) if main_live else None)
        row = task('handoff_index_archive', name, state=state, phase=phase,
                   observed=clock(value) if valid else clock(prepack) if sidecar_live else clock(queue),
                   activity=clock(prepack) if sidecar_live else clock(queue) if main_live else None,
                   generation=(name, value.get('source_identity'), progress.get('stage'), owner_generation),
                   proof=done, reason=reason, **counters)
        row['subject'] = name
        retry_at = timestamp(pending.get('retry_after_epoch'))
        if reason in {'handoff_source_owner_busy', 'handoff_source_external_hardlinks'} and retry_at is not None:
            row['next_attempt_at_utc'] = iso(retry_at)
        implementation = queue.get('implementation_sha256')
        wall_limit = LEGACY_ARCHIVE_OWNER_WALL_LIMITS.get(implementation) if isinstance(implementation, str) else None
        started = timestamp(pending.get('started_at_epoch'))
        if state == 'running' and main_live and wall_limit is not None and started is not None and started <= time.time()+120:
            row['_legacy_archive_deadline'] = started+wall_limit
            row['reason'] = 'handoff_archive_legacy_timeout'
        if sidecar_live and not main_live:
            row['_handoff_prepack_owner'] = True
        rows.append(row)
        if done:
            publication = value.get('D_publication')
            committed = (value.get('D_archive_committed') is True and isinstance(publication, dict)
                and publication.get('dataset') == name and publication.get('old_manifest_sha256') == value.get('source_identity')
                and (publication.get('state') == 'new_D_head_committed_from_exact_E_source'
                     or index_archive_existing.receipt_matches(publication, value.get('prepared_sha256')))
                and publication.get('source_mode') == 'retained_handoff_C_index')
            cleaned = committed and value.get('D_E_stage_cleanup_complete') is True
            if cleaned:
                row['phase'] = 'handoff_index_C_retired_D'
            dpending = d_queued.get(name, {})
            dprogress = value.get('D_progress') if isinstance(value.get('D_progress'), dict) else {}
            dlive = (dpending.get('state') == 'running' and clock(d_queue) is not None
                     and time.time()-clock(d_queue) <= 180)
            if cleaned:
                dstate, dphase, dreason = ('completed',
                    'handoff_source_D_direct_reused' if name in DIRECT_PUBLIC_COVERAGE_DATASETS
                    and publication.get('state') == 'existing_D_head_matches_source_inventory' else
                    'handoff_source_D_reused' if publication.get('state') == 'existing_D_head_matches_source_inventory'
                    else 'handoff_source_D_done', None)
            elif dlive:
                dstate, dphase, dreason = ('running', 'handoff_source_D_cleanup' if committed else
                    'handoff_index_D_parent_scan' if dprogress.get('stage') == 'scanning_D_parent_inventory'
                    or dprogress.get('stage') == 'comparing_D_existing_inventory'
                    and dprogress.get('files_completed') == 0 and name in index_archive_existing.SUBTREE_COVERAGES else
                    'handoff_index_comparing' if dprogress.get('stage') == 'comparing_D_existing_inventory' else
                    'handoff_source_D_validating' if dprogress.get('stage') == 'validating_D_inventory' else
                    'handoff_source_D_installing', None)
            elif dpending.get('state') == 'waiting':
                dstate, dphase, dreason = 'waiting', 'handoff_source_D_waiting', 'handoff_source_queued'
            elif dpending.get('state') == 'waiting_priority':
                dstate, dphase, dreason = 'waiting', 'handoff_source_D_waiting', 'waiting_E_only_priority'
            elif dpending.get('state') == 'retrying' and dpending.get('error') == 'owner_busy':
                dstate, dphase, dreason = 'waiting', 'handoff_source_D_waiting', 'handoff_index_D_owner_busy'
            else:
                dstate, dphase, dreason = ('blocked', 'handoff_source_D_waiting',
                    'handoff_index_D_capacity' if dpending.get('error') == 'capacity' else
                    'handoff_index_D_owner_busy' if dpending.get('error') == 'owner_busy' else 'handoff_source_retry')
            # A failed previous D attempt leaves its final counters in the
            # source receipt. They are not work performed by a waiting or
            # retrying owner and cannot be used as current progress/ETA.
            dcounts = ({} if not dlive else
                       {'done_items':number(dprogress.get('inventory_items_completed'))}
                       if dphase == 'handoff_index_D_parent_scan' else _D_progress_counters(dprogress))
            storage = task('handoff_index_D_storage', name, state=dstate, phase=dphase,
                observed=(clock(d_queue) if not cleaned and not dlive and clock(d_queue) is not None else clock(value)),
                activity=clock(d_queue) if dlive else None, proof=cleaned, reason=dreason,
                generation=(name, value.get('source_identity'), dprogress.get('stage')),
                **dcounts)
            storage['subject'] = name
            storage['next_attempt_at_utc'] = (iso(timestamp(dpending.get('retry_after_epoch')))
                                              if not cleaned and not dlive else None)
            if not committed and not dlive and dpending.get('error') == 'capacity':
                identity = value.get('source_identity')
                if isinstance(identity, str) and re.fullmatch(r'[a-f0-9]{64}', identity):
                    try:
                        remaining = _index_archive_remaining(root/name/identity, name, identity,
                                                              value.get('prepared_sha256'))
                        if remaining is not None:
                            storage['_D_encoded_remaining_bytes'] = remaining
                    except (OSError, ValueError, KeyError, TypeError, RuntimeError):
                        pass  # Metadata is not proof of a runnable install.
            rows.append(storage)
            if (value.get('public_dataset_merged') is not True
                    and name not in PRIVATE_ONLY_DATASETS and name not in DIRECT_PUBLIC_COVERAGE_DATASETS):
                comparison = metadata(root/(name+'.diff-status.json'))
                iqueued = integration_queued.get(name, {})
                queue_clock = clock(integration_queue)
                fresh_queue = queue_clock is not None and -60 <= time.time()-queue_clock <= 180
                comparison_bound = (committed and iqueued.get('state') in {'completed', 'waiting_generation_changed', 'blocked_semantic_merge'}
                    and comparison.get('contract') == 'stockagent.handoff_index_integration_plan.v1'
                    and comparison.get('dataset') == name and comparison.get('state') == 'metadata_compared'
                    and comparison.get('private_manifest_sha256') == publication.get('new_manifest_sha256')
                    and comparison.get('private_snapshot_id') == publication.get('new_snapshot_id')
                    and comparison.get('semantic_merge_complete') is False)
                generation_state = current_generation(name, comparison) if comparison_bound else None
                bound = comparison_bound and generation_state == 'current'
                running = committed and fresh_queue and iqueued.get('state') == 'running'
                integration_waiting = (committed
                    and iqueued.get('state') in {'waiting', 'waiting_semantic', 'retrying'})
                counts = comparison.get('counters') if bound and isinstance(comparison.get('counters'), dict) else {}
                current_covers_fixed = (type(counts.get('equal')) is int and counts['equal'] >= 0
                    and type(counts.get('public_only')) is int and counts['public_only'] >= 0
                    and all(counts.get(key) == 0 for key in
                            ('conflict', 'private_only', 'private_excluded', 'unexpected_public_excluded')))
                exact_match = current_covers_fixed and counts.get('public_only') == 0
                metrics = {out: number(counts.get(key)) for out, key in (
                    ('source_equal_items', 'equal'), ('source_public_only_items', 'public_only'),
                    ('source_private_only_items', 'private_only'), ('source_excluded_items', 'private_excluded'),
                    ('source_conflict_items', 'conflict'))}
                integrated = False
                latest_selected = False
                integration = metadata(root/name/'frankfurter-semantic-integration-v1.json') if name == 'forex-frankfurter' else {}
                if (bound and integration.get('contract') == 'stockagent.frankfurter_fixed_generation_integration.v1'
                        and integration.get('state') == 'public_generation_integrated'
                        and integration.get('private_original_manifest_sha256') == publication.get('new_manifest_sha256')
                        and integration.get('public_only_rows') == 0 and integration.get('conflicting_overlap_rows') == 0):
                    published = integration.get('new_public_manifest_sha256')
                    head = comparison.get('public_manifest_sha256')
                    if isinstance(published, str) and re.fullmatch('[a-f0-9]{64}', published):
                        integrated = head == published
                        if not integrated and isinstance(head, str) and re.fullmatch('[a-f0-9]{64}', head):
                            repack = metadata(workspace/'migration/source-local-exact-repack'/name/(head+'.json'))
                            integrated = (repack.get('contract') == 'stockagent.source_local_exact_repack.v1'
                                and repack.get('dataset') == name and repack.get('old_manifest_sha256') == published
                                and repack.get('new_manifest_sha256') == head
                                and repack.get('state') == 'new_D_head_committed_from_exact_E_source')
                if bound and name == 'binance-public-archive':
                    base = root/name
                    receipts = list(base.glob('semantic-promotion-*.json')) if base.is_dir() and not base.is_symlink() else []
                    if len(receipts) <= 64:
                        for path in receipts:
                            promoted = metadata(path)
                            if (promoted.get('contract') == 'stockagent.semantic_link_promotion.v1'
                                    and promoted.get('state') == 'committed'
                                    and (promoted.get('new_manifest_sha256') == comparison.get('public_manifest_sha256')
                                         or current_covers_fixed)
                                    and promoted.get('private_manifest_sha256') == publication.get('new_manifest_sha256')):
                                integrated = True
                                integration = (promoted if promoted.get('new_manifest_sha256') == comparison.get('public_manifest_sha256')
                                               else comparison)
                                break
                if bound and name in semantic_link_promotion.FAST_LATEST_DATASETS:
                    base = root/name
                    receipts = list(base.glob('latest-promotion-*.json')) if base.is_dir() and not base.is_symlink() else []
                    if len(receipts) <= 64:
                        for path in receipts:
                            promoted = metadata(path)
                            if (promoted.get('contract') == 'stockagent.latest_fixed_link_promotion.v1'
                                    and promoted.get('state') == 'committed'
                                    and (promoted.get('new_manifest_sha256') == comparison.get('public_manifest_sha256')
                                         or current_covers_fixed)
                                    and promoted.get('private_manifest_sha256') == publication.get('new_manifest_sha256')
                                    and promoted.get('changed_member_values_compared') is False
                                    and promoted.get('old_historical_value_coverage') == 'not_compared'
                                    and (name not in semantic_link_promotion.EXACT_EQUAL_DATASETS
                                         or promoted.get('uncompared_changed_members') == 0)):
                                latest_selected = True
                                integration = (promoted if promoted.get('new_manifest_sha256') == comparison.get('public_manifest_sha256')
                                               else comparison)
                                metrics['uncompared_changed_members'] = number(promoted.get('uncompared_changed_members'))
                                metrics['duplicate_encoded_bytes_pending'] = number(promoted.get('duplicate_encoded_bytes_pending'))
                                break
                selected = integrated or latest_selected
                pending_d = task('handoff_index_D', name, state='completed' if selected else
                    'running' if running else 'waiting' if integration_waiting or generation_state == 'unavailable' else 'blocked',
                    phase=('handoff_index_public_exact_equal' if latest_selected and name in semantic_link_promotion.EXACT_EQUAL_DATASETS and exact_match else
                           'handoff_index_public_fixed_covered' if latest_selected and name in semantic_link_promotion.EXACT_EQUAL_DATASETS else
                           'handoff_index_public_latest_selected' if latest_selected else
                           'handoff_index_public_integrated' if integrated else
                           'handoff_index_comparing' if running else
                           'handoff_index_integration_waiting' if integration_waiting or generation_state in {'changed', 'unavailable'} else
                           'handoff_index_equal_pending' if bound and exact_match else 'handoff_index_compared' if bound else
                           'handoff_index_D_archived' if committed else 'handoff_index_D_waiting'),
                    observed=clock(integration) if selected else clock(comparison) if bound or running else clock(value),
                    activity=queue_clock if running else None, proof=selected, metrics=metrics,
                    reason=None if selected or running else
                           'handoff_index_binance_union_required' if bound and iqueued.get('state') == 'blocked_semantic_merge' else
                           'handoff_index_binding_unavailable' if generation_state == 'unavailable' else
                           'handoff_index_generation_changed' if generation_state == 'changed' else
                           'handoff_index_integration_owner_busy' if integration_waiting and fresh_queue and iqueued.get('error') == 'owner_busy' else
                           'handoff_source_queued' if integration_waiting else 'handoff_index_generation_changed'
                           if bound and iqueued.get('state') == 'waiting_generation_changed' else
                           'handoff_index_equal_pending' if bound and exact_match else 'handoff_index_compared' if bound else
                           'handoff_index_D_archived' if committed else 'handoff_index_D_waiting')
                pending_d['subject'] = name
                # Show a bounded, phase-local counter only when it belongs to
                # the currently running queue and both fixed generations. A
                # previous attempt's progress cannot prove current activity.
                if running and name == 'binance-public-archive' and iqueued.get('work') in {'archive_rows', 'archive_controls'}:
                    control = iqueued['work'] == 'archive_controls'
                    audit = metadata(root/name/('control-audit-status.json' if control else 'row-audit-status.json'))
                    audit_time = clock(audit)
                    stage = audit.get('stage')
                    expected_contract = ('stockagent.binance_archive_control_audit.v1' if control
                                         else 'stockagent.binance_archive_generation_audit.v1')
                    identity = comparison.get('comparison_identity_sha256')
                    audit_bound = (audit.get('contract') == expected_contract and audit.get('state') == 'running'
                        and audit_time is not None and -60 <= time.time()-audit_time <= 180
                        and isinstance(identity, str) and re.fullmatch('[a-f0-9]{64}', identity) is not None
                        and audit.get('comparison_identity_sha256') == identity
                        and audit.get('public_manifest_sha256') == comparison.get('public_manifest_sha256')
                        and audit.get('private_manifest_sha256') == publication.get('new_manifest_sha256')
                        and comparison.get('dataset') == name
                        and comparison.get('contract') == 'stockagent.handoff_index_integration_plan.v1'
                        and stage in {'reading_fixed_private', 'reading_fixed_public', 'comparing_fixed_members'}
                        and current_generation(name, comparison) == 'current')
                    if audit_bound:
                        done, total = number(audit.get('stage_files_completed')), number(audit.get('stage_files_total'))
                        if done is not None and total is not None and 0 <= done <= total and total > 0:
                            pending_d.update(phase='handoff_index_'+stage, completed_items=done, total_items=total,
                                             observed_at_utc=iso(audit_time), activity_at_utc=iso(audit_time))
                            if stage.startswith('reading_'):
                                done_bytes, total_bytes = number(audit.get('stage_bytes_completed')), number(audit.get('stage_bytes_total'))
                                if done_bytes is not None and total_bytes is not None and 0 <= done_bytes <= total_bytes:
                                    pending_d.update(completed_bytes=done_bytes, total_bytes=total_bytes)
                if integration_waiting:
                    pending_d['next_attempt_at_utc'] = iso(timestamp(iqueued.get('retry_after_epoch')))
                rows.append(pending_d)
    return rows


def fred_pin_recovery_tasks(workspace: Path, *, workspace_checked=False, now=None) -> list[dict]:
    """Expose the exact FRED repair without treating it as a NAS restore."""
    if not workspace_checked:
        return []
    now = time.time() if now is None else now
    root = workspace/'migration/fred-exact-pin-recovery-v1'
    value, error = metadata(root/'status.json'), metadata(root/'last-error.json')
    contract = 'stockagent.fred_exact_pin_recovery.v1'
    valid = (value.get('contract') == contract and value.get('dataset') == 'fred-crypto-macro'
             and value.get('current_head_changed') is False and value.get('source_deleted') is False
             and isinstance(value.get('original_manifest_sha256'), str)
             and re.fullmatch('[a-f0-9]{64}', value['original_manifest_sha256']) is not None)
    stamp, error_stamp = clock(value), clock(error)
    failed = (error.get('contract') == contract and error.get('state') == 'waiting_retry'
              and error_stamp is not None and (stamp is None or error_stamp > stamp))
    if not valid and not failed:
        return []
    complete = (valid and value.get('state') in {'exact_pin_restored', 'already_available'}
                and value.get('exact_pin_resolved') is True and not failed)
    stage = value.get('state')
    running = (valid and not failed and stamp is not None and -60 <= now-stamp <= 180
               and stage in {'building_exact_candidate', 'installing_exact_objects'})
    expected, installed = number(value.get('expected_objects')), number(value.get('installed_objects'))
    if expected is None or expected <= 0 or installed is None or installed > expected:
        expected, installed = None, None
    row = task('fred_pin_recovery', 'fixed-original', state='completed' if complete else 'running' if running else 'waiting',
        phase='fred_pin_restored' if complete else 'fred_pin_building' if running and stage == 'building_exact_candidate'
              else 'fred_pin_installing' if running else 'fred_pin_waiting',
        observed=error_stamp if failed else stamp, done_items=installed, total_items=expected,
        generation=value.get('original_manifest_sha256', ''), proof=complete,
        reason=None if complete or running else 'fred_pin_retry')
    return [row]


def d_testing_cleanup_tasks(repo: Path, state_root=Path('/var/lib/stockagent/d-workflow-benchmark-cleanup-cohorts-20261106')) -> list[dict]:
    """Observe the single fixed D working-copy disposition and its own owner."""
    from stockagent.data_sync.testing_disposition import D_WORKFLOW_TESTING_CONTRACT, identity
    policy = metadata(repo / 'configs/data_sync/d_workflow_trial_directories_disposition_20261106.json')
    if not policy:
        return []
    value = metadata(state_root / 'status.json')
    targets = policy.get('targets')
    bound_policy = (policy.get('contract') == D_WORKFLOW_TESTING_CONTRACT
        and policy.get('identity_sha256') == identity({k:v for k,v in policy.items() if k != 'identity_sha256'})
        and isinstance(targets, list) and bool(targets))
    if not bound_policy:
        return [task('d_testing_cleanup', 'fixed', reason='metadata_unavailable')]
    if not value:
        return [task('d_testing_cleanup', policy['identity_sha256'], state='waiting',
                     reason='staging_queued', total_items=len(targets))]
    bound = (value.get('contract') == 'stockagent.local_testing_cleanup.v1'
        and value.get('policy_identity_sha256') == policy['identity_sha256']
        and value.get('apply') is True and value.get('preservation_waived_by_user') is True
        and value.get('recovery_claimed') is False
        and value.get('cold_objects_deleted') is False and value.get('nas_objects_deleted') is False
        and number(value.get('targets_total')) == len(targets))
    if not bound:
        return [task('d_testing_cleanup', 'fixed', reason='metadata_unavailable')]
    completed = value.get('state') == 'completed' and number(value.get('targets_completed')) == len(targets)
    blocked = value.get('state') == 'waiting_protected_targets'
    phase = 'disposable_cleanup_completed' if completed else value.get('state')
    return [task('d_testing_cleanup', policy['identity_sha256'],
        state='completed' if completed else 'blocked' if blocked else 'running',
        phase=phase if phase in PHASES else 'working', observed=clock(value),
        generation=(policy['identity_sha256'], 'd_workflow_copies_v1'),
        done_items=number(value.get('targets_completed')), total_items=len(targets), proof=completed,
        reason='dependency' if blocked else None,
        metrics={'discarded_allocated_bytes': number(value.get('reclaimed_allocated_bytes')),
                 'blocked_items': number(value.get('targets_blocked'))})]


def receiver_owner_diagnostic(root: Path, *, now: float) -> dict:
    """Project fresh owner readiness and a separately dated diagnostic.

    A fresh observer is not a fresh lock observation. Bind the one fixed
    diagnostic to the original paired readiness and show its own clock; a new
    readiness generation automatically supersedes it.
    """
    from scripts.backup_delivery_receipt import READY, READY_FIELDS, valid_signature

    ready = metadata(root / 'readiness.json')
    schedule = metadata(root / 'lab203-owner-schedule-v1.json')
    try:
        valid_signature(ready, READY, fields=READY_FIELDS)
        valid_signature(schedule, 'stockagent.lab203_owner_schedule_observation.v1')
        pairing = {k: ready[k] for k in ('producer_device_id', 'receiver_device_id', 'repository_id')}
        stamp = clock(schedule)
        reference = schedule.get('original_readiness_reference')
        if (schedule.get('observer_node_id') != 'lab203' or schedule.get('pairing') != pairing
                or stamp is None or not now - STALE_SECONDS <= stamp <= now + 120):
            return {}
    except (KeyError, ValueError, TypeError):
        return {}

    result = {'observed': stamp}
    owners = schedule.get('owners')
    owner = next((entry for entry in owners if isinstance(entry, dict)
                  and entry.get('role') == 'backup'), {}) if isinstance(owners, list) and len(owners) <= 32 else {}
    if owner.get('installed') is True and owner.get('enabled') is True and owner.get('timer_active') is True:
        result['schedule'] = {'observed_at_utc': iso(stamp), 'automatic': True,
            'owner_running': (owner.get('currently_executing') is True
                              and owner.get('systemd_active_state') == 'active'
                              and owner.get('systemd_sub_state') == 'running'),
            'next_check_at_utc': iso(timestamp(owner.get('next_check_at_utc')))}
    if (not isinstance(reference, dict)
            or reference.get('source_state') != 'waiting_original_owner_recheck'
            or reference.get('original_receipt_identity_sha256') != ready['identity_sha256']):
        return result
    result.update(waiting=True, reason='receiver_readiness')
    identity = RECEIVER_LOCK_DIAGNOSTIC
    diagnostic = metadata(root / f'lab203-restic-exclusive-lock-diagnostic-v1-{identity}.json')
    try:
        valid_signature(diagnostic, 'lab203.original_restic_lock_diagnostic_handoff.v1')
        detail = diagnostic.get('diagnosis')
        observed = clock(diagnostic)
        if (diagnostic.get('identity_sha256') == identity and diagnostic.get('pairing') == pairing
                and diagnostic.get('is_batch_acceptance_ACK') is False
                and isinstance(detail, dict) and detail.get('readiness_identity_sha256') == ready['identity_sha256']
                and detail.get('restic_reported_exclusive_lock') is True
                and observed is not None and observed <= stamp + 120):
            result.update(reason='receiver_readiness_lock_diagnosis', diagnosis_at_utc=iso(observed))
    except (ValueError, TypeError):
        pass
    return result


def receiver_task(root: Path, *, now: float | None = None) -> dict:
    pipeline = metadata(root / "pipeline-status.json")
    audit = metadata(root / "storage-relay-audit.json")
    if pipeline.get("contract") != "parallel_nas_backup_relay_status_v1":
        pipeline = {}
    if audit.get("contract") != "stockagent.storage_relay_audit.v1":
        audit = {}
    admission = audit.get("admission", {})
    admission = admission if isinstance(admission, dict) else {}
    jobs = pipeline.get("jobs")
    # v1 publishes a completed cycle's outcome list, not live worker states.
    # In particular accepted and queued entries must never count as running.
    valid_jobs = isinstance(jobs, list) and all(isinstance(j, dict) and choice(j.get("state"),
        {"accepted", "queued", "retry_wait", "rejected"}) for j in jobs)
    counts = {state: sum(j["state"] == state for j in jobs) if valid_jobs else None
              for state in ("accepted", "queued", "retry_wait", "rejected")}
    owner = receiver_owner_diagnostic(root, now=time.time() if now is None else now)
    blocked = admission.get("receiving_backpressure") is True or owner.get('waiting') is True
    result = task("receiver", "receiver", node="lab203", state="blocked" if blocked else "waiting" if pipeline else "unknown",
        phase="relay_readback", observed=owner.get('observed') if owner.get('waiting') else clock(audit), activity=clock(pipeline),
        reason=owner.get('reason') or ("dependency" if blocked else "no_counter"),
        metrics={"queued_jobs": counts["queued"], "last_cycle_accepted_jobs": counts["accepted"],
                 "retry_jobs": counts["retry_wait"], "rejected_jobs": counts["rejected"],
                 "pending_bytes": admission.get("pending_transport_bytes"),
                 "admission_available_bytes": admission.get("available_admission_bytes"),
                 "lab_reclaimed_allocated_bytes": audit.get("actual_reclaimed_allocated_bytes")} )
    result['diagnosis_at_utc'] = owner.get('diagnosis_at_utc')
    result['receiver_schedule'] = owner.get('schedule')
    return result


def lake_relay_tasks(root: Path, pairing: dict, *, now: float) -> list[dict]:
    """Observe a paired relay failure without interpreting it as an ACK.

    The remote owner's rolling status is not a signed final receipt. Expose
    only a recognised, currently deferred memory failure and its own clock;
    disappearance of this observation never establishes successful recovery.
    """
    value = metadata(root / 'lakehouse/relay-status.json', limit=128 * 1024)
    observed = clock(value)
    if (not value or observed is None or observed > now + 120
            or any(value.get(key) != pairing.get(key) or not isinstance(pairing.get(key), str)
                   or not pairing[key] for key in ('producer_device_id', 'receiver_device_id'))
            or any(value.get(key) is not True for key in
                   ('single_owner_verified', 'runtime_lock_verified', 'nas_mount_guard_verified'))):
        return []
    jobs = value.get('jobs')
    if not isinstance(jobs, list) or len(jobs) > 256:
        return []
    rows = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        delivery = job.get('delivery')
        failure = timestamp(job.get('last_failure_at_utc'))
        retry = timestamp(job.get('next_retry_at_utc'))
        attempts = number(job.get('attempts'))
        if (not isinstance(delivery, str) or re.fullmatch(r'lake-[0-9a-f]{64}', delivery) is None
                or job.get('state') not in {'deferred', 'retry_wait', 'rejected'}
                or job.get('failed_phase') != 'native_DuckLake_full_table_SHA'
                or job.get('original_error_type') != 'OutOfMemoryException'
                or failure is None or failure > observed + 120 or attempts is None):
            continue
        row = task('lake_relay', delivery, node='lab203', state='blocked',
            phase='lake_relay_semantic_hash', observed=observed,
            generation=(delivery, failure, attempts), reason='lake_relay_memory_exhausted',
            metrics={'attempt_number': attempts})
        row['diagnosis_at_utc'] = iso(failure)
        row['next_attempt_at_utc'] = iso(retry) if retry is not None else None
        rows.append(row)
    return rows


def _storage_priority_window_end(now: float, *, minimum_runway_minutes=120) -> float | None:
    """Evaluate the same official-calendar gate used by storage services."""
    from datetime import time as wall_time
    from scripts.check_outside_tw_opening_resource_window import evaluate
    local = datetime.fromtimestamp(now, ZoneInfo('Asia/Taipei'))
    decision = evaluate(local, minimum_runway_minutes=minimum_runway_minutes, protected_until=wall_time(13, 30),
                        official_calendar_root=Path(__file__).resolve().parents[2]/'data_tw_public')
    if decision.get('protected') is not True:
        return None
    end = local.replace(hour=13, minute=30, second=0, microsecond=0).timestamp()
    return end if end > now else None


def _storage_priority_condition_runway(fields: dict) -> int | None:
    """Do not guess the reason for any different or failed condition."""
    conditions = fields.get('_exec_conditions', [fields.get('ExecCondition', '')])
    if fields.get('Result') != 'exec-condition' or not isinstance(conditions, list):
        return None
    for runway in (120, 20):
        exact = (f'scripts/check_outside_tw_opening_resource_window.py --minimum-runway-minutes {runway} '
                 '--protected-until 13:30 --official-calendar-root data_tw_public')
        if any(isinstance(condition, str) and condition.count('argv[]=') == 1
               and exact in condition and '; code=exited ; status=1 }' in condition for condition in conditions):
            return runway
    return None


def _is_storage_priority_condition(fields: dict) -> bool:
    return _storage_priority_condition_runway(fields) is not None


def schedules(*, now=None) -> list[dict]:
    """Two bounded local queries per observer cycle; never called by HTTP."""
    try:
        result = subprocess.run(["systemctl", "list-timers", "--all", "--output=json", "--no-pager"],
                                capture_output=True, text=True, timeout=3, check=True)
        values = json.loads(result.stdout)
        if not isinstance(values, list):
            raise ValueError("timer_list_not_array")
    except (OSError, ValueError, subprocess.SubprocessError):
        values = []
    indexed = {row.get("unit"): row for row in values if isinstance(row, dict)}
    states = {}
    try:
        units = [unit for timer in TIMERS for unit in (timer, timer.removesuffix(".timer") + ".service")]
        units.extend(CONTINUOUS)
        result = subprocess.run(["systemctl", "show", *units, "-p", "Id", "-p", "ActiveState", "-p", "SubState",
                                 "-p", "LoadState", "-p", "Result", "-p", "ExecMainStatus", "-p", "MainPID",
                                 "-p", "ExecCondition", "--no-pager"],
                                capture_output=True, text=True, timeout=3, check=True)
        for block in result.stdout.strip().split("\n\n"):
            fields = {}
            for line in block.splitlines():
                if '=' not in line:
                    continue
                key, val = line.split('=', 1)
                if key == 'ExecCondition':
                    fields.setdefault('_exec_conditions', []).append(val)
                else:
                    fields[key] = val
            if fields.get("Id") in units:
                states[fields["Id"]] = fields
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    priority_end = None
    if any(_is_storage_priority_condition(fields) for fields in states.values()):
        try:
            priority_end = _storage_priority_window_end(time.time() if now is None else now)
        except (OSError, ValueError, TypeError, KeyError, RuntimeError):
            pass  # An unknown calendar cannot prove an earliest restart.
    plan = [{"id": kind, "registered": unit in indexed,
             "mode": "timer",
             "timer_active": states[unit].get("ActiveState") == "active" if unit in states else None,
             "owner_running": states[unit.removesuffix(".timer") + ".service"].get("ActiveState") in {"active", "activating"}
                 if unit.removesuffix(".timer") + ".service" in states else None,
             "owner_state": choice(states.get(unit.removesuffix(".timer") + ".service", {}).get("ActiveState"),
                                   OWNER_STATES, "unknown"),
             "next_check_at_utc": iso(finite(indexed.get(unit, {}).get("next")) / 1e6)
                 if finite(indexed.get(unit, {}).get("next")) else None,
             "last_check_at_utc": iso(finite(indexed.get(unit, {}).get("last")) / 1e6)
                 if finite(indexed.get(unit, {}).get("last")) else None}
            for unit, (kind, _) in TIMERS.items()]
    for unit, (kind, _) in CONTINUOUS.items():
        owner = states.get(unit, {})
        plan.append({"id": kind, "mode": "continuous", "registered": owner.get('LoadState') == 'loaded',
                     "owner_state": choice(owner.get('ActiveState'), OWNER_STATES, 'unknown'),
                     "owner_running": owner.get('ActiveState') in {'active', 'activating'} if owner else None})
    by_kind = {**{kind: unit.removesuffix('.timer') + '.service' for unit, (kind, _) in TIMERS.items()},
               **{kind: unit for unit, (kind, _) in CONTINUOUS.items()}}
    for entry in plan:
        owner = states.get(by_kind[entry['id']], {})
        entry_priority_end = priority_end
        if _storage_priority_condition_runway(owner) == 20:
            try:
                entry_priority_end = _storage_priority_window_end(
                    time.time() if now is None else now, minimum_runway_minutes=20)
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                entry_priority_end = None
        entry['owner_result'] = choice(owner.get('Result'),
            {'success', 'exit-code', 'exec-condition', 'signal', 'core-dump', 'timeout', 'watchdog', 'resources', 'start-limit-hit'}, 'unknown')
        if (entry['id'] in PRIORITY_GUARDED_SCHEDULES and entry_priority_end is not None
                and _is_storage_priority_condition(owner)
                and entry.get('owner_running') is False):
            entry['window_guard_reason'] = 'tw_day_trade_priority_window'
            entry['work_not_before_utc'] = iso(entry_priority_end)
        entry['owner_pid'] = int(owner['MainPID']) if owner.get('MainPID', '').isdigit() else None
        entry['exit_code'] = int(owner['ExecMainStatus']) if owner.get('ExecMainStatus', '').isdigit() else None
    return plan


def owner_locks(config: dict, plan: list[dict], *, proc=Path('/proc')) -> list[dict]:
    """Observe kernel locks on four enrolled inodes; never acquire or remove one.

    This is a non-atomic diagnostic, not a deadlock or recovery proof. An inode
    existing without a kernel holder is not a busy owner. PID/path details stay
    in this process; the observer publishes only fixed labels and counts.
    """
    options = config.get('operations') if isinstance(config.get('operations'), dict) else {}
    migration = Path(options.get('migration_state_root', '/var/lib/stockagent/lakehouse-control/dataset-versions'))
    backup = Path(options.get('backup_state_root', '/var/lib/stockagent/backup-stream'))
    paths = {'backup_owner': backup / 'owner.lock', 'dataset_owner': migration / 'owner.lock',
             'catalog_owner': migration.parent / 'catalog-owner.lock',
             'replication_owner': migration.parent / 'source-replication-owner.lock'}
    try:
        with (proc / 'locks').open() as stream:
            raw = stream.read(256 * 1024 + 1)
        if len(raw) > 256 * 1024:
            return []
    except OSError:
        return []
    pids = {entry['owner_pid']: entry['id'] for entry in plan
            if number(entry.get('owner_pid')) and entry['id'] in SCHEDULE_LABELS}

    def identify(pid):
        for _ in range(8):
            if pid in pids:
                return pids[pid]
            try:
                status = (proc / str(pid) / 'stat').read_text()[:16384]
                parent = int(status.rsplit(')', 1)[1].split()[1])
            except (OSError, ValueError, IndexError):
                break
            if parent <= 1 or parent == pid:
                break
            pid = parent
        return None

    result = []
    for name, path in paths.items():
        try:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or path.parent.resolve() != path.parent:
                continue
        except OSError:
            continue
        held, waiting = [], []
        for line in raw.splitlines():
            fields = line.split()
            waiter = len(fields) > 1 and fields[1] == '->'
            if waiter:
                fields.pop(1)
            if len(fields) != 8 or fields[1] != 'FLOCK':
                continue
            try:
                major, minor, inode = fields[5].split(':')
                if (int(major, 16), int(minor, 16), int(inode)) != (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino):
                    continue
                (waiting if waiter else held).append(identify(int(fields[4])))
            except (ValueError, IndexError):
                continue
        result.append({'id': name, 'held': bool(held), 'holder': next((p for p in held if p), None),
                       'waiter_count': len(waiting), 'waiting': sorted({p for p in waiting if p})})
    return result


def _estimate_counter(row: dict, previous: dict, *, now: float) -> tuple[dict, dict]:
    """Use observed counter deltas, not elapsed time from a different stage."""
    previous = previous if isinstance(previous, dict) else {}
    metric = "bytes" if number(row.get("total_bytes")) else "items"
    done, total = number(row.get("completed_" + metric)), number(row.get("total_" + metric))
    stamp = timestamp(row.get("observed_at_utc"))
    # A pause is a real scheduling boundary: a pre-pause throughput cannot
    # forecast when a blocked or waiting owner will start again.
    signature = key((row.get("generation"), row.get("phase"), row.get("state"), metric, total))
    points = previous.get("points", []) if previous.get("signature") == signature else []
    points = points if isinstance(points, list) else []
    points = [p for p in points[-24:] if isinstance(p, list) and len(p) == 2
              and finite(p[0]) is not None and number(p[1]) is not None
              and now - 1800 <= p[0] <= now + 120]
    eta = {"state": "unknown", "reason": "no_counter", "scope": "current_phase_only",
           "remaining_seconds": None, "fastest_seconds": None, "slowest_seconds": None,
           "completion_at_utc": None, "rate_per_second": None, "unit": metric}
    if done is not None and total is not None and 0 <= done <= total and stamp is not None and stamp <= now + 120:
        if points and (stamp < points[-1][0] or done < points[-1][1]):
            points = []
        if not points or stamp > points[-1][0]:
            points.append([stamp, done])
        elif points[-1][1] != done:
            points = [[stamp, done]]  # Ambiguous clock/counter pair, resample.
        points = points[-24:]
    history = {"signature": signature, "points": points}
    if row.get("complete_evidence") is True and row.get("state") == "completed":
        eta.update(state="completed", reason=None, remaining_seconds=0)
    elif row.get('phase') == 'source_refresh_trading_wait' and row.get('state') == 'waiting':
        eta.update(state='unknown', reason='tw_day_trade_priority_window')
    elif row.get('phase') == 'spool_paused_user' and row.get('reason') == 'user_paused_extra_verification':
        eta.update(state='unknown', reason='user_paused_extra_verification')
    elif row.get('phase') == 'spool_removed_user' and row.get('reason') == 'user_removed_vast_verification':
        eta.update(state='unknown', reason='user_removed_vast_verification')
    elif stamp is None:
        eta["reason"] = "metadata_unavailable"
    elif stamp > now + 120:
        eta.update(state="unknown", reason="clock_ahead")
    elif (row.get('state') == 'waiting' and row.get('phase') == 'waiting_new_sealed_packs'
          and (now - stamp <= STALE_SECONDS or
               (timestamp(row.get('activity_at_utc')) is not None
                and 0 <= now - timestamp(row['activity_at_utc']) <= STALE_SECONDS))):
        eta.update(state='unknown', reason='upstream_publication')
    elif now - stamp > STALE_SECONDS:
        eta.update(state="stale", reason="receipt_stale")
    elif row.get("state") == "blocked":
        eta.update(state="blocked", reason=row.get("reason") or "dependency")
    elif row.get("state") == "waiting":
        eta.update(state="unknown", reason="progress_pending")
    elif done is None or total is None or total <= 0 or done > total:
        pass
    elif done == total:
        eta.update(state="finalizing", reason="finalizing")
    elif len(points) < 2 or points[-1][0] - points[0][0] < 15:
        eta.update(state="sampling", reason="sampling")
    else:
        last_change = next((points[i][0] for i in range(len(points)-1, 0, -1)
                            if points[i][1] > points[i-1][1]), points[0][0])
        if now - last_change >= STALE_SECONDS:
            eta.update(state="stalled", reason="no_recent_progress")
        else:
            duration = points[-1][0] - points[0][0]
            rate = (points[-1][1] - points[0][1]) / duration
            rates = [(b[1]-a[1])/(b[0]-a[0]) for a, b in zip(points, points[1:]) if b[0] > a[0]]
            if rate > 0:
                remaining = total - done
                seconds = math.ceil(remaining / rate)
                # Work was measured at the source observation, not at this
                # minute's metadata refresh. Re-reading one receipt must not
                # slide its absolute deadline forward by another minute.
                completion = stamp + seconds
                if completion <= now:
                    eta.update(state='sampling', reason='forecast_overdue')
                else:
                    eta.update(state="estimated", reason=None, remaining_seconds=math.ceil(completion-now),
                        fastest_seconds=math.ceil(max(0, stamp+remaining/max(rates)-now)) if len(rates) >= 2 else None,
                        slowest_seconds=math.ceil(max(0, stamp+remaining/min(rates)-now)) if len(rates) >= 2 and min(rates) > 0 else None,
                        completion_at_utc=iso(completion), rate_per_second=round(rate, 4))
            else:
                eta.update(state="sampling", reason="sampling")
    return eta, history


def estimate(row: dict, previous: dict, *, now: float) -> tuple[dict, dict]:
    """Account for both byte work and per-file work within the same phase.

    Small-file tails may have almost no bytes left but substantial metadata
    work. Use the slower observed projection, never sum two overlapping costs.
    An unmeasured item rate cannot invent a finite item completion estimate.
    """
    if row.get('kind') in {'d_cold_e_handoff', 'd_cold_e_total'} and number(row.get('total_bytes')):
        # These waves contain a few multi-GiB packs and many tiny objects.
        # Extrapolating their early item rate over the tail invents hours of
        # work. Use measured byte throughput; item counters remain visible.
        eta, history = _estimate_counter({**row, 'total_items': None}, previous, now=now)
        forecast = row.get('_completed_wave_forecast')
        if (row.get('kind') == 'd_cold_e_total' and row.get('state') in {'running', 'waiting'}
                and isinstance(forecast, dict)):
            # Anchor the forecast at an actual completed-wave time. A single
            # deletion burst must not imply that the next wave takes seconds.
            eta.update(forecast)
        return eta, history
    if (row.get('kind') in {'workspace_e_resize', 'workspace_c_compaction'}
            and row.get('state') == 'blocked'):
        # An explicit external prerequisite remains pending after five minutes.
        # Re-reading the plan neither renews its clock nor predicts downtime.
        eta, history = _estimate_counter(row, previous, now=now)
        eta.update(state='blocked', reason=row.get('reason'), remaining_seconds=None,
                   fastest_seconds=None, slowest_seconds=None, completion_at_utc=None,
                   rate_per_second=None)
        return eta, history
    if (row.get('kind') == 'local_testing_cleanup' and row.get('_opening_perf_timer') is True
            and row.get('phase') == 'opening_perf_cleanup_working'):
        # This explicitly pinned batch consists of 21 similarly sized
        # single-file benchmark outputs. Its item delta measures the complete
        # per-file consumer check and unlink, not just a byte-copy rate.
        return _estimate_counter({**row, 'completed_bytes': None, 'total_bytes': None}, previous, now=now)
    if row.get('kind') in {'local_testing_cleanup', 'd_testing_cleanup', 'workspace_transport_retirement'} and row.get('complete_evidence') is not True:
        # A 20-GiB file and an 800k-entry tree are one target each. Their
        # completion count is a useful bar, never a comparable speed sample.
        eta, history = _estimate_counter({**row, 'total_bytes': None, 'total_items': None},
                                        previous, now=now)
        if row.get('kind') == 'workspace_transport_retirement' and number(row.get('total_bytes')):
            # Keep the byte progress usable by older public projectors too;
            # the unknown ETA still admits no rate or completion timestamp.
            eta['unit'] = 'bytes'
        if eta['state'] == 'unknown':
            eta['reason'] = 'heterogeneous_workloads'
        return eta, history
    content = row.get('eta_work')
    if isinstance(content, dict):
        # Bind to this exact phase's real member counters; no bucket-speed
        # history or earlier routing rate can cross this interpretation change.
        eta, history = estimate({**row, **content, 'eta_work': None,
                                 'generation': key((row.get('generation'), 'source_content_eta_v1'))},
                                previous, now=now)
        eta['scope'] = 'remaining_source_content'
        return eta, history
    eta, history = _estimate_counter(row, previous, now=now)
    if row.get('kind') == 'workspace_source_staging' and row.get('phase') == 'workspace_seed_copy':
        # rsync reports file-list checks, not independently verified originals.
        eta['scope'] = 'rsync_filelist_estimate'
    # Encoding content and metadata have separate explicit phases. File-count
    # extrapolation during decoding charges a large file's cost to every tiny
    # receipt. Preserve the general small-file-tail guard for other owners.
    content_decoding = ((row.get('kind') == 'encoding_stage_retirement'
                         and row.get('phase') in {'verify-encoding-stage-files', 'reconstruct-encoding-stage-files'})
                        or (row.get('kind') in {'handoff_source_repack', 'handoff_index_archive', 'offline_repair_archive'}
                            and row.get('phase') == 'handoff_source_packing')
                        or (row.get('kind') in {'handoff_private_d', 'handoff_index_D_storage'}
                            and row.get('phase') == 'handoff_source_D_installing')
                        or (row.get('kind') == 'source_local_repack_queue'
                            and row.get('phase') in {'source_local_E_packing', 'source_local_dedup_packing',
                                                     'source_local_D_installing'}))
    if number(row.get('total_bytes')) and number(row.get('total_items')) and not content_decoding:
        old_items = previous.get('items', {}) if isinstance(previous, dict) else {}
        item_eta, item_history = _estimate_counter({**row, 'total_bytes': None}, old_items, now=now)
        history['items'] = item_history
        byte_seconds, item_seconds = eta.get('remaining_seconds'), item_eta.get('remaining_seconds')
        if ((number(row.get('completed_bytes')) is None
             and number(row.get('completed_items')) is not None)
                or (eta['state'] == 'estimated' and item_eta['state'] == 'estimated'
             and item_seconds > byte_seconds)
                or (eta['state'] == 'finalizing'
                    and number(row.get('completed_items')) is not None
                    and row['completed_items'] < row['total_items'])):
            eta = item_eta
        if (row.get('kind') == 'vast_spool' and row.get('phase') == 'sealed_pack_staging'
                and eta.get('state') == 'estimated'
                and number(row.get('completed_bytes')) is not None
                and number(row.get('completed_items')) is not None):
            # A few tiny cache hits cannot predict the time needed for a
            # remaining queue dominated by much larger archives, or vice versa.
            # Keep actual progress; wait for representative work before ETA.
            byte_points = history.get('points', [])
            item_points = item_history.get('points', [])
            if len(byte_points) >= 2 and len(item_points) >= 2:
                delta_bytes = byte_points[-1][1] - byte_points[0][1]
                delta_items = item_points[-1][1] - item_points[0][1]
                remaining_items = row['total_items'] - row['completed_items']
                remaining_bytes = row['total_bytes'] - row['completed_bytes']
                if delta_bytes > 0 and delta_items > 0 and remaining_items > 0 and remaining_bytes > 0:
                    ratio = (remaining_bytes / remaining_items) / (delta_bytes / delta_items)
                    if ratio > 4 or ratio < .25:
                        eta.update(state='unknown', reason='heterogeneous_workloads',
                            remaining_seconds=None, fastest_seconds=None, slowest_seconds=None,
                            completion_at_utc=None, rate_per_second=None)
    if (((row.get('kind') == 'source_local_repack_queue'
          and row.get('phase') in {'source_local_E_packing', 'source_local_dedup_packing'})
         or (row.get('kind') == 'handoff_index_archive'
             and row.get('subject') in {'tw-futures-universe725-workspace', 'tw-futures-universe765-workspace'}
             and row.get('phase') == 'handoff_source_packing'))
            and row.get('state') == 'running'
            and (number(row.get('total_items')) or 0) >= 10000
            and number(row.get('completed_items')) is not None
            and number(row.get('completed_bytes')) is not None
            and number(row.get('total_bytes'))):
        # A large source can begin with a few multi-GB files and end with
        # millions of small ones. The byte progress is correct, but its early
        # speed is not a representative estimate of the remaining file work.
        # Keep a separate item sample and suppress a deadline while the sample
        # and remaining average file sizes differ by more than fourfold.
        old_items = previous.get('items', {}) if isinstance(previous, dict) else {}
        item_eta, item_history = _estimate_counter({**row, 'total_bytes': None}, old_items, now=now)
        history['items'] = item_history
        byte_points, item_points = history.get('points', []), item_history.get('points', [])
        if eta.get('state') == 'estimated' and len(byte_points) >= 2 and len(item_points) >= 2:
            delta_bytes = byte_points[-1][1] - byte_points[0][1]
            delta_items = item_points[-1][1] - item_points[0][1]
            remaining_items = row['total_items'] - row['completed_items']
            remaining_bytes = row['total_bytes'] - row['completed_bytes']
            mixed = False
            if delta_bytes > 0 and remaining_items > 1000 and remaining_bytes > 0:
                if delta_items <= 0:
                    mixed = True
                else:
                    ratio = (remaining_bytes / remaining_items) / (delta_bytes / delta_items)
                    mixed = ratio > 4 or ratio < .25
            if mixed:
                eta.update(state='unknown', reason='heterogeneous_workloads',
                    remaining_seconds=None, fastest_seconds=None, slowest_seconds=None,
                    completion_at_utc=None, rate_per_second=None)
            elif (item_eta.get('state') == 'estimated'
                    and item_eta['remaining_seconds'] > eta['remaining_seconds']):
                eta = item_eta
    return eta, history


def spool_tasks(repo):
    status = metadata(repo / 'service_data/pack_compute_spool/status.json')
    if status.get('contract') != 'stockagent.pack_compute_spool.v1':
        return []
    # Public progress is a bounded separate projection; detailed per-object
    # cache records remain private and are never parsed by this observer.
    scopes = status.get('scopes', [status])
    result = []
    for value in scopes[:2] if isinstance(scopes, list) else []:
        if not isinstance(value, dict):
            continue
        waiting = value.get('state') == 'waiting_for_new_sealed_packs'
        capacity_wait = value.get('state') == 'blocked' and value.get('capacity_wait') is True
        pressure = value.get('pressure_state') if value.get('state') == 'running' else None
        pressure_phase = {'verify_fixed_d_originals': 'verify_primary_sources',
                          'reclaim_remote_cache': 'unlink_verified_transport'}.get(pressure)
        pressure_plan = value.get('pressure_plan')
        generation = value.get('context_sha256')
        if pressure_phase and isinstance(pressure_plan, str) and re.fullmatch('[0-9a-f]{32}', pressure_plan):
            generation = str(generation) + ':' + pressure_plan
        elif not pressure_phase and timestamp(value.get('inventory_observed_at_utc')) is not None:
            # Each bounded owner turn re-discovers its workload. Do not reuse
            # the rate measured before a pause or a different candidate set.
            generation = str(generation) + ':' + value['inventory_observed_at_utc']
        result.append(task('vast_spool', value.get('context_sha256', 'unavailable'), node='penguin_vast',
            state='running' if value.get('state') == 'running' else 'blocked' if value.get('state') == 'blocked' else 'waiting',
            phase=pressure_phase or ('waiting_new_sealed_packs' if waiting else 'sealed_pack_staging'),
            observed=clock(value), activity=clock(status), generation=generation,
            done_items=None if pressure_phase else value.get('completed_items'),
            total_items=None if pressure_phase else value.get('total_items'),
            done_bytes=value.get('pressure_done_bytes') if pressure_phase else value.get('completed_bytes'),
            total_bytes=value.get('pressure_total_bytes') if pressure_phase else value.get('total_bytes'),
            metrics={'active_jobs': value.get('active_jobs'),
                     'blocked_items': value.get('retry_deferred_items', value.get('failed_items')),
                     'rejected_jobs': 1 if value.get('pressure_state') == 'blocked_unknown_prior_result' else None,
                     'retry_jobs': 1 if value.get('pressure_sync_state') == 'failed' else None,
                     'discarded_allocated_bytes': value.get('pressure_reclaimed_total_bytes', value.get('pressure_reclaimed_bytes')),
                     'source_free_bytes': value.get('remote_spool_available_bytes') if capacity_wait else None,
                     'source_reserve_bytes': value.get('remote_spool_reserve_bytes') if capacity_wait else None},
            reason='physical_capacity' if capacity_wait else 'dependency' if value.get('state') == 'blocked' else 'upstream_publication' if waiting else None))
        result[-1]['subject'] = value.get('dataset')
        result[-1]['inventory_at_utc'] = value.get('inventory_observed_at_utc')
        retry_at = number(value.get('pressure_retry_after_epoch'))
        if retry_at is not None:
            result[-1]['next_attempt_at_utc'] = iso(retry_at)
    return result


def apply_spool_user_pause(rows, pause, plan):
    """Project only a recorded user pause confirmed by the live owner state.

    Retain the original work counter and observation clock as historical data;
    stale worker capacity is not a present blocker or a new activity sample.
    An unavailable timer state never counts as a confirmed pause.
    """
    schedule = next((item for item in plan if item.get('id') == 'vast_spool'), {})
    if (not isinstance(pause, dict)
            or pause.get('contract') != 'stockagent.migration_timer_pause.v1'
            or pause.get('unit') != 'stockagent-pack-compute-spool.timer'
            or schedule.get('timer_active') is not False
            or schedule.get('owner_state') != 'inactive'):
        return
    for row in rows:
        if row.get('kind') != 'vast_spool' or row.get('complete_evidence'):
            continue
        row.update(state='waiting', phase='spool_paused_user', reason='user_paused_extra_verification')
        row.pop('next_attempt_at_utc', None)
        metrics = row.get('metrics')
        if isinstance(metrics, dict):
            for name in ('active_jobs', 'blocked_items', 'retry_jobs', 'source_free_bytes', 'source_reserve_bytes'):
                metrics.pop(name, None)


def apply_spool_user_removal(rows, marker, plan, verification_policy, spool_policy):
    """Show an explicit removal only while the unit and both policies agree."""
    schedule = next((item for item in plan if item.get('id') == 'vast_spool'), {})
    if (not isinstance(marker, dict) or marker.get('contract') != 'stockagent.vast_verification_decommission.v1'
            or marker.get('state') != 'removed' or marker.get('unit') != 'stockagent-pack-compute-spool.timer'
            or marker.get('training_data_removed') is not False or clock(marker) is None
            or not isinstance(verification_policy, dict) or verification_policy.get('enabled') is not False
            or not isinstance(spool_policy, dict) or spool_policy.get('enabled') is not False
            or schedule.get('registered') is not False or schedule.get('timer_active') is not False
            or schedule.get('owner_state') != 'inactive'):
        return
    for row in rows:
        if row.get('kind') != 'vast_spool' or row.get('complete_evidence'):
            continue
        row.update(state='waiting', phase='spool_removed_user', reason='user_removed_vast_verification',
                   observed_at_utc=iso(clock(marker)), activity_at_utc=iso(clock(marker)),
                   completed_bytes=None, total_bytes=None, completed_items=None, total_items=None, metrics={})
        row.pop('next_attempt_at_utc', None)


def apply_legacy_vast_backup_user_removal(rows, marker, plan):
    """Retain historical counters as evidence without projecting a removed owner."""
    schedule = next((item for item in plan if item.get('id') == 'bulk_return'), {})
    if (not isinstance(marker, dict) or marker.get('contract') != 'stockagent.vast_verification_decommission.v1'
            or marker.get('state') != 'removed' or marker.get('legacy_backup_units_removed') is not True
            or marker.get('training_data_removed') is not False or clock(marker) is None
            or schedule.get('registered') is not False or schedule.get('timer_active') is not False
            or schedule.get('owner_state') != 'inactive'):
        return
    for row in rows:
        if row.get('kind') != 'bulk_return' or row.get('complete_evidence'):
            continue
        row.update(state='waiting', phase='spool_removed_user', reason='user_removed_vast_verification',
                   observed_at_utc=iso(clock(marker)), activity_at_utc=iso(clock(marker)),
                   completed_bytes=None, total_bytes=None, completed_items=None, total_items=None, metrics={})
        row.pop('next_attempt_at_utc', None)


def encoding_stage_tasks(repo: Path, root: Path) -> list[dict]:
    """Read two separately enrolled encodings, never discover deletion roots."""
    policy = metadata(repo / 'configs/data_sync/legacy_stage_retirement.json', limit=65536)
    if (policy.get('schema_version') != 1
            or policy.get('contract') != 'reviewed_retired_legacy_d_encoding_v1'
            or policy.get('authority_node_id') != 'penguin'):
        return []
    bindings = policy.get('datasets')
    if not isinstance(bindings, dict) or len(bindings) > 32:
        return []
    rows = []
    for dataset, binding in bindings.items():
        if (not isinstance(dataset, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', dataset)
                or not isinstance(binding, dict) or not isinstance(binding.get('snapshot_id'), str)
                or not re.fullmatch('[a-f0-9]{64}', str(binding.get('manifest_sha256')))):
            continue
        target = root / dataset
        progress = metadata(target / 'stage-prune-progress.json')
        result = metadata(target / 'result.json')
        retry = metadata(target / 'retry.json')
        def bound(value):
            return (value.get('dataset') == dataset and value.get('snapshot_id') == binding['snapshot_id']
                    and value.get('manifest_sha256') == binding['manifest_sha256'])
        accepted = (bound(result) and result.get('action') == 'encoding-stage-pruned'
                    and result.get('deleted') is True and result.get('cold_objects_deleted') == 0
                    and result.get('nas_snapshots_deleted') == 0
                    and re.fullmatch('[a-f0-9]{64}', str(result.get('prune_receipt_sha256'))) is not None
                    and number(result.get('reclaimed_allocated_file_bytes')) is not None)
        current = (bound(progress) and progress.get('contract') == 'manual-stage-prune-progress-v1'
                   and progress.get('acceptance_proof') is False)
        exhausted = retry.get('binding') == binding and (number(retry.get('attempts')) or 0) >= 3
        failed = current and progress.get('state') in {'failed', 'interrupted'}
        phase = PRUNE_PHASES.get(progress.get('prune_step'), 'working') if current else 'waiting_preflight'
        state = 'completed' if accepted else 'blocked' if failed or exhausted else 'running' if current else 'waiting'
        reason = None if accepted or current and not failed else 'owner_retry_limit' if exhausted else 'original_owner'
        row = task('encoding_stage_retirement', dataset, state=state,
            phase='retired' if accepted else phase, proof=accepted, reason=reason,
            observed=clock(result) if accepted else clock(progress) if current else clock(retry),
            generation=(dataset, binding['manifest_sha256'], phase, progress.get('stage')),
            done_bytes=result.get('reclaimed_allocated_file_bytes') if accepted else progress.get('completed_bytes'),
            total_bytes=result.get('reclaimed_allocated_file_bytes') if accepted else progress.get('total_bytes'),
            done_items=progress.get('completed_items') if current and not accepted else None,
            total_items=progress.get('total_items') if current and not accepted else None)
        row['subject'] = dataset
        rows.append(row)
    return rows


def workspace_transport_tasks(root: Path) -> list[dict]:
    value=metadata(root/'progress.json')
    intent=metadata(root/'intent.json')
    handoff=metadata(root/'handoff.json')
    attempt=metadata(root/'attempt.json')
    reconciliation=metadata(root/'reconciliation.json')
    if not value:
        return [task('workspace_transport','D-E',state='waiting',phase='workspace_inventory',reason='no_counter')]
    comparison_for_retirement = metadata(root/'comparison-status.json')
    fixed_retirement = metadata(root/'retirement-receipt.json')
    fixed_files_retired = (fixed_retirement.get('contract') == 'stockagent.transport_relocation_retirement.v1'
        and fixed_retirement.get('state') == 'registered_files_retired'
        and comparison_for_retirement.get('contract') == 'stockagent.transport_relocation_comparison.v1'
        and comparison_for_retirement.get('state') == 'completed'
        and fixed_retirement.get('comparison_identity_sha256') == comparison_for_retirement.get('identity_sha256')
        and re.fullmatch('[a-f0-9]{64}', str(comparison_for_retirement.get('identity_sha256'))) is not None
        and number(intent.get('files')) is not None and number(intent.get('logical_bytes')) is not None
        and fixed_retirement.get('files_completed') == intent.get('files')
        and fixed_retirement.get('logical_bytes_completed') == intent.get('logical_bytes')
        and fixed_retirement.get('old_root_and_empty_directories_retained') is True
        and all(fixed_retirement.get(k) == 0 for k in
                ('source_observations_deleted', 'cold_objects_deleted', 'nas_objects_deleted')))
    done=(handoff.get('state')=='completed' and handoff.get('original_sender_path_preserved') is True
          and handoff.get('old_D_copy_retirement_verified') is True
          and handoff.get('old_D_source_absent_confirmed') is True
          and handoff.get('consumer_aliases_verified') is True
          and all(handoff.get(k)==0 for k in ('source_observations_deleted','cold_objects_deleted','nas_objects_deleted'))
          and handoff.get('copy',{}).get('all_source_sha256_and_destination_readback_verified') is True)
    fixed_cutover = (not done and fixed_files_retired and handoff.get('state') == 'completed'
        and handoff.get('original_sender_path_preserved') is True
        and handoff.get('copy', {}).get('all_source_sha256_and_destination_readback_verified') is True
        and all(handoff.get(k) == 0 for k in ('source_observations_deleted', 'cold_objects_deleted', 'nas_objects_deleted'))
        and reconciliation.get('syncthing_alias_verified') is True
        and reconciliation.get('namespace_inspection_complete') is True
        and reconciliation.get('stale_mount_namespaces') == 0
        and reconciliation.get('reachable_stale_mount_namespaces') == 0
        and clock(reconciliation) is not None and clock(fixed_retirement) is not None
        and clock(reconciliation) >= clock(fixed_retirement))
    old_completion_unverified = handoff.get('state') in {'completed', 'retirement_unverified'} and not done and not fixed_cutover
    phase=('workspace_completed' if done else 'workspace_transport_files_retired' if fixed_files_retired
           else 'workspace_inventory' if value.get('state')=='inventory'
           else 'workspace_copy' if value.get('state')=='copying_and_independent_sha_readback'
           else 'workspace_retirement' if old_completion_unverified or handoff.get('state') in {'quarantined','retiring_verified_D_copy'}
           else 'workspace_handoff')
    recent_failure=(not done and clock(attempt) is not None
                    and clock(attempt)>max(clock(value) or 0,clock(handoff) or 0))
    waiting=recent_failure and attempt.get('reason_code')=='original_owner'
    rows = [task('workspace_transport','D-E',state='completed' if done or fixed_cutover else
        'waiting' if fixed_files_retired else 'blocked' if old_completion_unverified else 'waiting' if waiting else 'blocked' if recent_failure else 'running',phase=phase,
        generation=('workspace_exact_relocation_v1',intent.get('started_at_epoch'),phase),
        observed=clock(handoff) if done else clock(reconciliation) if fixed_cutover else clock(fixed_retirement) if fixed_files_retired else clock(reconciliation) if old_completion_unverified and clock(reconciliation) is not None
            else clock(attempt) if recent_failure else clock(value),
        done_bytes=intent.get('logical_bytes') if done or fixed_files_retired else value.get('bytes_completed'),
        total_bytes=None if phase=='workspace_inventory' else intent.get('logical_bytes'),
        done_items=intent.get('files') if done or fixed_files_retired else value.get('files_completed',value.get('entries')),
        total_items=None if phase in {'workspace_inventory', 'workspace_retirement'} and not done else intent.get('files'),proof=done or fixed_cutover,
        reason=None if done or fixed_cutover else
            'transport_empty_root_pending' if fixed_files_retired else
            (choice(reconciliation.get('reason_code'), REASONS) or 'transport_retirement_unverified') if old_completion_unverified else
            choice(attempt.get('reason_code'), REASONS) if recent_failure else None,
        metrics={k: reconciliation.get(k) for k in ('stale_mount_namespaces', 'reachable_stale_mount_namespaces')})]
    comparison = metadata(root/'comparison-status.json')
    if (comparison.get('contract') == 'stockagent.transport_relocation_comparison.v1'
            and re.fullmatch('[a-f0-9]{64}', str(comparison.get('identity_sha256')))
            and comparison.get('source_names_deleted') == 0
            and comparison.get('retirement_authorized') is False
            and comparison.get('NAS_acceptance_claimed') is False):
        completed = (comparison.get('state') == 'completed'
                     and number(comparison.get('files_total')) == number(comparison.get('files_completed'))
                     and number(comparison.get('logical_bytes_total')) == number(comparison.get('logical_bytes_completed')))
        rows.append(task('workspace_transport_comparison', 'D-E-comparison',
            state='completed' if completed else 'running' if comparison.get('state') == 'running' else 'unknown',
            phase='workspace_comparison_done' if completed else 'workspace_comparison',
            observed=clock(comparison), generation=comparison.get('identity_sha256'), proof=completed,
            done_bytes=comparison.get('logical_bytes_completed'), total_bytes=comparison.get('logical_bytes_total'),
            done_items=comparison.get('files_completed'), total_items=comparison.get('files_total'),
            metrics={key: comparison.get(key) for key in
                     ('same_files', 'different_files', 'missing_files', 'unstable_files', 'unavailable_files')}))
    delta = metadata(root/'delta-status.json')
    delta_receipt = metadata(root/'delta-receipt.json')
    if (delta.get('contract') == 'stockagent.transport_relocation_delta.v1'
            and delta.get('comparison_identity_sha256') == comparison.get('identity_sha256')
            and delta.get('source_names_deleted') == 0 and delta.get('retirement_authorized') is False
            and delta.get('NAS_acceptance_claimed') is False):
        verified = (delta.get('state') == 'verified' and delta_receipt.get('state') == 'verified'
                    and delta_receipt.get('contract') == delta.get('contract')
                    and delta_receipt.get('comparison_identity_sha256') == delta.get('comparison_identity_sha256')
                    and delta_receipt.get('files') == delta.get('files_total')
                    and delta_receipt.get('logical_bytes') == delta.get('logical_bytes_total'))
        failed = (not verified and clock(attempt) is not None and clock(delta) is not None
                  and clock(attempt) > clock(delta) and attempt.get('state') == 'relocation_failed')
        phase = ('workspace_delta_done' if verified else 'workspace_delta_readback'
                 if delta.get('state') == 'readback' else 'workspace_delta_copy')
        rows.append(task('workspace_transport_delta', 'D-E-delta',
            state='completed' if verified else 'blocked' if failed else 'running', phase=phase,
            observed=clock(attempt) if failed else clock(delta), proof=verified,
            reason='retirement_validation' if failed else None,
            generation=(delta.get('comparison_identity_sha256'), phase),
            done_bytes=delta.get('logical_bytes_completed'), total_bytes=delta.get('logical_bytes_total'),
            done_items=delta.get('files_completed'), total_items=delta.get('files_total')))
    restore = metadata(root/'delta-restore-status.json')
    restore_receipt = metadata(root/'delta-restore-receipt.json')
    contract = 'stockagent.transport_relocation_retirement.v1.delta_restore'
    if restore.get('contract') == contract or restore_receipt.get('contract') == contract:
        verified = (restore_receipt.get('contract') == contract
                    and restore_receipt.get('state') == 'member_restore_verified'
                    and restore_receipt.get('comparison_identity_sha256') == comparison.get('identity_sha256')
                    and restore_receipt.get('files') == delta_receipt.get('files')
                    and restore_receipt.get('logical_bytes') == delta_receipt.get('logical_bytes')
                    and restore_receipt.get('member_bytes_and_portable_metadata_verified') is True
                    and restore_receipt.get('full_tree_restore_claimed') is False
                    and restore_receipt.get('NAS_acceptance_claimed') is False)
        failed = (not verified and clock(attempt) is not None and clock(restore) is not None
                  and clock(attempt) > clock(restore) and attempt.get('state') == 'relocation_failed')
        selected = restore_receipt if verified else restore
        rows.append(task('workspace_transport_restore', 'D-E-restore',
            state='completed' if verified else 'blocked' if failed else 'running',
            phase='workspace_delta_restore_done' if verified else 'workspace_delta_restore',
            observed=clock(selected), activity=clock(attempt) if failed else None, proof=verified,
            reason='retirement_validation' if failed else None, generation=comparison.get('identity_sha256'),
            done_bytes=selected.get('logical_bytes') if verified else selected.get('logical_bytes_completed'),
            total_bytes=delta_receipt.get('logical_bytes'),
            done_items=selected.get('files') if verified else selected.get('files_completed'),
            total_items=delta_receipt.get('files')))
    retiring = metadata(root/'retirement-status.json')
    retired = metadata(root/'retirement-receipt.json')
    if (retiring.get('contract') == 'stockagent.transport_relocation_retirement.v1'
            and retiring.get('comparison_identity_sha256') == comparison.get('identity_sha256')):
        complete = (retired.get('contract') == retiring.get('contract')
                    and retired.get('state') == 'registered_files_retired'
                    and retired.get('comparison_identity_sha256') == comparison.get('identity_sha256')
                    and retired.get('files_completed') == intent.get('files')
                    and retired.get('old_root_and_empty_directories_retained') is True
                    and all(retired.get(k) == 0 for k in
                            ('source_observations_deleted', 'cold_objects_deleted', 'nas_objects_deleted')))
        held = retiring.get('state') in {'waiting_held_members', 'waiting_unexpected_members'}
        namespace_wait = (not complete and clock(reconciliation) is not None and clock(retiring) is not None
                          and clock(reconciliation) > clock(retiring)
                          and reconciliation.get('reason_code') == 'transport_stale_mount_namespace')
        failure = (not complete and clock(attempt) is not None and clock(retiring) is not None
                   and clock(attempt) > clock(retiring) and attempt.get('state') == 'relocation_failed')
        owner_wait = (not complete and clock(attempt) is not None and clock(retiring) is not None
                      and clock(attempt) > clock(retiring)
                      and attempt.get('state') == 'waiting_for_original_owner_or_reader')
        selected = retired if complete else retiring
        held_wait = metadata(root / 'retirement-wait-status.json')
        current_held_wait = (held and held_wait.get('contract') ==
                            'stockagent.transport_relocation_retirement.v1.held_wait'
                            and held_wait.get('comparison_identity_sha256') == comparison.get('identity_sha256')
                            and held_wait.get('progress_observed_at_epoch') == clock(selected)
                            and held_wait.get('deletion_attempted') is False)
        held_observation = clock(held_wait) if current_held_wait else None
        attempt_observation = clock(attempt) if failure or owner_wait else None
        latest_observation = max((stamp for stamp in (held_observation, attempt_observation)
                                  if stamp is not None), default=None)
        rows.append(task('workspace_transport_retirement', 'D-E-fixed-retirement',
            state='completed' if complete else 'blocked' if held or namespace_wait or failure
                  else 'waiting' if owner_wait else 'running',
            phase='workspace_retirement_done' if complete else 'workspace_retirement',
            observed=clock(selected), activity=(clock(reconciliation) if namespace_wait
                      else latest_observation),
            proof=complete, generation=comparison.get('identity_sha256'),
            done_bytes=selected.get('logical_bytes_completed'),
            total_bytes=intent.get('logical_bytes'),
            done_items=selected.get('files_completed'), total_items=intent.get('files'),
            reason='retirement_transport_changed' if held else 'transport_stale_mount_namespace' if namespace_wait
                   else 'retirement_validation' if failure else 'original_owner' if owner_wait else None,
            metrics={'files_held': selected.get('files_held')}))
    worker = metadata(root / 'worker-reload.json')
    candidate = worker.get('candidate_code_identity_sha256')
    if re.fullmatch('[a-f0-9]{64}', str(candidate)):
        accepted = (worker.get('state') == 'accepted'
                    and worker.get('loaded_code_identity_sha256') == candidate
                    and worker.get('workflow_chains_preserved') is True
                    and worker.get('postgres_and_temporal_server_not_restarted') is True
                    and worker.get('publication_owner_boundary_verified') is True)
        failed = worker.get('state') == 'worker_reload_failed'
        waiting = worker.get('state') == 'waiting_for_original_owner'
        rows.append(task('workspace_worker_handoff', 'E-worker',
            state='completed' if accepted else 'blocked' if failed else 'waiting' if waiting else 'unknown',
            phase='workspace_worker_ready' if accepted else 'workspace_worker_wait',
            observed=clock(worker), generation=candidate, proof=accepted,
            reason=('owner_retry_limit' if (number(worker.get('failures')) or 0) >= 3
                    else 'worker_reload_failed') if failed else 'original_owner' if waiting else None))
    return rows


def workspace_partial_cold_readback_tasks(root: Path, workspace: Path) -> list[dict]:
    """Display an exact past readback; it never admits the later unlink."""
    value = metadata(workspace / 'verify/transport-retained-partial-cold-readback-20261106-v1.json')
    comparison = metadata(root / 'comparison-status.json')
    restore = metadata(root / 'delta-restore-receipt.json')
    identity = comparison.get('identity_sha256')
    records = value.get('records')
    if (value.get('contract') != 'stockagent.transport_partial_cold_readback.v1'
            or clock(value) is None or comparison.get('state') != 'completed'
            or not isinstance(identity, str) or re.fullmatch(r'[0-9a-f]{64}', identity) is None
            or value.get('comparison_identity_sha256') != identity
            or restore.get('comparison_identity_sha256') != identity
            or restore.get('state') != 'member_restore_verified'
            or restore.get('member_bytes_and_portable_metadata_verified') is not True
            or value.get('archive_sha256_verified') is not True
            or value.get('earlier_e_independent_member_restore_verified') is not True
            or value.get('retirement_authorized') is not False or value.get('nas_acceptance_claimed') is not False
            or type(value.get('source_files_deleted')) is not int or value['source_files_deleted'] != 0
            or type(value.get('cold_objects_deleted')) is not int or value['cold_objects_deleted'] != 0
            or not isinstance(records, list) or not 1 <= len(records) <= 4096
            or number(value.get('objects_verified')) != len(records)):
        return []
    object_ids = set()
    for record in records:
        if not isinstance(record, dict):
            return []
        obj, part = record.get('object_sha256'), record.get('partial_sha256')
        if (not isinstance(obj, str) or re.fullmatch(r'[0-9a-f]{64}', obj) is None
                or not isinstance(part, str) or re.fullmatch(r'[0-9a-f]{64}', part) is None
                or number(record.get('object_bytes')) is None or number(record.get('partial_bytes')) is None
                or type(record.get('partial_matches_full_object')) is not bool
                or record['partial_matches_full_object'] != (obj == part) or obj in object_ids):
            return []
        object_ids.add(obj)
    full = sum(r['partial_matches_full_object'] for r in records)
    empty = sum(r['partial_bytes'] == 0 for r in records)
    size = sum(r['object_bytes'] for r in records)
    if (value.get('partial_exact_full_objects') != full or value.get('empty_partial_count') != empty
            or value.get('object_logical_bytes') != size):
        return []
    return [task('workspace_partial_cold_readback', identity, state='completed',
        phase='partial_cold_verified', observed=clock(value), generation=(identity, clock(value)),
        done_items=len(records), total_items=len(records), proof=True,
        metrics={'partial_exact_full_files': full, 'partial_empty_files': empty, 'partial_cold_bytes': size})]


def workspace_partial_name_restore_tasks(root: Path) -> list[dict]:
    """Show the enrolled fixed preparation, independently of D retirement."""
    from stockagent.data_sync import transport_restore_held_partials as restore
    try:
        if not restore.enrolled(root):
            return []
    except (OSError, ValueError, TypeError):
        return [task('workspace_partial_name_restore', restore.COMPARISON, state='blocked',
                     phase='partial_name_pending', reason='source_or_copy_recheck_required')]
    comparison = metadata(root / 'comparison-status.json')
    if comparison.get('state') != 'completed' or comparison.get('identity_sha256') != restore.COMPARISON:
        return [task('workspace_partial_name_restore', restore.COMPARISON, state='blocked',
                     phase='partial_name_pending', reason='source_or_copy_recheck_required')]
    status_path = root / 'partial-name-restore-status.json'
    value = metadata(status_path)
    if not value:
        if status_path.exists() or status_path.is_symlink():
            return [task('workspace_partial_name_restore', restore.COMPARISON, state='blocked',
                         phase='partial_name_pending', reason='source_or_copy_recheck_required')]
        return [task('workspace_partial_name_restore', restore.COMPARISON, state='waiting',
                     phase='partial_name_pending', generation=restore.COMPARISON,
                     done_items=0, total_items=restore.FILES, done_bytes=0, total_bytes=restore.BYTES,
                     reason='staging_queued')]
    done, size = number(value.get('files_completed')), number(value.get('logical_bytes_completed'))
    state = value.get('state')
    valid = (value.get('contract') == restore.CONTRACT and
             value.get('comparison_identity_sha256') == restore.COMPARISON and
             value.get('files_total') == restore.FILES and value.get('logical_bytes_total') == restore.BYTES and
             done is not None and done <= restore.FILES and size is not None and size <= restore.BYTES and
             value.get('source_files_deleted') == 0 and value.get('NAS_acceptance_claimed') is False and
             value.get('retirement_authorized') is False and clock(value) is not None and
             state in {'restoring', 'restored'})
    members = value.get('members')
    member_paths = set()
    if valid and state == 'restored' and isinstance(members, list):
        for member in members:
            relative = member.get('relative') if isinstance(member, dict) else None
            sig = member.get('signature') if isinstance(member, dict) else None
            prefix = f'.staging/lake-raw-{restore.WAVE}/payload/'
            if (not isinstance(relative, str) or not relative.startswith(prefix)
                    or restore._NAME.fullmatch(relative[len(prefix):]) is None
                    or relative in member_paths or not isinstance(sig, list) or len(sig) != 9
                    or any(type(item) is not int or item < 0 for item in sig)):
                break
            member_paths.add(relative)
    complete = (valid and state == 'restored' and done == restore.FILES and size == restore.BYTES and
                len(member_paths) == restore.FILES and len(members) == restore.FILES)
    if not valid or (state == 'restored' and not complete):
        return [task('workspace_partial_name_restore', restore.COMPARISON, state='blocked',
                     phase='partial_name_pending', reason='source_or_copy_recheck_required')]
    return [task('workspace_partial_name_restore', restore.COMPARISON,
                 state='completed' if complete else 'running',
                 phase='partial_name_restored' if complete else 'partial_name_restoring',
                 observed=clock(value), generation=restore.COMPARISON, done_items=done,
                 total_items=restore.FILES, done_bytes=size, total_bytes=restore.BYTES, proof=complete)]


def _process_identity_matches(identity, *, proc_root=Path('/proc')):
    """A bounded observation, never a liveness or work-completion proof."""
    if (not isinstance(identity, dict) or not number(identity.get('pid'))
            or number(identity.get('start_ticks')) is None or not identity.get('boot_id')):
        return False
    try:
        with (proc_root / 'sys/kernel/random/boot_id').open() as stream:
            boot = stream.read(128).strip()
        with (proc_root / str(identity['pid']) / 'stat').open() as stream:
            fields = stream.read(16384).rsplit(')', 1)[1].split()
        return boot == identity['boot_id'] and int(fields[19]) == identity['start_ticks']
    except (OSError, ValueError, IndexError):
        return False


def _provider_runtime_failure(diagnostic, name, intent, progress, *, now):
    """Bind a read-only exception observation to that still-existing attempt.

    The original owner is never changed. New progress, a new plan, process
    replacement or an accepted handoff supersedes this diagnostic. A status
    read does not renew its clock or turn it into source/recovery acceptance.
    """
    if (diagnostic.get('contract') != 'stockagent.provider_runtime_observation.v1'
            or diagnostic.get('acceptance') is not False
            or diagnostic.get('source_files_deleted') != 0
            or not _process_identity_matches(diagnostic.get('owner'))):
        return None
    rows = diagnostic.get('providers')
    value = rows.get(name) if isinstance(rows, dict) else None
    if not isinstance(value, dict):
        return None
    observed = clock(value)
    digest = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True,
        separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    if (value.get('reason_code') not in PROVIDER_FAILURE_REASONS or observed is None
            or observed > now + 120 or observed < (clock(progress) or 0)
            or value.get('intent_sha256') != digest(intent)
            or value.get('progress_sha256') != digest(progress)
            or not _process_identity_matches(value.get('process'))):
        return None
    return {'reason_code': value['reason_code'], 'observed_at_utc': iso(observed)}


def provider_workspace_tasks(repo: Path, registry: Path, workspace: Path, *, diagnostic_path=None,
                             now=None) -> list[dict]:
    """Observe the original three-provider handoff without touching its owner.

    Read bounded control files only. The selected namespace can exclude an
    existing child tree; neither the byte bar nor a completed alias proves
    retirement of the retained C names or restoration of scheduled services.
    """
    value = metadata(registry)
    if not value:
        return []
    providers = value.get('providers')
    allowed = {'finlab', 'finmind', 'tej'}
    if (value.get('contract') != 'temporary_provider_e_workspace_v1'
            or not isinstance(providers, dict) or set(providers) - allowed):
        raise ValueError('provider workspace observation registry differs')
    generation = 'provider-state-temporary-20261103'
    diagnostic = metadata(diagnostic_path) if diagnostic_path is not None else {}
    now = time.time() if now is None else now
    rows = []
    placements = []
    for name in sorted(providers):
        entry = providers[name]
        root = workspace / 'migration' / generation / name
        destination = workspace / 'downloads' / generation / name / 'data'
        alias = repo / ('data_' + name)
        valid = (isinstance(entry, dict) and entry.get('evidence') == str(root)
                 and entry.get('destination') == str(destination) and entry.get('alias') == str(alias)
                 and entry.get('backup') == str(repo / ('.data_' + name + '.pre-e-20261103')))
        intent = metadata(root / 'intent.json') if valid else {}
        valid = (valid and intent.get('contract') == 'workspace_scoped_relocation_v1'
                 and intent.get('source') == str(alias) and intent.get('destination') == str(destination)
                 and number(intent.get('files')) is not None
                 and number(intent.get('logical_bytes')) is not None
                 and clock(intent) is not None)
        if not valid:
            row = task('provider_workspace', name, state='blocked', phase='workspace_inventory',
                       reason='metadata_unavailable')
            row['subject'] = name
            rows.append(row)
            continue
        progress = metadata(root / 'progress.json')
        copied = metadata(root / 'copy-result.json')
        placed = metadata(root / 'placement-result.json')
        namespaces = metadata(root / 'namespace-readback.json')
        failure = _provider_runtime_failure(diagnostic, name, intent, progress, now=now)
        copy_verified = (copied.get('contract') == intent['contract']
                         and copied.get('state') == 'exact_destination_readback_verified'
                         and copied.get('selected_source_sha256_and_destination_readback_verified') is True
                         and copied.get('source_namespace_unchanged') is True
                         and copied.get('source_files_deleted') == 0
                         and copied.get('files') == intent['files']
                         and copied.get('logical_bytes') == intent['logical_bytes']
                         and copied.get('excluded_subtrees') == intent.get('excluded_subtrees', [])
                         and clock(copied) is not None and clock(copied) >= clock(intent))
        binding = entry.get('copy_result_sha256')
        done = (copy_verified and entry.get('state') == placed.get('state') == 'active'
                and isinstance(binding, str) and re.fullmatch(r'[0-9a-f]{64}', binding)
                and placed.get('copy_result_sha256') == namespaces.get('copy_result_sha256') == binding
                and placed.get('provider') == namespaces.get('provider') == name
                and placed.get('original_files_deleted') == 0
                and namespaces.get('service_code_mounts_changed') == 0
                and isinstance(namespaces.get('namespace_readbacks'), list)
                and bool(namespaces['namespace_readbacks'])
                and placed.get('activated_at_epoch') == entry.get('activated_at_epoch')
                and timestamp(entry.get('activated_at_epoch')) is not None)
        stamp = (timestamp(entry['activated_at_epoch']) if done else clock(copied) if copy_verified
                 else clock(progress))
        current_progress = stamp is not None and stamp >= clock(intent)
        handoff = entry.get('state') in {'verified_copy', 'cutover', 'active'} or copy_verified
        phase = 'workspace_copy'
        state = ('completed' if copy_verified else 'blocked' if handoff or failure
                 else 'waiting' if not current_progress else 'running')
        row = task('provider_workspace', name, state=state, phase=phase,
                   generation=(generation, name, intent.get('started_at_epoch'), intent['files'],
                               intent['logical_bytes'], entry.get('filesystem_uuid'), phase),
                   observed=stamp if current_progress else None,
                   done_bytes=intent['logical_bytes'] if copy_verified else
                       progress.get('bytes_completed') if current_progress else None,
                   total_bytes=intent['logical_bytes'],
                   done_items=intent['files'] if copy_verified else
                       progress.get('files_completed') if current_progress else None,
                   total_items=intent['files'], proof=copy_verified,
                   reason=failure['reason_code'] if failure and not copy_verified
                       else 'metadata_unavailable' if state == 'blocked' else None)
        row['subject'] = name
        if failure and not copy_verified:
            row['failure_observed_at_utc'] = failure['observed_at_utc']
        rows.append(row)
        if copy_verified:
            # Copy bytes cannot measure an alias/namespace handoff. Keep the
            # closed copy's proof and clock, then expose the next phase with
            # its own counters. Never forecast namespace work from file rate.
            row['observed_at_utc'] = iso(clock(copied))
            handoff_row = task('provider_workspace', name + '-handoff',
                state='completed' if done else 'blocked' if failure else 'waiting',
                phase='provider_workspace_completed' if done else 'workspace_handoff',
                generation=(generation, name, binding, 'namespace_handoff'),
                observed=timestamp(entry['activated_at_epoch']) if done else clock(namespaces),
                done_items=1 if done else None, total_items=1, proof=bool(done),
                reason=None if done else failure['reason_code'] if failure else 'provider_workspace_handoff')
            handoff_row['subject'] = name
            if failure and not done:
                handoff_row['failure_observed_at_utc'] = failure['observed_at_utc']
            rows.append(handoff_row)
        if done:
            placements.append((entry, intent))
    if len(placements) == 3:
        # Three directory lookups, not a payload walk or retirement proof.
        # A retained name can be empty, hardlinked or have excluded children;
        # its copy's logical bytes must never become "reclaimable C bytes".
        present, measured = 0, True
        try:
            device = repo.stat().st_dev
            for entry, _ in placements:
                path = Path(entry['backup'])
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    continue
                if stat.S_ISDIR(info.st_mode) and path.resolve() == path:
                    present += info.st_dev == device
                else:
                    measured = False
        except OSError:
            measured = False
        retained = task('provider_originals', generation, state='waiting',
            phase='provider_originals_pending', generation=(generation, 'retirement_not_accepted'),
            reason='provider_originals_pending', metrics={
                'retained_original_roots': present if measured else None,
                'total_original_roots': len(placements),
                'selected_copy_bytes': sum(intent['logical_bytes'] for _, intent in placements)})
        retained['subject'] = 'provider-originals'
        rows.append(retained)
    return rows


def workspace_private_scratch_tasks(root: Path, *, identity='fixed-sqlite', expected_sha256=None,
                                    subject=None) -> list[dict]:
    journal = metadata(root / 'journal.json')
    progress = metadata(root / 'stream-progress.json')
    complete = metadata(root / 'complete.json')
    attempt = metadata(root / 'attempt.json')
    if expected_sha256 is not None and (not isinstance(expected_sha256, str)
            or not re.fullmatch(r'[0-9a-f]{64}', expected_sha256)
            or journal.get('snapshot_sha256') != expected_sha256
            or complete and complete.get('snapshot_sha256') != expected_sha256):
        raise ValueError('fixed SQLite workspace observation identity differs')
    done = (complete.get('contract') == 'private_sqlite_workspace_relocation_v1'
            and complete.get('state') == 'completed'
            and complete.get('owned_scratch_files_deleted') == 1
            and complete.get('source_files_deleted') == 0
            and complete.get('cold_objects_deleted') == 0
            and complete.get('nas_files_deleted') == 0)
    recent_failure = (not done and clock(attempt) is not None
                      and clock(attempt) > (clock(progress) or 0))
    wait = progress.get('state') == 'waiting_process_references'
    phase = ('scratch_completed' if done else 'scratch_readback' if progress.get('state') == 'readback'
             else 'scratch_binding' if progress.get('state') in {'checking_binding', 'waiting_process_references'}
             else 'scratch_copy')
    state = ('completed' if done else 'blocked' if recent_failure else 'waiting' if wait or not progress
             else 'running')
    total = journal.get('snapshot_bytes', progress.get('bytes_total'))
    row = task('workspace_private_scratch', identity, state=state, phase=phase,
        generation=('private_sqlite_workspace_relocation_v1', journal.get('snapshot_sha256'), phase),
        observed=clock(complete) if done else clock(attempt) if recent_failure else clock(progress),
        done_bytes=total if done else progress.get('bytes_completed'), total_bytes=total, proof=done,
        reason=choice(attempt.get('reason_code'), REASONS, 'scratch_error') if recent_failure
               else 'scratch_reference' if wait else None)
    row['subject'] = choice(subject, SUBJECTS)
    return [row]


def workspace_controls_tasks(candidates: list[dict]) -> list[dict]:
    """Project only fixed, enrolled handoffs; copying is not cold acceptance."""
    rows = []
    for candidate in candidates:
        root = Path(candidate['state_root'])
        batch = candidate['batch_number']
        expected = candidate['controls_identity']
        if (type(batch) is not int or not 1 <= batch <= 4
                or not isinstance(expected, str) or not re.fullmatch(r'[0-9a-f]{64}', expected)):
            raise ValueError('invalid fixed control workspace observation')
        journal = metadata(root / 'journal.json')
        copied = metadata(root / 'copy' / 'progress.json')
        stream = metadata(root / 'stream-progress.json')
        complete = metadata(root / 'complete.json')
        attempt = metadata(root / 'attempt.json')
        matched = not journal or journal.get('controls_identity') == expected
        done = (matched and complete.get('contract') == 'private_controls_workspace_relocation_v1'
                and complete.get('controls_identity') == expected and complete.get('state') == 'completed'
                and all(complete.get(k) == 0 for k in ('source_files_deleted', 'cold_objects_deleted', 'nas_files_deleted')))
        binding = stream.get('state') in {'checking_binding', 'waiting_process_references'}
        observed_progress = stream if binding else copied
        recent_failure = (not done and clock(attempt) is not None and clock(attempt) >
                          max(clock(observed_progress) or 0, clock(journal) or 0))
        waiting = stream.get('state') == 'waiting_process_references'
        phase = 'scratch_completed' if done else 'scratch_binding' if binding else 'workspace_copy'
        state = ('completed' if done else 'blocked' if not matched or recent_failure else
                 'waiting' if waiting or not journal else 'running')
        total_bytes = candidate.get('logical_bytes')
        total_items = candidate.get('files')
        rows.append(task('workspace_controls', batch, state=state, phase=phase,
            generation=('private_controls_workspace_relocation_v1', expected, phase), batch_number=batch,
            observed=clock(complete) if done else clock(attempt) if recent_failure else clock(observed_progress),
            done_bytes=total_bytes if done else None if binding else copied.get('bytes_completed'),
            total_bytes=total_bytes if not binding or done else None,
            done_items=total_items if done else None if binding else copied.get('files_completed'),
            total_items=total_items if not binding or done else None, proof=done,
            reason='scratch_error' if not matched else
                choice(attempt.get('reason_code'), REASONS, 'scratch_error') if recent_failure else
                'scratch_reference' if waiting else None))
    return rows


def remote_return_tasks(path: Path) -> list[dict]:
    """Project only the original Vast return owner's bounded status metadata."""
    value = metadata(path)
    current = value.get('state')
    if (value.get('automation_contract') != 'cold-return-v4-bounded-confirmations'
            or current not in {'cycle-running', 'cycle-complete', 'cycle_failed'}
            or clock(value) is None):
        return [task('remote_return', 'completed-training', reason='metadata_unavailable')]
    stages = {
        'discovering-completed-runs': 'remote_return_discovery',
        'checking-existing-release': 'remote_return_existing',
        'verifying-cold-and-retirement-gates': 'remote_return_existing',
        'transferring-from-origin': 'remote_return_transfer',
        'publishing-d-cold': 'remote_return_publish',
        'verifying-held-cold-wave': 'remote_return_confirmation',
        'prestage-e-transfer': 'remote_return_e_prestage',
        'prestage-e-content-proof': 'remote_return_e_check',
    }
    metrics, reason = {}, None
    if current == 'cycle-running':
        state, phase = 'running', stages.get(value.get('phase'), 'working')
    elif current == 'cycle_failed':
        state, phase = 'blocked', 'remote_return_failed'
        reason = ('remote_return_connection_failed' if value.get('remote_exit_code') == 255
                  else 'remote_return_failed')
    else:
        rows = value.get('rows')
        if value.get('apply') is not True or not isinstance(rows, list) or not all(isinstance(x, dict) for x in rows):
            return [task('remote_return', 'completed-training', reason='metadata_unavailable')]
        state, phase = 'waiting', 'remote_return_waiting'
        actions = [x.get('action') for x in rows]
        metrics = {'remote_observed': value.get('observed'), 'remote_eligible': value.get('eligible'),
                   'remote_published': value.get('published'),
                   'remote_cycle_deferred': sum(x in {'return-cycle-limit', 'publish-limit'} for x in actions),
                   'remote_pending_confirmation': value.get('pending_return_count'),
                   'remote_confirmation_limit': value.get('pending_return_limit'),
                   'remote_deferred_recovery': value.get('deferred_return_count')}
        if 'waiting-local-capacity' in actions:
            state, reason = 'blocked', 'remote_return_capacity'
        elif 'waiting-confirmation-capacity' in actions:
            state, reason = 'blocked', 'remote_return_confirmation_capacity'
        elif 'waiting-fixed-release-unavailable' in actions:
            state, reason = 'blocked', 'remote_return_release_unavailable'
        elif any(isinstance(row.get('source_retirement'), dict)
                 and row['source_retirement'].get('state') == 'recovery-deferred-user-migration' for row in rows):
            state, phase, reason = 'waiting', 'recovery_deferred_user_migration', 'recovery_deferred_user_migration'
    if current != 'cycle-complete':
        summary = value.get('last_completed_summary')
        if isinstance(summary, dict) and timestamp(summary.get('observed_at_epoch')) is not None:
            metrics = {'remote_observed': summary.get('observed'), 'remote_eligible': summary.get('eligible'),
                       'remote_published': summary.get('published'), 'remote_cycle_deferred': summary.get('deferred'),
                       'remote_pending_confirmation': summary.get('pending_return_count'),
                       'remote_confirmation_limit': summary.get('pending_return_limit'),
                       'remote_deferred_recovery': summary.get('deferred_return_count'),
                       'remote_summary_epoch': int(summary['observed_at_epoch'])}
    else:
        metrics['remote_summary_epoch'] = int(clock(value))
    cache = metadata(path.parent/'remote-cache-pressure.json', limit=64*1024)
    if (cache.get('contract') == 'stockagent.remote_cache_pressure_observation.v1'
            and cache.get('state') in {'checked', 'partial', 'not_needed'} and clock(cache) is not None
            and number(cache.get('available_before_bytes')) is not None
            and number(cache.get('threshold_bytes')) is not None
            and number(cache.get('removed_allocated_bytes')) is not None):
        after = number(cache.get('available_after_bytes'))
        metrics.update(remote_cache_available_bytes=(after if after is not None else cache['available_before_bytes']),
                       remote_cache_threshold_bytes=cache['threshold_bytes'],
                       remote_cache_reclaimed_bytes=cache['removed_allocated_bytes'],
                       remote_cache_observed_epoch=int(clock(cache)))
        if number(cache.get('profile_issues')) is not None:
            metrics['remote_cache_profile_issues'] = cache['profile_issues']
    elif (cache.get('contract') == 'stockagent.remote_cache_pressure_observation.v1'
          and cache.get('state') == 'waiting' and clock(cache) is not None):
        metrics['remote_cache_failure_epoch'] = int(clock(cache))
    return [task('remote_return', 'completed-training', state=state, phase=phase,
                 observed=clock(value), reason=reason, metrics=metrics,
                 total_bytes=number(value.get('source_logical_bytes')) if current == 'cycle-running' else None,
                 total_items=number(value.get('source_files')) if current == 'cycle-running' else None)]


def d_mount_repair_tasks(path: Path) -> list[dict]:
    value = metadata(path)
    current = value.get('state') if value.get('contract') == 'stockagent.d_root_mount_repair.v1' else None
    state, reason = {
        'healthy': ('waiting', 'd_mount_healthy'),
        'repaired': ('waiting', 'd_mount_healthy'),
        'waiting_D_mount_users': ('waiting', 'd_mount_busy'),
        'waiting_trading_priority_window': ('waiting', 'd_mount_trading'),
        'repairable_mount_drift': ('waiting', 'd_mount_deferred'),
        'deferred': ('blocked', 'd_mount_deferred'),
    }.get(current, ('unknown', 'metadata_unavailable'))
    return [task('d_mount_repair', 'd-root', state=state, phase='d_mount_monitoring',
                 observed=clock(value) if current is not None else None, reason=reason)]


def bulk_return_tasks(root: Path) -> list[dict]:
    value = metadata(root / 'status.json')
    current = value.get('state')
    capacity = current == 'waiting_cold_physical_capacity'
    failed = current == 'source_preserved_needs_audit'
    metrics = {'admission_available_bytes': value.get('available_bytes')} if capacity else {}
    reclaimed = number(value.get('original_carrier_reclaimed_allocated_bytes'))
    count = number(value.get('original_carriers_reclaimed'))
    if reclaimed is not None and count is not None and count > 0:
        metrics.update(D_bulk_carriers_reclaimed=count, D_bulk_carrier_reclaimed_allocated_bytes=reclaimed,
                       D_bulk_carrier_summary_incomplete=1 if value.get('original_carrier_summary_incomplete') is True else 0)
    parts = number(value.get('landing_parts_reclaimed'))
    parts_reclaimed = number(value.get('landing_parts_reclaimed_allocated_bytes'))
    if parts is not None and parts_reclaimed is not None and parts > 0:
        metrics.update(D_bulk_landing_parts_reclaimed=parts,
                       D_bulk_landing_parts_reclaimed_allocated_bytes=parts_reclaimed,
                       D_bulk_carrier_summary_incomplete=1 if value.get('original_carrier_summary_incomplete') is True else 0)
    return [task('bulk_return', 'coordinator',
        state='blocked' if capacity or failed else 'running' if current == 'checking_received_cohorts'
              else 'waiting' if current in {'cohorts_checked', 'deferred'} else 'unknown',
        phase='capacity-preflight' if capacity else 'working', observed=clock(value),
        reason='physical_capacity' if capacity else 'bulk_return_failed' if failed else None,
        metrics=metrics)]


def public_cold_publication_observation(path: Path) -> dict:
    """Expose fixed failure codes from the last original publication receipt."""
    value = metadata(path)
    codes = value.get('blocking_findings')
    observed = timestamp(value.get('completed_at_taipei'))
    if (value.get('schema_version') != 1 or value.get('dataset') != 'tw-public'
            or value.get('status') != 'failed' or value.get('return_code') != 75
            or value.get('materialization_performed') is not False
            or value.get('runtime_link_changed') is not False
            or value.get('local_publication_committed') is True
            or observed is None or not isinstance(codes, list)):
        return {}
    reasons = [code for code in codes[:len(PUBLICATION_FAILURES)] if choice(code, PUBLICATION_FAILURES)]
    return {'observed_at_utc': iso(observed), 'reason_codes': reasons} if reasons else {}


def boot_audit_observation(path: Path) -> dict:
    """Project a bounded audit without publishing service internals or paths."""
    value = metadata(path)
    audit = value.get('boot_recovery')
    if not isinstance(audit, dict) or clock(value) is None:
        return {}
    reasons = []
    owners = audit.get('unavailable_critical_owners')
    finmind_units = (
            'stockagent-finmind-free.service', 'stockagent-finmind-sponsor.service',
            'stockagent-finmind-complement.service')
    if isinstance(owners, list) and any(name in owners for name in finmind_units):
        states = audit.get('production_owner_states')
        # Report an observed disablement as such. It does not establish that
        # a replacement finite task is healthy, or clear other boot failures.
        disabled = isinstance(states, dict) and all(isinstance(states.get(unit), dict)
            and states[unit].get('UnitFileState') in {'disabled', 'masked', 'masked-runtime'}
            and states[unit].get('ActiveState') == 'inactive' for unit in finmind_units)
        reasons.append('finmind_disabled' if disabled else 'finmind_inactive')
    lake = audit.get('lakehouse_control')
    if isinstance(lake, dict) and lake.get('required') is True:
        for field, code in (
                ('receipts_fresh', 'lakehouse_receipts_stale'),
                ('loaded_worker_matches_current_code', 'lakehouse_worker_version_unverified'),
                ('workflow_queries_verified', 'lakehouse_queries_unverified')):
            if lake.get(field) is False:
                reasons.append(code)
    return {'observed_at_utc': iso(clock(value)), 'reason_codes': reasons}


def _staging_owner_pid(lock_rows: str, device: tuple[int, int], inode: int) -> int | None:
    """Resolve only one exclusive flock for the exact registered owner inode."""
    matches = []
    for line in lock_rows.splitlines():
        fields = line.split()
        if len(fields) != 8 or fields[1:4] != ['FLOCK', 'ADVISORY', 'WRITE'] or fields[6:] != ['0', 'EOF']:
            continue
        try:
            major, minor, observed_inode = fields[5].split(':')
            owner = int(fields[4])
            if (int(major, 16), int(minor, 16), int(observed_inode)) == (*device, inode) and owner > 0:
                matches.append(owner)
        except (ValueError, TypeError):
            continue
    return matches[0] if len(matches) == 1 else None


def _staging_cgroup_path(group: str) -> str | None:
    if not group.startswith('0::'):
        return None
    relative = group[3:]
    return relative if re.fullmatch(
        r'/system\.slice/(?:stockagent-agent-[A-Za-z0-9_.@-]+|stockagent-workspace-source-staging)\.service',
        relative) else None


def _staging_e_io(name: str, workspace: Path) -> int | None:
    """Observe current owner's E writes; these are not verified file bytes."""
    lock = Path('/var/lib/stockagent/workspace-volume') / (name + '.lock')
    try:
        if lock.is_symlink() or not stat.S_ISREG((info := lock.stat()).st_mode):
            return None
        with Path('/proc/locks').open('rb') as stream:
            lock_bytes = stream.read(4 * 1024**2 + 1)
        if len(lock_bytes) > 4 * 1024**2:
            return None
        owner = _staging_owner_pid(lock_bytes.decode('ascii'),
                                   (os.major(info.st_dev), os.minor(info.st_dev)), info.st_ino)
        if owner is None:
            return None
        proc = Path('/proc') / str(owner)
        before = (proc / 'stat').read_bytes()
        group = (proc / 'cgroup').read_text().strip()
        relative = _staging_cgroup_path(group)
        if relative is None:
            return None
        mount = workspace.stat()
        device = f'{os.major(mount.st_dev)}:{os.minor(mount.st_dev)}'
        io_path = Path('/sys/fs/cgroup') / relative.lstrip('/') / 'io.stat'
        lines = io_path.read_text().splitlines()
        result = []
        for line in lines:
            fields = line.split()
            if fields and fields[0] == device:
                result.extend(part[7:] for part in fields[1:] if part.startswith('wbytes='))
        if len(result) != 1 or not result[0].isdecimal():
            return None
        # Process CPU counters can change; PID and start ticks must not.
        def identity(raw):
            fields = raw.rsplit(b') ', 1)[1].split()
            return fields[19]  # proc stat field 22, after pid/comm
        with Path('/proc/locks').open('rb') as stream:
            final_locks = stream.read(4 * 1024**2 + 1)
        final_info = lock.stat()
        if (len(final_locks) > 4 * 1024**2 or (final_info.st_dev, final_info.st_ino) != (info.st_dev, info.st_ino)
                or _staging_owner_pid(final_locks.decode('ascii'),
                                      (os.major(info.st_dev), os.minor(info.st_dev)), info.st_ino) != owner
                or identity(before) != identity((proc / 'stat').read_bytes())
                or (proc / 'cgroup').read_text().strip() != group):
            return None
        return number(int(result[0]))
    except (OSError, ValueError, UnicodeError, IndexError):
        return None


def _cftc_e_restore_metrics(workspace: Path) -> dict:
    """Project historical fixed-release readback without scanning raw data."""
    receipt = metadata(workspace / 'verify/cftc-closed-release-e-20261104-v1.json')
    reread = metadata(workspace / 'verify/cftc-E-second-readback-20261104-v1.json')
    snapshot, digest = receipt.get('snapshot_id'), receipt.get('manifest_sha256')
    observed, items = finite(receipt.get('observed_at_epoch')), reread.get('items')
    if (receipt.get('contract') != 'stockagent.cftc_closed_E_restore.v1'
            or receipt.get('state') != 'fixed_release_materialized_and_readback_verified'
            or receipt.get('dataset') != 'cftc-legacy-pre2000'
            or not isinstance(snapshot, str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,199}', snapshot) is None
            or not isinstance(digest, str) or re.fullmatch(r'[0-9a-f]{64}', digest) is None
            or not isinstance(receipt.get('inventory_sha256'), str)
            or re.fullmatch(r'[0-9a-f]{64}', receipt['inventory_sha256']) is None
            or receipt.get('raw_originals_restored') != 2 or receipt.get('source_files_deleted') != 0
            or receipt.get('source_alias_changed') is not False or receipt.get('D_snapshot_changed') is not False
            or receipt.get('NAS_acceptance_claimed') is not False
            or reread.get('contract') != 'stockagent.cftc_e_raw_second_read.v1'
            or reread.get('state') != 'verified' or reread.get('fixed_manifest_sha256') != digest
            or reread.get('raw_files') != 2 or not number(reread.get('raw_bytes'))
            or not isinstance(items, list) or len(items) != 2
            or any(not isinstance(item, dict) or not number(item.get('bytes'))
                   or not isinstance(item.get('sha256'), str)
                   or re.fullmatch(r'[0-9a-f]{64}', item['sha256']) is None for item in items)
            or sum(item['bytes'] for item in items) != reread.get('raw_bytes')
            or observed is None or observed > time.time() + 120):
        return {}
    target = (workspace / 'restore/cftc-closed-release-e-20261104-v1'
              / 'cftc-legacy-pre2000' / snapshot)
    if receipt.get('E_materialized_path') != str(target):
        return {}
    try:
        if target.is_symlink() or target.resolve(strict=True) != target or not target.is_dir():
            return {}
    except OSError:
        return {}
    ready = metadata(target.parent / ('.' + snapshot + '.READY.json'))
    if (ready.get('snapshot_id') != snapshot or ready.get('manifest_sha256') != digest
            or ready.get('inventory_sha256') != receipt.get('inventory_sha256')):
        return {}
    return {'cftc_raw_restored_files': 2, 'cftc_raw_restored_bytes': reread['raw_bytes'],
            'cftc_raw_restored_epoch': int(observed)}


def staged_source_root_changed(source: Path, recorded) -> bool | None:
    """Detect root-generation drift cheaply; equality cannot prove child freshness."""
    if (not isinstance(recorded, list) or len(recorded) != 9
            or any(type(value) is not int or value < 0 for value in recorded)):
        return None
    try:
        info = source.lstat()
        if not stat.S_ISDIR(info.st_mode) or source.is_symlink():
            return None
        current = [info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns,
                   info.st_ctime_ns, info.st_uid, info.st_gid, info.st_nlink]
        # WSL can renumber a device across boot. Root inode and metadata are
        # still sufficient to show a difference, never to prove descendants
        # have remained unchanged.
        return current[1:] != recorded[1:]
    except OSError:
        return None


def offline_stream_inflight_bytes(evidence: Path, destination: Path) -> int | None:
    """Observe at most one reserved 64-file batch; never read its payload.

    Sizes are transient transfer telemetry, not durable completion or source
    verification. A race, lock, unbound inode or unexpected state returns no
    estimate; the archive owner retains sole authority over all changes.
    """
    index = evidence/'files.sqlite'
    try:
        if index.is_symlink() or not index.is_file() or destination.is_symlink():
            return None
        db = sqlite3.connect(f'file:{index}?mode=ro', uri=True, timeout=0.1)
        try:
            rows = db.execute("SELECT f.path,f.bytes,s.temp_name,s.device,s.inode "
                              "FROM stream_files s JOIN files f ON f.path=s.path "
                              "WHERE s.state='copying' AND f.state!='verified' LIMIT 65").fetchall()
        finally:
            db.close()
        if len(rows) > 64:
            return None
        total = 0
        for relative, expected, temporary, device, inode in rows:
            if (not isinstance(relative, str) or not relative or len(relative) > 8192
                    or Path(relative).is_absolute() or str(Path(relative)) != relative
                    or '..' in Path(relative).parts or type(expected) is not int or expected < 0
                    or not isinstance(temporary, str)
                    or re.fullmatch(r'\.source-stream-[0-9a-f]{24}\.partial', temporary) is None
                    or type(device) is not int or type(inode) is not int):
                return None
            target = destination/relative
            candidate = target.with_name(temporary)
            try:
                info = candidate.lstat()
            except FileNotFoundError:
                try:
                    info = target.lstat()
                except FileNotFoundError:
                    return None  # Names can be between link and unlink observations.
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (info.st_dev, info.st_ino) != (device, inode)
                    or not 0 <= info.st_size <= expected):
                return None
            total += info.st_size
        return total
    except (OSError, ValueError, TypeError, sqlite3.Error):
        return None


def workspace_source_staging_tasks(repo: Path, config_path: Path, workspace: Path, *, now=None) -> list[dict]:
    """Read the enrolled staging owner's bounded metadata, never scan payloads."""
    from scripts import run_workspace_source_staging as source_queue

    now = time.time() if now is None else now
    if source_queue.ROOT != repo:
        raise ValueError('staging observer and repository identities differ')
    entries = source_queue.load_sources(config_path)
    queue = metadata(workspace / 'migration' / 'source-staging-queue-v1.json')
    observations = queue.get('sources') if queue.get('contract') == source_queue.CONTRACT else []
    observations = observations if isinstance(observations, list) else []
    observed = {item.get('name'): item for item in observations[:64] if isinstance(item, dict)}
    subjects = {'tw-minute-source-e-primary-20261104-v1': 'tw-minute-source',
                'tw-minute-source-stream-e-20261108-v1': 'tw-minute-source',
                'tw-daytrade-atomic-repair-e-20261108-v1': 'tw-daytrade-offline-repair',
                'tw-daytrade-close-rollback-e-20261108-v1': 'tw-daytrade-close-rollback',
                'tw-minute-train-e-primary-20261104-v1': 'tw-minute-train',
                'tw-minute-train-stream-e-20261108-v1': 'tw-minute-train',
                'tw-microstructure-train-e-primary-20261104-v1': 'tw-microstructure-train',
                'tw-futures-prepared-e-primary-20261104-v1': 'tw-futures-prepared',
                'legacy-parquet-e-primary-20261104-v1': 'legacy-parquet',
                'public-economic-e-primary-20261104-v1': 'public-economic-history',
                'binance-archive-e-primary-20261104-v1': 'binance-public-archive',
                'binance-market-e-primary-20261104-v1': 'binance-market',
                'bybit-source-stream-e-20261108-v1': 'bybit-market',
                'okx-e-primary-20261104-v1': 'okx-public-archive',
                'tw-index-options-daily-e-primary-20261104-v1': 'tw-index-options-daily',
                'tw-index-options-daily-stream-e-20261108-v1': 'tw-index-options-daily',
                'tw-index-options-raw-stream-e-20261108-v1': 'tw-index-options-raw',
                'forex-frankfurter-e-primary-20261104-v1': 'forex-frankfurter',
                'news-training-local-e-primary-20261104-v1': 'news-training-local',
                'news-fulltext-bodies-e-primary-20261104-v1': 'news-fulltext-bodies',
                'taifex-public-history-e-primary-20261104-v1': 'taifex-public-workspace',
                'yahoo-market-e-primary-20261104-v1': 'yahoo-market',
                'free-public-context-e-primary-20261104-v1': 'free-public-context',
                'cftc-legacy-pre2000-e-primary-20261104-v1': 'cftc-legacy-pre2000',
                'forex-pepperstone-e-primary-20261104-v1': 'forex-pepperstone',
                'crypto-etf-history-e-primary-20261104-v1': 'crypto-etf-history',
                'keyed-public-catalogs-e-primary-20261104-v1': 'keyed-public-catalogs',
                'crypto-reference-e-primary-20261104-v1': 'crypto-reference',
                'crypto-public-context-e-primary-20261104-v1': 'crypto-public-context',
                'dune-crypto-e-primary-20261104-v1': 'dune-crypto',
                'fred-crypto-macro-e-primary-20261104-v1': 'fred-crypto-macro',
                'coinmetrics-community-e-primary-20261104-v1': 'coinmetrics-community'}
    phases = {'inventory': 'workspace_inventory', 'seeding_hardlinked_copy': 'workspace_seed_copy',
              'verifying_previously_copied_files': 'workspace_seed_recheck',
              'copying_and_independent_sha_readback': 'workspace_copy',
              'source_stream_copy': 'workspace_source_stream_copy',
              'preparing_destination_directories': 'workspace_directory_preparing',
              'flushing_destination': 'workspace_source_finalizing',
              'verifying_source_namespace': 'workspace_source_namespace',
              'applying_destination_directory_metadata': 'workspace_directory_metadata',
              'verifying_destination_namespace': 'workspace_destination_namespace'}
    rows = []
    for item in entries:
        name = item['name']
        if name not in subjects:
            continue  # Public labels require an explicit, non-private name.
        source, destination, evidence, _ = source_queue.paths(item, workspace)
        intent, progress = metadata(evidence / 'intent.json'), metadata(evidence / 'progress.json')
        preserve_links = item.get('preserve_symlinks', False)
        expected_contract = 'workspace_scoped_relocation_v1' if preserve_links else 'workspace_exact_relocation_v1'
        bound = (intent.get('contract') == expected_contract
                 and (not preserve_links or (intent.get('preserve_symlinks') is True
                                             and intent.get('excluded_subtrees') == []))
                 and intent.get('source') == str(source) and intent.get('destination') == str(destination)
                 and intent.get('state') in {'indexing', 'indexed'})
        current = observed.get(name, {})
        current = current if isinstance(current, dict) else {}
        queue_clock = clock(queue)
        observed_io = _staging_e_io(name, workspace) if bound else None
        fresh_owner = (observed_io is not None or current.get('state') == 'waiting' and current.get('reason') == 'original_owner_or_reader'
                       and current.get('error_type') == 'BlockingIOError' and queue_clock is not None
                       and now - STALE_SECONDS <= queue_clock <= now + 120)
        try:
            completed = source_queue.verified_report(item, workspace)
        except (OSError, ValueError, TypeError, KeyError):
            completed = None
            current = {'state': 'blocked', 'reason': 'source_or_copy_recheck_required'}
        proof = bool(bound and completed is not None
                     and completed.get('files') == intent.get('files')
                     and completed.get('logical_bytes') == intent.get('logical_bytes'))
        phase = (('workspace_source_stream_staged' if item.get('copy_mode') == 'source_stream_no_readback'
                  else 'workspace_source_copy_verified') if proof else
                 phases.get(progress.get('state'), 'workspace_inventory') if bound else 'workspace_inventory')
        # Older in-flight stream owners report their final byte count before
        # walking the two namespaces and committing the completion receipt.
        # Do not show their write phase as a completed migration or keep a
        # stale copy ETA while those remaining checks are running.
        if (not proof and fresh_owner and item.get('copy_mode') == 'source_stream_no_readback'
                and phase == 'workspace_source_stream_copy'
                and type(intent.get('files')) is int and type(intent.get('logical_bytes')) is int
                and progress.get('files_completed') == intent['files']
                and progress.get('bytes_completed') == intent['logical_bytes']):
            phase = 'workspace_source_finalizing'
        state = ('completed' if proof else 'blocked' if current.get('state') == 'blocked' else
                 'running' if fresh_owner else 'waiting')
        stamp = (clock(completed) if proof else clock(progress) if bound else None)
        done_bytes = completed.get('logical_bytes') if proof else progress.get('bytes_completed') if phase in {
            'workspace_copy', 'workspace_source_stream_copy'} else None
        # Namespace checks and a prior-checkpoint recheck do not read the full
        # source byte total in that phase. Keep their item counters distinct.
        total_bytes = (completed.get('logical_bytes') if proof else
                       intent.get('logical_bytes') if bound and phase in {'workspace_seed_copy', 'workspace_copy',
                                                                           'workspace_source_stream_copy'}
                       else None)
        done_items = completed.get('files') if proof else (
            progress.get('files_completed') if phase in {'workspace_copy', 'workspace_seed_recheck',
                                                         'workspace_source_stream_copy'}
            else progress.get('entries_completed') if phase in {'workspace_directory_preparing', 'workspace_source_namespace', 'workspace_directory_metadata', 'workspace_destination_namespace'}
            else progress.get('entries') if progress.get('state') == 'inventory' else None)
        total_items = completed.get('files') if proof else (
            progress.get('entries_total') if phase in {'workspace_directory_preparing', 'workspace_source_namespace', 'workspace_directory_metadata', 'workspace_destination_namespace'}
            else progress.get('files_total') if phase == 'workspace_seed_recheck'
            else intent.get('files') if bound and intent.get('state') == 'indexed' else None)
        metrics = ({'staging_e_write_bytes': observed_io}
                   if observed_io is not None and phase == 'workspace_seed_copy' else {})
        if (name == source_queue.OFFLINE_ROLLBACK_SCOPE[0] and bound and fresh_owner and not proof
                and phase == 'workspace_source_stream_copy'
                and type(done_bytes) is int and type(total_bytes) is int):
            inflight = offline_stream_inflight_bytes(evidence, destination)
            if inflight is not None and 0 <= done_bytes + inflight <= total_bytes:
                metrics.update(durable_streamed_bytes=done_bytes, inflight_streamed_bytes=inflight)
                done_bytes += inflight
                stamp = now
        seed_sample = None
        if bound and phase == 'workspace_seed_copy' and fresh_owner:
            from stockagent.data_sync.workspace_seed_observation import bound_checkpoint, bound_sample
            seed_sample = bound_sample(name, evidence, intent, now=now)
            checkpoint = bound_checkpoint(name, evidence, intent, now=now)
            if checkpoint:
                metrics.update(seed_checkpoint_files=checkpoint['verified_files'],
                    seed_checkpoint_bytes=checkpoint['verified_bytes'],
                    seed_checkpoint_epoch=int(checkpoint['observed_at_epoch']),
                    seed_checkpoint_total_files=intent['files'],
                    seed_checkpoint_total_bytes=intent['logical_bytes'])
        if seed_sample:
            # File-list counts and transferred bytes describe rsync's current
            # attempt only, not unique bytes, full SHA readback or acceptance.
            stamp = seed_sample['observed_at_epoch']
            done_bytes = total_bytes = None
            done_items, total_items = seed_sample['entries_checked'], seed_sample['entries_total']
            metrics.update(seed_transferred_bytes=seed_sample['transferred_bytes'],
                           seed_source_logical_bytes=intent['logical_bytes'])
        if name == 'cftc-legacy-pre2000-e-primary-20261104-v1':
            metrics.update(_cftc_e_restore_metrics(workspace))
        row = task('workspace_source_staging', name, state=state, phase=phase, observed=stamp,
                   activity=now if observed_io is not None else queue_clock if fresh_owner else None,
                   generation=(name, intent.get('started_at_epoch'), phase,
                               seed_sample.get('client_start_ticks') if seed_sample else None),
                   done_bytes=done_bytes, total_bytes=total_bytes, done_items=done_items,
                   total_items=total_items, proof=proof,
                   reason=None if proof or fresh_owner else choice(current.get('reason'), REASONS,
                       'staging_queued' if not current and not bound else 'original_owner'),
                   metrics=metrics)
        row['subject'] = subjects[name]
        row['_original_staging_owner_locked'] = fresh_owner
        if proof and staged_source_root_changed(source, intent.get('source_root_signature')) is True:
            row['_staged_source_root_changed'] = True
        rows.append(row)
    return rows


def workspace_source_handoff_tasks(repo: Path) -> list[dict]:
    """Observe each reviewed source binding; never change or retire files."""
    from stockagent.data_sync import workspace_source_handoff as handoff
    from scripts import run_workspace_source_staging as staging
    if repo != handoff.ROOT:
        raise ValueError('source handoff observer repository differs')
    subjects = {handoff.NAME: 'crypto-etf-history', handoff.COIN_NAME: 'coinmetrics-community',
                handoff.BYBIT_NAME: 'bybit-market',
                handoff.OPTIONS_RAW_NAME: 'tw-index-options-raw',
                handoff.OKX_NAME: 'okx-public-archive',
                handoff.NEWS_NAME: 'news-training-local',
                handoff.NEWS_BODIES_NAME: 'news-fulltext-bodies',
                handoff.HFT_NAME: 'tw-microstructure-train',
                handoff.MINUTE_NAME: 'tw-minute-train',
                handoff.MINUTE_RAW_NAME: 'tw-minute-source',
                handoff.FUTURES_NAME: 'tw-futures-prepared',
                handoff.FUTURES_725_NAME: 'tw-futures-universe725-workspace',
                handoff.FUTURES_765_NAME: 'tw-futures-universe765-workspace',
                handoff.YAHOO_NAME: 'yahoo-market',
                handoff.PEPPERSTONE_NAME: 'forex-pepperstone',
                handoff.TAIFEX_PUBLIC_NAME: 'taifex-public-workspace',
                handoff.CFTC_NAME: 'cftc-legacy-pre2000', handoff.FRED_NAME: 'fred-crypto-macro',
                **{name: label for name, (_directory, label) in handoff.EXTRA_SOURCES.items()}}
    retired_sources = {handoff.NAME: 'crypto-etf-history', handoff.COIN_NAME: 'coinmetrics-community',
                       'keyed-public-catalogs-e-primary-20261104-v1': 'keyed-public-catalogs',
                       'crypto-reference-e-primary-20261104-v1': 'crypto-reference',
                       'dune-crypto-e-primary-20261104-v1': 'dune-crypto',
                       'crypto-public-context-e-primary-20261104-v1': 'crypto-historical-public'}
    index_retired_sources = {handoff.LEGACY_PARQUET_NAME: 'legacy-parquet',
                             handoff.BYBIT_NAME: 'bybit',
                             handoff.FREE_PUBLIC_NAME: 'free-public-context',
                             handoff.PUBLIC_ECONOMIC_NAME: 'public-economic-history',
                             handoff.NEWS_NAME: 'news-training-workspace',
                             handoff.NEWS_BODIES_NAME: 'news-fulltext-bodies-workspace',
                             handoff.HFT_NAME: 'tw-microstructure-train-workspace',
                             handoff.MINUTE_NAME: 'tw-minute-train-workspace',
                             handoff.MINUTE_RAW_NAME: 'tw-minute-source-workspace',
                             handoff.OPTIONS_RAW_NAME: 'tw-index-options-raw-workspace',
                             handoff.FUTURES_725_NAME: 'tw-futures-universe725-workspace',
                             handoff.FUTURES_765_NAME: 'tw-futures-universe765-workspace',
                             handoff.YAHOO_NAME: 'yahoo-market',
                             handoff.PEPPERSTONE_NAME: 'forex-pepperstone',
                             handoff.CFTC_NAME: 'cftc-legacy-pre2000-workspace',
                             handoff.FRED_NAME: 'fred-crypto-macro-workspace',
                             handoff.TAIFEX_PUBLIC_NAME: 'taifex-public-workspace',
                             handoff.FOREX_NAME: 'forex-frankfurter', handoff.BINANCE_NAME: 'binance',
                             handoff.BINANCE_ARCHIVE_NAME: 'binance-public-archive', handoff.OKX_NAME: 'okx'}
    rows = []
    retained = []
    for name in handoff.registered_names():
        with handoff.selected_source(name):
            scope = handoff._scope()
            try:
                retired = False
                value = handoff.read_journal()
                if name == handoff.OKX_NAME and value is None:
                    handoff._validate_okx_enrollment()
                if name == handoff.NEWS_NAME and value is None:
                    handoff._validate_news_enrollment()
                if name == handoff.NEWS_BODIES_NAME and value is None:
                    handoff._validate_news_bodies_enrollment()
                if name == handoff.HFT_NAME and value is None:
                    handoff._validate_hft_enrollment()
                if name == handoff.MINUTE_NAME and value is None:
                    handoff._validate_minute_enrollment()
                if name == handoff.BYBIT_NAME and value is None:
                    handoff._validate_bybit_enrollment()
                if name == handoff.FUTURES_NAME and value is None:
                    handoff._validate_futures_enrollment()
                if name == handoff.FUTURES_725_NAME and value is None:
                    handoff._validate_futures_725_enrollment()
                if name == handoff.FUTURES_765_NAME and value is None:
                    handoff._validate_futures_765_enrollment()
                if name == handoff.YAHOO_NAME and value is None:
                    handoff._validate_yahoo_enrollment()
                if name == handoff.PEPPERSTONE_NAME and value is None:
                    handoff._validate_pepperstone_enrollment()
                if name in handoff.RETAINED_LOCK_SOURCES and value is None:
                    handoff._validate_retained_enrollment(name)
                last_check = metadata(scope.journal.with_suffix('.observation.json'))
                last_check_time = clock(last_check)
                last_check_valid = (last_check.get('contract') == 'stockagent.workspace_source_handoff_observation.v1'
                    and last_check.get('source') == name and last_check.get('state') in {'active', 'waiting', 'blocked'}
                    and last_check_time is not None and last_check_time <= time.time() + 120)
                journal_time = (timestamp(value.get('checked_at_epoch', value.get('activated_at_epoch')))
                                if value else None)
                recent_block = bool(last_check_valid and last_check.get('state') == 'blocked'
                                    and (journal_time is None or last_check_time >= journal_time))
                recent_wait = bool(last_check_valid and last_check.get('state') == 'waiting'
                                   and (journal_time is None or last_check_time >= journal_time))
                observations = value.get('last_namespace_readback') if value else None
                proof = (value is not None and value['state'] == 'active'
                         and isinstance(observations, list) and bool(observations)
                         and all(isinstance(entry, dict) and entry.get('verified') is True for entry in observations))
                if proof:
                    destination = handoff._assert_retained(value)
                    proof = handoff.provider_workspace.exact_mount(scope.source, destination)
                    if proof:
                        # _assert_retained checks the enrolled C root and its
                        # original lock inode. This bounded root observation
                        # neither walks payloads nor authorizes retirement.
                        retirement = (metadata(Path(value['workspace'])/'migration/handoff-source-repack-v1'
                                               /(retired_sources[name]+'.status.json'))
                                      if name in retired_sources else {})
                        detail = retirement.get('C_retirement') if isinstance(retirement.get('C_retirement'), dict) else {}
                        retired = (retirement.get('contract') == 'stockagent.handoff_source_repack.v1'
                                   and retirement.get('state') == 'C_payload_retired_root_and_lock_preserved'
                                   and retirement.get('C_payload_deleted') is True
                                   and detail.get('source_root_inode') == value.get('original_inode')
                                   and detail.get('source_lock_inode') == value.get('original_lock_inode'))
                        if not retired and name in index_retired_sources:
                            indexed = metadata(Path(value['workspace'])/'migration/handoff-index-archive-v1'
                                               /(index_retired_sources[name]+'.status.json'))
                            detail = indexed.get('C_retirement') if isinstance(indexed.get('C_retirement'), dict) else {}
                            retired = (indexed.get('contract') == 'stockagent.handoff_index_archive.v1'
                                and indexed.get('state') == 'C_payload_retired_root_and_locks_preserved'
                                and indexed.get('C_payload_deleted') is True
                                and detail.get('source_root_inode') == value.get('original_inode')
                                and detail.get('source_lock_inode') == value.get('original_lock_inode'))
                        if not retired:
                            retained.append((name, number(value.get('logical_bytes'))))
                if recent_block:
                    proof = False
                progress = metadata(scope.journal.with_suffix('.progress.json'))
                stage = progress.get('verification_pass')
                valid_progress = (name != handoff.NAME
                    and progress.get('contract') == 'stockagent.workspace_source_handoff_progress.v1'
                    and progress.get('source') == name and stage in {'initial', 'preexchange'}
                    and re.fullmatch(r'[0-9a-f]{32}', str(progress.get('generation', ''))) is not None
                    and timestamp(progress.get('observed_at_epoch')) is not None)
                checking = (not proof and not recent_block and valid_progress and progress.get('state') == 'running'
                            and progress.get('phase') in {'namespace', 'fence_install', 'full_sha_recheck', 'generation_recheck'})
                rechecking = checking and progress.get('phase') in {'full_sha_recheck', 'generation_recheck'}
                fence_progress = (checking and progress.get('phase') == 'fence_install'
                    and type(progress.get('fence_directories_checked')) is int
                    and type(progress.get('fence_directories_total')) is int
                    and 0 <= progress['fence_directories_checked'] <= progress['fence_directories_total'])
                namespace_progress = (checking and progress.get('phase') == 'namespace'
                    and type(progress.get('namespace_entries_checked')) is int
                    and type(progress.get('namespace_entries_total')) is int
                    and 0 <= progress['namespace_entries_checked'] <= progress['namespace_entries_total']
                    and type(progress.get('namespace_pass')) is int and 1 <= progress['namespace_pass'] <= 4)
                blocked = (recent_block or value is not None and value['state'] == 'active' and not proof
                           or not proof and valid_progress and progress.get('state') == 'blocked')
                trading_window = (name in handoff.HEAVY_SOURCES
                    and not proof and not checking and not blocked
                    and (value is None or value.get('state') in {'preparing', 'prepared'})
                    and not handoff.binance_handoff_window_allowed(for_start=True))
                # The fixed E copy's size is a planned recheck workload, not
                # evidence that any current handoff bytes have been checked.
                staged = None
                if not proof and not checking:
                    try:
                        item, workspace, *_ = handoff.enrolled()
                        staged = staging.verified_report(item, workspace)
                    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                        pass
                row = task('workspace_source_handoff', name,
                    state='completed' if proof else 'blocked' if blocked else 'running' if checking else 'waiting',
                    phase='source_handoff_retired' if proof and retired else
                          'provider_workspace_completed' if proof else
                          'source_handoff_generation' if rechecking and progress.get('phase') == 'generation_recheck' else
                          f'source_handoff_{stage}' if rechecking else
                          'source_handoff_fence_install' if fence_progress else
                          'source_handoff_namespace' if checking else 'workspace_handoff',
                    observed=timestamp(value.get('checked_at_epoch', value.get('activated_at_epoch'))) if proof else
                             last_check_time if recent_block else
                             last_check_time if recent_wait and not checking else
                             timestamp(progress.get('observed_at_epoch')) if valid_progress else None,
                    generation=(name, progress.get('generation'), progress.get('phase'),
                                progress.get('namespace_pass') if namespace_progress else None) if checking else
                               (name, value.get('created_at_epoch') if value else None),
                    done_items=value.get('files') if proof else progress.get('completed_files') if rechecking else
                               progress.get('fence_directories_checked') if fence_progress else
                               progress.get('namespace_entries_checked') if namespace_progress else None,
                    total_items=value.get('files') if proof else progress.get('total_files') if rechecking else
                                progress.get('fence_directories_total') if fence_progress else
                                progress.get('namespace_entries_total') if namespace_progress else
                                number(staged.get('files')) if isinstance(staged, dict) else None,
                    done_bytes=value.get('logical_bytes') if proof else progress.get('completed_bytes') if rechecking else None,
                    total_bytes=value.get('logical_bytes') if proof else progress.get('total_bytes') if rechecking else
                                number(staged.get('logical_bytes')) if isinstance(staged, dict) else None,
                    metrics={'namespace_pass': progress['namespace_pass'], 'namespace_passes_total': 4}
                            if namespace_progress else None,
                    proof=proof, reason=None if proof or checking else
                        choice(last_check.get('reason_code'), REASONS, 'source_or_copy_recheck_required') if recent_block else
                        'source_or_copy_recheck_required' if blocked else
                        choice(last_check.get('reason_code'), REASONS, 'original_owner') if recent_wait else
                        'tw_day_trade_priority_window' if trading_window else 'staging_queued')
                if recent_block:
                    row['failure_observed_at_utc'] = iso(last_check_time)
                if trading_window:
                    # The timer may fire later; this is only the earliest
                    # eligibility boundary, never a predicted completion.
                    local = datetime.now(ZoneInfo('Asia/Taipei'))
                    earliest = local.replace(hour=13, minute=30, second=0, microsecond=0)
                    if earliest > local:
                        row['next_attempt_at_utc'] = iso(earliest.timestamp())
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                row = task('workspace_source_handoff', name, state='blocked',
                           phase='workspace_handoff', reason='source_or_copy_recheck_required')
            row['subject'] = subjects[name]
            rows.append(row)
    if retained:
        sizes = [size for _, size in retained]
        original = task('provider_originals', 'source-handoff-retained', state='waiting',
            phase='provider_originals_pending',
            generation=('source-handoff-retained', tuple(name for name, _ in retained)),
            reason='provider_originals_pending', metrics={
                'retained_original_roots': len(retained), 'total_original_roots': len(retained),
                'selected_copy_bytes': sum(sizes) if all(size is not None for size in sizes) else None})
        original['subject'] = 'source-originals'
        rows.append(original)
    return rows


def unenrolled_source_handoff_tasks(entries: list[dict], registered: set[str], staged: list[dict]) -> list[dict]:
    """Expose fixed E copies the existing source handoff timer cannot select."""
    from scripts.run_workspace_source_staging import OFFLINE_REPAIR_SCOPES, OPTIONS_STREAM_SCOPE
    from stockagent.data_sync.workspace_source_handoff import MINUTE_RAW_NAME
    by_id = {row.get('id'): row for row in staged if isinstance(row, dict)
             and row.get('kind') == 'workspace_source_staging' and row.get('complete_evidence') is True}
    result = []
    for entry in entries:
        name = entry.get('name') if isinstance(entry, dict) else None
        if not isinstance(name, str) or name in registered or any(name == scope[0] for scope in OFFLINE_REPAIR_SCOPES):
            continue
        # The older fixed E copy remains visible as staging evidence, but it
        # is no longer the pending handoff once the same source's newer,
        # completed stream has been enrolled with the existing owner.
        newer = by_id.get(key(('workspace_source_staging', MINUTE_RAW_NAME)))
        if (name == 'tw-minute-source-e-primary-20261104-v1'
                and entry.get('source') == 'data_tw_minute/shioaji_1m'
                and MINUTE_RAW_NAME in registered and newer is not None
                and newer.get('subject') == 'tw-minute-source'):
            continue
        options_newer = by_id.get(key(('workspace_source_staging', OPTIONS_STREAM_SCOPE[0])))
        if (name == 'tw-index-options-daily-e-primary-20261104-v1'
                and entry.get('source') == OPTIONS_STREAM_SCOPE[1]
                and options_newer is not None and options_newer.get('subject') == 'tw-index-options-daily'):
            # Keep both copy receipts visible, but only the newer generation
            # is a candidate for this still-unenrolled handoff.
            continue
        source = by_id.get(key(('workspace_source_staging', name)))
        if source is None or not choice(source.get('subject'), SUBJECTS):
            continue
        reason = ('source_handoff_root_changed' if source.get('_staged_source_root_changed') is True
                  else 'source_handoff_unenrolled')
        row = task('workspace_source_handoff', name, state='blocked', phase=reason,
                   observed=timestamp(source.get('observed_at_utc')),
                   generation=(name, source.get('generation')),
                   # The fixed staging receipt describes the older copy, not
                   # the current namespace. Children may have changed or have
                   # already been handed off independently even if the root
                   # stat happens to be unchanged. Its size is never a reliable
                   # remaining-work denominator for this unenrolled handoff.
                   total_items=None, total_bytes=None,
                   reason=reason)
        row['subject'] = source['subject']
        result.append(row)
    return result


def workspace_symlink_closure_tasks(repo: Path, config_path: Path, workspace: Path) -> list[dict]:
    """Observe the link verifier's fixed receipts without following source links."""
    from scripts import run_workspace_source_staging as queue
    from stockagent.data_sync import workspace_symlink_closure as closure
    if repo != queue.ROOT:
        raise ValueError('symlink observer repository differs')
    status = metadata(workspace / 'migration/source-symlink-closure-status.json')
    observed_sources = status.get('sources')
    current = ({row.get('name'): row for row in observed_sources if isinstance(row, dict)}
               if status.get('contract') == closure.CONTRACT and isinstance(observed_sources, list) else {})
    subjects = {'tw-futures-prepared-e-primary-20261104-v1': 'tw-futures-prepared',
                'taifex-public-history-e-primary-20261104-v1': 'taifex-public-history'}
    rows = []
    for item in queue.load_sources(config_path):
        name = item['name']
        if not item.get('preserve_symlinks') or name not in subjects:
            continue
        source, destination, evidence, report_path = queue.paths(item, workspace)
        observed = current.get(name, {})
        receipt = metadata(evidence / 'symlink-closure-result.json')
        if observed.get('state') == 'no_links':
            try:
                copy = queue.verified_report(item, workspace)
                expected = closure._active_taifex_without_links(
                    item, workspace, source, destination, evidence, report_path, copy or {})
                if expected is not None and all(observed.get(k) == v for k, v in expected.items()):
                    row = task('workspace_symlink_closure', name, state='completed',
                               phase='workspace_link_no_links', observed=clock(status),
                               generation=(name, expected['source_copy_report_sha256']),
                               done_items=0, total_items=0, proof=True)
                    row['subject'] = subjects[name]
                    rows.append(row)
                    continue
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                pass
            observed = {'state': 'blocked'}
        partial = metadata(evidence / 'symlink-closure-after-725.json') if name == 'tw-futures-prepared-e-primary-20261104-v1' else {}
        partial_valid = False
        if partial:
            try:
                intent = metadata(evidence/'intent.json')
                _mapping, binding = closure._active_futures_725_boundary(
                    source, destination, intent.get('source_root_signature'), repo)
                second = closure._active_futures_765_boundary(source, destination)
                if second is not None:
                    partial = metadata(evidence/'symlink-closure-after-725-765.json')
                    partial_valid = (partial.get('scope') == 'indexed_links_after_725_765_child_handoffs'
                                     and partial.get('child_handoff') == binding
                                     and partial.get('second_child_handoff') == second)
                    if not partial:
                        observed = {'state': 'blocked'}
                else:
                    partial_valid = (partial.get('scope') == 'indexed_links_outside_725_after_child_handoff'
                                     and partial.get('child_handoff') == binding)
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                observed = {'state': 'blocked'}
            receipt = partial
        proof = False
        try:
            copy = queue.verified_report(item, workspace)
            if copy is not None and report_path.stat().st_size <= 128 * 1024 and not report_path.is_symlink():
                identity = hashlib.sha256(report_path.read_bytes()).hexdigest()
                proof = (receipt.get('contract') == closure.CONTRACT and receipt.get('name') == name
                         and receipt.get('state') == 'verified_internal'
                         and receipt.get('source_copy_report_sha256') == identity
                         and receipt.get('copy_result_sha256') == copy.get('relocation_evidence_sha256')
                         and type(receipt.get('unresolved_targets')) is int and receipt['unresolved_targets'] == 0
                         and type(receipt.get('links')) is int and receipt['links'] >= 0
                         and type(receipt.get('internal_targets_verified')) is int
                         and receipt.get('links') == receipt.get('internal_targets_verified')
                         and receipt.get('source_files_deleted') == 0
                         and (receipt.get('source_alias_changed') is True and partial_valid if partial
                              else receipt.get('source_alias_changed') is False)
                         and receipt.get('D_or_NAS_acceptance') is False
                         and receipt.get('retirement_authorized') is False
                         and (not partial or receipt.get('links') == copy.get('symlink_names_preserved')))
        except (OSError, ValueError, TypeError, KeyError):
            observed = {'state': 'blocked'}
        last_success = clock(receipt) if proof else None
        last_check = clock(status)
        # The immutable receipt records a historical fixed-copy success. A
        # later failed check of that same registered source is a current
        # blocker, not a reason to erase the earlier evidence or show it as a
        # fresh success. An older failure cannot override a newer success.
        newer_failure = (observed.get('state') in {'blocked', 'unresolved'}
                         and (last_check is None or last_success is None or last_check >= last_success))
        current_proof = proof and not newer_failure
        blocked = newer_failure or bool(receipt) and not proof
        links = number(receipt.get('links') if current_proof else observed.get('links'))
        verified = number(receipt.get('internal_targets_verified') if current_proof else observed.get('internal_targets_verified'))
        row = task('workspace_symlink_closure', name,
                   state='completed' if current_proof else 'blocked' if blocked else 'waiting',
                   phase='workspace_link_verified' if current_proof else 'workspace_link_wait',
                   observed=last_success if current_proof else last_check if newer_failure else None,
                   generation=(name, receipt.get('source_copy_report_sha256')),
                   done_items=verified, total_items=links, proof=current_proof,
                   reason=None if current_proof else 'source_or_copy_recheck_required' if blocked else 'staging_queued')
        row['subject'] = subjects[name]
        rows.append(row)
    return rows


def futures_projection_tasks(repo: Path, workspace: Path, scope=None) -> list[dict]:
    """Observe the fixed metadata-only projection without traversing payloads."""
    from scripts import project_futures_725_workspace_index as project
    scope = scope or project.default_scope()
    is_765 = scope == project.FUTURES_765
    kind = 'futures_765_projection' if is_765 else 'futures_725_projection'
    if repo != project.ROOT:
        raise ValueError('futures projection observer repository differs')
    status = metadata(workspace/'migration'/(scope.name+'.status.json'))
    report = metadata(workspace/'verify'/(scope.name+'.json'))
    proof = False
    try:
        proof = project.validated_projection(workspace, scope=scope) is not None
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        proof = False
    state = status.get('state') if status.get('contract') == project.CONTRACT else None
    active = state in {'indexing_existing_metadata','rechecking_committed_metadata','sealing_index_sha'}
    try:
        parent_report = workspace/'verify'/(project.PARENT+'.json')
        if active and (parent_report.is_symlink() or not parent_report.is_file()
                or parent_report.stat().st_size > 128*1024
                or status.get('parent_report_sha256') != hashlib.sha256(parent_report.read_bytes()).hexdigest()):
            state, active = 'blocked', False
    except OSError:
        state, active = 'blocked', False
    progress = status.get('progress') if isinstance(status.get('progress'), dict) else {}
    value = progress if active else {}
    phases = {'indexing_existing_metadata':'futures_725_indexing',
              'rechecking_committed_metadata':'futures_725_rechecking',
              'sealing_index_sha':'futures_725_sealing'}
    reason = ('source_or_copy_recheck_required' if report and not proof or state == 'blocked'
              else 'tw_day_trade_priority_window' if state == 'waiting'
                   and progress.get('reason_code') == 'tw_day_trade_priority_window'
              else 'staging_queued')
    row = task(kind, scope.name,
        state='completed' if proof else 'blocked' if report or state == 'blocked' else 'running' if active else 'waiting',
        phase='futures_725_projected' if proof else phases.get(state, 'working'),
        observed=clock(report) if proof else clock(status),
        generation=(scope.name, state, status.get('parent_report_sha256'), status.get('started_at_epoch')),
        done_items=number(report.get('entries')) if proof else number(value.get('completed_items')),
        total_items=number(report.get('entries')) if proof else number(value.get('total_items')),
        done_bytes=number(value.get('completed_bytes')), total_bytes=number(value.get('total_bytes')),
        proof=proof, reason=None if proof or active else reason)
    row['subject'] = 'tw-futures-universe765-workspace' if is_765 else 'tw-futures-universe725-workspace'
    return [row]


def futures_725_projection_tasks(repo: Path, workspace: Path) -> list[dict]:
    return futures_projection_tasks(repo, workspace)


def futures_765_projection_tasks(repo: Path, workspace: Path) -> list[dict]:
    from scripts.project_futures_725_workspace_index import FUTURES_765
    return futures_projection_tasks(repo, workspace, FUTURES_765)


def workspace_cold_inventory_tasks(repo: Path, workspace: Path) -> list[dict]:
    """Expose only a comparison bound to the currently observed D head."""
    from scripts import check_futures_e_cold_inventory as worker, run_workspace_source_staging as queue
    if repo != queue.ROOT:
        raise ValueError('cold inventory observer repository differs')
    state = metadata(workspace / 'migration/futures-e-cold-inventory-status.json')
    receipt = metadata(workspace / 'verify/futures-e-cold-inventory-comparison-v1.json')
    historical = proof = False
    current = copy = None
    head_unavailable = False
    try:
        matches = [item for item in queue.load_sources() if item['name'] == worker.NAME]
        if len(matches) != 1:
            raise ValueError('futures comparison enrollment differs')
        _, _, _, report_path = queue.paths(matches[0], workspace)
        copy = queue.verified_report(matches[0], workspace)
        if copy is not None and report_path.stat().st_size <= 128 * 1024 and not report_path.is_symlink():
            historical = worker.valid_result(receipt, copy, hashlib.sha256(report_path.read_bytes()).hexdigest())
            try:
                current = worker.current_head_identity()
                proof = (historical and worker.matches_current(receipt, current)
                         and worker.links_compared(receipt, copy))
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                head_unavailable = True
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        state = {'state': 'blocked'}
    bound_state = state.get('state') if state.get('contract') == worker.CONTRACT else None
    blocked = head_unavailable or bound_state == 'blocked' or bool(receipt) and not historical
    active = (bound_state == 'comparing' and not proof and not blocked and current is not None
              and worker.matches_current(state, current))
    changed = historical and current is not None and not worker.matches_current(receipt, current)
    links_pending = (historical and current is not None and not changed
                     and not worker.links_compared(receipt, copy))
    counts = receipt.get('counts', {}) if proof else {}
    metrics = {f'inventory_{kind}_files': counts.get(kind + '_files')
               for kind in ('exact', 'changed', 'e_only', 'd_only') if number(counts.get(kind + '_files')) is not None}
    metrics.update({f'inventory_{kind}_symlinks': counts.get(kind + '_symlinks')
                    for kind in ('exact', 'changed', 'e_only', 'd_only')
                    if number(counts.get(kind + '_symlinks')) is not None})
    progress = state.get('progress') if active and isinstance(state.get('progress'), dict) else {}
    scanned, total = number(progress.get('staged_files_scanned')), number(progress.get('staged_files_total'))
    if (scanned is None or total is None or scanned > total or not isinstance(copy, dict)
            or total != copy.get('files')):
        scanned = total = None
    else:
        # Both ordered streams consume entries: D may scan a long unrelated
        # prefix before the first E member. Count both so an early ETA does not
        # extrapolate from a misleading one-file E numerator.
        cold_scanned = number(progress.get('cold_entries_scanned'))
        cold_total = number(current.get('D_entries_total')) if current else None
        if cold_scanned is not None and cold_total is not None and cold_scanned <= cold_total:
            scanned, total = scanned+cold_scanned, total+cold_total
    tail = active and total is not None and scanned == total
    reason = (None if proof or active else 'futures_D_head_unavailable' if head_unavailable
              else 'source_or_copy_recheck_required' if blocked
              else 'tw_day_trade_priority_window' if bound_state == 'waiting_trading'
              else 'futures_D_head_changed' if changed or bound_state == 'waiting_head_changed'
              else 'futures_symlink_comparison_pending' if links_pending
              else 'staging_queued')
    row = task('workspace_cold_inventory', worker.NAME,
               state='completed' if proof else 'blocked' if blocked else 'running' if active else 'waiting',
               phase='workspace_cold_compared' if proof else 'workspace_cold_compare_tail' if tail else
                     'workspace_cold_compare' if active else 'workspace_cold_wait',
               observed=clock(receipt) if proof else clock(state) if active or changed else None,
               generation=(worker.NAME, current.get('D_manifest_sha256') if current else None,
                           receipt.get('source_copy_report_sha256')),
               done_items=scanned, total_items=total, proof=proof, reason=reason,
               metrics=metrics)
    row['subject'] = 'tw-futures-prepared'
    return [row]


def workspace_cold_comparison_snapshot_tasks(repo: Path, workspace: Path) -> list[dict]:
    """Expose only this fixed, read-only Binance inventory comparison."""
    from scripts import run_workspace_source_staging as queue
    if repo != queue.ROOT:
        raise ValueError('cold comparison observer repository differs')
    value = metadata(workspace / 'verify/binance-e-cold-inventory-20261106-evening.json', limit=128 * 1024)
    if (value.get('contract') != 'stockagent.e_stage_d_inventory_comparison.v2'
            or clock(value) is None or value.get('D_independent_restore_verified') is not False
            or value.get('NAS_acceptance') is not False or value.get('retirement_authorized') is not False
            or type(value.get('source_writes')) is not int or value['source_writes'] != 0
            or type(value.get('source_deletes')) is not int or value['source_deletes'] != 0):
        return []
    names = {'binance-market-e-primary-20261104-v1': ('binance-market', 'binance'),
             'binance-archive-e-primary-20261104-v1': ('binance-public-archive', 'binance-public-archive')}
    results = value.get('sources')
    if (not isinstance(results, list) or len(results) != len(names)
            or any(not isinstance(item, dict) for item in results)
            or any(not isinstance(item.get('name'), str) for item in results)
            or {item.get('name') for item in results} != set(names)):
        return []
    entries = {item['name']: item for item in queue.load_sources() if item['name'] in names}
    if set(entries) != set(names):
        return []
    rows = []
    for item in results:
        name = item['name']
        subject, dataset = names[name]
        counts = item.get('counts')
        count_keys = ('exact_files', 'changed_files', 'e_only_files', 'd_only_files',
                      'exact_bytes', 'changed_bytes', 'e_only_bytes')
        try:
            copy = queue.verified_report(entries[name], workspace)
        except (OSError, ValueError, TypeError, KeyError):
            copy = None
        if (item.get('state') != 'inventory_compared' or item.get('dataset') != dataset
                or item.get('D_recovery_verified') is not False or item.get('retirement_authorized') is not False
                or type(item.get('source_files_deleted')) is not int or item['source_files_deleted'] != 0
                or not isinstance(item.get('D_snapshot_id'), str) or not item['D_snapshot_id']
                or re.fullmatch(r'[0-9a-f]{64}', str(item.get('D_inventory_sha256', ''))) is None
                or not isinstance(counts, dict) or any(number(counts.get(key)) is None for key in count_keys)
                or not isinstance(copy, dict) or item.get('staged_files') != copy.get('files')
                or item.get('staged_bytes') != copy.get('logical_bytes')
                or item.get('staged_proof_at_epoch') != copy.get('observed_at_epoch')
                or sum(counts[key] for key in ('exact_files', 'changed_files', 'e_only_files')) != copy['files']
                or sum(counts[key] for key in ('exact_bytes', 'changed_bytes', 'e_only_bytes')) != copy['logical_bytes']):
            continue
        metrics = {f'inventory_{key}': counts[key] for key in count_keys}
        row = task('workspace_cold_inventory_snapshot', name, state='completed',
                   phase='workspace_cold_compared', observed=clock(value),
                   generation=(name, item['D_snapshot_id'], item['D_inventory_sha256'], clock(value)),
                   proof=True, metrics=metrics)
        row['subject'] = subject
        rows.append(row)
    return rows


def minute_validation_cache_tasks(repo: Path) -> list[dict]:
    """Project the exact cache-rebind status, without scanning source files."""
    path = repo/'artifacts/operations/source_validation/tw-minute-source-cold/device-rebind-status.json'
    value = metadata(path, limit=64 * 1024)
    state = value.get('state')
    generation = value.get('generation')
    count = number(value.get('cumulative_rebound'))
    raced = number(value.get('raced'))
    if (value.get('contract') != 'stockagent.minute_validation_device_rebind.v1'
            or state not in {'completed', 'time_budget', 'busy', 'waiting'}
            or state == 'waiting' and value.get('reason') != 'tw_day_trade_priority_window'
            or not isinstance(generation, str) or re.fullmatch(r'[0-9a-f]{64}', generation) is None
            or value.get('payloads_read') is not False
            or type(value.get('source_files_deleted')) is not int or value.get('source_files_deleted') != 0
            or count is None or raced is None or clock(value) is None):
        return []
    done = state == 'completed'
    return [task('minute_validation_cache_rebind', 'shioaji-minute',
        state='completed' if done else 'waiting' if state == 'waiting' else 'running',
        phase='minute_validation_cache_rebound' if done else 'minute_validation_cache_rebinding',
        observed=clock(value), generation=generation, done_items=count, proof=done,
        reason='tw_day_trade_priority_window' if state == 'waiting' else None,
        metrics={'validation_cache_rebound': count, 'validation_cache_raced': raced})]


def _observe_guest_e_capacity(*, now=None) -> dict:
    """Sample the enrolled E block device; never infer host or service readiness."""
    from stockagent.data_sync import workspace_volume
    now = time.time() if now is None else now
    try:
        mounted = workspace_volume.check(physical=False)
        device = os.stat(mounted['mount_point']).st_dev
        size = Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}' / 'size'
        sectors = int(size.read_text().strip())
        if mounted.get('state') != 'mounted' or sectors <= 0:
            return {}
        fs = os.statvfs(mounted['mount_point'])
        filesystem_bytes = fs.f_blocks * fs.f_frsize
        if filesystem_bytes <= 0:
            return {}
        return {'filesystem_uuid': mounted['filesystem_uuid'], 'virtual_bytes': sectors * 512,
                'filesystem_bytes': filesystem_bytes, 'observed_at_epoch': now}
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, subprocess.SubprocessError):
        return {}


def windows_storage_tasks(preparation: Path, *, receipt_root=None, now=None, guest_e_observation=None) -> list[dict]:
    """Observe the pinned native owner; never stop WSL or run a Windows command."""
    now = time.time() if now is None else now
    plan = metadata(preparation, limit=64 * 1024)
    native = plan.get('native_verification', {})
    sources = plan.get('native_sources')
    source = sources.get('worker', {}) if isinstance(sources, dict) else {}
    source = source if isinstance(source, dict) else {}
    target = 3298534883328
    sha = source.get('sha256')
    stamp = clock(native) if isinstance(native, dict) else None
    prepared_at = stamp
    if (plan.get('contract') != 'stockagent.windows_c_compaction_e_3tib_preparation.v1'
            or plan.get('state') != 'native_semantic_tests_passed_apply_requires_administrator'
            or plan.get('data_target_bytes') != target or not isinstance(sha, str)
            or re.fullmatch(r'[0-9a-f]{64}', sha) is None
            or not isinstance(native, dict) or native.get('worker_sha256') != sha
            or native.get('disks_modified') is not False
            or stamp is None or stamp > now + 120):
        return []
    if receipt_root is None:
        parts = PureWindowsPath(str(source.get('native_path', ''))).parts
        if (len(parts) != 8 or parts[0].upper() != 'C:\\' or parts[1] != 'Users'
                or not parts[2] or parts[2] in {'.', '..'}
                or parts[3:7] != ('AppData', 'Local', 'StockAgent', 'WslStorage')
                or parts[7] != 'worker-' + sha + '.ps1'):
            return []
        receipt_root = Path('/mnt/c', *parts[1:7], 'receipts')
    value = metadata(Path(receipt_root) / 'latest-c-e-maintenance.json')
    data = value.get('data_expansion', {})
    when = clock(value)
    valid = (value.get('contract') == 'stockagent-wsl-storage-maintenance-v1'
             and value.get('distro') == 'Ubuntu-26.04' and value.get('mode') == 'Compact'
             and str(value.get('script_sha256', '')).lower() == sha
             and isinstance(data, dict) and data.get('distribution') == 'StockAgentData'
             and data.get('target_bytes') == target
             and data.get('expected_filesystem_uuid') == plan.get('data_uuid')
             and when is not None and stamp <= when <= now + 120)
    latest = value if valid else None
    if latest:
        stamp = when
    accepted = bool(latest and latest.get('state') == 'compacted-and-boot-verified'
                    and latest.get('errors') == [] and isinstance(latest.get('recovery'), str)
                    and latest['recovery'])
    data = latest.get('data_expansion', {}) if latest else {}
    data_after = data.get('after')
    phase_stamp = clock(data)
    phase_clock_valid = ('observed_at_utc' not in data or
                         (phase_stamp is not None and prepared_at <= phase_stamp <= stamp + 1
                          and phase_stamp <= now + 120))
    e_done = bool(latest and latest.get('state') in {'compacted-and-boot-verified', 'needs-attention'}
                  and data.get('state') == 'expanded_and_guest_filesystem_verified'
                  and isinstance(data_after, dict) and data_after.get('virtual_bytes') == target
                  and isinstance(data.get('readback'), str) and data['readback'] and phase_clock_valid)
    before, after = (latest.get('before', {}), latest.get('after', {})) if latest else ({}, {})
    before = before if isinstance(before, dict) else {}
    after = after if isinstance(after, dict) else {}
    compaction = latest.get('compaction_readback') if latest else None
    events = latest.get('events', []) if latest else []
    matching = [clock({'observed_at_utc': event.get('at_utc')}) for event in events
                if isinstance(event, dict) and event.get('name') == 'offline-compaction-verified-without-resize'] if isinstance(events, list) else []
    compaction_clock = next((item for item in reversed(matching)
                             if item is not None and prepared_at <= item <= stamp + 1 and item <= now + 120), None)
    independently_compacted = bool(latest and latest.get('state') in {'compacted-and-boot-verified', 'needs-attention'}
                                   and isinstance(compaction, dict) and compaction_clock is not None)
    measured_after = compaction if independently_compacted else after
    c_done = bool((accepted or independently_compacted)
                  and number(before.get('allocated_bytes')) is not None
                  and number(measured_after.get('allocated_bytes')) is not None
                  and number(before.get('virtual_bytes'))
                  and before.get('virtual_bytes') == measured_after.get('virtual_bytes'))
    active = bool(latest and latest.get('state') in {'admitted', 'running', 'offline-capacity-observed'})
    reason = ('windows_storage_failed' if latest else 'windows_storage_administrator')
    guest = guest_e_observation if isinstance(guest_e_observation, dict) else {}
    guest_at = clock(guest)
    filesystem_bytes = guest.get('filesystem_bytes')
    guest_capacity = bool(guest.get('filesystem_uuid') == plan.get('data_uuid')
                          and isinstance(plan.get('data_uuid'), str)
                          and guest.get('virtual_bytes') == target
                          and type(filesystem_bytes) is int
                          and target * 95 // 100 <= filesystem_bytes <= target
                          and guest_at is not None and 0 <= now - guest_at <= STALE_SECONDS)
    rows = []
    for kind, receipt_done in (('workspace_e_resize', e_done), ('workspace_c_compaction', c_done)):
        current_guest = kind == 'workspace_e_resize' and guest_capacity
        # A current paired block device and mounted filesystem establish the
        # requested capacity, independently of whether an older maintenance
        # receipt proves the separate Windows operation and service recovery.
        current_only = current_guest and not receipt_done and not active
        done = receipt_done or current_only
        metrics = ({'reclaimed_allocated_bytes': max(0, before['allocated_bytes'] - measured_after['allocated_bytes'])}
                   if c_done and kind == 'workspace_c_compaction' else {})
        if current_guest:
            metrics.update(guest_e_capacity_bytes=target, guest_e_filesystem_capacity_bytes=filesystem_bytes,
                           guest_e_capacity_observed_epoch=int(guest_at))
        rows.append(task(kind, sha, state='completed' if done else 'running' if active else 'blocked',
            phase=('windows_storage_capacity_observed' if current_only else 'windows_storage_verified' if done
                   else 'windows_storage_running' if active
                   else 'windows_storage_failed' if latest else 'windows_storage_pending'),
            observed=(guest_at if current_only
                      else (phase_stamp or stamp) if kind == 'workspace_e_resize' and e_done
                      else (compaction_clock or stamp) if kind == 'workspace_c_compaction' and c_done else stamp),
            generation=sha, done_items=1 if done else None, total_items=1,
            proof=done, reason=None if done or active else reason,
            activity=guest_at if current_guest else None, metrics=metrics))
    return rows


def catalog_export_tasks(path: Path, progress_path: Path | None = None) -> list[dict]:
    """Project a read-only Temporal observation, retaining failure clocks."""
    value = metadata(path)
    if (value.get('contract') != 'stockagent.catalog_export_observation.v1'
            or value.get('read_only') is not True or clock(value) is None):
        return []
    activity = value.get('activity')
    if activity is None:
        # An absent pending activity is not an export or NAS success receipt.
        return []
    if (not isinstance(activity, dict) or value.get('phase') != 'exporting'
            or not isinstance(activity.get('id'), str) or not activity['id']
            or activity.get('state') not in {'scheduled', 'started', 'cancel_requested'}
            or type(activity.get('attempt')) is not int or activity['attempt'] < 1):
        return []
    failure = {'native_command_rejected': 'native_command_rejected',
               'other': 'catalog_export_other'}.get(activity.get('failure'))
    progress = metadata(progress_path) if progress_path is not None else {}
    phases = {'catalog_copy', 'enumerate_transport', 'hash_transport_native', 'hash_transport'}
    bound = (activity['state'] == 'started'
             and isinstance(value.get('workflow_run_id'), str) and bool(value['workflow_run_id'])
             and type(value.get('snapshot_id')) is int
             and progress.get('contract') == 'stockagent.catalog_export_progress.v1'
             and progress.get('workflow_run_id') == value['workflow_run_id']
             and progress.get('snapshot_id') == value['snapshot_id']
             and progress.get('activity_id') == activity['id']
             and type(progress.get('attempt')) is int and progress['attempt'] == activity['attempt']
             and progress.get('phase') in phases and clock(progress) is not None)
    row = task('catalog_export', 'canonical', state='running' if activity['state'] == 'started' else 'waiting',
               phase=progress['phase'] if bound else 'catalog_export',
               observed=clock(progress) if bound else clock(value), activity=clock(value),
               generation=(value.get('workflow_run_id'), value.get('snapshot_id'), activity['id'], activity['attempt']),
               done_items=progress.get('completed_items') if bound else None,
               total_items=progress.get('total_items') if bound else None,
               metrics={'attempt_number': activity['attempt']})
    row['previous_source_failure'] = failure
    row['failure_observed_at_utc'] = activity.get('failure_at_utc') if failure else None
    row['started_at_utc'] = activity.get('started_at_utc')
    return [row]


def _source_local_workspace_checked(workspace: Path) -> bool:
    from stockagent.data_sync import workspace_volume
    try:
        config = workspace_volume.configuration()
        return (Path(config['mount_point']) == workspace
                and workspace_volume.check(physical=False, c=config).get('state') == 'mounted')
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError):
        return False


def d_cold_e_handoff_tasks(workspace: Path, *, workspace_checked=False, now=None) -> list[dict]:
    """Sample only this wave's E file sizes; never read or decode payloads."""
    now = time.time() if now is None else now
    root = workspace/'migration/d-cold-object-staging-v1'
    value = metadata(root/'scheduler-status.json')
    if not value or value.get('contract') != 'stockagent.d_cold_e_staging.v1.scheduler':
        return []
    state = value.get('state')
    generation = value.get('plan_identity_sha256', '')
    if state == 'caught_up' and not generation and workspace_checked:
        # Older scheduler versions discarded the final wave identity on idle.
        # Recover it only from an existing completed journal; plans and member
        # identities are checked again below before computing any totals.
        journals = []
        for path in root.glob('*.handoff.json'):
            key = path.name.removesuffix('.handoff.json')
            record = metadata(path)
            completed = finite(record.get('completed_at_epoch'))
            if (re.fullmatch(r'[a-f0-9]{64}', key)
                    and record.get('state') == 'D_names_removed_E_copy_retained'
                    and record.get('plan_identity_sha256') == key and completed is not None):
                journals.append((completed, key))
        if journals:
            generation = max(journals)[1]
    total = number(value.get('bytes_total'))
    files_total = number(value.get('files_total'))
    done = number(value.get('bytes_written'))
    files_done = number(value.get('files_completed'))
    observed = (timestamp(value.get('last_checked_at_epoch')) if state == 'caught_up' else None) or clock(value)
    if state == 'copying' and workspace_checked and re.fullmatch(r'[a-f0-9]{64}', str(generation)):
        plan = metadata(root/(generation+'.plan.json'))
        items = plan.get('objects')
        plan_bytes = json.dumps({k: v for k, v in plan.items() if k != 'identity_sha256'},
                                sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
        if (plan.get('identity_sha256') == generation and hashlib.sha256(plan_bytes).hexdigest() == generation
                and isinstance(items, list)
                and len(items) <= 4096 and len(items) == files_total):
            from stockagent.data_sync.packed_backup import object_descriptor
            counted = finished = 0
            try:
                for item in items:
                    relative, size = item['relative'], number(item['bytes'])
                    if size is None or object_descriptor(relative) != item['sha256']:
                        raise ValueError('invalid wave member')
                    path = root/(generation+'.payload')/'cold'/relative
                    for candidate in (path, path.with_name(path.name+'.partial')):
                        if candidate.is_symlink():
                            raise ValueError('redirected wave member')
                        if candidate.exists():
                            if candidate.resolve(strict=True) != candidate or not candidate.is_file():
                                raise ValueError('invalid wave member')
                            count = candidate.stat().st_size
                            if count > size:
                                raise ValueError('oversized wave member')
                            counted += count
                            if candidate == path and count == size:
                                finished += 1
                            break
                done, files_done, observed = counted, finished, now
            except (OSError, ValueError, KeyError, TypeError, RuntimeError):
                pass  # Keep the original worker's last durable counter.
    phase = ('d_e_object_copy' if state == 'copying' else
             'd_e_object_handoff' if state in {'handoff', 'resuming_handoff', 'waiting_original_owner'}
             else 'd_e_next_wave')
    public_state = ('running' if state in {'copying', 'handoff', 'resuming_handoff'} else
                    'blocked' if state in {'blocked', 'interrupted_batches'} else 'waiting')
    reason = ('d_e_handoff_interrupted' if public_state == 'blocked' else
              'd_e_owner_busy' if state == 'waiting_original_owner' else None)
    if phase == 'd_e_object_handoff':
        journal = metadata(root/(str(generation)+'.handoff.json'))
        removed = journal.get('removed') if isinstance(journal.get('removed'), list) else []
        files_done = len(removed)
        done = sum(number(row.get('bytes')) or 0 for row in removed if isinstance(row, dict))
    if not workspace_checked:
        public_state, reason = 'blocked', 'd_e_handoff_interrupted'
        done = files_done = None
    rows = [task('d_cold_e_handoff', 'unreferenced-objects', state=public_state, phase=phase,
                 observed=observed, generation=str(generation), done_bytes=done, total_bytes=total,
                 done_items=files_done, total_items=files_total, reason=reason)]
    if workspace_checked and re.fullmatch(r'[a-f0-9]{64}', str(generation)):
        current = metadata(root/(generation+'.plan.json'))
        remaining_bytes = number(current.get('remaining_bytes_before_wave'))
        remaining_files = number(current.get('remaining_files_before_wave'))
        if remaining_bytes is not None and remaining_files is not None:
            try:
                prior_bytes = prior_files = removed_bytes = removed_files = 0
                finished_waves = []
                seen = set()
                plans = list(root.glob('*.plan.json'))
                if len(plans) > 1024:
                    raise ValueError('too many wave plans')
                for path in plans:
                    key = path.name.removesuffix('.plan.json')
                    if not re.fullmatch(r'[a-f0-9]{64}', key) or not (root/(key+'.payload')).is_dir():
                        continue
                    plan = current if key == generation else metadata(path)
                    raw = json.dumps({k: v for k, v in plan.items() if k != 'identity_sha256'},
                                     sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
                    members = plan.get('objects')
                    if (plan.get('identity_sha256') != key or hashlib.sha256(raw).hexdigest() != key
                            or not isinstance(members, list) or len(members) > 4096):
                        raise ValueError('invalid wave plan')
                    for item in members:
                        relative, size = item.get('relative'), number(item.get('bytes'))
                        if not isinstance(relative, str) or size is None or relative in seen:
                            raise ValueError('overlapping or invalid wave')
                        seen.add(relative)
                        if key != generation:
                            prior_files += 1
                            prior_bytes += size
                    journal = metadata(root/(key+'.handoff.json'))
                    completed = journal.get('removed', [])
                    if (not isinstance(completed, list) or len(completed) > len(members)
                            or (completed and (journal.get('contract') != 'stockagent.d_cold_to_e_handoff.v1'
                                               or journal.get('plan_identity_sha256') != key))):
                        raise ValueError('invalid handoff progress')
                    expected = {item['relative']: item['bytes'] for item in members}
                    removed_names = set()
                    for item in completed:
                        if (not isinstance(item, dict) or item.get('relative') in removed_names
                                or expected.get(item.get('relative')) != item.get('bytes')):
                            raise ValueError('invalid handoff member')
                        removed_names.add(item['relative'])
                        removed_files += 1
                        removed_bytes += item['bytes']
                    if (journal.get('state') == 'D_names_removed_E_copy_retained'
                            and len(completed) == len(members) and finite(journal.get('completed_at_epoch')) is not None):
                        finished_waves.append((float(journal['completed_at_epoch']), sum(item['bytes'] for item in completed)))
                all_bytes, all_files = prior_bytes + remaining_bytes, prior_files + remaining_files
                if removed_bytes > all_bytes or removed_files > all_files:
                    raise ValueError('handoff counter exceeds cohort')
                cohort_complete = (state == 'caught_up' and all_bytes == removed_bytes
                                   and all_files == removed_files and all_files > 0)
                overall = task('d_cold_e_total', 'unreferenced-cohort',
                    state='completed' if cohort_complete else public_state,
                    phase='d_e_object_handoff', observed=now, generation=str(current.get('catalog_identity_sha256')),
                    done_bytes=removed_bytes, total_bytes=all_bytes, done_items=removed_files,
                    total_items=all_files, reason=reason, proof=cohort_complete)
                finished_waves.sort()
                recent = finished_waves[-4:]
                intervals = [(b[0]-a[0], b[1]) for a, b in zip(recent, recent[1:])
                             if 10 <= b[0]-a[0] <= 86400 and b[1] > 0]
                if len(intervals) >= 2 and state not in {'waiting_original_owner', 'blocked', 'interrupted_batches'}:
                    rate = sum(size for _, size in intervals)/sum(seconds for seconds, _ in intervals)
                    completed_at_last_wave = sum(size for _, size in finished_waves)
                    remaining = max(0, all_bytes-completed_at_last_wave)
                    finish = recent[-1][0] + remaining/rate
                    rates = [size/seconds for seconds, size in intervals]
                    if finish > now and remaining:
                        overall['_completed_wave_forecast'] = {
                            'state': 'estimated', 'reason': None, 'unit': 'bytes',
                            'scope': 'current_phase_only', 'remaining_seconds': math.ceil(finish-now),
                            'completion_at_utc': iso(finish), 'rate_per_second': round(rate, 4),
                            'fastest_seconds': math.ceil(max(0, recent[-1][0]+remaining/max(rates)-now)),
                            'slowest_seconds': math.ceil(max(0, recent[-1][0]+remaining/min(rates)-now))}
                    elif remaining:
                        overall['_completed_wave_forecast'] = {'state': 'stalled', 'reason': 'forecast_overdue',
                            'remaining_seconds': None, 'completion_at_utc': None, 'rate_per_second': None,
                            'fastest_seconds': None, 'slowest_seconds': None, 'unit': 'bytes'}
                rows.append(overall)
            except (OSError, ValueError, KeyError, TypeError):
                pass  # Do not invent an overall denominator.
    return rows


def _stream_install_observation(workspace: Path, name: str, expected: str):
    """Observe the install journal when a running older worker lacks phase reports."""
    if (source_subject(name) is None or not isinstance(expected, str)
            or re.fullmatch(r'[a-f0-9]{64}', expected) is None):
        return None
    stage = workspace/'migration/stream-source-repack-v1'/name/expected
    path = stage/'install.json'
    if stage.is_symlink() or path.is_symlink() or (stage/'prepared.json').is_symlink():
        return None
    try:
        before = path.stat()
        install, prepared = metadata(path), metadata(stage/'prepared.json')
        after = path.stat()
    except OSError:
        return None
    if (before.st_ino != after.st_ino or before.st_mtime_ns != after.st_mtime_ns
            or install.get('contract') != 'stockagent.stream_source_local_repack.v1.install'
            or install.get('dataset') != name or install.get('old_manifest_sha256') != expected
            or prepared.get('contract') != 'stockagent.stream_source_local_repack.v1.prepared'
            or prepared.get('dataset') != name or prepared.get('old_manifest_sha256') != expected
            or install.get('prepared_sha256') != prepared.get('identity_sha256')
            or install.get('state') not in {'installing', 'head_pending'}
            or not isinstance(install.get('installed'), list) or not isinstance(prepared.get('packages'), list)
            or not isinstance(prepared.get('inventory'), dict)):
        return None
    items = [*prepared['packages'], prepared['inventory']]
    if not all(isinstance(row, dict) and re.fullmatch(r'[a-f0-9]{64}', str(row.get('sha256', '')))
               and number(row.get('bytes')) is not None for row in items):
        return None
    sizes = {row['sha256']: row['bytes'] for row in items}
    done = [row.get('sha256') for row in install['installed'] if isinstance(row, dict)]
    if len(sizes) != len(items) or len(set(done)) != len(done) or not set(done).issubset(sizes):
        return None
    return {'observed': after.st_mtime, 'done_bytes': sum(sizes[sha] for sha in done),
            'total_bytes': sum(sizes.values()), 'done_items': len(done), 'total_items': len(items),
            'phase': 'source_local_D_committing' if install['state'] == 'head_pending' else 'source_local_D_installing'}


def _retired_lab203_install_observation(workspace: Path, cold: Path, name: str, expected: str) -> dict | None:
    """Observe the fixed former head and capacity; never grant installation."""
    try:
        from scripts import run_stream_packed_repack as cli
        from stockagent.data_sync import stream_repack
        if name != cli._RETIRED_LAB203_DATASET or expected != cli._RETIRED_LAB203_MANIFEST:
            return None
        stage = workspace/'migration/stream-source-repack-v1'/name/expected
        if stage.is_symlink() or stage.resolve(strict=True) != stage:
            return None
        prepared = stream_repack._read_prepared(stage, name, expected)
        if not cli._retired_lab203_successor_ready(cold, name, expected, 'penguin', prepared):
            return None
        path = stage/'install.json'
        if path.is_symlink():
            return None
        journal = json.loads(path.read_text()) if path.is_file() else {}
        remaining = cli.remaining_install_bytes(cold, prepared, journal)
        reserve = cli.install_reserve_bytes(name, expected, private=False)
        space = os.statvfs(cold)
        available = space.f_bavail*space.f_frsize
        shortage = max(0, remaining+reserve-available)
        return {'state': 'waiting_capacity' if shortage else 'waiting',
                'encoded_bytes_remaining': remaining, 'D_free_bytes': available,
                'D_reserve_bytes': reserve, 'D_shortfall_bytes': shortage}
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        return None


def private_subset_reuse_tasks(workspace: Path, *, workspace_checked=False,
                               cold=Path('/srv/stockagent-packed/.local-state/private-history')) -> list[dict]:
    """Project fixed private-pair metadata, never its paths or payload."""
    if not workspace_checked:
        return []
    from stockagent.data_sync import packed_snapshots, private_subset_reuse as subset, repack_retirement, stream_repack
    value = metadata(workspace/'migration/source-local-exact-repack/private-subset-status.json')
    if (value.get('contract') != 'stockagent.private_same_root_subset_queue.v1'
            or value.get('independent_D_readback') is not False or value.get('NAS_accepted') is not False
            or not isinstance(value.get('entries'), list) or len(value['entries']) > len(subset.REVIEWED_PAIRS)):
        return []
    reasons = {'pending_turn':'subset_pending_turn', 'original_owner_busy':'subset_owner_busy',
        'waiting_lifecycle':'subset_lifecycle_wait', 'waiting_lifecycle_refresh':'subset_lifecycle_wait',
        'donor_generation_unproven':'subset_donor_unproven', 'capacity_or_volume':'subset_capacity_wait',
        'target_or_donor_changed':'subset_source_mismatch', 'comparison_or_source_error':'subset_error',
        'donor_not_covering_target':'subset_not_covered'}
    phases = {'checking_target_inventory':'private_subset_checking_target',
        'checking_donor_inventory':'private_subset_checking_donor',
        'comparing_fixed_inventories':'private_subset_comparing',
        'installing_D_objects':'source_local_D_installing', 'streaming_D_object':'source_local_D_installing',
        'validating_D_inventory':'source_local_D_validating', 'committing_D_head':'source_local_D_committing'}
    rows, seen = [], set()
    for item in value['entries']:
        if not isinstance(item, dict):
            continue
        name, state = item.get('dataset'), item.get('state')
        if name not in subset.REVIEWED_PAIRS or name in seen or state not in {'waiting','running','blocked','published'}:
            continue
        seen.add(name)
        # The lifecycle owner can hold its lock across several datasets. A
        # busy-queue observation must not erase a previously committed result:
        # project only a receipt for the *current* head, then apply the same
        # replacement proof as for a published queue entry below.
        if state == 'waiting' and item.get('reason') == 'original_owner_busy':
            try:
                current = packed_snapshots.resolve_latest_packed(cold, name, require_objects=False)
                recorded = metadata(workspace/'migration/source-local-exact-repack'/name/
                                    (current.manifest_sha256+'.json'))
                if (recorded.get('source_mode') == subset.SOURCE_MODE
                        and recorded.get('new_manifest_sha256') == current.manifest_sha256):
                    item = dict(item, state='published', generation=recorded.get('old_manifest_sha256'),
                                new_manifest_sha256=current.manifest_sha256,
                                logical_bytes=recorded.get('logical_bytes'))
                    state = 'published'
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                pass
        generation, new_sha = item.get('generation'), item.get('new_manifest_sha256')
        valid_generation = isinstance(generation, str) and re.fullmatch(r'[a-f0-9]{64}', generation) is not None
        if state == 'published':
            try:
                if not valid_generation or not isinstance(new_sha, str) or re.fullmatch(r'[a-f0-9]{64}', new_sha) is None:
                    raise ValueError('missing publication identity')
                receipt = metadata(workspace/'migration/source-local-exact-repack'/name/(new_sha+'.json'))
                if (receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
                        or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
                        or receipt.get('dataset') != name or receipt.get('private') is not True
                        or receipt.get('old_manifest_sha256') != generation or receipt.get('new_manifest_sha256') != new_sha
                        or receipt.get('source_mode') != subset.SOURCE_MODE
                        or receipt.get('source_inventory_sha_compared') is not True
                        or receipt.get('source_member_sha_streamed') is not False):
                    raise ValueError('publication receipt differs')
                stage = workspace/'migration/stream-source-repack-v1'/name/generation
                prepared = stream_repack._prepared_metadata(stage, name, generation)
                repack_retirement.check_replacement(cold, stage, name, generation, new_sha,
                                                    receipt['new_snapshot_id'], prepared)
                current = packed_snapshots.resolve_latest_packed(cold, name, require_objects=False)
                if current.manifest_sha256 != new_sha:
                    # A subsequent publication is legitimate, but the older
                    # receipt is not evidence about that new current head.
                    state = 'waiting'
                    reason = 'subset_pending_turn'
                else:
                    reason = None
            except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                state = 'blocked'
                reason = 'subset_source_mismatch'
        else:
            reason = None if state == 'running' else reasons.get(item.get('reason'))
        progress = item.get('progress') if isinstance(item.get('progress'), dict) else {}
        phase = ('private_subset_published' if state == 'published' else
                 phases.get(progress.get('stage'), 'private_subset_preparing'))
        if progress.get('stage') in {'installing_D_objects', 'streaming_D_object',
                                     'validating_D_inventory', 'committing_D_head'}:
            counters = _D_progress_counters(progress)
        else:
            counters = {'done_items':number(progress.get('items_completed')),
                        'total_items':number(progress.get('items_total'))}
        done, total = counters.get('done_items'), counters.get('total_items')
        if done is not None and total is not None and done > total:
            counters['done_items'] = counters['total_items'] = None
        row = task('private_subset_reuse', (name, 'same-root'),
            state='completed' if state == 'published' else state, phase=phase,
            observed=clock(value), generation=(str(generation)+':'+phase) if valid_generation else phase,
            **counters, proof=state == 'published', reason=reason,
            metrics={'source_logical_bytes':number(item.get('logical_bytes'))} if state == 'published' else None)
        row['subject'] = source_subject(name)
        rows.append(row)
    return rows


def source_local_repack_queue_tasks(workspace: Path, *, workspace_checked=False,
                                    cold=Path('/srv/stockagent-packed')) -> list[dict]:
    if not workspace_checked:
        return []
    root = workspace/'migration/source-local-exact-repack'
    status, fixed_progress = metadata(root/'queue-status.json'), metadata(root/'queue-progress.json')
    priority = metadata(root/'prepared-install-queue-status.json')
    priority_entries = priority.get('entries') if priority.get('contract') == 'stockagent.prepared_stream_install_queue.v1' else []
    priority_rows = {(item['dataset'], item['expected_manifest_sha256']): item
                     for item in priority_entries if isinstance(item, dict)
                     and isinstance(item.get('dataset'), str) and isinstance(item.get('expected_manifest_sha256'), str)} \
        if isinstance(priority_entries, list) and len(priority_entries) <= 1024 else {}
    base_priority_clock = clock(priority)
    base_priority_fresh = base_priority_clock is not None and -60 <= time.time()-base_priority_clock <= 180
    stream_progress = metadata(workspace/'migration/stream-source-repack-v1/queue-progress.json')
    if status.get('contract') != 'stockagent.source_local_repack_queue.v1.status':
        return []
    entries = status.get('entries')
    if not isinstance(entries, list) or len(entries) > 1024:
        return []
    # A newly enrolled dedup candidate may run while the source owner is
    # still processing another, long-running dataset. Project its fixed,
    # validated E-side identity now instead of waiting for the next queue scan.
    # This is observation only; it neither changes the queue nor authorizes D
    # publication or retirement.
    entries = list(entries)
    try:
        from scripts.run_source_local_repack_queue import prior_published
        from stockagent.data_sync import source_repack_bloat
        identities = {(entry.get('dataset'), entry.get('expected_manifest_sha256'))
                      for entry in entries if isinstance(entry, dict)}
        for planned in source_repack_bloat.auto_hold_rows(workspace):
            if len(entries) >= 1024:
                break
            name, expected = planned['dataset'], planned['expected_manifest_sha256']
            if (name, expected) in identities or prior_published(root, planned):
                continue
            dedup = metadata(workspace/'migration/source-local-dedup-candidate-v1'/(name+'.status.json'))
            if (dedup.get('contract') != 'stockagent.dedup_candidate_queue.v1'
                    or dedup.get('dataset') != name or dedup.get('expected_manifest_sha256') != expected
                    or dedup.get('publishable') is not False or dedup.get('retirement_authorized') is not False):
                continue
            entries.append({**planned, 'state': 'retrying', 'reason': 'source_duplicate_bloat'})
            identities.add((name, expected))
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        pass  # A missing or invalid observation must not block the dashboard.
    names, repeated = set(), set()
    for entry in entries:
        if isinstance(entry, dict) and isinstance(entry.get('dataset'), str):
            if entry['dataset'] in names:
                repeated.add(entry['dataset'])
            names.add(entry['dataset'])
    rows = []
    errors = status.get('private_discovery_errors', [])
    if isinstance(errors, list) and len(errors) <= 128:
        for error in errors:
            if not isinstance(error, dict):
                continue
            reason = choice(error.get('reason'), {'private_authority_unavailable',
                'private_head_unavailable', 'private_dependency_contract_unknown'})
            name = error.get('dataset')
            subject = source_subject(name) if name is not None else 'other-cold-dataset'
            if reason is None or subject is None:
                continue
            row = task('source_local_repack_queue', (name, 'private-discovery'), state='blocked',
                       phase='source_local_repack_waiting', observed=clock(status), reason=reason)
            row['subject'] = subject
            row['next_attempt_at_utc'] = iso(timestamp(status.get('private_discovery_next_scan_epoch')))
            rows.append(row)
    for item in entries:
        if not isinstance(item, dict) or source_subject(item.get('dataset')) is None:
            continue
        name, state = item['dataset'], item.get('state')
        if state not in {'published', 'running', 'waiting', 'retrying', 'blocked_mismatch'}:
            continue
        observed = clock(status)
        is_cold = item.get('origin') in ('cold', 'private_cold')
        priority_clock, priority_fresh = base_priority_clock, base_priority_fresh
        priority_item = priority_rows.get((name, item.get('expected_manifest_sha256')), {})
        if (state == 'retrying' and item.get('reason') == 'repack_foreign_head_wait'
                and item.get('origin') == 'cold' and item.get('E_prepared') is True
                and not (priority_fresh and priority_item.get('state') in {'running', 'running_relief'})):
            observed_ready = _retired_lab203_install_observation(
                workspace, Path(cold), name, item.get('expected_manifest_sha256'))
            if observed_ready is not None:
                priority_item, priority_clock, priority_fresh = observed_ready, time.time(), True
        priority_live = priority_fresh and priority_item.get('state') == 'running'
        relief_live = priority_fresh and priority_item.get('state') == 'running_relief'
        priority_capacity = (priority_fresh and state in {'waiting', 'retrying'}
                             and priority_item.get('state') in {'waiting_capacity', 'waiting_relief',
                                                               'waiting_relief_head_owner'}
                             or priority_fresh and state in {'waiting', 'retrying'}
                             and priority_item.get('state') == 'waiting_head_transition'
                             and number(priority_item.get('D_shortfall_bytes')) not in (None, 0))
        priority_ready = (priority_fresh and state in {'waiting', 'retrying'} and is_cold
                          and priority_item.get('state') == 'waiting'
                          and number(priority_item.get('encoded_bytes_remaining')) is not None
                          and number(priority_item.get('D_free_bytes')) is not None
                          and number(priority_item.get('D_reserve_bytes')) is not None
                          and priority_item.get('D_shortfall_bytes') == 0)
        priority_owner_wait = (priority_fresh and state in {'waiting', 'retrying'} and is_cold
                               and priority_item.get('state') == 'retrying'
                               and priority_item.get('error') in {'owner_busy', 'retirement_turn'})
        priority_exclusive_wait = (priority_fresh and state in {'waiting', 'retrying'} and is_cold
                                   and priority_item.get('state') == 'retrying'
                                   and priority_item.get('error') == 'insufficient_exclusive_space')
        if (priority_fresh and state in {'waiting', 'retrying'} and is_cold
                and priority_item.get('state') == 'waiting_dedup'
                and priority_item.get('reason') == 'source_duplicate_bloat'):
            item = {**item, 'reason': 'source_duplicate_bloat'}
        priority_foreign_head = (priority_fresh and state in {'waiting', 'retrying'}
                                 and (priority_item.get('state') in {'waiting_head_transition', 'waiting_relief_head_owner'}
                                      or priority_item.get('state') == 'retrying'
                                      and priority_item.get('error') == 'foreign_head_wait'))
        relief_status, relief_clock = {}, None
        expected = item.get('expected_manifest_sha256')
        if is_cold and isinstance(expected, str) and re.fullmatch(r'[a-f0-9]{64}', expected):
            relief_path = workspace/'migration/stream-source-repack-v1'/name/expected/'current-head-capacity-relief.json'
            candidate = metadata(relief_path)
            if (candidate.get('contract') == 'stockagent.staged_current_head_capacity_relief.v1'
                    and candidate.get('dataset') == name and candidate.get('old_manifest_sha256') == expected
                    and candidate.get('state') in {'planned', 'removing', 'completed'}):
                relief_status = candidate
                try:
                    relief_clock = relief_path.stat().st_mtime
                except OSError:
                    pass
        progress = stream_progress if is_cold else fixed_progress
        counters = {'total_bytes': number(item.get('logical_bytes')),
                    'total_items': number(item.get('files'))}
        phase = ('source_local_D_published' if state == 'published' else
                 'source_local_repack_working' if state == 'running' else 'source_local_repack_waiting')
        if state in {'retrying', 'blocked_mismatch'} and item.get('E_prepared') is True:
            phase = ('source_local_E_prepared' if item.get('reason') == 'repack_D_capacity'
                     else 'source_local_E_prepared_ready')
            counters = {'done_bytes': number(item.get('prepared_logical_bytes')),
                        'total_bytes': number(item.get('logical_bytes')),
                        'done_items': number(item.get('prepared_files')),
                        'total_items': number(item.get('files'))}
        running_progress = progress.get('progress')
        expected_contract = ('stockagent.stream_source_local_repack.v1.progress' if is_cold
                             else 'stockagent.source_local_repack_progress.v1')
        if (state == 'running' and progress.get('contract') == expected_contract
                and progress.get('dataset') == name
                and progress.get('expected_manifest_sha256') == item.get('expected_manifest_sha256')
                and isinstance(running_progress, dict)):
            observed = clock(progress)
            counters = {'done_bytes': number(running_progress.get('logical_bytes_processed')),
                        'total_bytes': number(progress.get('logical_bytes_total') if is_cold
                                              else running_progress.get('logical_bytes_total')),
                        'done_items': number(running_progress.get('files_completed')),
                        'total_items': number(progress.get('files_total') if is_cold
                                              else running_progress.get('files_total'))}
            if is_cold and running_progress.get('stage') in {'installing_D_objects', 'streaming_D_object',
                                                              'validating_D_inventory', 'committing_D_head'}:
                phase = ('source_local_D_committing' if running_progress['stage'] == 'committing_D_head' else
                         'source_local_D_validating' if running_progress['stage'] == 'validating_D_inventory' else
                         'source_local_D_installing')
                counters = _D_progress_counters(running_progress)
            elif is_cold and running_progress.get('stage') in {'loading_source_inventory',
                    'validating_source_inventory', 'indexing_source_inventory', 'writing_E_inventory'}:
                phase = {'loading_source_inventory': 'source_local_source_loading',
                         'validating_source_inventory': 'source_local_source_validating',
                         'indexing_source_inventory': 'source_local_source_indexing',
                         'writing_E_inventory': 'source_local_E_inventory'}[running_progress['stage']]
                counters = {'done_items': number(running_progress.get('inventory_items_completed')),
                            'total_items': number(running_progress.get('inventory_items_total'))}
            elif is_cold and running_progress.get('stage') == 'streaming_source_into_E_packs':
                phase = 'source_local_E_packing'
                # An already-running old producer may not report its output
                # inventory phase, or its final source counter. The producer
                # creates this exact temporary name only after all inputs and
                # packs finish. Reject a leftover older than its last progress;
                # the inventory's own row count remains unknown.
                inventory_clock = _inflight_E_inventory_clock(workspace, name, item.get('expected_manifest_sha256'))
                if inventory_clock is not None and observed is not None and inventory_clock >= observed-1:
                    phase, counters = 'source_local_E_inventory', {}
                    observed = max(observed or 0, inventory_clock)
            elif is_cold and running_progress.get('stage') == 'retiring_prepared_history':
                phase, counters = 'source_local_prepared_history', {}
        if priority_live:
            state, phase = 'running', 'source_local_D_installing'
            selected_progress = metadata(workspace/'migration/stream-source-repack-v1'/('prepared-install-'+name+'.progress.json'))
            current = selected_progress.get('progress')
            if (selected_progress.get('contract') == 'stockagent.stream_source_local_repack.v1.progress'
                    and selected_progress.get('dataset') == name
                    and selected_progress.get('expected_manifest_sha256') == item.get('expected_manifest_sha256')
                    and isinstance(current, dict) and current.get('stage') in
                    {'installing_D_objects', 'streaming_D_object', 'validating_D_inventory', 'committing_D_head'}):
                observed = clock(selected_progress)
                phase = ('source_local_D_committing' if current['stage'] == 'committing_D_head' else
                         'source_local_D_validating' if current['stage'] == 'validating_D_inventory' else
                         'source_local_D_installing')
                counters = _D_progress_counters(current)
            else:
                counters = {'done_bytes': 0, 'total_bytes': number(priority_item.get('encoded_bytes_remaining'))}
        elif priority_capacity:
            # The independent owner can discover a sealed E generation even
            # when the older main queue only recorded a lock conflict.
            state = 'retrying'
            phase = ('source_local_E_local_ready' if priority_item.get('E_temporary_authority_ready') is True
                     else 'source_local_E_prepared')
            observed = priority_clock
            counters = {'done_bytes': number(item.get('prepared_logical_bytes', item.get('logical_bytes'))),
                        'total_bytes': number(item.get('logical_bytes')),
                        'done_items': number(item.get('prepared_files', item.get('files'))),
                        'total_items': number(item.get('files'))}
        elif priority_ready:
            # The independent owner has reassessed this exact sealed stage
            # after an older foreign-head/capacity wait. It is queued, not
            # blocked or already installed.
            state, phase, observed = 'waiting', 'source_local_E_prepared_ready', priority_clock
            counters = {'done_bytes': number(item.get('prepared_logical_bytes', item.get('logical_bytes'))),
                        'total_bytes': number(item.get('logical_bytes')),
                        'done_items': number(item.get('prepared_files', item.get('files'))),
                        'total_items': number(item.get('files'))}
        elif priority_owner_wait:
            state, phase, observed = 'retrying', 'source_local_E_prepared_ready', priority_clock
            item = {**item, 'reason': 'd_e_owner_busy'}
            counters = {'done_bytes': number(item.get('prepared_logical_bytes', item.get('logical_bytes'))),
                        'total_bytes': number(item.get('logical_bytes')),
                        'done_items': number(item.get('prepared_files', item.get('files'))),
                        'total_items': number(item.get('files'))}
        elif priority_exclusive_wait:
            state = 'retrying'
            phase = ('source_local_E_local_ready' if priority_item.get('E_temporary_authority_ready') is True
                     else 'source_local_E_prepared')
            observed = priority_clock
            counters = {'done_bytes': number(item.get('prepared_logical_bytes', item.get('logical_bytes'))),
                        'total_bytes': number(item.get('logical_bytes')),
                        'done_items': number(item.get('prepared_files', item.get('files'))),
                        'total_items': number(item.get('files'))}
        elif priority_fresh and priority_item.get('state') == 'completed':
            from scripts.run_source_local_repack_queue import prior_published
            if prior_published(root, item):
                state, phase, observed = 'published', 'source_local_D_published', priority_clock
        # The E-first transaction intentionally makes the old D payload
        # incomplete before the replacement head exists. Show that state,
        # including an interrupted owner, independently of the old queue's
        # last capacity observation.
        relief_interrupted = False
        relief_waiting_install = False
        if relief_live:
            state = 'running'
            phase = ('source_local_E_first_retiring' if relief_status
                     else 'source_local_E_first_preparing')
            observed = relief_clock if relief_clock is not None else priority_clock
            counters = {'done_items': number(relief_status.get('removed_count')),
                        'total_items': number(relief_status.get('planned_object_count'))}
        elif relief_status and relief_status.get('state') in {'planned', 'removing'} and state != 'published':
            state, phase, relief_interrupted = 'retrying', 'source_local_E_first_retiring', True
            observed = relief_clock
            counters = {'done_items': number(relief_status.get('removed_count')),
                        'total_items': number(relief_status.get('planned_object_count'))}
        elif (relief_status.get('state') == 'completed' and state not in {'running', 'published'}
              and not priority_live):
            state = 'retrying' if priority_capacity else 'waiting'
            phase, relief_waiting_install = 'source_local_E_first_install_waiting', not priority_capacity
            observed, counters = relief_clock, {}
        if state == 'running' and is_cold and not relief_live:
            install = _stream_install_observation(workspace, name, item.get('expected_manifest_sha256'))
            if install is not None and (phase == 'source_local_repack_working' or observed is None
                                        or install['observed'] > observed):
                observed, phase = install.pop('observed'), install.pop('phase')
                counters = install
        # A held generation may now be preparing an E-only dedup candidate
        # under the same dataset owner. It is not a D head or acceptance.
        dedup_live, dedup_clock, dedup_retry, dedup_fallbacks, dedup_E_reclaimed = False, None, None, None, None
        census_live = False
        if is_cold and isinstance(expected, str) and re.fullmatch(r'[a-f0-9]{64}', expected):
            census = metadata(workspace/'migration/source-local-exact-repack/dedup-plans'/
                              (name+'-'+expected+'.status.json'))
            if (census.get('contract') == 'stockagent.source_repack_auto_dedup_hold.v1.census-v1'
                    and census.get('dataset') == name and census.get('old_manifest_sha256') == expected
                    and census.get('source_payload_reread') is False
                    and census.get('D_head_changed') is False and census.get('retirement_authorized') is False
                    and census.get('state') == 'running'
                    and _process_identity_matches(census.get('owner_identity'))
                    and not priority_live and not relief_live):
                state, phase, observed, dedup_live = 'running', 'source_local_dedup_indexing', clock(census), True
                census_live, dedup_clock = True, observed
                event = census.get('progress') if isinstance(census.get('progress'), dict) else {}
                counters = {'done_items': number(event.get('files_completed')),
                            'total_items': number(event.get('files_total'))}
        if item.get('reason') == 'source_duplicate_bloat' and not priority_live and not priority_ready and not priority_owner_wait and not relief_live and not dedup_live:
            dedup = metadata(workspace/'migration/source-local-dedup-candidate-v1'/(name+'.status.json'))
            if (dedup.get('contract') == 'stockagent.dedup_candidate_queue.v1'
                    and dedup.get('dataset') == name and dedup.get('expected_manifest_sha256') == expected
                    and dedup.get('publishable') is False and dedup.get('retirement_authorized') is False):
                dedup_clock = clock(dedup)
                dedup_fallbacks = number(dedup.get('fallback_count'))
                dedup_E_reclaimed = number(dedup.get('legacy_E_reclaimed_allocated_bytes'))
                if (dedup.get('state') in {'running', 'validating_v3_metadata', 'pruning_legacy_E_checkpoint'}
                        and _process_identity_matches(dedup.get('owner_identity'))):
                    state, observed, dedup_live = 'running', dedup_clock, True
                    event = dedup.get('progress') if isinstance(dedup.get('progress'), dict) else {}
                    step = event.get('stage')
                    phase = {'loading_source_inventory': 'source_local_source_loading',
                             'validating_source_inventory': 'source_local_source_validating',
                             'checking_dedup_plan': 'source_local_dedup_indexing',
                             'streaming_source_into_E_packs': 'source_local_dedup_packing',
                             'selective_native_fallback': 'source_local_dedup_fallback',
                             'writing_candidate_inventory': 'source_local_dedup_inventory',
                             'compressing_candidate_inventory': 'source_local_dedup_compressing',
                             'validate-packed-inventory': 'source_local_dedup_v3_validating',
                             'pruning_legacy_E_checkpoint': 'source_local_dedup_legacy_cleanup'}.get(
                                 step, 'source_local_dedup_preparing')
                    if step == 'streaming_source_into_E_packs':
                        counters = {'done_bytes': number(event.get('logical_bytes_processed')),
                                    'total_bytes': number(dedup.get('unique_bytes')),
                                    'done_items': number(event.get('files_completed')),
                                    'total_items': number(dedup.get('unique_contents'))}
                    else:
                        counters = {'done_items': number(event.get('completed_items') if step in {'validate-packed-inventory', 'pruning_legacy_E_checkpoint'}
                                                         else event.get('items_completed')),
                                    'total_items': number(event.get('total_items') if step in {'validate-packed-inventory', 'pruning_legacy_E_checkpoint'}
                                                          else event.get('items_total'))}
                        if step == 'pruning_legacy_E_checkpoint':
                            dedup_E_reclaimed = number(event.get('reclaimed_allocated_bytes'))
                    if step == 'writing_candidate_inventory':
                        # Older workers stop reporting while gzip writes the
                        # already-mapped rows. A fresh exact partial file and
                        # a live owner prove the phase, not the row count.
                        inventory_clock = _inflight_E_inventory_clock(
                            workspace, name, expected, queue='source-local-dedup-candidate-v1')
                        if inventory_clock is not None and observed is not None and inventory_clock >= observed-1:
                            phase, counters = 'source_local_dedup_compressing', {}
                            observed = max(observed, inventory_clock)
                elif dedup.get('state') == 'sealed_waiting_reader':
                    state, phase, observed = 'retrying', 'source_local_dedup_sealed', dedup_clock
                    item = {**item, 'reason': 'dedup_candidate_reader_pending'}
                    counters = {'done_items': number(dedup.get('unique_contents')),
                                'total_items': number(dedup.get('unique_contents'))}
                elif (dedup.get('state') == 'sealed_waiting_local_adoption'
                      and dedup.get('origin') == 'private_cold' and item.get('origin') == 'private_cold'):
                    state, phase, observed = 'retrying', 'source_local_dedup_sealed', dedup_clock
                    item = {**item, 'reason': 'dedup_candidate_handoff_pending'}
                    counters = {'done_items': number(dedup.get('unique_contents')),
                                'total_items': number(dedup.get('unique_contents'))}
                elif dedup.get('state') in {'v3_metadata_prepared_waiting_handoff',
                                             'v3_handed_off_waiting_activation'}:
                    state, phase, observed = 'retrying', 'source_local_dedup_v3_ready', dedup_clock
                    item = {**item, 'reason': 'dedup_candidate_handoff_pending'}
                    counters = {'done_items': number(dedup.get('files')),
                                'total_items': number(dedup.get('files'))}
                elif (dedup.get('state') == 'v3_handed_off_local_install_ready'
                      and dedup.get('origin') == 'private_cold' and item.get('origin') == 'private_cold'):
                    state, phase, observed = 'retrying', 'source_local_E_prepared_ready', dedup_clock
                    item = {**item, 'reason': 'staging_queued'}
                    counters = {'done_items': number(dedup.get('files')),
                                'total_items': number(dedup.get('files'))}
                elif dedup.get('state') in {'running', 'waiting_retry', 'validating_v3_metadata', 'waiting_adoption_retry', 'pruning_legacy_E_checkpoint'}:
                    state, observed = 'retrying', dedup_clock
                    dedup_retry = timestamp(dedup.get('retry_after_epoch'))
                    reason_code = ('dedup_candidate_interrupted' if dedup.get('state') in {'running', 'validating_v3_metadata', 'pruning_legacy_E_checkpoint'}
                                   else 'd_e_owner_busy' if dedup.get('error_type') == 'PackedPublicationBusy'
                                   else 'dedup_candidate_error')
                    item = {**item, 'reason': reason_code}
        if priority_foreign_head:
            # The independent installer checked this same fixed generation
            # after the original source queue last reported a busy lock.
            phase = ('source_local_E_local_ready' if priority_item.get('E_temporary_authority_ready') is True
                     else 'source_local_E_prepared_ready')
            observed = priority_clock
        public_state = ('completed' if state == 'published' else 'running' if state == 'running'
                        else 'blocked' if relief_interrupted else
                        'blocked' if priority_foreign_head else
                        'blocked' if priority_exclusive_wait or state == 'blocked_mismatch' or
                        state == 'retrying' and (priority_capacity or item.get('reason') in
                                                 {'repack_D_capacity', 'repack_foreign_head_wait', 'source_duplicate_bloat',
                                                  'dedup_candidate_reader_pending', 'dedup_candidate_handoff_pending', 'dedup_candidate_error',
                                                  'dedup_candidate_interrupted'}) else 'waiting')
        reason = ('repack_E_first_interrupted' if relief_interrupted else
                  ('repack_E_first_owner_busy' if priority_fresh and priority_item.get('error') == 'owner_busy'
                   else 'repack_E_first_install_waiting') if relief_waiting_install else
                  'repack_retirement_turn' if priority_owner_wait and priority_item.get('error') == 'retirement_turn' else
                  'repack_lab203_head_wait' if priority_foreign_head and priority_item.get('relief_head_owner') == 'lab203' else
                  'repack_foreign_head_wait' if priority_foreign_head else
                  'repack_exclusive_space' if priority_exclusive_wait else
                  'repack_D_capacity' if priority_capacity else
                  item.get('reason') if state in {'blocked_mismatch', 'retrying'} else
                  'staging_queued' if state == 'waiting' else None)
        capacity_metrics = {key: number(item.get(key)) for key in ('D_required_bytes', 'D_observed_free_bytes',
                            'D_reserve_bytes', 'D_shortfall_bytes')}
        capacity_stamp = timestamp(item.get('D_capacity_observed_epoch'))
        capacity_metrics['D_capacity_observed_epoch'] = int(capacity_stamp) if capacity_stamp is not None else None
        if priority_capacity or priority_exclusive_wait:
            capacity_metrics = {'D_required_bytes': number(priority_item.get('encoded_bytes_remaining')),
                'D_observed_free_bytes': number(priority_item.get('D_free_bytes')),
                'D_reserve_bytes': number(priority_item.get('D_reserve_bytes')),
                'D_shortfall_bytes': number(priority_item.get('D_shortfall_bytes')),
                'D_capacity_observed_epoch': int(priority_clock)}
            if priority_exclusive_wait:
                for metric in ('D_exclusive_allocated_bytes', 'D_relief_shortfall_bytes', 'relief_observed_at_epoch'):
                    value = number(priority_item.get(metric))
                    if value is not None:
                        capacity_metrics[metric] = value
        if (priority_live or priority_ready or priority_owner_wait
                or priority_foreign_head and not priority_capacity or state == 'published'):
            # A prior admission failure no longer describes an active or
            # committed install; retaining it makes the UI report a false block.
            capacity_metrics = {}
        if dedup_fallbacks:
            capacity_metrics['dedup_native_fallbacks'] = dedup_fallbacks
        if dedup_E_reclaimed is not None:
            capacity_metrics['E_legacy_reclaimed_allocated_bytes'] = dedup_E_reclaimed
        if (state != 'published' and relief_status
                and number(relief_status.get('removed_count')) not in (None, 0)):
            # An E-first transaction has already removed old D objects. This
            # remains visible during the replacement's active D write as well
            # as while it is waiting; do not imply normal D reads are safe.
            capacity_metrics['D_old_head_temporarily_unavailable'] = 1
        identity = (name, item.get('expected_manifest_sha256')) if name in repeated else name
        row = task('source_local_repack_queue', identity, state=public_state,
                   phase=phase,
                   observed=observed, generation=str(item.get('expected_manifest_sha256', '')),
                   proof=state == 'published', reason=reason, metrics=capacity_metrics,
                   activity=priority_clock if priority_live or relief_live else dedup_clock if dedup_live
                       else clock(status) if state == 'running' else None,
                   **counters)
        if priority_live or relief_live or census_live:
            row['_priority_install_owner'] = True
        if dedup_live and not census_live:
            row['_dedup_candidate_owner'] = True
        row['subject'] = source_subject(name)
        row['next_attempt_at_utc'] = (None if state in {'running', 'published'} or priority_ready or
                                      item.get('reason') in {'source_duplicate_bloat', 'dedup_candidate_reader_pending',
                                                             'dedup_candidate_handoff_pending'}
                                      else iso(timestamp(priority_item.get('retry_after_epoch')) if priority_foreign_head or priority_owner_wait or priority_exclusive_wait
                                               else dedup_retry if dedup_retry is not None
                                               else timestamp(item.get('retry_after_epoch'))))
        if phase in {'source_local_E_prepared', 'source_local_E_prepared_ready', 'source_local_E_local_ready'} and is_cold:
            expected = item.get('expected_manifest_sha256')
            if isinstance(expected, str) and re.fullmatch(r'[a-f0-9]{64}', expected):
                stage = workspace/'migration/stream-source-repack-v1'/name/expected
                try:
                    from stockagent.data_sync import stream_repack
                    if stage.is_symlink() or stage.resolve(strict=True) != stage:
                        raise ValueError('prepared stage redirected')
                    prepared = stream_repack._prepared_metadata(stage, name, expected)
                    sizes = [number(member.get('bytes')) for member in
                             [*prepared['packages'], prepared['inventory']]]
                    if sizes and all(size is not None for size in sizes):
                        install_file = stage/'install.json'
                        install = (_stream_install_observation(workspace, name, expected)
                                   if install_file.exists() or install_file.is_symlink() else None)
                        if install is not None or not (install_file.exists() or install_file.is_symlink()):
                            row['_D_encoded_remaining_bytes'] = sum(sizes) - (install['done_bytes'] if install else 0)
                            from scripts.run_stream_packed_repack import install_reserve_bytes
                            row['_D_reserve_bytes'] = install_reserve_bytes(
                                name, expected, private=item.get('origin') == 'private_cold')
                except (OSError, ValueError, TypeError, KeyError, RuntimeError):
                    pass  # A progress observation must not invent an admission requirement.
        rows.append(row)
    return rows


def source_local_promotion_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    """Expose bounded publication receipts without rereading archive content."""
    if not workspace_checked:
        return []
    root = workspace/'migration/source-local-exact-repack'
    rows = []
    if not root.is_dir() or root.is_symlink():
        return rows
    subjects = sorted(root.iterdir(), key=lambda path: (path.name not in SUBJECTS, path.name))[:128]
    for directory in subjects:
        if source_subject(directory.name) is None or directory.is_symlink() or not directory.is_dir():
            continue
        receipts = sorted(directory.glob('*.json'))
        if len(receipts) > 256:
            continue
        valid = []
        for path in receipts:
            value = metadata(path)
            stamp = clock(value)
            if (not path.is_symlink() and re.fullmatch(r'[a-f0-9]{64}', path.stem)
                    and value.get('contract') == 'stockagent.source_local_exact_repack.v1'
                    and value.get('state') == 'new_D_head_committed_from_exact_E_source'
                    and value.get('dataset') == directory.name
                    and value.get('new_manifest_sha256') == path.stem
                    and value.get('pack_layout') == 'source-dataset-v1'
                    and number(value.get('files')) is not None
                    and number(value.get('logical_bytes')) is not None and stamp is not None):
                valid.append((stamp, path.stem, value))
        if valid:
            stamp, digest, value = max(valid, key=lambda row: (row[0], row[1]))
            files, size = value['files'], value['logical_bytes']
            row = task('source_local_promotion', directory.name, state='completed',
                       phase='source_local_D_published', observed=stamp, generation=digest,
                       done_items=files, total_items=files, done_bytes=size, total_bytes=size,
                       proof=True)
            row['subject'] = source_subject(directory.name)
            rows.append(row)
    return rows


def ordinary_archive_tasks(repo: Path, state_root: Path, *, now: float) -> list[dict]:
    """Read the original retirement owner's small receipt, never its payloads."""
    from stockagent.data_sync import archive_schedule
    settings = metadata(repo/'configs/columnar_storage.json').get('dataset_version_lifecycle', {})
    if settings.get('ordinary_history_retention') != 'current-and-research-pins':
        return []
    window = archive_schedule.validate_window(settings.get('daily_merge_window'))
    if window is None:
        return []
    value = metadata(state_root/'status.json', limit=4*1024**2)
    allowed = (archive_schedule.allows_start(now, window)
               or settings.get('ordinary_history_migration_catchup') is True)
    enrolled = value.get('contract') == 'stockagent.ordinary_archive_cycle.v1'
    completed = value.get('completed', {}) if enrolled else {}
    owners = value.get('owners', []) if enrolled else []
    totals = value.get('totals', {}) if enrolled else {}
    protected = sum(number(row.get('coverage_protected_versions')) or 0
                    for row in completed.values() if isinstance(row, dict))
    running = allowed and enrolled and value.get('state') == 'running'
    row = task('ordinary_archive', 'whole-source-roots',
        state='running' if running else 'waiting',
        phase='ordinary_archive_check' if allowed else 'ordinary_archive_wait',
        observed=clock(value) if enrolled else None, generation=value.get('day'),
        done_items=len(completed) if enrolled else None, total_items=len(owners) if enrolled else None,
        reason='ordinary_archive_window' if not allowed else 'ordinary_archive_protected' if protected
               else 'ordinary_archive_owner_busy' if str(value.get('reason','')).startswith('retirement owner busy:')
               else 'ordinary_archive_retry' if value.get('state') == 'waiting' else None,
        metrics={'ordinary_history_versions_removed':number(totals.get('versions_removed')),
                 'ordinary_history_reclaimed_bytes':number(totals.get('estimated_allocated_bytes_reclaimed')),
                 'ordinary_history_coverage_protected':protected})
    if not allowed:
        row['next_attempt_at_utc'] = iso(archive_schedule.next_due(None, now, window))
    return [row]


def packed_archive_placement_tasks(state_root: Path) -> list[dict]:
    """Use only the bounded local owner receipt; never stat archive payloads."""
    value = metadata(state_root/'status.json')
    if value.get('contract') != 'stockagent.packed_object_placement.v1.queue':
        return []
    rows = []
    if value.get('state') == 'waiting_reader_rollout':
        rows.append(task('packed_archive_placement', 'reader-rollout', state='waiting',
                         phase='archive_tier_wait', observed=clock(value), reason='archive_reader_rollout'))
    entries = value.get('entries')
    if not isinstance(entries, list):
        entries = []
    totals = value.get('totals', {}).get('datasets', []) if isinstance(value.get('totals'), dict) else []
    for entry in entries[:128]:
        if not isinstance(entry, dict) or source_subject(entry.get('dataset')) is None:
            continue
        progress = entry.get('progress') if isinstance(entry.get('progress'), dict) else {}
        done = number(progress.get('completed_bytes'))
        total = number(progress.get('total_bytes', entry.get('bytes_in_wave')))
        done_items = number(progress.get('completed_objects'))
        total_items = number(progress.get('total_objects', entry.get('objects_in_wave')))
        completed = (entry.get('state') == 'wave_completed' and total is not None and total > 0
                     and done == total and done_items == total_items)
        checking = entry.get('state') == 'checking'
        running = checking or entry.get('state') == 'running'
        reason = None if running or completed else (
            'archive_retirement_turn' if entry.get('wait_kind') == 'retirement_turn' else
            'archive_tier_capacity' if entry.get('error_type') == 'CapacityDeferred'
            else 'archive_tier_failed' if entry.get('state') == 'blocked' else 'archive_tier_wait')
        reclaimed = sum(number(item.get('retired_D_allocated_bytes')) or 0 for item in totals
                        if isinstance(item, dict) and item.get('dataset') == entry['dataset'])
        row = task('packed_archive_placement', entry['dataset'],
            state='completed' if completed else 'running' if running else
                  'blocked' if entry.get('state') == 'blocked' else 'waiting',
            phase='archive_tier_check' if checking else 'archive_tier_done' if completed else 'archive_tier_handoff'
                  if progress.get('stage') == 'E_archive_registered_D_name_retired'
                  else 'archive_tier_copy' if running else 'archive_tier_wait',
            observed=clock(value), generation=(entry.get('manifest_sha256'), progress.get('wave_identity')),
            done_bytes=done, total_bytes=total, done_items=done_items, total_items=total_items,
            proof=completed, reason=reason,
            metrics={'archive_head_bytes': number(entry.get('current_head_bytes')),
                     'archive_selected_historical':1 if entry.get('selected_historical') is True else 0,
                     'archive_placed_bytes': number(entry.get('already_placed_bytes')),
                     'archive_remaining_bytes': number(entry.get('remaining_bytes')),
                     'archive_shared_D_objects': number(entry.get('shared_D_names_retained')),
                     'archive_D_reclaimed_bytes': reclaimed})
        row['subject'] = source_subject(entry['dataset'])
        rows.append(row)
    # The queue advances to another cohort after each wave. Keep previously
    # completed handoffs visible instead of losing their history on refresh.
    from stockagent.data_sync.packed_object_placement import Locations, AUTHORITY, WORKSPACE
    totals = Locations(state_root, WORKSPACE, AUTHORITY).totals()
    by_dataset = {}
    for entry in totals['datasets']:
        if entry['state'] not in {'E_authoritative', 'E_retired'} or source_subject(entry['dataset']) is None:
            continue
        current = by_dataset.setdefault(entry['dataset'], {'dataset':entry['dataset'],
            'files':0,'bytes':0,'retired_D_allocated_bytes':0})
        current['retired_D_allocated_bytes'] += entry['retired_D_allocated_bytes'] or 0
        if entry['state'] == 'E_authoritative':
            current['files'] += entry['files']
            current['bytes'] += entry['bytes']
    for entry in list(by_dataset.values())[:256]:
        row = task('packed_archive_total', entry['dataset'], state='completed',
                   phase='archive_tier_totals', observed=clock(value),
                   generation=(entry['dataset'], entry['files'], entry['bytes']),
                   done_bytes=entry['bytes'], total_bytes=entry['bytes'],
                   done_items=entry['files'], total_items=entry['files'], proof=True,
                   metrics={'archive_D_reclaimed_bytes': entry['retired_D_allocated_bytes']})
        row['subject'] = source_subject(entry['dataset'])
        rows.append(row)
    return rows


def source_local_dedup_legacy_cleanup_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    if not workspace_checked:
        return []
    root = workspace/'migration/source-local-dedup-candidate-v1'
    rows = []
    if root.is_symlink() or not root.is_dir():
        return rows
    for directory in sorted(root.iterdir(), key=lambda path: path.name)[:256]:
        if directory.is_symlink() or not directory.is_dir() or source_subject(directory.name) is None:
            continue
        value = metadata(directory/(directory.name+'.legacy-prune.json'))
        plan, removed = value.get('plan'), value.get('removed')
        if (value.get('contract') != 'stockagent.dedup_stream_stage_handoff.v1.legacy-prune'
                or value.get('dataset') != directory.name or value.get('state') != 'completed'
                or not isinstance(plan, list) or len(plan) > 100000 or not isinstance(removed, list)
                or not all(isinstance(item, dict) and isinstance(item.get('name'), str)
                           and type(item.get('allocated_bytes')) is int and item['allocated_bytes'] >= 0
                           for item in plan)
                or removed != [item['name'] for item in plan]
                or value.get('reclaimed_allocated_bytes') != sum(item['allocated_bytes'] for item in plan)
                or value.get('plan_sha256') != hashlib.sha256((json.dumps(
                    plan, ensure_ascii=False, sort_keys=True, separators=(',', ':'))+'\n').encode()).hexdigest()
                or timestamp(value.get('completed_at_epoch')) is None):
            continue
        row = task('source_local_dedup_legacy_cleanup', directory.name, state='completed',
                   phase='source_local_dedup_legacy_cleanup', observed=timestamp(value['completed_at_epoch']),
                   generation=str(value.get('old_manifest_sha256', '')), done_items=len(plan),
                   total_items=len(plan), proof=True,
                   metrics={'E_legacy_reclaimed_allocated_bytes': value['reclaimed_allocated_bytes']})
        row['subject'] = source_subject(directory.name)
        rows.append(row)
    return rows


def edge_reader_recovery_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    """Show the reader owner observation, never payload or recovery proof."""
    if not workspace_checked:
        return []
    value = metadata(workspace/'migration/edge-reader-recovery-v1/status.json')
    states = {'ready', 'recovered', 'disabled', 'bundle_invalid', 'remote_unavailable',
              'base_not_enrolled', 'control_mismatch', 'wrapper_mismatch', 'release_mismatch',
              'reader_intact', 'retrying', 'waiting_trading'}
    state = choice(value.get('state'), states)
    if (value.get('contract') != 'stockagent.edge_reader_recovery_queue.v1' or value.get('apply') is not True
            or state is None or value.get('remote_payload_fetched') is not False
            or value.get('market_payload_deleted') is not False or value.get('NAS_accepted') is not False
            or not isinstance(value.get('reader_manifest_sha256'), str)
            or re.fullmatch(r'[a-f0-9]{64}', value['reader_manifest_sha256']) is None):
        return []
    reasons = {'disabled': 'edge_reader_disabled', 'bundle_invalid': 'edge_reader_bundle_invalid',
               'remote_unavailable': 'edge_reader_remote_unavailable', 'base_not_enrolled': 'edge_reader_base_missing',
               'control_mismatch': 'edge_reader_identity_changed', 'wrapper_mismatch': 'edge_reader_identity_changed',
               'release_mismatch': 'edge_reader_identity_changed', 'reader_intact': 'edge_reader_runtime_wait',
               'retrying': 'edge_reader_retry', 'waiting_trading': 'edge_reader_trading_deferred'}
    healthy = state in {'ready', 'recovered'}
    if (healthy and (not isinstance(value.get('probe_dataset_manifest_sha256'), str)
                     or re.fullmatch(r'[a-f0-9]{64}', value['probe_dataset_manifest_sha256']) is None)):
        return []
    last = timestamp(value.get('last_recovered_at_epoch'))
    observed = clock(value)
    if last is not None and (observed is None or last > observed+120):
        last = None
    return [task('edge_reader_recovery', 'current', node='vastai1T',
                 state='waiting' if healthy or state in {'disabled', 'waiting_trading'} else 'blocked',
                 phase='edge_reader_monitoring' if healthy else 'edge_reader_recovery_waiting',
                 observed=observed, reason='edge_reader_monitoring' if healthy else reasons[state],
                 metrics={'edge_reader_last_recovered_epoch': int(last) if last is not None else None}, proof=False)]


def v3_reader_activation_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    """Report capability and E eligibility without claiming a D publication."""
    if not workspace_checked:
        return []
    status = metadata(workspace/'migration/source-local-exact-repack/v3-reader-activation-status.json')
    if (status.get('contract') != 'stockagent.v3_reader_activation_queue.v1'
            or status.get('apply') is not True or status.get('state') not in {'idle', 'reader_ready', 'waiting_reader'}):
        return []
    state = status['state']
    last_activation = timestamp(status.get('last_activated_at_epoch'))
    metrics = {'v3_candidates': number(status.get('candidate_count')),
               'v3_activated_last': number(status.get('last_activated_count')),
               'v3_activation_errors': number(status.get('blocked_count')),
               'v3_last_activation_epoch': int(last_activation) if last_activation is not None else None}
    return [task('v3_reader_activation', 'current', state='blocked' if state == 'waiting_reader' else 'waiting',
                 phase='v3_reader_idle' if state == 'idle' else 'v3_reader_check', observed=clock(status),
                 reason='v3_reader_unavailable' if state == 'waiting_reader'
                        else 'v3_reader_no_pending' if state == 'idle' else None,
                 metrics=metrics, proof=False)]


def foreign_head_handoff_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    """Project bounded handoff journals; never treat them as D retirement."""
    if not workspace_checked:
        return []
    from stockagent.data_sync import authoritative_head_handoff as handoff
    status = metadata(workspace/'migration/source-local-exact-repack/foreign-head-handoff-status.json')
    if status.get('contract') != 'stockagent.foreign_head_handoff_queue.v1' or status.get('apply') is not True:
        return []
    root = workspace/'migration/authoritative-head-handoffs'
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        return []
    paths = list(root.glob('*/*/journal.json'))
    if len(paths) > 4096:
        return []
    completed, pending = 0, []
    for path in paths:
        value = metadata(path, limit=65536)
        req = value.get('request')
        if not isinstance(req, dict):
            continue
        name, digest = req.get('dataset'), req.get('old_manifest_sha256')
        if (source_subject(name) is None or not isinstance(digest, str)
                or re.fullmatch(r'[a-f0-9]{64}', digest) is None or path.parent != root/name/digest):
            continue
        try:
            value = handoff._record(path.parent, req)
        except (OSError, ValueError, TypeError, KeyError, RuntimeError):
            continue
        if value['state'] == 'completed':
            completed += 1
        elif value['state'] in ('planned', 'moved', 'peer_confirmed'):
            pending.append((name, digest, value))
    metrics = {'foreign_head_candidates': number(status.get('candidate_count')),
               'foreign_head_completed': completed, 'foreign_head_pending': len(pending)}
    summary = task('foreign_head_handoff', 'bounded-current', state='waiting', phase='foreign_head_queue',
                   observed=clock(status), metrics=metrics, proof=False)
    rows = [summary]
    recovered = status.get('recovered') if isinstance(status.get('recovered'), list) else []
    checked = {(row.get('dataset'), row.get('old_manifest_sha256')) for row in recovered if isinstance(row, dict)
               and row.get('state') == 'waiting_peer'}
    for name, digest, value in pending[:16]:
        phase, reason = {
            'planned': ('foreign_head_recovery', 'foreign_head_recovery_pending'),
            'moved': ('foreign_head_peer_wait', 'foreign_head_peer_pending'),
            'peer_confirmed': ('foreign_head_release_pin', 'foreign_head_release_pin_pending'),
        }[value['state']]
        row = task('foreign_head_handoff', (name, digest), state='waiting', phase=phase,
                   observed=timestamp(value.get('updated_at_epoch')), generation=digest,
                   reason=reason, proof=False)
        if (name, digest) in checked and clock(status) is not None:
            row['checked_at_utc'] = iso(clock(status))
        row['subject'] = source_subject(name)
        rows.append(row)
    return rows


def _file_generation(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _fixed_retirement_plan_count(path: Path, digest: str, generation: tuple) -> int | None:
    """Read and hash one bounded immutable E metadata plan per generation."""
    try:
        if path.is_symlink() or path.parent.resolve(strict=True) != path.parent:
            return None
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if (not stat.S_ISREG(before.st_mode) or _file_generation(before) != generation
                    or before.st_size > 8*1024*1024):
                return None
            raw = stream.read(8*1024*1024+1)
            after = os.fstat(stream.fileno())
        current = path.lstat()
        if (_file_generation(before) != _file_generation(after) or _file_generation(after) != _file_generation(current)
                or len(raw) != before.st_size or not raw.endswith(b'\n')
                or hashlib.sha256(raw[:-1]).hexdigest() != digest):
            return None
        plan = json.loads(raw)
        items = plan.get('items') if isinstance(plan, dict) else None
        if ('identity_sha256' in plan or not isinstance(items, list) or len(items) > 100000
                or not all(isinstance(row, dict) and isinstance(row.get('relative'), str)
                           and row.get('kind') in {'history', 'manifest', 'object'} for row in items)):
            return None
        return len(items)
    except (OSError, ValueError, TypeError, RecursionError):
        return None


@lru_cache(maxsize=64)
def _cached_retirement_plan_count(path: Path, digest: str, generation: tuple) -> int:
    count = _fixed_retirement_plan_count(path, digest, generation)
    if count is None:
        # A transient read failure must be retried on the next observation.
        raise ValueError('retirement plan is temporarily unavailable')
    return count


def _active_retirement_cursor(workspace: Path, name: str, old: str, *, mode='direct') -> dict:
    """Project committed name-count progress, never the pending unlink batch."""
    stage = workspace/'migration/stream-source-repack-v1'/name/old
    try:
        if stage.is_symlink() or not stage.is_dir() or stage.resolve(strict=True) != stage:
            return {}
        if mode == 'direct':
            path = stage/'retirement.json'
            contract = 'stockagent.source_local_repack_retirement.v1'
        elif mode in {'history', 'prepared_history'}:
            prefix = 'prepared-history-retirement-' if mode == 'prepared_history' else 'history-retirement-'
            found = []
            with os.scandir(stage) as entries:
                for index, entry in enumerate(entries):
                    if index >= 512 or len(found) >= 256:
                        return {}
                    if re.fullmatch(re.escape(prefix)+r'[a-f0-9]{64}\.json', entry.name):
                        found.append(stage/entry.name)
            active = []
            for candidate in found:
                summary = metadata(candidate)
                if summary.get('state') in {'planned', 'removing'}:
                    active.append(candidate)
            if len(active) != 1:
                return {}
            path = active[0]
            contract = 'stockagent.superseded_repack_history.v1'
        else:
            return {}
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            return {}
        value = metadata(path)
        after = path.lstat()
        if (_file_generation(before) != _file_generation(after)
                or value.get('contract') != contract or value.get('dataset') != name
                or mode == 'direct' and value.get('old_manifest_sha256') != old
                or value.get('state') not in ({'planned', 'removing', 'completed'} if mode == 'direct'
                                               else {'planned', 'removing'})):
            return {}
        digest, count = value.get('plan_sha256'), value.get('removed_count')
        if (not isinstance(digest, str) or re.fullmatch(r'[a-f0-9]{64}', digest) is None
                or value.get('plan_ref') != 'retirement-plan-'+digest+'.json'
                or mode != 'direct' and path.name != prefix+digest+'.json'
                or type(count) is not int or count < 0):
            return {}
        plan_path = stage/value['plan_ref']
        plan_info = plan_path.lstat()
        total = _cached_retirement_plan_count(plan_path, digest, _file_generation(plan_info))
        if total is None or total <= 0 or count > total:
            return {}
        return {'observed': after.st_mtime, 'done_items': count, 'total_items': total,
                'plan_generation': digest}
    except (OSError, ValueError, TypeError):
        return {}


def _retirement_turn_observation(workspace: Path) -> dict | None:
    value = metadata(workspace/'migration/source-local-exact-repack/retirement-turn-observation.json', limit=16384)
    observed = timestamp(value.get('observed_at_epoch'))
    identity = value.get('owner_identity')
    if (value.get('contract') != 'stockagent.source_local_retirement_turn_observation.v1'
            or value.get('state') != 'waiting_writer' or value.get('completion_proved') is not False
            or observed is None or not isinstance(identity, dict)
            or type(identity.get('pid')) is not int or identity['pid'] < 1
            or type(identity.get('start_ticks')) is not int or identity['start_ticks'] < 0
            or not isinstance(identity.get('boot_id'), str)
            or re.fullmatch(r'[0-9a-f-]{36}', identity['boot_id']) is None):
        return None
    alive = _process_identity_matches(identity)
    return task('source_repack_retirement', ('waiting-writer', identity.get('pid'), identity.get('start_ticks')),
                state='waiting' if alive else 'blocked', phase='source_repack_retirement_wait_writer',
                observed=observed, reason='retirement_turn_waiting_writer' if alive
                else 'retirement_turn_interrupted', proof=False)


def source_repack_retirement_tasks(workspace: Path, *, workspace_checked=False) -> list[dict]:
    from stockagent.data_sync.semantic_link_promotion import FAST_LATEST_DATASETS
    if not workspace_checked:
        return []
    turn = _retirement_turn_observation(workspace)
    status = metadata(workspace/'migration/source-local-exact-repack/retirement-queue-status.json')
    if status.get('contract') != 'stockagent.source_local_repack_queue.v1.retirement-status':
        return [turn] if turn else []
    entries = status.get('entries')
    if not isinstance(entries, list) or len(entries) > 1024:
        return [turn] if turn else []
    rows, represented = ([turn] if turn else []), {}
    for item in entries:
        if not isinstance(item, dict) or source_subject(item.get('dataset')) is None:
            continue
        state = item.get('state')
        if state not in {'running', 'waiting', 'waiting_retry', 'completed'}:
            continue
        name = item['dataset']
        # A live canonical owner merely delays this scheduled transaction;
        # distinguish that retry from a pin, unknown metadata, or other
        # blocker that can require investigation. Never infer completion.
        transient_owner = item.get('reason') in {
            'd_e_owner_busy', 'retirement_source_replication_busy',
            'retirement_backup_cycle_busy', 'retirement_process_references',
        }
        public_state = ('completed' if state == 'completed' else 'running' if state == 'running'
                        else 'blocked' if state == 'waiting_retry' and not transient_owner else 'waiting')
        mode = item.get('mode')
        history = mode == 'history'
        prepared_history = mode == 'prepared_history'
        latest = mode == 'latest'
        dedup = mode == 'dedup'
        old = item.get('old_manifest_sha256')
        if not isinstance(old, str) or not re.fullmatch(r'[a-f0-9]{64}', old):
            continue
        disposition = ('dedup' if dedup else 'latest' if latest else
                       'prepared_history' if prepared_history else 'history' if history else 'direct')
        # Dataset and mode alone collapse different fixed generations into the
        # same browser card. Bind the task ID to the immutable old manifest.
        key = (name, disposition, old)
        phase = ('source_shared_object_completed' if dedup and state == 'completed' else
                 'source_shared_object_working' if dedup else 'source_local_prepared_history' if prepared_history else
                 'source_repack_history_working' if history else 'source_latest_retirement_working' if latest
                 else 'source_repack_retirement_working')
        cursor = (_active_retirement_cursor(workspace, name, old, mode=disposition)
                  if state == 'running' and disposition in {'direct', 'history', 'prepared_history'}
                  and item.get('private') is not True else {})
        if cursor:
            phase = 'source_repack_retirement_unlink' if disposition == 'direct' else 'source_repack_history_unlink'
        row = task('source_object_dedup' if dedup else 'source_repack_retirement', key, state=public_state,
                   phase=phase, observed=cursor.get('observed', clock(status)),
                   generation=old+':'+cursor['plan_generation'] if cursor else old, proof=state == 'completed',
                   reason=item.get('reason') if state == 'waiting_retry' else
                          'staging_queued' if state == 'waiting' else None,
                   done_bytes=number(item.get('deleted_bytes')) if state == 'completed' else None,
                   total_bytes=number(item.get('deleted_bytes')) if state == 'completed' else None,
                   done_items=number(item.get('deleted_objects')) if state == 'completed' else cursor.get('done_items'),
                   total_items=number(item.get('deleted_objects')) if state == 'completed' else cursor.get('total_items'),
                   metrics={**({'estimated_reclaimed_allocated_bytes': number(item.get('estimated_reclaimed_bytes'))}
                              if state == 'completed' else {}),
                            **({'uncompared_changed_members': number(item.get('uncompared_changed_members'))}
                               if latest else {}),
                            **({'head_transition_recheck': 1}
                               if mode == 'direct' and item.get('private') is not True
                               and state == 'waiting_retry' and item.get('reason') == 'retirement_current_head'
                               else {}),
                            **({'duplicate_encoded_bytes_pending': number(item.get('logical_bytes'))}
                               if dedup and state != 'completed' else {})})
        row['subject'] = source_subject(name)
        row['next_attempt_at_utc'] = iso(timestamp(item.get('retry_after_epoch')))
        rows.append(row)
        represented[(name, old, disposition)] = row
    root = workspace/'migration/source-local-exact-repack'
    if root.is_dir() and not root.is_symlink():
        for directory in sorted(root.iterdir(), key=lambda path: (path.name not in SUBJECTS, path.name))[:128]:
            if source_subject(directory.name) is None or directory.is_symlink() or not directory.is_dir():
                continue
            for path in sorted(directory.glob('*.json'))[:256]:
                value = metadata(path)
                result = value.get('retirement')
                if (path.is_symlink() or not re.fullmatch(r'[a-f0-9]{64}', path.stem)
                        or value.get('contract') != 'stockagent.source_local_exact_repack.v1'
                        or value.get('dataset') != directory.name or value.get('new_manifest_sha256') != path.stem
                        or value.get('old_release_removed') is not True or not isinstance(result, dict)
                        or result.get('state') != 'completed' or result.get('apply') is not True
                        or result.get('old_manifest_sha256') != value.get('old_manifest_sha256')):
                    continue
                count, size = number(result.get('object_count')), number(result.get('object_bytes'))
                stamp = clock(result)
                if count is None or size is None or stamp is None:
                    continue
                if (directory.name, value['old_manifest_sha256'], 'direct') not in represented:
                    row = task('source_repack_retirement', (directory.name, 'direct', value['old_manifest_sha256']), state='completed',
                               phase='source_repack_retirement_working', observed=stamp,
                               generation=str(value['old_manifest_sha256']), proof=True,
                               done_bytes=size, total_bytes=size, done_items=count, total_items=count,
                               metrics={'estimated_reclaimed_allocated_bytes': number(result.get('estimated_allocated_bytes_reclaimed'))})
                    row['subject'] = source_subject(directory.name)
                    rows.append(row)
                history_result = value.get('history_retirement')
                if isinstance(history_result, dict):
                    count = number(history_result.get('versions_removed'))
                    size = number(history_result.get('object_bytes_removed'))
                    history_stamp = clock(history_result)
                    if count is not None and size is not None and history_stamp is not None:
                        history_key = (directory.name, value['old_manifest_sha256'], 'history')
                        anchor_note = ({'history_anchor_missing': 1}
                            if history_result.get('original_anchor_missing_without_successor') is True else {})
                        if history_key in represented:
                            represented[history_key]['metrics'].update({
                                'history_cumulative_reclaimed_bytes': number(history_result.get('estimated_allocated_bytes_reclaimed')),
                                'history_cumulative_versions': count,
                                'history_cumulative_observed_epoch': int(history_stamp), **anchor_note})
                        else:
                            history_row = task('source_repack_retirement', (directory.name, 'history', value['old_manifest_sha256']), state='completed',
                                phase='source_repack_history_working', observed=history_stamp,
                                generation=str(value['old_manifest_sha256']), proof=True,
                                done_bytes=size, total_bytes=size, done_items=count, total_items=count,
                                metrics={'estimated_reclaimed_allocated_bytes': number(history_result.get('estimated_allocated_bytes_reclaimed')),
                                         **anchor_note})
                            history_row['subject'] = source_subject(directory.name)
                            rows.append(history_row)
    # The transient queue drops a successful semantic retirement on its next
    # scan.  Surface its durable, generation-bound receipt instead of making
    # the recovered space disappear from the dashboard a minute later.
    semantic_root = workspace/'migration/handoff-index-archive-v1/binance-public-archive'
    if semantic_root.is_dir() and not semantic_root.is_symlink():
        paths = list(semantic_root.glob('semantic-promotion-*.json'))
        for path in paths if len(paths) <= 64 else ():
            value = metadata(path)
            result = value.get('retirement')
            comparison, old, new = (value.get(field) for field in
                                     ('comparison_identity_sha256', 'old_manifest_sha256', 'new_manifest_sha256'))
            if (path.is_symlink() or not all(isinstance(x, str) and re.fullmatch('[a-f0-9]{64}', x)
                                             for x in (comparison, old, new))
                    or path.name != 'semantic-promotion-'+comparison+'.json'
                    or value.get('contract') != 'stockagent.semantic_link_promotion.v1'
                    or value.get('dataset') != 'binance-public-archive' or value.get('state') != 'committed'
                    or value.get('old_sources_deleted') is not True or not isinstance(result, dict)
                    or result.get('state') != 'completed' or result.get('apply') is not True
                    or result.get('dataset') != value['dataset']
                    or result.get('old_manifest_sha256') != old or result.get('new_manifest_sha256') != new):
                continue
            count, size, stamp = number(result.get('object_count')), number(result.get('object_bytes')), clock(result)
            if count is None or size is None or stamp is None:
                continue
            if ('binance-public-archive', old, 'direct') in represented:
                continue
            row = task('source_repack_retirement', ('binance-public-archive', 'semantic', old),
                       state='completed', phase='source_repack_retirement_working', observed=stamp,
                       generation=old, proof=True, done_bytes=size, total_bytes=size,
                       done_items=count, total_items=count,
                       metrics={'estimated_reclaimed_allocated_bytes':
                                number(result.get('estimated_allocated_bytes_reclaimed'))})
            row['subject'] = source_subject('binance-public-archive')
            rows.append(row)
    for dataset in sorted(FAST_LATEST_DATASETS):
        latest_root = workspace/'migration/handoff-index-archive-v1'/dataset
        if not latest_root.is_dir() or latest_root.is_symlink():
            continue
        paths = list(latest_root.glob('latest-promotion-*.json'))
        for path in paths if len(paths) <= 64 else ():
            value = metadata(path)
            result = value.get('retirement')
            comparison, old, new = (value.get(field) for field in
                                     ('comparison_identity_sha256', 'old_manifest_sha256', 'new_manifest_sha256'))
            if (path.is_symlink() or not all(isinstance(x, str) and re.fullmatch('[a-f0-9]{64}', x)
                                             for x in (comparison, old, new))
                    or path.name != 'latest-promotion-'+comparison+'.json'
                    or value.get('contract') != 'stockagent.latest_fixed_link_promotion.v1'
                    or value.get('dataset') != dataset or value.get('state') != 'committed'
                    or value.get('old_sources_deleted') is not True or not isinstance(result, dict)
                    or value.get('changed_member_values_compared') is not False
                    or value.get('old_historical_value_coverage') != 'not_compared'
                    or result.get('state') != 'completed' or result.get('apply') is not True
                    or result.get('dataset') != value['dataset']
                    or result.get('old_manifest_sha256') != old or result.get('new_manifest_sha256') != new
                    or result.get('changed_member_values_compared') is not False
                    or result.get('old_historical_value_coverage') != 'not_compared'):
                continue
            count, size, stamp = number(result.get('object_count')), number(result.get('object_bytes')), clock(result)
            if count is None or size is None or stamp is None or (dataset, old, 'latest') in represented:
                continue
            row = task('source_repack_retirement', (dataset, 'latest', old), state='completed',
                       phase='source_latest_retirement_working', observed=stamp, generation=old, proof=True,
                       done_bytes=size, total_bytes=size, done_items=count, total_items=count,
                       metrics={'estimated_reclaimed_allocated_bytes': number(result.get('estimated_allocated_bytes_reclaimed')),
                                'uncompared_changed_members': number(value.get('uncompared_changed_members')),
                                'duplicate_encoded_bytes_pending': number(value.get('duplicate_encoded_bytes_pending'))})
            row['subject'] = source_subject(dataset)
            rows.append(row)
        shared_paths = list(latest_root.glob('shared-objects-*.json'))
        for path in shared_paths if len(shared_paths) <= 64 else ():
            value = metadata(path)
            fixed = value.get('plan')
            original = fixed.get('promotion_before') if isinstance(fixed, dict) else None
            comparison = fixed.get('comparison_identity_sha256') if isinstance(fixed, dict) else None
            old = original.get('old_manifest_sha256') if isinstance(original, dict) else None
            if (path.is_symlink() or not isinstance(comparison, str) or re.fullmatch('[a-f0-9]{64}', comparison) is None
                    or not isinstance(old, str) or re.fullmatch('[a-f0-9]{64}', old) is None
                    or path.name != 'shared-objects-'+comparison+'.json'
                    or value.get('contract') != 'stockagent.fixed_generation_shared_objects.v1'
                    or value.get('state') != 'completed' or value.get('apply') is not True
                    or value.get('dataset') != dataset or value.get('old_manifest_sha256') != old
                    or value.get('independent_D_readback') is not False or value.get('NAS_accepted') is not False
                    or original.get('dataset') != dataset or original.get('comparison_identity_sha256') != comparison
                    or not isinstance(fixed, dict)
                    or value.get('plan_sha256') != hashlib.sha256(json.dumps(
                        {k: v for k, v in fixed.items() if k != 'identity_sha256'},
                        sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
                    or (dataset, old, 'dedup') in represented):
                continue
            count, size, stamp = (number(value.get('object_count')), number(value.get('object_bytes')),
                                  timestamp(value.get('completed_at_epoch')))
            if count is None or size is None or stamp is None:
                continue
            row = task('source_object_dedup', (dataset, 'dedup', old), state='completed',
                       phase='source_shared_object_completed', observed=stamp, generation=old, proof=True,
                       done_bytes=size, total_bytes=size, done_items=count, total_items=count,
                       metrics={'estimated_reclaimed_allocated_bytes':
                                number(value.get('estimated_allocated_bytes_reclaimed'))})
            row['subject'] = source_subject(dataset)
            rows.append(row)
    return rows


def source_local_candidate_tasks(workspace: Path, candidates, *, workspace_checked=False) -> list[dict]:
    """Display explicitly enrolled E candidates without reading their payloads."""
    if not isinstance(candidates, list) or len(candidates) > 32:
        return []
    rows = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        name, subject, expected = (candidate.get(key) for key in
                                   ('name', 'subject', 'original_manifest_sha256'))
        expected_layout = candidate.get('pack_layout')
        if (not isinstance(name, str) or re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', name) is None
                or not isinstance(subject, str) or subject not in SUBJECTS or not isinstance(expected, str)
                or re.fullmatch(r'[a-f0-9]{64}', expected) is None
                or (expected_layout is not None and expected_layout not in
                    ('source-directory-v1', 'source-dataset-v1'))):
            continue
        root = workspace / 'build' / name
        ready, status = metadata(root / 'candidate-ready.json'), metadata(root / 'status.json')
        files, size = number(ready.get('files')), number(ready.get('logical_bytes'))
        digest = ready.get('candidate_manifest_sha256')
        proof = (workspace_checked is True and ready.get('contract') == 'stockagent.source_local_E_candidate.v1'
                 and ready.get('state') == 'E_candidate_independently_restored'
                 and ready.get('dataset') == subject and ready.get('candidate_path') == str(root)
                 and ready.get('original_manifest_sha256') == expected
                 and (expected_layout is None or ready.get('new_pack_layout') == expected_layout)
                 and isinstance(digest, str) and re.fullmatch(r'[a-f0-9]{64}', digest) is not None
                 and files is not None and files > 0 and size is not None
                 and ready.get('original_reconstructed_files') == files
                 and ready.get('candidate_reconstructed_files') == files
                 and ready.get('portable_inventories_equal') is True
                 and ready.get('D_authority_changed') is False and ready.get('NAS_accepted') is False
                 and ready.get('source_retirement_authorized') is False and clock(ready) is not None
                 and clock(ready) <= time.time() + 120)
        invalid_ready = (bool(ready) or status.get('state') == 'E_candidate_independently_restored') and not proof
        failed = status.get('state') == 'failed_retained_for_inspection'
        row = task('source_local_candidate', name, state='completed' if proof else 'blocked' if failed or invalid_ready else 'waiting',
                   phase='source_local_candidate_ready' if proof else 'source_local_candidate_building',
                   observed=clock(ready) if proof else clock(status), generation=digest if proof else name,
                   done_items=files if proof else None, total_items=files if proof else None,
                   done_bytes=size if proof else None, total_bytes=size if proof else None,
                   proof=proof, reason=None if proof else 'source_local_candidate_unverified')
        row['subject'] = subject
        rows.append(row)
    return rows


def _apply_stream_D_capacity(rows, cold, *, now):
    stamp = clock(cold) if isinstance(cold, dict) else None
    available = number(cold.get('available_bytes')) if isinstance(cold, dict) else None
    if (isinstance(cold, dict) and cold.get('state') == 'measured' and available is not None
            and stamp is not None and now-STALE_SECONDS <= stamp <= now+120):
        from scripts.run_stream_packed_repack import D_INSTALL_RESERVE
        for row in rows:
            needed = number(row.get('_D_encoded_remaining_bytes'))
            if row.get('kind') in {'source_local_repack_queue', 'handoff_index_D_storage'} and needed is not None:
                reserve = number(row.get('_D_reserve_bytes')) or D_INSTALL_RESERVE
                shortage = max(0, needed+reserve-available)
                row['metrics'].update(D_required_bytes=needed, D_observed_free_bytes=available,
                    D_reserve_bytes=reserve, D_shortfall_bytes=shortage,
                    D_capacity_observed_epoch=int(stamp), D_capacity_live=1)
                if shortage == 0 and row.get('state') == 'blocked' and row.get('reason') in {
                        'repack_D_capacity', 'repack_exclusive_space', 'handoff_index_D_capacity'}:
                    row.update(state='waiting', reason='staging_queued')
                    if row.get('phase') == 'source_local_E_prepared':
                        row['phase'] = 'source_local_E_prepared_ready'


def _apply_source_capacity_recheck(rows, cold, *, now):
    """Keep an old capacity failure distinct from a fresh scheduler admission."""
    stamp = clock(cold) if isinstance(cold, dict) else None
    available = number(cold.get('available_bytes')) if isinstance(cold, dict) else None
    fresh = (isinstance(cold, dict) and cold.get('state') == 'measured' and available is not None
             and stamp is not None and now-STALE_SECONDS <= stamp <= now+120)
    for row in rows:
        minimum = row.pop('_capacity_recheck_minimum', None)
        previous = row.pop('_capacity_recheck_observed', None)
        if (fresh and row.get('kind') == 'migration_dependency' and row.get('reason') == 'physical_capacity'
                and minimum is not None and minimum > 0 and previous is not None
                and stamp > previous and available >= minimum):
            row.update(state='waiting', phase='source_capacity_recheck', reason='capacity_recheck_due')


def apply_legacy_archive_deadline(row: dict):
    """Do not project a phase beyond a bound old owner's known deadline."""
    deadline = row.pop('_legacy_archive_deadline', None)
    if not isinstance(deadline, (int, float)):
        return
    row['owner_deadline_at_utc'] = iso(deadline)
    eta = row.get('eta') if isinstance(row.get('eta'), dict) else {}
    projected = timestamp(eta.get('completion_at_utc'))
    if projected is None or projected > deadline:
        eta.update(state='unknown', reason='handoff_archive_legacy_timeout',
                   remaining_seconds=None, fastest_seconds=None, slowest_seconds=None,
                   completion_at_utc=None, rate_per_second=None)
    row['eta'] = eta


def next_refresh_priority_boundary(now: float, repo: Path) -> float | None:
    """Find the next actual trading-priority boundary from the same calendar gate."""
    from datetime import time as wall_time
    from scripts.check_outside_tw_opening_resource_window import evaluate
    local = datetime.fromtimestamp(now, ZoneInfo('Asia/Taipei'))
    for offset in range(8):
        day = local + timedelta(days=offset)
        boundary = day.replace(hour=6, minute=20, second=0, microsecond=0)
        end = day.replace(hour=13, minute=30, second=0, microsecond=0)
        if end.timestamp() <= now:
            continue
        decision = evaluate(boundary, minimum_runway_minutes=120, protected_until=wall_time(13, 30),
                            official_calendar_root=repo/'data_tw_public')
        if decision.get('protected') is True:
            return max(now, boundary.timestamp())
    return None


def apply_refresh_priority_boundary(row: dict, *, now: float, repo: Path):
    """Do not project uninterrupted work past the owner's verified stop window."""
    if not row.pop('_whole_root_refresh_yields', False):
        return
    eta = row.get('eta') if isinstance(row.get('eta'), dict) else {}
    projected = timestamp(eta.get('completion_at_utc'))
    if projected is None:
        return
    boundary = next_refresh_priority_boundary(now, repo)
    if boundary is None or projected >= boundary:
        eta.update(state='unknown', reason='refresh_priority_boundary', remaining_seconds=None,
                   fastest_seconds=None, slowest_seconds=None, completion_at_utc=None, rate_per_second=None)


def collect(repo: Path, config: dict, history: dict, *, now=None, runtime=None,
            capacities=None) -> tuple[dict, dict]:
    now = time.time() if now is None else now
    options = config.get("operations") if isinstance(config.get("operations"), dict) else {}
    queue = Path(options.get("retirement_queue_config", "configs/data_sync/wsl_retirement_queue.json"))
    sources = [
        ('ordinary_archive', lambda: ordinary_archive_tasks(repo,
            Path(options.get('ordinary_archive_state_root', '/var/lib/stockagent/ordinary-archive')), now=now)
            if options.get('ordinary_archive_state_root')
            or repo.resolve() == Path(__file__).resolve().parents[2] else []),
        ('packed_archive_placement', lambda: packed_archive_placement_tasks(
            Path(options.get('packed_archive_placement_state_root', '/var/lib/stockagent/packed-object-placement')))
            if options.get('packed_archive_placement_state_root')
            or repo.resolve() == Path(__file__).resolve().parents[2] else []),
        ('minute_validation_cache_rebind', lambda: minute_validation_cache_tasks(repo)),
        ('cold_scan_notification', lambda: cold_scan_notification_tasks(
            Path(options.get('cold_scan_retry_receipt', '/var/lib/stockagent-d-cold-scan-retry/latest.json')))
            if options.get('cold_scan_retry_receipt') or repo.resolve() == Path(__file__).resolve().parents[2] else []),
        ("vast_spool", lambda: spool_tasks(repo)),
        ("registered_backup", lambda: backup_tasks(Path(options.get("backup_state_root", "/var/lib/stockagent/backup-stream")),
            Path(options.get('cold_scan_retry_receipt', '/var/lib/stockagent-d-cold-scan-retry/latest.json')))),
        ("migration", lambda: migration_tasks(Path(options.get("migration_state_root", "/var/lib/stockagent/lakehouse-control/dataset-versions")),
            repo=repo, recovery_workspace=(Path(options['provider_workspace_root']) if options.get('provider_workspace_root')
                and _source_local_workspace_checked(Path(options['provider_workspace_root'])) else None), now=now)),
        ("lake_transport_retirement", lambda: lake_transport_tasks(Path(options.get("transport_gc_state_root", "/var/lib/stockagent/lakehouse-control")))),
        ("lifecycle_backup", lambda: lifecycle_backup_tasks(Path(options.get("control_backup_state_root", "/var/lib/stockagent/lakehouse-control")))),
        ("retirement", lambda: retirement_tasks(repo, queue if queue.is_absolute() else repo / queue)),
        ('local_testing_cleanup', lambda: local_testing_cleanup_tasks(repo,
            Path(options['provider_workspace_root']) if options.get('provider_workspace_root')
            and _source_local_workspace_checked(Path(options['provider_workspace_root'])) else None)),
        ('vast_testing_cleanup', lambda: vast_testing_cleanup_tasks(
            Path(options['provider_workspace_root']) if options.get('provider_workspace_root')
            and _source_local_workspace_checked(Path(options['provider_workspace_root'])) else None)),
        ('tw_rescue_original_manifests', lambda: tw_rescue_original_manifest_tasks(now=now)
            if repo.resolve() == Path(__file__).resolve().parents[2] else []),
        ('training_rescue_manifests', lambda: training_rescue_registered_manifest_tasks(now=now)
            if repo.resolve() == Path(__file__).resolve().parents[2] else []),
        ('training_rescue_capture', lambda: training_rescue_operational_tasks()
            if repo.resolve() == Path(__file__).resolve().parents[2] else []),
        ('source_replication_pause', lambda: source_replication_pause_tasks(
            Path(options.get('transport_gc_state_root', '/var/lib/stockagent/lakehouse-control')))
            if repo.resolve() == Path(__file__).resolve().parents[2] else []),
        ('catalog_maintenance', lambda: catalog_maintenance_tasks(
            Path(options.get('transport_gc_state_root', '/var/lib/stockagent/lakehouse-control')))
            if repo.resolve() == Path(__file__).resolve().parents[2] else []),
        ('d_testing_cleanup', lambda: d_testing_cleanup_tasks(repo)),
        ('encoding_stage_retirement', lambda: encoding_stage_tasks(repo,
            Path(options.get('legacy_stage_pruning_root', '/var/lib/stockagent-legacy-artifacts/stage-pruning')))),
        ("receiver", lambda: [receiver_task(Path(config.get("receipt_root", "/srv/stockagent-backup-receipts-lab203")), now=now)]),
        ('lake_relay', lambda: lake_relay_tasks(Path(config.get('receipt_root', '/srv/stockagent-backup-receipts-lab203')),
            metadata(repo / 'configs/data_sync/backup_stream.json', limit=128 * 1024), now=now))]
    if options.get('catalog_export_observation'):
        sources.insert(0, ('catalog_export', lambda: catalog_export_tasks(
            Path(options['catalog_export_observation']),
            Path(options['catalog_export_progress']) if options.get('catalog_export_progress') else None)))
    if options.get('windows_storage_preparation_receipt'):
        sources.insert(0, ('workspace_e_resize', lambda: windows_storage_tasks(
            Path(options['windows_storage_preparation_receipt']), now=now,
            guest_e_observation=_observe_guest_e_capacity(now=now)
                if repo.resolve() == Path(__file__).resolve().parents[2] else None)))
    if options.get('workspace_source_staging_config') and options.get('provider_workspace_root'):
        config_path = Path(options['workspace_source_staging_config'])
        config_path = config_path if config_path.is_absolute() else repo / config_path
        sources.insert(0, ('offline_repair_archive', lambda: offline_repair_archive_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('offline_repair_handoff', lambda: offline_repair_followup_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        from scripts import prepare_offline_repair_archive as offline_archive, run_workspace_source_staging as staging_queue
        configured = metadata(config_path).get('sources', [])
        if (isinstance(configured, list) and any(isinstance(item, dict)
                and staging_queue.stream_scope(item) == staging_queue.OFFLINE_ROLLBACK_SCOPE
                for item in configured)):
            sources.insert(0, ('offline_repair_archive_rollback', lambda: offline_repair_archive_tasks(
                Path(options['provider_workspace_root']), dataset=offline_archive.ROLLBACK_DATASET,
                workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
            sources.insert(0, ('offline_repair_handoff_rollback', lambda: offline_repair_followup_tasks(
                Path(options['provider_workspace_root']), dataset=offline_archive.ROLLBACK_DATASET,
                workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('workspace_source_staging', lambda: workspace_source_staging_tasks(
            repo, config_path, Path(options['provider_workspace_root']), now=now)))
        sources.insert(0, ('workspace_source_handoff', lambda: workspace_source_handoff_tasks(repo)))
        sources.insert(0, ('workspace_symlink_closure', lambda: workspace_symlink_closure_tasks(
            repo, config_path, Path(options['provider_workspace_root']))))
        sources.insert(0, ('workspace_cold_inventory', lambda: workspace_cold_inventory_tasks(
            repo, Path(options['provider_workspace_root']))))
        sources.insert(0, ('futures_725_projection', lambda: futures_725_projection_tasks(
            repo, Path(options['provider_workspace_root']))))
        sources.insert(0, ('futures_765_projection', lambda: futures_765_projection_tasks(
            repo, Path(options['provider_workspace_root']))))
        sources.insert(0, ('workspace_cold_inventory_snapshot', lambda: workspace_cold_comparison_snapshot_tasks(
            repo, Path(options['provider_workspace_root']))))
    if options.get('source_local_candidates') and options.get('provider_workspace_root'):
        sources.insert(0, ('source_local_candidate', lambda: source_local_candidate_tasks(
            Path(options['provider_workspace_root']), options['source_local_candidates'],
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
    if options.get('provider_workspace_root'):
        sources.insert(0, ('fred_pin_recovery', lambda: fred_pin_recovery_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])), now=now)))
        sources.insert(0, ('offline_replay_pair', lambda: offline_replay_pair_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('private_subset_reuse', lambda: private_subset_reuse_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('vast_sealed_e_handoff', lambda: vast_sealed_e_handoff_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('staged_cold_attribution', lambda: staged_cold_attribution_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('handoff_index_archive', lambda: handoff_index_archive_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('handoff_index_coverage', lambda: handoff_index_coverage_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('handoff_source_repack', lambda: handoff_source_repack_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('source_repack_retirement', lambda: source_repack_retirement_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('foreign_head_handoff', lambda: foreign_head_handoff_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('v3_reader_activation', lambda: v3_reader_activation_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('edge_reader_recovery', lambda: edge_reader_recovery_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('source_local_dedup_legacy_cleanup', lambda: source_local_dedup_legacy_cleanup_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('source_local_repack_queue', lambda: source_local_repack_queue_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('source_local_promotion', lambda: source_local_promotion_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])))))
        sources.insert(0, ('d_cold_e_handoff', lambda: d_cold_e_handoff_tasks(
            Path(options['provider_workspace_root']),
            workspace_checked=_source_local_workspace_checked(Path(options['provider_workspace_root'])), now=now)))
    if options.get('workspace_relocation_state_root'):
        sources.insert(0,('workspace_transport',lambda:workspace_transport_tasks(Path(options['workspace_relocation_state_root']))))
        sources.insert(0, ('workspace_partial_name_restore', lambda: workspace_partial_name_restore_tasks(
            Path(options['workspace_relocation_state_root']))))
        if options.get('provider_workspace_root'):
            sources.insert(0, ('workspace_partial_cold_readback', lambda: workspace_partial_cold_readback_tasks(
                Path(options['workspace_relocation_state_root']), Path(options['provider_workspace_root']))))
    if options.get('workspace_private_scratch_state_root'):
        sources.insert(0, ('workspace_private_scratch', lambda: workspace_private_scratch_tasks(
            Path(options['workspace_private_scratch_state_root']))))
    candidates = options.get('workspace_private_scratch_candidates', [])
    if isinstance(candidates, list):
        for candidate in candidates[:8]:
            sources.insert(0, ('workspace_private_scratch', lambda candidate=candidate:
                workspace_private_scratch_tasks(Path(candidate['state_root']),
                    identity=candidate['state_root'], expected_sha256=candidate['snapshot_sha256'],
                    subject=candidate.get('subject'))))
    if isinstance(options.get('workspace_controls_candidates'), list):
        sources.insert(0, ('workspace_controls', lambda: workspace_controls_tasks(
            options['workspace_controls_candidates'])))
    if options.get('provider_workspace_registry') and options.get('provider_workspace_root'):
        sources.insert(0, ('provider_workspace', lambda: provider_workspace_tasks(
            repo, Path(options['provider_workspace_registry']), Path(options['provider_workspace_root']),
            diagnostic_path=Path(options['provider_workspace_diagnostics'])
                if options.get('provider_workspace_diagnostics') else None, now=now)))
    if options.get('bulk_return_state_root'):
        sources.insert(0, ('bulk_return', lambda: bulk_return_tasks(Path(options['bulk_return_state_root']))))
    if options.get('d_mount_repair_status'):
        sources.insert(0, ('d_mount_repair', lambda: d_mount_repair_tasks(Path(options['d_mount_repair_status']))))
    if options.get('remote_return_status'):
        sources.insert(0, ('remote_return', lambda: remote_return_tasks(Path(options['remote_return_status']))))
    rows = []
    for kind, read in sources:
        try:
            rows.extend(read())
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            rows.append(task(kind, kind, reason="metadata_unavailable"))
    if options.get('workspace_source_staging_config') and options.get('provider_workspace_root'):
        try:
            from scripts import run_workspace_source_staging as source_queue
            from stockagent.data_sync import workspace_source_handoff as handoff
            rows.extend(unenrolled_source_handoff_tasks(
                source_queue.load_sources(config_path), set(handoff.registered_names()), rows))
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            rows.append(task('workspace_source_handoff', 'unenrolled-discovery', reason='metadata_unavailable'))
    # Reuse the observer's current bounded physical measurement. A source may
    # still be routing after a prior ENOSPC, but that speed cannot forecast a
    # writable publication on a full disk. Preserve counters and their clock;
    # only suspend the estimate and expose the independently measured blocker.
    cold = capacities.get('penguin-d') if isinstance(capacities, dict) else None
    stamp = clock(cold) if isinstance(cold, dict) else None
    available = number(cold.get('available_bytes')) if isinstance(cold, dict) else None
    _apply_stream_D_capacity(rows, cold, now=now)
    _apply_source_capacity_recheck(rows, cold, now=now)
    # Cold-tier compaction writes on D. Backup-stream reserves apply to the
    # enrolled transport filesystem, which can be E; never project those
    # transport limits onto D merely because the backup source is there.
    tier_config = metadata(repo / 'configs/data_sync/cold_tiers.json', limit=128 * 1024)
    tier_reserve = (number(tier_config.get('reserve_bytes'))
                    if tier_config.get('contract') == 'stockagent.cold_tiers.v1'
                    and tier_config.get('sync_root') == '/srv/stockagent-packed' else None)
    capacity_check = ({'observed_at_utc': iso(stamp), 'available_bytes': available,
                       'cold_tier_reserve_bytes': tier_reserve}
                      if isinstance(cold, dict) and cold.get('state') == 'measured'
                      and stamp is not None and now - STALE_SECONDS <= stamp <= now + 120
                      and available is not None and tier_reserve is not None
                      else None)
    if (isinstance(cold, dict) and cold.get('state') == 'measured'
            and stamp is not None and now-STALE_SECONDS <= stamp <= now+120
            and available is not None and available < 64 * 1024**2):
        for row in rows:
            if row['kind'] in {'migration', 'migration_publication'} and not row.get('complete_evidence'):
                row.update(state='blocked', reason='physical_capacity')
                row['capacity_checked_at_utc'] = iso(stamp)
    old = history.get("tasks", {}) if history.get("contract") == HISTORY_CONTRACT else {}
    old = old if isinstance(old, dict) else {}
    plan = schedules()
    apply_spool_user_pause(rows, metadata(Path('/var/lib/stockagent/cleanup-user-20261107/vast-speculative-spool-pause.json')),
                           plan)
    apply_spool_user_removal(rows,
        metadata(Path('/var/lib/stockagent/cleanup-user-20261109/vast-verification-decommission.json')),
        plan, metadata(Path('/etc/stockagent/pack-verification.json')),
        metadata(Path('/etc/stockagent/pack-compute-spool.json')))
    apply_legacy_vast_backup_user_removal(rows,
        metadata(Path('/var/lib/stockagent/cleanup-user-20261109/vast-verification-decommission.json')), plan)
    locks = owner_locks(config, plan) if any(number(p.get('owner_pid')) for p in plan) else []
    owners = {entry['id']: entry.get('owner_state', 'unknown') for entry in plan}
    # Reuse this cycle's existing local service observations. HTTP never
    # probes a process and an old service heartbeat cannot become a new one.
    if isinstance(runtime, dict) and runtime.get('contract') == 'stockagent.storage_runtime.v1':
        nodes = runtime.get('nodes')
        local = nodes.get('penguin') if isinstance(nodes, dict) else None
        if isinstance(local, dict) and (stamp := timestamp(local.get('observed_at_utc'))) is not None and now - STALE_SECONDS <= stamp <= now + 120:
            services = local.get('services')
            for service in services if isinstance(services, list) else []:
                if isinstance(service, dict) and service.get('id') == 'lakehouse':
                    for kind in ('migration', 'migration_publication', 'migration_dependency'):
                        owners[kind] = choice(service.get('state'),
                            {'active', 'activating', 'inactive', 'failed', 'deactivating'}, 'unknown')
    transport_owners = [owners.get(kind, 'unknown') for kind in ('private_backup', 'registered_backup')]
    owners['transport_retirement'] = next((state for state in transport_owners if state in {'active', 'activating'}),
                                         owners.get('registered_backup', 'unknown'))
    owners['lake_transport_retirement'] = owners.get('transport_gc', 'unknown')
    owners['local_testing_cleanup'] = owners.get('retirement', 'unknown')
    owners['workspace_transport_comparison'] = owners.get('workspace_transport', 'unknown')
    owners['workspace_transport_delta'] = owners.get('workspace_transport', 'unknown')
    owners['workspace_transport_restore'] = owners.get('workspace_transport', 'unknown')
    owners['workspace_partial_name_restore'] = owners.get('workspace_transport', 'unknown')
    owners['workspace_transport_retirement'] = owners.get('workspace_transport', 'unknown')
    owners['workspace_worker_handoff'] = owners.get('workspace_transport', 'unknown')
    owners['d_cold_e_total'] = owners.get('d_cold_e_handoff', 'unknown')
    owners['packed_archive_total'] = owners.get('packed_archive_placement', 'unknown')
    retirement_owners = [owners.get(kind, 'unknown') for kind in ('source_repack_retirement', 'source_local_repack_queue')]
    owners['source_repack_retirement'] = next((state for state in retirement_owners if state in {'active', 'activating'}),
                                             retirement_owners[0] if retirement_owners[0] != 'unknown' else retirement_owners[1])
    owners['source_object_dedup'] = owners['source_repack_retirement']
    owners['ordinary_archive'] = owners['source_repack_retirement']
    owners['offline_repair_D'] = owners.get('handoff_index_D_storage', 'unknown')
    owners['source_replication_pause'] = owners.get('migration', 'unknown')
    owners['catalog_maintenance'] = owners.get('migration', 'unknown')
    owners['whole_root_refresh'] = owners.get('migration', 'unknown')
    # When a long migration has more rows than the response budget, completed
    # receipts must not crowd an active or blocked operation out of the page.
    # Stable sorting preserves the existing order within each group and the
    # omitted count remains explicit in the response.
    if len(rows) > MAX_TASKS:
        rows.sort(key=lambda row: row.get('state') == 'completed')
    updated = {}
    for row in rows[:MAX_TASKS]:
        # This is a fresh metadata read, not a new counter sample, heartbeat
        # from the worker or acceptance proof. Keep the source clock intact.
        row['checked_at_utc'] = iso(now)
        row['owner_state'] = (owners.get('opening_perf_cleanup', 'unknown') if row.get('_opening_perf_timer')
                              else owners.get('handoff_index_prepack', 'unknown') if row.pop('_handoff_prepack_owner', False)
                              else owners.get('prepared_stream_install', 'unknown') if row.get('_priority_install_owner')
                              else owners.get('dedup_candidate', 'unknown') if row.get('_dedup_candidate_owner')
                              else owners.get(row['kind'], 'unknown'))
        if (row['kind'] == 'remote_return' and row['state'] == 'running'
                and row['owner_state'] in {'inactive', 'failed'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'remote_return_failed' if row['owner_state'] == 'failed' else 'staging_queued'
        if (row['kind'] == 'minute_validation_cache_rebind' and row['state'] == 'running'
                and row['owner_state'] not in {'active', 'activating'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'source_or_copy_recheck_required' if row['owner_state'] == 'failed' else 'staging_queued'
        if (row['kind'] == 'offline_repair_archive' and row['state'] == 'running'
                and row['owner_state'] not in {'active', 'activating'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'handoff_source_retry' if row['owner_state'] == 'failed' else 'staging_queued'
        if (row['kind'] == 'offline_replay_pair' and row['state'] == 'running'
                and row['owner_state'] in {'inactive', 'failed'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'handoff_source_retry' if row['owner_state'] == 'failed' else 'staging_queued'
        if (row['kind'] in {'offline_repair_handoff', 'offline_repair_D'} and row['state'] == 'running'
                and row['owner_state'] not in {'active', 'activating'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'handoff_source_retry' if row['owner_state'] == 'failed' else 'staging_queued'
        if (row['kind'] in {'source_local_repack_queue', 'source_repack_retirement', 'source_object_dedup',
                            'private_subset_reuse'}
                and row['state'] == 'running' and row['owner_state'] in {'inactive', 'failed'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = ('subset_error' if row['owner_state'] == 'failed' else 'subset_pending_turn') \
                if row['kind'] == 'private_subset_reuse' else \
                ('repack_temporary_failure' if row['owner_state'] == 'failed' else 'staging_queued')
        if (row['kind'] in {'d_cold_e_handoff', 'd_cold_e_total'} and row['state'] == 'running'
                and row['owner_state'] not in {'active', 'activating'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'd_e_handoff_interrupted' if row['owner_state'] == 'failed' else 'd_e_owner_busy'
        if (row['kind'] == 'handoff_index_coverage' and row['state'] == 'running'
                and row['owner_state'] in {'inactive', 'failed'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'source_or_copy_recheck_required' if row['owner_state'] == 'failed' else 'staging_queued'
        if (row['kind'] == 'workspace_partial_name_restore' and row['state'] == 'running'
                and row['owner_state'] not in {'active', 'activating'}):
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'source_or_copy_recheck_required' if row['owner_state'] == 'failed' else 'staging_queued'
        if row['kind'] == 'workspace_cold_inventory' and row['state'] == 'running' \
                and row['owner_state'] not in {'active', 'activating'}:
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'source_or_copy_recheck_required' if row['owner_state'] == 'failed' else 'staging_queued'
        if row['kind'] in {'futures_725_projection', 'futures_765_projection'} and row['state'] == 'running' \
                and row['owner_state'] not in {'active', 'activating'}:
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'source_or_copy_recheck_required' if row['owner_state'] == 'failed' else 'staging_queued'
        if row['kind'] == 'workspace_source_handoff' and row['state'] == 'running' \
                and row['owner_state'] not in {'active', 'activating'}:
            # A fresh file alone cannot prove the owning one-shot survived.
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'source_or_copy_recheck_required' if row['owner_state'] == 'failed' else 'staging_queued'
        if row['kind'] == 'workspace_source_staging':
            # A shared queue unit can be working on a different source. Only
            # the exact source lock can identify this row's active owner.
            row['owner_state'] = 'active' if row.pop('_original_staging_owner_locked', False) else 'unknown'
        if row['kind'] == 'local_testing_cleanup' and row['state'] == 'running' and row['owner_state'] == 'inactive':
            # The same executor may still be owned by the initial bounded
            # handoff run. An idle timer unit does not establish an idle job.
            row['owner_state'] = 'unknown'
        if row['kind'] == 'd_testing_cleanup' and row['state'] == 'running' and row['owner_state'] not in {'active', 'activating'}:
            row['state'] = 'blocked' if row['owner_state'] == 'failed' else 'waiting'
            row['reason'] = 'dependency' if row['owner_state'] == 'failed' else 'staging_queued'
        row["eta"], updated[row["id"]] = estimate(row, old.get(row["id"], {}), now=now)
        apply_legacy_archive_deadline(row)
        apply_refresh_priority_boundary(row, now=now, repo=repo)
        row.pop("generation", None)  # Private generation binding stays server-side.
    # Kernel PIDs help identify a wrapper's child locally; never persist them
    # into this observer's shared/public snapshot.
    for entry in plan:
        entry.pop('owner_pid', None)
    audit_path = Path(options.get('boot_audit_receipt', '/var/lib/stockagent/boot-recovery/latest.json'))
    from stockagent.data_sync.archive_plan import from_config
    from stockagent.data_sync.stream_repack import TARGET_BYTES
    try:
        archive_policy = from_config(metadata(repo/'configs/columnar_storage.json'),
            metadata(repo/'configs/data_sync/packed_object_placement.json'), pack_target_bytes=TARGET_BYTES)
    except (TypeError, ValueError, KeyError):
        archive_policy = None
    return ({"contract": CONTRACT, "observed_at_utc": iso(now), "tasks": rows[:MAX_TASKS],
             'archive_policy': archive_policy,
             "cold_capacity_check": capacity_check,
             "omitted_task_count": max(0, len(rows) - MAX_TASKS),
             "schedules": plan, "owner_locks": locks,
             "public_cold_publication": public_cold_publication_observation(
                 repo / 'artifacts/data_refresh/tw_public/cold_publish/latest.json'),
             "boot_audit": boot_audit_observation(audit_path)},
            {"contract": HISTORY_CONTRACT, "tasks": updated})


def public_forecast(value, *, now, stale=False, blocked=False):
    """Publish empirical timings, not raw owner strings, identities or paths."""
    value = value if isinstance(value, dict) else {}
    scope = choice(value.get('scope'), {'current_delivery', 'ready_queue'})
    state = choice(value.get('state'), {'estimated', 'sampling', 'partial', 'idle', 'overdue'}, 'unknown')
    stamp = timestamp(value.get('generated_at_utc'))
    end = timestamp(value.get('completion_at_utc'))
    if stale or (stamp is not None and (stamp > now+120 or now-stamp > STALE_SECONDS)):
        state = 'stale'
    elif blocked:
        state = 'blocked'
    elif state in {'estimated', 'overdue'}:
        if (stamp is None or end is None or value.get('basis') != 'recent_complete_workflows'
                or scope is None or not number(value.get('sample_count'))):
            state = 'unknown'
        elif end <= now:
            state = 'overdue'
    valid = state == 'estimated'
    earliest, latest = (timestamp(value.get(field)) for field in
                        ('fastest_completion_at_utc', 'slowest_completion_at_utc'))
    if valid and (earliest is None or latest is None or not earliest <= end <= latest):
        valid, state = False, 'unknown'
    return {'state': state, 'scope': scope,
            'basis': 'recent_complete_workflows' if value.get('basis') == 'recent_complete_workflows' else None,
            'confidence': choice(value.get('confidence'), {'low', 'medium'}),
            'sample_count': number(value.get('sample_count')),
            'remaining_seconds': math.ceil(end-now) if valid else None,
            'completion_at_utc': iso(end) if valid else None,
            'previous_completion_at_utc': iso(end) if state == 'overdue' else None,
            'fastest_seconds': math.ceil(max(0, earliest-now)) if valid else None,
            'slowest_seconds': math.ceil(max(0, latest-now)) if valid else None,
            'estimated_jobs': number(value.get('estimated_jobs')),
            'unknown_jobs': number(value.get('unknown_jobs')),
            'retry_jobs': number(value.get('retry_jobs'))}


def public_status(value, *, now=None) -> dict:
    """A second whitelist at the public gateway; no receipt strings or paths."""
    now = time.time() if now is None else now
    value = value if isinstance(value, dict) and value.get("contract") == CONTRACT else {}
    generated = timestamp(value.get("observed_at_utc"))
    observer_stale = generated is None or generated > now + 120 or now - generated > STALE_SECONDS
    rows = []
    raw = value.get("tasks", [])
    for item in raw[:MAX_TASKS] if isinstance(raw, list) else []:
        if not isinstance(item, dict) or not choice(item.get("kind"), LABELS) or not re.fullmatch(r"[a-f0-9]{24}", str(item.get("id", ""))):
            continue
        kind = item["kind"]
        phase = choice(item.get("phase"), PHASES, "working")
        state = choice(item.get("state"), {"running", "waiting", "blocked", "completed", "unknown"}, "unknown")
        proof = item.get("complete_evidence") is True and state == "completed"
        if state == "completed" and not proof:
            state = "unknown"
        stamp = timestamp(item.get("observed_at_utc"))
        activity = timestamp(item.get("activity_at_utc"))
        checked = timestamp(item.get('checked_at_utc'))
        progress_stale = stamp is None or stamp > now + 120 or now - stamp > STALE_SECONDS
        if (kind == 'vast_spool' and state == 'waiting' and phase == 'waiting_new_sealed_packs'
                and stamp is not None and stamp <= now + 120
                and activity is not None and 0 <= now - activity <= STALE_SECONDS):
            # A completed candidate set has no running counter to stall. Keep
            # its discovery/progress clocks, while the same queue's heartbeat
            # proves it is alive serving another explicitly enrolled scope.
            progress_stale = False
        # Older snapshots have no per-row observation clock. Preserve their
        # conservative behavior until the original observer is upgraded.
        check_stale = (checked is None or checked > now + 120 or generated is None
                       or checked > generated + 120 or now - checked > STALE_SECONDS)
        stale = observer_stale or (check_stale if 'checked_at_utc' in item else progress_stale)
        if not stale and kind in {'vast_spool', 'bulk_return'} and phase == 'spool_removed_user':
            progress_stale = False  # The confirmed absence has no running counter.
        if (not stale and kind == 'provider_workspace' and state == 'waiting'
                and phase == 'workspace_handoff' and item.get('reason') == 'provider_workspace_handoff'):
            # A fresh queue observation is not a worker heartbeat. This next
            # phase has no counter to expire; keep its source clock absent.
            progress_stale = False
        if (not stale and kind == 'workspace_source_handoff' and state == 'waiting'
                and phase == 'workspace_handoff'
                and item.get('reason') in {'staging_queued', 'tw_day_trade_priority_window'}):
            # A newly enrolled, scheduled handoff has not produced a work
            # receipt yet; the missing future receipt is not a read failure.
            progress_stale = False
        if not stale and kind == 'provider_originals' and state == 'waiting':
            progress_stale = False  # Fresh root observation, no retirement worker counter yet.
        if not stale and kind == 'offline_replay_pair' and state == 'waiting' and phase == 'offline_replay_wait':
            progress_stale = False  # A queued future transaction has not emitted its first receipt.
        if (not stale and kind == 'workspace_partial_name_restore' and state == 'waiting'
                and phase == 'partial_name_pending' and item.get('reason') == 'staging_queued'):
            progress_stale = False  # The first worker receipt does not exist yet.
        counters = {k: number(item.get(k)) for k in ("completed_bytes", "total_bytes", "completed_items", "total_items")}
        for metric in ("bytes", "items"):
            a, b = counters["completed_" + metric], counters["total_" + metric]
            if a is not None and b is not None and a > b:
                counters["completed_" + metric] = None
        eta_raw = item.get("eta") if isinstance(item.get("eta"), dict) else {}
        metric = ("bytes" if (kind in {'workspace_controls', 'workspace_transport_retirement', 'provider_workspace',
                                       'd_cold_e_handoff', 'd_cold_e_total'}
                              or (kind in {'handoff_source_repack', 'handoff_index_archive', 'offline_repair_archive'}
                                  and phase == 'handoff_source_packing')
                              or (kind in {'handoff_private_d', 'handoff_index_D_storage', 'offline_repair_D'}
                                  and phase == 'handoff_source_D_installing')
                              or (kind == 'source_local_repack_queue'
                                  and phase in {'source_local_E_packing', 'source_local_D_installing'}))
                              and counters['total_bytes'] else
                  "items" if eta_raw.get('unit') == 'items' and counters['total_items'] else
                  "bytes" if counters["total_bytes"] else "items")
        a, b = counters["completed_" + metric], counters["total_" + metric]
        pct = min(99.99 if a < b else 100, round(a / b * 100, 2)) if a is not None and b else None
        if (kind == 'source_local_repack_queue' and state != 'completed' and pct == 100
                and phase not in {'source_local_E_prepared', 'source_local_E_prepared_ready', 'source_local_E_local_ready'}):
            # An active source read can finish before its package is sealed.
            # A sealed E stage is complete in its own scope, while its task
            # remains waiting/blocked until D commit provides evidence.
            pct = 99.99
        eta_state = choice(eta_raw.get("state"), {"unknown", "sampling", "estimated", "completed", "blocked", "stale", "stalled", "finalizing"}, "unknown")
        reason = choice(eta_raw.get("reason"), REASONS, "no_counter")
        if proof:
            eta_state, reason = "completed", None
        elif (kind == 'vast_spool' and phase == 'spool_paused_user'
              and item.get('reason') == 'user_paused_extra_verification'):
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', 'user_paused_extra_verification')
        elif (kind in {'vast_spool', 'bulk_return'} and phase == 'spool_removed_user'
              and item.get('reason') == 'user_removed_vast_verification'):
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', 'user_removed_vast_verification')
        elif (kind in {'migration', 'migration_dependency'} and state == 'waiting'
              and phase == 'recovery_deferred_user_migration'
              and item.get('reason') == 'recovery_deferred_user_migration'):
            # A freshly observed intentional pause has no running counter and
            # cannot have a completion ETA; it is not a stalled receipt.
            eta_state, reason = (('stale', 'receipt_stale') if stale
                                 else ('unknown', 'recovery_deferred_user_migration'))
        elif (kind == 'remote_return' and state == 'waiting' and phase == 'recovery_deferred_user_migration'
              and item.get('reason') == 'recovery_deferred_user_migration'):
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', 'recovery_deferred_user_migration')
        elif kind == 'edge_reader_recovery' and state == 'waiting' and item.get('reason') in {
                'edge_reader_monitoring', 'edge_reader_disabled', 'edge_reader_trading_deferred'}:
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', item['reason'])
        elif kind == 'd_mount_repair' and state == 'waiting':
            mount_reason = ('d_mount_recurring' if item.get('reason') == 'd_mount_healthy'
                            else choice(item.get('reason'), REASONS, 'd_mount_deferred'))
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', mount_reason)
        elif (kind == 'workspace_source_handoff' and state == 'waiting'
              and phase == 'workspace_handoff'
              and item.get('reason') in {'staging_queued', 'tw_day_trade_priority_window'}):
            eta_state, reason = (('stale', 'receipt_stale') if stale
                                 else ('unknown', item['reason']))
        elif (kind == 'workspace_partial_name_restore' and state == 'waiting'
              and phase == 'partial_name_pending' and item.get('reason') == 'staging_queued'):
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', 'staging_queued')
        elif kind == 'provider_originals' and state == 'waiting':
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', 'provider_originals_pending')
        elif kind == 'offline_replay_pair' and state == 'waiting' and phase == 'offline_replay_wait':
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', 'staging_queued')
        elif (kind == 'provider_workspace' and state == 'blocked'
              and item.get('reason') in PROVIDER_FAILURE_REASONS):
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('blocked', item['reason'])
        elif (kind == 'provider_workspace' and phase == 'workspace_handoff'
              and state == 'waiting' and item.get('reason') == 'provider_workspace_handoff'):
            # This planned stage has no worker counter yet. Its prerequisite
            # is a real closed copy, checked in this observation; do not call
            # the absent future handoff receipt a broken metadata read.
            eta_state, reason = ('stale', 'receipt_stale') if stale else ('unknown', 'provider_workspace_handoff')
        elif (kind == 'workspace_source_staging' and phase == 'workspace_seed_copy' and not stale
              and activity is not None and 0 <= now - activity <= STALE_SECONDS
              and isinstance(item.get('metrics'), dict)
              and counters['completed_items'] is None
              and number(item['metrics'].get('staging_e_write_bytes')) is not None):
            # The process is alive and its E I/O is observable, but the seed
            # has no durable per-file counter from which to estimate a finish.
            eta_state, reason = 'unknown', 'no_counter'
        elif stamp is None:
            eta_state, reason = "unknown", "metadata_unavailable"
        elif stale:
            eta_state, reason = "stale", "receipt_stale"
        elif stamp > now + 120:
            eta_state, reason = "unknown", "clock_ahead"
        elif state == "blocked":
            eta_state, reason = "blocked", choice(item.get("reason"), REASONS, "dependency")
        elif progress_stale:
            eta_state, reason = "stale", "progress_pending"
        elif state == "waiting":
            # Do not present a cached running-rate projection as a finish
            # time while the owner is waiting on an unmeasured dependency.
            eta_state, reason = ("unknown", "upstream_publication") if (
                kind == 'vast_spool' and phase == 'waiting_new_sealed_packs') else ("unknown", "progress_pending")
        elif eta_state == "completed":
            eta_state, reason = "finalizing", "finalizing"
        content = item.get('eta_work')
        content_scope = eta_raw.get('scope') == 'remaining_source_content' and isinstance(content, dict)
        # The byte progress bar and the slower file-tail ETA can have different
        # units. Validate/rate-label the ETA against its own actual counters.
        estimate_metric = choice(eta_raw.get('unit'), {'bytes', 'items'}, metric)
        estimate_counters = content if content_scope else counters
        estimate_done = number(estimate_counters.get('completed_' + estimate_metric))
        estimate_total = number(estimate_counters.get('total_' + estimate_metric))
        estimated = eta_state == "estimated" and estimate_done is not None and estimate_total and estimate_done < estimate_total
        if eta_state == "estimated" and not estimated:
            eta_state, reason = "unknown", "no_counter"
        completion = timestamp(eta_raw.get("completion_at_utc"))
        # A missed estimate is an observation failure, not a negative countdown.
        if estimated and (completion is None or completion <= now):
            # An expired estimate is not evidence that the worker stopped.
            # Actual lack of progress is separately reported by the observer.
            estimated, eta_state, reason = False, "sampling", "forecast_overdue"
        elif estimated:
            reason = None
        age = max(0., now - generated) if generated is not None else 0.
        def remaining_bound(field):
            seconds = number(eta_raw.get(field))
            return math.ceil(max(0., seconds-age)) if estimated and seconds is not None else None
        eta_scope = ('remaining_source_content' if content_scope else
                     'rsync_filelist_estimate' if kind == 'workspace_source_staging'
                     and phase == 'workspace_seed_copy' and eta_raw.get('scope') == 'rsync_filelist_estimate'
                     else 'current_phase_only')
        eta = {"state": eta_state, "reason": reason, "message": REASONS.get(reason),
               "scope": eta_scope,
               "remaining_seconds": math.ceil(completion-now) if estimated else 0 if proof else None,
               "fastest_seconds": remaining_bound("fastest_seconds"),
               "slowest_seconds": remaining_bound("slowest_seconds"),
               "completion_at_utc": iso(completion) if estimated else None,
               "rate_per_second": finite(eta_raw.get("rate_per_second")) if estimated else None, "unit": estimate_metric}
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        metrics = {k: number(metrics.get(k)) for k in METRICS if number(metrics.get(k)) is not None}
        if not proof:
            metrics.pop("reclaimed_allocated_bytes", None)
        batch = number(item.get("batch_number"))
        control_names = {1: '台股公開資料', 2: '台指期貨', 3: 'OpenBB', 4: 'FinLab'}
        direct_minute_coverage = (kind in {'handoff_index_archive', 'handoff_index_D_storage'}
                                  and item.get('subject') == 'tw-minute-source-workspace')
        label = (f"WSL 安全回收 · 第 {batch} 輪" if kind == "retirement" and batch else
                 f"WSL → E · {control_names[batch]}準備副本" if kind == 'workspace_controls' and batch in control_names else
                 ('C 舊副本回收' if kind == 'handoff_index_archive' else 'D 既有版本') if direct_minute_coverage else
                 LABELS[kind])
        if choice(item.get("subject"), SUBJECTS):
            label += " · " + SUBJECTS[item["subject"]]
            if item.get('subject') in {'other-cold-dataset', 'tw-forward-research', 'tw-daytrade-research-artifact',
                                       'fred-history-copy', 'openbb-state', 'taifex-state', 'futures-state',
                                       'news-fulltext-state', 'news-training-state'}:
                label += ' #' + item['id'][:8]
        lease = item.get('receiver_readiness')
        lease = ({'state': choice(lease.get('state'), {'expired', 'future'}),
                  **{k: iso(timestamp(lease.get(k))) for k in ('observed_at_utc', 'valid_until_utc', 'checked_at_utc')}}
                 if kind == 'private_backup' and isinstance(lease, dict)
                    and choice(lease.get('state'), {'expired', 'future'}) else None)
        previous_failure = (choice(item.get('previous_source_failure'), PREVIOUS_SOURCE_FAILURES)
                            if kind in {'migration_dependency', 'catalog_export'} else None)
        # The native receipt describes a finished, failed offline maintenance
        # attempt. Its proof must remain visible, but it is not an active
        # prerequisite of the current dataset migration or its ETA.
        historical_only = (kind == 'workspace_c_compaction' and state == 'blocked'
                           and phase == 'windows_storage_failed'
                           and item.get('reason') == 'windows_storage_failed')
        rows.append({"id": item["id"], "kind": kind, "node_id": choice(item.get("node_id"), {"penguin", "nas", "lab203", "penguin_vast", "vastai1T"}, "penguin"),
            "label": label, "state": state, "phase": phase, "phase_label": PHASES[phase],
            "historical_only": historical_only,
            "scope": ('服務來源已切至 E；沿用逐項比對相符並固定的 D 封裝，回收 C 舊副本。此步不代表獨立還原或 NAS 驗收。'
                      if direct_minute_coverage else SCOPES[kind]),
            "complete_evidence": proof, "stale": stale, "checked_at_utc": iso(checked),
            "progress_stale": progress_stale and not proof,
            "owner_state": choice(item.get('owner_state'), {'active', 'activating', 'inactive', 'failed', 'deactivating'}, 'unknown') if not stale else 'unknown',
            "observed_at_utc": iso(stamp), "activity_at_utc": iso(activity),
            "diagnosis_at_utc": iso(timestamp(item.get('diagnosis_at_utc'))) if kind in {'receiver', 'lake_relay'} else None,
            "last_scan_observed_at_utc": iso(timestamp(item.get('last_scan_observed_at_utc'))) if kind == 'transport' else None,
            "notification_observation": choice(item.get('notification_observation'), {'scan', 'retry'}) if kind == 'transport' else None,
            "inventory_at_utc": iso(timestamp(item.get('inventory_at_utc'))),
            "reason_code": choice(item.get('reason'), REASONS),
            "failure_observed_at_utc": (iso(timestamp(item.get('failure_observed_at_utc')))
                if (kind == 'workspace_source_handoff' and item.get('reason') in {
                        'namespace_readback_mismatch', 'source_or_copy_recheck_required'}
                    or kind == 'provider_workspace' and item.get('reason') in PROVIDER_FAILURE_REASONS
                    or kind == 'catalog_export' and previous_failure)
                and timestamp(item.get('failure_observed_at_utc')) is not None
                and timestamp(item['failure_observed_at_utc']) <= now + 120 else None),
            "reason": REASONS.get(choice(item.get("reason"), REASONS)), "progress": {**counters, "metric": metric, "percent": pct},
            "previous_source_failure": ({"code": previous_failure,
                "message": PREVIOUS_SOURCE_FAILURES[previous_failure]}
                if previous_failure
                else None),
            "eta": eta, "metrics": metrics, 'receiver_readiness': lease,
            "next_attempt_at_utc": iso(timestamp(item.get("next_attempt_at_utc"))),
            'owner_deadline_at_utc': (iso(timestamp(item.get('owner_deadline_at_utc')))
                if kind == 'handoff_index_archive' and state == 'running'
                and item.get('reason') == 'handoff_archive_legacy_timeout'
                and timestamp(item.get('owner_deadline_at_utc')) is not None
                and now-86400 <= timestamp(item['owner_deadline_at_utc']) <= now+7*86400 else None),
            'started_at_utc': iso(timestamp(item.get('started_at_utc'))),
            'completion_forecast': public_forecast(item.get('completion_forecast'), now=now,
                stale=stale or progress_stale, blocked=state == 'blocked'),
            'queue_forecast': public_forecast(item.get('queue_forecast'), now=now,
                stale=stale or progress_stale and not proof, blocked=state == 'blocked')})
    schedule_labels = SCHEDULE_LABELS
    plan = value.get("schedules", [])
    plan = plan if isinstance(plan, list) else []
    public_plan = []
    for entry in plan[:len(SCHEDULE_LABELS)]:
        if not isinstance(entry, dict) or not choice(entry.get("id"), schedule_labels):
            continue
        nxt = timestamp(entry.get("next_check_at_utc"))
        guard_end = timestamp(entry.get('work_not_before_utc'))
        guarded = (not observer_stale and generated is not None
            and entry['id'] in PRIORITY_GUARDED_SCHEDULES
            and entry.get('window_guard_reason') == 'tw_day_trade_priority_window'
            and entry.get('owner_result') == 'exec-condition' and entry.get('owner_running') is False
            and guard_end is not None and now < guard_end <= generated + 86400)
        public_plan.append({"id": entry["id"], "label": schedule_labels[entry["id"]],
            "mode": choice(entry.get('mode'), {'timer', 'continuous'}, 'timer'),
            "registered": entry.get("registered") is True, "stale": observer_stale,
            "timer_active": entry.get("timer_active") if type(entry.get("timer_active")) is bool else None,
            "owner_running": entry.get("owner_running") if type(entry.get("owner_running")) is bool else None,
            "owner_state": choice(entry.get('owner_state'), OWNER_STATES, 'unknown') if not observer_stale else 'unknown',
            "owner_result": choice(entry.get('owner_result'),
                {'success', 'exit-code', 'exec-condition', 'signal', 'core-dump', 'timeout', 'watchdog', 'resources', 'start-limit-hit'}, 'unknown'),
            "exit_code": number(entry.get('exit_code')),
            "window_guard_reason": 'tw_day_trade_priority_window' if guarded else None,
            "work_not_before_utc": iso(guard_end) if guarded else None,
            "next_check_at_utc": iso(nxt) if not observer_stale and nxt and nxt >= now else None,
            "scheduled_check_at_utc": iso(nxt) if not observer_stale else None,
            "check_due": bool(not observer_stale and nxt and nxt < now),
            "last_check_at_utc": iso(timestamp(entry.get("last_check_at_utc")))})
    for row in rows:
        associated = [p for p in public_plan if p['id'] in TASK_SCHEDULES.get(row['kind'], ())]
        if row['kind'] == 'local_testing_cleanup' and any(
                isinstance(item, dict) and item.get('id') == row['id']
                and item.get('_one_off_without_timer') is True for item in raw):
            associated = []
        if row['kind'] == 'local_testing_cleanup' and any(
                isinstance(item, dict) and item.get('id') == row['id']
                and item.get('_opening_perf_timer') is True for item in raw):
            associated = [p for p in public_plan if p['id'] == 'opening_perf_cleanup']
        if row['kind'] == 'workspace_source_handoff' and row.get('reason_code') in {
                'source_handoff_unenrolled', 'source_handoff_root_changed'}:
            # The timer is real, but it does not discover this unenrolled
            # source; a fresh monitor observation is not automatic cutover.
            associated = []
        current = not observer_stale and not row['stale']
        enabled = current and any(p['timer_active'] is True or
            p['mode'] == 'continuous' and p['owner_running'] is True for p in associated)
        if row['kind'] in {'workspace_source_staging', 'workspace_symlink_closure', 'catalog_maintenance',
                           'whole_root_refresh'}:
            # The queue owner can be copying a different dataset. Only this
            # item's bound lock and fresh IO establish its own live work.
            activity = timestamp(row.get('activity_at_utc'))
            running = bool(current and row['state'] == 'running' and row['owner_state'] == 'active' and activity is not None
                           and 0 <= now - activity <= STALE_SECONDS)
        else:
            running = current and any(p['owner_running'] is True for p in associated)
        checks = [timestamp(p['next_check_at_utc']) for p in associated if p['next_check_at_utc']]
        row['continuation'] = {
            'automatic': bool(enabled), 'owner_running': bool(running),
            'schedule_labels': [p['label'] for p in associated],
            'next_check_at_utc': iso(min(checks)) if checks else None,
            'check_due': current and any(p['check_due'] for p in associated),
            'mode': 'running' if running else 'scheduled' if enabled else 'unconfirmed',
        }
        eligible = [p for p in associated if p['timer_active'] is True or p['owner_running'] is True]
        if (current and not running and eligible and all(p['mode'] == 'timer'
                and p['timer_active'] is True and p['window_guard_reason'] == 'tw_day_trade_priority_window'
                and p['work_not_before_utc'] for p in eligible)
                and set(TASK_SCHEDULES.get(row['kind'], ())).issubset({p['id'] for p in associated})
                and all(p['timer_active'] is not None for p in associated if p['mode'] == 'timer')):
            row['continuation']['window_guard_reason'] = 'tw_day_trade_priority_window'
            row['continuation']['work_not_before_utc'] = iso(max(
                timestamp(p['work_not_before_utc']) for p in eligible))
        remote = next((item.get('receiver_schedule') for item in raw
                       if isinstance(item, dict) and item.get('id') == row['id']), None) if row['kind'] == 'receiver' else None
        remote_stamp = timestamp(remote.get('observed_at_utc')) if isinstance(remote, dict) else None
        if (current and remote_stamp is not None and 0 <= now - remote_stamp <= STALE_SECONDS
                and remote.get('automatic') is True and type(remote.get('owner_running')) is bool):
            next_remote = timestamp(remote.get('next_check_at_utc'))
            row['continuation'] = {'automatic': True, 'owner_running': remote['owner_running'],
                'schedule_labels': ['lab203 原備份（接收端觀測）'],
                'next_check_at_utc': iso(next_remote) if next_remote is not None and next_remote >= now else None,
                'check_due': False, 'mode': 'running' if remote['owner_running'] else 'scheduled'}
        # This clock schedules a new metadata observation, never a new worker
        # heartbeat, acceptance, expensive retry or promised completion.
        checked = timestamp(row['checked_at_utc'])
        row['next_observation_at_utc'] = iso(checked+60) if current and checked is not None else None
    public_locks = []
    raw_locks = value.get('owner_locks')
    for entry in raw_locks[:len(LOCK_LABELS)] if isinstance(raw_locks, list) else []:
        if not isinstance(entry, dict) or not choice(entry.get('id'), LOCK_LABELS):
            continue
        waiting = entry.get('waiting')
        public_locks.append({'id': entry['id'], 'label': LOCK_LABELS[entry['id']],
            'held': entry.get('held') is True if not observer_stale else None,
            'holder_label': SCHEDULE_LABELS.get(choice(entry.get('holder'), SCHEDULE_LABELS)),
            'waiter_count': number(entry.get('waiter_count')) if not observer_stale else None,
            'waiting_labels': [SCHEDULE_LABELS[p] for p in waiting[:len(SCHEDULE_LABELS)] if choice(p, SCHEDULE_LABELS)]
                if isinstance(waiting, list) and not observer_stale else [], 'stale': observer_stale})
    attention = []
    if observer_stale:
        attention.append({'id': 'observer', 'level': 'blocked', 'label': '每分鐘觀測尚未更新',
                          'message': '保留上次工作與收據時間，需檢查原觀測服務；目前不能確認即時運行狀態。'})
    else:
        for entry in public_plan:
            if entry['owner_state'] == 'failed' or entry['owner_result'] not in {'success', 'unknown'}:
                audit = value.get('boot_audit') if entry['id'] == 'boot_recovery' else None
                audit = audit if isinstance(audit, dict) else {}
                audit_time = timestamp(audit.get('observed_at_utc'))
                codes = audit.get('reason_codes')
                codes = codes if isinstance(codes, list) else []
                reasons = [BOOT_AUDIT_FAILURES[code] for code in codes[:len(BOOT_AUDIT_FAILURES)]
                           if choice(code, BOOT_AUDIT_FAILURES)]
                audit_detail = (' 最近一次稽核：' + '；'.join(reasons) + '。'
                                if reasons and audit_time is not None and now - 720 <= audit_time <= now + 120 else '')
                if entry['id'] == 'public_cold_publish':
                    receipt = value.get('public_cold_publication')
                    receipt = receipt if isinstance(receipt, dict) else {}
                    audit_time = timestamp(receipt.get('observed_at_utc'))
                    codes = receipt.get('reason_codes')
                    reasons = [PUBLICATION_FAILURES[code] for code in codes[:len(PUBLICATION_FAILURES)]
                               if choice(code, PUBLICATION_FAILURES)] if isinstance(codes, list) else []
                    # A failed owner can retain its last veto for hours. Name
                    # that receipt's actual time, not this minute's heartbeat;
                    # never imply that it diagnoses a new running invocation.
                    audit_detail = (' 最近一次發布收據：' + '；'.join(reasons) + '。'
                        if reasons and audit_time is not None and audit_time <= now + 120 else '')
                attention.append({'id': entry['id'], 'level': 'blocked', 'label': entry['label'],
                    'message': ('本輪已在重試；上次執行未成功。' if entry['owner_running'] else '原執行者回報失敗。')
                        + (' 原排程仍啟用，下一次時間見完整排程；需核對失敗收據。' if entry['timer_active']
                           else ' 需檢查原服務及重試排程；目前沒有確認的自動接續時間。') + audit_detail,
                    'last_report_at_utc': iso(audit_time) if audit_detail else None})
            elif entry['registered'] and ((entry['mode'] == 'timer' and entry['timer_active'] is False)
                    or (entry['mode'] == 'continuous' and entry['owner_running'] is False)):
                attention.append({'id': entry['id'], 'level': 'blocked', 'label': entry['label'],
                    'message': '原排程或常駐執行者未啟用；需核對是否為預期停用，不能保證自動接續。'})
        capacity = value.get('cold_capacity_check')
        if isinstance(capacity, dict):
            capacity_stamp = timestamp(capacity.get('observed_at_utc'))
            free = number(capacity.get('available_bytes'))
            reserve = number(capacity.get('cold_tier_reserve_bytes'))
            if (capacity_stamp is not None and now - STALE_SECONDS <= capacity_stamp <= now + 120
                    and free is not None and reserve is not None
                    and 0 < reserve <= 1024**4 and free < reserve):
                gib = lambda n: f'{n / 1024**3:.2f}'
                message = (f'D 槽可用 {gib(free)} GiB；冷區壓縮整理設定預留 {gib(reserve)} GiB，'
                           f'距預留底線尚差 {gib(reserve-free)} GiB。')
                message += (' 這是冷區整理的容量准入觀測，不代表已開始的 D 讀取還原停止；'
                            'E 傳輸及其他備份依各自所在磁碟、原 owner 與實際收據判定。')
                attention.append({'id': 'd_cold_tier_capacity', 'level': 'blocked', 'label': 'D 冷區整理容量不足',
                                  'message': message, 'last_report_at_utc': iso(capacity_stamp)})
        for row in rows:
            if row['complete_evidence'] or row['historical_only']:
                continue
            if row['kind'] == 'catalog_export' and row['previous_source_failure']:
                attention.append({'id': row['id'] + '-previous-failure', 'level': 'waiting',
                    'label': row['label'], 'message': row['previous_source_failure']['message'],
                    'last_report_at_utc': row['failure_observed_at_utc']})
            reason_code = row['reason_code'] if row['state'] == 'blocked' else row['eta']['reason']
            if row['state'] == 'blocked' or reason_code in {'progress_pending', 'metadata_unavailable'}:
                attention.append({'id': row['id'], 'level': 'blocked' if row['state'] == 'blocked' else 'waiting',
                    'label': row['label'], 'message': FOLLOW_UP.get(reason_code, REASONS.get(reason_code, REASONS['dependency'])),
                    'next_attempt_at_utc': row['next_attempt_at_utc'], 'last_report_at_utc': row['observed_at_utc']})
        for entry in public_locks:
            if entry['held'] and entry['waiter_count']:
                attention.append({'id': entry['id'], 'level': 'waiting', 'label': entry['label'],
                    'message': f"{entry['waiter_count']} 個程序正在等待原執行者"
                        + (f"（{entry['holder_label']}）" if entry['holder_label'] else '')
                        + '釋放鎖；這是即時鎖等待觀測，尚不能認定死鎖。保留原 owner 自動接續。'})
    flow = []
    visible_ids = {row['id'] for row in rows}
    staged_subjects = {item.get('subject') for item in raw[:MAX_TASKS]
                       if isinstance(item, dict) and item.get('kind') == 'workspace_source_staging'
                       and item.get('id') in visible_ids and choice(item.get('subject'), SUBJECTS)} if isinstance(raw, list) else set()
    handoff_subjects = {item.get('subject') for item in raw[:MAX_TASKS]
                        if isinstance(item, dict) and item.get('kind') in {'workspace_source_handoff', 'offline_repair_handoff'}
                        and item.get('id') in visible_ids and choice(item.get('subject'), SUBJECTS)} if isinstance(raw, list) else set()
    unassessed_handoffs = len(staged_subjects - handoff_subjects)
    from stockagent.data_sync import archive_plan, archive_schedule
    local_task_ids = set()
    for identity, label, dependencies, kind, condition in FLOW:
        matching = [r for r in rows if r['kind'] == kind
                    or kind == 'retirement' and r['kind'] == 'provider_originals'
                    or kind in {'workspace_source_handoff', 'retirement'} and r['kind'] == 'offline_repair_handoff'
                    or kind == 'receiver' and r['kind'] == 'lake_relay'
                    or kind == 'migration_publication' and r['kind'] == 'migration_dependency'
                    or kind == 'source_repack_retirement' and r['kind'] == 'ordinary_archive'
                    or kind == 'transport_retirement' and r['kind'] == 'lake_transport_retirement']
        status = ('unknown' if not matching or observer_stale else
                  'completed' if all(r['complete_evidence'] for r in matching) else
                  'blocked' if any(r['state'] == 'blocked' for r in matching) else
                  'running' if any(r['state'] == 'running' for r in matching) else 'waiting')
        if identity in archive_plan.LOCAL_STAGES:
            local_task_ids.update(row['id'] for row in matching)
        if (identity == 'e_handoff' and not observer_stale and unassessed_handoffs
                and status not in {'running', 'blocked'}):
            # A timer only advances the registered handoffs.  Once those are
            # idle, uncovered source roots need an explicit consumer/owner
            # assessment; waiting alone cannot make them eligible.
            status = 'blocked'
        if identity == 'retire' and not observer_stale and unassessed_handoffs and status == 'completed':
            # A completed old retirement cohort cannot finish C source
            # handoff while staged sources have no reviewed cutover yet.
            status = 'blocked'
        pending_stage = [row for row in matching if not row['complete_evidence']]
        known_steps = [timestamp(row['eta']['completion_at_utc']) for row in pending_stage
                       if row['eta']['state'] == 'estimated' and not row['stale'] and not row['progress_stale']]
        known_steps = [value for value in known_steps if value is not None]
        checks = [timestamp(row['continuation']['next_check_at_utc']) for row in pending_stage
                  if row['continuation']['next_check_at_utc']]
        checks = [value for value in checks if value is not None and value >= now]
        flow.append({'id': identity, 'label': label, 'depends_on': list(dependencies),
                     'branch': archive_plan.branch(identity),
                     'required_for_local_migration': identity != 'recover' and identity not in archive_plan.REPLICATION_STAGES,
                     'state': status, 'condition': condition, 'scope': 'enrolled_current_work_only',
                     'completed_tasks': len(matching) - len(pending_stage), 'pending_tasks': len(pending_stage),
                     'blocked_tasks': sum(row['state'] == 'blocked' for row in pending_stage),
                     'estimated_current_steps': len(known_steps),
                     'unestimated_tasks': len(pending_stage) - len(known_steps),
                     'unassessed_source_count': unassessed_handoffs if identity in {'e_handoff', 'retire'} and not observer_stale else None,
                     'unassessed_source_labels': sorted(SUBJECTS[item] for item in staged_subjects - handoff_subjects)
                         if identity == 'e_handoff' and not observer_stale else [],
                     'known_current_step_completion_at_utc': iso(max(known_steps)) if known_steps else None,
                     'next_check_at_utc': iso(min(checks)) if checks else None})
    pending = [row for row in rows if not row['complete_evidence'] and not row['historical_only']
               and row['phase'] != 'waiting_accepted_transport'
               and not (row['kind'] == 'd_mount_repair' and row['state'] == 'waiting'
                        and row['reason_code'] == 'd_mount_healthy')]
    def measured_work_eta(work, scope):
        estimates = []
        for row in work:
            forecast = next((row[key] for key in ('queue_forecast', 'completion_forecast', 'eta')
                             if row[key].get('state') == 'estimated'), None)
            if forecast:
                end = timestamp(forecast.get('completion_at_utc'))
                if end is not None and not row['stale']:
                    estimates.append(end)
        return {'state': 'partial' if estimates else 'unknown',
                'message': REASONS['remaining_phases'], 'scope': scope,
                'estimated_tasks': len(estimates), 'unestimated_tasks': len(work)-len(estimates),
                'blocked_tasks': sum(row['state'] == 'blocked' for row in work),
                'known_completion_at_utc': iso(max(estimates)) if estimates else None}
    overall = measured_work_eta(pending, 'estimated_work_only')
    local_eta = measured_work_eta([row for row in pending if row['id'] in local_task_ids],
                                  'local_migration_measured_steps_only')
    policy = archive_plan.public_policy(value.get('archive_policy'))
    if policy is not None:
        window = policy['daily_merge_window']
        policy['daily_window_open'] = archive_schedule.allows_start(now, window) if not observer_stale else None
        policy['next_daily_merge_start_at_utc'] = (iso(archive_schedule.next_due(None, now, window))
            if not observer_stale and not policy['daily_window_open'] else None)
    return {"contract": CONTRACT, "read_only": True, "observed_at_utc": iso(generated),
        'archive_policy': policy,
        "sample_interval_seconds": 60, "state": "stale" if observer_stale else "available",
        "summary": {"running": sum(r["state"] == "running" and not r["stale"] for r in rows),
            "waiting": sum(r["state"] == "waiting" and not r['historical_only'] for r in rows),
            "blocked": sum(r["state"] == "blocked" and not r['historical_only'] for r in rows),
            "historical_unresolved": sum(r['historical_only'] for r in rows),
            "completed": sum(r["complete_evidence"] for r in rows),
            "omitted_task_count": number(value.get("omitted_task_count")),
            "penguin_reclaimed_allocated_bytes": sum(r["metrics"].get("reclaimed_allocated_bytes", 0)
                for r in rows if r['kind'] in {'retirement', 'handoff_source_repack', 'handoff_index_archive'}),
            "reclaimed_scope": "completed_WSL_retirements_excluding_physical_vhdx_compaction"},
        "overall_eta": overall,
        'local_migration_eta': local_eta,
        "tasks": rows, "schedules": public_plan, "owner_locks": public_locks,
        "attention": attention, "flow": flow,
        "schedule_scope": "enrolled_penguin_owners_and_paired_remote_receipts",
        "remote_schedule_message": "lab203／Vast 的即時運行依既有回執與探測更新；未回傳的遠端 timer、重試及開始時間維持未知。"}
