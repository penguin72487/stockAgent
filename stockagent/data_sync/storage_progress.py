"""Bounded operational receipts for the storage observer, never a work owner.

Only small metadata and enrolled receipt directories are read. No source walk,
payload hash, backup catalogue/ledger, SSH, or workflow mutation belongs here.
Rate history is scoped to an exact generation, phase and counter denominator.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import time

from stockagent.service_layout import canonical_service_path

CONTRACT = "stockagent.storage_operations.v1"
HISTORY_CONTRACT = "stockagent.storage_operations_history.v1"
MAX_METADATA_BYTES = 1024 * 1024
STALE_SECONDS = 300
MAX_TASKS = 96
RECEIVER_LOCK_DIAGNOSTIC = '5623bb85bc595d58b57ed3f6fa950bf8747558818a84e1bfa360891da17b4191'
LABELS = {
    "bulk_return": "Vast 歷史封存回傳與原件驗證",
    "workspace_private_scratch": "WSL → E · 固定 SQLite 準備快照",
    "workspace_controls": "WSL → E · 固定準備副本",
    "workspace_transport": "D → E · 備份傳輸暫存搬移",
    "workspace_transport_comparison": "D → E · 已複製檔案差異比對",
    "workspace_transport_delta": "D → E · 舊版差異封包保全",
    "workspace_transport_restore": "D → E · 差異檔案還原演練",
    "workspace_transport_retirement": "D → E · 舊傳輸檔案逐檔回收",
    "workspace_worker_handoff": "E 工作區 · 儲存 worker 安全交接",
    "vast_spool": "Penguin → Vast · 封閉壓縮包預驗證",
    "registered_backup": "已登錄冷庫 · NAS 檔案驗證",
    "private_backup": "私有冷庫 · NAS 檔案驗證",
    "transport": "Penguin → lab203 同步通知",
    "transport_retirement": "已驗收傳輸副本回收",
    "lake_transport_retirement": "冷庫複寫 · 已驗收副本回收",
    "receiver": "lab203 → NAS 接收與獨立讀回",
    "migration": "D 槽冷庫 · 原始資料還原驗證",
    "migration_publication": "D 槽冷庫 · 來源清點與封存",
    "migration_dependency": "待修復的冷庫來源",
    "cooperative": "Penguin／Vast · 分工驗證 packs",
    "retained_verification": "沿用固定版本的驗證證據",
    "retirement": "WSL 安全回收",
    "encoding_stage_retirement": "D 槽 · 已退休版本的編碼副本回收",
    "lifecycle_backup": "控制資料庫 · 加密 NAS 備份",
    "local_testing_cleanup": "WSL · 已授權刪除的舊效能測試副本",
}
PHASES = {
    "scratch_copy": "複製固定快照至 E",
    "scratch_readback": "獨立讀回 E 並核對 SHA",
    "scratch_binding": "核對引用、綁定並回收 WSL 準備副本",
    "scratch_completed": "固定準備快照已移至 E",
    "workspace_inventory": "建立固定搬移清冊；總量待清點",
    "workspace_copy": "複製並獨立讀回完整 SHA",
    "workspace_handoff": "核對原 owner／引用並切換原服務路徑",
    "workspace_retirement": "回收已核對 E 副本的 D 舊傳輸暫存",
    "workspace_comparison": "讀取 E 副本並與固定清冊逐檔比對 SHA",
    "workspace_comparison_done": "本輪逐檔差異比對完成",
    "workspace_delta_copy": "將固定差異檔順序封裝至 E",
    "workspace_delta_readback": "獨立讀回 E 差異封包並逐檔驗證",
    "workspace_delta_done": "固定差異檔的 E 封包已驗證",
    "workspace_delta_restore": "在 E 順序重建差異檔並獨立讀回",
    "workspace_delta_restore_done": "差異檔內容與中繼資料還原演練完成",
    "workspace_retirement_done": "固定清冊的舊 D 傳輸檔案已回收",
    "workspace_completed": "E 傳輸路徑切換及 D 舊副本回收完成",
    "workspace_worker_wait": "等待原發布工作釋放完整交接鎖",
    "workspace_worker_ready": "新版本 worker 已載入，原工作流程保留",
    "checking_exact_targets": "清點固定效能測試副本",
    "checking_consumers": "檢查現行程序、服務及引用",
    "waiting_artifact_owner": "等待原產物回收執行者",
    "discarding_exact_target": "隔離並刪除已授權的測試副本",
    "waiting_protected_targets": "其餘副本保留；等待引用解除或錯誤處理",
    "disposable_cleanup_completed": "固定效能測試副本清理完成",
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
    "censusing_source": "清點來源檔案與大小；尚未驗證內容",
    "loading_previous_inventory": "讀取前一版本的固定清冊",
    "validating_previous_inventory": "核對前一版本的固定清冊",
    "routing_and_hashing": "核對來源內容並重用既有物件",
    "packing_changed_small_files": "壓縮新增或變更的小檔案",
    "verifying_complete_file_storage": "核對完整檔案的儲存綁定",
    "committing_complete_inventory": "提交完整來源清冊",
    "verifying_observation_cohort_stability": "核對固定觀測範圍的穩定性",
    "verifying_source_stability": "核對來源在處理期間保持一致",
    "source_fences_verified": "來源檢查完成；等待版本提交及冷庫還原",
    "nas_file_readback": "NAS 配對檔案讀回與驗證",
    "scan_notification": "等待同步器處理封閉批次",
    "relay_readback": "接收、備份與獨立還原並行處理",
    "dataset_reconstruction": "逐資料集驗證原始資料可還原",
    "source_dependency": "等待原始來源修復",
    "encode-and-check-receipts": "壓縮原檔並核對逐檔收據",
    "pack_reconstruction": "驗證封閉 pack 的原始內容",
    "waiting_preflight": "等待原回收程序與前置檢查",
    "capacity-preflight": "等待實體容量足夠後自動開始",
    "independent-D-original-recovery-for-quarantine": "由 D 槽獨立還原並核對原檔",
    "full-quarantine-resume-audit": "核對隔離原檔與封存內容",
    "verify-materialized-files": "逐檔核對還原內容",
    "decode-originals": "解碼並核對原始檔案",
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
    'bulk_return_failed': '原回傳程序保留來源並等待錯誤排查',
    'scratch_owner_busy': '原資料集作業仍在執行，原鎖釋放後重試',
    'scratch_capacity': 'E 實體或工作區可用容量不足，保留原副本並重試',
    'scratch_reference': '原準備副本仍有程序引用，解除後重試',
    'scratch_error': '固定快照核對或搬移失敗，保留來源並由原排程重試',
    'worker_reload_failed': '儲存 worker 交接或版本驗收失敗；保留原錯誤並由原排程有限重試',
    'heterogeneous_workloads': '工作項目大小不同；目前沒有足夠工作量估計，不能用完成項數外推整批時間',
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
    "dependency": "仍有前置條件，無法可靠預估開始或完成時間",
    "source_link": "原始來源連結無效，等待原產生器修復",
    "source_predecessor": "等待前一份來源完成原遷移程序",
    "upstream_publication": "目前已發現的封閉包已完成預驗證；新包封閉後由原排程自動接續，整份來源是否完成仍以原驗收為準",
    "original_owner": "由原程序執行；下次檢查時間不是開始時間",
    "retirement_identity": "固定清冊、冷庫版本或完成收據不符，保留原失敗證據等待修復",
    "owner_retry_limit": "原程序已達重試上限，保留失敗證據；修復後由原排程接續",
    "receiver_readiness": "接收端就緒回執已過期，等待原 owner 更新；保留有效期限及容量檢查",
    "receiver_readiness_lock_diagnosis": "接收端就緒回執仍待原程序更新；最近一次診斷曾遇到 Restic 排他鎖，目前鎖狀態仍待接收端確認",
    "transport_capacity": "傳輸暫存已達在途容量邊界，等待原 ACK 與暫存回收",
    "transport_retirement_unverified": "E 傳輸路徑已切換；D 舊副本尚未全部完成回收，請查看逐檔回收進度與保留原因",
    "transport_stale_mount_namespace": "部分既有服務仍保留舊 D 傳輸掛載；原排程每分鐘重新檢查，尚未回收舊副本",
    "transport_namespace_unavailable": "無法完整檢查舊傳輸路徑或程序掛載，保留舊副本並等待下次檢查",
    "transport_consumer_alias": "Syncthing 的實際傳輸掛載尚未確認指向 E，保留 D 舊副本",
    "staging_capacity": "編碼／複製暫存空間不足，等待原暫存處理及回收",
    "physical_capacity": "實體磁碟寫入餘量不足，保留原服務及驗證空間",
    "finalizing": "計數已到目標，等待這個階段的正式驗收",
    "scope_incomplete": "只涵蓋已登錄固定版本；不代表全部專案歷史",
    "remaining_phases": "後續階段尚無完整工作量或速度，總完成時間待估",
}
PREVIOUS_SOURCE_FAILURES = {
    'io_error': '上次來源處理曾遇到 I/O 錯誤；空間恢復後仍需重新檢查，不能認定錯誤已解除。',
}
BOOT_AUDIT_FAILURES = {
    'finmind_inactive': 'FinMind 下載服務有未運行項目',
    'lakehouse_receipts_stale': '儲存控制回執過期',
    'lakehouse_worker_version_unverified': '儲存工作者載入版本尚未核對一致',
    'lakehouse_queries_unverified': '儲存工作流程查詢尚未驗證',
}
SCOPES = {
    "bulk_return": "固定歷史封存回傳的原程序；狀態不代表全部 Vast 資料或 NAS 已驗收",
    "workspace_private_scratch": "只遷移一份固定 SQLite 準備快照；不代表 D 冷庫或 NAS 已驗收",
    "workspace_controls": "只搬移原作業的固定控制及準備副本；不代表 D 冷庫或 NAS 已驗收",
    "workspace_transport": "只搬移備份傳輸暫存到 E；D 主冷庫、來源觀測與 NAS 原件不刪除，不代表全部來源已遷移或 NAS 驗收",
    "workspace_transport_comparison": "固定搬移清冊與 E 副本的讀取時比對；差異與缺檔保留，結果不授權刪除，也不是 NAS 驗收",
    "workspace_transport_delta": "只保全固定清冊中不同或缺少的舊 D 檔案，E 封包不是 D 冷庫或 NAS 驗收，亦不授權刪除",
    "workspace_transport_restore": "逐個重建 E 差異封包中的檔案並讀回驗證；不代表整棵資料樹、D 冷庫或 NAS 已驗收",
    "workspace_transport_retirement": "只回收固定清冊中的舊 D 傳輸副本；保留舊目錄、來源觀測、D 冷庫和 NAS，與 WSL 釋放量分開計算",
    "workspace_worker_handoff": "只核對 E 切換後的 worker 載入版本與持久工作流程；等待觀測不會更新資料進度或 NAS 驗收時間",
    "local_testing_cleanup": "使用者明確授權直接刪除的固定舊測試副本；保留精簡結果，不代表冷庫或 NAS 驗收，外部連結目標及正式資料不刪除",
    "vast_spool": "已封閉物件的暫存與原值計算；完整清冊待原驗證程序綁定，不代表 NAS 驗收或可刪除來源",
    "transport_retirement": "本輪固定已驗收傳輸副本的 logical bytes；不是 WSL 原始資料釋放量",
    "lake_transport_retirement": "原冷庫複寫回收程序的即時階段與重試；與備份封套回收分開，不代表 WSL 原檔已刪除",
    "registered_backup": "已登錄固定冷庫檔案；不含尚未發布來源，不代表完整歷史還原",
    "private_backup": "已登錄私有冷庫版本；與其他冷庫清冊可能重疊，不加總",
    "transport": "同步通知與待掃描批次；不代表接收或 NAS 驗收",
    "receiver": "lab203 當次管線與接收佇列；空佇列不代表完整備份",
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
}
CONTINUOUS = {"stockagent-storage-lifecycle.service": ("lakehouse", "冷庫發布、還原與來源複寫")}
SCHEDULE_LABELS = dict((*TIMERS.values(), *CONTINUOUS.values()))
TASK_SCHEDULES = {
    'bulk_return': ('bulk_return',),
    'workspace_private_scratch': ('workspace_private_scratch',),
    'workspace_controls': ('workspace_controls',),
    'workspace_transport': ('workspace_transport',),
    'workspace_transport_comparison': ('workspace_transport',),
    'workspace_transport_delta': ('workspace_transport',),
    'workspace_transport_restore': ('workspace_transport',),
    'workspace_transport_retirement': ('workspace_transport',),
    'workspace_worker_handoff': ('workspace_transport',),
    'registered_backup': ('registered_backup',), 'private_backup': ('private_backup',),
    'transport': ('transport',), 'transport_retirement': ('registered_backup', 'private_backup'),
    'lake_transport_retirement': ('transport_gc',),
    'migration': ('lakehouse',), 'migration_publication': ('lakehouse',),
    'migration_dependency': ('lakehouse',), 'cooperative': ('lakehouse',),
    'retained_verification': ('lakehouse',), 'retirement': ('retirement',),
    'local_testing_cleanup': ('retirement',),
    'vast_spool': ('vast_spool',),
    'lifecycle_backup': ('lifecycle_backup',),
    'encoding_stage_retirement': ('artifact_retirement',),
}
OWNER_STATES = {"active", "activating", "inactive", "failed", "deactivating"}
LOCK_LABELS = {"backup_owner": "備份寫入與收據對帳", "dataset_owner": "來源發布與完整還原",
               "catalog_owner": "冷庫清冊登錄", "replication_owner": "來源複寫"}
FOLLOW_UP = {
    "source_link": "保留上次來源連結失敗；由原遷移程序重試修復後的來源，尚未重新通過前不回收。",
    "source_predecessor": "先完成前一來源的封存及完整還原，原遷移程序再自動接續。",
    "dependency": "先完成此工作的前置驗證；尚無可靠的開始時間。",
    "retirement_identity": "需核對固定清冊與原收據的身分差異，修復前保留來源。",
    "owner_retry_limit": "已達原重試上限，需修復失敗原因後交回原排程，其他獨立工作繼續。",
    "receiver_readiness": "等待 lab203 沿既有回執通道回報新鮮就緒證據。",
    "receiver_readiness_lock_diagnosis": "請 lab203 原 owner 核對最近一次 Restic 排他鎖的持有者、備份活動及目前狀態，並回傳新的就緒證據；不要只依 PID 判定鎖已失效。",
    "transport_capacity": "等待 NAS 驗收及傳輸暫存回收釋出在途額度，原排程會再檢查。",
    "transport_retirement_unverified": "由原執行者逐檔核對並回收舊 D 副本；內容不同或仍有引用的項目保留，另列實際阻擋原因。",
    "transport_stale_mount_namespace": "由原服務逐一正常交接舊掛載；未確認所有使用者已切換前，不回收 D 副本。",
    "transport_namespace_unavailable": "恢復程序及磁碟掛載的檢查權限後，由原排程重新檢查。",
    "transport_consumer_alias": "由原同步服務恢復 E 傳輸路徑後，再核對舊副本。",
    "staging_capacity": "先回收已有驗收的暫存；容量足夠後由原排程接續。",
    "physical_capacity": "等待實體磁碟提供足夠工作空間；虛擬磁碟可用量不能替代此條件。",
    "progress_pending": "原程序尚未回報新的階段計數，完成時間暫停估算；下次仍會檢查。",
    "metadata_unavailable": "檢查原工作收據是否產生及可讀；本次未取得有效進度。",
}
FLOW = (
    ("publish", "Penguin → D：來源清點與封存", (), "migration_publication",
     "封閉固定版本後才能還原；開啟中的 pack 不能交給遠端驗證。"),
    ("recover", "Penguin／Vast：原始資料完整還原", ("publish",), "migration",
     "原 verifier 分工核對全部原值；遠端在線不代表本輪已接到工作。"),
    ("retire", "Penguin：WSL 安全回收", ("recover",), "retirement",
     "各固定清冊獨立排程；還原、引用、容量及退休驗收均通過才刪除。"),
    ("send", "Penguin → lab203：封閉批次同步", ("publish",), "transport",
     "可與其他固定版本的封存、還原並行；送達不等於 NAS 驗收。"),
    ("nas", "lab203 → NAS：獨立讀回與配對 ACK", ("send",), "receiver",
     "沿原接收 owner 執行；遠端完整排程未回傳時，開始時間維持未知。"),
    ("transport_gc", "Penguin：已驗收傳輸副本回收", ("nas",), "transport_retirement",
     "逐批核對配對 ACK、保留來源及引用；釋放 D 暫存和 WSL 原檔分開計量。"),
)
METRICS = {"discarded_allocated_bytes", "pending_batches", "pending_bytes", "active_jobs", "admission_available_bytes", "blocked_items",
           "stale_mount_namespaces", "reachable_stale_mount_namespaces", "files_held",
           "same_files", "different_files", "missing_files", "unstable_files", "unavailable_files",
           "reclaimed_allocated_bytes", "lab_reclaimed_allocated_bytes", "penguin_packs", "vast_packs", "retained_packs",
           "queued_jobs", "last_cycle_accepted_jobs", "retry_jobs", "rejected_jobs",
           "planned_reclaim_allocated_bytes", "planned_source_files",
           "processed_source_files", "total_source_files", "processed_source_bytes", "total_source_bytes",
           "source_free_bytes", "source_reserve_bytes"}
SUBJECTS = {"legacy-artifact-markets-crypto": "加密貨幣研究產物",
            "legacy-artifact-markets-us": "美股研究產物",
            "local-openbb": "OpenBB 本機歷史", "cftc-legacy-pre2000": "CFTC 2000 年以前歷史",
            "local-tw-public": "台股公開資訊本機歷史", "local-tw-futures": "台灣期貨本機歷史",
            "local-tw-index-futures": "台灣指數期貨本機歷史", "tw-index-futures": "台灣指數期貨歷史"}


def number(value):
    return value if type(value) is int and 0 <= value <= 2**53 - 1 else None


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
    if number(value.get('phase_completed_items')) is not None:
        return {'done_items': value['phase_completed_items'], 'total_items': value.get('phase_total_items')}
    return {'done_items': (value.get('files_routed') if value.get('stage') == 'routing_and_hashing'
                           else value.get('files_completed')),
            'total_items': value.get('files_total'),
            'done_bytes': value.get('logical_bytes_processed'),
            'total_bytes': value.get('logical_bytes_total')}


def publication_eta_work(value: dict):
    """A bucket can take hours; its owner already counts each hashed member.

    Keep the bucket bar. Estimate remaining source-content work from actual
    member/byte deltas in this packing phase; later publication/recovery is
    outside this ETA. Earlier routing observations are not speed samples.
    """
    total, done = number(value.get('files_total')), number(value.get('files_hashed'))
    if (value.get('stage') == 'packing_changed_small_files' and total and done is not None
            and done <= total and number(value.get('files_routed')) == total):
        return {'completed_items': done, 'total_items': total,
                'completed_bytes': number(value.get('logical_bytes_processed')),
                'total_bytes': number(value.get('logical_bytes_total'))}
    return None


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
                                'original_owner' if placement_wait else 'scope_incomplete'),
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


def migration_tasks(root: Path) -> list[dict]:
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
    waiting = bool(remaining) and all(v.get("state") == "waiting_retry" for v in remaining)
    rows = [task("migration", "migration", state="completed" if complete else "blocked" if waiting else "running" if progress else "unknown",
        phase="dataset_reconstruction", observed=clock(progress) or clock(current),
        generation=current.get("plan_sha256"), done_items=verified if progress else None,
        total_items=total, proof=complete, reason="dependency" if waiting else "remaining_phases")]
    for name, data in datasets.items():
        if not isinstance(data, dict) or not re.fullmatch(r"[a-z0-9_-]{1,96}", name):
            continue
        if data.get("state") == "waiting_retry":
            error = data.get("error", "")
            dependency = data.get('dependency_wait')
            capacity_wait = data.get('capacity_wait')
            reason = ('physical_capacity' if isinstance(capacity_wait, dict)
                      and capacity_wait.get('contract') == 'source_publication_capacity_v1' else
                      "source_predecessor" if isinstance(dependency, dict)
                      and dependency.get('dataset') == 'local-tw-index-futures' else
                      "source_link" if isinstance(error, str) and "symlink" in error else
                      "source_predecessor" if isinstance(error, str) and "original migration" in error else "dependency")
            row = task("migration_dependency", name, state="blocked", phase="source_dependency",
                observed=clock(progress) or clock(current), generation=data.get("attempt_id"), reason=reason)
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
                    observed=clock(publication), generation=(data.get('attempt_id'), 'source_phase_counters_v2'),
                    metrics={'processed_source_files': publication.get('files_completed'),
                             'total_source_files': publication.get('files_total'),
                             'processed_source_bytes': publication.get('logical_bytes_processed'),
                             'total_source_bytes': publication.get('logical_bytes_total')}, **counters)
                publishing['subject'] = name if name in SUBJECTS else None
                publishing['eta_work'] = publication_eta_work(publication)
                rows.append(publishing)
            p = metadata(operation / "recovery" / (name + ".progress.json"))
            if p and p.get("attempt_id") == data.get("attempt_id"):
                rows.append(task("migration", "active-recovery", state="running",
                    phase=p.get("stage") if p.get("stage") in PHASES else "working", observed=clock(p),
                    generation=(data.get("attempt_id"), data.get("manifest_sha256")),
                    done_bytes=p.get("completed_bytes"), total_bytes=p.get("total_bytes"),
                    done_items=p.get("completed_items"), total_items=p.get("total_items")))
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


def local_testing_cleanup_tasks(repo: Path) -> list[dict]:
    from stockagent.data_sync.testing_disposition import LOCAL_TESTING_CONTRACT, identity
    rows = []
    # Explicit generations only. A newer work-copy decision must not overwrite
    # or reclassify the earlier policy's completion and reclaimed bytes.
    for generation in ('20261102', '20261103'):
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
            rows.append(task('local_testing_cleanup', f'local-performance-{generation}', reason='metadata_unavailable'))
            continue
        completed = value.get('state') == 'completed' and number(value.get('targets_completed')) == len(targets)
        waiting = value.get('state') == 'waiting_protected_targets'
        phase = 'disposable_cleanup_completed' if completed else value.get('state')
        rows.append(task('local_testing_cleanup', policy['identity_sha256'],
            state='completed' if completed else 'blocked' if waiting else 'running',
            phase=phase if phase in PHASES else 'working', observed=clock(value),
            generation=(policy['identity_sha256'], 'disposable_workcopies_v1'),
            done_items=number(value.get('targets_completed')), total_items=len(targets), proof=completed,
            reason='dependency' if waiting else None,
            metrics={'discarded_allocated_bytes': number(value.get('reclaimed_allocated_bytes')),
                     'blocked_items': number(value.get('targets_blocked'))}))
    return rows


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


def schedules() -> list[dict]:
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
                                 "-p", "LoadState", "-p", "Result", "-p", "ExecMainStatus", "-p", "MainPID", "--no-pager"],
                                capture_output=True, text=True, timeout=3, check=True)
        for block in result.stdout.strip().split("\n\n"):
            fields = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
            if fields.get("Id") in units:
                states[fields["Id"]] = fields
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
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
        entry['owner_result'] = choice(owner.get('Result'),
            {'success', 'exit-code', 'signal', 'core-dump', 'timeout', 'watchdog', 'resources', 'start-limit-hit'}, 'unknown')
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
                    eta.update(state='stalled', reason='no_recent_progress')
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
    if row.get('kind') in {'local_testing_cleanup', 'workspace_transport_retirement'} and row.get('complete_evidence') is not True:
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
    # Encoding content and metadata have separate explicit phases. File-count
    # extrapolation during decoding charges a large file's cost to every tiny
    # receipt. Preserve the general small-file-tail guard for other owners.
    content_decoding = (row.get('kind') == 'encoding_stage_retirement'
                        and row.get('phase') in {'verify-encoding-stage-files', 'reconstruct-encoding-stage-files'})
    if number(row.get('total_bytes')) and number(row.get('total_items')) and not content_decoding:
        old_items = previous.get('items', {}) if isinstance(previous, dict) else {}
        item_eta, item_history = _estimate_counter({**row, 'total_bytes': None}, old_items, now=now)
        history['items'] = item_history
        byte_seconds, item_seconds = eta.get('remaining_seconds'), item_eta.get('remaining_seconds')
        if ((eta['state'] == 'estimated' and item_eta['state'] == 'estimated'
             and item_seconds > byte_seconds)
                or (eta['state'] == 'finalizing'
                    and number(row.get('completed_items')) is not None
                    and row['completed_items'] < row['total_items'])):
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
        result.append(task('vast_spool', value.get('context_sha256', 'unavailable'), node='penguin_vast',
            state='running' if value.get('state') == 'running' else 'blocked' if value.get('state') == 'blocked' else 'waiting',
            phase='waiting_new_sealed_packs' if waiting else 'sealed_pack_staging',
            observed=clock(value), activity=clock(status), generation=value.get('context_sha256'),
            done_items=value.get('completed_items'), total_items=value.get('total_items'),
            done_bytes=value.get('completed_bytes'), total_bytes=value.get('total_bytes'),
            metrics={'active_jobs': value.get('active_jobs')},
            reason='dependency' if value.get('state') == 'blocked' else 'upstream_publication' if waiting else None))
        result[-1]['subject'] = value.get('dataset')
        result[-1]['inventory_at_utc'] = value.get('inventory_observed_at_utc')
    return result


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
    done=(handoff.get('state')=='completed' and handoff.get('original_sender_path_preserved') is True
          and handoff.get('old_D_copy_retirement_verified') is True
          and handoff.get('old_D_source_absent_confirmed') is True
          and handoff.get('consumer_aliases_verified') is True
          and all(handoff.get(k)==0 for k in ('source_observations_deleted','cold_objects_deleted','nas_objects_deleted'))
          and handoff.get('copy',{}).get('all_source_sha256_and_destination_readback_verified') is True)
    old_completion_unverified = handoff.get('state') in {'completed', 'retirement_unverified'} and not done
    phase=('workspace_completed' if done else 'workspace_inventory' if value.get('state')=='inventory'
           else 'workspace_copy' if value.get('state')=='copying_and_independent_sha_readback'
           else 'workspace_retirement' if old_completion_unverified or handoff.get('state') in {'quarantined','retiring_verified_D_copy'}
           else 'workspace_handoff')
    recent_failure=(not done and clock(attempt) is not None
                    and clock(attempt)>max(clock(value) or 0,clock(handoff) or 0))
    waiting=recent_failure and attempt.get('reason_code')=='original_owner'
    rows = [task('workspace_transport','D-E',state='completed' if done else
        'blocked' if old_completion_unverified else 'waiting' if waiting else 'blocked' if recent_failure else 'running',phase=phase,
        generation=('workspace_exact_relocation_v1',intent.get('started_at_epoch'),phase),
        observed=clock(handoff) if done else clock(reconciliation) if old_completion_unverified and clock(reconciliation) is not None
            else clock(attempt) if recent_failure else clock(value),
        done_bytes=intent.get('logical_bytes') if done else value.get('bytes_completed'),
        total_bytes=None if phase=='workspace_inventory' else intent.get('logical_bytes'),
        done_items=intent.get('files') if done else value.get('files_completed',value.get('entries')),
        total_items=None if phase in {'workspace_inventory', 'workspace_retirement'} and not done else intent.get('files'),proof=done,
        reason=(choice(reconciliation.get('reason_code'), REASONS) or 'transport_retirement_unverified') if old_completion_unverified else
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
        rows.append(task('workspace_transport_retirement', 'D-E-fixed-retirement',
            state='completed' if complete else 'blocked' if held or namespace_wait or failure
                  else 'waiting' if owner_wait else 'running',
            phase='workspace_retirement_done' if complete else 'workspace_retirement',
            observed=clock(selected), activity=(clock(reconciliation) if namespace_wait
                      else clock(attempt) if failure or owner_wait else clock(held_wait) if current_held_wait else None),
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


def workspace_private_scratch_tasks(root: Path) -> list[dict]:
    journal = metadata(root / 'journal.json')
    progress = metadata(root / 'stream-progress.json')
    complete = metadata(root / 'complete.json')
    attempt = metadata(root / 'attempt.json')
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
    return [task('workspace_private_scratch', 'fixed-sqlite', state=state, phase=phase,
        generation=('private_sqlite_workspace_relocation_v1', journal.get('snapshot_sha256'), phase),
        observed=clock(complete) if done else clock(attempt) if recent_failure else clock(progress),
        done_bytes=total if done else progress.get('bytes_completed'), total_bytes=total, proof=done,
        reason=choice(attempt.get('reason_code'), REASONS, 'scratch_error') if recent_failure
               else 'scratch_reference' if wait else None)]


def workspace_controls_tasks(candidates: list[dict]) -> list[dict]:
    """Project only fixed, enrolled handoffs; copying is not cold acceptance."""
    rows = []
    for candidate in candidates:
        root = Path(candidate['state_root'])
        batch = candidate['batch_number']
        expected = candidate['controls_identity']
        if (type(batch) is not int or not 1 <= batch <= 3
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


def bulk_return_tasks(root: Path) -> list[dict]:
    value = metadata(root / 'status.json')
    current = value.get('state')
    capacity = current == 'waiting_cold_physical_capacity'
    failed = current == 'source_preserved_needs_audit'
    return [task('bulk_return', 'coordinator',
        state='blocked' if capacity or failed else 'running' if current == 'checking_received_cohorts'
              else 'waiting' if current in {'cohorts_checked', 'deferred'} else 'unknown',
        phase='capacity-preflight' if capacity else 'working', observed=clock(value),
        reason='physical_capacity' if capacity else 'bulk_return_failed' if failed else None,
        metrics={'admission_available_bytes': value.get('available_bytes')} if capacity else {})]


def boot_audit_observation(path: Path) -> dict:
    """Project a bounded audit without publishing service internals or paths."""
    value = metadata(path)
    audit = value.get('boot_recovery')
    if not isinstance(audit, dict) or clock(value) is None:
        return {}
    reasons = []
    owners = audit.get('unavailable_critical_owners')
    if isinstance(owners, list) and any(name in owners for name in (
            'stockagent-finmind-free.service', 'stockagent-finmind-sponsor.service',
            'stockagent-finmind-complement.service')):
        reasons.append('finmind_inactive')
    lake = audit.get('lakehouse_control')
    if isinstance(lake, dict) and lake.get('required') is True:
        for field, code in (
                ('receipts_fresh', 'lakehouse_receipts_stale'),
                ('loaded_worker_matches_current_code', 'lakehouse_worker_version_unverified'),
                ('workflow_queries_verified', 'lakehouse_queries_unverified')):
            if lake.get(field) is False:
                reasons.append(code)
    return {'observed_at_utc': iso(clock(value)), 'reason_codes': reasons}


def collect(repo: Path, config: dict, history: dict, *, now=None, runtime=None,
            capacities=None) -> tuple[dict, dict]:
    now = time.time() if now is None else now
    options = config.get("operations") if isinstance(config.get("operations"), dict) else {}
    queue = Path(options.get("retirement_queue_config", "configs/data_sync/wsl_retirement_queue.json"))
    sources = [
        ("vast_spool", lambda: spool_tasks(repo)),
        ("registered_backup", lambda: backup_tasks(Path(options.get("backup_state_root", "/var/lib/stockagent/backup-stream")),
            Path(options.get('cold_scan_retry_receipt', '/var/lib/stockagent-d-cold-scan-retry/latest.json')))),
        ("migration", lambda: migration_tasks(Path(options.get("migration_state_root", "/var/lib/stockagent/lakehouse-control/dataset-versions")))),
        ("lake_transport_retirement", lambda: lake_transport_tasks(Path(options.get("transport_gc_state_root", "/var/lib/stockagent/lakehouse-control")))),
        ("lifecycle_backup", lambda: lifecycle_backup_tasks(Path(options.get("control_backup_state_root", "/var/lib/stockagent/lakehouse-control")))),
        ("retirement", lambda: retirement_tasks(repo, queue if queue.is_absolute() else repo / queue)),
        ('local_testing_cleanup', lambda: local_testing_cleanup_tasks(repo)),
        ('encoding_stage_retirement', lambda: encoding_stage_tasks(repo,
            Path(options.get('legacy_stage_pruning_root', '/var/lib/stockagent-legacy-artifacts/stage-pruning')))),
        ("receiver", lambda: [receiver_task(Path(config.get("receipt_root", "/srv/stockagent-backup-receipts-lab203")), now=now)])]
    if options.get('workspace_relocation_state_root'):
        sources.insert(0,('workspace_transport',lambda:workspace_transport_tasks(Path(options['workspace_relocation_state_root']))))
    if options.get('workspace_private_scratch_state_root'):
        sources.insert(0, ('workspace_private_scratch', lambda: workspace_private_scratch_tasks(
            Path(options['workspace_private_scratch_state_root']))))
    if isinstance(options.get('workspace_controls_candidates'), list):
        sources.insert(0, ('workspace_controls', lambda: workspace_controls_tasks(
            options['workspace_controls_candidates'])))
    if options.get('bulk_return_state_root'):
        sources.insert(0, ('bulk_return', lambda: bulk_return_tasks(Path(options['bulk_return_state_root']))))
    rows = []
    for kind, read in sources:
        try:
            rows.extend(read())
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            rows.append(task(kind, kind, reason="metadata_unavailable"))
    # Reuse the observer's current bounded physical measurement. A source may
    # still be routing after a prior ENOSPC, but that speed cannot forecast a
    # writable publication on a full disk. Preserve counters and their clock;
    # only suspend the estimate and expose the independently measured blocker.
    cold = capacities.get('penguin-d') if isinstance(capacities, dict) else None
    stamp = clock(cold) if isinstance(cold, dict) else None
    available = number(cold.get('available_bytes')) if isinstance(cold, dict) else None
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
    owners['workspace_transport_retirement'] = owners.get('workspace_transport', 'unknown')
    owners['workspace_worker_handoff'] = owners.get('workspace_transport', 'unknown')
    updated = {}
    for row in rows[:MAX_TASKS]:
        # This is a fresh metadata read, not a new counter sample, heartbeat
        # from the worker or acceptance proof. Keep the source clock intact.
        row['checked_at_utc'] = iso(now)
        row['owner_state'] = owners.get(row['kind'], 'unknown')
        if row['kind'] == 'local_testing_cleanup' and row['state'] == 'running' and row['owner_state'] == 'inactive':
            # The same executor may still be owned by the initial bounded
            # handoff run. An idle timer unit does not establish an idle job.
            row['owner_state'] = 'unknown'
        row["eta"], updated[row["id"]] = estimate(row, old.get(row["id"], {}), now=now)
        row.pop("generation", None)  # Private generation binding stays server-side.
    # Kernel PIDs help identify a wrapper's child locally; never persist them
    # into this observer's shared/public snapshot.
    for entry in plan:
        entry.pop('owner_pid', None)
    audit_path = Path(options.get('boot_audit_receipt', '/var/lib/stockagent/boot-recovery/latest.json'))
    return ({"contract": CONTRACT, "observed_at_utc": iso(now), "tasks": rows[:MAX_TASKS],
             "schedules": plan, "owner_locks": locks,
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
        counters = {k: number(item.get(k)) for k in ("completed_bytes", "total_bytes", "completed_items", "total_items")}
        for metric in ("bytes", "items"):
            a, b = counters["completed_" + metric], counters["total_" + metric]
            if a is not None and b is not None and a > b:
                counters["completed_" + metric] = None
        eta_raw = item.get("eta") if isinstance(item.get("eta"), dict) else {}
        metric = ("bytes" if kind == 'workspace_transport_retirement' and counters['total_bytes'] else
                  "items" if eta_raw.get('unit') == 'items' and counters['total_items'] else
                  "bytes" if counters["total_bytes"] else "items")
        a, b = counters["completed_" + metric], counters["total_" + metric]
        pct = min(99.99 if a < b else 100, round(a / b * 100, 2)) if a is not None and b else None
        eta_state = choice(eta_raw.get("state"), {"unknown", "sampling", "estimated", "completed", "blocked", "stale", "stalled", "finalizing"}, "unknown")
        reason = choice(eta_raw.get("reason"), REASONS, "no_counter")
        if proof:
            eta_state, reason = "completed", None
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
        estimate_metric = choice(eta_raw.get('unit'), {'bytes', 'items'}, metric) if content_scope else metric
        estimate_done, estimate_total = (number(content.get('completed_'+estimate_metric)), number(content.get('total_'+estimate_metric))) if content_scope else (a, b)
        estimated = eta_state == "estimated" and estimate_done is not None and estimate_total and estimate_done < estimate_total
        if eta_state == "estimated" and not estimated:
            eta_state, reason = "unknown", "no_counter"
        completion = timestamp(eta_raw.get("completion_at_utc"))
        # A missed estimate is an observation failure, not a negative countdown.
        if estimated and (completion is None or completion <= now):
            estimated, eta_state, reason = False, "stalled", "no_recent_progress"
        elif estimated:
            reason = None
        age = max(0., now - generated) if generated is not None else 0.
        def remaining_bound(field):
            seconds = number(eta_raw.get(field))
            return math.ceil(max(0., seconds-age)) if estimated and seconds is not None else None
        eta = {"state": eta_state, "reason": reason, "message": REASONS.get(reason),
               "scope": 'remaining_source_content' if content_scope else "current_phase_only",
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
        control_names = {1: '台股公開資料', 2: '台指期貨', 3: 'OpenBB'}
        label = (f"WSL 安全回收 · 第 {batch} 輪" if kind == "retirement" and batch else
                 f"WSL → E · {control_names[batch]}準備副本" if kind == 'workspace_controls' and batch in control_names else
                 LABELS[kind])
        if choice(item.get("subject"), SUBJECTS):
            label += " · " + SUBJECTS[item["subject"]]
        lease = item.get('receiver_readiness')
        lease = ({'state': choice(lease.get('state'), {'expired', 'future'}),
                  **{k: iso(timestamp(lease.get(k))) for k in ('observed_at_utc', 'valid_until_utc', 'checked_at_utc')}}
                 if kind == 'private_backup' and isinstance(lease, dict)
                    and choice(lease.get('state'), {'expired', 'future'}) else None)
        previous_failure = choice(item.get('previous_source_failure'), PREVIOUS_SOURCE_FAILURES) if kind == 'migration_dependency' else None
        rows.append({"id": item["id"], "kind": kind, "node_id": choice(item.get("node_id"), {"penguin", "nas", "lab203", "penguin_vast"}, "penguin"),
            "label": label, "state": state, "phase": phase, "phase_label": PHASES[phase], "scope": SCOPES[kind],
            "complete_evidence": proof, "stale": stale, "checked_at_utc": iso(checked),
            "progress_stale": progress_stale and not proof,
            "owner_state": choice(item.get('owner_state'), {'active', 'activating', 'inactive', 'failed', 'deactivating'}, 'unknown') if not stale else 'unknown',
            "observed_at_utc": iso(stamp), "activity_at_utc": iso(activity),
            "diagnosis_at_utc": iso(timestamp(item.get('diagnosis_at_utc'))) if kind == 'receiver' else None,
            "last_scan_observed_at_utc": iso(timestamp(item.get('last_scan_observed_at_utc'))) if kind == 'transport' else None,
            "notification_observation": choice(item.get('notification_observation'), {'scan', 'retry'}) if kind == 'transport' else None,
            "inventory_at_utc": iso(timestamp(item.get('inventory_at_utc'))),
            "reason_code": choice(item.get('reason'), REASONS),
            "reason": REASONS.get(choice(item.get("reason"), REASONS)), "progress": {**counters, "metric": metric, "percent": pct},
            "previous_source_failure": ({"code": previous_failure,
                "message": PREVIOUS_SOURCE_FAILURES[previous_failure]}
                if previous_failure
                else None),
            "eta": eta, "metrics": metrics, 'receiver_readiness': lease,
            "next_attempt_at_utc": iso(timestamp(item.get("next_attempt_at_utc"))),
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
        public_plan.append({"id": entry["id"], "label": schedule_labels[entry["id"]],
            "mode": choice(entry.get('mode'), {'timer', 'continuous'}, 'timer'),
            "registered": entry.get("registered") is True, "stale": observer_stale,
            "timer_active": entry.get("timer_active") if type(entry.get("timer_active")) is bool else None,
            "owner_running": entry.get("owner_running") if type(entry.get("owner_running")) is bool else None,
            "owner_state": choice(entry.get('owner_state'), OWNER_STATES, 'unknown') if not observer_stale else 'unknown',
            "owner_result": choice(entry.get('owner_result'),
                {'success', 'exit-code', 'signal', 'core-dump', 'timeout', 'watchdog', 'resources', 'start-limit-hit'}, 'unknown'),
            "exit_code": number(entry.get('exit_code')),
            "next_check_at_utc": iso(nxt) if not observer_stale and nxt and nxt >= now else None,
            "scheduled_check_at_utc": iso(nxt) if not observer_stale else None,
            "check_due": bool(not observer_stale and nxt and nxt < now),
            "last_check_at_utc": iso(timestamp(entry.get("last_check_at_utc")))})
    for row in rows:
        associated = [p for p in public_plan if p['id'] in TASK_SCHEDULES.get(row['kind'], ())]
        current = not observer_stale and not row['stale']
        enabled = current and any(p['timer_active'] is True or
            p['mode'] == 'continuous' and p['owner_running'] is True for p in associated)
        running = current and any(p['owner_running'] is True for p in associated)
        checks = [timestamp(p['next_check_at_utc']) for p in associated if p['next_check_at_utc']]
        row['continuation'] = {
            'automatic': bool(enabled), 'owner_running': bool(running),
            'schedule_labels': [p['label'] for p in associated],
            'next_check_at_utc': iso(min(checks)) if checks else None,
            'check_due': current and any(p['check_due'] for p in associated),
            'mode': 'running' if running else 'scheduled' if enabled else 'unconfirmed',
        }
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
                attention.append({'id': entry['id'], 'level': 'blocked', 'label': entry['label'],
                    'message': ('本輪已在重試；上次執行未成功。' if entry['owner_running'] else '原執行者回報失敗。')
                        + (' 原排程仍啟用，下一次時間見完整排程；需核對失敗收據。' if entry['timer_active']
                           else ' 需檢查原服務及重試排程；目前沒有確認的自動接續時間。') + audit_detail,
                    'last_report_at_utc': iso(audit_time) if audit_detail else None})
            elif entry['registered'] and ((entry['mode'] == 'timer' and entry['timer_active'] is False)
                    or (entry['mode'] == 'continuous' and entry['owner_running'] is False)):
                attention.append({'id': entry['id'], 'level': 'blocked', 'label': entry['label'],
                    'message': '原排程或常駐執行者未啟用；需核對是否為預期停用，不能保證自動接續。'})
        for row in rows:
            if row['complete_evidence']:
                continue
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
    for identity, label, dependencies, kind, condition in FLOW:
        matching = [r for r in rows if r['kind'] == kind
                    or kind == 'transport_retirement' and r['kind'] == 'lake_transport_retirement']
        status = ('unknown' if not matching or observer_stale else
                  'completed' if all(r['complete_evidence'] for r in matching) else
                  'blocked' if any(r['state'] == 'blocked' for r in matching) else
                  'running' if any(r['state'] == 'running' for r in matching) else 'waiting')
        flow.append({'id': identity, 'label': label, 'depends_on': list(dependencies),
                     'state': status, 'condition': condition, 'scope': 'enrolled_current_work_only'})
    pending = [row for row in rows if not row['complete_evidence']
               and row['phase'] != 'waiting_accepted_transport']
    estimates = []
    for row in pending:
        forecast = next((row[key] for key in ('queue_forecast', 'completion_forecast', 'eta')
                         if row[key].get('state') == 'estimated'), None)
        if forecast:
            estimates.append(timestamp(forecast['completion_at_utc']))
    overall = {'state': 'partial' if estimates else 'unknown',
               'message': REASONS['remaining_phases'], 'scope': 'estimated_work_only',
               'estimated_tasks': len(estimates), 'unestimated_tasks': len(pending)-len(estimates),
               'blocked_tasks': sum(row['state'] == 'blocked' for row in pending),
               'known_completion_at_utc': iso(max(estimates)) if estimates else None}
    return {"contract": CONTRACT, "read_only": True, "observed_at_utc": iso(generated),
        "sample_interval_seconds": 60, "state": "stale" if observer_stale else "available",
        "summary": {"running": sum(r["state"] == "running" and not r["stale"] for r in rows),
            "waiting": sum(r["state"] == "waiting" for r in rows), "blocked": sum(r["state"] == "blocked" for r in rows),
            "completed": sum(r["complete_evidence"] for r in rows),
            "penguin_reclaimed_allocated_bytes": sum(r["metrics"].get("reclaimed_allocated_bytes", 0) for r in rows),
            "reclaimed_scope": "accepted_enrolled_retirement_cohorts_only"},
        "overall_eta": overall,
        "tasks": rows, "schedules": public_plan, "owner_locks": public_locks,
        "attention": attention, "flow": flow,
        "schedule_scope": "enrolled_penguin_owners_and_paired_remote_receipts",
        "remote_schedule_message": "lab203／Vast 的即時運行依既有回執與探測更新；未回傳的遠端 timer、重試及開始時間維持未知。"}
