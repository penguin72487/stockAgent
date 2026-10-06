# 自動增量冷儲存與安全回收

冷資料唯一權威仍是 **penguin 的 D**，Vast 是 index-only 計算節點。
本功能擴充既有 publisher、Syncthing、ACK、lease、timer／cron；沒有第二套
同步資料夾、冷庫、清理 daemon 或 raw-artifacts mirror。

2026-10-04 工程驗收：本機 246 項、Vast 40 項相關測試通過；Vast 原 hourly
cron 在 16:17 實際採用新本機 policy，最終路徑／root 群組保護也由原入口
實測，均零 errors、沒有新 cache unlink。另一個既有 1,816,673,839-byte／493-file
冷版通過新控制入口的完整獨立解碼與前後傳輸檢查，不落地、不發布、不刪除。
兩端當前 committed packed index 是 QUIC／idle／100%，不代表全量舊
`markets`／`ablations` 回傳已完成。新控制程式於既有 owner 下次安全邊界載入，
不為更新而中斷在途 D 長交易。私人工程收據位於
`artifacts/operations/automatic_cold_storage_20261004/acceptance.json`。

同日另依使用者明確授權啟動一次性整根 tar＋zstd 加密串流保存，包含不使用的
cache；接收、正式冷驗證與逐根回收仍是三個獨立狀態。執行／續傳／恢復與實際
收據見[全量壓縮回傳](vast_bulk_compressed_return_2026-10-04.md)。不是新的日常
snapshot 系統，不改原七日 GC，也不因傳輸片段存在就刪來源。

## 核心規則

資料分三層：不可重建的來源／唯一成果必須保存；正在使用的熱資料保留；
可重建且不使用的衍生快取才可回收。磁碟不足不會改變這個判準。

```text
Vast 完成且穩定的 markets／ablations
  → 既有 penguin ingress（單一共用鎖）
  → D content-addressed 冷發布
  → 當前同步檢查＋完整獨立冷恢復＋再次同步檢查
  → 新鮮、精確的 SSH ACK
  → Vast 原值／程序／服務／link／mount／dry-run 再檢查
  → 私人 quarantine／journal → 回收已保存的熱檔
```

相同內容共用 immutable object；release ID 是原子提交與恢復指標，不是另一份
完整目錄複本。沒有新 bytes 的相同 inventory 不發布 timestamp-only 新版。
冷資料到達不自動 `fetch`、`use`、解壓或覆寫正在運行的 artifact。

全量 SHA／解碼、權限、完整檔案集合與空目錄均依原契約核對；程序／服務／pin／
lease、來源變動、D 掛載、空間預算、Git 解析或同步狀態未知時保留來源。
不自動刪 D objects、唯一原始觀測、未完成訓練、未註冊 cache、事故 quarantine 或歷史收據。

## 已有排程與責任

| 功能 | 唯一執行入口／排程 | 條件與限制 |
| --- | --- | --- |
| 新完成訓練成果回傳 | penguin `stockagent-remote-cold-artifact-ingress.timer`，每次結束後 5 分鐘再檢查 | `training_return.json`；lifecycle-complete、穩定至少 10 分鐘；每次最多一個有界 run／未確認 wave |
| 舊資料完整清冊回傳 | penguin `stockagent-legacy-return@main.timer`／`@partitions.timer` | 只接手既有 cohort；已有 worker 時跳過，不擴張 allowlist，不清除失敗證據 |
| 冷索引／按需 payload 傳輸 | Syncthing＋penguin `stockagent-packed-transport.timer` | 原子發布後明確 scan；15 秒檢查既有 payload demand；不自動解封 |
| 七日熱資料回收 | penguin `stockagent-data-cache-gc.timer`；Vast 同名 cron 每 5 分鐘 | 引用續租；pin／有效 lease 保留；冷恢復、READY、來源與同步門檻均保留 |
| 編譯快取回收 | 原 `stockagent-storage-pressure.timer`／Vast 同名 hourly cron | 14 日未讀寫、89% 高水位／88% 目標；training／compiler、fd／mmap、ctime／mode／nlink gate |

時間是巡檢與穩定門檻，不是完成 ETA；大批次可能占用 owner 多個週期。
新完成 run 目前上限 32 GiB／100,000 檔，C staging 預留 64 GiB；不符合的項目
保留並另外審查。舊 cohort 的 12 小時 capture 是已授權的一次性保存例外，
不是自動捕捉所有新／未完成 artifact。原始資料發布仍走各自 catalog／採集 audit；
`publish: false` 與授權限制不因本功能改變。

新 completed-return 控制入口使用 `cold-return-v3-full-convergence`：共享全部同步
健康 gate，包含 folder/system errors、非 scanning、零 need items／bytes／deletes、
連線、100%／valid；完整冷解碼後再查一次。現有 v2 ACK 必須在 30 分鐘內，
允許最多 60 秒未來時鐘偏差；過期或非有限數值拒絕。plan／apply 與 unlink 前
重查新鮮度；失敗時保留原值／quarantine，下一輪重新驗證，不能重播舊 ACK 清理。

## 一個自助狀態入口

兩台都從 checkout 執行；已安裝 `stockagent-data` 時可省略 wrapper 路徑：

```bash
cd /root/stockAgent

# 唯讀：排程是否啟用、正在處理的根、收據距今多久與最近回收量
bash scripts/run_data_cache.sh automation-status --human

# 再檢查目前 paired packed index；不 scan、不解壓、不刪除
stockagent-data automation-status --human --live

# 保留完整機器可讀資訊；不包含 GUI API key／raw process argv
stockagent-data automation-status --live

# 原本的資料／熱租約與 GC dry run 仍保留
stockagent-data status --human
stockagent-data gc --dry-run

# 自己需要資料時，選一個固定冷版並續七日 lease；不是跟隨 moving latest
stockagent-data use DATASET --snapshot-id EXACT_RELEASE_ID --ttl-days 7
```

`scheduler_active` 只表示排程／cron daemon 活動，不是冷資料完整性。
`--live` 只證明當前已提交冷索引的傳輸，不是全歷史恢復或在途批次已完成。
legacy summary 的年齡是交易更新時間，不是 heartbeat；main／partition 的
item-status 與 task/run 收據仍是詳細階段證據。`cycle-complete` 只表示一次巡檢
結束，須另看 `source_roots_retired`／`deferred_sources`／waiting wave。

## Vast 的明確自動編譯快取範圍

先前 `/tmp/torchinductor_root` 只有手動清理。本次明確授權自動清理後，透過
root-owned 本機 `/etc/stockagent/storage-pressure.json` 納入**原本** hourly owner：
`~/.cache/torchinductor`、`triton`、`nv_cuda`，另加 `/tmp/torchinductor_root`。
預設未註冊其他主機／目錄；設定不由 Syncthing 下發，也不是可執行的指令。

```bash
source scripts/runtime_env.sh

# 先檢查本機 policy：不掃 cache、不刪資料、不安裝排程
run_fintech_python scripts/maintain_storage_pressure.py \
  --policy "$PWD/configs/data_sync/vastai_storage_pressure.json" --check-policy

# 唯讀完整 dry run
run_fintech_python scripts/maintain_storage_pressure.py \
  --policy "$PWD/configs/data_sync/vastai_storage_pressure.json"

# 既有排程已安裝：只註冊這份固定本機 policy，零 cache unlink
run_fintech_python scripts/maintain_storage_pressure.py \
  --policy "$PWD/configs/data_sync/vastai_storage_pressure.json" --enroll-policy

# 尚未安裝排程的節點：同一安裝器驗證、註冊並安裝 systemd 或 cron
sudo ./scripts/install_storage_pressure_service.sh \
  --policy "$PWD/configs/data_sync/vastai_storage_pressure.json"

# 按真正排程入口執行一次；仍遵守壓力、14 日與所有使用中 gate
bash scripts/run_storage_pressure_maintenance.sh
```

policy 只接受固定 schema／scope 與數值水位；root 持有、非 symlink／redirect、
不能被其他使用者寫入、最多 4 KiB，cache 需同一本機檔案系統。拒絕未知欄位、
重複 JSON key、NaN／Inf、少於 14 日、`--force`、自訂根／程序保護覆寫。
cache home 必須本機 root 持有且未被 symlink／redirect，不得 world-writable；
既有 group-write 只允許 root 群組，不改服務權限。環境變數也不能把固定三個
compiler 根改指到其他資料目錄；policy 本身仍禁止 group／world write。
既有不同 policy 不覆寫。手動 `--tmp-torchinductor-only` 仍可用，但不會自行
安裝／改變排程。編譯檔刪除後可重新編譯，下一次首次使用可能增加延遲。

收據：`/var/lib/stockagent-storage-pressure/receipts/`；`cache_scope` 與
`enrolled_policy_sha256` 證明該次採用範圍，`scope_results` 保留各組完整決策。
收到資料後不會自動產生訓練 cache；七日資料 lease 與 14 日 compiler cache 是
兩個不同的生命週期。

## 日誌、恢復與停用

penguin 可分別查原服務，不建立另一個總控 writer：

```bash
systemctl list-timers --all --no-pager
journalctl -u stockagent-remote-cold-artifact-ingress.service -n 60 --no-pager
journalctl -u stockagent-legacy-return@main.service -n 60 --no-pager
journalctl -u stockagent-legacy-return@partitions.service -n 60 --no-pager
```

Vast 的 cron 日誌是 `/var/log/stockagent-data-cache-gc.log` 與
`/var/log/stockagent-storage-pressure.log`。沒有 systemd 不代表無排程，
更不應停止 Caddy／portal／tunnel／Syncthing。

要停用編譯清理，可停用對應 timer 或停用**精確的同名** cron entry；這不影響
冷回傳、Syncthing 或七日資料 GC。不要刪 D，也不要以 `--force` 排除保護條件。
欲變更本機 policy，先停用該維護排程、保存原設定與收據、重新 dry run，
確認之後才由人工協調替換；安裝器故意不覆寫不同的既有規則。

其餘 source-preserved／quarantine／缺失物件／未完成模型不做自動推定；沿
[儲存契約](agents/storage.md)、[完整回傳說明](vast_all_artifacts_cold_return_2026-10-04.md)
與 [七日冷熱資料](packed_dataset_storage.md) 的恢復入口處理。
