# 2026-10-06 NAS 補傳與去重進度修正

本輪目標是把 penguin 現存 canonical cold 清冊補齊到 NAS，維持後續自動增量。
以已發布檔案及已收到封存的固定相對路徑、完整 SHA 和獨立 NAS 還原收據結案；不把傳輸
待辦為零、服務 active 或容量分項相加當作完成。未發布或已丟棄的歷史仍不在
原清冊內；已收到的五份封存另有固定清冊，等待入庫後合併驗收。USB 金鑰保管
已確認，Windows 登出驗收已取消。

10:33 台灣時間固定原清冊：機器 NAS 驗收 **751,567,954,942／817,741,140,347
bytes（91.9%）**，剩餘 **66,173,185,405 bytes／211 檔**。這是當時觀測，
後續增量以持續更新的收據為準，尚未全部完成。

## 上游遺漏封存的修復

09:38 的既有 `stockagent-vast-bulk-return.service` 因 `EXDEV` 失敗：私人 incoming
與 canonical cold 的兩個 bind mount，即使 device 相同也不能跨 mount hardlink。
四個已收到、已準備發布的 carrier 合計 **118,851,915,064 bytes**，尚未進入
08:46 的 fixed cohort；最大單檔 **71,140,449,081 bytes**。這不是新增抓取的
歷史，也不能把它漏出清冊後就宣稱全量同步完成。
完整 received receipt 盤點又找到尚未準備 publish alias 的第五個 cache carrier；
凍結範圍合計 **137,943,790,582 bytes／5 carriers**。三筆 failed transport prefix
另留證據，不把失敗片段當完整 frame；markets 的原 producer 非零，保留收到的
原值，仍標示 `must-reconcile`，不宣稱 Vast 整個 scope 歷史完整。

已修正 hardlink 走 physical D alias，先確認 parent 與 canonical parent 同 inode。
超過 2 GiB 的 carrier 使用新的 `bounded_zstd_tar_preservation_v2` dataset，保留
原 carrier 並發布有序 parts、plan、原始成員 index。每段最多 2 GiB，仍使用
既有 packed CAS 與固定 lab203 relay，不提高單檔 8 GiB 門檻、不要求接收端
執行新收到的程式。完整 SHA、source signature、exclusive write、flush、
分段獨立讀回、canonical full original decode 全部保留。

`bulk-final-regression.xml` 有 **130 passed**，包括 v1 相容、v2 正式發布／
完整解碼／原根還原、損毀與次序錯誤、變動來源、中斷／重試、physical alias
hardlink，以及先保存所有已收批次再清理的順序。10:00 原 owner/timer 已恢复，
`bulk-worker-resumed.json` 固定程式 SHA 與驗證收據。這只證明修正部署；大型
實際入庫與 NAS 收據仍需逐批驗收。原 fixed cohort 不改分母，封存入庫後另凍結
expanded cohort。NAS 檔案還原與原始成員語意解碼分開記錄。

10:31 第一份 v2 真實 carrier **21,223,985,638 bytes** 已發布，canonical
original decode 通過 **2,887 檔／167,293,069,861 logical bytes**，原始成員
fingerprint 與收到時一致。固定 manifest 為
`e10d1c9ba01283d62546e61a6ff59251acc3119dde930752fb5e1e16ddb70438`；
`first-bulk-v2-original-recovery.json` 保存來源冷恢復證據。此時該 carrier 的
12 個 unique CAS 成員尚待 NAS 收據，沒有將 penguin 還原當作 NAS 驗收。
同一 coordinator 已接續第二份 71.14 GB 的分段保存。

後續 `complete-scope-final-regression.xml` **138 passed**；
`complete-scope-transport-acceptance.xml` **73 passed** 覆蓋 paired connected、
同步狀態與 deletes/error 接受條件，`ordered-cas-part-parity.xml` **42 passed**
涵蓋有序 carrier 內多個邏輯 part 指向同一 CAS inode 的情況。這些測試彼此重疊，
不相加成總測試數。`proof-bound-admission-verified.xml` **81 passed**：v1／v2
將 received／part plan 的 SHA 傳給原 installer，保留 source 與 canonical 的
完整讀回，省去 caller 重讀 carrier。來源或分段在 admission 前改變會拒絕，
不把變動後的 hash 當新的正確身分；本輪大型在途程序不中斷，新週期載入此修正。

完整 received cohort 在
`/var/lib/stockagent/backup-stream/received-preservation-cohort-20261006.json`，SHA
`18792e82727659c31cb1c8dd0fe02775775b07843c63701d0549ded1467797b2`。
`audit_nas_sync.py --upstream-cohort PATH --upstream-sha256 SHA` 會等待五份原值
冷恢復收據，從原 current catalog 固定其精確 release／objects／manifest／head，
再和原 817.74 GB 清冊去重合併驗收。新資料仍沿原增量服務；observer 不發布
payload。加 `--require-transport-convergence` 後，完成還要正反向 idle／need 0／
error 0。可持續觀察最多七天，逾時仍退出 75；不能宣稱已完成。

`observer-outage-recovery.xml` **77 passed**：Syncthing API 暫時不可用時，觀測
輸出 `observation_unavailable`、拒絕完成並在下一輪重試；恢復後重新讀正反向
證據，不沿用舊成功狀態。`observer-api-retry-handoff.json` 固定只讀 observer
的 PID/start ticks 及舊收據 SHA；沒有中斷任何來源／NAS writer。

持續觀察已由原 task 的 tmux 監督啟動，最新輸出在
`artifacts/operations/nas-catchup-20261006/complete-source-cohort-final-progress.json`。
原 817.74 GB 與五份 carrier 的來源恢復待辦分列，避免未發布資料被漏算。
`status_vast_bulk_return.py` 同時列出 received verification、canonical partition、
canonical original recovery 的各別 bytes，不把收到壓縮檔當作冷庫／NAS 驗收。

10:00 的 60 秒真實 QUIC 傳輸視窗平均 **1,134,300 protocol bytes/s**；全程同一
直連，非 relay，來源與 peer 設定無限速。這只是該視窗，不能推算固定全歷史 ETA。
來源未取得 TCP WAN mapping，lab203 TCP 22000 探測 timeout；未改其他 peer 的
既有網路政策。更多連線需要兩端都配置，單改來源不能證明有效，依
[Syncthing 官方契約](https://docs.syncthing.net/advanced/device-numconnections.html)。

## 停滯原因與修正

原來源畫面只顯示 Restic 的 710.13 GB，未計入後續 immutable archive 的新增
覆蓋。以相對路徑及 SHA 精確去重後，08:47 約 732.28／817.74 GB 已覆蓋。
兩套有約 101 GB 重疊，不能把各自總量相加。

raw source owner 原為一批送出後等待 NAS ACK，再選下一批，並按大小排序；
許多小檔案在舊 Restic 已有備份。已驗收歷史 ledger 還在每輪逐批重寫，增加
HDD 延遲。本輪保留既有 owner、Temporal workflow 和 publication journal：

- 優先選沒有機器 NAS 證據的物件；所有已發布檔案均保留入列身分，避免重複波次。
- 私有 policy 使用公開 `configs/data_sync/lake_source_replication.json`：最多
  4 批在途、每批目標 2 GiB、單物件上限 8 GiB、在途總量上限 32 GiB，另保留
  D 的 64 GiB 容量門檻。超過目標但小於單物件上限的檔案可獨立成批。
- 新 ACK／transport retirement 先持久化；未變更的已驗收 ledger 不逐批重寫。
- raw bytes 沿 Temporal；原 Restic 排程補 manifest/head 原值。固定 cohort
  內已前進的舊 head 用 captured UTF-8 bytes 補傳，與目前新 head 分開發布。
- 使用精確 ledger 聯集顯示 NAS 覆蓋，另列只有使用者轉述 pilot 的證據。

這些是有界資源政策，不是新的權威冷庫。NAS 刪除、Restic prune 和接收端
自動刪批仍關閉；來源暫存回收沿原精確驗收和程序引用門檻。

## 固定本輪清冊

08:46:51（台灣時間）清冊共 **27,299 檔、817,741,140,347 bytes**；捕捉時
missing objects 和 metadata errors 均為零。

- 位置：`/var/lib/stockagent/backup-stream/full-sync-cohort-20261006.json`
- 檔案 SHA：`c714ff4078c4b365941126665c81e1e047af770424497a5bdffd421622e45227`
- Catalog identity：`bf88633330e8a10f0ec656b2052344319f074bff9c1040b7c583aabddb54e04f`

`scripts/audit_nas_sync.py` 對這份固定清冊核對 authority-validated Restic／archive
ledger。只有全部檔案均有機器獨立還原證據、沒有來源錯誤時，才寫
`all_available_cold_files_verified: true`；`all_history_backup_verified` 保持 false。
目前工作中最新清冊可繼續增加，不改這輪分母。

```bash
bash scripts/run_lakehouse_control.sh python scripts/audit_nas_sync.py \
  --cohort /var/lib/stockagent/backup-stream/full-sync-cohort-20261006.json \
  --output artifacts/operations/NEW_FIXED_COHORT_STATUS.json
```

output 必須全新。可加 `--watch-seconds 14400 --interval-seconds 30`；觀察期限
到了而尚未驗收退出 75，不能當作成功。不直接啟動第二個 production writer。

## Windows 原生 I/O 實測

只改 D cold → D `.staging` 的 byte I/O，保留 Linux authority、mount、source
signature、完整 SHA、exclusive CreateNew、durable flush 和封閉集合驗證。
來源或原值驗證失敗保留 partial；不退回 C 替代儲存。正式 systemd wrapper
補有效 WSL interop socket 與 PowerShell 路徑，避免依賴 IDE 的互動環境。

同一真實不可變檔案，封裝及獨立完整 SHA／集合驗證均得到相同 delivery identity：

| 檔案 bytes | 順序 | WSL 9p 秒 | Native 秒 |
| --- | --- | ---: | ---: |
| 19,261,109 | 9p → Native | 4.759 | 5.279 |
| 134,353,641 | 9p → Native → Native → 9p | 64.166、23.012 | 6.869、7.108 |

沒有清除 OS 快取；小檔案包含 PowerShell 啟動成本，完整流程未勝出，因此
正式 adapter 只對 **至少 128 MiB** 的檔案使用原生 copy／hash。這些時間限
penguin 封裝與獨立驗證，不包含 Syncthing 或 NAS，不宣稱全鏈吞吐倍率。

單檔 PowerShell 啟動成本仍拖累小物件。本輪另用同一固定腳本把 metadata
經 stdin 傳入，每個 native process 處理最多 16 個成員，總量不超過 16 GiB；
每檔仍保留完整 hash、size、source signature、exclusive copy 和 flush。
同樣四個真實來源物件、72,671,888 bytes，以 9p → Native → Native → 9p
交錯量測完整封裝與獨立驗證：**68.456／13.130／11.434／61.292 秒**，四次
delivery identity 完全一致。沒有清快取，且正式背景工作持續執行。據此對
同一波次至少 16 MiB 的成員採批次 copy／hash；單檔與非 D 路徑仍沿既有
adapter。這也是來源量測，不是 NAS 完整鏈的倍數。

## 驗證與進行中的驗收

本輪紀錄在 `artifacts/operations/nas-catchup-20261006/`：

- `baseline.json`：修改前精確去重與缺口。
- `source-policy-install.json`：私有 policy 前後 SHA 和驗證，不含秘密。
- `persisted-history-replay.json`：兩條真實持久 workflow history replay 通過。
- `source-io-comparison.json`、`large-source-io-comparison.json`：同一資料、同一
  delivery identity 的來源完整流程比較。
- `batched-source-io-comparison.json`：四個真實小物件交錯測得批次 I/O 改善。
- `regression-control.xml`：在正式 Mamba control 角色重跑，121 passed／2 skipped。
  `regression-batch.xml` 再驗批次中斷、變動來源與遺漏證據，125 passed／2 skipped。兩項本機 Restic integration
  因未提供本機 Restic binary 跳過，不能替代現場 NAS 驗收。
- `worker-rollout.json`：安全邊界換 worker 的已載入 fingerprint 證據；只有
  實際產生 accepted 收據才代表新版生效，不重啟 PG／Temporal server。
  批次 I/O 的後續切換另保存 `worker-rollout-batch.json`，不覆寫首次收據。
  10:24 的 `worker-rollout-final.json` 已 accepted，已載入 fingerprint
  `7e2428cf7b80a7a86ff82443633e2961949477e2576c0b6b46f4f86a816246ff`；
  前後兩條 workflow run ID 相同且 RUNNING，未重啟 PG／Temporal server。
- `cohort-progress.json`：固定 cohort 的持續驗收進度，最後 state 必須 accepted。

上列檔案如尚未產生或 state 非 accepted，即是待驗收；本文件不預先宣稱本輪
27,299 檔全數完成。日常新資料仍由原 systemd／Temporal／lab203 排程處理。
