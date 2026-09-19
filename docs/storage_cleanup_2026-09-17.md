# penguin 儲存清理稽核（2026-09-17 TST）

這份紀錄是一次性觀測，不取代 `AGENTS.md` 的儲存契約與
`docs/packed_cold_backup.md` 的操作規範。penguin C 冷庫是現行權威；D
為只增不刪的歷史備份。Syncthing 傳送冷庫，熱資料由明確的 `use` 與租約管理。

## 已完成的可驗證回收

| 操作 | 範圍 | 收據記錄的回收量 |
|---|---|---:|
| 舊編譯快取清理 | allowlist 內的 TorchInductor/Triton/CUDA 快取 | 5,021,433,856 bytes |
| SHA-256 相同快取共用 inode | 舊 execution tape 與 panel generation；路徑與內容保留 | 6,595,637,248 bytes |
| C 冷庫 rolling retention | 2,339 個不再受保護的歷史物件、65 個舊 manifest | 105,839,865,856 bytes |

前後 `df -B1 /` 可用空間由 298,290,618,368 增至約
415,895,355,392 bytes，差約 117.60 GB。`df` 含同期下載器寫入，不能直接當成
逐項回收量；以各操作收據為準。D 沒有刪除，C 現行 head、受保護 release、
來源 `data_*`、materialized 熱資料及執行中的 artifact 均未被此次冷庫清理刪除。

操作收據：

- `/var/lib/stockagent-storage-pressure/receipts/storage-pressure-apply-20260916T161055.695431Z.json`
- `/var/lib/stockagent-artifact-dedup/receipts/artifact-dedup-tw_day_trade_execution_v7_official_close_1325-20260916T161636.247616Z.json`
- `/var/lib/stockagent-artifact-dedup/receipts/artifact-dedup-generations-20260916T161653.541712Z.json`
- `/var/lib/stockagent-packed-retention/last-apply.json`

冷庫清理前的 plan 對 2,339 個候選物件逐一確認 D checksum 收據、現行
head／pin／租約、24 小時保護期及 penguin ↔ vastai1T 同步狀態。清理時停下
Syncthing 和備份服務、重查 plan 指紋並驗 C SHA-256；之後服務重啟，
`last-apply.json` 記錄清理後 Syncthing 100%、need 0、remoteState valid。
現行 `cftc-legacy-pre2000` release 另在 C、D 各做一次完整物件驗證，
manifest SHA-256 與 8 個物件均通過。這只是選定 release 的抽查，不能外推全庫。

## 本次修正的收據判斷

WSL 重新掛載 D: 後，Linux `st_dev` 從 44 變為 71。原有 1,631 個候選物件
（80,306,094,080 配置 bytes）的過往 SHA-256 回讀收據，除了 `st_dev`
以外，inode、大小、mtime、ctime 均未改變；另 708 個候選物件的完整
signature 相同。備份與 retention 現在在掛載／volume marker 檢查通過後，
只忽略可變的 `st_dev`，其餘四欄和收據有效期仍必須符合。缺收據、逾期、
檔案識別欄位或校驗值變動仍 fail closed。相關 39 項備份、retention、
packed snapshot 測試通過。

## 未清理與仍需追蹤

- D 全庫備份在清理前的最近狀態仍是 `degraded`，約 344.5 GB 待核對、
  41 個 head 待處理，且一些舊 manifest 參照的 C 物件已缺失。備份服務
  `active` 不等於 D 全庫可還原；這些歷史問題需單獨追查。
- 7 日熱快取 GC 的候選 release 有 pin，`would_evict=0`，未繞過 pin 清理。
- `artifacts/markets/us` 的內容 SHA-256 查核沒有完全相同檔案可去重；
  不能憑名稱或相似性移除。`data_openBB` 仍有下載器工作。
- 舊 `/srv/stockagent-artifacts-hot` 多數是與 repository `artifacts` 共 inode
  的硬連結名稱；其 `du` 名義大小不是可回收空間。仍有未核對的獨有路徑，
  因此沒有刪除整棵 transport。

後續仍用 `./scripts/run_packed_retention.sh plan`、
`./scripts/run_data_cache.sh gc --dry-run` 與來源／冷庫驗證決定下一批。
不得因本次回收成功而放寬 D 收據、pin、程序引用、peer 收斂或來源完整性門檻。
