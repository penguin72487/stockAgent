# WSL 安全清理紀錄：2026-10-27

此紀錄是 Penguin 於 2026-10-27 約 21:09 台北時間的現場結果；背景工作與
容量會持續變動。依 [儲存角色與分層](storage_roles_and_tiers_2026-10-25.md)，
WSL 保留增量、可變狀態及服務／研究工作集，D 槽持有本機 immutable 冷庫。
角色分類、名稱、檔案年齡或相同大小都不能取代固定版本的完整還原與回收驗證。

## 本次已完成

| 項目 | 操作 | 移除檔案 | 原檔佔用的配置空間 |
| --- | --- | ---: | ---: |
| APT 下載及索引快取 | `apt-get clean` | 76 | 407,920,640 bytes |
| pip 下載與 wheel 快取 | `python -m pip cache purge` | 340 | 428,642,304 bytes |
| npm content-addressable 下載快取 | `npm cache clean --force` | 39 | 839,847,936 bytes |
| 合計 | 未移除已安裝的套件或來源資料 | 455 | 1,676,410,880 bytes，約 1.56 GiB |

數值來自移除前後逐檔存在性與原 inode 配置區塊，不是 Windows C 槽的實體
回收量。pip 自行保留一個 selfcheck 檔案。計畫與結果保存在
`/var/lib/stockagent/wsl-cleanup-20261027/apt-cache-{plan,result}.json`、
`/var/lib/stockagent/wsl-cleanup-20261027/pip-cache-{plan,result}.json`、
`/var/lib/stockagent/wsl-cleanup-20261027/npm-cache-{plan,result}.json`。

線上 `fstrim` 回報 6,318,305,280 bytes 的可丟棄區塊，但執行前後的 C 槽
可用量並未量到增加，不能把該數字算成 C 槽釋出空間。收據位於
`/var/lib/stockagent/wsl-cleanup-20261027/online-trim-result.json`。
Windows 的既有離線 VHDX 壓縮排程在 WSL 執行中回報 `skipped-running`；
真正離線縮檔需要另行安排服務可停止的維護時段及前後實測。

## 已排入原有回收流程

以下大小是原固定清冊中的 unique-inode 配置量，不等於已回收或未來必定
能釋出的容量；硬連結、來源變動及回收驗收仍需逐項確認。

| 範圍 | 固定清冊大小 | 目前狀態 |
| --- | ---: | --- |
| `artifacts/benchmarks/workflow_speed_optimization_20261021/round11` | 13,404,770,304 bytes，約 12.48 GiB | 使用既有回收 owner 重新驗證固定 D 版本。13 個冷物件及 59,687 個編碼成員已讀回，尚在比對還原工作區；原始資料逐檔解碼、來源與服務引用、傳輸等驗收未完成。 |
| `artifacts/benchmarks/workflow_speed_optimization_20261021/round7` | 14,506,184,704 bytes，約 13.51 GiB | 已用 `stockagent-agent` 排入受監督的一次性串行工作；只在 round11 的原 run 成功、完整回收收據成立且原路徑消失後，才啟動 round7 的既有 frozen-inventory owner。前一批失敗或無法確認則安全停止。 |

run 分別為 `wsl-round11-retirement-20261027T125453-ba7ebd66` 和
`wsl-round7-sequential-retirement-20261027T130311-6ee0bf54`。round7 之前的
失敗與階段收據另保留在
`/var/lib/stockagent/wsl-cleanup-20261027/benchmark-round7-prior-state.json`。
串行入口另以八種模擬狀態驗證成功、等待及失敗關卡，收據為
`/var/lib/stockagent/wsl-cleanup-20261027/round7-gate-tests.json`；這不代表
實際 round7 回收已執行。
背景程序活動、讀回進度或舊成功收據都不能當作本次刪除完成；以新收據的
`all_selected_retired`、實際原路徑、回收配置量與後續讀回為準。

## 保留與待處理

- 一個約 5.25 GiB 的台股中斷解壓副本，固定 D 版本缺少 object
  `039395ce89379b80a370289ccbf4def9291f5c862f361d43f5872390b229c75f.zip`；
  重新 dry run 失敗，保持原樣。既有的
  `configs/data_sync/backup_history_disposition_20261004.json` 對這個固定歷史版本
  記錄放棄缺件恢復，但也明列不授權刪除既有來源或 metadata，不能把該決議
  當作刪除此副本的驗證。其日誌為
  `artifacts/operations/agent-workflow/runs/wsl-partial-cold-dryrun-20261027T124804-577df328/run.log`。
- 既有 materialization GC dry run 中，四個版本受到部署 pin 保護，另一個有
  活躍 lease，沒有可回收項目。分鐘 session 共用機制的候選 dry run 在 16 個
  支援且完整的 generations 中沒有找到重複群組；另有三個未完成 generation
  不在此次判定範圍。這不是整份分鐘快取完整性稽核。
- FinLab 的大型固定冷庫版本仍在完整還原；來源 writer／reader 的切換及回收
  驗收未完成，原始工作區保留。正在使用的來源、帳本、研究資料和模型快取
  不以名稱或年齡直接刪除。
- UV 快取檢查到 24 個多重硬連結檔案及 11 個符號連結；本次沒有對其執行
  批次清理，也沒有修改已安裝環境的檔案連結身分。
- `/lost+found` 約 2.6 GiB，共 2,521 個一般檔案及 487 個目錄（含根目錄）；
  其中兩個大檔是 Parquet，其 footer 分別宣告 86,415,040 和 81,450,490 筆，
  含日期、來源／目的 ID 與權重欄位；未掃描全部資料列。來源及可還原性尚未
  確認，全部保留。這是限定
  該目錄的檢查，不代表所有 WSL 檔案均已逐一分類。

約 21:09 的空間快照為：WSL ext4 可用約 275.33 GiB、Windows C 槽實體可用約
16.82 GiB、D 槽可用約 333.46 GiB。C 槽低於原本 25 GiB 保留門檻；WSL
檔案系統內可重用空間與宿主機 C 槽可用空間必須分開觀察，受容量門檻保護的
工作可能暫停。此紀錄未關閉 WSL、未修改既有容量門檻。
