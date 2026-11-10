# C 服務、E 常用封存與工作區、D 冷封存

同日後續角色、每日增量與研究保留政策，以
[每日去重封存](storage_daily_deduplicated_archives_2026-11-10.md) 為準。
搬移器現優先仍需保留的舊私有編碼，再處理最新版；下文的固定位置、SHA 與原 owner 合約不變。

本文件落實 2026-11-10 使用者的補充：D 放不下時，常用資料與服務封存可留 E，
OpenBB 等不常用資料留 D。這擴充 E 的角色；不是新增一份備份，也不改原始資料值。

| 位置 | 責任 | 保存方式 |
| --- | --- | --- |
| C／WSL | 程式、網站、下載器、現行服務資料、帳本、控制索引 | 保留服務正在使用的內容；不把帳本交給封存搬移器 |
| E ext4 | 下載、整理、解壓及重新封裝；常用資料集的不可變封存 | 按資料集、固定版本與有界波次保存；同一個封包只留一份正本 |
| D HDD | 不常用資料集的冷封存與所有固定清冊 | 沿原封裝器整理成大包；保留 dataset/source 路徑與內容身分 |
| NAS | 原備份 owner 管理的獨立備份 | 使用原封套與 ACK；本機搬移不冒充 NAS 驗收 |

## 固定清冊與實體檔案分工

`configs/data_sync/packed_object_placement.json` 明確登錄常用資料集，未列入者預設 D。
`/etc/stockagent/packed-object-placement.json` 是已安裝的操作設定。
D 原 manifest、head、物件相對路徑及 SHA 不變；C 的單一 SQLite 索引記錄已搬到 E
的精確物件位置及原 D／新 E 檔案世代。沒有以 symlink 假裝原 D 檔仍在。

E 位置為 `archives/<dataset>/<source-manifest-SHA>/<wave-SHA>/cold/objects/...`。
每批最多 8 GiB／512 個既有封包，並保留原封裝的資料夾與資料集邊界；不把不同
資料集重新混成同一個資料包。原資料集封裝器仍負責把舊細碎物件轉成循序大包。

1. 持有原資料集 owner 與不可變來源讀取 lease，先建立固定搬移計畫。
2. 沿用既有 D→E copier，單次順序複製並核對封包 SHA，fsync 後保存中斷紀錄。
3. 只有位置提交與原 D 名稱回收期間取得原 backup／replication／publication owner。
4. 先持久登錄 E 位置，再記錄 D unlink intent；核對原世代及程序引用後刪除該名稱。
5. 發生中斷時先完成原計畫，不以當時 latest 替換未完成的固定版本。

搬移不新增解壓或目的端全包讀回。原 inventory 中的原始檔 SHA、路徑、時鐘及
缺值語義不變。E 缺盤、世代變動、checksum 失敗或原 owner 忙碌時保留資料並重試，
不退回 C、不刪鎖、不清 pin。共用 hardlink 名稱暫留 D，交原引用與回收 owner 處理。

## 讀取、備份與回收

原 packed reader、v3 alias、chunk／pack verifier、subset reader、backup catalog、
封閉備份輸出與原退休器均使用同一個位置索引。讀取仍核對 E 世代和原內容 SHA；
備份封套仍使用原相對路徑，因此接收端不需要知道 Penguin 的 E 實體路徑。
原退休器仍先檢查 references、current head、research pins、程序及固定計畫，
之後才依該計畫回收 E 或 D 的實際檔案；E 回收量不計入 D 回收量。

部署時先通過本機讀取／搬移／中斷續跑／備份／退休語義測試，再由原 lifecycle
重載 owner 固定載入版本，最後寫入與程式 SHA 綁定的 `readers-ready.json`。
未就緒或程式變動時，自動排程只記錄等待，不先搬走 D 原件。
這不代表整機故障還原或 NAS 全歷史驗收已通過。

## 自動排程及觀測

`stockagent-packed-object-placement.timer` 每批結束後 20 秒重試；每次只做一個
有界波次並釋放原鎖，輪流處理資料集，避免大資料集讓其他服務封存飢餓。
全域讀取 lease 保護全域 GC，特定資料集 owner 為獨占；短交接只額外取得原
backup／replication owner，不要求其他資料集的長時間讀取 lease 全數退出。
已完成的 E 寫入只重新核對原 copier 的固定 metadata，不再複製，也不重複預留
已占用的 payload 空間。固定來源計畫另持久保存在同一 E 版本目錄，供控制索引修復。
E 仍使用原共享容量 lease，保留 64 GiB 工作餘量並核對
實體後端、guest 空間及固定 ext4 UUID。當沖保護時段停止背景大量 I/O。

既有 source-local repack、prepared install、dedup candidate 及退休排程繼續處理冷資料，
不另開下載器、不動其他視窗維護的 FinLab／FinMind／TEJ 原始資料搬移程式。
Vast 的備份驗證協助維持停用；訓練不受此變更影響。

全資料面板每分鐘讀取原 owner 的小型收據及 C 上的位置索引彙總，顯示本批 bytes／封包數、已分流量、
D 已回收配置空間與等待原因。ETA 使用相同固定版本、波次及階段的實際增量；
等待鎖或容量時不提供虛構完成時間。單批完成與全庫完成分開呈現。
已完成的資料集交接累計另列，排程切到下一批時不會消失；容量分類也分開列出
E 的常用封存及尚未交接資料，兩者均不因分類名稱授權自動刪除。

## 本輪證據與仍在進行的範圍

- 原期貨去重候選保留 4,684,161 個來源路徑，唯一內容為 83,988 份，
  新封包總長度 38,604,080,668 bytes；不是原資料的任意抽樣或刪欄位。
- 原 owner 已完成舊 E 編碼回收，實際配置量為 420,777,398,272 bytes（391.88 GiB），
  保留原計畫、完整來源 inventory、checkpoint 與歷史完成收據。
- 423 個搬移、讀取、原備份與原退休相關測試通過，2 個既有測試略過；
  面板與進度加上交接整合共 439 個測試通過；11 個清冊與檔名存在性測試通過，
  前端共用模組另有 47 個 Node 測試通過。
- 台股分鐘首批 8,145,650,807 bytes、7 個封包已於 17:09:44 完成交接，
  D 實際回收 8,145,670,144 allocated bytes；原 manifest SHA 不變。後續進度以
  `/var/lib/stockagent/packed-object-placement/status.json`、SQLite 與逐批 `.done` 為準。
- 全庫仍由原排程持續遷移；不是所有舊版、受保護名稱與歷史缺件均已處理完成。

17:08 完成的再次檔名查驗是 **partial**：掃描註冊的 1,303 個命名空間，
其中 `local-bybit`、`local-tw-public` 與 `training-dataset-recipes` owner 正忙，
未納入本輪計量。不能將與早上不同的範圍差額解讀成已完成遷移。

| 本輪 D 仍存在的格式 | 名稱數 | 名稱長度合計 |
| --- | ---: | ---: |
| 已確認新格式 | 1,939 | 1,657.58 GiB |
| 舊格式 | 55,611 | 834.90 GiB |
| 增量／混合、尚待確認 | 20,212 | 440.25 GiB |

這是 16:52–17:08 的非原子觀測，之後的搬移不回填成同一時刻的容量。
名稱長度含 hardlink 別名，不能相加作為實體占用；本輪去重後的 D 配置量
為 2,617.80 GiB，也不是 D 全磁碟已用量。仍缺的 103 個歷史名稱沿用原逐項證據，
不把舊 metadata 視為負載仍存在。

11:33–11:45 的較完整新舊封裝清點及解讀見
[原清冊](d_cold_pack_migration_inventory_2026-11-10.md)。本輪再次盤點期間部分資料集
owner 正忙，新的報表標示 partial，不把未取得的資料集當成零或已完成。
