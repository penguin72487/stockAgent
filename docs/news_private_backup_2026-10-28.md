# 新聞固定版本的私有同步：2026-10-28

22:55 接續：固定 plan 不變，19／30 波、6,243 原始檔、5,218,519,039
bytes 已本機封閉發布，NAS 還原 ACK 尚未取得。22:39 在唯讀目的驗證
界線受控停止舊 publisher，exit 143 明列為中斷；新版 run
`news-private-fair-owner-resume-20261028T143955-ff372381` 以
`--publish --yield-owner-between-waves` 沿原 plan／progress 接續。
完整 staging 先重驗 SHA，再重用固定來源，沒有從零重抄既有 ledger。
每波讓出原 owner、重讀其他人的提交及 ACK，再重新確認容量；全部發布
後切換 `--receipts-only`，只對帳、不再複製。等待鎖時保留已完成計數。
新版已觀察到載入並進入原未提交波次的完整 native staging 讀回。
一般私人 cold 補傳另有明確 policy，沿同一 owner 核心等待接手，避免
低頻輪詢錯過短暫讓鎖。細節、測量與未解範圍見
[WSL/NAS 進度](wsl_storage_progress_2026-10-28.md)。

使用者在 2026-10-28 明確要求把新聞資料同步到既有 lab203／NAS。先前
`configs/news_fulltext.json` 與 packed catalog 的本機研究預設，不再用來阻擋
這次使用者自有節點間的固定版本私有備份。這是使用者同步指令的範圍，
不是新增來源授權或開放公開分發；原始 manifest、來源權利與署名原樣保留。

## 唯一來源與範圍

- [私有同步政策](../configs/data_sync/news_private_backup.json) 固定 manifest
  `8aa535f89c3605fb530f2e23b1d4f8f22f273f7da2c015e323a7c03101046a54`，
  generation `versions/20261016T155152Z-890f02f7`。
- 原四張 Parquet 表有 6,278,162 articles、5,796,288 documents、
  6,105,349 article_stocks、4,703,099 metadata_observations。
- 完整閉包為 **11,494 個原始檔、9,776,598,701 bytes，約 9.10 GiB**：
  7,673 個表分區約 5.38 GiB、3,816 個凍結來源快照分塊約 3.73 GiB，
  以及原始 manifest、generation marker、來源快照清冊和驗證／品質報告。
- 依 manifest 解析跨 generation 的共用檔；每個唯一來源路徑只送一次。
  不遞迴複製舊版本、build work、即時 SQLite/WAL、HTTP 暫存或整個 crawler。
  此交付是既有固定資料版，不宣稱包含其後尚未建入此版的來源增量。

## 排程與證據

[原備份 owner 的新聞介面](../scripts/sync_news_private_backup.py) 使用
[固定計畫與分批實作](../stockagent/data_sync/news_private_backup.py)，不另建
Syncthing share、NAS writer 或下載器。配對 device、repository、實際 D mount、
原 backup runtime lock 及來源 builder 的共享鎖都需符合。

一次量測原 ingress 容量，持原 owner 鎖預留整份新快照的 staging／pending
預算，逐批更新原 ledger；不對每個小檔或每波重跑完整容量盤點。保留 D
64 GiB、staging 24 GiB、pending 32 GiB 等原門檻。由原設定的四路 worker
複製並計算來源 SHA，每波再獨立完整讀回目的檔 SHA、檢查 exact set，才
原子發布 READY／dispatch。原檔不刪除，遇到不符則保留證據。

目的檔完整讀回改用既有 `windows_cold_io.hash_many`，每組至多 16 個檔案、
四組並行，直接從 Windows 讀取已登錄的 D 卷，仍逐檔核對實際 SHA、大小、
前後 inode／時間簽名及目錄 exact set；接收端的 portable 預設不變。
同一組四檔 114,321,445 bytes，原生完整 SHA 實測 11.60 秒，WSL 完整讀回
120.01 秒；測量順序、快取與背景 I/O 未受控，因此這只是選擇 I/O 介面的
診斷證據，不能宣稱整體工作加速十倍。

21:08 只在本次新聞 publisher 的 read-only 驗證界線停止舊程序，保留已發布
六波、完整未提交 staging、原始資料與失敗紀錄。原 owner 由核心釋放後，
同一固定計畫續跑；完整 READY staging 先重新全檔 SHA 驗證，僅對本次剛
驗證且簽名相同的檔案省去重複來源複製，最後仍走原封閉發布完整驗證。

每波目標不超過 512 MiB，並遵守原 512 檔及 8 GiB 硬上限；這份閉包共
30 波，傳輸含計畫／envelope／READY 的大小另列在 progress。第一波不必
等待數 GiB 的來源檔全部複製完成。Parquet 與快照分塊保留原 bytes，沒有
再次展開資料表或重新組裝四 GiB SQLite。原 lab203 worker 可直接讀既有
dispatch，在其已部署的 worker 範圍內並行收件、Restic 備份及獨立還原。

通知執行與資料處理重疊；發布後不持 source owner 等待 Syncthing／NAS。
既有 folder scan 忙碌時保留 pending 通知，由同一通道續送。
實際 ingress 原本每 300 秒安排全掃描，20:21 開始的一次掃描持續超過
半小時。已把此 folder 的全掃描改為每 3,600 秒兜底，保留原 watcher、
逐批通知和其他 folders；原 lab203 device 的協定壓縮由 `metadata` 改為
`always`，所有其餘 device 欄位與其他 devices 的設定均讀回相符。來源
bytes、頻寬上限與收件範圍不改變。這採用
[官方的掃描與資料壓縮選項](https://docs.syncthing.net/users/config.html)，
不預先聲稱一個特定加速倍數。私有 `transport-tuning.json` 保留前後值與
不相關設定的 identity。
來源複製完成、Syncthing 收斂、NAS 新固定 snapshot 還原 ACK 與新聞可用性
仍是分開的驗收，沒有接收端回執就不宣稱送達或已備份。

計畫、進度與機器證據放在忽略的私有操作目錄：
`artifacts/operations/storage-single-authority-20261025/news-private-backup-20261028/`。
固定計畫 identity：
`5e8df00825f11db40fbef4d71004d920e99ece51e1c4dd5c3c058a41dd1a8422`。
最新進度見 [progress](../artifacts/operations/storage-single-authority-20261025/news-private-backup-20261028/progress.json)。
20:54 第一波完成；21:14 已完成八波的來源複製、獨立目的 SHA 與封閉發布，
827 個原始檔、4,244,658,549 bytes。這是本機已封閉發布的 bytes，不是遠端
已送達的 bytes。切換後的續跑由原 task 下的 run
`news-private-native-publication--20261028T130853-0d07e905` 監督；21:15 對
首波與當時最新波的精確 READY 查詢都尚未進入 Syncthing index，folder
仍在 20:53 啟動的全掃描，paired NAS ACK 尚未取得。實際最新狀態以
progress、精確傳輸查詢與 paired ACK 為準，不使用舊 index 的 100% 顯示。
21:25 的 [milestone](../artifacts/operations/storage-single-authority-20261025/news-private-backup-20261028/milestone.json)
記錄 10／30 波、1,662 原始檔、4,903,000,492 bytes 已本機封閉發布，佔來源
bytes 50.15%。21:21 對當時九波的所有 READY 分別查詢，尚未入 index；
NAS 還原 ACK 尚未取得。既有 `stockagent-d-cold-scan-retry.timer` 維持 active，
其 service 實際命令含 `--backup-transport`；掃描忙碌時保留待辦並自動續送。

驗證：新聞私有同步、原備份 queue、native delivery 與 handoff 共 156 項
回歸測試通過；增加完整 staging 中斷續跑的簽名測試後，新聞／native
delivery 的 24 項測試再次通過。兩組有重疊，不能加總為不同測試數。
`validation.json` 固定當次原始碼與測試紀錄，`native-rollout-stop.json`
保留受控停止及原進度，`transport-observation.json` 保留獨立傳輸觀測。

## 接收端還原

從原 paired ACK 列出的每個固定 Restic snapshot 還原該批，先使用原 portable
`verify_backup_delivery.py` 核對 envelope、READY、exact set 與全部 SHA。
同一計畫的 30 波內，`private-news/PLAN_ID/` 的檔案路徑不重複，合併成該
私有根後，`data_news/manifest.json` 與全部被引用 parts／snapshot chunks
可由原新聞 reader 直接使用。`backup-plan.json` 包含所有原始來源 SHA、
長度、角色與政策；不把同目錄其他計畫或 moving latest 混入。

本次不啟動訓練或修改 model/features；`publish`、`remote_training_export`
與 `training_injection_enabled` 的原始觀測值保留。若日後啟動具體研究工作，
使用這個固定版本及明確的研究設定，另記錄當次資料使用與訓練驗收。
