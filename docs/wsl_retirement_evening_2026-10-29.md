# WSL 退休與 lab203 備份：10 月 29 日晚間觀測

本頁記錄台北時間 21:03–21:16 的觀測及程式驗證，不是完成退休或 NAS
還原的證明。既有 [來源證據交接](lab203_source_evidence_handoff_2026-10-29.md)
的固定身分與限制仍適用。

## 空間與已完成的工作

- 21:11:42：WSL ext4 可用 265,152,344,064 bytes；Windows C 實體可用
  47,437,545,472 bytes；D 可用 367,156,994,048 bytes。WSL 位於 C，兩者
  不能相加。C 已低於一般下載的 64 GiB 保留線；不得限制當沖服務。
- 第 7、8、9、11 輪的既有 owner 收據合計記錄 WSL 釋放
  48,574,013,440 allocated bytes，約 45.24 GiB。這是歷史累計，不是本次
  新刪除。部分退休回執仍為 `single_d_volume`，不證明完整 NAS 還原。
- 本次 Windows 非現行 WSL swap 的原腳本只讀稽核回報 0 個可刪候選，3 個
  跳過，沒有執行刪除。WSL 內釋出空間不等於 Windows C 已歸還空間。
- 本次四個已結束、由本次 pytest 建立的合成 fixture，先盤點並確認無
  symlink、外部 hardlink 或程序引用，再以同檔案系統暫存名稱複核後刪除，
  共 19,398,656 allocated bytes；未刪專案原始資料。私有記錄為
  `artifacts/operations/storage-single-authority-20261025/own-test-fixture-cleanup-20261029T2114.private.json`。

當沖模擬 owner 曾在另一個當沖修復作業期間於 21:09 正常停止，21:11 啟動；
21:13 仍為 active，模擬程序 `nice=-5`，本次儲存 owner 為 `nice=19`。
但當時 `/api/status` 回 503，內容是 `JSONDecodeError`；另一作業的瀏覽器
驗證亦未通過。這不是交易服務已可用的證明，也沒有證據可將錯誤歸因於
儲存清理。21:15 的 `/api/revision` 回 200、`/api/summary` 仍回相同 503；
前者不證明完整服務就緒。只讀檢查同時發現穩定的
`opening_signal_latency.jsonl` 在 byte 649,941,847 有一條 8,901 bytes、
開頭含 NUL 的無效紀錄，該行 SHA-256 為
`849f2af2baf4ee580d8eee7bf05a2cabe7bf716ae1f6f2781280c459096fcc01`。
現有 dashboard 診斷解碼器本來會將損壞行標示為部分資料，尚未證明這條行
導致 503。另一個當沖分析程序正在執行；不修改或清除日誌、帳本及其
載入版本。

## 第 6、10 輪

第 6 輪仍由原 scheduler 執行。56,320 個來源檔案的編碼階段結束，包含三個
符號連結的最終還原和退休仍待後續檢查。現場 stack 在 D 發布後的
`_verify_materialized` 串行檢查。非阻塞 stack 先觀察到目前檔案在固定
inventory 的第 36,104、36,288 個條目；21:12:42 和 21:15:48 分別為第
38,428 與 41,895 個條目。inventory 有 56,578 個條目，包含目錄與在處理中
的當前條目。最後一個觀察是單一讀回子階段約 74.0%，不是完成率或驗收。
最近 186.1 秒約每秒 18.62 條目，若短期速率不變，此子階段餘量約
13 分鐘；後續樹狀核對、獨立 D 原件解碼、完整來源審核、consumer／同步
和退休另計，不能外推整輪完成時間。原執行緒已載入舊程式，不中斷它來套用
新版本。

第 10 輪獨立 D 原值解碼收據為 `cold_verified=true`、
`decoded_originals_verified=true`，原始 11,804,349,754 bytes；完整來源
審核後，程序引用檢查命中 `pid=2016454:cmdline:/root/stockAgent/artifacts`，
因此 `retired=false`，本輪尚未釋放候選的 11.09 GiB。該 PID 已退出；它與
本次廣域盤點的範圍一致，但收據不足以事後確認完整命令。盤點已停止。已用
`wsl-round10-fixed-stage-retry2-2-20261029T130452-e7189be4` 排入原 scheduler，
固定清冊與 D manifest，在第 6 輪後重新做容量、原件獨立解碼、完整審核及
退休檢查；目前只是等待，並非執行或完成。第一次排隊嘗試因 CLI 不支援
`--poll-seconds` 而退出 2，第二次使用既有預設參數；失敗收據保留。

## 新版候選與驗證

- 受保護 D stage 的 publication 讀回沿用既有四 worker 完整驗證器；其他
  路徑仍用原本單 worker。新增非驗收的逐項進度，所有物件與原值 SHA、
  檔案樹指紋和後續獨立讀回均保留。
- 僅有暫時性程序引用時，在持有 mutation owner 之前最多等待 30 秒；
  有限次重試只接受確認發生在任何 mutation 之前的指定 blocker。原完整
  驗證時間不延長，canonical apply 仍重新檢查來源版本、程序、服務、pin、
  冷庫與同步；其他 blocker、來源變更、超時、隔離或 unlink 錯誤仍拒絕。
- 實際 D stage 固定 96 個小檔，僅做 stat 與每檔 SHA：串行／四 worker／
  四 worker／串行分別 10.427／0.331／0.330／0.933 秒，四次 bytes 和
  SHA 一致。首輪冷讀與後續快取明顯不同；暖取樣下約 2.8 倍僅適用此子
  階段，不能視為整輪加速或硬體上限。
- 相關退休／等待 focused 測試 20 項、packed materialized 平行和損壞檢查
  8 項、publication 讀回及進度測試 3 項通過。這些是測試，不是上述真實
  來源退休驗收。程式變更只會在後續新 worker 啟動時載入。

## lab203 / NAS

21:08 的接收端稽核：`available_admission_bytes=0`，已在 ingress 的 logical
bytes 為 85,904,587,016，待傳 32,931,853,084 bytes／13,518 項；接收端原
owner 狀態 `no_eligible_unpinned_cohort`。同時間的來源 Syncthing 查詢為連線
中、`relay-client`，不構成接收或 NAS 驗收。21:07 接收端容量快照顯示 lab203
available 817,252,188,160 bytes，NAS available 12,400,115,646,464 bytes；
實體空間和 ingress admission 使用不同定義。

21:08 的私人備份狀態為 `private_backup_waiting_capacity`：固定 catalog
48,276 檔，機器收據覆蓋 512 檔，尚待 817,211,979,876 bytes；這是該 catalog
的清冊數量，不應與在途量相加，也不代表已確認的 NAS 遺失量。新聞 498 項、
25 個歷史版本的 114 個缺件與訓練 runtime 驗證仍須按既有固定身分繼續追查。

請 lab203 Codex 沿原 owner 和反向回執：更新三個固定控制包的接收／核驗狀態；
針對 93 個已退休來源批次，以固定清冊逐項比對接收端名稱、原 ACK、snapshot
及 pin，回報是否已有刪除事件；對指定歷史 Restic snapshots 繼續有界 SHA／
長度查找和實際讀回。這是當時的交接觀測；22:00 左右使用者已授權依實際
硬體容量提高 ceiling，詳見本頁末尾的新交接，不將診斷當成 ACK。

## 21:49–21:57 後續觀測與修正

- 21:56:38 可用量：WSL 262,399,909,888 bytes、Windows C
  47,381,389,312 bytes、D 366,416,343,040 bytes。WSL 位於 C，兩者不能
  相加；WSL 內釋放不保證即時回收到 C 的 VHDX。先前第 7、8、9、11 輪
  合計釋放 48,574,013,440 allocated bytes 是歷史收據，本次未再刪除來源。
- 第 6 輪固定 inventory 有 56,320 原始檔、9,881,169,920 allocated
  file bytes 與 3 個連結，這是候選量，尚非可回收結果。以同一固定
  archive manifest 與該 owner 的 `py-spy` 位置計算：21:51:44 其
  publication 後目錄樹掃描約到 45,697／56,578 個條目；21:54:58 到
  52,736／56,578，約 93.2%。這些是該子步驟的已觀察位置，不是完整
  SHA、還原或退休驗收。21:57:23 呼叫堆疊已轉至同一 publisher 的
  `verify_archive_directory`／`source_plan`，後面仍有獨立 D 原件還原、
  完整來源審核及所有退休 gate；不能預測整輪完成時間。
  21:58 堆疊已進入同一 archive 檢查的四 worker 驗證；21:58:51 和
  21:59:11 兩次觀察其主執行緒已完成 4,446 與 5,124／56,320 個原檔，
  約 33.8 檔／秒，若短時速率不變此子步驟約再 25 分鐘。檔案大小和 I/O
  壓力不均，這不是整輪 ETA、驗收或完成收據。
- 第 10 輪固定候選為 11,910,537,216 allocated file bytes。原獨立 D
  冷還原已通過，原退休因當時程序引用而拒絕；新的同一 scheduler 重試
  仍排在第 6 輪後。候選與已釋放分開計算。新啟動的 D publication
  讀回載入前述四 worker 修正；第 6 輪原程序不重啟、不改其內存狀態。
- `stockagent-vast-bulk-return.service` 的歷史 Vast 端點回報 SSH
  `No route to host`；原本同一輪每個遠端候選都重試並以 2 結束。現在僅
  對 SSH 255 且可精確辨識的「連線前」錯誤，保存原始私人錯誤收據、
  保留來源，暫緩其餘相同端點候選並於原 timer 下一輪重試。非 255、
  身分／認證／checksum／遠端執行／可能已執行後中斷仍保留為硬錯誤；
  不能從其他 Vast 節點連線狀態推知此舊端點已恢復。
  58 項 bulk organizer／transport 測試通過；21:54:43 啟動同一既有
  service，21:56:58 的實際狀態為 `deferred`、exit 75，記錄第一個
  無法連線請求與 3 個尚未嘗試的候選。這不是全歷史驗收或整體速度
  A/B 證明；原 D 與來源安全檢查均未移除。
- 來源 Syncthing 21:09 起顯示 `scanning`，21:56 觀察到 peer 已連線
  (`quic-server`)、peer pending 約 32.93 GB／13,442 項。scan 尚未完成，
  不能因同步索引存在或 peer 百分比宣稱 NAS 確認。三個固定控制包
  21:55 在 Penguin 按各自預期 identity、READY 與所有內容 SHA 重新讀回
  成功；lab203 最近逐包回執是 21:31 的三包尚未收到。
- 21:56:58 lab203 最新稽核顯示 in-flight ceiling 已從 103,079,215,104
  改為 146,028,888,064 bytes，並回報可接收 42,940,514,828 bytes、
  `receiving_backpressure=false`；這項變更不是本次 Penguin 工作做的，
  原因、授權、落地配置及實體容量檢查須由 lab203 原 owner 說明。當時
  lab203 WSL `available` 817,243,426,816、readiness 的 ingress 實體
  free 764,414,447,616、稽核 required 777,389,080,576 bytes 是不同
  快照／衡量邊界，不能以任一數值代替另一 gate。暫時看到少量待傳項目
  下降，不能當成整批接收、固定 snapshot 讀回或所有 NAS 備份完成。
  `ingress_deletion_enabled=false`，本次未更動接收端檔案、reserve、
  ceiling、pin 或 retry。21:31 回執記錄的 9 個 Restic snapshot 搜尋中，
  3 個逾時、6 個未查；114 個歷史物件與 498 個新聞缺件仍不可判為遺失。
- **21:57:54 新變動：** 接收稽核的 ceiling 再變為 794,568,949,760
  bytes（740 GiB），required free 變為 128,849,018,880 bytes（120 GiB），
  admission 變為 691,446,346,661 bytes。這與先前不提高 ceiling 的
  交接條件不同，原因及是否符合接收端安全契約尚待 lab203 確認；Penguin
  沒有做這些變更。此時 readiness 的 ingress free 764,396,908,544 bytes
  是另一份非原子快照，不能將 admission 當成可立即寫入的實體容量。
  已保存接收稽核原始 bytes 至
  `artifacts/operations/storage-single-authority-20261025/lab203-capacity-observation-20261029T135809Z-f8628551fea9.private.json`，
  驗證接收稽核 identity 與本地讀回 SHA。21:58:28 pending 為
  32,652,146,620 bytes／12,266 項，較前下降；下降本身仍不是 NAS ACK。
- 21:50 當沖 `/api/status` 已能回 200、ledger integrity ready，但整體
  `health=degraded`，仍有進場／官方開盤價及 guardian 的操作警示。
  active 或 HTTP 200 不證明下一交易日完整就緒；本次未改該服務、帳本
  或 factorized 產生器。

## 22:00 使用者授權、容量與接收進度

- 使用者明確同意提高 lab203 在途 ceiling，要求接近硬體可支撐上限；
  已更新[交接文件](lab203_source_evidence_handoff_2026-10-29.md)，
  由 lab203 原 owner 核對其設定、實際容量、完整在途與工作空間，並以原
  逐批驗收及安全回收流程續跑。這取代先前禁止提高 ceiling 的交接條件；
  不等於來源、pin 或尚未驗收的 ingress 已可刪除。使用者已確認將更新後的
  交接文件轉交 lab203 Codex；尚待對端回覆與獨立證據。
- 22:00:41 Penguin 可用量：WSL 260,910,923,776 bytes、Windows C
  44,338,012,160 bytes、D 366,407,966,720 bytes，WSL 在 C 上不能加總。
  第 6 輪檔案驗證觀測位置為 7,458／56,320，與前述子步驟相同但不代表
  全部還原和退休完成。第 10 輪仍等候同一 scheduler；本次沒有新增
  來源資料刪除或新增已釋放的來源空間。
- 22:01:37 的 lab203 原稽核：ceiling 794,568,949,760 bytes、
  reserve 68,719,476,736 bytes、margin 17,179,869,184 bytes，
  待傳 30,979,882,622 bytes／10,432 項，接收背壓為 false；可接收
  額度是排程數字，必須由對端核對每層真實可用容量，不能直接當成實體
  可寫空間。15:58 固定控制包的來源讀回仍不代替對端及 NAS 讀回。
- 同一既有 `stockagent-backup-stream` owner 正在後續排程週期，22:04
  堆疊已由 transport 全量容量盤點推進至以四個 copy workers 複製下一批。
  容量盤點保留未知及 staging 內容的真實檢查，不能因耗時就跳過。此輪
  22:06 程序記憶體中的 `published_waves` 顯示 1 個來源波次已完成發布，
  目前正對下一波再盤點容量；發布不等於遠端接收或 NAS 讀回，亦不能
  宣稱硬體上限已達成。
- 22:03:04 第 6 輪原檔核對進度為 9,045／56,320。由 21:59:11 的
  5,124／56,320 推算約 17 檔／秒，若假設此短時平均維持不變，該子步驟
  約再 46 分鐘；檔案大小及其他並行 I/O 並不均勻，此數字不包括其後
  還原、全部安全檢查、第 10 輪或 NAS 備份，不是整體完成時間。
- 22:05:02 同一子步驟觀測到 10,685／56,320。最新接收稽核於 22:04:35
  顯示 30,103,010,942 bytes／9,594 項待傳且無接收背壓；因待傳集合、
  檔案大小與網路速率會變，不能把短時下降直接當作 NAS 完成或穩定 ETA。
  22:05 的當沖 API 是 `health=degraded`，guardian 記錄來源事件未就緒；
  當沖來源資料建置正在執行，本次未限制或停止它。
- 22:07:03 第 6 輪為 12,138／56,320；以 21:59:11 起的約 7 分 52 秒
  均速外推，僅這一子步驟約再 50 分鐘，並不包括後續讀回和退休。同期
  WSL available 261,081,190,400 bytes、C available 44,226,035,712 bytes、
  D available 366,332,686,336 bytes，三者不是獨立容量可加總。lab203 的
  22:06:54 稽核記錄待傳 28,985,671,294 bytes／8,527 項、無接收背壓；
  後續新增波次可能改變總量。Vast timer 已再次啟動原 coordinator，當時
  正重查已收封存的 D 證明；上一輪以 75 保留後重試，並非遠端已恢復。
- 22:07:49 第 6 輪觀測到 12,641／56,320；22:07:40 的接收稽核待傳
  28,863,086,206 bytes／8,416 項，接收背壓仍為 false。這兩份分別是
  檢查中的子階段和傳輸索引，不是退休或 NAS 完成收據。
