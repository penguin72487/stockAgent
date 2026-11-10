# Vast 驗證分工退役（2026-11-09）

使用者指定取消 Vast 的備份／驗證分工，保留訓練服務、訓練資料及
產物。此處處理原本將 D 封包送到 Vast 預先計算的驗證用途；訓練
索引、按需資料傳輸與訓練結果回傳仍由原服務管理。

Penguin 的 `/etc/stockagent/pack-verification.json` 已設為 `enabled: false`
及 `remote_spool: false`，所以新工作不再建立 Vast 驗證 worker。
`pack-compute-spool.json` 的兩項啟用旗標也已關閉。既有本機預設的
封包重建驗證仍可使用；停用的是原先同時包含 Vast 與本機 lane 的
選用平行方案。已移除 `stockagent-pack-compute-spool.service`、timer
及其專屬 drop-in。安裝程式在政策未明確重新啟用前會拒絕重裝。

另外移除一次性歷史封存的 `stockagent-vast-bulk-return` 服務／timer
及 `stockagent-legacy-return@` 服務／timer；停用前五個已登錄 timer，
原工作均為 inactive。這些舊管線中的未完成封存、來源、日誌與收據
均保留，因此不能宣稱舊批次已回傳或完成。現行訓練成果的
`stockagent-remote-cold-artifact-ingress.timer` 及訓練資料傳輸
`stockagent-packed-transport.timer` 持續啟用。
例如 D 上兩份 2026-10-04 失敗的 `markets`／`ablations` 部分封存合計
50,421,299,136 bytes，收據沒有完整成員清冊，無法確認每一筆訓練
產物都有現存來源，因此保留；這不是已完成備份。

原 Vast `service_data/pack_compute_cache` 是預驗證用、以 SHA 命名的
可棄置快取。移除前清冊記錄 4,137 組封閉 payload／proof，無其他
hardlink，且當次沒有程序引用該快取。程式以原 per-key flock、每批
程序引用檢查及固定檔案身分移除成對檔案；不重新解壓、不要求新的
D/NAS payload readback。中斷後留下的 proof 可由同一流程辨識，未知
檔案與鎖檔保留。移除結果為 4,137 組、290,212,052,992 allocated
bytes；這只證明遠端驗證快取回收，沒有刪除 D source、NAS 或訓練資料，
也不是來源或 NAS 的完整性驗收。

執行前 Vast 檔案系統可用 11,077,570,560 bytes；完成後首次觀測
298,520,358,912 bytes。容量會受其他工作並行寫入而變化。驗證用
4,317 個零位元組 key lock 與原壓力記錄仍保留，避免與未知的舊執行者
產生 lock inode 分裂；它們不含訓練資料。遠端原始收據保存在
`/var/lib/stockagent-vast-verification-retirement/`；Penguin 的公開狀態
只投影已移除標記及有限計數，私有收據不傳給瀏覽器。

原日誌、歷史 code 與測試保留作稽核；這不表示服務仍安裝或排程。
清理流程的單元測試涵蓋使用中鎖、程序引用、hardlink、symlink 與
成對刪除中斷。實際使用之訓練傳輸 timer 與當沖服務維持原狀。
觀測時 Vast 的 `stockagent-daytrade-unit-mgda-reverse-20261108-v1`
訓練排程仍為 RUNNING；這只是當下程序狀態，不代表訓練已完成。

面板的「已移除」文字與狀態投影已完成測試；為避免開盤期間重啟
網站閘道，已排程 13:30:05 後在現有磁碟清點自然結束時重載。
重載前面板仍可能把舊預驗證顯示為「已暫停」，那是歷史投影，
並不代表服務仍安裝或快取仍存在。重載提交不等於公開頁面驗收。
