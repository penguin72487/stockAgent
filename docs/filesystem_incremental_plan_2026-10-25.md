# 增量儲存方案與本輪回收

本輪先以唯讀資料規劃，再修改既有入口及執行精確回收。資料的價值在原始觀察、
來源、單位、時間和版本；版本增加不應再複製未變的歷史。可重建的工作副本與
權威來源有不同生命週期，不能以檔名、重複外觀或年齡決定刪除。

## 已核對的現況

2026-10-25 台北時間，正式掛載命名空間的 metadata-only 觀測：
**33／33 個 `data_*` 根通過**，20 個 shared v2、12 個 host-private v2，以及
1 個已有完整驗證收據的新聞增量資料集。工作區為 ext4，D 主冷庫經原 guarded
掛載存取。IDE shell 直接執行底層 Python 會因不同掛載視角拒絕私人庫；正式
`scripts/run_data_release.sh layout` 沒有這個錯誤，不修改掛載保護。

[當前完整清冊](../artifacts/operations/filesystem-incremental-plan-20261025/layout-owner.json)。
清冊只讀設定、head 與完成收據，沒有重新讀取所有來源 payload。
[逐根格式與遷移狀態表](../artifacts/operations/filesystem-incremental-plan-20261025/source-layout-inventory.md)
分開列出版本入口與原生遷移驗收，避免將已有日分區或已有 head 誤報為完整遷移。

## 決定採用的架構

| 層 | 寫入及保存方式 | 驗收 |
| --- | --- | --- |
| 來源工作區 | 沿 provider 原 owner；已分期的資料保留原分期，封閉內容不可變 | 來源收據、數值／單位／時鐘及增量覆蓋 |
| 版本 | 每版完整邏輯清單，共用相同檔案及有界 SHA-256 區塊 | 新增／修訂／刪除及舊版獨立還原 |
| 冷庫 | D 保存 immutable objects；NAS 依原管線獨立接收驗收 | 全檔 SHA、固定 manifest、paired ACK 分別核對 |
| 訓練與服務 | 固定版本物化，必要投影／cache 由原 builder 產生 | reader ABI、READY、來源指紋及 lease／pin |
| 工作副本回收 | 完成清冊、全原檔冷還原與目前引用檢查後精確 unlink | 最後 inode 的 `st_blocks`，與新建暫存分開 |

不更換 ext4／NTFS、不新增資料 writer／備份管線、不以不斷串接差異檔取代
完整邏輯版本。相同內容不推進 head；更正及刪除形成新清單，舊版引用仍保留。
先前 FinLab 原生分片的完整工作流較慢，保留正式 writer 及原負面結果，不重跑
相同試驗或全庫轉寫。原生 producer 的逐期寫入與版本層增量分別驗收。

## 本輪實作順序

1. 將版本清冊接到既有 `stockagent-data` 入口，透過原 release wrapper 使用
   正式 namespace；不觸發 publish、recipe refresh、provider API 或物化。
2. 核對具名 `benchmarks/running_services_20261013/taifex_proof_memo_20261015`
   的原 producer、完整八輪比較、七種區間輸出、原始一日獨立對照與來源 SHA。
   舊回收辨識器未識別此既有格式，先補精確 ABI 的辨識及負例測試；不改寫舊
   收據，不把其他未完成嘗試回填成功。全副本及失敗證據都進入固定冷版本。
3. 由原 `retire_local_offline_artifacts` owner 建立 frozen inventory，核對
   source／encoded／decoded 全 SHA、目前 consumer／FD／mmap／pins／leases、
   transport、隔離後身分及精確 unlink。任一 gate 未通過即保留副本；不清鎖、
   不降低保留空間、不清 D 物件或 NAS 歷史。
4. 另一程序從固定 D 版本重建全部原檔；逐項核對 SHA、大小、集合和 portable
   metadata。成功後只移除自己建立的恢復暫存，留下小型證據。
5. 本輪原有空間回收、作業新建暫存移除、當前磁碟可用量和 Windows VHDX
   allocation 分開記錄。沒有實測便不宣稱全系統只剩硬體限制。

先前 01:24 的 9.35 GB 回收已完成，**不再計入本輪新增回收**。版本 head
存在不代表 provider 全歷史下載完成，也不代表來源全量和 NAS 全歷史已驗收。

## 驗收結果

實作、測試與回收完成後在此補上實際收據；規劃本身不預支完成或可回收量。
