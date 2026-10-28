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

[正式 CLI 最終清冊](../artifacts/operations/filesystem-incremental-plan-20261025/layout-final.json)
與[首次 owner 清冊](../artifacts/operations/filesystem-incremental-plan-20261025/layout-owner.json)。
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
新增版本的實體保存量是「新獨有內容＋新清單」，但完整檔案清點與改變檔案的
雜湊仍有成本。整表改寫的壓縮 Parquet 可能改變很多區塊；冷庫去重不能代替
來源 writer 的逐期寫入，也不能保證總計算量只與新增列數成正比。
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
6. 本筆退役交易完成後，整理編碼暫存清點中相鄰的重複 `lstat`：同一列的
   regular-file 檢查與完整身分使用同一份觀測，共用其清點實作。保留每次完整
   清點、原排序、目錄 symlink 檢查及發布／隔離前後的獨立 fence，不快取到
   下一輪。先核對完整 fingerprint 一致、檔案變動與非 regular 成員仍拒絕，
   再量測完整清點的 syscall 數與時間；不以這個局部數字宣稱全流程加速。

先前 01:24 的 9.35 GB 回收已完成，**不再計入本輪新增回收**。版本 head
存在不代表 provider 全歷史下載完成，也不代表來源全量和 NAS 全歷史已驗收。

## 驗收結果

2026-10-25 15:12 台北時間的最終清冊為 **33／33 accepted**，錯誤為零；
此次清冊沒有讀取來源 payload、寫入來源、呼叫網路或刪除資料。新增的
`stockagent-data layout` 入口已實際使用正式 namespace，後續可直接查看各根的
原生格式、版本入口和仍未驗收的原生遷移，無須自行呼叫底層 Python。

| 項目 | 實際結果 |
| --- | --- |
| 原有 WSL 工程副本精確回收 | **6,041,350,144 allocated bytes（6.04 GB）** |
| 本次新建 D 編碼暫存移除 | **499,105,792 allocated bytes（0.50 GB）**，另計 |
| 保存的固定原件 | 148 檔、6,040,300,040 logical bytes；冷封存約 223.40 MB |
| 刪除後獨立恢復 | 148／148 原 SHA、大小、精確集合與 portable metadata 全相符 |
| 先前原始比較證據 | 5 個來源、64 份完整輸出、舊程式與獨立一日資料對照均核對 |
| D 冷物件／NAS 歷史刪除 | 0／0 |
| 物化 GC dry run | 4 個 pin、1 個有效 lease 全保留，無可回收項目 |
| 當下 WSL 可用 | 43,065,393,152 bytes（43.07 GB）；持續寫入會使其改變 |

回收量取自最後 inode 的 `st_blocks`，不是可用空間的前後差，也沒有把本次新建
暫存或 01:24 已完成的回收重計。未執行 Windows VHDX 離線縮檔。
[原 owner 退役進度收據](../artifacts/operations/filesystem-incremental-plan-20261025/retirement/progress.json)
與[刪除後獨立全檔讀回](../artifacts/operations/filesystem-incremental-plan-20261025/post-unlink-readback.json)
保存固定 snapshot／manifest、清冊身分及原證據綁定。原未完成的相鄰 trial 沒有
改填成功；失敗與中斷證據保留。

清點程式已共用一份逐檔實作，以同一次新鮮 `lstat` 同時判定 regular file 及
完整身分。相鄰重複呼叫從兩次減為一次，仍逐次完整走訪、排序及核對全部身分；
隔離前後不共用快取，演算法仍為 O(N)。實際唯讀 ABBA 比較的完整指紋均相同；
這兩個小型 WSL／D 目錄的逐檔 `lstat` 數減半，D 案例中位時間
**18.25 → 13.12 ms**。這是局部 metadata 清點，不能推估全庫／整筆回收加速。
大型回收先按原程式完成，後續清點優化另外驗收，沒有混算其績效。
[完整測速與範圍](../artifacts/operations/filesystem-incremental-plan-20261025/stage-metadata-comparison.json)。

原完成格式、CLI 與共用回收的 **416 項回歸通過**。15:19 後續三個封存模組
**155 項完整回歸全部通過**，包含 stage-prune 的 66 項。新增測試覆蓋同大小／mtime
替換、mode、mtime、增刪、symlink、FIFO，以及清點後出現實際 FD 引用。
私有測試隔離自己的程序清冊，保留原 FD／maps／ancestor parser；正式回收仍使用
全系統引用檢查。初次新增測試把正確的「隔離後保留」誤期待為隔離前拒絕，已修正
測試後，單獨 66 項與最終三模組 155 項均通過；原失敗與被取代的慢測試收據
保留，不改寫其終態。
[首輪資格](../artifacts/operations/filesystem-incremental-plan-20261025/qualification.json)
及[最終程式、完整測試與恢復資格](../artifacts/operations/filesystem-incremental-plan-20261025/final-readback.json)
分別綁定當時的程式 SHA。

15:20 收尾時 WSL 可用空間為 **42,096,259,072 bytes（42.10 GB）**，與上方
15:12 觀察分開保存；其他工作持續寫入，兩者差額不作回收績效。
[唯讀檔案系統觀察](../artifacts/operations/filesystem-incremental-plan-20261025/filesystem-health.json)
顯示目前 ext4 superblock 為 clean，本次開機沒有觀測到 ext4／I/O 錯誤訊息；
觀察範圍是目前掛載及 kernel 記錄，沒有執行離線 fsck、媒體全掃或停止服務。

來源原生逐期寫入尚未全數驗收，逐根狀態見上方清冊；本輪沒有宣稱所有 writer
都已避免整表重寫。後續優先沿既有 owner 處理仍為 current-symbol／whole-table 的
來源，逐一驗證追加、歷史修訂及完整讀取成本後再採用。新聞既有增量 reader、
原分期資料與已驗證的 FinLab writer 保留，避免因統一外觀反而增加轉寫與讀取。
NAS 全歷史驗收仍是另一份遠端證據；本機讀到某一配對 transport 100% 不作替代。
