# TW 救援補件、歷史缺件與新聞版本核對：2026-10-28

## TW 原始參照與 runtime

已核對原反向通道的兩份請求，包含原始檔案 SHA、canonical identity、
producer／receiver／repository 配對、原 spec、bootstrap、checkpoint 與
context。兩份原始 prepared reference 已從 D 私有 CAS 完整讀回，原 SHA
及長度均相符。固定綁定及接收端操作見
[原始 runtime 補件合約](lab203_original_runtime_supplement_2026-10-28.md)。

不改動原 spec；由原 bootstrap 的唯一目標、來源路徑、角色、長度及 SHA
證明 step 0／1 的原參照。新版原離線驗證器支援
`legacy_original_bootstrap_reference_binding_v1`，拒絕有歧義或不同版本的替代。

原 runtime provenance 已固定連回 86,813 個原成員的 proof、原 source ZIP、
source receipt 與 wheel。原 observer 的 SHA 為
`71a7afe0658c6479b988b8706db93a4d406b17a41754d9c3642782cdc86118c0`。
本次完整讀回的是固定控制檔、source ZIP／wheel 與兩份參照；沒有再次
傳輸或在 Penguin 解開 8.51 GB capsule。原 runtime 成員完整 SHA 還原驗證
來自 lab203 原回報，新診斷會在接收端重新完整核對原 prefix。

六項差異保留如下，這是 lab203 的原回報，不冒充新的接收端觀測：

| 套件 | 原 lock | 原回報 |
| --- | --- | --- |
| charset-normalizer | 3.4.9 | 3.5.2 |
| click | 8.5.0 | 8.4.2 |
| cuda-pathfinder | 1.2.2 | 1.8.3 |
| stockagent | 0.1.0 | 缺 distribution metadata |
| typing-extensions | 4.15.0 | 4.16.0 |
| urllib3 | 2.7.0 | 2.8.0 |

五組有爭議的套件在原 capsule 中含有新舊 metadata；typing-extensions
另有原 setuptools vendor 目錄的 metadata，來源清冊也保留。原 lock 的
全部 duplicate inventory 不修改。檔案還原正確而查找結果不同，可能與
metadata 發現順序有關；原封存未記錄當時的目錄列舉順序，不能宣稱已證明
所有實際原因，也不能強制挑選版本製造通過。

原 prefix 與 source ZIP 缺少 StockAgent distribution metadata，但固定原
wheel 中存在真實的 Name／Version。版本化診斷使用這個已驗證的原 wheel
作為 metadata 查找位置，同時沿用原 frozen observer、原 Python 與原 lock，
保留全部實際 distribution、順序及比較結果。實測不符就輸出完整差異及
exit 75；它不安裝套件、不載入 checkpoint 或 GPU，也不宣稱可續訓。

合約 identity：
`dcb3f3dba24b25e9546369e3ce2045a7bad74bda01b7a4dfba6a91bc2326a754`。
原始 enrollment、checkpoint、capsule、prepared reference、兩個期貨 run
及過去失敗證據均保留。TW 嚴格 runtime 及實際 optimizer 續訓尚待接收端驗收。

## 25 個歷史版本／114 個缺失物件

本次重新核對原 disposition 清冊及 25 份原 manifest 的固定 SHA。版本建立
日期為 2026-08-19 至 2026-09-13；這是 release 日期，不是金融觀察值的
歷史起訖。114 個物件共有 **1,053,144,416 bytes，約 0.98 GiB** 編碼大小。

已檢查 canonical D 物件、登錄的 D 備份／ingress、WSL 與舊 hot backup
目錄，共 30,885 個檔案名稱；114 個 canonical 物件仍全數不存在，也未找到
完整 SHA 相符的副本。再用原 `repair_packed_source_objects` 規劃器逐版本
核對原 pack membership 與登錄來源；9 份遇到來源工作而暫緩的初次收據
保留，待來源釋放後，持原協調鎖完成重新核對。

結果為 **0 個可重建，114 個仍無法由本次檢查的 Penguin 來源還原**。
全部的第一個失敗原因都是 `source_bytes_differ_from_retained_release`。
例如原年度公司行動 JSON、永豐日資料及 feature 檔已更新，現存 bytes
與當時固定版本不同。重新下載的新版本不能替代舊版本的原始 bytes。

完整清冊：

- [114 個物件、受影響版本及第一個失敗來源](../artifacts/operations/storage-single-authority-20261025/tw-public-114-missing-object-recheck-20261028.csv)
- [25 個版本及 manifest SHA](../artifacts/operations/storage-single-authority-20261025/tw-public-25-historical-release-recheck-20261028.csv)

這次未連入所有機器或直接窮盡搜尋 NAS；結論限於列明的 Penguin 位置與
原來源修復器。原 disposition、heads、manifests、現存物件及所有失敗收據
不修改，也沒有刪除資料。這不是當前資料集完整性的驗收。

## 新聞固定版本

`data_news` 與 `data_news_fulltext` 都仍在。固定研究版本為
`versions/20261016T155152Z-890f02f7`，manifest SHA：
`8aa535f89c3605fb530f2e23b1d4f8f22f273f7da2c015e323a7c03101046a54`。

本次用既有 manifest 路徑解析器與完整檔案 SHA，持 builder 共用讀鎖核對
全部 **7,673 個 part、5,771,634,846 bytes（約 5.38 GiB）**，實際 Parquet
schema 與筆數均相符，耗時 17.61 秒。沒有重新計算逐篇文件文字 hash 或
重跑關聯稽核；前次完整驗證保留，這次不冒充新增的深層品質驗收。

| 固定表 | 實際筆數 |
| --- | ---: |
| articles | 6,278,162 |
| documents | 5,796,288 |
| article_stocks | 6,105,349 |
| metadata_observations | 4,703,099 |

來源／類型列數見
[固定版本來源分類](../artifacts/operations/storage-single-authority-20261025/news-fixed-version-source-counts-20261028.csv)。
此版本的 part 跨 generation 共用，因此只複製最新版本目錄會漏資料。
`data_news_fulltext` 當前 status 另外記錄 6,352,201 個已知文章及 3,917,924 個
有儲存正文的文章；這是目前來源狀態，不與上述固定版本混算。

現有整體政策仍是 `publish:false`、`remote_training_export:false` 與
`training_injection_enabled:false`。本次只交付版本識別、分類筆數與驗證
證據，不交付新聞原文或 live SQLite，也不放寬既有來源使用限制。因此
固定版本在 Penguin 存在，不代表 lab203 已有可用的新聞原文備份。

上述是 20:25 補件當時的範圍。使用者其後在同日明確要求整份私有同步，
已由[固定新聞私有備份](news_private_backup_2026-10-28.md)覆蓋原本的
「僅交付索引」同步限制，沿原配對管線交付資料表與完整來源快照閉包。
原始版本與來源權利標示保留；是否實際送達及備份仍以新 paired ACK 驗收。

## 補件封閉與驗證

新增的私有 evidence-only plan 為
`965a8cd00fb77f3bc7ad4f0a28a89afd06bb2540a737a4643cbd3745facff2f0`，
共 **6 個 JSON、954,419 bytes**，固定連回原 v7 plan。只新增本次證據，
不重傳原 runtime／checkpoint／prepared payload，也不另造完整救援備份。
2026-10-28 **20:25 台北時間**已由原加密 Restic owner 封閉發布；完整傳輸
集合為 9 檔、962,253 bytes，envelope identity：
`6b4d8362b475779d85dc1c6bf888812aebb056f66a56da57eb9bd813c7e9209c`。
Penguin 另外以原獨立傳輸驗證器重讀其中 7 個 payload／plan 檔，
959,423 bytes 的完整 SHA 均相符；NAS 新還原 ACK 與接收端採用仍待回報。

工具已封閉發布於 `tools/storage-relay-20261028-v1/`，36 個來源檔，完整
傳輸集合 39 檔、1,241,371 bytes；envelope identity 為
`d607175c40b9f79a563cb30cb5cd06e0cf764a9acfc3318265b19c303063b2be`。
發布沒有修改來源 ledger，沒有啟動接收端程式；當時 transport 通知回報
已有 folder scan，交由原通道續送。

20:34 的原通道仍在既有 folder scan。已登錄最長 15 分鐘的有限通知重試，
只對這個固定 dispatch 呼叫原 `scan_transport`；不重傳 payload、不重寫
來源 ledger，也不搶占既有 scan。若仍忙碌，保留原 pending intent 並以
exit 75 結束，後續沿原 backup owner 續送；本機整個 folder 顯示的 100%
不能代替這份新補件的接收端 ACK。

runtime 診斷、原離線 verifier、增量私有備份、evidence-only 限制與 handoff
依賴封閉共 **141 項測試通過**。檔案發布、NAS 讀回、TW strict runtime 與
實際續訓保持分別驗收。原 WSL 回收工作與 day-trade 優先權不受本次補件變更。
