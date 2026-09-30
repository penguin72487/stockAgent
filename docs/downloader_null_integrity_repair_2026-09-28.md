# 下載器空值／檔案完整性修復（2026-09-28）

## 1. 執行結果

- 已修復 FinMind 的 5 個零位元組 Parquet，共恢復 20,884 列；重新下載後的大小、列數與 SHA-256 均與原收據相同，沒有修改價格或補 0。
- FinMind Sponsor 155,285 個、Complement 36,796 個已完成非空分區，逐份驗證目前收據、檔案大小、Parquet 列數與 SHA-256，合計 **192,081 個，錯誤 0**。這不包含尚未下載／空回應的佇列，也不等於全體 FinMind 歷史已完整。
- FinLab **1,107 份既有資料**通過大小、Parquet 列數及 SHA-256 驗證。另有 2 個沒有合格資料收據的 key，其保留的原始 Arrow 重新計數仍完全無值，不能宣布已補齊。
- 前次 OpenBB 的 190 個空欄位來自舊 `compact/**/archive.parquet`；查驗較新的正式 L1 成功段後，**186 個已有觀測**。更正的是盤點範圍，不是補造 186 個特徵。
- 修正後清冊為 **12,023 筆來源欄位候選**，包含原清冊未列入的 2 個 FinLab 上游空 key；不是模型維度。FinMind 兩個修復資料集再掃 18,217 個當前分區，錯誤 0，原先受影響的 13 個欄位不再誤標來源不完整。
- FinMind Sponsor／Complement／Free 服務於台北時間 18:23:34 重新載入修正；後續有新的完整性巡檢收據。FinLab 為既有排程啟動的程序，下一次使用更新後下載器。不修改當沖、Discord、永豐即時行情或模型部署。
- 回歸測試 **712 passed**；最後結果以本目錄對應的 `test_results.txt` 為準。

證據目錄：`artifacts/data_quality/downloader_integrity_20260928/`。

| 來源 | 分區 | 原問題 | 恢復列數 |
|---|---|---|---:|
| FinMind TaiwanStockPriceLimit | 2023-08-23，全市場 | 收據 complete，但檔案 0 bytes | 2,444 |
| FinMind UKStockPrice | 0NW1.L / history | 同上 | 4,519 |
| FinMind UKStockPrice | 0NW2.L / history | 同上 | 4,248 |
| FinMind UKStockPrice | 0NW4.L / history | 同上 | 5,138 |
| FinMind UKStockPrice | 0NW7.L / history | 同上 | 4,535 |

原始收據與佇列列保留在各 lane 的 `local_integrity_audit`；被替換的空檔保留在 `corrupt_parquet_evidence/`，沒有刪除正常來源、checkpoint 或訓練產物。

## 2. 根因與實作

資料下載成功至少有四個不同條件：回應有觀測、解析不漏欄、資料檔已持久化、收據指向並驗證同一份檔案。檔案存在或 queue=complete 不能代替四個條件。

### FinMind

- 既有 `_store` 的資料檔使用 atomic rename，但未開 durable；收據卻先有持久化承諾。已改成資料 flush/fsync、rename 與目錄同步完成後，才提交收據與 queue。這證明原路徑有崩潰一致性缺口；不能僅據此斷言五個空檔的硬體故障原因。
- 新增共用 `parquet_integrity.py`：驗證安全路徑、大小、列數、SHA-256；快取以 inode、ctime、mtime、size 和收據內容為鍵，檔案改變即失效。
- Sponsor／Complement 每個正常 batch 按持久 cursor 巡檢 512 個完成分區；避開開盤保護時段。證實損壞才保留原始證據並把精確分區放回原佇列，不建立第二套下載器或額度池。修復任務在同優先級內優先，仍受原有權限、額度與時段限制。
- Free lane 也改用內容驗證，並補齊資料檔持久化。尚未有資料的合法 `observed_empty` 不會被當作壞 Parquet 重抓。
- `Table.from_pylist` 原本只靠第一列推斷欄名，可能漏掉後面才出現的欄位。一般儲存改成完整欄名聯集；既有具嚴格格式的分批前檢仍保留。收據增加逐欄非空數與全空欄名；真實 0 不會被視為缺值。
- 完全沒有任何欄位觀測的回應不發布為 complete；部分欄位為 NULL 仍保留原始觀測，不擅自猜測每個 endpoint 的必填財務欄位。

### FinLab

- `has_local_download` 不再只驗檔案存在；改查正確 dataset、有效觀測列、大小、Parquet 列數及 SHA-256。
- 正常表新增全空來源欄名與非空值總數，且明示寬表欄是股票／維度，不等於獨立模型特徵。拒絕轉字串後重複的欄名。
- 暫存 Parquet 驗收後才發布，資料／JSON／原始 Arrow 物件補上持久化順序。
- 若同一內容雜湊路徑已損壞，新取得的相同原始內容通過完整雜湊後，沿用既有保留證據的替換流程；不再永遠因舊壞檔存在而下載失敗。

### 盤點工具

- 舊 OpenBB compact 的 NULL 不再宣告整個 provider 缺資料，改要求查較新 L1。
- 日期解析錯誤旗標、來源調整係數等證據欄，不當作缺少的數值訊號。
- 保留前次原清冊；另產出 `feature_candidates_corrected.csv`，附較新來源與修復分區的再驗結果。不默默改舊模型的特徵 ABI。

## 3. 仍不能冒充已修好的項目

1. FinLab `dividend_otc:權息`、`management_change_events:變更交易開始日`：重新驗證保留的上游 Arrow，其非空值數都為 0。維持既有 provider-empty／退避重試，阻擋當作有效資料；沒有把另一個欄位或另一個來源冒稱同一個觀測。上游需提供真值，或另立可驗證的替代來源契約。
   本次嘗試安排即時重抓，但先被額度保護阻擋：18:33 的帳戶樣本顯示當日 5,000 MB 已使用 4,967.345 MB、剩 32.655 MB，低於既有 50 MB 保留額度。本次沒有對這兩個 key 發出新下載，所以來源全空的直接證據仍是當日 08:05 的保留原始回應，而非聲稱剛剛 API 再回空；後續沿用額度重置與退避排程。
2. OpenBB 仍有 3 個 `cftc_*_code_quotes` 識別欄及外匯 `vwap` 沒有觀測；識別欄不是連續模型訊號，不用補 0。
3. 其餘原本的 23 筆空欄位中，11 筆是診斷／來源證據；8 筆在較新股票研究表已有觀測，但舊衍生表仍空，不能跨股票／期貨 universe 直接複製。另有外匯量 2 項、期貨 spread volume、Binance annualized basis 共 4 項仍需原來源適用性證據，不因表格空值便製造數據。
4. 外匯匯率本身不構成市場成交量；Binance 官方 PERPETUAL basis 範例的 `annualizedBasisRate` 就是空字串。本次保留 NULL，沒有自行計算假年化或把匯率筆數當成交量。
5. **檔案完整、非空，不代表數值語意及歷史 PIT 合格。** FinLab 現修值、發布日、權限與實際市場可用時間仍按原研究契約處理。沒有重新訓練、變更訓練／實盤規則、發布新冷庫或同步授權原始資料。
6. 額外品質風險：既有研究表的 `twpub_tdcc_large_holder_ratio` profile 最大值為 2.0，聚合路徑用 `_tier >= 13`；後續應核對合計列是否混入。此次未改該訓練 ABI／重寫歷史研究表，也不因「有值」宣告其可正式訓練。

參考：[FinMind 技術資料定義](https://finmind.github.io/tutor/TaiwanMarket/Technical/)、[CFTC 報表類型](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm)、[OpenBB COT 參數](https://docs.openbb.co/odp/python/reference/cftc/cot)、[Frankfurter 匯率 API](https://frankfurter.dev/)、[Binance basis 官方範例](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#basis)。

## 4. 可重現檢查

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

# 只讀全量檔案完整性；不呼叫 API。
run_fintech_python scripts/audit_finmind_integrity.py \
  --root data_finmind/sponsor \
  --output artifacts/data_quality/downloader_integrity_20260928/sponsor_reaudit.json
run_fintech_python scripts/audit_finmind_integrity.py \
  --root data_finmind/complement \
  --output artifacts/data_quality/downloader_integrity_20260928/complement_reaudit.json

# 對照前次空欄位清冊，另產出修正後清冊；不改來源值。
run_fintech_python scripts/recheck_training_source_nulls.py \
  --catalog artifacts/data_quality/tw_day_trade_feature_catalog_20260928/feature_candidates.csv \
  --output-dir artifacts/data_quality/downloader_integrity_20260928 \
  --research-features artifacts/datasets/tw_day_trade_features_20260928_v1/research_features.parquet \
  --refresh-finmind-repaired-datasets

run_fintech_python -m pytest -q test/test_finmind*.py test/test_finlab*.py \
  test/test_downloader_integrity_repair.py test/test_tw_day_trade_feature_catalog.py
```

只有 `--repair` 會重排已證實損壞的任務，而且要求該 lane 的 writer lock；正常 worker 已內建巡檢，無須另啟下載服務。CLI 修復流程若需使用，先短暫停止該 lane，完成後立即恢復原服務。
