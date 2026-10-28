# 日、分鐘、逐筆與特徵資料清冊

實作日期：2026-10-25。入口為
[全資料監控清冊](https://penguin72487.ddnsgeek.com/data-monitor/#tick-inventory)，
以及每個 provider 的同名區塊。舊的 `#tick-inventory` 連結與 Tick API 保留。

資料範圍、逐列統計與觀測時間見
[完整 CSV](../artifacts/data_quality/minute_day_inventory_2026-10-25/datasets.csv)、
[統計摘要](../artifacts/data_quality/minute_day_inventory_2026-10-25/inventory_summary.json)。
CSV 是單次已發布快照；持續更新的數字以網站列出的統計時間為準。

## 顯示內容與口徑

總覽及來源頁共用同一元件，可交集篩選市場、provider、頻率、種類與文字，並分頁。
台股與臺灣期權排在前面，加密貨幣在後面。每列保留來源、資料集、頻率、
原始／衍生／重疊視圖、列數、實際資料日、起訖及證據。
原生頻率未確認的財報、營收、事件表等保留「其他原生頻率」，沒有升頻成每日或分鐘。

- **筆數保留原來的粒度。** FinLab 寬表列數是日期／期間列，非空資料格另列；
  長表是資料列。FinMind 逐股日分區數與實際相異日期數分開。
  原始分鐘、Tick 合成分鐘、每日聚合、熱資料尾端、期權 ATM 與訓練投影各自標記，
  不把來源或視圖加總成唯一市場資料量。
- **天數由實際日期聯集取得。** 不用日期跨度、合約日數或 symbol-day 數代替。
  窗口跨多日的收據只有起訖時，另核對原檔日期欄位，未核實時保留未知及已知下界。
  當前查詢仍有待清點項目；這次交付不宣稱所有來源已完成全檔日期驗收。
- **日期與發布時間分開。** FinLab 長表使用真正的事件日期，數字流水號不再顯示成日期；
  預告事件可含未來日期。期權交易日可能包含前一晚夜盤。
  起訖不代表發布、模型可用時間、可訓練性或中間無缺口。
- **未知不當作零。** 未啟用、不支援、尚無收據及讀取異常分別保留。
  已登錄的 16 個產品之日／分鐘粒度均可檢視；未啟用的交易所沒有因此開始下載。
  Yahoo 加密貨幣舊根目錄保留未分流標記，不把混合頻率整包稱為日行情。

## 責任與效率

`stockagent/live/bar_inventory.py` 擴充現有行情清冊，沿用原始收據、FinLab／FinMind
唯讀索引、正式 Parquet 清冊及 `InventorySnapshot.selected`。永豐歷史摘要由
原下載器新增有效 KBar 的交易日聯集，使用既有 writer lock 與 `--summary-only`，不登入。
同一 `stockagent-data-refresh-status-snapshot` owner 發布結果。

`/data-monitor/api/series` 僅篩選已發布快照；每頁最多 100 列。
HTTP 不呼叫 provider、不查來源 SQL、不遍歷原始資料樹、不回傳帳密或行情內容。
舊 `/data-monitor/api/ticks` 保持 Tick 範圍；公開／會員存取限制維持原樣。

冷清點只投影日期欄位，能用單日 row-group metadata 證明時直接使用。
字串日期先去重再解析，實測樣本結果一致；時間見
[日期解析比較](../artifacts/data_quality/minute_day_inventory_2026-10-25/date_projection_benchmark.json)。
快取使用檔案身份與壓縮日期 bitmap；manifest 同時核對原 pointer 與所有已驗證 member 的身份。
未變動檔案不再投影。FinLab 收據快取只保留需要的統計欄位，避免重複保存完整 schema／請求描述。

正常快照的額外冷清點預算為 2 秒，這個預算只計新的日期工作，避免已完成 metadata 檢查
耗盡預算而讓後面的來源永遠沒進度。provider 階段輪替，實存資料群依未完成與最近服務次序
分配；持續重寫的 hot tail 不再阻塞其他歷史群。這是增量清點，不新增下載流程或 API 請求。

每個檔案讀取前後都檢查身份；檔案粒度的工作可能略超過剩餘預算。
必要時可透過同一 owner 的私有 cache 鎖手動推進一次，正常快照負責對外發布：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/snapshot_data_refresh_services.py --bar-audit-only --bar-audit-seconds 30
```

不變的篩選結果有自己的 revision，其他 provider 更新不再清空目前分頁或重畫相同資料。
大量清冊不塞入首頁 summary；需要時才載入，隱藏分頁停止輪詢。

## 驗收

相關單測覆蓋日期聯集、空值、損毀、檔案變動、manifest member fence、收據路徑、
流水號與事件日期、寬表單位、登錄狀態、預算與飢餓、分頁 revision 及 HTTP 唯讀邊界。
實際桌機／手機與九個 provider 的驗證見
[瀏覽器驗收](../artifacts/data_quality/minute_day_inventory_2026-10-25/browser_acceptance.json)。
整體測試、存取權限與主要 owner 的讀回見
[驗收摘要](../artifacts/data_quality/minute_day_inventory_2026-10-25/acceptance.json)。

這些證據驗收的是清冊與呈現；收據索引不是全檔 SHA 或完整歷史證明。
沒有修改下載範圍、原始值、交易帳本、訓練輸入或 factorized 資料產生器。
