# FinMind 排程與面板修復（2026-09-28）

## 結論與範圍

修復 FinMind Free、Complement、Sponsor 的等待狀態、假性必要缺口與估時，
沿用既有 worker、共用帳號限速、SQLite 佇列、Parquet／收據及唯讀面板。
不改交易、模型、原始值、來源覆蓋宣告；沒有重啟永豐、交易或 Discord 服務。
原有 FinLab 工作保留。頁面仍為 `/finmind/`。

使用者在本次工作中明確同意：**FinMind 預留自身追新配額後，可先用剩餘配額做校驗，
不必等待其他 provider 所有必要資料完成。**
設定位於 [finmind_acquisition_policy.json](../configs/finmind_acquisition_policy.json)。
其他 provider 的全域必要資料優先規則未變。

## 根因與修正

| 問題 | 證據與修正 |
| --- | --- |
| 有額度卻等待 | 起初帳號取樣約 489／6,000 次，而 Sponsor 的 30 個必要待辦全是休市日。新增可逆 `calendar_wait`，不計成下載成功，也不計成可下載的必要債務；保存日曆判定依據。 |
| 歷史與預排日曆混用 | 有收據的實際交易日優先於週末規則；未知／修訂日曆重新開放任務。非空來源衝突不會被休市規則抹掉。 |
| 面板誤報停止 | 原 worker 休眠 10～60 分鐘，面板卻以 180 秒判過期。新增每 30 秒的獨立 `worker_status.json`，綁定原本完整工作輪次；執行中沿用有效工作輪次狀態，不會因沒有休眠心跳而顯示失聯。不修改資料觀測／收據日期。 |
| 交棒造成重複下載 | Complement 也檢查 Sponsor 的有效休眠心跳；正常等待不被視為失聯而切回逐檔抓取。 |
| 到期任務多等 10 分鐘 | 批次收尾時到期的必要追新，不能只等待「校驗放行」。喚醒與派送共用同一 SQL 到期條件；既有未來重試／發布邊界提前喚醒，等待中每 30 秒檢查新到期工作。 |
| 完工時間一直往後移 | 尚未放行、配額暫停時，不從現在加上工時冒充完工日期；有效工時、未知等待、13 個阻塞與 17 類未排程來源分列。 |
| 共用清冊自相矛盾 | 隨需快照原本同時「完成」與「過期」。修正為隨需參照，不能作為最新資料已取得的證明；保留清冊完整性檢查。 |
| 連假誤報過期 | FinMind 盤中彙總改用官方交易日曆與既有 14:00 發布邊界判斷應到哪個交易日，不再用固定 72 小時。下一個資料日也沿用交易日曆。 |

`calendar_wait` 的排程證據版本為 1；新增 FinMind 帳號範圍准入版本為 2。
來源查詢／數值與原始收據契約未變。日曆暫緩、空回、無權限、待下載保持不同意義。
准入重用既有只讀工作量清點，最多每程序每 30 秒重新分組一次；不是每個 API 請求重掃。
心跳不呼叫 provider，也不掃歷史 Parquet。

## 實際執行驗收

- 重啟前確認 Sponsor、Complement 在途任務均為 0；套用三支 FinMind worker 與唯讀 gateway。
- 17:15 的驗收快照：本機與外網 API 均為 `updating`，新政策已放行。
- 自 17:07 首次套用起新增 **211 個非空分區、9,432,376 筆、124,984,087 bytes**。
  這是新下載的 FinMind 原始校驗資料，不是宣告跨來源比對已全部完成。
- 最後一版排程修正後抽查 3 份新收據，SHA-256 與 Parquet 筆數均一致。
- 30 個假性必要待辦移出必要債務；共 70 個近期休市分區暫緩，其中包含先前已空回的分區。
  未刪除原始資料／空回收據，日曆變更可重新排程。
- 公開頁在 1280px／390px 的實際 Chromium 2D 檢查：130 項管線 → 2 項盤中 → 130 項還原，
  無 JavaScript 錯誤、無全頁水平溢出、無瀏覽器直連 FinMind API。
  截圖需等待捲動穩定；最終人工視檢以 `*_settled.png` 為準。

驗收資料：[after_restart.json](../artifacts/data_quality/finmind_repair_2026-09-28/after_restart.json)、
[receipt_acceptance.json](../artifacts/data_quality/finmind_repair_2026-09-28/receipt_acceptance.json)、
[browser_acceptance.json](../artifacts/data_quality/finmind_repair_2026-09-28/browser_acceptance.json)。
數字均有觀測時點，下載持續進行，不是固定總量或完工證明。

17:26 最後複驗：[final_acceptance.json](../artifacts/data_quality/finmind_repair_2026-09-28/final_acceptance.json)。
本輪 Sponsor 已成功落地 1,008 個非空分區、42,455,708 筆、536,578,911 bytes；
校驗待辦由 5,567 降至 4,559。這些是另一來源的校驗原始資料，並非全部都是跨來源不重複的新觀測。
公開與本機頁面均 `updating`；三支 worker 各自正確呈現等待／執行。
全資料面板的兩項盤中彙總已到 9/24，依日曆下一資料日為 9/29，完整性檢查 0 個矛盾。
當時 ETA 估計最快／中間／保守約 18:13／18:18／18:22（台北），
僅限目前已排程可執行工作，仍受後續重試、追新與額度等待影響。

測試結果：相關 Python 語意／回歸測試 **672 passed**，
FinMind 公開網頁測試 **4 passed、107 deselected**；JavaScript 語法與 `git diff --check` 通過。
不是整個 repository 的完整測試，也不是全歷史逐筆稽核。
17:30 最終本機實頁 1280px／390px 複驗通過，狀態與等待心跳文字正確、無水平溢出，
見 [final_browser_local.json](../artifacts/data_quality/finmind_repair_2026-09-28/final_browser_local.json)
與 `worker_final_*.png`。外網重測期間一度 HTTP 502，本機 API 同時正常、下載持續；
稍後外網重試恢復 HTTP 200 與有效 worker 狀態。不能據此宣稱外網傳輸永不間歇失敗。
`final_browser_acceptance.json` 的離屏 `innerText` 為空，不採作文字驗收證據；
最終改為穩定捲動後視檢與 DOM 文字核對，沒有變更 CSP 或繞過安全限制。

驗證指令（從 repo root）：

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q test/test_finmind* test/test_acquisition_policy.py test/test_data_monitor_dashboard.py
run_fintech_python -m pytest -q test/test_public_dashboards.py -k finmind
node --check services/finmind_dashboard/app.js
git diff --check
```

## 尚未解決的來源限制

13 個 `USStockPrice` 任務仍是來源 `provider_bad_request`：
`AKO/A`、`AKO/B`、`BF/A`、`BF/B`、`BIO/B`、`BRK/A`、`BRK/B`、`CRD/A`、`CRD/B`、
`HEI/A`、`HVT/A`、`RAC/WS`、`WSO/B`。
既有點號版本為空回，不能把換符號當作修復證據。
依 [官方 OpenAPI](https://github.com/FinMind/FinMind.github.io/blob/master/openapi.yaml) 與
[官方 SDK](https://github.com/FinMind/FinMind/blob/master/FinMind/data/finmind_api.py) 的
`datalist` 做了 2 次有界查詢（共用限速／流量帳本、Token 僅在 Authorization header），
實際回應仍為 HTTP 422 的 dataset 驗證錯誤，不能推定資料不存在，也沒有盲目重試 13 個別名。
證據保留於 `us_equity_datalist*.json`，可用性為未知，不當成 0 筆完整歷史。

17 類原有未排程／特殊高容量來源未因本次修復擅自啟用。空回資料仍需來源覆蓋稽核，
ETA 僅涵蓋已排程且可盤點的工作，不是全部 FinMind 歷史的完工承諾。

## 設定回復

如要恢復原全域優先策略，將設定中的 `secondary_validation` 改為 `global_required_first`。
設定有 mtime 快取失效檢查，不必另開下載器。未知設定一律不放行。
不刪佇列、不刪收據；切換政策只影響額外校驗的准入，固定增量及共用限速仍保留。
