# 網頁延遲優化與驗收（2026-09-09）

## 執行結果

已更新公開網頁的當沖／共用隔日沖通知、快取與繪圖路徑；只操作公開網頁服務的
重啟，沒有為 UI 重啟當沖引擎、Discord、TAIFEX BidAsk 或 Top-200 擷取。
現有交易、模型、日期篩選報酬與分鐘曲線資料語意不變。

## 找到的等待

1. 瀏覽器以 250 ms 固定輪詢 revision，通知本身先承擔 0–250 ms 排隊。
2. 訊號表 await 完整分鐘曲線；首次 localhost 曲線讀取實測 10.78–11.85 秒，
   與該訊號計算無關，卻延後其顯示。
3. history 的 55 秒後端 TTL 與 45 秒前端 TTL 沒有包含資料 revision，
   可能收到引擎更新通知卻仍讀到舊分鐘。明細也有 2 秒固定快取窗口。
4. revision 更新時 status 可先回傳有時間標記的舊快取；需要在新版快取完成時
   再通知，且前端必須分清楚「觀測到新版」與「已套用新版」。
5. 重複刷新同一分鐘陣列仍解碼、排序、解析時間並重建 SVG。

修正為有界 SSE + 原子 receipt 監看、revision 快取鍵、訊號與曲線獨立請求，
以及不可變資料解碼／SVG 重用。HTTP gzip、ETag、公開欄位遮罩、只讀及有界
快取契約不變；SSE 不壓縮／不代理緩衝，並保留斷線輪詢對帳。

## 實測邊界

| 項目 | 本次結果 | 邊界 |
|---|---:|---|
| 原子 receipt → SSE 客戶端（50 次） | 中位數 1.167 ms，P95 1.441 ms | 隔離暫存資料、本機 HTTP，不含網路及畫面 |
| 熱 status / signals / revision API | 中位數約 1.0–1.4 ms | localhost，含回應讀取 |
| 熱 TAIFEX status / 首頁 overview | 中位數約 1.87 / 0.95 ms | localhost，非來源更新頻率 |
| 正式 HTTPS 狀態請求 | 10.8 ms | Chrome 實際請求、同一主機網路環境 |
| 正式 HTTPS 首次訊號請求 | 59 ms；導覽後 285 ms 收完 | 100 筆訊號；不等曲線 |
| 2/25–9/8 全區間曲線 | 257,476 個現有點，約 1.21 秒完成 | 熱後端；包含切換日期、解碼與繪圖驗收 |
| 同曲線重新計算 / 重用 | 約 240 ms / 0–0.1 ms | SVG 完全一致，沒有刪點 |

這些數字不是全天或外網 SLA，也不是「理論最低」保證。第一次大區間冷查詢仍
可能耗時數秒；一次冷瀏覽量到約 25.8 秒，不能拿它和熱快取 1.21 秒宣稱是
等條件加速比。已移除它對訊號請求的順序阻塞，但沒有假裝消除磁碟、JSON、
CPU、網路與瀏覽器繪圖的成本。257,476 是現有資料保留量，並非來源完整性證明。

## 驗收

- 83 項 Python 測試與 9 項 JS 測試：原子替換通知、晚出現目錄、連線上限、
  關閉、快取完成通知、revision 立即失效、同源、隱藏分頁釋放、無 SSE 降級，
  以及既有 gzip / ETag / 讀寫隔離／安全回歸。
- 正式 HTTPS Chrome：最新當沖驗收收到 10 個 SSE 訊息、100 筆訊號；
  全區間曲線重用與完整重建的 SVG 相同，revision 一致，0 JavaScript / HTTP
  錯誤、0 整頁水平溢出。
- 七頁視覺驗收均無整頁水平溢出。第一輪當沖遇到其他同時部署作業於 01:26:05
  重啟網頁服務，留下一次 502，沒有視為通過；後續當沖專項驗收重新通過。
- 本輪只手動重啟公開網頁服務。其他並行作業也曾重啟網頁／Discord，不能將
  所有程序 PID 變動或冷熱快取差異歸因於本輪。

結果：`artifacts/operations/web_latency_20260909/benchmark.json`、`browser.json`。
七頁瀏覽器報告保存在 `/tmp/stockagent-web-latency-20260909/`（暫存）。

重測：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_dashboard_latency.py --repeats 50 \
  --output artifacts/operations/web_latency_20260909/benchmark.json
node scripts/benchmark_dashboard_browser.mjs 9224 \
  https://penguin72487.ddnsgeek.com/tw-day-trade/ \
  artifacts/operations/web_latency_20260909/browser.json
```

瀏覽器腳本需要獨立測試 Chrome 的 CDP 埠；只改測試分頁的日期，不改任何帳本。
