# 當沖切日與延遲驗收：2026-09-09

執行進度：修正已部署；451 項 Python、18 項 Node 測試通過。最後服務驗收時間為 2026-09-10 凌晨，以下開盤數據固定指 09/09，不是尚未開盤的 09/10。

## 結論與測量邊界

頁面未換日不是單純下載慢：第一次自動填入的日期被當成永久手動篩選，後續 status 請求一直帶舊日期；後端也依帳本是否已有當日紀錄來決定可選日期，且 latest 快取可能跨日沿用。

現在由官方驗證交易日行事曆決定 08:30 顯示時鐘，與資料 readiness、訊號產生、成交分開。預設跟隨最新交易日，手動歷史篩選保持不動；按「回到最新交易日」可恢復跟隨。現有分頁需重新整理一次載入 app.js v70。

實際帳本、隔離測試、同機 HTTPS 瀏覽器與 loopback API 是不同測量邊界；不能把它們相加或當成使用者外網延遲，也未宣稱已達理論最低。

## 09/09 開盤原始證據

| 項目 | 實測 |
|---|---:|
| 必要行情覆蓋到達本機 | 09:00:01.570 |
| 第一模型訊號 ready | 09:00:01.859350 |
| 最後模型訊號 ready | 09:00:02.976598 |
| 四模型 input → 模擬帳本落盤中位數 | 1,623.633 ms |
| 模型推論中位數 | 28.678 ms |
| 跨模型批次等待中位數 | 597.513 ms |
| Discord rich-artifact 輸出失敗 | 四模型均為 ComputeError |

只有四筆 registered 樣本，不能當作長期 P95。原 executor receipt 漏帶 price_receipt_timing，但四份原始 summary 有證據；覆蓋到 ready 分別為 MB22 289.350、v12 683.077、多基底 1,060.348、一億 1,406.598 ms。共用行情快取可早於後續模型的請求時間，不能把這種負 request-to-coverage 差當成負 RTT。

另外三筆 consumer_detected_at 早於 artifact_published_at，原因是用讀檔前的迴圈時間當偵測時間。舊紀錄沒有重寫；新版本改成讀完後取時。

## 實作

- `tw_day_trade_dashboard.py`：官方行事曆驗證的 08:30 session clock，日期切換納入 revision／日期快取；盤前只延續原帳戶權益及其原始 as-of，不延續昨日訊號、成交數或偽造新分鐘點。沒有有效行事曆不猜測平日開市。
- `serve_public_dashboards.py`：latest 快取按顯示交易日隔離，08:30 不會 stale-fallback 到昨日；時鐘邊界也使 tiny revision 快取失效。
- `app.js`：自動跟隨與手動歷史分離；服務端相對時間排定切日、SSE 通知、分頁恢復可見後重新核對；切日清除舊行並讓未完成請求失效。訊號、部位、事件不再等待分鐘曲線完成。
- `signal_engine.py`：輸出檔案依完整有界資料推斷型別，不讓前 100 筆皆為 null 的價格欄造成後續浮點值輸出失敗。保留嚴格型別，不丟資料。
- `run_tw_day_trade_simulation.py`：預設跨模型人為等待由 2 秒改為 0；同時已準備好的模型仍可共用行情批次，慢模型不阻擋已完成模型。保留原有因果最佳 bid/ask 與來源備援，不新增登入或券商委託。
- `tw_day_trade_simulation.py`：將原始行情 receipt/transport 證據保留到 executor latency，後續 Discord 輸出故障不再讓這些計時證據消失。
- 分鐘算式抽成共同純計算函式，避免每個分鐘點建立完整帳戶 DTO；算法及結果不變。
- 歷史明細只索引自己需要的帳本，不為日期清單讀基準曲線。既有有界尾端讀取改成逆向讀取精確的最近 N 筆，再還原來源順序；不再解碼整份 3,036,918,336 bytes 訊號檔才丟掉舊前綴。

## 修正前後測速

| 測量 | 前 | 後 | 解讀 |
|---|---:|---:|---|
| 四模型實際 execution weights 的型別建表 | 四個 ComputeError | 四個通過 | 每模型 2,744–2,746 筆；隔離 writer 約 12–28 ms，並非完整 Discord 推論與傳送 |
| 同一出版時間序列的第一模型批次等待 | 1,120 ms | 0 ms | 虛擬時鐘重播，只量人為 barrier；不含行情或成交 |
| 100,000 次相同權益算式中位數 | 133.36 ms | 26.75 ms | 5 次同程序比較；不是整頁加速五倍 |
| 半年日期篩選的瀏覽器訊號請求 | 15,000.7 ms 逾時 | 7,585.8 ms 完成 | 冷查詢、與其他歷史視圖同時讀；非嚴格受控性能實驗 |
| 半年曲線篩選到完成 | 19,830.8 ms | 13,205.8 ms | 259,374 點均保留，SVG 重建與快取重用一致 |

最後同機 HTTPS 瀏覽器：當日 status 6.3 ms、signals 150.3 ms、positions 140.1 ms、events 164.4 ms、當日分鐘歷史 91.2 ms；初始訊號與歷史分別 100 行／1,890 點。這些是 API body 完成時間，另有初次 render 約 41.3 ms。無 HTTP／JS 錯誤、無頁面水平溢出，SSE 收到 21 次通知。

最後 loopback、無並行瀏覽器的測速：status 熱回應中位數 1.888 ms、signals 1.405 ms、revision 1.185 ms；隔離 receipt → SSE 中位數 1.370 ms、P95 1.666 ms（25 次）。初始基線對應約 1.340／1.254／0.920 ms、SSE 0.965 ms：微小熱端延遲沒有全面改善，新增交易日時鐘驗證與主機負載也有成本，不能宣稱全面變快。

完整分鐘歷史 API 第一次觀測 7,123.551 → 6,473.487 ms；熱回應 60.291 → 57.468 ms。快取狀態、背景負載與執行順序不同，這是操作觀測，不是足以證明因果的嚴格 A/B。

## 驗收與仍存在的限制

- 真實帳本搭配注入時鐘：09/10 08:29:59 選 09/09；08:30:00 選 09/10，四模型 signal_id=null、entry_filled_shares=0、waiting_open。純讀取，不改系統時間或帳本。
- 測試覆蓋休市日、週末、行事曆缺失、無檔案更新的 revision 切換、跨日 cache 隔離、手動歷史保留、過期回應失效、曲線永不完成時其他視圖仍啟動、慢模型隔離、跨 byte span 尾端讀取一致性及稀疏型別。
- Discord Gateway 已連線、22 個 global commands 同步、startup-warm 4/4、最新 revision_lag=0；三項相關服務 active，這次重啟後未見 watchdog timeout／fatal／traceback。這不等於 09/10 所有盤前來源 gate 已經驗收。
- 訊號、委託、成交三份帳本 SHA-256 在更新前後一致。沒有改模型、手續費、成交規則、既有分鐘價格或交易結果。
- 半年冷曲線／部位仍需約 13 秒，是剩餘的批量讀取與 CPU 成本，不是理論極限。當日路徑不再等待其完成，但同一程序上的大量歷史工作仍可能增加 CPU 競爭。
- 原有訊號明細查詢上限是最近 100,000 筆，不是全期間約 148 萬筆；此次沒有調低上限或刪除來源，新增明確警告及 API 欄位，避免把局部方向合計當成全期間。分鐘曲線不受此上限影響。
- 新版 09:00 的實際行情與 Discord 端到端時延，需要下一交易日實測；此次未發送假訊號測試 DM，也未把虛擬重播當成實盤。

## 證據與重跑

操作證據目錄：`artifacts/operations/rollover_latency_20260909/`，包括 before、after、acceptance_idle、browser_acceptance、today_evidence、microbench_and_ledger_before、batch_wait_replay、service_acceptance JSON。原始 summary 路徑及 SHA-256 記於 microbench evidence。

修正紀錄僅維護本 Markdown 文件，不另提供 Notebook；測速數值、驗收結果及原始 JSON 證據保留於上述操作證據目錄。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest test/test_day_trade_rollover_latency.py test/test_tw_day_trade_simulation.py test/test_public_dashboards.py test/test_live_signal_helpers.py test/test_cross_surface_performance.py test/test_discord_bot_formatting.py test/test_dashboard_updates.py test/test_tw_overnight_simulation.py -q
node --test test/test_day_trade_rollover.mjs test/test_tw_day_trade_components.mjs test/test_dashboard_core.mjs
run_fintech_python scripts/benchmark_dashboard_latency.py --repeats 25 --output artifacts/operations/rollover_latency_20260909/recheck.json
```
