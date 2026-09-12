# 2026-09-10 收盤價補登清算

用戶明確要求：「今天的未平艙都用尾盤撮合的收盤價清算」。
本次處理 **paper ledger**，不是券商委託或真實帳戶交易。

## 最新結果：最後成交價補清算

使用者追加授權「用最後一個價格清算」後，於 20:36:05 起補登剩餘兩筆，
20:36:43 完成服務恢復。**五個當沖模式的未平倉均為零**；兩次共清算
81 個庫存批次。此為模擬帳本調整，沒有送出券商委託。

| 標的 | 多單股數 | 清算價格 | 價格日期 | 此次已實現損益增量 TWD |
|---|---:|---:|---|---:|
| 00787B | 5,000 | 31.39 | 2026-09-09 | -5,147.81 |
| 6680 | 1,000 | 53.30 | 2026-09-09 | -2,941.06 |

兩個價格均來自已下載的 TPEx 官方日報。9/10 成交量零、無收盤價，
因此取最後有成交的 9/9 收盤。已下載且通過檔案摘要核對的一分鐘
K 線也一致：最後正成交量 K 棒標籤分別為 9/9 12:46、13:22；
K 棒標籤不等於逐筆成交時間。未採用當日最後買價或零量 K 棒。

清算契約為 `user_authorized_last_traded_price_paper_settlement_v1`，
有效帳務端點仍為 9/10 13:30，`price_date` 明確保留 9/9；
`broker_fill=false`、`exchange_match_at=null`。不聲稱 9/10 有收盤成交。
這是一次性、指定標的的離線清算選項，不改一般執行器的成交條件。

100m 模式累計清算數保留為 75（先前 73 加本次 2），權益為
**108,434,189.67 元**；其餘四模式權益未變。此次損益合計 -8,088.87 元，
從原未實現部位轉入已實現；總權益變動不等於此損益增量，因先前已
含未實現估值。既有持倉成本及原始進場價格均保留。

### 本次驗證與回復

- 385 項相關測試通過（清算測試 18 項）；Ruff、`git diff --check` 通過。
- 逐筆獨立核對方向、股數、成本基礎、剩餘進場費、退出費稅與損益。
- 三層 API（引擎、內部 gateway、公網）HTTP 200，五模式未平倉 0，
  `ledger_integrity.ready=true`、divergence 0；Discord connected、revision lag 0。
- 公網 Playwright 驗證兩個價格、原始日期、累計 75 筆及剩餘 0 筆；
  page errors 0，1366px 無水平溢出。截圖已檢視；測試環境仍缺中文字型，
  中文內容以 DOM 斷言核對。
- 停止寫入前暫停 guardian 計時器，完成後恢復；沒有自動恢復搶鎖。
  引擎與公網服務 active、NRestarts 0。
- 完整原帳本：`artifacts/live/official-close-settlement-b6_qz_l1/ledger`。
  前次 81 筆未清算的原始備份 `official-close-settlement-ic2kly7u/ledger`
  也保留；訊號、訂單、成交、事件與延遲帳本原始前綴 SHA-256 全通過。
- 新計畫：`artifacts/operations/day_trade_close_settlement/2026-09-10-last-price-plan.json`。
- 最新收據：`artifacts/live/tw_day_trade_simulation/paper_close_settlement_receipt.json`；
  兩次獨立收據保留於同目錄 `settlement_receipts/`，前次官方收盤收據未覆寫。
- 截圖：`artifacts/operations/day_trade_close_settlement/2026-09-10-last-price-public.png`。
- 只更新 100m 的 13:30 曲線端點，其他日期與日內點未改。原本分鐘曲線
  缺口、歷史錯誤及尚未啟用的 strict-intraday 設定仍屬另案，不宣稱已修復。

## 第一批結果（20:12，以下保留當時紀錄）

- 原有 81 筆未平倉庫存批次、74 檔。
- **79 筆／72 檔已按 2026-09-10 官方收盤價清算。**
- 100m 帳戶仍保留 00787B 多單 5,000 股、6680 多單 1,000 股：
  TPEx 原始日報顯示當日成交量零、收盤 `----`。不能以昨天價格、
  買賣價或零元冒充今天的收盤價；已向用戶詢問替代清算基準。
- 其餘四個模式未平倉皆為零。
- 當天三筆短倉因原先未沖銷而計入的轉換成本，共 **1,869.25 元**，
  已新增對應沖回紀錄。原始扣款沒有刪除；舊留倉的既有成本仍保留。
- 原始訊號、進場、既有成交 JSONL 位元組前綴均驗證未變；
  五個當天 13:30 曲線端點同步更新，其他日期與日內點未改。

| 帳戶 | 本次清算批次 | 剩餘批次 | 清算後權益 TWD |
|---|---:|---:|---:|
| tw_day_trade_100m | 73 | 2 | 108,431,995.69 |
| tw_day_trade_attention_layernorm | 1 | 0 | 14,627,665.46 |
| tw_day_trade_multi_basis | 2 | 0 | 13,371,970.15 |
| tw_day_trade_multi_basis_22 | 1 | 0 | 15,796,707.11 |
| tw_day_trade_multi_basis_projection_l1_gelu | 2 | 0 | 16,269,776.68 |

權益含各模式既有累積損益，不是今天的單日報酬。

## 時間、價格與成交的界線

清算契約 `user_authorized_official_close_paper_settlement_v1`。
有效計價點是當日 13:30；補登作業時間保存為 20:12:21 起。
完成帳本交換後於 20:13:48 重新啟動服務。
每筆補登都有 `counterfactual_settlement`、官方來源摘要、原始資料
SHA-256，且 `exchange_match_at=null`、`broker_fill=false`。

使用已下載的 TWSE / TPEx 每日收盤原始 JSON；72 檔均與目前股票日資料
收盤欄位一致。沒有為此次清算呼叫券商歷史價格 API。
部分標的當天尾盤 K 棒量為零，其 K 棒價格可能是攜帶舊成交價；因此
採交易所公布的收盤價，而不是零量 K 棒價格，也不聲稱全部數量曾在
尾盤實際撮合。此流動性豁免僅屬用戶本次指定的模擬清算。

## 驗證

- 356 項相關測試通過，包含 7 項新增清算測試。
- 清算逐筆獨立核對：方向 × 股數 ×（收盤價 − 成本基礎）−
  剩餘進場費用 − 退出費稅；各模式已實現損益增量與逐筆合計一致。
- 引擎、內部 gateway、公網 `/tw-day-trade/api/status` 均 HTTP 200；
  `ledger_integrity.ready=true`，未平倉合計 2。
- Discord connected、五模式一致、revision lag 0。
- Playwright 實際載入公網，五個模式「收盤價補登清算」警示與剩餘數量
  可見，JavaScript page errors 0，1366px 視窗沒有水平溢出。
- 維護期間自動恢復服務曾嘗試搶鎖，被帳本排他鎖安全拒絕；完成後
  再次正式重啟，當沖與公網服務均 active、NRestarts 0。這些維護紀錄
  沒有被抹除。

原本今天每分鐘曲線仍有缺口（兩模式 268 點、三模式 146 點），本次只
修正清算端點，不宣稱已通過完整 270 點歷史 promotion 驗收。
先前 staged 的 `day_trade_strict_intraday` 旗標仍未啟用；本次不是
策略、checkpoint 或全段歷史的替換。

## 可追溯資料與回復

- 操作入口：`scripts/settle_tw_day_trade_official_close.py`。
- 原始計畫：`artifacts/operations/day_trade_close_settlement/2026-09-10-plan.json`。
- 原始官方報告副本：同目錄的 `2026-09-10-twse_daily_ohlcv.json`、
  `2026-09-10-tpex_daily_ohlcv.json`；完整 SHA-256 已核對。
- 逐筆清算收據：`artifacts/live/tw_day_trade_simulation/official_close_settlement_receipt.json`。
- 完整原帳本：`artifacts/live/official-close-settlement-ic2kly7u/ledger`。
  沒有刪除任何原始帳本；回復仍須先停寫入並核對後續是否已有新變更。
- 瀏覽器截圖：`artifacts/operations/day_trade_close_settlement/2026-09-10-public.png`。
  測試主機缺中文字型，截圖字形不完整；文字內容另外經 DOM 斷言驗證。
