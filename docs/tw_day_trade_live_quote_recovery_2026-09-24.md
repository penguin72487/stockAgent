# 2026-09-24 當沖即時報價修復紀錄

## 責任與證據邊界

- 五個 09:00 模型訊號均已寫入模擬引擎；當時零新進場的主因不是模型沒有輸出，而是執行端把 Shioaji Snapshot 缺少的 `simtrade` 視為未知，不能證明已離開試撮。
- 09:35–09:43 的另一版程式曾每 0.5 秒輪詢 Snapshot，建立 269 筆當日模擬進場成交。這些是 paper ledger 紀錄，不是券商／交易所成交；其來源不能事後改稱為串流證據。該版本兩次觸發 90 秒 systemd watchdog。一次中斷交易提交由成對的 append-only 訂單及成交列恢復，最後觀察 `ledger_integrity.divergence_count=0`。
- 永豐 [Snapshot 文件](https://sinotrade.github.io/tutor/market_data/snapshot/) 不提供 `simtrade`；[使用限制](https://sinotrade.github.io/tutor/limit/) 指定盤中即時資料使用訂閱，而不是反覆輪詢 Snapshot，且最多 200 筆訂閱。[QuoteSTKv1](https://sinotrade.github.io/tutor/market_data/streaming/stocks/) 同一筆推送包含最佳買賣一檔、成交量、`simtrade`、暫停旗標與交易所事件時間。

## 已實作的防再犯機制

1. 模擬進場使用同一個 process-local Shioaji 報價連線的 QuoteSTKv1 推送；沒有新增券商下單連線。Snapshot 即使有交易時段時間戳，也不能通過即時執行的非試撮門檻。
2. 每檔只佔一筆訂閱；同時最多 200 筆，25 筆一批建立／替換，超額標的輪替。未訂閱與未收到新事件分別揭露，不能把缺資料標成已成交。取消訂閱後短暫保留真實推送事件，僅在本機接收與交易所事件都不超過 10 秒時可用。
3. 進場同時檢查本機收到時間與交易所事件時間皆晚於訊號；後收的舊快取書不算新可成交報價。停牌、試撮、過期或缺少事件時間仍 fail closed。
4. 輪替訂閱在背景進行，不因此每秒重寫多模式紙上帳本；狀態仍按分鐘估值，盤中每分鐘印出 requested/subscribed/available/capacity_limited/quote_fetch_ms。
5. 新的進場成交列保留 `quote_source`、本機報價時間、交易所事件時間、`simtrade` 與訂閱狀態，供將來稽核；舊列不回填為不存在的證據。批次進場的資金保留額只加總一次並隨成交更新，避免每標的重掃持倉。

## 驗證及未解條件

- `py_compile` 與 271 個當沖、報價、訊號相關 Python 測試通過；包含訂閱額度輪替、取消後短期真實推送保留、試撮／舊事件拒絕、後續新事件才建立模擬部位、13:30 非試撮成交量保留。
- 10:11:37 載入串流輪替版；超過 90 秒 watchdog 門檻後仍無重啟，公網 `/tw-day-trade/api/status` 回 200，帳本 divergence 為 0。10:12、10:13、10:14 的每分鐘 `quote_fetch_ms` 分別約 13.745、6.787、5.920 ms。
- 當時 292 檔未平標的超過 200 訂閱上限；10:14 僅 109 檔在該分鐘檢查時有 10 秒內可用事件。輪替可以避免永久餓死，但不能保證 292 檔同時有即時報價，也不能保證每檔在一秒內更新或當日全部成交／沖銷。真正突破同時容量需提高供應商授權或新增可證明試撮狀態的合規即時資料源；不得把 Snapshot 或 1 分鐘 K 線偽裝成當時的最佳買賣一檔。
- 今日已記錄的 Snapshot 紙上成交屬獨立待稽核事項；不能由目前網站 HTTP 200 或本次串流修復推論為真正交易所成交。現有交易帳本不覆寫，也不把舊 Snapshot 事件改標為 QuoteSTKv1。

## 盤前 `final_arm` 防覆蓋

- 08:54 的 Discord 日誌顯示五個模式先完成 `final_arm=ready`，接著基礎盤前準備再次完成；最終磁碟 receipt 的五個模式都沒有 `final_arm`。根因是後完成的基礎準備以新 market row 取代舊 row。
- 現在 `_write_preopen_readiness` 保留同一 Bot 行程的 `final_arm`，而 `_preopen_prepare_key` 在基礎準備與 `final_arm` 都完成時不再重跑準備。補上「先 final arm、後 base retry」的回歸測試；相關盤前測試 3 個通過。
- Discord 服務於 09:51 重新啟動，已載入 09:30 寫入的防覆蓋程式碼；這不代表今天 08:54 的缺失被倒填，也不能代替下一交易日的實際盤前驗收。
