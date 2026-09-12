# 當沖優先當日沖銷：修正與啟用界線

後續狀態：用戶另行授權今日收盤價清算，已清算 79 筆、剩 2 筆缺官方
收盤價；服務已重啟，但 strict 旗標仍未啟用。詳見
[清算紀錄](DAY_TRADE_CLOSE_SETTLEMENT_2026-09-10.md)。以下保留原修正階段紀錄。

2026-09-10。**程式已修正並測試；尚未啟用或重啟現行服務。**
新契約為 `flatten_same_day_adverse_limit_exception_only_v1`，由 live market
設定 `day_trade_strict_intraday: true` 選入。現行五個市場尚未設定此旗標。
模型、checkpoint、原始訊號、既有成交帳本與歷史曲線沒有更換或回補。

## 用戶要求與行為

- 原則是當日退出，不是把所有未成交自動變成融資融券。
- 開盤部分成交後保留原始目標剩餘量；只用新行情與未消耗的容量繼續。
  不重推模型、不改方向、不拿空單收入擴張資金，不重開已開始退出的部位。
- 停損用不利的成交價／可成交側觸發；一旦觸發就持續退出。
  13:20 被動限價不能解除停損。13:24 市價重試、13:25 極端合法限價
  集合競價維持原排程。
- 收盤第一個空回應不再結案。逐筆證據必須非試撮、時間在委託之後，
  且為正式 13:30／延後 13:33 的撮合；觀測至 13:35 仍缺證據就報錯。
- 日內重複收到同一成交量不會重複使用；同分鐘晚到的新增量可更新，
  累積量倒退不能降低基線而製造流動性。多筆庫存共用一個股票／撮合容量。
- 只有不利方向鎖漲跌停、退出方向**明確為零**的對手量、合法價格界線
  與退出委託證據，才可留下例外模擬持倉。缺欄位不是零。
  例外仍是 critical，保留停損有無觸發、觀測時間及未確認券商轉換的註記。
- 所有前日殘餘先獨立減倉，不等新訊號，也不因新模型同方向就沿用舊庫存。
  未清理前不能建立新曝險；下一時段仍退出受阻則繼續顯示 critical。

這是保守的 paper execution，不是券商下單／成交保證，也沒有取消整張、
資金或成交容量約束。缺乏成交證據的部位不會被任意清零。

## 實作

- `stockagent/live/tw_day_trade_simulation.py`：凍結目標餘單、停損鎖定、
  收盤延後驗收、例外判斷、前日強制減倉、狀態與共享容量。
- `stockagent/live/quote_provider.py`：沿用同一行情登入，訂閱 Tick / BidAsk，
  保存各自的試撮旗標、交易所時間與原始接收時間。回調不寫磁碟；讀快取
  不更新接收時間；明確零深度不當成缺值；退訂已不使用的標的。
  新執行路徑不以反覆 snapshots 輪詢充當即時行情。
- `scripts/run_tw_day_trade_simulation.py`：餘單／持倉行情檢查與退出持續處理；
  界面仍沿用分鐘曲線，不因快速執行檢查增加盤中曲線點。
- dashboard DTO / `app.js` / unattended guardian：例外與真正未沖銷都明確顯示，
  不因「已轉融資融券」就通過健康檢查。

## 驗證與未完成的部署

截至此次檢查，370 項聚焦回歸測試通過（含 24 項新契約測試），
涵蓋原始當沖、margin 相容歷史、隔日沖、行情、曲線、公開 gateway 與冷啟動。
Ruff、JavaScript 語法檢查及 `git diff --check` 通過。
這不是下一個開盤的實際行情驗收；盤後無法證明未來逐筆資料一定完整。

以 canonical switch `plan` 執行唯讀驗收被拒：五個模式仍有未平倉部位。
本次再次讀取原帳本：100m 75 筆、attention 1 筆、multi-basis 2 筆、
multi-basis-22 1 筆、舊 projection/GELU 2 筆，合計 **81 筆庫存批次**。
其中同一股票可能有不同取得批次，不能把批次數當成股票檔數。
原服務 MainPID 511433 / NRestarts 0；本次未停機、未提交任何券商委託。

下一步必須先明確核准如何處理帶舊庫存的程式部署。一般策略／帳本
promotion 必須先平倉，不能跳過 gate。若核准的是保留原帳本的退出修正
維護部署，須另外核對重啟前後逐筆數量、成本、已實現損益及成交檔完整性，
再在五個 market 設定中啟用旗標。現有 session 的成交契約仍須保留；新
交易日才採新進場契約，舊殘餘只走減倉路徑。不得藉此把今天改成已成交。

## 官方依據與界線

- [TWSE 交易制度](https://www.twse.com.tw/zh/products/system/trading.html?hl=zh-TW)：
  集合競價與逐筆交易是不同階段，不能將試撮視為完成成交。
- [TWSE 投資問答](https://investoredu.twse.com.tw/pages/TWSE_InvestmentQA.aspx?ID=14&Page=2)：
  收盤可能延至 13:33。13:35 是本系統等待回報的工程截止時間，並非交易所新增交易時段。
- [Shioaji 即時股票行情](https://sinotrade.github.io/tutor/market_data/streaming/stocks/)
  與 [Snapshot](https://sinotrade.github.io/tutor/market_data/snapshot/)：
  即時行情應使用推送訂閱，snapshot 查詢不可反覆用作即時資料源。
- [TWSE 當日沖銷](https://www.twse.com.tw/zh/products/system/day-trading.html)：
  不能把模擬的融資融券假設視為券商確認。「只准不利鎖停例外」是用戶要求的
  更嚴格策略規則，不宣稱它是交易所所有帳戶的統一法定條件。
