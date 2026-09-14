# 台股隔日沖公開模擬契約

這個產品把「模型決策」、「委託參與」與「實際撮合證據」分開。公開頁面是
`/tw-overnight/`；它只讀取 `artifacts/live/tw_overnight_simulation`，不會呼叫
券商下單 API，也不共用當沖的持倉或成交帳本。

## 時鐘與成交

1. 13:00：切換至當日隔日沖交易日，完成 panel、checkpoint、模型與 CUDA
   cache 驗證後等待。這個階段不發布正式訊號，也不送模擬單。
2. 13:20：開始使用最新同日行情更新模型輸入。現階段暫時沿用對應的當沖
   checkpoint；產物會明確標成「未針對隔夜風險訓練」。
3. 計算完成至 13:30 前：多方以當日漲停價掛 `LMT_ROD` 買進，空方以
   當日跌停價掛 `LMT_ROD` 賣空。張數以決策時計價和該批進場前已對帳
   總權益計算；
   權益非正時保留訊號但不增加曝險。
4. 13:20–13:30：`simtrade=true` 只代表試撮，永遠不記為成交。若計算未在
   13:30 前完成，當日不補送也不回填成交。
5. 13:30：只在同一交易日、非試撮且帶交易所時間戳的實際收盤價出現後，
   才以該撮合價建立紙上部位。延緩收市的證券可等到 13:33。
6. 次一有效交易日 08:30：多方以當日跌停價掛賣出，空方以當日漲停價掛
   回補，參與開盤集合競價。08:30–09:00 的試撮仍不是成交。
7. 次一有效交易日 09:00：只在同日、非試撮且帶交易所時間戳的實際
   `open` 出現後，才以開盤撮合價沖銷。缺少證據時持倉保持未平並揭露原因。

13:00 先準備完成 panel、checkpoint、模型與 CUDA cache；這個預熱不要求
當沖資格或 09:00 MIS 開盤資料。13:20 的正式計算才取得當下行情並寫入獨立
產物。帳本另記錄訊號計算、發布、消費者發現、共用報價、metadata 與原子落盤
各階段延遲，避免只看總秒數卻不知道瓶頸。

收盤證據晚於延緩收市期限，或次日開盤觀測窗內始終沒有可證明的正式價格時，
委託會明確標為未成交／漏失，持倉不會用較晚的一般盤中 snapshot 假裝已在集合
競價成交。過期的 `ROD` 委託也不會跨交易日沿用；若部位仍存在，下一個有同日
行情證據的開盤會建立新的紙上沖銷委託。

集合競價的一檔快照無法證明排隊順位，因此模擬帳本的「全量成交」只是一項
明示的紙上流動性假設，不宣稱券商或交易所成交。手續費與交易稅採一般現股
隔夜規則，不套用當沖減半證交稅。

## 服務與資料流

```text
Discord 13:00 切換／預熱，13:20 正式計算（沿用模型）
  -> artifacts/live_signals/tw_overnight_*/latest_signal.json
  -> stockagent-tw-overnight-simulation.service
  -> 共用當沖 Shioaji 報價 broker（不建立第二個登入 session）
  -> 隔日沖 append-only signals/orders/fills/marks
  -> 唯讀公開閘道 /tw-overnight/api/*
  -> /tw-overnight/

官方收盤資料完成發布
  -> stockagent-tw-overnight-history.timer
  -> 只補尚未計算交易日的歷史 13:25 訊號
  -> 以官方 CLOSE／下一交易日 OPEN 重播完整帳戶
  -> 原子發布 overnight_history.json 與壓縮 Parquet 明細
  -> 同一組 /tw-overnight/api/* 合併即時與歷史資料
```

歷史曲線固定從 `2026-02-25` 開始。每個完成交易日有兩個事件點：09:00
官方開盤反事實沖銷、13:30 官方收盤反事實進場／估值；不插值成逐分鐘曲線。
歷史重播維持既有的 13:25 研究契約：13:25 分鐘資料存在時使用該觀測做決策
計價，缺少時依使用者授權改用同日收盤，
並在來源統計中獨立揭露。官方日 OPEN/CLOSE 是歷史反事實價格來源；歷史重建
漲跌幅只作資料品質稽核，不能因 ETF 規則誤分類而把已觀測的官方開盤價延到
後一日。這些價格都沒有交易所成交時間戳或集合競價排隊證據。

歷史訊號、委託／成交與持倉分別使用壓縮 Parquet 和不可變分區快照，避免把
百萬筆完整股票權重塞進即時 JSONL。即時帳本仍只接受當時收到的正式行情，
歷史反事實資料永遠不會回寫成即時成交。

安裝或更新服務：

```bash
sudo scripts/install_tw_overnight_simulation_service.sh
sudo scripts/install_public_dashboards_service.sh
```

驗收至少要同時證明：服務 active、狀態 receipt 可讀、試撮不成交、收盤實際
價格才建倉、次日試撮不平倉、09:00 實際開盤才沖銷、公開 API 不含內部路徑或
憑證；歷史另外驗證日期完整、每模式每日兩個事件、完整訊號可按日查詢、價格／
方向／費稅／權益對帳，以及反事實標籤未被呈現成真實成交。

手動補算與部署：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/maintain_tw_overnight_history.py --force
systemctl list-timers stockagent-tw-overnight-history.timer --all
```
