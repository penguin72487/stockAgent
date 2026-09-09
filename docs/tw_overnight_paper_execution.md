# 台股隔日沖公開模擬契約

這個產品把「模型決策」、「委託參與」與「實際撮合證據」分開。公開頁面是
`/tw-overnight/`；它只讀取 `artifacts/live/tw_overnight_simulation`，不會呼叫
券商下單 API，也不共用當沖的持倉或成交帳本。

## 時鐘與成交

1. 13:25：使用最新同日行情更新模型輸入。現階段暫時沿用對應的當沖
   checkpoint；產物會明確標成「未針對隔夜風險訓練」。
2. 13:25：多方以當日漲停價掛 `LMT_ROD` 買進，空方以當日跌停價掛
   `LMT_ROD` 賣空。張數以 13:25 價格和各模式的獨立初始資金計算。
3. 13:25–13:30：`simtrade=true` 只代表試撮，永遠不記為成交。
4. 13:30：只在同一交易日、非試撮且帶交易所時間戳的實際收盤價出現後，
   才以該撮合價建立紙上部位。延緩收市的證券可等到 13:33。
5. 次一有效交易日 08:30：多方以當日跌停價掛賣出，空方以當日漲停價掛
   回補，參與開盤集合競價。08:30–09:00 的試撮仍不是成交。
6. 次一有效交易日 09:00：只在同日、非試撮且帶交易所時間戳的實際
   `open` 出現後，才以開盤撮合價沖銷。缺少證據時持倉保持未平並揭露原因。

13:15 先準備完成 panel、checkpoint、模型與 CUDA cache；這個預熱不要求
當沖資格或 09:00 MIS 開盤資料。13:25 的正式訊號才取得當下行情並寫入獨立
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
Discord 13:25 排程（沿用模型）
  -> artifacts/live_signals/tw_overnight_*/latest_signal.json
  -> stockagent-tw-overnight-simulation.service
  -> 共用當沖 Shioaji 報價 broker（不建立第二個登入 session）
  -> 隔日沖 append-only signals/orders/fills/marks
  -> 唯讀公開閘道 /tw-overnight/api/*
  -> /tw-overnight/
```

安裝或更新服務：

```bash
sudo scripts/install_tw_overnight_simulation_service.sh
sudo scripts/install_public_dashboards_service.sh
```

驗收至少要同時證明：服務 active、狀態 receipt 可讀、試撮不成交、收盤實際
價格才建倉、次日試撮不平倉、09:00 實際開盤才沖銷、公開 API 不含內部路徑或
憑證。新服務不回補不存在的歷史隔日沖成交。
