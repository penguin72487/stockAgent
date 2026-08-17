# TAIFEX 全個股／ETF／指數期貨日資料與訓練模式

## 已整理的資料範圍

資料模式為 `tw_futures_portfolio_day`，只納入 TAIFEX 掛牌、且標的屬於：

- 個股期貨
- ETF 期貨
- 國內指數／產業指數期貨
- 海外指數期貨

不納入選擇權、匯率、黃金、原油、利率與債券等非本模式範圍商品。官方歷史
日檔的 756 個商品代碼中，本模式整理出 745 個商品家族：693 個股、28 ETF、
17 國內指數、7 海外指數；其中包含已下市的歷史期貨，不只目前仍掛牌商品。

主要輸出位於 `data_tw_futures/taifex_portfolio_daily/`：

| 檔案 | 用途 |
|---|---|
| `continuous_daily.parquet` | 完整日資料、實體合約、交割月槽位、報酬與清算標記 |
| `product_master.parquet` | 商品、Shioaji root、標的代碼、資產類別、國內／海外分類 |
| `model_features.parquet` | 期貨特徵加上可對齊的公開資料特徵 |
| `symbols/*_features.parquet` | 復用既有 `build_panel` 的每標的輸入 |
| `manifest.json` | 來源與輸出 SHA-256、資料量、契約版本、因果／執行契約 |

資料量、標準化序列數與 SHA-256 以同目錄的 `manifest.json` 為準；每次來源更新或
資料契約改版都會重新產生，不在文件中硬編可能過期的數字。

## M01～M12 與留倉定義

月契約依交割月份固定為 `PRODUCT_M01_L1`～`PRODUCT_M12_L1`。例如
`TX_M09_L1` 永遠是該 lane 中的九月交割合約，不會因八月合約到期而改名，所以
遠月部位可跨日續抱直到該實體合約到期。極少數海外指數期貨會同時掛牌兩個不同
年份、但同為九月交割的合約；這時用 `L1`、`L2` 做生命週期固定的 overlap lane，
兩張合約都保留，且不會在近年合約到期後把遠年合約換 lane。

一般盤來源另有 3,614 筆 MTX 週契約成交列，因此為了維持「所有 TAIFEX 指定商品」
而沒有偷偷刪除，週契約使用 `PRODUCT_M01W1_L1`～`PRODUCT_M12W5_Ln`。
`tenor_rank` 仍保留作為當日遠近月特徵，但不再決定模型 symbol，也不會觸發
不必要的換倉。

真正的留倉／清算判定以官方資料中的實體合約連續性為準：

- 同一交割月 lane 下一個交易日仍是同一實體合約：可留倉；有實際開盤時用
  `open[t] -> open[t+1]`，零成交日則接續上次可得的正數結算／收盤估值。
- 下一日換成別的實體合約、該實體合約的最後觀察交易日或資料終點：
  `must_liquidate=true`、`can_hold_overnight=false`，報酬只算舊合約自己的
  `open[t] -> close[t]`，並在收盤強制清零。
- 下一交易日的新合約可以重新開倉；舊／新合約的價差永遠不算投資報酬。

遠月在某個交易日完全沒有成交時，該日不可新開、加碼、減碼或主動平倉；既有
部位則使用上次可得的正數結算／收盤值延續估值，直到下一筆實際開盤價才一次反映
期間價格變動。它不會因零成交日被假設提前平倉。最終強制日以歷史資料最後觀察到
的該實體合約交易日為安全邊界；在缺少全商品 point-in-time 法定最後交易日主檔時，
不把第三個星期三代理值冒充成精確法定到期日。

`liquidation_reason` 會區分 `last_trade_date`、`expiry_slot_contract_change` 與
`dataset_terminal`。每個 validation/test fold 的最後一列
也會額外強制平倉，避免績效漏算研究區間終點的出場成本。

## Feature 與因果邊界

這是跨日持倉模式，不是當沖模式。它直接復用現有 daily panel、Transformer、
walk-forward、loss、checkpoint 與 lifecycle。模型每天可續抱、加減碼或主動平倉；
只有實體合約到期、資料中斷或研究 fold 結束才強制清零。模型在交易日 `t` 開盤
只能使用完成至 `t-1` 的資料：

- 期貨 OHLCV、結算價、未平倉量、最後 Bid/Ask、價差單量、相對近月與 tenor
  資訊。
- 個股／ETF 期貨會連結對應現貨標的的公開特徵。
- 指數與海外指數期貨使用市場層級的公開總經、匯率、TAIFEX 籌碼等特徵。

原始公開資料仍保留完整稽核來源，但專案明文禁止的 snapshot-only 欄位不進模型，
包括 TDCC、公司現況、月營收、財報、內部人、借券／可放空快照等欄位。模型實際
目前保留 115 個 causal features；實際名單由 config 與 panel log 共同稽核。

## 建置與訓練

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/download_taifex_contract_codes.py
run_fintech_python scripts/build_taifex_futures_portfolio_daily.py
run_fintech_python train.py --config configs/markets/tw_futures_portfolio_day.yaml
```

訓練設定檔：`configs/markets/tw_futures_portfolio_day.yaml`。

## 成本與目前限制

v1 是 continuous-notional 研究 ledger，按每側成交名目金額收取設定檔中的比例成本，
並正確計入換倉前舊合約的強制平倉成交。它不假裝是精確整數口數 ledger：目前尚未
擁有涵蓋全部 745 個商品、且具歷史時點版本的乘數、原始保證金、維持保證金與券商
固定手續費主檔。因此 v1 不會把不完整的現行規格倒灌到歷史。

若要升級為可交易資金／整數口數回測，下一個資料契約必須先補齊上述 point-in-time
商品規格與成本來源，再新增 exact integer oracle；不應以單一 TX/MTX/TMF 規格代替
全市場商品。
