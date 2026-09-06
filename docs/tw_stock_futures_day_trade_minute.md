# 個股期貨當沖：08:45 決策、08:46 執行、13:30 結束

本契約依 2026-09-06 的使用者要求，沿用一般股票當沖的每日模型與退出節奏。
正式設定為 `configs/markets/tw_stock_futures_day_trade_0845_minute.yaml`，模式為
`tw_stock_futures_day_trade_0845_minute`。模型、BF16、walk-forward、optimizer、
checkpoint、epoch curve 與報告均走既有 `train.py`，每日只產生一次權重。

## 資訊與訂單

| 時間 | 行為 | 可以使用的資料 |
|---|---|---|
| 08:45 | 模型決策，輸出各股票對應期貨的 signed 權重 | 完成至前一股票交易日的 98 個特徵；不使用當天 09:00 股票開盤 gap |
| 08:46 | 第一個完成分鐘棒執行整數口進場 | `[08:45,08:46)` 的期貨成交 VWAP 與 matched volume；未成交進場量取消 |
| 13:20 | 建立被動限價退出單 | `[13:19,13:20)` 完成棒的 Close |
| 13:21–13:24 | 限價單逐分鐘部分成交 | 後續分鐘棒嚴格穿越限價，僅碰價不成交；每棒容量最多 50% |
| 13:24 | 撤換為市價退出單 | 只影響此時之後的成交 |
| 13:25–13:30 | 市價退出餘量 | `[13:24,13:30)` 各完成棒 VWAP 與容量，最後標籤為 13:30 |

右標分鐘棒將成交記在區間完成時刻，與一般當沖的歷史分鐘研究假設一致。
這是從實際期貨交易聚合的分鐘資料；08:45 開盤不再被當成 09:00 決策的進場價。
VWAP 成交假設仍不等於真實委託簿排隊或券商成交保證。

期貨 13:25–13:30 繼續採期貨市場的成交資料，沒有套用現股的收盤集合競價。
一般股票期貨日盤至 13:45，到期月份最後交易日至 13:30；本策略將每天的最後
退出期限設為 13:30。[期交所股票期貨規格](https://www.taifex.com.tw/cht/2/sTF)

## 合約、資金與成本

- 沿用 canonical 近月選擇：同實體契約的前一交易日存在性、成交量與未平倉量，
  每標的各選一個標準（乘數 2,000）及小型（100）候選，當日成交量不能改變候選。
- 新台幣 1,000 萬、整數口數、完整名目金額擔保的研究帳戶。每個標的獨立擁有
  模型分配的資金；沿用標準／小型組合配置器，未成交資金不移給其他股票。
- 每側每口手續費 40 元，期交稅沿用既有日期版本與每口四捨五入契約。
  每次退出均依自己的成交價計稅，不使用股票交易稅、T+2 或融資融券。
- 每個分鐘棒最多成交 `floor(0.5 * matched contracts)`。未退出口數逐分鐘保留，
  同一分鐘容量不重複使用。當日損益與成本影響次日可用資金。
- 已驗證但沒有進場成交的交易日仍保留為零交易報酬；不能事後刪除而放大年化績效。
- 13:30 若仍有餘量，保留 `futures_residual_contract_quantities_history`，標記
  `settlement_default` 為執行失敗，停止該帳戶後續新交易。訓練採吸收式失敗懲罰，
  不能把該懲罰解讀成真實平倉價格或已實現損失；不能宣稱當天成功歸零。
- forward 採整數成交與同一費稅帳，backward 使用有容量限制的 fractional shadow。

## 資料建置與指令

延伸既有 09:00 sidecar builder，復用官方 ZIP parser 與 atomic writer：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_stock_futures_0900_entries.py \
  --execution-policy scheduled_0846 \
  --ticks-root data_tw_index_derivatives_ticks/raw/futures \
  --start-date 2026-06-25 --end-date 2026-09-03 \
  --archive-override data_tw_futures/taifex_stock_futures_minute_v1/source_revisions/Daily_2026_08_17.zip

bash scripts/run_tw_stock_futures_day_trade_0845_minute.sh
```

建置輸出為 `data_tw_futures/taifex_stock_futures_minute_v1/`。每個 requested session
都必須有完整官方 ZIP；缺日期時產生 failure receipt，不能覆寫已接受的資料。
Manifest 綁定 daily source SHA、逐日 ZIP SHA、輸出 SHA 與日期清單。Loader 再驗證
資料來源、唯一實體契約分鐘鍵、價格、量能及全部 panel 日期。

2026-09-06 已建置 2026-06-25 至 2026-09-03 的 50 個交易日、75,434 筆實際
契約分鐘棒。原始 08-17 ZIP 僅含夜盤；上例明確指定同一官方網址重新取得的
日盤完整版本，並保留舊檔與修復 receipt。建置器及 loader 均拒絕只有夜盤的
日期證明。

只有覆蓋已驗證的日期才可用於訓練。上面近期資料的建置指令不會產生 2014 年起
的歷史分鐘資料，故不能直接滿足預設跨年 walk-forward。不得以日 K、舊收盤、
13:45 收盤、插值或連續契約別名補足。歷史 09:00 v4/v5 與舊 08:45 日 K 設定
保留重現用途；新模式、資料 SHA 與執行契約均進 checkpoint，相互不可續跑。

## 2026-09-06 工程驗證

- 真實分鐘資料：50 日、250 個期貨標的的 `[50,250,2,63]` 執行張量，在 CUDA
  完成固定測試權重的回放與反向梯度；數值有限，口數與殘倉 artifact 可往返。
  逐日獨立測試中 08-12 有未平餘量，連續帳戶亦在該日停止。
  證明見 `artifacts/smoke/tw_stock_futures_0845_minute_real_tape_20260906/verification.json`。
- 共用訓練生命週期：合成資料的 98 特徵、lookback 32、Financial Transformer、
  CUDA BF16 兩輪訓練，包含 validation/test、逐 fold 九張必要報告及完成後續跑。
  Eager 與 compiled model 均完成；分鐘整數帳本走既有 eager adapter。
  編譯驗證在 `artifacts/smoke/tw_stock_futures_0845_minute_compiled_fresh_lifecycle_20260906/`。
- 共用舊快取曾出現 Triton `Unknown key: 'cubin'`；獨立快取完成編譯驗證。
  正式設定使用此模式專屬的 TorchInductor／Triton 快取目錄。
- 因果、分鐘邊界、整數費稅、long/short、嚴格穿價、容量、零成交日期、殘倉失敗、
  原始檔完整性、artifact、checkpoint、windowed 與既有 TX/TXO 回歸測試均通過。

上述是真實資料執行驗證及合成資料訓練整合驗證，尚未完成跨年真實資料訓練，
也沒有策略獲利或可上線的結論。
