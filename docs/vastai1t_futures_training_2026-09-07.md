# vastai1T 股票期貨當沖資料驗收（2026-09-07）

目前仍不能宣稱「只差輸入指令開始正式年度訓練」：原設定要求 2014 年起的
實體契約分鐘歷史，目前已建置的官方分鐘成品只有 2026-06-25 至 2026-09-04，
共 51 個交易日。2014-01-02 至 2026-06-24 的 3,042 個股票面板交易日尚未接入
目前這份實體契約分鐘成品；這不表示本機未下載永豐期貨歷史。
本機另有 2020-03-23 至 2026-09-04 的 R1／R2 原始 Tick，743 個查詢代碼中
729 個查詢完成（670 個有資料、59 個全空），14 個有來源代碼不可用紀錄。
完整清單見 [本機永豐期貨盤點](SHIOAJI_FUTURES_INVENTORY_2026-09-07.md)。
連續資料接入實體月份訓練仍須核對契約身分與換月規則。
新版期貨 release 已發布為
`tw-futures-20260907T024404680420085Z-l0-penguin-fb2f3972dafefd96`；
vastai1T 已完成完整物件驗證、解包、逐檔 SHA 與 `READY` 驗收，正式 YAML 已固定此版本。

訓練沿用 `train.py` 與
`configs/markets/tw_stock_futures_day_trade_0845_minute.yaml`。
FinancialTransformer、98 個前一完成日股票特徵、多基底、BF16、1000 epochs、
年度 walk-forward、checkpoint 與報告流程均保持原設定。
08:45 決策、08:46 進場、13:20 限價出場、13:24 改市價、13:30 沖銷期限保持一致。

## 已完成與已驗證

- vastai1T 正式股票面板：3,093 日、2,753 個股票欄位、98 個特徵；
  日期 2014-01-02 至 2026-09-04。約 3.6 GB 的可重建快取位於
  `artifacts/cache/tw_stock_futures_day_trade_0845_minute_panel`。
- 官方期貨日線已補至 2026-09-04；分鐘成品重新建置為 51 日、77,075 列，
  日線／分鐘 SHA 與來源收據均已核對，並已在 vastai1T 完整 materialize。
  `tw-futures` 共 1,859,740 檔、15,391,349,461 bytes；
  manifest SHA：`a3a7a99dc7068959f49087c51b23b6576e68da3976275f752c5bf6be2aac9fec`。
  對照舊版，原有分鐘列沒有刪除或變更；新增 2026-09-04 的 1,639 列及
  2026-09-03 的 2 列 VQF。VQF 在 09-03 尚不符合前一日候選條件，
  實際從 09-04 才成為候選，未回填成 09-03 的可交易標的。
- 實際到期月份的 Shioaji KBar／Tick 歷史已經由 `tw-shioaji-history` catalog
  發布並在 vastai1T 完整驗證、materialize：181,983 檔、2,486,170,659 bytes。
  遠端 release：
  `tw-shioaji-history-20260907T021442823767017Z-l0-penguin-58e1458edf26e68a`。
  此項僅表示來源可用，不代表符合當沖 VWAP 或完整年度歷史。
- 遠端既有分鐘資料接入／preflight／啟動器修正已整合回本機；新增重疊 KBar
  去重修正，兩台均通過相同 69 項整合測試。
- vastai1T 的嚴格 CUDA 環境檢查通過；51 日、250 個期貨標的的真實分鐘 tape
  `[51,250,2,63]` 通過 GPU 執行器、有限梯度與回測成品讀回檢查。
  固定未訓練權重在 2026-08-12 留下 1 口無法於 13:30 前退出；執行器如實標記失敗、
  停止該帳戶後續新交易，沒有捏造平倉價。這不是訓練策略績效或全數成功沖銷的證明。
- 2026-09-07 03:16 UTC，penguin 與 vastai1T 雙向 Syncthing 驗收通過：
  canonical folder idle、所有需要同步／刪除項目為零、錯誤為零、peer completion 100%、
  remoteState valid。實際連線為 QUIC／TLS 1.3。

## KBar 實際資料限制

下載器保留的增量查詢可能日期重疊。接入器現在比較同日全部正成交量日盤 KBar；
成交內容完全相同才去重，避免同一分鐘成交量計算兩次。內容衝突仍拒絕使用。

部分個股期貨 KBar 的 Amount 無法提供目前執行合約要求的精確 VWAP：
CAFI6 在 2025-11-12 的單口 KBar OHLC 全為 49.65，Amount 卻為 49。
保留原始資料與錯誤證據，未自動改用 Close 或修改原始數值。
證據：`kbar_price_validation.json`、`kbar_source_coverage.log`。

Shioaji 官方公布的期貨歷史起日為 2020-03-22；已到期實體代碼查不到時，
只能透過 R1／R2 查詢連續歷史。當前 R1 target 不能當成過去每一天的實體月份，
因此不能用目前的 mapping 把連續歷史重新標成實際月份。
[Shioaji 歷史資料文件](https://sinotrade.github.io/zh/tutor/market_data/historical/)

## 補足原年度設定所需的來源

需要含商品、實際到期月份、成交時間、成交價格與成交口數的歷史資料，
或能支持相同 VWAP／分鐘容量合約的可信一分鐘成品。
完整缺日表：`stock_panel_minute_coverage.csv`。
每個必須覆蓋的日期／實體契約清單：`required_physical_contract_days.parquet`。

期交所「期貨成交簡檔」公開價格為 NT$1,000／半年；
2014 上半年至 2026 上半年共 25 期，按牌價估算 NT$25,000。
官方起迄時間表載明自 1998-07-21 起提供至申購日前一完整月份。
此處是需求與牌價試算，沒有建立訂單或付款。
[官方商品頁](https://edatashop.taifex.com.tw/zh/product/list/3)、
[官方價格及起迄時間表](https://www.taifex.com.tw/file/taifex/CHINESE/3/交易歷史資料價格及提供時間_adj.odt)

官方商店範例已用既有 futures parser 驗證：24 列中，22 列單式成交成功解析，
2 列跨月價差被既有單式篩選排除。正式交付 ZIP 仍需檢查成員名稱、格式、日期、
SHA 與完整覆蓋；範例成功不代表尚未取得的檔案已驗證。
驗證收據：`official_store_sample_check.json`。

原始歷史先進入 catalog 的可寫來源，經既有 builder、publish、Syncthing、
完整 verify 與明確 materialize，再更新正式 YAML 的固定 release 與 output root。
不縮短年度切分、不補造分鐘、不使用日線價格代替分鐘執行。

## 入口與證據

在 `/root/stockAgent` 使用共用入口檢查：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_stock_futures_day_trade_0845_minute.yaml \
  --check-data-only
```

目前預期會因缺少 2014 起的分鐘歷史而拒絕正式訓練。
已在 vastai1T 實測退出碼 1，明確指出 `configured first panel year 2014` 的歷史缺口。
資料全數補齊、驗收通過後，同一指令移除 `--check-data-only` 即進入原訓練流程；
原 `scripts/run_tw_stock_futures_day_trade_0845_minute.sh` 也保留共用資料生命週期與入口。
本次固定資料對應的 output root 為
`artifacts/markets/tw_stock_futures_day_trade_0845_minute_v1_data_20260907`，
目前未建立，正式訓練未啟動。

本次操作與校驗收據位於：
`artifacts/operations/vastai1t_futures_prepare_20260907/`。
其中 `readiness.json` 為最後整合驗收；`stock_panel_preparation.json` 為完整股票
面板與逐日覆蓋；`minute_execution_smoke/verification.json` 為真實分鐘資料的
GPU 執行器與梯度驗證。固定未訓練權重的執行器驗證不代表完成模型訓練。
