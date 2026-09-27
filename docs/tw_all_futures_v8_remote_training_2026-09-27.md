# v5 改為全台灣期貨：當沖與一般留倉

遠端 `vastai1T` 已建立獨立工作目錄 `/root/stockAgent-futures-v8-20260927`，
沿用指定的 `tw_day_trade_v8_combined_annual_log_cash_sub_lot_first_session_all_folds_v5`。
兩個模式有不同設定、輸出目錄、持倉契約與 checkpoint 指紋。正式 1,000 epoch 訓練尚未啟動。

| 模式 | 行為 | 目前狀態 |
|---|---|---|
| `carry` 一般 | 同一實體期貨可跨日持倉，日盤開盤再平衡，到期使用官方結算價 | 雙 GPU 完成 3 epoch、完整 fold 測試與報表，可啟動研究訓練；受下述合約隔離限制 |
| `intraday` 當沖 | 08:45 決策，08:46 分鐘執行，13:20 限價出場、13:24 起市價出場，13:30 截止 | 程式與設定已備妥；全商品分鐘歷史不足，啟動會阻擋 |

## 遠端執行

```bash
cd /root/stockAgent-futures-v8-20260927
bash scripts/run_tw_futures_v8.sh carry check
bash scripts/run_tw_futures_v8.sh carry train
```

當沖補足並通過相同來源契約後，使用獨立指令：

```bash
cd /root/stockAgent-futures-v8-20260927
bash scripts/run_tw_futures_v8.sh intraday check
bash scripts/run_tw_futures_v8.sh intraday train
```

目前當沖的 `check` 與 `train` 都會以退出碼 2 回報資料缺口，不會啟動模型訓練。
`train.py` 自行啟動 DDP、隔離各 fold 並處理續訓；不要在這些指令外再包一層 `torchrun`。
啟動器先核對固定來源 manifest，正式啟動再執行 CUDA 嚴格檢查。
兩個正式模式共用工作目錄內的執行鎖，避免互相搶同一組 GPU。

設定：

- `configs/remote/tw_all_futures_v8_carry_20260927.yaml`
- `configs/remote/tw_all_futures_v8_intraday_20260927.yaml`
- `configs/remote/tw_all_futures_v8_provenance.json`

輸出分別是 `artifacts/markets/tw_all_futures_v8_carry_20260927_v1` 與
`artifacts/markets/tw_all_futures_v8_intraday_20260927_v1`。保留預設續訓設定；不可把兩個模式的 checkpoint 互換。

## 第一性原理調整

1. **可觀測資訊先於交易。** 保留完整股票市場作為背景資訊，但交易輸出只有期貨。
   99 個原始特徵維持 v5 的輸入順序，股票當日開盤 gap 再延後一個交易日，
   08:45 決策只能看到先前已完成的股票與期貨資料。停用當日股票規則與當日期貨開盤價輸入。
2. **模型決定風險與現金。** 繼承 FinancialTransformer、22 種時間基底與
   `score_entmax_log_cash`，輸出有號期貨配置與現金。使用既有全期貨輸出介面，
   固定 1,936 個 slot，不另寫 trainer、AMP、scheduler 或帳本。
3. **先核對真實口數經濟。** 帳戶為 TWD 10M，整口成交，研究用每口每邊佣金 TWD 40，
   依日期套用既有期交稅率；佣金是假設值，不代表券商報價。
   一般模式依前一交易日量限制交易，當沖依實際分鐘量取 50% 後向下取整。
   缺價、缺容量或無法平倉保留失敗狀態，不補虛構成交。
4. **持倉路徑決定訓練方式。** 每條完整軌跡才更新一次參數；一般模式延續整口持倉與權益，
   當沖只延續帳戶權益。移除原股票的年度帳戶重置與股票零股梯度機制，改用既有期貨可恢復梯度。
   正向損益仍由整口帳本計算，近似僅用於反向學習。
5. **來源模型只能看過往。** 一般模式按相同 train/validation 年份移植 v5 骨幹；
   當沖較短歷史只允許相同 validation 年、較早訓練前綴的來源 fold。
   不讀來源 test 表現選模型，不匯入來源 optimizer。目標驗證未優於現金時保留現金基準，訓練仍正常學習。
6. **以完整流程評估速度。** 保留 BF16、兩張 GPU、global batch 32、eval batch 16、
   CPU panel、已驗證的模型 forward/backward 編譯。一般帳本編譯候選雖更快，未達完全一致，維持關閉。

一般模式預計 10 folds，首個訓練年 2015；當沖預計 5 folds，首個訓練年 2020。
兩者都是每 fold 最多 1,000 epoch，保留原設定的早停規則與每 epoch 曲線。

## 資料與能力邊界

股票背景固定於 `tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`。
期貨日資料為既有 prepared bundle：
`/root/stockAgent/artifacts/data_preparation/futures_daily_70a57dd76de74fd0a365/continuous_daily.parquet`，
SHA-256 `70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`。
日資料截至 2026-09-04；沒有使用另一份未稽核的最新資料取代它。

商品範圍為既有契約支援的台灣掛牌股票、ETF 與指數期貨；歷史乘數或幣別不受支援的商品不交易，
也不包含選擇權。當沖採各產品日曆上的近月合約，不用當日成交量挑合約。
1,936 是固定輸出容量，不是每日都有 1,936 個可交易合約。

一般模式讀取 `/root/stockAgent/data_tw_futures/final_settlement_v1/futures_final_settlement_history.parquet`。
目前 3,597 個實體合約缺少可用官方到期結算價，既有資料契約隔離整段合約。
因此目前是**可驗證資料子集的全類別期貨研究**，不是所有歷史合約皆完整的宣稱。
合約隔離可能影響回測代表性，不能把結果當成完整市場無偏估計。

帳本以總名目曝險不超過資產淨值為限制，沒有驗證歷史保證金槓桿、SPAN、逐日追繳或夜盤盤中風控。
一般模式可跨夜，但每日只決策一次；當沖分鐘價量模擬不是五檔委託簿成交證明。
當沖報表以不計息的新台幣現金為基準；一般模式沿用同一實體近月 TX 的基準，
不把含隔夜損益的 TX 基準套到當沖報表。
13:30 是本策略的當沖截止時間；台指一般日盤時段與到期日時間應以
[期交所台指期規格](https://www.taifex.com.tw/cht/2/tX?menuid1=12)為準。
交易稅依[期交所費率表](https://www.taifex.com.tw/cht/4/feeSchedules)與程式內日期版本處理。

## 當沖缺口

遠端既有分鐘來源的核對範圍為 2020-03-23 至 2026-09-04：

| 類別 | 缺少合約交易日 | 涉及產品 |
|---|---:|---:|
| 指數期貨 | 28,111 | 22 |
| ETF 期貨 | 21,814 | 26 |
| 股票期貨 | 224 | 7 |
| 合計 | 50,149 | — |

需要 431,434 個合約交易日，已接受 381,285 個；選取範圍共 363 個歷史產品。
證據位於遠端 `artifacts/data_preparation/all_futures_intraday_v8/manifest.json` 與
`missing_contract_days.parquet`。本機另一份歷史來源缺 50,147，並發現 `OJFJ6` KBar 價格尺度不一致；
遠端正式入口採較嚴格來源，所以以上表為準。

可重跑來源稽核：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/prepare_all_futures_intraday.py \
  --daily /root/stockAgent/artifacts/data_preparation/futures_daily_70a57dd76de74fd0a365/continuous_daily.parquet \
  --legacy-minutes /root/stockAgent/artifacts/data_preparation/futures_minutes_capacity_ceil_quarantine_v2_20260909/minutes.parquet \
  --output artifacts/data_preparation/all_futures_intraday_v8
```

這會留下缺口清單；不下載、不合成分鐘量、不用日 OHLC 冒充 08:46 或 13:30 成交。
補資料須回到既有 canonical collector、來源 receipt 與發布流程。

## 驗證證據

遠端 `artifacts/operations/futures_v8_prepare_20260927/` 留存設定來源、preflight、短訓練與編譯比較。
一般模式實測 2 張 RTX 5090，完整首 fold 訓練 2015、驗證 2016、測試 2017–2026。
標準路徑第 3 epoch 約 3.53 秒；每 epoch 一次 optimizer step，梯度非零，BF16 scaler 為空。
帳本編譯候選第 3 epoch 約 1.00 秒，但 train/validation/test scalars 與 141 個最後模型張量不完全一致，未採用。
這是單一 fold 的有界測量，不能外推所有 fold 的速度或整個 epoch 皆經編譯。

兩次短訓練均通過 canonical lifecycle 的 18 個產物檢查。
最佳 checkpoint 仍為現金基準；三個 epoch 不構成獲利或部署驗證。
本機 focused suite 194 passed；加入因果拒絕與來源擾動測試後 adapter suite 12 passed（有重疊，不相加）。
已重跑相同已完成 fold 的續訓指令，跳過模型訓練，epoch 曲線 SHA-256 維持相同。
較廣回歸 138 passed、5 個舊分鐘 CLI 測試因要求已缺冷庫物件的舊股票 release 而失敗；
此舊 release 與本次固定的 `248d0869` 來源不同，沒有放寬任何來源檢查。

程式在獨立 worktree；主工作目錄原有變更保留。
遠端基底 Git 為 `fc1f2a18beaf8ac347eada758cf0e4c37c13efb6`，沿用來源當時的 152 個程式／設定 overlay，
清單記在 `workspace_source.json`。本次單獨差異另存 `futures_v8_only.patch`，可供後續程式審查。
