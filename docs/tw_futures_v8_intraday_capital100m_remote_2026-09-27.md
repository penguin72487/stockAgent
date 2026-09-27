# 期貨當沖：1 億元遠端訓練準備與驗收

2026-09-27 14:40 UTC（台灣 22:40）：資料已完成組裝並逐檔核對遠端 SHA-256；
雙 RTX 5090 的完整 3 epoch DDP、實際梯度、checkpoint 與報表驗收通過。
正式 1,000 epoch 訓練尚未啟動。完成狀態以
`artifacts/markets/tw_futures_v8_intraday_preparation/preparation_status.json` 為準。

## 訓練入口

遠端 `vastai1T` 的 `/root/stockAgent` 使用原有 `train.py`、模型、optimizer、
scheduler、checkpoint 與報表流程。執行：

```bash
cd /root/stockAgent
bash artifacts/markets/tw_futures_v8_intraday_preparation/run_intraday_capital100m_fold5_20260927.sh
```

此腳本先做嚴格 CUDA 環境檢查，再執行以下正式命令：

```bash
source scripts/runtime_env.sh
CUDA_VISIBLE_DEVICES=0,1 MPLBACKEND=Agg run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_intraday_capital100m.yaml \
  --mode train --start-fold 5 --max-folds 1 --epochs 1000 \
  --multi-gpu-strategy distributed_data_parallel \
  --resume --no-retrain-completed-folds --no-post-train-infer --profile-timing
```

正式輸出為 `artifacts/markets/tw_futures_v8_intraday_capital100m_fold5/`。
這與短測及舊一般模式輸出分開；正式訓練不承接短測 optimizer。
`--resume` 供這個正式目錄日後接續，已完成的 fold 會跳過。

## 資料範圍與證據

沿用 `scripts/build_tw_stock_futures_0900_entries.py`，將原 v3 已驗證事實與補齊的
FinMind 實體月份逐筆資料合併，輸出
`artifacts/data_preparation/all_futures_intraday_v8_20260927_v4/`。
沒有改造另一套下載器或訓練器。

| 檢查 | 結果 |
|---|---:|
| 日期 | 2020-03-23 至 2026-09-04 |
| 交易日 | 1,573 |
| 選定近月合約交易日 | 431,434 |
| 分鐘來源驗證 | 407,065 |
| 官方證明無單式成交 | 23,897 |
| 官方量證明參與率下不足一口 | 472 |
| 未解決合約交易日 | 0 |
| 排除日期／合約 | 0 |
| 排程成交事件分鐘列 | 1,238,169 |
| 私有遠端傳輸 | 7 檔、90,038,801 bytes；SHA-256 全數一致 |

選定合約交易日包括股票期貨 381,509、ETF 期貨 21,814、指數期貨 28,111。
這是既有因果近月 selector 的完整範圍，不代表所有到期月份，亦不表示每個合約
在每個出場分鐘都有成交。官方零量或不足一口只提供容量證據，不製造分鐘價格。
訓練檔提供既有策略排程所需事件分鐘；`all_minutes.parquet` 混合原排程分鐘與新增
來源的全時段分鐘，不應當成任意逐分鐘決策策略的完整行情表。

日線 SHA-256：`70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`。
分鐘 manifest SHA-256：`40db14897d9f39b0d2c4789495cdfa1c0a412b06a00767cd3a2174f6a5da13b0`。
分鐘 parquet SHA-256：`22b0520912b65db87542820be269942f7649a72972be8412cdf2ac8c21f7d5a0`。

股票背景固定為
`tw-public-20260913T030921278338896Z-l0-penguin-248d0869d7d53be4`，
遠端重新驗證 131,332 個 materialized 來源檔及 13,304,716,279 bytes 冷物件，
租約更新至 2026-10-04 14:02:06 UTC。
FinMind 原始逐筆留在本機；只傳本帳戶私有研究所需的衍生分鐘包，未發布到公共或冷庫分發。
補抓完成後原 Free／Sponsor／Complement 排程已恢復。

## 模型、資金與因果時間

設定繼承既有 v5 FinancialTransformer、22 種時間基底、99 個股票背景特徵、
`score_entmax_log_cash`。沿用雙 RTX 5090、BF16、global batch 32、eval batch 16、
最多 1,000 epoch 及既有早停。保留每 epoch 訓練、驗證、測試與曲線。

初始資金為 TWD 100,000,000；這是 `intraday`＋`notional` 全額預留模式。
交易標的是台灣掛牌股票、ETF、指數期貨，股票僅提供背景資訊。
08:45 以已完成的前期資料決策，最早使用右標 08:46 分鐘成交；13:20 掛出場限價，
13:24 改市價（最早消耗 13:25 右標分鐘），13:30 截止。
容量為 `floor(0.5 × 實際觀測分鐘口數)`，不足容量不以日線或假成交補足。
成交分鐘只交給執行帳本；未能平倉保留失敗與殘量。

當沖資料從 2020 年開始，因此相同驗證／測試年份改編為 fold 5：
train 2020–2024、validation 2025、test 2026。
來源 v5 fold 10 的 122 個骨幹張量依既有
`matching_validation_and_causal_train_superset` 政策移轉，保留來源因果 RMS 與時間投影，
不匯入 optimizer。此設定保留全模型訓練，未把一般留倉的凍結骨幹實驗直接套入當沖。

## 本次修復與驗收

- 對齊日期：把共用對齊鍵轉成交易日，支援 canonical panel 的 `datetime64[ns]`。
- 嚴格模式：轉接到既有分鐘執行器時，明確傳入期貨 action 軸的雙向權限；股票背景
  的買賣 mask 不套到不同寬度的期貨軸。
- 報表彙整：4D 全期貨分鐘 tape 沿用整數期貨 NAV、失敗與口數稽核緩衝；保留
  `contract_quantity` 單位、實際合約口數及未平倉殘量，存檔與載回均檢查。

本機與遠端各 189 項相關測試，加 1 項真實模型從空手初始化通過分鐘損益更新的
測試通過，包含分批 1／2／3 列對完整帳本的精確一致性、
多空、缺少出場成交、存檔載回，以及拒絕把殘量誤標成成功平倉。
遠端原有不同的 crypto 程式修改逐段保留，未以本機整份 trainer 覆蓋。

| 遠端完整短測 | 結果 |
|---|---|
| 產物 | `artifacts/markets/tw_futures_v8_intraday_capital100m_fold5_smoke3_20260927_r2/` |
| 完整 epoch | 3；每次保留 train／validation／test 與曲線 |
| optimizer | 144 組參數狀態皆完成 3 步；first moment 皆有限且非零 |
| 模型 | 148 個 checkpoint 張量皆有限；從零初始化的期貨輸出頭已更新 |
| BF16 | GradScaler 停用，儲存狀態為空；scheduler 狀態存在 |
| 曲線圖 | 2 張 PNG 解碼成功，完整 fold 報表通過 canonical lifecycle 檢查 |
| 完成後 `--resume` | 正確跳過，checkpoint 與 epoch curve SHA 未改變 |
| 執行器測試 | 另有 8 項實際 CUDA Graph 測試通過；21 項未選取 |
| 第 3 epoch 最大 rank 時間 | 8.593 秒；只有一個 epoch 3+ 樣本 |
| GPU 記憶體 | 500 ms 抽樣峰值 7,142／5,736 MiB；非 allocator 高水位 |

帳本修復後，驗證正確偵測到移轉初始策略的未平倉失敗，故沿用既有機制將期貨
輸出頭初始化為空手，保留骨幹與可學習的分鐘 backward 路徑。最佳 checkpoint
仍以現金為下限。短測只更新 optimizer 3 次，仍在 32-step warmup 內，三輪
train／validation／test loss 皆為零，最佳 checkpoint 報酬也為零；不能把它解讀為
已獲利或已充分探索整口交易。

曾懷疑零報酬代表零梯度，但實際 fresh optimizer 的 144 組 first moment 全數
有限且非零，輸出頭也由零變為非零；新增測試排除 weight decay／輔助 loss 後，
仍能由真實分鐘損益方向更新。因此保留原本安全初始化，沒有部署恢復失敗策略的
候選修改。這份驗收確認可以訓練，不保證收益；亦未驗證中斷時 optimizer／RNG
恢復，只驗證已完成 fold 的 resume 跳過。

主要收據：

- [完整資料驗證](../artifacts/markets/tw_futures_v8_intraday_preparation/complete_v4_validation_20260927.json)
- [遠端最終可訓練狀態](../artifacts/markets/tw_futures_v8_intraday_preparation/remote_ready_20260927.json)
- [完整三輪與圖表／resume 收據](../artifacts/markets/tw_futures_v8_intraday_preparation/remote_acceptance_20260927/smoke3_receipt.json)
- [實際梯度與模型更新驗證](../artifacts/markets/tw_futures_v8_intraday_preparation/learning_gate_20260927.json)
- [啟動修復紀錄](../artifacts/markets/tw_futures_v8_intraday_preparation/intraday_startup_repairs_20260927.json)
