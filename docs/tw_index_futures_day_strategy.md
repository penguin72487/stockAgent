# 股票橫截面驅動的台指期日盤策略

## 交易契約

這個策略把台股橫截面視為市場狀態，不直接交易個股。模型每天只輸出一個
`[-1, 1]` 的台指方向曝險；執行器再用同方向的 TX、MTX、TMF 整數口數逼近
目標名目本金。三種商品的乘數分別為每點新台幣 200、50、10 元。

預設時序如下：

1. 期貨交易日 `t` 的 08:45，只讀取截至股票交易日 `t-1` 收盤已完成的資料。
2. 以期貨日盤開盤價建立部位。
3. 同一日盤收盤前全部平倉，不承擔夜盤曝險。

模型不讀取交易日 `t` 的股票開盤、最高、最低、收盤或成交量。期貨日盤的
開盤價、可交易狀態與實際到期月份也只屬於執行器，不是模型特徵。

策略設定直接繼承 `tw_public_lanten_market_candles.yaml` 的股票資料、歷史
特徵、walk-forward、lookback、optimizer 與
`training.transformer_base_portfolio` 參數。唯一的資料特徵例外是
`next_session_open_gap_logret`：原 candles 策略可在股票開盤後使用它，但
台指期 08:45 決策時股票尚未開盤，因此子設定以
`data.day_trade_open_feature: false` 阻止它進入 feature schema。期貨專用模型
仍由原本 model factory 建立，再把個股投組輸出頭換成單一 `[-1, 1]` 指數曝險頭。

## 資料

下載器只接受臺灣期貨交易所官方 `futDataDown` 收據，保留原始 ZIP/CSV、
SHA-256 與 manifest，然後：

- 只保留 `一般` 交易時段；
- 只保留 TX、MTX、TMF；
- 排除週選式月份與跨月價差；
- 保留每個交易日的所有月契約，另外標記最近的未到期前月；
- 產出 `data_tw_index_futures/day_session_contracts.parquet`，舊的
  `day_session_front_month.parquet` 保留但不再供此策略使用。

保留完整月契約不是額外資料浪費，而是正確轉倉的必要條件。正式 benchmark
是「1 倍全額擔保、長期做多 TX 前月、未扣費用」：一般交易日使用當日前月
收盤相對同一合約前一交易日收盤；前月切換時，視為在前一交易日收盤轉入
新前月，第一筆新月報酬仍使用新合約自己的前一交易日收盤。因此新舊合約
價差不會被誤認為報酬。每個 fold 另存 `futures_benchmark_audit.npz`，包含
日期、持有月份、換月旗標、同合約前收、當日收盤與實際 benchmark 報酬。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/download_tw_index_futures_day_session.py \
  --start-year 2005
```

若官方原始收據已完整存在，只重建 v2 parquet、不重新下載：

```bash
run_fintech_python scripts/download_tw_index_futures_day_session.py \
  --start-year 2005 \
  --rebuild-normalized-only
```

## 訓練與評估

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/tw_index_futures_day.yaml
```

canonical 訓練預設以新台幣 1 億元本金寫入
`artifacts/markets/tw_index_futures_day_canonical_100m_rolling_v2_exact_v4`。
舊 checkpoint 只保留作推論與稽核；exact v4 把交易稅修正為只課賣出腿，
因此不得靜默續接舊 optimizer trajectory。
舊的 `artifacts/markets/tw_index_futures_day` 是已退役獨立 trainer 的 checkpoint，
因為缺少 canonical manifest，不會拿來接續 optimizer，也不會被自動刪除。

若要先驗證最新 walk-forward fold，而不修改正式 YAML：

```bash
run_fintech_python train.py \
  --config configs/markets/tw_index_futures_day.yaml \
  --epochs 2 \
  --start-fold 12 \
  --max-folds 1 \
  --no-resume \
  --no-post-train-infer \
  --output-dir artifacts_smoke/tw_index_futures_day
```

雙 RTX 5090／dual-EPYC 主機使用已實測的專用設定與 NUMA affinity：

```bash
bash scripts/run_tw_index_futures_day_dual_5090.sh
```

要保留完整 validation、sampled test、曲線、checkpoint 與 lifecycle gate，
但只測最新 fold 的兩個 epoch：

```bash
bash scripts/run_tw_index_futures_day_dual_5090.sh \
  --epochs 2 \
  --start-fold 12 \
  --max-folds 1 \
  --no-resume \
  --no-post-train-infer \
  --output-dir artifacts_smoke/tw_index_futures_day_dual_5090
```

在 `S=2744`、`F=98`、lookback 32 的 fold 12 實測，global batch
128／256／512／640 的 steady train throughput 分別約為
5276／5209／6064／5706 rows/s。global batch 768 雖通過 compile probe，卻在
完整 train hotpath 以 31.33/31.36 GiB OOM，因此專用設定採用最快且仍有
VRAM 餘裕的 global batch 512。

這不是另一套 trainer。面板快取、walk-forward、lazy window、torch.compile、
optimizer、checkpoint/resume、`epoch_curve.jsonl`、early stopping、推論與
fold artifacts 全部沿用 `train.py` 的 canonical 路徑。正式策略使用基準設定
繼承而來的 `temporal_pooling: last` 與 `temporal_query_mode: last_only`；
train/eval batch size、epoch、learning rate、weight decay、seed 與
torch.compile 也全部繼承，不再維護另一組訓練超參數。唯一的 CUDA 執行
穩定性覆寫是 `sdpa_batch_limit: 16384`：它只切分 `batch * symbols` 的 SDPA
工作，不改變模型參數、token 或注意力計算語意。

訓練目標使用連續曝險的費後 log utility；測試輸出則使用 TX/MTX/TMF 實際
乘數、整數口數、賣出期交稅、交易/結算費與設定中的券商手續費、滑價重算。
目前交易人實付的總手續費按每口、每邊設定為 TX 60 元、MTX 24 元、TMF
16 元。這些優惠價視為已包含期交所交易／結算費；載入器只在內部拆出剩餘
券商部分，避免重複收費。每次進場與平倉各收一次；`0.0002` 交易稅只按
賣出契約價值收取，多單是收盤平倉賣價，空單是開盤放空賣價。canonical
artifact 會另外保留連續訓練代理與整數
期貨執行結果；整數 holdings 表以 TX/MTX/TMF 記錄每日進出場口數。選擇權
22 元不屬於本策略商品集合，因此不進入
回測。滑價仍為零，正式解讀績效前應另做非零滑價敏感度測試。

整數執行本金固定為新台幣 1 億元。以近年的約略價格計算，一口 TMF 約只占
0.3%～0.5% 本金，可以執行模型常見的數個百分點曝險；不再出現 100 萬本金
下所有 3%～10% 目標都比半口 TMF 更小、最佳整數解永遠是零口的死區。

這條路徑目前只建立研究、訓練與整數執行預覽，不會送出 Shioaji 正式委託。
未來接實盤時，商品月份必須由 Contract V2 查詢具體契約，不得自行拼接代碼。
