# 股票橫截面驅動的台指期日盤策略

## 交易契約

這個策略把股票與期貨放在同一個可縮放的 cross-sectional market-token
bottleneck。模型輸入包含完整股票橫截面、所有有有效一般盤日資料的期貨根
之前一交易日前月 token，以及可執行的 TX／MTX／TMF E1..E6 token。模型輸出
是 18 個直接 signed capital fractions，總 gross 不超過 1；三種可執行商品的
乘數分別為每點新台幣 200、50、10 元。

預設時序如下：

1. 期貨交易日 `t` 的 08:45，只讀取截至股票交易日 `t-1` 收盤已完成的資料。
2. 以期貨日盤開盤價建立部位。
3. 同一日盤收盤前全部平倉，不承擔夜盤曝險。

模型不讀取交易日 `t` 的股票或期貨開盤、最高、最低、收盤或成交量。期貨
context row `t` 嚴格由 `t-1` 已完成 OHLCV、5/20 日統計與相對期限組成；當日
開收盤、當日可成交 mask 與具體月份只屬於 executor／label。

策略設定直接繼承 `tw_public_lanten_market_candles.yaml` 的股票資料、歷史
特徵、walk-forward、lookback、optimizer 與
`training.transformer_base_portfolio` 參數。唯一的資料特徵例外是
`next_session_open_gap_logret`：原 candles 策略可在股票開盤後使用它，但
台指期 08:45 決策時股票尚未開盤，因此子設定以
`data.day_trade_open_feature: false` 阻止它進入 feature schema。期貨專用模型
仍由原本 model factory 建立；所有股票與期貨先共同讀寫少量 market tokens，
再由最後 18 個已驗證 action token 產生直接部位。這避免對約 700 個期貨 token
做平方複雜度的 full self-attention。

## 資料

下載器只接受臺灣期貨交易所官方 `futDataDown` 收據，保留原始 ZIP/CSV、
SHA-256 與 manifest，然後：

- 只保留 `一般` 交易時段；
- index executor parquet 保留 TX、MTX、TMF 每日所有月契約 E1..E6；
- all-product input parquet 排除跨月價差，但保留每個契約根當日最近且有成交的
  月／週 outright futures；
- 產出 `data_tw_index_futures/day_session_contracts.parquet`，舊的
  `day_session_front_month.parquet` 保留但不再供此策略使用；
- 另產出 `all_products_day_session_front.parquet`、755 根的 inventory CSV 與
  content-addressed manifest。原始檔有 764 個根，其中 755 個至少一日具有完整
  正價 OHLC 與正成交量，因而能形成模型 token。

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

run_fintech_python scripts/build_tw_all_futures_day_context.py
```

## 訓練與評估

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/markets/tw_index_futures_day.yaml
```

canonical 訓練預設以新台幣 1 億元本金寫入
`artifacts/markets/tw_index_futures_day_joint_18_100m_exact_v7`。模型參數、輸入
token 軸與 backtest contract 都已改變，因此不得靜默續接舊 optimizer trajectory。
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

dual RTX 5090 設定使用完整 2,744 檔股票、729 個 all-root inputs、18 個
TX／MTX／TMF action tokens（期貨 token 合計 747）。依 power-of-two 實驗
契約，global train batch 固定為 512，也就是每個 rank 256；eval batch 128、
lookback 32、模型與回測 chunk 32／512 也都是 2 的冪。曾實測 global 736
穩態完整 epoch 為 0.984 秒、global 640 為 1.049 秒、global 512 為 1.930 秒，
因此 512 是形狀規則，不宣稱是最快的 batch。global 768 已在真實 train
hotpath 以約 31.33／31.36 GiB OOM；在相同模型下 batch 主導的 activation
記憶體隨 batch 單調增加，所以 1024 不是可行候選。epochs 1000、商品 18、
特徵 13、資料列數等是策略／資料語意，不為了外觀而改值。rank 1 的
sampled-test split 會直接共用已在 GPU 的 immutable train panel base，不再
重複配置約 4.07 GiB。新的 batch 會改變 optimizer trajectory，所以
power-of-two 設定寫入全新的
`tw_index_futures_day_joint_18_dual_5090_100m_exact_power2_v8` root，不續接
舊 v7 checkpoint。

精確期貨 ledger 不再把整個 batch 的權益 recurrence 展開成單一 fullgraph。
它以 32 個 session 為固定 CUDA block，跨 block 保持可微分的權益與存活狀態；
不足 32 列的尾端用不可交易零動作補齊後再切除。512 日 forward+backward
微基準中 eager 為 284.45 ms，固定 block 16／32／64 分別為 52.94／41.33／
25.69 ms；但 64 的首次 codegen 要 123.34 秒，128 超過六分鐘仍未完成。
考慮每個隔離 fold 都要各自暖機，以及 early stopping 的實際總 wall time，
32 日是總耗時最小的設定，不採用只看穩態 kernel 較快的 64／128 日設定。

這不是另一套 trainer。面板快取、walk-forward、lazy window、torch.compile、
optimizer、checkpoint/resume、`epoch_curve.jsonl`、early stopping、推論與
fold artifacts 全部沿用 `train.py` 的 canonical 路徑。正式策略使用基準設定
繼承而來的 `temporal_pooling: last` 與 `temporal_query_mode: last_only`；
train/eval batch size、epoch、learning rate、weight decay、seed 與
torch.compile 也全部繼承，不再維護另一組訓練超參數。唯一的 CUDA 執行
穩定性覆寫是 `sdpa_batch_limit: 16384`：它只切分 `batch * symbols` 的 SDPA
工作，不改變模型參數、token 或注意力計算語意。

訓練目標與測試都使用 18 槽的同一條權益路徑。loss forward 依 1 億元當期
權益把每槽目標向下取成整口，計入實際乘數、開收盤、雙邊期交稅、固定費與
滑價；反向傳播只對不可微的取整邊界使用 straight-through derivative，不另
造報酬代理。
目前交易人實付的總手續費按每口、每邊設定為 TX 60 元、MTX 24 元、TMF
16 元。這些優惠價視為已包含期交所交易／結算費；載入器只在內部拆出剩餘
券商部分，避免重複收費。每次進場與平倉各收一次；目前交易稅率設定為
`0.00002`，按每一筆期貨交易的契約金額計算，開倉與平倉都收。整數 holdings
表以具體 `root_contract_month` 記錄每日進出場口數。選擇權
22 元不屬於本策略商品集合，因此不進入
回測。滑價仍為零。

all-root 日檔沒有歷史乘數、個股期貨除權調整契約單位與逐商品券商費率，故
755 根目前全部是模型輸入，但不能偽裝成精確可交易標的。實際 action 僅限
擁有完整執行規格的 TX、MTX、TMF 18 槽；擴大成交 universe 前必須先補齊
point-in-time contract master。

整數執行本金固定為新台幣 1 億元。以近年的約略價格計算，一口 TMF 約只占
0.3%～0.5% 本金，可以執行模型常見的數個百分點曝險；不再出現 100 萬本金
下所有 3%～10% 目標都比半口 TMF 更小、最佳整數解永遠是零口的死區。

這條路徑目前只建立研究、訓練與整數執行預覽，不會送出 Shioaji 正式委託。
未來接實盤時，商品月份必須由 Contract V2 查詢具體契約，不得自行拼接代碼。
