# stockAgent

台股量化研究與訓練工作區，包含資料下載、特徵 parquet 管理、walk-forward 訓練與回測輸出。

## 專案架構總覽

```text
stockAgent/
├── stockagent/                 # 核心套件
│   ├── config.py               # YAML 設定載入與預設值合併
│   ├── data/
│   │   ├── panel.py            # 讀取 parquet，建立 features/returns/mask panel
│   │   └── walkforward.py      # 年度 expanding-window fold 切分
│   ├── models/
│   │   ├── base.py             # 抽象模型介面與權重正規化
│   │   ├── factory.py          # 模型工廠（由 config 選擇）
│   │   ├── mlp.py              # CrossSectionalMLP（只看當日特徵）
│   │   ├── portfolio_transformer.py # Portfolio Transformer 類模型
│   │   └── temporal_cross_asset.py # 時間編碼 + 跨資產編碼模型
│   ├── training/
│   │   ├── dataset.py          # 訓練資料集與 collate
│   │   ├── loss.py             # Sharpe-aware loss
│   │   ├── batch_optimizer.py  # 自動 batch size 與顯存策略
│   │   └── trainer.py          # 訓練主流程、checkpoint、評估與輸出
│   ├── backtest/
│   │   ├── simulator.py        # 權重回測、換手與報酬計算
│   │   ├── portfolio.py        # 投組輔助邏輯
│   │   └── report.py           # 指標與報表圖產出
│   └── evaluation/
│       └── metrics.py          # IC 與統計指標
├── configs/
│   ├── experiment_baseline.yaml # 主要實驗設定
│   └── models/                  # 模型專屬超參數設定
├── train.py                    # 訓練入口
├── download_yahoo_tw_ohlcv.py  # 資料下載入口
├── data_parquet/               # 特徵資料與下載報表
├── artifacts/                  # 各 fold checkpoint、metrics、回測輸出
└── docs/                       # 規格、分析與優化文件
```

## 系統資料流（由上到下）

1. `download_yahoo_tw_ohlcv.py` 下載並補齊台股資料到 `data_parquet/`
2. `train.py` 載入 `configs/experiment_baseline.yaml`
3. `stockagent.data.panel.build_panel` 建立共同 panel（features, returns, tradable_mask）
4. `stockagent.data.walkforward.build_expanding_year_folds` 產生 walk-forward folds
5. `stockagent.training.trainer.run_training` 執行訓練與驗證
6. `stockagent.backtest.simulator.run_backtest*` 產出回測與報表
7. 結果輸出至 `artifacts/fold_XX/` 與 `artifacts/summary.json`

## 模組責任對照

| 模組 | 主要責任 | 重要輸入 | 主要輸出 |
|---|---|---|---|
| `stockagent/config.py` | 載入與標準化實驗設定 | YAML 檔案 | `ExperimentConfig` |
| `stockagent/data/panel.py` | parquet 轉為密集張量與遮罩 | `data_parquet/*.parquet` | `PanelData` |
| `stockagent/data/walkforward.py` | 年度擴張式資料切分 | `dates` | `WalkForwardFold[]` |
| `stockagent/models/factory.py` | 依設定建立模型 | model name + model params | 可訓練模型實體 |
| `stockagent/models/mlp.py` | 橫截面權重/分數預測（不看 lookback） | `[B, L, S, F]` 特徵與 mask | 每日 symbol 權重 |
| `stockagent/models/portfolio_transformer.py` | Portfolio Transformer 類直出權重模型 | `[B, L, S, F]` 特徵與 mask | 每日 symbol 權重 |
| `stockagent/models/temporal_cross_asset.py` | 時間+跨資產編碼 | `[B, L, S, F]` 特徵與 mask | 每日 symbol 權重 |
| `stockagent/training/trainer.py` | 訓練、checkpoint、驗證、測試 | `PanelData`, folds, config | fold metrics, checkpoints |
| `stockagent/backtest/simulator.py` | 報酬、換手、交易成本模擬 | weights, returns, tradable_mask | `BacktestResult` |
| `stockagent/backtest/report.py` | 圖表與年度報告輸出 | 回測結果 | 圖檔、`annual_report.txt` |

## README 維護規範（持續更新）

以下情況發生時，請同步更新此 README：

- 新增或移除根目錄腳本（例如新訓練入口、資料處理工具）
- `stockagent/` 下新增、改名或刪除子模組
- `configs/experiment_baseline.yaml` 的關鍵欄位或預設策略變更
- `artifacts/` 輸出格式改變（新檔案或欄位）
- 訓練流程順序或回測假設調整（例如 fee、mask、benchmark 規則）

建議每次 PR 使用這份快速檢查清單：

- [ ] 專案架構總覽是否反映目前目錄
- [ ] 系統資料流是否仍符合實際程式邏輯
- [ ] 執行指令範例是否仍可直接執行
- [ ] 常見輸出位置與檔名是否正確

README 最後更新日：2026-05-22

## 模型架構（詳細）

目前模型由 `stockagent/models/factory.py` 依 `configs/experiment_*.yaml` 的 `model` 區塊決定。

- `mlp`：`CrossSectionalMLP`，只使用最新一天（當日）特徵做橫截面決策。
- `portfolio_transformer`：先做每檔股票時間 Transformer，再做跨資產 Transformer + decoder attention，直接輸出投組權重。
- `temporal_cross_asset`：先做單一股票時序編碼，再做跨資產注意力編碼。

### 輸入與輸出

- 輸入 `x` 形狀：`[B, lookback, S, F]`
- 輸入 `tradable_mask` 形狀：`[B, S]`
- 輸出 `weights` 形狀：`[B, S]`

其中：

- `B` = batch 內日期樣本數
- `lookback` = 回看天數
- `S` = 當日全市場 symbols 數量
- `F` = 特徵數（由 panel builder 固定欄位）

### 模型路徑

1. 特徵嵌入層（Feature Embedding）
- 先用 `Linear(num_features -> embedding_dim)` 壓縮每檔股票特徵。

2. 時序編碼路徑（依 lookback 自動切換）
- `lookback = 1`：不走 Transformer，直接進 MLP head。
- `lookback > 1`：使用 HuggingFace Transformer（BERT config）處理時序，取最後 timestep hidden state。

3. 投組打分頭（Portfolio Head）
- 由 `hidden_layers` 控制深度，結尾輸出每檔股票一個 logit。

4. 權重轉換
- `long_only = true`：對 logits 做 masked softmax，並重新正規化。
- `long_only = false`：先中心化 logits，再經 `tanh` 轉為有號分數，最後用 gross exposure 正規化。

### Mask 與可交易約束

- 任何 `tradable_mask=False` 的標的，權重會在 forward 與 backtest 兩端都被抑制。
- long-only 下，最終有效標的權重和會被規範為 1。
- 若有現金 symbol，訓練 loss 與交易成本計算都可用 `cash_symbol_mask` 排除。

## 資料建模與 Panel 結構

Panel 由 `stockagent/data/panel.py` 建立，核心輸出：

- `features[T, S, F]`
- `returns_1d[T, S]`
- `tradable_mask[T, S]`
- `alive_mask[T, S]`
- `benchmark_returns[T]`
- `open_prices[T, S]`
- `close_prices[T, S]`

### 特徵與報酬定義（目前實作）

- 特徵欄位：`open`, `max`, `min`, `close`, `adjclose`, `Trading_Volume`, `next_open`
- 訊號時間對齊：多數價量欄位使用前一日值（shift 1）
- 策略目標：同日調整後 `open -> close` 對數報酬（log return）
- 基準報酬：調整後 close-to-close 對數報酬，最終以當日可用標的等權平均

### 可交易規則

某日某檔 `tradable=True` 條件：

- `open_raw > 0`
- `close_raw > 0`
- `Trading_Volume > 0`
- 報酬絕對值未超過風險閾值（極端值會被過濾）

## 訓練流程（詳細）

訓練入口：`train.py` → `run_training(...)`

### Step 1: 設定與裝置初始化

- 載入 `configs/experiment_baseline.yaml`
- 強制檢查 CUDA 可用性（若 config 指定 cuda 但實機不可用會直接中止）
- 依 `amp_dtype` 決定混合精度策略（`tf32` / `bf16` / `fp16`）

### Step 2: 建立 Walk-forward Folds

`stockagent/data/walkforward.py` 採 expanding window：

- train: `years[:i]`
- val: `years[i]`（固定一年）
- test: `years[i+1:]`

保證每個 fold 至少有 train/val/test。

### Step 3: 建立資料集與 batch 策略

- `CrossSectionalDataset` 會自動濾除不滿足 lookback 的日期。
- 若 fold 資料長度不足，會直接拋錯避免無效訓練。
- 可啟用 `auto_batch_size`：trainer 會依顯存預算做 batch size 搜尋與估算。

### Step 4: 每個 epoch 的訓練與驗證

1. 訓練階段
- 使用 `sharpe_aware_loss` 作為主要目標（Sharpe 最大化 + turnover 懲罰）

2. 驗證階段
- 在驗證集先跑回測（tensor path）
- 以驗證 loss 更新 `checkpoint_best.pt`
- 同步寫入 `checkpoint_last.pt`

3. 群組續訓
- 同一 train_years 群組會另外保存 group checkpoint
- `--resume` 可讓中斷後從既有 checkpoint 接續

### Step 5: 測試與落地輸出

- 測試集先跑 tensor backtest 取得權重與信號品質（IC）
- 再跑 `run_backtest_integer_shares` 模擬整股交易（含買賣費率）
- 每 fold 落地 metrics、報表、圖表、持倉明細

## Loss 與最佳化目標

`stockagent/training/loss.py` 的 `sharpe_aware_loss`：

- 先以 `tradable_mask` 作用在權重上
- 以 gross exposure 做正規化
- 日報酬 = `weights * future_log_returns` 橫截面加總
- 成本 = 買入 turnover × buy_fee + 賣出 turnover × sell_fee
- Sharpe 以年化係數 `sqrt(252)` 處理

最終目標：

`loss = -gamma_sharpe * sharpe + gamma_turnover * turnover_cost`

## 驗證與測試說明

### 驗證（Validation）

- 以 val split 監控 loss、IC、Sharpe、累積報酬等指標
- 以最佳 val loss 保存最佳模型

### 測試（Test）

- 測試指標完全 out-of-sample（各 fold 的 test years）
- 回測交易假設（整股版）目前為：
	- 初始資金：1,000,000
	- 買入費率：`buy_fee_rate`
	- 賣出費率：`sell_fee_rate`
	- 單位：每手 1000 股
	- 同日開盤買入、收盤平倉

### 目前可直接執行的測試腳本

- `python test_mlp_simple.py`：檢查 MLP 前向與輸出形狀
- `python test_transformer_simple.py`：檢查 Transformer 基礎運作
- `python test_single_fold.py`：跑單一 fold 的端到端訓練驗證

## 圖表與報表（輸出解讀）

每個 fold（`artifacts/fold_XX/`）常見輸出：

- `equity_curve.png`
	- 策略 vs 基準 累積淨值曲線（線性 Y 軸）
- `equity_curve_log.png`
	- 策略 vs 基準 累積淨值曲線（對數 Y 軸）
- `annual_performance.png`
	- 年度報酬（柱狀）與年度 Sharpe（折線）
- `annual_report.txt`
	- 年度績效文字表（含 TOTAL 匯總）
- `holdings.csv`
	- 每日持倉明細、成交名目、費用

walk-forward 匯總（`artifacts/` 根目錄）：

- `summary.json`
	- 每 fold 的 train/val/test 年份、best val loss、metrics
- `walkforward_equity_curve_log.png`
	- 全 folds 拼接後的策略/基準對數淨值曲線
- `walkforward_first_year_cumulative_returns.png`
	- 各 fold 首個 test 年的累積淨值比較

## 指標定義（回測）

由 `stockagent/backtest/report.py` 計算：

- `cumulative_return`：`exp(sum(log_returns)) - 1`
- `annualized_return`：以 252 交易日年化
- `sharpe`、`baseline_sharpe`
- `max_drawdown`（log-space 計算，較數值穩定）
- `turnover`（平均每日換手）
- `daily_hit_rate`（日報酬為正比例）
- `excess_return_vs_universe_average`
- `cumulative_benchmark`

## 建議的 README 例行更新節奏

為了讓 README 持續維持「可直接操作」與「可追蹤決策」：

1. 每次調整模型結構（層數、激活、輸出規則）就更新「模型架構」章節
2. 每次調整訓練超參數欄位就更新「訓練流程」與範例指令
3. 每次新增/移除輸出圖表就更新「圖表與報表」章節
4. 每次改動交易成本或回測規則就更新「驗證與測試說明」
5. 每次合併 PR，更新最後更新日與變更摘要（1 到 3 行）

可在 README 最底部維護小型更新紀錄，例如：

- 2026-05-22: 補齊模型、訓練、驗證測試、圖表章節與輸出對照

## 專案重點

- 主要訓練設定在 [configs/experiment_baseline.yaml](configs/experiment_baseline.yaml)
- 訓練入口在 [train.py](train.py)
- Yahoo Finance 資料下載工具在 [download_yahoo_tw_ohlcv.py](download_yahoo_tw_ohlcv.py)
- 套件清單在 [requirements.txt](requirements.txt)
- 訓練規格文件在 [docs/training_spec.md](docs/training_spec.md)

## 環境準備

1. 建議使用 conda/mamba 環境 `fintech`
2. 安裝依賴：

```bash
pip install -r requirements.txt
```

3. 如需 GPU 訓練，請確認 `torch.cuda.is_available()` 為 `True`

## 1) 抓取 Yahoo Finance 台股資料

最基本下載（2000-01-01 到今天，寫入 `data_parquet`）：

```bash
python download_yahoo_tw_ohlcv.py --output-dir data_parquet
```

常用參數範例：

```bash
# 調整平行下載數
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --workers 16

# 只抓少量股票驗證流程
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --symbols 2330 2317 0050

# 強制重新下載
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --refresh

# 只做缺漏日期補齊
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --fill-only
```

資料輸出格式：

- 每檔一個 parquet，例如 `2330_features.parquet`
- 欄位：`date`, `open`, `max`, `min`, `close`, `adjclose`, `Trading_Volume`
- 預設輸出報表：`download_report.csv`, `download_summary.json`, `fill_report.csv`

## 2) 啟動訓練 (train)

最基本訓練：

```bash
python train.py --config configs/experiment_baseline.yaml --output-dir artifacts
```

不從既有 checkpoint 續訓：

```bash
python train.py --config configs/experiment_baseline.yaml --output-dir artifacts --no-resume
```

也可以用專案腳本啟動：

```bash
./coda_runner.sh
```

注意事項：

- `configs/experiment_baseline.yaml` 目前預設 `environment.device: cuda`
- 若目前資料都在 `data_parquet/`，請確認 `configs/experiment_baseline.yaml` 的 `data.parquet_root` 設為 `data_parquet`
- 若機器沒有可用 CUDA，請先改設定或換到有 GPU 的環境
- 訓練完成後會在 `artifacts/` 產生各 fold 結果與 `summary.json`

## 3) 建議執行流程

```bash
# 1. 抓資料
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --workers 16

# 2. 開始訓練
python train.py --config configs/experiment_baseline.yaml --output-dir artifacts --no-resume
```

## 4) 常見輸出位置

- 資料 parquet: `data_parquet/`
- 訓練輸出: `artifacts/fold_XX/`
- 訓練彙總: `artifacts/summary.json`
