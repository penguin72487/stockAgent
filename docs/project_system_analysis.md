# stockAgent Project System Analysis

更新日期: 2026-06-09

這份文件整理目前專案的資料結構、資料流、訓練與評估演算法、
plot backend 分工、可解釋性路徑，以及後續優化原則。目標是避免
重複計算、重複儲存，並讓長 lookback 與 `transformer_base_portfolio`
維持友善。

## 1. 核心資料結構

### PanelData

來源: `stockagent/data/panel.py`

`PanelData` 是全市場時間序列的 canonical layout:

- `dates`: `[T]`
- `symbols`: `S`
- `feature_names`: `F`
- `features`: `[T, S, F]`
- `returns_1d`: `[T, S]`
- `tradable_mask`: `[T, S]`
- `can_buy_mask`: `[T, S]`
- `can_sell_mask`: `[T, S]`
- `alive_mask`: `[T, S]`
- `benchmark_returns`: `[T]`
- `close_prices`: `[T, S]`

這個 layout 對長 lookback 友善，因為 window 可以用 date index
直接 gather 出 `[B, L, S, F]`，不需要把所有 sliding windows 永久展開。

### Panel Cache v2

來源: `stockagent/data/panel_cache.py`

目前已使用 `panel_cache_v2/*.npy` memmap cache:

- 避免每次從多個 parquet 重新 concat/pivot。
- 避免 legacy `.npz` 一次載入和解壓所有大陣列。
- 使用 `source_hash + backend_key + version + mtime` 驗證。

優化原則:

- feature schema 改變時必須 bump cache version 或改 source hash 相關邏輯。
- 不要再建立另一份展開後的 `[N, L, S, F]` 永久 cache 作為預設。

### WindowedSplitTensors

來源: `stockagent/training/windowed.py`

`WindowedSplitTensors` 保存 base tensors:

- `features`: `[T, S, F]`
- `valid_indices`: `[N]`
- target/masks/benchmark: canonical `[T, ...]`

每個 batch 用 index 生成:

```text
row_indices -> valid date indices -> window date indices -> features[window_idx]
```

這是目前最重要的長 lookback 友善設計。它避免 materialize
`[N, L, S, F]`，尤其 `L=32` 或更長時差異很大。

2026-06-09 實作更新:

- 連續日期 fold 會走 `unfold + narrow` fast path，避免每個 batch 重建
  window index matrix 和 target/mask advanced indexing。
- 非連續 `valid_indices` 仍保留原本 advanced indexing path。
- GPU cached base tensors 下的 micro-benchmark:
  - lookback8: fast path 約 1.11x。
  - lookback32: fast path 約 1.19x。

## 2. 資料流

### 建 panel

```text
parquet per symbol
-> load by pandas/polars/cuDF path
-> feature normalization and tradable masks
-> concat/pivot to PanelData
-> panel_cache_v2 memmap
```

目前 `panel_backend=auto` 會優先使用 cuDF when `use_rapids=true`，
否則使用 polars/pandas。注意 cuDF path 最後仍會轉成 pandas frame
去建 `PanelData`，所以它是 build-time 加速，不代表訓練 tensor 永遠留在 GPU。

### Walk-forward

```text
PanelData.dates
-> expanding-year folds
-> train/val/test date indices
-> CrossSectionalDataset
-> WindowedSplitTensors
```

### 訓練

```text
WindowedSplitTensors.batch_by_rows
-> [B, L, S, F] x + masks + returns
-> BF16 AMP model forward
-> portfolio weights
-> canonical tensor backtest/loss
-> optimizer
```

重點:

- `materialize_window_tensors: false` 應該是長 lookback 預設。
- `cache_train_tensors_on_gpu` / `cache_eval_tensors_on_gpu` 只 cache base tensors，
  不 cache 展開 windows。
- portfolio state 跨 batch/chunk 用 detached GPU clone，不搬 CPU。

### 驗證、test curve、final inference

```text
model forward
-> canonical run_backtest_torch
-> metrics reduction on GPU
-> final tensors/CSV/PNG artifacts
```

訓練、驗證、推論應共用相同 tensor backtest 報酬算法。

## 3. transformer_base_portfolio 演算法

來源: `stockagent/models/transformer_base_portfolio.py`

輸入:

```text
[B, L, S, F]
```

主幹:

```text
feature projection
-> temporal attention per stock
-> attention_mode-specific cross-asset mixing
-> score head
-> tanh/softmax-like portfolio normalization path
```

### attention_mode

- `full`: `O((L*S)^2)`，最完整，只適合小 universe 或 debug。
- `axial`: `O(S*L^2 + L*S^2)`，時間和股票分解。
- `latent`: `O(S*L^2 + S*K + K*M + S*(K+M))`，大型 universe 預設友善。
- `market_token`: `O(S*L^2 + S*M)`，更小的 cross-stock bottleneck。
- `temporal_only`: 只看個股時間序列，不做 cross-stock mixing。

目前 modern transformer 元件:

- RMSNorm
- Pre-Norm residual blocks
- SwiGLU FFN
- QK-Norm
- temporal RoPE
- PyTorch SDPA/FlashAttention path
- dynamic latent/market token generator

2026-06-09 熱路徑更新:

- Attention projection 改為 packed `in_proj`:
  - self-attention 用一次 GEMM 產生 QKV。
  - cross-attention 用同一組權重切成 Q 與 KV，減少獨立 projection overhead。
- Temporal RoPE cos/sin 依 lookback/head dim 預先 cache，不在每個 forward 重算。
- `temporal_pooling: last` 在 training/light aux path 使用 last-query fast path：
  最後一層 temporal block 只計算最後一天 query 對完整 lookback context 的 attention；
  `return_aux=True` 時仍保留完整 `[B,L,S,D]` token embedding 以供解釋性檢查。
- `torch.compile(model)` 在 TW full universe、lookback32、batch16 實測約 1.9x batch hotpath speedup。
- `compile_loss: true` 即使移除 Python timing graph break 後仍比 eager loss 慢，因此目前維持 `compile_loss: false`。

長 lookback 友善原則:

- 優先 `latent` 或 `market_token`。
- 保留 `sdpa_batch_limit`，避免 `B*S` 太大造成 CUDA invalid argument。
- `return_aux_details=false` 用於訓練，explainability 才開細節。
- full attention 必須受 `max_full_tokens` 保護。
- 對 `S≈2304` 的 TW full universe，lookback32 建議從
  `batch_size_train: 16` 起跑；batch32 會明顯增加 VRAM 並降低吞吐。
- `temporal_pooling: last` 是目前 active user preference。它比 attention pooling 稍快，
  但依賴 temporal blocks 把歷史資訊帶到最後一天 token；改回 attention/mean 前應重新 benchmark。

## 3.1 2026-06-09 Throughput Benchmark Summary

測試環境: RTX 4070 Ti SUPER 16GB，`data_yahoo/tw_stocks`，`S=2304`，
`loss_type=log_utility`，canonical tensor backtest，BF16 AMP。

代表性結果:

| Case | lookback | batch | compile model | compile loss | pooling | s/batch |
| --- | ---: | ---: | --- | --- | --- | ---: |
| eager | 32 | 16 | false | false | attention | 0.181 |
| eager | 32 | 16 | false | false | mean | 0.175 |
| eager, before last fast path | 32 | 16 | false | false | last | 0.176 |
| eager, last fast path | 32 | 16 | false | false | last | 0.151 |
| eager | 32 | 32 | false | false | attention | 1.072 |
| compiled model | 32 | 16 | true | false | mean | 0.091 |
| compiled model, last fast path | 32 | 16 | true | false | last | 0.085 |
| compiled model+loss | 32 | 16 | true | true | mean | 1.131 |

決策:

- 預設 lookback32 使用 batch16，而不是 batch32。
- 保留 model compile。
- 保留 loss eager。
- 保留 market-token bottleneck；`temporal_only` 更快但沒有跨股票 market token。

## 4. 避免重複計算與重複儲存

已經做好的:

- Panel cache v2 使用 memmap。
- Windowed tensor path 避免 materialize all windows。
- eval metrics online reduction，避免只為 summary 多跑 CPU metrics。
- final backtest 與 train loss 使用 canonical tensor backtest。

仍需注意:

- `CrossSectionalDataset` 會為每個 split 包裝同一份 panel tensor，
  但 base tensor 仍是 from numpy view，主要重複成本在 object wrapper，
  不是資料本體。
- explainability 的 perturbation 是 `L * F` 次 forward，成本高但可控。
  若 L 或 F 大幅增加，應改成分批 feature perturbation 或只跑 top candidates。
- UMAP projection 不應每個 epoch 跑，只在 fold 結束或手動 `explain_model.py` 跑。
- Plotly/SHAP 不應放在訓練主 loop。

## 5. Plot Backend 分工

### PyQtGraph

用途:

- live scalar stream
- loss / Sharpe / Sortino / equity curve monitor

不適合:

- headless fold artifact generation
- 大型 heatmap raster

建議做法:

- 未來可以新增獨立 monitor process 讀 `epoch_curve.jsonl`。
- trainer 不要直接開 GUI event loop。

### Datashader + cuDF + CuPy

用途:

- feature-time attribution heatmap
- aux dimension dense line/profile
- cuML UMAP embedding/token projection scatter
- 大量交易點、股票 embedding、latent/market token 分布
- final tensor equity curve 若資料仍在 CUDA

已實作:

- explainability dense plots 支援 Datashader。
- aux tensor 降維使用 cuML UMAP。
- UMAP projection CSV: `aux_projections/*.csv`
- UMAP projection PNG: `plots/aux_umap/*.png`

### Plotly

用途:

- 互動式 model analysis dashboard
- hover 查 symbol/date/token
- attention heatmap exploration

不建議預設放進訓練 artifact:

- `plotly` 目前不是環境依賴。
- HTML artifact 體積會比 PNG 大。
- headless training 沒有互動需求。

### SHAP

用途:

- tree model 或小 batch 的 model-agnostic analysis。

目前不預設使用:

- `shap` 未安裝。
- 多股票、多天 lookback 的 tensor model 做 SHAP 成本高。
- 目前 gradient / integrated gradients / perturbation 更 tensor-friendly。

## 6. Explainability 目前輸出

`python explain_model.py` 預設會畫完整 explainability set。

核心輸出:

- `feature_time_gradient.csv`
- `feature_importance_gradient.csv`
- `time_importance_gradient.csv`
- `feature_time_integrated_gradients.csv`
- `feature_importance_integrated_gradients.csv`
- `feature_time_perturbation.csv`
- `feature_importance_perturbation.csv`
- `feature_correlations.csv`
- `top_decisions.csv`
- `stock_contributions.csv`
- `aux_summary.csv`
- `aux_dims/*.csv`
- `aux_projections/*.csv`
- `report.md`
- `summary.json`

### cuML UMAP aux projections

UMAP candidates:

- `stock_embedding`
- `z_stock`
- `latent_factors`
- `market_tokens`
- `dynamic_latent_queries`
- `dynamic_market_queries`
- `dynamic_latent_delta`
- `dynamic_market_delta`
- `z_factor_context`
- `z_market_context`
- `token_embedding`

UMAP 解讀:

- collapsed cloud: token/embedding 可能沒有學到區分。
- date-only banding: 可能主要學 market regime/time，而非股票差異。
- symbol-only islands: 可能學到固定股票身份或價格/流動性 proxy。
- latent/market token 重疊: token bottleneck 可能沒有分工。
- dynamic delta cloud 很小: dynamic token gate 或 delta 可能太弱。

## 7. 推薦預設

長 lookback 32 和完整 universe:

```yaml
training:
  materialize_window_tensors: false
  model_name: transformer_base_portfolio
  lookback: 32
  plot_backend: auto
  explain_umap_enabled: true
  transformer_base_portfolio:
    attention_mode: latent   # or market_token for tighter VRAM
    use_flash_attention: true
    sdpa_batch_limit: 4096
    return_aux_details: false
```

做 explainability 時:

```yaml
training:
  explain_after_each_fold: true
  explain_first_test_year_only: true
  explain_umap_enabled: true
  explain_umap_max_points: 10000
  plot_backend: auto
  transformer_base_portfolio:
    return_aux_details: true
```

如果 VRAM 緊:

- 降低 `d_model`
- 降低 `num_latent_factors`
- 使用 `attention_mode: market_token`
- 降低 `explain_umap_max_points`
- 保持 `return_aux_details: false` during training

## 8. 下一步優化清單

高優先:

- 用 `epoch_curve.jsonl` 持續檢查 `epoch_wall_s`，不是只看 train step。
- 將 live monitor 獨立成 PyQtGraph process，不進 trainer main loop。
- 若需要互動式分析，再新增 Plotly dashboard 讀 explainability CSV。
- 若 feature schema 變更，強制更新 panel cache version。

中優先:

- perturbation attribution 支援分批 feature/time slice forward。
- aux UMAP 加上 token/date/symbol hover HTML dashboard。
- 對 `feature_correlations` 增加 raw price/liquidity suspicious feature groups。

低優先:

- SHAP 僅針對 tree models 或小型 ablation subset。
- Full attention 模式只做小 universe sanity check。
