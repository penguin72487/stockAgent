# TW Public 模型可解釋性操作與讀圖指南

## 第一性原理：到底要解釋什麼

一個交易模型的可解釋性至少要回答三個不同問題：

1. 輸入層：哪些特徵、哪些 lookback 日期改變了模型決策？
2. 組合層：模型把訊號轉成哪些多空部位，曝險是否過度集中？
3. 結果層：哪些股票與市場狀態造成獲利、虧損、換手與風險？

本專案不產生 Top-K／Top-N 解釋圖或截斷案例表。離線歸因使用每個抽樣日所有可交易、非零部位，並按 gross exposure 加權；完整的日期 × 股票清冊寫入 `decision_inventory.csv`。高維內容以完整矩陣、分布與累積覆蓋曲線呈現，避免把少數排名項目誤當成完整模型規則。

## 執行方式

先完成訓練：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_public.yaml
```

訓練完成後，對所有具有 `checkpoint_best.pt` 的 fold 產生 paper-grade 可解釋性：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python explain_model.py --config configs/markets/tw_public.yaml
```

執行時預設顯示 fold、日期 chunk、Integrated Gradients、feature-time perturbation、UMAP、跨資產 attention／shock／source 與 graph 階段的進度、速度及 ETA。非互動批次環境可加 `--no-progress`；計時資料仍會寫入每個 fold 的 `explainability_runner_timing.json` 與 `explainability_timing.json`。

`tw_public.yaml` 目前的 artifact root 是 `artifacts/markets/tw_public_lantent`。`explain_model.py` 會覆蓋 YAML 中為訓練期間節流而關閉的 explain 選項：預設分析所有可用 fold、所有 configured test years、所有有效日期、完整股票清冊、完整特徵集合與完整 32 日 lookback，並執行 IG、perturbation、SHAP、UMAP、regime、fold stability 與跨資產模組。

`--max-rows 0` 是預設值，以下命令與不加該參數相同：

```bash
run_fintech_python explain_model.py \
  --config configs/markets/tw_public.yaml \
  --max-rows 0
```

梯度、Integrated Gradients、逐 feature-time perturbation 與跨資產擾動的成本會隨日期、股票與特徵數增加。程式使用日期、IG／perturbation 與跨資產 source 分塊控制 VRAM；分塊不會減少輸出範圍。若使用正數 `--max-rows`，那是使用者明確要求的縮小執行，不再稱為完整報告。

```bash
run_fintech_python explain_model.py \
  --config configs/markets/tw_public.yaml \
  --fold 18 \
  --max-rows 0
```

若只想快速做完整範圍稽核，不做昂貴的 IG、perturbation、UMAP 與跨資產擾動：

```bash
run_fintech_python explain_model.py \
  --config configs/markets/tw_public.yaml \
  --fold 18 \
  --max-rows 0 \
  --ig-steps 0 \
  --no-perturb \
  --no-shap \
  --no-umap \
  --no-cross-asset
```

這個快速模式仍會保留全部部位清冊、gradient × input、特徵／日期矩陣、曝險曲線與完整性稽核。

## 先看完整性，再談模型規則

每個 fold 的輸出位於：

```text
artifacts/markets/tw_public_lantent/explainability/fold_XX_test/
```

第一個應檢查的檔案是：

```text
paper_tables/explainability_completeness.csv
```

判讀標準：

- `decision_inventory_rows == expected_decision_inventory_rows`：每個抽樣日 × 每檔股票都有紀錄。
- `position_count_coverage == 1`：所有可交易非零部位都進入歸因目標。
- `gross_exposure_coverage == 1`：歸因涵蓋 100% gross exposure。
- `gradient_feature_time_cells == expected_feature_time_cells`：gradient 熱圖包含全部 lookback × feature cells。
- IG 或 perturbation 有啟用時，對應 cell 數也應等於 expected cells。
- `sampled_date_coverage` 是日期覆蓋率；完整預設應為 `1`。只有明確傳入正數 `--max-rows` 時才可能小於 `1`。

若這一關不通過，不應用後面的圖做模型層級結論。

## 建議讀圖順序

### 1. `portfolio_exposure_coverage_curve.png`

位置：`plots_paper/`

- X 軸是由大到小納入的可交易股票比例。
- Y 軸是累積 gross exposure。
- 曲線越靠近左上角，表示越少股票承擔越多曝險。
- 若 5% 股票就承擔 80% 曝險，模型其實非常集中；此時少數案例圖不能代表整體穩定性。

### 2. `feature_attribution_coverage_curve.png`

- X 軸是由重要到不重要納入的全部特徵比例。
- Y 軸是累積 attribution share。
- 快速上升表示少數特徵主導；平緩表示訊號較分散。
- 少數特徵主導不一定錯，但若主導者是價格水準、原始成交量或資料可得性代理，需懷疑 shortcut 或資料洩漏。

完整數值在 `paper_tables/global_feature_attribution.csv`，不是只有圖上的前幾項。

### 3. 三張 feature-time heatmap

- `feature_time_gradient_grad_x_input_abs_heatmap.png`：局部敏感度，最快但可能受局部梯度影響。
- `feature_time_integrated_gradients_integrated_gradients_abs_heatmap.png`：從零 baseline 積分到真實輸入，通常比單點梯度平滑。
- `feature_time_perturbation_weight_abs_delta_heatmap.png`：把某個 feature-day 歸零後，最終部位改變多少，最接近交易行為。

`t-0` 是決策前最新輸入日。若只有一欄發亮，模型可能根本沒有有效使用 32 日歷史；若三種方法在完全不同區域發亮，代表規則不穩定。

### 4. `time_importance_gradient.png`

檢查 32 個 lookback 日的總歸因。健康的 temporal model 通常會使用一段時間形狀；若單日占比極端高，需確認這是策略設計還是模型退化成單日規則。

### 5. `feature_correlations_shortcut_checks.png`

這是原始特徵與 score／weight 的簡單相關，不是因果關係。高相關是 shortcut 篩檢器：特別注意原始價格、流動性、成交量與缺值型特徵。

### 6. `trust_checks.png`

依序處理 mask leakage、單名集中、換手、單一特徵／日期主導與 aux collapse。任何 mask leakage 都比報酬表現更優先處理。

### 7. `regime_analysis.png` 與 `decision_case_studies.png`

前者檢查上漲、下跌、高低波動環境是否只有單一 regime 有效；後者完整呈現所選案例日的所有股票部位。逐列數值可回到 `decision_inventory.csv` 稽核。

### 8. `aux_token_diagnostics.png` 與 UMAP

檢查 latent factors、market tokens、stock embeddings 是否接近全零、單一維度主導或所有點塌縮。UMAP 只能用來觀察群聚與 regime 結構，不能把 2D 距離直接解讀成因果關係。

## 跨 fold 判斷

模型層級結論必須看所有 fold。使用：

```text
explainability/fold_stability/
```

若同一特徵只在單一 fold 極端重要，或重要性排序每年劇烈翻轉，較可能是 regime-specific 或過擬合，不能直接宣稱為穩定交易規則。

## 工程與投資解讀要分開

- 工程事實：覆蓋率、歸因 cell 數、mask 是否洩漏、計算是否降級、SHAP surrogate R²。
- 投資解讀：訊號是否合理、集中是否可接受、不同 regime 是否符合策略假設。

可解釋性可以找出異常與建立信任證據，但不能單獨證明未來報酬或因果關係。
