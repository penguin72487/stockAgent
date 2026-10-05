# v8 OFAT 分析與續跑修正（2026-09-23）

續跑一致性已修正；它不能單獨解释泛化落差。後續分析把優先順序移到訓練與測試的
資金規模差異、整張梯度死區與多空方向，見
[訓練機制與方向消融](tw_day_trade_v8_training_mechanisms_2026-09-23.md)。
目前不宜將任何一個變體直接升為正式策略。原有實驗、checkpoint 與資料保持原樣。

## 比較範圍

來源：`artifacts/ablations/tw_day_trade_last_last_only_v8_twpublic_248d0869_ofat`。
2026-09-24 已再合併 `long_only`、`short_only`，目前主清單與根目錄比較圖共 14 組。
本報告下表保留 2026-09-23 的原 12 組分析快照；方向實驗與合併後指令見
[訓練機制診斷](tw_day_trade_v8_training_mechanisms_2026-09-23.md)。
12 個變體均通過共用 lifecycle 產物檢查，但目前每個變體都只有 fold 11：
訓練 2014–2024、驗證 2025、測試 2026-01-02 至 2026-09-11，共 168 個測試交易日。
這不是 11-fold 完成結果。所有變體使用 seed 42，沒有多 seed 的不確定性估計。

checkpoint 的 data、data_schema、trading、walk_forward fingerprints 全部一致。
資料 fingerprint 為 `ac8350cadb06fd769a9ef32e46f5da1ff6e15c673c6b041f8d47a2eef37f5122`。
共同契約包含 99 個輸入、TWD 10M、整張、實體 FIFO/minute 執行、原有費稅、
50% 普通參與率與 13:30 官方收盤價無容量上限的減倉假設；缺少早期分鐘資料時仍有
原實驗允許的 daily proxy。這些是研究契約，不能當成逐筆實際成交證明。

下表使用 `checkpoint_best.pt` 真正選中的 epoch，以及 `fold_11/metrics.json`。
測試欄是已看過的 fold test，不參與新的 checkpoint 選擇。

| 變體 | 選中 epoch | 驗證報酬 | 驗證 Sharpe | 驗證最大回撤 | fold 測試報酬 | fold 測試 Sharpe |
|---|---:|---:|---:|---:|---:|---:|
| baseline | 92 | 60.45% | 3.192 | -8.83% | 32.11% | 2.853 |
| center_long_short_scores | 637 | 70.62% | 2.960 | -7.76% | 16.84% | 1.246 |
| gelu_ffn | 89 | 1.88% | 0.274 | -11.08% | 2.10% | 0.370 |
| learned_cash_output | 181 | 5.43% | 2.562 | -1.17% | 1.89% | 1.420 |
| no_cross_asset_bottleneck | 68 | 22.47% | 0.498 | -23.43% | 54.46% | 1.429 |
| no_latent_factors | 14 | 26.64% | 1.927 | -7.39% | 13.11% | 1.263 |
| no_market_tokens | 22 | 25.19% | 1.902 | -7.83% | 2.57% | 0.263 |
| no_pretrained_initialization | 112 | 6.93% | 0.626 | -12.65% | 20.43% | 2.038 |
| no_qk_norm | 59 | 57.80% | 3.343 | -6.24% | 21.58% | 1.995 |
| no_temporal_basis | 189 | 79.61% | 3.480 | -7.48% | 40.22% | 3.212 |
| no_temporal_rope | 100 | 52.67% | 3.153 | -8.88% | 26.88% | 2.311 |
| no_time_position | 33 | 45.97% | 2.820 | -6.92% | 23.02% | 2.439 |

`learned_cash_output` 曲線的原始最低點是 epoch 197，但相對 epoch 181 的改善只有
約 0.0000053，小於原有 `early_stopping_min_delta=0.0001`；實際 checkpoint 是 181，
不是把曲線的最小值當成已保存模型。

部署重播與 fold test 必須分開。由 deployment NPZ 經共用 `compute_metrics` 重算，
baseline 部署報酬為 30.87%，no_temporal_basis 為 40.44%，learned_cash 為 1.91%。
這些檔案的 `strategy_returns` 是 **log returns**，必須以 `exp(sum(r))-1` 累積。
本次保留各自口徑，未把獨立 CPU 部署重播與 fold 評估的差異當成策略改善。

## 第一性原理：先確認模型函數沒有在續跑時改變

有效函數包含輸入 RMS buffer，不只有可訓練權重。若第一層接收 `x / s`，
載入 checkpoint 後把 `s` 換掉，即使權重和 Adam 狀態完全相同，也已改變函數。

同 feature ABI 的預訓練移轉會刻意保留來源的 RMS 與投影。此批來源 scale 前四項為
`[0.026668148, 0.023761783, 0.023062101, 0.023714038]`；目前訓練資料擬合值為
`[0.026218656, 0.023302851, 0.022572098, 0.023243647]`。
舊 resume 流程先載入 checkpoint，再呼叫 `_apply_causal_feature_rms_to_model`，
把來源尺度替換為目標尺度。最大絕對差為 `0.0004900023341178894`。

- `no_qk_norm` 有從 epoch 79、91 續跑的紀錄；最佳 epoch 59 保有來源尺度，
  最後 epoch 159 則已變成目標尺度。後半段曲線混入了非 QK 因素。
- `no_temporal_basis` 有從 epoch 3 續跑的紀錄，最佳及最後 checkpoint 都是目標尺度；
  baseline 保持來源尺度。結合移轉程式與續跑紀錄，不能把 79.61% 驗證報酬
  全部歸因於移除 basis。
- 沒有預訓練的變體本來就使用目標尺度；它與 baseline 的比較包含整套初始化差異。
  GELU、移除市場模組的可移轉張量也分別減少為 57、71 或 44 個，baseline 為 98 個。
  這是「現有移轉流程下的消融」，不是完全固定初始化的純架構因果試驗。

修正後 `_restore_resume_model_state` 保留 checkpoint 的全部模型 buffer 和移轉來源紀錄。
新增 `effective_causal_feature_rms_normalization.json`，分別記錄實際尺度、active mask，
以及與本次擬合值是否相同。合法來源尺度不同不應被偷偷修正。
這個修正不能回復已經被舊續跑流程改變的 optimizer 軌跡；因此新的對照使用新根目錄。

## 第一性原理：要求曝險、成交曝險與預測能力不同

以下統計來自已保存的 deployment requested weights，只作行為診斷。
有效檔數定義為 `(sum(abs(w)))² / sum(w²)`；空頭占比以全期要求 gross 加權。

| 變體 | 要求 gross 中位數 | 有效檔數中位數 | 空頭占要求 gross | 平均實際成交周轉 |
|---|---:|---:|---:|---:|
| baseline | 59.1% | 10.3 | 84.2% | 0.731 |
| learned_cash_output | 57.9% | 512.3 | 47.3% | 0.079 |
| center_long_short_scores | 72.8% | 7.1 | 98.7% | 0.975 |
| no_cross_asset_bottleneck | 95.4% | 1.0 | 90.2% | 1.319 |
| no_temporal_basis | 82.3% | 16.5 | 100.0% | 0.558 |

`learned_cash` 並非只要求很少風險；它把約 58% gross 分散得很細。TWD 10M 的
58% 平均分成 512 份，每份約 TWD 11,300；例如股價 TWD 50 的一張需要 TWD 50,000。
這與整張門檻造成碎片化的機制一致，但本次未逐筆區分整張、可交易性與容量各自的損失。
不得靠強制最低一張、提高資本、固定選股數或重新分配未成交額來美化這個實驗。
原 baseline 的 `score_entmax_cash` 已有可學習稀疏選擇與現金，先保留它作對照。

中心化 score 不保證稀疏非線性輸出後多空平衡；這次仍有 98.7% 的要求 gross 在空頭。
移除跨股票模組的高測試報酬則伴隨幾乎單一股票集中、較高周轉與驗證 23.43% 回撤，
不支持把它直接替換為新預設。no_temporal_basis 也幾乎是純空頭且 gross 變動很小，
其驗證優勢需要其他時間區間檢驗。

所有曲線都沒有記錄到零梯度 epoch 或死亡帳戶，不能用「訓練完全沒梯度」解釋差異。
多數變體訓練 loss 繼續下降、驗證 loss 已反轉，較像泛化或學習時程問題。

## 可立即重現的改進對照

baseline 在 epoch 192 早停時，LR 仍為峰值的 93.46%；no_qk_norm 在 epoch 159
仍為 95.85%。每個完整訓練軌跡才有一次 optimizer update，原 cosine horizon 是
1,000 次 update，patience 是 100 次驗證。多數 run 在充分衰減前已停止。
這不是 scheduler 實作錯誤，但值得測試更匹配的時程。

新增 `configs/ablations/tw_day_trade_v8_resume_corrected_20260923.yaml`，沿用既有 runner，
序列執行三個變體，各自保持雙 GPU DDP、global batch 32、1,000 epoch 上限與完整工作流程：

1. `baseline`：新的連續控制組，保留 22-family basis 與原 scheduler。
2. `no_temporal_basis`：只移除 basis，使用修正後的續跑邏輯。
3. `cosine_256`：只將 cosine horizon 改為 256 次 update，保留 32 次 warmup，
   之後維持原 `eta_min`。256 是待驗證的粗粒度候選，不是由 2026 測試最佳化的最佳值。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/run_ablation_experiments.py \
  --spec configs/ablations/tw_day_trade_v8_resume_corrected_20260923.yaml \
  --start-fold 11 --max-folds 1 \
  --multi-gpu-strategy distributed_data_parallel \
  --auto-resume --stop-on-fail
```

輸出為 `artifacts/ablations/tw_day_trade_v8_resume_corrected_20260923`，不覆寫舊根目錄。
已解析實際繼承設定並完成三變體 dry run；正式 1,000-epoch 實驗尚未啟動。
2025 驗證集已被這批變體與預訓練反覆使用，2026 測試也已檢視，因此後續比較屬探索性。
升級策略還需要其他 chronological folds、多 seed 及未參與選擇的後續資料。

## 工程驗證與可重現分析

分析工具不改來源，並對讀取的 manifest、checkpoint、曲線、NPZ 留存 SHA-256，
讀取前後再核對。原始數值與比較圖在 `artifacts/analysis/tw_day_trade_v8_ofat_20260923`。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_day_trade_ablation_results.py \
  --root artifacts/ablations/tw_day_trade_last_last_only_v8_twpublic_248d0869_ofat \
  --output-dir artifacts/analysis/tw_day_trade_v8_ofat_20260923
```

Focused tests：224 passed、3 skipped。新增的回歸測試刻意讓重新擬合尺度和 checkpoint
尺度不同，驗證只重載評估與繼續 AdamW 更新兩種情況，輸出、模型 buffer、權重及
optimizer state 均與未中斷對照逐值相同。嚴格 CUDA 檢查確認雙 RTX 5090 可用。
完整資料 DDP 的 bounded continuation 證據另存於
`artifacts/benchmarks/ofat_resume_correctness_20260923`；它只驗證工程一致性，不代表新策略報酬。
兩次 run 都完成 fold lifecycle：雙 RTX 5090、global batch 32、總計 2 epochs；
第二次從第一個 epoch 的 checkpoint 續跑。`comparison.json` 確認模型 buffer／權重、
AdamW state、scheduler、逐 epoch loss／梯度、完整 metrics 與三個 NPZ 的 105 個陣列
完全一致，來源實驗的 SHA-256 核對也全部通過。兩次均未使用正式 1,000-epoch 根目錄。
端到端對照涵蓋仍有訓練 epoch 的 resume；已達 epoch 上限、只補報表時的
compile/cache 精度路徑尚未完成相同驗收，不能將本次結果延伸為所有報表重建路徑的逐位一致性。
