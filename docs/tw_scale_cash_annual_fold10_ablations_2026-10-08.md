# 年度重置、完整 feature、無基底：fold10 消融選單

> 本頁是先前 v2 的 **73 項候選清單與當時驗證紀錄**，不再是目前排程。
> 使用者已只選 27 個變體，最新 v5 為 lookback 位移年度、每項只跑最後一個 fold，順序與驗證請看
> [本輪選定實驗](tw_scale_cash_annual_fold10_selected_2026-10-08.md)。
> 下方舊命令中的 `baseline` 等未選 ID 不受新版入口接受；不要照舊示例開跑。
> 最新入口無參數會前景執行整輪；只看清單請明確加 `--list`。

## 1. 執行進度與驗證邊界

2026-10-08 已在 vastai1T 準備 **73 個可選配置：1 個基準 + 72 個變體**。
沒有啟動正式訓練，也沒有修改原年度訓練設定、optimizer 或 frozen code。
沿用 training-reuse 的 canonical 消融編排、訓練 lifecycle、checkpoint/resume；
沿用 storage-operations 核對固定資料與遠端輸出位置。

已完成：

- 本機相關測試 **85 passed**：既有 OFAT scheduler、年度 fold 契約、新選擇入口與等價輸出去重。
- 遠端全部 73 配置的 canonical dry-run，退出碼 **0**。
- 用遠端**實際 frozen config loader / model factory**，全部 73 個模型按
  `F=14726 / S=2330` 在 meta device 建構成功；不配置真實 feature tensor。
- 檢查所有配置仍是 fold10、雙卡 DDP、resume；來源、成交規則、walk-forward
  分割與基準一致；基底/SVD、稀疏事件、pretrained 初始化均不開啟。
- 每個變體都真的改變解析後的設定；沒有重複設定或只改名字的假消融。

**未驗證：**各變體實際 CUDA forward/backward、完整雙卡 epoch/fold、顯存峰值、
數值誤差、吞吐量與報酬。Meta 建構及 dry-run 不等於訓練成功；原基準的速度／
VRAM 驗收不能套用到新架構。d128、lookback64、batch512/1024 等尤其需實跑。

遠端驗證收據：
`artifacts/operations/training_launches/tw_scale_cash_annual_fold10_ablations_20261008_v2/readiness.json`。
本機副本只保存小型驗證證據，不複製訓練資料集。
第一輪 v1 的75個配置曾通過建構；詳細語義檢查後排除兩個等價輸出，另立 v2，
不覆寫 v1 契約或任何 optimizer。v1 準備收據與生成配置仍保留，沒有正式訓練。

## 2. 精確基準

使用者選定入口：

```bash
cd /root/stockAgent
bash scripts/run_tw_no_basis_scale_separated_cash_annual_vastai1t.sh
```

消融從這個入口的**遠端已固定 launch config** 衍生，不從 moving latest
抓資料、不沿用舊 v8 小特徵清單、不混入這次新合併的分鐘來源。

| 項目 | 固定基準 |
| --- | --- |
| fold | 只跑 fold10；每次 train.py 都明確帶 `--start-fold 10 --max-folds 1` |
| 年份 | 2014 僅上下文；train 2015–2024；val 2025；test 2026 的既有資料範圍 |
| 輸入 | 14,726 個值通道：6 基礎行情 + 14,655 個別 + 65 共同 feature |
| 年度重置 | 開啟；只有 `annual_reset__off` 明確取消 |
| embedding / lookback | 32 / 32 |
| 時間模型 | 2 layers / 4 heads / FFN×4 / last-only query / last pooling |
| 跨股票模型 | latent factors 16 + market tokens 16；各 1 layer / 4 heads |
| 規範化與 FFN | causal feature RMS + LayerNorm + QK norm + SwiGLU |
| 位置 | 時間位置編碼 + temporal RoPE；不開股票位置 embedding |
| 輸出 | `score_entmax_scale_separated_cash`、long/short |
| 其他輸入壓縮 | 沒有 temporal basis、feature SVD、feature bottleneck |
| precision / devices | BF16 AMP，保留原 FP32 islands / 精確金融 ledger；GPU 0,1 DDP |
| batch | global 256 = 每卡 128；eval batch 16 |
| optimizer 相關 | lr 1e-4 / weight decay .01 / seed 42 / epochs 上限 1000，保留原 scheduler 與 early stopping |
| 成交 | 原實體 FIFO；容量 50%；09:01 初次成交、entry remainder **first_minute_only** |
| 尾盤 | 原本 13:20 / 13:24 路徑，最終 13:30 限定減倉、不限容量；有來源價才成交 |
| 日 K 代理 | 保留原授權：官方 open/close、不加不利 tick，容量公式不變 |
| 報表 | 保留原 train/val/test、epoch curves、plots、checkpoints、fold lifecycle；另產生 val/test 消融比較圖 |

注意：YAML 的 `attention_mode: market_token` 在兩個 enable 開關都為 true 時，
模型實際解析成 **latent（兩路皆開）**。不能只看 preset 名字就宣稱沒有 latent。
這裡的 latent tokens 也不是輸入特徵基底／SVD 分解。

資料的來源授權、歷史發布／修訂時間與既有 research-only 定位原樣保留；本次配置
驗證不會把研究代理升格成可實盤歷史 PIT 證據或券商成交證據。

固定 source SHA：
`44fdf2a5092306ec4a8702bb1f63bc38a578bfaff58642a1a34d1c5beb97d378`。
使用 `tw_scale_cash_stitched_report_fix_20261007` frozen source；維持遠端既有
report-only amendment gate，不用本機較舊的控制腳本覆寫它。

## 3. 已準備的全部選項

每個 ID 都獨立從基準修改，不是依次疊加。移除機制、替換算子、參數掃描、
backward／年度契約及效能比較屬不同類型，不把它們的結論混在一起。
標為配套／組合的選項不宣稱只有一個 YAML key 改變。

### A. 機制移除與架構替換

| 實驗 ID | 改變 |
| --- | --- |
| `baseline` | 原設定，另存於消融根，不續接正式 baseline optimizer |
| `cross_context__no_latent` | 移除 latent factors，保留 market tokens |
| `cross_context__no_market` | 移除 market tokens，保留 latent factors |
| `cross_context__temporal_only` | 同時移除兩路跨股票資訊；組合消融 |
| `time_position__off` | 移除時間位置編碼，保留 RoPE |
| `rope__off` | 移除 RoPE，保留時間位置編碼 |
| `qk_norm__off` | 移除 Q/K normalization |
| `input_rms__off` | 移除 training-owned 特徵 RMS 尺度校正 |
| `norm__rmsnorm` | LayerNorm → RMSNorm |
| `ffn__gelu` | SwiGLU → GELU |
| `temporal_query__full_then_last` | last-only → full-then-last |
| `pooling__mean` | full-then-last + mean pooling；配套架構修改 |
| `pooling__attention` | full-then-last + learned attention pooling；配套架構修改 |
| `temporal_layers__zero` | 移除時間 Transformer blocks，其餘路徑保留 |
| `head_depth__linear` | score head 的 gated hidden layer → 純 linear output |

### B. 模型容量與上下文掃描

| 實驗 ID | 選項與基準 |
| --- | --- |
| `embedding__d16`, `embedding__d64`, `embedding__d128` | d_model 16 / 64 / 128，基準 32；不做分解 |
| `temporal_layers__one`, `temporal_layers__three` | 1 / 3 層，基準 2 |
| `temporal_heads__h2`, `temporal_heads__h8` | 時間 heads 2 / 8，基準 4 |
| `cross_heads__h2`, `cross_heads__h8` | 跨股票 heads 2 / 8，基準 4 |
| `latent_count__n4`, `latent_count__n8`, `latent_count__n32`, `latent_count__n64` | latent tokens 4 / 8 / 32 / 64，基準 16 |
| `market_count__n4`, `market_count__n8`, `market_count__n32`, `market_count__n64` | market tokens 4 / 8 / 32 / 64，基準 16 |
| `latent_depth__two` | latent layer 1 → 2 |
| `market_depth__two` | market layer 1 → 2；也影響 stock-read blocks 的既有配套深度 |
| `temporal_ffn__m2`, `temporal_ffn__m8` | 時間 FFN expansion 2 / 8，基準 4 |
| `cross_ffn__m2`, `cross_ffn__m8` | 跨股票 FFN expansion 2 / 8，基準 4 |
| `head_depth__two` | score head hidden layers 1 → 2 |
| `head_width__d16`, `head_width__d64` | head hidden width 16 / 64，基準 32 |
| `lookback__d8`, `lookback__d16`, `lookback__d64` | 8 / 16 / 64 交易日上下文，基準 32；不是同張量算子比較 |

### C. 輸出與現金配置

只改模型 output mode，保留 `pre_normalized` executor 及原成交／資金規則。

| 實驗 ID | output mode |
| --- | --- |
| `output__log_cash` | `score_entmax_log_cash` |
| `output__score_cash` | `score_entmax_cash` |
| `output__score_cash_v2` | `score_entmax_cash_v2` |
| `output__global_cash` | `score_entmax_global_cash` |
| `output__bounded_cash` | `score_entmax_bounded_cash` |
| `output__learned_cash` | `learned_cash` |
| `output__cash_l1` | `cash_l1` |
| `output__l1` | `l1` |
| `output__projection_l1` | `projection_l1` |
| `output__signed_softmax` | `signed_softmax` |
| `output__signed_sparsemax` | `signed_sparsemax` |
| `output__signed_entmax` | `signed_entmax15` |

這些模式的曝險／現金表達不相同；沒有 cash slot 的模式不能解讀成只換了排序演算法。
`logits` 是未解決權重，不能直接配基準的 pre-normalized executor，所以沒有列為現成變體。
已排除：`activation_l1` 在 pre-normalized／identity activation 下等價於 `l1`；
`cash_entmax15` 在本基準 FP32 portfolio output 下等價於 `score_entmax_cash`。
`score_entmax_cash_v2` 則保留其零分數梯度契約差異，不把 forward 一樣誤認為 backward 一樣。

### D. 正則化、學習與穩健性

| 實驗 ID | 改變與類型 |
| --- | --- |
| `dropout__p10`, `dropout__p20` | dropout .1 / .2，基準 0 |
| `learning_rate__low`, `learning_rate__high` | lr 3e-5 / 3e-4，基準 1e-4 |
| `weight_decay__zero`, `weight_decay__high` | weight decay 0 / .1，基準 .01 |
| `seed__s7`, `seed__s123` | seed 7 / 123，基準 42；隨機穩健性，不是移除機制 |
| `annual_reset__off` | **只有此項關年度重置**；其餘皆保持開啟，train 年份不變 |
| `sub_lot_gradient__on` | 開零股恢復梯度；forward 的整股成交不變，backward ABI 改變 |

### E. 吞吐量與數值實驗

| 實驗 ID | 改變與限制 |
| --- | --- |
| `batch__b128`, `batch__b512`, `batch__b1024` | global batch 128 / 512 / 1024；梯度分段改變，不能宣稱等價加速 |
| `encoder_checkpoint__off` | 不重算 encoder activation，以 VRAM 換運算；forward 契約不變 |
| `compile__off` | 關 canonical model compile，獨立 input/ledger kernels 原樣保留，**不是全程 eager** |
| `temporal_fp32__off` | 時間 blocks 放入 BF16 autocast；保留其他 FP32 islands、金融 ledger 精度 |
| `portfolio_fp32__off` | portfolio blocks 放入 BF16 autocast；保留其他 FP32 islands、金融 ledger 精度 |

效能結論要按相同硬體／資料／source 測完整啟動、train/val/test、曲線、plot、checkpoint
及收尾；分開冷 compile 與 epoch3+ 最大 rank 穩態耗時。保留報酬、成交與數值差異。
不能因 meta 建構成功就宣稱 batch1024 可用，或因 kernel 更快就宣稱整個 fold 更快。

## 4. 其他可設計，但尚未作成直接可跑配置的實驗

參數可任意擴展，組合數也無上限；上面是目前有代碼、已驗證配置與 ABI 的離散選單，
不是聲稱所有可能的數學組合都已測完。以下須你先選擇，再準備相應資料／配套契約。

| 類型 | 候選 | 尚需準備／為什麼不是改一個 YAML 就好 |
| --- | --- | --- |
| 資料資訊消融 | 只行情、移除65共同 feature、移除個別財務、法人／籌碼、宏觀／海外、事件；leave-one-provider-out | 用現有 factorized builder/projection 做版本化 manifest，保留同一 calendar／universe／成交 tape。`feature_include` 的6欄只驗基礎 ABI，不能拿它當14,726通道的選擇器 |
| 發布頻率／品質 | 財報、營收、每週籌碼、月／季宏觀；真實缺值 vs 原本因果 carry；嚴格 PIT 子集 | 固定原發布時鐘、修訂及 carry 契約；原觀測不得補造。現在不加 availability/age/update 通道 |
| 聯合架構 | no-position + no-RoPE、no-RMS + RMSNorm、latent×market 開關的交互作用、時間深度×寬度 | 明確標為 factorial／配套組合，不從 OFAT 結果推斷不存在交互作用；每組新 artifact ID |
| 資料與方向 | 只股票/ETF、long-only/short-only、原始值 vs 因果比例／報酬量 | 模型方向、股票 universe 與 executor 掩碼必須同時一致；不能只改一個顯示名稱 |
| 成交／成本敏感性 | 逐分鐘續單、尾盤受容量限制、容量25/75%、資本額、費用／稅／融資融券成本、排除日K代理 | 屬**市場環境假設實驗**，不是模型消融；修改 tape/cache/fingerprint，必要時取消 flat-terminal fast path。原基準保持50%與不限容量尾盤不變 |
| loss / optimizer | Sharpe 等 loss、gradient clip、scheduler、warmup、learning-rate schedule、其他 optimizer、full-year optimizer cadence | 維持同一精確 forward accounting；目前 canonical loss/optimizer 的支援及專用模式條件需逐項核對；另存 optimizer trajectory |
| precision / backend | 全 FP32 參考、TF32 on/off、更多 BF16 islands、SDPA/manual/Flash、NVFP4、activation memory/cache/stream 預算 | source/env 配套及 headroom、誤差、全流程吞吐實測；NVFP4 不是改 `amp_dtype` 字串，compact FP32 input 契約不能偷偷取消 |
| 模型家族／全股票 attention | axial、full cross-stock Transformer、MLP/TCN 對照 | full attention 有 token/VRAM guard；須另核對支持14,726通道的 compact input 路徑，不默認配 global256 |
| 輸入分解 | SVD／temporal basis／bottleneck | **本次不準備也不開啟**；若另選研究，須新 ABI，不能直接接 compact no-basis 初始化或既有 optimizer |
| 重複試驗 | 同一機制多 seeds；以2025挑選、2026只作保留測試 | 可新增同基準/變體×seed配置；不能用2026 test決定哪個模型最好後再稱無偏測試 |

## 5. vastai1T 前景操作

列出全部 ID（不會訓練）：

```bash
cd /root/stockAgent
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh --list
```

只驗證你選的配置，仍不訓練：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh \
  --dry-run --only baseline,cross_context__no_latent,cross_context__no_market
```

**以下會訓練，是使用方式示例，不是已替你啟動。**例如你選上面三個：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh \
  --only baseline,cross_context__no_latent,cross_context__no_market
```

只選你決定的 ID；各 case 按 YAML 順序依次執行，一次佔兩張卡、沒有背景 detach。
未指定 `--only` 就顯示選單；未知 ID／空 ID 會拒絕，不會默默全跑。
只支援此專用 fold10 入口，不接受把範圍改成全部 folds 的 passthrough。

可先從基準 + 兩路跨股票移除 + full-then-last + GELU + log-cash 比較開始。
這是精簡研究候選，不是速度或投資績效的預測，實際執行組合由你決定。

## 6. 隔離、resume、輸出與比較

- 訓練輸出統一在遠端
  `artifacts/markets/tw_scale_cash_annual_fold10_ablations_20261008_v2/<ID>/`。
- 生成配置在同根 `generated_configs/<ID>.yaml`；操作收據在 `artifacts/operations/`；
  重用原 `data_preparation`、`cache`、`code_releases`，不另建巨型 panel。
- 各 ID 首次是新的 optimizer；**同一 ID 再次執行預設 canonical resume**，
  不從 baseline 或別的變體偷接 optimizer，不重訓已完成的 fold。
- Ctrl+C 後再次執行相同指令，依 canonical 持久化 checkpoint 恢復；不承諾未提交
  的當前 batch 都已保存，不自動改 seed／batch／資料來「成功續跑」。
- 明確關 profiling/debug sync。失敗停止本輪，保留 checkpoint，修復後重跑原選擇。
- baseline readiness gate 只驗原 source/data；新 output/data/backward 的 checkpoint
  相容性由 canonical trainer 擁有。source/catalog/data 身分不符會拒絕覆寫契約。
- 專用入口續租原精確來源，用既有 node-global GPU lease；不停止／搶占其他訓練。
- 先用2025 validation比較相同範圍的報酬、drawdown、turnover、現金／gross exposure、
  成交／容量、穩健性與總耗時；2026 test 是 out-of-sample diagnostic，不用於 scheduler
  或反覆挑最佳變體。優勝者再做多 seed，而不是把一次高報酬直接當機制有效。
- canonical fold 報表與曲線照常生成。跨實驗比較圖需有消融 `baseline/summary.json`；
  如果只跑變體尚無 baseline，聚合圖會明確 deferred，不偽造參考曲線。

配置來源：[OFAT YAML](../configs/ablations/tw_scale_cash_annual_fold10_20261008_v2.yaml)。
橋接入口：[Python](../scripts/run_tw_scale_cash_annual_ablations.py)、
[shell](../scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh)。
測試：[測試案例](../test/test_tw_scale_cash_annual_ablations.py)。
