# 最後一個 fold：使用者選定的 27 項消融

## 1. 執行進度與驗證邊界

2026-10-08 最新數值修復：依使用者要求，新版 **27 項全部重跑**，預設入口已切到
`artifacts/markets/tw_scale_cash_annual_last_fold_selected_20261008_v6`。embedding16
採 head-only SDPA 雙軸對齊，保留 Inductor fullgraph，**不採 AOT eager**。
真實雙卡 3 epoch 與 canonical resume 均通過，全 27 項配置／calendar 已驗收；
正式整輪尚未啟動。命令、數值邊界與證據見
[小型 head 梯度修復報告](tw_scale_cash_narrow_head_gradient_repair_2026-10-08.md)。
以下 v5 產圖與 171 檔不變的敘述是當時 report-only 驗收紀錄，不代表其後數值診斷
期間整個目錄未變動；本次仍驗證前兩個成功消融的 124 檔均未變，原模型保留。

2026-10-08 產圖修正已部署並實際驗證：遠端已完成的
`pooling__attention/fold_11`、`lookback__d256/fold_10` 已補 **26 張彙總 PNG**。
每項新實驗完成後立即同步刷新；續跑時先統一補已完成項目的圖，再接未完成的
訓練。這次沒有啟動正式訓練，沒有重算前兩項；**171 個既有訓練檔案**的 SHA256
全部不變，包含 27 份配置、checkpoint、結果、完成標記與根目錄 summary。
舊 `artifacts/ablations/tw_day_trade_v8_direction_20260923` 僅作樣式參考，未改動。

產圖／排程／配置與舊版報表回歸 **82 passed（70.60s）**，Ruff、shell 語法及
diff whitespace 檢查通過。遠端整輪 `--dry-run`、實際 `--plots-only` 均退出 **0**；
26 張 PNG 均通過影像解碼、尺寸與來源數值查核，並目視檢查 Sharpe、熱圖、
風險報酬與權益曲線。不同實際期間分組，沒有將缺對照的項目填成零，也不畫
單一 fold 的假信賴區間。這是產圖驗收，**不是 27 項全數訓練完成**。

遠端 frozen 報表版本的重點工程測試 **6 passed（13.32s）**：逐項同步刷新、
完成項目只補圖不重跑、read-only plots-only、不同 fold／日期分組、缺完成證據
拒絕、三面實際 PNG 及快取／缺檔恢復。首次遠端工程 fixture 依賴未打包的
通用配置，已改為自足 CPU fixture；正式配置不改。此 fixture 修正另於本機
重測 **2 passed（4.08s）**，不冒充 GPU 訓練或實際投資績效。

本機可直接檢視遠端實際圖的驗證副本：
[Sharpe](../artifacts/operations/training_launches/tw_scale_cash_annual_last_fold_selected_20261008_v5/reporting-v1-review/test_absolute_sharpe_by_variant.png)、
[風險報酬](../artifacts/operations/training_launches/tw_scale_cash_annual_last_fold_selected_20261008_v5/reporting-v1-review/test_risk_return_sharpe_medians.png)、
[權益曲線](../artifacts/operations/training_launches/tw_scale_cash_annual_last_fold_selected_20261008_v5/reporting-v1-review/test_equity_by_variant.png)、
[熱圖](../artifacts/operations/training_launches/tw_scale_cash_annual_last_fold_selected_20261008_v5/reporting-v1-review/test_fold_sharpe_heatmap.png)。

2026-10-08 最新更正：使用者說的 fold10 是指 **2026 實際測試期間**，不是固定
編號。全部 27 項仍依各自 lookback 位移年度，現在只跑**各項最後一個 canonical
fold**：26 項 lookback32 跑 fold11；lookback256 跑 fold10。使用獨立 **v5** root；
無參數啟動依序跑整輪，重啟接續未完成項目。不執行其他候選、不增加第 28 個
baseline。前一輪僅準備及 dry-run，沒有替使用者啟動 v5 正式實驗；本輪接續補圖。

本次排程／清單／模型建構測試 **65 passed (23.60s)**；walk-forward、年度設定、
checkpoint／resume／lifecycle 回歸 **216 passed (9.71s)**。新增測試確認 argv、
完成判定、重試與 summary 使用同一個 per-case fold；舊 fold10 marker 不能把
fold11 誤判成已完成，仍保留共用工具的明確 CLI override。Ruff、shell 語法與
diff whitespace 檢查通過。遠端 27 項 canonical dry-run、最後 fold 與實際年度
ownership 驗證全數通過，退出碼 **0**；每項 test 都是 2026 實際交易日期，
沒有缺少 owned training targets。資料 manifest SHA 未變，沒有重建 panel。

前一輪 v4 曾完成 **333 passed / 4 skipped** 與遠端 strict CUDA 檢查；
兩張 RTX 5090 的位移年度 full-batch loss／梯度
測試各 **1 passed (7.95s)**。v5 與 v4 的 trainer、carry bridge、checkpoint contract
逐檔 SHA 相同，本次只改共用排程。該歷史 DDP 證據不冒充本次重測，也不是
27 個正式模型的完整 epoch、compile 速度或 VRAM 驗收；尤其 embedding64/128
仍待正式實跑。lookback256 本輪已讀到使用者完成的真實結果及完整 fold receipt。

證據位於遠端 `artifacts/operations/training_launches/`
`tw_scale_cash_annual_last_fold_selected_20261008_v5/`：
`source-release.json`、`calendar-context.json`、
`readiness.json`。程式 release 為
`artifacts/code_releases/tw_scale_cash_last_fold_20261008/`
`20261007T182554884687Z-c3edd343c330/release.json`，source SHA256：
`c3edd343c330a7a64cbf2db7a7d6653b71724c3a303ac5fb9ba97a051b87edcc`。

沿用 training-reuse 的 canonical scheduler、DDP、checkpoint/resume、FIFO loss
和單一 full-panel 年度契約；storage-operations 用原 accepted release 建立新
wheel／source bundle：v4 已有的三個金融核心檔案保持原值，只新增 shared scheduler
的最後-fold 選擇修正。舊 release、v1/v2/v3/v4 root、資料與 optimizer 均保留。
遠端已有 v4 的 `pooling__attention/fold_10` 與 run manifest；不抹除、不改名，
也不將它們續接到不同年度 ownership 的 fold11。原曆年模式的
checkpoint boundary ABI 不變；位移模式另有 `observed_session_shifted_fresh_capital_v1`。

先前 v3 的 lookback256 阻擋是真實的「完整曆年」上下文不足。本次不是補造
缺值，而是依使用者明確授權改採位移年度；第一段以前的 rows 只供特徵上下文。
完整特徵 panel 仍從 `2014-01-06` 開始，沒有聲稱重建 2013 年完整特徵。

## 2. 執行順序

每項獨立從**共同的位移年度設定**修改，不把前三組互相疊加。例如 embedding128
仍用 lookback32、last-only/last pooling；lookback256 仍用 embedding32。

| 順序 | ID | 修改 |
| --- | --- | --- |
| 1 | `pooling__attention` | full-then-last + attention pooling |
| 2 | `lookback__d256` | lookback256；按自身 256 日邊界跨年取上下文 |
| 3 | `embedding__d16` | embedding16 |
| 4 | `embedding__d64` | embedding64 |
| 5 | `embedding__d128` | embedding128 |
| 6 | `cross_context__no_latent` | 移除 latent，保留 market tokens |
| 7 | `cross_context__no_market` | 移除 market tokens，保留 latent |
| 8 | `time_position__off` | 移除時間位置 |
| 9 | `rope__off` | 移除 RoPE |
| 10 | `qk_norm__off` | 移除 QK norm |
| 11 | `input_rms__off` | 移除特徵 RMS |
| 12 | `norm__rmsnorm` | LayerNorm → RMSNorm |
| 13 | `ffn__gelu` | SwiGLU → GELU |
| 14 | `output__log_cash` | score_entmax_log_cash |
| 15 | `output__score_cash` | score_entmax_cash |
| 16 | `output__score_cash_v2` | score_entmax_cash_v2 |
| 17 | `output__global_cash` | score_entmax_global_cash |
| 18 | `output__bounded_cash` | score_entmax_bounded_cash |
| 19 | `output__learned_cash` | learned_cash |
| 20 | `output__cash_l1` | cash_l1 |
| 21 | `output__l1` | l1 |
| 22 | `output__projection_l1` | projection_l1 |
| 23 | `output__signed_softmax` | signed_softmax |
| 24 | `output__signed_sparsemax` | signed_sparsemax |
| 25 | `output__signed_entmax` | signed_entmax15 |
| 26 | `annual_reset__off` | 基準年度重置已開；此對照關閉訓練重置 |
| 27 | `sub_lot_gradient__on` | 開啟零股恢復梯度；精確 forward/eval 不變 |

同時移除 latent+market、單獨 full-then-last、mean pooling、其他 lookback、
其他層數／heads／token 數、seed、lr、batch、compile、precision 等均不排入本輪。
`cash_entmax15` 和 `activation_l1` 是已核對的輸出別名，不重複算成獨立實驗。

## 3. 保留的設定與基準比較

年度邊界定義為 `B(Y,L) = 該年首個觀測交易日的 row index + L`；期間 Y 擁有
`[B(Y,L), B(Y+1,L))`。這裡的 L 是觀測交易日數，不是曆日。窗口可以跨年，
切分後不再扣第二次 lookback。只有完整日曆中確實存在的邊界才會建立，不憑空
補下一年度交易日。訓練重置與 fold 切分使用相同 boundary contract。

只跑**各項最後一個 fold**；年度標籤不是原曆年。
截至固定 panel 的 `2026-10-02`，實際樣本如下：

| 組別 | 最後 fold | train | validation | test |
| --- | --- | --- | --- | --- |
| 26 項 lookback32 | 11 | 2014-02-27～2025-02-25（2,685 日） | 2025-02-26～2026-02-25（243 日） | 2026-02-26～2026-10-02（149 日） |
| lookback256 | 10 | 2015-01-19～2025-01-21（2,443 日） | 2025-01-22～2026-01-20（242 日） | 2026-01-21～2026-10-02（168 日） |

fold1 的起點分別是 2014 首觀測交易日 +32／+256 日，後續 fold 增加一個年度
標籤；本輪只選最後一個。32 日模式的最後年度標籤為 train 2014–2024、val2025、
test2026；256 日模式為 train 2014–2023、val2024、test2025，但**實際 test 在2026**。
共用排程從固定 manifest 日曆交給 canonical fold builder 選最後一個，沒有
自行複製 split 算法。不能硬寫相同 fold 編號來冒充相同樣本。

完整 **14,726 個值通道**、無基底／SVD／bottleneck、無稀疏事件、無 pretrained
初始化；雙卡 DDP、BF16 與原 FP32 islands，global batch256（128/rank），
原 execution/FIFO、50% 容量、first-minute-only entry、尾盤不限容量規則不改。
保留所有 train/val/test、epoch curves、plots、checkpoints 與 canonical resume；
不開 profiling，也不 detach 到背景。失敗停止，修好後重跑同一選擇。

**不沿用舊曆年 fold10 的報酬當對照。**目前沒有同期間、同位移契約的正式
baseline；依使用者只跑 27 項的決定，不額外訓練 baseline。每項自己的 train／
val／test／epoch 曲線／plots／checkpoints 全保留。新增 absolute 產圖路徑，
**不再讓缺 baseline 擋住所有彙總圖**；Sharpe、熱圖、風險報酬、權益圖正常更新。
只有 baseline-paired 效果圖仍明確 unavailable，不複製舊指標或把第一個變體當
baseline。日後取得匹配對照才可畫 paired 圖。
lookback256 和其他 26 項連樣本期間也不同，直接比總報酬不能歸因為純 lookback
效應。不能反覆用 test 挑模型再稱無偏驗證。

所有變體輸出：
`artifacts/markets/tw_scale_cash_annual_last_fold_selected_20261008_v5/<ID>/`。
新根不建立舊 `baseline/` 參考；
`generated_configs/` 保存實際固定來源的配置。
操作／calendar 收據在 `artifacts/operations/training_launches/` 同名目錄。

### 增量報表與版本邊界

| 前綴 | 計算來源 | 目前 PNG 數 |
| --- | --- | --- |
| `val` | 保存的 validation 指標＋accepted calendar receipt 的真實區間 | 8 |
| `test` | canonical deployment ledger 重新計算指標、保存的 net-return 曲線 | 9 |
| `full_horizon_integer_audit` | 保存的完整 fold test 指標與曲線，明確標示 diagnostic | 9 |

圖族沿用舊版名稱：`*_absolute_sharpe_by_variant.png`、`*_fold_sharpe_heatmap.png`、
`*_risk_return_{cagr,sharpe,sortino,turnover,daily_hit_rate}_medians.png`、
`*_risk_return_medians.png`；兩個 test 面另有 `*_equity_by_variant.png`。
`medians` 僅保留相容檔名，這輪只有每變體一個 fold，圖上明確寫 observed fold、
真實日期與 session 數，不混不同期間取中位數／排名。原始低頻資料、訓練與成交
假設均未修改；權益曲線使用現有 canonical plot helper，不引入另一套複利公式。

每面另輸出 `*_fold_metrics.csv`、`*_absolute_sharpe_summary.csv` 與
`*_plot_status.json`（`absolute_ablation_charts_v1`）；CI 欄位留空，收據記錄來源
summary／returns／日期 SHA、各比較群及完成項目。每張 PNG 原子更新，狀態收據
最後發布。相同來源與 PNG／CSV 已存在時重用，不在每次 resume 重畫所有圖；
缺檔會重建。尚未完成的 fold 不冒充結果。

報表／排程使用獨立 frozen release：
`artifacts/code_releases/tw_scale_cash_incremental_reports_20261008/`
`20261008T042138726296Z-f7954bf29761/release.json`，source SHA256：
`f7954bf29761d90abdfbe929b88422f93991c55ac32571925a7dfa0afee2ef50`。
相對原 v5 只改 `scripts/run_ablation_experiments.py` 與
`scripts/plot_ablation_analysis.py`；金融核心、訓練／模型／optimizer／資料程式逐檔
不變。產圖固定 `Agg`，不再等待桌面 GUI 事件迴圈；新程式拒絕額外 core 改動。

真正的訓練 worker 仍由原 `c3edd343c330...` receipt 與 frozen `coda_runner.sh`
執行；不將 report source 當成 optimizer source。原 v5 catalog、
`ablation-contract.json`、`input-contract.json` 不改；只在 operational spec 覆蓋
報表描述，全部 27 份正式 training config 已驗證 byte-identical。
獨立 `reporting-source-v1.json`／`report-contract-v1.json` 保留報表版本，
`reporting-v1-before.json`／`reporting-v1-readiness.json` 保留實測與檔案保護證據。

每個新成功 fold 通過完成判定後同步刷新三面報表；產圖失敗會明確停止，不宣稱
「實驗完整完成但其實沒圖」。重新執行時先刷新已完成結果再啟動下一個 worker。
原本 paired 模式是共用工具的預設，舊消融流程保持相容。

## 4. 遠端前景指令

在 vastai1T 無參數即可從第一順位開始依序跑完 27 項；這是使用指令，並非已
由 agent 啟動正式實驗：

```bash
cd /root/stockAgent
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh
```

僅列出本輪 27 項，不會訓練：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh --list
```

檢查整輪配置與真實年初上下文，不會訓練；本次已測得退出碼 0：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh --dry-run
```

只跑某個已選項目時：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh --only pooling__attention
```

`--only` 的 CSV 順序不影響排程；按上表／YAML 順序執行。未知或未選的 ID
直接拒絕。沒有參數等同 `--run-selected`，不是只列清單。停止後再跑同一條
無參數指令，會依 canonical 持久化 checkpoint resume，驗證後跳過已完成的
變體。新契約不續接舊 v4／原基準 optimizer；只恢復同一 v5 下相容的 checkpoint。

只補／更新已完成項目的圖，**不訓練、不改 summary／progress／checkpoint**：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh --plots-only
```

此模式讀既有 accepted full-queue calendar receipt，不重新生成正式配置；沒有
calendar 驗證時先使用 `--dry-run`。一般無參數 resume 仍是原本的前景指令，
自動逐項更新圖表，不需另開背景產圖程序。

配置：[選定 v5 YAML](../configs/ablations/tw_scale_cash_annual_last_fold_selected_20261008_v5.yaml)。
入口：[Python](../scripts/run_tw_scale_cash_annual_ablations.py)、
[shell](../scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh)。
上下文檢查：[canonical calendar helper](../scripts/check_tw_scale_cash_ablation_context.py)。
歷史候選：[舊 73 項清單](tw_scale_cash_annual_fold10_ablations_2026-10-08.md)，不再是本輪排程。
