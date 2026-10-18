# 全視窗傳統模型基準與實測：2026-10-18

本次在已完成的 `tw_scratch_strategy_20261017_v9/baseline` 旁加入五組從零訓練的
傳統模型。沿用最後一個 fold、來源、帳戶、費率、成交與報告流程；不載入舊策略權重。
這是傳統學習方法的比較，並非神經網路的等價加速版。四次更新的工程測速不代表收斂或策略品質。

## 第一性原理與實作選擇

交易比較必須拆開「訊號學習」「配置」「實際帳務」：這次只替換訊號學習，配置仍為
`score_entmax_scale_separated_cash`，成交與績效由既有 canonical executor 計算。

| 模型 | 要回答的問題 | 專門的運算路徑 | 本輪後端 |
| --- | --- | --- | --- |
| Ridge | 線性訊號是否已能解釋績效？ | matrix-free 共軛梯度、實體 CSR × 全 lag 係數、共享欄位每日期只算一次 | CUDA；訓練 GPU1、驗證／測試 GPU0 |
| LightGBM regression | 非線性門檻與特徵交互是否有用？ | CSR 串流進原生 bins、共用來源綁定 binary cache、原生累積預測 | CPU 16 threads |
| LightGBM ranker | 日內橫斷面次序是否比報酬幅度更有用？ | 同日期群組 LambdaRank，沿用同一份特徵 bins、重新設定 labels/groups | CPU 16 threads |
| XGBoost | 另一種 histogram boosting 的對照 | 有界 dense DataIter + 外部記憶體 quantile pages，保留真正的數值零 | CUDA；先做容量 admission |
| CatBoost | 對稱樹與另一種 boosting 實作的對照 | Pool 量化一次、逐次追加樹、依金融 validation 選模 | CPU；先做容量 admission |

實體來源只保存一次，不把重疊視窗永久複製 32 份。Ridge 的 forward／adjoint 對同一個
樣本對映做 SpMM；CPU 版本平行處理來源區塊，限制巢狀 BLAS，按固定順序歸約。
CUDA 版本常駐實體稀疏區塊，GPU 特徵統計以 FP64 計算；已用 dense oracle 驗證
forward、adjoint、moments 及解算結果。每個 fold 及 inference 完成後釋放 operator 所有權。

LightGBM 使用官方 sampled-column／`PushRowsByCSR` 介面，避免先建立完整 lagged CSR。
CUDA 版本另外經原生 binary loader 初始化 GPU metadata；直接對串流建立的 Dataset
啟用 CUDA 曾失敗，已修正並保留實測。正式 CPU 選擇依本機全欄位有界 probe：
CPU16 的八棵樹及準備約 38.72 秒，CUDA 約 65.15 秒；這不是完整資料量的最終後端比較。

Cache 綁定來源、列／視窗座標、bins 參數與函式庫版本，驗證 SHA 後重用。
特徵 bins 不保存各模型 labels，ranker 不會繼承 regression labels；驗證／測試沿用
訓練期 bins，不用測試資料擬合轉換。原生預測累積器與 solver state 隨 checkpoint 保存。

## 保留與明確變動的契約

- fold10：訓練 2015–2024、validation 2025、test 2026；2014 僅供既有歷史脈絡。
- 保留完整 lookback32、14,726 特徵、2,757 股票槽；展開後 471,232 欄。
  訓練樣本 3,596,416 列。選用的是 v9 已解析 baseline，沒有回改成先前討論的其他尺寸。
- 195 個實體區塊共有 3,239,016,479 個非零，首次建立約 380.38 秒。
  此成本與後續重用 cache 的完整 fold 測速分開，不能宣稱為新節點首次建置速度。
- data、trading、evaluation、walk_forward 四個設定 owner 與解析 baseline 逐欄相同；
  整張、費稅、T+2、FIFO、容量、遮罩、資金與破產規則由原實作持有。
- benchmark 為原 baseline 的 **2330 adjusted-close buy-and-hold**。
  最終工程產物的 181 個測試日期及 benchmark returns，與 baseline 逐值完全相同。
- 保留每回合 validation/test、曲線、checkpoint、每日報酬／權重、fold 與 root plots、
  replay 與 completion 契約。checkpoint 可 `weights_only=True` 在 CPU 載入。
- 傳統模型以訓練期標準化的 future log-return 平方誤差學習；ranker 改用同日期排名。
  它們不對實際帳務 loss 做神經網路反傳，這是實驗的主要方法差異。
  缺少標籤的 22,676 個可交易槽不假造零報酬；評估仍為全部槽產生分數後套用既有遮罩。
- checkpoint 只依 validation 的 canonical 費後 log-utility 選取；test 只作回顧診斷。
  保留 epochs1000、no-improve ratio0.1／patience100；原生 solver 的 residual 收斂
  或樹無法再分裂也會結束，不製造重複更新湊回合。
- 原始資料仍繼承 baseline 的 `research_only=true`、`historical_point_in_time=false`、
  `live_eligible=false`，本次沒有把研究來源提升為可實盤來源。

XGBoost 的 CSR 隱含值具有 missing 語意，因此不能直接把數值零省略後宣稱輸入相同。
全視窗 dense pages 和 CatBoost 原生 Pool 可能超過本機容量；admission 會先拒絕，
保留原因並繼續下一組，不暗中縮短視窗或刪除欄位。容量預估拒絕不等於實際 OOM，
也不證明換一個原生實作、儲存格式或更大機器後仍無法執行。

## 工程測速與驗證

遠端 vastai1T：2×RTX5090（各 32,607 MiB），driver575.64.03，CUDA12.9。
實際 cgroup 上限約 241.76 GiB RAM、53.76 CPU cores；沒有把 host 的 503 GiB／224 cores
當成容器可用容量。正式設定每工作 CPU16、host admission160GiB、device budget26GiB。

相同最終來源、實際完整最後 fold、四次更新、保留全部產物，得到一組 CPU／GPU 對照：

| Ridge 後端 | 整個程序秒數 | 第3/4回合秒數 | 暖機後平均 |
| --- | ---: | --- | ---: |
| CPU16 | 153.7903 | 6.1981 / 6.2811 | 6.2396 |
| CUDA | 137.8308 | 2.9124 / 3.0002 | 2.9563 |

這一對觀察的完整流程 throughput 約 **1.116×**、暖機後約 **2.111×**，因此 Ridge 選 CUDA。
不是重複 A/B/B/A 的統計結論；也不能把早期未平行 CPU 核心 probe 的約40×宣稱為完整流程加速。
兩次完整產物通過 canonical lifecycle validator。最終 compatibility readback 使用
`source-proof-v4.json` 與 `classical-v1-final-cuda`，不是沿用早期候選的通過標記。

相關本機最終回歸組為 **151 passed、1 skipped**（未安裝對應版本的可選後端）；
遠端原生 XGBoost／CatBoost 兩組 **2 passed**。CatBoost 測試揭露 `Pool.quantize` 不接受 `thread_count`，
已移除該參數，保留 Pool／fit 的 thread 設定。最終封存來源的五組小型完整流程
**5 passed**，另記於 `native-frozen-v5-tests.log`，不可把重疊測試數直接相加。

**沒有證據宣稱只剩純 GPU 計算瓶頸。** source/hash 驗證、第一次帳務編譯、準備、
CPU 原生量化及輸出仍佔完整流程時間；GPU 也可能受記憶體頻寬限制。已消除重疊視窗
常駐複製與每棵樹完整重新預測等可觀察重工，保留後續正式長測及資源剖析邊界。

## 來源、正式排程與產物

以下路徑均在遠端 `/root/stockAgent/` 下。`W` 指
`artifacts/operations/training-quality-audit-20261015/classical-v1/`。

- 維護設定：`configs/ablations/tw_classical_full_window_20261018_v1.yaml`。
- 正式模型：`artifacts/markets/tw_classical_full_window_20261018_v1/`。
  五組名稱依序為 `classical__ridge`、`classical__lightgbm`、
  `classical__lightgbm_ranker`、`classical__xgboost`、`classical__catboost`。
- 每組公開目錄映射到 `_capacity/<case>/attempt-00/training/`。
  主要檔案為 `fold_10/checkpoint_best.pt`、`fold_10/model.pt`、
  `fold_10/test_backtest.npz`、`progress.json`、`ablation_worker.log`，以及
  `train_2015-2016-2017-2018-2019-2020-2021-2022-2023-2024/epoch_curve.jsonl`。
- 神經網路 comparator：`artifacts/markets/tw_scratch_strategy_20261017_v9/baseline/`，
  使用其已完成結果；這次沒有重新訓練神經網路 baseline。
- 完整工程 folds：`artifacts/benchmarks/training_quality_20261015/classical-v1-final-{cuda,cpu}/`。
- 計畫與驗收：`W/formal-plan.json`、`W/formal-plan-v2.json`、`W/formal-spec.yaml`、
  `W/backend-final-acceptance.json`、`W/compatibility-final-readback.json`。
- 來源 v4 SHA：`bb44e6315cea660720fc06a43b3aaf8bd9e5921361ac2af401836bf7450b3cf3`。
  正式 Ridge 保留此來源，不在訓練中改碼或換來源標籤。
- 僅修正 CatBoost 呼叫的來源 v5 SHA：
  `4a65f95446bcf443320ee89034638d0e084543d1ca84335012ce9b9a8616a8e2`；
  receipt：`artifacts/code_releases/training_quality_20261015/20261018T021842490766Z-4a65f95446bc/release.json`。
  runtime SHA：`fb4289fc341689b01a575f43359858fc569c89bdcfceae0f3bd935283cdeb691`。
- 原排程：`classical-full-window-v1-20261018T021108-69e456a5`；
  修正版接手排程：`classical-full-window-v2-20261018T021919-4a08e58b`。
  後者已在 Ridge 正常完成後接手；canonical queue 跳過已完成 Ridge、執行其餘四組。
  `W/catboost-amendment-handover.json` 記錄接手結果；各 fold 保留自身來源 manifest。
- 排程 log：`W/formal-queue.log`、`W/formal-queue-v2.log`；run 收據與 supervisor log 在
  `artifacts/operations/agent-workflow/runs/<run_id>/`。

正式各組仍須達到停止條件、保存完成、通過 artifact gate，才能列入收斂績效比較。
本文件的工程通過不表示已超越指定舊模型或 2330 buy-and-hold。

第一組 Ridge 已於 2026-10-18 10:21（台北）完成 195 次更新，最後連續100次無改善，
按 validation 選定第95回合。完整 lifecycle、三份 CPU finite checkpoint、181日日期／benchmark
一致性均通過；canonical 指標獨立重算最大誤差 `4.45e-16` 以下。

| 同一181日 test | 報酬 | Sharpe | Sortino | MDD跌幅 |
| --- | ---: | ---: | ---: | ---: |
| 已完成 v9 神經網路 baseline | 52.38% | 1.775 | 2.807 | 11.34% |
| Ridge：validation選第95回合 | 0.47% | 0.033 | 0.049 | 21.34% |

Ridge 未改善測試品質，不替換神經網路策略；LightGBM regression 已開始，其餘組尚無正式結果。
這不是與不同契約的歷史 fold_11 重播直接比較。證據：`W/formal-ridge-acceptance.json`；
完整已完成 Ridge 的離機回傳以 `W/formal-ridge-preservation-manifest.json` 與本機驗收收據為準。
已回傳本機 58 檔、128,514,391 bytes，全部逐檔 SHA 相符、完整 lifecycle 及三份模型／checkpoint
CPU finite 檢查通過。本機完整目錄為
`artifacts/markets/tw_classical_full_window_20261018_v1/_capacity/classical__ridge/attempt-00/training/`；
驗收收據為 `W/formal-ridge-local-preservation.json`。仍執行中的其他組以遠端目錄為準。

原生介面參考：[LightGBM C API](https://lightgbm.readthedocs.io/en/stable/C-API.html)、
[XGBoost external memory](https://xgboost.readthedocs.io/en/stable/tutorials/external_memory.html)、
[CatBoost fit](https://catboost.ai/docs/en/concepts/python-reference_catboostregressor_fit)。
