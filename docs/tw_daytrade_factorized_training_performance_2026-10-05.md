# 寬 panel 當沖訓練效能實測與優化（2026-10-05）

## 1. 執行進度

- 最新使用者已執行不分解 v3 正式訓練並保存 epoch3，目前停止於 epoch4；「正式未啟動」只適用以下舊工程收據。profiling-off 的正式 epoch3 rank0 wall 約216.96秒。新的搬運／單 fold 啟動／最大 rank telemetry 修正與雙卡完整 A/B 驗收，見[最新不分解工作流報告](tw_daytrade_no_basis_pipeline_optimization_2026-10-05.md)。候選在金融與產物一致性通過前不 promotion。
- 最新指示確定回到**不分解模型**；SVD32 僅保留為分析歷史。完整全量 FP64 光譜及 0–100% 門檻／全部 14,726 維明細已完成與獨立核對，見[光譜報告](tw_daytrade_full_feature_spectrum_2026-10-05.md)。原門檻拒絕 direct v1／v2；v3明確joint-stem FP32 island已通過雙卡六步oracle、完整fold11三輪／全部產物與獨立checkpoint驗收。第三輪train最大rank187.87秒、epoch320.04秒、零新graph；正式長訓練未啟動，入口與限制見[無基底驗收報告](tw_daytrade_no_basis_bf16_remote_2026-10-05.md)。以下基底版歷史測速不是同架構等價加速比較。
- **最新使用者決定：先不做基底分解版。** 已以驗證過 PID／boot／start-ticks 的 workflow SIGTERM 取消基底版 A/B；2026-10-05 09:42:03 UTC 收據為 `cancelled`，兩張 GPU 均回到空閒。既有資料、檢查點與診斷全部保留。
- 改為同份 14,726 通道 value-only panel 的直接輸入模型，沒有 temporal basis／PCA／KLT，也沒有 24 維瓶頸；模型參數 1,133,582、embedding32、resolved attention `latent`。此為明確的架構變更，不接續基底版 optimizer；新的雙卡驗收另記 [無基底實驗報告](tw_daytrade_no_basis_bf16_remote_2026-10-05.md)。
- 以下基底版數字保留為診斷歷史。三輪 A/B **未完成、未升級**；取消前兩輪曲線可供檢查，但不當作 epoch3+ 或完整 fold 比較。
- 原數值-only BF16 v4 的完整雙卡 fold11 工程生命週期及嚴格產物驗收已通過；正式長訓練未啟動。
- 最新要求是加速資料準備、訓練、驗證、回測、曲線與存檔。以真實 panel、雙 RTX5090 DDP、全域 batch32 為比較標準，不用縮資料或省略驗證宣稱加速。
- 真實資料模型／來源 profiler 已開始：`value-only-real-model-profile-v4-20261005T084832-e63c675f`。使用原共享模型及 DDP，沒有 optimizer 更新；線性梯度探測不是正式 recurrent 金融 loss，不當成完整 epoch。
- 新候選尚未升級；必須取得相同 benchmark 設定下第 3 輪以後最大 rank wall，以及完整 fold 產物與數值一致性證據。
- 第一次真實 profile 已完成：零解碼 miss，7.54 秒 model step 的巢狀 read／pin／H2D enqueue 約 6.98 秒；CPU dense 展開約 5.87 秒。forward＋checkpoint 重算共 88 次 source chunk。
- v5 無損 compact union 搬移的相鄰雙卡 step 中位數約 4.92 秒，dense 對照 7.88 秒；傳輸 15.27 → 1.82 GB／rank／step。但獨立程序 output／gradient hashes 不一致，原嚴格 gate 退出碼 1，**沒有升級**，不能稱位元一致的全模型。
- v6／v7 改為 verified storage rectangles 直接在 GPU 還原完整 dense Tensor，v7 再移除中間 contiguous array 複製，直接填入 bounded pinned allocation。無 sparse-event、不裁特徵、不減少模型計算。
- 控制與局部驗收工作：`value-only-direct-pinned-v7-vali-20261005T091411-65a6d57a` 已成功。原 fixture 兩 rank 各六步 oracle 通過；真實早期／後期的每個 44 chunks 位元相同，output max abs error0，gradient max abs error分別 7.45e-9／2.98e-8。native dense 重跑 output error也是0，沒有提高容差。
- v7 最新相關回歸 **216 項通過**（20.32 秒）。不將各版本重疊測試相加。
- 完整三輪 A/B 曾開始：`value-only-full-fold-transport-a-20261005T091934-2d7ec229`，後依使用者要求正常取消。工程 CLI 明確 `epochs=3, early_stop_ratio=0`；不把取消寫成完成、不執行後續基底 v4 對照。

## 2. 固定比較邊界

| 項目 | 固定設定 |
| --- | --- |
| 資料 | `tw_factorized_nullable_panel_values_lag1_v2`，截至 2026-10-02 |
| panel | 3,109 交易日 × 2,757 商品 × 14,726 通道 |
| 模型 | FinancialTransformer、lookback32、embedding32、22 基底家族、248,061,198 參數 |
| 精度 | BF16 AMP；Float32 master／Adam；basis、temporal、head 的既有 FP32 islands |
| 訓練 | fold11、雙卡 DDP、global batch32；每個 chronological trajectory 一次 AdamW |
| 執行 | 既定 first-minute-only 50% 容量；official close 不限容量 reduction-only |
| 產物 | 每輪 validation／test、同步圖、checkpoint、final best 回測與報告保留 |

原始 NULL、release clock、資格與 execution evidence 保留，不新增模型狀態通道、不啟用 sparse-event 執行、不變更架構或金融公式。

## 3. 原 v4 診斷基準

[完整原驗收](tw_daytrade_value_only_bf16_remote_2026-10-05.md)與[收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-value-only-metadata/fold11-acceptance-bf16-v4.json)。

| 階段 | 已測時間 | 限制 |
| --- | ---: | --- |
| 完整冷 PCA／RMS 準備 | 672.53 s | 與後續 cache-hit run 分列 |
| v4 cache-hit pre-epoch | 63.66 s | 完整 cold compile 仍在首輪 |
| 第 2 輪 train 最大 rank | 669.64 s | 83 batches、一次 AdamW |
| 第 2 輪 epoch 最大 rank | 951.18 s | callback 前；未達 epoch3+ 比較窗 |
| model forward，rank0 | 3,337.73 ms／batch | 包含 source 展開與 H2D，不是純 kernel |
| backward，rank0 | 3,893.63 ms／batch | 包含 checkpoint 重算及第二次讀取 |
| 金融 loss，rank0 | 372.85 ms／batch | 尚非目前最大佔比 |
| 每輪同步圖 callback | 0.57 s | 保留，不藉省略加速 |
| 完整受控工程 run | 2,668.02 s | 兩輪、前置 oracle、最終回測、全部產物 |

validation 與 test curve 在不同 rank 並行，不能將兩項時間相加當作 wall。第 2 輪零新 graph 僅是診斷基準。

## 4. 工具與候選機制

遠端已驗證 Nsight Systems 2025.2.1、Nsight Compute 2025.2.1 存在；先用 PyTorch CPU／CUDA profiler 在實際執行路徑取得 timeline，未取得的 counter 不當作已測。GPU0、GPU1 為 SYS 互連，分別鄰近 NUMA3／NUMA2，沒有 NVLink；需實測 NUMA／host buffers 與傳輸。

優化依實測最大瓶頸排序。候選包括有限記憶體的 source prefetch／pinned buffer 重用、checkpoint 的保存／重算分界、compiled graph 配置和 kernel 排程。既有全資料／全參數／loss／DDP 不會因操作調度而省略。

PyTorch selective checkpoint 支援精細選擇保存與重算，但需驗證編譯、記憶體及梯度契約；不能只因工具較新就升級。[PyTorch 官方說明](https://pytorch.org/blog/activation-checkpointing-techniques/)。搬移與 kernel overlap 以 timeline 判斷，不將相加的時間當成延遲 lower bound。[NVIDIA 搬移分析](https://developer.nvidia.com/blog/optimizing-cuda-memory-transfers-with-nsight-systems/)。

原生 NVFP4 的兩卡 Linear 訓練實測較 BF16 慢約 77%，且梯度誤差較大，仍保留探測結果而不接入主路徑；這不是所有低精度方法永遠較慢的結論。

尚未宣稱達到理論極限，也不以額外省略報告／checkpoint 換取測速。

## 5. 搬移候選與拒絕紀錄

原資料的壓縮 codec 只省略「已證明全為正零」的欄位，負零與非零位元仍保存。原路徑在 CPU 重新展開全寬度，再 pin／H2D；重算再做一次。v5 先做 compact union，降低傳輸但仍在 NumPy scatter 花費約 3.82 秒／step。v7 直接讀原物理矩形，做一次 pinned copy，在 GPU 寫回原坐標；既有 statistics／PCA／RMS reduction 路徑沒有改動。

v5 A/B：`value-only-lossless-transport-ab-20261005T085906-5a3c9536`，原 input／模型／checkpoint 相同，但獨立 CUDA 執行的 output／grad hashes 未全相同。保留失敗；新增逐 raw 位元核對及同程序 dense 重跑，查明輸入與 native 計算差異，不放寬既有 oracle 容差。

v6 額外新增半數正零＋負零的 131 通道 fixture：`value-only-rectangle-transport-v-20261005T090701-0d2bcce0` 在第 2 個 FP32 Adam 更新的 basis gradient 失敗（343／2,197,632 cells，max abs 約 1.46e-5，原 atol3e-6）；第一次 raw GPU 還原位元檢查已通過。v7 保留相同資料的 dense_cpu 控制來辨識原分塊／full-dense reduction 的誤差，不把額外 stress fixture 失敗改寫成通過。原模型 fixture 不因搬移設定改資料；其 FP32／BF16 六步 oracle 仍用原門檻。實際資料的每塊還原必須逐位元相同，不能靠 output 容差藏資料差異。

所有候選使用獨立 source release／設定／artifact root，v4 frozen code 和原正式入口保留；正式 1,000 epoch 沒有開始。

### 5.1 實際 v7 搬移驗收

| 真實 training batch | 最大 rank model step 中位数 | raw 位元核對 | output max abs | gradient max abs |
| --- | ---: | --- | ---: | ---: |
| global row0 | 0.681 s | 每 rank 44 chunks 全部相同 | 0 | 7.45e-9 |
| global row2048 | 0.792 s | 每 rank 44 chunks 全部相同 | 0 | 2.98e-8 |

包含 stock encoder／basis、compact head、checkpoint 重算與 DDP gradients，但採線性 gradient probe，沒有 optimizer 或正式 recurrent loss，不能以此當成 epoch。後期 rank0 每個 step logical full data 15,265,442,832 bytes，實際 host payload 1,693,696,880 bytes（約 11.1%）；原特徵與完整 dense GPU 計算都保留。[後期 rank0 profile](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-value-only-metadata/model-profile-transport-v7-row2048-rank0.json)。

v6 額外零欄位 fixture 的同資料 dense_cpu 控制，在原相同 FP32 第2更新／basis gradient 門檻也被拒絕，因此它辨識的是既有 stock-chunk 與 full-dense reduction 累積的邊界，不能說已修掉該額外 stress 的全模型差異。v7 原模型 fixture 不改輸入，所有實際 raw 值逐位元檢查及同模型／同形狀 output／gradient 比較通過；失敗控制、原未放寬容差和兩類驗收的區別完整保留。
