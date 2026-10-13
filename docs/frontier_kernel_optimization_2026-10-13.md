# Vastai1T frontier kernel 優化實測

本輪實作、遠端套件與組合試驗、完整 A/B/B/A 驗收已完成。結果適用於本次雙 RTX 5090 與 fold10 四輪工作負載；不宣稱全域物理最優或策略績效改善。

## 平衡比較結果

已鎖定並重測的配方：NUMA affinity + funding preparation v3 + Gregorian calendar cache + BF16 temporal + QuACK softmax；global batch 256、CPU 8 threads、compile 16 threads。FP32 master parameters、compact 統計／修正項、portfolio FP32 與金融 FP64 reductions 保留。

| A/B/B/A 順序 | 穩態 epoch 中位數 | 完整四輪 fold |
| --- | ---: | ---: |
| A1 原始 baseline | 95.57 s | 583.18 s |
| B1 鎖定候選 | 66.80 s | 461.86 s |
| B2 同一候選 | 76.32 s | 486.28 s |
| A2 原始 baseline | 81.85 s | 530.15 s |
| baseline 兩次中位數 | **88.71 s** | **556.67 s** |
| candidate 兩次中位數 | **71.56 s** | **474.07 s** |
| 時間減少 | **19.33%（1.240×）** | **14.84%（1.174×）** |

每個 trial 先取第三／四輪 maximum-rank wall time 中位數，再對同組兩個 trial 取中位數。兩個前後配對的 epoch speedup 為 1.431×／1.072×，完整流程為 1.263×／1.090×；時序波動明顯，這是四組工程量測，沒有以最初 95 秒或最快單輪放大最終收益，也不是統計信賴區間。

四組都使用相同 immutable 資料、fold10／seed 42／2439 筆 train rows、雙 GPU DDP、每輪一次年度 optimizer update，保留 validation/test、曲線、plots、checkpoint、結尾完整 backtest 與 lifecycle。穩態零新圖、有限非零梯度、CUDA strict preflight 及顯存准入均通過。baseline frozen source SHA 為 `1689b1b629e84de434c4f76bca3c7bff5c28a015e6099cfb0a6e60c393eede5a`；candidate 為 `daff3a8e0777e43ed0b0c969ac3b509d1f9bfd96c317fe56a41b27510923192a`。

本輪共盤點 **41 項套件／方法**，執行 **302 次算子探針（196 次取得可量測結果）**、**76 次完整 fold 嘗試（61 次通過，含 2 次不排名的 profiler 診斷）**。72 份配方通過設定解析與程式消費檢查；這項靜態檢查不替代真 GPU 的條件路徑驗收。失敗、較慢、不適用與未測完整模型的項目逐項保留。

可重跑的鎖定配方是 `configs/benchmarks/frontier_kernels_best_20261013.yaml`。它選擇穩態優先的候選；篩選階段 NUMA/funding/CPU1 曾取得更短的 444.91 秒四輪 fold，但尚未做該配方的 A/B/B/A，不能混用其總耗時與本配方的穩態收益。

## 第一性原理

一次訓練不是只有矩陣乘法。完整 epoch 的時間包含資料取用與搬運、編碼與 attention、帶年度狀態的財務 loss、反向重算、DDP 等待、驗證與報表。最後要比較最大 rank 的實際 wall time，而非兩卡平均利用率。

本輪保留兩個目標：`T_fold = source 驗證與準備 + 編譯暖機 + 四個完整 epoch + 結尾完整 backtest／artifact acceptance`；穩態則使用 epoch 3/4 的 maximum-rank wall time。資料搬運、CPU 提交、GPU kernel 和 NCCL 會重疊，profiling 各項累計秒數不能直接相加成 wall time。正式長訓練的編譯攤提不同，也不能拿短實驗的 scheduler／新權重數值宣稱策略更好。

這個模型只有 1,133,582 個主參數，FP32 為 4.32 MiB，而 activation/source 已用掉數十 GiB。因此 FSDP2／DTensor 的參數與 optimizer 分片省不了主要 VRAM；目前沒有 expert/router、decode KV cache，也不產生 DeepEP／vLLM／SGLang 需要的 workload。年度軌跡每 epoch 只更新一次 optimizer，fused AdamW 已在 baseline 使用，進一步縮短這一次 step 的總收益有限。這些是從實際形狀和 cadence 得出的適用性條件，沒有把未執行的方法列作已測成功。

允許 BF16／FP8／NVFP4 或不同 reduction grouping 改變數值與梯度，但 forward 的來源、日期、持倉權利、價格、費用、整股、FIFO 和吸收式破產仍遵循同一 contract。每種非預設 recipe 使用自己的 fingerprint 和 fresh artifact root；主參數、敏感統計及金融累計的 dtype 不會因 AMP 被永久降為 BF16。接受配方須完成 2439 筆年度 train rows、四個 epoch、非零有限梯度、穩態零新圖和完整 lifecycle。

可觀測的工作鏈是 immutable feature/source → compact packet → temporal/cross-stock 編碼 → 全域權重 gathering → Gregorian session 的 FIFO 帳務與年度狀態 → adjoint/gradient mean → 唯一年度 optimizer step → validation/test、曲線、plots、checkpoint 與結尾 backtest。每一種加速都需要指出它移除了鏈上的哪一種成本。矩陣 FLOP rate、GPU 利用率、單算子 latency 與完整 epoch 是不同分母。

即使本 fold 的訓練 session 都經 source 證明當日 flat，前一天現金、NAV 和存活狀態仍會影響下一天的 whole-lot sizing。因此不能把每一天改成互不相依的獨立 batch，也不能以任意 detach 縮短年度信用分配。這也使 `FIFO checkpoint block 16→32` 在已走 flat-terminal 快速路徑時成為 no-op，不能把觀測時序的差異歸因於它。

目前模型是 d_model 32、四個 head，每個 head 只有 8 個元素；lookback 32、last-only temporal query、16 factor token、16 market token、LayerNorm、SwiGLU、零 dropout。這與 head 64–128、數千 token 的 LLM 工作負載差異很大。當矩陣很窄時，額外量化、padding、轉置、kernel launch 與跨套件邊界的成本，可能超過 tensor core 帶來的收益。

第一個 BF16 配方只將 compact stem 的線性計算轉為 BF16；LayerNorm 統計、缺省零欄位的 affine 修正及主參數仍是 FP32。保留全部邏輯 feature 的貢獻，沒有丟掉 source 欄位。每個非預設 kernel/precision 配方會改變 training fingerprint，使用獨立 artifact root，拒絕不相容 optimizer resume。

金融帳務仍由 canonical physical FIFO/carry 路徑計算，保留精確費用、eligibility mask、whole lot、年度重設、因果時鐘、absorbing ruin 與敏感 FP64 reduction。使用者允許數值與梯度軌跡差異；這不等同於允許錯誤梯度、遺失 recurrent state 或更改交易機制。

## 遠端硬體實測邊界

strict preflight 已驗證兩張 GPU 的 CUDA 初始化、allocation、kernel 與 synchronize。Driver 575.64.03；CUDA SDK 12.9.86；PyTorch 2.11.0+cu128。RTX 5090 是 SM120，兩卡拓樸 SYS，實測彼此 `can_device_access_peer=false`。

| 指標 | GPU 0 | GPU 1 |
| --- | ---: | ---: |
| 1 GiB device copy，計入讀與寫 | 1,522.8 GB/s | 1,523.3 GB/s |
| pinned host → device | 11.35 GB/s | 23.66 GB/s |
| 8192³ BF16 GEMM | 236.58 TFLOP/s | 236.29 TFLOP/s |
| 4096³ FP32/TF32 GEMM | 103.43 TFLOP/s | 103.19 TFLOP/s |
| 1024³ FP64 GEMM | 1.304 TFLOP/s | 1.291 TFLOP/s |

這是指定矩陣/搬運大小的可達速率，不是整個模型的物理最優證明。使用 GPU 相鄰 CPU affinity 重測後，兩卡 pinned H2D 為 27.46／27.63 GB/s；完整 NUMA fold 的穩態 epoch 為 73.44 s，不將單一搬運的 2.4 倍比例套到整個 epoch。FP64 GEMM 的低速也不授權把金融 reduction 改成 BF16。

來源 feature manifest 的 4,224 個 immutable block 共 2.484 GiB compressed、47.487 GiB decoded values/columns、467.954 GiB logical dense。每 rank 的 48 GiB host cache 足以容納 decoded blocks；完整 physical source preparation 的大小是另一個分母。本輪保留既有 lossless cache，沒有重建一條資料管線。模型 compact encoder 有自己的 bounded packet cache，資料層 transfer mode 不會直接控制它；另加入可明確關閉該 encoder packet cache 的獨立配方。

## 真正的瓶頸與新的融合試驗

Nsight Systems 在完整 baseline 的中段擷取 120 秒，記錄約 515 萬次 CUDA kernel；逐卡 kernel 時段聯集只佔擷取窗的 10.04%／20.88%，其中仍包含 NCCL 等待。這是帶 profiler overhead 的診斷，不納入速度排名。450 萬次 `cudaLaunchKernel` API 共耗 25.42 秒，平均約 5.64 µs；11.45 萬次 `cudaMemcpyAsync` API 共耗 27.82 秒。H2D 的 49,311 次呼叫中，44,664 次不超過 16 bytes，佔 90.58% 的呼叫次數。這些 API 時間跨 thread/rank 可重疊，不能相加當成可直接刪除的 wall time；小 scalar 的呼叫比例也不是 byte 或傳輸時間比例。大量 FP64 fill/where/add 與小型金融狀態操作，讓 CPU launch、同步及跨 rank 等待比大型 GEMM 更值得優化。

因此加入三條共用路徑的 opt-in 試驗。DDP immutable buffer 配方先驗證 buffer 不會更新，省去逐 forward 的重複廣播；年度完整梯度同步配方保留所有 forward/backward 與全域金融 action gathering，只把 dense gradient mean 延到同一個年度 optimizer step 前，並檢查各 rank 的梯度存在性。兩 rank Gloo 測試涵蓋尾端 padding、unused parameter、clipping、不同初始 buffer 與單次更新，不能省略這些機制檢查。

金融日曆 cache 保留 Gregorian ordinal，回傳獨立 tensor，避免共享快取被 state 修改。資金準備融合直接重用原有 inventory、entitlement、claim、interest、source validity 與 opening NAV 的函式，僅把實際日期作為 scalar tensor 的編譯輸入，避免每個交易日重新產生 graph。CUDA 財務 adjoint、來源錯誤隔離、跨日期單一 graph 以及 bucket 邏輯狀態檢查均已通過；funding v3 與 calendar cache 的完整 fold 已驗收，compiled calendar 與財務 CUDA Graph 保留各自的重測入口。

首次 compiled calendar gate 因 PyTorch 2.11 的 Dynamo non-leaf `.grad` 探查警告升級成錯誤；使用既有精確 warning filter 後，遠端 4／4 通過。首次財務 CUDA Graph gate 則在 tree-manager 建立 allocator pool 的刻意空圖處失敗；新入口只在 manager 初始化範圍處理該警告，實際財務 capture 的空圖警告仍會失敗，並增加重複完整 adjoint 與非 bootstrap graph node 證明。[PyTorch 2.11 tree-manager 原始碼](https://github.com/pytorch/pytorch/blob/v2.11.0/torch/_inductor/cudagraph_trees.py) 可核對此初始化機制；重測結果不覆蓋原始失敗紀錄。

目前 driver 對應的官方 cu128 wheel 是 PyTorch 2.11。上游已發布 PyTorch 2.14；從 2.12 起 Blackwell 官方新 wheel 需要 CUDA 13 和至少 580.65.06 driver，因此本輪沒有修改實體主機 driver 來追新版。

## 套件與方法逐項清單

「不適用」是一個工作負載條件判定，不是假裝該套件已跑過完整訓練。待測、失敗、較慢和被機制條件排除的項目都保留。

原先 38 個套件／通用方法之外，另列 3 項從這個 workload 找出的優化；共 41 個條目，不代表 41 個獨立套件。

| 項目 | 減少什麼成本 | 本模型的實測／判定 |
| --- | --- | --- |
| NUMA / host threads | 跨 CPU socket 記憶體搬運與 host 排程 | GPU-local H2D 實測由 11.35／23.66 提升至 27.46／27.63 GB/s；NUMA 單項及資金融合組合已完成，1／4／16 threads 全部完整驗收，83.19／86.41／92.41 s／epoch，增加 threads 未改善 |
| funding preparation / capacity buckets | FP64 小 kernel、變長 cohort／claim 的編譯圖 | v3 中性容量補位保留原帳本與所有原始零列 credit；真 CUDA oracle、完整 fold 通過，NUMA 組合約 70.5 s／epoch；financial fusion 16／32 均已實測 |
| Gregorian calendar cache / compiled restore | 重複建立日期 tensor、calendar CPU→GPU 小傳輸 | cache／compiled restore 各真 GPU gate 4／4、完整 lifecycle 通過，分別 85.10／94.45 s／epoch；沒有額外加速宣稱 |
| panel slab | 重複展開 lookback 寬 feature | baseline 已使用；保留唯一 source row 的 canonical 路徑 |
| compact transfer | CPU→GPU 傳輸及寬欄位零值 materialization | cached baseline、uncached 82.88 s／epoch、packed 86.56 s 均完成完整 fold；encoder 自有 packet cache 使模式切換可能成為 no-op |
| torch.compile / Inductor | launch、elementwise fusion、反向算子整合 | baseline 已使用；穩態不准持續產生新 graph |
| cuBLAS / cuBLASLt | GEMM 排程與 tensor core | 已跑 FP32/BF16 前後向；窄矩陣仍是強 baseline |
| CUTLASS | 可定製 GEMM tile、epilogue | 經 QuACK/CuTe DSL 的實際 GPU 路徑執行；tuned GEMM 已測 |
| QuACK | CuTe DSL 實作的 norm、softmax、GEMM | d32 LayerNorm compiled 約 0.0088 ms，單項完整 fold 84.95 s／epoch；softmax 配合 BF16 temporal／manual、NUMA/funding 的篩選結果 66.36 s／epoch，A/B/B/A 合併後 71.56 s，較 baseline 減少 19.33%；14726 維探針未准入，並保留 GEMM／norm 較慢觀測 |
| SDPA | 選擇 fused attention | 已測 MATH/FLASH、compiled 自動選擇；bool key mask 的強制 FLASH 介面拒絕。舊 `sdpa` 在 Blackwell 被 workaround 導回 manual，不計為 SDPA 實測；另建 `sdpa_explicit` 與獨立 fingerprint，先驗證實際 dispatch／遮罩／完整梯度，再跑完整 fold |
| cuDNN attention | fused attention forward/backward | 代表性 temporal/portfolio BF16 probe 的 masked key/value derivative 檢查失敗；未 promotion |
| FlashAttention 4 | 少讀寫 attention 中間量 | SM120 mask_mod backward 被官方版本拒絕；無遮罩 temporal BF16 eager／compiled adjoint 通過；custom-op bridge 完整 fold 99.57 s／epoch、640.89 s，未採用為加速配方 |
| Liger | Triton norm、SwiGLU fusion | eager／compiled 真前後向與兩種完整模型均通過；LayerNorm 90.46 s／epoch、SwiGLU 85.97 s／epoch，均未勝過目前 NUMA/funding 組合 |
| CUDA Graph | CPU launch 與 stream starvation | 算子 replay 已測；partition 完整 fold 的 checkpoint device 一致性失敗；portfolio-only 完整 fold 較慢；財務 compiler 的非空 capture/repeated credit 通過，但 gate 仍因 pending backward 警告失敗 |
| fused AdamW | optimizer 小 kernel launch | baseline 已使用；與 foreach 的 step 探針已保留 |
| DDP overlap / bucket | gradient all-reduce 等待 | bucket 25 MiB 完整 fold 83.30 s／epoch；immutable buffer 83.72 s、trajectory 結尾同步 88.17 s、NUMA 組合 84.23 s，均完整 lifecycle 通過；與 funding 的組合另測 |
| selective checkpoint | activation VRAM 與反向重算的交換 | 一般關閉會受容量 guard 回退；明確 force-retention 的原精度／BF16 兩種完整試驗都在第一輪 OOM；保留重算仍是可行的容量策略 |
| BF16 / TF32 | tensor core 與 activation byte 數 | compact、temporal、portfolio 單項／組合均已完成完整 fold；NUMA/funding 配合 neural BF16 batch 512 也通過完整驗收，72.01 s／epoch、553.08 s／fold，未勝過 batch 256 |
| Triton | 自訂 tensor kernel 與融合 | SwiGLU 正確前後向已測，與 compiled native 接近 |
| Gluon | 明確指定 Triton layout | SwiGLU 正確前後向已測，未證明 epoch 收益 |
| CuTe DSL | 明確 tile/layout 與 tensor core 排程 | QuACK／FA4 真前後向及完整模型整合已執行；DSL 本身不保證加速 |
| TileLang | 定製 tile、記憶體搬運與管線 | SwiGLU 正確前後向已測，未證明 epoch 收益 |
| Helion | Python kernel 自動 layout / autotune | SwiGLU 正確前後向已測，未證明 epoch 收益 |
| cuTile | compiler 管理 tile kernel | 最新套件要求較新 driver；現場 575 不符合，沒有升級 host driver 強行測試 |
| ThunderKittens | C++ tile、warp primitive | 最新版本支援 SM120；tile-aligned C++ SwiGLU 正確前後向已測；FP32 0.00672 ms、含 BF16/FP32 轉換 0.0231 ms，仍較慢 |
| cuBLASDx | CUDA kernel 內融合小 GEMM | CUDA 12 的 0.4.1 tile GEMM 前後向通過，0.1036 ms；比 compiled native 窄 GEMM 慢 |
| Transformer Engine | FP8/MXFP8/NVFP4 quantized training | 原先較大形狀的 FP8／NVFP4 前後向可執行；實際 N=32 尾端 FP8 eager 通過，但 fullgraph 需要目前 Torch 2.11 缺少的 `is_opaque_constant_type`；NVFP4 尾端 cuBLAS 不支援，compiled 另有 bool lshift 錯誤；MXFP8 被 SM120 training layout gate 拒絕 |
| TorchAO | FP8 training、量化 recipe | functional adapter 修復 N=88 尾端，M/N/K 中性補位與 crop 的 8 項真 eager／compiled 前後向 gate 全過；完整模型 97.21 s／epoch、736.93 s／fold，未採用加速 |
| DeepGEMM | BF16/FP8 GEMM 與 MoE grouped GEMM | plain BF16 API 實際委派 cuBLASLt；修復 module descriptor 與 canonical SDK override，8 項 compact／tail 真 GPU gate、完整模型 81.18 s／epoch、668.75 s／fold 通過；未採用加速，不泛化為 FP8 支援 |
| DeepEP | MoE token dispatch/combine | 目前模型沒有 expert/router；不能將通信套件直接接到普通 DDP claim 加速 |
| MegaMoE | 多 expert grouped GEMM 與通信融合 | 沒有 expert workload；需要獨立 MoE 架構實驗 |
| NVSHMEM | 多 GPU 一側存取與通信 | 現場已安裝；沒有 P2P/NVLink，也沒有 MoE dispatch workload；不等同於現有 all-reduce 的替代 |
| FSDP2 / DTensor | 分片參數與 optimizer 狀態 | 實測參數 1,133,582 個、FP32 4.32 MiB；主要 VRAM 是 activation/source，分片這個小模型的收益有限；尚未測完整 fold |
| FlexAttention | 可程式遮罩與 fused attention | head 8 原生要求不符合；零補到 16、保留原 scale 後，代表性 temporal／portfolio 正確前後向已測；未證明勝過現有 reduction |
| cuSPARSELt | 2:4 structured sparse tensor core | padding、參數遮罩及 dense oracle 的明確 2:4 前後向通過，包含壓縮後窄 GEMM 0.0432 ms，仍較慢；未對模型或 source 作 pruning |
| Flash Linear Attention | 線性 recurrence、替代 sequence mixer | 改變 softmax attention 機制，不能作相同模型的直接 kernel 替換；另列架構實驗 |
| SageAttention 3 / SageBwd | FP4 inference 與低位元 attention training | SageBwd 2026 論文確實研究 trainable INT8；本次檢查官方 main commit `d1a57a5` 的 source tree，未找到 SageBwd kernels 或訓練 API，release issue 仍 open；無可驗證的官方 backward 路徑，不納入已實測訓練加速 |
| Torch-TensorRT | 推論圖與 runtime 排程 | inference 工具，沒有此完整金融訓練 backward 的直接路徑 |
| FlashInfer | LLM attention、KV cache 與 inference kernel | 目前沒有 KV cache／decode workload；不算訓練替代 |
| vLLM | LLM serving batching、KV 管理 | 目前不是自回歸 LLM serving；不適用這個 fold 工作流程 |
| SGLang | LLM serving runtime 與調度 | 同上；不以部署 runtime 取代 trainer |

## 可重跑的實作與證據

- 清單與配方：`configs/benchmarks/frontier_kernels_20261013.yaml`、`frontier_kernels_phase2_20261013.yaml`、`frontier_kernels_phase3_20261013.yaml`。
- shared model adapters：`stockagent/models/kernel_backends.py`；lazy import，保留 parameter identity、state-dict 名稱與 RNG。
- canonical 完整 fold wrapper：`scripts/run_frontier_training_trial.py`、`run_frontier_fold_queue.py`；凍結 source receipt、GPU lease、CUDA strict preflight、獨立 root。
- forward/adjoint 算子探針：`scripts/benchmark_frontier_kernels.py`、`frontier_dsl_operators.py`、`frontier_cpp_operators.py`；額外驗證遮罩及完全 masked row。
- 硬體 ceiling：`scripts/benchmark_frontier_roofline.py`。
- 保留快慢與失敗結果：`scripts/summarize_frontier_benchmarks.py`。

完整 JSON/CSV 與 roofline 回傳到 `artifacts/operations/frontier-kernel-optimization-vastai1t-20261013/`。遠端原始紀錄位於 `/root/stockAgent/artifacts/benchmarks/frontier_kernels_20261013/`，其正式 source/data paths 由各 trial 的 run-spec 記錄。資料與 formal checkpoint 沒有清理或回收。

在同一 Vastai1T 主機重跑鎖定配方，使用已回傳的 `final-best-recipe.yaml`；每次指定新的 output root。此命令執行四輪工程 benchmark，正式長訓練另使用相容 fingerprint 與 fresh artifact root。

```bash
source /root/stockAgent/scripts/runtime_env.sh
FRONTIER_ROOT=/root/stockAgent/artifacts/benchmarks/frontier_kernels_20261013
run_fintech_python "$FRONTIER_ROOT/candidate-source-v65/build-source/scripts/run_frontier_fold_queue.py" \
  --spec "$FRONTIER_ROOT/final-best-recipe.yaml" \
  --baseline-config /root/stockAgent/artifacts/operations/training_launches/tw_day_trade_factorized_values_20261007_no_basis_scale_separated_cash_annual_v1/config.yaml \
  --kernel-python "$FRONTIER_ROOT/kernel-env/bin/python" \
  --output "$FRONTIER_ROOT/best_replay_NEW_UNIQUE_ID"
```

本機證據目錄的 `abba-acceptance.json` 保存四組完整驗收、配對 speedup 與 source SHA；`results.json`／`folds.csv`／`operators.csv` 保存全部觀測，`workflow-tradeoffs.png` 與 `balanced-abba.png` 顯示穩態及完整流程交換。`final-evidence-manifest.json`／`final-evidence-readback.json` 記錄 bounded archive 的逐檔雜湊與獨立回讀；完整平衡試驗產物解開在 `final-evidence/final-abba/`。

60 個實作、測試、配方及報告檔案已合併回主工作區；推回當下核對 152 個無關檔案，`financial_transformer.py` 的另一項 inference 改動以三方合併完整保留。合併後模型／kernel／activation／inference／trial 回歸 89 項通過，36 個 Python 檔案的 Ruff 與 diff 檢查通過。本輪共 34 組通過的回歸紀錄，集合重疊不相加。平衡速度對應上方兩個凍結 source SHA；合併後主工作區另保有其他工作中的資料、inference 與服務改動，未將其宣稱為再次遠端測過的同一 release。

遠端證據 archive 約 114.9 MB，獨立解開並逐檔核對 1,485 個檔案，所有 size／SHA256 一致，含四組 checkpoint、曲線、圖表與 lifecycle；archive SHA256 為 `d8d5d99e6db47d2d0e03a2e896fea2ebfc987d3c7d6a59604aa9270a3e8a3e9c`。`final-delivery.json` 記錄最終檔案身分、驗收與適用邊界。所有 GPU benchmark 已結束，自有唯讀監測程序關閉，instance 未停止。

## 附錄：逐次完整 fold 與修復證據

同一份 fold10 年度 carry 工作負載、雙 RTX 5090 DDP、global batch 256、seed 42，完整執行四個 epoch。比較 epoch 3/4 的最大 rank wall time，保留每個 epoch 的 validation、sampled test、曲線、繪圖、checkpoint，以及結尾完整 backtest 和 lifecycle acceptance。

| 配方 | 穩態 epoch 中位數 | 完整四 epoch fold | 狀態 |
| --- | ---: | ---: | --- |
| accepted runtime baseline | 95.00 s | 792.15 s | lifecycle 通過 |
| compact projection GEMM BF16；統計量、修正項與主參數 FP32 | 87.19 s | 693.69 s | lifecycle 通過；首輪約 8.2% 較快 |
| temporal BF16 | 88.78 s | 638.92 s | lifecycle 通過；首輪約 6.6% 較快 |
| temporal + portfolio BF16 | 87.67 s | 642.10 s | lifecycle 通過；待平衡重測 |
| encoder checkpoint=false | 97.64 s | 603.64 s | lifecycle 通過；容量 guard 實際保留重算，不能稱為 retention 收益 |
| temporal + portfolio BF16，encoder checkpoint=false | 97.10 s | 576.27 s | lifecycle 通過；未重現前一輪 87.67 s，不能接受原先收益估計 |
| compact_cuda transfer | 82.88 s | 618.48 s | lifecycle 通過；compact encoder 自有 packet cache，模式切換可能未改主要路徑，待同條件 baseline 重測 |
| packed transfer | 86.56 s | 528.44 s | lifecycle 通過；與更早 baseline 的快取熱度不同 |
| NUMA-local CPU affinity | 73.44 s | 488.96 s | lifecycle 通過；完整流程較佳的候選，仍需平衡 baseline 重測 |
| compact + temporal + portfolio BF16 | 84.06 s | 598.66 s | lifecycle 通過；三項收益不能相加 |
| 明確保留 encoder activation | — | 120.33 s 後失敗 | 第一 epoch OOM；保留失敗，未接受 |
| 關閉 encoder packet cache | 89.93 s | 534.95 s | lifecycle 通過；未勝過 NUMA 單項 |
| 放寬編碼器 precision casts | 93.94 s | 677.10 s | lifecycle 通過；未見收益 |
| maximum autotune | 83.18 s | 969.40 s | lifecycle 通過；248 個 shared-memory 超限候選被正常丟棄，canonical 重新審核通過，保留原始誤判；未勝過 NUMA 單項 |
| partition CUDA Graph | — | — | checkpoint 重算的 saved/recomputed scalar device 不一致；拒絕，未關閉 checkpoint determinism 來繞過 |
| 資金準備融合 v1 | 71.49 s | 2153.17 s | lifecycle 通過；穩態無新增圖，但變長 claim history 使首次編譯／結尾 backtest 很慢；不以穩態單項收益掩蓋總耗時 |
| 資金準備融合 v2，claim 容量分桶 | 74.12 s | 755.83 s | lifecycle 通過；比 v1 的總 fold 快 64.9%，仍有逐日變長 cohort 的結尾編譯成本 |
| 資金準備融合 v3，cohort／claim 容量分桶 | 77.48 s | 714.69 s | 遠端 7 項 CUDA oracle、完整 lifecycle 通過；穩態沒有新增編譯圖，準備成本下降；尚未證明比原基準快 |
| portfolio-only CUDA Graph | 114.84 s | 807.38 s | 遠端 CUDA gate 2／2、完整 lifecycle 通過；穩態無新編譯圖，第三／四轮 121.66／108.02 秒，沒有接受為加速 |
| transport block 32 | 83.04 s | 599.65 s | 完整 lifecycle 通過；未勝過較佳完整流程候選 |
| DDP bucket 25 MiB | 83.30 s | 687.72 s | 完整 lifecycle 通過；僅約 4.32 MiB 的 master parameter，增大 bucket 未證明收益 |
| global batch 512 | 95.55 s，觀測值 | 706.06 s | 完整 lifecycle、四輪有限非零梯度／1 optimizer step 通過，沒有 OOM；峰值 98.4% VRAM、餘裕約 0.5 GiB，未符合本輪 1.5 GiB／96% 准入設定，未接受為配方 |
| NUMA + 資金準備融合 v3 | 70.44 s | 458.17 s | 完整 lifecycle 通過，穩態兩輪 70.96／69.92 秒、零新編譯圖；已成為組合候選，仍需平衡原基準重測 |
| NUMA + 資金準備融合 v3 + CPU 1 thread | 70.50 s | 444.91 s | 完整 lifecycle 通過，穩態 71.06／69.94 秒、零新編譯圖；與 8 threads 的穩態差在雜訊範圍，保留總流程較低的觀測，仍需平衡比較 |
| legacy SDPA cross BF16，workaround 實際為 manual | 79.93 s | 482.79 s | 完整 lifecycle 通過；保守 workaround 阻止 BF16 compiled SDPA，數字不能歸因 SDPA backend 收益 |
| 套件試驗後同環境 fresh-seed 原生 baseline | 76.11 s | 491.85 s | 完整 lifecycle 通過；說明快取／時序漂移，原先 95 秒不能作最終 speedup 分母 |
| explicit SDPA MATH-only，temporal／portfolio BF16 | 94.82 s | 682.09 s | 真 CUDA 遮罩／完整梯度與完整 lifecycle 通過；正確但較慢，未採用 |
| packed SwiGLU projection | 90.22 s | 628.90 s | 完整 lifecycle 通過；算子較快沒有轉成完整模型收益，未採用 |
| cuBLASLt 明確 library selection | 86.78 s | 533.69 s | 完整 lifecycle 通過；未勝過較佳 NUMA/funding 組合 |
| QuACK softmax，FP32 blocks 配方 | 88.17 s | 563.33 s | 完整 lifecycle 通過；FP32 SDPA 分支不會呼叫外接 softmax，另測 BF16 temporal 的確定執行配方，不把此時序歸因 QuACK |
| QuACK LayerNorm | 84.95 s | 625.02 s | 完整 lifecycle 通過，穩態零新編譯圖；未勝過 NUMA/funding 組合 |
| QuACK compact BF16 GEMM | 82.11 s | 734.76 s | 完整 lifecycle 通過；首輪 322.18 s，穩態零新编譯圖；未勝過 NUMA/funding 組合 |
| Liger LayerNorm | 90.46 s | 689.74 s | 修復 private launcher/toolchain 後完整 lifecycle 通過；穩態零新圖，未採用為加速配方 |
| Liger SwiGLU | 85.97 s | 660.44 s | 修復 private launcher/toolchain 後完整 lifecycle 通過；穩態零新圖，未勝過 NUMA/funding 組合 |
| 全 BF16 + 明確保留 encoder activation | — | 98.46 s 後失敗 | 第一 epoch OOM；仍需配置 2.71 GiB，而 GPU 僅餘約 1.72 GiB；沒有回退 checkpoint 後宣稱 retention 成功 |
| FA4 temporal BF16，原始 vendor wrapper | — | 86.71 s 後失敗 | fullgraph 拒絕 vendor 的 `active_fake_mode`；保留原始失敗，另驗證官方 forward/backward 的 custom-op bridge |
| DDP 不重複廣播已驗證不變的 buffers | 83.72 s | 516.52 s | 完整 lifecycle、有限非零梯度、穩態零新圖通過；未勝過目前 NUMA/funding 組合 |
| FA4 temporal BF16，vendor custom-op bridge | 99.57 s | 640.89 s | Q=1／32 compiled 真梯度及完整 lifecycle 通過；穩態零新圖，未採用為加速配方 |
| 年度 trajectory 結尾同步參數梯度 | 88.17 s | 536.72 s | 完整 lifecycle、有限非零梯度與每輪一次更新通過；穩態零新圖，NUMA 組合另測 |
| NUMA + 不變 buffer + 年度結尾梯度同步 | 84.23 s | 487.09 s | 完整 lifecycle 通過；穩態零新圖，未勝過 NUMA/funding 組合 |
| financial compiler fusion size 16 | 89.23 s | 609.83 s | CUDA 財務 oracle 與完整 lifecycle 通過；穩態零新圖，未證明收益 |
| financial compiler fusion size 32 | 82.54 s | 599.36 s | CUDA 財務 oracle 與完整 lifecycle 通過；穩態零新圖，未勝過 NUMA/funding 組合 |
| Gregorian calendar cache | 85.10 s | 534.72 s | 真 CUDA oracle 4／4、完整 lifecycle 通過；穩態零新圖，單獨使用未勝過 NUMA/funding 組合 |
| NUMA，CPU threads 1 | 83.19 s | 485.04 s | 四輪與完整 lifecycle 通過；第三／四輪 82.53／83.85 秒，沒有接受為更快的穩態配方 |
| NUMA，CPU threads 4 | 86.41 s | 494.05 s | 四輪與完整 lifecycle 通過；穩態零新圖，未證明增加 threads 有收益 |
| NUMA，CPU threads 16 | 92.41 s | 503.37 s | 四輪與完整 lifecycle 通過；穩態零新圖，沒有接受為更快配方 |
| Gregorian calendar compiled restore | 94.45 s | 537.63 s | 修復 tracing 警告後真 CUDA oracle 4／4、完整 lifecycle 通過；穩態零新圖，未接受為加速 |
| TorchAO FP8 functional adapter | 97.21 s | 736.93 s | MNK16 尾端修復後 8 項真 GPU eager／compiled 前後向與完整 lifecycle 通過；穩態零新圖，未接受為加速 |
| DeepGEMM，原始 module 入口 | — | 107.10 s 後失敗 | standalone fullgraph gate 可執行，但 module 的 bound Function.apply 導致 setup_context 描述符錯誤；保留原始失敗，修復後續測 |
| DeepGEMM，只設定 CUDA_HOME 的重測 | — | 92.94 s 後失敗 | canonical runtime 優先使用 Python 環境的 CUDA headers 路徑，該路徑沒有 nvcc；改用既有 STOCKAGENT_CUDA_ROOT override，保留原始失敗 |
| DeepGEMM BF16，共用入口與 canonical SDK | 81.18 s | 668.75 s | 修復後 8 項真 GPU eager／compiled 前後向、四輪與完整 lifecycle 通過；穩態零新圖，未勝過 NUMA/funding |
| native FP32 last-query reduction | 85.07 s | 616.31 s | 遮罩／RoPE／完整參數 credit 的真 CUDA gate 與完整 lifecycle 通過；穩態零新圖，单項未勝過 NUMA/funding |
| NUMA + funding v3 + FP32 last-query reduction | 76.55 s | 475.39 s | 完整 lifecycle 通過，第三／四輪 80.51／72.59 秒、穩態零新圖；未取最快一輪代替中位數 |
| NUMA + funding v3，batch 384 | 78.88 s | 571.05 s | 四輪與完整 lifecycle 通過，峰值 27,600 MiB、最小餘量 4.89 GiB；未勝過 batch 256 |
| NUMA + funding v3 + neural BF16，batch 512 | 72.01 s | 553.08 s | 四輪與完整 lifecycle 通過，峰值 94.89%、最小餘量 1.63 GiB，也符合一般准入門檻；第三／四輪 73.15／70.87 秒、零新圖，未勝過 batch 256 |
| NUMA + funding v3 + packed SwiGLU，CPU 1 thread | 69.06 s | 472.80 s | 四輪與完整 lifecycle 通過，第三／四輪 68.66／69.46 秒、零新圖；穩態略低、總流程較先前 CPU1 組合長，待平衡比較 |
| NUMA + funding v3 + 延後 DDP 同步，CPU 1 thread | 70.52 s | 496.42 s | 四輪與完整 lifecycle 通過，第三／四輪 69.56／71.49 秒、零新圖；未勝過 packed SwiGLU 組合 |
| NUMA + funding v3 + packed SwiGLU + neural BF16，CPU 1 thread | 75.24 s | 603.60 s | 四輪與完整 lifecycle 通過，第三／四輪 79.97／70.52 秒、零新圖；不以最快單輪代替中位數，未勝過原精度組合 |
| NUMA + funding v3 + packed SwiGLU + FP32 last-query reduction，CPU 1 thread | 74.40 s | 545.86 s | 四輪與完整 lifecycle 通過，第三／四輪 75.03／73.76 秒、零新圖；未勝過 packed SwiGLU 組合 |
| NUMA + funding v3 + QuACK softmax + BF16 temporal | 66.36 s | 524.35 s | 四輪與完整 lifecycle 通過，第三／四輪 67.27／65.45 秒、零新圖；manual attention 確定執行 vendor softmax，鎖定作平衡比較 |
| 財務 CUDA Graph | — | — | 修復 allocator bootstrap 後 gate 3／4；recurrent trajectory 的 pending backward 警告仍失敗，沒有宣稱 replay 收益，也沒有修改年度梯度鏈 |

這些輪次的首次編譯、OS page cache 與快取熱度不平衡。穩態數字也需要反向順序的重測，不能將兩個單項的百分比相加。先前正式長訓練的暖權重時序與本次固定 fresh seed、四 epoch scheduler 不同；本表不是對該正式長訓練時序的直接速度承諾。四 epoch 驗證工程完整性，不證明策略績效改善。

一般 benchmark 配方使用 1.5 GiB／96% 的事先顯存准入門檻；這是本輪配方設定，不是「機制正確」的定義。既有 BF16 batch 512 另宣告 0.125 GiB／99.9% 的容量邊界研究門檻，仍執行完整 rows、四輪、所有驗證與產物。門檻會原樣傳到 canonical benchmark 並寫入 run-spec；控制路徑、無效值提前拒絕與既有規則回歸共 57 項通過。低餘裕但已完成的原始 batch 512 數值仍在表內，不抹去觀測。

`candle_projection_fp32=true` 在 compact 合約中保留統計與修正項 FP32，`compact_projection_compute_dtype=bf16` 已獨立控制主要 GEMM。關掉前者會被現有 compact 合約拒絕；不把該結構錯誤當成可測的「全部 BF16」配方。文內 BF16 組合始終保留 FP32 masters、這些統計與 FP64 財務 reductions。

第一輪 baseline wrapper 曾漏接 accepted launch 的 FIFO checkpoint block 設定而 OOM。已修正為呼叫 canonical `_environment`；失敗紀錄保留，並以修正後 baseline 作比較。正式 Python 環境、driver、GPU instance 與正式 checkpoint 均未替換。

本機 kernel、activation/cache、source identity、CUDA preflight、checkpoint、compact projection、panel、carry training 與 checkpoint block 回歸：308 項通過、4 項跳過。新增資金準備融合的完整財務回歸：225 項通過、4 項跳過；既有 simulator 編譯器警告的針對性回歸：15 項通過、1 項跳過。本機 RTX 5070 Ti 的 CUDA 測試與遠端雙 RTX 5090 的完整 fold 證據分開保留。這不是全專案 test suite 的替代宣稱。

後續新增 package-version fingerprint、FP8／BF16 functional adapters、非 leaf compact stem credit、FP32 master parameter、TE padding/RNG 的針對性回歸為 55 項通過；這些包含 fake API 測試，不能替代真正套件的 GPU admission。TE 實際 GPU operator gate 另加入 strict checkpoint roundtrip。資金準備分桶與 package contract 回歸 30 項通過；原始一项版本字串測試未更新的失敗紀錄亦保留。各回歸集合重疊，通過數不相加。

資金準備融合 v1 的遠端完整 trial 顯示 claim axis 每日增長，例如 `(243,2757,3)`，導致大量長度專用圖；因此不能把小型固定空帳本的單圖測試當成年度工作負載驗證。v1 穩態兩輪為 71.34／71.63 秒，完整 fold 卻需 35.89 分鐘。v2 沿用既有 minute sweep 的 claim 容量分桶，只在 compiler boundary 補零金額列，輸出還原原始列及當日新增列。CUDA oracle 已驗證 0、1、3、31、32 列共用同一圖，並逐欄比較完整帳本、現金支付及非零梯度。v2 使用新的 checkpoint fingerprint 與獨立 artifact root。

v2 已通過遠端 6 項 CUDA oracle 與完整 lifecycle，pre-epoch 準備由約 1153 秒降至 24 秒，完整 fold 為 12.60 分鐘。結尾 cache 又觀察到 `(30,2757,12)` 的 cohort axis，表示只固定 claim axis 尚不足以消除所有編譯變體。v3 同時將非空 cohort／claim 固定於容量桶，空帳本保留零長度 specialization；只移除新加的中性補位，所有原始零持倉列及其 adjoint 仍保留。v3 的本機完整財務／adapter 回歸 169 項通過，遠端 7 項 oracle 及完整四輪 fold 也通過，pre-epoch 為 15.94 秒，穩態兩輪 80.05／74.91 秒，總 fold 714.69 秒。這項總成本改善尚未證明相對原基準的穩態加速。v1、v2、v3 各有自己的 checkpoint fingerprint，不相互 donation/resume。

整個 partition 的 CUDA Graph 在 encoder checkpoint 重算時出現 scalar device metadata 不一致，因此另建只捕捉 portfolio head 的配方。encoder checkpoint 保留原路徑；head 輸出在 graph boundary 複製，讓年度 loss 保留多個前向結果直到一次反向。兩項本機與遠端 CUDA gate 均通過完整 parameter/input gradient 比較，並檢查實際捕捉的 graph nodes，避免把空 allocator capture 當成模型 capture。此配方擁有獨立 fingerprint；遠端雙卡四輪完整 fold 通過，但穩態 114.84 秒，沒有接受為加速。新增 opt-in epoch graph root／node 計數，用於後續區分重新編譯與重新 capture；讀取失敗明確記錄未知，相關 33 項回歸通過。

舊 `compiled_cross_attention_backend=sdpa` 在 SM120 的 BF16 compiled 路徑仍會被 workaround 導回 manual。因此另建 explicit backend，原 auto／manual／legacy sdpa 的行為與 fingerprint 保留。首版 v42 在本機 Torch 通過，遠端 Torch 2.11 則發現 BF16 的被遮罩 context 有 25,664 個非零 scalar gradient，屬於機制錯誤，未進入完整 fold；FP32 通過。v43 局部排除 cuDNN 後仍出現相同問題，不能將原因只歸於 cuDNN。v44 使用局部 [sdpa_kernel](https://docs.pytorch.org/docs/2.11/generated/torch.nn.attention.sdpa_kernel.html) 限定 MATH，加入 `masked_credit_math_only_v2` dispatch contract；本機 33 項回歸及遠端 FP32/BF16 2 項真 CUDA 遮罩、輸入／參數梯度檢查通過，完整 fold 已通過驗收，94.82 秒／epoch、682.09 秒／fold，較慢而未採用。

新增 FP32 last-query reduction 配方重用原有點積／加權和的直接 reduction，避免 Q=1 的兩個窄 batched GEMM。全遮罩 row 的 attention 為零，遮罩輸入不獲得梯度；以獨立 `empty_key_mask_zero_credit_v1` 合約拒絕舊 optimizer resume。另排入 NUMA/funding 搭配此路徑、batch 384 與全編碼 BF16 batch 512；只有通過真 CUDA oracle、完整 fold 與顯存准入的配方才進入最終比較。新一批 packed SwiGLU 前後向為 0.033–0.035 ms，compiled 原生為 0.044–0.046 ms；完整模型單项 90.22 秒／epoch，NUMA/funding/CPU1 組合 69.06 秒／epoch、472.80 秒／fold；BF16 與 FP32 last-query 組合分別 75.24／74.40 秒，收益不能相加。原 QuACK softmax 配方在 FP32 SDPA 分支可能沒有實際 kernel 呼叫，另加入 temporal BF16、compiled manual 的確定執行配方。

三次 Liger/QuACK 模型試驗最初因 benchmark 的 `Path(sys.executable).resolve()` 把 private venv symlink 解析到主環境，DDP child 找不到套件；這是 launcher 問題，不能作套件相容性結論。新 launcher 保留 venv path、完整 fold wrapper 明確傳入 `--python`，真 venv 的 private-only module 回歸及整組 234 項測試通過。為讓已凍結的舊 source queue 也使用正確環境，僅將 private venv symlink 原子替換為 byte-identical interpreter copy；主 binary SHA 與主 site-packages 都保持原樣。原始失敗保留，三項模型後續均已完成重測與完整 lifecycle 驗收。

MATH-only 雖通過機制檢查，完整 fold 較慢，因此另試 `sdpa_padded`：對實體 key 軸作 128/16 對齊、channel 軸作 16 對齊，補入 key 全遮罩、value channel 為零，保留原 logical scale、既有 RoPE/qk normalization，並限定局部 dispatcher 排除 cuDNN。本機 81 項測試通過，遠端 Torch 2.11.0/SM120 則在 12.75 秒的兩個 oracle 中都失敗：FP32/BF16 有 25,984/25,664 個被遮罩 context scalar 收到非零 gradient。兩個完整 fold 都取消准入，padding 不是此現場問題的修復。已加入該 runtime/hardware 的明確拒絕執行保護；含真 CUDA rejection、原生預設與遮罩/參數梯度的 82 項本機回歸通過；遠端 constructor guard 的真 CUDA 驗證也通過，拒絕機制錯誤的 runtime 路徑。非預設 checkpoint contract 與 MATH-only 明確不同。

Liger 的實際 fullgraph 另遇到 `GetAttrVariable(AutogradFunctionVariable(), setup_context) has no type`。模組保存 bound `Function.apply` 讓該版本的 Dynamo 走入 Python descriptor 的比較；改用一般函式包裝保留原 vendor forward/backward，避免更改數學或偷偷用原生 backward。新的真套件 compiled norm/SwiGLU 准入涵蓋 FP32/BF16 與寬度 32/128，通過後才重跑完整模型。

這批 8 項 compiled vendor 前後向全部通過：d32 QuACK LayerNorm 約 0.0088 ms、Liger 約 0.0147–0.0149 ms；Liger SwiGLU d32 約 0.0107 ms、d128 約 0.0126–0.0146 ms。完整模型首次重跑又暴露 private interpreter 複製後，Inductor 尋找的 conda compiler alias 不在 private `bin`；僅增加指向原工具鏈的隔離 alias，驗證編譯 shared library 與 private Python 的獨立 readback，再重跑。這些 launcher/toolchain 失敗不當作算子機制或速度結論，原始紀錄均保留。

FA4 原始官方 wrapper 的 eager/CUDA Graph 前後向通過，但完整模型 fullgraph 在 `active_fake_mode` 被拒絕。新增 unmasked custom-op bridge 直接呼叫固定版本官方 `_flash_attn_fwd`／`_flash_attn_bwd`，公開輸出之外的 LSE 明確不取得梯度，fake implementations 只描述 shape/dtype。它沒有用 PyTorch attention 代替 vendor backward，亦不關閉 fullgraph；以 `vendor_forward_backward_custom_op_v2` 合約區分 checkpoint。Q=1／32 真套件 compiled adjoint 通過後才重跑完整模型。

遠端這兩種 query 形狀的 BF16 fullgraph vendor forward/backward 均通過，前後向分別約 0.923／0.667 ms。FP32 在接口明確拒絕，並不自動改成其他 dtype。新的 checkpoint 合約回歸 23 項通過；完整模型重測繼續檢查 rematerialization、DDP 與結尾產物。

DDP 單項的遠端第一輪觀測，原生、不重複廣播 buffers、年度結尾同步三者的 train loss 都為 `0.061783164739608765`，clipping 前 gradient norm 都為 `10.438643455505371`；validation 有小幅數值差。這支持首輪未出現丟失梯度或錯誤 world-size 倍率，並不宣稱四輪 optimizer trajectory 完全相等；另保留 two-rank parity 與尾端 padding 測試。

## 原始套件來源

這些來源說明能力與限制；速度判定以本次遠端實測為準。[QuACK](https://github.com/Dao-AILab/quack)、[CuTe DSL](https://docs.nvidia.com/cutlass/latest/media/docs/pythonDSL/cute_dsl_general/dsl_introduction.html)、[FA4](https://github.com/Dao-AILab/flash-attention/tree/main/flash_attn/cute)、[DeepGEMM](https://github.com/deepseek-ai/DeepGEMM)、[Transformer Engine](https://github.com/NVIDIA/TransformerEngine)、[TorchAO](https://github.com/pytorch/ao)、[Liger](https://github.com/linkedin/Liger-Kernel)、[TileLang](https://github.com/tile-ai/tilelang)、[Helion](https://github.com/pytorch/helion)、[ThunderKittens](https://github.com/HazyResearch/ThunderKittens)、[cuBLASDx 0.4.1 requirements](https://docs.nvidia.com/cuda/cublasdx/0.4.1/requirements_func.html)、[cuBLASDx current release notes](https://docs.nvidia.com/cuda/cublasdx/release_notes.html)、[cuTile](https://github.com/NVIDIA/cutile-python)、[DeepEP](https://github.com/deepseek-ai/DeepEP)、[FLA](https://github.com/fla-org/flash-linear-attention)、[SageAttention3](https://github.com/thu-ml/SageAttention/tree/main/sageattention3_blackwell)、[SageBwd 論文](https://arxiv.org/abs/2603.02170)、[SageBwd release issue](https://github.com/thu-ml/SageAttention/issues/229)、[PyTorch 2.12 Blackwell wheel／driver 說明](https://pytorch.org/blog/pytorch-2-12-release-blog/)、[PyTorch 2.14 release](https://pytorch.org/blog/pytorch-2-14-release-blog/)。
