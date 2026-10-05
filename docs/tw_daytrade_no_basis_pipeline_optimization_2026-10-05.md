# 不分解 fold11：完整工作流效能修正（2026-10-05）

## 1. 執行進度

- 使用者目前正式 checkpoint 已保存 epoch3；epoch4 被使用者停止。正式 last checkpoint SHA 為 `5aa8acf1cf84c18c1adab6d0d90c9f2247fe987514148b51b071c9ac65ecfb64`。本次工程工作不啟動正式 1,000 epochs、不改寫正式 optimizer。
- 已完成來源／搬運基線、實作有界的單 batch GPU 原始封包快取、相關 checkpoint／DDP／resume／生命週期回歸：188 項通過。Ruff 與修改模組的 py_compile 通過。
- 新 source release 保留原 frozen v3 全部來源，只改四個 code files；原 release、資料與 checkpoint 均保留。候選 source SHA：`ce9b09276a13e36bc7c5ea1d1fd604a7b2ec09b3281bfccc3cf396dfb2c843ec`。
- 已完成雙卡真實 14,726 通道的搬運與模型診斷；完整三輪 fold11 對照／候選、獨立金融與產物一致性驗收正在執行。**小型 model-only 診斷不當作完整 fold 加速或正式 promotion。**

## 2. 不變的研究與訓練邊界

資料仍是 3,109 交易日、2,757 個商品、14,726 個 value-only 通道；沒有加入年齡／可用性旗標，沒有 SVD、PCA/KLT、temporal basis 或 24 維 adapter。

FinancialTransformer 1,133,582 個參數，lookback32／embedding32／latent attention／零 dropout。BF16 AMP 保留 joint stem、temporal、portfolio、金融計算的明確 FP32 islands，以及 FP32 master／Adam；不為加速偷偷改成 TF32、永久 BF16 或 NVFP4。

雙 RTX5090 DDP、global batch32、每個 chronological trajectory 一次 optimizer step。既定 first-minute-only 50% 容量、official-close 不限容量 reduction-only、exact physical FIFO、企業行動遮罩、費用與資金規則都不變。每輪驗證、sampled test、同步 epoch 圖、checkpoint、最後完整回測／分鐘曲線／報告都保留。

此 dataset 原本就是 `research_only=true`、`historical_point_in_time=false`。加速不消除其資料／歷史企業行動研究限制，也不代表投資效益。

## 3. 真正的成本與重複工作

正式 run 的 profiling 已關閉。保存 epoch3 的 rank0 診斷 wall 為 216.957 秒，其中 train 201.792 秒；forward 約 1,091.52 ms／batch、backward 693.57 ms／batch、loss 434.75 ms／batch、fetch 73.56 ms／batch。這些未同步 GPU 的區間不能相加成 GPU kernel 時間；當時也沒有最大 rank 指標。

真正的 model-only 雙卡診斷採 formal local16、47 個 unique slab rows。44 個 stock chunks 在 forward 讀一次，activation checkpoint backward 又讀一次，合計 88 次。原模式每個 rank 每 batch 傳約 1.694 GB 的無損 compact observations，還原後完整 dense 邏輯張量合計 15.265 GB。

原單 fold 入口另外先由父程序建 panel，再進 isolated child／torchrun，由 rank0、rank1 依既有協調機制建／載入 panel。使用者選定只跑 fold11，可以直接使用既有 `--no-isolate-train-folds` 移除多餘的外層父程序，不改 trainer／DDP，也不取消 worker 的資料驗證。

## 4. 實作：搬運一次，重算重用

新增 `data.factorized_transfer_mode: compact_cuda_cached`。同一個 forward 的 slab 暫存原始 GPU byte packets，而不是 dense input、learned embedding、模型輸出或 loss。

- 每次原始 packet 仍來自既有 verified storage rectangles，還原成完整 FP32 dense tensor後才進既有 compiled encoder。
- backward 重算同一區塊時，省掉第二次讀來源、host staging、pin 和 H2D；仍執行完整 dense 還原與原模型重算。
- 每 slab 的快取上限為 `min(max_slab_bytes, 2 GiB, 當下 free VRAM / 8)`；超額走既有傳輸，不丟資料、不改輸出。
- cache 只活在該次 forward／checkpoint 的 slab。`.to()`／padding 會建立新 slab，不跨 batch、device、optimizer step、fold 重用；`no_grad` 評估不建立不必要快取。
- CUDA packet 在使用的 autograd stream 登記 lifetime，避免 allocator 過早重用。

GPU packet 本身位元不變；輸入仍保留 signed zero。不同 CUDA 梯度 reduction 的 hash 不必完全相同，因此另與 native dense repeat 比較原有容差，不能把輸入位元一致寫成所有梯度位元一致。

另延伸共享 epoch curve recorder：即使 `--no-profile-timing`，每輪仍將兩個既有 wall scalars 做一次 DDP MAX，記錄 `epoch_max_rank_s`／`train_max_rank_s`。沒有逐 batch CUDA events、debug sync 或 hot-path fence。

## 5. 小型雙卡診斷（不是金融完整 fold）

同一份正式 epoch3 checkpoint、global row2048、warmup2／repeats3、沒有 optimizer update、沒有 trace。

| 模式／每 rank CPU threads | batch 最大 rank wall（三次，秒） | rank0 H2D bytes／batch | backward packet hit |
| --- | --- | --- | --- |
| 原 compact_cuda／24 | 0.5261、0.5179、0.5221 | 1,693,696,880 | 0 |
| packed／24 | 0.5263、0.4990、0.4776 | 1,693,697,248 | 0 |
| cached／24 | 0.4114、0.3825、0.3753 | 846,848,624 | 44 |

cached／24 中位數比原模式低約 26.7%，H2D 減半；GPU peak allocation 約 0.52 → 1.37 GB。所有實際 44 raw chunks（1,908,180,354 個 FP32 values）對 native dense input 位元一致；輸出差 0，參數梯度通過原有 BF16 oracle。這是線性梯度探測，**沒有 recurrent financial loss／optimizer／完整產物**，不能直接外推整個 epoch 的幅度。

CPU threads 的數字指每 rank。正式入口原 host-wide48，canonical DDP 會分成每 rank24，不是兩個 rank 各48。沒有以可見224 logical CPUs冒充容器可用 CPU。

## 6. 完整 A/B 與恢復驗收

工程 runner 沿用原 `train.py`、GPU leases、strict CUDA preflight、canonical checkpoints、`validate_completed_training_artifacts`。對照和候選都全新獨立 optimizer、三 epochs／early-stop0、相同 seed／global32／全部輸出，profiling／debug timing sync 關閉。它們不能續接到正式 optimizer。

先以真實 panel 建 canonical manifest，唯讀驗證正式 epoch3 checkpoint 的 resume semantic fingerprints；再做雙卡六次 optimizer oracle。之後完整比較 epoch3 最大 rank、全部 fold wall、startup／pre-epoch／validation／test／final回測／plot／checkpoint，並獨立驗證金融曲線、metrics、daily weights、模型／optimizer。結果與 promotion 狀態待完成後更新。

遠端工程產物都在 `artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_payload_cache_v1`，不刪原 `...no_basis_v3` 或其他使用者資料。

## 7. lower bound 與未宣稱的事

給定目前 dense 模型，至少要讀取決策窗口真正非零的觀察，還原完整輸入並完成既有矩陣運算；給定既有 chronological ledger／gradient horizon，時間方向的狀態有相依性。移除 backward 的第二份 H2D 達到「已快取 raw chunk 的重搬運為 0」這個局部下界，不表示 FP32 encoder、FIFO、驗證與整體工作流已到硬體理論極限。

只換低精度、稀疏模型、減少 stock/features、改 batch／optimizer cadence、跳過圖表／驗證，均不是本次等價加速。compile cold start 與持久 cache 命中另外報告；不同 workload 的 wall 不混為加速證據。

官方技術參考：[PyTorch activation checkpointing](https://pytorch.org/blog/activation-checkpointing-techniques/)、[pinned memory 與 non-blocking 傳輸](https://docs.pytorch.org/tutorials/intermediate/pinmem_nonblock.html?highlight=cuda)。實際可用 API、效果與精度仍以本機 Torch 2.11.0+cu128／RTX5090 實測為準。
