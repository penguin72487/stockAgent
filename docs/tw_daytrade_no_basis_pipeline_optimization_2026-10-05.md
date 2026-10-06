# 不分解 fold11：完整工作流效能修正（2026-10-05～06）

## 1. 執行進度

- 使用者目前正式 checkpoint 已保存 epoch3；epoch4 被使用者停止。正式 last checkpoint SHA 為 `5aa8acf1cf84c18c1adab6d0d90c9f2247fe987514148b51b071c9ac65ecfb64`。本次工程工作不啟動正式 1,000 epochs、不改寫正式 optimizer。
- 已完成單 batch 原始 GPU packet cache、以有界 VRAM 換 encoder 重算、canonical CPU runtime 修正、移除單 fold 多餘外層 isolation。舊來源／設定／資料／optimizer 保留；只回補被量測的共享模組及目前主 repo 已有的 Numba mask 修正，沒有整份替换成並行修改中的 main repo。
- 已發布並獨立驗收新 frozen source `2147fccf74daf9b7740c92723baca9996f47666165e3183494c1f97d8fc304e5`，code／receipts 在遠端 `artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_runtime_optimized_v1`。同一使用者前景入口已切換至此來源，但正式產物仍寫原 `...no_basis_v3/training-bf16`，**預設 resume、profiling／debug timing sync 關閉**。
- 已完成雙卡真實 14,726 通道的搬運／模型診斷，以及兩份完整三輪 fold11。**小型 model-only 診斷不當作完整 fold 加速或正式 promotion。**
- 完整原模式三輪 fold11 總 wall **1,396.809 秒 → 最終模式 721.961 秒，少 48.31%**。第三輪最大 rank **325.936 → 194.183 秒，少 40.42%**；三轮 train／val／test loss 逐值相同。雙卡六次 optimizer 精度 oracle、完整三轮產物驗收、真實正式 epoch3 的隔離副本續跑 epoch4 均完成；正式 checkpoint 未寫入。
- 嚴格 canonical bit comparator **未通過，保留拒絕，不放寬**：最終模式模型 max abs `1.192e-7`、Adam `1.118e-8`、requested weights `3.166e-8`。RNG／scheduler／scaler／epoch 位元一致；70 個 NPZ arrays 僅兩個 requested weights arrays 不同。成交部位、報酬、分鐘 NAV、turnover、費用、結算等相同，三個 parquet 的 numeric fields 另以 bit comparator 驗證一致。採用依據是原精度 oracle 與實際金融結果，不宣稱 optimizer 逐位元一致。
- 無基底／無 SVD／無 window-RMS 的 CUDA 直接投影以 VRAM guard 保留 activations；不足自動用舊 checkpoint。真實 14,726 通道雙卡診斷輸出差 0、梯度 max abs `7.451e-9`（native repeat 亦同量級），model-only peak 約 17.7～18.6 GB／rank，**不是完整訓練 peak**。相同 actualCPU4 下，cached/checkpoint model-only 中位數0.4065秒、保留 activations0.3267秒；最終選擇仍以完整三轮為準。
- 正確 `inference_mode` ABI 下 actualCPU4／24 的完整 val／test roles MAX wall 中位數13.065／13.409秒。chunk8省下約0.5秒但引入 FP64 rounding；保留原 chunk32，沒有為小幅收益改金融分塊。只用 `no_grad` 的早期探測與 thread-count admission 失敗收據保留為拒絕證據，不能當正式速度結果。
- 遠端實際重現 frozen v3 的共用 OpenMP 問題：requested4，panel 前 Torch4／Numba224，panel 後 **Torch224／Numba224**。新來源回補目前 canonical `train.py` 已有的 Numba 初始化／mask 修正，並讓所有 profile 呼叫同一 helper、驗證載入後實際值；此前只寫 requested threads 的小型測速不能當成 CPU4／24 實際配置比較。完整舊 A/B wall 仍是原入口真實觀測。
- 同一遠端入口 `--check-only` 已退出0：三份 exact source full-SHA／7日 leases／training pins、formal resume／model manifest、strict CUDA（0 failures／0 warnings）通過。最後 scoped 回歸92項通過、1環境 skip（22.15秒）；入口／resume gate151項通過（7.20秒），兩組有重疊，不相加成唯一測試數。修改模組 Ruff、shell syntax、py_compile、`git diff --check` 通過；未宣稱整個維護 test suite 已跑完。
- 收尾時再次驗收canonical冷庫重建的54檔／full SHA／7日lease與18項完成gate；原工程路徑為immutable還原alias、正式checkpoint SHA未變，GPU owners為空。[重建驗收收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-runtime-optimized-v1-metadata/completed-candidate-cold-restore-acceptance.json)。

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

上述舊 frozen 模式存在「requested 與實際 thread pool 不同」：來源內舊 `train.py` 還沒有目前主 repo 的 Numba mask 初始化，首次 panel kernel 將共享 Torch OpenMP pool 改成 224。表格的 CPU threads 因而是 **requested**，不是已驗證的 actual；不能據此選 CPU budget。修正版 diagnostics 同時记录 requested／actual，並在 panel／dataset／model 建好後 assert；整輪接受仍使用修正版實際雙卡完整 fold。

## 6. 完整 A/B 與恢復驗收

工程 runner 沿用原 `train.py`、GPU leases、strict CUDA preflight、canonical checkpoints、`validate_completed_training_artifacts`。對照和候選都全新獨立 optimizer、三 epochs／early-stop0、相同 seed／global32／全部輸出，profiling／debug timing sync 關閉。它們不能續接到正式 optimizer。

先以真實 panel 建 canonical manifest，唯讀驗證正式 epoch3 checkpoint 的7項 semantic fingerprints；再做雙卡六次 optimizer oracle。完整三轮透過 canonical lifecycle 驗證18項必備產物，另外54個完整工作流檔案仍保留；第三轮 Dynamo新增圖為0，warmup cache misses為0。每輪 train_optimizer_steps=1，工程 final optimizer step=3。

| 真實雙卡完整工作流 | 原入口 | 只加 packet cache | 最終 runtime 組合 |
| --- | ---: | ---: | ---: |
| 3轮＋setup＋final backtest／分鐘曲線／全部圖表／驗收 | 1,396.809 s | 1,314.049 s | **721.961 s** |
| epoch3 最大 rank | 325.936 s | 291.075 s | **194.183 s** |
| epoch3 train 最大 rank | 193.088 s | 160.478 s | **181.939 s** |
| epoch3 validation角色 | 88.246 s | 78.307 s | **11.852 s** |
| epoch3 sampled-test角色 | 132.462 s | 130.242 s | **8.525 s** |
| 完整 wall 減去三轮 MAX wall（setup／final／其餘） | 394.550 s | 392.642 s | **129.845 s** |
| epoch3 checkpoint save | 0.332 s | 0.325 s | 0.353 s |

validation／test 角色並行，不相加成 epoch wall。checkpoint本來已不到0.5秒，不是瓶頸；最終不是每項都比中間候選更快：train181.939秒比只加cache160.478秒慢，完整驗證／final快得多，仍以使用者要求的整輪／整fold選擇。分項CPU wall與nested CUDA工作有重疊，不能相加作獨立kernel耗時。此表是同模型工程三轮的實測，未預測正式1,000轮的整體時間。

計時邊界是canonical `train.py` 前景子程序與其完整三轮／最後產物，不含額外六步工程oracle、獨立比對、source release發布、冷庫還原或使用者入口的三份full-SHA續租。這些另有實際成功收據，但沒有全部混入721.961秒；不能把trainer的48.31%當成從資料首次下載到正式開訓整條鏈的已量測加速比。

最終第1／2／3轮 MAX 分別183.443／214.490／194.183秒；第1轮新增12 graphs，第2與3轮新增0。保留1～3轮真實波動；不能以單次最小值宣稱持續速度或理論下界。

本次對照工程產物在驗收期間被既有 completed-training cold-ingress 服務封存後移除熱副本，不是資料遺失。以 canonical materialization／7-day lease 完整 SHA 還原同一 snapshot：`artifact-auto-complete-fold-control-5c55-20261005T163613716308655Z-l0-penguin-e8ee1ad5a2f4c54f`。54 files、inventory `e8ee1ad5a2f4c54ff0b147802a1de1c62e4caad862875d01576b5a080d10e6c8`；只對本次兩個工程根目錄加 recovery hold，不停全域清理、不改使用者正式 checkpoint、不刪 cold payload。receipt `packed-edge-1791218545843350413.json` 驗證 full／READY，lease 到 2026-10-12。

最終[獨立接受收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-runtime-optimized-v1-metadata/fold11-runtime-optimization-acceptance.json)、[完整工作流時間](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-runtime-optimized-v1-metadata/complete-fold-candidate-receipt.json)、[嚴格 bit 拒絕](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-runtime-optimized-v1-metadata/full-artifact-parity.json)、[量化差異](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-runtime-optimized-v1-metadata/full-artifact-numeric-differences.json)。第一個只加 cache 候選的相同類型證據仍保留在 `remote-runtime-optimization-metadata`，沒有覆蓋。

CUDA native **梯度探測**重跑存在 reduction 雜訊；未另跑完整fold的native A/A，不能把這項局部證據宣稱為全訓練浮點差的唯一已證實原因。没有修改原 oracle 容差，不拿 BF16 寬容差取代金融精度規則。本次三轮金融結果一致，不保證未来1,000轮的非連續整張成交閾值永遠相同。

遠端工程產物在 `artifacts/markets/...no_basis_payload_cache_v1` 與 `...20261006_no_basis_runtime_optimized_v1`。兩個精確 recovery holds 保護legacy／bulk retirement scope的工程根與正式入口引用的frozen code；**completed-run return是另一個既有owner，完成fold的熱副本仍會在驗收後封存**，不能把hold描述成停止所有回收。沒有停全域整理服務。

最終三轮fold也由completed-run owner回傳54檔／91,062,598bytes、26objects並獨立驗證後移除hot原副本；不是資料遺失，正式checkpoint與新程式不受影響。固定snapshot`artifact-auto-complete-fold-candidate-31-20261005T173054841747172Z-l0-penguin-56125ad0b9f326ed`，manifest`327e4cd9631827602a152f1ba7319b7bb114c4f934d860358bee96cd34d25243`。沿canonical materialization與7日lease重建，原工程路徑使用其readonly symlink；不是新optimizer，不能將此還原副本當正式續跑目錄。原`...no_basis_v3`正式模型／optimizer／資料、cache、舊來源和舊入口均保留；不把「同名／相似」當作可刪證據。

目前實測瓶頸不是 best-val 報告 callback（所選設定的兩個 best-val artifact 開關皆為 false），而是每輪保留的 physical FIFO val／sampled test。原模式第2輪 train 196.327 秒，val 88.384 秒／test 137.490 秒並行；總 max-rank 334.265 秒。分項為未同步 CPU wall，不能當成獨立 GPU kernel 時間。

第二個候選的記憶體 guard 使用 driver free + PyTorch inactive reusable bytes，預留兩個完整 FP32 slab、16 倍 narrow temporal activation bytes及原設定 safety margin；不足便使用舊 checkpoint 路徑。保留 activations 時不另外保存 GPU raw packets。原預設仍使用 checkpoint，不把本次形狀的 VRAM 策略強加給其他模型。新設定放在完整 source／config receipt 中；模型／輸入／optimizer 語意 fingerprint 沒有更換。

### 6.1 真實續跑與中斷驗收

只複製正式 epoch3 的 last／best checkpoint與curve到隔離工程輸出，`--resume` 真實雙卡續跑 epoch4，optimizer step3→4，curve epochs1、2、3、4無重複，四轮圖同步生成。canonical log 明確恢復Python／NumPy／Torch RNG；scheduler／AMP與checkpoint模型沿用原存檔。保存epoch4後用canonical owned-child reaper傳SIGINT；全部子程序／GPU owner退出，隔離輸出的failed/SystemExit130是**預期中斷**，不是完整fold，不拿它取代上方三轮的完成驗收。

此測試沒有使用`--epochs4`：原 warmup/cosine scheduler horizon由epochs1000決定，縮短成4會改fingerprint。首次admission主動拒絕這個不相容測試設定，尚未啟動GPU或複製optimizer；改為原1000設定＋一轮後受控中斷。驗收器一度誤要求非必填`interrupted`欄位，修正為實際canonical failed/SystemExit130 envelope後做**唯讀補驗收**，未重跑GPU、未回填假的before receipt。

隔離epoch4 MAX217.654秒，不能當cold／steady A/B速度證據；當次whole-job wall未保存，沒有補造。已驗證隔離checkpoint manifest仍可續epoch5；正式last SHA仍為本報告首段的原值、正式curve三轮前綴及best checkpoint與複本一致。詳見[實際隔離恢復收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-runtime-optimized-v1-metadata/isolated-formal-resume-epoch4-receipt.json)。

### 6.2 使用者同一前景指令

直接在vastai1T：

```bash
cd /root/stockAgent
bash artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_v3/train_vast_no_basis_fold11_v3.sh
```

已更新同一入口；有相容正式checkpoint就續跑，沒有才新建，完成fold按canonical gate跳過。仍只fold11／雙卡global32／1000epochs及原早停，所有輸出寫原`...no_basis_v3/training-bf16`；source code／runtime receipts在新的`...20261006_no_basis_runtime_optimized_v1`。此入口未放背景，本次没有替使用者啟動正式訓練。

前置`--check-only`驗證真實正式manifest、精確source/config/feature SHA、三份canonical full-SHA leases與long-training pins、CUDAstrict，不重建多GBpanel來驗證resume。訓練本身仍走原sharedloader與manifest gate，没有跳過來源驗證。入口備份`train_vast_no_basis_fold11_v3.pre_runtime_20261006.sh`保留；SHA`20c3af2be44463468b0c909f6f46c2504fd238721b880a65de65b7b044a01acb`。

## 7. lower bound 與未宣稱的事

給定目前 dense 模型，至少要讀取決策窗口真正非零的觀察，還原完整輸入並完成既有矩陣運算；給定既有 chronological ledger／gradient horizon，時間方向的狀態有相依性。移除 backward 的第二份 H2D 達到「已快取 raw chunk 的重搬運為 0」這個局部下界，不表示 FP32 encoder、FIFO、驗證與整體工作流已到硬體理論極限。

最終guard容許保留activations時，encoder forward/backward原始來源read次數88→44，省掉第二次dense還原／重算，不是只加`non_blocking`。以47 unique rows先投影，再構造32日narrow windows的canonical路徑已避免對重疊raw windows重複投影；本次没有把 dense 線性投影的數學複雜度說成零，也没有改FIFO遞推。後續可能的exact fusion、NUMA／PCIe placement與更多現代kernel仍需新實測；本次没有聲稱已窮盡所有library／algorithm或達到global lower bound。

只換低精度、稀疏模型、減少 stock/features、改 batch／optimizer cadence、跳過圖表／驗證，均不是本次等價加速。compile cold start 與持久 cache 命中另外報告；不同 workload 的 wall 不混為加速證據。

官方技術參考：[PyTorch activation checkpointing](https://pytorch.org/blog/activation-checkpointing-techniques/)、[pinned memory 與 non-blocking 傳輸](https://docs.pytorch.org/tutorials/intermediate/pinmem_nonblock.html?highlight=cuda)。實際可用 API、效果與精度仍以本機 Torch 2.11.0+cu128／RTX5090 實測為準。
