# 最新台股 panel：數值-only／BF16 遠端訓練驗收（2026-10-05）

## 1. 執行進度

- 已按最新要求移除模型輸入的可用性、資料年齡、更新旗標：58,886 → **14,726 通道**，沒有刪除任何已選定的經濟數值。
- 遠端逐位元投影、原始 NULL／資格／成交限制保留、新版資料 gate 與容量 admission 均通過。
- 本機 **200 項回歸通過**；新版雙 RTX 5090 compiled／dense oracle，兩 rank 各 **3 次 FP32＋3 次 BF16 optimizer 更新通過**，原誤差門檻不變。fixture 為 4 通道、3 股票，不等於完整 panel 驗收。
- **固定資料版本的遠端工程訓練就緒已通過。** 真實 **14,726 通道、2,757 商品、fold11、雙卡 DDP、全域 batch32** 的完整工作流退出碼 0；2026-10-05 08:28:51 UTC 獨立驗收亦成功，18 個 canonical 生命週期產物、兩個嚴格 checkpoint、有限參數及每輪圖檔均通過。[完整驗收收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-value-only-metadata/fold11-acceptance-bf16-v4.json)。
- 工程上限 3 輪依原 `early_stopping_no_improve_ratio=0.1` 得到 patience1，**實際跑兩輪後正常早停，第 2 輪沒有新編譯圖**；不宣稱跑完三輪、不修改正式最多 1,000 輪的 patience100。正式長訓練未啟動，工程 optimizer 不帶入正式訓練。
- 原 PCA partial baseline 和 checkpoint 92／89 失敗已保留；完整 canonical fold-owned 統計快取 **672.53 秒**，v4 的獨立 compiled graph ownership 修正經真實寬 panel 的兩輪 forward／backward 及最終產物驗證。
- 已在兩張 RTX 5090 實際執行原生 NVFP4 核心、精確 ones oracle、原生量化／forward／backward／DDP／各六次 AdamW 更新。此形狀 NVFP4 比 BF16 慢且誤差較大，未升級主訓練精度。
- 新增 PCA 調度、cache／checkpoint 與生命週期回歸 **211 項通過**；不與先前 200 項重疊測試直接相加。
- compiled 分區／DDP 的 scoped 設定恢復、新 trajectory 指紋及舊 optimizer 拒絕檢查後，最終相關回歸 **214 項通過**（18.25 秒；與先前測試重疊，不相加）。8 股票、131 通道、stock tile3（末段2）、最大參數 8.79 MB 超過 4 MiB bucket 的舊路徑 oracle 仍通過，故這個中型 fixture **沒有重現**實際寬 panel 的錯誤；不能以它確認根因或替代完整驗收。
- v4 擴大 oracle（相同 131 通道／8 股票／tile3）兩 rank 各六次更新通過，梯度最大絕對差 **0.0004638778045773506**；另外 scope 及「缺少 graph contract 的舊 optimizer 在 resume／artifact 皆被拒絕」五項針對性測試通過，沒有放寬容差。
- 未啟動正式 1,000 epoch 訓練，未刪來源、舊 view、快取或 optimizer。

本頁是記錄時點的交付報告，不是自動刷新服務。

## 2. 資料形狀與使用者最新覆蓋

邏輯模型輸入是 `[日期, 商品, feature]`：3,109 個交易日、2,757 個商品、14,726 通道；固定來源截止 **2026-10-02**。

| 輸入 | 數量 | 磁碟形狀／用途 |
| --- | ---: | --- |
| 基本價格／量／開盤 gap | 6 | 延續選定 v10 的 canonical 執行價格及決策契約 |
| 商品個別經濟數值 | 14,655 | 個別 `[T,S,F]` 壓縮日期／商品分塊，只解碼需要的 slab |
| 共享經濟數值 | 65 | 只儲存一次 `[T,G]`；模型取 batch 時共享到股票軸 |
| 可用性／年齡／更新旗標 | **0** | 44,160 個狀態通道不再輸入模型 |

同義合併、來源選擇、住宅／無意義 metadata 排除、不同更新頻率的既有規則保持不變。原始 NULL、公布時間、來源授權、TTL、報告／發行人生命週期 reset 及交易安全證據仍保留在 raw／稽核資料，不作為模型輸入。

訓練 view 使用最近已公布的有效數值；沒有觀測或已被 TTL／生命週期屏障阻擋的模型數值仍用中性零。這不等於原始資料的真實零值。按本次要求，模型不再收到辨別「未觀測」與「真零」的額外狀態通道。

不同頻率仍由已固定的 release clock 控制可使用日，不是把日頻股票與低頻財報全部強制變成每日更新，也不是撤掉執行資格或缺價 gate。資料仍為本人授權的私人研究包：`research_only=true`、`historical_point_in_time=false`、非 live／broker 成交認證。

## 3. 無損投影與儲存效果

採用共享轉換器 `stockagent/data/factorized_panel_projection.py`，不重建已驗證來源。先全成員驗證 parent，再從證明的四欄群組保留 value；新檔獨立解碼，比對每一個保留值的 Float32 位元，包括負零。來源、raw NULL、企業行動和 execution evidence 以同檔案系統不可變名稱重用；parent manifest 沒有改動。

- parent manifest SHA256：`e7be0c72fe6394ecf01140d4587c7c412de370e502f51fa05bb97cd2b4d746f3`。
- 新 manifest SHA256：`33bed11d08665f5578b6ef11f3689ee80c99b22b260f661683d3e35eb21d9f50`。
- 新 ABI：`tw_factorized_nullable_panel_values_lag1_v2`，`model_channel_policy=value_only`。
- 4,224 個別壓縮 parts：17,244,621,609 → **2,667,280,861 bytes**，減少約 **84.5%**。不是整份 source bundle 的大小。
- 完整投影＋全部成員驗證＋4,666 頁 Markdown 清冊：**1,024.08 秒**；不是純 codec 測速。
- 14,720 選定 quantities 不變；報告中的 451,548 排除紀錄不是 451,548 個獨立經濟特徵。
- 新來源／feature ABI／精度旗標和獨立 output，不續接舊 58,886 通道 optimizer。

遠端完整清冊：`prepared/feature_report/`；逐值收據：`prepared/value_projection_receipt.json`。

## 4. BF16 的實際邊界

主路徑維持 CUDA BF16 AMP，BF16 不使用 GradScaler。原始金融數值、master parameters、Adam states 仍是 Float32；會計／loss 的敏感累加保持既有穩定精度。**TF32 關閉不代表 BF16 Tensor Core 關閉。**

不是「整個模型都永遠 BF16」：basis contraction、時間區塊、compact attention／配置 head 是明確 FP32 區塊。先前不同 stock chunk 形狀的 BF16 rounding 經 Adam 更新被放大，不能以放寬 oracle 或捏造一致性處理；三個 precision contracts 都受 checkpoint 相容性約束。

可編譯的 Tensor 運算分成 effective-kernel、stock encoder、compact head 三個 region；I/O、字典、byte cache 留在 eager 邊界。AOT backward autocast 明確限定為 off，`emulate_precision_casts` 保留宣告的型別 rounding，cudagraphs 關閉。保留每個特徵、22 個 basis families、lookback32、embedding32、無 24 維瓶頸、原 topology／交易公式。

v3 另在這三個已獨立分區的 invocation scope 設 `optimize_ddp=False`，避免只在 DDP forward 活動範圍內重新依 bucket 切圖、到 checkpoint 重算又用不同 graph；scope 結束及例外均恢復設定。外層仍是 canonical DDP，仍保留相同全域 batch／gradient all-reduce、non-reentrant checkpoint 及重算檢查，不關閉 CUDA compile 或留存全部股票 raw GPU slab。這是目前候選根因修正，需完整寬 panel backward 證明；[PyTorch DDP 編譯邊界](https://docs.pytorch.org/docs/2.11/notes/ddp.html)、[上游相同錯誤討論](https://github.com/pytorch/pytorch/issues/144035)。新 `compiled_partition_no_ddp_resplit_v1` 指紋拒絕接續舊 optimizer。

新版數值 oracle 最大梯度絕對差 **0.00022125244140625**，兩卡各六次 optimizer 更新通過原門檻；這仍是小型工程 fixture。

## 5. 原生 NVFP4／BF16 操作數實測

NVIDIA support matrix 對 SM12.x 的 NVFP4 訓練是有條件支援，需要關閉 RHT 與 stochastic rounding；MXFP8 在該族群未列為可用。不能把 Blackwell datacenter 的 throughput 或預先量化 GEMM 數字當成 RTX5090 全模型結果。[官方支援矩陣](https://nvidia.github.io/TransformerEngine/support_matrix.html)、[官方量化開銷／形狀說明](https://docs.nvidia.com/deeplearning/transformer-engine/features/low_precision_training/speedups.html)。

本次兩 GPU 的 PyTorch 2.11.0+cu128、SM12.0 實測：`torch._scaled_mm`，真實 `float4_e2m1fn_x2` operands、E4M3 block scales、BF16 output，M=N=128、K=256，輸出 exact 256。沒有用 NF4／fake quant 代替原生 NVFP4。ones operands 不包含 quantization 或 gradient，故只證明原生 kernel 能執行。

另測實際寬度 14,726 的 lag-batched operator：每 rank batch16、stock tile32、L=D=32，包含型別轉換、forward、backward、兩卡 mean-gradient allreduce，三次穩態 maximum-rank wall：

| 隔離運算 | 三次 steady wall | 誤差對 FP32 |
| --- | --- | --- |
| 原 FP32 | 6.368／6.384／6.333 ms | reference |
| BF16 operands＋FP32 accumulation | 5.438／5.293／5.379 ms | output relative L2 約 0.234%–0.237%；gradient 約 0.288% |

約 16% 是此 synthetic operator 的速度差，**不是完整 epoch 加速**。目前 `bmm(out_dtype=Float32)` 沒有內建 autograd derivative，初次探測因此被拒絕；第二次隔離探測提供明確的 candidate STE 梯度，保留失敗紀錄，沒有接入主訓練。

進一步 source-built Transformer Engine 2.19.0 的真實原生訓練操作數探測，使用兩 RTX5090、`M=512,K=14726,N=32`、Float32 master、各六次 AdamW、包含量化、padding、forward、dgrad／wgrad、DDP 與 optimizer。兩 rank 原生 FP8／NVFP4 的 fallback 均為零，並保存 CUDA profiler kernel 清冊：

| Linear 精度 | 三次穩態最大 rank 中位數 | 第一個 step output 相對 L2 | weight gradient 相對 L2 |
| --- | ---: | ---: | ---: |
| FP32（TF32 off） | 1.097 ms | 0 | 0 |
| BF16 AMP | 1.204 ms | 0.329% | 0.422% |
| 原生 FP8 | 1.668 ms | 3.65% | 4.39% |
| 原生 NVFP4 | 2.136 ms | 14.52%–14.63% | 19.26% |

此小輸出寬度下，低位元省下的 GEMM 成本不足以抵銷量化／padding／dispatch 成本，NVFP4 比 BF16 操作數完整 step 約慢 77%，**沒有作為主訓練設定**。這不是整個 FinancialTransformer 的誤差或 epoch 時間，也不是投資報酬比較。NVFP4 的 RHT／stochastic rounding 關閉；FP8 使用原生 current scaling、dequantized backward。

目前大型 basis 使用 functional weight contraction，不經 `nn.Linear.forward`。僅換成 Transformer Engine Linear，不會覆蓋這個瓶頸；只有原生操作數驗收也不能替代整個 fold 測量。候選低位元路徑不自動升級正式精度。

## 6. 記憶體、CUDA 與 artifact 空間

- 兩張 RTX5090、各約 32.6 GB VRAM；容器 CPU quota 約 53.76 核，並非主機可見 224 核都可使用。
- 新模型參數上界 **248,061,198**；Float32 master parameters **992,244,792 bytes**。
- canonical Adam group-last、原子替換、fold best、buffers、4 GiB 報告預留共需 **11,240,913,634 bytes**。這是 checkpoint 磁碟 gate，不是已通過 VRAM gate。
- v4 即時顯存取樣約 **11,898–20,928 MiB／卡**；完整兩輪及 final artifacts 沒有 OOM，但沒有逐階段 allocator 峰值紀錄，取樣不當作全程峰值。驗收完成後遠端 filesystem 可用 **158,278,979,584 bytes**（約 158.28 GB）；不是永久容量保證。
- 全成員 SHA 掃描會把 clean 文件 pages 放入 cgroup cache。admission 只對兩份已驗證 derived panel 的 8,448 個壓縮 parts 作 scoped `POSIX_FADV_DONTNEED`；不 drop node-wide cache、不刪檔。保守 headroom 實測由 **2,044,583,936** → **22,015,307,776 bytes**，超過 host admission seed **9,500,682,352 bytes**。檔案 advice 的邏輯大小不當作可回收記憶體證據。
- 三份 exact sources 經 canonical cache use 完整驗證與七日續租；長訓練採 canonical packed source pin，而非違反 edge 的七日 TTL 上限。pins 位於 materialized root 的 `.training-pins/`，不放 GC 不掃描的 `.cache-state/`；保留到日後由 owner 按既有回收流程處理，不在訓練中斷時自動刪除。packed payload 由既有 rclone owner 傳輸，Syncthing 傳索引。未以 SCP 再傳金融來源。獨立 NVIDIA 公開軟體 distribution 可另作 full SHA code delivery；不會改動主訓練環境。
- 工程完整生命週期驗收遵守既有早停規則：最多三輪、實際兩輪，第 2 輪零新 graph 僅作診斷基準，**不等於效能升級要求的第 3 輪以後穩態比較**；冷啟動、RMS／PCA、compile、train、validation、test、curves、同步圖、checkpoint 和 final artifacts 分別記錄，不靠省略工作換速度。

### 6.1 真實 pre-epoch 統計與快取

先前 rank0 的 PCA 尚未完成就累積 **55,913.52 CPU 秒**；rank1 等待，未建立 optimizer。這是 partial baseline，不是完整舊工作流的時間。

對真實 14,726 通道的早期／中期／近期三個日期-商品 tile 比較 1／4／8／24 個執行緒與 1／8／32 個 lag batching；所有候選的抽樣 covariance 差值為零。不同測試輪次有變動，沒有把單次最快數字當成整個 fold lower bound。v2 只在 PCA scope 使用單執行緒，lag batch 保持 1；退出或例外均恢復原 thread pool。其他模型／舊設定的預設仍不動。

完整 canonical fold11 transform 準備，訓練年份 2014–2024，驗證／測試 rows 未使用：

| 階段 | Wall | CPU |
| --- | ---: | ---: |
| base panel＋全成員驗證與 factorized attach | 7.01 s | 15.07 s |
| training-only PCA 與 22 家族 basis metadata | 493.40 s | 491.38 s |
| training-only RMS／active mask | 171.54 s | 171.40 s |
| 完整 preparation | **672.53 s** | 各階段 CPU 如上 |

PCA 524 選定 basis rows 的時間軸 rank 為 31，符合 lookback32 去 DC 的上界，不是新增 24 維 feature 瓶頸。RMS 在此 fold 的訓練區間辨識到 **10,984／14,726** active channels；沒在訓練期間充分出現的欄位仍保留原 schema、靜態歸零，不能把未訓練的隨機投影權重用於測試。這不是額外可用性／年齡／更新輸入通道。

準備沿用 `_fit_group_temporal_basis`、`_fit_group_causal_feature_rms` 及其 verified content-addressed caches。正式與工程 root 都從 exact source／training indices／契約核對 cache；不同 fold 或 schema 不共用錯誤統計。這 672.53 秒列入冷準備成本，不能以之後 cache hit 假稱完整冷工作流零延遲。

### 6.2 完整形狀第 1 個 epoch 與測量邊界

v4 的兩個 fold-owned transform 均 cache hit，pre-epoch 為 **63.664 秒**。第 1 個 epoch 的最大 rank 為 **1,082.569 秒**，其中訓練最大 rank **722.500 秒**，83 batches、一次完整 chronological AdamW 更新；val 與 test curve 分別在兩 rank 並行計算，不能把其時間直接相加。

rank0 的 model forward 約 **3,924.918 ms／batch**，backward 約 **3,895.898 ms／batch**，loss 約 **422.037 ms／batch**。factorized `stock_chunk` 的解壓與 H2D 在 forward／checkpoint 重算內執行，因此 `train_fetch=172.374 ms／batch` **不是整個來源讀取成本**，CUDA event 也可能包含等待 CPU 補入 stream 的空檔；不能將這些值全解讀為純 GPU 算力。

validation **349.728 秒**、test curve **348.266 秒**（並行）；checkpoint 合計 **10.298 秒**。第 1 個 epoch 新增 15 個 Dynamo graphs，含首輪 train／eval warmup。`epoch_max_rank_s` 的記錄邊界不含之後同步繪圖 callback；完整 supervised run wall 才包含所有 plotting／final artifacts。第 2 個 epoch 記錄的最大 rank **951.183 秒**、訓練 **669.644 秒**、新增 graphs **0**。profile 日誌的 `total=1163.830s` 是階段總和，並行階段可能重疊，不等同 wall；同步圖 callback **0.567 秒**另列。沒有把 cold compile 或 callback 從整個工作流刪掉。

### 6.3 最終全工作流驗收與穩態成本

完整 supervised run 從 **07:41:09.830760 UTC 到 08:25:37.855346 UTC**，**2,668.025 秒／44.47 分鐘**；包含 CUDA strict、容量與資料 gate、擴大雙卡 oracle、主訓練兩輪、每輪驗證／測試與同步圖、checkpoint、最佳模型最終回測和總報告。先前 CPU-only preparation 的 672.53 秒、來源發布／傳輸和原生低位元獨立探測另列，不能宣稱包含在這 44.47 分鐘。

| 階段／量測 | 第 1 輪 | 第 2 輪（零新圖的診斷基準） |
| --- | ---: | ---: |
| train 最大 rank | 722.500 s | 669.644 s |
| epoch 最大 rank（plot callback 前） | 1,082.569 s | 951.183 s |
| model forward，rank0 | 3,924.918 ms／batch | 3,337.727 ms／batch |
| backward，rank0，含 checkpoint 重算 | 3,895.898 ms／batch | 3,893.635 ms／batch |
| loss，rank0 | 422.037 ms／batch | 372.851 ms／batch |
| 新 Dynamo graphs | 15 | **0** |
| synchronous curve callback | 0.565 s | 0.567 s |
| full-trajectory AdamW 更新 | 1 | 1 |

最終 fold11 best-checkpoint 驗證＋測試階段 **204.171 秒**，其中 final-test **108.808 秒**；fold save／plot **43.174 秒**（save 41.822 秒、plot 1.346 秒）。獨立嚴格驗收 **39.144 秒**。兩張卡 master parameters 與 Adam 狀態不使用永久 BF16 儲存，group-last 的實際 Adam step 是 2；最佳 checkpoint 來自 epoch1。每輪 loss／timing PNG 均讀取／繪出兩筆，並實際目視確認。

短程工程結果的 test cumulative return 是 **−6.915%**，未收斂，不當成正式訓練品質或可獲利證據。這次沒有證明 theoretical lower bound，也沒有把單次零新 graph、partial CPU baseline 或 synthetic operator 的收益宣稱為全工作流極限加速。

## 7. 遠端目錄與前景入口

全部新產物在 `/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_gaprepair_v4`。

- 最新精確 v4 code SHA256：`a9a3a36e6c40b4ea03d1b1267e034d501f29fd61b50471e7e9017a6d72661ce8`，`code-bf16-v4/`；source bundle SHA256：`8f166217de556c80e08757f1ffade3807c8c5ca428e5c1574777dcb75b4782fd`。1,335 個 frozen 成員已在遠端驗證。
- 原 v1／v2／v3 code／receipt 不動，保留追溯；v3 已有 scope 修復，但 v4 才把 graph 版本納入 strict training resume／artifact contract。
- 新設定：[value-only BF16 v4](../configs/deployments/tw_day_trade_factorized_values_20261005_gaprepair_v4_bf16_v4.yaml)。
- canonical base-panel／physical-cache 仍從原不可變根目錄重用；不是再解出一份全部來源。
- 最新受控真實測試：`smoke-bf16-v4/`，正式新訓練：`training-bf16-v4/`，不共用 optimizer。舊 `smoke-bf16-v2/` 失敗資料保留；CPU-only `prefit-bf16-v2/` 只建立 canonical 統計快取，沒有模型／optimizer。
- `training-bf16-v4/` 已由 canonical `--check-data-only` 留下唯一檔案 `startup_timing.jsonl`（`active_strategy=none`、world size1），不是正式訓練產物。驗收／入口只允許這個 preflight 紀錄；任何 run manifest、checkpoint、其他檔案或目錄均阻擋，不刪除或覆寫舊紀錄。
- NVIDIA 可選精度依賴僅放 `precision-overlay-v3/`，正式入口不會載入它；既有失敗 `precision-overlay-v2/` 保留。

在 vastai1T 的前景入口已建立：

```bash
bash /root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261005_gaprepair_v4/train_fold11.sh
```

**完整訓練與 checkpoint 驗收 gate 已通過。** 入口仍會驗證 `fold11-acceptance-bf16-v4.json`、source／config／manifest、exact leases、CUDA strict 與 GPU owner locks，只選 fold11、DDP兩卡、batch32、最多 1,000 epochs；啟動時透過 canonical cache owner 續租相同來源七天，並建立精確版本的長訓練 pin，不切換資料版本、不重新重建整份 panel。不用 nohup／tmux 代替使用者啟動正式訓練，也不覆寫或接續工程 optimizer。入口 preflight-only 檢查已成功、退出碼 0，沒有呼叫正式 train 命令。[精確版本 pin 收據](../artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-value-only-metadata/foreground-preflight-pins.json)。

訓練完整性與研究獲利結論分離；本次工程驗收不保證報酬，也不宣稱所有歷史公布版本與企業行動缺口已消失。

## 8. 定位工程證據

遠端原 workflow 根目錄保留於 `tw_day_trade_factorized_panel_20261005_gaprepair_v4/workflow/runs/`，避免改變既有 task 身分：

- `project-value-only-native-panel--20261005T062419-d7761d3f`：全 panel 投影、退出碼 0。
- `value-only-fold11-full-bf16-v2-20261005T064053-a7556fa6`：新資料 gate、oracle 通過；pre-epoch PCA partial baseline 主動取消，未建立 optimizer。
- `value-only-fold11-canonical-pref-20261005T071149-6ac95c83`：完整同資料 canonical PCA／RMS 快取準備。
- `value-only-fold11-full-bf16-v3-20261005T071502-3df18812`：PCA／RMS 均 cache hit；pre-epoch 84.15 秒；完整三 epoch 驗收在 epoch1 第一個 backward 失敗（checkpoint 92／89），退出碼 1，保留原 trace。
- `wide-ddp-checkpoint-baseline-v3-20261005T073500-4185be23`：131 通道、8 股票的原 scope 六步 oracle 通過；沒有重現真實寬 panel 的 checkpoint 錯誤。
- `value-only-fold11-checkpoint-v3-20261005T073804-d2b8c686`：資料 gate 通過，但 oracle worker 不在 source bundle 的錯誤路徑，尚未進入主訓練即失敗；沒有向任何程序發出取消訊號。
- `value-only-fold11-full-bf16-v4-20261005T074109-254fb019`：worker 路徑／strict graph contract 修正後的新完整驗收；真實 panel 完成兩個 epoch、零新 graph，依原規則早停；含所有 final artifacts，退出碼 0。
- `value-only-fold11-artifact-accep-20261005T082809-97d40f7f`：canonical 生命週期／嚴格 checkpoint／有限 Float32 state 驗收，退出碼 0，`training_ready=true`、`formal_training_started=false`。
- 先前 `value-only-fold11-artifact-accep-20261005T082249-708de4b8` 獨立驗收只建立 feature panel，未接分鐘成交標籤及 physical carry source，被 data fingerprint 擋住。補上與 `train.py` 相同的 canonical loaders 後，重算 data fingerprint 精確回到 `48ea726547adbc452b62a9e57a01faeccdcd5903549c8ef0befd9934b18c60ce`；沒有忽略資料契約或更改訓練模型。
- `formal-entry-missing-receipt-neg-20261005T081540-7ae9d588`：缺少完整收據時前景入口退出碼 1、正式 preflight 檔 SHA 未變，負向檢查本身退出碼 0；第一次負向 fixture 將 preflight 目錄誤判為正式產物，未呼叫入口即被 assertion 擋住，收據保留。
- `foreground-preflight-only-fixed--20261005T083312-f25ebbcd`：preflight-only 在違反七日 TTL 上限的 30 日要求被拒絕，尚未到 CUDA／GPU／訓練階段；修正為七日續租＋canonical pin，既有 TTL 上限不變。
- `foreground-preflight-only-fixed--20261005T083612-90ae4ff9`：七日續租、三份精確 canonical pins、CUDA strict 全部通過，退出碼 0；沒有啟動正式 train 或取得正式 optimizer。
- `native-te-dual-gpu-training-prob-20261005T071401-11b0ca51`：真實 native FP8／NVFP4 反向與 DDP 操作數探測，退出碼 0；沒有升級主模型。
- `native-basis-precision-dual-gpu--20261005T063431-8d2ddc77`：原生 NVFP4 與 isolated BF16 candidate，退出碼 0。
- 本機 [200 項回歸](../artifacts/operations/agent-workflow/runs/value-only-data-projection-regre-20261005T061832-7235e194/run.log)。

取消的舊 58,886 通道工作、只等待投影的舊 pipeline、第一次 out_dtype 探測失敗及軟體下載／delivery sequencing 收據均保留，不改寫成功、不刪除診斷。
