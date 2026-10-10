# Scale-separated cash 全流程吞吐量優化（2026-10-07）

## 1. 執行進度

已就緒：年度fold10／global256（local128）／2 GiB stock stream已通過真實雙卡oracle、完整3 epoch及18項lifecycle產物，第3輪MAX rank **91.761 s**、26.580有效日/s、穩態零新graph／failure／fallback；最小GPU／RAM餘裕9.419／54.201 GiB。相對同年度控制132.130 s，epoch縮短30.553%、吞吐量增加43.995%。正式入口已`ready`，新年度入口與舊b128相容入口的實際`--check-only`皆exit0，沒有啟動正式1000 epoch訓練。512不安全而未採用；所有工程GPU已退出、兩GPU回到2 MiB，沒有刪產物。

已完成硬體、config、來源與 E09 優化盤點、新head同年度雙卡控制與有界候選。256是本次測過的2冪次中最快且通過安全門檻的batch，不宣稱所有可能演算法的絕對下界。使用者最新選擇為**開年度重置，2014 只作上下文，訓練2015–2024／fold10，validation2025／test2026不變**。新版另開相容root，不接舊連續帳戶optimizer、不啟動正式長跑、不刪舊產物。比較使用相同目標日期的控制，沒有把排除2014的耗時差算成算法加速。

第一輪觀察：rank0冷路徑、rank1重用page cache的panel/physical驗證合計169.855 s；pre-epoch42.450 s。compact projection真實14726-feature BF16+TF32 output／所有參數梯度已通過；static ABI修復後完整128通過，但135.700 s/epoch未比134.496 s明顯改善。舊傳輸batch256峰值96.86%／僅1.001 GiB餘裕，已停止精確自有工程job，不聲稱OOM或完整穩態。分塊transport雙GPU逐位一致；training closure釋放已65 CPU tests＋真實雙卡兩種optimizer cadence驗證。接下來測年度版、48 GiB/rank decoded LRU與大batch，不重疊GPU工作。

操作途中另查到原 config 依賴的 `tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_v1/config.yaml` 已不存在；本任務沒有刪除它。控制組啟動時已保存 fully resolved config，仍可追溯並繼續測試。新候選用自包含 resolved config，不偽造原檔 SHA，也不要求恢復所有已清掉的工程產物。

| 已核對項目 | 目前證據 |
| --- | --- |
| Vast GPU | 2 × RTX 5090，單卡 32607 MiB，盤點時無訓練 GPU 程序 |
| CPU / RAM | EPYC 7663；224 logical CPU 不是可用預算：cgroup 約 53.76 CPU；RAM headroom 約 179.9 GiB |
| 空間 | overlay 可用約 610 GB，這個任務不需要刪資料 |
| 模型 | 無基底、無 SVD、無瓶頸、非稀疏；32 維、last-only、market-token；新 head `score_entmax_scale_separated_cash` |
| 輸入 | 6 base + 14655 個股 + 65 共用 = 14726；value-only、lag1，研究資料不宣稱歷史 PIT／實盤可用 |
| 最新訓練 | 年度重置；2014上下文、fold10／2015–2024訓練、2025 validation、2026 test；雙卡DDP；global256/local128 |
| 精度 | BF16 AMP、FP32 master/Adam、TF32；現有 projection/temporal/portfolio FP32 islands；財務 FP64 不降精度 |
| source | 正式固定`5369520914dd643554ab53692c5aac1d52df49ad9af596ea12bba1131df5539a`及單獨SHA-bound config；不是直接跑有無關修改的工作目錄 |
| 正式 resume | 新annual root首次初始化、後續canonical resume；不捐贈工程或舊fold11 optimizer |

任務收據：`tw-scale-cash-throughput-20261007`。新測速／trace放Vast `artifacts/benchmarks`，新凍結來源放`artifacts/code_releases`；正式模型放`artifacts/markets`。初期已存在的legacy工程目錄不在本任務移除／搬遷。

## 2. 第一性原理與測速口徑

總耗時拆成：來源／環境驗證 → panel/normalizer → DDP/compile warmup → 每 epoch 的 model/loss/backward/DDP/optimizer → validation/test → checkpoint/曲線 → final replay/report/flush。

降低其中一段只會改善它在 critical path 的部分；CPU 包住非同步 CUDA 的時間不能相加當作 GPU 百分比。大 batch 減少呼叫與 lookback 重複，但也增加活化與 recurrent-state 空間。若大 batch 迫使 stock chunk 縮小、重算變多，吞吐量可以下降；「顯存塞得下」不是最佳證據。

比較同 source/data/head 的完整工程 fold；warmup 與穩態分開，主要比較 epoch3+ **較慢 rank** 的完整 epoch／有效交易日每秒，再看從啟動到 lifecycle 驗收完成的 wall time、兩卡峰值／餘裕。模型 probe 僅用來定位，不替代完整驗收。小差異不反覆跑多輪；OOM 是該 shape/策略的結果，不證明其他大 batch 不可能。

財務與執行沿用所選 config：50% 容量、first-minute-only remainder、13:30 official-close unlimited reduction-only、FIFO、whole-trajectory once-per-epoch optimizer、完整 270 分鐘帳務與曲線。微小浮點差可接受；離散成交／mask／公司行動與 absorbing ruin 不可偷改。batch/BPTT 或精度策略若影響 optimizer 軌跡，使用匹配 fingerprint 與新 root。

## 3. 方法清單與實驗順序（先分析，再逐輪實測）

| 優先 | 瓶頸假設／方法 | 降低的工作量 | 測試及停止條件 |
| --- | --- | --- | --- |
| 0 | 新 head global128 完整 3 epoch 控制組 | 建立當前口徑；重用 E09，避免重測已否決的方法 | canonical trainer、雙卡、18 項 lifecycle artifacts；前兩 epoch warmup、第三 epoch 穩態 |
| 1 | global256/512 的 checkpoint／stock-chunk 空間；控制 dense packet live set | 減少 batch、lookback overlap、Python/kernel launch；防止大 batch 反而過度切碎 | 先 capacity，成立才完整 3 epoch；OOM 正確回收、不得影響正式 checkpoint |
| 2 | 寬特徵投影的重複計算／搬運，compiled encoder partition | 14726→32 是主要寬矩陣運算；評估共用列重用、精確 compact contraction、normalize/gather 融合 | 先讀真正 CandleEncoder：LayerNorm 的零欄並非可直接丟棄；對 output/gradient oracle，再 DDP 全流程 |
| 3 | FP32 islands 的 BF16/TF32 路徑與 attention backend | Tensor Core 利用率、casting／saved activation；不動財務 dtype | 分模組量測 output/gradient 差異，核對 native SDPA 的 qk/rope/mask/dropout 語意；差距過大不部署 |
| 4 | eval model batch / ledger batch 分離，compiled shape 與 encoder recomputation | 評估沒有 backward，不應照搬訓練 live set；減少尾批與多次 stock loops | eval32/64 按顯存測；test／curve／final replay 不省略，數值／欄位保留 |
| 5 | SHA/panel/RMS/cache/compile 啟動路徑；相同證據避免串行重讀 | 有界平行 I/O、不可變 cache 重用、只編譯當前 partition | 保留完整 source SHA/receipt/alignment；cache 命中不冒充冷啟動；不得改其他 storage 工作 |
| 6 | checkpoint、plot、final reports 的有界重疊／批量 I/O | 隱藏非 GPU critical-path CPU/I/O，減少逐日重複重播 | canonical lifecycle flush/exception/resume tests；同 18 artifacts，不少圖、不少 checkpoint |

先改最多占比且有實際 trace 支持的 canonical owner，前一輪已接受的 certified-flat FIFO、FP64 compact reduction、compact CUDA packet cache、TF32、block16 checkpoint 持續保留。已被 ComputeSanitizer 否決的 CUDA scan/index 融合不重開。不得將使用者已取消的基底/SVD/稀疏、NVFP4 量化或縮特徵當作純效能修正。

## 4. 邊界與待完成

新 head 控制組已通過雙卡、完整 3 epoch 與 lifecycle 驗收：較慢 rank 的 epoch3 **134.496 s**，train122.015 s、19.726 有效訓練日/s，啟動到完整 fold **695.377 s**；兩卡 peak18,102/18,140 MiB、最小餘裕14.128 GiB。穩態零新 graph／compile failure／flat fallback。Root：`tw_scale_cash_throughput_20261007/control_b128/batch_0128/attempt_01`。

epoch3 的未同步 host 呼叫區間（只做定位，不相加冒充 GPU 百分比）顯示 model forward 約3 s/batch、loss約1.4 s/batch、backward約1 s/batch；optimizer/checkpoint 不是主要限制。物理區塊非零個股欄中位1602、95th2331、最大4378（logical14655）；runtime union 仍需實測。

上一輪不同 head 的 global128：完整 epoch3 138.080 s、train 125.846 s、19.213 訓練日/s、完整 3 epoch/fold 691.534 s、peak19.08 GiB。它是方法線索，**不是**新 head 的基準，也不是機器理論 lower bound。這次僅一次新控制組，之後做有因果假設的候選；尚不能保證有限測試達到所有可能演算法的絕對極限。

後續每個里程碑補：source/config/hardware identity、工程 root、有效工作量、完整 wall/epoch/rank/VRAM、output/gradient 誤差、accepted/rejected 與下一 unresolved bottleneck。

## 5. Compact projection 編譯與數值迭代

候選保留全部 F 個 LayerNorm 的 mean/variance、gamma/beta 與 omitted-column gradients；僅將 FP32 零欄矩陣乘法代數收縮，不修剪資料／模型參數。不支援的基底、SVD、window-RMS、BF16 stem 必須拒絕。所有 source packet 原始值仍通過 bitwise codec 驗證。

| 隔離候選 | 結果與原因 |
| --- | --- |
| 全動態 encode (`compact_ddp_oracle`) | 拒絕：兩 rank 的 TorchInductor reduction tiling assertion；不是數值驗收、沒有正式部署 |
| 只動態 column width (`compact_feature_dynamic_oracle`) | 拒絕：編譯器 arithmetic-intensity guard 的 constraint solver 失敗；不以 eager fallback 掩蓋 |
| 靜態 power-of-two width bucket (`compact_bucket_oracle`) | 雙卡可編譯並 backward；TF32 compiled contraction 對 eager FP32 的最嚴格 oracle 拒絕，第一 rank 首步 output 最大差 5.719e-5。不是 full-fold 驗收 |

目前先以 `highest` 檢查代數／梯度一致性，再用正式 BF16+TF32 配置與 **同一個 compiled dense 控制組** 的真實 14726-feature batch 比較，保留原 BF16 oracle 容差與誤差明細。這兩種證明分開；不能把關閉 TF32 的 fixture 通過宣稱正式精度策略已驗收。固定 bucket 只添加傳輸 zero padding，sentinel 不成為模型特徵，也不改 F 分母；全寬時回到 canonical dense stem。

靜態 CPU-union v1 的 `highest` 六步雙卡 oracle 已通過（max gradient7.868e-6），但真實 row2048 BF16+TF32 probe 約4.91 s/batch，其中 NumPy union/scatter 約4 s，**拒絕此效能版本**。rank0 數值通過、rank1 有1/176448 output 超過原容差：0.000336196（relative1.025%）；没有提高容差。工程工具原本數值例外會令另一 rank 等 NCCL barrier，已中斷精確 PID 的自有工程 probe、修正為先寫雙 rank 明細再共同退出，不影響正式訓練。

新 v2 改用 canonical lossless rectangle packet→GPU column union，CPU 不生成／scatter 大 union，也不把 union 存進2 GiB packet cache。observed-column beta 保留在同一 GEMM 輸入，減少 TF32 input-rounding 的操作移動。raw packet CPU 重建逐位驗證（跨日期／stock／reordered／padding／negative zero／全寬 fallback），新六步雙卡 `highest` oracle 通過、max gradient9.179e-6；正式精度真實 probe 仍待。

更新：v2 正式 BF16+TF32、row2048/global128/local64 真實 probe 已雙 rank 通過；全部14726通道 raw chunks 逐位相同，既有 output/gradient 容差未變。max output abs rank0/1=0.000449046/0.000377744；DDP parameter gradient max abs0.000902414、max relative-L2=0.001716334。不是 bitwise model/optimizer 等價，也不保證1000輪帳戶軌跡相同。模型 probe 約0.5–0.8 s/batch（重算source read44次但pack22次，其餘22次cache hits）；只是定位，尚不外推完整fold speedup。

source receipt：`code-release/20261007T010707421337Z-f6ed7c05d454/release.json`，SHA `f6ed7c05d454d599aec146b4e8366bdb850c2f84d444283719651a6b222cf162`。新 flag disabled 的舊 optimizer fingerprint 不變；opt-in affine v2／compiled graph v4 要新 root。完整128/256/512/1024/2048工程測速位於 `compact_batch_scan_v2`；每個候選3輪、epoch3 MAX rank、同18項完整產物、獨立optimizer與VRAM驗收。

完整 member SHA 操作測速：4730檔、實際6,129,305,397 bytes，同一個已由原 DDP 全檔驗證暖過的來源；1 worker5.563 s，4 workers1.729 s（不是 cold disk／完整啟動加速證明）。所有 raw/model members、路徑邊界、checksum 皆保留，預設仍1 worker，選定入口才顯式使用有界4/rank。

## 6. 完整工作流揭露的 compile／RAM 瓶頸

`compact_batch_scan_v2` 在 train/eval、full/tail stock、不同 power-of-two column buckets 的有限 ABI 超過 Dynamo 預設8 variants 時警告後退回 eager。即使 exit0／模型 probe 過關也不能接受；因此精確核對自有 helper/torchrun cmdline 後停止整個掃描，保留 `compact_batch_scan_v2_stop.json`，沒有操作正式訓練、任意 CUDA PID 或其他服務。

compact encoder 的局部 budget 由 logical F 推導：`4 × (F.bit_length()+1)`，F14726 時最多60；只涵蓋 full/tail stock × train/eval ×有限 width buckets，不改全域 default。fullgraph 要求編譯失敗／超限直接失敗，完整工程 epoch3 仍必須零新 graph；測速器也明確拒絕 Dynamo cache-limit warnings，不把 exit0 當成功。這是有界 ABI 管理，不是放寬全域上限來掩蓋無限重編譯。

manifest individual records宣告的 uncompressed bytes 加總50,963,895,756 bytes，可能包含raw/model重複紀錄，不等於唯一觀測值或實際resident；manifest 僅給2 GiB／rank LRU，可能每輪反覆解壓。新增可選 `data.factorized_host_cache_bytes`，預設仍 manifest 值；沒有改 source/manifest、dtype、observations、semantic fingerprint。候選48 GiB／rank只存原始 FP32 block，需實測兩 rank cgroup memory.current 與最小餘裕，先不部署。RAM telemetry 包含其他 node 工作與可回收 file cache，不能冒稱每 rank RSS。

完整重跑：`artifacts/benchmarks/tw_scale_cash_throughput_20261007_static_abi_v4/batch_scan`；source `b51f805cb9b1e5ea33a382b37791009bcf4fa8e14c2dc470d69dbdb570239638`。各候選3輪、eval16、4 SHA workers／rank、同18項 lifecycle、epoch3 max-rank；本輪暫用原2 GiB decoded LRU，避免把RAM快取效果混進編譯修復／batch比較。v3 工具封裝缺 `stockagent/storage_layout.py` 已拒絕；v4 明確凍結此現有 placement 依賴，不關掉 guard。

本機入口另有未提交的 storage-layout 修改，改用 `run_relocated_training.py`；遠端仍是舊入口。本輪保留這份重疊使用者變更，先以凍結來源／獨立工程 root 測速，最後只接到有證據、相容的新正式 root，不覆蓋舊optimizer或還原他人的路徑整理。

v4 batch128 完整18項 lifecycle已接受：epoch3 MAX135.700 s、train MAX123.581 s、19.550有效日/s、complete fold582.058 s，GPU peak18,208/19,164 MiB、最低headroom13.128 GiB；cgroup peak134.816 GiB、最低RAM餘裕106.948 GiB。零steady新graph／failure／fallback。**epoch沒有比134.496 s控制組明顯改善，不因model-only probe0.5–0.8 s而宣稱whole-epoch加速。** complete wall有cache/初始化差異，不解讀為 cold-start 算法收益。

模型 probe 先前反覆使用同一批日期，decoded host blocks 因而保持熱快取；不能代表整個epoch的新日期與雙卡等待。核對後，forward內`.to()`會產生新slab，GPU packet cache主要重用同次forward／checkpoint backward，不是跨epoch的全資料GPU常駐。新增`--fresh-packets`可區分外層slab的生命週期，但仍是同批日期，**單靠此旗標不能消除host cache偏差**；只有完整真實epoch可作最終吞吐量證據。

下一個空间候選：完整dense FP64 exit-price/capacity/270 marks ABI不改，僅以16-row chunks搬運 padded raw transport packets、散射進完整結果。原temporary為O(B×padded_events)，新temporary為O(K×padded_events)，K16；最終dense必要O(B×S×270)仍保留。0／1／2／16-row源位元、NaN／negative-zero、空日、非contiguous、不同event長度、autograd fallback共19 tests通過；待雙卡CUDA／真實batch與完整fold，不能先宣稱512可跑。

本輪config/compact/member/attention/checkpoint/lifecycle/resume共有258 focused tests通過。較廣來源測試另發現既有inference completion marker要求`model.pt`，但該模式只讀`checkpoint_best.pt`且禁止重寫model的衝突；它不在此選定`post_train_infer=false`熱路徑，本輪尚未修改，不宣稱全test suite無錯。transport fixture原本錯將新增可選entry_path/stop_hits填成vector，已按其None預設修正；凍結entry規則沒有刪除。

## 7. 大batch的來源生命週期與年度重置

舊batch256只測到工程部分：warmup epoch1 271.006 s、兩GPU峰值30,822／31,582 MiB，超過選定90%／3GiB門檻。`unsafe_transport_stop.json`保留精確helper/torchrun cmdline、GPU／cgroup telemetry與rank清理證據；未執行其後512/1024/2048，也沒有把中斷結果標為完整acceptance。

`profile_day_trade_transport.py`以兩GPU、48 rows、2757 symbols、120分鐘事件fixture比對原transport與16-row block：每個field／每個dense cell、NaN與signed-zero SHA相同，CPU staging未變。rank0 allocated峰值2,251,477,504→1,714,606,592 bytes（只減temporary，完整270 marks與FP64價格／容量未變）。它是transport-only proof，不是模型／DDP epoch加速證明。證據：`artifacts/benchmarks/tw_scale_cash_throughput_20261007_transport_v5/rank_{0,1}/oracle.json`。

真正的training熱路徑另有closure生命週期浪費：`batch_loss_fn = bind(...)`先算右側，上一批closure仍保留全FP64 GPU tape。現在先釋放上一批closure，再建立下一批；保留小型carried account state、原backward、BPTT及optimizer cadence。CPU weakref regression與真實NCCL/DDP的per-batch／whole-trajectory兩種cadence均通過，模型參數與獨立CPU reference及兩rank一致。雙卡proof：`artifacts/benchmarks/tw_scale_cash_throughput_20261007_lifetime_ddp_v6/attempt_02`，每rank2 tests通過；fixture不替代完整年度fold。

共用年度分段原本先搬完整parent GPU tape，卻不用它，又重解碼／搬運每年child。改為先從CPU calendar找年界，child用同一個staging的`row_slice`，parent不造GPU tape。獨立年帳戶loss與gradient oracle、無部位／未付claims檢查、來源日序與padding驗證仍保留；只消除重複資料讀取／暫存，未改年份權重或財務加總順序。當時原設定annual=false，此共用修復不冒充該舊熱路徑的加速；使用者現在明確啟用年度重置，才納入新版測速。

新版`configs/deployments/tw_day_trade_factorized_values_20261007_scale_separated_cash_annual_v1.yaml`以可獨立解析的repository config鏈保留原模型／來源／BF16+TF32與scale-separated head，設定`day_trade_training_annual_episodes: true`、`split_start_year: 2015`、`start_fold: 10`、canonical resume與新root。2014缺年初lookback，不偽造2013特徵或把2月說成年初；使用者已選擇2014作上下文。年度重置只作用於training research accounts，validation/test依原連續account lifecycle，並不自動年度清空測試帳戶。

closure／row-slice／annual相關65 tests通過（3 GPU opt-in skipped）；真實兩GPUtests補足DDP路徑。來源SHA `0a088d4fdac12b8e90de98bb29b6ca4a0fd77b991bfdc6ea2b4bd9c432d3ed13`，receipt `artifacts/code_releases/tw_scale_cash_throughput_20261007/20261007T014417130756Z-0a088d4fdac1/release.json`。新版完整annual 3-epoch lifecycle／最適batch仍待；不啟動正式訓練。

## 8. 年度版完整流程揭露的前處理shape問題

`annual_v8/control`在第1輪不同calendar segment長度（例如85→43）累計超過Dynamo預設8 variants，compiled backtest prep嚴格失敗；退出1、沒有eager掩蓋、候選未啟動。它不是缺資料，也不是完整annual基準。原始trace與372.040 s失敗wall保留，不能拿來作加速分母。

共用前處理只做逐日normalize／mask，不含跨日帳務，因此改為共用可變row count圖、固定symbol axis，cache key不依每個年界長度分裂。recurrent account scan的原compile mode、實際日期、FIFO、dtype與gradient均不改；沒有提高全域Dynamo上限或padding假日期。12種長度含singleton的CPU／兩GPU CUDA output及gradient oracle通過，再跑第2遍零新graph；CPU其他相關162 tests通過，兩rank各1 CUDA test通過（9.07／9.08 s）。

`annual_v9`控制／候選皆含這項必要修復，控制只保留原dense projection／2GiB LRU／原transport；候選才加入compact／48GiB LRU／16-row staging與closure/year-slice。source控制`955e6fea80e4c7937891229bb1d254b5603e5df2655889ffec6d82045f4f863c`，候選`e2fea7a4e92a2475488ef1cce44aad42f2fb88420c911a21bc629b423b7ae20d`。年度版仍以完整3輪、第3輪MAX rank與18項lifecycle驗收；最適batch仍待實測。

入口沿用`run_relocated_training.py`，保留原fold11的固定resume contract；新annual fold10只能憑完整accepted DDP receipt、相符config/source/feature SHA及新root進入。首次沒有checkpoint可初始化，後續仍canonical resume；不讀取／捐贈工程optimizer。不接受僅warmup、single-GPU、fallback、缺plot／checkpoint或不足GPU／RAM餘裕的工程收據。

`annual_v9`控制仍繼承搬遷前的physical cache路徑，因而重建1606個分鐘session；這是本次placement遺漏，不是來源真的缺失。canonical新位置已有相同release、manifest SHA `8daeb8d2b419b1e832ccc0a5b4c416c68ce9e2c2ecae06fb16146238d3b5078e`、1609檔／11,482,877,666 bytes的READY。為避免浪費與比較不同cache狀態，已核對PID/cmdline停止自有v9工程tree、確認全部rank/helper退出；保留partial cache及stop receipt、不刪資料。`annual_v10`控制與候選均使用新`artifacts/cache`路徑，canonical loader仍驗證全cache內容，未用READY直接冒充全內容驗證。

最新focused主路徑回歸348 passed／4 opt-in CUDA skipped；前處理／backtest／loss另162 passed（部分重疊）。3項舊entry測試原先抓shell inline Python而失敗，已改測canonical placement runner，補上manifest/base schema、缺feature與new-annual-init gate；沒有把測試放寬成只看exit0。47 entry／layout／annual tests通過。

## 9. 相同年度契約的完整測速

| 方法 | global/local batch | epoch3 MAX rank | 有效日/s | 完整3輪fold | 最小GPU／RAM餘裕 | 驗收 |
| --- | --- | --- | --- | --- | --- | --- |
| 原dense／2GiB LRU／原transport，必要prep修復 | 128／64 | 132.130 s | 18.459 | 525.136 s | 13.771／104.196 GiB | complete lifecycle通過 |
| compact／48GiB LRU／block16／closure釋放／year-slice | 256／128 | 121.423 s | 20.087 | 572.949 s | 9.692／45.842 GiB | complete lifecycle通過 |
| 同上 | 512／256 | 未完成 | 未完成 | 未完成 | 0.513 GiB GPU | 安全門檻拒絕，精確停止 |
| 同上＋2 GiB bounded stock stream／ABI v6 | 256／128 | 91.761 s | 26.580 | 806.950 s | 9.419／54.201 GiB | complete lifecycle通過，正式採用 |

控制root：`artifacts/benchmarks/tw_scale_cash_throughput_20261007_annual_v10/control/batch_0128`；來源／feature／accounting與validation2025/test2026相同，所有2439 training rows保留。每輪包含validation/test、checkpoint與plots，最終replay/report亦完成。較短的2015–2024不能和原含2014的134.496 s直接相除當加速率。

256相對同年度控制epoch縮短8.10%、有效日吞吐量增加8.82%；完整3輪總耗時反而增加9.10%，首輪189.829 s對控制144.724 s、編譯成本較高，不能宣稱所有cold-start階段加速。第2輪105.426 s不取代第3輪較慢的121.423 s口徑。512的最終停止收據`annual_v10/unsafe_512_stop.json`保存全部精確PID/cmdline、sample峰值32070／32082 MiB、remaining空集合；這不是已觀測OOM，也不是成功完成512測速。

新增有界stock stream實驗：原compact固定1 GiB workspace與128-stock上限，使local128時每塊114股票、local256時63股票，batch增大反而增加內層呼叫。`factorized_compact_stream_chunk_bytes=0`完整保留原政策／fingerprint；正值才顯式採用有界logical-F workspace、解除128 cap，並加入新compiled ABI v6與optimizer fingerprint。2 GiB候選在global256把train chunk約114→229；eval16也可減少stock loops。資料、LayerNorm全F、參數、gradient與財務FP64均不刪減；兩卡真實BF16+TF32 output／全部gradient容差不放寬。新增171 focused tests通過，完整GPU驗收尚待，沒有把CPUfixture當性能證據。

新年度入口：`scripts/run_tw_no_basis_scale_separated_cash_annual_vastai1t.sh`；原`...b128...sh`保留相容名稱但導向同一個annual launch receipt，實際batch以receipt-bound config為準。原launcher bytes/SHA保留在新operations目錄的`launcher-before`，沒有刪旧formal run。入口52 tests通過；前處理resolver另外明確驗證不受recurrent scan dynamic flag影響、不同年段命中同一runner，相關163 tests通過（含部分重疊）。

## 10. Bounded stream與啟動快取

2 GiB/global256真實fold10、BF16+TF32雙rank oracle已過，rank0/1 max output abs=0.000369579／0.000612378；118組DDP參數 max gradient abs=0.000714898、max relative-L2=0.00230404；每rank檢查6,455,333,538個raw logical值逐位相等。維持output rtol0.003/atol0.0002、gradient rtol0.03/atol0.0005，沒有放寬。不等於完整financial loss／optimizer／whole fold效能證明。source `5369520914dd643554ab53692c5aac1d52df49ad9af596ea12bba1131df5539a`，oracle位於`tw_scale_cash_throughput_20261007_stream_v11/oracle/rank_{0,1}/profile.json`。

本輪工程包裝器誤讀`summary.json`而非既有`profile.json`，造成oracle成功後的readback例外；已修正操作工具並憑相同code/config及兩rank exit0收據繼續，沒有重跑oracle或把例外當GPU成功。原scope字串仍有歷史fold11名稱；本次實際`selected_fold=10`、global256、14726features及config日期才是驗證範圍。

完整v11的pre-epoch200.861 s中，`dataset_and_batch_size`182.434 s、`causal_feature_rms cache=miss`182.044 s，根因是工程包裝器錯用新空transform cache，而不是新head本身需要重做fitting。正式與後續操作改回canonical `artifacts/cache/tw_day_trade_factorized_values_20261005_gaprepair_v4/.training_transform_cache_v1`，保留原source/train-row/alive-mask/fit-contract key與payload fingerprint驗證；沒有把RMS等同optimizer resume。相同年度key `d98f37e1735406d904f70236a13e9b67001e5fc7800d86106659766e36902a2f`已存在，v10接受metadata與本輪獨立fresh-fit逐位相同，SHA `00c26ddb8b585408782b7e03c456cb99cea4400e95b0533192be450267ee87de`。不重跑三輪刷小差異；v11總wall會保留這182秒miss，不能事後扣掉再冒稱實測完整wall。

正式入口將直接重用接受的immutable frozen trainer路徑與compile cache，不另重建wheel或改code path；只發布匹配的standalone正式config、36份inheritance sources、SHA與完整accepted DDP proof。新config仍需全資料驗證／canonical checkpoints，沒有工程optimizer donation或減少epoch必備產物。最新focused 210 passed／1 opt-in CUDA skipped，含stock budget防錯入口與annual ownership測試；其中部分與先前測試重疊。

v11完整3輪806.950 s，比同年度控制525.136 s增加53.665%，包含本次182.044 s RMS miss與新shape首次編譯。穩態91.761 s才是已測得的epoch收益，不把移除RMS miss後的推算總時間稱為另一個實測。第2／3輪93.115／91.761 s均零新graphs；最終完整replay、曲線、plot、checkpoint、summary／manifest通過。正式canonical cache由共用loader驗證約0.10 s，metadata與工程實際fit相同；full startup仍有完整來源驗證、DDP／CUDA初始化等成本，未聲稱為零或達理論極限。

輔助live stack採樣工具只放本任務UV tool-cache/tool-env，不改fintech套件或driver；容器回覆Permission denied，未取得Python stack、未擴大ptrace權限。工具用途與邊界參照[py-spy官方文件](https://github.com/benfred/py-spy)。本次主要證據仍是trainer原有分階段timing、Dynamo counters與完整lifecycle，而非不存在的stack trace。

## 11. 正式訓練交付

在Vast直接前景執行（不加profiling、不用外層torchrun）：

```bash
cd /root/stockAgent
bash scripts/run_tw_no_basis_scale_separated_cash_annual_vastai1t.sh
```

原`bash scripts/run_tw_no_basis_scale_separated_cash_b128_vastai1t.sh`是同一份launch receipt的相容入口，名稱保留b128但實際global batch256。兩入口的`--check-only`已在實際Vast都exit0，回報14726模型輸入、相同annual root。入口自行source runtime、核對code/config/feature/full-DDP接受收據、續租原3份固定來源、strict CUDA check及獨佔GPU lease；canonical trainer自行啟動2-rank DDP。CUDA不足或有GPU owner會拒絕，不默默落回CPU。

交付時另讀回三份精確來源lease與原pin：panel-sources、minute-train、physical皆`hot`／`verification=full`、原pin存在，效期分別至2026-10-13 12:35:05／12:35:32／12:35:18 UTC。沒有選moving latest、重新搬raw data或刪source；正式啟動仍用既有續租流程。RMS共用loader實際接受0.095143 s。新正式output root尚不存在，確認沒有意外啟動正式optimizer。

設定是完整14726 value-only features、無basis/SVD/bottleneck/sparse；BF16 AMP／TF32、FP32 master/Adam與原FP32 islands、財務FP64、48 GiB/rank decoded LRU、checkpoint encoder、2 GiB stock stream、physical staging16、eval model16／ledger32。output mode為`score_entmax_scale_separated_cash`。各训练年度從1000萬重置；若年界有未結部位／未付權益則拒絕，不清零掩蓋；validation2025/test2026維持原連續帳戶。正式1000 epoch／原early-stop 0.1、每epoch必備val/test/plots/checkpoints保留。

正式產物：`/root/stockAgent/artifacts/markets/tw_day_trade_factorized_values_20261007_no_basis_scale_separated_cash_annual_v1/training-bf16`。入口目前沒有啟動或建立正式optimizer；首次是新的年度實驗，之後預設續跑最近相容checkpoint／不重訓已完成fold，不承諾恢復尚未checkpoint的半個epoch。

Vast接受與來源：

- `artifacts/operations/training_launches/tw_day_trade_factorized_values_20261007_no_basis_scale_separated_cash_annual_v1/runtime-launch.json`、`acceptance.json`、`input-contract.json`、standalone `config.yaml`及36份config sources。
- `artifacts/benchmarks/tw_scale_cash_throughput_20261007_stream_v11/full_fold/batch_0256/result.json`及`attempt_01/artifacts`；state accepted、return0、world2、2439 training rows、steady epoch3、failure list空。
- `artifacts/code_releases/tw_scale_cash_throughput_20261007/20261007T023917044440Z-5369520914dd/release.json`及其原有build-source；不修改immutable release。

沿用training-reuse與exact-optimization既有canonical executor、source／fit cache、financial accounting與resume，沒有另建trainer。這是工程效能与所選年度訓練契約驗收，不是已跑完正式1000 epoch、全庫test suite、當沖網站部署或投資結果；模型／batch數值軌跡並非逐位等價。仍保留歷史PIT未證明的research-only資料邊界，以及其他硬體／冷啟動／未測演算法可能更快的邊界。
