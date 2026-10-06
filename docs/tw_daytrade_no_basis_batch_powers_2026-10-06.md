# No-basis fold11：2 的冪次 batch 雙卡測速

## 1. 執行進度

- 2026-10-06：核對原前景入口、frozen source、資料與 GPU owner 完成；原正式 checkpoint 不變。
- 兩張 RTX 5090，strict CUDA preflight 通過；來源維持既有精確 snapshot，七日 lease/full SHA 驗證通過。
- 初篩以 global batch 32 起向上測 2 的冪次；完整 3 epoch 驗收可行候選，CUDA OOM 保留實際失敗證據。
- batch32 完整 3 epoch/lifecycle 通過：epoch3 MAX 156.695s、train MAX 144.895s、完整 fold 695.405s；峰值 24.31GiB/卡。
- batch64 automatic retained-activation path 在第一輪前段實測 CUDA OOM（兩卡約31.34GiB，16MiB allocation 失敗）。原 optimizer 未寫入。
- batch64 改用既有 `factorized_encoder_checkpoint: true` 後完整 3 epoch/lifecycle 通過：epoch3 MAX 176.369s、train MAX 160.143s；完整 fold 約681.90s，峰值21.32GiB/卡。
- checkpoint 模式的128與256在 replicated full-symbol exact FIFO loss forward/backward probe 實測 OOM，沒有進入正式 epoch；512/1024未實測，不列為已驗收。
- 使用者改為優先優化大batch的算法／空間複雜度，不再多輪比較小差異。已停止repeat64，取消尚未啟動的repeat32；64重測epoch3為155.502s/graph delta=0，但此中止run不是完整fold驗收，不選出「最佳batch」。原正式optimizer未修改。
- 已實作canonical FIFO block rematerialization：原BPTT/global-batch與完整cohort軸不變，區塊邊界不detach，不移動帳務到CPU、不啟用稀疏研究策略。整個batch仍只有一次solvency certificate/full fallback。
- 14項新測試通過：long/short、跨日、dated cash claims、locked stock delivery、terminal unlimited、absorbing default、分鐘/compact histories與梯度一致，saved-tensor量下降。這不是已量測的GPU峰值或吞吐量。
- 舊canonical carry測試68項通過；候選以新隔離source驗證，不覆寫原source/正式checkpoint、不啟動正式1000 epoch。
- block16/batch128 完整3 epoch/lifecycle通過：epoch3 MAX 220.783s、train MAX 203.631s、完整fold847.404s，峰值18.34GiB／卡，仍有13.50GiB headroom。突破原OOM，但比原32的156.695s慢，不作為速度提升或預設batch的依據。
- 進一步移除loss完全未使用的pre-conversion intraday NAV adjoint，仍執行原270分鐘逐點NAV、solvency/ruin/source檢查；closing NAV與完整金融狀態梯度不變。公共simulator預設保留分鐘診斷梯度，只有canonical risk_aware_loss明確選擇lean分支。24新測試與實際loss/head-gradient對照通過。
- 修正後較廣回歸425 passed／4 skipped。CPU 7日單股fixtures的saved-tensor累計量減少72%～75%；這不是GPU峰值或吞吐量估計。
- 雙卡CUDA／differentiable all-gather的long/short、cash claims、locked delivery、terminal、default案例全部通過：完整輸出及最終狀態bit exact，loss梯度rtol=1e-12／atol=1e-10；兩rank均正常退出。測試初次收尾阻塞的失敗證據保留，改為明確device及bounded Gloo completion。
- 第二版batch128／blocks=0仍在完整physical FIFO loss probe OOM（30.20GiB allocated／每卡31.36GiB），沒有進入epoch1；小型CPU saved-tensor比例不可推估真實GPU峰值。確認probe仍經由同一canonical loss／physical-source adapter，沒有錯用舊路徑。
- 第二版batch128＋block16完整雙卡/lifecycle通過：epoch3 MAX198.586s、train MAX181.260s、完整fold768.867s、峰值17.91GiB／卡。比第一版同batch epoch快約10.05%，但仍慢於原32，不能只因較大batch可跑就升為預設。原正式1000 epoch／optimizer未啟動或修改。
- 已用CPU篩除growing power-of-two cohort候選，不耗GPU測速：128日理論padding行數可從16384降至10923，但7股terminal案例realized PnL相差1.82e-12，default案例凍結後inactive acquisition date亦有1日差異。程式及開關已移除。使用者最新允許微小加總誤差，1.82e-12本身不再構成拒絕理由；日期與physical state錯誤仍須修正，故不恢復該舊候選。
- 第四版certified-flat／compiled-commit雙卡CUDA oracle通過、兩rank正常退出；carry/simulator/loss/inventory/scan五檔候選保留舊sum形狀，僅作數值證據，未做完整訓練。446項較廣回歸通過／4 skip。最初SSH banner timeout不是CUDA失敗；重新唯讀確認GPU無工作後成功執行。
- 第五版接受使用者容差，移除只為保持bit順序而新增的零padding／native PnL sum adapter，回到原canonical reducer。證明每日清倉後只保留本日一列physical cohort、不再重做先前零部位；realized PnL／fees／NAV持續跨日保留autograd，不重設資金或新增detach。仍保留所有原始分鐘估值與default檢查，certificate不成立就完整fallback。
- 實際frozen設定為BF16 AMP且`use_tensor_cores=false`，不是TF32已經啟用。新隔離候選明確開啟BF16＋TF32矩陣計算；FP32 master／Adam與physical FP64帳務不盲目降成BF16。第五版59項focused tests通過／1 CUDA條件skip，另補充非空7／127股CUDA財務容差與梯度驗收、避免假全零成交。
- 第五版目前447項相關回歸通過／4 skip（兩組不重疊186＋261），新增逐日incoming cohort行數固定0的算法保護測試。擴大CUDA oracle首次在120s定時faulthandler stack dump時雙rank SIGSEGV，已保存failed receipt，沒有進入完整訓練。Python堆疊仍在SymPy／Inductor codegen；尚未證明框架根因。改用外部600s程序期限、關閉非必要的定時非同步dump重驗，NCCL／Gloo與所有金融比較仍保留。
- 第五版擴大CUDA重验通過、兩rank126.55s正常退出，未達600s外部期限：7股NAV誤差0／gradient max abs3.86e-17；127股NAV與gradient誤差皆0，成交非空、physical fields精確。完整`[128,2757,12]`cohort commit八種條件與梯度精確且不按predicate重編圖。重新strict CUDA preflight後啟動唯一一次batch128／BF16＋TF32完整三輪/lifecycle驗收；正式1000輪仍未啟動。
- 第五版唯一一次完整雙卡三輪驗收完成，正常退出0；canonical lifecycle再驗18項必要產物通過，包含val/test、final replay、曲線／圖及checkpoint。epoch3 MAX **138.080s**、train MAX **125.846s**、2,653日／epoch、**19.213有效日／s**；穩態新增graph為0。三輪均21批、2,653個清倉session、flat fallback／compile failure為0。
- 相對同batch128的第二版（198.586s）steady wall減少**30.47%**；相對原batch32（156.695s）減少**11.88%**、有效吞吐量增加**13.48%**。完整三輪wall **691.534s**，相對原32的695.405s僅約0.56%差異，不反覆重跑這個小差距，也不說冷流程大幅加速。epoch1為249.641s／13張暖機新graph；epoch2為135.324s，僅epoch3列作選定steady驗收。
- 兩卡峰值17,922／19,542MiB，最大約**19.08GiB**，最小headroom **12.76GiB**；沒有OOM。這是含初始化／probe／評估的NVML採樣峰值，不等於某個kernel的精確峰值或硬體理論極限。
- 同batch128的三輪模型結果有小幅差異：第二版final-test cumulative return為−0.486042%，第五版為−0.532115%，相差**0.046073個百分點**；train loss epoch3為−0.000212961→−0.000183121。不可將數值容差通過說成optimizer／長期報酬相同；這只是三輪工程短測，不是收益實驗、TF32單因子消融或未來1000輪的誤差保證。
- 新root的前景／canonical resume／profiling-off入口已發布，正式optimizer沒有搬入。原batch32 checkpoint重新SHA驗證仍為`5aa8acf1cf84c18c1adab6d0d90c9f2247fe987514148b51b071c9ac65ecfb64`。本機新／舊入口24項測試通過；遠端實際`--check-only`退出0，三個exact來源full-SHA leases與canonical pins通過、保留payload且刪除0檔，期限延長到10/13；strict CUDA確認兩張5090、Torch2.11+cu128、無warnings/failures。沒有啟動正式1000輪。

## 2. 本次接受的改變與固定條件

使用者明確接受 batch 改變梯度分段，以吞吐量及效率優先。這不是舊 optimizer
軌跡的等價加速；每個 batch 用獨立 optimizer/output，不能繞過不相容 resume gate。

2026-10-06最新明確接受微小浮點加總差異及BF16／TF32混合試驗；不再把
1e-12級financial roundoff當作擋住候選的理由。帳務對照採rtol1e-12／atol1e-8，
多股fixture梯度採rtol1e-10／atol1e-8；股數、日期、claims、failure state仍精確對照。
模型TF32是新的numeric experiment，不宣稱整段optimizer等價，也不拿工程3輪
checkpoint續接正式1000輪。原batch32正式checkpoint不變。

保留 14,726 value-only channels、no basis/SVD/PCA、lookback32/embedding32、
latent attention、BF16 AMP 及FP32 master／island storage；新候選允許island的
eligible FP32矩陣使用TF32。fold11 的 train/val/test 與
exact FIFO、50% first-minute capacity、official-close unlimited reduction-only
帳務契約不改。evaluation batch/model chunk16、physical backtest chunk32 不變。
每 epoch 維持 whole-trajectory 一次 optimizer 更新。

沿用已驗證的 packet cache、activation/VRAM guard、Numba/Torch actual thread
修正與 single-fold no-isolation；仍以雙卡 DDP 執行，並非單卡診斷。
較大 batch 的新增 runtime-only 候選明確啟用 canonical encoder checkpoint；
自動 guard 的 retained path 並未保證新形狀可用。這個開關不改模型能力、精度、
資料或財務公式，代價是重算，因此仍要測吞吐量，不能只以「能跑」選它。

## 3. 測量與驗收

- 暖機與 steady epoch3+ 分開；使用 canonical trajectory 的 `epoch_max_rank_s`、
  `train_max_rank_s`，不以 rank0 的局部時間替代。
- 有效交易日 rows/s 扣除 padding；完整 fold wall 包含 setup、三輪 train/val/test、
  plots/checkpoints、final replay/report。wrapper 前置 SHA/lease 成本另列。
- 記錄完整曲線、GPU sampling、有限 loss/gradient、穩態 graph delta、canonical lifecycle gate。
- 初篩後以平衡順序重測最快候選；不以舊資料集的最快 batch 當本次結論。
- 上述重測計畫已依最新指示停止；改以有實質記憶體／算法改動的候選作一次有界雙卡完整驗收，不持續重跑32/64的小差異。
- 第一個 OOM 只代表該候選失敗。若較大 batch 會切換 activation checkpoint policy，
  必須繼續上方候選，不能據此宣稱所有更大 batch 都不可行。
- 本次已分別確認64的retained/checkpoint路徑，並在128/256確認loss記憶體邊界。
  model按時間切給兩卡，但physical FIFO ledger在每卡持有完整symbol/global batch；
  雙卡32GiB不等於一個loss可用64GiB。既有symbol-sharded ledger不支援physical FIFO，
  不會為了放大batch跳過probe或改掉金融帳務公式。
- 保留未通過與 OOM 的 log/receipt；不刪正式存檔、來源或 compiler cache。

## 4. 身分與可重用入口

- 原指定入口：`artifacts/markets/tw_day_trade_factorized_values_20261005_no_basis_v3/train_vast_no_basis_fold11_v3.sh`。
- 實際 code：`artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_runtime_optimized_v1/code`。
- code source SHA：`2147fccf74daf9b7740c92723baca9996f47666165e3183494c1f97d8fc304e5`。
- 遠端本次根目錄：`artifacts/markets/tw_day_trade_no_basis_batch_powers_20261006_v1/`。
- 原正式 checkpoint SHA：`5aa8acf1cf84c18c1adab6d0d90c9f2247fe987514148b51b071c9ac65ecfb64`。

擴充既有 `scripts/benchmark_tw_day_trade_batch_sizes.py`，支援 verified frozen
project root、trajectory MAX-rank 名稱、完整 fold wall、OOM frontier；DDP 缺少
MAX/graph telemetry 不接受。測速 helper 放在 frozen code 之外，不改訓練 code
receipt；16 個 focused tests 通過。

測速使用的 helper SHA 為 `c6b55af9b48980439fea410cdd5659b6f9acc52cbe1481f2e01b0440c932f733`。
後續工具修正另外放在 `engineering-tools-v2`，確保config parser與lifecycle imports
也屬於指定frozen project，遇到已載入的其他checkout直接拒絕，不混用設定schema。
既有測量helper/source不覆寫，v2不改測速公式或正在執行的候選。
v2 helper SHA為`6777ebb22d804d543e4faaff23cb83df11dd2dd29db32482eac0ad02ca9d8e9b`，
18項focused tests通過；第五版完整測速沿用同一helper，不另造計時器。

本機加入原入口的30 tests通過；遠端在frozen library下的18 tests亦通過。
另實際解析frozen設定確認global32/checkpoint=false及library origin相符。

## 5. 為何不預先認定 batch 越大越快

每卡 local batch 為 b，lookback32 的連續 slab 約有 b+31 個 unique rows；
每有效日重讀比例約為 `1+31/b`。增大 batch 可攤提這部分工作及 launcher/loss
呼叫，但會增加 activations、FIFO/BPTT graph 及 padding。VRAM guard 切回
checkpoint 時又要重算 encoder。因此峰值記憶體或 batch 數字最大，都不能代替
完整流程實測；這次只有梯度分段允許改變，沒有省略資料或輸出。

既有優化來源與數值限制見
[no-basis pipeline 報告](tw_daytrade_no_basis_pipeline_optimization_2026-10-05.md)。

## 6. 大batch的算法／空間邊界

原FIFO已使用prefix integral與searchsorted，避免`[cohort,symbol,minute]`
三維展開；本次不複製另一套成交公式，也不換回曾發生CUDA/layout錯誤
的scan/index融合。仍保留native prefix/searchsorted/column-stack邊界。

設global batch為B、symbol數為S、分鐘數為M=270、padded cohort軸為K，
每日期的financial autograd中間量約含`S*M`與`S*K`項。原先整批保留
約`O(B*S*(M+K))`；區塊G的重建則保留約`B/G`個跨日state邊界與G天
局部中間量，仍須保留來源輸入、輸出和完整FIFO state。K隨B增長時，
cohort部分仍有二次成本；checkpoint不是把整個算法變成線性，也不是時間
lower bound。其代價是再計算forward，必須獨立量測。

第二版直接移除loss不使用的分鐘估值反向分支：前向完整讀取及驗算270
分鐘，反向不再保存／回傳該diagnostic分支的scan/gather梯度。實際fill、
fees、claim、conversion、close、default及學習所用的loss梯度仍由原帳務
實作負責。不省略分鐘曲線、validation、test、plot或checkpoint。

第五版在**證明逐日清倉的batch**移除跨日惰性cohort，所以原逐日cohort工作
`O(B*K*S)`（K隨B增长）縮為`O(B*S)`，每分鐘行情掃描仍需`O(B*S*M)`；
重建完整公共cohort輸出仍為`O(B*S*F)`。不再加入為了bit加總順序而形成的
`O(B*K*S)`padding。這不是所有carry情境都線性：有殘倉、claims、source失敗或
破產時仍走原完整recurrence；各種成本沒有被隱藏或略過。compiled commit則合併
三段cohort條件選擇，保留所有source／calendar fields，避免按條件值重編圖。

### 混合精度的實際作用

BF16 autocast原本已開啟，但joint stem／temporal／portfolio有FP32 islands，
原`use_tensor_cores=false`也關閉了TF32。新候選開啟canonical Tensor Core flag，
不另建precision runner。TF32針對矩陣乘法／卷積，不會把native searchsorted、
prefix scan、scalar費用或FP64金融加總變快；因此先降低空部位重複工作的複雜度，
而不是把整個physical ledger强制轉低精度。BF16留FP32 master／optimizer，價格、
tick／股數／日期等不能以BF16量化取代。[PyTorch 2.11數值／TF32說明](https://docs.pytorch.org/docs/2.11/notes/numerical_accuracy.html)
與[AMP運算精度選擇](https://docs.pytorch.org/docs/2.11/amp.html)支持此分工；效益仍需完整雙卡實測。

### 隔離source

- block重建v1：`f3ef1f0ffcf54e7b5a83a8f447bb9bfb3fc8a36078100ba51b464d6228375a1e`，只修改carry。
- block＋unused-adjoint v2：`044e5cf4c0f13abec3471c773ceeeb4528cd1fa8e36c979a5ffe1e8422d5f4ce`，只修改canonical carry/simulator/loss三個檔案，1348個source/config成員逐一hash驗證；遠端重建wheel/receipt。不帶入同時進行的storage變更。
- 兩版分別位於遠端本次根目錄的`large_batch_optimization_v1`和`large_batch_optimization_v2`；不覆寫原frozen source、舊正式optimizer、舊結果或compiler cache。
- certified-flat／保留sum形狀v4：`293281275119c1f1cba253899f4f4e9e9f6bbbb15acca75457ca98f6c018b2f8`，遠端CUDA oracle通過；不據此宣稱完整fold更快。
- compact-sum／certified-flat v5：`b2c7055cac3fe49e3a08725c44000669331b43afdd60be82721577efc7df7b22`；移除新增financial padding adapter，只擴展canonical carry/simulator/loss。TF32候選override位於v5 root之外部YAML，source/config SHA各自固定。

### 分散式驗收收尾

初次CUDA oracle的五類案例均已通過，但測試未正常結束；只停止已驗證身分
的該次torchrun，保留log、原test SHA及失敗receipt。不能以已印出pass
取代正常退出證據。第二次明確指定rank裝置，並沿用repository的CPU
completion思路：NCCL負責GPU梯度，bounded Gloo monitored barrier負責
驗收結束。兩rank均於約43s通過且退出。這是測試harness修正，非以單卡
替代DDP，也未移除任何金融驗算。[PyTorch的device與barrier契約](https://docs.pytorch.org/docs/stable/distributed.html)
說明NCCL barrier的裝置選擇與Gloo monitored barrier；未取得原卡住程序的
Python堆疊，因此不宣稱已證明該次阻塞的完整framework根因。

## 7. 第五版完整驗收與前景訓練

| 實測候選（同資料／fold，batch與精度範圍各自標示） | epoch3 MAX | train MAX | 完整三輪含final/report | 峰值／卡 |
| --- | ---: | ---: | ---: | ---: |
| 原32／BF16、TF32 off、retained activations | 156.695s | 144.895s | 695.405s | 24.31GiB |
| 第二版128／BF16、TF32 off、lean adjoint＋block16 | 198.586s | 181.260s | 768.867s | 17.91GiB |
| 第五版128／BF16＋TF32、lean adjoint＋certified flat | 138.080s | 125.846s | 691.534s | 19.08GiB |

原32與新128的gradient分段不同；第二版與第五版同128，但新cohort算法與TF32
同時改變，不能把全部速度或報酬差異單獨歸因於TF32。每候選只有一個epoch3
steady樣本，沒有置信區間，不宣稱所有2的冪次batch已找到全域最佳。

新訓練入口位於**遠端**：

```bash
cd /root/stockAgent
bash artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_v1/train_fold11.sh
```

固定fold11、雙卡DDP、global128/local64、BF16 AMP＋TF32、no basis／sparse，
1000輪與原完整報告設定。首次在新root開新optimizer，後續直接執行同一命令會
用canonical resume續跑該root相容checkpoint；不要求改`--epochs`或加profiling。
舊root的進度與原入口保留，不把3輪工程checkpoint或舊32 optimizer移入新版。

主repo維護入口是`scripts/run_tw_no_basis_flat_b128_vastai1t.sh`及同名新deployment
YAML；launcher屬於外部驗收工具，不在verified source內原地修改。新root只存
`config.yaml`、`train_fold11.sh`、`training-ready.json`與之後的正式`training-bf16/`。
新source root為`large_batch_optimization_v5/code`，1348檔hash與wheel可驗；舊source
和compiler／transform cache不刪。資料exact snapshot不變，只延長既有lease與
建立canonical training pins；不重建所有資料，也不進行額外傳輸。

readiness receipt綁定source/config/feature manifest SHA、三個exact來源、完整fold
result SHA、正常退出的CUDA oracle SHA、18項canonical lifecycle產物及原checkpoint
SHA。正式入口先核對receipt、exact full-SHA leases／pins與strict CUDA，再以既有
GPU lease owner啟動canonical `train.py`；已有GPU owner則拒絕重疊。`--check-only`
只驗環境與資料，不啟動正式訓練。

本機小收據：
`artifacts/data_quality/tw_feature_expected_gaps_20261004/remote-no-basis-batch-powers-20261006-v1/`
下的`large-batch-flat-bf16-tf32-128-result.json`、
`large-batch-flat-bf16-tf32-training-ready.json`、
`large-batch-flat-bf16-tf32-foreground-preflight-pins.json`、
`large-batch-flat-v5-cuda-oracle-receipt.json`及保留的failed receipt。

實际`--check-only`已正常退出0；preflight pins receipt另記錄實際foreground
entry SHA及verified source SHA，不拿file存在冒充資料／環境通過。相關回歸447
passed／4 skip、入口24 passed、Ruff與shell syntax通過；不是宣稱整repo完整suite。
既有優化skill以最小參考更新記下M19/M20/M21與E09範圍，保留過往拒絕證據，
不把微小roundoff拒絕永久化、不再從零重做已存在的cache／compile／DDP方法。

### 下一個剩餘瓶頸

epoch3現有廉價host timing（未開CUDA同步profiling）顯示，rank0每批model
enqueue間隔約3.04s、loss約1.36s、backward約1.04s、fetch約0.30s；21批，
完整train MAX125.85s。這些欄位有nested／asynchronous等待，不能當GPU
kernel百分比或相加成嚴格critical path，但足以說明不是幾ms的H2D enqueue
可以主導下一次優化。下一個實質候選是canonical寬投影與FP32 islands／attention，
要有新的數值與activation headroom證據才改，未測低精度block不宣稱已啟用。

金融核心已證明清倉batch的cohort工作由二次變線性，native分鐘掃描和必要
寬特徵讀取仍在；當有殘倉／claims／default時完整fallback也仍在。BF16帳務
以1,000萬元量級計算的解析度不足以表示tick／費用，所以保留FP64 physical
帳務，不拿吞吐量偏好當作可以量化或省略交易規則的理由。資料本身仍是
value-only research/nonhistorical PIT契約，不聲稱已可實盤。
