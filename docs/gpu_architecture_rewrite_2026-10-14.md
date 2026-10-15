# 2026-10-14 GPU 計算結構改寫：完整特徵的常駐 CSR 投影

後續已完成專用帳務／反向 Graph 與來源快取改寫；目前預設入口改用獨立 v11。
本報告保留 v10 的歷史量測，最新共同來源完整對照與入口驗證見
[2026-10-15 帳務優化報告](ledger_block_optimization_2026-10-15.md)。

正式 v9 消融訓練已按使用者要求停止；最後 durable checkpoint 是 epoch 59。
本次以現有 v9 的完整工作流程為比較基準，保留模型維度、lookback、全部資料、
金融帳務與每輪輸出。Vastai1T 雙 RTX5090 四輪 A/B/B/A 的整個穩態 epoch
由53.372秒降至17.351秒，吞吐3.076倍；完整冷啟動四個 epoch fold 則由
482.979秒降至456.163秒，只減少5.55%時間。正式1000輪消融維持停止。

停止的 run 是 `formal-v9-queue-20261014T123730-3b08ab4e`，15 個身分核對後的
所屬程序全部退出，canonical run state 為 `cancelled`。best(epoch58) 和
last(epoch59) 已在 Penguin 以 SHA256 和 canonical CPU loader 讀回。
證據：[stop readback](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/stopped-readback.json)、
[off-node preservation](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/off-node-preservation.json)。

目前選定 baseline 實際為 d32/lookback32、14,726 個特徵、2,757 個股票 slots，
global train256/local128/eval16、fold11、BF16 AMP、FP32 master parameters、
原始年度帳戶/DDP 梯度與 FP64 敏感帳務。d128/lookback256 是原消融中的選項；
本次不更改任何選項的模型設定。原 prepared source 的 research/PIT 標記也維持原樣。

## 為何新增 GPU kernel 仍不足

已停止的 v9 epoch59 MAX-rank 是 51.970 秒，train41.769 秒，validation9.999秒。
host stage attribution 約為 train forward20.91秒、backward11.97秒、loss7.63秒，
這些未同步區段僅用來選擇診斷位置，不能當作獨立 CUDA kernel 時間相加。

雙卡、真實來源、每次新建 slab 的 CUDA trace 顯示，一個批次的 CPU 時間約70%
在 `_compact_column_packet`。它每次重新裝配大型矩形，輸入 projection 又擴成
寬的補零欄位，checkpoint backward 再讀一次其中一部分。一次測量 H2D 約3.3GB。
Tensor Core GEMM 與 softmax 的占比很小；繼續替換它們無法消除來源 staging。

原模型 probe 的 input/output/VJP 均由 canonical model 產生，沒有 optimizer
update，也沒有以 probe 取代金融 loss。日期分割使用 selected config 的
lookback-shifted annual contract。
證據：[rank0](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/baseline-model-rank-0.json)、
[rank1](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/baseline-model-rank-1.json)。

## 改寫的計算方式

來源仍由 `FactorizedPanelFeatures`、驗證過的 blocks 和 train-fitted RMS buffers
提供。新的 `FactorizedSparseRowStore` 把同一個 physical date block 建成 GPU CSR，
重疊視窗共用它；只省略儲存契約證明的正零，不依值大小截斷，也不挑特徵。
正規化快取只含固定輸入資料與每列統計，不含任何 learned embedding/weight。
來源 fingerprint、ordered symbol axis、RMS buffers 的內容、norm epsilon 與 device
都要相符。DDP 或 checkpoint reload 的相同值拷貝可重用；實際內容改變則拒絕。

LayerNorm 的 F-wide mean/variance 包含所有省略零欄位。一般列使用
`CSR(x / std) @ (W * gamma).T - mean / std * sum(W * gamma) + W @ beta + b`。
所有 F 個 gamma、beta、W 仍參與正確的 affine effect 和 parameter gradients。
統計在 CPU 以穩定的 FP64 centered variance 預算一次；模型存放/投影維持 FP32。
每列若存有全部 F 欄位，就先中心化再 GEMM，避免近常數列的 projected-mean
相消誤差；其他列的零欄位使 `abs(mean/std) <= sqrt(F/omitted_count)`，不會出現
無界的平均值修正。RMSNorm 則沒有 mean correction。

稀疏乘法的右矩陣使用連續的 `[F, projected_width]` 排列，讓同一個非零值取用
連續的輸出權重。第一版的 transpose view 是量到的主要 CUDA 瓶頸；同時取消了
第二次 indicator SpMM。所有股票的寬 projection 先做一次，後面的 canonical
temporal/portfolio blocks 沿窄 embedding 分塊與 checkpoint，避免反向再讀寬來源。
時序依賴、位置、RoPE、QK norm、action mode、annual accounts 和金融 executor
都保持 selected config；沒有把每日帳戶假裝成互相獨立的樣本。

這是與 dense stem 在實數算術下相同的完整 affine 計算，浮點 reduction/GEMM
的次序與 train trajectory 會改變。它有明確的 opt-in、checkpoint fingerprint
與新 output root，不能用來 exact-resume v9 optimizer。

實作使用 PyTorch CSR×Dense autograd；官方支援範圍見
[torch.sparse.mm](https://docs.pytorch.org/docs/stable/generated/torch.sparse.mm)。
實際相容性另以遠端 Torch2.11.0+cu128、雙 RTX5090 的計算與梯度測試確認，
不是僅依最新文件或 GPU discovery 推測。

## 已有的工程證據與量測範圍

- 模型 probe source SHA `b4474f33e261104f3779db07c6047b3db3f269bdc18ac2007dade7afe9c9d7eb`。
  完整重測的 v7 source 是 `4f5a42134080657c7d6c476f424e9b9a0c63b5bc0d7149eaebfa1f3b73aab84e`，
  加入只在冷建構時釋放 inactive allocator reservation 的記憶體管理。
- 本機32個 compact/sparse tests；另110個相關 regression tests。
- 遠端同一 frozen model 的32個 tests 通過，含 LayerNorm/RMSNorm、所有參數
  adjoint、股票/日期邊界、重複 tail、近常數完整列、cache eviction 與先 eval 再 train。
- 以同一 epoch59 checkpoint 的雙卡 probe，MAX-rank 三次新 slab 測量約0.166秒，
  原路徑約1.078秒，模型部分約6.5x。暖機後 H2D/source decode 均為0；沒有 optimizer。
- 原有嚴格 BF16 output/gradient comparator 的拒絕保留。新版約少量元素超出
  點對點舊容差、最大 parameter relative-L2 約1%；這不是 bit-equivalence 宣稱。
  依使用者明確允許數值/梯度軌跡差異，使用獨立 dense mechanism/adjoint oracle、
  finite/nonzero gradients、native repeated control 和完整帳務/lifecycle 驗證。
  `--numeric-experiment` 只允許選定研究軌跡，不刪除或放寬原 comparator 的紀錄。

主 checkout 整合後190個模型/來源相關測試與121個 checkpoint tests 通過；
消融 source-binding、recipe 與 catalog 的相關測試也通過。
以兩份 frozen source 各自的 loader 比對完整 config manifest，關閉選項時與 v9
完全相同；啟用時確實是不同 fingerprint。不是只以單一 helper 推測相容性。

原批次 global256/local128 的 CSR 全 fold 成功產生回測、圖表與 checkpoint，
但峰值32,084MiB、餘量0.511GiB，未通過1.5GiB與0.96fraction gates。
暖機 epoch3/4 為18.027/16.763秒；保留拒絕，不降低驗收門檻。
單純釋放 inactive CUDA pool 不能解決 persistent source 加上 recurrent workspace
的峰值。因此依容量例外改用 global128/local64；d_model、lookback、完整日期/
特徵/股票、逐日帳務與每輪一次 optimizer/scheduler update 維持原設定。
local BPTT 截斷由128改為64，梯度軌跡差異是明確的數值實驗，不是 exact equality。
比較控制維持目前 v9 的 global256/local128，沒有把較慢的縮小批次控制當作 baseline。

## 完整工作流程交叉實測

同一台主機、同一 Torch2.11.0+cu128、同一 frozen v7 source、同一 fold11，
順序為 A1→B3→B4→A2。A 是目前的 v9 計算路徑、global256；B 是 CSR 新路徑、
容量例外 global128。只改此兩個申報軸與獨立 output root。
scheduler 仍保留 baseline 的1000-epoch horizon 與32-step warmup，工程 cutoff 是4輪。
每個值均以完整 canonical result 的 MAX-rank epoch3/4 計算，沒有採用較快 rank。

| Trial | 穩態 MAX-rank epoch（秒） | 完整4輪 fold（秒） | GPU peak（MiB） | 最小餘量（GiB） |
| --- | ---: | ---: | ---: | ---: |
| v9 A1 | 53.4017 | 482.4387 | 20,170 | 12.146 |
| CSR B3 | 17.3173 | 469.0840 | 25,500 | 6.940 |
| CSR B4 | 17.3852 | 443.2413 | 25,498 | 6.942 |
| v9 A2 | 53.3421 | 483.5196 | 20,170 | 12.146 |

穩態 mean53.3719→17.3513秒，每輪時間減少67.49%，吞吐3.076倍；
完整冷 fold mean482.9791→456.1627秒，吞吐1.0588倍。
CSR B3/B4 的首輪分別256.869/227.922秒，原路徑首輪約127秒。
CSR 冷建構成本尚未消除，不能把3.076倍稱作完整冷 fold 的加速。
原先 CPU packet staging 和反覆 H2D 的成本只需建立一次，後續 epoch 可重用。

四份結果都通過原有完整 lifecycle、ownership、記憶體餘量與穩態無新 graph gates。
每輪均只有一次 optimizer/scheduler update，2685個有效訓練日期全部保留，
沒有 zero-gradient batches，年度帳戶保持 alive。149個 test日期均有有限值，
其中147日有實際非零 executed turnover；不同浮點/梯度軌跡未要求同一成交序列。
validation/test、curves、plots、checkpoint、final replay 全部保留。
四份完整產物已複寫至 Penguin，逐份核對遠端 result/run-spec/checkpoint/curve SHA，
重新執行 canonical completion validation 並以 CPU loader 讀取 epoch4 checkpoint。

證據：[ABBA acceptance](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/resident-balanced-acceptance.json)、
[mechanism validation](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/resident-mechanism-validation.json)、
[off-node readback](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/resident-off-node-verification.json)。

## 現在剩下的瓶頸與消融入口

CSR B3 epoch4 的 host stage 記錄顯示：24次 model forward 合計約0.893秒，
backward 約4.55秒，loss 約7.85秒，validation1.91秒（其中帳務約1.74秒）。
loss 與 backtest 的區段重疊，CUDA 也不同步，這些只能定位瓶頸，不能相加當成總時間。
移除 staging 後，優先順位已轉到逐日金融狀態與其反向；繼續加速 softmax/GEMM
無法再消掉主要時間。NVML busy%不是這個訓練的有效工作吞吐分母。

追加帳務 CUDA Graph 的工程篩選另存在 `ledger-graphs/`；只改環境開關，
保留金融 probe。首輪 MAX-rank716.181秒，第二輪128.695秒；graph 節點1158→1887，
第二輪 train10.964秒，但整輪帳務 eval/capture 等開銷仍大。原 log 保留
pending/uninvoked backward 警告。兩輪後按工程成本停止，11個身分核對後的程序
均退出。這是未完成 fold 的拒絕篩選，沒有 epoch3+ 或完整 fold 的加速證據，
也沒有宣稱所有 CUDA Graph 設計都無效；不混入上述 ABBA、不納入 v10 recipe。
不能為了 graph replay 清空尚待 backward 的金融狀態或 tape。
證據：[partial rejection](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/ledger-graphs-rejection.json)、
[owned-process stop](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/ledger-graphs-stop.json)。

`scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh` 的預設已改成
`--resident-sparse` v10，使用已驗收的 frozen source 與 hashed mechanism/performance
binding。[recipe](../configs/benchmarks/gpu_resident_best_20261014.yaml)、
[spec](../configs/ablations/tw_scale_cash_annual_last_fold_resident_20261014_v10.yaml)。
Baseline→Muon→其餘35-case catalog、每個 case 的最後 fold、失敗繼續與容量 retry
維持原流程。d128/lookback256 原選項保留，尚未宣稱它們都完成新架構訓練。
遠端35/35 dry-run 與 calendar-context 驗收通過；lookback256 因有效視窗使用自身
最後 fold10，其他 case 為fold11。新版完整 epoch 加速是 baseline 的實測結果，
不是所有消融或正式1000輪完成，也沒有策略品質或全域計算極限結論。

未來遠端啟動命令仍是 `bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh`。
本次只執行 `--dry-run`，正式35-case queue 沒有重啟；明確指定 `--structural`
仍可選回舊 v9 入口。新版使用獨立 root
`artifacts/markets/tw_scale_cash_annual_last_fold_resident_20261014_v10/`。
最後獨立讀回確認正式 run 和已中止的工程 run 仍是 cancelled、原 best58/last59
hash 未變且 CPU 可載入，Vastai1T 已沒有 GPU compute owners。
後續兩個同名 workflow run 只驗證完成/停止證據，未再執行 GPU 訓練，原 cancelled
receipts 保留。[final readback](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/resident-final-readback.json)、
[default dry-run](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/resident-default-validation.json)。

模型證據：[rank0](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/sparse-model-v6-rank-0.json)、
[rank1](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/sparse-model-v6-rank-1.json)、
[release](../artifacts/operations/gpu-architecture-rewrite-vastai1t-20261014/sparse-release-build-v6.json)。
遠端 benchmark root：`artifacts/benchmarks/gpu_architecture_rewrite_20261014/`。
