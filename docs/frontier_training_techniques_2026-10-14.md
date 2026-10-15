# 消融 baseline 的現代訓練方法實測，2026-10-14

本輪先停止 Vastai1T 原年度消融，再沿用它的 **baseline** 測試 optimizer。
目前結果以 `artifacts/benchmarks/frontier_training_20261014/screen-v2/screen.json`
為準；只有 `complete=true` 且個別 trial 通過完整 lifecycle，才算完成該項實測。
這是最後 fold 的 12 次更新診斷，不是收斂或投資績效的結論。

2026-10-14 16:36 的獨立回讀確認九組短測均完成並通過 lifecycle，正式 v8 baseline
完成 epoch208，Muon 到 epoch275、queue 繼續執行。GPU 現場量測、實際載入版本與
下一版帳務／年度多卡分工假說見
[GPU 利用率與結構診斷](gpu_utilization_first_principles_2026-10-14.md)。

## 第一性原理與適用性

本 baseline 是 FinancialTransformer：d_model32、lookback32、global batch256、
eval batch16、雙 RTX 5090 DDP。它不是 catalog 中的 d128 或 lookback256 消融。
baseline 有 1,133,582 個主參數，FP32 約 4.32 MiB；activation、來源資料與年度
金融狀態才是主要記憶體成本。以實際 baseline 作模板，不縮小其他消融的研究維度。

完整工作鏈為 immutable source → causal compact feature → temporal/latent/market
編碼 → 全域 action → 年度 FIFO/cash/NAV 狀態 → 完整年度梯度 → DDP 平均與
clipping → 一次 optimizer update → validation/test、圖、checkpoint 與結尾 backtest。
每個 epoch 只更新 **一次** optimizer，不能把幾千個交易日當成幾千次更新。

固定更新次數時，總耗時約為來源核對、編譯暖機、每輪完整工作與最後報表的總和。
若要比較到達相同品質的成本，還必須量測到達同一 validation 目標的更新次數。
因此 optimizer 的主要潛力是改善更新方向、減少所需更新；單次 step 的 microbenchmark
不能直接當成整輪加速。更好的 train loss 也不等於更好的 validation loss。

此次保留 NUMA affinity、funding preparation v3、Gregorian calendar cache、BF16
temporal、manual attention、QuACK softmax，FP32 masters 與敏感 FP32/FP64 金融
reductions。資料、決策時鐘、eligibility、價格、整股、fees、FIFO、年度 reset 與
absorbing ruin 不變。新 optimizer 可以改變梯度更新和後續持倉軌跡；固定 action
時的帳務契約不改。

## 本輪接入並實測的方法

| 方法 | 改變更新的哪一部分 | 本 workload 的理由與限制 |
| --- | --- | --- |
| [Muon](https://github.com/KellerJordan/Muon) | 將 momentum 的矩陣更新近似正交化 | 避免更新被少數奇異方向主導；使用原生 Torch Muon，只作用於內部 Linear weights。輸入、輸出、embedding、query、norm/bias 仍由 AdamW 更新。它正交化的是更新，不是模型 weights。 |
| [NorMuon](https://arxiv.org/abs/2510.05491) | Muon 後增加 neuron/row 的自適應尺度 | 嘗試平衡每個 neuron 的更新；不能把 LLM 收斂收益直接套到年度金融 loss。 |
| [Dion3](https://github.com/microsoft/dion) | 子矩陣正交化、error feedback，再做 neuron normalization | 用 fraction0.25 減少正交化工作，保留未立即使用的更新資訊；小 d32、每輪一次 step，整輪省時可能很有限。官方實作中 Dion3 是 NorDion2 的別名。 |
| [SOAP](https://github.com/nikhilvyas/SOAP) | 在 Shampoo 的曲率 eigenbasis 中做 Adam 更新 | 嘗試處理特徵/gradient 的強相關方向；本輪 precondition_frequency4、max_precond_dim512。首個矩陣 step 僅初始化 preconditioner，是上游算法的行為。 |
| [AdEMAMix](https://machinelearning.apple.com/research/ademamix-optimizer) | 混合短與長期 gradient EMA | 時間單位是 optimizer updates。原 beta3=0.9999 約對應 10,000 步記憶，遠大於本 baseline 的 1,000 步上限；本輪明列 beta3=0.99、alpha2、12 步 ramp 的 cadence 調整，沒有冒充原論文配方。 |
| Muon + AdEMAMix | 內部矩陣用 Muon，其餘參數用 AdEMAMix | 同一 parameter 只由一個 optimizer 更新；兩組共用 scheduler/scaler/checkpoint owner，測試中檢查精確續跑。 |
| Muon matrix LR ×2 | 同一 Muon 機制，探索矩陣更新幅度 | base LR 與 auxiliary LR 保留；只能視為一個明確超參數試驗，不能宣稱已公平調到各算法最佳。 |

篩選順序為 AdamW control、Muon、NorMuon、Dion3、SOAP、AdEMAMix、
Muon+AdEMAMix、Muon matrix LR×2、AdamW repeat。每項均使用 fresh root，
失敗保留 log/result 並繼續下一項。control 與 repeat 用來觀察機器時序波動。

矩陣分類依 module role，而不是「所有 2D tensor」；本版保留原 packed QKV 與
SwiGLU 的分組，尚未測 Q/K/V 或 per-head 分開做正交化。Microsoft 提供相關
[分組介面](https://github.com/microsoft/dion#per-head-newton-schulz-for-attention-projections)，
這屬於獨立 optimizer 分組實驗，不能混進此次比較。

遠端真實 Muon epoch8 checkpoint 的 readback 確認：30個內部矩陣，共75,264個
參數，由 Muon 持有 momentum；auxiliary 有89個 tensor，其中88個已有 AdamW
state，共983,758個 state-backed 參數。兩組 LR 相同，scheduler last_epoch8、
group_counts=[1,1]。75,264約佔全模型參數6.64%，不等於它們只影響6.64%的行為；
這也說明不能把此次 Muon 稱為全模型正交化。證據為操作目錄的
`muon-mechanism-readback.json` 與固定 SHA 的 checkpoint snapshot。

## 還沒用到，需另外驗證的研究方向

| 方法 | 從本模型推導出的用途 | 本輪狀態與下一個必要證據 |
| --- | --- | --- |
| [SF-NorMuon，2026-09 修訂](https://arxiv.org/abs/2605.23061) | 避免固定 horizon 的 cosine schedule，使中途 checkpoint 更可用 | 未跑完整模型。要先把 train/eval weight views、optimizer averaging、恢復與 best checkpoint 接入 canonical lifecycle；只替換 step 會讓驗證/存檔用錯權重。[Schedule-Free 官方介面](https://github.com/facebookresearch/schedule_free)明確區分 train/eval。 |
| [nGPT / Training nGPT，2026-08](https://arxiv.org/abs/2608.01284) | 以 hypersphere 的表示與更新幾何改善訓練尺度 | 未實作。它改 forward 的 activation、weights、normalization 與更新，不等同開啟 RMSNorm；要作獨立架構消融與相容性版本，不能冒充相同 baseline。 |
| [mHC](https://arxiv.org/abs/2512.24880) | 擴展 residual streams，同時限制跨層混合以穩定 identity 路徑 | 未實作。主要論據來自較大型/較深模型；本模型 d32 且較淺，新增 streams 會增加 activation 與搬運，必須先證明 baseline 有梯度傳播問題。 |
| [SAM](https://github.com/google-research/sam) | 對鄰近 weights 的 worst-case loss 做更新，追求對擾動較穩健的解 | 未實作，不是 2026 新方法。需要兩次完整年度 forward/backward，並從完全相同的 recurrent/FIFO 初始狀態重播；不能以第二次接續第一輪帳本代替。需量測兩倍計算能否換到跨年 validation 收益。 |
| EMA/SWA 與 Q/K/V、per-head Muon 分組 | 降低 checkpoint 波動，或避免不同 attention 投影共同正交化 | 未納入此次篩選。應保持 output/loss 相同，另測 averaging 的 train/eval/保存契約及同 budget validation，而不是據短曲線直接替換正式 checkpoint。 |

FSDP/ZeRO 的參數和 optimizer-state 分片只處理這個模型少量 MiB，無法解決主要
activation/source 成本；MoE/DeepEP 要先存在 expert/router 才有工作負載。它們不是
此次 baseline 的優先方向。這是由模型形狀推導的適用性，不是執行成功的宣稱。

FP8/FP4、FlashAttention4、DeepGEMM、TorchAO、CUDA Graphs、attention/norm
kernels 等已在[前一輪 41 項盤點](frontier_kernel_optimization_2026-10-13.md)包含
成功、較慢、不適用或失敗證據，因此不重複標為本輪「尚未嘗試」的方法。

## 公平比較與實作契約

最後 fold11 的 train 為 2014–2024、validation2025、test2026。
所有方法從相同 seed/初始化開始，保持原 batch256/eval16、模型與 source；
test 僅作既有診斷，禁止用它選方法。穩態速度使用 epoch3–12 最大 rank 時間
中位數，完整 fold 時間包含 setup、所有 train/validation/test、曲線/圖、
checkpoint、結尾 backtest 與 lifecycle acceptance。

12 輪全部位於原 warmup32 之內。把 epochs 改成12但留下 t_max0，會把原1000步
cosine 偷換成12步，因此 driver 明確將 t_max 固定為1000、保留 warmup32。
此短測只能檢查機制、有限非零梯度、成本與早期數值，不能認定 optimizer 收斂
勝負。新的 optimizer/options/source revision 納入 training fingerprint，拒絕
載入原 AdamW optimizer；預設 AdamW 的歷史 optimizer 指紋欄位保留。

所有 optimizer 走共用 trainer 的年度 finalizer：全域 gradient mean → clipping →
一次 step → 一次 scheduler。沒有略過 recurrent state、validation、test、plots、
checkpoint 或結尾回測。NorMuon/Dion3 不建立第二個分散式 reduction owner；
DDP 平均後在各 rank 用相同完整 gradient 更新。

上游來源固定為：

- Microsoft Dion：`7692479288f928a3d61d1cc58137b4dbe4f71000`，獨立 dependency target，未升級原 Torch/QuACK/CuTe 環境。
- SOAP：`a1e553530fde97d0e6b307d7c82ac6d38b072340`，原始實作與 MIT notice 原樣保留。
- AdEMAMix：`eb517ed3bbc09227e5d062b682723543f6c71f1a`，原始實作、MIT notice 和 acknowledgement 保留。
- Muon：遠端已安裝的 Torch2.11.0+cu128 原生實作；使用 `match_rms_adamw` scale policy。

## 停止、工程驗證與產物

原 run `formal-v7-20261013T081401-cec1f5b5` 已由 canonical supervisor 取消，
並按 PID/start-time 身分核對它的訓練、DDP、compiler 子樹全部退出；再次讀取 GPU
compute-app 列表為空。Vast instance 本身保留，用於新測試。baseline182、pooling142、
lookback256 的20輪既有 checkpoints 保留，沒有清理舊資料。
證據為 `artifacts/operations/tw-frontier-training-techniques-20261014/stop-receipt.json`。
三份 checkpoint 又經 canonical weights-only CPU loader 獨立回讀，model/optimizer
state 均存在，epochs 分別182/142/20；SHA 與原路徑記錄在
`preserved-checkpoints-readback.json`，不只檢查檔案大小。

本機共享契約回歸222 passed、3 skipped；路徑修正後 optimizer/orchestrator20 passed。
遠端 Torch2.11 的20項測試通過，包含原生 Muon 更新 oracle、scheduler/optimizer
精確續跑、分組不重複、失敗繼續，以及兩 rank global-mean-gradient 的單卡 oracle。
兩卡 strict CUDA preflight 實際 allocation/kernel/synchronize 通過。
工程測試不替代完整模型試驗；正式結果仍須讀取每項 result/lifecycle/curve。

第一個 screen-v1 因 child cwd 指向 frozen source，relative baseline path 無法解析，
九項都在 train 前失敗，沒有 optimizer 更新。已改為預先綁定 absolute paths、
新增 regression，保留原失敗 root，另建 screen-v2。

目前 frozen source SHA256：
`b121804baca8adf229bfadc977f2bc5c927f2db8d3e11ab21d2bfff1a51e5a52`。
受監督 run：`screen-v2-20261013T175430-6539be21`。

遠端結果根目錄：
`/root/stockAgent/artifacts/benchmarks/frontier_training_20261014/screen-v2/`。
各方法的 `full-fold/batch_0256/result.json`、`attempt_01/train.log`、
`attempt_01/artifacts/` 含完整模型 evidence。
可重跑計畫為 [frontier_training_techniques_20261014.yaml](../configs/benchmarks/frontier_training_techniques_20261014.yaml)，
driver 為 [run_frontier_training_techniques.py](../scripts/run_frontier_training_techniques.py)。
操作/驗證 receipts 在 `artifacts/operations/tw-frontier-training-techniques-20261014/`。

## 實測結果

| 已完成方法 | epoch3–12 MAX-rank 中位數 | 完整12輪 fold | 含 driver gates | 最佳 validation loss | 完整產物驗收 |
| --- | ---: | ---: | ---: | ---: | --- |
| 新 AdamW control | 76.447 s | 1,099.617 s | 1,107.827 s | 0.0252261 | 通過 |
| Muon + auxiliary AdamW | 78.061 s | 1,125.999 s | 1,133.469 s | 0.0311255 | 通過 |

此單次觀測中 Muon 未帶來整輪加速，也未改善12步最佳 validation loss；AdamW
repeat 與其他配方仍未完成，不能從這兩次 run 判定統計勝負或長期收斂。
完整 fold 欄為 canonical benchmark 的 train subprocess wall，包含資料載入、
編譯、train/validation/test、plots/checkpoints、結尾 backtest 與 lifecycle。
driver 欄另計 leaf source/runtime gates；整個 screen 共用的一次 source lease 核對
保留在 queue log，不重複加進每項速度。

其餘項目仍在序列測試中，完成結果由 `screen.json` 與各項 `result.json` 增量保存。
不使用 screen-v1 的啟動失敗時間，也不把原正式 baseline 的182輪時序混入新12輪
控制組。此階段不以短 warmup 的 validation 差異宣布收斂勝負。

## 正式消融：baseline 後加入 optimizer

使用者追加要求的正式入口仍是：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh
```

預設已改成 [v8 catalog](../configs/ablations/tw_scale_cash_annual_last_fold_frontier_20261014_v8.yaml)，
共35項：baseline → Muon → NorMuon → Dion3 → SOAP → AdEMAMix → Muon+AdEMAMix
→ Muon matrix LR×2 → 原27項 OFAT。已實作的七個 optimizer 配方進入正式清單；
SF-NorMuon、nGPT、mHC、SAM 等仍列為需要額外機制實作的研究方向，沒有假裝已測。

v8 繼承 v7，以 `matrix.prepend_dimensions` 接入 optimizer，原 dimensions/overrides
不複製、不變動。所有新 optimizer 保留 baseline 的 d32/lookback32/batch256/LR、
1000 epoch 上限、warmup32、early stopping 和完整輸出。原 embedding__d128 仍是
d128，lookback__d256 仍是256，各自保留最後 canonical fold。35項設定與日曆全部
通過；baseline/optimizer/其他26項為fold11，lookback256為fold10。

source SHA256 為
`4beacac6b8a9823e0a6f8097ae4f423a74e9e271ba713502c64e73462fc0b2c1`。
其中 stockagent/backtest 全部與遠端20項測試及短測的 B121 source 相同；新增的是
消融清單/entrypoint/orchestration。另有94項本機消融/排程回歸與5項接續排程檢查
通過，包含使用者取消、owner 消失或短測未完成時不會啟動新訓練。

遠端受監督接續 run：`formal-v8-after-screen-20261013T182331-772d072f`。
目前先等待九組短測的 owner 結束；完成後經 canonical GPU lease、strict CUDA 與
source/data gates 再進入正式隊列。短測中的個別失敗會保留並繼續；取消或不完整
的短測不觸發新正式訓練。正式35項每項失敗記錄後繼續，只有 CUDA OOM 才調容量，
舊 checkpoints/失敗 logs 均保留。

正式輸出根目錄：
`/root/stockAgent/artifacts/markets/tw_scale_cash_annual_last_fold_frontier_20261014_v8/`。
source/dependency binding、calendar receipts 在
`artifacts/operations/training_launches/tw_scale_cash_annual_last_fold_frontier_20261014_v8/`，
接續狀態為 `artifacts/operations/tw-frontier-training-techniques-20261014/formal-v8-handoff.json`。
等待排程或通過 calendar gate 不能描述成35項正式研究已完成。
