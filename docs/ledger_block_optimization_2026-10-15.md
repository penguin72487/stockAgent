# 逐日帳務與反向運算：先量測，再重寫

## 範圍與對照

使用者 2026-10-15 明確允許改寫計算架構、連續數值和梯度軌跡；金融機制仍須成立。
Vastai1T 雙 RTX 5090，已驗收 resident v10 為控制組：d32、lookback32、14726
features、2757 symbol slots、fold11、global128/local64。d128/lookback256 仍是
catalog 實驗，不能冒充目前 baseline。保留全部日期、validation/test、曲線、圖、
checkpoint 與最終回放。v13 驗收時正式 queue 尚未啟動；使用者後續已明確要求
解除眼前瓶頸後啟動，續作與最新啟動狀態記於本文末。

上一輪共同來源 ABBA 的熱態完整 MAX-rank epoch 是 17.351277 秒，冷啟動完整四
epoch fold 平均 456.162654 秒。這輪另用共同來源重新跑完整 A–B–B–A，
不拿歷史最快單次當作本次控制組。

## 已通過的完整流程驗收

| 指標 | 同來源控制組平均 | 優化組平均 | 吞吐量提升 |
| --- | ---: | ---: | ---: |
| 第 3、4 輪完整 MAX-rank epoch | 18.456044 秒 | 5.570165 秒 | 3.3134 倍 |
| 冷程序完整四 epoch fold | 481.042024 秒 | 171.358217 秒 | 2.8072 倍 |

兩個優化組都重用已建置、逐檔 SHA 驗證的 CSR 衍生快取；不能將 171 秒稱為
全新節點首次填入成本。最初 v10 填入紀錄 372.824 秒另存。v13 單一完整
screen 為 224.373 秒，包含該來源首次圖／編譯路徑的暖機，但也已重用 CSR。

| 順序 | 完整熱態 epoch | 完整冷 fold | 結果 |
| --- | ---: | ---: | --- |
| A1 | 19.171008 秒 | 511.945200 秒 | 完整通過 |
| B1 | 5.614144 秒 | 173.476593 秒 | 完整通過 |
| B2 | 5.526187 秒 | 169.239841 秒 | 完整通過 |
| A2 | 17.741080 秒 | 450.138847 秒 | 完整通過 |

同 d32/lookback32、全部 14726 features／2757 slots、global128/local64、
eval16、fold11、1000-step horizon／32-step warmup，每 epoch 一次更新。
驗收包含 validation、test、分鐘回放、曲線／圖表、best／last checkpoints。
兩 B 的 GPU 峰值為 28194 MiB，最低仍有 4.3096 GiB headroom。
兩者均有實際 Graph forward/backward replay，沒有 certificate fallback。
這是工程吞吐量結果，四 epoch 尚未建立策略品質或收斂優勢。

正式收據：
`artifacts/operations/ledger-block-optimization-vastai1t-20261015/ledger-balanced-acceptance.json`。
本機已 SHA 核對四輪的 311 個檔案與 8 個 CPU 可載入 checkpoints；額外 screen
另有 80 個檔案／2 個 checkpoints。這是 v13 驗收時的停止狀態；後續見本文末。

## 第一性原理

1. 每日資本、整股取整和成交容量使帳戶時間軸具有真正相依，不能直接用獨立日
   報酬累乘取代。股票與獨立年度帳戶是並行軸；每日相依應留在 GPU 執行序列。
2. 目前已有完整 session compile 和 terminal-flat cohort 特化；重複加入它們不會
   產生新收益。剩餘候選開銷是 Python 逐日組裝、日曆／狀態小 kernel、空 claim
   列的重複處理、評估的逐日 CPU 同步，以及反向的發射／狀態存活成本。
3. Graph replay 要固定地址、形狀、控制流和緩衝區生命週期。不同日期是輸入資料，
   不應成為不同的已捕捉程式。內層 Inductor Graph trees 關閉，外層顯式管理。
4. 反向可以用分段重算與 VJP：保存必要來源及邊界狀態，共用有限的前向與
   recompute+backward graph 緩衝區。不能讓後一次前向覆寫尚未使用的反向資料。
5. 清除已證明為零的 cohort/claim 必須帶 GPU 證書，保留公共狀態／checkpoint
   表示；證明失敗則完整回放 canonical executor，不能刪掉有權利義務的部位。

Graph 地址與記憶體池要求參照實際 Torch2.11 的
[官方說明](https://docs.pytorch.org/docs/2.11/notes/cuda.html#cuda-graphs)。

## 量測與實作順序

1. 凍結 v7 程式對照，使用已保存的真實 requested actions 與經 receipt 驗證的
   physical source，量測 64 個連續交易日、2757 slots 的 synchronized
   forward/backward；另留 CPU/CUDA trace，計算 kernel 次數、GPU 活躍區間與空洞。
   這是帳務診斷，不是新策略或完整 epoch 成績。
2. 使用固定形狀、日期 Tensor、有限共用緩衝區的純金融 block。先以固定動作對比
   逐日與分段結果，再加顯式 forward 與 recompute+VJP graphs。
3. 若 capture 遇到 host scalar、配置、別名或待完成反向，修改對應的 owner 結構與
   生命周期；不以單次旗標無收益作為方案結論。逐次保存拒絕原因與測量。
4. 將通過的路徑接回共同 simulator/loss/bridge，保留 train/eval/report 同一金融
   owner；新數值／執行身分使用新 fingerprint 和 artifact root，拒絕舊 optimizer resume。
5. 同來源、同 global128/local64、同 scheduler horizon 的完整四 epoch A/B/B/A。
   以 epoch3+ MAX-rank wall time 為熱態指標，同時報完整冷 fold 與編譯成本。

## 驗收

固定來源／動作下：整股、容量、費用、權利、現金／NAV、alive/default、日期和
270 點分鐘曲線符合 canonical 帳務；連續數值允許已記錄的 FP64 reduction 誤差。
梯度可以重排，但要有限且非零，不能令應學習的參數無故斷開。覆蓋尾段、跨年、
清倉失敗、缺價、現金權利和破產回退；失敗不能以忽略來源／交易取代。

驗證完整輸出、CPU 可讀 checkpoint、VRAM headroom 和停止／恢復邊界。GPU busy%
僅輔助：不以空轉、重算無用資料或提高配置量追求顯示 100%。有限工作量與必要
金融遞迴仍有硬體界線；沒有完整量測就不宣稱達到全球計算極限。

## 初期量測證據

- 工程 task：`ledger-block-optimization-vastai1t-20261015`。
- 量測程式：`scripts/profile_carry_ledger_blocks.py`。
- baseline probe：`artifacts/benchmarks/ledger_block_optimization_20261015/baseline-profile-v1/`。
- 原始對照的 rank0 三次暖機後 train block 為 0.520566、0.512279、0.511621
  秒（中位 0.512279）；rank1 為 0.497209、0.500029、0.504723 秒。
  固定 action 的非零 gradient L1 為 0.04464886。cold 首次 162.939 秒另計。
- rank0 train trace 有 21644 個 GPU kernel，合計／區間聯集約 78.24 毫秒；
  CPU user_annotation 前向 692.572、反向 263.164 毫秒。Profiler 有額外負擔，
  不將這兩個數字當成非 profiler 時間，GPU annotation 的 backward duration
  也不能代替跨 autograd stream 的實際區間。
- 此初期測量支持小 kernel 發射／主機間隙候選，而非 Tensor Core 算力上限。
  此處保留診斷紀錄；最終採用數字與收據見本文前段。

## 第二階段規畫：移除已量出的重算成本

專用重算 Graph 在兩卡固定動作、完整分鐘模式的 64 日區塊，MAX-rank 熱態 train 中位
0.512279 -> 0.151640 秒，eval 0.614411 -> 0.063747 秒。完整財務前向與 VJP
對照通過；另修正補齊尾段的重複 decision day，GPU 的不同日期／尾段／pending
backward 測試通過。共同帳務與 checkpoint CPU 回歸 390 passed、15 skipped。

trace 顯示新 train 區塊 CPU 範圍 173.18 毫秒，其中 GPU kernel 聯集 134.10
毫秒；driver/runtime 發射從約 21644 次降到 8 次 GraphLaunch 加 212 次普通
kernel。這是 profiler 範圍內的分母，非 NVML 或完整 epoch 利用率。新 kernel
總數反而 39172，因 VJP 重算一次前向；FP64 prefix scan 合計約 27.21 毫秒。

因此下一步保留固定來源緩衝區，使用有界的 saved-tape slots：前向各自保有
必要中間值，反向在共用來源緩衝區還原該次輸入後直接重播 VJP，避免再計算
一次帳務。slot 在反向完成或 context 釋放前不能覆寫；超過上限或重複反向
改用已驗證的重算 VJP。先測記憶體和 pending-backward，再測真實來源區塊。
若 prefix scan 仍是重要占比，再針對實際 FP64 形狀做專用 kernel；不能重開
已知有問題的通用 SplitScan codegen 而假設等價。

## 實際訓練路徑的整合拒絕與修正

第一組完整四 epoch 的 resident_A1 為 17.596015 秒／熱 epoch、452.620164
秒／冷 fold；explicit_B1 為 19.284437 秒／熱 epoch、446.351279 秒／冷 fold。
後者所有 explicit graph replay 計數為零，因此拒絕這次整合，不能稱為 Graph 成績。
實際訓練使用 canonical 的兩點分鐘摘要，原 admission 卻只允許完整分鐘模式。
停止 B2/A2 以免浪費，保留 A1/B1 的完整產物與 best/last checkpoint；正式訓練
仍未重啟。停止證據為 operation 目錄的 `ledger-v5-integration-stop.json`。

修正版將摘要模式納入固定形狀 Program，沒有把訓練改成另一套分鐘機制。
Vast 的 recompute/saved adjoint × 完整／摘要四組 GPU 測試通過：不同日期、
尾段、待反向前向、slot 上限回退、retain_graph 重複反向均驗證 canonical VJP。
新增 benchmark admission：每個穩態 epoch 都必須實際重播前向、反向並接納帳務
block；未啟用或金融證書回退均不能獲選。相關 CPU 測試 40 passed、4 deselected。
接下來的區塊量測與完整 fold 均使用實際摘要模式。

兩卡真實 64 日摘要路徑的區塊對照已完成：原 runner 的 MAX-rank 暖 train
0.447137 秒、eval 0.517430 秒；saved-tape Graph 為 0.082671／0.051248 秒，
分別 5.409x／10.096x。兩者固定 actions、日期、全部 2757 slots、fees 和財務
owner 相同，VJP L1 均 0.04464886，前向與 canonical 梯度對照通過。
Graph 構建一次、4 個 tape slots，24 次已測反向皆直接使用 saved tape，
沒有重算／金融證書回退；帳務 probe 峰值 allocated 為 3155499008 bytes。
原始證據為 `prototype-control-v8/`、`prototype-profile-v7/`。
這些是帳務區塊成績，尚不是整個模型 epoch 的 5.409x。

## 第三階段規畫：移除重複的不可變來源組裝

實際 resident_A1 epoch4：train 15.225839 秒，其中 loss/backtest host 範圍
8.062376 秒；validation 2.195651 秒，其中來源準備 0.635517、帳務 runner
1.384792 秒。64 日完整 transport packet 為 605039616 bytes。Graph 縮短 runner
後，重複 CPU 堆疊、H2D 的同步與來源 packet 重建會變成較大的比例。

先在既有 PreparedSource owner 加入有位元組上限的不可變 PreparedBatch cache，
key 綁定同一已驗證來源物件、日期列、symbol 投影、摘要／稀疏模式；驗證 scope
後才 lookup。用 pinned host buffers 和同一 CUDA stream 的非同步搬運消除逐欄
等待。只快取外生來源，不快取 model weights、learned embeddings 或帳戶 state。
比較第一次成本、熱 hit、容量、峰值 host/GPU 記憶體與完整 epoch；不能省略
原來源 receipt 或驗證，也不能將非同步提交時間稱為已完成搬運時間。

若 PCIe 仍占主要時間，再測 lossless compressed GPU source cache。真實
2026-02-26 的 FP64 marks 位元 RLE 為 107803 runs，5955120 -> 1089062 bytes
（5.468x），包括 NaN payload 和 signed zero；這只是一日觀察，不能外推全部
日期容量。要在全部 owned rows 和 Graph pool 加入後證明 headroom，再採用。

## 第四階段規畫：獨立實驗共享已驗證 CSR 衍生物

完整控制組的 epoch1 MAX-rank 為 241.660346 秒（train 187.378321、validation
53.894357）；穩態分別約 17.60、15.23、2.20 秒。大量冷成本在每個獨立程序重新
將不可變來源轉 CSR、做 full-schema normalization 和 moments，與帳務速度不同。
Vast 當前可用磁碟約 558.8GB，可容納獨立、可重建的 CSR cache；不修改來源。

在同一 FactorizedSparseRowStore 加入 opt-in 衍生 cache，key 包含來源內容指紋、
symbol 順序、shape、LayerNorm/RMSNorm、epsilon、sanitize、fold-fitted scale
與 active mask 的完整位元雜湊。每個 block 四個 typed arrays 原樣保存，原子
完成 manifest；重用前逐檔 SHA256、size、dtype、shape 與 CSR invariants 驗證。
不快取 learned affine weights 或 NN outputs，不跨不同 fold normalization 誤用。
遺失或損壞只重建該衍生物，保留損壞證據，不刪除來源或既有結果。

先用小型全 affine／梯度 oracle 驗證新程序 disk hit、改 scale/source/norm
隔離、損壞拒用；再在 Vast 真實 shape 量首次建置和後續完整 fold。首次 cache
填入成本與可重用後的冷程序時間分別報告，不能將已填入稱為全新節點首次成本。

## 完整流程發現的生命週期問題

v9 工程 screen 在第一個 train epoch 後進入 validation 時失敗：inference mode
內建立的 pinned tensor 沒有 version counter，不能用同一 mutation guard 接納。
這不是完成的 fold，也沒有策略品質結論。修正是在 PreparedBatch cache 的
put owner 關閉 inference mode，必要時先複製成正常、可追蹤版本的 tensor；
沒有關閉來源驗證或 mutation guard。GPU 回歸特別先在 inference mode 準備來源，
再驗證 bitwise transport 及後續 training 使用。

候選 v10 凍結來源為
`0d92ece2615fc42de9f906003e058dfbfcd772ce4e668f67954d7f536318fc81`。
主工作區相關 CPU 回歸 481 passed、22 skipped、7 deselected；獨立 GPU 回歸
11 passed，涵蓋兩種 adjoint、兩種分鐘表示、尾段、pending backward、eval-first
轉 training、無效 terminal 的 canonical atomic-failure 回退，以及 LayerNorm／
RMSNorm 的 disk-cache 所有參數梯度。消融 catalog／admission 測試 12 passed。
測試來源與 frozen release 分開雜湊保存；失敗的測試／執行紀錄也保留。

接下來對同一來源進行真實 64 日 profile 與完整四 epoch A–B–B–A。v11 消融
入口已準備，但在完整 workflow 驗收前仍以 resident v10 為預設，正式 queue 停止。

v10 的真實來源 profile 再次通過固定動作 canonical forward/VJP：MAX-rank
train 0.082621 秒、eval 0.051854 秒。605039616-byte packet 首次建立仍需約
0.88–0.90 秒；後續命中相同來源／日期範圍，準備中位時間 rank0 0.000296、
rank1 0.000321 秒，完成同步搬運中位 0.027363／0.030544 秒。這包含 sessions
重建與 GPU scatter，不能當作裸 PCIe 頻寬測試；首次成本沒有消失。兩 rank 均
3 hit／1 miss、無 eviction，source packet 的全部 bytes 位於 pinned cache。
原始結果在 `prototype-profile-v10/rank_*/profile.json`，摘要在 operation 目錄
`profile-summary-v10.json`。

## 第五階段：修正真實驗證分派，再量剩餘全模型路徑

prototype v10 三個完整 fold：A1 熱 epoch 17.485850／冷 fold 446.861900 秒，
B1 6.463214／372.824466 秒，B2 重用已填入 CSR 後為
6.392525／196.051253 秒。
B2 epoch1 為 56.760 秒，B1 為 232.508 秒。這是中途結果，沒有完成 ABBA 驗收。
epoch3+ B1 每輪 95 次 forward／backward replay、24 個 accepted batch，無
certificate fallback／recompute；峰值 VRAM 留有 2.946 GiB headroom。

完整指標揭露 validation runner 仍約 1.34 秒，沒有得到帳務 probe 預期的加速。
原因是 simulator 的分鐘 NAV 梯度要求預設為 true，即使 validation 在 inference
mode，舊 admission 仍拒絕 Graph。修正為在未啟用 autograd 時可使用完整 primal
曲線；真正要求分鐘 Jacobian 的訓練仍交給 canonical differentiable executor。
沒有刪減分鐘、日期或驗證工作。新增完整／摘要兩種 inference oracle，直接
對比同一 `require_minute_nav_grad=True` 的 canonical 輸出，再驗證後續訓練 VJP。

停止 v10 第四個控制組以免繼續量已知未完成的整合，保留三個完整 fold 與六份
best／last checkpoints。停止 receipt `ledger-v10-eval-dispatch-stop.json`
證明 28 個 owned processes 無存活、GPU owner 清空；正式 queue 仍停止。
新來源 prototype v11：
`0eab8fb7dba34755923a346802865f7ac1a33b55f7918fb2808f65330ed17df3`。

重新驗證後，以同一 v11 來源重做 A–B–B–A；此時兩個 B 都使用已建置的、
逐檔驗證的 CSR cache，不能將它們稱為新節點首次填入成本。原 v10 B1 填入
紀錄另外保留。最後增加獨立四 epoch 診斷：用 canonical runner，在 epoch3
完整 train 的前後啟停 Nsight CUDA/NVTX capture，解析 NN、帳務、反向、傳輸與
空洞。observer 不修改 frozen 金融／模型程式，保留 optimizer、全部評估和輸出；
profiler 時間不加入正式速度比較。只有實際 trace 支持才能稱哪一部分已由 GPU
運算主導，不能從 Graph 旗標或 NVML 推論 100%。

v11 A1 完整 fold 熱態 17.0563 秒、冷流程 444.969 秒；B1 的 epoch2–4
已達 5.915／6.229／5.651 秒，validation runner 約 0.288 秒。但 B1 在最後
best-checkpoint 的完整回放失敗，因此不採用這組速度。該 artifact 路徑刻意
暫停內層編譯以避免單次大圖編譯成本，顯式 Graph 卻首次捕捉 eager 金融 core：
`budget.new_tensor(1e-9)` 造成禁止的 CPU→CUDA copy，完整分鐘模式另有
`bool(GPU predicate)`。這是 capture/lifecycle 缺口，不能靠跳過報表掩蓋。

v12 直接修正共用 canonical owner：純 scalar 下界使用 device `clamp_min`；
捕捉中不讀回 solvency predicate，而執行原有完整 absorbing-ruin replay。
編譯／未編譯來源分開 engine 身分，防止測試重用先前 compiled Graph 掩蓋
eager 問題。GPU 測試增加兩條實際未編譯分支，驗證完整分鐘與壓縮摘要的
inference-first→training forward／VJP。先完成單一四 epoch 全流程 screen，
確認最終產物完整，再開始共同來源 ABBA。v11 失敗／停止與六份 checkpoints
保留，停止收據證明 29 個 owned processes 無存活、無 CUDA owner。

最終已驗收來源 prototype v13 為
`705ac5056914a7e2cd50a44916a6cde56e3270ffc67975cc049e76f3236ec336`。
額外限制 Graph 只接管既有 FP64/device/shape 的固定費率與外部來源；若呼叫者
要求費率或來源 Tensor 的 Jacobian，交回 canonical autograd，避免固定緩衝區
或 custom action adjoint 無聲切斷其他導數。這不影響本次 baseline 的執行路徑。
CPU 回歸 312 passed、27 skipped、10 deselected；checkpoint／lifecycle 135
passed。另直接對 canonical resume validator 實測 disabled／recompute／saved
全部九種組合：三個同模式允許，六個跨模式拒絕。GPU 回歸 14 passed、20
deselected（73.95 秒），包含未編譯 final-artifact capture；正式 queue 仍停止。

全模型 Nsight 診斷另用既有 `_cuda_timing` owner 加 NVTX phase，保持原本
`profile_timing=False`，不為量測插入每個 batch/phase 的同步。只有完整 epoch
外框同步以定界；CUPTI／NVTX 本身仍有成本，所以其數字不列入 ABBA 速度。

## 最終全模型時間線與剩餘瓶頸

獨立四 epoch 全流程診斷完整完成，`result.ok=true`，包含最後 best-checkpoint
回放及全部產物。只在 canonical trainer 第三個 train epoch 加 CUDA profiler
與 NVTX；兩個 rank 的完整範圍都錄到，量測窗 6.207664 秒。原始
`timeline.nsys-rep`、SQLite、解析收據已回傳本機。此窗含 profiler 與 device-side
CUDA event trace 負擔，不能代替非 profiler ABBA 的 5.570165 秒完整 epoch。

| 同一量測窗 | GPU 0 | GPU 1 |
| --- | ---: | ---: |
| kernel/copy/memset 區間聯集 | 3.957824 秒（63.76%） | 4.696492 秒（75.66%） |
| 去除 NCCL 的工作區間聯集 | 3.956938 秒（63.74%） | 3.388813 秒（54.59%） |
| NCCL kernel 區間聯集 | 0.000886 秒 | 1.307679 秒 |
| 無 GPU 工作區間 | 2.249840 秒 | 1.511172 秒 |
| H2D bytes／copy 累計時間 | 10.8488 GB／0.396433 秒 | 10.0522 GB／0.365812 秒 |

GPU 1 的較高 activity 含等待另一 rank 的 NCCL，不能解讀為更高有效算力。
兩 rank 的帳戶日期數與 NN 批次數不同，已按完整年度帳戶貪婪分配；11 個不可
切斷的帳戶仍會形成尾端差異。這裡的 activity 也不代表 SM occupancy、Tensor
Core 使用率或 NVML 利用率。主機區間／kernel／copy 可重疊，不能相加求 wall。

剩餘優先順序依本次實測：

1. **帳務與反向的小算子仍多。** GPU 0 共 590812 個 kernels，其中 elementwise
   214273 次、scan 13680 次；顯式 Graph 減少主機發射，沒有把它們變成一個
   融合 kernel。下一個結構性方向是固定帳務 block 的融合／persistent scan
   及專用 adjoint，須同時覆蓋資本依賴、整股／容量、費用、權利及破產機制。
   不能把依賴昨日資本的逐日遞迴假設成獨立日乘積。
2. **不可變來源仍要運到 GPU。** 本輪移除了重複 CPU 組裝，但 GPU 0 一輪仍
   傳約 10.85 GB；prepared source 與 resident CSR 爭用 32 GiB VRAM。較小且
   無損的 source ABI、跨 block 預取與雙緩衝值得下一輪驗證；不能用刪分鐘或
   低精度覆寫權威價格來降低搬運量。
3. **普通 autograd／來源還原仍有主機成本。** 兩 rank 合計仍有 71632 次 driver
   launch、52366 次 runtime launch、350 次 GraphLaunch、1158 次 stream sync。
   sync 的 host 累計時間含等候既有 GPU 工作，不能全算成可消除浪費。需要把
   來源填入、證書還原與反向放入更完整的固定程序，而非只增大 Graph 旗標。
4. **跨帳戶批次軸能改善兩卡尾端與單日小工作量。** 真正可並行的是獨立年度
   帳戶；下一輪可改成 `[account, day, symbol]` 的 GPU 執行與排程，但必須重寫
   狀態／padding／adjoint 與 checkpoint 身分，並重測 VRAM。這尚未實作或驗收。

GPU 0 的 top kernels 包含 radix sort 0.380546 秒、scan 0.245745 秒、CSR
multiply 0.227675 秒、attention backward 0.191274 秒；單換 attention 套件不會
讓整輪再快數倍。只以本次 trace 固定 GPU 程式粗估，完全移除 GPU 0 的空洞
也只是約 6.208/3.958=1.57 倍的時間比例；它不是新架構的效能上限或承諾。
要追求下一個數倍，必須改 kernel 數量／資料搬運／帳戶並行，無法靠忙碌率
顯示值推導。**本輪已驗收 3.313 倍增量吞吐，尚未達成「只剩純 GPU 算力」**。

原始診斷與可重跑解析：
`artifacts/operations/ledger-block-optimization-vastai1t-20261015/nsight-v13/epoch3-summary.json`、
`epoch3-cost-breakdown.json`；帳務固定真實動作 64 日 oracle 的兩卡 train 中位
MAX 為 0.083004 秒，非零 action gradient L1 為 0.044648856，完整金融 forward
與 VJP 對照均通過。單卡數字或帳務區塊不取代完整 epoch 成績。

## v13 驗收時的入口、產物與停止狀態

預設指令仍為：

```bash
bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh
```

v13 驗收時它選擇 `--ledger-graph`，綁定 prototype v13 已驗收來源，寫入獨立
`artifacts/markets/tw_scale_cash_annual_last_fold_ledger_20261015_v11/`。
這裡 v11 是消融 artifact 版本，與迭代 source prototype v13 不同。
baseline → Muon →其餘 optimizer／OFAT 共 35 cases，全部已通過遠端實際
`--dry-run` 和 canonical 日曆檢查。lookback256 是自己的最後 fold10，其餘
fold11；失敗繼續和原有 OOM capacity retry 保留。全部設定保持既有模型／資料
維度與金融機制；只驗收 d32/lookback32 baseline，不宣稱各 variant 都已測速。
入口回歸 12 passed；詳見 `ledger-default-validation.json` 與 dry-run log。

核心實作與生命週期 owner：

| 改動 | 位置 |
| --- | --- |
| 固定輸入、日期 Tensor、有界 saved tape 和 Graph/VJP | `stockagent/backtest/tw_day_trade_graph.py` |
| canonical finance capture、失敗證書回退 | `stockagent/backtest/tw_day_trade_carry.py`、`tw_day_trade_inventory.py` |
| byte-bounded、pinning／mutation-safe prepared source | `stockagent/training/day_trade_carry_bridge.py` |
| source／fold-normalizer 綁定的 SHA-verified CSR 衍生快取 | `stockagent/data/factorized_sparse_cache.py`、`factorized_sparse_rows.py` |
| Graph/adjoint fingerprint、拒絕跨 ABI optimizer resume | `stockagent/training/checkpoint_contract.py` |
| 已驗收消融 recipe | `configs/benchmarks/gpu_ledger_graph_best_20261015.yaml` |

工程與正式訓練分開：447 項 CPU 機制／checkpoint/lifecycle 測試、14 項 GPU
測試、九種 resume admission 及四輪完整流程皆通過；短跑沒有驗證策略收益或
1000-epoch 收斂。四輪 311 files／8 checkpoints，額外 screen 80／2，Nsight
完整 fold 80／2，真實帳務 oracle 16 files 已回傳本機並 SHA／CPU-load 核對。
原 best58／last59 checkpoint SHA 未變，formal v9 與被取消的舊候選仍停止；
最後 readback 沒有 CUDA compute owners，沒有重啟正式消融長訓練。

停止、來源、測速和入口證據集中在
`artifacts/operations/ledger-block-optimization-vastai1t-20261015/`，主收據為
`ledger-balanced-acceptance.json`、`ledger-mechanism-validation.json`、
`ledger-default-validation.json`、`ledger-final-readback.json`。

## 續作：完整帳務融合與正式消融啟動

任務 `ledger-critical-path-and-ablations-vastai1t-20261015` 延續相同的 35-case
設定與最後 fold 規則；新要求已授權在驗收後啟動正式 queue。此段取代上文
v13 驗收當時「正式 queue 尚未啟動」的操作狀態。v19 完整測速與正式入口
檢查均已通過；正式 queue 已啟動，實際進度由後面的獨立 readback 證明。

先將 `execute_carry_session` 的準備、交易、收尾、證書與狀態更新納入同一個
固定形狀編譯步驟，外層仍由有界 CUDA Graph 與保存的 adjoint 擁有。財務函式
沒有複製，FP64、來源、費用、整股、逐日相依與失敗回退仍由共同帳務控制。
真實 64 日／2757 slots 的兩 rank 前向與梯度 oracle 通過；v15 的最大 rank
獨立帳務加反向中位數約 0.067402 秒，舊 v13 約 0.083004 秒。Torch profiler
的 rank0 同類診斷由 22800 降到 10380 kernels；kernel duration 總和由
71.221 降到 59.360 ms。這些是局部診斷，不能代替完整 epoch。

v16 加入整批下一批來源預取後，完整四 epoch fold 通過所有產物、非零梯度
與 ownership 檢查，但熱態完整 MAX-rank epoch 為 5.489617 秒、冷程序完整
fold 217.675953 秒，GPU 峰值 30994 MiB／最低 headroom 1.575195 GiB。
這一個 screen 不足以證明比舊版 5.570165 秒更快，因此沒有直接升級正式入口。

接著針對預取本身修改：每個 field-major 配置只呼叫一次 `record_stream`，
不對每個日切片重複登記；下一批只預取前 32 日，消費端再展開後半，限制兩批
同時存活的顯存。另移除 `_Program` 對第一列來源 view 的長期引用，改存欄位
結構與可選設定；所有真正來源仍由本次呼叫、Graph 輸入與反向 tape 擁有。
弱引用測試要求原來源 Tensor 能釋放，並繼續驗證完整前向／VJP。

候選 v19 凍結來源為
`a53f69f68a7300a7699d1d58da5774a3418f5272b2b6fe35595ee0c384923c7a`。
CPU 語義／生命週期回歸 391 passed、35 skipped；遠端版本化 GPU 測試
13 passed，涵蓋 saved/recompute、完整／摘要、不同日期／尾段、pending/repeated
backward、eval-first、證書失敗回退、兩卡預取位元／切片／CPU-cache 隔離。
四種執行身分的 16 種 optimizer resume 組合均按預期接納相同身分、拒絕跨身分。
真實 64 日前向／VJP 兩卡皆通過，最大 rank train 中位數 0.068046 秒；
完整財務前向均已比對，action-gradient L1 為 0.044648855925。

本次完整比較直接採相同 v19 程式的 buffered A 與 fused＋bounded-prefetch B，
次序 A–B–B–A；兩邊都已啟用原 v13 Graph、prepared source cache 與已驗證
CSR disk reuse。模型、資料、fold、global128/local64、eval16、四 epoch、
1000-step scheduler horizon 和全部產物相同。這個比較量的是相對已加速版本的
增量，不會把舊 resident 的 18 秒重新當作新方案的比較分母。

2026-10-14 20:03 UTC 節點 readback：2×RTX5090／各 32607 MiB，cgroup
CPU 配額約 53.76 核、memory.max 259591766016 bytes，檔案系統尚餘約
493 GiB；當時 memory／I/O pressure 的 avg10 均為零。原始 readback 存於
`node-limits-during-balanced-v19.json`；這是本次量測條件，不是其他節點的建議。

新版本的來源、版本化測試、完整流程比較與啟動證據保存在
`artifacts/operations/ledger-critical-path-and-ablations-vastai1t-20261015/`。

### v19 完整 A/B/B/A 驗收

| 順序 | 完整熱態 MAX-rank epoch | 完整四 epoch fold | 最低顯存餘裕 |
| --- | ---: | ---: | ---: |
| buffered A1 | 5.615260 秒 | 170.415854 秒 | 7.965820 GiB |
| fused B1 | 5.006959 秒 | 167.735505 秒 | 1.983398 GiB |
| fused B2 | 4.994480 秒 | 179.123052 秒 | 1.983398 GiB |
| buffered A2 | 5.574853 秒 | 170.297348 秒 | 7.965820 GiB |

完整熱態平均 **5.595056 → 5.000720 秒，吞吐 1.118850 倍，時間少 10.62%**。
完整四 epoch fold 平均 **170.356601 → 173.429279 秒**，反而增加約 1.80%；
沒有建立冷程序四輪加速。正式入口保留 1000-epoch 上限及原 early stopping，
採用較快穩態是針對這個長訓練工作量：四輪多花約 3.07 秒，依本次穩態每輪
省約 0.594 秒估算，再多約六輪可抵銷；這是成本估算，不是完成 1000 輪的結果。
兩個 B 都有實際 Graph forward/backward、saved adjoint，沒有 certificate
fallback；舊分段 prepare/finalize/session compile 計數皆零，確認使用完整融合。

兩 B 的顯存峰值為 30576 MiB，通過既定 ≥1.5 GiB headroom／≤96% peak 門檻，
但餘裕比舊 v13 小。這不是其他 d128/lookback256 的容量驗收；它們維持原研究
設定，只有實際 CUDA OOM 才使用原容量重試，最多四次、最低 global batch16，
各 attempt 都有獨立 optimizer root；失敗後繼續下一案例。

四輪全部日期、非空交易、finite/nonzero gradients、每輪一次更新、ownership、
validation/test、曲線／圖表、最終回放與生命周期通過。311 個結果檔案及八個
best/last checkpoints 已回傳本機，逐檔 SHA 相符且 CPU 可載入；兩卡真實帳務
oracle 另有 16 個檔案已 SHA 比對。主工作區四個核心檔與凍結來源 v19 一致。

v12 spec／recipe 與來源綁定已通過完整 fold、CUDA、財務、checkpoint 驗收：
`configs/ablations/tw_scale_cash_annual_last_fold_fused_20261015_v12.yaml`、
`configs/benchmarks/gpu_ledger_fused_best_20261015.yaml`，正式輸出根目錄為
`artifacts/markets/tw_scale_cash_annual_last_fold_fused_20261015_v12/`。
舊 v9／v11 產物與 checkpoint 保留，拒絕跨不同 step/adjoint 的 optimizer resume。
不宣稱已消除帳戶逐日相依、兩卡尾端負載差或達到 100% 有效 GPU 利用率。

### 正式 queue 已啟動

Vastai1T 於 **2026-10-15 04:16:17（Asia/Taipei）** 以 tmux 啟動正式工作：
`annual-ablations-v12-20261014T201617-7a1f1705`，獨立 task 為
`tw-scale-cash-annual-ablations-vastai1t-20261015-v12`。原指令仍是
`bash scripts/run_tw_scale_cash_annual_ablations_vastai1t.sh`。
入口回歸 12 passed，遠端 35/35 設定與日曆檢查通過，接著完成來源續租、
嚴格 CUDA compute preflight，才交給 canonical sequential dual-GPU runner。

**04:21:17 的獨立 readback 已驗證 baseline fold11 完成 15 個 epoch，兩個
rank 正在持續訓練，last checkpoint 是 epoch15，CPU 可載入。**
首三輪完整 MAX-rank wall 為 45.128272（暖機）、5.537519、4.947215 秒；
每輪一次 optimizer 更新、非零 finite gradient、帳戶 alive，前向／反向均實際
重播 Graph。兩個 rank 的 cwd 是凍結 v19，實際環境逐項符合已驗收 recipe。
正式 runtime 另外加入已綁定／驗證的 Dion 0.1.0 optimizer dependency；
與測速 runtime 比對，Torch/Triton/Quack/CuTe 等既有套件版本均未變。

完整順序為 baseline → Muon → NorMuon → Dion3 → SOAP → AdEMAMix →
Muon+AdEMAMix → Muon LR×2 → 原 27 OFAT；lookback256 用最後 fold10，其餘
fold11。每案仍為 1000-epoch 上限和原 early stopping；實際失敗記錄後繼續，
CUDA OOM 才調容量，d128/lookback256 不縮小。舊 best58／last59 SHA 再次確認
未變。正式結果、checkpoint 與每輪曲線位於上述 v12 output root；尚未完成
35 案，尚無新策略品質或最終收斂結論。

啟動驗收收據：operation 目錄內 `formal-start-acceptance.json`；epoch15 CPU
checkpoint snapshot 為 `formal-initial-checkpoint-last.pt`。工程優化工作可依
此收據結案；獨立正式訓練 task 持續 active，兩者不得混稱已完成實驗。
