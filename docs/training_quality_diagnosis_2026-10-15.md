# 2026-10-15 訓練品質診斷與受控研究

本次目標是提高相對 benchmark 的報酬、Sharpe、Sortino，並減少最大回撤。原正式消融已依要求停止，原產物未覆寫；新方法必須用實際帳務、完整訓練流程及相同研究預算驗證。更快的 kernel 及更多訓練次數，本身不是更好的交易訊號。

最新要求是明確使用 **2330 buy and hold**，並實測改進以超越指定舊 fold11；使用者已取消舊模型微調，改排九組隨機初始化、全網路訓練。主線維持簡單費後 log-utility，另各自測 Sharpe／return／Sortino／Calmar 四個獨立 loss，不做四指標平均。先前 seed／集成研究已取消，歷史微調結果保留作對照；2026-10-18 接續四組從零訓練的 PCGrad／MGDA，第四個目標亦改用 Calmar。使用者指定的 `early_stopping_no_improve_ratio=0.1` 保持不變：最多1,000輪，每組100次 validation 無進步停止；四輪僅作完整工程檢查。舊協議與拒絕結果保留於後段。

原 baseline 目標為 `loss = -252 * mean(log(V[t] / V[t-1]))`，`V` 是 canonical 帳務扣除費稅後的淨值。新四組的完整年度指標對每日實際 log-return 求梯度，再沿原分塊帳務 VJP 反傳；不能將逐批 Sharpe 或 MDD 平均充當年度指標。每個獨立重置資本的年度帳戶只計算所選的一種指標，以有效日期數加權；return 在各年度內複利，MDD 含起始淨值。Return 已實作單次前後向：每個年度帳戶累積 log-return 梯度後，以該年度實際複利值重縮放；其餘三組採兩次固定權重重播，在更新前逐日核對報酬，完整計入兩次成本。既有整張 surrogate 與分塊 BPTT 契約保留，這不是整數交易的經典導數或全年度無截斷反傳。

本文件對應任務 `training-quality-audit-20261015`。可重算證據存於
`artifacts/operations/training-quality-audit-20261015/`，以下簡稱 `OP`。
大型資料、模型、日誌與操作狀態不納入 Git。

## 2026-10-18 23:00：上一輪五組正式完成，九組風險預算與年代實驗啟動

遠端 run `scratch-autonomous-v2-20261018T100558-defc75fc` 已結束，
exit2 對應一組工程容量拒絕，不能稱為六組全數完成；另外五組正式訓練
均達連續100回合無改善。`OP/local-scratch-autonomous-v1-preservation.json`
已驗證869檔案及CPU可讀、有限值的checkpoint。
至此本次神經網路主線共18組正式結果，另有1組工程拒絕；原參數空間MGDA
仍是較強的新模型，下一輪尚無正式績效。

下表仍統一2026-01-02至10-02共181日、相同起始帳戶、實際費後帳務，
MDD為正的回撤幅度。完整結果見 `OP/scratch-autonomous-v1-comparison.json`。

| 方法 | 選中／停止epoch | 費後報酬 | Sharpe | Sortino | MDD |
| --- | --- | --- | --- | --- | --- |
| 保留的參數空間MGDA | 35／135 | 61.81% | 3.095 | 5.324 | 7.34% |
| 解析entmax VJP＋log utility | 282／382 | 13.57% | 0.514 | 0.730 | 15.96% |
| 解析entmax VJP＋參數MGDA | 170／270 | 10.35% | 0.511 | 0.740 | 13.29% |
| 選股／方向／現金分離＋log | 71／171 | 0.53% | 1.027 | 1.863 | 0.82% |
| 選股／方向／現金分離＋MGDA | 19／119 | 0.54% | 1.491 | 2.416 | 0.29% |
| 編碼歷史＋分離輸出＋MGDA | 92／192 | 0.39% | 0.891 | 1.380 | 0.48% |
| 指定舊fold11 | — | 95.93% | 3.662 | 6.353 | 9.57% |
| 2330 adjusted-close buy-and-hold | — | 62.73% | 2.014 | 3.359 | 13.04% |

原參數MGDA沿用當時的舊entmax反向，屬保留的研究控制；不能把其表現較好
解讀成舊反向是正確導數。解析VJP修正、完整策略獲利與跨期泛化是不同證據。
2026已反覆觀察，只能作回溯研究；2025仍單獨選checkpoint，沒有拿2026選epoch。

### 本次第一性原理診斷

1. **兩次壓縮曝險**：原分離輸出為
   `w = sigmoid(-cash) * entmax(rank) * tanh(direction)`。
   其gross除了現金頭，還受方向的共同幅度壓縮；三組平均requested gross
   為5.60%、5.28%、3.85%。這是入場請求，不是成交持倉；目前可證明輸出
   條件不良與低曝險並存，不能宣稱已證明全部低報酬的唯一原因。
   新 `decoupled_entmax_cash_v2` 先將signed selection做L1標準化，再乘
   模型自選的現金風險預算；不強制滿倉、多空比例或與舊策略不同。
   方向完全為零或全部被mask時仍留現金。有限差分驗證選股、方向、現金
   三條梯度；新模型ABI為 `independent_selection_direction_unit_l1_cash_entmax_v2`。
2. **跨期落差**：解析VJP的log組在2025報酬110.71%、2026為13.57%；MGDA組
   為109.49%對10.35%。這支持檢查年代／驗證期適配，尚不能單獨證明
   concept drift。近四年及兩年實驗只改訓練target ownership，保留全部
   原始資料、特徵、panel-history、2025驗證與2026比較日期，各自重擬訓練RMS。
   [DoubleAdapt原始研究](https://arxiv.org/abs/2306.09862)討論金融預測的分布漂移；
   本次用既有共享trainer檢驗較簡單的窗口假說，沒有宣稱實作該論文。
3. **MGDA不是四指標等比參與**：原參數MGDA有124/135輪的log-return係數為1，
   分離輸出MGDA有116/119輪。raw objective units下，最小範數解常退回較小的
   log梯度。此為已宣告演算法的結果，不是四指標平均；本輪保留raw單位
   與純log對照，不從MGDA名稱推論四者一定同步改善。
4. **容量拒絕不等於歷史架構無法訓練**：原encoded-history log在global128 OOM，
   global64完整跑完但GPU0只剩0.919GiB，低於1.5GiB工程預留門檻，因此未正式
   放行。新history組將可重建device source cache從14GiB縮至10GiB，保留
   d32/L32與full-then-last query，依原OOM流程重測；不靠刪特徵或放寬門檻。

### 已排入既有遠端排程的九組

設定：[`tw_scratch_budget_recency_20261018_v1.yaml`](../configs/ablations/tw_scratch_budget_recency_20261018_v1.yaml)。
九組均獨立seed42隨機初始化、全參數訓練；無舊模型權重、teacher、蒸餾或
相似度懲罰。保持1000上限、`early_stopping_no_improve_ratio=0.1`、patience100、
AdamW、d32/L32、所有特徵、費稅與整張帳務；單組失敗保存證據後繼續。

| 排序 | 實驗 | 訓練年度 | 驗證／比較 |
| --- | --- | --- | --- |
| 1 | 單一現金風險預算＋log | 2015–2024 | 2025／2026 |
| 2 | 重跑解析entmax MGDA控制 | 2015–2024 | 2025／2026 |
| 3 | 單一現金風險預算＋MGDA | 2015–2024 | 2025／2026 |
| 4 | 單一風險預算＋整張以下梯度回饋＋log | 2015–2024 | 2025／2026 |
| 5 | 近四年解析entmax MGDA | 2021–2024 | 2025／2026 |
| 6 | 近兩年解析entmax MGDA | 2023–2024 | 2025／2026 |
| 7 | 近四年單一風險預算＋log | 2021–2024 | 2025／2026 |
| 8 | 近四年單一風險預算＋MGDA | 2021–2024 | 2025／2026 |
| 9 | 編碼歷史＋單一風險預算＋log，10GiB cache | 2015–2024 | 2025／2026 |

近四年為canonical fold4、近兩年為fold2，其餘fold10；由來源calendar解析，
不是把不同fold編號當成不同test年份。全九組同243日validation／181日test。
展開spec時已將split-start及source-cache從fixed-overrides移至base-overrides，
並逐組核對生效設定，避免fixed最後覆蓋而抹掉這兩個實驗軸。
整張以下回饋僅作用於允許的log訓練surrogate，actual orders／returns不變。

遠端run `scratch-budget-recency-v1-20261018T150041-ddb6eb5f` 於23:00台北啟動；
已通過strict CUDA及封存來源61項physical／ABI檢查，本機182項通過。
來源SHA `c0881ca148026f45aea37a7e7bc2e67482fbc73f7aa34d3322d35426305e0b32`；
1692來源檔與wheel已驗證離機保存。只加入5個reviewed輸出／ABI檔的改動，
排除工作區其他basis、classical與inference變動，金融executor與資料owner未改。
沿用原resident CSR、QuACK、attention、ledger CUDA Graph、完整圖表／checkpoint；
每組仍先4輪完整工程檢查，再另行隨機初始化正式收斂。
23:07台北第一組 `budget__independent_log` 工程通過並交接正式收斂：
雙卡global128，完整4輪fold296.426秒，穩態完整MAX-rank epoch2.350秒，
GPU最小餘裕2.685GiB。四輪均無zero-gradient、每輪一次optimizer更新，
第3/4輪無新compile graph；兩份checkpoint已CPU有限值讀回。
工程test181日有177日非零報酬、平均turnover0.2601，requested gross0.49984，
證明新輸出與帳務路徑有實際非空工作；這不是正式收斂績效。
證據：`OP/budget-recency-v1/engineering-first-summary.json`。
縮短訓練年代是研究工作量改變，不能冒充整段訓練加速。
目前仍沒有「只剩硬體運算瓶頸」或超越舊策略的驗收證據。

新正式產物：`vastai1T:/root/stockAgent/artifacts/markets/tw_scratch_budget_recency_20261018_v1/`。
工程產物：`vastai1T:/root/stockAgent/artifacts/benchmarks/training_quality_20261015/scratch-budget-recency-v1/`。
本機進度：`OP/scratch-budget-recency-v1-progress-current.json`；
計畫／來源／診斷：`OP/scratch-budget-recency-v1-plan.json`、`OP/budget-recency-v1/`。
完成後本機完整副本：`OP/scratch-budget-recency-v1-retained/`；只有collector核對成功
後才能稱為已保存。watcher與collector沿用既有監督入口與SHA／CPU-readback驗證。

## 2026-10-18：13組從零訓練完成，下一輪檢驗梯度與策略表達

已重新連線核對四組 queue 為 `complete`、run exit0；全部
`no_improve_epochs=100`，不是只跑4輪或64輪的結果。
四組577個檔案已SHA核對，16個工程／正式 checkpoint 可在CPU讀取且數值有限。
證據為 `OP/scratch-multiobjective-v1-{comparison,collection-status}.json`、
`OP/local-scratch-multiobjective-v1-preservation.json`。

下表統一使用2026-01-02至2026-10-02共181日，MDD列正的回撤幅度。
指定舊fold11亦按同樣完整日期重算；使用者原先69.85%的數字是共同149日、
相同起始帳戶的重播，不能混用。

| 從零訓練方法 | 選中／停止epoch | 費後報酬 | Sharpe | Sortino | MDD |
| --- | --- | --- | --- | --- | --- |
| 原v9 log utility | 85／185 | 52.38% | 1.775 | 2.807 | 11.34% |
| 整張回饋 | 276／376 | 46.83% | 1.167 | 1.777 | 21.37% |
| 整張＋稠密探索回饋 | 187／287 | 9.96% | 1.966 | 3.383 | 3.40% |
| 原獨立現金頭 | 101／201 | 2.84% | 1.228 | 1.931 | 1.90% |
| 市場token32 | 375／475 | 7.87% | 1.060 | 1.648 | 5.32% |
| 獨立Sharpe loss | 75／175 | 0.50% | 0.191 | 0.268 | 3.13% |
| 獨立return loss | 35／135 | 36.52% | 1.997 | 3.245 | 9.32% |
| 獨立Sortino loss | 97／197 | 0.32% | 0.143 | 0.176 | 1.89% |
| 獨立Calmar loss | 107／207 | 4.81% | 0.839 | 1.806 | 6.13% |
| 報酬空間PCGrad | 338／438 | 1.29% | 0.087 | 0.127 | 16.89% |
| 報酬空間MGDA | 102／202 | 40.64% | 1.403 | 2.199 | 15.49% |
| 參數空間PCGrad | 188／288 | -0.32% | -0.051 | -0.079 | 7.07% |
| 參數空間MGDA | 35／135 | **61.81%** | **3.095** | **5.324** | **7.34%** |
| 指定舊fold11，完整181日 | — | 95.93% | 3.662 | 6.353 | 9.57% |
| 2330 adjusted-close buy-and-hold | — | 62.73% | 2.014 | 3.359 | 13.04% |

參數空間MGDA是這13組中較強的新策略，仍未全面超越舊模型。
共同149日重播則為新MGDA **50.86%／3.137／5.481／7.35%**，
舊模型 **69.85%／3.485／5.995／9.50%**。
MGDA／PCGrad四組以2025純log utility選checkpoint；原四個獨立loss各依
自己的2025 validation目標選擇。2026已重複觀察，只能作回溯研究診斷。

### 第一性原理診斷與六組新實驗

新MGDA相對舊模型的requested持股集合平均Jaccard為0.299、配置cosine0.400，
已改變選股，不只是舊模型乘一個比例；但平均gross0.3904、net-0.3863，
仍幾乎集中同一方向。這些是requested入場目標，尚不等於實際成交。
`weights_history`及`shares_history`是強制日終平倉的零快照，不能用兩者全零
推論不同模型交易相同；實際執行差異要看canonical帳務重播與交易紀錄。

已在最簡三檔測例實測原entmax二分搜尋反向：
`[0.116311,-0.386382,0.270071]`；有限差分為
`[0.129260,-0.393886,0.264626]`，解析simplex VJP為
`[0.129258,-0.393893,0.264636]`。二分分支的autograd不等於隱函數根的導數。
新opt-in v2保留同一前向bitwise結果，只改用
`sqrt(p) * (v - sum(sqrt(p)*v)/sum(sqrt(p)))`；不把32次搜尋留在反向tape。
梯度定義參考[entmax原始研究](https://aclanthology.org/P19-1146/)，
原模式與舊checkpoint保留，不能將此小測例當成整段績效差距的因果證明。

第二層新增 `decoupled_entmax_cash`：每檔獨立rank與direction head，
rank經標準化entmax形成稀疏選股，direction經tanh決定方向及信心，
contextual cash gate決定可用風險預算；實際gross不足的餘額留現金，
不強制滿倉、多空比例、持有benchmark或與舊策略不同。
第三層保持lookback32及兩層temporal stack，將第一層由last-only改為
所有歷史query編碼，最後一層仍用last query；不新增平方全市場時空attention。

新矩陣為
[`tw_scratch_autonomous_20261018_v1.yaml`](../configs/ablations/tw_scratch_autonomous_20261018_v1.yaml)：

1. 只修正entmax VJP＋log utility。
2. 只修正entmax VJP＋參數空間MGDA。
3. 選股／方向／現金分離＋log utility。
4. 選股／方向／現金分離＋參數空間MGDA。
5. 編碼歷史＋分離輸出＋log utility。
6. 編碼歷史＋分離輸出＋參數空間MGDA。

六組均獨立seed42隨機初始化、全參數可訓練，沒有teacher、蒸餾或舊權重。
保留d32／lookback32、所有特徵、AdamW、1000輪上限／100次無改善、
2025純log utility選checkpoint、原始費稅／整張／來源／2330 buy-and-hold。
沿用resident CSR、QuACK、已測attention及ledger CUDA Graph加速，
各組先完整4輪工程容量及生命週期檢查，再另行隨機初始化正式訓練。
只有確認OOM才依128→64→32重試，失敗保全後繼續。
模型表達與反向ABI均寫入新fingerprint；不載入不相容optimizer。
score-only跨資產重配無法重建三個獨立輸出座標，會明確拒絕，不能默默
套回舊allocator；正常model forward／train／test直接產生完整weights。

正式產物root：`/root/stockAgent/artifacts/markets/tw_scratch_autonomous_20261018_v1/`。
工程root：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/scratch-autonomous-v1/`。
計畫／進度：`OP/scratch-autonomous-v1-{plan,status,progress-current}.json`；
隔離來源與排程證據：`OP/autonomous-strategy-v1/`。
整個epoch／fold仍須計入validation、test、曲線、繪圖及checkpoint。
目前沒有證明只剩硬體計算瓶頸，也不保證新架構必然超越舊策略。

18:06台北已由既有排程啟動第一組完整雙卡工程測試。
run為 `scratch-autonomous-v2-20261018T100558-defc75fc`，其後五組依序銜接。
本機相關176項測試通過；遠端封存來源55項通過，包含實際CUDA Graph。
首次測試入口因ABI測試的相對config fixture不在wheel目錄而在正式訓練前
退出；保留原收據，於獨立測試CWD補齊該fixture後通過，沒有修改來源或
訓練數學規則。來源SHA
`70e543c481d456a8f4ad10cce5e4a38f0c9a9e58fe9991469e46459acc4268a0`，
runtime仍為 `66926c5bcb2bdf743af48d7e9f646ae581f2497db26cbf8b8472235b5dd5eaf7`。
由上一輪精確來源僅帶入5個reviewed source檔的變更，排除同工作區其他
basis、classical及inference改動；1692個來源檔及wheel已SHA核對離機保存。
本機watcher及collector均綁定該run，進度刷新與完整產物保存沿用原工具。

18:11台北第一組 `autonomy__entmax_vjp_log` 已通過4輪完整雙卡工程
fold，開始其獨立正式收斂流程。global128／local64，完整fold301.443秒，
第3、4輪complete MAX-rank平均2.408秒；此為當輪測量，沒有建立較舊
baseline的加速倍數。GPU峰值29,838MiB、最少餘裕2.704GiB，host最少
餘裕84.30GiB，沒有省略validation／test／曲線／checkpoint。
完整證據為 `OP/scratch-autonomous-v1-engineering-admission.json`，
摘要為 `OP/autonomous-strategy-v1/engineering-first-summary.json`。
這是工程驗收，尚無本輪正式收斂績效。

## 2026-10-18：接續從零訓練的 PCGrad／MGDA（啟動紀錄）

重新連線核對後，v9 的九組最後一個 fold 均已完成，全部保存的
`no_improve_epochs=100`。不重設已收斂控制組的 patience，也不把舊模型
權重當成新的隨機策略。接續設定為
[`tw_scratch_multiobjective_20261018_v1.yaml`](../configs/ablations/tw_scratch_multiobjective_20261018_v1.yaml)，
依序比較 return-space PCGrad／MGDA、parameter-space PCGrad／MGDA。
每組獨立 seed42 初始化，全參數可訓練；d32、lookback32、global128／eval16、
BF16 DDP、完整特徵與帳戶、AdamW、收斂規則及 v9 已驗證加速設定均保留。
只有確認 CUDA OOM 才走既有 batch64／32 容量重試；失敗保留證據並接下一組。

四個梯度目標為負 Sharpe、負年度平均 log return、負 Sortino、負 Calmar，
沿用完整年度帳戶和日期加權，保留原始單位，沒有四項平均或隱藏權重。
與只壓低 MDD 相比，Calmar 同時包含複利報酬與回撤，值得檢驗是否能避免
僅縮小曝險的方向；這是研究假說，沒有績效提升保證。
新版契約為 `annual_four_metric_calmar_raw_gram_surgery_v2`，並記錄
Calmar 的年化、含初始 NAV 的回撤及 `1e-8` 分母下限。
舊 MDD 四目標 optimizer 不相容，舊來源封存保留。
四組 checkpoint 仍僅由 2025 純 log utility 選取，2026 僅作回溯診斷；
benchmark 仍為 2330 adjusted-close buy-and-hold。

來源由 v9 精確 bundle 建立隔離工作樹，只改 `multiobjective.py` 與
`return_adjoint.py`，帳務及模型檔案逐檔未變。
source SHA 為 `57f529520c65bf96db2d1bd14db2e92a7d7b510647d6fdc65546e77d3d897273`，
receipt 為 `artifacts/code_releases/training_quality_20261015/20261018T062405296957Z-57f529520c65/release.json`。
遠端 kernel 環境相對舊收據有五項套件版本變化，因此重新記錄 runtime SHA
`66926c5bcb2bdf743af48d7e9f646ae581f2497db26cbf8b8472235b5dd5eaf7`，
各組仍須重新通過完整工程 fold；不把舊速度視為當輪測量。
首次封裝的專案 distribution metadata 不一致已被 runtime gate 拒絕；
修正封裝觀察後重新建立 receipt，未安裝或修改原 kernel 環境套件。

本機相關測試 67 通過、3 項 CUDA 條件略過；遠端實際雙 GPU 的
parameter bank、Gloo／NCCL 帳務梯度及 deferred validation 共8項亦通過。
遠端由既有 agent 工作流
與 GPU leases 排程，先驗證實際雙 GPU 帳務梯度，再逐組跑四輪完整工程
fold，通過後另行隨機初始化正式訓練至100次無改善或1000輪上限。
保留所有 validation、test、曲線、繪圖、checkpoint 與最終重播。
首次遠端測試誤載 checkout 而非封存來源，已在 import gate 失敗並保留
日誌；第二次明確以封存來源為工作目錄，不修改訓練數學規則。

遠端正式 root：`/root/stockAgent/artifacts/markets/tw_scratch_multiobjective_20261018_v1/`；
工程 root：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/scratch-multiobjective-v1/`；
計畫與進度：`OP/scratch-multiobjective-v1-plan.json`、`OP/scratch-multiobjective-v1-status.json`；
環境、驗證、排程證據：`OP/neural-continuation-v1/`。
目前啟動收據為 `scratch-multiobjective-v2-20261018T062614-1e448926`；
實際 phase、epoch 與收斂結果須讀當次狀態檔，不以排程啟動視為策略完成。
本機沿用既有觀察與保全程式，產生 `OP/scratch-multiobjective-v1-progress-current.json`；
佇列完成後下載精確 archive，核對 SHA、CPU 可讀 model／optimizer 與有限值，
再由既有比較器重算完整181日及共同帳戶149日的舊模型對照。

第一組 return-space PCGrad 在 2026-10-18 14:32 台北通過完整四輪
工程 fold：`ok=true`、`lifecycle_ok=true`，完整耗時309.35秒、
epoch3起最大rank中位數3.742秒。四輪各一次optimizer更新，zero-gradient
batches皆為0；末輪四個task的梯度皆非零，帳務重播最大報酬誤差
`7.78e-16`，Graph adjoint實際重播79次、fallback為0。
此為工程結果，不是已收斂策略績效；沒有宣稱只剩GPU運算瓶頸。
該組已自動啟動獨立正式訓練，global128不變，其餘三組依序排隊。
admission見 `OP/scratch-multiobjective-v1-engineering-admission.json`。

## 2026-10-17：自主策略 v8，隨機初始化與全網路訓練

使用者明確取消以舊策略為底的微調，要求神經網路自行學習不同策略。
已停止v7 Calmar的工程程序，停止收據確認無殘留owned process；已完成的
v6b及舊模型只保留作比較。新設定為
[`tw_scratch_strategy_20261017_v8.yaml`](../configs/ablations/tw_scratch_strategy_20261017_v8.yaml)。
每個case獨立隨機初始化，`pretrained_initialization_root: null`、
trainable prefixes為空、跨fold warm start關閉、risk calibration關閉，
不載入舊權重／optimizer／RMS或固定策略表徵。GPU資料、CSR與來源cache
仍可重用；因編碼器要學習，凍結策略輸出cache明確設為0。

### 第一性原理與實驗次序

原風險微調的動作只能落在舊股票向量的縮放路徑上；全網路訓練才能改變
表徵、股票分數、方向與相對權重。單純解凍仍可能卡在兩道梯度邊界：
entmax外的股票沒有自身收益的直接探索訊號，未達整張的目標又沒有該次
成交收益。故先分開整張回饋與稀疏探索，再測動作參數化及市場表徵。
上述是可推翻的機制假說，不能當作已證明的績效歸因。

| 順序 | Case | Loss | 要檢驗的改動 |
| --- | --- | --- | --- |
| 1 | baseline | 費後log utility | 隨機初始化、全網路訓練的控制組 |
| 2 | mechanism__lot_credit | 同上 | 實際一張、費後、可執行且可存活的反事實收益，只補小部位梯度 |
| 3 | mechanism__discovery_credit | 同上 | 在第2組上加dense allocation backward，讓未入選股票有探索訊號 |
| 4 | mechanism__independent_cash | 同上 | 在第2組上用獨立contextual cash gate，自學方向、相對配置及總曝險 |
| 5 | mechanism__market32 | 同上 | 在第3組上市場token由16增為32，檢驗跨股交互表徵容量 |
| 6 | objective__sharpe | 負Sharpe | 相對第1組只改完整年度目標 |
| 7 | objective__return | 負Return | 相對第1組只改完整年度目標 |
| 8 | objective__sortino | 負Sortino | 相對第1組只改完整年度目標 |
| 9 | objective__calmar | 負Calmar | 相對第1組只改完整年度目標，取代獨立MDD |

第2–5組的gradient surrogate明示改變梯度，並非不連續整數帳務的真導數；
實際目標、交易數量、手續費、稅、資格、破產及費後報酬仍由canonical
physical FIFO帳務決定。dense allocation需要一張收益回饋才有探索意義，
故不排缺少該回饋的無效組合。現有sub-lot surrogate僅支援log utility，
第6–9組保持控制組梯度，不能把未支援的混搭假裝成有效設定。
每個獨立loss按完整年度帳戶計算及日期加權，沒有四指標平均。

這輪不增加人工持股名單、固定多空比例、benchmark混合、固定滿倉或種子
搜尋。新初始化不保證動作一定不同；正式結果會保留每日weights／fills。
比較器先核對日期與`carry_universe`順序，再分別對requested與executed
配置記錄持股重疊、方向一致度、配置cosine、L1差異及多空總曝險，
判斷是否只改縮放或已改選股／方向；相似度不進入訓練loss。
市場token只增加聚合容量，避免直接引入所有時空token平方的成本。
方法背景見[entmax原始論文](https://aclanthology.org/P19-1146/)與
[Set Transformer原始論文](https://proceedings.mlr.press/v97/lee19d.html)；
它們支持稀疏配置／集合交互的設計原理，不是本交易系統的績效證據。

### 固定協議、排程與驗收

固定14726個完整模型通道、2757個symbol slots、d32／lookback32、
BF16／雙RTX5090 DDP、global128／eval16、seed42、AdamW LR1e-4、
warmup32／cosine1000。訓練2015–2024、驗證2025、測試2026；
最多1000輪、每輪validation、100次無改善停止，達1000上限仍要獨立
標示是否收斂。benchmark為2330 adjusted-close buy-and-hold。
2026是反覆觀察過的回溯研究，不能稱為全新holdout。

每組依序完成四輪完整工程fold與artifact／VRAM／steady graph檢查，
通過後才從新的隨機初始化正式收斂，baseline排第一。工程optimizer不
移入正式訓練。只有確認CUDA OOM才可降低global batch至64、32，
不減少features／股票／lookback／d_model；失敗留存原因並繼續下一組。
比較時固定完整181日與相同帳戶149日兩個範圍，不混用數字。

CPU配置／模型實例核對九組皆全參數可訓練：一般組1133582個參數、
獨立cash組1133711、市場32組1134094。沿用已驗證來源
`3175c4cc3d01183813b41ae49342e357c22072594b6eded8664533576f28c6f7`，
僅改選定實驗及排程，不把工作目錄其他視窗的服務改動混入訓練。
相關語意測試91通過、1項條件略過；cash專項1通過。九組dry-run及
CPU construction通過；GPU工程結果另以即時收據為準，不預先宣稱加速或品質成功。

遠端佇列於2026-10-17 21:17台北啟動，run ID為
`scratch-strategy-v8-20261017T131739-718b80d6`。已核對owner與child的
boot／PID／start-time仍相符及兩個GPU子程序的歸屬。第一組baseline
於21:24完成四輪工程fold，`ok=true`、`lifecycle_ok=true`，global128
維持不變；完整fold耗時363.15秒，epoch3起最大rank中位數22.85秒。
工程與正式的模型／loss／梯度／初始化設定已比對一致，正式訓練隨即
從新初始化開始，未移入工程optimizer或checkpoint。
尚未產生正式收斂結果，亦未證明只剩GPU計算瓶頸；進度觀察器將工程
epoch與正式epoch分開記錄，直到遠端queue終止，不按固定天數提前停止觀察。

遠端正式root：`/root/stockAgent/artifacts/markets/tw_scratch_strategy_20261017_v8/`；
工程root：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/scratch-strategy-v8/`；
本機收回root：`OP/scratch-strategy-v8-retained/`。
計畫與當前進度入口：`OP/remote/scratch-strategy-v8-plan.json`、
`OP/scratch-strategy-v8-progress-current.json`、`OP/scratch-strategy-v8-locations.json`。

### 追加要求：全網路訓練的剩餘瓶頸

**2026-10-17 UTC計時範圍校正：**下列先前的`epoch_wall_s_max_rank`／
`epoch_max_rank_s`在曲線繪圖前記錄，不能稱為包含繪圖的完整epoch。
完整fold wall time仍包含啟動、繪圖及收尾，該範圍不變。後續source
追加`epoch_workflow_timing.jsonl`，在曲線與lifecycle完成後做雙rank MAX；
canonical benchmark優先採用`complete_epoch_wall_s_max_rank`。最終同source
A/B/B/A須有此欄位才接受，不能混用前後分母。

使用者追加「只剩計算瓶頸」後，正式baseline於第18輪停止；最佳輪也是18，
未收斂。原程序的identity-scoped停止收據為
`OP/remote/scratch-v8-stopped-for-performance-20261017.json`。
last／best checkpoint、曲線與進度保全至
`OP/scratch-v8-performance-v1/paused-baseline/`；本機逐檔SHA核對、CPU載入及
有限值驗證見`local-paused-baseline-preservation.json`。其餘八組尚未開始。

雙rank閉合Nsight範圍從epoch3開始到epoch4開始，包含validation、test與
checkpoint。profile run為`scratch-v8-full-profile-20261017T133524-5fe574bb`，
已成功完成，四輪完整fold的lifecycle亦通過。profile不是未插樁速度排名：
25.73秒範圍內，每卡非NCCL kernel聯集約4.10–4.13秒（15.9–16.0%）；
CPU的CSR source build巢狀range累計16.78／18.98秒，不能與kernel時間相加。
每卡仍H2D約16.55／17.59GB；來源建構112／105次，顯示反覆重讀及配置。
證據為`OP/scratch-v8-performance-v1/{profile-acceptance.json,nsight/coverage.json,nsight/phase-wall.json}`。

全日期normalized CSR約23.03GiB、185個block，GPU LRU預算12GiB。
原日誌中相同日期來源反覆出現，單個block最多重傳37次；這是可改進的
軟體工作集與驗證路徑，不能直接歸因硬體。新增fold／source／normalizer
所屬的有界pinned host快取：磁碟hash與型別檢查仍在首次載入執行，之後
使用獨立持有的已驗證常數bytes；GPU淘汰不再解壓、重算hash或重做pin。
host reuse亦不觸發cold allocator trim。未快取任何learned權重或embedding，
完整14726通道、2757 slots、全參數adjoint及帳務規則保持相同。
預設host budget為0，遠端候選明示32GiB／rank，並保留容量及來源失效檢查。

候選source `1564514f8751d70602b93968ff057f9d60074a0d5ab8f4dbb0381f7f7006358a`
只覆疊`factorized_sparse_rows.py`與`factorized_sparse_projection.py`，runtime
維持原`850a27d1…`；遠端canonical ABBA run為
`scratch-v8-host-abba-20261017T134801-37947ab6`。產物root為
`artifacts/benchmarks/training_quality_20261015/scratch-v8-performance-v1/host-abba/`。
四輪工程fold保留完整驗證、test、曲線、plot、checkpoint與final replay；
四次A/B/B/A皆通過完整lifecycle。epoch3起MAX-rank穩態中位數的兩次平均，
由22.303866秒降至6.636916秒，實測3.360577倍；fresh-process四輪完整fold
卻由320.586203秒變成358.491616秒，尚無完整fold加速證據。B1／B2的完整
fold為388.77／328.22秒，首次填充、編譯與啟動變異尚未完全拆開；不能將
穩態倍數當成短fold或新節點首次啟動倍數。正式100輪無改善的收斂也尚未完成。
數值、容量與逐次結果見`OP/scratch-v8-performance-v1/host-abba/acceptance.json`。

閉合epoch3的後續Nsight run
`scratch-v8-host-profile-20261017T135140-d6105a12`亦通過完整fold。
7.56秒範圍內，每卡非NCCL kernel聯集約4.10–4.13秒，占54.2–54.7%；
CSR source build的CPU巢狀range已降至0.033／0.094秒，但每輪H2D仍約
16.55／17.59GB，尚未達到只剩計算瓶頸。FP32 temporal attention的forward
及backward占主要有效運算；不得因NVML busy或CUDA同步等待直接改稱純計算。
證據見`OP/scratch-v8-performance-v1/{host-profile-acceptance.json,host-nsight/coverage.json,host-nsight/phase-wall.json}`。

第二組八輪完整fold A/B/B/A已單獨比較GPU LRU／LFU淘汰策略，保持年度
帳戶、處理順序、模型、算術及12GiB GPU／32GiB host預算相同；source為
`3c8d39ade469e7e1bb89352298af6996a42f65fa9083f60d748846b3fadf3a8e`。
run為`scratch-v8-cache-policy-abba-20261017T141832-059037e2`，工程root為
`artifacts/benchmarks/training_quality_20261015/scratch-v8-performance-v1/cache-policy-abba/`。
後接`scratch-v8-sync-audit-20261017T142354-5b50fd34`，記錄完整epoch3的
GPU tensor回讀呼叫位置及來源block存取序列；該插樁結果只供定位，不列速度排名。
四次lifecycle皆通過，LRU／LFU的穩態MAX-rank中位數兩次平均為
5.919768／5.456687秒（1.084865倍）；完整八輪fold為307.095585／
309.220898秒，尚無冷啟動整體加速證據。只接受這個已量測的穩態改善，
不把串接的不同對照倍數當成一次完整A/B。訓練metadata快取的累積命中280、
miss40；診斷確認rank0／rank1各約957.7／958.1MB，各自預算1.5GiB，
未見穩態循環淘汰；不盲目加大其預算。
原始比較見`OP/scratch-v8-performance-v1/cache-policy-abba/acceptance.json`。
尚未消除其餘同步，不提前標示工程完成；正式九組仍保留在epoch18。

2026-10-17即時`nvidia-smi topo -m`及PCIe查詢顯示，GPU0為PCIe4 x16、
NUMA3，GPU1為PCIe4 x8、NUMA2，雙卡經SYS連線。此為頻寬不對稱的硬體
證據，並非軟體快取／傳輸排程已最佳化的證據。

接回正式queue前另修正canonical `_build_configs` 的self-base命名問題：
單case以自身generated config為base時，重複準備曾累加`-ablation-baseline`
後綴，改變容量選擇綁定的config SHA。現在保留既有名稱，並非改寫舊的容量
收據；真正修改learning rate仍拒絕相容resume。含一個／三個既有後綴的
重複準備及capacity selection檢查通過，完整排程回歸43項通過。
證據見`OP/scratch-v8-generated-resume-regression.txt`。此控制面修正尚未
混入正在執行的immutable GPU release；下一版source需另外綁定。

同步診斷完整fold亦已通過，來源block trace的unique工作集為rank0
13.736522GiB／rank1 13.183125GiB。重複trace模擬顯示12GiB LFU仍需每輪
傳入約5.38／5.34GiB，14GiB可全留住；這是容量假說，尚非GPU速度或headroom
證據。`scratch-v8-resident-screens-20261017T144638-141c735d`正在依序驗證
14GiB來源快取，以及既有`manual_fp32_last`單query FP32 reduction，保留
d32／L32／global128／eval16、全部日期／通道與完整fold。後者先跑mask、
RoPE、QK與所有參數梯度oracle，再執行六輪雙卡篩選；若採用，必須使用
其獨立數值／optimizer fingerprint，不能把第18輪的舊optimizer硬接過去。
產物在`artifacts/benchmarks/training_quality_20261015/scratch-v8-performance-v1/resident-screens/`。
同步位置／metadata cache及模擬見
`OP/scratch-v8-performance-v1/sync-audit/{rank_0.json,rank_1.json,cache-simulation.json}`。

14GiB與單query篩選已完成。既有`manual`的繪圖前穩態中位數5.059059秒；
`manual_fp32_last`為2.383815秒，完整六輪fold為299.710969秒，最小顯存餘裕
2.854492GiB。mask／空集合、RoPE、QK及梯度oracle在遠端8項通過。
關閉每輪allocator trim的六輪篩選為2.368519秒，改善未達1%，維持原策略。
兩者為篩選，尚不是最終整體A/B/B/A。

完整新版epoch3的Nsight run
`scratch-current-profile-20261017T150733-b7d4a9aa`已完成；雙rank有效kernel
約1.714／1.754秒，占擷取範圍53.1／54.5%，仍非純計算瓶頸。每rank H2D已
降至約2.41／2.85GB；rank0仍有497次D2H、rank1有NCCL等待。其CPU／GPU
時間會重疊，不可直接相加。證據為
`OP/scratch-full-network-performance-v2-profile/{acceptance.json,nsight/coverage.json,nsight/host-boundaries.json}`。

checkpoint第一版只批次複製GPU storage，實測反而較慢。cProfile確認每次
會遍歷約29萬個metadata節點；torch.save亦對約30萬個物件執行persistent_id。
進一步改為：此訓練呼叫所屬的不可變manifest只編碼一次成自帶SHA256的JSON
bytes，可變模型／optimizer storage以64MiB有界分組回CPU，保留dtype、stride、
view alias及metadata。canonical loader驗證SHA並還原原manifest，再執行
checkpoint相容檢查；舊格式仍能讀取。原子替換、fsync及safe readback保留。
這是新root的明示儲存格式，直接torch.load時manifest仍是編碼封套；讀取完整
語義須使用`stockagent.training.runtime.load_checkpoint`。

遠端真實checkpoint payload A/B/B/A含fsync／safe readback的中位數平均由
0.463753降至0.138576秒（3.346554倍），一次manifest編碼約0.075342秒；
此為checkpoint局部量測，不是整個epoch加速倍數。遠端6項storage／metadata／
optimizer接續測試通過，本機完整相關回歸206項通過、1項跳過。
原始證據在`OP/scratch-compact-checkpoint-audit/`及
`OP/scratch-v9-full-workflow-tests.txt`。不匯入workspace中runtime.py其他未經
本輪審查的candle／live-window改動；frozen overlay只包含decoder差異。

非同步繪圖八輪完整fold通過，最後圖及JSONL均覆蓋8輪；繪圖前數值與舊同步
數值不可直接比較。最終完整epoch A/B/B/A run為
`scratch-final-abba-20261017T153125-a727cf7e`，接著做同候選閉合Nsight。
工程root為`artifacts/benchmarks/training_quality_20261015/scratch-full-network-performance-v5-abba/`，
收據為`OP/scratch-full-network-performance-v5-abba/`。正式九組尚未啟動；待
該完整工程門檻完成，使用`configs/ablations/tw_scratch_strategy_20261017_v9.yaml`
及獨立`artifacts/markets/tw_scratch_strategy_20261017_v9/`，全九組從隨機開始，
100輪無改善條件不變。v8的18輪last／best繼續保全，不做optimizer接續。

checkpoint消費端稽核另找到五個直接解讀舊manifest的工具：training rescue
worker／enrollment、ablation audit、artifact comparison與walk-forward診斷。
已改走canonical safe loader，補上新格式與舊格式的同一semantic identity、
救援完整optimizer及損壞SHA拒絕檢查。相關87項測試通過，見
`OP/scratch-v9-checkpoint-reader-tests.txt`。離機archive verifier亦使用此loader。

最終ABBA固定使用v5 source；後續v6只修上述五個metadata消費端，訓練、
benchmark、模型、loss、帳務、data producer及runtime檔案必須逐檔SHA不變。
`OP/verify-scratch-reader-release.py`會核對兩份完整release及實際native／
compact optimizer checkpoint，產生限定範圍的reuse證據。不能將v5速度
寫成已直接量過v6；v9每個case仍需在v6各做四輪完整工程驗收。

續接已納入canonical agent workflow：遠端
`scratch-v9-gated-continuation-20261017T154725-f5c2321d`等待完整ABBA及
閉合profile成功，接著封存reader修正、核對相容性、seal plan並排入正式
九組。本機`scratch-v9-observer-attachment-20261017T154822-60083f3e`會接上
既有progress watcher與完成後archive／common-account比較器。
當前位置統一在`OP/scratch-strategy-v9-locations.json`；等待／排程／工程／
正式epoch分開記錄，不把有process當作已正式訓練。

第一個候選B1六輪完整fold的startup receipt另顯示：panel載入52.137秒、
train-group準備21.341秒、epoch1為121.008秒；六輪完整fold269.737秒，
穩態epoch3–6中位數2.312秒。各scope可能巢狀，不能直接相加作獨立項目。
這說明冷啟動、編譯與收尾仍是重要軟體成本；即使穩態改善接近數量級，
也未建立「只剩純GPU計算」或整個短實驗相同倍數的結論。

最終v5 A/B/B/A四個獨立六輪完整fold已完成，四次完整lifecycle與最終六輪
圖像覆蓋皆通過，計時一律採繪圖／lifecycle之後的雙rank MAX。

| 次序 | 配方 | 穩態epoch3–6中位數（秒） | 六輪完整fold（秒） |
| --- | --- | ---: | ---: |
| A1 | 原配方 | 23.802200 | 370.089899 |
| B1 | host32／GPU14 LFU、單query、compact checkpoint、async plot | 2.311843 | 269.737246 |
| B2 | 同B1 | 2.396513 | 283.300670 |
| A2 | 同A1 | 23.423632 | 362.536467 |

兩次中位數平均為23.612916→2.354178秒，**10.030218倍**；完整六輪fold
平均366.313183→276.518958秒，**1.324731倍**。這是共同v5 source的整體
配方比較，不能拿個別早期篩選的倍數再相乘。A／B數值kernel ABI不同，
均為獨立fresh model；全14726通道／2757 slots、d32／L32／global128、
所有日期、validation／test／checkpoint／plot／final replay保持相同。
使用既有已驗證normalized-CSR disk cache，不是全新節點首次生產快取測速。
原始收據及完整runtime／override已收回
`OP/remote/scratch-full-network-performance-v5-abba/abba-acceptance.json`。
閉合profile及正式九組啟動狀態另以後續receipt為準。

閉合profile亦已成功完成：epoch3兩卡範圍2.549101／2.548077秒，非NCCL
kernel聯集1.762614／1.710465秒，占69.1465%／67.1277%。這是GPU kernel
時間占比，並非SM占用率；NCCL、傳輸及CPU同步時間可能彼此重疊。rank0
大payload D2H從前次497次降至27次（約13.1MB）；來源H2D仍約2.85／2.41GB，
每rank仍有數百個graph launch及眾多kernel節點。未宣稱只剩純計算。
證據見`OP/remote/scratch-full-network-performance-v5-abba/nsight/{coverage.json,host-boundaries.json}`。

reader-only v6 source為
`62df8a3644235eed55cc2d576efe825d063528aade513aa2ad01c2b70d702eb3`，
實際native-v8／compact-v5 optimizer checkpoint的CPU讀回、救援semantic
identity及batch128診斷皆通過；相對v5恰為五個消費端修正，其餘source檔案
及runtime逐檔相同。獨立plan SHA為
`2c995f582f82d2cebe025f9d8286df29dcb30872aa1e594cfd51555f4314e5ae`。
原始證據為`OP/remote/scratch-v9-reader-release-acceptance.json`及
`OP/remote/scratch-strategy-v9-plan.json`。

九組已於UTC15:59:10由canonical排程啟動：
`scratch-strategy-v9-20261017T155910-5c69e388`。baseline在v6完成四輪工程
驗收，完整fold280.292218秒、穩態完整epoch2.351303秒，lifecycle／容量
皆通過；此為v6接續檢查，不是另一組匹配A/B倍數。之後已進baseline的
正式1000上限／100次validation無改善流程。全九組各自隨機開始、全部
可訓練，不讀v8暫停權重、optimizer或RMS。

遠端正式root：`/root/stockAgent/artifacts/markets/tw_scratch_strategy_20261017_v9/`；
baseline實際owner：`_capacity/baseline/attempt-00/training/`，未達OOM所以
global128不變；完成後公開case symlink／fold10產物依canonical runner發布。
每組工程root：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/scratch-strategy-v9/`。
本機progress owner為`scratch-v9-progress-20261017T155937-2e4ecce4`，archive／
共同帳戶比較owner為`scratch-v9-collector-20261017T155938-d121872f`，兩者已
確認running；具體工程／正式輪數分開見`OP/scratch-strategy-v9-progress-current.json`。
完成後自動回傳`OP/scratch-strategy-v9-retained/`並核對SHA及CPU有限值。
目前尚無v9正式收斂結果，不能用四輪工程結果聲稱策略已勝過舊fold11。
正式baseline第36輪的last／best另以不可變inode快照收回
`OP/scratch-v9-first-formal-snapshot/`；兩檔SHA、canonical manifest解碼及
CPU模型／optimizer有限值皆通過，見
`OP/local-scratch-v9-first-formal-preservation.json`。這是訓練中保全，不能
冒充收斂或完整fold結果；後續最佳checkpoint仍由validation自行更新。

## 2026-10-17 21:00：v6b 六組收斂與重開機接續恢復

遠端v6b於19:49台北正常完成，六組皆達到100次validation無改善。
本機18:36重開機造成collector及Calmar continuation中斷，舊JSON的
running不是即時存活證據；依boot／PID／start-time核對原程序消失後，
以canonical run-reconcile恢復觀察及接續，沒有重跑已完成的訓練。
905個產物已在本機逐一驗證SHA；24個checkpoint與12個model檔案皆可
CPU載入且數值有限。149日固定帳戶重播仍在進行；Calmar等待其完成後
由已恢復的continuation啟動，不能把queued稱為GPU訓練中。

以下僅為完整181日（2026-01-02至10-02、相同TWD10M起始帳戶）結果：

| 模型 | 停止／最佳輪 | 報酬 | Sharpe | Sortino | MDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| 指定舊fold11 | 228／128 | 95.9319% | 3.66220 | 6.35313 | 9.57458% |
| v6b Sharpe | 131／31 | 52.1758% | 3.73374 | 6.55253 | 6.08382% |
| v6b Return | 249／149 | 274.3668% | 3.46576 | 5.78057 | 19.35163% |
| v6b Sortino | 100／0 | 96.0693% | 3.66601 | 6.36029 | 9.59091% |
| v6b MDD（已完成歷史組） | 481／381 | 0.1054% | 2.13387 | 14.80256 | 0.00780% |
| v6b parameter PCGrad | 367／267 | 182.8942% | 3.49313 | 5.93512 | 14.84361% |
| v6b parameter MGDA | 935／835 | 259.4791% | 3.51605 | 5.89456 | 18.51148% |

沒有一組在181日同時改善四項指標，因此尚未達成兩個固定範圍皆優於舊模型
的目標。Sortino選回epoch0，不算訓練改善；MDD以大幅降低部位取得極小
回撤，亦不能解讀成全面勝出。Sharpe改善風險比率，但犧牲絕對報酬。
2026仍是反覆觀察的回溯研究。

六組的checkpoint initialization及正式完成audit均再次核對：每組獨立
從指定舊fold11的`checkpoint_best.pt`（epoch128、SHA
`eb27db1a15ac32ae1fc359d11290788f46e4872f98661eaff3c98949f2811150`）
初始化；全部121個舊state tensor維持不變，只訓練新增的97個risk參數。
使用全新AdamW，未導入舊optimizer或上一組的新權重。模型仍為d32／
lookback32；新增risk層調整股票總曝險與現金，凍結股票方向及相對配置。
訓練2015–2024、驗證2025、測試2026；目標fold10與來源fold11的編號
差異來自訓練起年，不代表改用另一個來源checkpoint。Calmar沿用相同基底。

本次證據：`OP/v6b-completed-current-base-and-progress-readback.json`、
`OP/local-historical-risk-v6b-preservation.json`、
`OP/boot-recovery-v6b-calmar-v7-20261017T1259.json`。
同帳戶比較完成後以`OP/historical-risk-v6b-comparison.json`為準，
接續即時狀態見`OP/historical-risk-calmar-v7-continuation-status.json`。

## 2026-10-17：依使用者要求，以 Calmar 取代獨立 MDD 實驗

本次指令抵達前，v6b的獨立MDD組已完成481個epoch，PCGrad仍在執行、
MGDA待跑。既有MDD產物保留為歷史對照，不改寫已固定的v6b plan／來源，
另外接續單一Calmar替代組，其他五組不重跑。PCGrad／MGDA仍保留其原先
四任務梯度定義；本次只替換獨立MDD實驗。

Calmar使用與既有報表相同的費後年化複利報酬及初始淨值納入的最大回撤：
`loss = -expm1(252 * mean(daily_net_log_return)) / max(MDD_magnitude, 1e-8)`。
分母下限只處理零／極近零回撤，避免cash起點NaN並保留有限收益梯度；
不修改既有報表的零回撤顯示定義。每個完整年度帳戶各自計算，再依有效
日期加權，沿既有經逐日重播驗證的return-adjoint／帳務VJP反傳；不得以
batch的Calmar平均代替完整帳戶指標。2025驗證同樣用負Calmar選模，
2026仍是已觀測過的回溯研究。

設定為[`Calmar替代組`](../configs/ablations/tw_old_fold11_risk_calmar_20261017_v7.yaml)，
僅`risk_objective__calmar`。d32、lookback32、global128／eval16、seed42、
舊模型121個state、97個可訓練risk參數、原費稅及帳務均沿用；最多1000輪、
100次無改善停止。使用`annual_calmar_return_adjoint_v1`與新root，不續接
MDD optimizer。相對已驗收source432只改config允許值、objective指紋及
return-adjoint三個來源檔案；其他八種既有objective contract逐一比對未變。
新來源SHA為`3175c4cc3d01183813b41ae49342e357c22072594b6eded8664533576f28c6f7`。
本機相關測試66通過、3項雙GPU條件略過，包含獨立NumPy、有限差分、年度
reset、零回撤／近破產、非等長雙rank實體帳務梯度及不相容resume；遠端
單組dry-run亦通過。工程與收斂尚待既有佇列保全完成後依序驗收。

遠端正式root：
`/root/stockAgent/artifacts/markets/tw_old_fold11_risk_calmar_20261017_v7/`；
工程root：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/historical-risk-calmar-v7/`；
本機保全root：`OP/historical-risk-calmar-v7-retained/`。
排程／進度／比較入口為`OP/historical-risk-calmar-v7-locations.json`；
`OP/historical-risk-calmar-v7-continuation-status.json`區別等待、啟動與正式成果。

## 2026-10-17：v4 完整結果與分離風險校正 v5／v5c

v4 遠端佇列於10:23台北結束，603個檔案已收回並逐一驗證SHA；最佳／最後
checkpoint皆可CPU載入且數值有限。三組達到100次validation無進步，全年
score-head組跑滿1000輪，最後僅54次無進步，**不得稱為early-stop收斂**。
目前沒有任何新訓練模型同時超越指定舊模型的四項指標。

相同149日、TWD10M、固定意圖canonical重播結果（達上限另註）：

| 模型 | 停止／最佳輪 | 報酬 | Sharpe | Sortino | MDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| 指定舊fold11 | 228／128 | 69.8484% | 3.48461 | 5.99518 | 9.5040% |
| v4 全參數／年度帳戶 | 100／0 | 69.7158% | 3.48507 | 6.00349 | 9.5616% |
| v4 全年score-head（達上限） | 1000／946 | 79.5251% | 3.30071 | 5.74952 | 10.0310% |
| v4 近期score-head／年度帳戶 | 762／662 | 79.7092% | 3.25217 | 5.61269 | 9.9627% |
| v4 全參數／連續帳戶 | 100／0 | 69.7158% | 3.48507 | 6.00349 | 9.5616% |

全年score-head最佳946輪的完整181日報酬114.4289%、Sharpe3.57735、
Sortino6.25878、MDD10.0784%；同範圍舊模型為95.9319%、3.66220、
6.35313、9.57458%。這組完成產物生命週期，但尚未滿足100次無進步；
不能把181日數字混入149日表格，也不能把提高報酬說成全面改善。
另補作這組的149日重播並通過來源／動作SHA門檻；上表仍明示其達上限，
不把它升格為收斂結果。證據為
`OP/historical-refinement-v4-capped-score-head-common-account.json`。
選回epoch0的兩組僅重現舊權重在現行數值路徑的表現，不算訓練改進。
證據：`OP/historical-refinement-v4-comparison.json`、
`OP/local-historical-refinement-v4-preservation.json`、
`OP/historical-refinement-v4-formal-progress-current.json`。

完整181日的固定意圖診斷：舊模型平均requested gross0.47575，近期score-head
為0.53638，每日費後報酬相關0.98103，股票權重向量平均cosine0.97618；
共同非零股票未出現方向反轉，但active支持集約15.45%改變。這支持優先檢驗
風險規模與選股耦合，尚不是把績效差異因果分解成槓桿貢獻。
證據：`OP/historical-refinement-v4-risk-direction-diagnostic.json`。

再對經SHA驗證的149日固定帳戶每日log-return直接分解：近期score-head
平均每日log-return是舊模型的1.106532倍，但標準差為1.185620倍、下行
偏差為1.181939倍。因此Sharpe比例恰為`1.106532/1.185620`，Sortino
比例為`1.106532/1.181939`：提高的平均收益不足以補償增加的風險。
對舊報酬的描述性OLS beta為1.161827，殘差平均為每日-1.96589 log-bp；
這是相關結構，不能解釋成因果槓桿效果。兩個新score-head的最大日級
回撤都落在2026-04-30高點至05-13低點。診斷沒有用這些test日期設定
風險規則或挑選參數；它支持先測可學習的狀態風險控制，未證明方法有效。
公式、單位、日期及輸入SHA見
`OP/historical-refinement-v4-return-risk-decomposition.json`。

### 第一性原理與本輪可推翻的假說

v4 顯示改變既有股票分數可以增加報酬，但也增加風險；全參數梯度沒有留下
比初始化更好的驗證權重。分數同時決定entmax相對配置及總風險，兩個控制
混在一起。因此v5保留全部121個舊state tensor，新增一個獨立的風險比例層，
只更新其1個常數參數或97個條件參數，loss仍是canonical費後純log utility。

令舊signed weights為`w`，`g=sum(abs(w))`，新風險log-odds為`delta`：
`w_new = w / (g + (1-g)*exp(-delta))`。`delta=0`時恆等，股票方向與
相對比例不變，總gross仍不超過1；全現金與滿風險端點保持原端點。
條件輸入只取當日既有股票表徵的masked mean、std與mean absolute dispersion，
沒有未來報酬、外加持股數、benchmark混入或新的交易限制。為穩定計算，
`delta`限於[-12,12]；這是明示的新輸出ABI，不是舊optimizer續訓。

若以可分割、固定方向、比例費用的簡化模型表示，單日loss梯度為
`d[-log(1+g*q)]/dg = -q/(1+g*q)`。只調整常數gross通常不會改善
Sharpe；有機會改善它的是利用當日已知狀態分配風險。實際整張、費稅及
recurrent NAV仍由既有帳務／surrogate反傳，不能拿這個簡式替換。
這只是待測假說：校正資料也是舊模型訓練期，並非out-of-fold訊號；仍可能
高估歷史edge，或在2025驗證／2026回溯期失效。只有實測能決定。

### 計算方式與驗證

凍結encoder後反覆計算相同表徵是可移除的軟體工作，v5新增每rank最多
1.5GiB的CUDA表徵LRU；只在可更新參數全部位於score/risk head且dropout=0
時允許。key綁定來源fingerprint、精確row序列、mask、symbol IDs、AMP／
train-eval模式，並以所有非head參數與buffer的指標及版本作generation。
權重／RMS變動會失效；CUDA Graph輸出須複製成私有storage並以event保護。
市場互動與head每次重算，沒有快取仍可更新的結果。epoch curve另記
cache hits、misses、evictions及bytes，之後以完整DDP流程實測效益。

本機11項新增測試與220項相關回歸通過；從固定source26只套用本輪7個
來源檔案的隔離版本再跑171項測試通過，包含CUDA/BF16/compile的head
更新後輸出與梯度相符、失效與容量淘汰、零初始化權重遷移及指紋隔離。
隔離版本刻意排除另一工作視窗的live-inference修改。這些測試不等同遠端
完整fold、正式收斂或策略品質驗收。

設定：[`tw_old_fold11_risk_calibration_20261017_v5.yaml`](../configs/ablations/tw_old_fold11_risk_calibration_20261017_v5.yaml)。
四組為constant annual、conditional annual、conditional continuous及conditional
recent（2020起）；其餘2015起，均2025驗證／2026回溯。舊權重／RMS、seed42、
d32/lookback32/global128/eval16、AdamW、warmup32及1000輪上限保持；
新風險層LR0.001，耐心100。每組先4輪完整工程驗收，通過才重新初始化正式訓練，
失敗保留並續跑。新遠端root為
`/root/stockAgent/artifacts/markets/tw_old_fold11_risk_calibration_20261017_v5/`，
本機收回root為`OP/historical-risk-v5-retained/`，佇列／收回／比較證據位於
`OP/historical-risk-v5-*.json`。

12:09台北已由既有tmux排程啟動
`historical-v5-controlled-queue-20261017T040903-917478b1`，來源SHA為
`a62e01a08ad3ba0ed8031a2837d8b63be434cfed5364e3ee6a34b9adbc03948f`，
runtime未變；四組dry-run通過。另有本機canonical supervisor負責工程／
正式進度讀回及完成後SHA收回、CPU checkpoint驗證與固定帳戶重播比較。

12:17台北觀察：constant annual工程4輪及完整生命週期通過，完整冷fold
300.946秒、epoch3+ MAX-rank中位1.08741秒、最少顯存餘裕11.626GiB；
epoch4快取命中164／未命中36／淘汰0，約517MiB，新增Dynamo圖0、
零梯度batch0。最佳epoch4讀回全部121個舊tensor相同，僅新風險參數改為
0.000312499。conditional annual工程中，其餘待測，正式收斂尚未開始。
新head與快取同時變動，這不是隔離快取效益的ABBA，也不能把1秒穩態
當成301秒冷啟動fold或策略改善。後續比較保留validation、test、曲線、
checkpoint及最終重播。

### 12:22台北修正版v5b：逐組設定綁定

逐組檢查實際`config.yaml`發現原v5工程recipe共用第一組的baseline，
因此`conditional_annual`實際用了constant head。其4輪數字不得視為
linear head驗收。原佇列已取消並查驗全部子程序退出、正式`.pt`為0；
第一組有效證據、第二組誤跑及第三組中止記錄均保留，原停止證據位於
`OP/remote/historical-risk-v5-stop-request.json`與`*-stop-verification.json`。

修正canonical `run_frontier_fold_queue.py`，讓每個recipe可指定自己的
`baseline_config`，省略時保留原預設。v5b每組直接指向各自完整有效設定，
並在正式admission前比對全部FinancialTransformer模型欄位，以及loss、
optimizer、LR、seed、trainable prefixes、初始化來源、split、benchmark、
年度帳戶及sharding。新增回歸明確用constant與linear兩個檔案，防止再次
繼承另一變體；本機及隔離來源各8項queue／trial測試通過。

本輪實驗與正式輸出root不變；工程root改為
`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/historical-risk-v5b/`。
來源相對source26共8檔（原7檔加canonical queue），SHA為
`d015f1708e15139e5c7983ff6e9ac34354c32f79469e635cdce76ba6c7491989`。
12:22重新啟動`historical-v5b-controlled-queue-20261017T042250-cf8e0865`，
重新做四組完整工程驗收後才正式收斂。最新位置表為
`OP/historical-risk-v5b-locations.json`，本機收回目錄為
`OP/historical-risk-v5b-retained/`；觀察／比較／收回證據皆改用
`OP/historical-risk-v5b-*`，原v5位置表標記superseded。

### 12:53台北v5c：凍結完整策略，風險反傳使用私有暫存

v5b四組皆完成原工程門檻，12:41進入正式流程。之後增加的壓力測試發現：
相同編譯數學，連續保留三批forward後合併一次backward，暖機後的
encoder快取加CUDA Graph路徑輸出相同，參數梯度卻不相同。四向控制中，
關閉快取或關閉graphs的第二次更新梯度一致；兩者皆開啟時，最大梯度差
0.0327222。這是d8／28特徵測試的暫存所有權問題，不能拿數值軌跡許可
略過；也不能倒推它解釋v4或更早模型的策略表現。正式executor主要逐批
反傳，此測試覆蓋較強的多批合併情境，尚未證明先前正式逐批路徑有同樣錯誤。

先SIGSTOP核對，再對已驗證process identity的29個所屬程序送SIGTERM／
SIGCONT，全部退出。停止時沒有正式epoch curve，僅有一個初始化guard
checkpoint；不把它算成正式收斂結果，不沿用optimizer。原工程產物與停止
證據留在v5b位置。原v6條件續跑也已撤回，不會使用這個來源。
證據：`OP/risk-gradient-ownership-two-updates.txt`、
`OP/remote/historical-risk-v5b-gradient-stop-verification.json`。

v5c把凍結範圍推到完整舊策略：只訓練risk head時，cache保存舊weights
及計算risk log-odds所需的masked特徵，encoder、市場互動與舊score均只在
miss時計算。score權重、所有其他凍結參數與buffer的版本都納入generation；
temperature、mask、精確row與來源身分也納入key。超出容量的單次輸出
同樣取得私有storage，保留LRU與跨stream event。任何可更新score不走
此完整策略cache，仍重算score。沒有快取可更新的風險輸出。

大部分凍結前向保留CUDA Graph；新增的微小FP32風險readout獨立做Inductor
融合，其backward不使用圖共用暫存。這同時移除每輪凍結market／score的
重算，並讓多批risk forward可安全合併反傳。cache ABI升為v2，關閉功能
仍保持舊模型指紋；使用新root與fresh optimizer，拒絕舊ABI的resume。
本機隔離來源175項測試通過，包含原失敗的多批反傳、風險更新、凍結score
實際呼叫次數、temperature／score修改失效與既有遷移／checkpoint／queue。
另以獨立FP64解析式`dw_new/ddelta = w_new*(1-g_new)`檢驗常數及條件層
VJP，兩項皆通過；此處不依靠同一實作互相比較，亦未修改固定來源。
較廣的隔離模型測試為44通過、1項既有CPU fullgraph推論不支援；在未改的
source26父來源重現相同`torch.is_inference_mode_enabled` tracing失敗。
目前工作目錄已有另一視窗的相關修正，該目錄模型／trial測試62項通過，
本輪沒有將整份推論修改混入固定訓練來源，也不宣稱固定來源全套測試通過。
對照為`OP/risk-calibration-v5c-parent-source-shared-model-control.txt`，
訓練使用所選factorized sparse路徑，仍須以下遠端完整驗收。
遠端仍須先通過相同GPU oracle，再跑四組完整工程fold；本機通過不是遠端
工程或策略品質驗收。來源SHA為
`432b44766a5ba2db8ef55a1adf070fbbd9db49e45385f719ac6a177eacb1c435`。

設定：[`v5c純log風險校正`](../configs/ablations/tw_old_fold11_risk_calibration_20261017_v5c.yaml)，
遠端正式root為
`/root/stockAgent/artifacts/markets/tw_old_fold11_risk_calibration_20261017_v5c/`，
工程root為`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/historical-risk-v5c/`，
本機收回`OP/historical-risk-v5c-retained/`。最多1000輪、100次無進步與其餘
既定設定不變，四組都重新驗收。

若收回且驗證完成的v5c比較仍無四指標勝者，再接續
[`v6b六組獨立風險目標`](../configs/ablations/tw_old_fold11_risk_objectives_20261017_v6b.yaml)：
Sharpe、Return、Sortino、MDD各自一組，再加parameter-space PCGrad／MGDA。
前四組各自用自己的validation目標選模型；兩組多目標使用既有純log選模
契約，不把四個指標平均成scalar loss。全部固定同一舊選股與97個risk參數。
MGDA的參數surrogate幾何不代表整數帳務、AdamW或有限步長的共同改善保證。
遠端正式root為
`/root/stockAgent/artifacts/markets/tw_old_fold11_risk_objectives_20261017_v6b/`，
本機收回`OP/historical-risk-v6b-retained/`；條件排程須核對前輪plan、比較與
保全回執SHA後才啟動。這仍是反覆觀測2026的回溯研究，沒有未見holdout
或實盤超額報酬保證，目前亦尚未達成四項指標全面超越。

遠端首兩次GPU oracle在數值斷言之前，因Inductor提示小型softmax不採用
online reduction而中斷，沒有啟動工程或正式optimizer。第一次重試誤把
pytest `-W`的message當regex；讀取pytest實作後確認它會escape為literal。
修正版保留原project warnings-as-errors及既有例外，只在config filter追加
`torch._inductor.lowering`的該句提示regex；獨立查驗其他compiler warning
仍會raise。模型、plan、loss與數值／梯度斷言皆未變動；失敗log與adapter
SHA保留於`OP/historical-risk-v5c-compiler-warning-retry*.json`及遠端原log。
最新遠端run為`historical-v5c-oracle-retry-20261017T045939-4da37b82`，
正式進度需以`OP/historical-risk-v5c-formal-progress-current.json`讀回為準。

13:19台北讀回：遠端兩項ownership／失效GPU oracle通過，v5c四組
均完成4輪工程與生命週期驗收。constant annual冷fold285.346秒、
conditional annual297.013秒、conditional continuous288.571秒、
conditional recent239.469秒；epoch3+ MAX-rank中位依序為
1.05295、1.09154、2.31973、1.10098秒。四組epoch4新增圖0、
零梯度batch0，均有非零有限梯度；此為單次順序讀回，沒有因果加速比。
13:19:26正式佇列已啟動，先跑constant annual，其餘依序；當時尚無
完成的正式epoch。耐心100、上限1000不變，不把工程4輪當收斂績效。
13:23:47讀回constant annual正式第17輪，validation持續改善、當輪
無進步計數0；其餘三組待跑。四組工程最佳checkpoint獨立CPU讀回，
全部121個舊state tensor皆逐值不變，只有1／97個risk參數更新，完整
凍結策略cache均啟用。此證據位於
`OP/remote/historical-risk-v5c-engineering-risk-identity-readback.json`。
所有路徑見
[`目前產物位置`](../artifacts/operations/training-quality-audit-20261015/training-artifact-locations-current.json)，
v5c／v6b各自有`*-locations.json`，v6b controller等候v5c收回及比較。

## 2026-10-17 01:41 台北：修正延遲編譯，啟動舊策略受控適應 v4

**以下是啟動當時的02:17台北觀察，最終結果見上節：4/4完整工程fold通過，02:09已正式開始依序收斂，
上限1000、耐心100；控制組完成正式第10輪，另外三組待執行，尚無新的正式收斂績效。**

目標仍是超越使用者指定的舊fold11，不以工程4輪作策略結論。四項金融指標
必須在相同日期、TWD10M起始資本、固定動作重播及canonical費後帳務比較。
舊模型共同149日門檻為return69.848405%、Sharpe3.484615、Sortino5.995183、
MDD9.503996%；完整181日是另一個比較範圍，不能混用。

### 目前證據支持的機制與尚未證實的假說

1. v3兩組都從舊權重及RMS開始，但4次完整訓練更新使2025驗證loss由
   -0.76519364變為-0.6983796（LR1e-4）／-0.7550000（LR1e-5），同時
   訓練loss下降。這是訓練與驗證的局部方向不一致，不能推論梯度程式必然錯誤，
   也不能以繼續降低train loss當成泛化改善。
2. 新版年度帳戶與舊版連續訓練帳戶不同。若交易完全可分割且費用為比例，
   log增量相加對年度分段可相容；實際整張、資金限制與取整依賴NAV，因此
   重設資本會改變成交數量、資金約束和代理梯度。這是需要隔離測試的訓練
   機制，不是已證明的唯一退步原因。驗證／測試帳戶規則保持不變。
3. 全參數微調可同時移動已學好的表徵與配置。輸出層微調保留原表徵，只改
   股票分數與其內含風險配置。近期資料組仍初始化完整舊模型，因此只代表
   「近期微調梯度」，不代表刪除舊權重已學到的歷史資訊。
4. v3的新增3個Dynamo圖，與首次平行forward／adjoint安全回退及新增一個
   serial graph owner同輪發生。原canonical路徑第一次實際使用才編譯，與
   迴歸或金融證明門檻無關；不應放寬圖穩定門檻或移除安全回退。

命名參數核對：全模型1,133,582個參數，`score_head.`僅2,145個（約0.189%）。
v3小LR四次更新後，Candle Encoder、temporal blocks、score head相對L2變化約
0.00333%、0.00325%、0.00401%，latent／market queries約0.0205%／0.0224%。
小參數位移仍可能跨越entmax與整張成交邊界；這些數值不證明某一層是根因。
統計排除persistent RMS buffer，避免巨大normalizer尺度掩蓋實際learned-weight變化。
證據為`OP/historical-refinement-v3-parameter-displacement.json`；CPU只用Torch backend
建立參數名稱清單，沒有執行forward或變更遠端QuACK設定。

另直接核對舊曲線：舊模型第1、128、228輪皆為`train_optimizer_steps=1`，
各21個batch；純log、LR1e-4、warmup cosine、weight decay0.01、global128及
lookback32均有原manifest證據。不能將舊模型較好籠統歸因為「每輪更新比較多」。
指定舊模型最佳128／停止228，驗證loss最佳-0.76473331，最後已退到-0.58177215，
顯示對這條訓練軌跡，持續降低訓練loss沒有持續改善驗證表現。

另CPU載入指定epoch128的最佳checkpoint：只含模型／fold／manifest等欄位，沒有
optimizer state。不能拿最後epoch228的Adam moments宣稱恢復epoch128原軌跡；
本輪明確只初始化舊權重與RMS，重新建立optimizer。

v4在每個平行帳務形狀建立時，預熱並保留原canonical executor的forward及
VJP固定buffer，再允許平行replay。開關為
`STOCKAGENT_DAY_TRADE_PREWARM_CANONICAL_FALLBACK=1`，預設關閉；啟用時
加入checkpoint契約及engine cache identity。沒有修改證明條件、成交數量、
費稅、absorbing default或梯度公式。本機CUDA強制turnover回退測試已通過，
回退前後新增Dynamo圖為0，完整state梯度符合原路徑；實際稀疏形狀與遠端
容量／DDP仍由本輪工程fold驗證，不能只由此單元測試推定。

本機兩批focused檢查分別2／2及10／10通過，包含實際CUDA的dense K4與sparse
K16 pending-forward、全部獨立loss的state VJP、強制canonical回退及新增圖為0，
以及資金／整張邊界證明。第二批耗時456.91秒，完整結果保留於
`OP/historical-refinement-v4-focused-tests.txt`。

新來源SHA `0c427401f1194b56e263a08d4aca1652c91e7cd61ce92c8a431eae75ef06c6ea`，
相對source25只改`tw_day_trade_graph.py`及`tw_day_trade_parallel.py`。
runtime SHA未變，舊optimizer不匯入。portable設定為
[`tw_old_fold11_refinement_20261017_v4.yaml`](../configs/ablations/tw_old_fold11_refinement_20261017_v4.yaml)。

| case | 訓練資料 | 可更新參數 | 訓練帳戶 | 最後fold |
| --- | --- | --- | --- | ---: |
| `adaptation__all_parameters` | 2015–2024 | 全模型 | 年度重設 | 10 |
| `adaptation__score_head` | 2015–2024 | `score_head.` | 年度重設 | 10 |
| `adaptation__recent_score_head` | 2020–2024 | `score_head.` | 年度重設 | 5 |
| `adaptation__continuous_all_parameters` | 2015–2024 | 全模型 | 連續 | 10 |

四組維持同一舊權重、RMS、seed42、d32/lookback32、global128/eval16、純log、
AdamW LR1e-5、warmup32/cosine1000。每組先4輪完整工程fold，通過原1.5GiB
顯存餘裕、epoch3+無新增圖、梯度與lifecycle門檻，才重新初始化正式訓練，
上限1000、耐心100。2025驗證選模且保留epoch0，2026已被反覆觀察，只作
回溯研究報告；未發現全面勝過舊模型以前，不宣稱完成目標。

4/4 dry-run均通過，逐組最後fold正確。遠端既有tmux任務run為
`historical-v4-controlled-queue-20261016T174134-68559500`。
第一組先驗證prewarm在真實工作負載的成本；失敗保留並續跑其餘組，未豁免驗收。

01:54台北更新：前兩組皆已完成完整4輪工程驗收。

| case | steady epoch3+ MAX-rank中位數 | 完整4輪fold | 最小顯存餘裕 | 驗收 |
| --- | ---: | ---: | ---: | --- |
| all parameters | 26.029秒 | 403.599秒 | 1.644GiB | 通過 |
| score head | 18.284秒 | 339.875秒 | 11.146GiB | 通過 |

這是各一次工程觀察，不是ABBA效能證明。all-parameters第4輪有一次實際平行
forward回退，該輪仍新增0個Dynamo圖，確認原先的延遲編譯觸發已被處理。
score-head第3輪驗證loss為-0.76546305，優於其epoch0的-0.76519364；仍只是
早期驗證訊號，不能當成正式收斂、測試績效或投資結論。近期資料組已接續執行，
此時正式收斂尚未開始。

02:01台北再以實際4輪last checkpoint核對：121個state tensors中，117個非
score-head張量與指定舊模型完全相等，RMS相等，僅4個score-head權重／bias
張量改變。這是已載入產物的freeze readback，不只是config／參數數量聲明。
收據為`OP/historical-refinement-v4-head-freeze-readback.json`（遠端原件），
本機副本在`OP/remote/`。

02:03台北更新：近期輸出層組也完成4輪完整工程驗收，epoch3+新增圖均0，
MAX-rank穩態中位數1.923秒；第3輪驗證loss -0.76554126。此組只有2020–2024
的微調梯度，不能把對照18.284秒的差異直接叫作相同工作量加速。舊權重、
正規化、完整特徵、2025驗證與2026測試未刪減；只改動已宣告的微調期間。
共3/4工程組通過，最後連續訓練帳戶組已啟動。

02:10台北完整工程結果：最後連續帳戶組亦通過，穩態MAX-rank中位數32.268秒、
完整4輪fold394.043秒、最小顯存餘裕2.415GiB。四組epoch3+均新增0個Dynamo圖。
近期輸出層組完整4輪fold487.898秒、最小餘裕13.886GiB；不能用1.923秒熱身後
epoch時間取代487.898秒的cold完整流程，後者包含新視窗統計與編譯。

控制組及連續組的最佳工程checkpoint都是epoch0，121個state tensors均與
指定舊模型完全相等。兩組相同2015–2024資料的**首輪更新前**train loss分別
為-1.51775384及-0.64470530；改變年度重設確實改變純log看到的金融路徑，
不是只改會計報表名稱。整張的最小可配置比例隨NAV變動，成交量上限限制可擴張
的部位；連續複利與每年重設因此對取整、資金及流動性施加不同條件。
這項證據不單獨證明哪個機制令舊模型更會泛化，也不表示訓練loss越負一定越好。

正式訓練已於02:09啟動第一組，實體checkpoint／曲線root為
`/root/stockAgent/artifacts/markets/tw_old_fold11_refinement_20261017_v4/_capacity/adaptation__all_parameters/attempt-00/training/`，
當前worker日誌為同root下`ablation_worker.log`。生成config已讀回驗證cap1000及
ratio0.1，不匯入工程optimizer；其餘三組依序執行。完整即時證明為
`OP/historical-refinement-v4-readback-current.json`（本機副本在`OP/remote/`）。
本機已有獨立受監督的產物collector及固定帳戶比較job等待完成，目標仍未達成。

02:17台北以正式root讀回：控制組完成10輪，no-improve=10，最佳仍epoch0；
epoch3+ MAX-rank中位數21.180秒，第10輪新增Dynamo圖0、重試0、零梯度batch0，
每輪optimizer step仍1。這是正式進度及當前梯度／編譯觀察，不是100輪收斂結論。
其他三組目前正式epoch均0，先前各4輪屬工程驗收，不能合併成正式進度。
每30秒讀取canonical `progress.json`及正式曲線的監看檔為
`OP/historical-refinement-v4-formal-progress-current.json`；排程、collector與比較job
仍運作，尚未完成的正式產物未宣稱已回傳本機。

Vastai1T位置：

- 正式root：`/root/stockAgent/artifacts/markets/tw_old_fold11_refinement_20261017_v4/`。
- 工程root：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/historical-refinement-v4/`。
- 進度：`OP/historical-refinement-v4-status.json`，工程／正式日誌為
  `OP/historical-refinement-v4-engineering.log`、`OP/historical-refinement-v4-formal.log`。
- 單組最佳：正式root下`<case>/fold_10/checkpoint_best.pt`；近期組改為`fold_05`。
  最後checkpoint與逐輪曲線在各組`train_*/`；若容量重試，實體路徑由`_capacity/`
  selection指向，保留原始config及每次嘗試。
- 固定協議與SHA：`OP/historical-refinement-v4-plan.json`。
- 本機監看：`OP/historical-refinement-v4-collection-status.json`；完成後回傳至
  `OP/historical-refinement-v4-retained/`，逐檔SHA及CPU載入證明另存
  `OP/local-historical-refinement-v4-preservation.json`。尚未完成不能視作已有副本。
- 正式逐組進度：`OP/historical-refinement-v4-formal-progress-current.json`，未啟動組
  明確標為queued／0輪；collector的工程fallback進度不可當成正式epoch。

## 2026-10-17 01:31 台北：v3工程結束，正式訓練未啟動

即時核對Vastai1T：沒有trainer／queue程序，兩GPU均0%、2MiB。
v3佇列於10月16日22:10結束，兩組均成功完成4輪完整工程fold，returncode0、
canonical lifecycle通過；佇列因兩組皆未通過效能門檻而以exit2結束，
不是持續訓練中，也沒有新的正式收斂績效。

| case | 完成輪數 | 最小顯存餘裕 | 目前唯一拒絕原因 |
| --- | ---: | ---: | --- |
| `initialization__historical` | 4 | 2.788GiB | 第3輪新增3個Dynamo圖 |
| `initialization__historical_refine` | 4 | 2.712GiB | 第4輪新增3個Dynamo圖 |

不可變cache上限由16降至12GiB後，兩組顯存門檻均已通過；剩餘未解是熱身後
仍發生新編譯，尚未定位觸發來源，不能將其說成已確認的金融／梯度機制錯誤。
工程最佳checkpoint仍為初始化epoch0；四輪工程不能取代正式100次無改善收斂比較。
因此本輪尚無超越指定舊模型的新正式結果。

本機collector已於10月16日22:11成功回傳，165檔SHA核對通過，4份checkpoint及
2份model均可CPU載入且有限值，全部屬工程產物。實際副本位於
`OP/historical-refinement-v3-retained/artifacts/benchmarks/training_quality_20261015/historical-refinement-v3/`。
完成收據為 `OP/local-historical-refinement-v3-preservation.json`；
進度檔 `OP/historical-refinement-v3-collection-status.json` 已為 `preserved`。
正式root目前只有設定與佇列稽核紀錄，沒有新的正式模型。

## 2026-10-16 22:00 台北：兩組未完成微調排入既有遠端佇列

依使用者要求，沿用 `run_frontier_fold_queue.py` 完整工程fold、
`run_ablation_experiments.py` 正式順序佇列，以及 `manage_gpu_jobs.py` 的節點GPU租約；
由既有 `stockagent-agent` tmux監督，不另建訓練executor。
舊shell入口仍綁定其35組歷史catalog，因此本次直接使用相同的共用佇列owner，
明確只選兩組，沒有誤啟動35組舊消融。

新portable設定為
[`tw_old_fold11_refinement_20261016_v3.yaml`](../configs/ablations/tw_old_fold11_refinement_20261016_v3.yaml)。
依序為 `initialization__historical`、`initialization__historical_refine`；先各做4輪
完整工程fold，通過者才依序正式訓練。正式上限1000輪、100輪validation無改善停止，
不是以4輪評斷策略品質。失敗保留並跳過，不降低既有顯存或圖穩定性驗收門檻。
source25、指定舊權重/RMS、純log、d32/lookback32、global128/eval16、fold10及
calendar帳務均保留。針對前次顯存餘裕不足，兩項不可變資料cache由16降至12GiB；
實際config逐欄差異只有cache容量與輸出路徑，runtime另記source-cache容量。
已完成的v2 baseline選49／停149保留為control，不重訓或覆寫。

Vastai1T完整位置：

- 正式產物：`/root/stockAgent/artifacts/markets/tw_old_fold11_refinement_20261016_v3/`。
- 工程產物：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/historical-refinement-v3/`。
- 目前階段：`OP/historical-refinement-v3-status.json`。
- 正式log：`OP/historical-refinement-v3-formal.log`，正式階段開始才建立。
- 最佳模型預定位置：正式root的 `<case>/fold_10/checkpoint_best.pt`。
- 逐輪進度：各case的 `train_2015-2016-2017-2018-2019-2020-2021-2022-2023-2024/epoch_curve.jsonl`。

排程run為 `historical-v3-remote-queue-20261016T135951-dea895b0`；
完整log為 `artifacts/operations/agent-workflow/runs/<run-id>/run.log`。
預檢run `historical-v3-queue-prepare-20261016T135924-cdb357c5` 已成功，
兩組有效config及strict雙GPU CUDA均通過；目前第一組在資料／工程準備階段，
尚無新正式收斂結果。

本機collector為 `historical-v3-remote-collector-20261016T140014-d90da554`，
每30秒讀取run、階段與最新epoch，並於遠端封存完成後自動回傳、核對SHA及CPU載入。
本機目前進度檔：`OP/historical-refinement-v3-collection-status.json`；
完成後副本：`OP/historical-refinement-v3-retained/`。
全部新目的地和是否已存在的界線記於
[v3排程與產物位置](../artifacts/operations/training-quality-audit-20261015/historical-refinement-v3-locations.json)，
不能將預定checkpoint路徑算成已產出的模型。

## 2026-10-16 20:33 台北：產物位置與實際進度

本次使用者要求：每次啟動訓練都回報主機、完整產物根目錄、checkpoint與進度位置。
以下遠端路徑均位於 Vastai1T 的 `/root/stockAgent/`；本機副本位於 Penguin。
已盤點18個歷史market根目錄、36個case/fold路徑、130個checkpoint路徑、
101個quality工程目錄，詳列於 `OP/remote/training-artifact-locations-20261016-remote.json`。
這是本次讀取的目錄清單，不把工程、未收斂、取消或失敗產物算成正式完成。
遠端實體路徑、本機副本、checkpoint、逐輪曲線與狀態整合於
[產物總索引](../artifacts/operations/training-quality-audit-20261015/training-artifact-locations-current.json)。

| 實驗 | Vastai1T相對路徑 | 進度 |
| --- | --- | --- |
| source24九組 | `artifacts/markets/tw_multiobjective_convergence_last_fold_20261016_v24/` | 九組皆完成，patience100 |
| calendar修復v2 | `artifacts/markets/tw_old_fold11_refinement_20261016_v2/` | baseline選49／停149；另外兩組未進正式訓練 |
| 微調v2工程 | `artifacts/benchmarks/training_quality_20261015/historical-refinement-v2/` | 三組皆完成4輪；只有baseline通過工程門檻 |
| 微調v1失敗 | `artifacts/benchmarks/training_quality_20261015/historical-refinement-v1/` | 2014缺歷史背景，在optimizer前拒絕 |
| 較早純log／Muon | `artifacts/markets/tw_pure_log_convergence_last_fold_20261015_v1/` | 兩組完成，選236／停336與選282／停382 |
| 較早temporal FP32 | `artifacts/markets/tw_pure_log_mechanisms_convergence_20261015_v2/precision__temporal_fp32/` | 選107／停207；同root另有已取消seed研究，不能混算 |
| 較早v12消融 | `artifacts/markets/tw_scale_cash_annual_last_fold_fused_20261015_v12/` | 九個完成case；另有未完成lookback案例保留 |

九組與較早研究的最佳模型在各case的 `fold_11/checkpoint_best.pt`；修復calendar
baseline在 `baseline/fold_10/checkpoint_best.pt`。各fold同目錄含 `model.pt`、
`metrics.json`、`test_backtest.npz`、`equity_curve.png`、`annual_performance.png`；
resume檔與逐輪曲線在case的 `train_*/checkpoint_last.pt`、`train_*/epoch_curve.jsonl`。
case通常是canonical `_capacity/<case>/attempt-00/training` 的入口別名；離機封存
保存實體檔案，不將遠端絕對symlink原樣套到本機。

本機可直接使用：

- 九組：`OP/single-metric-convergence-v24-comparison-inputs/<case>/fold_11/`，
  指向 `OP/single-metric-convergence-v24-retained/artifacts/markets/tw_multiobjective_convergence_last_fold_20261016_v24/_capacity/<case>/attempt-00/training/`。
- 新baseline：`OP/historical-refinement-v2-comparison-inputs/baseline/fold_10/`，
  指向 `OP/historical-refinement-v2-retained/artifacts/markets/tw_old_fold11_refinement_20261016_v2/_capacity/baseline/attempt-00/training/`。
- 本輪工程副本：`OP/historical-refinement-v2-retained/artifacts/benchmarks/training_quality_20261015/historical-refinement-v2/`。
- 較早純log／Muon：`OP/convergence-retained/artifacts/markets/tw_pure_log_convergence_last_fold_20261015_v1/`。
- 較早FP32：`OP/quality-v2-fp32-readback-retained/artifacts/markets/tw_pure_log_mechanisms_convergence_20261015_v2/`。

20:33讀取時兩GPU皆0%且沒有CUDA compute或trainer程序，**目前沒有GPU訓練中**。
新baseline的formal trainer已成功完成；控制程序卻仍用hard-coded
`fold_11/metrics.json`，在應讀 `fold_10` 時失敗，所以collector未自動回傳。
本次用實際target fold核對canonical lifecycle、best/last checkpoint與patience100，
完成362檔離機SHA核對，8份checkpoint與4份模型均CPU可載入且有限值；其中包含
三組工程產物，不能當成四組正式結果。恢復收據為
`OP/local-historical-refinement-v2-preservation.json`，optimizer新增更新0。
新baseline完整181日報酬59.42%、Sharpe2.539、Sortino4.338、日MDD8.30%；同期
2330 buy hold62.73%，不能直接與149日的舊69.85%相比。
另已完成相同起始資金1,000萬元、共同149日、固定已存意圖的canonical重播：
報酬48.95%、Sharpe2.536、Sortino4.293、日MDD8.30%；相對指定舊模型，
回撤較小，但報酬及Sharpe／Sortino仍較低，不能宣稱全面超越。
收據為 `OP/historical-refinement-v2-baseline-common-account.json`，
恢復後的目前狀態為 `OP/historical-refinement-v2-recovery-status.json`；
原先失敗的controller／collector收據仍保留。

兩組舊初始化case雖完成工程fold，但第4輪新增3個Dynamo圖，最小顯存餘裕僅
0.53／0.57GiB、peak fraction0.983／0.982，未達既定1.5GiB／0.96門檻，故未啟動
其正式訓練。這是資源與圖穩定性未驗收，不是訓練中或正式策略失敗。
剩餘工作是處理兩組cache／圖編譯，再做工程與正式收斂；目前不能宣稱完成超越舊模型。

## 2026-10-16 18:08 台北：九組正式收斂結果與微調工程失敗

直接讀取遠端 run／metrics 與本機已完成 collector：source24 九組正式訓練全部
成功結束，皆在100次validation無改善後停止。相同149日為2026-02-26～10-02，
策略費後、起始資金1,000萬元；MDD以下採日收盤跌幅正值。每組 checkpoint 依各自
宣告的validation目標選擇，沒有依test挑epoch，也沒有將四個loss平均。

| 方法 | 選定／停止epoch | 報酬 | Sharpe | Sortino | 日MDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| 指定舊模型固定意圖、相同起始帳戶重播 | 128／228 | 69.85% | 3.485 | 5.995 | 9.50% |
| 2330 buy and hold | — | 25.18% | 1.097 | 1.755 | 12.37% |
| 本輪純log baseline | 57／157 | 16.96% | 0.944 | 1.454 | 14.86% |
| 獨立Return loss | 279／379 | 73.76% | 1.906 | 2.920 | 20.67% |
| 獨立Sharpe loss | 124／224 | 2.66% | 0.690 | 1.070 | 3.53% |
| 獨立Sortino loss | 255／355 | −0.12% | −0.031 | −0.039 | 5.06% |
| 獨立MDD loss | 64／164 | −0.48% | −0.901 | −0.952 | 0.73% |
| Return-space PCGrad | 79／179 | −0.13% | −0.048 | −0.065 | 2.64% |
| Return-space MGDA | 42／142 | −0.95% | −0.953 | −1.374 | 1.92% |
| Parameter-space PCGrad | 228／328 | 8.31% | 1.088 | 1.508 | 7.22% |
| Parameter-space MGDA | 20／120 | −1.91% | −1.129 | −1.381 | 3.81% |

Return loss報酬比舊策略高3.91個百分點，但Sharpe／Sortino較低，MDD多11.16個
百分點；沒有任何一組同時勝出四項指標。較低MDD本身不代表更好的獲利能力。
本輪log baseline16.96%與先前已收斂baseline51.84%屬不同訓練協議，不能混成
同一個baseline或以較好的一個替換本輪控制組。九組每日tape重算與保存metrics
誤差皆0，零帳戶default。666個檔案已離機核對SHA，18份checkpoint與9份獨立
模型皆可CPU載入且數值有限；這不是所有九組的獨立來源／實體帳務重建證明。

原排隊微調v1已在16:41台北因完整年度檢查失敗停止，三組沒有正式收斂結果，
不能繼續標記為等待中。三組同樣在任何optimizer更新前拒絕：面板自2014-01-06
開始，lookback32令首個可決策日為2014-02-27；2014年度聲稱擁有246日，實際僅214日。
檢查正確拒絕了缺少因果背景的完整年度，不應跳過檢查或補造價格。

修復版設定 `configs/ablations/tw_old_fold11_refinement_20261016_v2.yaml` 使用既有
基礎設定的2014背景／2015～2024完整calendar訓練，validation2025、test2026。
舊權重使用既有 `matching_validation_and_causal_train_superset` 政策，允許僅多
較早的2014訓練資訊，仍要求相同validation、相同特徵順序及精確state。
這不是相同訓練資料的resume：全新baseline只監督2015～2024，舊初始化另保留
其歷史2014學習資訊。新最後fold ID為10，來源仍是原fold11；兩者test均為
2026-01-02～10-02共181日，並另做相同149日／新帳戶比較。
程式來源與金融／梯度實作不變，保留d32／L32／global128／eval16、純log、
seed42、1000上限與patience100。實際calendar預檢通過十個完整年度，舊設定負控制
重現拒絕，兩個微調case均核對121個state逐值相等。

證據：`OP/single-metric-convergence-v24-comparison.json`、
`OP/single-metric-convergence-v24-collection-status.json`、
`OP/local-single-metric-convergence-v24-preservation.json`、
`OP/remote/historical-refinement-v1-failure-preservation.json`、
`OP/remote/historical-refinement-v2-calendar-readiness.json`。
修復版GPU工程與正式收斂另讀 `OP/historical-refinement-v2-collection-status.json`，
不能將calendar預檢與state載入當成正式績效。
18:13台北已啟動修復版控制程序
`historical-refinement-v2-exec-20261016T101305-2ff7a7bb`，遠端167項測試再次通過，
進入三組完整4輪工程fold；通過者才執行正式100次無改善收斂。
本機collector為 `historical-refinement-v2-collect-20261016T101321-64b6449e`。
v1失敗封存53檔已離機逐檔核對SHA，原失敗收據與未開始訓練的目錄保留。

## 2026-10-16：2330 buy and hold 與舊模型微調（v1歷史提案）

核對 dataset 的 session 對齊、原始 benchmark tape 與來源時鐘分解後，既有
2330 數列已是**調整後前收至今收、包含隔夜的 1x buy and hold total return**，
沒有每日開收盤往返。共同149日為2026-02-26～10-02：buy and hold 報酬25.18%、
Sharpe1.097、Sortino1.755、日MDD12.37%；官方開盤至收盤診斷則為−5.97%。
benchmark 為費稅前指數型報酬，股利／分割已包含一次於 adjusted close；策略仍為
實際帳務費稅後。不能因標示不清而把原數列替換成較弱的日內 comparator。

本次共用報告 helper 與 `tw_day_trade` mode contract 明列 benchmark 的
buy-and-hold 名稱、隔夜曝險、調整後收盤時鐘、費稅與無每日再平衡政策；
日頻與分鐘執行共用此定義。另從不可變 tape 重繪舊策略與近期已收斂 baseline
的圖表至 `OP/benchmark-buyhold-20261016-v1/`，原始指標沒有改變，optimizer
更新次數0。這是報告契約修正，不是策略改善，也不修改既有凍結實驗來源。

品質研究維持單一 `loss=-252*mean(net_daily_log_return)`，以先前觀察到的
年界／RMS／配置差異形成三組明確比較：

| 組別 | 初始化 | LR | 共同設定 |
| --- | --- | ---: | --- |
| baseline | seed42全新權重 | 1e-4 | 舊calendar train2014～2024／val2025／test2026 |
| initialization__historical | 指定舊fold11 epoch128權重及RMS | 1e-4 | d32／lookback32／global128／eval16／純log |
| initialization__historical_refine | 相同舊權重及RMS | 1e-5 | 最多1000輪／100次validation無改善停止 |

三組保留年度訓練帳戶重置，因此不是舊連續帳戶的 exact optimizer resume。
微調不匯入 optimizer，全部1,133,582個參數可訓練。第0輪先以目標 canonical
帳務評估並保存為候選，後續只有 validation 改善才能替換；測試不選 epoch。
不加入隱藏風險loss、top-K、強制全投資、benchmark混合或seed搜索。

實作時發現既有載入器不接受沒有 bottleneck 的 FinancialTransformer；已延伸
原共用 transfer helper，僅在特徵名稱與順序完全一致時直接載入完整 state，
保留持久化RMS，不創建巨大恆等投影。舊checkpoint全部**121個state tensor
逐值完全相同**地轉入，無不相容tensor；微型測試亦核對輸出及梯度完全相同，
改名／換序／重複特徵在載入前拒絕。本機與遠端凍結來源各**167項測試通過**。

新凍結來源SHA `dd4bc12cf6521c89a9c2b57db428355cff1d4c972892697be8a9a9fb1ff97594`，
相對source24僅改 transfer 與報告函式及mode registry。原計時修復與金融／VJP
實作保留。舊模型所需5個原始檔已按SHA恢復至遠端指定路徑，沒有換成別的模型。
正式產物根目錄為
`artifacts/markets/tw_old_fold11_refinement_20261016_v1/`，控制程序
`historical-refinement-v1-exec2-20261016T070724-c8351d6c`，接在現有九組完成後執行；
先做三組完整4輪工程fold，通過者才進入正式收斂，失敗組保留證據並跳過。
本機 `historical-refinement-v1-collect-20261016T070804-7c76b327` 自動保存、CPU驗收，
分別重算同181日完整fold與共同149日新起始帳戶，對照精確舊artifact和2330 buy hold。
**15:08台北時間讀取時尚未完成新研究的GPU工程／收斂驗收，也尚未證明超越舊模型或純GPU計算瓶頸。**

證據：`OP/benchmark-buyhold-20261016-v1/acceptance.json`、
`OP/historical-refinement-v1-exact-checkpoint-transfer.json`、
`OP/remote/historical-refinement-v1-execution-plan.json`、
`OP/remote/historical-refinement-v1-old-source-readback.json`、
`OP/historical-refinement-v1-collection-status.json`。
原proposal保留；bound plan 顯式將相同初始化來源的相對路徑綁為絕對路徑，
避免凍結程式的cwd改變檔案解析，並未改變訓練方法或checkpoint內容。
排程檢查另發現工程trial已有GPU鎖，外層再取相同鎖會自我拒絕。
已在任何GPU訓練前替換等待中的控制程序：工程trial各自持有唯一鎖，正式
ablation整輪持有唯一鎖。獨立lock目錄的負控制確實重現巢狀鎖exit1，三次
單一持有者控制皆exit0；execution plan明列此次純排程修正及原plan的SHA。
原九組GPU程序未受停止，舊控制程序與collector的失敗收據均保留。

## 2026-10-16：指定歷史 fold11 與驗證計時修復

使用者指定的
`artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_scale_separated_cash_v1/training-bf16/fold_11/`
就是本報告先前的舊模型。原始 manifest、checkpoint、test tape 及 metrics 與
`OP/historical-selected-artifacts/` 保存副本的 SHA 逐一相同；選定 epoch128、停在228。
逐列核對舊228輪與近期336輪的曲線，兩者最終`no_improve=100`，每輪均一次optimizer
更新；共同前228輪的LR逐值相同，最大差異0。這排除「只比較到64輪」及不同共同
LR時程的解釋，不代表兩者資料、年度重置與反傳幾何相同。
證據：`OP/old-vs-converged-update-schedule-audit.json`。

| 比較範圍 | 交易日數 | 費後報酬 | Sharpe | Sortino | 日 MDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| 指定舊 fold11 原完整期，2026-01-02～10-02 | 181 | 95.93% | 3.662 | 6.353 | 9.57% |
| 舊帳戶保留、只截共同日期，2026-02-26～10-02 | 149 | 69.10% | 3.429 | 5.873 | 9.57% |
| 舊意圖、共同首日重設相同帳戶 | 149 | 69.85% | 3.485 | 5.995 | 9.50% |
| 近期完整收斂純 log baseline，選236／停336 | 149 | 51.84% | 1.449 | 2.270 | 20.73% |

原完整期多32個交易日，不能將95.93%與51.84%視為同區間 A/B；第三、四列才控制
共同日期與1,000萬起始資金。固定舊意圖重播沒有重新執行模型推論，也沒有重訓。
逐列核對已完成的12組正式產物，測試日期也都是這149日；其中最高報酬為
Muon＋AdEMAMix的62.70%，最高Sharpe／Sortino為原Muon lr2的1.580／2.402，均低於
舊意圖同帳戶重播。12組中只有虧損2.41%的attention pooling日MDD7.12%比舊9.50%小。
這是同測試區間的描述比較，各輪訓練協議不同，不能當成某項訓練改動的因果效果。
直接重算的每日 log 報酬標準差為舊1.6196%、近期3.0705%；近期最大回撤區間
2026-04-30收盤至06-09收盤，舊策略同期報酬為−8.2677%，近期為−20.7281%。
較集中、較高曝險與較大波動是觀察；年度帳戶／年界／RMS／反傳與精度的各自因果
效果仍需受控重現。證據：`OP/requested-historical-fold11-comparison-20261016.json`、
`OP/old-vs-converged-baseline-mechanisms-20261016.json`。

本次另以近期51.84%的已收斂baseline補做曝險交叉重播；先鎖定
`OP/converged-exposure-crossover-plan.json`，保留各自同日的帶符號相對配置，只交換
原始請求gross，兩條對角線沿用已驗收重播。兩個新反事實各完成149日，來源、權限、
費率、整張、相同起始帳戶及指標都經canonical核對：

| 相對配置／每日gross來源 | 費後報酬 | Sharpe | Sortino | 日MDD |
| --- | ---: | ---: | ---: | ---: |
| 舊／舊 | 69.85% | 3.485 | 5.995 | 9.50% |
| 近期／近期 | 51.84% | 1.449 | 2.270 | 20.73% |
| 近期／舊 | 35.12% | 1.511 | 2.409 | 15.04% |
| 舊／近期 | 106.93% | 3.278 | 5.488 | 13.78% |

在這段固定意圖、兩種同日預算的對照裡，舊相對配置均優於近期配置；降低近期曝險
雖減少回撤，仍未補回配置差距。較高曝險不是全部解釋，仍應檢查配置的學習機制。
這是執行反事實，沒有重訓、沒有將106.93%當作新模型績效，也未依結果套入實際策略。
此表與後文針對−21.07%原baseline的較早交叉試驗是不同對照。
證據：`OP/converged-exposure-crossover-comparison.json`、
`OP/matched-account-converged-baseline_choice_old_budget.json`、
`OP/matched-account-converged-old_choice_baseline_budget.json`。

先前09:01的source23重試其後九組均失敗，未產生正式收斂結果。根因是
`_broadcast_epoch_eval_timing` 對 `TimingBreakdown.multiobjective_diagnostics` 字典
呼叫 `float()`；rank0先退出，rank1才在等待廣播時發生NCCL逾時。原失敗根目錄
`multiobjective-v23-complete-attempt2/` 與source23 capsule保留。

修正依 dataclass scalar defaults 決定數值欄位及還原型別；字典走結構化傳輸，
空字典維持單次 collective，CUDA event handles留在原程序。比對已建成的兩份
source bundle，source23→24只有此函式改變；模型、loss、金融帳務、VJP及設定模板
未改。新source24 SHA為
`61cd03892c02b002e0907c3a4fbf0bb9d7689cc789ea342ff212a75c8cdc36ca`，
runtime SHA仍為`850a27d155881eba53360c818f44a00519aeedf3fa0ab204c66da0bce295561f`。

本機46項focused／lifecycle／patience100測試通過，另1項雙CUDA測試交由遠端執行。
遠端已完成45項測試、零略過，包含實際雙CUDA rank的空／非空診斷字典傳輸及兩種
來源rank，另通過連續五代CUDA Graph完整參數梯度檢查；日誌及收據已離機核對SHA。
source24九組四epoch完整fold於2026-10-16 12:52台北全部通過，零拒絕；各組含驗證、
測試回測、曲線、繪圖與checkpoint。每輪一次optimizer更新，四種多目標的反傳次數
分別為報酬空間一次、參數空間四次；穩態第3、4輪沒有新增圖編譯。

| source24工程案例 | 第4輪MAX-rank秒 | 完整四epoch fold秒 |
| --- | ---: | ---: |
| baseline | 2.571 | 215.628 |
| return-space PCGrad | 3.113 | 228.580 |
| return-space MGDA | 2.996 | 217.710 |
| parameter-space PCGrad | 9.050 | 254.165 |
| parameter-space MGDA | 8.955 | 240.948 |
| Sharpe | 3.493 | 231.791 |
| return | 2.383 | 213.221 |
| Sortino | 3.319 | 224.099 |
| MDD | 2.995 | 226.080 |

這些是工程成本，不是正式收斂速度或策略品質。九組canonical dry-run全部通過；
3,121,608,474-byte工程封存已離機核對9,144檔，186份checkpoint與93份獨立模型均可
CPU載入且有限值。原失敗記錄與來源保留，封存SHA為
`e2d446acce12b9529dadd58c3bb27b0aee2c09de8aba4f9753f92e1e892db475`。

正式九組已於2026-10-16 13:00台北啟動，run為
`single-metric-convergence-v24-20261016T050026-9bbe325c`，遠端產物根目錄
`artifacts/markets/tw_multiobjective_convergence_last_fold_20261016_v24/`。
實際baseline前四輪已完成，manifest核對source24與原runtime，d32／lookback32、
global128／eval16、seed42、上限1000與ratio0.1均符合協議；每輪一次optimizer更新，
第2～4輪MAX-rank完整epoch為2.422／2.465／2.336秒，loss有限且沒有零梯度批次。
四輪初期曲線仍不是正式收斂績效；九組繼續按100次validation無改善停止。

交接末端另發現本機collector自訂regex只接受小寫，拒絕canonical run ID時間中的
`T`。已改用凍結來源內的共用`agent_workflow.identifier`，實際check-only通過後，
以`single-metric-v24-collector-20261016T050123-21783428`啟動收集器；沒有重啟正式
trainer。原失敗handoff的run/log及修補前collector均保留，當前handoff狀態另記恢復。
正式收集器正在執行，待完成後離機核對與彙整；目前沒有新九組的收斂結果，亦未
建立純GPU計算瓶頸。
證據：`OP/multiobjective-v24-timing-repair-provenance.json`、
`OP/multiobjective-v24-local-timing-admission.json`、
`OP/multiobjective-v24-remote-oracles-readback.json`、
`OP/local-independent-loss-stage3-preservation.json`、
`OP/remote/formal-first-epochs-v24-readback.json`、
`OP/multiobjective-v24-timing-repair-acceptance.json`。

## 2026-10-16 09:01 台北：所有實驗績效彙整

已完成的正式策略產物共有十二組：原始 v12 消融九組，以及後續純 log-utility 長訓練三組。兩輪訓練協議不同，分表比較。下表共同期間為2026-02-26至2026-10-02、149個交易日；報酬是費後累積報酬，MDD為日收盤跌幅正值。benchmark是2330，報酬25.18%、Sharpe1.097、Sortino1.755、MDD12.37%。這些是已反覆觀察的回顧研究。

| 近期純 log-utility 長訓練 | validation選定／停止epoch | 費後報酬 | Sharpe | Sortino | 日MDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline AdamW | 236／336 | 51.84% | 1.449 | 2.270 | 20.73% |
| Muon lr2 | 282／382 | 19.51% | 0.667 | 0.980 | 22.70% |
| 時間層FP32 | 107／207 | 34.87% | 1.343 | 2.043 | 18.91% |

三組都已按100次validation無改善停止，且獨立CPU canonical帳務重播通過。近期baseline在這三組中報酬、Sharpe及Sortino最好，但MDD仍較benchmark大。FP32減少回撤，同時降低報酬；Muon lr2未重現短測優勢。

| 較早原始v12消融 | validation選定／停止epoch | 費後報酬 | Sharpe | Sortino | 日MDD |
| --- | ---: | ---: | ---: | ---: | ---: |
| 原baseline | 209／309 | -21.07% | -0.883 | -1.195 | 39.23% |
| AdEMAMix | 44／144 | 21.64% | 1.083 | 1.678 | 16.41% |
| Dion3 | 365／465 | 26.25% | 0.771 | 1.119 | 30.15% |
| Muon | 98／198 | 23.03% | 1.040 | 1.511 | 15.21% |
| Muon＋AdEMAMix | 260／360 | 62.70% | 1.462 | 2.201 | 21.86% |
| Muon lr2 | 100／200 | 37.57% | 1.580 | 2.402 | 11.83% |
| NorMuon | 98／198 | 24.16% | 1.076 | 1.647 | 12.40% |
| SOAP | 187／287 | 11.64% | 0.444 | 0.651 | 21.80% |
| attention pooling | 60／160 | -2.41% | -0.530 | -0.763 | 7.12% |

九組保存指標已由每日報酬重算。原Muon lr2在該輪四項均優於同期間benchmark；Muon＋AdEMAMix報酬最高，回撤亦較大。這與近期Muon lr2結果不同，不能將不同訓練協議或更新預算混成單一optimizer結論。舊2026-10-06策略固定意圖、共同起始帳戶的反事實重播為報酬69.85%、Sharpe3.485、Sortino5.995、日MDD9.50%；那不是本輪重新訓練或完整PIT驗證。

最新獨立loss五組已在source22完成四epoch工程驗收，**尚未達正式收斂**：

| 四epoch工程案例 | 費後報酬 | Sharpe | Sortino | 日MDD | 穩態完整epoch秒 | 完整四epoch fold秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| log-utility | -1.59% | -0.837 | -1.106 | 1.98% | 2.590 | 226.824 |
| Sharpe | -2.12% | -0.708 | -0.953 | 3.50% | 3.263 | 221.976 |
| return | -1.66% | -0.749 | -1.009 | 2.33% | 2.416 | 212.483 |
| Sortino | -0.01% | -0.003 | -0.005 | 2.46% | 3.319 | 232.168 |
| MDD | -2.11% | -1.994 | -2.508 | 2.84% | 3.102 | 216.298 |

四epoch尚處學習初期，只用來核對完整生命週期及成本，不能以這張表淘汰任何loss。此前64-update初篩及fold10的392日對照保留在後段；fold10不能與上述149日直接排名。seed73／等權集成已取消；原lookback實驗未完成，calendar＋annual遭年界守則拒絕，continuous與部分96-update實驗遭容量門檻拒絕，均不列為正式策略成功。

PCGrad／MGDA已於遠端通過全部27項實際測試（CUDA略過0項）與連續五代完整參數CUDA Graph oracle。首輪九組完整fold全在訓練前因生成設定旁缺少`runtime-launch.json`退出，並無新策略數值。已保留原失敗根目錄，修正操作層：將既有canonical launch metadata原樣綁定到生成設定，核對acceptance SHA；source23及runtime不變，未改金融或optimizer規則。重試使用獨立`multiobjective-v23-complete-attempt2/`根目錄，baseline已進入真實兩卡trainer的panel載入。四組PCGrad／MGDA及四個獨立loss隨後依序執行。正式九組尚未啟動，不把等候程序當成訓練。

新工程run為`multiobjective-v23-engineering-l-20261016T010008-3d0148e8`，自動交接為`multiobjective-v23-automatic-han-20261016T010026-e88d85fb`。工程通過後仍須canonical dry-run與來源／checkpoint離機保全，再執行最多1000輪、patience100。版本22已閉合的兩卡profile顯示有效kernel時間比例約64.14%／57.34%，仍有同步與空隙，未達純GPU計算瓶頸。同源四臂重複比較的穩態epoch為3.87073→3.18018秒（1.21714倍），完整四epoch fold為233.19762→220.96920秒（1.05534倍）；不能當作新PCGrad／MGDA的加速倍數。

完整數值、不同協議、短測、未完成項目及來源SHA收錄於[實驗清單CSV](../artifacts/operations/training-quality-audit-20261015/experiment-performance-inventory-20261016.csv)與[JSON](../artifacts/operations/training-quality-audit-20261015/experiment-performance-inventory-20261016.json)。遠端讀回為`OP/remote/experiment-performance-readback-20261016.json`，SHA `d35f23f6c81af04222fd3fa4751a165d9b9c066acb5b3d718b06cfabc485a480`。本節更新取代後文較早的「待遠端27項驗證／source22尚未完成」狀態；歷史紀錄保留。

## PCGrad／MGDA：九組比較與版本23

[新矩陣](../configs/ablations/tw_multiobjective_convergence_last_fold_20261016_v1.yaml)繼承原獨立 loss 矩陣，順序為 baseline、報酬空間 PCGrad／MGDA、參數空間 PCGrad／MGDA，再接原四組獨立 loss。固定 seed42、d32／lookback32、global128／local64／eval16、BF16 AMP 與 FP32 參數、FP64 財務歸約、原費稅／整張／mask／年度 reset／輸出方式。每輪只更新一次 optimizer。新四組以 **validation 純 log utility** 選 checkpoint，四種訓練指標分開記錄；原四組仍各自按自己的 validation loss 選 checkpoint。沒有用 test 選模型。

新四組的任務是 `−Sharpe, −252×mean(log return), −Sortino, +MDD`，保留原單位，不做任務梯度正規化；第二項是 **log return**，與獨立 `trajectory_return` 的年度複利報酬不同。各自重置的年度帳戶仍以有效日期數加權，不能把逐批統計平均充當年度指標。

實作位於 [multiobjective.py](../stockagent/training/multiobjective.py)。PCGrad 依原演算法，將每份梯度依 seed 與 epoch 決定的隨機順序，投影到未修改的其他任務梯度，再相加；不干擾模型 RNG。MGDA 枚舉四任務 simplex 的15個非空面，求最小範數凸組合並驗證全域 KKT，沒有加入 ridge 或隱藏 return 項。兩者只把4×4 Gram 矩陣送到 CPU 解係數，再做一次大型梯度線性組合。[PCGrad 原論文](https://proceedings.neurips.cc/paper/2020/hash/3fe78a8acf5fda99de95303940a2420c-Abstract.html)、[MGDA 原論文](https://comptes-rendus.academie-sciences.fr/mathematique/articles/10.1016/j.crma.2012.03.014/)。

報酬空間版本先做一次不更新的完整帳務記錄，再將四個 `dL/d(log return)` 合成，經一次原帳務／模型 VJP 更新；參數空間版本記錄後做四次 VJP，先按原年度帳戶 DDP 契約全域歸約，保存未裁剪參數梯度，合成後才做唯一一次裁剪／AdamW／scheduler 更新。工程評分實際計入2或5次模型遍歷；每次重播在更新前都檢查每日報酬及配置。帳務驗證未通過時，不能執行梯度合成或更新。

**解讀邊界：** MGDA 的共同下降幾何只適用於其求解空間的精確梯度；報酬空間不能替代參數空間。此專案的參數梯度是已宣告的整張 surrogate／分塊 BPTT 梯度，且實際 AdamW 另含動量及 weight decay，因此不能宣稱離散交易或實際更新會四項同升。參數版本另記錄四項梯度與實際 optimizer 位移的內積。純 MDD 可能是零梯度，MGDA 因此可得到零方向；保留該結果，不能刪除 MDD 任務或暗加報酬來讓它動。

在原本已觀察過的 baseline 與 Muon checkpoint 測試報酬路徑上，先做固定路徑的報酬梯度診斷：兩條路徑的六對梯度均無負內積，PCGrad 的四個係數都是1；baseline 的 Sharpe／log-return cosine 約0.99586，Sharpe／Sortino 約0.99817。兩條路徑的 raw-unit MGDA 都選到 `alpha_MDD=1` 的面頂點，KKT 通過。這說明它們未必帶來新的有效方向：PCGrad 在這兩個點等同原梯度相加，MGDA 的最小範數解可能完全由小範數 MDD 梯度決定。此診斷沒有經過神經網路 Jacobian、沒有重新訓練，也不是新方法的績效結果；不據此刪除任何新實驗或改權重。證據含原NPZ及腳本SHA，為 `OP/multiobjective-return-geometry-v23.json`。

原因可以直接由可行方向理解：log utility 對每日 log return 的導數是相同的負常數，增加一段回撤內的報酬也會降低 MDD。把每日報酬當成可獨立修改的自由變數時，兩者並不直接衝突；實際上受共享模型、倉位與帳務限制，日報酬不能獨立增加。真正的取捨可能經過這個 Jacobian 才出現，因此參數空間比較有必要，不能只看便宜的報酬空間結果就宣布解決多目標問題。

2026-10-16工程進度：最終版本累計248項 CPU 檢查已通過（225項既有回歸加最終23項新測試，並非同一命令的單次綠燈）；本機兩項實際 CUDA 參數梯度／AdamW oracle 通過。新測試涵蓋原 PCGrad 向量公式對照、MGDA simplex／KKT、奇異／零梯度、原單位差異、空間差異反例、唯一 checkpoint 契約，以及兩rank整張帳務 VJP 與單機參考比較。新增 fault-injection 測試曾因在拒絕後才讀取已清除的 `.grad` 而失敗；已將通過時的梯度先保存再注入錯誤，重新通過，原失敗日誌保留。證據為 `OP/multiobjective-local-v23-admission.json` 與 `OP/multiobjective-local-v23-gpu-admission-attempt4.json`。

來源版本23已封存：17個程式 owner 及另行釘選的兩個 YAML 輸入，source SHA `6ffbfa8b7ea08ccb29e007e2b7da5d788a614c7c7a2b55b34ae382c06dc635bd`。runtime 沿用原雙5090實測環境。原版本22四臂交錯速度比較及閉合 profile 繼續；原待執行五組正式交接已在尚未訓練前停止，改由九組交接接續，原模型與證據保留。

遠端版本23首個 oracle 啟動碰到 canonical 專用 GPU 鎖而退出，空日誌不能視為 CUDA 測試結果。已改為等待原版本22工程結束、再取得兩卡專用鎖；重試使用獨立日誌與收據，未繞過 admission。測試重用既有已釘選的財務 fixture import closure，並使用 pytest append import mode 保持 spawn worker 的來源優先順序，避免誤載入無關dashboard；僅重啟尚未使用GPU的等候程序。遠端已成功收集27項新測試，並在真實PyTorch2.11／frozen fixture環境執行23項CPU測試通過、4項CUDA明確略過，耗時16.58秒。這是同23項測試在另一環境的驗證，不能再加到248個unique測試數。證據為 `OP/remote/multiobjective-remote-v23-collection.txt` 及 `OP/remote/multiobjective-remote-v23-cpu-fixture.txt`。待驗證包括全部27項（含實際 NCCL）及連續五代完整參數 CUDA Graph 反傳。通過後依序跑九個完整四epoch fold，比較完整成本及生命週期；工程拒絕的組別保留理由，後續組繼續。正式訓練仍須工程通過、canonical dry-run及來源／checkpoint離機保全。目前**尚無新四組的遠端完整fold或收斂績效，亦未建立純GPU計算瓶頸**。

版本22八個同來源完整fold交錯比較已完成，每個方案各兩次：control／deferred／recorded／combined 的穩態MAX-rank epoch均值分別為3.87073／3.37311／3.55509／3.18018秒；完整四epoch fold均值分別為233.19762／220.51199／228.57390／220.96920秒。selected combined 的穩態加速1.21714倍，完整fold加速1.05534倍；這是原獨立Sharpe路徑的引擎比較，不能當作新PCGrad／MGDA的耗時或策略收益。原五loss剩餘驗收與閉合profile仍繼續；九組會共用同一個通過驗收的引擎設定。離機screen與摘要為 `OP/remote/sparse-ledger-engine-screen-v22.json`、`OP/milestone-source22-balanced-engine-for-multiobjective-v23.json`。

新控制程序為 `multiobjective-v23-gpu-oracles-a-20261015T232731-dc83372e`、`multiobjective-v23-engineering-a-20261015T232731-b911ddfd`；本機自動交接為 `multiobjective-v23-automatic-han-20261015T232736-d004a124`。正式產物預定 `artifacts/markets/tw_multiobjective_convergence_last_fold_20261016_v23/`，在正式程序實際啟動前不能當作已有模型。工程結果預定 `artifacts/benchmarks/training_quality_20261015/multiobjective-v23-complete/`。

## 四個獨立 loss 與 GPU 工程驗收（版本22以前）

設定為 [單指標矩陣](../configs/ablations/tw_single_metric_convergence_last_fold_20261015_v1.yaml)：同 seed42 的純 log-utility control，接著 Sharpe、return、Sortino、MDD 四組獨立訓練。最小化 loss 分別是 `-Sharpe`、`-compound_return`、`-Sortino`、正的 MDD 跌幅；沒有四指標平均。Sharpe/Sortino 訓練分母使用固定 `1e-8` variance epsilon，使全現金附近梯度有限；報告指標仍用原 unsmoothed 定義。純 MDD 允許全現金零回撤、零梯度解，不能暗加 return 來掩蓋此性質。

實際容量 baseline 仍為 d32/lookback32、global128/local64、eval16；資料、整張、費稅、mask、年度 reset 與更新次數不變。輸出維持 `score_entmax_scale_separated_cash`，先隔離 loss 的影響；沒有宣稱已做輸出架構對照。全部正式組保持最多1000輪、100次 validation 無進步、每輪 validation/test/curve/plot/checkpoint，且只按各自 validation loss 選 checkpoint。正式收斂結果尚待新矩陣完成。

**2026-10-16最新工程狀態：** v21五組獨立loss的完整四epoch fold均通過，所有組別的帳務平行前後向回退計數為0；逐日return重播最大誤差約2.23e-16。這是工程驗收，尚非正式收斂或策略優劣。所選mode2帳務與同源mode0各兩次的穩態均值為3.69765對4.61179秒，約1.24722倍；冷啟動全程沒有相同比例收益。所選方案的完整profile已閉合，主卡有效kernel約52%、stream同步362次，尚未達純GPU計算。因已有下一版專用候選的交錯微測收益，v21尚未開始的正式交接已停止；v22整合及驗收中，正式五組仍待工程通過與離機保全後執行。

| 獨立目標 | 穩態MAX-rank epoch欄位秒 | 四epoch完整fold秒 |
| --- | ---: | ---: |
| log-utility control | 2.570 | 213.09 |
| Sharpe | 3.671 | 228.95 |
| compound return | 2.553 | 269.22 |
| Sortino | 3.745 | 219.49 |
| MDD跌幅 | 3.623 | 233.92 |

各列皆為獨立process，已使用既有來源列及編譯快取；`epoch_wall_s_max_rank`在寫入該輪curve之前取樣，包含當輪train／validation／test／checkpoint，並非最終繪圖與完整fold生命週期的總時間。四epoch全程另行包含暖入、曲線、繪圖、checkpoint及最終重播，不代表新租主機的資料／編譯冷啟動成本。證據為`OP/milestone-all-five-loss-folds-v21.json`及`OP/remote/adjoint-four-loss-engineering-admission-v21.json`。最新結論放在此處，下文保留各版本的機制、拒絕及量測過程。

v22的具體方案是顯式傳入第一遍記錄的daily log return，僅取代第二遍四次資金猜測；完整原財務證明及失敗回退保留。同程序十次交錯的2757檔／16日合成帳務，resident為16.492→12.240ms（1.347倍），含H2D為19.291→15.225ms（1.267倍）；kernel數821→426，報酬誤差約3.34e-16、action梯度最大誤差約1.16e-10、零回退。這尚不含NN或完整fold。另實作forward-owned驗證gate，讓反傳明確保留gate身分，predicate在producer stream先clone、跨stream用event於更新前共同驗收；可選擇將平行前向證明延後到完整pass，失敗仍用既有兩rank RNG／梯度／tape清除及canonical retry。兩個技術做獨立及合併的完整fold比較，並以新checkpoint契約拒絕不相容resume。

本機v22 CPU回歸283項通過、17項留待實卡；三項實際GPU檢查已全數通過，涵蓋不可信／省略候選、inactive tail、pending VJP、跨Python執行緒／CUDA stream的predicate快照及更新前拒絕。首輪CUDA失敗是測試使用未指定index的`torch.device('cuda')`呼叫`set_device`，其餘兩項當時已通過；只修正測試裝置編號後重跑，原失敗保留。16個source owner已封存，來源SHA為`a4e94897e506e1b5f7d7b51d203df8ad9d8c111afa4f2d8a05ac1fbba2a359b3`，runtime SHA不變。遠端雙5090驗收程序`independent-loss-recorded-v22-en-20261015T223823-177f6def`已實際啟動；四臂完整fold按正反順序各跑一次。獨立正式五組交接等候程序已啟動，須完整工程驗收及離機保全後才執行，尚非正式訓練已開始。計畫為`OP/recorded-candidate-v22-plan.json`；本機證明為`OP/recorded-candidate-local-v22-candidate.json`；微測為`OP/recorded-return-candidate-balanced-v21.json`；v21交接停止證明為`OP/v21-handoff-recorded-candidate-supersession-stop.json`。

v21工程程序最後因常數來源profile沒有產生`timeline.nsys-rep`而以失敗結束，不能將整個程序標成完成；先前五loss及selected/control閉合profile仍有效。根因是巢狀`runpy.run_path`暫時改寫`argv[0]`，使原observer的`train.py` guard跳過profiler start/stop。獨立版本v22 observer改以獨立namespace執行原observer並保持argv；CPU組合檢查重現舊版0次capture、修正版恰一次start/stop，且兩個wrapper均保留六次原函式呼叫。來源計數檔可讀，但沒有GPU時間線，不能用來計算時間占比；遠端仍須產生閉合真實profile。證據為`OP/constant-observer-v22-composition-admission.json`及`OP/remote/v22-predecessor-boundary.json`。

封存v22的canonical單指標模板dry-run已通過，生成五個不同loss的config，逐一讀回確認seed42、d32／lookback32、global128／eval16、epochs1000、ratio0.1、每epoch一次optimizer update及零額外風險loss。這是未選定引擎前的模板檢查；完整工程選出的Graph／runtime組合仍須由正式交接重新綁定及dry-run，不能把模板檢查當成訓練已啟動。證據為`OP/remote/single-metric-template-preview-v22-admission-attempt2.json`。首個預覽driver的結果讀回誤用`config.yaml`檔名，實際canonical dry-run已成功；改讀既有`generated_configs/*.yaml`後通過，原預覽與日誌保留，沒有修改模型或停止規則。

v22最終來源在遠端PyTorch2.11已通過299項pytest回歸，未略過CUDA；另三項完整NN／年度累積梯度oracle在production warning context獨立通過，從pytest明確deselect，不能重複加總。pytest耗時523.14秒。雙卡八項帳務／搬移探針均通過；11個原始證據檔已回本機逐一SHA核對，收據為`OP/local-source22-engineering-oracles-preservation.json`。硬體效能計數器依然權限不足，沒有roofline結論。

v22同來源control第一個完整四epoch fold已通過，完整流程234.217秒；epoch3／4的MAX-rank欄位為4.02961／3.93246秒，均值3.98103秒，兩輪新增編譯與驗證重試均為0。這只建立新版control，不是新優化的收益；其餘三臂及反向順序重複仍在執行。原始結果為`OP/remote/source22-control-1-result.json`，初次screen快照為`OP/remote/source22-screen-first-control.json`；正式五組100次停滯收斂的自動交接仍在等待完整工程驗收及離機保全，未開始正式收斂。任務保持active，不將微測、四輪fold或排隊狀態當成策略品質或compute-only完成。

已修正會讓兩次重播不一致的底層問題：同一 FP32 CSR、同一投影權重重複 cuSPARSE forward，最大差異約 `2.3e-5–3.05e-5`；整個模型可放大至約 `0.00104` 的配置差異，已被逐日報酬 guard 拒絕，沒有在失配後更新權重。新 Triton 固定列歸約保留完整特徵、FP32 權重及零欄位 affine 貢獻，微測 forward 約0.44–0.45ms、重播逐 bit 相同，原 forward 約0.55–0.56ms；backward 仍走 canonical 稀疏 VJP。微測不能代替完整 fold 加速證據；版本綁定 `full_schema_csr_fixed_row_fp32_forward_v3` 與 `annual_single_metric_return_adjoint_v3`，拒絕舊 optimizer resume。

v6 完整四輪 fold 工程檢查：Sharpe steady MAX-rank epoch 9.831秒、full fold 194.083秒；return 7.199秒、177.820秒，兩者 lifecycle 與重播檢查通過。v7 generic 合併傳輸的 Sharpe epoch 11.080秒，未改善；同源保留 encoder activations 為9.784秒、最低4.104GiB顯存餘裕，但 full fold 194.402秒並未比未保留版本193.014秒快。以上僅供工程選擇，不是策略品質比較。

v8 為固定輸入加入來源 identity/version、改寫拒絕及容量上限快取，並為 field-major 實體帳務來源增加一次證明的連續列傳輸。遠端101項 projection／return-adjoint／financial Graph／快取檢查通過，本機相關85項通過、4項CUDA略過。512MiB 首輪實測384次 miss、0次 hit，屬快取淘汰失敗，不能當作已消除重傳；提高容量後，336次 hit、48次首次載入，固定輸入1,148,959,436 bytes。完成同源 A/B/B/A 後，cache 開／關的 steady epoch 均值為9.572／9.438秒，因此快取單獨沒有取得加速接受。後續仍測它與消除同步的交互作用，不把單次9.225秒當作穩定收益。證據：`OP/adjoint-resident-screen-v8.json`。

完整 epoch 的 v5 Nsight 範圍已包含兩次 pass；兩卡有效 kernel 覆蓋約42.9%／36.3%，仍有大量同步、小額傳輸與 rank tail。這不是 SM occupancy，也未達「只剩 GPU 計算瓶頸」。v9 利用 Graph 已觀察成功的零持倉／零 claim 金額證明，在明確 detach 的 BPTT 邊界刪除惰性列，避免再次同步掃描及排序；公開完整帳務與 pending VJP 不改，張量改寫即失效回一般路徑。

v10 將逐批財務 predicate 留在 GPU，整個 pass 結束時才由兩 rank 共同驗收，且必須先通過才可更新。若任何一 rank 證明失敗，兩 rank 都清除梯度、還原各自 RNG、清空 rejected return tape，重跑原即時驗證路徑；沒有刪除驗證。跨 rank 重試的 CPU／NumPy／Python／CUDA RNG 與帳務 VJP 檢查已涵蓋。凍結來源 `f685691a53e45990d0b013c23bb0b64496dbca50462e619a0c9ef898659885f1` 在遠端雙 RTX5090 通過107項測試，本機回歸42項通過、1項雙卡測試略過。四種 cache／延後驗證搭配與平衡重複已完成：control 的 steady MAX-rank epoch 均值9.59295秒，組合9.03331秒，吞吐約1.062倍；完整四輪 fold 均值191.142秒對187.558秒，收益約1.9%。快取單獨沒有穩定加速證據。

v10 五種獨立目標均通過四輪完整 fold、lifecycle、容量與逐日重播（最大誤差0，retry0）：Sharpe9.003、return6.477、Sortino9.146、MDD8.797、純log6.402秒／steady epoch。return與純log只做一次前後向，其餘指標保留兩次固定參數重播。這些是工程速度，並非已收斂策略比較。證據：`OP/adjoint-async-screen-v10.json`、`OP/adjoint-four-loss-engineering-admission-v10.json`。

v10 的完整 train epoch Nsight 捕捉已閉合兩個 rank 與兩次 pass：有效 kernel 覆蓋51.85%／44.05%，串流同步329／287次，前向記錄階段僅38.09%／32.46%覆蓋。仍有大量 launch、格式轉換及 rank 等待，因此不能宣稱只剩GPU計算。此輪關閉CUDA event tracing，與v5舊量測設定不同，不將覆蓋差異全部歸因於程式改善。`OP/remote/coverage-v10.json`保留完整範圍、kernel及API分解。

後續v12專用FP64短軸掃描維持原自訂op及反向prefix，僅對scan長度≤32使用Triton，270分鐘長軸保留原native。原因是RTX5070Ti隔離微測中`[2757,2]`由47.307降至3.440微秒，但270長軸反而慢約40%；沒有把短軸微測外推成整輪13.75倍。36項短軸／帳務Graph檢查通過，另有完整resident模型同時保留兩次forward、encoder與head Graph後的全參數adjoint檢查通過。凍結runtime來源SHA為`b550deebd083f9798ba984fc30db7e180148455e1d7f25466c9024444aa2d035`，遠端組合驗收尚在執行；尚未選入正式收斂訓練。

遠端短軸微測已完成：RTX5090的`[2757,2]`從22.2415降至1.144微秒，約19.44倍；270長軸仍走native，並未把隔離速度套到整輪。遠端原始145項檢查通過，其中完整NN Graph adjoint在保留編譯器advisory的production warning context獨立執行，其餘144項由pytest驗證。較早pytest失敗是Inductor softmax advisory被warnings-as-errors升級，原錯誤日誌保留；沒有刪除梯度或實際Graph root斷言。證據為`OP/bounded-prefix-micro-v12.json`及`OP/bounded-prefix-remote-oracles-v12-production-context.txt`。

v14新增專用稀疏反向：固定列FP32 forward不變，反向直接將原始CSR交給cuSPARSE的transpose SpMM，避免PyTorch先把CSC轉回CSR及排序。已綁定實際PyTorch載入的cuSPARSE、裝置／執行緒handle、當前stream及Tensor生命週期，不重複儲存完整轉置來源。RTX5070Ti的12.58M nnz隔離反向測得native9.260ms、direct1.435–1.605ms，這只是局部微測。全projection／全參數／pending-forward／實際CUDA Graph測試27項通過；FP32反向加總次序容許約1e-6差異，forward仍須逐bit一致。實際遠端baseline的完整configuration與optimizer contract均證明native／alg0／alg6三者不同，拒絕跨排程resume。v13尚未用GPU訓練即因缺漏完整optimizer fingerprint而被v14取代；停止證明及原來源保留。遠端兩種algorithm的完整fold篩選已完成，數值與後續取代範圍見下段。

v12的四個NN Graph組合在完整年度梯度累積中均遭覆寫guard拒絕，不能把較小的pending-forward oracle當成年度訓練成功。已用d32、latent與market雙路、BF16、grad-enabled record pass及每批`cudagraph_mark_step_begin`重現：第一次compiled backward產生的leaf `.grad`屬Graph私人輸出，後續generation覆寫它。v15在canonical單卡與DDP backward邊界，將每個新產生的參數梯度移至caller-owned buffer，後續累積沿用同一buffer；未使用參數保持`grad=None`，不多做weight decay。每次更新後重新建立需要的梯度，與原帳務forward及整年單次optimizer step一致。

v15本機已通過head-only及encoder+head兩個真實Graph、兩輪record/replay、全參數梯度與buffer identity檢查，另28項projection／resume contract檢查通過。Graph組的configuration及完整optimizer契約改為`compiled_partition_resident_csr_owned_accumulated_leaf_gradients_abi_v11`；未啟用Graph仍維持v10。凍結來源SHA為`c23ccaf950377d5313bf6bd6cafb4c503099332aa65076035f0ca96517c8de8a`。v15遠端等待程序尚未使用GPU即被v16的整合比較取代，原來源及本機驗證保留；停止證明為`OP/prefix-v15-waiter-packed-transport-supersession-stop.json`。

v16將固定Graph輸入的許多小額copy改為pointer-table Triton kernel，同時處理1／2／4／8-byte原始位元、正stride／broadcast、逐欄dtype及最後一筆的inactive tail。快取只存GPU位址和幾何metadata，最多32表／256KiB，不持有來源Tensor、不快取其值；每次呼叫仍確認當前幾何與packed-row provenance並記錄stream。未支援的輸入沿用canonical copy。10項CUDA／來源生命週期／resume及四種financial pending-VJP組合通過；48欄60.7MB的實際維護函式局部量測，含幾何驗證／stream記錄，由1.173降至0.306ms（約3.83倍），不能用裸kernel約8.3倍代替維護路徑或整輪速度。

v12兩次同來源完整epoch測速的均值為native9.511、短軸專版9.343秒，僅約1.8%時間收益，且存在明顯波動。v12五個完整四輪fold已全部完成；這仍是工程驗證。v14直接CSR反向的151項遠端檢查及五次完整fold篩選通過，平衡重複均值為native9.022秒、alg6為8.587秒，約1.051倍；其餘目標與profile尚未完成即被新梯度修正取代。v16凍結來源`dfcbd984bf88ad29fbdf1fc5d68e165f088bcbf256f674561aa69123a9828a68`尚在等候時一併停止，沒有開始六組GPU比較。兩者原來源、已完成／中斷產物及停止證明保留；NN Graph、集中copy與稀疏搬移的組合改用下述修正後來源重新驗收。此時未達compute-only；不將局部倍數或已開啟Graph當作完成證明。

### 累積成交量的梯度與稀疏帳務Graph（v17／v18）

完整train-epoch的來源分解顯示兩卡各搬運約19.40／18.19GB的pinned H2D資料，另有317／275次僅合計2204／1996byte的pageable H2D傳輸。API時間與GPU工作可能重疊，不能直接加成epoch成本；來源為`OP/remote/host-boundaries-v12-selected.json`。只看微測或NVML不能宣稱純GPU計算。為追查小常數來源，另準備獨立Python-call NVTX observer；這種診斷會干擾CPU時間，不用於速度選擇。

稀疏來源直接使用既有canonical CSR事件executor與FP64端點積分，Graph仍執行原始帳務函式；新增固定事件形狀的saved／recompute／pending-VJP整合及來源staging快取，不改來源、費稅、整股或每日reset。遠端只讀檢查1606個session packet，最大189939事件，固定容量仍須262144。一般session中位數僅16058.5事件；把每個CPU packet都補到全期間最大容量會浪費H2D。因此另做緊縮packet傳輸，在消費端補相同的惰性事件尾端，保持Graph固定形狀。canonical來源與容量preflight仍須完整執行。

在加入每檔多次退出事件的2757檔微測時，前向帳務一致，但原dense反向與既有sparse反向出現約0.105的梯度差異，初次微測被拒絕。小型CPU重現證明這不是Graph、BF16或稀疏傳輸問題：原dense的`min(capacity, clamp(Q-capacity_before,0))`在Q正好等於累積容量時，會讓零容量分鐘的`min(0,0)`也貢獻0.5倍梯度，梯度因不存在的成交機會數量而放大。相同4日兩檔帳務，舊梯度約-0.094至-0.110，canonical sparse約-0.0043至-0.0055。這不是將原始價格或費用修正。

共享FIFO改用`C_t=min(Q,cumulative_capacity_t)`及`fill_t=C_t-C_(t-1)`。合法整數股數的前向成交不變，反向加總必須相消到最終成交量，不再受空格數量影響；完整minute-fill曲線仍可回傳。CPU重現的日報酬與turnover完全不變，修正後dense／sparse梯度逐值一致。新增空分鐘padding、正／負持倉、容量內／邊界／容量外的梯度不變性測試。`cumulative_closed_telescoping_volume_adjoint_v1`綁定configuration與完整optimizer checkpoint契約，Graph升至`canonical_terminal_flat_cumulative_fill_transport_v6`，不得續用舊optimizer。

修正前後診斷為`OP/sparse-liquidity-tie-cpu-v17-{diagnostic,corrected}.json`；正式交接停止證明為`OP/v16-handoff-volume-adjoint-supersession-stop.json`。CPU帳務／來源回歸154項通過、19項CUDA檢查略過；本機實際GPU Graph的saved／recompute、native／Triton copy與compact packet共8種pending-VJP組合通過。加入逐檔三次退出事件、2757檔、262144固定事件容量的完整帳務block，本機GPU確認報酬誤差0並通過全action梯度核對；緊縮傳輸使合成來源120513984降至6488128bytes，包含H2D的區塊由28.145降至13.123ms，約2.145倍。resident-only為9.722對8.777ms，僅1.108倍。這不包含NN及完整fold，不是訓練加速倍數。證據：`OP/sparse-financial-graph-local-v17-cumulative-compact-gpu0.json`。

v17來源封存為`b740360806c2aa9a1787c7b095c7ef9d89f09f2c4e985be22574573b0f685c9d`。遠端前兩次GPU測試先通過production NN adjoint／年度Graph累積檢查，之後因inventory測試依賴的fixture模組封裝與較新dashboard API不相容而停在collection，尚未開始完整fold。原測試與兩次失敗日誌保持不變。後續只擷取相同的六個fixture函式本體，保留原margin fixture與全部inventory斷言，以獨立manifest核對；不引入缺失的production API、不修改帳務或省略斷言。新的GPU／完整fold驗收改由v18完成；未證明策略品質改善，也不把舊baseline品質當成新梯度方案的結果。新的五組收斂比較仍須以修正後純log-utility作對照。

### 移除逐批GPU常數的主機等待（v18）

本機實際CUDA profiler確認：在已排入GPU工作後，四次`torch.as_tensor(Python_scalar, device='cuda')`都包含pageable H2D與stream synchronize；相同dtype／數值的`torch.full((), scalar, device=...)`四次均沒有memcpy或同步。此排隊診斷只證明等待來源，不是完整訓練加速。loss的年度化常數改在消費stream產生；第一個零梯度／死亡批次的diagnostic則用原位masked fill，不再建立GPU scalar。既有金融計算次序與選模規則不變。

以封存v17 loss對照FP16、BF16、FP32、FP64的log／Sharpe／Sortino／t-stat，以及完整physical FIFO log loss的252、365.25、1.1年度化值，20項loss／全action梯度與diagnostic檢查逐值相同；另84項訓練／兩rank CPU回歸通過、2項需CUDA或雙卡的檢查略過。證據為`OP/device-constant-local-v18-acceptance.json`及`OP/device-constant-adjoint-training-cpu-v18-regression.txt`。遠端v18已完成雙卡GPU oracle與八組完整fold及平衡重複；全部五個獨立loss與閉合兩rank的profile改由後續相同封存來源的驗收負責，才能交接正式patience100矩陣。尚未達純GPU計算瓶頸。

### 事件容量分桶與各形狀的Graph（v19）

把緊縮packet傳上GPU後又補至262144格，仍讓多數日期計算惰性尾端。v19直接讓Graph消費每批實際事件數的下一個2次方容量；完整來源的262144上限檢查仍保留，不刪除任何事件。Graph快取按Tensor幾何分開，最多8個形狀；待反傳的context仍持有其原owner及saved tape，不能被後來較大packet覆寫。此排程綁定configuration與optimizer契約的`power_of_two_packet_device_capacity_buckets_v1`及`eight_shape_banks_pending_tapes_keep_evicted_owners_v1`，拒絕跨排程optimizer resume。

本機12種實際GPU pending-VJP組合通過，涵蓋saved／recompute、native／Triton copy、固定／緊縮／分桶與尚未反傳時切換容量。另2項cache／checkpoint CPU檢查、43項來源／window／scan回歸通過；CPU回歸略過62項CUDA檢查，仍須遠端執行。2757檔、4日合成帳務區塊實際使用32768格，報酬與全action梯度誤差均為0；包含H2D的dense對分桶為25.055對7.666ms（3.269倍），resident-only僅7.455對6.652ms（1.121倍）。這不是整輪或策略品質加速。證據為`OP/sparse-capacity-buckets-local-v19-acceptance.json`；凍結來源SHA為`86c8eb78f524ab9d9a9f91daa208cf036acc4bcd4d809e51a3813cad9e4b1ba3`。

遠端v18已通過256項pytest與三項production完整模型／年度累積梯度檢查；兩張RTX5090的完整帳務區塊含傳輸分別為2.728與2.450倍，resident-only僅1.191與1.195倍。實際維護集中copy的遠端微測僅1.517與1.357倍，不能沿用本機3.83倍。

v18完整fold交互作用篩選已完成。各兩次完整四epoch流程的steady epoch3+最大rank中位數，control為8.574／8.244秒，緊縮稀疏帳務為4.572／4.384秒；兩次平均8.4086→4.4780秒，為1.8778倍。採用encoder+head Graph、native copy及完整事件緊縮，未改模型、lookback、batch或資料範圍。最新一次所選方案完整fold為159.89秒，含驗證、測試、checkpoint及繪圖；不能把epoch倍率直接套到fresh-process全程。證據為`OP/remote/sparse-ledger-engine-screen-v18.json`。v19已保留v18產物並停止其後續程序，停止收據為`OP/remote/sparse-capacity-v19-supersedes-v18-stop.json`。

v18的head Graph完整fold已完成且實際root／重播核對通過，但峰值31904MiB、僅0.687GiB餘裕，因此遭1.5GiB容量門檻拒絕。v19另外交叉測試分桶、native／Triton copy與eager／head Graph／encoder+head Graph，七種初始組合均通過完整fold、實際Graph與容量檢查；沒有縮小模型、lookback或batch。分桶與control各做兩次後，steady epoch均值為4.2777對4.4720秒，約1.0454倍，選入分桶、native copy與encoder+head Graph。第一個分桶fresh-process fold因新形狀編譯耗時401.14秒，不能把hot epoch收益代替冷啟動流程。證據為`OP/remote/sparse-ledger-engine-screen-v19.json`。

v19五組完整四epoch工程fold也全部通過：log-utility、Sharpe、return、Sortino、MDD的steady epoch分別為3.279、4.204、3.362、4.200、4.099秒，含每輪驗證、測試、曲線、繪圖及checkpoint。這些數值不是收斂或策略品質結果。v20保留其產物後依程序身分停止v19，`OP/remote/parallel-day-v20-supersedes-v19-stop.json`確認無剩餘所屬程序；當時未完成的v19 profile不作閉合時間線證據。

### 平行逐日帳務與解析反向前綴：可行性研究，尚未整合

逐日串行的原因是資金會決定整張數量。對通過每天收盤無持股、無claim金額、存活且carry cost不變證明的路徑，可將狀態縮到累積已實現損益`P[t]`，淨值為`capital + P[t] - cost`。每天仍呼叫相同的canonical session executor，取得`F[t](P[t-1], action[t])`；猜測一組前日損益後，所有日期可平行算出增量，再用prefix-sum更新猜測。這是因果下三角方程，最多T次迭代可逐日固定T日；若較少迭代已達原位整張結果與損益一致，才有加速價值。未通過證明或迭代未收斂時，必須回原串行路徑，不能把近似資金當作真實帳務。

反向不用保留所有固定點迭代。對每個獨立日同時計算`a[t]=dF[t]/dP[t-1]`及`J[t]=dF[t]/daction[t]`，一次日映射VJP即可取得兩者。令`beta[t]=dLoss/dlog_return[t]`，則狀態adjoint滿足`lambda[t]=(beta[t]-beta[t+1])/NAV[t] + a[t+1]*lambda[t+1]`；以反向affine prefix求解，再由`lambda[t]*J[t]`得到action梯度。分塊BPTT邊界與年度reset仍要按原計畫處理。純MDD的cash退化解、整張surrogate、費稅與無前視限制均保留。

本機8日與32日合成CPU oracle的前向報酬誤差0，五種loss的action梯度皆符合`rtol=1e-5, atol=1e-8`。32日壓力測例的每次前綴最大變動依序為280530、4623、577、5.82e-11元，四次已穩定；此結果不能推廣為所有真實來源都四次收斂。

RTX5070Ti的2757檔resident GPU原型通過16日及64日前向與五種獨立loss梯度核對。各做四次前綴更新，報酬誤差0、固定點殘差0；16日含action反向為18.599對10.135ms（1.835倍），64日為47.391對36.248ms（1.307倍）。這只包含合成帳務，不含NN、來源傳輸、完整fold與策略品質。原型已針對vmap欄位stack、scan與CSR lookup提供批次支援。初期lookup失敗後發現AOT重用先前不合適的process-local vmap快取；僅在探針中關閉AOT cache，保留原cache與失敗紀錄。另一個capture失敗以同stream新leaf預熱修正。尚未把這些探針設定套入正式trainer。

證據為`OP/parallel-flat-ledger-cpu-analytic-adjoint-v3.json`、`OP/parallel-flat-ledger-cpu-stress32-analytic-v4.json`、`OP/parallel-flat-ledger-local-gpu16-v5.json`與`OP/parallel-flat-ledger-local-gpu64-v6.json`。區塊間三種財務狀態的任意upstream VJP仍須獨立驗收；原型不影響遠端v18／v19的封存來源及正在執行的工程比較。

### 精確前向與平行日期反傳（v20遠端工程驗收）

固定點前向的速度取決於收斂迭代數。另一個較一般的方案沿用原canonical串行前向，僅額外記錄每日前的PNL、cost、NAV及calendar狀態；反向以這些精確輸入平行呼叫相同日映射，取得局部Jacobian，再用上述affine prefix組合。它不近似資金、不改成交或持倉，也不保留串行BPTT的整段activation tape。pending forward各自持有trace副本；真的使用turnover penalty時走原canonical VJP。

初期8日CPU oracle涵蓋五種獨立loss與任意upstream VJP、非零PNL/carry cost、獨立初始NAV、entry mask與corporate-action cursor；新程式三項CPU檢查通過。共享財務／return-adjoint回歸為135通過、51項CUDA留待實卡。初期證據為`OP/parallel-adjoint-cpu-v20-v3.txt`與`OP/parallel-adjoint-financial-cpu-v20.txt`，後續最終來源的檢查範圍另列於下。

四組GPU pending/chunk VJP隨後通過，但2757檔、16日完整帳務微測顯示，精確串行前向加平行重算反傳為27.565ms，原saved-tape為20.624ms；含H2D為31.569對23.404ms。這個分支數值正確而性能不合格，不能把早先純平行原型的1.835倍套在它身上。證據為`OP/parallel-adjoint-local-gpu-v20-v2.txt`與`OP/parallel-adjoint-financial-local-v20-gpu0.json`。

接續實作`STOCKAGENT_DAY_TRADE_PARALLEL_ADJOINT=2`：四次平行資金前綴更新只產生候選；除了原完整財務證明，還須殘差≤1e-6元，並逐股比較在候選與隱含前日資金的包絡、再向兩側擴張`block_rows * 1e-6`元後，canonical開盤配置的整張股數、entry cost及資金限制前的target shares完全一致。最後一項避免相同成交數掩蓋其他股票target改變與共同縮放比例的變化。另須整段日內NAV距離破產邊界超過誤差預算，log-return比例距離clamp邊界也保留相同包絡。即使殘差為零，整張、資金配置或破產邊界附近仍須回退。未通過時立刻使用原串行前向與VJP，沒有先以候選更新參數。通過時保存一次局部Jacobian，反向僅重播affine prefix，不再複製來源或重算日映射。此分支仍有GPU證明讀回主機的等待，尚非compute-only。

本機最終CPU狀態／checkpoint／資金與破產區間檢查為8項通過；共享financial／return-adjoint回歸為137通過、56項CUDA另驗。GPU驗證按來源階段保留：較早九項pending／inference／一般狀態檢查通過；縮窄候選輸出後重跑其中五項；最後加入target／日內破產證明後，以一個實卡測試驗收兩種拒絕情境。這些範圍重疊，不能相加成15個不同檢查。完整來源另在遠端重跑全部18項CPU／GPU測例及既有金融測試。證據與各階段來源snapshot見`OP/parallel-adjoint-local-v20-acceptance.json`；最終CPU日誌為`OP/parallel-adjoint-cpu-v20-v11.txt`。較早fixture誤用FP32與測試讀取requires-grad scalar引發warning的失敗日誌均保留；沒有放寬正式帳務或梯度驗證。

第一次平行完整日映射的微測沒有速度收益，因此針對方案將四次候選迭代縮成只計算PNL的canonical日映射，讓編譯器移除未使用的中間輸出，最終一次才計算完整財務輸出及局部Jacobian。最終來源2757檔、16日完整帳務區塊以六組交錯量測，resident為13.462→11.252ms（1.196倍），含相同25.95MB來源傳輸為18.558→16.081ms（1.154倍）；return誤差0、全action梯度最大誤差1.164e-10，53次平行前後向均實際重播且未回退。這只含帳務區塊，不能外推NN、完整epoch或策略品質。證據為`OP/parallel-adjoint-financial-local-v20-balance-gpu0.json`。

v20已封存，來源SHA為`bd3f1cba9bc76d769f7fa51b7d2572cdc70588a89187168dc1255fa5b4820727`，runtime SHA維持`850a27d155881eba53360c818f44a00519aeedf3fa0ab204c66da0bce295561f`。遠端雙RTX5090工程程序`independent-loss-parallel-v20-en-20261015T201940-323c1e31`已啟動，先驗收最終來源oracles，再執行完整fold、提前閉合前版profile、四種帳務排程和平衡重複、全部五個獨立loss及選定方案profile。正式五組收斂尚未啟動；通過工程驗收與離機checkpoint保全後才交接，patience100與每輪完整產物保持不變。

第一次遠端重驗先通過三個production NN oracle，pytest在129項通過後，遇到參考圖的`retain_graph=True`與fused canonical maps的buffer donation不相容。測試修訂只讓每個獨立loss重建一次參考前向再反傳，保留所有原有數值、狀態、pending output及梯度斷言，不關閉donation或修改production來源。原始17檔測試與失敗日誌保持不變，新測試另存`OP/gpu-tests-v20-attempt2/`及獨立SHA manifest；修訂理由見`OP/parallel-day-v20-attempt2-test-revision.json`。重驗程序為`independent-loss-parallel-v20-re-20261015T202612-9386eaba`，不能將前次失敗算成通過或正式訓練。

實際Nsight Compute計數器探針在本機回報`ERR_NVGPUCTRPERM`；未修改驅動權限。v21隨後在遠端獨立探測，也被相同權限拒絕。`OP/gpu-counter-access-local-v20-gpu0.json`與`OP/remote/gpu-counter-access-remote-v21-gpu0.json`保留實際結果，不能據此聲稱達到GPU roofline；完整時間線的有效kernel、傳輸與等待分析另行保留。

### 遠端編譯相容與最新瓶頸（v21，2026-10-16更新）

v20的donation相容參考圖在本機8項CPU檢查通過。遠端重驗則先通過132項pytest與三個production NN oracle，之後PyTorch2.11在平行日映射的`(raw_marks > 0).cumprod(0).bool()`相關pointwise codegen發生`tensor_dim=None`。單獨將tiling限制為一維的隔離探針仍有同一失敗；另一個平行前向案例通過，不能據此宣稱整個分支可用。本機為PyTorch2.13，不能將本機的GPU通過直接代替遠端相容性證據。原失敗日誌及來源均保留。

v21在shared inventory primitives加入opaque的存活prefix：先用`~(NAV > 0)`計算失敗比較，再以int64累積計數是否為零。這與原布林cumprod的吸收性逐值相同，包含零、負值、NaN與無窮值；來源價格、完整存活與帳務驗證仍由原canonical executor處理。第一次將int64接入僅接受FP64的金額scan被CPU測試拒絕，之後改為獨立布林primitive，沒有放寬原金額scan的dtype guard。三處存活prefix共用它，parallel vmap明確移動日／分鐘軸；Graph與checkpoint契約新增`opaque_int64_nonpositive_prefix_v1`，拒絕跨排程optimizer resume。

封存前76項CPU回歸通過、75項CUDA留待遠端；另有獨立的不同行情日失敗位置／分鐘軸CPU檢查通過、1項CUDA留待遠端。來源SHA為`dc53395d6e534dfc6b641e39bc2d2607352a6f3475c395a11f630892de685eee`，runtime不變。遠端工程程序為`independent-loss-prefix-v21-engi-20261015T204653-93c1b1cf`；本機CPU證據為`OP/parallel-prefix-local-v21-candidate.json`，它明確不是GPU／完整fold／正式收斂驗收。所有五個loss保持獨立、seed42及patience100，正式收斂尚未開始。

v21隨後已在實際雙RTX5090環境通過284項pytest（1256.80秒）；另三個完整NN／pending-forward／leaf-gradient累積檢查各自在production warning context通過，因此pytest的3項deselected沒有省略其語意檢查。這包含前版失敗的平行日映射。完整pytest日誌前綴已獨立保全回本機並核對SHA；收據為`OP/remote/parallel-prefix-remote-pytest-v21-admission.json`。這是GPU正確性驗收，後續兩卡financial微測、完整fold篩選、閉合profile與正式收斂仍分開驗收。

v21兩卡的packed copy、sparse／bucketed financial、device constant與平行前後向micro皆通過。2757檔、16日的平行帳務含傳輸微測：GPU0為15.370→9.981ms（1.540倍），GPU1為15.089→10.529ms（1.433倍）；resident為1.600／1.616倍。兩卡的報酬最大誤差均0、action梯度最大誤差約1.164e-10，各53次平行前後向且fallback0。這是合成帳務區塊，不含NN與fold lifecycle；真實來源完整fold的選擇另列於下。收據與逐檔SHA為`OP/milestone-return-adjoint-v21-gpu-and-micros.json`。

v21同來源六個完整四epoch fold完成後，選定mode2平行帳務。control與parallel各兩次的steady epoch3+最大rank中位數取平均，分別4.61179與3.69765秒，吞吐約1.24722倍。parallel的第一次／第二次完整fold分別821.28／228.95秒，control為465.53／190.66秒；因此冷啟動及短fold沒有同比例收益，不能只報hot epoch。第一次parallel各epoch均記錄190次平行前向、95次解析反傳及零回退；第二次的逐日return重播最大誤差約2.23e-16。首個parallel峰值顯存約25336MiB，最低餘裕7.10GiB。模型、lookback、batch及每輪完整產物不改。證據為`OP/milestone-real-parallel-ledger-v21-balanced.json`及`OP/remote/sparse-ledger-engine-screen-v21-completed.json`；五loss與閉合profile仍須另行通過。

修正編譯時，已另外完成source20的mode0分桶／encoder+head方案完整四epoch fold與閉合profile。兩rank的同一完整epoch分別為4.0096與4.0114秒，有效kernel聯集為2.9560與2.5274秒（73.72%／63.00%），共397813／336864次kernel；另一rank另有0.2878秒NCCL聯集。H2D各約2.95／5.08GB，stream synchronize為77／67次。這是帶tracing的時間線，不能拿來取代未instrument的平衡測速，也不是SM occupancy。證據為`OP/remote/coverage-v20-predecessor.json`及`OP/remote/host-boundaries-v20-predecessor.json`。

固定已量到的kernel時間、樂觀地刪除所有非有效kernel時間，主rank上限約為4.0096/2.9560=1.356倍；這不是GPU演算法加速的上限。若還要數倍收益，必須減少帳務／VJP的kernel數與工作量，而非僅增加busy百分比。平行逐日與解析反向會以真實來源的完整fold、實際重播／回退計數與容量測試決定是否選入；尚未證明compute-only或新策略品質改善。

同一閉合profile另外以kernel的correlation ID對回CPU enqueue範圍。GPU0的397803個非NCCL kernel中，290444個由loss／帳務前向發出，80389個由反傳發出，模型前向為24564個；各自kernel時間加總為1.346、1.166與0.441秒。全部kernel的82.0%單次≤5微秒、78.5%的grid≤32個block，中位數1.248微秒。這提供優先合併帳務工作及反傳的實測理由；時間加總可重疊，短kernel／小grid本身仍不等於SM occupancy或roofline。分析保留未配對correlation類別，這次沒有未配對項。證據為`OP/remote/kernel-granularity-v20-predecessor.json`，可用`OP/analyze-adjoint-kernel-granularity.py`重算。

當時提出的下一方案是重用return-adjoint第一遍已驗收的逐日log return作為第二遍資金候選。初始NAV與carry cost已知時，可用FP64的`NAV0 * exp(cumsum(recorded_returns))`重建下一日資金，讓第二遍直接計算一次canonical日映射及局部Jacobian，省去四次猜測迭代。它只能作候選：仍須通過完整資金／整張／target／破產區間證明及原`ReturnAdjointTape`重播驗收，失敗回canonical串行；不能快取NN投影或將過期狀態當帳務。此初始提案後來已實作為v22；最新的局部量測、驗收邊界及遠端狀態見本文前段，尚無正式策略品質結果。

### 有界fusion與單block歸約診斷（v21本機工程）

2757檔、16日完整合成帳務區塊以獨立process測試fusion上限8→32→8，三次均通過各自dense／sparse的報酬及全action梯度檢查。fusion32的resident sparse中位數29.047ms，fusion8兩次為19.254與13.370ms；含H2D則31.731對22.238／19.155ms。fusion8本身存在變異，不能據此宣稱跨硬體最適值；32未選入正式設定。

後續兩次真實Graph profiler顯示fusion8／32分別2400／1904個kernel，device時間加總7563／12344微秒。32最慢的一個歸約每日至少約213微秒，grid只有1個block、每block64threads；generated kernel為2757元素、76次load、兩次reduction的複雜FP64反向。少kernel反而變慢，具體原因是合併後的scalar reduction平行度不足；沒有硬體counter證據可聲稱register spill或SM occupancy。

接續在獨立process啟用cooperative reduction，並在一般compiler heuristic之外，額外選取小輸出、1024至4096元素的歸約。完整區塊resident sparse為23.226→12.042ms（1.929倍），含相同來源H2D為28.326→16.143ms（1.755倍）。跨policy的報酬、逐日收盤NAV、全shares及全action梯度最大誤差均為0；實際Graph trace出現64個block的歸約，kernel數1904→1936，device時間加總11697→3965微秒。這證明可透過修正分塊救回fusion32，不能說已勝過fusion8、遠端Torch2.11或完整fold。證據為`OP/scalar-reduction-v21-comparison.json`與各policy原始trace。

原生`force_cooperative_reductions=True`在fusion32實際失敗：CUDA回報`too many blocks in cooperative launch`，fusion8尚未執行；拒絕收據為`OP/native-cooperative-v21-rejection.json`。不能強制所有形狀共用cooperative grid。接續探針以專用financial backend的inner compiler作用域安裝選擇器，兼容Torch2.11的features參數與2.13的device／numel參數。本機fusion8／32各通過報酬、逐日NAV、整股、梯度及caller compiler還原；resident sparse為10.546／10.520ms，含H2D為13.422／14.025ms。證據為`OP/scoped-cooperative-v21-fusion{8,32}-summary.json`；這是串行帳務路徑，與平行方案的組合結果另列於下，封存production未改。

快取相容性另行驗收。Torch2.13會將factory回傳instance的uuid納入portable config，2.11則保留callable，不能假定兩者會以相同方式識別新實作。相容factory明確pickle內容uuid；兩節點均以實際`FxGraphCachePickler`驗證相同內容的獨立factory產生相同key、內容uuid改變則key改變，並還原caller choices。這是CPU介面／快取檢查，不是遠端GPU驗收。證據為`OP/scoped-compiler-policy-local-v21-compat-cache-admission.json`及`OP/remote/scoped-compiler-policy-remote-v21-compat-cache-admission.json`。

與mode2平行帳務合併的獨立process探針遭拒絕。未加cooperative的平行版resident為11.092ms、含H2D為14.272ms，53次平行前後向均零回退。cooperative組先通過報酬與梯度核對，後續卻出現一次證明失敗，嚴格的零回退斷言使探針失敗。另一次診斷保留該失敗的原始狀態：候選殘差17222.40元、state Jacobian非有限且候選NAV為NaN；canonical fallback保住了正確輸出，未使用該候選更新。其餘52次平行前後向通過。診斷下resident平行為12.541ms、含H2D為14.485ms，亦沒有比未加policy的平行版快。尚未證實非有限值的根因，沒有放寬財務證明或將串行微測收益相乘。此組合未選入遠端正式版本；證據為`OP/scoped-parallel-cooperative-v21-normal-summary.json`及`OP/scoped-parallel-v21-fallback-diagnostic.json`。

證據為`OP/financial-fusion-screen-local-v21-attempt2.json`、`OP/financial-fusion-profile-local-v21-{8,32}-summary.json`及原始trace。首個探針因tmux server繼承空的CUDA_VISIBLE_DEVICES而未進入GPU，失敗原樣保留；後續canonical launch明確設定GPU0並重新通過strict CUDA，不更動全域tmux環境。尚無完整fold或策略品質結論。

v21第一輪真實四epoch fold全部通過lifecycle及容量：稀疏／encoder+head Graph control的steady epoch3+最大rank中位數5.018秒，dense為9.242秒，序列前向＋平行反傳4.346秒，平行前向＋平行反傳3.724秒；fresh-process完整fold分別465.526、229.996、388.107、821.278秒，保留新形狀編譯及最終回測成本。平行前後向四個epoch均實際執行190次平行前向及95次平行VJP，fallback為0；這是目前四輪的真實來源結果，不保證後續策略狀態不回退。離機snapshot及數值為`OP/remote/sparse-ledger-engine-screen-v21-first-four-folds.json`、`OP/milestone-real-parallel-ledger-v21-first-fold.json`。

兩個finalist各重複一次完整fold後，平行方案第二次steady epoch為3.671秒、完整fold228.951秒；control第二次4.205秒、完整fold190.657秒。平衡steady epoch均值為3.69765對4.61179秒，選定平行前後向，約1.24722倍；不能把這個倍率套到四epoch完整fold或宣稱全部開銷已消失。其餘四個loss及選定方案的閉合兩rank profile仍須通過，才可交接正式收斂。證據為`OP/milestone-real-parallel-ledger-v21-balanced.json`及`OP/remote/sparse-ledger-engine-screen-v21-completed.json`。

先前停止／完成的 v5–v7 fold、失敗診斷與 v6–v8 凍結 source 已回傳：562檔逐一SHA核對、14份 checkpoint與6份獨立模型皆可CPU有限值載入。證據為 `OP/local-independent-loss-stage2-preservation.json`。舊 seed／集成任務已取消，其停止產物也已原樣保留，不能列為待續訓解法。

## 固定規則的長訓練

遠端監督 run 為 `pure-log-convergence-v1-20261015T075121-fbc6d9ab`，已於台北時間 2026-10-15 17:35 完成，exit0。執行順序為 baseline AdamW → Muon matrix lr scale2，每組使用新產物根目錄，不載入舊 64 輪研究的 optimizer 或 checkpoint。年度帳戶、年度 DDP 分工及每個完整 trajectory 一次更新都沿用已實測路徑。

兩組均達連續100次 validation 未改善，完整 lifecycle 驗收通過。同149日（2026-02-26至10-02）、起始1,000萬元、相同費稅及2330 benchmark：

| 方法 | validation 選定 / 停止 epoch | 費後報酬 | Sharpe | Sortino | 日收盤 MDD 跌幅 | 分鐘 MDD 跌幅 | steady 完整 epoch 秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline AdamW | 236 / 336 | 51.84% | 1.449 | 2.270 | 20.73% | 25.32% | 7.430 |
| Muon matrix lr2 | 282 / 382 | 19.51% | 0.667 | 0.980 | 22.70% | 23.09% | 7.385 |
| 2330 benchmark | — | 25.18% | 1.097 | 1.755 | 12.37% | 未用相同分鐘來源計算 | — |

表內報酬為這149日的非年化複利報酬；Sharpe／Sortino沿用canonical日log-return及252日年化。具體為`Sharpe=sqrt(252)*mean(r)/std(r,ddof=0)`、`Sortino=sqrt(252)*mean(r)/sqrt(mean(min(r,0)^2))`，零報酬為風險基準，downside分母涵蓋所有日期。MDD由含起始NAV的財富路徑計算，表中顯示正的跌幅，越小越好。策略與benchmark使用完全相同的指標定義、日期與年化政策。

baseline 達成報酬、Sharpe與Sortino三項，但未達較小MDD；Muon沒有維持64輪初篩優勢，四項均落後benchmark。因此不能以短研究選擇Muon，也沒有證據宣稱已同時改善四個目標。兩組速度相近，不能從較好策略結果推論硬體更忙或較快。

完整fold牆鐘時間（含準備、所有epochs、validation／test曲線、出圖、保存與lifecycle驗收）為baseline **2,927.22秒**、Muon **3,279.78秒**，兩組均未觸發容量重試。Muon多跑46輪，不能把較長的完整fold解讀為每輪較慢。來源為各組`*-speed.json`的`elapsed_s_including_failed_attempts`；steady epoch與完整fold分開報告。

原始逐日log-return重新計算，與保存metrics最大誤差0；分鐘最後一點與日複利NAV誤差小於1.4e-15倍初始資本。另以凍結來源及完整canonical bridge重播baseline的149日，日log-return最大誤差6.94e-18、分鐘NAV1.87e-9元，整張持倉、權重、違約欄位完全相同。回撤不是已知的報告前綴錯誤造成。配對10日區塊、2,000次bootstrap的baseline相對benchmark Sharpe差異95%區間為[-3.090,3.826]；149日不足以確認穩定優勢，這也不校正已做過的多次實驗。

完整產物已回傳本機，189個檔案逐一SHA核對，2份best模型checkpoint、2份含optimizer狀態的last checkpoint及2份獨立模型均可CPU有限值載入。best保存選模權重，last才包含optimizer／scheduler／RNG及停止計數，不能拿best冒充optimizer續跑點。證據：`OP/local-convergence-preservation.json`、`OP/converged-quality-audit.json`、`OP/converged-baseline-cpu-replay.json`。本機可讀入口：`OP/convergence-analysis-inputs/{baseline,optimizer__muon_lr2}/`。

同日期的[淨值與日回撤圖](../artifacts/operations/training-quality-audit-20261015/converged-figures/converged-nav-drawdown.png)直接來自上述保存的逐日帳務；另有[訓練與驗證loss圖](../artifacts/operations/training-quality-audit-20261015/converged-objective-figures/train-validation-objective.png)，展示達到最佳validation後，train仍改善、validation卻未同步改善的實際曲線。圖形收據與SVG在各自目錄。

| 固定項目 | 本輪設定 |
| --- | --- |
| 收斂規則 | 最多 1,000 epochs；每輪 validation；連續 100 次未超過既有 `min_delta=0.0001` 的改善即停止 |
| 選模 | 僅依 validation；不限制最佳 checkpoint 必須在前幾輪 |
| LR | 原 warmup cosine、32 次 warmup、horizon1,000、base LR1e-4、eta_min1e-6 |
| 模型與批次 | 實際 baseline 的 d32 / lookback32 / 14,726 features / 2,757 slots；global128、local64、eval16 |
| 目標 | 純費後 log-utility；三個額外風險權重、turnover 項與新增探索代理均為零或關閉；沿用既有整張梯度契約 |
| 加速 | 雙 RTX5090、BF16 AMP、resident sparse projection、Quack softmax、已驗證的帳務 Graph 與裝置預取 |
| 保存與報告 | 每輪 validation / test 診斷曲線及原有圖表；最佳與最後 checkpoint；完整 fold 產物驗收 |

到達 1,000 輪而未滿足 100 次停滯，會記為 `epoch_cap_not_proven_converged`，不稱為已收斂。本輪完成後已產生 `convergence-audit.json`並停止；最新使用者要求進一步分析及實測，才啟動下一節的兩個新對照。validation 停滯是操作上的收斂規則，不證明全域最佳。

可重用設定：[長訓練矩陣](../configs/ablations/tw_pure_log_convergence_last_fold_20261015_v1.yaml)。遠端以保留的完整 baseline YAML 作外部設定輸入，搭配不可變的來源 capsule 執行，未覆寫遠端主 checkout。鎖定協議為 `OP/pure-log-convergence-v1-final-plan.json`，SHA `3f28f9319cba36e79af10b868ffe4cc5da18640f49adf602668011c88f1dfd2a`。來源 SHA 為 `d4b024de78ed88894cb1cc92ed0bbf93ca00cf3a67a3c39eb30b79c83a017dc5`；重新封存的實際 runtime SHA 為 `850a27d155881eba53360c818f44a00519aeedf3fa0ab204c66da0bce295561f`。新 runtime 收據揭露 fonttools、wrapt 與來源套件 metadata 的差異；CUDA、模型及 financial owners 沒有因此變更，沒有強行通過舊 runtime lock。

遠端產物根目錄為 `artifacts/markets/tw_pure_log_convergence_last_fold_20261015_v1/`，其 `baseline/`、`optimizer__muon_lr2/` 為 canonical 容量選擇入口。只有實際 CUDA OOM 才允許 canonical capacity retry 減少 batch 並從新的 optimizer root 重跑；失敗組保留並繼續下一組。初篩量得的約 7.2 秒 steady epoch 不能代替此次長訓練的完整 epoch / fold 計時，也不代表已證明硬體全域最佳效率。

啟動後於07:55:39 UTC唯讀驗收前14輪：每輪梯度有限且非零、每輪一次optimizer update、無帳戶死亡／違約；epoch3以後沒有新增Dynamo graphs。最新一輪有111次帳務Graph forward replay、95次backward replay，並非只有開啟旗標。epoch3+最大rank時間中位數7.45秒，共12個樣本；當時GPU0 reserved22.91GiB、cgroup用量180.64/241.76GiB。last checkpoint模型及optimizer已CPU finite讀取，內存的patience確為100。此為啟動驗收，不是長期容量、完整fold速度或收斂品質結論。證據：`OP/remote/pure-log-convergence-v1-live-acceptance.json`。

## 收斂後的機制診斷與第二輪對照

1. **梯度存在，但支持集合很集中。** 全部336/382輪均記錄一次optimizer update、非零梯度、帳戶存活。baseline每次更新前梯度範數7.54至61.81，clip1每輪都作用；這不是梯度消失，也不能把AdamW的全域縮放直接解讀為等比例降低學習率。模型原始請求每日平均2.544檔，全數為空方；最大一檔平均占請求gross的80.18%，平均有效檔數`1/sum(p²)`只有1.560。稀疏entmax與整張成交使支持邊界對分數變動敏感；高return並不自動帶來低尾部風險。
2. **區分請求、成交與收盤。** baseline原始請求gross68.47%，canonical bridge後67.89%，可進場65.70%，整張目標64.04%，實際成交約62.48%；實際成交平均2.074檔。收盤全平倉是預期結果，不能用全零收盤持倉聲稱沒有承擔盤中風險。僅把請求攤到更多股票可能被整張門檻歸零，前面的dense/sub-lot研究已顯示此失敗，不再次當成未驗證的新解法。
3. **日報酬的順序影響MDD，純log增長不包含這個資訊。** baseline最大日收盤回撤發生在2026-04-30高點至06-09低點；分鐘MDD更大。相同報酬集合換順序，log-utility及Sharpe/Sortino不變，MDD可大幅不同。維持簡單loss時，應實測是否改善跨時期訊號穩定性，不能保證靠optimizer同時壓低MDD。
4. **選模與分布改變。** baseline在2025 validation的報酬238.84%、Sharpe3.484；2026 retrospective test為51.84%、1.449。Muon validation有222.53%、Sharpe3.116，test為19.51%、0.667。validation有243日、test149日，不能只由累積報酬差就判斷過擬合；baseline的年化平均log增長約1.266對0.706、Sharpe亦較低，顯示觀察到的跨期落差，尚未證明是哪一機制導致。訓練loss到最後仍持續下降，不能以此推定泛化改善。沒有使用test挑checkpoint；持續觀察過的2026資料也不再是未觸碰的持出證據。
5. **IC報告存在可重現錯誤。** 原IC使用`argsort`位置當排名，對大量同值未取平均rank，讓全零收盤權重也得到虛假的非零負IC。真實149日兩組收盤權重均全零；這個IC不能解釋方向優劣。修正為同值平均rank，常數或不足兩個有效樣本回報0，表示缺少可辨識相關；不是正面訊號證據。報告契約升至3，拒絕重用舊報告，optimizer ABI與金融帳務不變。51項測試通過，包括SciPy oracle、股票置換、全零大型面板、CPU/CUDA、完成報告與checkpoint lifecycle。

第二輪曾預先固定兩個假說，分別與上述seed42/BF16 baseline比較；其中FP32已完成，seed73已因使用者改要四個獨立loss而停止。不以test挑seed或超參數：

| 新對照 | 唯一變更 | 要辨識的原因 |
| --- | --- | --- |
| `precision__temporal_fp32` | seed42、時間層改FP32；其餘BF16 AMP政策不變 | 降低時間表示的捨入是否改善validation-selected產物；64輪初篩不足以判定 |
| `seed__s73` | 相同fold11、BF16，僅初始化seed42→73 | 分離隨機初始化變異；先前fold10+seed73同時改fold與seed，無法回答此問題 |

仍用d32/lookback32、global128/local64、純費後log-utility、最多1000輪及patience100，保留所有曲線、圖與checkpoint。凍結來源只修改`stockagent/evaluation/metrics.py`與報告版本常數，SHA `281dd060b9b5134abdfd81bacecb9f54fef116d4f6f1cf7ad23fa4ded9500b3d`；runtime沿用`850a27d...`。遠端監督run `quality-convergence-v2-20261015T130457-dd47a695`，產物根目錄`artifacts/markets/tw_pure_log_mechanisms_convergence_20261015_v2/`。遠端協議`OP/quality-convergence-v2-plan.json`，本機離機副本為`OP/remote/quality-convergence-v2-plan.json`，SHA `1bf3475429454ca2fa46b2a53ec102204997c057d5de15636c7038a5aed0b083`；狀態以當次readback為準，沒有宣稱新兩組已完成。

另直接比較實際產生的完整YAML與舊長訓練baseline：除實驗名稱與輸出目錄外，兩組分別只差`training.financial_transformer.temporal_blocks_fp32`與`training.seed`，沒有其他設定差異。證據：`OP/remote/quality-convergence-v2-resolved-config-diff.json`。

FP32組已達100次停滯，選定epoch107、停止207，完整fold lifecycle通過。其149日費後報酬**34.87%**、Sharpe**1.343**、Sortino**2.043**、日MDD**18.91%**、分鐘MDD**21.98%**；三項報酬／風險調整報酬高於benchmark，MDD仍較差。相較baseline，只有MDD較小，另外三項較低，尚無全面替換依據。完整fold **2,422.88秒**、steady MAX-rank epoch **10.280秒**，相較baseline每輪約慢38.4%；總時間較短是因較早達到停滯，不是每輪較快。

已先回傳此完成組的五個關鍵檔案，逐一SHA核對與best模型CPU finite通過；canonical指標重算誤差0。全部149日的CPU bridge／整張帳務重播，日log-return最大誤差6.94e-18、分鐘NAV1.87e-9元，離散欄位完全相同。實際平均開倉gross **50.87%**、平均成交 **2.020檔**；較baseline曝險低，因此較小MDD不能直接解讀為更好的預測。其gross成交損益5,522,259.73元、費稅2,035,405.77元，淨損益3,486,853.96元；逐日現金流最大殘差2.25e-9元。證據：`OP/local-quality-v2-fp32-readback-preservation.json`、`OP/quality-v2-first-three-audit.json`、`OP/converged-fp32-cpu-replay.json`及其`-cashflows.json`。[三組已完成策略的同期間比較圖](../artifacts/operations/training-quality-audit-20261015/quality-v2-first-three-figures/converged-nav-drawdown.png)另存；seed73後來已停止，未把其中途test當最終成果。

另逐bit核對這三組best checkpoint中的`candle_encoder.causal_feature_rms_scale`，14,726維完全相同，buffer SHA `becac594643a852d091281f90014952c0004b1ea32855d083c00ba06fb0ec88c`。因此本次差異不能歸因於這份RMS buffer改變；這也不證明中間activation或最終預測逐bit相同。證據：`OP/converged-input-rms-comparison.json`。

### 固定等權seed配置對照（已取消，保留歷史協議）

為實測「不增加loss項目、用不同初始化的選股互補」這一假說，新增**唯一一個seed42／seed73各50%原始意圖平均**的研究對照。登記時seed73尚未產生最終metrics、觀察至epoch104，沒有讀取其test數值；等權成員與比例之後不再改動。2026資料先前已被觀察，因此仍為回顧研究，不能改稱全新持出證據。

兩個成員各自依原validation log-utility選best checkpoint，皆須先達100次停滯與完整產物驗收。確認來源ABI、日期、股票順序、benchmarks、初始1,000萬元、帳務契約及RMS完全一致後，以FP32運算`(w_seed42 + w_seed73) * 0.5`，再走canonical bridge與全部整張／容量／資金／費稅帳務。每個成員的L1預算不超過1，平均仍在同一預算集合；實際成交可能因取整或多空抵銷而改變，必須量測。沒有平均return／NAV，也沒有新增訓練、探索代理、風險loss或混入benchmark。

這項研究已依使用者修正取消，沒有替換baseline或部署。歷史協議為`OP/seed-ensemble-plan.json`，SHA `535189f6e2e924cb0fa08311b853b2f1f8dc7a4c75c896c78028a1115f35d635`；時點證據`OP/remote/seed-ensemble-registration-status.json`。本機監督run `seed-ensemble-analysis-20261015T140517-d2f10422`已停止，不再等待成員續訓，沒有這組最終比較或四指標改善成果。

技術依據：同值平均rank與常數相關未定義可參照[SciPy Spearman](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.spearmanr.html)；局部FP32是既有AMP支持的計算精度政策，參照[PyTorch AMP recipe](https://docs.pytorch.org/tutorials/recipes/recipes/amp_recipe.html)。這些資料不提供本策略會改善的保證，效果只由上述實測決定。

### 費稅、回撤與 benchmark 的經濟機制

兩組都以凍結來源完整CPU重播149日，且對canonical FIFO實際成交作唯讀歸因。每日平倉後，`NAV變動 = gross成交損益 - 進場成本 - 出場成本`逐日對帳，最大殘差分別2.12e-9及1.93e-9元。首次歸因草稿漏記兩日的terminal close分支，已由對帳拒絕；v3同時觀察一般與terminal FIFO路徑才通過。這是診斷工具的修正，沒有因此改寫訓練帳務。

| 相同實際成交的現金流 | baseline AdamW | Muon lr2 |
| --- | ---: | ---: |
| gross成交損益 | 7,730,539元 | 4,224,771元 |
| 費稅成本 | 2,546,301元 | 2,274,088元 |
| 淨損益 | 5,184,238元 | 1,950,683元 |
| 費稅 / 正的全期gross成交損益 | 32.94% | 53.83% |
| 日平均費稅 / 當日開盤NAV | 12.92bps | 13.01bps |
| 實際開倉gross均值 | 62.48% | 62.87% |

Muon不是單純交易成本特別高或持倉特別少；相近曝險與每日相對成本之下，實際gross損益較差，才是本次落後的主要差異。表中的gross只是相同成交的現金流拆解，不是「零費率重新訓練／回測」的成果。純費後log-utility已將成本納入梯度目標，不能再把未扣費當成已找出的根因。

baseline日MDD的高點為2026-04-30、低點為06-09，中間27個交易日自13,341,556.95元下降2,765,449.86元。其中gross虧損2,331,573.13元、費稅433,876.73元；主要不利交易包括2481、1809、2313、2337、6155。最差五日占全期負simple-return平方和29.55%，但這不是刪除那五日後的新策略結果，也沒有根據test把這些股票列入黑名單。證據：`OP/converged-tail-risk-attribution.json`。

另外逐塊核對來源SHA，把同149日2330報酬分解為官方開盤到收盤與前調整收盤到今開盤的殘差：

| 2330觀測時鐘 | 複利報酬 | Sharpe |
| --- | ---: | ---: |
| canonical調整後前收→今收 | +25.18% | 1.097 |
| 官方開盤→收盤 | -5.97% | -0.511 |
| 前調整收盤→今開盤殘差 | +33.12% | 1.683 |

日log拆解最大誤差1.74e-18，複利應相乘，不能把百分比直接相加；殘差包含總報酬調整，不等於純粹未調整的隔夜價格報酬。這說明當期buy-and-hold收益與日內策略的收益來源不同，全空方也不必然與benchmark上漲矛盾。官方開盤不是保證可成交價，沒有把此拆解當成交易回測或替換原benchmark。證據：`OP/benchmark-clock-decomposition.json`。

### 簡單loss能改善什麼，不能保證什麼

以四個日報酬`[+10%,-10%,+10%,-10%]`與`[+10%,+10%,-10%,-10%]`作數學oracle：兩者純log loss完全相同，期末報酬都-1.99%，Sharpe與Sortino也相同，但日MDD分別10.9%與19%。這只驗證目標的路徑性質，沒有重排真實行情做回測。保持簡單loss的同時，仍需靠較穩定的預測與可實現配置改善回撤，不能要求同一個純log目標在數學上保證四指標同步提升。證據：`OP/log-utility-path-order-oracle.json`。

為辨識曝險與訊號品質，事先鎖定**唯一一個0.5倍下單意圖**診斷，不掃描最佳倍率。固定baseline訊號重新走全部canonical bridge、整張、資金、成交、費稅與帳戶遞推：

| 同一訊號的曝險診斷 | 費後報酬 | Sharpe | Sortino | 日MDD跌幅 | 分鐘MDD跌幅 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 原baseline | 51.84% | 1.449 | 2.270 | 20.73% | 25.32% |
| 0.5倍意圖重播 | 22.65% | 1.429 | 2.228 | 10.69% | 13.30% |
| 原2330 benchmark | 25.18% | 1.097 | 1.755 | 12.37% | 未測 |

半額實際開倉gross約30.91%，日MDD已低於benchmark，卻失去報酬優勢；Sharpe幾乎不變。非線性的整張／費稅必須實際重算，不能把原回撤或報酬乘0.5當作測量。這是已觀察test上的機制診斷，沒有改模型、正式baseline或部署，也沒有證明「選一個更巧的倍率」可在未來同時勝出。證據：`OP/half-budget-diagnostic-plan.json`、`OP/converged-baseline-half-budget-diagnostic.json`。

IC修正後，另對**原始配置意圖**與來源官方開盤→收盤報酬作同值平均rank診斷：baseline平均IC0.006249、Muon0.006733；兩者收盤全零配置的IC均為0。配置意圖IC不是raw model score IC，也沒有包含可成交性與費稅；Muon的平均配置IC稍高，費後成果卻更差，足以說明不能單憑此統計選optimizer。證據：`OP/converged-allocation-ic.json`。

### 由證據決定方法優先順序

| 機制／方法 | 影響訓練的路徑 | 本次證據與處置 |
| --- | --- | --- |
| 訓練、驗證、報告共用帳務 | 同一動作若走不同強制退出或費稅規則，評分不再對準可執行損益 | 已修復前綴報告bridge；兩組長訓練完整CPU重播與FIFO現金流對帳通過，沒有用這項修復解釋不存在的fold11績效提升 |
| 收斂及選模預算 | 每epoch僅一次更新，64epoch只有64次更新；短期快學不代表最後泛化較好 | 保留1000上限與100次停滯；Muon初篩優勢在正式長測反轉，不選用為較佳方法 |
| Optimizer實際覆蓋 | 名稱是Muon不代表所有權重都由Muon更新；本實驗只將隱藏層linear矩陣交給它 | 實際30個matrix tensor共75,264元素；auxiliary仍用AdamW，主要輸入投影也在此組。此分組不能等同「全模型Muon」的測試；CP讀回證據為`OP/converged-muon-named-parameter-coverage.json` |
| 局部FP32 | 捨入會影響時間表示、稀疏支持與整張邊界，進而改變梯度軌跡 | 同seed42長測已完成107/207；比baseline每輪慢38.4%，報酬與Sharpe／Sortino較低、MDD稍小且曝險較低，未證明全面改善 |
| 初始化與隨機變異 | 不同初始分數可能進入不同稀疏支持集合 | seed73研究已依最新要求停止；不再以不同seed替代loss／反傳機制改進 |
| 學習率與停止規則配合 | 原1000-step cosine在336/382停下時仍約為peak LR的77.8%/71.3%，有可能尚未進入小步精修 | 下一階段候選為沿用共用scheduler的validation plateau降LR，保持patience100；本次未實測，不改動正在執行的協議。負loss須沿用abs threshold，且要明示warmup差異 |
| 跨期泛化與輸入容量 | 約14,726維輸入、低千位數日期，重複訓練不會增加獨立市場樣本；強training表現不能排除過擬合 | 2025→2026落差已量測；下一階段候選為訓練期內的特徵群縮減／穩定性篩選與正規化。不得用2026挑欄位，日期RMS與來源版本需重算 |
| 稀疏配置與整張可實現性 | 給更多名字分配小金額，可能只是造成不可成交的一張以下意圖 | dense/sub-lot初期實測未改善，曾使大量資金被取整消除；目前關閉。若重設allocator，必須在固定loss下同時建模整張可行集合與現金，使用新fingerprint驗證 |
| 風險曝險與分散 | 單名集中、共同空方曝險、連續不利日期會放大路徑回撤 | 半額意圖只降低回撤而失去報酬優勢；沒有偷偷加topK、單股限額、強制多空平衡、benchmark混合或風險loss |
| EMA／SWA與多模型平均 | 參數或預測平均可能平滑不穩定更新，也可能跨越稀疏及整張門檻 | seed集成協議已取消；EMA／SWA未實測。不能平均NAV當策略，也不能將不同尺度的raw logits直接相加 |
| 帳戶年界、更新頻率與梯度截斷 | 年度reset改資金路徑；per-batch更新改策略在歷史中的參數；detach限制跨段credit assignment | 既有守則保留；連續帳戶與calendar研究的容量／年界拒絕已記錄，未把中止研究列為有效改善，也沒有宣稱目前是跨11年完整BPTT |
| CUDA Graph、Quack與resident projection | 提高相同運算的吞吐，讓更多受控研究可負擔；本身不創造預測資訊 | 保留實際Graph replay與完整fold保存流程。速度與策略品質分別量測，未把高GPU utilization當成較高Sharpe的證據 |
| 來源時間與驗證獨立性 | 不正確的可得時間或反覆看test會使研究結論偏樂觀 | 既有ABI仍為research_only、historical_point_in_time=false；149日重抽樣與同test多seed均不是新增未觸碰樣本，尚不支持live或跨年代優勢 |

上述方法庫的「候選」是由本次資料推導的後續假說，不代表已被證明更好，也沒有排入無限追加的實驗。plateau機制參見[PyTorch官方scheduler文件](https://docs.pytorch.org/docs/stable/generated/torch.optim.lr_scheduler.ReduceLROnPlateau.html)；本專案共用trainer已使用`threshold_mode="abs"`，正式啟用仍須採獨立設定與產物根目錄。

**可重寫的更新結構候選：在既有完整年度episode之間更新。** 現在一個完整資料pass只有一次optimizer step；年度episode各自從固定初始帳戶開始，若`ell_e`是episode的平均費後負log增長、`N_e`是有效決策日數，總目標就是`L=sum(N_e*ell_e)/sum(N_e)`。在相同參數與既有梯度截斷契約下，按日期加權的episode梯度可重建目前的累積梯度；以`N_e/sum(N_e)`抽樣獨立episode，才有對應的期望加權梯度。這與在同一個有持續帳戶狀態的episode中途改參數不同。

因此下一個結構假說是：保留目前episode邊界及所有日期，每完成一對GPU上的完整episodes才更新，再讀下一對；可能用同一資料pass產生更多有效更新。這不是已實作或已量得的倍數加速。正式測試必須新增optimizer trajectory fingerprint、拒絕舊optimizer續跑，保留每個完整pass的validation／patience100，正確處理兩rank日期數與奇數episode的collective，並在每次更新後失效**依賴模型權重的投影cache**；只有immutable來源CSR可以直接重用。漏掉cache失效會是訓練機制錯誤，不能當成快取優化。這個候選沒有混入目前已鎖定的收斂對照。

另外以相同架構在CPU strict載入實際last checkpoint，逐一對齊119個參數tensor與optimizer分組。唯一未建立optimizer state的是`symbol_position[1,1,2330,32]`的74,560元素，設定明確為`use_symbol_pos=false`，屬未使用的forward分支，並非輸入投影漏學。保留此參數以維持checkpoint ABI；沒有為了縮小參數數量改動當前模型。這項檢查也說明僅看全域梯度非零，不足以判斷每個子模組是否參與訓練。

針對全空方現象，另做2,757槽的配置器解析oracle：目前使用`w_i=p_i*z_i/(1+q)`，不是用hard sign切斷方向的舊分支。全零分數保留`1/2757`梯度，最大誤差2.92e-11；正負鏡射的forward誤差0；單一明確入選分數±2輸出±2/3，梯度符合解析值1/9。禁止放空與全遮罩也符合規則。因此此配置器沒有強制空方；模型全空方不由這個normalizer的方向鎖死解釋。這只是synthetic配置層驗證，不保證模型預測、未入選股票的探索或整張代理梯度都已解決。證據：`OP/scale-separated-direction-oracle.json`。

**精確帳務與梯度的邊界必須分開。** 整體鏈條為「當時可用的輸入→分數→稀疏配置意圖→政策bridge→整張／容量／資金限制→費後NAV→log loss→既有代理梯度與截斷→optimizer→validation選模」。目前`_ste_floor_lots(x)`的forward是`1000*floor(max(x,0)/1000)`，backward則保留對輸入斜率1。用凍結來源實測250、750、1250、1750、2250股，forward分別為0、0、1000、1000、2000股；autograd斜率均為1，±1股局部差分斜率均為0，符合既有明示契約。完整ledger後面另有方向、成交與遮罩分支，因此這不是所有小額請求都有有效學習訊號的證明。

離散交易目標在固定成交區間通常為平坦、在門檻處不連續；不能同時要求精確整數forward及處處有效的傳統導數。Graph/eager的VJP比對只能證明加速保留這份梯度程式，不能證明它是整數策略收益的真實導數或最佳credit assignment。這次沒有更換此規則，也沒有把有限非零梯度當作未來績效保證。證據：`OP/existing-lot-gradient-contract-oracle.json`；梯度截斷與新增探索代理的不同另見上表及歷史拒絕研究。

目前選模時的online validation loss與最終best-checkpoint replay的年化log值有小幅數值殘差：baseline約8.23e-4、Muon約3.26e-5，尚未逐算子歸因。checkpoint選定epoch正確、固定保存意圖的帳務可獨立精確重現，但這兩項不等於已證明不同batch／compiled執行脈絡下的模型輸出逐bit相同；不能把這個未解的數值邊界說成已找到帳務錯誤。

### 自動完成、回收與後續檢視

新兩組由遠端canonical runner依原規則跑完，監督driver會保存成功或拒絕結果、CPU finite檢查與完整archive。來源、wheel、協議與driver的13個檔案已先回傳核對；新來源解壓後1,687檔SHA通過。證據：`OP/local-quality-convergence-v2-protocol-preservation.json`、`OP/local-quality-convergence-v2-source-acceptance.json`。

本機另啟動唯讀回收run `collect-quality-convergence-v2-20261015T132531-d5e71724`；它只等待這個具名訓練run，回傳產物、核對SHA、載入CPU checkpoint，再以同一canonical audit產生四組比較JSON／CSV／Markdown及淨值圖，不啟動、停止、重試或部署模型。傳輸暫時timeout會記錄並繼續觀察，不把網路超時當作訓練失敗。

- 現況：`OP/quality-convergence-v2-collection-status.json`；`phase=waiting`不表示新研究已完成。
- 完成後：`OP/local-quality-convergence-v2-preservation.json`、`OP/quality-convergence-v2-comparison.{json,csv,md}`、`OP/quality-convergence-v2-figures/`。
- 原始模型與逐輪資料：`OP/quality-convergence-v2-retained/artifacts/markets/tw_pure_log_mechanisms_convergence_20261015_v2/`。
- 每組完整lifecycle、best/last選定與patience驗收：`OP/quality-convergence-v2-convergence-audit.json`。

任何組別被拒絕會保存拒絕證據，不冒充已完成四組比較。回收程序有12小時觀察期限；主機重開、來源拒絕或實驗中斷仍須沿原checkpoint／來源契約接手，不以自動續跑掩蓋。原35組年度消融保持停止。

## 先前 64 輪初篩結果，尚非收斂後比較

以下為fold11 / seed42 / 64次更新的validation-selected產物，使用同149日及1,000萬起點；七組主研究已結束，包含容量與年界拒絕結果。MDD為日收盤最大跌幅。此處不能判定各方法收斂後的品質。

| 方法 | 選定 epoch | 費後報酬 | Sharpe | Sortino | MDD 跌幅 | 完整 fold 秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| control | 63 | 8.33% | 0.450 | 0.664 | 13.64% | 642.55 |
| 時間層FP32 | 61 | 23.35% | 1.203 | 1.880 | 13.79% | 862.21 |
| Muon，矩陣lr scale2 | 64 | 27.02% | 1.367 | 2.088 | 11.89% | 640.89 |
| 2330 benchmark | — | 25.18% | 1.097 | 1.755 | 12.37% | — |

FP32相對control的報酬增加15.03個百分點、Sharpe增加0.753、Sortino增加1.216，MDD則多0.15個百分點，steady epoch約10.10秒對7.17秒。沒有全面勝過benchmark。2,000次配對10日區塊重抽樣的Sharpe差異95%區間為[-0.361,1.922]；這是固定報酬tape的條件不確定性，並未估計訓練seed變異或未來勝率。兩組所有保存指標均由canonical函式重算，無零梯度批次、破產epoch或第二輪之後的新圖編譯。

Muon同預算相對control報酬增加18.69個百分點、Sharpe增加0.917、Sortino增加1.425，MDD少1.75個百分點；steady epoch7.15秒、完整fold640.89秒，與control成本相當。本次四項均優於benchmark，但仍是單次回顧結果。Muon只改矩陣更新幾何，沒有加入新loss；不把FP32與Muon的各自改善假設成可直接相加。

Muon相對control的條件配對重抽樣Sharpe差異95%區間為[-0.028,1.861]，仍含0。Muon與FP32兩個新策略另各作完整149日、270分鐘CPU canonical帳務重播；日報酬最大誤差皆小於`2.3e-16`、分鐘淨值小於`3.8e-9`，整張持倉與違約欄位完全相同。

證據：`OP/remote/study-simple-log-v7-progress.json`、`OP/local-v7-matched_control_64-preservation.json`、`OP/local-v7-historical_temporal_fp32-preservation.json`。各74檔、兩個checkpoint及一份model已離機核對並CPU finite載入。

Muon保全與重播：`OP/local-v7-muon_lr2_64-preservation.json`、`OP/matched-account-muon-v7.json`；FP32重播：`OP/matched-account-temporal-fp32-v7.json`。

較早fold10的final-test依現行設定延伸至資料尾端，共392日，包含fold11的149日，因此不是獨立測試樣本。兩組fold10 / seed73可在同392日、相同起點下比較optimizer；跨fold另用共同149日、1,000萬重播固定動作，並明示其訓練資料與驗證市場不同。不得將兩個fold視為兩份獨立持出證據。

較早fold10 / seed73的64輪結果如下。每組checkpoint仍由validation選定；不是從test選最高epoch。

| 方法 | 選定 epoch | 費後報酬 | Sharpe | Sortino | MDD 跌幅 | 完整 fold 秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| control | 60 | 7.89% | 0.240 | 0.333 | 20.20% | 976.08 |
| Muon，矩陣lr scale2 | 59 | 37.77% | 1.092 | 1.578 | 17.62% | 810.02 |
| 2330 benchmark | — | 143.00% | 1.721 | 2.715 | 25.60% | — |

Muon相對control改善29.88個百分點報酬、0.851 Sharpe、1.244 Sortino，MDD少2.57個百分點；仍落後該段benchmark的報酬與風險調整報酬。配對10日區塊重抽樣Sharpe差異95%區間為[-0.001,1.771]，仍包含0。此處fold與seed同時不同於fold11，不能分離seed變異與市場切分效應。steady epoch分別6.35 / 6.64秒；完整fold時間包括下面發現需修正的CPU部署前綴重播，不把完整fold差異解釋為Muon的GPU加速。

證據：`OP/remote/study-simple-log-v7-audit.json`、`OP/local-v7-fold10_seed73_control-preservation.json`、`OP/local-v7-fold10_seed73_muon_lr2-preservation.json`。

### 真正發現的報告重播機制錯誤

fold10的部署前綴短於完整test，會另作CPU帳務重播；fold11的前綴等於完整test，直接沿用已記錄endpoint。獨立比對發現，舊前綴與完整test的前243日雖有完全相同的原始請求，日報酬卻不同，首次出現在2025-05-15，最大單日log return差0.002453、分鐘淨值差44,371.97元。差異不是只發生在末日，也不是單純截取終點的效果。

根因是`_replay_physical_carry_split_prefix`及`_replay_physical_carry_panel_segment`直接呼叫裸帳務核心，漏過正式訓練／評估使用的canonical bridge。保存的是模型原始意圖；重播必須再次套用強制退出、未解公司行動及強制回補等政策限制。合成回歸案例在修正前重現三種政策下的錯誤，修正後前綴與跨段帳戶重播共用同一bridge，且仍保存原始意圖。報告契約新增`physical_policy_replay_version=2`及`saved_requests_contract=raw_model_intents_before_causal_bridge`。

這項修正不改loss、訓練forward或optimizer；原始checkpoint與錯誤報告保留，另建修復產物。它影響部署前綴與串接報告，不能用來解釋已獨立重現的fold11完整149日策略成效。完整流程計時仍必須包含正確的前綴重播。

證據：`OP/fold10-control-prefix-consistency.json`、`OP/fold10-control-prefix-first-divergence.json`、`OP/prefix-policy-regression-before.txt`、`OP/prefix-policy-regression-after-v2.txt`。修正前3失敗、2通過；最後相關回歸組 **140 passed、3 skipped**，涵蓋政策、跨段 recurrent state、舊報告拒絕重用、inference 不改 checkpoint 及完成標記等契約。

真實來源重播亦已完成：control 與 Muon 各驗證全部243日前綴，並以128+115日兩段傳遞原帳戶狀態，再比對完整392日回測的同一前綴。兩組最大日 log-return 誤差均小於 `2.3e-16`，分鐘 NAV 小於 `3.8e-9` 元；原始意圖、整張持倉、權重與違約欄位完全相同。第一個舊錯誤日的非零動作包含6841的負請求，當日 `force_short_cover_mask=true`。舊三份模型／checkpoint 的 SHA 各自保持不變，修復報告在新目錄通過 canonical lifecycle 驗收。

修復產物：`artifacts/benchmarks/training_quality_20261015/simple-log-v7-report-repair-v2/`。驗收收據：`OP/remote/repair-acceptance.json`；測試：`OP/pure-log-and-policy-replay-tests-accepted.txt`。此為報告正確性修復，沒有新增 optimizer update 或策略品質提升。

## 已確認的現況

- 原 run `annual-ablations-v12-20261014T201617-7a1f1705` 已取消；停止收據確認沒有殘留的所屬訓練程序及 CUDA 計算程序。
- 352 個原結果檔案已回傳本機並逐一核對 SHA；29 個 checkpoint 可用 CPU 載入。原始模型與其 optimizer 狀態保留。
- 另回傳研究第一階段 439 個檔案；12 份研究 checkpoint 的模型及 optimizer 浮點張量均為有限值，CPU 可載入。原 v19 與兩版候選的 source archive、wheel 及 release receipt 也已保全，不依賴可變動的主 checkout。
- 原消融已完成 baseline、七個 optimizer 變體及 attention pooling；停止時 lookback 變體尚未完成，不能列為完成結果。
- 實際 baseline 是 **d32、lookback32、global batch128 / 每 rank64**，14,726 個特徵、2,757 個股票槽。歷史對話中的 d128/lookback256 不能當成這次已載入設定。

證據：`OP/remote/stop-acceptance-and-inventory.json`、
`OP/local-preservation-acceptance.json`、`OP/baseline-resolved.yaml`。

## 第一性原理：先分開五個問題

1. **資訊是否有預測力？** 正確的時間切分、特徵可得時間、資料來源與市場狀態決定可學到的訊號上限。增加吞吐量不能創造資訊。
2. **動作如何變成淨損益？** 開倉權限、強制退出、整張交易、成交容量、費稅與帳戶狀態決定模型能否把訊號實現。訓練、驗證及重播必須共用相同機制。
3. **梯度是否能看見改善方向？** 稀疏選股及整張門檻可使未入選或買不起一張的股票缺乏學習訊號。數學上非零但遠小於 optimizer epsilon 的梯度也沒有實際探索作用。
4. **目標函數是否對準需求？** 平均淨 log growth、Sharpe、Sortino 與整段 MDD 是不同目標。分段 Sharpe 的平均不等於全期 Sharpe；各段 MDD 的平均也不等於整段 MDD。
5. **模型選擇能否跨市場狀態？** 訓練與單一驗證年越來越好，不能保證下一年也變好。大量消融後再挑測試集最高者，會放大選擇偏誤。

同時提高四項指標，需要提高費後平均優勢、降低有害波動，並減少連續虧損的聚集。單純降低槓桿通常會同時降低報酬與回撤，不能創造更高的 Sharpe；改變 Sharpe 的計算方式也不會創造優勢。MDD 又依賴報酬的先後順序，僅壓低單日變異並不能保證整段路徑更好。因此後續在純 log-utility 下測試梯度、更新幾何與訓練切分能否增加訊號的實現能力；不保證四項指標有任意大的提升。

## 原產物的同日期比較

以下全部使用 2026-02-26 至 2026-10-02 的 149 個共同交易日、相同 canonical 指標定義。報酬為該段累積報酬；MDD 以正的跌幅百分比表示，越小越好。

| 模型 / 對照 | 報酬 | Sharpe | Sortino | MDD 跌幅 |
| --- | ---: | ---: | ---: | ---: |
| 2330 benchmark | 25.18% | 1.097 | 1.755 | 12.37% |
| 原 baseline，validation 選 epoch209 | -21.07% | -0.883 | -1.195 | 39.23% |
| 原 Muon lr2，validation 選 epoch100 | 37.57% | 1.580 | 2.402 | 11.83% |
| 原 Muon + AdEMAMix | 62.70% | 1.462 | 2.201 | 21.86% |
| 舊 2026-10-06 模型，共同日期後段 | 69.10% | 3.429 | 5.873 | 9.57% |
| 舊模型固定動作，相同起始帳戶重新執行 | 69.85% | 3.485 | 5.995 | 9.50% |

九個已完成原消融的保存指標，均可由保存日報酬使用 `compute_metrics` 重算；差異小於 `1e-8`。因此大幅落差並非單純把 Sharpe、Sortino 或 MDD 的公式算錯。

Muon lr2 在這段已觀察期間的四項指標都優於 benchmark，但這是對已完成消融的回顧，不是用測試集選模型的授權，也不是未來勝出的證據。舊模型的「共同日期後段」仍來自較早開始的帳戶。另補做固定動作重播：核對舊產物的股票順序、來源 release 與帳務 ABI 完全相同，以現行 canonical bridge、完整 270 分鐘來源及實際費率，從共同首日的 1,000 萬資金重新執行。結果為表中最後一列。

同一工具先重現原 baseline，日 log return 最大誤差小於 `1e-16`，離散帳務欄位完全相同；再重播舊動作。舊策略的明顯優勢仍在，因此帳戶起點及已測 forward 帳務不能解釋這次退步，後續優先檢驗訓練切分、梯度幾何、更新預算與選模。此為保存動作的反事實執行比較，沒有重新訓練舊模型，也不證明歷史 PIT 或未來優勢。

證據：`OP/local-quality-audit.json`、`OP/local-quality-audit-uncertainty.json`、
`OP/historical-return-surface.npz`、`OP/historical-action-provenance.json`、
`OP/matched-account-original-baseline.json`、`OP/matched-account-historical-policy.json`、
`OP/matched-account-quality-comparison.json`。舊選定模型、checkpoint、完整 test tape 與 manifest 亦另行保全，見 `OP/historical-selected-preservation.json`。

### 選股與曝險的交叉重播

固定新舊策略各自每日的帶符號相對配置，只交換同日的請求 gross；日期、1,000 萬起點、來源、整張、費率及完整 bridge 都相同。兩邊每天皆有非零配置，沒有靠填入新股票補足曝險。四組為描述性的固定動作對照，未作正式策略選擇：

| 相對配置 / 每日曝險來源 | 報酬 | Sharpe | Sortino | MDD 跌幅 |
| --- | ---: | ---: | ---: | ---: |
| 原 baseline / 原 baseline | -21.07% | -0.883 | -1.195 | 39.23% |
| 原 baseline / 舊策略 | -11.99% | -0.679 | -0.936 | 26.54% |
| 舊策略 / 舊策略 | 69.85% | 3.485 | 5.995 | 9.50% |
| 舊策略 / 原 baseline | 101.95% | 3.270 | 5.434 | 13.36% |

縮小 baseline 的曝險確實減少虧損與回撤，但仍未產生正優勢；把較大的曝險給舊相對配置，報酬增加但風險也上升。這支持優先修復訊號 / 相對配置的學習與跨期選模，而不是單靠降低下單量。整張與容量使結果非線性，此比較沒有假設損益可按倍數線性縮放。

證據：`OP/exposure-crossover-plan.json`、`OP/matched-account-baseline_choice_old_budget.json`、`OP/matched-account-old_choice_baseline_budget.json`。同帳戶舊策略的分鐘 MDD 為 11.83%，表中 9.50% 是日收盤 MDD；其相對 benchmark 的 2,000 次配對區塊重抽樣 Sharpe 差異區間仍為 [-0.93, +5.83]，不能當成未來勝出的保證。

## 已排除的帳務疑點

先前直接把保存的 `requested_weights_history` 丟進裸帳務核心，從 2026-06-18 的 2489 開始出現差異。這是**稽核少套既有政策限制**，並非證實了帳務 bug：保存的是模型意圖，正式 bridge 還會套用除權息避開、未解公司行動、強制退出與強制回補。

補齊全部 canonical 限制後：

- 遠端正式 evaluator 的 149 日重播，報酬、換手率、全部 270 分鐘淨值、請求權重、期末持倉與違約欄位，對原 baseline 的最大絕對誤差全部為 **0**。
- 獨立 CPU、未壓縮逐日帳務，以相同來源、費率及 bridge 轉譯後的動作重播：淨值最大誤差 `1.862645149230957e-9`，日 log return 最大誤差 `1.3877787807814457e-17`，離散欄位完全相同。
- 同一份未轉譯輸入下，CUDA Graph 與 eager 輸出逐項完全相同。
- 32 列來源批次、CPU/GPU 搬運及 16 列裝置預取，與保留的 8 列 CPU 來源逐欄相同。

這些證據支持已測路徑的帳務一致性，不能替代對每個新資料、動作或訓練路徑的驗證。原始被拒絕的稽核輸出保留，不能因檔名含 acceptance 就視為成功。

證據：`OP/remote/production-eval-traced-replay.json`、
`OP/fixed-action-complete-policy-eager-cpu.json`、
`OP/remote/source-transport-comparison.json`。

## 足以影響策略品質的機制

### 目標與選模

以沒有外部入金、且尚未破產的同一帳戶為例，`sum(log(Vt/Vt-1)) = log(VT/V0)`，所以固定日期數的純費後 log-utility，已直接對準最終資產成長。若 `x` 是單期算術報酬，`log(1+x) ≈ x-x²/2`；對數的曲率本身就會對大幅損失敏感。這解釋為何不加入 Sharpe / Sortino / MDD 項目，也可能得到好的風險指標，但不保證任何一項一定勝過 benchmark。

目標相同不代表訓練問題相同：年度帳戶重置會改變整張可買性與容量；切分會改變樣本、RMS 與驗證市場；截斷反向與更新頻率會改變哪些訊號能傳回參數。這些都應在簡單目標下逐項診斷。當帳戶有重置或外部資金流時，不能直接把全部日期 telescoping 成單一連續帳戶的財富。

原主目標為 `-252 * mean(net_daily_log_return)`。目前所選路徑中，設定檔的 `gamma_drawdown=0.5` 不會自動成為 log-utility 的 MDD 懲罰；不能只看這個設定值就宣稱正在直接優化回撤。

原 baseline 的驗證期報酬約 +211.90%、Sharpe3.320、Sortino5.461、MDD14.34%，但所選 checkpoint 的測試期為負報酬。已觀察曲線也顯示驗證指標改善時，測試 loss 會惡化。這支持「驗證市場狀態及選模可能不具轉移性」的假說；不能事後用測試曲線挑較早 epoch 修飾結果。

### 集中度與方向

原 baseline 的平均請求 gross 為 0.6689，平均只有 2.67 檔非零請求，單一最大請求權重達 0.7128。舊模型對應數值約為 0.4758、5.33 檔、0.5232。這些是**請求配置**，不是實際成交部位。

兩者在共同測試期間都以做空為主，不能把退步單獨歸因於「模型只做空」。較高集中度與曝險可能放大錯誤，但需要受控測試才能判定因果。未加入強制全投資、固定 top-K、禁做空或自動混入 benchmark 的隱藏限制。

### 訓練環境已改變

已驗證的歷史差異包括年度帳戶重置、calendar 改為 lookback-shifted 年界、temporal blocks FP32 改為 BF16 AMP 路徑，以及新的年度所有權 / BPTT 梯度幾何。全域資料、特徵順序與來源 fingerprint 相同，不代表每個 fold 的實際訓練及驗證列相同。

目前 shifted fold11 的驗證期為 2025-02-26 至 2026-02-25，測試自 2026-02-26 開始；不是 calendar 的整個 2025 / 2026。這是顯式切分契約，並非已發現資料重疊。歷史設定沒有列出的新 key，其當時預設值仍可能未知，不能一律推定為設定變更。

年度重置會改變帳戶 NAV、整張可買性、容量相對資本與樣本權重；BPTT 改變 credit assignment。這些屬於訓練問題的變化，不能只當成無關品質的速度開關。

實際模型約 113 萬個參數，其中輸入投影約 94 萬個，處理 14,726 個特徵。股票列彼此相關，同一天的許多股票不是同等數量的獨立市場狀態；參數更多、特徵更多或 GPU 更忙，都不能直接推導為樣本外優勢更高。特徵品質、跨年穩定性與正則化仍需另行量測，這次沒有以刪資料或縮小來源來製造結果。

本 fold 的 RMS 正規化確實只使用訓練資料，特徵日期截至 2025-02-24，`feature_lag=1`；10,984 個特徵啟用、3,742 個停用。這項訓練邊界證據，與來源本身是否具備歷史 PIT 是不同問題。

這裡的「特徵日期」是張量儲存索引：既有 `causal_state_blocks` 將決策日 t 可用的事實放於 t−1 列，再由 dataset 的 lag1 讀取。因此此次最後訓練決策日是 2025-02-25，不能直接拿原始發布事件日期與儲存列標籤比較。舊模型對應截止於儲存列 2024-12-30 / 決策日 2024-12-31。

逐特徵比對顯示啟用遮罩相同，但 224 個啟用欄位的 RMS 改變超過 5%。最大的兩項變成約 372、240 倍，來自 5864 的 2024Q4 個別財報 YTD 現金流：原始就緒日 2025-02-25，數值 −734,016,000 TWD 與 +114,883,000 TWD，恰好落在新訓練邊界的最後一列。核對 raw event 與 lossless block SHA 後，以舊平方和加這一筆資料並更新 alive-cell 分母，可得到**完全相同的兩個 FP32 新 RMS**。這個小範圍的 12 筆原始事件沒有重複鍵、NULL 或非有限值；未證實量綱或快取錯誤，也未證明這兩個欄位造成策略退步。它證明切分改變確實能大幅改變稀疏欄位尺度，不能把年界移動只當成速度設定。

證據：`OP/historical-normalizer-identity.json`、`OP/remote/normalizer-tail-source-profile-v2.json`、`OP/remote/normalizer-tail-raw-blocks.json`、`OP/normalizer-tail-explanation.json`。初次 profile 使用了儲存日期 cutoff，未納入決策當日事件；舊輸出保留，v2 才是對齊時鐘的稽核結果。此檢查僅涵蓋兩個欄位，未宣稱完整來源品質已通過。

原 baseline 每個 epoch 記錄 **1 次 optimizer 更新**，並非 24 個帳務批次各更新一次。309 個 epoch 均沒有整批零梯度或帳戶死亡；每次記錄的裁剪前梯度範數均大於 1，中央值約 25.07，裁剪門檻為 1。這不證明裁剪是退步原因。309 epochs 對應 309 次更新，96 epochs 對應 96 次更新，其中 32 次為 warmup；訓練長度差異不能當成方法改善。

### Benchmark 與資訊邊界

2330 benchmark 為相同交易日的調整後前收至今收 buy-and-hold；策略使用盤中執行及真實費稅。兩者承擔的隔夜曝險不同。維持原 comparator，另外說明這項經濟差異，不能改 benchmark 或替策略補不存在的隔夜收益。

149 日的短期結果不穩定。配對、10 日區塊、2,000 次 bootstrap 中，Muon lr2 相對 benchmark 的 Sharpe 差異 95% 區間約為 [-3.14, +4.15]。這是此段資料的條件重抽樣，沒有修正所有試驗選擇，也不是未來勝出機率。

此特徵契約仍明列 `research_only=true`、`historical_point_in_time=false`、`live_eligible=false`。訓練改進不能把研究快照變成已證明的歷史 PIT 或可直接上線的資訊。必須與工程驗收、回顧研究結果分開。

證據：`OP/historical-config-diff.json`、`OP/historical-source-identity-comparison.json`、
`OP/remote/feature-scope-audit.json`、`OP/model-capacity-audit.json`。

## 實作的研究方法

**最新範圍修訂：使用者要求維持簡單費後 log-utility。** v6 的 risk 組已中止，尚未啟動的風險組合取消；停止收據確認全部所屬程序退出。已完成 control、sub-lot、dense+sub-lot 與中斷 checkpoint 保留，未以中斷風險組作品質結論。

後續 v7 僅優化訓練機制，固定 `loss = -252 * mean(net_daily_log_return)`；三個新增風險權重皆為 0，sub-lot 與 dense backward 代理均關閉。Sharpe、Sortino、MDD 與 benchmark 只作評估，checkpoint 照 canonical validation loss 選取。保留既有整張、費稅、容量、現金及破產吸收語意。

v7 共七組、各 64 次預定更新：matched control、calendar、連續帳戶、temporal FP32、Muon lr2，以及 fold10 / seed73 的 control / Muon 對照。模型、來源、batch、warmup 及 scheduler horizon 不改；相同更新預算比較。前四項延續原機制檢查；Muon 沿用已完成研究的矩陣 lr scale2，檢查不同更新幾何是否能在相同簡單目標下改善學習。沒有為新結果調參。448 次更新是篩選，不能當作完整收斂或多 seed 穩健性。

`gamma_sharpe` 在 log-utility 路徑中是既有 loss 乘數，這裡固定 1；它的名稱不代表此路徑正在直接優化 Sharpe。風險指標設定與 benchmark 不應偷偷改變純 log-utility 的梯度，另以 focused semantic test 驗證。

協議：`OP/study-simple-log-v7-plan.json`，SHA `7f9f97ca86435e0ea14b7fd9531ed41f1523efb610362bbf0e46e8a097f6c492`。原 v6 協議不改寫；以下保留已實作但目前未選用的方法及歷史容量過程。

v7 的 64-update control 已完整完成並通過驗收：validation 選 epoch63，149 日測試報酬 +8.33%、Sharpe0.450、Sortino0.664、日 MDD13.64%；完整 fold642.55秒，steady epoch3+ 中位數7.17秒。74 檔及兩個 checkpoint / 一份 model 已離機保全、CPU finite 通過。

同來源 control96 與 control64 的共同前段，LR 與每輪一次更新皆完全相同，但第一輪 train loss 已差約 `9.8e-5`，至第64輪可達約0.084。這是保存曲線的觀察，尚未逐算子歸因；不能把不同長度控制組當作嚴格相同軌跡，也不能用小幅單次差距宣稱穩定方法優勢。證據：`OP/control64-prefix-comparison.json`。

另逐 bit 比較兩個 checkpoint 的14,726維 RMS buffer，完全相同；不能把這次兩個控制組的差異歸因於 RMS 重算。證據：`OP/control64-rms-comparison.json`。

calendar 單獨搭配年度重置被既有完整年界守則拒絕：2014 年應涵蓋 01-06 至 12-31 的246個來源交易日，但32日 lookback 使首個有效決策在02-27，僅214日。來源沒有前一年的真實因果 context；不能合成、跳過2014或放寬守則。原失敗 retained，研究其餘組別繼續。

因此另預先鎖定兩組有效的64-update補充：`calendar_continuous` 與 `calendar_continuous_fp32`。先以既有 shifted-continuous 組對照 calendar-continuous，再於相同 calendar / continuous 下比較時間層精度；後者同時恢復三項可確認的歷史設定，但不等於重現舊128-update軌跡。兩組在 v7 queue 結束後才用相同 canonical runner 執行，純 log-utility 不變。補充協議 SHA `2364525997db296d68c85f95eff3d38a3ec5d8e0f6b3030aac559c3465629987`；沒有依中途測試績效挑參數。年界改變會改訓練樣本、RMS及驗證區間，仍是聯合機制對照。

最後另鎖定兩組各4個完整資料 passes 的更新頻率探針：`continuous_minibatch4` 與 `continuous_minibatch_muon4`，排在 calendar 補充之後。沿用共用 trainer 的 chronological per-batch optimizer 路徑及 batch DDP 同步；保留連續 physical state、原 batch 邊界的 state-gradient detach、費後 log-utility、固定模型驗證及每 epoch 完整產物。原年度帳戶模式要求完整 trajectory 更新，故此探針明確關閉年度重置，沒有放寬該守則。

這兩組內部具有相同 passes 與 cadence，只改 optimizer；相對64-pass trajectory組僅能分析學習效率，**不是同資料量或同運算量的因果 A/B**。四個 passes 不等於四次更新，必須用實測 optimizer_steps 計數。訓練中的 policy 在批次間改變，不能宣稱是固定參數下完整歷史財富的精確梯度。協議 SHA `086898a66f34fe862574c4abc9737dcbc9412fa9e9895ed4dd1ea4a61b2b9880`；未打亂日期、重置現金、加入新風險項或修改成交。這輪先完成已鎖定組別，不再追加新方法。

v7 shifted-continuous雖完成64 epochs及保存流程，仍被既有主機RAM門檻拒絕：cgroup最少餘量僅0.00836GiB，要求16GiB；GPU餘量1.60059GiB、峰值94.97%則通過。沒有OOM kill證據；結束後主機用量約111GiB，其中包含共享記憶體及檔案頁。不能僅憑cgroup總量，斷言是存活張量洩漏或pinned allocator導致；拒絕的70檔、兩個checkpoint與一份model已離機SHA/CPU finite保全。

已停止尚未開始的calendar / cadence兩個等待程序，所屬程序皆退出，保留原協議。容量修訂v8僅將每rank immutable caches調成factorized host8GiB、CSR GPU14GiB、physical source4GiB、prepared batch2GiB；日期、特徵、正規化、模型、batch、梯度邊界、loss、費率與整張不變。兩組四pass完整fold smoke必須先通過原16GiB host / 1.5GiB GPU門檻，之後每組長測仍獨立驗收；不把短測當作長期容量通過。

新來源僅補上選用的host pinned allocator owned / active / peak counters，不同步或改張量生命期；不能把device `empty_cache()`當成回收host pinned cache。33項相關測試通過。來源SHA `fce2d79ef2c0c4728a41e0479db483d8f8bde7cc994394f8f72653ff00d6f9b1`，kernel runtime SHA與v7相同；保留新舊capsules，未覆寫遠端主程式。修訂協議SHA `200f0839a6cd2f4b701aea6865ff4f4cbe22d171bde7cc00bb9fcd77538b4bab`，研究五組共200 passes，另有8個容量smoke passes；假說與各組原預算保留，沒有按中途報酬改參數。

v8 兩組四 pass 完整 fold smoke 均通過容量門檻，牆鐘分別276.63秒與548.81秒；calendar-continuous-FP32 的 steady epoch 約41.28秒。其後64輪補充研究才開始第一組，便依使用者要求改做收斂長訓練而停止，不能列為完整研究成功。其所屬程序均已結束，canonical run state 為 cancelled，部分 checkpoint 原樣保留。host allocator 的 active counter 曾大於 owned 且與 peak 矛盾，只能視為計數器不一致，不能憑它診斷張量洩漏。

證據：`OP/remote/waiting-memory-protocol-stop.json`、`OP/study-simple-log-memory-v8-plan.json`、`OP/pinned-telemetry-tests.txt`、`OP/remote/simple-log-memory-v8-admission.json`、`OP/remote/simple-log-v8-convergence-stop.json`。v7、v8 smoke、v8 中斷研究及來源已共同封存為847檔，離機驗收另以 `OP/local-research-finite-preservation.json` 為準。

### 離散執行中的探索訊號

保留精確整張、稀疏 entmax 選股、費率、容量、遮罩及破產吸收狀態。訓練可選用既有 sub-lot recovery 的零 forward 差異代理，讓不足一張的候選也收到一張交易的反事實收益方向；另外新增 backward-only 的稠密配置代理，讓未進入稀疏支撐集的合法股票也能被發現。

初版使用 `softmax(shape)`，在 2,757 檔、單一突出分數的壓力樣本中，未入選股票梯度只有 `4.56e-23`。新版改為 `softmax(log1p(shape))`，同一樣本經實際 GPU 編譯後梯度約 `0.001053`，hard forward 最大差異為 0，全遮罩列梯度為 0。這是梯度可用性的機制證據，**不是策略報酬提升幅度**。

版本為 `dense_log_shape_softmax_adjoint_sparse_exact_forward_v2`，只在 training 啟用，要求同時開啟 physical sub-lot recovery。v1 的 optimizer checkpoint 不可接續新版梯度契約。

### 可按日期精確聚合的風險偏好（目前關閉）

新增選用目標，令 `r` 為實際費後日 log return，`b` 為當日 benchmark log return：

```text
u(r,b) = r - 0.5 * [λr*r² + λd*min(r,0)² + λb*min(r-b,0)²]
loss = -252 * mean(u)
```

本輪預先鎖定 `(λr, λd, λb)=(10,20,15)`。這分別抑制大幅波動、負報酬與落後 benchmark 的日期，能在現有 date-weighted DDP 下保留精確的整體平均與梯度。它是二階矩 / 下偏矩偏好，並非直接的全期 Sharpe、Sortino 或 MDD 公式，也不保證未來回撤上限。

預設三個權重皆為 0，維持舊行為及舊 checkpoint 相容契約。啟用風險偏好或探索代理即進入新 fingerprint；benchmark shortfall 使 benchmark 成為訓練敏感輸入，須納入資料身分。

共用實作：`stockagent/training/log_growth_risk.py`、`loss.py`、`trainer.py`、
`stockagent/models/normalization.py`、`checkpoint_contract.py`。未複製另一套成交或回測器。

## 舊協議與容量恢復紀錄（已由上述純 log-utility 協議取代）

研究主組固定相同 seed42、來源、d32/lookback32、batch128、1000-step cosine horizon、32-step warmup；各跑 96 epochs：control、sub-lot、dense+sub-lot、risk、dense+sub-lot+risk。checkpoint 僅由各組 canonical validation 選取，不能比較不同目標的 loss 數值來宣稱品質優劣。

三個歷史機制 OFAT 各 64 epochs：calendar、連續帳戶、temporal FP32。另以 fold10 / seed73 對照 control 與組合，各 64 epochs。歷史機制組須與 64-epoch control 比較正式指標；control96 的同預算 curve prefix 僅能比較已保存的 loss。十組總計 800 epochs，屬研究篩選，不等於 1000-epoch 收斂或多年度部署驗收。2026 測試期已看過，所有新結論均是回顧研究。

在歷史機制組出結果前，另鎖定一組 fold11 / seed42 / 64-epoch control，排在十組研究之後。原因是 epoch curve 僅保留 loss，沒有每個 epoch 的完整報酬 tape；96-epoch control 的 prefix 不能還原當時的 validation-selected 帳戶。新增 64 epochs 只補足公平 comparator，原十組假說與設定不變。總研究預算因此為 864 epochs，協議為 `OP/study-matched-control64-v4-plan.json`。

v2 的 control96 曾完成，選 epoch96，測試報酬 +30.22%、Sharpe1.325、Sortino2.068、MDD11.50%；因 VRAM / host RAM headroom 未達驗收門檻，被拒絕，不能當成新方法已改善的結果。之後 boot ID 改變，sub-lot 組中斷；沒有證據判定重開機原因。原 run 收據與中斷 checkpoint 保留。

容量恢復僅統一限制 immutable caches：factorized host24GiB、NN CSR resident16GiB、physical source16GiB、prepared batch8GiB、device prefetch16列。模型、資料範圍與 batch 保留。v3 為舊梯度代理的容量測試，v4 綁定全尺寸驗證過的新版代理；各版獨立輸出，避免混用結果。

v4 兩組各四 epoch 的完整 fold 曾通過，組合與 control 的 complete-fold wall time 分別為 213.24 秒及 173.77 秒；最少 VRAM 餘量分別為 4.99GiB、4.85GiB，主機記憶體餘量均超過 105GiB。但 **v4 control96 完成後未通過長時間容量驗收**：顯存最高 98.41%、最少餘量 0.505GiB。完整 fold 為 908.01 秒；測試報酬 +20.10%、Sharpe0.886、Sortino1.332、MDD11.34%，僅作容量拒絕產物的描述。其餘 v4 研究已停止，32 個所屬程序均已結束。不能把四個 epoch 的通過當成整輪已安全。

276 個 v4 及來源 v3 檔案已回傳並核對 SHA；6 個 checkpoint 與 3 份獨立 model 檔均可在 CPU 讀取，模型及 optimizer 張量有限。v4 保留來源 SHA `dd4da6487b702c5f76d21f853eeffbbbd8313b3b917b5b8b3689287c01870bf4` 的完整產物，不覆寫為後續版本。

v5 將 CSR resident cache 降到 12GiB。完整 control16 的容量通過，最少 VRAM 餘量 9.31GiB、主機餘量 106.50GiB，但 steady epoch 約 25 秒，complete-fold 549.73 秒。量測到存活張量峰值 17.95GiB、保留空間峰值約 21.4GiB。較小 cache 有明顯吞吐代價；已停止未完成的組合容量測試及尚在等候的 v5 研究，沒有執行 v5 control96。

v6 恢復 16GiB immutable CSR cache，新增選用的 `STOCKAGENT_CUDA_EPOCH_CACHE_TRIM_FRACTION=0.7`。只在 epoch 邊界、reserved 超過比例且至少有 512MiB 未使用 cache 時呼叫 `torch.cuda.empty_cache()`；來源張量、autograd 張量及 live Graph 保持引用。預設為 0，不變更既有行為。回收成本計入 epoch / full-fold wall time。此設計依循 [PyTorch allocator 與 Graph 記憶體契約](https://docs.pytorch.org/docs/stable/notes/cuda#graph-memory-management)，仍須以實際硬體實測，不能假設能回收 live Graph 的 private pool。

36 項相關測試通過；實際 RTX5070Ti Graph / adjoint probe 回收 780MiB、約 29ms，輸入 / 輸出 / 梯度位址不變，forward 與梯度誤差均為 0。這只是 allocator 機制測試，不能當成遠端吞吐或策略提升。

v6 來源 SHA 為 `022d91cde31f9aad74e80459db5b84cead57b1aacd3a369d62fb6e128440fe92`；新來源僅增加選用的 allocator 量測及 epoch 邊界回收，帳務及品質假說不變。第一個完整 control96 同時作容量 / 速度 admission 與正式研究 comparator；只有其保存、記憶體門檻、steady median ≤12秒及 complete-fold ≤1500秒全部通過，才執行其餘十組。總品質研究預算仍為 864 epochs。

v6 完整 control96 已通過：complete-fold **912.93 秒**、epoch3+ 最大 rank 中位數 **7.22 秒**、最高顯存占用 **76.85%**、最少 VRAM 餘量 **7.37GiB**、最少主機餘量 **105.55GiB**。其測試報酬 +22.40%、Sharpe1.026、Sortino1.571、MDD12.19%，不是全面優於 benchmark。舊 v2 / v4 / v6 控制組的數值軌跡不同，因此不能把小幅單次差異當成穩定品質提升；本輪仍須與相同來源和設定的當輪 comparator 比較。

目前結果以 `OP/study-plan-v6.json`、`OP/study-full96-admission-v6.json` 及
`artifacts/benchmarks/training_quality_20261015/study-v6-control-admission/`、
`artifacts/benchmarks/training_quality_20261015/study-v6/` 下的完整 lifecycle、曲線、保存 checkpoint 與 queue-exit 為準。**尚未完成的研究組不可稱為完成，也尚未證明跨世代的策略提升。**

目前完成的同預算組如下；均為 seed42 / fold11 / 96 次更新，僅按 validation 選取 checkpoint。此表尚非全輪結論：

| 方法 | 選定 epoch | 報酬 | Sharpe | Sortino | MDD 跌幅 | 完整 fold 秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| control | 87 | 22.40% | 1.026 | 1.571 | 12.19% | 912.93 |
| sub-lot | 96 | 4.25% | 0.212 | 0.303 | 24.47% | 1,037.68 |
| dense + sub-lot | 88 | 3.56% | 1.077 | 1.775 | 2.47% | 1,038.72 |

三組全部保存指標的 canonical 重算誤差皆為 0；各 74 檔、兩個 checkpoint 與一份獨立 model 已離機保全並通過 CPU finite 檢查。sub-lot 的探索訊號在此預算中並未改善測試品質；dense 組低回撤伴隨低報酬，尚須區分請求與實際成交。不能把機制測試通過當成投資優勢。其餘 v6 組已因使用者修訂研究方向而停止，後續以新鎖定的純 log-utility 協議為準。

另以完整 canonical bridge 及逐日開倉函式插入唯讀 trace，重播兩組全部 149 日；日 log return 最大誤差小於 `2.3e-16`、分鐘 NAV 小於 `3.8e-9`，離散欄位完全相同。每日收盤持股為零，不能拿 closing weights 當成盤中曝險。實際開倉的平均 gross 如下：

| 開倉階段 | control | dense + sub-lot |
| --- | ---: | ---: |
| 原始請求 | 52.18% | 62.34% |
| bridge 限制後 | 51.77% | 61.39% |
| 符合當日進場權限及有效開盤價 | 50.10% | 57.44% |
| 整張取整後目標 | 48.51% | 15.27% |
| 實際進場，以官方開盤價估值 | 47.65% | 14.94% |

dense 組的可進場請求有約 73.4% 金額被整張取整消除，平均每天約 106.49 檔不足一張；實際成交約 21.36 檔。control 對應不足一張約 0.42 檔、成交約 2.03 檔。這證明請求更分散會在本帳戶量級造成大量閒置資金；不能把低 MDD 全部解讀成更好的訊號。後段價量 / 資金限制另造成 gross 約 0.33 與 0.85 個百分點損失，尚未逐項歸因。這項診斷沒有改權重、資金、整張規則或成交。

證據：`OP/entry-trace-control-v6.json`、`OP/entry-trace-dense-v6.json`，及其 `-entries.npz`。v6 全部 278 檔已再作停止後保全，8 個 checkpoint 與 3 份 model 通過離機 SHA / CPU finite 檢查；風險組僅到 epoch28，屬中斷產物。

## 已完成工程驗證

- 舊版相關核心 / resume / DDP 測試組：200 passed、7 skipped。
- 舊版正規化 / 帳務 / Graph 相關組：260 passed、13 skipped；與前組有重疊，不相加為唯一測試數。
- 新風險與 full-width 探索代理：20 passed。
- 新版相關正規化及年度設定回歸組：115 passed；包含上述品質測試，不重複相加。
- allocator 量測 / 邊界回收及相關品質、launcher 組：36 passed；亦與前述組別有重疊。
- 最新政策重播、artifact、inference、選模、lifecycle、純log與DDP計時組：140 passed、3 skipped；跳過項目與遠端實際雙GPU訓練證據分開記錄。
- 實際 CUDA 編譯：2,757 檔 hard forward 精確相同，未入選訊號有限且足夠大，全遮罩梯度為 0。
- 已完成雙 RTX5090 的初版四 epoch 完整 fold smoke；新版容量與研究驗收另列，不把 CPU 單元測試當作 GPU 全流程成功。

後續完成時應在此補上同預算、validation-selected 的實測比較及限制。若新方法沒有穩定優於 matched control，保留拒絕證據；不得以改動 benchmark、縮短報告範圍或挑測試集 epoch 製造勝出。

2026-10-18 新增完整視窗的 Ridge、LightGBM regression／ranker、XGBoost、CatBoost
對照，沿用 v9 baseline 的來源、帳戶、配置、2330 buy-and-hold 與產物契約。
實作、完整 fold CPU／GPU 測速、容量限制及正式排程位置見
[全視窗傳統模型基準](tw_classical_baselines_2026-10-18.md)。工程短測不列為收斂策略結果。
