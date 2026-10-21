# 從零策略：弱擾動與方向混合實驗，2026-10-20

本報告接續[參數鄰域研究](training_sharpness_2026-10-20.md)與
[梯度幾何研究](training_gradient_frontier_2026-10-20.md)。本次即時查核已完成51組正式
收斂結果彙整、前輪失敗保全及環境修復，並將11組獨立從零訓練排入既有遠端排程。
工程驗證、正式收斂、策略品質與歷史資訊可得性分別驗收。

## 51組正式結果與本輪新增成果

資料：`artifacts/operations/training-quality-audit-20261015/neural-experiment-results-20261020-midday.{json,csv}`。
最新三組的完整產物已於11:21:34臺北時間回存；460個檔案、12份checkpoint與
6份獨立model通過逐檔SHA及CPU有限值讀取驗證。保全收據為
`local-scratch-sharpness-v2-preservation.json`，封存SHA為
`60898496b5a0ae9a78d0fa7842d188c2175ace9dc6ef5df9543256a7b3969449`。

以下是2026-01-02至10-02共181個交易日，同起始帳戶、策略扣費後的結果。
MDD用正回撤幅度表示。2330是adjusted-close含息買入持有、未扣成本的參考。

| 策略／Strategy | 報酬 | Sharpe | Sortino | MDD | 最佳／停止epoch |
| --- | ---: | ---: | ---: | ---: | ---: |
| 原參數空間單位MGDA／Original parameter-space unit MGDA | 153.13% | **5.933** | 17.091 | 6.83% | 128／228 |
| 前輪單一對數效用／Previous single log utility | **377.03%** | 4.993 | 9.293 | 13.48% | 83／183 |
| 單步方向議價／One-step DiBS | 79.15% | 3.781 | **19.914** | 3.34% | 99／199 |
| 最新MGDA控制／Latest unit MGDA control | 143.60% | 4.513 | 11.089 | 6.93% | 153／253 |
| 最新對數效用控制／Latest single log utility control | 156.57% | 3.546 | 5.782 | 18.09% | 50／150 |
| 鄰域穩健對數效用／SAM log utility, radius0.01 | 49.36% | 0.894 | 1.218 | 27.21% | 201／301 |
| 指定舊fold11／Specified historical fold11 | 95.93% | 3.662 | 6.353 | 9.57% | 歷史產物 |
| 台積電買入持有／2330 buy and hold | 62.73% | 2.014 | 3.359 | 13.04% | 基準 |

原MGDA仍為51組中的最高Sharpe；報酬與Sortino的最佳候選不同。沒有新候選同時在
完整181日及共同149日的報酬、Sharpe、Sortino、MDD全面超越原MGDA。

| 共同149日，同起始帳戶 | 報酬 | Sharpe | Sortino | MDD |
| --- | ---: | ---: | ---: | ---: |
| 原MGDA／Original MGDA | 125.76% | 7.052 | 22.290 | 3.03% |
| 前輪對數效用／Previous log utility | 225.12% | 4.371 | 7.890 | 14.74% |
| 最新MGDA控制／Latest MGDA control | 86.03% | 4.123 | 9.117 | 7.06% |
| 最新對數效用控制／Latest log utility control | 93.01% | 3.081 | 4.953 | 18.91% |
| SAM對數效用／SAM log utility | 54.01% | 1.165 | 1.619 | 27.12% |

原MGDA、不同時間的控制組及不同程式版本不構成嚴格的單一變因對照。
本次發現環境套件版本漂移，因此不能把績效差異全部歸因於梯度演算法，也不能僅由
同seed宣稱結果必定一致。本輪再跑兩個控制組，不更換seed搜尋較好的策略。

## 第一性原理：先確認實際改變了什麼

1. 報酬、風險指標都來自同一條扣費帳戶路徑。改loss與改更新方向會改變資金遞迴、
   整張門檻及選股；影像分類上的較平坦鄰域不自動帶來較好的金融績效。
2. 已完成SAM的301個epoch中，擾動點loss有301次高於原點；中位差0.077667，
   原擾動與新梯度方向的cosine中位數0.868554，沒有負cosine紀錄。這些證據支持
   擾動確實執行且沒有觀察到方向翻轉；它們不是整個訓練完全正確的充分證明。
3. 半徑0.01的SAM正式結果較差，應先區分「鄰域壓力太強」與「使用偏移點方向
   丟失原本有效訊號」。這兩項仍是假設；小半徑及固定α混合能分別檢驗。
4. 保留單一log loss可減少變因。另以SAM＋unit MGDA檢查鄰域穩健性是否能補足
   當下一階多目標合成的不足，不先增加四個固定權重或新的風險懲罰。
5. 同一次更新需兩次完整年度模型／帳務／反傳計算；固定XSAMα只增加小型方向運算，
   不照搬作者21次額外forward的搜尋。這避免把昂貴年度路徑當成廉價分類batch。

診斷原始數值與假設存於`sharpness-recovery-v3/diagnosis.json`。
本輪未改訓練數學程式；1693個封存來源檔與前輪逐檔SHA完全一致。

## 原始研究：從起點到2026前沿

本次按問題脈絡查原始論文與作者程式，並非聲稱涵蓋所有文獻。早期Kelly、MGDA、
PCGrad及後續CAGrad／Nash／FAMO／UPGrad／ConFIG／DiBS的沿革與已測結果見
[梯度研究](training_gradient_frontier_2026-10-20.md)。本次另核對下列研究：

| 論文與原始來源 | 本案含義及目前狀態 |
| --- | --- |
| [SAM，2020／ICLR2021](https://arxiv.org/abs/2010.01412) | 原點梯度決定擾動，再於擾動點求更新梯度。半徑0.01已完整收斂但品質較差；本輪測0.001、0.003，另補跑原先未開始的0.05。 |
| [ASAM，ICML2021](https://proceedings.mlr.press/v139/kwon21b.html) | 參數尺度會改變鄰域；本案明訂全座標`abs(theta)+0.01`尺度，不把ASAM的半徑直接當成SAM的歐氏半徑比較。本輪補跑原先環境拒絕組。 |
| [GSAM，ICLR2022](https://research.google/pubs/surrogate-gap-minimization-improves-sharpness-aware-training/)及[官方JAX程式](https://raw.githubusercontent.com/google-research/big_vision/main/big_vision/trainers/proj/gsam/gsam.py) | 原論文更新包含對擾動梯度正交之原點梯度分量的上升步，目的是減少surrogate gap；官方亦有保留原點下降為主的變體。它不等同一般方向平均，未實作、未列入本輪。 |
| [µP²，NeurIPS2024](https://proceedings.neurips.cc/paper_files/paper/2024/hash/449a016a6ce6fba3fe50d05482abf836-Abstract-Conference.html) | 全域擾動可能在寬網路集中於最後層；逐層尺度是可測的後續方向。不能直接斷言本案d32存在同一根因，亦不能把ASAM叫成µP²。尚未實作。 |
| [LETS，2024](https://arxiv.org/abs/2408.08222) | 用雙層問題學擾動半徑，說明半徑是重要變因。本輪用預先宣告的固定半徑；若用驗證梯度學半徑，將改變2025僅選模的契約，故未排入。 |
| [XSAM，ICLR2026](https://proceedings.iclr.cc/paper_files/paper/2026/hash/f2caa9c7caafa2375e643c8a98e8c58c-Abstract-Conference.html)及[作者程式](https://raw.githubusercontent.com/Cccjl219/XSAM/main/utils/xsam.py) | 實際擾動單位方向與偏移點梯度單位方向做SLERP，再乘回梯度範數。本輪α0.5是保留較多原方向的固定插值；α1為SAM，α1.5是原已排的外插。沒有宣稱複製作者完整動態搜尋。 |
| [EISAM，2026-07預印本全文](https://arxiv.org/html/2607.06151v1) | 先到預測點`y=w-s*g`，再用偏移點梯度更新`y`。實用近似重用原點梯度，計算兩個梯度；若另精算預測點梯度才增加一遍。不能套進恢復原參數的SAM鉤子而宣稱同一方法；正文Eq5與Algorithm1的擾動表達仍需作者程式釐清。尚未實作。 |
| [MOON，2026-08預印本](https://arxiv.org/abs/2608.11749)及[作者程式庫](https://github.com/KunlinLyu/MOON) | 以矩陣spectral／nuclear幾何處理多目標並正交化。原始核範數子問題及實用logit／momentum更新不同，不能把歐氏MGDA後接Muon當成同一演算法。需矩陣／向量角色、狀態及checkpoint新契約；列為下一階段候選，尚未實作。 |

MOON與EISAM目前是預印本，分類與多任務基準上的結果不能直接視為本案回測收益。
上述論文的光滑／精確梯度理論也不自動適用本案整張代理梯度。

## 已排11組：順序與可檢驗假設

設定：[tw_scratch_sharpness_20261020_v3.yaml](../configs/ablations/tw_scratch_sharpness_20261020_v3.yaml)。
所有case加上`sharpness_recovery__`前綴。

| 順序 | Case | 方法／Strategy | 檢驗重點 |
| --- | --- | --- | --- |
| 1 | unit_mgda_control | 單位MGDA控制／Unit MGDA control | 新環境下重建當前參考。 |
| 2 | simple_log_control | 單一對數效用／Single log utility | 不加穩健擾動的成長方向。 |
| 3 | xsam_log_r3e3_a05 | 小鄰域方向插值／XSAM radius0.003, alpha0.5 | 同時減輕擾動並保留原方向。 |
| 4 | sam_log_r1e3 | 微擾動SAM／SAM radius0.001 | 與已完成0.01相比，擾動減為十分之一。 |
| 5 | sam_log_r3e3 | 小擾動SAM／SAM radius0.003 | 與第3組隔離方向插值效果。 |
| 6 | xsam_log_r1e2_a05 | 同半徑方向插值／XSAM radius0.01, alpha0.5 | 與已完成SAM0.01隔離方向處理差異。 |
| 7 | recovered_sam_mgda_r001 | MGDA鄰域穩健／SAM with unit MGDA radius0.01 | 補跑前輪在訓練前遭拒的組別。 |
| 8 | recovered_asam_log_r005 | 尺度適應鄰域／ASAM log radius0.05 | 同上；不同鄰域尺度契約。 |
| 9 | recovered_sam_log_r005 | 大鄰域對數效用／SAM log radius0.05 | 同上；較強正則化對照。 |
| 10 | recovered_xsam_log_r005_a15 | 大鄰域方向外插／XSAM radius0.05, alpha1.5 | 同上；不是α0.5的插值。 |
| 11 | recovered_sam_mgda_r005 | 大鄰域MGDA／SAM with unit MGDA radius0.05 | 同上；多目標方向下的半徑對照。 |

每組獨立隨機初始化、全部參數可訓練，沒有舊模型、教師、權重或optimizer續接。
保留最新已選配方：d32、lookback32、global batch128、seed42、SGD lr0.1、
momentum0／weight decay0；2015–2024訓練，2025純net log-utility選模，2026報告。
epochs上限1000、無改善比例0.1，因此連續100回合無改善才停止；fold10。
MGDA四項為Sharpe、Log Return、Sortino、Calmar，MDD繼續報告。

每組先跑完整4個epoch及全部產物工程驗收，再獨立從零正式訓練。工程通過不沿用
其權重。僅確認CUDA OOM時允許global batch128→64→32；其他錯誤留證跳過，繼續
下一組。完成後自動逐檔回存，再依181日與149日兩範圍比較四個已固定參考策略。

## 遠端修復、排程與驗證

前輪v2於11:18:42臺北時間結束，3組正式收斂，另5組全部在訓練前因環境檢查拒絕。
它們沒有正式品質結果，不能解讀成SAM／ASAM／XSAM五個方法失敗。
錯誤是`runtime distribution metadata observation is incomplete`。

舊`certifi-2026.7.22.dist-info`只剩`licenses/LICENSE`989 bytes、沒有METADATA，
現行certifi程式與有效metadata是2026.10.18。先留下逐檔SHA，再整目錄移至
`sharpness-recovery-v3/quarantined-runtime-metadata/`，保留可逆性；未動現行套件程式。
亦觀察到cachetools、cuda-core、multidict、numba-cuda、pillow、platformdirs等
外部版本變更。這次建立新runtime指紋，不宣稱還原成原環境或端到端位元一致。

來源／環境：

- 訓練來源SHA：`b9869f415c24e9555a0b466e7b4ff77960fafe7a0627c5b5301eec534b121e88`。
- 新runtime SHA：`aaaf8db7800cd6313828d02daf2378b63d1b2586e07dc809d7feb841c30bede5`。
- 新release：`artifacts/code_releases/training_quality_20261015/20261020T035053513477Z-b9869f415c24/release.json`。
- 本機完整來源／wheel保全：`sharpness-recovery-v3/sealed-source/`及`source-off-node-preservation.json`。

嚴格CUDA allocation／kernel／synchronize已通過；封存來源實測sharpness 26 passed、
多目標62 passed、工程計時62 passed，共150項，另12項有範圍理由的deselected。
包含真實兩rank NCCL、獨立二次式、原參數恢復、兩次反傳與單次更新等檢查。
本機另針對α0.5新增獨立二次式參數案例，13 passed、14 deselected。

第一個v3啟動於11:54前在完整模型訓練前失敗：封存source bundle不含configs，
相對base_config找不到。原失敗run與日誌保留，改從已凍結worktree解析成單一基底，
綁定絕對路徑；11組每個有效設定欄位都與封存前probe一致，只允許輸出路徑不同。
新計畫不覆寫舊計畫：

- 正式綁定：`scratch-sharpness-v3-plan-admission2.json`。
- 計畫SHA：`1ac027b1b4000423d613f61d4a2dd6357aa7e1b700626db8759ee62348b1e7e9`。
- 設定修復證據：`sharpness-recovery-v3-admission2/config-portability-proof.json`。
- 遠端run：`scratch-sharpness-v3-admission2-20261020T035619-41abcb21`，11:56:19啟動。
- 本機watch／collect／reference監督均由既有agent workflow的systemd維護，綁定此run及修訂計畫。

12:02:26臺北時間確認：新α0.5、小半徑XSAM的完整4輪機制驗證已通過，
主佇列已接續第一組unit MGDA的工程驗證，11組正式收斂數仍為0。
獨立讀回四輪均為40個訓練batch、2次純量loss VJP、1次optimizer更新，
半徑0.003、α0.5，沒有零梯度batch。穩態epoch3、4的完整MAX-rank時間中位數
4.508388秒，含準備與所有收尾的完整四輪fold為294.648962秒；最少顯存餘裕
2.676758GiB。這是工程結果，不用四輪策略績效代替正式收斂。

機制驗證已獨立回存103個檔案、2份checkpoint與1份model，逐檔SHA及CPU有限值
驗證通過。封存SHA為`efc23fdb749206cce96bfdf7e3fbd8b4e30e5334899be71b27a755935d96d006`，
收據為`local-scratch-sharpness-v3-mechanism-preservation.json`；完整產物位於
`scratch-sharpness-v3-mechanism-retained/`。主佇列每組仍會獨立工程驗證後從零正式訓練，
即時數量以進度JSON為準。

## 產物位置與研究限制

以下相對路徑都以`artifacts/operations/training-quality-audit-20261015/`為起點：

- 51組完整成果：`neural-experiment-results-20261020-midday.{json,csv}`。
- 即時遠端進度：`scratch-sharpness-v3-progress-current.json`。
- 回存狀態：`scratch-sharpness-v3-collection-status.json`。
- 機制驗證原件，遠端：`sharpness-recovery-v3-admission2/pre-admission/`。
- 已回存機制實測全產物：`scratch-sharpness-v3-mechanism-retained/`。
- 本輪完成後的本機全產物：`scratch-sharpness-v3-retained/`；尚未完成前不得聲稱已回存。
- 完成後新舊參考比較：`scratch-sharpness-v3-current-reference-comparison.json`。
- 最新完成三組：`scratch-sharpness-v2-retained/artifacts/markets/tw_scratch_sharpness_20261020_v2/_capacity/{case}/attempt-00/training/fold_10/`。
- 原MGDA：`scratch-gradient-geometry-v1-retained/artifacts/markets/tw_scratch_gradient_geometry_20261019_v1/_capacity/geometry__unit_mgda_sgd_step/attempt-00/training/fold_10/`。
- 前輪377%對數效用：`scratch-gradient-frontier-v2-retained/artifacts/markets/tw_scratch_gradient_frontier_20261020_v2/_capacity/frontier__simple_log_control/attempt-00/training/fold_10/`。

遠端新正式根目錄為`/root/stockAgent/artifacts/markets/tw_scratch_sharpness_20261020_v3/`；
工程根目錄為`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/scratch-sharpness-v3/`。
工程及正式輸出均保留validation、test、曲線、圖、checkpoint與生命週期驗收。

2026已多次觀察，是回顧研究而非未觸碰樣本；選模仍只用2025驗證log。
14726維附加來源標記`research_only=True,historical_point_in_time=False`，不得據此
宣稱歷史PIT、可實盤收益或broker成交。年度梯度仍是年度目標聚合的分塊截斷BPTT與
整張代理梯度，不是整年精確帳務導數。本輪沿用既有編譯、快取、DDP與帳務圖，
但沒有建立只剩硬體純運算瓶頸的證據；增加第二次年度反傳有實際成本。
