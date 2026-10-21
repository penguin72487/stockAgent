# 從零策略：完整年度的參數鄰域穩健訓練，2026-10-20

最新51組成果、環境修復與11組弱擾動續排，見
[中午續報](training_sharpness_recovery_2026-10-20.md)；下文保留上午48組的查核快照。

## 最新成果與判斷

查核時間為2026-10-20上午。上一輪梯度前沿v2七組於07:34:56臺北時間完成，
07:40:51已回存本機；1003個檔案、28份checkpoint、14份模型通過SHA及CPU有限值驗證。
現在共有48組正式收斂結果。沒有新候選同時在181日與共同149日的四項指標超越原MGDA。
最新彙總：`artifacts/operations/training-quality-audit-20261015/neural-experiment-results-20261020-am.{json,csv}`。

以下為2026-01-02至10-02的181交易日、同起始帳戶、策略扣除費用後結果。
MDD以正回撤幅度呈現。選模僅看2025驗證純log-utility；2026已多次觀察，是回顧研究。
2330是adjusted-close含息買入持有、未扣交易成本的參考，不是當沖2330。

| 策略／Strategy | 報酬 | Sharpe | Sortino | MDD | 最佳／停止epoch |
| --- | ---: | ---: | ---: | ---: | ---: |
| 原參數空間單位MGDA／Previous parameter-space unit MGDA | 153.13% | **5.933** | 17.091 | 6.83% | 128／228 |
| 本輪MGDA控制／Current unit MGDA control | 91.26% | 4.197 | 13.255 | 4.68% | 105／205 |
| 單一對數效用控制／Single log utility control | **377.03%** | 4.993 | 9.293 | 13.48% | 83／183 |
| 對數優先投影／Log-preferred UPGrad | 229.96% | 3.542 | 6.050 | 15.41% | 100／200 |
| 等對齊方向／ConFIG matched length | 102.65% | 3.453 | 6.450 | 10.34% | 142／242 |
| 等對齊半步長／ConFIG half step | 101.82% | 4.425 | 12.058 | 4.33% | 191／291 |
| 單步方向議價／One-step DiBS | 79.15% | 3.781 | **19.914** | **3.34%** | 99／199 |
| 五步方向議價／Five-step DiBS | 94.93% | 3.428 | 7.184 | 10.38% | 309／409 |
| 指定舊fold11／Specified historical model | 95.93% | 3.662 | 6.353 | 9.57% | 歷史產物 |
| 台積電買入持有／2330 buy and hold | 62.73% | 2.014 | 3.359 | 13.04% | 基準 |

原MGDA在共同149日為報酬125.76%、Sharpe7.052、Sortino22.290、MDD3.03%。
新單一log在此範圍為225.12%、4.371、7.890、14.74%；DiBS單步為54.29%、
4.339、17.820、3.44%。只看181日的某個勝出指標，不能寫成全面超越原MGDA。
原MGDA、138.80%控制及本次91.26%控制雖設定相近，結果仍不同；單一seed與
小向量梯度相容檢查不能建立端到端位元重現，不能把差異全歸因於新演算法。

再讀取三個模型保存的原始入場請求與扣費日報酬，得到：

| 行為描述／Behavior | 原MGDA | 單一log377% | DiBS單步 |
| --- | ---: | ---: | ---: |
| 平均請求gross（非成交曝險） | 92.20% | 96.24% | 85.76% |
| 平均非零請求股票數（門檻1e-6） | 12.26 | 7.71 | 4.54 |
| 最差單日淨報酬 | -3.69% | -8.76% | -1.58% |
| 最佳單日淨報酬 | 7.61% | 9.38% | 12.71% |
| 下檔日報酬RMS（全181日分母） | 0.477% | 1.475% | 0.257% |
| 虧損日數 | 77 | 69 | 115 |
| 與原MGDA入場請求cosine均值 | 1.000 | 0.272 | 0.605 |

單一log的平均請求gross只高約4個百分點，下檔RMS卻約3.1倍；這支持優先研究
選股與參數穩健性的假設，不能僅由總曝險解釋差異。DiBS有較多小虧日及大正向日，
能同時出現高Sortino、較低Sharpe。這些是路徑描述，不建立機制因果或未來保證。
請求向量尚未經交易許可、容量與整張執行；每天收盤強制平倉的零權重也不能用來
判定兩個策略相同。原始診斷含SHA：`sharpness-v1/strategy-behavior-diagnosis.json`。

## 第一性原理：先改哪個機制

1. 單一log直接最佳化複利成長，卻不直接約束有限期間的最大回撤。
   現在377.03%報酬與13.48%回撤，證明成長訊號存在，也顯示風險有代價。
2. MGDA／ConFIG的共同下降只描述當下參數附近的一階梯度；不保證非線性更新後
   所有指標都改善，更不保證跨年度。這七組已實測此路線，沒有全面支配原MGDA。
3. 交易帳務含整張門檻與資金遞迴。參數微小改變可能切換持倉路徑；
   因此「附近參數仍能維持年度log表現」是值得獨立驗證的假設。
   目前證據不足以宣稱尖銳極小值或過擬合就是已確認根因。
4. 本輪選擇參數鄰域穩健性，保留簡單log，不先疊四項固定加權懲罰。
   SAM＋MGDA則隔離測試：先沿合成方向擾動，再於擾動點重新算四個梯度。
   四項為Sharpe、Log Return、Sortino、Calmar；MDD繼續作績效報告。
   這是多目標啟發式，不轉移純量SAM的理論保證。

SAM使用`δ=ρg/||g||`。ASAM使用`T=diag(|θ|+0.01)`，
`δ=ρT²g/||Tg||`；所有座標均採此明訂尺度，故其ρ不能與SAM歐氏半徑直接相比。
XSAM採作者二次更新的球面方向混合：從實際擾動單位方向到擾動點梯度單位方向
作SLERP，乘回擾動梯度長度。本輪固定α=1.5；α=1退化為SAM。
接近平行、反平行或零方向使用擾動梯度，避免不穩定除法，並輸出計數。

## 原始論文：起點至2026前沿

完整1956至2026梯度幾何文獻沿革見[前輪研究](training_gradient_frontier_2026-10-20.md)。
本輪另讀以下原始來源；跨領域效果不等於本案金融績效。

| 原始研究 | 與本輪的關係 |
| --- | --- |
| [Kelly, 1956](https://onlinelibrary.wiley.com/doi/10.1002/j.1538-7305.1956.tb03809.x) | 對數財富成長源頭；不直接解決有限期間MDD。 |
| [Risk-Constrained Kelly Gambling, 2016](https://web.stanford.edu/~boyd/papers/kelly.html) | 把成長與資產低於門檻機率分開處理。其假設及低於起始財富的事件不等同本案依賴報酬路徑的峰值MDD，不能套作回撤保證。 |
| [SAM, 2020預印本／ICLR2021](https://arxiv.org/abs/2010.01412) | 最佳化參數鄰域內的最大loss；採其一次擾動再求梯度的實作。 |
| [ASAM, ICML2021](https://proceedings.mlr.press/v139/kwon21b.html) | 調整鄰域幾何以處理參數尺度；本輪明訂`abs(θ)+.01`與全座標慣例。 |
| [SAM at the Edge of Stability, JMLR2024](https://jmlr.org/papers/v25/23-1285.html) | 說明SAM動力學與穩定性值得檢查；不能把較平坦直接視為必定提高金融Sharpe。 |
| [SAM: General Analysis and Improved Rates, 2025](https://arxiv.org/abs/2503.02225) | 核對理論假設與收斂限制，仍需本案非光滑帳務實測。 |
| [SAMO, ICCV2025](https://arxiv.org/html/2507.07883v1) | 已改從作者arXiv全文核對§5：混合平均梯度與各任務局部擾動，以SPSA雙邊forward近似局部梯度，再逐層正規化。與本輪單一共同擾動重算四梯度不同。四個任務仍可能多8遍年度forward，須量測整張帳務的差分方差，列作後續假設。 |
| [Sassha, ICML2025](https://proceedings.mlr.press/v267/shin25d.html) | 自適應二階與穩定Hessian近似是另一方向；本案反傳包含代理梯度，不能將其二階量直接解讀為整張實現報酬的真Hessian。本輪不混入新optimizer與近似二階變因。 |
| [XSAM, ICLR2026](https://proceedings.iclr.cc/paper_files/paper/2026/hash/f2caa9c7caafa2375e643c8a98e8c58c-Abstract-Conference.html)與[作者程式](https://github.com/Cccjl219/XSAM) | 核對實際擾動、SLERP及參數恢復順序。本輪是固定α消融，沒有照搬動態α搜尋；作者搜尋0至2、間隔0.1，需要21次額外forward，且其影像loss假設不宜直接搬入可為負的金融loss。 |
| [DA-SAM, ICML2026](https://proceedings.mlr.press/v306/he26m.html) | 動態domain尺度及多方向flattening的前沿方向。可把年度當domain研究，但還需定義年度權重、最壞年度帳戶與擾動，先列文獻假設，未實作或排入本輪。 |
| [MASAM, ICLR2026](https://proceedings.iclr.cc/paper_files/paper/2026/hash/94ffbc2939e18adac36eedf8d38d1ef3-Abstract-Conference.html) | 依模態主導程度調擾動；需先定義本案feature分支是否真為不同模態，不直接以不同金融績效指標代替模態，未排入。 |
| [MOON, 2026-08預印本](https://arxiv.org/abs/2608.11749) | 在矩陣spectral／nuclear幾何下聚合並正交化。尚未排入，因為需要矩陣／向量角色、optimizer與checkpoint的新實作；本輪先隔離穩健性假設。 |

SAMO的CVF頁面回403後，已從作者arXiv取得全文；沒有把本輪SAM＋MGDA誤標為SAMO。

## 已實作的八組與驗收

配置：`configs/ablations/tw_scratch_sharpness_20261020_v2.yaml`，繼承v1相同八組。
全部從零、全參數可訓練，seed42，d32/lookback32、global batch128、雙GPU DDP，
SGD lr0.1、無momentum／weight decay。2015–2024訓練、2025驗證、2026測試，fold10。
最高1000epoch；連續100回合無改善停止，比例0.1維持。
只有確認CUDA OOM可依128→64→32回退容量，其餘保留失敗後繼續。

| 次序 | 策略／Strategy | 設定 |
| --- | --- | --- |
| 1 | 單位MGDA控制／Unit MGDA control | 無擾動 |
| 2 | 單一對數效用控制／Single log utility control | 無擾動 |
| 3 | 對數鄰域穩健／SAM log utility | ρ0.01 |
| 4 | 對數鄰域穩健／SAM log utility | ρ0.05 |
| 5 | 自適應鄰域穩健／ASAM log utility | adaptive ρ0.05 |
| 6 | 固定係數顯式鄰域穩健／Fixed-alpha XSAM log utility | ρ0.05、α1.5 |
| 7 | MGDA鄰域穩健／SAM with unit MGDA | ρ0.01 |
| 8 | MGDA鄰域穩健／SAM with unit MGDA | ρ0.05 |

第一遍只求同步後未clip的全年度梯度，不更新optimizer或scheduler。
這裡指完整年度彙總，沿用原有分塊BPTT與整張交易代理梯度；不是宣稱跨全年
無截斷反傳或離散帳務的真導數，因此純量SAM的光滑理論只能作假設來源。
備份全部master參數；第二遍重播rank-local RNG，於擾動點重新執行原帳務與反傳。
在clip前精確還原所有原參數，再作唯一一次正式更新；第二遍未使用的參數也必須還原。
MGDA先合成四梯度再執行還原hook，更新方向診斷以還原後參數為起點。
失敗時`finally`還原；不從未通過驗收的半輪記憶體狀態產生相容resume。

共用trainer、checkpoint與生命週期持續持有責任。啟用方法有新optimizer契約與新root；
停用時省略新欄位，維持歷史語意fingerprint。費用、整張、FIFO、因果時鐘、特徵、
虧盡吸收狀態、純log驗證選模與完整輸出均沿用。
實際來源日誌仍標示14726維factorized attachment為`research_only=True`、
`historical_point_in_time=False`；沿用既有研究ABI不構成新增的歷史PIT或實盤證明。
本機117項測試通過、7項依硬體條件略過；遠端另外驗證實體雙GPU。
遠端新source的SAM／ASAM／XSAM測試25項通過，既有多目標梯度與實體
雙rank帳戶測試62項通過；strict CUDA與停用方法的兩種歷史契約／snapshot比較均通過。
正式隊列前先跑SAM Log與SAM＋MGDA各4完整epoch，不捐贈其權重。
每個正式候選仍經既有工程／容量與完整產物驗收，完成後回存並比較181／149日。

v1的SAM Log已完成4輪、訓練exit0及完整生命週期，但舊工程驗收器預期20批，
把實際兩遍40批誤判為工作量不符，因而在正式訓練前停止。
修正共用benchmark後，本機相關68項測試通過，增加漏遍、雙重更新、缺VJP及NaN拒絕測試；
同時讓零MGDA證明使用目前的Calmar任務名稱。
v2新來源只有benchmark一個檔案不同；訓練、梯度、帳務、資料與模型位元相同，
所以保留並重新評分v1的4輪證據，另外執行SAM＋MGDA完整4輪。
原失敗收據不覆寫，未從4輪模型續訓，也沒有降低覆蓋、CUDA Graph、記憶體或完整產物門檻。

## 計算成本與產物

前輪穩態epoch3起、完整工作流程最大rank中位數：單一log4.43秒、MGDA9.02秒、
UPGrad17.32秒、ConFIG9.42秒、ConFIG半步9.56秒、DiBS單步8.80秒、五步9.37秒。
數值包含當輪完整流程，冷啟動、首次編譯與最終fold輸出另計，不能宣稱只有GPU算力瓶頸。

本輪SAM Log需兩次年度VJP；SAM MGDA需八次VJP及兩次記錄forward。
這是額外有效計算成本，將量測完整epoch與fold，不隱藏驗證、曲線、checkpoint、繪圖。
沿用原本CUDA Graph、編譯、resident cache與同步後完整年度梯度機制；
額外參數向量操作與精確備份有界，不執行21遍年度forward的α搜尋。

首個SAM Log的完整4輪已實際執行，修正驗收器後通過兩遍、單次更新與完整生命週期驗收。
epoch3–4完整工作流程最大rank中位數4.336秒；完整fold281.10秒，包含冷資料準備、
首次編譯與最終產物。雙卡峰值約29886／27530 MiB，最低餘裕2.657 GiB。
這是機制工程證據，不是100回合無改善後的策略品質，也不是與前輪控制同程序的加速比。

v2已於09:01:53臺北時間進入遠端受監督排程，run ID為
`scratch-sharpness-v2-20261020T010153-43bbf919`。
來源SHA為`b9869f415c24e9555a0b466e7b4ff77960fafe7a0627c5b5301eec534b121e88`，
runtime SHA為`66926c5bcb2bdf743af48d7e9f646ae581f2497db26cbf8b8472235b5dd5eaf7`。
1693份來源檔、wheel及receipt已回存並逐檔驗證。
新來源再次通過strict CUDA與62項共用benchmark測試；87項實體梯度測試沿用訓練數學
檔案SHA完全一致的4b8b來源證據，不冒稱對新整包來源重新執行所有實體測試。
SAM＋MGDA的4輪也已通過：每輪200批計算、八次VJP、一次optimizer更新，
模型前向等價遍數10，沒有偵測到失敗或fallback；穩態完整epoch中位數17.739秒。
排程已自動交給既有八組工程／正式訓練runner，控制組排在前兩位。
本機監督、回存與四參考比較工作均已排入systemd；重接後從進度receipt讀回實際階段。
09:08:47獨立讀回時，run仍為running，正在第一組`sharpness__unit_mgda_control`
的工程驗收，八組正式訓練尚未開始；沒有把短試跑算進48組正式成果。

遠端新root：`/root/stockAgent/artifacts/markets/tw_scratch_sharpness_20261020_v2/`。
遠端八組工程root：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/scratch-sharpness-v2/`。
計畫與來源收據：`artifacts/operations/training-quality-audit-20261015/sharpness-v2/`。
首個SAM Log與87項實體梯度測試：同層`sharpness-v1/`。
兩個機制試跑的完整訓練產物在遠端
`sharpness-v1/pre-admission/`與`sharpness-v2/pre-admission/`，不是正式收斂結果。
這兩個試跑已完整回存至
`artifacts/operations/training-quality-audit-20261015/scratch-sharpness-v2-mechanism-retained/`，
178個檔案、4份checkpoint、2份獨立模型皆通過逐檔SHA與CPU有限值驗證；
archive SHA為`1d98efa00381dd4512cf5859ed884712a6f2247acef9bc574f8473abc6d7d760`。
驗收收據為同層`local-scratch-sharpness-v2-mechanism-preservation.json`。
SAM Log的重新評分sidecar保留舊結果的`reasons`欄位以保存原拒絕脈絡；
目前驗收看新評分的`ok`、`lifecycle_ok`與`two_pass_one_update_verified`，三者均為true。
本機進度：`artifacts/operations/training-quality-audit-20261015/scratch-sharpness-v2-progress-current.json`。
完成後保全：`artifacts/operations/training-quality-audit-20261015/scratch-sharpness-v2-retained/`。
正式品質結論須等100回合無改善與完整回存，不用4輪工程結果冒充策略改善。

原MGDA最佳模型已保全在
`artifacts/operations/training-quality-audit-20261015/scratch-gradient-geometry-v1-retained/artifacts/markets/tw_scratch_gradient_geometry_20261019_v1/_capacity/geometry__unit_mgda_sgd_step/attempt-00/training/fold_10/`。
目前377.03%單一log模型在
`artifacts/operations/training-quality-audit-20261015/scratch-gradient-frontier-v2-retained/artifacts/markets/tw_scratch_gradient_frontier_20261020_v2/_capacity/frontier__simple_log_control/attempt-00/training/fold_10/`。
兩者均含`model.pt`；每輪曲線與計時在其所屬`training/train_*/`群組目錄。
