# 從零策略的梯度方向研究：2026-10-20

2026-10-20上午更新：本頁下列41組表與v2隊列為凌晨快照。v2七組已於
07:34完成，07:40回存，現在共48組。最新完整結果、377.03%單一log、
DiBS單步與下一輪SAM／ASAM／XSAM見[鄰域穩健研究](training_sharpness_2026-10-20.md)。
下列「最高報酬／Sortino」只適用於原41組，不能解讀為最新排名。

本次目標是超越使用者指定的`geometry__unit_mgda_sgd_step`，同時保留
最新單一log-utility的報酬優勢。全部模型從隨機參數開始，沒有舊模型微調、
教師模型或跨實驗optimizer續跑。本文件記錄已完成結果、原始論文篩選、
實測推導與已排入vastai1T的下一輪；新一輪收斂結果另由監督程序回收。

## 已完成41組，最新七組均已收斂

2026-10-20 00:34臺北時間，最新七組1003檔案、28份checkpoint、14份獨立模型
已完成離機SHA驗證與CPU有限值檢查。七組都依連續100輪無改善停止。
完整結果為`artifacts/operations/training-quality-audit-20261015/neural-experiment-results-20261020.{json,csv}`。

下表為2026-01-02至2026-10-02、181交易日、手續費後實際帳務結果；
MDD以正的回撤幅度呈現。選模使用2025驗證純淨log-utility，兩年驗證組
則使用2024–2025。2026不參與checkpoint挑選，但已被多次觀察，屬回顧研究。
2330為adjusted-close含息買入持有參考，未扣交易成本；不是2330當沖策略。

| 策略／Strategy | 報酬 | Sharpe | Sortino | MDD | 最佳／停止epoch |
| --- | ---: | ---: | ---: | ---: | ---: |
| 原參數空間單位MGDA／Previous parameter-space unit MGDA | 153.13% | **5.933** | **17.091** | 6.83% | 128／228 |
| MGDA控制重跑／MGDA control repeat | 138.80% | 5.444 | 14.047 | 3.81% | 129／229 |
| 單一對數效用／Single log utility | **268.64%** | 4.624 | 8.834 | 11.13% | 59／159 |
| 對數效用小步長／Small-step log utility | 145.37% | 3.737 | 7.522 | 10.15% | 148／248 |
| MGDA半步長／Half-step MGDA | 138.67% | 4.709 | 12.884 | 6.17% | 215／315 |
| MGDA雙步長／Double-step MGDA | 196.97% | 4.365 | 10.898 | 11.66% | 245／345 |
| 近四年MGDA／Recent-four-year MGDA | 180.84% | 4.773 | 9.800 | 9.45% | 125／225 |
| 兩年驗證MGDA／Two-year-validation MGDA | 110.96% | 3.044 | 5.831 | 10.17% | 239／339 |
| 使用者指定舊fold11／Specified historical model | 95.93% | 3.662 | 6.353 | 9.57% | 歷史產物 |
| 台積電買入持有／2330 buy and hold | 62.73% | 2.014 | 3.359 | 13.04% | 基準 |

單一log-utility為41組神經網路中報酬最高者；原MGDA仍有最高Sharpe與Sortino。
低回撤MGDA控制組與它們構成有意義的取捨，不存在已證實全面更好的單一新冠軍。
只有低交易量的模型也能有更低MDD，因此不能只依MDD宣告最好。

相同起始帳戶、149個共通交易日的固定動作重播：

| 策略／Strategy | 報酬 | Sharpe | Sortino | MDD |
| --- | ---: | ---: | ---: | ---: |
| 原參數MGDA／Previous parameter MGDA | 125.76% | 7.052 | 22.290 | 3.03% |
| MGDA控制重跑／MGDA control repeat | 84.53% | 5.643 | 14.002 | 3.70% |
| 單一對數效用／Single log utility | 176.33% | 4.484 | 8.521 | 10.61% |
| 指定舊模型／Specified historical model | 69.85% | 3.485 | 5.995 | 9.50% |

最新兩組MGDA在兩個區間都四項優於舊fold11，不能因此宣稱優於原MGDA153.13%。
新一輪另設`*-current-reference-comparison.json`，明確對照原MGDA、低回撤MGDA、
高報酬log-utility三個參考，避免舊比較器只對舊fold11的通過旗標造成誤讀。

## 第一性原理診斷與已做的實測

一條日報酬路徑經四種統計量產生四個梯度。這裡的目標是
`-Sharpe`、`-252×mean(log-return)`、`-Sortino`、`-Calmar`，並非四個loss固定平均。
它們對成長率、整體波動、下行波動與極端回撤的偏好不同。更快或訓練集共同下降，
都不能推出下一年度四項績效必然提高。

最新七組沒有零梯度epoch；五組MGDA的實際參數更新診斷沒有任務上升epoch。
帳務重播最大誤差為2.32e-13。現有證據不支持用錯誤反傳解釋這次風險取捨。
同一設定重跑仍產生不同軌跡，符合非凸、混合精度與離散選股對微小擾動敏感；
這不是seed搜尋，也不是整條訓練bitwise可重現的證明。

完整epoch最大rank的中位時間：MGDA控制9.7476秒、單一log4.6154秒。
資料區間相同，但策略、交易路徑與epoch數不同，因此只是工作流觀測，不能當作
配對ABBA算子加速證明。新方法維持四次模型VJP，僅在既有4×4 Gram上計算，
不引入額外Transformer前反傳、四份CPU參數複製或大型QP。
仍須量測冷啟動、穩態與完整fold；未宣稱只剩硬體限制或100%有效GPU利用率。

令四個全域參數梯度為`g_i`，`u_i=g_i/||g_i||`，`Q=UUᵀ`。
MGDA解單形最小範數組合。當四個頂點全部有效時，KKT給出`Qα=λ1`；
因此ConFIG等對齊方向`UᵀQ⁺1`可能與MGDA同向，不能僅因論文更新就預期改善。

已用控制組229輪記錄的完整梯度cosine重建Q，以相同參數點驗算：

| 方向／Direction | 與MGDA不同的輪數 | 方向cosine中位數 | log局部下降量相對MGDA中位數 | 局部衝突輪數 |
| --- | ---: | ---: | ---: | ---: |
| 等對齊／ConFIG | 62／229 | 1.0000 | 1.0000 | 0 |
| 單步議價／One-step DiBS | 229／229 | 0.9826 | 0.9428 | 1 |
| 五步議價／Five-step DiBS | 229／229 | 0.9891 | 0.9566 | 0 |
| 對數優先投影／Log-preferred UPGrad | 229／229 | 0.7418 | **1.3575** | 0 |

這是固定參數下的梯度幾何反事實，不是報酬預測；不能把35.75%局部下降改善
寫成35.75%交易報酬改善。數據位於`gradient-frontier-v2/recorded-gram-counterfactual.json`。
這項實測使對數優先投影成為最直接對應目前瓶頸的假設，而ConFIG提供不同的平衡對照。

## 原始研究：起點、演進與2026前沿

本次查詢日期為2026-10-20；下列為與現有機制直接相關的原始研究，
不表示已讀完所有領域論文，也不把其他領域的實驗提升當作交易績效保證。

| 年代與原始研究 | 對本案的含義與取捨 |
| --- | --- |
| [Kelly, 1956, A New Interpretation of Information Rate](https://onlinelibrary.wiley.com/doi/10.1002/j.1538-7305.1956.tb03809.x) | 對數資產成長的源頭；支持保留簡單log-utility控制，但不直接約束有限期間MDD。 |
| [Moody與Saffell, 1998, Reinforcement Learning for Trading Systems and Portfolios](https://www.aaai.org/Papers/KDD/1998/KDD98-049.pdf) | 直接學交易政策並最佳化利潤與風險指標；不是先預測價格再替代交易目標。 |
| [Désidéri, 2012, MGDA](https://www.numdam.org/item/10.1016/j.crma.2012.03.014/) | 共同下降與Pareto stationary的基礎；它保證的層次不是跨年度實現損益。 |
| [PCGrad, NeurIPS 2020](https://proceedings.neurips.cc/paper/2020/hash/3fe78a8acf5fda99de95303940a2420c-Abstract.html) | 過去已測；逐對處理一次不等於四任務最終方向共同下降。 |
| [CAGrad, NeurIPS 2021](https://proceedings.nips.cc/paper_files/paper/2021/hash/9d27fdf2477ffbff837d73ef7ae23db9-Abstract.html) | 以平均loss為主並控制最差局部改善；本案不先加回四個固定平均loss，較直接的是帶明確log偏好的投影。 |
| [Nash-MTL, ICML 2022](https://proceedings.mlr.press/v162/navon22a.html) | 梯度議價的前身。查核了此路線，再選擇只需單位方向與小Gram的DiBS作本輪新假設；並非判定Nash無效。 |
| [FAMO, NeurIPS 2023](https://papers.neurips.cc/paper_files/paper/2023/hash/b2fe1ee8d936ac08dd26f2ff58986c8f-Abstract-Conference.html) | 以動態權重降低多任務反傳負擔。它改變現有精確四梯度機制；需另驗跨epoch權重與金融loss適用性，不作本輪方向控制的混合變因。 |
| [Jacobian Descent／UPGrad, 2024→2025修訂](https://arxiv.org/abs/2406.16232) | 先投影到所有任務的共降錐，再按偏好組合。[作者實作](https://torchjd.org/v0.10.0/docs/aggregation/upgrad/)明確支援preference vector；本輪採log偏好`[0,1,0,0]`，不平均四個loss。 |
| [ConFIG, ICLR 2025 Spotlight](https://arxiv.org/abs/2408.11104) | 等化各單位梯度與更新方向的對齊。原研究主要為PINN；本案只借方向，使用明訂長度規則並驗證奇異系統。 |
| [DiBS-MTL, 2025預印本→2026-02修訂](https://arxiv.org/abs/2509.23948) | 最新題名為Monotonic Transformation Invariant Multi-task Learning。用單位方向進行局部議價，適合檢查不同指標尺度的干擾；本次未從官方核實會議錄用，因此標示預印本。 |
| [SAM, ICLR 2021](https://research.google/pubs/sharpness-aware-minimization-for-efficiently-improving-generalization/)→[XSAM, ICLR 2026](https://proceedings.iclr.cc/paper_files/paper/2026/hash/f2caa9c7caafa2375e643c8a98e8c58c-Abstract-Conference.html) | 參數鄰域穩健性是下一個正交假設。相較普通梯度更新，金融年帳需要額外擾動點完整forward/backward與精確恢復；本輪先隔離方向效應，不能直接套影像資料的效果與耗時結論。 |
| [CAME-Grad, 2026-05預印本](https://arxiv.org/abs/2605.22635) | 針對放射報告的方向衝突與更新幅度一起調整；其跨領域實驗不證明金融效果。本輪刻意控制長度，不混入額外能量／幅度變因。 |
| [MOON, 2026-08預印本](https://arxiv.org/abs/2608.11749) | 直接在矩陣的spectral／nuclear幾何下議價，再用polar因子更新；比單純MGDA後接Muon更有一致幾何。需逐層矩陣、向量參數及optimizer狀態的新契約，列為下一個架構層假設，本輪尚未實作或排入。 |
| [SIMS, KDD2026；2026-09預印本](https://arxiv.org/abs/2609.12599) | 以log轉換的merit function實現尺度不變scalarization。需先核實金融有符號loss、定義域與shift的含義；尚未加入，不把它誤認為資產log-utility。 |
| [Adaptive Projected-Gradient for SAA, 2026-09預印本](https://arxiv.org/abs/2609.02722) | 不依loss值或line search更新正則化，值得對照學習率。其閉凸可行集與sample-average假設不直接等同含離散交易的Transformer，暫不直接搬入。 |

## 下一輪七組：已排入既有排程

配置：`configs/ablations/tw_scratch_gradient_frontier_20261020_v2.yaml`。
全部fold10、訓練2015–2024、驗證2025、測試2026，seed42，d32/lookback32，
global batch128，SGD無momentum／weight decay，最高1000epoch、patience100。
只有確認CUDA OOM才允許既有batch128→64→32容量回退；其餘失敗留證後繼續。
資料、金融loss定義、訊號時點、整張限制、費用、分鐘帳務與2330買入持有基準均沿用。

| 次序 | 策略／Strategy | 對照假設 |
| --- | --- | --- |
| 1 | 單位MGDA控制／Unit MGDA control | 新source仍重現原有機制，從零開始。 |
| 2 | 單一對數效用控制／Single log utility control | 保留最高報酬與單次反傳參考。 |
| 3 | 對數優先共降投影／Log-preferred UPGrad | 最大限度保留成長方向，對其他三個梯度施加局部不衝突條件。 |
| 4 | 等對齊梯度／ConFIG matched length | 檢查等對齊與最小範數方向在有效面改變時的差異。 |
| 5 | 等對齊半步長／ConFIG half step | 檢查離散帳務對有限步長的敏感性。 |
| 6 | 單步方向議價／One-step DiBS | 單位方向聚合，不以loss值平均。 |
| 7 | 五步方向議價／Five-step DiBS | 以4×4 Gram做五個局部議價步，無額外模型反傳。 |

四種新方向先匹配當下單位MGDA梯度長度，再做一次全域clip與SGD。
因此它們是明確的研究適配版，不是未修改原論文版本；同名設定的完整ABI與
fingerprint由checkpoint contract綁定。對數投影枚舉16個active faces並驗KKT；
ConFIG不一致奇異解若產生衝突，記錄後回退MGDA。DiBS不宣稱逐步共同下降，
即使出現任務上升也如實記錄。零任務不丟棄；近零MGDA方向不硬造非零更新。
若MGDA的Gram平方範數落在`64×FP64 epsilon×Gram scale`的消去誤差範圍，
直接保留該輸入的原MGDA向量，不以不可靠平方範數開根號放大，也不把真正微小更新硬設為零。

本機修正後測試93 passed、5 skipped；涵蓋獨立全向量／SLSQP oracle、單位尺度、
固定log偏好的置換、奇異／零梯度與雙rank Gloo帳務重播。
108組FP32／FP64舊方法小向量與診斷逐值相同，五個舊objective contract不變。
這些不是整條神經網路訓練bitwise一致證明。
遠端在2026-10-20 01:05:24臺北時間完成strict CUDA、實體CUDA／雙rank NCCL
與帳務oracle（62 passed），以及optimizer測試（13 passed），三個gate退出碼均為0。
每組再跑四個完整工程epoch、檢查artifact生命周期後才從零正式訓練。

source `9df3a16a6a6f538a260a6489788d87d026f2871c042f0e9f80c68b187838a594`
只修改三個已審查檔案；runtime仍為
`66926c5bcb2bdf743af48d7e9f646ae581f2497db26cbf8b8472235b5dd5eaf7`。
1692個source檔、wheel與receipt已回本機逐SHA驗證。

## 產物與持續進度

- 遠端正式產物：`/root/stockAgent/artifacts/markets/tw_scratch_gradient_frontier_20261020_v2/`。
  每組public alias指向`_capacity/<case>/attempt-00/training/fold_10/`；容量回退以`selection.json`為準。
- 遠端工程產物：`/root/stockAgent/artifacts/benchmarks/training_quality_20261015/scratch-gradient-frontier-v2/`。
- 本機進度：`artifacts/operations/training-quality-audit-20261015/scratch-gradient-frontier-v2-progress-current.json`。
- 本機新產物自動回收目的地：`artifacts/operations/training-quality-audit-20261015/scratch-gradient-frontier-v2-retained/`。
  只有完成驗收後才存在完整回收模型，不能把目的地當作已完成產物。
- 操作、source與驗收：`artifacts/operations/training-quality-audit-20261015/gradient-frontier-v2/`。
- 收斂後對目前三個參考的結果：`artifacts/operations/training-quality-audit-20261015/scratch-gradient-frontier-v2-current-reference-comparison.json`。
- 最新七組已回收：`artifacts/operations/training-quality-audit-20261015/scratch-sgd-validation-v1-retained/`。
- 原MGDA最佳本機模型：`artifacts/operations/training-quality-audit-20261015/scratch-gradient-geometry-v1-retained/artifacts/markets/tw_scratch_gradient_geometry_20261019_v1/_capacity/geometry__unit_mgda_sgd_step/attempt-00/training/fold_10/`。

遠端run：`scratch-gradient-frontier-v2-20261019T170413-9db8171b`，
2026-10-20 01:04:13臺北時間排入。第一組MGDA控制組四輪完整工程fold於01:12驗收通過，
global128、雙GPU、完整fold 393.57秒（包含冷啟動及產物）、第3–4輪最大rank完整epoch
中位10.56秒，穩態新增圖0；四輪皆有optimizer更新、沒有零梯度輪。
正式訓練已由全新參數啟動，總epoch1000、patience100，其餘六組待序。
工程admission已回本機`gradient-frontier-v2/engineering-admission.json`。
最新即時觀測以progress-current與physical-oracle receipt為準；正式epoch與工程epoch分開，
未完成收斂前不宣告新方法勝出。

v1在00:57的實體oracle攔下近零方向放大問題，未開始正式訓練。其source0918dff與
測試日誌完整保留於`gradient-frontier-v1/`及v2的`rejected-v1-*`證據。
v2使用新ABI與新產物根目錄；原有opposite尺度測試容差不變，新添的nearly-opposite
案例則逐輸入精確核對回退原MGDA，避免對近奇異原始solver宣稱不存在的bitwise尺度不變。
v2 source已成功封存後，plan binding另因遠端缺少v8基底配置拒絕；補齊同SHA配置，
以`complete-plan.py`從同一封存source重做binding，沒有重建或偷換source。
