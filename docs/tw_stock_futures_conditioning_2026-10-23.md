# 2011 起股票期貨：目前證據與雙時鐘訓練實驗

**10/23 21:51 更新：**下列 17:09 是保留的歷史快照。兩個時鐘的完整 A/B/B/A 已通過，
08:45／09:00 正式對照均已收斂，另外五組工程亦已修復並通過。全商品財務元件
只通過部分商品，尚未全商品訓練。新共同期間個股兩組已准入；擴大兩組發現並修正週月價差腿
解析缺陷，1,857／1,857 差異有官方來源證明，正在遠端新根重建，之後才可工程與正式訓練。
最新範圍、結果、文獻與排程見 [晚間研究報告](tw_futures_equity_universe_2026-10-23.md)，
全目錄明細保留於 [全台幣期貨範圍盤點](tw_futures_universe_and_progress_2026-10-23.md)。

最後遠端讀回：2026-10-23 **17:09（臺北）**。這一輪研究的是可跨日持倉的股票期貨，
兩個決策時鐘為 08:45、09:00；新模型全部從零訓練。

最新進展：原 2011 起隊列仍等待下述一口逐筆缺口。另行建立的 **2013 起完整資料**
已完成 canonical 來源核驗：1,705,459 契約日、1,341,942 verified intraday 契約日、
零缺口。v1 的 `clock0845__a1` 已完成四回合及完整產物，穩態完整 workflow
MAX-rank 平均 **104.027 秒**；其短測試淨報酬 **29.04%**、Sharpe **1.262**、
Sortino **1.829**、MDD 幅度 **21.95%**，同期 TX 毛 benchmark 報酬 **30.72%**。
這不是收斂結果，也沒有超越舊模型的證明。B1 隨後在裁切日期的 sparse source
座標上失敗，沒有接受任何 B epoch；已修正並給 sparse-only checkpoint 新 ABI。
兩卡均已通過 native 與 direct-CSR adjoint 小測，各後端每卡 16 項。
v2 已於 **16:05** 重新排入原 Supervisor，先跑八組完整 workflow A/B/B/A，
再接六組工程與六組從零收斂。原 2011 主請求及 source bytes 保持原版本。
16:53 獨立讀回 v2 08:45 A1、B1、B2、A2 均已完成 4/4 epoch；
09:00 A1 已於 16:59 跑完四回合，合計 20 個市場 epoch。16:55 逐個驗證 08:45
四組完整 lifecycle、梯度及整口 tape，四組均通過；17:00 亦確認 09:00 A1 完整
lifecycle 通過。17:08 已有 28 個市場 epoch，17:09 核對 09:00 B1／B2
完整 lifecycle 亦通過；八組 A/B/B/A 已完成七組，僅餘 09:00 A2，
六組工程與六組正式訓練尚未開始，failure 為 null。
v2 A1 完整 case wall **647.699 秒**，完整 workflow epoch3/4 MAX 平均
**103.958 秒**。epoch4 梯度 norm 2.883、zero-gradient batches 0、
adjoint replay 0、新圖 0；145 個 test／deployment 日都有實際整口持倉。
這與 v1 是單對觀測，其他 warm-cache 效果未隔離，不用來宣稱整體加速倍率。
B1 完整 workflow epoch3/4 MAX 平均 **27.743015 秒**，有非零梯度、
adjoint replay 0、穩態新圖 0；相對 A1 為 **3.747 倍**，只是目前單對觀測。
B1 的首次 source build 納入第 1 回合，其完整四回合 case wall 為
**649.663 秒**，此短 fold 仍未比 A1 的 647.699 秒更快。
B 的首次 source build、後續 verified disk／host cache 必須分開報告；
不能把穩態倍率當成初次建置 fold 的倍率。08:45 完整 A/B/B/A 的
A 平均穩態 **103.964667 秒**、B 平均 **27.827847 秒**，完整穩態快 **3.736 倍**；
四回合 case wall 平均由 **650.204 秒降至 522.328 秒，快 1.245 倍**。
依實測冷 wall 加 96 次穩態的 100 回合估計為 10630.812／3193.801 秒（3.329 倍），
只是攤提估計，沒有實測完成 100 回合；B1 建置、B2 暖 cache 成本仍分開保留。
原始數值及全部四組機制讀回見
[08:45 完整 A/B/B/A](../artifacts/operations/futures-history2013-20261023-v2/clock0845-completed-abba-measurement.json)。
09:00 A/B/B/A 及兩時鐘共同選路尚未完成；尚無本輪正式收斂模型。
GPU busy 接近 100% 亦不是計算上限證據。
原始列見 [第一對完整投影量測](../artifacts/operations/futures-history2013-20261023-v2/initial-projection-pair-measurement.json)。

| v2 短程測試，每次四回合，2026-02-26～09-24，145 日 | 報酬 | Sharpe | Sortino | MDD 幅度 |
| --- | ---: | ---: | ---: | ---: |
| 08:45 原配置，四回合，扣原費稅 | 29.1801% | 1.26689 | 1.83749 | 21.9542% |
| 08:45 稀疏投影，四回合，扣原費稅 | 29.0907% | 1.26181 | 1.83103 | 22.0680% |
| 08:45 稀疏投影 B2，扣原費稅 | 28.6889% | 1.24638 | 1.80493 | 22.0258% |
| 08:45 原配置 A2，扣原費稅 | 29.0255% | 1.26168 | 1.82990 | 21.9397% |
| 09:00 原配置 A1，扣原費稅 | 10.1470% | 0.61789 | 0.85572 | 20.3420% |
| 09:00 稀疏投影 B1，扣原費稅 | 10.0717% | 0.61299 | 0.84916 | 20.3107% |
| 09:00 稀疏投影 B2，扣原費稅 | 11.0516% | 0.66375 | 0.92168 | 20.4580% |
| 同期 TX 近月轉倉，1 倍名目本金，未扣成本 | 30.7217% | 1.35995 | 2.03942 | 17.2172% |

七組均尚未勝過同期 benchmark，四回合只是工程驗證，並非收斂／績效選優證據。
09:00 A1 完整穩態 MAX-rank 平均 102.715505 秒；145 個 test／deployment 日均有
整口持倉，adjoint replay 0、有限非零梯度、穩態新圖 0。
[09:00 A1 完整數值及機制讀回](../artifacts/operations/futures-history2013-20261023-v2/clock0900-a1-completed-measurement.json)。
09:00 B1／B2 完整穩態分別為 27.685763／27.755608 秒，也有有限非零梯度、
adjoint replay 0、穩態新圖 0、145 日整口觀測；A2 尚未完成，不能先宣告該時鐘
的完整 A/B/B/A 倍率或共同選路通過。各組完整數值見
[09:00 B1](../artifacts/operations/futures-history2013-20261023-v2/clock0900-b1-completed-measurement.json)、
[09:00 B2](../artifacts/operations/futures-history2013-20261023-v2/clock0900-b2-completed-measurement.json)。
基準 MDD 由相同 NPZ 的 `benchmark_returns` 經 canonical drawdown reducer
重算，原 metrics、curves、workflow、整口 tape SHA 及驗收結果保存於
[v2 A1 精簡量測收據](../artifacts/operations/futures-history2013-20261023-v2/initial-baseline-measurement.json)。
B1 亦已核對 test／deployment 各 145 個非零整口持倉日，沒有非零要求卻全現金的日數；
其獨立數值與 SHA 見 [v2 B1 精簡量測收據](../artifacts/operations/futures-history2013-20261023-v2/clock0845__b1-initial-measurement.json)。
同一前置準備階段實測由 205.514 秒降至 5.999 秒；不是整個 epoch 快 34 倍。
本次資料移位、暫存刪除、固定來源及最新排程見後段「10/23 續修」。
本機讀回：[v2 原始 A1 量測收據](../artifacts/operations/futures-history2013-20261023-v2/superseded-v1-measurement-summary.json)、
[v2 請求](../artifacts/operations/futures-history2013-20261023-v2/queue-request.json)。

## 目前進度與結果的適用範圍

新 Vast `114.34.26.236:43269` 的財務帳戶及股票特徵已建成。
全量逐筆讀回完成 3,826 個日期後，資料準備於 13:35 被一個來源缺口擋下：

| 日期／物理合約 | 官方一般市場單邊量 | 已有逐筆單邊量 | H/L/C |
| --- | ---: | ---: | --- |
| 2012-06-04，`CTF:201206@2012-06-20#generation=1` | 108 | 107 | 三者相符，但數量仍不足 |

核驗範圍共 1,890,107 個合約日，已驗證日內來源 1,470,972 個合約日；
最後只剩上述一個可執行合約日缺口。這些數量不是 GPU epoch 數。
不能把少一口視為完整日、宣告該分鐘無成交，或補成任意開盤價。
失敗診斷已保留，原 2011 基準目前仍為 **0 個市場訓練 epoch**；
不包含上方獨立 2013 起研究的已完成回合。

13:45 再以 canonical `download_tw_futures_intraday_gaps.py` 做一次有帳戶配額
與期限限制的 CTF 商品日查詢，取得 47 筆該月份 outright，單邊合計仍為 107。
它與舊全市場快取的 H/L/C、量一致，尚未補足缺口；不是重試傳輸就已恢復。
新 Float64 回應與原全市場 Float32 encoding ABI 不同，原件另存，未冒充原 inventory。
收據為 `fresh-ctf-source-probe.json`，原始來源在本機
`data_finmind/futures_intraday/TaiwanFuturesTick/CTF/2012-06-04/`，沒有發布或傳至新機。

下一步必須取得該日完整逐筆，或能核驗官方日量差異的來源更正證據。
[期交所 E-Data Shop 的期貨成交簡檔](https://edatashop.taifex.com.tw/zh/product/detail/40283ab7890b3664018920782ec40004)
列有自 1998-07-21 起的申購範圍，可作官方補證管道；本輪沒有購買。
缺口補證後仍須保留 partial clocks、用 canonical builder 重建、獨立 SHA 讀回並產生
有效 admission，再讓排程接續；不能只建立放行檔或盲目重啟已失敗的準備器。

基準控制器仍在等待 `data-admission.json`；本輪新增的十組控制器已在
Supervisor 啟動，等待兩個基準完成。註冊成功、CUDA 小測通過及排隊中，
均不等於已完成真實市場 DDP 訓練。

歷史期貨紀錄提供下列參考。這次讀的是保存的研究報告，沒有恢復舊 checkpoint
重播；它們不是新雙時鐘 v5 的同資料、同規則、同切片對照。

| 歷史研究，2026-01-02～09-04、163 日 | 淨報酬 | Sharpe | Sortino | MDD 幅度 |
| --- | ---: | ---: | ---: | ---: |
| v8，較低回撤的舊控制 | 32.1893% | 2.35608 | 3.61329 | 7.80952% |
| v10，1 億元、舊候選接受控制 | 55.66% | 2.533 | 3.970 | 12.06% |
| v16，1 億元、舊模型初始化 | 64.1990% | 2.32049 | 3.53369 | 14.6151% |
| v14，1,000 萬元 | 78.7045% | 0.75133 | 1.06417 | 58.0740% |
| TX 近月轉倉，1 倍名目本金、未扣成本 | 54.5166% | 2.02897 | 3.08957 | 17.2172% |
| margin-components，10/04 報告的 102-epoch 執行 | −36.53% | −1.23 | −1.63 | 52.24% |

v8 回撤較低、v10 Sharpe 較高、v16 報酬較高，不能壓成單一「全面最強」；
v14 資金亦不同。v10 當年因候選全被拒而於第 38 輪結束；v16 依驗證選
epoch 189、停止於 289。它們是舊全期貨研究，與本輪股票期貨 universe／時鐘不同。
本輪不載入其權重、optimizer 或舊候選拒絕機制。
來源：[歷史風險控制結果](tw_futures_margin_risk_training_2026-09-28.md)、
[v10／v16 完整結果](tw_futures_fold14_benchmark_training_2026-09-28.md)、
[10/04 components 診斷](tw_futures_components_fold14_performance_2026-10-04.md)。

10/03 components 報告記載同名產物根目錄的另一個 334-epoch 快照（報酬
19.7624%）；它與 10/04 的 102-epoch 紀錄不同。沒有逐 byte 恢復兩份產物前，
不把它們當成同一次執行或合併排名。股票當沖的參數空間 MGDA 也不列為期貨結果。

## 第一性原理：這次改什麼，為什麼

整個流程是「來源與時鐘 → 因果股票／期貨特徵 → 市場策略係數 → 逐日真實帳戶
回饋 → 整口交易及費稅／結算 → 淨值效用 → 反傳 → 完整驗證、測試及產物讀回」。
合法保證金預算、名目曝險、風險、可成交口數，是四種不同的量。

令權益為 \(E\)、單口原始保證金為 \(IM\)、前次已知單口名目金額為
\(N^-\)、有方向的保證金預算為 \(w\)。忽略整口、容量及多商品耦合，
局部有 \(q\approx wE/IM\)。同樣的 score 變化會透過 \(N^-/IM\)
放大成不同的名目曝險。這是優化座標的條件問題，不能靠更換注意力 kernel 解決。

**名目本金座標／Notional-coordinate policy** 使用決策時已知的
\(r=IM/N^-\)，將市場 logit 和全部帳戶回應係數一起乘上 \(r\)。
單商品近零、局部輸出近似線性時，\(dq/ds\approx E/N^-\)。
多商品 entmax、整口及容量耦合仍存在；並未把帳本變成線性問題。
正的座標倍率不另加曝險上限，亦不使用今天尚未發生的開盤／結算。

這不是未曾測過的必勝技巧：舊 v15 在 1,000 萬帳戶曾把 4.27% 保證金預算
分至 12 組，每組都不足一口。新控制已有 1 億本金、單口金額 context 及逐日
真實帳戶回饋，條件不同，才值得重測。工程驗收會逐個保存驗證／測試實際整口
持倉日數、非零要求但沒有部位的日數、全現金旗標與 NPZ SHA；不以短測收益挑選
候選，也不把全現金或非零 surrogate 梯度當成已學會交易的證據。

**較大 AdamW 步長／Larger AdamW step** 單獨比較 1e−5 與 3e−5，
用來分辨有效更新太小與座標不佳。保留同一 scheduler 下限、裁剪與訓練預算。
Adam 的適應性使全域 gradient clipping 不能簡化成固定倍率的 LR 縮減。

**隱藏矩陣正交更新／Muon hidden matrices** 使用現有 PyTorch 原生 Muon，
依 canonical optimizer 的 module-role partition 選隱藏 Linear；輸入投影、
embedding、query、action/cash/account heads 等仍由 AdamW 更新。
使用 `adjust_lr_fn: match_rms_adamw`、matrix/aux LR scale 1。
這是原生形狀／尺度調整，不代表已量到每步 update RMS 完全相同，也不是完整重現
2026 大型語言模型論文的分散式優化器。此模型能否受益，要看實際驗證及總耗時。

Loss 保留單一 `log_utility`：\(L=-252\,\mathrm{mean}[\log(E_t/E_{t-1})]\)。
它直接對應扣費後複利成長，具有小報酬近似下的變異代價，卻不保證最大 Sharpe、
Sortino 或最小 MDD。整口 forward 的真導數幾乎處處為零；目前有訊號的 backward
是明列的 surrogate／recovery 契約，工程一致性不構成無偏性或績效保證。

10/04 虧損執行的算術歸因約 83.7% 來自持倉毛損益、16.3% 來自費稅。
這支持同時檢查方向／曝險的學習與成本回應，不能推論「降低 churn 就必然盈利」，
也不能把此比例套用到 v16 或尚未執行的新模型。

## 原 2011 隊列的固定實驗：仍待來源補證

設定：[tw_stock_futures_conditioning_20261023_v1.yaml](../configs/ablations/tw_stock_futures_conditioning_20261023_v1.yaml)。
兩個既有基準先跑，各五個新候選接續；共 2 個基準加 10 個候選。
這是原 2011 等待隊列，並非目前佔用 GPU 的 2013 v2 六組清單；
後者的設定與進度見本文開頭及「10/23 續修」。

| 每個時鐘的候選 suffix | 策略中英文 | 對該時鐘基準的變更 |
| --- | --- | --- |
| `notional` | 名目本金座標／Notional-coordinate policy | 只開啟座標倍率 |
| `adamw_lr3e5` | 較大 AdamW 步長／Larger AdamW step | LR 1e−5 → 3e−5 |
| `muon` | 隱藏矩陣正交更新／Muon hidden matrices | optimizer partition |
| `notional_lr3e5` | 名目座標＋較大步長／Notional + larger AdamW step | 上述兩者交互作用 |
| `notional_muon` | 名目座標＋Muon／Notional + Muon | 座標與矩陣更新交互作用 |

名稱順序為 `clock0845__<suffix>` 五組，再 `clock0900__<suffix>` 五組。
相同時鐘的比較才是該優化的控制；08:45 與 09:00 亦保留相同 lag-1 股票歷史特徵。

不變項：FinancialTransformer **d32／lookback32**、global batch128、雙 RTX5090
DDP、BF16 AMP、單一 Log Utility、1 億本金、自由多空／可學現金、真實整口、
原費稅／容量／到期／公司行動／跨日結算／破產吸收、相同 seed 與資料。
最後 fold 由已建成特徵的實際 session calendar 決定；維持年度邊界 +32 與
`panel_history`，不硬編 fold 號或把年份標籤當自然年。

每個候選先做四 epoch 的完整市場工程驗收，再以全新 optimizer／權重啟動正式訓練，
不從工程 checkpoint 續訓。正式維持最多 1000 epoch、連續 **100 回合無改善**、
`min_delta=0.0001`。每 epoch 完整驗證／測試、同步畫圖、存 checkpoint；
失敗保留並接續下一組。只有 OOM 才沿既有 capacity retry 降 global batch
128 → 64 → 32，記錄新嘗試與數值軌跡差異。

只用驗證期選 checkpoint 與候選；報酬以淨值計，Sharpe／Sortino 口徑一致，
MDD 記正的幅度。同時報告非支配組合，不把某一項最高稱為全面最好。
2026 測試已被研究觀察；此十組清單在訓練前固定，不依測試結果追加搜索。
新結果必須重算同日期 TX `tx_front_rolling_1x_gross`，不能套用上表舊數字。

## 原始到近期文獻：可用部分與限制

查詢截至 2026-10-23，以下優先採原文／作者稿；2026 arXiv 稿並非全部經同行審查。
閱讀範圍涵蓋效用、直接交易學習、成本結構、時序穩健性與優化器；不宣稱窮盡所有論文。

| 年份 | 原始文獻 | 對本研究的意義／處理 |
| --- | --- | --- |
| 1956 | [Kelly, A New Interpretation of Information Rate](https://onlinelibrary.wiley.com/doi/10.1002/j.1538-7305.1956.tb03809.x) | 複利對數成長基礎；不等於 MDD 或 Sharpe 保證。保留簡單 loss。 |
| 1991 | [Cover, Universal Portfolios](https://onlinelibrary.wiley.com/doi/10.1111/j.1467-9965.1991.tb00002.x) | 與最佳常數再平衡組合比較；無摩擦投組理論不能直接移植槓桿整口帳本。 |
| 1998／2001 | [Moody & Saffell, Reinforcement Learning for Trading](https://proceedings.neurips.cc/paper_files/paper/1998/file/4e6cd95227cb0c280e99a195be5f6615-Paper.pdf)；[Learning to trade via direct reinforcement](https://pubmed.ncbi.nlm.nih.gov/18249919/) | 直接透過交易路徑與成本學習；支持沿用端到端帳戶目標，非另做報酬 MSE。 |
| 2018／2019 | [Bühler et al., Deep Hedging](https://arxiv.org/abs/1802.03042) | 將摩擦與限制放入策略學習；避險研究不構成臺灣期貨 alpha 證據。 |
| 2019 | [Lim et al., Enhancing Time Series Momentum Strategies Using Deep Neural Networks](https://arxiv.org/abs/1904.04912) | 學訊號、配置與成本，風險尺度重要；連續期貨實驗不能替代本帳本。 |
| 2021／2023 | [Imaki et al., No-Transaction Band Network](https://arxiv.org/abs/2103.01775) | 可學的不交易區間以實際已持倉為狀態；下一波輸出結構候選，需新 packet／adjoint ABI。 |
| 2024／2025 | [Vyas et al., SOAP](https://arxiv.org/abs/2409.11321) | 曲率預條件化；目前不混入本輪，以免將 LR、座標與多個 optimizer 同時混淆。 |
| 2025 | [Liu et al., Muon is Scalable for LLM Training](https://arxiv.org/abs/2502.16982) | 隱藏矩陣更新與尺度配置；已選原生 Muon 小模型實驗，收益另驗。 |
| 2026-01 | [Wood et al., DeePM](https://arxiv.org/abs/2601.05975) | 因果延遲、經濟圖先驗及 worst-window 風險效用。先保留既有因果延遲；複雜效用暫不加入。 |
| 2026-03 | [Arzel & Lehdili, Bridging Stochastic Control and Deep Hedging](https://arxiv.org/abs/2603.29994) | 不交易區間的結構先驗／soft clamp；主要為避險問題，不直接宣告提升選時。 |
| 2026-05，9 月修訂 | [Fernandes & Desell, Financially Guided Deep Portfolio Optimization](https://arxiv.org/abs/2605.28853) | 多種金融效用可比較，不能推出混合更多 loss 必然更好；本輪保留 Log Utility。 |
| 2026-07-01 | [Pollok & Robik, End-to-End Parametric Portfolio Policies for Cross-Asset Futures Timing](https://arxiv.org/abs/2607.00475) | 毛績效相近的網路，扣成本後排名可能不同，且沒有普遍勝出；檢查實際換倉與淨值。 |
| 2026-07-13 | [Khona et al., SOAP, Muon, and Beyond](https://arxiv.org/abs/2607.20548) | 公平 LR 轉移須關注 update RMS、成本與記憶體；不套用 LLM 的速度／收益倍率。 |
| 2026-09-28 | [Zhang et al., Universal Dynamic Portfolios](https://arxiv.org/abs/2609.34643) | 動態比較者與非平穩性；理論域與有成本、可放空整口帳本不同，作後續時序控制參考。 |
| 2026-10-01 | [Gang & Kim, Portfolio Choice under General Utility with Transaction Costs and Search Frictions](https://arxiv.org/abs/2610.00998) | 支持持倉／財富狀態回饋的結構思考；原文禁止借貸與放空，不能直接套用其最適性結論。 |
| 2026-10-03 | [Cai & Song, Robust enhanced index tracking under distributional uncertainty](https://arxiv.org/abs/2610.04221) | 下行風險與報酬限制的穩健建模；暫留後續研究，不把不同域理論當作當前實測。 |

若本輪只改善訓練、驗證仍弱，下一個可識別改動是保留簡單效用的可學不交易區間、
較長 recurrent 信用分配，或訓練歷史內的時間分段穩健驗證。
它們尚未實作／排入這十組；尤其期貨逐軌跡分支目前只支援可分解 Log Utility，
不能把股票的 PCGrad／MGDA 或整條 Sharpe／Calmar 切換開關就視為已接上。

## 工程證據與效能邊界

- 設定完整差分、名目座標與帳戶係數梯度、frontier optimizer、trajectory 更新測試：
  **33 passed、4 skipped**；最後設定再驗 2 passed。跳過不算通過。
- 雙實體 GPU 各四組小型合成 exact 帳戶（AdamW／Muon × 原座標／名目座標）：
  **8 組通過**。每組有整口部位，CPU/GPU 口數完全相等、log return 最大差 0、
  head／帳戶回應／encoder 梯度有限非零，optimizer 確實更新參數。
- 此小測為 d8、兩個股票 context、三日、合成價格，不是完整 DDP／BF16 市場訓練，
  也不是獲利結果。全市場工程驗收仍被上游一口來源差異擋住。
- SSH 原預檢選到鏡像預設 Python；已改用現有 Supervisor 原生環境 `/venv/main`，
  十組設定及原始 source inventory 在遠端驗證通過；初次失敗保留。
- 沿用 canonical compact CUDA cache、ledger graph、資金選擇編譯與完整生命週期。
  新機速度仍待實測；以 `epoch_workflow_timing.jsonl` 的
  `complete_epoch_wall_s_max_rank` 計入同步曲線／畫圖，同時保留冷啟動與完整 fold 時間。
  本輪沒有宣稱只有純 GPU 計算瓶頸，亦未用舊機數字代替新機驗收。

## 排程與產物位置

以下為遠端 `/root/stockAgent/` 下的路徑；operations 的小型收據亦回讀至本機。
正式／工程訓練目錄是排程目的地，目前尚無本輪完成的模型。

| 用途 | 路徑 |
| --- | --- |
| 既有 2011 雙時鐘基準 | `artifacts/markets/tw_stock_futures_history2011_20261021_v5/clock__0845`、`clock__0900` |
| 新十組正式產物 | `artifacts/markets/tw_stock_futures_conditioning_20261023_v1/clock0845__<suffix>`、`clock0900__<suffix>` |
| 新十組四 epoch 工程產物 | `artifacts/benchmarks/tw_stock_futures_conditioning_20261023_v1/engineering/<case>` |
| 原資料準備／基準狀態 | `artifacts/operations/futures-migration-20261023/` |
| 原逐筆缺口 | `artifacts/data_preparation/tw_stock_futures_history2011_20261021_v5/clocks/coverage_gaps.parquet`、`source_coverage.parquet` |
| 本輪控制／請求／原生 GPU 小測 | `artifacts/operations/futures-conditioning-20261023-v1/` |
| 歷史 v16 產物識別 | `artifacts/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_warm_policy_v16/`；本輪未恢復其 payload |

新 Supervisor program：`stockagent-futures-conditioning-20261023-v1`。
讀取 `remote-progress.json`、`configuration-acceptance.json`、
`gpu-mechanism-readback.json`；後續正式放行另須 `engineering-acceptance.json`、
`runtime-launch.json`，不能只看 Supervisor 為 RUNNING。
本次獨立讀回見
[live-research-status-readback.json](../artifacts/operations/futures-conditioning-20261023-v1/live-research-status-readback.json)：
新舊請求及 controls SHA 相符、兩個 queue 均 RUNNING 等待、preparation EXITED，
`market_admission_present=false`，新正式 epoch curves 為空。

目前新 queue-request SHA-256：
`a1f6f2665f74c5fe52f3fb2fe84d7854a0224734f360d08bd423c660fd5bb958`。
加入真實整口觀測前的 `b9cf93af284c…` 請求已保留在
`prior-request-b9cf93af284c/`，只重啟本輪等待控制器。
既有基準請求仍為
`aa65e02a18dee45731e4f1f579b7cfa74750d8de598b65d3f7ab698022397595`。
沿用的 frozen source SHA 為
`00c03c35d752febc8ce44cd85149e720ba78ab6362ed1f52bb449a311d9bbec4`，
沒有改寫原 source bundle 或正在等待的基準設定。

所有候選等待原基準正式隊列退出、來源 SHA 重新讀回及同時鐘完整 lifecycle，
才經 canonical GPU lease 獨占兩卡依序啟動。若來源補證失敗，保持等待並保留原因；
若某個基準或工程候選失敗，保存證據並繼續可驗證的其他案例。
整體來源仍為 research-only，既有歷史 revision 與分鐘 VWAP 成交代理的限制未消失。

## 14:50 後續：獨立 2013 起實驗與新機投影比較

本節接續原 2011 排程，不覆寫其來源、日期、模型或請求。
重新向期交所下載 2012-06-04 全市場日報，CTF/201206 成交量仍為 200，
201206/201207 價差為 92、價差對單式欄為 0；與原年檔相同。
扣價差腿後應有 108 口，FinMind 新舊逐筆均為 107；這不是用新官方日報即可修正的分類差。
新的官方 CSV SHA-256：
`c5e3a7ba639ab80c2046d8eb520492e4ad114122fc34259fc0411606fafea219`。
原件保留於 operations 的 `gap-authority-probe/2012-06-04_all.csv`，
不把該差異改成零成交、不調整容量、不新增隔離契約日。

獨立讀回 2013-01-02 至 2026-09-24：1,341,942 個可執行契約日全部 verified；
兩個時鐘均無 missing_source。新增整段 2013 起研究，透過 canonical builder
重新核驗、建立自己的 complete manifests 與 pair identity。
它提供較近期歷史的對照，也讓研究不必一直等待另一個期間的單筆缺口；
**不能稱為已完成使用者要求的 2011 訓練**。

保留原 2011 股票／金融 context 與 factorized manifest。既有
`attach_futures_decision_clock` 先建立 prior、公司行動及完整金融 context，
再依 complete clock 的起日裁切；因此不修改 `data.panel_start_date`，
不重編股票特徵、不補值、不破壞完整特徵日期 ABI。
`expected_first_year`／`split_start_year` 明列為 2013，最後 fold 仍由實際日曆推導。

六組正式研究依序為：

| 案例 | 中文／English | 改動 |
| --- | --- | --- |
| `clock__0845` | 08:45 基準／08:45 control | 同期間原模型，從零訓練 |
| `clock__0900` | 09:00 基準／09:00 control | 同一份前一交易日股票資訊，不同觀測與成交時間 |
| `clock0845__notional` | 名目本金座標／Notional-coordinate policy | 同時縮放 score 與帳戶回應係數 |
| `clock0845__muon` | 隱藏矩陣正交更新／Muon hidden matrices | 輸入／嵌入／輸出頭仍 AdamW |
| `clock0900__notional` | 名目本金座標／Notional-coordinate policy | 09:00 對照 |
| `clock0900__muon` | 隱藏矩陣正交更新／Muon hidden matrices | 09:00 對照 |

全數 d32、lookback32、global128/local64、簡單 Log Utility、相同本金／整口帳務，
上限 1000 epochs、連續 100 回合無改善。工程 optimizer 不捐贈給正式研究。
只選最後一個 fold，明列 `runner.isolate_train_folds: false`，重用 canonical
直接 DDP，省去額外父程序的同一 panel／金融 context 重建。
初次隔離啟動僅做準備、沒有 epoch curve；保留於舊 `abba/`，不納入測速。
受控停止後的七個分離子程序由 canonical `_terminate_process_tree` 清除，
按 PID/create_time 獨立讀回全數退出；新控制器的 stop 亦重用此 helper。
小型新 session 子孫程序停止實測 exit143、零殘留；不宣稱已測物理冷重啟。
設定及裁切／時鐘驗證 18 passed；投影、來源稀疏儲存與設定檢查
43 passed、14 skipped，CPU 測試不能代替 CUDA 或全市場驗收。

### 為何先測投影，再選訓練路徑

目前 `compact_cuda_cached` 只減少 raw packet 重傳，並未消除模型內
日期 × 股票 × 14727 特徵的寬張量展開（新 2011 context manifest：3854 日期、2757 股票）。
線性投影的核心為 `H = XW + b`；若 X 的實際來源值有大量零，
可從完整 schema 的 immutable CSR 直接求投影及其 adjoint，
再只對窄 H 建立 lookback 窗。零欄的 normalization affine 效應仍必須保留，
權重與 embedding 每次更新仍重算，不能快取過期的 learned 輸出。

重用既有 `factorized_compact_projection`、`factorized_sparse_projection` 與其
固定 row reduction、來源 cache，不另寫模型或 trainer。
對照 A 為原 compact raw packet cache；B 同時啟用這兩個旗標與
`candle_projection_fp32`。這是明列的精度／梯度軌跡實驗，非 bit-equivalent 宣稱。
不做特徵選擇，不變動整口、費用、margin、到期／公司行動、破產吸收規則。

在新機每時鐘執行 A1 → B1 → B2 → A2，八組均為完整四 epoch 最後 fold。
比較第 3／4 回合 `complete_epoch_wall_s_max_rank`，也保留完整冷 case wall；
每個 case 的驗證、測試、曲線、同步畫圖、checkpoint、最終 replay 全部保留。
速度比較禁止 OOM 縮 batch 後混入同批對照；這類失敗保留並另作容量診斷。

預先定義放行：四 trial 均通過既有 lifecycle、finite/nonzero gradient、
adjoint replay 為零、穩態無新圖及真實整口觀測；B 的平均穩態時間與
「實測冷四回合 wall + 96 × 穩態」估計均至少少 3%。後者只是 100 回合攤提估計，
**不是實測完成的 100 回合或收斂時間**。兩個時鐘都通過才共同選 B，
否則兩者都保留 A；不讓時鐘比較混入不同數值路徑，也不以短跑績效選技術。

選定路徑會一致套用六組，再各自完成四回合工程檢查，最後從零正式收斂。
正式訓練仍可依原容量例外重試 128 → 64 → 32，保留新 fingerprint；
不同容量下的結果不冒充同 batch 效能對照。
若工程個案失敗，保留診斷並繼續；只有同時鐘基準已通過的候選才進正式比較。

### 本次新增查閱及可移植邊界

延續上表 Kelly／Cover／直接交易學習至 2026 的文獻，這次另核對下列原文。
新論文結果不當作本研究已實測的提升倍率。

| 原始文獻 | 能用的推論；本次處理 |
| --- | --- |
| [Martin & Cundy 2017/2018，Parallelizing Linear Recurrent Neural Nets](https://arxiv.org/abs/1709.04057) | 線性依賴可 associative scan；含整口／margin／違約分支的完整帳戶轉移不直接符合此前提。先平行化外生市場 encoder，保留真實帳本。 |
| [Wang et al. 2019/2020，BPPSA](https://arxiv.org/abs/1907.10134) | backward 的 scan 表示可研究；顯式帳戶 Jacobian 的維度及稀疏結構必須先量到，不能套用其 RNN 倍率。 |
| [Linghu, Liu & Deng，IPMO，2025；2026-09-09 修訂](https://arxiv.org/abs/2512.11273) | 凸多期投組的 mirror-descent fixed-point implicit differentiation 可省 KKT 分解；本帳本無該凸性，不能把近似 backward 稱為精確整口梯度。 |
| [Zhao et al.，ICLR 2026，From Sequential to Parallel](https://proceedings.iclr.cc/paper_files/paper/2026/hash/bc09efb501c801ed92e181e26a885c2d-Abstract-Conference.html) | 跨情境／狀態轉移的 GPU min-plus DP；不是宣告所有跨時間依賴都消失。高維連續 NAV + 庫存不能無成本列舉狀態。 |
| [Eilles-Chan Way et al.，IJCAI 2026，DFL with Lagrangian Decomposition](https://arxiv.org/abs/2606.08797)；[作者實作](https://github.com/corail-research/DFL-LD) | 可分解組合限制與 SPO+/IMLE 供未來 surrogate 比較；其新 loss 與約束子問題不混入本次簡單 Log Utility。 |
| [Li et al.，AlphaLedger，2026-10-16](https://arxiv.org/abs/2610.23744) | 關注扣成本的邊際貢獻、已成熟的狀態條件回饋；原文為中國 A 股因子生成及 teacher distillation，不是本次從零期貨策略的績效保證。 |

再閱讀 No-Transaction Band Network 的 clamp 結構、DeePM 的 worst-window 與
兩遍 microbatching、Cross-Asset Futures Timing 的扣成本排序。
下一輪結構候選仍是「真實已持倉 → 可學不交易區間」；應讓神經網路決定區間，
不是事後替策略硬加手動停損。尚未接入 packet／adjoint ABI，沒有假稱已排入本輪。
既有期貨 trajectory 分支仍只支援 Log Utility，Sharpe／Calmar／MGDA 不能只改名字接上。

### 新排程產物與驗收邊界

遠端 `/root/stockAgent/`：

- 控制與狀態：`artifacts/operations/futures-history2013-20261023-v1/`。
- 獨立來源：`artifacts/data_preparation/tw_stock_futures_history2013_20261023_v1/clocks/`。
- A/B/B/A：`artifacts/benchmarks/tw_stock_futures_projection_speed_20261023_v1/abba_no_isolate/`。
- 六組工程：`artifacts/benchmarks/tw_stock_futures_history2013_20261023_v1/engineering/`。
- 六組正式：`artifacts/markets/tw_stock_futures_history2013_20261023_v1/<case>/`。
- 宣告設定：[2013 六組](../configs/ablations/tw_stock_futures_history2013_20261023_v1.yaml)、
  [投影比較](../configs/ablations/tw_stock_futures_projection_speed_20261023_v1.yaml)。

`stockagent-futures-history2013-20261023-v1` 使用原 Supervisor 和 canonical GPU lease，
不另建 trainer／optimizer／checkpoint／回傳所有者。原來源完成後受控交接，
保留最初只宣告六組的請求 `81f090cc3c57…` 及原準備日誌；新增投影比較後重新釘定請求。
後續移除單 fold 重複父程序的請求為
`6c5396b84f5bba2af65f305e5c224da501523fa007d7120d693831178e08e577`；
前一個 `0c05bb469614…` 與未產生 epoch 的嘗試一併保留。
完整 2013 來源 pair identity 為
`9927cd4742ab3fda87b9732963e466a2f731092603da05aa53824c81639703f5`：
1,705,459 契約日、1,341,942 verified intraday 契約日、缺口 0。
新機已實際啟動 `clock0845__a1` 的兩卡 DDP，來源／adapter 冷準備仍須計入 wall。
第 2 回合完整 workflow MAX-rank 105.233 秒，reported-rank 的訓練約 95.18 秒、
模型 forward 約 61.81 秒、autograd 約 16.65 秒、帳本 loss backtest 約 5.65 秒。
這些是非同步 host 包含時間，並非純 CUDA kernel 時間，不能相加冒稱 GPU 分解；
模型仍為主要觀測成本，投影比較尚待 B 完整實測。
梯度有限且 norm 2.833、zero-gradient batches 0、adjoint replay 誤差 0、該回合新圖 0。
這不是四 epoch 驗收或績效結論。外部 perf 缺對應 kernel tools，py-spy live attach
被容器拒絕；失敗另存 `external-profiler-attempt.json`，沒有冒稱取得函式級 trace。
不把進入工程工作、GPU 上下文或 NCCL 等待的 busy 指標說成已有收斂成果。
應讀 `data-admission.json`、`projection-speed-acceptance.json`、
`selected-training-path.json`、`engineering-acceptance.json` 與 `runtime-launch.json`，
不能以目錄存在、排程 RUNNING 或 GPU busy 取代訓練／績效驗收。

新機兩卡各 12 項小型完整 affine／所有參數 adjoint／快取與共享模型測試通過，
共 24 passed；嚴格 CUDA preflight 同時驗證兩卡實際運算。
鏡像初缺 repo 已宣告的 dev dependency pytest，保留首次失敗，安裝與本機一致的
pytest 9.1.1 後重跑；未更動 Torch、CUDA、模型或帳務實作。
這些小測仍不代表全市場 DDP、速度倍率或交易獲利。
另輸出 `bottleneck-readback.json` 保留穩態各階段觀測；rank0 host stage
不冒稱 MAX-rank／CUDA compute，只有 complete workflow MAX 是效能比較分母。

目前仍有來源準備、冷初始化、寬特徵處理、帳本 recurrence 與報告等成本。
完整新機的市場數據測量尚未完成，**未證明只剩硬體／純計算瓶頸，也未證明超越舊模型**。

## 10/23 續修：日期座標、梯度來源與測速產物生命週期

16:05 重新釘定的請求是
`02855e09a314731aa2af7ba23f311b5629215335ac9b9aa12f48723b45f095f4`。
原始 2011 請求 `aa65e02a18de…` 的單口缺口仍在；2013 完整資料是獨立研究，
不是將 2011 admission 改成通過，也不是刪去缺口日後冒稱 2011 全期間完整。

### 實際完成與尚未完成

| 工作 | 可驗證結果 | 尚不代表 |
| --- | --- | --- |
| v1 A1 原配置 | 完整四 epoch case wall 966.403 秒；完整 workflow epoch3/4 MAX 平均 104.027 秒；非零有限梯度、adjoint replay 0、穩態新圖 0；145 個測試日均有整口持倉 | 收斂、未來報酬、與舊研究同日期排名 |
| v2 A1 原配置 | 完整四 epoch case wall 647.699 秒；完整 workflow epoch3/4 MAX 平均 103.958 秒；測試 29.1801%、Sharpe 1.26689、Sortino 1.83749、MDD 幅度 21.9542%；完整機制與生命週期驗收通過 | 正式收斂、A/B/B/A 已選出更快路徑、整體速度倍率或超越 benchmark |
| v2 B1 稀疏投影 | 完整四 epoch case wall 649.663 秒，含第一次 source build；完整 workflow epoch3/4 MAX 平均 27.743 秒；機制／實際整口 tape／生命週期通過 | 首次建置 fold 已加速、完成重複比較、已達純 GPU 計算上限或收益提升 |
| v1 B1 | 揭露尾端 common shape 25 與 physical block 32 的錯配，沒有 B epoch 被接受 | 稀疏投影已證明比較慢或無法用 |
| 日期與參數梯度修復 | 獨立原始日曆 dense oracle；兩種 norm、巢狀裁切、首尾不完整 block、股票與日期重排／重複；兩卡 native 各 16 passed，direct-CSR 各 16 passed | 全市場 B 已過四 epoch／吞吐驗收 |
| 共用契約回歸 | 81 passed、20 CUDA-only skipped；遠端 CUDA 小測另列，不以 CPU skip 當 GPU 通過 | 整個 repository 全測完成 |
| 資料移位 | 四個指定 prepared 根透過 canonical move owner 原 inode rename，原來源路徑保留相容連結，已核對舊 pins 與原 frozen source | 額外來源複本、來源修訂、冷庫刪除 |
| 已完成／作廢暫存 | 第一批舊測速與 profiling runtime 回收 168,419,328 allocated bytes；後續零 epoch 暫存回收 8,323,072 bytes；保留精簡診斷及量測 | 正式模型刪除或 NAS／冷庫回收 |

A1 的報酬／風險數字是本輪刪除前讀取 canonical `fold_12/metrics.json` 的結果；
該模式並不持久化 `best_val_backtest.npz`。工程 reader 已改為遵守其 canonical
completion contract，另外核對實際存在的 test／deployment 整口 tape；
驗證 loss 仍逐 epoch 核對，沒有拿 test tape 假稱 validation tape。
本次對照的 test 是 2026 年度切片內的 **145 個有效日**，與舊研究 163 日不同。

### 根因與第一性原理的實作

`FactorizedPanelFeatures.slice_dates` 保留原 manifest 的 physical starts，
而 base/common 已裁成局部日期。原 CSR reader 卻將 physical start 直接索引
局部 common，且 individual rectangles 未按可見區間裁切，可能先錯對日期，
最後才因 25／32 shape mismatch 報錯。修復採用同一個區間交集：

`physical block ∩ [row_offset, row_offset + visible_dates)`，

再分別換成 source-local 與 rectangle-local 索引。查找 block 時先將可見 row
加回累積 offset，不能僅補尾端零值或縮短 common 來掩蓋錯位。
正規化 row store 升為 `full_schema_csr_affine_stable_date_view_gpu_resident_v3`，
sparse-only 訓練契約為 `full_schema_csr_fixed_row_fp32_date_view_forward_v4`。
CSR 關閉的舊 optimizer fingerprint 保持原值；新 sparse 不接舊 optimizer。

之後保留已存在的 full-schema affine 投影，專門配置三層不可變 row cache：
每 rank **12 GiB GPU／40 GiB host**，加上 source-view／normalizer 綁定的
SHA-verified CSR disk cache。80 GiB host cache 前先確認主機可用 RAM 至少
140 GiB；這是本節點候選容量，不是其他模型的最佳常數。
direct CSR transpose adjoint 使用既有 cuSPARSE backend、algorithm 6，省去
PyTorch native sparse transpose 的格式轉換；它有自己的 numerical contract，
兩卡參數梯度 oracle 均通過後才加入新的完整 workflow 比較。

另發現各 case 的預設 transform cache 落在私有 attempt，會重新擬合相同的
訓練期 normalizer。已完成的兩份 training-only RMS cache 經 canonical
`_load_training_transform_cache` 驗證、`_store_training_transform_cache` 耐久寫入，
再逐 payload 與 SHA 讀回至 `data_*` 共用 cache。沒有捐贈模型、optimizer 或
將 test／validation 用於 fit；正確的 source／train-index key 決定能否命中。
該次零 epoch 冷準備的 pre-epoch 累積 205.514 秒只是原配置觀測，
後續同一 A1 已命中共用 cache，pre-epoch 累積 **5.998995 秒**，
此單對觀測的前置階段比為 **34.258 倍**；未隔離其他 warm-cache 效果，
不能全算作 normalizer 時間，更不能外推成 epoch／fold 的提升倍率。
原始前置列與 cache payload 讀回保存在
`shared-transform-setup-measurement.json`；已完成的 GPU 單元測試 payload、
私有測試複本與本輪隔離 build worktree 也已清理，來源測試及固定 release 保留。

完整舊 A1 的 reported-rank 非同步 host 欄位顯示：模型 forward 約 2.66 秒／批，
帳本 backtest 約 0.21 秒／批，optimizer step 約 0.041 毫秒／批。
目前先移除寬投影／傳輸／重建的重複成本，比優化 optimizer kernel 有較大機會。
這些區間互含且沒有同步，不能相加、拿零 CUDA-event 欄位當成零運算時間，
也不能從它們算硬體 roofline 或精確 Amdahl 倍率。
後續依完整 MAX-rank、有效日期／秒、CUDA trace 與實際傳輸 bytes 決定下一個改動；
如果帳本成為主因，再處理其順序深度／VJP，不能以高 NVML busy 當完成標準。

### 排程、固定來源與資料位置

新訓練 source SHA：
`def65564b57bf02375162793db633c8352ad3adffaa0ecabe70f02dff6d6707a`。
以原始 source bundle 精確覆寫隔離 build tree，再只加入四個指定 canonical 修復；
未將本機其他新聞／儲存／網站 dirty work 帶進遠端。新 receipt 驗證 1,827 個
frozen code/config 檔；原 remote root 的 1,826 個原固定檔另行驗證，未覆寫。
build staging 的 shell 原為 0600，第一次排程在啟動前被拒；已保留失敗收據，
只補 canonical shell runner 的 owner execute bit，bytes 與 SHA 不變，並加啟動前 gate。
該次沒有市場 epoch，暫存也已按使用者要求清除。

v2 仍為 d32／lookback32、global128／local64、eval16、BF16＋TF32、簡單 Log Utility、
一億元整口跨日帳戶，最後一個 fold 12；訓練 2013–2024、驗證 2025、測試 2026。
來源有 `research_only=true`／`historical_point_in_time=false` 的財務特徵標記，
本次沒有把這個研究邊界改稱已取得全歷史 PIT 證明。
六組為兩個時鐘的原配置／Original policy、名目本金座標／Notional-coordinate policy、
隱藏矩陣正交更新／Muon。先同輸入 A/B/B/A，再六組工程，才從零正式訓練；
仍為 cap1000、連續100回合無改善、min_delta0.0001，不由四 epoch 檢查判定收斂。

遠端 `/root/stockAgent/` 下：

- 資料與時鐘：`data_tw_index_futures/training_prepared/`。
- 新 panel／transform cache：`data_tw_index_futures/training_cache/history2013_20261023_v2/`。
- normalized CSR：`data_tw_index_futures/training_prepared/normalized_csr_20261023_v3/`。
- **正式模型／曲線／報表**：`artifacts/markets/tw_stock_futures_history2013_20261023_v2/<case>/`；完成工程前不宣稱已有正式 checkpoint。
- 控制與精簡收據：`artifacts/operations/futures-history2013-20261023-v2/`。
- 執行中的 A/B/B/A：`artifacts/benchmarks/tw_stock_futures_projection_speed_20261023_v2/abba/`。
- 工程暫存：`artifacts/benchmarks/tw_stock_futures_history2013_20261023_v2/engineering/`。

每個完整測速／工程階段結束後，先保存 timings、curves 數值、metrics、失敗摘要及
機制收據，再用原 `training_layout` 清除指定內部 alias，經原 explicit-testing
disposition owner 的 process／pin／service／inode／quarantine gates 後刪除暫存。
回收數字取實際 `st_blocks` 及最後 inode link 讀回，不搬走或刪除正式模型／來源／冷庫。
舊 v1 benchmark 本體已清理；本文件較早的 v1 路徑是歷史執行位置。
模型只依驗證期選 checkpoint；多次看過的 2026 test 屬研究期，仍不能當全新盲測。

### 新補的 2026 年 7–10 月原始論文

| 原文 | 對當前實作的判斷 |
| --- | --- |
| [Reassessing Muon for Matrix Factorization，2026-07-14／08-01](https://arxiv.org/abs/2607.13246) | 受控低秩問題未顯示一致勝過調妥的 AdamW；保留 AdamW 控制、相同驗證規則，不先宣稱 Muon 提升收益。 |
| [MeqMuon，2026-09-28](https://arxiv.org/abs/2609.35701) | 行列尺度平衡值得後續研究；目前 optimizer 不是觀測到的大成本，且 LLM 結論不能保證股票期貨表現，尚未放進此六組。 |
| [Small-Batch Noise Restores Simplicity Bias of Muon，2026-10-12](https://arxiv.org/abs/2610.16868) | 本模式整條 trajectory 才更新，microbatch 不等於有獨立小樣本噪音；不照抄 shuffle／加噪音，也不破壞帳戶時序。 |
| [BitSparse，2026-10-15](https://arxiv.org/abs/2610.22256) | 原文針對 ReLU／ReLU² 稀疏 activation，現用 SwiGLU 不自動符合；保留預配置／無 host sync 的設計思路，未偷改 activation 或照抄其 LLM 顯存倍率。 |

從 Kelly／直接交易學習到近期期貨策略文獻，較一致的可檢驗方向是成本、持倉狀態、
尺度條件及時序泛化，沒有單一套件能保證跨越數倍風險調整報酬。
目前先取得六個可比較的收斂結果；下一個結構候選仍是神經網路學習的不交易區間，
需把實際持倉接到輸出與完整 adjoint 契約才可排正式比較，現在並未假稱已實作。

## 16:55 續修：減少每批次 CPU 同步，追加四組條件與步長實驗

### 從當前量測定位下一個候選

寬特徵投影改善後，B1 第 3／4 回合的 reported-rank host 欄位顯示，
`train_finite_check_ms_per_batch` 約 593–600 毫秒、autograd 約 414 毫秒。
這些非同步區間互相包含 GPU 等待，**不能把 600 毫秒當成能全部刪除的運算**。
它指出一個應驗證的同步點：整條市場 trajectory 才更新一次參數，卻每個
microbatch 把 loss／gradient 的有限性旗標 `.item()` 讀回 CPU。
[PyTorch 2.11 CUDA semantics](https://docs.pytorch.org/docs/2.11/notes/cuda.html#asynchronous-execution)
說明了這種非同步排程與同步計時的區別。

已在既有 canonical trainer 加入選用的 **裝置端軌跡有限性檢查／Device-side
trajectory finite guard**。每個 batch 仍檢查 loss 及累積梯度，GPU 上 sticky
旗標保留第一次異常；即使後來清空梯度，異常也不能被抹掉。僅在唯一 optimizer
update 之前讀回，DDP 先以 MIN 合併旗標，任一 rank 出錯則兩個 rank 均拒絕更新。
原 finalizer 的裁剪、更新前後檢查仍保留，不快取 learned outputs。
只對完整 trajectory、無啟用 GradScaler 的路徑開放，預設關閉。

另外發現每批次進度 log 的 `loss.detach().cpu()` 仍會同步；僅移動 finite check
不能消除此等待。因此新專用版本也將每批 loss 在 GPU 上 detach/clone 保存，
只於 trajectory 邊界一次讀回並輸出全部逐批紀錄。clone 保護 CUDA Graph 可能
重用的 scalar buffer；逐批時間明列為 `enqueue_elapsed`，避免當成裝置實際耗時。
曲線、loss 數值、驗證／測試、checkpoint 及正式產物工作均保留。

新增程式：[finite_checks.py](../stockagent/training/finite_checks.py)；
環境開關 `STOCKAGENT_TRAJECTORY_DEVICE_FINITE_GUARD`。
啟用才將 `per_batch_device_flags_before_trajectory_update_v1` 寫入 optimizer
契約；關閉時舊 fingerprint 不變。現有父排程／舊 frozen source 不被覆寫。

本機完整重跑 **81 passed、3 CUDA-only skipped**：涵蓋無 host scalar
觀測、有效軌跡參數更新逐 bit 相等、中途 NaN／Inf、梯度清空後仍拒絕、
雙程序 Gloo 任一 rank 異常時雙方拒絕，以及既有期貨 trajectory／carry／runtime
回歸，以及三個 batch loss 完整保留、在最後邊界只做一次進度 CPU 讀回。
測試及 log 保存在新 v3 operations；沒有把 CPU 結果當作遠端 CUDA 通過。
新 frozen source 已核驗 1,828 個 code/config 檔，source SHA：
`aed5c44399c0e68e8f8b8320d67a748bde42cff04aa266b20cfd236b3e58829d`。
先前只含 finite guard 的 `0b9d28565dd3…` source 保留；其尚未執行的後續
Supervisor 受控停止、重新釘定新 source／request 後才交接，父 v2 不停止或覆寫。

新 Supervisor **`stockagent-futures-conditioning-history2013-20261023-v3`** 已註冊、
獨立讀回 RUNNING，request/source SHA 均匹配；當前明確狀態為
`waiting_for_parent_formal_lifecycle`，**沒有啟動新 guard GPU 測試或正式 epoch**。
17:08 新版重新註冊後已獨立核對 RUNNING、request/source pins 全數匹配。
它會等待父 v2 六組正式流程退出、核驗同時鐘完整基準，再取得 canonical GPU lease，
先 strict CUDA preflight 及雙 RTX5090／NCCL 的有效／NaN／Inf／清空梯度 oracle，
再做 guard A/B/B/A 各四 epoch 的完整 workflow。

只有四組皆通過機制、整口 tape、生命週期，且穩態 MAX-rank 及攤提 100 回合
估計均至少快 3%，才讓後續候選使用新 guard；否則沿用原檢查。
這是完成專用非同步版本後的實測門檻，尚無 GPU 加速倍率，也沒有宣稱只剩計算。
尚待量測的主要邊界是 sparse transpose／窄投影的實際裝置時間、帳本 recurrence
及 VJP、CPU launch/sync 和每 epoch 完整報告。應以 CUDA trace／events 及完整 wall
驗證；單一 host 欄位或 NVML busy 不足以排除上述成本。

### 已固定並接入遠端的四個後續研究

現有 v2 六組提供每個時鐘的原座標 AdamW、名目座標 AdamW、原座標 Muon。
新增以下四組，區分學習步長與「座標 × optimizer」交互作用，不更換 seed 搜索。

| 案例 | 中文／English | 相對同時鐘原基準 |
| --- | --- | --- |
| `clock0845__adamw_lr3e5` | 較大 AdamW 步長／Larger AdamW step | LR 1e−5 → 3e−5 |
| `clock0845__notional_muon` | 名目本金座標＋正交矩陣更新／Notional + Muon | 兩個已定義改動的交互作用，LR 1e−5 |
| `clock0900__adamw_lr3e5` | 較大 AdamW 步長／Larger AdamW step | 09:00 同上 |
| `clock0900__notional_muon` | 名目本金座標＋正交矩陣更新／Notional + Muon | 09:00 同上 |

設定：[2013 四組後續研究](../configs/ablations/tw_stock_futures_conditioning_history2013_20261023_v3.yaml)。
四份 canonical expanded config 已逐一核對時鐘、LR、optimizer、無 pretrained、
resume=false；資料／帳務／簡單 Log Utility／d32／lookback32／global128／local64／
eval16／cap1000／patience100／min_delta0.0001 與 v2 相同。
四組各先四回合完整工程驗收，再從零正式訓練；不捐贈工程權重或 optimizer。
失敗保留診斷並繼續其他可驗收組，模型只依驗證期選擇。

遠端 `/root/stockAgent/` 下的新增位置：

- **正式模型、曲線、checkpoint**：`artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3/<case>/`。
- 控制、狀態、驗收、GPU oracle 數值：`artifacts/operations/futures-conditioning-2013-20261023-v3/`。
- 暫時 guard 測速：`artifacts/benchmarks/tw_stock_futures_device_finite_guard_20261023_v3/`。
- 暫時四組工程：`artifacts/benchmarks/tw_stock_futures_conditioning_history2013_20261023_v3/engineering/`。
- 資料、時鐘及 CSR 仍共用上述 `data_tw_index_futures/training_prepared/` 與 `training_cache/`。

新固定 request SHA：
`fa158051ccca7b4de42daa1c6fc26af898fcf2c1e7261c1b05b4c855764cfb3a`。
先前未執行的 `19535d518c8b…` 請求及 source 收據另存，沒有用改動後程式
冒充原固定來源的測速。
所有完成的測速／工程 payload 仍先保存精簡數值與失敗診斷，再交原 disposal owner；
正式模型、來源及 frozen release 不在此刪除範圍。

本輪完成的本機兩個 pytest basetemp 已經逐檔 SHA、process/config reference 及
quarantine 讀回後交 pytest owner 清除；隔離 build worktree 已核對 frozen source
及保留 bundle，交 Git worktree owner 清除。其刪除前 regular-file allocated bytes
分別為 16,384、8,192、81,047,552；精簡收據與測試程式保留。
operations 的本輪 compiler cache 另經原 explicit-testing disposal owner 回收
28,672 allocated bytes，沒有刪除其他來源、正式模型或冷庫。
收據：[暫存回收](../artifacts/operations/futures-conditioning-2013-20261023-v3/temporary-retirement-result.json)。
含延後進度的新版本在 81 項測試通過、bundle/SHA 核對後，亦已清除其第二個隔離
worktree 及第三個 pytest basetemp；刪除前 regular-file allocated bytes 為
81,223,680 及 24,576。原始測試程式、固定 release 與精簡驗證 log 均保留。
收據：[完整非同步版本暫存回收](../artifacts/operations/futures-conditioning-2013-20261023-v3/with-progress-temporary-retirement-result.json)。

### 文獻與既有實驗如何影響這次選擇

重新檢查 v8／v10／v16、10/03–10/04 components 與 causal account-policy 報告：
目前已經有 16 維實際庫存／NAV／成本／風險回饋，不再重寫另一個「持倉回饋」模型。
v16 的高報酬部分伴隨較高 gross exposure；舊結果沒有所有指標同時支配 v8/v10。
與新 145 日切片比較前，必須同資料、同時鐘、同資金與費稅重播，不能拿舊 163 日
數字當作新研究已輸贏的證據。

新增核對的 sparse 原文與已有交易論文給出的可檢驗方向為：

| 原文 | 對本輪的具體處理 |
| --- | --- |
| [Design Principles for Sparse Matrix Multiplication on the GPU，2018](https://arxiv.org/abs/1803.08601) | 先減少格式轉換、改善 row load balance／記憶體合併；目前重用 CSR 與 direct transpose，下一步須量實際 row 分布及窄輸出形狀。 |
| [AsyncSparse，2026-04-20](https://arxiv.org/html/2604.17834v1) | 原文以 Hopper H100、TMA／WGMMA、WCSR／BCSR 及較寬輸出量測；本案 RTX5090、d32 不能直接套其 kernel 或 6.24× 數字。保留重疊搬移與負載平衡原理，padding 可能反而增加工作。 |
| [Cross-Asset Futures Timing，2026-07-01](https://arxiv.org/abs/2607.00475) | 16 個 CME 期貨的成本會改變網路排序，沒有所有子資產都勝出的模型；本案仍以實際整口扣成本淨值與時序泛化驗證。 |
| [No-Transaction Band Network，2021／2023](https://arxiv.org/abs/2103.01775) | 下一個有結構依據的研究是從實際已持倉學習不交易區間；尚未實作／排入這四組，避險最適性亦不等於投機 alpha 保證。 |

當前可接受結論是：08:45 已量到完整穩態約 3.74 倍的工程改善；新增四組與
非同步有限性檢查已實作並排入遠端。尚未完成 09:00 比較、正式收斂或證明優於
歷史模型，2011 缺口與財務特徵非全歷史 PIT、分鐘成交代理的研究邊界均仍存在。
