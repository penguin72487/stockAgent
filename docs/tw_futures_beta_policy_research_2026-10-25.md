# 期貨市場依賴性、完成結果與先驗 beta 輸出實驗

2026-10-25，遠端 `114.34.26.236:43269`，兩張 RTX 5090。
本報告接續[10 月 24 日相對價值研究](tw_futures_relative_value_research_2026-10-24.md)，
保留當日快照，不把原本「排隊」改寫成當時已完成。

20 組正式案例現在都已依「連續 100 回合無改善」完成；原四個 queue 正常退出。
獨立以保留來源及逐日 tape 核對 lifecycle、財務指標與商品範圍，
見[完整20組表格與曲線](../artifacts/markets/tw_futures_beta_policy_20261025_v1/COMPARISON.md)、
[來源與依賴性讀回](../artifacts/operations/futures-alpha-research-20261025-v1/completed-dependence-audits.json)。
這些是研究回測，尚未證明可部署或實盤獲利。

## 已完成結果

全部以下數字使用同一 145 個 test 日期：2026-02-26～2026-09-24。
策略含原定費稅、容量、保證金、整口及跨日帳務；相關性是**簡單日報酬**，不是累積資產水準。
不同訓練起年、商品集合或座標組合，不能當成單因素因果比較。

| 案例／Strategy | 訓練起年 | 報酬 | Sharpe | Sortino | MDD 幅度 | TX相關性 | 實現 beta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 09:00 名目座標＋Muon／Notional coordinates with Muon | 2013 | 24.28% | 1.803 | 2.618 | 12.76% | 0.780 | 0.477 |
| 09:00 名目座標＋AdamW／Notional coordinates with AdamW | 2013 | 23.59% | 1.783 | 2.581 | 12.48% | 0.777 | 0.467 |
| 09:00 擴大股票／ETF／指數集合／Expanded equity universe | 2020 | 45.72% | 1.223 | 1.838 | 28.25% | 0.880 | 1.386 |
| 08:45 自由多空名目座標／Unconstrained long-short | 2020 | 15.60% | 1.386 | 1.987 | 11.61% | 0.767 | 0.406 |
| 08:45 名目本金中性／Prior-notional neutral | 2020 | 2.61% | 0.708 | 1.034 | 4.49% | 0.458 | 0.084 |
| 09:00 名目本金中性／Prior-notional neutral | 2020 | -2.55% | -0.878 | -1.212 | 6.77% | -0.569 | -0.085 |
| TX近月持有轉倉，1x gross／TX rolling gross benchmark | — | 30.72% | 1.360 | 2.039 | 17.22% | 1 | 1 |

TX 是既有 `tx_front_rolling_1x_gross`，並非同本金／同費稅／整口保證金帳戶；
也不是股票當沖 benchmark。新09:00 Muon 的 Sharpe 與 Sortino 略優於先前最佳，
但 MDD 略差，報酬仍低於這條 TX 毛基準，沒有全指標支配者。
最高報酬45.72%的 beta 為1.386，不能只用報酬宣稱找到更多獨立 alpha。

目前最高 Sharpe 的完整遠端資料夾：

```
/root/stockAgent/artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3/clock0900__notional_muon/fold_12/
```

本機[曲線索引](../artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3/CURVES.md)
已含四組；完整 checkpoint 仍在遠端相同位置。
這次讀回20組共520個檔案、400張PNG、195,739,638 bytes；
遠端傳送前後雜湊與本機一致，PNG皆通過解碼。
[交付收據](../artifacts/operations/futures-alpha-research-20261025-v1/plot-delivery-receipt.json)
沒有宣稱 checkpoint 已完成冷儲存或全量回傳。

## 為何多種商品仍像 benchmark

寫成每日近似分解：`r_strategy = beta * r_TX + residual`。
商品多只增加選擇，不會自動消去共同市場因子。最高 Sharpe 組雖持有過183個商品，
日報酬與TX的相關性仍0.780；其單因子 OLS 的 R² 約0.609。
這是同期線性解釋比例，並非因果歸因或未來預測力。

單純 log utility 不獎勵與TX獨立。如果上漲市場提供較容易的報酬來源，
模型有理由使用它。把固定 benchmark return 從 loss 減掉，對參數梯度是零，
不能迫使策略改學殘差；參見[早期v16核對](tw_futures_v16_dependence_and_fold1_2026-09-28.md)。

上一批名目本金中性只限制 `sum(n_i)=0`；真正的市場因子限制近似是
`sum(beta_i * n_i)=0`。不同股票、ETF、指數與到期月的 beta 不同，兩式不等價。
09:00 中性組的負相關與虧損也表明：得到相反曲線不等於得到有價值的新策略。
08:45 中性組明顯縮小風險，但報酬和 Sharpe 一起下降，尚無強殘差訊號證據。

保留已存在的實際庫存／NAV／成本回饋，不以零梯度或帳務bug解釋所有泛化落差。
舊股票當沖 MGDA 與這批期貨的來源、持倉與帳務不同，仍不列作同條件排名。

## 商品範圍：仍非全部可交易台幣期貨

目前新實驗重用通過來源檢查的 TWD 股票、ETF、指數子集合。
來源歷史目錄765代碼，其中已准入725個：682股票、27 ETF、16指數。
這是跨歷史代碼數，**不是今天上市家數**；145日test實際有375個可用商品，
股票對照則337個。另有40代碼、132,622來源列尚未通過原來源／規則准入。
共同完整擴大集合的實驗區間始於2020-03-23，終於2026-09-24；
2011的 CTF 日期108對107缺口及9月24日以後的新鮮度，不能由本輪訓練證明。
[本次原來源准入與實際Parquet商品類別讀回](../artifacts/operations/futures-alpha-research-20261025-v1/universe-live-readback.json)
再次確認上述725個歷史代碼，不以目錄名稱推論全部商品完成。

今日重新查閱[期交所商品目錄](https://www.taifex.com.tw/cht/2/products)，
其範圍也包含臺幣黃金、布蘭特原油及其他股價指數。
[臺幣黃金](https://www.taifex.com.tw/cht/2/tGF)、
[布蘭特原油](https://www.taifex.com.tw/cht/2/bRF)
不能因為模型可收更多槽位，就跳過各自的歷史保證金、乘數、費稅、
到期、交易時段、決策時點成交與容量證據。BRF、TGF、GBF等仍需由原來源owner修復准入。
本次六組 request 保留 `all_twd_training_admitted=false`，不冒稱全市場完成。

新beta觀測本身經先驗時序驗證；整個原寬特徵集合的 manifest 仍標記
`research_only=True, historical_point_in_time=False`。
把已完成日資料向後移，不會自動補出所有基本面資料當時真正發布的時間戳。
完整PIT與全商品資料准入是獨立未完成邊界。

## 本輪實作與六組實驗

保留簡單費後 log utility、原本金、模型寬度、lookback、費稅、容量、整口、
跨日與最後fold。這批沿用當前期貨樣板的 d32／lookback32／global batch128，
不是較早股票當沖的d128樣板，也沒有拿舊模型微調。

新增 `futures_beta_notional_cap`，與原淨名目本金限制互斥。
每個決策日只用之前已完成、同契約且有觀測證據的日報酬，
以32列滾動OLS估計個別契約對TX的 beta，8個pseudo-observations收縮向1。
少於兩筆、無TX證據或市場變異近零時使用明示先驗1，這是估計假設，
不把缺失交易價格或報酬假裝成觀測。所有新值均只作策略投影，不改原帳本報酬。

將模型保證金權重 `u` 轉成先驗名目本金／NAV：`n_i = u_i * N_i / M_i`。
令 `e = sum(beta_i*n_i)`；以 `beta * (e-clamp(e,-cap,cap)) / sum(beta²)`
移除超出上限的分量，再只向下縮放，以保留原學得的L1預算上限。
不強迫滿倉。投影在原市場輸出及**實際帳戶狀態回饋後**使用同一共用函式。
整口取整、殘留持倉、容量、估計誤差與盤中變動，仍可能使**實現**beta或相關性偏離零。

| 案例 | 中文／English | cap | 優化器 |
| --- | --- | ---: | --- |
| beta0_0845 | 08:45 先驗台指beta中性／Prior TX-beta neutral | 0 | AdamW |
| beta025_0845 | 08:45 限制先驗台指beta／Bounded prior TX beta | 0.25 | AdamW |
| beta0_muon_0845 | 08:45 beta中性＋Muon／Prior TX-beta neutral with Muon | 0 | Muon |
| beta0_0900 | 09:00 先驗台指beta中性／Prior TX-beta neutral | 0 | AdamW |
| beta025_0900 | 09:00 限制先驗台指beta／Bounded prior TX beta | 0.25 | AdamW |
| beta0_muon_0900 | 09:00 beta中性＋Muon／Prior TX-beta neutral with Muon | 0 | Muon |

兩時點仍使用相同歷史資訊。沿用第一筆真實時段開盤成交／尚未開盤時最近已知真實成交的
開盤參考規則與來源證據，不把參考價當可成交價格。
先驗 beta 只用投影，不增加模型可學參數；此批不另加複合loss或PPO。
原六組free／net025／neutral是相同2020資料的對照，避免重跑已完成的相同設定。

每組先完整四回合工程，通過有限非零梯度、帳務、lifecycle、曲線、
無穩態重編譯、實際多空整口與回饋後 beta cap 檢查才進正式1,000回合上限。
正式連續100回合無改善停止，`min_delta=0.0001`；失敗保留證據並繼續其他組。
驗證集選checkpoint，2026已檢視test只作探索。未來跨多個年度fold與成本壓力測試，
必須另行完成，不能以這次單一研究期證明泛化。

## 機制與效能證據

擴大回歸425項測試通過（62.27秒）：獨立NumPy投影oracle、有限差分adjoint、
未來擾動不影響當前觀測、缺失／常數市場先驗、原執行張量不變、
實際帳戶回饋後限制、多空整口、序列化與stitched replay、舊checkpoint拒絕混用。
新packet ABI為v3；停用新選項時原v1/v2與舊fingerprint保留。
測試log留在[操作產物](../artifacts/operations/futures-alpha-research-20261025-v1/expanded-regression.log)。

beta在準備階段以FP64 prefix sums一次計算，時間與空間皆 O(T×S)，
不建立S×S共變異矩陣，不逐日呼叫CPU回歸、不增加Transformer forward。
逐步投影是O(S)，進入既有編譯帳務／adjoint路徑；不複製另一份金融loss。
保留已驗證CSR常駐與cuSPARSE transpose adjoint，金融觀測及累積值保持FP32／FP64。

先前六組完整穩態最大rank訓練約11～12.5秒／epoch，含驗證、測試、繪圖等約12.17～13.53秒。
epoch曲線的部分stage timer並未CUDA同步，不能把它們當成純kernel時間或相加做GPU分解。
2020資料比2013資料短，不能把27～29秒降到12秒宣稱成2倍演算法加速。
先前真正同資料CSR比較3.736倍保留在[條件化報告](tw_stock_futures_conditioning_2026-10-23.md)，
不是本輪新收益。finite-guard ABBA僅0.34%，未達3%選用門檻，繼續停用。

目前仍未證明只剩硬體計算瓶頸。冷啟動、資料準備、編譯、繪圖及回傳仍有軟體成本；
不能由NVML利用率或四回合工程宣稱到達計算極限。
這次新路徑的完整GPU工程會保留每回合實測與接受／拒絕理由。
後續優化須使用相同資料、同形狀、同完整工作流程的對照，再決定是否採用。

第一組08:45 beta中性已完成四回合並獨立通過上述驗收，
145個test／deployment日皆有整口持倉且同時持有多空；
回饋後先驗beta曝險最大絕對值1.1484e-6，穩態完整epoch均值13.2814秒。
這只是工程結果，未拿四回合的報酬做正式排名。
[第一組讀回](../artifacts/operations/futures-alpha-research-20261025-v2/first-case-engineering-readback.json)。
08:45 cap25%組亦完成四回合並通過獨立驗收，145日皆有多空整口，
先驗beta曝險最大0.24951，完整穩態epoch均值13.3684秒。
[第二組讀回](../artifacts/operations/futures-alpha-research-20261025-v2/second-case-engineering-readback.json)。

CPU診斷另保留負面證據：3850×1936、lookback32的初測並未顯示prefix實作比預先轉換資料的
window oracle更快；兩者計時邊界不同，不能當公平加速比。
完整相同估計函式的連續時間軸配置ABBA亦未顯示穩定收益（且有冷頁面波動），未選用。
另測分塊prefix版本，避免只從漸近複雜度判定實際效能；候選碼與原始計時均保留，
沒有用這些CPU準備微測速代替完整GPU工作流程。
[原始診斷](../artifacts/operations/futures-alpha-research-20261025-v1/beta-estimator-benchmark.json)、
[連續配置ABBA](../artifacts/operations/futures-alpha-research-20261025-v1/beta-estimator-layout-abba.json)、
[64／128／256欄分塊診斷](../artifacts/operations/futures-alpha-research-20261025-v1/beta-estimator-tiled-screen.json)。
分塊128欄兩次1.75／1.68秒且逐位元一致，但尚無配對完整fold證據，沒有換掉已凍結的正式來源。

## 基礎到前沿文獻：可借用的部分與限制

本輪查閱相關原文，不宣稱窮盡全部文獻或直接復現其報酬。

| 原始來源 | 對本研究的用途 | 本輪決定 |
| --- | --- | --- |
| Gatev等，Pairs Trading，1999 working paper／2006 | 相對價格價差與配對交易的早期基礎 | [NBER](https://www.nber.org/papers/w7032)本次403，未假稱重新取得全文；不硬套固定配對 |
| Avellaneda與Lee，Statistical Arbitrage in the U.S. Equities Market，2008稿／2010 | 先移除共同因子，再研究殘差；成本不可省略 | [作者原稿](https://math.nyu.edu/~avellane/AvellanedaLeeStatArb071008.pdf)，支持先區分共同因子與商品數量 |
| Guijarro-Ordonez等，Deep Learning Statistical Arbitrage，2021／2022版本 | 可學訊號與配置共同最佳化，從殘差組合映回可交易資產 | [原文](https://arxiv.org/html/2106.04028v2)；下一階段可測殘差信號輸入，尚未實作成本文方法 |
| Belyakov，AlphaZeroBeta，2026-09-20 | 政策投影、費用與benchmark依賴性一併檢查 | [原文](https://link.springer.com/article/10.1186/s40854-026-00955-4)；其美元中性投影加相關性reward不等於本文beta投影，不直接採用其複合reward或PPO |
| Chang，Amortized Neural Filtering for Calibrated Posteriors in Statistical Arbitrage，2026-10-19 | 保留殘差狀態估計的不確定性；避免把估計最樂觀者當真正最佳 | [原文](https://arxiv.org/html/2610.25802v1)；支持研究收縮／校準，但本文8筆先驗不是該論文復現；不把韓股結果當台灣期貨證據 |
| Paluszkiewicz，From pixels to profits，2026-10-23 | 聯合股票／因子避險／殘差訊號，結果依賴beta估計與交易成本 | [原文](https://link.springer.com/article/10.1007/s11408-026-00507-x)；先測因子幾何，不先增加影像轉換及CNN開銷 |

## 產物與當前排程

新正式產物根目錄：`artifacts/markets/tw_futures_beta_policy_20261025_v1/`。
每個案例完整名稱前綴 `twd_equity__`，選最後fold05；loss在`train_<years>/`。
四回合工程在`artifacts/benchmarks/tw_futures_beta_policy_20261025_v1/engineering/`，
由既有retirement owner在保留精簡驗收證據、程序參照門檻後清理；失敗證據不直接刪掉。
研究資料沿用`data_tw_index_futures/`和既有`data_*`，未另放入artifacts。

目前排程owner：`stockagent-futures-beta-policy-20261025-v2`，
操作收據`artifacts/operations/futures-alpha-research-20261025-v2/`。
11:38台北時間已取得既有雙GPU獨占lease、通過嚴格CUDA配置／運算／同步前檢，進入六組工程。
v1 prototype只作未啟動的配置核對；lease重新進入路徑於v2修正，舊request保留，沒有跑兩個GPU owner。
正式收斂結果尚未產生；後續狀態以[工程／正式進度](../artifacts/operations/futures-alpha-research-20261025-v2/progress-latest.json)為準。
11:53台北時間讀回：前兩組08:45工程已完成並獨立驗收，第三組08:45 Muon正在準備資料；
三組09:00仍排隊，六組新正式收斂尚未開始。Supervisor持續運行，工程全數檢查後自動只啟動通過的正式案例。

新凍結來源SHA：`49e2a0f6448c01176ef4fba47d0552760c19e47c55512834a60325d852b74bf0`，
1854檔案。從上一個凍結來源僅加入本研究10個程式差異與新base config；
未帶入工作區無關的config cache或其他dirty工作。
遠端source.zip、wheel與逐檔雜湊已獨立核對。
[來源差異收據](../artifacts/operations/futures-alpha-research-20261025-v1/curated-source-delta.json)。
本機臨時建置輸入亦已核對逐檔可恢復、程序／設定／FD／mmap與共享inode門檻，
隔離後再次核對再清理3254個檔案、80,617,472 allocated bytes；
正式產物與冷物件刪除數皆0。
[精確暫存清理收據](../artifacts/operations/futures-alpha-research-20261025-v1/temporary-build-input-retirement.json)。

可接受的新研究結論，必須同時交代費後績效、TX相關性／beta、回撤、成本、實際交易、
不同年度與來源邊界。曲線形狀不同本身不算策略改善；現階段也沒有保證新六組會勝過目前最佳。
