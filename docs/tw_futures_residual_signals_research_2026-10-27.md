# 期貨研究：市場依賴、因果殘差訊號與765碼資料修復

2026-10-27。接續[相對價值研究](tw_futures_relative_value_research_2026-10-24.md)、
[先驗beta投影](tw_futures_beta_policy_research_2026-10-25.md)及
[原725來源修復](tw_futures_universe725_recovery_2026-10-25.md)。
本報告記錄研究與工程證據；新訊號六組的工程回合不作正式績效。

## 全台幣八組已登錄遠端：2026-10-27

最新資料、限制與讀回入口為[765碼報告](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/README.md)
及[全台幣遠端研究v3](../artifacts/markets/tw_futures_all_twd_recovery_20261027_v3/README.md)。
今天官方列出的343個台幣標準商品家族全部在765碼歷史目錄內；
343個家族在2011-01-26至2026-09-24均有至少一個金融准入與300分鐘窗容量日。
這個分母不包含每一客製RFQ交易形式，也不代表截止日後所有實體調整月份契約已驗證。

金融v8的754個正成交准入碼沒有改為全期共同上市的交集。
完整成交時鐘的1,044個未知契約日已逐日隔離；原始行情、持倉與帳務保留。
300分鐘事前單口試單P50的來源容量742碼，P100為754碼；
P100是較強的成交研究假設，需要對照與完整費稅帳務，不能當成實盤流動性保證。
新八組維持d32／lookback32／global batch128、從零訓練、fee-net log utility與patience100。
遠端已從固定原始來源重建，13:20 UTC獨立讀回確認完整資料、八組有效設定與相同最後fold14通過。
金融、一分鐘窗與開盤參考價逐位元相同；兩個300分鐘窗全部5,861,248個報價值比較，
最大差9.10e-13，其餘欄位、缺值遮罩與來源驗收相同。
v1的Float64 VWAP末位逐位元差異、v2的參考設定閉包缺件均保留失敗證據；
v3固定遠端實際SHA並補足獨立設定快照，沒有捨入或改寫金融／成交原值。
訓練2011-03-22至2025-02-25，驗證2025-02-26至2026-02-25，測試2026-02-26至2026-09-24。
新GPU訓練尚未開始，正在原隊列後等待；取得GPU後仍須通過完整四回合工程驗收。

TEJ原生REST key目前只有28個試用表，沒有非試用原生表；
Fugle既有key股票控制成功、期貨日資料與商品目錄控制403，尚未證明拒絕原因。
Shioaji官方期貨歷史從2020-03-22起，不能補2012至2018的1,044個完整時鐘缺口。
這些有限來源查核不等於世界上所有供應商已耗盡。
未購買官方歷史檔，沒有修改原717碼研究的來源或中斷其GPU訓練。

## 765碼資料修復驗收：2026-10-27 11:25 UTC（歷史快照）

本段取代下文早期世代的「目前」數字，舊收據保留作為修復過程。
完整清單與分母見[正式765碼入口](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/README.md)。
全目錄765碼均為TWD：710股票、28 ETF、23指數、2商品、2利率期貨；
2011-01-26至2026-09-24有正成交的755碼中，修正後v8已有754碼至少一個時點通過金融規則。
這不是754碼全部交易日通過，也不是754碼已完成GPU訓練。
[逐碼金融准入表](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/product_admission_v8.csv)
保留8個無正成交證據碼、2個只有2010正成交碼及IT1的不完整金融組件，沒有把它們從目錄刪掉。

本次v8完整重編1,114.51秒，金融發布另296.73秒，兩者均為本機CPU資料準備範圍；
不能換算為GPU epoch加速倍數。
EWF／EW1正式公司行動日固定2011-09-13，修除錯誤日期衍生條款。
[全量原值讀回](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/corporate_terms_full_readback_v11.json)
確認4,975,909列行情與其他商品金融規則不變；可到達受阻帳務日599→570。
[來源邊界](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/remaining_source_boundaries_v8.json)
記錄345個缺估值鍵已被29份TEJ原查詢覆蓋但未回傳；44個缺前值帳務日的前值也全在該組鍵內。
185個庫存續行問題不能以市場OI為0為由清除策略部位；16份官方鉅額原件也沒有8個無正成交碼的交易。
這些是目前授權來源的實證，並非宣稱已窮盡世界上所有供應商。

v8金融發布已驗收，但保留明示的`frozen_contract_value_research_v1`估值研究假設，
`financial_point_in_time_verified=false`；它不是嚴格PIT、完整歷史可成交的實盤資料。
08:45／09:00、一分鐘／300分鐘窗口正在以同一版來源完整讀回，未完成前不沿用舊27缺口數宣稱新版本通過。
新六組設定已指向v8，schema檢查通過；32維、lookback32、batch128、簡單log utility、
從零訓練與連續100回合無改善停止均保留。尚未通過資料、遠端完整重建與四回合工程驗收。

遠端717碼世代維持原工作：11:20 UTC，08:45三組分別在221／245／335回合完成；
09:00對照161回合，另外兩組尚未開始。新765碼資料沒有被誤標為現行訓練輸入。

## 已完成實驗與可重現比較

遠端 `114.34.26.236:43269` 的30組正式實驗均完成完整產物驗收：
先前20組、新beta六組、2011歷史四組。全部從零訓練，連續100回合無改善停止，
並保留驗證／測試／曲線／checkpoint。另四組 dated-membership 在啟動工程時失敗，
未產生正式模型，不納入30組排名。

本次獨立讀回每組正式manifest、金融指標、每日tape、梯度及停止規則，
並把840檔／600張PNG回傳本機；傳送前後遠端SHA與本機一致，所有PNG驗證及解碼通過。
權重沒有在這次曲線回傳中一併下載；仍在遠端原路徑。
見[交付收據](../artifacts/operations/futures-residual-signals-20261027-v1/result-delivery-receipt.json)、
[完整30組表格與曲線](../artifacts/markets/tw_futures_residual_signals_20261027_v1/README.md)。

全部比較同一145個測試日期：2026-02-26至2026-09-24。
下表MDD顯示幅度，相關係數使用每日簡單報酬；Sharpe／Sortino沿用正式log-return定義。
不同訓練起點、商品池及成交窗是不同研究條件，不能把排名當作單因素因果證據。

| 策略／Strategy | 報酬 | Sharpe | Sortino | MDD | TX相關係數 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 09:00 名目座標＋Muon／Notional coordinates + Muon | 24.28% | 1.803 | 2.618 | 12.76% | 0.780 |
| 2011起08:45一分鐘成交窗／History 08:45, 1m | 19.47% | 1.566 | 2.250 | 12.52% | 0.749 |
| 2011起09:00一分鐘成交窗／History 09:00, 1m | 21.11% | 1.389 | 2.012 | 14.34% | 0.792 |
| 09:00擴充商品池／Expanded universe | 45.72% | 1.223 | 1.838 | 28.25% | 0.880 |
| 08:45名目中性／Notional neutral | 2.61% | 0.708 | 1.034 | 4.49% | 0.458 |
| 09:00先驗beta中性／Prior beta neutral | -5.03% | -1.924 | -2.474 | 8.41% | -0.548 |
| TX近月展期1x毛基準／TX rolling 1x gross | 30.72% | 1.360 | 2.039 | 17.22% | 1.000 |

最佳Sharpe遠端模型：

```
/root/stockAgent/artifacts/markets/tw_stock_futures_conditioning_history2013_20261023_v3/clock0900__notional_muon/fold_12/
```

實體容量重試目錄為同根下
`_capacity/clock0900__notional_muon/attempt-00/training/`。
本次新beta六組全部虧損：08:45三組報酬為-0.26%、-1.58%、-1.02%，
09:00三組為-3.81%、-5.03%、-4.84%。報酬轉負且相關性變號，沒有證明找到獨立alpha。
2011起30分鐘窗的08:45報酬32.53%、Sharpe1.136、MDD30.17%；
09:00為28.07%、1.054、29.74%，同樣沒有全指標超越。

本比較保留原benchmark契約
`tx_front_month_rolling_buy_hold_1x_gross_same_contract_close_v1`。
這是TX近月展期1倍名目毛報酬，不是2330當沖，也不是同本金、同費稅、
整口保證金的單口TX帳戶。策略含其原定費稅與容量模型；不以不同資本口徑作實盤結論。

## 第一性原理：商品多，不等於訊號獨立

近似分解為 `r_strategy = beta * r_TX + residual`。
擴大選股池不會自動消除共同因子；簡單log utility也不會獎勵「曲線看起來不同」。
固定benchmark從loss減去後，對模型參數的梯度為零，不能藉此創造新的學習方向。
將報酬曲線反號、縮小槓桿或壓低波動，都不是增加獨立預測力的證據。

最高Sharpe組的同期OLS R²約0.609；這是線性解釋比例，不是因果歸因。
該組實際持有183種商品、測試期可用聯集337種；擴充2011組可用聯集375種，
08:45一分鐘組實際持有127種。375並非要求全期間共同存在後留下的交集。

帳務進一步顯示最佳組淨利24,279,064 TWD，隔夜損益30,380,177.64 TWD，
開盤後損益與全部成本合計-6,101,113.64 TWD。
2011起08:45一分鐘組同樣淨利19,472,960 TWD、隔夜23,366,347.77 TWD、
其餘-3,893,387.77 TWD。這支持「目前收益相當依賴隔夜持倉」的診斷，
但隔夜分解本身不能證明其全由市場beta造成，也不能證明移除隔夜後會改善。

下一步先改變訊號表示，再單獨測試曝險限制。保留由神經網路自行決定方向、
庫存與交易，不指定固定反轉規則，也不拿舊模型微調。

## 已實作：因果殘差訊號／Causal residual signals

新的 `futures_residual_signal_context` 由共用資料／模型／checkpoint路徑承載。
每個決策日只使用之前已結束、同一實體契約且有觀測證據的報酬，
配對之前的TX報酬，以32列滾動OLS及8筆向beta=1收縮的pseudo-observations估計loading。
使用簡單報酬的 `x - beta*y` 形成統計訊號；它不是另一筆虛構hedge成交或帳本PnL。

新增10欄包括收縮beta、1／5／20列殘差均值除以歷史RMS、RMS本身、
5／20列配對觀測比例、上一列是否有觀測，以及TX的1／20列標準化訊號。
缺失配對不加入估計；沒有觀測時訊號為0並另給coverage，beta的1是明示先驗。
不插補成交價、不把缺值當成零報酬觀測，不以今天的成交可用性偷看今天結算。
候選仍依當日生命週期、規則及成交時鐘遮罩。

10欄透過新的小型線性分支進入既有期貨token與注意力網路。
分支初始權重為0，保留對照的原參數及RNG；梯度透過真正整數帳務的既有adjoint學習。
市場分解與缺值統計採FP64 prefix sufficient statistics，時間及記憶體皆O(T×S)，
沒有逐日CPU回歸、T×S×lookback展開或S×S協方差。
保留費後log utility，不把Sharpe、Sortino、Calmar和相關性混成新複合loss。

啟用時使用新輸入／模型／交易fingerprint，拒絕舊checkpoint恢復；
停用時保留既有fingerprint。固定輸入與動作下，原整口、費稅、跨日、
保證金、公司行動及吸收破產帳務不變。策略學習軌跡改變是本實驗的目的。

167項測試通過，涵蓋獨立窗口oracle、未來擾動、缺值／常數市場、
來源manifest、實際帳務反向傳播、新分支非零梯度、初始化及checkpoint相容性。
見[JUnit及凍結源核對](../artifacts/operations/futures-residual-signals-20261027-v1/mechanism-source-readback.json)。
原寬特徵manifest仍標記research_only及historical_point_in_time=false；
新殘差的因果測試不代表所有原始基本面欄位已有歷史發布時間證據。

遠端整合另暴露windowed張量入口的舊欄寬限制：增加10個訊號欄後，
模型能接受但`WindowedSplitTensors`尚未接受。修正在原共用入口加入
四種因果帳戶訊號欄寬，沒有放寬為任意欄數；77項相關回歸通過，
新增測試實際走dataset→windowed、1936／2816兩種槽位及beta／開盤參考組合。
原167項不是完整遠端整合通過的宣告。見
[修正版來源及測試](../artifacts/operations/futures-residual-signals-20261027-v2/mechanism-source-readback.json)。

## 六組正式實驗與驗收

[基礎設定](../configs/markets/tw_futures_residual_signals_20261027_v1.yaml)、
[消融設定](../configs/ablations/tw_futures_residual_signals_20261027_v1.yaml)。

| 決策時間 | 自由對照／Control | 殘差訊號／Residual signals | 殘差訊號＋先驗beta中性／Signals + prior beta neutral |
| --- | --- | --- | --- |
| 08:45 | control_0845 | signal_0845 | signal_beta0_0845 |
| 09:00 | control_0900 | signal_0900 | signal_beta0_0900 |

固定當前期貨樣板：d32、lookback32、global batch128、Muon、BF16、兩GPU DDP、
1億TWD起始帳戶、同一2011起已驗收資料與一分鐘成交窗、最後fold14。
兩時點使用相同歷史資訊。開盤參考沿用「該時段第一筆真實開盤成交，
尚未開盤才取最近已知真實成交」，參考價和實際可成交價格仍分開。
不強制當沖。只有OOM可走既有容量重試，未觸發前不改batch或模型尺寸。

全部從零訓練，最多1000回合，連續100回合無改善停止，min_delta=0.0001。
先完整四回合工程，驗證有限非零梯度、replay adjoint、無穩態重編譯、
整口持倉、全部曲線及checkpoint；訊號組還須從checkpoint讀到分支已學習。
只有通過工程的案例才能進正式queue，失敗留證並繼續。
驗證集選模；反覆檢視過的2026測試期只作探索，不宣稱全新外部樣本。
若六組有候選優勢，再以其他年度fold和成本壓力檢查，而不是挑當年test最漂亮的回合。

原v1 source SHA `be2170616bffdefb0592cee14aea69f5e5b472b51438871ebb1beed3815893fb`，
request SHA `132166a7dfe2198f8cda0c87b0efac10684c03ed8cd33079fcbe6fa85dd18b67`。
共用GPU租約、雙卡CUDA配置／kernel／synchronize預檢已通過；
v1在00:03 UTC啟動，08:45對照完成四回合及完整產物，訊號組在windowed欄寬檢查失敗。
00:38 UTC依原Supervisor停止，保存失敗與未完成案例，並停用其自動啟動。
沒有覆寫原凍結程式、request或失敗產物。

修正v2 source SHA `2d6c1fd7509f76a1f90c8362e782936e88e7e7b4482f00f5d8b0d6b8b8bc7dc6`，
request SHA `035f97d9a946660f6190d79544eddac650e6809564b2dc5f4dd0e1ede93c4958`。
遠端Supervisor `stockagent-futures-residual-signals-20261027-v2`在00:42 UTC啟動；
3,396份交付文件SHA相等，原資料、模型參數、loss及帳務規則相同。
01:49:39 UTC（台北09:49）六組已全部通過工程驗收，開始正式1000上限／100耐心訓練，
不等待資料修復完成。03:56:51 UTC（台北11:56）首組08:45對照完成epoch 221，
最佳validation在121回合，正好連續100回合無改善；正式lifecycle與完整帳務驗收通過。
145日費後報酬17.56%、Sharpe1.471、Sortino2.107、MDD12.69%，TX簡單日報酬相關0.752，
OLS beta 0.419、R² 0.566。完整穩態MAX-rank epoch中位數33.063秒，穩態無新增Dynamo圖。
這仍是市場相關偏高的對照，未超過舊最佳；第二組08:45殘差已開始。
05:29:37 UTC（台北13:29）獨立讀回完成157回合，最佳validation loss為-0.3187658，
當時no_improve=12、非零有限梯度、無零梯度回合，另外四組等候。
新訊號尚無正式收斂績效，不把監控中的test loss當作正式選模結果。
首組28份產物／20張PNG已完成傳送前後及本機SHA核對、圖片解碼，
見[新結果與曲線入口](../artifacts/markets/tw_futures_residual_signals_20261027_v2/README.md)。
09:00 beta中性工程組曾容量重試，成功回合有54批，
其餘組為27批；不將該組時間當作相同batch的架構單因素比較。
見[啟動與舊owner停止證據](../artifacts/operations/futures-residual-signals-20261027-v2/launch-readback.json)、
[即時回讀](../artifacts/operations/futures-residual-signals-20261027-v2/live-readback-latest.json)。

新正式模型根目錄：`artifacts/markets/tw_futures_residual_signals_20261027_v2/`。
控制與即時進度：`artifacts/operations/futures-residual-signals-20261027-v2/`。
既有30組結果表與比較曲線仍在`artifacts/markets/tw_futures_residual_signals_20261027_v1/`。
工程暫存根`artifacts/benchmarks/tw_futures_residual_signals_20261027_v2/engineering/`已在
六組完成且保留loss、完整epoch計時、績效與失敗摘要後，由原有受保護discard流程清理：
452份暫存、938,745,856 allocated bytes。正式權重未刪除。
見[工程驗收](../artifacts/operations/futures-residual-signals-20261027-v2/engineering-acceptance.json)、
[保留曲線及失敗摘要](../artifacts/operations/futures-residual-signals-20261027-v2/engineering-retained-evidence.json)、
[正式啟動](../artifacts/operations/futures-residual-signals-20261027-v2/runtime-launch.json)。

已停止的v1工程暫存於00:48 UTC完成保護清理：先將136份曲線、設定與原始失敗日誌
逐檔SHA保留至`artifacts/markets/tw_futures_residual_signals_20261027_v1/rejected_engineering_evidence/`，
再由已驗證凍結版的canonical儲存owner檢查alias、程序／設定／FD／mmap、
乾跑identity、隔離及最後unlink。原checkout的owner過舊及短暫PID權限拒絕的失敗均保留，
沒有略過掃描；新完整掃描通過後才刪除148個暫存檔，回收160,817,152 allocated bytes。
讀回確認舊暫存根已不存在，正式權重與來源資料未刪除；v2工作仍在執行。
見[清理與獨立讀回](../artifacts/operations/futures-residual-signals-20261027-v1/superseded-engineering-independent-readback.json)。

## 從舊當沖研究抽取可檢驗的機制

舊`geometry__unit_mgda_sgd_step`是參數空間L2單位梯度MGDA加無動量SGD，
分別求Sharpe、log return、Sortino及Calmar四個目標梯度，再解最小範數凸組合。
不是四個loss直接平均；validation仍用log utility，從零訓練，128回合選模、228回合停止。
181日完整測試報酬153.13%、Sharpe5.933、Sortino17.091、MDD6.83%。
日期、資本與當沖／跨日帳務不同，不能直接當作目前145日期貨的可達績效。
原模型熱路徑已依前次要求回收；恢復入口為
[冷儲存模型索引](../artifacts/operations/daytrade-closeout-futures-20261021/best-model-index.json)，
研究結論見[當沖研究結案](daytrade_research_closeout_futures_2026-10-21.md)。

當前MGDA的四目標adjoint入口綁定年度當沖episode及account offsets；
期貨有跨日庫存、保證金及公司行動狀態，不能僅切換loss名稱便宣稱已移植。
本輪先隔離「無動量更新」：新增08:45／09:00兩組SGD，learning rate0.1、
momentum／dampening／weight decay均0，保留單一費後log utility及4摘要殘差模型。
它是SGD機制對照，**不是MGDA**；不同有效步長也不能解讀成純優化器家族因果排名。
其餘d32／lookback32／batch128、717碼、原整口帳務及100回合耐心保持相同，
全部從零訓練；工程還須從實際optimizer state驗證沒有動量或自適應狀態。

04:04 UTC獨立讀回：新Supervisor已等待前六組與兩組多摘要研究，沒有佔用另一份GPU訓練。
source SHA `04457b15e702bb9d72ef7a80b921fb831a390487cd2a6906d19a606c19f985f7`，
request SHA `fab02d371a6a5685e294aaa5448ea3838c25d3f5a229cba3785ac472347716e4`；
3,400份固定交付文件SHA一致，原兩個owner PID未變。
設定、排程與未來產物位置見
[SGD正式入口](../artifacts/markets/tw_futures_plain_sgd_20261027_v1/README.md)。

## 原725修復與「全台幣期貨」邊界

目前新研究重用2011起已驗收版本：725歷史目錄碼，717碼有歷史每日可執行紀錄，
有11個契約日成交時鐘隔離。這不是最新財務修復版，也不是全725已完成分鐘與GPU驗收。
原725包含682股票、27 ETF及16指數期貨；歷史台幣期貨目錄另有40碼，總計765。
本輪直接核對來源分類表：765列的settlement_currency皆為TWD，分類為710股票、
28 ETF、23指數、2商品及2利率期貨。新增40碼為28股票、1 ETF、7指數、2商品與2利率。
目錄來源為`data_tw_index_futures/preparation_sources/margin_sources/universe/products.csv`，
原要求名單為`data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/requested_725.csv`。
因此目前正在訓練的717碼版本仍不是全部765碼；不把名稱all_twd解讀成全准入。

獨立修復線最新已驗收v534、完成收據v558：725帳務主體、721個有歷史正成交准入，
固定1,766,347個完整正成交契約日中准入1,745,524日（98.8211%），仍待修20,823日。
相對父版v485新增561日，原有准入損失0；碼數仍是721。
[逐碼與逐類別清單](../artifacts/markets/tw_futures_universe725_recovery_20261025_v1/coverage/financial_v534/README.md)
保留NULL和明示零量的差異；額外329列不混入完整四OHLC分母。
FC1／HW2／IQ2／LG1有公司行動庫存與估值用途，但目前無已驗正成交。
FinMind逐商品和TEJ回覆的112列量明示0，不能為了達成725交易數創造成交。
尚未宣稱所有供應商已查盡。

現有優先下載已實際完成：TEJ745筆有限工作，FinMind12碼實際回覆與18,097列範圍核對，
以及FinLab缺欄重查。此次不重排完成／明示空工作，也不重設quota。
v534沿唯一來源owner合併已核對的早期指數保證金684個開盤及672個收盤輸入，
00:18 UTC完成accounting、selection、publication及audit。原v536驗收將允許範圍
限在最初684個契約日及其下一日，因同一商品規則也補回其他歷史月份而拒絕；
失敗收據與候選均保留，沒有改寫原checker或先採用候選。

新v556從來源區間差集推導日期／商品範圍，逐筆檢查全部變動保證金、公告／生效時鐘，
並從上一市場日的來源區間重算變動承接額。原始規則的13:45保證金生效時鐘與
契約到期13:30事件分開；warmup來源有已知金額，不等於可新開倉或已准入帳務日。
原已知金額全部不變，範圍外4,911,496列金融資料一致。v557再保留原現金、
公司行動、3,918筆原生參考價、兩版完整庫存可達圖及父版准入差集檢查。
00:31 UTC全部通過後才接受561日；不是把684個來源輸入數當作恢復交易日。
見[來源範圍證據](../artifacts/operations/futures-universe725-repair-20261025-v1/source-bound-financial-scope-v556.json)、
[完整金融驗收](../artifacts/operations/futures-universe725-repair-20261025-v1/retained-financial-v558-complete.json)。

剩餘固定正成交契約日工作表v537中：19,849列warmup／價格界線、
864列涉及組件其他日期的來源、106列庫存續行、4列缺前值／價格界線。
新v560已將864列全部追到215個庫存組件、576個實際阻擋帳務日：
351日缺估值／期末值／結算、348日缺前值與界線、177日庫存續行、8日生命週期，
各原因可重疊，不能相加作分母。沒有剩餘組件因2011之前的指數保證金缺件阻擋。
其中359個估值／生命週期帳務日涉及25碼；NYF已核對，另外24碼尚無逐商品查詢。
00:38 UTC已透過原Complement登錄有限優先查詢，不重抓全市場日檔，不重送完成或明示空任務。
09:43回讀24碼仍pending；開盤保護結束後，來源查核仍被約1.7萬筆
已完成美／英／日歷史資料的priority0例行續更壓住。這是可修正的調度政策，
不能當成FinMind外部限速。已在原Complement加入有限查核優先：只扣除當前已接受的
外國id_history續更預留，保留pending／failed工作、真正發布、未來預留及實際帳號額度。
來源查核仍走同一把鎖、序列exact-key請求及原始收據，不修改task優先值或due clock。
當前來源259項回歸通過並鎖定前後SHA；原reload工具完成安全drained交接，
01:56:46 UTC回讀canonical_owner_schema_and_status_verified，保護中的開盤服務身份不變。
見[調度機制驗收](../artifacts/operations/futures-universe725-repair-20261025-v1/finmind-source-priority-mechanism-v565.json)、
[原owner交接讀回](../artifacts/operations/futures-universe725-repair-20261025-v1/finmind-source-priority-owner-reload-v566.json)。
24碼之後全部complete，共357,571列；01:58 UTC獨立核對原件、收據及SHA全部通過。
限定原2011範圍、正常時段、單式月份後比較318,590列，另38,981列在範圍外。
新增正成交、正結算、缺席帳務鍵、正結算矛盾及成交量差異全為0，候選格也是0；
這批回覆沒有新增可准入日，沒有改寫官方值或加入正在訓練的資料。
四個目前無正成交的特殊碼也沒有因此出現新正成交證據；仍未宣稱所有來源已查盡。
見[24份原件獨立驗證](../artifacts/operations/futures-universe725-repair-20261025-v1/finmind-product-query-source-readback-v562.json)、
[逐鍵比較與零增量結果](../artifacts/markets/tw_futures_universe725_recovery_20261025_v1/source_recovery/finmind_product_reconciliation_v563/README.md)。
見[組件來源追查](../artifacts/operations/futures-universe725-repair-20261025-v1/unexplained-component-source-trace-v560.json)、
[新增24碼優先登錄](../artifacts/operations/futures-universe725-repair-20261025-v1/finmind-component-product-registration-v561.json)。
這些尚未全部證明為外部限制。五筆官方行情與原生參考價矛盾、
27個契約月份成交時鐘與新增40碼規則仍分項追查。
官方五批時鐘原件NT$5,000仍依使用者決定暫不購買；FinHa目錄宣稱歷史逐筆，
目前沒有通過下載／原件／時鐘驗收，不把目錄宣稱算成已補齊。

本次credential-bound回讀FinMind帳號為SponsorPro、官方額度20,000次／小時，
舊公開Free快照不再作為目前權限結論。六個缺口日期的逐筆及分鐘全市場檔
原已在2026-10-06由canonical complement下載完成，本輪不是新取得。
12份原件／收據SHA與筆數重核，按該日正常交易時段及正式單式量口徑比對，
27份逐筆對應沒有一份通過完整成交量核對，分鐘結果亦重現先前差異。
不能把總日量、夜盤或含鉅額的量錯當缺口的單式量，也沒有因此解除隔離。
見[權限回讀](../artifacts/operations/futures-residual-signals-20261027-v1/provider-entitlement-readback.json)、
[27份成交時鐘再核對](../artifacts/operations/futures-residual-signals-20261027-v1/retained-clock27-current-readback.json)。

## 文獻脈絡與下一階段

本次765碼盤點及財務／成交分層成果見
[765碼正式研究入口](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/README.md)。
目前已獨立確認全歷史765碼都有正常時段實體月份來源、757碼有正成交；
2011範圍是763碼有來源、755碼有正成交。現在的742碼每日准入與717碼遠端訓練
是不同版本；還沒有765碼完整訓練，不將此差距隱藏於all_twd名稱。

新的按範圍工作表修正先前診斷：CPF在2011以後7,029列非warmup帳務沒有financial blocker，
目前排除來自`twd_equity`消費者範圍，非找不到同期保證金。
canonical CLI現已明確接受`all_twd`，涵蓋股票、ETF、指數、商品及利率五類；
範圍與carry相關27項測試通過，原已驗收金融／遠端資料未改寫。
其餘12碼仍有真正的規格、估值或庫存組件問題。
舊工作表混入2011之前帳務，已保留並由v2明示修正；不改原稽核腳本的期間語義。

CPF的20個正成交日期，既有FinMind全市場分鐘及逐筆40份原件已全部讀回；
不用插隊重抓已完成工作。canonical all-TWD normalizer以原dated tick、正常時段、
成交量除二及官方單式量／高低收認證，20/20全日核對通過。
目前一分鐘窗與50%參與率，08:45只有一個契約日有一口matched volume、
取整後零容量，09:00沒有窗口成交；這是所選執行政策下的真實流動性限制。
180分鐘預先掛單窗則兩時點各11個契約日具備至少一口的窗容量上限，
仍須再過前日容量、保證金及完整財務准入。
這直接說明「補齊來源」與「任意時點可成交」不能互相替代；長窗必須獨立版本化，
不偷換正在跑的一分鐘對照。原件、逐窗表及精確分母均在
[窗口核對](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/cpf_execution_window_readback_v1.json)。

此外已實作固定tick分階段價格帶v2：原ETF v1保持相容，確切upper／lower繼續NULL，
只按官方±7%最內圈及dated tick給保守交易權限，不用13%／20%外圈虛構當日時點。
原dated-spec owner新增精確內容綁定的reviewed staged delta，保留原2,797個規格區間，
UDF／SPF的上市規格只涵蓋2017-05-15至下一未審查修訂2020-11-23之前。
195項相關回歸通過，涵蓋獨立Decimal取整、未知stage／manifest／來源拒絕、
固定動作金融值及舊ETF相容性。這是軟體支援缺口修正，不是市場觀測插補。

真實金融重編：UDF完整候選帳務0→2,148日（正成交1,184日），
SPF 0→1,810日（正成交758日）。再經canonical完整庫存組件選擇及continuation驗證：
UDF有4個完整lifetime／979列／541個正成交契約日，SPF仍為0；
其餘日期保證金缺口會阻擋整個相關組件，不能以零散通過列數冒充已可訓練。
原始價格及帳戶特徵全相等，候選尚未正式publication／成交時鐘驗收。
20份對應保證金原件（13份原待OCR）已經canonical agent workflow完成擷取及
原件／完整頁數／逐輸出SHA核對；不重抓既有文件、不把搜索摘要或OCR候選金額直接採入帳務。
來源時鐘與數字區間仍須金融規則審查，見
[來源擷取收據](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/fixed_margin_extraction_v1.json)。
見[完整原值與組件讀回](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/financial_candidates_component_readback_v1.json)。

本次查閱以下原始論文及作者／期刊來源；它們提供假說，不承諾台灣期貨複製美股績效。
Deep Learning Statistical Arbitrage的正式刊登資訊另由
[作者研究頁](https://mpelger.people.stanford.edu/research)核對。

| 年份／來源 | 核心用途 | 在本研究的處理 |
| --- | --- | --- |
| [Gatev、Goetzmann、Rouwenhorst 1999／2006](https://www.nber.org/papers/w7032) | 歷史相似資產與相對價格 | 作研究起點；不移植固定配對及交易門檻。 |
| [Avellaneda、Lee 2008／2010](https://math.nyu.edu/~avellane/AvellanedaLeeStatArb071008.pdf) | 市場／產業分解後的殘差訊號 | 支持先分開共同曝險與個別訊號；單因子只是起步。 |
| [Deep Learning Statistical Arbitrage，2021／2022預印本，2026正式刊登](https://arxiv.org/abs/2106.04028) | 殘差投資組合、序列訊號及受限神經策略 | 採用訊號→政策的分工，保留既有真實帳務；作者頁列Management Science 72(9), 7502–7549。 |
| [Attention Factors for Statistical Arbitrage，2025](https://arxiv.org/abs/2510.11616) | 因子與費後策略共同學習 | 下一個高優先架構假說：低秩可學因子，避免逐日S²回歸；本六組不是完整AFM重現。 |
| [Machine Learning and the Implementable Efficient Frontier，2026](https://academic.oup.com/rfs/advance-article/doi/10.1093/rfs/hhag022/8524346) | 可實作、含成本的投資組合學習 | 以費後帳務評價，保留容量與整口，不用毛績效代替。 |
| [AlphaZeroBeta，2026-07](https://arxiv.org/abs/2607.18001) | 相關性、相對報酬及成本的強化學習 | 作市場中性比較；先保留使用者偏好的簡單loss，未新增PPO。 |
| [Amortized Neural Filtering，2026-10-19／20](https://arxiv.org/abs/2610.25802) | 用後驗不確定性輔助殘差部位 | 先保留觀測coverage／風險，再研究可學置信度；不能把缺失當確定訊號。 |
| [Signature-Based Optimal Execution for Statistical Arbitrage，2026-06／07](https://arxiv.org/abs/2606.31387) | 將路徑訊號與庫存、交易速度、衝擊共同最佳化 | 留待檢查成本是否吃掉殘差後比較；其連續部位／二次規劃不能直接代替整口保證金帳務。 |
| [From pixels to profits，2026-10-23](https://link.springer.com/article/10.1007/s11408-026-00507-x) | 證券、避險及殘差的聯合序列表示 | 本輪用數值序列避免額外影像生成成本；日後需直接比較才談表示優勢。 |
| [Sparse Portfolio Optimization with Transaction Cost，ICML2026](https://proceedings.mlr.press/v306/yao26s.html) | 同時處理稀疏持倉與交易成本 | K-sparse問題是NP-hard，論文用DC／proximal法；其simplex連續配置不直接代替整口期貨帳務，先驗證是否有可學的費後殘差。 |
| [Certified Alpha Capacity，2026-10-01／04](https://arxiv.org/abs/2610.01115) | 訊號衰減與有限樣本下的可證實alpha | 是帶假設的統計容量分析，不是台灣期貨獲利證明；支持預先列明假說、驗證選模及保留新年度檢查，避免反覆看同一測試期後宣稱找到新alpha。 |

[A Deep Learning Approach for Trading Factor Residuals](https://arxiv.org/abs/2412.11432)
已於2025-01-03撤回，作者明示靜態S&P500名單取代動態投資池的錯誤。
不以其超高Sharpe作有效支持，並保留本研究逐日生命週期與可交易mask。

先完成這六組，從validation、費後結果、實際beta／相關性、殘差波動、成本、
整口利用率及隔夜分解判斷訊號是否有價值。後接兩組增加可學市場摘要的對照，
檢驗原四組摘要是否壓縮了個別商品關係；若成本吃掉殘差，應改善庫存／調整政策；
若來源時鐘不足，先修來源。不能把更多演算法名稱堆進loss視為改進。

本輪重新查到2026-10-23的圖像相對價值論文，並讀取2026-10-20神經過濾論文全文。
後者明確指出校準改善並未額外創造alpha：同一因子及交易規則下，後驗部位與點估計
的費後績效相近。故不把更複雜估計器本身當作收益來源；下一個架構假說仍優先驗證
可學低秩因子是否提供額外可交易訊號，再比較加入不確定性後的成本與風險。

## 計算瓶頸：已採用與未證明

沿用已驗收的全schema CSR常駐、cuSPARSE transpose adjoint、
折內共享擬合轉換及正規化快取、既有編譯帳務、單一epoch更新與GPU租約。
既有同源雙5090 A/B/B/A曾將完整穩態MAX-rank epoch約103.97降至27.83秒（3.736倍），
但冷／暖四回合整體為650.20→522.33秒（1.245倍）；不能把3.736倍寫成整個工作流加速。
見[效能契約與實測](agents/performance.md)。

本次30組讀回中，2011一分鐘正式組完整穩態epoch中位數約33.13／33.46秒，
最佳2013組約28.88秒，2020beta組約14.26～14.77秒。
資料長度不同，不能把這些差異解釋成新訊號的速度效果。
本輪新殘差以O(T×S)準備、窄分支推論，不增加另一輪Transformer或金融反傳。
新六組將保留同源對照的完整epoch MAX-rank及四回合完整wall time，
包括準備、編譯、驗證、測試、繪圖與checkpoint。
現已取得六組工程穩態完整epoch均值：08:45對照32.293秒、殘差31.979秒、
殘差＋beta中性34.592秒；09:00對照32.281秒、殘差31.988秒。
09:00 beta中性容量重試後為32.431秒，但成功回合批次數不同，另列。
單輪觀察未作對稱冷暖控制，不宣稱這些小差異為新訊號的加速收益。

目前啟動、原始來源驗證、主機傳輸、帳務遞迴／adjoint、rank尾端與產物I/O
尚未逐一證明無可改善開銷；GPU忙碌率不是有效計算率。
此次首組08:45對照的startup紀錄已實測panel載入／附件243.504秒，
launcher與import合計約46.87秒；後續training transform顯示cache hit。
train-group的epoch前準備另為261.032秒，其中`sampled_test_tensor_preparation`
標籤含dataset建構及張量準備共201.036秒，還沒有細分到能將全部時間歸因於某個函數。
控制組第3／4回合的`epoch_wall_s_max_rank`為31.702／31.577秒；
把每回合曲線、存檔等工作算入的`complete_epoch_wall_s_max_rank`為32.466／32.301秒，
兩者不互相替代，且都不是含啟動與最後完整報告的四回合case總耗時。
第2回合起無新Dynamo圖、四回合均有非零有限梯度。
每批finite-check時間包含等待先前GPU工作，既有device finite guard完整ABBA僅0.34%
收益、未達選用門檻，繼續停用；不能把同步等待欄位直接當成可刪除的計算成本。
這直接證明目前仍有顯著的epoch前主機開銷，不能因另一rank在collective等待時
GPU顯示忙碌，就稱為純運算極限。此數字是首組啟動觀察，未作同源優化A/B。
因此沒有宣稱已達100%有效利用率、純硬體極限或已解決所有軟體瓶頸。
要在新實際shape上量測主路徑、保留負面方案並依完整工作流選擇，再作下一次改動。
修正v2首組較暖的同設定觀察：panel／附件95.556秒，包含launcher與import的startup
累計149.382秒，epoch前準備21.144秒；四個完整epoch分別155.039、32.004、
32.604、31.982秒。冷暖快取及同機負載並未做對稱控制，不能將相對v1的準備時間差
歸功於欄寬修正或某一個演算法，也不能忽略尚在執行的最後完整eval／報告。
已修正一個重複工作：當epoch測試曲線與完整test索引逐項完全相同時，
重用既有`context.test_ds`，省去第二次建構`CrossSectionalDataset`與金融側檔準備；
索引不同仍走原路徑。評估、曲線與checkpoint均保留。
修正在後接兩組的新凍結版，沒有熱改正在執行的v2。
尚未量得此改動對完整冷啟動／穩態及最終報告的淨收益，不寫成已證實加速。

## 全765碼：這次實際完成的來源工作

本輪分母正式擴為765碼，保留原725版與遠端717版不變。
獨立讀回完整原生歷史，765碼都有正常時段實體月份資料，757碼曾有正成交量；
選定2011-01-26至2026-09-24範圍後，763碼有來源、755碼有正成交量，
750碼有金融release列，742碼有每日正成交准入。742不是遠端正在訓練的碼數，
也不代表這些碼所有契約日、分鐘時鐘與來源品質已驗收。
見[765碼逐碼盤點](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/README.md)。

兩碼差異是CV1與DK1：原生來源分別為2010-07-12～08-18與2010-07-22～12-09，
其中正成交15列／2列。這證明來源不缺此兩碼；沒有2011後觀測，不能創造後續成交。
上述是觀測範圍，並非以觀測末日冒充官方下市日。若要在其實際出現時段交易，
另需2010資料與相應規則／時鐘；不偷偷改現行2011比較期間。
見[完整歷史讀回](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/full_history_readback.json)。

新增40碼中21碼已有每日正成交准入，4碼目前僅有估值／零量資料，
13碼有正成交但未准入，另兩碼在選定期間之前。
13碼為BRF、CPF、F1F、GBF、I5F、IT1、NY1、SPF、SXF、TGF、TJF、UDF、UNF；
原盤點中BRF、F1F、GBF、I5F、SPF、SXF、TGF、TJF、UDF、UNF缺已綁定的歷史規格；
UDF／SPF現已有2017上市至2020-11-23前的來源綁定候選，仍未發布新金融release。
IT1有庫存續行缺口，NY1缺前值／價格界線。修正版工作表另確認CPF在2011範圍的
7,029列沒有金融阻擋，原twd_equity分類排除利率類；不能把範圍排除說成缺資料。
原因可以重疊，帳務日列數也不等於正成交契約日分母。
見[13碼工作表](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/positive_unadmitted_worklist.json)。
這些仍是可處理工作，沒有宣稱供應商已查盡。

已將39個尚未查詢的新增碼插入原FinMind Complement有限優先查核；NY1沿用已完成原件。
原政策僅讓位已接受外國歷史續更，pending外國history仍預留全部額度，實測再次壓住有限修復。
本輪依明確授權修正在同一dispatcher：目前到期、非delegated的美／英／日id_history工作
在有限查核預留計算時讓位，包含pending／failed；原task狀態、優先值、due與retry filter不改。
新發布、未來預留、實際帳號quota、正常pacing及開盤保護仍生效。
161項回歸通過，02:42:43 UTC原owner安全drain後交接，保護中的交易服務沒有重啟。

39份新查詢加NY1舊件全部complete，共206,882列，逐檔核對原件、收據與SHA。
限定2011範圍及正常時段單式月份後，102,278列對應金融frame比較：
缺席鍵、新正成交、新正結算、正結算矛盾、成交量差異皆為0。
因此這是來源排除與完整性進展，新增准入為0，不能用下載列數宣稱資料已補好。
見[原件與逐鍵讀回](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/finmind_readback.json)。

全商品決策時鐘共用入口已加入明示v5：按日期綁定規格與manifest SHA，
區分指數點值、商品報價金額及利率／公債面額，使用各商品正常時段與到期截止；
次分價格格點及FinMind Float32還原也按已綁定tick驗證。
legacy v1～v4保持原ABI，缺規格／晚生效／已終止規格不得補成可成交。
108項相關CPU回歸通過、兩項GPU編譯核對在先前合併測試通過；
該次合併測試另有兩個新fixture時間單位錯誤，修正後108項全通過，失敗收據仍保留。
這是準備能力，尚未把765碼規格資料全部補齊或交給遠端訓練。

原canonical官方規則庫留存了尚待OCR的舊PDF；本次不是以現在網站值回填歷史。
已完成2017上市函4頁及其引用的交易／結算規則43頁，保留原檔SHA、原生文字、
逐頁PNG、OCR文字與座標。頁面視讀核對上市函10602005490（2017-04-18）、
2017-05-15上市生效、UDF指數每點20 TWD／tick1點、SPF每點200 TWD／tick0.25點，
原正常交易時段均08:45～13:45。原函明示保證金另公告；尚需後續修訂與規則區間核對。
不以OCR成功等同金融規則准入，也不把列出的外層三階段漲跌界線當成當時已觸發的階段。
見[47頁來源及視讀紀錄](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/rule_review_readback.json)。

從26個歷史規格／上市／下市公告選出18份不同PDF（其中13份原待OCR），
排除已完成三份重複內容後，已完成全部259頁與獨立來源／輸出SHA核對；
合計21份、306頁規格材料。另20份保證金原件101頁及本輪補讀3份原件11頁完成，
都使用canonical庫中已有原件，沒有重抓或採用現在金額回填過去。
見[規格原件驗收](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/normative_rule_extraction_v1.json)、
[保證金原件](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/fixed_margin_extraction_v1.json)及
[追加11頁讀回](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/fixed_margin_extra_sources_v1.json)。
擷取完成仍不等於日期／欄位綁定與金融准入。

### 本輪實際修復：UDF／SPF上市及春節保證金鏈

逐頁核對2017-05-08上市金額、2018春節調高／回調、2019回調、2020春節調高／
明示維持原春節金額及2020-03-19公告。2018掃描表格的欄名與原生新聞稿不一致，
以同日官方原生新聞稿及回調前後金額交叉核對，沒有依錯誤欄名猜測。
原件URL、公開日、PDF／頁面SHA、保證金三欄及生效階段均保留；
2020-01-30「維持原春節標準」是新公告，不把原本計畫到期日直接延長為既定事實。

修復工具原本只允許父資料包列出的季節延續原件，漏掉canonical collector已完整保存
但尚未被父資料包引用的公告。共用修復入口新增明確URL／公開日／SHA綁定，
只接受既有complete原件及不可變parsed版本；原延續解析器仍須證明原額、日期與
排除的另調整商品。未放寬金融金額、發布時鐘或完整庫存組件的檢查。
131項相關回歸通過；失敗的來源銜接／轉錄嘗試、run與原腳本均保留，沒有覆寫為成功。

| 商品／範圍2017-05-15至2020-11-22 | 規格候選v1完整帳務日 | 本輪v2完整帳務日 | 本輪完整組件 | 組件內正成交契約日 |
| --- | ---: | ---: | --- | ---: |
| SPF | 1,810 | 4,328 | 17個lifetime、3,841列 | 1,631 |
| UDF | 2,148 | 3,460 | 14個lifetime、2,945列 | 1,737 |

此前SPF沒有完整組件，UDF只有4個、979列；本輪兩商品的保證金缺口皆為0。
剩餘2／4列庫存續行阻擋位於2020-11-20，涉及跨越本候選規格結束日的月份；
需接2020-11-23修訂規格，不能刪掉跨界負債或假設強制平倉。
來源報價與特徵逐值相等，完整組件的價格限制與帳務續行均由新程序核對。
這是金融候選進展，尚未替換遠端717碼，也沒有宣稱755個正成交碼全部准入。
見[金額／元件進展與SHA](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/fixed_margin_component_progress_v2.json)。

下一層已由同一all-TWD時鐘owner完成08:45／09:00一分鐘窗查核。
所需825個歷史日期的全市場Tick物件原已complete；原始Parquet合計0.758GB，
不重抓既有資料。實際使用有金融規則的824日，逐日核對全日單式量、高低收與真實成交，
3,361個金融候選正成交契約日全部驗證通過、0個未驗證；未驗證日期不能提供entry。
完整讀回88.012秒，這是來源讀回耗時，不是GPU訓練加速。

| 商品 | 08:45有窗口成交／50%參與率至少一口容量 | 09:00同口徑 |
| --- | ---: | ---: |
| SPF | 308／148 | 182／85 |
| UDF | 977／843 | 786／599 |

這是窗口容量上限，尚須前日容量、帳戶保證金、正式publication與可攜時鐘manifest，
不等於GPU訓練准入。3,361分母排除7列僅保留作必要前置歷史的正量warmup觀測，
與上表完整組件的3,368個正量來源列不同。
見[全日與窗口原件證據](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/fixed_staged_execution_window_readback_v1.json)。
新程序另核對原件與輸出pin、3,361列全日coverage、全部entry來源鍵、有限正報價及
各商品兩時點的整口容量計數，見[獨立讀回](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/fixed_staged_clock_independent_readback_v1.json)。
新資料／查核留在`data_tw_index_futures/`，
來源修復收據與GPU策略績效分開，不把時鐘可讀取當作完整訓練准入。

## 已排遠端：多摘要殘差策略／Multi-summary Residual Policy

兩組為`factors16_0845`與`factors16_0900`，把聯合股票／期貨市場的學習摘要由4增為16。
它們是注意力摘要，沒有宣稱已得到16個可解釋風險因子或完成Attention Factors論文重現。
目的是檢驗原低維摘要是否使不同股票與期貨訊號被共同市場方向淹沒；
也可能增加過擬合或beta，必須用實際費後、持倉、殘差及跨年度結果判斷。
這是架構假說，不是既有六組尚未完成就宣稱成功。

保持717碼資料、d32、lookback32、batch128、Muon、BF16、簡單log utility、
整口跨日帳務、相同起始資本與100回合無改善規則；兩組均從零訓練，沒有舊模型微調。
以既有4摘要訊號組作同時點對照，不額外強迫beta／名目中性。
對槽位數的主交互作用為O(K×(股票數＋期貨槽位數))，不引入全槽位S²注意力。
每組正式開始前仍要通過四回合完整GPU工程、非零有限梯度、曲線與checkpoint驗收。
61項設定／checkpoint／真實帳務反傳回歸通過，包含16摘要query的非零梯度。

新凍結source SHA `6d3617f9131ed62d4d60cc76194ef00dba04a067bcd0f7eeac0e59a656554b15`，
request SHA `943f8296db9bf2d6c7c913b43854b1a8c0606e220e26b5e9bbef7dccc04150a1`。
3,408份交付檔逐檔SHA核對；02:48:57 UTC獨立回讀Supervisor
`stockagent-futures-factor-context-20261027-v1`存活，stage為等待原六組，GPU尚未啟動。
原六組PID4176783不變；不搶用原訓練GPU租約。
設定與正式產物索引見[新兩組目錄](../artifacts/markets/tw_futures_factor_context_20261027_v1/README.md)。

## 14:19 UTC+8讀回：第二組收斂與765碼續修

08:45殘差訊號／Residual signals在245回合停止，最佳validation為145回合，
連續100回合無改善。相同145個測試日，費後報酬18.39%、Sharpe1.507、
Sortino2.165、MDD13.02%、TX每日相關0.751；對照為17.56%、1.471、2.107、12.69%、0.752。
增加訊號沒有明顯降低共同市場曝險，也還沒超過舊09:00名目座標＋Muon結果。
不能把validation loss改善等同超額獲利機制成立。
前兩組56份報告及40張PNG已回本機並逐檔SHA／解碼驗證，權重仍在遠端。
見[完整績效與產物](../artifacts/markets/tw_futures_residual_signals_20261027_v2/README.md)。

765碼續修從可觀測因果鏈分解：商品在某天存在，具有完整合約義務與估值，
決策時知道可用資訊，決策後確有可負擔的真實成交，才形成可訓練的交易。
不要求全期間上市交集，也不以有一列日行情代替這四層證據。

本輪將官方公告的名稱搜尋由簡稱擴至完整名稱；新增15份215頁規章全部抽取完成、
無失敗，另確認2008年黃金上市Word原件已存在。
全部重用canonical archive，沒有對已complete的FinMind／TAIFEX資料重複呼叫API。
黃金早期單一±15%限制、後來分階段限制、2026年契約大小調整，
以及國外指數最後交易日與次日現金結算日，都必須依 dated source處理，不能套現行規格。
另追加4份176頁2015～2017結算作業原件，已全部完成擷取，無需購買；
本輪共19份391頁、0失敗。2015-12-11上市函明確將現金結算修訂自2015-12-21實施。
海外指數09:30現金結算以最後結算價已可取得為條件，否則為14:30～17:00作業窗；
日資料上存在最後結算價，並不能單獨證明它在09:30前已可取得。
因此保留此條件，不把到期夜盤收盤時間或來源擷取時間當成現金結算時鐘。

NYF→NY1沿既有研究估值ABI凍結停牌前完整契約價值，第一天3個阻擋降為0。
保留官方原價、量及成交mask，沒有把停牌估值冒充成交，沒有新增terminal cash conversion。
NY1的25個正成交契約日及CPF的20日全日核對均通過；
一分鐘容量為0，延長預先承諾委託至300分鐘後，至少一口容量上限分別為21日及12／11日。
這是執行政策可調的證據，還不是金融發布或實際成交准入。

帳務重建移除重複解析官方每日原件的成本，保留輸入雜湊、版本、完整覆蓋及前後一致性閘門。
相同NYF／NY1完整帳務重編及組件選擇，124.264秒→40.589秒，3.06倍，
十份財務產物完全相同；161項相關測試通過。
只對此14,698列工作成立，沒有外推765碼、完整fold或GPU吞吐量。

全765碼、4,968,220列實體契約日的canonical `all_twd`準備已成功結束，耗時2,423秒。
選出753碼完整組件，其中745碼有正成交；比原報告新增CPF、SPF、UDF三碼，原有碼無損失。
三份全765碼官方日證據／生命週期／每日估值檔與父版本SHA完全相同。
候選materialization有4,560,837列，含必要的零持倉特徵前置歷史，
狀態為`selected_materialization_ready_not_published`，尚未通過金融發布及訓練准入。
既有717碼GPU訓練保持原輸入與request，不宣稱已採新資料。
另已形成兩時點×三種委託／容量政策的六組比較規劃；
來源時鐘、金融發布、完整庫存組件及四回合GPU工程驗收完成後才能建立正式執行request。
見[本輪證據、剩餘修復與實驗規劃](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/README.md#本輪續修公司行動委託期間與重建成本)。

另以獨立2010年補充範圍檢查原僅存在於2010年的CV1／DK1，不改原2011年baseline。
CV1的36列、15個正成交契約日全部通過完整金融組件；DK1選出20列、1個正成交契約日，
仍有1列庫存終止續行待修。沒有新增估值或終止現金轉換假設；分鐘成交與特徵來源仍待驗收。
FinMind官方Tick／分K起點2011-01-03是該來源限制，沒有將它擴大解讀成所有來源均不可恢復。
見[2010年獨立讀回](../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/history2010_independent_readback_v1.json)。
