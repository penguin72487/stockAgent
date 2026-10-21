# 股票當沖研究結案與多檔股票期貨研究起點

更新日期：2026-10-21。使用者選擇結束本階段股票當沖搜尋，保存成果後轉向多檔股票期貨，允許跨日持倉。

## 本階段保留的模型

目前「最強」定義為這批已完成實驗中，完整 181 個交易日的 Sharpe 最高。模型是**參數空間單位 MGDA／Parameter-space unit MGDA**，資料夾名稱為 `geometry__unit_mgda_sgd_step`，選中的 epoch 為 128，連續 100 回合沒有改善後於 228 停止。不同指標有不同優勝者，沒有一個模型在兩個比較區間的四項指標全部勝過它。

| 策略／Strategy | 181 日報酬 | Sharpe | Sortino | MDD |
| --- | ---: | ---: | ---: | ---: |
| 參數空間單位 MGDA／Parameter-space unit MGDA | 153.13% | **5.933** | 17.091 | 6.83% |
| 單一對數效用／Single log utility | **377.03%** | 4.993 | 9.293 | 13.48% |
| 單步方向議價／One-step DiBS | 79.15% | 3.781 | **19.914** | 3.34% |
| 指定舊模型／Original fold 11 | 95.93% | 3.662 | 6.353 | 9.57% |
| 2330 買入持有／Buy and hold | 62.73% | 2.014 | 3.359 | 13.04% |

181 日為 2026-01-02 至 2026-10-02。2330 是還原收盤價的買入持有參考，為未扣交易成本的總報酬序列，不是每日平倉策略。策略指標來自現有帳務與費用設定；這個參考不等同相同資金帳本的可成交策略。

相同起始帳戶的共同 149 日重播中，MGDA 為報酬 **125.76%**、Sharpe **7.052**、Sortino **22.290**、MDD **3.03%**；指定舊模型為 **69.85%、3.485、5.995、9.50%**。這兩個區間不能互換比較。

最後一輪 11 組已完成並補齊共同日期重播，與前 51 組合計 **62 組**。SAM、ASAM、XSAM 與恢復版本沒有在兩個區間的四項指標全面超越原 MGDA。最終逐案數值、選中與停止 epoch、原產物路徑及來源雜湊，見 [完整結果](../artifacts/operations/daytrade-closeout-futures-20261021/neural-experiment-results-20261021-final.json) 與 [CSV](../artifacts/operations/daytrade-closeout-futures-20261021/neural-experiment-results-20261021-final.csv)。這些是有限範圍的觀察，不能推論所有方法或超參數都已耗盡。

## 第一性原理結論

訓練訊號、部位與帳務共同決定淨報酬路徑。優化器只能改變如何使用現有訊號，無法創造資料沒有提供的預測資訊。單一 Log Utility 偏好較高複利增長，這批實驗同時出現較高報酬與較大回撤；MGDA 讓多個績效梯度的尺度與方向參與更新，這次選出了較平衡的路徑。這個觀察支持保留兩者作為不同取捨的參考，不能把 MGDA 的局部梯度性質解讀為未來績效保證。

這些股票實驗的 2026 結果已被反覆觀察、比較和用來安排下一輪研究，不能繼續宣稱為未觸碰的樣本外測試。應將其作為研究集，未來另留鎖定的測試區間。相同 seed 也不能讓改動後的優化軌跡相同：精度、梯度合併與整張交易門檻都可能改變後續持倉。修正後控制組與歷史模型的差異，不能全歸因於某一個新方法。

原 14,726 欄 factorized manifest 明列 `research_only=true`、`historical_point_in_time=false`。延後一列能防止直接讀取下一列，但不能補造財報與外部資訊的歷史發布時間。本階段績效仍是該研究資料契約下的回測，尚未證明可部署的歷史 PIT 因果性。

整張／整口交易的 authoritative forward、費用、可成交遮罩與破產狀態須維持一致。整數交易的 backward 使用明列的 surrogate，不能稱為離散交易函數的精確導數；分塊反傳亦需揭露狀態延續與截斷邊界。先前帳務 oracle 校驗與非零梯度檢查支持其工程機制，並不能證明未來獲利。

吞吐量必須包含訓練、驗證、測試、報表與 checkpoint。既有股票加速結果只在各自硬體、shape、快取與帳務 ABI 成立；不能宣稱期貨版已只剩硬體瓶頸，亦不能把股票年度分塊帳務直接套到必須延續存貨的期貨帳本。

## 冷存與回收

最強模型原始 fold：

```text
artifacts/operations/training-quality-audit-20261015/scratch-gradient-geometry-v1-retained/artifacts/markets/tw_scratch_gradient_geometry_20261019_v1/_capacity/geometry__unit_mgda_sgd_step/attempt-00/training/fold_10
```

這個本機目錄已完成 D 槽冷發布、獨立重建、原始檔案逐檔解碼與 SHA 校驗，並經引用與隔離交易檢查後回收。**今後請使用 [模型與冷庫索引](../artifacts/operations/daytrade-closeout-futures-20261021/best-model-index.json)，不要把舊熱路徑當成仍存在的模型。**

固定冷庫資料集為 `legacy-wsl-offline-11ea2b79e4cea2ca06eb2255`；固定 snapshot 為 `legacy-wsl-offline-11ea2b79e4cea2ca06eb2-20261021T032649138233399Z-l0-penguin-69677a834bf3282e`，位於 `/srv/stockagent-packed`。此證明不代表 NAS 已完成複寫。checkpoint、模型、特徵正規化、報表及原始回測檔的 SHA 與還原資訊都記在索引中。

其餘清理依凍結清冊執行。沒有完成冷庫精確恢復、仍被程序或服務引用、含不支援的連結、未達穩定時間的來源，保留並列出原因；不依目錄名稱推定可刪。原始股票與期貨資料、source receipts、期貨準備來源及仍在使用的部署產物另行保護。進度證據位於 `artifacts/operations/daytrade-closeout-futures-20261021/`；任務 ID 為 `daytrade-closeout-futures-20261021`。這份報告不是全部產物已回收的收據。

## 多檔股票期貨的經濟與資料契約

使用者選定多檔股票期貨，保留跨股票訊號及選擇能力，允許隔夜與多日持有。股票特徵是訊號，期貨實際契約價格、乘數與整口存貨決定損益。延續持倉可降低不必要的每日重開成本，也增加隔夜跳空、追繳與公司行動風險；是否改善績效需由新帳本實測。

依本次查核的期交所官方規則，第一批資料需逐契約檢查：

| 邊界 | 實作要求與依據 |
| --- | --- |
| 契約與上市 | 股票期貨標準契約通常對應 2,000 股、小型 100 股；ETF 與調整契約有不同單位，必須使用當時契約條款。[契約規格](https://www.taifex.com.tw/cht/2/sTF) |
| 決策與成交 | 08:45 決策只使用已知的股票歷史與前期期貨觀察。不得讀取尚未發生的 09:00 股票開盤；每日開盤價僅為研究成交代理，須另外測滑價與延遲。[交易制度沿革](https://www.taifex.com.tw/cht/4/historyOfTrading) |
| 隔夜與結算 | 延續同一 physical contract 的整口存貨，每日洗價；維持保證金不足依既定帳本補繳或平倉，破產為吸收狀態。到期依官方最後結算辦理。[交易問答](https://www.taifex.com.tw/cht/9/futuresQA) |
| 保證金 | 採生效日的原始／維持保證金與級距，不以今天標準回填歷史。[股票期貨保證金](https://www.taifex.com.tw/cht/5/margingReqSSF) |
| 公司行動 | 綁定除權息、現金調整與乘數變更，依前一日已持有的有號口數入帳一次；調整後代碼不能視為原契約無縫價格序列。[契約調整](https://www.taifex.com.tw/cht/4/contractAdj)、[交易問答](https://www.taifex.com.tw/cht/9/tradersQATrading) |
| 成本與流動性 | 股票期貨交易稅為成交金額十萬分之二（0.00002）每邊，另加券商手續費與滑價。沿用的每口每邊 40 元只是現有研究假設，非已取得券商報價。[商品問答](https://www.taifex.com.tw/cht/9/tradersQAProducts) |

跨日持倉不要求每個標的都有夜盤。夜盤交易資格、日夜交易日歸屬與最早可用日期仍需逐契約驗證；第一階段以一般交易時段決策與每日結算作研究代理。[盤後交易制度](https://www.taifex.com.tw/cht/4/aHIntroduction)

沿用 canonical `FinancialTransformerFuturesModel → risk_aware_loss → run_backtest_torch → integer margin ledger`，不另造訓練器。模型輸出包含市場訊號與帳戶反應係數，由逐日帳本使用實際存貨、淨值及保證金決定整口交易。舊 MGDA 權重只冷存留作參考，新期貨模型從零訓練。

現有 source universe 共有 765 個商品代碼，其中股票期貨 710 個，對應 389 檔股票，包含歷史及現行、標準及調整後代碼。本次逐一比對原 2,757 檔股票輸入軸，710 個代碼都能找到對應股票。這是來源探索結果，尚非可交易集合；逐日是否上市、到期、可成交及帳務完備仍需另外 admission。清冊及來源 SHA 見 [候選商品](../artifacts/operations/daytrade-closeout-futures-20261021/futures-stock-product-candidates.csv) 與 [比對證據](../artifacts/operations/daytrade-closeout-futures-20261021/futures-scope-discovery.json)。

一般交易時段的原始逐月契約觀察，自 2014-01-06 至 2026-09-24 共 3,547,197 筆、629 個上述商品代碼；其中 1,400,181 筆同時有正成交量與有效開盤價。另有 35,723 筆未平倉量大於零但沒有當日觀察結算價，不能據此直接建立可跨日持有的交易集合；須由完整的官方最後結算、公司行動與逐日估值規則裁定。這個檢查未補值，且尚未依公司行動拆分 physical lifetime。原 parquet SHA 已與來源清冊核對，見 [來源品質觀察](../artifacts/operations/daytrade-closeout-futures-20261021/futures-source-quality.json)。

## 已實作的橋接與接續實驗

新設定為 [stock futures factorized carry v1](../configs/markets/tw_stock_futures_factorized_carry_20261021_v1.yaml)。保留 14,726 欄介面、d32、lookback32、batch128、1,000 回合上限及連續 100 回合無改善早停；移除舊期貨設定的特徵瓶頸與 temporal basis，保留手續費後單一 Log Utility。沒有載入舊 checkpoint、跨 fold warm start 或四個指標平均 loss。

新設定關閉舊期貨的 `frozen_contract_value_research_v1` 估值例外，要求新 release 另行通過官方／觀察價格的帳務覆蓋；缺少持倉估值時拒絕通過，不補造價格。沿用的券商保證金倍率 1.0、風險強平比例 0.25 與每邊 40 元仍是明列的研究假設，需要獨立的券商條款與成本敏感度驗證，不能標成期交所統一規則。

共享 factorized attachment 現在接受股票期貨模式，並在展開大特徵前檢查小型 base panel：`next_session_open_gap_logret` 必須全部有限且為零。新設定明確歸零該欄，防止 08:45 使用當天 09:00 資訊；其餘研究欄仍保留原本非 PIT 的限制。時點契約也加入 panel fingerprint。分塊特徵路徑與期貨輸出頭的介面已接通，需以 dense oracle 核對動作與參數梯度。

本次工程驗證：factorized、期貨 causal account、股票 context 與保證金四個測試模組共 **158 passed**；遠端封存控制模組 **43 passed**；股票期貨範圍與資料準備／稽核模組 **91 passed**，合計 **292 項**。dense 與 factorized 的期貨動作及全部參數梯度已通過 oracle 比較，且確認多個參數有非零有限梯度。新 config 已通過載入與語義檢查。這些是工程結果，目前未進行新期貨實際 GPU fold 訓練。

| 順序 | 實驗／驗證 | 成功條件 |
| --- | --- | --- |
| 1 | 資料與帳務 admission | 固定 source release；只選有原股票映射且帳務完整的股票期貨；上市、成交、到期、調整、保證金逐契約覆蓋，排除項逐筆揭露 |
| 2 | 單一對數效用／Log utility carry | 從零、相同起始 1 億元帳戶；逐日延續整口存貨，完整跑到 100 回合無改善，輸出全部曲線及收據 |
| 3 | 期限與持倉消融／Tenor and holding ablation | 同資料與費率，比較自選期限、近月及每日平倉；在研究分割內選擇，最後測試不調參 |
| 4 | 梯度幾何／Gradient geometry | 先為 recurrent futures bridge 驗證整條軌跡 VJP，再比較參數空間單位 MGDA；現有 trajectory cadence 僅支援可分解 Log Utility，不能直接標成 MGDA 可跑 |
| 5 | 吞吐量／Throughput | 真實 GPU 上測冷準備、完整 epoch/fold、graph capture、驗證、報圖及 checkpoint，逐項報剩餘 CPU/IO/序列依賴；每次加速保留 forward 帳務 oracle |

舊期貨 `margin_components_current` 準備路徑目前不存在，原始來源仍保留在 `data_tw_index_futures/preparation_sources/`。因此新 config 使用獨立、待建立的股票期貨 release 路徑，避免誤用舊的全商品範圍或移動別名。正式 GPU 訓練要等固定 release、時點與帳務 admission 通過；目前沒有新期貨績效結果。

共享 builder 新增 `--stock-futures-only`，以來源的 `asset_class=stock_future` 與明確股票映射限制商品範圍，保留調整後代碼，不以固定 2,000 股篩掉公司行動契約。商品清單及範圍進入 accounting checkpoint 身分，不能與舊全商品結果相容續跑。資料準備已於 2026-10-21 12:10（臺北）由既有監督入口啟動，run 為 `closeout-stock-futures-preparati-20261021T040954-4d8edffb`，輸出為 `artifacts/data_preparation/tw_stock_futures_carry_20261021_v1/`。建置前容量准入通過，原始 55,852 個來源檔案由 canonical builder 逐一驗證；未使用舊凍結估值 policy。啟動並不代表 release 已接受，請以 [準備進度](../artifacts/operations/daytrade-closeout-futures-20261021/futures-preparation-progress.json) 與最終 `build_acceptance.json` 為準。

這批期貨原始資料終點為 2026-09-24，股票特徵則到 2026-10-02。已沿 `build_tx_front_benchmark.py` 核對原始 37 個官方檔案並重建獨立 TX 參考，涵蓋 2011-01-03 至 2026-09-24 的 3,850 個交易日、189 次換月。新 config 綁定這個 reference 的 SHA；既有訓練入口會依其實際終點裁切股票 panel，避免把後續股票日算成零報酬。reference 位於 `artifacts/data_preparation/tx_front_reference_20261021_v1/`。正式資料 admission 仍需檢查中間每個期貨交易日、股票 context 與分割邊界是否完整。

台指期一倍名目本金滾動序列可保留作市場方向參考；若比較「一口台指期加必要補款」，需獨立維護現金流、費用、換月與資金分母，不能用 gross 指數序列代稱此投資帳戶。

## 文獻提供的方法方向

[Deep Momentum Networks](https://arxiv.org/abs/1904.04912)（Lim、Zohren、Roberts，2019／2020）支持從特徵直接學部位、把績效與成本納入訓練的研究方向；其跨市場期貨實驗不等同本研究已通過的台灣契約帳務或績效。[Deep Hedging](https://arxiv.org/abs/1802.03042)（Buehler 等，2018）提供在交易摩擦及約束下學習動態策略的框架；避險研究不能直接當作獲利證據。本次查核這兩篇的摘要與研究設定，未宣稱已逐篇重現全文結果。

本階段的研究決定是保存已得到的風險／報酬取捨，將下一輪實驗資源轉向不同的持倉經濟與標的契約。是否更好，由新研究的資料時點、現金帳本與保留測試集一起裁定。

## 2026-10-21 接續：08:45／09:00 配對決策實驗

使用者要求兩個時點都實作並排程，並已明確選擇「先只改決策時間，兩組使用相同歷史特徵」。因此固定相同資訊集合，隔離決策／成交時間對交易機會與策略學習的影響。兩組只使用前一個已完成交易日的股票與期貨特徵，當日股票開盤 gap 歸零；**09:00 組也不假設所有股票在該瞬間已完成開盤、且模型已收到價格**。股票開盤可能延後，交易所時間不等於資料接收時間。[TWSE 交易制度](https://www.twse.com.tw/zh/products/system/trading.html?hl=zh-TW)

| 實驗／Experiment | 決策 | 相同的一分鐘計算延遲 | 執行代理與產物子目錄 |
| --- | --- | --- | --- |
| 開盤前歷史資訊策略／08:45 prior-session policy | 08:45 | 08:45–08:46 | `[08:46,08:47)` 成交 VWAP，`clock__0845` |
| 開盤時歷史資訊策略／09:00 prior-session policy | 09:00 | 09:00–09:01 | `[09:01,09:02)` 成交 VWAP，`clock__0900` |

一分鐘 bar 採右端標籤，因此取 08:47／09:02，而不是把截至決策時點的 bar 當成稍後可成交價格。這是兩組共同的研究延遲及 trade-print proxy，沒有重建 bid/ask 深度、交易衝擊或券商實際成交。舊 09:00 設定若仍使用期貨 08:45 日 OPEN，不屬於本次配對。

跨日損益來自舊存貨與新存貨兩段：`q_old × (entry_value − prior_value) + q_new × (end_value − entry_value)`，再依既有帳本扣實際變動口數的費用與稅，另處理公司行動。只換 OPEN 價格會漏改乘數／交付現金、初始及維持保證金、入場稅、漲跌停方向和容量。因此新 [decision-clock adapter](../stockagent/data/tw_futures_decision_clock.py) 在 canonical margin attachment 完成後重建這些入場欄位，保留前日狀態、結算／到期、公司行動與獨立的終端成交容量，模型特徵及候選遮罩不讀取未來分鐘有無成交。

入場容量為 `min(原前日容量, floor(該分鐘已觀察口數 × participation))`。完整來源證明沒有該窗口成交時，入場容量為零、保留原存貨及真實結算損益，前日官方值只供估值，不是假成交；來源缺失則拒絕 admission。不得回退 08:45 日開盤價、補造分鐘、補零報酬或刪除失敗日期。

兩組沿用本輪期貨基準：原 14,726 欄研究輸入、d32、lookback32、batch128、起始一億元、單一 Log Utility、相同 AdamW、從零訓練、無跨 fold warm start。最多 1,000 回合，連續 100 回合改善未達 0.0001 才早停。都先跑最後一個 fold；依現有日期與 `split_start_year=2020` 解析為 **fold 5**，失敗個案跳過繼續下一組。訓練、驗證、測試、曲線及 checkpoint 均由原 [ablation runner](../scripts/run_ablation_experiments.py) 及 canonical trainer 執行。

- [08:45 設定](../configs/markets/tw_stock_futures_carry_0845_20261021_v1.yaml)
- [09:00 設定](../configs/markets/tw_stock_futures_carry_0900_20261021_v1.yaml)
- [配對排程 spec](../configs/ablations/tw_stock_futures_decision_clocks_20261021_v1.yaml)

歷史分鐘自 2020-03-23 才有實際證據；兩組使用相同的 2020-03-23 至 2026-09-24 模型區間。先建立原 2014 起 factorized source ABI 與完整公司行動／前日存貨關係，再裁切共同研究區間，不重寫原特徵 manifest 或遺失 panel fingerprint。原資料的非 PIT 研究限制仍在，這不是部署就緒證明。

本輪股票期貨金融來源已完成 canonical preparation：599 個歷史商品代碼、1,529,524 個契約日；111 個未通過完整帳務範圍的代碼另列，選擇屬事後資料品質範圍，不是 599 個當前同時可交易商品。來源有 3,096 個日期、2014-01-17 至 2026-09-24，全部可對應原股票日期。來源與排除證據在 `artifacts/data_preparation/tw_stock_futures_carry_20261021_v1/build_acceptance.json`，不能把金融帳務覆蓋當作分鐘成交覆蓋。

最後結算原收據仍引用舊 `artifacts/markets` 路徑。透過 [共用來源 staging](../scripts/stage_tw_futures_margin_sources.py) 的 `materialize_portable_final_settlements`，依 source manifest、原位置 map 與原始內容 SHA 產生新的相對路徑投影；保留原收據、原始檔與 parquet 原位元組。新投影的 **430 筆來源收據、51,608 筆結算列**已通過 canonical reader，資料 SHA `82c77c8c9c8194be26cfce45e226815925bd5a43fc8f37f1b8fe9ef47da8be12` 不變。原收據與 source bundle 沒有修改。

實際 2020-03-23 有 591 個舊來源契約日；127 個通過歷史來源原始檔與收據 readback，兩個入場窗口有成交的分別為 13／24 個，另有 199 個仍需補分鐘證據，不能把這一日稽核當成新範圍全歷史已可訓練。新 [配對來源 builder](../scripts/prepare_tw_futures_decision_clocks.py) 正在對新來源完整檢查、比對明確的逐日商品／交割月與 lifetime，並以官方高／低／收及量上限查核歷史與 exact-contract KBar；缺口輸出 `coverage_gaps.parquet`。

工程驗證包括新增 **14 項**時點、無成交持倉、入場重估、來源錯配、資料裁切、世代 identity 與結算來源搬遷測試。含原分鐘來源的 focused run 為 **54 passed**；另保證金帳務 regression、原排程／股票 context／margin binding／all-futures preparation 的 **376 passed** 均已通過（不同 run 有重疊，不相加）。遠端兩張 RTX 5090 已通過 initialized CUDA strict check；1814 個凍結程式／設定檔已獨立驗證，code SHA 為 `01c690f490c8647d1c790c9becd1450671ea96c85a161d8a9d164a19152d4f3a`。尚未有新模型績效或本工作負載的完整 epoch 吞吐量結果。

已實際啟動的排程：本機 systemd run `futures-decision-clock-prepare-20261021T045415-89d534b5` 準備及傳送固定資料；遠端 Supervisor program `stockagent-futures-decision-clocks-20261021` 等待相同 pair identity、逐檔 SHA readback、資料驗證與 GPU lease，通過後依序啟動兩組。等待資料期間不佔用訓練 GPU；24 小時未 admission 則留下 deferred 狀態，不將 timeout 視為成功。

遠端訓練產物預定在 `/root/stockAgent/artifacts/markets/tw_stock_futures_decision_clocks_20261021_v1/{clock__0845,clock__0900}`。本機來源準備位於 `artifacts/data_preparation/tw_stock_futures_decision_clocks_20261021_v1/`。request、程式 freeze、CUDA、即時狀態與 readback 證據集中在 `artifacts/operations/daytrade-closeout-futures-20261021/clock-*`。GPU 訓練狀態必須讀 `clock-remote-progress.json`，不能只看 Supervisor `RUNNING`。

## 2026-10-21 修訂：真實開盤／前筆成交參考 v2

使用者進一步定義開盤參考價，並確認：**該交易時段第一筆開盤成交已被觀察到，就固定使用該筆成交；尚未觀察到開盤才沿用決策時點已知的最近一筆真實成交**。前日實際收盤成交或有真實成交證據的盤前價均可成為參考；官方結算、試撮、無成交報價不能冒充成交。來源缺失時保留已知成交及其原始時間，不宣稱已證明沒有新成交。本節及 v2 設定取代上一節 v1 的排程；v1 的程式與 request 收據保留，遠端 program 已於 05:05 UTC 停止，未開始 GPU 訓練。

第一性原理上，市場觀察回答「模型當下知道什麼」，成交帳本回答「決策之後能成交多少、付多少成本」。兩者分離：新輸入不重寫成交價、成交量、容量、整口存貨或損益。共享 adapter 為每個物理契約保留原始成交日、成交時間（未知則留空）、可知時間界限、來源 SHA、參考種類與沿用天數。模型新增四個相對尺度通道：相對前筆歷史成交的對數變動、是否有參考、是否當期開盤、沿用天數的 `log1p`；以小型線性編碼器接入原期貨 token。舊特徵、候選遮罩、帳戶回饋索引與原始帳務不變。

歷史來源只有日線與完成分鐘時，不能還原實際接收延遲。前一完整交易日的實際收盤價帶日期精度；當期開盤價只在經驗證的首個成交分鐘完成後才可見，並與該分鐘高低價核對。這是明列的歷史研究可知時間界限，不能標成逐筆接收時鐘。故一般 08:45 歷史樣本沿用前筆成交；09:00 已有開盤證據時使用固定首筆開盤價，沒有證據時仍保留已知前筆及來源狀態。

新設定：[08:45 開盤參考](../configs/markets/tw_stock_futures_carry_0845_openref_20261021_v2.yaml)、[09:00 開盤參考](../configs/markets/tw_stock_futures_carry_0900_openref_20261021_v2.yaml)、[v2 配對實驗](../configs/ablations/tw_stock_futures_decision_clocks_20261021_v2.yaml)。兩組仍從零、最後一個 fold 5、d32／lookback32／batch128、起始一億元、單一 Log Utility、最多 1,000 回合、連續 100 回合無改善才早停；失敗個案跳過繼續。新來源與編碼器契約寫入 checkpoint fingerprint，拒絕舊模型或來源不同的 resume。

完整來源核驗於 v1 查出 371,277 個可執行契約日仍缺分鐘證據；接續 exact KBar 核驗又遇到 `CAFC7` 在 2026-09-01 的一口成交 `OHLC=240.5`、`Amount/Volume=240.0`，原始檔與收據保留。v2 共享讀取器仍嚴格拒絕該價格；來源準備可將錯誤逐日記錄為修補工作，繼續核對其他日期。有效的另一來源仍可補足該日，不能把錯誤或缺失當成零成交。另修正物理契約世代對應：同一券商 root／交割月在不重疊日期可屬不同 lifetime，同日歧義則拒絕。

新遠端產物根目錄為 `/root/stockAgent/artifacts/markets/tw_stock_futures_decision_clocks_20261021_v2/`，兩個子目錄為 `clock__0845`、`clock__0900`。模型輸入與成交證據根目錄為 `artifacts/data_preparation/tw_stock_futures_decision_clocks_20261021_v2/`。新排程 request、凍結程式、核驗及進度集中於 `artifacts/operations/futures-opening-reference-20261021-v2/`；讀 `remote-progress.json`、`data-status.json` 及各組 canonical trainer 產物判斷是否真正開始訓練。來源完整覆蓋與遠端獨立 readback 前不啟動 GPU；排程等待不代表已有新模型績效。

v2 工程驗證為 **69 passed**（開盤首筆固定、沿用前筆、未來擾動不洩漏、世代隔離、輸入與成交分離、真實整口帳本反傳、來源／ABI 變更拒絕續訓及原 KBar regression），另有帳務與 checkpoint **223 passed**。兩組 spec dry run 均解析為最後一個 fold 5。上述為工程證據，尚未完成真實 GPU fold 或吞吐量驗收。

截至 2026-10-21 13:31（臺北），新版遠端 Supervisor `stockagent-futures-opening-reference-20261021-v2` 已實際啟動，狀態為 `waiting_for_causal_reference_and_executable_minute_admission`、`gpu_training_started=false`。本機 systemd run 為 `futures-opening-reference-prepar-20261021T052924-3ff5bb74`，已進入 350,271 個歷史來源候選的 raw readback。凍結 code SHA `f0529d2935e45cf80ddadf85afa392d2b6065e19f9e7822a2cdbf75cee38fa8c`、1,817 個程式／設定檔已遠端獨立核驗，兩張 RTX 5090 的 initialized CUDA strict check 通過。request SHA `a00efb9dd6001556f6c9ff55001c40aad2c232965661292fba62f4d109dd6106`。來源缺口若無法補齊，佇列保留阻擋原因，不會繞過缺口開始訓練；24 小時未 admission 則退出並記錄 deferred。
