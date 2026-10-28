# 歷史725商品清冊、逐日可交易範圍與擴充歷史實驗

## 直接結論與目前分母

2026-10-25 於 `114.34.26.236:43269` 逐列讀回財務來源：
2,004,377列，725個歷史商品代碼，其中682個股、27 ETF、16指數。
代碼包含公司行動調整契約，對應412個已知underlying識別，不能把725解讀成
同時存在的725個獨立標的，也不能假設它們有725種獨立市場風險。
已通過財務核驗的歷史片段為2011-01-26至2026-09-24，本次測試為2026-02-26至2026-09-24。
這是完整代碼清冊下的已核驗片段，不是每個實體契約整段歷史全部通過。

| 互斥分類 | 代碼數 | 含義 |
| --- | ---: | --- |
| 最後財務合格來源紀錄早於2020-03-23 | 256 | 原2020起的訓練窗完全看不到這些代碼 |
| 2020後有財務合格紀錄，但該片段在測試前結束 | 93 | 不等於法律上已下市；必須再查原始來源 |
| 測試有來源紀錄、沒有可成交日 | 1 | LR1，本期間只有不可執行資料 |
| 測試期間至少一日可成交 | 375 | 全期聯集，每日實際數量更少 |
| 合計 | 725 | 完整對帳，沒有把未選中的商品刪掉 |

不能把這349個「財務合格來源片段在測試前結束」全部說成商品到期。
再以SHA已核驗的官方原始日資料逐代碼交叉核對，725個的真正測試分母為：

| 原始來源與准入分類 | 代碼數 |
| --- | ---: |
| 測試期間原始資料沒有紀錄 | 343 |
| 原始資料有紀錄但無正成交與完整成交價 | 3（LR1、LX1、MAF） |
| 原始有成交，但目前未進入財務合格片段 | 4（KA1、PZ1、RR1、SQF） |
| 已進入財務合格片段且曾可執行 | 375 |
| 合計 | 725 |

其中KA1／RR1分別有瑞儀2026-08-13～08-21、寶雅2026-07-30～08-07
母契約暫停期間估值及調整首日參考價缺口；SQF有2026-07-06起的日期版商品規格、
稅率與價格界限缺口。PZ1原有成交，但本代契約的整口容量證明仍不可入場，
不是來源沒有行情，也不是GPU沒有選到。原始資料不能直接越過這些帳務／容量門檻。
精確持倉相依與阻擋日見`current-code-component-worklist.json`。

放寬到原始761個股票／ETF／指數代碼，測試期共有390個代碼有紀錄，387個有
正成交與完整成交價；另外8個未在725清冊的正成交代碼是F1F、NB1、SPF、SXF、
TJF、UDF、UNF、VQ1。它們仍需自己的日期版財務與原始成交核驗，
不能把原始387直接宣稱為已可執行387。
證據：`artifacts/operations/futures-historical-universe-20261025-v2/raw-vs-admitted-test-universe.json`、
`current-code-financial-blockers.json`及`current-code-component-worklist.json`。

測試每日有可成交日資料的商品為295～332個。現有1分鐘時鐘中，
08:45組有311個、09:00組有347個代碼至少出現一次入場成交資料；
這仍未扣除50%參與率取整、前日容量、保證金與組合限制。
同一支新審計程式對舊時鐘實測，在50%參與率、前日整口容量與同實體契約條件下，
兩組測試期至少一次具一口容量的代碼分別為279與323；實際帳本還會套用財務與組合限制。
**可選、具備成交容量與最後實際持有是三個不同分母。**
再以同一份SHA核驗的daily去重，375個測試期可執行代碼對應271個已知標的識別；
逐日為239～264個已知標的。這也不是271種獨立風險，個股、ETF與指數仍會共享市場因子。
證據：`artifacts/operations/futures-historical-universe-20261025-v3/economic-universe-readback.json`。

已核驗歷史片段有可執行日資料的代碼是717個，其餘DR1、FC1、HF1、HW2、IQ2、
JJ1、LG1、NK1保留於來源清冊。向上追查官方原始825列後，這8個並不是沒有官方日紀錄。
財務release的`source_row_observed`是`executable`的別名；應以`official_row_observed`
區分原始日紀錄是否存在，不能據前一欄把官方估值資料說成缺失。

| 剩餘代碼 | 原始資料與拒絕原因 | 改善邊界 |
| --- | --- | --- |
| FC1、HW2、IQ2、LG1 | 現有原始日紀錄沒有正成交量 | 無法憑軟體製造真實交易 |
| HF1、JJ1、NK1 | 各有5、2、1列正成交；依前日容量與50%參與率的canonical整口判斷，可能入場日均為0 | 改容量政策需新帳務ABI，重建空持倉前綴證明與財務來源；不能只放寬mask |
| DR1 | 有原始成交，原完整財務frame有6個可能整口入場日，但相關公司行動組件受估值／終端價值缺口阻擋 | 需要補齊原始估值與延續證據，不能假造終端損益 |

逐列證據保留於`artifacts/operations/futures-historical-universe-20261025-v2/`的
`inventory-only-raw-observations.json`、`inventory-only-capacity-proof.json`及
`inventory-only-financial-blockers.json`。相較原2020起訓練窗的469個
有可執行紀錄代碼，完整歷史可恢復248個，來源覆蓋上限增加52.9%；
是否真正具有整口成交容量、被神經網路選中與改善績效，仍須分別實測。
初次完整讀回見[原始分母證據](../artifacts/markets/tw_futures_historical_universe_20261025_v1/initial-universe-readback.json)。

2026-10-25 13:19完成3,826個原始來源日讀回與四個時鐘的實際覆蓋審計。
全歷史每組2,004,377列；1,569,915個可執行契約日通過逐筆核驗，11個精確停單。

| 決策 | 全歷史1分鐘具一口容量代碼 | 全歷史30分鐘具一口容量代碼 | 測試1分鐘 | 測試30分鐘 |
| --- | ---: | ---: | ---: | ---: |
| 08:45 | 410 | 674 | 279 | 364 |
| 09:00 | 516 | 686 | 323 | 368 |

這些是「全期間至少一天通過窗口量、前日量、同契約一口門檻」的代碼聯集，
不是每日同時可交易、實際成交或模型最後持倉；帳本還會套用價格界限、保證金與組合限制。
30分鐘測試窗口兩組各有374個代碼出現成交，其中HL1在兩組窗口中都沒有觀測成交。
09:00全歷史725清冊可完整分為：686具一口容量、12未在該窗口觀測成交、
19受窗口／前日容量或同契約整口門檻限制、8無已准入日可執行資料。
08:45則是674、17、26、8，均精確合計725。
其中容量與窗口是明示的研究政策限制；若要再放寬，須改版、重建財務可達性與
空持倉前綴證明再驗證，不能把它們一律說成硬體或外部限制。

完整[725列CSV](../artifacts/markets/tw_futures_historical_universe_20261025_v3/coverage/products.csv)
保留各代碼歷史範圍、underlying、原始／財務准入分類、四窗口與整口門檻結果。
來源與SHA見同目錄`universe_coverage.json`、`products_csv_receipt.json`。

## 實作與第一性原理

1. **讓歷史商品回到自己的年代。** 新設定從2011開始建訓練日曆，
   沿用dated physical contract、公司行動、whole-contract carry、保證金與費稅帳務。
   來源清冊涵蓋725，不強迫神經網路買遍所有商品，也不復活已結束的代碼。
   財務發布仍只採通過核驗的完整持倉連通組件，保留未通過片段及排除原因。
2. **區分缺資料與無成交。** v2完成3,826來源日後，確認四日共11個契約日不符，
   而不是原stock-only來源已知的單一缺口。v3逐項SHA讀回後，新增綁定daily/rules SHA
   與精確契約日的`tw_futures_unknown_entry_quarantine_v1`：該日下單容量為零，保留既有持倉與
   官方估值、終端帳務。狀態明記`unverified_execution_quarantined`，
   不冒充`verified_no_window_trade`。新缺口或來源改變會拒絕admission。
   不完整當日逐筆也不能聲稱找到第一筆開盤成交；模型沿用最近已核驗的真實成交及原時鐘。
3. **擴大真實可執行容量。** 比較固定1分鐘與30分鐘窗口，
   都在決策後保留1分鐘延遲，僅聚合窗口內真實分鐘成交量與FP64成交量加權價。
   08:45組窗口為08:46–08:47或08:46–09:16；09:00組為09:01–09:02或09:01–09:31。
   這是明確版本的研究VWAP代理；沒有以開盤參考價、前收或補值代替成交。
4. **刪除重複的來源工作。** 同一批原始逐筆只核驗一次，供四個時鐘窗口共用。
   容量、同一physical identity與來源證書分組均走Polars欄式處理，
   不對日期×商品建立虛構笛卡兒成交表，也不因每個窗口再次下載歷史資料。

第一輪遠端v1核驗保留為`blocked_source_admission`：原價格模組對非stock/TX/MTX
的歷史只開到2020起，並非所有較早商品不存在。v2補齊有官方證據的範圍：

- TE、TF、T5F、GTF、XIF的2011 tick，依
  [期交所2011年5月簡報，第11頁](https://www.taifex.com.tw/chinese/9/股價指數類及股票類契約最後結算價取位方式調整.pdf)
  的普通期貨報價單位；不把最後結算價視為交易價。
- ETF自2014-10-06的0.01／0.05價格級距與初期13:45收盤，依
  [2014年9月原始上市摺頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/ETF期貨介紹摺頁文宣.pdf)。
- 國外成分ETF自2015-07-20延長至16:15，之前維持13:45；到期仍依正式日曆縮短。
  依[期交所2015年延長時段摺頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/ExtendingTradingHours_pamphlet.pdf)。

`TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION`升為5，其他未查核商品仍保留原歷史邊界。
原文已用canonical官方擷取／封存函式保存並解壓讀回驗SHA：
`data_tw_index_futures/preparation_sources/historical_price_rules_20261025_v1/manifest.json`。

v2的第二次阻擋也保留原樣，沒有改寫已封存請求或把缺口標成完整：

| 日期 | 契約日數 | 已獨立讀回的差異 | v3處理 |
| --- | ---: | --- | --- |
| 2012-06-04 | 1 | CTF官方108口，逐筆107口 | 當日停單，保留估值與持倉 |
| 2013-12-25 | 1 | MTX官方17,629口，逐筆17,640口，無逐列差異解釋 | 同上；不任意刪除11口 |
| 2016-01-28 | 5 | NZF／OAF在該日保留的全市場逐筆中完全缺列 | 同上；不當成零成交 |
| 2017-05-16 | 4 | OAF／OKF逐筆量等於競價量加鉅額量，沒有可可靠拆分的交易類別欄 | 同上；不任意扣量或使用混合VWAP |

11項均以日期、physical contract、原始SHA及核驗數量寫成明確清單；
任何新增、減少或來源變更仍阻擋准入，不是自動放行所有缺資料。
v3沿用同一份已核驗程式碼，改用新來源／訓練根與新請求，核驗worker由2調到8；
這只提高獨立原始物件讀回的並行度，不改數量、價格或財務規則。
證據：`artifacts/operations/futures-historical-universe-20261025-v3/source-gap-inventory.json`、
`bootstrap-readback.json`與`launch-readback.json`。

## 遠端排程與產物

新四組均從零訓練，沿用d32、lookback32、名目座標Muon、費後Log Utility、
最多1000 epoch與連續100回合無改善；最後fold先跑，驗證集選checkpoint。
四組正式來源的日曆已通過canonical驗證：最後fold為14，前32個交易日僅供上下文；
訓練2011-03-22～2025-02-25、驗證2025-02-26～2026-02-25、
測試2026-02-26～2026-09-24（145日），沒有遺漏訓練目標日。
完整日曆3,833個交易日，保留145個測試日，與先前投影一致。

| 策略名稱／English | 決策 | 成交窗口 | case |
| --- | --- | --- | --- |
| 歷史商品清冊／Historical universe | 08:45 | 1分鐘 | `twd_equity__history_0845_1m` |
| 歷史商品清冊／Historical universe | 09:00 | 1分鐘 | `twd_equity__history_0900_1m` |
| 歷史商品與寬窗口／Historical universe, wider execution window | 08:45 | 30分鐘 | `twd_equity__history_0845_30m` |
| 歷史商品與寬窗口／Historical universe, wider execution window | 09:00 | 30分鐘 | `twd_equity__history_0900_30m` |

- Supervisor：`stockagent-futures-historical-universe-20261025-v5`。
- 正式產物：`/root/stockAgent/artifacts/markets/tw_futures_historical_universe_20261025_v3/`。
- 資料：`/root/stockAgent/data_tw_index_futures/training_prepared/tw_futures_historical_universe_20261025_v3/`。
- 排程／日曆／進度證據：`artifacts/operations/futures-historical-universe-20261025-v5/`。
- 來源審計與獨立比對：`artifacts/operations/futures-historical-universe-20261025-v3/`。
- 設定：[市場](../configs/markets/tw_futures_historical_universe_20261025_v3.yaml)、
  [四組消融](../configs/ablations/tw_futures_historical_universe_20261025_v3.yaml)。

來源完成後曾因設定繼承依賴未隨部署完整落地而阻擋，保留v3／v4失敗證據。
v5從原SHA封存的所有父設定加上已綁定的v3覆寫，產生無外部繼承的完整基底；
先跑完四組實際設定與日曆檢查，才封存80項輸入並註冊Supervisor。
沒有覆寫遠端既有相異設定。已核驗資料、程式版本、財務與優化器規則及正式產物根均不變，
不用重新掃描原始逐筆；v5只是接續控制器版本。

先完整來源核驗與覆蓋審計，再等待既有beta隊列退出並取得原本的雙GPU獨占lease。
沿用canonical嚴格CUDA檢查、完整4 epoch工程驗收、正式訓練、loss／績效曲線與checkpoint。
個別工程失敗保留證據，其他通過案例繼續。工程暫存由既有retirement helper處理。
有效設定已逐組讀回：d32、lookback32、global batch 128、Muon、Log Utility、
無預訓練與無resume；`record_epoch_curve=true`、`curve_plot_interval=1`、
同步繪圖且不延後到結束。沿用已選取的模型compile、futures CUDA Graph與funding compile。

另用8個真實來源日做2／16執行緒的A/B/B/A診斷，四次投影完整SHA一致；
2執行緒平均3.876秒，16執行緒平均3.696秒，僅約4.9%差異。
這不是完整歷史或訓練fold測速，保留原Polars設定，沒有據此宣稱已找到主要瓶頸或達到極限。
收據：`artifacts/operations/futures-historical-universe-20261025-v3/source-reader-thread-diagnostic.json`。

264項相關測試通過：核心225項（19.28秒），另39項歷史與KBar回歸（214.27秒）。
包括既有帳務、公司行動、容量、長短持倉延續、
精確隔離名單、價格／時段邊界、窗口因果性、錯誤來源證書與歷史清冊分母。
兩組新舊共同期間各993,412列完整比對相等；每組1分鐘／30分鐘開盤參考資料SHA一致。
所有2,004,377列窗口對齊、原容量保留與停單限制均通過，容量／停單回歸各為0。
收據：`artifacts/operations/futures-historical-universe-20261025-v3/one-minute-overlap-parity.json`。
截至2026-10-25 13:25臺北時間，來源、四組設定與日曆已准入，
v5狀態為`waiting_for_previous_canonical_queue`，尚未啟動本批GPU工程／正式訓練。
既有beta訓練確實持續更新，獨立讀回曾確認第215 epoch，而非僅從Supervisor推定。
尚無四組正式收斂結果；完整工程吞吐亦待實測，
**沒有宣稱已達純GPU運算極限或策略績效必然提高**。

己方三個複製建置輸入已經清冊、逐檔恢復SHA、程序／服務／FD／mmap／設定引用、
quarantine再檢查及canonical逐inode unlink後清理：9,768檔，回收241,999,872 allocated bytes；
正式產物及冷物件刪除均為0。收據：
`artifacts/operations/futures-historical-universe-20261025-v2/temporary-build-input-retirement.json`。

研究資料原有`research_only`／`historical_point_in_time=false`限制仍需依來源解讀；
新增時鐘因果檢查不會讓整套回補特徵自動成為歷史實盤可得資料。
原財務來源仍明記`selection_is_retrospective=true`、`full_history_training_ready=false`、
132,622個受阻會計列；不能把新增歷史日曆說成這些缺口也已修復。
四組歷史／窗口實驗維持這一已核驗財務世代，不宣稱同時修好了當前4個代碼的
剩餘准入／容量問題；上表是下一批有來源與精確相依的修復工作單。
測試期已反覆檢視，不是未曾使用的樣本外holdout。其他商品／利率期貨仍未涵蓋，
不能將這725個財務通過代碼宣稱為所有765個原始TWD商品全部通過。
