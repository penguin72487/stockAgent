# 725個歷史期貨代碼的來源修復與成交准入

## 2026-10-26 本輪實測

### 23:39 UTC 原生事件完整財務v485驗收與保證金v534實跑

完整v485四階段及全部獨立控制完成：7組現金、108個公司行動／496列、3,918筆
原生參考值、非目標70個金融欄、雙版完整庫存圖及與父版准入差集均通過。
完成收據`retained-financial-v486-complete.json` SHA為
`040a7d8219fa16e94240cb4b0d83826357bf313c9f22c5c2d8b600dc30cd2dc2`。
原725個帳務主體、721個有正成交准入；固定1,766,347個完整正成交契約日中，
准入1,744,963日（98.7894%）、待修21,384日，比立即父版v452恢復13日、准入損失0。
正式逐碼清單改看`coverage/financial_v485/`，總入口為正式目錄的`README.md`。

全新程序v553於23:39:16驗證31個輸入pin與正式輸出SHA，獨立raw→daily join重現
同一分母；全部可執行發布列為1,745,292，額外329列仍不在完整四OHLC分母內，
逐列原始值保存在`source_recovery/accepted_denominator_v553/`。
原唯讀v552誤假設舊v514的source-gate schema而KeyError，保留原失敗及原日誌；
v553明示驗證當前gate的財務完成SHA、品質來源收據SHA與五筆矛盾，不放寬任何控制。

唯一保證金續行owner已完成v523與事件來源的bounded composition，v534於23:38:41
進入accounting，後續同樣必須完成四階段及全部獨立控制。684個開盤／672個收盤
保證金輸入尚未提前算成v534成交恢復日數。五筆價格來源矛盾仍未解，
`full_history_source_ready=false`與`full_history_training_ready=false`保持；未啟動新GPU訓練。

另查到FinHa候選來源。v550保存4份公開回覆與redirect鏈，v551不再發網路請求，
僅解析保留的JSON字串literal，共107筆目錄、4筆相關原生定義。供應商目錄把期貨逐筆
列為2011起、source=unknown／cadence=locked；分K則明示只涵蓋TX／MTX／TMF／TE／TF，
並揭露歷史tick缺開收盤集合競價印。兩個公開明細連結實測都導回總目錄，尚未取得
成交檔或驗證下載端點，不能把目錄年份當成27個股票期貨時鐘缺口已補齊。
原v550直接解析JS內嵌資料未取到記錄，保留原件後改以literal解析，沒有重送請求、
登入、付款或聯絡供應商；正式證據在`source_recovery/finha_public_catalog_v551/`。
所有供應商已查盡仍為False，後續保持逐操作數、逐來源查證，不以API可達代替資料驗收。

### 23:31 UTC FinMind實際逐商品回讀與測速載荷清理

正式已驗收數字仍是v452：725個帳務主體、721個有正成交准入；固定1,766,347個
完整正成交契約日中准入1,744,950日（98.7886%）、待修21,397日。v485已完成
accounting／selection，當下在publication；v534保證金續行等待其全部獨立控制通過。
來源v523的684個開盤及672個收盤保證金輸入不提前計入財務恢復數。

找到12筆FinMind工作一直pending的軟體原因：整市場下載器的dataset delegation，
把另外要求的逐商品全歷史核對也一併擋住。新增共用有限逐商品核對契約，僅允許
有來源SHA、明示目的的既有精確id_history工作通過該層；其他商品、增量優先順序、
quota／reserve、retry／空回覆／終態、原生身份檢查及單一worker鎖均保留。
v541通過231項測試，v542登錄12個原精確鍵；v544沿既有排空閒置交接流程換入程式，
獨立驗證新owner狀態與schema、既有來源及受保護交易服務身份未變。

v543已取得並驗證全部12個canonical實際回覆的原件、收據、parquet與身份，
不再是僅登錄pending。FC1／HW2／IQ2／LG1分別14／56／28／14列，共112列的
原生量均明示為0，無正量、無NULL；與TEJ原生零量回覆一致，官方原NULL未改寫。
v548再逐鍵核對2011-01-26至2026-09-24、一般時段與單一交割月份的18,097列，
缺席實體帳務鍵、新正成交量、新正結算候選、正結算矛盾、量差異均為0。
範圍外、其他盤別與價差等14,637列單列，不當成訓練恢復量。正式來源報告在
`source_recovery/finmind_product_queries_v543/`與`finmind_product_reconciliation_v548/`。
原唯讀v547因官方結算原生字串直接參與數值運算失敗，日誌保留；v548另開數值比較欄，
保留原字串與缺值，不改官方資料或成交旗標。這些結果未證明其他供應商已全部查盡。

測速v517／v521載荷已按共用local preparation benchmark discard契約完成清理：
1953個檔名、486,123,678個邏輯bytes被移除，但多數為共用hardlink，實際最後連結
釋放的配置量僅1,085,440 bytes（約1.04 MiB），不宣稱回收486 MB。
v545全新程序確認兩個目錄不存在，28個保留來源／控制SHA仍一致；v503失敗來源、
v523正式來源、v452正式財務、原始檔與測速數值／失敗收據均保留。

五筆原生參考價與官方價格界線矛盾、剩餘首日規則／估值與分鐘時鐘仍有具體待查工作，
不能統稱已證明的外源限制。未啟動未驗收的新GPU訓練，仍依使用者指示不購買付費原件。

### 23:15 UTC 完整v452驗收、684個保證金來源輸入與續行v534

目前正式已驗收完整財務為v452，完成收據
`artifacts/operations/futures-universe725-repair-20261025-v1/retained-financial-v453-complete.json`
（SHA `6efbe00ffb121b9948339a973d12b6820040c40c8a9473be9fea04d3327ebfcd`）。
725個帳務主體、721個有正成交准入；固定1,766,347個完整正成交契約日中，准入
1,744,950日（98.7886%）、待修21,397日，比立即父版v433恢復49日、准入損失0，
比v115累計恢復107,230日。正式逐碼清單在
`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/coverage/financial_v452/`。
全新程序v526驗證31個輸入與正式輸出SHA，獨立raw→daily join重現固定分母；
CSV的全部已發布可執行列另含329列不在「四個OHLC均完整且為正」的分母內，
未將它們加到准入比例。逐列原始欄位與分類保存在
`source_recovery/accepted_denominator_v526/`，原財務與原報告數值未改寫。

保證金v503嚴格核對未過：原生PDF把TX／TF印成「T X」／「T F」，既有重讀器漏掉
兩碼。共用附件隔離只在獨立日期、原件SHA、完整前後金額與生效時點均成立時採用；
新增解析只接受原件本身明示相同括號商品碼的單行分隔字母標頭，拒絕跨行、任意文字、
錯碼與選擇權標頭。凍結v522通過1,579項機制回歸，23項既有範圍外案例未執行。
來源v523獨立核對六種指數期貨、12條實體組件，補回684個開盤及672個收盤保證金輸入，
隔離24個錯掛公告視圖；其他商品區間、既有已知金額與原件不變。
正式來源證據在`source_recovery/reused_index_margin_v523/`；這些是來源輸入數，
不是已恢復684個正成交日，也不是已通過全部決策時鐘或GPU訓練。

同時修掉固定金額來源準備時不必要的全股票處置恢復讀取；僅在明示固定商品範圍且
沒有額外來源／還原選項時重用父版非目標上下文，未辨識幣別仍保持未辨識。
同一舊解析器、原件與工作清單的單對實測由490.344秒降至62.915秒，約7.79倍。
v525全新程序逐欄及重複列計數核對10,588列來源一致，只有列序不同；金額區間、
問題與拒絕紀錄完全一致。這只證明來源準備步驟，尚未證明完整財務或訓練加速倍數。
數值與驗收收據保存，v517／v521兩份測速載荷沿共用受保護discard流程清理；
v503失敗原件與v523正式來源不在刪除範圍，實際清理以v540收據為準。

當下v485仍在四階段的accounting，來源為3,918筆原生參考價；新保證金續行owner
`reused-margin-financial-v534-20261026T230602-6f53a7be`已排在其完整驗收之後。
v534沿canonical bounded composition合併v451、v523、事件v484，不重寫既有估值政策；
再檢查7組現金、108個公司行動、全部3,918個原生值、70個非目標金融欄、雙版庫存圖
與父版准入差集後才發布`coverage/financial_v534/`。失敗v503／依賴失敗v507及v539
recipe preflight的原失敗均另存，成功恢復不覆蓋原失敗。

五筆參考價／官方行情矛盾仍是開放來源工作；FC1、HW2、IQ2、LG1仍沒有經驗證的
正成交。v520重新核對5,721個FinMind既有日收據，全部未變；12筆有限逐商品優先工作
仍為pending，不能稱為已下載，亦未重設完成／空回覆或越過原quota與owner規則。
FinLab兩欄已完成重查，四碼量仍為NULL；TEJ全部20個原生目錄月份已查，回覆112列
量字串為0，未回覆的6,799個座標不補零。未修軟體與未查來源不能統稱外部限制。
依使用者指示仍不購買NT$5,000原件；未改遠端來源、未啟動未驗收的新GPU訓練。

### 22:48 UTC 原生事件來源、五筆價格矛盾與共用附件的保證金修復

目前正式已驗收父版仍是v433／完成收據v465：725個帳務主體、721個有正成交准入，
固定1,766,347個正成交契約日中准入1,744,901日、尚缺21,446日（98.7859%）。
新v452已完成accounting／selection／publication／audit，正進行現金、公司行動、
原生來源、非目標金融值及完整庫存圖控制；尚未以未完成控制的數字取代父版。

745筆TEJ有限優先工作已全部完成，回覆7,990列原生資料；其中534筆為空匯出，
沒有把省略座標填零。v450保留原3,837筆並增加49筆上市參考價，共3,886筆。
v483沿同一批原件再辨識32筆明示除息、除權、減資、現增等標準契約新實體參考價，
原3,886筆逐欄不變，累計3,918筆。32筆均通過實際空倉起點、歷史單位及開盤時鐘前置核對；
只有13筆直接對應目前正成交首日缺口，不能將32筆直接稱為恢復32個成交日。
來源與完整財務順序為v452 → v485 → v507，各階段保留獨立驗收與新產物根目錄。

另對1,450個原生參考價與官方正成交控制核對，確認五筆尚待釐清的價格界線矛盾：
CJF201111／2011-11-07、JEF201205／2012-03-22、EWF201309／2013-09-17、
JIF201410／2014-10-13、ONF201710／2017-08-17。五組TEJ精確優先查詢完成，
15個座標實回13列，其原生OHLC與官方一致；保留FinMind逐筆原件的單式量與高低收
也全部吻合，排除僅由價差腿造成矛盾的解釋。ONF原生字串5505仍按原值保留，
不能猜測倍率或以漲跌百分比反解替代值。正式證據在
`source_recovery/native_source_conflicts_v499/`與`native_source_ticks_v500/`；
完整來源與訓練就緒仍為False，五筆是待查來源工作，尚未證明是外部限制。

組件追查另找到六種指數期貨的2010年早期保證金缺口：共用「新聞稿.pdf」網址的
後來原件被掛在較早公告日期，形成額外衝突；獨立且日期正確的官方函文已留存。
canonical `prepare_margin_repairs`新增明示選用的來源隔離：只在另一份有正確公告綁定、
原件SHA、相同商品／完整前後金額／生效時點且原生重讀一致的文件提供事件時，隔離錯掛列。
不改寫其公告日期、不刪原件，拒絕列完整保存。14項正反控制與凍結v502的
1,567項機制回歸通過，23項原有歷史範圍外案例未執行。來源v503正在核對12條組件、
684個開盤及672個收盤保證金輸入；這是來源層控制，不能先加到成交恢復數。
v507已接在已驗收v485之後，必須再次完成四階段、7組現金、108個公司行動、
3,918筆原生值、非目標金融值、雙版可達圖及父版准入差集後才發布。

兩次回歸啟動包裝錯誤分別保留為v502、v504失敗：前者誤猜不存在的驗證器檔名，
後者未傳明示版本而跑到歷史v1、未執行測試，且可能改寫舊無版本驗證摘要；
現存摘要另存，先前摘要復原尚未證明。修正後v505明示驗證既存凍結v502，
未重建來源或改寫v433完成證據；正式通過收據為`frozen-mechanism-acceptance-v502.json`。
資料與原件在`data_tw_index_futures/`，正式報告在`artifacts/markets/`，
排程及失敗收據在`artifacts/operations/`。未更換遠端資料、未啟動新GPU訓練，
NT$5,000付費原件仍依使用者指示不購買。

### 19:36 UTC 上市原生值完整驗收與9,145個未查座標的優先續行

完整財務v433已通過四階段、7組現金、108個公司行動、3,837筆原生值與立即父版差集。
`retained-financial-v465-complete.json`及全新程序
`independent-published-source-readback-v470.json`逐檔驗證輸入、完成收據與正式報告SHA。
固定1,766,347個正成交契約日中，准入1,744,901日（98.7859%）、剩21,446日；
相對v415恢復44日、准入損失0，相對v115累計恢復107,181日。
725碼都有帳務主體，721碼有正成交准入；正式逐碼清單在`coverage/financial_v433/`。
不要求商品全期間都出現，也不把資料帳務驗收稱為08:45／09:00或新GPU訓練完成。

原437控制假設規則鍵完全相同而失敗；新增原生起點確實使107個原缺席首日規則建帳。
恢復460又發現非目標庫存可達旗標沿零成交日傳播30列。恢復465保留全部失敗，
獨立重算兩版完整可達圖，確認只從False變True且均在對應來源實體首日之後；
非目標70欄金融數值、承接與費稅及原始frame不變，未放寬任何成交規則。
原控制摘要以允許鍵數扣除父版列數，錯扣107個父版尚未建帳的鍵；
`source_recovery/native_listing_v464/count_addendum.json`以實際配對回讀修正為
4,918,809列，原失敗、控制與金融數值均保留。

首日來源範圍審核v448發現19,955個目標中，10,810個已有完整／在途原生查詢，
另9,145個確實從未送查；全部有TEJ原生目錄商品與日期軸，不是外部資料限制。
已依使用者授權，以canonical TEJ優先介面排成745筆有限工作，priority=-47，
原較高優先的組件價格工作與其他下載狀態不重設，既有10,810座標不重送。
19:36:09 UTC快照為136 complete、608 pending、1 running；登錄不是取得來源的證明。
正式排程證據在`source_recovery/first_day_priority_v449/`，資料與逐座標清單在
`data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/first_day_source_scopes_v448/`。

來源回讀owner為`first-day-source-readback-v450-20261026T190518-9caa3dfe`。
唯一財務續行owner為`first-day-financial-v473-20261026T193403-c8a852ce`，
等待745筆原件、原生字串、收據及正規化完成，並保留原3,837筆逐欄值，再檢查
實際新實體、歷史單位、空倉起點，才執行accounting／selection／publication／audit。
有新增有效來源才重算；現金、公司行動、非目標金融值、兩版完整庫存圖及准入差集
全過後才發布`coverage/financial_v452/`。父版仍是已驗收的v433，未覆蓋遠端舊來源。
預先控制v474另測試「修復既有首日規則」與「新增原缺席首日規則」兩種情況，
兩個負控制也拒絕來源外新增列或缺席來源起點。原468僅停止尚在等待的metadata程序；
下載器、財務工作與訓練均未被停止，交接收據保留。

組件報價v447的66個來源任務全部完成，408個座標回覆53個正結算值。
其中4個目標日價格與既有當日值完全一致，真正缺的是前一帳務日原庫存估值，
那4個精確座標已查詢但未回覆；因此恢復前值為0，沒有把當日價格誤算成修復成果。
欄位歸因在`source_recovery/component_quotes_v447/field_attribution.json`。
另外49條受阻持倉生命週期均未納入既有研究估值政策，涉及351個非成交帳務日。
v471以同一凍結研究機制實測可估計351日，種子都來自更早的同實體來源，成交與原件不變；
但這不是找回官方結算價，候選政策未改寫已驗收政策、未加入accepted_policy_inputs，
不計入恢復統計，也未自動納入訓練。逐事件適用法源及正式財務驗收仍是開放工作。

FC1／HW2／IQ2／LG1仍只有估值／承接證據，不能以報價、月份、NULL或明示零量製造成交。
未修來源、法源、時鐘與軟體問題繼續分別追查，不統稱外部限制。
NT$5,000付費原件仍依指示暫不購買；沒有以未完成的資料啟動新GPU訓練。

### 18:44 UTC ETF完整財務驗收、上市原生值及FinLab優先重查

完整財務v415已由`retained-financial-v442-complete.json`驗收，正式逐碼清單在
`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/coverage/financial_v415/`。
固定1,766,347個正成交契約日，准入1,744,857日（98.7834%）、剩21,490日；
比立即父版v394恢復1,698日、准入損失0，比v115累計恢復107,137日。
725個帳務主體、721個有正成交准入，仍無須全期間存在才交易。
四階段、7組現金、108個公司行動、3,730筆原生值、非目標帳務及9種可能ETF階段均通過。
這是資料財務驗收，尚非兩個決策時鐘驗收或新GPU策略績效。

原v415外層bootstrap在selection子程序重入時誤拒絕已存在目錄；已完成的accounting
保留原SHA，由唯一恢復owner v429接續其餘三階段。原419控制把當日711筆股票規格
也算入14筆ETF新規格而失敗，修正scope的441控制重新驗證全部財務範圍。
兩次原失敗、原程式與恢復收據均保留；沒有為通過驗收改寫財務數值或放寬准入。

凍結v426通過1,515項機制回歸（23項既有範圍外deselected），compiler16新增明示的
`native_newly_listed_standard_opening_reference_v1`：僅採原生「上市」、Y旗標、
標準契約、實際日期／單位／開盤時鐘一致、確定空倉的新實體世代。來源v428新增107筆，
原3,730筆逐欄不變；其中44筆直接對應目前正成交首日缺口，不能先把107筆都算成恢復成交。
來源v432整合時另外23,273個父檔不變；107個實際frame座標已通過獨立前置核對。
完整財務唯一owner為`full-native-listing-finance-v433-20261026T184119-493522be`；
獨立現金、公司行動、原生值、父版准入與報告owner為
`native-listing-controls-v437-20261026T184409-bab0f803`。當下未把v433列入已驗收統計。

已透過既有FinLab優先意圖送查`futures_price:成交量`與`futures_price:到期月份(週別)`。
canonical下載器於18:32 UTC完成兩欄上游增量重查；四碼FC1／HW2／IQ2／LG1的成交量
仍全部NULL，月份有78個原生非空值，不能由月份／報價推定曾有成交。
`finlab-priority-recheck-v435-complete.json`保留資料集、座標、查核時間與來源模式。
先前TEJ全部20個目錄月份的6,911個有限座標則實際回覆112列，來源量字串全為0；
未回覆座標與原官方NULL均保持原狀。這些證據不代表所有資料商已耗盡。

剩餘工作清單v417含19,955個首日參考值缺口、106個當日承接缺口、4個前估值缺口，
另1,425個當日無直接旗標的缺口正追溯同一持倉連通組件的其他受阻日期。
沒有將可修的規則／軟體問題概括列成外部限制；NT$5,000付費檔案仍依使用者指示暫不購買。

### 18:05 UTC 完整財務v394驗收、四碼原生零量及ETF規格續修

完整財務v394四階段、7組現金、108個公司行動及3,730筆原生首日值均已回讀。
獨立恢復v423另驗證327個受影響商品日期、30,305個查詢列及全部合成時鐘；
價格、金額、費稅、庫存承接邊及原始frame不變，舊備援額度的改變有原始公告支持。
原控制v400要求合成時鐘完全相同而失敗；完整逐欄診斷、原失敗及後續context／
研究群組控制器失敗均保留。v423補驗來源範圍、未知規格context及既有群組最小額度，
沒有更改已完成的財務數值或放行未解規格日期。正式控制在
`source_recovery/position_boundary_v423/`，完成收據`retained-financial-v423-complete.json`。

固定1,766,347個正成交契約日，准入1,743,159日（98.687%），剩23,188日。
相對立即父版v315恢復1,714日、准入損失0；相對v115恢復105,439日。
25個已修部位正成交缺口解開21條實體生命週期，不能把直接修復數和完整組件恢復數混用。
725個帳務主體、721個有正成交准入；正式逐碼清單為`coverage/financial_v394/`。

四碼FC1／HW2／IQ2／LG1的全部20個TEJ原生月份已完成3筆有限優先查詢。
6,911個日期座標中，來源實際回覆112列、涉及6個實體月份，112列原生`Volume`均為
字串`0`；其他6,799座標未回覆不等於零。未覆蓋的原生月份也未假裝有觀測。
原官方frame的113列NULL成交量未改寫，未將參考價當成新成交。
逐列證據在`source_recovery/carry_only_quote_v409/`；這是本次來源的實際無成交證據，
仍不宣稱全體資料商已耗盡。

ETF的977個正成交規格缺口均在2026-07-06起14檔商品。官方1150000840修正原件及
1150001759生效函已逐份核對native PDF、壓縮原件與內容SHA。compiler15新增明示的
`staged_inner_band_permissions_v1`研究ABI：不知道實際上／下階段，不把±15%或第一階段
假裝成已觀測限制；只允許最窄合法區間內、所有可能階段都接受的價格，區間外不新增容量。
日資料、終端與決策時鐘共用同一個permission函式，真實限價仍保留NULL。
207項聚焦測試、凍結v413的1,499項回歸通過（23項既有範圍外deselected），
14筆規格只追加至來源v414，26,073份父來源逐檔不變；來源frame未重寫。

完整財務v415由`full-staged-etf-finance-v415-20261026T180544-f1c93c96`唯一owner執行；
`staged-financial-readback-v418-20261026T180852-d7cfdc09`等待四階段成功後，才執行
現金、公司行動、剩餘工作清單、9種可能階段、非目標帳務及父版准入差集驗收，
通過後才發布`coverage/financial_v415/`。當下v415尚未列入已恢復統計。
資料仍在`data_tw_index_futures/`，正式證據仍在`artifacts/markets/`。
沒有付費購買，沒有把剩餘來源問題全稱為外部限制，也未啟動新的GPU訓練。

### 17:22 UTC 完整財務v315、已知舊部位上限及有限來源續查

第二批227筆原生查詢已全部完成；整合後的v334有3,730筆原生首日參考價，
先前2,855筆來源值全部保留。完整財務v315的四階段、7組現金控制、108個公司行動、
3,730筆首日值與持倉邊、原frame逐位元相同、父版准入差集均通過。
固定1,766,347個正成交契約日中，准入1,741,445日（98.5902%），剩24,902日；
相對v304恢復63日，准入損失0；相對v115恢復103,725日。
725個台幣帳務主體、721個有正成交准入的代碼，並未要求商品全期間存在。
正式逐碼報告在`coverage/financial_v315/`，17:31 UTC另以全新程序核對報告、
完成收據與其SHA，見`published-native-coverage-independent-v406.json`。
這仍是財務資料驗收，不代表08:45／09:00或遠端新訓練已完成。

29個部位缺口來自7個商品日期。原始掃描公告及舊額度已有來源，缺口不全是缺資料。
compiler14新增`prior_known_cap_within_verified_absolute_relaxation_v1`：僅在相鄰、
完整、絕對口數上限且已驗證為不緊縮的邊界，以此前已知的較嚴格舊額度限制動作，
保留新公告原始可知時鐘與獨立政策證明，不提前揭露較大新額度。
收緊、換算、共用群組、公式、撤銷、缺來源等情況仍拒絕。
凍結v393通過1,460項機制測試（23項既有範圍外未執行）；實際來源的22個鄰近日控制
只有目標7日改變。完整財務v394由
`position-financial-followup-v398-20261026T171552-900d8cab`接續驗算，
通過四階段、公司行動、首日值、舊值及准入差集後，才發布`coverage/financial_v394/`。
當下不能把29個候選缺口先算入已恢復的統計。

17:03 UTC FinMind狀態快照恢復SponsorPro；狀態快照不是credential身分或新資料證明。
已核對5,721份既有日資料收據：未變更5,721、變更0、缺檔0，因此沒有重複下載。
既有12筆逐碼優先工作保留，沒有為進度數字重設原始狀態。
五批TEJ精確實體月份探測已完成並驗證原始字串、收據與Parquet：50列來源，
10個首日缺口都沒有前一交易日正結算值；同日同商品的10個對照月份全部有正結算值。
這只證明本批沒有補回首日值，不代表所有來源已窮盡；ROI反推值未採用。
正式逐座標結果在`source_recovery/prior_quote_probe_v405/`。

FC1、HW2、IQ2、LG1目前只有估值／承接來源、成交量為NULL。
不能把NULL當零，也不能把這四碼稱為已驗證可新增交易。
已將TEJ原生目錄中這四碼的全部20個實體月份、6,911個有限日期座標，
排成3筆canonical優先工作；既有來源座標不重送，查詢日期範圍也不宣稱是上市日期。
登錄證據在`carry-only-quote-recovery-source-plan-v408.json`。
剩餘首日參考值、2026年ETF分階段限制與成交時鐘仍分開續修，沒有概括列成外部限制，
沒有付費購買或啟動未經驗收的新神經網路訓練。

### 16:25 UTC 完整財務v304驗收與後續續行

完整財務v304的accounting、selection、publication、audit均完成；獨立公司行動、
原生參考價、父版准入與原frame控制通過，逐碼正式報告已發布至
`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/coverage/financial_v304/`。
725個帳務主體、721個有正成交准入的代碼；固定1,766,347個正成交契約日，
准入1,741,382日（98.59%），剩24,965日。相對立即父版v275恢復1,229日、准入損失0；
相對v115恢復103,662日。這些是完整財務資料覆蓋，尚非新GPU訓練結果或兩個決策時鐘驗收。

v309最初在工作清單差集使用`physical_contract`而失敗；該清單實際以
`date/product/contract`唯一識別。原控制程式與失敗進度已保留於
`control-schema-recovery-v390/`。修正差集鍵、增加無NULL及唯一性檢查後，
v390重跑全部獨立控制，沒有重算或變更四階段財務值，也沒有放寬任何驗收：
2,855筆原生首日值、2,827條新增空倉起始承接邊、4,912,359列非目標規則相同，
原frame逐位元相同，公司行動108事件／496實體列全通過。
`retained-financial-v307-complete.json`明示此次schema恢復並綁定原四階段與新控制SHA。

v306逐鍵工作清單中，20,018日仍涉及首日上下文、977日缺歷史規格；其餘估值、
公告時鐘與連通組件問題保留原旗標，重疊數不相加。FC1、HW2、IQ2、LG1仍缺已驗正成交，
保留帳務／持倉用途，沒有用NULL、報價或參考價製造新成交。

第二批227筆下載未中斷。因父驗收失敗而安全停止的後續等待器，在父版完成後重新啟動：
`build-native-batched-v364-recove-20261026T162601-cba595e4`與
`continue-native-batched-v370-rec-20261026T162602-6aa62817`；兩者已獨立回讀running。
v315後續控制模板亦修正相同工作清單鍵，九個產生腳本的語法與綁定已驗證。
實際進度仍看`native-reference-source-v312-progress.json`及`native-followup-v370-progress.json`，
尚未宣稱第二批或完整v315完成。凍結v360b的1,440項機制測試通過，沒有宣稱已達純運算瓶頸。

### 15:20 UTC 原生首日參考價與優先下載修復

此段記錄來源補強進度；完整財務准入仍以下段已驗收的v275為準，不能把來源
候選數當成已恢復的正成交日數。新增版本必須通過全部四階段、原始正成交分母、
公司行動及立即父版差集，才更新正式覆蓋報告。

15:41 UTC續修：原202筆查詢已全部完成，854列原生結果及收據逐檔回讀。
v301d完整驗證5,705份非空匯出、37,871列，取得218個商品的2,855筆首日參考價；
相對v301c新增164筆，先前已驗的來源值損失為0。v303正在逐檔建立不可變來源別名，
完成後由v307接續完整財務v304。這些來源數尚未算入v275的准入統計。

另沿官方公司行動的精確商品／月份／日期界線，找到907個尚未送查的原生座標，
透過相同canonical優先請求介面排入227筆、priority=-49；原202筆為-50，先後關係保留。
已查、執行中、UNKNOWN及空結果的範圍均未重送，沒有新增平行桌面下載器。
登錄證據為`additional-native-boundaries-priority-readback-v311.json`，
v312等待這227筆逐檔回讀後才正規化來源，不執行供應商查詢。

進一步回查原件，找到9個小型股票商品、21個新實體月份的正參考價，其原生單位為100股。
v1限2000股是先前的驗證範圍，不能視為所有標準契約的規格。
期交所[股票／ETF契約規格](https://www.taifex.com.tw/cht/2/sTF)分別明示
2000／100股及10000／1000受益權單位；歷史可用單位仍必須和各日已綁定規格完全一致。
`native_reissued_standard_opening_reference_v2`／compiler13依此擴充，
保留原生報價、不按比例猜價、不繼承舊世代庫存，來源建置器亦直接拒絕規格單位衝突。
110項聚焦測試與凍結v333的1,402項機制回歸通過，23項既有範圍外測試deselected。
原件稽核在`native-reference-unit-scope-v313.json`，沒有新增供應商請求。

後續owner為`build-native-final-v334`與`continue-native-standard-v350`：等待有限來源
全部完成，以凍結v333重建來源v334；原完整財務v304與報告通過後，有新增有效來源才建置
完整財務v315及全部獨立控制，再發布`coverage/financial_v315/`。立即父版v304的准入
差集和原始v275機制控制分別計量。舊的僅支援v1等待器v321已按PID、start ticks及boot ID
精確停止，沒有停止下載器或v307，證據為`native-followup-v321-supersession-v335.json`。
新後續流程不啟動GPU訓練；缺口、失敗及來源原值均保留。

15:55 UTC效能續修：v303於15:45:07完成23,265份父來源及原生參考價來源的不可變別名，
正式v304已進入accounting。來源發布約12分鐘內觀察到逐別名目錄fsync的journal等待；
這不是Transformer或GPU算力。canonical `artifact_dedup.ImmutableSourceBatch`已新增
有界目錄同步：原始檔仍獨立複製、每個payload仍fsync及全SHA驗證，目錄與祖先各自
驗證inode後，在整批結束時同步；完成manifest必須在成功退出context後才發布。
84項聚焦測試通過，涵蓋producer獨立、原始字串、同步失敗、目錄替換及程序中斷後重讀。
未聲稱這些測試已證明整個財務或訓練流程的加速倍數。

v360凍結器因測試清單計數斷言失敗，尚未建立來源release，失敗log保留；v360b補全
新測試與既有dedup測試的closure後重跑。後續owner改為`build-native-batched-v364`與
`continue-native-batched-v370`，等待v360b凍結通過及全部227筆來源完成，沿相同v334來源、
v314整合與完整v315財務流程；只有來源別名步驟採用目錄批次同步。
原v350與v334等待器均在尚未產生候選前按精確程序身分停止，下載器與v307未中斷；
分別見`native-followup-v350-supersession-v371.json`及`native-final-v334-supersession-v372.json`。
目前下載／來源進度為`native-reference-source-v312-progress.json`，後續完整建置進度為
`native-followup-v370-progress.json`，原v350進度不再代表執行中的owner。

16:08 UTC續驗：凍結v360b已通過1,440項機制回歸、23項範圍外未執行，來源SHA為
`06ea534e28c8092b0634964a6b74feb58964fa53a573d397bceed959bfb360ba`。
v304的accounting已完成並進入selection；全部四階段與獨立控制尚未完成前，
v275仍是已驗收的完整財務基準。批次同步的聚焦測試不代表完整流程加速倍數。

公式診斷v380保留另一個負向結果：將官方日報收盤減其漲跌，和獨立原生開盤參考價比較，
2,855筆控制中有1,271筆可算，1,255筆相符、16筆不符。首日缺口中20,705筆雖可算得
正有限候選，但新掛牌、公司行動及原生漲跌欄的歷史基準尚未逐類證明，未納入財務或訓練。
原始來源雜湊與逐筆差異保存在`exchange_difference_reference_diagnostic_v380/`及
`native-vs-difference-reference-diagnostic-v380.json`；不能把一致率當成所有首日的合法替代證明。

16:16 UTC下載路徑回讀：FinMind帳戶狀態快照回覆Free／600次每小時，Sponsor路徑
為`not_entitled`；原12筆有限逐商品優先請求仍存在、均pending，沒有冒稱已下載。
`provider-capacity-observation-v381.json`保留各狀態快照SHA與時間；這不是私有credential
身分或新資料收據的證明。先前已驗證的5,721日期歷史不因這次狀態變化丟棄，
TEJ第二批仍由既有owner持續處理，沒有購買或重設原始來源任務。

- 修正197筆從未送出的TEJ優先請求：其原生`max_cells`小於介面允許的100，
  因而一直不能執行。新請求只將此界線提高到100，契約、日期、欄位及列數完全相同；
  原請求保留為`superseded_priority_capacity_v1`，不修改原始請求或重置已送出的工作。
  加上5筆原本有效的請求，共202筆、863個精確來源格。
- WSL工作者中斷後仍留在Windows的6筆未歸屬Preview已完整保留，未塞入任何來源表。
  原查詢仍是UNKNOWN；在既有授權及配額下另送一次新查詢，取得10筆新結果及獨立收據。
  canonical排程已接上相同的「先完整保留，再受限重播」路徑；未驗證的視窗、資料格、
  時鐘或截斷資料仍不送查詢。225項排程／恢復測試通過，平順切換服務後亦有新工作完成。
- 15:20:19 UTC的202筆優先來源快照為168完成、34待執行，已取得695筆原生資料。
  這是當時快照；現況以`native-reference-source-v301d-progress.json`及逐筆收據為準。

最大缺口是新實體世代缺少首日計價，而不是要求每個商品全期間都有資料。
舊世代的結算值與持倉不能直接移到新世代。此次採用的來源定義為
[TEJ TWN/AFUTRSTK](https://tquant.tejwin.com/%E8%B3%87%E6%96%99%E9%9B%86/)的`ex_p`：
交易所公布的調整生效日開盤參考價。原頁、原始雜湊、實際取得時間與官方契約調整問答
均保存在`data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/native_initial_reference_definitions_v300/`。
期交所[撮合原則](https://www.taifex.com.tw/cht/4/oamPrinciple)另明定新掛牌或新月份參考價
由交易所參酌相關資訊決定，不能把前月價格或股票收盤價當成所有新月份的通用替代值。

`native_reissued_standard_opening_reference_v1`僅綁定：

1. 原生原因為「契約調整重新推出標準契約」、原生發行旗標Y、單位2000股、正參考價。
2. 原生契約與日期和查詢座標相同，且對應到帳務中確定為零起始持倉的新標準世代。
   部分原生調整代碼雖也標示重新發行、單位恰為2000，仍不適用此標準契約政策。
3. 歷史規格已綁定，單位與開盤時間一致；保留原始檔、Parquet、收據及欄位定義雜湊。
4. 參考價只用於首日保證金／合法價格範圍的計價；不創造成交、不沿用舊世代庫存，
   不新增模型特徵。日後仍沿原有逐日持倉承接規則。

市場價值的適用時點與資料商實際傳送時鐘分開保存。此來源定義建立的是交易所當日
開盤參考價；不能據此宣稱TEJ每個歷史日期都在06:00送達，也不將2026年取得的快照
回填為2011年的資料商接收紀錄。

完整回查5,616份非空原生匯出、37,496列，v301c已驗證641份相關來源匯出，
正規化出199個商品的2,691筆首日參考價；先前僅檢查部分交界時找到821筆。
這2,691筆不是已恢復的交易。凍結v302c含typed compiler、來源建置器及回歸測試，
通過1,391項機制測試、23項既有範圍外測試deselected。`finish-native-reference-source-v301d`
會等待原202筆查詢完成、逐檔回讀後重建來源；`complete-native-finance-v307`接續來源v303
及完整財務v304四階段，並驗算所有新增首日值、持倉邊、原frame逐位元相同及v275既有准入。
任何失敗都保留候選與證據，不發布成已完成資料，也不啟動新神經網路訓練。

本輪證據均在`artifacts/operations/futures-universe725-repair-20261025-v1/`：
`tej-priority-capacity-independent-v296.json`、`tej-preserved-unknown-recovery-v298.json`、
`native-reference-definition-readback-v300.json`、`frozen-mechanism-acceptance-v302c.json`及
`tej-scheduled-preservation-handoff-v308.json`。正式說明在
`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/source_recovery/native_initial_reference_repair_v300/`。

來源v274已完成完整財務v275的四階段建置與獨立驗算。原始725碼全部保有
帳務主體，721碼有正成交准入。固定1,766,347個正成交契約日的缺口由v115的128,627，
經v159的47,262、v187的31,411、v199的30,672、v218的30,450、v235的30,337，
經v242的29,510、v260的27,585，減至v275的26,194；最新逐鍵差集在`remaining_source_worklist_v277.parquet`。
春節續行、九月處置延長、XIF規格及TMF颱風順延修復，分別恢復739、222、113及827個
正成交契約日。三份掃描公告的保證金日期修復再恢復1,925日，TE／NZF／OAF的公告時鐘
修復再恢復1,391日；相對v115的缺口淨減102,433日，現在准入1,740,153日（固定分母的
98.52%）；對立即父版v260的既有准入損失為0。
這些數字是完整帳務及持倉組件發布的回讀，不是來源候選數量。
`retained-financial-v278-complete.json`連結最新完整四階段證據；完成時間為
2026-10-26 10:20 UTC。v279的24個商品日期、116實體列與全部四個金額／可知時鐘控制
通過，另有4,855,009列非目標完整財務資料相同及原frame逐位元相同。
前版v264c的28個商品日期、146實體列與全部四個金額／可知時鐘
控制於09:12 UTC通過，另有4,829,570列非目標完整財務資料相同及原frame逐位元相同。
先前v246逐列驗證兩次margin修復的原始／維持保證金與可知時鐘，
非目標完整財務列分別4,890,094及4,912,056列相同，原frame逐位元相同。
原始725碼逐商品來源、用途與未解決日數在
`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/coverage/financial_v275/original725_coverage.csv`。
舊v187／v199／v218／v235證據保留，不能將資料准入等同已完成神經網路訓練。

另外找到可修正的編譯器錯誤：舊公司行動的要求沿用到已重新發行的實體世代，以及
2011-05-03現金股利制度邊界未覆蓋舊公告。編譯器11僅在同月份、明示轉出與新實體
出生證據同時成立時清除舊要求；後來新增而未綁定的事件仍會阻擋。
完整4,968,270列驗算修正39,495列，其中15,833個有成交契約日不再有直接的公司行動
錯誤阻擋。完整重新發布後的缺口減少15,851日；直接遮罩數與持倉組件准入數分開計量。
v187的8,528個本身沒有直接錯誤的受阻正成交日，已全部追到同持倉組件的來源缺口；
v199再以自己的frame、blockers及lifetime scope獨立回讀，剩餘8,113個間接受阻日也全部
追到同組件來源缺口，未歸因鍵為0。v218自己的7,941個間接受阻日也已全部追到來源缺口，
未歸因鍵為0。分別見`remaining-component-source-trace-v193.json`、
`remaining-component-source-trace-v222.json`及`remaining-component-source-trace-v233.json`；
這不表示這些來源問題已修好或屬外部限制。
v242自己的7,079個間接受阻正成交日亦全部追到實際持倉組件缺口，未歸因鍵為0，
見`remaining-component-source-trace-v255.json`。v260的5,191個間接受阻日亦全部追到
同持倉組件缺口，未歸因鍵為0，見`remaining-component-source-trace-v269.json`。
最新v275的3,837個間接受阻日全部追到同組件來源缺口，未歸因鍵為0，
見`remaining-component-source-trace-v281.json`；這些來源缺口尚未全部修復。
尚餘21,247個首次觀測日；
不能把當日收盤減漲跌數字直接回填為已證明開盤前可知的初始參考價。

凍結v186來源SHA為`131c58cfd627ec01ccc8ff0867ba27991eb0a3fc5b251f4592577d181cbe12a1`，
實際凍結程式通過1,333項機制測試，23項既有範圍外歷史fixture為deselected。
新的`financial_research_equity_v187`沿canonical owner重算遮罩及完整持倉組件准入，
原財務數值檔案逐位元相同。遮罩重算65.39秒、RSS尖峰15,075,033,088 bytes；
這不是整個準備或訓練工作流的加速倍數。完整v159帳務重建原耗1,165.37秒、
RSS尖峰61,007,921,152 bytes。v187後續的selection、publication、audit仍各自留存計時。

121個原件控制對應108個不同商品／生效日、496個實際實體月份。舊v163驗收將
DLF 2017-07-21／201806及NQF 2014-07-18／201506錯視為已有舊庫存，現已沿前一日
原始行情獨立證明它們僅有不可成交warmup，沒有股利可領；v185控制全部通過。
原現金值與原件未變，舊v163失敗及v179控制器序列化失敗仍保留。

本機重新開機後已精確恢復85,868個原交付來源、7,392,657,376 bytes並逐檔SHA回讀。
只停止自己的遠端等待器v160，其他既有訓練未停止。v174及v176舊本機等待器因重開機
中斷，不能視為仍在執行。`complete-local-finance-v188`完成財務v187；股票特徵接續owner
`complete-local-context-v189`使用既有5,292,847,433 bytes原始來源，避免重複搬入。
v187及完整來源已完成遠端有限交付：81,734檔、7,530,866,337 bytes兩端全SHA回讀；
其中81,680檔沿已驗不可變物件重用，WAN傳輸52個有效檔案、824,071,159 bytes。
見`financial-v195-source-independent-readback.json`。擴充後發布有750個商品，不應拿它
取代原始725個商品的固定分母；原始725個帳務主體仍逐項保留。
v189於05:00:59 UTC完成4,105個日期（2010-01-04至2026-10-02）的因子化特徵、
全成員SHA及財務交易日覆蓋驗證。特徵manifest SHA為
`0a0798d346b4b869bddcd76df032d80c6ec54920c1fd108ebb3ddda311efad03`。
v199的4,027個財務日期全部包含在特徵日期內，省略日為0；新八組GPU訓練尚未開始。

另沿已保留官方2020-01-30公告修復春節提高保證金的明示續行條款。
G2F／GTF／T5F／XIF延續的是原已提高金額，並非假設恢復節前金額。新原件SHA為
`6d088d6f07ff5d8efc36f52de6d364e5ac136c1a5560556857aa5a1ad57df971`；
配對日期可核實的1月17日新聞稿及父來源manifest，保留缺正文掃描附件的拒絕紀錄。
來源修復`seasonal_margin_repair_v196f`耗時4.97秒，僅改四商品166個商品日期；
其他商品有效金額逐值相同。這不是166個可成交契約日，也不是全流程加速倍數。
凍結v207程式SHA為`94e6310eb8b1b989a7cc0bc5347c689b373a12282968f19e88bf13f359b9c69f`，
通過1,334項機制測試、23項既有範圍外fixture deselected。完整整合來源v198的manifest
SHA為`3189038fde06d979c8846a9704f549628cd9f9a9c25d290fbcd9b1719aa71cdb`。
`complete-seasonal-finance-v209`於04:02 UTC啟動`financial_research_equity_v199`完整帳務，
04:31 UTC已完成selection、publication、audit、108事件／496月份及固定正成交分母回讀。
帳務階段耗1,023.61秒、process-tree RSS尖峰55,895,994,368 bytes；不把此單階段與舊完整工作流
比較為加速倍數。原始725碼帳務主體、721碼正成交准入均保留，未解釋的准入回歸為0。
特徵與財務是可獨立處理的CPU階段；按當下容量允許並行，兩者原process-tree RAM guard
及8GiB reserve不變。兩項均完成且遠端回讀前，不交給GPU訓練。

另確認PTF／ULF的2026-09-03處置提高保證金，後來由已保留的09-08公告提高至未處置基準的
兩倍，並延長至09-14。現行來源漏接後一原件；不能直接在09-09恢復原值。
原櫃買2026年處置完整回覆及雙鴻官方日行情均已保留，無須重抓API。
canonical margin repair已新增具SHA、公告索引連結、刊登日期及商品範圍約束的
`bind_retained_margin_sources`，重用原始公告庫，不增設下載器。
凍結v214程式通過1,342項機制測試、23項原範圍外fixture deselected。
`retained_margin_extension_v215b`的1,509份來源成員已全SHA回讀，其他商品有效金額不變；
影響24個商品日期，還不能當作24個正成交契約日已恢復。
09-14還原金額的可知時鐘仍是13:35，date-only binder保留same-day clock review，
完整帳務編譯才可依商品結算時鐘判定。來源成功後控制器誤把這個NULL當成數值而失敗的
v215b紀錄仍保留；v216獨立控制確認規則與時鐘正確，沒有改可知時間來消除錯誤。
`stage-retained-source-v217`整合來源後，`complete-retained-finance-v221`按實測容量
執行v218的accounting、selection、publication、audit、固定分母及全部公司行動控制；
05:22 UTC已完成，原始725／721碼保留、未解釋的准入回歸為0。
`readback-retained-financial-v223`已逐實體月份確認09-09、09-14及09-15開盤／結算
原始與維持保證金、13:35條件還原時鐘、原frame byte parity及其他4,905,780列完整財務值相同。
v224只按PID、start ticks及boot ID停止尚未變更遠端request的v213本機等待器，
保存原兩份設定，原v212來源交付繼續。v225只把四個資料路徑改綁v218；解析後模型、
optimizer、費率與停止設定相同，八組variant逐項核對。
v226已沿原有限交付器重用v212成員，兩端全SHA回讀89,815檔、14,516,802,319 bytes；
WAN差量為73檔、575,869,363 bytes，完整股票特徵未重傳。
v227獨立檢查遠端財務、特徵及新時鐘控制通過，原queue確認無checkpoint後綁定；
但preflight發現仍被request引用的舊RSS政策已被保存移走，故拒絕啟動。
v231保留失敗及舊政策；因其source SHA與現程式不符，明示移除這個可選的舊測速准入，
使用原有32GiB硬預算、16GiB最低准入及8GiB reserve。全request檔案及preflight重新通過，
05:29 UTC遠端canonical owner開始完整成交時鐘回讀；不是GPU訓練已開始。
06:12 UTC完成3,833日期、2,856,766個時鐘合約日檢查後發現27個未驗證成交合約日，
其中15個不在原先有限來源例外清單；owner明確拒絕擴增隔離並EXITED。
沒有queue-request、GPU程序、checkpoint或新正式績效。v256以原request SHA及當前
Supervisor狀態獨立回讀；完整缺口表為正式coverage目錄下`execution_clock_gaps.csv`。
實際membership、嚴格CUDA及原四epoch工程驗收仍在正式訓練之前。

另回用已保留的2026-07-01官方原件，發現非金電期貨XIF的規格調整保證金漏入候選鏈，
使06-18至08-12區間因前後金額不一致而封鎖。原件SHA為
`e890f0f405584022c8755f833121ddfcd1ea858df09aa468f0d9ba3034d014f2`，
已逐頁核對07-06一般交易時段起的原始9000／維持7000／結算6000新臺幣元；
不按縮小比例猜測或提前套用。v232沿原source-bound visual review與margin repair接入，
v234／v238依序整合來源及完整驗算財務v235；只有後者及獨立控制完成，才可計入新增准入。
兩者及v246 XIF控制已完成，淨恢復113個正成交契約日；並未改動其他商品財務值。

再從逐鍵持倉鏈找到TMF 2024-11-01的開盤保證金缺口。原正式公告已接入颱風順延，
但同一調整的新聞稿仍在10-31形成舊日期邊界；兩者金額相同卻截斷09-27的舊金額區間。
v239逐頁核對新聞稿原始16900／維持12950／結算12500及調整前16100／12350／11900，
並綁定已保留的官方10-30休市順延公告。兩份原件SHA分別為
`4691b6eec688e95f36a5e7c26a0045c2ed9d8a6a23f7bafbe66084756daf0824`及
`26b108d04bfb6df119f7fba8755cd6411c8e83773690a44a901c7609d3aa3d56`。
v240b沿原source review修復新聞稿的同一TMF列，11-01開盤仍是16100、收盤後16900；
10-30與11-04亦各自控制，其他商品金額逐值相同。只修復一個缺保證金商品日，
不能直接宣稱其整條持倉鏈的正成交日已准入。v241整合來源、v245重建財務v242，
另由v246對v235／v242逐一驗證完整非目標商品財務值及開盤／結算時鐘。
v240控制器字串替換斷言的失敗保留，沒有進入財務或改原件；v240b重新獨立綁定來源。
v245與v246 TMF控制已完成，淨恢復827個正成交契約日。v249完成候選資料有限交付，
101,759檔、10,229,023,511 bytes兩端全SHA回讀；這是source-only交付，遠端原request
仍綁定v218，沒有不相容續訓。

v250有限補抓在API呼叫前失敗：財務release未含可選的`shioaji_roots`，舊收集器卻
強制讀該欄。canonical collector改為lazy半連接，只在欄位存在時讀broker查詢提示，
保留實體商品根碼、帳戶全域節流、來源驗證及未知值語意。v251凍結的唯一函式差異
通過53項focused測試，另保留缺欄、空欄及舊alias三個測試案例；這不是訓練程式改版。
七個商品日重抓仍為四個ETF回空、三個TX量價矛盾，新增准入0；原件與失敗均保留。
v254回讀已完成的whole-market分鐘K來源，16個ETF合約日仍缺，另11個合約日的原生
單位／量差未通過逐契約核對，沒有把分鐘K改稱逐筆成交。v253c沿canonical官方下載
取得鉅額交易明細，2017年若干量差與明示鉅額數量一致，但原件沒有時間及trade ID，
沒有僅憑總量或成交價猜測刪除逐筆。以上均不能當作所有資料商耗盡的證明。

v257再逐頁讀取三份已保留的2011-08-22、2014-02-05、2019-04-16掃描公告，確認
生效均為次日交易時段結束後。舊候選漏讀日期形成不正確的當日開盤邊界；
v258沿既有source-bound visual review修復七個GTF／TF／XIF原始金額列，28組前後日期
控制通過，恢復14個缺保證金商品日期；這不是14個正成交契約日。v259於08:31 UTC完成
來源整合，其他23,186個已驗父來源檔案不變，manifest SHA為
`a72e970dda891c2d3b22abae7c0ac7d69070c6828dedc6e39321b89163094439`。
v263於08:32 UTC開始重建財務v260，09:00 UTC完成全部四階段、108事件／496月份控制
及固定分母回讀。v264獨立控制原誤選2014-01-28休市日，v264b再發現2019-04-17的
到期月13:30結算與一般13:45收盤後調整不可共用金額。兩次失敗均保留，沒有修改財務
資料來迎合控制表。沿已保留的2014年春節休市公告與01-24提高保證金原件，將三個控制
改至01-27，並分別檢驗開盤舊值、收盤新值；v264c另逐實體月份確認到期13:30仍適用
調整前金額，13:45收盤才適用新金額。28商品日期控制全通過，沒有放寬可知時鐘。
成功收據仍由`financial-margin-clock-controls-v264.json`發布，明示`successful_verifier=v264c`
及兩個失敗run ID。v266於09:12 UTC完成新725碼清冊；v270已完成候選v260的有限交付，
101,823檔、10,265,702,750 bytes兩端全SHA回讀；見
`financial-candidate-v270-source-independent-readback.json`。原request231仍選v218，
沒有變更原request或開始訓練。

v272／v273d再重用TE、NZF、OAF的原始公告與新聞稿，修復12個缺保證金商品日期，
24個前後日期控制通過，其他商品金額相同；原件371個成員全部SHA回讀，沒有新API請求。
來源v274於09:47 UTC整合完成，`complete-remaining-margin-finance-v278`接續重建完整
`financial_research_equity_v275`，10:20 UTC完成四階段、公司行動控制及固定分母回讀。
v279、v280、v281的獨立控制、725碼清冊與持倉組件來源歸因均已完成，實際恢復1,391個
正成交契約日，對立即父版v260損失為0。到期月13:30及一般收盤13:45／16:15的保證金
分別按原件控制。v282於10:25 UTC完成101,917檔、10,308,563,588 bytes兩端全SHA回讀；
`financial-candidate-v282-source-independent-readback.json`是獨立證據。
這是候選來源交付，原request231仍選v218，沒有開始新訓練或不相容續訓。

來源建置剖析發現，同一份還原公告與歷史事實被反覆全文解析及全表掃描。canonical parser
新增單次建置內、綁定同一份事實集合的索引與純文字解析快取，保留衝突列、公告日期綁定、
可知時鐘與SHA／visual review驗證；未用跨次全域快取。凍結v284通過1,356項機制測試，
23項既有範圍外fixture deselected。v285以v273d相同輸入重做完整來源建置，10,604列事件
與2,983列金額區間的所有欄位、NULL及重複數均相同，兩個Parquet逐位元相同，24個控制
亦相同。canonical prepare由919.54秒減至262.18秒，單次約3.51倍；這是並行負載下的一組
來源建置測量，不是正規化ABBA或訓練加速倍數。RSS尖峰由856,358,912增至
1,128,775,680 bytes。證據`restoration-index-source-parity-v285.json`；v284尚未替換
正在跑的v214財務引擎，暫存建置資料的回收也尚未完成。

v265另沿canonical官方fetch／raw archive保留歷史資料目錄與四份原生ODT格式文件，
解析其`content.xml`，原件與衍生表逐SHA回讀。官方目錄列期貨成交簡檔自1998-07-21起、
每半年NT$1,000；27個成交時鐘缺口落在2012上半年、2013下半年、2016上／下半年、
2017上半年，目錄金額合計NT$5,000。這是候選資料範圍，不是已購報價、成交准入或
資料必定完整的證明。v267／v267b沿既有官方ZIP檔名契約檢查這六個日期，均未取得有效
原生ZIP，拒絕回覆與原始失敗保留；不是宣稱歷史資料不存在或其他供應商已耗盡。
可審核的範圍、逐契約鍵、單位與驗收要求在正式
`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/source_recovery/clock27/`。
沒有下單、付款或傳送供應商訊息；此方案只處理27個成交時鐘鍵，其餘首次觀測、
分階段漲跌幅、部位公告時鐘與持倉終止來源仍分開追蹤，未改列成外部限制。
使用者於2026-10-26明確選擇「先繼續修復其他資料，暫不購買」；付費取得步驟停止，
不把等待時間視為授權，繼續既有與公開來源修復。原`request_spec.json`保持不變。

v247重用舊TEJ原生匯出，不只重讀欄位目錄：424個已完成非空Future Attribute任務，
原始回覆、parquet與receipt逐SHA回讀，涵蓋原始725清單中的284商品、12,703商品月份。
剩餘106個正成交持倉延續缺口全部對到無衝突的商品月份，其中74筆恰好是TEJ最後交易日，
106筆當日官方未平倉量均為0。這是繼續尋找逐契約終止／轉倉原件的定位資料；
TEJ目前快照未證明當年公告時鐘或實體世代，沒有直接補價格、清空庫存或解除阻擋。
證據為`retained-tej-lifecycle-readback-v247.json`及資料目錄下
`retained_tej_lifecycle_readback_v247/continuation_gap_readback.csv`。

使用者明示「需要查的資料可以直接插隊下載器」，本輪沿既有 finite priority owner
直接登記，不再重問；付費檔案仍依暫不購買的決定。v287先回讀66份非空TEJ日行情匯出，
405列、232商品月份，對21,247個首次觀測鍵的前日結算精確匹配為0，沒有重複抓同批來源。
v288再比對已保留官方日行情，找到1,314個前日價格候選；v289以最新v275 frame及原件
證明它們全部跨越不同實體世代，不能把舊契約價格冒認為新契約前日結算。
同日轉出條款可綁定1,291個事件候選，其餘仍保留缺口。

v289逐SHA回讀5,503份非空TEJ契約調整匯出、37,007列原生資料，對2,605個所需
參考價查詢鍵找到1,725個原生鍵。新標準契約有827個正參考價鍵，其中821個原生原因
明示「契約調整重新推出標準契約」；欄位定義與當年開盤前可知時間尚未驗證，未進入
帳務，沒有把(NULL)補成價格。原資料在`data_tej/`，衍生查核在
`data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/retained_tej_adjustment_v289/`。
863個尚未查詢的原生鍵，按相同日期合併為202筆有限任務；另有5鍵不在已捕捉原生軸，
沒有猜標籤或解除舊查詢隔離。

v292於10:37:56 UTC沿canonical `download_tej_history prioritize`完成登記，優先權-50、
863個商品月份／日期鍵、7,767個欄位值，未改全域計畫、未啟動第二個桌面或重送未知查詢。
v293按202個task ID獨立回讀均為pending；不是下載完成。目前原TEJ owner被舊任務
`7dd4ef72034c6a58e7b63353`的UNKNOWN擋住：pre-Preview stage未保留，而存活桌面仍有
原生預覽，原恢復器正確拒絕直接清掉重送。這是待處理的軟體／證據恢復問題，沒有改列
為不可解的外部限制。FinMind原12筆逐商品日資料優先意圖保留，但當前由既有Sponsor
全市場來源接管；pending per-ID沒有冒稱已執行，已完成／空回覆未被重設。
正式交接在`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/source_recovery/first_generation_reference/`，
含來源SHA、佇列回讀、尚未採用的界線與下一步；v294連結其驗收。

優先排程檢查原本對每個歷史查詢展開「商品×日期」，放大記憶體與比對成本。
canonical `tej_priority.query_axes`保留兩個集合，以兩軸皆相交判斷同一原生查詢鍵是否
重疊；KEY=1、日期正規化、完整／部分pending、已完成空回覆、UNKNOWN與批次原子性
規則均保留。396項相關測試通過。v291對256個固定歷史範圍與32個probe做ABBA測量，
每輪8,192項重疊／等同控制SHA一致；含tracemalloc時舊版136.12／142.74秒，
新版1.23／1.37秒，traced peak約602.0 MB降至8.26 MB。
這只是固定範圍檢查，不是SQLite、完整下載或訓練的加速倍數；實際202筆登記的完整
managed command耗72.59秒，保留獨立計時，沒有宣稱所有軟體瓶頸已消失。

| 現行產物／證據 | 相對repository root位置 |
| --- | --- |
| 原始資料與已驗來源 | `data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/corporate_integrated_rule_delta_v170/` |
| 已完成基準財務 | `data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v159/` |
| 舊已驗財務 | `data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v187/` |
| 上一版已驗財務 | `data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v260/` |
| 最新已驗財務 | `data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v275/` |
| 已完成2010起股票特徵 | `data_tw_index_futures/training_prepared/tw_futures_universe725_recovery_20261025_v1/stock_context2010_v46/` |
| 原始725碼用途及未解決清單 | `artifacts/markets/tw_futures_universe725_recovery_20261025_v1/coverage/financial_v275/` |
| 進度與獨立控制 | `artifacts/operations/futures-universe725-repair-20261025-v1/` |
| 新八組正式實驗根目錄 | `artifacts/markets/tw_futures_universe725_recovery_20261025_v1/` |

剩餘缺規格、保證金、部位時鐘、首次觀測參考價及同持倉組件缺口仍是待修工作，
不是外部限制。四個FC1／HW2／IQ2／LG1只有已查來源中的持倉／報價、沒有正成交證據；
保留來源與持倉延續，不能用參考價創造成交，也未宣稱所有資料商皆已耗盡。

## 2026-10-25 19:26 UTC 歷史里程碑（已由上段取代）

當時來源v170有17,652條公司行動條款；454份未完整綁定原件及其未綁定月份事件均已降至0。
來源v158交付85,868檔、7,392,657,376 bytes，兩端逐檔SHA通過；遠端v160當時等待節點鎖。

原始725個歷史代碼固定保留，各日期只依當時商品生命週期、條款、可知資訊與
實際成交准入，不要求商品出現在整段期間。歷史代碼清冊、帳務持倉主體與能新建倉
是不同分母：補來源能修復被錯擋的交易，不能替沒有成交的商品製造成交。

這次回收舊FinLab矩陣、官方原公告、GPU OCR收據與頁面影像，修正的根因包括
代碼辨識、日期／年份、實體月份、現金金額與修訂公告的精確範圍。
錯誤舊解析視圖只按原件SHA及完整舊欄位快照替換，正確的其他商品、日期與月份保留；
出版時鐘仍採已知的較晚索引時間，沒有倒填較早可知。公司行動推導版本為10，
財務編譯器維持10；拒絕續用舊帳務checkpoint。

v170通過121組逐原件條款控制，保證金候選、有效保證金與有效部位限制狀態逐項不變。
部位候選的差異已獨立核對為兩份原件的視圖證據與重複視圖，不改有效限額。
十筆舊錯誤狀態分別有EZF→IZF與LWF年份修正原件，沒有未解釋的正確狀態回歸。
v155的50筆差異拒絕與v167舊證據路徑失敗均保留，未以放寬財務規則通過；
v170原執行器因候選視圖身分不同而停止，另由v172完整獨立回讀確認有效狀態、
121組控制及政策SHA後，v173才將來源階段標為完成。

v165凍結程式SHA為`e0e00b4c3eeb420e80735b2fe7929c932d477a9313ab07e2d05868b16cdc2aa6`，
實際凍結套件通過1324項機制測試（47.73秒），原明列範圍外23項歷史fixture仍為deselected。
v170資料manifest SHA為`aeafeb0d6b0bc08e95e95577fb1213c457d7ba7433116647cb1de272071d3fb3`。

目前最近完成的完整財務驗算仍是v115：725個帳務主體、721個有正成交准入的代碼，
七組原件現金控制通過。其1,766,347個正成交契約日中仍有128,627日未准入；
不能把v170來源新增數量當成這些日期已恢復。新帳務v159須重建、逐正成交鍵回讀、
七組既有現金控制及121組新原件事件驗算，才可知道實際改善。

遠端`stockagent-futures-source-preparation-v160`已核對程式、完整來源與控制清冊，
現況為`waiting_for_exclusive_source_build_window`；只停止自己的舊等待器v141並關閉其
autostart，其他既有訓練未停止。完整財務先前RSS尖峰51.1GiB，仍保留RAM預算與8GiB
reserve；等待同機資源是排程狀態，不是宣稱資料無法取得的依據。
19:26獨立回讀確認38個控制檔SHA、PID／start ticks／boot ID／cwd及實際命令，
當下cgroup raw headroom為18,647,257,088 bytes，v159輸出尚未建立。
證據為`source-v170-remote-runtime-readback-v177.json`。

另修正排程與模型設定的資料身分不一致：僅更新四個帳務／成交時鐘路徑，
base與八組ablation全部改讀`financial_research_equity_v159`／`source_v159`。
凍結v175保留v165所有程式與祖先設定，避免把同時存在的工作區父設定異動帶進來；
解析後設定差異恰為四項，模型、optimizer、費率、loss與停止規則不變。
完整來源／設定套件3263檔、93,338,425 bytes已兩端SHA回讀，
`native-financial-admission-v174-20261025T192320-78ae53f8`受監督等待v160完成後，
須再次核對v159財務、七組現金、121組事件、特徵及未啟動queue的狀態才交回原排程。
同時排入`remaining-positive-source-v176-20261025T192530-d676ea3c`，沿原逐鍵差集稽核
產生`remaining_source_worklist_v176.parquet`；使用原process-tree RSS與raw reserve guard，
未解原因不會自動轉成外部限制。這兩個等待器不表示資料或新訓練已驗收。

FC1／HW2／IQ2／LG1保留帳務及持倉延續。在已查官方八個歷史月份的112列、
保留FinLab矩陣、FinMind日資料與98日逐筆查核中沒有正成交證據；不以歷史收盤參考價
代替可成交價，也不宣稱所有供應商已耗盡。七條轉入持倉邊已有獨立控制，但這不表示
神經網路已實際持有或已證明前手的決策時點容量。

| 本輪現況 | 位置（相對repository root） | 驗收邊界 |
| --- | --- | --- |
| 最新已驗來源 | `data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/source_rule_validation_v170/` | 來源層，0份未綁定原件、0個未綁定商品月份事件 |
| 遠端來源位置 | `data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/corporate_integrated_rule_delta_v170/` | 有限交付清冊已展開本機有界來源租約，兩端全SHA |
| 原件控制 | `artifacts/operations/futures-universe725-repair-20261025-v1/complete-corporate-source-controls-v170.json` | 121組來源條款控制，尚非完整帳務驗算 |
| 來源交付證明 | `artifacts/operations/futures-universe725-repair-20261025-v1/financial-v158-source-independent-readback.json` | 兩端全SHA，不是冷備份或訓練通過 |
| 遠端準備進度 | `artifacts/operations/futures-universe725-repair-20261025-v1/remote-cpu-preparation-v160/progress.json` | 等待兩GPU節點鎖涵蓋CPU財務與特徵，防止主記憶體競爭 |
| 遠端財務目標 | `data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v159/` | 尚待完整建置及獨立回讀 |
| 財務驗收後接手進度 | `artifacts/operations/futures-universe725-repair-20261025-v1/native-amendment-v174-progress.json` | 本機受監督，等待財務與2010起股票特徵 |
| 重建後逐日缺口稽核 | `artifacts/operations/futures-universe725-repair-20261025-v1/remaining-positive-v176-watch-progress.json` | 本機受監督；非外部限制判定 |
| 新八組正式實驗 | `artifacts/markets/tw_futures_universe725_recovery_20261025_v1/` | 尚未開始GPU訓練 |

控制檔交付曾有單一SFTP通道停滯，已精確停止自己該通道並保留程序身分收據；
改用有時限的有限清冊SSH串流，維持原部署器逐檔SHA、舊owner停止與Supervisor配置驗收。
`controller-v145-transport-independent-readback.json`及
`remote-cpu-preparation-v141-controller-readback.json`保留獨立證明。

以下18:09 UTC以前記錄保留為歷史里程碑；現況以上述新回讀為準。

v115的原始正成交契約日分母維持1,766,347，其中128,627日仍未准入。
對照舊計算版本新增30,317日，但96,893個舊准入日因修正後的公司行動／持倉鏈檢查
而拒絕；全部有直接或同持倉組件的缺口證據，未把舊版覆蓋率當成財務正確性。
`positive-day-recovery-v115.json`的機制驗收通過不表示剩餘資料完整。

新增451份原始公告全部完成GPU重讀，零失敗，耗時1975.67秒，worker RSS尖峰
2,795,143,168 bytes；重用另3份舊完整OCR收據。沿原來源準備器產生1205個新視圖。
來源合成v120先被五份「2,000股」誤讀成「2股」的數字檢查拒絕，未交付或發布。
DLF、KHF、KOF、JQF、NOF五份公告的發文頁和期貨表格已逐頁查核，逐格轉錄及
頁面SHA保存在`corporate_units_visual_v125/reviewed-corporate-unit-cells.json`，
原OCR和失敗收據保留；同一未發布候選正以此證據重試，未放寬小乘數檢查。

完整新來源的交付器及遠端v123部署器已受監督等待獨立來源驗收。遠端財務與2010起
股票特徵建置將沿用canonical owner、兩GPU節點鎖和RAM預算，不與既有大工作競爭
主記憶體。本機容量不足時只使用明示租約的有界可重建tmpfs；來源與OCR需另經有限
清冊、全SHA與遠端獨立回讀才算交付。這些準備工作不是八組GPU訓練已開始。

以下16:48 UTC記錄保留為前一里程碑；現況以本節及上述收據為準。

這次先回查舊資料：保留的FinLab九張矩陣、官方日行情、原始公司行動公告及
2026-09-29的GPU OCR模型／完整收據均已找到。四個只有carry/OI的調整碼
FC1／HW2／IQ2／LG1，在本輪八個歷史月份官方日行情重查中共112列，成交量均為0、
OHLC為官方缺值符號；FinLab保留值為NULL。這些是已查來源的證據，不能概括成所有來源皆無資料。

原公司行動文字欄位保留的`|`、`：`等分隔符號阻止「不調整」欄位判斷，已修正為
僅移除標籤緊接的分隔符號，沒有改動任何字詞或數字。公司行動推導版本為6，
財務編譯器仍為10。凍結v110來源SHA
`92214ab538529ed0e58667c9c023b6c620e408031e0d4f095671c840835fb195`，
1265項機制測試通過，23項既有歷史設定fixture仍屬原明列範圍外。

逐頁檢查另外找出MBF 2019-07-30現金應為1000而非OCR的1800，並補回
GEF 2012-07-11的3000、IGF 2012-07-18修訂公告的1621（原公告1600）、
CDF 2019-06-24的16000。原件、頁面、文字與明示修訂關係均保留SHA；
CAF／HOF／DOF／MBF／GEF／IGF／CDF共七組來源控制已通過。完整財務控制仍在驗算，
來源控制通過不能當作可成交准入或訓練通過。

修正前的完整v100驗算已完成但拒絕採用：725清冊中僅723碼有帳務主體、719碼有
正成交准入，MBF現金控制也失敗。所有130719個未准入正成交鍵都已追到直接來源
或同一持倉組件的來源缺口，沒有將其歸類為無解限制；資料及失敗收據保留。
GE1／IG1失去全部帳務的母契約來源已補入v113，須由v115獨立回讀證實恢復。

v113來源仍有454份原件未完整綁定財務條款，其中3份可重用以前完整GPU OCR收據，
其餘451份沿維護中的單一GPU OCR owner重讀，逐頁保留原件、文字、位置及SHA。
候選置於明示租約的有界tmpfs，未當成持久來源或正式帳務發布；不把GPU OCR成功
當成文字正確或交易條款完成。程式保持FP32、固定既有模型與版本，未以低精度改寫數字。

| 本輪目前owner／產物 | 位置或名稱 | 已核對狀態 |
| --- | --- | --- |
| 凍結程式驗收 | `artifacts/operations/futures-universe725-repair-20261025-v1/frozen-mechanism-acceptance-v110.json` | 1265 passed |
| 修正來源 | `data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/corporate_integrated_rule_delta_v113/` | 本機來源整合完成，尚未交付此代 |
| 本機財務驗算 | `complete-source-validation-v115-20261025T163447-00df794d` | running；完整四階段＋七組控制＋逐正成交鍵回讀 |
| 財務候選 | `data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/financial_source_validation_v115/` | 明示有界tmpfs租約，非持久訓練panel |
| GPU OCR | `unbound-original-gpu-ocr-v117-20261025T164549-cd64cc49` | running；451份原件，不是模型訓練 |
| OCR候選 | `data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/corporate_gpu_ocr_v117/` | 明示有界tmpfs租約，逐頁候選 |
| 舊遠端來源owner | `stockagent-futures-source-preparation-v93` | STOPPED、autostart=false，精確停止／回讀收據已保留 |
| 舊接手器 | `native-financial-admission-v96-20261025T143012-03d29d8d` | 已精確停止，不再交付舊來源給八組訓練 |
| 新八組正式實驗root | `artifacts/markets/tw_futures_universe725_recovery_20261025_v1/` | 尚未開始GPU訓練；既有其他訓練未停止 |

16:48 UTC前的已完成來源交付為v108：78515檔、6944048537 bytes，兩端完整SHA回讀。
這不包含v110程式／v113後新增來源，不能據此宣稱新版已於遠端訓練。
收據詳見同一operations root的`superseded-source-owners-fenced-v114.json`、
`financial-v108-source-independent-readback.json`、`component-source-gap-trace-v109.json`、
`unbound-original-ocr-v117-live.json`及`recovery-v115-accounting-live.json`。

以下保留14:33 UTC以前各輪進度及歷史證據；owner的現況以上述新回讀為準。

原公告`1303_20110713.pdf`明列CAF在2011-07-13每口多單權益加9,400元、空單減9,400元，
但v12編譯值`carry_cash_twd=0`。OCR漏掉貨幣及現金欄位後，標準碼走一般契約備援，
導致缺少公司行動現金，也可能改變漲跌停參考價。這是可修正的軟體／來源問題，
不是外部限制；後續完整帳務需重新建置，舊版只保留為覆蓋與差異對照。

已修正同一原件明示每股現金股利乘以不變契約乘數的精確換算；可讀金額若矛盾則拒絕，
不借用選擇權現金欄、不改OCR數字、不把NULL當0。原件只有一個缺標籤的舊解析視圖時，
也不能豁免2011-05-03後同碼、不變約定標的物的現金轉換。編譯器新增原公告事件邊界
與完整條款的對照；標準碼同樣須通過，不以新解析失敗為由使用舊條款跨越公司行動。
`EXECUTION_TERMS_COMPILER_VERSION=10`，公司行動推導版本5；拒絕重用舊計算checkpoint。
未解析的公司行動不只阻擋當天，後續也不得重新沿用已過期條款，直到同原件的完整條款可綁定。

指定725清冊選中的2,594份公告已全數重讀，包含原本漏掉的空實體月份工作列。
工作清單為415個商品、12,395個鍵；連同相關原件共讀2,697份，產生8,818個新視圖，
其中1,779份原件的5,700個視圖可由明示原文精確推導現金。這些數量屬來源層，不能直接當成可交易日。
18份需補OCR／公告日期的原件已完成完整頁面OCR；以原始索引公告、唯一發文日及明示生效日
約束較晚的公告日期，仍採較晚索引日期，沒有回填成較早可知。HOF另由原始兩頁人工查核。
整合時抓到DOF 2016-07-15的OCR把2,000讀成2，已逐頁查核改以原件2000及每口721元。
來源層CAF／HOF／DOF的五個月份均分別為9400／11000／721元、乘數2000；
完整帳務另設三組獨立控制，不以來源層通過代替財務發布通過。
凍結v80通過1193項機制測試，23項原有歷史設定fixture明列排除。
原生財務來源已完成61,121檔、6,548,675,189bytes的兩端完整SHA交付，但該交付不能
證明上述公司行動帳務已修好。`corporate-cash-mechanism-audit-v50.json`及
`native-amendment-v48-fenced-v50.json`保留發現與精確停止收據。

14:09 UTC前，新版v82完整來源閉包78,511檔、6,943,682,070bytes已在兩端逐檔SHA核對。
遠端既有57,700檔可直接核對，20,729檔由相同SHA的既有內容複製重用；實際跨網路差量僅
82檔、15,384,440bytes，沒有以傳輸硬連結共享可變輸出。清冊SHA為
`748a99a2f068359fe7bf95c0bff64095a0cbc5a8d34a0b79caf2f0fe5ff57510`。
此交付證明輸入到位，完整財務與正成交日驗收仍待執行。

14:17 UTC，v84的完整財務階段由自己的容量保護程序以143停止，v85接手器隨之拒絕接手。
原保護程序沒有保存觸發分項，不能把退出碼解讀為CUDA錯誤、kernel OOM或單一RSS超限。
同期另一個既有GPU訓練擴張主記憶體使用，顯示兩個大型工作共用主機的入場競爭需要協調；
這是可修正的排程問題，不列為無解的外部資料限制。v82未完成候選完整保留，沒有發布或續用。

v93改沿現有兩GPU節點鎖等待獨占來源建置時段；鎖涵蓋CPU財務及股票特徵準備，避免
GPU配置前的主機記憶體競爭。既有訓練未停止。取得鎖後須實測至少96GiB raw headroom，
帳務worker上限80GiB、最低入場64GiB，8GiB raw reserve保留；這些是容量預算，並非加速結果。
v94補上每15秒資源收據及觸發停止的確切分項；只建立新的輸出目錄，科學輸入仍為
v82來源／研究政策、v80程式。三組現金控制、逐正成交鍵回歸驗算及全部4105日期特徵
未通過前，v96不得將八組實驗交給原訓練owner。v98在獨立驗算完成後產生逐鍵剩餘來源清單，
不把未解決原因自動歸類為外部限制。原v91等待器已精確停止，失敗及候選證據均保留。

目前監督與產物位置（相對repository root；財務候選在遠端建立）：

| 內容 | 位置或監督名稱 | 已核對狀態 |
| --- | --- | --- |
| 修正來源 | `data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/corporate_integrated_rule_delta_v82/` | 已交付，完整SHA核對 |
| 財務候選 | `data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v94/` | 尚未建立，等待鎖 |
| CPU owner | `stockagent-futures-source-preparation-v93` | RUNNING；等待獨占來源建置時段 |
| CPU進度 | `artifacts/operations/futures-universe725-repair-20261025-v1/remote-cpu-preparation-v93/progress.json` | 遠端即時狀態 |
| 原生驗收接手 | `native-financial-admission-v96-20261025T143012-03d29d8d` | 本機受監督，等待財務／特徵 |
| 剩餘來源逐鍵清單 | `remaining-source-worklist-v98-20261025T143040-97d603de` | 本機受監督，等待v94驗算 |
| 正式實驗root | `artifacts/markets/tw_futures_universe725_recovery_20261025_v1/` | 保留原root；新八組尚未開始GPU訓練 |

13份controller與依賴的SHA已於遠端獨立回讀。來源修正1193項機制測試不等於這次完整
財務、特徵或epoch/fold驗收；不據此宣稱純GPU運算瓶頸或策略改善。
剩餘首次實體觀察、部位公告時鐘、ETF分段限幅等仍須逐項查證，並非已判定無法補齊。
FC1／HW2／IQ2／LG1目前只有carry/OI證據，無已驗證正成交；四碼保留帳務主體與查證清單，
不以假成交、NULL變0或同族代碼價格使其看似可交易，也未宣稱已窮盡所有外部來源。

此前最新完成完整發布、逐碼及逐正成交日覆蓋驗收的本機版本是
`financial_research_equity_v12`：原725個代碼均保留帳務主體，721個有正成交的代碼
均至少有可執行日行情；原始1,766,347個正成交契約日中，1,707,153個已准入，
59,194個尚未准入。v12比v8新增2,857個正成交契約日，原已准入日回歸0個。
這些是舊計算版本的覆蓋量；上述公司行動缺陷解決前，不再宣稱它們通過完整財務正確性驗收，
也不能與僅在規則來源層恢復的日期相加。

八組遠端實驗尚未啟動GPU訓練。舊準備排程在10:50 UTC因股票特徵缺少177個
2010年期貨交易日而失敗；只改`panel_start_date`不能補回實際特徵。其後v24真正
建置至第3,648個日期邊界，但12:39 UTC年度事件版本判斷的程序樹RSS達
34,831,409,152bytes，超過32GiB程序預算而由保護程序停止；不是GPU錯誤或資料
不存在。原候選、已寫區塊、失敗收據及其他排程均保留。

已修正版本判斷的中間表：排序／window僅攜帶日期、商品、狀態鍵整數、版本rank與
原列索引，最後從原表取回數值，不搬運完整長名稱及數值到每個中間表。
凍結來源v47的1,106項測試通過，23項既有歷史設定測試仍在原本的別項fixture範圍；
此次額外10項synthetic準備測試已補齊其獨立凍結YAML閉包，不以缺fixture略過。
來源SHA為`efdcffffee91d59043e6c52ae2e6fb3673091ec2a34caf3edb0dbe7b571ce16d`。
遠端3,301檔、94,999,016bytes程式／CPU工具已逐檔兩端SHA核對。

v47 CPU owner在13:42 UTC因raw cgroup保留量而停止，特徵僅完成2208／4105個日期，
程序樹RSS尖峰31,739,998,208bytes，低於48GiB程序上限；不可誤報為仍在運行或特徵完成。
獨立v84 CPU owner已交付三個控制工具、逐檔SHA核對並由Supervisor啟動，先等待v82完整來源交付，
再完整重建財務、驗證三組公司行動控制，最後接續股票context。只會搬移保留自己失敗的
未發布候選並核對全部SHA，不刪除；沿用同一已驗證年度快取，context入場最低56GiB、
程序上限64GiB及8GiB raw reserve。這是待實測的容量調整，未宣稱已解決全流程記憶體瓶頸。
原v48接手器已精確停止，不會啟動有舊公司行動機制的實驗。
完整財務、全部特徵成員SHA及日期涵蓋均需重新核對，才可修訂同一八組佇列。
v29輸入曾完成61,077檔兩端SHA驗收，
但未開始完整財務建置，現保留為v46來源父代，不能算成已發布資料。

| 原始725清冊的驗收 | 修改前 | v5 | v8 |
| --- | ---: | ---: | ---: |
| 有財務帳務列的代碼 | 725 | 725 | 725 |
| 至少一天有正成交且日行情財務准入 | 717 | 721 | 721 |
| 有正成交來源但全代碼未准入 | 4 | 0 | 0 |
| 無已驗證新成交、保留轉入持倉的代碼 | 4 | 4 | 4 |

救回DR1、HF1、JJ1、NK1；原717代碼沒有回歸。獨立本機驗算為
`financial-recovery-independent-readback-v5.json`，15份遠端來源驗收／ABBA收據已逐檔
遠端前後及本機SHA核對，見`v5-accepted-independent-readback.json`。
這是代碼層驗收；不代表每一天都可成交，也不代表全歷史官方財務資料已補齊。

## 本輪新增來源與前處理修正

v28來源候選曾恢復16,861個正成交契約日所需保證金；後續v34的17,523日候選
含一個未採用的人工轉錄錯誤：TF 2020春節調整前應為52,000／40,000／38,000，
不是50,000／39,000／37,000。兩份原公告已逐格重看，v45明示更正；v34從未進入
完整財務或GPU訓練。缺值覆蓋沒有退步不足以證明轉錄正確。

目前完成來源驗收的是v45：在62,980個商品日對照，恢復8,175個保證金數值完整
商品日、18,161個正成交契約日的所需來源，原可用日期回歸0個。這包含先前候選，
不可相加，也不是完整財務新增准入量。保留六個2018年TE／T5F逐格覆核的真實
錯值修正，沒有新增任意數值容忍。

本次原件修復包括：2018樺漢OQF被OCR誤辨成大立光OOF；2015春節T5F漏表；
2019富櫃200 G2F初始保證金附表；2020春節各商品原件；2024 TMF上市金額；
康芮休市把2024-10-31收盤後調整順延至11月1日收盤後。前2017-05-15公告的
明示「收盤後生效」以當時唯一日盤關閉時點解讀，較晚日期仍須自己的交易時段證據。
代碼更正進入合成器時再次核對原／新代碼、原公告SHA、頁面SHA及全部六個金額，
只有該確切原件可以擴大舊缺口的商品範圍，沒有全域OOF→OQF替換。
來源證據見`nonwarmup-margin-independent-source-readback-v45.json`及
`remaining_primary_chain_originals_v41/source-readback.json`。

舊FinLab資料已找回並逐檔核對九張期貨矩陣與原下載收據，沒有新增供應商請求。
FC1／HW2／IQ2／LG1皆有未沖銷部位紀錄，但其OHLC及成交量全為NULL，並非數字0；
DR1／HF1／JJ1／NK1的正成交對照均正常。這支持四個調整碼保留持倉接續與估值，
目前不能憑空新增開倉；不代表已窮盡所有供應商。FinLab商品／交易時段矩陣也不能
無證據拆成各實體到期月份。原始授權payload留在本機，未當作可自由轉散的來源上傳。
證據：`retained-finlab-source-reconciliation-v30.json`。

完整建置另修復記憶體生命週期：在最後使用位置釋放`observed`、`evidence`、`raw`、
`physical`四個大型DataFrame。移除這四個`del`後，帳務函式AST與已接受版本相同；
不修改算式、遮罩、精度或資料。凍結版418項機制測試通過，另23項既有設定fixture
不在膠囊驗收範圍。來源SHA為
`c6c0959cf6190778394cb0362290fdb4ba8e5951162af033f9e73d0950c5cd52`。
完整財務記憶體與端到端耗時仍待遠端實測，不能由釋放敘述推算加速倍數。

八組計畫把資料、時鐘與2010特徵指向獨立的新世代；typed loader須逐欄驗證其餘解析設定相同，
維持從零訓練、d32／lookback32／global128、Muon、log utility、最多1000回合、
連續100回合無改善、08:45／09:00成對比較。原生遠端建置驗收與跨機交付收據分開，
不替不存在的本機完整副本偽造「已同步」證明。舊時鐘與checkpoint不覆寫或混用。

尚未修完的軟體／來源工作包括：2026-07-06後14種ETF的三階段漲跌幅、29個
部位公告時鐘相關正成交日，以及其餘有限保證金鏈缺口。原件已保留；不得稱為外部
不可解限制。首次觀測日21,245個則屬目前帳務context規則，開放初始空倉首日下單
需另驗證上市參考價與盤前容量，不能把warmup旗標直接清掉。

首次觀測另有具體可查來源：原始每日行情已保留漲跌價。全來源、跨商品與完整期間，已知前筆實體結算價的
1,930,958筆正成交列中，`close - reported_price_change`有1,922,938筆與其一致；
8,020筆不一致仍需逐項辨認公司行動、缺日與報價語義。首次實體觀測的19,384個
受阻正成交鍵中，19,349筆可算出正的候選參考價。這是診斷線索，尚未建立盤前
可知性與初始空倉帳務契約，不會寫成已觀測前日結算價、開盤成交或模型特徵。
收據為`first-observation-reference-audit-v44.json`；這仍是待解工作，未判為外部限制。

目前正式本機資料：
`data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v12/`。
遠端待建置目標資料：同父目錄的`financial_research_equity_v82/`；股票特徵：
`data_tw_index_futures/training_prepared/tw_futures_universe725_recovery_20261025_v1/stock_context2010_v46/`。
修復收據：`artifacts/operations/futures-universe725-repair-20261025-v1/`；
新八組正式訓練產物預定在`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/`，
尚無新GPU checkpoint或可比較的新策略績效。

截至11:37 UTC，已完成的完整發布與來源候選分開列示：

| 驗收層 | 版本 | 相對v8的結果 | 可作何種結論 |
| --- | --- | ---: | --- |
| 完整財務、逐正成交契約日差集 | v12 | 新增2,857日；回歸0日 | 本機正式財務資料可用範圍擴大 |
| 保證金來源對齊 | v19 | 補回14,141個原缺保證金的正成交契約日；來源覆蓋回歸0日 | 尚需完整財務編譯確認實際新增准入量 |
| 規則整合與遠端來源交付 | v20 | 61,023檔全SHA一致 | 僅證明完整輸入已到Vast |
| 額外上市與春節公告 | v25／v28 | 已取得並檢視11份既有官方原件；候選編譯中 | 不先計為已增加的可交易日 |

v19找回的18檔ETF上市初始保證金，原本散在`other_announcements`的上市附表，
並非不存在資料。另補2013、2018、2019、2020年春節／一般調整原件。
2018年的臨時調高與恢復前後，TE及T5F在2月12、21、22日共六個產品日的舊金額
被原件否定，已逐欄更正；其餘已知金額不能用全面容差放過。
缺少恢復規則、會造成306日來源覆蓋回歸的v17候選被保留為失敗證據，未採用。
v18補回2018年2月22日收盤後恢復規則，v19沿用此修正。

額外原件包括半導體30／航運、小型電子／小型金融、中型100、永續／生技指數
期貨的上市初始保證金，以及2017年恢復與2019年春節臨時調高完整鏈。
原件與逐頁SHA位於`data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/primary_chain_retained_v25/`。
v25初次手工轉錄省略了2019年新聞稿的發行抬頭，因發行日期無法綁定而正確拒絕；
保留失敗候選，v28加回原抬頭，未放寬同一來源時鐘驗證。

股票context使用遠端既有、SHA核對過的原始股票快照重建，保留1999年起原始日曆，
實際輸出從2010年開始，walk-forward仍由2011年起切分。新維護程式提供
`--release-source-read-cache`：每個來源完整SHA及檔案身分前後核對後，僅建議OS
釋放乾淨讀取快取頁；不刪檔、不省略來源驗證、不降低8 GiB記憶體保留量。
凍結程式v23的418項相關測試通過，另有23個不在所選發行包的舊fixture案例明列
排除，並非全測試套件通過。這不證明完整fold已加速或達到純GPU計算上限。

遠端獨立閒置檔案核對已完成396,266檔、44,729,388,725 bytes，235.10秒，
逐檔完整SHA及inode／metadata前後一致，刪除0檔、未做全系統cache flush。
當時原始記憶體headroom由24,884,973,568增至148,924,071,936 bytes；
其他訓練同時改變配置，不能把整個差值或時間改善全歸因於快取建議。
後續容量重新量測仍可能不足，串行owner再核對三個具完整驗收的舊財務資料根，
跳過任何開啟／mmap中的inode，完整帳務工作以當下容量及持續監測准入。

61,023檔來源交付另外修正了兩個工程問題：只按發行包的canonical code inventory
核對程式，不將執行期產生的`.pyc`當成不可變來源；原財務來源由`/srv/`既有
immutable materialization提供，按其完整來源manifest和逐檔SHA核對，不能誤以為
所有讀取來源都必須物理位於repo內。兩次拒絕的清冊及錯誤均保留，未覆寫原始來源。

驗收收據位於`artifacts/operations/futures-universe725-repair-20261025-v1/`：
`positive-day-recovery-v12.json`、`nonwarmup-margin-independent-source-readback-v19.json`、
`financial-v20-source-independent-readback.json`、`frozen-mechanism-acceptance-v23.json`。
遠端即時CPU階段在同根的`remote-cpu-preparation-v47/progress.json`；v24是保留的失敗版本；
GPU正式產物根仍為`artifacts/markets/tw_futures_universe725_recovery_20261025_v1/`。
目前未將59,194個未准入日、21,245個首次觀測context日、12個分鐘來源矛盾鍵
或2026年分階段漲跌幅的971個規格缺口宣稱為「已證明的外部限制」。

## 研究分母與驗收

目標是讓2011-01-26至2026-09-24清冊內的725個台幣股票、ETF與指數期貨代碼，
在各自實際存在的日期可被研究與選擇。這725個代碼含公司行動後的調整代碼，
不是725個同時存續或相互獨立的風險。逐日集合沿用
[逐決策商品集合](tw_futures_dated_membership_2026-10-25.md)，不要求全期間都出現。

本次把問題分成七個可以分別驗收的條件：

1. 商品身分、上市與公司行動譜系可追溯。
2. 官方日行情與獨立成交來源的日期、實體契約、價量可核對。
3. 保證金、乘數、價格界限、結算與持倉轉換有明確版本。
4. 同一持倉連通組件的逐日估值完整，不能靠丟棄持倉跨過缺日。
5. 決策前已知的特徵與參考價符合時鐘；沿用參考價不等於成交。
6. 決策後有真實成交量，並滿足窗口、參與率、整口與資金限制。
7. 通過共同日期與來源驗收後才進入訓練，模型表現另行比較。

清冊收錄、候選集合、日行情可執行、一口入場代理與帳戶實際成交必須各自報數。
任何一層的聯集都不能當成每一天的可交易集合。既有717／725是財務准入後至少一天
有可執行日行情的數量；375則是舊測試期間的聯集，分母與期間不同。

## 找回既有來源後的實測

官方全市場來源有55,852個檔案。本次另外重用既有FinMind Sponsor歷史，
核對5,721個日期、5,788,179列日行情：3,833個非空日期、1,881個明確空回覆，
以及7個已有官方依據的空日期修正；沒有為此重打整段API。
原始官方來源的NULL／破折號與FinMind的數值零分別保留。

原始725清冊中，721個有官方正成交量；以下四個有真實成交，但舊財務發布沒有准入：
DR1、HF1、JJ1、NK1。它們是本次要修復的軟體／財務來源問題，不能歸咎於低流動性。
另有FC1、HW2、IQ2、LG1四個代碼，現有官方日資料沒有已證實的正成交量。
四者在FinMind分別有14、56、28、14列，皆為數值零；它們不是被全期間交集排掉。
保留的FinMind全市場逐筆另核對98個相關日期、18,104,418筆成交，未找到四者的實際成交，
同批來源能找到HF1與JJ1的正控制。這是有限來源查核的否定結果，
不宣稱所有供應商都已窮盡，也不將缺資料改成零成交。

四者均有官方未平倉量，最大值依序為1、22、2、9口；LG1另外有部分日終買賣報價。
這些不是決策時點的委託深度，也不能證明08:45／09:00可成交。
舊已接受帳務可找到FCF→FC1、HW1→HW2、IQ1→IQ2、LGF→LG1的公司行動轉入邊，
以及四者的現金到期結算。因此「沒有新開倉成交證據」不等於「整個代碼不能用」；
新發布須核對所有實際轉入邊。來源與結構性路徑收據為
`nonprint-code-market-evidence.json`與`nonprint-owner-continuation-parent.json`，
不代表神經網路實際選擇過這些持倉。
v5回讀已確認原有五個轉入邊的前一契約、日期、有理數口數、每口完整前值與
現金結算逐項相符；四個代碼分別保留15、56、28、14列帳務，沒有新增可執行列。
這份驗證為`nonprint-owner-continuation-v5.json`。完整邊清冊另已驗證七條轉入邊，
涵蓋四個持倉主體；其中六個目的契約有原生現金結算，一個FC1契約沿用原已披露
研究現金轉換。見`nonprint-owner-continuation-all-edges-v5.json`與
`nonprint-all-edges-independent-readback-v5.json`，不能將後者當作官方終端清算觀測。

SR1在舊財務發布中已經有7列可執行日行情，不能列為「整個代碼新增救回」。
其0050分割停牌期間的估值延續仍需另外核對：TEJ有限查詢取得5個停牌前對照、
5個停牌日未回列；保留的FinMind亦未提供相應停牌日數值。
恢復交易日開盤參考值的交叉核對不能代替所有停牌日的官方清算價。
分割日期、契約調整與暫停交易依據見
[期交所0050契約調整公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/0050_20250618.pdf)；
公告與日後開盤參考值不能補成當時未觀測的逐日價格。

37個稀有實體契約日已走過官方成交、扣除鉅額交易、合法價量與窗口的獨立核對：

| 代碼 | 正成交實體契約日 | 08:45後30／60／300分鐘有成交 | 09:00後30／60／300分鐘有成交 |
| --- | ---: | --- | --- |
| DR1 | 19 | 0／4／17 | 2／5／17 |
| HF1 | 5 | 0／0／5 | 0／0／5 |
| JJ1 | 2 | 0／0／2 | 0／1／2 |
| NK1 | 1 | 0／0／1 | 0／0／1 |
| PZ1 | 10 | 2／4／10 | 4／5／10 |

表中的「有成交」仍不等於一口容量。以50%參與率、整口向下取整，
窗口只有一口成交時容量為`floor(1 * 0.5) = 0`。
不得為了湊滿代碼而改成向上取整，也不能把前次成交參考值當成本日成交。

## 修正與計算設計

`prepare_tw_futures_margin_training.py`重用有雜湊、計算身分與政策差異驗證的舊帳務，
只重算研究估值政策受影響的214個商品及其公司行動閉包，而非全765個來源商品。
原先使用142商品的`frozen_contract_value_research_v1`明示研究估值政策；
沒有把研究估計冒充歷史官方財務觀測或新的實盤成交。
v5將有限SRF／SR1停牌延續加入同一具名研究ABI，政策商品144個，受影響閉包218個。
SR1小範圍驗證納入SRF、SR1、NYF、NY1四商品，25列停牌日估值沿用停牌前
已觀測完整契約價值，保留公司行動現金／單位調整與原生觀測優先；不是補回官方清算價。
該探針已驗證四個SR1首次交易日的財務阻擋解除、原生OHLCV／可執行列不變、
沒有新增終端估值假設。v5完整發布及原725清冊驗收亦已完成。
金融帳務快取仍保留全來源；發布時再選取股票、ETF、指數期貨，
並拒絕切斷範圍內外的庫存轉換邊。利率／商品期貨不混入本次台股研究。

修正新舊資料合併時研究旗標變成NULL的問題：已驗證沒有研究估值的舊列明確標False，
原始價格與其他NULL保持原樣；缺少研究披露的列仍會被拒絕。
帳務、完整組件篩選、正式發布、獨立清冊查核分成四個程序，
讓每階段完成後釋放記憶體。依當下cgroup剩餘容量配置程序預算，保留8GiB原始餘裕，
不假設頁面快取一定可回收，也不停止其他訓練來騰空間。

另外修正完整組件篩選的研究起點判斷：訓練從2011年起，不代表公司行動祖先契約也
必須從2011年後上市。新具名合約允許完整、財務已驗證的組件與訓練期間相交，
保留更早歷史作為持倉／公司行動上下文；更早的財務缺口仍會拒絕，不剪斷持倉鏈。
此修正直接處理DL2、DR1被歷史起點排掉的問題，特徵／決策日期不因此回溯。

300分鐘來源走逐日串流：同樣核對完整交易時段的SHA、價量與鉅額扣除，
只保留各窗口需要的FP64縮減結果，不常駐全部歷史分鐘。
實際來源ABBA必須同時核對報價、數量、身分、狀態及來源；
小型37日測量不能當作全市場加速或硬體上限的證據。

37個稀有契約日的真實來源ABBA已通過，四次完整均值reference1.952173秒、
bounded2.216112秒，報價最大絕對差0、容量／身分／狀態／來源完全一致。
串流版在此小樣本約慢13.5%，不宣稱這份測量有加速；選用理由是限制完整歷史
分鐘資料常駐量，完整3833個非空來源日期的準備仍須實測。
ABBA使用canonical篩選前帳務frame並逐列對照官方價量：37日中PZ1有四日未進
財務發布，不能因分鐘查核成功而放行。缺欄位及分母不符的兩次測量失敗均保留。

v6另修正G2F價格格距證據起點：官方2019-09-30上市資料不能被通用的
2020-03-23界限排除。舊v5完整時鐘在第2200日遇到此問題停止，失敗證據保留於
`clock-readback-failure-v5.json`。修訂來源SHA
`8ff0c9a7a74d7e9f63456661d14d37302db100a56e7aac1bb0a4e32476c8d094`，
通過296項機制測試；沒有擴大原有有限來源例外。
v6的37日ABBA均值reference2.083730秒、bounded2.134359秒，約慢2.4%，
報價最大絕對差0、容量／身分／狀態／來源一致。證據在`clock-abba-v6/`，
`v6-clock-and-policy-independent-readback.json`完成五份ABBA及相關小型來源文件
遠端前後與本機SHA核對。此測量仍不是全市場速度或GPU上限證據。

## 八組從零訓練對照

設定為`configs/ablations/tw_futures_universe725_recovery_20261025_v1.yaml`。
08:45／09:00各四組：前日量容量30分鐘、一口探單30分鐘、一口探單300分鐘、
以及300分鐘全觀測量參與敏感度。探單只放寬決策前的委託容量准入，
成交仍依決策後實際窗口量與整口向下取整。
300分鐘是事先承諾等待的研究成交窗口，不使用刷新後的盤中特徵；
它不等於所有外國ETF期貨完整交易時段。100%參與組明示為成交量上界敏感度，
不能視為無滑價、無排隊或保證實盤可成交。

所有組保持d32、lookback32、global batch128、Muon、單一手續費後log utility、
先驗beta上限0.25、從零初始化、最多1000回合及連續100回合無改善停止。
先做完整4回合工程驗收，包含val／test、曲線、圖與checkpoint；
通過者才由既有canonical排程在dated-membership實驗後執行正式收斂。
測試區已被反覆觀察，屬研究比較，不能稱未觸碰的外部樣本。

## 產物與目前階段

以下路徑均為repository-relative；大型來源與準備資料主要位於遠端114.34.26.236:43269。

- 來源、失敗證據、來源回讀、凍結程式測試：
  `artifacts/operations/futures-universe725-repair-20261025-v1/`。
- 新排程與進度：`artifacts/operations/futures-universe725-recovery-20261025-v1/`。
- 新財務資料：
  `data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v5/`。
- 新決策時鐘：
  `data_tw_index_futures/training_prepared/tw_futures_universe725_recovery_20261025_v1/twd_equity/clocks/`。
- 正式模型、曲線與逐組清冊：
  `artifacts/markets/tw_futures_universe725_recovery_20261025_v1/`。

07:29 UTC快照：凍結來源`beb13c583ce8a7afae994d3cb16aa116a4676904fdca1fb07c19d8b374c33be1`
通過173項相關機制測試；13項依賴舊設定檔的歷史測試在凍結來源膠囊中排除，
不計為本次通過。財務重建與後續準備Supervisor均已啟動；
後者目前等待前者的獨立來源清冊驗收，八組尚未開始GPU訓練。
最終「原始725中救回幾個」必須以新`financial_research_equity_v3-source-inventory.json`
和八組canonical membership報告為準；不能拿全來源745個代碼代替。

07:34 UTC讀回：帳務階段完成，281.075秒、程序peak RSS42,548,944,896bytes；
這只是帳務階段，不是完整準備或epoch的計時。篩選階段以新程序重用已驗證的
4,915,195列規則，沒有再做金融重算；原有GPU訓練仍保留，cgroup沒有OOM／OOM kill。
新排程另加入原725清冊前後對照，若原717個已准入代碼有任何消失，正式訓練前會拒絕。

07:39 UTC修訂：篩選第一次實測在原始cgroup餘裕8GiB保護線停止，
peak RSS48,937,930,752bytes，沒有OOM。追到快取dictionary仍保有全量引用，
發布範圍另外複製完整frame／rules，以及兩輪完整篩選結果同時常駐。
已刪除過期引用、在第二輪篩選前釋放第一輪結果，範圍檢查先投影兩個必要欄位。
金融計算與篩選條件不變，既有281秒帳務快取經計算身分及全檔SHA驗證後重用。
新凍結來源`a14c5edc3ccd15a5acc7a317fd08a9f0eb8fe63f81e7a965d54e28ffef4ae555`
再次通過173項機制測試，56.48秒；重試的峰值與完整發布仍須實測回讀。

07:46 UTC完成舊候選發布（`financial_research_equity_v3`）的工程查核：
篩選194.744秒、peak RSS41,273,823,232bytes；發布129.186秒、peak RSS27,090,960,384bytes。
原725清冊中可執行代碼717→718：新增HF1、JJ1、NK1，但DL2、SR1消失，
因此該候選未進入訓練。這不是合格的來源修復，也不是全流程加速比例。

07:58 UTC：v5小範圍SR1驗證通過，46.887秒、peak RSS5,654,315,008bytes；
原生價量及可執行列完全相同，新增25列皆明示研究估值。
v5凍結來源完成196項機制測試，46.71秒；13項歷史設定檔測試仍未計為通過。
整份財務重建已開始，尚未完成逐碼驗收、實際來源ABBA或新八組GPU工程驗收。
`owner-coverage-amendment.json`新增725個會計持倉主體完整性檢查，
保留721個有正成交證據代碼與無新成交證據持倉主體的差別。

08:08 UTC：v5完整財務發布與原始725清冊查核完成。帳務282.870秒、
peak RSS42,737,106,944bytes；篩選200.625秒、peak RSS41,559,977,984bytes；
發布130.837秒、peak RSS27,273,166,848bytes；查核3.965秒。
上述是各階段時間，不能代替包含等待、分鐘重建、GPU訓練與報表的全流程時間。

08:20 UTC：完整決策時鐘重建進行中，尚未完成八組canonical membership或GPU工程驗收。
暫時不足的CPU記憶體改為留在同一Supervisor等待，按原始餘裕設定受監控的程序預算，
保留8GiB原始cgroup餘裕；ABBA程序樹亦納入RSS監控，不只測父程序。
新八組仍未開始GPU訓練，後續依dated-membership canonical佇列順序執行。

## 代碼已齊與逐日期仍有缺口的區別

另將原始725清冊所有1,766,347個完整正成交契約日，與新發布的實體日期鍵做差集。
1,680,427個有對應可執行列；85,920個尚未發布，涉及545個至少已有其他准入日期的代碼。
這不是85,920個代碼，也不是四個無新成交證據持倉主體的問題。
原因含保證金／法律時鐘缺口，以及同一持倉組件內缺口造成其餘日期不能安全接續；
不能把這些都標為外部不可解限制。完整明細與重疊阻擋旗標記於
`positive-day-remainder-v5.json`，旗標計數不可直接相加當成不同缺日。

已再對照[2011年TX／MTX已驗證來源](tw_futures_margin_earliest_history_2026-09-28.md)及
[過去完整修復工作](tw_futures_remaining_gap_execution_plan_2026-10-03.md)。
較早的規則公告與來源轉錄仍保留在原canonical規則工作區；當前來源也已含2010／2011
TX／MTX保證金區間，不能把所有缺日一律歸為「以前沒下載」。
本輪完成725代碼可進入研究帳務的範圍，不宣稱上述85,920日或全歷史PIT已修完。
SR1的25列新增停牌估值仍為具名研究估計，官方逐日清算值尚未恢復。

## 重用舊來源修復TX／MTX逐日保證金

舊`verified_scope_2011_20260928/rules/`的728份原始來源均完成SHA核對。
其41,967個TX／MTX契約日規則，與當前已編譯財務規則重疊的非缺值座標無數值差異，
卻有9,481個契約日在通用保證金區間對齊時缺至少一個開盤／結算保證金值。
根因包含通用來源建置未讀取舊schema的人工來源覆核，以及未解析的重複公告提前
結束有效區間；不能把這些當成供應商沒有資料。

修復沿用canonical公告、六個保證金數值格及發布／生效時鐘處理，未直接採用舊衍生
財務表取代來源。先復原8,611個契約日；剩餘870個進一步找回官方春節續用及
颱風休市更正公告後，已全部補上來源區間。資料均已留存，兩輪沒有新增API或OCR查詢，
沒有推測保證金數字。第二輪獨立對齊結果為9,481→0個缺值契約日、零新增缺日，
涉及1,761個商品日；明細為`retained-margin-clock-recovery-comparison-v7.json`。

與舊衍生表有22個契約日數字差異，其中10個原已存在，與到期日結算階段不同有關；
新增12個皆為2024-11-01開盤。舊表把10月31日休市期間預定調整直接提前到開盤，
本次依[期交所10月30日更正公告](https://www.taifex.com.tw/cht/11/newsDetail?idx=15446&newsType=1)
改為11月1日一般交易時段結束後生效，保留開盤前較低的舊保證金，
結算階段才使用新額度。來源與舊結果不同時，依公告機制驗證，不強制抄回舊表。

v8共同合併管線通過402項測試；23項依賴另份歷史設定／覆核資料的測試不計為通過。
獨立合併前後回讀確認只有TX、MTX保證金候選與區間改變，公司行動、契約單位、
持倉限額及其他商品狀態均未改動，原已接受delta未被覆寫。
合併delta SHA為`a88d1df048760fea341316cd79491c1bf69d36ece4785649da967d751ac5fa85`，
見`integrated-margin-rule-staging-readback-v8.json`。

v8財務重建使用來源SHA
`d1787af106ffa2a8579ac9cfa5539aa89be27ae16638251d29013a9342ca4daa`，
144商品的研究估值範圍、值與假設保持原樣，僅將來源綁定移到通過獨立回讀的新delta，
見`margin-rebuild-policy-binding-v8.json`。由於來源規則身分改變，沒有繞過舊快取的
身分檢查；先用canonical完整財務重建做驗收，不宣稱此部分已做到計算上限。

09:02 UTC啟動本機受監督的重建，run ID為
`source-bound-margin-rebuild-v8-20261025T090228-6de71a71`，資料目標為
`data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v8/`。
本機來源55,852檔完整，先驗證SHA，再依序完成帳務、篩選、發布及逐碼驗收；
程序保留8GiB記憶體及磁碟餘裕。遠端當時原始cgroup餘裕約26GB，
不足新完整編譯的入場預算，原有遠端訓練未停止。
這9,481日是保證金來源層恢復，尚不能直接報為85,920個未發布正成交日全部解除。

09:02 UTC遠端時鐘快照已核對3,550／3,833個日期；新八組仍在CPU資料準備，
尚未開始GPU工程驗收。後續先核對與原有限例外相同的來源缺口，才進入既有
dated-membership serial GPU排程；不因時鐘程序仍在執行而宣稱完整歷史已准入。

## 完整時鐘的失敗修復與新來源補查

v6在09:08 UTC完成3,833個來源日、1,771.658秒的全來源讀取後，
遇到精確K棒補查的空集合被建成Null型別篩選條件而停止。
這是軟體錯誤，原始例外與所有來源覆核結果保留，不能當作來源不可得。
v9改為具名日期欄位的anti-join，覆蓋空／非空日期與缺口組合；
另將完整來源縮減寫成綁定來源SHA、窗口、鉅額資料及實作身分的檢查點。
重啟仍逐檔核對原始來源SHA，只有完全相同的輸入才能重用縮減，
後續精確成交／K棒補充仍重新查核，不以快取略過新證據。
v9凍結來源SHA為
`b5675f10c6bbc2841c188e5979773a5b27faa8299f7c6bb0a8222e38f18ad56a`，
通過408項機制測試，68.44秒；23項膠囊外的歷史測試不計為通過。

完整讀取另顯示12個需修復的分時契約日，其中11個原有，新增2016-01-28
NZF遠月201606的一口官方成交日。以既有canonical下載器再查六個商品日，
供應商額度允許、六次請求完成，但沒有一日同時通過完整成交量與高低收核對：
NZF／OAF的2016-01-28回覆仍為空；CTF的2012-06-04少一口、
MTX的2013-12-25多11口；OAF／OKF的2017-05-16含未能逐筆辨認的鉅額量。
不能任意扣除幾筆成交湊成官方成交量。
原始補查資料在`data_finmind/futures_intraday_repair_20261025_v1/`，
逐日比較為`finite-intraday-source-comparison-v9.json`。

這12個契約日逐日列為待修復、不新下單，保留原持倉估值與其他已驗證日期；
沒有排除整個商品，沒有把未知改成零成交，也沒有聲稱已查遍所有供應商。
新12日覆核收據另建，原11日收據保留；之後出現的新缺口仍拒絕，
不會自動擴大例外。09:24 UTC v9準備身分檢查通過，既有Supervisor已重新啟動，
正在完整時鐘來源覆核；新八組尚未開始GPU訓練。

v8新保證金來源的完整帳務重建已於09:21 UTC完成，帳務階段1,178.768秒、
peak RSS65,587,195,904bytes、exit 0；正在以驗證後帳務進行篩選與發布。
這是新規則的完整重建時間，不能與先前差量帳務時間當作相同工作比較。
後續仍須完成逐碼及正成交日期增量驗收，才可宣稱新資料已被訓練採用。

## v8完整發布、逐日增量與待修復範圍

09:32 UTC v8四階段均成功，並完成原始725清冊查核。
帳務1,178.768秒、篩選373.530秒、發布222.792秒、清冊查核4.735秒。
受監督命令由啟動至回收的完整時間為1,830.419秒，包含本次四階段及其讀寫／查核；
不含先前來源修復、跨主機傳輸、分鐘準備或GPU訓練。
帳務peak RSS65,587,195,904bytes，篩選39,407,190,016bytes；
發布程序監控peak RSS27,762,024,448bytes，程序自身採樣27,789,750,272bytes。
保留兩種採樣口徑，不將任一階段時間當成完整訓練速度。

獨立先從遠端已接受v5匯出85,920個缺少的精確日期／商品／月份鍵，
核對原始清冊、官方日行情及v5發布SHA，才以同一分母與新發布做差集：

| 完整正成交契約日，2011-01-26至2026-09-24 | v5 | v8 |
| --- | ---: | ---: |
| 官方有完整OHLC及正成交量 | 1,766,347 | 1,766,347 |
| 已發布且日行情可執行 | 1,680,427 | 1,704,296 |
| 未發布 | 85,920 | 62,051 |
| 本次新增准入 | — | 23,869 |
| 原已准入日消失 | — | 0 |

增量為MTX12,251日、TX11,618日；9,481個保證金來源缺值座標修復，
透過完整持倉組件接續解除更多日期阻擋。兩者分母不同。
證據為`positive-day-before-export-v5.json`、`positive-day-recovery-v8.json`及
`financial_research_equity_v8-source-inventory.{json,csv,parquet}`。
每碼至少一天日級准入仍不等於08:45／09:00每個窗口有足夠整口成交量。

62,051個未發布日進一步分成21,245個首次觀測上下文日，以及40,806個非warmup日。
前者屬現行帳務／入場規則，不應當作供應商缺成交資料；改為首日可下單需要另驗證
起始帳戶、開盤估值與容量，不能直接清除warmup旗標。
後者中19,752日有直接金融來源阻擋，21,054日自身無直接阻擋，仍被持倉連通組件
其他日期缺口阻擋。保證金、規格、終端值等旗標互相重疊，不相加當作獨立缺日。
分類證據為`positive-day-remainder-classification-v8.json`，有限來源工作表為
`data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/positive_day_recovery/nonwarmup_source_worklist_v8.parquet`。

已確認其中971個非warmup規格缺口位於國外成分ETF期貨2026-07-06新制之後，
舊來源規格在該日截止。[期交所1150001759號函](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/台期交字第1150001759號函.pdf)
明示由單一漲跌幅改為三階段。這屬尚待補齊的規則／執行支援，不能歸咎於資料不存在，
也不能把舊15%單一區間直接延長冒充新制已實作。保留在同一待修復工作範圍。

v8資料位於
`data_tw_index_futures/training_prepared/tw_futures_universe725_20261025_v2/financial_research_equity_v8/`。
訓練可見發布與驗收清冊共56,922檔、6,393,387,872bytes；
遠端依內容SHA重用56,905檔、6,058,204,336bytes，只有17檔335,183,536bytes需要WAN。
不同邏輯路徑的既有來源亦按SHA查找並複製，不建立可互相改寫的硬連結。
09:55 UTC `financial-v8-independent-readback.json`已通過：遠端全量56,922檔、
6,393,387,872bytes及本機所有成員SHA均相符，plan SHA為
`50cefd89a1f48ddcb3acd705ca5abe9467a6cd9c071f5d1235b73d4a18d843e8`。
單路WAN先遇到實測TCP重傳／cwnd=2，保留部分檔後改為最多兩路、互斥檔案差量；
成功重試的傳輸248.050秒、完整傳輸及兩端驗收280.143秒，不能省略先前失敗重試
而稱為本輪全部交付耗時。原失敗收據保留並由成功run登記superseded。
未建立硬連結，沒有停止其他工作。這是發布位元組交付，不代表GPU已採用v8。

## v9完整時鐘及後續有限修復

v9完整3,833個來源日讀取為1,788.597秒，沒有再發生Null predicate錯誤。
10:03 UTC 30分鐘與300分鐘、08:45及09:00四組時鐘均完成，完整查核
2,776,363個帳務契約日，其中1,681,141個分時來源契約日經驗證。
12個已覆核的矛盾契約日保持不下單；不存在新增自動例外。
`source_coverage_complete=false`保留，不能宣稱全部分時來源都完整。

完整讀取的實測程序樹peak RSS為12,883,607,552bytes；原固定最低預算16GiB
在仍有約25.6GB raw headroom時造成等待。依該固定來源測量加25%入場餘裕，
將這個時鐘階段的最低預算改為15GiB，8GiB raw reserve和程序樹超限停止均保留。
來源身分綁定的checkpoint重用仍完整核對原始檔SHA，不重複解碼3,833天；
帶12日例外的完整時鐘發布peak RSS為9,291,755,520bytes。
證據為`clock-capacity-amendment-v10.json`與遠端相同準備目錄內的
`source-exception-readback.json`及各階段memory收據。
後續CPU metadata／membership階段亦以可用預算受監督實測，保留相同硬停止與reserve；
沒有以固定記憶體門檻冒充硬體限制，尚未宣稱GPU效率或訓練完成。

有限來源工作表實際含19,752個直接受阻非warmup鍵；另21,054個連通組件受阻日
留在差集清冊，不混入直接缺來源數。v10沿原修復入口再解析1,252個保證金事實，
209.585秒，修正GTF／PTF／QJF／TE／TF／ULF／XIF七個商品。
獨立在17,934個商品日（含這些商品所有v8已准入日期及受阻正成交鍵）比對，
補回43個商品日、98個正成交契約日的保證金來源，缺值回歸0、既有數值差異0。
這是來源層恢復，尚未完整帳務發布，不加到23,869個已發布新增日。
見`nonwarmup-margin-independent-source-readback-v10.json`。

剩餘鏈條另確認2017春節原始掃描公告只被解析到部分商品；已逐頁檢視
原件第1、7、8頁，為TE、TF、T5F、GTF、XIF各自轉錄六個保證金數值，
不依TX比例推算。新覆核檔位於
`data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3/nonwarmup_native_review_v11/`，
v11透過同一修復入口受監督執行，保持v10候選及原始SHA。
v11已完成20,134個商品日的獨立對照：新增574個數值完整商品日、
1,351個正成交契約日保證金來源，缺值回歸0、既有數值差異0。
這1,351日包含v10的98日；仍為未發布候選，不加到v8的23,869日。

## 10:35 UTC修復與排程驗收

八組第一次canonical membership於10:10 UTC拒絕缺少研究估值opt-in的設定，
沒有開始GPU訓練。新v13設定明示`frozen_contract_value_research_v1`，
凍結程式碼與v9相同，408項機制測試通過、23項既有設定fixture測試不在膠囊範圍內。
3,263檔、93,254,703bytes設定膠囊已逐檔遠端回讀。
舊排程metadata與原本的rendered-config receipt均移入具名amendment目錄保存，
不繞過canonical設定身分檢查，不以新設定續跑舊checkpoint。

另定位config-only新膠囊的絕對程式路徑改變，使相同計算快取未命中。
只對兩端程式SHA完全相同的v9/v13作明示路徑重綁，逐份縮減結果SHA保持相同，
不刪舊快取；canonical讀回仍核對所有原始來源SHA與stat fence。
10:32 UTC已確認`bulk_checkpoint_reused`覆蓋3,833天；四份時鐘再次完成。
證據為`clock-path-only-rebind-v13.json`及`valuation-optin-amendment-v13.json`。
這是資料準備階段修復，尚未提供完整epoch/fold加速或策略收益結論。

v12將v11中的六個不重疊保證金修復，與v8已接受的原24商品修復及TX/MTX修復
透過原合併器整合。PTF/ULF在已接受修復中已有相同規則，不能把來源候選1,351日
直接當作v12淨增量。獨立整合檢查確認公司行動、單位、部位規則均不變，
只有GTF/QJF/T5F/TE/TF/XIF保證金輸出變動。
初次完整建置在帳務前因研究政策相對依賴未一同重綁而停止；已修正三份葉節點，
以canonical閉包檢查通過，保留失敗收據與僅含requested-universe的失敗目錄。
10:29 UTC重啟完整四階段；最終逐正成交鍵驗算另由受監督run接續，
輸出`financial_research_equity_v12/`，尚未發布、未送遠端、未被GPU採用。

v14另重用三份原有掃描PDF：2015春節、2016春節與2016年OOF上市保證金。
OOF原件發文2016-11-23、保存公告目錄日期2016-11-24，沿較晚日期作已知時鐘，
不回填成更早的可知資訊。逐格檢視其9,000/7,000/6,000元初始／維持／結算金額，
其他商品各自轉錄完整原表；2021與2025維持原額公告則只連結各自先前來源金額。
原件／頁面／覆核位於`nonwarmup_native_review_v14/`，沒有新增下載或價格推估。
v14來源修復236.707秒，獨立59,977商品日對照恢復1,858個數值完整商品日、
4,008個正成交契約日的保證金來源，缺值回歸0、既有數值差異0。
4,008日包含v11候選；尚不是完整財務發布或新增實際成交。
見`nonwarmup-margin-independent-source-readback-v14.json`。

來源證據：`unknown-volume-tick-audit.{json,csv,parquet}`、
`recovered-trade-opportunities.{json,csv,parquet}`與
`retained-provider-history-audit.json`均在上述repair operations目錄；
`source-audit-independent-readback.json`保存前兩組六個檔案的遠端前後與本機相同SHA，
合計173,575bytes，沒有為報表複製原始逐筆payload。
