# 全臺幣期貨保證金模式準備狀態

更新：2026-10-02。最新逐座標修復與測試見
[會計缺口修復計畫](tw_futures_accounting_gap_plan_2026-09-30.md)；下方保留 9 月 30 日
的歷史交付與驗收結果，不能當成今天新版本的遠端驗收。
**全 765 個商品仍未完成；本次使用者已授權先排除 54 個非股票代碼，
再以其餘商品中規則及持倉接續完整的契約期間準備獨立訓練版本。**
初始資金一億元、保證金槓桿、多空與跨日留倉不變。
有償付能力但無法成交的持倉保留，不能僅因當日未沖銷而虛構違約、罰款或成交。

目前依使用者最新指示，優先使用歷史 JSON／CSV／HTML、原生文字及既有
FinMind 運算元，只有必要欄位無等價來源時才用公告圖像。只驗收受影響商品的
增量，全部修復完成後再做一次完整帳本檢查。截至 v811 的局部快取含
**253 個代碼、1,939,507 列來源、1,918,308 列會計日、23,657 列受阻**，
不是全 711 個代碼的最新總數。以 v205 快取及後續新增代碼在原始 v47 的
對應列比較，相同有效座標解除 9,975 列、新揭露 1,452 列。新增代碼皆有
凍結原始來源的座標證明；121／142 列暖身與會計身分轉換另列，既有座標
零遺失，不加總重疊批次的改善。

已修跨頁原生表格、Word 財務／部位續列、同代碼月份／世代的部位來源綁定、
全型／小型契約各自的單位換算及兩代碼合併規則。潤泰新新增四個有實際
運算元的終端事件，原 122 筆保留；日月光 2017 年直接採用官方完整結算
金額，未提供的除息入帳欄位保持空值。2011 年沒有最新級數條款的原件
不套用後來的語意。新增中信金 2010 原生多目的商品拆分，以及被收購公司
原件持有的元大金 2016 合併條款；現金給付與權益加減項分開核對。
新增中信金 2013／2015、玉山金 2017、元大金 2015 的原件條款；已固定與
待定認購利益分開計算，四筆失效的 DN1 到期索引完整保留於隔離表。
HS1 缺歷史上限適用範圍的原型已撤回，其改善數未計入。
另修裕隆新世代被舊股數換算覆蓋、恢復遺漏的彰銀已驗證來源分支，以及
台中銀 `2.000` 誤辨；四組有自身單位證據的商品接入原件合併規則。
另核對中信金、啟碁、華南金、喬山的原件欄位與月份；喬山第二期間的年份
錯字沒有擅改，揭露的 880 列當期上限缺口仍保留。修正玉山金／兆豐金的
留存同股數來源接續，以及台新金／永豐金／長榮航／合庫金的三商品群組。
較早一批只重算 14 個相依代碼，10.80 秒完成；與修改前快取比較解除 **607 列、
零新增受阻**，全部財務 facts、報價、量與執行允許欄位保留。
後續修正 DN／HW／LO 各自的股數來源及原件明示群組，六份財務原件的數量、
現金與權益欄位，以及南紡／遠百／欣銓／台中銀的月份與部位表。台中銀
2011 年換代改以完整原件證明固定約束的承接，保留原來的公告引用證明與
所有原公告數字。2015 年 HW2 的原件沒有印出自身新上限；其 52 列既有
約束接續仍待修，沒有填入 2011 年股數或自行增列三商品合併。
再接入十份完整財務原件及五筆認購利益：原始 MOPS／TWSE 與 FinMind
交叉核對，局部快取的公司行動及調整條款缺口已為零，沒有填造零利益。
長榮 2019 年部位表的 10 月 16 日重疊保留為歧義；共用解析器只准入
其後已確定的上限。重算三個代碼解除 809 列，並揭露該日 9 列受阻。
這批再解除 89 列、零新增受阻，局部快取保證金缺口為零；CPF 早期保證金
仍待補，不能把局部零缺口稱為全部商品完成。修復涵蓋南帝／穩懋原件時段、
現金到期事件與收盤後法律時鐘、聯電／東聯停市及同邊界恢復、調整契約的
同日候選承接。台虹原件發文日與索引公開日分開保留，不提前 `known_at`。
最新再驗證大成鋼、永豐金、台中銀原件的新世代 2,000 股及自身 15 個月份，
修正較早原件遮住已證明合併規則的接合問題。只重算九個相依代碼，解除
**486 列、零新增受阻**，保留其他股數與原公告上限；大成鋼舊快取另接回
先前已驗收的 13:30 現金到期時鐘修正，沒有替換來源保證金。
[原件與逐欄驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/unchanged_member_replay_acceptance_v808.json)。
本批十個受影響模組的共用回歸 **636 passed、2 skipped**；本輪來源修復
沒有新增 OCR、影像轉檔、全部帳本重建或遠端訓練。

QM1／CJ1 的單日分鐘回應已完成，但沒有出現所需的持倉月份，尚未用於
會計准入。CR1 已驗證 29 列分鐘資料及自身四個月份，仍缺 201306，首根
09:17 不能用來補 08:45。TEJ Pro Smart Wizard 原生 Preview 已完成 CR1
調整表一列九欄的可驗證匯出；CJ1 兩個缺失月份的行情查詢由供應商明示空。
TEJ 公告時點、欄位單位及全歷史完整性仍未證明，未用於會計准入。
新的 HW 查詢註冊前遇到既有未知結果鎖，沒有重送或清除其他工作者的狀態。
後續 TEJ 原生調整表再取得五列並與原件核對；這個帳號可見的六張期貨相關
表未找到歷史保證金／部位上限欄位。最新佇列證據顯示舊未知任務已完成；
三個精確日期範圍已註冊。CN2 先因前景不可用、後因精確來源選項缺失，
三次都在 Preview 前停止；不將本機選取失敗視為供應商缺資料。KI1 的三個
指定遠月已收到供應商明示空回覆；CJ1 的有限嘗試受共用佇列防護阻擋，
最新唯讀快照確認另一張 TDR 表的下載結果未知，原始回應檔未留存；保留
`unknown_outcome_no_auto_retry`，沒有重送或清除狀態。三筆優先序均為 100，
沒有新行情准入，不重複已完成的空查詢。
[TEJ 選取障礙及 KI1 空回覆](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_transfer_gap_runtime_boundary_v712.json)、
[CJ1 優先序恢復](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_cj_far_month_finite_priority_v748.json)、
[最新 TEJ 障礙快照](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_remaining_scope_runtime_boundary_v796.json)。
FinMind 三筆有限分鐘查詢已插隊完成，優先序及服務已恢復；只回應近月，
沒有補到缺失遠月，未用於會計准入。

**受阻尚未歸零，遠端原 437 商品 release 未變更。** 剩餘仍包含當期部位
法律／時鐘、月份股數與真實承接行情，以及 CPF 的早期法律與保證金變更鏈。
局部測試通過不能當成全商品訓練完成。
[完整修復計畫及根因](tw_futures_accounting_gap_plan_2026-09-30.md)、
[最新總收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/repair_batch_progress_v811.json)、
[逐商品缺口](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/affected_gap_worklist_v810/products.csv)。

## 2026-09-30 修復：規格 v5、來源 v79、會計條款 v18

全範圍受阻會計列由 **712,907 降至 333,640，淨減 379,267 列（53.2%）**。
保留 765 個歷史代碼及原始行情 SHA；240 個代碼改善，69 個代碼的受阻列
較基準增加，完整差異仍保留，不能稱為全商品改善或已達零缺口。
[實際資料驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/acceptance.json)、
[逐商品差異](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/final_product_delta.csv)。
實體日曆由 4,968,246 改為 4,968,156 列，原因是原始公司行動公告修正了契約
承接關係；生命週期及首次觀測暖身也隨之重建。這是受阻列的淨變動，並非所有
比較列都有相同的生命週期身分。沒有刪除原始報價或填造估值。
逐日期／商品／契約比對另證明：相同會計座標有 385,574 列解除阻擋、6,284 列
新增阻擋，日曆及暖身差異合計抵銷 23 列淨改善。
[逐列轉換驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/row_transition_audit.json)。

第一性原理是逐日證明「一口值多少、何時生效、需要多少保證金與可持有多少、
舊持倉與到期利益如何接續」。資料取得、候選數值解析、時間准入及 GPU 訓練
各有自己的驗收；其中一項通過不能替其他項目宣告完成。

- 以原始上市公告及交易規則接入 5 個指數、28 個 ETF 商品的日期化規格，
  包括漲跌幅、交易時間與小型契約單位。規格候選涵蓋 755 個代碼；10 個完全
  未接規格，另有早期／新制期間仍受阻，不能用目前官網規格回填所有歷史。
- 完成 718 份本輪選定公告的文字／表格提取，依原頁核對 21 組公司行動數值；
  個別 OCR 批次的失敗與未處理項目保留。接入櫃買 2010～2026 年 3,887 筆
  官方處置原文，修正多商品公告借錯恢復基準、缺失發文頁與部位上限第二段期間。
- 智原原公告明載遇休市則恢復日順延。證交所原件證明 2023-08-03 全日休市；
  十二個實際股票成交日於 2023-08-10 完成，因此保證金恢復由 8 月 9 日順延
  至 8 月 10 日收盤。盤前仍採提高後金額，不能提前採恢復值；缺行情、其他
  處置延長及中途另改保證金的反例仍保留阻擋。
  [證交所休市原件](https://investoredu.twse.com.tw/FileSystem/FileUpload/be0109d1-76a0-4412-a00d-58a88c6e1714.pdf)、
  [期交所原公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/20230725智原保證金(公告).pdf)。

167 項 parser／binding／execution terms／release／scope 測試通過。
資料已傳至遠端 `/root/stockAgent`，**55,009 個檔案 SHA 校驗通過，六個資料包
的遠端重算與本機逐欄一致**。
[測試結果](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/delivery_regression_tests.log)、
[交付收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/remote_delivery_receipt.json)、
[遠端重算](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/remote_acceptance.json)。
另以真正輸出的 `rules.parquet` 驗證智原五個月份、三個日期，共 15 筆可執行
條款：8 月 10 日開盤原始保證金率 30.38%，收盤才恢復 20.25%；8 月 11 日
開盤為 20.25%。
[執行日期邊界驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/execution_boundary_acceptance.json)。
遠端使用獨立程式快照驗證候選資料，原有訓練程式及 437 商品 release 未變更；
本次唯讀檢查時原正式訓練已進入 fold7。新包尚未接入正式訓練或 GPU smoke。

| 剩餘互斥原因 | 受阻會計列 | 受影響代碼數 | 必須補齊的內容 |
| --- | ---: | ---: | --- |
| 日期化商品規格 | 162,150 | 31 | 10 個未接商品、早期 T+1 結算及分段漲跌幅等來源與執行支援 |
| 調整契約條款 | 27,925 | 116 | 月份、獨立標的數量、現金及增資權利的原件數值 |
| 保證金規則 | 93,595 | 105 | 缺失歷史金額、生效時刻、完整恢復條件與中間轉換 |
| 部位限制 | 41,959 | 316 | 遺漏期間、公告時間及互相衝突的上限／合併規則 |
| 持倉接續 | 1,846 | 325 | 來源身分、轉換及終端承接關係 |
| 估值與其他 | 6,165 | 107 | 真實到期／結算價格與其他會計欄位 |
| **合計** | **333,640** | — | **全商品仍未可訓練** |

[缺口統計](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/remaining_gap_counts.json)、
[10 個未接規格清單](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/remaining_10_no_specification.csv)、
[逐商品工作表](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_execution_terms_v18_20260930/rule_worklist.csv)。
另有 39 段實體生命週期、2,538 列估值及 166 筆調整終端事件的輸入未完整；
它們與上表重疊，不可加總。既有 FinMind 日資料對 634 個估值缺口日期的比對
只找到 65 筆匹配、零筆有效正結算價；重抓相同資料並不能解決這批缺口。

既有 FinMind 分鐘插隊持續執行。本次單次唯讀快照為 24,750 個成功任務、
6,692,817 列；12,735 個空回應、1 個失敗及 94,950 個待抓任務。
[排程快照](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/finmind_priority_snapshot.json)
只證明當時的取得進度，不能證明分鐘歷史完整或已可用於會計准入。
本輪新增休市／處置原件直接來自官方來源，未重複消耗 FinMind 查詢額度。

[最新準備收據 v13](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_products_readiness_v13_20260930.json)
及 active pointer 均保留 `all_products_training_ready=false`。
[重算入口](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v2_20260930/rebuild.sh)
沿用共用 CLI；資料編譯仍回報全範圍阻擋。獨立 validator 的 `status=passed`
只表示資料、差異及指定邊界檢查通過，不能轉解成全商品訓練可用。
下面保留 437 商品的已驗證指令及先前階段紀錄。

## 前一輪修復：規格 v3、來源 v64、會計條款 v9（2026-09-30）

本輪維持 **765 個代碼、4,968,246 列來源**，受阻會計列由 **847,659 降至
712,907**，修復 **134,752 列（15.9%）**；87 個商品改善，沒有商品的受阻列增加。
本機前後行情 Parquet 的 SHA 相同。沒有以刪除商品、日期或補造報價降低缺口。
[實際資料驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v1_20260930/acceptance.json)、
[逐商品差異](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v1_20260930/final_product_delta.csv)。

第一性原理的拆解是：先確定每口契約代表多少價值，再確定當時何時可用哪些
保證金與部位規則，最後驗證舊持倉如何轉為新契約、現金或到期價值。
更多行情不能補出缺少的法律條款，模型或槓桿參數也無法修復錯誤的會計基礎。

- **規格接合**：從既有期交所原始規章與上市公告接入 BTF、E4F、G2F、GTF、M1F、
  SHF、SOF、TMF、XIF、ZEF、ZFF 共 11 個代碼，規格候選由 711 增至 722 個。
  分開處理漲跌幅變更、颱風延後上市及 XIF 契約縮小；持倉數量轉換只發生一次。
  這一階段消除 96,946 列阻擋；GTF／XIF 的 2008-11-21 以前結算時鐘仍待接合，
  G2F／E4F／BTF 的調降上限後既有持倉例外仍未完成來源審閱，不能視為已准入。
- **保證金恢復日**：FinMind 部分處置內容只有「第一次處置」摘要，無法證明處置
  經過了規定的完整營業日。使用既有官方下載器取得 8 個年度查詢的 1,586 筆
  TWSE 處置原文，與原資料精確接合，保留未匹配來源及延長處置記錄。
  原有條件檢查新增驗證 **69 個商品、139 個恢復事件**；行情須實際涵蓋所有
  要求的交易日，仍不以日曆推測復原日。此階段再消除 37,806 列阻擋。
- **時間及持倉驗證**：通過 89 項 margin binding／execution terms／release／scope
  測試，以及 12 組實際規格日期邊界、公告時間延後反例、5 組 QWF 盤前／盤後
  保證金值、6 段 XIF 契約的一次性數量承接。QWF 恢復日盤前仍使用舊保證金，
  盤後才採新值；沒有把盤後資訊提前給開盤。

新候選包已傳至遠端 `/root/stockAgent`，**51,457 個檔案 SHA 校驗通過**，
遠端實際資料驗收與本機結果一致；其中 v64 候選包包含 51,297 份來源記錄。
[交付收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v1_20260930/remote_delivery_receipt.json)、
[遠端驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v1_20260930/remote_acceptance.json)。
保留遠端原有資料及訓練程式；本機與先前遠端獨立建置的父版本會有序列化 SHA
差異，因此修復比較攜帶其原本基準收據，沒有覆寫正在使用的父版本來配合檢查。

完整生命週期的會計篩選由 437 增至 **438 個候選代碼**，新增 QWF；合格會計列
由 2,473,978 增至 2,495,539，契約生命週期由 32,108 增至 32,422。
[範圍診斷](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v1_20260930/updated_stock_scope_diagnostic.json)
尚不等於來源准入或 GPU 訓練通過。**既有 437 商品的 release、啟動器與正在執行的
當輪訓練不受該批變更影響；沒有把候選資料接進舊 checkpoint。**

剩餘缺口如下。每列依順序只歸入第一個原因，列數可以相加；同一商品可出現在
多個原因中，商品數不可相加。補掉上游原因後，下游原因可能變得可見，不代表退步。

| 尚待接合原因 | 互斥受阻會計列 | 受影響代碼數 | 下一個需要的證據或修正 |
| --- | ---: | ---: | --- |
| 日期化商品規格 | 433,708 | 45 | 43 個完全未接入代碼，加上 GTF／XIF 早期結算時鐘 |
| 調整契約條款 | 61,421 | 150 | 月份別標的物、現金及增資權利原始公告 |
| 保證金規則 | 77,585 | 91 | 歷史額度、生效時刻及其餘恢復條件證據 |
| 部位限制 | 127,728 | 305 | 歷史上限、合併計算及調降例外；GG1 仍缺原件數值 |
| 持倉接續 | 1,298 | 295 | 舊契約轉換到新契約的完整承接關係 |
| 估值與其他 | 11,167 | 111 | 到期、終端價值與實際持倉估值證據 |
| **合計** | **712,907** | — | 全商品仍為 blocked |

[缺口統計](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v1_20260930/remaining_gap_counts.json)、
[43 個未接規格代碼](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v1_20260930/remaining_43_no_specification.csv)、
[逐商品／期間工作表](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_execution_terms_v9_20260930/rule_worklist.csv)。
另有 300 筆調整終端價值、75 段生命週期及 2,628 列估值待完成，這些與表中原因
重疊，不可再加到合計。GBF 實物交割、BRF 海外結算時鐘等商品語義也尚未准入。

目前修復鏈為 `dated_product_specifications_v3 → all_products_rule_facts_native_v64 →
all_twd_margin_inputs_v25 → all_twd_execution_materialization_v8 → all_twd_execution_terms_v9`。
[最新全範圍準備收據](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_products_readiness_v12_20260930.json)
仍明確記錄 `all_products_training_ready=false`。
[重建入口](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v1_20260930/rebuild.sh)
沿用共用 CLI；它是資料重建指令，全商品尚有阻擋時退出碼為 2。
下面保留已可訓練的 437 商品指令及前一輪修復紀錄，不能把新的候選數量代入舊指令。

## 先行訓練範圍與分鐘補抓（2026-09-30 新授權）

本次保留原全商品來源及 blocked 收據，另建立
[`scoped_stock_margin_20260930`](../artifacts/markets/tw_futures_v8_margin_preparation/scoped_stock_margin_20260930/summary.json)。
這份 release 已通過資料／會計驗證並傳至遠端，雙 RTX 5090 的 fold10
三輪訓練及共用產物驗收已通過；已成功恢復 optimizer／scheduler／RNG，
接續第 4 輪，原前三輪曲線未改寫也未重複。
[三輪 GPU 驗收](../artifacts/markets/tw_futures_v8_margin_preparation/scoped_stock_margin_20260930/runtime_acceptance_3epochs.json)
與[續跑驗收](../artifacts/markets/tw_futures_v8_margin_preparation/scoped_stock_margin_20260930/runtime_acceptance_4epochs.json)
皆通過訓練、驗證、測試、圖表與 18 個必要產物檢查；119 項 checkpoint／lifecycle
測試另通過。這是可訓練性驗收，不是正式千輪結果或獲利驗證。

- 實際候選商品為 **437 個標準／小型股票期貨代碼**，32,108 段實體契約生命週期。
  來源 2,506,086 列；扣除 32,108 列首次觀測暖身，會計資料為 **2,473,978 列**。
  來源期間 2010-11-18～2026-09-04；walk-forward 仍由 2011 年起分割。
- 原 54 個排除之外，另有 274 個歷史代碼未進入先行版：272 個股票調整代碼、
  QWF 與 CPF。這是使用者同意的可驗證範圍，不代表其餘 711 個全部已可訓練。
  [納入清單](../artifacts/markets/tw_futures_v8_margin_preparation/scoped_stock_margin_20260930/included_products.csv)、
  [全部暫緩代碼](../artifacts/markets/tw_futures_v8_margin_preparation/scoped_stock_margin_20260930/not_in_training_products.csv)、
  [逐生命週期選取原因](../artifacts/markets/tw_futures_v8_margin_preparation/scoped_stock_margin_20260930/lifetime_scope.csv)。
- 以完整生命週期為單位，排除中途缺規則、跨代碼承接、現金／數量調整及普通
  報價價值恆等式不成立的情況，不只刪掉某個缺值日。訓練期間終點僅按市價估值，
  不虛構平倉。範圍是事後來源品質篩選，可能有樣本選擇偏差，不能稱作當時已知的市場全集。
- 沿用遠端 v17 的 v5 衍生模型、共用 margin 帳本、BF16、雙 GPU DDP；每 fold 從頭初始化。
  為保留 2,816 槽及完整股票背景的顯存餘裕，先把面板留在 host。
  比較基準沿用 dated-margin 模式的零利息台幣現金；不要求 TX 重新進入可交易商品。
- 這次是**日頻保證金 carry 的研究資料**，仍使用官方日 OPEN 成交代理及日結算估值；
  尚不是把新增 FinMind 分鐘資料接成可執行分鐘帳本，也未驗證夜盤／盤中追繳。

本輪 fold10 第一次完整反向傳播揭露梯度問題：schema-5 多空拆分在零持倉
同時使用兩個 `clamp_min`，把本應為 1 的局部導數變為 2，經逐日承接放大。
遠端診斷觀測到有限但高達約 `2.21e30` 的模型梯度，導致 FP32 L2 裁切溢位。
修正零點兩側各取半斜率，合回淨持倉後導數為 1；整口整數分支、費用及前向
報酬不變。訓練梯度契約由 10 升至 11，禁止接續舊梯度契約的 checkpoint。
修正後另用 `tw_futures_v8_margin_scoped_stock_fold10_smoke3_v11_20260930`
執行驗收，保留原失敗紀錄。220 項帳本及保證金測試通過；128 天零持倉
承接的導數維持 1。GPU 三輪皆有一次完整軌跡 optimizer 更新，第二、三輪
新增編譯圖為 0；第 3 輪 `epoch_max_rank_s` 為 12.943 秒，含後續報表工作的
profile 最大 rank 總時間為 13.445 秒。三輪裁切前梯度範數分別為 7.764、7.261、6.803。

遠端 `/root/stockAgent` 已備妥自含設定與啟動器，沿用 1 億元、保證金、多空及跨日。
先跑 fold10（最多 1,000 輪，早停依原設定）：

```bash
cd /root/stockAgent
bash artifacts/markets/tw_futures_v8_margin_preparation/run_scoped_stock_margin_20260930.sh fold10
```

跑全部 walk-forward folds 時把最後的 `fold10` 改成 `train`；
兩者共用正式產物根目錄 `artifacts/markets/tw_futures_v8_margin_scoped_stock_capital100m_20260930`，
可續跑且跳過已完成 fold。各 fold 仍只從自己的訓練年份學習，437 是整份資料的
商品代碼範圍，不代表較晚上市商品會被放進較早的訓練年份。

分鐘補抓已使用原 Complement worker、共享限流器及原始 Parquet／收據路徑啟動。
[`request.json`](../artifacts/markets/tw_futures_v8_margin_preparation/finmind_minute_priority_20260930/request.json)
列出 54 個代碼、**132,436 個逐商品曆日查詢**，依現有來源起訖與 2011-01-03 API 下界取交集，
新日期優先。此範圍定義優先序，不是對更早資料不存在的判定；假日／夜盤不套用現貨週末排除。
原 task priority 不變，追新及官方配額預留仍有效；優先表沒有待處理工作時自動回原排程。
`observed_empty` 只表示查詢回空，不等於歷史完整。
2026-09-30 14:30 台北時間實查：2,067 個成功請求、555,357 筆分鐘資料，
1,153 個回空請求，129,216 個優先請求尚待抓取；服務仍在背景執行。
[觀測進度](../artifacts/markets/tw_futures_v8_margin_preparation/finmind_minute_priority_20260930/observed_progress.json)、
[實際收據與 Parquet 驗證](../artifacts/markets/tw_futures_v8_margin_preparation/finmind_minute_priority_20260930/receipt_verification.json)。

[FinMind 官方期貨分 K](https://finmind.github.io/tutor/TaiwanMarket/Derivative/#taiwanfutureskbar-sponsor)
標示自 2011-01-03 起，Sponsor 每次單商品單日，SponsorPro 另有整日全商品物件下載。
本機帳號核實為 Sponsor，因此沿用逐商品方式，沒有提升方案或繞過權限。
分鐘行情不能補回歷史規格；歷史保證金需依
[期交所歷史公告查詢說明](https://www.taifex.com.tw/cht/9/tradersQAClearing)
及已下載的契約修訂、上市與調整公告接合。

以下保留全 765 商品準備歷程及未完成項目，與先行版分開。

上一輪（會計條款 v7）先修復來源解析及商品計算方式。相同 765 個商品、4,968,246 列來源下，
需修復的會計列由 1,037,907 降至 **847,659**，減少 **190,248** 列；
每日市場資料的 SHA 完全相同，沒有藉由刪除商品或日期降低缺口。
原始資料齊備、條款可接合、帳本可執行及 GPU 訓練通過是四個不同條件。

以下分開記錄原始來源、數值整理、執行准入與實際訓練。
資料及修正已送至遠端 `/root/stockAgent/artifacts/markets/tw_futures_v8_margin_preparation/`；
數值整理成功不代表規則已全部具備當時可知性，也不代表模型已完成訓練。

## 全範圍資料（前一輪 v7 紀錄）

| 項目 | 結果 |
| --- | --- |
| 歷史代碼 | 765：491 標準代碼、274 調整代碼；不是目前上市商品數 |
| 類別 | 指數 23、股票 710、ETF 28、商品 2、利率 2 |
| 官方日／時段原始紀錄 | 5,108,470 筆；1998-07-21～2026-09-24 |
| 一般時段契約日 | 4,968,246 列；54,046 段實體契約候選生命週期 |
| 保證金數值接合 | 4,718,193 個契約日；765 個代碼皆有部分期間可接合 |
| 部位限制數值接合 | 764 個代碼有部分期間可接合；GG1 尚無；CM2 仍非全歷史完整 |
| 調整契約條款 | 274 個代碼皆有部分已解析期間；不代表所有月份或整段歷史完整 |
| 調整契約終端價值 | 2,310 筆待接合事件，2,010 筆已有數值，300 筆未解決 |
| 生命週期／估值缺口 | 75 段到期未解決，2,628 列估值未解決 |
| 遠端來源校驗 | 本次 v63 候選包 51,278 份來源及 14 份輸出通過 SHA 校驗；前次跨資料集稽核為 51,777 份，兩者不相加 |
| 槽位布局 | 2,816 槽；觀測需求 2,588 槽，資料 ABI 6 |

8 個歷史代碼沒有正成交量：CJ2、EB1、FC1、HW2、IQ2、JE2、JH1、LG1。
它們仍保留在商品清單及來源資料中；零成交紀錄不能產生可執行的買賣。

前一輪整理版本（最新版本見本文首節）：

- [完整商品清單](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_currency_scope_v3_20260929/products.csv)。
- [v63 候選來源與解析證據](../artifacts/markets/tw_futures_v8_margin_preparation/all_products_rule_facts_native_v63_20260930/manifest.json)。
- [v14 實體契約與估值](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_physical_history_v14_20260930/manifest.json)。
- [v24 逐商品數值覆蓋](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_margin_inputs_v24_20260930/product_accounting_input_coverage.csv)。
- [遠端全來源稽核](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_products_audit_v13_remote_20260930/manifest.json)。

## 前一輪已修正（會計條款 v7）

- 原始影像逐頁核對 2013 年部位限制表的 234 個期貨商品，補回漏讀的續頁；
  排除只有選擇權的 CX，保留 LW／LX／LY／LZ 的次日上市例外。
  後公告明確調高這四檔商品的上限，保留舊上市公告並綁定其被取代的來源，
  不使用「公告比較新」作為任意解決衝突的規則。
- 修正 2015 年漢微科 NFF 被 OCR 誤讀為 NEF，使另一商品的保證金歷史被錯誤截斷。
  修正 2014 年 13 檔部位限制表的調高／調降時點，調降保留 2014-03-20 生效，
  不提前到公告日。[原件影像與轉錄](../artifacts/markets/tw_futures_v8_margin_preparation/dated_priority_rules_review_20260930/positions.json)、
  [2014 年日期證據](../artifacts/markets/tw_futures_v8_margin_preparation/dated_priority_rules_review_20260930/position_2014.json)。
- 共用條款編譯器升至版本 **3**，支援固定點數漲跌幅與商品代碼不變的整批契約拆分。
  拆分必須守恆，僅在切換日承接一次；同日新掛月份不憑空取得舊持倉。
  這是計算能力與測試通過，TGF／XIF 的完整日期化來源准入仍未完成。
- 接入 CPF 原始上市 Word 規格：每 0.005 點為 411 元、每日上下各 0.5 點，
  一億元面額供計稅使用，與損益點值分開；沿用日期化稅率。
  [日期化規格 v2](../artifacts/markets/tw_futures_v8_margin_preparation/dated_product_specifications_v2_20260930/manifest.json)
  有 711 個代碼的候選規格，仍不是完整來源准入。
- 完成既有 CLI 的 `inputs → materialize → terms → release` 接點。
  `terms` 使用日期化商品規格、保證金、合併部位限制及公司行動，
  實際計算全商品 schema-6 每日會計條款，輸出逐列缺口及來源 SHA。
  **目前編譯結果仍為 blocked，尚未成功發布全商品執行 release。**
- 修正現金增資權利價值的會計時點：固定權利價值不加進每日契約估值；
  到期前不需要猜測未定權利價值，到期才使用已驗證的完整約定標的物價值。
  這符合[期交所結算計算方式](https://www.taifex.com.tw/cht/5/margingCal)。
  已加入「改動到期價值不影響較早日期」、保證金及計稅基礎分離的測試。
- 逐頁核對 KP1（2013）、CZ1（2019）、IQ1（2015）原始 PDF，補回 15 組月份條款；
  保留現金調整、增資權利及新掛標準契約月份的區別。
  [原件影像與轉錄](../artifacts/markets/tw_futures_v8_margin_preparation/dated_rights_carry_review_20260930/review.json)。
- 來源證據留在已核對的收據，讀取及接合時只展開需要計算的欄位；
  避免將重複的來源清單放大至近 500 萬列。來源 SHA 校驗仍執行。
- 每日資料使用 `physical_instance` 區分重用代碼，沿用共用固定槽位配置與特徵計算。
  週契約按日曆排序，不會因字串排序被排在更晚到期的月契約後。
- 官方資料列、實際成交價與持倉估值分開保存。1,232,599 列未填成交量、
  31 列有成交量但無 OPEN／CLOSE，保留來源缺值並禁止在該列成交；
  有當日結算價及前日結算價時仍可估值舊持倉。這不表示將缺值補為零成交量。
- 帳戶契約版本升為 **7**：缺少可執行報價時終端可成交量為零，
  不使用結算估值冒充平倉價；舊版本 optimizer checkpoint 不可直接續訓。
- 發布前檢查持倉承接圖，防止把新調整契約第一天當成無持倉暖身而刪掉；
  也檢查股票背景資料是否缺少任何要求的期貨日期。

本輪產物為 [v7 每日資料](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_execution_materialization_v7_20260930/manifest.json)、
[v7 會計條款](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_execution_terms_v7_20260930/manifest.json)
及[逐商品／原因工作清單](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_execution_terms_v7_20260930/rule_worklist.csv)。
共 4,968,246 列來源、4,915,506 列會計資料、52,740 列無舊部位的首筆暖身資料。
需要修復的會計列為 **847,659**；範圍仍為全部 765 個歷史代碼。
這不是交易准入率，也沒有將缺失列刪除。

前次相同輸入的 `terms` 階段，本機修正前／後為 109.71／87.97 秒，遠端為
66.72／59.72 秒。欄位精簡後再做讀檔投影，本機峰值記憶體從 28.1 降到 25.9 GiB；
遠端最終為 24.6 GiB。這是共用主機上的單次建置測量，非隔離效能基準，
不代表已達運算極限，也不是訓練 epoch／fold 測速。
[本機資源紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v2_20260930/local_preparation_performance.json)、
[遠端資源紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v2_20260930/remote_io_projected_time.json)。

先前來源修復仍保留：

- 同商品不同交割月份分別核對股數；缺一個月份不會使其他月份失效，也不會借用其他月份的股數。
- 將契約股數、現金給付與認股權分開接合。可確認股數的資料不因現金尚缺而整筆消失，
  但只有股數的資料不能冒充完整交割條款。
- 修正部位合併時的單位繼承，拒絕把舊的 1:1 換算套到已改變股數的契約。
- 從原件補入 DX1、EL1、NR1、ML1、DN1／DN2、IQ1／IQ2 的指定月份、股數或部位上限。
  DX1 的上限是 5,251,730.25 股；小數股數上限不代表允許小數口交易。
- 綁定 2013 年公告的失效日期，阻止已撤銷的未來調整繼續生效。
- 更正先前人工核對記錄：`2887_20140219.pdf` 的 CM2 調整範圍只有 2014 年 3 月、6 月，
  不能多加 9 月。原始影像與更正保留在
  [CM2 更正證據](../artifacts/markets/tw_futures_v8_margin_preparation/dated_cm2014_correction_20260930/review.json)。
- 撤回未檢查合法掛牌狀態的同標的結算價推導，避免將未平倉量歸零後已終止掛牌的契約
  延伸到月份到期日。[撤回記錄](../artifacts/markets/tw_futures_v8_margin_preparation/rejected_derivations_20260930.md)。

原件、收據及舊版產物保留。來源整理沿用既有收集器、解析器與共用訓練帳本；
沒有以今天的規則回填歷史、插值成交價或把缺失保證金填成零。

## 尚未完成的條件

1. **CM2 尚未完整接合**：本次已補出 20 個商品日的部位限制，不能再說完全沒有；
   其餘日期仍須核對當期級數、契約世代與失效日期，不能僅由股數推算。
2. **GG1 原件有內容缺口**：`2374_20151117.pdf` 本機及已取得的官方版本只有兩頁，
   最後一頁的部位限制表中斷，無法讀出適用上限。現行限額不能代替 2015 年規則。
   [原公告附件](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/2374_20151117.pdf)。
3. **仍有日期／月份層級缺口**：上述 300 筆終端價值、75 段生命週期與 2,628 列估值，
   以及暫時保證金、合併部位級數及失效時點，尚未全部接齊。
4. **全商品可執行資料集仍未完成**：日期化交易時段、漲跌停與價格格、現金移轉、
   TGF／XIF 拆分、CPF 計稅與名目本金、GBF 實物交割、BRF 結算語義仍須完成准入。
   逐日條款編譯器已實作；日期化規格目前整理股票期貨共同規則及 CPF，
   其餘 54 個歷史非股票代碼及各別調整例外仍須補齊。
   現有資料還需要解讀與整合，不能把這些工作一概當作使用者缺資料。
5. **全商品實際訓練尚未執行**：train／validation／test、曲線、checkpoint、resume
   與完整 fold 測速都必須在同一份通過准入的資料集上驗收。

## 已完成的工程驗證

本次本機與遠端各 **321 passed、2 skipped**；遠端 CUDA 環境檢查通過。
兩端 4,915,506 列會計條款及 4,968,246 列缺口資料逐欄精確相同。
工作清單在筆數相同時的排序不同，以 `product, reason` 接合後數值完全一致；
Parquet 的序列化 SHA 也不同；此處驗證的是精確欄位值，沒有將位元組誤報為一致。
[本次本機測試](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v3_20260930/local_tests.json)、
[本次遠端測試](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v3_20260930/remote_tests.json)、
[兩端結果比對](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v3_20260930/v7_remote_comparison.json)。

此次遠端 `terms` 指令約 60.4 秒；本機約 92.5 秒，含啟動與輸出時間，
且本機同時執行回歸測試。輸入及功能與前次不同，不將這兩次視為效能改善證據。
來源傳輸沿用遠端已校驗檔案，3.25 GB 邏輯來源包僅傳送約 63 MB；
這是資料交付最佳化，並非 epoch／fold 訓練加速。
[建置紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v3_20260930/remote_terms.json)、
[傳輸紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v3_20260930/candidate_transfer.log)。

按依賴順序，把每列只歸到第一個未解決原因後，847,659 列分為：
商品規格 542,017、調整契約條款 61,396、保證金 105,206、部位限制 126,601、
持倉接續 1,293、估值或其他 11,146。這是優先修復順序的互斥列數，
與下方各原因可重疊的總數不同。
[優先順序收據](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v3_20260930/exclusive_blocker_priority.json)。
最優先應接齊現有歷史公告中的非股票規格，再完成公司行動及持倉接續；
調整模型大小、訓練輪數或槓桿不會補回這些會計定義。

前次語義修正後，本機與遠端各 **316 passed、2 skipped**，涵蓋規則接合、
帳戶、稅費、發布與資料介面。最後欄位精簡另跑 30 項焦點測試。
[本機測試紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v2_20260930/local_tests.json)、
[遠端測試紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v2_20260930/remote_tests.json)。
既有合成資料發布、帳本及反向傳播測試也保留在
[先前收據](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_execution_validation_v1_20260930/tests.json)。
以上不等於全商品 GPU 訓練、完整 fold10 測速，也沒有證明投資報酬改善。

先前 v4 每日資料的 4,968,246 列已在兩端逐欄比較。原始市場欄位、執行欄位、
槽位與缺口表完全一致；唯一差異為未平倉量變化的 `asinh` 派生特徵，
Float64 最大差異為 `1.7763568394002505e-15`，轉為模型實際使用的 Float32 後完全一致。
保留差異，沒有將 Float64 誤報成逐位元相同。
[比較收據](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_execution_validation_v1_20260930/materialization_comparison.json)。

前次 v6 會計條款、逐列缺口、商品覆蓋與工作清單，在本機及遠端逐欄完全一致，
也與效能精簡前的 v4 條款完全一致。
[遠端比對](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v2_20260930/v6_remote_comparison.json)、
[最佳化前後比對](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v2_20260930/v4_v6_comparison.json)。
遠端另核對本輪候選包 51,260 份來源及 14 個輸出的 SHA；此數量不同於先前跨資料集稽核範圍，
不應相加或解讀成交易規則已全部准入。
[來源驗證](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v2_20260930/remote_candidate_sources.json)。

目前會計條款中，開盤保證金缺 245,713 列、部位限制缺 246,928 列、
調整條款缺 61,396 列。這些集合有重疊，且會計列與全來源列的分母不同，
不能相加成「不能交易的商品數」。
仍有 **1,528** 筆持倉接續缺口：公司行動 1,211、零未平倉量後下市 172、
未明確到期 75、官方結算日／交割處理 70。
最後未平倉量為零不能單獨證明模擬帳戶無持倉，不會因此自動消掉部位。
[逐筆持倉缺口](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_terms_validation_v2_20260930/continuation_failures.parquet)。

狀態入口只讀取驗收收據：

```bash
cd /root/stockAgent
bash artifacts/markets/tw_futures_v8_margin_preparation/run_margin_history_20260929.sh status
```

`all`／`twd` 在尚未通過全商品驗收時會退出；舊 `verified-*` 入口的範圍是 TX／MTX。
最新指標由 [active_preparation_scope.json](../artifacts/markets/tw_futures_v8_margin_preparation/active_preparation_scope.json)
指向。上一版報告保留於
[文件歷史](../artifacts/markets/tw_futures_v8_margin_preparation/documentation_history/tw_futures_all_product_margin_history_before_inputs_v20_20260930.md)。
