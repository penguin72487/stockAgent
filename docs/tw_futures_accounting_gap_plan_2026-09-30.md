# 全台幣期貨會計缺口修復計畫（2026-09-30）

本計畫在本輪修改來源、編譯器與下載排程之前建立。基準為
`all_products_rule_facts_native_v79_20260930`、實體歷史 v18、輸入 v35、
物化 v17、執行條款 v18、規格 v5。保留 765 個歷史台幣代碼，來源範圍
1998-07-21～2026-09-24，初始資金一億元、保證金、多空與跨日持倉。
已准入的 437 商品 release 不屬於本輪變更；本輪沒有重新驗證遠端程序狀態。

### 2026-10-02 已驗收增量（截至 v811）

目前先修使用者保留的 711 個代碼：710 個股票期貨代碼及 CPF；暫緩的 54 個
代碼與其原始資料仍保留。沿用既有來源與共用會計程式，只重算受影響商品及
明示的合併部位／持倉依賴，尚未再做全 711／765 個商品的完整建置。

最新局部快取包含 **253 個代碼、1,939,507 列原始座標、1,918,308 列會計日，
其中 23,657 列受阻**。新增代碼的座標均與凍結原始來源逐列核對，不是填造行情。
以 v205 快取及後續新增代碼在原始 v47 的對應列比較，相同有效會計座標解除
**9,975** 列、新揭露 **1,452** 列；另有 121 列會計轉為暖身、142 列暖身轉為
會計，原始座標零遺失。不同範圍不直接比較分母，也不累加重疊批次的改善。
較早商品的快取仍等待最終來源整合；這些數字不能稱為全 711 個代碼的目前總數。
[端點比較與驗收總收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/repair_batch_progress_v811.json)、
[局部工作表](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/affected_gap_worklist_v810/manifest.json)、
[逐商品缺口](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/affected_gap_worklist_v810/products.csv)。

| 互斥上游原因（僅此局部快取） | 受阻會計列 |
| --- | ---: |
| 日期化商品規格 | 0 |
| 公司行動及調整契約條款 | 0 |
| 保證金 | 0 |
| 部位限制及時鐘 | 21,025 |
| 持倉接續 | 1,013 |
| 估值及其他 | 1,619 |
| 合計 | 23,657 |

v720→v794 的同一局部範圍再解除 **89 列、零新增受阻**，原始座標與暖身
身分不變。先完成各批來源與時鐘計畫，再修改共用接合；沒有重建全部帳本。
保證金歸零只適用此 253 代碼快取，CPF 的早期保證金鏈仍在快取以外待補。

v794→v810 再解除 **486 列、零新增受阻**。大成鋼、永豐金、台中銀的新
世代原件已證明 2,000 股及合併部位，舊原件較晚排定的上限却仍在商品日
接合遮住新世代；相同月份也不能當成相同世代。新接合只使用重新驗證的
原件／頁面 SHA、公開時鐘、15 個自身月份及明示群組，承接已完整解析的
標準契約股數上限；不同股數的 FE2／DE2／HW2、獨立上限與仍適用的自身
數值上限不覆蓋，沒有新增或猜測上限數字。只重算九個相依代碼的 45,160 列。
三份財務原件與所有行情、交易權限、單位及暖身身分保持相同；大成鋼較早
快取的 13:30 現金到期欄位另外依先前已驗收的編譯器 v6 規則核對，沒有把
13:45 保證金提前生效。初次不相關 DIF／HAF 保證金重算診斷未准入。
[修改前計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/unchanged_member_binding_plan_v800.json)、
[原件及月份證明](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/unchanged_member_proof_v803/manifest.json)、
[逐欄及逐座標驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/unchanged_member_replay_acceptance_v808.json)。

最終完整重建使用既有 `scripts/build_tw_futures_margin_rules.py` 的
`--position-generation-proof <unchanged_member_scopes.parquet>`，配合既有日期化
`--position-family-review`；新證明使用 v4 範圍契約、原件與實作指紋，不得
繼續使用舊輸入／檢查點指紋。該參數已接回共用流程，但尚未執行全 711
重建或遠端訓練驗收。

- 南帝 2017 年及穩懋 2018 年的處置保證金，接回自身完整原件、原生新聞稿
  與當時股票單一交易時段規則；各解除 44／5 列。穩懋原件的兩筆未定時段
  候選精確替換，原 37 列保留於原始候選表。
- 共用編譯器 v6 分開現金到期的 13:30 事件與一般收盤後的保證金法律時鐘，
  到期事件只用當時已生效的開盤保證金；解除 7 列，不把後來規則提前生效。
- 聯電、東聯處置期間的 2026-07-10 停市，由官方原生停班資訊、現貨停市
  法律及修法索引證明，重算實際十個現貨交易日。四筆有限取得中三筆證據
  採用、一筆歷史法條路徑無結果留存而未採用，沒有新增 OCR。聯電同一邊界
  的恢復與一般調升按原件串接；兩商品解除淨 23 列，與上述到期修復不重算。
- 已在同日公布的標準商品保證金候選，依日期化同標的法律傳給調整契約；
  數字仍空白，由執行時鐘最後驗證。HA／HH／KI 合計再解除 10 列。
- 台虹 2016 年原件修正 KI1 為 2,040 股與自身月份。新增原件發文日核對，
  保留發文日 8 月 10 日與索引公開日 8 月 12 日的差異，`known_at` 仍以
  8 月 12 日為準。初次診斷揭露的 63 列沒有併入快取；補回自身來源後，
  原會計財務值及旗標不變，不能把恢復這 63 列算成額外改善。

[南帝來源及會計驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/jnf2017_margin_replay_acceptance_v734.json)、
[現金到期時鐘驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/early_cash_margin_replay_acceptance_v740.json)、
[聯電／東聯來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cc_ey_original_repair_acceptance_v763.json)、
[同日承接、台虹及穩懋驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/remaining_margin_and_ki_source_acceptance_v793.json)。

十份完整既有原件的財務修復已接入：DP／HA／IP／JI／CM／DX、CJ／CZ／NM
及 HS2019。新增的 VQ1／VQF 只接入已存在的原始座標。局部快取的缺調整
條款旗標為零；HS2 的已固定認購利益依自身原件為 246 元。
最後五筆待估認購利益使用已留存的原始 MOPS／TWSE 資料、FinMind 股利
回應及官方股票收盤價補齊：CJ1 201111 為 148 元，其餘 CJ1 三個月份及
HW2 201109 因實際股票收盤低於認購價而為零。既有 122 筆終端利益完整保留。
這五筆只解除一列受阻，另四列仍缺部位規則；不能把補齊利益稱為五列均可交易。
[五筆利益及原始來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cj_hw_terminal_acceptance_v707.json)、
[實際快取比較](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cj_hw_terminal_replay_acceptance_v709.json)。

長榮 2019 年原件的前後部位期間都印 10 月 16 日，且上限不同。共用解析器
保留兩個日期與金額，在該日禁止准入，10 月 17 日以後才接入自身印載的
800 萬股上限及已驗證的最新級數接續。只重算 HS1／HS2／HSF 的 20,617 個
原始座標，10.71 秒完成；相對實際 v710 快取解除 **809 列**、揭露 **9 列**
10 月 16 日歧義，沒有改寫日期、刪除座標或替換行情。
[修改前計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hs2019_overlap_boundary_plan_v713.json)、
[部位原件驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hs2019_position_source_acceptance_v717.json)、
[局部會計驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hs2019_position_replay_acceptance_v719.json)、
[當時共用測試指紋：630 passed、2 skipped](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hs2019_overlap_shared_contract_acceptance_v723.json)。

最新一輪先把財務擷取、部位股數／群組、實體契約換代及缺行情分開診斷，再依
每批計畫修正。沒有可用原生文字的，只核對既有完整頁面及其 SHA，沒有重做
OCR。具體解除的原因包括：大成鋼 2,200 股誤讀成 200、亞聚 2,040 股誤讀成
2.04、瑞昱漏讀權益加項；中信金、合庫金與昱晶的認購權利仍保留實際需求，
不補零利益。南紡、遠百、欣銓與台中銀另外接入各自的月份、股數及完整部位表。

台中銀 2011 年 HW1 先為 2,060 股，後轉成 HW2，新 HW1 為 2,000 股。
共用接合器新增有明確版本的「完整原件、單位不變、固定股數約束蘊含」證明，
只有完整舊成員逐一承接、月份與時鐘已綁定、新約束涵蓋舊約束，才解除重用
代碼造成的衝突。原來要求公告明示引用的證明路徑保留；動態級數及較大的新
上限不套用此新增證明。舊公告數字完整保留，不推定法律撤銷。
[來源及約束驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hw2011_generation_source_acceptance_v653.json)、
[程式與測試指紋](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hw_generation_shared_contract_acceptance_v659.json)。

這批也揭露 HW2 2015-10-14～2015-12-16 的 **52 列**部位接續缺口：完整
2015 年原件的 HW2 為 2,128 股，但部位段只明示 HWF／HW1 合併，沒有印出
HW2 新上限。下一步需接入 2015 年原實體契約的既有約束，並保留不能互相
蘊含的多重約束；不能把 2011 年 2,060 股上限延用，或自行填三商品合併。
另有 HWF／IFF／NEF 轉換前的十列缺承接月份報價，仍受阻。
[完整原件的實際範圍](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hw2015_original_scope_boundary_v661.json)。

分批比較均以實際前一快取為分母：DN／HW／LO 的股數修正只重算 11 代碼，
8.42 秒解除 574 列、零新增；六份財務修正重算 24 代碼，14.76 秒解除
128 列、零新增；四份原件重算 9 代碼，12.56 秒解除 288 列、揭露 37 列；
台中銀換代重算 3 代碼，6.98 秒解除 30 列、揭露上述 52 列。這些批次有
重疊，總改善只看上方端點收據。新增 FE2 的四列原始座標及暖身身分另驗，
原始報價、量及執行允許欄位均保留。

這批先完成來源／接合診斷與有限修復計畫，再改共用實作；沒有為每次修改重建
711／765 帳本。最新三商品群組修正只重算 14 個代碼，實際耗時 **10.80 秒**；
與修改前的 v560 快取比較，解除 **607** 個相同有效座標、**零新增受阻**，
會計／暖身身分及原始報價、量與執行允許欄位均不變。
[修改前的計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/mixed_generation_repair_plan_v570.json)、
[來源及財務不變驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/mixed_generation_source_acceptance_v574.json)、
[實際快取逐列比較](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/mixed_generation_replay_acceptance_v579.json)。

本輪共用修正與來源邊界：

- 原生 Word 財務／部位表格現在逐欄接合空白續列，並使用自身上市日與月份；
  原生兩代碼群組只有各自的標準契約表、明示合併條款及財務單位一致時才接為
  1:1。缺上限、缺自身單位、選擇權群組及競爭表格都不推定數字。
- 2011 年原件沒有「最新適用級數」條款的，仍保留沒有該條款的事實，不能
  套用後來的公告語意。日月光 2011 年誤辨成 CIF／CI1 的身分，僅依相同
  原件 SHA、日期、月份及數量的逐列收據替換為 CTF／CT1；錯誤列保留於
  拒收收據，最終來源整合也必須套用該精確替換。
- 潤泰新 2011／2015 年認購權利，分開保留契約單位、除息現金及終端權利。
  2015 年 FinMind 現金股利與原件不一致，沒有用該股利計算會計；只用已綁定
  的認購價／比率及實際未還原收盤。新增四個實際終端事件，原 122 筆完全保留；
  2015 年兩筆也與官方完整最後結算契約金額相符。
- 日月光 2017 年原件未提供除息入帳欄位，保留空值而非填零；官方已提供
  二月／三月完整結算契約金額 77,824／77,425 元，直接由既有接合器採用。
  不為已有完整官方金額的事件另猜認購運算元。這批解除 27 列、新揭露 3 列
  接續缺口，原始報價及執行允許欄位保留。
- 中信金 2010 年原生 PDF 的 `CN2(原CN1)`、`CNA(原CNO)及CN1(原CNF)`
  按各自目的商品、原商品與實體月份拆開；七月 CN1 的 1,280 元現金給付及
  八月 CN2 的同額給付不借給八月新 CN1。原件未提供的權益入帳欄位仍空白。
  相同快取範圍解除 202 列、新揭露 2 列，淨減 200 列。
- 元大金 2016 年合併原件存於被收購的大眾銀 `2847_20160310.pdf`，而非
  元大金股票代號的檔名。依原件接入 ICF→DO1、921.944 股及買賣方權益
  加減項 16,300 元；現金交割部分為零，部位合併的標準商品是 DOF。
  共用解析器接受原件的「期貨部位合併計算」，相同快取解除 459 列、無新增受阻。
- 中信金 2013／2015、玉山金 2017、元大金 2015 的四份留存原件，接入六筆
  財務事件及二十筆部位來源列。固定認購利益只有原件已確認涵蓋全部利益時，
  才解除待定要求；新光金 DD2 的舊 45 元及新利益、新世紀 JA2 的舊 327 元
  及新利益仍分開計算。舊索引中四筆已被新契約調整取代的 DN1 來源綁定，
  原列完整保留於隔離表；有效索引由 126 筆改為 122 筆，不刪原始行情。
  本批 v352→v389 同有效座標解除 214 列、新揭露 3 列，暖身身分另列，淨減
  210 列。先前 v372 對五筆舊利益的完整性推定已撤回，未併入工作表。
- HS1 的獨立上限原型沒有正面歷史證據證明適用範圍，已撤回且未併入來源或
  快取。原型顯示的 925 列改善未計入本報告，不能以後來規則推定 2011 年語意。
- 裕隆 2023 年原生條款證明新 FN1 與 FNF 各 2,000 股、合併 1:1；只有來源
  明示、事前可知的同單位新世代才終止舊世代的衍生換算，保留公告原列及
  其他群組成員。合併 FY／FN／EL 的已驗證財務來源後解除 219 個既有受阻座標。
- 國泰金、彰銀、啟碁及合庫金四組已有自身月份與單位證據，接入原件的合併
  群組。初次候選遺漏 HY1 2018 年已驗證來源，造成 63 列新受阻，未准入；
  恢復舊來源分支後只重算 HY1／HYF，合併批次解除 478 列、沒有新增受阻。
- 台中銀 2015 年原始留存頁面證明 HW1 為 2,000 股，修正 OCR 的 `2.000`。
  HW2 原財務列完全保留；認購權利仍須實際認購價與指定日價格，不填零利益。
  本批既有座標解除 18 列、沒有新增受阻。
- 修正中信金 2015、啟碁 2016、華南金 2017、喬山 2013 原件的身分、月份
  及財務／部位欄位歸屬；前三份另與 TEJ 原生匯出的五列調整資料核對。
  喬山原件第二期間印成 `101.10.17`，沒有擅改為 2013 年；只接入原件可證明
  的第一期間。這項正確性修復揭露 KTF／KT1 合計 880 列缺當期上限。
  v517→v560 同有效座標解除 539 列、揭露 945 列；新增 HZ1／HZF 的原始
  座標另有 78 列受阻，所以此階段總數增加，不能報成整體改善。
- 玉山金 2017、兆豐金 2013 的原件證明新調整契約仍為 2,000 股，且明示與
  標準契約合併。共用編譯器重新驗證留存原件與頁面 SHA，才終止舊世代的
  衍生上限；只重算六個相依代碼，解除 277 列、零新增受阻。
- 台新金 2014、永豐金 2014、長榮航 2018 只重用已存頁面的完整標準／合併
  段落，合庫金 2015 優先重用原生表格及完整收據。三商品群組中的 2,000 股
  成員可接 1:1；2,100／2,161.6／2,178.1805 股的其他成員沒有套用該比例。
  共用編譯器原先只辨識兩商品措辭，現在以同一個來源及單位解析器驗證三商品
  群組的同股數成員，停止五筆舊世代衍生上限，保留原公告上限及全部財務 facts。

[裕隆新世代與原件驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/fn2023_native_position_acceptance_v443.json)、
[四組單位來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/known_unit_generation_source_acceptance_v455.json)、
[彰銀舊來源恢復驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hy_accepted_branch_source_acceptance_v461.json)、
[台中銀原始欄位驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/hw_unit_source_acceptance_v472.json)。

[原生 Word 財務驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/retained_word_cz_dg_source_acceptance_v245.json)、
[原生 Word 部位驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/native_word_position_source_acceptance_v253.json)、
[九份財務原件驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/regular_unit_cells_source_acceptance_v287.json)、
[潤泰新原件驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/kp_rights_source_acceptance_v300.json)、
[潤泰新終端驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/kp_terminal_acceptance_v303.json)、
[日月光官方完整金額驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/ct2017_official_total_acceptance_v315.json)。

[中信金原生來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cn2010_native_source_acceptance_v339.json)、
[元大金合併原件驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/do2016_merger_source_acceptance_v348.json)、
[HS1 原型撤回](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/dated_group_prototype_retirement_v334.json)。

[四份契約調整驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cn_dn_do_complete_source_acceptance_v384.json)、
[固定及待定利益修正計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/fixed_and_pending_rights_plan_v379.json)、
[到期索引完整分割驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cn_dn_do_terminal_revalidation_acceptance_v387.json)。

本批十個受影響模組的共用回歸為 **636 passed、2 skipped**；先前來源批次
另有 v783 的 658 passed、2 skipped 紀錄，測試集合不同，不比較測試數量。
本輪來源修復沒有新增 OCR、影像
轉檔、完整帳本建置或遠端訓練；來源取得與本機測試分開驗收。
[本批共用回歸紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/unchanged_member_regression_v805.log)。

已完成的 FinMind 分鐘回應不等於缺失持倉月份已補到：QM1 單日 43 列只有
202207／202208，所需 202212／202303 沒有出現；CJ1 單日 6 列只有
201508／201603，所需 201512／201606 沒有出現。兩者均未用於會計准入，
不重抓同鍵或借用其他月份。CR1 2012-08-22 已找到且驗證完成收據與 29 列
分鐘來源，月份為 201209／201210／201212／201303，201306 不在回應中；
第一根為 09:17，不能倒填為 08:45 可成交價或官方結算價。
[CR1 分鐘來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cr_minute_source_acceptance_v450.json)。

TEJ Pro Smart Wizard 的共用 collector 已用原生 MSAA Preview 完成有限匯出：
CR1201209 在 2012-08-22 的調整表一列九欄，原始來源、收據及 Parquet SHA
均驗證。CJ1201512／CJ1201606 在 2015-08-13 的行情查詢由供應商明示空結果，
沒有補到缺失持倉行情。已恢復這兩筆暫時優先任務的優先序。授權原始數值
保留於私有 `data_tej`，不重刊；公告時點、全部欄位單位及全歷史完整性仍未
證明，因此未准入會計。原生調整參考價不能當成執行行情。
新的 HW 有限查詢在註冊前遇到既有 `unknown_outcome_no_auto_retry`，沒有
啟動 GUI 或資料查詢，也沒有清除其他工作者的未知結果；此欄位改用留存原件。
[TEJ 有限來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_exact_gap_source_acceptance_v449.json)、
[HW 查詢受阻證據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_hw_registration_block_v467.json)、
[共用 TEJ 取得方式](tej_smart_wizard_acquisition_2026-10-02.md)。

後續 TEJ 調整表另完成 CJ1／KI1／CN2 三個精確範圍，共五列；只作原件欄位
交叉核對，授權原始資料仍留在私有 `data_tej`。參考價不是成交或官方結算價。
已檢查這個帳號可見目錄的 45,826 欄位與六張期貨相關表，未找到歷史保證金／
部位上限欄位；這不代表所有 TEJ 帳號或資料庫都沒有。每日行情表可查結算價，
較早十個精確契約／日期查詢曾被既有股票查詢的未知結果阻擋。2026-10-02
重新讀取共用佇列，該舊任務已完成；不再把舊鎖當成目前障礙。已用共用 collector
註冊 CN2／KI1／CJ1 的三個精確日期範圍，沒有展開全部日期／商品網格。
CN2 任務先有兩次前景不可用、未送出查詢的有限嘗試；前景短暫可用後，
第三次仍在 Preview 前因精確來源選項名稱不存在停止。其後原生軸唯讀檢查
遇到所屬視窗 `FromHandle` 讀取失敗，不能將這些本機障礙當成供應商沒有歷史。
目前 KI1 2016-08-19 的三個指定遠月已完成，供應商明示空回覆，沒有可准入
行情；不重複這筆已完成查詢。CJ1 的有限優先嘗試被共用 collector 的
`inflight_requires_recovery` 防護攔下，沒有送出新查詢。其後唯讀檢查發現
先前觀測的 discovery 狀態不是現在的障礙。最新一次唯讀快照顯示另一張
`TDR Qfii Broker Trading` 表的下載為 `unknown_outcome_no_auto_retry`；
嘗試已結束，但記錄的原始回應檔不存在。這不是供應商空結果，也不能自動
重送付費查詢。CN2／CJ1 仍待查，KI1 空回覆保留；三筆優先序均為 100。
先保留並對帳既有未知結果，再處理精確來源選項，不清除其他工作者的狀態。
[原件與 TEJ 核對驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_original_source_acceptance_v539.json)、
[每日行情有限查詢計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_transfer_price_gap_plan_v546.json)、
[歷史未知結果及可見目錄證據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_transfer_query_barrier_v576.json)、
[本機選取障礙與 KI1 空回覆](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_transfer_gap_runtime_boundary_v712.json)、
[CJ1 有限優先與優先序恢復](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_cj_far_month_finite_priority_v748.json)、
[最新 TEJ 唯讀障礙快照](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/tej_remaining_scope_runtime_boundary_v796.json)。

後續已按上游因果需求重新排定：先補歷史部位上限及適用時鐘，再補實際持倉
月份行情與 CPF 早期規則；此局部快取已歸零的利益及保證金不重抓。每批仍只重算自身與明示
相依商品，所有增量完成後才整合來源、做一次全 711 驗收及遠端訓練 smoke。
[剩餘缺口與准入條件](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/remaining_after_margin_repair_plan_v797.json)。

另外依使用者授權暫時插隊 FinMind，只對 CN2 2015-10-02、KI1 2016-08-19、
CJ1 2017-08-17 各查一次分鐘資料，分別回應 1／7／3 列。三筆皆只涵蓋近月，
沒有補到缺失遠月；不將較晚分鐘價倒填開盤或當成官方結算價。這次有取得共用
worker 鎖，完成後三筆優先序與原下載服務均已恢復。較早 v564 未持有 worker 鎖
的嘗試沒有送出分鐘資料查詢，原收據保留；v568 是本次正確持鎖的有限取得。
[分鐘來源與優先序恢復驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/transfer_minute_source_acceptance_v569.json)。

剩餘依上游依賴分開處理：先確認早期季度上限與調整契約合併上限的歷史適用
關係，再補各世代月份與生效時鐘；CPF 另補法律起算及中間保證金鏈；接續及
估值須取得自身月份的真實價格／官方完整金額。已有來源的群組衝突不能按公告
新舊或最小數字自行選擇。上述資料缺口未補齊前，不降低檢查或把受阻列隱藏。

**尚未歸零，遠端原 437 商品 release 未變更。** 剩餘要逐原件補齊當期上限、
獨立限制、生效時刻、月份股數與真實持倉承接行情；CPF 還缺早期部位法律起算
及 17,000→12,000 元的中間保證金變更鏈。2007 年原件不能倒填到 2004 年。
下面 10 月 1 日及更早的數字皆為各自當時的驗收快照。

### 2026-10-01 範圍校正與本次修復

先依使用者最後同意的安排，處理排除既有 54 個代碼後的 711 個代碼；
實際組成為 710 個股票期貨代碼及 CPF 利率期貨。排除清單沿用
`scoped_stock_margin_20260930/deferred_54_products.csv`，不重新推定商品分類。
765 個代碼的原始資料與全範圍稽核仍保留，但暫緩商品的特殊交割問題
不作為這 711 個代碼先行訓練的前提。

最後一次完成全範圍逐座標驗收的執行條款為 **v46**：711 範圍保留
**4,373,106** 列會計日，受阻 **89,407** 列。v46 的部位群組檢查揭露新增受阻，
不能把 v45 的 49,228 列當成目前完成數字。此後只驗收受影響來源增量，
不重複重建全 765 個商品；下表是已保存的歷史驗收，依上游順序互斥分類。

| 上游原因 | v37 | v39 | v41 | v42 | v44 | v45 | v46 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 商品規格 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 調整條款 | 10,227 | 10,227 | 10,227 | 10,227 | 10,227 | 10,227 | 10,227 |
| 保證金 | 15,358 | 15,358 | 15,358 | 14,638 | 14,084 | 13,994 | 13,994 |
| 部位限制 | 22,414 | 18,736 | 18,048 | 18,048 | 17,401 | 17,401 | 57,590 |
| 持倉接續 | 1,744 | 1,750 | 1,750 | 1,750 | 1,752 | 1,752 | 1,742 |
| 估值及其他 | 5,775 | 5,775 | 5,775 | 5,775 | 5,854 | 5,854 | 5,854 |
| 合計 | 55,518 | 51,846 | 51,158 | 50,438 | 49,318 | 49,228 | 89,407 |

v45→v46 的全 765 範圍保留全部 4,915,123 個會計座標，解除 1,725 列、
新增受阻 41,904 列。後續修正等價股數／口數表示及最新級數接合，不以退回
舊檢查掩蓋衝突。已建置的 v47 尚未完成全範圍逐座標驗收；其已保存的
逐商品摘要可計得當時 711 範圍受阻 **77,810** 列，但這不是後續增量修復後的
目前總數，不能從候選完成或局部改善推定全範圍歸零。
證據：[最後完整稽核](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/remaining_711_v46/audit.json)、
[v45→v46 座標驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/transitions_v45_v46/audit.json)、
[v47 歷史商品摘要](../artifacts/markets/tw_futures_v8_margin_preparation/all_twd_execution_terms_v47_20261001/product_coverage.csv)。

### 已修商品的來源增量與目前限制（2026-10-01）

依既有來源、月份／單位及群組關係，只重算受影響商品；沒有重建全部 765 個
商品的帳本。最新局部工作表 v163 保留 **205 個已處理代碼、1,575,039 列來源**，
其中 **1,557,606 列會計日、29,620 列受阻**。相同 203 代碼快取範圍的 v94→v156
由 30,843 降至 29,934 列，淨減 909 列；v156→v163 在相同會計座標再解除
316 列、無新增受阻，另將先前快取未涵蓋的 CL2／CZ2 原始行情 197 列納入，
其中兩列仍受阻。不能直接用不同代碼／列數的分母計算總改善率。
較早商品仍等待最終來源整合，這不是全 711 範圍的目前受阻數。
[最新局部工作表](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/affected_gap_worklist_v163/manifest.json)、
[逐商品缺口](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/affected_gap_worklist_v163/products.csv)、
[修復進度與驗收總收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/repair_batch_progress_v166.json)。

本批根因及共用修正：

- **同代碼、不同月份及世代借錯部位上限**：直接引用調整契約股數上限時，
  必須對上原件 SHA、商品及實體月份；來源範圍契約升為 v2，規則建置版本升為 4。
  不以相同代碼或相同股數跨公告借用上限，正確檢查揭露的新增受阻也保留。
- **原生表格跨頁漏接**：只接受相鄰頁、唯一標籤與完整乘數／給付兩側證明。
  補入台泥 2010 年及小型上銀 2022 年的指定月份，現金、股票與認購權利分開保留。
- **多商品換算沿用舊股數**：上銀 2022 原件明示 FFF／FF1 各 2,000 股、
  QMF／QM1 各 100 股及四商品合併。共用解析器逐欄核對自身標準契約，
  建立兩條 1:1 公式；仍須接當時有效上限，不新增假定數字。
- **公文首頁提及的標題遮住附件**：兩代碼解析器改為按每次標題切出本地區段，
  拒絕跨區借單位、矛盾標準表及缺列數字上限。上銀 2020 原件因此接回
  FF1→FFF 的 1:1 公式，財務 facts 與四個財務 JSON 輸出不變。
- **已公告的第二期間級數被錯拒**：只有原公司調整函明示最新適用級數、
  級數已於該函發布前得知且在印出的第二期間生效，才可接合。
  本次五商品來源輸出與父版本逐檔相同，沒有為這項工程修正再重算相同帳本。

各批與保存的 v47 原始會計座標比較如下；有重疊修復的舊版本不能再相加。

| 本批明示相依商品 | v47 受阻列 | 本批受阻列 | 解除／新增受阻原座標 |
| --- | ---: | ---: | --- |
| DR1、DRF、LO1、LO2、LOF | 3,179 | 1,819 | 1,392／28 |
| CJ1、CJ2、CJF、HY1、HYF | 2,654 | 2,249 | 416／6 |
| DN1、DN2、DNF、HG1、HGF | 2,159 | 1,158 | 1,016／12 |
| FF1、FFF、QM1、QMF | 586 | 43 | 545／2 |
| CL1、CL2、CLF、CZ1、CZ2、CZF、QO1、QOF | 1,162 | 789 | 435／62 |

各批原始來源座標與 provider 報價／執行允許欄位保留；暖身與實際會計列的
轉換另列於收據，因此解除列數不等於兩欄相減。上銀最後一批相對前一局部版本
再解除 126 列，無新增受阻、無座標損失。
證據：[LO 批次](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/lo_same_original_group_replay_v101/manifest.json)、
[CJ／HY 批次](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/retained_grid_replay_v115/manifest.json)、
[DN／HG 批次](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/missing_month_replay_v116/manifest.json)、
[上銀批次](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cover_heading_group_replay_v155/manifest.json)、
[長興／長榮／兆豐金批次](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/retained_group_batch_replay_v162/manifest.json)。

依已保存的缺口月份／單位，有限查閱 174 份原件，找到三份可新增合併公式的
來源。長興 QO1、兆豐金 CL1 直接核對原生 PDF；長榮 CZ1 核對已保存的兩頁
影像及其完成收據。只將各自不變的 2,000 股契約接為 1:1，不把 CL2 的
2,016 股或 CZ2 的 2,100 股當成相同單位，也不把選擇權群組當成期貨群組。
CL2／CZ2 在原始商品範圍中有實際行情，這次納入局部快取；新行情沒有填造。
相對 v47 新揭露的 62 列是 CL1 早期月份來源限制，在先前局部快取已受阻，
並非本次三份合併公式新增的回歸。不能以退回無月份檢查消除它們。
[有限來源計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/retained_group_batch_plan_v157.json)、
[來源掃描結果](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/retained_group_batch_probe_v158.json)、
[三原件獨立驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/retained_group_batch_acceptance_v161.json)。

原件、月份、現金及股數另經獨立來源驗收；認購價、繳款截止日收盤與期貨
最後結算價不得混用。台泥 2010 年僅新增一筆有實際未還原收盤與認購價的
終端權利值，父版本 121 筆終端事件保留，沒有填造其他月份的價格。
[財務原件驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/retained_financial_cell_acceptance_v120.json)、
[上銀早期原件驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/ff_earlier_financial_acceptance_v131.json)、
[原生小型契約驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/qm_native_financial_acceptance_v137.json)、
[四商品公式驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/mixed_standard_group_acceptance_v142.json)、
[兩商品公式驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cover_heading_group_acceptance_v153.json)、
[台泥終端驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/df_native_terminal_acceptance_v90.json)。

目前共用版本 **443 passed、2 skipped**，涵蓋來源解析、部位接合、保證金、
會計條款與 release。先前 v145 的證據標點比對失敗已修正，失敗紀錄保留；
v154 使用錯誤的權利資料路徑而在輸出前中止，未納入工作表，正確路徑的 v155
才有完成收據。本批未新增 OCR、影像轉檔、provider 請求、完整帳本建置或遠端訓練。
[本機回歸紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/cover_heading_group_regression_v159.log)。

**尚未歸零，也未更新遠端 437 商品 release。** 剩餘須按具體原件／欄位處理：
CPF 2004～2007 的部位法律起算及 17,000→12,000 元中間保證金變更鏈、
其他商品的當期股數／級數與生效時刻、真實持倉承接行情及終端估值。
上銀四代碼目前剩 10 列早期部位時鐘及 33 列實際持倉接續問題；
新揭露的 QMF 2022-07-11 兩個月份承接缺口保留，未以舊契約或假行情消除。
最新局部工作表中的非互斥原因包括部位上限 26,270 列、部位時鐘 5,697 列、
契約單位／調整條款各 3,690 列、持倉接續 978 列；這些原因重疊，不能加總。
已將 QM1／2022-07-12 的單日分鐘任務註冊到既有 FinMind 優先佇列，
保留共用額度、增量預留及原排程，未另開 collector 或重啟 daemon。
本次狀態仍為 pending，沒有將排入佇列當成取得行情或會計准入。
[單日補抓計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/qm_exact_minute_priority_plan_v164.json)、
[優先佇列收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/qm_exact_minute_priority_receipt_v165.json)。
最後仍須一次整合目前來源，逐座標驗收全部 711，再完成遠端重算及訓練 smoke。

### 增量修復與非 OCR 來源優先（2026-10-01）

先使用本機已驗證資料、官方歷史 JSON／CSV／HTML、日期化法規與原生文字；
其次補確實缺少的 FinMind 行情／公司行動運算元。既有已核驗轉錄直接重用，
只有其他來源無法提供同一歷史欄位時，才處理必要公告頁面的圖像。
等價性須涵蓋商品、實體月份、單位、適用期間、發布時刻及群組／例外條件；
資料格式不限定為公告 PDF，但今日快照不能冒充歷史規則。
[來源優先計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/non_ocr_repair_plan_v39.json)、
[局部修復計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/incremental_repair_plan_v33.json)。

部位候選 v122 的原件與父版本驗收已通過，其他五個財務輸出 SHA 保持相同。
之後只建立缺漏商品及其明示群組的 position delta v42：補回 2012 年第二季
漏接的 18 筆商品級數，44 個受影響合併上限邊界通過來源驗收。
共用接合器也接受公司調整原件明示的「最新適用部位限制級數／級距」條款，
仍要求原件 SHA、數字級數、完整群組及生效／已知時刻均已驗證。
證據：[v122 原件驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_parent_acceptance_v122.json)、
[財務檔案未改](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_unchanged_financials_v122.json)、
[部位增量](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_delta_v42/manifest.json)、
[44 邊界驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_delta_active_acceptance_v42.json)。

國巨 LXF 的 2018-05-22 停牌、05-23 復牌已由證交所歷史 JSON 證明。
修正共用下載器對「欄名＋陣列資料列」格式誤判空回的問題，直接重新解析
留存回覆；不重抓、不把 2026 年下載日當成 2018 年發布日。
結合已核驗的規則、發布來源與十個實際正成交日，建立 margin delta v44：
31 筆父事件完整保留，新增一筆原件事件及兩份來源各自的恢復事件；四個原始
暫時邊界由 05-28 改為 05-29。05-29 開盤仍用提高後比例，收盤才恢復
13.5%／10.35%／10%；05-30 開盤沿用恢復值。獨立檢查通過四個真實日期邊界。
本批沒有新增 OCR、provider 查詢或完整帳本重建；部分來源驗收不等於訓練准入。
389 項下載器、部位上下文及保證金相關回歸測試通過。
證據：[JSON 解析收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/structured_halt_normalized_v41/manifest.json)、
[保證金增量](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/margin_delta_v44/manifest.json)、
[原件守恆與邊界驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/margin_delta_acceptance_v44.json)、
[回歸測試](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/non_ocr_and_halt_regression_v44.log)。

v37→v39 解除 3,672 列、v39→v41 再解除 688 列，兩次均無新增受阻座標。
v39→v41 保留全部 4,915,123 個全 765 範圍的共同會計座標；711 的日期及月份
也沒有刪減。v41 的全 765 範圍受阻為 277,679 列，不能把它與 711 的數字混用。
證據：[711 稽核](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/remaining_711_v41/audit.json)、
[逐座標比較](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/transitions_v39_v41/audit.json)。

v41 使用候選 v114、實體歷史 v26、輸入 v58、物化 v40 及規格 v5。
本次接入 DE、FE、CL 原件明示的未變更股數合併公式，以及 LV 原件明示的
三代碼部位股數及上限。部位審閱共用既有逐月單位解析器，排除有獨立股數
佐證的 2,000 被 OCR 截成 200 的錯誤；真正的兩組完整股數衝突仍拒絕。
公司調整財務 facts、完整交付條款、單位區間及保證金 facts 與 v111 的 SHA
逐檔相同，因此重用 v26 實體歷史，沒有重寫既有現金或認購權利。
證據：[原件與父版本驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_scoped_parent_acceptance_v114.json)。

v41→v42 已驗收，解除 720 列、無新增受阻，保留全部 4,915,123 個會計座標；
全 765 範圍受阻 276,959 列。使用候選 v116、輸入 v59、物化 v41，
實體歷史及認購權利終端資料保持原來源。已修「結束後，恢復為」逗號、
一函多商品各自的恢復日、日期在未處置前金額引用中被誤當恢復日的問題。
ITF／PLF 表格的「未經處置前」是恢復參考值，不能當成當時調整前金額；
只准入原件明示的調整後比例，再與較早原件的相同日期、商品及比例組合。
仍須驗證實際處置期間、完成營業日及休市順延，不能只憑預定日期自動恢復。
只重算本次兩份公告的恢復事件；其他來源已驗證的恢復證據完整保留。
311 項相關測試通過、2 項略過。
證據：[來源審閱](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/margin_reviews_v16.json)、
[父來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/margin_parent_acceptance_v116.json)、
[逐座標比較](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/transitions_v41_v42/audit.json)、
[711 稽核](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/remaining_711_v42/audit.json)、
[測試紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/focused_tests_scoped_margin_restoration_full.log)。

v42→v43 再解除 566 列，無新增受阻，711 範圍受阻為 49,872 列；
互斥原因為調整條款 10,227、保證金 14,638、部位 17,401、接續 1,752、
估值及其他 5,854。已審閱 DN、CM、DW 留存原件，接入 DN 的不變股數公式與
2018 年兩期股數上限，CM 僅接入明示 2,000 股的 CM1，未將 CM2 當成相同股數。
候選 v117 保留全部父版本部位 facts，另新增 6 筆；公司調整及保證金五個
輸出逐檔 SHA 相同。CM 2017 PDF 缺少部位表、DW 原件沒有明示合併／上限，
仍記錄為缺口。證據：[來源審閱](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_reviews_v17.json)、
[父版本驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_parent_acceptance_v117.json)、
[逐座標比較](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/transitions_v42_v43/audit.json)。

v43→v44 已接入本機已有、未形成金額事件的三份延長處置函：華新科
11503016001、信昌電 11503016171、聯茂 11503018191。原件的調高後比例
與「標的證券未經處置前」分開；後者只對接原件指名的更早調整前比例。
保留舊公告，只以新公告明示的生效時刻終止其適用，再按新的實際處置期間
及完成營業日驗證恢復。其他來源的恢復 facts 不重算、不替換。
解除 554 列、無新增受阻，保留全部 4,915,123 個全範圍會計座標；
全 765 範圍受阻為 275,839 列。候選 v118 新增三筆調整後比例及三筆
有實際完成營業日證據的恢復事件，沒有移除父來源身份，其他五個財務輸出 SHA 相同。
證據：[來源審閱](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/margin_reviews_v18.json)、
[父版本驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/margin_parent_acceptance_v118.json)、
[逐座標比較](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/transitions_v43_v44/audit.json)、
[711 稽核](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/remaining_711_v44/audit.json)。

新聞稿的共用述詞已修：例如「甲於9月8日、乙於9月10日一般交易時段
結束後，恢復為調整前保證金」。只有已綁定發布日、原件唯一有效年份、
全部商品名稱／代碼及完整述詞，才分派每個日期；不把最後一個日期套給全商品。
反例須涵蓋缺年份、混合年份、未知商品、重複矛盾日期、缺收盤時刻及較早日期。
共用解析器通過 64 項時點測試及 321 項相關回歸測試（2 項略過）。
只刷新原件已缺失／矛盾的時鐘，保留金額與已驗證恢復。
首次 v119 建置因執行期間共用來源檔案變動而被版本檢查拒絕，未發布完成收據；
保留失敗日誌後以固定版本重建，不能採用未完成輸出。
v44→v45 已驗收，再解除 90 列、無新增受阻，保留全範圍全部 4,915,123 個
會計座標；全 765 範圍受阻為 275,749 列。保證金原件 facts 身份與金額不變，
其他五個財務輸出逐檔 SHA 相同。證據：
[父來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/margin_parent_acceptance_v119.json)、
[逐座標比較](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/transitions_v44_v45/audit.json)、
[711 稽核](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/remaining_711_v45/audit.json)。

LV 的原件明示 LVF→LV1、LV1→LV2，並引用先前函號，9 月 19 日的新表
完整包含轉換後的舊持倉且上限相同。依
[來源與反例計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_generation_plan_v20.json)
實作來源綁定的限制蘊含驗證；這不表示原公告曾被明文撤銷。
另已審閱 HSF 2011 年 10 月 20 日起的 7,500,000 股期間，沿用位置表審閱器補漏接區間。
DE 2014 年的新原件只明示合併商品、沒有金額表，不套用 LV 的修復。
候選 v120 的父來源驗收已通過；10 筆舊表示由相同兩份原件的 10 筆完整表示
取代，未解釋的刪除為零。公司調整、交付條款、單位、保證金五個輸出 SHA
與 v119 相同；候選驗收尚不是會計或訓練完成。
證據：[父來源驗收](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_parent_acceptance_v120.json)、
[財務檔案相同](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_unchanged_financials_v120.json)。

**修正先前 EUF／GLF 的限制判斷：** 2012-07-24 季度公告第 3 頁明示，
新級數生效後，調整契約須按最新級數計算。因此 2012-09-20 起的 350 口
級數應轉成 **700,000 股合併上限**，不能把舊的 2,500,000 股上限繼續當成
另一項存續限制。已檢視原件全部三頁，保留原判斷紀錄並另建
[更正計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_latest_grade_plan_v26.json)。
共用來源組合器新增季度條款核驗，仍要求舊級數、原表、標準單位、唯一新級數
及真正已知時刻；未證明的暫時擴大上限仍拒絕。原始金額與來源不改成假數據。

實際 v62 輸入還揭露 EUF／EU1、GLF／GL1 的合併股數被分成獨立群組。
已依原件明示成員修群組，保留各成員自己的股數，且將同日群組上限／計量單位
的衝突列為受阻。真正獨立的口數限制沿用既有兩組帳本欄位，不能讓調整契約
繼承標準契約獨有上限；第三項獨立限制仍拒絕。編譯器版本升至 4，新產物
與先前 release 分開。當時 392 項相關測試通過、2 項略過；候選 v121／v122
後續已完成，完整會計驗收與增量進度以本文件上方的日期化收據為準。證據：
[群組修復計畫](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_group_plan_v24.json)、
[季度原頁收據](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_recovery_probe_v25/manifest.json)、
[測試紀錄](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/focused_tests_independent_constraint_and_groups_full_v2.log)。

HSF 2019 原件兩個期間都印有 10 月 16 日，不能逕自把下一期移到 17 日。
證據：[原件與限制工作表](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/position_constraint_conflicts_v21.json)。
NU 事後修訂仍須保留原生效日與實際已知時刻，禁止把較晚公告回填成較早資訊。

剩餘缺口的來源工作表保留商品、日期與原件定位；「最近一份公告」只是查找線索，
不是該月份已具備規則的證明。CPF 的 2004～2008 年保證金變更鏈及早期部位
法規仍缺原件，不能把上市金額直接延用四年；LV 舊代碼轉換後的預定部位
恢復也需要對應正確實體世代，不能任取較新公告或較小上限。
證據：[原件工作表](../artifacts/markets/tw_futures_v8_margin_preparation/remaining_gap_repair_v3_20260930/source_worklist_v39/manifest.json)。

## 基準與第一性原理

每個會計日必須回答：這口契約代表何種資產與數量、何時知道適用規則、
哪些價格實際可取得、持倉如何接續、終止時如何完成義務。更多行情不能
補出法律規則；正確數值若套錯月份、日期或資訊時刻，仍不能准入。

初始 v18 基準有 4,914,926 列會計日，333,640 列受阻。下表是該初始基準，依上游順序互斥分類，
可以加總；商品數與非互斥原因不能加總。原始行情 SHA：
`4c8ae197ca80dbdb1c75b38e75da0b0c2c10acf4e5a3a7284d8d8426ed2380d6`。

| 上游原因 | 互斥列數 | 修復所需證據 |
| --- | ---: | --- |
| 歷史商品規格 | 162,150 | 原上市與修訂規則、日期化單位與結算時鐘 |
| 調整契約條款 | 27,925 | 月份、交付股數、乘數、現金與認購權利原件 |
| 保證金 | 93,595 | 金額、盤前／盤後生效、暫時調整與恢復證據 |
| 部位限制 | 41,959 | 歷史級數、合併股數、不同期間與既有持倉例外 |
| 持倉接續 | 1,846 | 原契約與目的契約、調整日、月份與生命週期 |
| 估值與其他 | 6,165 | 實際結算、停牌處理、終止與實物交割證據 |

相對早一輪基準，6,284 個相同日期／商品／月份座標新增受阻：其中
5,185 列有部位限制問題、504 列有盤後保證金問題、564 列有接續問題。
原因重疊，不能加總成 6,284。需查明是錯誤回歸還是新來源揭露舊缺口，
不能回退到舊的假完整狀態。

## 實施順序與逐項驗收

1. **建立全範圍工作表。** 從 v18 的實際 blocker 逐商品、年份與原因統計，
   另外保存新增受阻座標及來源問題清單。不能只看候選 facts 或測試通過。
2. **先修原件解析與接合。** 以 DN1／2014-04-18、IB1／2014-08-05 等
   原件確認小數／千分位、月份標題及權利公式；修共用解析器可證明的情況，
   個別轉錄沿用 SHA 綁定審閱。反例包含真正小數股數、不相干月份、錯來源
   及較晚公告，禁止全域把小數點改成千分位。
3. **修部位限制生命週期。** 追查 CBF、JNF、CLF、GLF、CQF、CMF、QZF、HSF
   的新增缺口。原件明示最新級數才可與該日級數公告組合；暫時擴大股數上限、
   回復後上限與獨立義務要分開。缺少條文不得取任意較小上限或沿用舊期間。
4. **補保證金區間。** 區分原件缺失、公告上下文漏接、前後表與恢復條件。
   1998～2008、MTX 2014～2017、延長處置等按來源鏈修復。TX 與 MTX 比例
   只能採用當時有效且留存的明文規則；重用網址必須核對原件內容與發布日期。
5. **暫緩範圍後續接入規格與未支援會計義務。** 完全缺規格的 BRF、F1F、GBF、I5F、TGF、
   TJF、SPF、SXF、UDF、UNF 要逐商品接入。另處理 2008-11-21 以前指數
   T+1 最終結算、境外標的時鐘與 2026-07-06 起 ETF 三階段限幅。
   GBF 實物交割須具備相應義務／交割資料；不能偽裝成即日現金結算。
   凡改變帳本、狀態或可用性，更新 ABI／指紋，驗證 train/eval/inference。
6. **補實際估值運算元與生命週期。** 166 個調整終端輸入缺口、28 列
   認購權利阻擋、39 段未解生命週期及 2,538 列行情估值缺口互相重疊。
   使用原件認購價與實際未還原收盤，分清到期日／繳款截止日；接續只能使用
   實際月份轉換與有效官方生命週期。不得以前日收盤或插值填補執行價格。
7. **累積局部修復，再做一次完整驗收。** 每批只處理受影響商品、期間及其
   直接群組／持倉相依，保留父來源 SHA、增量與局部反例驗收。所有修復批次
   完成後，才整合一次全範圍帳本，檢查六類缺口、逐座標變化及全部代碼覆蓋。
   零阻擋後建立 release，校驗遠端檔案、重算真實邊界與有限
   train/validation/test；通過後交付指令。

## 下載與排程

先查本機已抓來源與收據，再查同義的官方歷史結構化資料及原生文字。
FinMind 優先補具體缺少的行情、股利與認購等運算元；每個欄位必須驗證
歷史日期、單位、事件與發布時刻。法律／契約事實可採用具備相同歷史涵義的
官方表格或日期化條文，無須強制經過公告影像。若只有目前快照，或欄位沒有
適用月份／特殊權利／群組例外，則仍缺少該歷史事實，不能以數值近似代替。
每項新 OCR 工作須列出必要欄位、頁數及非 OCR 來源查找結果；已核驗轉錄
不再辨識。已查 634 個估值日期的既有日資料只有
65 個匹配、零個有效正結算價，因此不重複查相同零值以求表面完整。
沿用 `download_finmind_complement` 的有限 priority keys 及共用 limiter；
按缺少的 dataset／商品／日期去重，保留已有成功與來源空回。任務完成後
自動回到一般派送，不建立第二個下載器，不改全帳號額度或繞過來源節流。
原有 54 非股票商品分鐘插隊任務繼續保留。

參考：[FinMind 股利政策欄位](https://finmind.github.io/tutor/TaiwanMarket/Fundamental/)、
[FinMind 期貨行情](https://finmind.github.io/tutor/TaiwanMarket/Derivative/)。
提供欄位不等於具有某個歷史事件的正確實際值，取得後仍須核對事件及原件。

## 完成定義

先行的 711 代碼及其原始歷史不縮水；會計、來源准入、持倉接續及終端義務均有
留存證據；受阻列為零；遠端重算與訓練 smoke 通過。若確實找不到原件，
工作表保留具體商品、日期、所缺欄位與來源狀態，不能宣稱歸零。
暫緩 54 個代碼另列稽核，後續完整 765 範圍也適用同樣驗收條件。
本計畫本身不表示全商品訓練已可用。
