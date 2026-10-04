# 台灣市場價格精度與歷史 tick 契約

更新：2026-09-19。完整的已登錄來源價格格點稽核：`source scripts/runtime_env.sh && run_fintech_python scripts/audit_tw_price_precision.py --full --strict`；本地逐筆原始擷取：`source scripts/runtime_env.sh && run_fintech_python scripts/audit_tw_capture_price_grid.py`。收據在 `artifacts/data_quality/tw_price_precision/`；每次資料或契約更新後須重新取得收據。

## 第一性原理

資料中的數字先按**經濟意義**分類，再決定精度。單筆成交、單式委託及最佳買賣報價是交易所接受的價格格點；均價、結算價、最後結算價、調整後價格、除權息金額、外匯、比率、費用與模型特徵是計算或公告結果，沒有理由落在同一格點。小數位數也不是 tick：`1.00` 與 `1` 數值相同，但原始字串、公告單位與時間版本都值得保留。原始資料保持字串或原精度，僅在生成委託價時依方向量化，再驗證合法性；不回寫歷史原始資料。

判斷某個正的來源報價 `p` 是否落格：先用交易日、商品、交易方式、幣別和**該報價自己的價位**取得 `q`，再要求 `p/q` 是整數。跨越級距邊界時，移動一跳還須重新查下一個價位的 `q`；浮點序列化的極小誤差只用有上限的 ULP 容差，不把價差的可見部分四捨五入。`OHLC` 的時間聚合、原始 K 棒量綱和 `High ≥ max(Open,Close)`、`Low ≤ min(Open,Close)` 是其他獨立檢查。若 `p` 是均價或加權後回報，就不應拿交易格點測試。

規則索引至少含商品、交易日、交易場所與市場機制、幣別、欄位角色。股期的實際契約（含調整契約）也必須保留。缺少其中一項而無法確定格點時，狀態是「未驗證」，不能自動視為合法或任意四捨五入。歷史回測用當日規則；即時委託也核對 Shioaji Contract V2 的 `tick_rule` / `tick_bands()`，但現在的契約屬性不能回填過去。

普通上市櫃股票自身的六級距／漲跌幅規則未改，故保留 `TW_PRICE_RULE_CONTRACT_VERSION=3`；擴大商品辨識及委託價變更分別記為 `TW_ORDER_PRICE_CONTRACT_VERSION=2`、`TW_DERIVATIVE_PRICE_CONTRACT_VERSION=2`。歷史來源位元組未因新的解釋而被改寫。舊訓練快取、成交模擬或 checkpoint 若綁定有變動的委託／期貨契約，需核對指紋、重新建構或重新訓練，不能用同一模型識別碼靜默更新。

## 已證實的 TWD 一般交易單式報價規則

| 商品 | 生效期間 | 價格級距與 tick（新臺幣） | 程式契約 |
|---|---|---|---|
| 現股 | 2005-03-01 前 | `<5: .01`; `5–<15: .05`; `15–<50: .1`; `50–<150: .5`; `150–<1000: 1`; `≥1000: 5` | `TW_PRICE_RULE_CONTRACT_VERSION=3` |
| 現股 | 2005-03-01 起 | `<10: .01`; `10–<50: .05`; `50–<100: .1`; `100–<500: .5`; `500–<1000: 1`; `≥1000: 5` | 同上 |
| 興櫃股票 | 2020-03-22 前 | 不分股價，`.01`；需交易當日仍為興櫃的正式證據 | 同上 |
| 興櫃股票 | 2020-03-23 起 | 同現股六級距 | 同上 |
| ETF 現貨，含外幣加掛 ETF | 本專案覆蓋的 ETF 期間 | `<50: .01`; `≥50: .05`，單位是該證券的交易幣別 | `TW_ORDER_PRICE_CONTRACT_VERSION=2` |
| REIT | 2006-03-06 起 | 同 ETF 格點；此前按一般股票格點 | 同上 |
| ETN | 本專案涵蓋的 ETN 期間 | 同 ETF 格點 | 同上 |
| 權證 | 2005-03-01 前後 | 依當時權證專屬的價格級距，見 `tw_price_rules.py` | 同上 |
| 可轉換公司債 | 1997 年 8 月調整起；本專案實際資料自其後才開始 | `<150: .05`; `150–<1000: 1`; `≥1000: 5` | 同上 |
| 股票期貨 | 2010-01-25–2026-07-05 | `<10: .01`; `10–<50: .05`; `50–<100: .1`; `100–<500: .5`; `500–<1000: 1`; `≥1000: 5` | `TW_DERIVATIVE_PRICE_CONTRACT_VERSION=2` |
| 股票期貨 | 2026-07-06 起 | `<10: .01`; `10–<50: .05`; `50–<100: .1`; `100–<500: .5`; `500–<2500: 1`; `≥2500: 5` | 同上 |
| ETF 期貨 | 2014-10-06 起 | `<50: .01`; `≥50: .05` | 同上 |
| TX/MTX/TMF 單式期貨 | 已核對的合約期間 | `1` 指數點 | 同上 |
| TXO 一般交易權利金 | 已核對的合約期間 | `<10: .1`; `10–<50: .5`; `50–<500: 1`; `500–<1000: 5`; `≥1000: 10` | 同上 |
| TXO 鉅額交易權利金 | 2019-05-27 起 | `.1` 指數點，與一般交易不同 | 同上 |
| TEO 權利金 | 2025-12-08 前後 | 前：`<.5: .005`, `.5–<2.5: .025`, `2.5–<25: .05`, `25–<50: .25`, `≥50: .5`；後：`<2: .02`, `2–<10: .1`, `10–<100: .2`, `100–<200: 1`, `≥200: 2` | 同上 |
| TFO 權利金 | 已核對的合約期間 | 同 TEO 新制五級距 | 同上 |
| 以新臺幣報價的股票及 ETF 選擇權權利金 | 已核對的合約期間 | `<5: .01`; `5–<15: .05`; `15–<50: .1`; `50–<150: .5`; `150–<1000: 1`; `≥1000: 5` | 同上；外幣商品另查 |

**其他已查到但不屬上述一般市場單式報價的變動：** [證交所 2008-04-14 鉅額交易公告](https://www.twse.com.tw/staticFiles/marketAnnounce/setAnnounce/0970000062.htm)把上下限內的鉅額申報格點改為 `.01` 元；櫃買中心[鉅額交易系統](https://www.tpex.org.tw/zh-tw/mainboard/trading/rules/system.html)也明載限價範圍內 `.01` 元；[期交所股期跨月價差委託](https://www.taifex.com.tw/cht/4/oamIntroduction)同樣可用 `.01`，不能據此把股期單式或一般現股改成 `.01`。本地現有一般市場 OHLC 不包括這些特殊交易方法的獨立逐筆明細，故不對未收錄價格宣稱通過。

2005 現股沿革與目前級距：[證交所交易制度](https://accessibility.twse.com.tw/zh/products/system/trading.html)、[證交所歷史整理](https://wwwc.twse.com.tw/staticFiles/product/publication/twse60/html/184/index.html)。股票期貨開始日、早期規則：[期交所沿革](https://www.taifex.com.tw/cht/1/originOfEstablish)、[期交所股票期貨問答手冊](https://www.taifex.com.tw/file/taifex/CHINESE/10/stf.pdf)。2026 年規則及**正式生效日 7 月 6 日**：[期交所 1150001529 號函與修正對照表](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/%E5%8F%B0%E6%9C%9F%E4%BA%A4%E5%AD%97%E7%AC%AC1150001529%E8%99%9F%E5%87%BD.pdf)。ETF 期貨開始日：[期交所說明](https://www.taifex.com.tw/file/taifex/CHINESE/10/ETF%E6%9C%9F%E8%B2%A8%E4%BB%8B%E7%B4%B9%E6%91%BA%E9%A0%81%E6%96%87%E5%AE%A3.pdf)。[期交所現行規格](https://www.taifex.com.tw/cht/2/sTF)僅作現行交叉檢查。

**範例：** 2026-07-03 的股期報價 `1001` 不在格點；2026-07-06 起是合法股期報價；同日現股 `1001` 仍不合法。股期跨月價差可用不同格點，例如 `.01`，不能用單式股期規則；見[期交所問答](https://www.taifex.com.tw/cht/9/tradersQATrading)。

**日期與交易機制證據：** [證交所 2005 股票／權證調整與 2006 REIT 沿革](https://wwwc.twse.com.tw/staticFiles/product/publication/twse60/html/184/index.html)；[櫃買中心 2020-03-23 興櫃調整](https://www.tpex.org.tw/zh-tw/esb/trading/rules/qa.html)；[2743 上櫃日 2020-03-09](https://www.tpex.org.tw/storage/eb_data/10903/10900013051.html)、[6716 上櫃日 2020-03-27](https://www.tpex.org.tw/storage/eb_data/10903/10900018451.html)；[期交所 TXO 2019 鉅額交易調整](https://www.taifex.com.tw/file/taifex/CHINESE/10/moth/P30-31%20%E8%A6%8F%E7%AB%A0%E5%BF%AB%E6%98%93%E9%80%9A%287%29.pdf)；[期交所 TEO 修改附表](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/%E4%BF%AE%E6%AD%A3%E6%B3%95%E8%A6%8F%E9%99%84%E4%BB%B6.pdf)、[新制實施日](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/4_%E6%B3%95%E8%A6%8F%E7%95%B0%E5%8B%95%E5%BD%99%E6%95%B411411_V2(%E4%BF%AE%E6%AD%A3).pdf)；[TX/MTX/TMF](https://www.taifex.com.tw/cht/2/tMF)、[TXO](https://www.taifex.com.tw/cht/2/tXO)、[股票選擇權](https://www.taifex.com.tw/cht/2/sSO)。2025-12-08 的股票週選擇權增掛屬**到期序列**變更，不能推論一般權利金格點也變了；履約價間距與權利金格點是兩個量綱。零股股價在[證交所現行說明](https://www.twse.com.tw/zh/products/system/trading.html)明訂同一般交易格點，因此逐筆擷取的零股 OHLC／報價可驗證數值格點；其撮合時間、交易量及回測執行仍須另外處理。鉅額、議價、組合／價差、結算價格不得逕用單式一般交易規則。

**官方文件本身也可能衝突。** 證交所 60 週年文字把早期 ETF／REIT 分界寫成「10 元」，但 [2003 年生效的 ETF 買賣辦法第 6 條](https://twse-regulation.twse.com.tw/TW/law/DAT0601_print.aspx?FLCODE=FL007114&FLDATE=20030516&LSER=001)和 [2006 年 3 月 6 日生效的 REIT 修法原文及舊條文](https://www.twse.com.tw/downloads/zh/announcement/download/market/950113-0950200080-1.pdf)都明訂「50 元」。程式採當時生效的規範原文，不採紀念刊物的回顧敘述；這是來源優先序與實際日期的判斷，不能只因為稽核零異常就省略查證。

**興櫃錯分與訓練修復：** 先以股票規則掃描 2020-03-02 至 2020-03-20 共 15 個分鐘分區，原有 566 個看似違例的 OHLC 值。逐檔追查為 `6716` 476 個（15 日）、`2743` 90 個（5 日）。兩檔當時已在券商資料中標示 `tpex`，但交易所正式上櫃分別是 3 月 27 日與 3 月 9 日；上櫃前採興櫃 0.01 元規則。原始價格未修改。另一個獨立問題是普通上市櫃策略資格：上櫃前的 6716 分鐘來源共 306 列，`feature_valid` 13 列、`label_valid_1m` 59 列，兩者同時有效 **2 列**；2743 有 53 列，兩者同時有效 0 列。`stockagent/data/tw_listing_admission.py` 現以公告日期排除兩檔上櫃前的普通市場特徵與成交。`tw_minute` schema 5 載入器將其特徵、標籤、日終狀態、當日日頻上下文與引導權重遮罩，normalizer 從來源 manifest 的彙總統計扣除相關有效列；日頻 `tw_day_trade` 的 schema 4 分鐘執行帶也在載入時遮罩。研究策略評分與回測使用同一日期表。原始 Parquet 與來源 manifest 不改寫；分鐘模型資料指紋、日頻分鐘執行快取及 checkpoint／成品契約加入資格版本。舊 checkpoint 不能直接沿用，須使用新 artifact root 重新訓練並產生新報告。

[證交所雙幣 ETF 制度](https://www.twse.com.tw/zh/products/system/dual-etf/introduction.html)確認外幣加掛 ETF 亦用 `<50: .01`、`≥50: .05`，但單位是自己的交易幣別。代碼第六碼 K/M/S/C 可識別外幣櫃台；不能單靠代碼判定是人民幣或美元。
[2016-03-08 雙幣機制](https://accessibility.twse.com.tw/zh/products/system/dual-etf/introduction.html)及 [2017-04-24 加掛 ETF 交易單位可不同的修法](https://www.twse.com.tw/downloads/zh/announcement/download/market/1060208-10600015981-1.pdf)影響交易幣別及每張受益權單位，不能錯寫成數值 tick 變更；金額與數量換算要依當時商品主檔獨立驗證。

## 資料欄位與稽核邊界

| 資料 | 格點稽核欄位 | 保留公告或計算精度的欄位 | 狀態 |
|---|---|---|---|
| `twse_daily_ohlcv.parquet` | 全檔可辨識股票、ETF、REIT、ETN、權證、TDR、可轉債等開高低收、最後買賣價 | 成交額、成交量、本益比等 | `audit_tw_price_precision.py` 逐欄統計；未知代碼不得暗中跳過 |
| `tpex_daily_ohlcv.parquet` | 已辨識證券類別的開高低收、最後買賣價 | `均價`、`次日參考價`、比率等 | 同上；早期來源異常另列 |
| `taifex_portfolio_daily_v4/continuous_daily.parquet` | 有來源觀察的股期/ETF 期貨開高低收及最後買賣價 | 結算、評價、連續合約回報、合約調整現金 | 收據逐欄統計；建構器也會在寫出前拒絕錯誤報價；填補估值列排除 |
| 官方每檔股票 `stocks/*_features.parquet` | 僅有證交所/櫃買所原始交易報價且未再縮放的列 | `adjclose`、Yahoo 回補後縮放的 OHLC、訓練特徵 | 上游原始報價已查；下游轉換與每檔一致性另需驗證 |
| Shioaji 日 K、分鐘 K、HFT 訓練輸入、留存期貨／選擇權 tick/KBar | 原始 OHLC、成交、Bid/Ask；按商品及交易當日分流 | Amount/Volume 算出的 VWAP、撮合平均、中間價 | `--full` 納入 manifest 選中之分鐘／HFT 全檔及歷史 Tick/KBar；結果以該次收據為準 |
| Shioaji 原始微結構擷取 | 成交與五檔買賣價，含所選擇逐秒快照 | `avg_price`、`price_chg`、`pct_chg` | `audit_tw_capture_price_grid.py` 掃描留存 16 日，不能取代歷史名單 SHA 的完整核對 |
| TAIFEX TX/MTX/TMF 日資料及 TXO 期權全鏈／官方近期 tick | 單式期貨成交及選擇權權利金 | 每日及最後結算價、選擇權履約價、跨月價差另需獨立語義 | 已支援依商品稽核；其他期貨、股選、電子／金融選與早年格點未涵蓋者需另查歷史規範 |
| 股利、基本面、總經、外匯及模型 feature | 不適用報價格點 | 保留來源有效位數與特徵計算精度 | 不做 tick 四捨五入；需另查單位、缺漏及 PIT |

本機另有 **111,823 個** Shioaji 歷史期貨、期權、指數 Tick/KBar Parquet。其股期、ETF 期貨、TXO 依日期與已確認規則查驗；其他一般期貨可拿當下 Contract V2 的商品 `tick` 做**數值交叉檢查**，但這張 2026-09-19 快照沒有歷年生效區間。只要仍以當下快照代替過去規則，歷史檔應標 `historical_rule_unverified`，即使數值全部吻合也不能寫成「已證實所有歷史 tick 均未變」。指數值本身不是期貨成交報價，只做正值及有限數檢查。

`scripts/snapshot_shioaji_tw_tick_rules.py` 的本地 Contract V2 快照在 `artifacts/data_quality/tw_price_precision/shioaji_contract_v2_tick_snapshot.json`，共 414 條現行 FUT/OPT 契約、`problems=0`，由模擬端點取得。這是**當前的商品屬性**，不是逐日歷史版本；股期歷史改制與 TEO 歷史改制仍以交易所公告日為準。

該券商歷史收集器自己的 `summary.json` 狀態仍是 `waiting_source`、`coverage_state=source_gaps`；114,424,998 列現存檔案的格點檢查通過，也不能因此宣稱目標期間完整回填。

另有獨立的 `artifacts/data_quality/tw_price_precision/emerging_admission.json`（schema 4）及 `emerging_admission_v5.json`（schema 5）：各查核正式上櫃日前 19 個分區的內容 SHA。兩份收據都保留來源雙重有效 **2 列**，並依目前訓練資格契約計算有效筆數 **0 列**，狀態 `training_admission_excludes_source_rows`。schema 5 在這 19 天的 normalizer 來源有效列為 2,236,508，新規則扣除 6716 上櫃前的 **13 列**後為 2,236,495；這與只數「特徵和標籤同時有效」的 2 列是不同統計。這只證明已核實兩檔的排除與實際資料遮罩；未知興櫃轉板、其他商品的歷史資格及策略可成交性仍需逐案核實。資格收據與報價格點收據不可互換。

截至本次檢查，schema 4 原始研究資料涵蓋 2020-03-02 至 2026-09-18（1,598 日），schema 5 developing-candle 資料僅至 2026-08-04（1,567 日）。`configs/markets/tw_minute.yaml` 已改指向可載入的 schema 5 及新成品目錄；此修復不等於 schema 5 已追上原始資料，也不等於舊 checkpoint 已重訓。需要截至最新日的分鐘模型時，先用既有 `scripts/upgrade_tw_minute_developing_candles.py --resume` 完成增量升級並重新稽核。

分鐘來源另有既有的 `data_tw_minute/audits/full_latest.json`：同一份 manifest SHA `30bb6b19ff25b38cda59cc615bd7bf2d7502a066e1adadb52e584d2efc86de0a`，2026-09-19 01:22 執行過 1,598 個分區／312,196,881 列的完整來源 SHA 與日曆核對，所有現有分區的 mtime 都比該稽核完成時間早；此次價格稽核另保存來源檔案狀態集合的 SHA。HFT 訓練分區 `trade_date=2026-08-11` 的內容 SHA 也與自己的 manifest `output_sha256` 相符。這些核對分別是內容身分與價格格點證據，不應把檔案 metadata digest 當作逐檔檔案內容 SHA。

稽核只回答列出的交易報價欄位是否符合目前已證實的歷史格點；不能證明資料完整、沒有重複、可於當時取得、成交可執行，或所有台灣欄位的有效位數均正確。修正異常時先核對原始回應與交易所公告，將來源修正、非交易欄位、特殊交易機制分開；重建衍生檔與訓練快取時更新其收據及契約版本，不事後悄悄重寫已用於模型的資料。

## 2026-09-19 本地實測：早期日線收據與擴充後全量收據

| 本地來源／已稽核價格範圍 | 現有最早日至最近日 | 行數或分區；意義 |
|---|---|---|
| TWSE／TPEx 官方日線 | 2004-02-11／2003-08-01 至 2026-09-18 | 5,324,046／4,414,943 列，依原始證券類別分流 |
| 股票／ETF 期貨來源成交日線 | 2010-01-25／2014-10-06 至 2026-09-18 | 1,582,970／64,962 列；估值填補列不視為成交 |
| TX、MTX、TMF 已選日盤近月 | 2005-01-03 至 2026-08-10 | 11,114 列；比最新官方日線較舊，不代表近月覆蓋到今日 |
| TXO 月選／週選原始權利金全鏈 | 2001-12-24／2012-11-21 至 2026-09-17 | 2,349,656／521,546 列；結算價與履約價排除格點測試 |
| 官方 TX／TXO 近期逐筆成交 | 2026-08-10 至 2026-09-18 | 30 個交易日、19,657,200 列；TX 跨月價差另列、不能套 TX 單式格點 |
| 券商訓練分鐘 K 棒 | 2020-03-02 至 2026-09-18 | 1,598 分區、312,196,881 列 |
| HFT 訓練輸入 | 2026-08-11 一日 | 3,240,000 列；不代表原始擷取只存在該日 |
| 券商歷史期貨、選擇權、指數 KBar／Tick | 依 111,823 個檔案的日期與類別，見收據 | 114,424,998 列；指數值與僅以目前契約 tick 核對的期貨不能列入「歷史規則證實」 |

早期 `audit.json` 掃描 TWSE 5,324,046 列、TPEx 4,414,943 列、股期連續日線 2,936,965 列，以及 Shioaji 日 K 2,340 檔、3,199,767 列；依商品與日期在當時指定的原始交易報價欄位檢查，`off_grid_values=0`。股期只計有來源觀察的 1,647,932 列，排除估值填補列。這是擴充全部商品分類與更多資料源**之前**的收據；後續優先查 `full_audit_20260919.json`。這些數字是報價格點檢查，不是所有台股資料已修正的宣告。

2026-07-06 起至 2026-09-18 的現有股期日資料中，有 **2,160 列**來源觀察收盤價位於 1,000–2,500 且不是 5 的倍數；它們符合生效後 1 元 tick。套用現股或舊股期規則會錯誤拒絕這些有效值。

稽核收據另以 `currency_classes` 區分 ETF 外幣櫃台與新臺幣：TWSE 外幣櫃台 12,276 列、TPEx 410 列、Shioaji 日 K 5,605 列。兩者數值格點相同，但幣別不能混用。外幣櫃台的精確幣別仍需逐檔產品主檔，不由代號猜測。

首次稽核發現 TWSE 原始日線 **1 列錯誤日期**：`8070 / 2014-12-;1`，`_url` 指向 2014-12-31，原始 `raw/twse_daily_ohlcv/2014-12-31.json` 有相同證券與 OHLC；當時衍生 `stocks/8070_features.parquet` 亦缺當日。已用既有下載器的 `repair` 模式重新取得該日期；新增的合併規則僅在找到同一證券的官方替代列時移除錯誤列。然後重建 2,755 檔每檔股票資料及全量公開訓練特徵。現核對 `stocks/8070_features.parquet` 已有 2014-12-31 的官方開盤 `69.1`、收盤 `70.0`；`tw_public_stock_daily.parquet` 同日 `8070` 的官方成交股數為 `89,060`。重新執行本稽核的 `--strict` 回傳 0，四組已列出的資料均為 `quote_grid_valid`、`off_grid_values=0`。既有 TW public 資料層稽核以 `--build-panel --strict` 重建了新的 panel 快取；再用 `--strict` 複查，`model_safe=True`，無 critical/high 發現，且新增的 TWSE/TPEx 格點稽核已進入該嚴格流程。舊 checkpoint 不會因此自動更新；使用時須核對新的來源 SHA、panel 與特徵建構收據。

本次後續 `--full` 稽核另列於 `full_audit_20260919.json`，原始擷取收據為 `capture_all_20260919.json` 及 `capture_2026-07-21_ambiguous.json`。留存的 16 日逐筆擷取中，15 日清單與價格格點通過，分別檢查成交 10,193,585 列、五檔事件 52,938,963 列、逐秒五檔 47,059,517 列，報價違例為零；2026-07-21 的舊 schema-2 檔案數量超出雙 worker manifest 宣告，補充收據另掃描成交 1,521,276 列、五檔事件 7,762,044 列、逐秒五檔 6,945,751 列且報價違例零，但標示 `source_provenance_ambiguous`，不能把多出的檔案認作唯一正確來源。舊 `top_200.csv` 歷史版本不在本機，因此逐筆獨立稽核也明標 `historical_universe_sha256_verified=false`。有效位數仍應另外核對股利與財報等欄位的公告單位與原始版本；報價格點合法不能證明這些值正確。

擴充後 v3 全量收據包含 11 組來源：10 組 `quote_grid_valid`，各組指定報價欄位的違例皆為 **0**，包含 TWSE、TPEx 全證券類別 9,738,989 列、股期／ETF 期貨連續日線 2,936,965 列（其中僅 1,647,932 列來源觀察報價受格點檢查）、2020–2026 的 312,196,881 列分鐘 K 棒、2026-08-11 的 3,240,000 列 HFT 輸入、官方 TX／TXO 近期逐筆與完整 TXO 期權全鏈。剩餘券商歷史檔 114,424,998 列中，12,489,165 列一般期貨僅以 **2026 當下契約格點**交叉比對：不符為 0，但缺少歷年逐日版本，因此這一組以及總收據為 `historical_rule_unverified`。另有 88,632,524 列現貨指數 KBar／Tick 屬指數數值，只通過數值領域檢查，不能算期貨報價。全量嚴格指令回傳 2 是這個待證歷史規則的結果，不代表發現違例原始報價；實際完整性還受 `waiting_source/source_gaps` 限制。

修復後 TW public 已經由既有發佈流程生成冷版本 `tw-public-20260918T191833410217217Z-l0-penguin-d1b4e23826c336ab`；`artifacts/data_quality/tw_price_precision/cold_publish.json` 記錄發佈收據，`cold_verify.json` 驗證 2,615 個本機封裝物件、22,829,127,049 bytes 與 manifest/inventory。此項證據僅代表 penguin 本機冷版本有效；跨機同步、D: 備份、其他節點 materialization 與舊 checkpoint 可用性未由這項檢查證明。
