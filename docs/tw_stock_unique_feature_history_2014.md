# 台股訓練特徵去重與 2014 歷史缺口

2026-09-19 增補的 10 個期交所免費歷史原值欄、實際下載狀態與付費缺口見[免費來源下載與訓練接入稽核](tw_free_feature_download_training_audit_2026-09-19.md)；本頁的 266 欄仍是 2026-09-18 七個既有主線配置的去重基準。

資料核對日：2026-09-18（Asia/Taipei）。本文件把[七個主線訓練配置的逐欄清單](tw_stock_training_feature_status_2026-09-18.md)、正式來源表 143 欄、研究來源表 122 欄，以**特徵名稱**去重。七個主線覆蓋目前日當沖、13:25 隔夜、開盤前及同日收盤研究；期貨、加密貨幣與其他舊實驗屬另一個 ABI。名稱相同而來源表不同的欄位在同一列同時列出兩種 2014 筆數；經濟意義相近但公式、單位或時鐘不同的欄位仍分列。

**「2014 有一格」只證明本機有某個 2014 資料日期的值；不證明從 2014 起逐日／逐期／全公司完整，也不證明當年可見的初版或開盤前可用。**這份清單是下載優先順序與研究候選，不能取代 PIT／模型安全稽核。

## 從第一性原理決定補什麼

原始發布檔與其日期、版次是獨立證據；對數、年增率、比率、事件旗標和可用性旗標應從原值重算。因此先補原始檔，再產生衍生欄。若正式表 2014 缺值，但研究表保有同名現值，先完成資料血緣、公式及時鐘對齊。對每個 2014 缺口，還要分清四件事：公開查詢是否有值、能否合法批次下載、是否保存原始發布版本、能否在指定決策時鐘使用。

### 官方歷史入口與既有下載器的接法

| 路線 | 對應資料及已查到的歷史入口 | 實作路徑與可驗收條件 |
|---|---|---|
| P1 | 證交所／櫃買中心每日行情、估值、法人與資券；[TWSE OpenAPI](https://openapi.twse.com.tw/)列出的多是當期表，歷史回查應用交易所逐日查詢頁，不能把 OpenAPI 當成全歷史 API。 | 沿用 downloader/download_tw_public_data.py 的逐日資料集、state／raw 收據與日曆缺日稽核；以市場、日期、代號去重，只補缺日或新版。 |
| P2 | 股票 OHLCV 及其公式欄。 | 從已存股票面板原始行情重算並逐欄核 2014 有效格；不要從 2026 特徵表反推 2014 公式。 |
| C1 | [央行 2014 每日新臺幣／美元匯率](https://www.cbc.gov.tw/tw/cp-2151-45403-0C615-1.html)確有逐日歷史頁；[央行金融指標歷史檔](https://www.cbc.gov.tw/tw/cp-523-995-CC8D8-1.html)及[2014 當期 M1B/M2 新聞稿](https://www.cbc.gov.tw/tw/cp-302-43692-46092-1.html)提供不同版本證據。 | 已有 downloader/download_tw_cbc_fx_annual_pages.py、download_tw_cbc_money_release_archive.py、download_tw_cbc_overnight_pages.py；從原始頁／原稿抽值，研究用途可用現修值，但保留原稿與資料日期。匯率當日下午才發布，09:00 使用需下一交易日。 |
| G1 | [主計總處物價時間序列](https://www.stat.gov.tw/cp.aspx?n=2665&s=2539)及[原始新聞稿](https://www.stat.gov.tw/News.aspx?n=2668&sms=11023)供 CPI、失業與 GDP。 | 沿用 downloader/download_tw_dgbas_release_archive.py 補逐期原稿；研究原值先驗公式、基期、修訂與公告時鐘。若官方限速，保留缺期。 |
| F1 | 財政部貿易、稅收的逐期原稿及統計長表，另須檢查跨 2015/2016 制度的口徑；[國貿署資料說明](https://publicinfo.trade.gov.tw/cuswebo/FSC3000F/FSC3000)明示新舊制差異。 | 沿用 downloader/download_tw_mof_release_archive.py、download_tw_mof_trade_release_values.py；對照每月原稿數值及單位。研究表原值已有 2014，正式表零值對數欄不表示原始數字缺失。 |
| M1 | [MOPS 歷史月營收、重大訊息](https://mops.twse.com.tw/mops/web/t120sb02_q10)可按年／月／公司查；重大訊息有公告時間。 | 從官方逐期公告建 raw 分區與版本收據，按公司、期別、公告版次合併；衍生 MoM／YoY 需上一年／月值。現有 OpenAPI 當期快照只能向前保存。先確認站方允許的批次方式與請求限制。 |
| M2 | [MOPS XBRL 整批下載](https://mopsov.twse.com.tw/mops/web/t203sb02)提供已結束季度 ZIP；本機已匯入歷史季度。 | 沿用 downloader/download_tw_mops_xbrl.py、scripts/import_tw_mops_xbrl_local.py 與 scripts/build_tw_mops_publication_candidates.py。逐公司、季別、報表制式及修訂版映射；缺的舊式毛利／營業利益欄先驗 TIFRS 對應概念，不能改名後直接當同一會計項目。自動批次下載須遵守[證交所網站使用條款](https://www.twse.com.tw/zh/terms/use.html)與既有授權門檻。 |
| M3 | [MOPS 公司歷年變更登記、董監持股、質押、內部人轉讓](https://mops.twse.com.tw/mops/web/t120sb02_q10)有分項歷史查詢。 | 對每個屬性找歷史生效日與當年公告，而不是將今日公司快照倒填；若只有當期快照，2014 PIT 標無法證明。 |
| E1 | [TWSE 注意公告歷史查詢](https://www.twse.com.tw/zh/announcement/notice.html)、[TWSE 處置歷史查詢](https://wwwc.twse.com.tw/zh/announcement/punish.html)均標示自 2001-01 提供並可匯出 CSV；櫃買公告另核。[TWSE 注意與處置 CSV 商品](https://eshop.twse.com.tw/zh/product/detail/ac35dc52883d42c1a2eeacfa332f7e7f)從 2006-04-01 起但為付費批次。 | 先抽樣確認免費歷史查詢的 2014 日期／上市櫃覆蓋、列數、結束日期與公告時刻，再決定合法增量抓取；批量完整性不能由某天零事件推定。付費 CSV 是完整批次備案。 |
| E2 | [TWSE 除權息計算結果](https://www.twse.com.tw/zh/announcement/ex-right/twt49u.html)自 2003-05-05 提供；[MOPS 股利與除權息公告](https://mops.twse.com.tw/mops/web/t120sb02_q10)可查議案／決議歷史。 | 除權日條款可先核交易所歷史；董事會提案、股東會通過和修訂需 MOPS 各版公告。把已知日、除權日及派息日分開。 |
| S1 | [TWSE 當日融券賣出與借券賣出成交量值](https://wwwc.twse.com.tw/zh/trading/historical/twtasu.html)自 2008-09-26 提供；[借券欄位定義](https://www.twse.com.tw/zh/products/sbl/disclosures/info.html)區分借券餘額與借券賣出餘額。 | 逐日驗證 SBL 餘額、可借券賣出額與借券成交的欄位語義及上市櫃覆蓋；欄位不等價時不得互補。 |
| T1 | [期交所期貨每日／年度行情](https://www.taifex.com.tw/cht/3/futDailyMarketView)、[選擇權每日／年度行情](https://www.taifex.com.tw/cht/3/optDailyMarketView)：日區間至多一個月，年度 ZIP 可下載。 | 沿用 scripts/run_taifex_public_history.sh 和正式來源建表，按年度 ZIP 補 2014 起的 TX／TXO 原始量、OI、結算價與到期合約；合約月份與日夜盤交易日期要固定。 |
| T2 | [期交所三大法人分契約資料](https://www.taifex.com.tw/cht/3/futContractsDateView)從 2008-04-07 起，但免費頁只開放近三年，早期須申購；大額交易人資料亦需逐項核官方期限。 | 現有 2026 快照不能變 2014。向期交所確認歷史商品、價金與使用權後匯入原始檔；未取得前 2014 列標無法免費驗證。 |
| D1 | [集保股權分散表](https://original-www.tdcc.com.tw/portal/zh/smWeb/qryStock)說明歷史檔自 2008-07 建置，但**網站僅保存一年**；[TDCC OpenAPI](https://openapi.tdcc.com.tw/)是當期開放資料入口。 | 現有快照從首次觀測日增量保存；2014 原始週檔須向集保確認是否供歷史申請／授權，不能以當期股東比例回填。 |
| I1 | 個股 13:25 真實成交價：本機分鐘來源最早 2020-03-02；[證交所資訊商店](https://www.twse.com.tw/zh/products/dataeshop.html)有歷史交易資料申購入口。 | 先核 2014 逐筆／分鐘商品的可取得性、時戳粒度、成交量與授權，再聚合右標 13:25 價；沒有真實來源時，2014–2020-02 應維持缺值或縮短訓練期，同日最終收盤價僅能標研究代理。 |

原始下載只能寫 catalog 指定的 mutable tw-public producer 目錄；完成來源收據、缺期、schema、PIT 與模型稽核後，沿既有發布流程進冷庫。查詢頁、原始 ZIP、Parquet、模型輸入是不同層。批量下載之前，先用 2014 一期／一交易日做格式與授權驗證，並以現有 state/receipt 只補缺期。

**執行順序：**先將「僅研究表已有 2014」的同名欄及「原值可重算」的候選原值完成映射和公式驗證；再用免費且有 2014 歷史的 C1、T1、E1、E2、S1 入口補原始缺期；之後處理 MOPS 的公告版次與 XBRL taxonomy；最後單獨評估 T2、D1、I1 的付費或不可免費取得部分。對於事件欄，要驗來源日期範圍的「全日已查」收據，不能因特徵為空就回填零。

## 全部不重複特徵及 2014 判讀

<!-- BEGIN GENERATED UNIQUE FEATURE HISTORY -->
資料快照合併：**266 個不重複名稱**；七個主線配置選入 232 個不重複通道，另有 34 個僅來源表欄位。

| 分類 | 名稱數 | 判讀 |
|---|---:|---|
| 正式表已有 2014 | 58 | 正式來源表 2014 有至少一格有限值 |
| 僅研究表已有 2014 | 44 | 正式表 2014 零格，研究表有歷史現值 |
| 原值可重算 | 15 | 同名衍生欄無 2014，但研究表另一原值欄可作候選 |
| 待回補 | 78 | 兩張來源表及候選原值均未證明 2014 有值 |
| 股票面板 | 21 | 由股票面板原值或公式產生，未逐欄核 2014 |
| 旗標 | 50 | 可用性指標是衍生通道，歷史性取決於基礎值 |

真正待回補的 78 個名稱依來源路線分布：D1 3、E1 8、E2 11、I1 1、M1 7、M2 17、M3 12、S1 3、T1 10、T2 6。一份原始資料可供多個衍生特徵，因此名稱數不是下載檔案數。

**口徑：**2014 數字是當年來源表有限值格數，不是完整交易日、公司覆蓋、舊版原稿或決策時刻證明。稀疏事件的整年零值表示本機沒有觀測，不能直接解讀成全年沒有事件。`2015–2025 零值年`只抓整年零格；逐日、逐公司和月季缺口仍需來源稽核。兩表同名只列一次，正式與研究數字放在同一列。

模式代碼：D=線上日當沖 fold 11／正式日當沖，O=13:25 隔夜研究，S=嚴格開盤前，R=2014 原值開盤前，W=2014 寬覆蓋研究，C=同日收盤研究；D 合併兩個 ABI 相同的模式，但該欄如只在其中一個配置出現仍會分別標記。`—` 代表來源表中沒有此欄。

### 正式表已有 2014（58）

| 特徵 | 家族 | 配置 | 首日 | 最新 | 正式表 2014 格 | 研究表 2014 格 | 2015–2025 零值年 | 判讀 | 路線 |
|---|---|---|---|---|---:|---:|---|---|---|
| `twpub_dgbas_gdp_yoy` | 主計總處 GDP | D線、D訓、W、C | 2012-02-01 | 2026-09-17 | 8 | 8 | 無整年零值 | 正式表數值有 2014 | G1 |
| `twpub_dgbas_gdp_yoy_pct_raw` | 主計總處 GDP | R、W | 2012-02-01 | 2026-08-17 | 8 | 8 | 無整年零值 | 正式表數值有 2014 | G1 |
| `twpub_dgbas_unemployment_pct_raw` | 主計總處失業 | R、W | 2005-03-01 | 2026-08-25 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | G1 |
| `twpub_dgbas_unemployment_rate` | 主計總處失業 | D線、D訓、W、C | 2005-03-01 | 2026-09-17 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | G1 |
| `twpub_dgbas_cpi_yoy` | 主計總處物價 | D線、D訓、W、C | 2004-02-06 | 2026-09-17 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | G1 |
| `twpub_dgbas_cpi_yoy_pct_raw` | 主計總處物價 | R、W | 2004-02-06 | 2026-09-09 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | G1 |
| `twpub_pb_log` | 估值 | D線、D訓、C | 2003-08-01 | 2026-09-18 | 373,660 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_pb_raw` | 估值 | R、W | 2003-08-01 | 2026-09-18 | 373,660 | 373,660 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_pe_log` | 估值 | D線、D訓、C | 2003-08-01 | 2026-09-18 | 283,953 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_pe_raw` | 估值 | R、W | 2003-08-01 | 2026-09-18 | 283,953 | 283,953 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_twse_taiex_log` | 加權指數 | D線、D訓、C | 1999-01-05 | 2026-09-18 | 248 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_twse_taiex_logret_1d` | 加權指數 | D線、D訓、C | 1999-01-06 | 2026-09-18 | 248 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_twse_taiex_pct` | 加權指數 | D線、D訓、W、C | 1999-01-06 | 2026-09-18 | 248 | 248 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_twse_taiex_raw` | 加權指數 | R、W | 1999-01-05 | 2026-09-18 | 248 | 248 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_cbc_fx_reserves_chg` | 央行外匯存底 | D線、D訓、S、C | 2000-06-08 | 2026-09-17 | 12 | — | 無整年零值 | 正式表數值有 2014 | C1 |
| `twpub_cbc_fx_reserves_log` | 央行外匯存底 | D線、D訓、S、C | 2000-05-30 | 2026-09-17 | 12 | — | 無整年零值 | 正式表數值有 2014 | C1 |
| `twpub_cbc_fx_reserves_usd_billion_raw` | 央行外匯存底 | R、W | 2000-05-30 | 2026-09-17 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | C1 |
| `twpub_cbc_m1b_yoy` | 央行貨幣 | D線、D訓、W、C | 2000-01-26 | 2026-09-17 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | C1 |
| `twpub_cbc_m1b_yoy_pct_raw` | 央行貨幣 | R、W | 2000-01-26 | 2026-09-17 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | C1 |
| `twpub_cbc_m2_yoy` | 央行貨幣 | D線、D訓、W、C | 2000-01-26 | 2026-09-17 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | C1 |
| `twpub_cbc_m2_yoy_pct_raw` | 央行貨幣 | R、W | 2000-01-26 | 2026-09-17 | 12 | 12 | 無整年零值 | 正式表數值有 2014 | C1 |
| `twpub_official_close_logret_1d` | 官方日行情 | D線、D訓、O、S、C | 2003-08-04 | 2026-09-18 | 374,739 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_close_to_high` | 官方日行情 | D線、D訓、O、S、W、C | 2003-08-01 | 2026-09-18 | 377,121 | 377,121 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_close_to_low` | 官方日行情 | D線、D訓、O、S、W、C | 2003-08-01 | 2026-09-18 | 377,121 | 377,121 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_intraday_range` | 官方日行情 | D線、D訓、O、S、W、C | 2003-08-01 | 2026-09-18 | 377,121 | 377,121 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_trades_log` | 官方日行情 | D線、D訓、O、S、C | 2003-08-01 | 2026-09-18 | 377,600 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_trades_raw` | 官方日行情 | R、W | 2003-08-01 | 2026-09-18 | 381,813 | 381,813 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_trading_value_log` | 官方日行情 | D線、D訓、O、S、C | 2003-08-01 | 2026-09-18 | 377,600 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_trading_value_raw` | 官方日行情 | R、W | 2003-08-01 | 2026-09-18 | 381,813 | 381,813 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_trading_volume_log` | 官方日行情 | D線、D訓、O、S、C | 2003-08-01 | 2026-09-18 | 377,600 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_trading_volume_raw` | 官方日行情 | 僅來源 | 2003-08-01 | 2026-09-18 | 381,813 | 381,813 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_official_turnover_ratio` | 官方日行情 | D線、D訓、O、S、W、C | 2004-10-28 | 2026-09-18 | 165,363 | 165,363 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_dealer_net_buy_flow` | 法人 | D線、D訓、S、C | 2004-06-02 | 2026-09-18 | 302,815 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_dealer_net_buy_shares_raw` | 法人 | R、W | 2004-06-02 | 2026-09-18 | 302,815 | 302,815 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_foreign_net_buy_flow` | 法人 | D線、D訓、C | 2004-06-02 | 2026-09-18 | 302,815 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_foreign_net_buy_shares_raw` | 法人 | R、W | 2004-06-02 | 2026-09-18 | 302,815 | 302,815 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_institutional_net_buy_flow` | 法人 | D線、D訓、C | 2004-06-02 | 2026-09-18 | 301,999 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_institutional_net_buy_shares_raw` | 法人 | R、W | 2004-06-02 | 2026-09-18 | 301,999 | 301,999 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_investment_trust_net_buy_flow` | 法人 | D線、D訓、S、C | 2004-06-02 | 2026-09-18 | 302,815 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_investment_trust_net_buy_shares_raw` | 法人 | R、W | 2004-06-02 | 2026-09-18 | 302,815 | 302,815 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_dividend_per_share_log` | 股利 | D線、D訓、C | 2007-01-02 | 2026-09-18 | 104,052 | — | 無整年零值 | 正式表數值有 2014 | E2 |
| `twpub_dividend_per_share_raw` | 股利 | W | 2007-01-02 | 2026-09-18 | 164,313 | 164,313 | 無整年零值 | 正式表數值有 2014 | E2 |
| `twpub_dividend_yield` | 股利 | D線、D訓、W、C | 2003-08-01 | 2026-09-18 | 373,674 | 373,674 | 無整年零值 | 正式表數值有 2014 | E2 |
| `twpub_dividend_yield_pct_raw` | 股利 | R、W | 2003-08-01 | 2026-09-18 | 373,674 | 373,674 | 無整年零值 | 正式表數值有 2014 | E2 |
| `twpub_margin_balance_chg` | 資券 | D線、D訓、S、C | 2001-01-03 | 2026-09-18 | 350,872 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_margin_balance_log` | 資券 | D線、D訓、S、C | 2001-01-03 | 2026-09-18 | 321,948 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_margin_balance_lots_raw` | 資券 | R、W | 2001-01-03 | 2026-09-18 | 350,872 | 350,872 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_margin_buy_flow` | 資券 | D線、D訓、S、C | 2001-01-03 | 2026-09-18 | 350,872 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_margin_buy_lots_raw` | 資券 | R、W | 2001-01-03 | 2026-09-18 | 350,872 | 350,872 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_margin_sell_flow` | 資券 | D線、D訓、S、C | 2001-01-03 | 2026-09-18 | 350,872 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_margin_sell_lots_raw` | 資券 | R、W | 2001-01-03 | 2026-09-18 | 350,872 | 350,872 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_short_balance_chg` | 資券 | D線、D訓、S、C | 2001-01-03 | 2026-09-18 | 350,872 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_short_balance_log` | 資券 | D線、D訓、S、C | 2001-01-03 | 2026-09-18 | 258,326 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_short_balance_lots_raw` | 資券 | R、W | 2001-01-03 | 2026-09-18 | 350,872 | 350,872 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_short_buy_flow` | 資券 | D線、D訓、S、C | 2001-01-03 | 2026-09-18 | 350,872 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_short_buy_lots_raw` | 資券 | R、W | 2001-01-03 | 2026-09-18 | 350,872 | 350,872 | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_short_sell_flow` | 資券 | D線、D訓、S、C | 2001-01-03 | 2026-09-18 | 350,872 | — | 無整年零值 | 正式表數值有 2014 | P1 |
| `twpub_short_sell_lots_raw` | 資券 | R、W | 2001-01-03 | 2026-09-18 | 350,872 | 350,872 | 無整年零值 | 正式表數值有 2014 | P1 |

### 僅研究表已有 2014（44）

| 特徵 | 家族 | 配置 | 首日 | 最新 | 正式表 2014 格 | 研究表 2014 格 | 2015–2025 零值年 | 判讀 | 路線 |
|---|---|---|---|---|---:|---:|---|---|---|
| `twpub_xbrl_assets_twd_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 7,093 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_basic_eps_quarter_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 4,642 | 2018 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_basic_eps_ytd_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 6,941 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_cash_twd_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 7,091 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_current_assets_twd_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2026-09-02 | — | 6,802 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_current_liabilities_twd_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2026-09-02 | — | 6,803 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_current_ratio` | MOPS XBRL 財報 | W | 2013-04-18 | 2026-09-02 | — | 6,802 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_debt_ratio` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 7,093 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_equity_ratio` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 7,093 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_equity_twd_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 7,123 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_inventories_twd_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2026-09-02 | — | 6,403 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_inventory_to_assets_ratio` | MOPS XBRL 財報 | W | 2013-04-18 | 2026-09-02 | — | 6,403 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_liabilities_twd_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 7,093 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_net_margin` | MOPS XBRL 財報 | W | 2013-04-25 | 2026-09-02 | — | 33 | 2018 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_operating_cash_flow_twd_quarter_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-07-20 | — | 1,566 | 2018 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_operating_cash_flow_twd_ytd_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 7,092 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_profit_twd_quarter_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 4,735 | 2018 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_profit_twd_ytd_raw` | MOPS XBRL 財報 | W | 2013-04-17 | 2026-09-02 | — | 7,122 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_revenue_twd_quarter_raw` | MOPS XBRL 財報 | W | 2013-04-25 | 2026-09-02 | — | 33 | 2018 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_revenue_twd_ytd_raw` | MOPS XBRL 財報 | W | 2013-04-25 | 2026-09-02 | — | 53 | 無整年零值 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_gross_margin` | MOPS XBRL 財報 | W | 2013-04-18 | 2017-12-04 | — | 4,481 | 2018–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_gross_profit_twd_quarter_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2019-03-19 | — | 4,489 | 2020–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_gross_profit_twd_ytd_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2019-08-12 | — | 6,596 | 2020–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_net_gross_margin` | MOPS XBRL 財報 | W | 2013-04-18 | 2017-12-04 | — | 4,481 | 2018–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_net_gross_profit_twd_quarter_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2017-12-04 | — | 4,489 | 2018–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_net_gross_profit_twd_ytd_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2018-07-27 | — | 6,596 | 2019–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_net_operating_income_twd_quarter_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2017-12-04 | — | 4,490 | 2018–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_net_operating_income_twd_ytd_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2018-07-27 | — | 6,606 | 2019–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_operating_margin` | MOPS XBRL 財報 | W | 2013-04-18 | 2017-12-04 | — | 4,481 | 2018–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_operating_revenue_twd_quarter_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2017-12-04 | — | 4,489 | 2018–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw` | MOPS XBRL 財報 | W | 2013-04-18 | 2018-07-27 | — | 6,596 | 2019–2025 | 研究現值有 2014；正式版未納 | M2 |
| `twpub_dgbas_gdp_raw` | 主計總處 GDP | W | 2013-01-31 | 2026-08-03 | — | 4 | 無整年零值 | 研究現值有 2014；正式版未納 | G1 |
| `twpub_dgbas_cpi_raw` | 主計總處物價 | W | 2013-01-07 | 2026-09-09 | — | 12 | 無整年零值 | 研究現值有 2014；正式版未納 | G1 |
| `twpub_usdtwd_raw` | 匯率 | W | 2013-01-02 | 2026-09-18 | 0 | 248 | 無整年零值 | 研究現值有 2014；正式版未納 | C1 |
| `twpub_cbc_overnight_pct_raw` | 央行利率 | W | 2013-01-02 | 2026-09-18 | — | 248 | 無整年零值 | 研究現值有 2014；正式版未納 | C1 |
| `twpub_cbc_m1b_raw` | 央行貨幣 | W | 2013-01-28 | 2026-09-17 | 0 | 12 | 無整年零值 | 研究現值有 2014；正式版未納 | C1 |
| `twpub_cbc_m2_raw` | 央行貨幣 | W | 2013-01-28 | 2026-09-17 | 0 | 12 | 無整年零值 | 研究現值有 2014；正式版未納 | C1 |
| `twpub_mof_business_tax_raw` | 財政部貿易／稅收 | W | 2013-01-14 | 2026-09-14 | — | 12 | 無整年零值 | 研究現值有 2014；正式版未納 | F1 |
| `twpub_mof_export_raw` | 財政部貿易／稅收 | W | 2014-02-10 | 2026-09-10 | — | 11 | 無整年零值 | 研究現值有 2014；正式版未納 | F1 |
| `twpub_mof_futures_tax_raw` | 財政部貿易／稅收 | W | 2013-01-14 | 2026-09-14 | — | 12 | 無整年零值 | 研究現值有 2014；正式版未納 | F1 |
| `twpub_mof_import_raw` | 財政部貿易／稅收 | W | 2014-02-10 | 2026-09-10 | — | 11 | 無整年零值 | 研究現值有 2014；正式版未納 | F1 |
| `twpub_mof_securities_tax_raw` | 財政部貿易／稅收 | W | 2013-01-14 | 2026-09-14 | — | 12 | 無整年零值 | 研究現值有 2014；正式版未納 | F1 |
| `twpub_mof_tax_total_raw` | 財政部貿易／稅收 | W | 2013-01-14 | 2026-09-14 | — | 12 | 無整年零值 | 研究現值有 2014；正式版未納 | F1 |
| `twpub_mof_trade_balance_raw` | 財政部貿易／稅收 | W | 2014-02-10 | 2026-09-10 | — | 11 | 無整年零值 | 研究現值有 2014；正式版未納 | F1 |

### 原值可重算（15）

| 特徵 | 家族 | 配置 | 首日 | 最新 | 正式表 2014 格 | 研究表 2014 格 | 2015–2025 零值年 | 判讀 | 路線 |
|---|---|---|---|---|---:|---:|---|---|---|
| `twpub_dgbas_gdp_log` | 主計總處 GDP | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_dgbas_gdp_raw）；待驗公式 | G1 |
| `twpub_dgbas_cpi_log` | 主計總處物價 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_dgbas_cpi_raw）；待驗公式 | G1 |
| `twpub_usdtwd_log` | 匯率 | D線、D訓、C | 2026-09-17 | 2026-09-18 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_usdtwd_raw）；待驗公式 | C1 |
| `twpub_usdtwd_logret_1d` | 匯率 | D線、D訓、C | 2026-09-18 | 2026-09-18 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_usdtwd_raw）；待驗公式 | C1 |
| `twpub_cbc_overnight_rate` | 央行利率 | D線、D訓、C | 2026-09-17 | 2026-09-18 | 0 | 0 | 2014–2025 | 候選原值有 2014（twpub_cbc_overnight_pct_raw）；待驗公式 | C1 |
| `twpub_cbc_overnight_rate_chg` | 央行利率 | D線、D訓、C | — | — | 0 | 0 | 2014–2025 | 候選原值有 2014（twpub_cbc_overnight_pct_raw）；待驗公式 | C1 |
| `twpub_cbc_m1b_log` | 央行貨幣 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_cbc_m1b_raw）；待驗公式 | C1 |
| `twpub_cbc_m2_log` | 央行貨幣 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_cbc_m2_raw）；待驗公式 | C1 |
| `twpub_mof_business_tax_log` | 財政部貿易／稅收 | D線、D訓、C | — | — | 0 | — | 2014–2025 | 候選原值有 2014（twpub_mof_business_tax_raw）；待驗公式 | F1 |
| `twpub_mof_export_log` | 財政部貿易／稅收 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_mof_export_raw）；待驗公式 | F1 |
| `twpub_mof_futures_tax_log` | 財政部貿易／稅收 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_mof_futures_tax_raw）；待驗公式 | F1 |
| `twpub_mof_import_log` | 財政部貿易／稅收 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_mof_import_raw）；待驗公式 | F1 |
| `twpub_mof_securities_tax_log` | 財政部貿易／稅收 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_mof_securities_tax_raw）；待驗公式 | F1 |
| `twpub_mof_tax_total_log` | 財政部貿易／稅收 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_mof_tax_total_raw）；待驗公式 | F1 |
| `twpub_mof_trade_balance_asinh` | 財政部貿易／稅收 | D線、D訓、C | 2026-09-17 | 2026-09-17 | 0 | — | 2014–2025 | 候選原值有 2014（twpub_mof_trade_balance_raw）；待驗公式 | F1 |

### 待回補（78）

| 特徵 | 家族 | 配置 | 首日 | 最新 | 正式表 2014 格 | 研究表 2014 格 | 2015–2025 零值年 | 判讀 | 路線 |
|---|---|---|---|---|---:|---:|---|---|---|
| `next_session_1325_gap_logret` | 13:25 分鐘價差 | O | 2020-03-02 | 2026-09-18 | — | — | 2014–2019；2020 部分 | 分鐘價自 2020-03-02；2014 缺 | I1 |
| `twpub_xbrl_gross_margin` | MOPS XBRL 財報 | W | 2019-04-17 | 2026-09-02 | — | 0 | 2014–2018 | 兩表均無 2014 觀測 | M2 |
| `twpub_xbrl_gross_profit_twd_quarter_raw` | MOPS XBRL 財報 | W | 2019-04-17 | 2026-09-02 | — | 0 | 2014–2018 | 兩表均無 2014 觀測 | M2 |
| `twpub_xbrl_gross_profit_twd_ytd_raw` | MOPS XBRL 財報 | W | 2019-04-17 | 2026-09-02 | — | 0 | 2014–2018 | 兩表均無 2014 觀測 | M2 |
| `twpub_xbrl_operating_margin` | MOPS XBRL 財報 | W | 2019-04-17 | 2026-09-02 | — | 0 | 2014–2018 | 兩表均無 2014 觀測 | M2 |
| `twpub_xbrl_operating_profit_twd_quarter_raw` | MOPS XBRL 財報 | W | 2019-04-17 | 2026-09-02 | — | 0 | 2014–2018 | 兩表均無 2014 觀測 | M2 |
| `twpub_xbrl_operating_profit_twd_ytd_raw` | MOPS XBRL 財報 | W | 2019-04-17 | 2026-09-02 | — | 0 | 2014–2018 | 兩表均無 2014 觀測 | M2 |
| `twpub_cumulative_revenue_yoy` | MOPS 月營收／重大訊息 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M1 |
| `twpub_monthly_revenue_log` | MOPS 月營收／重大訊息 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M1 |
| `twpub_monthly_revenue_mom` | MOPS 月營收／重大訊息 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M1 |
| `twpub_monthly_revenue_yoy` | MOPS 月營收／重大訊息 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M1 |
| `twpub_borrow_available_log` | 借券／賣空 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | S1 |
| `twpub_sbl_balance_log` | 借券／賣空 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | S1 |
| `twpub_company_age_years` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_company_has_preferred_stock` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_company_industry_code` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_company_is_foreign` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_company_issued_shares_log` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_company_listed_age_years` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_company_paidin_capital_log` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_company_par_value_log` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_company_private_shares_log` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_insider_holdings_log` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_insider_pledge_ratio` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_insider_transfer_shares_log` | 公司／內部人 | 僅來源 | 2026-07-20 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M3 |
| `twpub_taifex_dealer_net_oi_asinh` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T2 |
| `twpub_taifex_foreign_net_oi_asinh` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T2 |
| `twpub_taifex_trust_net_oi_asinh` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T2 |
| `twpub_taifex_tx_final_settlement_logret` | 期交所 | D線、D訓、C | 2025-11-12 | 2025-12-03 | 0 | — | 2014–2024 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_tx_large_oi_log` | 期交所 | D線、D訓、C | 2026-08-03 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T2 |
| `twpub_taifex_tx_open_interest_log` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_tx_settlement_logret_1d` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_tx_top10_long_ratio` | 期交所 | D線、D訓、C | 2026-08-03 | 2026-09-17 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | T2 |
| `twpub_taifex_tx_top5_long_ratio` | 期交所 | D線、D訓、C | 2026-08-03 | 2026-09-17 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | T2 |
| `twpub_taifex_tx_volume_log` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_txo_call_oi_log` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_txo_call_volume_log` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_txo_put_call_oi_ratio` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_txo_put_call_volume_ratio` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_txo_put_oi_log` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_taifex_txo_put_volume_log` | 期交所 | D線、D訓、C | 2026-07-17 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | T1 |
| `twpub_attention_close_log` | 注意股票 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | E1 |
| `twpub_attention_count_log` | 注意股票 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | E1 |
| `twpub_attention_event_flag` | 注意股票 | W | 2026-07-20 | 2026-09-18 | — | 0 | 2014–2025 | 兩表均無 2014 觀測 | E1 |
| `twpub_attention_pe_log` | 注意股票 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | E1 |
| `twpub_attention_source_covered` | 注意股票 | W | 2026-07-20 | 2026-09-17 | — | 0 | 2014–2025 | 兩表均無 2014 觀測 | E1 |
| `twpub_dividend_board_approved` | 股利 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_dividend_cash_per_share` | 股利 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_dividend_confirmed` | 股利 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_dividend_stock_per_share` | 股利 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_dividend_total_cash_log` | 股利 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_dividend_total_stock_log` | 股利 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_disposal_count_log` | 處置股票 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | E1 |
| `twpub_disposal_event_flag` | 處置股票 | W | 2026-07-20 | 2026-09-18 | — | 0 | 2014–2025 | 兩表均無 2014 觀測 | E1 |
| `twpub_disposal_source_covered` | 處置股票 | W | 2026-07-20 | 2026-09-18 | — | 0 | 2014–2025 | 兩表均無 2014 觀測 | E1 |
| `twpub_financial_assets_log` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_book_value_per_share_log` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_current_ratio` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_debt_ratio` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_eps` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_equity_ratio` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_gross_margin` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_net_income_asinh` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_net_margin` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_operating_margin` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_financial_revenue_log` | 財報 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M2 |
| `twpub_short_sale_available_log` | 資券 | 僅來源 | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | S1 |
| `twpub_material_clause_log` | 重大訊息 | D線、D訓、C | 2026-07-17 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M1 |
| `twpub_material_event_count_log` | 重大訊息 | D線、D訓、C | 2026-07-17 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | M1 |
| `twpub_material_fact_lag_days` | 重大訊息 | D線、D訓、C | 2026-07-17 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | M1 |
| `twpub_exdiv_cash_dividend` | 除權息／公司行動 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_exdiv_known` | 除權息／公司行動 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_exdiv_stock_dividend_ratio` | 除權息／公司行動 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_exdiv_subscription_price_log` | 除權息／公司行動 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_exdiv_subscription_ratio` | 除權息／公司行動 | D線、D訓、C | 2026-07-20 | 2026-09-18 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | E2 |
| `twpub_tdcc_holder_count_log` | 集保股權分散 | 僅來源 | 2026-07-20 | 2026-09-17 | 0 | — | 2014–2025 | 兩表均無 2014 觀測 | D1 |
| `twpub_tdcc_large_holder_ratio` | 集保股權分散 | 僅來源 | 2026-07-20 | 2026-09-17 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | D1 |
| `twpub_tdcc_retail_holder_ratio` | 集保股權分散 | 僅來源 | 2026-07-20 | 2026-09-17 | 0 | 0 | 2014–2025 | 兩表均無 2014 觀測 | D1 |

### 股票面板（21）

| 特徵 | 家族 | 配置 | 首日 | 最新 | 正式表 2014 格 | 研究表 2014 格 | 2015–2025 零值年 | 判讀 | 路線 |
|---|---|---|---|---|---:|---:|---|---|---|
| `body_ratio` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `close_logret_1d` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `close_raw` | 股票行情／面板 | R、W | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `clv` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `clv_centered` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `delta_body_ratio` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `delta_clv` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `high_raw` | 股票行情／面板 | R、W | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `low_raw` | 股票行情／面板 | R、W | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `lower_shadow` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `max_logret_1d` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `min_logret_1d` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `next_session_open_gap_logret` | 股票行情／面板 | D線、D訓 | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `open_logret_1d` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `open_raw` | 股票行情／面板 | R、W | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `shadow_imbalance` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `signed_body_ratio` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `signed_vol` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `trading_volume_logret_1d` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `trading_volume_raw` | 股票行情／面板 | R、W | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |
| `upper_shadow` | 股票行情／面板 | D線、D訓、O、S、C | 面板計算 | 面板計算 | — | — | 未逐欄核 | 2014 未逐欄核數 | P2 |

### 旗標（50）

| 特徵 | 家族 | 配置 | 首日 | 最新 | 正式表 2014 格 | 研究表 2014 格 | 2015–2025 零值年 | 判讀 | 路線 |
|---|---|---|---|---|---:|---:|---|---|---|
| `twpub_cbc_m1b_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | C1 |
| `twpub_cbc_m2_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | C1 |
| `twpub_dgbas_cpi_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | G1 |
| `twpub_dgbas_gdp_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | G1 |
| `twpub_mof_business_tax_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | F1 |
| `twpub_mof_export_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | F1 |
| `twpub_mof_futures_tax_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | F1 |
| `twpub_mof_import_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | F1 |
| `twpub_mof_securities_tax_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | F1 |
| `twpub_mof_tax_total_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | F1 |
| `twpub_mof_trade_balance_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | F1 |
| `twpub_pb_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | P1 |
| `twpub_pe_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | P1 |
| `twpub_xbrl_assets_twd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_basic_eps_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_basic_eps_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_cash_twd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_current_assets_twd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_current_liabilities_twd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_current_ratio__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_debt_ratio__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_equity_ratio__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_equity_twd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_gross_margin__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值缺 2014 | M2 |
| `twpub_xbrl_gross_profit_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值缺 2014 | M2 |
| `twpub_xbrl_gross_profit_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值缺 2014 | M2 |
| `twpub_xbrl_inventories_twd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_inventory_to_assets_ratio__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_liabilities_twd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_net_margin__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_operating_cash_flow_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_operating_cash_flow_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_operating_margin__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值缺 2014 | M2 |
| `twpub_xbrl_operating_profit_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值缺 2014 | M2 |
| `twpub_xbrl_operating_profit_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值缺 2014 | M2 |
| `twpub_xbrl_profit_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_profit_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_revenue_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_revenue_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_gross_margin__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_gross_profit_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_gross_profit_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_net_gross_margin__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_net_gross_profit_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_net_gross_profit_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_net_operating_income_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_net_operating_income_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_operating_margin__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_operating_revenue_twd_quarter_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
| `twpub_xbrl_tifrs_operating_revenue_twd_ytd_raw__available` | 可用性旗標 | W | 衍生 | 衍生 | — | — | 由基礎值決定 | 基礎值有 2014 | M2 |
<!-- END GENERATED UNIQUE FEATURE HISTORY -->

## 重建清單

先載入 scripts/runtime_env.sh，再執行 run_fintech_python scripts/summarize_tw_stock_unique_feature_history.py。腳本會確認兩份年度盤點與當前來源 Parquet 的 inode、大小和 mtime 簽章一致；若來源已變，先依[現況報告](tw_stock_training_feature_status_2026-09-18.md)中的命令重建兩份年度盤點，再重跑。
