# 全期貨當沖歷史價格網格及日盤證據（2026-09-27）

本次範圍是 `artifacts/markets/tw_futures_v8_intraday_preparation/missing_by_product.csv` 的 2020-03-23 以後缺口：22 種指數、26 種 ETF，以及沿用原股票期貨規則的股票期貨。下列規則只驗證普通單式實際成交／原始 OHLC，不是 settlement、VWAP、跨月價差、鉅額交易或還原價的規則。通過價格網格不等於已有可訓練的完整分鐘資料。

## 程式契約

沿用 `stockagent/data/tw_price_rules.py`，新增：

```python
price_on_taifex_futures_tick_grid_numpy(price, dates, *, product_codes, asset_classes)
taifex_futures_tick_size_numpy(price, dates, *, product_codes, asset_classes)
taifex_futures_day_session_minutes(day, *, product_code, asset_class,
                                 is_last_trading_day=False)
```

新 API 必須有真實 execution-session date。初版接受 2020-03-23 至 2026-09-27；2026-09-28 僅將 TX、MTX 下限延伸至 2011-01-03，其他商品的查核下限不變。未知指數／ETF 代碼、錯誤資產類別、缺日期、超出查核範圍、指數上市前或已知終止上市後均拒絕。索引表中的早期商品下限是本次資料範圍，**不是宣稱該日上市**。ETF 數值 grid 是 2014 年起的共同契約規則；股票與 ETF 每支商品的上市、終止上市、調整型及到期月份身份仍須由 canonical dated contract master 和官方每日身份比對驗證，這個數值模組不替代它們。

`TAIFEX_FUTURES_HISTORY_CONTRACT_VERSION = 2` 綁定上述 TX/MTX 擴充（初版為 1）。既有 `TW_DERIVATIVE_PRICE_CONTRACT_VERSION = 2`、stock／ETF 計算、date-less TX/MTX/TMF API 均不變。新 product-aware receipt/cache 應綁新版本，避免讓未變更的既有股票來源 cache 全部失效。

新增範圍以 [CFTC 2004-06-02 原始函](https://www.cftc.gov/sites/default/files/tm/letters/04letters/tm04-16.htm)的 TX/MTX 一點 tick、每點 200/50 元及一般時段，配合[期交所 2013 年印製摺頁](https://www.taifex.com.tw/chinese/WeeklyMTX/摺頁文宣.pdf)的日盤／到期時間交叉驗證。CFTC 舊函中的最後結算方式不沿用；每日與到期估值直接使用當日官方資料。擴充來源原文、內容雜湊與範圍記於 `configs/data/taifex_margin_source_reviews_2011_20260928.json`，完整保證金資料契約見 [較早歷史準備報告](tw_futures_margin_earliest_history_2026-09-28.md)。

## 指數期貨

除 TJF 以外，表列一般日盤均為臺北時間 08:45–13:45。TJF 為 08:00–16:15。這是完整來源身份驗證時段，不改策略的 08:46 與 13:20–13:30 event bars。

| 商品 | 普通單式 tick（指數點） | 新 API 起日 | 歷史官方依據 |
|---|---:|---|---|
| TX | 1 | 2011-01-03 | 上述 2004 年原始函；[2011 年 5 月官方簡報，第 11 頁 tick 表](https://www.taifex.com.tw/chinese/9/股價指數類及股票類契約最後結算價取位方式調整.pdf) |
| T5F、XIF | 1 | 2020-03-23 | 同一 2011 年 tick 表 |
| TE、GTF | 0.05 | 2020-03-23 | 同一 2011 年表；TE 不套用 2025 年 TEO 選擇權修正 |
| TF | 0.2 | 2020-03-23 | 同一 2011 年表 |
| MTX | 1 | 2011-01-03 | 上述 2004 年原始函、2013 年摺頁；[2020 年永續期貨宣導簡報，第 12 頁](https://report.taifex.com.tw/file/taifex/event/cht/train1090420/臺灣永續期貨_市場宣導簡報.pdf) |
| TJF | 0.25 | 2020-03-23 | [2015 年 12 月官方雙月刊，第 6 頁](https://www.taifex.com.tw/chinese/10/moth_all/201512_all.pdf)：tick 及 08:00–16:15 |
| UDF、SPF | 1、0.25 | 2020-03-23 | [2017 年 5 月印製上市摺頁，契約規格表](https://www.taifex.com.tw/file/taifex/CHINESE/10/UDF_SPF_futures_pamphlet.pdf) |
| UNF | 1 | 2020-03-23 | [2019 年 8 月官方簡報，第 14、16 頁](https://report.taifex.com.tw/file/taifex/event/cht/G2F_UNF/file_unf/美國那斯達克100期貨_簡報.pdf) |
| G2F | 1 | 2020-03-23 | [2019 年上市官方摺頁](https://report.taifex.com.tw/file/taifex/event/cht/G2F_UNF/file_g2f/富櫃200期貨_摺頁文宣.pdf) |
| E4F、BTF | 1 | 2020-06-08 | [2020 年 6 月規章摘要，第 30 頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/eBooks/202006/30/)：生效日、tick、日盤時間 |
| F1F | 1 | 2020-11-23 | [2020 年上市簡報，第 12–13 頁](https://www.taifex.com.tw/file/taifex/event/cht/F1F_future/英國富時100指數期貨.pdf)；[2023 年報之歷史上市日期](https://www.taifex.com.tw/file/taifex/CHINESE/10/AnnualReport/2.公司簡介2023.pdf) |
| ZEF | 0.05 | 2021-06-28 | [2021 年 8 月規章摘要，第 31 頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/eBooks/202108/31/) |
| ZFF | 0.2 | 2021-12-06 | [2021 年 12 月官方介紹，第 9 頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/eBooks/202112/9/) |
| SOF、SHF | 1、0.05 | 2022-06-27 | [2022 年 6 月官方雙月刊之上市規格](https://www.taifex.com.tw/file/taifex/CHINESE/10/moth_all/202206_all.pdf)；[SHF 原始規章函 1110200754](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/1110200754.pdf) |
| SXF | 0.5 | 2023-12-18 | [上市規章函 1120201997](https://www.taifex.com.tw/file/taifex/eng/eng11/1120201997.pdf)；[2023 年 12 月官方上市報導](https://www.taifex.com.tw/file/taifex/CHINESE/10/moth_all/202312_all.pdf) |
| TMF | 1 | 2024-07-29 | [2024 年 8 月雙月刊第 7、16–17 頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/eBooks/202408/index.html) |
| M1F | 1 | 2024-12-09 | [2024-11-15 官方上市新聞稿](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/5_中100上市日期新聞稿_v8.pdf)；[原始契約規格公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/中型100期貨公告.pdf) |

T5F 僅接受至 2022-09-21；2022-09-22 起終止上市。[官方公告 1110002149，2022-07-12](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/公告小型美元兌人民幣選擇權、美元兌人民幣選擇權及臺灣50期貨終止上市有關事項.pdf)。

歷史基準／上市文件與目前商品規格交叉核對，才在上述有限期間沿用同一 tick；不是由 2026 年商品頁直接反推 2020 年。已核對的當前例外包括 [TJF 0.25／08:00–16:15](https://www.taifex.com.tw/cht/2/tJF)、[SHF 0.05](https://www.taifex.com.tw/cht/2/sHF)、[SXF 0.5／08:45–13:45](https://www.taifex.com.tw/cht/2/sXF)。本次不擴充未選中的匯率、商品期貨、選擇權或客製化商品。

## ETF 期貨

所有 26 種均用未滿 50 元 tick 0.01、50 元以上 tick 0.05。[2014 年 9 月印製、2014-10-06 上市官方摺頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/ETF期貨介紹摺頁文宣.pdf)已記載此數值規則，與[現行股票／ETF 期貨契約規格](https://www.taifex.com.tw/cht/2/sTF)相同。2026-07-06 的股票期貨千元 tick 修正不得套用 ETF。

一般日盤依**標的成分證券**分組，不依 ETF 在臺灣掛牌與否：

| 一般日盤 | 本次明確納入的產品 |
|---|---|
| 08:45–13:45，國內成分 | NYF 元大台灣50、PFF 元大高股息、RIF 國泰永續高股息、RYF 群益台ESG低碳50、SMF 群益台灣精選高息、SNF 復華台灣科技優息、SRF 小型元大台灣50、SSF 小型元大高股息、SUF 元大台灣價值高息、VHF 富邦科技 |
| 08:45–16:15，國外成分 | NZF 元大寶滬深、OAF 富邦上証、OBF 元大上證50、OCF 復華滬深、OJF 國泰中國A50、OKF 富邦深100、OOF 群益深証中小、RXF 富邦越南、RZF 元大美債20年、SGF 國泰智能電動車、SIF 元大美債1–3、SQF 中信高評級公司債、UJF 群益ESG投等債20+、UKF 國泰20年美債、URF 統一FANG+、USF 小型統一FANG+ |

2014 初始摺頁的日盤仍是全部 13:45 收盤，不能拿它當作 2020 日盤依據。[2015 年 7 月官方延長交易時段摺頁](https://www.taifex.com.tw/file/taifex/CHINESE/10/ExtendingTradingHours_pamphlet.pdf)確認自 2015-07-20 起延長，並列出 FH 滬深等陸股 ETF；[2015 年 8 月規章摘要](https://www.taifex.com.tw/chinese/10/moth_all/201508_all.pdf)明定法律上的範圍是國外成分／境外 ETF，並非僅限當時六支陸股商品。商品名稱與代碼交叉核對[期交所標的／保證金表](https://www.taifex.com.tw/cht/5/stockMargining)及本次 dated daily master；此查核不將目前保證金金額回填歷史。

OCF 2020-03-23 的本機 source probe：13:45 前 77 口，16:12 另 2 口，完整日盤合計 79 口及 close/low 20.15 才與官方日資料相符。因此不得把 16:12 排為夜盤，也不得用截短的 13:45 OHLC 宣稱來源身份失敗。該筆 source receipt 仍由 acquisition audit 保存；價格模組不以行情吻合倒推交易規則。

## 日盤邊界與因果限制

session helper 的整數是臺北午夜後分鐘，含收盤分鐘，僅提供一般日盤 envelope。國內指數、股票、ETF 的真正最後交易日可由 dated physical-contract master 傳入 `is_last_trading_day=True`，縮短至 13:30；不可自行只用第三個星期三推算，避免假日／颱風變更。未傳入時取普通日盤的上界，並不合成提前收盤後的成交。美國／英國指數之日盤不套用國內 13:30 到期邊界；其夜盤最後交易時間的歷史變更超出本 helper 範圍。

這些 API 不判定 FinMind `date`／`deal_time` 的跨日意義、不校正原始 timestamp、不補成交、不插值，也不接受僅用目前 continuous ticker 證明歷史實體月份。builder 仍須檢查原始來源日期、session、正式每日 OHLC/volume 及 physical contract identity。完整日盤身份驗證和策略可執行 event bars 是不同要求。

## 驗證

2026-09-27，CPU threads ≤4、`MPLBACKEND=Agg`、CUDA disabled：

```text
pytest -q test/test_tw_all_futures_price_grid.py test/test_tw_order_price_grid.py
87 passed in 1.03s
```

測試涵蓋全部 22 指數、26 ETF 的 tick、上市／終止日期、缺日期、未知／錯配商品、ETF 50 元邊界、股票 2026 修正、float32 精度、TJF／OCF 延長日盤、到期縮短及舊 API 相容性。

另以缺口 CSV 實測，50,149 個 product/date keys 全部解析成功；open/high/low/close 各 50,149 個正有限價格、合計 200,596 個價格全部通過 grid，沒有 off-grid。記錄在 `artifacts/markets/tw_futures_v8_intraday_preparation/price_grid_scope_validation.json`，包含輸入 CSV SHA-256。這只是官方每日價格的數值檢查；來源 tick/minute 的身份與量能尚未因而驗收。來源補齊、整包 coverage 驗收與遠端訓練仍由各自 receipt 證明，不能由本測試推論完成。
