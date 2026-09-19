# 臺灣資料歷史完整度與可用性（2026-09-17 14:45 Asia/Taipei）

這份清冊把「抓到檔案」、「商品與交易日覆蓋」、「欄位有值」和「當時已發布、可供訓練」分開。空值不自動等於下載失敗；目前公開來源也不能保證每個商品從上市以前就有資料。

## 已驗證的範圍

- 官方 TWSE／TPEx 個股日價：目前 `stocks/symbols.csv` 的 2,755 個股票／ETF 均有 Parquet；`date/open/max/min/close/Trading_Volume/adjclose` 的逐商品檔案 footer 未見缺值。官方逐日來源最早分別從 2004-02-11（TWSE）及 2003-08-01（TPEx）建立覆蓋；11 個官方逐日資料集到 2026-09-16 的狀態憑證均為 `coverage_complete=true`、`missing_dates_after=0`。這不證明每檔股票在停牌日也應有交易列。
- Shioaji：股票 KBar 官方歷史窗口自 2020-03-02 開始，[單次查詢不超過 30 天](https://sinotrade.github.io/zh/tutor/market_data/historical/)。現有分鐘來源憑證可重建 2,337 個商品的日資料；另外 127 個合約不可取得、291 個在來源窗口外，保留官方資料而不製造 Shioaji 報價。混合資料共 2,755 個商品、8,982,676 列；完整稽核驗證了 168,236 個來源分鐘分塊憑證。
- 台股公開資料產品：註冊 159 個原始產品，另有 3 個原始新聞稿歷史版本產品。分類為 11 個官方逐日查詢、2 個下市歷史列表、9 個可取得舊統計期但值可能修訂的 bulk 表、137 個從首次擷取才有版本的快照、3 個原始新聞稿版本庫。全部 162 個產品在本機有可讀 Parquet；「有檔案」不等於歷史版本或逐股完整。
- 訓練特徵表：已重建 9,592,140 個實體列、116 個特徵欄位，另外有識別／規則欄。CBC 外匯存底原始新聞稿 317/317 筆、DGBAS CPI／失業率／GDP 原始數值公告 649/649 筆、CBC 貨幣新聞稿 321/321 篇均通過獨立原始位元組與雜湊稽核。貨幣稿中 319 個不同月份有 M1B/M2 **當月年增率**，訓練表兩欄各有 319 筆非空發布事件；一篇提前發布通知只保留原文，2008-05 同值鏡像稿不重複算月份。DGBAS 三項在訓練表中分別有 272、259、118 筆非空歷史發布值；GDP 年增率已按當年原稿直接接入，而 GDP 水準值沒有從增長率推造。對帳器於重建後須回報 `rebuild=false`，以本次驗證紀錄為準。
- 在本節 14:45 快照仍有 15 個全空特徵：美元／新臺幣匯率 2 欄、央行隔夜利率與 M1B/M2 **水準**共 4 欄、主計總處 CPI/GDP 水準 2 欄、財政部貿易與稅收 7 欄。這些是尚無已驗證歷史發布值、或其首次觀測晚於本次截止日的欄，不可用現在的整包修訂值倒填；9/17 收盤後的非空數已更新於下方「重新驗證與持續更新」。CBC 貨幣年增率已有值，但 1999-12–2026-07 的原稿序列仍缺 2000-06，不能宣稱整段連續完整。
- `fallback_reason`、`raw_ohlc_scale_factor`、`return_quarantine_reason` 等是只在來源替代、尺度校正或異常時才應有值的證據欄；其全空或稀疏不能算成 OHLCV 缺漏。
- 嚴格模型驗收只針對 `configs/markets/tw_public_preopen_pit.yaml` 已選的 35 欄：先前完整 panel 有 5,337 個交易日、2,755 個商品；[先前稽核](../artifacts/data_quality/tw_public_preopen_pit_live_20260917_649/report.md)為 `model_safe=true`，critical/high 皆 0、medium 1。這不是對全部 116 個特徵或 162 個產品的訓練保證，且同一策略的收盤價撮合仍是研究近似。新增的 M1B/M2 年增率尚未選入這個設定。

## 仍不能宣稱「全部完成」的地方

- 137 個快照產品的舊列不是原始發布版本。MOPS 網站有歷史查詢入口，但目前本機對其靜態歷史網址收到「FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED」；不應把這個阻擋偽裝成下載成功或繞過。對沒有原始值版本的月營收／財報／持股資料，僅憑固定申報規律逆推日期也無法知道當年的數值是否曾修訂。DGBAS 原始公告雖已補全，也不能代替其他 137 項資料的歷史版本。
- 公告庫的 `value_history_complete` 只表示目前官方原稿中，界定期間都有可解析且可稽核的數值；本機第一次保存這些原稿是在 2026 年，不能單憑今日頁面證明其位元組從歷史首發起未被改動。原文所載發布日期、當年官方時刻規則與「首發版本位元組」是三種不同證據；模型須保留此版本不確定性，後續捕獲的修訂應另留版本而非覆寫舊原件。
- Yahoo 臺股原始替代檔從 2000-01-03 起有資料，但在第一個官方重疊日之前，Yahoo raw OHLC 與官方每股價格尺度沒有因果錨點；這些原始列保存在替代檔，不能直接併入可交易價格。TWSE 官方[逐日總表說明自 2004-02-11 提供](https://wwwc.twse.com.tw/en/trading/historical/mi-index.html)。
- 14:45 快照中 15 個無足夠歷史發布版本憑證的欄位仍維持全空；9/17 收盤後其中 12 個僅有 1 筆首次觀測值，歷史缺口仍存在。這是正確性界限，不是用 0 或臆測值補完。完整模型安全性仍需以選定 config 的嚴格 panel/PIT 稽核為準；此處的檔案／欄位審核不取代它。

## 尚未接入嚴格訓練的替代來源

| 缺口 | 找到的官方替代入口 | 接入前還要證明 |
|---|---|---|
| 美元／新臺幣每日收盤匯率 2 欄 | [央行每日收盤匯率](https://www.cbc.gov.tw/tw/lp-645-1-3-20.html)與[年度日資料檔](https://www.cbc.gov.tw/tw/lp-2151-1.html) | 央行頁面寫工作日 16:00–17:00 提供當日值，因此最早是下一交易日開盤；但現行歷史列表不等於逐日初版，需原始版本或更正紀錄，不能把抓取日的整包值冒充各歷史日原值。 |
| 金融業隔夜拆款利率 2 欄 | [央行每日歷史表](https://www.cbc.gov.tw/tw/lp-641-1.html) | 表列資料日期與利率，但逐日首次上線時刻／修訂版尚未驗證；月平均新聞稿不是同一個日資料定義。 |
| M1B、M2 水準 2 欄 | [央行逐期金融情況原稿](https://www.cbc.gov.tw/tw/lp-302-1-1-20.html)及原稿附件 | 目前只解析有原文證據的當月年增率，不能從年增率反推絕對水準；附件需逐版解析、核對單位與歷年口徑變化。 |
| CPI、GDP 水準 2 欄 | [主計總處 CPI](https://www.stat.gov.tw/News.aspx?n=2668&sms=10980)、[GDP 原稿](https://www.stat.gov.tw/News.aspx?n=2677&sms=10980) | 原稿附件中的當期指數／水準、基期改制與修訂版本需逐期核對，不能用今日整包歷史表倒填。 |
| 財政部貿易、稅收 7 欄 | [進出口統計發布規則](https://www.mof.gov.tw/singlehtml/979b54e408fb499eae3c1d9efe978868?cntId=9c62e8eb502c4600b8490a829dfbeefe)及[歷年賦稅新聞稿 PDF 範例](https://service.mof.gov.tw/public/Data/statistic/news/11401/11401%E6%9C%AC%E6%96%87%E5%8F%8A%E9%99%84%E8%A1%A8.pdf) | 按月找初報 PDF、附表、公告時刻與後續修訂；稅目口徑和期別要明示，不能把年度累計誤當單月值。 |

這些入口是可調查的來源，不是已完成下載或嚴格 PIT 認證。既有 137 個快照產品的早年版本、MOPS 受阻的逐期申報以及 2000-06 央行貨幣數值仍是獨立缺口。

主計總處公告只有日期、沒有逐篇可信時刻時，使用[2012 年改為 08:30 的官方公告](https://www.stat.gov.tw/News_Content.aspx?n=3703&s=23721)與[2017 年改回 16:00 的官方公告](https://www.stat.gov.tw/News_Content.aspx?n=2642&s=97506)重建發布時鐘；原稿明載時刻優先。這是有官方制度依據的推定，仍與逐篇原始時刻證據分開，不把 2012–2017 年開盤前發布的舊資料無故延遲一天。

公告原始 HTML 在輪詢時可能只有版型雜湊變動；原始位元組與 SHA-256 仍個別保留及稽核。特徵對帳另對發布日、時鐘、值和證據資格做語義收據：實測兩版 Parquet 位元組不同而語義 SHA-256 相同時，不觸發 959 萬列全表重建；值或時鐘改變則必須重建。

2026-09-17 15:09 已在本機啟用 `stockagent-tw-public-release-archives.timer`；系統排程顯示下次 16:30。與台股正式資料製作共用同一來源鎖，忙碌時由 systemd 安全略過且保留後續排程。日結守門現在會依公告版本庫的 `feature_semantics_v1` 收據核對語義內容，而不是誤拿語義 JSON 長度去比 Parquet 實體檔案大小；不支援的收據類型仍拒絕。這修正了當日日結重建成功卻被錯誤判為公告檔尺寸不符的問題，但不代表 2026-09-17 的所有交易日來源和嚴格模型稽核已過關，仍須以當次日結收據為準。

增量特徵對帳還必須處理日間收盤更新：早晨根目錄 `download_summary.json` 可能停在前一交易日，不能拿它覆蓋當日下午已建好的表。現在只在同一 live root 的已驗收 `close_initial`／`close_final` 收據、兩市場日價 footer 都證明當日覆蓋時，將目標日提升到該收盤日；任何擬將現有特徵表回退的重建一律拒絕。2026-09-17 的實際唯讀 dry-run 回報 `rebuild=false end_date=2026-09-17`。

獨立特徵對帳計時器也須在整個「讀來源收據 → 建表 → 再驗收」區間持有同一個 canonical producer lock；若來源監測／日結占用鎖，計時器安全延後，不在來源改寫中途產出混合版本。已持鎖的官方公告封存協調器才使用內部 `--source-update-lock-held`，避免自鎖；正式 systemd 單元已補鎖目錄的寫入權限。

2026-09-17 15:24 網站欄位清冊新快照：訓練表已到 9,594,667 列、9/17；上方 14:45 快照所列 15 個「當時全空」欄位已有 12 個在**首次觀測後**各出現 1 個值，剩 3 個仍全空（隔夜拆款利率變動、營業稅水準、匯率日變動）。這不等於補齊 12 個欄位的歷史版本：它們的過去仍不能用今日整包值倒填。判斷資料可用性必須同看歷史發布版本覆蓋與非空筆數，不能只看欄位是否不再全空。

2026-09-17 15:40 在來源鎖內完成[嚴格 panel/PIT 稽核](../artifacts/data_quality/tw_public_preopen_pit_live_20260917_locked/report.md)：選定 35 欄、5,338 交易日、2,755 商品，`model_safe=true`，critical/high 皆 0，medium 1（下市公告交叉覆蓋不完整，保持 fail-closed）。這是固定當次 live 位元組的稽核快照；鎖釋放後來源更新可能使收據過期，實際訓練仍須固定精確資料 release，不得把此結果擴張成全部 116 欄的歷史版本完成。

## 重新驗證與持續更新

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_symbol_history_coverage.py
run_fintech_python scripts/audit_tw_official_release_archives.py
run_fintech_python scripts/refresh_tw_public_release_archives.py --dry-run
run_fintech_python scripts/audit_tw_shioaji_dataset.py
run_fintech_python scripts/reconcile_tw_public_training_features.py --dry-run
```

逐商品、逐欄位結果在 `artifacts/data_completeness/latest/{symbols.csv,fields.csv,summary.json}`；舊快照在重新掃描前仍反映先前的 17 欄全空。`stockagent-tw-public-release-archives.timer` 於交易日 07:00、16:30、19:30 平行抓取不同官方主機，兩個 CBC 庫在同站點依序執行，週日 03:00 重掃完整索引；原稿稽核過關後立即對帳特徵。原有 `stockagent-tw-public-feature-reconcile.timer` 另在交易日 08:20、14:20、19:00 檢查實際來源內容；未變就跳過，變動則用建表器完整重建並再次驗證。網站 `/data-monitor/` 的欄位清冊另外標示條件式證據欄及未證明發布版本的全空訓練欄。

## 15 欄候選數值與發布時鐘補齊（2026-09-17）

新增獨立的 `artifacts/data_quality/tw_public_provisional_macro/events.parquet`，依「來源數值 → 原稿實際發布時刻 → 原稿發布日加推定時刻 → 歷史規律推定日期」標記每筆值。2026-09-17 首次實際建置有 15 欄、29,798 筆不重複的特徵／資料期事件。候選表逐欄保存來源網址、來源 Parquet 雜湊、首次觀測、資料所屬期、臺北發布時刻及其證據等級、其後首個開盤可用交易日；每筆 `strict_pit_eligible=false`。它**不是**嚴格訓練表，也不把今日修訂過的整包值偽裝為當年初版。缺乏原始版本時，即使精確找到了公告日期，數值版本仍須另驗證。網站另外為 15 欄各建獨立條目，分別顯示筆數、最早／最新資料期與只剩原始數值而不可轉成現行特徵的筆數；這 15 欄與候選總帳重疊，不能加總。

候選表也保留不適合現有 `log1p` 定義的原始數值：財政部月度營業稅 632 期中 195 期為負，證券交易稅另有 2 期為負。這些列的 `source_value` 有值、`feature_value` 為空、`transform_status=outside_existing_log_domain`；這是變換定義問題而非下載缺漏。是否新增 signed-log/asinh 特徵需另立模型欄位及 checkpoint 契約，不能把舊 `*_log` 偷換定義。

同日後續新增 12 個獨立 `_raw` 候選欄，不覆寫舊 `*_log`／`*_asinh` 語意。重建後候選總帳為 27 欄、40,805 筆特徵／資料期事件；負營業稅 195 期與負證交稅 2 期在原始值欄的 `feature_value` 都保留數值。網站分母依實際收據中的欄位數顯示，不再固定寫 15。這些 `_raw` 宏觀數值仍全部是**非嚴格 PIT 候選**，不能因修正 log 定義就進入正式訓練。另見[原始值輸入實驗](tw_raw_feature_input.md)。

- 央行隔夜拆款[每日官方索引](https://www.cbc.gov.tw/tw/lp-641-1.html)：304 頁、6,071 個日期，2002-05-02 至 2026-09-17，原始 HTML 和 SHA-256 已存；補正開放資料 CSV 只到 2026-03-18 的尾端。重輪詢未變更數值時保留首次觀測時間，避免虛構新版本。逐日首次上線時刻未知，暫按收盤後 18:00 映射，並保留推定標記。
- 央行[美元／新臺幣歷年日收盤](https://www.cbc.gov.tw/tw/lp-2151-1.html)：已保存 2000–2025 年 26 份年度原始 HTML、6,477 個日期；與現有開放資料重疊部分比對無差異，補足 2000–2007 年。央行說明[當日匯率 16:00–17:00 提供](https://www.cbc.gov.tw/tw/lp-645-1-3-20.html)，因此以 17:00 最晚發布窗作下個交易日可用時點；年表現值不是每日日初版。
- 財政部[貿易／賦稅新聞稿索引](https://www.mof.gov.tw/multiplehtml/384fb3077bb349ea973e7fc6f13b6974?categoryCode=STAT)：已掃完 38 頁貿易與 31 頁賦稅索引，取得 263 個不同期別的官方發布日；貿易從 2016-01、賦稅從 2014-01。逐篇時刻未明時仍標為 `official_release_date_inferred_clock`，用 16:00 推估，不把推定時刻說成精確發布時刻。更早期別保留 `estimated_historical_pattern`，持續尋找原稿。
- 海關整包千元精度 CSV 目前只到 2026-06；7、8 月[初報](https://www.mof.gov.tw/singlehtml/384fb3077bb349ea973e7fc6f13b6974?cntId=61f5437eabfe44bcbe442a02eaeea30a) PDF 可給出口、進口、出入超的臺幣億元整數。新增缺月補抓器，原 PDF 留存 SHA-256；候選欄位把其精度寫成 `nearest_100000_twd_thousand`，只在整包 CSV 缺月時使用。嚴格特徵仍待相同精度的正式來源或明確模型契約變更，不以四捨五入值冒充精確千元。
- 財政部索引另有逐期原始新聞稿 PDF 封存管線；已保存目前找到的貿易／賦稅原稿 277/277 份、雜湊和本機首次觀測收據，並核對期別文字。現存舊 PDF 不自動證明當年首發位元組。索引缺漏中的 2018–2019 年賦稅 17 期，有 14 期另找到財政部原稿 PDF 所載發布日；3 期官方 PDF 路徑仍查無原稿，改採前一期原稿所公告的「下次發布日期／時刻」作推定，標記 `estimated_official_advance_schedule`，不謊稱已驗證當期實際發稿。

新下載器與候選表均已註冊至來源清冊及 `/data-monitor/`，由既有 `stockagent-tw-public-release-archives.timer` 增量更新；首次全索引、後續近期頁面、週日全索引。獨立啟動時會爭取 canonical 台股來源鎖，遇到 MOPS／日結等來源寫入時拒絕混合更新。下載與候選重建指令：

23:00 重新清點時，公開資料來源清冊 168/168 項有本機檔案，新增的財政部原稿為 277/277 份且逐檔雜湊與期別核對通過；但原稿索引仍缺 3 個賦稅月、137 項來源仍是「從第一次擷取起」的快照，而非可追溯歷史初版。網站故將財政部原稿列為有缺口、15 項候選特徵列為非嚴格 PIT；本機檔案齊不等於訓練時點資料齊。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/refresh_tw_public_release_archives.py --dry-run
run_fintech_python scripts/build_tw_public_provisional_macro.py --input-dir data_tw_public
```

仍未完成的證據：大部分整包歷史值的初版／修訂序列；央行隔夜逐日首次發布時刻；財政部 2014 年前的賦稅與 2016 年前的貿易逐期原稿日期；以及來源本身沒有提供的上市前、停牌日或未發布月份。這些不能靠固定延遲或空值填 0 解決。
