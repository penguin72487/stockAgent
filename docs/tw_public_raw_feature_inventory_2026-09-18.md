# 台股原始特徵、歷史完整性與發布時點盤點（2026-09-18）

這份盤點的範圍是 `data_tw_public` 的台股及相關總體資料，不把「今天查得到一個歷史數值」等同於「當年開盤前可得」。2014–2026 專項逐欄筆數、首末日、年度分布與逐欄決策見 `artifacts/data_quality/tw_public_raw_2014_v1/` 下的 `feature_inventory.csv`、`annual_feature_inventory.csv`、`2014_training_feature_decisions.csv`；嚴格訓練品質另以 `model_audit/` 為準。

## 2026-09-18 研究接受規則

研究資料的接受次序改為：**當期原始發布版本 > 現在可下載的官方版本**；發布日期證據依序為 **原稿時間 > 官方公告日期 > 按官方發布制度推定的日期 > 本機首次觀測時間**。兩個維度分開記錄，不因日期較可靠就把現在的修訂數值標成當年的原值。當年原稿不存在時，允許用現在官方版本與公告日／推定公告日建立「研究估計」特徵；保留 `value_vintage_basis`、`publication_time_basis`、原始檔雜湊、首次觀測時間與假設可用日。只有統計期間、沒有任何發布規律或公告依據的資料，最多從首次觀測日起使用。

這個放寬是使用者接受的**研究資料政策**，不是原有 `strict_pit_eligible` 或 `model_safe` 的重新定義。當前正式 2014 配置和嚴格稽核仍維持原證據門檻；既有的 [2014 寬覆蓋研究配置](tw_public_wide_research_2014.md)已提供另一張接受目前修訂值與推定發布日的特徵表。新增來源仍須先逐欄做日期映射和配置版本更新，不能把這份盤點 CSV 當作已經建好的特徵。盤點 CSV 新增 `research_value_basis`、`research_publication_basis`、`research_acceptance` 三欄，標示可接受的候選來源和啟用前提。

經濟部商工與金管會兩個官方目錄已接入既有 `download_tw_public_data.py` 的 `gcis`、`fsc` 標籤。商工保留每個 CSV 資源的原始檔；金管會保留每個公開資料集的官方 CSV 匯出。兩者都按資源及內容雜湊保存版本、記錄下載時刻與狀態。日常只讀目錄、處理未抓及官方更新標記變動的資源；同日期或同筆數的靜默修正需要明確執行 `--refresh` 才能發現。這些新來源先作原始資料庫與特徵候選，尚未加入現行 143 欄 ABI。

| 新增候選來源 | 已觀測的舊期範圍 | 研究使用方式與限制 |
|---|---|---|
| [商工公司／商業登記統計表](https://data.gcis.nat.gov.tw/od/datacategory) | 兩份 CSV 均為 2010-01～2026-07 | 家數、設立／解散、資本額可作總體或產業狀態；CSV 為目前版本，歷史公告日須依官方公告或有標籤的規律另外映射。 |
| [商工登記清冊及 API](https://data.gcis.nat.gov.tw/od/rule) | 舊設立日期可查；網頁目前只公開部分月清冊連結 | 統編可供公司對照；以舊日期篩選目前登記資料，不等於取得當年完整公司狀態。 |
| [金管會公開統計 API](https://stat.fsc.gov.tw/api/swagger/openapi.json) | 銀行競爭力月序列 2014-01～2026-07；證期局 2016-08～2026-04；各表不同 | 銀行健全度、放款、信用卡較有新增價值；上市櫃市值／周轉率先與已有 TWSE／TPEx 比對。`公告日期` 若為整包上架日，不得冒充逐期首次發布日。 |

### 2026-09-18 首輪下載實測

- 商工目錄 565 項全部檢查，其中 505 項有 CSV 連結、60 項沒有 CSV 連結（多為 API），共保存 527 份原始 CSV；「項目數」因此不等於「檔案數」。`公司登記(依營業項目別)－國際貿易業` 曾暫時回 HTTP 500，在同日增量重試成功後保存原檔。
- 金管會目錄 180 項全部處理完成，保存 180 份官方 CSV。兩個來源共 707 份、2,520,083,840 bytes；已依 state receipt 對每份原檔重算 SHA-256 與大小，錯誤 0。這是已保存位元組的完整性驗證，不是每個歷史月份、欄位或舊版本的完整性證明。
- 商工「公司登記統計表」「商業登記統計表」各有 199 個月（2010-01～2026-07）。金管會「金融競爭力_銀行局」有 151 個月（2014-01～2026-07），「金融競爭力_證期局」有 117 個月（2016-08～2026-04）。上述期間從本機原始 CSV 核對；之後更新以目錄狀態及逐表資料為準。
- 商工明細頁標示政府資料開放授權條款第 1 版及來源顯名要求；若對外發布原檔或衍生物，須依各資料集官方明細頁的來源資訊處理標示。下載成功本身不代表已完成授權與對外散布稽核。
- 增量驗證再次執行後兩個目錄均回 `up_to_date`，`fetched_dates=0`，不觸發冷庫新版本。最新本機冷庫版本 `tw-public-20260918T112115110979714Z-l0-penguin-1b219da785bc9c3a` 的 manifest 與 2,556 個物件（22,260,283,623 bytes）已獨立通過雜湊驗證，707 份原檔均列在其清冊。這不代表其他節點收斂、D: 備份或模型 pin 已驗證。
- 60 項無 CSV 連結者需依各 API 的必要條件查詢，不能視為 60 份可直接全量下載的表。官方開發指引有依民國核准日期查公司設立、變更與解散的端點；本機抽測部分公司端點可讀到舊日期的**現存查詢結果**，商業異動端點則回「非授權介接之 IP」。解散端點的 `top=1/50`、`skip=0/50` 抽測均回同樣 115 筆，故不能依文件宣稱分頁真的有效。依舊日期查今日資料仍非當年原版，也無法證明中間曾更新、後來不再符合條件的公司都在結果中；此類 API 必須另作授權、分頁與涵蓋率驗證，不能混入本批 CSV 完整度。

## 判斷順序

令 `s` 為資料所描述的交易／統計期間，`p` 為該**版本數值**首次發布時刻，`o` 為本機首次觀測時刻。對台股 09:00 決策，資料只可進入 `p < 09:00` 的首個交易日；盤後的交易日 `t` 數值進入 `t+1` 個**實際**交易日，不加任意多日緩衝。週末或休市則映射至下一個交易日。若原文只證明發布日期而非時分，先結合當年的官方發布時間制度推定；無法證明 09:00 前發布者，最早使用下一交易日。`o` 是可證明的保守上界，不能用當前查到的 `s` 取代 `p`。

從強到弱的證據是：原始版本與其原文時間戳；原始版本與日期加當年官方時間表；日期加官方長期規律的推定；本機首次觀測；僅有統計期間或今日回填的單一修訂版本。後兩者不能自動升級成嚴格 PIT 訓練值。即使 `p` 推定得很準，今日修訂的 `value` 也不會變成當年的值。這是資料版本與資料時鐘兩個獨立維度。

## 來源範圍及現況

`data_tw_public/dataset_manifest.json` 登錄 159 項產品：11 個可逐日查詢歷史的交易表、2 個下市歷史、13 個 data.gov.tw 產品及 133 個快照 URL；另有 3 個官方逐期新聞稿封存器，因此完整下載器清單共 162 類。產品可下載，**不代表**其全部舊版本可重建。2026-09-18 凌晨的本機來源檔檢查如下；「完整」在這裡只表示交易日來源覆蓋，不保證每一證券、每一欄都非空。

| 來源 | 第一筆 | 最新交易日 | 來源列數 | 原始模型值的邊界 |
|---|---:|---:|---:|---|
| TWSE / TPEx 日 OHLCV | 2004-02-11 / 2003-08-01 | 2026-09-17 | 5,322,669 / 4,413,930 | 完成交易日才有全日 OHLCV；成交股數、金額、筆數均保留原值 |
| TWSE / TPEx 每日估值 | 2005-09-02 / 2003-08-01 | 2026-09-17 | 4,445,919 / 3,785,363 | 本益比在盈餘非正數時官方本來就不計算，不能補成零 |
| TWSE / TPEx 融資融券 | 2001-01-02 / 2003-08-01 | 2026-09-17 | 5,466,850 / 3,237,585 | 來源日 `t` 的餘額與買賣量映射到下一交易日，原單位「張」 |
| TWSE / TPEx 三大法人 | 2012-05-02 / 2004-06-01 | 2026-09-17 | 3,500,405 / 2,577,793 | 淨買超原始股數保留正負號，映射到下一交易日 |
| TWSE 加權指數月檔 | 1999-01-05 | 2026-09-17 | 6,866 | 每日收盤指數原值；日檔覆蓋時先比對月檔一致性 |

來源日期覆蓋以已下載來源及 `scripts/audit_tw_public_data_layer.py` 驗證；不能由上述總列數推出全市場每個 symbol 全史完整。消失的證券、IPO 前、停牌、商品分類及官方未計算值是不同的缺值原因，不能以回填／插值冒充當時觀測。

## 這次已落地的原始值

`stockagent/data/tw_public_features.py` 現有特徵 ABI 為 143 欄；原先 5 個 stock OHLCV 原值由 `stockagent/data/panel.py` 按需提供。所有觀測到的原始浮點值不做 log、asinh、比率或絕對值轉換；來源本身的單位保留在名稱內。`configs/markets/tw_public_preopen_raw_v1.yaml` 原先只選以下 22 個有明確 09:00 時點分類的欄位，訓練年限自 2013 年起；這是保守的初版，不是「其餘 118 欄皆無歷史」。主力 2014–2026 應使用新增的 `configs/markets/tw_public_preopen_raw_2014_v1.yaml`：在上述 22 欄加上央行 M1B/M2 原稿年增率及主計總處 CPI、失業率、GDP 原稿百分比，共 27 欄。

若以歷史長度優先，`configs/markets/tw_public_preopen_raw_long_history_v1.yaml` 選 15 個原始欄位，排除上述較晚起始的估值與法人，保留 2005 年起的完整年度。兩者的特徵 ABI、fold 起點和 checkpoint 不能混用。

全欄位稽核得到 143 欄中 140 欄有至少一個有限數值、3 欄全空：`twpub_usdtwd_logret_1d`、`twpub_cbc_overnight_rate_chg`、`twpub_mof_business_tax_log`。這裡的「有值」僅指至少一個稀疏事件，不代表 2014–2026 連續、有原始版本、全 symbol 非空或能在當時開盤前使用。原始估值的非空量差異很大，例如 PE 6,247,564 列、PB 8,223,515 列、殖利率 8,211,200 列；融資融券餘額原值 8,643,520 列。外資淨買超在表頭修復後為 5,832,814 列、2,647 個 symbol，仍不能寫成「每個 symbol 每日都有」。

| 類別 | 實際輸入 | 開盤前的處置 |
|---|---|---|
| 股票交易（7） | `open_raw`, `high_raw`, `low_raw`, `close_raw`, `trading_volume_raw`, `twpub_official_trading_value_raw`, `twpub_official_trades_raw` | `t` 日交易值到 `t+1`；09:00 前不使用當日開盤價 |
| 每日估值（3） | `twpub_pe_raw`, `twpub_pb_raw`, `twpub_dividend_yield_pct_raw` | `t` 日盤後資料到 `t+1`；百分比維持官方 `%` 數值 |
| 融資融券（6） | `twpub_margin_balance_lots_raw`, `twpub_short_balance_lots_raw`, `twpub_margin_buy_lots_raw`, `twpub_margin_sell_lots_raw`, `twpub_short_sell_lots_raw`, `twpub_short_buy_lots_raw` | 建表器已映射至下一個有來源證據的交易日，面板不再二次延遲 |
| 三大法人（4） | `twpub_foreign_net_buy_shares_raw`, `twpub_investment_trust_net_buy_shares_raw`, `twpub_dealer_net_buy_shares_raw`, `twpub_institutional_net_buy_shares_raw` | 同上，負的淨買超不被截斷 |
| 大盤及央行（2） | `twpub_twse_taiex_raw`, `twpub_cbc_fx_reserves_usd_billion_raw` | 指數 `t→t+1`；外匯存底採原始新聞稿日期及下一個已驗證交易日 |

2014 配置再加 `twpub_cbc_m1b_yoy_pct_raw`、`twpub_cbc_m2_yoy_pct_raw`（央行各月原始新聞稿百分比）與 `twpub_dgbas_cpi_yoy_pct_raw`、`twpub_dgbas_unemployment_pct_raw`、`twpub_dgbas_gdp_yoy_pct_raw`（主計總處原始新聞稿百分比）。後三欄避免把官方 3.39% 先變成 0.0339；帶負號的 CPI/GDP 亦不截斷。原稿時間優先；2012–2017 制度內 08:30 原稿可在當日開盤前使用，16:00 或只有日期且無法證明 09:00 前發布者從下一交易日才可用。GDP 同一季度可有概估與修正發布，按各次原稿的發表日期順序更新，而非把最後修正值回灌整季。

2014 專項的 143 個 `twpub_*` 欄位逐欄決策數：22 欄選入（另有面板原有 5 個 OHLCV）、34 欄為轉換／比率／重複通道但使用者本次要求原值、51 欄是沒有 2014–2025 原始版本的快照、16 欄 TAIFEX 本機只有晚近捕捉、15 欄只有今日整包值而無可驗證舊版原值、1 欄重複成交量、1 欄只覆蓋 TPEx、3 欄全空。完整名稱、首末日、2014–2026 年度有值情形與原因，見 `2014_training_feature_decisions.csv`。其中「非原值而沒選」不等於源數據不可用；「快照目前無舊版本」則不能靠推定申報日修好數值版本。

不能把 27 欄說成「每個股票每天都有 27 個觀測值」。正式稀疏表在 2014 年有 384,517 個股票事件格，PE 283,953（73.8%）、PB 373,660（97.2%）、法人外資 302,815（78.8%）；2025 年有 538,339 格，對應 PE 344,171（63.9%）、PB 460,579（85.6%）、外資 496,522（92.2%）。2025-12-31 的 2,274 個股票／ETF 列中，317 個 `00*` ETF 全部沒有個股 PE/PB，非 `00*` 的 1,957 列仍有 543 個 PE 空值、23 個 PB 空值。IPO 前、ETF 沒有個股估值、官方不計算負盈餘 PE、停牌或來源沒有報值，都不得用未來數值補齊；需另存可觀測性遮罩才能無歧義地區分真零與缺值。

外匯存底原始新聞稿封存器已驗證 317／317 期原文與數值；正式特徵表另含一筆首次觀測的當期值，因此 `twpub_cbc_fx_reserves_usd_billion_raw` 有 318 個非空市場事件。這不是把 318 個月份都當作精確秒級發布時間。

全量稽核曾找到外資淨買超原值在 2017→2018 年的異常覆蓋跳變。根因是 TWSE T86 歷史表在同一來源檔混用舊欄名「外資買賣超股數」與新欄名「外陸資買賣超股數(不含外資自營商)」；舊程式只讀新名。現在依每一列實際存在的欄名取原始值，並將特徵契約升版迫使全量重建，不能沿用未修正的增量前綴。本機來源 3,500,405 列正好分成舊名 1,151,911 列、新名 2,348,494 列，沒有同列雙值；名稱／定義演變仍需在模型解讀時保留。最初 2005 年起的 22 欄稽核另有 4 個起始年覆蓋跳變高風險；改從第一個完整的 2013 年訓練，沒有放寬 25% 稽核門檻。

另有原值但**未加入** 2014 配置：TWSE 無對應的 TPEx 每股股利欄、`twpub_usdtwd_raw`、CBC M1B/M2 **水準**。後兩個水準來自今日單一歷史整包版本；相反，原始新聞稿的 M1B/M2 **年增率**在 2013–2026 連續且有每期舊值，現已開放給 2014 配置。整個 1999–2026 原稿仍因 2000-06 缺期而 `value_history_complete=false`，不等於 2014 區間不完整。因首次觀測門檻，`twpub_usdtwd_raw`、`twpub_cbc_m1b_raw` 和 `twpub_cbc_m2_raw` 目前在正式 PIT 表各只有 1 筆，不能把原下載檔的數千筆舊日期誤當可訓練歷史。既有快照型 MOPS 財報、營收、TDCC、內部人等亦不因推算申報期限而取得原始版本證據。

注意：稀疏 Parquet 保留真正的 `null` 與 Float64 原值；當前模型面板組 Float32 張量時仍把缺值填 `0`，因此「模型接到原值」是指不做 log/asinh/比率變換，**不是**十進位整數逐位無損，也只適用有觀測值的格子，`0` 不能單獨代表已觀測。若要比較缺值敏感策略，須另設缺值通道或受證明的訓練區間，不能把來源未發布視為真零。新配置不與舊 checkpoint 相容，且繼承的 naive 收盤成交簿只是研究近似，不是可執行 09:00 成交證據。

## 尚不能宣稱完整 PIT 的資料

`artifacts/data_quality/tw_public_provisional_macro/events.parquet` 目前有 27 種候選特徵及 40,805 筆事件，逐筆保留 `published_at_taipei`、`publication_time_basis`、`value_vintage_basis` 和 `strict_pit_eligible`；嚴格合格列仍為 0。它提供「先推定日期再補原文」的工作佇列，不是讓現值直接進入歷史回測的白名單。例如 CBC M1B/M2 471 個月份各有 319 個帶官方日期／時鐘線索、152 個推定發布日；DGBAS CPI 548 個月份有 272／276；GDP 178 季有 59／119。數值版本仍需逐次原始稿；依官方預告時間無法逆推出後來修訂前的數值。

| 未列入這兩個原值模型的類別 | 目前可得 | 不能直接開放的原因／下一步 |
|---|---|---|
| MOPS 財報、營收、內部人及公司快照 | 現況值與部分歷史期間、已捕捉後的快照 | 舊期的初版／修正版與首次時刻未證實；需逐次申報原稿或可驗證歷史封存 |
| TDCC、借券、注意處置等快照 | 本機自首次觀測後的版本 | 不可把首次捕捉前的現值回灌過去 |
| CBC M1B/M2 水準、DGBAS CPI/GDP 水準、MOF 貿易／稅收 | 可下載長期整包數值及部分公告日 | 公告時鐘不等於舊版數值；補每次原始新聞稿／附表後再升級 |
| CBC M1B/M2 原稿年增率 | 321 期原稿頁、319 個期間各有 M1B/M2 原值 | 只缺 2000-06；2013–2026 區間連續，已加入 2014 配置，但 1999 起的全史旗標仍為 `value_history_complete=false` |
| TAIFEX 期貨／選擇權及其他市場欄位 | 本機表主要自 2025/2026 捕捉、既有變換後特徵 | 免費官網有每日／年度行情下載可另建歷史；部分法人資料僅開放近三年查詢，更早歷史的官方申購另計，不能把晚近捕捉冒充 2014 全史 |

主計總處官方載明 2012 年起部分統計曾改在 08:30 發布，2017-07-01 起統一回 16:00；現有管線對原始文件時間優先，只有日期的原始稿才依該期間規則映射，故 08:30 已發布資料可在當日開盤前使用，不機械延遲一天。官方時間表也明示會變更或延遲，因此推定規則始終低於實際原稿時間。央行新聞稿可證明某月外匯存底原值及下一次預定 16:20，但不能把一篇的時間外推為所有歷史月份的精確秒數。

## 驗證與重跑

本次新增測試核對原值、負淨流量、舊／新 T86 表頭、`t→t+1` 單次位移、TAIEX 月／日檔一致性及兩套配置的時點／來源分類。生產特徵檔仍由既有 builder 與來源 receipt 維護，欄位版本變更會強制第一次全量重建；之後既有排程可走增量尾段。逐欄盤點不自動變更訓練白名單。

2026-09-18 的正式表為 9,594,667 列／143 特徵、可觀測欄 140，特徵契約 v10。`--strict --require-live-selected-features` 全量稽核結果：

| 原值配置 | 交易日 × symbol | 模型欄 | Walk-forward fold | 品質結論 |
|---|---:|---:|---:|---|
| **2014 起、現行 v10** | **3,102 × 2,755** | **27** | **12** | **`model_safe=true`；critical/high 0，medium 1** |

2013 起 22 欄與 2005 起 15 欄曾在 v9 表通過嚴格稽核（3,348／5,338 日，分別 13／21 fold），但不把舊版驗證結果當作 v10 的新證明。主力 2014 配置才是本次 v10 全量重驗對象。

2014 配置唯一的 medium 為下市公告與官方下市清單未全數交叉吻合，相關不確定性仍保留，不能解讀成完整下市事件資料。`model_safe` 是這個資料／時點稽核的結論，不是保證模型數值穩定、投資獲利或 naive 收盤成交真能執行。原始公告頁面於 2026 年抓取並驗證內容與公告日期；若官方在本機首次觀測前曾無版本號覆寫舊頁，單憑現存網頁不能絕對證明其最初位元組內容，這項歷史檔案限制仍須保留。

2014 的 27 欄配置 `train.py --check-data-only` 已接受 3,102 日／2,755 symbol／12 fold，沒有啟動模型、最佳化器或 checkpoint。舊兩個配置在 v9 各自也曾通過入口預檢，但不替代現行 ABI 驗證。入口通過不等於已有完成的模型訓練。

本次 v10 的新冷庫版本為 `tw-public-20260917T165313822462385Z-l0-penguin-469c697a141282c3`；本機已對 manifest 與 2,346 個引用物件（18,939,472,654 bytes）完成雜湊驗證。這是來源冷保存驗證，**未**執行冷庫 materialization，也**未**證實任何其他節點同步收斂或改動舊訓練 pin。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_tw_public_training_features.py --input-dir data_tw_public --output-path data_tw_public/features/tw_public_stock_daily.parquet --symbols-root data_tw_public/stocks --end-date 2026-09-17
run_fintech_python scripts/inventory_tw_public_feature_table.py --output-dir artifacts/data_quality/tw_public_raw_2014_v1
run_fintech_python scripts/classify_tw_public_2014_features.py --inventory-dir artifacts/data_quality/tw_public_raw_2014_v1
run_fintech_python scripts/audit_tw_public_data_layer.py --config configs/markets/tw_public_preopen_raw_2014_v1.yaml --output-dir artifacts/data_quality/tw_public_raw_2014_v1/model_audit --build-panel --strict --require-live-selected-features
run_fintech_python train.py --config configs/markets/tw_public_preopen_raw_2014_v1.yaml --check-data-only
```

## 官方依據

- [TWSE 每日收盤行情歷史查詢](https://www.twse.com.tw/zh/trading/historical/mi-index.html)：官網註明自 2004-02-11 提供；[TWSE 發行量加權股價指數月史](https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?date=20160901&response=html)。
- [TWSE 每日本益比、殖利率、淨值比](https://wwwc.twse.com.tw/en/trading/historical/bwibbu-day.html)：自 2005-09-02，且 EPS 非正時不計算 PE；[官網盤後估值產品](https://eshop.twse.com.tw/zh/product/detail/8a82e9e697fc5f620198abeec9830097)列出現行 18:00 產製時間。後者是收費產品，不能直接充當免費查詢端點的歷史秒級時間戳。
- [TWSE 融資融券餘額產品](https://eshop.twse.com.tw/zh/product/detail/388dd3a09824427d8c01a9d2b21e820b)列出現行約 21:00 產製；這支持盤後到下一交易日的方向，但仍不等同於免費查詢端點每一歷史日的精確發布秒數。
- [TWSE 2017 年 T86 原始欄名](https://www.twse.com.tw/fund/T86?date=20170614&response=html&selectType=ALL)、[現行 T86 欄名](https://www.twse.com.tw/fund/T86?response=html)可核對外資欄位變遷；單純只讀現行欄名會抹掉舊值。
- [TPEx 上櫃融資融券餘額](https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/transactions.html)及 [TPEx OpenAPI](https://www.tpex.org.tw/openapi/)；[TWSE OpenAPI](https://openapi.twse.com.tw/)列出可查公開產品。
- [主計總處 2017 發布時間變更公告](https://www.stat.gov.tw/News_Content.aspx?n=2642&s=97506)、[現行預告發布時間表](https://www.stat.gov.tw/News_NoticeCalendar.aspx?Dept=4527&n=3717)；前者證明 08:30／16:00 制度，後者證明預告日會調整。
- [央行原始外匯存底新聞稿示例](https://www.cbc.gov.tw/tw/cp-302-192552-e54df-1.html)列原值、發布日期與下一次 16:20 預定時間；[央行金融情況原稿示例](https://www.cbc.gov.tw/tw/cp-302-182976-efa40-1.html)直接記載 M1B/M2 當月年增率；[MOPS 財報更補正查詢](https://mops.twse.com.tw/mops/web/t120sb02_q10)說明財報並非不可修訂。
- [期交所選擇權每日／年度行情下載](https://www.taifex.com.tw/cht/3/dlOptDailyMarketView)註明每次日區間不超過一個月、年度 ZIP 可下載；[期交所法人歷史查詢](https://www.taifex.com.tw/cht/3/optContractsDateView?menuid1=03)註明免費查詢近三年、更早須申請，[歷史資料申購頁](https://www.taifex.com.tw/cht/3/hisAppForm)列出交易資料申購方式。
