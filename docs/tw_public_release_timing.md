# 台股公開資料：歷史覆蓋與發布時刻契約

`scripts/audit_tw_public_source_catalog.py` 是目前註冊來源的完整、可重跑清單。它列出端點、來源、歷史模式、本地列數及下載器的缺口紀錄：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_public_source_catalog.py --root data_tw_public --format markdown
run_fintech_python scripts/audit_tw_public_source_catalog.py --root data_tw_public --format json
```

## 「資料日期」不是「發布日期」

模型在交易日 `D` 的 09:00 台北時間決策時，只能使用在 `D 09:00:00` **之前**已公開的該版內容。恰好 09:00:00 或之後才公開者，歸下一個經官方資料證實的交易日；週末與休市日也是同樣的交易日映射。若策略在 08:30 已送委託，該策略的實際資訊截止應改為 08:30，不能套用 09:00 的研究截止。證交所的[一般交易與委託時間](https://www.twse.com.tw/zh/products/system/trading.html)分別是 09:00–13:30 與 08:30–13:30。

原始資料需區分：`subject_period`（統計所屬月份/事件日）、`published_at`（來源公告時刻）、`first_observed_at`（下載回應完成時刻）、`payload_sha256`（該版內容）與 `effective_session`（可用交易日）。實際公告時刻優先；有當年官方逐期預告可重建「預定發布日」但不能假定一定準時；再退而使用第一次觀測到**該版內容**的時刻。純粹由平均落後天數推估的日期只能用於抓取排程/ETA，不能標為已證實的歷史可用日。

所有公開來源另遵循同一個**研究候選日期**優先序：來源可核對到該筆／該版的實際公告日期時間 > 來源的實際公告日加明示為推定的時鐘 > 官方逐期預告或文件內可辨認的核准日推定 > 同期發布習慣的統計估計 > 有歷史適用證據的法定期限代理。新找到精確紀錄時，依來源檔與內容雜湊升級候選，保留舊估計與依據；不能只改標籤。找不到所屬期、對象或適用規則時保持未知，不借用另一資料集的發布日。這個候選序列可供探索與補抓排程；嚴格 PIT 訓練仍要求當時數值版本、真正公告／首次觀測證據與策略決策時鐘，不能用推測日期倒填。`artifacts/data_quality/tw_public_provisional_macro/events.parquet` 已逐列標示宏觀資料的候選證據等級；MOPS XBRL 另見下方逐文件候選表。

目前的 `configs/markets/tw_public.yaml` 是獨立的 **same-close 研究近似**，它仍允許同一交易日的完整 OHLCV/收盤聚合值與當日收盤成交假設。它不是 09:00 開盤前策略，也不能因本次修正快照/總經時間就宣稱整個模型無前視。真正的開盤前策略需另建決策時鐘與 panel/view：前一日收盤資料可用，當日開盤價及當日完整量價不可用；如果 08:30 已下單，截止時間還要再提前。不要在同一模型版本中偷換這個語義。

這不是任意多等幾天：同日 08:59:59 可用，同日 09:00:00 不可用。MOPS 重大訊息的逐筆 `發言日期` 和 `發言時間`可還原公告時鐘；沒有時鐘的快照以回應完成時間作上界。交易所盤後日資料須歸下一交易日。修訂過的月/季統計，單靠原始發布排程**無法重建修訂前數值**；必須封存當時版本。主計總處曾於[2012-05-07 改為 08:30](https://www.stat.gov.tw/News_Content.aspx?n=3703&s=23721)，又於[2017-07-01 統一改為 16:00](https://www.stat.gov.tw/News_Content.aspx?n=2642&s=97506)；不能把現行時刻套回全部歷史。

## 已註冊資料與邊界

目前下載器註冊 162 個產品：11 個 TWSE/TPEx 官方逐日歷史查詢、2 個下市櫃歷史清單、133 個交易所/期交所/集保快照端點、13 個 data.gov 資源，以及 3 個官方逐期新聞稿封存器。新加入的 3 個基金/ETF 開放資料是現況快照，不能當成過去每月持股歷史。註冊數和本地檔案數都**不是**歷史完整性證明。TWSE/TPEx 逐日交易、指數、融資融券、三大法人、估值、當沖標的可以按日期查；後三類不能因為欄位內有「交易日」就誤當開盤前公開。MOPS 公司資訊、財報、營收、股利、內部人、借券、注意/處置及總經資料，多數端點是現況/整包歷史值，需另保留每次修訂版及其時鐘。

其中 159 個市場/整包產品涵蓋 `download_tw_public_data.py` 的 `DEFAULT_DATASETS`，另 3 個是 `dgbas_release_vintages`、`cbc_fx_reserve_release_vintages` 與 `cbc_money_release_vintages`。這仍**不包含**獨立的公司行動/增減資與股份交換下載器、TAIFEX 歷史腳本，以及需永豐帳戶的 Shioaji 分鐘/逐筆資料。這些不能因為同在專案內就混稱「免費公開且歷史完整」；各自仍需來源、授權、日期覆蓋與 PIT 稽核。

2026-09-16 本機檢查：11 個逐日產品在各自現存首末日間，均沒有遺漏已驗證 TAIEX 交易日。特別注意：`tpex_margin_balance` 原先有 331 日（2007-06-01 至 2008-09-29）被**錯誤**標成 `official_endpoint_archive_gap`；重新直接查官方端點確認仍可取得，例如 2007-06-01 有 409 列、2008-09-26 有 412 列。程式已撤銷硬編碼例外並重新補抓；現有 3,236,665 列、5,694 個交易日，對照同區間 TAIEX 交易日缺 0。舊 `raw_empty` 回應/日誌保留為調查證據，但不再能滿足覆蓋。這是 2026-09-16 的本機快照，後續以即時重跑清單及 `data_tw_public/state/*.json` 為準。

公開可看的主要資料面：

| 類別 | 主要官方入口 | 歷史下載的真實邊界 |
|---|---|---|
| 上市/上櫃日行情、指數、估值、法人、融資融券、當沖標的、注意/處置 | [TWSE OpenAPI](https://openapi.twse.com.tw/)、[TPEx 市場資訊](https://www.tpex.org.tw/zh-tw/market-infomation.html) | 逐日查詢與現況快照要分開；本庫 11 個逐日產品之外的快照不保證舊版。 |
| 公司重大訊息、營收、財報、股利、基本資料、股權變動、法說 | [公開資訊觀測站](https://mops.twse.com.tw/mops/web/t120sb02_q10) | 網頁有歷史查詢，但每一類要另驗證可批次取得、公告時刻與修訂版；目前不能宣稱全都已完成歷史下載。 |
| 集保股權分散、質押、庫存/交割 | [TDCC 股權分散表](https://www.tdcc.com.tw/portal/zh/smWeb/qryStock)、[TDCC OpenAPI](https://openapi.tdcc.com.tw/) | 官方查詢留存/現況資源與多年逐版檔案不是同一件事。 |
| 台指與選擇權日行情、結算、未平倉、法人 | [TAIFEX 歷史下載](https://www.taifex.com.tw/file/taifex/event/cht/download/download.html) | 不同產品的免費歷史窗口不同；例如[三大法人依日期頁](https://www.taifex.com.tw/cht/3/totalTableDateView?menuid1=03)明載線上僅供交易日前三年，早年資料需另外申請。 |
| 匯率/外匯存底/貨幣、CPI/GDP/就業、海關/稅收 | [央行](https://www.cbc.gov.tw/tw/mp-1.html)、[主計總處發布預告](https://ws.dgbas.gov.tw/win/dgbas03/bs7/calendar/calendar.asp)、[財政部發布規則](https://www.mof.gov.tw/singlehtml/979b54e408fb499eae3c1d9efe978868?cntId=9c62e8eb502c4600b8490a829dfbeefe) | 統計期間和現值可下載，不等於有每期初版/修正版；原始逐期公告及版次才是歷史 PIT 所需。 |

[TDCC 股權分散表](https://www.tdcc.com.tw/portal/zh/smWeb/qryStock)說明它是每週最後營業日營業結束後的持有結構，網頁歷史檔僅保留一年；這不是「資料日＋7 天」的正式發布規則，也不保證能向官方補回多年逐版紀錄。[財政部進出口統計](https://www.mof.gov.tw/singlehtml/979b54e408fb499eae3c1d9efe978868?cntId=9c62e8eb502c4600b8490a829dfbeefe)說明初步值通常在每月 7–9 日 16:00，確切日期須看逐月預告；[央行外匯存底新聞稿](https://www.cbc.gov.tw/tw/cp-302-191217-4ef14-1.html)也逐次公告下一次發布時刻。不可把 45/90 天固定延遲或月份結束日冒充精確發布日。

## 原始公告封存與本次實際覆蓋

- [主計總處 CPI](https://www.stat.gov.tw/News.aspx?n=2668&sms=10980)、[失業/人力資源](https://www.stat.gov.tw/News.aspx?n=2706&sms=11041)、[經濟成長/GDP](https://www.stat.gov.tw/News.aspx?n=2677&sms=10980)：`download_tw_dgbas_release_archive.py` 封存官方逐篇清單、文章與選定原始附件；原始 PDF 可讀出舊 CPI／失業率值，以及多數 GDP／失業率公告的實際發布時鐘，而非倒填現在的整包表。排除一筆被誤識別為數值公告的「提前發布通知」後，2026-09-17 已保存真正的公告 649/649 篇，所引用的原稿及附件共 1,241 個檔案通過 SHA-256 稽核；CPI、失業率、GDP 分別有 272、259、118 筆可解析原稿值，三類合計 649 筆。先前 Cloudflare HTTP 429 的缺口已在官網恢復回應後補齊；被排除通知的原始 DOC 仍保留在下載工作區，不以數值公告計入分母。
- [央行新聞稿索引](https://www.cbc.gov.tw/tw/lp-302-1-1-20.html)：`download_tw_cbc_fx_release_archive.py` 已重新查完官方 392 頁索引，封存 317 篇外匯存底原始公告，2000-04–2026-08 每月 317 個數值、缺期 0、失敗 0，`state/cbc_fx_reserve_release_vintages.json` 標記本次索引完整。舊稿中中文數字金額按原文換算；遇清單標題寫錯月份，以文章正文的月份更正，同時保留清單月份與更正旗標。`--cached-list-pages` 只用於補抓剛保存的清單所缺文章，不能宣稱重新確認最新清單。
- 同一[央行原始新聞稿索引](https://www.cbc.gov.tw/tw/lp-302-1-1-20.html)另以 `download_tw_cbc_money_release_archive.py` 封存 M1B/M2 **當月年增率**。2026-09-17 完成 392 頁全索引原文掃描後，以快取原索引重解析 321 篇文章；1999-12–2026-07 共 319 個不同月份有可驗證值，仍缺 `2000-06`，另有一篇提前發布通知、同一月份兩個內容相同的鏡像頁。原始位元組／發布日／雜湊稽核已通過，但 `value_history_complete=false`，不得宣稱整段連續無缺；現值的 M1B/M2 水準也沒有從年增率倒推。舊版解析器一度把「累計平均」誤認為「當月年增率」，已改為只接受當月正文語句，無法確認時留原文且不產生模型值。只有日期、無逐篇可信發布時刻的稿件，在其後首個已驗證交易日可用；文章預告的 16:20 是未來**預定**時刻，不是此篇實際上線時間證據。`--cached-list-pages` 僅是失敗後離線重解析，不證明索引最新；日常抓最新兩頁，週日做全索引重掃。
- 原始稿明載的確切時刻優先；無逐篇時刻者，按當年的官方時間表推定：2012-05-07 至 2017-06-30 的 CPI、失業率及標題明確寫「概估」的 GDP 為 08:30，同期間 GDP 初步統計維持盤後時刻；2017-07-01 後統一 16:00。其餘日期不臆測開盤前時刻，只映射到公告日之後首個已驗證交易日。央行外匯存底原稿金額為「億美元」，特徵與既有央行表一致換算成十億美元。CPI、失業率與 GDP 的原稿 headline 百分值作為各自的年增率／失業率特徵；GDP 水準值仍不可從年增率推造。
- 歷史官網保留的新聞稿比「現在整包表」更接近當年可見版本，但官網未提供每篇自發布起不可變的密碼學證明；若舊稿曾被原站靜默替換，僅憑今天下載的檔案仍無法證明其最初位元組。資料集保留首次抓取時刻、URL、SHA-256、清單與原稿，並將這個限制與真正當年封存的 vintage 分開標示。
- 上述 Parquet 存在於 catalog 註冊的可寫 live source；這**不是** audited packed 冷庫 release、訓練重新啟用或面板服務已重啟的證明。發布前仍須完整來源/特徵稽核及正常原子發布流程。來源網站拒絕自動存取時，不繞過網站限制，也不製造通過的收據。

其餘可查到舊期、但**尚未建立完整原始版本管線**的高價值來源：MOPS 各類逐筆公告與財報（需逐類查公告時鐘、修正與撤銷）、央行 M1B/M2/利率等逐月新聞稿、財政部貿易/稅收逐月新聞稿與原始 PDF、集保股權分散官方僅一年線上歷史、期交所各期貨/選擇權資料在不同產品上的不同線上留存期限。`data.gov` 或 OpenAPI 目前整包表即使有數十年資料，也不能填補這些「當年版內容」的證據。不能把這份候補清單算入已下載完成數，也不能把需要授權的永豐行情當成免費公開歷史。

## 實作狀態與驗收

### 09:00 前資訊的訓練候選設定

`configs/markets/tw_public_preopen_pit.yaml` 繼承原本的模型與回測參數，但使用獨立實驗/檢查點目錄。它只選擇 35 個有來源分類的模型欄位，不把尚缺歷史版本的欄位保留成永遠為零的假訊號；當日開盤價與完整日線/聚合值共 23 欄一律移至下一個經官方確認的交易日。前一交易日盤後融資融券/法人欄位已在來源建表時移至下一交易日，不能再位移第二次。央行外匯存底兩欄有 317/317 筆原始逐期新聞稿支撐；主計總處原稿現也補齊，但其特徵是否選入模型仍須更新設定並重新跑嚴格 PIT 稽核。其他沒有舊版證據的資料仍不進入此設定。

2026-09-17 實際驗收：以同一把來源更新鎖完成 live 特徵重建與嚴格稽核，輸出 9,592,140 列、5,337 個交易日、2,755 個標的；`artifacts/data_rebuild/preopen-pit-20260917-locked/audit/summary.json` 為 `model_safe=true`、critical/high 皆 0、medium 1。唯一 medium 是下市公告和融券回補通知並非全面交叉覆蓋，不能由下市本身推造通知。`train.py --check-data-only` 接受 35 個特徵與 21 個 folds，並在來源實體路徑正規化後重用有效面板快取。冷庫已原子發布精確版本 `tw-public-20260916T171214687367304Z-l0-penguin-028dc00cc959f551`，完整驗證 2,198 個封裝物件；該發布沒有切換 live 工作區連結。

2026-09-17 12:48 歷史紀錄：上述冷庫版本是**先前**固定釘選的資料，不代表當時新增的 649 篇主計總處原始稿已發布至冷庫。當時 live 工作區已用 649/649 篇公告重建 9,592,140 列特徵，其中 CPI／失業率／GDP 原稿值分別有 272／259／118 筆；`artifacts/data_quality/tw_public_preopen_pit_live_20260917_649/summary.json` 的**已選 35 欄**完整 panel 稽核再次為 `model_safe=true`、critical/high 皆 0、medium 1。當時仍有 17 個全空特徵；此段是舊快照而非現在的全空欄位數。新的不可變冷庫發布仍須單獨經正常來源、完整性及原子發布閘門。

2026-09-17 14:45 live 更新：新增央行 M1B/M2 原稿後又重建相同 9,592,140 列，兩個當月年增率欄各有 319 筆原稿事件，實際全空特徵由 17 降為 15；原稿唯一查無月份仍是 2000-06。這個 live 工作區變化尚不代表原先固定冷庫版本或既有已訓練模型同步更新。

若要固定這次實驗，使用 `configs/markets/tw_public_preopen_pit_pinned_20260916.yaml`，其資料路徑直接指向上述精確版本，寫入型面板快取則在 `artifacts/cache`。先執行 `./scripts/run_data_cache.sh use tw-public --snapshot-id tw-public-20260916T171214687367304Z-l0-penguin-028dc00cc959f551`，確認完整解封與租約；再對固定設定執行 `train.py --check-data-only`。本機已對 145,713 個來源檔完成 materialized 驗證並取得七天租約；固定版的 `artifacts/data_quality/tw_public_preopen_pit_pinned_20260916/summary.json` 也通過 `--build-panel --require-live-selected-features --strict`，仍只有同一項 medium，固定版訓練前檢查以約 6 秒重用獨立快取並接受 21 folds。不要將執行中的 `data_tw_public` 連結切向 materialized tree，且不要在實驗中把固定 ID 改成 `latest`。未取得不可變資料的其他機器必須各自解封並驗證，不能據此宣稱 fleet 皆已可訓練。

原始公告收據中的 live 絕對路徑只保留為來源線索；搬到不可變版本時，稽核僅准其重新定位到同一資料集的 `raw/<dataset>/` 相對目錄，再以 SHA-256 驗證位元組，拒絕 `..` 與 symlink 逃逸。這使冷庫解封資料可驗證而不讀回後來改變的 live 原檔。

這是「09:00 前已知的**模型輸入**」契約，不是已證明能在 09:00 成交的交易策略。它繼承的 `naive` 帳本仍用收盤價作成交代理；要宣稱可執行的開盤策略，還需要獨立的委託/撮合/容量與成交時鐘，並依實際下單截止（可能是 08:30）重新限定資訊集合。

正式資料更新後，必須對**同一組實體 live 路徑**執行下面的稽核；`--require-live-selected-features` 不允許已選欄位悄悄變成零填充，`--strict` 對任何 high/critical 問題回傳非零。稽核只證明目前 live 工作區的模型資料，不等於 immutable packed release 或長期訓練已固定版本。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_official_release_archives.py --root /srv/stockagent-live/data_tw_public
run_fintech_python scripts/audit_tw_public_data_layer.py \
  --config configs/markets/tw_public_preopen_pit.yaml \
  --public-dir /srv/stockagent-live/data_tw_public \
  --parquet-root /srv/stockagent-live/data_tw_public/stocks \
  --public-feature-path /srv/stockagent-live/data_tw_public/features/tw_public_stock_daily.parquet \
  --output-dir artifacts/data_quality/tw_public_preopen_pit \
  --build-panel --require-live-selected-features --strict
run_fintech_python train.py --config configs/markets/tw_public_preopen_pit.yaml --check-data-only
```

特徵建置器以解析後的輸出路徑取得跨行程鎖，避免排程與手動重建同時寫同一個 `.tmp`。增量尾段只有在來源依賴、輸出版本與股票名單收據均未改變時才可重用舊前綴；一般來源以完整 Parquet 內容計算收據，主計總處／央行的逐期公告另以**模型實際使用的值、發布日、時鐘與證據資格**計算語義收據。官方 HTML 外框改版仍存原始雜湊與檔案並獨立稽核，但不會只因外框位元組變更就重建全表。若依賴值改變、又無逐分區更正範圍證明，仍退回全量重建以免歷史修正留在舊前綴。要恢復安全的高效增量，須建立逐日期／分區的不可變來源收據，不能直接放寬這個條件。

- 快照與 data.gov 回應改以內容雜湊做不可變版本；同日更正保留兩版，連續抓到相同內容則不重寫；即使更正後又回復舊內容，也保留「回復」的新觀測時刻。新版本從此後開始累積；過去被覆蓋的版本不會憑程式變更自動復原。
- 特徵可用日採 09:00 台北時間嚴格界線；週末/休市公告映射到官方索引或雙市場當沖清單已證實的下一交易日。重大訊息使用逐筆發言時刻。月/季與 TDCC 不再假造 `+45/+90/+7` 天；沒有當年原始版本時只能從首次觀測日起使用該版內容。
- 要讓**其餘**舊月/季值在早年模型中「當時可用」，仍須逐期取得官方公告/新聞稿與當時數值版本，建立 period × release × revision 的證據表。上述 CPI/央行外匯存底僅對實際封存、能解析原始數值的期間提供此證據；其他資料只有規律推測、沒有逐期公告或原始版時，仍屬研究推測，不能通過嚴格 PIT 驗收。
- 本次只就官方 `tpex_margin_balance` 在 live 來源補抓 331 日；**沒有**覆蓋正式特徵表、重啟服務或宣稱完成冷儲存發布。後者仍需正常資料稽核與原子發布流程。修改後先跑 `test/test_tw_public_training_features.py`、`test/test_tw_public_data_downloader.py`、`test/test_tw_public_data_layer_tools.py`，並用正式資料跑一次來源/特徵稽核。
- 2026-09-16 驗證補充：一份獨立的 163 個來源 Parquet 固定快照完成全量特徵重建，最終產出 9,592,140 列、116 個特徵、`availability_contract_version=4`，暫存驗證摘要位於 `/tmp/tw-public-feature-smoke.EuBAHd/summary_v4.json`。在 2026-09-16 截止時，TDCC 模型值首次出現於 2026-07-20；MOPS 重大訊息值首次出現於 2026-07-17；匯率、CBC 隔夜利率與月度總經值均為 0 列，因目前的整包歷史資料首次觀測在當日 09:00 之後，下一個可用交易日尚未進入建表截止。這是嚴格歷史版本不足的訊號，不應用資料日期或固定延遲補回。最終全量重建約 5 分鐘、行程 RSS 峰值約 34 GB；日常應用既有增量尾段流程，且必須從固定來源快照或原子發布版本讀取。
- 新增官方原始稿後，以相同固定來源快照加兩個逐期公告 Parquet 完成 `availability_contract_version=5` 全量重建；結果仍是 9,592,140 列、116 個特徵，摘要及輸出分別在 `/tmp/tw-public-feature-smoke.EuBAHd/summary_v5.json` 與 `features_v5.parquet`。v4 的市場 CPI 年增率/央行外匯存底原始稿特徵都是 0 列；v5 分別有 265 與 317 個已發布交易日事件。CPI 第一筆在 PDF 明載 2004-02-05 16:00 發布後的 2004-02-06，不在 2 月 5 日開盤前；央行 2026-09-04 公告的 2026-08 月值在 9 月 7 日才進入特徵。這是隔離快照 smoke，**沒有**覆蓋 live 訓練特徵或宣稱整個 same-close 策略是嚴格 pre-open。
