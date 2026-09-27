# 資料取得分工與去重契約（2026-09-26）

後續修復與現行校驗門檻見 [2026-09-27 配額／排程協調](data_acquisition_coordination_2026-09-27.md)。
最新逐列匯出為 `artifacts/data_quality/data_acquisition_inventory_2026-09-27.csv`；下列數字保留為當時觀測，不代表現在進度。

本文件是**下載排程契約與當下量測**，不是宣稱全史完成。完整逐項清冊由既有唯讀 `/data-monitor/api/details` 和 `artifacts/live/data_monitor/public_status.json` 產生；不要另建第二套與實體收據分離的資料註冊表。2026-09-26 01:10（臺北）快照共 1,748 個註冊項、414 個 active-scope endpoint，面板健康為 `critical`。FinLab 目錄 1,110 鍵中 1,104 有本機收據，這是**鍵取得率**，不是逐檔逐日或可訓練率。FinMind Sponsor 原本 59 個全市場系列只列 64,212 個任務，但許多「整月／整年完成」實際只拿到單日。修正後正式佇列是 **299,163 個日期／事件任務**；01:22 的 SQLite 當下約 309 非空完成、11 確認空回、2 在途、298,841 待處理。前後分母不同，不可當成下載退步或已抓齊。

逐項 CSV 位於 `artifacts/data_quality/data_acquisition_inventory_2026-09-26.csv`（來源、endpoint ID、別名、負責服務、排程、狀態、覆蓋分母與首末實存）。更新時執行 `source scripts/runtime_env.sh && run_fintech_python -m scripts.export_data_acquisition_inventory --output artifacts/data_quality/data_acquisition_inventory_2026-09-26.csv`；此命令僅讀既有快照、不打供應商 API。`acquisition_role=*_unreconciled` 表示尚無足夠欄位／單位證據可宣稱跨源相同，並非故障。

## 判定相同資料的單位

只有 `(標的 ID、交易所／板別、交易日或右標時間、原始／調整價、幣別、數量單位、資料修訂版本、發布／取得時間)` 都相容，才能視為可比較的同一事實。欄位同名不構成去重證據。不同來源原始檔與收據仍獨立保留，訓練／正式發版另由 PIT、授權和雜湊 gate 決定。多源衝突只記稽核，不用較晚資料靜默覆寫官方或製造盤前可用時間。

## 取得責任與 API

| 資料／粒度 | 主取得者與 API | 次來源用途 | 執行／配額邊界 |
| --- | --- | --- | --- |
| 台股日原始 OHLCV、交易日、法人、融資券與公告 | TWSE `MI_INDEX`、TPEx 官方歷史端點；MOPS、TDCC、CBC、DGBAS、MOF、TAIFEX 各自官方端點，由 `download_tw_public_data.py`、來源事件監看及各專用下載器負責 | FinMind／FinLab 先補主來源起點之前、欄位或標的缺口，再驗證相同語意的交集 | 依每來源發布事件／交易日收據追新；休市不虛造新資料。官方歷史 `state/*.json` 的日期覆蓋不等於每檔完整 |
| 台股 1 分鐘 K、永豐逐筆與期權歷史 | Shioaji `api.kbars`／`api.ticks` 與訂閱串流；獨立 minute、historical、TX backfill 服務 | FinLab 已上架的 tick／分鐘股日分區只作缺口補充或稽核，不能因無成交分鐘強填假 K | 開盤保護即時服務；歷史 K 單次至多 30 曆日。共用同一人最多 5 連線與券商實際 `api.usage()` 位元組／速率，不把 50/10 秒當壓線目標 |
| FinLab 獨有研究特徵、歷史欄位 | `finlab.data.search()` 目錄、`data.get(key)` 或已上架的 `tw_tick:SYMBOL` 日期分區 | `price:收盤價` 首次可補早期缺口；已有本地收據後，其整表刷新排在缺失特徵與一般追新後作價量校驗 | VIP 每日 MB 配額從 SDK 狀態讀取；本機觀測 5,000 MB、08:00 臺北重置；保留 50 MB，停止時不等同完成。私人研究權限與冷發布契約另核 |
| FinMind 免費／Sponsor 台股、期權、總經 | `/api/v4/data`；Free 補充與 Sponsor 全市場日期分區分工，`user_info` 驗證帳號 tier／每小時額度 | Sponsor 批量查詢期間 Free 相同系列讓位；`TaiwanStockPrice` 在 TWSE／TPEx 日期覆蓋有正式收據的交集降為補缺／校驗；2004 年前、當期和未覆蓋區間仍優先 | 三服務共用 `finmind-v4-data` 行程間限流與 402/429 冷卻；本機帳號回覆 Sponsor 6,000/h。三大法人寬表由已驗證長表本機衍生，免第二次 API。新聞停用 |
| TAIFEX 期貨／選擇權官方日與 tick | TAIFEX 專用官方歷史／日資料服務 | FinMind FuturesDaily／OptionDaily 目前另存來源，不宣稱語意完全相同；做單位、合約及結算日對齊後方能判重 | 開盤／夜盤歸屬與券商期貨流量分開處理 |
| Crypto Binance／OKX／Bybit 1m、日、資金費率 | 三交易所各自官方 REST／公開封存，分交易所獨立事實 | 不跨交易所合併價格；封存與 REST 同交易所同 interval 優先 checksum／缺口補充 | 各端點獨立限流、冷卻與容量安全線；逐筆／L2／liquidation 不在目前必要抓取路徑 |
| 美股／FX／總經／其它免費來源 | Yahoo、Frankfurter、FRED、OpenBB、Dune、SEC／ETF 發行商及 `configs/free_public_data_sources.json` 的既有下載器 | 有端點身分／權利／單位相同證據才合併；OpenBB 為多供應商封存，非單一真值 | `registered-data` daily／intraday／features／backfill 分組；各憑證、容量和官方條款獨立，不把單一全域 10 req/s 套給不同來源 |

## 優先次序與安全條件

1. P0：正在交易的串流和官方已發布的新資料，不讓歷史回補搶登入、開盤資源或主來源配額。
2. P1：沒有主來源或早於主來源起點的歷史、缺少的欄位／標的／日期。不可用「檔案存在」「服務 active」「來源空回」代替完成。
3. P2：已存在本地資料但未達最新，按來源修訂／發布事件做增量；同一 provider 的 Free 與 Sponsor 不同時請求相同原始事實。
4. P3：有權限與剩餘配額時才重新查重疊資料；精確 `(symbol, date, grain, unit, adjustment)` 對齊後記錄差異、容許時差與來源版本，不自動覆蓋主來源。

FinMind Sponsor 先前有三個單日端點多送 `end_date`，共 10,001 任務被 `provider_bad_request` 封鎖。2026-09-26 對官方文件所列的 `start_date` 形狀各做一次有額度的實測，`TaiwanStockEvery5SecondsIndex`、`TaiwanStockGovernmentBankBuySell`、`TaiwanStockBlockTradingDailyReport` 都回 HTTP 200 與非空資料。下載器僅對這三類舊 query-shape 400 做**一次性**佇列遷移，沒有通用解除 4xx 封鎖；新 400 仍須人工檢查。隨後根據文件和本機收據證明，將 35 個原本錯用多日分區的「全市場只取指定日」系列改成逐日分區、去掉 `end_date`。舊月／年請求的 806 個收據另保存在 `data_finmind/sponsor/legacy_query_shape_receipts`；非空且資料日恰等於起始日的舊分區只算**那一天**，其它舊完成／空回退回待抓，不再虛稱整月／整年完整。歷史成功分區不自動每 30 天重抓；新增日持續增量。每 5 秒指數量大，與 1m K 不是同一粒度，仍需容量驗收。

相同優先級的 Sponsor 系列採資料集輪流派工，單一系列內從最新分區向舊資料補；輪轉游標保存在 SQLite。索引 `(priority,dataset,partition)` 使派工不必每次對整個優先級重排。2026-09-26 01:28 服務重啟後維持 `active`，佇列觀察到 345 非空完成、59 空回、4 在途、298,755 待處理；這只證明服務開始消化更正後的佇列，並不證明資料完整。原 Parquet 以內容 SHA-256 命名，舊收據仍可指向未被新日分區覆寫的原檔。

寬表轉換另用 2026-09-01 長表正式下載檔唯讀重算：與先前 FinMind 直接提供的同日寬表都是 **24,711 列**，按 `(date, stock_id)` 排序後**逐列全部相同**。這證明該日來源形狀的等值，不是其它日期／修訂版本的全量證明；未來每個衍生分區仍保留長表 SHA-256 驗證與獨立衍生收據。

當前多源校驗是 `scripts/audit_finmind_sponsor_overlap.py` 的 FinMind 日價對 TWSE／TPEx 已存日 OHLCV，比對不花新增 API 額度；其它來源尚無完整的交集驗證器。所有來源的全史完整、逐檔粒度、發布時刻／PIT、冷庫可還原與遠端訓練可用性都需要各自稽核；此表沒有把其中一個驗證當成另一個。

官方文件：[Shioaji 歷史資料](https://sinotrade.github.io/tutor/market_data/historical/)／[使用限制](https://sinotrade.github.io/tutor/limit/)、[FinLab 資料 API](https://finlab.finance/docs/reference/data/)／[盤後分區](https://finlab.finance/docs/en/details/intraday/)、[FinMind 資料集與查詢形狀](https://finmind.github.io/llms-full.txt)、[TWSE OpenAPI](https://openapi.twse.com.tw/)。

## 2026-09-26 08:03 臺北逐來源故障分層

最新唯讀快照有 1,749 個註冊列、414 個 active 資料端點、34 個群組彙總；81 個 active `provider` 顯示名稱包含複合來源，不是 81 組互不相干的 API 帳號。逐列依 `endpoint_id` 看上方 CSV 或 `/data-monitor/api/details`；`reference`、群組與實體檔案別名不可重複加入下載分母。以下只把可執行失敗與真實來源限制列為待修，不把研究用 PIT 未驗證說成傳輸故障。

| 來源／責任 | 本輪驗證的狀態 | 下一步與不能冒充完成的界線 |
| --- | --- | --- |
| FinMind Sponsor／Free | Sponsor 佇列曾把交易日價格、法人等 9 個系列的休市日當必要任務。以已驗 SHA-256 的 TAIEX 交易日檔排除 24,764 個零資料的非交易日任務；歷史週六交易日保留，財報／股利等非交易日可能發布的系列不套用此規則。服務持續非空成功，未完成分區仍在佇列。 | 面板逐系列顯示「已驗證非交易日排除」與真正分母；空回不算資料，舊官方日曆驗證失效會重新開放任務。Free、Sponsor 共用帳號限流；不可因已排除而宣稱數值完整。 |
| 永豐 Shioaji 股票 1m | 2020-03-23 的 8455 原始 Tick 有一筆價格 0／量 0 占位訊息，使舊 fallback 把整檔標成失敗。現在僅略過完全雙零的非成交、保留原始 Tick 與來源缺口；重跑後本輪 `failed=0`、`complete_with_gaps=89`，歷史稽核仍有 68 個正式日來源缺口及大量歷史合約不可用。 | 不造分鐘成交；保留缺口與不可查合約。2026-09-25 是證交所公告的中秋節休市，最新應有交易日為 9/24，不是漏抓 9/25。僅重啟歷史 worker，未重啟盤中串流或交易服務。 |
| TWSE／TPEx／TAIFEX／MOPS／央行等官方來源 | 多數已具獨立收據；27 個總體特徵只有候選歷史值，未達原始發布版本驗證；MOPS XBRL 本機 71/71 季不等於原始來源／申報時刻全驗。 | 以原始數值版本、申報／發布證據與修訂鏈稽核；不把「查得到今天的值」回填成當年已知。 |
| FinLab | 07:59 前目錄 1,110 鍵中 1,104 有本機資料；6 鍵未得，其中 2 個是 `tw_minute:2330`／`tw_tick:2330`，2 個一般鍵因超大表預先暫緩。前一日配額剩 48.95 MB，低於原設定 50 MB 保留額，並非登入失效。08:00 新目錄變為 1,111 鍵，刷新服務已實際啟動、開始消耗新日 5,000 MB 額度。 | 既有排程繼續逐鍵抓，檢查本輪結束收據；維持逐鍵失敗／額度收據，不用已存鍵數假裝逐檔全史完整，也不繞過超大表容量保護。 |
| Binance／OKX／Bybit | 三交易所 1m 與其獨立衍生資料持續增量；歷史與場館／標的生命週期缺口仍顯示 `catching_up`／`unable`。 | 不跨交易所合併價格；逐筆、L2、強平不在目前必要抓取路徑。 |
| OpenBB／FMP | OpenBB 歷史任務持續回補；FMP 上游方案回 HTTP 429 時在來源冷卻，不是本機 10 req/s 可以強行解除。 | 冷卻屆滿後既有排程續抓，不能繞過來源方案限制。 |
| Dune／SEC／ETF 發行商 | SEC／發行商可得系列有收據；3 個 Dune SQL 查詢明確回「訂閱不支援 API SQL 執行」。 | 需要可執行 SQL 的帳號權限或另行證明等價公開端點；不以空表標成完成。 |
| Yahoo、Pepperstone、Stooq、GDELT、NOAA 等 | 部分 1m／tick 端點只有註冊，尚無可執行管線；Stooq 權利、天氣／事件映射仍未核定。物理清冊另有 Yahoo crypto 17 個及舊台股 feature 2 個 Parquet footer 損毀。 | 已註冊不代表可抓；不刪損毀原檔，先尋找可驗證備份或重新取得來源，再以收據修復。Yahoo crypto 不取代指定三大交易所的原生 1m。 |

本輪修復只改變有證據的排程與占位資料處理；網站 `health=critical` 仍如實反映上述 19 個實體 Parquet 壞檔，不能因 downloader 活著就降成綠燈。公開面板只提供來源與收據摘要，不暴露 API 金鑰或原始研究資料。
