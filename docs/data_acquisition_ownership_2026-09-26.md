# 資料取得分工與去重契約（2026-09-26）

本文件是**下載排程契約與當下量測**，不是宣稱全史完成。完整逐項清冊由既有唯讀 `/data-monitor/api/details` 和 `artifacts/live/data_monitor/public_status.json` 產生；不要另建第二套與實體收據分離的資料註冊表。2026-09-26 01:10（臺北）快照共 1,748 個註冊項、414 個 active-scope endpoint，面板健康為 `critical`。FinLab 目錄 1,110 鍵中 1,104 有本機收據，這是**鍵取得率**，不是逐檔逐日或可訓練率。FinMind Sponsor 59 個全市場系列的 64,212 個任務中，當時 309 非空完成、371 空回、4 在途、63,528 待處理；即使程序在跑也遠非抓齊。

若需要逐項 CSV（來源、負責服務、排程、狀態、覆蓋分母與首末實存），執行 `source scripts/runtime_env.sh && run_fintech_python -m scripts.export_data_acquisition_inventory`；此命令僅讀既有快照、不打供應商 API。`acquisition_role=*_unreconciled` 表示尚無足夠欄位／單位證據可宣稱跨源相同，並非故障。

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

FinMind Sponsor 先前有三個單日端點多送 `end_date`，共 10,001 任務被 `provider_bad_request` 封鎖。2026-09-26 對官方文件所列的 `start_date` 形狀各做一次有額度的實測，`TaiwanStockEvery5SecondsIndex`、`TaiwanStockGovernmentBankBuySell`、`TaiwanStockBlockTradingDailyReport` 都回 HTTP 200 與非空資料。下載器現在僅對這三類舊 query-shape 400 做**一次性**佇列遷移，沒有通用解除 4xx 封鎖；新 400 仍須人工檢查。每 5 秒指數量大，與 1m K 不是同一粒度，仍需容量驗收。

寬表轉換另用 2026-09 長表正式下載檔唯讀重算：與先前 FinMind 直接提供的同月寬表都是 **24,711 列**，按 `(date, stock_id)` 排序後**逐列全部相同**。這證明該月來源形狀的等值，不是其它月份／修訂版本的全量證明；未來每個衍生分區仍保留長表 SHA-256 驗證與獨立衍生收據。

當前多源校驗是 `scripts/audit_finmind_sponsor_overlap.py` 的 FinMind 日價對 TWSE／TPEx 已存日 OHLCV，比對不花新增 API 額度；其它來源尚無完整的交集驗證器。所有來源的全史完整、逐檔粒度、發布時刻／PIT、冷庫可還原與遠端訓練可用性都需要各自稽核；此表沒有把其中一個驗證當成另一個。

官方文件：[Shioaji 歷史資料](https://sinotrade.github.io/tutor/market_data/historical/)／[使用限制](https://sinotrade.github.io/tutor/limit/)、[FinLab 資料 API](https://finlab.finance/docs/reference/data/)／[盤後分區](https://finlab.finance/docs/en/details/intraday/)、[FinMind 資料集與查詢形狀](https://finmind.github.io/llms-full.txt)、[TWSE OpenAPI](https://openapi.twse.com.tw/)。
