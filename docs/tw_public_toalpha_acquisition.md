# 台灣公開資料與 ToAlpha：下載責任及驗收順序

本計畫按「可合法取得的原始值、當時可見的版本、可重跑的證據、對策略的增量價值」排序。來源可以公開查詢，不代表可以大量自動擷取；資料期別也不等於公告時刻。

## 來源分工

| 優先 | 資料 | 取得方式與目前實作 | 歷史與品質界線 |
|---|---|---|---|
| P0 | TWSE/TPEx 日行情、法人、融資融券、估值、當沖規則 | 沿用 `download_tw_public_data.py` 的官方逐日來源與既有稽核；不重抓 ToAlpha | 各來源首日不同；每日交易日覆蓋看 `state/*.json`、原始收據及日曆稽核 |
| P1 | MOPS 月營收、產品別營收、三大財報原值、重大訊息、法說、內部人、股利 | 優先官方開放資料與 MOPS/XBRL 正式批次出口；現有 TWSE/TPEx OpenAPI 快照繼續逐版留存。正式歷史批次須先驗證下載方式及使用授權 | 目前快照不能重建首次擷取前的修訂版；尤其不能把 2026 年抓到的舊月份現值放進舊回測 |
| P1 | 公司行動及現金發放條款 | 沿用 `download_tw_corporate_action_entitlements.py`；逐項核對未配對事件與原始申報 | `coverage_complete` 是請求覆蓋，不表示現金條款全數精確匹配；另看 `unmatched_mops_cash_events` 與 `cash_events_without_exact_terms` |
| P2 | 境內基金身分、ISIN、月報網址、ETF 基本資料 | 新增 3 個 `data.gov.tw` 開放資料集至既有 `tw-public` 下載器：`21404`、`43476`、`157399` | 均為當期快照。首次擷取前的歷史版本未知；這三份也不是基金前十大、ETF 每日實際持股或 PCF |
| P2 | 基金月前十大、主動式 ETF 每日實際持股、發行人 PCF | 先核對公會、交易所與發行人提供的日期、檔案及可用方式，再加官方來源收據 | PCF 是申贖籃子，不保證等於實際投資組合；從首次擷取日起保存每版，不倒填歷史 |
| P3 | 分析師共識衍生值、券商評等/目標價、ToAlpha 精選新聞 | `scripts/query_toalpha_unique.py` 單次唯讀 MCP 查詢，使用 `.env` 中的 `TOALPHA_MCP_API_KEY` | 僅當下研究查詢；不寫入 `tw-public`、不做全市場巡迴或歷史訓練。原始分析師逐筆預估值與券商報告全文沒有在 MCP 回傳 |
| 自算 | 成長率、財務比率、估值位階、選股/排名、ETF 加減碼、投組風險 | 從已驗證原始資料計算並固定公式版本 | 同時保存來源版本、單位、股數還原規則與決策時鐘；ToAlpha 結果只作抽樣核對 |

ToAlpha 的 [主 MCP 文件](https://toalpha.tw/mcp-docs)說明工具範圍、逐次回傳上限與金鑰額度；[MOPS MCP 文件](https://toalpha.tw/mcp/mops)列出申報資料範圍。[ToAlpha 條款](https://toalpha.tw/terms)禁止大量自動擷取及重製網站內容。證交所[使用條款](https://www.twse.com.tw/zh/terms/use.html)限制未經同意的自動網站下載；政府資料開放平臺的資料集另有明示開放授權。官方 [XBRL 整批下載說明](https://dsp.twse.com.tw/public/static/downloads/announcement/official/R-1130603962-1.pdf)證明有批次入口，但仍須逐一核對機器可用性、版本及權利。

## 實作與操作

三個新開放資料集由現有下載器處理原始回應、內容雜湊、不可變收據、Parquet 與版本時間。首次試抓（2026-09-16，暫存目錄）取得基金月報索引 4,427 列、基金基本資料 4,427 列、ETF 基本資料 271 列；兩份基金檔的 `年月` 均只有 `202608`。重跑沒有新增列。2026-09-17 00:34–00:35（臺北）的正式 `preopen_all` 掃描選取 159/159 個來源；00:38 來源監控器又套用一份期交所新版，所以在 00:55 完成第二輪全量掃描。截至 2026-09-16 的最新下載報告為 158 個 `ok`、1 個 `up_to_date`、0 個失敗與 0 個缺日；後續來源探測顯示 0 個未套用事件與 0 個批次後異動。

來源目錄稽核將這 159 個產品分成 137 個「首次擷取起累積的當期快照」、11 個官方逐日查詢、2 個下市櫃歷史清單，以及 9 個會修訂的整包歷史表。另有主計總處與央行逐期原稿封存：央行 317/317 期完成；主計總處 267/650 期，官方網站回傳 Cloudflare 429 後停止自動請求，仍缺 383 期。嚴格模型稽核於 00:45 回報 `model_safe=false`，其中 2 個重大問題是來源更新後的股票與特徵建置收據過期，另 2 個高風險問題是主計總處與財政部沒有足夠的逐期原始版本。前兩者走既有重建與複驗；後兩者不能用現在的整包表倒填歷史。`tw_public_verified_features_20260917.yaml` 明確選取 33 個目前有來源、時點與面板契約的欄位，其他原始資料仍保留於 release。研究設定仍沿用原本不可實際成交的 same-close 近似語義。

penguin 於 00:58 將第二輪來源掃描發布為 `tw-public-20260916T165816562965668Z-l0-penguin-53527a472d53e98e`，但清冊交叉比對發現 `twse_institutional_trades.parquet` 在特徵重建後由全量掃描更新，導致特徵收據指向舊內容。此 release 保留作來源證據，不作訓練版。盤前 PIT 流程接著重建 live 特徵；01:12 再發布 `tw-public-20260916T171214687367304Z-l0-penguin-028dc00cc959f551`。新版本的股票建置收據中 8 個原始來源與 5 個生命周期來源，以及特徵收據中 168 個來源，均與冷庫 inventory 的雜湊相符，freshness 為 2026-09-16。

修正版於 01:27 完成冷庫物件與還原檔案驗證，寫出 READY 及固定 pin。01:44 對該 release 的 33 個實際選用欄位執行嚴格稽核，`model_safe=true`，重大 0、高風險 0、中度 2；面板為 5,337 交易日 × 2,755 檔 × 33 欄。中度限制為下市公告交叉覆蓋與同日收盤近似。此設定可供該研究契約訓練，不能把同日完整收盤資訊解讀成可在當日收盤價成交，也不代表 159 個來源都有逐期歷史版本。面板快取放在 repo `artifacts/cache`，沒有寫入不可變 materialized release。

01:46 來源監控另收到 `taifex_institutional_total` 新版；01:48 再跑 159/159 全量掃描後重建特徵，01:56 發布 `tw-public-20260916T175608063777890Z-l0-penguin-78ae168002975483`。此最新冷庫版的 2,212 個 packed 物件已全數驗證（16,718,012,472 bytes），並與最新全量批次收據相符；尚未 materialize，也尚未重新執行模型稽核。33 欄研究設定仍固定到先前已 READY、已 pin 且模型稽核通過的 release。網站分別顯示「最新冷庫版」與「研究用固定版」，避免來源增量到來時將有效的舊版研究資料誤判為不存在。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_public_data_layer.py \
  --config configs/markets/tw_public_verified_features_20260917.yaml \
  --panel-cache-root artifacts/cache/tw_public_verified_features_20260917 \
  --output-dir artifacts/data_quality/tw_public_verified_features_20260917 \
  --build-panel --strict --require-live-selected-features
```

先前 00:37 與 00:44 的發布因來源在打包期間改變而拒絕；00:50 有活躍特徵建置程序，服務按 writer gate 延後。現行發布器在打包期間持有 canonical 來源更新鎖，catalog 會阻擋股票、特徵與 Shioaji 日資料建置程序；新增發布前的股票與特徵收據檢查，避免把已知過期的衍生檔再發布成新訓練版。冷庫清冊列名仍不是物件逐一重建、對端同步或可訓練證據。

唯讀[全資料監控頁](https://penguin72487.ddnsgeek.com/data-monitor/)的「臺灣公開資料擴充進度」把來源觀測、live 下載、完整批次稽核與本機冷庫清冊分別呈現。完整掃描由既有 `preopen_all`／08:30 驗收流程負責；頁面不觸發下載。

```bash
source scripts/runtime_env.sh
run_fintech_python downloader/download_tw_public_data.py \
  --mode daily \
  --datasets sitca_domestic_fund_monthly_report_index sitca_domestic_fund_basic twse_etf_fund_basic \
  --output-dir /path/to/staged-tw-public \
  --request-interval 1.0

run_fintech_python scripts/query_toalpha_unique.py --tool data_status
run_fintech_python scripts/query_toalpha_unique.py --tool estimates --symbol 2330
run_fintech_python scripts/query_toalpha_unique.py --tool analyst_consensus --symbol 2330
run_fintech_python scripts/query_toalpha_unique.py --tool broker_ratings --symbol 2330 --days 30
run_fintech_python scripts/query_toalpha_unique.py --tool top_news --limit 10
```

ToAlpha 命令每次只查一個工具與一檔股票（`top_news` 最多十則），不建立本地鏡像。輸出標示 `first_observed_at_utc` 和 `historical_point_in_time: false`。不要把輸出導入訓練特徵、冷發布或排程。開放資料進入正式 `tw-public` 前，須沿用現有完整來源稽核及 `stockagent-data publish` 原子發布流程，不直接寫入 packed/materialized 目錄。

## MOPS 按季 XBRL 批次：已接入、授權門檻與因果邊界

`downloader/download_tw_mops_xbrl.py` 只從 [MOPS 官方案例文件整批下載頁](https://mopsov.twse.com.tw/mops/web/t203sb02)解析實際列出的 ZIP 連結，不猜網址；依 `tw-gaap`／`tifrs` 與季度去重，未結束的季度不列為應下載。此索引在 2026-09-17 清出 71 個已結束季度連結（2009Q4–2026Q2），但「列在頁上」不是已發布、已可下載或逐公司資料完整的證明。上線時仍需檢查 HTTP、ZIP、CRC、路徑安全、SHA-256、XML/iXBRL 解析與事實筆數。

```bash
source scripts/runtime_env.sh
# 人工單次清點；只讀取一個官方索引頁，另寫監控狀態，不抓 ZIP。
run_fintech_python downloader/download_tw_mops_xbrl.py --mode discover --write-state \
  --output-dir /srv/stockagent-live/data_tw_public/mops_xbrl

# 使用者從官方網站手動取得的 ZIP 可離線匯入；來源真實性仍標示未驗證。
run_fintech_python downloader/download_tw_mops_xbrl.py --mode ingest \
  --input /path/to/tifrs-2024Q4.zip \
  --output-dir /srv/stockagent-live/data_tw_public/mops_xbrl

# 已有逐檔 ZIP/CRC 稽核報告時，整批匯入本機檔案並留下進度收據。
run_fintech_python scripts/import_tw_mops_xbrl_local.py \
  --input-dir /path/to/mops_xbrl --audit-report /path/to/mops_audit.json \
  --output-dir /srv/stockagent-live/data_tw_public/mops_xbrl --workers 4
```

原始 ZIP 依內容雜湊保留版本於 `mops_xbrl/raw/{ifrs,tw_gaap}/YYYYQn/`，長表事實存於 `normalized/.../facts.parquet`，每版有 JSON 收據。整批匯入另寫 `local_import_progress.json`；`state.json` 分開記本機匯入的 `local_imported_periods`／`local_fact_rows` 與來源已驗證的 `completed_periods`／`fact_rows`。監控頁單列 `tw-public:mops_xbrl_quarterly`，不與原本 159 個公開來源或訓練特徵混算。`mops_xbrl` 暫列 `tw-public` 冷發布排除子樹：尚無可靠的逐公司首次申報時刻、修訂鏈、schema/單位跨年映射與全公司覆蓋稽核。季度結束日、首次擷取時間、ZIP 目前內容都不能冒充發布時間；收據固定 `historical_point_in_time=false`、`training_eligible=false`。因此目前新增的是可稽核原件與研究用長表，不是已可訓練的歷史 PIT 特徵。

匯入後可執行 `run_fintech_python scripts/build_tw_mops_publication_candidates.py --root /srv/stockagent-live/data_tw_public/mops_xbrl`，建立逐文件 `publication_candidates.parquet` 與稽核摘要。文件附註可辨認的董事會核准日只用作**推定發布日**；無法辨認時，用同季其他文件核准日期的第 75 百分位作習慣估計，再無資料才用有歷史依據的申報期限代理。每列保留來源 ZIP、文件 SHA-256、公司、期別、候選日期與證據等級，`publication_clock_taipei` 留空，`exact_filing_time_lookup_status` 標為待查。這不會覆寫 1.52 億筆事實資料中的 `published_at_utc`，也不會把董事會日期冒充 MOPS 實際申報時刻；若取得與該文件版本相符的官方申報時間，再以更高優先序另行驗證並更新候選表。

2026-09-17 實跑結果：71 個季度 ZIP 產出 152,885 份逐文件候選，覆蓋 151,969,985 筆事實；109,763 份採文件核准日、3,763 份採同季第 75 百分位、39,359 份採申報期限代理，精確申報時刻仍為 0。舊制與現行制、公司類型及假日可能適用不同期限；例如證交所[114 年第 2 季申報期限公告](https://www.twse.com.tw/staticFiles/news/news/tsecnews/8a8216d697fc438f01987eedfea50228.pdf)分別列出 8 月 14 日與順延至 9 月 1 日。期限代理只是低優先序日期猜測，不能解讀成某公司的真正申報日。[證交所投資資訊中心](https://www.twse.com.tw/IIH2/zh/market/information.html)設有「財務報告書公告日期時間」欄位；目前尚無能把其歷史公告逐筆核對到這 152,885 份 XBRL 位元版本的批次收據，因此保留逐文件待查狀態。

[TWSE 網站條款](https://www.twse.com.tw/zh/terms/use.html)對未經同意的自動下載設限；官方提供整批入口不等於同意機器長期鏡像。只有取得與保存證交所允許此自動方式的證據後，才建立權限收據並安裝排程。權限檔範例（`evidence_reference` 必須指向**實際**核准函/合約；自填 JSON 不是授權）：

```json
{
  "provider": "TWSE",
  "scope": "automated_mops_xbrl_bulk_download",
  "authorized": true,
  "evidence_reference": "TWSE approval or contract identifier",
  "expires_on": "2027-12-31"
}
```

取得批准後，如要一次完成歷史缺口，可執行以下命令。`--backfill-all` 只遍歷官方索引上實際列出的已結束季度、每季在這次執行中最多嘗試一次；遇到 307/404 保留缺口並繼續，遇到 403/429、格式改版或驗證失敗則停止並保存狀態。中斷後重跑只處理仍缺的季度，並回查最近兩季修訂；不換代理、繞過封鎖或把失敗標成完成。

```bash
source scripts/runtime_env.sh
run_fintech_python downloader/download_tw_mops_xbrl.py --mode download \
  --authorization-file /path/to/authorization.json --backfill-all \
  --request-interval 1.0 \
  --output-dir /srv/stockagent-live/data_tw_public/mops_xbrl
```

後續增量排程可執行 `sudo bash scripts/install_tw_mops_xbrl_service.sh /path/to/authorization.json`，它才會啟用每四小時一次的 timer；每輪先回查最新兩季 ZIP 是否修訂，再以最久未嘗試的缺季補歷史，每輪最多 4 次。限流預設每秒最多一次請求；這是保守設定，**不是**宣稱已知官方上限。

ToAlpha 的 [MCP 文件](https://toalpha.tw/mcp-docs)與[條款](https://toalpha.tw/terms)雖提供免費查詢，但明示不得大量抓取；因此「MOPS 缺一季就自動巡迴 ToAlpha 全公司補洞」不在此自動管線中。既有 `scripts/query_toalpha_unique.py` 保留有限、按需、唯讀核對。若取得 ToAlpha 另行授權的批量資料契約，需獨立新增授權、來源版本、逐筆發布時間與缺口驗收，不能僅換用現有 API key 就啟動。

## 下一批次的停走條件

規模估計先以 ToAlpha 宣稱的 2016 年至 2026-08 月營收 128 個月份、2001 年至 2026-08 產品別營收 308 個月份，以及 2016Q1 至 2026Q2 財報 42 季作上界。若官方可核准使用「每月／每季一份」批次檔，下載請求數按期數成長；若只能每家公司每季查一次，2,300 家 × 42 季就約 96,600 次請求，單一報表在每秒一次的純節流下限約 27 小時，尚未計入重試、傳輸、解析及稽核。這是方案比較，不是已驗證的免費批次能力或下載承諾。

1. **MOPS 歷史批次**：各取一個月份、一個季度及一份更正案，記錄官方 URL/格式、授權方式、請求數、下載速率、檔案雜湊、公告及更正時刻。只有逐期原版可驗證，才補嚴格歷史特徵；若只剩現值，保留作研究或從現在開始逐版存檔。
2. **基金持股與 PCF**：先拿一檔基金及一檔主動式 ETF，比對公會月前十大、發行人 PCF、實際持股的揭露日期與欄位。來源允許且檔案可穩定重抓後，再擴大到全體。
3. **品質閘門**：每批檢查 `(來源, 市場, 代號, 資料期別, 發布版次)` 唯一性、缺漏/撤銷、單位、原始檔雜湊，以及 `published_at`、`first_observed_at`、`effective_session`。發布批次不能以檔案存在或 HTTP 200 代替驗收。
4. **耗時估計**：實測合法批次的請求粒度與吞吐量後，用 `請求數 / 實測有效速率 + 解析/稽核時間` 給 ETA；不得拿 ToAlpha 的每分鐘額度推算一個被條款禁止的全站鏡像工期。

本庫精確的 09:00 可用規則與目前快照邊界見 [台股公開資料發布時刻契約](tw_public_release_timing.md)。
