# 期交所公開資料：清冊、歷史回補與資料界線

## 範圍與目前結果

本次範圍是交易研究資料：官方 OpenAPI 全目錄、既有期貨／選擇權歷史下載器，以及歷史公告、保證金、部位限制、契約調整與商品規格。不是宣稱整站所有文章、外站或需申請的歷史檔案均已取得。

逐端點清冊：

- `artifacts/data_quality/taifex_public_inventory_2026-09-27/taifex_public_inventory.md`
- 同目錄 CSV / JSON：每一端點的名稱、資料粒度、筆數、最早／最新資料日、抓取日、owner、歷史限制與缺口。
- `data_taifex_public_history/openapi_latest.json`：本輪 135 個官方 API；127 個非委派端點已於 2026-09-27 取得、0 個下載失敗、109,987 列快照。其餘 8 個由既有下載器負責，但委派本身不代表覆蓋完整。
- `data_taifex_public_history/rules/manifest.json`：動態公告回補進度，不能以本文件的靜態數字代替。

首次索引已保存 1997～2026 共 30 個年度查詢，回傳 13,301 則公告，觀察到的發布日為 2001-10-18～2026-09-23。這不證明 2001 年前沒有公告。公告種類包含保證金 1,586、契約調整 3,913、部位限制 179、商品／價格規則 182、交易日／時段 197、結算 2,097、其他公告 5,147；分類是標題規則，不是官方完整標籤。

初始約 1.12 萬個官方 URL 去重後排程；解析 HTML 又會發現附件，因此分母會增加。另有 733 則外站連結僅保留索引，不跨站自動抓取。文件仍在背景下載，沒有宣稱已抓齊。

## 有哪些資料

| 類別 | 已保存或既有管線 | 尚未等同完整的部分 |
|---|---|---|
| 全商品期貨日行情、日夜盤、成交量、未平倉 | 1998-07-21 起的全商品日資料 | 各商品上市／下市及欄位完整性仍須逐商品核對 |
| 選擇權日行情 | 全商品原始年度 ZIP；TXO 近月與最近週到期標準化表 | 非 TXO、其他到期月份尚未全數標準化成訓練表 |
| 逐筆成交與價差成交 | 官方近期全商品 ZIP；TX／TXO 衍生表 | 早期全市場逐筆、委託簿不可由日行情重建；免費官方下載視窗有限 |
| Put/Call 比、三大法人期貨／選擇權／買賣權 | Put/Call 2001-12 起；三大法人保存近三年 | 三大法人舊歷史不可因當前 HTTP 200 就認定可取得 |
| 大額交易人全市場部位 | 新增期貨、選擇權三曆月 CSV 回補 | 不再只抓 TX；空回應不代表完整，也不證明某年沒有資料 |
| 最後結算價、到期部位、參考開盤價、Delta、VIX、外幣匯率 | 歷史下載器或 OpenAPI／近期頁面 | 各來源起日不同；資料日與今日抓取日必須分開 |
| 原始／維持／結算保證金、抵繳標的 | 現行 OpenAPI 快照＋新增公告原文與附件回補 | 舊公告可能只剩生效文字、金額附件已失效，不能以現行數值倒填 |
| 交易人部位限額、全市場限額、契約調整、商品規格 | 現行快照＋新增 HTML／CSV／PDF 保存 | 歷史各版商品與生效時鐘尚待核對，不自動注入可執行規則 |
| 費率、年度商品交易量、期貨商日週月年交易量、市占與市場參與者 | 官方目錄快照 | 目錄有總表與分表，屬來源視圖，不能直接相加或當成不同經濟特徵 |
| 期貨商財務、損益、EPS、淨值、名冊、結算銀行、會員與監理資訊 | 官方目錄快照 | 保留版本，不宣稱回溯到各資料開辦日起的所有版本 |

完整名稱請看逐端點清冊；這張表只是分類摘要。

## 本次修正

1. **下載時鐘與可用時鐘分開。** 最新已收盤資料即使尚無下一個已驗證交易日仍會下載。`available_date` 保留 null，待交易日曆有證據再更新衍生表；不回寫原始回應或猜工作日。
2. **全市場批次。** 官方大額交易人下載表單允許至多三個月，一次所有商品。新 `large_trader_futures_all` / `large_trader_options_all` 不採逐商品乘逐日請求。首次從 1998 年探測後，再由官方說明確認歷史起日 **2004-07-01**，後續以此為來源下界，不再重試此前不在來源範圍的季度。
3. **完整性分層。** 檢查表頭、尾註、日期窗、自然鍵、官方交易日日曆覆蓋，缺口寫進 range status。全市場 CSV 有資料不等於每個歷史商品均獨立驗證。
4. **公告不漏類型。** 歷史 `newsType=契約調整` 與所有類型＋關鍵字結果不同，改為全年全部類型索引。相同 URL 只排一份工作，內容用 SHA-256 去重並保留版本；跨 URL 的來源收據分開。
5. **發布、生效、擷取不同。** 保留發布日精度、文字中的生效日期／一般時段收盤後等邊界，不將其硬改成午夜。恢復日、引用函日期分開標記。
6. **單位明示才轉換。** 保留原字串、Decimal 數值、幣別／比例證據。CSV 沒寫單位就保留 unknown，不從 0.405 或 40.5 猜比例。期貨部位的口數／契約等值也不是股票股數。
7. **獨立來源失敗隔離。** 盤後 positioning 失敗不會阻止季度回補、OpenAPI／VIX；整個 runner 仍回傳失敗，不能因此發布假的完整冷庫版本。父資料集也有 manifest `status=complete` 的冷庫發布條件，仍有缺口時不推進發行版。
8. **資源與網頁。** 新服務低 CPU／I/O 優先權、1 GiB 記憶體上限；網頁只讀小型收據，不為顯示進度呼叫 API 或掃描全文。
9. **解析升版不重抓。** Parser v2 新增有界 DOCX／ODT／ODS 抽取；先驗原始壓縮檔與解壓內容的 SHA-256，再離線產生版本化 JSON／Parquet。原始擷取時間、capture versions 與舊解析檔不覆寫；新增獨立 `parsed_at` 與 append-only `parse_versions`。一般批次共用文件數／時間預算，先升版既有原件，再接續未取得的文件。
10. **真實失效與檔名分開。** 修復以 URL 包含 `/404` 就判定失效的錯誤，合法 `4047_001.pdf` 不再被擋；實際重新導向 `404.htm` 或明示 404 的舊連結仍保持失敗。

單一 phase 的執行結果與全域完整度也分開：本階段成功不會被其他階段尚未修復的舊 `partial` 誤報成執行失敗；全域 manifest 仍保留那些缺口，冷庫發布門檻不放寬。

實抓驗證：2026Q3 至 9/24 的兩次批次請求取得期貨 84,106 列／347 個商品、選擇權 19,824 列／38 個商品，均涵蓋 61 個已驗證交易日，該窗口自然鍵重複為 0。最新 9/24 的 `available_date` 仍等下一個已驗證交易日，而不是漏下載。

全史實抓另發現 2013 年起部分期貨 CSV 有兩列完全相同的 `-` 占位列。官方尾註說明這代表沒有週到期契約，不是部位為零，也不是交易人類別 0／1。修復保留原始列識別與 `row_quality=source_no_weekly_contract_placeholder`，數值維持 NULL；一般數值資料仍驗自然鍵唯一。解析失敗的已保存 CSV 可驗雜湊後離線重解析，無需重下載；官方「查無資料」與 HTTP 失敗不能用這個路徑假裝已修好。

另外，官方沿革記載大額交易人資料 **2005-01-03 才開始公布、回溯至 2004-07-01**。2004 年的觀測值不能被當成 2004 年當時已公開；其可用日需受首次發布日下界約束。下界的日內時刻沿用盤後發布慣例推定，並非已查到精確歷史時分秒。來源原件保留，修正的是衍生可用日投影。

### 2026-09-27 17:06（台北）實抓驗收

| 全市場大額交易人資料 | 列數 | 日期 | 已觀察／應有交易日 | 缺少交易日 | 官方無週到期契約占位列 |
|---|---:|---|---:|---:|---:|
| 期貨 | 3,952,182 | 2004-07-01～2026-09-24 | 5,471／5,471 | 0 | 1,084 |
| 選擇權 | 2,126,364 | 2004-07-01～2026-09-24 | 5,471／5,471 | 0 | 2,332 |

- 修復的 39 個期貨季度與 42 個選擇權季度全數由已保存原件重解析，新增資料請求 **0** 次；恢復 3,358,136 列，其中 3,354,720 列為正常觀測列、3,416 列為有明確來源意義的占位列。
- 歷史首次發布下界修正期貨 3,072 列、選擇權 27,024 列。最新交易日尚無下一個已驗證交易日時，仍保留已下載值與 null 可用日，不把它列成下載失敗。
- 全域下載 manifest 在 17:06 已轉為 `complete`；17:07 重跑顯示 `phase_execution_status=complete`。這是上述來源／日期範圍的取得證據，**不是**所有歷史商品逐欄無缺、公告數值全部可訓練、或冷庫對端同步證明。
- 另逐批讀取兩個最終 Parquet 的觀測日／可用日檢查：30,096 列開辦前回溯資料最早可用日均不早於 2005-01-04，違反首次發布下界的列數為 0；Parquet footer 列數與 manifest 一致。
- 公告封存同時繼續背景工作；17:07 取樣為 1,708 份已保存、9,552 份待完成（含 1 個失效連結），801 份有解析缺口。後續發現附件會增加總工作數，請以網頁／manifest 的最新值為準。
- 冷庫來源發行後，17:10 的 Syncthing head-history 掃描通知曾逾時；既有 `stockagent-d-cold-scan-retry.service` 在 17:13:15 回報 `scan_request_acknowledged`、待補通知降為 0。這是通知重試成功，不是對端收斂或完整物件驗證證明；沒有重跑下載來處理傳輸通知問題。

### 2026-09-27 17:48（台北）離線升版與下載恢復

- 3,427 份既有文件全部升至 parser v2；逐件重新驗證原始壓縮／內容 SHA-256，原 capture versions 全表 digest 與擷取時間未改，3,427 份舊解析檔原樣保留。另驗證 3,427 份新 JSON 及其精確引用的 1,501 個 Parquet shard，錯誤 0。
- 修復 15 份先前未支援的 Office 文件（DOCX 10、ODT 3、ODS 2），新增 2,978 個實體表格列及 398 個日期證據，資料 HTTP 請求為 0。這些通用欄位尚未變成已核定保證金規則，沒有產生猜測的 `margin_changes`。
- 升版後當下解析分布：可抽取 1,511、待 OCR 1,770、未支援舊二進位／封裝格式 144、受頁數限制而部分抽取 2。`parsed` 不代表歷史或 PIT 完整。
- 同一既有下載器成功重新取得 `4047_001.pdf`，失敗由 2 降至 1；該 PDF 仍需 OCR。此時原件已保存 3,428／11,260，尚待 7,832（含 1 個實際失效舊連結）。兩個數字會隨發現附件及後續下載變動。
- 只暫停新規則 timer 以驗證升版，未中止舊批次、交易／行情或其他下載器；17:48 已恢復 timer 與 collector，繼續背景下載。
- 收據：`artifacts/data_quality/taifex_public_inventory_2026-09-27/rule_reparse_acceptance.json`、`rule_404_filename_repair.json`。最近完成的 20 分鐘下載批次累積 CPU 約 42.9 秒、記憶體峰值約 319.7 MiB，沒有 GPU 工作；這是單批觀察，不是整機資源承諾。

## 增量與背景服務

- `stockagent-taifex-rules.timer`：啟動後 5 分鐘、每批結束後 5 分鐘續跑；單批最多 1,200 文件／25 分鐘。休市日也會執行。
- 當年索引、現行規則／規格頁每 6 小時檢查；最近 14 天公告可重查並跟進附件。舊年度索引每 30 天再檢查。這是排程政策，不是官方發布時間承諾。
- 新增公告下載器與籌碼歷史下載器共用 `SharedRateLimiter('taifex_public_history')`，目前公告的保守間隔為 1 秒；尚無查到這些網頁的官方 req/s 額度，不能稱為「官方極速」，也不宣稱其他既有 TAIFEX collector 已統一成這個 bucket。429 尊重 Retry-After；403/WAF 共享冷卻並停止該批。
- Provider 冷卻截止 UTC 寫入 `state/provider_cooldown.json`，下個程序在期限到達前不建立 HTTP session、不進入長時間 limiter wait。Retry-After 同時支援秒數與 HTTP-date，不截短官方較長期限；manifest 保留 `provider_deferred` 與 `provider_cooldown_until_utc`。
- 單文件一般失敗有界退避 60 秒至 1 小時；新工作先於重試。歷史 CSV 的明示查無資料與失敗範圍另外留存，再次驗證不會當成完成證明。
- 既有 `stockagent-taifex-public-history.timer` 盤後更新日資料與季度缺口，最新目標使用台北今天，仍受官方已驗證交易日日曆限制。
- `/data-monitor/providers/TAIFEX/` 沿用現有 provider 頁，分開顯示主歷史及公告／規則工作列；索引年數、公告數、文件數、附件數、解析缺口分別顯示，不混加。主 manifest 的 7／7 不是全站 135 個 API 的完整率，也不包含獨立 VIX 來源。
- 父群組是不同發布時鐘的聚合，沒有共同「下一資料日」；已移除由日曆猜出的日期，改顯示個別來源發布時程。真實 systemd 下次執行時間保留。
- 全商品期貨日期改讀品質收據的實際觀測範圍（本輪 1998-07-21～2026-09-24），不再把查詢截止 9/25 當成實際最新一筆。品質日期缺少或前後矛盾時顯示未知；期貨／選擇權／逐筆父群組同樣不推測共同下一資料日。

## 操作

```bash
source scripts/runtime_env.sh

# 更新唯讀、低成本逐端點清冊，不呼叫提供者 API。
run_fintech_python -m scripts.audit_taifex_public_inventory \
  --output-dir artifacts/data_quality/taifex_public_inventory_2026-09-27

# 繼續本機公告封存；內建 flock 防雙 writer。
run_fintech_python -m scripts.download_taifex_rule_history \
  --max-documents 1200 --max-seconds 1500

# 升版已保存原件；不建立 HTTP session、不消耗 API 請求。
# 與背景服務共用鎖，不應同時啟動第二個 writer。
run_fintech_python -m scripts.download_taifex_rule_history \
  --reparse-only --max-documents 5000 --max-seconds 300

# 使用標準 runner 的鎖，指定全市場大額交易人歷史回補。
bash scripts/run_taifex_public_history.sh --phase large-trader-range

# 僅修復已保存但解析失敗的季度 CSV，不發出資料請求。
bash scripts/run_taifex_public_history.sh --phase large-trader-range \
  --reparse-failed-ranges-only

# 只安裝新公告服務，不重裝交易或其他下載服務。
bash scripts/install_registered_data_refresh_services.sh taifex-rules-only
systemctl start --no-block stockagent-taifex-rules.service
journalctl -u stockagent-taifex-rules.service -n 20 --no-pager
```

## 尚未解決、不會假裝完成

本次驗收執行 TAIFEX downloader／parser／清冊、監控投影、shell orchestration 與冷庫發布 gate 的 381 項相關測試，全數通過；亦驗證公開 provider API 的主歷史／公告分列與群組未誤報完成。這些工程測試不替代逐商品、逐欄歷史完整性審核。

- 部分舊連結已不存在；掃描 PDF、舊二進位 DOC／XLS／PPT 等能保存原件，但尚未變成可用數值。DOCX／ODT／ODS 已支援安全的文字與實體表格抽取，但不等於完成財務欄位對齊／語義驗證。
- PDF 文字抽取不是表格版面還原。通用 parser 不會把 PDF 排版猜成金額欄。
- 同名 `margintable.csv` 可能跨年覆用；今日下載的內容不能自動綁定所有引用它的舊公告。
- 原件封存與可訓練、可執行的歷史保證金規則是不同產品；`point_in_time_verified=false`、`historical_values_complete=false` 保持可見。
- 讀取解析結果須由 `documents.parsed_path` 取得當前版本，再讀該 JSON 引用的精確 shard 路徑；不可 glob 所有歷史版本後合併，否則同一文件會被重複計算。原件損毀會標記 `integrity_failed`，不偷偷以網路新版本覆蓋舊證據。
- `--offline-audit` 是無網路的本機清冊匯出，不是全 archive 重新校驗。Manifest 明示完整性證據只含寫入時 hash 與續跑檔案存在檢查。
- 新 `taifex-rule-history` 登記為 `publish:false`；父資料集排除 `rules`，未完成／未核對的來源不會混入冷庫或訓練發行版。公開頁只顯示摘要，不提供原始文件或帳號。

## 官方依據

- [官方 API 目錄](https://openapi.taifex.com.tw/)
- [歷史公告](https://www.taifex.com.tw/cht/11/hisNews)、[保證金查詢 FAQ](https://www.taifex.com.tw/cht/9/tradersQAClearing)
- [期貨大額交易人批次下載](https://www.taifex.com.tw/cht/3/largeTraderFutView)、[選擇權批次下載](https://www.taifex.com.tw/cht/3/largeTraderOptView)
- [大額交易人資料開始日](https://www.taifex.com.tw/cht/3/largeTraderOptQryDetail)、[首次公布／回溯範圍的官方沿革](https://www.taifex.com.tw/cht/1/historyOfSurveillance)
- [非個股部位限制](https://www.taifex.com.tw/cht/4/traderPLNonEquity)、[個股部位限制](https://www.taifex.com.tw/cht/4/traderPLEquity)
- [契約調整](https://www.taifex.com.tw/cht/4/contractAdj)、[股票期貨規格](https://www.taifex.com.tw/cht/2/sTF)
- [網站使用條款](https://www.taifex.com.tw/cht/edu/userTerms)：政府開放資料與其他網站內容不應一律視為同一授權。
