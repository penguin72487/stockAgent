# 公開網站 review、修正與可重複測速 — 2026-09-12

範圍：penguin 上八個公開頁面、共用前端、唯讀 gateway、snapshot/cache、SSE、測速工具與實際 HTTPS 驗收。
本輪已部署網站修正；不是交易模型／成交機制重審，也不是全機冷啟動通過證明。

## 第一性原理：先分清真值、等待與呈現

資料產生／訊號發布 → 唯讀 snapshot → 公開欄位投影 → HTTP → JSON 解析 → DOM／繪製機會。

- 真值仍由既有來源、receipt 與模擬帳本擁有。網站不補成交、不改損益、不改模型、不觸發下載。
- 單一請求的時間分為連線／TLS、等待標頭（包含伺服器）、本文／解壓縮、解析、呈現。
  Server-Timing 已包含在等標頭時間，不能重複相加。
- 多個獨立請求可平行，首屏延遲看必要依賴中的最慢路徑，不是把所有 API 時間相加。
- 頁面載入成功、資料最新、資料完整、策略可執行是四件事。缺口仍顯示，不把 HTTP 200 當成策略正常。
- 移除重複工作優先於更換框架：保留既有共用元件、single-flight、SSE、gzip、分頁與無損分鐘欄位。

## Review findings 與處理

| 優先度 | 問題／證據 | 處理 |
|---|---|---|
| P1 | 工作樹已有四個未完成 merge 的交易／訓練檔；跨服務測試 import 到 simulator 時遇 conflict marker | 保留原變更，不混入本輪修正；不重啟交易服務。全機冷啟動不可宣稱安全 |
| P1 | fetch 在收到標頭時就解除 timeout／上游取消；本文可能繼續卡住，過期請求繼續耗資源 | 共用 Response lifetime 保留至 JSON/text 消費完成；丟棄回應主動取消；補本文逾時與競態測試 |
| P1 | 本輪開始時 staged file-signature 與 JSON 快取每次命中仍讀完整檔案；隔日沖約 9 MB Parquet 每次 hash 約 11–21 ms | 熱路徑只核對 dev/inode/size/mtime_ns/ctime_ns；變動才 hash，快取有界；同大小／還原 mtime／原子替換仍能失效 |
| P1 | 讀 JSON 與後續 pathname stat 可能跨越原子替換，將舊內容記在新版本下 | descriptor 前後與 pathname 複核一致才入快取；加入替換競態測試 |
| P1 | 本機連 HTTPS 的 IPv6 失敗，新 Python connection 約多等 3 秒後走 IPv4 | 已分離量測，未更動 DDNS／路由器／Windows 防火牆；仍需另行修復 IPv6 可達性或確認 AAAA 政策 |
| P2 | 全歷史分鐘投影反覆轉換同一時間、重複排序及重新解析已知時間字串 | 共用每次投影的時間快取、沿用排序、直接使用既有 timestamp；不改數字、點數或品質旗標 |
| P2 | 同源 guard 原本只檢查字串 URL，URL/Request 物件與 redirect 邊界不完整 | 納入 URL/Request 並設定 redirect:error；加入回歸測試 |
| P2 | 靜態快取只有 size/mtime，還原時間戳可能繼續傳舊 JS | 與 receipt 使用一致 metadata invalidation，檔案讀取前後複核；新 JS/CSS 同步升版 |
| P2 | 1366px 當沖文件沒有溢出，但事件表內部仍超寬 215px | 重用 compact-table，六欄換行完整呈現；修正後 829px 區域／表格皆 829px，101 列不橫向溢出 |
| P2 | 測速混合新連線與重用連線、少量樣本 percentile 不精確，沒有所有頁面完整證據 | 保留原始樣本與 git 狀態，新增 connection_first/warm_reuse、nearest-rank p95/p99、錯誤、JSON root 與吞吐；八頁 CDP 驗收含本文錯誤、deadline、內部表格及截圖 |
| P2 | gateway 已快取序列化／gzip history，但 builder 又保留最多四份 decoded 大型物件 | 公開 history 關閉第二層物件快取；直接使用 builder 的服務維持預設相容性；新增 cache opt-out 與 gateway 契約測試 |
| P2 | 瀏覽器 history LRU 同時強引用欄式本文與解碼列，長日期範圍約留下兩份資料 | 解碼後立即丟棄 `minute_series`，LRU 僅保存可繪製列；完整點數、欄位與輸出語意不變 |
| P2 | 全資料監控首屏立即下載約 1.52 MB、渲染 421 筆明細，即使首屏只看摘要 | summary 保留 29 個實體群組，逐來源明細接近 viewport 才抓；重疊刷新會排隊一次 detail 而不漏掉使用者需求 |
| P2 | 各頁只顯示當次請求時間，沒有跨頁、跨日、按功能可比較的使用者體感樣本 | 共用 core 記錄 page load、互動與 API→兩次 rAF；流量頁新增本機測速表、篩選、主要耗時、JSON 匯出與清除；全域 256／每序列 32 筆有界且不回傳伺服器 |
| P2 | TAIFEX 切換未建置 range 時，歷史掃描及公網清洗可讓首次互動約 3.82 秒 | 七個 range 新增原子 disk cache、背景刷新、記憶體 LRU=3；公開 allowlist 改成先投影再清洗，保留數值精度及敏感欄位 fail-closed |
| P2 | 全資料明細每次篩選都重算 421 筆排序鍵，首批渲染 100 列；details 又會誤清 summary 群組 | 排序只在 details revision 改變時做一次，首批改 25 列、其餘可載入；修正群組所有權，不再消失 |
| P3 | 當沖有兩個未使用的篩選函式；每秒時鐘在隱藏分頁仍執行 | 刪除死函式；時鐘改用 visibility-aware 共用排程，頁面再顯示時立即校時 |

沒有新增全域 rate limit 或 32 併發伺服器上限；benchmark 的 concurrency 只是此次測試客戶端數量。

## 實測：哪些確實變快，哪些還沒有

### 受控原始計算與正確性

在同一主機、同一份實際歷史資料，清除相同的程序內快取，對照 staged 舊版 history 函式與新版：

| 項目 | 舊版 | 新版 | 判讀 |
|---|---:|---:|---|
| 全部分鐘歷史原始建置 | 12,980.66 ms | 8,137.88 ms | 單次配對約減少 37.3%，不是長期 SLA |
| 輸出點數 | 232,179 | 232,179 | 完整 JSON 雜湊相同，不是只核對筆數 |
| 9,197,786-byte 隔日沖訊號 signature | 每次約 11–21 ms | 熱命中中位數 0.005 ms | staged 舊實作的 microbenchmark；不是舊線上版本 API 延遲 |

完整 JSON SHA-256（sort_keys、compact separators、UTF-8）：
`b5a5d4566eaaabced2c2d07352f662ee2ea7a97a99c13019130a00b197ae5af2`。

後續獨立 CLI 重測原始建置 8,167.58 ms，程序熱命中 0.59／0.39 ms，輸出仍相同。
此 receipt 的 `source_stable=false` 是因量測期間 live receipt 仍在更新；不能說所有來源已凍結。
OS page cache／既有索引未清除，因此也不是磁碟冷開機測試。

### HTTP 與互動

前後全路由掃描各 31 條，localhost／HTTPS 均零 HTTP 錯誤；每條 first_observed 1 次、batch 5 次。
下表 batch 為相同舊版統計口徑（含該 worker 第一次 connection），原始 receipt 另分 warm_reuse。

| 路徑／邊界 | 修改前 | 修改後 | 注意 |
|---|---:|---:|---|
| localhost 當沖 status，batch median | 3.68 ms | 1.81 ms | 小樣本線上觀測 |
| localhost 當沖 signals，batch median | 2.60 ms | 1.97 ms | 同上 |
| localhost 全分鐘 history，first_observed | 8,910.33 ms | 7,802.21 ms | 首次觀測不等於已證實 cold miss |
| localhost 全分鐘 history，batch median | 65.82 ms | 64.53 ms | 大本文傳輸仍有成本 |
| localhost TAIFEX 1d history，batch median | 42.70 ms | 23.36 ms | 未重寫 TAIFEX history；不能將差額全歸功程式修改 |
| HTTPS 全分鐘 history，batch median | 79.15 ms | 134.86 ms | 此輪有瀏覽器／其他測試重疊，沒有宣稱全面改善 |

進一步重測（新 connection 與重用連線分開）：

- localhost 當沖 status：29 個重用樣本，median 1.52 ms、p95 2.04 ms；server app median 0.20 ms。
- localhost 隔日沖 revision：29 個重用樣本，median 1.00 ms、p95 1.25 ms，無持續 14 ms 的回歸。
- HTTPS 全分鐘 history：29 個重用樣本，median 80.47 ms、p95 161.02 ms；本文／解壓縮 median 76.60 ms。
- HTTPS 當沖 status：29 個重用樣本，median 3.83 ms、p95 14.89 ms。
- 隔離 atomic receipt → loopback SSE：30 次，median 1.79 ms、p95 2.65 ms。這不是交易所／開盤到訊號 SLA。
- 4 個客戶端、各 25 次的有限併發測試：當沖 status 約 288 requests/s、signals 約 366 requests/s、隔日沖 status 約 130 requests/s，303 次含首次探測零錯誤。不是伺服器容量極限。
- 最終 1366×768 Chromium HTTPS 驗收：當沖更新到兩次 rAF 繪製機會 44.4 ms（其中 status 請求含解析 8.6 ms）；隔日沖 32.5 ms（status 8.8 ms）。每頁只發生一次 navigation。

第二輪去重與延遲收斂的獨立量測：

- 4 個 loopback 客戶端、每路由 20 個樣本的兩輪皆零錯誤：當沖 status median 11.18／15.13 ms、
  signals 9.35／8.69 ms；全分鐘 history 首次 7,435.58／9,870.38 ms。後者另測 summary median
  9.04 ms、deferred details 58.10 ms。全歷史的併發 response-body median 1,087.49／652.00 ms 含四個
  客戶端同時傳輸／解壓，不可與先前單客戶端熱樣本直接比較或只挑最快值。
- 兩個獨立程序以同一份 232,179 點 history 建置後刪除區域變數並 `gc.collect()`：保留 builder object cache
  時有 1 entry／RSS 698,512 KiB；公開 gateway 使用的 opt-out 為 0 entry／RSS 508,692 KiB，差 189,820 KiB
  （27.2%）。兩者先後執行，8,069.93／9,416.62 ms 建置時間受 OS page cache 影響，不能拿來比較快慢。
  線上 gateway 經不同重型壓測後觀測過 646–872 MB，證明 allocator 與工作集合會改變 RSS；不把重啟後低點
  宣稱成固定記憶體 SLA。可證明的契約是公開路徑不再將 decoded history 留在 builder cache。
- 全資料監控 API summary 約 115.2 KB，完整 status 約 1.52 MB；首屏 Chromium 全部解碼資源
  約 177,311 bytes，前一輪立即載明細時為 1,580,562 bytes，觀測少 88.8%。明細未刪除：滑入後仍渲染
  100／421 列，1366px 從滑入到兩次 rAF 為 179.4 ms（details 含解析 53.5 ms），390px 為 240.9 ms
  （details 49.7 ms）。summary 與 details 分離後，滑入明細的全頁累計解碼資源為 1,575,121 bytes，
  沒有以延後載入交換更大的最終傳輸。
- 最終公網 Chromium：1366px 當沖手動更新到繪製機會 58.6 ms（status 12.3 ms）；390px 為 74.6 ms
  （status 8.7 ms）。八頁在兩個 viewport 均為零 document overflow、console error、API failure 與 timing error。

兩次 rAF 代表瀏覽器有繪製機會，不是螢幕實際顯示／使用者感知時間。不同輪的瀏覽器／網路／背景負載有變化，不能挑最快數字當保證。

### 2026-09-13 跨頁功能測速與 TAIFEX 冷路徑

- 共用 core 現在對八頁記錄三種口徑：`page_load`、`interaction`、`api`。瀏覽器本機最多
  256 筆、每個功能序列最多 32 筆；只保存 pathname、穩定 control id/data 屬性、階段耗時、
  狀態碼及 viewport，不保存輸入文字、query、IP、User-Agent、Cookie、帳號或 API payload。
- 6.44 MB TAIFEX 1 日樣本的 `json.loads` 約 0.10–0.12 秒；原清洗順序單次約
  0.70–1.10 秒。改成固定欄位先投影後，清洗中位數約 208.51 ms，清洗加 JSON encode
  中位數約 270.51 ms。這是同機 microbenchmark，不含上游、TLS、傳輸或瀏覽器繪圖。
- 重啟**只有公開 gateway**後，以全新瀏覽器 profile 第一次切換 TAIFEX 1h：按鈕到觀測完成
  250.2 ms，API 232.4 ms、Server-Timing 201.548 ms；修正前獨立 smoke 為 3,823.6 ms、
  server 3,764.4 ms，這一對樣本約減少 93.5%。這是一次冷 range 對照，不是長期 SLA。
- 最終 1366×768 當沖手動更新到兩次 rAF 為 129.6 ms（status 93.0 ms）；隔日沖
  80.8 ms。390×844 為當沖 67.2 ms、隔日沖 77.0 ms。較早同版本桌面樣本曾為
  64.0 ms，故不把單次最快值作承諾。八頁兩種 viewport 都沒有文件／筆電表格
  溢出、console exception、API failure 或 timing error；每次當沖操作仍只有一個 navigation。
- 全資料明細改成筆電 25 列、手機 10 列後，最終 1366px 從 reveal 到兩次 rAF 為
  108.7 ms（details 64.9 ms），390px 為 216.7 ms（details 84.1 ms）；獨立手機重測為
  181.1 ms（details 71.2 ms）。421 筆仍可逐頁取得，summary 群組保持可見。
- 四個 loopback client、每路由 10 次的最終 receipt 為 32 條路由零錯誤。當沖 status
  median 21.02 ms、signals 16.64 ms；完整 232,179 點 history 在四客戶同時傳輸／解壓時
  median 1,123.83 ms。這不是單一筆電互動數字，也沒有刪除歷史點。
- TAIFEX disk cache 已用 localhost 已投影快照建立七個 range，合計約 38 MiB。獨立 restore
  量測 1h 約 36.09 ms、1d 126.33 ms、1w 77.45 ms；新服務程式會在下次 TAIFEX 正常重啟／
  冷啟動時採用，本輪為避免干擾行情服務沒有重啟它。公開 gateway 已載入先投影清洗修正。
- Caddy active health probe 已由 30 秒／2 秒調整為 5 秒／1 秒，passive fail duration 由
  10 秒調整為 2 秒；Windows 現行設定已驗證並 graceful reload。公開 gateway 本次受控重啟
  的 localhost listener 約 560.8 ms 可用；沒有執行全機斷電或最壞相位 Caddy 恢復測試，
  因此不宣稱固定恢復 SLA。

### 公網連線限制

本機 `curl -6` 無法連到此網域，約 3.06 秒失敗；`curl -4` 同網址成功約 29.16 ms
（DNS 11.9、TCP 累計 13.2、TLS 累計 26.4、first-byte 28.6 ms）。
Python requests 的新 connection 約 3.1 秒；預設 curl 的連線 fallback 約 232 ms。
這證明本機所走 IPv6 路徑有問題，不足以單憑此判斷是 AAAA 過期、Windows 防火牆或路由器哪一層。
本輪沒有擅自關閉系統 IPv6 或修改 DNS。HTTPS 測試來自 penguin，不是獨立外部 ISP 探針。

## 驗收與部署證據

- 本輪最終 dashboard-focused suite 共 315 項 Python、29 項 Node 測試通過；Ruff、相關前端
  JS syntax 與 whitespace 檢查通過。這與下方 2026-09-12 較早輪次的計數是不同測試快照。
- 網站第二輪合併 focused suite 296 項 Python、27 項 Node 測試通過；Ruff、所有前端 JS syntax、相關 diff whitespace 檢查通過。
- 八頁全部完成 Chromium 實際載入，最終無文件／筆電表格溢出、console exception、API timing error 或失敗請求。
- 檢查表單 label、重複 ID、24px 以下操作目標、表格鍵盤區域；本次樣本無上述問題。
- 實測 `/.env`、`/api/order`、README 路徑不可讀（404），未知 query 400；POST 405 且關閉連線。
  gzip 正常，對應 ETag conditional GET 回傳 304 且無本文。DTO/sensitive-field/security regression tests 保持通過。
- 第一輪在 2026-09-12 18:29:08 +08:00 只重啟公開 gateway：PID 1538386 → 1770580。
  第二輪兩次只重啟 gateway：PID 1770580 → 1797734 → 1817514，NRestarts 0（最後一次加入
  summary/details 傳輸分層）。當沖 PID 137／0、隔日沖
  1513312／0、Discord 1635／0 均保持；TAIFEX BidAsk capture PID 149030／1 自 09-11 05:00
  持續 active/running，累計 restart 1 是本輪前既有狀態。
- gateway 本次啟動後 journal 檢查無 traceback/fatal/watchdog/error。
- 正常公開回應仍保留當沖 `health=degraded`、隔日沖／TAIFEX `waiting`、全資料監控
  `health=critical`；原有未完整成交、補登與資料缺口未被隱藏。

測試瀏覽器最初未套用主機 Windows CJK 字型，因此只適合 DOM 檢查；最終透過**測試專用** FONTCONFIG_FILE
納入既有 Windows 字型，重新測量並目視核對中文截圖。沒有把字型打包、上傳公網或修改網站樣式來偽裝測試結果。

## 可直接重跑

### HTTP、SSE 與版本比較

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_dashboard_latency.py --repeats 30
run_fintech_python scripts/benchmark_dashboard_latency.py \
  --base-url https://penguin72487.ddnsgeek.com --repeats 30 --skip-notification
```

預設自動保存在 `artifacts/benchmarks/dashboards/http-<UTC timestamp>.json`，不覆蓋上一份。
最新工具另記錄實際 web code 的 SHA-256 fingerprints，不只記 Git HEAD／dirty file 名稱；
這讓尚未 commit 的修改也能與日後重測區別。初期 before/after receipt 尚無此新增欄位。
32 條路由各測一次 first_observed，再每 worker 測 repeats 次。預設不清快取、不重啟服務、不改帳本。
`--path` 可重複指定，只測重點 API；`--concurrency 4` 是有限並發測速；大流量測試避開開盤保護窗口。

```bash
run_fintech_python scripts/benchmark_dashboard_latency.py --repeats 5 \
  --skip-notification \
  --compare artifacts/benchmarks/dashboards/2026-09-12-after-loopback.json
run_fintech_python scripts/benchmark_dashboard_latency.py --repeats 3 \
  --profile-history artifacts/live/tw_day_trade_simulation \
  --path /tw-day-trade/api/status --skip-notification
```

比較要求同 schema、origin、repeats、concurrency，並逐路徑比較；來源期間、負載、cache state 仍需人工確認。
原始 history profiler 使用新的 CLI process，保存點數與輸出 hash；不重啟服務、不 flush OS cache。
signature microbenchmark 對超過 16 MiB 檔案明確標示 skipped，避免為測小型 invalidation 去掃數 GB 訊號帳本。

### 瀏覽器

使用支援 Node WebSocket 的環境與已安裝 Chromium，先開一個**獨立測試 profile**，CDP 僅綁 localhost：

```bash
review_browser_profile=$(mktemp -d /tmp/stockagent-web-review.XXXXXX)
chromium --headless --remote-debugging-address=127.0.0.1 \
  --remote-debugging-port=9229 --user-data-dir="$review_browser_profile" about:blank
```

另一個終端執行（本機亦可使用 Playwright 安裝的 Chromium executable；root 執行時僅針對隔離測試加 `--no-sandbox`）：

```bash
node scripts/audit_public_dashboards_browser.mjs 9229 \
  https://penguin72487.ddnsgeek.com \
  artifacts/benchmarks/dashboards/browser-retest 1366 768
```

最後可選參數可限定逗號分隔頁面，例如只重測重型資料清冊：

```bash
node scripts/audit_public_dashboards_browser.mjs 9229 \
  https://penguin72487.ddnsgeek.com \
  artifacts/benchmarks/dashboards/browser-retest 1366 768 data-monitor
```

安裝／選擇支援繁中的本地字型後再做目視驗收；本輪 QA fontconfig 在 artifacts，僅適用此 Windows/WSL 主機。
輸出 JSON、每頁 PNG、每筆 API 的前後端 timing、long tasks、按鈕互動與錯誤；結束後關閉自己建立的測試瀏覽器。

### 主要證據檔

根目錄：`artifacts/benchmarks/dashboards/`（本機量測輸出，不透過公開 API 提供）。

- `2026-09-12-before-loopback.json`／`after-loopback.json`：31 路由前後觀測。
- `2026-09-12-before-public.json`／`after-public.json`：同參數 HTTPS，保留 IPv6 fallback 與較慢樣本。
- `2026-09-12-local-profile.json`、`2026-09-12-history-before.prof`：原始 history／cProfile。
- `2026-09-12-overnight-profile-final.json`：含約 9 MB Parquet 的 signature cold/hot。
- `2026-09-12-steady-recheck.json`、`2026-09-12-public-recheck.json`：30 次重測與 SSE。
- `2026-09-12-concurrency4.json`：有限併發原始樣本。
- `2026-09-12-code-fingerprint.json`：最終測速工具含實際程式碼 fingerprints 的 smoke receipt。
- `2026-09-12-final-http.json`：新增 Content-Type/JSON-root 嚴格檢查後 31 路由零錯誤；
  此輪與 pytest 同時執行，亦保留較慢的當沖 status 45.73 ms／history 199.36 ms batch median，不作閒置效能保證。
- `2026-09-12-browser-verified/`：最終八頁報告與中文截圖；`2026-09-12-browser-final/events-1366x768.png` 是事件表目視檢查。
- `2026-09-12-cleanup-loopback-c4.json`／`2026-09-12-cleanup-loopback-c4-final.json`：
  第二輪去重後四客戶端、20 樣本／路由的兩次測速；後者含獨立 details API。
- `2026-09-12-cleanup-history-profile.json`：第二輪獨立程序 history 8,051.34 ms／熱命中 0.38 ms，
  232,179 點與既有 SHA-256 完全相同，且量測期間來源 metadata 穩定。
- `2026-09-12-cleanup-browser-final3/`：第二輪八頁 1366×768 與 390×844 公網報告及首屏／明細截圖；
  含資料監控 summary-first 與明細 reveal-to-paint 量測。
- `2026-09-12-function-metrics-loopback.json`／`2026-09-12-function-metrics-public.json`：新增功能測速後的 HTTP 基線。
- `2026-09-13-final-loopback-c4.json`：最終 32 路由、四客戶端、每路由 10 次的原始樣本與分階段統計。
- `2026-09-13-final-public-targeted.json`：公網重點路由的新連線與 warm-reuse 分離；新連線受本機 IPv6 失敗影響。
- `2026-09-13-browser-performance-acceptance/`：跨頁本機 metrics、代表性操作、TAIFEX 冷切換與兩種 viewport 截圖／報告。
- `2026-09-13-browser-performance-recheck-mobile/`：390px 全資料明細的獨立變動性重測。
- `2026-09-13-data-monitor-responsive-final/`：筆電 25 列／手機 10 列的獨立響應式延遲驗收。
- `2026-09-13-taifex-cold-range-final/`：全新瀏覽器 profile、公開 gateway 冷啟動後的 TAIFEX 1h 首次切換。

## 2026-09-13 跨裝置與資訊密度續審

本輪把「各種螢幕都適配」拆成四個可驗證條件：資訊不能遺失、操作不能裁切、文字與圖表
必須可讀、非首屏成本不能拖慢首屏。物理 2K 不等於固定 CSS 寬度，因此驗收同時改變
viewport、DPR、直橫向與短螢幕高度，不用 User-Agent 或單一 `max-width` 猜裝置。

- 七份實體 HTML（隔日沖重用當沖 shell）統一最後載入 `dashboard-responsive.css`，
  共用可收合導覽、觸控尺寸、內容寬度與窄版資料表；不再由各頁互相覆寫成七套不同規則。
- 共用 core 從每張表的實際表頭建立窄版 label；動態訊號、持倉與事件列也會補上。
  1,100px 以下以卡片保留全部欄位，筆電不需左右滑表格；既有專用卡片表排除二次轉換。
- 手機導覽由水平滑列改為預設關閉的二欄選單，極窄改一欄；頁內跳轉會換行。
  桌面 1080p、2K 與 ultrawide 擴大有效內容區但限制行長，避免大螢幕只剩中央窄欄。
- 當沖與隔日沖首屏不再先下載完整委託／成交事件；實際手機驗收證明初始 API 清單沒有
  `/api/events`，捲到該區後才載入 100 列。警示仍完整保留，但首頁只先顯示首要問題與
  總數，其餘放在同一警示內可展開，避免重複長文把所有核心狀態推離首屏。
- 瀏覽器稽核新增 DPR／touch emulation、操作元件裁切與重疊、導覽溢位、圖表 marks、
  代表性功能操作及延後事件的真實 API 證據；另以固定 11 種裝置矩陣產生 aggregate receipt。

本輪只重啟公開唯讀 gateway。沒有重啟當沖、隔日沖、TAIFEX capture/dashboard 或 Discord；
畫面原本的 degraded／critical、未完整成交與資料缺口也沒有改寫成正常。最終矩陣、focused
tests、HTTP 安全探針及服務 PID 證據以本節下方的最新 acceptance artifact／命令為準。

最終公網 acceptance receipt：
`artifacts/benchmarks/dashboards/2026-09-13-responsive-acceptance-final/responsive-audit.json`。
11 個 profile、88 個頁面全部 status 0；audit/document/navigation/laptop-table/small-target/
mobile-touch/clipping/overlap/console/API/timing gate 全部為 0。手機當沖首屏的 initial resources
不含 events；捲到事件區後取得 100 列。Focused regression 為 261 Python＋25 Node 全通過，
相關 JavaScript syntax、Ruff 與 whitespace 檢查通過。

最新 HTTP receipt：
`2026-09-13-web-review-final-loopback.json` 與 `2026-09-13-web-review-final-public.json`。
本機四客戶端熱路徑中位數：當沖 status 9.89 ms、signals 8.56 ms、events 4.80 ms；
公網 keep-alive 熱中位數分別 2.14／1.94／5.85 ms。這不是冷啟動 SLA：全新公網連線仍因
本機 IPv6 失敗後回退 IPv4 約 3.06–3.25 秒；完整當沖 all-minute 首次觀測仍為 7.21 秒，
其 11.69 MB 回應在四客戶端下熱中位數仍約 754 ms。一般頁面 IPv4 live probe 為
16.6–23.0 ms。上述變動受同機、cache 與背景負載影響，原始樣本保留供後續同條件比較。

Live 安全探針：八頁與共用 CSS/JS 皆 200；`/.env`、`README.md`、`/api/order` 404，
POST 405，未知 query 400；gzip、representation ETag、immutable cache、CSP、`nosniff` 與
`Vary: Accept-Encoding` 皆存在。公開 gateway PID 2117858／NRestarts 0；當沖 137／0、
隔日沖 1513312／0、TAIFEX dashboard 187／0、Discord 1635／0 未改變。BidAsk capture
149030／1 的一次 restart 是本輪之前既有狀態。

## 尚未完成／下次優先順序

1. **先完成現有 merge 的語意審查**：`stockagent/backtest/simulator.py`、`stockagent/training/loss.py`、
   `stockagent/training/trainer.py`、`test/test_checkpoint_manifest.py`。本輪不擅自選擇交易／訓練分支；
   `test/test_cross_surface_performance.py` 因 conflict marker 無法收集，不能說全專案測試全綠。
2. **處理 IPv6 可達性／AAAA 政策**：先從獨立網路分別測 v4/v6，再決定 DNS、路由器或 Windows 規則的正確修復。
3. **繼續降低完整當沖歷史冷建置與大本文成本**：現在仍約 8 秒原始建置、11.69 MB 解碼 JSON；
   已有 cProfile 與輸出 hash 作回歸基線，後續優先檢視 canonical ledger 解析／持久索引，不能用刪分鐘或錯報熱快取掩蓋。
4. 延長基準樣本、固定來源與背景負載，加入不同筆電／ISP 的瀏覽器測試；本次不是長期高併發、DDoS、完整無障礙或全機斷電驗收。

## 規範依據

- [MDN Fetch API](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch)：headers 與 body 的完成邊界。
- [MDN AbortController](https://developer.mozilla.org/en-US/docs/Web/API/AbortController)：取消 fetch 與本文讀取。
- [W3C Server Timing](https://www.w3.org/TR/server-timing/) 與 [Resource Timing](https://www.w3.org/TR/resource-timing/)：前後端量測欄位與邊界。

## 2026-09-13 未完成項收斂驗收

本節取代上方「尚未完成／下次優先順序」中已完成的第 1、3 項；原始紀錄保留，避免把先前邊界改寫成當時已完成。

- 四個 Git conflict 已做語意合併，不是任選 ours/theirs：實體 FIFO 當沖狀態與期貨 carry 狀態並存，evaluation 同時保留原始 panel row 證據，physical loss 仍經資料 guard 並呼叫 batch-bound loss；兩組 checkpoint 測試都保留。`git ls-files -u` 為空，5,404 個測試可完整收集。
- 當沖 `all + 1m` 新增嚴格綁定 canonical source fingerprint 的持久 lossless projection。第一次從 canonical ledger 建立仍為 8.773 秒；第二個全新 Python 程序恢復為 0.565 秒，點數 232,179、序列化 11,694,359 bytes，兩者 SHA-256 同為 `b5a5d4566eaaabced2c2d07352f662ee2ea7a97a99c13019130a00b197ae5af2`。任何 ledger inode/size/mtime 或 benchmark generation 改變都會失效重建，沒有刪點、插值或把舊內容宣稱為新內容。
- TAIFEX dashboard 除既有七個 history range 外，新增 3.1 GiB marks ledger 的持久 line-count 與 daily-P&L endpoint 增量索引；相同 inode 僅讀 append tail，原子私有檔寫入。再次冷啟動實測 status 24 ms、1 日歷史 163 ms；BidAsk capture PID／restart count 未變。
- 部署後當沖 origin 冷 status 503 ms、summary 84 ms、完整曲線 459 ms。公開 gateway 第一次建立自己的隔離 projection 為 9.92 秒；該 projection 建立後再次重啟，完整曲線首次公開回應為 1.98 秒，熱路徑 70--185 ms。完整本文仍約 3.33 MB gzip，網路傳輸成本沒有被隱藏。
- 真瀏覽器驗收涵蓋 11 種 CSS viewport／DPR（320 手機至 2K／超寬桌面）乘 8 頁，共 88 頁；overflow、table overflow、touch target、clipping、overlap、console、API 與 timing gates 全為 0。receipt：`artifacts/benchmarks/dashboards/2026-09-13-responsive-review/responsive-audit.json`。
- 聚焦驗收為 428 個 Python tests passed、2 skipped，加 15 個 Node tests passed；全 repository 做了 5,404 tests collect-only 並成功，但沒有把 collect-only 宣稱成 5,404 tests 全部執行通過。
- 即時狀態刻意沒有改綠：當沖 `degraded`（既有未／部分成交與人工收盤結算證據）、隔日沖與 TAIFEX `waiting`、全資料監控 `critical`（42 unable、55 catching up）。這些是上游資料／交易事實，不是 UI 呈現錯誤。
- IPv4 HTTPS 實測約 25 ms 且正常。這一輪起初從同一台 WSL 做的 `curl -6` 仍失敗，但後續分層驗證確認：DNS AAAA 與路由器選定的 Windows IPv6 完全一致、路由器 `SUNSHINE6` 有精確的 80/443 forward allow、Windows Caddy 監聽 `::`:80/443，且 Windows 對該 IPv6 的 443 self-connect 成功。最後由 Internet.nl 的獨立外網 IPv6 probe 實測 `done=true, success=true`；因此公網 IPv6 已通，先前結果是同機 WSL／mirrored network／hairpin 路徑的假陰性，不再列為路由器待修。

## 2026-09-13 深度收斂續審

- 新增 `scripts/audit_public_ipv6.py`，以公開 DNS AAAA 加獨立 Internet.nl IPv6 probe 共同驗收，輸出原子 JSON receipt；不再把 dashboard 主機自己的 WSL `curl -6` 冒充外部 WAN 證據。實測 receipt：`artifacts/benchmarks/dashboards/2026-09-13-public-ipv6-audit.json`，結果通過。
- 全資料監控修正 active-scope 分母：供應商明確不提供的粒度屬 `reference/not_applicable`；尚缺必要憑證的產品屬 `deferred/waiting_credential`。兩者仍顯示於完整清冊，但不再冒充可執行下載失敗。部署後由 `critical / 42 attention` 修正為 `degraded / 35 attention`；35 筆仍是真實失敗或已註冊但尚未接管線，沒有藏掉。
- 首頁快速狀態原先只讀 engine schedule receipt，因此休市會顯示 `waiting`，即使明細已有 partial/no-fill 或 stale position。現在快速 projection 用同一份小 receipt 聚合執行異常，不讀大 ledger 仍可與明細一致顯示當沖 `degraded`；隔日沖正常休市等待仍為 `waiting`。
- `stockagent-shioaji-minute-backfill.service` 原本每輪掃完 2,754 個商品後，才因直接執行 `scripts/*.py` 導致 repo root 不在 import path，於 `import downloader` 失敗；累積 1,597 次 restart。runner 已全面改成 `python -m scripts...`，重啟後 `NRestarts=0` 並越過原失敗點完成資料集 build，完整 partition audit 仍依 receipt fail closed。
- 真瀏覽器全頁矩陣首先找到 OpenBB 頁內「進度／分類」在橫向手機與平板只有 38×40 px；共享 responsive layer 改成最小 44×44 px 並升版資源 URL，針對該頁的 11 種 viewport/DPR 重測全部通過。receipt：`artifacts/benchmarks/dashboards/2026-09-13-responsive-retest3/responsive-audit.json`。
- 分鐘曲線維護的錯誤 receipt 過去只留下 `missing_after=3`，無法知道缺哪個商品日期。錯誤契約已新增最多 20 筆 `missing_after_sample`，下一輪能直接定位上游缺口，仍不會插值或製造價格。

## 2026-09-13 最終根因修復與實跑驗收

- 永豐分鐘回補的完整正式週期已成功結束，不再於昂貴掃描後重啟：2,754 個商品中 2,627 個有分鐘資料，涵蓋 1,593 個交易日、311,191,907 列；本機日線 materialization 為 2,336 complete、127 unavailable、291 outside source window，且沒有發出額外 API request。衍生資料集保留 68 筆 source-gap fallback 並遮罩，不把它寫成無缺口。partition audit 為 `status=ok`，服務 `Result=success`、`NRestarts=0`，實跑 wall time 40 分 39 秒。
- 永豐頁原把「已追到 target date、但來源有已遮罩缺口」寫成尚未追到最新。現在 acquisition clock 與 completeness 分開：`data_through=target=2026-09-11` 顯示「已追到最新交易日；來源缺口已遮罩」，狀態仍為 `partial`，不會因日期已追平而變綠。
- 完整分鐘曲線正式維護第一次暴露 00787B／6680（2026-09-10）與 6655（2026-06-22）三個無分鐘成交點。前兩者只在 hash-pinned 的人工「最後成交價、非券商成交」receipt 通過時豁免；6655 由 canonical open 的官方無成交清單與前一筆實際價格支持。三者都沒有補造 K 棒。
- 第二次整合重建再找到一個獨立帳務時鐘錯誤：同日 13:30 融券轉換費的 immutable reversal 被提前套到 09:01。修正後原 debit 與 reversal 都只在 13:30 生效；09:01 兩者皆不生效，13:30 淨額為零。最終正式維護 `Result=success`：137 個交易日、5 個模式、每模式每天 270 點，共 184,950 個策略點；49,255 個必要商品日期組合缺口 0，183,580 個歷史 interior rows 全部通過來源稽核，09:01／13:30 端點保持，插值 0。
- 基準歷史過去即使只有 top-level `created_at` 改變，也會原子替換 290.8 MB canonical 檔並使公開 projection/cache 失效。writer 現在先做去除 volatile envelope 欄位的語意比較；內容相同便保留原檔、mtime 與快取，真實 mark／provenance 變更仍原子發布，並在 CLI receipt 回報 `changed`。
- 公開 gateway 的 OpenMP 啟動警告不是單純 UI 噪音。受控相容性探針確認 portable `OMP_PROC_BIND=false` 在此環境仍會觸發 affinity syscall；最終只設定 Intel runtime 的 `KMP_AFFINITY=disabled`。最終部署 gateway 自 04:36 啟動後 PID 2290629、`NRestarts=0`，該 invocation 的 journal 無 warning/error，沒有把 process abort 留給 systemd warming 掩蓋。
- 外部 IPv6 結論已由 receipt 固化：DNS A/AAAA、路由器／Windows 路徑與 Internet.nl 獨立 WAN probe 分開記錄；外部 probe `done=true, success=true`。同機 WSL `curl -6` 仍只是 mirrored/hairpin 限制，不再列作公網雙棧失敗。
- 資料監控的 active failure 分母已從 42 修正為 35；49 個待憑證項目與 18 個不適用參考項仍完整顯示但不冒充執行失敗。35 個 active attention 仍包含已註冊未實作的 materializer、真實來源缺口及上游失敗，網站沒有隱藏。`stockagent-registered-data-daily.service` 仍因 `dune_crypto_history` 失敗而是 failed；這是上游修復／Dune entitlement 問題，不是前端能消除的狀態。
- 首頁快速摘要已會因當沖殘餘顯示 degraded，但「需要注意」原本沒有獨立列出非 `critical*` 命名的 residual 狀態。現在所有 `tw_day_trade*` 模式只要收盤後 `open_position_count > 0` 就新增 `intraday_residual_open` error；隔日沖正常跨夜庫存不套用此規則。現場已顯示 5314 的 1 筆殘餘、1 筆 stale 與 1 次強制退出失敗，並明示它必須優先沖銷、不是正常隔夜持倉或券商成交保證。
- 最終全 repository Python suite 不是 collect-only：`5,401 passed, 18 skipped, 1 xfailed`，0 failed，耗時 887.95 秒。29 個 Node tests 全通過；Node 曾找到一筆 rollover 測試仍要求 eager events，已改成同時驗證核心訊號／持倉不被歷史請求阻塞，且事件維持 viewport lazy-load。Focused Ruff 與 `git diff --check` 通過。
- 最終乾淨負載真瀏覽器 receipt：`artifacts/benchmarks/dashboards/2026-09-13-responsive-final-clean/responsive-audit.json`。11 種 viewport/DPR × 8 頁共 88/88 通過；document/navigation/table overflow、small/touch target、clipping、overlap、console、API、timing 共 11 類 gate 全為 0。前一輪 2K 曾出現單次 `ERR_NETWORK_CHANGED`，獨立同 profile 重測與這次全矩陣皆通過，沒有刪掉該舊 receipt。
- 最終 32 路由 loopback、四 client、每 client 10 次 receipt：`artifacts/benchmarks/dashboards/2026-09-13-final-review-loopback.json`，全部零錯誤。當沖 status/signals/events 熱中位數為 11.30/11.36/6.52 ms；TAIFEX 1 日歷史四 client 同時傳輸的中位數 283.14 ms。canonical 分鐘曲線剛完成真實更新，使完整 232,179 點公開 projection 正當失效；重建首次觀測 11.84 秒，之後四 client 大本文中位數 950.75 ms。這仍是下一階段增量 projection 的主要效能缺口，不能用熱快取值掩蓋。
- 最終同主機 HTTPS keep-alive、單 client、每路由 10 次的重點 receipt：`artifacts/benchmarks/dashboards/2026-09-13-final-review-public-targeted.json`。overview、當沖 status/signals、隔日沖 status、TAIFEX status、data summary 熱中位數為 6.72/3.38/2.49/2.52/3.42/2.24 ms，零錯誤；這包含 Caddy/TLS 路徑但不是外部 ISP RTT。
- Live 安全重驗：八頁與兩個共用資源皆 200；`/.env`、`/README.md`、`/api/order` 404，POST 405，未知 query 400；同 representation ETag 為 304，gzip、immutable asset cache、CSP、HSTS、`nosniff`、`Vary: Accept-Encoding` 均存在。origin 服務仍只監聽 127.0.0.1:8765/8766/8770。
- OpenBB L1 壓縮不是卡死：目前每次正常工作都要先驗證 16 GiB SQLite manifest 與約 250 萬筆既有 membership，舊的 `TimeoutStartSec=20min` 已連續把仍有進度的工作誤殺。短期可靠性修正把 timeout 改為 40 分鐘，保留單一 instance lock、2,048 source-file 上限、3 GiB `MemoryMax` 與低 IO/CPU 權重。安裝後的正式週期在 12 分 14.28 秒完成：20 個新 segment、0 stale、0 failed、42 個 views、2,516,371 個已壓縮 source files，服務 `Result=success`、`NRestarts=0`。這只修正 supervisor 邊界；4,801,271 個 pending files 與每輪全 manifest 驗證仍需後續以 task-mutation 增量失效索引處理，不能把加 timeout 寫成延遲優化。
- 最後一次 `systemctl --failed` 只剩 `stockagent-registered-data-daily.service`；其 `dune_crypto_history` 仍需有效 entitlement／query source 才能修復。全 Python suite 的 `5,401 passed` 是 OpenBB systemd template 最後調整之前執行；template 調整後另跑其 boot/data-monitor contract focused tests（6 passed），沒有把未重跑的完整 suite 說成最後一個位元都重新驗證。

## 2026-09-13 全歷史熱點、繁中字型與可重測基線續審

本輪從責任邊界重新拆解頁面延遲：來源建置、服務端序列化、HTTP 傳輸、JSON 解碼與畫面更新
分開量測；不以頁面已顯示、熱快取命中或刪除分鐘點代替正確性。`cProfile` 顯示真正的來源重建
會逐行做約 20 萬次 Python `json.loads`，而完整 `marks.jsonl` 約 219 MB、
`benchmark_history.json` 約 291 MB、即時基準 ledger 約 28 MB。這是本輪優先處理的 CPU/GIL 熱點。

- 全期間策略與即時基準 ledger 改走 Polars native NDJSON reader；小檔、缺少 Polars 或混合 schema
  仍回退既有嚴格逐行 decoder，不改來源權威、時間粒度或錯誤語意。獨立程序 A/B 為
  15.749 秒降至 13.629 秒，點數同為 300,304、序列化長度同為 15,125,865 bytes，SHA-256
  同為 `e94814a9ee84dd3e440f29ed5c55444888a944b4b6941c2107d2055f39dc797c`；這約 13.5%
  改善，不宣稱已達完整歷史的最終目標。
- 測速工具新增 `--profile-history-source-rebuild`。它明確關閉最終記憶／持久 projection，仍保留
  OS page cache 與 helper indexes 的邊界說明，並只以真正影響 builder 的來源判定量測期間是否穩定；
  `state.json` 只比較實際使用的 `product`，不再因無關的即時 receipt 原子更新產生假失敗。可重測
  receipt `artifacts/benchmarks/dashboards/2026-09-13-native-ledger-source-rebuild-final.json`
  實測 13.053 秒、來源穩定、300,304 點、8 條序列且 hash 相同。
- 七份實體 shell（隔日沖重用當沖 shell）改用同一繁中字型 fallback token，明列 Noto Sans TC、
  PingFang TC 與 Microsoft JhengHei，且不連第三方字型服務。Linux 驗收主機原只有 DejaVu 與一份
  Chromium 未採用的 TTC，畫面雖通過 DOM 稽核卻實際出現 tofu；安裝開源 Noto CJK、重開全新
  Chromium 後已用截圖確認繁中可讀。這項發現也證明 DOM/console 零錯誤不能取代視覺驗收。
- 使用實際 CJK glyph metrics 重跑 11 種 CSS viewport/DPR × 8 頁，共 88/88 通過；document、
  navigation、table overflow、small/mobile touch target、clipping、overlap、console、API 與 timing
  gate 全為 0。receipt：
  `artifacts/benchmarks/dashboards/2026-09-13-native-ledger-responsive-noto-v9/responsive-audit.json`。
  1366×768 八頁獨立驗收也全通過；當沖 status 首次可見樣本 11.7 ms、事件區延後載入至可繪製
  426.5 ms，這是同主機公網/hairpin 當次觀測，不是外部 ISP SLA。
- 公開 gateway 部署後維持 `active/running`、`Result=success`、`NRestarts=0`，origin 只監聽
  `127.0.0.1:8765/8766/8770`。八頁與共用資源 200；`/.env`、`README.md`、`/api/order`
  404，POST 405，未知 history query 400，CSP/HSTS/`nosniff` 與 immutable asset cache 仍存在。
  本輪只重啟唯讀公開 gateway，沒有重啟交易、Discord 或資料擷取服務。
- 目前狀態沒有被 UI 改綠：資料監控 active scope 為 286，其中 35 unable、46 catching up、
  205 complete；另有 49 deferred 與 18 reference 完整保留。當沖仍為 `degraded`，5 個模式中
  `tw_day_trade_100m` 有 1 筆收盤後異常殘餘及過期估值；隔日沖與 TAIFEX 在休市日維持
  `waiting`。這些需要上游資料或執行證據修復，不是前端刪除警示能解決。

下一個最大效能缺口仍是 source rebuild 約 13 秒及 15.1 MB JSON 本文。正確的下一步是把
canonical minute projection 依 session/series 做不可變分片與 append-only delta 更新，再比較
相同 300,304 點與 hash/語意 receipt；不是縮短歷史、降低分鐘粒度或把持久快取命中寫成重建速度。

## 2026-09-13 完整歷史來源重建優化

本輪把上述約 13 秒再拆成 ledger 日期索引、NDJSON schema 推斷、Python row projection、時間解析、
去重／排序與輸出編碼。修改仍只針對唯讀 projection；canonical ledger、每分鐘粒度、帳務計算與
成交證據都沒有改動。

- `benchmark_marks.jsonl` 的前段有欄位全為 null、後段才出現字串。原生 bounded schema 推斷失敗時，
  過去會把整檔退回逐列 `json.loads`；現在先做一次 native full-schema retry，真正格式錯誤才回到嚴格
  decoder。`marks.jsonl` 則維持較快的 bounded inference。
- 完整策略 ledger 只投影曲線需要的 18 欄，經 Polars positional rows 進入 Python；不再為 184,950 列
  建立 28 欄 named dict。相同的 `add_values` 契約仍負責時區、09:01--13:30、資本報酬、估值來源與
  canonical benchmark 優先權，避免快路徑產生第二套財務語意。
- 全歷史本來就會讀每一列，不再先額外掃描 246 MB NDJSON 建日期 byte-span index；日期直接由同一批
  已解析分鐘列取得。起訖日篩選仍使用持久 span index，因此舊日期 append 隔離與範圍正確性不變。
- 同一 history query 出現新 revision 時，gateway 只在有限 stale window 內立即回傳上一份帶時間戳、
  已驗證 response，並以 single background rebuild 建立新版；第一個冷 build、逾期內容與不同查詢不共用。
  啟動預熱也在首頁、當日狀態與 TAIFEX 首要資料就緒後建立完整 1m history，避免瀏覽器成為付首次成本者。

同一份穩定來源、同一個新 CLI process、關閉 final memory/persistent projection 的單次重測如下；
它是可重現樣本，不是長期 SLA：

- 無 helper-index 環境：13,052.73 ms 降至 9,698.07 ms（25.7%）；receipt：
  `artifacts/benchmarks/dashboards/2026-09-13-history-no-double-scan-final.json`。
- 正式服務的 `/var/cache/stockagent-public-dashboards` helper-index 環境：7,632.66 ms 降至
  6,318.10 ms（17.2%）；receipt：
  `artifacts/benchmarks/dashboards/2026-09-13-history-source-rebuild-optimized-final.json`。
- 四份比較輸出均為 300,304 點、8 序列、15,125,865 bytes，canonical SHA-256 均為
  `e94814a9ee84dd3e440f29ed5c55444888a944b4b6941c2107d2055f39dc797c`，來源量測期間穩定。

部署後完整 history 已預熱；loopback 實際回應的 `Server-Timing app` 為 1.539 ms，gzip 傳輸
91.6 ms，解碼後仍是相同 300,304 點與 canonical hash。外部 IPv4 HTTPS 單次完整本文為 400.0 ms；
它包含網路與 3.59 MB gzip 本文，不是 source rebuild。224 個 Python 與 29 個 Node regression 全通過；
真 Chromium 對當沖頁跑 11 種手機／平板／筆電／1080p／2K profile，11/11 通過且 overflow、touch、
clipping、overlap、console、API、timing gate 均為 0。receipt：
`artifacts/benchmarks/dashboards/2026-09-13-history-optimization-browser/responsive-audit.json`。

公開 gateway 只監聽 `127.0.0.1:8770`，部署後 PID 2413325、`NRestarts=0`；當沖、隔日沖與 Discord
服務沒有重啟。13 秒已不是正式服務條件下的現況，但 6.3 秒仍是完整 source rebuild 成本；下一階段若要
繼續壓低計算本身，應將 canonical projection 改成 session/series immutable shards 加 append tail，
並繼續用完整點數與 hash 驗收，不能用 stale response 或熱快取數字冒充來源重建。

## 2026-09-13 繪圖架構、明細索引與互動延遲收斂

本輪沒有改 canonical 帳本、財務語意、分鐘粒度或交易服務；調整範圍是唯讀 projection、公開 gateway
與瀏覽器呈現。第一性拆解後，完整歷史的使用者延遲其實由四個獨立問題組成：傳輸結構、瀏覽器物件
配置、SVG DOM 數量，以及日期控制器重複要求語意相同的全期間資料。明細頁另有寬列解碼與泛用物件
快取放大問題，不能只靠替換前端框架處理。

- 新增相容的 `minute_columns_v2`：一份排序後 epoch-minute axis 加每條序列的 index/value/quality 欄，
  舊 v1 API 維持預設。v1/v2 對 8 條序列逐點比對皆為 300,304 點。raw JSON 由 15,125,865 降至
  13,913,805 bytes（約 8.0%）；gzip 則由 3,593,127 增至約 3,628,851 bytes，因此本輪主要收益是
  瀏覽器配置與 DOM，不把 gzip 寫成改善。
- 完整曲線改用本機 vendored、MIT 授權的 uPlot 1.6.32 Canvas renderer；沒有 CDN、eval 或外站依賴。
  renderer、資料解碼、legend、缺值／過期品質與 resize 被拆成可重用 `chart-renderer.js`，並沿用共享
  dashboard core。舊實測 SVG 流程總計約 940.8 ms、heap 110.7 MB；新架構的 11-profile Chromium
  實測完整互動中位數 802.1 ms，其中 API 328.3 ms、JSON parse 40.8 ms、資料對齊 45.3 ms、Canvas
  construct 0.8 ms、paint opportunity 138.2 ms。每次都真的繪出 300,304 點且 Canvas 1、SVG path 0。
- 日期起訖等於完整可用邊界時，前端改重用同一個無界、啟動已預熱的 canonical cache key，再在 client
  保留明確的起訖語意。這修正了完全相同資料因 explicit-date key 再建一次的錯誤；1366×768 loopback
  互動由舊 receipt 的 17.708 秒降至 744.6 ms，公網 HTTPS 為 827.9 ms。公網樣本的 server 7.083 ms、
  body 165.8 ms、parse 43.0 ms、prepare 84.1 ms、Canvas 1.0 ms，console/API error 皆為 0。
- 瀏覽器效能紀錄新增 `render` 類別，保留 prepare/draw/paint、點數、序列數與 viewport；流量頁可直接
  篩選「資料整理與繪圖」。資料只留在該瀏覽器的 bounded localStorage，不上傳查詢文字、帳號或輸入值。
- 持倉歷史不再讓泛用 `_object` cache 長期保留 690 份寬 JSON。新增以 inode/size/mtime 驗證的 9 欄
  locator index，先排序／篩選 compact entries，再只回讀畫面實際需要的來源列。真實 203 MB source
  第一次建立 4.257 秒；1.25 MB 持久索引完成後的新程序為 0.911 秒，輸出仍是 44,930 筆中的相同頁面。
- 訊號全期間仍忠實掃描 bounded 最新 100,000 列，但先倒序定位原始列，再由 native reader 只投影公開
  與稽核需要欄位；冷請求由 7.393 秒降至 2.105--2.293 秒。事件改成 Arrow batch projection、去重計數
  與 bounded page heap，不再同時展開 20 萬份完整 Python dict；冷請求由 9.520 秒降至 2.531--2.679 秒。
  後續同 revision 的公開回應中位數為持倉 1.860 ms、事件 2.296 ms、訊號 1.983 ms；receipt：
  `artifacts/benchmarks/dashboards/2026-09-13-final-uplot-targeted.json`。
- 改版前一次全頁稽核令 gateway 常駐 RSS 升至約 2.54 GiB、cgroup peak 3.58 GB；根因不是 55 MB 的
  HTTP response cache，而是歷史 JSON 寬物件與 native allocator。乾淨部署預熱後 RSS 約 604 MB；
  全期間三明細、重複模式篩選與瀏覽器矩陣後穩定約 0.93--1.02 GiB，沒有線性成長，cgroup peak
  1.257 GB。這是約 2.8 倍 peak 降幅，不把它寫成零記憶體成本。
- 最終 responsive receipt：`artifacts/benchmarks/dashboards/2026-09-13-final-responsive/responsive-audit.json`。
  11 種 320px 手機至 2K／超寬 viewport 的當沖與隔日沖全部 status 0；overflow、navigation、table、
  touch target、clipping、overlap、console、API、timing gate 全為 0。公網 1366 receipt：
  `artifacts/benchmarks/dashboards/2026-09-13-final-postrestart-public/report-1366x768.json`；重啟後完整互動
  769.7 ms、API 323.3 ms、prepare 65.8 ms、Canvas 0.7 ms、paint opportunity 127.5 ms。
- 本輪聚焦回歸為 308 個 Python tests 與 30 個 Node tests 全通過，focused Ruff、所有修改 JS syntax、
  `git diff --check` 皆通過；沒有把先前的全庫測試結果冒充成這次修改後重新執行 5,401 個測試。
- 部署只重啟 `stockagent-public-dashboards.service`。最終 PID 2509479、active/running、`NRestarts=0`，
  來源仍只監聽 `127.0.0.1:8765/8766/8770`。公網 shell/vendor/traffic 均 200，`/.env`、`README.md`、
  `/api/order` 為 404，POST 為 405，未知 encoding 為 400；CSP、HSTS、`nosniff` 與 `Vary` 仍存在。
- 狀態沒有被效能優化改綠：當沖仍 `degraded`（包含 1 筆異常殘餘）、隔日沖與休市 TAIFEX 為
  `waiting`；資料監控為 `degraded`，active scope 286 中 35 unable、58 catching up、193 complete。

完整 canonical source rebuild 在 helper-index 條件下仍約 6.3 秒，這和已預熱公開回應是兩個不同指標。
若來源重建也必須進一步下降，下一階段才應實作 immutable per-session projection shards 與 append delta；
目前前端不需要為此導入一套新的 SPA build/runtime framework，否則增加冷啟動與供應鏈面，卻不會移除
Python canonical join 的主要成本。

## 2026-09-13 immutable session projection 與 append-only delta

前一節留下的 6.3 秒問題已實作，不需要全面重寫前端。資料流現在分成三個可獨立驗證的層次：

1. `benchmark_history.json` 保留為相容的 canonical 單體來源；正式重建器另外發布
   `benchmark_history_projection/v1`。每個交易日是 SHA-256 content-addressed gzip shard，`head.json`
   原子切換目前版本，`delta.jsonl` 只追加實際新增、替換或移除的交易日。來源欄位壓縮與 dashboard
   使用同一份 contract；每個 interior minute 只保留投影需要欄位，每個 benchmark/session 的末列仍保留
   完整 audit 欄位。所有檔案為 0600，讀取時逐 shard 驗證路徑、schema、日期、筆數及內容 hash；任一
   不符就 fail closed 回退 canonical JSON。
2. 策略 `marks.jsonl` 與 live `benchmark_marks.jsonl` 保留 append-only canonical ledger。既有 byte-span
   index 現在額外記住每個 session 的內容 digest；同 inode append 時只重新 hash span 有變化的日期，
   原子換檔則重新 hash bytes 但不重新配置所有 JSON row。benchmark source session hash、live rebasing
   origin hash、product/schema contract 一起形成 session source fingerprint，避免錯誤重用。
3. 唯讀 dashboard cache 以該 fingerprint 發布第二層 `history-session-projection-v2` immutable shard 與
   append-only delta。最終 all-history cache 失效時直接合併未變日；最多 8 個變更日才逐日走原本的
   時區、09:01--13:30、資本報酬、canonical benchmark 優先權與品質旗標邏輯，異常大量變更則回到完整
   rebuild。日分片只儲存共同 minute axis 的 indexes、原始 cumulative return、品質旗標與精確 aggregate，
   全期間選定起點 0% 與絕對權益累積仍在 merge 時按原 contract 計算，沒有插值、刪分鐘或改成交證據。

完整 300,304 點、8 序列的舊路徑與新路徑逐欄序列化後 SHA-256 都是
`d82297cf36c91bab7f022573535467beef02268fdfd8519331fca1aba8240811`；包含浮點 aggregate、日期、
品質旗標與 range summary 都完全相同。量測邊界仍是新 CLI process、OS/helper index 可熱，不冒充磁碟
cold SLA：

- 同時 bypass final cache 與 session projection 的完整 canonical control：7,043.51 ms；receipt：
  `artifacts/benchmarks/dashboards/2026-09-13-full-source-rebuild-control.json`。
- 只 bypass final cache、保留可驗證 session projection：第一次 859.94 ms，後續 722.13／745.63 ms，
  對 control 首樣本減少 87.8%；receipt：
  `artifacts/benchmarks/dashboards/2026-09-13-session-shard-source-rebuild-v2.json`。
- 在獨立 cache copy 刻意刪除最新 `2026-09-11` shard，確認只回讀該日 strategy/live spans 並合併其餘
  136 日；新 process 建置 1,070.40 ms、peak RSS 240,620 KiB，輸出 hash 仍相同。這是一次受控故障
  樣本，不是未來每個新增日的 SLA。

首次完全沒有任何 session projection 時仍需真實掃過 canonical rows；同步建立 137 份分片的本次樣本
約 8.0 秒，之後才進入增量路徑。公開 gateway 本來就在 listener 啟動後的背景預熱 worker 建 full history，
因此不阻塞 socket bind 或首頁／當日狀態；benchmark projection head 也加入小檔 watcher 和 revision token，
不需要每秒 hash 218--291 MB 大檔。基準重建、分鐘曲線候選、直接 publish 與 open-price replay candidate
現在都共用同一 projection writer，避免不同維護入口產生風格或語意分叉。

效能工具也把兩個事實分開：`--profile-history-source-rebuild` 測「final cache 失效、session projection
有效」，新增 `--profile-history-full-source-rebuild` 才同時 bypass session shards，保留真正全掃描 control。
本輪另移除 benchmark script 重複／含混的量測語意，未導入新的前端框架或 CDN；UI、Canvas renderer、
共用 components、responsive 規則及公開 DTO 都維持上一節已驗證的一致 contract。

部署只重啟 `stockagent-public-dashboards.service`；最終 PID 2529907、active/running、`NRestarts=0`，
8765、8766、8770 仍只監聽 `127.0.0.1`。重啟後在完整 final projection 尚未預熱完成時，公網第一個
all-history request 實際走 session merge，`Server-Timing build=1263.378 ms`，仍回傳相同 300,304 點與
SHA-256；預熱後 1366×768 真 Chromium 完整曲線互動為 609.4 ms（API 223.3、parse 37.8、prepare
35.2、Canvas 0.5、paint opportunity 92.3 ms），Canvas 1、SVG path 0、console/API error 0。receipt：
`artifacts/benchmarks/dashboards/2026-09-13-session-shards-final/report-1366x768.json`。

11 種 320px 手機至 2K／超寬螢幕的當沖與隔日沖重新驗收，11/11 status 0；document/navigation/table
overflow、small/touch target、clipping、overlap、console、API、timing gate 均為 0。receipt：
`artifacts/benchmarks/dashboards/2026-09-13-session-shards-responsive/responsive-audit.json`。本輪相關
回歸共 344 個 Python、30 個 Node tests，Ruff 與 `git diff --check` 全通過。公網 `/.env`、`README.md`、
`/api/order` 為 404，POST 為 405，未知 encoding 為 400；CSP、HSTS、`nosniff`、same-origin policy
與 `Vary` 都存在。本輪產生後已被 v2 取代的 3,406,884-byte 私有 v1 cache 已移出 active cache 到
`/tmp/stockagent-obsolete-history-session-projection-v1-20260913`，可復原也可由 canonical source 重建。

效能修改沒有把營運狀態改綠：最終公網仍忠實顯示當沖 `degraded`（1 個 open position）、隔日沖與
休市 TAIFEX `waiting`；data monitor `degraded`，286 個 active endpoint 中 35 unable、56 catching up、
195 complete。這些上游資料／部位問題不是投影分片能消除的。
