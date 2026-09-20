# 公開面板架構與正確性契約

這份文件是公開面板的維護入口。目標不是描述每一行程式，而是讓人與 AI 在修改前能先回答四個問題：資料真值在哪裡、公開層可以做什麼、快取何時失效、什麼證據才算上線成功。

## 系統邊界

```text
本機資料／receipt／模擬帳本
        │
        ▼
stockagent.live.* snapshot builders
        │  僅投影 allowlist 欄位
        ▼
scripts/serve_public_dashboards.py
  ├─ 同源唯讀 API
  ├─ single-flight + stale-while-refresh
  ├─ ETag + gzip + 有界 LRU 回應快取
  └─ 安全標頭與靜態資源
        │
        ▼
services/public_dashboards/dashboard-core.js
  ├─ 同源 fetch／timeout／JSON 根型別驗證
  ├─ latest-request-wins 競態控制
  ├─ 共用格式化、導覽與更新排程
  └─ 跳過未變更文字／HTML DOM 寫入
        │
        ▼
八個公開頁面（隔日沖重用當沖 renderer）
```

公開閘道是唯讀檢視層，不是交易控制面。它不得送單、修改帳本、啟動訓練、觸發下載或將反事實回放改稱即時成交。面板部署只能重啟 `stockagent-public-dashboards.service`；不得為了更新 UI 重啟當沖模擬或交易引擎。

## 頁面與資料責任

| 頁面 | 主要 API | 真值責任 |
|---|---|---|
| `/` | `/api/overview` | 各面板的低成本可用性摘要 |
| `/taifex/` | `/taifex/api/status`, `/taifex/api/history` | TAIFEX 策略狀態與歷史投影 |
| `/tw-day-trade/` | `/tw-day-trade/api/*` | 當沖狀態、分鐘曲線、訊號、持倉、事件與公開資料進度 |
| `/tw-overnight/` | `/tw-overnight/api/*` | 隔日沖獨立帳本；共用畫面但保留集合競價事件時間粒度 |
| `/shioaji/` | `/shioaji/api/status` | 永豐資料流程、配額、流量與儲存量 |
| `/openbb/` | `/openbb/api/status`, `/openbb/api/history` | OpenBB 封存與歷史進度 |
| `/data-monitor/` | `/data-monitor/api/summary`, `/data-monitor/api/details`, `/data-monitor/api/features`（完整相容回應：`status`） | 全資料來源的 receipt、覆蓋、freshness 與實存 Parquet 逐欄位清冊 |
| `/traffic/` | `/traffic/api/status`, `/traffic/api/history` | 匿名請求延遲、分階段耗時、吞吐、錯誤率、快取容量及 90 天趨勢 |

新增欄位時，應先在資料建置層定義語意，再加入公開 allowlist，最後才渲染。前端不得從名稱猜測單位、時區、成交狀態或資料完整性。

## 不可破壞的正確性契約

- 缺資料必須顯示為缺口、等待、過期或部分完成；不得插值或用其他價格冒充。
- 當沖歷史回放與即時模擬是不同資料域。回放必須保留反事實標籤，不能宣稱是券商成交。
- 當沖 `/api/status` 為低成本摘要，`position_rows_included=false` 時其中的 `positions: []` 代表未載入，不代表當日沒有持倉。完整「持倉與生命週期」僅以受日期／模式／狀態篩選的 `/api/positions` 分頁為準；刷新摘要不得覆蓋已顯示的明細或將未載入標示成零筆。
- 當沖與隔日沖共用的訊號、持倉、事件分頁，只有日期範圍、offset／returned／total／has_more 和請求版本驗證通過後才可取代畫面資料；同篩選的更新失敗保留上一份並標示失敗，切換篩選時不得把舊範圍偽裝成新結果。分鐘曲線與台股官方來源監控遵守同一「保留最後成功資料並揭露更新失敗」原則。
- 每分鐘曲線的絕對權益持續從初始資金累積；使用者選定期間只把顯示報酬率的第一個可見點設為 0%，不重置絕對資金。
- 0050、2330 與 TXFR1 是 Buy-and-Hold 基準；跨日報酬以同一實體部位相對前一收盤計算。TXFR1 換月價差只調整外部現金，不製造投資報酬或槓桿。
- API 時間一律附時區或明確標示 UTC；瀏覽器顯示轉為 `Asia/Taipei`。
- 所有公開 API 回應根節點必須是 JSON object。錯誤回應不得被當成正常資料繼續渲染。

## 性能與更新契約

### 共用響應式與跨裝置契約

瀏覽器排版依 CSS pixel 與可用 viewport，不依面板的物理像素名稱猜裝置。HiDPI／2K
螢幕由 `devicePixelRatio` 提高銳利度，但相同 CSS viewport 應維持相同資訊層級。
`dashboard-responsive.css` 是八頁最後載入的共用響應式層；頁面專屬 CSS 只保留品牌、
圖表與領域元件差異，不得各自複製全站導覽、觸控目標或表格窄版規則。

- `dashboard-core.js` 建立同一套可收合全站導覽，手機預設關閉，Escape 與選取連結會收合；
  導覽與表單在觸控 viewport 至少保留 40px，主要控制至少 44px。
- 320–700px 使用雙欄（極窄時單欄）導覽與可換行的頁內跳轉，不以整頁水平捲動藏內容。
- 資料表在 1,100px 以下以實際 `th` 文字自動補上 `td[data-label]`，重排為保留全部欄位的
  標籤卡片；動態新增的列由一個共用 observer 補強。已有領域卡片版面的資料監控與
  TAIFEX 策略表排除重複轉換。
- 1,280×720／1,366×768 筆電不得出現文件、導覽或資料表水平溢位；1,920×1,080、
  2,560×1,440 與 2,560×1,080 使用較寬但有上限的內容區，避免內容只擠在中央小欄或
  無限制拉長行寬。
- 手機圖表可在明確標示的局部區域水平檢視，以保住分鐘刻度與曲線可讀性；不得讓整份文件
  水平捲動，也不得用隱藏欄位、截斷資料或 10px 以下文字假裝適配。
- 當沖／隔日沖的訊號仍屬首屏關鍵資料；完整委託成交事件只有接近事件區時才讀取。
  未啟用前須顯示載入說明，使用者捲動後才可視為事件 API 驗收完成。

完整重測入口：

```bash
node scripts/audit_public_dashboards_responsive.mjs 9229 \
  https://penguin72487.ddnsgeek.com \
  artifacts/benchmarks/dashboards/responsive-retest
```

固定矩陣涵蓋 320／390 手機直向、手機橫向、768／1024 平板、720p／標準／HiDPI
筆電、1080p、2K 與 ultrawide。每個 profile 必須跑完八頁，檢查文件／導覽／筆電表格
溢位、裁切與重疊操作元件、console／API 錯誤、代表性互動、延後明細及實際截圖。

### 2026-09-12／13 可重測的請求與快取邊界

- `createFetch` 回傳的是收到標頭的 Response；timeout 與上游取消訊號會保留到本文讀完。
  呼叫者必須使用 `readJsonResponse`／`readTextResponse`，不需要的本文使用 `cancelResponse`。
  同源檢查包含字串、URL 與 Request，並拒絕 HTTP redirect，避免跳出唯讀同源邊界。
- 每頁 `StockAgentDashboard.performanceSnapshot()` 留存最近 128 筆請求的標頭、本文、
  JSON parse 與總時間；只記 pathname，不記 query、payload 或使用者身分，不回傳伺服器。
- 所有頁面的 `performanceHistorySnapshot()` 另保存頁面載入、click/change/input 與 API
  到第二次 `requestAnimationFrame` 的瀏覽器觀測。它只寫同源 `localStorage`：全域最多 256 筆、
  每個 page/action/kind/path 序列最多 32 筆，且不保存輸入值、query、IP、User-Agent、Cookie
  或帳號。`/traffic/` 可按頁面／類型查看 latest、p50、p95、max、成功率及中位數主要耗時，
  也可複製 JSON 或只清除此瀏覽器的紀錄；伺服器不接收這些瀏覽器資料。
- 使用者事件的 `durationMs` 從事件 listener 收到到兩次 rAF，`inputDelayMs` 是事件 timestamp
  到 listener 的佇列時間；API 的 `durationMs` 包含取得本文與 JSON parse，`paintMs` 才延伸到
  兩次 rAF。兩次 rAF 只是繪製機會，不是螢幕像素已完成，也不是交易執行延遲。
- `Server-Timing` 的 `app` 是回應標頭前的應用耗時，`cache_wait` 是同步 cache-key 等待，
  `build` 是最外層同步 JSON 建置，`cache` 是固定結果名稱。背景刷新不冒充本次建置；
  各欄可能包含／重疊，不能全部相加。每個 keep-alive 請求都重設，不混入上一筆。
- 公開閘道另將每筆完成請求拆成互斥的 `cache_wait`、`build`、`write` 與 `other`；總和近似從
  handler 開始到 body 寫完的 wall time。每個階段與 allowlist 路由都保存 count、sum、max 及
  固定 histogram，因此 `/traffic/api/history?range=1h|24h|7d|30d|90d` 可比較 p50／p95／p99。
  分位數不是可加總量；找總延遲來源先比較各階段平均，再回到該路由的冷／熱實測。
- 持久監測只在 request path 更新有界記憶體計數；背景 writer 每分鐘將匿名聚合 append 到
  `/var/lib/stockagent-public-dashboards/performance/YYYY-MM-DD.jsonl` 並 fsync。systemd
  `StateDirectory` 以 0700 建立目錄、檔案為 0600；不保存 query、IP、User-Agent、Cookie、帳號、
  payload 或輸入內容。佇列丟棄及寫入錯誤必須顯示為 degraded，不能用記憶體成功冒充持久化成功。
- 程序內只保留兩天分鐘列與 90 天小時 rollup；7／30／90 天查詢不反覆展開十萬個 Python row。
  第一個不足一小時的邊界從對應日 JSONL 精確讀取，其餘合併 hour rollup。歷史 API 另有 55 秒
  single-flight response cache，避免無應用層 rate limit 時被相同長區間查詢拖垮。
- 每個存活分鐘都有 heartbeat aggregate；服務停止期間沒有 row。UI 以 coverage 顯示實際觀測分鐘，
  trend 遇未觀測 bucket 中斷折線，不得把缺口補成零流量或零延遲。服務正常重啟會先 flush 當前分鐘，
  新程序以不同 process key 接續；同分鐘各程序區段可加總，coverage 仍只算一分鐘。
- 瀏覽器頁面載入、互動、API、JSON、DOM／Canvas 與繪製機會仍只存在該瀏覽器的有界 localStorage，
  可在流量頁查看或手動複製 JSON。公開閘道維持 GET／HEAD-only，不為集中 RUM 新增寫入端點；因此
  90 天全站歷史是伺服器與路由真實樣本，不冒充所有訪客裝置的像素呈現時間。
- 本地 receipt 的熱快取以 dev/inode/size/mtime_ns/ctime_ns 檢查，不反覆讀取整份檔案。
  變動時才計算 digest；讀取前後核對 descriptor 與 pathname，避免原子替換競態。
  這是本機 Linux 檔案變動偵測，不是冷庫完整性證明，也不承諾任意遠端檔案系統語意。
- 完整分鐘曲線在同一份投影內共用時間轉換，沿用已排序順序；不刪分鐘、不改報酬或品質旗標。
- 當沖完成交易日的 09:01 與 09:02–13:29 必須逐模式驗證最新 append-only mark 的
  分鐘價格來源；270 個時間戳齊全不代表估值有效。盤中 Shioaji 斷線留下的 stale mark
  保持明示，不得標成即時可成交報價。盤後由分鐘曲線 timer 使用本地已驗證 K 線優先、
  缺口才查歷史來源；僅在來源完整、原始 fills 雜湊不變且 13:30 已接受端點保留時，
  將 09:01 改為完成分鐘 Close 的反事實估值。當日新建部位若缺少 09:01 bar，
  維持 waiting/failed；跨日舊部位可沿用先前已觀測成交價，但必須標為 stale。
  不以開盤價、未來價格或插值冒充。
- `benchmark_history.json` 保留 canonical 相容來源；其重建器同步發布
  `benchmark_history_projection/v1/{head.json,delta.jsonl,shards/...}`。日分片不可變且以內容 hash
  定址，head 原子切換，delta 只追加實際改變的 session。dashboard 必須逐 shard 驗證 schema、日期、
  筆數、路徑與 SHA-256；companion 缺漏或不符時回退 canonical JSON，不得把快取當真值。
- all-history final projection 失效時，`history-session-projection-v3` 以策略/live ledger session span
  digest、benchmark session digest、origin、product 與 schema 組合 fingerprint。未變日直接合併；少量
  變更只重建該日，超過安全界線則完整重建。任何快路徑輸出都須和 full-source control 點數及 hash
  相同。
- gateway 的 history 回應快取已保存序列化本文與 gzip 表示，因此公開 history 建置不得再把
  同一份大型 decoded Python object 留在 builder cache。直接使用 builder 的私有服務仍可選擇
  程序內物件快取；兩層責任不能同時保留同一份完整歷史。
- 瀏覽器收到 `minute_columns_v1` 後只保留解碼後的列資料；LRU 不得同時強引用欄式本文與列式副本。
  這是記憶體去重，不是刪除分鐘，點數與品質旗標必須保持一致。
- 全資料監控首屏只取得摘要及實體群組；逐來源明細在來源清冊接近 viewport
  時才讀取。摘要／明細仍來自同一份公開 snapshot，延後傳輸不能省略資料或改變完整度判定。
- `features` 是另一份由同一輪背景 snapshot 產生的唯讀欄位清冊，接近欄位區塊才載入，
  依市場、供應商、實體資料集分組，臺灣資料在前、加密貨幣最後；瀏覽器只逐頁建立欄位 DOM。
  每個實體資料集 × 欄位獨立列出名稱、型別、含欄位檔案數與實際非空值筆數。
  `non_null_count` 只在該資料集所有檔案已核實，且每個含欄位檔案的 Parquet row-group
  `null_count` 皆完整時顯示；否則只顯示已驗證下界。多型別顯示 schema drift。
  此處的 `dataset_first`／`dataset_last` 是整表時間界限，不是欄位首末有效值，
  更不是 point-in-time 發布或訓練可用時間。非 Parquet 原始格式仍由來源清冊顯示未核實，
  不可冒稱逐欄位已清點。台股公開資料訓練特徵、TWSE／TPEx 個股特徵、永豐個股補充
  特徵、期貨 v4 模型特徵與 Binance／OKX／Bybit 日資料特徵為獨立衍生家族，
  不加進原始群組筆數；舊版根目錄行情特徵獨立標示，避免冒稱與 1 分 K 去重。
  背景清冊在來源 footer 與分類定義未變時重用欄位快照，不每 30 秒重算所有欄位。
- 資料監控的 `record_stats` 是另一個證據域：最早／最新取實體 Parquet 時間欄位的
  row-group footer min/max，總筆數取所有選定實體檔案的 footer `num_rows`；不得把
  `start_date` 下載目標、`end_date` 回執日期或本次 `rows_this_run` 冒充實存首末筆／總筆數。
  30 秒背景 snapshot 每次最多重新讀取 4096 個新增或變動 footer，依檔案大小與 mtime
  快取未變檔；HTTP 只讀已完成的本機清冊。掃描未完成或 Parquet 損毀時總筆數保持
  未知；若列數可核實但檔案缺時間欄位／footer 統計，僅時間界限未知，不抹掉已核實列數。
  面板列出選定檔案、核實檔案及異常數；憑證閘門與來源根本不提供的粒度標不適用。
  台股官方資料逐資料集、台股研究分鐘資料按
  `trade_date` 唯一檔、台股微結構按交易日、三家交易所 1 分 K 與 Coin Metrics 按標的檔、
  Frankfurter 外匯按幣對檔、Yahoo 按台股／海外股票／加密／外匯分類。Shioaji 原始重疊
  回補 chunks 不加總。TAIFEX 日資料、期權全鏈、逐筆、OpenBB 端點封存、Dune、SEC／ETF、
  FRED、CFTC、Pepperstone 與免費公開觀測主表另有明確實體家族；衍生視圖各自顯示但不加進母群組。
  未涵蓋的原始分片、非 Parquet 及其他未建立無重疊主表契約的來源，必須顯示未核實，
  不能由批次回執列數填補。來源列與母群組是同一份實體資料，不能重複相加。
  有明確時區的實存時間轉為 UTC 比較；無時區欄位保留來源牆上時間（可能為臺灣本地時間），
  `snapshot_ts_ns` 明確以 Unix 奈秒解為 UTC。跨不同時間欄位的群組只比較日界限。
  實存時間不等於官方發布時刻或模型可用時間；不能把未附時區的欄位強制轉成瀏覽器時區。
  Binance／OKX／Bybit 的 1 分 K 若實存最新列落後於摘要的要求截止日或六小時新鮮度窗，
  群組與 1 分 K 產品列不可宣告已到最新；`requested_data_through` 保留原摘要供比對，
  `data_through` 改呈實存尾端，不覆蓋更強的 blocked／degraded 狀態。
- 全資料明細第一次只建立筆電 25 列／手機 10 列，其餘列仍可用「載入更多」取得；穩定排序鍵只在新
  details revision 計算一次，先依市場（臺灣優先、加密最後）與供應商，分類內依作業狀態排序；
  篩選不重排全部資料。欄位清冊第一次建立桌面 80 列／手機 25 列，其餘逐頁載入。details 回應不含 `groups` 時不得清掉已由
  summary 顯示的群組。
- TAIFEX 歷史服務將七個合法 range 的完整 display projection 原子保存於本機 cache；程序
  冷啟動可先回傳最後一份已通過 schema、range、mark-limit 及欄位白名單的快照，再以背景
  工作刷新。來源時間仍使用快照原值，不把 disk restore 冒充新資料；記憶體只保留最近三個
  range。公開閘道對大型 history 必須先固定欄位投影，再對異常巢狀值做敏感欄位清洗，不能
  先遞迴掃描即將丟棄的欄位。
- 一秒時鐘及週期刷新使用共用 visibility-aware scheduler；隱藏分頁不得持續做純呈現更新。
- 筆電驗收同時檢查文件與內部表格溢出，不能只看 document 寬度。事件表重用 compact-table，
  以換行保留六欄，不以隱藏／截斷或縮成不可讀字體換取窄版。

重測入口：`scripts/benchmark_dashboard_latency.py`（32 條路由、原始樣本、p50/p95/p99、
first/new-session/warm-reuse、吞吐、JSON 正確性、隔離 SSE、可選原始 history profiler），
以及 `scripts/audit_public_dashboards_browser.mjs`（八頁、Resource Timing、long tasks、
更新按鈕、無重載、截圖、API／版面錯誤）。測速工具遇錯仍留 receipt，HTTP／瀏覽器驗收失敗
回傳非零。HTTP timeout 是 connect/read inactivity timeout，不是單次完整下載的硬期限。

history profiler 的兩個 source 邊界不得混稱：

```bash
# final cache 失效，但允許驗證過的 immutable session projections
run_fintech_python scripts/benchmark_dashboard_latency.py \
  --profile-history artifacts/live/tw_day_trade_simulation \
  --profile-history-source-rebuild

# 同時 bypass session projections，保留真正完整 canonical scan control
run_fintech_python scripts/benchmark_dashboard_latency.py \
  --profile-history artifacts/live/tw_day_trade_simulation \
  --profile-history-source-rebuild \
  --profile-history-full-source-rebuild
```

數值、證據限制與可直接重跑的命令見
[`WEB_PROJECT_REVIEW_2026-09-12.md`](WEB_PROJECT_REVIEW_2026-09-12.md)。

### 2026-09-09 即時通知與關鍵畫面

- 當沖及共用隔日沖前端透過同源 `api/updates` SSE 接收唯讀 revision。
  公開服務僅監看既有 receipt：Linux inotify 察覺原子替換後喚醒所有讀者，
  不呼叫交易引擎、不送單、不下載，也不重啟其他服務。
- 全服務最多 64 條更新連線、一個檔案 watcher；每 topic 僅保存最新 generation，
  不累積逐客戶事件佇列。慢客戶寫入逾時 5 秒；15 秒心跳，隱藏分頁關閉串流。
  缺 inotify 時改用 250 ms 檔案檢查；串流失敗時前端每秒輪詢，正常時每 15 秒對帳。
- 通知不是資料本身。引擎 revision 改變，以及該 revision 的公開 status 快取完成，
  都會通知。前端以「已套用 status」而非「已收到通知」判斷進度，避免
  stale-while-rebuild 回傳舊畫面後就吞掉新版。來源缺口與等待狀態仍原樣顯示。
- history/signals/positions/events 快取鍵含內容 revision；前端分鐘快取也包含
  已套用 revision，不能為等待 45/55 秒 TTL 而漏顯新訊號或新分鐘。
- 訊號請求先送出，分鐘歷史隨即獨立載入；兩者不互相 await。次要明細等主畫面
  後才載入。日期改變時過期 status 回應不得覆寫新篩選。
- 分鐘欄位只解碼一次；未變更曲線不重建 SVG，共用時間座標每分鐘只計算一次。
  保留全部既有分鐘點、缺口與品質旗標，不以下採樣、插值或偽造價格換速度。

測量腳本、冷熱快取限制及正式 HTTPS 驗收見
[`WEB_LATENCY_2026-09-09.md`](WEB_LATENCY_2026-09-09.md)。

資料是一分鐘更新一次的頁面，不應靠整頁 reload。背景更新取得 JSON 後，只更新變動區塊：

1. `dashboard-core.js` 的 `setText` 與 `setTrustedHtml` 會跳過內容相同的 DOM 寫入。
2. `createLatestRequest` 保證範圍或日期快速切換時只有最新回應可以提交；舊請求會被取消，不能覆蓋新狀態。
3. 首屏狀態與重型歷史／明細分開載入，重型曲線不能阻塞首屏。
4. 前端短期快取以完整查詢鍵區分日期與範圍，且必須有明確筆數上限。
5. 後端同一 cache key 只允許一個建置者；其他請求共用結果。過期但仍在 grace 期間的結果可立即回傳並在背景刷新。
6. JSON 回應快取同時受 `MAX_CACHE_ENTRIES` 與 `MAX_CACHE_BYTES` 約束。容量計算含原始與 gzip body；淘汰採最近最少使用順序。
   單筆超過容量的回應可傳給當前請求，但不常駐快取。正在建置／等待的 key lock
   與快取 entry 分開管理；淘汰不能替換等待者正在使用的鎖，失敗查詢也不能永久累積鎖。
7. 帶版本 query 的靜態資源可長期 immutable cache。任何 JS/CSS 行為變更都必須同步提高 HTML 中的版本號。

快取只改善重複投影與傳輸，不能改變資料真值。快取鍵必須包含所有會影響輸出的日期、範圍、篩選、分頁與來源 revision。

當沖曲線預設前端選用 `resolution=1m&encoding=v2`，以一份共同 minute axis、每序列 indexes 與平行
數值／品質欄傳輸全部分鐘；`minute_columns_v1` 僅保留相容。不得把長區間的抽樣點稱為完整分鐘。
日期區間、期間歸零基準與完整點數須一起驗證；
大範圍極值計算不得使用 `Math.min(...points)` 等有參數數量上限的做法。
已回補的 `benchmark_history.json` 同分鐘優先於舊 `benchmark_marks.jsonl`，
即時帳本只能補尚未回補的分鐘，避免歷史 Close 與 Bid/Ask 因讀檔順序混接。

### 09:00 當沖延遲契約

`artifacts/live/tw_day_trade_simulation/opening_signal_latency.jsonl` 是每日開盤訊號的
小型 append-only 測速帳本。每個排程嘗試都須留下成功或失敗終態；成功列分別記錄排程喚醒、
漏失盤前補備、即時狀態準備、模型鎖等待、行情請求、推論輸入、模型推論、格式化與原子發布。
共享報價程序另記 request queue、provider、snapshot serialization 與 client round trip。

- 跨程序的 09:00、行情到達與 signal-ready 使用帶時區牆鐘；單一程序內的階段耗時使用
  monotonic clock。
- `PriceSnapshot.exchange_timestamps_ms` 是市場牆鐘，`timestamps_ms` 才是本機 callback
  收到資料的因果時間；兩者不得互換。行情覆蓋時間不是交易所 RTT 或券商回報時間。
- 目標 `1,000 ms` 同時呈現「第一個訊號」與「全部啟用模式」；不得因少跑模式就宣稱達標。
  `source_ready_from_open_ms` 與 `source_ready_to_signal_ms` 分開，避免把資料商尚未送達的時間
  誤判成模型運算。
- 當沖開盤路徑必須 `ensure_previous_signal=false` 且
  `previous_signal_backfill_limit=0`。當日帳戶從空倉開始，遞迴補生前幾日訊號不改變今日目標，
  只會增加延遲；測速列會保存這個守門結果。
- 公開頁仍為唯讀，不接受瀏覽器回寫測速。前端顯示本次 API 請求時間；伺服器每日趨勢以
  immutable signal-ready/published 與模擬帳本時間為準，不把某位訪客何時打開頁面混入策略 SLA。

重啟後，狀態 API 在完整帳本索引尚未就緒時只讀目前 state 與最新連續交易日尾段；
`STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR` 的完整日期、行數及 benchmark-history 索引在背景建立。
索引未完成時 `record_counts_ready=false` 且累積筆數為 `null`，不得顯示成零或同步掃描數 GiB
帳本來阻塞即時頁面。

## 台股策略身分與明細元件

`modes[].market` 是不可變的帳戶／帳本關聯鍵，`modes[].label` 才是目前部署策略的顯示名稱。
舊帳戶鍵包含 `gelu` 不代表還在執行 GELU：模型身分仍須用 selector、checkpoint
fingerprint 和 replay promotion receipt 驗證。不得為了改畫面名稱重命名帳本、
訂閱、signal key 或曲線 series ID，也不得在 JavaScript 寫策略別名對照表。

| 元件 | 責任 |
|---|---|
| `dashboard-core.js` | 同源請求、取消、排程、安全跳脫、避免重複 DOM 更新 |
| `tw-day-trade/app.js` | 篩選與請求狀態、最新請求優先、圖表與元件組裝 |
| `presentation.js` | 從 API 模式名稱解析顯示名稱、成交規則字典、期貨三態語意 |
| `detail-components.js` | 訊號／持倉／成交列、期貨徽章、共用分頁及錯誤／載入狀態 |
| `tw_stock_futures_catalog.py` | 完整官方名單驗證、快取索引、證券代號精確關聯 |

所有模式卡、篩選、曲線標籤、明細、feature 與稽核區必須共用名稱解析；
缺少名稱顯示「策略名稱未提供」，不得把舊 machine ID 當策略名稱。
改動名稱須納入 render revision；不需要重新載入整頁或重啟交易引擎。

「所有訊號」的期貨標記採用[期交所標的名單](https://www.taifex.com.tw/cht/2/stockLists)，
依 `證券代號` 與明確的「是否為股票期貨標的」欄位關聯，不能把選擇權、商品名稱相似
或歷史產品曾存在推論成現在有股票期貨。顯示「有期貨／無期貨（名單未列）／期貨未確認」、
官方商品代碼及名單取得日期。這是**最新名單註記，不是歷史訊號日期的資格或下單保證**，
不進入模型特徵、交易遮罩或回測成交判斷。

既有 `stockagent-taifex-futures-daily.timer` 在正式 daily panel 建置完成後，
透過 `download_taifex_contract_codes.py --stock-futures-only` 更新此名單；
名單更新失敗可見於維護狀態，但不會阻止正式 panel 建置。只更新輕量名單可執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/download_taifex_contract_codes.py --stock-futures-only
```

來源保存於 catalog-resolved `data_tw_futures/taifex_stock_futures_catalog.json`，
HTML 原文按 SHA256 保存在同層 `stock_futures_catalog_sources/`。
下載器須驗證 schema、唯一代碼、官方總計與異常縮減，再原子發布。
網站只讀本地、核對 rows hash；缺檔、hash 不符、未完成、時間超前或超過三天均顯示未確認，
不把失敗當成「無期貨」。只豐富當前分頁，不逐股票連線外部 API；名單版本／過期狀態
是訊號快取鍵的一部分。更新後至多等待公開 API 快取週期，不需重啟服務。

元件行為測試：`node --test test/test_tw_day_trade_components.mjs`；
名單／分頁／公開 DTO 測試：`run_fintech_python -m pytest test/test_tw_stock_futures_catalog.py`。

## 安全契約

- 瀏覽器請求限制為同源 HTTP(S)，不接受任意外部 URL。
- 公開資料使用明確欄位投影；禁止公開帳密、token、cookie、內網位置、任意檔案路徑或環境變數。
  既有 status 投影另以遞迴 credential-key guard 防止巢狀 API key／帳戶識別外洩，
  同時保留非秘密的 `revision_token`。這是額外防護，不代表任意自由文字可安全公開；
  新 DTO 欄位仍須逐一審查。
- 來源字串進 HTML 前必須經 `Dashboard.escapeHtml`。`setTrustedHtml` 只負責避免相同 markup 重寫，不是 sanitizer。
- 使用文字 DOM API 建立表格時優先指定 `textContent`；不要把來源字串直接串入 `innerHTML`。
- 公開服務保留 systemd 的 localhost 網路限制、唯讀檔案系統、隱藏 secrets、空 capabilities 與 `NoNewPrivileges`。
- 非 GET/HEAD 方法必須拒絕，未知路由與無效 query 必須 fail closed。
  不接受參數的 API 不可靜默忽略 query。GET/HEAD 不支援 request body／chunked input；
  拒絕本文後必須關閉連線，不得將尚未消費的本文解析成下一個 keep-alive request。
- gzip 必須遵守 `Accept-Encoding` 權重及 `q=0`，無可接受表示時回覆 406。
  identity/gzip 各有符合實際 bytes 的強 ETag；gzip mtime 固定以維持同內容可重現。
  兩種表示與 304 均攜帶 `Vary: Accept-Encoding`；GET/HEAD conditional request
  支援 weak comparison、ETag list 與 `*`。304 不傳輸本文，也不附錯誤的零長度表示。
  規範依據：[RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.8.3.3)。

## 修改與驗收清單

每次修改至少完成以下檢查：

1. `git status --short`，確認沒有覆蓋無關的既有修改。
2. 對所有前端 JS 執行 `node --check`。
3. 執行公開閘道、對應資料建置器與頁面 shell 測試。
4. 驗證同一冷 cache key 的併發請求只建置一次、回應快取不超過筆數／bytes 上限，範圍切換只有最新請求可提交。
5. 以 GET 驗證八個頁面與所有公開 API；檢查 status、Content-Type、gzip、ETag、CSP 及敏感欄位掃描。
6. 重啟前記錄交易引擎 PID／restart count，只重啟公開面板服務；部署後證明交易引擎 PID 未變。
7. 分別量測冷請求、熱快取、本機閘道及公網端到端時間。網路／TLS 與應用建置時間要分開報告，不能用熱快取數字冒充冷路徑。
8. 檢查本次啟動後 journal 無 traceback、fatal、watchdog 或持續重試。

沒有瀏覽器渲染工具時，只能宣稱靜態 shell 與 API 驗收通過；不得宣稱已完成像素或互動視覺驗收。
