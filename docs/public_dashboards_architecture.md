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
七個專用頁面 renderer
```

公開閘道是唯讀檢視層，不是交易控制面。它不得送單、修改帳本、啟動訓練、觸發下載或將反事實回放改稱即時成交。面板部署只能重啟 `stockagent-public-dashboards.service`；不得為了更新 UI 重啟當沖模擬或交易引擎。

## 頁面與資料責任

| 頁面 | 主要 API | 真值責任 |
|---|---|---|
| `/` | `/api/overview` | 各面板的低成本可用性摘要 |
| `/taifex/` | `/taifex/api/status`, `/taifex/api/history` | TAIFEX 策略狀態與歷史投影 |
| `/tw-day-trade/` | `/tw-day-trade/api/*` | 當沖狀態、分鐘曲線、訊號、持倉、事件與公開資料進度 |
| `/shioaji/` | `/shioaji/api/status` | 永豐資料流程、配額、流量與儲存量 |
| `/openbb/` | `/openbb/api/status`, `/openbb/api/history` | OpenBB 封存與歷史進度 |
| `/data-monitor/` | `/data-monitor/api/status`, `/data-monitor/api/summary` | 全資料來源的 receipt、覆蓋與 freshness |
| `/traffic/` | `/traffic/api/status` | 匿名請求延遲、吞吐、錯誤率及回應快取容量 |

新增欄位時，應先在資料建置層定義語意，再加入公開 allowlist，最後才渲染。前端不得從名稱猜測單位、時區、成交狀態或資料完整性。

## 不可破壞的正確性契約

- 缺資料必須顯示為缺口、等待、過期或部分完成；不得插值或用其他價格冒充。
- 當沖歷史回放與即時模擬是不同資料域。回放必須保留反事實標籤，不能宣稱是券商成交。
- 每分鐘曲線的絕對權益持續從初始資金累積；使用者選定期間只把顯示報酬率的第一個可見點設為 0%，不重置絕對資金。
- 0050、2330 與 TXFR1 是 Buy-and-Hold 基準；跨日報酬以同一實體部位相對前一收盤計算。TXFR1 換月價差只調整外部現金，不製造投資報酬或槓桿。
- API 時間一律附時區或明確標示 UTC；瀏覽器顯示轉為 `Asia/Taipei`。
- 所有公開 API 回應根節點必須是 JSON object。錯誤回應不得被當成正常資料繼續渲染。

## 性能與更新契約

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

當沖曲線預設前端選用 `resolution=1m`，以 `minute_columns_v1` 數字欄位傳輸全部分鐘，
不得把長區間的抽樣點稱為完整分鐘。日期區間、期間歸零基準與完整點數須一起驗證；
大範圍極值計算不得使用 `Math.min(...points)` 等有參數數量上限的做法。
已回補的 `benchmark_history.json` 同分鐘優先於舊 `benchmark_marks.jsonl`，
即時帳本只能補尚未回補的分鐘，避免歷史 Close 與 Bid/Ask 因讀檔順序混接。

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
5. 以 GET 驗證七個頁面與所有公開 API；檢查 status、Content-Type、gzip、ETag、CSP 及敏感欄位掃描。
6. 重啟前記錄交易引擎 PID／restart count，只重啟公開面板服務；部署後證明交易引擎 PID 未變。
7. 分別量測冷請求、熱快取、本機閘道及公網端到端時間。網路／TLS 與應用建置時間要分開報告，不能用熱快取數字冒充冷路徑。
8. 檢查本次啟動後 journal 無 traceback、fatal、watchdog 或持續重試。

沒有瀏覽器渲染工具時，只能宣稱靜態 shell 與 API 驗收通過；不得宣稱已完成像素或互動視覺驗收。
