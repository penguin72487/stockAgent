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
7. 帶版本 query 的靜態資源可長期 immutable cache。任何 JS/CSS 行為變更都必須同步提高 HTML 中的版本號。

快取只改善重複投影與傳輸，不能改變資料真值。快取鍵必須包含所有會影響輸出的日期、範圍、篩選、分頁與來源 revision。

## 安全契約

- 瀏覽器請求限制為同源 HTTP(S)，不接受任意外部 URL。
- 公開資料使用明確欄位投影；禁止公開帳密、token、cookie、內網位置、任意檔案路徑或環境變數。
- 來源字串進 HTML 前必須經 `Dashboard.escapeHtml`。`setTrustedHtml` 只負責避免相同 markup 重寫，不是 sanitizer。
- 使用文字 DOM API 建立表格時優先指定 `textContent`；不要把來源字串直接串入 `innerHTML`。
- 公開服務保留 systemd 的 localhost 網路限制、唯讀檔案系統、隱藏 secrets、空 capabilities 與 `NoNewPrivileges`。
- 非 GET/HEAD 方法必須拒絕，未知路由與無效 query 必須 fail closed。

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
