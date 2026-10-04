# 抓取面板資訊層級與一致版型

## 目標與現況

使用者要求：重要、少量且人類易懂的資訊在前；完整清冊與大量技術資訊在後；
各來源使用一致版型，只保留機制必要差異。這次只改呈現，不改下載來源、配額、
排程、資料語意、交易或訓練。

2026-10-03 本機唯讀盤點：全資料 summary 有 156 個來源標籤，使用同一個
provider 模板。來源標籤包括組合／別名，不代表 156 個獨立帳號。專用下載頁為
永豐、FinLab、FinMind、TEJ、OpenBB。全資料入口、所有 provider 子頁、公開首頁
與網站流量工具也納入。`/taifex/` 是策略模擬頁，不是期交所下載器；期交所抓取
資訊在全資料的 TAIFEX provider 頁。交易頁不變更交易資訊順序，但驗證共用樣式
沒有造成回歸。

現有問題：FinMind 清冊、流量曲線及容量在進度前；FinLab／TEJ 先展開大量估時
與工作量欄位；跨頁缺少統一的少量摘要；同一份資訊可散落幾屏之後。窄螢幕的
多子節點表格儲存格可能錯位。不能靠刪資料、假合計或縮小字體解決。

## 已決定的完整改版方案

1. 共用呈現層：沿用既有 HTML、唯讀 API、頁面 renderer 和導航。新增一個薄的
   acquisition 模組及樣式；依頁面設定讀取已渲染欄位，不新增 provider/API 查詢，
   不複製下載架構或重新計算既有進度。
2. 統一首頁順序：精簡標題與健康／觀測時間 → 真實警示 → 四張摘要卡與一條有
   明確分母的進度（來源有提供才顯示）→ 下一動作／排程 → 可展開明細。
3. 統一明細：進度與排程、配額與流量、資料清冊、儲存與交付、統計口徑。
   保留原節點、ID、所有行、搜尋、分頁、篩選及既有深連結。原跳轉須自動打開
   對應區塊；重新取樣不能關閉使用者已打開的區塊或重設選擇。
4. 首頁以全資料及抓取入口優先；策略模擬另列，保留原連結、狀態與語意。
   全資料頁保留台股優先、加密貨幣在後、完整來源／特徵清冊。
5. 效能：只移動既有 DOM 一次，少量摘要按指定節點變更合併更新。摺疊的全資料
   明細繼續按需載入，不因隱藏節點的零尺寸誤觸發；沒有額外輪詢／全表掃描。
6. 響應式與可用性：一致留白、字級、卡片、提示、頁內導覽、原生 disclosure。
   320px 手機至桌機保持全部資訊可讀；鍵盤可展開，深連結可定位；不增加第三方
   字型、圖示 CDN 或外部資源。

## 每頁摘要與不可混用的口徑

| 頁面 | 首要資訊 | 保留的來源差異 |
| --- | --- | --- |
| 永豐 | 即時擷取狀態、期貨 tick 回補、當日流量、該範圍估時 | 訂閱／請求／流量不同；期貨 tick 進度不是全部股票歷史 |
| FinLab | 執行中資料鍵、估計下載量進度、當日用量、全域估時 | byte 分母是估計；取得不等於冷庫發布／訓練就緒 |
| FinMind | 執行中工作、候選搜尋進度、最近 60 分鐘本站請求、全域估時 | 候選不是確定缺筆；本站請求不等於帳號全用量；美股分鐘最後 |
| TEJ | 桌面工作、目標範圍查驗進度、桌面操作、條件式估時 | 桌面操作不是 HTTP call；空回查驗與有數值分開；未排程不能承諾時間 |
| OpenBB | 已接受目標進度、未解決工作、已取得筆數、最近吞吐 | 不能把 unavailable 算下載成功，也不能合計不同 provider 配額 |
| 全資料 | 追新／回補、串流、當期完成、需處理端點 | 完成只對本輪端點目標；跨來源不能硬合計 ETA／請求／容量 |
| 所有 provider 子頁 | 回補端點、需處理端點、活躍端點、來源配額 | 各來源不同機制或沒有觀測則明確標示，不偽造全域估時 |
| 網站流量 | 網站請求、RPS、延遲、所選期間錯誤 | gateway 流量不是 provider 配額，不共用分母 |

## 驗收順序

1. 保存改版前主要頁首屏與 DOM／API 請求量，確認資料來源與節點。
2. 元件／靜態契約測試：頁面覆蓋、原 ID 保留、未知進度、摘要相等、警示、
   原生展開、深連結、使用者狀態保留、無新增 fetch／poll。
3. 本機與公開 gateway 實際瀏覽器：全專用頁、全資料、provider 代表機制、
   首頁及網站流量；手機與桌機；156 個註冊 provider 路徑／共同模板驗證。
4. 既有跨頁 CDP／響應式稽核及受影響 Python／Node 測試；查看實際截圖，
   記錄溢出、主控台、API 失敗、首屏、明細與控制項行為，不以 HTTP 200 代替。
5. 只在新增靜態資產路由需要時重載唯讀公開 gateway；不重啟下載器與交易服務。
   驗收收據記錄資產版本、範圍、測試結果與未解決事項，再完成原 task。

工程驗收與來源資料完整性分開；下節只證明本次版型、操作與部署。

## 實作與驗收結果

### 已實作並部署

共用資產為 [dashboard-acquisition.js](../services/public_dashboards/dashboard-acquisition.js)
`v2` 與 [dashboard-acquisition.css](../services/public_dashboards/dashboard-acquisition.css)
`v3`；全資料 app 使用 `v47`。公開 HTTPS 資產 SHA-256 與本機檔案一致，詳見
[最終收據](../artifacts/operations/acquisition-pages-hierarchy-20261003/FINAL_RECEIPT.json)。

所有抓取頁使用相同標題、摘要卡、進度、頁內導覽與原生展開區。真實警示不藏入
清冊；原來源的狀態、數值、進度分母與未知值直接映射，不重新推算成另一個
「完成率」。大量欄位移到後方，搜尋、分頁、篩選、所有行與原 ID／事件保留。
首頁先列抓取入口，再列策略模擬。各來源的配額單位、串流、估時限制與交付狀態
仍依上表保留，沒有把不相容的機制硬套成相同數值。

摘要只監測指定欄位，以單次 animation frame 合併更新，未變更的值不重寫；
共用模組沒有 fetch、額外計時輪詢或全表掃描。全資料清冊保持按需載入，包含
沒有 IntersectionObserver 的相容路徑。這降低首屏呈現及重複 DOM 更新工作，
不是縮減原始資料，也不代表已測得下載器 CPU／GPU 或吞吐改善。

### 邊做邊驗證所修正的問題

- 深連結及原控制項可能落在收合區：統一先打開所有祖先，保留既有連結；防止
  較早排入的捲動覆蓋新的使用者跳轉。
- 展開後的 `content-visibility` 預估高度使錨點偏離：展開內容使用實際高度。
- 晚到的 provider 配額連結在收合區留下重疊測量框：明確隱藏未展開的內容，
  並加入 1920px、2560px 回歸測試。
- 隱藏節點可能被按需載入誤判為進入畫面：檢查實際矩形及收合祖先，支援原
  程式化跳轉，避免首屏就載入全部來源與特徵清冊。
- 手機儲存格有多個子節點時錯位：值固定在同一欄，不刪資料欄位。
- 驗收不能只等 API 標頭或 HTTP 200：等待實際狀態、清冊行與特徵 DOM；摘要
  和原欄位在同一時點比對。公開 CSP 的 `unsafe-eval` 仍禁止，改正稽核 predicate，
  沒有放寬安全政策。

### 驗收結果與證據

| 範圍 | 結果與收據 |
| --- | --- |
| 本機實際 UI | [28 案例通過](../artifacts/operations/acquisition-pages-hierarchy-20261003/final-local/acceptance.json)：9 版型 × 桌面／390px／320px，另加無 IntersectionObserver 相容路徑 |
| 響應式矩陣 | [99 案例通過](../artifacts/operations/acquisition-pages-hierarchy-20261003/final-local/responsive/responsive-audit.json)：9 版型 × 11 尺寸；320px 至 2560px，含橫向、HiDPI、超寬桌面 |
| 公開 HTTPS | [49 案例通過](../artifacts/operations/acquisition-pages-hierarchy-20261003/public-v3/acceptance.json)：9 版型與 7 種 provider 代表 × 3 尺寸，另加相容路徑；主控台錯誤、API 失敗、外部資源請求均為 0 |
| 全 provider 路徑 | [逐一清點的 156 個桌面案例通過](../artifacts/operations/acquisition-pages-hierarchy-20261003/preview-4/acceptance.json)：只取 `kind=provider` 且沒有 `profile` 的列；來源標籤不是獨立 API 帳號數 |
| 受影響 gateway／頁面回歸 | [327 passed](../artifacts/operations/agent-workflow/runs/gateway-regression-20261003T013204-cd2fde56/run.log) |
| 終版共同版型契約 | [29 passed](../artifacts/operations/agent-workflow/runs/layout-contracts-20261003T013417-ed1bd96f/run.log)，含兩個晚到配額連結回歸；與 327 有重疊，不相加 |
| 共用前端 | Node 33 passed；`npm run check:dashboard-types`、JS 語法與受影響 Python 編譯檢查通過 |

156 個路徑的逐一清點收據是較早的共同模板版本；該收據整體仍為失敗，原因含
4 個稽核 predicate 不相容及寬螢幕重疊框，不能宣稱整份通過。這些問題已修正，
終版由上述 28／99／49 案例與 29 個共同契約重新驗收；歷次失敗證據保留。

可用性檢查包含摘要與原數值／進度一致、唯一 ID、鍵盤展開、深連結、搜尋與
分頁、展開狀態保留、按需清冊／特徵載入及無額外外部查詢。所有 99 個響應式
案例的溢出、控制項遮蔽／重疊、導航錯誤、主控台及 API 失敗檢查均為 0。
另已人工查看改版前後桌面及手機截圖，不只依靠尺寸數字。

一次 1366×768 觀測中，FinMind 預設頁高由約 43,505px 降至 1,205px；這是大量
明細預設收合後的呈現差異，不是刪除資料、減少下載量或 CPU 效能基準。首頁
因拆開來源與策略入口，頁高不一定減少；不把所有頁變短當作成功條件。

### 部署與未變更邊界

只有 `stockagent-public-dashboards.service` 為載入兩個靜態路由重啟一次，使用
`--job-mode=ignore-dependencies` 避免連帶啟動 Wants。終版 gateway PID 為 1173185；
TAIFEX 策略頁、當沖、隔夜及 FinMind complement 的 PID 仍分別為 196、1551092、
764、1113610，與本次改版前一致。沒有重啟下載器與交易服務，沒有改排程、配額、
下載來源、資料、交易策略或額外呼叫官方 API。

網站仍會顯示來源真實的失敗、等待重試或不完整，不能因版型驗收通過就改成成功。
本次沒有宣稱所有資料已抓完、官方歷史完整或冷庫／訓練交付已完成。

### 可重跑的唯讀驗收

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/verify_acquisition_dashboards.py \
  --base-url https://penguin72487.ddnsgeek.com \
  --provider-examples --output-dir artifacts/operations/acquisition-ui-recheck

run_fintech_python -m pytest -q -s test/test_acquisition_layout.py
node --test test/test_dashboard_core.mjs test/test_public_overview.mjs
npm run check:dashboard-types
```

需要逐一清點來源可加 `--all-providers`。要跑完整響應式矩陣，在 UI 命令加
`--cdp-port 9347 --responsive-audit --responsive-pages
overview,shioaji,finlab,finmind,tej,openbb,data-monitor,data-provider,traffic`；選擇未占用的
CDP port。這些命令查唯讀 gateway，不使用金鑰或直接抓供應商原始資料。
