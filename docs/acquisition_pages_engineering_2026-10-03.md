# 抓取網頁全鏈工程檢查與修正

## 目的與邊界

本次承接已完成的抓取頁資訊層級整理，從軟體工程檢查來源證據到瀏覽器的整條鏈。
先盤點、確認缺陷及完成方案，再實作、回歸、實測與部署。基線 HEAD 為
`894e782dfde5e12d1f1c6a9d94393a8bc3383ba6`，已有 237 個 dirty paths，其他工作仍在
修改檔案；不得把整份 dirty diff 當成本次修改或覆蓋既有工作。

這是抓取監控／唯讀 API 修正，不是重寫全部 SDK，也不更動下載來源、配額、交易
帳本、訓練特徵或發布日規則。`/taifex/` 是策略模擬頁；期交所抓取資料位於
TAIFEX provider 頁。共用前端核心有交易頁呼叫者，因此核心修正也要驗證既有契約。

## 從目的推導責任與一致性

```text
下載服務／SDK → 原始檔與來源收據 → 背景公開快照
                                      ↓
                allowlist projection → 有界快取／唯讀 gateway
                                      ↓
               同源請求／取消／頁面驗證 → 領域 renderer
                                      ↓
                 共用摘要與按需明細 → 使用者當前篩選與視圖
```

- 來源服務決定資料真假、發布、覆蓋及進度分母；網頁不得為了好看重算成另一種完成率。
- gateway 決定可公開欄位、快取及讀取隔離；單一來源故障不能令其他獨立來源一起消失。
- 前端決定哪一份回應能提交：必須同時符合目前請求、篩選、分頁與快照身分。
- 摘要持續更新；未展開、未進入視野的明細不需要反覆查詢／建立 DOM。重新展開
  時必須取得或渲染最後一份有效結果，不是永遠不載入。
- API 成功、下載服務活動、來源資料最新及訓練就緒是不同事實。失敗保留已成功
  觀測，但標示更新失敗；不同篩選的舊結果不得假裝是新範圍。
- 即時性由來源快照、gateway TTL、頁面刷新及網路共同決定；提高瀏覽器輪詢頻率
  不能令尚未發布的上游證據更新。本次保留來源配額與既有摘要節奏。

## 全範圍清冊與已確認問題

| 頁面／元件 | 真值與主要入口 | 本次檢查／修正 |
| --- | --- | --- |
| 首頁 | `public_overview`，十個獨立摘要 | 一個 future／本機 builder 例外會讓整個首頁 API 失敗；要逐來源隔離，未知數不能補成 0 |
| 永豐 | `build_shioaji_public_status`，本機券商收據 | 保留訂閱／請求／byte 差別，按需處理明細／曲線，共用請求與安全回歸 |
| FinLab | `build_finlab_public_status`，配額與目錄收據 | 保留 MB、估計 byte 與實存量不同口徑；收合清冊仍在重建 DOM，需延後渲染 |
| FinMind | `build_finmind_public_status`，worker/frontier/配額 | 保留候選及真實筆數、等待重試與 ETA 失效；收合的管線和流量明細需按需渲染 |
| TEJ | `build_tej_public_status`、分頁 features | 每次首次 status 後會立即查收合 features；切換搜尋到 debounce 發出前，舊請求仍可能提交 |
| OpenBB | 本機 monitor/status/history | 歷史請求失敗清掉上次曲線；收合曲線仍持續查詢；切換範圍須分開空資料與讀取失敗 |
| 全資料 | 背景 summary/details 與 feature page | 特徵回應只核對 sequence，未核對現有篩選／完整 envelope；新快照將使用者已展開來源列重設為首批 |
| 所有 provider 子頁 | 同一 `project_provider_detail`，156 來源標籤 | 分頁缺少嚴格頁面／provider 驗證；等待發布狀態在頁面與 query allowlist 不一致；配額失敗需明示保留舊值 |
| 網站流量 | gateway 匿名聚合，非供應商配額 | 舊請求錯誤可覆蓋新範圍；非 2xx 在讀本文前丟例外，未完成 shared request lifetime |
| 共用核心／摘要層 | 同源 fetch、latest-request、scheduler、少量 DOM 映射 | 重用現有 cancellation／排程，不另建 fetch 系統；新增可驗證的按需呈現能力，維持原節點和原位更新 |

## 已決定方案與實作順序

1. **故障隔離與未知值**：首頁每一個獨立讀取在原責任處隔離，公開只回安全來源
   標籤／固定錯誤碼，不公開 exception message、路徑、key 或原始 response。
   成功來源仍呈現；缺來源的計數維持未知，明確回報部分不可讀。
2. **一致性與恢復**：沿用 `createLatestRequest`，補上目前篩選與分頁 envelope
   驗證、失敗狀態和快照邊界。相同篩選失敗保留最後成功結果；換篩選不得錯置。
   同一來源清冊更新保留使用者已載入列數，不把來源等待發布當作清冊參照。
3. **共用按需呈現**：在既有核心增加小型 visibility／deferred-render helper，
   用原生展開、視野與頁面 visibility 事件觸發；隱藏時只保留最新參數，不遍歷
   全清冊／重建 SVG。摘要不延遲，明細資訊及列數不刪減。
4. **每頁接線**：先完成 TEJ／OpenBB 明細查詢按需化及各頁明細 renderer 接線，
   再驗證列表、圖表、搜尋、分頁、鍵盤與恢復時沒有漏掉資料。
5. **安全與相容性**：保留 GET/HEAD-only、同源、CSP、ETag/gzip、敏感欄位
   allowlist、query 上限及有界快取。只調整確認不一致的合法狀態；不放寬任意 URL、
   檔案路徑、憑證或權限。資產版本同步更新，避免新 app 載入舊核心。
6. **驗收與部署**：先錯誤／競態／邊界測試，再共用及領域回歸、實際瀏覽器、
   首屏與展開後的查詢／DOM 工作量、本機和 HTTPS 實測。必要時只重啟唯讀
   gateway，記錄其他服務 PID 不變；保留失敗收據，不能只挑通過案例。

## 七個工程面向的完成條件

| 面向 | 可驗證條件 |
| --- | --- |
| 可讀性 | 共用責任集中、小型具名 helper；來源機制不被重複框架覆蓋，清冊可追到實際入口 |
| 正確性 | 未知與 0 分開、讀取失敗與合法空資料分開；錯誤及不相符分頁不提交 |
| 連動性 | 原 ID、事件、篩選、分頁與摘要同步保留；展開後使用最新資料 |
| 即時性 | 摘要原節奏不變；回前景／重新展開後更新；單一來源失敗不遮蔽其他來源 |
| 一致性 | scope／request／revision 與實際頁面一致；等待發布在全資料、provider 和 query 一致 |
| 執行效率 | 沒看明細時不額外查歷史／features、不反覆建立明細；量測完整請求與 DOM，不以熱快取冒充冷路徑 |
| 安全性 | 公開 payload、錯誤文字與 telemetry 不洩漏秘密；同源、唯讀、CSP 與非法 query 回歸通過 |

## 實作與驗收結果

### 已完成的工程修正

| 責任層 | 實作結果 |
| --- | --- |
| 共用核心 | 在原 `dashboard-core.js` 加入 `observeVisibility`／`createDeferredRenderer`；展開、捲動、前景與無 IntersectionObserver 行為共用。事件每影格合併，隱藏明細只留最後一份參數，沒有新增網路或下載排程框架。 |
| 首頁 gateway | 八種獨立來源故障注入各自隔離；未知計數回 `null`，保留可讀來源。五個 concurrent peer 共用 8 秒等待預算，不因 executor 離開時的等待抵銷期限；來源上游 timeout、原快取與本機 builder 職責不變。 |
| 全資料／provider／TEJ | 回應核對 request、目前篩選、頁面 offset／limit、筆數／has-more、公開安全旗標與快照邊界。輸入立即取消舊請求，不等 debounce 才失效；被清冊移除的已選 scope 保留並註明，不默默切回全部。 |
| 全資料／provider 分頁 | 全資料刷新保留已展開 50 列；同 generation 可合併既有尾頁，不同 generation 不混用。Provider 分頁變更時刷新已載入前綴，不固定退回第一個 30 筆；仍遵守最多 1500 筆的 request 上限，不承諾無限展開保留。 |
| TEJ／OpenBB／網站流量 | 收合的 features／history 不再查詢。TEJ 表格按鈕實際展開並定位到 features；OpenBB 同範圍歷史失敗保留上次觀測，換範圍清楚區隔舊曲線。慢歷史查詢不阻塞摘要。流量非 2xx 本文完整消費與釋放，舊錯誤不能覆蓋新範圍。 |
| 領域頁面 | 永豐、FinLab、FinMind 的清冊、管線、流量／儲存明細及曲線按需呈現，原摘要節奏和來源進度口徑保留。FinLab 在入口排除不合法的 dataset item，不讓收合後延遲出現 renderer 例外。 |
| 時間與狀態 | `waiting_publication` 在投影、合法 query、篩選、文字與顏色一致。FinMind provider 帳號配額採官方觀測時間，不拿本站請求時間替代；來源讀取失敗明示保留上次用量。TEJ provider 移除沒有可呈現 quota 的重複 status 請求。 |
| 共同版面 | 首屏摘要與原有節點／事件不變；快速導覽在所有抓取頁採正常文流，修復桌面 legacy sticky 規則遮住折疊標題。共用核心資產 v14、acquisition JS v3／CSS v4，同步更新引用。 |

### 已取得的回歸與部署證據

- 最新相關 Python 測試為 **342 passed**，含 gateway、公開安全、來源投影、分頁、
  FinLab／FinMind ETA 介面、實際 DOM 與 UI 契約；不是全 repository pytest。
  收據：`dashboard-regression-20261003T023153-70aa02fe`。
- 前端 Node 語意測試 **70 passed**，涵蓋共用核心、overview、TEJ、request 競態、
  DataMonitor、既有當沖取消與 time-axis 相容性。收據：
  `request-regression-20261003T021907-5cde0c3b`。
- `npm run check:dashboard-types` 通過，但只涵蓋既有 time-axis 的型別入口，
  **不能宣稱所有前端 JavaScript 都已嚴格型別化**。
- 新版 responsive harness／版面測試 **31 passed**（與 342 測試重疊，不能相加）。
  不認識的 page name 直接失敗，`provider` 可映射 canonical `data-provider`。
- 正式 HTTPS 的 **9 個 HTML、25 個版本化 JS／CSS 資產 URL** SHA-256 與本機相符，
  HTML CSP 存在且沒有 unsafe-eval；測試只讀同源靜態檔。
  指紋：`asset-acceptance-20261003T023420-f5db1fa2` 的 run log。
- 2026-10-03 **10:21:16 Asia/Taipei** 只重啟
  `stockagent-public-dashboards.service`；10:21:30 開始 listening，之後 active、
  NRestarts=0。永豐行情／期交所策略、當沖、隔日沖、FinMind complement 的
  PID／InvocationID／NRestarts 與部署前相同，沒有重啟下載或交易程序。
- 本次 port 8771 預覽 gateway 已核對精確 PID／命令後以 SIGINT 正常結束。
  預覽流量寫入隔離目錄；未更動原始資料、來源收據、帳本或 API key。

### 證據與尚存邊界

正式 HTTPS 最終驗收為 **217／217 案例通過**：三個首屏尺寸及 provider 代表案例／
無 IO 首屏共 49、逐一 source-owner label 頁面 156、按需工作量／重開及無 IO 行為 12。
156 是目前登記的來源標籤，不一定是 156 個獨立法人或 SDK；217 不是 unique URL 數。
另有 **99／99 響應式案例通過**：9 種頁面 × 11 個尺寸（320×568 至 2560×1440，
含手機／平板橫直、筆電與 ultrawide），每個尺寸都實際包含 provider，所有記錄的
溢出、遮擋、按鈕尺寸、未命名／未標示控制、重複 ID、console／API 錯誤與互動 gates
皆為零。最新 run：`public-browser-20261003T023313-efbfb247`。

工作量實測：8 個領域頁及 4 個 no-IO 案例，每頁首次資料完成後，再連續刷新 3 次，
被監測的大型 DOM MutationRecord 總數均為 **0**；TEJ／OpenBB／DataMonitor／traffic
沒有發出未展開的 features／history／details 請求。展開後實際取得並呈現資料；重收合
再刷新 3 次，額外明細 query 次數仍為 **0**。全資料實際點選載入更多並更新 generation
重繪路徑後保留 **50 列**，TEJ 表格按鈕到 features 的操作也在實際瀏覽器通過。
這不是把缺資料偽裝完整，也不是任意 API 回應一律延後。

最終瀏覽器收據與尺寸／截圖位於：

- `artifacts/operations/acquisition-pages-engineering-20261003/public-final/acceptance.json`
- `artifacts/operations/acquisition-pages-engineering-20261003/public-final/responsive/responsive-audit.json`
- `artifacts/operations/acquisition-pages-engineering-20261003/deployment-final.json`

### HTTP 延遲：重啟觀測與穩態分開

正式頁面／尺寸驗收結束後，本機同一 origin、10 個唯讀 route、concurrency=1、
每 route 另取首次觀測與 3 個樣本，**40 次 HTTP 均沒有錯誤**。
最新 run：`steady-http-20261003T024326-8e09b7dc`。以下為 `.steady.total_ms` 中位數：

| API | 修改前觀測 ms | 最後穩態觀測 ms |
| --- | ---: | ---: |
| 首頁 | 3.386 | 4.020 |
| 永豐 | 5.985 | 4.121 |
| FinLab | 32.426 | 12.663 |
| FinMind | 5.703 | 2.763 |
| TEJ | 5.560 | 3.529 |
| OpenBB | 5.642 | 2.039 |
| 全資料摘要 | 10.841 | 2.447 |
| TAIFEX provider | 2.344 | 1.473 |
| 特徵分頁 | 7.317 | 2.958 |
| 網站流量 | 108.502 | 50.418 |

首頁穩態略慢，沒有宣稱所有路徑都加速；來源輸入、背景負載與快取不同，這不是
控制全機條件的 AB 因果試驗，也不是極限吞吐量。前端修正可直接驗證的效益是
隱藏大量 DOM／無用途明細 query 歸零，而非把以上每一項變化都歸因於它。
FinLab 最後量測的首次觀測仍約 **2.251 秒**，不能拿隨後的 12.663 ms 冒充冷路徑。

`final-http.json` 保留重啟期間的首次首頁請求 **11.113 秒**，其中 Server-Timing
記錄 gateway app 約 2.196 秒、主要為 cache-wait；同輪 FinLab 首次請求約
**5.281 秒**。該輪處於服務啟動與公開瀏覽器驗收負載期間，不和之後的穩態混成
單一平均值。8 秒預算只限制首頁 peer future 等待，**不是全部 handler／重啟總期限**。

第一次 steady 比較誤用 7 次樣本對 3 次 baseline，既有工具正確拒絕比較、exit=2；
所有 HTTP 當時雖無錯誤，該 run 仍保留 failed，不當作成功收據。改用與 baseline
一致的 3 次樣本後才取得上述成功結果，沒有放寬工具的比較條件。

完整來源、首次／連線重用／P95、指紋與差異分別在 `baseline-http.json`、
`final-http.json`、`steady-http.json`；最終工程收據為 `FINAL_RECEIPT.json`，
均位於 `artifacts/operations/acquisition-pages-engineering-20261003/`。

本次改善的原理是「沒有使用者要看的明細，就不做那些查詢／大量 DOM 工作」，
而不是刪除來源、減少列數、放寬發布規則或降低下載吞吐量。頁面摘要仍經由同一
source receipt／公開快照更新；來源沒有發布或 snapshot 沒有更新時，瀏覽器不能
自行產生新資料，也不能把配額估計誤稱官方觀測。

初次正式 responsive run 保留為 failed：早期 harness 在展開可見前等待 hidden
資料，且頁名錯誤漏選 provider；後續又揭露 legacy sticky 遮擋。修正 harness
順序及實際 CSS 後重新完整驗收，不擦掉失敗、不略過頁面或解除 CSP。
其他早期路徑、mock、資產版本與 CSP predicate 測試失敗亦保留原收據；完成判定
只依各必要 run 名稱的最新已核對結果。

即時性仍受來源觀測、snapshot 更新週期、gateway TTL 及網路延遲限制。這次沒有
更改 provider API 配額、下載優先級、ETA 演算法、歷史資料可用性或模型訓練 ABI。
公開頁面／唯讀 API 通過不等於底層所有歷史下載完畢、PIT 安全或交易就緒。
CPU-2D 瀏覽器驗收不涵蓋 GPU/WebGL；尚未量測整個主機 CPU／RAM／GPU 降幅。
部分 status 仍需傳輸／解析完整公開 DTO，延後 DOM 不等於這些成本已歸零。
