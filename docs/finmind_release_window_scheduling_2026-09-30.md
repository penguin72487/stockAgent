# FinMind：發布時段預留與到期優先排程

## 契約與問題

這次修改下載排程與 ETA，不改原始資料、歷史發布證據、訓練 ABI，也不表示全歷史已抓齊。

先前面板的約 1,023 次／小時是 ETA 的日均追新模型，不是實際每小時鎖住的配額。但執行排程仍有三個問題：Free 固定保留四次、海外更新用固定比例分配、Complement 每四次強制插入背景工作。它們不能精確表達「到期追新最高優先，其他時間盡量回補」。

## 新規則

| 範圍 | 行為 |
| --- | --- |
| 發布前其他小時 | 不替尚未到期的工作保留配額。例如 18:30 工作不在 17:00 預留。 |
| 發布所在小時 | 按已知任務實際待執行次數預留。18:30 工作從 18:00 的預留窗口開始納入；18:30 才能派送。 |
| 已到期 | 三個 worker 共用到期需求；其他 worker 不再啟動低優先背景請求。已送出的請求不取消、不重抓。 |
| 同層追新 | 在最高優先級內依資料集輪轉，避免數萬檔海外日資料擋住每小時新聞／較小的結算批次；不混入低優先歷史。狀態預覽不會推進輪轉位置。 |
| 已完成／仍冷卻／已封鎖 | 不算可派送追新；未到重試時間不搶占背景作業。到期失敗會沿用來源重試規則。 |
| 同資料別名 | Sponsor 已主責且健康的 Complement 別名不重複保留，避免沒人會執行的假需求卡住全部回補。 |
| 無到期工作 | 既有歷史與指定期貨回補恢復使用可用配額；tick 仍保留原低優先級。 |
| 配額 | 官方帳號用量＋帳號取樣後本站請求；同一 shared limiter。每分鐘更新長批次帳號觀測。剩餘量為零／證據過期時，追新也不能繞過配額守門。 |

保留量另含兩次既有並行在途緩衝。這不是每日追新配額。台北整點只是排程窗口邊界，**不假設 FinMind 在整點重置配額**。大型已到期批次可能占滿當時配額，這是實際待抓工作，不是整天固定保留。開盤資源保護維持原契約。

等待追新清空的 worker 每五秒僅檢查本機預算／佇列；可回補即喚醒，不必再空等原本一分鐘。這個檢查不打 FinMind API；來源明示的節流／封鎖等待不會被縮短。

## 日曆

- FinMind `TaiwanStockTradingDate` 每小時重查，另外在官方表定平日 18:00 邊界重新檢查。成功時間與失敗／重試時間分開記錄；一般失敗五分鐘後重試，來源較長節流時間優先。
- 新的 `stockagent-tw-public-calendar-refresh.timer` 每半小時更新官方 TWSE 休市表，包含假日。沿用 `download_tw_public_data.py`、共用來源 writer lock、原始回應與 Parquet；批次收據獨立放在 `artifacts/data_refresh/tw_public/calendar/latest/`，不覆寫盤後主批次收據、不另啟冷庫同步。
- 寫入者忙碌時保留舊證據，記錄跳過並等待下個 timer，不偽造更新時間。下載失敗時亦保留最後有效資料。
- 開盤保護一律使用共用官方交易日決策，不再由 Complement 用星期幾或 FinMind 年度日期快取自行判斷。
- 新聞、財報／公司事件、海外市場與期貨不套用台股休市刪除規則。TAIFEX 公告歸檔維持其既有獨立服務；這次沒有宣稱重建或校驗全部海外及期貨交易日曆。
- `TaiwanStockPrice` 的設定依官方文件由 18:00 修正為 17:30；實際來源是否已發布仍以回應／收據為準，不因表定時間而標成完成。

來源：[FinMind 技術面文件](https://finmind.github.io/tutor/TaiwanMarket/Technical/)、[官方 API 用量文件](https://finmind.github.io/api_usage_count/)、[TWSE 休市表](https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule)。

## ETA v4／面板

保留原五階段與同一帳號吞吐證據，新增按時點的追新事件模型：

1. 從現有佇列的下次執行時間和成功續期規律產生事件，時間向上取整至分鐘。
2. 尚未發布不扣計算容量；發布後插入共用容量。開盤保護期間到達的追新也會保留，不能遺失。
3. 當前到期欠帳只在原始工作分母計一次；後續階段不重複計入前階段已處理的發布事件。
4. 「此刻實際保留」與「本階段平均回補速度」分開顯示。整段平均可能因未來發布批次而降低，並不代表目前每小時扣住相同額度。
5. 低優先歷史重驗、新日分區仍為階段平均模型；新上市商品、未來額外失敗、未確認假日不假裝精確。估時不是永久完工承諾。

讀取真實佇列的抽樣：44 組發布事件；百萬次請求情境的事件模擬約 5 ms。這只量測新增事件模型，不是整個 ETA 盤點服務的耗時。

## 部署與驗證

- 最終 `run_fintech_python -m pytest -q test/test_finmind*.py test/test_market_status_calendar.py`：668 項通過。包含發布／休市／重試邊界、跨 worker 優先級、同層輪轉、五秒恢復、事件估時與公開投影。
- 當時較廣的網頁測試發現 `test_day_trade_clock_ends_at_auction_without_overnight_contract_details` 沿用舊收盤文字。後續已依使用者要求對齊現行不限容量紙上平倉規則，補明缺價／停牌／未交付時仍保留部位；公開網站與收盤平倉回歸 124 項通過，見 [當沖時鐘修復](tw_day_trade_clock_text_repair_2026-09-30.md)。
- 18:09:29 台北：官方日曆首輪抓取成功；CPU 約 522 ms、尖峰記憶體 66.8 MiB。
- 18:09:57 台北：三個 FinMind worker 重載；18:10:01 唯讀 gateway 重載。
- 18:10 觀測：FinMind 日曆已取得新成功收據；到期 0 次，本發布小時尚有 17 次未到期工作，連同在途緩衝保留 19 次。歷史期貨 K 棒請求隨後恢復。
- 18:22:23 最終三個 worker 再次重載，18:22:28 gateway 重載。18:22:46 的 API 實際回傳 `schedule_verified=true`、`allowed=true`、到期 0 次、保留 14 次；最近 60 秒有 82 次 `TaiwanFuturesKBar` 請求（此時間窗跨越重載）。這是派送證據，不表示全部歷史完整。
- 18:23:25 前的重載後收據：95 個期貨 K 棒分區成功（20,442 列）、3 個來源空回；空回與成功分開統計，沒有把空值當有資料。
- Playwright 已檢查 1440／390 寬的面板，無 JavaScript 錯誤與橫向頁面溢出；查看實際截圖後，修正流量／估時區塊跳轉時的延遲渲染空白。截圖在 `artifacts/data_quality/finmind_schedule_20260930/`。外部既有 `/finmind/` 頁面回傳 200；數值驗收使用本機唯讀 API。

主要實作在 `downloader/finmind_scheduling.py`、`finmind_account.py`、三個既有下載器、`finmind_eta_schedule.py` 與原 ETA／面板路徑。安裝日曆 timer：`bash scripts/install_tw_public_calendar_refresh_service.sh`。

## 追加：全清冊追新觀測與每 call 合批

本輪範圍是 FinMind 的完整既有註冊清冊：106 個不重複 dataset、132 個 owner／別名展示列，不是所有其他 provider 的發布時鐘。新聞已納入；tick 仍維持最低優先級。清冊含已排程來源與其缺口，不能把註冊、查驗或來源空回當成全部歷史完整。

### 時間與更新事件契約

沿用 Free、Sponsor、Complement 的下載回應及驗證結果，新增 `downloader/finmind_updates.py`。**觀測紀錄本身不新增 API 呼叫**；發布後確實空回的重試仍會消耗配額。

| 欄位／事件 | 定義 |
| --- | --- |
| `request_started_at_utc`／`checked_at_utc` | 真實送出前時鐘與收到、解碼回應的時鐘，與原始資料日期分開。 |
| `source_first_date`／`source_last_date` | 本次比較內容的資料日期，不是發布時間。全市場分區與逐檔 stream 分開比較。 |
| `first_nonempty`／`first_empty` | 監測啟用後的初始基線；首次取得不能推定來源首次發布。 |
| `unchanged` | 正規化欄位／值及重複列均未變；只推進檢查時間，不推進內容更新時間。 |
| `new_data`／`revision` | 內容改變且最新資料日期增加／未增加。後者包含同日期內容修訂，不只判斷最大日期。 |
| `first_nonempty_after_empty`／`coverage_changed` | 從空回取得值／內容縮小或轉空；下載器原有「非空後意外空回」防護仍保留。 |
| `last_data_observed_at_utc`／`last_changed_at_utc` | 最近取得新非空內容、最近比較發現內容改變的時間；初始基線不冒充修訂。 |
| `availability_after_utc`／`availability_by_utc` | 前次舊內容的請求開始，到本次新內容回應完成的 API 觀測區間，**不是精確發布區間或 PIT 證據**。來源快取與處理延遲可能使之更寬。 |
| `provider_published_at_utc` | 無精確來源證據時保持 `null`；不拿資料日期、表定時間或抓取時間代填。 |
| `response_rows`／`network_seconds` | 最近真實 call 的回應列數與請求至回應解碼耗時；歷史合併後列數另列，不能相加冒充下載量。 |
| `next_check_at_utc` | 優先讀實際佇列期限；僅預計可排，不保證配額、前置依賴或網路允許準時完成。 |

每個 worker 在既有資料根目錄新增 `update_observations.sqlite3`，保留逐次 `checks`、逐 stream 比較頭與有界 dataset 頭。收據加上 `update_observation` v1，發布／檢查階段設定為 v2。舊收據及原始 Parquet 仍保留；不回填成監測啟用以前的發布證據。失敗紀錄獨立，不抹掉最後有效內容與更新時間。

同 dataset 不同分區並行完成時，內容／觀測區間／失敗時間各自依實際時間保護；較早成功不能抹掉較晚失敗，逆序處理不讓公開更新時間倒退。

比較使用逐列 canonical JSON 的多重集合 fingerprint，列順序不影響結果，但重複列、NULL、欄位缺少與負值均被區分。複雜度為 O(回應內容 bytes)，不全量排序、不讀歷史 Parquet。歷史低優先 tick 不逐次加入追新 fingerprint；當期／近期已排下載仍可觀測。批次 fan-out 共用 transport request ID，重試同一 request 不重複累加檢查。

### 發布邊界、失敗與省配額

- 對照 [技術面](https://finmind.github.io/tutor/TaiwanMarket/Technical/)、[籌碼面](https://finmind.github.io/tutor/TaiwanMarket/Chip/)、[基本面](https://finmind.github.io/tutor/TaiwanMarket/Fundamental/)、[期選](https://finmind.github.io/tutor/TaiwanMarket/Derivative/) 與 [可轉債](https://finmind.github.io/tutor/TaiwanMarket/ConvertibleBond/) 官方文件，整理 45 個明確鐘點。它們是預期發布邊界，不是已發布證明；其平日／週六規則只用於未來檢查，不能刪除歷史特殊交易日。
- 例如還原股價 17:30、法人 20:00、融資融券 21:00、股票分鐘 15:50、期貨分鐘 16:30、借券餘額 21:00。對應主責 worker 與預留／喚醒時間一起修正，避免只改 UI。
- 非空取得後，有確認規律的資料改排下一個發布階段，取消無意義的固定四小時回查。空回先 1、2、4、8 分鐘重試，再封頂 15 分鐘；表定發布以前／非其發布星期則等待有效窗口。失敗、節流、權限與意外空回仍沿用既有分類和守門。
- 當沖成交量值官方為 21:30：保留原 18:00 名單補查政策，另在 21:30 查最終欄位。**18:00 不是盤前名單的實際發布時間**。最終欄位取得後排下一個有效階段，不再顯示凌晨四小時重查；ETA 分別計入兩階段，不重複把成功佇列續期再算一次。
- 舊佇列的當期成功／空回期限經 `finmind_release_clock_migrations` 逐版本保留審計，資料與收據不被覆寫成新證據。發布前成功取樣亦不能阻止當天較晚的官方最終更新。
- 「每天 1:30」但未說上午／下午、時間範圍、事件型更新及其他未確認來源，明示既有檢查政策／發布未確認，沒有把推測當作精確鐘點。尚未到第一次排程者顯示尚未觀測，不為了填滿畫面重抓。
- 非空但未變更且無來源最終版本／完整性指標的回應，不能證明表定更新已完成；也不盲目重打所有非空來源。如果來源延後修訂，首次看到時間仍取決於後續既有追新／更正排程。本輪沒有宣稱即時、精確偵測所有來源內部版本更新。
- 配額預留仍只發生在所需發布小時；到期最高優先，三支 worker 共用同一帳號 limiter。未改成每個 worker 各拿一份 [官方 Sponsor 6,000 次／小時](https://finmind.github.io/api_usage_count/) 配額。

### 每 call 最大有效資料量

1. 已驗證的 11 個 Sponsor range contracts，追新也能把已到期、連續、同 dataset 的分區合成一次請求。此前背景預算不允許時，追新也被迫取消合批；現改成只合併 p0 到期任務，不順帶領走歷史低優先或尚未到重試時間的任務。
2. Complement 的已驗證總經／全市場年度 range 亦採相同到期合併；原最大歷史範圍與每週完整重驗保留。合併寫入不能把保留歷史算成本 call 的回應量。
3. 官方單日全市場、逐檔逐日、新聞與其他受方向／日期限制的端點不硬拼超限日期。安全 body／row guard 與資源失敗縮批機制繼續有效；它們是本機保護，不冒稱官方限額。
4. 未知歷史商品集合、端點未證實最大跨度及可能來源截斷仍是限制：`global_minimum_requests=null`，不宣稱已證明全站理論最少呼叫。目標是最大有效新資料量，而非反覆載入已取得數值來提高列數。

### 網頁、清冊與驗收

既有 [FinMind 頁面](https://penguin72487.ddnsgeek.com/finmind/) 的卡片及進度表新增發布規律、最後檢查、內容首次觀測／變更、下次可排、更新觀測區間與每 call 列數。委派別名讀取實際 owner 的同一觀測，不再呈現舊 owner 的空白監測。寬表等本機衍生顯示本機寫入及源資料檢查，不能冒充獨立 API 更新。

公開投影只讀三個小型 dataset 頭與有索引佇列；不掃描原始 Parquet、不向 FinMind 呼叫補畫面，金鑰、request ID、原始列及內部路徑不傳到瀏覽器。觀測資料庫損壞時明示不可讀，不讓頁面整體崩潰。static app v13／FinMind CSS v7。

- 最終 FinMind／日曆回歸 **691 項通過**；另外 JS 語法與 `git diff --check` 通過。測試含內容重排、重複列、NULL、修訂、空回轉非空、失敗保留、時間倒退、跨分區逆序完成／成功失敗逆序、批次 request 冪等、發布／休市邊界、別名／衍生、實際佇列期限與無 API 的公開投影。
- 部署後已驗證真正新收據包含 observer v1，不只檢查 systemd active。2026-09-30 21:37 法人一次回應 126,443 列、當沖一次 2,072 列；這是該次 API 回應量，不是所有歷史完整。
- 網頁 1440／390 寬檢查全部 132 列、主檔篩選往返、當沖新觀測與進度表，無 JS 錯誤、無頁面橫向溢出；真實截圖及 API 驗收保存在 `artifacts/data_quality/finmind_update_monitor_2026-09-30/`。使用 CPU 2D browser profile，並非 GPU／WebGL 驗收。
- 公開頁及 API 已驗證為新版本。原始投影兩次單次量測分別約 0.14／0.38 秒；不是全系統效能基準，也不保證所有佇列規模都相同。
- 21:57:32–38 台北：發布階段 v2 三個 worker 與 gateway 重載；22:06 最終桌機／手機驗收完成。22:22:50 台北：補上跨分區逆序觀測時間保護後，再重載三個 worker。當時既有 Free／Sponsor／Complement 的 49 次觀測未發現 ledger 與公開新內容時間不一致，不需改寫舊收據。

完整獨立資料集 JSON／CSV 由現有清點工具零 API 呼叫產生，包含原查詢形狀、起迄日期、觀測列數、owner、失敗／缺口、發布規律及更新觀測。可重新產生：

本輪最終清冊：[106 類資料集 CSV](../artifacts/data_quality/finmind_update_monitor_2026-09-30/finmind_query_ranges_20260930T142425570530Z.csv)、[完整 JSON](../artifacts/data_quality/finmind_update_monitor_2026-09-30/finmind_query_ranges_20260930T142425570530Z.json)。清冊是 22:24:25 台北的觀測快照；持續追新狀態看網頁，不把快照時間當作發布時間。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_finmind_query_ranges \
  --output-dir artifacts/data_quality/finmind_update_monitor_2026-09-30
```

實作與部署只證明本輪排程、觀測及網頁行為；**不宣稱全部歷史已完整、所有來源真實發布時間可考或可直接 PIT／實盤使用**。精確來源時點有新證據時再增補，不替已保存的推測或觀測區間改寫成歷史事實。
