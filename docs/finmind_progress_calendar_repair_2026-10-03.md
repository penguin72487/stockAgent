# FinMind 進度、日曆與排程修正

## 本次目標與查核基線

沿用 Free／Sponsor／Complement、帳號共用 limiter、SQLite frontier、每分鐘 quota sampler
與公開唯讀 gateway。目標是正確的進度與條件式分階段估時、減少沒有資訊增益的呼叫；
不是增加來源、付費升級、修改訓練 ABI 或宣稱全部歷史已抓完。

2026-10-03 21:56（Asia/Taipei）本機快照：共用 Sponsor 額度 6,000/h；
約 6,853 萬剩餘請求／搜尋候選，534,309 已查驗任務。台股分鐘持續下載。
131 筆 UKStockPrice 的 `unexpected_empty_after_nonempty` 保留原非空收據並重試；
其中 129 筆追新、2 筆一般回補。這不是台股分鐘停止，也不能將錯誤視為成功。
數字是取樣而非固定清冊，候選不是尚缺的資料列數。

官方來源重新核對：

- [FinMind 方案](https://finmind.github.io/Pricing/)：Sponsor 6,000/h；
  [分鐘文件](https://finmind.github.io/tutor/TaiwanMarket/Technical/)的整日全市場物件需 Sponsor Pro。
  保持目前單商品／單日形狀，不以增加 end_date 假裝減少請求。
- [TWSE 開休市日期](https://www.twse.com.tw/zh/trading/holiday.html)：
  實際歷史場次與當年表定日曆分開，週末不能任意回套到歷史星期六開市。

## 已定位的責任與修正計畫

1. `tw_stock_day_decision` 的已驗證表定週末仍回傳普通「weekend」理由。
   Complement 只接受帶官方證據的休市理由，因此最新週末可能被當成 pending。
   補齊證據標籤；compact closure 同時採用實際場次及有限表定尾端。
   證據失效／修訂時可逆重開，非空衝突仍停用整個資料集的排除。
2. 發布檢查只有 weekdays、沒有具體台股休市日；長期新分區模型對現貨仍乘每天。
   現貨來源依官方日曆跳過已證實休市；未知仍保留工作。
   長期增量採可核實的最近一年實際場次密度並標示為模型，不能稱未來的精確交易日。
   不排除財報／企業事件／新聞，也不對衍生品夜盤或海外套台股日曆。
3. 已完成舊分區的週期重驗可能以原本 acquisition 優先級搶在缺漏回補前。
   正常必要回補在前，舊分區維護在後；priority=0 追新與已明確排入的修正不受影響。
   原始值／收據／更正公告管線不刪除。
4. ETA 以 retry 作真實阻塞正確，但將未知 predecessor 傳播到全部後續階段，
   連條件試算也不可見。保留 `waiting_retry` 與無可靠完工倒數，新增獨立的
   「已知冷卻結束、下次重試成功、其後沒有新增錯誤」模型。
   後續階段包含所有前序、發布時點、開盤保護與已知新增工作；不把條件日期當成功。
5. 每次 telemetry 重算 reservation 多次會在同一秒讀到不同 queue 狀態。
   一次讀取的計畫同時供 demand、reserve、budget、公開 DTO 使用。

## 驗收計畫

聚焦測試：官方／缺證據週末、歷史星期六、臨時休市、更正／證據撤回、
非空衝突、期貨不受現貨排除；追新優先及既有歷史順序；
重試條件試算不改真實狀態／不改工作分母／不產生可靠倒數；
stale／overdue 前端及 public sanitization；預留計畫只讀一次。

固定唯讀 queue 備份比較候選及實際排程，不能把候選差異當實測下載吞吐。
通過後僅更新受影響的下載服務與公開 gateway，核對部署後非空 Parquet 的 SHA／footer，
以及公開桌面／手機頁、進度比率、分階段試算與零瀏覽器 provider 呼叫。
實測／剩餘問題附於下方，失敗收據不覆蓋。

## 已實作的計數與估時契約

進度分母是互斥的「已建任務＋未建 frontier 搜尋候選」；空回只表示已查驗，
不表示有數值。非空分區、實際筆數、候選、失敗、未知代號及未排程範圍分別列出。
不能以完成幾個 dataset、非空檔案率或搜尋進度宣稱全歷史資料完整。

估時順序共用一個帳號，不讓各階段都獨占 6,000/h。先處理到期追新與指定修復，
再處理核心缺漏、原指定九類歷史，最後才是已取得分區的例行重驗。
priority=0 追新與明確修正仍優先；不將交易新聞、企業事件或期貨夜盤依現貨休市排除。
同一次 reservation 快照供 demand、budget、telemetry 使用，避免相互對不起來。
未到發布／重試時間不是 ready；只在該次到期所在小時預留，不改成全天固定扣額。

每個階段：

1. 保留單次請求已驗證的查詢粒度與合批範圍。已驗證批次不重新拆開；
   未驗證更大的 range／全市場形狀不得當成可等價下載。
2. 速度以共用帳號實際滾動一小時與各階段有效速度建模，
   請求發出數不等於任務成功數，當下 reserve 與長期新增工作不可重複扣除。
3. 累計前序工時、已知發布／重試等待、開盤保護及前序等待期間新增分區。
   現貨未來新增負載用最近一年 **241/365** 場次密度；這是模型、不是未來精確日曆。
   其他市場無證據時保留每日上側模型；缺少完整一年或證據過期也不套現貨密度。
4. 真實 `waiting_retry` 仍沒有可靠完成倒數。另列
   `retry_condition: next_retry_succeeds_no_additional_failures`，包括現有冷卻，
   明示下次成功且無新增錯誤的假設，不修改工作分母或失敗狀態。
5. snapshot 逾期或估計期限已過，畫面顯示過期／尚未完成，不將舊「一分鐘」不停重播。
   無已知工作的末階段顯示「目前無已知待發請求」，不把前序未完成說成全域完工。

版本：Complement dispatch **4**、ETA snapshot／public estimate **7**、
發布時鐘 **3**、compact history calendar **2**。舊估時 v6 仍可讀，但沒有新增重試條件模型。
本次不改來源原始值、訓練 ABI、broker／paper fills 或其他 provider。

## 本機固定快照：減少候選與計算

[benchmark 收據](../artifacts/data_quality/finmind_progress_calendar_2026-10-03/benchmark.json)
使用同一 SQLite backup，**不改正式 queue、不呼叫 provider**：

| 項目 | 修正前 | 修正後 |
| --- | ---: | ---: |
| 待查歷史／forward 請求候選 | 68,527,595 | 68,520,821 |
| 台股分鐘候選 | 6,352,694 | 6,352,692 |
| 台股 tick 候選 | 6,433,400 | 6,430,014 |
| 台股分點候選 | 4,330,722 | 4,327,336 |
| 完整 ETA 日曆計算耗時中位數 | 2.7788 秒 | 1.6698 秒 |

新排除表定、已驗證週末 **2026-10-03**，共少 **6,774** 個候選。
期貨、選擇權、美股與尚未到時點的權證候選未因此刪除。
已經成功查空的舊股票分鐘任務保留原空回證據，沒有改寫成新下載。
175,028 個非空任務／收據 metadata fingerprint 前後相同：
`524567d6709811c6e376133d45e752c7c6427519679dafc651cf7e05e5840443`。

完整估時用同一 v7 模型比較「額外做一遍獨立日曆預估」與
「單一順序日曆投影」，交錯各跑三次，完整 JSON SHA 完全一致。
計算耗時中位數減少約 **39.9%**；取樣區間仍有重疊。
這只證明本次 ETA 計算改善，**不表示 API 吞吐提升 40%、
全系統 CPU／RAM 降低 40%，或已實測省下 6,774 次 provider 呼叫**。

## 正式部署與來源驗收

2026-10-03 22:17～22:18（台北）只重載 Complement、Sponsor、public gateway。
[前快照](../artifacts/data_quality/finmind_progress_calendar_2026-10-03/deployment_before.json)與
[後快照](../artifacts/data_quality/finmind_progress_calendar_2026-10-03/deployment_after.json)證明：

- 三個受影響服務有新的 InvocationID；dispatch 4、ETA 7、公開 DTO 7 已載入。
- Free、永豐行情、當沖及隔夜模擬服務 InvocationID／NRestarts 未改。
- 已查驗由 535,787 增至 535,816；總候選 69,062,958 降至 69,056,184。
  分母下降來自官方休市證據，不是刪資料讓進度看起來變快。
- 後快照台股分鐘仍是第一個未完成歷史階段，美股分鐘仍最後。
- [新來源驗收](../artifacts/data_quality/finmind_progress_calendar_2026-10-03/runtime_acceptance.json)：
  `TaiwanStockKBar / 00741B / 2026-09-21` 在部署後取得 10 列，
  收據筆數、Parquet footer、SHA-256 一致：
  `b003ed53cf719e28c129ed190e785b35b98a4e901e8778d56e9f3b6e25eb1ef7`。
  這是一筆來源檔驗收，不是保證每分鐘都有成交或全市場每分鐘無缺。
- 正式 compact calendar 3,259 個休市日期，來源實際場次界線
  1999-01-05～2026-10-02，加表定週末尾端；四個現貨 dataset，0 非空衝突。
  TWSE 實際場次收據 SHA：
  `23b042cdb7d0716b663ea82bdd4236847c634d2f4b1a467f275e6b4e3424ddca`。
- 既有官方交易日曆 timer 仍每 30 分鐘更新、含休市；未另開重複日曆下載器。
  FinMind 官方帳號流量與 ETA 仍按既有每分鐘 timer 取樣。

22:24 的[最終再驗收](../artifacts/data_quality/finmind_progress_calendar_2026-10-03/final/deployment_after.json)
仍為 dispatch 4／ETA 7／公開 DTO 7；已查驗 536,284，比重載前多 497。
受保護服務沒有重啟，原 131 筆 UK 與既有 delegated 504 錯誤仍保留。
公開 `app.js?v=20` 與本機 asset SHA 相同：
`c036898ef9adf53f76583f8c8afccaef7d7f8fa984dad2f1888bc3dcc141a85c`。

## 22:18 快照的分階段完成日期

以下是 **下次重試成功、其後無新增錯誤且資源足夠** 的條件試算，
不是服務成功收據、保證期限或統計信賴區間。日時全為台北；
每分鐘觀測更新後會改變，不應把此表當未來固定排程。
已知冷卻最晚約 22:32，表示可重試，不表示屆時必定成功。

| 階段 | 本階段剩餘請求／候選 | 最快累計完成 | 中間累計完成 | 保守累計完成 |
| --- | ---: | --- | --- | --- |
| 到期追新 | 129 | 2026-10-03 22:32 | 2026-10-03 22:32 | 2026-10-03 22:34 |
| 核心缺漏 | 2 | 2026-10-03 22:32 | 2026-10-03 22:32 | 2026-10-03 22:34 |
| 台股分鐘 K | 6,352,097 | 2026-11-29 20:26 | 2026-12-14 23:56 | 2026-12-16 02:10 |
| 期貨分鐘 K | 6,066,080 | 2027-01-24 06:25 | 2027-02-23 16:45 | 2027-02-25 21:23 |
| 台股分點 | 4,327,336 | 2027-03-07 16:18 | 2027-04-19 07:38 | 2027-04-22 16:23 |
| 權證分點 | 826,368 | 2027-03-16 05:59 | 2027-04-30 22:27 | 2027-05-04 04:03 |
| 台股 tick | 6,430,014 | 2027-05-19 20:51 | 2027-07-24 02:15 | 2027-07-28 22:26 |
| 期貨 tick | 6,218,993 | 2027-07-20 20:35 | 2027-10-12 22:11 | 2027-10-19 02:26 |
| 選擇權 tick | 1,524,545 | 2027-08-05 04:29 | 2027-11-02 00:38 | 2027-11-08 17:09 |
| 期貨價差 tick | 160 | 2027-08-05 04:34 | 2027-11-02 00:46 | 2027-11-08 17:17 |
| 美股分鐘 K | 36,774,633 | 2028-12-10 21:28 | 2029-10-06 22:52 | 2029-10-30 20:09 |
| 次要維護／校驗 | 0 | 目前無已知待發 | 目前無已知待發 | 目前無已知待發 |

22:18:02 的 ETA 取樣待辦合計 **68,520,357** 次請求／搜尋候選；
目前查驗進度約 **0.78%**，不是資料列完整率。未清點歷史代號、
未知來源上市存續期間與未來新增代號另計，因此日期仍是模型。
進度清冊另於 22:18:15 取樣；正式 worker 持續改變佇列，兩者需按時間對帳。
持續增量沒有永久「全部抓完」；盤後追新、歷史回補與來源錯誤分別觀察。

## 重試來源問題與呼叫證據

131 個 `UKStockPrice / unexpected_empty_after_nonempty` 仍是失敗、仍保留非空基線，
本次沒有重設為 complete／observed_empty 或清掉 error。
另有一筆已交由 Sponsor 的舊 `TaiwanDailyShortSaleBalances / http_504` 保留稽核；
不得混成 Complement 新的阻塞工作。

[正確參數的有限探測](../artifacts/data_quality/finmind_progress_calendar_2026-10-03/uk_retry_validated_probe.json)：
`0D00.L` 原非空基線 2,142 列、2018-01-26～2026-09-29。
全史 1900 起、既有基線起點、最近七日，三種合法請求皆回 0 列。
因此**此樣本**不能靠縮日期修好；這不是已證明所有 131 檔永久缺資料，
也不是 FinMind 全來源完全錯誤的結論。續由既有 worker 依現有冷卻重試，
不能用新空回覆蓋既有非空來源。

探測共 6 次 data endpoint 呼叫：先前診斷程式漏傳 dataset 的 3 次 400
是本次工具錯誤，不是來源失敗；修正後合法探測 3 次。
[原失敗證據](../artifacts/data_quality/finmind_progress_calendar_2026-10-03/uk_retry_probe.json)保留。
所有探測都走原共享 limiter／request ledger；正式 queue／來源 head 寫入為 0。
benchmark、runtime 與瀏覽器驗收本身各為 0 provider 呼叫；正式 worker 正常下載另計。

## 回歸、公開畫面與可重跑指令

- 聚焦測試 151 項；前端條件標記追加回歸 47 項。
- 相關 FinMind／日曆／共享 gateway／acquisition policy 最終廣泛回歸 925 項通過（62.65 秒）。
  這些有重疊，不把它們相加當成不重複測試數；也不是全 repository test suite。
- [公開瀏覽器驗收](../artifacts/data_quality/finmind_progress_calendar_2026-10-03/browser/browser_acceptance.json)：
  1440、1024、390 三種寬度；12 個階段、9 條歷史進度條，
  比率對應同次 API 快照，條件日期／階段開始顯示正確，
  完整資料清冊與篩選一致，0 JavaScript error、0 頁面水平溢出、0 provider 呼叫。
  桌面與手機 PNG 已人工檢視；畫面 API 為既有公開唯讀 metadata gateway。
- 初版 browser QA 將「零待辦維護階段」誤當成也要顯示條件日期而拒絕；
  修正驗收分支後重跑通過，保留原失敗 log。初版估時 parity 抓到 unknown 前序的
  舊 opening-pause 欄位殘值，已清除後全 JSON parity 通過，沒有放寬比較條件。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s test/test_finmind_*.py test/test_market_status_calendar.py test/test_public_dashboards.py test/test_acquisition_policy.py
run_fintech_python -m scripts.benchmark_finmind_progress_calendar --output artifacts/data_quality/finmind_progress_calendar_2026-10-03/benchmark_rerun.json
run_fintech_python -m scripts.verify_finmind_history_sequence --since 2026-10-03T14:17:28.777965+00:00 --output artifacts/data_quality/finmind_progress_calendar_2026-10-03/runtime_rerun.json
run_fintech_python -m scripts.verify_finmind_scheduler_dashboard --base-url https://penguin72487.ddnsgeek.com --output-dir artifacts/data_quality/finmind_progress_calendar_2026-10-03/browser_rerun
```

**完成的是本次估時／排程／日曆修正與部署驗收，不是所有歷史下載完成、
來源永不出錯、完整 PIT 訓練發版或全歷史存儲容量驗收。**
