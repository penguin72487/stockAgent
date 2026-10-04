# FinMind 派工查詢與有效下載吞吐修正

本輪重新檢查 Free、Sponsor、Complement 的排程、共用配額、任務落盤和
公開狀態，確認新的主要本機瓶頸是任務庫缺少查詢統計。已修正共用初始化，
2026-10-04 01:54:59 台北部署到 Complement、Sponsor；本輪沒有重啟交易服務。
控制實驗的 Complement 本機批次 CPU 中位數降低 55.4%，全部輸出一致。
這不是 API 吞吐提升 55.4%，也不是全歷史已下載完成。

前輪發布日曆、優先清單查詢和重複統計修正見
[週期效率報告](finmind_cycle_efficiency_2026-10-04.md)；同源重試、BUSY
恢復及連線生命週期見[故障恢復報告](finmind_resilience_throughput_2026-10-04.md)。
本輪取新的基線，沒有把不同輪的百分比相加。

## 資料流程與新的基線

下載仍由原 worker 建立任務，透過同帳號 limiter 取得配額，再發出合法
來源請求、驗證原值及檔案、保存 Parquet 與收據，最後發布進度與估時快照。
因此請求開始、空回覆、非空分區、淨新增資料、歷史完整率是不同指標。
降低 SQL 成本只能改善本機等待與計算，不會解除來源限制或補出不存在的值。

截至 2026-10-04 01:38:20 台北的前 15 分鐘：

| 指標 | 實測 |
| --- | ---: |
| 共用帳本請求開始 | 1,310 次 |
| 台股分鐘請求 | 1,295 次，等比約 5,180 次每小時 |
| 分鐘任務最新狀態為非空 | 1,027 個分區，92,125 列 |
| 分鐘任務最新狀態為空 | 267 個分區 |
| 英股來源探測 | 15 次，原 131 個失敗歷史仍保留 |
| 相鄰請求間隔中位數及第 95 百分位 | 0.610 秒及 0.716 秒 |
| 超過 2 秒的間隔 | 23 段，共 102.05 秒 |

最新任務狀態可能合併同一 key 的多次重試，尚在執行的請求也未形成完整
收據，不能拿上述比例當成功率。這個窗口包含前輪重啟及首次建單。

## 查詢瓶頸與修正

對正在下載的 Python worker 做 49 Hz、30 秒有界剖析，確認 CPU 活動包括
追新檢查、配額預留、完整狀態聚合及一般派工。兩個 owner 的 SQLite
都沒有 `sqlite_stat1`；Complement 的追新檢查採用按 priority 查找的計畫，
需要檢查同優先級的 21,165 個工作，即使絕大部分早已完成或尚未到期。
已有索引不等於查詢規劃器會選到正確索引。

修正放在既有共用入口，不建立第二個 scheduler：

- `finmind_runtime.optimize_queue()` 使用 `PRAGMA optimize=0x10002`。
  本機 SQLite 3.53.4 的分析範圍會自動受限；較舊分支暫設不超過 1,000
  的 analysis limit，並恢復呼叫端設定。這不是無界的全庫 ANALYZE。
- Complement 的 `_db()` 在既有 schema／索引建立後維護統計。Sponsor
  直接共用同一函式；正常批次重開連線也會檢查是否需要更新統計。
- 初始化任何一步失敗時先關閉連線，再保留原例外交給既有錯誤處理。
  解決呼叫端尚未進入 `closing()` 時的連線洩漏；只對 BUSY／LOCKED 重試，
  不把 SQL、損壞或 I/O 錯誤當作普通競爭。
- 不新增索引、不強制固定查詢計畫、不改 WAL／FULL 耐久性，不改任務值、
  收據、時間、API 參數、配額、追新優先或九類歷史順序。唯讀 reader 不維護統計。
- 診斷工具用獨立 backup 副本測候選，避免初始化候選時悄悄更新基準庫。
  同時凍結時計與 delegation，保留完整建單、狀態發布和逐列結果比對。

[SQLite 官方文件](https://www.sqlite.org/lang_analyze.html)建議新連線使用這個
optimize 模式，並說明 3.46 起的有界分析；官方同樣提醒統計不保證讓所有
查詢都變快，所以本輪另測整段派工流程，而不是只比較單條 SQL。

部署後唯讀查詢計畫證據：Complement／Sponsor 分別有 26／19 筆統計。
Complement 的同一追新 SQL 已改成 `MULTI-INDEX OR`，分別依空 deadline
及到期 deadline 在原索引查找，沒有再掃整個高優先級集合。
Sponsor 仍有部分 priority 掃描；本輪不宣稱它的所有查詢均已變成索引 seek。

## 控制實驗

基準保留部署前 schema 與缺少統計的狀態；候選在獨立副本呼叫新的初始化。
兩邊均使用前輪已部署的發布規則與 `seed(include_status=False)`，隔離本輪
統計修正的作用，沒有再次計入前輪省下的重複清冊成本。

| 測量範圍 | 基準 CPU 中位數 | 候選 CPU 中位數 | 差異 |
| --- | ---: | ---: | ---: |
| 完整單次 selector，7 次 | 22.13 ms | 8.36 ms | 降低 62.2% |
| 開庫、建單、120 次 selector、5 次完整狀態，3 次交錯順序 | 10.12 s | 4.51 s | 降低 55.4% |

首次候選 planner 初始化另計約 0.422 秒；完整批次比較已包含這個成本。
完整批次 wall 中位數 10.24→4.45 秒，降低 56.6%。基準 CPU 樣本
6.16／10.94／10.12 秒，候選 3.72／4.51／5.96 秒；主機背景負載與小樣本
限制仍在，不能把中位數當所有情境的保證。

兩邊全部 task rows、frontier rows、公開 status 和 120 次選擇結果的
SHA 相同，固定基準檔亦未改動。完整批次的六份輸出 SHA 均為
`a4d1c4d7d5a6f2e4990b6030e4bd8b325dfa3644d4b31c9fafa453c93b0b05be`。
此測試不含 HTTP、原值轉換與資料落盤，也不涵蓋每個請求的 account 預留
檢查，不能用來宣稱整個 worker 或 provider 加速同樣百分比。

## 部署驗收與下載結果

- **980 passed**：FinMind 全部相關測試，加上配額、交易日曆與共用公開面板。
  七個新測試涵蓋共用初始化、原始任務資料不變、統計形成、耐久性、較舊
  分支的設定恢復，以及 BUSY／非 BUSY 初始化失敗的連線回收。
- 01:54:59 只重啟 Complement、Sponsor。01:55 的初次驗收中，Free、
  唯讀 gateway、永豐及台股當沖／隔夜模擬的 invocation 與 restart 計數
  均與部署前一致。
  兩個更新後 worker 的自動 restart 計數仍為零。
- 實際新分鐘資料收據通過身份、Parquet SHA 和 footer 列數驗證。
  dispatch 6、ETA snapshot 8、公開 projection 8 一致；本輪無來源或派工
  語意變更，沒有為報表改動而另創不相容資料契約。
- 正式網站 1440／1024／390 px 驗收通過，無 JS 錯誤或橫向溢出。
  九個歷史進度、十二個估時階段、前後總數與候選分母均吻合；瀏覽器
  不呼叫 provider。仍依台股分鐘、期貨分鐘、分點、權證分點、各類 tick、
  美股分鐘最後的原順序執行。發布日曆、現貨休市與衍生品夜盤區別均保留。

最後驗收另外偵測到唯讀 gateway 在 **02:01:30** 開始重啟、02:01:36
恢復 running；invocation 與初次驗收不同，自動 restart 計數仍為零。
它不在本輪保存的重啟指令內，不能根據這點推定實際發起者。第一次最後
驗收的「所有受保護 invocation 不變」檢查因此正確失敗，原收據保留；
不能把這個檢查改稱已通過。補驗目前 gateway 的唯讀契約、快照、分母
與瀏覽器行為後再交付；其餘受保護服務及更新後 FinMind invocation
保持不變。本輪不回滾可能來自其他工作的網頁服務變更。

01:54:59–01:59:55 台北的部署後短窗為 4.941 分鐘，包含重啟及首次建單：

| 指標 | 實測 |
| --- | ---: |
| 共用請求開始 | 439 次，台股分鐘 434 次及英股探測 5 次 |
| 分鐘請求等比速率 | 約 5,271 次每小時，基準約 5,180 |
| 固定基準未完成、部署後形成的新非空分鐘分區 | 326 個，28,146 列 |
| 分鐘最新空回覆 | 107 個分區 |
| 相鄰請求間隔中位數及第 95 百分位 | 0.610 秒及 0.636 秒 |
| 超過 2 秒的間隔 | 5 段，共 21.92 秒 |

短窗的日期、商品密度及背景負載不同，不能將速率約 1.7% 的差異認定為
穩定、因果性的 provider 加速。已確認的是本機派工 CPU 成本下降、正式
新資料持續落盤、來源與公開狀態一致；不縮資料範圍或降低驗證來換吞吐。

## 配額與仍待重試的資料

[FinMind 官方方案](https://finmind.github.io/Pricing/)的 Sponsor 上限仍為
共用每小時 6,000 次；整日全商品分鐘／逐筆類批次下載屬 Sponsor Pro 的
另一種權限。未授權的批次形狀不能拿來當減少呼叫的方法。本輪不增加 API
呼叫，也不另開重複管線。此前的當發布小時預留、追新優先、合法最大日期
範圍與相同資料的單一主責機制保持不變。

131 個 `UKStockPrice / unexpected_empty_after_nonempty` 仍待重試。
部署後已逐個驗證舊收據內容、Parquet SHA 與 footer 筆數，沒有被新空回覆
覆蓋，也沒有改成 complete。既有輪流探測會繼續尋找恢復；這不是全來源
永久錯誤的證明，也不是全資料已抓齊。

更新：以上是當時驗收的重試政策；後續依使用者指示改成
[達上限即停止自動重試並保留紀錄](finmind_bounded_retry_2026-10-04.md)。
這 131 個任務不再無限探測，原值與收據仍保留；不算完成。

另外一個 Complement 的舊 `TaiwanDailyShortSaleBalances / http_504` 是轉交
Sponsor 前的稽核失敗。01:59 的現任 owner 證據為 5,227／5,227 個目標
分區 complete、零 failed／blocked，7,995,030 列，實際日期 2005-07-01
到 2026-10-02。舊錯誤保留，不重複逐檔補抓，也不把它計入現在的主責阻塞。
這只能證明已註冊目標分區的收據狀態，不能保證來源從未漏列或修訂。

面板在驗收時顯示約 951.1 天的中間情境，但明確標示「若下次重試成功」
的條件試算；約 6,852 萬未建歷史搜尋候選不等於確定尚缺同樣數目的 K 棒。
資料持續增量也沒有永久抓完日。本機 SQL 優化不能直接消除這個配額與
候選搜尋規模，後續正常服務繼續留下較長時間窗口的流量和資料收據。

## 證據與重現入口

[完整證據目錄](../artifacts/data_quality/finmind_effective_throughput_2026-10-04/)
包含新的 `before.json`、固定 queue backup、`worker_before_stacks.txt`、
`selector_planner.json`、`local_planner_cycle.json`、`planner_before.json`、
`planner_after.json`、`restart.json`、`deployment/deployment_after.json`、
`history_sequence.json`、`browser/browser_acceptance.json`、`postdeploy.json`、
`current_error_deadlines.json`、`delegated_owner.json`、
`browser_final/browser_acceptance.json` 及 `final_acceptance.json`。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_finmind_throughput \
  --root data_finmind --minutes 60 \
  --output artifacts/data_quality/finmind_window_current.json
run_fintech_python -m scripts.audit_finmind_throughput \
  --root data_finmind \
  --benchmark-planner artifacts/data_quality/finmind_effective_throughput_2026-10-04/before.json \
  --output artifacts/data_quality/finmind_planner_recheck.json
```

以上診斷不呼叫 provider、不寫正式 queue，也不掃歷史 Parquet 樹；固定
基準若被更動會拒絕比較。程式碼、單元測試及即時部署驗收共同建立本輪
結論，任一 worker active 或 HTTP 200 均不能單獨替代這些證據。
