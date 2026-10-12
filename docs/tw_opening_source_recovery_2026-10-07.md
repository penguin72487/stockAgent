# 當沖開盤來源修復與實測：2026-10-07

本頁保留 10/07 原始處置及測速。10/08 發現此輪「所有非零目標先有價格」及
「90% 正值開盤價」門檻造成嚴重延遲；其修正由
[10/08 事故報告與契約 v4](tw_opening_latency_repair_2026-10-08.md) 接續。
不能把以下歷史成功測試視為後續開盤已經驗收。

## 結果與未完成邊界

開盤來源共用入口已修正並由 Discord 服務載入：有效開盤價保留整個交易日，
只補缺漏；原生 Snapshot 回傳完整清單但沒有有效開盤價時，立即接續 canonical
TWSE／TPEx MIS。缺少模型必要價格不得再被過濾成「模型選擇空倉」。

2026-10-07 10:49:01 的自動來源收據為 **2,209／2,302** 個有效開盤價；
尚缺 93 個，其中 88 個官方 MIS 回報當日成交量為 0、開盤價缺少，5 個來源
仍無法確認。自動缺漏重試正在執行；不能宣稱已取得所有價格，也不能替尚未
首筆成交的標的製造開盤價。

來源修復階段的 412 個相關 Python 測試通過。其後使用者明確選擇可追溯分鐘
資料的 09:01 模擬回放，已於 12:48:40 追加修復 10/06、10/07 Multi-Basis22；
原始訊號未改，其他模式的 16 個部位保留。這次受鎖保護的修復重新啟動了
執行引擎，新增 readiness 防呆已載入。回放及網站的另外 471 個相關測試通過。
下一次真正 09:00 的來源、推論及進場總延遲仍待現場驗收，沒有達成毫秒級或
一秒內的證據；回放不算即時開盤成功。

## 根因與成功條件

10-06 與 10-07 原始訊號的 active universe 分別為 2,302、2,294 個標的，
Snapshot 回應數完整，但當日有效開盤價只有 48、42 個。

舊路徑把「有回應」當作「模型價格輸入已齊」，因而略過缺漏補抓；訊號輸出再
把欠缺開盤價的非零模型權重過濾為零。兩日原始模型各有 3 個非零標的，卻都
輸出 0 個執行目標。帳本把結果記為 `no_order` 並消耗當日訊號，後續價格變齊
也無法自動救回原本已消耗的紀錄。

成功條件必須分開：

- transport response：API 有回傳資料，不證明當日成交或開盤價存在。
- opening input：正值、當日 09:00 後的來源時間，且非試撮行情。
- model readiness：符合既有有效輸入覆蓋門檻，且非零模型目標所需價格齊全。
- all-symbol completeness：每個請求標的都取得有效開盤價；不得以模型可運算
  代替完整來源的完成宣告。
- execution：仍須自己的當時可執行價格與模擬帳務證據；模型開盤輸入不是券商成交。

## 共用修正

`stockagent/live/quote_provider.py`：共用 `tw_session_open_observed_mask` 驗證
價格與交易時鐘；交易所時間與本機接收時間分離，不能以今日收到的昨日行情
冒充今日開盤。日內成功值不可被後續缺漏或衝突回應覆蓋，短 TTL 不使它失效。
schema 3 收據保存請求清單、成功值、來源時間與尚缺清單，支援重啟恢復。
量欄位保留真實 0，未知仍是未知；無首筆成交與取得失敗分開記錄。

`stockagent/live/signal_engine.py`：多模式共用取價，不各自重抓完整 universe。
先復用成功值，原生 Snapshot 只抓缺漏；若有效開盤仍缺，立即接續 MIS。
沿用原本設定的 90% 有效輸入門檻，修正其計數基礎；選中標的缺價時以可重試的
`waiting_source` 保留工作，不輸出假空倉。不縮減模型 universe 或更換 checkpoint。
保存來源排隊、原生取價、MIS 補抓、有效筆數及總耗時。

`services/discord_bot/bot.py`：新增既有服務管理的 `opening_source_repair`。
盤中只補來源缺漏；開盤訊號工作忙碌或重試時讓出資源。成功值持續保留，
缺漏以 0.5 秒逐步退避至最多 30 秒重試，有新成功則恢復短間隔。
最新狀態原子寫入，逐次耗時 append-only 保存；不推論、不下單、不回填歷史。

`stockagent/live/tw_day_trade_simulation.py`：登記訊號時再次核對 readiness 與
實際／必要輸入數量。資料未齊不得消耗當日訊號，真正模型零權重仍可正常記錄
一次。這項防呆已隨 12:48 的獲授權帳本修復重新載入執行引擎。

## 實測，不混用冷啟動與既有成功值

下表均為 **10-07 盤中的真實來源可用性**，不是 09:00 現場總延遲，也不是
同時點平衡 ABBA 性能比較。交易進行中的新增首筆成交會改變可取得筆數。

| 時間／路徑 | wall time | 有效開盤價 | 說明 |
| --- | ---: | ---: | --- |
| 10:15，修復原 42 筆 canonical 收據 | 2,238.55 ms | 2,165／2,294 | 2252 個缺漏原生查詢，再以 MIS 補缺；3 個選中標的皆齊 |
| 同一輪第二次缺漏補抓 | 593.31 ms | 2,165／2,294 | 僅重試 129 個缺漏；不是全量冷啟動 |
| 同一輪第三次缺漏補抓 | 325.57 ms | 2,165／2,294 | 缺漏仍保持明確未完成 |
| 10:40，隔離收據、0 筆既有開盤快取 | 5,431.19 ms | 2,200／2,294 | 原生 990.97 ms，含冷 MIS bootstrap／補抓 4414.13 ms |
| 10:43，完整模型只讀驗證 | 4,139.79 ms | 2,200／2,294 | 推論含來源查詢；不含 Python 模組 import；不發布、不下單 |

模型只讀驗證保留原 config fingerprint `ac2292b92c30`、checkpoint fingerprint
`44957faaac45`，完整 2,746 個模型標的。3 個原始非零權重皆保留為非零目標，
標的為 2330、3008、3661，gross 約 1；這是 **10:43 的驗證訊號**，不是冒充
當時 09:00 的訊號或成交。

共享來源的 2,302 個請求與 Multi-Basis22 的 2,294 個 active 標的不是相同分母。
10:49 的共享來源尚未確認標的為 `2067, 3632, 4155, 6167, 6173`；已查兩市場
回應仍無法證明當日開盤，不能擅自判定它們停牌或填入昨收。

## 部署、恢復與驗收

- 已重新載入 Discord：Invocation ID `580b1e103d974c0eae05863762a0e659`。
  Gateway 登入與命令同步正常，新背景工作產生實際重試收據。
- 來源修復階段當沖引擎 Invocation ID 保持
  `df1e90f8d3e24e7590ee83aaf8693f95`，12 個原始訊號檔 SHA 未改。
  隨後經使用者批准分鐘回放，12:48 起為
  `aab032b84d1948f3956b5c1bea105998`；不得把前一階段的未重啟描述當作目前狀態。
- Dashboard HTTP 200，但資料健康仍為 `degraded`，不能據此宣稱全系統正常。
- 已安排 `stockagent-opening-source-safe-reload-20261007.timer` 於 13:40
  執行既有 `reload_tw_day_trade_services_if_flat.py`。若仍有部位，回報
  `blocked_nonflat`／exit 75，不強制平倉或重啟。收據為
  `artifacts/live/tw_day_trade_simulation/safe_reload_receipt.json`。
  此次 timer 是 transient；不是已完成盤後載入或重開機驗收的證據。
- 412 測試涵蓋 48／2302、42／2294、失敗續抓、日內成功保持、重啟恢復、
  四模式來源共用、過期價格拒絕、0 成交量、資料未齊不消耗訊號及真實空倉。
  未宣稱全 repository 測試已執行。

## 可重測入口與證據

盤中來源核對使用 `scripts/audit_tw_opening_source.py`，完整保留當日 frozen
active universe。預設隔離來源收據；只有明確加 `--apply-current-session`
才修復 canonical cache。拒絕其他交易日的 weights，避免當日行情回填歷史。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_opening_source.py \
  --weights /absolute/path/to/today/target_weights.parquet \
  --parquet-root /srv/stockagent-live/data_tw_public/stocks \
  --output-root /absolute/path/to/a/new/audit_directory \
  --rounds 3
```

完整數據與收據：

- [原部分收據修復與逐輪耗時](../artifacts/operations/opening_source_recovery_20261007/live_repair/report.json)
- [獨立來源冷讀驗證](../artifacts/operations/opening_source_recovery_20261007/postdeploy_audit/report.json)
- [完整模型只讀驗證](../artifacts/operations/opening_source_recovery_20261007/model_verification.json)
- [部署前](../artifacts/operations/opening_source_recovery_20261007/deployment_before.json)、[部署後](../artifacts/operations/opening_source_recovery_20261007/deployment_after.json)
- [自動修復最新狀態](../artifacts/live/tw_day_trade_simulation/opening_source_repair.json)、[逐次真實耗時](../artifacts/live/tw_day_trade_simulation/opening_source_repair_latency.jsonl)
- [412 個相關測試](../artifacts/operations/agent-workflow/runs/opening-source-predeploy-validat-20261007T023647-c3f073be/run.log)

後續驗收：核對盤後 flat guard 收據，下一交易日分別量測第一筆有效
開盤輸入、門檻通過、模型完成、訊號發布、紙上交易進場與全來源完成時間。
零成交與未知來源持續保留，不以覆蓋率或 HTTP 成功掩蓋。

## 已批准的 09:01 模擬帳本修復

此次只修復 10/06、10/07 Multi-Basis22 的缺價假空倉，不更換模型、不重置
累積本金、不重新執行其他模式。沿用 config `ac2292b92c30`、checkpoint
`44957faaac45`、完整 2,746 個模型標的。原始非零訊號每一天均為 3 檔；
回放仍遵守整張股數、既有分鐘成交容量與價格契約，不把不足一張硬改為成交。

| 交易日 | 分鐘重建範圍 | 模擬進場 | 重建結果 |
| --- | --- | --- | --- |
| 10/06 | 09:01–13:30，270 點 | 7769，1,000 股 | 已沖銷；累積權益 NT$ 16,011,107.53 |
| 10/07 | 09:01–12:46，226 點；另補 12:47 接縫 | 2330 空 2,000 股、3008 空 1,000 股 | 12:48 起接續原服務的即時估值與出場流程 |

10/07 的分鐘 VWAP 進場價分別為 2330 的 NT$ 2,564.97、3008 的
NT$ 6,809.75。3661 的 -11.46% 模型目標不足一張，仍完整顯示在所有訊號，
不納入成交損益。10/06 原本的假空倉更正使累積已實現損益減少 NT$ 8,229.34；
修復不是只增加獲利。10/07 的後續未實現損益隨實際行情改變。

13:27 的唯讀驗收確認兩筆皆已由原服務沖銷：3008 於 13:22 以 NT$ 6,865
被動限價成交，2330 於 13:24 以 NT$ 2,575 市價模擬出場，剩餘股數均為 0。
今日合計已實現淨損益 NT$ -100,047.50，累積已實現淨損益
NT$ 5,911,060.03。此驗收未新增成交，也不代表 13:30 曲線終點已發生。

13:32 再核對實際已完成的收盤終點：兩日均為 09:01–13:30、各 270 點且無
缺分鐘。私有 API、公開 gateway、HTTPS API 的 NAV 和回放標記一致；今日
最後權益為 NT$ 15,911,060.03。這是原服務正常完成的收盤紀錄，沒有補造未來點。

來源先使用既有 10/06 台股分鐘 Parquet；只有今日缺少的三檔才用 Shioaji
歷史 K 棒補取。成交量依 canonical parser 驗證張／股與成交金額，不補造價格。
按交易日從保留的官方原始檔物化資格規則，避免拿最新資格替代歷史交易日。
所有使用的分鐘資料、官方原始檔、原始訊號、修復訊號與設定均保存 SHA。

原訊號時鐘、模擬生效時間與實際修復時間分別保留：今日原訊號為
09:00:02.267200，回放模擬進場為 09:01，實際帳本發布為 12:48:40。
`simulation_only=true`、`live_opening_slo_claim=false`。沒有券商下單。

操作入口為 [repair_tw_day_trade_flat_opening.py](../scripts/repair_tw_day_trade_flat_opening.py)：
`prepare` 只建立隔離候選，核對無既有成交與財務錨點；`apply` 再驗證來源 SHA、
候選新鮮度及最新帳本，取得 canonical engine lock 後才追加紀錄。其他模式
的完整狀態於鎖內比對不變。中途失敗保留稽核狀態並恢復原 owner，不盲目重跑。
這不是允許替換已有成交的 session，也不是整個帳本目錄的覆蓋。

12:47 的交接分鐘使用實際分鐘來源另行補上；此步只追加估值，未改持倉、
訂單、成交或服務狀態。497 個修復分鐘的 NAV 在私有 API、公開 gateway 與
HTTPS 頁面 API 一致，最大浮點還原差為 NT$ 0.0000000019。

另修正共用曲線讀取器：Polars 前 1,000 行推斷可能靜默遺漏後來才出現的
回放欄位；改為顯式財務／來源欄位 schema，並升版持久快取及 session shards。
避免回放被顯示成即時資料，也避免全量 schema 推斷的不必要掃描。公開 gateway
與私有 dashboard 均已載入；只重新啟動私有唯讀 dashboard child，未再次重啟
有持倉的 engine。

驗證範圍：331 個回放／帳務／分鐘曲線測試，140 個 dashboard／session
projection／gateway 測試；最終合併重跑 471 個，34.78 秒全部通過。
未宣稱全 repository 測試完成。13:22 的三層 API 均確認今日 09:01–13:22
連續 262 點，回放標記保留。公開瀏覽器實際選取
Multi-Basis22，確認筆電 1366×768、手機 390×844 的持倉、未成交完整訊號與
回放標籤，整頁橫向溢出為 0。瀏覽器使用 `cpu-2d`，只驗證 HTML／Canvas2D，
不宣稱 GPU／WebGL 通過。`content-visibility` 的延後布局必須先捲動並完成
原生繪製再讀 rendered text；不能把未布局區塊誤判為資料消失。

修復與驗收證據：

- [修復 manifest 與來源 SHA](../artifacts/operations/tw_daytrade_ledger_recovery_20261007/ledger_repair_manifest.json)
- [原始帳本狀態備份](../artifacts/operations/tw_daytrade_ledger_recovery_20261007/state_before_apply.json)
- [分鐘接縫修復](../artifacts/operations/tw_daytrade_ledger_recovery_20261007/mark_bridge_receipt.json)
- [分鐘 API 核對](../artifacts/operations/tw_daytrade_ledger_recovery_20261007/curve_http_acceptance.json)
- [瀏覽器核對](../artifacts/operations/tw_daytrade_ledger_recovery_20261007/browser_acceptance.json)
- [原服務沖銷驗收](../artifacts/operations/tw_daytrade_ledger_recovery_20261007/live_exit_acceptance.json)
- [兩日完整 270 分鐘終點驗收](../artifacts/operations/tw_daytrade_ledger_recovery_20261007/terminal_curve_acceptance.json)
- [331 個語義測試](../artifacts/operations/agent-workflow/runs/ledger-repair-tests-20261007T050754-d448127c/run.log)
- [140 個網站測試](../artifacts/operations/agent-workflow/runs/ledger-dashboard-tests-20261007T051239-05dec3eb/run.log)
- [最終合併 471 個測試](../artifacts/operations/agent-workflow/runs/ledger-final-regression-20261007T052629-067289b6/run.log)
- [本次最終驗收](../artifacts/operations/tw_daytrade_ledger_recovery_20261007/ledger_repair_acceptance.json)
